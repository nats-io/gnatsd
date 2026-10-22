// Copyright 2020-2026 The NATS Authors
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//go:build !skip_js_tests && !skip_js_cluster_tests

package server

import (
	"bytes"
	"context"
	crand "crypto/rand"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"math/rand/v2"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/klauspost/compress/s2"
	"github.com/nats-io/jwt/v2"
	"github.com/nats-io/nats.go"
)

func TestJetStreamClusterConfig(t *testing.T) {
	conf := createConfFile(t, []byte(`
		listen: 127.0.0.1:-1
		jetstream: {max_mem_store: 16GB, max_file_store: 10TB, store_dir: '%s'}
		cluster { listen: 127.0.0.1:-1 }
	`))

	check := func(errStr string) {
		t.Helper()
		opts, err := ProcessConfigFile(conf)
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		if _, err := NewServer(opts); err == nil || !strings.Contains(err.Error(), errStr) {
			t.Fatalf("Expected an error of `%s`, got `%v`", errStr, err)
		}
	}

	check("requires `server_name`")

	conf = createConfFile(t, []byte(`
		listen: 127.0.0.1:-1
		server_name: "TEST"
		jetstream: {max_mem_store: 16GB, max_file_store: 10TB, store_dir: '%s'}
		cluster { listen: 127.0.0.1:-1 }
	`))

	check("requires `cluster.name`")
}

func TestJetStreamClusterLeader(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "JSC", 3)
	defer c.shutdown()

	// Kill our current leader and force an election.
	c.leader().Shutdown()
	c.waitOnLeader()

	// Now killing our current leader should leave us leaderless.
	c.leader().Shutdown()
	c.expectNoLeader()
}

func TestJetStreamClusterExpand(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "JSC", 2)
	defer c.shutdown()

	c.addInNewServer()
	c.waitOnPeerCount(3)
}

func TestJetStreamClusterAccountInfo(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "JSC", 3)
	defer c.shutdown()

	nc := clientConnectToServer(t, c.randomServer())
	defer nc.Close()

	reply := nats.NewInbox()
	sub, _ := nc.SubscribeSync(reply)

	if err := nc.PublishRequest(JSApiAccountInfo, reply, nil); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	checkSubsPending(t, sub, 1)
	resp, _ := sub.NextMsg(0)

	var info JSApiAccountInfoResponse
	if err := json.Unmarshal(resp.Data, &info); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if info.JetStreamAccountStats == nil || info.Error != nil {
		t.Fatalf("Did not receive correct response: %+v", info.Error)
	}
	// Make sure we only got 1 response.
	// Technically this will always work since its a singleton service export.
	if nmsgs, _, _ := sub.Pending(); nmsgs > 0 {
		t.Fatalf("Expected only a single response, got %d more", nmsgs)
	}
}

func TestJetStreamClusterStreamLimitWithAccountDefaults(t *testing.T) {
	// 2MB memory, 8MB disk
	c := createJetStreamClusterWithTemplate(t, jsClusterLimitsTempl, "R3L", 3)
	defer c.shutdown()

	// Client based API
	s := c.randomNonLeader()
	nc, js := jsClientConnect(t, s)
	defer nc.Close()

	_, err := js.AddStream(&nats.StreamConfig{
		Name:     "TEST",
		Subjects: []string{"foo", "bar"},
		Replicas: 2,
		MaxBytes: 4 * 1024 * 1024,
	})
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	_, err = js.AddStream(&nats.StreamConfig{
		Name:     "TEST2",
		Replicas: 2,
		MaxBytes: 15 * 1024 * 1024,
	})
	require_Contains(t, err.Error(), "no suitable peers for placement", "insufficient storage")
}

func TestJetStreamClusterInfoRaftGroup(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "R1S", 3)
	defer c.shutdown()

	s := c.randomNonLeader()
	nc, js := jsClientConnect(t, s)
	defer nc.Close()

	acc := s.GlobalAccount()

	_, err := js.AddStream(&nats.StreamConfig{
		Name:     "TEST",
		Subjects: []string{"foo", "bar"},
		Storage:  nats.FileStorage,
		Replicas: 3,
	})
	require_NoError(t, err)

	nfoResp, err := nc.Request("$JS.API.STREAM.INFO.TEST", nil, time.Second)
	require_NoError(t, err)

	var si StreamInfo
	err = json.Unmarshal(nfoResp.Data, &si)
	require_NoError(t, err)

	if si.Cluster == nil {
		t.Fatalf("Expected cluster info, got none")
	}

	stream, err := acc.lookupStream("TEST")
	require_NoError(t, err)

	if si.Cluster.RaftGroup != stream.raftGroup().Name {
		t.Fatalf("Expected raft group %q to equal %q", si.Cluster.RaftGroup, stream.raftGroup().Name)
	}

	var sscfg StreamConfig
	rCfgData, err := os.ReadFile(filepath.Join(s.opts.StoreDir, "jetstream", "$SYS", "_js_", stream.raftGroup().Name, "meta.inf"))
	require_NoError(t, err)
	err = json.Unmarshal(rCfgData, &sscfg)
	require_NoError(t, err)
	if !reflect.DeepEqual(sscfg.Metadata, map[string]string{"account": "$G", "stream": "TEST", "type": "stream"}) {
		t.Fatalf("Invalid raft stream metadata: %v", sscfg.Metadata)
	}

	_, err = js.AddConsumer("TEST", &nats.ConsumerConfig{Durable: "DURABLE", Replicas: 3})
	require_NoError(t, err)

	consumer := stream.lookupConsumer("DURABLE")

	var ci ConsumerInfo
	nfoResp, err = nc.Request("$JS.API.CONSUMER.INFO.TEST.DURABLE", nil, time.Second)
	require_NoError(t, err)

	var cscfg ConsumerConfig
	rCfgData, err = os.ReadFile(filepath.Join(s.opts.StoreDir, "jetstream", "$SYS", "_js_", consumer.raftGroup().Name, "meta.inf"))
	require_NoError(t, err)
	err = json.Unmarshal(rCfgData, &cscfg)
	require_NoError(t, err)
	if !reflect.DeepEqual(cscfg.Metadata, map[string]string{"account": "$G", "consumer": "DURABLE", "stream": "TEST", "type": "consumer"}) {
		t.Fatalf("Invalid raft stream metadata: %v", cscfg.Metadata)
	}

	err = json.Unmarshal(nfoResp.Data, &ci)
	require_NoError(t, err)

	if ci.Cluster.RaftGroup != consumer.raftGroup().Name {
		t.Fatalf("Expected raft group %q to equal %q", ci.Cluster.RaftGroup, consumer.raftGroup().Name)
	}
}

func TestJetStreamClusterSingleReplicaStreams(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "R1S", 3)
	defer c.shutdown()

	// Client based API
	s := c.randomNonLeader()
	nc, js := jsClientConnect(t, s)
	defer nc.Close()

	si, err := js.AddStream(&nats.StreamConfig{
		Name:     "TEST",
		Subjects: []string{"foo", "bar"},
	})
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if si.Cluster == nil {
		t.Fatalf("Expected si to have cluster info")
	}
	// Send in 10 messages.
	msg, toSend := []byte("Hello JS Clustering"), 10
	for i := 0; i < toSend; i++ {
		if _, err = js.Publish("foo", msg); err != nil {
			t.Fatalf("Unexpected publish error: %v", err)
		}
	}
	// Now grab info for this stream.
	si, err = js.StreamInfo("TEST")
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if si == nil || si.Config.Name != "TEST" {
		t.Fatalf("StreamInfo is not correct %+v", si)
	}
	// Check active state as well, shows that the owner answered.
	if si.State.Msgs != uint64(toSend) {
		t.Fatalf("Expected %d msgs, got bad state: %+v", toSend, si.State)
	}
	// Now create a consumer. This should be pinned to same server that our stream was allocated to.
	// First do a normal sub.
	sub, err := js.SubscribeSync("foo")
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	checkSubsPending(t, sub, toSend)

	// Now create a consumer as well.
	ci, err := js.AddConsumer("TEST", &nats.ConsumerConfig{Durable: "dlc", AckPolicy: nats.AckExplicitPolicy})
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if ci == nil || ci.Name != "dlc" || ci.Stream != "TEST" {
		t.Fatalf("ConsumerInfo is not correct %+v", ci)
	}

	// Now make sure that if we kill and restart the server that this stream and consumer return.
	sl := c.streamLeader("$G", "TEST")
	sl.Shutdown()
	c.restartServer(sl)

	c.waitOnStreamLeader("$G", "TEST")
	si, err = js.StreamInfo("TEST")
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if si == nil || si.Config.Name != "TEST" {
		t.Fatalf("StreamInfo is not correct %+v", si)
	}
	// Now durable consumer.
	c.waitOnConsumerLeader("$G", "TEST", "dlc")
	if _, err = js.ConsumerInfo("TEST", "dlc"); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
}

func TestJetStreamClusterMultiReplicaStreams(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "RNS", 5)
	defer c.shutdown()

	// Client based API
	s := c.randomServer()
	nc, js := jsClientConnect(t, s)
	defer nc.Close()

	_, err := js.AddStream(&nats.StreamConfig{
		Name:     "TEST",
		Subjects: []string{"foo", "bar"},
		Replicas: 3,
	})
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	// Send in 10 messages.
	msg, toSend := []byte("Hello JS Clustering"), 10
	for i := 0; i < toSend; i++ {
		if _, err = js.Publish("foo", msg); err != nil {
			t.Fatalf("Unexpected publish error: %v", err)
		}
	}

	// Now grab info for this stream.
	si, err := js.StreamInfo("TEST")
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if si == nil || si.Config.Name != "TEST" {
		t.Fatalf("StreamInfo is not correct %+v", si)
	}
	// Check active state as well, shows that the owner answered.
	if si.State.Msgs != uint64(toSend) {
		t.Fatalf("Expected %d msgs, got bad state: %+v", toSend, si.State)
	}
	// Now create a consumer. This should be affinitize to the same set of servers as the stream.
	// First do a normal sub.
	sub, err := js.SubscribeSync("foo")
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	checkSubsPending(t, sub, toSend)

	// Now create a consumer as well.
	ci, err := js.AddConsumer("TEST", &nats.ConsumerConfig{Durable: "dlc", AckPolicy: nats.AckExplicitPolicy})
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if ci == nil || ci.Name != "dlc" || ci.Stream != "TEST" || ci.NumPending != uint64(toSend) {
		t.Fatalf("ConsumerInfo is not correct %+v", ci)
	}
}

func TestJetStreamClusterMultiReplicaStreamsDefaultFileMem(t *testing.T) {
	const testConfig = `
	listen: 127.0.0.1:-1
	server_name: %s
	jetstream: {store_dir: '%s'}

	cluster {
		name: %s
		listen: 127.0.0.1:%d
		routes = [%s]
	}
`
	c := createJetStreamClusterWithTemplate(t, testConfig, "RNS", 3)
	defer c.shutdown()

	// Client based API
	s := c.randomServer()
	nc, js := jsClientConnect(t, s)
	defer nc.Close()

	_, err := js.AddStream(&nats.StreamConfig{
		Name:     "TEST",
		Subjects: []string{"foo", "bar"},
		Replicas: 3,
		MaxBytes: 1024,
	})
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	// Send in 10 messages.
	msg, toSend := []byte("Hello JS Clustering"), 10
	for i := 0; i < toSend; i++ {
		if _, err = js.Publish("foo", msg); err != nil {
			t.Fatalf("Unexpected publish error: %v", err)
		}
	}

	// Now grab info for this stream.
	si, err := js.StreamInfo("TEST")
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if si == nil || si.Config.Name != "TEST" {
		t.Fatalf("StreamInfo is not correct %+v", si)
	}
	// Check active state as well, shows that the owner answered.
	if si.State.Msgs != uint64(toSend) {
		t.Fatalf("Expected %d msgs, got bad state: %+v", toSend, si.State)
	}
	// Now create a consumer. This should be affinitize to the same set of servers as the stream.
	// First do a normal sub.
	sub, err := js.SubscribeSync("foo")
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	checkSubsPending(t, sub, toSend)

	// Now create a consumer as well.
	ci, err := js.AddConsumer("TEST", &nats.ConsumerConfig{Durable: "dlc", AckPolicy: nats.AckExplicitPolicy})
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if ci == nil || ci.Name != "dlc" || ci.Stream != "TEST" || ci.NumPending != uint64(toSend) {
		t.Fatalf("ConsumerInfo is not correct %+v", ci)
	}
}

func TestJetStreamClusterMemoryStore(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "R3M", 3)
	defer c.shutdown()

	// Client based API
	nc, js := jsClientConnect(t, c.randomServer())
	defer nc.Close()

	_, err := js.AddStream(&nats.StreamConfig{
		Name:     "TEST",
		Subjects: []string{"foo", "bar"},
		Replicas: 3,
		Storage:  nats.MemoryStorage,
	})
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	// Send in 100 messages.
	msg, toSend := []byte("Hello MemoryStore"), 100
	for i := 0; i < toSend; i++ {
		if _, err = js.Publish("foo", msg); err != nil {
			t.Fatalf("Unexpected publish error: %v", err)
		}
	}
	// Now grab info for this stream.
	si, err := js.StreamInfo("TEST")
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if si == nil || si.Config.Name != "TEST" {
		t.Fatalf("StreamInfo is not correct %+v", si)
	}
	if si.Cluster == nil || len(si.Cluster.Replicas) != 2 {
		t.Fatalf("Cluster info is incorrect: %+v", si.Cluster)
	}
	// Check active state as well, shows that the owner answered.
	if si.State.Msgs != uint64(toSend) {
		t.Fatalf("Expected %d msgs, got bad state: %+v", toSend, si.State)
	}
	// Do a normal sub.
	sub, err := js.SubscribeSync("foo")
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	checkSubsPending(t, sub, toSend)
}

func TestJetStreamClusterDelete(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "RNS", 3)
	defer c.shutdown()

	// Client for API requests.
	nc, js := jsClientConnect(t, c.randomServer())
	defer nc.Close()

	cfg := &nats.StreamConfig{
		Name:     "C22",
		Subjects: []string{"foo", "bar", "baz"},
		Replicas: 2,
		Storage:  nats.FileStorage,
		MaxMsgs:  100,
	}
	if _, err := js.AddStream(cfg); err != nil {
		t.Fatalf("Error adding stream: %v", err)
	}

	// Now create a consumer.
	if _, err := js.AddConsumer("C22", &nats.ConsumerConfig{
		Durable:   "dlc",
		AckPolicy: nats.AckExplicitPolicy,
	}); err != nil {
		t.Fatalf("Error adding consumer: %v", err)
	}

	// Now delete the consumer.
	if err := js.DeleteConsumer("C22", "dlc"); err != nil {
		t.Fatalf("Error deleting consumer: %v", err)
	}

	// Now delete the stream.
	if err := js.DeleteStream("C22"); err != nil {
		t.Fatalf("Error deleting stream: %v", err)
	}

	// This will get the current information about usage and limits for this account.
	checkFor(t, time.Second, 15*time.Millisecond, func() error {
		info, err := js.AccountInfo()
		if err != nil {
			return err
		}
		if info.Streams != 0 {
			return fmt.Errorf("Expected no remaining streams, got %d", info.Streams)
		}
		return nil
	})
}

func TestJetStreamClusterStreamPurge(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "R5S", 5)
	defer c.shutdown()

	s := c.randomServer()

	// Client based API
	nc, js := jsClientConnect(t, s)
	defer nc.Close()

	_, err := js.AddStream(&nats.StreamConfig{
		Name:     "TEST",
		Subjects: []string{"foo", "bar"},
		Replicas: 3,
	})
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	msg, toSend := []byte("Hello JS Clustering"), 100
	for i := 0; i < toSend; i++ {
		if _, err = js.Publish("foo", msg); err != nil {
			t.Fatalf("Unexpected publish error: %v", err)
		}
	}

	// Now grab info for this stream.
	si, err := js.StreamInfo("TEST")
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	// Check active state as well, shows that the owner answered.
	if si.State.Msgs != uint64(toSend) {
		t.Fatalf("Expected %d msgs, got bad state: %+v", toSend, si.State)
	}

	// Now purge the stream.
	if err := js.PurgeStream("TEST"); err != nil {
		t.Fatalf("Unexpected purge error: %v", err)
	}
	si, err = js.StreamInfo("TEST")
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	if si.State.Msgs != 0 || si.State.FirstSeq != uint64(toSend+1) {
		t.Fatalf("Expected no msgs, got: %+v", si.State)
	}
}

func TestJetStreamClusterStreamUpdateSubjects(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "R3S", 3)
	defer c.shutdown()

	// Client based API
	s := c.randomServer()
	nc, js := jsClientConnect(t, s)
	defer nc.Close()

	cfg := &nats.StreamConfig{
		Name:     "TEST",
		Subjects: []string{"foo", "bar"},
		Replicas: 3,
	}

	if _, err := js.AddStream(cfg); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	// Make sure we can update subjects.
	cfg.Subjects = []string{"bar", "baz"}

	si, err := js.UpdateStream(cfg)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if si == nil {
		t.Fatalf("Expected a stream info, got none")
	}
	if !reflect.DeepEqual(si.Config.Subjects, cfg.Subjects) {
		t.Fatalf("Expected subjects to be updated: got %+v", si.Config.Subjects)
	}
	// Make sure it registered
	js2, err := nc.JetStream(nats.MaxWait(250 * time.Millisecond))
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	if _, err = js2.Publish("foo", nil); err == nil {
		t.Fatalf("Expected this to fail")
	}
	if _, err = js2.Publish("baz", nil); err != nil {
		t.Fatalf("Unexpected publish error: %v", err)
	}
}

func TestJetStreamClusterBadStreamUpdate(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "R3S", 3)
	defer c.shutdown()

	// Client based API
	s := c.randomServer()
	nc, js := jsClientConnect(t, s)
	defer nc.Close()

	cfg := &nats.StreamConfig{
		Name:     "TEST",
		Subjects: []string{"foo", "bar"},
		Replicas: 3,
	}

	if _, err := js.AddStream(cfg); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	msg, toSend := []byte("Keep Me"), 50
	for i := 0; i < toSend; i++ {
		if _, err := js.Publish("foo", msg); err != nil {
			t.Fatalf("Unexpected publish error: %v", err)
		}
	}

	// Make sure a bad update will not remove our stream.
	cfg.Subjects = []string{"foo..bar"}
	if _, err := js.UpdateStream(cfg); err == nil || err == nats.ErrTimeout {
		t.Fatalf("Expected error but got none or timeout")
	}

	// Make sure we did not delete our original stream.
	si, err := js.StreamInfo("TEST")
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if !reflect.DeepEqual(si.Config.Subjects, []string{"foo", "bar"}) {
		t.Fatalf("Expected subjects to be original ones, got %+v", si.Config.Subjects)
	}
}

func TestJetStreamClusterConsumerRedeliveredInfo(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "R3S", 3)
	defer c.shutdown()

	// Client based API
	nc, js := jsClientConnect(t, c.randomServer())
	defer nc.Close()

	cfg := &nats.StreamConfig{Name: "TEST"}
	if _, err := js.AddStream(cfg); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	if _, err := js.Publish("TEST", []byte("CI")); err != nil {
		t.Fatalf("Unexpected publish error: %v", err)
	}

	sub, _ := nc.SubscribeSync("R")
	sub.AutoUnsubscribe(2)

	ci, err := js.AddConsumer("TEST", &nats.ConsumerConfig{
		DeliverSubject: "R",
		AckPolicy:      nats.AckExplicitPolicy,
		AckWait:        100 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	checkSubsPending(t, sub, 2)
	sub.Unsubscribe()

	ci, err = js.ConsumerInfo("TEST", ci.Name)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if ci.NumRedelivered != 1 {
		t.Fatalf("Expected 1 redelivered, got %d", ci.NumRedelivered)
	}
}

func TestJetStreamClusterConsumerState(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "R3S", 5)
	defer c.shutdown()

	s := c.randomServer()

	// Client based API
	nc, js := jsClientConnect(t, s)
	defer nc.Close()

	_, err := js.AddStream(&nats.StreamConfig{
		Name:     "TEST",
		Subjects: []string{"foo", "bar"},
		Replicas: 3,
	})
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	msg, toSend := []byte("Hello JS Clustering"), 10
	for i := 0; i < toSend; i++ {
		if _, err = js.Publish("foo", msg); err != nil {
			t.Fatalf("Unexpected publish error: %v", err)
		}
	}

	// Make sure we are not connected to any of the stream servers so that we do not do client reconnect
	// when we take out the consumer leader.
	if s.JetStreamIsStreamAssigned("$G", "TEST") {
		nc.Close()
		for _, ns := range c.servers {
			if !ns.JetStreamIsStreamAssigned("$G", "TEST") {
				s = ns
				nc, js = jsClientConnect(t, s)
				defer nc.Close()
				break
			}
		}
	}

	sub, err := js.PullSubscribe("foo", "dlc")
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	// Pull 5 messages and ack.
	for _, m := range fetchMsgs(t, sub, 5, 5*time.Second) {
		m.AckSync()
	}

	// Let state propagate for exact comparison below.
	time.Sleep(200 * time.Millisecond)

	ci, err := sub.ConsumerInfo()
	if err != nil {
		t.Fatalf("Unexpected error getting consumer info: %v", err)
	}
	if ci.AckFloor.Consumer != 5 {
		t.Fatalf("Expected ack floor of %d, got %d", 5, ci.AckFloor.Consumer)
	}

	c.consumerLeader("$G", "TEST", "dlc").Shutdown()
	c.waitOnConsumerLeader("$G", "TEST", "dlc")

	nci, err := sub.ConsumerInfo()
	if err != nil {
		t.Fatalf("Unexpected error getting consumer info: %v", err)
	}
	// nil out timestamp for better comparison
	nci.Delivered.Last, ci.Delivered.Last = nil, nil
	if nci.Delivered != ci.Delivered {
		t.Fatalf("Consumer delivered did not match after leader switch, wanted %+v, got %+v", ci.Delivered, nci.Delivered)
	}
	nci.AckFloor.Last, ci.AckFloor.Last = nil, nil
	if nci.AckFloor != ci.AckFloor {
		t.Fatalf("Consumer ackfloor did not match after leader switch, wanted %+v, got %+v", ci.AckFloor, nci.AckFloor)
	}

	// Now make sure we can receive new messages.
	// Pull last 5.
	for _, m := range fetchMsgs(t, sub, 5, 5*time.Second) {
		m.AckSync()
	}

	nci, _ = sub.ConsumerInfo()
	if nci.Delivered.Consumer != 10 || nci.Delivered.Stream != 10 {
		t.Fatalf("Received bad delivered: %+v", nci.Delivered)
	}
	if nci.AckFloor.Consumer != 10 || nci.AckFloor.Stream != 10 {
		t.Fatalf("Received bad ackfloor: %+v", nci.AckFloor)
	}
	if nci.NumAckPending != 0 {
		t.Fatalf("Received bad ackpending: %+v", nci.NumAckPending)
	}
}

func TestJetStreamClusterFullConsumerState(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "R3S", 3)
	defer c.shutdown()

	s := c.randomServer()

	// Client based API
	nc, js := jsClientConnect(t, s)
	defer nc.Close()

	_, err := js.AddStream(&nats.StreamConfig{
		Name:     "TEST",
		Subjects: []string{"foo", "bar"},
		Replicas: 3,
	})
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	msg, toSend := []byte("Hello JS Clustering"), 10
	for i := 0; i < toSend; i++ {
		if _, err = js.Publish("foo", msg); err != nil {
			t.Fatalf("Unexpected publish error: %v", err)
		}
	}

	sub, err := js.PullSubscribe("foo", "dlc")
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	fetchMsgs(t, sub, 1, 5*time.Second)

	// Now purge the stream.
	if err := js.PurgeStream("TEST"); err != nil {
		t.Fatalf("Unexpected purge error: %v", err)
	}
}

func TestJetStreamClusterMetaSnapshotsAndCatchup(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "R3S", 3)
	defer c.shutdown()

	// Shut one down.
	rs := c.randomServer()
	rs.Shutdown()

	c.waitOnLeader()
	s := c.leader()

	// Client based API
	nc, js := jsClientConnect(t, s)
	defer nc.Close()

	numStreams := 4
	// Create 4 streams
	// FIXME(dlc) - R2 make sure we place properly.
	for i := 0; i < numStreams; i++ {
		sn := fmt.Sprintf("T-%d", i+1)
		_, err := js.AddStream(&nats.StreamConfig{Name: sn})
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
	}

	c.leader().JetStreamSnapshotMeta()

	rs = c.restartServer(rs)
	c.checkClusterFormed()
	c.waitOnServerCurrent(rs)

	rs.Shutdown()
	c.waitOnLeader()

	for i := 0; i < numStreams; i++ {
		sn := fmt.Sprintf("T-%d", i+1)
		err := js.DeleteStream(sn)
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
	}

	rs = c.restartServer(rs)
	c.checkClusterFormed()
	c.waitOnServerCurrent(rs)
}

func TestJetStreamClusterMetaSnapshotsMultiChange(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "R3S", 2)
	defer c.shutdown()

	s := c.leader()

	// Client based API
	nc, js := jsClientConnect(t, s)
	defer nc.Close()

	// Add in 2 streams with 1 consumer each.
	if _, err := js.AddStream(&nats.StreamConfig{Name: "S1"}); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	c.waitOnStreamLeader(globalAccountName, "S1")
	_, err := js.AddConsumer("S1", &nats.ConsumerConfig{Durable: "S1C1", AckPolicy: nats.AckExplicitPolicy})
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	c.waitOnConsumerLeader(globalAccountName, "S1", "S1C1")

	if _, err = js.AddStream(&nats.StreamConfig{Name: "S2"}); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	c.waitOnStreamLeader(globalAccountName, "S2")
	_, err = js.AddConsumer("S2", &nats.ConsumerConfig{Durable: "S2C1", AckPolicy: nats.AckExplicitPolicy})
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	c.waitOnConsumerLeader(globalAccountName, "S2", "S2C1")

	// Add in a new server to the group. This way we know we can delete the original streams and consumers.
	rs := c.addInNewServer()
	c.waitOnServerCurrent(rs)
	rsn := rs.Name()

	// Shut it down.
	rs.Shutdown()

	// Wait for the peer to be removed.
	checkFor(t, 2*time.Second, 100*time.Millisecond, func() error {
		for _, p := range s.JetStreamClusterPeers() {
			if p == rsn {
				return fmt.Errorf("Old server still in peer set")
			}
		}
		return nil
	})

	// We want to make changes here that test each delta scenario for the meta snapshots.
	// Add new stream and consumer.
	if _, err = js.AddStream(&nats.StreamConfig{Name: "S3"}); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	c.waitOnStreamLeader(globalAccountName, "S3")
	_, err = js.AddConsumer("S3", &nats.ConsumerConfig{Durable: "S3C1", AckPolicy: nats.AckExplicitPolicy})
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	c.waitOnConsumerLeader(globalAccountName, "S3", "S3C1")
	// Delete stream S2
	resp, _ := nc.Request(fmt.Sprintf(JSApiStreamDeleteT, "S2"), nil, time.Second)
	var dResp JSApiStreamDeleteResponse
	if err := json.Unmarshal(resp.Data, &dResp); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if !dResp.Success || dResp.Error != nil {
		t.Fatalf("Got a bad response %+v", dResp.Error)
	}
	// Delete the consumer on S1 but add another.
	resp, err = nc.Request(fmt.Sprintf(JSApiConsumerDeleteT, "S1", "S1C1"), nil, time.Second)
	require_NoError(t, err)
	require_NotNil(t, resp)
	var cdResp JSApiConsumerDeleteResponse
	if err = json.Unmarshal(resp.Data, &cdResp); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if !cdResp.Success || cdResp.Error != nil {
		t.Fatalf("Got a bad response %+v", cdResp)
	}
	// Add new consumer on S1
	_, err = js.AddConsumer("S1", &nats.ConsumerConfig{Durable: "S1C2", AckPolicy: nats.AckExplicitPolicy})
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	c.waitOnConsumerLeader(globalAccountName, "S1", "S1C2")

	cl := c.leader()
	cl.JetStreamSnapshotMeta()
	c.waitOnServerCurrent(cl)

	rs = c.restartServer(rs)
	c.checkClusterFormed()
	c.waitOnServerCurrent(rs)
}

func TestJetStreamClusterStreamSynchedTimeStamps(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "R3S", 3)
	defer c.shutdown()

	// Client based API
	s := c.randomServer()
	nc, js := jsClientConnect(t, s)
	defer nc.Close()

	_, err := js.AddStream(&nats.StreamConfig{Name: "foo", Storage: nats.MemoryStorage, Replicas: 3})
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	if _, err = js.Publish("foo", []byte("TSS")); err != nil {
		t.Fatalf("Unexpected publish error: %v", err)
	}

	// Grab the message and timestamp from our current leader
	sub, err := js.SubscribeSync("foo")
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	m, err := sub.NextMsg(time.Second)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	meta, _ := m.Metadata()

	sub.Unsubscribe()

	sl := c.streamLeader("$G", "foo")

	sl.Shutdown()

	c.waitOnLeader()
	c.waitOnStreamLeader("$G", "foo")

	nc, js = jsClientConnect(t, c.leader())
	defer nc.Close()

	sm, err := js.GetMsg("foo", 1)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if !sm.Time.Equal(meta.Timestamp) {
		t.Fatalf("Expected same timestamps, got %v vs %v", sm.Time, meta.Timestamp)
	}
}

// Test to mimic what R.I. was seeing.
func TestJetStreamClusterRestoreSingleConsumer(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "R3S", 3)
	defer c.shutdown()

	// Client based API
	s := c.randomServer()
	nc, js := jsClientConnect(t, s)
	defer nc.Close()

	_, err := js.AddStream(&nats.StreamConfig{Name: "foo"})
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	if _, err = js.Publish("foo", []byte("TSS")); err != nil {
		t.Fatalf("Unexpected publish error: %v", err)
	}

	sub, err := js.SubscribeSync("foo", nats.Durable("dlc"))
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if m, err := sub.NextMsg(time.Second); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	} else {
		m.AckSync()
	}

	c.stopAll()
	c.restartAll()
	c.waitOnLeader()
	c.waitOnStreamLeader("$G", "foo")

	s = c.randomServer()
	nc, js = jsClientConnect(t, s)
	defer nc.Close()

	var names []string
	for name := range js.StreamNames() {
		names = append(names, name)
	}
	if len(names) != 1 {
		t.Fatalf("Expected only 1 stream but got %d", len(names))
	}

	// Now do detailed version.
	var infos []*nats.StreamInfo
	for info := range js.StreamsInfo() {
		infos = append(infos, info)
	}
	if len(infos) != 1 {
		t.Fatalf("Expected 1 stream but got %d", len(infos))
	}
	si, err := js.StreamInfo("foo")
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if si == nil || si.Config.Name != "foo" {
		t.Fatalf("StreamInfo is not correct %+v", si)
	}

	// Now check for consumer.
	names = names[:0]
	for name := range js.ConsumerNames("foo") {
		names = append(names, name)
	}
	if len(names) != 1 {
		t.Fatalf("Expected 1 consumer but got %d", len(names))
	}
}

func TestJetStreamClusterMaxBytesForStream(t *testing.T) {
	// Has max_file_store of 2GB
	c := createJetStreamClusterExplicit(t, "R3S", 3)
	defer c.shutdown()

	// Client based API
	nc, js := jsClientConnect(t, c.randomServer())
	defer nc.Close()

	info, err := js.AccountInfo()
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	// Make sure we still are dynamic.
	if info.Limits.MaxStore != -1 || info.Limits.MaxMemory != -1 {
		t.Fatalf("Expected dynamic limits for the account, got %+v\n", info.Limits)
	}
	// Stream config.
	cfg := &nats.StreamConfig{
		Name:     "TEST",
		Replicas: 2,
		MaxBytes: 2 * 1024 * 1024 * 1024, // 2GB
	}
	_, err = js.AddStream(cfg)
	require_NoError(t, err)

	// Make sure going over the single server limit though is enforced (for now).
	cfg.Name = "TEST2"
	cfg.MaxBytes *= 2
	_, err = js.AddStream(cfg)
	require_Contains(t, err.Error(), "no suitable peers for placement")
}

func TestJetStreamClusterStreamPublishWithActiveConsumers(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "R3S", 3)
	defer c.shutdown()

	s := c.randomServer()

	// Client based API
	nc, js := jsClientConnect(t, s)
	defer nc.Close()

	_, err := js.AddStream(&nats.StreamConfig{Name: "foo", Replicas: 3})
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	if _, err = js.Publish("foo", []byte("TSS")); err != nil {
		t.Fatalf("Unexpected publish error: %v", err)
	}

	sub, err := js.SubscribeSync("foo", nats.Durable("dlc"))
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	// FIXME(dlc) - Need to track this down.
	c.waitOnConsumerLeader("$G", "foo", "dlc")

	if m, err := sub.NextMsg(time.Second); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	} else {
		m.AckSync()
	}

	// Send 10 messages.
	for i := 1; i <= 10; i++ {
		payload := []byte(fmt.Sprintf("MSG-%d", i))
		if _, err = js.Publish("foo", payload); err != nil {
			t.Fatalf("Unexpected publish error: %v", err)
		}
	}
	checkSubsPending(t, sub, 10)
	// Sanity check for duplicate deliveries..
	if nmsgs, _, _ := sub.Pending(); nmsgs > 10 {
		t.Fatalf("Expected only %d responses, got %d more", 10, nmsgs)
	}
	for i := 1; i <= 10; i++ {
		m, err := sub.NextMsg(time.Second)
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		payload := []byte(fmt.Sprintf("MSG-%d", i))
		if !bytes.Equal(m.Data, payload) {
			t.Fatalf("Did not get expected msg, expected %q, got %q", payload, m.Data)
		}
	}

	var ci *nats.ConsumerInfo
	checkFor(t, time.Second, 100*time.Millisecond, func() error {
		ci, err = sub.ConsumerInfo()
		if err != nil {
			return fmt.Errorf("Unexpected error getting consumer info: %v", err)
		}
		if ci.Delivered.Stream != 11 {
			return fmt.Errorf("expected delivered stream sequence to be %d, got %d", 11, ci.Delivered.Stream)
		}
		return nil
	})

	c.consumerLeader("$G", "foo", "dlc").Shutdown()
	c.waitOnConsumerLeader("$G", "foo", "dlc")

	ci2, err := sub.ConsumerInfo()
	if err != nil {
		t.Fatalf("Unexpected error getting consumer info: %v", err)
	}

	ci.Cluster = nil
	ci2.Cluster = nil

	// nil out timestamp for better comparison
	ci.Delivered.Last, ci2.Delivered.Last = nil, nil
	ci.AckFloor.Last, ci2.AckFloor.Last = nil, nil
	if !reflect.DeepEqual(ci, ci2) {
		t.Fatalf("Consumer info did not match: %+v vs %+v", ci, ci2)
	}

	// In case the server above was also stream leader.
	c.waitOnStreamLeader("$G", "foo")

	// Now send more..
	// Send 10 more messages.
	for i := 11; i <= 20; i++ {
		payload := []byte(fmt.Sprintf("MSG-%d", i))
		if _, err = js.Publish("foo", payload); err != nil {
			t.Fatalf("Unexpected publish error: %v", err)
		}
	}

	checkSubsPending(t, sub, 10)
	// Sanity check for duplicate deliveries..
	if nmsgs, _, _ := sub.Pending(); nmsgs > 10 {
		t.Fatalf("Expected only %d responses, got %d more", 10, nmsgs)
	}

	for i := 11; i <= 20; i++ {
		m, err := sub.NextMsg(time.Second)
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		payload := []byte(fmt.Sprintf("MSG-%d", i))
		if !bytes.Equal(m.Data, payload) {
			t.Fatalf("Did not get expected msg, expected %q, got %q", payload, m.Data)
		}
	}
}

func TestJetStreamClusterStreamOverlapSubjects(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "R3", 3)
	defer c.shutdown()

	// Client based API
	s := c.randomServer()
	nc, js := jsClientConnect(t, s)
	defer nc.Close()

	if _, err := js.AddStream(&nats.StreamConfig{Name: "TEST", Subjects: []string{"foo"}}); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	if _, err := js.AddStream(&nats.StreamConfig{Name: "TEST2", Subjects: []string{"foo"}}); err == nil || err == nats.ErrTimeout {
		t.Fatalf("Expected error but got none or timeout: %v", err)
	}

	// Now grab list of streams and make sure the second is not there.
	var names []string
	for name := range js.StreamNames() {
		names = append(names, name)
	}
	if len(names) != 1 {
		t.Fatalf("Expected only 1 stream but got %d", len(names))
	}

	// Now do a detailed version.
	var infos []*nats.StreamInfo
	for info := range js.StreamsInfo() {
		infos = append(infos, info)
	}
	if len(infos) != 1 {
		t.Fatalf("Expected only 1 stream but got %d", len(infos))
	}
}

func TestJetStreamClusterStreamInfoList(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "R3S", 3)
	defer c.shutdown()

	// Client based API
	s := c.randomServer()
	nc, js := jsClientConnect(t, s)
	defer nc.Close()

	createStream := func(name string) {
		t.Helper()
		if _, err := js.AddStream(&nats.StreamConfig{Name: name}); err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
	}

	createStream("foo")
	createStream("bar")
	createStream("baz")

	sendBatch := func(subject string, n int) {
		t.Helper()
		// Send a batch to a given subject.
		for i := 0; i < n; i++ {
			if _, err := js.Publish(subject, []byte("OK")); err != nil {
				t.Fatalf("Unexpected publish error: %v", err)
			}
		}
	}

	sendBatch("foo", 10)
	sendBatch("bar", 22)
	sendBatch("baz", 33)

	// Now get the stream list info.
	var infos []*nats.StreamInfo
	checkFor(t, 2*time.Second, 100*time.Millisecond, func() error {
		infos = infos[:0]
		for info := range js.StreamsInfo() {
			infos = append(infos, info)
		}
		if len(infos) != 3 {
			return fmt.Errorf("StreamInfo expected 3 results, got %d", len(infos))
		}
		return nil
	})

	for _, si := range infos {
		switch si.Config.Name {
		case "foo":
			if si.State.Msgs != 10 {
				t.Fatalf("Expected %d msgs but got %d", 10, si.State.Msgs)
			}
		case "bar":
			if si.State.Msgs != 22 {
				t.Fatalf("Expected %d msgs but got %d", 22, si.State.Msgs)
			}
		case "baz":
			if si.State.Msgs != 33 {
				t.Fatalf("Expected %d msgs but got %d", 33, si.State.Msgs)
			}
		}
	}
}

func TestJetStreamClusterConsumerInfoList(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "R3S", 3)
	defer c.shutdown()

	// Client based API
	s := c.randomServer()
	nc, js := jsClientConnect(t, s)
	defer nc.Close()

	if _, err := js.AddStream(&nats.StreamConfig{Name: "TEST", Replicas: 3}); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	// Place messages so we can generate consumer state.
	for i := 0; i < 10; i++ {
		if _, err := js.Publish("TEST", []byte("OK")); err != nil {
			t.Fatalf("Unexpected publish error: %v", err)
		}
	}

	createConsumer := func(name string) *nats.Subscription {
		t.Helper()
		sub, err := js.PullSubscribe("TEST", name)
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		return sub
	}

	subFoo := createConsumer("foo")
	subBar := createConsumer("bar")
	subBaz := createConsumer("baz")

	// Place consumers in various states.
	for _, ss := range []struct {
		sub   *nats.Subscription
		fetch int
		ack   int
	}{
		{subFoo, 4, 2},
		{subBar, 2, 0},
		{subBaz, 8, 6},
	} {
		msgs := fetchMsgs(t, ss.sub, ss.fetch, 5*time.Second)
		for i := 0; i < ss.ack; i++ {
			msgs[i].AckSync()
		}
	}

	// Now get the consumer list info.
	var infos []*nats.ConsumerInfo
	for info := range js.ConsumersInfo("TEST") {
		infos = append(infos, info)
	}
	if len(infos) != 3 {
		t.Fatalf("ConsumerInfo expected 3 results, got %d", len(infos))
	}
	for _, ci := range infos {
		switch ci.Name {
		case "foo":
			if ci.Delivered.Consumer != 4 {
				t.Fatalf("Expected %d delivered but got %d", 4, ci.Delivered.Consumer)
			}
			if ci.AckFloor.Consumer != 2 {
				t.Fatalf("Expected %d for ack floor but got %d", 2, ci.AckFloor.Consumer)
			}
		case "bar":
			if ci.Delivered.Consumer != 2 {
				t.Fatalf("Expected %d delivered but got %d", 2, ci.Delivered.Consumer)
			}
			if ci.AckFloor.Consumer != 0 {
				t.Fatalf("Expected %d for ack floor but got %d", 0, ci.AckFloor.Consumer)
			}
		case "baz":
			if ci.Delivered.Consumer != 8 {
				t.Fatalf("Expected %d delivered but got %d", 8, ci.Delivered.Consumer)
			}
			if ci.AckFloor.Consumer != 6 {
				t.Fatalf("Expected %d for ack floor but got %d", 6, ci.AckFloor.Consumer)
			}
		}
	}
}

func TestJetStreamClusterStreamUpdate(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "R3S", 3)
	defer c.shutdown()

	// Client based API
	s := c.randomServer()
	nc, js := jsClientConnect(t, s)
	defer nc.Close()

	sc := &nats.StreamConfig{
		Name:     "TEST",
		Subjects: []string{"foo"},
		Replicas: 3,
		MaxMsgs:  10,
		Discard:  DiscardNew,
	}

	if _, err := js.AddStream(sc); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	for i := 1; i <= int(sc.MaxMsgs); i++ {
		msg := []byte(fmt.Sprintf("HELLO JSC-%d", i))
		if _, err := js.Publish("foo", msg); err != nil {
			t.Fatalf("Unexpected publish error: %v", err)
		}
	}

	// Expect error here.
	if _, err := js.Publish("foo", []byte("fail")); err == nil {
		t.Fatalf("Expected publish to fail")
	}

	// Now update MaxMsgs, select non-leader
	s = c.randomNonStreamLeader("$G", "TEST")
	nc, js = jsClientConnect(t, s)
	defer nc.Close()

	sc.MaxMsgs = 20
	si, err := js.UpdateStream(sc)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if si.Config.MaxMsgs != 20 {
		t.Fatalf("Expected to have config updated with max msgs of %d, got %d", 20, si.Config.MaxMsgs)
	}

	// Do one that will fail. Wait and make sure we only are getting one response.
	sc.Name = "TEST22"

	rsub, _ := nc.SubscribeSync(nats.NewInbox())
	defer rsub.Unsubscribe()
	nc.Flush()

	req, _ := json.Marshal(sc)
	if err := nc.PublishRequest(fmt.Sprintf(JSApiStreamUpdateT, "TEST"), rsub.Subject, req); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	// Wait incase more than one reply sent.
	time.Sleep(250 * time.Millisecond)

	if nmsgs, _, _ := rsub.Pending(); err != nil || nmsgs != 1 {
		t.Fatalf("Expected only one response, got %d", nmsgs)
	}

	m, err := rsub.NextMsg(time.Second)
	if err != nil {
		t.Fatalf("Error getting message: %v", err)
	}

	var scResp JSApiStreamCreateResponse
	if err := json.Unmarshal(m.Data, &scResp); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if scResp.StreamInfo != nil || scResp.Error == nil {
		t.Fatalf("Did not receive correct response: %+v", scResp)
	}
}

func TestJetStreamClusterStreamExtendedUpdates(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "R3S", 3)
	defer c.shutdown()

	// Client based API
	s := c.randomServer()
	nc, js := jsClientConnect(t, s)
	defer nc.Close()

	cfg := &nats.StreamConfig{
		Name:     "TEST",
		Subjects: []string{"foo"},
		Replicas: 3,
	}
	if _, err := js.AddStream(cfg); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	updateStream := func() *nats.StreamInfo {
		si, err := js.UpdateStream(cfg)
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		return si
	}

	// Subjects can be updated
	cfg.Subjects = []string{"bar", "baz"}
	if si := updateStream(); !reflect.DeepEqual(si.Config.Subjects, cfg.Subjects) {
		t.Fatalf("Did not get expected stream info: %+v", si)
	}
	// Mirror changes are not supported for now
	cfg.Subjects = nil
	cfg.Mirror = &nats.StreamSource{Name: "ORDERS"}
	_, err := js.UpdateStream(cfg)
	require_Error(t, err, NewJSStreamMirrorNotUpdatableError())
}

func TestJetStreamClusterDoubleAdd(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "R32", 2)
	defer c.shutdown()

	s := c.randomServer()

	// Client based API
	nc, js := jsClientConnect(t, s)
	defer nc.Close()

	if _, err := js.AddStream(&nats.StreamConfig{Name: "TEST", Replicas: 2}); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	// Streams should allow double add.
	if _, err := js.AddStream(&nats.StreamConfig{Name: "TEST", Replicas: 2}); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	// Check Consumers.
	cfg := &nats.ConsumerConfig{Durable: "dlc", AckPolicy: nats.AckExplicitPolicy}
	if _, err := js.AddConsumer("TEST", cfg); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	// Check double add ok.
	if _, err := js.AddConsumer("TEST", cfg); err != nil {
		t.Fatalf("Expected no error but got: %v", err)
	}
}

func TestJetStreamClusterDefaultMaxAckPending(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "R32", 2)
	defer c.shutdown()

	s := c.randomServer()

	// Client based API
	nc, js := jsClientConnect(t, s)
	defer nc.Close()

	if _, err := js.AddStream(&nats.StreamConfig{Name: "TEST", Replicas: 2}); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	// Do Consumers too.
	cfg := &nats.ConsumerConfig{Durable: "dlc", AckPolicy: nats.AckExplicitPolicy}
	ci, err := js.AddConsumer("TEST", cfg)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	// Check that we have a default set now for the max ack pending.
	if ci.Config.MaxAckPending != JsDefaultMaxAckPending {
		t.Fatalf("Expected a default for max ack pending of %d, got %d", JsDefaultMaxAckPending, ci.Config.MaxAckPending)
	}
}

func TestJetStreamClusterStreamNormalCatchup(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "R3S", 3)
	defer c.shutdown()

	// Client based API
	s := c.randomServer()
	nc, js := jsClientConnect(t, s)
	defer nc.Close()

	_, err := js.AddStream(&nats.StreamConfig{
		Name:     "TEST",
		Subjects: []string{"foo", "bar"},
		Replicas: 3,
	})
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	toSend := 10
	for i := 1; i <= toSend; i++ {
		msg := []byte(fmt.Sprintf("HELLO JSC-%d", i))
		if _, err = js.Publish("foo", msg); err != nil {
			t.Fatalf("Unexpected publish error: %v", err)
		}
	}

	sl := c.streamLeader("$G", "TEST")
	sl.Shutdown()
	c.waitOnStreamLeader("$G", "TEST")

	// Send 10 more while one replica offline.
	for i := toSend; i <= toSend*2; i++ {
		msg := []byte(fmt.Sprintf("HELLO JSC-%d", i))
		if _, err = js.Publish("foo", msg); err != nil {
			t.Fatalf("Unexpected publish error: %v", err)
		}
	}

	// Delete the first from the second batch.
	dreq := JSApiMsgDeleteRequest{Seq: uint64(toSend)}
	dreqj, err := json.Marshal(dreq)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	resp, _ := nc.Request(fmt.Sprintf(JSApiMsgDeleteT, "TEST"), dreqj, time.Second)
	var delMsgResp JSApiMsgDeleteResponse
	if err = json.Unmarshal(resp.Data, &delMsgResp); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if !delMsgResp.Success || delMsgResp.Error != nil {
		t.Fatalf("Got a bad response %+v", delMsgResp.Error)
	}

	sl = c.restartServer(sl)
	c.checkClusterFormed()

	c.waitOnServerCurrent(sl)
	c.waitOnStreamCurrent(sl, "$G", "TEST")
}

func TestJetStreamClusterStreamSnapshotCatchup(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "R3S", 3)
	defer c.shutdown()

	s := c.randomServer()

	// Client based API
	nc, js := jsClientConnect(t, s)
	defer nc.Close()

	_, err := js.AddStream(&nats.StreamConfig{
		Name:     "TEST",
		Subjects: []string{"foo"},
		Replicas: 3,
	})
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	pseq := uint64(1)
	sendBatch := func(n int) {
		t.Helper()
		// Send a batch.
		for i := 0; i < n; i++ {
			msg := []byte(fmt.Sprintf("HELLO JSC-%d", pseq))
			if _, err = js.Publish("foo", msg); err != nil {
				t.Fatalf("Unexpected publish error: %v", err)
			}
			pseq++
		}
	}

	sendBatch(2)

	sl := c.streamLeader("$G", "TEST")
	sl.Shutdown()
	c.waitOnStreamLeader("$G", "TEST")

	sendBatch(100)

	deleteMsg := func(seq uint64) {
		if err := js.DeleteMsg("TEST", seq); err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
	}

	// Delete the first from the second batch.
	deleteMsg(pseq / 2)
	// Delete the next one too.
	deleteMsg(pseq/2 + 1)

	nsl := c.streamLeader("$G", "TEST")
	nsl.JetStreamSnapshotStream("$G", "TEST")

	// Do some activity post snapshot as well.
	// Delete next to last.
	deleteMsg(pseq - 2)
	// Send another batch.
	sendBatch(100)

	mset, err := nsl.GlobalAccount().lookupStream("TEST")
	require_NoError(t, err)
	ostate := mset.stateWithDetail(true)

	sl = c.restartServer(sl)
	c.checkClusterFormed()

	c.waitOnServerCurrent(sl)
	c.waitOnStreamCurrent(sl, "$G", "TEST")

	mset, err = sl.GlobalAccount().lookupStream("TEST")
	require_NoError(t, err)

	checkFor(t, 2*time.Second, 100*time.Millisecond, func() error {
		if nstate := mset.stateWithDetail(true); !reflect.DeepEqual(ostate, nstate) {
			return fmt.Errorf("States do not match after recovery: %+v vs %+v", ostate, nstate)
		}
		return nil
	})
}

func TestJetStreamClusterDeleteMsg(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "R3S", 3)
	defer c.shutdown()

	// Client based API
	s := c.randomServer()
	nc, js := jsClientConnect(t, s)
	defer nc.Close()

	// R=1 make sure delete works.
	_, err := js.AddStream(&nats.StreamConfig{Name: "TEST"})
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	toSend := 10
	for i := 1; i <= toSend; i++ {
		msg := []byte(fmt.Sprintf("HELLO JSC-%d", i))
		if _, err = js.Publish("TEST", msg); err != nil {
			t.Fatalf("Unexpected publish error: %v", err)
		}
	}

	deleteMsg := func(seq uint64) {
		if err := js.DeleteMsg("TEST", seq); err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
	}

	deleteMsg(1)

	// Also make sure purge of R=1 works too.
	if err := js.PurgeStream("TEST"); err != nil {
		t.Fatalf("Unexpected purge error: %v", err)
	}
}

func TestJetStreamClusterDeleteMsgAndRestart(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "R3S", 3)
	defer c.shutdown()

	// Client based API
	s := c.randomServer()
	nc, js := jsClientConnect(t, s)
	defer nc.Close()

	// R=1 make sure delete works.
	_, err := js.AddStream(&nats.StreamConfig{Name: "TEST", Replicas: 2})
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	toSend := 10
	for i := 1; i <= toSend; i++ {
		msg := []byte(fmt.Sprintf("HELLO JSC-%d", i))
		if _, err = js.Publish("TEST", msg); err != nil {
			t.Fatalf("Unexpected publish error: %v", err)
		}
	}

	deleteMsg := func(seq uint64) {
		if err := js.DeleteMsg("TEST", seq); err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
	}

	deleteMsg(1)

	c.stopAll()
	c.restartAll()

	c.waitOnStreamLeader("$G", "TEST")
}

func TestJetStreamClusterStreamSnapshotCatchupWithPurge(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "R5S", 5)
	defer c.shutdown()

	s := c.randomServer()

	// Client based API
	nc, js := jsClientConnect(t, s)
	defer nc.Close()

	_, err := js.AddStream(&nats.StreamConfig{
		Name:     "TEST",
		Subjects: []string{"foo"},
		Replicas: 3,
	})
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	sl := c.streamLeader("$G", "TEST")

	sl.Shutdown()
	c.waitOnStreamLeader("$G", "TEST")

	toSend := 10
	for i := 0; i < toSend; i++ {
		if _, err = js.Publish("foo", []byte("OK")); err != nil {
			t.Fatalf("Unexpected publish error: %v", err)
		}
	}

	nsl := c.streamLeader("$G", "TEST")
	if err := nsl.JetStreamSnapshotStream("$G", "TEST"); err != nil {
		t.Fatalf("Error snapshotting stream: %v", err)
	}
	time.Sleep(250 * time.Millisecond)

	sl = c.restartServer(sl)
	c.checkClusterFormed()

	// Now purge the stream while we are recovering.
	if err := js.PurgeStream("TEST"); err != nil {
		t.Fatalf("Unexpected purge error: %v", err)
	}

	c.waitOnServerCurrent(sl)
	c.waitOnStreamCurrent(sl, "$G", "TEST")

	nsl.Shutdown()
	c.waitOnStreamLeader("$G", "TEST")

	if _, err := js.StreamInfo("TEST"); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
}

func TestJetStreamClusterExtendedStreamInfo(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "R3S", 3)
	defer c.shutdown()

	// Client based API
	s := c.randomServer()
	nc, js := jsClientConnect(t, s)
	defer nc.Close()

	_, err := js.AddStream(&nats.StreamConfig{
		Name:     "TEST",
		Subjects: []string{"foo"},
		Replicas: 3,
	})
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	toSend := 50
	for i := 0; i < toSend; i++ {
		if _, err = js.Publish("foo", []byte("OK")); err != nil {
			t.Fatalf("Unexpected publish error: %v", err)
		}
	}

	leader := c.streamLeader("$G", "TEST").Name()

	si, err := js.StreamInfo("TEST")
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if si.Cluster == nil {
		t.Fatalf("Expected cluster info")
	}
	if si.Cluster.Name != c.name {
		t.Fatalf("Expected cluster name of %q, got %q", c.name, si.Cluster.Name)
	}

	if si.Cluster.Leader != leader {
		t.Fatalf("Expected leader of %q, got %q", leader, si.Cluster.Leader)
	}
	if len(si.Cluster.Replicas) != 2 {
		t.Fatalf("Expected %d replicas, got %d", 2, len(si.Cluster.Replicas))
	}

	// Make sure that returned array is ordered
	for i := 0; i < 50; i++ {
		si, err := js.StreamInfo("TEST")
		require_NoError(t, err)
		require_True(t, len(si.Cluster.Replicas) == 2)
		s1 := si.Cluster.Replicas[0].Name
		s2 := si.Cluster.Replicas[1].Name
		if s1 > s2 {
			t.Fatalf("Expected replicas to be ordered, got %s then %s", s1, s2)
		}
	}

	// We may need to wait a bit for peers to catch up.
	checkFor(t, 2*time.Second, 100*time.Millisecond, func() error {
		return checkState(t, c, "$G", "TEST")
	})

	// Shutdown the leader.
	oldLeader := c.streamLeader("$G", "TEST")
	oldLeader.Shutdown()

	c.waitOnStreamLeader("$G", "TEST")

	// Re-request.
	leader = c.streamLeader("$G", "TEST").Name()
	si, err = js.StreamInfo("TEST")
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if si.Cluster == nil {
		t.Fatalf("Expected cluster info")
	}
	if si.Cluster.Leader != leader {
		t.Fatalf("Expected leader of %q, got %q", leader, si.Cluster.Leader)
	}
	if len(si.Cluster.Replicas) != 2 {
		t.Fatalf("Expected %d replicas, got %d", 2, len(si.Cluster.Replicas))
	}
	for _, peer := range si.Cluster.Replicas {
		if peer.Name == oldLeader.Name() {
			if peer.Current {
				t.Fatalf("Expected old leader to be reported as not current: %+v", peer)
			}
		} else if !peer.Current {
			t.Fatalf("Expected replica to be current: %+v", peer)
		}
	}

	// Now send a few more messages then restart the oldLeader.
	for i := 0; i < 10; i++ {
		if _, err = js.Publish("foo", []byte("OK")); err != nil {
			t.Fatalf("Unexpected publish error: %v", err)
		}
	}

	oldLeader = c.restartServer(oldLeader)
	c.checkClusterFormed()

	c.waitOnStreamLeader("$G", "TEST")
	c.waitOnStreamCurrent(oldLeader, "$G", "TEST")

	// Re-request.
	leader = c.streamLeader("$G", "TEST").Name()
	si, err = js.StreamInfo("TEST")
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if si.Cluster == nil {
		t.Fatalf("Expected cluster info")
	}
	if si.Cluster.Leader != leader {
		t.Fatalf("Expected leader of %q, got %q", leader, si.Cluster.Leader)
	}
	if len(si.Cluster.Replicas) != 2 {
		t.Fatalf("Expected %d replicas, got %d", 2, len(si.Cluster.Replicas))
	}

	// We may need to wait a bit for peers to catch up.
	checkFor(t, 10*time.Second, 100*time.Millisecond, func() error {
		return checkState(t, c, "$G", "TEST")
	})

	nc, js = jsClientConnect(t, c.randomServer())
	defer nc.Close()

	// Now do consumer.
	sub, err := js.PullSubscribe("foo", "dlc")
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	defer sub.Unsubscribe()
	fetchMsgs(t, sub, 10, 5*time.Second)

	leader = c.consumerLeader("$G", "TEST", "dlc").Name()
	ci, err := sub.ConsumerInfo()
	if err != nil {
		t.Fatalf("Unexpected error getting consumer info: %v", err)
	}

	if ci.Cluster.Leader != leader {
		t.Fatalf("Expected leader of %q, got %q", leader, ci.Cluster.Leader)
	}
	if len(ci.Cluster.Replicas) != 2 {
		t.Fatalf("Expected %d replicas, got %d", 2, len(ci.Cluster.Replicas))
	}
	checkFor(t, 10*time.Second, 100*time.Millisecond, func() error {
		return checkState(t, c, "$G", "TEST")
	})
}

func TestJetStreamClusterExtendedStreamInfoSingleReplica(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "R3S", 3)
	defer c.shutdown()

	// Client based API
	s := c.randomServer()
	nc, js := jsClientConnect(t, s)
	defer nc.Close()

	_, err := js.AddStream(&nats.StreamConfig{
		Name:     "TEST",
		Subjects: []string{"foo"},
	})
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	toSend := 50
	for i := 0; i < toSend; i++ {
		if _, err = js.Publish("foo", []byte("OK")); err != nil {
			t.Fatalf("Unexpected publish error: %v", err)
		}
	}

	leader := c.streamLeader("$G", "TEST").Name()

	si, err := js.StreamInfo("TEST")
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if si.Cluster == nil {
		t.Fatalf("Expected cluster info")
	}
	if si.Cluster.Name != c.name {
		t.Fatalf("Expected cluster name of %q, got %q", c.name, si.Cluster.Name)
	}
	if si.Cluster.Leader != leader {
		t.Fatalf("Expected leader of %q, got %q", leader, si.Cluster.Leader)
	}
	if len(si.Cluster.Replicas) != 0 {
		t.Fatalf("Expected no replicas but got %d", len(si.Cluster.Replicas))
	}

	// Make sure we can grab consumer lists from any
	var infos []*nats.ConsumerInfo
	for info := range js.ConsumersInfo("TEST") {
		infos = append(infos, info)
	}
	if len(infos) != 0 {
		t.Fatalf("ConsumerInfo expected no paged results, got %d", len(infos))
	}

	// Now add in a consumer.
	cfg := &nats.ConsumerConfig{Durable: "dlc", AckPolicy: nats.AckExplicitPolicy}
	if _, err := js.AddConsumer("TEST", cfg); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	infos = infos[:0]
	for info := range js.ConsumersInfo("TEST") {
		infos = append(infos, info)
	}
	if len(infos) != 1 {
		t.Fatalf("ConsumerInfo expected 1 result, got %d", len(infos))
	}

	// Now do direct names list as well.
	var names []string
	for name := range js.ConsumerNames("TEST") {
		names = append(names, name)
	}
	if len(names) != 1 {
		t.Fatalf("Expected only 1 consumer but got %d", len(names))
	}
}

func TestJetStreamClusterInterestRetention(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "R3S", 3)
	defer c.shutdown()

	// Client based API
	s := c.randomServer()
	nc, js := jsClientConnect(t, s)
	defer nc.Close()

	_, err := js.AddStream(&nats.StreamConfig{Name: "foo", Retention: nats.InterestPolicy, Replicas: 3})
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	sub, err := js.SubscribeSync("foo", nats.Durable("dlc"))
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	sl := c.streamLeader("$G", "foo")
	cl := c.consumerLeader("$G", "foo", "dlc")
	if sl == cl {
		_, err := nc.Request(fmt.Sprintf(JSApiStreamLeaderStepDownT, "foo"), nil, time.Second)
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		c.waitOnStreamLeader("$G", "foo")
	}

	if _, err = js.Publish("foo", []byte("OK")); err != nil {
		t.Fatalf("Unexpected publish error: %v", err)
	}

	m, err := sub.NextMsg(time.Second)
	if err != nil {
		t.Fatalf("Unexpected error getting msg: %v", err)
	}
	m.AckSync()

	waitForZero := func() {
		checkFor(t, 2*time.Second, 100*time.Millisecond, func() error {
			si, err := js.StreamInfo("foo")
			if err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}
			if si.State.Msgs != 0 {
				return fmt.Errorf("Expected 0 msgs, got state: %+v", si.State)
			}
			return nil
		})
	}

	waitForZero()

	// Add in 50 messages.
	for i := 0; i < 50; i++ {
		if _, err = js.Publish("foo", []byte("more")); err != nil {
			t.Fatalf("Unexpected publish error: %v", err)
		}
	}
	checkSubsPending(t, sub, 50)

	// Now delete the consumer and make sure the stream goes to zero.
	if err := js.DeleteConsumer("foo", "dlc"); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	waitForZero()
}

// https://github.com/nats-io/nats-server/issues/2243
func TestJetStreamClusterWorkQueueRetention(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "R3S", 3)
	defer c.shutdown()

	// Client based API
	s := c.randomServer()
	nc, js := jsClientConnect(t, s)
	defer nc.Close()

	_, err := js.AddStream(&nats.StreamConfig{
		Name:      "FOO",
		Subjects:  []string{"foo.*"},
		Replicas:  2,
		Retention: nats.WorkQueuePolicy,
	})
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	sub, err := js.PullSubscribe("foo.test", "test")
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	if _, err = js.Publish("foo.test", []byte("OK")); err != nil {
		t.Fatalf("Unexpected publish error: %v", err)
	}
	si, err := js.StreamInfo("FOO")
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if si.State.Msgs != 1 {
		t.Fatalf("Expected 1 msg, got state: %+v", si.State)
	}

	// Fetch from our pull consumer and ack.
	for _, m := range fetchMsgs(t, sub, 1, 5*time.Second) {
		m.AckSync()
	}

	// Make sure the messages are removed.
	checkFor(t, 5*time.Second, 100*time.Millisecond, func() error {
		si, err := js.StreamInfo("FOO")
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		if si.State.Msgs != 0 {
			return fmt.Errorf("Expected 0 msgs, got state: %+v", si.State)
		}
		return nil
	})

}

func TestJetStreamClusterMirrorAndSourceWorkQueues(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "WQ", 3)
	defer c.shutdown()

	// Client for API requests.
	nc, js := jsClientConnect(t, c.randomServer())
	defer nc.Close()

	_, err := js.AddStream(&nats.StreamConfig{
		Name:      "WQ22",
		Subjects:  []string{"foo"},
		Replicas:  2,
		Retention: nats.WorkQueuePolicy,
	})
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	_, err = js.AddStream(&nats.StreamConfig{
		Name:     "M",
		Replicas: 2,
		Mirror:   &nats.StreamSource{Name: "WQ22"},
	})
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	_, err = js.AddStream(&nats.StreamConfig{
		Name:     "S",
		Replicas: 2,
		Sources:  []*nats.StreamSource{{Name: "WQ22"}},
	})
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	// Allow direct sync consumers to connect.
	time.Sleep(1500 * time.Millisecond)

	if _, err = js.Publish("foo", []byte("ok")); err != nil {
		t.Fatalf("Unexpected publish error: %v", err)
	}

	checkFor(t, 5*time.Second, 250*time.Millisecond, func() error {
		if si, _ := js.StreamInfo("WQ22"); si.State.Msgs != 0 {
			return fmt.Errorf("Expected no msgs for %q, got %d", "WQ22", si.State.Msgs)
		}
		if si, _ := js.StreamInfo("M"); si.State.Msgs != 1 {
			return fmt.Errorf("Expected 1 msg for %q, got %d", "M", si.State.Msgs)
		}
		if si, _ := js.StreamInfo("S"); si.State.Msgs != 1 {
			return fmt.Errorf("Expected 1 msg for %q, got %d", "S", si.State.Msgs)
		}
		return nil
	})

}

func TestJetStreamClusterMirrorAndSourceInterestPolicyStream(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "WQ", 3)
	defer c.shutdown()

	// Client for API requests.
	nc, js := jsClientConnect(t, c.randomServer())
	defer nc.Close()

	_, err := js.AddStream(&nats.StreamConfig{
		Name:      "IP22",
		Subjects:  []string{"foo"},
		Replicas:  3,
		Retention: nats.InterestPolicy,
	})
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	_, err = js.AddStream(&nats.StreamConfig{
		Name:     "M",
		Replicas: 2,
		Mirror:   &nats.StreamSource{Name: "IP22"},
	})
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	_, err = js.AddStream(&nats.StreamConfig{
		Name:     "S",
		Replicas: 2,
		Sources:  []*nats.StreamSource{{Name: "IP22"}},
	})
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	// Allow sync consumers to connect.
	time.Sleep(1500 * time.Millisecond)

	if _, err = js.Publish("foo", []byte("ok")); err != nil {
		t.Fatalf("Unexpected publish error: %v", err)
	}

	checkFor(t, 5*time.Second, 250*time.Millisecond, func() error {
		// This one will be 0 since no other interest exists.
		if si, _ := js.StreamInfo("IP22"); si.State.Msgs != 0 {
			return fmt.Errorf("Expected no msgs for %q, got %d", "IP22", si.State.Msgs)
		}
		if si, _ := js.StreamInfo("M"); si.State.Msgs != 1 {
			return fmt.Errorf("Expected 1 msg for %q, got %d", "M", si.State.Msgs)
		}
		if si, _ := js.StreamInfo("S"); si.State.Msgs != 1 {
			return fmt.Errorf("Expected 1 msg for %q, got %d", "S", si.State.Msgs)
		}
		return nil
	})

	// Now create other interest on IP22.
	sub, err := js.SubscribeSync("foo")
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	defer sub.Unsubscribe()
	// Allow consumer state to propagate.
	time.Sleep(500 * time.Millisecond)

	if _, err = js.Publish("foo", []byte("ok")); err != nil {
		t.Fatalf("Unexpected publish error: %v", err)
	}

	checkFor(t, 5*time.Second, 250*time.Millisecond, func() error {
		// This one will be 0 since no other interest exists.
		if si, _ := js.StreamInfo("IP22"); si.State.Msgs != 1 {
			return fmt.Errorf("Expected 1 msg for %q, got %d", "IP22", si.State.Msgs)
		}
		if si, _ := js.StreamInfo("M"); si.State.Msgs != 2 {
			return fmt.Errorf("Expected 2 msgs for %q, got %d", "M", si.State.Msgs)
		}
		if si, _ := js.StreamInfo("S"); si.State.Msgs != 2 {
			return fmt.Errorf("Expected 2 msgs for %q, got %d", "S", si.State.Msgs)
		}
		return nil
	})
}

func TestJetStreamClusterInterestRetentionWithFilteredConsumers(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "R3S", 3)
	defer c.shutdown()

	// Client based API
	nc, js := jsClientConnect(t, c.randomServer())
	defer nc.Close()

	_, err := js.AddStream(&nats.StreamConfig{Name: "TEST", Subjects: []string{"*"}, Retention: nats.InterestPolicy, Replicas: 3})
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	fsub, err := js.SubscribeSync("foo", nats.Durable("d1"))
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	defer fsub.Unsubscribe()

	bsub, err := js.SubscribeSync("bar", nats.Durable("d2"))
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	defer bsub.Unsubscribe()

	msg := []byte("FILTERED")
	sendMsg := func(subj string) {
		t.Helper()
		if _, err = js.Publish(subj, msg); err != nil {
			t.Fatalf("Unexpected publish error: %v", err)
		}
	}

	getAndAck := func(sub *nats.Subscription) {
		t.Helper()
		m, err := sub.NextMsg(time.Second)
		if err != nil {
			t.Fatalf("Unexpected error getting msg: %v", err)
		}
		m.AckSync()
	}

	jsq, err := nc.JetStream(nats.MaxWait(250 * time.Millisecond))
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	checkState := func(expected uint64) {
		t.Helper()
		checkFor(t, 5*time.Second, 100*time.Millisecond, func() error {
			t.Helper()
			si, err := jsq.StreamInfo("TEST")
			if err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}
			if si.State.Msgs != expected {
				return fmt.Errorf("Expected %d msgs, got %d", expected, si.State.Msgs)
			}
			return nil
		})
	}

	sendMsg("foo")
	checkState(1)
	getAndAck(fsub)
	checkState(0)
	sendMsg("bar")
	sendMsg("foo")
	checkState(2)
	getAndAck(bsub)
	checkState(1)
	getAndAck(fsub)
	checkState(0)

	// Now send a bunch of messages and then delete the consumer.
	for i := 0; i < 10; i++ {
		sendMsg("foo")
		sendMsg("bar")
	}
	checkState(20)

	if err := js.DeleteConsumer("TEST", "d1"); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if err := js.DeleteConsumer("TEST", "d2"); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	checkState(0)

	// Now make sure pull based consumers work same.
	if _, err := js.PullSubscribe("foo", "dlc"); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	// Now send a bunch of messages and then delete the consumer.
	for i := 0; i < 10; i++ {
		sendMsg("foo")
		sendMsg("bar")
	}
	checkState(10)

	if err := js.DeleteConsumer("TEST", "dlc"); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	checkState(0)
}

func TestJetStreamClusterEphemeralConsumerNoImmediateInterest(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "R3S", 3)
	defer c.shutdown()

	// Client based API
	s := c.randomServer()
	nc, js := jsClientConnect(t, s)
	defer nc.Close()

	_, err := js.AddStream(&nats.StreamConfig{Name: "TEST", Replicas: 3})
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	// We want to relax the strict interest requirement.
	ci, err := js.AddConsumer("TEST", &nats.ConsumerConfig{DeliverSubject: "r"})
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	cl := c.consumerLeader("$G", "TEST", ci.Name)
	mset, err := cl.GlobalAccount().lookupStream("TEST")
	if err != nil {
		t.Fatalf("Expected to find a stream for %q", "TEST")
	}
	o := mset.lookupConsumer(ci.Name)
	if o == nil {
		t.Fatalf("Error looking up consumer %q", ci.Name)
	}
	o.setInActiveDeleteThreshold(500 * time.Millisecond)

	// Make sure the consumer goes away though eventually.
	// Should be 5 seconds wait.
	checkFor(t, 5*time.Second, 100*time.Millisecond, func() error {
		if _, err := js.ConsumerInfo("TEST", ci.Name); err != nil {
			return nil
		}
		return fmt.Errorf("Consumer still present")
	})
}

func TestJetStreamClusterEphemeralConsumerCleanup(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "R3S", 3)
	defer c.shutdown()

	// Client based API
	s := c.randomServer()
	nc, js := jsClientConnect(t, s)
	defer nc.Close()

	_, err := js.AddStream(&nats.StreamConfig{Name: "foo", Replicas: 2})
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	sub, err := js.Subscribe("foo", func(m *nats.Msg) {})
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	ci, _ := sub.ConsumerInfo()
	if ci == nil {
		t.Fatalf("Unexpected error: no consumer info")
	}

	// We will look up by hand this consumer to set inactive threshold lower for this test.
	cl := c.consumerLeader("$G", "foo", ci.Name)
	if cl == nil {
		t.Fatalf("Could not find consumer leader")
	}
	mset, err := cl.GlobalAccount().lookupStream("foo")
	if err != nil {
		t.Fatalf("Expected to find a stream for %q", "foo")
	}
	o := mset.lookupConsumer(ci.Name)
	if o == nil {
		t.Fatalf("Error looking up consumer %q", ci.Name)
	}
	o.setInActiveDeleteThreshold(10 * time.Millisecond)

	msg, toSend := []byte("Hello JS Clustering"), 10
	for i := 0; i < toSend; i++ {
		if _, err = js.Publish("foo", msg); err != nil {
			t.Fatalf("Unexpected publish error: %v", err)
		}
	}

	getConsumers := func() []string {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()

		var names []string
		for name := range js.ConsumerNames("foo", nats.Context(ctx)) {
			names = append(names, name)
		}
		return names
	}

	checkConsumer := func(expected int) {
		consumers := getConsumers()
		if len(consumers) != expected {
			t.Fatalf("Expected %d consumers but got %d", expected, len(consumers))
		}
	}

	checkConsumer(1)

	// Now Unsubscribe, since this is ephemeral this will make this go away.
	sub.Unsubscribe()

	checkFor(t, 2*time.Second, 100*time.Millisecond, func() error {
		if consumers := getConsumers(); len(consumers) == 0 {
			return nil
		} else {
			return fmt.Errorf("Still %d consumers remaining", len(consumers))
		}
	})
}

func TestJetStreamClusterEphemeralConsumersNotReplicated(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "R3S", 3)
	defer c.shutdown()

	// Client based API
	s := c.randomServer()
	nc, js := jsClientConnect(t, s)
	defer nc.Close()

	_, err := js.AddStream(&nats.StreamConfig{Name: "foo", Replicas: 3})
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	sub, err := js.SubscribeSync("foo")
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	ci, _ := sub.ConsumerInfo()
	if ci == nil {
		t.Fatalf("Unexpected error: no consumer info")
	}

	if _, err = js.Publish("foo", []byte("OK")); err != nil {
		t.Fatalf("Unexpected publish error: %v", err)
	}
	checkSubsPending(t, sub, 1)
	sub.NextMsg(0)

	if ci.Cluster == nil || len(ci.Cluster.Replicas) != 0 {
		t.Fatalf("Expected ephemeral to be R=1, got %+v", ci.Cluster)
	}
	scl := c.serverByName(ci.Cluster.Leader)
	if scl == nil {
		t.Fatalf("Could not select server where ephemeral consumer is running")
	}

	// Test migrations. If we are also metadata leader will not work so skip.
	if scl == c.leader() {
		return
	}

	scl.Shutdown()
	c.waitOnStreamLeader("$G", "foo")

	if _, err = js.Publish("foo", []byte("OK")); err != nil {
		t.Fatalf("Unexpected publish error: %v", err)
	}

	if _, err := sub.NextMsg(500 * time.Millisecond); err != nil {
		t.Logf("Expected to see another message, but behavior is optimistic so can fail")
	}
}

func TestJetStreamClusterUserSnapshotAndRestore(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "R3S", 3)
	defer c.shutdown()

	// Client based API
	s := c.randomServer()
	nc, js := jsClientConnect(t, s)
	defer nc.Close()

	_, err := js.AddStream(&nats.StreamConfig{
		Name:     "TEST",
		Subjects: []string{"foo"},
		Replicas: 2,
	})
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	toSend := 200

	for i := 0; i < toSend; i++ {
		if _, err = js.Publish("foo", []byte("OK")); err != nil {
			t.Fatalf("Unexpected publish error: %v", err)
		}
	}

	// Create consumer with no state.
	_, err = js.AddConsumer("TEST", &nats.ConsumerConfig{Durable: "rip", AckPolicy: nats.AckExplicitPolicy})
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	// Create another consumer as well and give it a non-simplistic state.
	_, err = js.AddConsumer("TEST", &nats.ConsumerConfig{Durable: "dlc", AckPolicy: nats.AckExplicitPolicy, AckWait: 10 * time.Second})
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	// Clustered ephemeral consumers use the assignment name as their identity,
	// while the original consumer config can have an empty Name. Make sure a
	// snapshot remains restorable in that case.
	ephSubj := nats.NewInbox()
	ephSub, err := nc.Subscribe(ephSubj, func(*nats.Msg) {})
	if err != nil {
		t.Fatalf("Unexpected error subscribing for ephemeral consumer: %v", err)
	}
	defer ephSub.Unsubscribe()
	if err := nc.Flush(); err != nil {
		t.Fatalf("Unexpected error flushing ephemeral consumer subscription: %v", err)
	}
	eph, err := js.AddConsumer("TEST", &nats.ConsumerConfig{
		DeliverSubject:    ephSubj,
		DeliverPolicy:     nats.DeliverNewPolicy,
		AckPolicy:         nats.AckNonePolicy,
		InactiveThreshold: time.Minute,
	})
	if err != nil {
		t.Fatalf("Unexpected error creating ephemeral consumer: %v", err)
	}

	jsub, err := js.PullSubscribe("foo", "dlc")
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	// Ack first 50.
	for _, m := range fetchMsgs(t, jsub, 50, 5*time.Second) {
		require_NoError(t, m.AckSync())
	}
	// Now ack every third message for next 50.
	for i, m := range fetchMsgs(t, jsub, 50, 5*time.Second) {
		if i%3 == 0 {
			require_NoError(t, m.AckSync())
		}
	}

	// Snapshot consumer info.
	ci, err := jsub.ConsumerInfo()
	if err != nil {
		t.Fatalf("Unexpected error getting consumer info: %v", err)
	}

	sreq := &JSApiStreamSnapshotRequest{
		DeliverSubject: nats.NewInbox(),
		ChunkSize:      512,
	}

	req, _ := json.Marshal(sreq)
	rmsg, err := nc.Request(fmt.Sprintf(JSApiStreamSnapshotT, "TEST"), req, time.Second)
	if err != nil {
		t.Fatalf("Unexpected error on snapshot request: %v", err)
	}

	var resp JSApiStreamSnapshotResponse
	json.Unmarshal(rmsg.Data, &resp)
	if resp.Error != nil {
		t.Fatalf("Did not get correct error response: %+v", resp.Error)
	}

	// Grab state for comparison.
	state := *resp.State
	config := *resp.Config

	var snapshot []byte
	done := make(chan bool)

	sub, _ := nc.Subscribe(sreq.DeliverSubject, func(m *nats.Msg) {
		// EOF
		if len(m.Data) == 0 {
			done <- true
			return
		}
		// Could be writing to a file here too.
		snapshot = append(snapshot, m.Data...)
		// Flow ack
		m.Respond(nil)
	})
	defer sub.Unsubscribe()

	// Wait to receive the snapshot.
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatalf("Did not receive our snapshot in time")
	}

	var rresp JSApiStreamRestoreResponse
	rreq := &JSApiStreamRestoreRequest{
		Config: config,
		State:  state,
	}
	req, _ = json.Marshal(rreq)

	// Make sure a restore to an existing stream fails.
	rmsg, err = nc.Request(fmt.Sprintf(JSApiStreamRestoreT, "TEST"), req, 2*time.Second)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	json.Unmarshal(rmsg.Data, &rresp)
	if !IsNatsErr(rresp.Error, JSStreamNameExistRestoreFailedErr) {
		t.Fatalf("Did not get correct error response: %+v", rresp.Error)
	}

	if _, err := js.StreamInfo("TEST"); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	// Now make sure a restore will work.
	// Delete our stream first.
	if err := js.DeleteStream("TEST"); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if _, err := js.StreamInfo("TEST"); err == nil || !strings.Contains(err.Error(), "not found") {
		t.Fatalf("Expected not found error: %v", err)
	}

	// This should work properly.
	rmsg, err = nc.Request(fmt.Sprintf(JSApiStreamRestoreT, "TEST"), req, 5*time.Second)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	rresp.Error = nil
	json.Unmarshal(rmsg.Data, &rresp)
	if rresp.Error != nil {
		t.Fatalf("Got an unexpected error response: %+v", rresp.Error)
	}
	if rresp.DeliverSubject == _EMPTY_ {
		t.Fatalf("No deliver subject set on response: %+v", rresp)
	}
	// Send our snapshot back in to restore the stream.
	// Can be any size message.
	var chunk [1024]byte
	for r := bytes.NewReader(snapshot); ; {
		n, err := r.Read(chunk[:])
		if err != nil {
			break
		}
		nc.Request(rresp.DeliverSubject, chunk[:n], time.Second)
	}
	rmsg, err = nc.Request(rresp.DeliverSubject, nil, 2*time.Second)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	rresp.Error = nil
	json.Unmarshal(rmsg.Data, &rresp)
	if rresp.Error != nil {
		t.Fatalf("Got an unexpected error response: %+v", rresp.Error)
	}
	pa, err := js.Publish("foo", []byte("OK"))
	if err != nil {
		t.Fatalf("Unexpected publish error after restore: %v", err)
	}
	if pa.Sequence != uint64(toSend+1) {
		t.Fatalf("Expected sequence %d after restore, got %d", toSend+1, pa.Sequence)
	}
	toSend++

	si, err := js.StreamInfo("TEST")
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if si == nil || si.Config.Name != "TEST" || si.State.Msgs != uint64(toSend) {
		t.Fatalf("StreamInfo is not correct %+v", si)
	}

	// Make sure the replicas become current eventually. They will be doing catchup.
	checkFor(t, 10*time.Second, 100*time.Millisecond, func() error {
		return checkState(t, c, "$G", "TEST")
	})

	// Wait on the system to elect a leader for the restored consumer.
	c.waitOnConsumerLeader("$G", "TEST", "dlc")
	c.waitOnConsumerLeader("$G", "TEST", eph.Name)

	// Now check for the consumer being recreated.
	nci, err := js.ConsumerInfo("TEST", "dlc")
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	// Cluster consumer info does not include the sparse pending state, so the
	// restore conservatively rolls delivery back to the ack floor.
	nci.Delivered.Last = nil
	wantDelivered := ci.AckFloor
	wantDelivered.Last = nil
	if nci.Delivered != wantDelivered {
		t.Fatalf("Delivered state does not match ack floor %+v vs %+v", nci.Delivered, wantDelivered)
	}
	nci.AckFloor.Last, ci.AckFloor.Last = nil, nil
	if nci.AckFloor != ci.AckFloor {
		t.Fatalf("Ack floors did not match %+v vs %+v", nci.AckFloor, ci.AckFloor)
	}

	ephInfo, err := js.ConsumerInfo("TEST", eph.Name)
	if err != nil {
		t.Fatalf("Unexpected error getting restored ephemeral consumer info: %v", err)
	}
	if ephInfo.Config.Durable != _EMPTY_ {
		t.Fatalf("Expected restored consumer to remain ephemeral, got durable %q", ephInfo.Config.Durable)
	}

	// Make sure consumer works.
	// It should pick up immediately after the ack floor and deliver everything
	// through the current stream tail.
	wantSeq := int(ci.AckFloor.Stream + 1)
	toFetch := toSend - int(ci.AckFloor.Stream)
	for _, m := range fetchMsgs(t, jsub, toFetch, 5*time.Second) {
		meta, err := m.Metadata()
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		if meta.Sequence.Stream != uint64(wantSeq) {
			t.Fatalf("Expected stream sequence of %d, but got %d", wantSeq, meta.Sequence.Stream)
		}
		require_NoError(t, m.AckSync())
		wantSeq++
	}

	// Now make sure the other server was properly caughtup.
	// Need to call this by hand for now.
	rmsg, err = nc.Request(fmt.Sprintf(JSApiStreamLeaderStepDownT, "TEST"), nil, time.Second)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	var sdResp JSApiStreamLeaderStepDownResponse
	if err := json.Unmarshal(rmsg.Data, &sdResp); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if sdResp.Error != nil {
		t.Fatalf("Unexpected error: %+v", sdResp.Error)
	}

	c.waitOnStreamLeader("$G", "TEST")
	si, err = js.StreamInfo("TEST")
	if err != nil {
		t.Fatalf("Unexpected error: %+v", err)
	}
	if si.State.Msgs != uint64(toSend) {
		t.Fatalf("Unexpected stream info: %+v", si)
	}

	// Check idle consumer
	c.waitOnConsumerLeader("$G", "TEST", "rip")

	// Now check for the consumer being recreated.
	if _, err := js.ConsumerInfo("TEST", "rip"); err != nil {
		t.Fatalf("Unexpected error: %+v", err)
	}
}

func TestJetStreamClusterUserSnapshotAndRestoreConfigChanges(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "R3S", 3)
	defer c.shutdown()

	// Client based API
	nc, js := jsClientConnect(t, c.randomServer())
	defer nc.Close()

	// FIXME(dlc) - Do case with R=1
	cfg := &nats.StreamConfig{
		Name:     "TEST",
		Subjects: []string{"foo"},
		Replicas: 2,
	}

	if _, err := js.AddStream(cfg); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	toSend := 10
	for i := 0; i < toSend; i++ {
		if _, err := js.Publish("foo", []byte("OK")); err != nil {
			t.Fatalf("Unexpected publish error: %v", err)
		}
	}

	getSnapshot := func() ([]byte, *StreamState) {
		t.Helper()
		sreq := &JSApiStreamSnapshotRequest{
			DeliverSubject: nats.NewInbox(),
			ChunkSize:      1024,
		}

		req, _ := json.Marshal(sreq)
		rmsg, err := nc.Request(fmt.Sprintf(JSApiStreamSnapshotT, "TEST"), req, time.Second)
		if err != nil {
			t.Fatalf("Unexpected error on snapshot request: %v", err)
		}

		var resp JSApiStreamSnapshotResponse
		json.Unmarshal(rmsg.Data, &resp)
		if resp.Error != nil {
			t.Fatalf("Did not get correct error response: %+v", resp.Error)
		}

		var snapshot []byte
		done := make(chan bool)

		sub, _ := nc.Subscribe(sreq.DeliverSubject, func(m *nats.Msg) {
			// EOF
			if len(m.Data) == 0 {
				done <- true
				return
			}
			// Could be writing to a file here too.
			snapshot = append(snapshot, m.Data...)
			// Flow ack
			m.Respond(nil)
		})
		defer sub.Unsubscribe()

		// Wait to receive the snapshot.
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			t.Fatalf("Did not receive our snapshot in time")
		}
		return snapshot, resp.State
	}

	restore := func(cfg *StreamConfig, state *StreamState, snap []byte) *nats.StreamInfo {
		rreq := &JSApiStreamRestoreRequest{
			Config: *cfg,
			State:  *state,
		}
		req, err := json.Marshal(rreq)
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		rmsg, err := nc.Request(fmt.Sprintf(JSApiStreamRestoreT, cfg.Name), req, 5*time.Second)
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		var rresp JSApiStreamRestoreResponse
		json.Unmarshal(rmsg.Data, &rresp)
		if rresp.Error != nil {
			t.Fatalf("Got an unexpected error response: %+v", rresp.Error)
		}
		if rresp.DeliverSubject == _EMPTY_ {
			t.Fatalf("No deliver subject set on response: %+v", rresp)
		}
		// Send our snapshot back in to restore the stream.
		// Can be any size message.
		var chunk [1024]byte
		for r := bytes.NewReader(snap); ; {
			n, err := r.Read(chunk[:])
			if err != nil {
				break
			}
			nc.Request(rresp.DeliverSubject, chunk[:n], time.Second)
		}
		rmsg, err = nc.Request(rresp.DeliverSubject, nil, 2*time.Second)
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		rresp.Error = nil
		json.Unmarshal(rmsg.Data, &rresp)
		if rresp.Error != nil {
			t.Fatalf("Got an unexpected error response: %+v", rresp.Error)
		}
		si, err := js.StreamInfo(cfg.Name)
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		return si
	}

	snap, state := getSnapshot()

	if err := js.DeleteStream("TEST"); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	// Now change subjects.
	ncfg := &StreamConfig{
		Name:     "TEST",
		Subjects: []string{"bar", "baz"},
		Storage:  FileStorage,
		Replicas: 2,
	}
	if si := restore(ncfg, state, snap); !reflect.DeepEqual(si.Config.Subjects, ncfg.Subjects) {
		t.Fatalf("Did not get expected stream info: %+v", si)
	}
	if err := js.DeleteStream("TEST"); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	// Storage
	ncfg.Storage = MemoryStorage
	if si := restore(ncfg, state, snap); !reflect.DeepEqual(si.Config.Subjects, ncfg.Subjects) {
		t.Fatalf("Did not get expected stream info: %+v", si)
	}
	if err := js.DeleteStream("TEST"); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	// Now replicas
	ncfg.Replicas = 3
	if si := restore(ncfg, state, snap); !reflect.DeepEqual(si.Config.Subjects, ncfg.Subjects) {
		t.Fatalf("Did not get expected stream info: %+v", si)
	}
}

func TestJetStreamClusterAccountInfoAndLimits(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "R5S", 5)
	defer c.shutdown()

	// Adjust our limits.
	c.updateLimits("$G", map[string]JetStreamAccountLimits{
		_EMPTY_: {
			MaxMemory:    1024,
			MaxStore:     8000,
			MaxStreams:   3,
			MaxConsumers: 2,
		},
	})

	// Client based API
	s := c.randomServer()
	nc, js := jsClientConnect(t, s)
	defer nc.Close()

	if _, err := js.AddStream(&nats.StreamConfig{Name: "foo", Replicas: 1}); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if _, err := js.AddStream(&nats.StreamConfig{Name: "bar", Replicas: 2}); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if _, err := js.AddStream(&nats.StreamConfig{Name: "baz", Replicas: 3}); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	// Create with same config is idempotent, and must not exceed max streams as it already exists.
	if _, err := js.AddStream(&nats.StreamConfig{Name: "baz", Replicas: 3}); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	sendBatch := func(subject string, n int) {
		t.Helper()
		for i := 0; i < n; i++ {
			if _, err := js.Publish(subject, []byte("JSC-OK")); err != nil {
				t.Fatalf("Unexpected publish error: %v", err)
			}
		}
	}

	sendBatch("foo", 25)
	sendBatch("bar", 75)
	sendBatch("baz", 10)

	accountStats := func() *nats.AccountInfo {
		t.Helper()

		info, err := js.AccountInfo()
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		return info
	}

	// If subject is not 3 letters or payload not 2 this needs to change.
	const msgSize = uint64(22 + 3 + 6 + 8)

	stats := accountStats()
	if stats.Streams != 3 {
		t.Fatalf("Should have been tracking 3 streams, found %d", stats.Streams)
	}
	expectedSize := 25*msgSize + 75*msgSize*2 + 10*msgSize*3
	// This may lag.
	checkFor(t, 5*time.Second, 500*time.Millisecond, func() error {
		if stats.Store != expectedSize {
			err := fmt.Errorf("Expected store size to be %d, got %+v\n", expectedSize, stats)
			stats = accountStats()
			return err

		}
		return nil
	})

	// Check limit enforcement.
	if _, err := js.AddStream(&nats.StreamConfig{Name: "fail", Replicas: 3}); err == nil {
		t.Fatalf("Expected an error but got none")
	}

	// We should be at 7995 at the moment with a limit of 8000, so any message will go over.
	if _, err := js.Publish("baz", []byte("JSC-NOT-OK")); err == nil {
		t.Fatalf("Expected publish error but got none")
	}

	// Check consumers
	_, err := js.AddConsumer("foo", &nats.ConsumerConfig{Durable: "dlc", AckPolicy: nats.AckExplicitPolicy})
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	// Create (with explicit create API) for the same consumer must be idempotent, and not trigger limit.
	obsReq := CreateConsumerRequest{
		Stream: "foo",
		Config: ConsumerConfig{Durable: "bar"},
		Action: ActionCreate,
	}
	req, err := json.Marshal(obsReq)
	require_NoError(t, err)

	msg, err := nc.Request(fmt.Sprintf(JSApiDurableCreateT, "foo", "bar"), req, time.Second)
	require_NoError(t, err)
	var resp JSApiConsumerInfoResponse
	require_NoError(t, json.Unmarshal(msg.Data, &resp))
	if resp.Error != nil {
		t.Fatalf("Unexpected error: %v", resp.Error)
	}

	msg, err = nc.Request(fmt.Sprintf(JSApiDurableCreateT, "foo", "bar"), req, time.Second)
	require_NoError(t, err)
	var resp2 JSApiConsumerInfoResponse
	require_NoError(t, json.Unmarshal(msg.Data, &resp2))
	if resp2.Error != nil {
		t.Fatalf("Unexpected error: %v", resp2.Error)
	}

	// This should fail.
	_, err = js.AddConsumer("foo", &nats.ConsumerConfig{Durable: "dlc22", AckPolicy: nats.AckExplicitPolicy})
	if err == nil {
		t.Fatalf("Expected error but got none")
	}
}

func TestJetStreamClusterMaxStreamsReached(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "R3S", 3)
	defer c.shutdown()

	nc, js := jsClientConnect(t, c.randomNonLeader())
	defer nc.Close()

	// Adjust our limits.
	c.updateLimits("$G", map[string]JetStreamAccountLimits{
		_EMPTY_: {
			MaxMemory:  1024,
			MaxStore:   1024,
			MaxStreams: 1,
		},
	})

	// Many stream creations in parallel for the same stream should not result in
	// maximum number of streams reached error. All should have a successful response.
	var wg sync.WaitGroup
	for i := 0; i < 15; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, err := js.AddStream(&nats.StreamConfig{
				Name:     "TEST",
				Subjects: []string{"foo"},
				Replicas: 1,
			})
			require_NoError(t, err)
		}()
	}
	wg.Wait()
	require_NoError(t, js.DeleteStream("TEST"))

	// Adjust our limits.
	c.updateLimits("$G", map[string]JetStreamAccountLimits{
		_EMPTY_: {
			MaxMemory:  1024,
			MaxStore:   1024,
			MaxStreams: 2,
		},
	})

	// Setup streams beforehand.
	for d := 0; d < 2; d++ {
		_, err := js.AddStream(&nats.StreamConfig{
			Name:     fmt.Sprintf("TEST-%d", d),
			Subjects: []string{fmt.Sprintf("foo.%d", d)},
			Replicas: 1,
		})
		require_NoError(t, err)
	}

	// Many stream creations in parallel for streams that already exist should not result in
	// maximum number of streams reached error. All should have a successful response.
	for i := 0; i < 15; i++ {
		wg.Add(1)
		d := i % 2
		go func() {
			defer wg.Done()
			_, err := js.AddStream(&nats.StreamConfig{
				Name:     fmt.Sprintf("TEST-%d", d),
				Subjects: []string{fmt.Sprintf("foo.%d", d)},
				Replicas: 1,
			})
			require_NoError(t, err)
		}()
	}
	wg.Wait()
}

func TestJetStreamClusterStreamLimits(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "R3S", 3)
	defer c.shutdown()

	// Client based API
	s := c.randomServer()
	nc, js := jsClientConnect(t, s)
	defer nc.Close()

	// Check that large R will fail.
	if _, err := js.AddStream(&nats.StreamConfig{Name: "foo", Replicas: 5}); err == nil {
		t.Fatalf("Expected error but got none")
	}

	maxMsgs := 5

	_, err := js.AddStream(&nats.StreamConfig{
		Name:       "foo",
		Replicas:   3,
		Retention:  nats.LimitsPolicy,
		Discard:    DiscardNew,
		MaxMsgSize: 11,
		MaxMsgs:    int64(maxMsgs),
		MaxAge:     250 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	// Large message should fail.
	if _, err := js.Publish("foo", []byte("0123456789ZZZ")); err == nil {
		t.Fatalf("Expected publish to fail")
	}

	for i := 0; i < maxMsgs; i++ {
		if _, err := js.Publish("foo", []byte("JSC-OK")); err != nil {
			t.Fatalf("Unexpected publish error: %v", err)
		}
	}

	// These should fail.
	if _, err := js.Publish("foo", []byte("JSC-OK")); err == nil {
		t.Fatalf("Expected publish to fail")
	}

	// Make sure when space frees up we can send more.
	checkFor(t, 2*time.Second, 100*time.Millisecond, func() error {
		si, err := js.StreamInfo("foo")
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		if si.State.Msgs != 0 {
			return fmt.Errorf("Expected 0 msgs, got state: %+v", si.State)
		}
		return nil
	})

	if _, err := js.Publish("foo", []byte("ROUND2")); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
}

func TestJetStreamClusterStreamInterestOnlyPolicy(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "R3S", 3)
	defer c.shutdown()

	// Client based API
	s := c.randomServer()
	nc, js := jsClientConnect(t, s)
	defer nc.Close()

	_, err := js.AddStream(&nats.StreamConfig{
		Name:      "foo",
		Replicas:  3,
		Retention: nats.InterestPolicy,
	})
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	toSend := 10

	// With no interest these should be no-ops.
	for i := 0; i < toSend; i++ {
		if _, err := js.Publish("foo", []byte("JSC-OK")); err != nil {
			t.Fatalf("Unexpected publish error: %v", err)
		}
	}

	si, err := js.StreamInfo("foo")
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if si.State.Msgs != 0 {
		t.Fatalf("Expected no messages with no interest, got %d", si.State.Msgs)
	}

	// Now create a consumer.
	sub, err := js.SubscribeSync("foo", nats.Durable("dlc"))
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	for i := 0; i < toSend; i++ {
		if _, err := js.Publish("foo", []byte("JSC-OK")); err != nil {
			t.Fatalf("Unexpected publish error: %v", err)
		}
	}
	checkSubsPending(t, sub, toSend)

	si, err = js.StreamInfo("foo")
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if si.State.Msgs != uint64(toSend) {
		t.Fatalf("Expected %d messages with interest, got %d", toSend, si.State.Msgs)
	}
	if si.State.FirstSeq != uint64(toSend+1) {
		t.Fatalf("Expected first sequence of %d, got %d", toSend+1, si.State.FirstSeq)
	}

	// Now delete the consumer.
	sub.Unsubscribe()
	// That should make it go away.
	if _, err := js.ConsumerInfo("foo", "dlc"); err == nil {
		t.Fatalf("Expected not found error, got none")
	}

	// Wait for the messages to be purged.
	checkFor(t, 5*time.Second, 20*time.Millisecond, func() error {
		si, err := js.StreamInfo("foo")
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		if si.State.Msgs == 0 {
			return nil
		}
		return fmt.Errorf("Wanted 0 messages, got %d", si.State.Msgs)
	})
}

func TestJetStreamClusterExtendedAccountInfo(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "R3S", 3)
	defer c.shutdown()

	// Client based API
	s := c.randomServer()
	nc, js := jsClientConnect(t, s)
	defer nc.Close()

	sendBatch := func(subject string, n int) {
		t.Helper()
		for i := 0; i < n; i++ {
			if _, err := js.Publish(subject, []byte("JSC-OK")); err != nil {
				t.Fatalf("Unexpected publish error: %v", err)
			}
		}
	}

	// Add in some streams with msgs and consumers.
	if _, err := js.AddStream(&nats.StreamConfig{Name: "TEST-1", Replicas: 2}); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if _, err := js.SubscribeSync("TEST-1"); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	sendBatch("TEST-1", 25)

	if _, err := js.AddStream(&nats.StreamConfig{Name: "TEST-2", Replicas: 2}); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if _, err := js.SubscribeSync("TEST-2"); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	sendBatch("TEST-2", 50)

	if _, err := js.AddStream(&nats.StreamConfig{Name: "TEST-3", Replicas: 3, Storage: nats.MemoryStorage}); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if _, err := js.SubscribeSync("TEST-3"); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	sendBatch("TEST-3", 100)

	// Go client will lag so use direct for now.
	getAccountInfo := func() *nats.AccountInfo {
		t.Helper()

		info, err := js.AccountInfo()
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		return info
	}

	checkFor(t, 5*time.Second, 250*time.Millisecond, func() error {
		ai := getAccountInfo()
		if ai.Streams != 3 || ai.Consumers != 3 {
			return fmt.Errorf("AccountInfo not correct: %+v", ai)
		}
		if ai.API.Total < 7 {
			return fmt.Errorf("Expected at least 7 total API calls, got %d", ai.API.Total)
		}
		return nil
	})

	// Now do a failure to make sure we track API errors.
	js.StreamInfo("NO-STREAM")
	js.ConsumerInfo("TEST-1", "NO-CONSUMER")
	js.ConsumerInfo("TEST-2", "NO-CONSUMER")
	js.ConsumerInfo("TEST-3", "NO-CONSUMER")

	checkFor(t, 5*time.Second, 250*time.Millisecond, func() error {
		ai := getAccountInfo()
		if ai.API.Errors != 4 {
			return fmt.Errorf("Expected 4 API calls to be errors, got %d", ai.API.Errors)
		}
		return nil
	})
}

func TestJetStreamClusterPeerRemovalAPI(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "R5S", 5)
	defer c.shutdown()

	// Client based API
	ml := c.leader()
	nc, err := nats.Connect(ml.ClientURL(), nats.UserInfo("admin", "s3cr3t!"))
	if err != nil {
		t.Fatalf("Failed to create system client: %v", err)
	}
	defer nc.Close()

	// Expect error if unknown peer
	req := &JSApiMetaServerRemoveRequest{Server: "S-9"}
	jsreq, err := json.Marshal(req)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	rmsg, err := nc.Request(JSApiRemoveServer, jsreq, time.Second)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	var resp JSApiMetaServerRemoveResponse
	if err := json.Unmarshal(rmsg.Data, &resp); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if resp.Error == nil {
		t.Fatalf("Expected an error, got none")
	}

	sub, err := nc.SubscribeSync(JSAdvisoryServerRemoved)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	rs := c.randomNonLeader()
	req = &JSApiMetaServerRemoveRequest{Server: rs.Name()}
	jsreq, err = json.Marshal(req)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	rmsg, err = nc.Request(JSApiRemoveServer, jsreq, time.Second)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	resp.Error = nil
	if err := json.Unmarshal(rmsg.Data, &resp); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if resp.Error != nil {
		t.Fatalf("Unexpected error: %+v", resp.Error)
	}
	c.waitOnLeader()
	ml = c.leader()

	checkSubsPending(t, sub, 1)
	madv, _ := sub.NextMsg(0)
	var adv JSServerRemovedAdvisory
	if err := json.Unmarshal(madv.Data, &adv); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if adv.Server != rs.Name() {
		t.Fatalf("Expected advisory about %s being removed, got %+v", rs.Name(), adv)
	}

	checkFor(t, 5*time.Second, 250*time.Millisecond, func() error {
		for _, s := range ml.JetStreamClusterPeers() {
			if s == rs.Name() {
				return fmt.Errorf("Still in the peer list")
			}
		}
		return nil
	})
}

func TestJetStreamClusterPeerRemovalAndStreamReassignment(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "R5S", 5)
	defer c.shutdown()

	// Client based API
	s := c.randomNonLeader()
	nc, js := jsClientConnect(t, s)
	defer nc.Close()

	si, err := js.AddStream(&nats.StreamConfig{
		Name:     "TEST",
		Subjects: []string{"foo", "bar"},
		Replicas: 3,
	})
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	// Admin based API
	ml := c.leader()
	nc, err = nats.Connect(ml.ClientURL(), nats.UserInfo("admin", "s3cr3t!"))
	if err != nil {
		t.Fatalf("Failed to create system client: %v", err)
	}
	defer nc.Close()

	// Select the non-leader server for the stream to remove.
	if len(si.Cluster.Replicas) < 2 {
		t.Fatalf("Not enough replicas found: %+v", si.Cluster)
	}
	toRemove, cl := si.Cluster.Replicas[0].Name, c.leader()
	if toRemove == cl.Name() {
		toRemove = si.Cluster.Replicas[1].Name
	}

	req := &JSApiMetaServerRemoveRequest{Server: toRemove}
	jsreq, err := json.Marshal(req)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	rmsg, err := nc.Request(JSApiRemoveServer, jsreq, time.Second)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	var resp JSApiMetaServerRemoveResponse
	if err := json.Unmarshal(rmsg.Data, &resp); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if resp.Error != nil {
		t.Fatalf("Unexpected error: %+v", resp.Error)
	}
	// In case that server was also meta-leader.
	c.waitOnLeader()

	checkFor(t, 10*time.Second, 250*time.Millisecond, func() error {
		for _, s := range ml.JetStreamClusterPeers() {
			if s == toRemove {
				return fmt.Errorf("Server still in the peer list")
			}
		}
		return nil
	})

	// Now wait until the stream is now current.
	checkFor(t, 10*time.Second, 100*time.Millisecond, func() error {
		si, err := js.StreamInfo("TEST", nats.MaxWait(time.Second))
		if err != nil {
			return fmt.Errorf("Could not fetch stream info: %v", err)
		}
		// We should not see the old server at all.
		for _, p := range si.Cluster.Replicas {
			if p.Name == toRemove {
				t.Fatalf("Peer not removed yet: %+v", toRemove)
			}
			if !p.Current {
				return fmt.Errorf("Expected replica to be current: %+v", p)
			}
		}
		if len(si.Cluster.Replicas) != 2 {
			return fmt.Errorf("Expected 2 replicas, got %d", len(si.Cluster.Replicas))
		}
		return nil
	})
}

func TestJetStreamClusterPeerRemovalAndStreamReassignmentWithoutSpace(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "R3S", 3)
	defer c.shutdown()

	// Client based API
	s := c.randomNonLeader()
	nc, js := jsClientConnect(t, s)
	defer nc.Close()

	si, err := js.AddStream(&nats.StreamConfig{
		Name:     "TEST",
		Subjects: []string{"foo", "bar"},
		Replicas: 3,
	})
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	// Admin based API
	ml := c.leader()
	nc, err = nats.Connect(ml.ClientURL(), nats.UserInfo("admin", "s3cr3t!"))
	if err != nil {
		t.Fatalf("Failed to create system client: %v", err)
	}
	defer nc.Close()

	// Select the non-leader server for the stream to remove.
	if len(si.Cluster.Replicas) < 2 {
		t.Fatalf("Not enough replicas found: %+v", si.Cluster)
	}
	toRemove, cl := si.Cluster.Replicas[0].Name, c.leader()
	if toRemove == cl.Name() {
		toRemove = si.Cluster.Replicas[1].Name
	}

	req := &JSApiMetaServerRemoveRequest{Server: toRemove}
	jsreq, err := json.Marshal(req)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	rmsg, err := nc.Request(JSApiRemoveServer, jsreq, 2*time.Second)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	var resp JSApiMetaServerRemoveResponse
	if err := json.Unmarshal(rmsg.Data, &resp); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if resp.Error != nil {
		t.Fatalf("Unexpected error: %+v", resp.Error)
	}
	checkFor(t, 10*time.Second, 250*time.Millisecond, func() error {
		for _, s := range ml.JetStreamClusterPeers() {
			if s == toRemove {
				return fmt.Errorf("Server still in the peer list")
			}
		}
		return nil
	})
	// Make sure only 2 peers at this point.
	c.waitOnPeerCount(2)

	// Now wait until the stream is now current.
	streamCurrent := func(nr int) {
		checkFor(t, 10*time.Second, 100*time.Millisecond, func() error {
			si, err := js.StreamInfo("TEST", nats.MaxWait(time.Second))
			if err != nil {
				return fmt.Errorf("Could not fetch stream info: %v", err)
			}
			// We should not see the old server at all.
			for _, p := range si.Cluster.Replicas {
				if p.Name == toRemove {
					return fmt.Errorf("Peer not removed yet: %+v", toRemove)
				}
				if !p.Current {
					return fmt.Errorf("Expected replica to be current: %+v", p)
				}
			}
			if len(si.Cluster.Replicas) != nr {
				return fmt.Errorf("Expected %d replicas, got %d", nr, len(si.Cluster.Replicas))
			}
			return nil
		})
	}

	// Make sure the peer was removed from the stream and that we did not fill the new spot.
	streamCurrent(1)

	// Now add in a new server and make sure it gets added to our stream.
	c.addInNewServer()
	c.waitOnPeerCount(3)

	streamCurrent(2)
}

// Evacuating a server out of an R3 stream can leave it with two peers, needing a new one
// admitted. That's only done by whoever is meta leader at the time, so if there's none then
// (or it can't see the peer yet) the stream stays under-replicated. Becoming meta leader
// must reconcile it, without handing the stream back to the evacuated server.
func TestJetStreamClusterPeerRemovalAndStreamReassignmentOnMetaLeaderChange(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "R4S", 4)
	defer c.shutdown()

	nc, js := jsClientConnect(t, c.randomNonLeader())
	defer nc.Close()

	// The server scoped endpoint is system account only.
	snc, _ := jsClientConnect(t, c.randomServer(), nats.UserInfo("admin", "s3cr3t!"))
	defer snc.Close()

	_, err := js.AddStream(&nats.StreamConfig{
		Name:     "TEST",
		Subjects: []string{"foo"},
		Replicas: 3,
	})
	require_NoError(t, err)
	_, err = js.AddConsumer("TEST", &nats.ConsumerConfig{
		Durable:   "dur",
		AckPolicy: nats.AckExplicitPolicy,
	})
	require_NoError(t, err)

	ml := c.leader()
	peers := metaStreamPeers(ml, globalAccountName, "TEST")
	require_Len(t, len(peers), 3)

	// The one server not hosting the stream, the only peer that can be admitted once the
	// stream is left with missing peers. And a stream member we can peer-remove, must not
	// be the meta leader so it stays able to propose.
	var spare, member *Server
	for _, s := range c.servers {
		if !slices.Contains(peers, s.Node()) {
			spare = s
		} else if s != ml {
			member = s
		}
	}
	require_NotNil(t, spare)
	require_NotNil(t, member)

	// The spare must stay invisible to the meta leader until we say otherwise, so stop its
	// statsz. Receiving one would both make it visible again and reconcile.
	spare.mu.Lock()
	if spare.sys != nil {
		clearTimer(&spare.sys.stmr)
	}
	spare.mu.Unlock()

	// Make the meta leader unaware of the spare server, as happens for real when a server
	// just joined the meta group but its statsz hasn't arrived yet. Peer selection can't
	// pick it then, and nothing retries.
	sir, ok := ml.nodeToInfo.Load(spare.Node())
	require_True(t, ok)
	si := sir.(nodeInfo)
	ml.nodeToInfo.Delete(spare.Node())

	// Evacuate one of the stream's peers, no replacement can be selected for it. The
	// server is going away, so this is applied even though the stream is left short.
	jsreq, err := json.Marshal(&JSApiMetaServerEvacuateRequest{Peer: member.Node()})
	require_NoError(t, err)
	rmsg, err := snc.Request(JSApiEvacuateServer, jsreq, 5*time.Second)
	require_NoError(t, err)
	var resp JSApiMetaServerEvacuateResponse
	require_NoError(t, json.Unmarshal(rmsg.Data, &resp))
	require_True(t, resp.Success)

	// The stream, and its consumer, are now left with just two peers.
	checkFor(t, 5*time.Second, 200*time.Millisecond, func() error {
		if sp := metaStreamPeers(ml, globalAccountName, "TEST"); len(sp) != 2 {
			return fmt.Errorf("expected 2 stream peers, got: %v", sp)
		}
		if cp := metaConsumerPeers(ml, globalAccountName, "TEST", "dur"); len(cp) != 2 {
			return fmt.Errorf("expected 2 consumer peers, got: %v", cp)
		}
		return nil
	})

	// The spare server becomes visible to the meta leader again.
	ml.nodeToInfo.Store(spare.Node(), si)

	// Becoming meta leader must reconcile the assignments and admit a new peer.
	require_NoError(t, ml.getJetStream().getMetaGroup().StepDown())

	checkFor(t, 5*time.Second, 200*time.Millisecond, func() error {
		nml := c.leader()
		if nml == nil {
			return errors.New("no meta leader")
		}
		sp := metaStreamPeers(nml, globalAccountName, "TEST")
		if len(sp) != 3 {
			return fmt.Errorf("expected 3 stream peers, got: %v", sp)
		}
		// The spare must have been admitted, not the peer we just evacuated.
		if slices.Contains(sp, member.Node()) {
			return fmt.Errorf("evacuated peer %q back in stream peers: %v", member.Node(), sp)
		}
		if !slices.Contains(sp, spare.Node()) {
			return fmt.Errorf("spare peer %q not in stream peers: %v", spare.Node(), sp)
		}
		cp := metaConsumerPeers(nml, globalAccountName, "TEST", "dur")
		if len(cp) != 3 {
			return fmt.Errorf("expected 3 consumer peers, got: %v", cp)
		}
		if slices.Contains(cp, member.Node()) {
			return fmt.Errorf("evacuated peer %q back in consumer peers: %v", member.Node(), cp)
		}
		for _, p := range cp {
			if !slices.Contains(sp, p) {
				return fmt.Errorf("consumer peers %v not hosted by stream peers %v", cp, sp)
			}
		}
		return nil
	})
}

// A stream left with missing peers, because there was nowhere to place a replacement, must
// be reconciled once a server that can host one joins, without needing a meta leader change.
// The peer that was deliberately evacuated must stay out of it.
func TestJetStreamClusterPeerRemovalAndStreamReassignmentOnNewServer(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "R3S", 3)
	defer c.shutdown()

	nc, js := jsClientConnect(t, c.randomNonLeader())
	defer nc.Close()

	// The server scoped endpoint is system account only.
	snc, _ := jsClientConnect(t, c.randomServer(), nats.UserInfo("admin", "s3cr3t!"))
	defer snc.Close()

	_, err := js.AddStream(&nats.StreamConfig{
		Name:     "TEST",
		Subjects: []string{"foo"},
		Replicas: 3,
	})
	require_NoError(t, err)
	_, err = js.AddConsumer("TEST", &nats.ConsumerConfig{
		Durable:   "dur",
		AckPolicy: nats.AckExplicitPolicy,
	})
	require_NoError(t, err)

	// Evacuate one of the stream's peers, must not be the meta leader so it stays able to
	// propose. Every server hosts the stream, so no replacement can be selected. The server
	// is going away, so this is applied even though the stream is left short.
	ml := c.leader()
	var member *Server
	for _, s := range c.servers {
		if s != ml {
			member = s
			break
		}
	}
	require_NotNil(t, member)

	jsreq, err := json.Marshal(&JSApiMetaServerEvacuateRequest{Peer: member.Node()})
	require_NoError(t, err)
	rmsg, err := snc.Request(JSApiEvacuateServer, jsreq, 5*time.Second)
	require_NoError(t, err)
	var resp JSApiMetaServerEvacuateResponse
	require_NoError(t, json.Unmarshal(rmsg.Data, &resp))
	require_True(t, resp.Success)

	// The stream, and its consumer, are now left with just two peers.
	checkFor(t, 5*time.Second, 200*time.Millisecond, func() error {
		if sp := metaStreamPeers(ml, globalAccountName, "TEST"); len(sp) != 2 {
			return fmt.Errorf("expected 2 stream peers, got: %v", sp)
		}
		if cp := metaConsumerPeers(ml, globalAccountName, "TEST", "dur"); len(cp) != 2 {
			return fmt.Errorf("expected 2 consumer peers, got: %v", cp)
		}
		return nil
	})

	// Adding a server must reconcile the assignments and admit it as the new peer. The peer
	// we removed is eligible again for placement, but was taken out deliberately so it must
	// not be handed the stream back.
	ns := c.addInNewServer()
	c.waitOnPeerCount(4)

	checkFor(t, 10*time.Second, 200*time.Millisecond, func() error {
		sp := metaStreamPeers(ml, globalAccountName, "TEST")
		if len(sp) != 3 {
			return fmt.Errorf("expected 3 stream peers, got: %v", sp)
		}
		if !slices.Contains(sp, ns.Node()) {
			return fmt.Errorf("expected new server in stream peers, got: %v", sp)
		}
		if slices.Contains(sp, member.Node()) {
			return fmt.Errorf("removed peer back in stream peers: %v", sp)
		}
		cp := metaConsumerPeers(ml, globalAccountName, "TEST", "dur")
		if len(cp) != 3 {
			return fmt.Errorf("expected 3 consumer peers, got: %v", cp)
		}
		for _, p := range cp {
			if !slices.Contains(sp, p) {
				return fmt.Errorf("consumer peers %v not hosted by stream peers %v", cp, sp)
			}
		}
		return nil
	})

	// All without the meta leader having changed.
	require_True(t, c.leader() == ml)

	// The exclusion only exists to keep placement off the evacuated peer while the group
	// is short. Now that it's healed it must be gone, the peer is not fenced off forever.
	sjs := ml.getJetStream()
	sjs.mu.RLock()
	defer sjs.mu.RUnlock()
	sa := sjs.streamAssignmentOrInflight(globalAccountName, "TEST")
	require_NotNil(t, sa)
	require_Len(t, len(sa.Group.Excluded), 0)
}

// The meta leader can change halfway through proposing the stream and consumer updates that
// follow a peer removal. The stream update lands, the consumer updates don't, leaving the
// consumer pointing at a peer that's gone. Becoming meta leader must reconcile it.
func TestJetStreamClusterPeerRemovalAndConsumerReassignmentOnMetaLeaderChange(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "R5S", 5)
	defer c.shutdown()

	nc, js := jsClientConnect(t, c.randomNonLeader())
	defer nc.Close()

	_, err := js.AddStream(&nats.StreamConfig{
		Name:     "TEST",
		Subjects: []string{"foo"},
		Replicas: 3,
	})
	require_NoError(t, err)
	_, err = js.AddConsumer("TEST", &nats.ConsumerConfig{
		Durable:   "dur",
		AckPolicy: nats.AckExplicitPolicy,
	})
	require_NoError(t, err)

	ml := c.leader()
	peers := metaStreamPeers(ml, globalAccountName, "TEST")
	require_Len(t, len(peers), 3)

	// Pick a server that neither hosts the stream nor is the meta leader, and remove it from
	// the meta group. The stream and its consumer are unaffected by that removal.
	var gone *Server
	for _, s := range c.servers {
		if s != ml && !slices.Contains(peers, s.Node()) {
			gone = s
			break
		}
	}
	require_NotNil(t, gone)
	goneID := gone.Node()

	snc, err := nats.Connect(ml.ClientURL(), nats.UserInfo("admin", "s3cr3t!"))
	require_NoError(t, err)
	defer snc.Close()

	jsreq, err := json.Marshal(&JSApiMetaServerRemoveRequest{Server: gone.Name()})
	require_NoError(t, err)
	rmsg, err := snc.Request(JSApiRemoveServer, jsreq, 5*time.Second)
	require_NoError(t, err)
	var resp JSApiMetaServerRemoveResponse
	require_NoError(t, json.Unmarshal(rmsg.Data, &resp))
	require_True(t, resp.Error == nil)
	c.waitOnPeerCount(4)

	// Simulate the meta leader having proposed the stream update but losing leadership before
	// the consumer updates: the consumer is left referencing the removed peer.
	mjs := ml.getJetStream()
	mjs.mu.Lock()
	cc := mjs.cluster
	oca := mjs.consumerAssignmentOrInflight(globalAccountName, "TEST", "dur")
	cca := oca.copyGroup()
	cca.Group.Peers = append(copyStrings(peers[:2]), goneID)
	err = cc.meta.Propose(cc.term, encodeAddConsumerAssignment(cca))
	mjs.mu.Unlock()
	require_NoError(t, err)

	// Nothing re-triggers reconciling the consumer, it stays pointing at the removed peer.
	checkFor(t, 5*time.Second, 200*time.Millisecond, func() error {
		if cp := metaConsumerPeers(ml, globalAccountName, "TEST", "dur"); !slices.Contains(cp, goneID) {
			return fmt.Errorf("consumer peers %v no longer contain %q", cp, goneID)
		}
		return nil
	})

	// Becoming meta leader must reconcile the consumer back onto the stream's peer set.
	require_NoError(t, mjs.getMetaGroup().StepDown())

	checkFor(t, 5*time.Second, 200*time.Millisecond, func() error {
		nml := c.leader()
		if nml == nil {
			return errors.New("no meta leader")
		}
		sp := metaStreamPeers(nml, globalAccountName, "TEST")
		if len(sp) != 3 {
			return fmt.Errorf("expected 3 stream peers, got: %v", sp)
		}
		cp := metaConsumerPeers(nml, globalAccountName, "TEST", "dur")
		if len(cp) != 3 {
			return fmt.Errorf("expected 3 consumer peers, got: %v", cp)
		}
		if slices.Contains(cp, goneID) {
			return fmt.Errorf("consumer peers %v still contain removed peer %q", cp, goneID)
		}
		for _, p := range cp {
			if !slices.Contains(sp, p) {
				return fmt.Errorf("consumer peers %v not hosted by stream peers %v", cp, sp)
			}
		}
		return nil
	})
}

// An under-replicated stream or consumer, one holding fewer peers than its configured
// replica count, is not migrating. There are no peers to move off of, and the meta leader
// heals it by adding peers back rather than through migration. Reporting it as migrating
// leaves the migration monitor running against a group that cannot converge, which registers
// desired state and clears it again every cycle, churning assignment proposals through the
// meta log forever.
func TestJetStreamClusterUnderReplicatedNotMigrating(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "R3S", 3)
	defer c.shutdown()

	nc, js := jsClientConnect(t, c.randomServer())
	defer nc.Close()

	_, err := js.AddStream(&nats.StreamConfig{Name: "TEST", Subjects: []string{"foo"}, Replicas: 3})
	require_NoError(t, err)

	_, err = js.AddConsumer("TEST", &nats.ConsumerConfig{Durable: "dur", AckPolicy: nats.AckExplicitPolicy})
	require_NoError(t, err)

	// Strip a stream member out of the meta group, on a cluster with no spare to replace it.
	ml := c.leader()
	require_NotNil(t, ml)
	peers := metaStreamPeers(ml, globalAccountName, "TEST")
	require_Len(t, len(peers), 3)

	// The member to remove must not be the meta leader, so the leader stays able to propose.
	var gone, kept *Server
	for _, s := range c.servers {
		if s == ml || !slices.Contains(peers, s.Node()) {
			continue
		}
		if gone == nil {
			gone = s
		} else {
			kept = s
		}
	}
	if kept == nil {
		kept = ml
	}
	require_NotNil(t, gone)

	snc, err := nats.Connect(ml.ClientURL(), nats.UserInfo("admin", "s3cr3t!"))
	require_NoError(t, err)
	defer snc.Close()

	jsreq, err := json.Marshal(&JSApiMetaServerRemoveRequest{Server: gone.Name()})
	require_NoError(t, err)
	rmsg, err := snc.Request(JSApiRemoveServer, jsreq, 5*time.Second)
	require_NoError(t, err)
	var resp JSApiMetaServerRemoveResponse
	require_NoError(t, json.Unmarshal(rmsg.Data, &resp))
	require_True(t, resp.Error == nil)
	c.waitOnPeerCount(2)

	// There is no spare left to take the removed peer's place, so the stream settles below its
	// replica count and stays there. The consumer follows it down to 2 peers, while both still
	// resolve to 3 replicas.
	checkFor(t, 10*time.Second, 100*time.Millisecond, func() error {
		l := c.leader()
		if l == nil {
			return errors.New("no meta leader")
		}
		if p := metaStreamPeers(l, globalAccountName, "TEST"); len(p) != 2 {
			return fmt.Errorf("expected stream on 2 peers, got %d", len(p))
		}
		if p := metaConsumerPeers(l, globalAccountName, "TEST", "dur"); len(p) != 2 {
			return fmt.Errorf("expected consumer on 2 peers, got %d", len(p))
		}
		return nil
	})

	mset, err := kept.globalAccount().lookupStream("TEST")
	require_NoError(t, err)

	// Both must settle out of migrating, rather than staying there because they're short a peer.
	var o *consumer
	checkFor(t, 10*time.Second, 100*time.Millisecond, func() error {
		if mset.isMigrating() {
			return errors.New("stream still reports migrating while under-replicated")
		}
		if o = mset.lookupConsumer("dur"); o == nil {
			return errors.New("consumer not found")
		}
		if o.isMigrating() {
			return errors.New("consumer still reports migrating while under-replicated")
		}
		return nil
	})

	// And must stay settled, no desired state churn on either assignment.
	for range 200 {
		require_False(t, mset.isMigrating())
		require_False(t, o.isMigrating())
		l := c.leader()
		require_NotNil(t, l)
		ljs := l.getJetStream()
		ljs.mu.RLock()
		sa := ljs.streamAssignmentOrInflight(globalAccountName, "TEST")
		ca := ljs.consumerAssignmentOrInflight(globalAccountName, "TEST", "dur")
		streamChurning := sa != nil && sa.Group != nil && sa.Group.Desired != nil
		consumerChurning := ca != nil && ca.Group != nil && ca.Group.Desired != nil
		ljs.mu.RUnlock()
		if streamChurning {
			t.Fatalf("Desired state re-registered for an under-replicated stream, migration monitor is churning")
		}
		if consumerChurning {
			t.Fatalf("Desired state re-registered for an under-replicated consumer, migration monitor is churning")
		}
		time.Sleep(5 * time.Millisecond)
	}
}

func TestJetStreamClusterReconcileOnlyForNewMetaPeers(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "R3S", 3)
	defer c.shutdown()

	ml := c.leader()
	js := ml.getJetStream()
	require_NotNil(t, js)

	// Grab a peer we've already got STATSZ for, so it's selectable for placement.
	var selectable string
	checkFor(t, 2*time.Second, 100*time.Millisecond, func() error {
		js.mu.RLock()
		cc := js.cluster
		js.mu.RUnlock()
		if cc == nil || cc.meta == nil {
			return errors.New("no meta group yet")
		}
		for _, p := range cc.meta.Peers() {
			if ml.peerSelectable(p.ID) {
				selectable = p.ID
				return nil
			}
		}
		return errors.New("no selectable peer yet")
	})

	reset := func() {
		t.Helper()
		checkFor(t, 2*time.Second, 10*time.Millisecond, func() error {
			js.prMu.Lock()
			defer js.prMu.Unlock()
			if js.prRunning {
				return errors.New("peer reconcile still in flight")
			}
			js.prPeers, js.prNewPeers = nil, nil
			return nil
		})
	}
	signaled := func() bool {
		js.prMu.Lock()
		defer js.prMu.Unlock()
		return js.prRunning || len(js.prPeers) > 0
	}
	tracked := func(peer string) bool {
		js.prMu.Lock()
		defer js.prMu.Unlock()
		_, ok := js.prNewPeers[peer]
		return ok
	}
	// Holds the JS lock, so a signaled sweep parks before doing any work, which keeps
	// the signal itself observable.
	locked := func(f func()) {
		js.mu.Lock()
		defer js.mu.Unlock()
		f()
	}

	// Leaving the meta group before ever reporting in drops the tracking.
	reset()
	js.processAddPeer("PEER-GONE")
	require_True(t, tracked("PEER-GONE"))
	js.processRemovePeer("PEER-GONE")
	require_False(t, tracked("PEER-GONE"))

	// STATSZ for a node that was not just added to the meta group must not reconcile.
	reset()
	locked(func() {
		js.processPeerStatsz("PEER-NOT-ADDED")
		require_False(t, signaled())
		// Not even for one we already know all about.
		js.processPeerStatsz(selectable)
		require_False(t, signaled())
	})

	// A new peer we can't place on yet is held until its first STATSZ.
	reset()
	locked(func() {
		js.processAddPeer("PEER-NEW")
		require_True(t, tracked("PEER-NEW"))
		require_False(t, signaled())

		// Which is what reconciles it.
		js.processPeerStatsz("PEER-NEW")
		require_False(t, tracked("PEER-NEW"))
		require_True(t, signaled())
	})

	// Only that first one, it's not a newly added peer anymore.
	reset()
	locked(func() {
		js.processPeerStatsz("PEER-NEW")
		require_False(t, signaled())
	})

	// A peer we already have STATSZ for reconciles as soon as it joins, there is no
	// transition to selectable left to wait for. Also covers STATSZ racing ahead of
	// the peer being added to the meta group.
	reset()
	locked(func() {
		js.processAddPeer(selectable)
		require_False(t, tracked(selectable))
		require_True(t, signaled())
	})

	// Every peer has reported in, so becoming leader leaves nothing to wait on.
	reset()
	locked(func() { js.seedNewPeers(true) })
	require_False(t, tracked(selectable))

	// A peer we have no STATSZ for, as if it had joined the meta group while we were
	// still a follower and never got tracked. Seeding on leader change is what lets
	// its first STATSZ reconcile the assignments the sweep could not place on it. A
	// live peer keeps reporting in, so retry until we seed while it's unselectable.
	si, ok := ml.nodeToInfo.Load(selectable)
	require_True(t, ok)
	defer ml.nodeToInfo.Store(selectable, si)
	checkFor(t, 5*time.Second, 50*time.Millisecond, func() error {
		ml.nodeToInfo.Store(selectable, nodeInfo{})
		locked(func() { js.seedNewPeers(true) })
		if !tracked(selectable) {
			return fmt.Errorf("unselectable peer %q not tracked", selectable)
		}
		return nil
	})
	locked(func() {
		js.processPeerStatsz(selectable)
		require_False(t, tracked(selectable))
		require_True(t, signaled())
	})

	// Losing leadership drops what we were waiting on, it's not ours to reconcile.
	reset()
	locked(func() {
		js.processAddPeer("PEER-NEW")
		require_True(t, tracked("PEER-NEW"))
		js.seedNewPeers(false)
		require_False(t, tracked("PEER-NEW"))
	})
}

func TestJetStreamClusterPeerRemovalAndServerBroughtBack(t *testing.T) {
	// Speed up for this test
	peerRemoveTimeout = 2 * time.Second
	defer func() {
		peerRemoveTimeout = peerRemoveTimeoutDefault
	}()

	c := createJetStreamClusterExplicit(t, "R5S", 5)
	defer c.shutdown()

	// Client based API
	ml := c.leader()
	nc, err := nats.Connect(ml.ClientURL(), nats.UserInfo("admin", "s3cr3t!"))
	if err != nil {
		t.Fatalf("Failed to create system client: %v", err)
	}
	defer nc.Close()

	getPeersCount := func() int {
		js := ml.getJetStream()
		if js == nil {
			return 0
		}
		js.mu.RLock()
		defer js.mu.RUnlock()

		cc := js.cluster
		if !cc.isLeader() || cc.meta == nil {
			return 0
		}
		return len(cc.meta.Peers())
	}

	checkFor(t, 2*time.Second, 250*time.Millisecond, func() error {
		if l := getPeersCount(); l != 5 {
			return fmt.Errorf("expected 5 peers, got %d", l)
		}
		return nil
	})

	// Shutdown server first.
	rs := c.randomNonLeader()
	rs.Shutdown()

	// Peers should still remain the same, even if one server is shut down.
	checkFor(t, 2*time.Second, 250*time.Millisecond, func() error {
		if l := getPeersCount(); l != 5 {
			return fmt.Errorf("expected 5 peers, got %d", l)
		}
		return nil
	})

	// Peer-remove after shutdown.
	req := &JSApiMetaServerRemoveRequest{Server: rs.Name()}
	jsreq, err := json.Marshal(req)
	require_NoError(t, err)
	rmsg, err := nc.Request(JSApiRemoveServer, jsreq, time.Second)
	require_NoError(t, err)

	var resp JSApiMetaServerRemoveResponse
	require_NoError(t, json.Unmarshal(rmsg.Data, &resp))
	if resp.Error != nil {
		t.Fatalf("Unexpected error: %+v", resp.Error)
	}

	// Peer should be removed.
	checkFor(t, 2*time.Second, 250*time.Millisecond, func() error {
		if l := getPeersCount(); l != 4 {
			return fmt.Errorf("expected 4 peers, got %d", l)
		}
		return nil
	})

	// Bringing back the server should re-add to peers after peer-remove timeout.
	c.restartServer(rs)
	checkFor(t, 5*time.Second, 250*time.Millisecond, func() error {
		if l := getPeersCount(); l != 5 {
			return fmt.Errorf("expected 5 peers, got %d", l)
		}
		return nil
	})
}

func TestJetStreamClusterPeerRemoveAndEvacuateMatrix(t *testing.T) {
	const clusterSize = 3
	for _, test := range []struct {
		name        string
		evacuate    bool
		serverScope bool
		replicas    int
	}{
		{"Remove/Stream/R1", false, false, 1},
		{"Remove/Stream/R3", false, false, 3},
		{"Remove/Server/R1", false, true, 1},
		{"Remove/Server/R3", false, true, 3},
		{"Evacuate/Stream/R1", true, false, 1},
		{"Evacuate/Stream/R3", true, false, 3},
		{"Evacuate/Server/R1", true, true, 1},
		{"Evacuate/Server/R3", true, true, 3},
	} {
		t.Run(test.name, func(t *testing.T) {
			// A peer can only be replaced if there is a server outside of the
			// stream's peer set to hand it to.
			replaceable := test.replicas < clusterSize

			// The stream scoped endpoint rejects the request when there is nowhere to
			// move the stream to, leaving the peer set untouched. The server scoped
			// endpoint is best effort and only reports on the peer itself, so it
			// always succeeds.
			opSuccess := test.serverScope || replaceable

			// Removing the only peer of an R1 stream hands it to an empty
			// replacement, discarding the data. An evacuation migrates the data
			// across first.
			retainsData := test.evacuate || test.replicas > 1

			// A server remove takes the peer out of the meta group as well, an
			// evacuation leaves it in.
			metaPeersAfterOp := clusterSize
			if test.serverScope && !test.evacuate {
				metaPeersAfterOp--
			}

			// A server scoped op without a replacement leaves the stream one peer short
			// until a new server shows up to backfill it. A rejected stream scoped op
			// leaves the peer set exactly as it was. Either way the stream ends on its
			// configured replica count once a new server is added.
			peersAfterOp := test.replicas
			if opSuccess && !replaceable {
				peersAfterOp--
			}

			c := createJetStreamClusterExplicit(t, "R3S", clusterSize)
			defer c.shutdown()

			nc, js := jsClientConnect(t, c.randomServer())
			defer nc.Close()

			// The meta stepdown and server scoped endpoints are system account only.
			snc, _ := jsClientConnect(t, c.randomServer(), nats.UserInfo("admin", "s3cr3t!"))
			defer snc.Close()

			_, err := js.AddStream(&nats.StreamConfig{
				Name:     "TEST",
				Subjects: []string{"foo"},
				Replicas: test.replicas,
			})
			require_NoError(t, err)

			toSend := 10
			for range toSend {
				_, err = js.Publish("foo", nil)
				require_NoError(t, err)
			}

			// Target a stream peer that is not the meta leader, so the meta layer
			// stays put for the duration of the operation.
			rs := c.randomNonStreamLeader(globalAccountName, "TEST")
			if rs == nil {
				rs = c.streamLeader(globalAccountName, "TEST")
			}
			require_NotNil(t, rs)
			target := rs.Node()

			ml := c.leader()
			if ml == rs {
				require_NoError(t, ml.getJetStream().getMetaGroup().StepDown())
				c.waitOnLeader()
				ml = c.leader()
			}
			require_NotNil(t, ml)
			require_NotEqual(t, ml, rs)

			// Wait for the stream to settle on the expected number of peers. The targeted
			// peer must be gone if the op was applied, and still there if it was rejected.
			checkPeers := func(expected int, containsTarget bool) {
				t.Helper()
				checkFor(t, 30*time.Second, 250*time.Millisecond, func() error {
					sjs := ml.getJetStream()
					sjs.mu.RLock()
					defer sjs.mu.RUnlock()
					sa := sjs.streamAssignment(globalAccountName, "TEST")
					if sa == nil || sa.Group == nil {
						return fmt.Errorf("stream not found")
					}
					if sa.Group.Desired != nil {
						return fmt.Errorf("stream still converging")
					}
					if len(sa.Group.Peers) != expected {
						return fmt.Errorf("expected %d peers, got %d", expected, len(sa.Group.Peers))
					}
					if got := slices.Contains(sa.Group.Peers, target); got != containsTarget {
						return fmt.Errorf("peer %q in peer set = %v, want %v (peers %v)", target, got, containsTarget, sa.Group.Peers)
					}
					return nil
				})
			}

			if test.serverScope {
				subj := JSApiRemoveServer
				if test.evacuate {
					subj = JSApiEvacuateServer
				}
				b, err := json.Marshal(JSApiMetaServerRemoveRequest{Peer: target})
				require_NoError(t, err)
				msg, err := snc.Request(subj, b, 10*time.Second)
				require_NoError(t, err)
				var resp JSApiMetaServerRemoveResponse
				require_NoError(t, json.Unmarshal(msg.Data, &resp))
				require_Equal(t, resp.Success, opSuccess)
			} else {
				subjT := JSApiStreamRemovePeerT
				if test.evacuate {
					subjT = JSApiStreamEvacuatePeerT
				}
				b, err := json.Marshal(JSApiStreamRemovePeerRequest{Peer: target})
				require_NoError(t, err)
				msg, err := nc.Request(fmt.Sprintf(subjT, "TEST"), b, 10*time.Second)
				require_NoError(t, err)
				var resp JSApiStreamRemovePeerResponse
				require_NoError(t, json.Unmarshal(msg.Data, &resp))
				require_Equal(t, resp.Success, opSuccess)
				if !opSuccess {
					// There was nowhere to move the peer to, so the request is rejected
					// and the stream is left alone.
					require_Error(t, resp.Error, NewJSPeerRemapError())
				}
			}

			// A server remove also takes the peer out of the meta group.
			c.waitOnPeerCount(metaPeersAfterOp)
			checkPeers(peersAfterOp, !opSuccess)

			// A peer remove hands an R1 stream to an empty replacement, an evacuation
			// migrates its data across first.
			expectedMsgs := uint64(toSend)
			if !retainsData {
				expectedMsgs = 0
			}
			si, err := js.StreamInfo("TEST")
			require_NoError(t, err)
			require_Equal(t, si.State.Msgs, expectedMsgs)

			// Whatever happened, the stream must still be writable.
			_, err = js.Publish("foo", []byte("HELLO"))
			require_NoError(t, err)
			si, err = js.StreamInfo("TEST")
			require_NoError(t, err)
			require_Equal(t, si.State.Msgs, expectedMsgs+1)

			// Now add capacity back. A stream that is short of its replica count must
			// take the new server on, one that is already at its replica count must be
			// left alone.
			ns := c.addInNewServer()
			c.waitOnPeerCount(metaPeersAfterOp + 1)
			checkPeers(test.replicas, !opSuccess)

			// The new server must have been taken on by a stream that was short of its
			// replica count, and must have been left alone by one that was not.
			checkFor(t, 2*time.Second, 250*time.Millisecond, func() error {
				sjs := ml.getJetStream()
				sjs.mu.RLock()
				defer sjs.mu.RUnlock()
				sa := sjs.streamAssignment(globalAccountName, "TEST")
				if sa == nil || sa.Group == nil {
					return fmt.Errorf("stream not found")
				}
				got, want := slices.Contains(sa.Group.Peers, ns.Node()), peersAfterOp < test.replicas
				if got != want {
					return fmt.Errorf("new server in peer set = %v, want %v (peers %v)", got, want, sa.Group.Peers)
				}
				return nil
			})

			si, err = js.StreamInfo("TEST")
			require_NoError(t, err)
			require_Equal(t, si.State.Msgs, expectedMsgs+1)
		})
	}
}

func TestJetStreamClusterAccountUsageAfterServerRemove(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "R4S", 4)
	defer c.shutdown()

	ml := c.leader()
	require_NotNil(t, ml)

	nc, js := jsClientConnect(t, ml)
	defer nc.Close()

	_, err := js.AddStream(&nats.StreamConfig{
		Name:     "TEST",
		Subjects: []string{"foo"},
		Replicas: 3,
	})
	require_NoError(t, err)

	msg := bytes.Repeat([]byte("A"), 32*1024)
	for range 128 {
		_, err = js.Publish("foo", msg)
		require_NoError(t, err)
	}

	si, err := js.StreamInfo("TEST")
	require_NoError(t, err)
	expectedStore := si.State.Bytes * 3

	checkAccountStore := func(expected uint64) {
		t.Helper()
		checkFor(t, 10*time.Second, 100*time.Millisecond, func() error {
			info, err := js.AccountInfo()
			if err != nil {
				return err
			}
			if info.Store != expected {
				return fmt.Errorf("expected account store %d, got %d", expected, info.Store)
			}
			return nil
		})
	}
	checkAccountStore(expectedStore)

	// Remove a server that hosts a stream replica, but not the meta leader. The
	// stream should move to the spare fourth server without changing the account's
	// replicated storage total.
	var removed *Server
	for _, s := range c.servers {
		if s != ml && s.JetStreamIsStreamAssigned(globalAccountName, "TEST") {
			removed = s
			break
		}
	}
	require_NotNil(t, removed)
	removedPeer := removed.Node()
	removed.Shutdown()
	removed.WaitForShutdown()

	snc, _ := jsClientConnect(t, ml, nats.UserInfo("admin", "s3cr3t!"))
	defer snc.Close()
	b, err := json.Marshal(JSApiMetaServerRemoveRequest{Peer: removedPeer})
	require_NoError(t, err)
	rmsg, err := snc.Request(JSApiRemoveServer, b, 10*time.Second)
	require_NoError(t, err)
	var resp JSApiMetaServerRemoveResponse
	require_NoError(t, json.Unmarshal(rmsg.Data, &resp))
	require_True(t, resp.Success)

	c.waitOnPeerCount(3)
	checkFor(t, 10*time.Second, 100*time.Millisecond, func() error {
		si, err := js.StreamInfo("TEST", nats.MaxWait(time.Second))
		if err != nil {
			return err
		}
		if si.Cluster == nil || len(si.Cluster.Replicas) != 2 {
			return fmt.Errorf("stream has not returned to R3")
		}
		for _, peer := range si.Cluster.Replicas {
			if !peer.Current {
				return fmt.Errorf("stream replica %q is not current", peer.Name)
			}
		}
		return nil
	})

	checkAccountStore(expectedStore)

	// A usage update already in flight when the peer removal commits must not
	// recreate the departed server's remote usage entry.
	jsa := ml.getJetStream().lookupAccount(ml.globalAccount())
	require_NotNil(t, jsa)
	lateUpdate := make([]byte, minUsageUpdateLen)
	binary.LittleEndian.PutUint64(lateUpdate[8:], si.State.Bytes)
	jsa.remoteUpdateUsage(nil, nil, nil, fmt.Sprintf(jsaUpdatesPubT, globalAccountName, removedPeer), _EMPTY_, lateUpdate)
	jsa.usageMu.RLock()
	_, stale := jsa.rusage[removedPeer]
	jsa.usageMu.RUnlock()
	require_False(t, stale)
	checkAccountStore(expectedStore)
}

func TestJetStreamClusterStreamPeerRemoveNoReplacementIsRejected(t *testing.T) {
	for _, test := range []struct {
		name     string
		evacuate bool
	}{
		{"Remove", false},
		{"Evacuate", true},
	} {
		t.Run(test.name, func(t *testing.T) {
			// An R3 stream on a 3 node cluster has nowhere to hand a peer to.
			c := createJetStreamClusterExplicit(t, "R3S", 3)
			defer c.shutdown()

			nc, js := jsClientConnect(t, c.randomServer())
			defer nc.Close()

			_, err := js.AddStream(&nats.StreamConfig{
				Name:     "TEST",
				Subjects: []string{"foo"},
				Replicas: 3,
			})
			require_NoError(t, err)

			ml := c.leader()
			require_NotNil(t, ml)

			sjs := ml.getJetStream()
			sjs.mu.RLock()
			sa := sjs.streamAssignmentOrInflight(globalAccountName, "TEST")
			peers := append([]string(nil), sa.Group.Peers...)
			sjs.mu.RUnlock()
			require_Len(t, len(peers), 3)

			rs := c.randomNonStreamLeader(globalAccountName, "TEST")
			require_NotNil(t, rs)
			target := rs.Node()

			subjT := JSApiStreamRemovePeerT
			if test.evacuate {
				subjT = JSApiStreamEvacuatePeerT
			}
			b, err := json.Marshal(JSApiStreamRemovePeerRequest{Peer: target})
			require_NoError(t, err)
			msg, err := nc.Request(fmt.Sprintf(subjT, "TEST"), b, 10*time.Second)
			require_NoError(t, err)
			var resp JSApiStreamRemovePeerResponse
			require_NoError(t, json.Unmarshal(msg.Data, &resp))

			// The request must be rejected up front, rather than shortening the group
			// and having a later reconcile add the peer straight back.
			require_False(t, resp.Success)
			require_Error(t, resp.Error, NewJSPeerRemapError())

			checkPeers := func() {
				t.Helper()
				sjs := ml.getJetStream()
				sjs.mu.RLock()
				defer sjs.mu.RUnlock()
				sa := sjs.streamAssignment(globalAccountName, "TEST")
				require_NotNil(t, sa)
				require_True(t, sa.Group.Desired == nil)
				require_True(t, slices.Equal(sa.Group.Peers, peers))
				require_True(t, sjs.cluster.inflightStreams[globalAccountName]["TEST"] == nil)
			}
			// The meta leader could still be applying the create.
			checkFor(t, 2*time.Second, 10*time.Millisecond, func() error {
				sjs := ml.getJetStream()
				sjs.mu.RLock()
				defer sjs.mu.RUnlock()
				if sjs.cluster.inflightStreams[globalAccountName]["TEST"] != nil {
					return errors.New("stream assignment still inflight")
				}
				return nil
			})
			checkPeers()

			// A meta leader change runs reconcilePeerAssignments, which must find
			// nothing to heal. The old leader keeps reporting itself as leader until
			// its monitor goroutine processes the leader change, wait for a newer term.
			term := ml.getJetStream().getMetaGroup().Term()
			require_NoError(t, ml.getJetStream().getMetaGroup().StepDown())
			checkFor(t, 2*time.Second, 50*time.Millisecond, func() error {
				ml = c.leader()
				if ml == nil {
					return errors.New("no meta leader")
				}
				if nterm := ml.getJetStream().getMetaGroup().Term(); nterm <= term {
					return fmt.Errorf("meta leader still in term %d", nterm)
				}
				return nil
			})
			checkPeers()

			// The stream is untouched and still fully usable.
			_, err = js.Publish("foo", []byte("HELLO"))
			require_NoError(t, err)
			si, err := js.StreamInfo("TEST")
			require_NoError(t, err)
			require_Equal(t, si.State.Msgs, 1)
			require_Len(t, len(si.Cluster.Replicas), 2)
		})
	}
}

// A stream peer remove that would leave the group short is rejected even when the peer is
// offline. Scaling the stream down is the way out, that drops offline peers first.
func TestJetStreamClusterStreamPeerRemoveOfflinePeerIsRejected(t *testing.T) {
	for _, test := range []struct {
		name     string
		evacuate bool
	}{
		{"Remove", false},
		{"Evacuate", true},
	} {
		t.Run(test.name, func(t *testing.T) {
			// An R3 stream on a 3 node cluster has nowhere to hand a peer to.
			c := createJetStreamClusterExplicit(t, "R3S", 3)
			defer c.shutdown()

			// Connect to the meta leader, it is the one server we know stays up below.
			ml := c.leader()
			require_NotNil(t, ml)
			nc, js := jsClientConnect(t, ml)
			defer nc.Close()

			_, err := js.AddStream(&nats.StreamConfig{
				Name:     "TEST",
				Subjects: []string{"foo"},
				Replicas: 3,
			})
			require_NoError(t, err)

			// Take down a stream peer that is neither the meta nor the stream leader, so
			// both layers stay able to make progress.
			sl := c.streamLeader(globalAccountName, "TEST")
			var member *Server
			for _, s := range c.servers {
				if s != ml && s != sl {
					member = s
					break
				}
			}
			require_NotNil(t, member)
			target := member.Node()
			member.Shutdown()

			// Wait for the meta leader to consider it offline, that's the state under test.
			checkFor(t, 10*time.Second, 250*time.Millisecond, func() error {
				si, ok := ml.nodeToInfo.Load(target)
				if !ok || si == nil || !si.(nodeInfo).offline {
					return fmt.Errorf("peer %q not offline yet", target)
				}
				return nil
			})

			subjT := JSApiStreamRemovePeerT
			if test.evacuate {
				subjT = JSApiStreamEvacuatePeerT
			}
			b, err := json.Marshal(JSApiStreamRemovePeerRequest{Peer: target})
			require_NoError(t, err)
			msg, err := nc.Request(fmt.Sprintf(subjT, "TEST"), b, 10*time.Second)
			require_NoError(t, err)
			var resp JSApiStreamRemovePeerResponse
			require_NoError(t, json.Unmarshal(msg.Data, &resp))
			require_False(t, resp.Success)
			require_Error(t, resp.Error, NewJSPeerRemapError())

			// The peer set is untouched, offline peer and all.
			sjs := ml.getJetStream()
			sjs.mu.RLock()
			sa := sjs.streamAssignmentOrInflight(globalAccountName, "TEST")
			require_NotNil(t, sa)
			require_Len(t, len(sa.Group.Peers), 3)
			require_True(t, slices.Contains(sa.Group.Peers, target))
			require_Len(t, len(sa.Group.Excluded), 0)
			sjs.mu.RUnlock()

			// Scaling down is the documented way out, and it takes the offline peer.
			_, err = js.UpdateStream(&nats.StreamConfig{
				Name:     "TEST",
				Subjects: []string{"foo"},
				Replicas: 2,
			})
			require_NoError(t, err)

			checkFor(t, 10*time.Second, 250*time.Millisecond, func() error {
				sjs := ml.getJetStream()
				sjs.mu.RLock()
				defer sjs.mu.RUnlock()
				sa := sjs.streamAssignment(globalAccountName, "TEST")
				if sa == nil || sa.Group == nil {
					return fmt.Errorf("stream not found")
				}
				if sa.Group.Desired != nil {
					return fmt.Errorf("stream still converging")
				}
				if len(sa.Group.Peers) != 2 {
					return fmt.Errorf("expected 2 peers, got %v", sa.Group.Peers)
				}
				if slices.Contains(sa.Group.Peers, target) {
					return fmt.Errorf("offline peer %q still in peer set %v", target, sa.Group.Peers)
				}
				return nil
			})
		})
	}
}

// An exclusion is not a reason to migrate on its own, so a stale one is left alone until
// the group has some other reason to move, and is gone once the group is healed.
func TestJetStreamClusterEvacuateExclusionNotClearedByNoOpMigration(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "R3S", 3)
	defer c.shutdown()

	// Connect to the meta leader, it is the one server we know stays put below.
	ml := c.leader()
	require_NotNil(t, ml)
	nc, js := jsClientConnect(t, ml)
	defer nc.Close()

	// The server scoped endpoints are system account only.
	snc, _ := jsClientConnect(t, ml, nats.UserInfo("admin", "s3cr3t!"))
	defer snc.Close()

	_, err := js.AddStream(&nats.StreamConfig{
		Name:     "TEST",
		Subjects: []string{"foo"},
		Replicas: 3,
	})
	require_NoError(t, err)

	// Evacuate a server other than the meta leader. With only three servers there is no
	// replacement, so the stream is left short and the peer is excluded.
	var member *Server
	for _, s := range c.servers {
		if s != ml {
			member = s
			break
		}
	}
	require_NotNil(t, member)
	target := member.Node()

	b, err := json.Marshal(JSApiMetaServerEvacuateRequest{Peer: target})
	require_NoError(t, err)
	msg, err := snc.Request(JSApiEvacuateServer, b, 10*time.Second)
	require_NoError(t, err)
	var resp JSApiMetaServerEvacuateResponse
	require_NoError(t, json.Unmarshal(msg.Data, &resp))
	require_True(t, resp.Success)

	excluded := func() []string {
		t.Helper()
		l := c.leader()
		require_NotNil(t, l)
		sjs := l.getJetStream()
		sjs.mu.RLock()
		defer sjs.mu.RUnlock()
		sa := sjs.streamAssignment(globalAccountName, "TEST")
		require_NotNil(t, sa)
		return copyStrings(sa.Group.Excluded)
	}

	// Wait for the evacuation to settle, so what follows can't race its migration.
	checkFor(t, 10*time.Second, 250*time.Millisecond, func() error {
		l := c.leader()
		if l == nil {
			return errors.New("no meta leader")
		}
		sjs := l.getJetStream()
		sjs.mu.RLock()
		defer sjs.mu.RUnlock()
		sa := sjs.streamAssignment(globalAccountName, "TEST")
		if sa == nil || sa.Group == nil {
			return fmt.Errorf("stream not found")
		}
		if sa.Group.Desired != nil {
			return fmt.Errorf("stream still converging")
		}
		if len(sa.Group.Peers) != 2 || slices.Contains(sa.Group.Peers, target) {
			return fmt.Errorf("expected 2 peers without %q, got %v", target, sa.Group.Peers)
		}
		if !slices.Contains(sa.Group.Excluded, target) {
			return fmt.Errorf("expected %q to be excluded, got %v", target, sa.Group.Excluded)
		}
		return nil
	})

	// Now take the same server out of the meta group, so the exclusion is dead weight,
	// placement only ever draws from the meta peer set.
	msg, err = snc.Request(JSApiRemoveServer, b, 10*time.Second)
	require_NoError(t, err)
	require_NoError(t, json.Unmarshal(msg.Data, &resp))
	require_True(t, resp.Success)
	c.waitOnPeerCount(2)

	// Reconcile every assignment. Dropping the stale exclusion is not worth a migration
	// of its own, so the group must be left exactly as it is.
	require_NoError(t, c.leader().getJetStream().getMetaGroup().StepDown())
	c.waitOnLeader()
	time.Sleep(500 * time.Millisecond)
	require_True(t, slices.Contains(excluded(), target))

	// Adding a server gives the group a real reason to move. Now it heals to its replica
	// count, and the exclusions go with it.
	ns := c.addInNewServer()
	c.waitOnPeerCount(3)

	checkFor(t, 10*time.Second, 250*time.Millisecond, func() error {
		l := c.leader()
		if l == nil {
			return errors.New("no meta leader")
		}
		sjs := l.getJetStream()
		sjs.mu.RLock()
		defer sjs.mu.RUnlock()
		sa := sjs.streamAssignment(globalAccountName, "TEST")
		if sa == nil || sa.Group == nil {
			return fmt.Errorf("stream not found")
		}
		if len(sa.Group.Peers) != 3 {
			return fmt.Errorf("expected 3 peers, got %v", sa.Group.Peers)
		}
		if !slices.Contains(sa.Group.Peers, ns.Node()) {
			return fmt.Errorf("expected new server in peers, got %v", sa.Group.Peers)
		}
		if len(sa.Group.Excluded) != 0 {
			return fmt.Errorf("expected exclusions cleared, got %v", sa.Group.Excluded)
		}
		return nil
	})
}

// Lowering the replica count to match a short group heals it just as well as adding a peer
// back, so the exclusions must not survive that convergence either. Otherwise the evacuated
// peer stays ineligible and a later loss can fail to heal against available capacity.
func TestJetStreamClusterEvacuateExclusionClearedByReplicaDecrease(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "R3S", 3)
	defer c.shutdown()

	// Connect to the meta leader, it is the one server we know stays put below.
	ml := c.leader()
	require_NotNil(t, ml)
	nc, js := jsClientConnect(t, ml)
	defer nc.Close()

	// The server scoped endpoints are system account only.
	snc, _ := jsClientConnect(t, ml, nats.UserInfo("admin", "s3cr3t!"))
	defer snc.Close()

	_, err := js.AddStream(&nats.StreamConfig{
		Name:     "TEST",
		Subjects: []string{"foo"},
		Replicas: 3,
	})
	require_NoError(t, err)

	// Evacuate a server other than the meta leader. With only three servers there is no
	// replacement, so the stream is left short and the peer is excluded.
	var member *Server
	for _, s := range c.servers {
		if s != ml {
			member = s
			break
		}
	}
	require_NotNil(t, member)
	target := member.Node()

	b, err := json.Marshal(JSApiMetaServerEvacuateRequest{Peer: target})
	require_NoError(t, err)
	msg, err := snc.Request(JSApiEvacuateServer, b, 10*time.Second)
	require_NoError(t, err)
	var resp JSApiMetaServerEvacuateResponse
	require_NoError(t, json.Unmarshal(msg.Data, &resp))
	require_True(t, resp.Success)

	// Wait for the evacuation to settle, so the update below can't race its migration.
	checkFor(t, 10*time.Second, 250*time.Millisecond, func() error {
		l := c.leader()
		if l == nil {
			return errors.New("no meta leader")
		}
		sjs := l.getJetStream()
		sjs.mu.RLock()
		defer sjs.mu.RUnlock()
		sa := sjs.streamAssignment(globalAccountName, "TEST")
		if sa == nil || sa.Group == nil {
			return fmt.Errorf("stream not found")
		}
		if sa.Group.Desired != nil {
			return fmt.Errorf("stream still converging")
		}
		if len(sa.Group.Peers) != 2 || slices.Contains(sa.Group.Peers, target) {
			return fmt.Errorf("expected 2 peers without %q, got %v", target, sa.Group.Peers)
		}
		if !slices.Contains(sa.Group.Excluded, target) {
			return fmt.Errorf("expected %q to be excluded, got %v", target, sa.Group.Excluded)
		}
		return nil
	})

	// The evacuated peer is still in the meta group, so it remains a placement candidate
	// for as long as the exclusion is kept around.
	c.waitOnPeerCount(3)

	// Accept the group as it stands by lowering the replica count to the surviving peers.
	// The group is no longer short, so the evacuation is fully accounted for.
	_, err = js.UpdateStream(&nats.StreamConfig{
		Name:     "TEST",
		Subjects: []string{"foo"},
		Replicas: 2,
	})
	require_NoError(t, err)

	checkFor(t, 10*time.Second, 250*time.Millisecond, func() error {
		l := c.leader()
		if l == nil {
			return errors.New("no meta leader")
		}
		sjs := l.getJetStream()
		sjs.mu.RLock()
		defer sjs.mu.RUnlock()
		sa := sjs.streamAssignment(globalAccountName, "TEST")
		if sa == nil || sa.Group == nil {
			return fmt.Errorf("stream not found")
		}
		if sa.Group.Desired != nil {
			return fmt.Errorf("stream still converging")
		}
		if len(sa.Group.Peers) != 2 || slices.Contains(sa.Group.Peers, target) {
			return fmt.Errorf("expected 2 peers without %q, got %v", target, sa.Group.Peers)
		}
		if len(sa.Group.Excluded) != 0 {
			return fmt.Errorf("expected exclusions cleared, got %v", sa.Group.Excluded)
		}
		return nil
	})
}

func TestJetStreamClusterPeerRemoveAndEvacuateConsumersMatrix(t *testing.T) {
	const clusterSize = 3
	c := createJetStreamClusterExplicit(t, "R3S", clusterSize)
	defer c.shutdown()

	nc, js := jsClientConnect(t, c.randomServer())
	defer nc.Close()

	for i, test := range []struct {
		name             string
		evacuate         bool
		streamReplicas   int
		consumerReplicas int
		ephemeral        bool
	}{
		{"Remove/StreamR1/DurableR1", false, 1, 1, false},
		{"Remove/StreamR1/EphemeralR1", false, 1, 1, true},
		{"Remove/StreamR3/DurableR1", false, 3, 1, false},
		{"Remove/StreamR3/EphemeralR1", false, 3, 1, true},
		{"Remove/StreamR3/DurableR3", false, 3, 3, false},
		{"Remove/StreamR3/EphemeralR3", false, 3, 3, true},
		{"Evacuate/StreamR1/DurableR1", true, 1, 1, false},
		{"Evacuate/StreamR1/EphemeralR1", true, 1, 1, true},
		{"Evacuate/StreamR3/DurableR1", true, 3, 1, false},
		{"Evacuate/StreamR3/EphemeralR1", true, 3, 1, true},
		{"Evacuate/StreamR3/DurableR3", true, 3, 3, false},
		{"Evacuate/StreamR3/EphemeralR3", true, 3, 3, true},
	} {
		t.Run(test.name, func(t *testing.T) {
			// A peer can only be replaced if there is a server outside of the
			// stream's peer set to hand it to.
			replaceable := test.streamReplicas < clusterSize

			// Without a replacement the request is rejected and nothing moves, so
			// either way the stream keeps its replica count.
			peersAfterOp := test.streamReplicas

			// A peer remove drops an ephemeral that loses its only peer, an
			// evacuation migrates it across instead. Durables are always remapped.
			// A rejected request leaves the consumer alone.
			consumerDeleted := replaceable && !test.evacuate && test.ephemeral && test.consumerReplicas == 1

			// Each case gets its own stream so that the cases stay independent.
			stream := fmt.Sprintf("TEST_%d", i)
			subject := fmt.Sprintf("foo.%d", i)
			_, err := js.AddStream(&nats.StreamConfig{
				Name:     stream,
				Subjects: []string{subject},
				Replicas: test.streamReplicas,
			})
			require_NoError(t, err)

			ccfg := &nats.ConsumerConfig{
				AckPolicy:         nats.AckExplicitPolicy,
				Replicas:          test.consumerReplicas,
				InactiveThreshold: time.Hour,
			}
			if !test.ephemeral {
				ccfg.Durable = "CONSUMER"
			}
			cinfo, err := js.AddConsumer(stream, ccfg)
			require_NoError(t, err)
			consumer := cinfo.Name

			// Target a peer that hosts the consumer, so that every case actually
			// exercises the consumer being taken off of the targeted peer. A
			// replicated consumer sits on every stream peer, so any of them will do.
			var rs *Server
			if test.consumerReplicas == 1 {
				rs = c.consumerLeader(globalAccountName, stream, consumer)
			} else {
				rs = c.randomNonStreamLeader(globalAccountName, stream)
				if rs == nil {
					rs = c.streamLeader(globalAccountName, stream)
				}
			}
			require_NotNil(t, rs)
			target := rs.Node()

			subjT := JSApiStreamRemovePeerT
			if test.evacuate {
				subjT = JSApiStreamEvacuatePeerT
			}
			b, err := json.Marshal(JSApiStreamRemovePeerRequest{Peer: target})
			require_NoError(t, err)
			msg, err := nc.Request(fmt.Sprintf(subjT, stream), b, 10*time.Second)
			require_NoError(t, err)
			var resp JSApiStreamRemovePeerResponse
			require_NoError(t, json.Unmarshal(msg.Data, &resp))
			require_Equal(t, resp.Success, replaceable)
			if !replaceable {
				// There was nowhere to move the peer to, so the request is rejected
				// and the stream is left alone.
				require_Error(t, resp.Error, NewJSPeerRemapError())
			}

			ml := c.leader()
			require_NotNil(t, ml)

			// The stream must settle without the targeted peer, unless the request
			// was rejected, in which case the peer set must be untouched.
			checkFor(t, 5*time.Second, 250*time.Millisecond, func() error {
				sjs := ml.getJetStream()
				sjs.mu.RLock()
				defer sjs.mu.RUnlock()
				sa := sjs.streamAssignment(globalAccountName, stream)
				if sa == nil || sa.Group == nil {
					return fmt.Errorf("stream not found")
				}
				if sa.Group.Desired != nil {
					return fmt.Errorf("stream still converging")
				}
				if len(sa.Group.Peers) != peersAfterOp {
					return fmt.Errorf("expected %d peers, got %d", peersAfterOp, len(sa.Group.Peers))
				}
				if got := slices.Contains(sa.Group.Peers, target); got != !replaceable {
					return fmt.Errorf("peer %q in peer set = %v, want %v (peers %v)", target, got, !replaceable, sa.Group.Peers)
				}
				return nil
			})

			// The consumer either got dropped, or it moved off of the targeted peer
			// onto as much of the stream's peer set as its own replica count asks for.
			// A rejected request leaves it where it was.
			expectedConsumerPeers := min(test.consumerReplicas, peersAfterOp)
			checkFor(t, 5*time.Second, 250*time.Millisecond, func() error {
				sjs := ml.getJetStream()
				sjs.mu.RLock()
				defer sjs.mu.RUnlock()
				ca := sjs.consumerAssignment(globalAccountName, stream, consumer)
				if consumerDeleted {
					if ca != nil {
						return fmt.Errorf("consumer still assigned to %v", ca.Group.Peers)
					}
					return nil
				}
				if ca == nil || ca.Group == nil {
					return fmt.Errorf("consumer not found")
				}
				if ca.Group.Desired != nil {
					return fmt.Errorf("consumer still converging")
				}
				if len(ca.Group.Peers) != expectedConsumerPeers {
					return fmt.Errorf("expected %d consumer peers, got %v", expectedConsumerPeers, ca.Group.Peers)
				}
				if got := slices.Contains(ca.Group.Peers, target); got != !replaceable {
					return fmt.Errorf("peer %q in consumer peer set = %v, want %v (peers %v)", target, got, !replaceable, ca.Group.Peers)
				}
				return nil
			})

			// A surviving consumer must still be usable.
			if !consumerDeleted {
				_, err = js.Publish(subject, []byte("HELLO"))
				require_NoError(t, err)
				// Bind by name, an ephemeral has no durable name to pass here.
				sub, err := js.PullSubscribe(subject, _EMPTY_, nats.Bind(stream, consumer))
				require_NoError(t, err)
				defer sub.Unsubscribe()
				msgs, err := sub.Fetch(1, nats.MaxWait(10*time.Second))
				require_NoError(t, err)
				require_Len(t, len(msgs), 1)
			}
		})
	}
}

func TestJetStreamClusterConsumerPeerRemoveAndEvacuateMatrix(t *testing.T) {
	const clusterSize = 3
	c := createJetStreamClusterExplicit(t, "R3S", clusterSize)
	defer c.shutdown()

	nc, js := jsClientConnect(t, c.randomServer())
	defer nc.Close()

	for i, test := range []struct {
		name             string
		evacuate         bool
		streamReplicas   int
		consumerReplicas int
	}{
		{"Remove/StreamR1/ConsumerR1", false, 1, 1},
		{"Remove/StreamR3/ConsumerR1", false, 3, 1},
		{"Remove/StreamR3/ConsumerR3", false, 3, 3},
		{"Evacuate/StreamR1/ConsumerR1", true, 1, 1},
		{"Evacuate/StreamR3/ConsumerR1", true, 3, 1},
		{"Evacuate/StreamR3/ConsumerR3", true, 3, 3},
	} {
		t.Run(test.name, func(t *testing.T) {
			// The stream's group is left alone, so the replacement has to be a stream
			// peer the consumer is not on yet. A consumer that already spans the stream
			// has none, and neither does one on a single peer stream.
			replaceable := test.consumerReplicas < test.streamReplicas

			// Each case gets its own stream so that the cases stay independent.
			stream := fmt.Sprintf("TEST_%d", i)
			subject := fmt.Sprintf("foo.%d", i)
			_, err := js.AddStream(&nats.StreamConfig{
				Name:     stream,
				Subjects: []string{subject},
				Replicas: test.streamReplicas,
			})
			require_NoError(t, err)

			_, err = js.AddConsumer(stream, &nats.ConsumerConfig{
				Durable:           "CONSUMER",
				AckPolicy:         nats.AckExplicitPolicy,
				Replicas:          test.consumerReplicas,
				InactiveThreshold: time.Hour,
			})
			require_NoError(t, err)

			ml := c.leader()
			require_NotNil(t, ml)

			sjs := ml.getJetStream()
			sjs.mu.RLock()
			sa := sjs.streamAssignmentOrInflight(globalAccountName, stream)
			streamPeers := append([]string(nil), sa.Group.Peers...)
			sjs.mu.RUnlock()
			require_Len(t, len(streamPeers), test.streamReplicas)

			// Target a peer that hosts the consumer, so that every case actually
			// exercises the consumer being taken off of the targeted peer. A
			// replicated consumer sits on every stream peer, so any of them will do.
			var rs *Server
			if test.consumerReplicas == 1 {
				rs = c.consumerLeader(globalAccountName, stream, "CONSUMER")
			} else {
				rs = c.randomNonStreamLeader(globalAccountName, stream)
				if rs == nil {
					rs = c.streamLeader(globalAccountName, stream)
				}
			}
			require_NotNil(t, rs)
			target := rs.Node()

			subjT := JSApiConsumerRemovePeerT
			if test.evacuate {
				subjT = JSApiConsumerEvacuatePeerT
			}
			b, err := json.Marshal(JSApiConsumerRemovePeerRequest{Peer: target})
			require_NoError(t, err)
			msg, err := nc.Request(fmt.Sprintf(subjT, stream, "CONSUMER"), b, 10*time.Second)
			require_NoError(t, err)
			var resp JSApiConsumerRemovePeerResponse
			require_NoError(t, json.Unmarshal(msg.Data, &resp))
			require_Equal(t, resp.Success, replaceable)
			if !replaceable {
				// There was nowhere to move the consumer to, so the request is
				// rejected and the consumer is left alone.
				require_Error(t, resp.Error, NewJSPeerRemapError())
			}

			// The consumer moved off of the targeted peer onto another of the stream's
			// peers, keeping its replica count. A rejected request leaves it where it was.
			checkFor(t, 5*time.Second, 250*time.Millisecond, func() error {
				sjs.mu.RLock()
				defer sjs.mu.RUnlock()
				ca := sjs.consumerAssignment(globalAccountName, stream, "CONSUMER")
				if ca == nil || ca.Group == nil {
					return fmt.Errorf("consumer not found")
				}
				if ca.Group.Desired != nil {
					return fmt.Errorf("consumer still converging")
				}
				if len(ca.Group.Peers) != test.consumerReplicas {
					return fmt.Errorf("expected %d consumer peers, got %v", test.consumerReplicas, ca.Group.Peers)
				}
				if got := slices.Contains(ca.Group.Peers, target); got != !replaceable {
					return fmt.Errorf("peer %q in consumer peer set = %v, want %v (peers %v)", target, got, !replaceable, ca.Group.Peers)
				}
				for _, p := range ca.Group.Peers {
					if !slices.Contains(streamPeers, p) {
						return fmt.Errorf("consumer peer %q is not a stream peer %v", p, streamPeers)
					}
				}
				return nil
			})

			// The consumer must still be usable.
			_, err = js.Publish(subject, []byte("HELLO"))
			require_NoError(t, err)
			sub, err := js.PullSubscribe(subject, _EMPTY_, nats.Bind(stream, "CONSUMER"))
			require_NoError(t, err)
			defer sub.Unsubscribe()
			msgs, err := sub.Fetch(1, nats.MaxWait(10*time.Second))
			require_NoError(t, err)
			require_Len(t, len(msgs), 1)
		})
	}
}

func TestJetStreamClusterConsumerPeerRemoveNoReplacementIsRejected(t *testing.T) {
	for _, test := range []struct {
		name     string
		evacuate bool
	}{
		{"Remove", false},
		{"Evacuate", true},
	} {
		t.Run(test.name, func(t *testing.T) {
			// An R3 consumer on an R3 stream has nowhere to hand a peer to.
			c := createJetStreamClusterExplicit(t, "R3S", 3)
			defer c.shutdown()

			nc, js := jsClientConnect(t, c.randomServer())
			defer nc.Close()

			_, err := js.AddStream(&nats.StreamConfig{
				Name:     "TEST",
				Subjects: []string{"foo"},
				Replicas: 3,
			})
			require_NoError(t, err)

			_, err = js.AddConsumer("TEST", &nats.ConsumerConfig{
				Durable:   "CONSUMER",
				AckPolicy: nats.AckExplicitPolicy,
				Replicas:  3,
			})
			require_NoError(t, err)

			ml := c.leader()
			require_NotNil(t, ml)

			sjs := ml.getJetStream()
			sjs.mu.RLock()
			ca := sjs.consumerAssignmentOrInflight(globalAccountName, "TEST", "CONSUMER")
			peers := append([]string(nil), ca.Group.Peers...)
			sjs.mu.RUnlock()
			require_Len(t, len(peers), 3)

			rs := c.randomNonStreamLeader(globalAccountName, "TEST")
			require_NotNil(t, rs)
			target := rs.Node()

			subjT := JSApiConsumerRemovePeerT
			if test.evacuate {
				subjT = JSApiConsumerEvacuatePeerT
			}
			b, err := json.Marshal(JSApiConsumerRemovePeerRequest{Peer: target})
			require_NoError(t, err)
			msg, err := nc.Request(fmt.Sprintf(subjT, "TEST", "CONSUMER"), b, 10*time.Second)
			require_NoError(t, err)
			var resp JSApiConsumerRemovePeerResponse
			require_NoError(t, json.Unmarshal(msg.Data, &resp))
			require_False(t, resp.Success)
			require_Error(t, resp.Error, NewJSPeerRemapError())

			checkPeers := func() {
				t.Helper()
				sjs := ml.getJetStream()
				sjs.mu.RLock()
				defer sjs.mu.RUnlock()
				ca := sjs.consumerAssignment(globalAccountName, "TEST", "CONSUMER")
				require_NotNil(t, ca)
				require_True(t, ca.Group.Desired == nil)
				require_True(t, slices.Equal(ca.Group.Peers, peers))
				require_True(t, sjs.cluster.inflightConsumers[globalAccountName]["TEST"]["CONSUMER"] == nil)
			}
			// The meta leader could still be applying the create.
			checkFor(t, 2*time.Second, 10*time.Millisecond, func() error {
				sjs := ml.getJetStream()
				sjs.mu.RLock()
				defer sjs.mu.RUnlock()
				if sjs.cluster.inflightConsumers[globalAccountName]["TEST"]["CONSUMER"] != nil {
					return errors.New("consumer assignment still inflight")
				}
				return nil
			})
			checkPeers()

			// A meta leader change runs reconcilePeerAssignments, which must find
			// nothing to heal. The old leader keeps reporting itself as leader until
			// its monitor goroutine processes the leader change, wait for a newer term.
			term := ml.getJetStream().getMetaGroup().Term()
			require_NoError(t, ml.getJetStream().getMetaGroup().StepDown())
			checkFor(t, 2*time.Second, 50*time.Millisecond, func() error {
				ml = c.leader()
				if ml == nil {
					return errors.New("no meta leader")
				}
				if nterm := ml.getJetStream().getMetaGroup().Term(); nterm <= term {
					return fmt.Errorf("meta leader still in term %d", nterm)
				}
				return nil
			})
			checkPeers()

			// The consumer is untouched and still fully usable.
			_, err = js.Publish("foo", []byte("HELLO"))
			require_NoError(t, err)
			sub, err := js.PullSubscribe("foo", _EMPTY_, nats.Bind("TEST", "CONSUMER"))
			require_NoError(t, err)
			defer sub.Unsubscribe()
			msgs, err := sub.Fetch(1, nats.MaxWait(10*time.Second))
			require_NoError(t, err)
			require_Len(t, len(msgs), 1)
		})
	}
}

func TestJetStreamClusterConsumerPeerRemoveInvalidRequestIsRejected(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "R3S", 3)
	defer c.shutdown()

	nc, js := jsClientConnect(t, c.randomServer())
	defer nc.Close()

	_, err := js.AddStream(&nats.StreamConfig{
		Name:     "TEST",
		Subjects: []string{"foo"},
		Replicas: 3,
	})
	require_NoError(t, err)

	_, err = js.AddConsumer("TEST", &nats.ConsumerConfig{
		Durable:   "CONSUMER",
		AckPolicy: nats.AckExplicitPolicy,
		Replicas:  1,
	})
	require_NoError(t, err)

	ml := c.leader()
	require_NotNil(t, ml)

	sjs := ml.getJetStream()
	sjs.mu.RLock()
	ca := sjs.consumerAssignmentOrInflight(globalAccountName, "TEST", "CONSUMER")
	peers := append([]string(nil), ca.Group.Peers...)
	sjs.mu.RUnlock()
	require_Len(t, len(peers), 1)

	// A peer of the stream that the consumer does not sit on is not a member of the
	// consumer's group, so it can not be taken out of it.
	var notMember string
	for _, s := range c.servers {
		if n := s.Node(); n != peers[0] {
			notMember = n
			break
		}
	}
	require_NotEqual(t, notMember, _EMPTY_)

	for _, test := range []struct {
		name     string
		evacuate bool
		stream   string
		consumer string
		peer     string
		wantErr  *ApiError
	}{
		{"Remove/PeerNotMember", false, "TEST", "CONSUMER", notMember, NewJSClusterPeerNotMemberError()},
		{"Remove/UnknownStream", false, "NO_STREAM", "CONSUMER", peers[0], NewJSStreamNotFoundError()},
		{"Remove/UnknownConsumer", false, "TEST", "NO_CONSUMER", peers[0], NewJSConsumerNotFoundError()},
		{"Remove/NoPeer", false, "TEST", "CONSUMER", _EMPTY_, NewJSBadRequestError()},
		{"Evacuate/PeerNotMember", true, "TEST", "CONSUMER", notMember, NewJSClusterPeerNotMemberError()},
		{"Evacuate/UnknownStream", true, "NO_STREAM", "CONSUMER", peers[0], NewJSStreamNotFoundError()},
		{"Evacuate/UnknownConsumer", true, "TEST", "NO_CONSUMER", peers[0], NewJSConsumerNotFoundError()},
		{"Evacuate/NoPeer", true, "TEST", "CONSUMER", _EMPTY_, NewJSBadRequestError()},
	} {
		t.Run(test.name, func(t *testing.T) {
			subjT := JSApiConsumerRemovePeerT
			if test.evacuate {
				subjT = JSApiConsumerEvacuatePeerT
			}
			b, err := json.Marshal(JSApiConsumerRemovePeerRequest{Peer: test.peer})
			require_NoError(t, err)
			msg, err := nc.Request(fmt.Sprintf(subjT, test.stream, test.consumer), b, 10*time.Second)
			require_NoError(t, err)
			var resp JSApiConsumerRemovePeerResponse
			require_NoError(t, json.Unmarshal(msg.Data, &resp))
			require_False(t, resp.Success)
			require_Error(t, resp.Error, test.wantErr)

			// The consumer that was named is left where it was.
			sjs.mu.RLock()
			defer sjs.mu.RUnlock()
			ca := sjs.consumerAssignmentOrInflight(globalAccountName, "TEST", "CONSUMER")
			require_NotNil(t, ca)
			require_True(t, slices.Equal(ca.Group.Peers, peers))
		})
	}
}

func TestJetStreamClusterConsumerPeerRemoveOfflinePeerIsReplaced(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "R3S", 3)
	defer c.shutdown()

	cnc, js := jsClientConnect(t, c.randomServer())
	defer cnc.Close()

	_, err := js.AddStream(&nats.StreamConfig{
		Name:     "TEST",
		Subjects: []string{"foo"},
		Replicas: 3,
	})
	require_NoError(t, err)

	_, err = js.AddConsumer("TEST", &nats.ConsumerConfig{
		Durable:   "CONSUMER",
		AckPolicy: nats.AckExplicitPolicy,
		Replicas:  1,
	})
	require_NoError(t, err)

	// A single replica consumer can not be stepped down onto another server, its group has
	// only the one peer. Move the meta leader instead, the server we take down must not be
	// the one that has to serve the request once it is gone.
	cl := c.consumerLeader(globalAccountName, "TEST", "CONSUMER")
	require_NotNil(t, cl)
	ml := c.leader()
	if ml == cl {
		require_NoError(t, ml.getJetStream().getMetaGroup().StepDown())
		c.waitOnLeader()
		ml = c.leader()
	}
	require_NotNil(t, ml)
	require_NotEqual(t, ml, cl)
	target := cl.Node()

	// Connect to the meta leader, it is the one server we know stays up below.
	nc, js := jsClientConnect(t, ml)
	defer nc.Close()

	// Take the consumer's only peer down and wait for the meta leader to see it go.
	cl.Shutdown()
	cl.WaitForShutdown()
	checkFor(t, 5*time.Second, 250*time.Millisecond, func() error {
		si, ok := ml.nodeToInfo.Load(target)
		if !ok || si == nil || !si.(nodeInfo).offline {
			return fmt.Errorf("peer %q not offline yet", target)
		}
		return nil
	})

	b, err := json.Marshal(JSApiConsumerRemovePeerRequest{Peer: target})
	require_NoError(t, err)
	msg, err := nc.Request(fmt.Sprintf(JSApiConsumerRemovePeerT, "TEST", "CONSUMER"), b, 2*time.Second)
	require_NoError(t, err)
	var resp JSApiConsumerRemovePeerResponse
	require_NoError(t, json.Unmarshal(msg.Data, &resp))
	require_True(t, resp.Success)

	// It must move onto a server that is up. Handing it to the other downed peer would
	// leave it exactly as unservable as it was.
	checkFor(t, 5*time.Second, 250*time.Millisecond, func() error {
		sjs := ml.getJetStream()
		sjs.mu.RLock()
		defer sjs.mu.RUnlock()
		ca := sjs.consumerAssignment(globalAccountName, "TEST", "CONSUMER")
		if ca == nil || ca.Group == nil {
			return errors.New("consumer not found")
		}
		if slices.Contains(ca.Group.Peers, target) {
			return fmt.Errorf("consumer still on removed peer %q", target)
		}
		if len(ca.Group.Peers) != 1 {
			return fmt.Errorf("expected 1 consumer peer, got %v", ca.Group.Peers)
		}
		return nil
	})

	// And it must actually come back to life on its new peer.
	c.waitOnConsumerLeader(globalAccountName, "TEST", "CONSUMER")
	_, err = js.ConsumerInfo("TEST", "CONSUMER")
	require_NoError(t, err)
}

func TestJetStreamClusterRemapConsumerPendingScaleDown(t *testing.T) {
	const a, b, c = "A", "B", "C"

	cc := &jetStreamCluster{s: &Server{}}
	// The stream has scaled down onto A and B, so C is no longer one of its peers.
	// The consumer is following it down, C is still one of its actual peers but the set it
	// is converging to has already dropped it.
	newAssignments := func(replicas int) (*streamAssignment, *consumerAssignment) {
		sa := &streamAssignment{
			Config: &StreamConfig{Name: "TEST", Replicas: 2, Retention: LimitsPolicy},
			Group:  &raftGroup{Name: "S", Peers: []string{a, b}},
		}
		ca := &consumerAssignment{
			Name:   "CONSUMER",
			Stream: "TEST",
			Config: &ConsumerConfig{Durable: "CONSUMER", Replicas: replicas},
			Group: &raftGroup{
				Name:  "C",
				Peers: []string{a, b, c},
				Desired: &desiredRaftGroup{
					Peers:     []string{a, b},
					ScaleDown: true,
				},
			},
		}
		sa.consumers = map[string]*consumerAssignment{ca.Name: ca}
		return sa, ca
	}

	// Take A out of a consumer heading for a single peer. B alone still satisfies that, and
	// the stream has nothing to replace A with.
	sa, ca := newAssignments(1)
	cca := cc.remapConsumerAssignment(sa, ca, a, true)
	require_NotNil(t, cca)

	// C must not come back into the set the consumer converges to, the stream is not on it.
	require_False(t, slices.Contains(cca.Group.Desired.Peers, c))
	require_True(t, slices.Equal(cca.Group.Desired.Peers, []string{b}))
	require_True(t, cca.Group.Desired.ScaleDown)

	// The removal itself is recorded, so a group below quorum can evict the peer.
	require_False(t, slices.Contains(cca.Group.Peers, a))
	require_True(t, slices.Contains(cca.Group.Desired.Removed, a))

	// The group keeps its name. Renaming a single node group belongs to the reconcile,
	// which does it once the desired state lands.
	require_Equal(t, cca.Group.Name, ca.Group.Name)

	// A pending scale down is no license to shrink. Those desired peers are what the group
	// leader still has to choose from, so taking one out of a consumer heading for two
	// peers would leave it unable to pick a full set, and is refused.
	sa, ca = newAssignments(2)
	require_True(t, cc.remapConsumerAssignment(sa, ca, a, true) == nil)

	// Evacuating a peer that the consumer is already converging away from changes nothing,
	// so it must not be proposed as a no-op migration.
	sa, ca = newAssignments(1)
	require_True(t, cc.remapConsumerAssignment(sa, ca, c, false) == nil)

	// Removing that same peer is not a no-op, it still has to be recorded as removed.
	sa, ca = newAssignments(1)
	cca = cc.remapConsumerAssignment(sa, ca, c, true)
	require_NotNil(t, cca)
	require_False(t, slices.Contains(cca.Group.Peers, c))
	require_True(t, slices.Contains(cca.Group.Desired.Removed, c))
	require_True(t, slices.Equal(cca.Group.Desired.Peers, []string{a, b}))
}

func TestJetStreamClusterRemapConsumerOnDroppedStreamPeer(t *testing.T) {
	const a, b, c = "A", "B", "C"

	// The stream is scaling down onto B. C is a leaving peer, but the consumer is still hosted on it.
	newAssignments := func(metaPeers []string, durable bool) (*jetStream, *streamAssignment) {
		cfg := &ConsumerConfig{Replicas: 1}
		if durable {
			cfg.Durable = "CONSUMER"
		}
		sa := &streamAssignment{
			Config: &StreamConfig{Name: "TEST", Replicas: 1, Retention: LimitsPolicy},
			Group: &raftGroup{
				Name:    "S",
				Peers:   metaPeers,
				Desired: &desiredRaftGroup{Peers: []string{b}},
			},
			consumers: map[string]*consumerAssignment{"CONSUMER": {
				Name:   "CONSUMER",
				Stream: "TEST",
				Config: cfg,
				Group:  &raftGroup{Name: "C", Peers: []string{c}},
			}},
		}
		js := &jetStream{cluster: &jetStreamCluster{
			streams: map[string]map[string]*streamAssignment{globalAccountName: {sa.Config.Name: sa}},
		}}
		return js, sa
	}

	// The stream's peer set still lists C, it has been removed from the meta group but the
	// assignment has not caught up yet. The consumer is moved through desired state, so it
	// stays assigned to C, which is what keeps blocking the stream's scale-down.
	js, sa := newAssignments([]string{a, b, c}, false)
	consumers, deleted, done := js.remapConsumerAssignments(globalAccountName, sa)
	require_Len(t, len(deleted), 0)
	require_Len(t, len(consumers), 1)
	require_True(t, slices.Equal(consumers[0].Group.Peers, []string{c}))
	require_True(t, slices.Equal(consumers[0].Group.Desired.Peers, []string{b}))
	require_False(t, done)

	// Once the stream's peer set has caught up, the consumer must not be skipped: it holds
	// a peer the stream no longer has, and nothing else can move it there. An ephemeral is
	// deleted, which unblocks the scale-down and lets the desired state complete.
	sa.Group.Peers = []string{a, b}
	sa.consumers["CONSUMER"] = consumers[0]
	consumers, deleted, done = js.remapConsumerAssignments(globalAccountName, sa)
	require_Len(t, len(consumers), 0)
	require_Len(t, len(deleted), 1)
	require_Equal(t, deleted[0].Name, "CONSUMER")
	require_False(t, done)

	// A durable can't be deleted, it moves onto the peers the stream is keeping.
	js, sa = newAssignments([]string{a, b}, true)
	consumers, deleted, done = js.remapConsumerAssignments(globalAccountName, sa)
	require_Len(t, len(deleted), 0)
	require_Len(t, len(consumers), 1)
	require_True(t, slices.Equal(consumers[0].Group.Peers, []string{b}))
	require_False(t, done)
}

func TestJetStreamClusterRemapConsumerOverReplicated(t *testing.T) {
	const a, b, c = "A", "B", "C"

	cc := &jetStreamCluster{s: &Server{}}
	sa := &streamAssignment{
		Config: &StreamConfig{Name: "TEST", Replicas: 3, Retention: LimitsPolicy},
		Group:  &raftGroup{Name: "S", Peers: []string{a, b, c}},
	}
	// Assigned three peers while its config asks for two, for instance a replica count
	// that was lowered but not reconciled yet.
	ca := &consumerAssignment{
		Name:   "CONSUMER",
		Stream: "TEST",
		Config: &ConsumerConfig{Durable: "CONSUMER", Replicas: 2},
		Group:  &raftGroup{Name: "C", Peers: []string{a, b, c}},
	}
	sa.consumers = map[string]*consumerAssignment{ca.Name: ca}

	// Every stream peer is taken, so there is no replacement, but dropping to two still
	// satisfies the configured count.
	cca := cc.remapConsumerAssignment(sa, ca, a, true)
	require_NotNil(t, cca)
	require_False(t, slices.Contains(cca.Group.Desired.Peers, a))
	require_Len(t, len(cca.Group.Desired.Peers), 2)
}

func TestJetStreamClusterPeerExclusionTag(t *testing.T) {
	c := createJetStreamClusterWithTemplateAndModHook(t, jsClusterTempl, "C", 3,
		func(serverName, clusterName, storeDir, conf string) string {
			switch serverName {
			case "S-1":
				return fmt.Sprintf("%s\nserver_tags: [server:%s, intersect, %s]", conf, serverName, jsExcludePlacement)
			case "S-2":
				return fmt.Sprintf("%s\nserver_tags: [server:%s, intersect]", conf, serverName)
			default:
				return fmt.Sprintf("%s\nserver_tags: [server:%s]", conf, serverName)
			}
		})
	defer c.shutdown()

	nc, js := jsClientConnect(t, c.randomServer())
	defer nc.Close()

	for i, c := range []nats.StreamConfig{
		{Replicas: 1, Placement: &nats.Placement{Tags: []string{"server:S-1"}}},
		{Replicas: 2, Placement: &nats.Placement{Tags: []string{"intersect"}}},
		{Replicas: 3}, // not enough server without !jetstream
	} {
		c.Name = fmt.Sprintf("TEST%d", i)
		c.Subjects = []string{c.Name}
		_, err := js.AddStream(&c)
		require_Error(t, err)
		require_Contains(t, err.Error(), "no suitable peers for placement", "exclude tag set")
	}

	// Test update failure
	cfg := &nats.StreamConfig{Name: "TEST", Subjects: []string{"foo"}, Replicas: 2}
	_, err := js.AddStream(cfg)
	require_NoError(t, err)

	cfg.Replicas = 3
	_, err = js.UpdateStream(cfg)
	require_Error(t, err)
	require_Contains(t, err.Error(), "no suitable peers for placement", "exclude tag set")
	// Test tag reload removing !jetstream tag, and allowing placement again

	srv := c.serverByName("S-1")

	v, err := srv.Varz(nil)
	require_NoError(t, err)
	require_True(t, v.Tags.Contains(jsExcludePlacement))
	content, err := os.ReadFile(srv.configFile)
	require_NoError(t, err)
	newContent := strings.ReplaceAll(string(content), fmt.Sprintf(", %s]", jsExcludePlacement), "]")
	changeCurrentConfigContentWithNewContent(t, srv.configFile, []byte(newContent))

	require_NoError(t, srv.Reload())
	srv.sendStatszUpdate()

	v, err = srv.Varz(nil)
	require_NoError(t, err)
	require_True(t, !v.Tags.Contains(jsExcludePlacement))

	checkFor(t, 20*time.Second, 500*time.Millisecond, func() error {
		for _, s := range c.servers {
			var err error
			s.nodeToInfo.Range(func(sn any, value any) bool {
				ni := value.(nodeInfo)
				if ni.name != "S-1" {
					return true
				}
				if ni.tags.Contains("!jetstream") {
					err = fmt.Errorf("%s still has jetstream exclude tag", sn)
					return false
				}
				return true
			})
			if err != nil {
				return err
			}
		}
		return nil
	})

	cfg.Replicas = 3
	_, err = js.UpdateStream(cfg)
	require_NoError(t, err)
}

func TestJetStreamClusterAccountPurge(t *testing.T) {
	sysKp, syspub := createKey(t)
	sysJwt := encodeClaim(t, jwt.NewAccountClaims(syspub), syspub)
	sysCreds := newUser(t, sysKp)
	accKp, accpub := createKey(t)
	accClaim := jwt.NewAccountClaims(accpub)
	accClaim.Limits.JetStreamLimits.DiskStorage = 1024 * 1024 * 5
	accClaim.Limits.JetStreamLimits.MemoryStorage = 1024 * 1024 * 5
	accJwt := encodeClaim(t, accClaim, accpub)
	accCreds := newUser(t, accKp)

	tmlp := `
		listen: 127.0.0.1:-1
		server_name: %s
		jetstream: {max_mem_store: 256MB, max_file_store: 2GB, store_dir: '%s'}
		leaf {
			listen: 127.0.0.1:-1
		}
		cluster {
			name: %s
			listen: 127.0.0.1:%d
			routes = [%s]
		}
	`
	c := createJetStreamClusterWithTemplateAndModHook(t, tmlp, "cluster", 3,
		func(serverName, clustername, storeDir, conf string) string {
			return conf + fmt.Sprintf(`
				operator: %s
				system_account: %s
				resolver: {
					type: full
					dir: '%s/jwt'
					timeout: "10ms"
				}`, ojwt, syspub, storeDir)
		})
	defer c.shutdown()

	c.waitOnLeader()

	updateJwt(t, c.randomServer().ClientURL(), sysCreds, sysJwt, 3)
	updateJwt(t, c.randomServer().ClientURL(), sysCreds, accJwt, 3)

	c.waitOnAccount(accpub)

	createTestData := func(t *testing.T) {
		nc, js := jsClientConnect(t, c.randomNonLeader(), nats.UserCredentials(accCreds))
		defer nc.Close()

		// Prevent 'nats: JetStream not enabled for account' when creating the first stream.
		c.waitOnAccount(accpub)

		_, err := js.AddStream(&nats.StreamConfig{
			Name:     "TEST1",
			Subjects: []string{"foo"},
			Replicas: 3,
		})
		require_NoError(t, err)
		c.waitOnStreamLeader(accpub, "TEST1")

		ci, err := js.AddConsumer("TEST1",
			&nats.ConsumerConfig{Durable: "DUR1",
				AckPolicy: nats.AckExplicitPolicy})
		require_NoError(t, err)
		require_True(t, ci.Config.Replicas == 0)

		ci, err = js.AddConsumer("TEST1",
			&nats.ConsumerConfig{Durable: "DUR2",
				AckPolicy: nats.AckExplicitPolicy,
				Replicas:  1})
		require_NoError(t, err)
		require_True(t, ci.Config.Replicas == 1)

		toSend := uint64(1_000)
		for i := uint64(0); i < toSend; i++ {
			_, err = js.Publish("foo", nil)
			require_NoError(t, err)
		}

		_, err = js.AddStream(&nats.StreamConfig{
			Name:     "TEST2",
			Subjects: []string{"bar"},
			Replicas: 1,
		})
		require_NoError(t, err)

		ci, err = js.AddConsumer("TEST2",
			&nats.ConsumerConfig{Durable: "DUR1",
				AckPolicy: nats.AckExplicitPolicy,
				Replicas:  0})
		require_NoError(t, err)
		require_True(t, ci.Config.Replicas == 0)

		for i := uint64(0); i < toSend; i++ {
			_, err = js.Publish("bar", nil)
			require_NoError(t, err)
		}
	}

	inspectDirs := func(t *testing.T, sysTotal, accTotal int) error {
		t.Helper()
		sysDirs := 0
		accDirs := 0
		for _, s := range c.servers {
			files, err := os.ReadDir(filepath.Join(s.getOpts().StoreDir, "jetstream", syspub, "_js_"))
			require_NoError(t, err)
			sysDirs += len(files) - 1 // sub 1 for _meta_
			files, err = os.ReadDir(filepath.Join(s.getOpts().StoreDir, "jetstream", accpub, "streams"))
			if err == nil || err.(*os.PathError).Error() == "no such file or directory" {
				accDirs += len(files)
			}
		}
		if sysDirs != sysTotal || accDirs != accTotal {
			return fmt.Errorf("expected directory count does not match %d == %d, %d == %d",
				sysDirs, sysTotal, accDirs, accTotal)
		}
		return nil
	}

	checkForDirs := func(t *testing.T, sysTotal, accTotal int) {
		t.Helper()
		checkFor(t, 20*time.Second, 250*time.Millisecond, func() error {
			return inspectDirs(t, sysTotal, accTotal)
		})
	}

	purge := func(t *testing.T) {
		t.Helper()
		ncsys, err := nats.Connect(c.randomServer().ClientURL(), nats.UserCredentials(sysCreds))
		require_NoError(t, err)
		defer ncsys.Close()

		request := func() error {
			var resp JSApiAccountPurgeResponse
			m, err := ncsys.Request(fmt.Sprintf(JSApiAccountPurgeT, accpub), nil, time.Second)
			if err != nil {
				return err
			}
			if err := json.Unmarshal(m.Data, &resp); err != nil {
				return err
			}
			if !resp.Initiated {
				return fmt.Errorf("not started")
			}
			return nil
		}
		checkFor(t, 30*time.Second, 250*time.Millisecond, request)
	}

	t.Run("startup-cleanup", func(t *testing.T) {
		_, newCleanupAcc1 := createKey(t)
		_, newCleanupAcc2 := createKey(t)
		for _, s := range c.servers {
			os.MkdirAll(filepath.Join(s.getOpts().StoreDir, JetStreamStoreDir, newCleanupAcc1, streamsDir), defaultDirPerms)
			os.MkdirAll(filepath.Join(s.getOpts().StoreDir, JetStreamStoreDir, newCleanupAcc2), defaultDirPerms)
		}
		createTestData(t)
		checkForDirs(t, 6, 4)
		c.stopAll()
		c.restartAll()
		for _, s := range c.servers {
			accDir := filepath.Join(s.getOpts().StoreDir, JetStreamStoreDir, newCleanupAcc1)
			_, e := os.Stat(filepath.Join(accDir, streamsDir))
			require_Error(t, e)
			require_True(t, os.IsNotExist(e))
			_, e = os.Stat(accDir)
			require_Error(t, e)
			require_True(t, os.IsNotExist(e))
			_, e = os.Stat(filepath.Join(s.getOpts().StoreDir, JetStreamStoreDir, newCleanupAcc2))
			require_Error(t, e)
			require_True(t, os.IsNotExist(e))
		}
		checkForDirs(t, 6, 4)
		// Make sure we have a leader for all assets before moving to the next test
		c.waitOnStreamLeader(accpub, "TEST1")
		c.waitOnConsumerLeader(accpub, "TEST1", "DUR1")
		c.waitOnConsumerLeader(accpub, "TEST1", "DUR2")
		c.waitOnStreamLeader(accpub, "TEST2")
		c.waitOnConsumerLeader(accpub, "TEST2", "DUR1")
	})

	t.Run("purge-with-restart", func(t *testing.T) {
		createTestData(t)
		checkForDirs(t, 6, 4)
		purge(t)
		checkForDirs(t, 0, 0)
		c.stopAll()
		c.restartAll()
		checkForDirs(t, 0, 0)
	})

	t.Run("purge-with-reuse", func(t *testing.T) {
		createTestData(t)
		checkForDirs(t, 6, 4)
		purge(t)
		checkForDirs(t, 0, 0)
		createTestData(t)
		checkForDirs(t, 6, 4)
		purge(t)
		checkForDirs(t, 0, 0)
	})

	t.Run("purge-deleted-account", func(t *testing.T) {
		createTestData(t)
		checkForDirs(t, 6, 4)
		c.stopAll()
		for _, s := range c.servers {
			require_NoError(t, os.Remove(s.getOpts().StoreDir+"/jwt/"+accpub+".jwt"))
		}
		c.restartAll()
		checkForDirs(t, 6, 4)
		purge(t)
		checkForDirs(t, 0, 0)
		c.stopAll()
		c.restartAll()
		checkForDirs(t, 0, 0)
	})
}

func TestJetStreamClusterScaleConsumer(t *testing.T) {
	c := createJetStreamClusterWithTemplate(t, jsClusterTempl, "C", 3)
	defer c.shutdown()

	srv := c.randomNonLeader()
	nc, js := jsClientConnect(t, srv)
	defer nc.Close()

	si, err := js.AddStream(&nats.StreamConfig{
		Name:     "TEST",
		Subjects: []string{"foo"},
		Replicas: 3,
	})
	require_NoError(t, err)

	durCfg := &nats.ConsumerConfig{Durable: "DUR", AckPolicy: nats.AckExplicitPolicy}
	ci, err := js.AddConsumer("TEST", durCfg)
	require_NoError(t, err)
	require_True(t, ci.Config.Replicas == 0)

	toSend := uint64(1_000)
	for i := uint64(0); i < toSend; i++ {
		_, err = js.Publish("foo", nil)
		require_NoError(t, err)
	}

	s, err := js.PullSubscribe("foo", "DUR")
	require_NoError(t, err)

	consumeOne := func(expSeq uint64) error {
		if ci, err := js.ConsumerInfo("TEST", "DUR"); err != nil {
			return err
		} else if ci.Delivered.Stream != expSeq {
			return fmt.Errorf("pre: not expected delivered stream %d, got %d", expSeq, ci.Delivered.Stream)
		} else if ci.Delivered.Consumer != expSeq {
			return fmt.Errorf("pre: not expected delivered consumer %d, got %d", expSeq, ci.Delivered.Consumer)
		} else if ci.AckFloor.Stream != expSeq {
			return fmt.Errorf("pre: not expected ack stream %d, got %d", expSeq, ci.AckFloor.Stream)
		} else if ci.AckFloor.Consumer != expSeq {
			return fmt.Errorf("pre: not expected ack consumer %d, got %d", expSeq, ci.AckFloor.Consumer)
		}
		if m, err := s.Fetch(1); err != nil {
			return err
		} else if err := m[0].AckSync(); err != nil {
			return err
		}
		expSeq = expSeq + 1
		if ci, err := js.ConsumerInfo("TEST", "DUR"); err != nil {
			return err
		} else if ci.Delivered.Stream != expSeq {
			return fmt.Errorf("post: not expected delivered stream %d, got %d", expSeq, ci.Delivered.Stream)
		} else if ci.Delivered.Consumer != expSeq {
			return fmt.Errorf("post: not expected delivered consumer %d, got %d", expSeq, ci.Delivered.Consumer)
		} else if ci.AckFloor.Stream != expSeq {
			return fmt.Errorf("post: not expected ack stream %d, got %d", expSeq, ci.AckFloor.Stream)
		} else if ci.AckFloor.Consumer != expSeq {
			return fmt.Errorf("post: not expected ack consumer %d, got %d", expSeq, ci.AckFloor.Consumer)
		}
		return nil
	}

	require_NoError(t, consumeOne(0))

	// scale down, up, down and up to default == 3 again
	for i, r := range []int{1, 3, 1, 0} {
		durCfg.Replicas = r
		if r == 0 {
			r = si.Config.Replicas
		}
		_, err = js.UpdateConsumer("TEST", durCfg)
		require_NoError(t, err)

		checkFor(t, time.Second*30, time.Millisecond*250, func() error {
			if ci, err = js.ConsumerInfo("TEST", "DUR"); err != nil {
				return err
			} else if ci.Cluster == nil {
				return errors.New("no cluster info")
			} else if ci.Cluster.Leader == _EMPTY_ {
				return errors.New("no leader")
			} else if len(ci.Cluster.Replicas) != r-1 {
				return fmt.Errorf("not enough replica, got %d wanted %d", len(ci.Cluster.Replicas), r-1)
			} else {
				for _, r := range ci.Cluster.Replicas {
					if !r.Current || r.Offline || r.Lag != 0 {
						return fmt.Errorf("replica %s not current %t offline %t lag %d", r.Name, r.Current, r.Offline, r.Lag)
					}
				}
			}
			return nil
		})
		c.waitOnConsumerLeader(globalAccountName, "TEST", "DUR")

		require_NoError(t, consumeOne(uint64(i+1)))
	}
}

func TestJetStreamClusterConsumerScaleUp(t *testing.T) {
	c := createJetStreamCluster(t, jsClusterTempl, "HUB", _EMPTY_, 3, 22020, true)
	defer c.shutdown()

	// Client based API
	srv := c.randomNonLeader()
	nc, js := jsClientConnect(t, srv)
	defer nc.Close()

	scfg := nats.StreamConfig{
		Name:     "TEST",
		Subjects: []string{"foo"},
		Replicas: 1,
	}
	_, err := js.AddStream(&scfg)
	require_NoError(t, err)
	defer js.DeleteStream("TEST")

	dcfg := nats.ConsumerConfig{
		Durable:   "DUR",
		AckPolicy: nats.AckExplicitPolicy,
		Replicas:  0}
	_, err = js.AddConsumer("TEST", &dcfg)
	require_NoError(t, err)

	for i := 0; i < 100; i++ {
		_, err = js.Publish("foo", nil)
		require_NoError(t, err)
	}

	scfg.Replicas = 2
	_, err = js.UpdateStream(&scfg)
	require_NoError(t, err)

	// The scale up issue shows itself as permanent loss of consumer leadership
	// So give it some time for the change to propagate to new consumer peers and the quorum to disrupt
	// 2 seconds is a value arrived by experimentally, no sleep or a sleep of 1sec always had the test pass a lot.
	time.Sleep(2 * time.Second)

	c.waitOnStreamLeader("$G", "TEST")

	// There is also a timing component to the issue triggering.
	c.waitOnConsumerLeader("$G", "TEST", "DUR")
}

func TestJetStreamClusterStreamScaleFromAndToR1SkipsTeardown(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "R3S", 3)
	defer c.shutdown()

	nc, js := jsClientConnect(t, c.randomServer())
	defer nc.Close()

	_, err := js.AddStream(&nats.StreamConfig{Name: "ORIGIN", Subjects: []string{"foo"}, Replicas: 1})
	require_NoError(t, err)

	cfg := &nats.StreamConfig{
		Name:     "S",
		Sources:  []*nats.StreamSource{{Name: "ORIGIN"}},
		Replicas: 1,
	}
	_, err = js.AddStream(cfg)
	require_NoError(t, err)

	for range 10 {
		_, err = js.Publish("foo", nil)
		require_NoError(t, err)
	}

	sl := c.streamLeader(globalAccountName, "S")
	mset, err := sl.GlobalAccount().lookupStream("S")
	require_NoError(t, err)

	// Wait for the source consumer to be running and capture its subscription as
	// a canary. A setLeader teardown stops the source consumers and would have
	// to recreate this subscription.
	var canary *subscription
	checkFor(t, 2*time.Second, 100*time.Millisecond, func() error {
		mset.mu.RLock()
		defer mset.mu.RUnlock()
		for _, si := range mset.sources {
			if si.sub != nil {
				canary = si.sub
				return nil
			}
		}
		return fmt.Errorf("source consumer not active yet")
	})

	// The R1 leader stores the coerced term 1.
	mset.mu.RLock()
	term := mset.term
	mset.mu.RUnlock()
	require_Equal(t, term, 1)

	// Scale up. The same server is preferred to win the election, at term 1,
	// which means setLeader skips the teardown.
	cfg.Replicas = 3
	_, err = js.UpdateStream(cfg)
	require_NoError(t, err)
	c.waitOnStreamLeader(globalAccountName, "S")
	require_True(t, c.streamLeader(globalAccountName, "S") == sl)

	checkFor(t, 2*time.Second, 100*time.Millisecond, func() error {
		mset.mu.RLock()
		defer mset.mu.RUnlock()
		if mset.node == nil {
			return fmt.Errorf("no raft node yet")
		}
		if mset.term != 1 {
			return fmt.Errorf("expected mset.term 1, got %d", mset.term)
		}
		// Although the teardown was skipped, the leader must have subscribed to
		// respond to sync requests now that it's clustered.
		if mset.syncSub == nil {
			return fmt.Errorf("expected sync subscription")
		}
		return nil
	})

	// The source consumer must not have been torn down and recreated.
	mset.mu.RLock()
	var sub *subscription
	for _, si := range mset.sources {
		sub = si.sub
	}
	mset.mu.RUnlock()
	require_True(t, sub == canary)

	// Messages flowing in through the source must replicate to the new followers.
	for range 10 {
		_, err = js.Publish("foo", nil)
		require_NoError(t, err)
	}
	checkFor(t, 2*time.Second, 100*time.Millisecond, func() error {
		state, err := checkStateAndErr(t, c, globalAccountName, "S")
		if err != nil {
			return err
		}
		if state.Msgs != 20 {
			return fmt.Errorf("expected 20 messages, got %d", state.Msgs)
		}
		return nil
	})

	// Move stream leadership away so the term moves past 1.
	_, err = nc.Request(fmt.Sprintf(JSApiStreamLeaderStepDownT, "S"), nil, time.Second)
	require_NoError(t, err)
	c.waitOnStreamLeader(globalAccountName, "S")

	// Re-capture the canary on the new leader, which was elected at term > 1.
	nl := c.streamLeader(globalAccountName, "S")
	mset, err = nl.globalAccount().lookupStream("S")
	require_NoError(t, err)
	checkFor(t, 2*time.Second, 100*time.Millisecond, func() error {
		mset.mu.RLock()
		defer mset.mu.RUnlock()
		if mset.term <= 1 {
			return fmt.Errorf("expected mset.term > 1, got %d", mset.term)
		}
		canary = nil
		for _, si := range mset.sources {
			if si.sub != nil {
				canary = si.sub
			}
		}
		if canary == nil {
			return fmt.Errorf("source consumer not active yet")
		}
		return nil
	})

	// Scale back down. The current leader is always retained, and although it was
	// elected at term > 1 it remains the continuing leader, so setLeader skips the
	// teardown and stores the coerced term 1.
	cfg.Replicas = 1
	_, err = js.UpdateStream(cfg)
	require_NoError(t, err)
	c.waitOnStreamLeader(globalAccountName, "S")
	require_True(t, c.streamLeader(globalAccountName, "S") == nl)

	checkFor(t, 2*time.Second, 100*time.Millisecond, func() error {
		mset.mu.RLock()
		defer mset.mu.RUnlock()
		if mset.node != nil {
			return fmt.Errorf("raft node not removed yet")
		}
		if mset.term != 1 {
			return fmt.Errorf("expected mset.term 1, got %d", mset.term)
		}
		return nil
	})

	// The source consumer must still not have been torn down and recreated.
	mset.mu.RLock()
	sub = nil
	for _, si := range mset.sources {
		sub = si.sub
	}
	mset.mu.RUnlock()
	require_True(t, sub == canary)

	// Messages flowing in through the source must still be stored after the scale-down.
	for range 10 {
		_, err = js.Publish("foo", nil)
		require_NoError(t, err)
	}
	checkFor(t, 2*time.Second, 100*time.Millisecond, func() error {
		state, err := checkStateAndErr(t, c, globalAccountName, "S")
		if err != nil {
			return err
		}
		if state.Msgs != 30 {
			return fmt.Errorf("expected 30 messages, got %d", state.Msgs)
		}
		return nil
	})
}

func TestJetStreamClusterConsumerScaleFromAndToR1SkipsTeardown(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "R3S", 3)
	defer c.shutdown()

	nc, js := jsClientConnect(t, c.randomServer())
	defer nc.Close()

	_, err := js.AddStream(&nats.StreamConfig{Name: "TEST", Subjects: []string{"foo"}, Replicas: 3})
	require_NoError(t, err)

	cfg := &nats.ConsumerConfig{Durable: "DUR", AckPolicy: nats.AckExplicitPolicy, Replicas: 1}
	_, err = js.AddConsumer("TEST", cfg)
	require_NoError(t, err)

	for range 20 {
		_, err = js.Publish("foo", nil)
		require_NoError(t, err)
	}

	sub, err := js.PullSubscribe("foo", "DUR")
	require_NoError(t, err)
	msgs, err := sub.Fetch(10)
	require_NoError(t, err)
	require_Len(t, len(msgs), 10)
	for _, m := range msgs {
		require_NoError(t, m.AckSync())
	}

	cl := c.consumerLeader(globalAccountName, "TEST", "DUR")
	mset, err := cl.globalAccount().lookupStream("TEST")
	require_NoError(t, err)
	o := mset.lookupConsumer("DUR")
	require_NotNil(t, o)

	// Capture the ack subscription as a canary. A setLeader teardown would have
	// to unsubscribe and recreate it. The R1 leader stores the coerced term 1.
	o.mu.RLock()
	canary, term := o.ackSub, o.term
	o.mu.RUnlock()
	require_NotNil(t, canary)
	require_Equal(t, term, 1)

	// Scale up. The same server is preferred to win the election, at term 1,
	// which means setLeader skips the teardown.
	cfg.Replicas = 3
	_, err = js.UpdateConsumer("TEST", cfg)
	require_NoError(t, err)

	// The scale-up must have spun up the proposal loop, without tearing down
	// the running consumer.
	checkFor(t, 2*time.Second, 100*time.Millisecond, func() error {
		o.mu.RLock()
		defer o.mu.RUnlock()
		if o.node == nil {
			return fmt.Errorf("no raft node yet")
		}
		if o.term != 1 {
			return fmt.Errorf("expected o.term 1, got %d", o.term)
		}
		if o.pch == nil {
			return fmt.Errorf("no proposal channel yet")
		}
		return nil
	})
	o.mu.RLock()
	same := o.ackSub == canary
	o.mu.RUnlock()
	require_True(t, same)

	// Acks after the scale-up must be replicated to the new followers.
	msgs, err = sub.Fetch(10)
	require_NoError(t, err)
	require_Len(t, len(msgs), 10)
	for _, m := range msgs {
		require_NoError(t, m.AckSync())
	}

	// Move consumer leadership away so the term moves past 1.
	_, err = nc.Request(fmt.Sprintf(JSApiConsumerLeaderStepDownT, "TEST", "DUR"), nil, 5*time.Second)
	require_NoError(t, err)
	c.waitOnConsumerLeader(globalAccountName, "TEST", "DUR")

	// The new leader must agree on the state that was proposed since the scale-up.
	checkFor(t, 2*time.Second, 100*time.Millisecond, func() error {
		ci, err := js.ConsumerInfo("TEST", "DUR")
		if err != nil {
			return err
		}
		if ci.AckFloor.Consumer != 20 {
			return fmt.Errorf("ack floor %d, expected 20", ci.AckFloor.Consumer)
		}
		if ci.NumPending != 0 {
			return fmt.Errorf("num pending %d, expected 0", ci.NumPending)
		}
		return nil
	})

	// Re-capture the canary on the new leader, which was elected at term > 1.
	cl = c.consumerLeader(globalAccountName, "TEST", "DUR")
	mset, err = cl.globalAccount().lookupStream("TEST")
	require_NoError(t, err)
	o = mset.lookupConsumer("DUR")
	require_NotNil(t, o)
	checkFor(t, 2*time.Second, 100*time.Millisecond, func() error {
		o.mu.RLock()
		defer o.mu.RUnlock()
		if o.term <= 1 {
			return fmt.Errorf("expected o.term > 1, got %d", o.term)
		}
		if o.ackSub == nil {
			return fmt.Errorf("no ack subscription yet")
		}
		return nil
	})
	o.mu.RLock()
	canary = o.ackSub
	o.mu.RUnlock()

	// Scale back down to R1. The current leader is always retained, and although
	// it was elected at term > 1 it remains the continuing leader, so setLeader
	// skips the teardown, clears the proposal channel/loop, and stores the
	// coerced term 1.
	cfg.Replicas = 1
	_, err = js.UpdateConsumer("TEST", cfg)
	require_NoError(t, err)
	checkFor(t, 2*time.Second, 100*time.Millisecond, func() error {
		o.mu.RLock()
		defer o.mu.RUnlock()
		if o.node != nil {
			return fmt.Errorf("raft node not removed yet")
		}
		if o.pch != nil {
			return fmt.Errorf("proposal channel not cleared yet")
		}
		if o.term != 1 {
			return fmt.Errorf("expected o.term 1, got %d", o.term)
		}
		return nil
	})
	o.mu.RLock()
	same = o.ackSub == canary
	o.mu.RUnlock()
	require_True(t, same)
}

func TestJetStreamClusterPeerOffline(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "R5S", 5)
	defer c.shutdown()

	ml := c.leader()
	rs := c.randomNonLeader()

	checkPeer := func(ml, rs *Server, shouldBeOffline bool) {
		t.Helper()

		checkFor(t, 5*time.Second, 50*time.Millisecond, func() error {
			var found bool
			for _, s := range ml.JetStreamClusterPeers() {
				if s == rs.Name() {
					found = true
					break
				}
			}
			if !shouldBeOffline && !found {
				return fmt.Errorf("Server %q not in the peers list", rs.Name())
			} else if shouldBeOffline && found {
				return fmt.Errorf("Server %q should not be in the peers list", rs.Name())
			}

			var ok bool
			ml.nodeToInfo.Range(func(k, v any) bool {
				if si := v.(nodeInfo); si.name == rs.Name() {
					if shouldBeOffline && si.offline || !shouldBeOffline && !si.offline {
						ok = true
						return false
					}
				}
				return true
			})
			if !ok {
				if shouldBeOffline {
					return fmt.Errorf("Server %q should be marked as online", rs.Name())
				}
				return fmt.Errorf("Server %q is still marked as online", rs.Name())
			}
			return nil
		})
	}

	// Shutdown the server and make sure that it is now showing as offline.
	rs.Shutdown()
	checkPeer(ml, rs, true)

	// Now restart that server and check that is no longer offline.
	oldrs := rs
	rs, _ = RunServerWithConfig(rs.getOpts().ConfigFile)
	defer rs.Shutdown()

	// Replaced old with new server
	for i := 0; i < len(c.servers); i++ {
		if c.servers[i] == oldrs {
			c.servers[i] = rs
		}
	}

	// Wait for cluster to be formed
	checkClusterFormed(t, c.servers...)

	// Make sure that we have a leader (there can always be a re-election)
	c.waitOnLeader()
	ml = c.leader()

	// Now check that rs is not offline
	checkPeer(ml, rs, false)
}

func TestJetStreamClusterNoQuorumStepdown(t *testing.T) {
	lqi := lostQuorumInterval
	lostQuorumInterval = 2 * time.Second
	defer func() { lostQuorumInterval = lqi }()

	c := createJetStreamClusterExplicit(t, "R3S", 3)
	defer c.shutdown()

	// Client based API
	s := c.randomServer()
	nc, js := jsClientConnect(t, s)
	defer nc.Close()

	// Setup subscription for leader elected.
	lesub, err := nc.SubscribeSync(JSAdvisoryStreamLeaderElectedPre + ".*")
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	if _, err := js.AddStream(&nats.StreamConfig{Name: "NO-Q", Replicas: 2}); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	// Make sure we received our leader elected advisory.
	leadv, _ := lesub.NextMsg(0)
	if leadv == nil {
		t.Fatalf("Expected to receive a leader elected advisory")
	}
	var le JSStreamLeaderElectedAdvisory
	if err := json.Unmarshal(leadv.Data, &le); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if ln := c.streamLeader("$G", "NO-Q").Name(); le.Leader != ln {
		t.Fatalf("Expected to have leader %q in elect advisory, got %q", ln, le.Leader)
	}

	payload := []byte("Hello JSC")
	for i := 0; i < 10; i++ {
		if _, err := js.Publish("NO-Q", payload); err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
	}

	// Setup subscription for leader elected.
	clesub, err := nc.SubscribeSync(JSAdvisoryConsumerLeaderElectedPre + ".*.*")
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	// Make durable to have R match Stream.
	sub, err := js.SubscribeSync("NO-Q", nats.Durable("rr"))
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	ci, err := sub.ConsumerInfo()
	if err != nil || ci == nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	// Make sure we received our consumer leader elected advisory.
	leadv, _ = clesub.NextMsg(0)
	if leadv == nil {
		t.Fatalf("Expected to receive a consumer leader elected advisory")
	}

	// Shutdown the non-leader.
	c.randomNonStreamLeader("$G", "NO-Q").Shutdown()

	// This should eventually have us stepdown as leader since we would have lost quorum with R=2.
	checkFor(t, 5*time.Second, 500*time.Millisecond, func() error {
		if sl := c.streamLeader("$G", "NO-Q"); sl == nil {
			return nil
		}
		return fmt.Errorf("Still have leader for stream")
	})

	notAvailableErr := func(err error) bool {
		return err != nil && (strings.Contains(err.Error(), "unavailable") || err == context.DeadlineExceeded)
	}

	checkFor(t, 2*time.Second, 100*time.Millisecond, func() error {
		if cl := c.consumerLeader("$G", "NO-Q", ci.Name); cl == nil {
			return nil
		}
		return fmt.Errorf("Still have leader for consumer")
	})

	if _, err = js.ConsumerInfo("NO-Q", ci.Name); !notAvailableErr(err) {
		t.Fatalf("Expected an 'unavailable' error, got %v", err)
	}
	if _, err := sub.ConsumerInfo(); !notAvailableErr(err) {
		t.Fatalf("Expected an 'unavailable' error, got %v", err)
	}

	// Now let's take out the other non meta-leader
	// We should get same error for general API calls.
	c.randomNonLeader().Shutdown()
	c.expectNoLeader()

	// Now make sure the general JS API responds with system unavailable.
	if _, err = js.AccountInfo(); !notAvailableErr(err) {
		t.Fatalf("Expected an 'unavailable' error, got %v", err)
	}
	if _, err := js.AddStream(&nats.StreamConfig{Name: "NO-Q33", Replicas: 2}); !notAvailableErr(err) {
		t.Fatalf("Expected an 'unavailable' error, got %v", err)
	}
	if _, err := js.UpdateStream(&nats.StreamConfig{Name: "NO-Q33", Replicas: 2}); !notAvailableErr(err) {
		t.Fatalf("Expected an 'unavailable' error, got %v", err)
	}
	if err := js.DeleteStream("NO-Q"); !notAvailableErr(err) {
		t.Fatalf("Expected an 'unavailable' error, got %v", err)
	}
	if err := js.PurgeStream("NO-Q"); !notAvailableErr(err) {
		t.Fatalf("Expected an 'unavailable' error, got %v", err)
	}
	if err := js.DeleteMsg("NO-Q", 1); !notAvailableErr(err) {
		t.Fatalf("Expected an 'unavailable' error, got %v", err)
	}
	// Consumer
	if _, err := js.AddConsumer("NO-Q", &nats.ConsumerConfig{Durable: "dlc", AckPolicy: nats.AckExplicitPolicy}); !notAvailableErr(err) {
		t.Fatalf("Expected an 'unavailable' error, got %v", err)
	}
	if err := js.DeleteConsumer("NO-Q", "dlc"); !notAvailableErr(err) {
		t.Fatalf("Expected an 'unavailable' error, got %v", err)
	}
	if _, err := js.ConsumerInfo("NO-Q", "dlc"); !notAvailableErr(err) {
		t.Fatalf("Expected an 'unavailable' error, got %v", err)
	}
	// Listers
	for info := range js.StreamsInfo() {
		t.Fatalf("Unexpected stream info, got %v", info)
	}
	for info := range js.ConsumersInfo("NO-Q") {
		t.Fatalf("Unexpected consumer info, got %v", info)
	}
}

func TestJetStreamClusterCreateResponseAdvisoriesHaveSubject(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "R3S", 3)
	defer c.shutdown()

	// Client based API
	s := c.randomServer()
	nc, js := jsClientConnect(t, s)
	defer nc.Close()

	sub, err := nc.SubscribeSync("$JS.EVENT.ADVISORY.API")
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	defer sub.Unsubscribe()

	if _, err := js.AddStream(&nats.StreamConfig{Name: "TEST", Replicas: 2}); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if _, err := js.SubscribeSync("TEST", nats.Durable("DLC")); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if err := js.PurgeStream("TEST"); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if err := js.DeleteStream("TEST"); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	checkSubsPending(t, sub, 6)

	for m, err := sub.NextMsg(0); err == nil; m, err = sub.NextMsg(0) {
		var audit JSAPIAudit
		if err := json.Unmarshal(m.Data, &audit); err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		if audit.Subject == _EMPTY_ {
			t.Fatalf("Expected subject, got nothing")
		}
	}
}

func TestJetStreamClusterRestartAndRemoveAdvisories(t *testing.T) {
	// FIXME(dlc) - Flaky on Travis, skip for now.
	skip(t)

	c := createJetStreamClusterExplicit(t, "R3S", 3)
	defer c.shutdown()

	// Client based API
	s := c.randomServer()
	nc, js := jsClientConnect(t, s)
	defer nc.Close()

	sub, err := nc.SubscribeSync("$JS.EVENT.ADVISORY.API")
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	defer sub.Unsubscribe()

	csub, err := nc.SubscribeSync("$JS.EVENT.ADVISORY.*.CREATED.>")
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	defer csub.Unsubscribe()
	nc.Flush()

	sendBatch := func(subject string, n int) {
		t.Helper()
		for i := 0; i < n; i++ {
			if _, err := js.Publish(subject, []byte("JSC-OK")); err != nil {
				t.Fatalf("Unexpected publish error: %v", err)
			}
		}
	}

	// Add in some streams with msgs and consumers.
	if _, err := js.AddStream(&nats.StreamConfig{Name: "TEST-1", Replicas: 2}); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if _, err := js.SubscribeSync("TEST-1", nats.Durable("DC")); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	sendBatch("TEST-1", 25)

	if _, err := js.AddStream(&nats.StreamConfig{Name: "TEST-2", Replicas: 2}); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if _, err := js.SubscribeSync("TEST-2", nats.Durable("DC")); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	sendBatch("TEST-2", 50)

	if _, err := js.AddStream(&nats.StreamConfig{Name: "TEST-3", Replicas: 3, Storage: nats.MemoryStorage}); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if _, err := js.SubscribeSync("TEST-3", nats.Durable("DC")); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	sendBatch("TEST-3", 100)

	drainSub := func(sub *nats.Subscription) {
		for _, err := sub.NextMsg(0); err == nil; _, err = sub.NextMsg(0) {
		}
	}

	// Wait for the advisories for all streams and consumers.
	checkSubsPending(t, sub, 12) // 3 streams, 3*2 consumers, 3 stream names lookups for creating consumers.
	drainSub(sub)

	// Created audit events.
	checkSubsPending(t, csub, 6)
	drainSub(csub)

	usub, err := nc.SubscribeSync("$JS.EVENT.ADVISORY.*.UPDATED.>")
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	defer usub.Unsubscribe()
	nc.Flush()

	checkSubsPending(t, csub, 0)
	checkSubsPending(t, sub, 0)
	checkSubsPending(t, usub, 0)

	// Now restart the other two servers we are not connected to.
	for _, cs := range c.servers {
		if cs != s {
			cs.Shutdown()
			c.restartServer(cs)
		}
	}
	c.waitOnAllCurrent()

	checkSubsPending(t, csub, 0)
	checkSubsPending(t, sub, 0)
	checkSubsPending(t, usub, 0)

	dsub, err := nc.SubscribeSync("$JS.EVENT.ADVISORY.*.DELETED.>")
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	defer dsub.Unsubscribe()
	nc.Flush()

	c.waitOnConsumerLeader("$G", "TEST-1", "DC")
	c.waitOnLeader()

	// Now check delete advisories as well.
	if err := js.DeleteConsumer("TEST-1", "DC"); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	checkSubsPending(t, csub, 0)
	checkSubsPending(t, dsub, 1)
	checkSubsPending(t, sub, 1)
	checkSubsPending(t, usub, 0)
	drainSub(dsub)

	if err := js.DeleteStream("TEST-3"); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	checkSubsPending(t, dsub, 2) // Stream and the consumer underneath.
	checkSubsPending(t, sub, 2)
}

func TestJetStreamClusterNoDuplicateOnNodeRestart(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "ND", 2)
	defer c.shutdown()

	// Client based API
	s := c.randomServer()
	nc, js := jsClientConnect(t, s)
	defer nc.Close()

	_, err := js.AddStream(&nats.StreamConfig{
		Name:     "TEST",
		Subjects: []string{"foo"},
	})
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	sl := c.streamLeader("$G", "TEST")
	if s == sl {
		nc.Close()
		nc, js = jsClientConnect(t, s)
		defer nc.Close()
	}

	sub, err := js.SubscribeSync("foo", nats.Durable("dlc"))
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	js.Publish("foo", []byte("msg1"))
	if m, err := sub.NextMsg(time.Second); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	} else {
		m.AckSync()
	}

	sl.Shutdown()
	c.restartServer(sl)
	c.waitOnStreamLeader("$G", "TEST")
	c.waitOnConsumerLeader("$G", "TEST", "dlc")

	// Send second msg
	js.Publish("foo", []byte("msg2"))
	msg, err := sub.NextMsg(5 * time.Second)
	if err != nil {
		t.Fatalf("Error getting message: %v", err)
	}
	if string(msg.Data) != "msg2" {
		t.Fatalf("Unexpected message: %s", msg.Data)
	}
	msg.AckSync()

	// Make sure we don't get a duplicate.
	msg, err = sub.NextMsg(250 * time.Millisecond)
	if err == nil {
		t.Fatalf("Should have gotten an error, got %s", msg.Data)
	}
}

func TestJetStreamClusterNoDupePeerSelection(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "NDP", 3)
	defer c.shutdown()

	// Client based API
	s := c.randomServer()
	nc, js := jsClientConnect(t, s)
	defer nc.Close()

	// Create 10 streams. Make sure none of them have a replica
	// that is the same as the leader.
	for i := 1; i <= 10; i++ {
		si, err := js.AddStream(&nats.StreamConfig{
			Name:     fmt.Sprintf("TEST-%d", i),
			Replicas: 3,
		})
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		if si.Cluster == nil || si.Cluster.Leader == "" || len(si.Cluster.Replicas) != 2 {
			t.Fatalf("Unexpected cluster state for stream info: %+v\n", si.Cluster)
		}
		// Make sure that the replicas are not same as the leader.
		for _, pi := range si.Cluster.Replicas {
			if pi.Name == si.Cluster.Leader {
				t.Fatalf("Found replica that is same as leader, meaning 2 nodes placed on same server")
			}
		}
		// Now do a consumer and check same thing.
		sub, err := js.SubscribeSync(si.Config.Name)
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		ci, err := sub.ConsumerInfo()
		if err != nil {
			t.Fatalf("Unexpected error getting consumer info: %v", err)
		}
		for _, pi := range ci.Cluster.Replicas {
			if pi.Name == ci.Cluster.Leader {
				t.Fatalf("Found replica that is same as leader, meaning 2 nodes placed on same server")
			}
		}
	}
}

func TestJetStreamClusterStreamRemovePeer(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "RNS", 5)
	defer c.shutdown()

	// Client based API
	s := c.randomServer()
	nc, js := jsClientConnect(t, s)
	defer nc.Close()

	_, err := js.AddStream(&nats.StreamConfig{Name: "TEST", Replicas: 3})
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	// Send in 10 messages.
	msg, toSend := []byte("Hello JS Clustering"), 10
	for i := 0; i < toSend; i++ {
		if _, err = js.Publish("TEST", msg); err != nil {
			t.Fatalf("Unexpected publish error: %v", err)
		}
	}

	sub, err := js.SubscribeSync("TEST", nats.Durable("cat"))
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	checkSubsPending(t, sub, toSend)

	// Do ephemeral too.
	esub, err := js.SubscribeSync("TEST")
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	checkSubsPending(t, esub, toSend)

	_, err = js.AddConsumer("TEST", &nats.ConsumerConfig{Durable: "pull", AckPolicy: nats.AckNonePolicy})
	require_NoError(t, err)

	pullSub, err := js.PullSubscribe("TEST", "pull")
	require_NoError(t, err)

	// First fetch the messages that are already there.
	msgs, err := pullSub.Fetch(toSend, nats.MaxWait(500*time.Millisecond))
	require_NoError(t, err)
	require_Equal(t, toSend, len(msgs))

	// Now prepare a check to see if we get unwated `Consumer Deleted` error on peer remove.
	pullResults := make(chan error, 1)
	go func() {
		_, err := pullSub.Fetch(1, nats.MaxWait(30*time.Second))
		// Let's check if we get unwted `Consumer Deleted` error on peer remove.
		// Everything else is fine (Leader Changed, Timeout, etc.)
		if err != nats.ErrConsumerDeleted {
			close(pullResults)
		} else {
			pullResults <- err
		}
	}()

	ci, err := esub.ConsumerInfo()
	if err != nil {
		t.Fatalf("Could not fetch consumer info: %v", err)
	}
	// Capture ephemeral's server and name.
	es, en := ci.Cluster.Leader, ci.Name

	// Grab stream info.
	si, err := js.StreamInfo("TEST")
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	peers := []string{si.Cluster.Leader}
	for _, p := range si.Cluster.Replicas {
		peers = append(peers, p.Name)
	}
	// Pick a truly random server to remove.
	rand.Shuffle(len(peers), func(i, j int) { peers[i], peers[j] = peers[j], peers[i] })
	toRemove := peers[0]

	// First test bad peer.
	req := &JSApiStreamRemovePeerRequest{Peer: "NOT VALID"}
	jsreq, err := json.Marshal(req)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	// Need to call this by hand for now.
	resp, err := nc.Request(fmt.Sprintf(JSApiStreamRemovePeerT, "TEST"), jsreq, time.Second)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	var rpResp JSApiStreamRemovePeerResponse
	if err := json.Unmarshal(resp.Data, &rpResp); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if rpResp.Error == nil || !strings.Contains(rpResp.Error.Description, "peer not a member") {
		t.Fatalf("Expected error for bad peer, got %+v", rpResp.Error)
	}
	rpResp.Error = nil

	req = &JSApiStreamRemovePeerRequest{Peer: toRemove}
	jsreq, err = json.Marshal(req)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	resp, err = nc.Request(fmt.Sprintf(JSApiStreamRemovePeerT, "TEST"), jsreq, time.Second)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if err := json.Unmarshal(resp.Data, &rpResp); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if rpResp.Error != nil {
		t.Fatalf("Unexpected error: %+v", rpResp.Error)
	}

	c.waitOnStreamLeader("$G", "TEST")

	checkFor(t, 10*time.Second, 100*time.Millisecond, func() error {
		si, err := js.StreamInfo("TEST", nats.MaxWait(time.Second))
		if err != nil {
			return fmt.Errorf("Could not fetch stream info: %v", err)
		}
		if len(si.Cluster.Replicas) != 2 {
			return fmt.Errorf("Expected 2 replicas, got %d", len(si.Cluster.Replicas))
		}
		for _, peer := range si.Cluster.Replicas {
			if !peer.Current {
				return fmt.Errorf("Expected replica to be current: %+v", peer)
			}
		}
		if si.Cluster.Leader == toRemove {
			return fmt.Errorf("Peer not removed yet: %+v", toRemove)
		}
		for _, p := range si.Cluster.Replicas {
			if p.Name == toRemove {
				return fmt.Errorf("Peer not removed yet: %+v", toRemove)
			}
		}
		return nil
	})

	c.waitOnConsumerLeader("$G", "TEST", "cat")
	c.waitOnConsumerLeader("$G", "TEST", "pull")

	// Now check consumer info as well.
	checkFor(t, 10*time.Second, 100*time.Millisecond, func() error {
		ci, err := js.ConsumerInfo("TEST", "cat", nats.MaxWait(time.Second))
		if err != nil {
			return fmt.Errorf("Could not fetch consumer info: %v", err)
		}
		if len(ci.Cluster.Replicas) != 2 {
			return fmt.Errorf("Expected 2 replicas, got %d", len(ci.Cluster.Replicas))
		}
		for _, peer := range ci.Cluster.Replicas {
			if !peer.Current {
				return fmt.Errorf("Expected replica to be current: %+v", peer)
			}
		}
		if ci.Cluster.Leader == toRemove {
			return fmt.Errorf("Peer not removed yet: %+v", toRemove)
		}
		for _, p := range ci.Cluster.Replicas {
			if p.Name == toRemove {
				return fmt.Errorf("Peer not removed yet: %+v", toRemove)
			}
		}
		return nil
	})

	// Check if we got the `Consumer Deleted` error on the pull consumer.
	select {
	case err := <-pullResults:
		if err != nil {
			t.Fatalf("Expected timeout error or nil, got %v", err)
		}
	default:
	}

	// Now check ephemeral consumer info.
	// Make sure we did not stamp same new group into the ephemeral where R=1.
	ci, err = esub.ConsumerInfo()
	// If the leader was same as what we just removed, this should fail.
	if es == toRemove {
		if err != nats.ErrConsumerNotFound {
			t.Fatalf("Expected a not found error, got %v", err)
		}
		// Also make sure this was removed all together.
		// We may proactively move things in the future.
		for cn := range js.ConsumerNames("TEST") {
			if cn == en {
				t.Fatalf("Expected ephemeral consumer to be deleted since we removed its only peer")
			}
		}
	} else {
		if err != nil {
			t.Fatalf("Could not fetch consumer info: %v", err)
		}
		if len(ci.Cluster.Replicas) != 0 {
			t.Fatalf("Expected no replicas for ephemeral, got %d", len(ci.Cluster.Replicas))
		}
	}

	// Confirm we can peer-remove by the peer ID instead of the server name too.
	rs := c.randomNonStreamLeader(globalAccountName, "TEST")
	nodeName := rs.Node()
	require_Equal(t, nodeName, getHash(rs.Name()))
	req = &JSApiStreamRemovePeerRequest{Peer: nodeName}
	jsreq, err = json.Marshal(req)
	require_NoError(t, err)
	resp, err = nc.Request(fmt.Sprintf(JSApiStreamRemovePeerT, "TEST"), jsreq, time.Second)
	require_NoError(t, err)
	rpResp = JSApiStreamRemovePeerResponse{}
	require_NoError(t, json.Unmarshal(resp.Data, &rpResp))
	require_True(t, rpResp.Success)
}

func TestJetStreamClusterStreamLeaderStepDown(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "RNS", 3)
	defer c.shutdown()

	// Client based API
	s := c.randomServer()
	nc, js := jsClientConnect(t, s)
	defer nc.Close()

	_, err := js.AddStream(&nats.StreamConfig{Name: "TEST", Replicas: 3})
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	// Send in 10 messages.
	msg, toSend := []byte("Hello JS Clustering"), 10
	for i := 0; i < toSend; i++ {
		if _, err = js.Publish("TEST", msg); err != nil {
			t.Fatalf("Unexpected publish error: %v", err)
		}
	}

	sub, err := js.SubscribeSync("TEST", nats.Durable("cat"))
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	defer sub.Unsubscribe()

	oldLeader := c.streamLeader("$G", "TEST").Name()

	// Need to call this by hand for now.
	resp, err := nc.Request(fmt.Sprintf(JSApiStreamLeaderStepDownT, "TEST"), nil, time.Second)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	var sdResp JSApiStreamLeaderStepDownResponse
	if err := json.Unmarshal(resp.Data, &sdResp); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if sdResp.Error != nil {
		t.Fatalf("Unexpected error: %+v", sdResp.Error)
	}

	// Grab shorter timeout jetstream context.
	js, err = nc.JetStream(nats.MaxWait(250 * time.Millisecond))
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	checkFor(t, 2*time.Second, 50*time.Millisecond, func() error {
		si, err := js.StreamInfo("TEST")
		if err != nil {
			return fmt.Errorf("Could not fetch stream info: %v", err)
		}
		if si.Cluster.Leader == oldLeader {
			return fmt.Errorf("Still have old leader")
		}
		if len(si.Cluster.Replicas) != 2 {
			return fmt.Errorf("Expected 2 replicas, got %d", len(si.Cluster.Replicas))
		}
		for _, peer := range si.Cluster.Replicas {
			if !peer.Current {
				return fmt.Errorf("Expected replica to be current: %+v", peer)
			}
		}
		return nil
	})

	// Now do consumer.
	oldLeader = c.consumerLeader("$G", "TEST", "cat").Name()

	// Need to call this by hand for now.
	resp, err = nc.Request(fmt.Sprintf(JSApiConsumerLeaderStepDownT, "TEST", "cat"), nil, time.Second)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	var cdResp JSApiConsumerLeaderStepDownResponse
	if err := json.Unmarshal(resp.Data, &cdResp); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if cdResp.Error != nil {
		t.Fatalf("Unexpected error: %+v", cdResp.Error)
	}

	checkFor(t, 2*time.Second, 50*time.Millisecond, func() error {
		ci, err := js.ConsumerInfo("TEST", "cat")
		if err != nil {
			return fmt.Errorf("Could not fetch consumer info: %v", err)
		}
		if ci.Cluster.Leader == oldLeader {
			return fmt.Errorf("Still have old leader")
		}
		if len(ci.Cluster.Replicas) != 2 {
			return fmt.Errorf("Expected 2 replicas, got %d", len(ci.Cluster.Replicas))
		}
		for _, peer := range ci.Cluster.Replicas {
			if !peer.Current {
				return fmt.Errorf("Expected replica to be current: %+v", peer)
			}
		}
		return nil
	})
}

func TestJetStreamClusterRemoveServer(t *testing.T) {
	skip(t)

	c := createJetStreamClusterExplicit(t, "RNS", 5)
	defer c.shutdown()

	// Client based API
	s := c.randomServer()
	nc, js := jsClientConnect(t, s)
	defer nc.Close()

	_, err := js.AddStream(&nats.StreamConfig{Name: "TEST", Replicas: 3})
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	// Send in 10 messages.
	msg, toSend := []byte("Hello JS Clustering"), 10
	for i := 0; i < toSend; i++ {
		if _, err = js.Publish("TEST", msg); err != nil {
			t.Fatalf("Unexpected publish error: %v", err)
		}
	}
	sub, err := js.SubscribeSync("TEST")
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	checkSubsPending(t, sub, toSend)
	ci, err := sub.ConsumerInfo()
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	cname := ci.Name

	sl := c.streamLeader("$G", "TEST")
	c.removeJetStream(sl)

	c.waitOnLeader()
	c.waitOnStreamLeader("$G", "TEST")

	// Faster timeout since we loop below checking for condition.
	js, err = nc.JetStream(nats.MaxWait(250 * time.Millisecond))
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	// Check the stream info is eventually correct.
	checkFor(t, 20*time.Second, 100*time.Millisecond, func() error {
		si, err := js.StreamInfo("TEST")
		if err != nil {
			return fmt.Errorf("Could not fetch stream info: %v", err)
		}
		if len(si.Cluster.Replicas) != 2 {
			return fmt.Errorf("Expected 2 replicas, got %d", len(si.Cluster.Replicas))
		}
		for _, peer := range si.Cluster.Replicas {
			if !peer.Current {
				return fmt.Errorf("Expected replica to be current: %+v", peer)
			}
		}
		return nil
	})

	// Now do consumer.
	c.waitOnConsumerLeader("$G", "TEST", cname)
	checkFor(t, 20*time.Second, 50*time.Millisecond, func() error {
		ci, err := js.ConsumerInfo("TEST", cname)
		if err != nil {
			return fmt.Errorf("Could not fetch consumer info: %v", err)
		}
		if len(ci.Cluster.Replicas) != 2 {
			return fmt.Errorf("Expected 2 replicas, got %d", len(ci.Cluster.Replicas))
		}
		for _, peer := range ci.Cluster.Replicas {
			if !peer.Current {
				return fmt.Errorf("Expected replica to be current: %+v", peer)
			}
		}
		return nil
	})
}

func TestJetStreamClusterPurgeReplayAfterRestart(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "P3F", 3)
	defer c.shutdown()

	// Client based API
	s := c.randomNonLeader()
	nc, js := jsClientConnect(t, s)
	defer nc.Close()

	if _, err := js.AddStream(&nats.StreamConfig{Name: "TEST", Replicas: 3}); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	sendBatch := func(n int) {
		t.Helper()
		// Send a batch to a given subject.
		for i := 0; i < n; i++ {
			if _, err := js.Publish("TEST", []byte("OK")); err != nil {
				t.Fatalf("Unexpected publish error: %v", err)
			}
		}
	}

	sendBatch(10)
	if err := js.PurgeStream("TEST"); err != nil {
		t.Fatalf("Unexpected purge error: %v", err)
	}
	sendBatch(10)

	c.stopAll()
	c.restartAll()

	c.waitOnStreamLeader("$G", "TEST")

	s = c.randomServer()
	nc, js = jsClientConnect(t, s)
	defer nc.Close()

	si, err := js.StreamInfo("TEST")
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if si.State.Msgs != 10 {
		t.Fatalf("Expected 10 msgs after restart, got %d", si.State.Msgs)
	}
}

func TestJetStreamClusterStreamGetMsg(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "R3F", 3)
	defer c.shutdown()

	// Client based API
	s := c.randomServer()
	nc, js := jsClientConnect(t, s)
	defer nc.Close()

	if _, err := js.AddStream(&nats.StreamConfig{Name: "TEST"}); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if _, err := js.Publish("TEST", []byte("OK")); err != nil {
		t.Fatalf("Unexpected publish error: %v", err)
	}

	mreq := &JSApiMsgGetRequest{Seq: 1}
	req, err := json.Marshal(mreq)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	rmsg, err := nc.Request(fmt.Sprintf(JSApiMsgGetT, "TEST"), req, time.Second)
	if err != nil {
		t.Fatalf("Could not retrieve stream message: %v", err)
	}
	if err != nil {
		t.Fatalf("Could not retrieve stream message: %v", err)
	}

	var resp JSApiMsgGetResponse
	err = json.Unmarshal(rmsg.Data, &resp)
	if err != nil {
		t.Fatalf("Could not parse stream message: %v", err)
	}
	if resp.Message == nil || resp.Error != nil {
		t.Fatalf("Did not receive correct response: %+v", resp.Error)
	}
}

func TestJetStreamClusterStreamDirectGetMsg(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "R3F", 3)
	defer c.shutdown()

	// Client based API
	s := c.randomServer()
	nc, _ := jsClientConnect(t, s)
	defer nc.Close()

	// Do by hand for now.
	cfg := &StreamConfig{
		Name:        "TEST",
		Subjects:    []string{"foo"},
		Storage:     MemoryStorage,
		Replicas:    3,
		MaxMsgsPer:  1,
		AllowDirect: true,
	}
	addStream(t, nc, cfg)
	sendStreamMsg(t, nc, "foo", "bar")

	getSubj := fmt.Sprintf(JSDirectMsgGetT, "TEST")
	getMsg := func(req *JSApiMsgGetRequest) *nats.Msg {
		var b []byte
		var err error
		if req != nil {
			b, err = json.Marshal(req)
			require_NoError(t, err)
		}
		m, err := nc.Request(getSubj, b, time.Second)
		require_NoError(t, err)
		return m
	}

	m := getMsg(&JSApiMsgGetRequest{LastFor: "foo"})
	require_True(t, string(m.Data) == "bar")
	require_True(t, m.Header.Get(JSStream) == "TEST")
	require_True(t, m.Header.Get(JSSequence) == "1")
	require_True(t, m.Header.Get(JSSubject) == "foo")
	require_True(t, m.Subject != "foo")
	require_True(t, m.Header.Get(JSTimeStamp) != _EMPTY_)
}

func TestJetStreamClusterStreamPerf(t *testing.T) {
	// Comment out to run, holding place for now.
	skip(t)

	c := createJetStreamClusterExplicit(t, "R3S", 3)
	defer c.shutdown()

	// Client based API
	s := c.randomServer()
	nc, js := jsClientConnect(t, s)
	defer nc.Close()

	_, err := js.AddStream(&nats.StreamConfig{
		Name:     "TEST",
		Subjects: []string{"foo"},
		Replicas: 3,
	})
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	numConnections := 4
	var conns []nats.JetStream
	for i := 0; i < numConnections; i++ {
		s := c.randomServer()
		nc, js := jsClientConnect(t, s)
		defer nc.Close()
		conns = append(conns, js)
	}

	toSend := 100000
	numProducers := 8

	payload := []byte("Hello JSC")

	startCh := make(chan bool)
	var wg sync.WaitGroup

	for n := 0; n < numProducers; n++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			js := conns[rand.IntN(numConnections)]
			<-startCh
			for i := 0; i < int(toSend)/numProducers; i++ {
				if _, err = js.Publish("foo", payload); err != nil {
					t.Errorf("Unexpected publish error: %v", err)
				}
			}
		}()
	}

	// Wait for Go routines.
	time.Sleep(250 * time.Millisecond)

	start := time.Now()
	close(startCh)
	wg.Wait()

	tt := time.Since(start)
	fmt.Printf("Took %v to send %d msgs with %d producers and R=3!\n", tt, toSend, numProducers)
	fmt.Printf("%.0f msgs/sec\n\n", float64(toSend)/tt.Seconds())
}

func TestJetStreamClusterConsumerPerf(t *testing.T) {
	// Comment out to run, holding place for now.
	skip(t)

	c := createJetStreamClusterExplicit(t, "R3S", 3)
	defer c.shutdown()

	// Client based API
	s := c.randomServer()
	nc, js := jsClientConnect(t, s)
	defer nc.Close()

	_, err := js.AddStream(&nats.StreamConfig{Name: "TEST", Replicas: 3})
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	toSend := 500000
	msg := make([]byte, 64)
	crand.Read(msg)

	for i := 0; i < toSend; i++ {
		nc.Publish("TEST", msg)
	}
	nc.Flush()

	checkFor(t, 10*time.Second, 250*time.Millisecond, func() error {
		si, err := js.StreamInfo("TEST")
		if err != nil {
			return fmt.Errorf("Unexpected error: %v", err)
		}
		if si.State.Msgs != uint64(toSend) {
			return fmt.Errorf("Expected to have %d messages, got %d", toSend, si.State.Msgs)
		}
		return nil
	})

	received := int32(0)
	deliverTo := "r"
	done := make(chan bool)
	total := int32(toSend)
	var start time.Time

	nc.Subscribe(deliverTo, func(m *nats.Msg) {
		if r := atomic.AddInt32(&received, 1); r >= total {
			done <- true
		} else if r == 1 {
			start = time.Now()
		}
	})

	_, err = js.AddConsumer("TEST", &nats.ConsumerConfig{DeliverSubject: deliverTo, Durable: "gf"})
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatalf("Timed out?")
	}
	tt := time.Since(start)
	fmt.Printf("Took %v to receive %d msgs\n", tt, toSend)
	fmt.Printf("%.0f msgs/sec\n\n", float64(toSend)/tt.Seconds())
}

// This test creates a queue consumer for the delivery subject,
// and make sure it connects to the server that is not the leader
// of the stream. A bug was not stripping the $JS.ACK reply subject
// correctly, which means that ack sent on the reply subject was
// dropped by the route.
func TestJetStreamClusterQueueSubConsumer(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "R2S", 2)
	defer c.shutdown()

	// Client based API
	s := c.randomServer()
	nc, js := jsClientConnect(t, s)
	defer nc.Close()

	_, err := js.AddStream(&nats.StreamConfig{
		Name:     "TEST",
		Subjects: []string{"foo.>"},
		Replicas: 1,
	})
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	inbox := nats.NewInbox()
	obsReq := CreateConsumerRequest{
		Stream: "TEST",
		Config: ConsumerConfig{
			Durable:        "ivan",
			DeliverSubject: inbox,
			DeliverGroup:   "queue",
			AckPolicy:      AckExplicit,
			AckWait:        100 * time.Millisecond,
		},
	}
	req, err := json.Marshal(obsReq)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	resp, err := nc.Request(fmt.Sprintf(JSApiDurableCreateT, "TEST", "ivan"), req, time.Second)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	var ccResp JSApiConsumerCreateResponse
	if err = json.Unmarshal(resp.Data, &ccResp); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if ccResp.Error != nil {
		t.Fatalf("Unexpected error, got %+v", ccResp.Error)
	}

	ci, err := js.ConsumerInfo("TEST", "ivan")
	if err != nil {
		t.Fatalf("Error getting consumer info: %v", err)
	}

	// Now create a client that does NOT connect to the stream leader.
	// Start with url from first server in the cluster.
	u := c.servers[0].ClientURL()
	// If leader is "S-1", then use S-2 to connect to, which is at servers[1].
	if ci.Cluster.Leader == "S-1" {
		u = c.servers[1].ClientURL()
	}
	qsubnc, err := nats.Connect(u)
	if err != nil {
		t.Fatalf("Error connecting: %v", err)
	}
	defer qsubnc.Close()

	ch := make(chan struct{}, 2)
	if _, err := qsubnc.QueueSubscribe(inbox, "queue", func(m *nats.Msg) {
		m.Respond(nil)
		ch <- struct{}{}
	}); err != nil {
		t.Fatalf("Error creating sub: %v", err)
	}

	// Use the other connection to publish a message
	if _, err := js.Publish("foo.bar", []byte("hello")); err != nil {
		t.Fatalf("Error on publish: %v", err)
	}

	// Wait that we receive the message first.
	select {
	case <-ch:
	case <-time.After(time.Second):
		t.Fatal("Did not receive message")
	}

	// Message should be ack'ed and not redelivered.
	select {
	case <-ch:
		t.Fatal("Message redelivered!!!")
	case <-time.After(250 * time.Millisecond):
		// OK
	}
}

func TestJetStreamClusterLeaderStepdown(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "JSC", 3)
	defer c.shutdown()

	c.waitOnLeader()
	cl := c.leader()
	// Now ask the system account to have the leader stepdown.
	s := c.randomNonLeader()
	nc, err := nats.Connect(s.ClientURL(), nats.UserInfo("admin", "s3cr3t!"))
	if err != nil {
		t.Fatalf("Failed to create system client: %v", err)
	}
	defer nc.Close()

	resp, err := nc.Request(JSApiLeaderStepDown, nil, time.Second)
	if err != nil {
		t.Fatalf("Error on stepdown request: %v", err)
	}
	var sdr JSApiLeaderStepDownResponse
	if err := json.Unmarshal(resp.Data, &sdr); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if sdr.Error != nil || !sdr.Success {
		t.Fatalf("Unexpected error for leader stepdown: %+v", sdr.Error)
	}

	c.waitOnLeader()
	if cl == c.leader() {
		t.Fatalf("Expected a new metaleader, got same")
	}
}

func TestJetStreamClusterSourcesFilteringAndUpdating(t *testing.T) {
	owt := srcConsumerWaitTime
	srcConsumerWaitTime = 2 * time.Second
	defer func() { srcConsumerWaitTime = owt }()

	c := createJetStreamClusterExplicit(t, "MSR", 5)
	defer c.shutdown()

	// Client for API requests.
	nc, js := jsClientConnect(t, c.randomServer())
	defer nc.Close()

	sendBatch := func(subject string, n int) {
		t.Helper()
		// Send a batch to a given subject.
		for i := 0; i < n; i++ {
			if _, err := js.Publish(subject, []byte("OK")); err != nil {
				t.Fatalf("Unexpected publish error: %v", err)
			}
		}
	}

	checkSync := func(msgsTest, msgsM uint64) {
		t.Helper()
		checkFor(t, 30*time.Second, time.Second, func() error {
			if tsi, err := js.StreamInfo("TEST"); err != nil {
				return err
			} else if msi, err := js.StreamInfo("M"); err != nil {
				return err
			} else if tsi.State.Msgs != msgsTest {
				return fmt.Errorf("received %d msgs from TEST, expected %d", tsi.State.Msgs, msgsTest)
			} else if msi.State.Msgs != msgsM {
				return fmt.Errorf("received %d msgs from M, expected %d", msi.State.Msgs, msgsM)
			}
			return nil
		})
	}

	// Origin
	_, err := js.AddStream(&nats.StreamConfig{
		Name:     "TEST",
		Subjects: []string{"foo", "bar"},
		Replicas: 2,
	})
	require_NoError(t, err)
	defer js.DeleteStream("TEST")

	// Create M stream with a single source on "foo"
	_, err = js.AddStream(&nats.StreamConfig{
		Name:     "M",
		Sources:  []*nats.StreamSource{{Name: "TEST", FilterSubject: "foo"}},
		Replicas: 2,
	})
	require_NoError(t, err)
	defer js.DeleteStream("M")

	// check a message on "bar" doesn't get sourced
	sendBatch("bar", 100)
	checkSync(100, 0)
	// check a message on "foo" does get sourced
	sendBatch("foo", 100)
	checkSync(200, 100)

	// change remove the source on "foo" and add a new source on "bar"
	_, err = js.UpdateStream(&nats.StreamConfig{
		Name:     "M",
		Sources:  []*nats.StreamSource{{Name: "TEST", FilterSubject: "bar"}},
		Replicas: 2,
	})
	require_NoError(t, err)

	// As it is a new source (never been sourced before) it starts sourcing at the start of TEST
	// and therefore sources the message on "bar" that is in TEST
	checkSync(200, 200)

	// new messages on "foo" are being filtered as it's not being currently sourced
	sendBatch("foo", 100)
	checkSync(300, 200)
	// new messages on "bar" are being sourced
	sendBatch("bar", 100)
	checkSync(400, 300)

	// re-add the source for "foo" keep the source on "bar"
	_, err = js.UpdateStream(&nats.StreamConfig{
		Name:     "M",
		Sources:  []*nats.StreamSource{{Name: "TEST", FilterSubject: "bar"}, {Name: "TEST", FilterSubject: "foo"}},
		Replicas: 2,
	})
	require_NoError(t, err)

	// check the 'backfill' of messages on "foo" that were published while the source was inactive
	checkSync(400, 400)

	// causes resetSourceInfo() to be called
	nc.Close()
	c.stopAll()
	c.restartAll()
	c.waitOnStreamLeader("$G", "TEST")
	c.waitOnStreamLeader("$G", "M")

	nc, js = jsClientConnect(t, c.randomServer())
	defer nc.Close()

	// check that it restarted the sources' consumers at the right place
	checkSync(400, 400)

	// check both sources are still active
	sendBatch("bar", 100)
	checkSync(500, 500)
	sendBatch("foo", 100)
	checkSync(600, 600)

	// Check that purging the stream and does not cause the re-sourcing of the messages
	js.PurgeStream("M")
	checkSync(600, 0)

	// Even after a leader change or restart
	nc.Close()
	c.stopAll()
	c.restartAll()
	c.waitOnStreamLeader("$G", "TEST")
	c.waitOnStreamLeader("$G", "M")

	// Allow direct sync consumers to connect.
	// This test could pass if we do not hook up and try to deliver the messages when we should not.
	time.Sleep(1500 * time.Millisecond)

	nc, js = jsClientConnect(t, c.randomServer())
	defer nc.Close()

	checkSync(600, 0)
}

func TestJetStreamClusterSourcesUpdateOriginError(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "MSR", 5)
	defer c.shutdown()

	// Client for API requests.
	nc, js := jsClientConnect(t, c.randomServer())
	defer nc.Close()

	sendBatch := func(subject string, n int) {
		t.Helper()
		// Send a batch to a given subject.
		for i := 0; i < n; i++ {
			if _, err := js.Publish(subject, []byte("OK")); err != nil {
				t.Fatalf("Unexpected publish error: %v", err)
			}
		}
	}

	checkSync := func(msgsTest, msgsM uint64) {
		t.Helper()
		checkFor(t, 10*time.Second, 500*time.Millisecond, func() error {
			if tsi, err := js.StreamInfo("TEST"); err != nil {
				return err
			} else if msi, err := js.StreamInfo("M"); err != nil {
				return err
			} else if tsi.State.Msgs != msgsTest {
				return fmt.Errorf("received %d msgs from TEST, expected %d", tsi.State.Msgs, msgsTest)
			} else if msi.State.Msgs != msgsM {
				return fmt.Errorf("received %d msgs from M, expected %d", msi.State.Msgs, msgsM)
			}
			return nil
		})
	}

	// Origin
	_, err := js.AddStream(&nats.StreamConfig{
		Name:     "TEST",
		Subjects: []string{"foo"},
		Replicas: 2,
	})
	require_NoError(t, err)

	_, err = js.AddStream(&nats.StreamConfig{
		Name:     "M",
		Sources:  []*nats.StreamSource{{Name: "TEST", FilterSubject: "foo"}},
		Replicas: 2,
	})

	require_NoError(t, err)

	// Send 100 msgs.
	sendBatch("foo", 100)
	checkSync(100, 100)

	// update makes source invalid
	_, err = js.UpdateStream(&nats.StreamConfig{
		Name:     "TEST",
		Subjects: []string{"bar"},
		Replicas: 2,
	})
	require_NoError(t, err)

	// TODO check for downstream error propagation

	_, err = js.Publish("foo", nil)
	require_Error(t, err)

	sendBatch("bar", 100)
	// The source stream remains at 100 msgs as it still uses foo as it's filter
	checkSync(200, 100)

	nc.Close()
	c.stopAll()
	c.restartAll()
	c.waitOnStreamLeader("$G", "TEST")
	c.waitOnStreamLeader("$G", "M")

	nc, js = jsClientConnect(t, c.randomServer())
	defer nc.Close()

	checkSync(200, 100)

	_, err = js.Publish("foo", nil)
	require_Error(t, err)
	require_Equal(t, err.Error(), "nats: no response from stream")

	sendBatch("bar", 100)
	// The source stream remains at 100 msgs as it still uses foo as it's filter
	checkSync(300, 100)
}

func TestJetStreamClusterMirrorAndSourcesClusterRestart(t *testing.T) {
	owt := srcConsumerWaitTime
	srcConsumerWaitTime = 2 * time.Second
	defer func() { srcConsumerWaitTime = owt }()

	test := func(t *testing.T, mirror bool, filter bool) {
		c := createJetStreamClusterExplicit(t, "MSR", 5)
		defer c.shutdown()

		// Client for API requests.
		nc, js := jsClientConnect(t, c.randomServer())
		defer nc.Close()

		// Origin
		_, err := js.AddStream(&nats.StreamConfig{
			Name:     "TEST",
			Subjects: []string{"foo", "bar", "baz.*"},
			Replicas: 2,
		})
		require_NoError(t, err)

		filterSubj := _EMPTY_
		if filter {
			filterSubj = "foo"
		}

		// Create Mirror/Source now.
		if mirror {
			_, err = js.AddStream(&nats.StreamConfig{
				Name:     "M",
				Mirror:   &nats.StreamSource{Name: "TEST", FilterSubject: filterSubj},
				Replicas: 2,
			})
		} else {
			_, err = js.AddStream(&nats.StreamConfig{
				Name:     "M",
				Sources:  []*nats.StreamSource{{Name: "TEST", FilterSubject: filterSubj}},
				Replicas: 2,
			})
		}
		require_NoError(t, err)

		expectedMsgCount := uint64(0)

		sendBatch := func(subject string, n int) {
			t.Helper()
			if subject == "foo" || !filter {
				expectedMsgCount += uint64(n)
			}
			// Send a batch to a given subject.
			for i := 0; i < n; i++ {
				if _, err := js.Publish(subject, []byte("OK")); err != nil {
					t.Fatalf("Unexpected publish error: %v", err)
				}
			}
		}

		checkSync := func(msgsTest, msgsM uint64) {
			t.Helper()
			checkFor(t, 40*time.Second, time.Second, func() error {
				if tsi, err := js.StreamInfo("TEST"); err != nil {
					return err
				} else if msi, err := js.StreamInfo("M"); err != nil {
					return err
				} else if tsi.State.Msgs != msgsTest {
					return fmt.Errorf("received %d msgs from TEST, expected %d", tsi.State.Msgs, msgsTest)
				} else if msi.State.Msgs != msgsM {
					return fmt.Errorf("received %d msgs from M, expected %d", msi.State.Msgs, msgsM)
				}
				return nil
			})
		}

		sendBatch("foo", 100)
		checkSync(100, expectedMsgCount)
		sendBatch("bar", 100)
		checkSync(200, expectedMsgCount)

		nc.Close()
		c.stopAll()
		c.restartAll()
		c.waitOnStreamLeader("$G", "TEST")
		c.waitOnStreamLeader("$G", "M")

		nc, js = jsClientConnect(t, c.randomServer())
		defer nc.Close()

		checkSync(200, expectedMsgCount)
		sendBatch("foo", 100)
		checkSync(300, expectedMsgCount)
		sendBatch("bar", 100)
		checkSync(400, expectedMsgCount)
	}
	t.Run("mirror-filter", func(t *testing.T) {
		test(t, true, true)
	})
	t.Run("mirror-nofilter", func(t *testing.T) {
		test(t, true, false)
	})
	t.Run("source-filter", func(t *testing.T) {
		test(t, false, true)
	})
	t.Run("source-nofilter", func(t *testing.T) {
		test(t, false, false)
	})
}

func TestJetStreamClusterMirrorAndSourcesFilteredConsumers(t *testing.T) {
	c := createJetStreamClusterWithTemplate(t, jsClusterMirrorSourceImportsTempl, "MS5", 5)
	defer c.shutdown()

	// Client for API requests.
	s := c.randomServer()
	nc, js := jsClientConnect(t, s)
	defer nc.Close()

	// Origin
	_, err := js.AddStream(&nats.StreamConfig{
		Name:     "TEST",
		Subjects: []string{"foo", "bar", "baz.*"},
	})
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	// Create Mirror now.
	_, err = js.AddStream(&nats.StreamConfig{
		Name:   "M",
		Mirror: &nats.StreamSource{Name: "TEST"},
	})
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	dsubj := nats.NewInbox()
	nc.SubscribeSync(dsubj)
	nc.Flush()

	createConsumer := func(sn, fs string) {
		t.Helper()
		_, err = js.AddConsumer(sn, &nats.ConsumerConfig{DeliverSubject: dsubj, FilterSubject: fs})
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
	}
	expectFail := func(sn, fs string) {
		t.Helper()
		_, err = js.AddConsumer(sn, &nats.ConsumerConfig{DeliverSubject: dsubj, FilterSubject: fs})
		if err == nil {
			t.Fatalf("Expected error but got none")
		}
	}

	createConsumer("M", "foo")
	createConsumer("M", "bar")
	createConsumer("M", "baz.foo")
	expectFail("M", ".")
	expectFail("M", ">.foo")

	// Make sure wider scoped subjects work as well.
	createConsumer("M", "*")
	createConsumer("M", ">")

	// Now do some sources.
	if _, err := js.AddStream(&nats.StreamConfig{Name: "O1", Subjects: []string{"foo.*"}}); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if _, err := js.AddStream(&nats.StreamConfig{Name: "O2", Subjects: []string{"bar.*"}}); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	// Create downstream now.
	_, err = js.AddStream(&nats.StreamConfig{
		Name:    "S",
		Sources: []*nats.StreamSource{{Name: "O1"}, {Name: "O2"}},
	})
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	createConsumer("S", "foo.1")
	createConsumer("S", "bar.1")

	// Now cross account stuff.
	nc2, js2 := jsClientConnect(t, s, nats.UserInfo("rip", "pass"))
	defer nc2.Close()

	if _, err := js2.AddStream(&nats.StreamConfig{Name: "ORIGIN", Subjects: []string{"foo.*"}}); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	cfg := StreamConfig{
		Name:    "SCA",
		Storage: FileStorage,
		Sources: []*StreamSource{{
			Name: "ORIGIN",
			External: &ExternalStream{
				ApiPrefix:     "RI.JS.API",
				DeliverPrefix: "RI.DELIVER.SYNC.SOURCES",
			},
		}},
	}
	req, err := json.Marshal(cfg)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	resp, err := nc.Request(fmt.Sprintf(JSApiStreamCreateT, cfg.Name), req, time.Second)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	var scResp JSApiStreamCreateResponse
	if err := json.Unmarshal(resp.Data, &scResp); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if scResp.StreamInfo == nil || scResp.Error != nil {
		t.Fatalf("Did not receive correct response: %+v", scResp.Error)
	}

	// Externals skip the checks for now.
	createConsumer("SCA", "foo.1")
	createConsumer("SCA", "bar.1")
	createConsumer("SCA", "baz")
}

func TestJetStreamClusterCrossAccountMirrorsAndSources(t *testing.T) {
	c := createJetStreamClusterWithTemplate(t, jsClusterMirrorSourceImportsTempl, "C1", 3)
	defer c.shutdown()

	// Create source stream under RI account.
	s := c.randomServer()
	nc, js := jsClientConnect(t, s, nats.UserInfo("rip", "pass"))
	defer nc.Close()

	if _, err := js.AddStream(&nats.StreamConfig{Name: "TEST", Replicas: 2}); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	// use large number to tease out FC issues
	toSend := 3000
	for i := 0; i < toSend; i++ {
		if _, err := js.Publish("TEST", []byte("OK")); err != nil {
			t.Fatalf("Unexpected publish error: %v", err)
		}
	}

	nc2, js2 := jsClientConnect(t, s)
	defer nc2.Close()

	// Have to do this direct until we get Go client support.
	// Need to match jsClusterMirrorSourceImportsTempl imports.
	_, err := js2.AddStream(&nats.StreamConfig{
		Name: "MY_MIRROR_TEST",
		Mirror: &nats.StreamSource{
			Name: "TEST",
			External: &nats.ExternalStream{
				APIPrefix:     "RI.JS.API",
				DeliverPrefix: "RI.DELIVER.SYNC.MIRRORS",
			},
		},
	})
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	checkFor(t, 20*time.Second, 500*time.Millisecond, func() error {
		si, err := js2.StreamInfo("MY_MIRROR_TEST")
		if err != nil {
			t.Fatalf("Could not retrieve stream info: %s", err)
		}
		if si.State.Msgs != uint64(toSend) {
			return fmt.Errorf("Expected %d msgs, got state: %+v", toSend, si.State)
		}
		return nil
	})

	// Now do sources as well.
	_, err = js2.AddStream(&nats.StreamConfig{
		Name: "MY_SOURCE_TEST",
		Sources: []*nats.StreamSource{
			{
				Name: "TEST",
				External: &nats.ExternalStream{
					APIPrefix:     "RI.JS.API",
					DeliverPrefix: "RI.DELIVER.SYNC.SOURCES",
				},
			},
		},
	})
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	checkFor(t, 20*time.Second, 100*time.Millisecond, func() error {
		si, err := js2.StreamInfo("MY_SOURCE_TEST")
		if err != nil {
			t.Fatalf("Could not retrieve stream info")
		}
		if si.State.Msgs != uint64(toSend) {
			return fmt.Errorf("Expected %d msgs, got state: %+v", toSend, si.State)
		}
		return nil
	})

}

// https://github.com/nats-io/nats-server/issues/8630
func TestJetStreamClusterCrossAccountMirrorsAndSourcesFlowControlStalled(t *testing.T) {
	fcImport := `{ service: {account: JS, subject: "$JS.FC.>" }}`
	require_True(t, strings.Contains(jsClusterMirrorSourceImportsTempl, fcImport))

	for _, test := range []struct {
		name  string
		templ string
		stall bool
	}{
		{"with FC import", jsClusterMirrorSourceImportsTempl, false},
		{"without FC import", strings.Replace(jsClusterMirrorSourceImportsTempl, fcImport, "", 1), true},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := createJetStreamClusterWithTemplate(t, test.templ, "C1", 3)
			defer c.shutdown()

			s := c.randomServer()
			nc, js := jsClientConnect(t, s, nats.UserInfo("rip", "pass"))
			defer nc.Close()

			_, err := js.AddStream(&nats.StreamConfig{Name: "TEST", Replicas: 2})
			require_NoError(t, err)

			// Publish enough bytes so the first delivery burst triggers a flow control request.
			toSend := 3000
			for range toSend {
				_, err = js.Publish("TEST", make([]byte, 1024))
				require_NoError(t, err)
			}

			nc2, js2 := jsClientConnect(t, s)
			defer nc2.Close()

			ext := &nats.ExternalStream{APIPrefix: "RI.JS.API", DeliverPrefix: "RI.DELIVER.SYNC"}
			_, err = js2.AddStream(&nats.StreamConfig{Name: "M", Mirror: &nats.StreamSource{Name: "TEST", External: ext}})
			require_NoError(t, err)
			_, err = js2.AddStream(&nats.StreamConfig{Name: "S", Sources: []*nats.StreamSource{{Name: "TEST", External: ext}}})
			require_NoError(t, err)

			checkFor(t, 10*time.Second, 200*time.Millisecond, func() error {
				for _, name := range []string{"M", "S"} {
					si, err := js2.StreamInfo(name)
					require_NoError(t, err)
					ssi := si.Mirror
					if name == "S" {
						ssi = si.Sources[0]
					}
					if !test.stall {
						if ssi.Error != nil || si.State.Msgs != uint64(toSend) {
							return fmt.Errorf("%s: err=%v, msgs=%d", name, ssi.Error, si.State.Msgs)
						}
					} else if ssi.Error == nil || ssi.Error.ErrorCode != nats.ErrorCode(JSSourceConsumerFlowControlStalledErr) || si.State.Msgs >= uint64(toSend) {
						return fmt.Errorf("%s: expected stalled error, err=%v, msgs=%d", name, ssi.Error, si.State.Msgs)
					}
				}
				return nil
			})
		})
	}
}

func TestJetStreamClusterFailMirrorsAndSources(t *testing.T) {
	c := createJetStreamClusterWithTemplate(t, jsClusterMirrorSourceImportsTempl, "C1", 3)
	defer c.shutdown()

	// Create source stream under RI account.
	s := c.randomServer()
	nc, js := jsClientConnect(t, s, nats.UserInfo("rip", "pass"))
	defer nc.Close()

	if _, err := js.AddStream(&nats.StreamConfig{Name: "TEST", Replicas: 2, Subjects: []string{"test.>"}}); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	nc2, _ := jsClientConnect(t, s, nats.UserInfo("rip", "pass"))
	defer nc2.Close()

	testPrefix := func(testName string, id ErrorIdentifier, cfg StreamConfig) {
		t.Run(testName, func(t *testing.T) {
			req, err := json.Marshal(cfg)
			if err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}
			resp, err := nc2.Request(fmt.Sprintf(JSApiStreamCreateT, cfg.Name), req, time.Second)
			if err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}
			var scResp JSApiStreamCreateResponse
			if err := json.Unmarshal(resp.Data, &scResp); err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}
			if scResp.Error == nil {
				t.Fatalf("Did expect an error but got none")
			} else if !IsNatsErr(scResp.Error, id) {
				t.Fatalf("Expected different error: %s", scResp.Error.Description)
			}
		})
	}

	testPrefix("mirror-bad-apiprefix", JSStreamExternalApiOverlapErrF, StreamConfig{
		Name:    "MY_MIRROR_TEST",
		Storage: FileStorage,
		Mirror: &StreamSource{
			Name: "TEST",
			External: &ExternalStream{
				ApiPrefix:     "$JS.API",
				DeliverPrefix: "here",
			},
		},
	})
	testPrefix("source-bad-apiprefix", JSStreamExternalApiOverlapErrF, StreamConfig{
		Name:    "MY_SOURCE_TEST",
		Storage: FileStorage,
		Sources: []*StreamSource{{
			Name: "TEST",
			External: &ExternalStream{
				ApiPrefix:     "$JS.API",
				DeliverPrefix: "here",
			},
		},
		},
	})
}

func TestJetStreamClusterConsumerDeliveredSyncReporting(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "R3S", 3)
	defer c.shutdown()

	nc, js := jsClientConnect(t, c.randomServer())
	defer nc.Close()

	_, err := js.AddStream(&nats.StreamConfig{
		Name:      "TEST",
		Retention: nats.WorkQueuePolicy,
		Subjects:  []string{"foo.*"},
		Replicas:  3,
	})
	require_NoError(t, err)

	sub, err := js.PullSubscribe("foo.bar", "mc")
	require_NoError(t, err)

	// Make us match first, but not next 10.
	_, err = js.Publish("foo.bar", nil)
	require_NoError(t, err)
	for i := 0; i < 10; i++ {
		_, err = js.Publish("foo.baz", nil)
		require_NoError(t, err)
	}

	opts := &JSzOptions{Accounts: true, Streams: true, Consumer: true}
	for _, s := range c.servers {
		jsz, err := s.Jsz(opts)
		require_NoError(t, err)
		ci := jsz.AccountDetails[0].Streams[0].Consumer[0]
		require_Equal(t, ci.Delivered.Consumer, 0)
		require_Equal(t, ci.Delivered.Stream, 0)
	}

	msgs, err := sub.Fetch(1)
	require_NoError(t, err)
	require_Equal(t, len(msgs), 1)
	meta, err := msgs[0].Metadata()
	require_NoError(t, err)
	require_Equal(t, meta.Sequence.Consumer, 1)
	require_Equal(t, meta.Sequence.Stream, 1)

	// Allow some time for the state to propagate.
	maxWait := 200 * time.Millisecond
	time.Sleep(maxWait)

	for _, s := range c.servers {
		jsz, err := s.Jsz(opts)
		require_NoError(t, err)
		ci := jsz.AccountDetails[0].Streams[0].Consumer[0]
		require_Equal(t, ci.Delivered.Consumer, 1)
		require_Equal(t, ci.Delivered.Stream, 1)
	}

	// Now we want to make sure that jsz reporting will show the same
	// state, including delivered, which will have skipped to the end.
	// The skip can happen on several factors, but for here we just send
	// another pull request which we will let fail.
	_, err = sub.Fetch(1, nats.MaxWait(maxWait))
	require_Error(t, err)

	for _, s := range c.servers {
		jsz, err := s.Jsz(opts)
		require_NoError(t, err)
		ci := jsz.AccountDetails[0].Streams[0].Consumer[0]
		require_Equal(t, ci.Delivered.Consumer, 1)
		require_Equal(t, ci.Delivered.Stream, 11)
	}
}

// This is to test follower ack fill logic when pending not empty.
// There was a bug that would update p.Sequence in the stores (mem & file)
// that would cause the logic to fail. Redeliveries were required to trigger.
func TestJetStreamClusterConsumerAckSyncReporting(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "R3S", 3)
	defer c.shutdown()

	nc, js := jsClientConnect(t, c.randomServer())
	defer nc.Close()

	_, err := js.AddStream(&nats.StreamConfig{
		Name:      "TEST",
		Retention: nats.WorkQueuePolicy,
		Subjects:  []string{"foo.*"},
		Replicas:  3,
	})
	require_NoError(t, err)

	sub, err := js.PullSubscribe("foo.bar", "mc", nats.AckWait(250*time.Millisecond))
	require_NoError(t, err)

	for i := 0; i < 10; i++ {
		_, err = js.Publish("foo.bar", nil)
		require_NoError(t, err)
	}

	// Wait for all servers to converge on the same state. Stream and consumer leader could be different.
	checkFor(t, time.Second, 100*time.Millisecond, func() error {
		return checkState(t, c, globalAccountName, "TEST")
	})

	msgs, err := sub.Fetch(10)
	require_NoError(t, err)
	require_Equal(t, len(msgs), 10)
	// Let redeliveries kick in.
	time.Sleep(time.Second)
	msgs, err = sub.Fetch(10)
	require_NoError(t, err)
	require_Equal(t, len(msgs), 10)

	// Randomize
	rand.Shuffle(len(msgs), func(i, j int) { msgs[i], msgs[j] = msgs[j], msgs[i] })

	dontAck := uint64(7)
	var skipped, last *nats.Msg
	for _, m := range msgs {
		meta, err := m.Metadata()
		require_NoError(t, err)
		if meta.Sequence.Stream == dontAck {
			skipped = m
			continue
		}
		if meta.Sequence.Stream == 10 {
			last = m
			continue
		}
		err = m.AckSync()
		require_NoError(t, err)
	}
	require_NotNil(t, skipped)
	require_NotNil(t, last)

	checkAckFloor := func(consumer, stream uint64) {
		opts := &JSzOptions{Accounts: true, Streams: true, Consumer: true}
		checkFor(t, 3*time.Second, 200*time.Millisecond, func() error {
			for _, s := range c.servers {
				jsz, err := s.Jsz(opts)
				if err != nil {
					return err
				}
				ci := jsz.AccountDetails[0].Streams[0].Consumer[0]
				if ci.AckFloor.Consumer != consumer {
					return fmt.Errorf("AckFloor.Consumer is not %d: %v", consumer, ci.AckFloor.Consumer)
				}
				if ci.AckFloor.Stream != stream {
					return fmt.Errorf("AckFloor.Stream is not %d: %v", stream, ci.AckFloor.Stream)
				}
			}
			return nil
		})
	}

	// Now we want to make sure that jsz reporting will show the same
	// state for ack floor.
	checkAckFloor(dontAck-1, dontAck-1)

	// Now ack the skipped message
	err = skipped.AckSync()
	require_NoError(t, err)
	checkAckFloor(9, 9)

	// Now ack the last message
	err = last.AckSync()
	require_NoError(t, err)
	checkAckFloor(20, 10)
}

func TestJetStreamClusterConsumerDeleteInterestPolicyMultipleConsumers(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "R3S", 3)
	defer c.shutdown()

	nc, js := jsClientConnect(t, c.randomServer())
	defer nc.Close()

	_, err := js.AddStream(&nats.StreamConfig{
		Name:      "TEST",
		Retention: nats.InterestPolicy,
		Subjects:  []string{"foo.*"},
		Replicas:  3,
	})
	require_NoError(t, err)

	// Make the first sequence high. We already protect against it but for extra sanity.
	err = js.PurgeStream("TEST", &nats.StreamPurgeRequest{Sequence: 100_000_000})
	require_NoError(t, err)

	// Create 2 consumers.
	_, err = js.AddConsumer("TEST", &nats.ConsumerConfig{
		Durable:   "C1",
		AckPolicy: nats.AckExplicitPolicy,
	})
	require_NoError(t, err)

	_, err = js.AddConsumer("TEST", &nats.ConsumerConfig{
		Durable:   "C2",
		AckPolicy: nats.AckExplicitPolicy,
	})
	require_NoError(t, err)

	for i := 0; i < 100; i++ {
		js.PublishAsync("foo.bar", []byte("ok"))
	}
	select {
	case <-js.PublishAsyncComplete():
	case <-time.After(5 * time.Second):
		t.Fatalf("Did not receive completion signal")
	}

	expectedStreamMsgs := func(msgs uint64) {
		t.Helper()
		checkFor(t, 2*time.Second, 200*time.Millisecond, func() error {
			si, err := js.StreamInfo("TEST")
			if err != nil {
				return err
			}
			if si.State.Msgs != msgs {
				return fmt.Errorf("require uint64 equal, but got: %d != %d", si.State.Msgs, msgs)
			}
			return nil
		})
	}
	expectedStreamMsgs(100)

	sub, err := js.PullSubscribe("foo.bar", "C1")
	require_NoError(t, err)
	msgs, err := sub.Fetch(50)
	require_NoError(t, err)
	require_Equal(t, len(msgs), 50)
	for _, m := range msgs {
		require_NoError(t, m.AckSync())
	}

	// Now delete second one and make sure accounting correct.
	err = js.DeleteConsumer("TEST", "C2")
	require_NoError(t, err)

	expectedStreamMsgs(50)
}

func TestJetStreamClusterConsumerAckNoneInterestPolicyShouldNotRetainAfterDelivery(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "R3S", 3)
	defer c.shutdown()

	nc, js := jsClientConnect(t, c.randomServer())
	defer nc.Close()

	_, err := js.AddStream(&nats.StreamConfig{
		Name:      "TEST",
		Retention: nats.InterestPolicy,
		Subjects:  []string{"foo.*"},
		Replicas:  3,
	})
	require_NoError(t, err)

	// Make the first sequence high. We already protect against it but for extra sanity.
	err = js.PurgeStream("TEST", &nats.StreamPurgeRequest{Sequence: 100_000_000})
	require_NoError(t, err)

	_, err = js.AddConsumer("TEST", &nats.ConsumerConfig{
		Durable:   "C1",
		AckPolicy: nats.AckNonePolicy,
	})
	require_NoError(t, err)

	for i := 0; i < 100; i++ {
		js.PublishAsync("foo.bar", []byte("ok"))
	}
	select {
	case <-js.PublishAsyncComplete():
	case <-time.After(5 * time.Second):
		t.Fatalf("Did not receive completion signal")
	}

	si, err := js.StreamInfo("TEST")
	require_NoError(t, err)
	require_Equal(t, si.State.Msgs, 100)

	sub, err := js.PullSubscribe("foo.bar", "C1")
	require_NoError(t, err)
	msgs, err := sub.Fetch(100)
	require_NoError(t, err)
	require_Equal(t, len(msgs), 100)

	checkFor(t, time.Second, 100*time.Millisecond, func() error {
		si, err = js.StreamInfo("TEST")
		if err != nil {
			return err
		}
		if si.State.Msgs != 0 {
			return fmt.Errorf("require uint64 equal, but got: %d != 0", si.State.Msgs)
		}
		return nil
	})
}

func TestJetStreamClusterConsumerDeleteAckNoneInterestPolicyWithOthers(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "R3S", 3)
	defer c.shutdown()

	nc, js := jsClientConnect(t, c.randomServer())
	defer nc.Close()

	_, err := js.AddStream(&nats.StreamConfig{
		Name:      "TEST",
		Retention: nats.InterestPolicy,
		Subjects:  []string{"foo.*"},
		Replicas:  3,
	})
	require_NoError(t, err)

	// Make the first sequence high. We already protect against it but for extra sanity.
	err = js.PurgeStream("TEST", &nats.StreamPurgeRequest{Sequence: 100_000_000})
	require_NoError(t, err)

	// Create 2 consumers.
	_, err = js.AddConsumer("TEST", &nats.ConsumerConfig{
		Durable:   "C1",
		AckPolicy: nats.AckExplicitPolicy,
	})
	require_NoError(t, err)

	_, err = js.AddConsumer("TEST", &nats.ConsumerConfig{
		Durable:   "C2",
		AckPolicy: nats.AckNonePolicy,
	})
	require_NoError(t, err)

	for i := 0; i < 100; i++ {
		js.PublishAsync("foo.bar", []byte("ok"))
	}
	select {
	case <-js.PublishAsyncComplete():
	case <-time.After(5 * time.Second):
		t.Fatalf("Did not receive completion signal")
	}

	expectedStreamMsgs := func(msgs uint64) {
		t.Helper()
		checkFor(t, 2*time.Second, 200*time.Millisecond, func() error {
			si, err := js.StreamInfo("TEST")
			if err != nil {
				return err
			}
			if si.State.Msgs != msgs {
				return fmt.Errorf("require uint64 equal, but got: %d != %d", si.State.Msgs, msgs)
			}
			return nil
		})
	}
	expectedStreamMsgs(100)

	sub, err := js.PullSubscribe("foo.bar", "C1")
	require_NoError(t, err)
	msgs, err := sub.Fetch(100)
	require_NoError(t, err)
	require_Equal(t, len(msgs), 100)
	for _, m := range msgs {
		require_NoError(t, m.AckSync())
	}
	// AckNone will hold.
	expectedStreamMsgs(100)

	// Now delete second one and make sure accounting correct.
	err = js.DeleteConsumer("TEST", "C2")
	require_NoError(t, err)

	expectedStreamMsgs(0)
}

// Make sure to not allow non-system accounts to move meta leader.
func TestJetStreamClusterMetaStepdownFromNonSysAccount(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "R3S", 3)
	defer c.shutdown()

	s := c.randomServer()

	// Client based API
	nc, _ := jsClientConnect(t, s)
	defer nc.Close()

	ml := c.leader()

	_, err := nc.Request(JSApiLeaderStepDown, nil, time.Second)
	require_Error(t, err, nats.ErrTimeout)

	// Make sure we did not move.
	c.waitOnLeader()
	require_Equal(t, ml, c.leader())

	// System user can move it.
	snc, _ := jsClientConnect(t, c.randomServer(), nats.UserInfo("admin", "s3cr3t!"))
	defer snc.Close()

	resp, err := snc.Request(JSApiLeaderStepDown, nil, time.Second)
	require_NoError(t, err)

	var sdr JSApiLeaderStepDownResponse
	require_NoError(t, json.Unmarshal(resp.Data, &sdr))
	require_True(t, sdr.Success)
	require_Equal(t, sdr.Error, nil)

	// Make sure we did move this time.
	c.waitOnLeader()
	require_NotEqual(t, ml, c.leader())
}

func TestJetStreamClusterMaxDeliveriesOnInterestStreams(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "R3S", 3)
	defer c.shutdown()

	// Client based API
	nc, js := jsClientConnect(t, c.randomServer())
	defer nc.Close()

	_, err := js.AddStream(&nats.StreamConfig{
		Name:      "TEST",
		Subjects:  []string{"foo.*"},
		Retention: nats.InterestPolicy,
	})
	require_NoError(t, err)

	sub1, err := js.PullSubscribe("foo.*", "c1", nats.AckWait(10*time.Millisecond), nats.MaxDeliver(1))
	require_NoError(t, err)

	sub2, err := js.PullSubscribe("foo.*", "c2", nats.AckWait(10*time.Millisecond), nats.MaxDeliver(1))
	require_NoError(t, err)

	_, err = js.Publish("foo.bar", []byte("HELLO"))
	require_NoError(t, err)

	si, err := js.StreamInfo("TEST")
	require_NoError(t, err)
	require_Equal(t, si.State.Msgs, 1)

	msgs, err := sub1.Fetch(1)
	require_NoError(t, err)
	require_Equal(t, len(msgs), 1)

	msgs, err = sub2.Fetch(1)
	require_NoError(t, err)
	require_Equal(t, len(msgs), 1)

	// Wait for redelivery to both consumers which will do nothing.
	time.Sleep(250 * time.Millisecond)

	// Now check that stream and consumer infos are correct.
	si, err = js.StreamInfo("TEST")
	require_NoError(t, err)
	// Messages that are skipped due to max deliveries should NOT remove messages.
	require_Equal(t, si.State.Msgs, 1)
	require_Equal(t, si.State.Consumers, 2)

	for _, cname := range []string{"c1", "c2"} {
		ci, err := js.ConsumerInfo("TEST", cname)
		require_NoError(t, err)
		require_Equal(t, ci.Delivered.Consumer, 1)
		require_Equal(t, ci.Delivered.Stream, 1)
		require_Equal(t, ci.AckFloor.Consumer, 0)
		require_Equal(t, ci.AckFloor.Stream, 0)
		require_Equal(t, ci.NumAckPending, 0)
		require_Equal(t, ci.NumRedelivered, 0)
		require_Equal(t, ci.NumPending, 0)
	}
}

func TestJetStreamClusterMetaRecoveryUpdatesDeletesConsumers(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "R3S", 3)
	defer c.shutdown()

	js := c.leader().getJetStream()

	create := []*Entry{
		{EntryNormal, encodeAddStreamAssignment(&streamAssignment{
			Config: &StreamConfig{Name: "TEST", Storage: FileStorage},
		})},
		{EntryNormal, encodeAddConsumerAssignment(&consumerAssignment{
			Stream: "TEST",
			Config: &ConsumerConfig{Name: "consumer"},
		})},
	}

	delete := []*Entry{
		{EntryNormal, encodeDeleteStreamAssignment(&streamAssignment{
			Config: &StreamConfig{Name: "TEST", Storage: FileStorage},
		})},
	}

	// Need to be recovering so that we accumulate recoveryUpdates.
	js.setMetaRecovering()
	ru := &recoveryUpdates{
		removeStreams:   make(map[string]*streamAssignment),
		removeConsumers: make(map[string]map[string]*consumerAssignment),
		addStreams:      make(map[string]*streamAssignment),
		updateStreams:   make(map[string]*streamAssignment),
		updateConsumers: make(map[string]map[string]*consumerAssignment),
	}

	// Push recovery entries that create the stream & consumer.
	_, _, err := js.applyMetaEntries(create, ru)
	require_NoError(t, err)
	require_Len(t, len(ru.updateConsumers), 1)

	// Now push another recovery entry that deletes the stream. The
	// entry that creates the consumer should now be gone.
	_, _, err = js.applyMetaEntries(delete, ru)
	require_NoError(t, err)
	require_Len(t, len(ru.removeStreams), 1)
	require_Len(t, len(ru.updateConsumers), 0)
}

func TestJetStreamClusterMetaRecoverySnapshotReconcilesStagedUpdates(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "R3S", 3)
	defer c.shutdown()

	ml := c.leader()
	require_NotNil(t, ml)
	js := ml.getJetStream()

	// Helpers to build the recovery log entries that stage operations.
	addStream := func(name string) *Entry {
		return &Entry{EntryNormal, encodeAddStreamAssignment(&streamAssignment{
			Client: &ClientInfo{Account: globalAccountName},
			Config: &StreamConfig{Name: name, Storage: FileStorage},
		})}
	}
	updateStream := func(name string) *Entry {
		return &Entry{EntryNormal, encodeUpdateStreamAssignment(&streamAssignment{
			Client: &ClientInfo{Account: globalAccountName},
			Config: &StreamConfig{Name: name, Storage: FileStorage},
		})}
	}
	addConsumer := func(stream, name string) *Entry {
		return &Entry{EntryNormal, encodeAddConsumerAssignment(&consumerAssignment{
			Client: &ClientInfo{Account: globalAccountName}, Stream: stream, Name: name,
			Config: &ConsumerConfig{Name: name},
		})}
	}
	delConsumer := func(stream, name string) *Entry {
		return &Entry{EntryNormal, encodeDeleteConsumerAssignment(&consumerAssignment{
			Client: &ClientInfo{Account: globalAccountName}, Stream: stream, Name: name,
			Config: &ConsumerConfig{Name: name},
		})}
	}

	// snapStream describes a stream (and its consumers) present in the incoming compacted snapshot.
	type snapStream struct {
		name      string
		consumers []string
	}
	encodeSnapshot := func(t *testing.T, streams ...snapStream) []*Entry {
		t.Helper()
		asa := map[string]*streamAssignment{}
		for _, s := range streams {
			cfg := &StreamConfig{Name: s.name, Storage: FileStorage}
			cfgJSON, err := json.Marshal(cfg)
			require_NoError(t, err)
			sa := &streamAssignment{Client: &ClientInfo{Account: globalAccountName}, Config: cfg, ConfigJSON: cfgJSON}
			for _, cn := range s.consumers {
				if sa.consumers == nil {
					sa.consumers = map[string]*consumerAssignment{}
				}
				ccfg := &ConsumerConfig{Name: cn}
				ccfgJSON, err := json.Marshal(ccfg)
				require_NoError(t, err)
				sa.consumers[cn] = &consumerAssignment{
					Client: &ClientInfo{Account: globalAccountName}, Stream: s.name, Name: cn,
					Config: ccfg, ConfigJSON: ccfgJSON,
				}
			}
			asa[s.name] = sa
		}
		snap, _, _, err := js.encodeMetaSnapshot(map[string]map[string]*streamAssignment{globalAccountName: asa})
		require_NoError(t, err)
		return []*Entry{{EntrySnapshot, snap}}
	}

	// Collect the names staged in each part of ru, sorted for stable comparison.
	streamNames := func(m map[string]*streamAssignment) []string {
		var out []string
		for _, sa := range m {
			out = append(out, sa.Config.Name)
		}
		slices.Sort(out)
		return out
	}
	consumerNames := func(m map[string]map[string]*consumerAssignment) []string {
		var out []string
		for _, cs := range m {
			for _, ca := range cs {
				out = append(out, ca.Name)
			}
		}
		slices.Sort(out)
		return out
	}
	assertNames := func(t *testing.T, label string, got, want []string) {
		t.Helper()
		want = slices.Clone(want)
		slices.Sort(want)
		if !slices.Equal(got, want) {
			t.Fatalf("%s: got %v, want %v", label, got, want)
		}
	}

	for _, test := range []struct {
		name string
		// Entries staged during recovery, before the snapshot arrives.
		entries []*Entry
		// Streams (and their consumers) the compacted snapshot contains.
		snapshot []snapStream
		// Staged state expected after the entries, before the snapshot reconciles them.
		preStreams   []string
		preUpdates   []string
		preConsumers []string
		preRemoves   []string
		// Staged state expected after the snapshot has reconciled the staged updates.
		wantStreams   []string
		wantUpdates   []string
		wantConsumers []string
		wantRemoves   []string
	}{
		{
			// A stream staged for add that the snapshot omits is a phantom and must be dropped;
			// a stream the snapshot contains is (re)staged as an add.
			name:        "deleted stream",
			entries:     []*Entry{addStream("TEST")},
			snapshot:    []snapStream{{name: "KEEP"}},
			preStreams:  []string{"TEST"},
			wantStreams: []string{"KEEP"},
		},
		{
			// When the stream itself is superseded, its staged consumer add (CA) and remove (CB)
			// must be dropped along with it.
			name: "deleted stream with consumers",
			entries: []*Entry{
				addStream("TEST"),
				addConsumer("TEST", "CA"),
				addConsumer("TEST", "CB"),
				delConsumer("TEST", "CB"),
			},
			snapshot:     []snapStream{{name: "KEEP"}},
			preStreams:   []string{"TEST"},
			preConsumers: []string{"CA"},
			preRemoves:   []string{"CB"},
			wantStreams:  []string{"KEEP"},
		},
		{
			// The stream survives, but a staged consumer (C2) the snapshot omits is a phantom and
			// must be dropped; the consumer the snapshot still contains (C1) is kept.
			name: "deleted consumer",
			entries: []*Entry{
				addStream("TEST"),
				addConsumer("TEST", "C1"),
				addConsumer("TEST", "C2"),
			},
			snapshot:      []snapStream{{name: "TEST", consumers: []string{"C1"}}},
			preStreams:    []string{"TEST"},
			preConsumers:  []string{"C1", "C2"},
			wantStreams:   []string{"TEST"},
			wantConsumers: []string{"C1"},
		},
		{
			// Nothing is a phantom: every staged consumer is present in the snapshot, and the
			// snapshot also (re)stages its own consumers (C2 here was not staged via an entry).
			name: "snapshot adds consumer",
			entries: []*Entry{
				addStream("TEST"),
				addConsumer("TEST", "C1"),
			},
			snapshot:      []snapStream{{name: "TEST", consumers: []string{"C1", "C2"}}},
			preStreams:    []string{"TEST"},
			preConsumers:  []string{"C1"},
			wantStreams:   []string{"TEST"},
			wantConsumers: []string{"C1", "C2"},
		},
		{
			// Both paths at once: stream KEEP (and consumer KC) survive, while stream GONE (and
			// its consumer GC) are superseded and dropped.
			name: "mixed stream delete",
			entries: []*Entry{
				addStream("KEEP"),
				addConsumer("KEEP", "KC"),
				addStream("GONE"),
				addConsumer("GONE", "GC"),
			},
			snapshot:      []snapStream{{name: "KEEP", consumers: []string{"KC"}}},
			preStreams:    []string{"GONE", "KEEP"},
			preConsumers:  []string{"GC", "KC"},
			wantStreams:   []string{"KEEP"},
			wantConsumers: []string{"KC"},
		},
		{
			// An empty snapshot (all streams deleted) supersedes everything staged.
			name: "empty snapshot",
			entries: []*Entry{
				addStream("TEST"),
				addConsumer("TEST", "C1"),
			},
			snapshot:     nil,
			preStreams:   []string{"TEST"},
			preConsumers: []string{"C1"},
		},
		{
			// A stream created and then updated during recovery is staged once, in addStreams,
			// with the updated assignment (the update replaces the staged add). When superseded
			// it is dropped.
			name: "added-and-updated stream",
			entries: []*Entry{
				addStream("TEST"),
				updateStream("TEST"),
			},
			snapshot:    []snapStream{{name: "KEEP"}},
			preStreams:  []string{"TEST"},
			wantStreams: []string{"KEEP"},
		},
		{
			// A stream created and then updated during recovery is staged once, in addStreams,
			// with the updated assignment. When the snapshot keeps the stream it re-stages it as
			// an add; nothing is left in updateStreams to be reapplied after the add.
			name: "added-and-updated stream kept",
			entries: []*Entry{
				addStream("TEST"),
				updateStream("TEST"),
			},
			snapshot:    []snapStream{{name: "TEST"}},
			preStreams:  []string{"TEST"},
			wantStreams: []string{"TEST"},
		},
		{
			// A stream staged only in updateStreams (its add is outside this recovery batch) is
			// reconciled by the dedicated updateStreams loop: when superseded it and its staged
			// consumer ops are dropped. This is the path the addStreams loop does not cover.
			name: "update-only stream",
			entries: []*Entry{
				updateStream("GONE"),
				addConsumer("GONE", "GC"),
			},
			snapshot:     []snapStream{{name: "KEEP"}},
			preUpdates:   []string{"GONE"},
			preConsumers: []string{"GC"},
			wantStreams:  []string{"KEEP"},
		},
		{
			// A stream staged only via an update, that the snapshot keeps: the snapshot re-stages it as
			// an add which supersedes the now-stale staged update (so it is not reapplied after the add
			// at recovery completion). The phantom consumer under it (C2) is still dropped; C1 survives.
			name: "update-only stream kept",
			entries: []*Entry{
				updateStream("TEST"),
				addConsumer("TEST", "C1"),
				addConsumer("TEST", "C2"),
			},
			snapshot:     []snapStream{{name: "TEST", consumers: []string{"C1"}}},
			preUpdates:   []string{"TEST"},
			preConsumers: []string{"C1", "C2"},
			wantStreams:  []string{"TEST"},
			// wantUpdates intentionally empty: the snapshot's re-add supersedes the stale staged update.
			wantConsumers: []string{"C1"},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			// Need to be recovering so that we accumulate recoveryUpdates.
			js.setMetaRecovering()
			ru := &recoveryUpdates{
				removeStreams:   make(map[string]*streamAssignment),
				removeConsumers: make(map[string]map[string]*consumerAssignment),
				addStreams:      make(map[string]*streamAssignment),
				updateStreams:   make(map[string]*streamAssignment),
				updateConsumers: make(map[string]map[string]*consumerAssignment),
			}

			// Stage the recovery entries, then verify what they staged (so the snapshot assertions
			// below are not vacuously satisfied by entries that never staged anything).
			_, _, err := js.applyMetaEntries(test.entries, ru)
			require_NoError(t, err)
			assertNames(t, "pre stream adds", streamNames(ru.addStreams), test.preStreams)
			assertNames(t, "pre stream updates", streamNames(ru.updateStreams), test.preUpdates)
			assertNames(t, "pre consumers", consumerNames(ru.updateConsumers), test.preConsumers)
			assertNames(t, "pre removes", consumerNames(ru.removeConsumers), test.preRemoves)

			// Apply the compacted snapshot, which reconciles the staged updates against it.
			_, _, err = js.applyMetaEntries(encodeSnapshot(t, test.snapshot...), ru)
			require_NoError(t, err)
			assertNames(t, "stream adds", streamNames(ru.addStreams), test.wantStreams)
			assertNames(t, "stream updates", streamNames(ru.updateStreams), test.wantUpdates)
			assertNames(t, "consumer adds", consumerNames(ru.updateConsumers), test.wantConsumers)
			assertNames(t, "consumer removes", consumerNames(ru.removeConsumers), test.wantRemoves)
		})
	}
}

func TestJetStreamClusterMetaRecoveryRecreateFileStreamAsMemory(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "R3S", 3)
	defer c.shutdown()

	js := c.leader().getJetStream()

	createFileStream := []*Entry{
		{EntryNormal, encodeAddStreamAssignment(&streamAssignment{
			Config: &StreamConfig{Name: "TEST", Storage: FileStorage},
		})},
	}

	deleteFileStream := []*Entry{
		{EntryNormal, encodeDeleteStreamAssignment(&streamAssignment{
			Config: &StreamConfig{Name: "TEST", Storage: FileStorage},
		})},
	}

	createMemoryStream := []*Entry{
		{EntryNormal, encodeAddStreamAssignment(&streamAssignment{
			Config: &StreamConfig{Name: "TEST", Storage: FileStorage},
		})},
	}

	createConsumer := []*Entry{
		{EntryNormal, encodeAddConsumerAssignment(&consumerAssignment{
			Stream: "TEST",
			Config: &ConsumerConfig{Name: "consumer"},
		})},
	}

	// Need to be recovering so that we accumulate recoveryUpdates.
	js.setMetaRecovering()
	ru := &recoveryUpdates{
		removeStreams:   make(map[string]*streamAssignment),
		removeConsumers: make(map[string]map[string]*consumerAssignment),
		addStreams:      make(map[string]*streamAssignment),
		updateStreams:   make(map[string]*streamAssignment),
		updateConsumers: make(map[string]map[string]*consumerAssignment),
	}

	// We created a file-based stream first, but deleted it shortly after.
	_, _, err := js.applyMetaEntries(createFileStream, ru)
	require_NoError(t, err)
	require_Len(t, len(ru.addStreams), 1)
	require_Len(t, len(ru.removeStreams), 0)

	// Now push another recovery entry that deletes the stream.
	// The file-based stream should not have been created.
	_, _, err = js.applyMetaEntries(deleteFileStream, ru)
	require_NoError(t, err)
	require_Len(t, len(ru.addStreams), 0)
	require_Len(t, len(ru.removeStreams), 1)

	// Now stage a memory-based stream to be created.
	_, _, err = js.applyMetaEntries(createMemoryStream, ru)
	require_NoError(t, err)
	require_Len(t, len(ru.addStreams), 1)
	require_Len(t, len(ru.removeStreams), 1)
	require_Len(t, len(ru.updateConsumers), 0)

	// Also create a consumer on that memory-based stream.
	_, _, err = js.applyMetaEntries(createConsumer, ru)
	require_NoError(t, err)
	require_Len(t, len(ru.addStreams), 1)
	require_Len(t, len(ru.removeStreams), 1)
	require_Len(t, len(ru.updateConsumers), 1)
}

func TestJetStreamClusterMetaRecoveryConsumerCreateAndRemove(t *testing.T) {
	tests := []struct {
		title                       string
		encodeAddConsumerAssignment func(ca *consumerAssignment) []byte
	}{
		{title: "simple", encodeAddConsumerAssignment: encodeAddConsumerAssignment},
		{title: "compressed", encodeAddConsumerAssignment: encodeAddConsumerAssignmentCompressed},
	}
	for _, test := range tests {
		t.Run(test.title, func(t *testing.T) {
			c := createJetStreamClusterExplicit(t, "R3S", 3)
			defer c.shutdown()

			js := c.leader().getJetStream()

			ca := &consumerAssignment{Stream: "TEST", Name: "consumer"}
			createConsumer := []*Entry{{EntryNormal, test.encodeAddConsumerAssignment(ca)}}
			deleteConsumer := []*Entry{{EntryNormal, encodeDeleteConsumerAssignment(ca)}}

			// Need to be recovering so that we accumulate recoveryUpdates.
			js.setMetaRecovering()
			ru := &recoveryUpdates{
				removeStreams:   make(map[string]*streamAssignment),
				removeConsumers: make(map[string]map[string]*consumerAssignment),
				addStreams:      make(map[string]*streamAssignment),
				updateStreams:   make(map[string]*streamAssignment),
				updateConsumers: make(map[string]map[string]*consumerAssignment),
			}

			// Creating the consumer should append to update consumers list.
			_, _, err := js.applyMetaEntries(createConsumer, ru)
			require_NoError(t, err)
			require_Len(t, len(ru.updateConsumers[":TEST"]), 1)
			require_Len(t, len(ru.removeConsumers), 0)

			// Deleting the consumer should append to remove consumers list and remove from update list.
			_, _, err = js.applyMetaEntries(deleteConsumer, ru)
			require_NoError(t, err)
			require_Len(t, len(ru.removeConsumers[":TEST"]), 1)
			require_Len(t, len(ru.updateConsumers[":TEST"]), 0)

			// When re-creating the consumer, add to update list.
			_, _, err = js.applyMetaEntries(createConsumer, ru)
			require_NoError(t, err)
			require_Len(t, len(ru.removeConsumers[":TEST"]), 1)
			require_Len(t, len(ru.updateConsumers[":TEST"]), 1)
		})
	}
}

func TestJetStreamClusterMetaRecoveryAddAndUpdateStream(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "R3S", 3)
	defer c.shutdown()

	js := c.leader().getJetStream()
	ci := &ClientInfo{Account: globalAccountName, Cluster: "R3S"}
	cfg := &StreamConfig{Name: "TEST", Storage: FileStorage, Replicas: 3}
	rg, perr := js.createGroupForStream(ci, cfg)
	if perr != nil {
		require_NoError(t, perr)
	}
	sa := &streamAssignment{Client: ci, Config: cfg, Group: rg}

	// Need to be recovering so that we accumulate recoveryUpdates.
	js.setMetaRecovering()
	ru := &recoveryUpdates{
		removeStreams:   make(map[string]*streamAssignment),
		removeConsumers: make(map[string]map[string]*consumerAssignment),
		addStreams:      make(map[string]*streamAssignment),
		updateStreams:   make(map[string]*streamAssignment),
		updateConsumers: make(map[string]map[string]*consumerAssignment),
	}

	// We create a new stream, and we'll update it shortly after.
	entries := []*Entry{{EntryNormal, encodeAddStreamAssignment(sa)}}
	_, _, err := js.applyMetaEntries(entries, ru)
	require_NoError(t, err)
	require_Len(t, len(ru.addStreams), 1)
	require_Len(t, len(ru.updateStreams), 0)
	require_Len(t, len(ru.removeStreams), 0)

	// Now update the stream. The stream is still only staged for creation, so the
	// update must replace the staged add: creating it from the old assignment and
	// updating it afterwards would briefly run the stale assignment.
	sa.Config.Subjects = []string{"foo"}
	entries = []*Entry{{EntryNormal, encodeUpdateStreamAssignment(sa)}}
	_, _, err = js.applyMetaEntries(entries, ru)
	require_NoError(t, err)
	require_Len(t, len(ru.addStreams), 1)
	require_Len(t, len(ru.updateStreams), 0)
	require_Len(t, len(ru.removeStreams), 0)

	// Check the stream is added with the updated assignment.
	for _, sa := range ru.addStreams {
		js.processStreamAssignment(sa)
	}
	sa = js.streamAssignment(globalAccountName, "TEST")
	require_NotNil(t, sa)
	require_Len(t, len(sa.Config.Subjects), 1)

	// An update for a stream that already exists is staged as an update.
	sa.Config.Subjects = []string{"foo", "bar"}
	entries = []*Entry{{EntryNormal, encodeUpdateStreamAssignment(sa)}}
	ru.addStreams = make(map[string]*streamAssignment)
	_, _, err = js.applyMetaEntries(entries, ru)
	require_NoError(t, err)
	require_Len(t, len(ru.addStreams), 0)
	require_Len(t, len(ru.updateStreams), 1)
	for _, sa := range ru.updateStreams {
		js.processUpdateStreamAssignment(sa)
	}
	sa = js.streamAssignment(globalAccountName, "TEST")
	require_NotNil(t, sa)
	require_Len(t, len(sa.Config.Subjects), 2)
}

// https://github.com/nats-io/nats-server/issues/7229
func TestJetStreamClusterProcessClusterUpdateStreamNilStreamAssignment(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "R3S", 3)
	defer c.shutdown()

	nc, js := jsClientConnect(t, c.randomServer())
	defer nc.Close()

	_, err := js.AddStream(&nats.StreamConfig{Name: "TEST", Subjects: []string{"foo"}, Replicas: 3})
	require_NoError(t, err)

	s := c.randomServer()
	acc, err := s.lookupAccount(globalAccountName)
	require_NoError(t, err)
	// Only the stream leader is known to have created the stream when it responds.
	var mset *stream
	checkFor(t, 2*time.Second, 10*time.Millisecond, func() error {
		mset, err = acc.lookupStream("TEST")
		return err
	})

	// Capture osa and build a sa that reflects the post-restart meta-replay
	// conditions: a scale-up path where both the old and new raftGroup nodes
	// are nil on this server (alreadyRunning=false, needsNode=true).
	sjs := s.getJetStream()
	sjs.mu.Lock()
	realOsa := sjs.streamAssignment(globalAccountName, "TEST")
	if realOsa == nil {
		sjs.mu.Unlock()
		t.Fatal("stream assignment not found")
	}

	// Intentionally omit Group.node so that in processClusterUpdateStream
	// alreadyRunning (osa.Group.node != nil) is false and needsNode
	// (sa.Group.node == nil) is true, taking the scale-up branch.
	cloneGroup := func() *raftGroup {
		return &raftGroup{
			Name:    realOsa.Group.Name,
			Peers:   append([]string{}, realOsa.Group.Peers...),
			Storage: realOsa.Group.Storage,
		}
	}
	osa := realOsa.clone()
	osa.Group = cloneGroup()
	sa := osa.clone()
	cfg := *realOsa.Config
	cfg.MaxMsgs = 1_000
	sa.Config = &cfg
	sa.Group = cloneGroup()
	sjs.mu.Unlock()

	// Simulate the state right after recoverStream() before meta catchup:
	// no stream assignment, no raft node, no cluster subs registered yet.
	mset.mu.Lock()
	mset.sa = nil
	mset.node = nil
	if mset.syncSub != nil {
		mset.srv.sysUnsubscribe(mset.syncSub)
		mset.syncSub = nil
	}
	mset.mu.Unlock()

	sjs.processClusterUpdateStream(acc, osa, sa)

	mset.mu.RLock()
	got := mset.sa
	mset.mu.RUnlock()
	require_NotNil(t, got)
}

// Make sure if we received acks that are out of bounds, meaning past our
// last sequence or before our first that they are ignored and errored if applicable.
func TestJetStreamClusterConsumerAckOutOfBounds(t *testing.T) {
	testJetStreamClusterConsumerAckOutOfBounds(t, false)
}

func TestJetStreamClusterConsumerAckV2OutOfBounds(t *testing.T) {
	testJetStreamClusterConsumerAckOutOfBounds(t, true)
}

func testJetStreamClusterConsumerAckOutOfBounds(t *testing.T, ackV2 bool) {
	c := createJetStreamClusterExplicit(t, "R3S", 3)
	defer c.shutdown()

	for _, s := range c.servers {
		opts := *s.getOpts()
		opts.FeatureFlags = map[string]bool{FeatureFlagJsAckFormatV2: ackV2}
		s.setOpts(&opts)
	}

	nc, js := jsClientConnect(t, c.randomServer())
	defer nc.Close()

	_, err := js.AddStream(&nats.StreamConfig{
		Name:      "TEST",
		Subjects:  []string{"foo.*"},
		Retention: nats.WorkQueuePolicy,
		Replicas:  3,
	})
	require_NoError(t, err)

	for i := 0; i < 10; i++ {
		_, err = js.Publish("foo.bar", []byte("OK"))
		require_NoError(t, err)
	}

	sub, err := js.PullSubscribe("foo.*", "C")
	require_NoError(t, err)

	msgs, err := sub.Fetch(1)
	require_NoError(t, err)
	require_Equal(t, len(msgs), 1)
	msgs[0].AckSync()

	// Now ack way past the last sequence.
	cl := c.consumerLeader(globalAccountName, "TEST", "C")
	require_NotNil(t, cl)
	mset, err := cl.globalAccount().lookupStream("TEST")
	require_NoError(t, err)
	o := mset.lookupConsumer("C")
	require_NotNil(t, o)
	o.mu.RLock()
	ackReply := o.ackReply(10000000000, 0, 1, 0, 0)
	o.mu.RUnlock()
	if ackV2 {
		require_Equal(t, ackReply, "$JS.ACK._.szMpdrwD.TEST.C.1.10000000000.0.0.0")
	} else {
		require_Equal(t, ackReply, "$JS.ACK.TEST.C.1.10000000000.0.0.0")
	}
	_, err = nc.Request(ackReply, nil, 250*time.Millisecond)
	require_Error(t, err, nats.ErrTimeout)

	// Make sure that now changes happened to our state.
	ci, err := js.ConsumerInfo("TEST", "C")
	require_NoError(t, err)
	require_Equal(t, ci.Delivered.Consumer, 1)
	require_Equal(t, ci.Delivered.Stream, 1)
	require_Equal(t, ci.AckFloor.Consumer, 1)
	require_Equal(t, ci.AckFloor.Stream, 1)

	s := c.consumerLeader("$G", "TEST", "C")
	s.Shutdown()
	s.WaitForShutdown()
	c.restartServer(s)
	c.waitOnConsumerLeader(globalAccountName, "TEST", "C")

	// Confirm new leader has same state for delivered and ack floor.
	ci, err = js.ConsumerInfo("TEST", "C")
	require_NoError(t, err)
	require_Equal(t, ci.Delivered.Consumer, 1)
	require_Equal(t, ci.Delivered.Stream, 1)
	require_Equal(t, ci.AckFloor.Consumer, 1)
	require_Equal(t, ci.AckFloor.Stream, 1)
}

func TestJetStreamClusterCatchupLoadNextMsgTooManyDeletes(t *testing.T) {
	tests := []struct {
		title          string
		catchupRequest *streamSyncRequest
		setup          func(js nats.JetStreamContext)
		assert         func(sub *nats.Subscription)
	}{
		{
			title: "within-delete-gap",
			setup: func(js nats.JetStreamContext) {},
		},
		{
			title: "EOF",
			setup: func(js nats.JetStreamContext) {
				err := js.DeleteMsg("TEST", 100)
				require_NoError(t, err)
			},
		},
	}

	for _, test := range tests {
		t.Run(test.title, func(t *testing.T) {
			c := createJetStreamClusterExplicit(t, "R3S", 3)
			defer c.shutdown()

			nc, js := jsClientConnect(t, c.randomServer())
			defer nc.Close()

			_, err := js.AddStream(&nats.StreamConfig{
				Name:     "TEST",
				Subjects: []string{"foo", "bar"},
				Replicas: 3,
			})
			require_NoError(t, err)

			// Starts and ends with subject "foo", we'll purge so there's a large gap of deletes in the middle.
			// This should force runCatchup to use LoadNextMsg instead of LoadMsg.
			for i := 0; i < 100; i++ {
				subject := "bar"
				if i == 0 || i == 99 {
					subject = "foo"
				}
				_, err = js.Publish(subject, nil)
				require_NoError(t, err)
			}
			err = js.PurgeStream("TEST", &nats.StreamPurgeRequest{Subject: "bar"})
			require_NoError(t, err)

			// Optionally run some extra setup.
			test.setup(js)

			// Reconnect to stream leader.
			l := c.streamLeader(globalAccountName, "TEST")
			nc.Close()
			nc, _ = jsClientConnect(t, l, nats.UserInfo("admin", "s3cr3t!"))
			defer nc.Close()

			// Setup wiretap and grab stream.
			sendSubject := "test-wiretap"
			sub, err := nc.SubscribeSync(sendSubject)
			require_NoError(t, err)
			err = nc.Flush() // Must flush, otherwise our subscription could be too late.
			require_NoError(t, err)
			acc, err := l.lookupAccount(globalAccountName)
			require_NoError(t, err)
			mset, err := acc.lookupStream("TEST")
			require_NoError(t, err)

			// Run custom catchup request and the test's asserts.
			sreq := &streamSyncRequest{Peer: "peer", FirstSeq: 5, LastSeq: 5, DeleteRangesOk: true}
			require_True(t, mset.srv.startGoRoutine(func() { mset.runCatchup(sendSubject, sreq) }))

			// Our first message should be a skip msg.
			msg, err := sub.NextMsg(time.Second)
			require_NoError(t, err)
			require_Equal(t, entryOp(msg.Data[0]), streamMsgOp)
			subj, _, _, _, seq, ts, _, err := decodeStreamMsg(msg.Data[1:])
			require_NoError(t, err)
			require_Equal(t, seq, 5)
			require_Equal(t, subj, _EMPTY_)
			require_Equal(t, ts, 0)

			// And end with EOF.
			msg, err = sub.NextMsg(time.Second)
			require_NoError(t, err)
			require_Len(t, len(msg.Data), 0)
		})
	}
}

func TestJetStreamClusterCatchupMustStallWhenBehindOnApplies(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "R3S", 3)
	defer c.shutdown()

	nc, js := jsClientConnect(t, c.randomServer())
	defer nc.Close()

	_, err := js.AddStream(&nats.StreamConfig{
		Name:     "TEST",
		Subjects: []string{"foo"},
		Replicas: 3,
	})
	require_NoError(t, err)

	_, err = js.Publish("foo", nil)
	require_NoError(t, err)

	// Reconnect to stream leader.
	l := c.streamLeader(globalAccountName, "TEST")
	nc.Close()
	nc, _ = jsClientConnect(t, l, nats.UserInfo("admin", "s3cr3t!"))
	defer nc.Close()

	// Setup wiretap and grab stream.
	sendSubject := "test-wiretap"
	sub, err := nc.SubscribeSync(sendSubject)
	require_NoError(t, err)
	err = nc.Flush() // Must flush, otherwise our subscription could be too late.
	require_NoError(t, err)
	acc, err := l.lookupAccount(globalAccountName)
	require_NoError(t, err)
	mset, err := acc.lookupStream("TEST")
	require_NoError(t, err)

	// We have a message at sequence 1, so expect a successful catchup.
	sreq1 := &streamSyncRequest{Peer: "peer", FirstSeq: 1, LastSeq: 1, DeleteRangesOk: true}
	require_True(t, mset.srv.startGoRoutine(func() { mset.runCatchup(sendSubject, sreq1) }))
	// Expect the message at sequence 1.
	msg, err := sub.NextMsg(time.Second)
	require_NoError(t, err)
	require_Equal(t, entryOp(msg.Data[0]), streamMsgOp)
	subj, _, _, _, seq, _, _, err := decodeStreamMsg(msg.Data[1:])
	require_NoError(t, err)
	require_Equal(t, seq, 1)
	require_Equal(t, subj, "foo")
	// And end with EOF.
	msg, err = sub.NextMsg(time.Second)
	require_NoError(t, err)
	require_Len(t, len(msg.Data), 0)

	// Add one additional entry into the log that's not applied yet.
	n := mset.node.(*raft)
	n.Lock()
	ae := n.buildAppendEntry(nil)
	err = n.storeToWAL(ae)
	n.Unlock()
	index, commit, applied := n.Progress()
	require_NoError(t, err)
	require_LessThan(t, applied, index)
	require_Equal(t, commit, applied)
	// We have a message at sequence 1, but we haven't applied as many append entries.
	// We can't fulfill the request right now as we don't know yet if
	// that message will be deleted as part of upcoming append entries.
	sreq2 := &streamSyncRequest{Peer: "peer", FirstSeq: 1, LastSeq: 1, DeleteRangesOk: true, MinApplied: index}
	require_True(t, mset.srv.startGoRoutine(func() { mset.runCatchup(sendSubject, sreq2) }))
	_, err = sub.NextMsg(time.Second)
	require_Error(t, err, nats.ErrTimeout)

	// We have a message at sequence 1, but we haven't applied as many append entries.
	// Also, we seem to have a log that doesn't contain enough entries, even though we became leader.
	// Something has already gone wrong and got the logs to desync.
	// Value availability here and just fulfill the request.
	sreq3 := &streamSyncRequest{Peer: "peer", FirstSeq: 1, LastSeq: 1, DeleteRangesOk: true, MinApplied: 100}
	require_True(t, mset.srv.startGoRoutine(func() { mset.runCatchup(sendSubject, sreq3) }))
	// Expect the message at sequence 1.
	msg, err = sub.NextMsg(time.Second)
	require_NoError(t, err)
	require_Equal(t, entryOp(msg.Data[0]), streamMsgOp)
	subj, _, _, _, seq, _, _, err = decodeStreamMsg(msg.Data[1:])
	require_NoError(t, err)
	require_Equal(t, seq, 1)
	require_Equal(t, subj, "foo")
	// And end with EOF.
	msg, err = sub.NextMsg(time.Second)
	require_NoError(t, err)
	require_Len(t, len(msg.Data), 0)
}

func TestJetStreamClusterConsumerInfoAfterCreate(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "R3S", 3)
	defer c.shutdown()

	nl := c.randomNonLeader()
	nc, js := jsClientConnect(t, nl)
	defer nc.Close()

	cfg := &nats.StreamConfig{
		Name:     "TEST",
		Subjects: []string{"foo"},
		Replicas: 3,
	}
	si, err := js.AddStream(cfg)
	require_NoError(t, err)

	// We want to ensure the consumer can be created and be applied.
	// On the non-meta-leader server we'll pause applies, so need to make
	// sure the consumer is not created on that server.
	if si.Cluster.Leader == nl.Name() {
		ml := c.leader()
		jreq, err := json.Marshal(&JSApiLeaderStepdownRequest{Placement: &Placement{Preferred: ml.Name()}})
		require_NoError(t, err)
		resp, err := nc.Request(fmt.Sprintf(JSApiStreamLeaderStepDownT, "TEST"), jreq, time.Second)
		require_NoError(t, err)
		var sdr JSApiLeaderStepDownResponse
		require_NoError(t, json.Unmarshal(resp.Data, &sdr))
	}

	// Wait some time for the leader to settle.
	time.Sleep(500 * time.Millisecond)

	// Scale down to ensure the consumer gets created on this server.
	cfg.Replicas = 1
	si, err = js.UpdateStream(cfg)
	require_NoError(t, err)
	require_NotEqual(t, si.Cluster.Leader, nl.Name())

	// Wait for the scale down to be reconciled, otherwise the consumer could
	// be placed on a peer that's still being removed from the stream.
	c.waitOnStreamLeader(globalAccountName, "TEST")

	// We pause applies for the server we're connected to.
	// This is fine for the RAFT log and allowing the consumer to be created,
	// but we will not be able to apply the consumer assignment for some time.
	mjs := nl.getJetStream()
	require_NotNil(t, mjs)
	mg := mjs.getMetaGroup()
	require_NotNil(t, mg)
	err = mg.(*raft).PauseApply()
	require_NoError(t, err)

	// Add consumer.
	_, err = js.AddConsumer("TEST", &nats.ConsumerConfig{Durable: "CONSUMER"})
	require_NoError(t, err)

	// Consumer info should not fail, this server should not short-circuit because
	// it was not able to apply the consumer assignment.
	_, err = js.ConsumerInfo("TEST", "CONSUMER")
	require_NoError(t, err)

	// Resume applies.
	mg.(*raft).ResumeApply()

	// Check consumer info still works.
	_, err = js.ConsumerInfo("TEST", "CONSUMER")
	require_NoError(t, err)
}

func TestJetStreamClusterStreamUpscalePeersAfterDownscale(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "R5S", 5)
	defer c.shutdown()

	nc, js := jsClientConnect(t, c.randomServer())
	defer nc.Close()

	checkPeerSet := func() {
		t.Helper()
		checkFor(t, 5*time.Second, 500*time.Millisecond, func() error {
			for _, s := range c.servers {
				acc, err := s.lookupAccount(globalAccountName)
				if err != nil {
					return err
				}
				mset, err := acc.lookupStream("TEST")
				if err != nil {
					return err
				}
				peers := mset.raftNode().Peers()
				if len(peers) != 5 {
					return fmt.Errorf("expected 5 peers, got %d", len(peers))
				}
			}
			return nil
		})
	}

	cfg := &nats.StreamConfig{
		Name:     "TEST",
		Subjects: []string{"foo"},
		Replicas: 5,
	}
	_, err := js.AddStream(cfg)
	require_NoError(t, err)

	checkPeerSet()

	cfg.Replicas = 3
	_, err = js.UpdateStream(cfg)
	require_NoError(t, err)

	cfg.Replicas = 5
	_, err = js.UpdateStream(cfg)
	if err != nil {
		// If still in progress, wait for it to complete before retrying.
		require_Error(t, err, NewJSStreamMoveInProgressError())
		c.waitOnStreamLeader(globalAccountName, "TEST")
		_, err = js.UpdateStream(cfg)
	}
	require_NoError(t, err)

	checkPeerSet()
}

func TestJetStreamClusterClearAllPreAcksOnRemoveMsg(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "R3S", 3)
	defer c.shutdown()

	nc, js := jsClientConnect(t, c.randomServer())
	defer nc.Close()

	_, err := js.AddStream(&nats.StreamConfig{
		Name:      "TEST",
		Subjects:  []string{"foo"},
		Replicas:  3,
		Retention: nats.WorkQueuePolicy,
	})
	require_NoError(t, err)

	_, err = js.AddConsumer("TEST", &nats.ConsumerConfig{
		Durable:   "CONSUMER",
		AckPolicy: nats.AckExplicitPolicy,
	})
	require_NoError(t, err)

	for i := 0; i < 3; i++ {
		_, err = js.Publish("foo", nil)
		require_NoError(t, err)
	}

	// Wait for all servers to converge on the same state.
	checkFor(t, 5*time.Second, 500*time.Millisecond, func() error {
		return checkState(t, c, globalAccountName, "TEST")
	})

	// Register pre-acks on all servers.
	// Normally this can't happen as the stream leader will have the message that's acked available, just for testing.
	for _, s := range c.servers {
		acc, err := s.lookupAccount(globalAccountName)
		require_NoError(t, err)
		mset, err := acc.lookupStream("TEST")
		require_NoError(t, err)
		o := mset.lookupConsumer("CONSUMER")
		require_NotNil(t, o)

		// Register pre-acks for the 3 messages.
		mset.registerPreAckLock(o, 1)
		mset.registerPreAckLock(o, 2)
		mset.registerPreAckLock(o, 3)
	}

	// Check there's an expected amount of pre-acks, and there are no pre-acks for the given sequence.
	checkPreAcks := func(seq uint64, expected int) {
		t.Helper()
		checkFor(t, 5*time.Second, time.Second, func() error {
			for _, s := range c.servers {
				acc, err := s.lookupAccount(globalAccountName)
				if err != nil {
					return err
				}
				mset, err := acc.lookupStream("TEST")
				if err != nil {
					return err
				}
				mset.mu.RLock()
				numPreAcks := len(mset.preAcks)
				numSeqPreAcks := len(mset.preAcks[seq])
				mset.mu.RUnlock()
				if numPreAcks != expected {
					return fmt.Errorf("expected %d pre-acks, got %d", expected, numPreAcks)
				}
				if seq > 0 && numSeqPreAcks != 0 {
					return fmt.Errorf("expected 0 pre-acks for seq %d, got %d", seq, numSeqPreAcks)
				}
			}
			return nil
		})
	}
	// Check all pre-acks were registered.
	checkPreAcks(0, 3)

	// Deleting the message should clear the pre-ack.
	err = js.DeleteMsg("TEST", 1)
	require_NoError(t, err)
	checkPreAcks(1, 2)

	// Erasing the message should clear the pre-ack.
	err = js.SecureDeleteMsg("TEST", 2)
	require_NoError(t, err)
	checkPreAcks(2, 1)

	// Purging should clear all pre-acks below the purged floor.
	err = js.PurgeStream("TEST", &nats.StreamPurgeRequest{Sequence: 4})
	require_NoError(t, err)
	checkPreAcks(3, 0)
}

func TestJetStreamClusterStreamHealthCheckMustNotRecreate(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "R3S", 3)
	defer c.shutdown()

	nc, js := jsClientConnect(t, c.randomServer())
	defer nc.Close()

	waitForStreamAssignments := func() {
		t.Helper()
		checkFor(t, 5*time.Second, time.Second, func() error {
			for _, s := range c.servers {
				js := s.getJetStream()
				js.mu.RLock()
				sa := js.streamAssignment(globalAccountName, "TEST")
				js.mu.RUnlock()
				if sa == nil {
					return fmt.Errorf("stream assignment not found on %s", s.Name())
				}
			}
			return nil
		})
	}
	waitForNoStreamAssignments := func() {
		t.Helper()
		checkFor(t, 5*time.Second, time.Second, func() error {
			for _, s := range c.servers {
				js := s.getJetStream()
				js.mu.RLock()
				sa := js.streamAssignment(globalAccountName, "TEST")
				js.mu.RUnlock()
				if sa != nil {
					return fmt.Errorf("stream assignment still available on %s", s.Name())
				}
			}
			return nil
		})
	}
	getStreamAssignment := func(rs *Server) (*jetStream, *Account, *streamAssignment, *stream) {
		acc, err := rs.lookupAccount(globalAccountName)
		require_NoError(t, err)
		mset, err := acc.lookupStream("TEST")
		require_NoError(t, err)

		sjs := rs.getJetStream()
		sjs.mu.Lock()
		defer sjs.mu.Unlock()

		sas := sjs.cluster.streams[globalAccountName]
		require_True(t, sas != nil)
		sa := sas["TEST"]
		require_True(t, sa != nil)
		sa.Created = time.Time{}
		return sjs, acc, sa, mset
	}
	checkNodeIsClosed := func(sjs *jetStream, sa *streamAssignment) {
		t.Helper()
		sjs.mu.RLock()
		defer sjs.mu.RUnlock()
		require_True(t, sa.Group != nil)
		require_True(t, sa.Group.node != nil)
		require_Equal(t, sa.Group.node.State(), Closed)
	}

	_, err := js.AddStream(&nats.StreamConfig{
		Name:     "TEST",
		Subjects: []string{"foo"},
		Replicas: 3,
	})
	require_NoError(t, err)
	waitForStreamAssignments()

	// We manually stop the RAFT node and ensure it doesn't get restarted.
	rs := c.randomNonStreamLeader(globalAccountName, "TEST")
	sjs, acc, sa, mset := getStreamAssignment(rs)
	sjs.mu.RLock()
	rg := sa.Group
	var n RaftNode
	if rg != nil {
		n = rg.node
	}
	sjs.mu.RUnlock()
	require_True(t, rg != nil)
	require_True(t, n != nil)
	n.Stop()
	n.WaitForStop()

	// We wait for the monitor to exit, so we can set the flag back manually.
	checkFor(t, 5*time.Second, time.Second, func() error {
		mset.mu.RLock()
		defer mset.mu.RUnlock()
		if mset.inMonitor {
			return errors.New("waiting for monitor to stop")
		}
		return nil
	})
	mset.mu.Lock()
	mset.inMonitor = true
	mset.mu.Unlock()

	// The RAFT node should be closed. Checking health must not change that.
	// Simulates a race condition where we're shutting down, but we're still in the stream monitor.
	checkNodeIsClosed(sjs, sa)
	sjs.isStreamHealthy(acc, sa)
	checkNodeIsClosed(sjs, sa)

	err = js.DeleteStream("TEST")
	require_NoError(t, err)
	waitForNoStreamAssignments()

	// Underlying layer would be aware the health check made a copy.
	// So we sneakily set these values back, which simulates a race condition where
	// the health check is called while the deletion is in progress. This could happen
	// depending on how the locks are used.
	sjs.mu.Lock()
	sjs.cluster.streams = make(map[string]map[string]*streamAssignment)
	sjs.cluster.streams[globalAccountName] = make(map[string]*streamAssignment)
	sjs.cluster.streams[globalAccountName]["TEST"] = sa
	sa.Group.node = n
	sjs.mu.Unlock()

	// The underlying stream has been deleted. Checking health must not recreate the stream.
	checkNodeIsClosed(sjs, sa)
	sjs.isStreamHealthy(acc, sa)
	checkNodeIsClosed(sjs, sa)
}

func TestJetStreamClusterStreamHealthCheckMustNotDeleteEarly(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "R3S", 3)
	defer c.shutdown()

	nc, js := jsClientConnect(t, c.randomServer())
	defer nc.Close()

	waitForStreamAssignments := func() {
		t.Helper()
		checkFor(t, 5*time.Second, time.Second, func() error {
			for _, s := range c.servers {
				js := s.getJetStream()
				js.mu.RLock()
				sa := js.streamAssignment(globalAccountName, "TEST")
				js.mu.RUnlock()
				if sa == nil {
					return fmt.Errorf("stream assignment not found on %s", s.Name())
				}
			}
			return nil
		})
	}
	getStreamAssignment := func(rs *Server) (*jetStream, *Account, *streamAssignment, *stream) {
		acc, err := rs.lookupAccount(globalAccountName)
		require_NoError(t, err)
		mset, err := acc.lookupStream("TEST")
		require_NoError(t, err)

		sjs := rs.getJetStream()
		sjs.mu.Lock()
		defer sjs.mu.Unlock()

		sas := sjs.cluster.streams[globalAccountName]
		require_True(t, sas != nil)
		sa := sas["TEST"]
		require_True(t, sa != nil)
		sa.Created = time.Time{}
		return sjs, acc, sa, mset
	}

	_, err := js.AddStream(&nats.StreamConfig{
		Name:     "TEST",
		Subjects: []string{"foo"},
		Replicas: 3,
	})
	require_NoError(t, err)
	waitForStreamAssignments()

	// We manually clear the node on the stream.
	rs := c.randomNonStreamLeader(globalAccountName, "TEST")
	sjs, acc, sa, mset := getStreamAssignment(rs)
	mset.mu.Lock()
	mset.node = nil
	mset.mu.Unlock()
	sjs.mu.Lock()
	group := sa.Group
	if group == nil {
		sjs.mu.Unlock()
		t.Fatal("sa.Group not initialized")
	}
	node := group.node
	if node == nil {
		sjs.mu.Unlock()
		t.Fatal("sa.Group.node not initialized")
	}
	sjs.mu.Unlock()

	// The health check gets the Raft node of the assignment and checks it against the
	// Raft node of the stream. We simulate a race condition where the stream's Raft node
	// is not yet initialized. The health check MUST NOT delete the node.
	sjs.isStreamHealthy(acc, sa)
	require_Equal(t, node.State(), Follower)
}

func TestJetStreamClusterStreamHealthCheckOnlyReportsSkew(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "R3S", 3)
	defer c.shutdown()

	nc, js := jsClientConnect(t, c.randomServer())
	defer nc.Close()

	waitForStreamAssignments := func() {
		t.Helper()
		checkFor(t, 5*time.Second, time.Second, func() error {
			for _, s := range c.servers {
				js := s.getJetStream()
				js.mu.RLock()
				sa := js.streamAssignment(globalAccountName, "TEST")
				js.mu.RUnlock()
				if sa == nil {
					return fmt.Errorf("stream assignment not found on %s", s.Name())
				}
			}
			return nil
		})
	}
	getStreamAssignment := func(rs *Server) (*jetStream, *Account, *streamAssignment, *stream) {
		acc, err := rs.lookupAccount(globalAccountName)
		require_NoError(t, err)
		mset, err := acc.lookupStream("TEST")
		require_NoError(t, err)

		sjs := rs.getJetStream()
		sjs.mu.Lock()
		defer sjs.mu.Unlock()

		sas := sjs.cluster.streams[globalAccountName]
		require_True(t, sas != nil)
		sa := sas["TEST"]
		require_True(t, sa != nil)
		sa.Created = time.Time{}
		return sjs, acc, sa, mset
	}

	_, err := js.AddStream(&nats.StreamConfig{
		Name:     "TEST",
		Subjects: []string{"foo"},
		Replicas: 3,
	})
	require_NoError(t, err)
	waitForStreamAssignments()

	// Confirm the stream and assignment Raft nodes are equal.
	rs := c.randomNonStreamLeader(globalAccountName, "TEST")
	sjs, acc, sa, mset := getStreamAssignment(rs)
	mset.mu.RLock()
	msetNode := mset.node
	mset.mu.RUnlock()
	sjs.mu.Lock()
	group := sa.Group
	if group == nil {
		sjs.mu.Unlock()
		t.Fatal("sa.Group not initialized")
	}
	node := group.node
	if node == nil {
		sjs.mu.Unlock()
		t.Fatal("sa.Group.node not initialized")
	}
	sjs.mu.Unlock()
	require_Equal(t, msetNode, node)

	// Simulate stopping and restarting a new instance.
	node.Stop()
	node.WaitForStop()
	node, err = sjs.createRaftGroup(globalAccountName, group, false, FileStorage, pprofLabels{})
	require_NoError(t, err)
	require_NotEqual(t, node.State(), Closed)

	// The health check gets the Raft node of the assignment and checks it against the
	// Raft node of the stream. We simulate a race condition where the assignment's Raft node
	// is re-newed, but the stream's node is still the old instance.
	// The health check MUST NOT delete the node.
	require_Error(t, sjs.isStreamHealthy(acc, sa), errors.New("cluster node skew detected"))
	require_NotEqual(t, node.State(), Closed)
}

func TestJetStreamClusterStreamHealthCheckStreamCatchup(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "R3S", 3)
	defer c.shutdown()

	nc, js := jsClientConnect(t, c.randomServer())
	defer nc.Close()

	_, err := js.AddStream(&nats.StreamConfig{
		Name:     "TEST",
		Subjects: []string{"foo"},
		Replicas: 3,
	})
	require_NoError(t, err)

	sl := c.streamLeader(globalAccountName, "TEST")
	sjs := sl.getJetStream()
	acc := sl.globalAccount()
	mset, err := acc.lookupStream("TEST")
	require_NoError(t, err)
	sjs.mu.Lock()
	sa := sjs.streamAssignment(globalAccountName, "TEST")
	sjs.mu.Unlock()

	require_NoError(t, sjs.isStreamHealthy(acc, sa))

	// Check we can report unhealthy.
	n := mset.raftNode().(*raft)
	n.Lock()
	n.commit = 0
	n.Unlock()
	require_Error(t, sjs.isStreamHealthy(acc, sa), errors.New("group node unhealthy"))

	// Catching up should have precedence.
	mset.setCatchingUp()
	require_True(t, mset.isCatchingUp())
	require_Error(t, sjs.isStreamHealthy(acc, sa), errors.New("stream catching up"))
}

func TestJetStreamClusterConsumerHealthCheckMustNotRecreate(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "R3S", 3)
	defer c.shutdown()

	nc, js := jsClientConnect(t, c.randomServer())
	defer nc.Close()

	waitForConsumerAssignments := func() {
		t.Helper()
		checkFor(t, 2*time.Second, 200*time.Millisecond, func() error {
			for _, s := range c.servers {
				sjs := s.getJetStream()
				sjs.mu.RLock()
				ca := sjs.consumerAssignment(globalAccountName, "TEST", "CONSUMER")
				sjs.mu.RUnlock()
				if ca == nil {
					return fmt.Errorf("stream assignment not found on %s", s.Name())
				}
			}
			return nil
		})
	}
	getConsumerAssignment := func(rs *Server) (*jetStream, *streamAssignment, *consumerAssignment, *stream) {
		acc, err := rs.lookupAccount(globalAccountName)
		require_NoError(t, err)
		mset, err := acc.lookupStream("TEST")
		require_NoError(t, err)

		sjs := rs.getJetStream()
		sjs.mu.Lock()
		defer sjs.mu.Unlock()

		sas := sjs.cluster.streams[globalAccountName]
		require_True(t, sas != nil)
		sa := sas["TEST"]
		require_True(t, sa != nil)
		ca := sa.consumers["CONSUMER"]
		require_True(t, ca != nil)
		ca.Created = time.Time{}
		return sjs, sa, ca, mset
	}
	checkNodeIsClosed := func(sjs *jetStream, ca *consumerAssignment) {
		t.Helper()
		sjs.mu.RLock()
		defer sjs.mu.RUnlock()
		require_True(t, ca.Group != nil)
		require_True(t, ca.Group.node != nil)
		require_Equal(t, ca.Group.node.State(), Closed)
	}

	_, err := js.AddStream(&nats.StreamConfig{
		Name:      "TEST",
		Subjects:  []string{"foo"},
		Replicas:  3,
		Retention: nats.InterestPolicy, // Replicated consumers by default
	})
	require_NoError(t, err)
	_, err = js.AddConsumer("TEST", &nats.ConsumerConfig{Durable: "CONSUMER"})
	require_NoError(t, err)
	waitForConsumerAssignments()

	// We manually stop the RAFT node and ensure it doesn't get restarted.
	rs := c.randomNonConsumerLeader(globalAccountName, "TEST", "CONSUMER")
	sjs, sa, ca, mset := getConsumerAssignment(rs)
	sjs.mu.RLock()
	rg := ca.Group
	var n RaftNode
	if rg != nil {
		n = rg.node
	}
	sjs.mu.RUnlock()
	require_True(t, rg != nil)
	require_True(t, n != nil)
	n.Stop()
	n.WaitForStop()

	o := mset.lookupConsumer("CONSUMER")
	require_NotNil(t, o)
	checkFor(t, 2*time.Second, 200*time.Millisecond, func() error {
		if o.isMonitorRunning() {
			return errors.New("monitor goroutine still running")
		}
		return nil
	})

	// The RAFT node should be closed. Checking health must not change that.
	// Simulates a race condition where we're shutting down.
	checkNodeIsClosed(sjs, ca)
	require_Error(t, sjs.isConsumerHealthy(mset, sa, "CONSUMER", ca), errors.New("monitor goroutine not running"))
	checkNodeIsClosed(sjs, ca)

	// We create a new RAFT group, the health check should detect this skew.
	_, err = sjs.createRaftGroup(globalAccountName, ca.Group, false, MemoryStorage, pprofLabels{})
	require_NoError(t, err)
	sjs.mu.Lock()
	// We set creating to now, since previously it would delete all data but NOT restart if created within <10s.
	ca.Created = time.Now()
	sjs.mu.Unlock()
	require_Error(t, sjs.isConsumerHealthy(mset, sa, "CONSUMER", ca), errors.New("cluster node skew detected"))

	err = js.DeleteConsumer("TEST", "CONSUMER")
	require_NoError(t, err)

	// Wait for deletion to clear the node on the other replica,
	// otherwise cleanup could later remove the node that is
	// restored below.
	checkFor(t, 2*time.Second, 200*time.Millisecond, func() error {
		sjs.mu.RLock()
		defer sjs.mu.RUnlock()
		if ca.Group.node != nil {
			return errors.New("consumer node cleanup still in progress")
		}
		return nil
	})

	// Underlying layer would be aware the health check made a copy.
	// So we sneakily set these values back, which simulates a race condition where
	// the health check is called while the deletion is in progress. This could happen
	// depending on how the locks are used.
	sjs.mu.Lock()
	sjs.cluster.streams = make(map[string]map[string]*streamAssignment)
	sjs.cluster.streams[globalAccountName] = make(map[string]*streamAssignment)
	sjs.cluster.streams[globalAccountName]["TEST"] = sa
	ca.Created = time.Time{}
	ca.Group.node = n
	sjs.mu.Unlock()

	// The underlying consumer has been deleted. Checking health must not recreate the consumer.
	checkNodeIsClosed(sjs, ca)
	require_Error(t, sjs.isConsumerHealthy(mset, sa, "CONSUMER", ca), errors.New("consumer not found"))
	checkNodeIsClosed(sjs, ca)
}

func TestJetStreamClusterConsumerHealthCheckMustNotDeleteEarly(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "R3S", 3)
	defer c.shutdown()

	nc, js := jsClientConnect(t, c.randomServer())
	defer nc.Close()

	waitForConsumerAssignments := func() {
		t.Helper()
		checkFor(t, 5*time.Second, time.Second, func() error {
			for _, s := range c.servers {
				sjs := s.getJetStream()
				sjs.mu.RLock()
				ca := sjs.consumerAssignment(globalAccountName, "TEST", "CONSUMER")
				sjs.mu.RUnlock()
				if ca == nil {
					return fmt.Errorf("stream assignment not found on %s", s.Name())
				}
			}
			return nil
		})
	}
	getConsumerAssignment := func(rs *Server) (*jetStream, *consumerAssignment, *stream, *consumer) {
		acc, err := rs.lookupAccount(globalAccountName)
		require_NoError(t, err)
		mset, err := acc.lookupStream("TEST")
		require_NoError(t, err)
		o := mset.lookupConsumer("CONSUMER")

		sjs := rs.getJetStream()
		sjs.mu.Lock()
		defer sjs.mu.Unlock()

		sas := sjs.cluster.streams[globalAccountName]
		require_True(t, sas != nil)
		sa := sas["TEST"]
		require_True(t, sa != nil)
		ca := sa.consumers["CONSUMER"]
		require_True(t, ca != nil)
		ca.Created = time.Time{}
		return sjs, ca, mset, o
	}

	_, err := js.AddStream(&nats.StreamConfig{
		Name:      "TEST",
		Subjects:  []string{"foo"},
		Replicas:  3,
		Retention: nats.InterestPolicy, // Replicated consumers by default
	})
	require_NoError(t, err)
	_, err = js.AddConsumer("TEST", &nats.ConsumerConfig{Durable: "CONSUMER"})
	require_NoError(t, err)
	waitForConsumerAssignments()

	// We manually clear the node on the consumer.
	rs := c.randomNonConsumerLeader(globalAccountName, "TEST", "CONSUMER")
	sjs, ca, mset, o := getConsumerAssignment(rs)
	o.mu.Lock()
	o.node = nil
	o.mu.Unlock()
	sjs.mu.Lock()
	group := ca.Group
	if group == nil {
		sjs.mu.Unlock()
		t.Fatal("ca.Group not initialized")
	}
	node := group.node
	if node == nil {
		sjs.mu.Unlock()
		t.Fatal("ca.Group.node not initialized")
	}
	sjs.mu.Unlock()

	// The health check gets the Raft node of the assignment and checks it against the
	// Raft node of the consumer. We simulate a race condition where the consumer's Raft node
	// is not yet initialized. The health check MUST NOT delete the node.
	sjs.isConsumerHealthy(mset, nil, "CONSUMER", ca)
	require_Equal(t, node.State(), Follower)
}

func TestJetStreamClusterConsumerHealthCheckOnlyReportsSkew(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "R3S", 3)
	defer c.shutdown()

	nc, js := jsClientConnect(t, c.randomServer())
	defer nc.Close()

	waitForConsumerAssignments := func() {
		t.Helper()
		checkFor(t, 5*time.Second, time.Second, func() error {
			for _, s := range c.servers {
				sjs := s.getJetStream()
				sjs.mu.RLock()
				ca := sjs.consumerAssignment(globalAccountName, "TEST", "CONSUMER")
				sjs.mu.RUnlock()
				if ca == nil {
					return fmt.Errorf("stream assignment not found on %s", s.Name())
				}
			}
			return nil
		})
	}
	getConsumerAssignment := func(rs *Server) (*jetStream, *consumerAssignment, *stream, *consumer) {
		acc, err := rs.lookupAccount(globalAccountName)
		require_NoError(t, err)
		mset, err := acc.lookupStream("TEST")
		require_NoError(t, err)
		o := mset.lookupConsumer("CONSUMER")

		sjs := rs.getJetStream()
		sjs.mu.Lock()
		defer sjs.mu.Unlock()

		sas := sjs.cluster.streams[globalAccountName]
		require_True(t, sas != nil)
		sa := sas["TEST"]
		require_True(t, sa != nil)
		ca := sa.consumers["CONSUMER"]
		require_True(t, ca != nil)
		ca.Created = time.Time{}
		return sjs, ca, mset, o
	}

	_, err := js.AddStream(&nats.StreamConfig{
		Name:      "TEST",
		Subjects:  []string{"foo"},
		Replicas:  3,
		Retention: nats.InterestPolicy, // Replicated consumers by default
	})
	require_NoError(t, err)
	_, err = js.AddConsumer("TEST", &nats.ConsumerConfig{Durable: "CONSUMER"})
	require_NoError(t, err)
	waitForConsumerAssignments()

	// Confirm the consumer and assignment Raft nodes are equal.
	rs := c.randomNonConsumerLeader(globalAccountName, "TEST", "CONSUMER")
	sjs, ca, mset, o := getConsumerAssignment(rs)
	o.mu.RLock()
	oNode := o.node
	o.mu.RUnlock()
	sjs.mu.Lock()
	group := ca.Group
	if group == nil {
		sjs.mu.Unlock()
		t.Fatal("ca.Group not initialized")
	}
	node := group.node
	if node == nil {
		sjs.mu.Unlock()
		t.Fatal("ca.Group.node not initialized")
	}
	sjs.mu.Unlock()
	require_Equal(t, oNode, node)

	// Simulate stopping and restarting a new instance.
	node.Stop()
	node.WaitForStop()
	node, err = sjs.createRaftGroup(globalAccountName, group, false, FileStorage, pprofLabels{})
	require_NoError(t, err)
	require_NotEqual(t, node.State(), Closed)

	// The health check gets the Raft node of the assignment and checks it against the
	// Raft node of the consumer. We simulate a race condition where the assignment's Raft node
	// is re-newed, but the consumer's node is still the old instance.
	// The health check MUST NOT delete the node.
	require_Error(t, sjs.isConsumerHealthy(mset, nil, "CONSUMER", ca), errors.New("cluster node skew detected"))
	require_NotEqual(t, node.State(), Closed)
}

// https://github.com/nats-io/nats-server/issues/7149
func TestJetStreamClusterConsumerHealthCheckDeleted(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "R3S", 3)
	defer c.shutdown()

	nc, js := jsClientConnect(t, c.randomServer())
	defer nc.Close()

	_, err := js.AddStream(&nats.StreamConfig{
		Name:     "TEST",
		Subjects: []string{"foo"},
		Replicas: 1,
	})
	require_NoError(t, err)
	_, err = js.AddConsumer("TEST", &nats.ConsumerConfig{Durable: "CONSUMER"})
	require_NoError(t, err)

	cl := c.consumerLeader(globalAccountName, "TEST", "CONSUMER")
	require_NotNil(t, cl)
	mset, err := cl.globalAccount().lookupStream("TEST")
	require_NoError(t, err)

	sjs := cl.getJetStream()
	sjs.mu.Lock()
	ca := sjs.consumerAssignment(globalAccountName, "TEST", "CONSUMER")
	if ca == nil {
		sjs.mu.Unlock()
		t.Fatal("ca not found")
	}
	// Reset created time, simulating the consumer existed already for a while.
	ca.Created = time.Time{}
	sjs.mu.Unlock()

	// The health check gathers all assignments and does checking after.
	// If the consumer was deleted in the meantime, it should not report an error.
	require_NoError(t, js.DeleteConsumer("TEST", "CONSUMER"))
	require_Error(t, sjs.isConsumerHealthy(mset, nil, "CONSUMER", ca), errors.New("consumer not found"))

	// The health check could run earlier than we're able to create the consumer.
	// In that case, wait before erroring.
	sjs.mu.Lock()
	ca.Created = time.Now()
	sjs.mu.Unlock()
	require_NoError(t, sjs.isConsumerHealthy(mset, nil, "CONSUMER", ca))
}

func TestJetStreamClusterStreamHealthCheckSurfacesAssignmentErr(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "R3S", 3)
	defer c.shutdown()

	nc, js := jsClientConnect(t, c.randomServer())
	defer nc.Close()

	_, err := js.AddStream(&nats.StreamConfig{
		Name:     "TEST",
		Subjects: []string{"foo"},
		Replicas: 3,
	})
	require_NoError(t, err)

	sl := c.streamLeader(globalAccountName, "TEST")
	sjs := sl.getJetStream()
	acc := sl.globalAccount()

	sjs.mu.Lock()
	sa := sjs.streamAssignment(globalAccountName, "TEST")
	sjs.mu.Unlock()
	require_True(t, sa != nil)

	// Baseline: healthy.
	require_NoError(t, sjs.isStreamHealthy(acc, sa))

	// Persisted assignment-level error must surface via the health check.
	wantErr := errors.New("synthetic stream assignment failure")
	sjs.mu.Lock()
	sa.err = wantErr
	sjs.mu.Unlock()

	err = sjs.isStreamHealthy(acc, sa)
	require_Error(t, err)
	require_Contains(t, err.Error(), wantErr.Error())

	// Clearing the err brings health back.
	sjs.mu.Lock()
	sa.err = nil
	sjs.mu.Unlock()
	require_NoError(t, sjs.isStreamHealthy(acc, sa))
}

func TestJetStreamClusterConsumerHealthCheckSurfacesAssignmentErr(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "R3S", 3)
	defer c.shutdown()

	nc, js := jsClientConnect(t, c.randomServer())
	defer nc.Close()

	_, err := js.AddStream(&nats.StreamConfig{
		Name:     "TEST",
		Subjects: []string{"foo"},
		Replicas: 3,
	})
	require_NoError(t, err)
	_, err = js.AddConsumer("TEST", &nats.ConsumerConfig{Durable: "CONSUMER"})
	require_NoError(t, err)

	cl := c.consumerLeader(globalAccountName, "TEST", "CONSUMER")
	require_NotNil(t, cl)
	mset, err := cl.globalAccount().lookupStream("TEST")
	require_NoError(t, err)

	sjs := cl.getJetStream()
	sjs.mu.Lock()
	ca := sjs.consumerAssignment(globalAccountName, "TEST", "CONSUMER")
	sjs.mu.Unlock()
	require_True(t, ca != nil)

	// Baseline: healthy.
	require_NoError(t, sjs.isConsumerHealthy(mset, nil, "CONSUMER", ca))

	// Persisted assignment-level error must surface via the health check.
	wantErr := errors.New("synthetic consumer assignment failure")
	sjs.mu.Lock()
	ca.err = wantErr
	sjs.mu.Unlock()

	err = sjs.isConsumerHealthy(mset, nil, "CONSUMER", ca)
	require_Error(t, err)
	require_Contains(t, err.Error(), wantErr.Error())

	// Clearing the err brings health back.
	sjs.mu.Lock()
	ca.err = nil
	sjs.mu.Unlock()
	require_NoError(t, sjs.isConsumerHealthy(mset, nil, "CONSUMER", ca))
}

func TestJetStreamClusterConsumerHealthCheckStreamMissingNoLockLeak(t *testing.T) {
	js := &jetStream{}
	ca := &consumerAssignment{}
	err := js.isConsumerHealthy(nil, nil, "consumer", ca)
	require_Error(t, err, errors.New("stream missing"))

	// The JS lock must have been released. Attempt to acquire the write lock
	// from a separate goroutine with a timeout so a leaked lock does not hang
	// the test forever.
	done := make(chan struct{})
	go func() {
		js.mu.Lock()
		defer js.mu.Unlock()
		close(done)
	}()

	select {
	case <-done:
		// Lock acquired, no leak.
	case <-time.After(2 * time.Second):
		t.Fatal("js.mu lock leaked: could not acquire write lock after isConsumerHealthy returned")
	}
}

func TestJetStreamClusterProcessStreamAssignmentResults(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "R3S", 3)
	defer c.shutdown()

	nc, js := jsClientConnect(t, c.randomServer())
	defer nc.Close()

	_, err := js.AddStream(&nats.StreamConfig{
		Name:     "TEST",
		Subjects: []string{"foo"},
		Replicas: 3,
	})
	require_NoError(t, err)

	// The result handler runs on the metadata leader.
	ml := c.leader()
	require_NotNil(t, ml)
	mjs := ml.getJetStream()

	// Malformed payloads and unknown accounts are dropped silently.
	mjs.processStreamAssignmentResults(nil, nil, nil, _EMPTY_, _EMPTY_, []byte("{not-json"))
	badAcc, err := json.Marshal(&streamAssignmentResult{Account: "DOES_NOT_EXIST", Stream: "TEST"})
	require_NoError(t, err)
	mjs.processStreamAssignmentResults(nil, nil, nil, _EMPTY_, _EMPTY_, badAcc)

	// The stream leader responds to a create, wait for the meta leader to apply it.
	checkFor(t, 2*time.Second, 10*time.Millisecond, func() error {
		mjs.mu.RLock()
		defer mjs.mu.RUnlock()
		if mjs.streamAssignment(globalAccountName, "TEST") == nil {
			return errors.New("stream assignment not applied")
		}
		return nil
	})

	// Reset the assignment so it's treated as not-yet-responded, and ensure we're
	// inside the canDelete window.
	mjs.mu.Lock()
	sa := mjs.streamAssignment(globalAccountName, "TEST")
	require_True(t, sa != nil)
	sa.clearResponded()
	sa.Created = time.Now()
	mjs.mu.Unlock()

	// Craft a rejection result, as a member sends after a failed local create.
	result := &streamAssignmentResult{
		Account: globalAccountName,
		Stream:  "TEST",
		Response: &JSApiStreamCreateResponse{
			ApiResponse: ApiResponse{
				Type:  JSApiStreamCreateResponseType,
				Error: NewJSStreamLimitsError(errors.New("synthetic limits error")),
			},
		},
	}
	b, err := json.Marshal(result)
	require_NoError(t, err)
	mjs.processStreamAssignmentResults(nil, nil, nil, _EMPTY_, _EMPTY_, b)

	// The handler forwards the response, so the assignment is now marked responded.
	mjs.mu.Lock()
	responded := sa.hasResponded()
	mjs.mu.Unlock()
	require_True(t, responded)

	// The rejected assignment must be cleaned up from the meta layer.
	checkFor(t, 2*time.Second, 100*time.Millisecond, func() error {
		mjs.mu.Lock()
		sa := mjs.streamAssignment(globalAccountName, "TEST")
		mjs.mu.Unlock()
		if sa != nil {
			return fmt.Errorf("stream assignment still present")
		}
		return nil
	})
}

func TestJetStreamClusterProcessStreamAssignmentResultsRetry(t *testing.T) {
	sc := createJetStreamSuperCluster(t, 3, 2)
	defer sc.shutdown()

	nc, js := jsClientConnect(t, sc.randomServer())
	defer nc.Close()

	_, err := js.AddStream(&nats.StreamConfig{
		Name:     "TEST",
		Subjects: []string{"foo"},
		Replicas: 3,
	})
	require_NoError(t, err)

	ml := sc.leader()
	require_NotNil(t, ml)
	mjs := ml.getJetStream()

	// The stream leader responds to a create, wait for the meta leader to apply it.
	checkFor(t, 2*time.Second, 10*time.Millisecond, func() error {
		mjs.mu.RLock()
		defer mjs.mu.RUnlock()
		if mjs.streamAssignment(globalAccountName, "TEST") == nil {
			return errors.New("stream assignment not applied")
		}
		return nil
	})

	// Determine the assigned cluster and an alternate to retry into.
	mjs.mu.Lock()
	sa := mjs.streamAssignment(globalAccountName, "TEST")
	require_True(t, sa != nil)
	assignedCluster := sa.Client.Cluster
	var alternate string
	for _, cl := range sc.clusters {
		if cl.name != assignedCluster {
			alternate = cl.name
			break
		}
	}
	// Prepare the assignment so the retry branch is eligible: fresh, unresponded,
	// no fixed placement, and with an alternate cluster to fall back to.
	sa.clearResponded()
	sa.Created = time.Now()
	sa.Config.Placement = nil
	sa.Client.Alternates = []string{alternate}
	mjs.mu.Unlock()
	require_NotEqual(t, alternate, _EMPTY_)

	// Craft an insufficient-resources rejection.
	result := &streamAssignmentResult{
		Account: globalAccountName,
		Stream:  "TEST",
		Response: &JSApiStreamCreateResponse{
			ApiResponse: ApiResponse{
				Type:  JSApiStreamCreateResponseType,
				Error: NewJSInsufficientResourcesError(),
			},
		},
	}
	b, err := json.Marshal(result)
	require_NoError(t, err)

	mjs.processStreamAssignmentResults(nil, nil, nil, _EMPTY_, _EMPTY_, b)

	// The retry branch re-proposes the assignment and flags it as reassigning.
	mjs.mu.Lock()
	nsa := mjs.streamAssignmentOrInflight(globalAccountName, "TEST")
	responded := nsa.hasResponded()
	reassigning := nsa.reassigning
	mjs.mu.Unlock()
	// Retry returns before responding, so the client is not told it failed.
	require_False(t, responded)
	require_True(t, reassigning)
}

func TestJetStreamClusterProcessConsumerAssignmentResults(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "R3S", 3)
	defer c.shutdown()

	nc, js := jsClientConnect(t, c.randomServer())
	defer nc.Close()

	_, err := js.AddStream(&nats.StreamConfig{
		Name:     "TEST",
		Subjects: []string{"foo"},
		Replicas: 3,
	})
	require_NoError(t, err)
	_, err = js.AddConsumer("TEST", &nats.ConsumerConfig{Durable: "CONSUMER"})
	require_NoError(t, err)

	ml := c.leader()
	require_NotNil(t, ml)
	mjs := ml.getJetStream()

	// Malformed payloads and unknown accounts are dropped silently.
	mjs.processConsumerAssignmentResults(nil, nil, nil, _EMPTY_, _EMPTY_, []byte("{not-json"))
	badAcc, err := json.Marshal(&consumerAssignmentResult{Account: "DOES_NOT_EXIST", Stream: "TEST", Consumer: "CONSUMER"})
	require_NoError(t, err)
	mjs.processConsumerAssignmentResults(nil, nil, nil, _EMPTY_, _EMPTY_, badAcc)

	// The consumer leader responds to a create, wait for the meta leader to apply it.
	checkFor(t, 2*time.Second, 10*time.Millisecond, func() error {
		mjs.mu.RLock()
		defer mjs.mu.RUnlock()
		if mjs.consumerAssignment(globalAccountName, "TEST", "CONSUMER") == nil {
			return errors.New("consumer assignment not applied")
		}
		return nil
	})

	mjs.mu.Lock()
	ca := mjs.consumerAssignment(globalAccountName, "TEST", "CONSUMER")
	require_True(t, ca != nil)
	ca.clearResponded()
	ca.Created = time.Now()
	mjs.mu.Unlock()

	result := &consumerAssignmentResult{
		Account:  globalAccountName,
		Stream:   "TEST",
		Consumer: "CONSUMER",
		Response: &JSApiConsumerCreateResponse{
			ApiResponse: ApiResponse{
				Type:  JSApiConsumerCreateResponseType,
				Error: NewJSConsumerStoreFailedError(errors.New("synthetic store error")),
			},
		},
	}
	b, err := json.Marshal(result)
	require_NoError(t, err)
	mjs.processConsumerAssignmentResults(nil, nil, nil, _EMPTY_, _EMPTY_, b)

	// The handler is synchronous: the assignment must now be marked responded and
	// carry the recorded assignment-level error.
	mjs.mu.Lock()
	responded := ca.hasResponded()
	caErr := ca.err
	mjs.mu.Unlock()
	require_True(t, responded)
	require_Error(t, caErr, NewJSClusterNotAssignedError())
}

func TestJetStreamClusterStreamHealthCheckRecoversAfterSuccessfulUpdate(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "R3S", 3)
	defer c.shutdown()

	nc, js := jsClientConnect(t, c.randomServer())
	defer nc.Close()

	_, err := js.AddStream(&nats.StreamConfig{
		Name:     "TEST",
		Subjects: []string{"foo"},
		Replicas: 3,
	})
	require_NoError(t, err)

	sl := c.streamLeader(globalAccountName, "TEST")
	sjs := sl.getJetStream()
	acc := sl.globalAccount()

	sjs.mu.Lock()
	sa := sjs.streamAssignment(globalAccountName, "TEST")
	sjs.mu.Unlock()
	require_True(t, sa != nil)

	// Plant a transient assignment-level error. Health check must now fail.
	wantErr := errors.New("synthetic stream assignment failure")
	sjs.mu.Lock()
	sa.err = wantErr
	sjs.mu.Unlock()
	err = sjs.isStreamHealthy(acc, sa)
	require_Error(t, err)
	require_Contains(t, err.Error(), wantErr.Error())

	// A successful update should clear it.
	_, err = js.UpdateStream(&nats.StreamConfig{
		Name:     "TEST",
		Subjects: []string{"foo", "bar"},
		Replicas: 3,
	})
	require_NoError(t, err)

	sjs.mu.RLock()
	cur := sjs.streamAssignment(globalAccountName, "TEST")
	var gotErr error
	if cur != nil {
		gotErr = cur.err
	}
	sjs.mu.RUnlock()
	require_True(t, cur != nil)
	require_NoError(t, gotErr)
	require_NoError(t, sjs.isStreamHealthy(acc, cur))
}

func TestJetStreamClusterConsumerHealthCheckUnmappedDuringMigration(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "R3S", 3)
	defer c.shutdown()

	nc, js := jsClientConnect(t, c.randomServer())
	defer nc.Close()

	_, err := js.AddStream(&nats.StreamConfig{
		Name:     "TEST",
		Subjects: []string{"foo"},
		Replicas: 1,
	})
	require_NoError(t, err)
	// No explicit replica count, so it inherits the stream's.
	_, err = js.AddConsumer("TEST", &nats.ConsumerConfig{
		Durable:   "CONSUMER",
		AckPolicy: nats.AckExplicitPolicy,
	})
	require_NoError(t, err)

	// The single server hosting the R1 stream.
	var s *Server
	for _, cs := range c.servers {
		if _, err := cs.globalAccount().lookupStream("TEST"); err == nil {
			s = cs
			break
		}
	}
	require_NotNil(t, s)
	mset, err := s.globalAccount().lookupStream("TEST")
	require_NoError(t, err)

	sjs := s.getJetStream()
	sjs.mu.RLock()
	sa := sjs.streamAssignment(globalAccountName, "TEST")
	ca := sjs.consumerAssignment(globalAccountName, "TEST", "CONSUMER")
	sjs.mu.RUnlock()
	require_NotNil(t, sa)
	require_NotNil(t, ca)

	// Healthy to start with.
	require_NoError(t, sjs.isConsumerHealthy(mset, sa, "CONSUMER", ca))

	// Record desired state to scale up to R3, as the meta leader would: the config
	// holds the target while the group stays at its origin. The consumer is left
	// untouched, it is only mapped onto the new peers later in the migration.
	peers := []string{sa.Group.Peers[0]}
	for _, cs := range c.servers {
		if id := cs.Node(); id != sa.Group.Peers[0] {
			peers = append(peers, id)
		}
	}
	sjs.mu.Lock()
	sa.Config.Replicas = 3
	sa.Group.Desired = &desiredRaftGroup{
		Created: time.Now().UTC(),
		ID:      "DESIRED",
		Peers:   peers,
		Cluster: sa.Group.Cluster,
		Origin: &desiredRaftGroupOrigin{
			Peers:    copyStrings(sa.Group.Peers),
			Cluster:  sa.Group.Cluster,
			Replicas: 1,
		},
	}
	sjs.mu.Unlock()
	// The stream runs at its origin, but its replica count is the target already.
	mset.cfgMu.Lock()
	mset.cfg.Replicas = 3
	mset.cfgMu.Unlock()

	// The consumer is unmapped: still on the origin peer, with no desired state and
	// no Raft node of its own, while inheriting the stream's target replica count.
	sjs.mu.RLock()
	require_Len(t, len(ca.Group.Peers), 1)
	require_True(t, ca.Group.Desired == nil)
	require_True(t, ca.Group.node == nil)
	sjs.mu.RUnlock()
	rc, err := mset.lookupConsumer("CONSUMER").replica()
	require_NoError(t, err)
	require_Equal(t, rc, 3)

	// It must not be reported unhealthy for that.
	require_NoError(t, sjs.isConsumerHealthy(mset, sa, "CONSUMER", ca))

	// The stream can scale up while the consumer remap lags behind. The group is no
	// longer R1, but the consumer still is and must remain healthy.
	sjs.mu.Lock()
	groupPeers := sa.Group.Peers
	sa.Group.Peers = copyStrings(peers)
	sjs.mu.Unlock()
	sjs.mu.RLock()
	require_Len(t, len(ca.Group.Peers), 1)
	require_True(t, ca.Group.node == nil)
	sjs.mu.RUnlock()
	require_NoError(t, sjs.isConsumerHealthy(mset, sa, "CONSUMER", ca))
	sjs.mu.Lock()
	sa.Group.Peers = groupPeers
	sjs.mu.Unlock()

	// Without the recorded origin to fall back on, the inherited target count is all
	// we have, and the missing node is reported. Confirms the check above is not
	// passing for some other reason.
	sjs.mu.Lock()
	origin := sa.Group.Desired.Origin
	sa.Group.Desired.Origin = nil
	sjs.mu.Unlock()
	require_Error(t, sjs.isConsumerHealthy(mset, sa, "CONSUMER", ca), errors.New("group node missing"))
	sjs.mu.Lock()
	sa.Group.Desired.Origin = origin
	sjs.mu.Unlock()
	require_NoError(t, sjs.isConsumerHealthy(mset, sa, "CONSUMER", ca))
}

func TestJetStreamClusterConsumerHealthCheckRecoversAfterSuccessfulUpdate(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "R3S", 3)
	defer c.shutdown()

	nc, js := jsClientConnect(t, c.randomServer())
	defer nc.Close()

	_, err := js.AddStream(&nats.StreamConfig{
		Name:     "TEST",
		Subjects: []string{"foo"},
		Replicas: 3,
	})
	require_NoError(t, err)
	_, err = js.AddConsumer("TEST", &nats.ConsumerConfig{
		Durable:       "CONSUMER",
		AckPolicy:     nats.AckExplicitPolicy,
		MaxAckPending: 100,
	})
	require_NoError(t, err)

	cl := c.consumerLeader(globalAccountName, "TEST", "CONSUMER")
	require_NotNil(t, cl)
	mset, err := cl.globalAccount().lookupStream("TEST")
	require_NoError(t, err)

	sjs := cl.getJetStream()
	sjs.mu.Lock()
	ca := sjs.consumerAssignment(globalAccountName, "TEST", "CONSUMER")
	sjs.mu.Unlock()
	require_True(t, ca != nil)

	// Plant a transient assignment-level error. Health check must now fail.
	wantErr := errors.New("synthetic consumer assignment failure")
	sjs.mu.Lock()
	ca.err = wantErr
	sjs.mu.Unlock()
	err = sjs.isConsumerHealthy(mset, nil, "CONSUMER", ca)
	require_Error(t, err)
	require_Contains(t, err.Error(), wantErr.Error())

	// A successful update should clear it.
	_, err = js.UpdateConsumer("TEST", &nats.ConsumerConfig{
		Durable:       "CONSUMER",
		AckPolicy:     nats.AckExplicitPolicy,
		MaxAckPending: 200,
	})
	require_NoError(t, err)

	sjs.mu.RLock()
	cur := sjs.consumerAssignment(globalAccountName, "TEST", "CONSUMER")
	var gotErr error
	if cur != nil {
		gotErr = cur.err
	}
	sjs.mu.RUnlock()
	require_True(t, cur != nil)
	require_NoError(t, gotErr)
	require_NoError(t, sjs.isConsumerHealthy(mset, nil, "CONSUMER", cur))
}

func TestJetStreamClusterRespectConsumerStartSeq(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "R3S", 3)
	defer c.shutdown()

	nc, js := jsClientConnect(t, c.randomServer())
	defer nc.Close()

	// Create replicated stream.
	_, err := js.AddStream(&nats.StreamConfig{
		Name:     "TEST",
		Subjects: []string{"foo"},
		Replicas: 3,
	})
	require_NoError(t, err)

	// We could have published messages into the stream that have not yet been applied on the follower.
	// If we create a consumer with a starting sequence in the future, we must respect it.
	ci, err := js.AddConsumer("TEST", &nats.ConsumerConfig{
		DeliverPolicy: nats.DeliverByStartSequencePolicy,
		OptStartSeq:   20,
	})
	require_NoError(t, err)
	require_Equal(t, ci.Delivered.Stream, 19)

	// Same thing if the first sequence is not 0.
	err = js.PurgeStream("TEST", &nats.StreamPurgeRequest{Sequence: 10})
	require_NoError(t, err)

	// Ensure all servers are up-to-date.
	checkFor(t, 2*time.Second, 500*time.Millisecond, func() error {
		return checkState(t, c, globalAccountName, "TEST")
	})

	ci, err = js.AddConsumer("TEST", &nats.ConsumerConfig{
		DeliverPolicy: nats.DeliverByStartSequencePolicy,
		OptStartSeq:   20,
	})
	require_NoError(t, err)
	require_Equal(t, ci.Delivered.Stream, 19)

	// Only if we're requested to start at a sequence that's not available anymore
	// can we safely move it up. That data is gone already, so can't do anything else.
	ci, err = js.AddConsumer("TEST", &nats.ConsumerConfig{
		DeliverPolicy: nats.DeliverByStartSequencePolicy,
		OptStartSeq:   5,
	})
	require_NoError(t, err)
	require_Equal(t, ci.Delivered.Stream, 9)
}

func TestJetStreamClusterPeerRemoveStreamConsumerDesync(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "R3S", 3)
	defer c.shutdown()

	nc, js := jsClientConnect(t, c.randomServer())
	defer nc.Close()

	_, err := js.AddStream(&nats.StreamConfig{
		Name:     "TEST",
		Subjects: []string{"foo"},
		Replicas: 3,
	})
	require_NoError(t, err)

	// Must have at least one message in the stream.
	_, err = js.Publish("foo", nil)
	require_NoError(t, err)

	sl := c.streamLeader(globalAccountName, "TEST")
	acc, err := sl.lookupAccount(globalAccountName)
	require_NoError(t, err)
	mset, err := acc.lookupStream("TEST")
	require_NoError(t, err)

	rs := c.randomNonStreamLeader(globalAccountName, "TEST")
	peer := rs.Name()
	rn := mset.raftNode()
	mset.mu.Lock()
	esm := encodeStreamMsgAllowCompress("foo", _EMPTY_, nil, nil, mset.clseq, time.Now().UnixNano(), false)
	mset.clseq++
	mset.mu.Unlock()
	// Propose both remove peer and a normal entry within the same append entry.
	err = rn.ProposeMulti(rn.Term(), []*Entry{
		newEntry(EntryRemovePeer, []byte(peer)),
		newEntry(EntryNormal, esm),
	})
	require_NoError(t, err)

	// If the previous normal entry was skipped, we'd get a seq mismatch error here.
	_, err = js.Publish("foo", nil)
	require_NoError(t, err)

	// Now check the same but for a consumer.
	_, err = js.PullSubscribe("foo", "CONSUMER")
	require_NoError(t, err)

	cl := c.consumerLeader(globalAccountName, "TEST", "CONSUMER")
	acc, err = cl.lookupAccount(globalAccountName)
	require_NoError(t, err)
	mset, err = acc.lookupStream("TEST")
	require_NoError(t, err)
	o := mset.lookupConsumer("CONSUMER")
	require_NotNil(t, o)

	updateDeliveredBuffer := func() []byte {
		var b [4*binary.MaxVarintLen64 + 1]byte
		b[0] = byte(updateDeliveredOp)
		n := 1
		n += binary.PutUvarint(b[n:], 100)
		n += binary.PutUvarint(b[n:], 100)
		n += binary.PutUvarint(b[n:], 1)
		n += binary.PutVarint(b[n:], time.Now().UnixNano())
		return b[:n]
	}

	rs = c.randomNonConsumerLeader(globalAccountName, "TEST", "CONSUMER")
	peer = rs.Name()
	rn = o.raftNode()
	// Propose both remove peer and a normal entry within the same append entry.
	err = rn.ProposeMulti(rn.Term(), []*Entry{
		newEntry(EntryRemovePeer, []byte(peer)),
		newEntry(EntryNormal, updateDeliveredBuffer()),
	})
	require_NoError(t, err)

	// Check the normal entry was applied.
	checkFor(t, 2*time.Second, 500*time.Millisecond, func() error {
		o.mu.Lock()
		defer o.mu.Unlock()
		cs, err := o.store.State()
		if err != nil {
			return err
		}
		if cs.Delivered.Consumer != 100 || cs.Delivered.Stream != 100 {
			return fmt.Errorf("expected sequence 100, got: %v", cs.Delivered)
		}
		return nil
	})
}

func TestJetStreamClusterStuckConsumerAfterLeaderChangeWithUnknownDeliveries(t *testing.T) {
	testJetStreamClusterStuckConsumerAfterLeaderChangeWithUnknownDeliveries(t, false)
}

func TestJetStreamClusterStuckConsumerV2AfterLeaderChangeWithUnknownDeliveries(t *testing.T) {
	testJetStreamClusterStuckConsumerAfterLeaderChangeWithUnknownDeliveries(t, true)
}

func testJetStreamClusterStuckConsumerAfterLeaderChangeWithUnknownDeliveries(t *testing.T, ackV2 bool) {
	c := createJetStreamClusterExplicit(t, "R3S", 3)
	defer c.shutdown()

	for _, s := range c.servers {
		opts := *s.getOpts()
		opts.FeatureFlags = map[string]bool{FeatureFlagJsAckFormatV2: ackV2}
		s.setOpts(&opts)
	}

	nc, js := jsClientConnect(t, c.randomServer())
	defer nc.Close()

	_, err := js.AddStream(&nats.StreamConfig{
		Name:     "TEST",
		Subjects: []string{"foo"},
		Replicas: 3,
	})
	require_NoError(t, err)

	// Publish some messages into the stream.
	for i := 0; i < 3; i++ {
		_, err = js.Publish("foo", nil)
		require_NoError(t, err)
	}

	// Ensure all servers are up-to-date.
	checkFor(t, 2*time.Second, 500*time.Millisecond, func() error {
		return checkState(t, c, globalAccountName, "TEST")
	})

	sub, err := js.PullSubscribe("foo", "CONSUMER")
	require_NoError(t, err)
	defer sub.Unsubscribe()

	// We only fetch 1 message here, since the condition is hard to trigger otherwise.
	// But, we're simulating fetching 3 messages and the consumer leader changing while
	// deliveries are happening. This will result in the new consumer leader not knowing
	// that the last two messages were also delivered (since we don't wait for quorum before delivering).
	msgs, err := sub.Fetch(1)
	require_NoError(t, err)
	require_Len(t, len(msgs), 1)

	cl := c.consumerLeader(globalAccountName, "TEST", "CONSUMER")
	require_NotNil(t, cl)
	mset, err := cl.globalAccount().lookupStream("TEST")
	require_NoError(t, err)
	o := mset.lookupConsumer("CONSUMER")
	require_NotNil(t, o)

	// The client could send an acknowledgement, while the new consumer leader doesn't know about it
	// ever being delivered. It must NOT adjust any state and ignore the request to remain consistent.
	o.mu.RLock()
	ackReply := o.ackReply(3, 3, 1, 0, 0)
	o.mu.RUnlock()
	if ackV2 {
		require_Equal(t, ackReply, "$JS.ACK._.szMpdrwD.TEST.CONSUMER.1.3.3.0.0")
	} else {
		require_Equal(t, ackReply, "$JS.ACK.TEST.CONSUMER.1.3.3.0.0")
	}
	_, err = nc.Request(ackReply, nil, time.Second)
	require_Error(t, err, nats.ErrTimeout)

	// Acknowledging a message that is known to be delivered is accepted still.
	o.mu.RLock()
	ackReply = o.ackReply(1, 1, 1, 0, 0)
	o.mu.RUnlock()
	if ackV2 {
		require_Equal(t, ackReply, "$JS.ACK._.szMpdrwD.TEST.CONSUMER.1.1.1.0.0")
	} else {
		require_Equal(t, ackReply, "$JS.ACK.TEST.CONSUMER.1.1.1.0.0")
	}
	_, err = nc.Request(ackReply, nil, time.Second)
	require_NoError(t, err)

	// Check for consistent consumer info.
	ci, err := js.ConsumerInfo("TEST", "CONSUMER")
	require_NoError(t, err)
	require_Equal(t, ci.Delivered.Consumer, 1)
	require_Equal(t, ci.Delivered.Stream, 1)
	require_Equal(t, ci.AckFloor.Consumer, 1)
	require_Equal(t, ci.AckFloor.Stream, 1)

	// Fetching for new messages MUST return the two messages the new consumer leader didn't
	// know were delivered before. If we wouldn't deliver these we'd have a stuck consumer.
	msgs, err = sub.Fetch(2)
	require_NoError(t, err)
	require_Len(t, len(msgs), 2)
	for _, msg := range msgs {
		require_NoError(t, msg.AckSync())
	}

	// Check for consistent consumer info.
	ci, err = js.ConsumerInfo("TEST", "CONSUMER")
	require_NoError(t, err)
	require_Equal(t, ci.Delivered.Consumer, 3)
	require_Equal(t, ci.Delivered.Stream, 3)
	require_Equal(t, ci.AckFloor.Consumer, 3)
	require_Equal(t, ci.AckFloor.Stream, 3)
}

// This is for when we are still using $SYS for NRG replication but we want to make sure
// we track this in something visible to the end user.
func TestJetStreamClusterAccountStatsForReplicatedStreams(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "R5S", 5)
	defer c.shutdown()

	nc, js := jsClientConnect(t, c.randomServer())
	defer nc.Close()

	_, err := js.AddStream(&nats.StreamConfig{
		Name:     "TEST",
		Subjects: []string{"foo"},
		Replicas: 5,
	})
	require_NoError(t, err)

	// Let's connect to stream leader to make sent messages predictable, otherwise we get those to come up based on routing to stream leader.
	s := c.streamLeader(globalAccountName, "TEST")
	require_True(t, s != nil)

	nc, js = jsClientConnect(t, s)
	defer nc.Close()

	// NRG traffic can be compressed, so make this unique so we can check stats correctly.
	msg := make([]byte, 1024*1024)
	crand.Read(msg)

	// Publish some messages into the stream.
	for i := 0; i < 10; i++ {
		_, err = js.Publish("foo", msg)
		require_NoError(t, err)
	}
	time.Sleep(250 * time.Millisecond)

	// Now grab the account stats for us and make sure we account for the replicated messages.

	// Opts to grab our account.
	opts := &AccountStatzOptions{
		Accounts: []string{globalAccountName},
	}
	as, err := s.AccountStatz(opts)
	require_NoError(t, err)
	require_Equal(t, len(as.Accounts), 1)

	accStats := as.Accounts[0]

	// We need to account for possibility that the stream create was also on this server, hence the >= vs strict ==.
	require_True(t, accStats.Received.Msgs >= 10)
	require_True(t, accStats.Received.Bytes >= 1024*1024)
	// For sent, we will have 10 pub acks, and then should have 40 extra messages that are sent and accounted for
	// during the nrg propsal to the R5 peers.
	require_True(t, accStats.Sent.Msgs >= 50)
	require_True(t, accStats.Sent.Bytes >= accStats.Received.Bytes*4)
}

func TestJetStreamClusterRecreateConsumerFromMetaSnapshot(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "R3S", 3)
	defer c.shutdown()

	nc, js := jsClientConnect(t, c.randomServer())
	defer nc.Close()

	// Initial setup.
	_, err := js.AddStream(&nats.StreamConfig{
		Name:     "TEST",
		Subjects: []string{"foo"},
		Replicas: 3,
	})
	require_NoError(t, err)
	_, err = js.Publish("foo", nil)
	require_NoError(t, err)
	_, err = js.AddConsumer("TEST", &nats.ConsumerConfig{Durable: "CONSUMER"})
	require_NoError(t, err)

	// Wait for all servers to be fully up-to-date.
	checkFor(t, 2*time.Second, 500*time.Millisecond, func() error {
		if err = checkState(t, c, globalAccountName, "TEST"); err != nil {
			return err
		}
		for _, s := range c.servers {
			if acc, err := s.lookupAccount(globalAccountName); err != nil {
				return err
			} else if mset, err := acc.lookupStream("TEST"); err != nil {
				return err
			} else if o := mset.lookupConsumer("CONSUMER"); o == nil {
				return errors.New("consumer doesn't exist")
			}
		}
		return nil
	})

	// Shutdown a random server.
	rs := c.randomServer()
	rs.Shutdown()
	rs.WaitForShutdown()

	// Recreate connection, since we could have shutdown the server we were connected to.
	nc.Close()
	c.waitOnLeader()
	nc, js = jsClientConnect(t, c.randomServer())
	defer nc.Close()

	// Recreate consumer.
	require_NoError(t, js.DeleteConsumer("TEST", "CONSUMER"))
	_, err = js.AddConsumer("TEST", &nats.ConsumerConfig{Durable: "CONSUMER"})
	require_NoError(t, err)

	// Wait for all servers (except for the one that's down) to have recreated the consumer.
	var consumerRg string
	checkFor(t, 2*time.Second, 500*time.Millisecond, func() error {
		consumerRg = _EMPTY_
		for _, s := range c.servers {
			if s == rs {
				continue
			}
			if acc, err := s.lookupAccount(globalAccountName); err != nil {
				return err
			} else if mset, err := acc.lookupStream("TEST"); err != nil {
				return err
			} else if o := mset.lookupConsumer("CONSUMER"); o == nil {
				return errors.New("consumer doesn't exist")
			} else if ccrg := o.raftNode().Group(); consumerRg == _EMPTY_ {
				consumerRg = ccrg
			} else if consumerRg != ccrg {
				return errors.New("consumer raft groups don't match")
			}
		}
		return nil
	})

	// Install snapshots on all remaining servers to "hide" the intermediate consumer recreate requests.
	for _, s := range c.servers {
		if s != rs {
			sjs := s.getJetStream()
			require_NotNil(t, sjs)
			snap, _, _, err := sjs.metaSnapshot()
			require_NoError(t, err)
			sjs.mu.RLock()
			meta := sjs.cluster.meta
			sjs.mu.RUnlock()
			require_NoError(t, meta.InstallSnapshot(snap, false))
		}
	}

	// Restart the server, it should receive a meta snapshot and recognize the consumer recreation.
	rs = c.restartServer(rs)
	checkFor(t, 2*time.Second, 500*time.Millisecond, func() error {
		consumerRg = _EMPTY_
		for _, s := range c.servers {
			if acc, err := s.lookupAccount(globalAccountName); err != nil {
				return err
			} else if mset, err := acc.lookupStream("TEST"); err != nil {
				return err
			} else if o := mset.lookupConsumer("CONSUMER"); o == nil {
				return errors.New("consumer doesn't exist")
			} else if rn := o.raftNode(); rn == nil {
				return errors.New("consumer raft node doesn't exist")
			} else if ccrg := rn.Group(); ccrg == _EMPTY_ {
				return errors.New("consumer raft group doesn't exist")
			} else if consumerRg == _EMPTY_ {
				consumerRg = ccrg
			} else if consumerRg != ccrg {
				return errors.New("consumer raft groups don't match")
			}
		}
		return nil
	})
}

func TestJetStreamClusterUpgradeStreamVersioning(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "R3S", 3)
	defer c.shutdown()

	nc, js := jsClientConnect(t, c.randomServer())
	defer nc.Close()

	ml := c.leader()
	sjs := ml.getJetStream()
	rn := sjs.getMetaGroup()
	acc, err := ml.lookupAccount(globalAccountName)
	require_NoError(t, err)

	// Create stream config.
	cfg, apiErr := ml.checkStreamCfg(&StreamConfig{Name: "TEST", Subjects: []string{"foo"}}, acc, false)
	require_True(t, apiErr == nil)

	// Create and propose stream assignment.
	ci := &ClientInfo{Cluster: "R3S", Account: globalAccountName}
	rg, perr := sjs.createGroupForStream(ci, &cfg)
	require_True(t, perr == nil)
	sa := &streamAssignment{Group: rg, Sync: syncSubjForStream(), Config: &cfg, Client: ci, Created: time.Now().UTC()}
	require_NoError(t, rn.Propose(rn.Term(), encodeAddStreamAssignment(sa)))

	// Wait for the stream assignment to have gone through.
	checkFor(t, 2*time.Second, 500*time.Millisecond, func() error {
		sjs.mu.RLock()
		defer sjs.mu.RUnlock()
		if sjs.streamAssignment(globalAccountName, "TEST") == nil {
			return errors.New("stream assignment does not exist yet")
		}
		return nil
	})

	for _, create := range []bool{true, false} {
		title := "create"
		if !create {
			title = "update"
		}
		t.Run(title, func(t *testing.T) {
			if create {
				// Create on 2.11+ should be idempotent with previous create on 2.10-.
				mcfg := &StreamConfig{}
				setStaticStreamMetadata(mcfg)
				si, err := js.AddStream(&nats.StreamConfig{
					Name:     "TEST",
					Subjects: []string{"foo"},
					Metadata: setDynamicStreamMetadata(mcfg).Metadata,
				})
				require_NoError(t, err)
				deleteDynamicMetadata(si.Config.Metadata)
				require_Len(t, len(si.Config.Metadata), 0)
			} else {
				// Update populates the versioning metadata.
				si, err := js.UpdateStream(&nats.StreamConfig{Name: "TEST", Subjects: []string{"foo"}})
				require_NoError(t, err)
				require_Len(t, len(si.Config.Metadata), 3)
			}
		})
	}
}

func TestJetStreamClusterUpgradeConsumerVersioning(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "R3S", 3)
	defer c.shutdown()

	nc, js := jsClientConnect(t, c.randomServer())
	defer nc.Close()

	_, err := js.AddStream(&nats.StreamConfig{
		Name:      "TEST",
		Subjects:  []string{"foo"},
		Retention: nats.LimitsPolicy,
		Replicas:  1,
	})
	require_NoError(t, err)

	ml := c.leader()
	sjs := ml.getJetStream()

	// Wait for the stream assignment to have gone through.
	checkFor(t, 2*time.Second, 500*time.Millisecond, func() error {
		sjs.mu.RLock()
		defer sjs.mu.RUnlock()
		if sjs.streamAssignment(globalAccountName, "TEST") == nil {
			return errors.New("stream assignment does not exist yet")
		}
		return nil
	})

	rn := sjs.getMetaGroup()
	sa := sjs.streamAssignment(globalAccountName, "TEST")
	require_NotNil(t, sa)
	acc, err := ml.lookupAccount(globalAccountName)
	require_NoError(t, err)

	// Create consumer config.
	cfg := &ConsumerConfig{Durable: "CONSUMER", Name: "CONSUMER"}
	streamCfg, ok := sjs.clusterStreamConfig(acc.Name, "TEST")
	if !ok {
		require_NoError(t, NewJSStreamNotFoundError())
	}
	selectedLimits, _, _, apiErr := acc.selectLimits(cfg.replicas(&streamCfg))
	if apiErr != nil {
		require_NoError(t, apiErr)
	}
	srvLim := &ml.getOpts().JetStreamLimits
	apiErr = setConsumerConfigDefaults(cfg, &streamCfg, srvLim, selectedLimits, false)
	if apiErr != nil {
		require_NoError(t, apiErr)
	}

	// Create and propose consumer assignment.
	ci := &ClientInfo{Cluster: "R3S", Account: globalAccountName}
	rg, cerr := sjs.cluster.createGroupForConsumer(cfg, sa)
	require_True(t, cerr == nil)
	ca := &consumerAssignment{Group: rg, Stream: "TEST", Name: "CONSUMER", Config: cfg, Client: ci, Created: time.Now().UTC()}
	require_NoError(t, rn.Propose(rn.Term(), encodeAddConsumerAssignment(ca)))

	// Wait for the consumer assignment to have gone through.
	checkFor(t, 2*time.Second, 500*time.Millisecond, func() error {
		sjs.mu.RLock()
		defer sjs.mu.RUnlock()
		if sjs.consumerAssignment(globalAccountName, "TEST", "CONSUMER") == nil {
			return errors.New("consumer assignment does not exist yet")
		}
		return nil
	})

	for _, create := range []bool{true, false} {
		title := "create"
		if !create {
			title = "update"
		}
		t.Run(title, func(t *testing.T) {
			createConsumerRequest := func(obsReq CreateConsumerRequest) (*JSApiConsumerInfoResponse, error) {
				req, err := json.Marshal(obsReq)
				if err != nil {
					return nil, err
				}
				msg, err := nc.Request(fmt.Sprintf(JSApiDurableCreateT, "TEST", "CONSUMER"), req, time.Second)
				if err != nil {
					return nil, err
				}
				var resp JSApiConsumerInfoResponse
				require_NoError(t, json.Unmarshal(msg.Data, &resp))
				if resp.Error != nil {
					return nil, resp.Error
				}
				return &resp, nil
			}

			if create {
				// Create on 2.11+ should be idempotent with previous create on 2.10-.
				ncfg := &ConsumerConfig{Durable: "CONSUMER"}
				setStaticConsumerMetadata(ncfg)
				obsReq := CreateConsumerRequest{
					Stream: "TEST",
					Config: *setDynamicConsumerMetadata(ncfg),
					Action: ActionCreate,
				}
				resp, err := createConsumerRequest(obsReq)
				require_NoError(t, err)
				deleteDynamicMetadata(resp.Config.Metadata)
				require_Len(t, len(resp.Config.Metadata), 0)
			} else {
				// Update populates the versioning metadata.
				obsReq := CreateConsumerRequest{
					Stream: "TEST",
					Config: ConsumerConfig{Durable: "CONSUMER"},
					Action: ActionUpdate,
				}
				resp, err := createConsumerRequest(obsReq)
				require_NoError(t, err)
				require_Len(t, len(resp.Config.Metadata), 3)
			}
		})
	}
}

func TestJetStreamClusterInterestPolicyAckAll(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "R3S", 3)
	defer c.shutdown()

	nc, js := jsClientConnect(t, c.randomServer())
	defer nc.Close()

	_, err := js.AddStream(&nats.StreamConfig{
		Name:      "TEST",
		Retention: nats.InterestPolicy,
		Subjects:  []string{"foo"},
		Replicas:  3,
	})
	require_NoError(t, err)

	_, err = js.AddConsumer("TEST", &nats.ConsumerConfig{
		Durable:   "CONSUMER",
		AckPolicy: nats.AckAllPolicy,
	})
	require_NoError(t, err)

	for i := 0; i < 100; i++ {
		_, err = js.Publish("foo", []byte("ok"))
		require_NoError(t, err)
	}

	expectedStreamMsgs := func(msgs uint64) {
		t.Helper()
		checkFor(t, 2*time.Second, 200*time.Millisecond, func() error {
			si, err := js.StreamInfo("TEST")
			if err != nil {
				return err
			}
			if si.State.Msgs != msgs {
				return fmt.Errorf("require uint64 equal, but got: %d != %d", si.State.Msgs, msgs)
			}
			return nil
		})
	}
	expectedStreamMsgs(100)

	for _, s := range c.servers {
		acc, err := s.lookupAccount(globalAccountName)
		require_NoError(t, err)
		mset, err := acc.lookupStream("TEST")
		require_NoError(t, err)
		o := mset.lookupConsumer("CONSUMER")
		require_NotNil(t, o)
		o.mu.Lock()
		// Ensure o.checkStateForInterestStream can't hide that the issue happened.
		o.chkflr = 1000
		o.mu.Unlock()
	}

	sub, err := js.PullSubscribe("foo", "CONSUMER")
	require_NoError(t, err)
	msgs, err := sub.Fetch(50)
	require_NoError(t, err)
	require_True(t, len(msgs) == 50)
	require_NoError(t, msgs[49].AckSync())

	expectedStreamMsgs(50)
}

func TestJetStreamClusterPreserveRedeliveredWithLaggingStream(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "R3S", 3)
	defer c.shutdown()

	nc, js := jsClientConnect(t, c.randomServer())
	defer nc.Close()

	_, err := js.AddStream(&nats.StreamConfig{
		Name:      "TEST",
		Subjects:  []string{"foo"},
		Retention: nats.LimitsPolicy,
		Replicas:  3,
	})
	require_NoError(t, err)

	_, err = js.Publish("foo", nil)
	require_NoError(t, err)

	sub, err := js.PullSubscribe("foo", "CONSUMER", nats.AckWait(500*time.Millisecond))
	require_NoError(t, err)
	defer sub.Unsubscribe()

	msgs, err := sub.Fetch(1)
	require_NoError(t, err)
	require_Len(t, len(msgs), 1)
	require_NoError(t, msgs[0].AckSync())

	rsf := c.randomNonStreamLeader(globalAccountName, "TEST")
	acc, err := rsf.lookupAccount(globalAccountName)
	require_NoError(t, err)
	mset, err := acc.lookupStream("TEST")
	require_NoError(t, err)
	require_NoError(t, mset.raftNode().PauseApply())

	_, err = js.Publish("foo", nil)
	require_NoError(t, err)

	// Move consumer leader to equal stream leader.
	sl := c.streamLeader(globalAccountName, "TEST")
	req := JSApiLeaderStepdownRequest{Placement: &Placement{Preferred: sl.Name()}}
	data, err := json.Marshal(req)
	require_NoError(t, err)
	_, err = nc.Request(fmt.Sprintf(JSApiConsumerLeaderStepDownT, "TEST", "CONSUMER"), data, time.Second)
	require_NoError(t, err)
	c.waitOnConsumerLeader(globalAccountName, "TEST", "CONSUMER")
	require_Equal(t, c.consumerLeader(globalAccountName, "TEST", "CONSUMER").Name(), sl.Name())

	// Get the message to be stored in redelivered state.
	msgs, err = sub.Fetch(1)
	require_NoError(t, err)
	require_Len(t, len(msgs), 1)
	meta, err := msgs[0].Metadata()
	require_NoError(t, err)
	require_Equal(t, meta.NumDelivered, 1)

	msgs, err = sub.Fetch(1)
	require_NoError(t, err)
	require_Len(t, len(msgs), 1)
	meta, err = msgs[0].Metadata()
	require_NoError(t, err)
	require_Equal(t, meta.NumDelivered, 2)

	// Move consumer leader to a different server.
	req = JSApiLeaderStepdownRequest{Placement: &Placement{Preferred: rsf.Name()}}
	data, err = json.Marshal(req)
	require_NoError(t, err)
	_, err = nc.Request(fmt.Sprintf(JSApiConsumerLeaderStepDownT, "TEST", "CONSUMER"), data, time.Second)
	require_NoError(t, err)
	c.waitOnConsumerLeader(globalAccountName, "TEST", "CONSUMER")
	require_Equal(t, c.consumerLeader(globalAccountName, "TEST", "CONSUMER").Name(), rsf.Name())

	// Confirming the stream contents of the consumer leader don't contain the message yet.
	msgs, err = sub.Fetch(1, nats.MaxWait(time.Second))
	require_Error(t, err, nats.ErrTimeout)
	require_Len(t, len(msgs), 0)

	// Now let it apply the missing message.
	mset.raftNode().ResumeApply()

	// Should preserve delivered state properly, otherwise the client would observe NumDelivered rolled back.
	msgs, err = sub.Fetch(1)
	require_NoError(t, err)
	require_Len(t, len(msgs), 1)
	meta, err = msgs[0].Metadata()
	require_NoError(t, err)
	require_Equal(t, meta.NumDelivered, 3)
}

func TestJetStreamClusterInvalidJSACKOverRoute(t *testing.T) {
	test := func(t *testing.T, stream, consumer, subject string) {
		c := createJetStreamClusterExplicit(t, "R3S", 3)
		defer c.shutdown()

		nc, js := jsClientConnect(t, c.randomServer())
		defer nc.Close()

		_, err := js.AddStream(&nats.StreamConfig{
			Name:      stream,
			Subjects:  []string{"foo.>"},
			Retention: nats.LimitsPolicy,
			Replicas:  3,
		})
		require_NoError(t, err)

		_, err = js.AddConsumer(stream, &nats.ConsumerConfig{
			Durable:   consumer,
			AckPolicy: nats.AckExplicitPolicy,
		})
		require_NoError(t, err)

		// Reconnect to consumer follower.
		nc.Close()
		rs := c.randomNonConsumerLeader(globalAccountName, stream, consumer)
		nc, js = jsClientConnect(t, rs)
		defer nc.Close()

		_, err = js.Publish(subject, nil)
		require_NoError(t, err)

		checkFor(t, time.Second, 100*time.Millisecond, func() error {
			return checkState(t, c, globalAccountName, stream)
		})

		sub, err := js.PullSubscribe("foo.>", consumer)
		require_NoError(t, err)
		defer sub.Unsubscribe()

		// The message went over a route and should have the correct subject, and working ACK.
		msgs, err := sub.Fetch(1)
		require_NoError(t, err)
		require_Len(t, len(msgs), 1)
		require_Equal(t, msgs[0].Subject, subject)
		require_NoError(t, msgs[0].AckSync())
	}

	for _, s := range []string{"StreamNoAt", "StreamWith@At"} {
		for _, c := range []string{"ConsumerNoAt", "ConsumerWith@At"} {
			for _, subject := range []string{"foo.no.at", "foo.with@at"} {
				t.Run(fmt.Sprintf("%s/%s/%s", s, c, subject), func(t *testing.T) {
					test(t, s, c, subject)
				})
			}
		}
	}
}

func TestJetStreamClusterConsumerOnlyDeliverMsgAfterQuorum(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "R3S", 3)
	defer c.shutdown()

	nc, js := jsClientConnect(t, c.randomServer())
	defer nc.Close()

	_, err := js.AddStream(&nats.StreamConfig{
		Name:      "TEST",
		Subjects:  []string{"foo"},
		Retention: nats.LimitsPolicy,
		Replicas:  3,
	})
	require_NoError(t, err)

	_, err = js.AddConsumer("TEST", &nats.ConsumerConfig{
		Durable:   "CONSUMER",
		AckPolicy: nats.AckExplicitPolicy,
		Replicas:  3,
		AckWait:   2 * time.Second,
	})
	require_NoError(t, err)

	_, err = js.Publish("foo", nil)
	require_NoError(t, err)

	checkFor(t, time.Second, 100*time.Millisecond, func() error {
		return checkState(t, c, globalAccountName, "TEST")
	})

	cl := c.consumerLeader(globalAccountName, "TEST", "CONSUMER")
	acc, err := cl.lookupAccount(globalAccountName)
	require_NoError(t, err)
	mset, err := acc.lookupStream("TEST")
	require_NoError(t, err)
	o := mset.lookupConsumer("CONSUMER")
	require_NotNil(t, o)
	rn := o.raftNode().(*raft)

	// Force the leader to not be able to make proposals.
	rn.Lock()
	rn.werr = errors.New("block proposals")
	rn.Unlock()

	sub, err := js.PullSubscribe("foo", "CONSUMER")
	require_NoError(t, err)
	defer sub.Unsubscribe()

	// We must only receive a message AFTER quorum was met for updating delivered state.
	// This should time out since proposals are blocked.
	msgs, err := sub.Fetch(1, nats.MaxWait(2*time.Second))
	require_Error(t, err, nats.ErrTimeout)
	require_Len(t, len(msgs), 0)

	// Allow proposals to be made again.
	rn.Lock()
	rn.werr = nil
	rn.Unlock()

	// Now it should pass.
	msgs, err = sub.Fetch(1, nats.MaxWait(2*time.Second))
	require_NoError(t, err)
	require_Len(t, len(msgs), 1)
	require_NoError(t, msgs[0].AckSync())
}

func TestJetStreamClusterConsumerResetPendingDeliveriesOnMaxAckPendingUpdate(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "R3S", 3)
	defer c.shutdown()

	nc, js := jsClientConnect(t, c.randomServer())
	defer nc.Close()

	_, err := js.AddStream(&nats.StreamConfig{
		Name:      "TEST",
		Subjects:  []string{"foo"},
		Retention: nats.LimitsPolicy,
		Replicas:  3,
	})
	require_NoError(t, err)

	cfg := &nats.ConsumerConfig{
		Durable:       "CONSUMER",
		AckPolicy:     nats.AckExplicitPolicy,
		Replicas:      3,
		AckWait:       2 * time.Second,
		MaxAckPending: 100,
	}
	_, err = js.AddConsumer("TEST", cfg)
	require_NoError(t, err)

	cl := c.consumerLeader(globalAccountName, "TEST", "CONSUMER")
	acc, err := cl.lookupAccount(globalAccountName)
	require_NoError(t, err)
	mset, err := acc.lookupStream("TEST")
	require_NoError(t, err)
	o := mset.lookupConsumer("CONSUMER")
	require_NotNil(t, o)

	o.mu.Lock()
	o.pendingDeliveries = map[uint64]*jsPubMsg{0: getJSPubMsgFromPool()}
	o.mu.Unlock()

	// Increasing does not reset pending deliveries.
	cfg.MaxAckPending++
	_, err = js.UpdateConsumer("TEST", cfg)
	require_NoError(t, err)

	o.mu.Lock()
	l := len(o.pendingDeliveries)
	o.mu.Unlock()
	require_Equal(t, l, 1)

	// Decreasing does reset pending deliveries, so we can shrink the map.
	cfg.MaxAckPending--
	_, err = js.UpdateConsumer("TEST", cfg)
	require_NoError(t, err)

	o.mu.Lock()
	l = len(o.pendingDeliveries)
	o.mu.Unlock()
	require_Equal(t, l, 0)
}

func TestJetStreamClusterConsumerActiveAfterDidNotDeliverOverRoute(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "R3S", 3)
	defer c.shutdown()

	nc, js := jsClientConnect(t, c.randomServer())
	defer nc.Close()

	_, err := js.AddStream(&nats.StreamConfig{
		Name:     "TEST",
		Subjects: []string{"foo"},
		Replicas: 3,
	})
	require_NoError(t, err)

	pubAck, err := js.Publish("foo", nil)
	require_NoError(t, err)
	require_Equal(t, pubAck.Sequence, 1)

	_, err = js.AddConsumer("TEST", &nats.ConsumerConfig{
		Durable:        "CONSUMER",
		Replicas:       3,
		DeliverSubject: "deliver_subject",
	})
	require_NoError(t, err)

	ci, err := js.ConsumerInfo("TEST", "CONSUMER")
	require_NoError(t, err)
	require_False(t, ci.PushBound)

	cl := c.consumerLeader(globalAccountName, "TEST", "CONSUMER")
	rs := c.randomNonConsumerLeader(globalAccountName, "TEST", "CONSUMER")
	ncRs, jsRs := jsClientConnect(t, rs)
	defer ncRs.Close()

	// Just noop callback.
	cb := func(msg *nats.Msg) {}
	_, err = jsRs.Subscribe("foo", cb, nats.Bind("TEST", "CONSUMER"))
	require_NoError(t, err)

	// Eventually the sub should be known to the leader, and report active/bound.
	checkFor(t, 2*time.Second, 100*time.Millisecond, func() error {
		ci, err = js.ConsumerInfo("TEST", "CONSUMER")
		if err != nil {
			return err
		}
		if !ci.PushBound {
			return errors.New("push not bound")
		}
		return nil
	})

	// Simulate failed delivery over route connection.
	acc, err := cl.lookupAccount(globalAccountName)
	require_NoError(t, err)
	mset, err := acc.lookupStream("TEST")
	require_NoError(t, err)
	o := mset.lookupConsumer("CONSUMER")
	require_NotNil(t, o)
	o.didNotDeliver(1, "foo")

	// Consumer should still be active/bound.
	ci, err = js.ConsumerInfo("TEST", "CONSUMER")
	require_NoError(t, err)
	require_True(t, ci.PushBound)
}

func TestJetStreamClusterOfflineR1StreamDenyUpdate(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "R3S", 3)
	defer c.shutdown()

	nc, js := jsClientConnect(t, c.randomServer())
	defer nc.Close()

	cfg := &nats.StreamConfig{
		Name:     "TEST",
		Subjects: []string{"foo"},
		Replicas: 1,
	}
	_, err := js.AddStream(cfg)
	require_NoError(t, err)

	// Stop current R1 stream leader.
	sl := c.streamLeader(globalAccountName, "TEST")
	sl.Shutdown()
	nc.Close()
	nc, js = jsClientConnect(t, c.randomServer())
	defer nc.Close()

	// Wait for meta leader, so we can send an update.
	c.waitOnLeader()

	_, err = js.StreamInfo("TEST")
	require_Error(t, err, NewJSStreamOfflineError())

	cfg.Replicas = 3
	_, err = js.UpdateStream(cfg)
	require_Error(t, err, NewJSStreamOfflineError())
}

func TestJetStreamClusterOfflineR1ConsumerDenyUpdate(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "R3S", 3)
	defer c.shutdown()

	nc, js := jsClientConnect(t, c.randomServer())
	defer nc.Close()

	_, err := js.AddStream(&nats.StreamConfig{
		Name:     "TEST",
		Subjects: []string{"foo"},
		Replicas: 3,
	})
	require_NoError(t, err)

	cfg := &nats.ConsumerConfig{
		Durable:  "CONSUMER",
		Replicas: 1,
	}
	_, err = js.AddConsumer("TEST", cfg)
	require_NoError(t, err)

	// Stop current R1 consumer leader.
	cl := c.consumerLeader(globalAccountName, "TEST", "CONSUMER")
	cl.Shutdown()
	nc.Close()
	nc, js = jsClientConnect(t, c.randomServer())
	defer nc.Close()

	// Wait for meta leader, so we can send an update.
	c.waitOnLeader()

	_, err = js.ConsumerInfo("TEST", "CONSUMER")
	require_Error(t, err, NewJSConsumerOfflineError())

	cfg.Replicas = 3
	_, err = js.UpdateConsumer("TEST", cfg)
	require_Error(t, err, NewJSConsumerOfflineError())
}

func TestJetStreamClusterSnapshotStreamAssetOnShutdown(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "R3S", 3)
	defer c.shutdown()

	nc, js := jsClientConnect(t, c.randomServer())
	defer nc.Close()

	_, err := js.AddStream(&nats.StreamConfig{
		Name:     "TEST",
		Subjects: []string{"foo"},
		Replicas: 3,
	})
	require_NoError(t, err)

	var sds []string
	for _, s := range c.servers {
		sds = append(sds, s.StoreDir())
	}

	// Snapshots installed for the stream's Raft group. Skips temporary files, an
	// install writes into one before renaming it into place.
	streamSnapshots := func(sd string) ([]string, error) {
		matches, err := filepath.Glob(filepath.Join(sd, "$SYS", "_js_", "*", snapshotsDir))
		if err != nil {
			return nil, err
		}
		// Matches _meta_ and stream raft groups.
		if len(matches) != 2 {
			return nil, fmt.Errorf("expected 2 snapshot dirs for %s, got %d", sd, len(matches))
		}
		for _, match := range matches {
			if !strings.Contains(match, "S-R3F") {
				continue
			}
			entries, err := os.ReadDir(match)
			if err != nil {
				return nil, err
			}
			var snaps []string
			for _, entry := range entries {
				if name := entry.Name(); !strings.HasSuffix(name, ".tmp") {
					snaps = append(snaps, name)
				}
			}
			return snaps, nil
		}
		return nil, fmt.Errorf("no stream raft group for %s", sd)
	}

	// Publish, so we have something new to snapshot.
	_, err = js.Publish("foo", nil)
	require_NoError(t, err)
	checkFor(t, 2*time.Second, 200*time.Millisecond, func() error {
		return checkState(t, c, globalAccountName, "TEST")
	})

	applied := make(map[string]uint64, len(sds))
	for i, s := range c.servers {
		mset, err := s.globalAccount().lookupStream("TEST")
		require_NoError(t, err)
		_, _, a := mset.raftNode().Progress()
		applied[sds[i]] = a
	}

	// Shutdown servers, and check if all made stream snapshots.
	for _, s := range c.servers {
		s.Shutdown()
	}
	for _, sd := range sds {
		snaps, err := streamSnapshots(sd)
		require_NoError(t, err)
		if len(snaps) != 1 {
			t.Fatalf("Expected 1 snapshot for %s, got %d: %v", sd, len(snaps), snaps)
		}
		_, index, err := termAndIndexFromSnapFile(snaps[0])
		require_NoError(t, err)
		if index < applied[sd] {
			t.Fatalf("Snapshot %s for %s doesn't cover applied index %d", snaps[0], sd, applied[sd])
		}
	}
}

func TestJetStreamClusterDontReviveRemovedStream(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "R3S", 3)
	defer c.shutdown()

	nc, js := jsClientConnect(t, c.randomServer())
	defer nc.Close()

	cfg := &nats.StreamConfig{
		Name:     "TEST",
		Subjects: []string{"foo"},
		Replicas: 3,
	}
	_, err := js.AddStream(cfg)
	require_NoError(t, err)

	// Wait for stream to be assigned on all servers.
	checkFor(t, 2*time.Second, 200*time.Millisecond, func() error {
		for _, s := range c.servers {
			if !s.JetStreamIsStreamAssigned(globalAccountName, "TEST") {
				return fmt.Errorf("stream not yet assigned on %s", s.Name())
			}
		}
		return nil
	})

	sl := c.streamLeader(globalAccountName, "TEST")
	rs := c.randomNonStreamLeader(globalAccountName, "TEST")
	mset, err := rs.globalAccount().lookupStream("TEST")
	require_NoError(t, err)

	// Simulate the stream being scaled down, quickly after it was scaled up.
	cfg.Replicas = 1
	_, err = js.UpdateStream(cfg)
	require_NoError(t, err)

	// Wait for the stream to not be assigned on the servers (except the leader).
	checkFor(t, 2*time.Second, 200*time.Millisecond, func() error {
		for _, s := range c.servers {
			if s == sl {
				continue
			}
			if s.JetStreamIsStreamAssigned(globalAccountName, "TEST") {
				return fmt.Errorf("stream still assigned on %s", s.Name())
			}
		}
		if !mset.closed.Load() {
			return fmt.Errorf("stream not closed yet")
		}
		return nil
	})

	// Simulating the stream was catching up and is resetting after timing out.
	require_True(t, mset.resetClusteredState(mset.raftNode(), nil))

	// Allow for some time here, mset.resetClusteredState might
	// spin up a goroutine if it's resetting the stream.
	// Confirm the stream is not revived.
	time.Sleep(200 * time.Millisecond)
	_, err = rs.globalAccount().lookupStream("TEST")
	require_Error(t, err, ErrJetStreamStreamNotFound)
}

func TestJetStreamClusterCreateR3StreamWithOfflineNodes(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "R3S", 3)
	defer c.shutdown()

	ml := c.leader()
	nc, js := jsClientConnect(t, ml)
	defer nc.Close()

	_, err := js.AddStream(&nats.StreamConfig{Name: "FOO", Replicas: 3})
	require_NoError(t, err)

	// Shutdown one server, we'll still have quorum.
	rs1 := c.randomNonLeader()
	rs1.Shutdown()
	_, err = js.AddStream(&nats.StreamConfig{Name: "BAR", Replicas: 3})
	require_NoError(t, err)

	// While we still have quorum, publish a few messages to the streams.
	// The offline server will need to both assign the stream and catchup when it comes back.
	for _, subj := range []string{"FOO", "BAR"} {
		_, err = js.Publish(subj, nil)
		require_NoError(t, err)
	}

	// Shutdown one more server, only one server left that can't reach quorum on its own.
	rs2 := c.randomNonLeader()
	rs2.Shutdown()
	_, err = js.AddStream(&nats.StreamConfig{Name: "BAZ", Replicas: 3})
	require_Contains(t, err.Error(), "no suitable peers for placement", "peer offline")

	// Restart offline servers and ensure all comes back up.
	c.restartServer(rs1)
	c.restartServer(rs2)
	c.waitOnLeader()
	c.waitOnStreamLeader(globalAccountName, "FOO")
	c.waitOnStreamLeader(globalAccountName, "BAR")
	checkFor(t, 10*time.Second, 200*time.Millisecond, func() error {
		if err := checkState(t, c, globalAccountName, "FOO"); err != nil {
			return err
		}
		if err := checkState(t, c, globalAccountName, "BAR"); err != nil {
			return err
		}
		return nil
	})
}

func TestJetStreamClusterCreateEphemeralConsumerWithOfflineNodes(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "R3S", 3)
	defer c.shutdown()

	ml := c.leader()
	nc, js := jsClientConnect(t, ml)
	defer nc.Close()
	_, err := js.AddStream(&nats.StreamConfig{Name: "TEST", Replicas: 3})
	require_NoError(t, err)

	// Shutdown a random server.
	c.randomNonLeader().Shutdown()

	for range 10 {
		_, err = js.AddConsumer("TEST", &nats.ConsumerConfig{})
		require_NoError(t, err)
	}
}

func TestJetStreamClusterSetPreferredToOnlineNode(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "R3S", 3)
	defer c.shutdown()

	// Shutdown one random server.
	ml := c.leader()
	rs := c.randomNonLeader()
	js := rs.getJetStream()
	cc := js.cluster
	require_NotNil(t, cc)
	meta := cc.meta
	require_NotNil(t, meta)
	rsid := meta.ID()
	rs.Shutdown()

	cfg := &StreamConfig{Storage: FileStorage, Replicas: 3}
	js = ml.getJetStream()
	cc = js.cluster
	require_NotNil(t, cc)
	peers, err := cc.selectPeerGroup(3, "R3S", cfg, nil, 0, nil, 1, nil)
	// Need explicit nil-check.
	if err != nil {
		require_NoError(t, err)
	}
	require_Len(t, len(peers), 3)

	// Should prefer online peers when selecting preferred.
	rg := &raftGroup{Name: groupNameForStream(peers, cfg.Storage), Storage: cfg.Storage, Peers: peers, Cluster: "R3S"}
	rg.setPreferred(ml)
	require_NotEqual(t, rg.Preferred, rsid)
}

func TestJetStreamClusterAsyncFlushBasics(t *testing.T) {
	test := func(t *testing.T, syncAlways bool) {
		supportsAsync := !syncAlways

		c := createJetStreamClusterExplicit(t, "R3S", 3)
		defer c.shutdown()

		for _, s := range c.servers {
			s.optsMu.Lock()
			s.opts.SyncAlways = syncAlways
			s.optsMu.Unlock()
		}

		nc, js := jsClientConnect(t, c.randomServer())
		defer nc.Close()

		var s *Server
		checkStoreIsAsync := func(expectAsync bool) {
			t.Helper()
			checkFor(t, 2*time.Second, 50*time.Millisecond, func() error {
				mset, err := s.globalAccount().lookupStream("TEST")
				if err != nil {
					return err
				}
				fs := mset.Store().(*fileStore)
				fs.mu.RLock()
				lmb := fs.lmb
				asyncFlush := fs.fcfg.AsyncFlush
				fip := fs.fip
				fs.mu.RUnlock()
				if asyncFlush != expectAsync {
					return fmt.Errorf("expected async flush config %v, got %v", expectAsync, asyncFlush)
				}
				if fip != !expectAsync {
					return fmt.Errorf("expected flush in place %v, got %v", !expectAsync, fip)
				}
				if lmb == nil {
					return errors.New("expected last message block to be set")
				}
				lmb.mu.RLock()
				flusher := lmb.flusher
				lmb.mu.RUnlock()
				if flusher != expectAsync {
					return fmt.Errorf("expected flusher initialized %v, got %v", expectAsync, flusher)
				}
				return nil
			})
		}

		cfg := &StreamConfig{
			Name:     "TEST",
			Subjects: []string{"foo"},
			Storage:  FileStorage,
			Replicas: 1,
		}
		// Test disabled async flush on create.
		_, err := jsStreamCreate(t, nc, cfg)
		require_NoError(t, err)
		s = c.streamLeader(globalAccountName, "TEST")
		_, err = js.Publish("foo", nil)
		require_NoError(t, err)
		checkStoreIsAsync(false)

		// Enabling async flush.
		cfg.Replicas = 3
		_, err = jsStreamUpdate(t, nc, cfg)
		require_NoError(t, err)
		c.waitOnStreamLeader(globalAccountName, "TEST")
		checkFor(t, 2*time.Second, 200*time.Millisecond, func() error {
			return checkState(t, c, globalAccountName, "TEST")
		})
		_, err = js.Publish("foo", nil)
		require_NoError(t, err)
		checkStoreIsAsync(supportsAsync)

		// Disabling async flush.
		cfg.Replicas = 1
		_, err = jsStreamUpdate(t, nc, cfg)
		require_NoError(t, err)
		c.waitOnStreamLeader(globalAccountName, "TEST")
		_, err = js.Publish("foo", nil)
		require_NoError(t, err)
		checkStoreIsAsync(false)

		// Test async flush on create.
		require_NoError(t, js.DeleteStream("TEST"))
		cfg.Replicas = 3
		_, err = jsStreamCreate(t, nc, cfg)
		require_NoError(t, err)
		s = c.streamLeader(globalAccountName, "TEST")
		_, err = js.Publish("foo", nil)
		require_NoError(t, err)
		checkStoreIsAsync(supportsAsync)
	}

	t.Run("Default", func(t *testing.T) { test(t, false) })
	t.Run("SyncAlways", func(t *testing.T) { test(t, true) })
}

func TestJetStreamClusterFileStoreSyncOnFlushReplicaTransitions(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "R3S", 3)
	defer c.shutdown()

	for _, s := range c.servers {
		s.optsMu.Lock()
		s.opts.SyncAlways = true
		s.optsMu.Unlock()
	}

	nc, _ := jsClientConnect(t, c.randomServer())
	defer nc.Close()

	cfg := &StreamConfig{
		Name:     "TEST",
		Subjects: []string{"foo"},
		Storage:  FileStorage,
		Replicas: 3,
	}
	_, err := jsStreamCreate(t, nc, cfg)
	require_NoError(t, err)

	checkMode := func(expectSyncAlways, expectSyncOnFlush bool) {
		t.Helper()
		s := c.streamLeader(globalAccountName, cfg.Name)
		mset, err := s.globalAccount().lookupStream(cfg.Name)
		require_NoError(t, err)
		fs := mset.Store().(*fileStore)
		fs.mu.RLock()
		configuredSyncAlways := fs.fcfg.SyncAlways
		configuredSyncOnFlush := fs.fcfg.SyncOnFlush
		fs.mu.RUnlock()
		require_True(t, configuredSyncAlways)
		require_True(t, configuredSyncOnFlush)
		require_Equal(t, fs.syncAlways.Load(), expectSyncAlways)
		require_Equal(t, fs.syncOnFlush.Load(), expectSyncOnFlush)
	}

	checkMode(false, true)

	cfg.Replicas = 1
	_, err = jsStreamUpdate(t, nc, cfg)
	require_NoError(t, err)
	c.waitOnStreamLeader(globalAccountName, cfg.Name)
	checkMode(true, false)
	const numMsgs = 10
	for range numMsgs {
		sendStreamMsg(t, nc, "foo", "msg")
	}

	cfg.Replicas = 3
	_, err = jsStreamUpdate(t, nc, cfg)
	require_NoError(t, err)
	c.waitOnStreamLeader(globalAccountName, cfg.Name)
	checkFor(t, 2*time.Second, 200*time.Millisecond, func() error {
		return checkState(t, c, globalAccountName, cfg.Name)
	})
	checkMode(false, true)

	s := c.streamLeader(globalAccountName, cfg.Name)
	mset, err := s.globalAccount().lookupStream(cfg.Name)
	require_NoError(t, err)
	state := mset.state()
	require_Equal(t, state.Msgs, numMsgs)
	require_Equal(t, state.LastSeq, numMsgs)
}

func TestJetStreamClusterPrepareForWALReplayDoesNotAdvanceStore(t *testing.T) {
	s := RunBasicJetStreamServer(t)
	defer s.Shutdown()

	nc, js := jsClientConnect(t, s)
	defer nc.Close()

	_, err := js.AddStream(&nats.StreamConfig{Name: "TEST", Subjects: []string{"foo"}, Storage: nats.FileStorage})
	require_NoError(t, err)
	_, err = js.Publish("foo", []byte("one"))
	require_NoError(t, err)

	mset, err := s.GlobalAccount().lookupStream("TEST")
	require_NoError(t, err)
	require_NoError(t, mset.prepareForWALReplay(&StreamReplicatedState{LastSeq: 2}))

	state := mset.state()
	require_Equal(t, state.Msgs, 1)
	require_Equal(t, state.LastSeq, 1)
	require_Equal(t, mset.lastSeq(), 1)
}

func TestJetStreamClusterWALReplayPreservesFirstSeq(t *testing.T) {
	c := createJetStreamClusterWithTemplate(t, jsClusterSyncAlwaysTempl, "R3S", 3)
	defer c.shutdown()

	nc, js := jsClientConnect(t, c.randomServer())
	defer nc.Close()

	const firstSeq = 1000
	si, err := js.AddStream(&nats.StreamConfig{
		Name:     "TEST",
		Subjects: []string{"foo"},
		Storage:  nats.FileStorage,
		Replicas: 3,
		FirstSeq: firstSeq,
	})
	require_NoError(t, err)
	require_Equal(t, si.State.FirstSeq, firstSeq)
	require_Equal(t, si.State.LastSeq, firstSeq-1)
	c.waitOnStreamLeader(globalAccountName, "TEST")

	// Restart a follower without allowing server shutdown to create its first
	// snapshot. Its filestore must therefore be recovered from the complete WAL.
	restartWithoutSnapshot := func(rs *Server) *Server {
		t.Helper()
		mset, err := rs.globalAccount().lookupStream("TEST")
		require_NoError(t, err)
		rn := mset.raftNode()
		require_True(t, mset.shouldReplayFromWAL())
		_, _, err = rn.LoadLastSnapshot()
		require_Error(t, err, errNoSnapAvailable)

		rn.Stop()
		rn.WaitForStop()
		rs.Shutdown()
		rs.WaitForShutdown()
		rs = c.restartServer(rs)
		c.waitOnStreamCurrent(rs, globalAccountName, "TEST")
		return rs
	}

	rs := c.randomNonStreamLeader(globalAccountName, "TEST")
	require_NotNil(t, rs)
	rs = restartWithoutSnapshot(rs)

	// An empty stream has no WAL entry recording FirstSeq, so make sure recovery preserves it.
	mset, err := rs.globalAccount().lookupStream("TEST")
	require_NoError(t, err)
	require_Equal(t, mset.state().Msgs, 0)
	require_Equal(t, mset.state().FirstSeq, firstSeq)
	require_Equal(t, mset.state().LastSeq, firstSeq-1)

	// Repeat with an existing publish to exercise replay at FirstSeq as well.
	pa, err := js.Publish("foo", []byte("one"))
	require_NoError(t, err)
	require_Equal(t, pa.Sequence, firstSeq)
	checkFor(t, 2*time.Second, 200*time.Millisecond, func() error {
		return checkState(t, c, globalAccountName, "TEST")
	})

	rs = restartWithoutSnapshot(rs)
	mset, err = rs.globalAccount().lookupStream("TEST")
	require_NoError(t, err)
	require_Equal(t, mset.state().Msgs, 1)
	require_Equal(t, mset.state().FirstSeq, firstSeq)
	require_Equal(t, mset.state().LastSeq, firstSeq)

	// The next publish must continue from the configured first sequence.
	pa, err = js.Publish("foo", []byte("two"))
	require_NoError(t, err)
	require_Equal(t, pa.Sequence, firstSeq+1)
}

type snapshotlessRaftNode struct {
	RaftNode
	id    string
	peers []string
}

func (n *snapshotlessRaftNode) ID() string                         { return n.id }
func (n *snapshotlessRaftNode) Progress() (uint64, uint64, uint64) { return 1, 1, 0 }
func (n *snapshotlessRaftNode) LoadLastSnapshot() (uint64, []byte, error) {
	return 0, nil, errNoSnapAvailable
}
func (n *snapshotlessRaftNode) Peers() []*Peer {
	peers := make([]*Peer, 0, len(n.peers))
	for _, p := range n.peers {
		peers = append(peers, &Peer{ID: p})
	}
	return peers
}

func TestJetStreamClusterPrepareForWALReplayPreservesR1ScaleUpSource(t *testing.T) {
	const source = "source"
	for _, test := range []struct {
		title   string
		peers   []string
		origin  *desiredRaftGroupOrigin
		desired []string
		msgs    uint64
	}{
		// The original R1 peer restarting before bootstrap completed.
		{"R1 origin", []string{source}, &desiredRaftGroupOrigin{Peers: []string{source}, Replicas: 1}, []string{source, "peer-2", "peer-3"}, 3},
		// After stacked updates the origin needn't name the source, only the membership does.
		{"stacked origin", []string{source}, &desiredRaftGroupOrigin{Peers: []string{source, "peer-2", "peer-3"}, Replicas: 3}, []string{source, "peer-4", "peer-5"}, 3},
		// The source only adds peers after snapshotting, so a WAL without one is complete.
		{"not sole member", []string{source, "peer-2"}, &desiredRaftGroupOrigin{Peers: []string{source}, Replicas: 1}, []string{source, "peer-2", "peer-3"}, 0},
		// A plain R1 isn't scaling up, its WAL is complete.
		{"plain R1", []string{source}, nil, nil, 0},
		// A scale down to R1 doesn't grow, its WAL is complete.
		{"scale down to R1", []string{source}, &desiredRaftGroupOrigin{Peers: []string{source, "peer-2", "peer-3"}, Replicas: 3}, []string{source}, 0},
	} {
		t.Run(test.title, func(t *testing.T) {
			s := RunBasicJetStreamServer(t)
			defer s.Shutdown()

			nc, js := jsClientConnect(t, s)
			defer nc.Close()

			_, err := js.AddStream(&nats.StreamConfig{Name: "TEST", Subjects: []string{"foo"}, Storage: nats.FileStorage})
			require_NoError(t, err)
			for range 3 {
				_, err = js.Publish("foo", []byte("msg"))
				require_NoError(t, err)
			}

			mset, err := s.GlobalAccount().lookupStream("TEST")
			require_NoError(t, err)
			fs := mset.store.(*fileStore)
			fs.syncOnFlush.Store(true)

			n := &snapshotlessRaftNode{id: source, peers: test.peers}
			mset.mu.Lock()
			mset.node = n
			mset.sa = &streamAssignment{Group: &raftGroup{}}
			if test.desired != nil {
				mset.sa.Group.Desired = &desiredRaftGroup{Peers: test.desired, Origin: test.origin}
			}
			mset.mu.Unlock()
			defer func() {
				mset.mu.Lock()
				mset.node, mset.sa = nil, nil
				mset.mu.Unlock()
			}()

			// A sole member's new WAL can't reconstruct the existing stream state, so its store must be preserved.
			require_NoError(t, prepareStreamRecovery(mset, n))
			state := mset.state()
			require_Equal(t, state.Msgs, test.msgs)
			require_Equal(t, state.LastSeq, test.msgs)
		})
	}
}

func TestJetStreamClusterInterestCheckDoesNotCompactPastReplay(t *testing.T) {
	s := RunBasicJetStreamServer(t)
	defer s.Shutdown()

	nc, js := jsClientConnect(t, s)
	defer nc.Close()

	_, err := js.AddStream(&nats.StreamConfig{
		Name:      "TEST",
		Subjects:  []string{"foo"},
		Storage:   nats.FileStorage,
		Retention: nats.InterestPolicy,
	})
	require_NoError(t, err)
	_, err = js.AddConsumer("TEST", &nats.ConsumerConfig{Durable: "C", AckPolicy: nats.AckExplicitPolicy})
	require_NoError(t, err)

	const numMsgs = 3
	for range numMsgs {
		_, err = js.Publish("foo", []byte("msg"))
		require_NoError(t, err)
	}
	sub, err := js.PullSubscribe("foo", "C")
	require_NoError(t, err)
	msgs, err := sub.Fetch(numMsgs)
	require_NoError(t, err)
	for _, m := range msgs {
		require_NoError(t, m.AckSync())
	}

	mset, err := s.GlobalAccount().lookupStream("TEST")
	require_NoError(t, err)
	checkFor(t, 2*time.Second, 50*time.Millisecond, func() error {
		if state := mset.state(); state.Msgs != 0 {
			return fmt.Errorf("expected acked messages to be removed, got %d", state.Msgs)
		}
		return nil
	})

	// Checking interest on the full store leaves the consumer's check floor
	// past the last message, as when a replicated stream starts up.
	mset.checkInterestState()

	// Recovery truncates the store to replay the Raft log, while the
	// consumer's ack and check floors are still at the last message.
	require_NoError(t, mset.prepareForWALReplay(nil))
	require_Equal(t, mset.lastSeq(), 0)

	// A leader change checks interest before the replay has caught up. It
	// must not compact the store past the messages that are still to come.
	mset.checkInterestState()
	require_Equal(t, mset.state().LastSeq, 0)
	for seq := uint64(1); seq <= numMsgs; seq++ {
		require_NoError(t, mset.store.StoreRawMsg("foo", nil, []byte("msg"), seq, time.Now().UnixNano(), 0, false))
	}
	require_Equal(t, mset.state().LastSeq, numMsgs)
}

func TestJetStreamClusterPrepareForWALReplayTruncatesStore(t *testing.T) {
	s := RunBasicJetStreamServer(t)
	defer s.Shutdown()

	nc, js := jsClientConnect(t, s)
	defer nc.Close()

	_, err := js.AddStream(&nats.StreamConfig{Name: "TEST", Subjects: []string{"foo"}, Storage: nats.FileStorage})
	require_NoError(t, err)
	for range 3 {
		_, err = js.Publish("foo", []byte("msg"))
		require_NoError(t, err)
	}

	mset, err := s.GlobalAccount().lookupStream("TEST")
	require_NoError(t, err)

	state := mset.state()
	require_Equal(t, state.Msgs, 3)
	require_Equal(t, state.LastSeq, 3)
	require_Equal(t, mset.lastSeq(), 3)

	require_NoError(t, mset.prepareForWALReplay(&StreamReplicatedState{LastSeq: 2}))

	state = mset.state()
	require_Equal(t, state.Msgs, 2)
	require_Equal(t, state.LastSeq, 2)
	require_Equal(t, mset.lastSeq(), 2)
}

func TestJetStreamClusterWALReplayTruncatesDeletedSnapshotSequence(t *testing.T) {
	const (
		snapshotLastSeq = uint64(5)
		finalLastSeq    = uint64(12)
	)

	// SyncAlways is relaxed to SyncOnFlush for replicated streams, enabling
	// recovery from the stream snapshot and its retained Raft WAL tail.
	c := createJetStreamClusterWithTemplate(t, jsClusterSyncAlwaysTempl, "R3S", 3)
	defer c.shutdown()

	nc, js := jsClientConnect(t, c.randomServer())
	defer nc.Close()

	_, err := js.AddStream(&nats.StreamConfig{
		Name:     "TEST",
		Subjects: []string{"foo"},
		Storage:  nats.FileStorage,
		Replicas: 3,
	})
	require_NoError(t, err)

	sl := c.streamLeader(globalAccountName, "TEST")
	mset, err := sl.globalAccount().lookupStream("TEST")
	require_NoError(t, err)
	fs := mset.store.(*fileStore)
	require_True(t, mset.shouldReplayFromWAL())

	// Ten of these messages fit in 350 bytes, placing sequences 1-10 in the
	// first block and 11-12 in the next. This leaves sequence 5 as an interior
	// message in a non-last block that can be compacted safely.
	fs.mu.Lock()
	fs.fcfg.BlockSize = 350
	fs.mu.Unlock()

	for range snapshotLastSeq {
		_, err = js.Publish("foo", []byte("ok"))
		require_NoError(t, err)
	}

	rn := mset.raftNode().(*raft)
	// Recovery will restore this snapshot and truncate the filestore to its
	// LastSeq before replaying the operations retained after it.
	require_NoError(t, rn.InstallSnapshot(mset.stateSnapshot(), false))

	// Delete the snapshot boundary itself, then append later messages so the
	// retained WAL has a tail to replay after truncation.
	for _, seq := range []uint64{snapshotLastSeq - 1, snapshotLastSeq} {
		require_NoError(t, js.DeleteMsg("TEST", seq))
	}
	for range finalLastSeq - snapshotLastSeq {
		_, err = js.Publish("foo", []byte("ok"))
		require_NoError(t, err)
	}

	state := mset.state()
	require_Equal(t, state.Msgs, 10)
	require_Equal(t, state.LastSeq, finalLastSeq)
	require_NoError(t, fs.FlushAllPending())

	// Rewrite the first block without its deleted records, leaving sequence 5
	// represented only by a delete marker.
	smb := fs.selectMsgBlock(snapshotLastSeq)
	require_NotNil(t, smb)
	require_True(t, smb != fs.lmb)
	smb.mu.Lock()
	err = smb.compact()
	smb.mu.Unlock()
	require_NoError(t, err)

	snapIndex, snapData, err := rn.LoadLastSnapshot()
	require_NoError(t, err)
	snap, err := decodeStreamSnapshot(snapData)
	require_NoError(t, err)
	require_Equal(t, snap.LastSeq, snapshotLastSeq)
	index, _, _ := rn.Progress()
	require_True(t, index > snapIndex)

	// Stop the Raft group before the server so shutdown cannot replace the old
	// snapshot. Restart must truncate the store and replay the retained WAL tail.
	rn.Stop()
	rn.WaitForStop()
	sl.Shutdown()
	sl.WaitForShutdown()
	sl = c.restartServer(sl)
	c.waitOnStreamCurrent(sl, globalAccountName, "TEST")

	mset, err = sl.globalAccount().lookupStream("TEST")
	require_NoError(t, err)
	// Wait for the expected final last sequence.
	checkFor(t, 5*time.Second, 100*time.Millisecond, func() error {
		state = mset.store.State()
		if state.LastSeq != finalLastSeq {
			return fmt.Errorf("stream replay at sequence %d", state.LastSeq)
		}
		return nil
	})
	require_Equal(t, state.Msgs, 10)
	require_Equal(t, state.FirstSeq, 1)
	require_Equal(t, state.LastSeq, finalLastSeq)
	require_Len(t, len(state.Deleted), 2)
	require_Equal(t, state.Deleted[0], snapshotLastSeq-1)
	require_Equal(t, state.Deleted[1], snapshotLastSeq)
	require_True(t, mset.isMonitorRunning())
}

func TestJetStreamClusterAsyncFlushFileStoreFlushOnSnapshot(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "R3S", 3)
	defer c.shutdown()

	nc, js := jsClientConnect(t, c.randomServer())
	defer nc.Close()

	_, err := jsStreamCreate(t, nc, &StreamConfig{
		Name:     "TEST",
		Subjects: []string{"foo"},
		Storage:  FileStorage,
		Replicas: 3,
	})
	require_NoError(t, err)

	// Now shutdown flusher and wait for it to be closed,
	// we'll not flush published messages automatically anymore.
	sl := c.streamLeader(globalAccountName, "TEST")
	mset, err := sl.globalAccount().lookupStream("TEST")
	require_NoError(t, err)
	fs := mset.Store().(*fileStore)
	fs.mu.RLock()
	lmb := fs.lmb
	fs.mu.RUnlock()
	require_NotNil(t, lmb)

	lmb.mu.Lock()
	if lmb.qch != nil {
		close(lmb.qch)
		lmb.qch = nil
	}
	lmb.mu.Unlock()
	checkFor(t, 2*time.Second, 200*time.Millisecond, func() error {
		lmb.mu.RLock()
		defer lmb.mu.RUnlock()
		if lmb.flusher {
			return errors.New("flusher still active")
		}
		return nil
	})

	// Get the highest applied count from snapshot before below publish.
	n := mset.raftNode().(*raft)
	_, _, previousApplied := n.Progress()

	// An initial snapshot is installed shortly after the stream is created. Wait for it
	// and remove it again, since the leader change below only snapshots if one is needed.
	checkFor(t, 5*time.Second, 200*time.Millisecond, func() error {
		n.Lock()
		defer n.Unlock()
		if n.snapfile == _EMPTY_ {
			return errors.New("no initial snapshot yet")
		}
		os.Remove(n.snapfile)
		n.snapfile = _EMPTY_
		return nil
	})

	// Confirm no pending writes.
	require_Equal(t, lmb.pendingWriteSize(), 0)

	// Publishing a message will still work, because writes are async.
	_, err = js.Publish("foo", nil)
	require_NoError(t, err)
	checkFor(t, 2*time.Second, 200*time.Millisecond, func() error {
		return checkState(t, c, globalAccountName, "TEST")
	})

	// Confirm above write is pending.
	require_Equal(t, lmb.pendingWriteSize(), 33)

	// Make the upper layer snapshot by sending leader change signal.
	// It doesn't matter that we're already leader, it still gets handled.
	// Previously this used the mqch, but that now only snapshots on shutdown.
	n.leadc <- leadChange{isLeader: true, term: n.Term()}

	checkFor(t, 2*time.Second, 200*time.Millisecond, func() error {
		n.Lock()
		snap, err := n.loadLastSnapshot()
		n.Unlock()
		if err != nil {
			return err
		}
		if snap.lastIndex <= previousApplied {
			return fmt.Errorf("snapshot still at lastIndex=%d, expected=%d", snap.lastIndex, previousApplied)
		}
		return nil
	})

	// Confirm the write is flushed as a result of the snapshot.
	require_Equal(t, lmb.pendingWriteSize(), 0)
}

func TestJetStreamClusterScheduledDelayedMessage(t *testing.T) {
	for _, replicas := range []int{1, 3} {
		for _, storage := range []StorageType{FileStorage, MemoryStorage} {
			t.Run(fmt.Sprintf("R%d/%s", replicas, storage), func(t *testing.T) {
				c := createJetStreamClusterExplicit(t, "R3S", 3)
				defer c.shutdown()

				nc, js := jsClientConnect(t, c.randomServer())
				defer nc.Close()

				_, err := jsStreamCreate(t, nc, &StreamConfig{
					Name:              "SchedulesDisabled",
					Subjects:          []string{"disabled"},
					Storage:           storage,
					Replicas:          replicas,
					AllowMsgSchedules: false,
				})
				require_NoError(t, err)

				schedulePattern := "@at 1970-01-01T00:00:00Z"
				m := nats.NewMsg("disabled")
				m.Header.Set("Nats-Schedule", schedulePattern) // Needs to be valid, but is not used.
				_, err = js.PublishMsg(m)
				require_Error(t, err, NewJSMessageSchedulesDisabledError())

				m = nats.NewMsg("disabled")
				m.Header.Set("Nats-Schedule", "disabled")
				_, err = js.PublishMsg(m)
				require_Error(t, err, NewJSMessageSchedulesDisabledError())

				_, err = jsStreamCreate(t, nc, &StreamConfig{
					Name:              "SchedulesEnabledNoTtl",
					Subjects:          []string{"disabled.ttl"},
					Storage:           storage,
					Replicas:          replicas,
					AllowMsgSchedules: true,
				})
				require_NoError(t, err)

				m = nats.NewMsg("disabled.ttl")
				m.Header.Set("Nats-Schedule", schedulePattern) // Needs to be valid, but is not used.
				m.Header.Set("Nats-Schedule-TTL", "1s")
				_, err = js.PublishMsg(m)
				require_Error(t, err, NewJSMessageTTLDisabledError())

				cfg := &StreamConfig{
					Name:              "SchedulesEnabled",
					Subjects:          []string{"foo.*"},
					Storage:           storage,
					Replicas:          replicas,
					AllowMsgSchedules: true,
					AllowMsgTTL:       true,
				}
				_, err = jsStreamCreate(t, nc, cfg)
				require_NoError(t, err)

				m = nats.NewMsg("foo.invalid")
				m.Header.Set("Nats-Schedule", "invalid")
				_, err = js.PublishMsg(m)
				require_Error(t, err, NewJSMessageSchedulesPatternInvalidError())

				// Unknown time zone must be reported as time-zone-invalid, not pattern-invalid.
				m = nats.NewMsg("foo.invalid")
				m.Header.Set("Nats-Schedule", "0 * * * * *")
				m.Header.Set("Nats-Schedule-Time-Zone", "Not/A/Zone")
				_, err = js.PublishMsg(m)
				require_Error(t, err, NewJSMessageSchedulesTimeZoneInvalidError())

				// An empty time zone is also invalid.
				m.Header.Set("Nats-Schedule-Time-Zone", "")
				_, err = js.PublishMsg(m)
				require_Error(t, err, NewJSMessageSchedulesTimeZoneInvalidError())

				// A valid time zone with an invalid pattern still surfaces as pattern-invalid.
				m = nats.NewMsg("foo.invalid")
				m.Header.Set("Nats-Schedule", "invalid")
				m.Header.Set("Nats-Schedule-Time-Zone", "UTC")
				_, err = js.PublishMsg(m)
				require_Error(t, err, NewJSMessageSchedulesPatternInvalidError())

				m = nats.NewMsg("foo.invalid")
				m.Header.Set("Nats-Schedule", schedulePattern)
				m.Header.Set("Nats-Schedule-Target", "not.matching")
				_, err = js.PublishMsg(m)
				require_Error(t, err, NewJSMessageSchedulesTargetInvalidError())

				m = nats.NewMsg("foo.invalid")
				m.Header.Set("Nats-Schedule", schedulePattern)
				m.Header.Set("Nats-Schedule-Target", "foo.*") // Must be literal.
				_, err = js.PublishMsg(m)
				require_Error(t, err, NewJSMessageSchedulesTargetInvalidError())

				m = nats.NewMsg("foo.invalid")
				m.Header.Set("Nats-Schedule", schedulePattern)
				m.Header.Set("Nats-Schedule-Target", "foo.invalid") // Can't equal the publish subject.
				_, err = js.PublishMsg(m)
				require_Error(t, err, NewJSMessageSchedulesTargetInvalidError())

				m = nats.NewMsg("foo.invalid")
				m.Header.Set("Nats-Schedule", schedulePattern)
				m.Header.Set("Nats-Schedule-Target", "foo.publish")
				m.Header.Set("Nats-Schedule-TTL", "invalid")
				_, err = js.PublishMsg(m)
				require_Error(t, err, NewJSMessageSchedulesTTLInvalidError())

				m = nats.NewMsg("foo.invalid")
				m.Header.Set("Nats-Schedule", schedulePattern)
				m.Header.Set("Nats-Schedule-Target", "foo.publish")
				m.Header.Set("Nats-Schedule-TTL", "1s")
				m.Header.Set("Nats-Rollup", "all")
				_, err = js.PublishMsg(m)
				require_Error(t, err, NewJSMessageSchedulesRollupInvalidError())

				// Schedule with a delay that takes a very long time.
				schedule := time.Now().Add(time.Hour).Format(time.RFC3339Nano)
				schedulePattern = fmt.Sprintf("@at %s", schedule)
				m = nats.NewMsg("foo.schedule")
				m.Header.Set("Nats-Schedule", schedulePattern)
				m.Header.Set("Nats-Schedule-Target", "foo.publish")
				m.Header.Set("Nats-Schedule-TTL", "1s")
				m.Header.Set("Nats-Rollup", "sub")
				pubAck, err := js.PublishMsg(m)
				require_NoError(t, err)
				require_Equal(t, pubAck.Sequence, 1)

				// Schedule where the rollup is missing, automatically gets the rollup added.
				// Also, will rollup the previous message for this schedule.
				schedule = time.Now().Add(time.Second).Format(time.RFC3339Nano)
				schedulePattern = fmt.Sprintf("@at %s", schedule)
				m = nats.NewMsg("foo.schedule")
				m.Data = []byte("hello")
				m.Header.Set("Nats-Schedule", schedulePattern)
				m.Header.Set("Nats-Schedule-Target", "foo.publish")
				m.Header.Set("Nats-Schedule-TTL", "1s")
				// Add a bunch of headers that will be stripped on the scheduled message.
				m.Header.Set("Nats-Expected-Stream", "SchedulesEnabled")
				m.Header.Set("Nats-Expected-Last-Sequence", "1")
				m.Header.Set("Nats-Expected-Last-Subject-Sequence", "1")
				m.Header.Set("Nats-Msg-Id", "X")
				m.Header.Set("Nats-TTL", "60s")
				m.Header.Set("Header", "Value")
				pubAck, err = js.PublishMsg(m)
				require_NoError(t, err)
				require_Equal(t, pubAck.Sequence, 2)

				sl := c.streamLeader(globalAccountName, "SchedulesEnabled")
				mset, err := sl.globalAccount().lookupStream("SchedulesEnabled")
				require_NoError(t, err)

				// Only one schedule exists.
				state := mset.state()
				require_Equal(t, state.LastSeq, 2)
				require_Equal(t, state.Msgs, 1)

				// Waiting for the delayed message to be published.
				checkFor(t, 2*time.Second, 200*time.Millisecond, func() error {
					state = mset.state()
					if state.LastSeq != 3 {
						return fmt.Errorf("expected last seq 3, got %d", state.LastSeq)
					} else if state.Msgs != 1 {
						return fmt.Errorf("expected 1 msg, got %d", state.Msgs)
					}
					return nil
				})

				// Confirm the scheduled message has the correct data.
				rsm, err := js.GetLastMsg("SchedulesEnabled", "foo.publish")
				require_NoError(t, err)
				require_Equal(t, rsm.Sequence, 3)
				require_True(t, bytes.Equal(rsm.Data, []byte("hello")))
				require_Len(t, len(rsm.Header), 4)
				require_Equal(t, rsm.Header.Get("Nats-Scheduler"), "foo.schedule")
				require_Equal(t, rsm.Header.Get("Nats-Schedule-Next"), "purge")
				require_Equal(t, rsm.Header.Get("Nats-TTL"), "1s")
				require_Equal(t, rsm.Header.Get("Header"), "Value")

				// Waiting for the delayed message to age out due to its TTL.
				checkFor(t, 2*time.Second, 200*time.Millisecond, func() error {
					state = mset.state()
					if state.FirstSeq != 4 {
						return fmt.Errorf("expected first seq 4, got %d", state.FirstSeq)
					} else if state.LastSeq != 3 {
						return fmt.Errorf("expected last seq 3, got %d", state.LastSeq)
					} else if state.Msgs != 0 {
						return fmt.Errorf("expected no messages, got %d", state.Msgs)
					}
					return nil
				})

				// Servers should be synced.
				checkFor(t, 2*time.Second, 200*time.Millisecond, func() error {
					return checkState(t, c, globalAccountName, "SchedulesEnabled")
				})

				cfg.AllowMsgSchedules = false
				_, err = jsStreamUpdate(t, nc, cfg)
				require_Error(t, err, NewJSStreamInvalidConfigError(fmt.Errorf("message schedules can not be disabled")))
			})
		}
	}
}

func TestJetStreamClusterScheduledMessageSubjectSourcing(t *testing.T) {
	for _, replicas := range []int{1, 3} {
		for _, storage := range []StorageType{FileStorage, MemoryStorage} {
			t.Run(fmt.Sprintf("R%d/%s", replicas, storage), func(t *testing.T) {
				c := createJetStreamClusterExplicit(t, "R3S", 3)
				defer c.shutdown()

				nc, js := jsClientConnect(t, c.randomServer())
				defer nc.Close()

				cfg := &StreamConfig{
					Name:              "SchedulesEnabled",
					Subjects:          []string{"foo.*"},
					Storage:           storage,
					Replicas:          replicas,
					AllowMsgSchedules: true,
					AllowMsgTTL:       true,
				}
				_, err := jsStreamCreate(t, nc, cfg)
				require_NoError(t, err)

				m := nats.NewMsg("foo.data")
				m.Header.Set("Header", "Value")
				m.Data = []byte("data")

				pubAck, err := js.PublishMsg(m)
				require_NoError(t, err)
				require_Equal(t, pubAck.Sequence, 1)

				m = nats.NewMsg("foo.schedule")
				m.Header.Set("Nats-Schedule", "@at 1970-01-01T00:00:00Z")
				m.Header.Set("Nats-Schedule-Target", "foo.publish")

				// Invalid sources include if the subject:
				// - matches the schedule/target subject
				// - contains wildcard/is not literal
				// - doesn't match the stream subjects
				for _, src := range []string{"foo.schedule", "foo.publish", "foo.*", "foo.>", "bar"} {
					m.Header.Set("Nats-Schedule-Source", src)
					_, err = js.PublishMsg(m)
					require_Error(t, err, NewJSMessageSchedulesSourceInvalidError())
				}

				// Now publish using a correct source subject.
				m.Header.Set("Nats-Schedule-Source", "foo.data")
				pubAck, err = js.PublishMsg(m)
				require_NoError(t, err)
				require_Equal(t, pubAck.Sequence, 2)

				sl := c.streamLeader(globalAccountName, "SchedulesEnabled")
				mset, err := sl.globalAccount().lookupStream("SchedulesEnabled")
				require_NoError(t, err)

				state := mset.state()
				require_Equal(t, state.LastSeq, 2)
				require_Equal(t, state.Msgs, 2)

				// Waiting for the delayed message to be published.
				checkFor(t, 2*time.Second, 200*time.Millisecond, func() error {
					state = mset.state()
					if state.LastSeq != 3 {
						return fmt.Errorf("expected last seq 3, got %d", state.LastSeq)
					} else if state.Msgs != 2 {
						// One is the scheduled message, one is the sourced message.
						return fmt.Errorf("expected 2 msgs, got %d", state.Msgs)
					}
					return nil
				})

				// Confirm the scheduled message has the correct data.
				rsm, err := js.GetLastMsg("SchedulesEnabled", "foo.publish")
				require_NoError(t, err)
				require_Equal(t, rsm.Sequence, 3)
				require_True(t, bytes.Equal(rsm.Data, []byte("data")))
				require_Len(t, len(rsm.Header), 3)
				require_Equal(t, rsm.Header.Get("Nats-Scheduler"), "foo.schedule")
				require_Equal(t, rsm.Header.Get("Nats-Schedule-Next"), "purge")
				require_Equal(t, rsm.Header.Get("Header"), "Value")

				// Servers should be synced.
				checkFor(t, 2*time.Second, 200*time.Millisecond, func() error {
					return checkState(t, c, globalAccountName, "SchedulesEnabled")
				})
			})
		}
	}
}

func TestJetStreamClusterScheduledMessageSubjectSourcingFallback(t *testing.T) {
	for _, replicas := range []int{1, 3} {
		for _, storage := range []StorageType{FileStorage, MemoryStorage} {
			t.Run(fmt.Sprintf("R%d/%s", replicas, storage), func(t *testing.T) {
				c := createJetStreamClusterExplicit(t, "R3S", 3)
				defer c.shutdown()

				nc, js := jsClientConnect(t, c.randomServer())
				defer nc.Close()

				cfg := &StreamConfig{
					Name:              "SchedulesEnabled",
					Subjects:          []string{"foo.*"},
					Storage:           storage,
					Replicas:          replicas,
					AllowMsgSchedules: true,
					AllowMsgTTL:       true,
				}
				_, err := jsStreamCreate(t, nc, cfg)
				require_NoError(t, err)

				// Publish the schedule WITHOUT first publishing to the source subject.
				// The schedule should fall back to its own content when the source has no last message.
				m := nats.NewMsg("foo.schedule")
				m.Header.Set("Nats-Schedule", "@at 1970-01-01T00:00:00Z")
				m.Header.Set("Nats-Schedule-Target", "foo.publish")
				m.Header.Set("Nats-Schedule-Source", "foo.data")
				m.Header.Set("Header", "Fallback")
				m.Data = []byte("fallback")

				pubAck, err := js.PublishMsg(m)
				require_NoError(t, err)
				require_Equal(t, pubAck.Sequence, 1)

				sl := c.streamLeader(globalAccountName, "SchedulesEnabled")
				mset, err := sl.globalAccount().lookupStream("SchedulesEnabled")
				require_NoError(t, err)

				state := mset.state()
				require_Equal(t, state.LastSeq, 1)
				require_Equal(t, state.Msgs, 1)

				// Waiting for the delayed message to be published using the fallback content.
				checkFor(t, 2*time.Second, 200*time.Millisecond, func() error {
					state = mset.state()
					if state.LastSeq != 2 {
						return fmt.Errorf("expected last seq 2, got %d", state.LastSeq)
					} else if state.Msgs != 1 {
						// Only the published message remains; the schedule itself is purged.
						return fmt.Errorf("expected 1 msg, got %d", state.Msgs)
					}
					return nil
				})

				// Confirm the published message contains the scheduled message's own content.
				rsm, err := js.GetLastMsg("SchedulesEnabled", "foo.publish")
				require_NoError(t, err)
				require_Equal(t, rsm.Sequence, 2)
				require_True(t, bytes.Equal(rsm.Data, []byte("fallback")))
				require_Len(t, len(rsm.Header), 3)
				require_Equal(t, rsm.Header.Get("Nats-Scheduler"), "foo.schedule")
				require_Equal(t, rsm.Header.Get("Nats-Schedule-Next"), "purge")
				require_Equal(t, rsm.Header.Get("Header"), "Fallback")
				checkFor(t, 2*time.Second, 200*time.Millisecond, func() error {
					return checkState(t, c, globalAccountName, "SchedulesEnabled")
				})
			})
		}
	}
}

func TestJetStreamClusterScheduledMessageSubjectSourcingRepeatingNotStuck(t *testing.T) {
	for _, replicas := range []int{1, 3} {
		for _, storage := range []StorageType{FileStorage, MemoryStorage} {
			t.Run(fmt.Sprintf("R%d/%s", replicas, storage), func(t *testing.T) {
				c := createJetStreamClusterExplicit(t, "R3S", 3)
				defer c.shutdown()

				nc, js := jsClientConnect(t, c.randomServer())
				defer nc.Close()

				cfg := &StreamConfig{
					Name:              "SchedulesEnabled",
					Subjects:          []string{"foo.*"},
					Storage:           storage,
					Replicas:          replicas,
					AllowMsgSchedules: true,
				}
				_, err := jsStreamCreate(t, nc, cfg)
				require_NoError(t, err)

				// Seed foo.data with a message that carries a stale Nats-Scheduler header.
				m := nats.NewMsg("foo.data")
				m.Header.Set("Nats-Schedule-Next", "purge")
				m.Header.Set("Nats-Scheduler", "foo.victim")
				_, err = js.PublishMsg(m)
				require_NoError(t, err)

				// Repeating schedule sourcing from foo.data. If the Nats-Scheduler is not
				// stripped from the source, the scheduled output carries two Nats-Scheduler
				// headers and the store's schedule update path targets the wrong scheduler
				// subject. Resulting in the scheduling getting stuck.
				m = nats.NewMsg("foo.sb")
				m.Header.Set("Nats-Schedule", "@every 1s")
				m.Header.Set("Nats-Schedule-Target", "foo.publish")
				m.Header.Set("Nats-Schedule-Source", "foo.data")
				_, err = js.PublishMsg(m)
				require_NoError(t, err)

				// Wait until the schedule has fired multiple times. With the bug it fires
				// once and then get stuck, without it it keeps firing on its @every 1s cadence.
				checkFor(t, 5*time.Second, 200*time.Millisecond, func() error {
					mi, err := js.StreamInfo("SchedulesEnabled", &nats.StreamInfoRequest{SubjectsFilter: "foo.publish"})
					if err != nil {
						return err
					}
					if n := mi.State.Subjects["foo.publish"]; n < 3 {
						return fmt.Errorf("expected at least 3 messages on foo.publish, got %d", n)
					}
					return nil
				})

				// The schedule message itself must still be present.
				rsm, err := js.GetLastMsg("SchedulesEnabled", "foo.sb")
				require_NoError(t, err)
				require_Equal(t, rsm.Header.Get("Nats-Schedule"), "@every 1s")
				checkFor(t, 2*time.Second, 200*time.Millisecond, func() error {
					return checkState(t, c, globalAccountName, "SchedulesEnabled")
				})
			})
		}
	}
}

func TestJetStreamClusterScheduledDelayedMessageRollup(t *testing.T) {
	for _, replicas := range []int{1, 3} {
		for _, storage := range []StorageType{FileStorage, MemoryStorage} {
			t.Run(fmt.Sprintf("R%d/%s", replicas, storage), func(t *testing.T) {
				c := createJetStreamClusterExplicit(t, "R3S", 3)
				defer c.shutdown()

				nc, js := jsClientConnect(t, c.randomServer())
				defer nc.Close()

				cfg := &StreamConfig{
					Name:              "SchedulesEnabled",
					Subjects:          []string{"foo.*"},
					Storage:           storage,
					Replicas:          replicas,
					AllowMsgSchedules: true,
				}
				_, err := jsStreamCreate(t, nc, cfg)
				require_NoError(t, err)

				_, err = js.Publish("foo.one", []byte("one"))
				require_NoError(t, err)
				_, err = js.Publish("foo.publish", []byte("previous"))
				require_NoError(t, err)

				// Schedule with an invalid rollup
				schedule := time.Now().Add(time.Second).Format(time.RFC3339Nano)
				schedulePattern := fmt.Sprintf("@at %s", schedule)
				m := nats.NewMsg("foo.schedule")
				m.Header.Set("Nats-Schedule", schedulePattern)
				m.Header.Set("Nats-Schedule-Target", "foo.publish")
				m.Header.Set("Nats-Schedule-Source", "foo.one")
				m.Header.Set("Nats-Schedule-Rollup", "invalid")
				_, err = js.PublishMsg(m)
				require_Error(t, err, NewJSMessageSchedulesRollupInvalidError())

				// Now send with the correct rollup.
				m.Header.Set("Nats-Schedule-Rollup", "sub")
				pubAck, err := js.PublishMsg(m)
				require_NoError(t, err)
				require_Equal(t, pubAck.Sequence, 3)

				sl := c.streamLeader(globalAccountName, "SchedulesEnabled")
				mset, err := sl.globalAccount().lookupStream("SchedulesEnabled")
				require_NoError(t, err)

				// Both the schedule and the previous messages exist.
				state := mset.state()
				require_Equal(t, state.LastSeq, 3)
				require_Equal(t, state.Msgs, 3)

				// Waiting for the delayed message to be published, and rollup the previous message.
				sawPrevious := false
				checkFor(t, 2*time.Second, 200*time.Millisecond, func() error {
					sm, _, err := mset.store.LoadNextMsg("foo.publish", false, 0, nil)
					if err != nil {
						return err
					}
					if bytes.Equal(sm.msg, []byte("previous")) {
						sawPrevious = true
						return errors.New("still previous")
					}
					if !sawPrevious {
						return errors.New("expected previous message to be rolled up")
					}

					state = mset.state()
					if state.LastSeq != 4 {
						return fmt.Errorf("expected last seq 4, got %d", state.LastSeq)
					} else if state.Msgs != 2 {
						return fmt.Errorf("expected 2 msgs, got %d", state.Msgs)
					}
					return nil
				})

				// Confirm the scheduled message has the correct data.
				rsm, err := js.GetLastMsg("SchedulesEnabled", "foo.publish")
				require_NoError(t, err)
				require_Equal(t, rsm.Sequence, 4)
				require_Equal(t, string(rsm.Data), "one")
				require_Len(t, len(rsm.Header), 3)
				require_Equal(t, rsm.Header.Get("Nats-Scheduler"), "foo.schedule")
				require_Equal(t, rsm.Header.Get("Nats-Schedule-Next"), "purge")
				require_Equal(t, rsm.Header.Get("Nats-Rollup"), "sub")

				// Servers should be synced.
				checkFor(t, 2*time.Second, 200*time.Millisecond, func() error {
					return checkState(t, c, globalAccountName, "SchedulesEnabled")
				})
			})
		}
	}
}

func TestJetStreamClusterPurgeScheduleWithInvalidScheduler(t *testing.T) {
	for _, replicas := range []int{1, 3} {
		t.Run(fmt.Sprintf("R%d", replicas), func(t *testing.T) {
			c := createJetStreamClusterExplicit(t, "R3S", 3)
			defer c.shutdown()

			nc, js := jsClientConnect(t, c.randomServer())
			defer nc.Close()

			cfg := &StreamConfig{
				Name:              "SchedulesEnabled",
				Subjects:          []string{"foo.*"},
				Storage:           FileStorage,
				Replicas:          replicas,
				AllowMsgSchedules: true,
			}
			_, err := jsStreamCreate(t, nc, cfg)
			require_NoError(t, err)

			// Publish a normal non-schedule message.
			_, err = js.Publish("foo.normal", []byte("previous"))
			require_NoError(t, err)

			// Publish a schedule message.
			m := nats.NewMsg("foo.schedule")
			m.Header.Set("Nats-Schedule", "@every 1h")
			m.Header.Set("Nats-Schedule-Target", "foo.target")
			m.Data = []byte("previous")
			_, err = js.PublishMsg(m)
			require_NoError(t, err)

			m = nats.NewMsg("foo.schedule")
			m.Header.Set("Nats-Schedule-Next", "purge")
			// Missing scheduler
			_, err = js.PublishMsg(m)
			require_Error(t, err, NewJSMessageSchedulesSchedulerInvalidError())

			// Scheduler must be a literal subject
			m.Header.Set("Nats-Scheduler", "foo.*")
			_, err = js.PublishMsg(m)
			require_Error(t, err, NewJSMessageSchedulesSchedulerInvalidError())

			// Scheduler can't equal the publish subject.
			m.Header.Set("Nats-Scheduler", "foo.schedule")
			_, err = js.PublishMsg(m)
			require_Error(t, err, NewJSMessageSchedulesSchedulerInvalidError())

			// Should error if the scheduler message is not actually a schedule.
			// Otherwise, non-schedule messages could be purged this way.
			m.Header.Set("Nats-Scheduler", "foo.normal")
			m.Subject = "foo.last"
			_, err = js.PublishMsg(m)
			require_Error(t, err, NewJSMessageSchedulesSchedulerInvalidError())

			// We're allowed to purge a schedule, as long as the subject we're
			// publishing to doesn't equal and it is a schedule.
			m.Header.Set("Nats-Scheduler", "foo.schedule")
			m.Subject = "foo.last"
			_, err = js.PublishMsg(m)
			require_NoError(t, err)

			// Purge the normal message separately.
			require_NoError(t, js.PurgeStream("SchedulesEnabled", &nats.StreamPurgeRequest{Subject: "foo.normal"}))

			// Only the last message should exist.
			checkFor(t, 2*time.Second, 200*time.Millisecond, func() error {
				state, err := checkStateAndErr(t, c, globalAccountName, "SchedulesEnabled")
				if err != nil {
					return err
				}
				if state.Msgs != 1 || state.FirstSeq != 3 || state.LastSeq != 3 {
					return fmt.Errorf("expected 1 msg, got %v", state)
				}
				return nil
			})

			// If the scheduler message does not exist, we should preserve backward-compatibility
			// and allow the message to be persisted with a noop purge.
			m.Header.Set("Nats-Scheduler", "foo.none")
			m.Subject = "foo.last"
			_, err = js.PublishMsg(m)
			require_NoError(t, err)

			// Should still be able to perform an "expected at sequence" check to make sure the
			// schedule actually exists before persisting the message and performing a purge.
			m.Header.Set("Nats-Expected-Last-Subject-Sequence", "1")
			m.Header.Set("Nats-Expected-Last-Subject-Sequence-Subject", "foo.none")
			_, err = js.PublishMsg(m)
			require_Error(t, err, NewJSStreamWrongLastSequenceError(0))
		})
	}
}

func TestJetStreamClusterScheduledPurgeHeadersRequiresSchedulingEnabled(t *testing.T) {
	for _, replicas := range []int{1, 3} {
		t.Run(fmt.Sprintf("R%d", replicas), func(t *testing.T) {
			c := createJetStreamClusterExplicit(t, "R3S", 3)
			defer c.shutdown()

			nc, js := jsClientConnect(t, c.randomServer())
			defer nc.Close()

			_, err := jsStreamCreate(t, nc, &StreamConfig{
				Name:              "TEST",
				Storage:           FileStorage,
				Subjects:          []string{"foo.*"},
				Replicas:          replicas,
				AllowMsgSchedules: false,
			})
			require_NoError(t, err)

			_, err = js.Publish("foo.victim", []byte("OK"))
			require_NoError(t, err)

			m := nats.NewMsg("foo.attacker")
			m.Header.Set(JSScheduleNext, JSScheduleNextPurge)
			m.Header.Set(JSScheduler, "foo.victim")

			_, err = js.PublishMsg(m)
			require_Error(t, err, NewJSMessageSchedulesDisabledError())
		})
	}
}

func TestJetStreamClusterScheduledRearmHeaderRejectedFromClient(t *testing.T) {
	for _, replicas := range []int{1, 3} {
		for _, storage := range []StorageType{FileStorage, MemoryStorage} {
			t.Run(fmt.Sprintf("R%d/%s", replicas, storage), func(t *testing.T) {
				c := createJetStreamClusterExplicit(t, "R3S", 3)
				defer c.shutdown()

				nc, js := jsClientConnect(t, c.randomServer())
				defer nc.Close()

				_, err := jsStreamCreate(t, nc, &StreamConfig{
					Name:              "TEST",
					Storage:           storage,
					Subjects:          []string{"foo.>"},
					Replicas:          replicas,
					AllowMsgSchedules: true,
				})
				require_NoError(t, err)

				// Arm a schedule that fires shortly.
				fireIn := 500 * time.Millisecond
				m := nats.NewMsg("foo.schedule")
				m.Header.Set("Nats-Schedule", fmt.Sprintf("@at %s", time.Now().Add(fireIn).Format(time.RFC3339Nano)))
				m.Header.Set("Nats-Schedule-Target", "foo.msg")
				_, err = js.PublishMsg(m)
				require_NoError(t, err)

				// An attacker with publish access should not be able to shift
				// the schedule's next fire time by forging Nats-Schedule-Next.
				attack := nats.NewMsg("foo.attacker")
				attack.Header.Set(JSScheduleNext, time.Now().Add(100*365*24*time.Hour).Format(time.RFC3339Nano))
				attack.Header.Set(JSScheduler, "foo.schedule")
				_, err = js.PublishMsg(attack)
				require_Error(t, err, NewJSMessageSchedulesSchedulerInvalidError())

				attack.Header.Del(JSScheduleNext)
				_, err = js.PublishMsg(attack)
				require_Error(t, err, NewJSMessageSchedulesSchedulerInvalidError())

				// The schedule must still fire on time.
				checkFor(t, 2*time.Second, 50*time.Millisecond, func() error {
					if _, err = js.GetLastMsg("TEST", "foo.msg"); err != nil {
						return err
					}
					return nil
				})
			})
		}
	}
}

func TestJetStreamClusterScheduledDelayedMessageReversedHeaderOrder(t *testing.T) {
	for _, replicas := range []int{1, 3} {
		for _, storage := range []StorageType{FileStorage, MemoryStorage} {
			t.Run(fmt.Sprintf("R%d/%s", replicas, storage), func(t *testing.T) {
				c := createJetStreamClusterExplicit(t, "R3S", 3)
				defer c.shutdown()

				nc, _ := jsClientConnect(t, c.randomServer())
				defer nc.Close()

				cfg := &StreamConfig{
					Name:              "TEST",
					Subjects:          []string{"foo.*"},
					Storage:           storage,
					Replicas:          replicas,
					AllowMsgSchedules: true,
					AllowMsgTTL:       true,
				}
				_, err := jsStreamCreate(t, nc, cfg)
				require_NoError(t, err)

				sl := c.streamLeader(globalAccountName, "TEST")
				gacc := sl.globalAccount()
				// The Nats-Schedule headers share a common prefix, so make sure if these are ordered differently
				// we can still properly schedule a message.
				hdr := genHeader(nil, "Nats-Schedule-Target", "foo.publish")
				hdr = genHeader(hdr, "Nats-Schedule", "@at 1970-01-01T00:00:00Z")
				require_NoError(t, sl.sendInternalAccountMsgWithReply(gacc, "foo.schedule", _EMPTY_, hdr, nil, false))

				mset, err := gacc.lookupStream("TEST")
				require_NoError(t, err)

				// Waiting for the delayed message to be published.
				checkFor(t, 2*time.Second, 200*time.Millisecond, func() error {
					state := mset.state()
					if state.LastSeq != 2 {
						return fmt.Errorf("expected last seq 2, got %d", state.LastSeq)
					} else if state.Msgs != 1 {
						return fmt.Errorf("expected 1 msg, got %d", state.Msgs)
					}
					return nil
				})
			})
		}
	}
}

func TestJetStreamClusterScheduledIntervalMessage(t *testing.T) {
	for _, replicas := range []int{1, 3} {
		for _, storage := range []StorageType{FileStorage, MemoryStorage} {
			t.Run(fmt.Sprintf("R%d/%s", replicas, storage), func(t *testing.T) {
				c := createJetStreamClusterExplicit(t, "R3S", 3)
				defer c.shutdown()

				nc, js := jsClientConnect(t, c.randomServer())
				defer nc.Close()

				cfg := &StreamConfig{
					Name:              "SchedulesEnabled",
					Subjects:          []string{"foo.*"},
					Storage:           storage,
					Replicas:          replicas,
					AllowMsgSchedules: true,
				}
				_, err := jsStreamCreate(t, nc, cfg)
				require_NoError(t, err)

				// Schedule with an interval that triggers often.
				m := nats.NewMsg("foo.schedule")
				m.Header.Set("Nats-Schedule", "@every 1s")
				m.Header.Set("Nats-Schedule-Target", "foo.publish")
				pubAck, err := js.PublishMsg(m)
				require_NoError(t, err)
				require_Equal(t, pubAck.Sequence, 1)

				sl := c.streamLeader(globalAccountName, "SchedulesEnabled")
				mset, err := sl.globalAccount().lookupStream("SchedulesEnabled")
				require_NoError(t, err)

				// Waiting for the repeated message to be published enough times.
				checkFor(t, 4*time.Second, 200*time.Millisecond, func() error {
					total := mset.Store().SubjectsTotals("foo.publish")["foo.publish"]
					if total < 3 {
						return fmt.Errorf("expected at least 3 messages, got %d", total)
					}
					return nil
				})

				// Servers should be synced.
				checkFor(t, 2*time.Second, 200*time.Millisecond, func() error {
					return checkState(t, c, globalAccountName, "SchedulesEnabled")
				})

				// If we remove the schedule, the scheduling should stop.
				var removed bool
				total := mset.Store().SubjectsTotals("foo.publish")["foo.publish"]
				for i := 1; ; i++ {
					before := total
					time.Sleep(1200 * time.Millisecond)
					total = mset.Store().SubjectsTotals("foo.publish")["foo.publish"]
					if before == total {
						// If the scheduling stopped before we removed, that's a problem.
						require_True(t, removed)
						break
					}
					if !removed {
						removed = true
						require_NoError(t, js.DeleteMsg("SchedulesEnabled", pubAck.Sequence))
					}
					if i > 2 {
						t.Fatal("Scheduling didn't stop")
					}
				}

				// Servers should be synced.
				checkFor(t, 2*time.Second, 200*time.Millisecond, func() error {
					return checkState(t, c, globalAccountName, "SchedulesEnabled")
				})
			})
		}
	}
}

func TestJetStreamClusterOfflineStreamAndConsumerAfterAssetCreateOrUpdate(t *testing.T) {
	clusterName := "R3S"
	c := createJetStreamClusterExplicit(t, clusterName, 3)
	defer c.shutdown()

	nc, js := jsClientConnect(t, c.randomServer())
	defer nc.Close()

	ml := c.leader()
	require_NotNil(t, ml)

	sjs := ml.getJetStream()
	require_NotNil(t, sjs)
	sjs.mu.Lock()
	cc := sjs.cluster
	if cc == nil || cc.meta == nil {
		sjs.mu.Unlock()
		t.Fatalf("Expected cluster to be initialized")
	}

	restart := func() {
		t.Helper()
		for _, s := range c.servers {
			sjs = s.getJetStream()
			snap, _, _, err := sjs.metaSnapshot()
			require_NoError(t, err)
			meta := sjs.getMetaGroup()
			meta.InstallSnapshot(snap, false)
		}

		c.stopAll()
		c.restartAllSamePorts()
		c.waitOnLeader()
		ml = c.leader()
		require_NotNil(t, ml)
		require_NoError(t, nc.ForceReconnect())

		sjs = ml.getJetStream()
		require_NotNil(t, sjs)
		sjs.mu.Lock()
		cc = sjs.cluster
		if cc == nil || cc.meta == nil {
			sjs.mu.Unlock()
			t.Fatalf("Expected cluster to be initialized")
		}
		sjs.mu.Unlock()
	}

	getValidMetaSnapshot := func() (wsas []writeableStreamAssignment) {
		t.Helper()
		snap, _, _, err := sjs.metaSnapshot()
		require_NoError(t, err)
		require_True(t, len(snap) > 0)
		dec, err := s2.Decode(nil, snap)
		require_NoError(t, err)
		require_NoError(t, json.Unmarshal(dec, &wsas))
		return wsas
	}

	// Create a stream that's unsupported.
	ci := &ClientInfo{
		Account: globalAccountName,
		Cluster: clusterName,
	}
	scfg := &StreamConfig{
		Name:     "DowngradeStreamTest",
		Storage:  FileStorage,
		Replicas: 3,
		Metadata: map[string]string{"_nats.req.level": strconv.Itoa(math.MaxInt - 1)},
	}
	rg, perr := sjs.createGroupForStream(ci, scfg)
	if perr != nil {
		sjs.mu.Unlock()
		require_NoError(t, perr)
	}
	sa := &streamAssignment{
		Config:  scfg,
		Sync:    syncSubjForStream(),
		Group:   rg,
		Created: time.Now().UTC(),
		Client:  ci,
	}
	err := cc.meta.Propose(cc.meta.Term(), encodeAddStreamAssignment(sa))
	sjs.mu.Unlock()
	require_NoError(t, err)
	c.waitOnAllCurrent()

	unsupported := func(requiredApiLevel int) string {
		return fmt.Sprintf("unsupported - required API level: %d, current API level: %d", requiredApiLevel, JSApiLevel)
	}
	expectStreamInfo := func(offlineReason, streamName string) {
		var msg *nats.Msg
		checkFor(t, 3*time.Second, 200*time.Millisecond, func() error {
			msg, err = nc.Request(fmt.Sprintf(JSApiStreamInfoT, streamName), nil, time.Second)
			return err
		})
		var si JSApiStreamInfoResponse
		require_NoError(t, json.Unmarshal(msg.Data, &si))
		require_NotNil(t, si.Error)
		require_Error(t, si.Error, NewJSStreamOfflineReasonError(errors.New(offlineReason)))

		var sn JSApiStreamNamesResponse
		msg, err = nc.Request(JSApiStreams, nil, time.Second)
		require_NoError(t, err)
		require_NoError(t, json.Unmarshal(msg.Data, &sn))
		require_Len(t, len(sn.Streams), 1)
		require_Equal(t, sn.Streams[0], streamName)

		var sl JSApiStreamListResponse
		msg, err = nc.Request(JSApiStreamList, nil, 2*time.Second)
		require_NoError(t, err)
		require_NoError(t, json.Unmarshal(msg.Data, &sl))
		require_Len(t, len(sl.Streams), 0)
		require_Len(t, len(sl.Missing), 1)
		require_Equal(t, sl.Missing[0], streamName)
		require_Len(t, len(sl.Offline), 1)
		require_Equal(t, sl.Offline[streamName], offlineReason)
	}

	// Stream should be reported as offline, but healthz should report healthy to not block downgrades.
	expectStreamInfo(unsupported(math.MaxInt-1), "DowngradeStreamTest")
	health := ml.healthz(&HealthzOptions{})
	require_Equal(t, health.StatusCode, 200)
	restart()
	expectStreamInfo(unsupported(math.MaxInt-1), "DowngradeStreamTest")
	health = ml.healthz(&HealthzOptions{})
	require_Equal(t, health.StatusCode, 200)

	wsas := getValidMetaSnapshot()
	require_Len(t, len(wsas), 1)
	nsa := &streamAssignment{ConfigJSON: wsas[0].ConfigJSON}
	require_NoError(t, decodeStreamAssignmentConfig(ml, nsa))
	require_Equal(t, nsa.Config.Name, "DowngradeStreamTest")
	require_Equal(t, nsa.Config.Metadata["_nats.req.level"], strconv.Itoa(math.MaxInt-1))

	// Update a stream that's unsupported.
	sjs.mu.Lock()
	scfg.Metadata = map[string]string{"_nats.req.level": strconv.Itoa(math.MaxInt)}
	err = cc.meta.Propose(cc.meta.Term(), encodeUpdateStreamAssignment(sa))
	sjs.mu.Unlock()
	require_NoError(t, err)
	c.waitOnAllCurrent()

	// Stream should be reported as offline, but healthz should report healthy to not block downgrades.
	expectStreamInfo(unsupported(math.MaxInt), "DowngradeStreamTest")
	health = ml.healthz(&HealthzOptions{})
	require_Equal(t, health.StatusCode, 200)
	restart()
	expectStreamInfo(unsupported(math.MaxInt), "DowngradeStreamTest")
	health = ml.healthz(&HealthzOptions{})
	require_Equal(t, health.StatusCode, 200)

	wsas = getValidMetaSnapshot()
	require_Len(t, len(wsas), 1)
	nsa = &streamAssignment{ConfigJSON: wsas[0].ConfigJSON}
	require_NoError(t, decodeStreamAssignmentConfig(ml, nsa))
	require_Equal(t, nsa.Config.Name, "DowngradeStreamTest")
	require_Equal(t, nsa.Config.Metadata["_nats.req.level"], strconv.Itoa(math.MaxInt))

	// Deleting a stream should always work, even if it is unsupported.
	require_NoError(t, js.DeleteStream("DowngradeStreamTest"))
	snap, _, _, err := sjs.metaSnapshot()
	require_NoError(t, err)
	require_True(t, snap == nil)

	// Create a supported stream and consumer.
	_, err = js.AddStream(&nats.StreamConfig{Name: "DowngradeConsumerTest", Replicas: 3})
	require_NoError(t, err)
	_, err = js.AddConsumer("DowngradeConsumerTest", &nats.ConsumerConfig{Name: "consumer"})
	require_NoError(t, err)

	// Create a consumer that's unsupported.
	sjs.mu.Lock()
	ccfg := &ConsumerConfig{
		Name:     "DowngradeConsumerTest",
		Replicas: 3,
		Metadata: map[string]string{"_nats.req.level": strconv.Itoa(math.MaxInt - 1)},
	}
	rg, cerr := cc.createGroupForConsumer(ccfg, sa)
	require_True(t, cerr == nil)
	ca := &consumerAssignment{
		Config:  ccfg,
		Group:   rg,
		Stream:  "DowngradeConsumerTest",
		Name:    "DowngradeConsumerTest",
		Created: time.Now().UTC(),
		Client:  ci,
	}
	err = cc.meta.Propose(cc.meta.Term(), encodeAddConsumerAssignment(ca))
	sjs.mu.Unlock()
	require_NoError(t, err)
	c.waitOnAllCurrent()

	expectConsumerInfo := func(offlineReason string) {
		var msg *nats.Msg
		checkFor(t, 3*time.Second, 200*time.Millisecond, func() error {
			msg, err = nc.Request(fmt.Sprintf(JSApiConsumerInfoT, "DowngradeConsumerTest", "DowngradeConsumerTest"), nil, time.Second)
			return err
		})
		var ci JSApiConsumerInfoResponse
		require_NoError(t, json.Unmarshal(msg.Data, &ci))
		require_NotNil(t, ci.Error)
		require_Error(t, ci.Error, NewJSConsumerOfflineReasonError(errors.New(offlineReason)))

		var cn JSApiConsumerNamesResponse
		msg, err = nc.Request(fmt.Sprintf(JSApiConsumersT, "DowngradeConsumerTest"), nil, time.Second)
		require_NoError(t, err)
		require_NoError(t, json.Unmarshal(msg.Data, &cn))
		require_Len(t, len(cn.Consumers), 2)
		require_Equal(t, cn.Consumers[0], "DowngradeConsumerTest")
		for _, name := range cn.Consumers {
			require_True(t, name == "consumer" || name == "DowngradeConsumerTest")
		}

		var cl JSApiConsumerListResponse
		msg, err = nc.Request(fmt.Sprintf(JSApiConsumerListT, "DowngradeConsumerTest"), nil, 5*time.Second)
		require_NoError(t, err)
		require_NoError(t, json.Unmarshal(msg.Data, &cl))
		require_Len(t, len(cl.Consumers), 0)
		require_Len(t, len(cl.Missing), 2)
		for _, name := range cl.Missing {
			require_True(t, name == "consumer" || name == "DowngradeConsumerTest")
		}
		require_Len(t, len(cl.Offline), 1)
		require_Equal(t, cl.Offline["DowngradeConsumerTest"], offlineReason)

		// Stream should also be reported as offline.
		// Specifically, as "stopped" because it's still supported, but can't run due to the unsupported consumer.
		expectStreamInfo("stopped - unsupported consumer \"DowngradeConsumerTest\"", "DowngradeConsumerTest")
	}

	// Consumer should be reported as offline, but healthz should report healthy to not block downgrades.
	expectConsumerInfo(unsupported(math.MaxInt - 1))
	health = ml.healthz(&HealthzOptions{})
	require_Equal(t, health.StatusCode, 200)
	restart()
	expectConsumerInfo(unsupported(math.MaxInt - 1))
	health = ml.healthz(&HealthzOptions{})
	require_Equal(t, health.StatusCode, 200)

	wsas = getValidMetaSnapshot()
	require_Len(t, len(wsas), 1)
	nsa = &streamAssignment{ConfigJSON: wsas[0].ConfigJSON}
	require_NoError(t, decodeStreamAssignmentConfig(ml, nsa))
	require_Equal(t, nsa.Config.Name, "DowngradeConsumerTest")
	require_Equal(t, nsa.Config.Metadata["_nats.req.level"], "0")
	require_Len(t, len(wsas[0].Consumers), 2)
	for _, wca := range wsas[0].Consumers {
		nca := &consumerAssignment{ConfigJSON: wca.ConfigJSON}
		require_NoError(t, decodeConsumerAssignmentConfig(nca))
		if nca.Config.Name == "DowngradeConsumerTest" {
			require_Equal(t, nca.Config.Metadata["_nats.req.level"], strconv.Itoa(math.MaxInt-1))
		} else {
			require_Equal(t, nca.Config.Name, "consumer")
		}
	}

	// Update a consumer (with compressed data) that's unsupported.
	ccfg.Metadata = map[string]string{"_nats.req.level": strconv.Itoa(math.MaxInt)}
	sjs.mu.Lock()
	err = cc.meta.Propose(cc.meta.Term(), encodeAddConsumerAssignmentCompressed(ca))
	sjs.mu.Unlock()
	require_NoError(t, err)
	c.waitOnAllCurrent()

	// Consumer should be reported as offline, but healthz should report healthy to not block downgrades.
	expectConsumerInfo(unsupported(math.MaxInt))
	health = ml.healthz(&HealthzOptions{})
	require_Equal(t, health.StatusCode, 200)
	restart()
	expectConsumerInfo(unsupported(math.MaxInt))
	health = ml.healthz(&HealthzOptions{})
	require_Equal(t, health.StatusCode, 200)

	wsas = getValidMetaSnapshot()
	require_Len(t, len(wsas), 1)
	nsa = &streamAssignment{ConfigJSON: wsas[0].ConfigJSON}
	require_NoError(t, decodeStreamAssignmentConfig(ml, nsa))
	require_Equal(t, nsa.Config.Name, "DowngradeConsumerTest")
	require_Equal(t, nsa.Config.Metadata["_nats.req.level"], "0")
	require_Len(t, len(wsas[0].Consumers), 2)
	for _, wca := range wsas[0].Consumers {
		nca := &consumerAssignment{ConfigJSON: wca.ConfigJSON}
		require_NoError(t, decodeConsumerAssignmentConfig(nca))
		if nca.Config.Name == "DowngradeConsumerTest" {
			require_Equal(t, nca.Config.Metadata["_nats.req.level"], strconv.Itoa(math.MaxInt))
		} else {
			require_Equal(t, nca.Config.Name, "consumer")
		}
	}

	// Deleting a consumer should always work, even if it is unsupported.
	require_NoError(t, js.DeleteConsumer("DowngradeConsumerTest", "DowngradeConsumerTest"))
	c.waitOnAllCurrent()

	wsas = getValidMetaSnapshot()
	require_Len(t, len(wsas), 1)
	nsa = &streamAssignment{ConfigJSON: wsas[0].ConfigJSON}
	require_NoError(t, decodeStreamAssignmentConfig(ml, nsa))
	require_Equal(t, nsa.Config.Name, "DowngradeConsumerTest")
	require_Equal(t, nsa.Config.Metadata["_nats.req.level"], "0")
	require_Len(t, len(wsas[0].Consumers), 1)
	nca := &consumerAssignment{ConfigJSON: wsas[0].Consumers[0].ConfigJSON}
	require_NoError(t, decodeConsumerAssignmentConfig(nca))
	require_Equal(t, nca.Config.Name, "consumer")
}

func TestJetStreamClusterOfflineStreamAndConsumerAfterDowngrade(t *testing.T) {
	clusterName := "R3S"
	c := createJetStreamClusterExplicit(t, clusterName, 3)
	defer c.shutdown()

	nc, js := jsClientConnect(t, c.randomServer())
	defer nc.Close()

	ml := c.leader()
	require_NotNil(t, ml)

	sjs := ml.getJetStream()
	require_NotNil(t, sjs)
	sjs.mu.Lock()
	cc := sjs.cluster
	if cc == nil || cc.meta == nil {
		sjs.mu.Unlock()
		t.Fatalf("Expected cluster to be initialized")
	}

	restart := func() {
		t.Helper()
		for _, s := range c.servers {
			sjs = s.getJetStream()
			snap, _, _, err := sjs.metaSnapshot()
			require_NoError(t, err)
			meta := sjs.getMetaGroup()
			meta.InstallSnapshot(snap, false)
		}

		c.stopAll()
		c.restartAllSamePorts()
		c.waitOnLeader()
		ml = c.leader()
		require_NotNil(t, ml)
		require_NoError(t, nc.ForceReconnect())

		sjs = ml.getJetStream()
		require_NotNil(t, sjs)
		sjs.mu.Lock()
		cc = sjs.cluster
		if cc == nil || cc.meta == nil {
			sjs.mu.Unlock()
			t.Fatalf("Expected cluster to be initialized")
		}
		sjs.mu.Unlock()
	}

	getValidMetaSnapshot := func() (wsas []writeableStreamAssignment) {
		t.Helper()
		snap, _, _, err := sjs.metaSnapshot()
		require_NoError(t, err)
		require_True(t, len(snap) > 0)
		dec, err := s2.Decode(nil, snap)
		require_NoError(t, err)
		require_NoError(t, json.Unmarshal(dec, &wsas))
		return wsas
	}

	// Create a stream that's unsupported.
	ci := &ClientInfo{
		Account: globalAccountName,
		Cluster: clusterName,
	}
	scfg := &StreamConfig{
		Name:     "DowngradeStreamTest",
		Storage:  FileStorage,
		Replicas: 3,
	}
	rg, perr := sjs.createGroupForStream(ci, scfg)
	if perr != nil {
		sjs.mu.Unlock()
		require_NoError(t, perr)
	}
	sa := &streamAssignment{
		Config:  scfg,
		Sync:    syncSubjForStream(),
		Group:   rg,
		Created: time.Now().UTC(),
		Client:  ci,
	}
	err := cc.meta.Propose(cc.meta.Term(), encodeAddStreamAssignment(sa))
	sjs.mu.Unlock()
	require_NoError(t, err)
	c.waitOnStreamLeader(globalAccountName, "DowngradeStreamTest")

	expectStreamInfo := func(offline bool) {
		if !offline {
			c.waitOnStreamLeader(globalAccountName, "DowngradeStreamTest")
		}
		var msg *nats.Msg
		checkFor(t, 3*time.Second, 200*time.Millisecond, func() error {
			msg, err = nc.Request(fmt.Sprintf(JSApiStreamInfoT, "DowngradeStreamTest"), nil, time.Second)
			return err
		})
		var si JSApiStreamInfoResponse
		require_NoError(t, json.Unmarshal(msg.Data, &si))
		if !offline {
			require_True(t, si.Error == nil)
		} else {
			require_NotNil(t, si.Error)
			require_Contains(t, si.Error.Error(), "stream is offline", "unsupported", "required API level")
		}
	}

	// healthz can transiently report not-current right after a restart while the
	// asset's raft group node is still catching up, so poll until it reports healthy.
	expectHealthz := func() {
		t.Helper()
		checkFor(t, 5*time.Second, 100*time.Millisecond, func() error {
			if health := ml.healthz(&HealthzOptions{Details: true}); health.StatusCode != 200 {
				return fmt.Errorf("healthz not ready (%d): %+v", health.StatusCode, health.Errors)
			}
			return nil
		})
	}

	// Stream is still supported, so it should be available and healthz should report healthy.
	expectStreamInfo(false)
	expectHealthz()
	restart()
	expectStreamInfo(false)
	expectHealthz()

	wsas := getValidMetaSnapshot()
	require_Len(t, len(wsas), 1)
	nsa := &streamAssignment{ConfigJSON: wsas[0].ConfigJSON}
	require_NoError(t, decodeStreamAssignmentConfig(ml, nsa))
	require_Equal(t, nsa.Config.Name, "DowngradeStreamTest")

	// Update a stream to be unsupported.
	sjs.mu.Lock()
	scfg.Metadata = map[string]string{"_nats.req.level": strconv.Itoa(math.MaxInt)}
	err = cc.meta.Propose(cc.meta.Term(), encodeUpdateStreamAssignment(sa))
	sjs.mu.Unlock()
	require_NoError(t, err)
	c.waitOnAllCurrent()

	// Stream should be reported as offline, but healthz should report healthy to not block downgrades.
	expectStreamInfo(true)
	expectHealthz()
	restart()
	expectStreamInfo(true)
	expectHealthz()

	wsas = getValidMetaSnapshot()
	require_Len(t, len(wsas), 1)
	nsa = &streamAssignment{ConfigJSON: wsas[0].ConfigJSON}
	require_NoError(t, decodeStreamAssignmentConfig(ml, nsa))
	require_Equal(t, nsa.Config.Name, "DowngradeStreamTest")

	// Deleting a stream should always work, even if it is unsupported.
	require_NoError(t, js.DeleteStream("DowngradeStreamTest"))
	snap, _, _, err := sjs.metaSnapshot()
	require_NoError(t, err)
	require_True(t, snap == nil)

	// Create a supported stream and consumer.
	_, err = js.AddStream(&nats.StreamConfig{Name: "DowngradeConsumerTest", Replicas: 3})
	require_NoError(t, err)

	sjs.mu.Lock()
	ccfg := &ConsumerConfig{
		Name:       "DowngradeConsumerTest",
		Replicas:   3,
		MaxWaiting: JSWaitQueueDefaultMax,
	}
	rg, cerr := cc.createGroupForConsumer(ccfg, sa)
	require_True(t, cerr == nil)
	ca := &consumerAssignment{
		Config:  ccfg,
		Group:   rg,
		Stream:  "DowngradeConsumerTest",
		Name:    "DowngradeConsumerTest",
		Created: time.Now().UTC(),
		Client:  ci,
	}
	err = cc.meta.Propose(cc.meta.Term(), encodeAddConsumerAssignment(ca))
	sjs.mu.Unlock()
	require_NoError(t, err)
	c.waitOnConsumerLeader(globalAccountName, "DowngradeConsumerTest", "DowngradeConsumerTest")

	expectConsumerInfo := func(offline bool) {
		if !offline {
			c.waitOnConsumerLeader(globalAccountName, "DowngradeConsumerTest", "DowngradeConsumerTest")
		}
		var msg *nats.Msg
		checkFor(t, 3*time.Second, 200*time.Millisecond, func() error {
			msg, err = nc.Request(fmt.Sprintf(JSApiConsumerInfoT, "DowngradeConsumerTest", "DowngradeConsumerTest"), nil, 2*time.Second)
			return err
		})
		var ci JSApiConsumerInfoResponse
		require_NoError(t, json.Unmarshal(msg.Data, &ci))
		if !offline {
			require_True(t, ci.Error == nil)
		} else {
			require_NotNil(t, ci.Error)
			require_Contains(t, ci.Error.Error(), "consumer is offline", "unsupported", "required API level")
		}
	}

	// Consumer is still supported, so it should be available and healthz should report healthy.
	expectConsumerInfo(false)
	expectHealthz()
	restart()
	expectConsumerInfo(false)
	expectHealthz()

	wsas = getValidMetaSnapshot()
	require_Len(t, len(wsas), 1)
	nsa = &streamAssignment{ConfigJSON: wsas[0].ConfigJSON}
	require_NoError(t, decodeStreamAssignmentConfig(ml, nsa))
	require_Equal(t, nsa.Config.Name, "DowngradeConsumerTest")
	require_Len(t, len(wsas[0].Consumers), 1)

	// Update a consumer to be unsupported.
	ccfg.Metadata = map[string]string{"_nats.req.level": strconv.Itoa(math.MaxInt)}
	sjs.mu.Lock()
	err = cc.meta.Propose(cc.meta.Term(), encodeAddConsumerAssignment(ca))
	sjs.mu.Unlock()
	require_NoError(t, err)
	c.waitOnAllCurrent()

	// Consumer should be reported as offline, but healthz should report healthy to not block downgrades.
	expectConsumerInfo(true)
	expectHealthz()
	restart()
	expectConsumerInfo(true)
	expectHealthz()

	wsas = getValidMetaSnapshot()
	require_Len(t, len(wsas), 1)
	nsa = &streamAssignment{ConfigJSON: wsas[0].ConfigJSON}
	require_NoError(t, decodeStreamAssignmentConfig(ml, nsa))
	require_Equal(t, nsa.Config.Name, "DowngradeConsumerTest")
	require_Equal(t, nsa.Config.Metadata["_nats.req.level"], "0")
	require_Len(t, len(wsas[0].Consumers), 1)
	nca := &consumerAssignment{ConfigJSON: wsas[0].Consumers[0].ConfigJSON}
	require_NoError(t, decodeConsumerAssignmentConfig(nca))
	require_Equal(t, nca.Config.Metadata["_nats.req.level"], strconv.Itoa(math.MaxInt))
}

func TestJetStreamClusterOfflineStreamAndConsumerUpdate(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "R3S", 3)
	defer c.shutdown()

	nc, js := jsClientConnect(t, c.randomServer())
	defer nc.Close()

	_, err := js.AddStream(&nats.StreamConfig{Name: "DowngradeTest", Replicas: 3})
	require_NoError(t, err)
	_, err = js.AddConsumer("DowngradeTest", &nats.ConsumerConfig{Durable: "D", Replicas: 3})
	require_NoError(t, err)
	c.waitOnAllCurrent()

	ml := c.leader()
	require_NotNil(t, ml)
	sjs := ml.getJetStream()
	require_NotNil(t, sjs)

	var sa *streamAssignment
	var ca *consumerAssignment
	checkFor(t, 2*time.Second, 200*time.Millisecond, func() error {
		sjs.mu.Lock()
		defer sjs.mu.Unlock()
		sa = sjs.streamAssignment(globalAccountName, "DowngradeTest")
		if sa == nil {
			return errors.New("stream assignment missing")
		}
		ca = sjs.consumerAssignment(globalAccountName, "DowngradeTest", "D")
		if ca == nil {
			return errors.New("consumer assignment missing")
		}
		return nil
	})

	getValidMetaSnapshot := func() (wsas []writeableStreamAssignment) {
		t.Helper()
		snap, _, _, err := sjs.metaSnapshot()
		require_NoError(t, err)
		require_True(t, len(snap) > 0)
		dec, err := s2.Decode(nil, snap)
		require_NoError(t, err)
		require_NoError(t, json.Unmarshal(dec, &wsas))
		return wsas
	}

	wsas := getValidMetaSnapshot()
	require_Len(t, len(wsas), 1)
	nsa := &streamAssignment{ConfigJSON: wsas[0].ConfigJSON}
	require_NoError(t, decodeStreamAssignmentConfig(ml, nsa))
	require_Equal(t, nsa.Config.Name, "DowngradeTest")
	require_Equal(t, nsa.Config.Metadata["_nats.req.level"], "0")
	require_Len(t, len(wsas[0].Consumers), 1)
	nca := &consumerAssignment{ConfigJSON: wsas[0].Consumers[0].ConfigJSON}
	require_NoError(t, decodeConsumerAssignmentConfig(nca))
	require_Equal(t, wsas[0].Consumers[0].Name, "D")
	require_Equal(t, nca.Config.Metadata["_nats.req.level"], "0")

	// Update a consumer to be unsupported.
	sjs.mu.Lock()
	ca.Config.Metadata = map[string]string{"_nats.req.level": strconv.Itoa(math.MaxInt)}
	meta := sjs.cluster.meta
	err = meta.Propose(meta.Term(), encodeAddConsumerAssignment(ca))
	sjs.mu.Unlock()
	require_NoError(t, err)

	// Update the stream to be unsupported.
	sjs.mu.Lock()
	sa.Config.Metadata = map[string]string{"_nats.req.level": strconv.Itoa(math.MaxInt)}
	err = meta.Propose(meta.Term(), encodeUpdateStreamAssignment(sa))
	sjs.mu.Unlock()
	require_NoError(t, err)
	c.waitOnAllCurrent()

	wsas = getValidMetaSnapshot()
	require_Len(t, len(wsas), 1)
	nsa = &streamAssignment{ConfigJSON: wsas[0].ConfigJSON}
	require_NoError(t, decodeStreamAssignmentConfig(ml, nsa))
	require_Equal(t, nsa.Config.Name, "DowngradeTest")
	require_Equal(t, nsa.Config.Metadata["_nats.req.level"], strconv.Itoa(math.MaxInt))
	require_Len(t, len(wsas[0].Consumers), 1)
	nca = &consumerAssignment{ConfigJSON: wsas[0].Consumers[0].ConfigJSON}
	require_NoError(t, decodeConsumerAssignmentConfig(nca))
	require_Equal(t, wsas[0].Consumers[0].Name, "D")
	require_Equal(t, nca.Config.Metadata["_nats.req.level"], strconv.Itoa(math.MaxInt))
}

func TestJetStreamClusterOfflineStreamAndConsumerStrictDecoding(t *testing.T) {
	s := RunBasicJetStreamServer(t)
	defer s.Shutdown()

	unsupportedJson := []byte("{\"unknown\": true}")
	unsupportedStreamJson := []byte(fmt.Sprintf("{\"stream\":%s}", unsupportedJson))
	unsupportedConsumerJson := []byte(fmt.Sprintf("{\"consumer\":%s}", unsupportedJson))

	sa, err := decodeStreamAssignment(s, unsupportedStreamJson)
	require_NoError(t, err)
	require_True(t, bytes.Equal(sa.ConfigJSON, unsupportedJson))
	require_True(t, sa.unsupported != nil)

	ca, err := decodeConsumerAssignment(unsupportedConsumerJson)
	require_NoError(t, err)
	require_True(t, bytes.Equal(ca.ConfigJSON, unsupportedJson))
	require_True(t, ca.unsupported != nil)

	var bb bytes.Buffer
	s2e := s2.NewWriter(&bb)
	_, err = s2e.Write(unsupportedConsumerJson)
	require_NoError(t, err)
	require_NoError(t, s2e.Close())
	ca, err = decodeConsumerAssignmentCompressed(bb.Bytes())
	require_NoError(t, err)
	require_True(t, bytes.Equal(ca.ConfigJSON, unsupportedJson))
	require_True(t, ca.unsupported != nil)
}

func TestJetStreamClusterStreamMonitorShutdownWithoutRaftNode(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "R3S", 3)
	defer c.shutdown()

	nc, js := jsClientConnect(t, c.randomServer())
	defer nc.Close()

	_, err := js.AddStream(&nats.StreamConfig{
		Name:     "TEST",
		Subjects: []string{"foo"},
		Replicas: 3,
	})
	require_NoError(t, err)
	checkFor(t, 2*time.Second, 200*time.Millisecond, func() error {
		for _, s := range c.servers {
			if !s.JetStreamIsStreamAssigned(globalAccountName, "TEST") {
				return fmt.Errorf("stream not assigned on %s", s.Name())
			}
		}
		return nil
	})

	var nodes []RaftNode
	for _, s := range c.servers {
		mset, err := s.globalAccount().lookupStream("TEST")
		require_NoError(t, err)
		// Manually nil-out the node. This shouldn't happen normally,
		// but tests we can shut down purely with the monitor goroutine quit channel.
		mset.mu.Lock()
		n := mset.node
		mset.node = nil
		mset.mu.Unlock()
		require_NotNil(t, n)
		nodes = append(nodes, n)
	}
	for _, n := range nodes {
		require_NotEqual(t, n.State(), Closed)
	}

	require_NoError(t, js.DeleteStream("TEST"))
	checkFor(t, 2*time.Second, 200*time.Millisecond, func() error {
		for _, n := range nodes {
			if state := n.State(); state != Closed {
				return fmt.Errorf("node not closed on %s: %s", n.ID(), state.String())
			}
		}
		return nil
	})
}

func TestJetStreamClusterConsumerMonitorShutdownWithoutRaftNode(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "R3S", 3)
	defer c.shutdown()

	nc, js := jsClientConnect(t, c.randomServer())
	defer nc.Close()

	_, err := js.AddStream(&nats.StreamConfig{
		Name:     "TEST",
		Subjects: []string{"foo"},
		Replicas: 3,
	})
	require_NoError(t, err)
	_, err = js.AddConsumer("TEST", &nats.ConsumerConfig{
		Durable:  "DURABLE",
		Replicas: 3,
	})
	require_NoError(t, err)
	checkFor(t, 2*time.Second, 200*time.Millisecond, func() error {
		for _, s := range c.servers {
			_, cc := s.getJetStreamCluster()
			if !cc.isConsumerAssigned(s.globalAccount(), "TEST", "DURABLE") {
				return fmt.Errorf("consumer not assigned on %s", s.Name())
			}
		}
		return nil
	})

	var nodes []RaftNode
	for _, s := range c.servers {
		mset, err := s.globalAccount().lookupStream("TEST")
		require_NoError(t, err)
		o := mset.lookupConsumer("DURABLE")
		require_NotNil(t, o)
		// Manually nil-out the node. This shouldn't happen normally,
		// but tests we can shut down purely with the monitor goroutine quit channel.
		o.mu.Lock()
		n := o.node
		o.node = nil
		o.mu.Unlock()
		require_NotNil(t, n)
		nodes = append(nodes, n)
	}
	for _, n := range nodes {
		require_NotEqual(t, n.State(), Closed)
	}

	require_NoError(t, js.DeleteStream("TEST"))
	checkFor(t, 2*time.Second, 200*time.Millisecond, func() error {
		for _, n := range nodes {
			if state := n.State(); state != Closed {
				return fmt.Errorf("node not closed on %s: %s", n.ID(), state.String())
			}
		}
		return nil
	})
}

func TestJetStreamClusterUnsetEmptyPlacement(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "R3S", 3)
	defer c.shutdown()

	nc, js := jsClientConnect(t, c.randomServer())
	defer nc.Close()

	cfg := &nats.StreamConfig{
		Name:      "TEST",
		Subjects:  []string{"foo"},
		Placement: &nats.Placement{},
	}
	si, err := js.AddStream(cfg)
	require_NoError(t, err)
	require_True(t, si.Config.Placement == nil)

	si, err = js.UpdateStream(cfg)
	require_NoError(t, err)
	require_True(t, si.Config.Placement == nil)

	// Set a placement level
	cfg.Placement = &nats.Placement{Cluster: "R3S"}
	si, err = js.UpdateStream(cfg)
	require_NoError(t, err)
	require_True(t, si.Config.Placement != nil)
	require_Equal(t, si.Config.Placement.Cluster, "R3S")

	// And ensure it can be reset.
	cfg.Placement = &nats.Placement{}
	si, err = js.UpdateStream(cfg)
	require_NoError(t, err)
	require_True(t, si.Config.Placement == nil)
}

func TestJetStreamClusterPersistModeAsync(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "R3S", 3)
	defer c.shutdown()

	nc := clientConnectToServer(t, c.randomServer())
	defer nc.Close()

	cfg := &StreamConfig{
		Name:        "TEST",
		Subjects:    []string{"foo"},
		Storage:     FileStorage,
		Replicas:    3,
		PersistMode: AsyncPersistMode,
	}
	_, err := jsStreamCreate(t, nc, cfg)
	require_Error(t, err, NewJSStreamInvalidConfigError(fmt.Errorf("async persist mode is not supported on replicated streams")))
}

func TestJetStreamClusterDeleteMsgEOF(t *testing.T) {
	for _, replicas := range []int{1, 3} {
		t.Run(fmt.Sprintf("R%d", replicas), func(t *testing.T) {
			c := createJetStreamClusterExplicit(t, "R3S", 3)
			defer c.shutdown()

			nc, js := jsClientConnect(t, c.randomServer())
			defer nc.Close()

			_, err := js.AddStream(&nats.StreamConfig{
				Name:     "TEST",
				Subjects: []string{"foo"},
				Replicas: replicas,
			})
			require_NoError(t, err)

			_, err = js.Publish("foo", nil)
			require_NoError(t, err)

			require_Error(t, js.DeleteMsg("TEST", 0), NewJSNoMessageFoundError())
			require_NoError(t, js.DeleteMsg("TEST", 1))
			require_Error(t, js.DeleteMsg("TEST", 1), NewJSNoMessageFoundError())
			require_Error(t, js.DeleteMsg("TEST", 2), NewJSStreamMsgDeleteFailedError(ErrStoreEOF))
		})
	}
}

func TestJetStreamClusterCatchupSkipMsgDesync(t *testing.T) {
	for _, storage := range []nats.StorageType{nats.FileStorage, nats.MemoryStorage} {
		t.Run(storage.String(), func(t *testing.T) {
			c := createJetStreamClusterExplicit(t, "R3S", 3)
			defer c.shutdown()

			nc, js := jsClientConnect(t, c.randomServer())
			defer nc.Close()

			_, err := js.AddStream(&nats.StreamConfig{
				Name:     "TEST",
				Subjects: []string{"foo"},
				Storage:  storage,
				Replicas: 3,
			})
			require_NoError(t, err)
			checkFor(t, 2*time.Second, 200*time.Millisecond, func() error {
				return checkState(t, c, globalAccountName, "TEST")
			})

			rs := c.randomNonStreamLeader(globalAccountName, "TEST")
			mset, err := rs.globalAccount().lookupStream("TEST")
			require_NoError(t, err)
			sa := mset.streamAssignment()

			// Make sure this server can't become the leader.
			n := mset.raftNode().(*raft)
			n.SetObserver(true)

			sysNc, err := nats.Connect(rs.ClientURL(), nats.UserInfo("admin", "s3cr3t!"))
			require_NoError(t, err)
			defer sysNc.Close()

			sjs := rs.getJetStream()
			sjs.mu.RLock()
			syncSubj := sa.Sync
			sjs.mu.RUnlock()

			// Respond to the catchup with an out-of-order SkipMsg.
			var eof bool
			sub, err := sysNc.Subscribe(syncSubj, func(msg *nats.Msg) {
				if !eof {
					msg.Respond(encodeStreamMsg(_EMPTY_, _EMPTY_, nil, nil, 10, 0, false))
					eof = true
				}
				msg.Respond(nil)
			})
			require_NoError(t, err)
			defer sub.Drain()
			require_NoError(t, sysNc.Flush()) // Must flush, otherwise our subscription could be too late.

			err = mset.processSnapshot(&StreamReplicatedState{FirstSeq: 1, LastSeq: 1}, 1)
			require_Error(t, err, errCatchupTooManyRetries)
			c.waitOnStreamLeader(globalAccountName, "TEST")

			pubAck, err := js.Publish("foo", nil)
			require_NoError(t, err)
			require_Equal(t, pubAck.Sequence, 1)
			checkFor(t, 2*time.Second, 200*time.Millisecond, func() error {
				return checkState(t, c, globalAccountName, "TEST")
			})
		})
	}
}

func TestJetStreamClusterJszRaftLeaderReporting(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "R3S", 3)
	defer c.shutdown()

	nc, js := jsClientConnect(t, c.randomServer())
	defer nc.Close()

	_, err := js.AddStream(&nats.StreamConfig{
		Name:     "TEST",
		Subjects: []string{"foo"},
		Replicas: 3,
	})
	require_NoError(t, err)

	_, err = js.AddConsumer("TEST", &nats.ConsumerConfig{
		Durable:  "DURABLE",
		Replicas: 3,
	})
	require_NoError(t, err)

	checkFor(t, 2*time.Second, 200*time.Millisecond, func() error {
		for _, s := range c.servers {
			_, _, jsa := s.globalAccount().getJetStreamFromAccount()
			if !jsa.streamAssigned("TEST") {
				return fmt.Errorf("stream not assigned on %s", s.Name())
			}
			if !jsa.consumerAssigned("TEST", "DURABLE") {
				return fmt.Errorf("consumer not assigned on %s", s.Name())
			}
		}
		return nil
	})

	sl := c.streamLeader(globalAccountName, "TEST")
	cl := c.consumerLeader(globalAccountName, "TEST", "DURABLE")

	for _, s := range c.servers {
		jsi, err := s.Jsz(&JSzOptions{RaftGroups: true})
		require_NoError(t, err)
		if s == sl {
			require_Equal(t, jsi.StreamsLeader, 1)
		} else {
			require_Equal(t, jsi.StreamsLeader, 0)
		}
		if s == cl {
			require_Equal(t, jsi.ConsumersLeader, 1)
		} else {
			require_Equal(t, jsi.ConsumersLeader, 0)
		}
	}
}

func TestJetStreamClusterNoInterestDesyncOnConsumerCreate(t *testing.T) {
	test := func(t *testing.T, twoConsumers bool) {
		c := createJetStreamClusterExplicit(t, "R3S", 3)
		defer c.shutdown()

		nc, js := jsClientConnect(t, c.randomServer())
		defer nc.Close()

		_, err := js.AddStream(&nats.StreamConfig{
			Name:      "TEST",
			Subjects:  []string{"foo", "bar"},
			Replicas:  3,
			Retention: nats.InterestPolicy,
		})
		require_NoError(t, err)

		// Pick a random server that will not know about the new consumer being created.
		// If servers determine "no interest" individually, these servers will desync.
		rs := c.randomNonLeader()
		sjs := rs.getJetStream()
		meta := sjs.getMetaGroup()
		require_NoError(t, meta.PauseApply())

		sub, err := js.PullSubscribe("foo", "DURABLE", nats.BindStream("TEST"))
		require_NoError(t, err)
		defer sub.Drain()

		checkConsumersAssigned := func(expected int) {
			t.Helper()
			checkFor(t, 2*time.Second, 200*time.Millisecond, func() error {
				var count int
				for _, s := range c.servers {
					mset, err := s.globalAccount().lookupStream("TEST")
					if err != nil {
						return err
					}
					count += mset.numConsumers()
				}
				if count != expected {
					return fmt.Errorf("expected %d, got %d", expected, count)
				}
				return nil
			})
		}
		// Confirm only two servers know about the consumer.
		checkConsumersAssigned(2)
		c.waitOnConsumerLeader(globalAccountName, "TEST", "DURABLE")

		// Publish a single message. All servers will receive this, but only two will store it.
		_, err = js.Publish("foo", nil)
		require_NoError(t, err)
		checkLastSeq := func(lseq uint64) {
			t.Helper()
			checkFor(t, 2*time.Second, 200*time.Millisecond, func() error {
				for _, s := range c.servers {
					mset, err := s.globalAccount().lookupStream("TEST")
					if err != nil {
						return err
					}
					if seq := mset.lastSeq(); seq != lseq {
						return fmt.Errorf("expected %d, got %d", lseq, seq)
					}
				}
				return nil
			})
		}
		checkLastSeq(1)

		// Resume the meta layer such that the consumer gets created on the remaining server.
		meta.ResumeApply()
		checkConsumersAssigned(3)

		if twoConsumers {
			_, err = js.AddConsumer("TEST", &nats.ConsumerConfig{Durable: "DURABLE2", FilterSubject: "bar"})
			require_NoError(t, err)
			checkConsumersAssigned(6)
		}

		// All servers will now store another published message.
		_, err = js.Publish("foo", nil)
		require_NoError(t, err)
		checkLastSeq(2)

		// Make sure the consumer leader is on the same server that didn't store the first message.
		cl := c.consumerLeader(globalAccountName, "TEST", "DURABLE")
		if cl != rs {
			mset, err := cl.globalAccount().lookupStream("TEST")
			require_NoError(t, err)
			o := mset.lookupConsumer("DURABLE")
			require_NotNil(t, o)
			n := o.raftNode()
			require_NoError(t, n.StepDown(rs.NodeName()))
			c.waitOnConsumerLeader(globalAccountName, "TEST", "DURABLE")
			cl = c.consumerLeader(globalAccountName, "TEST", "DURABLE")
			require_Equal(t, cl, rs)
		}

		// Since the consumer leader is the same as the server that didn't store the first message,
		// it can only receive and ack the second message.
		msgs, err := sub.Fetch(1, nats.MaxWait(time.Second))
		require_NoError(t, err)
		require_Len(t, len(msgs), 1)
		metadata, err := msgs[0].Metadata()
		require_NoError(t, err)
		require_Equal(t, metadata.Sequence.Stream, 2)
		require_Equal(t, metadata.NumPending, 0)
		require_NoError(t, msgs[0].AckSync())

		if twoConsumers {
			require_NoError(t, js.DeleteConsumer("TEST", "DURABLE"))
		}

		checkFor(t, 2*time.Second, 200*time.Millisecond, func() error {
			// The servers will eventually be synced up again, but this relies on the interest state being checked.
			for _, s := range c.servers {
				if s == rs {
					continue
				}
				mset, err := s.globalAccount().lookupStream("TEST")
				if err != nil {
					return err
				}
				mset.checkInterestState()
			}
			return checkState(t, c, globalAccountName, "TEST")
		})
	}
	t.Run("OneConsumer", func(t *testing.T) { test(t, false) })
	t.Run("TwoConsumers", func(t *testing.T) { test(t, true) })
}

func TestJetStreamClusterRaftCatchupSignalsMetaRecovery(t *testing.T) {
	test := func(t *testing.T, stall bool) {
		c := createJetStreamClusterExplicit(t, "R3S", 3)
		defer c.shutdown()

		nc, js := jsClientConnect(t, c.randomServer())
		defer nc.Close()

		_, err := js.AddStream(&nats.StreamConfig{
			Name:     "TEST",
			Subjects: []string{"foo"},
			Replicas: 3,
		})
		require_NoError(t, err)

		rs := c.randomNonLeader()
		checkFor(t, 2*time.Second, 200*time.Millisecond, func() error {
			if !rs.JetStreamIsStreamAssigned(globalAccountName, "TEST") {
				return errors.New("not assigned")
			}
			return nil
		})

		sjs := rs.getJetStream()
		sjs.mu.Lock()
		sa := sjs.streamAssignment(globalAccountName, "TEST")
		encodedDelete := encodeDeleteStreamAssignment(sa)
		sjs.mu.Unlock()
		meta := sjs.getMetaGroup().(*raft)
		apply := meta.ApplyQ()

		// Should not panic if we receive a nil entry "randomly".
		_, err = apply.push(nil)
		require_NoError(t, err)

		// Should put us in upper-layer recovery mode.
		// For this test using two large values so we remain in catchup.
		meta.Lock()
		meta.createCatchup(&appendEntry{pterm: 100, pindex: 100})
		meta.sendCatchupSignal()
		meta.Unlock()

		// Deleting a stream should be staged, not immediately performed.
		_, err = apply.push(newCommittedEntry(1, []*Entry{{EntryNormal, encodedDelete}}))
		require_NoError(t, err)
		time.Sleep(200 * time.Millisecond)
		require_True(t, rs.JetStreamIsStreamAssigned(globalAccountName, "TEST"))

		// Healthz reports the in-flight catchup.
		hs := rs.healthz(nil)
		require_Equal(t, hs.Error, "JetStream is not current with the meta leader")

		if stall {
			// A stalled catchup is re-requested through createCatchup, which
			// replaces the catchup state. The upper layer was already signaled,
			// so completing this catchup must still signal it back.
			meta.Lock()
			meta.createCatchup(&appendEntry{pterm: 100, pindex: 100})
			meta.Unlock()
		}

		// Canceling the catchup, because it's completed, should result in the staged changes to be applied.
		meta.Lock()
		meta.cancelCatchup()
		meta.Unlock()
		checkFor(t, 2*time.Second, 200*time.Millisecond, func() error {
			if rs.JetStreamIsStreamAssigned(globalAccountName, "TEST") {
				return errors.New("still assigned")
			}
			return nil
		})

		// And healthz is healthy again.
		hs = rs.healthz(nil)
		require_Equal(t, hs.StatusCode, 200)
		require_Equal(t, hs.Error, _EMPTY_)
	}

	t.Run("NoStall", func(t *testing.T) { test(t, false) })
	t.Run("Stall", func(t *testing.T) { test(t, true) })
}

func TestJetStreamClusterRaftCatchupSignalsMetaRecoveryRecreateStream(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "R3S", 3)
	defer c.shutdown()

	nc, js := jsClientConnect(t, c.randomServer())
	defer nc.Close()

	_, err := js.AddStream(&nats.StreamConfig{
		Name:     "TEST",
		Subjects: []string{"foo"},
		Replicas: 3,
		Storage:  nats.FileStorage,
	})
	require_NoError(t, err)

	rs := c.randomNonLeader()
	checkFor(t, 2*time.Second, 200*time.Millisecond, func() error {
		if !rs.JetStreamIsStreamAssigned(globalAccountName, "TEST") {
			return errors.New("not assigned")
		}
		return nil
	})

	sjs := rs.getJetStream()
	sjs.mu.Lock()
	sa := sjs.streamAssignment(globalAccountName, "TEST")
	encodedDelete := encodeDeleteStreamAssignment(sa)
	sa.Config.Storage = MemoryStorage
	encodedAdd := encodeAddStreamAssignment(sa)
	encodedUpdate := encodeUpdateStreamAssignment(sa)
	sjs.mu.Unlock()
	meta := sjs.getMetaGroup().(*raft)
	apply := meta.ApplyQ()

	// Should not panic if we receive a nil entry "randomly".
	_, err = apply.push(nil)
	require_NoError(t, err)

	// Should put us in upper-layer recovery mode.
	// For this test using two large values so we remain in catchup.
	meta.Lock()
	meta.createCatchup(&appendEntry{pterm: 100, pindex: 100})
	meta.Unlock()

	// Deleting a stream should be staged, not immediately performed.
	_, err = apply.push(newCommittedEntry(1, []*Entry{
		{EntryNormal, encodedDelete},
		{EntryNormal, encodedAdd},
		{EntryNormal, encodedUpdate},
	}))
	require_NoError(t, err)

	// Canceling the catchup, because it's completed, should result in the staged changes to be applied.
	meta.Lock()
	meta.cancelCatchup()
	meta.Unlock()
	checkFor(t, 2*time.Second, 200*time.Millisecond, func() error {
		mset, err := rs.globalAccount().lookupStream("TEST")
		if err != nil {
			return err
		}
		cfg := mset.config()
		if cfg.Storage != MemoryStorage {
			return errors.New("still the old stream config")
		}
		return nil
	})
}

func TestJetStreamClusterRaftCatchupSignalsMetaRecoveryRecreateStreamRemoved(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "R3S", 3)
	defer c.shutdown()

	nc, js := jsClientConnect(t, c.randomServer())
	defer nc.Close()

	_, err := js.AddStream(&nats.StreamConfig{
		Name:     "TEST",
		Subjects: []string{"foo"},
		Replicas: 3,
	})
	require_NoError(t, err)

	rs := c.randomNonLeader()
	checkFor(t, 2*time.Second, 200*time.Millisecond, func() error {
		if !rs.JetStreamIsStreamAssigned(globalAccountName, "TEST") {
			return errors.New("not assigned")
		}
		return nil
	})

	// The stream is removed, recreated under the same name (new raft group
	// and creation time), and removed again, all while we are catching up.
	sjs := rs.getJetStream()
	sjs.mu.Lock()
	sa := sjs.streamAssignment(globalAccountName, "TEST")
	encodedDelete := encodeDeleteStreamAssignment(sa)
	nsa := sa.copyGroup()
	nsa.Group.Name = groupNameForStream(nsa.Group.Peers, FileStorage)
	nsa.Created = time.Now().UTC()
	encodedAdd := encodeAddStreamAssignment(nsa)
	encodedDeleteRecreated := encodeDeleteStreamAssignment(nsa)
	sjs.mu.Unlock()
	meta := sjs.getMetaGroup().(*raft)
	apply := meta.ApplyQ()

	// Should put us in upper-layer recovery mode.
	// For this test using two large values so we remain in catchup.
	meta.Lock()
	meta.createCatchup(&appendEntry{pterm: 100, pindex: 100})
	meta.sendCatchupSignal()
	meta.Unlock()

	_, err = apply.push(newCommittedEntry(1, []*Entry{
		{EntryNormal, encodedDelete},
		{EntryNormal, encodedAdd},
		{EntryNormal, encodedDeleteRecreated},
	}))
	require_NoError(t, err)
	time.Sleep(200 * time.Millisecond)
	require_True(t, rs.JetStreamIsStreamAssigned(globalAccountName, "TEST"))

	// Canceling the catchup, because it's completed, should result in the staged changes to be applied.
	// The end state is no stream, and specifically not the incarnation we started with.
	meta.Lock()
	meta.cancelCatchup()
	meta.Unlock()
	checkFor(t, 2*time.Second, 200*time.Millisecond, func() error {
		if rs.JetStreamIsStreamAssigned(globalAccountName, "TEST") {
			return errors.New("still assigned")
		}
		if _, err := rs.globalAccount().lookupStream("TEST"); err == nil {
			return errors.New("stream still exists")
		}
		return nil
	})
}

func TestJetStreamClusterRaftCatchupSignalsMetaRecoveryRecreateConsumer(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "R3S", 3)
	defer c.shutdown()

	nc, js := jsClientConnect(t, c.randomServer())
	defer nc.Close()

	_, err := js.AddStream(&nats.StreamConfig{
		Name:     "TEST",
		Subjects: []string{"foo"},
		Replicas: 3,
	})
	require_NoError(t, err)

	sub, err := js.PullSubscribe(_EMPTY_, "CONSUMER", nats.ConsumerReplicas(3), nats.BindStream("TEST"))
	require_NoError(t, err)
	defer sub.Drain()

	_, err = js.Publish("foo", nil)
	require_NoError(t, err)

	msgs, err := sub.Fetch(1)
	require_NoError(t, err)
	require_Len(t, len(msgs), 1)
	require_NoError(t, msgs[0].AckSync())

	rs := c.randomNonLeader()
	sjs := rs.getJetStream()
	checkFor(t, 2*time.Second, 200*time.Millisecond, func() error {
		mset, err := rs.globalAccount().lookupStream("TEST")
		if err != nil {
			return err
		}
		o := mset.lookupConsumer("CONSUMER")
		if o == nil {
			return errors.New("not found")
		}
		state, err := o.store.State()
		if err != nil {
			return err
		}
		if state.Delivered.Stream != 1 || state.AckFloor.Stream != 1 {
			return errors.New("incorrect delivered/ack state")
		}
		return nil
	})

	sjs.mu.Lock()
	ca := sjs.consumerAssignment(globalAccountName, "TEST", "CONSUMER")
	encodedDelete := encodeDeleteConsumerAssignment(ca)
	cca := ca.copyGroup()
	cca.Group.Peers = []string{rs.NodeName()}
	cca.Group.Name = groupNameForConsumer(cca.Group.Peers, FileStorage)
	encodedAdd := encodeAddConsumerAssignment(cca)
	sjs.mu.Unlock()
	meta := sjs.getMetaGroup().(*raft)
	apply := meta.ApplyQ()

	// Should not panic if we receive a nil entry "randomly".
	_, err = apply.push(nil)
	require_NoError(t, err)

	// Should put us in upper-layer recovery mode.
	// For this test using two large values so we remain in catchup.
	meta.Lock()
	meta.createCatchup(&appendEntry{pterm: 100, pindex: 100})
	meta.Unlock()

	// Deleting a consumer should be staged, not immediately performed.
	_, err = apply.push(newCommittedEntry(1, []*Entry{
		{EntryNormal, encodedDelete},
		{EntryNormal, encodedAdd},
	}))
	require_NoError(t, err)

	// Canceling the catchup, because it's completed, should result in the staged changes to be applied.
	meta.Lock()
	meta.cancelCatchup()
	meta.Unlock()
	checkFor(t, 2*time.Second, 200*time.Millisecond, func() error {
		mset, err := rs.globalAccount().lookupStream("TEST")
		if err != nil {
			return err
		}
		o := mset.lookupConsumer("CONSUMER")
		if o == nil {
			return errors.New("not found")
		}
		state, err := o.store.State()
		if err != nil {
			return err
		}
		if state.Delivered.Stream != 0 || state.AckFloor.Stream != 0 {
			return errors.New("old delivered/ack state")
		}
		return nil
	})
}

func TestJetStreamClusterRaftCatchupSignalsMetaRecoveryRecreateConsumerRemoved(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "R3S", 3)
	defer c.shutdown()

	nc, js := jsClientConnect(t, c.randomServer())
	defer nc.Close()

	_, err := js.AddStream(&nats.StreamConfig{
		Name:     "TEST",
		Subjects: []string{"foo"},
		Replicas: 3,
	})
	require_NoError(t, err)

	_, err = js.AddConsumer("TEST", &nats.ConsumerConfig{Durable: "CONSUMER", Replicas: 3})
	require_NoError(t, err)

	rs := c.randomNonLeader()
	sjs := rs.getJetStream()
	assigned := func() bool {
		sjs.mu.RLock()
		defer sjs.mu.RUnlock()
		return sjs.consumerAssignment(globalAccountName, "TEST", "CONSUMER") != nil
	}
	checkFor(t, 2*time.Second, 200*time.Millisecond, func() error {
		if !assigned() {
			return errors.New("not assigned")
		}
		return nil
	})

	// The consumer is removed, recreated under the same name (new raft group
	// and creation time), and removed again, all while we are catching up.
	sjs.mu.Lock()
	ca := sjs.consumerAssignment(globalAccountName, "TEST", "CONSUMER")
	encodedDelete := encodeDeleteConsumerAssignment(ca)
	cca := ca.copyGroup()
	cca.Group.Name = groupNameForConsumer(cca.Group.Peers, FileStorage)
	cca.Created = time.Now().UTC()
	encodedAdd := encodeAddConsumerAssignment(cca)
	encodedDeleteRecreated := encodeDeleteConsumerAssignment(cca)
	sjs.mu.Unlock()
	meta := sjs.getMetaGroup().(*raft)
	apply := meta.ApplyQ()

	// Should put us in upper-layer recovery mode.
	// For this test using two large values so we remain in catchup.
	meta.Lock()
	meta.createCatchup(&appendEntry{pterm: 100, pindex: 100})
	meta.sendCatchupSignal()
	meta.Unlock()

	_, err = apply.push(newCommittedEntry(1, []*Entry{
		{EntryNormal, encodedDelete},
		{EntryNormal, encodedAdd},
		{EntryNormal, encodedDeleteRecreated},
	}))
	require_NoError(t, err)
	time.Sleep(200 * time.Millisecond)
	require_True(t, assigned())

	// Canceling the catchup, because it's completed, should result in the staged changes to be applied.
	// The end state is no consumer, and specifically not the incarnation we started with.
	meta.Lock()
	meta.cancelCatchup()
	meta.Unlock()
	checkFor(t, 2*time.Second, 200*time.Millisecond, func() error {
		if assigned() {
			return errors.New("still assigned")
		}
		mset, err := rs.globalAccount().lookupStream("TEST")
		if err != nil {
			return err
		}
		if mset.lookupConsumer("CONSUMER") != nil {
			return errors.New("consumer still exists")
		}
		return nil
	})
}

func TestJetStreamClusterMetaCatchupRespondsToStagedRequests(t *testing.T) {
	c := createJetStreamClusterWithTemplateAndModHook(t, jsClusterTempl, "R3S", 3,
		func(serverName, _, _, conf string) string {
			return fmt.Sprintf("%s\nserver_tags: [%s]", conf, serverName)
		})
	defer c.shutdown()

	// All assets are R1 on rs, so rs responds for them.
	rs := c.randomNonLeader()
	tags := []string{rs.Name()}
	nc, js := jsClientConnect(t, c.leader())
	defer nc.Close()
	for _, name := range []string{"TEST", "OLD"} {
		_, err := js.AddStream(&nats.StreamConfig{Name: name, Placement: &nats.Placement{Tags: tags}})
		require_NoError(t, err)
	}
	_, err := js.AddConsumer("TEST", &nats.ConsumerConfig{Durable: "OLD"})
	require_NoError(t, err)

	// Put rs in a catchup to an unreachable index, so it ignores new entries. Once stalled for 2s,
	// the leader's next heartbeat makes rs request a real catchup, which delivers the requests below.
	meta := rs.getJetStream().getMetaGroup().(*raft)
	meta.Lock()
	meta.createCatchup(&appendEntry{pterm: 100, pindex: 100})
	meta.Unlock()

	sub := natsSubSync(t, nc, "reply")
	for _, r := range []struct {
		subj string
		req  any
	}{
		{fmt.Sprintf(JSApiStreamCreateT, "NEW"), &StreamConfig{Name: "NEW", Storage: FileStorage, Placement: &Placement{Tags: tags}}},
		{fmt.Sprintf(JSApiDurableCreateT, "TEST", "NEW"), &CreateConsumerRequest{Stream: "TEST", Config: ConsumerConfig{Durable: "NEW"}}},
		{fmt.Sprintf(JSApiStreamDeleteT, "OLD"), nil},
		{fmt.Sprintf(JSApiConsumerDeleteT, "TEST", "OLD"), nil},
	} {
		var b []byte
		if r.req != nil {
			b, err = json.Marshal(r.req)
			require_NoError(t, err)
		}
		require_NoError(t, nc.PublishRequest(r.subj, "reply", b))
	}

	// Wait for the meta leader to apply all requests, so they're all part of the catchup.
	mjs := c.leader().getJetStream()
	checkFor(t, 2*time.Second, 10*time.Millisecond, func() error {
		mjs.mu.RLock()
		defer mjs.mu.RUnlock()
		if mjs.streamAssignment(globalAccountName, "NEW") == nil ||
			mjs.consumerAssignment(globalAccountName, "TEST", "NEW") == nil ||
			mjs.streamAssignment(globalAccountName, "OLD") != nil ||
			mjs.consumerAssignment(globalAccountName, "TEST", "OLD") != nil {
			return errors.New("not applied yet")
		}
		return nil
	})

	for range 4 {
		msg, err := sub.NextMsg(5 * time.Second)
		require_NoError(t, err)
		var resp ApiResponse
		require_NoError(t, json.Unmarshal(msg.Data, &resp))
		require_True(t, resp.Error == nil)
	}
}

func TestJetStreamClusterMetaRecoveryRecreateStream(t *testing.T) {
	test := func(t *testing.T, newStream bool) {
		c := createJetStreamClusterExplicit(t, "R3S", 3)
		defer c.shutdown()

		nc, js := jsClientConnect(t, c.randomServer())
		defer nc.Close()

		// Create a stream containing 5 messages.
		cfg := &nats.StreamConfig{
			Name:     "TEST",
			Subjects: []string{"foo"},
			Replicas: 3,
		}
		_, err := js.AddStream(cfg)
		require_NoError(t, err)
		for range 5 {
			_, err = js.Publish("foo", nil)
			require_NoError(t, err)
		}

		// Select a server that's neither meta nor stream leader.
		var rs *Server
		for _, s := range c.servers {
			if s.JetStreamIsLeader() || s.JetStreamIsStreamLeader(globalAccountName, "TEST") {
				continue
			}
			rs = s
		}

		// When testing old stream state is cleaned up, pause the meta layer now.
		// Otherwise, we'll pause after creating the new stream.
		meta := rs.getJetStream().getMetaGroup()
		if !newStream {
			require_NoError(t, meta.PauseApply())
		}

		// Delete the stream.
		require_NoError(t, js.DeleteStream("TEST"))

		if newStream {
			// Need to wait for the stream to be deleted on our selected server.
			checkFor(t, 2*time.Second, 200*time.Millisecond, func() error {
				if rs.JetStreamIsStreamAssigned(globalAccountName, "TEST") {
					return errors.New("still assigned")
				}
				return nil
			})
		}

		// Recreate the stream with a single message.
		_, err = js.AddStream(cfg)
		require_NoError(t, err)
		_, err = js.Publish("foo", nil)
		require_NoError(t, err)

		if newStream {
			// Wait for the state to be the new state, then pause the meta layer.
			checkFor(t, 2*time.Second, 200*time.Millisecond, func() error {
				return checkState(t, c, globalAccountName, "TEST")
			})
			require_NoError(t, meta.PauseApply())

			// Also create a snapshot so that the stream can't "cheat" by replaying Raft entries.
			mset, err := rs.globalAccount().lookupStream("TEST")
			require_NoError(t, err)
			streamNode := mset.raftNode()
			require_NoError(t, streamNode.InstallSnapshot(mset.stateSnapshot(), true))
		}

		// Although the meta node is paused, we add one more meta entry to move the commit up one more.
		_, err = js.AddStream(&nats.StreamConfig{Name: "RANDOM", Replicas: 3})
		require_NoError(t, err)

		// We'll now restart this server.
		meta.Stop()
		meta.WaitForStop()
		rs.Shutdown()

		if newStream {
			// If we're testing new state, make sure we can't recover from our peers.
			for _, s := range c.servers {
				if s == rs {
					continue
				}
				mset, err := s.globalAccount().lookupStream("TEST")
				require_NoError(t, err)
				n := mset.raftNode()
				n.SetObserver(true)
				n.StepDown()
			}
		}
		rs = c.restartServer(rs)
		c.waitOnServerCurrent(rs)

		// We should arrive at the new state.
		checkFor(t, 2*time.Second, 200*time.Millisecond, func() error {
			mset, err := rs.globalAccount().lookupStream("TEST")
			if err != nil {
				return err
			}
			state := mset.state()
			if state.Msgs == 1 {
				return nil
			}
			return fmt.Errorf("incorrect amount of messages: %d", state.Msgs)
		})
	}

	t.Run("OldStream", func(t *testing.T) { test(t, false) })
	t.Run("NewStream", func(t *testing.T) { test(t, true) })
}

func TestJetStreamClusterMetaRecoveryRecreateConsumer(t *testing.T) {
	test := func(t *testing.T, newConsumer bool) {
		c := createJetStreamClusterExplicit(t, "R3S", 3)
		defer c.shutdown()

		nc, js := jsClientConnect(t, c.randomServer())
		defer nc.Close()

		// Create a stream containing 5 messages.
		_, err := js.AddStream(&nats.StreamConfig{
			Name:     "TEST",
			Subjects: []string{"foo"},
			Replicas: 3,
		})
		require_NoError(t, err)
		for range 5 {
			_, err = js.Publish("foo", nil)
			require_NoError(t, err)
		}

		// Create a consumer and ack all 5 messages.
		cfg := &nats.ConsumerConfig{
			Durable:   "CONSUMER",
			Replicas:  3,
			AckPolicy: nats.AckExplicitPolicy,
		}
		_, err = js.AddConsumer("TEST", cfg)
		require_NoError(t, err)

		sub, err := js.PullSubscribe(_EMPTY_, "CONSUMER", nats.Bind("TEST", "CONSUMER"))
		require_NoError(t, err)
		defer sub.Drain()

		msgs, err := sub.Fetch(5)
		require_NoError(t, err)
		require_Len(t, len(msgs), 5)
		for _, msg := range msgs {
			require_NoError(t, msg.AckSync())
		}

		// Select a server that's neither meta nor consumer leader.
		var rs *Server
		for _, s := range c.servers {
			if s.JetStreamIsLeader() || s.JetStreamIsConsumerLeader(globalAccountName, "TEST", "CONSUMER") {
				continue
			}
			rs = s
		}

		// When testing old consumer state is cleaned up, pause the meta layer now.
		// Otherwise, we'll pause after creating the new consumer.
		meta := rs.getJetStream().getMetaGroup()
		if !newConsumer {
			require_NoError(t, meta.PauseApply())
		}

		// Delete the consumer.
		sub.Drain()
		require_NoError(t, js.DeleteConsumer("TEST", "CONSUMER"))

		if newConsumer {
			// Need to wait for the consumer to be deleted on our selected server.
			checkFor(t, 2*time.Second, 200*time.Millisecond, func() error {
				sjs := rs.getJetStream()
				sjs.mu.RLock()
				ca := sjs.consumerAssignment(globalAccountName, "TEST", "CONSUMER")
				sjs.mu.RUnlock()
				if ca != nil {
					return errors.New("still assigned")
				}
				return nil
			})
		}

		// Recreate the consumer and ack a single message.
		_, err = js.AddConsumer("TEST", cfg)
		require_NoError(t, err)

		sub, err = js.PullSubscribe(_EMPTY_, "CONSUMER", nats.Bind("TEST", "CONSUMER"))
		require_NoError(t, err)
		defer sub.Drain()

		msgs, err = sub.Fetch(1)
		require_NoError(t, err)
		require_Len(t, len(msgs), 1)
		require_NoError(t, msgs[0].AckSync())

		if newConsumer {
			// Wait for the state to be the new state, then pause the meta layer.
			checkFor(t, 2*time.Second, 200*time.Millisecond, func() error {
				for _, s := range c.servers {
					mset, err := s.globalAccount().lookupStream("TEST")
					if err != nil {
						return err
					}
					o := mset.lookupConsumer("CONSUMER")
					if o == nil {
						return errors.New("consumer not found")
					}
					state, err := o.store.State()
					if err != nil {
						return err
					}
					if state.Delivered.Stream != 1 || state.AckFloor.Stream != 1 {
						return errors.New("incorrect delivered/ack state")
					}
				}
				return nil
			})
			require_NoError(t, meta.PauseApply())
		}

		// Although the meta node is paused, we add one more meta entry to move the commit up one more.
		_, err = js.AddStream(&nats.StreamConfig{Name: "RANDOM", Replicas: 3})
		require_NoError(t, err)

		// We'll now restart this server.
		sub.Drain()
		meta.Stop()
		meta.WaitForStop()
		rs.Shutdown()

		if newConsumer {
			// If we're testing new state, make sure we can't recover from our peers.
			for _, s := range c.servers {
				if s == rs {
					continue
				}
				mset, err := s.globalAccount().lookupStream("TEST")
				require_NoError(t, err)
				o := mset.lookupConsumer("CONSUMER")
				require_NotNil(t, o)
				n := o.raftNode()
				n.SetObserver(true)
				n.StepDown()
			}
		}
		rs = c.restartServer(rs)
		c.waitOnServerCurrent(rs)

		// We should arrive at the new state.
		checkFor(t, 2*time.Second, 200*time.Millisecond, func() error {
			mset, err := rs.globalAccount().lookupStream("TEST")
			if err != nil {
				return err
			}
			o := mset.lookupConsumer("CONSUMER")
			if o == nil {
				return errors.New("consumer not found")
			}
			state, err := o.store.State()
			if err != nil {
				return err
			}
			if state.Delivered.Stream != 1 || state.AckFloor.Stream != 1 {
				return errors.New("incorrect delivered/ack state")
			}
			return nil
		})
	}

	t.Run("OldConsumer", func(t *testing.T) { test(t, false) })
	t.Run("NewConsumer", func(t *testing.T) { test(t, true) })
}

func TestJetStreamClusterMetaPeerRemoveResponseAfterQuorum(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "R5S", 5)
	defer c.shutdown()

	ml := c.leader()
	nc, err := nats.Connect(ml.ClientURL(), nats.UserInfo("admin", "s3cr3t!"))
	require_NoError(t, err)
	defer nc.Close()

	var (
		s1 *Server
		s2 *Server
		s3 *Server
	)
	for _, s := range c.servers {
		if s == ml {
			continue
		} else if s1 == nil {
			s1 = s
		} else if s2 == nil {
			s2 = s
		} else {
			s3 = s
			break
		}
	}

	// Shutdown a majority.
	remove1 := s3.Name()
	remove2 := s2.Name()
	s1.Shutdown()
	s2.Shutdown()
	s3.Shutdown()

	reply := nats.NewInbox()
	sub, err := nc.SubscribeSync(reply)
	require_NoError(t, err)
	defer sub.Drain()

	auditSub, err := nc.SubscribeSync(JSAuditAdvisory)
	require_NoError(t, err)
	defer auditSub.Drain()

	// Since a majority is down, we'll expect to time out since there's no quorum.
	req := &JSApiMetaServerRemoveRequest{Server: remove1}
	jsreq, err := json.Marshal(req)
	require_NoError(t, err)
	require_NoError(t, nc.PublishRequest(JSApiRemoveServer, reply, jsreq))
	_, err = sub.NextMsg(time.Second)
	require_Error(t, err, nats.ErrTimeout)

	// Retrying the request should fail, as the leader has already removed it as its peer.
	rmsg, err := nc.Request(JSApiRemoveServer, jsreq, time.Second)
	require_NoError(t, err)
	var resp JSApiMetaServerRemoveResponse
	require_NoError(t, json.Unmarshal(rmsg.Data, &resp))
	if resp.Error == nil {
		t.Fatalf("Expected an error, got none")
	}
	require_Error(t, resp.Error, NewJSClusterServerMemberChangeInflightError())

	// Audit should reflect the same.
	rmsg, err = auditSub.NextMsg(time.Second)
	require_NoError(t, err)
	var auditResp JSAPIAudit
	require_NoError(t, json.Unmarshal(rmsg.Data, &auditResp))
	resp = JSApiMetaServerRemoveResponse{}
	require_NoError(t, json.Unmarshal([]byte(auditResp.Response), &resp))
	if resp.Error == nil {
		t.Fatalf("Expected an error, got none")
	}
	require_Error(t, resp.Error, NewJSClusterServerMemberChangeInflightError())

	// Don't allow concurrent meta membership changes.
	req = &JSApiMetaServerRemoveRequest{Server: remove2}
	jsreq, err = json.Marshal(req)
	require_NoError(t, err)
	rmsg, err = nc.Request(JSApiRemoveServer, jsreq, time.Second)
	require_NoError(t, err)
	resp = JSApiMetaServerRemoveResponse{}
	require_NoError(t, json.Unmarshal(rmsg.Data, &resp))
	if resp.Error == nil {
		t.Fatalf("Expected an error, got none")
	}
	require_Error(t, resp.Error, NewJSClusterServerMemberChangeInflightError())

	// Audit should reflect the same.
	rmsg, err = auditSub.NextMsg(time.Second)
	require_NoError(t, err)
	auditResp = JSAPIAudit{}
	require_NoError(t, json.Unmarshal(rmsg.Data, &auditResp))
	resp = JSApiMetaServerRemoveResponse{}
	require_NoError(t, json.Unmarshal([]byte(auditResp.Response), &resp))
	if resp.Error == nil {
		t.Fatalf("Expected an error, got none")
	}
	require_Error(t, resp.Error, NewJSClusterServerMemberChangeInflightError())

	// Bring back one server so that the peer-remove can get quorum now.
	// The response should come in shortly after.
	c.restartServer(s1)

	c.waitOnLeader()
	require_Equal(t, c.leader().Name(), ml.Name())

	rmsg, err = sub.NextMsg(5 * time.Second)
	require_NoError(t, err)
	resp = JSApiMetaServerRemoveResponse{}
	require_NoError(t, json.Unmarshal(rmsg.Data, &resp))
	if resp.Error != nil {
		require_NoError(t, resp.Error)
	}
	require_True(t, resp.Success)

	// Audit should reflect the same.
	rmsg, err = auditSub.NextMsg(time.Second)
	require_NoError(t, err)
	auditResp = JSAPIAudit{}
	require_NoError(t, json.Unmarshal(rmsg.Data, &auditResp))
	resp = JSApiMetaServerRemoveResponse{}
	require_NoError(t, json.Unmarshal([]byte(auditResp.Response), &resp))
	if resp.Error != nil {
		require_NoError(t, resp.Error)
	}
	require_True(t, resp.Success)

	// A retry of the first peer-remove should return an error that the peer is already removed.
	// Both a "success" response and this error should be used to know the peer-remove was successful.
	req = &JSApiMetaServerRemoveRequest{Server: remove1}
	jsreq, err = json.Marshal(req)
	require_NoError(t, err)
	rmsg, err = nc.Request(JSApiRemoveServer, jsreq, time.Second)
	require_NoError(t, err)
	resp = JSApiMetaServerRemoveResponse{}
	require_NoError(t, json.Unmarshal(rmsg.Data, &resp))
	if resp.Error == nil {
		t.Fatalf("Expected an error, got none")
	}
	require_Error(t, resp.Error, NewJSClusterServerNotMemberError())

	// Audit should reflect the same.
	rmsg, err = auditSub.NextMsg(time.Second)
	require_NoError(t, err)
	auditResp = JSAPIAudit{}
	require_NoError(t, json.Unmarshal(rmsg.Data, &auditResp))
	resp = JSApiMetaServerRemoveResponse{}
	require_NoError(t, json.Unmarshal([]byte(auditResp.Response), &resp))
	if resp.Error == nil {
		t.Fatalf("Expected an error, got none")
	}
	require_Error(t, resp.Error, NewJSClusterServerNotMemberError())
}

func TestJetStreamClusterStreamPeerRemovePeerSetDesync(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "R5S", 5)
	defer c.shutdown()

	nc, js := jsClientConnect(t, c.randomServer())
	defer nc.Close()

	_, err := js.AddStream(&nats.StreamConfig{
		Name:     "TEST",
		Subjects: []string{"foo"},
		Replicas: 3,
	})
	require_NoError(t, err)

	sl := c.streamLeader(globalAccountName, "TEST")
	ml := c.leader()

	// Map peer IDs to servers.
	peerSrv := make(map[string]*Server, len(c.servers))
	for _, s := range c.servers {
		peerSrv[s.Node()] = s
	}

	// Get the stream's current peer set from the meta leader.
	mjs := ml.getJetStream()
	mjs.mu.RLock()
	sa := mjs.streamAssignmentOrInflight(globalAccountName, "TEST")
	var origPeers []string
	if sa != nil {
		origPeers = copyStrings(sa.Group.Peers)
	}
	mjs.mu.RUnlock()
	require_Len(t, len(origPeers), 3)

	// Pick a stream member to peer-remove that is neither the stream leader nor
	// the meta leader. The stream leader must stay in place so it is the one
	// processing the updated assignment, and not picking the meta leader means
	// stalling this server's meta apply queue doesn't stall the proposal itself.
	var rs *Server
	for _, p := range origPeers {
		if s := peerSrv[p]; s != sl && s != ml {
			rs = s
			break
		}
	}
	require_NotNil(t, rs)
	removedID := rs.Node()

	// The stream leader's raft node for the stream group.
	mset, err := sl.globalAccount().lookupStream("TEST")
	require_NoError(t, err)
	srn := mset.raftNode().(*raft)

	// Stall the to-be-removed server's meta apply queue. Its stream raft node
	// keeps running and acking append entries while being unaware of its own
	// removal. This simulates the natural lag between different servers
	// applying the same meta entry.
	rsMeta := rs.getJetStream().getMetaGroup()
	require_NoError(t, rsMeta.PauseApply())
	defer rsMeta.ResumeApply()

	// Peer-remove the selected stream member.
	req := &JSApiStreamRemovePeerRequest{Peer: rs.Name()}
	jsreq, err := json.Marshal(req)
	require_NoError(t, err)
	resp, err := nc.Request(fmt.Sprintf(JSApiStreamRemovePeerT, "TEST"), jsreq, time.Second)
	require_NoError(t, err)
	var rpResp JSApiStreamRemovePeerResponse
	require_NoError(t, json.Unmarshal(resp.Data, &rpResp))
	require_True(t, rpResp.Error == nil)

	// Wait for the stream leader to apply the updated assignment: a replacement
	// peer that was not part of the original peer set must show up in its raft
	// peer set.
	checkFor(t, 2*time.Second, 200*time.Millisecond, func() error {
		srn.RLock()
		raftPeers := srn.peerNames()
		srn.RUnlock()
		for _, p := range raftPeers {
			if !slices.Contains(origPeers, p) {
				return nil
			}
		}
		return fmt.Errorf("stream leader has not applied the new assignment yet, raft peers: %v", raftPeers)
	})

	// Observe the reproduction: the removed peer acks the leader's peer state
	// append entry (or a heartbeat), the leader treats it as an unknown peer
	// and re-adds it via EntryAddPeer. Give it a few heartbeat intervals; on a
	// correctly behaving server this never happens.
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		srn.RLock()
		raftPeers := srn.peerNames()
		srn.RUnlock()
		if len(raftPeers) > 3 {
			t.Logf("Removed peer %q was re-added to the raft group, peer set is now: %v", removedID, raftPeers)
			break
		}
		time.Sleep(50 * time.Millisecond)
	}

	// Let the removed server process its removal and delete its stream raft node.
	rsMeta.ResumeApply()
	checkFor(t, 2*time.Second, 200*time.Millisecond, func() error {
		if _, err = rs.globalAccount().lookupStream("TEST"); err == nil {
			return fmt.Errorf("removed server %s still has the stream", rs.Name())
		}
		return nil
	})

	// The raft layer peer set on all remaining stream members must converge
	// to the meta layer peer set: 3 peers, not including the removed peer.
	checkFor(t, 5*time.Second, 200*time.Millisecond, func() error {
		// Meta layer peer set, according to the meta leader.
		mjs.mu.RLock()
		sa = mjs.streamAssignment(globalAccountName, "TEST")
		var metaPeers []string
		if sa != nil {
			metaPeers = copyStrings(sa.Group.Peers)
		}
		mjs.mu.RUnlock()
		if len(metaPeers) != 3 {
			return fmt.Errorf("expected 3 peers in meta assignment, got: %v", metaPeers)
		}
		slices.Sort(metaPeers)

		// Each member's raft-layer peer set must match it.
		for _, p := range metaPeers {
			s := peerSrv[p]
			mset, err = s.globalAccount().lookupStream("TEST")
			if err != nil {
				return fmt.Errorf("stream not found on %s: %v", s.Name(), err)
			}
			node := mset.raftNode()
			if node == nil {
				return fmt.Errorf("no raft node for stream on %s", s.Name())
			}
			rn := node.(*raft)
			rn.RLock()
			raftPeers := rn.peerNames()
			rn.RUnlock()
			slices.Sort(raftPeers)
			if !slices.Equal(metaPeers, raftPeers) {
				return fmt.Errorf("%s: raft peer set %v desynced from meta peer set %v, contains removed peer %q: %v",
					s.Name(), raftPeers, metaPeers, removedID, slices.Contains(raftPeers, removedID))
			}
		}
		return nil
	})
}

func TestJetStreamClusterMalformedEntrySetsWriteErr(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "R3S", 3)
	defer c.shutdown()

	nc, js := jsClientConnect(t, c.randomServer())
	defer nc.Close()

	_, err := js.AddStream(&nats.StreamConfig{
		Name:     "TEST",
		Subjects: []string{"foo"},
		Replicas: 3,
	})
	require_NoError(t, err)

	// Confirm the stream is healthy and has no write error to begin with.
	_, err = js.Publish("foo", []byte("ok"))
	require_NoError(t, err)

	sl := c.streamLeader(globalAccountName, "TEST")
	mset, err := sl.globalAccount().lookupStream("TEST")
	require_NoError(t, err)
	require_NoError(t, mset.getWriteErr())

	// Craft a malformed streamMsgOp, this must surface as a stream write error.
	bad := append([]byte{byte(streamMsgOp)}, 1, 2, 3)
	n := mset.raftNode().(*raft)
	n.sendAppendEntry([]*Entry{newEntry(EntryNormal, bad)})

	checkFor(t, 2*time.Second, 50*time.Millisecond, func() error {
		werr := mset.getWriteErr()
		if werr == nil {
			return fmt.Errorf("stream write error not set yet")
		}
		require_Error(t, werr, errBadStreamMsg)
		return nil
	})

	// Sanity: the server must still be running (no panic took it down).
	require_True(t, sl.Running())
}

func TestJetStreamClusterMalformedConsumerEntrySetsWriteErr(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "R3S", 3)
	defer c.shutdown()

	nc, js := jsClientConnect(t, c.randomServer())
	defer nc.Close()

	_, err := js.AddStream(&nats.StreamConfig{
		Name:     "TEST",
		Subjects: []string{"foo"},
		Replicas: 3,
	})
	require_NoError(t, err)

	_, err = js.AddConsumer("TEST", &nats.ConsumerConfig{
		Durable:   "CONS",
		AckPolicy: nats.AckExplicitPolicy,
		Replicas:  3,
	})
	require_NoError(t, err)

	// Confirm the consumer is healthy and has no write error to begin with.
	cl := c.consumerLeader(globalAccountName, "TEST", "CONS")
	mset, err := cl.globalAccount().lookupStream("TEST")
	require_NoError(t, err)
	o := mset.lookupConsumer("CONS")
	require_True(t, o != nil)
	require_NoError(t, o.getWriteErr())

	cjs := cl.getJetStream()
	ca := o.consumerAssignment()
	require_NoError(t, cjs.isConsumerHealthy(mset, nil, "CONS", ca))

	// Craft a malformed consumer entry using an unknown op, this must surface as a consumer write error.
	bad := []byte{255, 1, 2, 3}
	n := o.raftNode().(*raft)
	n.sendAppendEntry([]*Entry{newEntry(EntryNormal, bad)})

	checkFor(t, 2*time.Second, 50*time.Millisecond, func() error {
		werr := o.getWriteErr()
		if werr == nil {
			return fmt.Errorf("consumer write error not set yet")
		}
		// Healthz must now reflect the broken state.
		if err := cjs.isConsumerHealthy(mset, nil, "CONS", ca); err == nil {
			return fmt.Errorf("consumer still reported healthy")
		}
		return nil
	})

	// Sanity: the server must still be running (no panic took it down).
	require_True(t, cl.Running())
}

func TestJetStreamClusterMalformedMetaEntrySetsWriteErr(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "R3S", 3)
	defer c.shutdown()

	// Confirm the meta group is healthy and has no write error to begin with.
	ml := c.leader()
	require_True(t, ml != nil)
	mjs := ml.getJetStream()
	require_True(t, mjs != nil)
	require_NoError(t, mjs.getMetaWriteErr())
	require_Equal(t, ml.healthz(nil).Error, _EMPTY_)

	// Craft a malformed meta entry using an unknown op, this must surface as a meta write error.
	bad := []byte{255, 1, 2, 3}
	n := mjs.getMetaGroup().(*raft)
	n.sendAppendEntry([]*Entry{newEntry(EntryNormal, bad)})

	checkFor(t, 2*time.Second, 50*time.Millisecond, func() error {
		werr := mjs.getMetaWriteErr()
		if werr == nil {
			return fmt.Errorf("meta write error not set yet")
		}
		// Healthz must now reflect the broken state.
		if hs := ml.healthz(nil); hs.Error == _EMPTY_ {
			return fmt.Errorf("meta group still reported healthy")
		}
		return nil
	})

	// Sanity: the server must still be running (no panic took it down).
	require_True(t, ml.Running())
}

func TestJetStreamClusterConsumerAutoScaleWithStream(t *testing.T) {
	for _, test := range []struct {
		name      string
		cfg       *nats.ConsumerConfig
		autoScale bool // when the consumer is expected to follow the stream's replica count
	}{
		{
			name: "EphemeralAutoName",
			cfg: &nats.ConsumerConfig{
				AckPolicy:         nats.AckExplicitPolicy,
				InactiveThreshold: time.Minute,
			},
			autoScale: false,
		},
		{
			name: "EphemeralProvidedName",
			cfg: &nats.ConsumerConfig{
				Name:              "EPH",
				AckPolicy:         nats.AckExplicitPolicy,
				InactiveThreshold: time.Minute,
			},
			autoScale: false,
		},
		{
			name: "Durable",
			cfg: &nats.ConsumerConfig{
				Durable:   "DUR",
				AckPolicy: nats.AckExplicitPolicy,
			},
			autoScale: true,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := createJetStreamClusterExplicit(t, "R3S", 3)
			defer c.shutdown()

			nc, js := jsClientConnect(t, c.randomServer())
			defer nc.Close()

			// Start the stream out at R1.
			cfg := &nats.StreamConfig{
				Name:     "TEST",
				Subjects: []string{"foo"},
				Replicas: 1,
			}
			_, err := js.AddStream(cfg)
			require_NoError(t, err)

			ci, err := js.AddConsumer("TEST", test.cfg)
			require_NoError(t, err)
			cname := ci.Name
			require_NotEqual(t, cname, _EMPTY_)

			observedReplicas := func() (int, error) {
				ci, err := js.ConsumerInfo("TEST", cname, nats.MaxWait(time.Second))
				if err != nil {
					return 0, err
				}
				if ci.Cluster == nil || ci.Cluster.Leader == _EMPTY_ {
					return 0, fmt.Errorf("no consumer leader yet")
				}
				return len(ci.Cluster.Replicas) + 1, nil
			}

			scaleAndCheck := func(replicas int) {
				t.Helper()
				cfg.Replicas = replicas
				_, err := js.UpdateStream(cfg)
				require_NoError(t, err)

				wantR := 1
				if test.autoScale {
					wantR = replicas
				}

				// Ephemeral consumers must never follow the stream's replica count, not even transiently.
				if !test.autoScale {
					until := time.Now().Add(500 * time.Millisecond)
					for time.Now().Before(until) {
						if gotR, err := observedReplicas(); err == nil && gotR > 1 {
							t.Fatalf("stream R%d: %s consumer %q scaled to R%d, expected to stay R1",
								replicas, test.name, cname, gotR)
						}
						time.Sleep(100 * time.Millisecond)
					}
				}

				// Every consumer must converge to and hold the expected replica count.
				c.waitOnStreamLeader(globalAccountName, "TEST")
				checkFor(t, 5*time.Second, 100*time.Millisecond, func() error {
					gotR, err := observedReplicas()
					if err != nil {
						return err
					}
					if gotR != wantR {
						return fmt.Errorf("stream R%d: expected consumer R%d, got R%d", replicas, wantR, gotR)
					}
					return nil
				})
				c.waitOnConsumerLeader(globalAccountName, "TEST", cname)
			}

			// Scale up and back down.
			for _, streamR := range []int{1, 2, 3, 2, 1} {
				scaleAndCheck(streamR)
			}
		})
	}
}

func TestJetStreamClusterConsumerNoAutoScaleOnRetentionChange(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "R5S", 5)
	defer c.shutdown()

	nc, js := jsClientConnect(t, c.randomServer())
	defer nc.Close()

	// Interest retention stream at R3. Under interest policy consumers keep peer
	// parity with the stream, so the legacy ephemeral starts out at R3.
	cfg := &nats.StreamConfig{
		Name:      "TEST",
		Subjects:  []string{"foo"},
		Retention: nats.InterestPolicy,
		Replicas:  3,
	}
	_, err := js.AddStream(cfg)
	require_NoError(t, err)

	// Legacy ephemeral, presents as R=0.
	ci, err := js.AddConsumer("TEST", &nats.ConsumerConfig{
		AckPolicy:         nats.AckExplicitPolicy,
		InactiveThreshold: time.Minute,
	})
	require_NoError(t, err)
	cname := ci.Name
	require_NotEqual(t, cname, _EMPTY_)

	observedReplicas := func() (int, error) {
		ci, err := js.ConsumerInfo("TEST", cname, nats.MaxWait(time.Second))
		if err != nil {
			return 0, err
		}
		if ci.Cluster == nil || ci.Cluster.Leader == _EMPTY_ {
			return 0, fmt.Errorf("no consumer leader yet")
		}
		return len(ci.Cluster.Replicas) + 1, nil
	}
	// Confirm the consumer follows the interest stream up to R3 to begin with.
	gotR, err := observedReplicas()
	require_NoError(t, err)
	require_Equal(t, gotR, 3)

	// Switch to limits retention while also scaling the stream up to R5.
	// The ephemeral must not be auto scaled to follow the stream's replica count,
	// not even transiently. Switching the retention policy is not an auto scale event.
	cfg.Retention = nats.LimitsPolicy
	cfg.Replicas = 5
	_, err = js.UpdateStream(cfg)
	require_NoError(t, err)
	c.waitOnStreamLeader(globalAccountName, "TEST")

	start := time.Now()
	checkFor(t, 5*time.Second, 100*time.Millisecond, func() error {
		gotR, err := observedReplicas()
		if err != nil {
			return err
		}
		if gotR > 3 {
			t.Fatalf("ephemeral consumer %q scaled to R%d, expected to never follow the stream up to R5",
				cname, gotR)
		}
		if gotR != 1 {
			return fmt.Errorf("expected ephemeral consumer to downscale to R1, got R%d", gotR)
		}
		if elapsed := time.Since(start); elapsed < 2*time.Second {
			return fmt.Errorf("at R1, still observing for the full window (%v elapsed)", elapsed.Round(time.Millisecond))
		}
		return nil
	})
}

func TestJetStreamClusterConsumerScaleOnRetentionChange(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "R3S", 3)
	defer c.shutdown()

	nc, js := jsClientConnect(t, c.randomServer())
	defer nc.Close()

	// Interest retention stream at R3. Under interest policy consumers keep peer
	// parity with the stream, so the legacy ephemeral starts out at R3.
	cfg := &nats.StreamConfig{
		Name:      "TEST",
		Subjects:  []string{"foo"},
		Retention: nats.InterestPolicy,
		Replicas:  3,
	}
	_, err := js.AddStream(cfg)
	require_NoError(t, err)

	// Legacy ephemeral, presents as R=0.
	ci, err := js.AddConsumer("TEST", &nats.ConsumerConfig{
		AckPolicy:         nats.AckExplicitPolicy,
		InactiveThreshold: time.Minute,
	})
	require_NoError(t, err)
	cname := ci.Name
	require_NotEqual(t, cname, _EMPTY_)

	observedReplicas := func() (int, error) {
		ci, err := js.ConsumerInfo("TEST", cname, nats.MaxWait(time.Second))
		if err != nil {
			return 0, err
		}
		if ci.Cluster == nil || ci.Cluster.Leader == _EMPTY_ {
			return 0, fmt.Errorf("no consumer leader yet")
		}
		return len(ci.Cluster.Replicas) + 1, nil
	}
	// Confirm the consumer follows the interest stream up to R3 to begin with.
	gotR, err := observedReplicas()
	require_NoError(t, err)
	require_Equal(t, gotR, 3)

	updateRetentionAndCheck := func(retention nats.RetentionPolicy, wantR int) {
		t.Helper()
		cfg.Retention = retention
		_, err = js.UpdateStream(cfg)
		require_NoError(t, err)

		checkFor(t, 5*time.Second, 100*time.Millisecond, func() error {
			gotR, err := observedReplicas()
			if err != nil {
				return err
			}
			if gotR != wantR {
				return fmt.Errorf("expected ephemeral consumer at R%d, got R%d", wantR, gotR)
			}
			return nil
		})
	}

	// Switching to limits retention must downscale the legacy ephemeral to R1,
	// switching back to interest retention must restore peer parity with the stream.
	updateRetentionAndCheck(nats.LimitsPolicy, 1)
	updateRetentionAndCheck(nats.InterestPolicy, 3)
}

func TestJetStreamClusterRetentionChangeWaitsForConsumerParity(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "R3S", 3)
	defer c.shutdown()

	nc, js := jsClientConnect(t, c.randomServer())
	defer nc.Close()

	// Limits retention at R3, so the legacy ephemeral starts out at R1.
	cfg := &nats.StreamConfig{
		Name:      "TEST",
		Subjects:  []string{"foo"},
		Retention: nats.LimitsPolicy,
		Replicas:  3,
	}
	_, err := js.AddStream(cfg)
	require_NoError(t, err)

	ci, err := js.AddConsumer("TEST", &nats.ConsumerConfig{
		AckPolicy:         nats.AckExplicitPolicy,
		InactiveThreshold: time.Minute,
	})
	require_NoError(t, err)
	cname := ci.Name
	require_NotEqual(t, cname, _EMPTY_)

	// How many peers the consumer is assigned to, according to the meta leader.
	consumerPeers := func() int {
		ml := c.leader()
		if ml == nil {
			return 0
		}
		mjs := ml.getJetStream()
		mjs.mu.RLock()
		defer mjs.mu.RUnlock()
		ca := mjs.consumerAssignmentOrInflight(globalAccountName, "TEST", cname)
		if ca == nil || ca.Group == nil {
			return 0
		}
		return len(ca.Group.Peers)
	}
	require_Equal(t, consumerPeers(), 1)

	cfg.Retention = nats.InterestPolicy
	_, err = js.UpdateStream(cfg)
	require_NoError(t, err)

	// The consumer must reach peer parity with the stream BEFORE any server starts applying interest
	// retention. A stream peer that runs interest retention without hosting a consumer replica would
	// compute interest over an incomplete consumer set and remove messages that are still of interest.
	deadline := time.Now().Add(5 * time.Second)
	for {
		interest := 0
		for _, s := range c.servers {
			mset, err := s.globalAccount().lookupStream("TEST")
			if err != nil || mset == nil || !mset.isInterestRetention() {
				continue
			}
			interest++
			// Re-read here, the consumer could have converged in the meantime.
			if peers := consumerPeers(); peers < 3 {
				t.Fatalf("Server %q applied interest retention while consumer was only on %d peer(s)", s.Name(), peers)
			}
		}
		if interest == len(c.servers) && consumerPeers() == 3 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("Timed out with %d/%d servers on interest retention and consumer on %d peer(s)",
				interest, len(c.servers), consumerPeers())
		}
		time.Sleep(2 * time.Millisecond)
	}
}

// A retention change that leaves no consumer to remap must apply in one go, without desired state.
func TestJetStreamClusterRetentionChangeWithoutRemapAppliesImmediately(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "R3S", 3)
	defer c.shutdown()

	ml := c.leader()
	nc, js := jsClientConnect(t, ml)
	defer nc.Close()

	cfg := &nats.StreamConfig{
		Name:      "TEST",
		Subjects:  []string{"foo"},
		Retention: nats.LimitsPolicy,
		Replicas:  3,
	}
	_, err := js.AddStream(cfg)
	require_NoError(t, err)

	// Durables at parity with the stream, one by default and one explicitly.
	_, err = js.AddConsumer("TEST", &nats.ConsumerConfig{Durable: "DEFAULT", AckPolicy: nats.AckExplicitPolicy})
	require_NoError(t, err)
	_, err = js.AddConsumer("TEST", &nats.ConsumerConfig{Durable: "EXPLICIT", AckPolicy: nats.AckExplicitPolicy, Replicas: 3})
	require_NoError(t, err)

	// Stop the meta leader from reconciling desired state. A retention change that registers
	// desired state can then never converge, so only a change applied in one go reaches the members.
	mjs := ml.getJetStream()
	mjs.mu.Lock()
	streamReconcile := mjs.cluster.streamReconcile
	mjs.cluster.streamReconcile = nil
	mjs.mu.Unlock()
	require_NotNil(t, streamReconcile)
	ml.sysUnsubscribe(streamReconcile)

	// Waits for the meta leader to have applied the change into retention, and reports whether
	// the assignment still has desired state pending at that point.
	metaApplied := func(t *testing.T, retention RetentionPolicy) (pending bool) {
		t.Helper()
		checkFor(t, 5*time.Second, 50*time.Millisecond, func() error {
			require_True(t, ml == c.leader())
			mjs.mu.RLock()
			defer mjs.mu.RUnlock()
			sa := mjs.streamAssignment(globalAccountName, "TEST")
			if sa == nil {
				return fmt.Errorf("stream assignment not found")
			}
			if sa.Config.Retention != retention {
				return fmt.Errorf("meta leader at %v, expected %v", sa.Config.Retention, retention)
			}
			pending = sa.Group.Desired != nil
			return nil
		})
		return pending
	}
	membersAt := func(retention RetentionPolicy) error {
		for _, s := range c.servers {
			mset, err := s.globalAccount().lookupStream("TEST")
			if err != nil {
				return err
			}
			if r := mset.config().Retention; r != retention {
				return fmt.Errorf("server %q at %v, expected %v", s.Name(), r, retention)
			}
		}
		return nil
	}
	requireApplied := func(t *testing.T, retention RetentionPolicy) {
		t.Helper()
		require_False(t, metaApplied(t, retention))
		checkFor(t, 5*time.Second, 100*time.Millisecond, func() error { return membersAt(retention) })
		// The consumers must have been left alone.
		for _, name := range []string{"DEFAULT", "EXPLICIT"} {
			mjs.mu.RLock()
			ca := mjs.consumerAssignment(globalAccountName, "TEST", name)
			mjs.mu.RUnlock()
			require_NotNil(t, ca)
			require_Len(t, len(ca.Group.Peers), 3)
			require_True(t, ca.Group.Desired == nil)
		}
	}

	cfg.Retention = nats.InterestPolicy
	_, err = js.UpdateStream(cfg)
	require_NoError(t, err)
	requireApplied(t, InterestPolicy)

	// Going back to Limits needs no consumer scaled down either.
	cfg.Retention = nats.LimitsPolicy
	_, err = js.UpdateStream(cfg)
	require_NoError(t, err)
	requireApplied(t, LimitsPolicy)

	// But a consumer that must be scaled up first still forces the change through desired
	// state, keeping the members on Limits until it has parity.
	_, err = js.AddConsumer("TEST", &nats.ConsumerConfig{Durable: "R1", AckPolicy: nats.AckExplicitPolicy, Replicas: 1})
	require_NoError(t, err)
	cfg.Retention = nats.InterestPolicy
	_, err = js.UpdateStream(cfg)
	require_NoError(t, err)
	require_True(t, metaApplied(t, InterestPolicy))
	require_NoError(t, membersAt(LimitsPolicy))

	// Unblock reconciling, now the change converges the usual way.
	require_True(t, ml == c.leader())
	mjs.mu.Lock()
	mjs.startUpdatesSub()
	mjs.mu.Unlock()
	checkFor(t, 10*time.Second, 100*time.Millisecond, func() error {
		mjs.mu.RLock()
		sa := mjs.streamAssignment(globalAccountName, "TEST")
		ca := mjs.consumerAssignment(globalAccountName, "TEST", "R1")
		mjs.mu.RUnlock()
		if sa == nil || sa.Group.Desired != nil {
			return fmt.Errorf("desired state still pending")
		}
		if ca == nil || len(ca.Group.Peers) != 3 || ca.Group.Desired != nil {
			return fmt.Errorf("consumer not at parity yet")
		}
		return membersAt(InterestPolicy)
	})
}

func TestJetStreamClusterRetentionChangeOriginSurvivesScale(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "R5S", 5)
	defer c.shutdown()

	nc, js := jsClientConnect(t, c.randomServer())
	defer nc.Close()

	cfg := &nats.StreamConfig{
		Name:      "TEST",
		Subjects:  []string{"foo"},
		Retention: nats.LimitsPolicy,
		Replicas:  3,
	}
	_, err := js.AddStream(cfg)
	require_NoError(t, err)

	// Legacy ephemeral, presents as R=0 and sits at R1 under limits retention.
	ci, err := js.AddConsumer("TEST", &nats.ConsumerConfig{
		AckPolicy:         nats.AckExplicitPolicy,
		InactiveThreshold: time.Minute,
	})
	require_NoError(t, err)
	cname := ci.Name
	require_NotEqual(t, cname, _EMPTY_)

	// The peers assigned to the stream and to the consumer, according to the meta leader.
	assignedPeers := func() (speers, cpeers []string) {
		ml := c.leader()
		if ml == nil {
			return nil, nil
		}
		mjs := ml.getJetStream()
		mjs.mu.RLock()
		defer mjs.mu.RUnlock()
		if sa := mjs.streamAssignmentOrInflight(globalAccountName, "TEST"); sa != nil && sa.Group != nil {
			speers = sa.Group.Peers
		}
		if ca := mjs.consumerAssignmentOrInflight(globalAccountName, "TEST", cname); ca != nil && ca.Group != nil {
			cpeers = ca.Group.Peers
		}
		return speers, cpeers
	}
	speers, cpeers := assignedPeers()
	require_Equal(t, len(speers), 3)
	require_Equal(t, len(cpeers), 1)

	// Block the consumer from finishing its scale-up by stopping the meta leader from
	// reconciling consumer assignments.
	ml := c.leader()
	mjs := ml.getJetStream()
	mjs.mu.Lock()
	consumerReconcile := mjs.cluster.consumerReconcile
	mjs.cluster.consumerReconcile = nil
	mjs.mu.Unlock()
	require_NotNil(t, consumerReconcile)
	ml.sysUnsubscribe(consumerReconcile)

	// Switch to interest retention. The consumer needs to reach peer parity before it can apply,
	// so the old retention is held in the desired state's origin while the consumer scales up.
	cfg.Retention = nats.InterestPolicy
	_, err = js.UpdateStream(cfg)
	require_NoError(t, err)

	// While that is still converging the stream has desired state. A scale stacks on top,
	// retargeting the peer set while the pending retention origin must be preserved.
	cfg.Replicas = 5
	_, err = js.UpdateStream(cfg)
	require_NoError(t, err)

	// Unblock the consumer scale-up so the stream can converge.
	require_True(t, ml == c.leader())
	mjs.mu.Lock()
	mjs.startUpdatesSub()
	mjs.mu.Unlock()

	c.waitOnStreamLeader("$G", "TEST")

	// The scale must land on top of the applied retention change, not roll it back to the
	// origin's limits retention, and the consumer must follow the stream to the new peer set.
	checkFor(t, 5*time.Second, 10*time.Millisecond, func() error {
		interest := 0
		for _, s := range c.servers {
			mset, err := s.GlobalAccount().lookupStream("TEST")
			if err != nil || mset == nil || !mset.isInterestRetention() {
				continue
			}
			interest++
		}
		speers, cpeers := assignedPeers()
		if interest != len(c.servers) || len(speers) != 5 || len(cpeers) != 5 {
			return fmt.Errorf("have %d/%d servers on interest retention, stream peers %v, consumer peers %v",
				interest, len(c.servers), speers, cpeers)
		}
		return nil
	})
}

func TestJetStreamClusterScaleStackingMoveExclusion(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "R5S", 5)
	defer c.shutdown()

	nc, js := jsClientConnect(t, c.randomServer())
	defer nc.Close()

	cfg := &nats.StreamConfig{
		Name:     "TEST",
		Subjects: []string{"foo"},
		Replicas: 3,
	}
	_, err := js.AddStream(cfg)
	require_NoError(t, err)

	// A move and a scale can't combine in a single update, even with nothing in flight.
	// The guard fires before peer selection, so the placement doesn't need to resolve.
	comboCfg := *cfg
	comboCfg.Replicas = 5
	comboCfg.Placement = &nats.Placement{Tags: []string{"na"}}
	_, err = js.UpdateStream(&comboCfg)
	require_Error(t, err, NewJSStreamMoveAndScaleError())

	// Block the meta leader from reconciling desired stream assignments, so the
	// desired state below stays in flight deterministically.
	ml := c.leader()
	mjs := ml.getJetStream()
	mjs.mu.Lock()
	streamReconcile := mjs.cluster.streamReconcile
	mjs.cluster.streamReconcile = nil
	mjs.mu.Unlock()
	require_NotNil(t, streamReconcile)
	ml.sysUnsubscribe(streamReconcile)

	// Scale up, it stays converging while reconciliation is blocked.
	cfg.Replicas = 5
	_, err = js.UpdateStream(cfg)
	require_NoError(t, err)

	// A move can't combine with the in-flight scale.
	moveCfg := *cfg
	moveCfg.Placement = &nats.Placement{Tags: []string{"na"}}
	_, err = js.UpdateStream(&moveCfg)
	require_Error(t, err, NewJSStreamReconfigureInProgressError())

	// Another scale stacks onto the in-flight one, retargeting the desired state while
	// the origin from before the first scale is preserved.
	cfg.Replicas = 4
	_, err = js.UpdateStream(cfg)
	require_NoError(t, err)

	mjs.mu.RLock()
	var replicas, originReplicas int
	var hasDesired bool
	if sa := mjs.streamAssignmentOrInflight(globalAccountName, "TEST"); sa != nil && sa.Config != nil && sa.Group != nil {
		replicas = sa.Config.Replicas
		if sa.Group.Desired != nil {
			hasDesired = true
			if sa.Group.Desired.Origin != nil {
				originReplicas = sa.Group.Desired.Origin.Replicas
			}
		}
	}
	mjs.mu.RUnlock()
	require_Equal(t, replicas, 4)
	require_True(t, hasDesired)
	require_Equal(t, originReplicas, 3)

	// Unblock reconciliation, the stacked scale must converge.
	require_True(t, ml == c.leader())
	mjs.mu.Lock()
	mjs.startUpdatesSub()
	mjs.mu.Unlock()

	c.waitOnStreamLeader(globalAccountName, "TEST")
	checkFor(t, 10*time.Second, 100*time.Millisecond, func() error {
		mjs.mu.RLock()
		defer mjs.mu.RUnlock()
		sa := mjs.streamAssignment(globalAccountName, "TEST")
		if sa == nil || sa.Group == nil {
			return fmt.Errorf("no stream assignment")
		}
		if sa.Group.Desired != nil {
			return fmt.Errorf("desired state still in flight")
		}
		if len(sa.Group.Peers) != 4 {
			return fmt.Errorf("expected 4 peers, got %d", len(sa.Group.Peers))
		}
		return nil
	})
}

func TestJetStreamClusterRetentionChangeMoveExclusion(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "R3S", 3)
	defer c.shutdown()

	nc, js := jsClientConnect(t, c.randomServer())
	defer nc.Close()

	cfg := &nats.StreamConfig{
		Name:      "TEST",
		Subjects:  []string{"foo"},
		Retention: nats.LimitsPolicy,
		Replicas:  3,
	}
	_, err := js.AddStream(cfg)
	require_NoError(t, err)

	// An R1 consumer must be scaled up before the stream can become Interest, so the
	// retention change below has to go through desired state.
	_, err = js.AddConsumer("TEST", &nats.ConsumerConfig{Durable: "C", AckPolicy: nats.AckExplicitPolicy, Replicas: 1})
	require_NoError(t, err)

	// A retention change converges through desired state just like a scale does, so it
	// can't combine with a move in a single update either. The guard fires before peer
	// selection, so the placement doesn't need to resolve.
	comboCfg := *cfg
	comboCfg.Retention = nats.InterestPolicy
	comboCfg.Placement = &nats.Placement{Tags: []string{"na"}}
	_, err = js.UpdateStream(&comboCfg)
	require_Error(t, err, NewJSStreamMoveAndScaleError())

	// Block the meta leader from reconciling desired stream assignments, so the
	// desired state below stays in flight deterministically.
	ml := c.leader()
	mjs := ml.getJetStream()
	mjs.mu.Lock()
	streamReconcile := mjs.cluster.streamReconcile
	mjs.cluster.streamReconcile = nil
	mjs.mu.Unlock()
	require_NotNil(t, streamReconcile)
	ml.sysUnsubscribe(streamReconcile)

	// A retention change that must scale a consumer first goes through desired state,
	// it stays converging while reconciliation is blocked. No stream scale is involved.
	cfg.Retention = nats.InterestPolicy
	_, err = js.UpdateStream(cfg)
	require_NoError(t, err)

	mjs.mu.RLock()
	sa := mjs.streamAssignmentOrInflight(globalAccountName, "TEST")
	var hasDesired, isMove bool
	var originReplicas int
	if sa != nil && sa.Group != nil && sa.Group.Desired != nil {
		hasDesired = true
		isMove = sa.Group.Desired.Move
		if sa.Group.Desired.Origin != nil {
			originReplicas = sa.Group.Desired.Origin.Replicas
		}
	}
	mjs.mu.RUnlock()
	require_True(t, hasDesired)
	// Confirm it is not a move, and the replicas are untouched so it is not a scale either.
	require_False(t, isMove)
	require_Equal(t, originReplicas, 3)

	// A move can't combine with the in-flight retention change. The error must reflect
	// that a reconfiguration is in flight, reporting a scale here would be wrong.
	moveCfg := *cfg
	moveCfg.Placement = &nats.Placement{Tags: []string{"na"}}
	_, err = js.UpdateStream(&moveCfg)
	require_Error(t, err, NewJSStreamReconfigureInProgressError())

	// Unblock reconciliation, the retention change must converge.
	require_True(t, ml == c.leader())
	mjs.mu.Lock()
	mjs.startUpdatesSub()
	mjs.mu.Unlock()

	c.waitOnStreamLeader(globalAccountName, "TEST")
	checkFor(t, 10*time.Second, 100*time.Millisecond, func() error {
		mjs.mu.RLock()
		defer mjs.mu.RUnlock()
		sa := mjs.streamAssignment(globalAccountName, "TEST")
		if sa == nil || sa.Group == nil || sa.Config == nil {
			return fmt.Errorf("no stream assignment")
		}
		if sa.Group.Desired != nil {
			return fmt.Errorf("desired state still in flight")
		}
		if sa.Config.Retention != InterestPolicy {
			return fmt.Errorf("expected interest retention, got %v", sa.Config.Retention)
		}
		return nil
	})
}

func TestJetStreamClusterStreamPeerRemoveConsumerRemap(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "R5S", 5)
	defer c.shutdown()

	nc, js := jsClientConnect(t, c.randomServer())
	defer nc.Close()

	_, err := js.AddStream(&nats.StreamConfig{
		Name:     "TEST",
		Subjects: []string{"foo"},
		Replicas: 3,
	})
	require_NoError(t, err)

	// An R1 durable, its single peer should be left alone if it's not the peer being removed.
	_, err = js.AddConsumer("TEST", &nats.ConsumerConfig{
		Durable:   "R1",
		AckPolicy: nats.AckExplicitPolicy,
		Replicas:  1,
	})
	require_NoError(t, err)

	// An R3 durable, it should be remapped along with the stream.
	_, err = js.AddConsumer("TEST", &nats.ConsumerConfig{
		Durable:   "R3",
		AckPolicy: nats.AckExplicitPolicy,
	})
	require_NoError(t, err)

	// An ephemeral, it should survive as long as its peer is not the one being removed.
	sub, err := js.SubscribeSync("foo")
	require_NoError(t, err)
	defer sub.Unsubscribe()
	eci, err := sub.ConsumerInfo()
	require_NoError(t, err)
	ephemeral, ephemeralPeer := eci.Name, eci.Cluster.Leader

	ci, err := js.ConsumerInfo("TEST", "R1")
	require_NoError(t, err)
	require_Len(t, len(ci.Cluster.Replicas), 0)
	r1Peer := ci.Cluster.Leader

	// Remove a stream peer that hosts neither the R1 durable nor the ephemeral.
	si, err := js.StreamInfo("TEST")
	require_NoError(t, err)
	peers := []string{si.Cluster.Leader}
	for _, r := range si.Cluster.Replicas {
		peers = append(peers, r.Name)
	}
	var toRemove string
	for _, p := range peers {
		if p != r1Peer && p != ephemeralPeer {
			toRemove = p
			break
		}
	}
	require_NotEqual(t, toRemove, _EMPTY_)

	b, err := json.Marshal(JSApiStreamRemovePeerRequest{Peer: toRemove})
	require_NoError(t, err)
	msg, err := nc.Request(fmt.Sprintf(JSApiStreamRemovePeerT, "TEST"), b, time.Second)
	require_NoError(t, err)
	var resp JSApiStreamRemovePeerResponse
	require_NoError(t, json.Unmarshal(msg.Data, &resp))
	require_True(t, resp.Success)

	// Wait for the stream to be back at R3 with the removed peer replaced.
	checkFor(t, 5*time.Second, 200*time.Millisecond, func() error {
		si, err := js.StreamInfo("TEST", nats.MaxWait(time.Second))
		if err != nil {
			return err
		}
		if si.Cluster == nil || si.Cluster.Leader == _EMPTY_ {
			return fmt.Errorf("no stream leader yet")
		}
		if len(si.Cluster.Replicas) != 2 {
			return fmt.Errorf("expected 2 replicas, got %d", len(si.Cluster.Replicas))
		}
		if si.Cluster.Leader == toRemove {
			return fmt.Errorf("removed peer still stream leader")
		}
		for _, r := range si.Cluster.Replicas {
			if r.Name == toRemove {
				return fmt.Errorf("removed peer still in stream peer set")
			}
		}
		return nil
	})

	// The R3 durable should have been remapped off of the removed peer.
	checkFor(t, 5*time.Second, 200*time.Millisecond, func() error {
		ci, err := js.ConsumerInfo("TEST", "R3", nats.MaxWait(time.Second))
		if err != nil {
			return err
		}
		if ci.Cluster == nil || ci.Cluster.Leader == _EMPTY_ {
			return fmt.Errorf("no consumer leader yet")
		}
		if len(ci.Cluster.Replicas) != 2 {
			return fmt.Errorf("expected 2 replicas, got %d", len(ci.Cluster.Replicas))
		}
		if ci.Cluster.Leader == toRemove {
			return fmt.Errorf("removed peer still consumer leader")
		}
		for _, r := range ci.Cluster.Replicas {
			if r.Name == toRemove {
				return fmt.Errorf("removed peer still in consumer peer set")
			}
		}
		return nil
	})

	// The R1 durable was not on the removed peer, it must be left alone and specifically
	// must NOT be scaled up to the full new stream peer set.
	ci, err = js.ConsumerInfo("TEST", "R1")
	require_NoError(t, err)
	require_Equal(t, ci.Cluster.Leader, r1Peer)
	require_Len(t, len(ci.Cluster.Replicas), 0)

	// The ephemeral was not on the removed peer either, it should still exist.
	_, err = js.ConsumerInfo("TEST", ephemeral)
	require_NoError(t, err)
}

func TestJetStreamClusterStreamPeerRemoveEphemeralDeleted(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "R5S", 5)
	defer c.shutdown()

	nc, js := jsClientConnect(t, c.randomServer())
	defer nc.Close()

	_, err := js.AddStream(&nats.StreamConfig{
		Name:     "TEST",
		Subjects: []string{"foo"},
		Replicas: 3,
	})
	require_NoError(t, err)

	sub, err := js.SubscribeSync("foo")
	require_NoError(t, err)
	defer sub.Unsubscribe()
	eci, err := sub.ConsumerInfo()
	require_NoError(t, err)
	ephemeral, ephemeralPeer := eci.Name, eci.Cluster.Leader

	// Remove the ephemeral's peer from the stream.
	b, err := json.Marshal(JSApiStreamRemovePeerRequest{Peer: ephemeralPeer})
	require_NoError(t, err)
	msg, err := nc.Request(fmt.Sprintf(JSApiStreamRemovePeerT, "TEST"), b, time.Second)
	require_NoError(t, err)
	var resp JSApiStreamRemovePeerResponse
	require_NoError(t, json.Unmarshal(msg.Data, &resp))
	require_True(t, resp.Success)

	// The ephemeral had its only peer removed, it should be deleted rather than moved.
	checkFor(t, 5*time.Second, 200*time.Millisecond, func() error {
		_, err := js.ConsumerInfo("TEST", ephemeral, nats.MaxWait(time.Second))
		if err == nil {
			return fmt.Errorf("expected ephemeral consumer to be deleted")
		}
		require_Error(t, err, NewJSConsumerNotFoundError())
		return nil
	})
}

func TestJetStreamClusterStreamAddPeerConsumerRemap(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "R3S", 3)
	defer c.shutdown()

	nc, js := jsClientConnect(t, c.randomServer())
	defer nc.Close()

	_, err := js.AddStream(&nats.StreamConfig{
		Name:     "TEST",
		Subjects: []string{"foo"},
		Replicas: 3,
	})
	require_NoError(t, err)

	// An R1 durable, its single peer should be left alone when the stream gains a peer.
	_, err = js.AddConsumer("TEST", &nats.ConsumerConfig{
		Durable:   "R1",
		AckPolicy: nats.AckExplicitPolicy,
		Replicas:  1,
	})
	require_NoError(t, err)

	// An R3 durable, it should be scaled back up along with the stream once a new peer comes in.
	_, err = js.AddConsumer("TEST", &nats.ConsumerConfig{
		Durable:   "R3",
		AckPolicy: nats.AckExplicitPolicy,
	})
	require_NoError(t, err)

	// An ephemeral, it should stay untouched on its own single peer throughout.
	sub, err := js.SubscribeSync("foo")
	require_NoError(t, err)
	defer sub.Unsubscribe()
	eci, err := sub.ConsumerInfo()
	require_NoError(t, err)
	ephemeral, ephemeralPeer := eci.Name, eci.Cluster.Leader

	ci, err := js.ConsumerInfo("TEST", "R1")
	require_NoError(t, err)
	require_Len(t, len(ci.Cluster.Replicas), 0)
	r1Peer := ci.Cluster.Leader

	// Evacuate a server hosting neither the R1 durable nor the ephemeral. With only three
	// servers there is no replacement, which leaves the stream with a missing peer.
	si, err := js.StreamInfo("TEST")
	require_NoError(t, err)
	peers := []string{si.Cluster.Leader}
	for _, r := range si.Cluster.Replicas {
		peers = append(peers, r.Name)
	}
	var toRemove string
	for _, p := range peers {
		if p != r1Peer && p != ephemeralPeer {
			toRemove = p
			break
		}
	}
	require_NotEqual(t, toRemove, _EMPTY_)

	// The server scoped endpoint is system account only.
	snc, _ := jsClientConnect(t, c.randomServer(), nats.UserInfo("admin", "s3cr3t!"))
	defer snc.Close()

	b, err := json.Marshal(JSApiMetaServerEvacuateRequest{Server: toRemove})
	require_NoError(t, err)
	msg, err := snc.Request(JSApiEvacuateServer, b, 5*time.Second)
	require_NoError(t, err)
	var resp JSApiMetaServerEvacuateResponse
	require_NoError(t, json.Unmarshal(msg.Data, &resp))
	// The peer could not be replaced, but the server is going away so it is still
	// taken out of the stream.
	require_True(t, resp.Success)

	checkStreamPeers := func(replicas int) {
		t.Helper()
		checkFor(t, 5*time.Second, 200*time.Millisecond, func() error {
			si, err := js.StreamInfo("TEST", nats.MaxWait(time.Second))
			if err != nil {
				return err
			}
			if si.Cluster == nil || si.Cluster.Leader == _EMPTY_ {
				return fmt.Errorf("no stream leader yet")
			}
			if len(si.Cluster.Replicas) != replicas {
				return fmt.Errorf("expected %d replicas, got %d", replicas, len(si.Cluster.Replicas))
			}
			if si.Cluster.Leader == toRemove {
				return fmt.Errorf("removed peer still stream leader")
			}
			for _, r := range si.Cluster.Replicas {
				if r.Name == toRemove {
					return fmt.Errorf("removed peer still in stream peer set")
				}
			}
			return nil
		})
	}
	checkStreamPeers(1)

	// Now add in a new server, the stream should get its missing peer backfilled.
	c.addInNewServer()
	c.waitOnPeerCount(4)
	checkStreamPeers(2)

	// The R3 durable should scale back up to the full stream peer set.
	checkFor(t, 5*time.Second, 200*time.Millisecond, func() error {
		ci, err := js.ConsumerInfo("TEST", "R3", nats.MaxWait(time.Second))
		if err != nil {
			return err
		}
		if ci.Cluster == nil || ci.Cluster.Leader == _EMPTY_ {
			return fmt.Errorf("no consumer leader yet")
		}
		if len(ci.Cluster.Replicas) != 2 {
			return fmt.Errorf("expected 2 replicas, got %d", len(ci.Cluster.Replicas))
		}
		return nil
	})

	// The R1 durable must be left alone and specifically must NOT be scaled up
	// to the full new stream peer set.
	ci, err = js.ConsumerInfo("TEST", "R1")
	require_NoError(t, err)
	require_Equal(t, ci.Cluster.Leader, r1Peer)
	require_Len(t, len(ci.Cluster.Replicas), 0)

	// Same for the ephemeral, and it should not have been deleted either.
	eci, err = js.ConsumerInfo("TEST", ephemeral)
	require_NoError(t, err)
	require_Equal(t, eci.Cluster.Leader, ephemeralPeer)
	require_Len(t, len(eci.Cluster.Replicas), 0)
}

//
// DO NOT ADD NEW TESTS IN THIS FILE (unless to balance test times)
// Add at the end of jetstream_cluster_<n>_test.go, with <n> being the highest value.
//
