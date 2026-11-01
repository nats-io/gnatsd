// Copyright 2022-2026 The NATS Authors
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

//go:build !skip_js_tests && !skip_js_cluster_tests_3

package server

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"math/rand/v2"
	"net"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nats-io/jwt/v2"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

func TestJetStreamClusterRemovePeerByID(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "R3S", 3)
	defer c.shutdown()

	s := c.randomNonLeader()
	nc, js := jsClientConnect(t, s)
	defer nc.Close()

	_, err := js.AddStream(&nats.StreamConfig{
		Name:     "TEST",
		Subjects: []string{"foo", "bar"},
		Replicas: 3,
	})
	require_NoError(t, err)

	// Wait for a leader
	c.waitOnStreamLeader(globalAccountName, "TEST")

	// Get the name of the one that is not restarted
	srvName := c.opts[2].ServerName
	// And its node ID
	peerID := c.servers[2].Node()

	nc.Close()
	// Now stop the whole cluster
	c.stopAll()
	// Restart all but one
	for i := 0; i < 2; i++ {
		opts := c.opts[i]
		s, o := RunServerWithConfig(opts.ConfigFile)
		c.servers[i] = s
		c.opts[i] = o
	}

	c.waitOnClusterReadyWithNumPeers(2)
	c.waitOnStreamLeader(globalAccountName, "TEST")

	// Now attempt to remove by name, this should fail because the cluster
	// was restarted and names are not persisted.
	ml := c.leader()
	nc, err = nats.Connect(ml.ClientURL(), nats.UserInfo("admin", "s3cr3t!"))
	require_NoError(t, err)
	defer nc.Close()

	req := &JSApiMetaServerRemoveRequest{Server: srvName}
	jsreq, err := json.Marshal(req)
	require_NoError(t, err)
	rmsg, err := nc.Request(JSApiRemoveServer, jsreq, 2*time.Second)
	require_NoError(t, err)

	var resp JSApiMetaServerRemoveResponse
	err = json.Unmarshal(rmsg.Data, &resp)
	require_NoError(t, err)
	require_True(t, resp.Error != nil)
	require_True(t, IsNatsErr(resp.Error, JSClusterServerNotMemberErr))

	// Now try by ID, but first with an ID that does not match any peerID
	req.Peer = "some_bad_id"
	jsreq, err = json.Marshal(req)
	require_NoError(t, err)
	rmsg, err = nc.Request(JSApiRemoveServer, jsreq, 2*time.Second)
	require_NoError(t, err)

	resp = JSApiMetaServerRemoveResponse{}
	err = json.Unmarshal(rmsg.Data, &resp)
	require_NoError(t, err)
	require_True(t, resp.Error != nil)
	require_True(t, IsNatsErr(resp.Error, JSClusterServerNotMemberErr))

	// Now with the proper peer ID
	req.Peer = peerID
	jsreq, err = json.Marshal(req)
	require_NoError(t, err)
	rmsg, err = nc.Request(JSApiRemoveServer, jsreq, 2*time.Second)
	require_NoError(t, err)

	resp = JSApiMetaServerRemoveResponse{}
	err = json.Unmarshal(rmsg.Data, &resp)
	require_NoError(t, err)
	require_True(t, resp.Error == nil)
	require_True(t, resp.Success)
}

func TestJetStreamClusterDiscardNewAndMaxMsgsPerSubject(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "R3S", 3)
	defer c.shutdown()

	// Client for API requests.
	s := c.randomNonLeader()
	nc, js := jsClientConnect(t, s)
	defer nc.Close()

	for _, test := range []struct {
		name     string
		storage  StorageType
		replicas int
	}{
		{"MEM-R1", MemoryStorage, 1},
		{"FILE-R1", FileStorage, 1},
		{"MEM-R3", MemoryStorage, 3},
		{"FILE-R3", FileStorage, 3},
	} {
		t.Run(test.name, func(t *testing.T) {
			js.DeleteStream("KV")
			// Make sure setting new without DiscardPolicy also being new is error.
			cfg := &StreamConfig{
				Name:          "KV",
				Subjects:      []string{"KV.>"},
				Storage:       test.storage,
				AllowDirect:   true,
				DiscardNewPer: true,
				MaxMsgs:       10,
				Replicas:      test.replicas,
			}
			if _, apiErr := addStreamWithError(t, nc, cfg); apiErr == nil {
				t.Fatalf("Expected API error but got none")
			} else if apiErr.ErrCode != 10052 || !strings.Contains(apiErr.Description, "discard new per subject requires discard new policy") {
				t.Fatalf("Got wrong error: %+v", apiErr)
			}

			// Set broad discard new policy to engage DiscardNewPer
			cfg.Discard = DiscardNew
			// We should also error here since we have not setup max msgs per subject.
			if _, apiErr := addStreamWithError(t, nc, cfg); apiErr == nil {
				t.Fatalf("Expected API error but got none")
			} else if apiErr.ErrCode != 10052 || !strings.Contains(apiErr.Description, "discard new per subject requires max msgs per subject > 0") {
				t.Fatalf("Got wrong error: %+v", apiErr)
			}

			cfg.MaxMsgsPer = 1
			addStream(t, nc, cfg)

			// We want to test that we reject new messages on a per subject basis if the
			// max msgs per subject limit has been hit, even if other limits have not.
			_, err := js.Publish("KV.foo", nil)
			require_NoError(t, err)

			_, err = js.Publish("KV.foo", nil)
			// Go client does not have const for this one.
			require_Error(t, err, errors.New("nats: maximum messages per subject exceeded"))
		})
	}
}

func TestJetStreamClusterCreateConsumerWithReplicaOneGetsResponse(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "R3S", 3)
	defer c.shutdown()

	s := c.randomNonLeader()
	nc, js := jsClientConnect(t, s)
	defer nc.Close()

	_, err := js.AddStream(&nats.StreamConfig{
		Name:     "TEST",
		Subjects: []string{"foo"},
		Replicas: 3,
	})
	require_NoError(t, err)

	c.waitOnStreamLeader(globalAccountName, "TEST")

	_, err = js.AddConsumer("TEST", &nats.ConsumerConfig{
		Durable:   "C3",
		AckPolicy: nats.AckExplicitPolicy,
	})
	require_NoError(t, err)

	c.waitOnConsumerLeader(globalAccountName, "TEST", "C3")

	// Update to scale down to R1, that should work (get a response)
	_, err = js.UpdateConsumer("TEST", &nats.ConsumerConfig{
		Durable:   "C3",
		AckPolicy: nats.AckExplicitPolicy,
		Replicas:  1,
	})
	require_NoError(t, err)

	c.waitOnConsumerLeader(globalAccountName, "TEST", "C3")

	ci, err := js.ConsumerInfo("TEST", "C3")
	require_NoError(t, err)
	require_True(t, ci.Config.Replicas == 1)
	require_True(t, len(ci.Cluster.Replicas) == 0)
}

func TestJetStreamClusterMetaRecoveryLogic(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "R3S", 3)
	defer c.shutdown()

	s := c.randomNonLeader()
	nc, js := jsClientConnect(t, s)
	defer nc.Close()

	_, err := js.AddStream(&nats.StreamConfig{
		Name:     "TEST",
		Subjects: []string{"foo"},
		Replicas: 3,
	})
	require_NoError(t, err)

	_, err = js.UpdateStream(&nats.StreamConfig{
		Name:     "TEST",
		Subjects: []string{"foo", "bar"},
		Replicas: 1,
	})
	require_NoError(t, err)

	// Stream delete is answered by stream leader, stream add is answered by meta leader.
	// If meta leader is slower to delete, a quick add-after-delete would error with stream already exists.
	waitForDeleteStream := func() {
		t.Helper()
		checkFor(t, time.Second, 100*time.Millisecond, func() error {
			ml := c.leader()
			if ml == nil {
				return errors.New("no meta leader")
			}
			sjs := ml.getJetStream()
			sjs.mu.RLock()
			sa := sjs.streamAssignment("$G", "TEST")
			sjs.mu.RUnlock()
			if sa != nil {
				return errors.New("stream exists still")
			}
			return nil
		})
	}

	err = js.DeleteStream("TEST")
	require_NoError(t, err)
	waitForDeleteStream()

	_, err = js.AddStream(&nats.StreamConfig{
		Name:     "TEST",
		Subjects: []string{"foo"},
		Replicas: 3,
	})
	require_NoError(t, err)

	err = js.DeleteStream("TEST")
	require_NoError(t, err)
	waitForDeleteStream()

	_, err = js.AddStream(&nats.StreamConfig{
		Name:     "TEST",
		Subjects: []string{"baz"},
		Replicas: 1,
	})
	require_NoError(t, err)

	osi, err := js.StreamInfo("TEST")
	require_NoError(t, err)

	c.stopAll()
	c.restartAll()
	checkFor(t, 10*time.Second, 200*time.Millisecond, func() error {
		s := c.leader()
		hs := s.healthz(&HealthzOptions{
			JSMetaOnly: true,
		})
		if hs.Error != _EMPTY_ {
			return errors.New(hs.Error)
		}
		return nil
	})
	c.waitOnLeader()
	c.waitOnStreamLeader("$G", "TEST")

	s = c.randomNonLeader()
	checkFor(t, 10*time.Second, 200*time.Millisecond, func() error {
		hs := s.healthz(&HealthzOptions{
			JSMetaOnly: true,
		})
		if hs.Error != _EMPTY_ {
			return errors.New(hs.Error)
		}
		return nil
	})

	nc, js = jsClientConnect(t, s)
	defer nc.Close()

	si, err := js.StreamInfo("TEST")
	require_NoError(t, err)

	if !reflect.DeepEqual(si.Config, osi.Config) {
		t.Fatalf("Expected %+v, but got %+v", osi.Config, si.Config)
	}
}

func TestJetStreamClusterDeleteConsumerWhileServerDown(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "R3S", 3)
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
		Durable:   "DC",
		AckPolicy: nats.AckExplicitPolicy,
		Replicas:  3,
	})
	require_NoError(t, err)

	s := c.randomNonConsumerLeader("$G", "TEST", "DC")
	s.Shutdown()

	c.waitOnLeader()                                 // In case that was metaleader.
	nc, js = jsClientConnect(t, c.randomNonLeader()) // In case we were connected there.
	defer nc.Close()

	err = js.DeleteConsumer("TEST", "DC")
	require_NoError(t, err)

	// Restart.
	s = c.restartServer(s)
	checkFor(t, 10*time.Second, 200*time.Millisecond, func() error {
		hs := s.healthz(&HealthzOptions{
			JSEnabledOnly: false,
			JSServerOnly:  false,
		})
		if hs.Error != _EMPTY_ {
			return errors.New(hs.Error)
		}
		return nil
	})

	// Make sure we can not see it on the server that was down at the time of delete.
	mset, err := s.GlobalAccount().lookupStream("TEST")
	require_NoError(t, err)

	if o := mset.lookupConsumer("DC"); o != nil {
		t.Fatalf("Expected to not find consumer, but did")
	}

	// Now repeat but force a meta snapshot.
	_, err = js.AddConsumer("TEST", &nats.ConsumerConfig{
		Durable:   "DC",
		AckPolicy: nats.AckExplicitPolicy,
		Replicas:  3,
	})
	require_NoError(t, err)

	// Shut down the server but ensure it can't make a snapshot during shutdown.
	s = c.randomNonConsumerLeader("$G", "TEST", "DC")
	meta := s.getJetStream().getMetaGroup().(*raft)
	meta.Lock()
	meta.progress = make(map[string]*ipQueue[uint64])
	meta.progress["blockSnapshots"] = newIPQueue[uint64](meta.s, "blockSnapshots")
	meta.Unlock()
	s.Shutdown()

	c.waitOnLeader()                                 // In case that was metaleader.
	nc, js = jsClientConnect(t, c.randomNonLeader()) // In case we were connected there.
	defer nc.Close()

	err = js.DeleteConsumer("TEST", "DC")
	require_NoError(t, err)

	err = c.leader().JetStreamSnapshotMeta()
	require_NoError(t, err)

	// Restart.
	s = c.restartServer(s)
	checkFor(t, time.Second*2, 200*time.Millisecond, func() error {
		hs := s.healthz(&HealthzOptions{
			JSEnabledOnly: false,
			JSServerOnly:  false,
		})
		if hs.Error != _EMPTY_ {
			return errors.New(hs.Error)
		}
		return nil
	})

	// Make sure we can not see it on the server that was down at the time of delete.
	mset, err = s.GlobalAccount().lookupStream("TEST")
	require_NoError(t, err)

	if o := mset.lookupConsumer("DC"); o != nil {
		t.Fatalf("Expected to not find consumer, but did")
	}
}

func TestJetStreamClusterNegativeReplicas(t *testing.T) {
	s := RunBasicJetStreamServer(t)
	defer s.Shutdown()

	c := createJetStreamClusterExplicit(t, "R3S", 3)
	defer c.shutdown()

	testBadReplicas := func(t *testing.T, s *Server, name string) {
		nc, js := jsClientConnect(t, s)
		defer nc.Close()

		_, err := js.AddStream(&nats.StreamConfig{
			Name:     name,
			Replicas: -1,
		})
		require_Error(t, err, NewJSReplicasCountCannotBeNegativeError())

		_, err = js.AddStream(&nats.StreamConfig{
			Name:     name,
			Replicas: 1,
		})
		require_NoError(t, err)

		// Check update now.
		_, err = js.UpdateStream(&nats.StreamConfig{
			Name:     name,
			Replicas: -11,
		})
		require_Error(t, err, NewJSReplicasCountCannotBeNegativeError())

		// Now same for consumers
		durName := fmt.Sprintf("%s_dur", name)
		_, err = js.AddConsumer(name, &nats.ConsumerConfig{
			Durable:  durName,
			Replicas: -1,
		})
		require_Error(t, err, NewJSReplicasCountCannotBeNegativeError())

		_, err = js.AddConsumer(name, &nats.ConsumerConfig{
			Durable:  durName,
			Replicas: 1,
		})
		require_NoError(t, err)

		// Check update now
		_, err = js.UpdateConsumer(name, &nats.ConsumerConfig{
			Durable:  durName,
			Replicas: -11,
		})
		require_Error(t, err, NewJSReplicasCountCannotBeNegativeError())
	}

	t.Run("Standalone", func(t *testing.T) { testBadReplicas(t, s, "TEST1") })
	t.Run("Clustered", func(t *testing.T) { testBadReplicas(t, c.randomServer(), "TEST2") })
}

func TestJetStreamClusterUserGivenConsName(t *testing.T) {
	s := RunBasicJetStreamServer(t)
	defer s.Shutdown()

	c := createJetStreamClusterExplicit(t, "R3S", 3)
	defer c.shutdown()

	test := func(t *testing.T, s *Server, stream string, replicas int, cons string) {
		nc, js := jsClientConnect(t, s)
		defer nc.Close()

		_, err := js.AddStream(&nats.StreamConfig{
			Name:     stream,
			Replicas: replicas,
		})
		require_NoError(t, err)

		cc := &CreateConsumerRequest{
			Stream: stream,
			Config: ConsumerConfig{
				Name:              cons,
				FilterSubject:     stream,
				InactiveThreshold: 10 * time.Second,
			},
		}
		subj := fmt.Sprintf(JSApiConsumerCreateExT, stream, cons, stream)
		req, err := json.Marshal(cc)
		require_NoError(t, err)

		reply, err := nc.Request(subj, req, 2*time.Second)
		require_NoError(t, err)

		var cresp JSApiConsumerCreateResponse
		json.Unmarshal(reply.Data, &cresp)
		if cresp.Error != nil {
			t.Fatalf("Unexpected error: %v", cresp.Error)
		}
		require_Equal(t, cresp.Name, cons)
		require_Equal(t, cresp.Config.Name, cons)

		// Resend the add request but before change something that the server
		// should reject since the consumer already exist and we don't support
		// the update of the consumer that way.
		cc.Config.DeliverPolicy = DeliverNew
		req, err = json.Marshal(cc)
		require_NoError(t, err)
		reply, err = nc.Request(subj, req, 2*time.Second)
		require_NoError(t, err)

		cresp = JSApiConsumerCreateResponse{}
		json.Unmarshal(reply.Data, &cresp)
		require_Error(t, cresp.Error, NewJSConsumerCreateError(errors.New("deliver policy can not be updated")))
	}

	t.Run("Standalone", func(t *testing.T) { test(t, s, "TEST", 1, "cons") })
	t.Run("Clustered R1", func(t *testing.T) { test(t, c.randomServer(), "TEST2", 1, "cons2") })
	t.Run("Clustered R3", func(t *testing.T) { test(t, c.randomServer(), "TEST3", 3, "cons3") })
}

func TestJetStreamClusterUserGivenConsNameWithLeaderChange(t *testing.T) {
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

	c.waitOnStreamLeader(globalAccountName, "TEST")
	for i := 0; i < 100; i++ {
		sendStreamMsg(t, nc, "foo", "msg")
	}

	consName := "myephemeral"
	cc := &CreateConsumerRequest{
		Stream: "TEST",
		Config: ConsumerConfig{
			Name:              consName,
			FilterSubject:     "foo",
			InactiveThreshold: time.Hour,
			Replicas:          3,
		},
	}
	subj := fmt.Sprintf(JSApiConsumerCreateExT, "TEST", consName, "foo")
	req, err := json.Marshal(cc)
	require_NoError(t, err)

	reply, err := nc.Request(subj, req, 2*time.Second)
	require_NoError(t, err)

	var cresp JSApiConsumerCreateResponse
	json.Unmarshal(reply.Data, &cresp)
	if cresp.Error != nil {
		t.Fatalf("Unexpected error: %v", cresp.Error)
	}
	require_Equal(t, cresp.Name, consName)
	require_Equal(t, cresp.Config.Name, consName)

	// Consumer leader name
	clname := cresp.ConsumerInfo.Cluster.Leader

	nreq := &JSApiConsumerGetNextRequest{Batch: 1, Expires: time.Second}
	req, err = json.Marshal(nreq)
	require_NoError(t, err)

	sub := natsSubSync(t, nc, "xxx")
	rsubj := fmt.Sprintf(JSApiRequestNextT, "TEST", consName)
	err = nc.PublishRequest(rsubj, "xxx", req)
	require_NoError(t, err)

	msg := natsNexMsg(t, sub, time.Second)
	require_Equal(t, string(msg.Data), "msg")

	// Shutdown the consumer leader
	cl := c.serverByName(clname)
	cl.Shutdown()

	// Wait for a bit to be sure that we lost leadership
	time.Sleep(250 * time.Millisecond)

	// Wait for new leader
	c.waitOnStreamLeader(globalAccountName, "TEST")
	c.waitOnConsumerLeader(globalAccountName, "TEST", consName)

	// Make sure we can still consume.
	for i := 0; i < 2; i++ {
		err = nc.PublishRequest(rsubj, "xxx", req)
		require_NoError(t, err)

		msg = natsNexMsg(t, sub, time.Second)
		if len(msg.Data) == 0 {
			continue
		}
		require_Equal(t, string(msg.Data), "msg")
		return
	}
	t.Fatal("Did not receive message")
}

func TestJetStreamClusterMirrorCrossDomainOnLeadnodeNoSystemShare(t *testing.T) {
	tmpl := strings.Replace(jsClusterAccountsTempl, "store_dir:", "domain: HUB, store_dir:", 1)
	c := createJetStreamCluster(t, tmpl, "CORE", _EMPTY_, 3, 18033, true)
	defer c.shutdown()

	tmpl = strings.Replace(jsClusterTemplWithSingleLeafNode, "store_dir:", "domain: SPOKE, store_dir:", 1)
	ln := c.createLeafNodeWithTemplateNoSystem("LN-SPOKE", tmpl)
	defer ln.Shutdown()

	checkLeafNodeConnectedCount(t, ln, 1)

	// Create origin stream in hub.
	nc, js := jsClientConnect(t, c.randomServer())
	defer nc.Close()

	_, err := js.AddStream(&nats.StreamConfig{
		Name:              "TEST",
		Subjects:          []string{"foo"},
		MaxMsgsPerSubject: 10,
		AllowDirect:       true,
	})
	require_NoError(t, err)

	// Now create the mirror on the leafnode.
	lnc, ljs := jsClientConnect(t, ln)
	defer lnc.Close()

	_, err = ljs.AddStream(&nats.StreamConfig{
		Name:              "M",
		MaxMsgsPerSubject: 10,
		AllowDirect:       true,
		MirrorDirect:      true,
		Mirror: &nats.StreamSource{
			Name: "TEST",
			External: &nats.ExternalStream{
				APIPrefix: "$JS.HUB.API",
			},
		},
	})
	require_NoError(t, err)

	// Publish to the hub stream and make sure the mirror gets those messages.
	for i := 0; i < 20; i++ {
		js.Publish("foo", nil)
	}

	si, err := js.StreamInfo("TEST")
	require_NoError(t, err)
	require_True(t, si.State.Msgs == 10)

	checkFor(t, time.Second, 200*time.Millisecond, func() error {
		si, err := ljs.StreamInfo("M")
		require_NoError(t, err)
		if si.State.Msgs == 10 {
			return nil
		}
		return fmt.Errorf("State not current: %+v", si.State)
	})
}

func TestJetStreamClusterFirstSeqMismatch(t *testing.T) {
	c := createJetStreamClusterWithTemplateAndModHook(t, jsClusterTempl, "C", 3,
		func(serverName, clusterName, storeDir, conf string) string {
			tf := createTempFile(t, "")
			logName := tf.Name()
			tf.Close()
			return fmt.Sprintf("%s\nlogfile: '%s'", conf, logName)
		})
	defer c.shutdown()

	rs := c.randomServer()
	nc, js := jsClientConnect(t, rs)
	defer nc.Close()

	_, err := js.AddStream(&nats.StreamConfig{
		Name:     "TEST",
		Subjects: []string{"foo"},
		Replicas: 3,
		MaxAge:   2 * time.Second,
	})
	require_NoError(t, err)

	c.waitOnStreamLeader(globalAccountName, "TEST")

	mset, err := c.streamLeader(globalAccountName, "TEST").GlobalAccount().lookupStream("TEST")
	require_NoError(t, err)
	node := mset.raftNode()

	nl := c.randomNonStreamLeader(globalAccountName, "TEST")
	if rs == nl {
		nc.Close()
		for _, s := range c.servers {
			if s != nl {
				nc, _ = jsClientConnect(t, s)
				defer nc.Close()
				break
			}
		}
	}

	wg := sync.WaitGroup{}
	wg.Add(1)
	ch := make(chan struct{})
	go func() {
		defer wg.Done()
		for i := 0; ; i++ {
			sendStreamMsg(t, nc, "foo", "msg")
			select {
			case <-ch:
				return
			default:
			}
		}
	}()

	time.Sleep(2500 * time.Millisecond)
	nl.Shutdown()

	time.Sleep(500 * time.Millisecond)
	node.InstallSnapshot(mset.stateSnapshot(), false)
	time.Sleep(3500 * time.Millisecond)

	c.restartServer(nl)
	c.waitOnAllCurrent()

	close(ch)
	wg.Wait()

	log := nl.getOpts().LogFile
	nl.Shutdown()

	content, err := os.ReadFile(log)
	require_NoError(t, err)
	if bytes.Contains(content, []byte(errFirstSequenceMismatch.Error())) {
		t.Fatalf("First sequence mismatch occurred!")
	}
}

func TestJetStreamClusterConsumerInactiveThreshold(t *testing.T) {
	// Create a standalone, a cluster, and a super cluster

	s := RunBasicJetStreamServer(t)
	defer s.Shutdown()

	c := createJetStreamClusterExplicit(t, "R3S", 3)
	defer c.shutdown()

	sc := createJetStreamSuperCluster(t, 3, 2)
	defer sc.shutdown()

	test := func(t *testing.T, c *cluster, s *Server, replicas int) {
		if c != nil {
			s = c.randomServer()
		}
		nc, js := jsClientConnect(t, s)
		defer nc.Close()

		sname := fmt.Sprintf("TEST%d", replicas)
		_, err := js.AddStream(&nats.StreamConfig{
			Name:     sname,
			Subjects: []string{sname},
			Replicas: replicas,
		})
		require_NoError(t, err)

		if c != nil {
			c.waitOnStreamLeader(globalAccountName, sname)
		}

		for i := 0; i < 10; i++ {
			js.PublishAsync(sname, []byte("ok"))
		}
		select {
		case <-js.PublishAsyncComplete():
		case <-time.After(5 * time.Second):
			t.Fatalf("Did not receive completion signal")
		}

		waitOnCleanup := func(ci *nats.ConsumerInfo) {
			t.Helper()
			checkFor(t, 2*time.Second, 50*time.Millisecond, func() error {
				_, err := js.ConsumerInfo(ci.Stream, ci.Name)
				if err == nil {
					return fmt.Errorf("Consumer still present")
				}
				return nil
			})
		}

		// Test to make sure inactive threshold is enforced for all types.
		// Ephemeral and Durable, both push and pull.

		// Ephemeral Push (no bind to deliver subject)
		ci, err := js.AddConsumer(sname, &nats.ConsumerConfig{
			DeliverSubject:    "_no_bind_",
			InactiveThreshold: 50 * time.Millisecond,
		})
		require_NoError(t, err)
		waitOnCleanup(ci)

		// Ephemeral Pull
		ci, err = js.AddConsumer(sname, &nats.ConsumerConfig{
			AckPolicy:         nats.AckExplicitPolicy,
			InactiveThreshold: 50 * time.Millisecond,
		})
		require_NoError(t, err)
		waitOnCleanup(ci)

		// Support InactiveThresholds for Durables as well.

		// Durable Push (no bind to deliver subject)
		ci, err = js.AddConsumer(sname, &nats.ConsumerConfig{
			Durable:           "d1",
			DeliverSubject:    "_no_bind_",
			InactiveThreshold: 50 * time.Millisecond,
		})
		require_NoError(t, err)
		waitOnCleanup(ci)

		// Durable Push (no bind to deliver subject) with an activity
		// threshold set after creation
		ci, err = js.AddConsumer(sname, &nats.ConsumerConfig{
			Durable:        "d2",
			DeliverSubject: "_no_bind_",
		})
		require_NoError(t, err)
		if c != nil {
			c.waitOnConsumerLeader(globalAccountName, sname, "d2")
		}
		_, err = js.UpdateConsumer(sname, &nats.ConsumerConfig{
			Durable:           "d2",
			DeliverSubject:    "_no_bind_",
			InactiveThreshold: 50 * time.Millisecond,
		})
		require_NoError(t, err)
		waitOnCleanup(ci)

		// Durable Pull
		ci, err = js.AddConsumer(sname, &nats.ConsumerConfig{
			Durable:           "d3",
			AckPolicy:         nats.AckExplicitPolicy,
			InactiveThreshold: 50 * time.Millisecond,
		})
		require_NoError(t, err)
		waitOnCleanup(ci)

		// Durable Pull with an inactivity threshold set after creation
		ci, err = js.AddConsumer(sname, &nats.ConsumerConfig{
			Durable:   "d4",
			AckPolicy: nats.AckExplicitPolicy,
		})
		require_NoError(t, err)
		if c != nil {
			c.waitOnConsumerLeader(globalAccountName, sname, "d4")
		}
		_, err = js.UpdateConsumer(sname, &nats.ConsumerConfig{
			Durable:           "d4",
			AckPolicy:         nats.AckExplicitPolicy,
			InactiveThreshold: 50 * time.Millisecond,
		})
		require_NoError(t, err)
		waitOnCleanup(ci)
	}

	t.Run("standalone", func(t *testing.T) { test(t, nil, s, 1) })
	t.Run("cluster-r1", func(t *testing.T) { test(t, c, nil, 1) })
	t.Run("cluster-r3", func(t *testing.T) { test(t, c, nil, 3) })
	t.Run("super-cluster-r1", func(t *testing.T) { test(t, sc.randomCluster(), nil, 1) })
	t.Run("super-cluster-r3", func(t *testing.T) { test(t, sc.randomCluster(), nil, 3) })
}

// To capture our false warnings for clustered stream lag.
type testStreamLagWarnLogger struct {
	DummyLogger
	ch chan string
}

func (l *testStreamLagWarnLogger) Warnf(format string, v ...any) {
	msg := fmt.Sprintf(format, v...)
	if strings.Contains(msg, "has high message lag") {
		select {
		case l.ch <- msg:
		default:
		}
	}
}

// False triggering warnings on stream lag because not offsetting by failures.
func TestJetStreamClusterStreamLagWarning(t *testing.T) {
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

	sl := c.streamLeader("$G", "TEST")

	l := &testStreamLagWarnLogger{ch: make(chan string, 10)}
	sl.SetLogger(l, false, false)

	// We only need to trigger post RAFT propose failures that increment mset.clfs.
	// Dedupe with msgIDs is one, so we will use that.
	m := nats.NewMsg("foo")
	m.Data = []byte("OK")
	m.Header.Set(JSMsgId, "zz")

	// Make sure we know we will trip the warning threshold.
	for i := 0; i < 2*streamLagWarnThreshold; i++ {
		js.PublishMsgAsync(m)
	}
	select {
	case <-js.PublishAsyncComplete():
	case <-time.After(5 * time.Second):
		t.Fatalf("Did not receive completion signal")
	}

	select {
	case msg := <-l.ch:
		t.Fatalf("Unexpected msg lag warning seen: %s", msg)
	case <-time.After(100 * time.Millisecond):
		// OK
	}
}

// https://github.com/nats-io/nats-server/issues/3603
func TestJetStreamClusterSignalPullConsumersOnDelete(t *testing.T) {
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

	// Create 2 pull consumers.
	sub1, err := js.PullSubscribe("foo", "d1")
	require_NoError(t, err)

	sub2, err := js.PullSubscribe("foo", "d2")
	require_NoError(t, err)

	// We want to make sure we get kicked out prior to the timeout
	// when consumers are being deleted or the parent stream is being deleted.
	// Note this should be lower case, Go client needs to be updated.
	expectedErr := errors.New("nats: consumer deleted")

	// Queue up the delete for sub1
	time.AfterFunc(250*time.Millisecond, func() { js.DeleteConsumer("TEST", "d1") })
	start := time.Now()
	_, err = sub1.Fetch(1, nats.MaxWait(10*time.Second))
	require_Error(t, err, expectedErr)

	// Check that we bailed early.
	if time.Since(start) > time.Second {
		t.Fatalf("Took to long to bail out on consumer delete")
	}

	time.AfterFunc(250*time.Millisecond, func() { js.DeleteStream("TEST") })
	start = time.Now()
	_, err = sub2.Fetch(1, nats.MaxWait(10*time.Second))
	require_Error(t, err, expectedErr)
	if time.Since(start) > time.Second {
		t.Fatalf("Took to long to bail out on stream delete")
	}
}

// https://github.com/nats-io/nats-server/issues/3559
func TestJetStreamClusterSourceWithOptStartTime(t *testing.T) {
	s := RunBasicJetStreamServer(t)
	defer s.Shutdown()

	c := createJetStreamClusterExplicit(t, "R3S", 3)
	defer c.shutdown()

	test := func(t *testing.T, c *cluster, s *Server) {

		replicas := 1
		if c != nil {
			s = c.randomServer()
			replicas = 3
		}
		nc, js := jsClientConnect(t, s)
		defer nc.Close()

		_, err := js.AddStream(&nats.StreamConfig{
			Name:     "TEST",
			Subjects: []string{"foo"},
			Replicas: replicas,
		})
		require_NoError(t, err)

		yesterday := time.Now().Add(-24 * time.Hour)

		_, err = js.AddStream(&nats.StreamConfig{
			Name:     "SOURCE",
			Replicas: replicas,
			Sources: []*nats.StreamSource{{
				Name:         "TEST",
				OptStartTime: &yesterday,
			}},
		})
		require_NoError(t, err)

		_, err = js.AddStream(&nats.StreamConfig{
			Name:     "MIRROR",
			Replicas: replicas,
			Mirror: &nats.StreamSource{
				Name:         "TEST",
				OptStartTime: &yesterday,
			},
		})
		require_NoError(t, err)

		total := 10
		for i := 0; i < total; i++ {
			sendStreamMsg(t, nc, "foo", "hello")
		}

		checkCount := func(sname string, expected int) {
			t.Helper()
			checkFor(t, 10*time.Second, 50*time.Millisecond, func() error {
				si, err := js.StreamInfo(sname)
				if err != nil {
					return err
				}
				if n := si.State.Msgs; n != uint64(expected) {
					return fmt.Errorf("Expected stream %q to have %v messages, got %v", sname, expected, n)
				}
				return nil
			})
		}

		checkCount("TEST", 10)
		checkCount("SOURCE", 10)
		checkCount("MIRROR", 10)

		err = js.PurgeStream("SOURCE")
		require_NoError(t, err)
		err = js.PurgeStream("MIRROR")
		require_NoError(t, err)

		checkCount("TEST", 10)
		checkCount("SOURCE", 0)
		checkCount("MIRROR", 0)

		nc.Close()
		if c != nil {
			c.stopAll()
			c.restartAll()

			c.waitOnStreamLeader(globalAccountName, "TEST")
			c.waitOnStreamLeader(globalAccountName, "SOURCE")
			c.waitOnStreamLeader(globalAccountName, "MIRROR")

			s = c.randomServer()
		} else {
			sd := s.JetStreamConfig().StoreDir
			s.Shutdown()
			s = RunJetStreamServerOnPort(-1, sd)
			defer s.Shutdown()
		}

		// Wait a bit before checking because sync'ing (even with the defect)
		// would not happen right away. I tried with 1 sec and test would pass,
		// so need to be at least that much.
		time.Sleep(2 * time.Second)

		nc, js = jsClientConnect(t, s)
		defer nc.Close()
		checkCount("TEST", 10)
		checkCount("SOURCE", 0)
		checkCount("MIRROR", 0)
	}

	t.Run("standalone", func(t *testing.T) { test(t, nil, s) })
	t.Run("cluster", func(t *testing.T) { test(t, c, nil) })
}

type networkCableUnplugged struct {
	net.Conn
	sync.Mutex
	unplugged bool
	wb        bytes.Buffer
	wg        sync.WaitGroup
}

func (c *networkCableUnplugged) Write(b []byte) (int, error) {
	c.Lock()
	if c.unplugged {
		c.wb.Write(b)
		c.Unlock()
		return len(b), nil
	} else if c.wb.Len() > 0 {
		c.wb.Write(b)
		buf := c.wb.Bytes()
		c.wb.Reset()
		c.Unlock()
		if _, err := c.Conn.Write(buf); err != nil {
			return 0, err
		}
		return len(b), nil
	}
	c.Unlock()
	return c.Conn.Write(b)
}

func (c *networkCableUnplugged) Read(b []byte) (int, error) {
	c.Lock()
	wait := c.unplugged
	c.Unlock()
	if wait {
		c.wg.Wait()
	}
	return c.Conn.Read(b)
}

func TestJetStreamClusterScaleDownWhileNoQuorum(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "R5S", 5)
	defer c.shutdown()

	s := c.randomServer()
	nc, js := jsClientConnect(t, s)
	defer nc.Close()

	si, err := js.AddStream(&nats.StreamConfig{
		Name:     "TEST",
		Subjects: []string{"foo"},
		Replicas: 2,
	})
	require_NoError(t, err)

	for i := 0; i < 1000; i++ {
		sendStreamMsg(t, nc, "foo", "msg")
	}

	// Let's have a server from this R2 stream be network partitionned.
	// We will take the leader, but doesn't have to be.
	// To simulate partition, we will replace all its routes with a
	// special connection that drops messages.
	sl := c.serverByName(si.Cluster.Leader)
	if s == sl {
		nc.Close()
		for s = c.randomServer(); s != sl; s = c.randomServer() {
		}
		nc, js = jsClientConnect(t, s)
		defer nc.Close()
	}

	sl.mu.Lock()
	sl.forEachRoute(func(r *client) {
		r.mu.Lock()
		ncu := &networkCableUnplugged{Conn: r.nc, unplugged: true}
		ncu.wg.Add(1)
		r.nc = ncu
		r.mu.Unlock()
	})
	sl.mu.Unlock()

	// Wait for the stream info to fail
	checkFor(t, 10*time.Second, 100*time.Millisecond, func() error {
		si, err := js.StreamInfo("TEST", nats.MaxWait(time.Second))
		if err != nil {
			return err
		}
		if si.Cluster.Leader == _EMPTY_ {
			return nil
		}
		return fmt.Errorf("stream still has a leader")
	})

	// Make sure if meta leader was on same server as stream leader we make sure
	// it elects new leader to receive update request.
	c.waitOnLeader()

	// Now try to edit the stream by making it an R1. In some case we get
	// a context deadline error, in some no error. So don't check the returned error.
	js.UpdateStream(&nats.StreamConfig{
		Name:     "TEST",
		Subjects: []string{"foo"},
		Replicas: 1,
	}, nats.MaxWait(5*time.Second))

	sl.mu.Lock()
	sl.forEachRoute(func(r *client) {
		r.mu.Lock()
		ncu := r.nc.(*networkCableUnplugged)
		ncu.Lock()
		ncu.unplugged = false
		ncu.wg.Done()
		ncu.Unlock()
		r.mu.Unlock()
	})
	sl.mu.Unlock()

	checkClusterFormed(t, c.servers...)
	c.waitOnStreamLeader(globalAccountName, "TEST")
}

// We noticed that ha_assets enforcement seemed to not be upheld when assets created in a rapid fashion.
func TestJetStreamClusterHAssetsEnforcement(t *testing.T) {
	tmpl := strings.Replace(jsClusterTempl, "store_dir:", "limits: {max_ha_assets: 2}, store_dir:", 1)
	c := createJetStreamClusterWithTemplateAndModHook(t, tmpl, "R3S", 3, nil)
	defer c.shutdown()

	nc, js := jsClientConnect(t, c.randomServer())
	defer nc.Close()

	_, err := js.AddStream(&nats.StreamConfig{
		Name:     "TEST-1",
		Subjects: []string{"foo"},
		Replicas: 3,
	})
	require_NoError(t, err)

	_, err = js.AddStream(&nats.StreamConfig{
		Name:     "TEST-2",
		Subjects: []string{"bar"},
		Replicas: 3,
	})
	require_NoError(t, err)

	// Should fail.
	_, err = js.AddStream(&nats.StreamConfig{
		Name:     "TEST-3",
		Subjects: []string{"baz"},
		Replicas: 3,
	})
	require_Error(t, err, errors.New("no suitable peers"))
}

func TestJetStreamClusterInterestStreamConsumer(t *testing.T) {
	checkInterestStateT = 4 * time.Second
	checkInterestStateJ = 1
	defer func() {
		checkInterestStateT = defaultCheckInterestStateT
		checkInterestStateJ = defaultCheckInterestStateJ
	}()

	c := createJetStreamClusterExplicit(t, "R5S", 5)
	defer c.shutdown()

	nc, js := jsClientConnect(t, c.randomServer())
	defer nc.Close()

	_, err := js.AddStream(&nats.StreamConfig{
		Name:      "TEST",
		Subjects:  []string{"foo"},
		Retention: nats.InterestPolicy,
		Replicas:  3,
	})
	require_NoError(t, err)

	var subs []*nats.Subscription
	ns := 5

	for i := 0; i < ns; i++ {
		dn := fmt.Sprintf("d%d", i)
		sub, err := js.PullSubscribe("foo", dn)
		require_NoError(t, err)
		subs = append(subs, sub)
	}

	// Send 10 msgs
	n := 10
	for i := 0; i < n; i++ {
		sendStreamMsg(t, nc, "foo", "msg")
	}

	// Collect all the messages.
	var msgs []*nats.Msg
	for _, sub := range subs {
		lmsgs := fetchMsgs(t, sub, n, time.Second)
		if len(lmsgs) != n {
			t.Fatalf("Did not receive all msgs: %d vs %d", len(lmsgs), n)
		}
		msgs = append(msgs, lmsgs...)
	}

	// Shuffle
	rand.Shuffle(len(msgs), func(i, j int) { msgs[i], msgs[j] = msgs[j], msgs[i] })
	for _, m := range msgs {
		require_NoError(t, m.AckSync())
	}

	// Make sure replicated acks are processed.
	checkFor(t, 20*time.Second, 250*time.Millisecond, func() error {
		si, err := js.StreamInfo("TEST")
		if err != nil {
			return err
		}
		if si.State.Msgs != 0 {
			return fmt.Errorf("Should not have any messages left: %d of %d", si.State.Msgs, n)
		}
		return nil
	})
}

func TestJetStreamClusterNoPanicOnStreamInfoWhenNoLeaderYet(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "R3S", 3)
	defer c.shutdown()

	nc := natsConnect(t, c.randomServer().ClientURL())
	defer nc.Close()

	js, _ := nc.JetStream(nats.MaxWait(500 * time.Millisecond))

	wg := sync.WaitGroup{}
	wg.Add(1)
	ch := make(chan struct{})
	go func() {
		defer wg.Done()

		for {
			js.StreamInfo("TEST")
			select {
			case <-ch:
				return
			case <-time.After(15 * time.Millisecond):
			}
		}
	}()

	time.Sleep(250 * time.Millisecond)

	// Don't care if this succeeds or not (could get a context deadline
	// due to the low MaxWait() when creating the context).
	js.AddStream(&nats.StreamConfig{
		Name:     "TEST",
		Subjects: []string{"foo"},
		Replicas: 3,
	})

	close(ch)
	wg.Wait()
}

func TestJetStreamClusterNoTimeoutOnStreamInfoOnPreferredLeader(t *testing.T) {
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

	_, err = js.StreamInfo("TEST")
	require_NoError(t, err)

	// Simulate the preferred stream leader to not have initialized the raft node yet.
	sl := c.streamLeader(globalAccountName, "TEST")
	acc, err := sl.lookupAccount(globalAccountName)
	require_NoError(t, err)
	mset, err := acc.lookupStream("TEST")
	require_NoError(t, err)
	sjs := sl.getJetStream()
	rg := mset.raftGroup()
	sjs.mu.Lock()
	rg.node = nil
	sjs.mu.Unlock()

	// Should not time out on the stream info during this condition.
	_, err = js.StreamInfo("TEST")
	require_NoError(t, err)
}

// Issue https://github.com/nats-io/nats-server/issues/3630
func TestJetStreamClusterPullConsumerAcksExtendInactivityThreshold(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "R3S", 3)
	defer c.shutdown()

	nc, js := jsClientConnect(t, c.randomServer())
	defer nc.Close()

	js.AddStream(&nats.StreamConfig{
		Name:     "TEST",
		Subjects: []string{"foo"},
		Replicas: 3,
	})

	n := 10
	for i := 0; i < n; i++ {
		sendStreamMsg(t, nc, "foo", "msg")
	}

	// Pull Consumer
	sub, err := js.PullSubscribe("foo", "d", nats.InactiveThreshold(time.Second), nats.AckWait(time.Second))
	require_NoError(t, err)

	fetchMsgs(t, sub, n/2, time.Second)
	// Will wait for .5s.
	time.Sleep(500 * time.Millisecond)
	msgs := fetchMsgs(t, sub, n/2, time.Second)
	if len(msgs) != n/2 {
		t.Fatalf("Did not receive msgs: %d vs %d", len(msgs), n/2)
	}

	// Wait for .5s.
	time.Sleep(500 * time.Millisecond)
	msgs[0].Ack() // Ack
	// Wait another .5s.
	time.Sleep(500 * time.Millisecond)
	msgs[1].Nak() // Nak
	// Wait another .5s.
	time.Sleep(500 * time.Millisecond)
	msgs[2].Term() // Term
	time.Sleep(500 * time.Millisecond)
	msgs[3].InProgress() // WIP

	// The above should have kept the consumer alive.
	_, err = js.ConsumerInfo("TEST", "d")
	require_NoError(t, err)

	// Make sure it gets cleaned up.
	time.Sleep(3500 * time.Millisecond)
	_, err = js.ConsumerInfo("TEST", "d")
	require_Error(t, err, nats.ErrConsumerNotFound)
}

// https://github.com/nats-io/nats-server/issues/3677
func TestJetStreamClusterParallelStreamCreation(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "R3S", 3)
	defer c.shutdown()

	np := 100

	startCh := make(chan bool)
	errCh := make(chan error, np)

	wg := sync.WaitGroup{}
	wg.Add(np)

	start := sync.WaitGroup{}
	start.Add(np)

	for i := 0; i < np; i++ {
		go func() {
			defer wg.Done()

			// Individual connection
			nc, js := jsClientConnect(t, c.randomServer())
			defer nc.Close()
			// Signal we are ready
			start.Done()
			// Make them all fire at once.
			<-startCh

			if _, err := js.AddStream(&nats.StreamConfig{
				Name:     "TEST",
				Subjects: []string{"common.*.*"},
				Replicas: 3,
			}); err != nil {
				errCh <- err
			}
		}()
	}

	start.Wait()
	close(startCh)
	wg.Wait()

	if len(errCh) > 0 {
		t.Fatalf("Expected no errors, got %d: %v", len(errCh), <-errCh)
	}

	// We had a bug during parallel stream creation as well that would overwrite the sync subject used for catchups, etc.
	// Test that here as well by shutting down a non-leader, adding a whole bunch of messages, and making sure on restart
	// we properly recover.
	nl := c.randomNonStreamLeader(globalAccountName, "TEST")
	nl.Shutdown()

	nc, js := jsClientConnect(t, c.randomServer())
	defer nc.Close()

	msg := bytes.Repeat([]byte("Z"), 128)
	for i := 0; i < 100; i++ {
		js.PublishAsync("common.foo.bar", msg)
	}
	select {
	case <-js.PublishAsyncComplete():
	case <-time.After(5 * time.Second):
		t.Fatalf("Did not receive completion signal")
	}
	// We need to force the leader to do a snapshot so we kick in upper layer catchup which depends on syncSubject.
	sl := c.streamLeader(globalAccountName, "TEST")
	mset, err := sl.GlobalAccount().lookupStream("TEST")
	require_NoError(t, err)
	node := mset.raftNode()
	require_NotNil(t, node)
	node.InstallSnapshot(mset.stateSnapshot(), false)

	nl = c.restartServer(nl)
	c.waitOnStreamCurrent(nl, globalAccountName, "TEST")

	mset, err = nl.GlobalAccount().lookupStream("TEST")
	require_NoError(t, err)

	// Check state directly.
	checkFor(t, 10*time.Second, 250*time.Millisecond, func() error {
		mset.mu.Lock()
		defer mset.mu.Unlock()
		var state StreamState
		mset.store.FastState(&state)
		if state.Msgs != 100 {
			return fmt.Errorf("expected 100 msgs, got %d", state.Msgs)
		}
		if state.FirstSeq != 1 {
			return fmt.Errorf("expected first sequence 1, got %d", state.FirstSeq)
		}
		if state.LastSeq != 100 {
			return fmt.Errorf("expected last sequence 100, got %d", state.LastSeq)
		}
		return nil
	})
}

// In addition to test above, if streams were attempted to be created in parallel
// it could be that multiple raft groups would be created for the same asset.
func TestJetStreamClusterParallelStreamCreationDupeRaftGroups(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "R3S", 3)
	defer c.shutdown()

	np := 20

	startCh := make(chan bool)
	wg := sync.WaitGroup{}
	wg.Add(np)
	for i := 0; i < np; i++ {
		go func() {
			defer wg.Done()

			// Individual connection
			nc, _ := jsClientConnect(t, c.randomServer())
			js, _ := nc.JetStream(nats.MaxWait(time.Second))
			defer nc.Close()

			// Make them all fire at once.
			<-startCh

			// Ignore errors in this test, care about raft group and metastate.
			js.AddStream(&nats.StreamConfig{
				Name:     "TEST",
				Subjects: []string{"common.*.*"},
				Replicas: 3,
			})
		}()
	}

	close(startCh)
	wg.Wait()

	// Restart a server too.
	s := c.randomServer()
	s.Shutdown()
	s = c.restartServer(s)
	c.waitOnLeader()
	c.waitOnStreamLeader(globalAccountName, "TEST")
	// Check that this server has only two active raft nodes after restart.
	if nrn := s.numRaftNodes(); nrn != 2 {
		t.Fatalf("Expected only two active raft nodes, got %d", nrn)
	}

	// Make sure we only have 2 unique raft groups for all servers.
	// One for meta, one for stream.
	expected := 2
	rg := make(map[string]struct{})
	for _, s := range c.servers {
		s.rnMu.RLock()
		for _, ni := range s.raftNodes {
			n := ni.(*raft)
			rg[n.Group()] = struct{}{}
		}
		s.rnMu.RUnlock()
	}
	if len(rg) != expected {
		t.Fatalf("Expected only %d distinct raft groups for all servers, go %d", expected, len(rg))
	}
}

func TestJetStreamClusterParallelConsumerCreation(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "R3S", 3)
	defer c.shutdown()

	nc, js := jsClientConnectNewAPI(t, c.randomServer())
	defer nc.Close()

	ctx := context.Background()

	_, err := js.CreateStream(ctx, jetstream.StreamConfig{
		Name:     "TEST",
		Subjects: []string{"common.*.*"},
		Replicas: 3,
	})
	require_NoError(t, err)
	c.waitOnStreamLeader(globalAccountName, "TEST")

	np := 50

	startCh := make(chan bool)
	errCh := make(chan error, np)

	cfg := jetstream.ConsumerConfig{
		Durable:  "dlc",
		Replicas: 3,
	}

	wg := sync.WaitGroup{}
	swg := sync.WaitGroup{}
	wg.Add(np)
	swg.Add(np)

	for i := 0; i < np; i++ {
		go func() {
			defer wg.Done()

			// Individual connection
			nc, js := jsClientConnectNewAPI(t, c.randomServer())
			defer nc.Close()

			swg.Done()

			// Make them all fire at once.
			<-startCh

			if _, err := js.CreateConsumer(ctx, "TEST", cfg); err != nil {
				errCh <- err
			}
		}()
	}

	swg.Wait()
	close(startCh)

	wg.Wait()

	if len(errCh) > 0 {
		t.Fatalf("Expected no errors, got %d: %v", len(errCh), <-errCh)
	}

	// Make sure we only have 3 unique raft groups for all servers.
	// One for meta, one for stream, one for consumer.
	expected := 3
	rg := make(map[string]struct{})
	for _, s := range c.servers {
		s.rnMu.RLock()
		for _, ni := range s.raftNodes {
			n := ni.(*raft)
			rg[n.Group()] = struct{}{}
		}
		s.rnMu.RUnlock()
	}
	if len(rg) != expected {
		t.Fatalf("Expected only %d distinct raft groups for all servers, go %d", expected, len(rg))
	}
}

func TestJetStreamClusterGhostEphemeralsAfterRestart(t *testing.T) {
	consumerNotActiveStartInterval = time.Second
	consumerNotActiveMaxInterval = time.Second
	t.Cleanup(func() {
		consumerNotActiveStartInterval = defaultConsumerNotActiveStartInterval
		consumerNotActiveMaxInterval = defaultConsumerNotActiveMaxInterval
	})

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

	// Add in 100 memory based ephemerals.
	for i := 0; i < 100; i++ {
		_, err = js.AddConsumer("TEST", &nats.ConsumerConfig{
			Replicas:          1,
			InactiveThreshold: time.Second,
			MemoryStorage:     true,
		})
		require_NoError(t, err)
	}

	// Grab random server.
	rs := c.randomServer()
	// Now shutdown cluster.
	c.stopAll()

	// Let the consumers all expire.
	time.Sleep(2 * time.Second)

	// Restart first and wait so that we know it will try cleanup without a metaleader.
	// It will fail as there's no metaleader at that time, it should keep retrying on an interval.
	c.restartServer(rs)
	time.Sleep(time.Second)

	c.restartAll()
	c.waitOnLeader()
	c.waitOnStreamLeader(globalAccountName, "TEST")

	nc, _ = jsClientConnect(t, c.randomServer())
	defer nc.Close()

	subj := fmt.Sprintf(JSApiConsumerListT, "TEST")
	checkFor(t, 20*time.Second, 200*time.Millisecond, func() error {
		// Request will take at most 4 seconds if some consumers can't be found.
		m, err := nc.Request(subj, nil, 5*time.Second)
		if err != nil {
			return err
		}
		var resp JSApiConsumerListResponse
		err = json.Unmarshal(m.Data, &resp)
		require_NoError(t, err)
		if len(resp.Consumers) != 0 {
			return fmt.Errorf("Still have %d consumers", len(resp.Consumers))
		}
		if len(resp.Missing) != 0 {
			return fmt.Errorf("Still have %d missing consumers", len(resp.Missing))
		}

		return nil
	})
}

func TestJetStreamClusterConsumerAssignmentSameIdentity(t *testing.T) {
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
		Replicas:  1,
		AckPolicy: nats.AckExplicitPolicy,
	})
	require_NoError(t, err)

	// Snapshot the original assignment from the meta leader.
	captureCa := func() *consumerAssignment {
		ml := c.leader()
		require_NotNil(t, ml)
		mjs := ml.getJetStream()
		require_NotNil(t, mjs)
		mjs.mu.RLock()
		defer mjs.mu.RUnlock()
		ca := mjs.consumerAssignmentOrInflight(globalAccountName, "TEST", "CONSUMER")
		require_NotNil(t, ca)
		// Clone so it survives subsequent delete/recreate of the live entry.
		return ca.clone()
	}
	oldCa := captureCa()

	// A config update keeps the same Name/Stream/Group/Created, so an inflight
	// deleteNotActive holding the prior ca must still consider this the same
	// logical consumer.
	_, err = js.UpdateConsumer("TEST", &nats.ConsumerConfig{
		Durable:           "CONSUMER",
		Replicas:          1,
		AckPolicy:         nats.AckExplicitPolicy,
		InactiveThreshold: time.Hour,
	})
	require_NoError(t, err)

	checkFor(t, 2*time.Second, 200*time.Millisecond, func() error {
		updatedCa := captureCa()
		if updatedCa.Config.InactiveThreshold != time.Hour {
			return fmt.Errorf("update not yet reflected, got %v", updatedCa.Config.InactiveThreshold)
		}
		if !oldCa.sameIdentity(updatedCa) {
			return fmt.Errorf("expected same identity across update")
		}
		return nil
	})

	// Delete and recreate. The new consumer must have a distinct identity so a
	// stale deleteNotActive holding oldCa would correctly skip the proposal.
	require_NoError(t, js.DeleteConsumer("TEST", "CONSUMER"))
	_, err = js.AddConsumer("TEST", &nats.ConsumerConfig{
		Durable:   "CONSUMER",
		Replicas:  1,
		AckPolicy: nats.AckExplicitPolicy,
	})
	require_NoError(t, err)

	checkFor(t, 2*time.Second, 200*time.Millisecond, func() error {
		newCa := captureCa()
		if oldCa.sameIdentity(newCa) {
			return fmt.Errorf("expected different identity across recreate (old.Created=%v new.Created=%v old.Group=%q new.Group=%q)",
				oldCa.Created, newCa.Created, oldCa.Group.Name, newCa.Group.Name)
		}
		return nil
	})
}

func TestJetStreamClusterDeleteNotActiveOnFollowerDoesNotDeleteConsumer(t *testing.T) {
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

	// Long InactiveThreshold so the normal timer does not fire during the test.
	_, err = js.AddConsumer("TEST", &nats.ConsumerConfig{
		Durable:           "CONSUMER",
		AckPolicy:         nats.AckExplicitPolicy,
		Replicas:          3,
		InactiveThreshold: time.Hour,
	})
	require_NoError(t, err)

	cl := c.consumerLeader(globalAccountName, "TEST", "CONSUMER")
	require_NotNil(t, cl)
	cf := c.randomNonConsumerLeader(globalAccountName, "TEST", "CONSUMER")
	require_NotNil(t, cf)
	require_NotEqual(t, cl, cf)

	// Get the follower's local consumer object.
	mset, err := cf.globalAccount().lookupStream("TEST")
	require_NoError(t, err)
	o := mset.lookupConsumer("CONSUMER")
	require_NotNil(t, o)
	require_False(t, o.isLeader())

	// Simulate a stale cleanup timer firing post-stepdown on a follower.
	go o.deleteNotActive()

	// Give any erroneous delete proposal time to apply.
	time.Sleep(2 * time.Second)

	_, err = js.ConsumerInfo("TEST", "CONSUMER")
	require_NoError(t, err)
}

func TestJetStreamClusterReplacementPolicyAfterPeerRemove(t *testing.T) {
	// R3 scenario where there is a redundant node in each unique cloud so removing a peer should result in
	// an immediate replacement also preserving cloud uniqueness.

	sc := createJetStreamClusterExplicit(t, "PR9", 9)
	sc.waitOnPeerCount(9)

	reset := func(s *Server) {
		s.mu.Lock()
		rch := s.sys.resetCh
		s.mu.Unlock()
		if rch != nil {
			rch <- struct{}{}
		}
		s.sendStatszUpdate()
	}

	tags := []string{"cloud:aws", "cloud:aws", "cloud:aws", "cloud:gcp", "cloud:gcp", "cloud:gcp", "cloud:az", "cloud:az", "cloud:az"}

	var serverUTags = make(map[string]string)

	for i, s := range sc.servers {
		s.optsMu.Lock()
		serverUTags[s.Name()] = tags[i]
		s.opts.Tags.Add(tags[i])
		s.opts.JetStreamUniqueTag = "cloud"
		s.optsMu.Unlock()
		reset(s)
	}

	ml := sc.leader()
	js := ml.getJetStream()
	require_True(t, js != nil)
	js.mu.RLock()
	cc := js.cluster
	require_True(t, cc != nil)

	// Walk and make sure all tags are registered.
	expires := time.Now().Add(10 * time.Second)
	for time.Now().Before(expires) {
		allOK := true
		for _, p := range cc.meta.Peers() {
			si, ok := ml.nodeToInfo.Load(p.ID)
			require_True(t, ok)
			ni := si.(nodeInfo)
			if len(ni.tags) == 0 {
				allOK = false
				reset(sc.serverByName(ni.name))
			}
		}
		if allOK {
			break
		}
	}
	js.mu.RUnlock()
	defer sc.shutdown()

	sc.waitOnClusterReadyWithNumPeers(9)

	s := sc.leader()
	nc, jsc := jsClientConnect(t, s)
	defer nc.Close()

	_, err := jsc.AddStream(&nats.StreamConfig{
		Name:     "TEST",
		Subjects: []string{"foo"},
		Replicas: 3,
	})
	require_NoError(t, err)

	sc.waitOnStreamLeader(globalAccountName, "TEST")

	osi, err := jsc.StreamInfo("TEST")
	require_NoError(t, err)

	// Double check original placement honors unique_tag
	var uTags = make(map[string]struct{})

	uTags[serverUTags[osi.Cluster.Leader]] = struct{}{}
	for _, replica := range osi.Cluster.Replicas {
		evalTag := serverUTags[replica.Name]
		if _, exists := uTags[evalTag]; !exists {
			uTags[evalTag] = struct{}{}
			continue
		} else {
			t.Fatalf("expected initial placement to honor unique_tag")
		}
	}

	// Remove a peer and select replacement 5 times to avoid false good
	for i := 0; i < 5; i++ {
		// Remove 1 peer replica (this will be random cloud region as initial placement was randomized ordering)
		// After each successful iteration, osi will reflect the current RG peers
		toRemove := osi.Cluster.Replicas[0].Name
		resp, err := nc.Request(fmt.Sprintf(JSApiStreamRemovePeerT, "TEST"), []byte(`{"peer":"`+toRemove+`"}`), time.Second)
		require_NoError(t, err)
		var rpResp JSApiStreamRemovePeerResponse
		err = json.Unmarshal(resp.Data, &rpResp)
		require_NoError(t, err)
		require_True(t, rpResp.Success)

		sc.waitOnStreamLeader(globalAccountName, "TEST")

		checkFor(t, time.Second, 200*time.Millisecond, func() error {
			osi, err = jsc.StreamInfo("TEST")
			require_NoError(t, err)
			if len(osi.Cluster.Replicas) != 2 {
				return fmt.Errorf("expected R3, got R%d", len(osi.Cluster.Replicas)+1)
			}
			// STREAM.PEER.REMOVE is asynchronous command; make sure remove has occurred by
			// checking that the toRemove peer is gone.
			for _, replica := range osi.Cluster.Replicas {
				if replica.Name == toRemove {
					return fmt.Errorf("expected replaced replica, old replica still present")
				}
			}
			return nil
		})

		// Validate that replacement with new peer still honors
		uTags = make(map[string]struct{}) //reset

		uTags[serverUTags[osi.Cluster.Leader]] = struct{}{}
		for _, replica := range osi.Cluster.Replicas {
			evalTag := serverUTags[replica.Name]
			if _, exists := uTags[evalTag]; !exists {
				uTags[evalTag] = struct{}{}
				continue
			} else {
				t.Fatalf("expected new peer and revised placement to honor unique_tag")
			}
		}
	}
}

func TestJetStreamClusterReplacementPolicyAfterPeerRemoveNoPlace(t *testing.T) {
	// R3 scenario where there are exactly three unique cloud nodes, so removing a peer should NOT
	// result in a new peer

	sc := createJetStreamClusterExplicit(t, "threeup", 3)
	sc.waitOnPeerCount(3)

	reset := func(s *Server) {
		s.mu.Lock()
		rch := s.sys.resetCh
		s.mu.Unlock()
		if rch != nil {
			rch <- struct{}{}
		}
		s.sendStatszUpdate()
	}

	tags := []string{"cloud:aws", "cloud:gcp", "cloud:az"}

	var serverUTags = make(map[string]string)

	for i, s := range sc.servers {
		s.optsMu.Lock()
		serverUTags[s.Name()] = tags[i]
		s.opts.Tags.Add(tags[i])
		s.opts.JetStreamUniqueTag = "cloud"
		s.optsMu.Unlock()
		reset(s)
	}

	ml := sc.leader()
	js := ml.getJetStream()
	require_True(t, js != nil)
	js.mu.RLock()
	cc := js.cluster
	require_True(t, cc != nil)

	// Walk and make sure all tags are registered.
	expires := time.Now().Add(10 * time.Second)
	for time.Now().Before(expires) {
		allOK := true
		for _, p := range cc.meta.Peers() {
			si, ok := ml.nodeToInfo.Load(p.ID)
			require_True(t, ok)
			ni := si.(nodeInfo)
			if len(ni.tags) == 0 {
				allOK = false
				reset(sc.serverByName(ni.name))
			}
		}
		if allOK {
			break
		}
	}
	js.mu.RUnlock()
	defer sc.shutdown()

	sc.waitOnClusterReadyWithNumPeers(3)

	s := sc.leader()
	nc, jsc := jsClientConnect(t, s)
	defer nc.Close()

	_, err := jsc.AddStream(&nats.StreamConfig{
		Name:     "TEST",
		Subjects: []string{"foo"},
		Replicas: 3,
	})
	require_NoError(t, err)

	sc.waitOnStreamLeader(globalAccountName, "TEST")

	osi, err := jsc.StreamInfo("TEST")
	require_NoError(t, err)

	// Double check original placement honors unique_tag
	var uTags = make(map[string]struct{})

	uTags[serverUTags[osi.Cluster.Leader]] = struct{}{}
	for _, replica := range osi.Cluster.Replicas {
		evalTag := serverUTags[replica.Name]
		if _, exists := uTags[evalTag]; !exists {
			uTags[evalTag] = struct{}{}
			continue
		} else {
			t.Fatalf("expected initial placement to honor unique_tag")
		}
	}

	// Evacuate 1 peer replica (this will be random cloud region as initial placement was
	// randomized ordering). A stream scoped peer remove would be rejected here, there is no
	// eligible replacement, so go through the server scoped endpoint which is best effort.
	snc, _ := jsClientConnect(t, s, nats.UserInfo("admin", "s3cr3t!"))
	defer snc.Close()
	ereq, err := json.Marshal(JSApiMetaServerEvacuateRequest{Server: osi.Cluster.Replicas[0].Name})
	require_NoError(t, err)
	emsg, err := snc.Request(JSApiEvacuateServer, ereq, time.Second*10)
	require_NoError(t, err)
	var eresp JSApiMetaServerEvacuateResponse
	require_NoError(t, json.Unmarshal(emsg.Data, &eresp))
	require_True(t, eresp.Success)

	sc.waitOnStreamLeader(globalAccountName, "TEST")

	// Verify R2 since no eligible peer can replace the removed peer without braking unique constraint
	checkFor(t, 10*time.Second, 200*time.Millisecond, func() error {
		osi, err = jsc.StreamInfo("TEST")
		require_NoError(t, err)
		if len(osi.Cluster.Replicas) != 1 {
			return fmt.Errorf("expected R2, got R%d", len(osi.Cluster.Replicas)+1)
		}
		return nil
	})

	// Validate that remaining members still honor unique tags
	uTags = make(map[string]struct{}) //reset

	uTags[serverUTags[osi.Cluster.Leader]] = struct{}{}
	for _, replica := range osi.Cluster.Replicas {
		evalTag := serverUTags[replica.Name]
		if _, exists := uTags[evalTag]; !exists {
			uTags[evalTag] = struct{}{}
			continue
		} else {
			t.Fatalf("expected revised placement to honor unique_tag")
		}
	}
}

// https://github.com/nats-io/nats-server/issues/3191
func TestJetStreamClusterLeafnodeDuplicateConsumerMessages(t *testing.T) {
	// Cluster B
	c := createJetStreamCluster(t, jsClusterTempl, "B", _EMPTY_, 2, 22020, false)
	defer c.shutdown()

	// Cluster A
	// Domain is "A'
	lc := c.createLeafNodesWithStartPortAndDomain("A", 2, 22110, "A")
	defer lc.shutdown()

	lc.waitOnClusterReady()

	// We want A-S-1 connected to B-S-1 and A-S-2 connected to B-S-2
	// So adjust if needed.
	checkFor(t, 5*time.Second, 100*time.Millisecond, func() error {
		for i, ls := range lc.servers {
			ls.mu.RLock()
			var remoteServer string
			for _, rc := range ls.leafs {
				rc.mu.Lock()
				remoteServer = rc.leaf.remoteServer
				rc.mu.Unlock()
				break
			}
			ls.mu.RUnlock()

			wantedRemote := fmt.Sprintf("S-%d", i+1)
			if remoteServer != wantedRemote {
				ls.Shutdown()
				lc.restartServer(ls)
				return fmt.Errorf("Leafnode server %d not connected to %q", i+1, wantedRemote)
			}
		}
		return nil
	})

	// Wait on ready again.
	lc.waitOnClusterReady()

	// Create a stream and a durable pull consumer on cluster A.
	lnc, ljs := jsClientConnect(t, lc.randomServer())
	defer lnc.Close()

	_, err := ljs.AddStream(&nats.StreamConfig{
		Name:     "TEST",
		Subjects: []string{"foo"},
		Replicas: 2,
	})
	require_NoError(t, err)

	// Make sure stream leader is on S-1
	checkFor(t, 5*time.Second, 100*time.Millisecond, func() error {
		si, err := ljs.StreamInfo("TEST")
		require_NoError(t, err)
		if si.Cluster.Leader == "A-S-1" {
			return nil
		}
		_, err = lnc.Request(fmt.Sprintf(JSApiStreamLeaderStepDownT, "TEST"), nil, time.Second)
		require_NoError(t, err)
		return fmt.Errorf("Stream leader not placed on A-S-1")
	})

	_, err = ljs.StreamInfo("TEST")
	require_NoError(t, err)

	_, err = ljs.AddConsumer("TEST", &nats.ConsumerConfig{
		Durable:    "dlc",
		Replicas:   2,
		MaxDeliver: 1,
		AckPolicy:  nats.AckNonePolicy,
	})
	require_NoError(t, err)

	// Make sure consumer leader is on S-2
	checkFor(t, 5*time.Second, 100*time.Millisecond, func() error {
		ci, err := ljs.ConsumerInfo("TEST", "dlc")
		require_NoError(t, err)
		if ci.Cluster.Leader == "A-S-2" {
			return nil
		}
		_, err = lnc.Request(fmt.Sprintf(JSApiConsumerLeaderStepDownT, "TEST", "dlc"), nil, time.Second)
		require_NoError(t, err)
		return fmt.Errorf("Stream leader not placed on A-S-1")
	})

	_, err = ljs.ConsumerInfo("TEST", "dlc")
	require_NoError(t, err)

	// Send 2 messages.
	sendStreamMsg(t, lnc, "foo", "M-1")
	sendStreamMsg(t, lnc, "foo", "M-2")

	// Now bind apps to cluster B servers and bind to pull consumer.
	nc1, _ := jsClientConnect(t, c.servers[0])
	defer nc1.Close()
	js1, err := nc1.JetStream(nats.Domain("A"))
	require_NoError(t, err)

	sub1, err := js1.PullSubscribe("foo", "dlc", nats.BindStream("TEST"))
	require_NoError(t, err)
	defer sub1.Unsubscribe()

	nc2, _ := jsClientConnect(t, c.servers[1])
	defer nc2.Close()
	js2, err := nc2.JetStream(nats.Domain("A"))
	require_NoError(t, err)

	sub2, err := js2.PullSubscribe("foo", "dlc", nats.BindStream("TEST"))
	require_NoError(t, err)
	defer sub2.Unsubscribe()

	// Make sure we can properly get messages.
	msgs, err := sub1.Fetch(1)
	require_NoError(t, err)
	require_True(t, len(msgs) == 1)
	require_True(t, string(msgs[0].Data) == "M-1")

	msgs, err = sub2.Fetch(1)
	require_NoError(t, err)
	require_True(t, len(msgs) == 1)
	require_True(t, string(msgs[0].Data) == "M-2")

	// Make sure delivered state makes it to other server to not accidentally send M-2 again
	// and fail the test below.
	time.Sleep(250 * time.Millisecond)

	// Now let's introduce and event, where A-S-2 will now reconnect after a restart to B-S-2
	checkFor(t, 5*time.Second, 100*time.Millisecond, func() error {
		ls := lc.servers[1]
		wantedRemote := "S-1"
		var remoteServer string

		ls.mu.RLock()
		for _, rc := range ls.leafs {
			rc.mu.Lock()
			remoteServer = rc.leaf.remoteServer
			rc.mu.Unlock()
			break
		}
		ls.mu.RUnlock()

		if remoteServer != wantedRemote {
			ls.Shutdown()
			lc.restartServer(ls)
			return fmt.Errorf("Leafnode server not connected to %q", wantedRemote)
		}
		return nil
	})

	// Wait on ready again.
	lc.waitOnClusterReady()
	lc.waitOnStreamLeader(globalAccountName, "TEST")
	lc.waitOnConsumerLeader(globalAccountName, "TEST", "dlc")

	// Send 2 more messages.
	sendStreamMsg(t, lnc, "foo", "M-3")
	sendStreamMsg(t, lnc, "foo", "M-4")

	msgs, err = sub1.Fetch(2)
	require_NoError(t, err)
	require_True(t, len(msgs) == 2)
	require_True(t, string(msgs[0].Data) == "M-3")
	require_True(t, string(msgs[1].Data) == "M-4")

	// Send 2 more messages.
	sendStreamMsg(t, lnc, "foo", "M-5")
	sendStreamMsg(t, lnc, "foo", "M-6")

	msgs, err = sub2.Fetch(2)
	require_NoError(t, err)
	require_True(t, len(msgs) == 2)
	require_True(t, string(msgs[0].Data) == "M-5")
	require_True(t, string(msgs[1].Data) == "M-6")
}

func snapRGSet(pFlag bool, banner string, osi *nats.StreamInfo) *map[string]struct{} {
	var snapSet = make(map[string]struct{})
	if pFlag {
		fmt.Println(banner)
	}
	if osi == nil {
		if pFlag {
			fmt.Printf("bonkers!\n")
		}
		return nil
	}

	snapSet[osi.Cluster.Leader] = struct{}{}
	if pFlag {
		fmt.Printf("Leader: %s\n", osi.Cluster.Leader)
	}
	for _, replica := range osi.Cluster.Replicas {
		snapSet[replica.Name] = struct{}{}
		if pFlag {
			fmt.Printf("Replica: %s\n", replica.Name)
		}
	}

	return &snapSet
}

func TestJetStreamClusterAfterPeerRemoveZeroState(t *testing.T) {
	// R3 scenario (w/messages) in a 4-node cluster. Peer remove from RG and add back to same RG later.
	// Validate that original peer brought no memory or issues from its previous RG tour of duty, specifically
	// that the restored peer has the correct filestore usage bytes for the asset.
	var err error

	sc := createJetStreamClusterExplicit(t, "cl4", 4)
	defer sc.shutdown()

	sc.waitOnClusterReadyWithNumPeers(4)

	s := sc.leader()
	nc, jsc := jsClientConnect(t, s)
	defer nc.Close()

	_, err = jsc.AddStream(&nats.StreamConfig{
		Name:     "foo",
		Subjects: []string{"foo.*"},
		Replicas: 3,
	})
	require_NoError(t, err)

	sc.waitOnStreamLeader(globalAccountName, "foo")

	osi, err := jsc.StreamInfo("foo")
	require_NoError(t, err)

	// make sure 0 msgs
	require_True(t, osi.State.Msgs == 0)

	// load up messages
	toSend := 10000
	// storage bytes with JS message overhead
	assetStoreBytesExpected := uint64(460000)

	for i := 1; i <= toSend; i++ {
		msg := []byte("Hello World")
		if _, err = jsc.Publish("foo.a", msg); err != nil {
			t.Fatalf("unexpected publish error: %v", err)
		}
	}

	osi, err = jsc.StreamInfo("foo")
	require_NoError(t, err)

	// make sure 10000 msgs
	require_True(t, osi.State.Msgs == uint64(toSend))

	origSet := *snapRGSet(false, "== Orig RG Set ==", osi)

	// remove 1 peer replica (1 of 2 non-leaders)
	origPeer := osi.Cluster.Replicas[0].Name
	resp, err := nc.Request(fmt.Sprintf(JSApiStreamRemovePeerT, "foo"), []byte(`{"peer":"`+origPeer+`"}`), time.Second)
	require_NoError(t, err)
	var rpResp JSApiStreamRemovePeerResponse
	err = json.Unmarshal(resp.Data, &rpResp)
	require_NoError(t, err)
	require_True(t, rpResp.Success)

	// validate the origPeer is removed with a replacement newPeer
	sc.waitOnStreamLeader(globalAccountName, "foo")
	checkFor(t, 10*time.Second, 200*time.Millisecond, func() error {
		osi, err = jsc.StreamInfo("foo")
		require_NoError(t, err)
		if len(osi.Cluster.Replicas) != 2 {
			return fmt.Errorf("expected R3, got R%d", len(osi.Cluster.Replicas)+1)
		}
		// STREAM.PEER.REMOVE is asynchronous command; make sure remove has occurred
		for _, replica := range osi.Cluster.Replicas {
			if replica.Name == origPeer {
				return fmt.Errorf("expected replaced replica, old replica still present")
			}
		}
		return nil
	})

	// identify the new peer
	var newPeer string
	osi, err = jsc.StreamInfo("foo")
	require_NoError(t, err)
	newSet := *snapRGSet(false, "== New RG Set ==", osi)
	for peer := range newSet {
		_, ok := origSet[peer]
		if !ok {
			newPeer = peer
			break
		}
	}
	require_True(t, newPeer != "")

	// kick out newPeer which will cause origPeer to be assigned to the RG again
	resp, err = nc.Request(fmt.Sprintf(JSApiStreamRemovePeerT, "foo"), []byte(`{"peer":"`+newPeer+`"}`), time.Second)
	require_NoError(t, err)
	err = json.Unmarshal(resp.Data, &rpResp)
	require_NoError(t, err)
	require_True(t, rpResp.Success)

	// validate the newPeer is removed and R3 has reformed (with origPeer)
	sc.waitOnStreamLeader(globalAccountName, "foo")
	checkFor(t, 10*time.Second, 200*time.Millisecond, func() error {
		osi, err = jsc.StreamInfo("foo")
		require_NoError(t, err)
		if len(osi.Cluster.Replicas) != 2 {
			return fmt.Errorf("expected R3, got R%d", len(osi.Cluster.Replicas)+1)
		}
		// STREAM.PEER.REMOVE is asynchronous command; make sure remove has occurred
		for _, replica := range osi.Cluster.Replicas {
			if replica.Name == newPeer {
				return fmt.Errorf("expected replaced replica, old replica still present")
			}
		}
		return nil
	})

	osi, err = jsc.StreamInfo("foo")
	require_NoError(t, err)

	// make sure all msgs reported in stream at this point with original leader
	require_True(t, osi.State.Msgs == uint64(toSend))

	snapRGSet(false, "== RG Set w/origPeer Back ==", osi)

	// get a handle to original peer server
	var origServer *Server = sc.serverByName(origPeer)
	if origServer == nil {
		t.Fatalf("expected to get a handle to original peer server by name")
	}

	checkFor(t, 10*time.Second, 200*time.Millisecond, func() error {
		jszResult, err := origServer.Jsz(nil)
		require_NoError(t, err)
		if jszResult.Store != assetStoreBytesExpected {
			return fmt.Errorf("expected %d storage on orig peer, got %d", assetStoreBytesExpected, jszResult.Store)
		}
		return nil
	})
}

func TestJetStreamClusterMemLeaderRestart(t *testing.T) {
	// Test if R3 clustered mem store asset leader server restarted, that asset remains stable with final quorum
	c := createJetStreamClusterExplicit(t, "R3S", 3)
	defer c.shutdown()

	ml := c.leader()
	nc, jsc := jsClientConnect(t, ml)
	defer nc.Close()

	_, err := jsc.AddStream(&nats.StreamConfig{
		Name:     "foo",
		Storage:  nats.MemoryStorage,
		Subjects: []string{"foo.*"},
		Replicas: 3,
	})
	require_NoError(t, err)

	// load up messages
	toSend := 10000
	for i := 1; i <= toSend; i++ {
		msg := []byte("Hello World")
		if _, err = jsc.Publish("foo.a", msg); err != nil {
			t.Fatalf("unexpected publish error: %v", err)
		}
	}

	osi, err := jsc.StreamInfo("foo")
	require_NoError(t, err)
	// make sure 10000 msgs
	require_True(t, osi.State.Msgs == uint64(toSend))

	// Shutdown the stream leader server
	rs := c.serverByName(osi.Cluster.Leader)
	rs.Shutdown()

	// Make sure that we have a META leader (there can always be a re-election)
	c.waitOnLeader()
	c.waitOnStreamLeader(globalAccountName, "foo")

	// Should still have quorum and a new leader
	checkFor(t, 5*time.Second, 200*time.Millisecond, func() error {
		osi, err = jsc.StreamInfo("foo")
		if err != nil {
			return fmt.Errorf("expected healthy stream asset, got %s", err.Error())
		}
		if osi.Cluster.Leader == _EMPTY_ {
			return fmt.Errorf("expected healthy stream asset with new leader")
		}
		if osi.State.Msgs != uint64(toSend) {
			return fmt.Errorf("expected healthy stream asset %d messages, got %d messages", toSend, osi.State.Msgs)
		}
		return nil
	})

	// Now restart the old leader peer (old stream state)
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

	// Can we get stream info after return
	osi, err = jsc.StreamInfo("foo")
	if err != nil {
		t.Fatalf("expected stream asset info return, got %s", err.Error())
	}

	// When asset leader came back did we re-form with quorum
	if osi.Cluster.Leader == "" {
		t.Fatalf("expected a current leader after old leader restarted")
	}
}

// Customer reported R1 consumers that seemed to be ghosted after server restart.
func TestJetStreamClusterLostConsumers(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "GHOST", 3)
	defer c.shutdown()

	nc, js := jsClientConnect(t, c.randomServer())
	defer nc.Close()

	_, err := js.AddStream(&nats.StreamConfig{
		Name:     "TEST",
		Subjects: []string{"events.>"},
		Replicas: 3,
	})
	require_NoError(t, err)

	for i := 0; i < 10; i++ {
		for j := 0; j < 10; j++ {
			_, err := js.Publish(fmt.Sprintf("events.%d.%d", i, j), []byte("test"))
			require_NoError(t, err)
		}
	}

	s := c.randomServer()
	s.Shutdown()

	c.waitOnLeader()
	c.waitOnStreamLeader(globalAccountName, "TEST")

	nc, _ = jsClientConnect(t, c.randomServer())
	defer nc.Close()

	cc := CreateConsumerRequest{
		Stream: "TEST",
		Config: ConsumerConfig{
			AckPolicy: AckExplicit,
			Replicas:  1,
		},
	}
	req, err := json.Marshal(cc)
	require_NoError(t, err)

	reqSubj := fmt.Sprintf(JSApiConsumerCreateT, "TEST")

	// Now create 50 consumers. Ensure they are successfully created, so they're included in our snapshot.
	for i := 0; i < 50; i++ {
		_, err = nc.Request(reqSubj, req, time.Second)
		require_NoError(t, err)
	}

	// Grab the meta leader.
	ml := c.leader()
	require_NoError(t, ml.JetStreamSnapshotMeta())

	numConsumerAssignments := func(s *Server) int {
		t.Helper()
		js := s.getJetStream()
		js.mu.RLock()
		defer js.mu.RUnlock()
		cc := js.cluster
		for _, asa := range cc.streams {
			for _, sa := range asa {
				return len(sa.consumers)
			}
		}
		return 0
	}

	checkFor(t, time.Second, 100*time.Millisecond, func() error {
		num := numConsumerAssignments(ml)
		if num == 50 {
			return nil
		}
		return fmt.Errorf("Consumers is only %d", num)
	})

	// Restart the server we shutdown. We snapshotted to the snapshot
	// has to fill in the new consumers.
	// The bug would fail to add them to the meta state since the stream
	// existed.
	s = c.restartServer(s)

	checkFor(t, time.Second, 100*time.Millisecond, func() error {
		num := numConsumerAssignments(s)
		if num == 50 {
			return nil
		}
		return fmt.Errorf("Consumers is only %d", num)
	})
}

// https://github.com/nats-io/nats-server/issues/3636
func TestJetStreamClusterScaleDownDuringServerOffline(t *testing.T) {
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

	for i := 0; i < 100; i++ {
		sendStreamMsg(t, nc, "foo", "hello")
	}

	s := c.randomNonStreamLeader(globalAccountName, "TEST")
	s.Shutdown()

	c.waitOnLeader()

	nc, js = jsClientConnect(t, c.randomServer())
	defer nc.Close()

	_, err = js.UpdateStream(&nats.StreamConfig{
		Name:     "TEST",
		Subjects: []string{"foo"},
		Replicas: 1,
	})
	require_NoError(t, err)

	s = c.restartServer(s)
	checkFor(t, time.Second, 200*time.Millisecond, func() error {
		hs := s.healthz(nil)
		if hs.Error != _EMPTY_ {
			return errors.New(hs.Error)
		}
		return nil
	})
}

// Reported by a customer manually upgrading their streams to support direct gets.
// Worked if single replica but not in clustered mode.
func TestJetStreamClusterDirectGetStreamUpgrade(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "R3S", 3)
	defer c.shutdown()

	nc, js := jsClientConnect(t, c.randomServer())
	defer nc.Close()

	_, err := js.AddStream(&nats.StreamConfig{
		Name:              "KV_TEST",
		Subjects:          []string{"$KV.TEST.>"},
		Discard:           nats.DiscardNew,
		MaxMsgsPerSubject: 1,
		DenyDelete:        true,
		Replicas:          3,
	})
	require_NoError(t, err)

	kv, err := js.KeyValue("TEST")
	require_NoError(t, err)

	_, err = kv.PutString("name", "derek")
	require_NoError(t, err)

	entry, err := kv.Get("name")
	require_NoError(t, err)
	require_True(t, string(entry.Value()) == "derek")

	// Now simulate a update to the stream to support direct gets.
	_, err = js.UpdateStream(&nats.StreamConfig{
		Name:              "KV_TEST",
		Subjects:          []string{"$KV.TEST.>"},
		Discard:           nats.DiscardNew,
		MaxMsgsPerSubject: 1,
		DenyDelete:        true,
		AllowDirect:       true,
		Replicas:          3,
	})
	require_NoError(t, err)

	// Rebind to KV to make sure we DIRECT version of Get().
	kv, err = js.KeyValue("TEST")
	require_NoError(t, err)

	// Make sure direct get works.
	entry, err = kv.Get("name")
	require_NoError(t, err)
	require_True(t, string(entry.Value()) == "derek")
}

// For interest (or workqueue) based streams its important to match the replication factor.
// This was the case but now that more control over consumer creation is allowed its possible
// to create a consumer where the replication factor does not match. This could cause
// instability in the state between servers and cause problems on leader switches.
func TestJetStreamClusterInterestPolicyStreamForConsumersToMatchRFactor(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "R3S", 3)
	defer c.shutdown()

	nc, js := jsClientConnect(t, c.randomServer())
	defer nc.Close()

	_, err := js.AddStream(&nats.StreamConfig{
		Name:      "TEST",
		Subjects:  []string{"foo"},
		Retention: nats.InterestPolicy,
		Replicas:  3,
	})
	require_NoError(t, err)

	_, err = js.AddConsumer("TEST", &nats.ConsumerConfig{
		Durable:   "XX",
		AckPolicy: nats.AckExplicitPolicy,
		Replicas:  1,
	})

	require_Error(t, err, NewJSConsumerReplicasShouldMatchStreamError())
}

// https://github.com/nats-io/nats-server/issues/3791
func TestJetStreamClusterKVWatchersWithServerDown(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "R3S", 3)
	defer c.shutdown()

	nc, js := jsClientConnect(t, c.randomServer())
	defer nc.Close()

	kv, err := js.CreateKeyValue(&nats.KeyValueConfig{
		Bucket:   "TEST",
		Replicas: 3,
	})
	require_NoError(t, err)

	kv.PutString("foo", "bar")
	kv.PutString("foo", "baz")

	// Shutdown a follower.
	s := c.randomNonStreamLeader(globalAccountName, "KV_TEST")
	s.Shutdown()
	c.waitOnLeader()

	nc, _ = jsClientConnect(t, c.randomServer())
	defer nc.Close()

	js, err = nc.JetStream(nats.MaxWait(2 * time.Second))
	require_NoError(t, err)

	kv, err = js.KeyValue("TEST")
	require_NoError(t, err)

	for i := 0; i < 100; i++ {
		w, err := kv.Watch("foo")
		require_NoError(t, err)
		w.Stop()
	}
}

// TestJetStreamClusterCurrentVsHealth is designed to show the
// difference between "current" and "healthy" when async publishes
// outpace the rate at which they can be applied.
func TestJetStreamClusterCurrentVsHealth(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "R3S", 3)
	defer c.shutdown()

	c.waitOnLeader()
	server := c.randomNonLeader()

	nc, js := jsClientConnect(t, server)
	defer nc.Close()

	_, err := js.AddStream(&nats.StreamConfig{
		Name:     "TEST",
		Subjects: []string{"foo"},
		Replicas: 3,
	})
	require_NoError(t, err)

	server = c.randomNonStreamLeader(globalAccountName, "TEST")
	stream, err := server.GlobalAccount().lookupStream("TEST")
	require_NoError(t, err)

	raft, ok := stream.raftGroup().node.(*raft)
	require_True(t, ok)

	for i := 0; i < 1000; i++ {
		_, err := js.PublishAsync("foo", []byte("bar"))
		require_NoError(t, err)

		raft.RLock()
		commit := raft.commit
		applied := raft.applied
		raft.RUnlock()

		current := raft.Current()
		healthy := raft.Healthy()

		if !current || !healthy || commit != applied {
			t.Logf(
				"%d | Current %v, healthy %v, commit %d, applied %d, pending %d",
				i, current, healthy, commit, applied, commit-applied,
			)
		}
	}
}

// Several users and customers use this setup, but many times across leafnodes.
// This should be allowed in same account since we are really protecting against
// multiple pub acks with cycle detection.
func TestJetStreamClusterActiveActiveSourcedStreams(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "R3S", 3)
	defer c.shutdown()

	nc, js := jsClientConnect(t, c.randomServer())
	defer nc.Close()

	_, err := js.AddStream(&nats.StreamConfig{
		Name:     "A",
		Subjects: []string{"A.>"},
	})
	require_NoError(t, err)

	_, err = js.AddStream(&nats.StreamConfig{
		Name:     "B",
		Subjects: []string{"B.>"},
	})
	require_NoError(t, err)

	_, err = js.UpdateStream(&nats.StreamConfig{
		Name:     "A",
		Subjects: []string{"A.>"},
		Sources: []*nats.StreamSource{{
			Name:          "B",
			FilterSubject: "B.>",
		}},
	})
	require_NoError(t, err)

	// Before this would fail.
	_, err = js.UpdateStream(&nats.StreamConfig{
		Name:     "B",
		Subjects: []string{"B.>"},
		Sources: []*nats.StreamSource{{
			Name:          "A",
			FilterSubject: "A.>",
		}},
	})
	require_NoError(t, err)
}

func TestJetStreamClusterUpdateConsumerShouldNotForceDeleteOnRestart(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "R7S", 7)
	defer c.shutdown()

	nc, js := jsClientConnect(t, c.randomServer())
	defer nc.Close()

	_, err := js.AddStream(&nats.StreamConfig{
		Name:     "TEST",
		Subjects: []string{"foo", "bar"},
		Replicas: 3,
	})
	require_NoError(t, err)

	ci, err := js.AddConsumer("TEST", &nats.ConsumerConfig{
		Durable:        "D",
		DeliverSubject: "_no_bind_",
	})
	require_NoError(t, err)

	// Shutdown a consumer follower.
	nc.Close()
	s := c.serverByName(ci.Cluster.Replicas[0].Name)
	s.Shutdown()

	c.waitOnLeader()

	nc, js = jsClientConnect(t, c.randomServer())
	defer nc.Close()

	// Change delivery subject.
	_, err = js.UpdateConsumer("TEST", &nats.ConsumerConfig{
		Durable:        "D",
		DeliverSubject: "_d_",
	})
	require_NoError(t, err)

	// Create interest in new and old deliver subject.
	_, err = nc.SubscribeSync("_d_")
	require_NoError(t, err)
	_, err = nc.SubscribeSync("_no_bind_")
	require_NoError(t, err)
	nc.Flush()

	c.restartServer(s)
	c.waitOnAllCurrent()

	// Wait on bad error that would cleanup consumer.
	time.Sleep(time.Second)

	_, err = js.ConsumerInfo("TEST", "D")
	require_NoError(t, err)
}

func TestJetStreamClusterInterestPolicyEphemeral(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "R3S", 3)
	defer c.shutdown()

	for _, test := range []struct {
		testName string
		stream   string
		subject  string
		durable  string
		name     string
		policy   nats.RetentionPolicy
	}{
		{testName: "LimitsWithName", name: "eph", subject: "limeph", stream: "LIMIT_EPH", policy: nats.LimitsPolicy},
		{testName: "InterestWithDurable", durable: "eph", subject: "intdur", stream: "INT_DUR", policy: nats.InterestPolicy},
		{testName: "InterestWithName", name: "eph", subject: "inteph", stream: "INT_EPH", policy: nats.InterestPolicy},
	} {
		t.Run(test.testName, func(t *testing.T) {
			var err error

			nc, js := jsClientConnect(t, c.randomServer())
			defer nc.Close()

			_, err = js.AddStream(&nats.StreamConfig{
				Name:      test.stream,
				Subjects:  []string{test.subject},
				Retention: test.policy,
				Replicas:  3,
			})
			require_NoError(t, err)

			const inactiveThreshold = time.Second

			_, err = js.AddConsumer(test.stream, &nats.ConsumerConfig{
				DeliverSubject:    nats.NewInbox(),
				AckPolicy:         nats.AckExplicitPolicy,
				InactiveThreshold: inactiveThreshold,
				Durable:           test.durable,
				Name:              test.name,
			})
			require_NoError(t, err)

			name := test.durable
			if test.durable == _EMPTY_ {
				name = test.name
			}

			const msgs = 5_000
			sub, err := js.Subscribe(_EMPTY_, func(msg *nats.Msg) {
				require_NoError(t, msg.Ack())
			}, nats.Bind(test.stream, name), nats.ManualAck())
			require_NoError(t, err)

			// This happens only if we start publishing messages after consumer was created.
			pubErr := make(chan error, 1)
			pubDone := make(chan struct{})
			go func(subject string) {
				defer close(pubDone)
				for i := 0; i < msgs; i++ {
					_, err := js.Publish(subject, []byte("DATA"))
					if err != nil {
						select {
						case pubErr <- err:
						default:
						}
						return
					}
				}
			}(test.subject)

			// Wait for inactive threshold to expire and all messages to be published and received
			// Bug is we clean up active consumers when we should not.
			time.Sleep(3 * inactiveThreshold / 2)

			select {
			case <-pubDone:
			case <-time.After(10 * time.Second):
				t.Fatalf("Did not receive completion signal")
			}

			select {
			case err := <-pubErr:
				t.Fatalf("Publish error: %v", err)
			default:
			}

			checkFor(t, time.Second, 100*time.Millisecond, func() error {
				info, err := js.ConsumerInfo(test.stream, name)
				if err != nil {
					return fmt.Errorf("Expected to be able to retrieve consumer: %v", err)
				}
				if info.Delivered.Stream != msgs {
					return fmt.Errorf("require uint64 equal, but got: %d != %d", info.Delivered.Stream, msgs)
				}
				return nil
			})

			// Stop the subscription and remove the interest.
			err = sub.Unsubscribe()
			require_NoError(t, err)

			// Now wait for interest inactivity threshold to kick in.
			time.Sleep(3 * inactiveThreshold / 2)

			// Check if the consumer has been removed.
			_, err = js.ConsumerInfo(test.stream, name)
			require_Error(t, err, nats.ErrConsumerNotFound)
		})
	}
}

// TestJetStreamClusterWALBuildupOnNoOpPull tests whether or not the consumer
// RAFT log is being compacted when the stream is idle but we are performing
// lots of fetches. Otherwise the disk usage just spirals out of control if
// there are no other state changes to trigger a compaction.
func TestJetStreamClusterWALBuildupOnNoOpPull(t *testing.T) {
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

	sub, err := js.PullSubscribe(
		"foo",
		"durable",
		nats.ConsumerReplicas(3),
	)
	require_NoError(t, err)

	for i := 0; i < 10000; i++ {
		_, _ = sub.Fetch(1, nats.MaxWait(time.Microsecond))
	}

	// Needs to be at least 10 seconds, otherwise we won't hit the
	// minSnapDelta that prevents us from snapshotting too often
	time.Sleep(time.Second * 11)

	for i := 0; i < 1024; i++ {
		_, _ = sub.Fetch(1, nats.MaxWait(time.Microsecond))
	}

	time.Sleep(time.Second)

	server := c.randomNonConsumerLeader(globalAccountName, "TEST", "durable")

	stream, err := server.globalAccount().lookupStream("TEST")
	require_NoError(t, err)

	consumer := stream.lookupConsumer("durable")
	require_NotNil(t, consumer)

	entries, bytes := consumer.raftNode().Size()
	t.Log("new entries:", entries)
	t.Log("new bytes:", bytes)

	if max := uint64(1024); entries > max {
		t.Fatalf("got %d entries, expected less than %d entries", entries, max)
	}
}

// Found in https://github.com/nats-io/nats-server/issues/3848
// When Max Age was specified and stream was scaled up, new replicas
// were expiring messages much later than the leader.
func TestJetStreamClusterStreamMaxAgeScaleUp(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "R3S", 3)
	defer c.shutdown()

	nc, js := jsClientConnect(t, c.randomServer())
	defer nc.Close()

	for _, test := range []struct {
		name    string
		storage nats.StorageType
		stream  string
		purge   bool
	}{
		{name: "file", storage: nats.FileStorage, stream: "A", purge: false},
		{name: "memory", storage: nats.MemoryStorage, stream: "B", purge: false},
		{name: "file with purge", storage: nats.FileStorage, stream: "C", purge: true},
		{name: "memory with purge", storage: nats.MemoryStorage, stream: "D", purge: true},
	} {

		t.Run(test.name, func(t *testing.T) {
			ttl := time.Second * 5
			// Add stream with one replica and short MaxAge.
			_, err := js.AddStream(&nats.StreamConfig{
				Name:     test.stream,
				Replicas: 1,
				Subjects: []string{test.stream},
				MaxAge:   ttl,
				Storage:  test.storage,
			})
			require_NoError(t, err)

			// Add some messages.
			for i := 0; i < 10; i++ {
				sendStreamMsg(t, nc, test.stream, "HELLO")
			}
			// We need to also test if we properly set expiry
			// if first sequence is not 1.
			if test.purge {
				err = js.PurgeStream(test.stream)
				require_NoError(t, err)
				// Add some messages.
				for i := 0; i < 10; i++ {
					sendStreamMsg(t, nc, test.stream, "HELLO")
				}
			}
			// Mark the time when all messages were published.
			start := time.Now()

			// Sleep for half of the MaxAge time.
			time.Sleep(ttl / 2)

			// Scale up the Stream to 3 replicas.
			_, err = js.UpdateStream(&nats.StreamConfig{
				Name:     test.stream,
				Replicas: 3,
				Subjects: []string{test.stream},
				MaxAge:   ttl,
				Storage:  test.storage,
			})
			require_NoError(t, err)
			c.waitOnStreamLeader(globalAccountName, test.stream)

			// All messages should still be there.
			info, err := js.StreamInfo(test.stream)
			require_NoError(t, err)
			require_Equal(t, info.State.Msgs, 10)

			// Wait until MaxAge is reached.
			time.Sleep(ttl - time.Since(start) + (1 * time.Second))

			// Check if all messages are expired.
			info, err = js.StreamInfo(test.stream)
			require_NoError(t, err)
			require_Equal(t, info.State.Msgs, 0)

			// Now switch leader to one of replicas
			_, err = nc.Request(fmt.Sprintf(JSApiStreamLeaderStepDownT, test.stream), nil, time.Second)
			require_NoError(t, err)
			c.waitOnStreamLeader(globalAccountName, test.stream)

			// and make sure that it also expired all messages
			info, err = js.StreamInfo(test.stream)
			require_NoError(t, err)
			require_Equal(t, info.State.Msgs, 0)
		})
	}
}

func TestJetStreamClusterWorkQueueConsumerReplicatedAfterScaleUp(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "R3S", 3)
	defer c.shutdown()

	nc, js := jsClientConnect(t, c.randomServer())
	defer nc.Close()

	_, err := js.AddStream(&nats.StreamConfig{
		Name:      "TEST",
		Replicas:  1,
		Subjects:  []string{"WQ"},
		Retention: nats.WorkQueuePolicy,
	})
	require_NoError(t, err)

	// Create an ephemeral consumer.
	sub, err := js.SubscribeSync("WQ")
	require_NoError(t, err)

	// Scale up to R3.
	_, err = js.UpdateStream(&nats.StreamConfig{
		Name:      "TEST",
		Replicas:  3,
		Subjects:  []string{"WQ"},
		Retention: nats.WorkQueuePolicy,
	})
	require_NoError(t, err)
	c.waitOnStreamLeader(globalAccountName, "TEST")

	ci, err := sub.ConsumerInfo()
	require_NoError(t, err)

	require_True(t, ci.Config.Replicas == 0 || ci.Config.Replicas == 3)

	c.waitOnConsumerLeader(globalAccountName, "TEST", ci.Name)
	s := c.consumerLeader(globalAccountName, "TEST", ci.Name)
	require_NotNil(t, s)

	mset, err := s.GlobalAccount().lookupStream("TEST")
	require_NoError(t, err)

	o := mset.lookupConsumer(ci.Name)
	require_NotNil(t, o)
	require_NotNil(t, o.raftNode())
}

// https://github.com/nats-io/nats-server/issues/3953
func TestJetStreamClusterWorkQueueAfterScaleUp(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "R3S", 3)
	defer c.shutdown()

	nc, js := jsClientConnect(t, c.randomServer())
	defer nc.Close()

	_, err := js.AddStream(&nats.StreamConfig{
		Name:      "TEST",
		Replicas:  1,
		Subjects:  []string{"WQ"},
		Retention: nats.WorkQueuePolicy,
	})
	require_NoError(t, err)

	_, err = js.AddConsumer("TEST", &nats.ConsumerConfig{
		Durable:        "d1",
		DeliverSubject: "d1",
		AckPolicy:      nats.AckExplicitPolicy,
	})
	require_NoError(t, err)

	wch := make(chan bool, 1)
	_, err = nc.Subscribe("d1", func(msg *nats.Msg) {
		msg.AckSync()
		wch <- true
	})
	require_NoError(t, err)

	_, err = js.UpdateStream(&nats.StreamConfig{
		Name:      "TEST",
		Replicas:  3,
		Subjects:  []string{"WQ"},
		Retention: nats.WorkQueuePolicy,
	})
	require_NoError(t, err)
	c.waitOnStreamLeader(globalAccountName, "TEST")

	sendStreamMsg(t, nc, "WQ", "SOME WORK")

	select {
	case <-wch:
	case <-time.After(5 * time.Second):
		t.Fatalf("Did not receive ack signal")
	}

	checkFor(t, time.Second, 200*time.Millisecond, func() error {
		si, err := js.StreamInfo("TEST")
		require_NoError(t, err)
		if si.State.Msgs == 0 {
			return nil
		}
		return fmt.Errorf("Still have %d msgs left", si.State.Msgs)
	})
}

func TestJetStreamClusterInterestBasedStreamAndConsumerSnapshots(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "R3S", 3)
	defer c.shutdown()

	nc, js := jsClientConnect(t, c.randomServer())
	defer nc.Close()

	_, err := js.AddStream(&nats.StreamConfig{
		Name:      "TEST",
		Replicas:  3,
		Subjects:  []string{"foo"},
		Retention: nats.InterestPolicy,
	})
	require_NoError(t, err)

	sub, err := js.SubscribeSync("foo", nats.Durable("d22"))
	require_NoError(t, err)

	num := 200
	for i := 0; i < num; i++ {
		js.PublishAsync("foo", []byte("ok"))
	}
	select {
	case <-js.PublishAsyncComplete():
	case <-time.After(5 * time.Second):
		t.Fatalf("Did not receive completion signal")
	}

	checkSubsPending(t, sub, num)

	// Shutdown one server.
	s := c.randomServer()
	s.Shutdown()

	c.waitOnStreamLeader(globalAccountName, "TEST")

	nc, js = jsClientConnect(t, c.randomServer())
	defer nc.Close()

	// Now ack all messages while the other server is down.
	for i := 0; i < num; i++ {
		m, err := sub.NextMsg(time.Second)
		require_NoError(t, err)
		m.AckSync()
	}

	// Wait for all message acks to be processed and all messages to be removed.
	checkFor(t, time.Second, 200*time.Millisecond, func() error {
		si, err := js.StreamInfo("TEST")
		require_NoError(t, err)
		if si.State.Msgs == 0 {
			return nil
		}
		return fmt.Errorf("Still have %d msgs left", si.State.Msgs)
	})

	// Force a snapshot on the consumer leader before restarting the downed server.
	cl := c.consumerLeader(globalAccountName, "TEST", "d22")
	require_NotNil(t, cl)

	mset, err := cl.GlobalAccount().lookupStream("TEST")
	require_NoError(t, err)

	o := mset.lookupConsumer("d22")
	require_NotNil(t, o)

	snap, err := o.store.EncodedState()
	require_NoError(t, err)

	n := o.raftNode()
	require_NotNil(t, n)
	require_NoError(t, n.InstallSnapshot(snap, false))

	// Now restart the downed server.
	s = c.restartServer(s)

	// Make the restarted server the eventual leader.
	checkFor(t, 20*time.Second, 200*time.Millisecond, func() error {
		c.waitOnStreamLeader(globalAccountName, "TEST")
		if sl := c.streamLeader(globalAccountName, "TEST"); sl != s {
			sl.JetStreamStepdownStream(globalAccountName, "TEST")
			return fmt.Errorf("Server %s is not leader yet", s)
		}
		return nil
	})

	si, err := js.StreamInfo("TEST")
	require_NoError(t, err)
	require_True(t, si.State.Msgs == 0)
}

func TestJetStreamClusterConsumerFollowerStoreStateAckFloorBug(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "R3S", 3)
	defer c.shutdown()

	nc, js := jsClientConnect(t, c.randomServer())
	defer nc.Close()

	_, err := js.AddStream(&nats.StreamConfig{
		Name:     "TEST",
		Replicas: 3,
		Subjects: []string{"foo"},
	})
	require_NoError(t, err)

	sub, err := js.PullSubscribe(_EMPTY_, "C", nats.BindStream("TEST"), nats.ManualAck())
	require_NoError(t, err)

	num := 100
	for i := 0; i < num; i++ {
		sendStreamMsg(t, nc, "foo", "data")
	}

	// This one prevents the state for pending from reaching 0 and resetting, which would not show the bug.
	sendStreamMsg(t, nc, "foo", "data")

	// Ack all but one and out of order and make sure all consumers have the same stored state.
	msgs, err := sub.Fetch(num, nats.MaxWait(time.Second))
	require_NoError(t, err)
	require_True(t, len(msgs) == num)

	_, err = sub.Fetch(1, nats.MaxWait(time.Second))
	require_NoError(t, err)

	rand.Shuffle(len(msgs), func(i, j int) { msgs[i], msgs[j] = msgs[j], msgs[i] })
	for _, m := range msgs {
		if err := m.AckSync(); err != nil {
			t.Fatalf("Ack failed :%+v", err)
		}
	}

	checkConsumerState := func(delivered, ackFloor nats.SequenceInfo, numAckPending int) error {
		expectedDelivered := uint64(num) + 1
		if delivered.Stream != expectedDelivered || delivered.Consumer != expectedDelivered {
			return fmt.Errorf("Wrong delivered, expected %d got %+v", expectedDelivered, delivered)
		}
		expectedAck := uint64(num)
		if ackFloor.Stream != expectedAck || ackFloor.Consumer != expectedAck {
			return fmt.Errorf("Wrong ackFloor, expected %d got %+v", expectedAck, ackFloor)
		}
		if numAckPending != 1 {
			return errors.New("Expected num ack pending to be 1")
		}
		return nil
	}

	ci, err := js.ConsumerInfo("TEST", "C")
	require_NoError(t, err)
	require_NoError(t, checkConsumerState(ci.Delivered, ci.AckFloor, ci.NumAckPending))

	// Check each consumer on each server for it's store state and make sure it matches as well.
	checkFor(t, 20*time.Second, 200*time.Millisecond, func() error {
		for _, s := range c.servers {
			mset, err := s.GlobalAccount().lookupStream("TEST")
			if err != nil {
				return err
			}
			if mset == nil {
				return errors.New("Mset should not be nil")
			}
			o := mset.lookupConsumer("C")
			if o == nil {
				return errors.New("Consumer should not be nil")
			}

			state, err := o.store.State()
			if err != nil {
				return err
			}
			delivered := nats.SequenceInfo{Stream: state.Delivered.Stream, Consumer: state.Delivered.Consumer}
			ackFloor := nats.SequenceInfo{Stream: state.AckFloor.Stream, Consumer: state.AckFloor.Consumer}
			if err := checkConsumerState(delivered, ackFloor, len(state.Pending)); err != nil {
				return err
			}
		}
		return nil
	})

	// Now stepdown the consumer and move its leader and check the state after transition.
	// Make the restarted server the eventual leader.
	seen := make(map[*Server]bool)
	cl := c.consumerLeader(globalAccountName, "TEST", "C")
	require_NotNil(t, cl)
	seen[cl] = true

	allSeen := func() bool {
		for _, s := range c.servers {
			if !seen[s] {
				return false
			}
		}
		return true
	}

	checkAllLeaders := func() {
		t.Helper()
		checkFor(t, 20*time.Second, 200*time.Millisecond, func() error {
			c.waitOnConsumerLeader(globalAccountName, "TEST", "C")
			if allSeen() {
				return nil
			}
			cl := c.consumerLeader(globalAccountName, "TEST", "C")
			seen[cl] = true
			ci, err := js.ConsumerInfo("TEST", "C")
			if err != nil {
				return err
			}
			if err := checkConsumerState(ci.Delivered, ci.AckFloor, ci.NumAckPending); err != nil {
				return err
			}
			cl.JetStreamStepdownConsumer(globalAccountName, "TEST", "C")
			return fmt.Errorf("Not all servers have been consumer leader yet")
		})
	}

	checkAllLeaders()

	// No restart all servers and check again.
	c.stopAll()
	c.restartAll()
	c.waitOnLeader()

	nc, js = jsClientConnect(t, c.randomServer())
	defer nc.Close()

	seen = make(map[*Server]bool)
	checkAllLeaders()
}

func TestJetStreamClusterInterestLeakOnDisableJetStream(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "R3S", 3)
	defer c.shutdown()

	nc, js := jsClientConnect(t, c.leader())
	defer nc.Close()

	for i := 1; i <= 5; i++ {
		_, err := js.AddStream(&nats.StreamConfig{
			Name:     fmt.Sprintf("test_%d", i),
			Subjects: []string{fmt.Sprintf("test_%d", i)},
			Replicas: 3,
		})
		require_NoError(t, err)
	}

	c.waitOnAllCurrent()

	server := c.randomNonLeader()
	account := server.SystemAccount()

	server.DisableJetStream()

	checkFor(t, 2*time.Second, 100*time.Millisecond, func() error {
		var sublist []*subscription
		account.sl.localSubs(&sublist, false)

		var danglingJSC, danglingRaft int
		for _, sub := range sublist {
			if strings.HasPrefix(string(sub.subject), "$JSC.") {
				danglingJSC++
			} else if strings.HasPrefix(string(sub.subject), "$NRG.") {
				danglingRaft++
			}
		}
		if danglingJSC > 0 || danglingRaft > 0 {
			return fmt.Errorf("unexpected dangling interests for JetStream assets after shutdown (%d $JSC, %d $NRG)", danglingJSC, danglingRaft)
		}
		return nil
	})
}

func TestJetStreamClusterDisableVsShutdownJetStreamMetaState(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "R3S", 3)
	defer c.shutdown()

	nc, js := jsClientConnect(t, c.leader())
	defer nc.Close()

	_, err := js.AddStream(&nats.StreamConfig{
		Name:     "TEST",
		Subjects: []string{"foo"},
		Replicas: 3,
	})
	require_NoError(t, err)
	c.waitOnAllCurrent()

	var followers []*Server
	for _, s := range c.servers {
		if s.Running() && !s.JetStreamIsLeader() {
			followers = append(followers, s)
		}
	}
	require_True(t, len(followers) >= 2)
	sShutdown, sDisable := followers[0], followers[1]

	// ShutdownJetStream preserves meta-raft state on disk.
	shutdownDir := filepath.Join(sShutdown.JetStreamConfig().StoreDir, DEFAULT_SYSTEM_ACCOUNT, defaultStoreDirName, defaultMetaGroupName)
	tavFile := filepath.Join(shutdownDir, termVoteFile)
	peersFile := filepath.Join(shutdownDir, peerStateFile)
	for _, f := range []string{shutdownDir, tavFile, peersFile} {
		if _, err := os.Stat(f); err != nil {
			t.Fatalf("expected %s to exist before Shutdown: %v", f, err)
		}
	}
	require_NoError(t, sShutdown.ShutdownJetStream())
	for _, f := range []string{shutdownDir, tavFile, peersFile} {
		if _, err := os.Stat(f); err != nil {
			t.Fatalf("expected %s to be preserved after Shutdown: %v", f, err)
		}
	}

	// DisableJetStream wipes meta-raft state on disk.
	disableDir := filepath.Join(sDisable.JetStreamConfig().StoreDir, DEFAULT_SYSTEM_ACCOUNT, defaultStoreDirName, defaultMetaGroupName)
	if _, err := os.Stat(disableDir); err != nil {
		t.Fatalf("expected %s to exist before Disable: %v", disableDir, err)
	}
	require_NoError(t, sDisable.DisableJetStream())
	if _, err := os.Stat(disableDir); !os.IsNotExist(err) {
		t.Fatalf("expected %s to be removed after Disable, got err=%v", disableDir, err)
	}
}

// https://github.com/nats-io/nats-server/issues/8150
func TestJetStreamClusterHandleWritePermissionErrorPreservesMetaState(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "R3S", 3)
	defer c.shutdown()

	nc, js := jsClientConnect(t, c.leader())
	defer nc.Close()

	_, err := js.AddStream(&nats.StreamConfig{
		Name:     "TEST",
		Subjects: []string{"foo"},
		Replicas: 3,
	})
	require_NoError(t, err)
	c.waitOnAllCurrent()

	s := c.randomNonLeader()
	tavFile := filepath.Join(s.JetStreamConfig().StoreDir, DEFAULT_SYSTEM_ACCOUNT, defaultStoreDirName, defaultMetaGroupName, termVoteFile)
	if _, err := os.Stat(tavFile); err != nil {
		t.Fatalf("expected %s to exist before the simulated permission error: %v", tavFile, err)
	}

	s.handleWritePermissionError()

	checkFor(t, 2*time.Second, 200*time.Millisecond, func() error {
		if s.JetStreamEnabled() {
			return fmt.Errorf("JetStream still enabled")
		}
		return nil
	})

	if _, err := os.Stat(tavFile); err != nil {
		t.Fatalf("meta state was wiped after a transient permission error: %v", err)
	}
}

func TestJetStreamClusterNoLeadersDuringLameDuck(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "R3S", 3)
	defer c.shutdown()

	// Grab the first server and set lameduck option directly.
	s := c.servers[0]
	s.optsMu.Lock()
	s.opts.LameDuckDuration = 5 * time.Second
	s.opts.LameDuckGracePeriod = -5 * time.Second
	s.optsMu.Unlock()

	// Connect to the third server.
	nc, js := jsClientConnect(t, c.servers[2])
	defer nc.Close()

	allServersHaveLeaders := func() bool {
		haveLeader := make(map[*Server]bool)
		for _, s := range c.servers {
			s.rnMu.RLock()
			for _, n := range s.raftNodes {
				if n.Leader() {
					haveLeader[s] = true
					break
				}
			}
			s.rnMu.RUnlock()
		}
		return len(haveLeader) == len(c.servers)
	}

	// Create streams until we have a leader on all the servers.
	var index int
	checkFor(t, 10*time.Second, time.Millisecond, func() error {
		if allServersHaveLeaders() {
			return nil
		}
		index++
		_, err := js.AddStream(&nats.StreamConfig{
			Name:     fmt.Sprintf("TEST_%d", index),
			Subjects: []string{fmt.Sprintf("foo.%d", index)},
			Replicas: 3,
		})
		require_NoError(t, err)
		return fmt.Errorf("All servers do not have at least one leader")
	})

	// Put our server into lameduck mode.
	// Need a client.
	dummy, _ := jsClientConnect(t, s)
	defer dummy.Close()
	go s.lameDuckMode()

	// Wait for all leaders to move off.
	checkFor(t, 2*time.Second, 50*time.Millisecond, func() error {
		s.rnMu.RLock()
		defer s.rnMu.RUnlock()
		for _, n := range s.raftNodes {
			if n.Leader() {
				return fmt.Errorf("Server still has a leader")
			}
		}
		return nil
	})

	// All leader evacuated.

	// Create a go routine that will create streams constantly.
	qch := make(chan bool)
	go func() {
		var index int
		for {
			select {
			case <-time.After(time.Millisecond):
				index++
				_, err := js.AddStream(&nats.StreamConfig{
					Name:     fmt.Sprintf("NEW_TEST_%d", index),
					Subjects: []string{fmt.Sprintf("bar.%d", index)},
					Replicas: 3,
				})
				if err != nil {
					return
				}
			case <-qch:
				return
			}
		}
	}()
	defer close(qch)

	// Make sure we do not have any leaders placed on the lameduck server.
	for s.isRunning() {
		var hasLeader bool
		s.rnMu.RLock()
		for _, n := range s.raftNodes {
			hasLeader = hasLeader || n.Leader()
		}
		s.rnMu.RUnlock()
		if hasLeader {
			t.Fatalf("Server had a leader when it should not due to lameduck mode")
		}
	}
}

func TestJetStreamClusterNoR1AssetsDuringLameDuck(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "R3S", 3)
	defer c.shutdown()

	// Grab the first server and set lameduck option directly.
	s := c.servers[0]
	s.optsMu.Lock()
	s.opts.LameDuckDuration = 5 * time.Second
	s.opts.LameDuckGracePeriod = -5 * time.Second
	s.optsMu.Unlock()

	// Connect to the server to keep it alive when we go into LDM.
	dummy, _ := jsClientConnect(t, s)
	defer dummy.Close()

	// Connect to the third server.
	nc, js := jsClientConnect(t, c.servers[2])
	defer nc.Close()

	// Connect to the third server as the system account.
	snc, _ := jsClientConnect(t, c.servers[2], nats.UserInfo("admin", "s3cr3t!"))
	defer snc.Close()

	// Now put the first server into lame duck mode.
	go s.lameDuckMode()

	checkOffline := func() error {
		id := s.info.ID
		s := c.servers[2]
		s.mu.RLock()
		defer s.mu.RUnlock()

		var isOffline bool
		s.nodeToInfo.Range(func(_, v any) bool {
			ni := v.(nodeInfo)
			if ni.id == id {
				isOffline = ni.offline
				return false
			}
			return true
		})

		if !isOffline {
			return fmt.Errorf("first node is still online unexpectedly")
		}
		return nil
	}

	// Wait for news to arrive that the first server has gone into
	// lame duck mode and been marked offline.
	checkFor(t, 2*time.Second, 50*time.Millisecond, checkOffline)

	// Request a statsz broadcast, it must not mark the first server online again.
	require_NoError(t, snc.Publish(fmt.Sprintf("$SYS.REQ.SERVER.%s.STATSZ", s.ID()), nil))
	require_NoError(t, snc.Flush())
	for range 10 {
		time.Sleep(50 * time.Millisecond)
		require_NoError(t, checkOffline())
	}

	// Create a go routine that will create streams constantly.
	qch := make(chan bool)
	go func() {
		var index int
		for {
			select {
			case <-time.After(time.Millisecond * 25):
				index++
				_, err := js.AddStream(&nats.StreamConfig{
					Name:     fmt.Sprintf("NEW_TEST_%d", index),
					Subjects: []string{fmt.Sprintf("bar.%d", index)},
					Replicas: 1,
				})
				if err != nil {
					return
				}
			case <-qch:
				return
			}
		}
	}()
	defer close(qch)

	gacc := s.GlobalAccount()
	if gacc == nil {
		t.Fatalf("No global account")
	}
	// Make sure we do not have any R1 assets placed on the lameduck server.
	for s.isRunning() {
		if len(gacc.streams()) > 0 {
			t.Fatalf("Server had an R1 asset when it should not due to lameduck mode")
		}
		time.Sleep(15 * time.Millisecond)
	}
	s.WaitForShutdown()
}

func TestJetStreamClusterRepliesDuringLameDuck(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "R3S", 3)
	defer c.shutdown()

	// Grab the meta leader and set lameduck option directly.
	// It has a leader to transfer, so JetStream stays up for a second in LDM.
	s := c.leader()
	s.optsMu.Lock()
	s.opts.LameDuckDuration = 5 * time.Second
	s.opts.LameDuckGracePeriod = -5 * time.Second
	s.optsMu.Unlock()

	// Connect to the server to keep it alive when we go into LDM.
	dummy, _ := jsClientConnect(t, s)
	defer dummy.Close()

	// Connect to another server, as a user and as the system account.
	rs := c.randomNonLeader()
	nc, js := jsClientConnect(t, rs)
	defer nc.Close()
	snc, _ := jsClientConnect(t, rs, nats.UserInfo("admin", "s3cr3t!"))
	defer snc.Close()

	// Create R1 streams until one is placed on the lameduck server.
	var stream string
	var index int
	checkFor(t, 5*time.Second, time.Millisecond, func() error {
		index++
		stream = fmt.Sprintf("TEST_%d", index)
		_, err := js.AddStream(&nats.StreamConfig{Name: stream, Replicas: 1})
		require_NoError(t, err)
		if c.streamLeader(globalAccountName, stream) != s {
			return fmt.Errorf("Stream not placed on the lameduck server")
		}
		return nil
	})

	// Now put the server into lame duck mode.
	go s.lameDuckMode()

	// Wait for news to arrive that the server has gone into
	// lame duck mode and been marked offline.
	checkFor(t, 2*time.Second, 50*time.Millisecond, func() error {
		id := s.info.ID
		rs.mu.RLock()
		defer rs.mu.RUnlock()
		var isOffline bool
		rs.nodeToInfo.Range(func(_, v any) bool {
			ni := v.(nodeInfo)
			if ni.id == id {
				isOffline = ni.offline
				return false
			}
			return true
		})
		if !isOffline {
			return fmt.Errorf("Lameduck server is still online unexpectedly")
		}
		return nil
	})

	// The lameduck server must still reply to requests it serves.
	_, err := js.StreamInfo(stream, nats.MaxWait(250*time.Millisecond))
	if err != nil {
		t.Fatalf("No stream info reply from the lameduck server: %v", err)
	}
	_, err = snc.Request(fmt.Sprintf("$SYS.REQ.SERVER.%s.VARZ", s.ID()), nil, 250*time.Millisecond)
	if err != nil {
		t.Fatalf("No varz reply from the lameduck server: %v", err)
	}
}

// If a consumer has not been registered (possible in heavily loaded systems with lots of assets)
// it could miss the signal of a message going away. If that message was pending and expires the
// ack floor could fall below the stream first sequence. This test will force that condition and
// make sure the system resolves itself.
func TestJetStreamClusterConsumerAckFloorDrift(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "R3S", 3)
	defer c.shutdown()

	nc, js := jsClientConnect(t, c.randomServer())
	defer nc.Close()

	_, err := js.AddStream(&nats.StreamConfig{
		Name:     "TEST",
		Subjects: []string{"*"},
		Replicas: 3,
		MaxAge:   time.Second,
		MaxMsgs:  10,
	})
	require_NoError(t, err)

	sub, err := js.PullSubscribe("foo", "C")
	require_NoError(t, err)

	// Publish as many messages as the ack floor check threshold +5 (what we set ackfloor to later).
	totalMessages := 55
	for i := 0; i < totalMessages; i++ {
		sendStreamMsg(t, nc, "foo", "HELLO")
	}

	// No-op but will surface as delivered.
	_, err = sub.Fetch(10)
	require_NoError(t, err)

	// We will initialize the state with delivered being 10 and ackfloor being 0 directly.
	// Fetch will asynchronously propagate this state, so can't reliably request this from the leader immediately.
	state := &ConsumerState{Delivered: SequencePair{Consumer: 10, Stream: 10}}

	// Now let messages expire.
	checkFor(t, 5*time.Second, time.Second, func() error {
		si, err := js.StreamInfo("TEST")
		require_NoError(t, err)
		if si.State.Msgs == 0 {
			return nil
		}
		return fmt.Errorf("stream still has msgs")
	})

	// Set state to ackfloor of 5 and no pending.
	state.AckFloor.Consumer = 5
	state.AckFloor.Stream = 5
	state.Pending = nil

	// Now put back the state underneath of the consumers.
	for _, s := range c.servers {
		mset, err := s.GlobalAccount().lookupStream("TEST")
		require_NoError(t, err)
		o := mset.lookupConsumer("C")
		require_NotNil(t, o)
		o.mu.Lock()
		o.applyState(state)
		cfs := o.store.(*consumerFileStore)
		o.mu.Unlock()
		// The lower layer will ignore, so set more directly.
		cfs.mu.Lock()
		cfs.state = *state
		cfs.mu.Unlock()
		// Also snapshot to remove any raft entries that could affect it.
		snap, err := o.store.EncodedState()
		require_NoError(t, err)
		require_NoError(t, o.raftNode().InstallSnapshot(snap, false))
	}

	cl := c.consumerLeader(globalAccountName, "TEST", "C")
	require_NotNil(t, cl)
	err = cl.JetStreamStepdownConsumer(globalAccountName, "TEST", "C")
	require_NoError(t, err)
	c.waitOnConsumerLeader(globalAccountName, "TEST", "C")

	checkFor(t, 5*time.Second, 100*time.Millisecond, func() error {
		ci, err := js.ConsumerInfo("TEST", "C")
		if err != nil {
			return err
		}
		// Replicated state should stay the same.
		if ci.AckFloor.Stream != 5 && ci.AckFloor.Consumer != 5 {
			return fmt.Errorf("replicated AckFloor not correct, expected %d, got %+v", 5, ci.AckFloor)
		}

		cl = c.consumerLeader(globalAccountName, "TEST", "C")
		mset, err := cl.GlobalAccount().lookupStream("TEST")
		require_NoError(t, err)
		o := mset.lookupConsumer("C")
		require_NotNil(t, o)
		o.mu.RLock()
		defer o.mu.RUnlock()

		// Make sure we catch this and adjust.
		if o.asflr != uint64(totalMessages) && o.adflr != 10 {
			return fmt.Errorf("leader AckFloor not correct, expected %d, got %+v", 10, ci.AckFloor)
		}
		return nil
	})
}

func TestJetStreamClusterInterestStreamFilteredConsumersWithNoInterest(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "R5S", 5)
	defer c.shutdown()

	nc, js := jsClientConnect(t, c.randomServer())
	defer nc.Close()

	_, err := js.AddStream(&nats.StreamConfig{
		Name:      "TEST",
		Subjects:  []string{"*"},
		Retention: nats.InterestPolicy,
		Replicas:  3,
	})
	require_NoError(t, err)

	// Create three subscribers.
	ackCb := func(m *nats.Msg) { m.Ack() }

	_, err = js.Subscribe("foo", ackCb, nats.BindStream("TEST"), nats.ManualAck())
	require_NoError(t, err)

	_, err = js.Subscribe("bar", ackCb, nats.BindStream("TEST"), nats.ManualAck())
	require_NoError(t, err)

	_, err = js.Subscribe("baz", ackCb, nats.BindStream("TEST"), nats.ManualAck())
	require_NoError(t, err)

	// Now send 100 messages, randomly picking foo or bar, but never baz.
	for i := 0; i < 100; i++ {
		if rand.IntN(2) > 0 {
			sendStreamMsg(t, nc, "foo", "HELLO")
		} else {
			sendStreamMsg(t, nc, "bar", "WORLD")
		}
	}

	// Messages are expected to go to 0.
	checkFor(t, time.Second, 100*time.Millisecond, func() error {
		si, err := js.StreamInfo("TEST")
		require_NoError(t, err)
		if si.State.Msgs == 0 {
			return nil
		}
		return fmt.Errorf("stream still has msgs")
	})
}

func TestJetStreamClusterChangeClusterAfterStreamCreate(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "NATS", 3)
	defer c.shutdown()

	nc, js := jsClientConnect(t, c.randomServer())
	defer nc.Close()

	_, err := js.AddStream(&nats.StreamConfig{
		Name:     "TEST",
		Subjects: []string{"*"},
		Replicas: 3,
	})
	require_NoError(t, err)

	for i := 0; i < 1000; i++ {
		sendStreamMsg(t, nc, "foo", "HELLO")
	}

	_, err = js.UpdateStream(&nats.StreamConfig{
		Name:     "TEST",
		Subjects: []string{"*"},
		Replicas: 1,
	})
	require_NoError(t, err)

	c.stopAll()

	c.name = "FOO"
	for _, o := range c.opts {
		buf, err := os.ReadFile(o.ConfigFile)
		require_NoError(t, err)
		nbuf := bytes.Replace(buf, []byte("name: NATS"), []byte("name: FOO"), 1)
		err = os.WriteFile(o.ConfigFile, nbuf, 0640)
		require_NoError(t, err)
	}

	c.restartAll()
	c.waitOnLeader()
	c.waitOnStreamLeader(globalAccountName, "TEST")

	nc, js = jsClientConnect(t, c.randomServer())
	defer nc.Close()

	_, err = js.UpdateStream(&nats.StreamConfig{
		Name:     "TEST",
		Subjects: []string{"*"},
		Replicas: 3,
	})
	// This should fail with no suitable peers, since the asset was created under the NATS cluster which has no peers.
	require_Error(t, err, errors.New("nats: no suitable peers for placement"))

	// Make sure we can swap the cluster.
	_, err = js.UpdateStream(&nats.StreamConfig{
		Name:      "TEST",
		Subjects:  []string{"*"},
		Placement: &nats.Placement{Cluster: "FOO"},
	})
	require_NoError(t, err)
}

// Delivered/AckFloor are replicated, so Jsz for followers must report the same
// values as the leader once the followers have applied the updates.
func TestJetStreamClusterConsumerInfoForJszForFollowers(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "NATS", 3)
	defer c.shutdown()

	nc, js := jsClientConnect(t, c.randomServer())
	defer nc.Close()

	_, err := js.AddStream(&nats.StreamConfig{
		Name:     "TEST",
		Subjects: []string{"*"},
		Replicas: 3,
	})
	require_NoError(t, err)

	for range 1000 {
		sendStreamMsg(t, nc, "foo", "HELLO")
	}

	sub, err := js.PullSubscribe("foo", "d")
	require_NoError(t, err)

	fetch, ack := 122, 22
	msgs, err := sub.Fetch(fetch, nats.MaxWait(10*time.Second))
	require_NoError(t, err)
	require_True(t, len(msgs) == fetch)
	for _, m := range msgs[:ack] {
		require_NoError(t, m.AckSync())
	}
	// Let deliveries and acks propagate to the followers.
	checkFor(t, 2*time.Second, 100*time.Millisecond, func() error {
		for _, s := range c.servers {
			jsz, err := s.Jsz(&JSzOptions{Accounts: true, Consumer: true})
			if err != nil {
				return err
			}
			if len(jsz.AccountDetails) != 1 || len(jsz.AccountDetails[0].Streams) != 1 ||
				len(jsz.AccountDetails[0].Streams[0].Consumer) != 1 {
				return fmt.Errorf("consumer not found for %v", s)
			}
			consumer := jsz.AccountDetails[0].Streams[0].Consumer[0]
			if consumer.Delivered.Consumer != uint64(fetch) || consumer.Delivered.Stream != uint64(fetch) {
				return fmt.Errorf("incorrect delivered for %v: %+v", s, consumer.Delivered)
			}
			if consumer.AckFloor.Consumer != uint64(ack) || consumer.AckFloor.Stream != uint64(ack) {
				return fmt.Errorf("incorrect ackfloor for %v: %+v", s, consumer.AckFloor)
			}
		}
		return nil
	})
}

// Make sure that stopping a stream shutdowns down it's raft node.
func TestJetStreamClusterStreamNodeShutdownBugOnStop(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "NATS", 3)
	defer c.shutdown()

	nc, js := jsClientConnect(t, c.randomServer())
	defer nc.Close()

	_, err := js.AddStream(&nats.StreamConfig{
		Name:     "TEST",
		Subjects: []string{"*"},
		Replicas: 3,
	})
	require_NoError(t, err)

	for i := 0; i < 100; i++ {
		sendStreamMsg(t, nc, "foo", "HELLO")
	}

	s := c.randomServer()
	numNodesStart := s.numRaftNodes()
	mset, err := s.GlobalAccount().lookupStream("TEST")
	require_NoError(t, err)
	node := mset.raftNode()
	require_NotNil(t, node)
	node.InstallSnapshot(mset.stateSnapshot(), false)
	// Stop the stream
	mset.stop(false, false)
	node.WaitForStop()

	if numNodes := s.numRaftNodes(); numNodes != numNodesStart-1 {
		t.Fatalf("RAFT nodes after stream stop incorrect: %d vs %d", numNodesStart, numNodes)
	}
}

func TestJetStreamClusterStreamAccountingOnStoreError(t *testing.T) {
	c := createJetStreamClusterWithTemplate(t, jsClusterMaxBytesAccountLimitTempl, "NATS", 3)
	defer c.shutdown()

	nc, js := jsClientConnect(t, c.randomServer())
	defer nc.Close()

	_, err := js.AddStream(&nats.StreamConfig{
		Name:     "TEST",
		Subjects: []string{"*"},
		MaxBytes: 1 * 1024 * 1024 * 1024,
		Replicas: 3,
	})
	require_NoError(t, err)

	msg := strings.Repeat("Z", 32*1024)
	for i := 0; i < 10; i++ {
		sendStreamMsg(t, nc, "foo", msg)
	}
	s := c.randomServer()
	acc, err := s.LookupAccount("$U")
	require_NoError(t, err)
	mset, err := acc.lookupStream("TEST")
	require_NoError(t, err)
	mset.mu.Lock()
	mset.store.Stop()
	sjs := mset.js
	mset.mu.Unlock()

	// Now delete the stream
	js.DeleteStream("TEST")

	// Wait for this to propgate.
	// The bug will have us not release reserved resources properly.
	checkFor(t, 10*time.Second, 200*time.Millisecond, func() error {
		info, err := js.AccountInfo()
		require_NoError(t, err)
		// Default tier
		if info.Store != 0 {
			return fmt.Errorf("Expected store to be 0 but got %v", friendlyBytes(info.Store))
		}
		return nil
	})

	// Now check js from server directly regarding reserved.
	sjs.mu.RLock()
	reserved := sjs.storeReserved
	sjs.mu.RUnlock()
	// Under bug will show 1GB
	if reserved != 0 {
		t.Fatalf("Expected store reserved to be 0 after stream delete, got %v", friendlyBytes(reserved))
	}
}

func TestJetStreamClusterStreamAccountingDriftFixups(t *testing.T) {
	c := createJetStreamClusterWithTemplate(t, jsClusterMaxBytesAccountLimitTempl, "NATS", 3)
	defer c.shutdown()

	nc, js := jsClientConnect(t, c.randomServer())
	defer nc.Close()

	_, err := js.AddStream(&nats.StreamConfig{
		Name:     "TEST",
		Subjects: []string{"*"},
		MaxBytes: 2 * 1024 * 1024,
		Replicas: 3,
	})
	require_NoError(t, err)

	msg := strings.Repeat("Z", 32*1024)
	for i := 0; i < 100; i++ {
		sendStreamMsg(t, nc, "foo", msg)
	}

	err = js.PurgeStream("TEST")
	require_NoError(t, err)

	checkFor(t, 5*time.Second, 200*time.Millisecond, func() error {
		info, err := js.AccountInfo()
		require_NoError(t, err)
		if info.Store != 0 {
			return fmt.Errorf("Store usage not 0: %d", info.Store)
		}
		return nil
	})

	s := c.leader()
	jsz, err := s.Jsz(nil)
	require_NoError(t, err)
	require_True(t, jsz.JetStreamStats.Store == 0)

	acc, err := s.LookupAccount("$U")
	require_NoError(t, err)
	mset, err := acc.lookupStream("TEST")
	require_NoError(t, err)
	mset.mu.RLock()
	jsa, tier, stype := mset.jsa, mset.tier, mset.stype
	mset.mu.RUnlock()
	// Drift the usage.
	jsa.updateUsage(tier, stype, -100)

	checkFor(t, time.Second, 200*time.Millisecond, func() error {
		info, err := js.AccountInfo()
		require_NoError(t, err)
		if info.Store != 0 {
			return fmt.Errorf("Store usage not 0: %d", info.Store)
		}
		return nil
	})
	jsz, err = s.Jsz(nil)
	require_NoError(t, err)
	require_True(t, jsz.JetStreamStats.Store == 0)
}

// Some older streams seem to have been created or exist with no explicit cluster setting.
// For server <= 2.9.16 you could not scale the streams up since we could not place them in another cluster.
func TestJetStreamClusterStreamScaleUpNoGroupCluster(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "NATS", 3)
	defer c.shutdown()

	nc, js := jsClientConnect(t, c.randomServer())
	defer nc.Close()

	_, err := js.AddStream(&nats.StreamConfig{
		Name:     "TEST",
		Subjects: []string{"*"},
	})
	require_NoError(t, err)

	// Manually going to grab stream assignment and update it to be without the group cluster.
	s := c.streamLeader(globalAccountName, "TEST")
	mset, err := s.GlobalAccount().lookupStream("TEST")
	require_NoError(t, err)

	sa := mset.streamAssignment()
	require_NotNil(t, sa)
	// Make copy to not change stream's
	sa = sa.copyGroup()
	// Remove cluster and preferred.
	sa.Group.Cluster = _EMPTY_
	sa.Group.Preferred = _EMPTY_
	// Insert into meta layer, proposed by the meta leader itself.
	ml := c.leader()
	require_NotNil(t, ml)
	mjs := ml.getJetStream()
	require_NotNil(t, mjs)
	mjs.mu.RLock()
	meta, term := mjs.cluster.meta, mjs.cluster.term
	mjs.mu.RUnlock()
	require_NotNil(t, meta)
	require_NoError(t, meta.Propose(term, encodeUpdateStreamAssignment(sa)))
	// Make sure it got propagated..
	checkFor(t, 10*time.Second, 200*time.Millisecond, func() error {
		sa := mset.streamAssignment().copyGroup()
		require_NotNil(t, sa)
		if sa.Group.Cluster != _EMPTY_ {
			return fmt.Errorf("Cluster still not cleared")
		}
		return nil
	})
	// Now we know it has been nil'd out. Make sure we can scale up.
	_, err = js.UpdateStream(&nats.StreamConfig{
		Name:     "TEST",
		Subjects: []string{"*"},
		Replicas: 3,
	})
	require_NoError(t, err)
}

// https://github.com/nats-io/nats-server/issues/4162
func TestJetStreamClusterStaleDirectGetOnRestart(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "NATS", 3)
	defer c.shutdown()

	nc, js := jsClientConnect(t, c.randomServer())
	defer nc.Close()

	kv, err := js.CreateKeyValue(&nats.KeyValueConfig{
		Bucket:   "TEST",
		Replicas: 3,
	})
	require_NoError(t, err)

	_, err = kv.PutString("foo", "bar")
	require_NoError(t, err)

	// Close client in case we were connected to server below.
	// We will recreate.
	nc.Close()

	// Shutdown a non-leader.
	s := c.randomNonStreamLeader(globalAccountName, "KV_TEST")
	s.Shutdown()

	nc, js = jsClientConnect(t, c.randomServer())
	defer nc.Close()

	kv, err = js.KeyValue("TEST")
	require_NoError(t, err)

	_, err = kv.PutString("foo", "baz")
	require_NoError(t, err)

	errCh := make(chan error, 100)
	done := make(chan struct{})

	go func() {
		nc, js := jsClientConnect(t, c.randomServer())
		defer nc.Close()

		kv, err := js.KeyValue("TEST")
		if err != nil {
			errCh <- err
			return
		}

		for {
			select {
			case <-done:
				return
			default:
				entry, err := kv.Get("foo")
				if err != nil {
					errCh <- err
					return
				}
				if v := string(entry.Value()); v != "baz" {
					errCh <- fmt.Errorf("Got wrong value: %q", v)
				}
			}
		}
	}()

	// Restart
	c.restartServer(s)
	// Wait for a bit to make sure as this server participates in direct gets
	// it does not server stale reads.
	time.Sleep(2 * time.Second)
	close(done)

	if len(errCh) > 0 {
		t.Fatalf("Expected no errors but got %v", <-errCh)
	}
}

// This test mimics a user's setup where there is a cloud cluster/domain, and one for eu and ap that are leafnoded into the
// cloud cluster, and one for cn that is leafnoded into the ap cluster.
// We broke basic connectivity in 2.9.17 from publishing in eu for delivery in cn on same account which is daisy chained through ap.
// We will also test cross account delivery in this test as well.
func TestJetStreamClusterLeafnodePlusDaisyChainSetup(t *testing.T) {
	var cloudTmpl = `
		listen: 127.0.0.1:-1
		server_name: %s
		jetstream: {max_mem_store: 256MB, max_file_store: 2GB, domain: CLOUD, store_dir: '%s'}

		leaf { listen: 127.0.0.1:-1 }

		cluster {
			name: %s
			listen: 127.0.0.1:%d
			routes = [%s]
		}

		accounts {
			F {
				jetstream: enabled
				users = [ { user: "F", pass: "pass" } ]
				exports [ { stream: "F.>" } ]
			}
			T {
				jetstream: enabled
				users = [ { user: "T", pass: "pass" } ]
				imports [ { stream: { account: F, subject: "F.>"} } ]
			}
			$SYS { users = [ { user: "admin", pass: "s3cr3t!" } ] }
		}`

	// Now create the cloud and make sure we are connected.
	// Cloud
	c := createJetStreamCluster(t, cloudTmpl, "CLOUD", _EMPTY_, 3, 22020, true)
	defer c.shutdown()

	var lnTmpl = `
		listen: 127.0.0.1:-1
		server_name: %s
		jetstream: {max_mem_store: 256MB, max_file_store: 2GB, store_dir: '%s'}

		{{leaf}}

		cluster {
			name: %s
			listen: 127.0.0.1:%d
			routes = [%s]
		}

		accounts {
			F {
				jetstream: enabled
				users = [ { user: "F", pass: "pass" } ]
				exports [ { stream: "F.>" } ]
			}
			T {
				jetstream: enabled
				users = [ { user: "T", pass: "pass" } ]
				imports [ { stream: { account: F, subject: "F.>"} } ]
			}
			$SYS { users = [ { user: "admin", pass: "s3cr3t!" } ] }
		}`

	var leafFrag = `
			leaf {
				listen: 127.0.0.1:-1
				remotes [ { urls: [ %s ], account: "T" }, { urls: [ %s ], account: "F" } ]
			}`

	genLeafTmpl := func(tmpl string, c *cluster) string {
		t.Helper()
		// Create our leafnode cluster template first.
		var lnt, lnf []string
		for _, s := range c.servers {
			if s.ClusterName() != c.name {
				continue
			}
			ln := s.getOpts().LeafNode
			lnt = append(lnt, fmt.Sprintf("nats://T:pass@%s:%d", ln.Host, ln.Port))
			lnf = append(lnf, fmt.Sprintf("nats://F:pass@%s:%d", ln.Host, ln.Port))
		}
		lntc := strings.Join(lnt, ", ")
		lnfc := strings.Join(lnf, ", ")
		return strings.Replace(tmpl, "{{leaf}}", fmt.Sprintf(leafFrag, lntc, lnfc), 1)
	}

	// Cluster EU
	// Domain is "EU'
	tmpl := strings.Replace(lnTmpl, "store_dir:", fmt.Sprintf(`domain: "%s", store_dir:`, "EU"), 1)
	tmpl = genLeafTmpl(tmpl, c)
	lceu := createJetStreamCluster(t, tmpl, "EU", "EU-", 3, 22110, false)
	lceu.waitOnClusterReady()
	defer lceu.shutdown()

	for _, s := range lceu.servers {
		checkLeafNodeConnectedCount(t, s, 2)
	}

	// Cluster AP
	// Domain is "AP'
	tmpl = strings.Replace(lnTmpl, "store_dir:", fmt.Sprintf(`domain: "%s", store_dir:`, "AP"), 1)
	tmpl = genLeafTmpl(tmpl, c)
	lcap := createJetStreamCluster(t, tmpl, "AP", "AP-", 3, 22180, false)
	lcap.waitOnClusterReady()
	defer lcap.shutdown()

	for _, s := range lcap.servers {
		checkLeafNodeConnectedCount(t, s, 2)
	}

	// Cluster CN
	// Domain is "CN'
	// This one connects to AP, not the cloud hub.
	tmpl = strings.Replace(lnTmpl, "store_dir:", fmt.Sprintf(`domain: "%s", store_dir:`, "CN"), 1)
	tmpl = genLeafTmpl(tmpl, lcap)
	lccn := createJetStreamCluster(t, tmpl, "CN", "CN-", 3, 22280, false)
	lccn.waitOnClusterReady()
	defer lccn.shutdown()

	for _, s := range lccn.servers {
		checkLeafNodeConnectedCount(t, s, 2)
	}

	// Now connect to CN on account F and subscribe to data.
	nc, _ := jsClientConnect(t, lccn.randomServer(), nats.UserInfo("F", "pass"))
	defer nc.Close()
	fsub, err := nc.SubscribeSync("F.EU.>")
	require_NoError(t, err)

	// Same for account T where the import is.
	nc, _ = jsClientConnect(t, lccn.randomServer(), nats.UserInfo("T", "pass"))
	defer nc.Close()
	tsub, err := nc.SubscribeSync("F.EU.>")
	require_NoError(t, err)

	// Let sub propagate.
	time.Sleep(500 * time.Millisecond)

	// Now connect to EU on account F and generate data.
	nc, _ = jsClientConnect(t, lceu.randomServer(), nats.UserInfo("F", "pass"))
	defer nc.Close()

	num := 10
	for i := range num {
		err := nc.Publish("F.EU.DATA", []byte(fmt.Sprintf("MSG-%d", i)))
		require_NoError(t, err)
	}

	checkSubsPending(t, fsub, num)
	// Since we export and import in each cluster, we will receive 4x.
	// First hop from EU -> CLOUD is 1F and 1T
	// Second hop from CLOUD -> AP is 1F, 1T and another 1T
	// Third hop from AP -> CN is 1F, 1T, 1T and 1T
	// Each cluster hop that has the export/import mapping will add another T message copy.
	checkSubsPending(t, tsub, num*4)

	// Create stream in cloud.
	nc, js := jsClientConnect(t, c.randomServer(), nats.UserInfo("F", "pass"))
	defer nc.Close()

	_, err = js.AddStream(&nats.StreamConfig{
		Name:     "TEST",
		Subjects: []string{"TEST.>"},
		Replicas: 3,
	})
	require_NoError(t, err)
	// Only the stream leader subscribes, wait for its interest to propagate.
	for _, s := range c.servers {
		checkSubInterest(t, s, "F", "TEST.0", 2*time.Second)
	}

	for i := range 100 {
		sendStreamMsg(t, nc, fmt.Sprintf("TEST.%d", i), "OK")
	}

	// Now connect to EU.
	nc, js = jsClientConnect(t, lceu.randomServer(), nats.UserInfo("F", "pass"))
	defer nc.Close()

	// Create a mirror.
	_, err = js.AddStream(&nats.StreamConfig{
		Name: "M",
		Mirror: &nats.StreamSource{
			Name:   "TEST",
			Domain: "CLOUD",
		},
	})
	require_NoError(t, err)

	checkFor(t, time.Second, 200*time.Millisecond, func() error {
		si, err := js.StreamInfo("M")
		require_NoError(t, err)
		if si.State.Msgs == 100 {
			return nil
		}
		return fmt.Errorf("State not current: %+v", si.State)
	})
}

// https://github.com/nats-io/nats-server/pull/4197
func TestJetStreamClusterPurgeExReplayAfterRestart(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "P3F", 3)
	defer c.shutdown()

	// Client based API
	nc, js := jsClientConnect(t, c.randomServer())
	defer nc.Close()

	_, err := js.AddStream(&nats.StreamConfig{
		Name:     "TEST",
		Subjects: []string{"TEST.>"},
		Replicas: 3,
	})
	require_NoError(t, err)

	sendStreamMsg(t, nc, "TEST.0", "OK")
	sendStreamMsg(t, nc, "TEST.1", "OK")
	sendStreamMsg(t, nc, "TEST.2", "OK")

	runTest := func(f func(js nats.JetStreamManager)) *nats.StreamInfo {
		nc, js := jsClientConnect(t, c.randomServer())
		defer nc.Close()

		// install snapshot, then execute interior func, ensuring the purge will be recovered later
		fsl := c.streamLeader(globalAccountName, "TEST")
		fsl.JetStreamSnapshotStream(globalAccountName, "TEST")

		f(js)
		time.Sleep(250 * time.Millisecond)

		fsl.Shutdown()
		fsl.WaitForShutdown()
		fsl = c.restartServer(fsl)
		c.waitOnServerCurrent(fsl)

		nc, js = jsClientConnect(t, c.randomServer())
		defer nc.Close()

		c.waitOnStreamLeader(globalAccountName, "TEST")
		sl := c.streamLeader(globalAccountName, "TEST")

		// keep stepping down so the stream leader matches the initial leader
		// we need to check if it restored from the snapshot properly
		for sl != fsl {
			_, err := nc.Request(fmt.Sprintf(JSApiStreamLeaderStepDownT, "TEST"), nil, time.Second)
			require_NoError(t, err)
			c.waitOnStreamLeader(globalAccountName, "TEST")
			sl = c.streamLeader(globalAccountName, "TEST")
		}

		si, err := js.StreamInfo("TEST")
		require_NoError(t, err)
		return si
	}
	si := runTest(func(js nats.JetStreamManager) {
		err = js.PurgeStream("TEST", &nats.StreamPurgeRequest{Subject: "TEST.0"})
		require_NoError(t, err)
	})
	if si.State.Msgs != 2 {
		t.Fatalf("Expected 2 msgs after restart, got %d", si.State.Msgs)
	}
	if si.State.FirstSeq != 2 || si.State.LastSeq != 3 {
		t.Fatalf("Expected FirstSeq=2, LastSeq=3 after restart, got FirstSeq=%d, LastSeq=%d",
			si.State.FirstSeq, si.State.LastSeq)
	}

	si = runTest(func(js nats.JetStreamManager) {
		err = js.PurgeStream("TEST")
		require_NoError(t, err)
		// Send 2 more messages.
		sendStreamMsg(t, nc, "TEST.1", "OK")
		sendStreamMsg(t, nc, "TEST.2", "OK")
	})
	if si.State.Msgs != 2 {
		t.Fatalf("Expected 2 msgs after restart, got %d", si.State.Msgs)
	}
	if si.State.FirstSeq != 4 || si.State.LastSeq != 5 {
		t.Fatalf("Expected FirstSeq=4, LastSeq=5 after restart, got FirstSeq=%d, LastSeq=%d",
			si.State.FirstSeq, si.State.LastSeq)
	}

	// Now test a keep
	si = runTest(func(js nats.JetStreamManager) {
		err = js.PurgeStream("TEST", &nats.StreamPurgeRequest{Keep: 1})
		require_NoError(t, err)
		// Send 4 more messages.
		sendStreamMsg(t, nc, "TEST.1", "OK")
		sendStreamMsg(t, nc, "TEST.2", "OK")
		sendStreamMsg(t, nc, "TEST.3", "OK")
		sendStreamMsg(t, nc, "TEST.1", "OK")
	})
	if si.State.Msgs != 5 {
		t.Fatalf("Expected 5 msgs after restart, got %d", si.State.Msgs)
	}
	if si.State.FirstSeq != 5 || si.State.LastSeq != 9 {
		t.Fatalf("Expected FirstSeq=5, LastSeq=9 after restart, got FirstSeq=%d, LastSeq=%d",
			si.State.FirstSeq, si.State.LastSeq)
	}

	// Now test a keep on a subject
	si = runTest(func(js nats.JetStreamManager) {
		err = js.PurgeStream("TEST", &nats.StreamPurgeRequest{Subject: "TEST.1", Keep: 1})
		require_NoError(t, err)
		// Send 3 more messages.
		sendStreamMsg(t, nc, "TEST.1", "OK")
		sendStreamMsg(t, nc, "TEST.2", "OK")
		sendStreamMsg(t, nc, "TEST.3", "OK")
	})
	if si.State.Msgs != 7 {
		t.Fatalf("Expected 7 msgs after restart, got %d", si.State.Msgs)
	}
	if si.State.FirstSeq != 5 || si.State.LastSeq != 12 {
		t.Fatalf("Expected FirstSeq=5, LastSeq=12 after restart, got FirstSeq=%d, LastSeq=%d",
			si.State.FirstSeq, si.State.LastSeq)
	}
}

func TestJetStreamClusterConsumerCleanupWithSameName(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "R3F", 3)
	defer c.shutdown()

	// Client based API
	nc, js := jsClientConnect(t, c.randomServer())
	defer nc.Close()

	_, err := js.AddStream(&nats.StreamConfig{
		Name:     "UPDATES",
		Subjects: []string{"DEVICE.*"},
		Replicas: 3,
	})
	require_NoError(t, err)

	// Create a consumer that will be an R1 that we will auto-recreate but using the same name.
	// We want to make sure that the system does not continually try to cleanup the new one from the old one.

	// Track the sequence for restart etc.
	var seq atomic.Uint64

	msgCB := func(msg *nats.Msg) {
		msg.AckSync()
		meta, err := msg.Metadata()
		require_NoError(t, err)
		seq.Store(meta.Sequence.Stream)
	}

	waitOnSeqDelivered := func(expected uint64) {
		checkFor(t, 10*time.Second, 200*time.Millisecond, func() error {
			received := seq.Load()
			if received == expected {
				return nil
			}
			return fmt.Errorf("Seq is %d, want %d", received, expected)
		})
	}

	doSub := func() {
		_, err = js.Subscribe(
			"DEVICE.22",
			msgCB,
			nats.ConsumerName("dlc"),
			nats.SkipConsumerLookup(),
			nats.StartSequence(seq.Load()+1),
			nats.MaxAckPending(1), // One at a time.
			nats.ManualAck(),
			nats.ConsumerReplicas(1),
			nats.ConsumerMemoryStorage(),
			nats.MaxDeliver(1),
			nats.InactiveThreshold(time.Second),
			nats.IdleHeartbeat(250*time.Millisecond),
		)
		require_NoError(t, err)
	}

	// Track any errors for consumer not active so we can recreate the consumer.
	errCh := make(chan error, 10)
	nc.SetErrorHandler(func(c *nats.Conn, s *nats.Subscription, err error) {
		if errors.Is(err, nats.ErrConsumerNotActive) {
			s.Unsubscribe()
			errCh <- err
			doSub()
		}
	})

	doSub()

	sendStreamMsg(t, nc, "DEVICE.22", "update-1")
	sendStreamMsg(t, nc, "DEVICE.22", "update-2")
	sendStreamMsg(t, nc, "DEVICE.22", "update-3")
	waitOnSeqDelivered(3)

	// Shutdown the consumer's leader.
	s := c.consumerLeader(globalAccountName, "UPDATES", "dlc")
	s.Shutdown()
	c.waitOnStreamLeader(globalAccountName, "UPDATES")

	// In case our client connection was to the same server.
	nc, _ = jsClientConnect(t, c.randomServer())
	defer nc.Close()

	sendStreamMsg(t, nc, "DEVICE.22", "update-4")
	sendStreamMsg(t, nc, "DEVICE.22", "update-5")
	sendStreamMsg(t, nc, "DEVICE.22", "update-6")

	// Wait for the consumer not active error.
	<-errCh
	// Now restart server with the old consumer.
	c.restartServer(s)
	// Wait on all messages delivered.
	waitOnSeqDelivered(6)
	// Make sure no other errors showed up
	require_True(t, len(errCh) == 0)
}
func TestJetStreamClusterConsumerActions(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "R3F", 3)
	defer c.shutdown()

	nc, js := jsClientConnect(t, c.randomServer())
	defer nc.Close()

	var err error
	_, err = js.AddStream(&nats.StreamConfig{
		Name:     "TEST",
		Subjects: []string{"test"},
	})
	require_NoError(t, err)

	ecSubj := fmt.Sprintf(JSApiConsumerCreateExT, "TEST", "CONSUMER", "test")
	crReq := CreateConsumerRequest{
		Stream: "TEST",
		Config: ConsumerConfig{
			DeliverPolicy: DeliverLast,
			FilterSubject: "test",
			AckPolicy:     AckExplicit,
		},
	}

	// A new consumer. Should not be an error.
	crReq.Action = ActionCreate
	req, err := json.Marshal(crReq)
	require_NoError(t, err)
	resp, err := nc.Request(ecSubj, req, 500*time.Millisecond)
	require_NoError(t, err)
	var ccResp JSApiConsumerCreateResponse
	err = json.Unmarshal(resp.Data, &ccResp)
	require_NoError(t, err)
	if ccResp.Error != nil {
		t.Fatalf("Unexpected error: %v", ccResp.Error)
	}
	ccResp.Error = nil

	// Consumer exists, but config is the same, so should be ok
	resp, err = nc.Request(ecSubj, req, 500*time.Millisecond)
	require_NoError(t, err)
	err = json.Unmarshal(resp.Data, &ccResp)
	require_NoError(t, err)
	if ccResp.Error != nil {
		t.Fatalf("Unexpected er response: %v", ccResp.Error)
	}
	ccResp.Error = nil
	// Consumer exists. Config is different, so should error
	crReq.Config.Description = "changed"
	req, err = json.Marshal(crReq)
	require_NoError(t, err)
	resp, err = nc.Request(ecSubj, req, 500*time.Millisecond)
	require_NoError(t, err)
	err = json.Unmarshal(resp.Data, &ccResp)
	require_NoError(t, err)
	if ccResp.Error == nil {
		t.Fatalf("Unexpected ok response")
	}

	ccResp.Error = nil
	// Consumer update, so update should be ok
	crReq.Action = ActionUpdate
	crReq.Config.Description = "changed again"
	req, err = json.Marshal(crReq)
	require_NoError(t, err)
	resp, err = nc.Request(ecSubj, req, 500*time.Millisecond)
	require_NoError(t, err)
	err = json.Unmarshal(resp.Data, &ccResp)
	require_NoError(t, err)
	if ccResp.Error != nil {
		t.Fatalf("Unexpected error response: %v", ccResp.Error)
	}

	ecSubj = fmt.Sprintf(JSApiConsumerCreateExT, "TEST", "NEW", "test")
	ccResp.Error = nil
	// Updating new consumer, so should error
	crReq.Config.Name = "NEW"
	req, err = json.Marshal(crReq)
	require_NoError(t, err)
	resp, err = nc.Request(ecSubj, req, 500*time.Millisecond)
	require_NoError(t, err)
	err = json.Unmarshal(resp.Data, &ccResp)
	require_NoError(t, err)
	if ccResp.Error == nil {
		t.Fatalf("Unexpected ok response")
	}
}

func TestJetStreamClusterSnapshotAndRestoreWithHealthz(t *testing.T) {
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

	toSend, msg := 1000, bytes.Repeat([]byte("Z"), 1024)
	for i := 0; i < toSend; i++ {
		_, err := js.PublishAsync("foo", msg)
		require_NoError(t, err)
	}
	select {
	case <-js.PublishAsyncComplete():
	case <-time.After(5 * time.Second):
		t.Fatalf("Did not receive completion signal")
	}

	// Take a backup of the stream.
	sc, ss, snapshot := performStreamBackup(t, nc, "TEST")

	// Delete before we try to restore.
	require_NoError(t, js.DeleteStream("TEST"))

	checkHealth := func() {
		t.Helper()
		checkFor(t, 10*time.Second, 200*time.Millisecond, func() error {
			for _, s := range c.servers {
				status := s.healthz(nil)
				if status.Error != _EMPTY_ {
					return fmt.Errorf("%s - %v", s.Name(), status.Error)
				}
				if status.Status != "ok" {
					return fmt.Errorf("%s - %v", s.Name(), status.Status)
				}
			}
			return nil
		})
	}

	// Restore the backup.
	require_True(t, performStreamRestore(t, nc, sc, ss, snapshot))
	checkHealth()

	// Make sure stepdown works, this would fail before the fix.
	_, err = nc.Request(fmt.Sprintf(JSApiStreamLeaderStepDownT, "TEST"), nil, 5*time.Second)
	require_NoError(t, err)

	si, err := js.StreamInfo("TEST")
	require_NoError(t, err)
	require_True(t, si.State.Msgs == uint64(toSend))

	checkHealth()

	// Now make sure if we try to restore to a single server that the artifact is cleaned up and the server returns ok for healthz.
	s := RunBasicJetStreamServer(t)
	defer s.Shutdown()

	nc, _ = jsClientConnect(t, s)
	defer nc.Close()

	// Restore the backup.
	require_True(t, performStreamRestore(t, nc, sc, ss, snapshot))

	status := s.healthz(nil)
	require_Equal(t, status.StatusCode, 200)
}

func TestJetStreamClusterBadEncryptKey(t *testing.T) {
	c := createJetStreamClusterWithTemplate(t, jsClusterEncryptedTempl, "JSC", 3)
	defer c.shutdown()

	nc, js := jsClientConnect(t, c.randomServer())
	defer nc.Close()

	// Create 10 streams.
	for i := 0; i < 10; i++ {
		_, err := js.AddStream(&nats.StreamConfig{
			Name:     fmt.Sprintf("TEST-%d", i),
			Replicas: 3,
		})
		require_NoError(t, err)
	}

	// Grab random server.
	s := c.randomServer()
	s.Shutdown()
	s.WaitForShutdown()

	var opts *Options
	for i := 0; i < len(c.servers); i++ {
		if c.servers[i] == s {
			opts = c.opts[i]
			break
		}
	}
	require_NotNil(t, opts)

	// Replace key with an empty key.
	buf, err := os.ReadFile(opts.ConfigFile)
	require_NoError(t, err)
	nbuf := bytes.Replace(buf, []byte("key: \"s3cr3t!\""), []byte("key: \"\""), 1)
	err = os.WriteFile(opts.ConfigFile, nbuf, 0640)
	require_NoError(t, err)

	// Make sure trying to start the server now fails.
	s, err = NewServer(LoadConfig(opts.ConfigFile))
	require_NoError(t, err)
	require_NotNil(t, s)
	s.Start()
	if err := s.readyForConnections(1 * time.Second); err == nil {
		t.Fatalf("Expected server not to start")
	}
}

func TestJetStreamClusterAccountUsageDrifts(t *testing.T) {
	tmpl := `
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
	opFrag := `
			operator: %s
			system_account: %s
			resolver: { type: MEM }
			resolver_preload = {
				%s : %s
				%s : %s
			}
		`

	sysKp, syspub := createKey(t)
	sysJwt := encodeClaim(t, jwt.NewAccountClaims(syspub), syspub)
	sysCreds := newUser(t, sysKp)

	accKp, aExpPub := createKey(t)
	accClaim := jwt.NewAccountClaims(aExpPub)
	accClaim.Limits.JetStreamTieredLimits["R1"] = jwt.JetStreamLimits{
		DiskStorage: -1, Consumer: 1, Streams: 1}
	accClaim.Limits.JetStreamTieredLimits["R3"] = jwt.JetStreamLimits{
		DiskStorage: -1, Consumer: 1, Streams: 1}
	accJwt := encodeClaim(t, accClaim, aExpPub)
	accCreds := newUser(t, accKp)

	template := tmpl + fmt.Sprintf(opFrag, ojwt, syspub, syspub, sysJwt, aExpPub, accJwt)
	c := createJetStreamClusterWithTemplate(t, template, "R3S", 3)
	defer c.shutdown()

	nc, js := jsClientConnect(t, c.randomServer(), nats.UserCredentials(accCreds))
	defer nc.Close()

	// Prevent 'nats: JetStream not enabled for account' when creating the first stream.
	c.waitOnAccount(aExpPub)

	_, err := js.AddStream(&nats.StreamConfig{
		Name:     "TEST1",
		Subjects: []string{"foo"},
		MaxBytes: 1 * 1024 * 1024 * 1024,
		MaxMsgs:  1000,
		Replicas: 3,
	})
	require_NoError(t, err)

	_, err = js.AddStream(&nats.StreamConfig{
		Name:     "TEST2",
		Subjects: []string{"bar"},
	})
	require_NoError(t, err)

	// These expected store values can come directly from stream info's state bytes.
	// We will *= 3 for R3
	checkAccount := func(r1u, r3u uint64) {
		t.Helper()
		r3u *= 3

		// Remote usage updates can be delayed, so wait for a bit for values we want.
		checkFor(t, 10*time.Second, 250*time.Millisecond, func() error {
			info, err := js.AccountInfo()
			require_NoError(t, err)
			require_True(t, len(info.Tiers) >= 2)
			// These can move.
			if u := info.Tiers["R1"].Store; u != r1u {
				return fmt.Errorf("Expected R1 to be %v, got %v", friendlyBytes(r1u), friendlyBytes(u))
			}
			if u := info.Tiers["R3"].Store; u != r3u {
				return fmt.Errorf("Expected R3 to be %v, got %v", friendlyBytes(r3u), friendlyBytes(u))
			}
			return nil
		})
	}

	checkAccount(0, 0)

	// Now add in some R3 data.
	msg := bytes.Repeat([]byte("Z"), 32*1024)     // 32k
	smallMsg := bytes.Repeat([]byte("Z"), 4*1024) // 4k

	for i := 0; i < 1000; i++ {
		js.Publish("foo", msg)
	}
	sir3, err := js.StreamInfo("TEST1")
	require_NoError(t, err)

	checkAccount(0, sir3.State.Bytes)

	// Now add in some R1 data.
	for i := 0; i < 100; i++ {
		js.Publish("bar", msg)
	}

	sir1, err := js.StreamInfo("TEST2")
	require_NoError(t, err)

	checkAccount(sir1.State.Bytes, sir3.State.Bytes)

	// We will now test a bunch of scenarios to see that we are doing accounting correctly.

	// Since our R3 has a limit of 1000 msgs, let's add in more msgs and drop older ones.
	for i := 0; i < 100; i++ {
		js.Publish("foo", smallMsg)
	}
	sir3, err = js.StreamInfo("TEST1")
	require_NoError(t, err)

	checkAccount(sir1.State.Bytes, sir3.State.Bytes)

	// Move our R3 stream leader and make sure acounting is correct.
	_, err = nc.Request(fmt.Sprintf(JSApiStreamLeaderStepDownT, "TEST1"), nil, time.Second)
	require_NoError(t, err)
	c.waitOnStreamLeader(aExpPub, "TEST1")

	checkAccount(sir1.State.Bytes, sir3.State.Bytes)

	// Now scale down.
	_, err = js.UpdateStream(&nats.StreamConfig{
		Name:     "TEST1",
		Subjects: []string{"foo"},
		MaxBytes: 1 * 1024 * 1024 * 1024,
		MaxMsgs:  1000,
		Replicas: 1,
	})
	require_NoError(t, err)

	checkAccount(sir1.State.Bytes+sir3.State.Bytes, 0)

	// Add in more msgs which will replace the older and bigger ones.
	for i := 0; i < 100; i++ {
		js.Publish("foo", smallMsg)
	}
	sir3, err = js.StreamInfo("TEST1")
	require_NoError(t, err)

	// Now scale back up.
	cfg := &nats.StreamConfig{
		Name:     "TEST1",
		Subjects: []string{"foo"},
		MaxBytes: 1 * 1024 * 1024 * 1024,
		MaxMsgs:  1000,
		Replicas: 3,
	}
	_, err = js.UpdateStream(cfg)
	if err != nil {
		// If still in progress, wait for it to complete before retrying.
		require_Error(t, err, NewJSStreamMoveInProgressError())
		c.waitOnStreamLeader(aExpPub, "TEST1")
		_, err = js.UpdateStream(cfg)
	}
	require_NoError(t, err)
	c.waitOnStreamLeader(aExpPub, "TEST1")

	checkAccount(sir1.State.Bytes, sir3.State.Bytes)

	// Test Purge.
	err = js.PurgeStream("TEST1")
	require_NoError(t, err)

	checkAccount(sir1.State.Bytes, 0)

	for i := 0; i < 1000; i++ {
		js.Publish("foo", smallMsg)
	}
	sir3, err = js.StreamInfo("TEST1")
	require_NoError(t, err)

	checkAccount(sir1.State.Bytes, sir3.State.Bytes)

	// Need system user here to move the leader.
	snc, _ := jsClientConnect(t, c.randomServer(), nats.UserCredentials(sysCreds))
	defer snc.Close()

	requestLeaderStepDown := func() {
		ml := c.leader()
		checkFor(t, 5*time.Second, 250*time.Millisecond, func() error {
			if cml := c.leader(); cml == ml {
				snc.Request(JSApiLeaderStepDown, nil, time.Second)
				return fmt.Errorf("Metaleader has not moved yet")
			}
			return nil
		})
	}

	// Test meta leader stepdowns.
	for i := 0; i < len(c.servers); i++ {
		requestLeaderStepDown()
		checkAccount(sir1.State.Bytes, sir3.State.Bytes)
	}

	// Now test cluster reset operations where we internally reset the NRG and optionally the stream too.
	// Only applicable to TEST1 stream which is R3.
	nl := c.randomNonStreamLeader(aExpPub, "TEST1")
	acc, err := nl.LookupAccount(aExpPub)
	require_NoError(t, err)
	mset, err := acc.lookupStream("TEST1")
	require_NoError(t, err)
	// NRG only
	mset.resetClusteredState(mset.raftNode(), nil)
	checkAccount(sir1.State.Bytes, sir3.State.Bytes)
	// Need to re-lookup this stream since we will recreate from reset above.
	checkFor(t, 5*time.Second, 200*time.Millisecond, func() error {
		mset, err = acc.lookupStream("TEST1")
		return err
	})
	// Now NRG and Stream state itself.
	mset.resetClusteredState(mset.raftNode(), errFirstSequenceMismatch)
	checkAccount(sir1.State.Bytes, sir3.State.Bytes)

	// Now test server restart
	for _, s := range c.servers {
		s.Shutdown()
		s.WaitForShutdown()
		s = c.restartServer(s)

		// Wait on healthz and leader etc.
		checkFor(t, 10*time.Second, 200*time.Millisecond, func() error {
			if hs := s.healthz(nil); hs.Error != _EMPTY_ {
				return errors.New(hs.Error)
			}
			return nil
		})
		c.waitOnLeader()
		c.waitOnStreamLeader(aExpPub, "TEST1")
		c.waitOnStreamLeader(aExpPub, "TEST2")

		// Now check account again.
		checkAccount(sir1.State.Bytes, sir3.State.Bytes)
	}
}

func TestJetStreamClusterStreamFailTracking(t *testing.T) {
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

	m := nats.NewMsg("foo")
	m.Data = []byte("OK")

	b, bsz := 0, 5
	sendBatch := func() {
		for i := b * bsz; i < b*bsz+bsz; i++ {
			msgId := fmt.Sprintf("ID:%d", i)
			m.Header.Set(JSMsgId, msgId)
			// Send it twice on purpose.
			js.PublishMsg(m)
			js.PublishMsg(m)
		}
		b++
	}

	sendBatch()

	_, err = nc.Request(fmt.Sprintf(JSApiStreamLeaderStepDownT, "TEST"), nil, time.Second)
	require_NoError(t, err)
	c.waitOnStreamLeader(globalAccountName, "TEST")

	sendBatch()

	// Now stop one and restart.
	nl := c.randomNonStreamLeader(globalAccountName, "TEST")
	mset, err := nl.GlobalAccount().lookupStream("TEST")
	require_NoError(t, err)
	// Reset raft
	mset.resetClusteredState(mset.raftNode(), nil)
	time.Sleep(100 * time.Millisecond)

	nl.Shutdown()
	nl.WaitForShutdown()

	sendBatch()

	nl = c.restartServer(nl)

	sendBatch()

	for {
		_, err = nc.Request(fmt.Sprintf(JSApiStreamLeaderStepDownT, "TEST"), nil, time.Second)
		require_NoError(t, err)
		c.waitOnStreamLeader(globalAccountName, "TEST")
		if nl == c.streamLeader(globalAccountName, "TEST") {
			break
		}
	}

	sendBatch()

	_, err = js.UpdateStream(&nats.StreamConfig{
		Name:     "TEST",
		Subjects: []string{"foo"},
		Replicas: 1,
	})
	require_NoError(t, err)

	// Make sure all in order.
	errCh := make(chan error, 100)
	var wg sync.WaitGroup
	wg.Add(1)

	expected, seen := b*bsz, 0

	sub, err := js.Subscribe("foo", func(msg *nats.Msg) {
		expectedID := fmt.Sprintf("ID:%d", seen)
		if v := msg.Header.Get(JSMsgId); v != expectedID {
			errCh <- err
			wg.Done()
			msg.Sub.Unsubscribe()
			return
		}
		seen++
		if seen >= expected {
			wg.Done()
			msg.Sub.Unsubscribe()
		}
	})
	require_NoError(t, err)
	defer sub.Unsubscribe()

	wg.Wait()
	if len(errCh) > 0 {
		t.Fatalf("Expected no errors, got %d: %v", len(errCh), <-errCh)
	}
}

func TestJetStreamClusterStreamFailTrackingSnapshots(t *testing.T) {
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

	m := nats.NewMsg("foo")
	m.Data = []byte("OK")

	// Send 1000 a dupe every msgID.
	for i := 0; i < 1000; i++ {
		msgId := fmt.Sprintf("ID:%d", i)
		m.Header.Set(JSMsgId, msgId)
		// Send it twice on purpose.
		js.PublishMsg(m)
		js.PublishMsg(m)
	}

	// Now stop one.
	nl := c.randomNonStreamLeader(globalAccountName, "TEST")
	nl.Shutdown()
	nl.WaitForShutdown()

	// Now send more and make sure leader snapshots.
	for i := 1000; i < 2000; i++ {
		msgId := fmt.Sprintf("ID:%d", i)
		m.Header.Set(JSMsgId, msgId)
		// Send it twice on purpose.
		js.PublishMsg(m)
		js.PublishMsg(m)
	}

	sl := c.streamLeader(globalAccountName, "TEST")
	mset, err := sl.GlobalAccount().lookupStream("TEST")
	require_NoError(t, err)
	node := mset.raftNode()
	require_NotNil(t, node)
	node.InstallSnapshot(mset.stateSnapshot(), false)

	// Now restart nl
	nl = c.restartServer(nl)
	c.waitOnServerCurrent(nl)

	// Move leader to NL
	for {
		_, err = nc.Request(fmt.Sprintf(JSApiStreamLeaderStepDownT, "TEST"), nil, time.Second)
		require_NoError(t, err)
		c.waitOnStreamLeader(globalAccountName, "TEST")
		if nl == c.streamLeader(globalAccountName, "TEST") {
			break
		}
	}

	_, err = js.UpdateStream(&nats.StreamConfig{
		Name:     "TEST",
		Subjects: []string{"foo"},
		Replicas: 1,
	})
	require_NoError(t, err)

	// Make sure all in order.
	errCh := make(chan error, 100)
	var wg sync.WaitGroup
	wg.Add(1)

	expected, seen := 2000, 0

	sub, err := js.Subscribe("foo", func(msg *nats.Msg) {
		expectedID := fmt.Sprintf("ID:%d", seen)
		if v := msg.Header.Get(JSMsgId); v != expectedID {
			errCh <- err
			wg.Done()
			msg.Sub.Unsubscribe()
			return
		}
		seen++
		if seen >= expected {
			wg.Done()
			msg.Sub.Unsubscribe()
		}
	})
	require_NoError(t, err)
	defer sub.Unsubscribe()

	wg.Wait()
	if len(errCh) > 0 {
		t.Fatalf("Expected no errors, got %d: %v", len(errCh), <-errCh)
	}
}

func TestJetStreamClusterOrphanConsumerSubjects(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "R3S", 3)
	defer c.shutdown()

	nc, js := jsClientConnect(t, c.randomServer())
	defer nc.Close()

	_, err := js.AddStream(&nats.StreamConfig{
		Name:     "TEST",
		Subjects: []string{"foo.>", "bar.>"},
		Replicas: 3,
	})
	require_NoError(t, err)

	_, err = js.AddConsumer("TEST", &nats.ConsumerConfig{
		Name:          "consumer_foo",
		Durable:       "consumer_foo",
		FilterSubject: "foo.something",
	})
	require_NoError(t, err)

	for _, replicas := range []int{3, 1, 3} {
		cfg := &nats.StreamConfig{
			Name:     "TEST",
			Subjects: []string{"bar.>"},
			Replicas: replicas,
		}
		_, err = js.UpdateStream(cfg)
		if err != nil {
			// If still in progress, wait for it to complete before retrying.
			require_Error(t, err, NewJSStreamMoveInProgressError())
			c.waitOnStreamLeader("$G", "TEST")
			_, err = js.UpdateStream(cfg)
		}
		require_NoError(t, err)
		c.waitOnAllCurrent()
	}

	c.waitOnStreamLeader("$G", "TEST")
	c.waitOnConsumerLeader("$G", "TEST", "consumer_foo")

	info, err := js.ConsumerInfo("TEST", "consumer_foo")
	require_NoError(t, err)
	require_True(t, info.Cluster != nil)
	require_NotEqual(t, info.Cluster.Leader, "")
	require_Equal(t, len(info.Cluster.Replicas), 2)
}

func TestJetStreamClusterDurableConsumerInactiveThresholdLeaderSwitch(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "R3S", 3)
	defer c.shutdown()

	nc, js := jsClientConnect(t, c.randomServer())
	defer nc.Close()

	_, err := js.AddStream(&nats.StreamConfig{
		Name:     "TEST",
		Subjects: []string{"*"},
		Replicas: 3,
	})
	require_NoError(t, err)

	// Queue a msg.
	sendStreamMsg(t, nc, "foo", "ok")

	thresh := 250 * time.Millisecond

	// This will start the timer.
	sub, err := js.PullSubscribe("foo", "dlc", nats.InactiveThreshold(thresh))
	require_NoError(t, err)

	// Switch over leader.
	cl := c.consumerLeader(globalAccountName, "TEST", "dlc")
	cl.JetStreamStepdownConsumer(globalAccountName, "TEST", "dlc")
	c.waitOnConsumerLeader(globalAccountName, "TEST", "dlc")

	// Create activity on this consumer.
	msgs, err := sub.Fetch(1)
	require_NoError(t, err)
	require_True(t, len(msgs) == 1)

	// This is consider activity as well. So we can watch now up to thresh to make sure consumer still active.
	msgs[0].AckSync()

	// The consumer should not disappear for next `thresh` interval unless old leader does so.
	timeout := time.Now().Add(thresh)
	for time.Now().Before(timeout) {
		_, err := js.ConsumerInfo("TEST", "dlc")
		if err == nats.ErrConsumerNotFound {
			t.Fatalf("Consumer deleted when it should not have been")
		}
	}
}

func TestJetStreamClusterConsumerMaxDeliveryNumAckPendingBug(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "R3S", 3)
	defer c.shutdown()

	nc, js := jsClientConnect(t, c.randomServer())
	defer nc.Close()

	_, err := js.AddStream(&nats.StreamConfig{
		Name:     "TEST",
		Subjects: []string{"*"},
		Replicas: 3,
	})
	require_NoError(t, err)

	// send 50 msgs
	for i := 0; i < 50; i++ {
		_, err := js.Publish("foo", []byte("ok"))
		require_NoError(t, err)
	}

	subscribeAdvisoriesCount := func(consumer string) *atomic.Int64 {
		t.Helper()
		var count atomic.Int64
		subj := fmt.Sprintf("%s.%s.%s", JSAdvisoryConsumerMaxDeliveryExceedPre, "TEST", consumer)
		sub, err := nc.Subscribe(subj, func(*nats.Msg) { count.Add(1) })
		require_NoError(t, err)
		t.Cleanup(func() { sub.Unsubscribe() })
		require_NoError(t, nc.Flush())
		return &count
	}
	requireAdvisoriesCount := func(consumer string, count *atomic.Int64, want int64, when string) {
		t.Helper()
		checkFor(t, 2*time.Second, 200*time.Millisecond, func() error {
			if got := count.Load(); got != want {
				return fmt.Errorf("%s consumer expected %d advisories %s, got %d", consumer, want, when, got)
			}
			return nil
		})
	}

	// File based.
	fileAdv := subscribeAdvisoriesCount("file")
	sub, err := js.PullSubscribe("foo", "file",
		nats.ManualAck(),
		nats.MaxDeliver(1),
		nats.AckWait(time.Second),
		nats.MaxAckPending(10),
	)
	require_NoError(t, err)

	msgs, err := sub.Fetch(10)
	require_NoError(t, err)
	require_Equal(t, len(msgs), 10)

	// Let first batch expire.
	time.Sleep(1200 * time.Millisecond)
	requireAdvisoriesCount("file", fileAdv, 10, "before stepdown")

	cia, err := js.ConsumerInfo("TEST", "file")
	require_NoError(t, err)

	// Make sure followers will have exact same state.
	_, err = nc.Request(fmt.Sprintf(JSApiConsumerLeaderStepDownT, "TEST", "file"), nil, time.Second)
	require_NoError(t, err)
	c.waitOnConsumerLeader(globalAccountName, "TEST", "file")

	cib, err := js.ConsumerInfo("TEST", "file")
	require_NoError(t, err)

	// Want to compare sans cluster details which we know will change due to leader change.
	// Also last activity for delivered can be slightly off so nil out as well.
	checkConsumerInfo := func(a, b *nats.ConsumerInfo, replicated bool) {
		t.Helper()
		require_Equal(t, a.Delivered.Consumer, 10)
		require_Equal(t, a.Delivered.Stream, 10)
		// Agreed upon state is always used. Otherwise, o.asflr and o.adflr would be skipped ahead.
		require_Equal(t, a.AckFloor.Consumer, 0)
		require_Equal(t, a.AckFloor.Stream, 0)
		require_Equal(t, a.NumPending, 40)
		require_Equal(t, a.NumRedelivered, 0)
		a.Cluster, b.Cluster = nil, nil
		a.Delivered.Last, b.Delivered.Last = nil, nil
		if !reflect.DeepEqual(a, b) {
			t.Fatalf("ConsumerInfo do not match\n\t%+v\n\t%+v", a, b)
		}
	}

	checkConsumerInfo(cia, cib, true)

	// Give the new leader a settle window. setLeader calls checkPending
	// synchronously, but the apply goroutine may run a few ms later.
	time.Sleep(500 * time.Millisecond)
	requireAdvisoriesCount("file", fileAdv, 10, "after stepdown")

	// Memory based.
	memAdv := subscribeAdvisoriesCount("mem")
	sub, err = js.PullSubscribe("foo", "mem",
		nats.ManualAck(),
		nats.MaxDeliver(1),
		nats.AckWait(time.Second),
		nats.MaxAckPending(10),
		nats.ConsumerMemoryStorage(),
	)
	require_NoError(t, err)

	msgs, err = sub.Fetch(10)
	require_NoError(t, err)
	require_Equal(t, len(msgs), 10)

	// Let first batch retry and expire.
	time.Sleep(1200 * time.Millisecond)
	requireAdvisoriesCount("mem", memAdv, 10, "before stepdown")

	cia, err = js.ConsumerInfo("TEST", "mem")
	require_NoError(t, err)

	// Make sure followers will have exact same state.
	_, err = nc.Request(fmt.Sprintf(JSApiConsumerLeaderStepDownT, "TEST", "mem"), nil, time.Second)
	require_NoError(t, err)
	c.waitOnConsumerLeader(globalAccountName, "TEST", "mem")

	cib, err = js.ConsumerInfo("TEST", "mem")
	require_NoError(t, err)

	checkConsumerInfo(cia, cib, true)
	requireAdvisoriesCount("mem", memAdv, 10, "after stepdown")

	// Now file based but R1 and server restart.
	r1Adv := subscribeAdvisoriesCount("r1")

	sub, err = js.PullSubscribe("foo", "r1",
		nats.ManualAck(),
		nats.MaxDeliver(1),
		nats.AckWait(time.Second),
		nats.MaxAckPending(10),
		nats.ConsumerReplicas(1),
	)
	require_NoError(t, err)

	msgs, err = sub.Fetch(10)
	require_NoError(t, err)
	require_Equal(t, len(msgs), 10)

	// Let first batch retry and expire.
	time.Sleep(1200 * time.Millisecond)
	requireAdvisoriesCount("r1", r1Adv, 10, "before restart")

	cia, err = js.ConsumerInfo("TEST", "r1")
	require_NoError(t, err)

	cl := c.consumerLeader(globalAccountName, "TEST", "r1")
	cl.Shutdown()
	cl.WaitForShutdown()
	cl = c.restartServer(cl)
	c.waitOnServerCurrent(cl)
	c.waitOnStreamLeader(globalAccountName, "TEST")
	c.waitOnConsumerLeader(globalAccountName, "TEST", "r1")

	cib, err = js.ConsumerInfo("TEST", "r1")
	require_NoError(t, err)

	// Created can skew a small bit due to server restart, this is expected.
	now := time.Now()
	cia.Created, cib.Created = now, now
	checkConsumerInfo(cia, cib, false)

	// On startup the R1 consumer's setLeader -> readStoredState -> applyState
	// reloads pending from disk; if ghosts persisted, setLeader's checkPending
	// re-fires every advisory exactly as a follower-promoted leader would.
	time.Sleep(500 * time.Millisecond)
	requireAdvisoriesCount("r1", r1Adv, 10, "after server restart")
}

// https://github.com/nats-io/nats-server/issues/7148
func TestJetStreamClusterConsumerMaxDeliveryLoweredRedeliveredAdvisory(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "R3S", 3)
	defer c.shutdown()

	nc, js := jsClientConnect(t, c.randomServer())
	defer nc.Close()

	_, err := js.AddStream(&nats.StreamConfig{
		Name:     "TEST",
		Subjects: []string{"*"},
		Replicas: 3,
	})
	require_NoError(t, err)

	// send 10 msgs, exactly what we pull, so no new deliveries remain after the
	// pending ones are expired.
	for i := 0; i < 10; i++ {
		_, err := js.Publish("foo", []byte("ok"))
		require_NoError(t, err)
	}

	subscribeAdvisoriesCount := func(consumer string) *atomic.Int64 {
		t.Helper()
		var count atomic.Int64
		subj := fmt.Sprintf("%s.%s.%s", JSAdvisoryConsumerMaxDeliveryExceedPre, "TEST", consumer)
		sub, err := nc.Subscribe(subj, func(*nats.Msg) { count.Add(1) })
		require_NoError(t, err)
		t.Cleanup(func() { sub.Unsubscribe() })
		require_NoError(t, nc.Flush())
		return &count
	}
	requireAdvisoriesCount := func(consumer string, count *atomic.Int64, want int64, when string) {
		t.Helper()
		checkFor(t, 2*time.Second, 200*time.Millisecond, func() error {
			if got := count.Load(); got != want {
				return fmt.Errorf("%s consumer expected %d advisories %s, got %d", consumer, want, when, got)
			}
			return nil
		})
	}

	// File based.
	redeliveredAdv := subscribeAdvisoriesCount("redelivered")
	sub, err := js.PullSubscribe("foo", "redelivered",
		nats.ManualAck(),
		// Start with unlimited MaxDeliver so redeliveries can grow the count.
		nats.MaxDeliver(-1),
		// Keep AckWait long enough that messages are expired through getNextMsg on
		// redelivery rather than through the AckWait expiry path.
		nats.AckWait(30*time.Second),
		nats.MaxAckPending(10),
	)
	require_NoError(t, err)

	// Two NAK rounds push the delivery count past what a later MaxDeliver of 1
	// would allow.
	for i := 0; i < 2; i++ {
		msgs, err := sub.Fetch(10)
		require_NoError(t, err)
		require_Equal(t, len(msgs), 10)
		for _, m := range msgs {
			require_NoError(t, m.Nak())
		}
	}

	// Lower MaxDeliver well below the current delivery count.
	_, err = js.UpdateConsumer("TEST", &nats.ConsumerConfig{
		Durable:       "redelivered",
		FilterSubject: "foo",
		AckPolicy:     nats.AckExplicitPolicy,
		MaxDeliver:    1,
		AckWait:       30 * time.Second,
		MaxAckPending: 10,
	})
	require_NoError(t, err)

	// The next pull runs getNextMsg over the redelivery queue, which expires the
	// messages since they have hit MaxDeliver.
	_, err = sub.Fetch(10, nats.MaxWait(500*time.Millisecond))
	require_Error(t, err, nats.ErrTimeout)
	requireAdvisoriesCount("redelivered", redeliveredAdv, 10, "before stepdown")

	// Make sure followers will have exact same state.
	_, err = nc.Request(fmt.Sprintf(JSApiConsumerLeaderStepDownT, "TEST", "redelivered"), nil, time.Second)
	require_NoError(t, err)
	c.waitOnConsumerLeader(globalAccountName, "TEST", "redelivered")

	// Give the new leader a settle window. setLeader calls checkPending
	// synchronously, but the apply goroutine may run a few ms later.
	time.Sleep(500 * time.Millisecond)
	requireAdvisoriesCount("redelivered", redeliveredAdv, 10, "after stepdown")
}

// https://github.com/nats-io/nats-server/issues/7148
func TestJetStreamClusterConsumerMaxDeliveryLoweredAckWaitAdvisory(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "R3S", 3)
	defer c.shutdown()

	nc, js := jsClientConnect(t, c.randomServer())
	defer nc.Close()

	_, err := js.AddStream(&nats.StreamConfig{
		Name:     "TEST",
		Subjects: []string{"*"},
		Replicas: 3,
	})
	require_NoError(t, err)

	// send 50 msgs
	for i := 0; i < 50; i++ {
		_, err := js.Publish("foo", []byte("ok"))
		require_NoError(t, err)
	}

	subscribeAdvisoriesCount := func(consumer string) *atomic.Int64 {
		t.Helper()
		var count atomic.Int64
		subj := fmt.Sprintf("%s.%s.%s", JSAdvisoryConsumerMaxDeliveryExceedPre, "TEST", consumer)
		sub, err := nc.Subscribe(subj, func(*nats.Msg) { count.Add(1) })
		require_NoError(t, err)
		t.Cleanup(func() { sub.Unsubscribe() })
		require_NoError(t, nc.Flush())
		return &count
	}
	requireAdvisoriesCount := func(consumer string, count *atomic.Int64, want int64, when string) {
		t.Helper()
		checkFor(t, 2*time.Second, 200*time.Millisecond, func() error {
			if got := count.Load(); got != want {
				return fmt.Errorf("%s consumer expected %d advisories %s, got %d", consumer, want, when, got)
			}
			return nil
		})
	}

	// File based.
	ackWaitAdv := subscribeAdvisoriesCount("ackwait")
	sub, err := js.PullSubscribe("foo", "ackwait",
		nats.ManualAck(),
		// Start with unlimited MaxDeliver so redeliveries can grow the count.
		nats.MaxDeliver(-1),
		nats.AckWait(2*time.Second),
		nats.MaxAckPending(10),
	)
	require_NoError(t, err)

	// Two NAK rounds raise the delivery count, then a final pull leaves the
	// messages on pending, not on the redelivery queue, so that the AckWait
	// expiry path rather than getNextMsg evaluates them.
	for i := 0; i < 2; i++ {
		msgs, err := sub.Fetch(10)
		require_NoError(t, err)
		require_Equal(t, len(msgs), 10)
		for _, m := range msgs {
			require_NoError(t, m.Nak())
		}
	}
	msgs, err := sub.Fetch(10)
	require_NoError(t, err)
	require_Equal(t, len(msgs), 10)

	// Lower MaxDeliver before the AckWait for the final delivery expires.
	_, err = js.UpdateConsumer("TEST", &nats.ConsumerConfig{
		Durable:       "ackwait",
		FilterSubject: "foo",
		AckPolicy:     nats.AckExplicitPolicy,
		MaxDeliver:    1,
		AckWait:       2 * time.Second,
		MaxAckPending: 10,
	})
	require_NoError(t, err)

	// Let the pending messages expire via checkPending -> hasMaxDeliveries.
	time.Sleep(2500 * time.Millisecond)
	requireAdvisoriesCount("ackwait", ackWaitAdv, 10, "before stepdown")

	// Make sure followers will have exact same state.
	_, err = nc.Request(fmt.Sprintf(JSApiConsumerLeaderStepDownT, "TEST", "ackwait"), nil, time.Second)
	require_NoError(t, err)
	c.waitOnConsumerLeader(globalAccountName, "TEST", "ackwait")

	// Give the new leader a settle window. setLeader calls checkPending
	// synchronously, but the apply goroutine may run a few ms later.
	time.Sleep(500 * time.Millisecond)
	requireAdvisoriesCount("ackwait", ackWaitAdv, 10, "after stepdown")
}

func TestJetStreamClusterConsumerDefaultsFromStream(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "R3S", 3)
	defer c.shutdown()

	nc, js := jsClientConnect(t, c.randomServer())
	defer nc.Close()

	streamTmpl := &StreamConfig{
		Name:     "test",
		Subjects: []string{"test.*"},
		Storage:  MemoryStorage,
		ConsumerLimits: StreamConsumerLimits{
			MaxAckPending:     0,
			InactiveThreshold: 0,
		},
	}

	// Since nats.go doesn't yet know about the consumer limits, craft
	// the stream configuration request by hand.
	streamCreate := func(maxAckPending int, inactiveThreshold time.Duration) (*StreamConfig, error) {
		cfg := streamTmpl
		cfg.ConsumerLimits = StreamConsumerLimits{
			MaxAckPending:     maxAckPending,
			InactiveThreshold: inactiveThreshold,
		}
		j, err := json.Marshal(cfg)
		if err != nil {
			return nil, err
		}
		msg, err := nc.Request(fmt.Sprintf(JSApiStreamCreateT, "test"), j, time.Second*3)
		if err != nil {
			return nil, err
		}
		var resp JSApiStreamCreateResponse
		if err := json.Unmarshal(msg.Data, &resp); err != nil {
			return nil, err
		}
		if resp.StreamInfo == nil {
			return nil, resp.ApiResponse.ToError()
		}
		return &resp.Config, resp.ApiResponse.ToError()
	}
	streamUpdate := func(maxAckPending int, inactiveThreshold time.Duration) (*StreamConfig, error) {
		cfg := streamTmpl
		cfg.ConsumerLimits = StreamConsumerLimits{
			MaxAckPending:     maxAckPending,
			InactiveThreshold: inactiveThreshold,
		}
		j, err := json.Marshal(cfg)
		if err != nil {
			return nil, err
		}
		msg, err := nc.Request(fmt.Sprintf(JSApiStreamUpdateT, "test"), j, time.Second*3)
		if err != nil {
			return nil, err
		}
		var resp JSApiStreamUpdateResponse
		if err := json.Unmarshal(msg.Data, &resp); err != nil {
			return nil, err
		}
		if resp.StreamInfo == nil {
			return nil, resp.ApiResponse.ToError()
		}
		return &resp.Config, resp.ApiResponse.ToError()
	}

	if _, err := streamCreate(15, time.Second); err != nil {
		t.Fatalf("Failed to add stream: %v", err)
	}

	t.Run("InheritDefaultsFromStream", func(t *testing.T) {
		ci, err := js.AddConsumer("test", &nats.ConsumerConfig{
			Name: "InheritDefaultsFromStream",
		})
		require_NoError(t, err)

		switch {
		case ci.Config.InactiveThreshold != time.Second:
			t.Fatalf("InactiveThreshold should be 1s, got %s", ci.Config.InactiveThreshold)
		case ci.Config.MaxAckPending != 15:
			t.Fatalf("MaxAckPending should be 15, got %d", ci.Config.MaxAckPending)
		}
	})

	t.Run("CreateConsumerErrorOnExceedMaxAckPending", func(t *testing.T) {
		_, err := js.AddConsumer("test", &nats.ConsumerConfig{
			Name:          "CreateConsumerErrorOnExceedMaxAckPending",
			MaxAckPending: 30,
		})
		switch e := err.(type) {
		case *nats.APIError:
			if ErrorIdentifier(e.ErrorCode) != JSConsumerMaxPendingAckExcessErrF {
				t.Fatalf("invalid error code, got %d, wanted %d", e.ErrorCode, JSConsumerMaxPendingAckExcessErrF)
			}
		default:
			t.Fatalf("should have returned API error, got %T", e)
		}
	})

	t.Run("CreateConsumerErrorOnExceedInactiveThreshold", func(t *testing.T) {
		_, err := js.AddConsumer("test", &nats.ConsumerConfig{
			Name:              "CreateConsumerErrorOnExceedInactiveThreshold",
			InactiveThreshold: time.Second * 2,
		})
		switch e := err.(type) {
		case *nats.APIError:
			if ErrorIdentifier(e.ErrorCode) != JSConsumerInactiveThresholdExcess {
				t.Fatalf("invalid error code, got %d, wanted %d", e.ErrorCode, JSConsumerInactiveThresholdExcess)
			}
		default:
			t.Fatalf("should have returned API error, got %T", e)
		}
	})

	t.Run("UpdateStreamErrorOnViolateConsumerMaxAckPending", func(t *testing.T) {
		_, err := js.AddConsumer("test", &nats.ConsumerConfig{
			Name:          "UpdateStreamErrorOnViolateConsumerMaxAckPending",
			MaxAckPending: 15,
		})
		require_NoError(t, err)

		if _, err = streamUpdate(10, 0); err == nil {
			t.Fatalf("stream update should have errored but didn't")
		}
	})

	t.Run("UpdateStreamErrorOnViolateConsumerInactiveThreshold", func(t *testing.T) {
		_, err := js.AddConsumer("test", &nats.ConsumerConfig{
			Name:              "UpdateStreamErrorOnViolateConsumerInactiveThreshold",
			InactiveThreshold: time.Second,
		})
		require_NoError(t, err)

		if _, err = streamUpdate(0, time.Second/2); err == nil {
			t.Fatalf("stream update should have errored but didn't")
		}
	})
}

func TestJetStreamClusterConsumerLimitsUpdateGatedAtMetaLeader(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "R3S", 3)
	defer c.shutdown()

	nc, js := jsClientConnect(t, c.randomServer())
	defer nc.Close()

	cfg := &StreamConfig{
		Name:     "test",
		Subjects: []string{"test.*"},
		Storage:  MemoryStorage,
		Replicas: 3,
		ConsumerLimits: StreamConsumerLimits{
			MaxAckPending:     20,
			InactiveThreshold: 10 * time.Second,
		},
	}
	_, err := jsStreamCreate(t, nc, cfg)
	require_NoError(t, err)

	// Consumer sits right at the current stream limit.
	_, err = js.AddConsumer("test", &nats.ConsumerConfig{
		Name:          "consumer",
		AckPolicy:     nats.AckExplicitPolicy,
		MaxAckPending: 20,
	})
	require_NoError(t, err)

	// Reads the consumer limits the meta leader currently has assigned.
	metaLeaderLimits := func() StreamConsumerLimits {
		t.Helper()
		var limits StreamConsumerLimits
		checkFor(t, 2*time.Second, 100*time.Millisecond, func() error {
			ml := c.leader()
			if ml == nil {
				return errors.New("no meta leader")
			}
			sjs := ml.getJetStream()
			sjs.mu.RLock()
			sa := sjs.streamAssignmentOrInflight(globalAccountName, "test")
			if sa == nil || sa.Config == nil {
				sjs.mu.RUnlock()
				return errors.New("no stream assignment")
			}
			limits = sa.Config.ConsumerLimits
			sjs.mu.RUnlock()
			return nil
		})
		return limits
	}

	// Sanity: the meta leader has the original limits assigned.
	require_Equal(t, metaLeaderLimits().MaxAckPending, 20)

	// Lowering MaxAckPending below the existing consumer must be rejected.
	badCfg := *cfg
	badCfg.ConsumerLimits.MaxAckPending = 10
	_, err = jsStreamUpdate(t, nc, &badCfg)
	require_Error(t, err, NewJSStreamUpdateError(fmt.Errorf("change to limits violates consumers: consumer")))

	// And crucially, the meta leader must not have committed the rejected config:
	// the assignment should still carry the original limit, not the rejected one.
	require_Equal(t, metaLeaderLimits().MaxAckPending, 20)

	// A valid update (raise the limit) should still succeed.
	goodCfg := *cfg
	goodCfg.ConsumerLimits.MaxAckPending = 30
	_, err = jsStreamUpdate(t, nc, &goodCfg)
	require_NoError(t, err)
	require_Equal(t, metaLeaderLimits().MaxAckPending, 30)

	// Clearing a limit to zero (unlimited) must not be treated as a violation.
	clearCfg := *cfg
	clearCfg.ConsumerLimits.MaxAckPending = 0
	_, err = jsStreamUpdate(t, nc, &clearCfg)
	require_NoError(t, err)
}

// Discovered that we are not properly setting certain default filestore blkSizes.
func TestJetStreamClusterCheckFileStoreBlkSizes(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "R3S", 3)
	defer c.shutdown()

	nc, js := jsClientConnect(t, c.randomServer())
	defer nc.Close()

	// Normal Stream
	_, err := js.AddStream(&nats.StreamConfig{
		Name:     "TEST",
		Subjects: []string{"*"},
		Replicas: 3,
	})
	require_NoError(t, err)

	_, err = js.AddConsumer("TEST", &nats.ConsumerConfig{
		Durable:   "C3",
		AckPolicy: nats.AckExplicitPolicy,
	})
	require_NoError(t, err)

	// KV
	_, err = js.CreateKeyValue(&nats.KeyValueConfig{
		Bucket:   "TEST",
		Replicas: 3,
	})
	require_NoError(t, err)

	blkSize := func(fs *fileStore) uint64 {
		fs.mu.RLock()
		defer fs.mu.RUnlock()
		return fs.fcfg.BlockSize
	}

	// We will check now the following filestores.
	//  meta
	//  TEST stream and NRG
	//  C3 NRG
	//  KV_TEST stream and NRG
	for _, s := range c.servers {
		js, cc := s.getJetStreamCluster()
		// META
		js.mu.RLock()
		meta := cc.meta
		js.mu.RUnlock()
		require_True(t, meta != nil)
		fs := meta.(*raft).wal.(*fileStore)
		require_True(t, blkSize(fs) == defaultMetaFSBlkSize)

		// TEST STREAM
		mset, err := s.GlobalAccount().lookupStream("TEST")
		require_NoError(t, err)
		mset.mu.RLock()
		fs = mset.store.(*fileStore)
		mset.mu.RUnlock()
		require_True(t, blkSize(fs) == defaultLargeBlockSize)

		// KV STREAM
		// Now the KV which is different default size.
		kv, err := s.GlobalAccount().lookupStream("KV_TEST")
		require_NoError(t, err)
		kv.mu.RLock()
		fs = kv.store.(*fileStore)
		kv.mu.RUnlock()
		require_True(t, blkSize(fs) == defaultKVBlockSize)

		// Now check NRGs
		// TEST Stream
		n := mset.raftNode()
		require_True(t, n != nil)
		fs = n.(*raft).wal.(*fileStore)
		require_True(t, blkSize(fs) == defaultMediumBlockSize)
		// KV TEST Stream
		n = kv.raftNode()
		require_True(t, n != nil)
		fs = n.(*raft).wal.(*fileStore)
		require_True(t, blkSize(fs) == defaultMediumBlockSize)
		// Consumer
		o := mset.lookupConsumer("C3")
		require_True(t, o != nil)
		n = o.raftNode()
		require_True(t, n != nil)
		fs = n.(*raft).wal.(*fileStore)
		require_True(t, blkSize(fs) == defaultMediumBlockSize)
	}
}

func TestJetStreamClusterDetectOrphanNRGs(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "R3S", 3)
	defer c.shutdown()

	nc, js := jsClientConnect(t, c.randomServer())
	defer nc.Close()

	// Normal Stream
	_, err := js.AddStream(&nats.StreamConfig{
		Name:     "TEST",
		Subjects: []string{"*"},
		Replicas: 3,
	})
	require_NoError(t, err)

	_, err = js.AddConsumer("TEST", &nats.ConsumerConfig{
		Durable:   "DC",
		AckPolicy: nats.AckExplicitPolicy,
	})
	require_NoError(t, err)

	// We will force an orphan for a certain server.
	s := c.randomNonStreamLeader(globalAccountName, "TEST")

	mset, err := s.GlobalAccount().lookupStream("TEST")
	require_NoError(t, err)
	sgn := mset.raftNode().Group()

	o := mset.lookupConsumer("DC")
	require_True(t, o != nil)
	ogn := o.raftNode().Group()

	require_NoError(t, js.DeleteStream("TEST"))

	// Should only be meta NRG left.
	checkFor(t, 2*time.Second, 500*time.Millisecond, func() error {
		if rns := s.numRaftNodes(); rns != 1 {
			return fmt.Errorf("expected only 1 (meta) RAFT node, got: %d", rns)
		}
		return nil
	})

	s.rnMu.RLock()
	defer s.rnMu.RUnlock()
	require_True(t, s.lookupRaftNode(sgn) == nil)
	require_True(t, s.lookupRaftNode(ogn) == nil)
}

// https://github.com/nats-io/nats-server/issues/4732
func TestJetStreamClusterStreamLimitsOnScaleUpAndMove(t *testing.T) {
	tmpl := `
			listen: 127.0.0.1:-1
			server_name: %s
			jetstream: {max_mem_store: 256MB, max_file_store: 2GB, store_dir: '%s'}
			cluster {
				name: %s
				listen: 127.0.0.1:%d
				routes = [%s]
			}
	`
	opFrag := `
			operator: %s
			system_account: %s
			resolver: { type: MEM }
			resolver_preload = {
				%s : %s
				%s : %s
			}
		`

	_, syspub := createKey(t)
	sysJwt := encodeClaim(t, jwt.NewAccountClaims(syspub), syspub)

	accKp, aExpPub := createKey(t)
	accClaim := jwt.NewAccountClaims(aExpPub)
	accClaim.Limits.JetStreamTieredLimits["R1"] = jwt.JetStreamLimits{
		DiskStorage: -1, Consumer: -1, Streams: 1}
	accClaim.Limits.JetStreamTieredLimits["R3"] = jwt.JetStreamLimits{
		DiskStorage: 0, Consumer: -1, Streams: 1}
	accJwt := encodeClaim(t, accClaim, aExpPub)
	accCreds := newUser(t, accKp)

	template := tmpl + fmt.Sprintf(opFrag, ojwt, syspub, syspub, sysJwt, aExpPub, accJwt)

	c := createJetStreamCluster(t, template, "CLOUD", _EMPTY_, 3, 22020, true)
	defer c.shutdown()

	nc, js := jsClientConnect(t, c.randomServer(), nats.UserCredentials(accCreds))
	defer nc.Close()

	// Prevent 'nats: JetStream not enabled for account' when creating the first stream.
	c.waitOnAccount(aExpPub)

	_, err := js.AddStream(&nats.StreamConfig{
		Name:     "TEST",
		Subjects: []string{"foo"},
	})
	require_NoError(t, err)

	toSend, msg := 100, bytes.Repeat([]byte("Z"), 1024)
	for i := 0; i < toSend; i++ {
		_, err := js.PublishAsync("foo", msg)
		require_NoError(t, err)
	}
	select {
	case <-js.PublishAsyncComplete():
	case <-time.After(5 * time.Second):
		t.Fatalf("Did not receive completion signal")
	}

	// Scale up should fail here since no R3 storage.
	_, err = js.UpdateStream(&nats.StreamConfig{
		Name:     "TEST",
		Subjects: []string{"foo"},
		Replicas: 3,
	})
	require_Error(t, err, errors.New("insufficient storage resources"))
}

func TestJetStreamClusterAPIAccessViaSystemAccount(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "R3S", 3)
	defer c.shutdown()

	// Connect to system account.
	nc, js := jsClientConnect(t, c.randomServer(), nats.UserInfo("admin", "s3cr3t!"))
	defer nc.Close()

	_, err := js.AddStream(&nats.StreamConfig{Name: "TEST"})
	require_Error(t, err, NewJSNotEnabledForAccountError())

	// Make sure same behavior swith single server.
	tmpl := `
		listen: 127.0.0.1:-1
		jetstream: {max_mem_store: 256MB, max_file_store: 2GB, store_dir: '%s'}
		accounts { $SYS { users = [ { user: "admin", pass: "s3cr3t!" } ] } }
	`
	conf := createConfFile(t, []byte(fmt.Sprintf(tmpl, t.TempDir())))
	s, _ := RunServerWithConfig(conf)
	defer s.Shutdown()

	nc, js = jsClientConnect(t, s, nats.UserInfo("admin", "s3cr3t!"))
	defer nc.Close()

	_, err = js.AddStream(&nats.StreamConfig{Name: "TEST"})
	require_Error(t, err, NewJSNotEnabledForAccountError())
}

func TestJetStreamClusterStreamResetPreacks(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "R3S", 3)
	defer c.shutdown()

	nc, js := jsClientConnect(t, c.randomServer())
	defer nc.Close()

	_, err := js.AddStream(&nats.StreamConfig{
		Name:      "TEST",
		Subjects:  []string{"foo"},
		Retention: nats.InterestPolicy,
		Replicas:  3,
	})
	require_NoError(t, err)

	err = js.PurgeStream("TEST", &nats.StreamPurgeRequest{Sequence: 100_000_000})
	require_NoError(t, err)

	sub, err := js.PullSubscribe("foo", "dlc")
	require_NoError(t, err)

	// Put 20 msgs in.
	for i := 0; i < 20; i++ {
		_, err := js.Publish("foo", nil)
		require_NoError(t, err)
	}

	// Consume and ack 10.
	msgs, err := sub.Fetch(10, nats.MaxWait(time.Second))
	require_NoError(t, err)
	require_Equal(t, len(msgs), 10)

	for _, msg := range msgs {
		msg.AckSync()
	}

	// Now grab a non-leader server.
	// We will shut it down and remove the stream data.
	nl := c.randomNonStreamLeader(globalAccountName, "TEST")
	mset, err := nl.GlobalAccount().lookupStream("TEST")
	require_NoError(t, err)
	fs := mset.store.(*fileStore)
	mdir := filepath.Join(fs.fcfg.StoreDir, msgDir)
	nl.Shutdown()
	// In case that was the consumer leader.
	c.waitOnConsumerLeader(globalAccountName, "TEST", "dlc")

	// Now consume the remaining 10 and ack.
	msgs, err = sub.Fetch(10, nats.MaxWait(10*time.Second))
	require_NoError(t, err)
	require_Equal(t, len(msgs), 10)

	for _, msg := range msgs {
		msg.AckSync()
	}

	// Now remove the stream manually.
	require_NoError(t, os.RemoveAll(mdir))
	nl = c.restartServer(nl)
	c.waitOnAllCurrent()

	mset, err = nl.GlobalAccount().lookupStream("TEST")
	require_NoError(t, err)

	checkFor(t, 10*time.Second, 200*time.Millisecond, func() error {
		state := mset.state()
		if state.Msgs != 0 || state.FirstSeq != 100_000_020 {
			return fmt.Errorf("Not correct state yet: %+v", state)
		}
		return nil
	})
}

func TestJetStreamClusterDomainAdvisory(t *testing.T) {
	tmpl := strings.Replace(jsClusterAccountsTempl, "store_dir:", "domain: NGS, store_dir:", 1)
	c := createJetStreamCluster(t, tmpl, "R3S", _EMPTY_, 3, 18033, true)
	defer c.shutdown()

	// Connect to system account.
	nc, _ := jsClientConnect(t, c.randomServer(), nats.UserInfo("admin", "s3cr3t!"))
	defer nc.Close()

	sub, err := nc.SubscribeSync(JSAdvisoryDomainLeaderElected)
	require_NoError(t, err)

	// Ask meta leader to move and make sure we get an advisory.
	nc.Request(JSApiLeaderStepDown, nil, time.Second)
	c.waitOnLeader()

	checkSubsPending(t, sub, 1)

	m, err := sub.NextMsg(time.Second)
	require_NoError(t, err)

	var adv JSDomainLeaderElectedAdvisory
	require_NoError(t, json.Unmarshal(m.Data, &adv))

	ml := c.leader()
	js, cc := ml.getJetStreamCluster()
	js.mu.RLock()
	peer := cc.meta.ID()
	js.mu.RUnlock()

	require_Equal(t, adv.Leader, peer)
	require_Equal(t, adv.Domain, "NGS")
	require_Equal(t, adv.Cluster, "R3S")
	require_Equal(t, len(adv.Replicas), 3)
}

func TestJetStreamClusterLimitsBasedStreamFileStoreDesync(t *testing.T) {
	conf := `
	listen: 127.0.0.1:-1
	server_name: %s
	jetstream: {
		store_dir: '%s',
	}
	cluster {
		name: %s
		listen: 127.0.0.1:%d
		routes = [%s]
	}
        system_account: sys
        no_auth_user: js
	accounts {
	  sys {
	    users = [
	      { user: sys, pass: sys }
	    ]
	  }
	  js {
	    jetstream = { max_store = 3mb }
	    users = [
	      { user: js, pass: js }
	    ]
	  }
	}`
	c := createJetStreamClusterWithTemplate(t, conf, "limits", 3)
	defer c.shutdown()

	nc, js := jsClientConnect(t, c.randomServer())
	defer nc.Close()

	cnc, cjs := jsClientConnect(t, c.randomServer())
	defer cnc.Close()

	_, err := js.AddStream(&nats.StreamConfig{
		Name:     "LTEST",
		Subjects: []string{"messages.*"},
		Replicas: 3,
		MaxAge:   10 * time.Minute,
		MaxMsgs:  100_000,
	})
	require_NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	psub, err := cjs.PullSubscribe("messages.*", "consumer")
	require_NoError(t, err)

	var (
		wg          sync.WaitGroup
		received    uint64
		errCh       = make(chan error, 100_000)
		receivedMap = make(map[string]*nats.Msg)
	)
	wg.Add(1)
	go func() {
		tick := time.NewTicker(20 * time.Millisecond)
		for {
			select {
			case <-ctx.Done():
				wg.Done()
				return
			case <-tick.C:
				msgs, err := psub.Fetch(10, nats.MaxWait(200*time.Millisecond))
				if err != nil {
					continue
				}
				for _, msg := range msgs {
					received++
					receivedMap[msg.Subject] = msg
					if meta, _ := msg.Metadata(); meta.NumDelivered > 1 {
						t.Logf("GOT MSG: %s :: %+v :: %d", msg.Subject, meta, len(msg.Data))
					}
					msg.Ack()
				}
			}
		}
	}()

	// Send 20_000 msgs at roughly 1 msg per msec
	shouldDrop := make(map[string]error)
	wg.Add(1)
	go func() {
		payload := []byte(strings.Repeat("A", 1024))
		tick := time.NewTicker(1 * time.Millisecond)
		for i := 1; i < 100_000; {
			select {
			case <-ctx.Done():
				wg.Done()
				return
			case <-tick.C:
				// This should run into 3MB quota and get errors right away
				// before the max msgs limit does.
				subject := fmt.Sprintf("messages.%d", i)
				_, err := js.Publish(subject, payload, nats.RetryAttempts(0))
				if err != nil {
					errCh <- err
				}
				i++

				// Any message over this number should not be a success
				// since the stream should be full due to the quota.
				// Here we capture that the messages have failed to confirm.
				if err != nil && i > 1000 {
					shouldDrop[subject] = err
				}
			}
		}
	}()

	// Collect enough errors to cause things to get out of sync.
	var errCount int
Setup:
	for {
		select {
		case <-errCh:
			errCount++
			if errCount >= 20_000 {
				// Stop both producing and consuming.
				cancel()
				break Setup
			}
		case <-time.After(5 * time.Second):
			t.Fatalf("Timed out waiting for limits error")
		}
	}

	// Both goroutines should be exiting now..
	wg.Wait()

	// Check messages that ought to have been dropped.
	for subject := range receivedMap {
		found, ok := shouldDrop[subject]
		if ok {
			t.Errorf("Should have dropped message published on %q since got error: %v", subject, found)
		}
	}

	getStreamDetails := func(t *testing.T, srv *Server) *StreamDetail {
		t.Helper()
		jsz, err := srv.Jsz(&JSzOptions{Accounts: true, Streams: true, Consumer: true})
		require_NoError(t, err)
		if len(jsz.AccountDetails) > 0 && len(jsz.AccountDetails[0].Streams) > 0 {
			details := jsz.AccountDetails[0]
			stream := details.Streams[0]
			return &stream
		}
		t.Error("Could not find account details")
		return nil
	}
	checkState := func(t *testing.T) error {
		t.Helper()

		leaderSrv := c.streamLeader("js", "LTEST")
		streamLeader := getStreamDetails(t, leaderSrv)
		// t.Logf("Stream Leader: %+v", streamLeader.State)
		errs := make([]error, 0)
		for _, srv := range c.servers {
			if srv == leaderSrv {
				// Skip self
				continue
			}
			stream := getStreamDetails(t, srv)
			if stream.State.Msgs != streamLeader.State.Msgs {
				err := fmt.Errorf("Leader %v has %d messages, Follower %v has %d messages",
					stream.Cluster.Leader, streamLeader.State.Msgs,
					srv.Name(), stream.State.Msgs,
				)
				errs = append(errs, err)
			}
		}
		if len(errs) > 0 {
			return errors.Join(errs...)
		}
		return nil
	}

	// Confirm state of the leader.
	leaderSrv := c.streamLeader("js", "LTEST")
	streamLeader := getStreamDetails(t, leaderSrv)
	if streamLeader.State.Msgs != received {
		t.Errorf("Leader %v has %d messages stored but %d messages were received (delta: %d)",
			leaderSrv.Name(), streamLeader.State.Msgs, received, received-streamLeader.State.Msgs)
	}
	cinfo, err := psub.ConsumerInfo()
	require_NoError(t, err)
	if received != cinfo.Delivered.Consumer {
		t.Errorf("Unexpected consumer sequence. Got: %v, expected: %v",
			cinfo.Delivered.Consumer, received)
	}

	// Check whether there was a drift among the leader and followers.
	var (
		lastErr  error
		attempts int
	)
Check:
	for range time.NewTicker(1 * time.Second).C {
		lastErr = checkState(t)
		if attempts > 5 {
			break Check
		}
		attempts++
	}

	// Read the stream
	psub2, err := cjs.PullSubscribe("messages.*", "")
	require_NoError(t, err)

Consume2:
	for {
		msgs, err := psub2.Fetch(100)
		if err != nil {
			continue
		}
		for _, msg := range msgs {
			msg.Ack()

			meta, _ := msg.Metadata()
			if meta.NumPending == 0 {
				break Consume2
			}
		}
	}

	cinfo2, err := psub2.ConsumerInfo()
	require_NoError(t, err)

	a := cinfo.Delivered.Consumer
	b := cinfo2.Delivered.Consumer
	if a != b {
		t.Errorf("Consumers to same stream are at different sequences: %d vs %d", a, b)
	}

	// Test is done but replicas were in sync so can stop testing at this point.
	if lastErr == nil {
		return
	}

	// Now we will cause a few step downs while out of sync to get different results.
	t.Errorf("Replicas are out of sync:\n%v", lastErr)

	stepDown := func() {
		_, err = nc.Request(fmt.Sprintf(JSApiStreamLeaderStepDownT, "LTEST"), nil, time.Second)
	}
	// Check StreamInfo in this state then trigger a few step downs.
	var prevLeaderMsgs uint64
	leaderSrv = c.streamLeader("js", "LTEST")
	sinfo, err := js.StreamInfo("LTEST")
	prevLeaderMsgs = sinfo.State.Msgs
	for i := 0; i < 10; i++ {
		stepDown()
		time.Sleep(2 * time.Second)

		leaderSrv = c.streamLeader("js", "LTEST")
		sinfo, err = js.StreamInfo("LTEST")
		if err != nil {
			t.Logf("Error: %v", err)
			continue
		}
		if leaderSrv != nil && sinfo != nil {
			t.Logf("When leader is %v, Messages: %d", leaderSrv.Name(), sinfo.State.Msgs)

			// Leave the leader as the replica with less messages that was out of sync.
			if prevLeaderMsgs > sinfo.State.Msgs {
				break
			}
		}
	}
	t.Logf("Changed to use leader %v which has %d messages", leaderSrv.Name(), sinfo.State.Msgs)

	// Read the stream again
	psub3, err := cjs.PullSubscribe("messages.*", "")
	require_NoError(t, err)

Consume3:
	for {
		msgs, err := psub3.Fetch(100)
		if err != nil {
			continue
		}
		for _, msg := range msgs {
			msg.Ack()

			meta, _ := msg.Metadata()
			if meta.NumPending == 0 {
				break Consume3
			}
		}
	}

	cinfo3, err := psub3.ConsumerInfo()
	require_NoError(t, err)

	// Compare against consumer that was created before resource limits error
	// with one created before the step down.
	a = cinfo.Delivered.Consumer
	b = cinfo2.Delivered.Consumer
	if a != b {
		t.Errorf("Consumers to same stream are at different sequences: %d vs %d", a, b)
	}

	// Compare against consumer that was created before resource limits error
	// with one created AFTER the step down.
	a = cinfo.Delivered.Consumer
	b = cinfo3.Delivered.Consumer
	if a != b {
		t.Errorf("Consumers to same stream are at different sequences: %d vs %d", a, b)
	}

	// Compare consumers created after the resource limits error.
	a = cinfo2.Delivered.Consumer
	b = cinfo3.Delivered.Consumer
	if a != b {
		t.Errorf("Consumers to same stream are at different sequences: %d vs %d", a, b)
	}
}

func TestJetStreamClusterAccountFileStoreLimits(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "limits", 3)
	defer c.shutdown()

	limits := map[string]JetStreamAccountLimits{
		"R1": {
			MaxMemory:    1 << 10,
			MaxStore:     1 << 10,
			MaxStreams:   -1,
			MaxConsumers: -1,
		},
		"R3": {
			MaxMemory:    1 << 10,
			MaxStore:     1 << 10,
			MaxStreams:   -1,
			MaxConsumers: -1,
		},
	}

	// Update the limits in all servers.
	for _, s := range c.servers {
		acc := s.GlobalAccount()
		if err := acc.UpdateJetStreamLimits(limits); err != nil {
			t.Fatalf("Unexpected error updating jetstream account limits: %v", err)
		}
	}
	nc, js := jsClientConnect(t, c.randomServer())
	defer nc.Close()

	for _, replicas := range []int64{1, 3} {
		sname := fmt.Sprintf("test-stream:%d", replicas)
		t.Run(sname, func(t *testing.T) {
			sconfig := &nats.StreamConfig{
				Name:      sname,
				Replicas:  int(replicas),
				Storage:   nats.FileStorage,
				Retention: nats.LimitsPolicy,
			}
			_, err := js.AddStream(sconfig)
			if err != nil {
				t.Fatalf("Unexpected error creating stream: %v", err)
			}

			data := []byte(strings.Repeat("A", 1<<8))
			for i := 0; i < 30; i++ {
				if _, err = js.Publish(sname, data); err != nil && !strings.Contains(err.Error(), "resource limits exceeded for account") {
					t.Errorf("Error publishing random data (iteration %d): %v", i, err)
				}

				if err = nc.Flush(); err != nil {
					t.Fatalf("Unexpected error flushing connection: %v", err)
				}

				_, err = js.StreamInfo(sname)
				require_NoError(t, err)
			}

			si, err := js.StreamInfo(sname)
			require_NoError(t, err)
			st := si.State
			maxStore := limits[fmt.Sprintf("R%d", replicas)].MaxStore
			if int64(st.Bytes) > replicas*maxStore {
				t.Errorf("Unexpected size of stream: got %d, expected less than %d\nstate: %#v", st.Bytes, maxStore, st)
			}
		})
	}
}

func TestJetStreamClusterScaleToMissingTier(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "R3S", 3)
	defer c.shutdown()

	// Only R1 and R3 tiers are defined, so R2 must not be allowed.
	limits := map[string]JetStreamAccountLimits{
		"R1": {MaxMemory: -1, MaxStore: -1, MaxStreams: -1, MaxConsumers: -1},
		"R3": {MaxMemory: -1, MaxStore: -1, MaxStreams: -1, MaxConsumers: -1},
	}
	for _, s := range c.servers {
		require_NoError(t, s.globalAccount().UpdateJetStreamLimits(limits))
	}

	nc, js := jsClientConnect(t, c.randomServer())
	defer nc.Close()

	// Scaling up into a missing tier is rejected.
	_, err := js.AddStream(&nats.StreamConfig{Name: "UP", Subjects: []string{"up"}, Replicas: 1})
	require_NoError(t, err)
	_, err = js.UpdateStream(&nats.StreamConfig{Name: "UP", Subjects: []string{"up"}, Replicas: 2})
	require_Error(t, err, NewJSNoLimitsError())

	// Scaling down into a missing tier must be rejected just the same.
	_, err = js.AddStream(&nats.StreamConfig{Name: "DOWN", Subjects: []string{"down"}, Replicas: 3})
	require_NoError(t, err)
	_, err = js.UpdateStream(&nats.StreamConfig{Name: "DOWN", Subjects: []string{"down"}, Replicas: 2})
	require_Error(t, err, NewJSNoLimitsError())
}

func TestJetStreamClusterTieredReservationConsistency(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "R3S", 3)
	defer c.shutdown()

	nc, cjs := jsClientConnect(t, c.randomServer())
	defer nc.Close()

	for _, replicas := range []int{1, 3} {
		subj := fmt.Sprintf("R%d", replicas)
		maxBytes := int64(1)
		if replicas > 1 {
			maxBytes = 10
		}
		_, err := cjs.AddStream(&nats.StreamConfig{
			Name:      subj,
			Replicas:  replicas,
			Storage:   nats.FileStorage,
			Retention: nats.LimitsPolicy,
			MaxBytes:  maxBytes,
		})
		require_NoError(t, err)
	}
	checkFor(t, 2*time.Second, 200*time.Millisecond, func() error {
		return checkState(t, c, globalAccountName, "R3")
	})

	sl := c.streamLeader(globalAccountName, "R1")
	_, js, jsa := sl.globalAccount().getJetStreamFromAccount()

	js.mu.RLock()
	defer js.mu.RUnlock()
	jsa.mu.RLock()
	defer jsa.mu.RUnlock()

	cfg := &StreamConfig{Storage: FileStorage}

	// No tier, R1: 1, R3: 10*R
	tier := _EMPTY_
	require_Equal(t, jsa.tieredReservation(tier, cfg), 31)
	streams, reservation := js.tieredStreamAndReservationCount(globalAccountName, tier, cfg)
	require_Equal(t, streams, 2)
	require_Equal(t, reservation, 31)

	// R1 tier, R1: 1
	tier, cfg.Replicas = "R1", 1
	require_Equal(t, jsa.tieredReservation(tier, cfg), 1)
	streams, reservation = js.tieredStreamAndReservationCount(globalAccountName, tier, cfg)
	require_Equal(t, streams, 1)
	require_Equal(t, reservation, 1)

	// R3 tier, R3: 10
	tier, cfg.Replicas = "R3", 3
	require_Equal(t, jsa.tieredReservation(tier, cfg), 10)
	streams, reservation = js.tieredStreamAndReservationCount(globalAccountName, tier, cfg)
	require_Equal(t, streams, 1)
	require_Equal(t, reservation, 10)
}

func TestJetStreamClusterTieredReservationOverflow(t *testing.T) {
	tmpl := strings.Replace(jsClusterMaxBytesAccountLimitTempl, "max_file_store: 4GB", "max_file_store: 9223372036854775807", 1)
	tmpl = strings.Replace(tmpl, "max_file:  3GB", "max_file:  9223372036854775807", 1)
	c := createJetStreamClusterWithTemplate(t, tmpl, "R3S", 3)
	defer c.shutdown()

	nc, js := jsClientConnect(t, c.randomServer())
	defer nc.Close()

	// Create a stream with R=3 and MaxBytes large enough that
	// Replicas * MaxBytes overflows int64:
	//   3 * 4e18 = 12e18 > MaxInt64 (9.22e18)
	// The account limit is MaxInt64, so the first stream is accepted: its
	// reservation saturates to MaxInt64 rather than wrapping negative.
	_, err := js.AddStream(&nats.StreamConfig{
		Name:     "S1",
		Subjects: []string{"s1"},
		MaxBytes: 4_000_000_000_000_000_000, // 4e18
		Replicas: 3,
	})
	require_NoError(t, err)

	// The true reservation for S1 is 3 * 4e18 = 12e18, which saturates to
	// MaxInt64 and consumes the whole account. Creating any additional
	// stream should be rejected. With the overflow bug, the reservation
	// computation wraps to a negative value, making the account appear
	// to have plenty of room.
	_, err = js.AddStream(&nats.StreamConfig{
		Name:     "S2",
		Subjects: []string{"s2"},
		MaxBytes: 1,
		Replicas: 3,
	})
	require_Error(t, err, errors.New("nats: insufficient storage resources available"))
}

func TestJetStreamClusterCorruptMetaSnapshot(t *testing.T) {
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
	nc.Close()

	// Restart the server so it generates a snapshot.
	s := c.randomServer()
	s.Shutdown()
	s.WaitForShutdown()
	s = c.restartServer(s)
	require_False(t, s.isShuttingDown())

	// Perform a couple leader elections to add a couple more entries to the Raft log.
	// Once we corrupt the snapshot, we shouldn't recover with only the log.
	for range 2 {
		c.waitOnLeader()
		ml := c.leader()
		require_NotNil(t, ml)
		meta := ml.getJetStream().getMetaGroup()
		require_NoError(t, meta.StepDown())
	}

	// Stop the meta group of our selected server early, making sure it can't generate a new snapshot.
	meta := s.getJetStream().getMetaGroup().(*raft)
	meta.Stop()
	meta.WaitForStop()

	meta.RLock()
	snapfile := meta.snapfile
	meta.RUnlock()
	configFile := s.getOpts().ConfigFile
	s.Shutdown()
	s.WaitForShutdown()

	// Truncate/corrupt the snapshot.
	require_NoError(t, os.Truncate(snapfile, 0))

	// The server should not start up.
	opts := LoadConfig(configFile)
	s, err = NewServer(opts)
	require_NoError(t, err)
	s.Start()
	require_Equal(t, s.numRaftNodes(), 0)
}

func TestJetStreamClusterProcessSnapshotPanicAfterStreamDelete(t *testing.T) {
	s := RunBasicJetStreamServer(t)
	defer s.Shutdown()

	nc, js := jsClientConnect(t, s)
	defer nc.Close()

	_, err := js.AddStream(&nats.StreamConfig{Name: "TEST"})
	require_NoError(t, err)

	mset, err := s.globalAccount().lookupStream("TEST")
	require_NoError(t, err)
	mset.mu.RLock()
	sa, node := mset.sa, mset.node
	mset.mu.RUnlock()
	require_True(t, sa == nil)
	require_True(t, node == nil)
	require_Error(t, mset.processSnapshot(&StreamReplicatedState{}, 0), errCatchupStreamStopped)

	mset.setStreamAssignment(&streamAssignment{}) // If the stream assignment is set, but the node is nil.
	mset.mu.RLock()
	sa, node = mset.sa, mset.node
	mset.mu.RUnlock()
	require_True(t, sa != nil)
	require_True(t, node == nil)
	require_Error(t, mset.processSnapshot(&StreamReplicatedState{}, 0), errCatchupStreamStopped)
}

func TestJetStreamClusterProcessSnapshotWhenLimitsExceeded(t *testing.T) {
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
	mset, err := sl.globalAccount().lookupStream("TEST")
	require_NoError(t, err)

	// Force this server to believe it has exceeded its storage limits.
	sjs := sl.getJetStream()
	atomic.StoreInt64(&sjs.storeMax, -1)
	require_True(t, sjs.limitsExceeded(FileStorage))

	// The snapshot should bail with insufficient resources. As otherwise
	// the snapshot would be incorrectly marked successful.
	snap := StreamReplicatedState{FirstSeq: 1, LastSeq: 100}
	err = mset.processSnapshot(&snap, 100)
	require_Error(t, err, NewJSInsufficientResourcesError())
	require_False(t, isClusterResetErr(err))
	require_False(t, isOutOfSpaceErr(err))
}

func TestJetStreamClusterDiscardNewPerSubjectRejectsWithoutCLFSBump(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "R3S", 3)
	defer c.shutdown()

	nc, js := jsClientConnect(t, c.randomServer())
	defer nc.Close()

	_, err := js.AddStream(&nats.StreamConfig{
		Name:                 "TEST",
		Subjects:             []string{"foo"},
		Replicas:             3,
		Discard:              nats.DiscardNew,
		DiscardNewPerSubject: true,
		MaxMsgsPerSubject:    1,
	})
	require_NoError(t, err)

	// First publish should succeed.
	pubAck, err := js.Publish("foo", nil)
	require_NoError(t, err)
	require_Equal(t, pubAck.Sequence, 1)

	// The second should fail, since the limit is hit.
	_, err = js.Publish("foo", nil)
	require_Error(t, err, ErrMaxMsgsPerSubject)

	// Retry after deleting, it should succeed afterward.
	require_NoError(t, js.DeleteMsg("TEST", 1))
	pubAck, err = js.Publish("foo", nil)
	require_NoError(t, err)
	require_Equal(t, pubAck.Sequence, 2)
	checkFor(t, 2*time.Second, 200*time.Millisecond, func() error {
		return checkState(t, c, globalAccountName, "TEST")
	})

	// CLFS should NOT be bumped.
	for _, s := range c.servers {
		mset, err := s.globalAccount().lookupStream("TEST")
		require_NoError(t, err)
		require_Equal(t, mset.getCLFS(), 0)
	}
}

func TestJetStreamClusterStreamDesyncDuringSnapshot(t *testing.T) {
	const (
		KindRemoveMsg = iota
		KindReset
		KindTruncate
	)
	test := func(t *testing.T, kind int) {
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

		pubAck, err := js.Publish("foo", nil)
		require_NoError(t, err)
		require_Equal(t, pubAck.Sequence, 1)
		pubAck, err = js.Publish("foo", nil)
		require_NoError(t, err)
		require_Equal(t, pubAck.Sequence, 2)
		checkFor(t, 2*time.Second, 200*time.Millisecond, func() error {
			return checkState(t, c, globalAccountName, "TEST")
		})

		rs := c.randomNonStreamLeader(globalAccountName, "TEST")
		mset, err := rs.globalAccount().lookupStream("TEST")
		require_NoError(t, err)
		fs := mset.store.(*fileStore)
		fs.mu.Lock()
		fs.sips++
		fs.mu.Unlock()

		switch kind {
		case KindRemoveMsg:
			require_NoError(t, js.DeleteMsg("TEST", 1))
		case KindReset:
			for _, s := range c.servers {
				mset, err = s.globalAccount().lookupStream("TEST")
				require_NoError(t, err)
				require_NoError(t, mset.store.Truncate(0))
			}
		case KindTruncate:
			for _, s := range c.servers {
				mset, err = s.globalAccount().lookupStream("TEST")
				require_NoError(t, err)
				require_NoError(t, mset.store.Truncate(1))
			}
		}

		checkFor(t, 2*time.Second, 200*time.Millisecond, func() error {
			return checkState(t, c, globalAccountName, "TEST")
		})
	}

	t.Run("RemoveMsg", func(t *testing.T) { test(t, KindRemoveMsg) })
	t.Run("Reset", func(t *testing.T) { test(t, KindReset) })
	t.Run("Truncate", func(t *testing.T) { test(t, KindTruncate) })
}

func TestJetStreamClusterDeletedNodeDoesNotReviveStreamAfterCatchup(t *testing.T) {
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

	_, err = js.Publish("foo", nil)
	require_NoError(t, err)
	checkFor(t, 2*time.Second, 200*time.Millisecond, func() error {
		return checkState(t, c, globalAccountName, "TEST")
	})

	rs := c.randomNonStreamLeader(globalAccountName, "TEST")
	for _, s := range c.servers {
		if s == rs {
			continue
		}
		s.Shutdown()
		s.WaitForShutdown()
	}

	mset, err := rs.globalAccount().lookupStream("TEST")
	require_NoError(t, err)
	snap := mset.stateSnapshot()

	// Reset the entire store so we can catchup based on the above snapshot.
	fs := mset.store.(*fileStore)
	require_NoError(t, fs.reset())

	// Mark the node as leaderless, and get the upper-layer to start a catchup from a snapshot.
	node := mset.raftNode()
	node.(*raft).hasleader.Store(false)
	node.ApplyQ().push(newCommittedEntry(10, []*Entry{{EntrySnapshot, snap}}))

	// Since the node is leaderless, it will retry after some time. We wait a little here to ensure
	// it's waiting there as well, and then we delete the node outright.
	time.Sleep(time.Second)
	node.Delete()

	// The stream's goroutine should eventually be stopped. This will fail if the stream is revived.
	var retries int
	checkFor(t, 10*time.Second, 200*time.Millisecond, func() error {
		mset, err = rs.globalAccount().lookupStream("TEST")
		if err != nil {
			retries = 0
			return err
		}
		if mset.isMonitorRunning() {
			retries = 0
			return errors.New("monitor still running")
		}
		if state := mset.raftNode().State(); state != Closed {
			retries = 0
			return errors.New("node not closed")
		}
		retries++
		if retries < 3 {
			return errors.New("still confirming stable state")
		}
		return nil
	})
}

// https://github.com/nats-io/nats-server/issues/7718
func TestJetStreamClusterLeakedSubsWithStreamImportOverlappingJetStreamSubs(t *testing.T) {
	tmpl := `
	listen: 127.0.0.1:-1
	server_name: %s
	jetstream: {max_mem_store: 2GB, max_file_store: 2GB, store_dir: '%s'}

	leaf {
		listen: 127.0.0.1:-1
	}

	cluster {
		name: %s
		listen: 127.0.0.1:%d
		routes = [%s]
	}

	accounts {
	  ACC: {
		jetstream: enabled
		users: [{user: acc, password: acc}]
		imports: [{stream: {account: zone, subject: ">"}}]
	  }
	  zone: {
		jetstream: enabled
		users: [{user: zone, password: zone}]
		exports: [{stream: ">"}]
	  }
	}
	no_auth_user: acc
`
	c := createJetStreamClusterWithTemplate(t, tmpl, "R3S", 3)
	defer c.shutdown()

	nc, js := jsClientConnect(t, c.randomServer())
	defer nc.Close()

	checkExpectedSubs := func(expected uint32) (actual uint32) {
		t.Helper()
		checkFor(t, 2*time.Second, 100*time.Millisecond, func() error {
			e := expected
			for _, s := range c.servers {
				subs := s.NumSubscriptions()
				if e == 0 {
					e = subs
				} else if e != subs {
					return fmt.Errorf("expected %d subs, got %d", e, subs)
				}
			}
			actual = e
			return nil
		})
		return actual
	}

	// Track subscription counts between stream/consumer create/deletes.
	var baseline, sc, cc uint32

	// Perform a couple iterations to check we get to predictable subscription counts.
	for range 3 {
		// Zero means we don't know the expected count, but still ALL servers must equal.
		initial := checkExpectedSubs(0)

		// If we've iterated once, we'll know the baseline. Each next iteration must be equal to this.
		if baseline != 0 {
			require_Equal(t, baseline, initial)
		}

		// Add the stream.
		_, err := js.AddStream(&nats.StreamConfig{
			Name:     "TEST",
			Subjects: []string{"foo"},
			Replicas: 1,
			Storage:  nats.FileStorage,
		})
		require_NoError(t, err)
		sl := c.streamLeader("ACC", "TEST")
		require_NotNil(t, sl)
		afterStreamCreate := checkExpectedSubs(sl.NumSubscriptions())
		if sc == 0 {
			sc = afterStreamCreate
		}
		require_Equal(t, sc, afterStreamCreate)

		// Add the consumer.
		_, err = js.AddConsumer("TEST", &nats.ConsumerConfig{Durable: "CONSUMER"})
		require_NoError(t, err)
		afterConsumerCreate := checkExpectedSubs(sl.NumSubscriptions())
		if cc == 0 {
			cc = afterConsumerCreate
		}
		require_Equal(t, cc, afterConsumerCreate)

		// Delete the consumer, the subscriptions should drop down to what they were after the stream was created.
		require_NoError(t, js.DeleteConsumer("TEST", "CONSUMER"))
		afterConsumerDelete := checkExpectedSubs(sl.NumSubscriptions())
		require_Equal(t, afterStreamCreate, afterConsumerDelete)

		// Deleting the stream should drop the subscriptions back to the baseline.
		require_NoError(t, js.DeleteStream("TEST"))
		afterStreamDelete := checkExpectedSubs(sl.NumSubscriptions())
		if baseline == 0 {
			baseline = afterStreamDelete
		}
		require_Equal(t, baseline, afterStreamDelete)
	}
}

func TestJetStreamClusterInterestStreamWithConsumerFilterUpdate(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "R3S", 3)
	defer c.shutdown()

	nc, js := jsClientConnect(t, c.randomServer())
	defer nc.Close()

	_, err := js.AddStream(&nats.StreamConfig{
		Name:      "TEST",
		Subjects:  []string{"foo.*"},
		Replicas:  3,
		Retention: nats.InterestPolicy,
	})
	require_NoError(t, err)

	// Create a consumer with a filter on 'foo.a'.
	cfg := &nats.ConsumerConfig{
		Durable:        "CONSUMER",
		FilterSubjects: []string{"foo.a", "foo.c"},
		AckPolicy:      nats.AckExplicitPolicy,
	}
	_, err = js.AddConsumer("TEST", cfg)
	require_NoError(t, err)

	sub, err := js.PullSubscribe(_EMPTY_, "CONSUMER", nats.Bind("TEST", "CONSUMER"))
	require_NoError(t, err)
	defer sub.Drain()

	checkFilterSubject := func(expected string) {
		checkFor(t, 2*time.Second, 100*time.Millisecond, func() error {
			for _, s := range c.servers {
				mset, err := s.globalAccount().lookupStream("TEST")
				if err != nil {
					return err
				}
				o := mset.lookupConsumer("CONSUMER")
				if o == nil {
					return errors.New("consumer not found")
				}
				if !slices.Contains(o.config().FilterSubjects, expected) {
					return fmt.Errorf("expected filter subject %q, got %q", expected, o.config().FilterSubjects)
				}
			}
			return nil
		})
	}
	checkFilterSubject("foo.a")

	// Publishing a message to 'foo.a' should be persisted since it matches the consumer.
	_, err = js.Publish("foo.a", nil)
	require_NoError(t, err)
	checkFor(t, 2*time.Second, 200*time.Millisecond, func() error {
		if state, err := checkStateAndErr(t, c, globalAccountName, "TEST"); err != nil {
			return err
		} else if state.Msgs != 1 || state.FirstSeq != 1 || state.LastSeq != 1 {
			return fmt.Errorf("expected 1 msg, got %d [%d:%d]", state.Msgs, state.FirstSeq, state.LastSeq)
		}
		return nil
	})

	// Fetch and ack the message with 'foo.a'.
	msgs, err := sub.Fetch(1)
	require_NoError(t, err)
	require_Len(t, len(msgs), 1)
	msg := msgs[0]
	require_Equal(t, msg.Subject, "foo.a")
	require_NoError(t, msg.AckSync())

	// Update the consumer, removing the 'foo.a' filter, and adding 'foo.b'.
	cfg.FilterSubjects = []string{"foo.b", "foo.c"}
	_, err = js.UpdateConsumer("TEST", cfg)
	require_NoError(t, err)
	checkFilterSubject("foo.b")

	// Publishing a message to 'foo.b' should be persisted since it matches the consumer.
	_, err = js.Publish("foo.b", nil)
	require_NoError(t, err)
	checkFor(t, 2*time.Second, 200*time.Millisecond, func() error {
		if state, err := checkStateAndErr(t, c, globalAccountName, "TEST"); err != nil {
			return err
		} else if state.Msgs != 1 || state.FirstSeq != 2 || state.LastSeq != 2 {
			return fmt.Errorf("expected 1 msg, got %d [%d:%d]", state.Msgs, state.FirstSeq, state.LastSeq)
		}
		return nil
	})

	// Fetch and ack the message with 'foo.b'.
	msgs, err = sub.Fetch(1)
	require_NoError(t, err)
	require_Len(t, len(msgs), 1)
	msg = msgs[0]
	require_Equal(t, msg.Subject, "foo.b")
	require_NoError(t, msg.AckSync())

	checkFor(t, 2*time.Second, 200*time.Millisecond, func() error {
		if state, err := checkStateAndErr(t, c, globalAccountName, "TEST"); err != nil {
			return err
		} else if state.Msgs != 0 || state.FirstSeq != 3 || state.LastSeq != 2 {
			return fmt.Errorf("expected 0 msgs, got %d [%d:%d]", state.Msgs, state.FirstSeq, state.LastSeq)
		}
		return nil
	})
}

func TestJetStreamClusterStreamRecreateChangesRaftGroup(t *testing.T) {
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
	_, err = js.Publish("foo", nil)
	require_NoError(t, err)
	checkFor(t, 2*time.Second, 200*time.Millisecond, func() error {
		return checkState(t, c, globalAccountName, "TEST")
	})

	rs := c.randomServer()
	mset, err := rs.globalAccount().lookupStream("TEST")
	require_NoError(t, err)
	n := mset.raftNode()
	old := n.Group()

	// Recreate the stream.
	require_NoError(t, js.DeleteStream("TEST"))
	_, err = js.AddStream(cfg)
	require_NoError(t, err)

	for range 2 {
		_, err = js.Publish("foo", nil)
		require_NoError(t, err)
	}
	checkFor(t, 2*time.Second, 200*time.Millisecond, func() error {
		return checkState(t, c, globalAccountName, "TEST")
	})

	// Expect the group to change.
	mset, err = rs.globalAccount().lookupStream("TEST")
	require_NoError(t, err)
	n = mset.raftNode()
	require_NotEqual(t, old, n.Group())
}

func TestJetStreamClusterStreamScaleDownChangesRaftGroup(t *testing.T) {
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

	// Publish a couple messages.
	for range 2 {
		_, err = js.Publish("foo", []byte("A"))
		require_NoError(t, err)
	}
	checkFor(t, 2*time.Second, 200*time.Millisecond, func() error {
		return checkState(t, c, globalAccountName, "TEST")
	})

	var pausedServer *Server
	var streamLeader *Server
	for _, s := range c.servers {
		if streamLeader != nil && pausedServer != nil {
			break
		}
		if s.JetStreamIsStreamLeader(globalAccountName, "TEST") {
			streamLeader = s
			continue
		}
		// Select a server that's neither the stream leader nor the meta leader.
		if pausedServer == nil && !s.JetStreamIsLeader() {
			pausedServer = s
			continue
		}
	}
	require_NotNil(t, pausedServer)
	require_NotNil(t, streamLeader)

	// Pause the meta layer on one server, simulating slow meta changes.
	// The stream update will take a while to apply on this server.
	sjs := pausedServer.getJetStream()
	meta := sjs.getMetaGroup()
	require_NoError(t, meta.PauseApply())
	mset, err := pausedServer.globalAccount().lookupStream("TEST")
	require_NoError(t, err)
	n := mset.raftNode()
	old := n.Group()

	// Scale stream down and back up.
	cfg.Replicas = 1
	_, err = js.UpdateStream(cfg)
	require_NoError(t, err)
	// Wait for scale down to finish, since the group is NOT changed if scaling
	// too fast since it would remain replicated throughout.
	c.waitOnStreamLeader(globalAccountName, "TEST")
	// Publish a couple more messages while it's R1.
	for range 2 {
		_, err = js.Publish("foo", []byte("B"))
		require_NoError(t, err)
	}
	cfg.Replicas = 3
	_, err = js.UpdateStream(cfg)
	require_NoError(t, err)
	c.waitOnStreamLeader(globalAccountName, "TEST")

	// Wait for some time to let the servers catch each other up. Can't use equality checks here.
	time.Sleep(500 * time.Millisecond)

	// Step down the current stream leader without selecting a new preferred leader.
	lmset, err := streamLeader.globalAccount().lookupStream("TEST")
	require_NoError(t, err)
	lmset.raftNode().(*raft).switchToFollower(noLeader)

	// The paused server is the first to campaign. If Raft groups are reused, this will be the leader.
	require_NoError(t, n.CampaignImmediately())
	time.Sleep(500 * time.Millisecond)
	c.waitOnStreamLeader(globalAccountName, "TEST")

	// Publish a couple more messages.
	for range 2 {
		_, err = js.Publish("foo", []byte("C"))
		require_NoError(t, err)
	}

	// Wait for some time to have the published messages be persisted. Can't use equality checks here.
	time.Sleep(500 * time.Millisecond)

	// Check that the messages are received in the right order.
	// The paused server should only contain a subset.
	for _, s := range c.servers {
		mset, err = s.globalAccount().lookupStream("TEST")
		require_NoError(t, err)
		for seq := uint64(1); seq <= 6; seq++ {
			sm, err := mset.store.LoadMsg(seq, nil)
			if err != nil {
				continue
			}
			if seq <= 2 {
				require_Equal(t, "A", string(sm.buf))
			} else if seq <= 4 {
				require_Equal(t, "B", string(sm.buf))
			} else {
				require_Equal(t, "C", string(sm.buf))
			}
		}
	}

	// Unpause the server and have it create the scaled up stream under a new Raft group.
	meta.ResumeApply()
	checkFor(t, 2*time.Second, 200*time.Millisecond, func() error {
		mset, err = pausedServer.globalAccount().lookupStream("TEST")
		if err != nil {
			return err
		}
		n = mset.raftNode()
		if ng := n.Group(); old == ng {
			return fmt.Errorf("expected new group but got %q", ng)
		}
		return nil
	})
	// Now all servers should end up being synchronized.
	checkFor(t, 2*time.Second, 200*time.Millisecond, func() error {
		return checkState(t, c, globalAccountName, "TEST")
	})
}

func TestJetStreamClusterStreamRescaleCatchup(t *testing.T) {
	test := func(t *testing.T, doSnapshot bool) {
		c := createJetStreamClusterExplicit(t, "R3S", 3)
		defer c.shutdown()

		nc, js := jsClientConnect(t, c.leader())
		defer nc.Close()

		cfg := &nats.StreamConfig{
			Name:     "TEST",
			Subjects: []string{"foo"},
			Replicas: 3,
		}
		_, err := js.AddStream(cfg)
		require_NoError(t, err)

		_, err = js.Publish("foo", nil)
		require_NoError(t, err)
		checkFor(t, 2*time.Second, 200*time.Millisecond, func() error {
			return checkState(t, c, globalAccountName, "TEST")
		})

		var rs *Server
		for _, s := range c.servers {
			// Select a server that's neither the stream leader nor the meta leader.
			if !s.JetStreamIsLeader() && !s.JetStreamIsStreamLeader(globalAccountName, "TEST") {
				rs = s
				break
			}
		}
		require_NotNil(t, rs)
		rs.Shutdown()
		rs.WaitForShutdown()

		// Scale stream down and back up.
		cfg.Replicas = 1
		_, err = js.UpdateStream(cfg)
		require_NoError(t, err)
		// Wait for scale down to finish, since the group is NOT changed if scaling
		// too fast since it would remain replicated throughout.
		c.waitOnStreamLeader(globalAccountName, "TEST")
		cfg.Replicas = 3
		_, err = js.UpdateStream(cfg)
		require_NoError(t, err)

		// Wait for some time to let the servers catch each other up. Can't use equality checks here.
		time.Sleep(500 * time.Millisecond)
		if doSnapshot {
			for _, s := range c.servers {
				if s == rs {
					continue
				}
				sjs := s.getJetStream()
				n := sjs.getMetaGroup()
				snap, _, _, err := sjs.metaSnapshot()
				require_NoError(t, err)
				require_NoError(t, n.InstallSnapshot(snap, false))
			}
		}

		// Publish another message, after restart all servers should become synchronized.
		_, err = js.Publish("foo", nil)
		require_NoError(t, err)
		c.restartServer(rs)

		// Now all servers should end up being synchronized.
		checkFor(t, 2*time.Second, 200*time.Millisecond, func() error {
			return checkState(t, c, globalAccountName, "TEST")
		})
	}
	t.Run("Catchup", func(t *testing.T) { test(t, false) })
	t.Run("Snapshot", func(t *testing.T) { test(t, true) })
}

func TestJetStreamClusterConsumerRecreateChangesRaftGroup(t *testing.T) {
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
		Replicas: 3,
	}
	_, err = js.AddConsumer("TEST", cfg)
	require_NoError(t, err)
	checkConsumerCount := func(expected int) {
		checkFor(t, 2*time.Second, 200*time.Millisecond, func() error {
			for _, s := range c.servers {
				mset, err := s.globalAccount().lookupStream("TEST")
				if err != nil {
					return err
				}
				if consumers := mset.numConsumers(); consumers != expected {
					return fmt.Errorf("expected %d consumer, got %d", expected, consumers)
				}
			}
			return nil
		})
	}
	checkConsumerCount(1)

	rs := c.randomServer()
	mset, err := rs.globalAccount().lookupStream("TEST")
	require_NoError(t, err)
	n := mset.lookupConsumer("CONSUMER").raftNode()
	old := n.Group()

	// Recreate the consumer.
	require_NoError(t, js.DeleteConsumer("TEST", "CONSUMER"))
	checkConsumerCount(0)
	_, err = js.AddConsumer("TEST", cfg)
	require_NoError(t, err)
	checkConsumerCount(1)

	// Expect the group to change.
	mset, err = rs.globalAccount().lookupStream("TEST")
	require_NoError(t, err)
	n = mset.lookupConsumer("CONSUMER").raftNode()
	require_NotEqual(t, old, n.Group())
}

func TestJetStreamClusterConsumerScaleDownChangesRaftGroup(t *testing.T) {
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
		Replicas: 3,
	}
	_, err = js.AddConsumer("TEST", cfg)
	require_NoError(t, err)
	checkConsumerCount := func(expected int) {
		checkFor(t, 2*time.Second, 200*time.Millisecond, func() error {
			for _, s := range c.servers {
				mset, err := s.globalAccount().lookupStream("TEST")
				if err != nil {
					return err
				}
				if consumers := mset.numConsumers(); consumers != expected {
					return fmt.Errorf("expected %d consumer, got %d", expected, consumers)
				}
			}
			return nil
		})
	}
	checkConsumerCount(1)

	var pausedServer *Server
	for _, s := range c.servers {
		// Select a server that's neither the consumer leader nor the meta leader.
		if !s.JetStreamIsLeader() && !s.JetStreamIsConsumerLeader(globalAccountName, "TEST", "CONSUMER") {
			pausedServer = s
			break
		}
	}
	require_NotNil(t, pausedServer)

	// Pause the meta layer on one server, simulating slow meta changes.
	// The consumer update will take a while to apply on this server.
	sjs := pausedServer.getJetStream()
	meta := sjs.getMetaGroup()
	require_NoError(t, meta.PauseApply())
	mset, err := pausedServer.globalAccount().lookupStream("TEST")
	require_NoError(t, err)
	n := mset.lookupConsumer("CONSUMER").raftNode()
	old := n.Group()

	// Scale consumer down and back up.
	cfg.Replicas = 1
	_, err = js.UpdateConsumer("TEST", cfg)
	require_NoError(t, err)
	// Wait for scale down to finish, since the group is NOT changed if scaling
	// too fast since it would remain replicated throughout.
	c.waitOnConsumerLeader(globalAccountName, "TEST", "CONSUMER")
	cfg.Replicas = 3
	_, err = js.UpdateConsumer("TEST", cfg)
	require_NoError(t, err)

	// Wait for some time to let the servers catch each other up.
	time.Sleep(500 * time.Millisecond)

	// Our paused server should still have the old consumer.
	mset, err = pausedServer.globalAccount().lookupStream("TEST")
	require_NoError(t, err)
	n = mset.lookupConsumer("CONSUMER").raftNode()
	require_Equal(t, old, n.Group())

	// Unpause the server and have it create the scaled up stream under a new Raft group.
	meta.ResumeApply()
	checkFor(t, 2*time.Second, 200*time.Millisecond, func() error {
		mset, err = pausedServer.globalAccount().lookupStream("TEST")
		if err != nil {
			return err
		}
		o := mset.lookupConsumer("CONSUMER")
		if o == nil {
			return fmt.Errorf("consumer not found")
		}
		n = o.raftNode()
		if n == nil {
			return fmt.Errorf("raft node not found")
		}
		if ng := n.Group(); old == ng {
			return fmt.Errorf("expected new group but got %q", ng)
		}
		return nil
	})
}

func TestJetStreamClusterConsumerRescaleCatchup(t *testing.T) {
	test := func(t *testing.T, doSnapshot bool) {
		c := createJetStreamClusterExplicit(t, "R3S", 3)
		defer c.shutdown()

		nc, js := jsClientConnect(t, c.leader())
		defer nc.Close()

		_, err := js.AddStream(&nats.StreamConfig{
			Name:     "TEST",
			Subjects: []string{"foo"},
			Replicas: 3,
		})
		require_NoError(t, err)

		cfg := &nats.ConsumerConfig{
			Durable:  "CONSUMER",
			Replicas: 3,
		}
		_, err = js.AddConsumer("TEST", cfg)
		require_NoError(t, err)
		checkFor(t, 2*time.Second, 200*time.Millisecond, func() error {
			for _, s := range c.servers {
				mset, err := s.globalAccount().lookupStream("TEST")
				if err != nil {
					return err
				}
				if consumers := mset.numConsumers(); consumers != 1 {
					return fmt.Errorf("expected 1 consumer, got %d", consumers)
				}
			}
			return nil
		})

		var rs *Server
		for _, s := range c.servers {
			// Select a server that's neither the stream leader nor the meta leader.
			if !s.JetStreamIsLeader() && !s.JetStreamIsConsumerLeader(globalAccountName, "TEST", "CONSUMER") {
				rs = s
				break
			}
		}
		require_NotNil(t, rs)
		rs.Shutdown()
		rs.WaitForShutdown()

		// Scale consumer down and back up.
		cfg.Replicas = 1
		_, err = js.UpdateConsumer("TEST", cfg)
		require_NoError(t, err)
		// Wait for scale down to finish, since the group is NOT changed if scaling
		// too fast since it would remain replicated throughout.
		c.waitOnConsumerLeader(globalAccountName, "TEST", "CONSUMER")
		cfg.Replicas = 3
		_, err = js.UpdateConsumer("TEST", cfg)
		require_NoError(t, err)

		// Wait for some time to let the servers catch each other up. Can't use equality checks here.
		time.Sleep(500 * time.Millisecond)
		if doSnapshot {
			for _, s := range c.servers {
				if s == rs {
					continue
				}
				sjs := s.getJetStream()
				n := sjs.getMetaGroup()
				snap, _, _, err := sjs.metaSnapshot()
				require_NoError(t, err)
				require_NoError(t, n.InstallSnapshot(snap, false))
			}
		}

		// After restart all servers should become synchronized.
		c.restartServer(rs)
		checkFor(t, 2*time.Second, 200*time.Millisecond, func() error {
			var g string
			for _, s := range c.servers {
				mset, err := s.globalAccount().lookupStream("TEST")
				if err != nil {
					return err
				}
				o := mset.lookupConsumer("CONSUMER")
				if o == nil {
					return errors.New("consumer not found")
				}
				n := o.raftNode()
				if n == nil {
					return errors.New("no raft node")
				} else if ng := n.Group(); g == "" {
					g = ng
				} else if ng != g {
					return fmt.Errorf("expected same group, got %q and %q", g, ng)
				}
			}
			return nil
		})
	}
	t.Run("Catchup", func(t *testing.T) { test(t, false) })
	t.Run("Snapshot", func(t *testing.T) { test(t, true) })
}

func TestJetStreamClusterConcurrentStreamUpdate(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "R3S", 3)
	defer c.shutdown()

	ml := c.leader()
	nc, js := jsClientConnect(t, ml)
	defer nc.Close()

	cfg := &nats.StreamConfig{
		Name:     "TEST",
		Subjects: []string{"foo"},
		Replicas: 3,
	}
	_, err := js.AddStream(cfg)
	require_NoError(t, err)

	for _, s := range c.servers {
		if s == ml {
			continue
		}
		s.Shutdown()
	}

	// The stream update to seal the stream will be received and proposed by the meta leader,
	// but it will not be able to achieve quorum immediately.
	cfg.Sealed = true
	req, err := json.Marshal(cfg)
	require_NoError(t, err)
	require_NoError(t, nc.Publish(fmt.Sprintf(JSApiStreamUpdateT, cfg.Name), req))

	// We need to wait a bit to ensure the above request is handled first.
	time.Sleep(100 * time.Millisecond)

	// A concurrent stream update should error immediately if the config update check fails.
	cfg.Sealed = false
	_, err = js.UpdateStream(cfg)
	require_Error(t, err, NewJSStreamInvalidConfigError(fmt.Errorf("stream configuration update can not unseal a sealed stream")))

	// Confirm the meta leader actually tracked the inflight stream update that was sent first.
	sjs, cc := ml.getJetStreamCluster()
	sjs.mu.RLock()
	i := len(cc.inflightStreams)
	sjs.mu.RUnlock()
	require_Equal(t, i, 1)

	// Restart the servers, eventually the stream should be reporting as sealed.
	for _, s := range c.servers {
		if s == ml {
			continue
		}
		c.restartServer(s)
	}
	checkFor(t, 5*time.Second, 200*time.Millisecond, func() error {
		m, err := nc.Request(fmt.Sprintf(JSApiStreamInfoT, cfg.Name), nil, 200*time.Millisecond)
		if err != nil {
			return err
		}
		var resp JSApiStreamInfoResponse
		if err = json.Unmarshal(m.Data, &resp); err != nil {
			return err
		}
		if !resp.Config.Sealed {
			return errors.New("stream isn't sealed yet")
		}
		return nil
	})

	// The inflight state should be cleared.
	for _, s := range c.servers {
		sjs, cc = s.getJetStreamCluster()
		sjs.mu.RLock()
		i = len(cc.inflightStreams)
		sjs.mu.RUnlock()
		require_Equal(t, i, 0)
	}
}

func TestJetStreamClusterMetaLeaderRespectsInflight(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "R3S", 3)
	defer c.shutdown()

	ml := c.leader()
	require_NotNil(t, ml)

	sjs, cc := ml.getJetStreamCluster()
	require_NotNil(t, sjs)
	require_NotNil(t, cc)

	ci := &ClientInfo{Account: globalAccountName}
	rg := &raftGroup{Name: "INFLIGHT_G", Peers: []string{"offline"}, Storage: MemoryStorage}

	sjs.mu.Lock()
	sa := &streamAssignment{
		Client:  ci,
		Created: time.Now(),
		Config:  &StreamConfig{Name: "S"},
		Group:   rg,
	}
	ca := &consumerAssignment{
		Client:  ci,
		Created: time.Now(),
		Name:    "C",
		Stream:  "S",
		Config:  &ConsumerConfig{},
		Group:   rg,
	}
	cc.trackInflightStreamProposal(globalAccountName, sa, false)
	cc.trackInflightConsumerProposal(globalAccountName, "S", ca, false)

	asa := sjs.streamAssignment(globalAccountName, "S")
	isa := sjs.streamAssignmentOrInflight(globalAccountName, "S")
	aca := sjs.consumerAssignment(globalAccountName, "S", "C")
	ica := sjs.consumerAssignmentOrInflight(globalAccountName, "S", "C")
	sjs.mu.Unlock()

	require_True(t, asa == nil)
	require_True(t, aca == nil)
	require_NotNil(t, isa)
	require_NotNil(t, ica)

	// Meta leader should return Offline (inflight assignment with no live peers)
	// rather than NotFound, because it sees the inflight proposal.
	nc, js := jsClientConnect(t, ml)
	defer nc.Close()

	_, err := js.StreamInfo("S")
	require_Error(t, err, NewJSStreamOfflineError())
	_, err = js.ConsumerInfo("S", "C")
	require_Error(t, err, NewJSConsumerOfflineError())
}

func TestJetStreamClusterConcurrentConsumerCreateWithMaxConsumers(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "R3S", 3)
	defer c.shutdown()

	ml := c.leader()
	nc, js := jsClientConnect(t, ml)
	defer nc.Close()

	_, err := js.AddStream(&nats.StreamConfig{
		Name:         "TEST",
		Subjects:     []string{"foo"},
		Replicas:     3,
		MaxConsumers: 1,
	})
	require_NoError(t, err)

	for _, s := range c.servers {
		if s == ml {
			continue
		}
		s.Shutdown()
	}

	// The consumer create will be received and proposed by the meta leader,
	// but it will not be able to achieve quorum.
	ccr := CreateConsumerRequest{Stream: "TEST", Config: ConsumerConfig{Durable: "C1", Replicas: 1}}
	req, err := json.Marshal(ccr)
	require_NoError(t, err)
	require_NoError(t, nc.Publish(fmt.Sprintf(JSApiDurableCreateT, "TEST", "C1"), req))

	// We need to wait a bit to ensure the above request is handled first.
	time.Sleep(100 * time.Millisecond)

	// Another consumer create should error immediately since with the inflight consumer we're at limits.
	_, err = js.AddConsumer("TEST", &nats.ConsumerConfig{Durable: "C2", Replicas: 1})
	require_Error(t, err, NewJSMaximumConsumersLimitError())

	// Confirm the meta leader actually tracked the inflight consumer create that was sent first.
	sjs, cc := ml.getJetStreamCluster()
	sjs.mu.RLock()
	iu := len(cc.inflightConsumers)
	sjs.mu.RUnlock()
	require_Equal(t, iu, 1)

	// Restart the servers, eventually the consumer should exist.
	for _, s := range c.servers {
		if s == ml {
			continue
		}
		c.restartServer(s)
	}
	checkFor(t, 5*time.Second, 200*time.Millisecond, func() error {
		var found bool
		for _, s := range c.servers {
			sjs = s.getJetStream()
			sjs.mu.RLock()
			ca := sjs.consumerAssignment(globalAccountName, "TEST", "C1")
			sjs.mu.RUnlock()
			if ca != nil {
				found = true
			}
		}
		if !found {
			return errors.New("consumer not found")
		}

		sjs, cc = ml.getJetStreamCluster()
		sjs.mu.RLock()
		iu = len(cc.inflightConsumers)
		sjs.mu.RUnlock()
		if iu != 0 {
			return fmt.Errorf("expected no inflight consumer updates, got %d", iu)
		}
		return nil
	})
}

func TestJetStreamClusterLostConsumerAfterInflightConsumerUpdate(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "R3S", 3)
	defer c.shutdown()

	ml := c.leader()
	nc, js := jsClientConnect(t, ml)
	defer nc.Close()

	_, err := js.AddStream(&nats.StreamConfig{
		Name:     "TEST",
		Subjects: []string{"foo"},
		Replicas: 3,
	})
	require_NoError(t, err)

	_, err = js.AddConsumer("TEST", &nats.ConsumerConfig{Durable: "CONSUMER"})
	require_NoError(t, err)

	for _, s := range c.servers {
		if s == ml {
			continue
		}
		s.Shutdown()
	}

	// The consumer update will be received and proposed by the meta leader,
	// but it will not be able to achieve quorum.
	ccr := CreateConsumerRequest{Stream: "TEST", Config: ConsumerConfig{Durable: "CONSUMER"}}
	req, err := json.Marshal(ccr)
	require_NoError(t, err)
	require_NoError(t, nc.Publish(fmt.Sprintf(JSApiDurableCreateT, "TEST", "CONSUMER"), req))

	// We need to wait a bit to ensure the above request is handled.
	time.Sleep(100 * time.Millisecond)

	// Confirm the meta leader actually tracked the inflight consumer update that was sent.
	sjs, cc := ml.getJetStreamCluster()
	sjs.mu.RLock()
	iu := len(cc.inflightConsumers)
	sjs.mu.RUnlock()
	require_Equal(t, iu, 1)

	// Snapshot meta, if it didn't separately track the inflight consumer update, it could lose the entire consumer.
	require_NoError(t, ml.JetStreamSnapshotMeta())
	ml.Shutdown()

	// Restart all servers.
	for _, s := range c.servers {
		c.restartServer(s)
	}
	// Check the consumer still exists on all servers.
	checkFor(t, 2*time.Second, 200*time.Millisecond, func() error {
		for _, s := range c.servers {
			sjs = s.getJetStream()
			sjs.mu.RLock()
			ca := sjs.consumerAssignment(globalAccountName, "TEST", "CONSUMER")
			sjs.mu.RUnlock()
			if ca == nil {
				return errors.New("consumer not found")
			}
		}
		return nil
	})
}

func TestJetStreamClusterConcurrentR1ConsumerUpdatesAllRespond(t *testing.T) {
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

	_, err = js.AddConsumer("TEST", &nats.ConsumerConfig{Name: "C", AckPolicy: nats.AckExplicitPolicy, Replicas: 1})
	require_NoError(t, err)

	// Send many concurrent, identical create-or-update requests for the R1 consumer.
	req, err := json.Marshal(CreateConsumerRequest{
		Stream: "TEST",
		Config: ConsumerConfig{Name: "C", AckPolicy: AckExplicit, Replicas: 1},
		Action: ActionCreateOrUpdate,
	})
	require_NoError(t, err)
	subject := fmt.Sprintf(JSApiConsumerCreateT, "TEST") + ".C"

	const requests = 100
	subs := make([]*nats.Subscription, 0, requests)
	for range requests {
		sub, err := nc.SubscribeSync(nats.NewInbox())
		require_NoError(t, err)
		subs = append(subs, sub)
	}
	for _, sub := range subs {
		require_NoError(t, nc.PublishRequest(subject, sub.Subject, req))
	}
	require_NoError(t, nc.Flush())

	for i, sub := range subs {
		msg, err := sub.NextMsg(2 * time.Second)
		if err != nil {
			t.Fatalf("Request %d did not receive a response: %v", i, err)
		}
		var resp JSApiConsumerCreateResponse
		require_NoError(t, json.Unmarshal(msg.Data, &resp))
		require_True(t, resp.Error == nil)
		require_NotNil(t, resp.ConsumerInfo)
	}
}

func TestJetStreamClusterStreamRaftGroupChangesWhenMovingToOrOffR1(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "R5S", 5)
	defer c.shutdown()

	nc, js := jsClientConnect(t, c.randomServer())
	defer nc.Close()

	requireGroupPrefix := func(prefix string) {
		t.Helper()
		c.waitOnStreamLeader(globalAccountName, "TEST")
		sl := c.streamLeader(globalAccountName, "TEST")
		require_NotNil(t, sl)
		sjs := sl.getJetStream()
		sjs.mu.RLock()
		defer sjs.mu.RUnlock()
		sa := sjs.streamAssignment(globalAccountName, "TEST")
		if sa.Group == nil {
			t.Fatal("no group")
		} else if !strings.HasPrefix(sa.Group.Name, prefix) {
			t.Fatalf("expected group prefix %q, got %q", prefix, sa.Group.Name)
		}
	}

	cfg := &nats.StreamConfig{
		Name:     "TEST",
		Subjects: []string{"foo"},
		Replicas: 1,
	}
	_, err := js.AddStream(cfg)
	require_NoError(t, err)
	requireGroupPrefix("S-R1F-")

	cfg.Replicas = 3
	_, err = js.UpdateStream(cfg)
	require_NoError(t, err)
	requireGroupPrefix("S-R3F-")

	cfg.Replicas = 1
	_, err = js.UpdateStream(cfg)
	require_NoError(t, err)
	requireGroupPrefix("S-R1F-")

	cfg.Replicas = 5
	_, err = js.UpdateStream(cfg)
	require_NoError(t, err)
	requireGroupPrefix("S-R5F-")

	cfg.Replicas = 3
	_, err = js.UpdateStream(cfg)
	require_NoError(t, err)
	// The group MUST remain the same as what it was for R5.
	// Changing it would violate replication safety.
	requireGroupPrefix("S-R5F-")
}

func TestJetStreamClusterConsumerRaftGroupChangesWhenMovingToOrOffR1(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "R5S", 5)
	defer c.shutdown()

	nc, js := jsClientConnect(t, c.randomServer())
	defer nc.Close()

	requireGroupPrefix := func(prefix string) {
		t.Helper()
		c.waitOnConsumerLeader(globalAccountName, "TEST", "CONSUMER")
		cl := c.consumerLeader(globalAccountName, "TEST", "CONSUMER")
		require_NotNil(t, cl)
		sjs := cl.getJetStream()
		sjs.mu.RLock()
		defer sjs.mu.RUnlock()
		ca := sjs.consumerAssignment(globalAccountName, "TEST", "CONSUMER")
		if ca.Group == nil {
			t.Fatal("no group")
		} else if !strings.HasPrefix(ca.Group.Name, prefix) {
			t.Fatalf("expected group prefix %q, got %q", prefix, ca.Group.Name)
		}
	}

	_, err := js.AddStream(&nats.StreamConfig{
		Name:     "TEST",
		Subjects: []string{"foo"},
		Replicas: 5,
	})
	require_NoError(t, err)

	cfg := &nats.ConsumerConfig{
		Durable:  "CONSUMER",
		Replicas: 1,
	}
	_, err = js.AddConsumer("TEST", cfg)
	require_NoError(t, err)
	requireGroupPrefix("C-R1F-")

	cfg.Replicas = 3
	_, err = js.UpdateConsumer("TEST", cfg)
	require_NoError(t, err)
	requireGroupPrefix("C-R3F-")

	cfg.Replicas = 1
	_, err = js.UpdateConsumer("TEST", cfg)
	require_NoError(t, err)
	requireGroupPrefix("C-R1F-")

	cfg.Replicas = 5
	_, err = js.UpdateConsumer("TEST", cfg)
	require_NoError(t, err)
	requireGroupPrefix("C-R5F-")

	cfg.Replicas = 3
	_, err = js.UpdateConsumer("TEST", cfg)
	require_NoError(t, err)
	// The group MUST remain the same as what it was for R5.
	// Changing it would violate replication safety.
	requireGroupPrefix("C-R5F-")
}

func TestJetStreamClusterStreamUpdateMaxConsumersLimit(t *testing.T) {
	test := func(t *testing.T, replicas int, remove bool) {
		var (
			getStreamLeader func() *Server
			restart         func()
			s               *Server
		)
		require_NotEqual(t, replicas, 0)
		if replicas == 1 {
			tmpl := `
				listen: 127.0.0.1:-1
				jetstream: {max_mem_store: 256MB, max_file_store: 2GB, store_dir: '%s'}
			`
			conf := createConfFile(t, []byte(fmt.Sprintf(tmpl, t.TempDir())))
			s, _ = RunServerWithConfig(conf)
			defer s.Shutdown()

			getStreamLeader = func() *Server { return s }
			restart = func() {
				s.Shutdown()
				s.WaitForShutdown()
				s, _ = RunServerWithConfig(conf)
				// No need to defer shutdown here, that's already handled by the defer above.
				getStreamLeader = func() *Server { return s }
			}
		} else {
			c := createJetStreamClusterExplicit(t, "R3S", 3)
			defer c.shutdown()
			s = c.randomServer()

			getStreamLeader = func() *Server { return c.streamLeader(globalAccountName, "TEST") }
			restart = func() {
				c.stopAll()
				c.restartAll()
				c.waitOnLeader()
				c.waitOnStreamLeader(globalAccountName, "TEST")
				c.waitOnConsumerLeader(globalAccountName, "TEST", "CONSUMER_1")
				if !remove {
					c.waitOnConsumerLeader(globalAccountName, "TEST", "CONSUMER_2")
				}
			}
		}

		nc, js := jsClientConnect(t, s)
		defer nc.Close()

		cfg := &nats.StreamConfig{
			Name:     "TEST",
			Subjects: []string{"foo"},
			Replicas: replicas,
		}
		_, err := js.AddStream(cfg)
		require_NoError(t, err)

		// Pre-create consumer configs to be reused.
		cc1 := &nats.ConsumerConfig{Durable: "CONSUMER_1", Replicas: replicas}
		cc2 := &nats.ConsumerConfig{Durable: "CONSUMER_2", Replicas: replicas}
		cc3 := &nats.ConsumerConfig{Durable: "CONSUMER_3", Replicas: replicas}

		// Create two consumers.
		for _, cc := range []*nats.ConsumerConfig{cc1, cc2} {
			_, err = js.AddConsumer("TEST", cc)
			require_NoError(t, err)
		}

		// Check we have two consumers.
		sl := getStreamLeader()
		require_NotNil(t, sl)
		mset, err := sl.globalAccount().lookupStream("TEST")
		require_NoError(t, err)
		require_Len(t, len(mset.getPublicConsumers()), 2)

		// Updating the max consumers limit should preserve the current consumers, even if they're over the limit.
		cfg.MaxConsumers = 1
		_, err = js.UpdateStream(cfg)
		require_NoError(t, err)
		require_Len(t, len(mset.getPublicConsumers()), 2)

		// Adding a consumer shouldn't be allowed as we're already over the limit (2 > 1).
		_, err = js.AddConsumer("TEST", cc3)
		require_Error(t, err, NewJSMaximumConsumersLimitError())

		// We should still be allowed to update a pre-existing consumer.
		for _, cc := range []*nats.ConsumerConfig{cc1, cc2} {
			_, err = js.UpdateConsumer("TEST", cc)
			require_NoError(t, err)
		}

		// If we're testing removes, if we delete a consumer we're still at limit, so can't add another.
		if remove {
			require_NoError(t, js.DeleteConsumer("TEST", "CONSUMER_2"))
			_, err = js.AddConsumer("TEST", cc3)
			require_Error(t, err, NewJSMaximumConsumersLimitError())
		}

		// Restart, and if we didn't remove a consumer above, we should still come up with
		// all consumers even if it's over the limit.
		restart()
		sl = getStreamLeader()
		require_NotNil(t, sl)
		mset, err = sl.globalAccount().lookupStream("TEST")
		require_NoError(t, err)
		if remove {
			require_Len(t, len(mset.getPublicConsumers()), 1)
		} else {
			require_Len(t, len(mset.getPublicConsumers()), 2)
		}

		// Reconnect.
		nc.Close()
		nc, js = jsClientConnect(t, sl)
		defer nc.Close()

		// Allow 'infinite' consumers again, and confirm all consumers can be created.
		cfg.MaxConsumers = -1
		_, err = js.UpdateStream(cfg)
		require_NoError(t, err)
		for _, cc := range []*nats.ConsumerConfig{cc1, cc2, cc3} {
			_, err = js.AddConsumer("TEST", cc)
			require_NoError(t, err)
		}
		require_Len(t, len(mset.getPublicConsumers()), 3)
	}

	for _, replicas := range []int{1, 3} {
		for _, remove := range []bool{false, true} {
			desc := "Add"
			if remove {
				desc = "Remove"
			}
			t.Run(fmt.Sprintf("R%d/%s", replicas, desc), func(t *testing.T) { test(t, replicas, remove) })
		}
	}
}

func TestJetStreamClusterScaleDownWaitsForMonitorRoutineQuit(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "R3S", 3)
	defer c.shutdown()

	nc, js := jsClientConnect(t, c.randomServer())
	defer nc.Close()

	// Create R3 stream and consumer.
	scfg := &nats.StreamConfig{Name: "TEST", Subjects: []string{"foo"}, Replicas: 3}
	ccfg := &nats.ConsumerConfig{Name: "CONSUMER", Replicas: 3}
	_, err := js.AddStream(scfg)
	require_NoError(t, err)
	_, err = js.AddConsumer("TEST", ccfg)
	require_NoError(t, err)
	checkFor(t, 2*time.Second, 200*time.Millisecond, func() error {
		for _, s := range c.servers {
			sjs := s.getJetStream()
			if sjs.streamAssignment(globalAccountName, "TEST") == nil {
				return errors.New("stream not found")
			}
			if sjs.consumerAssignment(globalAccountName, "TEST", "CONSUMER") == nil {
				return errors.New("consumer not found")
			}
		}
		return nil
	})

	cf := c.randomNonConsumerLeader(globalAccountName, "TEST", "CONSUMER")
	require_NotNil(t, cf)
	mset, err := cf.globalAccount().lookupStream("TEST")
	require_NoError(t, err)
	o := mset.lookupConsumer("CONSUMER")
	require_NotNil(t, o)

	// Increment the wait group for this test to confirm the right ordering.
	// The Add must be done under monitorMu, just like shouldStartMonitor does,
	// so it cannot race a concurrent monitorWg.Wait.
	o.mu.RLock()
	inMonitor := o.inMonitor
	o.mu.RUnlock()
	require_True(t, inMonitor)
	o.monitorMu.Lock()
	wg := &o.monitorWg
	wg.Add(1)
	o.monitorMu.Unlock()

	// The monitor routine should stop.
	ccfg.Replicas = 1
	_, err = js.UpdateConsumer("TEST", ccfg)
	require_NoError(t, err)
	checkFor(t, 2*time.Second, 200*time.Millisecond, func() error {
		o.mu.RLock()
		defer o.mu.RUnlock()
		if o.inMonitor {
			return errors.New("consumer still in monitor")
		}
		return nil
	})

	// The consumer itself should still exist.
	require_NotNil(t, mset.lookupConsumer("CONSUMER"))

	// Simulate the monitor routine being done now and the consumer being removed.
	wg.Done()
	checkFor(t, 2*time.Second, 200*time.Millisecond, func() error {
		if mset.lookupConsumer("CONSUMER") != nil {
			return errors.New("consumer still exists")
		}
		return nil
	})

	sf := c.randomNonStreamLeader(globalAccountName, "TEST")
	require_NotNil(t, sf)
	mset, err = sf.globalAccount().lookupStream("TEST")
	require_NoError(t, err)

	// Increment the wait group for this test to confirm the right ordering.
	// The Add must be done under monitorMu, just like startMonitorWg does,
	// so it cannot race a concurrent monitorWg.Wait.
	mset.mu.RLock()
	inMonitor = mset.inMonitor
	mset.mu.RUnlock()
	require_True(t, inMonitor)
	mset.monitorMu.Lock()
	wg = &mset.monitorWg
	wg.Add(1)
	mset.monitorMu.Unlock()

	// The monitor routine should stop.
	scfg.Replicas = 1
	_, err = js.UpdateStream(scfg)
	require_NoError(t, err)
	checkFor(t, 2*time.Second, 200*time.Millisecond, func() error {
		mset.mu.RLock()
		defer mset.mu.RUnlock()
		if mset.inMonitor {
			return errors.New("stream still in monitor")
		}
		return nil
	})

	// The stream itself should still exist.
	_, err = sf.globalAccount().lookupStream("TEST")
	require_NoError(t, err)

	// Simulate the monitor routine being done now and the stream being removed.
	wg.Done()
	checkFor(t, 2*time.Second, 200*time.Millisecond, func() error {
		_, err = sf.globalAccount().lookupStream("TEST")
		if !errors.Is(err, NewJSStreamNotFoundError()) {
			return errors.New("stream still exists")
		}
		return nil
	})
}

func TestJetStreamClusterConsumerRemapWaitsForMonitorRoutineQuit(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "R3S", 3)
	defer c.shutdown()

	nc, js := jsClientConnect(t, c.randomServer())
	defer nc.Close()

	// Create R3 stream and consumer.
	scfg := &nats.StreamConfig{Name: "TEST", Subjects: []string{"foo"}, Replicas: 3}
	ccfg := &nats.ConsumerConfig{Name: "CONSUMER", Replicas: 3}
	_, err := js.AddStream(scfg)
	require_NoError(t, err)
	_, err = js.AddConsumer("TEST", ccfg)
	require_NoError(t, err)
	ml := c.leader()
	sjs, cc := ml.getJetStreamCluster()
	checkFor(t, 2*time.Second, 200*time.Millisecond, func() error {
		if sjs.streamAssignment(globalAccountName, "TEST") == nil {
			return errors.New("stream not found")
		}
		if sjs.consumerAssignment(globalAccountName, "TEST", "CONSUMER") == nil {
			return errors.New("consumer not found")
		}
		return nil
	})

	mset, err := ml.globalAccount().lookupStream("TEST")
	require_NoError(t, err)
	o := mset.lookupConsumer("CONSUMER")
	require_NotNil(t, o)

	// Increment the wait group for this test to confirm the right ordering.
	// The Add must be done under monitorMu, just like shouldStartMonitor does,
	// so it cannot race a concurrent monitorWg.Wait.
	o.mu.RLock()
	inMonitor := o.inMonitor
	rn := o.node
	o.mu.RUnlock()
	require_True(t, inMonitor)
	o.monitorMu.Lock()
	wg := &o.monitorWg
	wg.Add(1)
	o.monitorMu.Unlock()

	// Simulate a consumer Raft group remapping that has been collapsed down into just a single update.
	// Instead of one update to R1 and then to R3, it's just one update straight to the new R3 group.
	sjs.mu.Lock()
	ca := sjs.consumerAssignment(globalAccountName, "TEST", "CONSUMER")
	if ca == nil {
		sjs.mu.Unlock()
		t.Fatal("consumer assignment not found")
	}
	cca := ca.copyGroup()
	cca.Group.Name = groupNameForConsumer(cca.Group.Peers, cca.Group.Storage)
	err = cc.meta.Propose(cc.meta.Term(), encodeAddConsumerAssignment(cca))
	sjs.mu.Unlock()
	require_NoError(t, err)

	// The monitor routine should stop.
	checkFor(t, 2*time.Second, 200*time.Millisecond, func() error {
		o.mu.RLock()
		defer o.mu.RUnlock()
		if o.inMonitor {
			return errors.New("consumer still in monitor")
		}
		return nil
	})

	// The previous Raft node should be stopped.
	require_Equal(t, rn.State(), Closed)

	// Simulate the monitor routine being done now and the new monitor routine being started.
	wg.Done()
	checkFor(t, 2*time.Second, 200*time.Millisecond, func() error {
		o.mu.RLock()
		defer o.mu.RUnlock()
		if o.node == nil {
			return errors.New("consumer has no Raft node yet")
		} else if !o.inMonitor {
			return errors.New("consumer monitor not started")
		}
		return nil
	})
}

// Must be run with -race.
func TestJetStreamClusterConsumerMonitorWaitGroupRace(t *testing.T) {
	o := &consumer{}

	var ready sync.WaitGroup
	ready.Add(1)
	done := make(chan struct{})

	var fin sync.WaitGroup
	fin.Add(2)

	// Repeatedly start and stop the monitor, exercising monitorWg.Add / Done.
	go func() {
		defer fin.Done()
		ready.Wait()
		for {
			select {
			case <-done:
				return
			default:
			}
			if o.shouldStartMonitor() {
				o.clearMonitorRunning()
			}
		}
	}()

	// Repeatedly wait on the monitor, exercising monitorWg.Wait.
	go func() {
		defer fin.Done()
		ready.Done()
		for range 500_000 {
			o.stopMonitoring()
		}
		close(done)
	}()

	fin.Wait()
}

// Must be run with -race.
func TestJetStreamClusterStreamMonitorWaitGroupRace(t *testing.T) {
	mset := &stream{}

	var ready sync.WaitGroup
	ready.Add(1)
	done := make(chan struct{})

	var fin sync.WaitGroup
	fin.Add(2)

	// Repeatedly register and unregister a monitor goroutine, exercising
	// monitorWg.Add (via startMonitorWg) / Done.
	go func() {
		defer fin.Done()
		ready.Wait()
		for {
			select {
			case <-done:
				return
			default:
			}
			mset.startMonitorWg()
			mset.monitorWg.Done()
		}
	}()

	// Repeatedly wait on the monitor, exercising monitorWg.Wait.
	go func() {
		defer fin.Done()
		ready.Done()
		for range 500_000 {
			mset.stopMonitoring()
		}
		close(done)
	}()

	fin.Wait()
}

func TestJetStreamClusterAccountStoreLimits(t *testing.T) {
	test := func(t *testing.T, replicas int, storage nats.StorageType) {
		storeLimit := fileStoreMsgSize("B", nil, nil)
		memLimit := memStoreMsgSize("B", nil, nil)
		limit := JetStreamAccountLimits{
			MaxMemory:            int64(memLimit * 6),
			MemoryMaxStreamBytes: int64(memLimit * 3),
			MaxStore:             int64(storeLimit * 6),
			StoreMaxStreamBytes:  int64(storeLimit * 3),
			MaxBytesRequired:     true,
		}
		tier := fmt.Sprintf("R%d", replicas)
		limits := map[string]JetStreamAccountLimits{tier: limit}

		var s *Server
		var c *cluster
		if replicas == 1 {
			s = RunBasicJetStreamServer(t)
			defer s.Shutdown()
			require_NoError(t, s.globalAccount().UpdateJetStreamLimits(limits))
		} else {
			c = createJetStreamClusterExplicit(t, "R3S", 3)
			defer c.shutdown()
			for _, cs := range c.servers {
				require_NoError(t, cs.globalAccount().UpdateJetStreamLimits(limits))
			}
			s = c.randomServer()
		}

		resourcesErr := NewJSStorageResourcesExceededError()
		maxAcc, maxBytes := limit.MaxStore, limit.StoreMaxStreamBytes
		if storage == nats.MemoryStorage {
			resourcesErr = NewJSMemoryResourcesExceededError()
			maxAcc, maxBytes = limit.MaxMemory, limit.MemoryMaxStreamBytes
		}

		nc, js := jsClientConnect(t, s)
		defer nc.Close()

		// No MaxBytes errors because it's required.
		_, err := js.AddStream(&nats.StreamConfig{Name: "A", Replicas: replicas, Storage: storage})
		require_Error(t, err, NewJSStreamMaxBytesRequiredError())

		// MaxBytes larger than account limit errors.
		_, err = js.AddStream(&nats.StreamConfig{Name: "A", Replicas: replicas, Storage: storage, MaxBytes: maxAcc + 1})
		if replicas == 1 {
			require_Error(t, err, resourcesErr)
		} else {
			require_Error(t, err, NewJSStreamMaxStreamBytesExceededError())
		}

		// MaxBytes larger than bytes limit errors.
		_, err = js.AddStream(&nats.StreamConfig{Name: "A", Replicas: replicas, Storage: storage, MaxBytes: maxBytes + 1})
		require_Error(t, err, NewJSStreamMaxStreamBytesExceededError())

		// Create two streams that exactly fit the limit.
		for _, subj := range []string{"A", "B"} {
			_, err = js.AddStream(&nats.StreamConfig{Name: subj, Replicas: replicas, Storage: storage, MaxBytes: maxBytes})
			require_NoError(t, err)
		}

		// Another stream over the limit errors.
		_, err = js.AddStream(&nats.StreamConfig{Name: "C", Replicas: replicas, Storage: storage, MaxBytes: 1})
		require_Error(t, err, resourcesErr)

		// We can publish more than the maximum bytes into the stream as it is DiscardOld.
		for range 10 {
			_, err = js.Publish("A", nil)
			require_NoError(t, err)
		}

		// Once the last stream fills up too close to the account limit, we eventually get an error.
		start := time.Now()
		for i := 0; ; i++ {
			if time.Since(start) > 5*time.Second {
				t.Fatalf("timed out waiting for error")
			}
			_, err = js.Publish("B", nil)
			if i < 3 {
				require_NoError(t, err)
			} else if replicas == 1 {
				// Expect an error immediately after we hit the limit if we're R1.
				require_Error(t, err, NewJSAccountResourcesExceededError())
				break
			} else if err != nil {
				// For replicated this might take some time as servers report about their usage.
				require_Error(t, err, NewJSAccountResourcesExceededError())
				break
			}
			time.Sleep(50 * time.Millisecond)
		}

		// If clustered, confirm all is in sync still.
		if c != nil {
			checkFor(t, 2*time.Second, 200*time.Millisecond, func() error {
				return checkState(t, c, globalAccountName, "B")
			})
		}
	}

	for _, replicas := range []int{1, 3} {
		t.Run(fmt.Sprintf("R%d", replicas), func(t *testing.T) {
			t.Run("Memory", func(t *testing.T) { test(t, replicas, nats.MemoryStorage) })
			t.Run("File", func(t *testing.T) { test(t, replicas, nats.FileStorage) })
		})
	}
}

func TestJetStreamClusterDontEncodeConsumerStateInMetaSnapshot(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "R3S", 3)
	defer c.shutdown()

	nc, js := jsClientConnect(t, c.randomServer())
	defer nc.Close()

	// Add a replicated stream and a single replica consumer.
	scfg := &nats.StreamConfig{Name: "TEST", Replicas: 3}
	_, err := js.AddStream(scfg)
	require_NoError(t, err)
	_, err = js.AddConsumer("TEST", &nats.ConsumerConfig{Name: "CONSUMER", Replicas: 1})
	require_NoError(t, err)

	// Ensure the stream and consumer leaders differ.
	sl := c.streamLeader(globalAccountName, "TEST")
	cl := c.consumerLeader(globalAccountName, "TEST", "CONSUMER")
	if sl == cl {
		mset, err := sl.globalAccount().lookupStream("TEST")
		require_NoError(t, err)
		require_NoError(t, mset.raftNode().StepDown())
		c.waitOnStreamLeader(globalAccountName, "TEST")
		sl = c.streamLeader(globalAccountName, "TEST")
	}
	require_NotEqual(t, sl, cl)

	// Scale down the stream so the R1 consumer needs to be moved to a different server.
	scfg.Replicas = 1
	_, err = js.UpdateStream(scfg)
	require_NoError(t, err)

	// Signal that the meta leader should create a snapshot. We need to do this indirectly
	// through a noop peer change, as we need the monitor goroutine to perform the snapshot.
	ml := c.leader()
	require_NotNil(t, ml)
	meta := ml.getJetStream().getMetaGroup().(*raft)
	meta.RLock()
	papplied := meta.papplied
	meta.RUnlock()
	require_NoError(t, meta.ProposeAddPeer("_random_"))
	checkFor(t, 2*time.Second, 200*time.Millisecond, func() error {
		meta.RLock()
		defer meta.RUnlock()
		if papplied == meta.papplied {
			return errors.New("no snapshot yet")
		}
		return nil
	})

	// Load the new snapshot and validate consumer state isn't preserved.
	snap, err := meta.loadLastSnapshot()
	require_NoError(t, err)
	sjs := ml.getJetStream()
	accStreams, err := sjs.decodeMetaSnapshot(snap.data)
	require_NoError(t, err)
	require_Len(t, len(accStreams), 1)
	streams := accStreams[globalAccountName]
	require_Len(t, len(streams), 1)
	stream := streams["TEST"]
	require_NotNil(t, stream)
	require_Len(t, len(stream.consumers), 1)
	consumer := stream.consumers["CONSUMER"]
	require_NotNil(t, consumer)
	require_True(t, consumer.State == nil)
}

func TestJetStreamClusteredStreamCreateIdempotentWithSources(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "JSC", 3)
	defer c.shutdown()

	nc, js := jsClientConnect(t, c.randomServer())
	defer nc.Close()

	_, err := js.AddStream(&nats.StreamConfig{
		Name:     "SOURCE",
		Subjects: []string{"source.>"},
		Replicas: 3,
	})
	require_NoError(t, err)

	cfg := &nats.StreamConfig{
		Name:     "SOURCED",
		Subjects: []string{"sourced.>"},
		Replicas: 3,
		Sources: []*nats.StreamSource{
			{
				Name:          "SOURCE",
				FilterSubject: "source.>",
			},
		},
	}
	_, err = js.AddStream(cfg)
	require_NoError(t, err)

	// The leadership transfer is best-effort and another peer could win the election, so retry.
	stepDownTo := func(target *Server) {
		t.Helper()
		checkFor(t, 10*time.Second, 100*time.Millisecond, func() error {
			c.waitOnStreamLeader(globalAccountName, "SOURCED")
			sl := c.streamLeader(globalAccountName, "SOURCED")
			if sl == nil {
				return errors.New("no stream leader")
			} else if sl == target {
				return nil
			}
			mset, err := sl.globalAccount().lookupStream("SOURCED")
			require_NoError(t, err)
			// The stream leader could have changed in the meantime.
			if err = mset.raftNode().StepDown(target.Node()); err != nil && err != errNotLeader {
				return err
			}
			return fmt.Errorf("stream leader is %s, expected %s", sl.Name(), target.Name())
		})
	}

	// Move the stream leader across all servers, ending on the meta leader.
	// This ensures all stored assignments have iname populated via the shared StreamSource pointer.
	ml := c.leader()
	require_NotNil(t, ml)
	for _, s := range c.servers {
		if s != ml {
			stepDownTo(s)
		}
	}
	stepDownTo(ml)

	for _, s := range c.servers {
		mset, err := s.globalAccount().lookupStream("SOURCED")
		require_NoError(t, err)
		sjs := s.getJetStream()
		sjs.mu.RLock()
		iname := mset.streamAssignment().Config.Sources[0].iname
		sjs.mu.RUnlock()
		require_NotEqual(t, iname, _EMPTY_)
	}

	// The second create should be idempotent, and succeed even though iname was set.
	_, err = js.AddStream(cfg)
	require_NoError(t, err)
}

func TestJetStreamClusterMetaSnapshotPreservesConsumersOnStreamUpdate(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "R3S", 3)
	defer c.shutdown()

	nc, js := jsClientConnect(t, c.randomServer())
	defer nc.Close()

	cfg := &nats.StreamConfig{Name: "TEST", Subjects: []string{"foo"}, Replicas: 3}
	_, err := js.AddStream(cfg)
	require_NoError(t, err)
	_, err = js.AddConsumer("TEST", &nats.ConsumerConfig{Name: "CONSUMER", Replicas: 3})
	require_NoError(t, err)

	var metaRemove bool
	triggerMetaSnapshot := func(t *testing.T, c *cluster) {
		t.Helper()
		ml := c.leader()
		require_NotNil(t, ml)
		meta := ml.getJetStream().getMetaGroup().(*raft)
		meta.RLock()
		papplied := meta.papplied
		meta.RUnlock()
		if metaRemove {
			require_NoError(t, meta.ProposeRemovePeer("_random_"))
		} else {
			require_NoError(t, meta.ProposeAddPeer("_random_"))
		}
		metaRemove = !metaRemove
		checkFor(t, 2*time.Second, 200*time.Millisecond, func() error {
			meta.RLock()
			defer meta.RUnlock()
			if papplied == meta.papplied {
				return errors.New("no snapshot yet")
			}
			return nil
		})
	}

	loadSnapshotStreams := func(t *testing.T, c *cluster) map[string]map[string]*streamAssignment {
		t.Helper()
		ml := c.leader()
		require_NotNil(t, ml)
		meta := ml.getJetStream().getMetaGroup().(*raft)
		snap, err := meta.loadLastSnapshot()
		require_NoError(t, err)
		sjs := ml.getJetStream()
		accStreams, err := sjs.decodeMetaSnapshot(snap.data)
		require_NoError(t, err)
		return accStreams
	}

	// Create a baseline snapshot that includes consumers.
	triggerMetaSnapshot(t, c)
	accStreams := loadSnapshotStreams(t, c)
	stream := accStreams[globalAccountName]["TEST"]
	require_NotNil(t, stream)
	require_NotNil(t, stream.consumers["CONSUMER"])

	// Update stream config, then create a new snapshot from the previous checkpoint.
	cfg.Description = "updated"
	_, err = js.UpdateStream(cfg)
	require_NoError(t, err)
	triggerMetaSnapshot(t, c)

	// Updated snapshots must preserve existing stream consumers.
	accStreams = loadSnapshotStreams(t, c)
	stream = accStreams[globalAccountName]["TEST"]
	require_NotNil(t, stream)
	require_NotNil(t, stream.consumers["CONSUMER"])
}

func TestJetStreamClusterCheckForOrphansDoesntDeleteDirectConsumers(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "R3S", 3)
	defer c.shutdown()

	nc, js := jsClientConnect(t, c.randomServer())
	defer nc.Close()

	_, err := js.AddStream(&nats.StreamConfig{Name: "TEST", Subjects: []string{"foo"}})
	require_NoError(t, err)
	_, err = js.AddStream(&nats.StreamConfig{Name: "MIRROR", Mirror: &nats.StreamSource{Name: "TEST"}})
	require_NoError(t, err)

	sl := c.streamLeader(globalAccountName, "TEST")
	require_NotNil(t, sl)
	mset, err := sl.globalAccount().lookupStream("TEST")
	require_NoError(t, err)
	checkFor(t, 2*time.Second, 200*time.Millisecond, func() error {
		if c := mset.numDirectConsumers(); c != 1 {
			return fmt.Errorf("expected 1 consumer, got %d", c)
		}
		return nil
	})

	require_Equal(t, mset.numDirectConsumers(), 1)
	sl.getJetStream().checkForOrphans()
	require_Equal(t, mset.numDirectConsumers(), 1)
}

func TestJetStreamClusterConsumerAssignmentsOrInflightSeqWithInflightStream(t *testing.T) {
	const acc, stream, consumer = "A", "S", "C"
	js := &jetStream{cluster: &jetStreamCluster{
		streams: map[string]map[string]*streamAssignment{},
		inflightStreams: map[string]map[string]*inflightStreamInfo{
			acc: {stream: {streamAssignment: &streamAssignment{Config: &StreamConfig{Name: stream}}}},
		},
		inflightConsumers: map[string]map[string]map[string]*inflightConsumerInfo{
			acc: {stream: {consumer: {consumerAssignment: &consumerAssignment{Name: consumer}}}},
		},
	}}

	var got []string
	for ca := range js.consumerAssignmentsOrInflightSeq(acc, stream) {
		got = append(got, ca.Name)
	}
	if len(got) != 1 || got[0] != consumer {
		t.Fatalf("Unexpected consumers: %+v", got)
	}
}

func TestJetStreamClusterStreamConfigConsidersInflight(t *testing.T) {
	const acc, stream = "A", "S"
	appliedCfg := &StreamConfig{Name: stream, Replicas: 1}
	inflightCfg := &StreamConfig{Name: stream, Replicas: 3}

	js := &jetStream{cluster: &jetStreamCluster{}}
	expectCfg := func(expected *StreamConfig) {
		t.Helper()
		cfg, ok := js.clusterStreamConfig(acc, stream)
		require_Equal(t, ok, expected != nil)
		if expected != nil {
			require_True(t, reflect.DeepEqual(cfg, *expected))
		}
	}

	// Unknown stream.
	expectCfg(nil)

	// A stream create that has been proposed but not applied yet must already be
	// visible, so a consumer create for it that races ahead of the meta apply
	// loop is not rejected with 'stream not found'.
	js.cluster.inflightStreams = map[string]map[string]*inflightStreamInfo{
		acc: {stream: {streamAssignment: &streamAssignment{Config: inflightCfg}}},
	}
	expectCfg(inflightCfg)

	// The inflight proposal is more recent than an applied assignment.
	js.cluster.streams = map[string]map[string]*streamAssignment{
		acc: {stream: {Config: appliedCfg}},
	}
	expectCfg(inflightCfg)

	// An inflight delete means the stream is going away, even if the applied
	// assignment still exists.
	js.cluster.inflightStreams[acc][stream].deleted = true
	expectCfg(nil)

	// Only an applied assignment remains.
	js.cluster.inflightStreams = nil
	expectCfg(appliedCfg)
}

func TestJetStreamClusterApiPagedRequestOffsetValidation(t *testing.T) {
	test := func(t *testing.T, replicas int) {
		var s *Server
		if replicas == 1 {
			s = RunBasicJetStreamServer(t)
			defer s.Shutdown()
		} else {
			c := createJetStreamClusterExplicit(t, "JSC", 3)
			defer c.shutdown()
			s = c.randomServer()
		}

		nc, js := jsClientConnect(t, s)
		defer nc.Close()

		_, err := js.AddStream(&nats.StreamConfig{Name: "TEST", Subjects: []string{"foo"}, Replicas: replicas})
		require_NoError(t, err)
		_, err = js.Publish("foo", nil)
		require_NoError(t, err)
		_, err = js.AddConsumer("TEST", &nats.ConsumerConfig{Name: "CONSUMER", Replicas: replicas})
		require_NoError(t, err)

		paged := ApiPagedRequest{Offset: -1}

		t.Run("StreamNames", func(t *testing.T) {
			req := JSApiStreamNamesRequest{ApiPagedRequest: paged}
			b, err := json.Marshal(req)
			require_NoError(t, err)
			rmsg, err := nc.Request(JSApiStreams, b, time.Second)
			require_NoError(t, err)
			var resp JSApiStreamNamesResponse
			require_NoError(t, json.Unmarshal(rmsg.Data, &resp))
			require_Equal(t, resp.Offset, 0)
			require_Equal(t, resp.Total, 1)
			require_Len(t, len(resp.Streams), 1)
		})

		t.Run("StreamList", func(t *testing.T) {
			req := JSApiStreamListRequest{ApiPagedRequest: paged}
			b, err := json.Marshal(req)
			require_NoError(t, err)
			rmsg, err := nc.Request(JSApiStreamList, b, time.Second)
			require_NoError(t, err)
			var resp JSApiStreamListResponse
			require_NoError(t, json.Unmarshal(rmsg.Data, &resp))
			require_Equal(t, resp.Offset, 0)
			require_Equal(t, resp.Total, 1)
			require_Len(t, len(resp.Streams), 1)
		})

		t.Run("StreamInfo", func(t *testing.T) {
			req := JSApiStreamInfoRequest{ApiPagedRequest: paged, SubjectsFilter: ">"}
			b, err := json.Marshal(req)
			require_NoError(t, err)
			rmsg, err := nc.Request(fmt.Sprintf(JSApiStreamInfoT, "TEST"), b, time.Second)
			require_NoError(t, err)
			var resp JSApiStreamInfoResponse
			require_NoError(t, json.Unmarshal(rmsg.Data, &resp))
			require_Equal(t, resp.Offset, 0)
			require_Equal(t, resp.Total, 1)
			require_Len(t, len(resp.StreamInfo.State.Subjects), 1)
		})

		t.Run("ConsumerNames", func(t *testing.T) {
			req := JSApiConsumersRequest{ApiPagedRequest: paged}
			b, err := json.Marshal(req)
			require_NoError(t, err)
			rmsg, err := nc.Request(fmt.Sprintf(JSApiConsumersT, "TEST"), b, time.Second)
			require_NoError(t, err)
			var resp JSApiConsumerNamesResponse
			require_NoError(t, json.Unmarshal(rmsg.Data, &resp))
			require_Equal(t, resp.Offset, 0)
			require_Equal(t, resp.Total, 1)
			require_Len(t, len(resp.Consumers), 1)
		})

		t.Run("ConsumerList", func(t *testing.T) {
			req := JSApiConsumersRequest{ApiPagedRequest: paged}
			b, err := json.Marshal(req)
			require_NoError(t, err)
			rmsg, err := nc.Request(fmt.Sprintf(JSApiConsumerListT, "TEST"), b, time.Second)
			require_NoError(t, err)
			var resp JSApiConsumerListResponse
			require_NoError(t, json.Unmarshal(rmsg.Data, &resp))
			require_Equal(t, resp.Offset, 0)
			require_Equal(t, resp.Total, 1)
			require_Len(t, len(resp.Consumers), 1)
		})
	}
	for _, replicas := range []int{1, 3} {
		t.Run(fmt.Sprintf("R%d", replicas), func(t *testing.T) { test(t, replicas) })
	}
}

func TestJetStreamClusterStreamRestoreNameMismatch(t *testing.T) {
	test := func(t *testing.T, replicas int) {
		var s *Server
		if replicas == 1 {
			s = RunBasicJetStreamServer(t)
			defer s.Shutdown()
		} else {
			c := createJetStreamClusterExplicit(t, "R3S", 3)
			defer c.shutdown()
			s = c.randomServer()
		}

		nc, js := jsClientConnect(t, s)
		defer nc.Close()

		_, err := js.AddStream(&nats.StreamConfig{Name: "EXISTS", Subjects: []string{"foo"}, Replicas: replicas})
		require_NoError(t, err)

		b := []byte(`{"config":{}}`)
		rmsg, err := nc.Request(fmt.Sprintf(JSApiStreamRestoreT, "EXISTS"), b, time.Second)
		require_NoError(t, err)
		var resp JSApiStreamRestoreResponse
		require_NoError(t, json.Unmarshal(rmsg.Data, &resp))
		require_True(t, resp.Error != nil)
		require_Error(t, resp.Error, NewJSStreamNameExistRestoreFailedError())

		b = []byte(`{"config":{"name":"RANDOM"}}`)
		rmsg, err = nc.Request(fmt.Sprintf(JSApiStreamRestoreT, "TEST"), b, time.Second)
		require_NoError(t, err)
		resp = JSApiStreamRestoreResponse{}
		require_NoError(t, json.Unmarshal(rmsg.Data, &resp))
		require_True(t, resp.Error != nil)
		require_Error(t, resp.Error, NewJSStreamMismatchError())
	}
	for _, replicas := range []int{1, 3} {
		t.Run(fmt.Sprintf("R%d", replicas), func(t *testing.T) { test(t, replicas) })
	}
}

// https://github.com/nats-io/nats-server/issues/8687
func TestJetStreamClusterStreamRestoreStallPublishesAdvisory(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "R3S", 3)
	defer c.shutdown()

	nc, js := jsClientConnect(t, c.randomServer())
	defer nc.Close()

	_, err := js.AddStream(&nats.StreamConfig{Name: "TEST", Subjects: []string{"foo"}, Replicas: 3})
	require_NoError(t, err)
	for i := 0; i < 100; i++ {
		sendStreamMsg(t, nc, "foo", "msg")
	}
	sc, ss, snapshot := performStreamBackup(t, nc, "TEST")
	require_NoError(t, js.DeleteStream("TEST"))

	sub, err := nc.SubscribeSync(JSAdvisoryStreamRestoreCompletePre + ".TEST")
	require_NoError(t, err)
	defer sub.Unsubscribe()

	// Send the whole archive, but never the empty chunk that ends the transfer.
	req, err := json.Marshal(struct {
		StreamState  `json:"state"`
		StreamConfig `json:"config"`
	}{StreamState: ss, StreamConfig: sc})
	require_NoError(t, err)
	rmsg, err := nc.Request(fmt.Sprintf(JSApiStreamRestoreT, "TEST"), req, 5*time.Second)
	require_NoError(t, err)
	var resp JSApiStreamRestoreResponse
	require_NoError(t, json.Unmarshal(rmsg.Data, &resp))
	require_True(t, resp.Error == nil)
	const chunk = 64 * 1024
	for i := 0; i < len(snapshot); i += chunk {
		_, err = nc.Request(resp.DeliverSubject, snapshot[i:min(i+chunk, len(snapshot))], time.Second)
		require_NoError(t, err)
	}

	// The transfer stalls, and the restore is reported like any other failed restore.
	msg, err := sub.NextMsg(15 * time.Second)
	require_NoError(t, err)
	var adv JSRestoreCompleteAdvisory
	require_NoError(t, json.Unmarshal(msg.Data, &adv))
	require_Equal(t, adv.Stream, "TEST")
	require_Equal(t, adv.Bytes, int64(len(snapshot)))
}

func TestJetStreamClusterRemoveStatusHeaderOnStreamInbound(t *testing.T) {
	test := func(t *testing.T, replicas int) {
		var s *Server
		if replicas == 1 {
			s = RunBasicJetStreamServer(t)
			defer s.Shutdown()
		} else {
			c := createJetStreamClusterExplicit(t, "R3S", 3)
			defer c.shutdown()
			s = c.randomServer()
		}

		nc, js := jsClientConnect(t, s)
		defer nc.Close()

		_, err := js.AddStream(&nats.StreamConfig{Name: "TEST", Subjects: []string{"foo"}, Replicas: replicas})
		require_NoError(t, err)

		hdr := []byte("NATS/1.0 100 Description\r\n\r\n")
		a := s.globalAccount()
		err = s.sendInternalAccountMsgWithReply(a, "foo", "reply", hdr, nil, false)
		require_NoError(t, err)

		checkFor(t, 2*time.Second, 200*time.Millisecond, func() error {
			mset, err := a.lookupStream("TEST")
			if err != nil {
				return err
			}
			sm, err := mset.store.LoadMsg(1, nil)
			if err != nil {
				return err
			} else if len(sm.hdr) != 0 {
				return fmt.Errorf("expected empty header, got %d bytes", len(sm.hdr))
			}
			return nil
		})
	}
	for _, replicas := range []int{1, 3} {
		t.Run(fmt.Sprintf("R%d", replicas), func(t *testing.T) { test(t, replicas) })
	}
}

func TestJetStreamClusterStreamUpdateCombinedScaleUpWithSubjectsChange(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "R3S", 3)
	defer c.shutdown()

	nc, js := jsClientConnect(t, c.randomServer())
	defer nc.Close()

	cfg := &nats.StreamConfig{
		Name:     "TEST",
		Subjects: []string{"a"},
	}
	_, err := js.AddStream(cfg)
	require_NoError(t, err)

	_, err = js.Publish("a", nil)
	require_NoError(t, err)

	cfg.Replicas = 3
	cfg.Subjects = append(cfg.Subjects, "b")
	_, err = js.UpdateStream(cfg)
	require_NoError(t, err)

	checkFor(t, 2*time.Second, 200*time.Millisecond, func() error {
		_, err = js.Publish("b", nil, nats.AckWait(200*time.Millisecond))
		if err != nil {
			return err
		}
		return nil
	})
}

func TestJetStreamClusterStreamUpdateCombinedScaleDownWithSubjectsChange(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "R3S", 3)
	defer c.shutdown()

	nc, js := jsClientConnect(t, c.randomServer())
	defer nc.Close()

	cfg := &nats.StreamConfig{
		Name:     "TEST",
		Replicas: 3,
		Subjects: []string{"a"},
	}
	_, err := js.AddStream(cfg)
	require_NoError(t, err)

	_, err = js.Publish("a", nil)
	require_NoError(t, err)

	// Make sure the current stream leader doesn't respond to cluster stream info requests.
	// This allows the scale down to not (always) select the correct server to scale down to.
	sl := c.streamLeader(globalAccountName, "TEST")
	require_NotNil(t, sl)
	mset, err := sl.globalAccount().lookupStream("TEST")
	require_NoError(t, err)
	mset.mu.Lock()
	if mset.infoSub == nil {
		mset.mu.Unlock()
		t.Fatal("infoSub is nil")
	}
	mset.srv.sysUnsubscribe(mset.infoSub)
	mset.infoSub = nil
	mset.mu.Unlock()

	// If a different server gets picked than the current stream leader, it needs to
	// subscribe to all subjects to be able to respond.
	cfg.Replicas = 1
	cfg.Subjects = append(cfg.Subjects, "b")
	_, err = js.UpdateStream(cfg)
	require_NoError(t, err)

	checkFor(t, 2*time.Second, 200*time.Millisecond, func() error {
		_, err = js.Publish("b", nil, nats.AckWait(200*time.Millisecond))
		if err != nil {
			return err
		}
		return nil
	})
}

func TestJetStreamClusterStreamUpdateCombinedScaleDownWithSourcesRemoved(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "R3S", 3)
	defer c.shutdown()

	nc, js := jsClientConnect(t, c.randomServer())
	defer nc.Close()

	cfg := &nats.StreamConfig{
		Name:     "TEST",
		Replicas: 3,
		Subjects: []string{"a"},
		Sources: []*nats.StreamSource{
			{Name: "SOURCE"},
		},
	}
	si, err := js.AddStream(cfg)
	require_NoError(t, err)
	require_Len(t, len(si.Sources), 1)

	_, err = js.Publish("a", nil)
	require_NoError(t, err)

	// Stepdown the leader a couple of times, to increase the likelihood of the leader
	// after scaledown to have been leader once due to this.
	for range 3 {
		sl := c.streamLeader(globalAccountName, "TEST")
		require_NotNil(t, sl)
		require_NoError(t, sl.JetStreamStepdownStream(globalAccountName, "TEST"))
		c.waitOnStreamLeader(globalAccountName, "TEST")
	}

	// Make sure the current stream leader doesn't respond to cluster stream info requests.
	// This allows the scale down to not (always) select the correct server to scale down to.
	sl := c.streamLeader(globalAccountName, "TEST")
	require_NotNil(t, sl)
	mset, err := sl.globalAccount().lookupStream("TEST")
	require_NoError(t, err)
	mset.mu.Lock()
	if mset.infoSub == nil {
		mset.mu.Unlock()
		t.Fatal("infoSub is nil")
	}
	mset.srv.sysUnsubscribe(mset.infoSub)
	mset.infoSub = nil
	mset.mu.Unlock()

	// After scaledown with sources removed, should not report sources in the stream info anymore.
	cfg.Replicas = 1
	cfg.Sources = nil
	si, err = js.UpdateStream(cfg)
	require_NoError(t, err)
	require_Len(t, len(si.Sources), 0)
}

func TestJetStreamClusterRollupWithDiscardNewPerSubject(t *testing.T) {
	test := func(t *testing.T, replicas int, storage nats.StorageType) {
		var s *Server
		if replicas == 1 {
			s = RunBasicJetStreamServer(t)
			defer s.Shutdown()
		} else {
			c := createJetStreamClusterExplicit(t, "R3S", 3)
			defer c.shutdown()
			s = c.randomServer()
		}

		nc, js := jsClientConnect(t, s)
		defer nc.Close()

		_, err := js.AddStream(&nats.StreamConfig{
			Name:                 "TEST",
			Subjects:             []string{"kv.>"},
			Replicas:             replicas,
			Storage:              storage,
			Discard:              nats.DiscardNew,
			DiscardNewPerSubject: true,
			MaxMsgsPerSubject:    1,
			AllowRollup:          true,
		})
		require_NoError(t, err)

		// Populate two subjects at their per-subject limit.
		sendStreamMsg(t, nc, "kv.A", "value-A")
		sendStreamMsg(t, nc, "kv.B", "value-B")

		si, err := js.StreamInfo("TEST")
		require_NoError(t, err)
		require_Equal(t, si.State.Msgs, 2)
		require_Equal(t, si.State.FirstSeq, 1)
		require_Equal(t, si.State.LastSeq, 2)

		// Rollup on subject should succeed despite per-subject limit.
		m := nats.NewMsg("kv.A")
		m.Data = []byte("rolled-up-A")
		m.Header.Set(JSMsgRollup, JSMsgRollupSubject)
		_, err = js.PublishMsg(m)
		require_NoError(t, err)

		// Should still have 2 messages: the rollup replaced kv.A, kv.B untouched.
		si, err = js.StreamInfo("TEST")
		require_NoError(t, err)
		require_Equal(t, si.State.Msgs, 2)
		require_Equal(t, si.State.FirstSeq, 2)
		require_Equal(t, si.State.LastSeq, 3)

		// Verify the rollup message is what we stored.
		sub, err := js.SubscribeSync("kv.A")
		require_NoError(t, err)
		defer sub.Drain()
		msg, err := sub.NextMsg(time.Second)
		require_NoError(t, err)
		require_Equal(t, string(msg.Data), "rolled-up-A")

		// Rollup all should also succeed.
		m = nats.NewMsg("kv.A")
		m.Data = []byte("rolled-up-all")
		m.Header.Set(JSMsgRollup, JSMsgRollupAll)
		_, err = js.PublishMsg(m)
		require_NoError(t, err)

		// Only the rollup message should remain.
		si, err = js.StreamInfo("TEST")
		require_NoError(t, err)
		require_Equal(t, si.State.Msgs, 1)
		require_Equal(t, si.State.FirstSeq, 4)
		require_Equal(t, si.State.LastSeq, 4)
	}

	for _, replicas := range []int{1, 3} {
		for _, storage := range []nats.StorageType{nats.FileStorage, nats.MemoryStorage} {
			t.Run(fmt.Sprintf("R%d/%s", replicas, storage), func(t *testing.T) {
				test(t, replicas, storage)
			})
		}
	}
}

func TestJetStreamClusterInterestStreamMsgWithNoInterestStillAppliesRollup(t *testing.T) {
	test := func(t *testing.T, replicas int) {
		c := createJetStreamClusterExplicit(t, "R3S", 3)
		defer c.shutdown()

		nc, js := jsClientConnect(t, c.randomServer())
		defer nc.Close()

		_, err := js.AddStream(&nats.StreamConfig{
			Name:        "TEST",
			Subjects:    []string{"foo", "bar"},
			Replicas:    replicas,
			Retention:   nats.InterestPolicy,
			AllowRollup: true,
		})
		require_NoError(t, err)

		_, err = js.AddConsumer("TEST", &nats.ConsumerConfig{
			Durable:       "CONSUMER",
			FilterSubject: "foo",
		})
		require_NoError(t, err)

		pubAck, err := js.Publish("foo", nil)
		require_NoError(t, err)
		require_Equal(t, pubAck.Sequence, 1)

		// Publishing on a subject without interest should still result in the rollup being performed.
		m := nats.NewMsg("bar")
		m.Header.Set("Nats-Rollup", "all")
		pubAck, err = js.PublishMsg(m)
		require_NoError(t, err)
		require_Equal(t, pubAck.Sequence, 2)

		checkFor(t, 2*time.Second, 200*time.Millisecond, func() error {
			state, err := checkStateAndErr(t, c, globalAccountName, "TEST")
			if err != nil {
				return err
			}
			if state.Msgs != 0 || state.FirstSeq != 3 || state.LastSeq != 2 {
				return fmt.Errorf("invalid state, got %v", state)
			}
			return nil
		})
	}
	for _, replicas := range []int{1, 3} {
		t.Run(fmt.Sprintf("R%d", replicas), func(t *testing.T) { test(t, replicas) })
	}
}

func TestJetStreamClusterSubjectTransformWithExpectedSubjectSequenceHeader(t *testing.T) {
	test := func(t *testing.T, replicas int) {
		var s *Server
		if replicas == 1 {
			s = RunBasicJetStreamServer(t)
			defer s.Shutdown()
		} else {
			c := createJetStreamClusterExplicit(t, "R3S", 3)
			defer c.shutdown()
			s = c.randomServer()
		}

		nc, js := jsClientConnect(t, s)
		defer nc.Close()

		_, err := jsStreamCreate(t, nc, &StreamConfig{
			Name:     "TEST",
			Subjects: []string{"foo", "bar"},
			Replicas: replicas,
			Storage:  FileStorage,
			SubjectTransform: &SubjectTransformConfig{
				Source:      "foo",
				Destination: "bar",
			},
			AllowBatchPublish:  true,
			AllowAtomicPublish: true,
		})
		require_NoError(t, err)

		// We publish on "foo", but it gets mapped to "bar".
		_, err = js.Publish("foo", nil)
		require_NoError(t, err)

		// Check the subject transform worked.
		msg, err := js.GetMsg("TEST", 1)
		require_NoError(t, err)
		require_Equal(t, msg.Subject, "bar")

		inbox := nats.NewInbox()
		sub, err := nc.SubscribeSync(fmt.Sprintf("%s.>", inbox))
		require_NoError(t, err)
		defer sub.Drain()

		// Publishing to either subject should pass consistency checks under the "bar" subject after transform.
		for _, subj := range []string{"bar", "foo"} {
			// Normal publish.
			m := nats.NewMsg(subj)
			m.Header.Set("Nats-Expected-Last-Subject-Sequence", "0")
			_, err = js.PublishMsg(m)
			require_Error(t, err, NewJSStreamWrongLastSequenceError(1))

			// Fast batch publish.
			m.Reply = generateFastBatchReply(inbox, "uuid", 1, 0, FastBatchGapFail, FastBatchOpCommit)
			require_NoError(t, nc.PublishMsg(m))
			rmsg, err := sub.NextMsg(time.Second)
			require_NoError(t, err)
			var pubAck JSPubAckResponse
			require_NoError(t, json.Unmarshal(rmsg.Data, &pubAck))
			require_NotNil(t, pubAck.Error)
			require_Error(t, pubAck.Error, NewJSStreamWrongLastSequenceError(1))

			// Atomic batch publish.
			m.Header.Set("Nats-Expected-Last-Subject-Sequence", "0")
			m.Header.Set("Nats-Batch-Id", "uuid")
			m.Header.Set("Nats-Batch-Sequence", "1")
			m.Header.Set("Nats-Batch-Commit", "1")
			_, err = js.PublishMsg(m)
			require_Error(t, err, NewJSStreamWrongLastSequenceError(1))
		}

		// Fast batch publish, but without consistency checks.
		m := nats.NewMsg("foo")
		m.Reply = generateFastBatchReply(inbox, "uuid", 1, 0, FastBatchGapFail, FastBatchOpCommit)
		require_NoError(t, nc.PublishMsg(m))
		rmsg, err := sub.NextMsg(time.Second)
		require_NoError(t, err)
		var pubAck JSPubAckResponse
		require_NoError(t, json.Unmarshal(rmsg.Data, &pubAck))
		require_True(t, pubAck.Error == nil)
	}
	for _, replicas := range []int{1, 3} {
		t.Run(fmt.Sprintf("R%d", replicas), func(t *testing.T) { test(t, replicas) })
	}
}

func TestJetStreamClusterSubjectTransformDoesntCycle(t *testing.T) {
	test := func(t *testing.T, replicas int) {
		var s *Server
		if replicas == 1 {
			s = RunBasicJetStreamServer(t)
			defer s.Shutdown()
		} else {
			c := createJetStreamClusterExplicit(t, "R3S", 3)
			defer c.shutdown()
			s = c.randomServer()
		}

		nc, js := jsClientConnect(t, s)
		defer nc.Close()

		_, err := jsStreamCreate(t, nc, &StreamConfig{
			Name:     "TEST",
			Subjects: []string{"foo"},
			Replicas: replicas,
			Storage:  FileStorage,
			// Use a subject transform that can cycle if applied multiple times.
			// Applying this transform on X twice would result in dst.X.X.
			// A subject transform must only be applied once, so such a transform is invalid.
			SubjectTransform: &SubjectTransformConfig{
				Source:      ">",
				Destination: "dst.>",
			},
			AllowBatchPublish:  true,
			AllowAtomicPublish: true,
		})
		require_NoError(t, err)

		inbox := nats.NewInbox()
		sub, err := nc.SubscribeSync(fmt.Sprintf("%s.>", inbox))
		require_NoError(t, err)
		defer sub.Drain()

		// Normal publish.
		_, err = js.Publish("foo", nil)
		require_NoError(t, err)
		msg, err := js.GetMsg("TEST", 1)
		require_NoError(t, err)
		require_Equal(t, msg.Subject, "dst.foo")

		// Fast batch publish.
		m := nats.NewMsg("foo")
		m.Reply = generateFastBatchReply(inbox, "uuid", 1, 0, FastBatchGapFail, FastBatchOpCommit)
		require_NoError(t, nc.PublishMsg(m))
		rmsg, err := sub.NextMsg(time.Second)
		require_NoError(t, err)
		var pubAck JSPubAckResponse
		require_NoError(t, json.Unmarshal(rmsg.Data, &pubAck))
		require_True(t, pubAck.Error == nil)
		msg, err = js.GetMsg("TEST", 2)
		require_NoError(t, err)
		require_Equal(t, msg.Subject, "dst.foo")

		// Atomic batch publish.
		m.Header.Set("Nats-Batch-Id", "uuid")
		m.Header.Set("Nats-Batch-Sequence", "1")
		m.Header.Set("Nats-Batch-Commit", "1")
		_, err = js.PublishMsg(m)
		require_NoError(t, err)
		msg, err = js.GetMsg("TEST", 3)
		require_NoError(t, err)
		require_Equal(t, msg.Subject, "dst.foo")

		// Fast batch publish, but without consistency checks.
		m = nats.NewMsg("foo")
		m.Reply = generateFastBatchReply(inbox, "uuid", 1, 0, FastBatchGapFail, FastBatchOpCommit)
		require_NoError(t, nc.PublishMsg(m))
		rmsg, err = sub.NextMsg(time.Second)
		require_NoError(t, err)
		pubAck = JSPubAckResponse{}
		require_NoError(t, json.Unmarshal(rmsg.Data, &pubAck))
		require_True(t, pubAck.Error == nil)
		msg, err = js.GetMsg("TEST", 4)
		require_NoError(t, err)
		require_Equal(t, msg.Subject, "dst.foo")
	}
	for _, replicas := range []int{1, 3} {
		t.Run(fmt.Sprintf("R%d", replicas), func(t *testing.T) { test(t, replicas) })
	}
}

func TestJetStreamClusterStreamLeaderStepsDownIfSnapshotCatchupRequired(t *testing.T) {
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

	// Publish a message and ensure everyone is synced up.
	_, err = js.Publish("foo", nil)
	require_NoError(t, err)
	checkFor(t, 2*time.Second, 200*time.Millisecond, func() error {
		return checkState(t, c, globalAccountName, "TEST")
	})

	// Get the current stream leader.
	sl := c.streamLeader(globalAccountName, "TEST")
	require_NotNil(t, sl)
	mset, err := sl.globalAccount().lookupStream("TEST")
	require_NoError(t, err)
	rn := mset.raftNode()

	// Grab the current state of the leader which contains the message we've published.
	snap := mset.stateSnapshot()
	// Truncate this leader's store to be empty, while remaining Raft leader.
	require_NoError(t, mset.store.Truncate(0))
	// Send the snapshot containing the message. Even though we're Raft leader, we must
	// still check we're up-to-date and if not: step down and catch up.
	require_NoError(t, rn.SendSnapshot(snap))
	checkFor(t, 10*time.Second, 200*time.Millisecond, func() error {
		return checkState(t, c, globalAccountName, "TEST")
	})
}

func TestJetStreamClusterStreamMoveCatchupStallKeepsPeerTracked(t *testing.T) {
	// Shorten the catchup inactivity timer so the stall fires in ~2s instead of 30s.
	streamCatchupActivityInterval = 2 * time.Second
	t.Cleanup(func() {
		streamCatchupActivityInterval = defaultStreamCatchupActivityInterval
	})

	c := createJetStreamClusterExplicit(t, "MOVE", 3)
	defer c.shutdown()

	nc, js := jsClientConnect(t, c.randomServer())
	defer nc.Close()

	_, err := js.AddStream(&nats.StreamConfig{Name: "TEST", Subjects: []string{"foo"}, Replicas: 3})
	require_NoError(t, err)
	for range 20 {
		_, err = js.Publish("foo", []byte("x"))
		require_NoError(t, err)
	}

	sl := c.streamLeader(globalAccountName, "TEST")
	require_NotNil(t, sl)
	mset, err := sl.globalAccount().lookupStream("TEST")
	require_NoError(t, err)

	// Force the leader's outbound catchup budget to a single byte.
	sl.gcbMu.Lock()
	sl.gcbOutMax = 1
	sl.gcbMu.Unlock()

	// Stand-in for a target-cluster peer still pulling the data mid-move. checkClusterInfo
	// keys catchup lag by getHash(replicaName), so key the sync request the same way.
	const peerName = "MOVE-newpeer"
	peer := getHash(peerName)
	sreq := &streamSyncRequest{Peer: peer, FirstSeq: 1, LastSeq: 20, MinApplied: 0}

	// Serve the catchup to a reply subject that nobody acks.
	done := make(chan struct{})
	launch := time.Now()
	started := sl.startGoRoutine(func() {
		mset.runCatchup("test.catchup.reply", sreq)
		close(done)
	})
	require_True(t, started)

	checkFor(t, 2*time.Second, 200*time.Millisecond, func() error {
		if mset.lagForCatchupPeer(peer) == 0 {
			return fmt.Errorf("catchup not armed yet")
		}
		return nil
	})
	ciDuring := &ClusterInfo{Replicas: []*PeerInfo{{Name: peerName, Peer: peer, Current: true}}}
	mset.checkClusterInfo(ciDuring)
	if ciDuring.Replicas[0].Current {
		t.Fatalf("precondition failed: peer should be reported NOT current while catching up")
	}

	// Let the catchup stall. runCatchup returns when its notActive timer fires.
	select {
	case <-done:
	case <-time.After(streamCatchupActivityInterval + 5*time.Second):
		t.Fatalf("runCatchup did not return; expected an inactivity stall")
	}
	if elapsed := time.Since(launch); elapsed < streamCatchupActivityInterval/2 {
		t.Fatalf("catchup returned after %v; expected it to STALL (~%v), not complete", elapsed, streamCatchupActivityInterval)
	}

	// After stalling, the peer should remain tracked.
	lag := mset.lagForCatchupPeer(peer)
	ciAfter := &ClusterInfo{Replicas: []*PeerInfo{{Name: peerName, Peer: peer, Current: true}}}
	mset.checkClusterInfo(ciAfter)

	if lag == 0 {
		t.Fatalf("BUG reproduced: a transient catchup stall cleared the tracked lag; the " +
			"still-catching-up peer is now treated as caught up (clearCatchupPeer on stall)")
	}
	if ciAfter.Replicas[0].Current {
		t.Fatalf("BUG reproduced: migration gate reports a still-catching-up peer as Current after a stall")
	}
}

func TestJetStreamClusterDurableStreamMirror(t *testing.T) {
	test := func(t *testing.T, replicas int, retention RetentionPolicy) {
		var s *Server
		if replicas == 1 {
			s = RunBasicJetStreamServer(t)
			defer s.Shutdown()
		} else {
			c := createJetStreamClusterExplicit(t, "R3S", 3)
			defer c.shutdown()
			s = c.randomServer()
		}

		nc, js := jsClientConnect(t, s)
		defer nc.Close()

		_, err := jsStreamCreate(t, nc, &StreamConfig{
			Name:      "O",
			Subjects:  []string{"foo"},
			Storage:   FileStorage,
			Replicas:  replicas,
			Retention: retention,
		})
		require_NoError(t, err)

		_, err = jsConsumerCreate(t, nc, "O", ConsumerConfig{
			Durable:        "C",
			DeliverSubject: "deliver-subject",
			Replicas:       replicas,
			AckPolicy:      AckFlowControl,
			Heartbeat:      time.Second,
		}, false)
		require_NoError(t, err)

		pubAck, err := js.Publish("foo", nil)
		require_NoError(t, err)
		require_Equal(t, pubAck.Sequence, 1)

		_, err = jsStreamCreate(t, nc, &StreamConfig{
			Name: "M",
			Mirror: &StreamSource{
				Name: "O",
				Consumer: &StreamConsumerSource{
					Name:           "C",
					DeliverSubject: "deliver-subject",
				},
			},
			Storage:  FileStorage,
			Replicas: replicas,
		})
		require_NoError(t, err)

		checkFor(t, 2*time.Second, 200*time.Millisecond, func() error {
			si, err := js.StreamInfo("M")
			if err != nil {
				return err
			}
			if si.Mirror == nil {
				return errors.New("no mirror")
			}
			if si.Mirror.Error != nil {
				return si.Mirror.Error
			}
			_, err = js.GetMsg("M", 1)
			return err
		})
	}

	for _, replicas := range []int{1, 3} {
		for _, retention := range []RetentionPolicy{LimitsPolicy, InterestPolicy, WorkQueuePolicy} {
			t.Run(fmt.Sprintf("R%d/%s", replicas, retention), func(t *testing.T) {
				test(t, replicas, retention)
			})
		}
	}
}

func TestJetStreamClusterDurableStreamMirrorServerManaged(t *testing.T) {
	test := func(t *testing.T, replicas int, retention RetentionPolicy) {
		var s *Server
		if replicas == 1 {
			s = RunBasicJetStreamServer(t)
			defer s.Shutdown()
		} else {
			c := createJetStreamClusterExplicit(t, "R3S", 3)
			defer c.shutdown()
			s = c.randomServer()
		}

		nc, js := jsClientConnect(t, s)
		defer nc.Close()

		_, err := jsStreamCreate(t, nc, &StreamConfig{
			Name:      "O",
			Subjects:  []string{"foo"},
			Storage:   FileStorage,
			Replicas:  replicas,
			Retention: retention,
		})
		require_NoError(t, err)

		_, err = js.AddConsumer("O", &nats.ConsumerConfig{Durable: "C", AckPolicy: nats.AckExplicitPolicy})
		require_NoError(t, err)

		pubAck, err := js.Publish("foo", nil)
		require_NoError(t, err)
		require_Equal(t, pubAck.Sequence, 1)

		_, err = jsStreamCreate(t, nc, &StreamConfig{
			Name:     "M",
			Mirror:   &StreamSource{Name: "O"},
			Storage:  FileStorage,
			Replicas: replicas,
		})
		require_NoError(t, err)

		checkFor(t, 2*time.Second, 200*time.Millisecond, func() error {
			si, err := js.StreamInfo("M")
			if err != nil {
				return err
			}
			if si.Mirror == nil {
				return errors.New("no mirror")
			}
			if si.Mirror.Error != nil {
				return si.Mirror.Error
			}
			_, err = js.GetMsg("M", 1)
			return err
		})
	}

	for _, replicas := range []int{1, 3} {
		for _, retention := range []RetentionPolicy{LimitsPolicy, InterestPolicy, WorkQueuePolicy} {
			t.Run(fmt.Sprintf("R%d/%s", replicas, retention), func(t *testing.T) {
				test(t, replicas, retention)
			})
		}
	}
}

func TestJetStreamDurableProbeShortCircuitsBackoff(t *testing.T) {
	for _, mirror := range []bool{false, true} {
		kind := "Source"
		if mirror {
			kind = "Mirror"
		}
		t.Run(kind, func(t *testing.T) {
			// Make every request time out, so we always end up backing off.
			owt := srcDurableConsumerWaitTime
			srcDurableConsumerWaitTime = time.Nanosecond
			defer func() { srcDurableConsumerWaitTime = owt }()

			s := RunBasicJetStreamServer(t)
			defer s.Shutdown()

			nc, _ := jsClientConnect(t, s)
			defer nc.Close()

			_, err := jsStreamCreate(t, nc, &StreamConfig{
				Name:     "O",
				Subjects: []string{"foo"},
				Storage:  FileStorage,
			})
			require_NoError(t, err)
			_, err = jsConsumerCreate(t, nc, "O", ConsumerConfig{
				Durable:        "C",
				DeliverSubject: "d",
				AckPolicy:      AckFlowControl,
				Heartbeat:      time.Second,
			}, false)
			require_NoError(t, err)

			cfg := &StreamConfig{Name: "S", Storage: FileStorage}
			ss := &StreamSource{Name: "O", Consumer: &StreamConsumerSource{Name: "C", DeliverSubject: "d"}}
			if mirror {
				cfg.Mirror = ss
			} else {
				cfg.Sources = []*StreamSource{ss}
			}
			_, err = jsStreamCreate(t, nc, cfg)
			require_NoError(t, err)

			mset, err := s.globalAccount().lookupStream("S")
			require_NoError(t, err)

			// Mirrors and sources share the sourceInfo, and only one of the two is set,
			// so the rest of the test does not need to care which we are.
			lastReq := func() time.Time {
				mset.mu.RLock()
				defer mset.mu.RUnlock()
				si := mset.mirror
				for _, ss := range mset.sources {
					si = ss
				}
				return si.lreq
			}

			// The consumer is alive and pushing at us, so the probe we put up on each
			// timeout must keep cutting the backoff short, and must be put back up for
			// the timeout after that. Without it the next request would be 10s out and
			// climbing, with it we only wait out the retry throttle.
			var requests int
			last := lastReq()
			checkFor(t, 8*time.Second, 50*time.Millisecond, func() error {
				if lreq := lastReq(); lreq.After(last) {
					requests, last = requests+1, lreq
				}
				if requests < 2 {
					return fmt.Errorf("only %d requests, backoff is not being cut short", requests)
				}
				return nil
			})
		})
	}
}

func TestJetStreamClusterDurableStreamSource(t *testing.T) {
	test := func(t *testing.T, replicas int, retention RetentionPolicy) {
		var s *Server
		if replicas == 1 {
			s = RunBasicJetStreamServer(t)
			defer s.Shutdown()
		} else {
			c := createJetStreamClusterExplicit(t, "R3S", 3)
			defer c.shutdown()
			s = c.randomServer()
		}

		nc, js := jsClientConnect(t, s)
		defer nc.Close()

		_, err := jsStreamCreate(t, nc, &StreamConfig{
			Name:      "O",
			Subjects:  []string{"foo"},
			Storage:   FileStorage,
			Replicas:  replicas,
			Retention: retention,
		})
		require_NoError(t, err)

		_, err = jsConsumerCreate(t, nc, "O", ConsumerConfig{
			Durable:        "C",
			DeliverSubject: "deliver-subject",
			Replicas:       replicas,
			AckPolicy:      AckFlowControl,
			Heartbeat:      time.Second,
		}, false)
		require_NoError(t, err)

		pubAck, err := js.Publish("foo", nil)
		require_NoError(t, err)
		require_Equal(t, pubAck.Sequence, 1)

		_, err = jsStreamCreate(t, nc, &StreamConfig{
			Name: "S",
			Sources: []*StreamSource{{
				Name: "O",
				Consumer: &StreamConsumerSource{
					Name:           "C",
					DeliverSubject: "deliver-subject",
				},
			}},
			Storage:  FileStorage,
			Replicas: replicas,
		})
		require_NoError(t, err)

		checkFor(t, 2*time.Second, 200*time.Millisecond, func() error {
			si, err := js.StreamInfo("S")
			if err != nil {
				return err
			}
			if len(si.Sources) != 1 {
				return errors.New("no source")
			}
			if si.Sources[0].Error != nil {
				return si.Sources[0].Error
			}
			_, err = js.GetMsg("S", 1)
			return err
		})
	}

	for _, replicas := range []int{1, 3} {
		for _, retention := range []RetentionPolicy{LimitsPolicy, InterestPolicy, WorkQueuePolicy} {
			t.Run(fmt.Sprintf("R%d/%s", replicas, retention), func(t *testing.T) {
				test(t, replicas, retention)
			})
		}
	}
}

func TestJetStreamClusterDurableStreamSourceServerManaged(t *testing.T) {
	test := func(t *testing.T, replicas int, retention RetentionPolicy) {
		var s *Server
		if replicas == 1 {
			s = RunBasicJetStreamServer(t)
			defer s.Shutdown()
		} else {
			c := createJetStreamClusterExplicit(t, "R3S", 3)
			defer c.shutdown()
			s = c.randomServer()
		}

		nc, js := jsClientConnect(t, s)
		defer nc.Close()

		_, err := jsStreamCreate(t, nc, &StreamConfig{
			Name:      "O",
			Subjects:  []string{"foo"},
			Storage:   FileStorage,
			Replicas:  replicas,
			Retention: retention,
		})
		require_NoError(t, err)

		_, err = js.AddConsumer("O", &nats.ConsumerConfig{Durable: "C", AckPolicy: nats.AckExplicitPolicy})
		require_NoError(t, err)

		pubAck, err := js.Publish("foo", nil)
		require_NoError(t, err)
		require_Equal(t, pubAck.Sequence, 1)

		_, err = jsStreamCreate(t, nc, &StreamConfig{
			Name:     "S",
			Sources:  []*StreamSource{{Name: "O"}},
			Storage:  FileStorage,
			Replicas: replicas,
		})
		require_NoError(t, err)

		checkFor(t, 2*time.Second, 200*time.Millisecond, func() error {
			si, err := js.StreamInfo("S")
			if err != nil {
				return err
			}
			if len(si.Sources) != 1 {
				return errors.New("no source")
			}
			if si.Sources[0].Error != nil {
				return si.Sources[0].Error
			}
			_, err = js.GetMsg("S", 1)
			return err
		})
	}

	for _, replicas := range []int{1, 3} {
		for _, retention := range []RetentionPolicy{LimitsPolicy, InterestPolicy, WorkQueuePolicy} {
			t.Run(fmt.Sprintf("R%d/%s", replicas, retention), func(t *testing.T) {
				test(t, replicas, retention)
			})
		}
	}
}

func TestJetStreamDurableStreamSourcesWQFwcExclusivity(t *testing.T) {
	test := func(t *testing.T, replicas int) {
		var s *Server
		if replicas == 1 {
			s = RunBasicJetStreamServer(t)
			defer s.Shutdown()
		} else {
			c := createJetStreamClusterExplicit(t, "R3S", 3)
			defer c.shutdown()
			s = c.randomServer()
		}

		nc, js := jsClientConnect(t, s)
		defer nc.Close()

		_, err := js.AddStream(&nats.StreamConfig{
			Name:      "O",
			Subjects:  []string{"foo"},
			Retention: nats.WorkQueuePolicy,
			Replicas:  replicas,
		})
		require_NoError(t, err)

		_, err = js.AddStream(&nats.StreamConfig{
			Name:   "M",
			Mirror: &nats.StreamSource{Name: "O"},
		})
		require_NoError(t, err)

		mset, err := s.globalAccount().lookupStream("O")
		require_NoError(t, err)
		checkFor(t, 2*time.Second, 200*time.Millisecond, func() error {
			if c := mset.numConsumers(); c != 1 {
				return fmt.Errorf("expected 1 consumer, got %d", c)
			}
			return nil
		})

		// A sourcing consumer already exists, but we should still be able to add an app consumer.
		// Only this one counts toward WQ exclusivity.
		_, err = js.AddConsumer("O", &nats.ConsumerConfig{Durable: "CONSUMER", AckPolicy: nats.AckExplicitPolicy})
		require_NoError(t, err)
	}

	for _, replicas := range []int{1, 3} {
		t.Run(fmt.Sprintf("R%d", replicas), func(t *testing.T) { test(t, replicas) })
	}
}

func TestJetStreamDurableStreamSourcesWQFilterExclusivity(t *testing.T) {
	test := func(t *testing.T, replicas int) {
		var s *Server
		if replicas == 1 {
			s = RunBasicJetStreamServer(t)
			defer s.Shutdown()
		} else {
			c := createJetStreamClusterExplicit(t, "R3S", 3)
			defer c.shutdown()
			s = c.randomServer()
		}

		nc, js := jsClientConnect(t, s)
		defer nc.Close()

		_, err := js.AddStream(&nats.StreamConfig{
			Name:      "O",
			Subjects:  []string{"a", "b"},
			Retention: nats.WorkQueuePolicy,
			Replicas:  replicas,
		})
		require_NoError(t, err)

		_, err = js.AddStream(&nats.StreamConfig{
			Name: "M",
			Mirror: &nats.StreamSource{
				Name: "O",
				SubjectTransforms: []nats.SubjectTransformConfig{
					{Source: "a"},
					{Source: "b"},
				},
			},
		})
		require_NoError(t, err)

		mset, err := s.globalAccount().lookupStream("O")
		require_NoError(t, err)
		checkFor(t, 2*time.Second, 200*time.Millisecond, func() error {
			if c := mset.numConsumers(); c != 1 {
				return fmt.Errorf("expected 1 consumer, got %d", c)
			}
			return nil
		})

		// A sourcing consumer already exists, but we should still be able to add filtered app consumers.
		// Only these count toward WQ exclusivity.
		_, err = js.AddConsumer("O", &nats.ConsumerConfig{
			Durable:       "CONSUMER1",
			AckPolicy:     nats.AckExplicitPolicy,
			FilterSubject: "a",
		})
		require_NoError(t, err)
		_, err = js.AddConsumer("O", &nats.ConsumerConfig{
			Durable:       "CONSUMER2",
			AckPolicy:     nats.AckExplicitPolicy,
			FilterSubject: "b",
		})
		require_NoError(t, err)
	}

	for _, replicas := range []int{1, 3} {
		t.Run(fmt.Sprintf("R%d", replicas), func(t *testing.T) { test(t, replicas) })
	}
}

func TestJetStreamDurableStreamSourcesMaxConsumers(t *testing.T) {
	test := func(t *testing.T, replicas int) {
		var s *Server
		if replicas == 1 {
			s = RunBasicJetStreamServer(t)
			defer s.Shutdown()
		} else {
			c := createJetStreamClusterExplicit(t, "R3S", 3)
			defer c.shutdown()
			s = c.randomServer()
		}

		nc, js := jsClientConnect(t, s)
		defer nc.Close()

		_, err := js.AddStream(&nats.StreamConfig{
			Name:         "O",
			Subjects:     []string{"foo"},
			Retention:    nats.WorkQueuePolicy,
			Replicas:     replicas,
			MaxConsumers: 1,
		})
		require_NoError(t, err)

		_, err = js.AddStream(&nats.StreamConfig{
			Name:   "M",
			Mirror: &nats.StreamSource{Name: "O"},
		})
		require_NoError(t, err)

		mset, err := s.globalAccount().lookupStream("O")
		require_NoError(t, err)
		checkFor(t, 2*time.Second, 200*time.Millisecond, func() error {
			if c := mset.numConsumers(); c != 1 {
				return fmt.Errorf("expected 1 consumer, got %d", c)
			}
			return nil
		})

		// A sourcing consumer already exists, but we should still be able to add an app consumer.
		// Only this one counts toward the MaxConsumers limit.
		_, err = js.AddConsumer("O", &nats.ConsumerConfig{Durable: "CONSUMER", AckPolicy: nats.AckExplicitPolicy})
		require_NoError(t, err)
	}

	for _, replicas := range []int{1, 3} {
		t.Run(fmt.Sprintf("R%d", replicas), func(t *testing.T) { test(t, replicas) })
	}
}

func TestJetStreamDurableStreamMirrorDeletesConsumerAfterMirrorRemoval(t *testing.T) {
	test := func(t *testing.T, replicas int) {
		var s *Server
		if replicas == 1 {
			s = RunBasicJetStreamServer(t)
			defer s.Shutdown()
		} else {
			c := createJetStreamClusterExplicit(t, "R3S", 3)
			defer c.shutdown()
			s = c.randomServer()
		}

		nc, js := jsClientConnect(t, s)
		defer nc.Close()

		_, err := js.AddStream(&nats.StreamConfig{
			Name:      "O",
			Subjects:  []string{"foo"},
			Retention: nats.WorkQueuePolicy,
			Replicas:  replicas,
		})
		require_NoError(t, err)

		cfg := &nats.StreamConfig{
			Name:   "M",
			Mirror: &nats.StreamSource{Name: "O"},
		}
		_, err = js.AddStream(cfg)
		require_NoError(t, err)

		mset, err := s.globalAccount().lookupStream("O")
		require_NoError(t, err)
		checkFor(t, 2*time.Second, 200*time.Millisecond, func() error {
			if c := mset.numConsumers(); c != 1 {
				return fmt.Errorf("expected 1 consumer, got %d", c)
			}
			return nil
		})

		// Removing the mirror config should result in the sourcing consumer to be deleted.
		cfg.Mirror = nil
		_, err = js.UpdateStream(cfg)
		require_NoError(t, err)
		checkFor(t, 2*time.Second, 200*time.Millisecond, func() error {
			if c := mset.numConsumers(); c != 0 {
				return fmt.Errorf("expected 0 consumers, got %d", c)
			}
			return nil
		})
	}

	for _, replicas := range []int{1, 3} {
		t.Run(fmt.Sprintf("R%d", replicas), func(t *testing.T) { test(t, replicas) })
	}
}

func TestJetStreamDurableStreamMirrorDeletesConsumerAfterStreamRemoval(t *testing.T) {
	test := func(t *testing.T, replicas int) {
		var s *Server
		if replicas == 1 {
			s = RunBasicJetStreamServer(t)
			defer s.Shutdown()
		} else {
			c := createJetStreamClusterExplicit(t, "R3S", 3)
			defer c.shutdown()
			s = c.randomServer()
		}

		nc, js := jsClientConnect(t, s)
		defer nc.Close()

		_, err := js.AddStream(&nats.StreamConfig{
			Name:      "O",
			Subjects:  []string{"foo"},
			Retention: nats.WorkQueuePolicy,
			Replicas:  replicas,
		})
		require_NoError(t, err)

		cfg := &nats.StreamConfig{
			Name:   "M",
			Mirror: &nats.StreamSource{Name: "O"},
		}
		_, err = js.AddStream(cfg)
		require_NoError(t, err)

		mset, err := s.globalAccount().lookupStream("O")
		require_NoError(t, err)
		checkFor(t, 2*time.Second, 200*time.Millisecond, func() error {
			if c := mset.numConsumers(); c != 1 {
				return fmt.Errorf("expected 1 consumer, got %d", c)
			}
			return nil
		})

		// Deleting the stream that mirrors should result in the sourcing consumer to be deleted.
		require_NoError(t, js.DeleteStream("M"))
		checkFor(t, 2*time.Second, 200*time.Millisecond, func() error {
			if c := mset.numConsumers(); c != 0 {
				return fmt.Errorf("expected 0 consumers, got %d", c)
			}
			return nil
		})
	}

	for _, replicas := range []int{1, 3} {
		t.Run(fmt.Sprintf("R%d", replicas), func(t *testing.T) { test(t, replicas) })
	}
}

func TestJetStreamDurableStreamSourceDeletesConsumerAfterSourceUpdate(t *testing.T) {
	test := func(t *testing.T, replicas int) {
		var s *Server
		if replicas == 1 {
			s = RunBasicJetStreamServer(t)
			defer s.Shutdown()
		} else {
			c := createJetStreamClusterExplicit(t, "R3S", 3)
			defer c.shutdown()
			s = c.randomServer()
		}

		nc, js := jsClientConnect(t, s)
		defer nc.Close()

		_, err := js.AddStream(&nats.StreamConfig{
			Name:      "O",
			Subjects:  []string{"a", "b"},
			Retention: nats.WorkQueuePolicy,
			Replicas:  replicas,
		})
		require_NoError(t, err)

		_, err = js.AddStream(&nats.StreamConfig{
			Name:      "T",
			Subjects:  []string{"foo"},
			Retention: nats.WorkQueuePolicy,
			Replicas:  replicas,
		})
		require_NoError(t, err)

		mset, err := s.globalAccount().lookupStream("O")
		require_NoError(t, err)
		msetT, err := s.globalAccount().lookupStream("T")
		require_NoError(t, err)

		t.Run("Basic", func(t *testing.T) {
			cfg := &nats.StreamConfig{
				Name:    "S",
				Sources: []*nats.StreamSource{{Name: "O"}},
			}
			_, err = js.AddStream(cfg)
			require_NoError(t, err)
			checkFor(t, 2*time.Second, 200*time.Millisecond, func() error {
				if c := mset.numConsumers(); c != 1 {
					return fmt.Errorf("expected 1 consumer, got %d", c)
				}
				return nil
			})

			cfg.Sources = []*nats.StreamSource{
				{Name: "O", FilterSubject: "a"},
				{Name: "O", FilterSubject: "b"},
			}
			_, err = js.UpdateStream(cfg)
			require_NoError(t, err)
			checkFor(t, 2*time.Second, 200*time.Millisecond, func() error {
				if c := mset.numConsumers(); c != 2 {
					return fmt.Errorf("expected 2 consumers, got %d", c)
				}
				return nil
			})

			// Removing the source config should result in the sourcing consumer to be deleted.
			cfg.Sources = nil
			_, err = js.UpdateStream(cfg)
			require_NoError(t, err)
			checkFor(t, 2*time.Second, 200*time.Millisecond, func() error {
				if c := mset.numConsumers(); c != 0 {
					return fmt.Errorf("expected 0 consumers, got %d", c)
				}
				return nil
			})
		})

		t.Run("Multiple", func(t *testing.T) {
			// Now test adding two separate sources that would use the same consumer name but for separate streams.
			cfg := &nats.StreamConfig{
				Name: "S",
				Sources: []*nats.StreamSource{
					{Name: "O"},
					{Name: "T"},
				},
			}
			_, err = js.UpdateStream(cfg)
			require_NoError(t, err)

			var sourceConsumerName string
			checkFor(t, 2*time.Second, 200*time.Millisecond, func() error {
				oc := mset.getPublicConsumers()
				tc := msetT.getPublicConsumers()
				if len(oc) != 1 || len(tc) != 1 {
					return fmt.Errorf("expected 1 consumer on O and 1 consumer on T, got %d and %d", len(oc), len(tc))
				}
				occ := oc[0].info()
				tcc := tc[0].info()
				if occ.Name != tcc.Name {
					return fmt.Errorf("expected consumer names to match, got %q and %q", occ.Name, tcc.Name)
				}
				sourceConsumerName = tcc.Name
				return nil
			})

			// Removing one source should still properly clean up the sourcing consumer.
			cfg.Sources = []*nats.StreamSource{{Name: "T"}}
			_, err = js.UpdateStream(cfg)
			require_NoError(t, err)

			checkFor(t, 2*time.Second, 200*time.Millisecond, func() error {
				if c := mset.numConsumers(); c != 0 {
					return fmt.Errorf("expected 0 consumers on O, got %d", c)
				}
				tc := msetT.getPublicConsumers()
				if len(tc) != 1 {
					return fmt.Errorf("expected 1 consumer on T, got %d", len(tc))
				}
				tcc := tc[0].info()
				if tcc.Name != sourceConsumerName {
					return fmt.Errorf("expected consumer names to match, got %q, expected %q", tcc.Name, sourceConsumerName)
				}
				return nil
			})
		})

		t.Run("Mixed", func(t *testing.T) {
			cfg := &StreamConfig{
				Name:    "S",
				Storage: FileStorage,
				Sources: []*StreamSource{
					{Name: "O"},
				},
			}
			_, err = jsStreamUpdate(t, nc, cfg)
			require_NoError(t, err)

			// Capture the consumer name used for the sourcing.
			var sourceConsumerName string
			checkFor(t, 2*time.Second, 200*time.Millisecond, func() error {
				oc := mset.getPublicConsumers()
				if len(oc) != 1 {
					return fmt.Errorf("expected 1 consumer on O, got %d", len(oc))
				}
				occ := oc[0].info()
				sourceConsumerName = occ.Name
				return nil
			})

			checkExpectedCount := func(expected int) {
				t.Helper()
				checkFor(t, 2*time.Second, 200*time.Millisecond, func() error {
					if c := mset.numConsumers(); c != expected {
						return fmt.Errorf("expected %d consumer(s) on O, got %d", expected, c)
					}
					return nil
				})
			}

			// Create a consumer that will be used for the sourcing instead.
			_, err = js.AddConsumer("O", &nats.ConsumerConfig{Durable: "CONSUMER", AckPolicy: nats.AckExplicitPolicy, DeliverSubject: "deliver"})
			require_NoError(t, err)
			checkExpectedCount(2)

			// After the update, the previous sourcing consumer should be cleaned up.
			cfg.Sources = []*StreamSource{
				{Name: "O", Consumer: &StreamConsumerSource{Name: "CONSUMER", DeliverSubject: "deliver"}},
			}
			_, err = jsStreamUpdate(t, nc, cfg)
			require_NoError(t, err)
			checkExpectedCount(1)

			// Recreate the sourcing consumer so we can validate it doesn't get removed if not referenced in the config.
			_, err = jsConsumerCreate(t, nc, "O", ConsumerConfig{Durable: sourceConsumerName, AckPolicy: AckExplicit, Sourcing: true}, false)
			require_NoError(t, err)
			checkExpectedCount(2)

			// Since we used a pre-existing consumer, the above source consumer replica that the source stream
			// didn't create shouldn't be deleted.
			cfg.Sources = nil
			_, err = jsStreamUpdate(t, nc, cfg)
			require_NoError(t, err)
			time.Sleep(500 * time.Millisecond)
			checkExpectedCount(2)

			// Similarly, shouldn't remove the source consumer replica when deleting the source stream entirely.
			// But need to reset it to contain sources again first.
			cfg.Sources = []*StreamSource{
				{Name: "O", Consumer: &StreamConsumerSource{Name: "CONSUMER", DeliverSubject: "deliver"}},
			}
			_, err = jsStreamUpdate(t, nc, cfg)
			require_NoError(t, err)
			require_NoError(t, js.DeleteStream("S"))
			time.Sleep(500 * time.Millisecond)
			checkExpectedCount(2)
		})
	}

	for _, replicas := range []int{1, 3} {
		t.Run(fmt.Sprintf("R%d", replicas), func(t *testing.T) { test(t, replicas) })
	}
}

func TestJetStreamDurableStreamSourceDeletesConsumerAfterStreamRemoval(t *testing.T) {
	test := func(t *testing.T, replicas int) {
		var s *Server
		if replicas == 1 {
			s = RunBasicJetStreamServer(t)
			defer s.Shutdown()
		} else {
			c := createJetStreamClusterExplicit(t, "R3S", 3)
			defer c.shutdown()
			s = c.randomServer()
		}

		nc, js := jsClientConnect(t, s)
		defer nc.Close()

		_, err := js.AddStream(&nats.StreamConfig{
			Name:      "O",
			Subjects:  []string{"foo"},
			Retention: nats.WorkQueuePolicy,
			Replicas:  replicas,
		})
		require_NoError(t, err)

		cfg := &nats.StreamConfig{
			Name:    "S",
			Sources: []*nats.StreamSource{{Name: "O"}},
		}
		_, err = js.AddStream(cfg)
		require_NoError(t, err)

		mset, err := s.globalAccount().lookupStream("O")
		require_NoError(t, err)
		checkFor(t, 2*time.Second, 200*time.Millisecond, func() error {
			if c := mset.numConsumers(); c != 1 {
				return fmt.Errorf("expected 1 consumer, got %d", c)
			}
			return nil
		})

		// Deleting the stream that sources should result in the sourcing consumer to be deleted.
		require_NoError(t, js.DeleteStream("S"))
		checkFor(t, 2*time.Second, 200*time.Millisecond, func() error {
			if c := mset.numConsumers(); c != 0 {
				return fmt.Errorf("expected 0 consumers, got %d", c)
			}
			return nil
		})
	}

	for _, replicas := range []int{1, 3} {
		t.Run(fmt.Sprintf("R%d", replicas), func(t *testing.T) { test(t, replicas) })
	}
}

func TestJetStreamClusterConsumerSelectStartingSeqDeferred(t *testing.T) {
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

	_, err = js.AddConsumer("TEST", &nats.ConsumerConfig{
		Durable:   "C",
		AckPolicy: nats.AckExplicitPolicy,
		Replicas:  3,
	})
	require_NoError(t, err)

	leader := c.consumerLeader(globalAccountName, "TEST", "C")
	require_NotNil(t, leader)
	follower := c.randomNonConsumerLeader(globalAccountName, "TEST", "C")
	require_NotNil(t, follower)

	getConsumer := func(s *Server) *consumer {
		t.Helper()
		mset, err := s.globalAccount().lookupStream("TEST")
		require_NoError(t, err)
		o := mset.lookupConsumer("C")
		require_NotNil(t, o)
		return o
	}

	// On the leader, selectStartingSeqNo ran inside setLeader(true).
	l := getConsumer(leader)
	l.mu.RLock()
	ldseq, lsseq := l.dseq, l.sseq
	l.mu.RUnlock()
	require_Equal(t, ldseq, 1)
	require_Equal(t, lsseq, 1)

	// On the follower, meta apply must not have run selectStartingSeqNo.
	checkFor(t, 2*time.Second, 200*time.Millisecond, func() error {
		f := getConsumer(follower)
		f.mu.RLock()
		defer f.mu.RUnlock()
		if f.dseq != 1 {
			return fmt.Errorf("expected follower dseq 1, got %d", f.dseq)
		}
		if f.sseq != 1 {
			return fmt.Errorf("expected follower sseq 1, got %d", f.sseq)
		}
		return nil
	})
}

type failProposeRaftNode struct {
	RaftNode
}

func (n *failProposeRaftNode) Propose(uint64, []byte) error {
	return errNotLeader
}

func (n *failProposeRaftNode) ProposeMulti(uint64, []*Entry) error {
	return errNotLeader
}

func TestJetStreamClusterProposeFailureDoesNotDriftClseq(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "R3S", 3)
	defer c.shutdown()

	nc, js := jsClientConnect(t, c.randomServer())
	defer nc.Close()

	_, err := jsStreamCreate(t, nc, &StreamConfig{
		Name:               "TEST",
		Subjects:           []string{"foo"},
		Replicas:           3,
		Storage:            FileStorage,
		AllowAtomicPublish: true,
	})
	require_NoError(t, err)

	// Populate the stream with some messages.
	for range 10 {
		_, err = js.Publish("foo", nil)
		require_NoError(t, err)
	}
	checkFor(t, 2*time.Second, 200*time.Millisecond, func() error {
		return checkState(t, c, globalAccountName, "TEST")
	})

	sl := c.streamLeader(globalAccountName, "TEST")
	mset, err := sl.globalAccount().lookupStream("TEST")
	require_NoError(t, err)

	// forceProposeFailure changes node.Propose / node.ProposeMulti to always
	// return errNotLeader.
	// We deliberately do NOT touch n.leaderState, the upper layer still sees
	// the node as leader, reproducing the race window.
	forceProposeFailure := func(fn func() error) error {
		mset.mu.Lock()
		prevNode := mset.node
		mset.node = &failProposeRaftNode{RaftNode: prevNode}
		mset.mu.Unlock()

		err = fn()

		mset.mu.Lock()
		mset.node = prevNode
		mset.mu.Unlock()

		return err
	}

	readClseq := func() uint64 {
		mset.clMu.Lock()
		defer mset.clMu.Unlock()
		return mset.clseq
	}

	t.Run("SingleMessage", func(t *testing.T) {
		before := readClseq()
		err = forceProposeFailure(func() error {
			return mset.processClusteredInboundMsg("foo", _EMPTY_, nil, nil, nil, false)
		})
		require_Error(t, err, errNotLeader)
		require_Equal(t, readClseq(), before)
	})

	t.Run("AtomicBatch", func(t *testing.T) {
		hdr := genHeader(nil, "Nats-Batch-Id", "uuid")
		hdr = genHeader(hdr, "Nats-Batch-Sequence", "1")
		hdr = genHeader(hdr, "Nats-Batch-Commit", "1")

		before := readClseq()
		err = forceProposeFailure(func() error {
			return mset.processJetStreamAtomicBatchMsg("uuid", "foo", _EMPTY_, hdr, nil, nil)
		})
		require_Error(t, err, errNotLeader)
		require_Equal(t, readClseq(), before)
	})

	// Confirm we can still publish a new message.
	_, err = js.Publish("foo", nil)
	require_NoError(t, err)
	checkFor(t, 2*time.Second, 200*time.Millisecond, func() error {
		state, err := checkStateAndErr(t, c, globalAccountName, "TEST")
		if err != nil {
			return err
		}
		if state.Msgs != 11 {
			return fmt.Errorf("expected 11 messages, got %d", state.Msgs)
		}
		return nil
	})

	// Verify no Raft node on any replica reports itself as deleted.
	for _, s := range c.servers {
		mset, err = s.globalAccount().lookupStream("TEST")
		require_NoError(t, err)
		require_False(t, mset.raftNode().IsDeleted())
	}
}

func TestJetStreamClusterSkipMsgsRaftDeleteRange(t *testing.T) {
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

	sl := c.streamLeader(globalAccountName, "TEST")
	mset, err := sl.globalAccount().lookupStream("TEST")
	require_NoError(t, err)

	// Use a huge gap to assert the O(1) apply path.
	gap := uint64(100_000_000)
	start := time.Now()
	mset.mu.Lock()
	err = mset.skipMsgs(2, gap)
	mset.mu.Unlock()
	require_NoError(t, err)
	if elapsed := time.Since(start); elapsed > 2*time.Second {
		t.Fatalf("Expected to skip msgs in <2s but got %v", elapsed)
	}

	// Wait for the skip to be applied on the leader before publishing,
	// since the clustered paths are async.
	checkFor(t, 2*time.Second, 200*time.Millisecond, func() error {
		mset.mu.RLock()
		lseq := mset.lseq
		mset.mu.RUnlock()
		if lseq < gap {
			return fmt.Errorf("leader lseq=%d, want >=%d", lseq, gap)
		}
		return nil
	})

	// After the skip, publish one more live message so we have a
	// message at LastSeq to compare against.
	_, err = js.Publish("foo", nil)
	require_NoError(t, err)
	checkFor(t, 2*time.Second, 200*time.Millisecond, func() error {
		for _, s := range c.servers {
			mset, err = s.globalAccount().lookupStream("TEST")
			if err != nil {
				return err
			}
			var state StreamState
			mset.store.FastState(&state)
			if state.LastSeq != gap+1 {
				return fmt.Errorf("server %s LastSeq=%d, want %d", s.Name(), state.LastSeq, gap+1)
			}
			if state.Msgs != 2 {
				return fmt.Errorf("server %s Msgs=%d, want 2", s.Name(), state.Msgs)
			}
		}
		return nil
	})
}

func TestJetStreamClusterApplyDeleteRangeOpIdempotent(t *testing.T) {
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

	sl := c.streamLeader(globalAccountName, "TEST")
	mset, err := sl.globalAccount().lookupStream("TEST")
	require_NoError(t, err)
	sjs := sl.getJetStream()

	mset.mu.Lock()
	require_NoError(t, mset.store.SkipMsgs(2, 999))
	mset.lseq = 1000
	mset.mu.Unlock()

	var before StreamState
	mset.store.FastState(&before)

	// Ignore delete range if already applied.
	replay := newCommittedEntry(1, []*Entry{
		newEntry(EntryNormal, encodeDeleteRange(&DeleteRange{First: 100, Num: 401})),
	})
	batch := &batchApply{}
	_, err = sjs.applyStreamEntries(mset, mset.raftNode(), replay, false, batch)
	require_NoError(t, err)
	require_Equal(t, mset.lastSeq(), 1000)

	var after StreamState
	mset.store.FastState(&after)
	require_Equal(t, after.FirstSeq, before.FirstSeq)
	require_Equal(t, after.LastSeq, before.LastSeq)
	require_Equal(t, after.Msgs, before.Msgs)

	// Full delete range.
	dr := newCommittedEntry(2, []*Entry{
		newEntry(EntryNormal, encodeDeleteRange(&DeleteRange{First: 1001, Num: 1000})),
	})
	_, err = sjs.applyStreamEntries(mset, mset.raftNode(), dr, false, batch)
	require_NoError(t, err)
	require_Equal(t, mset.lastSeq(), 2000)
	mset.store.FastState(&after)
	require_Equal(t, after.LastSeq, 2000)

	// Partial delete range.
	dr = newCommittedEntry(3, []*Entry{
		newEntry(EntryNormal, encodeDeleteRange(&DeleteRange{First: 1501, Num: 1000})),
	})
	_, err = sjs.applyStreamEntries(mset, mset.raftNode(), dr, false, batch)
	require_NoError(t, err)
	require_Equal(t, mset.lastSeq(), 2500)
	mset.store.FastState(&after)
	require_Equal(t, after.LastSeq, 2500)
}

func TestJetStreamClusterMirrorSkipMsgsPropagatesProposeFailure(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "R3S", 3)
	defer c.shutdown()

	nc, js := jsClientConnect(t, c.randomServer())
	defer nc.Close()

	_, err := js.AddStream(&nats.StreamConfig{
		Name:     "SRC",
		Subjects: []string{"src.>"},
		Replicas: 3,
	})
	require_NoError(t, err)

	stored := uint64(5)
	for range stored {
		_, err = js.Publish("src.a", nil)
		require_NoError(t, err)
	}

	_, err = js.AddStream(&nats.StreamConfig{
		Name:     "M",
		Replicas: 3,
		Mirror:   &nats.StreamSource{Name: "SRC"},
	})
	require_NoError(t, err)

	checkFor(t, 2*time.Second, 200*time.Millisecond, func() error {
		si, err := js.StreamInfo("M")
		if err != nil {
			return err
		}
		if si.State.Msgs != stored {
			return fmt.Errorf("got %d msgs, want %d", si.State.Msgs, stored)
		}
		return nil
	})

	sl := c.streamLeader(globalAccountName, "M")
	require_NotNil(t, sl)
	mset, err := sl.globalAccount().lookupStream("M")
	require_NoError(t, err)

	// Step the mirror's Raft node down so node.Propose / ProposeMulti will return
	// errNotLeader for any subsequent proposal; including the one skipMsgs makes.
	require_NoError(t, mset.raftNode().StepDown())
	checkFor(t, 2*time.Second, 200*time.Millisecond, func() error {
		if mset.IsLeader() {
			return fmt.Errorf("still leader")
		}
		return nil
	})

	mset.mu.Lock()
	err = mset.skipMsgs(stored+1, stored+10)
	mset.mu.Unlock()
	require_Error(t, err, errNotLeader)
}

func TestJetStreamClusterStreamScaleDownOfflinePeersHonorsReplicaCount(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "R7S", 7)
	defer c.shutdown()

	// Connect to the meta leader, it is guaranteed to remain running.
	ml := c.leader()
	nc, js := jsClientConnect(t, ml)
	defer nc.Close()

	_, err := js.AddStream(&nats.StreamConfig{
		Name:     "TEST",
		Subjects: []string{"foo"},
		Replicas: 5,
	})
	require_NoError(t, err)

	// Map peer IDs to servers.
	peerSrv := make(map[string]*Server, len(c.servers))
	srvPeer := make(map[*Server]string, len(c.servers))
	for _, s := range c.servers {
		peerSrv[s.Node()] = s
		srvPeer[s] = s.Node()
	}

	// Get the stream peer set from the meta leader.
	mjs := ml.getJetStream()
	mjs.mu.RLock()
	sa := mjs.streamAssignmentOrInflight(globalAccountName, "TEST")
	var streamPeers []string
	if sa != nil {
		streamPeers = copyStrings(sa.Group.Peers)
	}
	mjs.mu.RUnlock()
	require_Len(t, len(streamPeers), 5)

	// Shut down three of the stream's peers. Only two of the five stream peers
	// remain online, so the stream's group has lost quorum.
	sl := c.streamLeader(globalAccountName, "TEST")
	var offline []*Server
	var online []string
	for _, p := range streamPeers {
		if s := peerSrv[p]; s != sl && s != ml && len(offline) < 3 {
			offline = append(offline, s)
		} else {
			online = append(online, p)
		}
	}
	require_Len(t, len(offline), 3)
	require_Len(t, len(online), 2)
	for _, s := range offline {
		s.Shutdown()
	}

	// Wait for the meta leader to mark the stopped servers as offline.
	checkFor(t, 2*time.Second, 100*time.Millisecond, func() error {
		for _, s := range offline {
			if ni, ok := ml.nodeToInfo.Load(srvPeer[s]); !ok || !ni.(nodeInfo).offline {
				return fmt.Errorf("server %q not marked offline yet", s.Name())
			}
		}
		return nil
	})

	// Scale the stream down to R3. The update is accepted and recorded as
	// desired state, but with a majority of the peers offline the group can
	// not commit membership changes. The scale down must stay pending rather
	// than override quorum, which could select peers that miss acknowledged
	// writes or split the group.
	_, err = js.UpdateStream(&nats.StreamConfig{
		Name:     "TEST",
		Subjects: []string{"foo"},
		Replicas: 3,
	})
	require_NoError(t, err)

	// Wait for the desired scale down to be registered in the meta layer.
	checkFor(t, 2*time.Second, 100*time.Millisecond, func() error {
		mjs.mu.RLock()
		defer mjs.mu.RUnlock()
		sa := mjs.streamAssignment(globalAccountName, "TEST")
		if sa == nil {
			return fmt.Errorf("stream assignment not found")
		}
		if sa.Group.Desired == nil {
			return fmt.Errorf("desired state not registered yet")
		}
		return nil
	})

	// While the group has no quorum the scale down must remain pending and
	// the assigned peer set must remain unchanged.
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		mjs.mu.RLock()
		sa := mjs.streamAssignment(globalAccountName, "TEST")
		var peers int
		var pending bool
		if sa != nil {
			peers = len(sa.Group.Peers)
			pending = sa.Group.Desired != nil
		}
		mjs.mu.RUnlock()
		require_True(t, sa != nil)
		require_Len(t, peers, 5)
		require_True(t, pending)
		time.Sleep(100 * time.Millisecond)
	}

	// Restart one of the offline peers. Three of the five peers are online
	// again, restoring quorum, so the scale down can now proceed safely by
	// committing membership changes through the group.
	c.restartServer(offline[0])

	// Wait for the scale down to complete in the meta layer.
	var newPeers []string
	checkFor(t, 5*time.Second, 200*time.Millisecond, func() error {
		mjs.mu.RLock()
		defer mjs.mu.RUnlock()
		sa := mjs.streamAssignment(globalAccountName, "TEST")
		if sa == nil {
			return fmt.Errorf("stream assignment not found")
		}
		if sa.Group.Desired != nil {
			return fmt.Errorf("scale down still pending")
		}
		if len(sa.Group.Peers) != 3 {
			return fmt.Errorf("expected 3 peers, got %d", len(sa.Group.Peers))
		}
		newPeers = copyStrings(sa.Group.Peers)
		return nil
	})

	// The peers that stayed online must be preferred and part of the new peer set.
	for _, p := range online {
		if !slices.Contains(newPeers, p) {
			t.Fatalf("Online peer %q not selected by the scale down", peerSrv[p].Name())
		}
	}

	// The new peer set must be a subset of the original peer set.
	for _, p := range newPeers {
		require_True(t, slices.Contains(streamPeers, p))
	}
}

func TestJetStreamClusterConsumerScaleDownPrefersOnlinePeers(t *testing.T) {
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

	_, err = js.AddConsumer("TEST", &nats.ConsumerConfig{
		Durable:   "DUR",
		AckPolicy: nats.AckExplicitPolicy,
		Replicas:  5,
	})
	require_NoError(t, err)

	// Map peer IDs to servers.
	peerSrv := make(map[string]*Server, len(c.servers))
	srvPeer := make(map[*Server]string, len(c.servers))
	for _, s := range c.servers {
		peerSrv[s.Node()] = s
		srvPeer[s] = s.Node()
	}

	// Get the consumer peer set, in order, from the meta leader.
	c.waitOnLeader()
	ml := c.leader()
	mjs := ml.getJetStream()
	mjs.mu.RLock()
	ca := mjs.consumerAssignmentOrInflight(globalAccountName, "TEST", "DUR")
	var consumerPeers []string
	if ca != nil {
		consumerPeers = copyStrings(ca.Group.Peers)
	}
	mjs.mu.RUnlock()
	require_Len(t, len(consumerPeers), 5)

	cl := c.consumerLeader(globalAccountName, "TEST", "DUR")
	require_NotNil(t, cl)
	leaderPeer := srvPeer[cl]

	// Simulate which peers the old scale down to R3 would keep: the current
	// leader is moved to the end of the peer list, and the last three peers
	// of the list are kept.
	sim := copyStrings(consumerPeers)
	for i, p := range sim {
		if p == leaderPeer {
			sim[i] = sim[len(sim)-1]
			sim[len(sim)-1] = p
		}
	}
	keep := sim[len(sim)-3:]
	require_Equal(t, keep[2], leaderPeer)

	// Reconnect the client to the consumer leader, it remains running.
	nc.Close()
	nc, js = jsClientConnect(t, cl)
	defer nc.Close()

	// Shut down the two non-leader peers that the scale down will keep. The
	// other two peers, as well as the consumer leader, remain online.
	var offline []*Server
	for _, p := range keep[:2] {
		s := peerSrv[p]
		require_True(t, s != cl)
		s.Shutdown()
		offline = append(offline, s)
	}
	require_Len(t, len(offline), 2)

	// The meta leader might have been shut down, wait for a new one.
	c.waitOnLeader()
	ml = c.leader()

	// Wait for the meta leader to mark the stopped servers as offline.
	checkFor(t, 2*time.Second, 100*time.Millisecond, func() error {
		for _, s := range offline {
			if ni, ok := ml.nodeToInfo.Load(srvPeer[s]); !ok || !ni.(nodeInfo).offline {
				return fmt.Errorf("server %q not marked offline yet", s.Name())
			}
		}
		return nil
	})

	// The consumer kept quorum (3 of 5 peers online), the leader must not
	// have changed.
	c.waitOnConsumerLeader(globalAccountName, "TEST", "DUR")
	require_Equal(t, c.consumerLeader(globalAccountName, "TEST", "DUR").Name(), cl.Name())

	// Scale the consumer down to R3. The consumer leader and two more peers
	// are online, so the scale down must select those peers and not the
	// offline ones.
	_, err = js.UpdateConsumer("TEST", &nats.ConsumerConfig{
		Durable:   "DUR",
		AckPolicy: nats.AckExplicitPolicy,
		Replicas:  3,
	})
	require_NoError(t, err)

	// Wait for the scale down to be applied in the meta layer.
	mjs = ml.getJetStream()
	var newPeers []string
	checkFor(t, 2*time.Second, 100*time.Millisecond, func() error {
		mjs.mu.RLock()
		defer mjs.mu.RUnlock()
		ca := mjs.consumerAssignment(globalAccountName, "TEST", "DUR")
		if ca == nil {
			return fmt.Errorf("consumer assignment not found")
		}
		if len(ca.Group.Peers) != 3 {
			return fmt.Errorf("scale down not applied yet, still %d peers", len(ca.Group.Peers))
		}
		newPeers = copyStrings(ca.Group.Peers)
		return nil
	})

	// The leader must have been preserved.
	require_True(t, slices.Contains(newPeers, leaderPeer))

	// The new peer set must consist of online peers only.
	for _, s := range offline {
		if slices.Contains(newPeers, srvPeer[s]) {
			t.Fatalf("Scale down selected offline peer %q over online peers", s.Name())
		}
	}
}

func TestJetStreamClusterMetaSnapshotRecoveryRecreateStream(t *testing.T) {
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

	for range 1000 {
		_, err = js.Publish("foo", nil)
		require_NoError(t, err)
	}
	checkFor(t, 2*time.Second, 100*time.Millisecond, func() error {
		state, err := checkStateAndErr(t, c, globalAccountName, "TEST")
		if err != nil {
			return err
		}
		if state.Msgs != 1000 {
			return fmt.Errorf("not enough messages: %d", state.Msgs)
		}
		return nil
	})

	// Pick a stream replica that is not the meta leader and shut it down.
	rs := c.randomNonLeader()
	require_NotNil(t, rs)
	rs.Shutdown()
	c.waitOnStreamLeader(globalAccountName, "TEST")

	// Reconnect in case our client was on the downed server.
	nc.Close()
	nc, js = jsClientConnect(t, c.leader())
	defer nc.Close()

	// Delete and recreate the stream while the server is down.
	require_NoError(t, js.DeleteStream("TEST"))
	_, err = js.AddStream(cfg)
	require_NoError(t, err)

	for range 5 {
		_, err = js.Publish("foo", nil)
		require_NoError(t, err)
	}
	si, err := js.StreamInfo("TEST")
	require_NoError(t, err)
	require_Equal(t, si.State.Msgs, 5)

	// Compact the meta log so the downed server recovers via snapshot and
	// never replays the delete+create entries.
	require_NoError(t, c.leader().JetStreamSnapshotMeta())

	// Restart. The server must delete the old stream and create/catchup the new.
	rs = c.restartServer(rs)
	c.checkClusterFormed()
	c.waitOnServerCurrent(rs)

	checkFor(t, 5*time.Second, 100*time.Millisecond, func() error {
		state, err := checkStateAndErr(t, c, globalAccountName, "TEST")
		if err != nil {
			return err
		}
		if state.LastSeq > 5 {
			return fmt.Errorf("server still has stream state from previous incarnation: first=%d last=%d", state.FirstSeq, state.LastSeq)
		}
		return nil
	})
}

func TestJetStreamClusterMetaSnapshotRecoveryScaleStream(t *testing.T) {
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

	// Make sure the R1 stream is not hosted on the meta leader, so we can shut
	// down its host while keeping the meta leader around.
	sl := c.streamLeader(globalAccountName, "TEST")
	require_NotNil(t, sl)
	ml := c.leader()
	require_NotNil(t, ml)
	if sl == ml {
		meta := ml.getJetStream().getMetaGroup()
		require_NoError(t, meta.StepDown())
		c.waitOnLeader()
	}
	ml = c.leader()
	require_NotNil(t, ml)
	require_NotEqual(t, sl, ml)

	for range 100 {
		_, err = js.Publish("foo", nil)
		require_NoError(t, err)
	}

	// Capture the full peer set while all servers are still up.
	peers := ml.ActivePeers()
	require_Len(t, len(peers), 3)

	sl.Shutdown()
	sl.WaitForShutdown()

	// Scale the stream up to R3 in a way the downed server misses the update.
	// The public API refuses to update an offline stream, but a partition or a
	// crash right after the proposal has the same effect. Build and propose the
	// scaled-up assignment like jsClusteredStreamUpdateRequest would: this
	// renames the raft group and records the new peer set as desired state,
	// but keeps the assignment created time and the stream data.
	mljs := ml.getJetStream()
	mljs.mu.Lock()
	var osa, nsa *streamAssignment
	if osa = mljs.streamAssignmentOrInflight(globalAccountName, "TEST"); osa != nil {
		nsa = osa.copyGroup()
	}
	cc := mljs.cluster
	meta, term := cc.meta, cc.term
	mljs.mu.Unlock()
	require_NotNil(t, nsa)

	ncfg := *nsa.Config
	ncfg.Replicas = 3
	nsa.Config = &ncfg
	nsa.Group.Peers = peers
	nsa.Group.Name = groupNameForStream(peers, nsa.Group.Storage)
	nsa.Group = osa.Group.withDesired(nsa.Group)
	nsa.Group.populateOrigin(osa)
	require_NoError(t, meta.Propose(term, encodeUpdateStreamAssignment(nsa)))

	// Wait until the update is applied.
	checkFor(t, 2*time.Second, 100*time.Millisecond, func() error {
		mljs.mu.RLock()
		defer mljs.mu.RUnlock()
		if sa := mljs.streamAssignment(globalAccountName, "TEST"); sa == nil || sa.Group.Name != nsa.Group.Name {
			return fmt.Errorf("scaled-up assignment not applied yet")
		}
		return nil
	})

	// Compact the meta log so the downed server recovers via snapshot and
	// never replays the update entry.
	require_NoError(t, c.leader().JetStreamSnapshotMeta())

	// Restart. The server must attach its stream store to the renamed raft
	// group, it holds the only copy of the data.
	sl = c.restartServer(sl)
	c.checkClusterFormed()
	c.waitOnServerCurrent(sl)
	c.waitOnStreamLeader(globalAccountName, "TEST")

	checkFor(t, 5*time.Second, 100*time.Millisecond, func() error {
		state, err := checkStateAndErr(t, c, globalAccountName, "TEST")
		if err != nil {
			return err
		}
		if state.Msgs != 100 {
			return fmt.Errorf("stream data lost on scale up: %+v", state)
		}
		return nil
	})
}

func TestJetStreamClusterMetaSnapshotRecoveryScaleConsumer(t *testing.T) {
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

	for range 100 {
		_, err = js.Publish("foo", nil)
		require_NoError(t, err)
	}

	ccfg := &nats.ConsumerConfig{
		Durable:   "DUR",
		AckPolicy: nats.AckExplicitPolicy,
		Replicas:  1,
	}
	_, err = js.AddConsumer("TEST", ccfg)
	require_NoError(t, err)

	// Make sure the R1 consumer is not hosted on the meta leader, so we can
	// shut down its host while keeping the meta leader around.
	cl := c.consumerLeader(globalAccountName, "TEST", "DUR")
	require_NotNil(t, cl)
	ml := c.leader()
	require_NotNil(t, ml)
	if cl == ml {
		meta := ml.getJetStream().getMetaGroup()
		require_NoError(t, meta.StepDown())
		c.waitOnLeader()
	}
	ml = c.leader()
	require_NotNil(t, ml)
	require_NotEqual(t, cl, ml)

	// Give the consumer some state that must survive the scale below.
	sub, err := js.PullSubscribe("foo", "DUR")
	require_NoError(t, err)
	msgs, err := sub.Fetch(10)
	require_NoError(t, err)
	require_Len(t, len(msgs), 10)
	for _, m := range msgs {
		require_NoError(t, m.AckSync())
	}

	// Capture the full peer set while all servers are still up.
	peers := ml.ActivePeers()
	require_Len(t, len(peers), 3)

	cl.Shutdown()
	cl.WaitForShutdown()
	c.waitOnStreamLeader(globalAccountName, "TEST")

	// Scale the consumer up to R3 in a way the downed server misses the update.
	// The public API refuses to update an offline consumer, but a partition or a
	// crash right after the proposal has the same effect. Build and propose the
	// scaled-up assignment like jsClusteredConsumerRequest would: this renames
	// the raft group and records the new peer set as desired state, but keeps
	// the assignment created time and consumer state.
	mljs := ml.getJetStream()
	mljs.mu.Lock()
	var oca, nca *consumerAssignment
	if oca = mljs.consumerAssignmentOrInflight(globalAccountName, "TEST", "DUR"); oca != nil {
		nca = oca.copyGroup()
	}
	cc := mljs.cluster
	meta, term := cc.meta, cc.term
	mljs.mu.Unlock()
	require_NotNil(t, nca)

	ncfg := *nca.Config
	ncfg.Replicas = 3
	nca.Config = &ncfg
	nca.Group.Peers = peers
	nca.Group.Name = groupNameForConsumer(peers, nca.Group.Storage)
	nca.Group = oca.Group.withDesired(nca.Group)
	require_NoError(t, meta.Propose(term, encodeAddConsumerAssignment(nca)))

	// Wait until the update is applied.
	checkFor(t, 2*time.Second, 100*time.Millisecond, func() error {
		mljs.mu.RLock()
		defer mljs.mu.RUnlock()
		if ca := mljs.consumerAssignment(globalAccountName, "TEST", "DUR"); ca == nil || ca.Group.Name != nca.Group.Name {
			return fmt.Errorf("scaled-up assignment not applied yet")
		}
		return nil
	})

	// Compact the meta log so the downed server recovers via snapshot and
	// never replays the update entry.
	require_NoError(t, c.leader().JetStreamSnapshotMeta())

	// Restart. The server must attach its consumer store to the renamed raft
	// group, it holds the only copy of the consumer state.
	cl = c.restartServer(cl)
	c.checkClusterFormed()
	c.waitOnServerCurrent(cl)
	c.waitOnConsumerLeader(globalAccountName, "TEST", "DUR")

	checkFor(t, 5*time.Second, 100*time.Millisecond, func() error {
		mset, err := cl.globalAccount().lookupStream("TEST")
		if err != nil {
			return err
		}
		o := mset.lookupConsumer("DUR")
		if o == nil {
			return fmt.Errorf("consumer not found")
		}
		state, err := o.store.State()
		if err != nil {
			return err
		}
		if state.Delivered.Consumer != 10 {
			return fmt.Errorf("consumer state lost on scale up: %+v", state)
		}
		return nil
	})
}

func TestJetStreamClusterDesiredOriginRetention(t *testing.T) {
	// Builds the assignment as it exists prior to the update, optionally already
	// having desired state with a recorded origin retention.
	newAssignment := func(retention RetentionPolicy, origin *RetentionPolicy, peers ...string) *streamAssignment {
		if len(peers) == 0 {
			peers = []string{"S1", "S2", "S3"}
		}
		osa := &streamAssignment{
			Config: &StreamConfig{Name: "TEST", Retention: retention, Replicas: len(peers)},
			Group:  &raftGroup{Name: "G", Peers: peers, Cluster: "C1"},
		}
		if origin != nil {
			osa.Group.Desired = &desiredRaftGroup{
				ID:    "ID",
				Peers: osa.Group.Peers,
				Origin: &desiredRaftGroupOrigin{
					Peers:    osa.Group.Peers,
					Cluster:  osa.Group.Cluster,
					Replicas: osa.Config.Replicas,
					// The stream was already moving toward its config retention, the origin
					// retention is what remains active until the desired state is reached.
					Retention: origin,
				},
			}
		}
		return osa
	}

	// Note that changing retention to/from WorkQueue is currently rejected by config
	// validation, but the origin retention logic itself is retention agnostic.
	limits, interest, workQueue := LimitsPolicy, InterestPolicy, WorkQueuePolicy
	for _, test := range []struct {
		name string
		// Retention of the stream config prior to the update.
		retention RetentionPolicy
		// Origin retention that's already recorded (if any).
		origin *RetentionPolicy
		// Retention the user wants to move to.
		newRetention RetentionPolicy
		// Origin retention that must be recorded after the update.
		expected RetentionPolicy
	}{
		{
			// Consumers must be scaled up to have parity with the stream before the
			// stream can truly become Interest, so remain on Limits until then.
			name: "LimitsToInterest", retention: limits, newRetention: interest, expected: limits,
		},
		{
			name: "LimitsToWorkQueue", retention: limits, newRetention: workQueue, expected: limits,
		},
		{
			// Interest already requires consumer parity, but the origin must still be
			// recorded so a cancel can revert to it.
			name: "InterestToWorkQueue", retention: interest, newRetention: workQueue, expected: interest,
		},
		{
			// The origin must be the retention from before any desired state changes
			// were made, so it can't be overwritten by a subsequent change.
			name: "LimitsToInterestToWorkQueue", retention: interest, origin: &limits,
			newRetention: workQueue, expected: limits,
		},
		{
			// Moving to Limits is not restrictive and applies immediately, but the origin
			// must still be recorded so a cancel can revert to Interest.
			name: "InterestToLimits", retention: interest, newRetention: limits, expected: interest,
		},
		{
			// Same, but now the origin was recorded by a previous change and must be kept.
			name: "InterestToWorkQueueToLimits", retention: workQueue, origin: &interest,
			newRetention: limits, expected: interest,
		},
		{
			// Moving back to where we came from keeps the origin, it's harmless.
			name: "LimitsToInterestToLimits", retention: interest, origin: &limits,
			newRetention: limits, expected: limits,
		},
		{
			// The stream must not act under Interest while consumers scale down, so the
			// recorded origin must not become active again on a subsequent change.
			name: "InterestToLimitsToInterest", retention: limits, origin: &interest,
			newRetention: interest, expected: interest,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			osa := newAssignment(test.retention, test.origin)
			rg := osa.copyGroup().Group.withRetentionChange(osa, test.newRetention)

			newCfg := osa.Config.clone()
			newCfg.Retention = test.newRetention

			// A retention change must always register desired state, and it must always
			// have an origin recorded, or it can't be rolled back or canceled.
			require_NotNil(t, rg.Desired)
			require_NotNil(t, rg.Desired.Origin)
			require_NotNil(t, rg.Desired.Origin.Retention)
			require_Equal(t, *rg.Desired.Origin.Retention, test.expected)
			// Any retention change happens through Limits, the stream acts under it until
			// the desired state is reached, regardless of origin and target retention.
			require_Equal(t, newCfg.atDesiredOrigin(rg).Retention, limits)
		})
	}

	// An unchanged retention must be left alone entirely.
	t.Run("NoRetentionChange", func(t *testing.T) {
		osa := newAssignment(interest, nil)
		rg := osa.copyGroup().Group.withRetentionChange(osa, interest)
		require_True(t, rg.Desired == nil)
	})

	// A singleton has no consumers to scale up first, so it can be applied immediately.
	t.Run("Singleton", func(t *testing.T) {
		osa := newAssignment(limits, nil, "S1")
		rg := osa.copyGroup().Group.withRetentionChange(osa, interest)
		require_True(t, rg.Desired == nil)

		newCfg := osa.Config.clone()
		newCfg.Retention = interest
		require_Equal(t, newCfg.atDesiredOrigin(rg).Retention, interest)
	})

	// But a singleton that's already moving or scaling must still go through desired state.
	t.Run("SingletonWithDesiredState", func(t *testing.T) {
		osa := newAssignment(limits, nil, "S1")
		rg := osa.Group.withDesired(osa.copyGroup().Group)
		rg = rg.withRetentionChange(osa, interest)
		require_NotNil(t, rg.Desired)
		require_NotNil(t, rg.Desired.Origin)
		require_NotNil(t, rg.Desired.Origin.Retention)
		require_Equal(t, *rg.Desired.Origin.Retention, limits)
	})

	// The origin retention must survive further desired state changes, or the stream
	// would flip to the config's retention before consumers have parity.
	t.Run("SurvivesWithDesired", func(t *testing.T) {
		osa := newAssignment(limits, nil)
		rg := osa.copyGroup().Group.withRetentionChange(osa, interest)
		// A scale stacked onto it re-registers the desired state.
		rg = rg.withDesired(rg.copyGroup())
		require_NotNil(t, rg.Desired.Origin)
		require_NotNil(t, rg.Desired.Origin.Retention)
		require_Equal(t, *rg.Desired.Origin.Retention, limits)
		newCfg := osa.Config.clone()
		newCfg.Retention = interest
		require_Equal(t, newCfg.atDesiredOrigin(rg).Retention, limits)
	})
}

// A retention change only needs desired state if a consumer must be remapped first. If all
// consumers already have parity with the stream, the change can be applied in one go. The
// stream update request decides that by dry-running the consumer remap, exercised here.
func TestJetStreamClusterRetentionChangeConvergedConsumers(t *testing.T) {
	const a, b, c, d = "A", "B", "C", "D"
	streamPeers := []string{a, b, c}

	newStream := func(retention RetentionPolicy) *streamAssignment {
		return &streamAssignment{
			Config:    &StreamConfig{Name: "TEST", Retention: retention, Replicas: 3},
			Group:     &raftGroup{Name: "S", Peers: streamPeers, Cluster: "C1"},
			consumers: map[string]*consumerAssignment{},
		}
	}
	newConsumer := func(name string, cfg *ConsumerConfig, peers ...string) *consumerAssignment {
		return &consumerAssignment{
			Name:   name,
			Stream: "TEST",
			Config: cfg,
			Group:  &raftGroup{Name: "C", Peers: peers, Cluster: "C1"},
		}
	}
	durable := func(name string, replicas int) *ConsumerConfig {
		return &ConsumerConfig{Durable: name, Replicas: replicas}
	}

	limits, interest := LimitsPolicy, InterestPolicy
	for _, test := range []struct {
		name      string
		retention RetentionPolicy
		target    RetentionPolicy
		consumers []*consumerAssignment
		inflight  []*consumerAssignment
		desired   bool // The stream itself already has desired state.
		converged bool
	}{
		{
			name: "NoConsumers", retention: limits, target: interest, converged: true,
		},
		{
			// A durable without an explicit replica count follows the stream's.
			name: "DurableDefaultReplicas", retention: limits, target: interest,
			consumers: []*consumerAssignment{newConsumer("C", durable("C", 0), a, b, c)},
			converged: true,
		},
		{
			name: "DurableAtParity", retention: limits, target: interest,
			consumers: []*consumerAssignment{newConsumer("C", durable("C", 3), c, a, b)},
			converged: true,
		},
		{
			// Going back to Limits needs no consumer scaled down either.
			name: "DurableAtParityToLimits", retention: interest, target: limits,
			consumers: []*consumerAssignment{newConsumer("C", durable("C", 3), a, b, c)},
			converged: true,
		},
		{
			// An R1 durable must be scaled up first.
			name: "DurableBelowParity", retention: limits, target: interest,
			consumers: []*consumerAssignment{newConsumer("C", durable("C", 1), a)},
		},
		{
			// A legacy ephemeral is R1 while the stream is Limits, it must be scaled up.
			name: "LegacyEphemeralScaleUp", retention: limits, target: interest,
			consumers: []*consumerAssignment{newConsumer("E", &ConsumerConfig{}, a)},
		},
		{
			// And scaled back down to R1 when going back to Limits.
			name: "LegacyEphemeralScaleDown", retention: interest, target: limits,
			consumers: []*consumerAssignment{newConsumer("E", &ConsumerConfig{}, a, b, c)},
		},
		{
			// The right count, but not on the stream's peers.
			name: "OffStreamPeers", retention: limits, target: interest,
			consumers: []*consumerAssignment{newConsumer("C", durable("C", 3), a, b, d)},
		},
		{
			// Still moving toward its desired peer set.
			name: "ConsumerConverging", retention: limits, target: interest,
			consumers: func() []*consumerAssignment {
				ca := newConsumer("C", durable("C", 3), a, b, c)
				ca.Group.Desired = &desiredRaftGroup{ID: "ID", Peers: []string{a, b, c}}
				return []*consumerAssignment{ca}
			}(),
		},
		{
			// Inflight consumers must be considered as well.
			name: "InflightBelowParity", retention: limits, target: interest,
			consumers: []*consumerAssignment{newConsumer("C", durable("C", 3), a, b, c)},
			inflight:  []*consumerAssignment{newConsumer("I", durable("I", 1), b)},
		},
		{
			name: "InflightAtParity", retention: limits, target: interest,
			inflight:  []*consumerAssignment{newConsumer("I", durable("I", 3), a, b, c)},
			converged: true,
		},
		{
			// A change stacked onto a scale must converge through desired state.
			name: "StreamHasDesiredState", retention: limits, target: interest,
			consumers: []*consumerAssignment{newConsumer("C", durable("C", 3), a, b, c)},
			desired:   true,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			osa := newStream(test.retention)
			for _, ca := range test.consumers {
				osa.consumers[ca.Name] = ca
			}
			inflight := map[string]*inflightConsumerInfo{}
			for _, ca := range test.inflight {
				inflight[ca.Name] = &inflightConsumerInfo{consumerAssignment: ca}
			}
			js := &jetStream{cluster: &jetStreamCluster{
				streams: map[string]map[string]*streamAssignment{
					globalAccountName: {"TEST": osa},
				},
				inflightConsumers: map[string]map[string]map[string]*inflightConsumerInfo{
					globalAccountName: {"TEST": inflight},
				},
			}}

			rg := osa.copyGroup().Group
			if test.desired {
				rg = osa.Group.withDesired(rg)
			}
			newCfg := osa.Config.clone()
			newCfg.Retention = test.target

			// The dry-run as done by the stream update request.
			var converged bool
			if rg.Desired == nil {
				tsa := &streamAssignment{Group: rg, Config: newCfg}
				consumers, deleted, done := js.remapConsumerAssignments(globalAccountName, tsa)
				converged = done && len(consumers) == 0 && len(deleted) == 0
			}
			require_Equal(t, converged, test.converged)

			// The dry-run must not have touched the assignments.
			require_Equal(t, osa.Config.Retention, test.retention)
			for _, ca := range test.consumers {
				require_True(t, slices.Equal(ca.Group.Peers, osa.consumers[ca.Name].Group.Peers))
				require_Equal(t, ca.Config.Replicas, osa.consumers[ca.Name].Config.Replicas)
			}

			// A converged change applies in one go, otherwise desired state is registered.
			rg = rg.withRetentionChange(osa, newCfg.Retention)
			if converged {
				// Only the caller skips desired state, withRetentionChange itself is
				// unaware. Verify the config would be applied as-is without it.
				require_Equal(t, newCfg.atDesiredOrigin(osa.Group).Retention, test.target)
				return
			}
			require_NotNil(t, rg.Desired)
			require_NotNil(t, rg.Desired.Origin)
		})
	}
}

// Moving into a more restrictive retention must keep the previous retention active
// until all consumers have been scaled up to have parity with the stream.
func TestJetStreamClusterDesiredOriginRetentionScaleUpFirst(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "R3S", 3)
	defer c.shutdown()

	ml := c.leader()
	nc, js := jsClientConnect(t, ml)
	defer nc.Close()

	_, err := js.AddStream(&nats.StreamConfig{
		Name:      "TEST",
		Subjects:  []string{"foo"},
		Retention: nats.LimitsPolicy,
		Replicas:  3,
	})
	require_NoError(t, err)

	// An R1 consumer, it must be scaled up to R3 before the stream can become Interest.
	_, err = js.AddConsumer("TEST", &nats.ConsumerConfig{
		Durable:   "C",
		AckPolicy: nats.AckExplicitPolicy,
		Replicas:  1,
	})
	require_NoError(t, err)

	_, err = js.UpdateStream(&nats.StreamConfig{
		Name:      "TEST",
		Subjects:  []string{"foo"},
		Retention: nats.InterestPolicy,
		Replicas:  3,
	})
	require_NoError(t, err)

	mjs := ml.getJetStream()
	// While the desired state is pending the stream must remain on Limits, and only
	// flip to Interest once the desired state is reached.
	checkFor(t, 10*time.Second, 50*time.Millisecond, func() error {
		mjs.mu.RLock()
		defer mjs.mu.RUnlock()
		sa := mjs.streamAssignment(globalAccountName, "TEST")
		if sa == nil {
			return fmt.Errorf("stream assignment not found")
		}
		// The config always contains the retention the user wants to move to.
		if sa.Config.Retention != InterestPolicy {
			return fmt.Errorf("expected config retention to be Interest, got %v", sa.Config.Retention)
		}
		if sa.Group.Desired != nil {
			// Not converged yet, must still be on Limits.
			if r := sa.Config.atDesiredOrigin(sa.Group).Retention; r != LimitsPolicy {
				return fmt.Errorf("expected effective retention to remain Limits, got %v", r)
			}
			return fmt.Errorf("desired state still pending")
		}
		// Converged, the consumer must have parity with the stream.
		ca := mjs.consumerAssignment(globalAccountName, "TEST", "C")
		if ca == nil {
			return fmt.Errorf("consumer assignment not found")
		}
		if len(ca.Group.Peers) != 3 {
			return fmt.Errorf("expected consumer to be scaled up to 3 peers, got %d", len(ca.Group.Peers))
		}
		return nil
	})

	// And all replicas must now be on Interest. The assignment is replicated, so the
	// members don't all apply it at the same time.
	checkFor(t, 5*time.Second, 100*time.Millisecond, func() error {
		for _, s := range c.servers {
			mset, err := s.globalAccount().lookupStream("TEST")
			if err != nil {
				return err
			}
			if r := mset.config().Retention; r != InterestPolicy {
				return fmt.Errorf("expected Interest on all members, got %v", r)
			}
		}
		return nil
	})
}

// Moving into Limits is not restrictive and must be applied immediately, even if a
// previous change had recorded an origin retention.
func TestJetStreamClusterDesiredOriginRetentionBackToLimits(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "R3S", 3)
	defer c.shutdown()

	ml := c.leader()
	nc, js := jsClientConnect(t, ml)
	defer nc.Close()

	_, err := js.AddStream(&nats.StreamConfig{
		Name:      "TEST",
		Subjects:  []string{"foo"},
		Retention: nats.LimitsPolicy,
		Replicas:  3,
	})
	require_NoError(t, err)

	// An R1 consumer, it must be scaled up before the stream can become Interest.
	_, err = js.AddConsumer("TEST", &nats.ConsumerConfig{
		Durable:   "C",
		AckPolicy: nats.AckExplicitPolicy,
		Replicas:  1,
	})
	require_NoError(t, err)

	// Move to Interest first, this records Limits as the origin retention.
	_, err = js.UpdateStream(&nats.StreamConfig{
		Name:      "TEST",
		Subjects:  []string{"foo"},
		Retention: nats.InterestPolicy,
		Replicas:  3,
	})
	require_NoError(t, err)

	mjs := ml.getJetStream()
	// Snapshot the state, we must not assert while holding the lock.
	snapshot := func() (retention, effective RetentionPolicy, origin *RetentionPolicy, pending bool) {
		mjs.mu.RLock()
		defer mjs.mu.RUnlock()
		sa := mjs.streamAssignmentOrInflight(globalAccountName, "TEST")
		require_NotNil(t, sa)
		if d := sa.Group.Desired; d != nil {
			pending = true
			// Pending desired state must always have an origin, or it can't be canceled.
			require_NotNil(t, d.Origin)
			if d.Origin.Retention != nil {
				r := *d.Origin.Retention
				origin = &r
			}
		}
		return sa.Config.Retention, sa.Config.atDesiredOrigin(sa.Group).Retention, origin, pending
	}

	// Either we're still moving toward Interest, in which case the origin must hold
	// Limits, or we've already converged and the origin is gone entirely.
	retention, _, origin, pending := snapshot()
	require_Equal(t, retention, InterestPolicy)
	if pending {
		require_NotNil(t, origin)
		require_Equal(t, *origin, LimitsPolicy)
	}

	// And then move back to Limits, which must be applied immediately.
	_, err = js.UpdateStream(&nats.StreamConfig{
		Name:      "TEST",
		Subjects:  []string{"foo"},
		Retention: nats.LimitsPolicy,
		Replicas:  3,
	})
	require_NoError(t, err)

	// The effective retention must be Limits regardless of the desired state still being
	// pending. The origin is kept, it remains what a cancel would revert to.
	retention, effective, origin, pending := snapshot()
	require_Equal(t, retention, LimitsPolicy)
	require_Equal(t, effective, LimitsPolicy)
	if pending {
		require_NotNil(t, origin)
		require_Equal(t, *origin, LimitsPolicy)
	}

	// Must fully converge without ever acting under Interest.
	checkFor(t, 10*time.Second, 50*time.Millisecond, func() error {
		mjs.mu.RLock()
		defer mjs.mu.RUnlock()
		sa := mjs.streamAssignment(globalAccountName, "TEST")
		if sa == nil {
			return fmt.Errorf("stream assignment not found")
		}
		if r := sa.Config.atDesiredOrigin(sa.Group).Retention; r != LimitsPolicy {
			return fmt.Errorf("expected effective retention to be Limits, got %v", r)
		}
		if sa.Group.Desired != nil {
			return fmt.Errorf("desired state still pending")
		}
		return nil
	})

	checkFor(t, 5*time.Second, 100*time.Millisecond, func() error {
		for _, s := range c.servers {
			mset, err := s.globalAccount().lookupStream("TEST")
			if err != nil {
				return err
			}
			if r := mset.config().Retention; r != LimitsPolicy {
				return fmt.Errorf("expected Limits on all members, got %v", r)
			}
		}
		return nil
	})
}

// Moving out of Interest records it as the origin, so the change can be canceled back to it.
// The stream must act under Limits throughout, both while consumers scale down and while they
// scale back up after the cancel, and only act under Interest again once they have parity.
func TestJetStreamClusterDesiredOriginRetentionCancelBackToInterest(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "R3S", 3)
	defer c.shutdown()

	ml := c.leader()
	nc, js := jsClientConnect(t, ml)
	defer nc.Close()

	_, err := js.AddStream(&nats.StreamConfig{
		Name:      "TEST",
		Subjects:  []string{"foo"},
		Retention: nats.InterestPolicy,
		Replicas:  3,
	})
	require_NoError(t, err)

	// A legacy ephemeral, it has parity with the stream under Interest and must be scaled
	// back down to R1 under Limits.
	ci, err := js.AddConsumer("TEST", &nats.ConsumerConfig{
		AckPolicy:         nats.AckExplicitPolicy,
		InactiveThreshold: time.Minute,
	})
	require_NoError(t, err)
	cname := ci.Name

	mjs := ml.getJetStream()
	consumerPeers := func() int {
		mjs.mu.RLock()
		defer mjs.mu.RUnlock()
		ca := mjs.consumerAssignment(globalAccountName, "TEST", cname)
		if ca == nil {
			return -1
		}
		return len(ca.Group.Peers)
	}
	require_Equal(t, consumerPeers(), 3)

	// Block the meta leader from reconciling desired state, so the consumer can't be
	// scaled down and the change stays in flight deterministically.
	mjs.mu.Lock()
	streamReconcile := mjs.cluster.streamReconcile
	mjs.cluster.streamReconcile = nil
	mjs.mu.Unlock()
	require_NotNil(t, streamReconcile)
	ml.sysUnsubscribe(streamReconcile)

	_, err = js.UpdateStream(&nats.StreamConfig{
		Name:      "TEST",
		Subjects:  []string{"foo"},
		Retention: nats.LimitsPolicy,
		Replicas:  3,
	})
	require_NoError(t, err)

	// Snapshot the state, we must not assert while holding the lock.
	snapshot := func() (retention, effective RetentionPolicy, origin *RetentionPolicy, pending bool) {
		mjs.mu.RLock()
		defer mjs.mu.RUnlock()
		sa := mjs.streamAssignmentOrInflight(globalAccountName, "TEST")
		require_NotNil(t, sa)
		if d := sa.Group.Desired; d != nil {
			pending = true
			require_NotNil(t, d.Origin)
			if d.Origin.Retention != nil {
				r := *d.Origin.Retention
				origin = &r
			}
		}
		return sa.Config.Retention, sa.Config.atDesiredOrigin(sa.Group).Retention, origin, pending
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

	// The change is in flight, Limits applies immediately and Interest is the origin.
	require_True(t, ml == c.leader())
	retention, effective, origin, pending := snapshot()
	require_True(t, pending)
	require_Equal(t, retention, LimitsPolicy)
	require_Equal(t, effective, LimitsPolicy)
	require_NotNil(t, origin)
	require_Equal(t, *origin, InterestPolicy)
	checkFor(t, 5*time.Second, 100*time.Millisecond, func() error { return membersAt(LimitsPolicy) })

	// Cancel, the config must revert to Interest right away. But the members must keep acting
	// under Limits, the consumer might have been scaled down already.
	rmsg, err := nc.Request(fmt.Sprintf(JSApiStreamCancelMoveT, "TEST"), nil, 5*time.Second)
	require_NoError(t, err)
	var resp JSApiStreamCancelMoveResponse
	require_NoError(t, json.Unmarshal(rmsg.Data, &resp))
	require_Equal(t, resp.Type, JSApiStreamCancelMoveResponseType)
	require_True(t, resp.Error == nil)

	require_True(t, ml == c.leader())
	checkFor(t, 5*time.Second, 50*time.Millisecond, func() error {
		retention, effective, origin, pending := snapshot()
		if retention != InterestPolicy {
			return fmt.Errorf("config not reverted yet, at %v", retention)
		}
		if !pending || effective != LimitsPolicy || origin == nil || *origin != InterestPolicy {
			t.Fatalf("Unexpected state after cancel: pending=%v effective=%v origin=%v", pending, effective, origin)
		}
		return nil
	})
	require_NoError(t, membersAt(LimitsPolicy))

	// A plain config update while the rollback is still pending must still answer as a stream update.
	si, err := js.StreamInfo("TEST")
	require_NoError(t, err)
	ucfg := si.Config
	ucfg.MaxMsgs = 1_000_000
	req, err := json.Marshal(ucfg)
	require_NoError(t, err)
	rmsg, err = nc.Request(fmt.Sprintf(JSApiStreamUpdateT, "TEST"), req, 5*time.Second)
	require_NoError(t, err)
	var uresp JSApiStreamUpdateResponse
	require_NoError(t, json.Unmarshal(rmsg.Data, &uresp))
	require_Equal(t, uresp.Type, JSApiStreamUpdateResponseType)
	require_True(t, uresp.Error == nil)
	require_Equal(t, uresp.Config.MaxMsgs, 1_000_000)
	retention, _, origin, pending = snapshot()
	require_True(t, pending)
	require_Equal(t, retention, InterestPolicy)
	require_NotNil(t, origin)

	// Unblock reconciling, the rollback must converge to Interest with the consumer at parity.
	require_True(t, ml == c.leader())
	mjs.mu.Lock()
	mjs.startUpdatesSub()
	mjs.mu.Unlock()
	checkFor(t, 10*time.Second, 50*time.Millisecond, func() error {
		mjs.mu.RLock()
		sa := mjs.streamAssignment(globalAccountName, "TEST")
		mjs.mu.RUnlock()
		if sa == nil {
			return fmt.Errorf("stream assignment not found")
		}
		if sa.Group.Desired != nil {
			// A member must never act under Interest while the consumer lacks parity.
			if consumerPeers() < 3 {
				for _, s := range c.servers {
					mset, err := s.globalAccount().lookupStream("TEST")
					if err == nil && mset != nil && mset.isInterestRetention() {
						t.Fatalf("Server %q acts under Interest while consumer is on %d peer(s)", s.Name(), consumerPeers())
					}
				}
			}
			return fmt.Errorf("desired state still pending")
		}
		if sa.Config.Retention != InterestPolicy {
			return fmt.Errorf("expected Interest, got %v", sa.Config.Retention)
		}
		if p := consumerPeers(); p != 3 {
			return fmt.Errorf("expected consumer on 3 peers, got %d", p)
		}
		return membersAt(InterestPolicy)
	})
}

// A singleton without desired state applies retention changes immediately.
func TestJetStreamClusterDesiredOriginRetentionSingleton(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "R3S", 3)
	defer c.shutdown()

	ml := c.leader()
	nc, js := jsClientConnect(t, ml)
	defer nc.Close()

	_, err := js.AddStream(&nats.StreamConfig{
		Name:      "TEST",
		Subjects:  []string{"foo"},
		Retention: nats.LimitsPolicy,
		Replicas:  1,
	})
	require_NoError(t, err)
	c.waitOnStreamLeader(globalAccountName, "TEST")

	_, err = js.UpdateStream(&nats.StreamConfig{
		Name:      "TEST",
		Subjects:  []string{"foo"},
		Retention: nats.InterestPolicy,
		Replicas:  1,
	})
	require_NoError(t, err)

	mjs := ml.getJetStream()
	checkFor(t, 5*time.Second, 50*time.Millisecond, func() error {
		mjs.mu.RLock()
		defer mjs.mu.RUnlock()
		sa := mjs.streamAssignment(globalAccountName, "TEST")
		if sa == nil {
			return fmt.Errorf("stream assignment not found")
		}
		if sa.Config.Retention != InterestPolicy {
			return fmt.Errorf("expected config retention to be Interest, got %v", sa.Config.Retention)
		}
		if sa.Group.Desired != nil {
			return fmt.Errorf("unexpected desired state for singleton")
		}
		if r := sa.Config.atDesiredOrigin(sa.Group).Retention; r != InterestPolicy {
			return fmt.Errorf("expected effective retention to be Interest, got %v", r)
		}
		return nil
	})
}

// An ephemeral consumer that auto-scales (Replicas:0) must follow the stream's
// retention: R1 while the stream is Limits, and scaled up to have parity while
// the stream is Interest.
func TestJetStreamClusterDesiredOriginRetentionEphemeralAutoScale(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "R3S", 3)
	defer c.shutdown()

	ml := c.leader()
	nc, js := jsClientConnect(t, ml)
	defer nc.Close()

	_, err := js.AddStream(&nats.StreamConfig{
		Name:      "TEST",
		Subjects:  []string{"foo"},
		Retention: nats.LimitsPolicy,
		Replicas:  3,
	})
	require_NoError(t, err)

	// An old-school ephemeral, it auto-scales with the stream's retention.
	sub, err := js.SubscribeSync("foo")
	require_NoError(t, err)
	defer sub.Unsubscribe()
	ci, err := sub.ConsumerInfo()
	require_NoError(t, err)
	name := ci.Name

	mjs := ml.getJetStream()
	consumerPeers := func() int {
		mjs.mu.RLock()
		defer mjs.mu.RUnlock()
		ca := mjs.consumerAssignment(globalAccountName, "TEST", name)
		if ca == nil {
			return -1
		}
		return len(ca.Group.Peers)
	}
	requirePeers := func(t *testing.T, expected int) {
		t.Helper()
		checkFor(t, 10*time.Second, 100*time.Millisecond, func() error {
			mjs.mu.RLock()
			sa := mjs.streamAssignment(globalAccountName, "TEST")
			pending := sa == nil || sa.Group.Desired != nil
			mjs.mu.RUnlock()
			if pending {
				return fmt.Errorf("desired state still pending")
			}
			if p := consumerPeers(); p != expected {
				return fmt.Errorf("expected consumer to have %d peers, got %d", expected, p)
			}
			return nil
		})
	}

	// Ephemerals are R1 while the stream is Limits.
	requirePeers(t, 1)

	// Interest requires parity with the stream, so it must be scaled up.
	_, err = js.UpdateStream(&nats.StreamConfig{
		Name:      "TEST",
		Subjects:  []string{"foo"},
		Retention: nats.InterestPolicy,
		Replicas:  3,
	})
	require_NoError(t, err)
	requirePeers(t, 3)

	// And scaled back down once the stream is Limits again.
	_, err = js.UpdateStream(&nats.StreamConfig{
		Name:      "TEST",
		Subjects:  []string{"foo"},
		Retention: nats.LimitsPolicy,
		Replicas:  3,
	})
	require_NoError(t, err)
	requirePeers(t, 1)
}

// A read-modify-write of an unrelated config field must not cancel a pending retention
// change, since clients are reported the requested config they then send back.
func TestJetStreamClusterDesiredOriginRetentionSurvivesConfigEdit(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "R3S", 3)
	defer c.shutdown()

	ml := c.leader()
	nc, js := jsClientConnect(t, ml)
	defer nc.Close()

	_, err := js.AddStream(&nats.StreamConfig{
		Name:      "TEST",
		Subjects:  []string{"foo"},
		Retention: nats.LimitsPolicy,
		Replicas:  3,
	})
	require_NoError(t, err)

	// An R1 consumer, it must be scaled up before the stream can become Interest.
	_, err = js.AddConsumer("TEST", &nats.ConsumerConfig{
		Durable:   "C",
		AckPolicy: nats.AckExplicitPolicy,
		Replicas:  1,
	})
	require_NoError(t, err)

	// Block the consumer from finishing its scale-up by stopping the meta leader from
	// reconciling consumer assignments.
	mjs := ml.getJetStream()
	mjs.mu.Lock()
	consumerReconcile := mjs.cluster.consumerReconcile
	mjs.cluster.consumerReconcile = nil
	mjs.mu.Unlock()
	require_NotNil(t, consumerReconcile)
	ml.sysUnsubscribe(consumerReconcile)

	_, err = js.UpdateStream(&nats.StreamConfig{
		Name:      "TEST",
		Subjects:  []string{"foo"},
		Retention: nats.InterestPolicy,
		Replicas:  3,
	})
	require_NoError(t, err)

	require_True(t, ml == c.leader())
	mjs.mu.RLock()
	sa := mjs.streamAssignmentOrInflight(globalAccountName, "TEST")
	wasPending := sa != nil && sa.Group.Desired != nil
	mjs.mu.RUnlock()
	require_True(t, wasPending)

	// Now edit an unrelated field, the way a client would: fetch, modify, update.
	si, err := js.StreamInfo("TEST")
	require_NoError(t, err)
	cfg := si.Config
	cfg.Description = "edited"
	_, err = js.UpdateStream(&cfg)
	require_NoError(t, err)

	// Unblock the consumer scale-up so the stream can converge.
	require_True(t, ml == c.leader())
	mjs.mu.Lock()
	mjs.startUpdatesSub()
	mjs.mu.Unlock()

	// The retention change must not have been canceled by the edit.
	checkFor(t, 10*time.Second, 100*time.Millisecond, func() error {
		mjs.mu.RLock()
		defer mjs.mu.RUnlock()
		sa := mjs.streamAssignment(globalAccountName, "TEST")
		if sa == nil {
			return fmt.Errorf("stream assignment not found")
		}
		if sa.Config.Retention != InterestPolicy {
			return fmt.Errorf("retention change was canceled, got %v", sa.Config.Retention)
		}
		if sa.Group.Desired != nil {
			return fmt.Errorf("desired state still pending")
		}
		return nil
	})

	checkFor(t, 5*time.Second, 100*time.Millisecond, func() error {
		for _, s := range c.servers {
			mset, err := s.globalAccount().lookupStream("TEST")
			if err != nil {
				return err
			}
			cfg := mset.config()
			if cfg.Retention != InterestPolicy || cfg.Description != "edited" {
				return fmt.Errorf("members not caught up: %v/%q", cfg.Retention, cfg.Description)
			}
		}
		return nil
	})
}

// The running config is at its origin, while the config as requested is the target.
// Both placement and retention must be reverted independently of one another.
func TestJetStreamClusterDesiredOriginTarget(t *testing.T) {
	origin := &Placement{Cluster: "C1"}
	target := &Placement{Cluster: "C2"}
	limits, interest := LimitsPolicy, InterestPolicy

	for _, test := range []struct {
		name string
		// The origin recorded when the desired state was registered.
		origin *desiredRaftGroupOrigin
		// Placement/retention as requested by the user.
		targetPlacement *Placement
		targetRetention RetentionPolicy
		// What the stream must run at while the desired state is pending.
		runPlacement *Placement
		runRetention RetentionPolicy
	}{
		{
			// Adding a placement is held back like any other move. A nil origin retention
			// instead means retention never changed, so the target retention stays.
			name:            "PlacementAddedNoRetentionChange",
			origin:          &desiredRaftGroupOrigin{},
			targetPlacement: target, targetRetention: interest,
			runPlacement: nil, runRetention: interest,
		},
		{
			// Only placement is held back, the retention was not changed.
			name:            "PlacementOnly",
			origin:          &desiredRaftGroupOrigin{Placement: origin},
			targetPlacement: target, targetRetention: interest,
			runPlacement: origin, runRetention: interest,
		},
		{
			// And only retention is held back, the placement was not changed.
			name:            "RetentionOnly",
			origin:          &desiredRaftGroupOrigin{Placement: target, Retention: &limits},
			targetPlacement: target, targetRetention: interest,
			runPlacement: target, runRetention: limits,
		},
		{
			// The origin retention is what a cancel reverts to, but it also marks the change
			// as in flight. The stream must act under Limits then, not under the origin.
			name:            "RetentionFromInterest",
			origin:          &desiredRaftGroupOrigin{Placement: target, Retention: &interest},
			targetPlacement: target, targetRetention: limits,
			runPlacement: target, runRetention: limits,
		},
		{
			// And the same when moving back toward Interest before the change converged.
			name:            "RetentionBackToInterest",
			origin:          &desiredRaftGroupOrigin{Placement: target, Retention: &interest},
			targetPlacement: target, targetRetention: interest,
			runPlacement: target, runRetention: limits,
		},
		{
			// Moving and changing retention at the same time holds back both.
			name:            "PlacementAndRetention",
			origin:          &desiredRaftGroupOrigin{Placement: origin, Retention: &limits},
			targetPlacement: target, targetRetention: interest,
			runPlacement: origin, runRetention: limits,
		},
		{
			// Removing placement must be held back as well.
			name:            "PlacementRemoved",
			origin:          &desiredRaftGroupOrigin{Placement: origin},
			targetPlacement: nil, targetRetention: limits,
			runPlacement: origin, runRetention: limits,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			targetCfg := &StreamConfig{Name: "TEST", Placement: test.targetPlacement, Retention: test.targetRetention}
			rg := &raftGroup{Name: "G", Desired: &desiredRaftGroup{ID: "ID", Origin: test.origin}}

			// What the stream runs at while the desired state is pending.
			runCfg := targetCfg.atDesiredOrigin(rg)
			require_Equal(t, runCfg.Retention, test.runRetention)
			if test.runPlacement == nil {
				require_True(t, runCfg.Placement == nil)
			} else {
				require_NotNil(t, runCfg.Placement)
				require_Equal(t, runCfg.Placement.Cluster, test.runPlacement.Cluster)
			}

			// Must be reported as requested, i.e. reverting back to the target.
			reported := runCfg.atDesiredTarget(targetCfg)
			require_Equal(t, reported.Retention, test.targetRetention)
			if test.targetPlacement == nil {
				require_True(t, reported.Placement == nil)
			} else {
				require_NotNil(t, reported.Placement)
				require_Equal(t, reported.Placement.Cluster, test.targetPlacement.Cluster)
			}
		})
	}

	// Without desired state there's nothing to revert.
	t.Run("NoDesiredState", func(t *testing.T) {
		cfg := &StreamConfig{Name: "TEST", Placement: target, Retention: interest}
		runCfg := cfg.atDesiredOrigin(&raftGroup{Name: "G"})
		require_Equal(t, runCfg.Placement.Cluster, target.Cluster)
		reported := runCfg.atDesiredTarget(cfg)
		require_Equal(t, reported.Placement.Cluster, target.Cluster)
		require_Equal(t, reported.Retention, interest)
	})
}

// The origin must capture the state from before any desired state changes were made,
// which for a legacy move is only encoded as an over-replicated peer set.
func TestJetStreamClusterDesiredOriginPopulate(t *testing.T) {
	newAssignment := func(peers []string, replicas int, desired *desiredRaftGroup) *streamAssignment {
		return &streamAssignment{
			Config: &StreamConfig{Name: "TEST", Retention: LimitsPolicy, Replicas: replicas},
			Group:  &raftGroup{Name: "G", Peers: peers, Cluster: "C1", Desired: desired},
		}
	}

	// A legacy move only appended the destination peers, the first Replicas peers
	// are the peer set it started from.
	t.Run("LegacyMove", func(t *testing.T) {
		osa := newAssignment([]string{"S1", "S2", "S3", "S4", "S5", "S6"}, 3, nil)
		rg := osa.copyGroup().Group
		rg.Desired = &desiredRaftGroup{ID: "ID", Peers: rg.Peers}
		rg.populateOrigin(osa)

		require_NotNil(t, rg.Desired.Origin)
		require_Equal(t, len(rg.Desired.Origin.Peers), 3)
		require_True(t, slices.Equal(rg.Desired.Origin.Peers, []string{"S1", "S2", "S3"}))
		require_Equal(t, rg.Desired.Origin.Cluster, "C1")
		require_Equal(t, rg.Desired.Origin.Replicas, 3)
	})

	// Same, but now reached through a retention change registering the desired state.
	t.Run("LegacyMoveWithRetentionChange", func(t *testing.T) {
		osa := newAssignment([]string{"S1", "S2", "S3", "S4", "S5", "S6"}, 3, nil)
		rg := osa.copyGroup().Group.withRetentionChange(osa, InterestPolicy)

		require_NotNil(t, rg.Desired)
		require_NotNil(t, rg.Desired.Origin)
		require_True(t, slices.Equal(rg.Desired.Origin.Peers, []string{"S1", "S2", "S3"}))
		require_NotNil(t, rg.Desired.Origin.Retention)
		require_Equal(t, *rg.Desired.Origin.Retention, LimitsPolicy)
	})

	// Without a legacy move the current peer set is the origin.
	t.Run("NoLegacyMove", func(t *testing.T) {
		osa := newAssignment([]string{"S1", "S2", "S3"}, 3, nil)
		rg := osa.copyGroup().Group
		rg.Desired = &desiredRaftGroup{ID: "ID", Peers: rg.Peers}
		rg.populateOrigin(osa)

		require_NotNil(t, rg.Desired.Origin)
		require_True(t, slices.Equal(rg.Desired.Origin.Peers, []string{"S1", "S2", "S3"}))
	})

	// Desired state without an origin means we rolled back, capture what we rolled back to.
	t.Run("DesiredWithoutOrigin", func(t *testing.T) {
		desired := &desiredRaftGroup{ID: "ID", Peers: []string{"S4", "S5", "S6"}, Cluster: "C2"}
		osa := newAssignment([]string{"S1", "S2", "S3"}, 3, desired)
		rg := osa.copyGroup().Group
		rg.populateOrigin(osa)

		require_NotNil(t, rg.Desired.Origin)
		require_True(t, slices.Equal(rg.Desired.Origin.Peers, []string{"S4", "S5", "S6"}))
		require_Equal(t, rg.Desired.Origin.Cluster, "C2")
	})

	// An already recorded origin MUST NOT be overwritten, it's from before any changes.
	t.Run("OriginNotOverwritten", func(t *testing.T) {
		origin := &desiredRaftGroupOrigin{Peers: []string{"S7", "S8", "S9"}, Cluster: "C3", Replicas: 3}
		desired := &desiredRaftGroup{ID: "ID", Peers: []string{"S4", "S5", "S6"}, Origin: origin}
		osa := newAssignment([]string{"S1", "S2", "S3"}, 3, desired)
		rg := osa.copyGroup().Group
		rg.populateOrigin(osa)

		require_NotNil(t, rg.Desired.Origin)
		require_True(t, slices.Equal(rg.Desired.Origin.Peers, []string{"S7", "S8", "S9"}))
		require_Equal(t, rg.Desired.Origin.Cluster, "C3")
	})

	// And without desired state there's nothing to record onto.
	t.Run("NoDesiredState", func(t *testing.T) {
		osa := newAssignment([]string{"S1", "S2", "S3"}, 3, nil)
		rg := osa.copyGroup().Group
		rg.populateOrigin(osa)
		require_True(t, rg.Desired == nil)
	})
}

// While the desired state is pending the stream keeps running at its origin, but every
// endpoint that reports the config must report it as the user requested it. Both placement
// and retention are changed at once, so all of them are checked against both.
func TestJetStreamClusterDesiredOriginReportsTarget(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "R3S", 3)
	defer c.shutdown()

	ml := c.leader()
	nc, js := jsClientConnect(t, ml)
	defer nc.Close()

	// Start out with placement, so the origin has something to hold back.
	_, err := js.AddStream(&nats.StreamConfig{
		Name:      "TEST",
		Subjects:  []string{"foo"},
		Retention: nats.LimitsPolicy,
		Placement: &nats.Placement{Cluster: "R3S"},
		Replicas:  3,
	})
	require_NoError(t, err)

	// An R1 consumer, it must be scaled up before the stream can become Interest.
	_, err = js.AddConsumer("TEST", &nats.ConsumerConfig{
		Durable:   "C",
		AckPolicy: nats.AckExplicitPolicy,
		Replicas:  1,
	})
	require_NoError(t, err)

	mjs := ml.getJetStream()
	pending := func() bool {
		mjs.mu.RLock()
		defer mjs.mu.RUnlock()
		sa := mjs.streamAssignment(globalAccountName, "TEST")
		return sa != nil && sa.Group.Desired != nil
	}
	memberConfigs := func(t *testing.T) []StreamConfig {
		t.Helper()
		var cfgs []StreamConfig
		for _, s := range c.servers {
			mset, err := s.globalAccount().lookupStream("TEST")
			require_NoError(t, err)
			cfgs = append(cfgs, mset.config())
		}
		return cfgs
	}
	// Whether any member's running config is held back at its origin.
	atOrigin := func(t *testing.T) bool {
		t.Helper()
		var atOrigin bool
		for _, s := range c.servers {
			sjs := s.getJetStream()
			sjs.mu.RLock()
			sa := sjs.streamAssignment(globalAccountName, "TEST")
			atOrigin = atOrigin || (sa != nil && sa.Group.Desired != nil && sa.Group.Desired.Origin != nil)
			sjs.mu.RUnlock()
		}
		return atOrigin
	}
	// Reports the stream as jsz sees it, on every server.
	jszDetails := func(t *testing.T) []StreamDetail {
		t.Helper()
		var details []StreamDetail
		for _, s := range c.servers {
			jsz, err := s.Jsz(&JSzOptions{Accounts: true, Streams: true, Config: true})
			require_NoError(t, err)
			for _, ad := range jsz.AccountDetails {
				if ad.Name != globalAccountName {
					continue
				}
				for _, sd := range ad.Streams {
					if sd.Name == "TEST" {
						details = append(details, sd)
					}
				}
			}
		}
		require_Len(t, len(details), len(c.servers))
		return details
	}

	// Without any desired state the running config is never at its origin.
	require_False(t, atOrigin(t))

	// Block the consumer from finishing its scale-up by stopping the meta leader from
	// reconciling consumer assignments.
	mjs.mu.Lock()
	consumerReconcile := mjs.cluster.consumerReconcile
	mjs.cluster.consumerReconcile = nil
	mjs.mu.Unlock()
	require_NotNil(t, consumerReconcile)
	ml.sysUnsubscribe(consumerReconcile)

	// Remove the placement and change retention, the desired state holds back both. The
	// description is not held back, so it marks the update as having been applied.
	si, err := js.UpdateStream(&nats.StreamConfig{
		Name:        "TEST",
		Subjects:    []string{"foo"},
		Description: "applied",
		Retention:   nats.InterestPolicy,
		Replicas:    3,
	})
	require_NoError(t, err)

	// The update response must report the config as requested.
	require_Equal(t, si.Config.Retention, nats.InterestPolicy)
	require_True(t, si.Config.Placement == nil)

	// The assignment is replicated, so wait for all members to have applied the update
	// before asserting on it. Must not wait on the desired state itself, since that would
	// wait out the very window where the running config and the target differ.
	checkFor(t, 5*time.Second, 10*time.Millisecond, func() error {
		for _, cfg := range memberConfigs(t) {
			if cfg.Description != "applied" {
				return fmt.Errorf("update not applied on all members yet")
			}
		}
		return nil
	})

	// All members applied it, so they must know they're running at their origin and the
	// desired state must not have been reached yet.
	require_True(t, ml == c.leader())
	require_True(t, pending())
	require_True(t, atOrigin(t))

	// The members must still be running at their origin, so consumers keep their parity
	// guarantee until they've all been scaled up.
	for _, cfg := range memberConfigs(t) {
		require_Equal(t, cfg.Retention, LimitsPolicy)
		require_NotNil(t, cfg.Placement)
		require_Equal(t, cfg.Placement.Cluster, "R3S")
	}

	// But stream info, stream list and jsz must all report the config as requested.
	si, err = js.StreamInfo("TEST")
	require_NoError(t, err)
	require_Equal(t, si.Config.Retention, nats.InterestPolicy)
	require_True(t, si.Config.Placement == nil)

	var listed bool
	for si := range js.StreamsInfo() {
		if si.Config.Name == "TEST" {
			listed = true
			require_Equal(t, si.Config.Retention, nats.InterestPolicy)
			require_True(t, si.Config.Placement == nil)
		}
	}
	require_True(t, listed)

	for _, sd := range jszDetails(t) {
		require_NotNil(t, sd.Config)
		require_Equal(t, sd.Config.Retention, InterestPolicy)
		require_True(t, sd.Config.Placement == nil)
	}

	// Unblock the consumer scale-up so the stream can converge.
	require_True(t, ml == c.leader())
	mjs.mu.Lock()
	mjs.startUpdatesSub()
	mjs.mu.Unlock()

	// Wait for the desired state to be reached.
	checkFor(t, 10*time.Second, 100*time.Millisecond, func() error {
		if pending() {
			return fmt.Errorf("desired state still pending")
		}
		return nil
	})

	// Once converged the running config must have caught up with the target, and the
	// members must go back to the fast path.
	c.waitOnStreamLeader(globalAccountName, "TEST")
	checkFor(t, 5*time.Second, 100*time.Millisecond, func() error {
		for _, cfg := range memberConfigs(t) {
			if cfg.Retention != InterestPolicy || cfg.Placement != nil {
				return fmt.Errorf("running config not caught up: %v/%v", cfg.Retention, cfg.Placement)
			}
		}
		if atOrigin(t) {
			return fmt.Errorf("still marked as running at origin")
		}
		return nil
	})

	// And jsz must no longer report any desired state alongside it.
	for _, sd := range jszDetails(t) {
		require_NotNil(t, sd.Config)
		require_Equal(t, sd.Config.Retention, InterestPolicy)
		require_True(t, sd.Config.Placement == nil)
		if sd.Cluster != nil {
			require_True(t, sd.Cluster.Desired == nil)
		}
	}
}

// peerRemove takes the given servers out as an operator would, either out of the
// cluster entirely or just out of the stream.
func peerRemove(t *testing.T, c *cluster, nc *nats.Conn, stream string, serverScope bool, peers []string) {
	t.Helper()
	if !serverScope {
		for _, name := range peers {
			req, err := json.Marshal(&JSApiStreamRemovePeerRequest{Peer: name})
			require_NoError(t, err)
			rmsg, err := nc.Request(fmt.Sprintf(JSApiStreamRemovePeerT, stream), req, time.Second)
			require_NoError(t, err)
			var resp JSApiStreamRemovePeerResponse
			require_NoError(t, json.Unmarshal(rmsg.Data, &resp))
			require_True(t, resp.Error == nil)
		}
		return
	}

	// The server scoped endpoint is system account only.
	ml := c.leader()
	require_NotNil(t, ml)
	snc, err := nats.Connect(ml.ClientURL(), nats.UserInfo("admin", "s3cr3t!"))
	require_NoError(t, err)
	defer snc.Close()

	for _, name := range peers {
		req, err := json.Marshal(&JSApiMetaServerRemoveRequest{Server: name})
		require_NoError(t, err)
		rmsg, err := snc.Request(JSApiRemoveServer, req, time.Second)
		require_NoError(t, err)
		var resp JSApiMetaServerRemoveResponse
		require_NoError(t, json.Unmarshal(rmsg.Data, &resp))
		require_True(t, resp.Error == nil)
	}
}

// proposeSelectedScaleDown rewrites a consumer assignment as if a scale-down had
// already selected its final peer set before the group lost quorum: the desired
// peers hold only the kept peer, while the actual peers still hold the downed
// ones. A subsequent peer-remove of the downed servers is a no-op for the desired
// peers, so recovering from this shape requires recording the removals against
// the actual peers.
func proposeSelectedScaleDown(t *testing.T, c *cluster, stream, consumer string, replicas int, keep *Server) {
	t.Helper()
	const desiredID = "selected-scale-down"

	ml := c.leader()
	require_NotNil(t, ml)
	mjs := ml.getJetStream()
	mjs.mu.Lock()
	cc := mjs.cluster
	ca := mjs.consumerAssignmentOrInflight(globalAccountName, stream, consumer)
	if ca == nil || cc == nil || cc.meta == nil {
		mjs.mu.Unlock()
		t.Fatalf("no consumer assignment for %q", consumer)
	}
	nca := ca.copyGroup()
	if replicas > 0 {
		cfg := *nca.Config
		cfg.Replicas = replicas
		nca.Config = &cfg
	}
	nca.Group.Desired = &desiredRaftGroup{ID: desiredID, Peers: []string{keep.Node()}}
	err := cc.meta.Propose(cc.term, encodeAddConsumerAssignment(nca))
	mjs.mu.Unlock()
	require_NoError(t, err)

	// Wait for the assignment to be applied before returning.
	checkFor(t, 5*time.Second, 100*time.Millisecond, func() error {
		mjs.mu.RLock()
		defer mjs.mu.RUnlock()
		if ca := mjs.consumerAssignment(globalAccountName, stream, consumer); ca == nil ||
			ca.Group.Desired == nil || ca.Group.Desired.ID != desiredID {
			return fmt.Errorf("consumer %q assignment not applied yet", consumer)
		}
		return nil
	})
}

// A stream and consumer that lost quorum, because their peers were shut down,
// must recover once those peers are removed. They can't commit the peer removal
// through their own log, since that needs the quorum they lost. Removing the
// servers from the cluster takes them out of every group, removing them from the
// stream only takes them out of this one and leaves them cluster members.
func TestJetStreamClusterPeerRemoveRestoresQuorum(t *testing.T) {
	for _, test := range []struct {
		name        string
		serverScope bool
	}{
		{"Server", true},
		{"Stream", false},
	} {
		t.Run(test.name, func(t *testing.T) {
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

			_, err = js.AddConsumer("TEST", &nats.ConsumerConfig{
				Durable:   "dur",
				AckPolicy: nats.AckExplicitPolicy,
			})
			require_NoError(t, err)

			// A second consumer that will be rewritten below as a stuck scale-down.
			_, err = js.AddConsumer("TEST", &nats.ConsumerConfig{
				Durable:   "scaled",
				AckPolicy: nats.AckExplicitPolicy,
			})
			require_NoError(t, err)

			_, err = js.Publish("foo", nil)
			require_NoError(t, err)

			// Shut down both stream followers, taking the stream and its consumer
			// below quorum. The meta group has 3 of 5 servers left, so it stays
			// available.
			si, err := js.StreamInfo("TEST")
			require_NoError(t, err)
			require_Equal(t, len(si.Cluster.Replicas), 2)

			var downed []string
			for _, r := range si.Cluster.Replicas {
				downed = append(downed, r.Name)
			}
			sl := c.serverByName(si.Cluster.Leader)
			for _, name := range downed {
				c.serverByName(name).Shutdown()
			}
			c.waitOnLeader()

			// Reconnect to a server that's still up.
			nc.Close()
			nc, js = jsClientConnect(t, sl)
			defer nc.Close()

			// Both groups must be stuck at this point. The old leaders step down
			// once they notice they've lost quorum, and no new leader can be
			// elected. Can take up to lostQuorumInterval plus lostQuorumCheck for
			// them to notice.
			checkFor(t, 10*time.Second, 250*time.Millisecond, func() error {
				if l := c.streamLeader(globalAccountName, "TEST"); l != nil {
					return fmt.Errorf("stream still has leader %q", l.Name())
				}
				for _, consumer := range []string{"dur", "scaled"} {
					if l := c.consumerLeader(globalAccountName, "TEST", consumer); l != nil {
						return fmt.Errorf("consumer %q still has leader %q", consumer, l.Name())
					}
				}
				return nil
			})
			_, err = js.Publish("foo", nil)
			require_Error(t, err, nats.ErrNoStreamResponse)

			// The second consumer already selected its scale-down peer set, so the
			// peer-remove below can't touch its desired peers, only its actual ones.
			proposeSelectedScaleDown(t, c, "TEST", "scaled", 1, sl)

			// Now remove both downed servers. Only a server peer-remove changes
			// cluster membership, so that's the only one there is something to
			// wait for. A stream peer-remove leaves it alone, which the check at
			// the end of the test confirms.
			peerRemove(t, c, nc, "TEST", test.serverScope, downed)
			if test.serverScope {
				checkFor(t, 10*time.Second, 250*time.Millisecond, func() error {
					ml := c.leader()
					if ml == nil {
						return errors.New("no meta leader")
					}
					if n := len(ml.getJetStream().getMetaGroup().Peers()); n != 3 {
						return fmt.Errorf("expected 3 meta members, got %d", n)
					}
					return nil
				})
			}

			// Both the stream and the consumers must now be able to elect a leader
			// again, having evicted the removed peers, and become fully available.
			c.waitOnStreamLeader(globalAccountName, "TEST")
			c.waitOnConsumerLeader(globalAccountName, "TEST", "dur")
			c.waitOnConsumerLeader(globalAccountName, "TEST", "scaled")

			_, err = js.Publish("foo", nil)
			require_NoError(t, err)

			// And no group should reference the removed servers anymore.
			// Waiting on the leaders above also waits for the groups to have
			// converged, so this must hold right away.
			si, err = js.StreamInfo("TEST")
			require_NoError(t, err)
			require_NotNil(t, si.Cluster)
			require_Equal(t, len(si.Cluster.Replicas), 2)

			ci, err := js.ConsumerInfo("TEST", "dur")
			require_NoError(t, err)
			require_NotNil(t, ci.Cluster)
			require_Equal(t, len(ci.Cluster.Replicas), 2)

			// The stuck scale-down settles on its selected peer.
			sci, err := js.ConsumerInfo("TEST", "scaled")
			require_NoError(t, err)
			require_NotNil(t, sci.Cluster)
			require_Equal(t, sci.Cluster.Leader, sl.Name())
			require_Equal(t, len(sci.Cluster.Replicas), 0)

			for _, name := range downed {
				require_NotEqual(t, si.Cluster.Leader, name)
				require_NotEqual(t, ci.Cluster.Leader, name)
				for _, r := range si.Cluster.Replicas {
					require_NotEqual(t, r.Name, name)
				}
				for _, r := range ci.Cluster.Replicas {
					require_NotEqual(t, r.Name, name)
				}
			}

			// Only a server peer-remove takes them out of the cluster as well.
			expectedMetaPeers := 5
			if test.serverScope {
				expectedMetaPeers = 3
			}
			ml := c.leader()
			require_NotNil(t, ml)
			require_Equal(t, len(ml.getJetStream().getMetaGroup().Peers()), expectedMetaPeers)
		})
	}
}

// A stream peer remove that would leave the group below its replica count is rejected, even
// when the peers are offline and the group is below quorum. Scaling the stream down is the
// way out, after which the same removes are accepted and evict the peers, restoring the
// stream without needing a server peer remove.
func TestJetStreamClusterStreamPeerRemoveAfterScaleDownRestoresQuorum(t *testing.T) {
	// Tag placement so the stream can only ever live on S-1, S-2 and S-3. That leaves no
	// eligible replacement once peers go down, while the meta group keeps quorum on five.
	c := createJetStreamClusterWithTemplateAndModHook(t, jsClusterTempl, "C", 5,
		func(serverName, clusterName, storeDir, conf string) string {
			switch serverName {
			case "S-1", "S-2", "S-3":
				return fmt.Sprintf("%s\nserver_tags: [server:%s, grp:a]", conf, serverName)
			default:
				return fmt.Sprintf("%s\nserver_tags: [server:%s, grp:b]", conf, serverName)
			}
		})
	defer c.shutdown()

	// The meta leader must be outside the stream's peer set so it stays able to propose.
	ml := c.leader()
	require_NotNil(t, ml)
	for ml.Name() != "S-4" && ml.Name() != "S-5" {
		old := ml
		require_NoError(t, old.getJetStream().getMetaGroup().StepDown())
		checkFor(t, 30*time.Second, 10*time.Millisecond, func() error {
			ml = c.leader()
			if ml == nil || ml == old {
				return errors.New("waiting for leader change")
			}
			return nil
		})
	}

	nc, js := jsClientConnect(t, ml)
	defer nc.Close()

	cfg := &nats.StreamConfig{
		Name:      "TEST",
		Subjects:  []string{"foo"},
		Replicas:  3,
		Placement: &nats.Placement{Tags: []string{"grp:a"}},
	}
	_, err := js.AddStream(cfg)
	require_NoError(t, err)

	toSend := 5
	for range toSend {
		_, err = js.Publish("foo", nil)
		require_NoError(t, err)
	}

	// Take two of the three stream peers down for good, putting the stream below quorum.
	var offline []string
	for _, name := range []string{"S-2", "S-3"} {
		s := c.serverByName(name)
		offline = append(offline, s.Node())
		s.Shutdown()
	}
	checkFor(t, 15*time.Second, 250*time.Millisecond, func() error {
		for _, p := range offline {
			si, ok := ml.nodeToInfo.Load(p)
			if !ok || si == nil || !si.(nodeInfo).offline {
				return fmt.Errorf("peer %q not offline yet", p)
			}
		}
		return nil
	})

	// The stream must actually be stuck before we exercise the recovery. The old leader
	// steps down once it notices it has lost quorum, which can take up to
	// lostQuorumInterval plus lostQuorumCheck.
	checkFor(t, 30*time.Second, 250*time.Millisecond, func() error {
		if l := c.streamLeader(globalAccountName, "TEST"); l != nil {
			return fmt.Errorf("stream still has leader %q", l.Name())
		}
		return nil
	})

	removePeer := func(peer string) *JSApiStreamRemovePeerResponse {
		t.Helper()
		b, err := json.Marshal(JSApiStreamRemovePeerRequest{Peer: peer})
		require_NoError(t, err)
		msg, err := nc.Request(fmt.Sprintf(JSApiStreamRemovePeerT, "TEST"), b, 10*time.Second)
		require_NoError(t, err)
		var resp JSApiStreamRemovePeerResponse
		require_NoError(t, json.Unmarshal(msg.Data, &resp))
		return &resp
	}

	// At R3 there is nowhere to hand the peer to, so this must be rejected and the group
	// left exactly as it was.
	resp := removePeer(offline[0])
	require_False(t, resp.Success)
	require_Error(t, resp.Error, NewJSPeerRemapError())
	require_Len(t, len(metaStreamPeers(ml, globalAccountName, "TEST")), 3)

	// Scale the stream down. The request can time out waiting on a stream that has no
	// leader to respond, what matters is that the meta layer records it.
	cfg.Replicas = 1
	_, _ = js.UpdateStream(cfg, nats.MaxWait(time.Second))
	checkFor(t, 10*time.Second, 250*time.Millisecond, func() error {
		sjs := ml.getJetStream()
		sjs.mu.RLock()
		defer sjs.mu.RUnlock()
		sa := sjs.streamAssignment(globalAccountName, "TEST")
		if sa == nil {
			return errors.New("stream not found")
		}
		if sa.Config.Replicas != 1 {
			return fmt.Errorf("expected R1, got R%d", sa.Config.Replicas)
		}
		return nil
	})

	// Now the same removes are accepted, the group is no longer short of its replica count.
	for _, peer := range offline {
		require_True(t, removePeer(peer).Success)
	}

	// Both offline peers are evicted, leaving the stream on its one surviving peer.
	checkFor(t, 30*time.Second, 250*time.Millisecond, func() error {
		sjs := ml.getJetStream()
		sjs.mu.RLock()
		defer sjs.mu.RUnlock()
		sa := sjs.streamAssignment(globalAccountName, "TEST")
		if sa == nil || sa.Group == nil {
			return errors.New("stream not found")
		}
		if sa.Group.Desired != nil {
			return fmt.Errorf("stream still converging: peers=%v desired=%v scaleDown=%v",
				sa.Group.Peers, sa.Group.Desired.Peers, sa.Group.Desired.ScaleDown)
		}
		if len(sa.Group.Peers) != 1 {
			return fmt.Errorf("expected 1 peer, got %v", sa.Group.Peers)
		}
		for _, p := range offline {
			if slices.Contains(sa.Group.Peers, p) {
				return fmt.Errorf("offline peer %q still in peer set %v", p, sa.Group.Peers)
			}
		}
		return nil
	})

	// And the stream is usable again, with its data, without a server peer remove.
	c.waitOnStreamLeader(globalAccountName, "TEST")
	_, err = js.Publish("foo", nil)
	require_NoError(t, err)
	si, err := js.StreamInfo("TEST")
	require_NoError(t, err)
	require_Equal(t, si.State.Msgs, uint64(toSend+1))
	require_Len(t, len(si.Cluster.Replicas), 0)
}

// A scale-down or move that appended a removal of the surviving peer, but lost
// quorum before committing it, leaves that peer speculatively dropped from its
// own peer set while the assignment still lists it. Peer-removing the downed
// servers must still evict them, reverting the stuck self-removal, or the
// survivor could never campaign again.
func TestJetStreamClusterStreamPeerRemoveWithUncommittedSelfRemoval(t *testing.T) {
	// Tag placement so the stream can only ever live on S-1, S-2 and S-3, same as
	// TestJetStreamClusterStreamPeerRemoveAfterScaleDownRestoresQuorum.
	c := createJetStreamClusterWithTemplateAndModHook(t, jsClusterTempl, "C", 5,
		func(serverName, clusterName, storeDir, conf string) string {
			switch serverName {
			case "S-1", "S-2", "S-3":
				return fmt.Sprintf("%s\nserver_tags: [server:%s, grp:a]", conf, serverName)
			default:
				return fmt.Sprintf("%s\nserver_tags: [server:%s, grp:b]", conf, serverName)
			}
		})
	defer c.shutdown()

	// The meta leader must be outside the stream's peer set so it stays able to propose.
	ml := c.leader()
	require_NotNil(t, ml)
	for ml.Name() != "S-4" && ml.Name() != "S-5" {
		old := ml
		require_NoError(t, old.getJetStream().getMetaGroup().StepDown())
		checkFor(t, 30*time.Second, 10*time.Millisecond, func() error {
			ml = c.leader()
			if ml == nil || ml == old {
				return errors.New("waiting for leader change")
			}
			return nil
		})
	}

	nc, js := jsClientConnect(t, ml)
	defer nc.Close()

	cfg := &nats.StreamConfig{
		Name:      "TEST",
		Subjects:  []string{"foo"},
		Replicas:  3,
		Placement: &nats.Placement{Tags: []string{"grp:a"}},
	}
	_, err := js.AddStream(cfg)
	require_NoError(t, err)

	_, err = js.Publish("foo", nil)
	require_NoError(t, err)

	// Take the other two stream peers down for good, leaving S-1 as the survivor.
	survivor := c.serverByName("S-1")
	var offline []string
	for _, name := range []string{"S-2", "S-3"} {
		s := c.serverByName(name)
		offline = append(offline, s.Node())
		s.Shutdown()
	}

	// The stream must be stuck before we exercise the recovery.
	checkFor(t, 30*time.Second, 250*time.Millisecond, func() error {
		if l := c.streamLeader(globalAccountName, "TEST"); l != nil {
			return fmt.Errorf("stream still has leader %q", l.Name())
		}
		return nil
	})

	// Put the survivor in the state a scale-down or move would leave behind if it
	// appended a removal of this peer and lost quorum before committing it. This
	// is the same speculative apply processAppendEntry does for the entry.
	mset, err := survivor.GlobalAccount().lookupStream("TEST")
	require_NoError(t, err)
	node := mset.raftNode().(*raft)
	node.Lock()
	node.membChange = &membChange{index: node.pindex + 1, peer: node.id, prev: node.peers[node.id]}
	delete(node.peers, node.id)
	node.adjustClusterSizeAndQuorum()
	pendingSelfRemoval := node.pendingSelfRemoval()
	node.Unlock()
	require_True(t, pendingSelfRemoval)

	// Scale the stream down, so the peer removes below aren't rejected for leaving
	// the group short of its replica count. No leader can respond to the request.
	cfg.Replicas = 1
	_, _ = js.UpdateStream(cfg, nats.MaxWait(time.Second))
	checkFor(t, 10*time.Second, 250*time.Millisecond, func() error {
		sjs := ml.getJetStream()
		sjs.mu.RLock()
		defer sjs.mu.RUnlock()
		sa := sjs.streamAssignment(globalAccountName, "TEST")
		if sa == nil {
			return errors.New("stream not found")
		}
		if sa.Config.Replicas != 1 {
			return fmt.Errorf("expected R1, got R%d", sa.Config.Replicas)
		}
		return nil
	})

	// Remove both downed peers.
	for _, peer := range offline {
		b, err := json.Marshal(JSApiStreamRemovePeerRequest{Peer: peer})
		require_NoError(t, err)
		msg, err := nc.Request(fmt.Sprintf(JSApiStreamRemovePeerT, "TEST"), b, 10*time.Second)
		require_NoError(t, err)
		var resp JSApiStreamRemovePeerResponse
		require_NoError(t, json.Unmarshal(msg.Data, &resp))
		require_True(t, resp.Success)
	}

	// The survivor must evict the removed peers, revert its own stuck removal and
	// be able to elect a leader again, settling the group at R1.
	checkFor(t, 30*time.Second, 250*time.Millisecond, func() error {
		node.RLock()
		pendingSelfRemoval := node.pendingSelfRemoval()
		node.RUnlock()
		if pendingSelfRemoval {
			return errors.New("self removal still pending")
		}
		if !slices.Contains(node.PeerNames(), node.ID()) {
			return fmt.Errorf("not a member of our own group: %v", node.PeerNames())
		}
		sjs := ml.getJetStream()
		sjs.mu.RLock()
		defer sjs.mu.RUnlock()
		sa := sjs.streamAssignment(globalAccountName, "TEST")
		if sa == nil || sa.Group == nil {
			return errors.New("stream not found")
		}
		if len(sa.Group.Peers) != 1 || sa.Group.Peers[0] != survivor.Node() {
			return fmt.Errorf("expected the survivor as only peer, got %v", sa.Group.Peers)
		}
		return nil
	})
	c.waitOnStreamLeader(globalAccountName, "TEST")

	_, err = js.Publish("foo", nil)
	require_NoError(t, err)
	si, err := js.StreamInfo("TEST")
	require_NoError(t, err)
	require_Equal(t, si.State.Msgs, uint64(2))
	require_Equal(t, si.Cluster.Leader, survivor.Name())
	require_Len(t, len(si.Cluster.Replicas), 0)
}

// A stream that lost quorum can't scale down either, removing peers needs the
// quorum it just lost. Removing the downed servers must let it shrink around
// them and settle at its new replica count.
func TestJetStreamClusterPeerRemoveRestoresQuorumWhileScalingDown(t *testing.T) {
	for _, test := range []struct {
		name        string
		serverScope bool
	}{
		{"Server", true},
		{"Stream", false},
	} {
		t.Run(test.name, func(t *testing.T) {
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

			// A consumer that will be rewritten below as a stuck scale-down.
			_, err = js.AddConsumer("TEST", &nats.ConsumerConfig{
				Durable:   "scaled",
				AckPolicy: nats.AckExplicitPolicy,
			})
			require_NoError(t, err)

			_, err = js.Publish("foo", nil)
			require_NoError(t, err)

			// Shut down both stream followers, taking the stream below quorum. The
			// meta group has 3 of 5 servers left, so it stays available.
			si, err := js.StreamInfo("TEST")
			require_NoError(t, err)
			require_Equal(t, len(si.Cluster.Replicas), 2)

			var downed []string
			for _, r := range si.Cluster.Replicas {
				downed = append(downed, r.Name)
			}
			sl := c.serverByName(si.Cluster.Leader)
			for _, name := range downed {
				c.serverByName(name).Shutdown()
			}
			c.waitOnLeader()

			// Reconnect to a server that's still up.
			nc.Close()
			nc, js = jsClientConnect(t, sl)
			defer nc.Close()

			// Scale the stream down to R1. The meta layer takes the new replica
			// count, but the group can't shrink to it while it has no quorum. The
			// request races the old leader stepping down, so it may not get a
			// response at all. What matters is that the new config took.
			_, _ = js.UpdateStream(&nats.StreamConfig{
				Name:     "TEST",
				Subjects: []string{"foo"},
				Replicas: 1,
			})
			checkFor(t, 10*time.Second, 250*time.Millisecond, func() error {
				ml := c.leader()
				if ml == nil {
					return errors.New("no meta leader")
				}
				mjs := ml.getJetStream()
				mjs.mu.RLock()
				sa := mjs.streamAssignment(globalAccountName, "TEST")
				var replicas int
				if sa != nil && sa.Config != nil {
					replicas = sa.Config.Replicas
				}
				mjs.mu.RUnlock()
				if replicas != 1 {
					return fmt.Errorf("expected 1 replica in the assignment, got %d", replicas)
				}
				return nil
			})

			// It must be stuck, no leader can be elected.
			checkFor(t, 10*time.Second, 250*time.Millisecond, func() error {
				if l := c.streamLeader(globalAccountName, "TEST"); l != nil {
					return fmt.Errorf("stream still has leader %q", l.Name())
				}
				if l := c.consumerLeader(globalAccountName, "TEST", "scaled"); l != nil {
					return fmt.Errorf("consumer still has leader %q", l.Name())
				}
				// The old leader unsubscribes only after its stream monitor processes the step down.
				if sl.gacc.sl.HasInterest("foo") {
					return errors.New("stream still has interest")
				}
				return nil
			})
			_, err = js.Publish("foo", nil)
			require_Error(t, err, nats.ErrNoStreamResponse)

			// The consumer already selected its scale-down peer set, so the
			// peer-remove below can't touch its desired peers, only its actual ones.
			proposeSelectedScaleDown(t, c, "TEST", "scaled", 0, sl)

			// Now remove both downed servers. Only a server peer-remove changes
			// cluster membership, so that's the only one there is something to
			// wait for. A stream peer-remove leaves it alone, which the check at
			// the end of the test confirms.
			peerRemove(t, c, nc, "TEST", test.serverScope, downed)
			if test.serverScope {
				checkFor(t, 10*time.Second, 250*time.Millisecond, func() error {
					ml := c.leader()
					if ml == nil {
						return errors.New("no meta leader")
					}
					if n := len(ml.getJetStream().getMetaGroup().Peers()); n != 3 {
						return fmt.Errorf("expected 3 meta members, got %d", n)
					}
					return nil
				})
			}

			// The stream and consumer must be able to elect a leader again, having
			// evicted the removed peers, and settle at R1.
			c.waitOnStreamLeader(globalAccountName, "TEST")
			c.waitOnConsumerLeader(globalAccountName, "TEST", "scaled")

			_, err = js.Publish("foo", nil)
			require_NoError(t, err)

			si, err = js.StreamInfo("TEST")
			require_NoError(t, err)
			require_NotNil(t, si.Cluster)
			require_Equal(t, si.Config.Replicas, 1)
			require_Equal(t, len(si.Cluster.Replicas), 0)
			for _, name := range downed {
				require_NotEqual(t, si.Cluster.Leader, name)
			}

			// The stuck scale-down settles on its selected peer.
			ci, err := js.ConsumerInfo("TEST", "scaled")
			require_NoError(t, err)
			require_NotNil(t, ci.Cluster)
			require_Equal(t, ci.Cluster.Leader, sl.Name())
			require_Equal(t, len(ci.Cluster.Replicas), 0)

			// Only a server peer-remove takes them out of the cluster as well.
			expectedMetaPeers := 5
			if test.serverScope {
				expectedMetaPeers = 3
			}
			ml := c.leader()
			require_NotNil(t, ml)
			require_Equal(t, len(ml.getJetStream().getMetaGroup().Peers()), expectedMetaPeers)
		})
	}
}

// A new desired state must not lose which peers were peer-removed, the group can
// still be carrying them and need to shrink around them. Peers the new desired
// set takes back on are no longer removed.
func TestJetStreamClusterDesiredStateCarriesRemoved(t *testing.T) {
	const a, b, c, d, e = "A", "B", "C", "D", "E"

	newGroup := func() *raftGroup {
		return &raftGroup{
			Name:  "G",
			Peers: []string{a, b, c},
			Desired: &desiredRaftGroup{
				Peers:   []string{a, c, d},
				Removed: []string{b},
			},
		}
	}

	// A further removal must keep the earlier one on record.
	rg := newGroup()
	ng := rg.withDesired(&raftGroup{Name: "G", Peers: []string{a, d}})
	require_True(t, slices.Contains(ng.Desired.Removed, b))
	// And must not have mutated the original.
	require_True(t, slices.Contains(rg.Desired.Removed, b))

	// A peer the new desired set takes back on is no longer removed.
	rg = newGroup()
	ng = rg.withDesired(&raftGroup{Name: "G", Peers: []string{a, b, d}})
	require_False(t, slices.Contains(ng.Desired.Removed, b))

	// Recording is idempotent, and never records a peer that's still desired.
	// In the real flow the peers are stripped from the desired set first, so that
	// guard only catches a caller that hasn't.
	rg = newGroup()
	rg.Desired.addRemoved([]string{b, c, e})
	require_Equal(t, len(rg.Desired.Removed), 2)
	require_True(t, slices.Contains(rg.Desired.Removed, b))
	require_True(t, slices.Contains(rg.Desired.Removed, e))
	require_False(t, slices.Contains(rg.Desired.Removed, c))
}

func TestJetStreamClusterDesiredStatePreservedOnRetarget(t *testing.T) {
	const a, b, c, d = "A", "B", "C", "D"

	created := time.Now().UTC().Add(-time.Hour)
	origin := &desiredRaftGroupOrigin{Peers: []string{a, b, c}, Cluster: "C1", Replicas: 3}

	rg := &raftGroup{
		Name:  "G",
		Peers: []string{a, b, c},
		Desired: &desiredRaftGroup{
			ID:      "ID",
			Peers:   []string{a, c, d},
			Created: created,
			Term:    7,
			Move:    true,
			Origin:  origin,
		},
	}
	ng := rg.withDesired(&raftGroup{Name: "G", Peers: []string{a, c, d}})

	// Reconciliation has been ongoing since the first desired state, so the clock must not
	// restart, or the elapsed time reported for the migration resets on every retarget.
	require_True(t, ng.Desired.Created.Equal(created))
	// The leader already driving keeps its term, so it can act on the new desired state
	// without waiting for its term to be re-recorded. Dropping to zero would also lower the
	// fence that keeps a stale leader from acting.
	require_Equal(t, ng.Desired.Term, 7)
	// A move that is retargeted is still a move in flight.
	require_True(t, ng.Desired.Move)
	// The origin is what a cancel rolls back to, and must survive as a copy rather than be
	// shared with the group we replaced.
	require_NotNil(t, ng.Desired.Origin)
	require_Equal(t, ng.Desired.Origin.Cluster, "C1")
	require_Equal(t, ng.Desired.Origin.Replicas, 3)
	require_NotEqual(t, ng.Desired.Origin, rg.Desired.Origin)
	// It is new desired state though, so it gets its own identity and the group leader knows
	// to reassess against it.
	require_NotEqual(t, ng.Desired.ID, _EMPTY_)
	require_NotEqual(t, ng.Desired.ID, "ID")
	// And none of this may have mutated the group we replaced.
	require_True(t, rg.Desired.Created.Equal(created))
	require_Equal(t, rg.Desired.Term, 7)
	require_Equal(t, rg.Desired.ID, "ID")

	// A group entering desired state for the first time starts its own clock, with nobody
	// driving it yet and nothing to roll back to.
	fresh := &raftGroup{Name: "G", Peers: []string{a, b, c}}
	ng = fresh.withDesired(&raftGroup{Name: "G", Peers: []string{a, b, d}})
	require_False(t, ng.Desired.Created.IsZero())
	require_Equal(t, ng.Desired.Term, 0)
	require_False(t, ng.Desired.Move)
	require_True(t, ng.Desired.Origin == nil)
}

// A consumer already scaling down can hold a peer-removed peer only in its actual
// peer set, with its desired peers untouched by the removal. Remapping must still
// record that removal, or the group could never evict the peer and stay leaderless
// below quorum.
func TestJetStreamClusterRemapConsumerRecordsRemovedActualPeer(t *testing.T) {
	const a, b, c = "A", "B", "C"

	js := &jetStream{cluster: &jetStreamCluster{}}
	newAssignments := func(consumerPeers []string) (*streamAssignment, *consumerAssignment) {
		// Stream already had peer C removed by the operator.
		sa := &streamAssignment{
			Config: &StreamConfig{Name: "TEST", Replicas: 2, Retention: LimitsPolicy},
			Group: &raftGroup{
				Name:  "S",
				Peers: []string{a, b},
				Desired: &desiredRaftGroup{
					Peers:   []string{a, b},
					Removed: []string{c},
				},
			},
		}
		// Consumer is mid-scale-down with its final peers already selected, C is
		// gone from its desired peers but still in its actual peers, so the
		// peer-remove itself is a desired no-op.
		ca := &consumerAssignment{
			Name:   "CONSUMER",
			Stream: "TEST",
			Config: &ConsumerConfig{Durable: "CONSUMER", Replicas: 2},
			Group: &raftGroup{
				Name:  "C",
				Peers: consumerPeers,
				Desired: &desiredRaftGroup{
					Peers: []string{a, b},
				},
			},
		}
		sa.consumers = map[string]*consumerAssignment{ca.Name: ca}
		js.cluster.streams = map[string]map[string]*streamAssignment{globalAccountName: {sa.Config.Name: sa}}
		return sa, ca
	}

	// The removed peer must be dropped from the actual peers and recorded as removed.
	sa, _ := newAssignments([]string{a, b, c})
	consumers, deleted, done := js.remapConsumerAssignments(globalAccountName, sa)
	require_Len(t, len(deleted), 0)
	require_False(t, done)
	require_Len(t, len(consumers), 1)
	cca := consumers[0]
	require_False(t, slices.Contains(cca.Group.Peers, c))
	require_True(t, slices.Contains(cca.Group.Desired.Removed, c))
	require_True(t, slices.Contains(cca.Group.Peers, a))
	require_True(t, slices.Contains(cca.Group.Peers, b))
	require_True(t, slices.Equal(cca.Group.Desired.Peers, []string{a, b}))
	require_False(t, cca.Group.Desired.ScaleDown)

	// Recording is idempotent, once recorded there's nothing left to propose.
	sa.consumers[cca.Name] = cca
	consumers, deleted, _ = js.remapConsumerAssignments(globalAccountName, sa)
	require_Len(t, len(consumers), 0)
	require_Len(t, len(deleted), 0)

	// If all actual peers were removed, the consumer jumps to its desired set.
	sa, _ = newAssignments([]string{c})
	consumers, deleted, _ = js.remapConsumerAssignments(globalAccountName, sa)
	require_Len(t, len(deleted), 0)
	require_Len(t, len(consumers), 1)
	cca = consumers[0]
	require_False(t, slices.Contains(cca.Group.Peers, c))
	require_True(t, slices.Contains(cca.Group.Desired.Removed, c))
	require_True(t, slices.Contains(cca.Group.Peers, a))
	require_True(t, slices.Contains(cca.Group.Peers, b))
}

// A consumer following a stream move is moving, not scaling down, the stream
// dictates its peers. A pending scale down stays pending regardless.
func TestJetStreamClusterRemapConsumerFollowingStreamMoveIsNotScaleDown(t *testing.T) {
	const a, b, c, d, e = "A", "B", "C", "D", "E"

	js := &jetStream{cluster: &jetStreamCluster{}}
	// Stream and consumer are both R3 on A, B and C.
	newAssignments := func() (*streamAssignment, *consumerAssignment) {
		sa := &streamAssignment{
			Config: &StreamConfig{Name: "TEST", Replicas: 3, Retention: LimitsPolicy},
			Group:  &raftGroup{Name: "S", Peers: []string{a, b, c}},
		}
		ca := &consumerAssignment{
			Name:   "CONSUMER",
			Stream: "TEST",
			Config: &ConsumerConfig{Durable: "CONSUMER", Replicas: 3},
			Group:  &raftGroup{Name: "C", Peers: []string{a, b, c}},
		}
		sa.consumers = map[string]*consumerAssignment{ca.Name: ca}
		js.cluster.streams = map[string]map[string]*streamAssignment{globalAccountName: {sa.Config.Name: sa}}
		return sa, ca
	}

	// B and C went down. The consumer is scaled down to R1, but without a leader
	// nothing can select which peer remains, so the scale down stays pending.
	sa, ca := newAssignments()
	ca.Config.Replicas = 1
	ca.Group.Desired = &desiredRaftGroup{Peers: []string{a, b, c}, ScaleDown: true}

	// Then the stream is moved, peer-removing B and C hands it D and E instead.
	sa.Group.Peers = []string{a}
	sa.Group.Desired = &desiredRaftGroup{Peers: []string{a, d, e}, Removed: []string{b, c}}

	consumers, deleted, done := js.remapConsumerAssignments(globalAccountName, sa)
	require_Len(t, len(deleted), 0)
	require_False(t, done)
	require_Len(t, len(consumers), 1)
	cca := consumers[0]
	// The consumer follows the stream, but the scale down stays pending. It keeps A and
	// backfills the stream's replacements, the scale down must not shrink the peer set
	// here, its leader still selects which of those peers remains. The backfill order is random.
	require_True(t, cca.Group.Desired.ScaleDown)
	require_Len(t, len(cca.Group.Desired.Peers), 3)
	require_Equal(t, cca.Group.Desired.Peers[0], a)
	for _, p := range cca.Group.Desired.Peers {
		require_True(t, slices.Contains(sa.Group.Desired.Peers, p))
	}
	// The removed peers are dropped from the actual peers and recorded as removed.
	require_True(t, slices.Equal(cca.Group.Peers, []string{a}))
	require_True(t, slices.Equal(cca.Group.Desired.Removed, []string{b, c}))

	// A consumer that isn't scaling down simply follows a stream move. It keeps A and B,
	// loses C, and backfills D from the stream's desired peers. It kept some peers and
	// lost another, but since D is known upfront this is a move, not a scale down.
	sa, _ = newAssignments()
	sa.Group.Desired = &desiredRaftGroup{Peers: []string{a, b, d}}
	consumers, deleted, done = js.remapConsumerAssignments(globalAccountName, sa)
	require_Len(t, len(deleted), 0)
	require_False(t, done)
	require_Len(t, len(consumers), 1)
	cca = consumers[0]
	require_False(t, cca.Group.Desired.ScaleDown)
	require_True(t, slices.Equal(cca.Group.Desired.Peers, []string{a, b, d}))
	require_True(t, slices.Equal(cca.Group.Peers, []string{a, b, c}))
	require_Len(t, len(cca.Group.Desired.Removed), 0)

	// The same holds when the consumer was already moving and the stream is retargeted.
	// Its actual peers share nothing with the stream's new destination, but it's still a move.
	sa, ca = newAssignments()
	sa.Group.Desired = &desiredRaftGroup{Peers: []string{d, e, "F"}}
	ca.Group.Desired = &desiredRaftGroup{Peers: []string{d, e, "G"}}
	consumers, deleted, done = js.remapConsumerAssignments(globalAccountName, sa)
	require_Len(t, len(deleted), 0)
	require_False(t, done)
	require_Len(t, len(consumers), 1)
	cca = consumers[0]
	require_False(t, cca.Group.Desired.ScaleDown)
	require_True(t, slices.Equal(cca.Group.Desired.Peers, []string{d, e, "F"}))

	// Without stream desired state there is nothing dictating the consumer's peers.
	// Keeping some peers while dropping others is then a scale down, and the consumer's
	// leader selects which of the remaining peers it keeps.
	sa, ca = newAssignments()
	sa.Config.Replicas = 2
	sa.Group.Peers = []string{a, b}
	ca.Config.Replicas = 2
	consumers, deleted, done = js.remapConsumerAssignments(globalAccountName, sa)
	require_Len(t, len(deleted), 0)
	require_False(t, done)
	require_Len(t, len(consumers), 1)
	cca = consumers[0]
	require_True(t, slices.Equal(cca.Group.Desired.Peers, []string{a, b}))
	require_True(t, cca.Group.Desired.ScaleDown)
	require_True(t, slices.Equal(cca.Group.Peers, []string{a, b}))
	require_True(t, slices.Equal(cca.Group.Desired.Removed, []string{c}))
}

func TestJetStreamClusterStreamCreateRetryPreservesAssignmentCreated(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "R3S", 3)
	defer c.shutdown()

	nc, js := jsClientConnect(t, c.randomServer())
	defer nc.Close()

	cfg := &nats.StreamConfig{Name: "TEST", Subjects: []string{"foo"}, Replicas: 3}
	_, err := js.AddStream(cfg)
	require_NoError(t, err)

	getCreated := func(s *Server) (time.Time, error) {
		sjs := s.getJetStream()
		sjs.mu.RLock()
		defer sjs.mu.RUnlock()
		sa := sjs.streamAssignmentOrInflight(globalAccountName, "TEST")
		if sa == nil {
			return time.Time{}, fmt.Errorf("server %q has no stream assignment", s)
		}
		return sa.Created, nil
	}
	c.waitOnLeader()
	created, err := getCreated(c.leader())
	require_NoError(t, err)

	// An idempotent create retry re-proposes the stream assignment.
	_, err = js.AddStream(cfg)
	require_NoError(t, err)

	// The retry responds after its proposal applied, but replicas might lag.
	checkFor(t, 2*time.Second, 100*time.Millisecond, func() error {
		for _, s := range c.servers {
			sc, err := getCreated(s)
			if err != nil {
				return err
			}
			if !sc.Equal(created) {
				return fmt.Errorf("server %q changed Created from %v to %v", s, created, sc)
			}
		}
		return nil
	})
}

func TestJetStreamClusterMetaSnapshotIdempotentCreatePreservesStream(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "R3S", 3)
	defer c.shutdown()

	nc, js := jsClientConnect(t, c.randomServer())
	defer nc.Close()

	cfg := &nats.StreamConfig{Name: "TEST", Subjects: []string{"foo"}, Replicas: 1}
	_, err := js.AddStream(cfg)
	require_NoError(t, err)

	// Make sure the R1 stream is not hosted on the meta leader, so we can shut
	// down its host while keeping the meta leader around.
	sl := c.streamLeader(globalAccountName, "TEST")
	require_NotNil(t, sl)
	ml := c.leader()
	require_NotNil(t, ml)
	if sl == ml {
		meta := ml.getJetStream().getMetaGroup()
		require_NoError(t, meta.StepDown())
		c.waitOnLeader()
	}
	ml = c.leader()
	require_NotNil(t, ml)
	require_NotEqual(t, sl, ml)

	for range 10 {
		_, err = js.Publish("foo", nil)
		require_NoError(t, err)
	}

	// Capture the assignment's created time before the host goes down.
	mljs := ml.getJetStream()
	mljs.mu.RLock()
	sa := mljs.streamAssignmentOrInflight(globalAccountName, "TEST")
	if sa == nil {
		mljs.mu.RUnlock()
		t.Fatal("stream assignment not found")
	}
	created := sa.Created
	mljs.mu.RUnlock()

	sl.Shutdown()
	sl.WaitForShutdown()

	// Reconnect in case our client was on the downed server.
	nc.Close()
	nc, js = jsClientConnect(t, ml)
	defer nc.Close()

	// Perform an idempotent create while the host is down. The duplicate
	// assignment is proposed, but the only member is down and can't respond.
	_, err = js.AddStream(cfg, nats.MaxWait(time.Second))
	require_Error(t, err, nats.ErrTimeout, context.DeadlineExceeded)

	// Wait for the duplicate assignment to be applied, it must preserve the
	// original created time.
	var created2 time.Time
	checkFor(t, 2*time.Second, 100*time.Millisecond, func() error {
		mljs.mu.RLock()
		defer mljs.mu.RUnlock()
		if len(mljs.cluster.inflightStreams[globalAccountName]) > 0 {
			return fmt.Errorf("create proposal not applied yet")
		}
		nsa := mljs.streamAssignment(globalAccountName, "TEST")
		if nsa == nil {
			return fmt.Errorf("stream assignment not found")
		}
		if nsa == sa {
			return fmt.Errorf("duplicate create proposal not applied yet")
		}
		created2 = nsa.Created
		return nil
	})
	require_True(t, created2.Equal(created))

	// Compact the meta log so the downed server recovers via snapshot and
	// never replays the duplicate create entry.
	require_NoError(t, ml.JetStreamSnapshotMeta())

	// Restart. The server must not treat the idempotent create as a recreate,
	// it holds the only copy of the data.
	sl = c.restartServer(sl)
	c.checkClusterFormed()
	c.waitOnServerCurrent(sl)
	c.waitOnStreamLeader(globalAccountName, "TEST")

	checkFor(t, 5*time.Second, 100*time.Millisecond, func() error {
		state, err := checkStateAndErr(t, c, globalAccountName, "TEST")
		if err != nil {
			return err
		}
		if state.Msgs != 10 {
			return fmt.Errorf("stream data lost on idempotent create: %+v", state)
		}
		return nil
	})
}

func TestJetStreamClusterApplyMetaSnapshotRecreateDetection(t *testing.T) {
	const acc, stream, consumer = "A", "TEST", "C"
	created := time.Now().UTC()
	recreatedTS := created.Add(time.Second)

	group := func(name string) *raftGroup {
		if name == _EMPTY_ {
			return nil
		}
		return &raftGroup{Name: name, Storage: FileStorage}
	}
	scfg := &StreamConfig{Name: stream, Subjects: []string{"foo"}, Storage: FileStorage, Replicas: 3}
	scfgJSON, err := json.Marshal(scfg)
	require_NoError(t, err)
	ccfg := &ConsumerConfig{Durable: consumer}
	ccfgJSON, err := json.Marshal(ccfg)
	require_NoError(t, err)

	mkStream := func(created time.Time, groupName string) *streamAssignment {
		return &streamAssignment{Client: &ClientInfo{Account: acc}, Created: created, Group: group(groupName), Config: scfg, ConfigJSON: scfgJSON}
	}
	mkConsumer := func(created time.Time, groupName string) *consumerAssignment {
		return &consumerAssignment{Client: &ClientInfo{Account: acc}, Created: created, Group: group(groupName), Name: consumer, Stream: stream, Config: ccfg, ConfigJSON: ccfgJSON}
	}

	skey := mkStream(created, _EMPTY_).recoveryKey()
	ckey := mkConsumer(created, _EMPTY_).recoveryKey()

	applySnapshot := func(t *testing.T, osa, nsa *streamAssignment) *recoveryUpdates {
		t.Helper()
		js := &jetStream{srv: &Server{}, cluster: &jetStreamCluster{
			streams: map[string]map[string]*streamAssignment{acc: {stream: osa}},
		}}
		buf, _, _, err := js.encodeMetaSnapshot(map[string]map[string]*streamAssignment{acc: {stream: nsa}})
		require_NoError(t, err)
		ru := &recoveryUpdates{
			removeStreams:   make(map[string]*streamAssignment),
			removeConsumers: make(map[string]map[string]*consumerAssignment),
			addStreams:      make(map[string]*streamAssignment),
			updateStreams:   make(map[string]*streamAssignment),
			updateConsumers: make(map[string]map[string]*consumerAssignment),
		}
		require_NoError(t, js.applyMetaSnapshot(buf, ru, true))
		return ru
	}

	for _, test := range []struct {
		name      string
		nCreated  time.Time
		group     string
		nGroup    string
		recreated bool
	}{
		{"same created same group", created, "G-A", "G-A", false},
		{"same created different group", created, "G-A", "G-B", false},
		{"same created nil groups", created, _EMPTY_, _EMPTY_, false},
		{"different created same group", recreatedTS, "G-A", "G-A", false},
		{"different created different group", recreatedTS, "G-A", "G-B", true},
		{"different created nil old group", recreatedTS, _EMPTY_, "G-A", true},
		{"different created nil new group", recreatedTS, "G-A", _EMPTY_, true},
		{"different created nil groups", recreatedTS, _EMPTY_, _EMPTY_, true},
	} {
		t.Run("stream/"+test.name, func(t *testing.T) {
			osa := mkStream(created, test.group)
			nsa := mkStream(test.nCreated, test.nGroup)
			ru := applySnapshot(t, osa, nsa)

			_, removed := ru.removeStreams[skey]
			_, added := ru.addStreams[skey]
			_, updated := ru.updateStreams[skey]
			require_Equal(t, removed, test.recreated)
			require_Equal(t, added, test.recreated)
			require_Equal(t, updated, !test.recreated)
		})

		t.Run("consumer/"+test.name, func(t *testing.T) {
			// Keep the stream itself unchanged so only the consumer varies.
			osa := mkStream(created, "G-S")
			osa.consumers = map[string]*consumerAssignment{consumer: mkConsumer(created, test.group)}
			nsa := mkStream(created, "G-S")
			nsa.consumers = map[string]*consumerAssignment{consumer: mkConsumer(test.nCreated, test.nGroup)}
			ru := applySnapshot(t, osa, nsa)

			_, updated := ru.updateStreams[skey]
			require_True(t, updated)
			_, removed := ru.removeConsumers[skey][ckey]
			require_Equal(t, removed, test.recreated)
			// The consumer from the snapshot is always (re)applied.
			_, added := ru.updateConsumers[skey][ckey]
			require_True(t, added)
		})
	}
}

// https://github.com/nats-io/nats-server/issues/8423
func TestJetStreamClusterWorkQueueSourceMustNotAckOtherSubjects(t *testing.T) {
	for _, replicas := range []int{1, 3} {
		t.Run(fmt.Sprintf("R%d", replicas), func(t *testing.T) {
			c := createJetStreamClusterExplicit(t, "R3S", 3)
			defer c.shutdown()

			nc, js := jsClientConnect(t, c.randomServer())
			defer nc.Close()

			_, err := jsStreamCreate(t, nc, &StreamConfig{
				Name:      "WQ",
				Subjects:  []string{"x.>"},
				Storage:   FileStorage,
				Retention: WorkQueuePolicy,
				Replicas:  replicas,
			})
			require_NoError(t, err)

			// Wrap two matching sourced messages around a gap.
			_, err = js.Publish("x.target", nil)
			require_NoError(t, err)
			for range 3 {
				_, err = js.Publish("x.other", nil)
				require_NoError(t, err)
			}
			_, err = js.Publish("x.target", nil)
			require_NoError(t, err)

			_, err = jsStreamCreate(t, nc, &StreamConfig{
				Name:     "TARGET",
				Storage:  FileStorage,
				Replicas: replicas,
				Sources:  []*StreamSource{{Name: "WQ", FilterSubject: "x.target"}},
			})
			require_NoError(t, err)

			// Wait for both messages to be sourced.
			checkFor(t, 5*time.Second, 100*time.Millisecond, func() error {
				si, err := js.StreamInfo("TARGET")
				if err != nil {
					return err
				}
				if si.State.Msgs != 2 {
					return fmt.Errorf("expected 2 messages in TARGET, got %d", si.State.Msgs)
				}
				return nil
			})

			// The ack of sequence 5 must not have removed sequences 2-4 on "x.other".
			checkFor(t, 2*time.Second, 200*time.Millisecond, func() error {
				si, err := js.StreamInfo("WQ")
				if err != nil {
					return err
				}
				if si.State.Msgs != 3 {
					return fmt.Errorf("expected 3 messages in WQ, got %d (first=%d, last=%d)",
						si.State.Msgs, si.State.FirstSeq, si.State.LastSeq)
				}
				return nil
			})
		})
	}
}

// https://github.com/nats-io/nats-server/issues/8423
func TestJetStreamClusterScheduledMessagesWithWorkQueueSource(t *testing.T) {
	for _, replicas := range []int{1, 3} {
		t.Run(fmt.Sprintf("R%d", replicas), func(t *testing.T) {
			c := createJetStreamClusterExplicit(t, "R3S", 3)
			defer c.shutdown()

			nc, js := jsClientConnect(t, c.randomServer())
			defer nc.Close()

			_, err := jsStreamCreate(t, nc, &StreamConfig{
				Name:              "SCHEDULES",
				Subjects:          []string{"x.>"},
				Storage:           FileStorage,
				Retention:         WorkQueuePolicy,
				Replicas:          replicas,
				AllowMsgSchedules: true,
			})
			require_NoError(t, err)

			publishSchedule := func(subject string, due time.Time) {
				t.Helper()
				m := nats.NewMsg(subject)
				m.Header.Set(JSSchedulePattern, "@at "+due.UTC().Format(time.RFC3339))
				m.Header.Set(JSScheduleTarget, "x.target")
				_, err = js.PublishMsg(m)
				require_NoError(t, err)
			}
			// A fired schedule writes to "x.target" and purges its own message, so we
			// can wait on the resulting stream sequence.
			waitForFire := func(lseq uint64) {
				t.Helper()
				checkFor(t, 5*time.Second, 50*time.Millisecond, func() error {
					si, err := js.StreamInfo("SCHEDULES")
					if err != nil {
						return err
					}
					if si.State.LastSeq != lseq {
						return fmt.Errorf("expected last sequence %d, got %d", lseq, si.State.LastSeq)
					}
					return nil
				})
			}

			soon := time.Now().Add(time.Second)
			never := time.Now().Add(time.Hour)

			// Fire a schedule first, so a message for "x.target" ends up below the
			// schedules published next. Without a match at either end the sourcing
			// consumer has one pending sequence, and the ack range collapses down to
			// it, hiding the bug.
			publishSchedule("x.fire-1", soon) // Sequence 1, purged once fired.
			waitForFire(2)                    // Sequence 2, for "x.target".

			// These are due long after the test, they must be kept.
			publishSchedule("x.keep-1", never) // Sequence 3.
			publishSchedule("x.keep-2", never) // Sequence 4.

			publishSchedule("x.fire-2", soon) // Sequence 5, purged once fired.
			waitForFire(6)                    // Sequence 6, for "x.target".

			// Only source once both messages for "x.target" are there, so a single
			// AckAll spans the schedules in between.
			_, err = jsStreamCreate(t, nc, &StreamConfig{
				Name:     "TARGET",
				Storage:  FileStorage,
				Replicas: replicas,
				Sources:  []*StreamSource{{Name: "SCHEDULES", FilterSubject: "x.target"}},
			})
			require_NoError(t, err)

			// Both fired schedules must end up in TARGET.
			checkFor(t, 5*time.Second, 100*time.Millisecond, func() error {
				si, err := js.StreamInfo("TARGET")
				if err != nil {
					return err
				}
				if si.State.Msgs != 2 {
					return fmt.Errorf("expected 2 messages in TARGET, got %d", si.State.Msgs)
				}
				return nil
			})

			// The ack of sequence 6 must not have removed the schedules at 3 and 4.
			checkFor(t, 2*time.Second, 200*time.Millisecond, func() error {
				si, err := js.StreamInfo("SCHEDULES")
				if err != nil {
					return err
				}
				if si.State.Msgs != 2 {
					return fmt.Errorf("expected 2 schedules in SCHEDULES, got %d (first=%d, last=%d)",
						si.State.Msgs, si.State.FirstSeq, si.State.LastSeq)
				}
				return nil
			})
		})
	}
}

func TestJetStreamClusterAckAllAfterConsumerFilterUpdate(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "R3S", 3)
	defer c.shutdown()

	nc, js := jsClientConnect(t, c.randomServer())
	defer nc.Close()

	_, err := jsStreamCreate(t, nc, &StreamConfig{
		Name:      "WQ",
		Subjects:  []string{"x.>"},
		Storage:   FileStorage,
		Retention: WorkQueuePolicy,
		Replicas:  3,
	})
	require_NoError(t, err)

	_, err = js.Publish("x.a", nil)
	require_NoError(t, err)
	_, err = js.Publish("x.b", nil)
	require_NoError(t, err)

	inbox := nats.NewInbox()
	sub, err := nc.SubscribeSync(inbox)
	require_NoError(t, err)
	defer sub.Unsubscribe()

	// A flow-controlled consumer acks like an AckAll, and is allowed on a WQ stream.
	cfg := ConsumerConfig{
		Durable:        "C",
		DeliverSubject: inbox,
		FilterSubjects: []string{"x.a", "x.b"},
		AckPolicy:      AckFlowControl,
		FlowControl:    true,
		Heartbeat:      time.Second,
	}
	_, err = jsConsumerCreate(t, nc, "WQ", cfg, false)
	require_NoError(t, err)

	// Both messages must be delivered, so both are pending. Also need the flow
	// control subject, replying to that is what acks for this consumer.
	var delivered int
	var fcSubject string
	for delivered < 2 || fcSubject == _EMPTY_ {
		m, err := sub.NextMsg(2 * time.Second)
		require_NoError(t, err)
		if len(m.Header) == 0 {
			delivered++
		} else if m.Reply != _EMPTY_ {
			fcSubject = m.Reply
		} else if stalled := m.Header.Get(JSConsumerStalled); stalled != _EMPTY_ {
			fcSubject = stalled
		}
	}

	// Narrow the filters, "x.a" is pending but no longer matches.
	cfg.FilterSubjects = []string{"x.b"}
	_, err = jsConsumerCreate(t, nc, "WQ", cfg, false)
	require_NoError(t, err)

	// Reply to flow control, acking everything delivered up to sequence 2.
	m := nats.NewMsg(fcSubject)
	m.Header.Set(JSLastConsumerSeq, "2")
	m.Header.Set(JSLastStreamSeq, "2")
	require_NoError(t, nc.PublishMsg(m))

	// Both messages were acked by the consumer, so both must be removed.
	checkFor(t, 5*time.Second, 100*time.Millisecond, func() error {
		si, err := js.StreamInfo("WQ")
		if err != nil {
			return err
		}
		if si.State.Msgs != 0 {
			return fmt.Errorf("expected 0 messages in WQ, got %d (first=%d, last=%d)",
				si.State.Msgs, si.State.FirstSeq, si.State.LastSeq)
		}
		return nil
	})
}

func TestJetStreamClusterMetaRescueRejectedWithLeader(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "R3S", 3)
	defer c.shutdown()

	s := c.randomServer()
	nc, err := nats.Connect(s.ClientURL(), nats.UserInfo("admin", "s3cr3t!"))
	require_NoError(t, err)
	defer nc.Close()

	inbox := nats.NewInbox()
	sub, err := nc.SubscribeSync(inbox)
	require_NoError(t, err)

	req, err := json.Marshal(&JSApiMetaRescueRequest{QuorumNeeded: 1})
	require_NoError(t, err)
	require_NoError(t, nc.PublishRequest(JSApiMetaRescue, inbox, req))

	// This is a broadcast subject, every online server evaluates and responds
	// to the request independently. All of them know of a healthy meta leader
	// so all must reject the rescue.
	for range 3 {
		msg, err := sub.NextMsg(time.Second)
		require_NoError(t, err)
		var resp JSApiMetaRescueResponse
		require_NoError(t, json.Unmarshal(msg.Data, &resp))
		require_True(t, resp.Error != nil)
		require_Equal(t, resp.Error.ErrCode, uint16(JSClusterRescueErr))
		require_True(t, strings.Contains(resp.Error.Description, errRescueLeaderKnown.Error()))
		require_Equal(t, resp.NewQuorum, 0)
		require_NotEqual(t, resp.Server, _EMPTY_)
		require_NotEqual(t, resp.ServerID, _EMPTY_)
	}
	// And no more responses than that.
	_, err = sub.NextMsg(250 * time.Millisecond)
	require_Error(t, err, nats.ErrTimeout)
}

func TestJetStreamClusterMetaRescue(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "R5S", 5)
	defer c.shutdown()

	// Keep two servers that are not the meta leader, and shut down the other
	// three without peer-removing them. The meta group still believes its size
	// is 5 and needs 3 votes for quorum, so the meta layer is now stuck.
	leader := c.leader()
	var survivors []*Server
	var downed []string
	for _, s := range c.servers {
		if s != leader && len(survivors) < 2 {
			survivors = append(survivors, s)
			continue
		}
		downed = append(downed, s.Name())
		s.Shutdown()
		s.WaitForShutdown()
	}
	require_Len(t, len(survivors), 2)
	require_Len(t, len(downed), 3)

	// Wait until the survivors see no meta leader.
	checkFor(t, 10*time.Second, 200*time.Millisecond, func() error {
		for _, s := range survivors {
			if leader := s.getJetStream().getMetaGroup().GroupLeader(); leader != _EMPTY_ {
				return fmt.Errorf("server %q still sees meta leader %q", s.Name(), leader)
			}
		}
		return nil
	})

	nc, err := nats.Connect(survivors[0].ClientURL(), nats.UserInfo("admin", "s3cr3t!"))
	require_NoError(t, err)
	defer nc.Close()

	// Listen for the rescue advisories.
	advSub, err := nc.SubscribeSync(JSAdvisoryMetaRescue)
	require_NoError(t, err)
	// Make sure interest has propagated to the other survivor.
	checkSubInterest(t, survivors[1], "$SYS", JSAdvisoryMetaRescue, time.Second)

	inbox := nats.NewInbox()
	sub, err := nc.SubscribeSync(inbox)
	require_NoError(t, err)
	checkSubInterest(t, survivors[1], "$SYS", inbox, time.Second)

	req, err := json.Marshal(&JSApiMetaRescueRequest{QuorumNeeded: 2})
	require_NoError(t, err)
	require_NoError(t, nc.PublishRequest(JSApiMetaRescue, inbox, req))

	// Both survivors respond independently. At least one must have applied the
	// rescue, the other may already know of a new leader by the time it
	// evaluates the request and reject it.
	var applied int
	for range 2 {
		msg, err := sub.NextMsg(time.Second)
		require_NoError(t, err)
		var resp JSApiMetaRescueResponse
		require_NoError(t, json.Unmarshal(msg.Data, &resp))
		if resp.Error != nil {
			require_Equal(t, resp.Error.ErrCode, uint16(JSClusterRescueErr))
			require_True(t, strings.Contains(resp.Error.Description, errRescueLeaderKnown.Error()))
			continue
		}
		// No error means the rescue was applied and the new quorum is set.
		require_Equal(t, resp.PrevQuorum, 3)
		require_Equal(t, resp.NewQuorum, 2)
		applied++
	}
	require_True(t, applied >= 1)

	// Every server that applied the rescue also published an advisory.
	for range applied {
		msg, err := advSub.NextMsg(time.Second)
		require_NoError(t, err)
		var adv JSMetaRescueAdvisory
		require_NoError(t, json.Unmarshal(msg.Data, &adv))
		require_Equal(t, adv.Type, JSMetaRescueAdvisoryType)
		require_Equal(t, adv.PrevQuorum, 3)
		require_Equal(t, adv.NewQuorum, 2)
		require_Equal(t, adv.Cluster, c.name)
	}

	// With the lowered quorum the survivors can now elect a meta leader.
	var metaLeader string
	checkFor(t, 5*time.Second, 250*time.Millisecond, func() error {
		for _, s := range survivors {
			if leader := s.getJetStream().getMetaGroup().GroupLeader(); leader != _EMPTY_ {
				metaLeader = leader
				return nil
			}
		}
		return fmt.Errorf("expected a meta leader among the survivors")
	})
	require_True(t, metaLeader == getHash(survivors[0].Name()) || metaLeader == getHash(survivors[1].Name()))

	// While the rescue is active, JSZ exposes the lowered quorum and the
	// rescue state on the servers that applied it.
	var inRescue int
	for _, s := range survivors {
		jsz, err := s.Jsz(nil)
		require_NoError(t, err)
		require_True(t, jsz.Meta != nil)
		if jsz.Meta.Rescue {
			require_Equal(t, jsz.Meta.QuorumNeeded, 2)
			inRescue++
		}
	}
	require_True(t, inRescue >= applied)

	// The meta layer is unblocked, use the normal peer-remove path to
	// permanently drop the lost peers.
	for _, name := range downed {
		rmReq, err := json.Marshal(&JSApiMetaServerRemoveRequest{Server: name})
		require_NoError(t, err)
		rmsg, err := nc.Request(JSApiRemoveServer, rmReq, 2*time.Second)
		require_NoError(t, err)
		var resp JSApiMetaServerRemoveResponse
		require_NoError(t, json.Unmarshal(rmsg.Data, &resp))
		require_True(t, resp.Error == nil)
		require_True(t, resp.Success)
	}

	// The peer set should now only contain the survivors, and with the natural
	// quorum of the shrunken peer set the meta group operates normally.
	checkFor(t, 5*time.Second, 250*time.Millisecond, func() error {
		for _, s := range survivors {
			if cs := s.getJetStream().getMetaGroup().ClusterSize(); cs != 2 {
				return fmt.Errorf("expected cluster size 2 on %q, got %d", s.Name(), cs)
			}
			if qn := s.getJetStream().getMetaGroup().QuorumNeeded(); qn != 2 {
				return fmt.Errorf("expected quorum 2 on %q, got %d", s.Name(), qn)
			}
			// The natural quorum of the shrunken peer set reached the rescued
			// value, which stops the rescue.
			if s.getJetStream().getMetaGroup().InRescue() {
				return fmt.Errorf("expected rescue to be stopped on %q", s.Name())
			}
		}
		return nil
	})
}

func TestJetStreamClusterMetaRescueBadRequest(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "R3S", 3)
	defer c.shutdown()

	s := c.randomServer()
	nc, err := nats.Connect(s.ClientURL(), nats.UserInfo("admin", "s3cr3t!"))
	require_NoError(t, err)
	defer nc.Close()

	inbox := nats.NewInbox()
	sub, err := nc.SubscribeSync(inbox)
	require_NoError(t, err)

	// Empty request.
	require_NoError(t, nc.PublishRequest(JSApiMetaRescue, inbox, nil))
	for range 3 {
		msg, err := sub.NextMsg(time.Second)
		require_NoError(t, err)
		var resp JSApiMetaRescueResponse
		require_NoError(t, json.Unmarshal(msg.Data, &resp))
		require_True(t, resp.Error != nil)
		require_Equal(t, resp.Error.ErrCode, uint16(JSBadRequestErr))
	}

	// Quorum larger than the current effective quorum, it can only be
	// lowered. This is checked before the leader-known check would reject it.
	req, err := json.Marshal(&JSApiMetaRescueRequest{QuorumNeeded: 4})
	require_NoError(t, err)
	require_NoError(t, nc.PublishRequest(JSApiMetaRescue, inbox, req))
	for range 3 {
		msg, err := sub.NextMsg(time.Second)
		require_NoError(t, err)
		var resp JSApiMetaRescueResponse
		require_NoError(t, json.Unmarshal(msg.Data, &resp))
		require_True(t, resp.Error != nil)
		require_Equal(t, resp.Error.ErrCode, uint16(JSClusterRescueErr))
		require_True(t, strings.Contains(resp.Error.Description, errRescueBadQuorum.Error()))
	}
}

func TestJetStreamClusterMetaRescueSingleSurvivor(t *testing.T) {
	lqi := lostQuorumInterval
	lostQuorumInterval = 2 * time.Second
	defer func() { lostQuorumInterval = lqi }()

	c := createJetStreamClusterExplicit(t, "R3S", 3)
	defer c.shutdown()

	// Keep the first server, it seeds the routes for the servers added in
	// later. Shut down the other two without peer-removing them, the meta
	// group still believes its size is 3 and needs 2 votes for quorum.
	survivor := c.servers[0]
	var downed []string
	for _, s := range c.servers[1:] {
		downed = append(downed, s.Name())
		s.Shutdown()
		s.WaitForShutdown()
	}
	// Only track the survivor, so the servers added in later join through it.
	c.servers = c.servers[:1]
	c.opts = c.opts[:1]

	// Wait until the survivor sees no meta leader. If the survivor was the
	// meta leader itself, it steps down once it detects the lost quorum.
	checkFor(t, 5*time.Second, 250*time.Millisecond, func() error {
		if leader := survivor.getJetStream().getMetaGroup().GroupLeader(); leader != _EMPTY_ {
			return fmt.Errorf("survivor still sees meta leader %q", leader)
		}
		return nil
	})

	nc, err := nats.Connect(survivor.ClientURL(), nats.UserInfo("admin", "s3cr3t!"))
	require_NoError(t, err)
	defer nc.Close()

	inbox := nats.NewInbox()
	sub, err := nc.SubscribeSync(inbox)
	require_NoError(t, err)

	req, err := json.Marshal(&JSApiMetaRescueRequest{QuorumNeeded: 1})
	require_NoError(t, err)
	require_NoError(t, nc.PublishRequest(JSApiMetaRescue, inbox, req))

	// Only the survivor is online to respond, and it applies the rescue.
	msg, err := sub.NextMsg(time.Second)
	require_NoError(t, err)
	var resp JSApiMetaRescueResponse
	require_NoError(t, json.Unmarshal(msg.Data, &resp))
	require_True(t, resp.Error == nil)
	require_Equal(t, resp.PrevQuorum, 2)
	require_Equal(t, resp.NewQuorum, 1)
	_, err = sub.NextMsg(250 * time.Millisecond)
	require_Error(t, err, nats.ErrTimeout)

	// With a quorum of 1 the survivor can elect itself.
	checkFor(t, 5*time.Second, 250*time.Millisecond, func() error {
		if !survivor.JetStreamIsLeader() {
			return fmt.Errorf("survivor is not meta leader yet")
		}
		return nil
	})

	// The meta layer is unblocked, use the normal peer-remove path to
	// permanently drop the lost peers.
	for _, name := range downed {
		rmReq, err := json.Marshal(&JSApiMetaServerRemoveRequest{Server: name})
		require_NoError(t, err)
		rmsg, err := nc.Request(JSApiRemoveServer, rmReq, 2*time.Second)
		require_NoError(t, err)
		var resp JSApiMetaServerRemoveResponse
		require_NoError(t, json.Unmarshal(rmsg.Data, &resp))
		require_True(t, resp.Error == nil)
		require_True(t, resp.Success)
	}

	// The peer set shrunk to just the survivor. The natural quorum reached
	// the rescued value, which stops the rescue.
	checkFor(t, 2*time.Second, 250*time.Millisecond, func() error {
		meta := survivor.getJetStream().getMetaGroup()
		if cs := meta.ClusterSize(); cs != 1 {
			return fmt.Errorf("expected cluster size 1, got %d", cs)
		}
		if qn := meta.QuorumNeeded(); qn != 1 {
			return fmt.Errorf("expected quorum 1, got %d", qn)
		}
		if meta.InRescue() {
			return fmt.Errorf("expected rescue to be stopped")
		}
		return nil
	})

	// Add two new servers to go back to R3, under names not seen before.
	// Reusing the names of the peer-removed servers would not work, they
	// map to the same peer IDs which are marked as removed and can not
	// immediately be re-added to the peer set.
	for _, sn := range []string{"S-NEW-1", "S-NEW-2"} {
		seedRoute := fmt.Sprintf("nats-route://127.0.0.1:%d", c.opts[0].Cluster.Port)
		conf := fmt.Sprintf(jsClusterTempl, sn, t.TempDir(), c.name, -1, seedRoute)
		s, o := RunServerWithConfig(createConfFile(t, []byte(conf)))
		c.servers = append(c.servers, s)
		c.opts = append(c.opts, o)
	}
	c.checkClusterFormed()
	c.waitOnPeerCount(3)

	// Quorum is back at its natural value for the new R3 peer set.
	checkFor(t, 2*time.Second, 250*time.Millisecond, func() error {
		meta := survivor.getJetStream().getMetaGroup()
		if qn := meta.QuorumNeeded(); qn != 2 {
			return fmt.Errorf("expected quorum 2, got %d", qn)
		}
		return nil
	})

	// And the meta layer is fully operational again.
	ncjs, js := jsClientConnect(t, survivor)
	defer ncjs.Close()
	_, err = js.AddStream(&nats.StreamConfig{Name: "TEST", Replicas: 3})
	require_NoError(t, err)
}

// A two-node meta cluster that permanently loses one node needs the rescue API
// to regain a meta leader, after which a peer-remove reconciles the assets down
// onto the survivor. The stream must stay available throughout as a single peer,
// and once the lost server returns both the meta group and the stream must take
// it back on their own.
func TestJetStreamClusterMetaRescueTwoNodeRecovery(t *testing.T) {
	lqi := lostQuorumInterval
	lostQuorumInterval = 2 * time.Second
	defer func() { lostQuorumInterval = lqi }()
	// So the returning server can rejoin the meta peer set without waiting
	// out the full removal timeout.
	prt := peerRemoveTimeout
	peerRemoveTimeout = 2 * time.Second
	defer func() { peerRemoveTimeout = prt }()

	c := createJetStreamClusterExplicit(t, "R2S", 2)
	defer c.shutdown()

	nc, js := jsClientConnect(t, c.randomServer())
	defer nc.Close()

	_, err := js.AddStream(&nats.StreamConfig{Name: "TEST", Subjects: []string{"foo"}, Replicas: 2})
	require_NoError(t, err)
	for range 5 {
		_, err = js.Publish("foo", []byte("hello"))
		require_NoError(t, err)
	}
	nc.Close()

	// Lose one of the two nodes, which takes quorum with it. Keep the first
	// server, it seeds the routes the restarted server rejoins through.
	survivor, dead := c.servers[0], c.servers[1]
	deadName := dead.Name()

	// Remember the R2 group, it gets renamed once the stream converges onto the
	// single surviving peer.
	sjs := survivor.getJetStream()
	sjs.mu.RLock()
	origGroup := sjs.streamAssignment(globalAccountName, "TEST").Group.Name
	sjs.mu.RUnlock()

	dead.Shutdown()
	dead.WaitForShutdown()

	checkFor(t, 10*time.Second, 250*time.Millisecond, func() error {
		if leader := survivor.getJetStream().getMetaGroup().GroupLeader(); leader != _EMPTY_ {
			return fmt.Errorf("survivor still sees meta leader %q", leader)
		}
		return nil
	})

	// Make sure the stream becomes leaderless as well, so it'll need to perform
	// the leaderless eviction after peer-remove.
	mset, err := survivor.globalAccount().lookupStream("TEST")
	require_NoError(t, err)
	err = mset.raftNode().StepDown()
	require_True(t, err == nil || err == errNotLeader)

	nc, err = nats.Connect(survivor.ClientURL(), nats.UserInfo("admin", "s3cr3t!"))
	require_NoError(t, err)
	defer nc.Close()

	// Rescue the meta group so the survivor can elect itself.
	req, err := json.Marshal(&JSApiMetaRescueRequest{QuorumNeeded: 1})
	require_NoError(t, err)
	rmsg, err := nc.Request(JSApiMetaRescue, req, time.Second)
	require_NoError(t, err)
	var rescueResp JSApiMetaRescueResponse
	require_NoError(t, json.Unmarshal(rmsg.Data, &rescueResp))
	require_True(t, rescueResp.Error == nil)
	require_Equal(t, rescueResp.PrevQuorum, 2)
	require_Equal(t, rescueResp.NewQuorum, 1)
	c.waitOnLeader()

	// Peer-remove the lost server, which clears the rescue and reconciles the
	// stream down onto the survivor.
	removeReq, err := json.Marshal(&JSApiMetaServerRemoveRequest{Server: deadName})
	require_NoError(t, err)
	rmsg, err = nc.Request(JSApiRemoveServer, removeReq, time.Second)
	require_NoError(t, err)
	var removeResp JSApiMetaServerRemoveResponse
	require_NoError(t, json.Unmarshal(rmsg.Data, &removeResp))
	require_True(t, removeResp.Error == nil)
	require_True(t, removeResp.Success)
	nc.Close()

	checkFor(t, 10*time.Second, 250*time.Millisecond, func() error {
		meta := survivor.getJetStream().getMetaGroup()
		if cs := meta.ClusterSize(); cs != 1 {
			return fmt.Errorf("expected meta cluster size 1, got %d", cs)
		}
		if meta.InRescue() {
			return fmt.Errorf("expected rescue to be stopped")
		}
		return nil
	})

	// The group is renamed once it converges onto the single peer, and the stream
	// is restarted under the new group. That is where it used to be left without
	// a leader, so wait for the rename to land before checking it is usable.
	checkFor(t, 5*time.Second, 100*time.Millisecond, func() error {
		js := survivor.getJetStream()
		js.mu.RLock()
		defer js.mu.RUnlock()
		sa := js.streamAssignment(globalAccountName, "TEST")
		if sa == nil {
			return fmt.Errorf("no stream assignment")
		}
		if len(sa.Group.Peers) != 1 || sa.Group.Desired != nil {
			return fmt.Errorf("stream has not converged onto a single peer yet, peers %+v", sa.Group.Peers)
		}
		if sa.Group.Name == origGroup {
			return fmt.Errorf("stream group has not been renamed yet, still %q", origGroup)
		}
		return nil
	})

	// The stream must be usable again on the survivor alone. It stays configured
	// as R2, it is only under-replicated until the lost server comes back.
	nc, js = jsClientConnect(t, survivor)
	defer nc.Close()
	checkFor(t, 5*time.Second, 100*time.Millisecond, func() error {
		si, err := js.StreamInfo("TEST")
		if err != nil {
			return err
		}
		if si.Cluster == nil || si.Cluster.Leader == _EMPTY_ {
			return fmt.Errorf("no stream leader")
		}
		if len(si.Cluster.Replicas) != 0 {
			return fmt.Errorf("expected no replicas, got %d", len(si.Cluster.Replicas))
		}
		if si.Config.Replicas != 2 {
			return fmt.Errorf("expected the stream to stay configured as R2, got R%d", si.Config.Replicas)
		}
		if si.State.Msgs != 5 {
			return fmt.Errorf("expected 5 messages, got %d", si.State.Msgs)
		}
		return nil
	})
	_, err = js.Publish("foo", []byte("after-recovery"))
	require_NoError(t, err)

	// Now the lost server comes back, under the same name and with its old state.
	c.restartServer(dead)
	c.checkClusterFormed()

	// The meta group must take it back into the peer set.
	c.waitOnPeerCount(2)
	checkFor(t, 5*time.Second, 100*time.Millisecond, func() error {
		meta := survivor.getJetStream().getMetaGroup()
		if cs := meta.ClusterSize(); cs != 2 {
			return fmt.Errorf("expected meta cluster size 2, got %d", cs)
		}
		if qn := meta.QuorumNeeded(); qn != 2 {
			return fmt.Errorf("expected quorum 2, got %d", qn)
		}
		return nil
	})

	// And the stream must heal back up to its configured replica count.
	checkFor(t, 5*time.Second, 100*time.Millisecond, func() error {
		si, err := js.StreamInfo("TEST")
		if err != nil {
			return err
		}
		if si.Cluster == nil || si.Cluster.Leader == _EMPTY_ {
			return fmt.Errorf("no stream leader")
		}
		if len(si.Cluster.Replicas) != 1 {
			return fmt.Errorf("expected 1 replica, got %d", len(si.Cluster.Replicas))
		}
		if !si.Cluster.Replicas[0].Current {
			return fmt.Errorf("replica %q is not current", si.Cluster.Replicas[0].Name)
		}
		state, err := checkStateAndErr(t, c, globalAccountName, "TEST")
		if err != nil {
			return err
		}
		if state.Msgs != 6 {
			return fmt.Errorf("expected 6 messages, got %d", state.Msgs)
		}
		return nil
	})
}

func TestJetStreamClusterMetaReplicasInJsz(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "R3S", 3)
	defer c.shutdown()

	// The configured meta peer set must be visible on every server, not just
	// the meta leader, so it can be inspected during disaster recovery when
	// there is no meta leader.
	checkFor(t, 2*time.Second, 250*time.Millisecond, func() error {
		for _, s := range c.servers {
			jsz, err := s.Jsz(nil)
			if err != nil {
				return err
			}
			if jsz.Meta == nil {
				return fmt.Errorf("no meta cluster info on %q", s.Name())
			}
			// Excludes the server itself, hence one less than the peer set size.
			if len(jsz.Meta.Replicas) != 2 {
				return fmt.Errorf("expected 2 meta replicas on %q, got %d", s.Name(), len(jsz.Meta.Replicas))
			}
			if jsz.Meta.QuorumNeeded != 2 {
				return fmt.Errorf("expected quorum 2 on %q, got %d", s.Name(), jsz.Meta.QuorumNeeded)
			}
			if jsz.Meta.Rescue {
				return fmt.Errorf("expected no active rescue on %q", s.Name())
			}
		}
		return nil
	})
}

func TestJetStreamClusterPendingPeersReportedInClusterInfo(t *testing.T) {
	js := &jetStream{srv: &Server{}}

	peerInfo := func(ci *ClusterInfo, peer string) *PeerInfo {
		t.Helper()
		for _, pi := range ci.Replicas {
			if pi.Peer == peer {
				return pi
			}
		}
		t.Fatalf("peer %q not reported in cluster info", peer)
		return nil
	}

	// The assignment has scaled up to three peers, but only "a" has joined the
	// Raft group so far. The peers that are only known from the assignment must
	// be distinguishable from the one that's an actual peer of the group.
	rg := &raftGroup{
		Name:  "test",
		Peers: []string{"a", "b", "c"},
		node:  &raft{peers: map[string]*lps{"a": {}}},
	}
	ci := js.clusterInfo(rg)
	require_Len(t, len(ci.Replicas), 3)
	require_False(t, peerInfo(ci, "a").Pending)
	require_True(t, peerInfo(ci, "b").Pending)
	require_True(t, peerInfo(ci, "c").Pending)

	// Once they've all joined the group nothing is pending anymore.
	rg.node = &raft{peers: map[string]*lps{"a": {}, "b": {}, "c": {}}}
	ci = js.clusterInfo(rg)
	require_Len(t, len(ci.Replicas), 3)
	for _, pi := range ci.Replicas {
		require_False(t, pi.Pending)
	}

	// Without a Raft node we have no knowledge of group membership, so we can't
	// claim any of the assigned peers are pending.
	rg.node = nil
	rg.Desired = &desiredRaftGroup{ID: "id", Cluster: "C1", Peers: []string{"a", "b", "c"}}
	ci = js.clusterInfo(rg)
	require_Len(t, len(ci.Replicas), 3)
	for _, pi := range ci.Replicas {
		require_False(t, pi.Pending)
	}
}

func TestJetStreamClusterLocalPeerReportedInClusterInfoWithoutNode(t *testing.T) {
	s := &Server{}
	s.sys = &internal{shash: "a"}
	js := &jetStream{srv: s}

	hasPeer := func(ci *ClusterInfo, peer string) bool {
		t.Helper()
		return slices.ContainsFunc(ci.Replicas, func(pi *PeerInfo) bool { return pi.Peer == peer })
	}
	// We're serving the request, so we must be reported as current and not pending.
	requireOurPeerCurrent := func(ci *ClusterInfo) {
		t.Helper()
		i := slices.IndexFunc(ci.Replicas, func(pi *PeerInfo) bool { return pi.Peer == "a" })
		require_True(t, i >= 0)
		require_True(t, ci.Replicas[i].Current)
		require_False(t, ci.Replicas[i].Offline)
		require_False(t, ci.Replicas[i].Pending)
	}

	// Scaling up from R1: we are the only current peer and the node isn't up yet.
	rg := &raftGroup{
		Name:    "test",
		Peers:   []string{"a"},
		Desired: &desiredRaftGroup{ID: "id", Cluster: "C1", Peers: []string{"a", "b", "c"}},
	}
	ci := js.clusterInfo(rg)
	require_Equal(t, ci.Leader, _EMPTY_)
	require_Len(t, len(ci.Replicas), 1)
	require_True(t, hasPeer(ci, "a"))
	requireOurPeerCurrent(ci)
	require_Len(t, len(ci.Desired.Replicas), 3)

	// Same for a multi-peer group that's being remapped, where the node was cleared.
	rg.Peers = []string{"a", "b", "c"}
	ci = js.clusterInfo(rg)
	require_Equal(t, ci.Leader, _EMPTY_)
	require_Len(t, len(ci.Replicas), 3)
	require_True(t, hasPeer(ci, "a"))
	requireOurPeerCurrent(ci)

	// Scaling down doesn't report desired peers, but must still report the current set.
	rg.Desired.ScaleDown = true
	ci = js.clusterInfo(rg)
	require_Equal(t, ci.Leader, _EMPTY_)
	require_Len(t, len(ci.Replicas), 3)
	require_True(t, hasPeer(ci, "a"))
	requireOurPeerCurrent(ci)
	require_Len(t, len(ci.Desired.Replicas), 0)

	// With a node we keep suppressing ourselves, the leader is reported separately.
	rg.node = &raft{peers: map[string]*lps{"a": {}, "b": {}, "c": {}}, leader: "a"}
	s.nodeToInfo.Store("a", nodeInfo{name: "S-1"})
	ci = js.clusterInfo(rg)
	require_Equal(t, ci.Leader, "S-1")
	require_Len(t, len(ci.Replicas), 2)
	require_False(t, hasPeer(ci, "a"))
}

func TestJetStreamClusterMigrationStatusReportedInClusterInfo(t *testing.T) {
	js := &jetStream{srv: &Server{}}

	newGroup := func(desired *desiredRaftGroup) *raftGroup {
		return &raftGroup{
			Name:    "test",
			Peers:   []string{"a"},
			node:    &raft{},
			Desired: desired,
		}
	}

	// A status without any desired state must still be reported, on an otherwise
	// empty desired block. This is what a group requesting desired state looks like.
	rg := newGroup(nil)
	js.setMigrationStatus(rg, mstat(MigrationStatusMeta, "requesting desired state from meta leader"))
	ci := js.clusterInfo(rg)
	require_NotNil(t, ci.Desired)
	require_NotNil(t, ci.Desired.Status)
	require_Equal(t, ci.Desired.Status.Description, "requesting desired state from meta leader")
	require_Equal(t, ci.Desired.Status.Type, "meta")
	require_Equal(t, ci.Desired.Status.Err, _EMPTY_)
	require_Equal(t, ci.Desired.Name, _EMPTY_)
	require_Len(t, len(ci.Desired.Replicas), 0)

	// With desired state the status sits alongside it, and doesn't disturb it.
	rg = newGroup(&desiredRaftGroup{ID: "id", Cluster: "C1", Peers: []string{"a", "b"}})
	js.setMigrationStatus(rg, mstat(MigrationStatusMembership, "adding peer %s", "s2"))
	ci = js.clusterInfo(rg)
	require_NotNil(t, ci.Desired)
	require_NotNil(t, ci.Desired.Status)
	require_Equal(t, ci.Desired.Status.Description, "adding peer s2")
	require_Equal(t, ci.Desired.Status.Type, "membership")
	require_Equal(t, ci.Desired.Name, "C1")
	require_Len(t, len(ci.Desired.Replicas), 2)

	// A persistent fault is reported beside the line, leaving the line itself stable.
	rg = newGroup(nil)
	diskErr := errors.New("no space left on device")
	js.setMigrationStatus(rg, mstat(MigrationStatusSnapshot, "waiting to encode state for snapshot").withErr(diskErr))
	ci = js.clusterInfo(rg)
	require_NotNil(t, ci.Desired)
	require_NotNil(t, ci.Desired.Status)
	require_Equal(t, ci.Desired.Status.Description, "waiting to encode state for snapshot")
	require_Equal(t, ci.Desired.Status.Type, "snapshot")
	require_Equal(t, ci.Desired.Status.Err, "no space left on device")

	// Errors that resolve themselves on the next cycle must not be reported, they'd
	// only flap in and out of stream info while the migration is healthy.
	for _, benign := range []error{ErrStoreClosed, errNotLeader, errNodeClosed, errMembershipChange, errNoSnapAvailable} {
		rg = newGroup(nil)
		js.setMigrationStatus(rg, mstat(MigrationStatusMembership, "adding peer s2").withErr(benign))
		ci = js.clusterInfo(rg)
		require_NotNil(t, ci.Desired)
		require_NotNil(t, ci.Desired.Status)
		require_Equal(t, ci.Desired.Status.Description, "adding peer s2")
		require_Equal(t, ci.Desired.Status.Err, _EMPTY_)
	}

	// A converged group reports desired state without a status.
	rg = newGroup(&desiredRaftGroup{ID: "id", Cluster: "C1", Peers: []string{"a", "b"}})
	ci = js.clusterInfo(rg)
	require_NotNil(t, ci.Desired)
	require_True(t, ci.Desired.Status == nil)

	// Clearing the status must not leave an empty desired block behind.
	rg = newGroup(nil)
	js.setMigrationStatus(rg, mstat(MigrationStatusSnapshot, "installing snapshot"))
	js.setMigrationStatus(rg, nil)
	ci = js.clusterInfo(rg)
	require_True(t, ci.Desired == nil)
}

// The sourcing state outlives the messages it was collected from, so unlike other
// per-message derived state it can't always be rebuilt from what's replicated. A
// replica that catches up over a stream whose sourced messages are already gone
// never sees the headers that carried the source's position, and would resume
// sourcing from an earlier point once it becomes leader.
func TestJetStreamClusterSourcesStateSnapshot(t *testing.T) {
	for _, enabled := range []bool{false, true} {
		title := "Disabled"
		if enabled {
			title = "Enabled"
		}
		t.Run(title, func(t *testing.T) {
			c := createJetStreamClusterExplicit(t, "R3S", 3)
			defer c.shutdown()

			for _, s := range c.servers {
				s.optsMu.Lock()
				s.opts.FeatureFlags = map[string]bool{FeatureFlagJsSnapshotSources: enabled}
				s.optsMu.Unlock()
			}

			nc, js := jsClientConnect(t, c.randomServer())
			defer nc.Close()

			_, err := js.AddStream(&nats.StreamConfig{
				Name:     "ORIGIN",
				Subjects: []string{"foo"},
				Replicas: 3,
			})
			require_NoError(t, err)

			// Start at R1, so the other peers never take part in the sourcing.
			cfg := &nats.StreamConfig{
				Name:     "SOURCE",
				Sources:  []*nats.StreamSource{{Name: "ORIGIN"}},
				Replicas: 1,
			}
			_, err = js.AddStream(cfg)
			require_NoError(t, err)

			const total = 5
			for range total {
				_, err = js.Publish("foo", nil)
				require_NoError(t, err)
			}

			c.waitOnStreamLeader(globalAccountName, "SOURCE")
			sl := c.streamLeader(globalAccountName, "SOURCE")
			mset, err := sl.globalAccount().lookupStream("SOURCE")
			require_NoError(t, err)

			checkFor(t, 5*time.Second, 100*time.Millisecond, func() error {
				if msgs := mset.store.State().Msgs; msgs != total {
					return fmt.Errorf("expected %d messages, got %d", total, msgs)
				}
				return nil
			})

			const iname = "ORIGIN > >"
			require_Equal(t, mset.store.SourcesState()[iname].Seq, total)

			// Purge. The tracked state deliberately survives, but the messages that
			// established it are gone, so it can no longer be derived by a scan.
			_, err = mset.purge(nil)
			require_NoError(t, err)
			require_Equal(t, mset.store.State().Msgs, 0)
			require_Equal(t, mset.store.SourcesState()[iname].Seq, total)

			// Scale out. The added replicas catch up over an emptied stream, so they
			// never see the headers that carried the source's position.
			cfg.Replicas = 3
			_, err = js.UpdateStream(cfg)
			require_NoError(t, err)
			c.waitOnStreamLeader(globalAccountName, "SOURCE")
			for _, s := range c.servers {
				c.waitOnStreamCurrent(s, globalAccountName, "SOURCE")
			}

			var known int
			checkFor(t, 5*time.Second, 100*time.Millisecond, func() error {
				known = 0
				for _, s := range c.servers {
					m, err := s.globalAccount().lookupStream("SOURCE")
					if err != nil {
						return err
					}
					if m.store.SourcesState()[iname].Seq == total {
						known++
					}
				}
				if enabled && known != 3 {
					return fmt.Errorf("expected all 3 replicas to know the source position, got %d", known)
				}
				return nil
			})

			if enabled {
				// The snapshot carried it, so every replica agrees.
				require_Equal(t, known, 3)
			} else {
				// Only the replica that did the sourcing knows where to resume.
				require_Equal(t, known, 1)
			}
		})
	}
}

// A stream that gets remapped into a new Raft group has its old node removed out from
// underneath the running monitor. A monitor parked in catchup only surfaces the resulting
// error afterwards, and must not reset the stream: the meta layer has already moved us to
// a new group, so a reset would tear the stream down and resurrect the replaced group.
func TestJetStreamClusterNoResetClusteredStateAfterStreamRemap(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "R3S", 3)
	defer c.shutdown()

	nc, js := jsClientConnect(t, c.randomServer())
	defer nc.Close()

	for _, name := range []string{"TEST", "OTHER"} {
		_, err := js.AddStream(&nats.StreamConfig{
			Name:     name,
			Subjects: []string{name + ".foo"},
			Replicas: 3,
		})
		require_NoError(t, err)
	}

	// Use a follower so we don't disturb the stream leader.
	s := c.randomNonStreamLeader(globalAccountName, "TEST")
	mset, err := s.globalAccount().lookupStream("TEST")
	require_NoError(t, err)

	// The node this monitor generation owns.
	n := mset.raftNode()
	require_NotNil(t, n)

	t.Run("NodeRemoved", func(t *testing.T) {
		// Simulate processClusterUpdateStream detecting a stream remap: the node is removed
		// and the monitor is torn down, both while the monitor is parked in catchup.
		mset.removeNode()
		mset.stopMonitoring()
		require_True(t, mset.raftNode() == nil)

		// This is what the parked monitor does once its catchup finally aborts.
		require_True(t, mset.resetClusteredState(n, errCatchupAbortedNoLeader))

		// The stream must be left intact for the meta layer to converge on, not stopped
		// and recreated from the now stale stream assignment.
		time.Sleep(500 * time.Millisecond)
		require_False(t, mset.closed.Load())
		nmset, err := s.globalAccount().lookupStream("TEST")
		require_NoError(t, err)
		require_True(t, nmset == mset)
	})

	t.Run("NodeReplaced", func(t *testing.T) {
		// Same remap, but caught after a different node has already been linked in. The
		// monitor is still stopped from the subtest above, so nothing races us here.
		other, err := s.globalAccount().lookupStream("OTHER")
		require_NoError(t, err)
		replacement := other.raftNode()
		require_NotNil(t, replacement)

		mset.mu.Lock()
		mset.node = replacement
		mset.mu.Unlock()

		// Our generation's node is no longer the stream's node, so we must not reset.
		require_True(t, mset.resetClusteredState(n, errCatchupAbortedNoLeader))

		time.Sleep(500 * time.Millisecond)
		require_False(t, mset.closed.Load())
		require_False(t, other.closed.Load())

		mset.mu.Lock()
		mset.node = nil
		mset.mu.Unlock()
	})
}

// A scale down is an explicit request to shrink the group. Peers being removed do
// not need the data, so a catchup to them must not hold up the scale down, even
// when that catchup can't complete on its own.
func TestJetStreamClusterScaleDownCancelsCatchup(t *testing.T) {
	// Speed up catchup stall/retry cycles.
	streamCatchupActivityInterval = 2 * time.Second
	t.Cleanup(func() {
		streamCatchupActivityInterval = defaultStreamCatchupActivityInterval
	})

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

	toSend := 1_000
	for range toSend {
		_, err = js.Publish("foo", []byte("HELLO WORLD"))
		require_NoError(t, err)
	}

	sl := c.streamLeader(globalAccountName, "TEST")
	require_NotNil(t, sl)
	mset, err := sl.globalAccount().lookupStream("TEST")
	require_NoError(t, err)

	scale := func(r int) {
		t.Helper()
		_, err := js.UpdateStream(&nats.StreamConfig{
			Name:     "TEST",
			Subjects: []string{"foo"},
			Replicas: r,
		})
		require_NoError(t, err)
	}
	// Waits for the group to settle at R, with every replica store-current.
	waitReplicas := func(r int) {
		t.Helper()
		checkFor(t, 5*time.Second, 100*time.Millisecond, func() error {
			si, err := js.StreamInfo("TEST", nats.MaxWait(time.Second))
			if err != nil {
				return err
			}
			if si.Cluster == nil {
				return fmt.Errorf("no cluster info")
			}
			if len(si.Cluster.Replicas) != r-1 {
				return fmt.Errorf("expected %d replicas, got %d", r-1, len(si.Cluster.Replicas))
			}
			if si.Config.Replicas != r {
				return fmt.Errorf("expected R%d config, got R%d", r, si.Config.Replicas)
			}
			for _, r := range si.Cluster.Replicas {
				if !r.Current || r.Lag != 0 {
					return fmt.Errorf("replica %s not store-current", r.Name)
				}
			}
			return nil
		})
	}

	// Exhaust the leader's outbound catchup budget, as if another large catchup
	// holds all of it, so the new peers can't complete their store catchup.
	hold := new(int64)
	sl.gcbAdd(hold, int64(1)<<30)

	// Scale up to R3. The new peers receive a raft snapshot and must pull the
	// stream data through the (stalled) upper layer catchup.
	scale(3)

	// Wait for the group to grow with store catchups in flight.
	checkFor(t, 5*time.Second, 100*time.Millisecond, func() error {
		node := mset.raftNode()
		if node == nil {
			return fmt.Errorf("no raft node yet")
		}
		if peers := node.PeerNames(); len(peers) != 3 {
			return fmt.Errorf("expected 3 peers, got %d", len(peers))
		}
		if !mset.hasCatchupPeers() {
			return fmt.Errorf("no store catchups in flight yet")
		}
		return nil
	})

	// Now scale back down to R1 while those catchups can't make progress. This must
	// complete without releasing the budget, canceling the catchups rather than
	// waiting on them.
	scale(1)
	waitReplicas(1)

	// The catchups must have been canceled.
	checkFor(t, 5*time.Second, 100*time.Millisecond, func() error {
		if mset.hasCatchupPeers() {
			return fmt.Errorf("still tracking catchup peers")
		}
		return nil
	})
	require_NoError(t, checkState(t, c, globalAccountName, "TEST"))

	// Release the budget and scale back up. The peers we just canceled must now be
	// caught up as normal.
	sl.gcbSubLast(hold)
	scale(3)
	waitReplicas(3)
	// Replicas are Raft-current once added, their store catchup may still be finishing.
	checkFor(t, 5*time.Second, 100*time.Millisecond, func() error {
		return checkState(t, c, globalAccountName, "TEST")
	})
}

func TestJetStreamClusterConsumerScaleDownDesiredAfterMetaLeaderChange(t *testing.T) {
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

	_, err = js.AddConsumer("TEST", &nats.ConsumerConfig{
		Durable:   "CONSUMER",
		AckPolicy: nats.AckExplicitPolicy,
		Replicas:  3,
	})
	require_NoError(t, err)

	ml := c.leader()
	require_NotNil(t, ml)
	mjs := ml.getJetStream()

	mjs.mu.RLock()
	ca := mjs.consumerAssignmentOrInflight(globalAccountName, "TEST", "CONSUMER")
	require_NotNil(t, ca)
	peers := copyStrings(ca.Group.Peers)
	mjs.mu.RUnlock()
	require_Len(t, len(peers), 3)

	// Stop two of the consumer's peers, but keep the meta leader running. The
	// consumer group loses quorum, the meta group keeps it.
	var down int
	for _, s := range c.servers {
		if s == ml || !slices.Contains(peers, s.Node()) {
			continue
		}
		s.Shutdown()
		s.WaitForShutdown()
		if down++; down == 2 {
			break
		}
	}
	require_Len(t, down, 2)

	// Only its leader can resolve a scale down, wait for it to lose it.
	checkFor(t, 10*time.Second, 200*time.Millisecond, func() error {
		if cl := c.consumerLeader(globalAccountName, "TEST", "CONSUMER"); cl != nil {
			return fmt.Errorf("consumer still has leader %q", cl.Name())
		}
		return nil
	})

	// Scale the consumer down R3->R1. Nothing can select which peer remains,
	// so the scale down stays pending and the request times out.
	nc.Close()
	nc, js = jsClientConnect(t, ml)
	defer nc.Close()

	_, err = js.UpdateConsumer("TEST", &nats.ConsumerConfig{
		Durable:   "CONSUMER",
		AckPolicy: nats.AckExplicitPolicy,
		Replicas:  1,
	}, nats.MaxWait(time.Second))
	require_Error(t, err, context.DeadlineExceeded)

	// Confirm the scale down is registered, and still pending.
	checkFor(t, 5*time.Second, 200*time.Millisecond, func() error {
		mjs.mu.RLock()
		defer mjs.mu.RUnlock()
		ca := mjs.consumerAssignment(globalAccountName, "TEST", "CONSUMER")
		if ca == nil {
			return errors.New("no consumer assignment")
		}
		if ca.Group.Desired == nil || !ca.Group.Desired.ScaleDown {
			return fmt.Errorf("expected pending scale down, got: %+v", ca.Group.Desired)
		}
		if len(ca.Group.Peers) != 3 {
			return fmt.Errorf("expected 3 peers, got: %v", ca.Group.Peers)
		}
		return nil
	})

	// Now change the meta leader, which reconciles all peer assignments on election.
	meta := mjs.getMetaGroup()
	require_NoError(t, meta.StepDown())
	c.waitOnLeader()

	nml := c.leader()
	require_NotNil(t, nml)
	njs := nml.getJetStream()

	// Reconciling and applying meta entries happen on the same goroutine. So once a
	// stream we only add now is applied, the new meta leader must have reconciled.
	nc.Close()
	nc, js = jsClientConnect(t, nml)
	defer nc.Close()
	_, err = js.AddStream(&nats.StreamConfig{Name: "MARKER", Replicas: 1})
	require_NoError(t, err)
	checkFor(t, 5*time.Second, 100*time.Millisecond, func() error {
		njs.mu.RLock()
		defer njs.mu.RUnlock()
		if njs.streamAssignment(globalAccountName, "MARKER") == nil {
			return errors.New("marker stream not applied yet")
		}
		return nil
	})

	// The peers must not be scaled down already, while desired state still says to.
	njs.mu.RLock()
	defer njs.mu.RUnlock()
	ca = njs.consumerAssignment(globalAccountName, "TEST", "CONSUMER")
	require_NotNil(t, ca)
	require_NotNil(t, ca.Group.Desired)
	require_True(t, ca.Group.Desired.ScaleDown)
	if len(ca.Group.Peers) != len(ca.Group.Desired.Peers) {
		t.Fatalf("Consumer assignment scaled down to peers=%v, while desired state is still a scale down from %v",
			ca.Group.Peers, ca.Group.Desired.Peers)
	}
}

func TestJetStreamClusterReconcileMigratesConsumerBeforeDroppingStreamPeer(t *testing.T) {
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

	// R1 consumer, so it lives on exactly one of the stream's peers.
	_, err = js.AddConsumer("TEST", &nats.ConsumerConfig{
		Durable:   "CONSUMER",
		AckPolicy: nats.AckExplicitPolicy,
		Replicas:  1,
	})
	require_NoError(t, err)

	ml := c.leader()
	require_NotNil(t, ml)
	mjs := ml.getJetStream()

	// The consumer leader responds to a create, wait for the meta leader to apply it.
	checkFor(t, 2*time.Second, 10*time.Millisecond, func() error {
		mjs.mu.RLock()
		defer mjs.mu.RUnlock()
		if mjs.consumerAssignment(globalAccountName, "TEST", "CONSUMER") == nil {
			return errors.New("consumer assignment not applied yet")
		}
		return nil
	})

	// The peer hosting the consumer is the one the scale down drops.
	cl := c.consumerLeader(globalAccountName, "TEST", "CONSUMER")
	require_NotNil(t, cl)
	victim := cl.Node()
	var metaPeers []string
	for _, s := range c.servers {
		if p := s.Node(); p != victim {
			metaPeers = append(metaPeers, p)
		}
	}
	require_Len(t, len(metaPeers), 2)

	// Scale down R3->R1 onto a peer that isn't hosting the consumer, as a stream leader
	// that didn't know about the consumer would have selected. The term is deliberately
	// far ahead of any real group term, so the real group leader's updates are fenced off,
	// and only the requests below are applied.
	const desiredID = "reconcile-consumer-hosted-peer"
	mjs.mu.Lock()
	sa := mjs.streamAssignment(globalAccountName, "TEST")
	if sa != nil {
		sa.Group.Desired = &desiredRaftGroup{
			ID:      desiredID,
			Term:    math.MaxUint64,
			Peers:   []string{metaPeers[0]},
			Created: time.Now().UTC(),
		}
	}
	mjs.mu.Unlock()
	require_NotNil(t, sa)

	// The stream leader reports it already removed the peer from its group.
	reconcile := func(id string) {
		t.Helper()
		msg, err := json.Marshal(&streamAssignmentReconcile{
			Account: globalAccountName,
			Stream:  "TEST",
			desiredAssignmentUpdate: desiredAssignmentUpdate{
				ID:         id,
				Term:       math.MaxUint64,
				MetaPeers:  metaPeers,
				PeersMatch: true,
			},
		})
		require_NoError(t, err)
		mjs.reconcileDesiredStreamAssignment(nil, nil, nil, _EMPTY_, _EMPTY_, msg)
	}
	reconcile(desiredID)

	// The consumer must be migrated off the peer, keeping it while it moves so its state
	// is copied over, and the stream must hold on to that peer until the move completes.
	// Asserted on the applied assignments, so this also waits for the staged proposals to
	// land, and the state we adjust below isn't overwritten by one arriving afterward.
	checkFor(t, 5*time.Second, 100*time.Millisecond, func() error {
		mjs.mu.RLock()
		defer mjs.mu.RUnlock()
		ca := mjs.consumerAssignment(globalAccountName, "TEST", "CONSUMER")
		if ca == nil || ca.Group == nil || ca.Group.Desired == nil {
			return errors.New("consumer not migrating yet")
		}
		if !slices.Contains(ca.Group.Peers, victim) {
			return fmt.Errorf("consumer must keep %q while it migrates, got %+v", victim, ca.Group.Peers)
		}
		if slices.Contains(ca.Group.Desired.Peers, victim) {
			return fmt.Errorf("consumer must migrate off %q, got %+v", victim, ca.Group.Desired.Peers)
		}
		sa := mjs.streamAssignment(globalAccountName, "TEST")
		if sa == nil || sa.Group == nil || sa.Group.Desired == nil {
			return errors.New("stream assignment not applied yet")
		}
		// The stream can't drop the peer from under the consumer, it would have nothing
		// to migrate its state from.
		if !slices.Contains(sa.Group.Peers, victim) {
			return fmt.Errorf("stream dropped %q while the consumer is hosted there, got %+v", victim, sa.Group.Peers)
		}
		return nil
	})

	// Land the consumer on its desired peers, as completing the migration would.
	var nextID string
	mjs.mu.Lock()
	if ca := mjs.consumerAssignment(globalAccountName, "TEST", "CONSUMER"); ca != nil && ca.Group.Desired != nil {
		ca.Group.Peers, ca.Group.Desired = ca.Group.Desired.Peers, nil
	}
	if sa := mjs.streamAssignment(globalAccountName, "TEST"); sa != nil && sa.Group.Desired != nil {
		nextID = sa.Group.Desired.ID
	}
	mjs.mu.Unlock()
	require_NotEqual(t, nextID, _EMPTY_)

	// Nothing holds the peer anymore, so now the same update drops it.
	reconcile(nextID)
	checkFor(t, 5*time.Second, 100*time.Millisecond, func() error {
		mjs.mu.RLock()
		defer mjs.mu.RUnlock()
		sa := mjs.streamAssignmentOrInflight(globalAccountName, "TEST")
		if sa == nil || sa.Group == nil {
			return errors.New("no stream assignment")
		}
		if slices.Contains(sa.Group.Peers, victim) {
			return fmt.Errorf("expected %q to be dropped, got %+v", victim, sa.Group.Peers)
		}
		return nil
	})
}

// Scale up peers observe while the source is down, and take over once it's back or once it's peer-removed.
func TestJetStreamClusterScaleUpSourceDown(t *testing.T) {
	// Shorter election timeouts keep the wait for a non-election short.
	omin, omax := minElectionTimeout, maxElectionTimeout
	minElectionTimeout, maxElectionTimeout = 250*time.Millisecond, time.Second
	defer func() {
		minElectionTimeout, maxElectionTimeout = omin, omax
	}()

	for _, remove := range []bool{false, true} {
		t.Run(fmt.Sprintf("Remove=%v", remove), func(t *testing.T) {
			c := createJetStreamClusterExplicit(t, "R4S", 4)
			defer c.shutdown()

			nc, js := jsClientConnect(t, c.randomServer())
			defer nc.Close()
			_, err := js.AddStream(&nats.StreamConfig{Name: "TEST", Subjects: []string{"foo"}, Replicas: 1})
			require_NoError(t, err)
			_, err = js.AddConsumer("TEST", &nats.ConsumerConfig{Durable: "C", AckPolicy: nats.AckExplicitPolicy})
			require_NoError(t, err)
			for range 10 {
				_, err = js.Publish("foo", nil)
				require_NoError(t, err)
			}

			// The source goes down below, so it mustn't be the meta leader.
			sl := c.streamLeader(globalAccountName, "TEST")
			var ml *Server
			checkFor(t, 10*time.Second, 100*time.Millisecond, func() error {
				if ml = c.leader(); ml == nil || ml == sl {
					if ml != nil {
						ml.getJetStream().getMetaGroup().StepDown()
					}
					return fmt.Errorf("meta leader is unknown or the source")
				}
				return nil
			})
			slID, mljs := sl.Node(), ml.getJetStream()
			nc, js = jsClientConnect(t, ml)
			defer nc.Close()

			// The stream's assignment and Raft node on a running server.
			assignment := func(s *Server) (*raftGroup, RaftNode) {
				sjs := s.getJetStream()
				sjs.mu.RLock()
				defer sjs.mu.RUnlock()
				sa := sjs.streamAssignment(globalAccountName, "TEST")
				if sa == nil || sa.Group == nil {
					return nil, nil
				}
				return sa.Group.copyGroup(), sa.Group.node
			}

			// Hold back the source's report extending the assignment, it's applied once the source is down.
			expand := make(chan []byte, 1)
			var hold atomic.Bool
			hold.Store(true)
			mljs.mu.Lock()
			cc := mljs.cluster
			origSub := cc.streamReconcile
			cc.streamReconcile = nil
			mljs.mu.Unlock()
			ml.sysUnsubscribe(origSub)
			mljs.mu.Lock()
			cc.streamReconcile, err = ml.systemSubscribe(streamAssignmentReconcileSubj, _EMPTY_, false, cc.c,
				func(sub *subscription, c *client, acc *Account, subject, reply string, msg []byte) {
					var reconcile streamAssignmentReconcile
					if json.Unmarshal(msg, &reconcile) == nil && hold.Load() && len(reconcile.MetaPeers) > len(reconcile.Members) {
						select {
						case expand <- copyBytes(msg):
						default:
						}
						return
					}
					mljs.reconcileDesiredStreamAssignment(sub, c, acc, subject, reply, msg)
				})
			mljs.mu.Unlock()
			require_NoError(t, err)

			_, err = js.UpdateStream(&nats.StreamConfig{Name: "TEST", Subjects: []string{"foo"}, Replicas: 3})
			require_NoError(t, err)
			var msg []byte
			select {
			case msg = <-expand:
			case <-time.After(10 * time.Second):
				t.Fatalf("source never reported the extended assignment")
			}
			sl.Shutdown()
			sl.WaitForShutdown()
			hold.Store(false)
			mljs.reconcileDesiredStreamAssignment(nil, nil, nil, _EMPTY_, _EMPTY_, msg)

			// The new peers run their nodes, but without a leader to add them they never elect one among themselves.
			var others []*Server
			checkFor(t, 10*time.Second, 50*time.Millisecond, func() error {
				others = others[:0]
				for _, s := range c.servers {
					if s == sl {
						continue
					}
					if _, n := assignment(s); n != nil {
						others = append(others, s)
					}
				}
				if len(others) != 2 {
					return fmt.Errorf("%d new peers run a node, want 2", len(others))
				}
				return nil
			})
			time.Sleep(2 * maxElectionTimeout)
			require_True(t, c.streamLeader(globalAccountName, "TEST") == nil)
			for _, s := range others {
				_, n := assignment(s)
				require_True(t, n.IsObserver())
			}

			if remove {
				// The source was the only member, the empty peers take over.
				b, err := json.Marshal(JSApiStreamRemovePeerRequest{Peer: slID})
				require_NoError(t, err)
				rmsg, err := nc.Request(fmt.Sprintf(JSApiStreamRemovePeerT, "TEST"), b, 10*time.Second)
				require_NoError(t, err)
				var resp JSApiStreamRemovePeerResponse
				require_NoError(t, json.Unmarshal(rmsg.Data, &resp))
				require_True(t, resp.Success)
			} else {
				sl = c.restartServer(sl)
			}

			// The scale up completes, no peer is left observing.
			checkFor(t, 10*time.Second, 250*time.Millisecond, func() error {
				rg, _ := assignment(c.leader())
				if rg.Desired != nil || rg.ScaleUp || len(rg.Peers) != 3 || remove == slices.Contains(rg.Peers, slID) {
					return fmt.Errorf("stream not converged: peers=%v desired=%v scale up=%v", rg.Peers, rg.Desired != nil, rg.ScaleUp)
				}
				for _, s := range c.servers {
					if s == sl && remove {
						continue
					}
					if _, n := assignment(s); n != nil && n.IsObserver() {
						return fmt.Errorf("%s still an observer", s.Name())
					}
				}
				ci, err := js.ConsumerInfo("TEST", "C", nats.MaxWait(time.Second))
				if err != nil {
					return err
				}
				if len(ci.Cluster.Replicas) != 2 {
					return fmt.Errorf("consumer has %d replicas, want 2", len(ci.Cluster.Replicas))
				}
				return nil
			})
			for _, s := range c.servers {
				if s == sl && remove {
					continue
				}
				if _, n := assignment(s); n != nil {
					c.waitOnStreamCurrent(s, globalAccountName, "TEST")
				}
			}

			// The data went with the source if it was removed.
			want := uint64(10)
			if remove {
				want = 0
			}
			si, err := js.StreamInfo("TEST")
			require_NoError(t, err)
			require_Equal(t, si.State.Msgs, want)
			_, err = js.Publish("foo", nil)
			require_NoError(t, err)
		})
	}
}

// Only peers that aren't members yet run their node as a scale up observer.
func TestJetStreamClusterScaleUpPeerNodeCreation(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "R3S", 3)
	defer c.shutdown()

	s := c.randomServer()
	sjs := s.getJetStream()
	ourID := s.Node()
	peers := serverPeerNames(c.servers)
	other := slices.DeleteFunc(copyStrings(peers), func(p string) bool { return p == ourID })

	for _, test := range []struct {
		name         string
		rg           *raftGroup
		recovering   bool
		observer     bool
		initializing bool
	}{
		{"NewGroup", &raftGroup{Peers: peers}, false, false, true},
		{"MemberOfMigration", &raftGroup{Peers: peers, ScaleUp: true,
			Desired: &desiredRaftGroup{ID: "desired", Peers: peers, Members: peers}}, false, false, true},
		{"ScaleUpPeer", &raftGroup{Peers: peers, ScaleUp: true,
			Desired: &desiredRaftGroup{ID: "desired", Peers: peers, Members: other}}, false, true, false},
		// A recorded membership holds on recovery as well.
		{"RecoveringScaleUpPeer", &raftGroup{Peers: peers, ScaleUp: true,
			Desired: &desiredRaftGroup{ID: "desired", Peers: peers, Members: other}}, true, true, false},
		{"LegacyDesiredNoMembers", &raftGroup{Peers: peers, ScaleUp: true, Preferred: other[0],
			Desired: &desiredRaftGroup{ID: "desired", Peers: peers}}, false, true, false},
		// Without a recorded membership the preferred peer is the source.
		{"LegacyPreferredSource", &raftGroup{Peers: peers, ScaleUp: true, Preferred: ourID,
			Desired: &desiredRaftGroup{ID: "desired", Peers: peers}}, false, false, true},
		// Recovery clears the preferred peer, members would otherwise all come back as observers.
		{"LegacyRecovering", &raftGroup{Peers: peers, ScaleUp: true,
			Desired: &desiredRaftGroup{ID: "desired", Peers: peers}}, true, false, false},
	} {
		t.Run(test.name, func(t *testing.T) {
			test.rg.Name = "TEST-" + test.name
			test.rg.Storage = MemoryStorage
			node, err := sjs.createRaftGroup(globalAccountName, test.rg, test.recovering, MemoryStorage, pprofLabels{})
			require_NoError(t, err)
			defer node.Delete()
			n := node.(*raft)
			require_Equal(t, n.IsObserver(), test.observer)
			n.RLock()
			initializing := n.initializing
			n.RUnlock()
			require_Equal(t, initializing, test.initializing)
		})
	}
}
