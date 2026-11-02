// Copyright 2022-2025 The NATS Authors
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

	exceededErrs := []error{errors.New("system limit reached"), errors.New("no suitable peers")}

	// Should fail.
	_, err = js.AddStream(&nats.StreamConfig{
		Name:     "TEST-3",
		Subjects: []string{"baz"},
		Replicas: 3,
	})
	require_Error(t, err, exceededErrs...)
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
		ca := mjs.consumerAssignment(globalAccountName, "TEST", "CONSUMER")
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

	// Remove 1 peer replica (this will be random cloud region as initial placement was randomized ordering)
	_, err = nc.Request("$JS.API.STREAM.PEER.REMOVE.TEST", []byte(`{"peer":"`+osi.Cluster.Replicas[0].Name+`"}`), time.Second*10)
	require_NoError(t, err)

	sc.waitOnStreamLeader(globalAccountName, "TEST")

	// Verify R2 since no eligible peer can replace the removed peer without braking unique constraint
	checkFor(t, time.Second, 200*time.Millisecond, func() error {
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

// The consumer info() call does not take into account whether a consumer
// is a leader or not, so results would be very different when asking servers
// that housed consumer followers vs leaders.
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

	for i := 0; i < 1000; i++ {
		sendStreamMsg(t, nc, "foo", "HELLO")
	}

	sub, err := js.PullSubscribe("foo", "d")
	require_NoError(t, err)

	fetch, ack := 122, 22
	msgs, err := sub.Fetch(fetch, nats.MaxWait(10*time.Second))
	require_NoError(t, err)
	require_True(t, len(msgs) == fetch)
	for _, m := range msgs[:ack] {
		m.AckSync()
	}
	// Let acks propagate.
	time.Sleep(100 * time.Millisecond)

	for _, s := range c.servers {
		jsz, err := s.Jsz(&JSzOptions{Accounts: true, Consumer: true})
		require_NoError(t, err)
		require_True(t, len(jsz.AccountDetails) == 1)
		require_True(t, len(jsz.AccountDetails[0].Streams) == 1)
		require_True(t, len(jsz.AccountDetails[0].Streams[0].Consumer) == 1)
		consumer := jsz.AccountDetails[0].Streams[0].Consumer[0]
		if consumer.Delivered.Consumer != uint64(fetch) || consumer.Delivered.Stream != uint64(fetch) {
			t.Fatalf("Incorrect delivered for %v: %+v", s, consumer.Delivered)
		}
		if consumer.AckFloor.Consumer != uint64(ack) || consumer.AckFloor.Stream != uint64(ack) {
			t.Fatalf("Incorrect ackfloor for %v: %+v", s, consumer.AckFloor)
		}
	}
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
	// Insert into meta layer.
	if sjs := s.getJetStream(); sjs != nil {
		sjs.mu.RLock()
		meta := sjs.cluster.meta
		sjs.mu.RUnlock()
		if meta != nil {
			meta.ForwardProposal(encodeUpdateStreamAssignment(sa))
		}
	}
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
	c := createJetStreamCluster(t, cloudTmpl, "CLOUD", _EMPTY_, 3, 22020, false)
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
	for i := 0; i < num; i++ {
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

	for i := 0; i < 100; i++ {
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

	sreq := &JSApiStreamSnapshotRequest{
		DeliverSubject: nats.NewInbox(),
		ChunkSize:      512,
	}
	req, _ := json.Marshal(sreq)
	rmsg, err := nc.Request(fmt.Sprintf(JSApiStreamSnapshotT, "TEST"), req, time.Second)
	require_NoError(t, err)

	var resp JSApiStreamSnapshotResponse
	json.Unmarshal(rmsg.Data, &resp)
	require_True(t, resp.Error == nil)

	state := *resp.State
	cfg := *resp.Config

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

	var rresp JSApiStreamRestoreResponse
	rreq := &JSApiStreamRestoreRequest{
		Config: cfg,
		State:  state,
	}
	req, _ = json.Marshal(rreq)

	rmsg, err = nc.Request(fmt.Sprintf(JSApiStreamRestoreT, "TEST"), req, 5*time.Second)
	require_NoError(t, err)

	rresp.Error = nil
	json.Unmarshal(rmsg.Data, &rresp)
	require_True(t, rresp.Error == nil)

	checkHealth()

	// We will now chunk the snapshot responses (and EOF).
	var chunk [1024]byte
	for i, r := 0, bytes.NewReader(snapshot); ; {
		n, err := r.Read(chunk[:])
		if err != nil {
			break
		}
		nc.Request(rresp.DeliverSubject, chunk[:n], time.Second)
		i++
		// We will call healthz for all servers halfway through the restore.
		if i%100 == 0 {
			checkHealth()
		}
	}
	rmsg, err = nc.Request(rresp.DeliverSubject, nil, time.Second)
	require_NoError(t, err)
	rresp.Error = nil
	json.Unmarshal(rmsg.Data, &rresp)
	require_True(t, rresp.Error == nil)

	si, err := js.StreamInfo("TEST")
	require_NoError(t, err)
	require_True(t, si.State.Msgs == uint64(toSend))

	checkHealth()

	// Make sure stepdown works, this would fail before the fix.
	_, err = nc.Request(fmt.Sprintf(JSApiStreamLeaderStepDownT, "TEST"), nil, 5*time.Second)
	require_NoError(t, err)

	si, err = js.StreamInfo("TEST")
	require_NoError(t, err)
	require_True(t, si.State.Msgs == uint64(toSend))

	checkHealth()

	// Now make sure if we try to restore to a single server that the artifact is cleaned up and the server returns ok for healthz.
	s := RunBasicJetStreamServer(t)
	defer s.Shutdown()

	nc, _ = jsClientConnect(t, s)
	defer nc.Close()

	rmsg, err = nc.Request(fmt.Sprintf(JSApiStreamRestoreT, "TEST"), req, 5*time.Second)
	require_NoError(t, err)

	rresp.Error = nil
	json.Unmarshal(rmsg.Data, &rresp)
	require_True(t, rresp.Error == nil)

	for i, r := 0, bytes.NewReader(snapshot); ; {
		n, err := r.Read(chunk[:])
		if err != nil {
			break
		}
		_, err = nc.Request(rresp.DeliverSubject, chunk[:n], time.Second)
		require_NoError(t, err)
		i++
	}
	rmsg, err = nc.Request(rresp.DeliverSubject, nil, time.Second)
	require_NoError(t, err)
	rresp.Error = nil
	json.Unmarshal(rmsg.Data, &rresp)

	require_True(t, rresp.Error != nil)
	require_Equal(t, rresp.ApiResponse.Error.ErrCode, 10074)

	status := s.healthz(nil)
	require_Equal(t, status.StatusCode, 200)
}

func TestJetStreamClusterBinaryStreamSnapshotCapability(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "NATS", 3)
	defer c.shutdown()

	nc, js := jsClientConnect(t, c.randomServer())
	defer nc.Close()

	_, err := js.AddStream(&nats.StreamConfig{
		Name:     "TEST",
		Subjects: []string{"foo"},
		Replicas: 3,
	})
	require_NoError(t, err)

	mset, err := c.streamLeader(globalAccountName, "TEST").GlobalAccount().lookupStream("TEST")
	require_NoError(t, err)

	if !mset.supportsBinarySnapshot() {
		t.Fatalf("Expected to signal that we could support binary stream snapshots")
	}
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
	_, err = js.UpdateStream(&nats.StreamConfig{
		Name:     "TEST1",
		Subjects: []string{"foo"},
		MaxBytes: 1 * 1024 * 1024 * 1024,
		MaxMsgs:  1000,
		Replicas: 3,
	})
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
		_, err = js.UpdateStream(&nats.StreamConfig{
			Name:     "TEST",
			Subjects: []string{"bar.>"},
			Replicas: replicas,
		})
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
			sa := sjs.streamAssignment(globalAccountName, "test")
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
	// Publish a couple more messages while it's R1.
	for range 2 {
		_, err = js.Publish("foo", []byte("B"))
		require_NoError(t, err)
	}
	cfg.Replicas = 3
	_, err = js.UpdateStream(cfg)
	require_NoError(t, err)

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

	// Scale stream down and back up.
	cfg.Replicas = 1
	_, err = js.UpdateConsumer("TEST", cfg)
	require_NoError(t, err)
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

		// Scale stream down and back up.
		cfg.Replicas = 1
		_, err = js.UpdateConsumer("TEST", cfg)
		require_NoError(t, err)
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
	ca := sjs.consumerAssignment(globalAccountName, "TEST", "CONSUMER")
	require_NotNil(t, ca)
	cca := ca.copyGroup()
	cca.Group.Name = groupNameForConsumer(cca.Group.Peers, cca.Group.Storage)
	require_NoError(t, cc.meta.Propose(cc.meta.Term(), encodeAddConsumerAssignment(cca)))

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
				s.opts.FeatureFlags = map[string]bool{FeatureFlagJsRaftDeleteRange: enabled}
				s.optsMu.Unlock()
			}

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

			// Enabled uses a huge gap to assert the O(1) apply path; disabled
			// lowers it so the O(n) per-seq path finishes in a reasonable time.
			gap := uint64(100_000_000)
			if !enabled {
				gap = uint64(50_000)
			}
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
		})
	}
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
	sa := mjs.streamAssignment(globalAccountName, "TEST")
	var streamPeers []string
	if sa != nil {
		streamPeers = copyStrings(sa.Group.Peers)
	}
	mjs.mu.RUnlock()
	require_Len(t, len(streamPeers), 5)

	// Shut down three of the stream's peers. Only two of the five stream peers remain online.
	sl := c.streamLeader(globalAccountName, "TEST")
	var offline []*Server
	for _, p := range streamPeers {
		if len(offline) == 3 {
			break
		}
		if s := peerSrv[p]; s != sl && s != ml {
			offline = append(offline, s)
		}
	}
	require_Len(t, len(offline), 3)
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

	// Scale the stream down to R3. Only two of the five peers are online, but
	// the stream must still end up with the requested number of replicas.
	_, err = js.UpdateStream(&nats.StreamConfig{
		Name:     "TEST",
		Subjects: []string{"foo"},
		Replicas: 3,
	})
	require_NoError(t, err)

	// Wait for the scale down to be applied in the meta layer.
	var newPeers []string
	checkFor(t, 2*time.Second, 100*time.Millisecond, func() error {
		mjs.mu.RLock()
		defer mjs.mu.RUnlock()
		sa := mjs.streamAssignment(globalAccountName, "TEST")
		if sa == nil {
			return fmt.Errorf("stream assignment not found")
		}
		if len(sa.Group.Peers) == len(streamPeers) {
			return fmt.Errorf("scale down not applied yet, still %d peers", len(sa.Group.Peers))
		}
		newPeers = copyStrings(sa.Group.Peers)
		return nil
	})

	// The stream peer set must reflect the requested replica count.
	require_Len(t, len(newPeers), 3)

	// The online peers must be preferred and part of the new peer set.
	for _, p := range streamPeers {
		if slices.Contains(offline, peerSrv[p]) {
			continue
		}
		if !slices.Contains(newPeers, p) {
			t.Fatalf("Online peer %q not selected by the scale down", peerSrv[p].Name())
		}
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
	ca := mjs.consumerAssignment(globalAccountName, "TEST", "DUR")
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
	// renames the raft group, but keeps the assignment created time and the
	// stream data.
	mljs := ml.getJetStream()
	mljs.mu.Lock()
	var nsa *streamAssignment
	if osa := mljs.streamAssignment(globalAccountName, "TEST"); osa != nil {
		nsa = osa.copyGroup()
	}
	cc := mljs.cluster
	meta, term := cc.meta, cc.term
	mljs.mu.Unlock()
	require_NotNil(t, nsa)

	ncfg := *nsa.Config
	ncfg.Replicas = 3
	nsa.Config = &ncfg
	nsa.Group.Preferred = nsa.Group.Peers[0]
	nsa.Group.ScaleUp = true
	nsa.Group.Peers = peers
	nsa.Group.Name = groupNameForStream(peers, nsa.Group.Storage)
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
	// the raft group, but keeps the assignment created time and consumer state.
	mljs := ml.getJetStream()
	mljs.mu.Lock()
	var nca *consumerAssignment
	if oca := mljs.consumerAssignment(globalAccountName, "TEST", "DUR"); oca != nil {
		nca = oca.copyGroup()
	}
	cc := mljs.cluster
	meta, term := cc.meta, cc.term
	mljs.mu.Unlock()
	require_NotNil(t, nca)

	ncfg := *nca.Config
	ncfg.Replicas = 3
	nca.Config = &ncfg
	nca.Group.Preferred = nca.Group.Peers[0]
	nca.Group.ScaleUp = true
	nca.Group.Peers = peers
	nca.Group.Name = groupNameForConsumer(peers, nca.Group.Storage)
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

// Mimics a v2.15 server (the meta leader) recording desired state to scale an R1
// stream up to R3. This server is only expected to persist that desired state and
// expose it, the group leader acting on it lands in a later release.
func TestJetStreamClusterDesiredStateFromNewerServerScaleUp(t *testing.T) {
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

	ml := c.leader()
	mjs, cc := ml.getJetStreamCluster()

	// The peer set a v2.15 meta leader would have selected, with the origin peer first.
	mjs.mu.RLock()
	osa := mjs.streamAssignment(globalAccountName, "TEST")
	require_NotNil(t, osa)
	require_Len(t, len(osa.Group.Peers), 1)
	origin := osa.Group.Peers[0]
	peers := []string{origin}
	for _, s := range c.servers {
		if id := s.Node(); id != origin {
			peers = append(peers, id)
		}
	}
	// Build the assignment as a v2.15 meta leader would: the group stays at its
	// origin peer set and only the desired state holds the target.
	nsa := osa.copyGroup()
	// Must not mutate the live assignment's config, it's shared by the clone.
	nsa.Config = osa.Config.clone()
	nsa.Config.Replicas = 3
	nsa.Group.Name = groupNameForStream(peers, nsa.Group.Storage)
	nsa.Group.Desired = &desiredRaftGroup{
		Created:   time.Now().UTC(),
		ID:        "DESIRED",
		Peers:     peers,
		Cluster:   osa.Group.Cluster,
		Preferred: origin,
		Origin: &desiredRaftGroupOrigin{
			Peers:     copyStrings(osa.Group.Peers),
			Cluster:   osa.Group.Cluster,
			Replicas:  osa.Config.Replicas,
			Placement: osa.Config.Placement.clone(),
		},
	}
	desired := nsa.Group.Desired
	oGroupName := osa.Group.Name
	mjs.mu.RUnlock()

	require_NoError(t, cc.meta.Propose(cc.term, encodeUpdateStreamAssignment(nsa)))

	// Every server must have persisted the desired state onto its local assignment,
	// without the assignment itself having moved toward it.
	checkFor(t, 5*time.Second, 100*time.Millisecond, func() error {
		for _, s := range c.servers {
			sjs := s.getJetStream()
			sjs.mu.RLock()
			sa := sjs.streamAssignment(globalAccountName, "TEST")
			var (
				cfg *StreamConfig
				rg  *raftGroup
				d   *desiredRaftGroup
			)
			if sa != nil {
				cfg, rg = sa.Config, sa.Group
				d = rg.Desired
			}
			sjs.mu.RUnlock()

			if sa == nil {
				return fmt.Errorf("no stream assignment on %s", s)
			}
			if d == nil {
				return fmt.Errorf("desired state not persisted on %s", s)
			}
			if d.ID != desired.ID {
				return fmt.Errorf("desired ID on %s, got %q, want %q", s, d.ID, desired.ID)
			}
			if !slices.Equal(d.Peers, peers) {
				return fmt.Errorf("desired peers on %s, got %+v, want %+v", s, d.Peers, peers)
			}
			if d.Preferred != origin {
				return fmt.Errorf("desired preferred on %s, got %q, want %q", s, d.Preferred, origin)
			}
			if d.Origin == nil {
				return fmt.Errorf("desired origin not persisted on %s", s)
			}
			if d.Origin.Replicas != 1 || !slices.Equal(d.Origin.Peers, []string{origin}) {
				return fmt.Errorf("desired origin on %s, got %d/%+v, want 1/%+v",
					s, d.Origin.Replicas, d.Origin.Peers, []string{origin})
			}
			// The config is the requested target, while the group must be untouched:
			// this server persists desired state, it doesn't act on it.
			if cfg.Replicas != 3 {
				return fmt.Errorf("config replicas on %s, got %d, want 3", s, cfg.Replicas)
			}
			if !slices.Equal(rg.Peers, []string{origin}) {
				return fmt.Errorf("group peers on %s, got %+v, want %+v", s, rg.Peers, []string{origin})
			}
			if rg.Name == oGroupName {
				return fmt.Errorf("group not renamed on %s, still %q", s, rg.Name)
			}
			// The group keeps a node while the desired state is pending, even at a single
			// peer, so the Raft term a newer server resumes the migration from survives.
			// Only the peers the group is actually at, we don't act on the desired set.
			if isOrigin := s.Node() == origin; isOrigin != (rg.node != nil) {
				if isOrigin {
					return fmt.Errorf("no raft node on origin %s while desired state is pending", s)
				}
				return fmt.Errorf("raft node created on %s, must not act on the desired peer set", s)
			}
		}
		return nil
	})

	// The group has a node now that a desired state is pending, so wait for it to have
	// settled on a leader before asking, or the answering peer is not suppressed below.
	c.waitOnStreamLeader(globalAccountName, "TEST")

	// Stream info must expose the desired state as well.
	var resp JSApiStreamInfoResponse
	msg, err := nc.Request(fmt.Sprintf(JSApiStreamInfoT, "TEST"), nil, time.Second)
	require_NoError(t, err)
	require_NoError(t, json.Unmarshal(msg.Data, &resp))
	require_True(t, resp.Error == nil)
	require_NotNil(t, resp.StreamInfo)

	ci := resp.StreamInfo.Cluster
	require_NotNil(t, ci)
	// Still only the origin peer, and it's the server answering, so it's not listed.
	require_Len(t, len(ci.Replicas), 0)
	require_NotNil(t, ci.Desired)
	require_Equal(t, ci.Desired.Name, desired.Cluster)
	require_True(t, ci.Desired.Created.Equal(desired.Created))
	require_NotNil(t, ci.Desired.Origin)
	require_Equal(t, ci.Desired.Origin.Replicas, 1)

	// The desired replicas are the full target set, named after the servers.
	var names []string
	for _, pi := range ci.Desired.Replicas {
		names = append(names, pi.Name)
	}
	slices.Sort(names)
	var expected []string
	for _, s := range c.servers {
		expected = append(expected, s.Name())
	}
	slices.Sort(expected)
	require_True(t, slices.Equal(names, expected))
}

func TestJetStreamClusterDesiredStateRetentionReportedInStreamInfo(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "R3S", 3)
	defer c.shutdown()

	nc, js := jsClientConnect(t, c.randomServer())
	defer nc.Close()

	_, err := js.AddStream(&nats.StreamConfig{
		Name:      "TEST",
		Subjects:  []string{"foo"},
		Replicas:  3,
		Retention: nats.LimitsPolicy,
	})
	require_NoError(t, err)

	ml := c.leader()
	mjs, cc := ml.getJetStreamCluster()

	// Build the assignment as a v2.15 meta leader would for a Limits->Interest
	// retention change: the config holds the requested target, while the origin
	// records what the stream must keep running at until the change is reached.
	mjs.mu.RLock()
	osa := mjs.streamAssignment(globalAccountName, "TEST")
	require_NotNil(t, osa)
	limits := LimitsPolicy
	nsa := osa.copyGroup()
	// Must not mutate the live assignment's config, it's shared by the clone.
	nsa.Config = osa.Config.clone()
	nsa.Config.Retention = InterestPolicy
	nsa.Group.Desired = &desiredRaftGroup{
		Created: time.Now().UTC(),
		ID:      "DESIRED",
		Peers:   copyStrings(osa.Group.Peers),
		Cluster: osa.Group.Cluster,
		Origin: &desiredRaftGroupOrigin{
			Peers:     copyStrings(osa.Group.Peers),
			Cluster:   osa.Group.Cluster,
			Replicas:  osa.Config.Replicas,
			Placement: osa.Config.Placement.clone(),
			Retention: &limits,
		},
	}
	mjs.mu.RUnlock()

	require_NoError(t, cc.meta.Propose(cc.term, encodeUpdateStreamAssignment(nsa)))

	// Every server must hold the requested retention on its assignment, while the
	// stream itself keeps running at the origin retention until the change is reached.
	checkFor(t, 5*time.Second, 100*time.Millisecond, func() error {
		for _, s := range c.servers {
			sjs := s.getJetStream()
			sjs.mu.RLock()
			sa := sjs.streamAssignment(globalAccountName, "TEST")
			var cfg *StreamConfig
			if sa != nil {
				cfg = sa.Config
			}
			sjs.mu.RUnlock()

			if sa == nil {
				return fmt.Errorf("no stream assignment on %s", s)
			}
			if cfg.Retention != InterestPolicy {
				return fmt.Errorf("assignment retention on %s, got %v, want %v", s, cfg.Retention, InterestPolicy)
			}
			mset, err := s.globalAccount().lookupStream("TEST")
			if err != nil {
				return err
			}
			if r := mset.config().Retention; r != LimitsPolicy {
				return fmt.Errorf("running retention on %s, got %v, want %v", s, r, LimitsPolicy)
			}
		}
		return nil
	})

	// Stream info must report the retention as requested, not the origin the
	// stream is still running at.
	var resp JSApiStreamInfoResponse
	msg, err := nc.Request(fmt.Sprintf(JSApiStreamInfoT, "TEST"), nil, time.Second)
	require_NoError(t, err)
	require_NoError(t, json.Unmarshal(msg.Data, &resp))
	require_True(t, resp.Error == nil)
	require_NotNil(t, resp.StreamInfo)
	require_Equal(t, resp.StreamInfo.Config.Retention, InterestPolicy)
	require_NotNil(t, resp.StreamInfo.Cluster)
	require_NotNil(t, resp.StreamInfo.Cluster.Desired)
	require_NotNil(t, resp.StreamInfo.Cluster.Desired.Origin)
	require_NotNil(t, resp.StreamInfo.Cluster.Desired.Origin.Retention)
	require_Equal(t, *resp.StreamInfo.Cluster.Desired.Origin.Retention, LimitsPolicy)

	// The same must hold for the monitoring endpoint, which reports the config
	// alongside the cluster info that already exposes the desired state.
	for _, s := range c.servers {
		jsz, err := s.Jsz(&JSzOptions{Accounts: true, Streams: true, Config: true})
		require_NoError(t, err)
		for _, ad := range jsz.AccountDetails {
			for _, sd := range ad.Streams {
				if sd.Name != "TEST" {
					continue
				}
				require_NotNil(t, sd.Config)
				require_Equal(t, sd.Config.Retention, InterestPolicy)
				require_NotNil(t, sd.Cluster)
				require_NotNil(t, sd.Cluster.Desired)
				require_NotNil(t, sd.Cluster.Desired.Origin)
				require_NotNil(t, sd.Cluster.Desired.Origin.Retention)
				require_Equal(t, *sd.Cluster.Desired.Origin.Retention, LimitsPolicy)
			}
		}
	}
}

// A v2.15 meta leader records the desired state on the stream first, its consumers
// are only mapped onto the new peer set later, as part of the migration. This server
// does not act on desired state, so both the stream and its consumers keep running at
// their origin, without a Raft node, until a v2.15 server drives the migration. They
// must not be reported unhealthy for that.
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
	acc := s.globalAccount()
	mset, err := acc.lookupStream("TEST")
	require_NoError(t, err)

	sjs := s.getJetStream()
	sjs.mu.RLock()
	sa := sjs.streamAssignment(globalAccountName, "TEST")
	ca := sjs.consumerAssignment(globalAccountName, "TEST", "CONSUMER")
	sjs.mu.RUnlock()
	require_NotNil(t, sa)
	require_NotNil(t, ca)

	// Not new assets anymore, so missing nodes get no startup grace.
	sjs.mu.Lock()
	sa.Created, ca.Created = time.Time{}, time.Time{}
	sjs.mu.Unlock()

	// Healthy to start with.
	require_NoError(t, sjs.isStreamHealthy(acc, sa))
	require_NoError(t, sjs.isConsumerHealthy(mset, sa, "CONSUMER", ca))

	// Record desired state to scale up to R3, as a v2.15 meta leader would: the config
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

	// Neither the stream nor the consumer moved: still on the origin peer, with no
	// Raft node, while the replica count they are judged by is the target.
	sjs.mu.RLock()
	require_Len(t, len(sa.Group.Peers), 1)
	require_True(t, sa.Group.node == nil)
	require_Len(t, len(ca.Group.Peers), 1)
	require_True(t, ca.Group.Desired == nil)
	require_True(t, ca.Group.node == nil)
	sjs.mu.RUnlock()
	rc, err := mset.lookupConsumer("CONSUMER").replica()
	require_NoError(t, err)
	require_Equal(t, rc, 3)

	// They must not be reported unhealthy for that.
	require_NoError(t, sjs.isStreamHealthy(acc, sa))
	require_NoError(t, sjs.isConsumerHealthy(mset, sa, "CONSUMER", ca))

	// The stream can scale up while the consumer remap stalls, which is the normal
	// case here: we don't drive the migration, so the consumer stays behind until a
	// v2.15 server maps it. The group is no longer R1, but the consumer still is.
	sjs.mu.Lock()
	sa.Group.Peers = copyStrings(peers)
	sjs.mu.Unlock()
	sjs.mu.RLock()
	require_Len(t, len(ca.Group.Peers), 1)
	require_True(t, ca.Group.node == nil)
	sjs.mu.RUnlock()
	require_NoError(t, sjs.isConsumerHealthy(mset, sa, "CONSUMER", ca))
	sjs.mu.Lock()
	sa.Group.Peers = copyStrings(sa.Group.Desired.Origin.Peers)
	sjs.mu.Unlock()

	// Without the desired state there is nothing explaining the missing node, and both
	// are reported. Confirms the checks above are not passing for some other reason.
	sjs.mu.Lock()
	desired := sa.Group.Desired
	sa.Group.Desired = nil
	sjs.mu.Unlock()
	require_Error(t, sjs.isStreamHealthy(acc, sa), errors.New("group node missing"))
	require_Error(t, sjs.isConsumerHealthy(mset, sa, "CONSUMER", ca), errors.New("group node missing"))
	sjs.mu.Lock()
	sa.Group.Desired = desired
	sjs.mu.Unlock()
	require_NoError(t, sjs.isStreamHealthy(acc, sa))
	require_NoError(t, sjs.isConsumerHealthy(mset, sa, "CONSUMER", ca))
}

// A stream left with desired state by a newer server must not stay frozen in between its
// origin and target. The user asks for the config they were already reported to have,
// which must not be a no-op: the scale up happens and the retention is released.
func TestJetStreamClusterDesiredStateResolvedOnStreamUpdate(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "R3S", 3)
	defer c.shutdown()

	nc, js := jsClientConnect(t, c.randomServer())
	defer nc.Close()

	_, err := js.AddStream(&nats.StreamConfig{
		Name:      "TEST",
		Subjects:  []string{"foo"},
		Replicas:  1,
		Retention: nats.LimitsPolicy,
	})
	require_NoError(t, err)
	// No explicit replica count, so it follows the stream.
	_, err = js.AddConsumer("TEST", &nats.ConsumerConfig{
		Durable:   "CONSUMER",
		AckPolicy: nats.AckExplicitPolicy,
	})
	require_NoError(t, err)

	for range 10 {
		_, err = js.Publish("foo", []byte("OK"))
		require_NoError(t, err)
	}

	ml := c.leader()
	mjs, cc := ml.getJetStreamCluster()

	// Record desired state as a newer meta leader would: the config holds the target
	// while the group and the stream stay at their origin.
	mjs.mu.RLock()
	osa := mjs.streamAssignment(globalAccountName, "TEST")
	require_NotNil(t, osa)
	require_Len(t, len(osa.Group.Peers), 1)
	origin, limits := osa.Group.Peers[0], LimitsPolicy
	peers := []string{origin}
	for _, s := range c.servers {
		if id := s.Node(); id != origin {
			peers = append(peers, id)
		}
	}
	nsa := osa.copyGroup()
	// Must not mutate the live assignment's config, it's shared by the clone.
	nsa.Config = osa.Config.clone()
	nsa.Config.Replicas, nsa.Config.Retention = 3, InterestPolicy
	nsa.Group.Name = groupNameForStream(peers, nsa.Group.Storage)
	nsa.Group.Desired = &desiredRaftGroup{
		Created: time.Now().UTC(),
		ID:      "DESIRED",
		Peers:   peers,
		Cluster: osa.Group.Cluster,
		Origin: &desiredRaftGroupOrigin{
			Peers:     copyStrings(osa.Group.Peers),
			Cluster:   osa.Group.Cluster,
			Replicas:  osa.Config.Replicas,
			Retention: &limits,
		},
	}
	mjs.mu.RUnlock()

	require_NoError(t, cc.meta.Propose(cc.term, encodeUpdateStreamAssignment(nsa)))

	// Confirm we did not act on it.
	checkFor(t, 5*time.Second, 100*time.Millisecond, func() error {
		for _, s := range c.servers {
			sjs := s.getJetStream()
			sjs.mu.RLock()
			sa := sjs.streamAssignment(globalAccountName, "TEST")
			var frozen bool
			if sa != nil && sa.Group != nil {
				frozen = sa.Group.Desired != nil && len(sa.Group.Peers) == 1
			}
			sjs.mu.RUnlock()
			if !frozen {
				return fmt.Errorf("desired state not pending on %s", s)
			}
			if mset, err := s.globalAccount().lookupStream("TEST"); err == nil {
				if r := mset.config().Retention; r != LimitsPolicy {
					return fmt.Errorf("running retention on %s, got %v", s, r)
				}
			}
		}
		return nil
	})

	// Ask for the config we're reported to have.
	si, err := js.UpdateStream(&nats.StreamConfig{
		Name:      "TEST",
		Subjects:  []string{"foo"},
		Replicas:  3,
		Retention: nats.InterestPolicy,
	})
	require_NoError(t, err)
	require_Equal(t, si.Config.Replicas, 3)

	checkFor(t, 10*time.Second, 200*time.Millisecond, func() error {
		for _, s := range c.servers {
			sjs := s.getJetStream()
			sjs.mu.RLock()
			sa := sjs.streamAssignment(globalAccountName, "TEST")
			ca := sjs.consumerAssignment(globalAccountName, "TEST", "CONSUMER")
			var nPeers, nConsumerPeers int
			var desired *desiredRaftGroup
			if sa != nil {
				nPeers, desired = len(sa.Group.Peers), sa.Group.Desired
			}
			if ca != nil {
				nConsumerPeers = len(ca.Group.Peers)
			}
			sjs.mu.RUnlock()

			if sa == nil {
				return fmt.Errorf("no stream assignment on %s", s)
			}
			if desired != nil {
				return fmt.Errorf("desired state not cleared on %s", s)
			}
			if nPeers != 3 {
				return fmt.Errorf("group peers on %s, got %d, want 3", s, nPeers)
			}
			// The consumers must have followed onto the new peer set.
			if nConsumerPeers != 3 {
				return fmt.Errorf("consumer peers on %s, got %d, want 3", s, nConsumerPeers)
			}
			// And the stream must be running there, at the released retention.
			acc := s.globalAccount()
			mset, err := acc.lookupStream("TEST")
			if err != nil {
				return fmt.Errorf("stream not on %s: %s", s, err)
			}
			if r := mset.config().Retention; r != InterestPolicy {
				return fmt.Errorf("running retention on %s, got %v", s, r)
			}
			if mset.raftNode() == nil {
				return fmt.Errorf("no raft node on %s", s)
			}
			if err = sjs.isStreamHealthy(acc, sa); err != nil {
				return err
			}
		}
		return nil
	})

	// Nothing is pending, so stream info must not report a desired state.
	c.waitOnStreamLeader(globalAccountName, "TEST")
	var resp JSApiStreamInfoResponse
	msg, err := nc.Request(fmt.Sprintf(JSApiStreamInfoT, "TEST"), nil, 2*time.Second)
	require_NoError(t, err)
	require_NoError(t, json.Unmarshal(msg.Data, &resp))
	require_NotNil(t, resp.StreamInfo)
	require_True(t, resp.StreamInfo.Cluster.Desired == nil)
	require_Equal(t, resp.StreamInfo.State.Msgs, 10)
}

// Same, for a move. The requested placement equals the config's, so it's only recognized
// as a move when compared against the placement the stream is still running at.
func TestJetStreamClusterDesiredStateMoveResolvedOnStreamUpdate(t *testing.T) {
	sc := createJetStreamSuperCluster(t, 3, 2)
	defer sc.shutdown()
	sc.waitOnPeerCount(6)

	nc, js := jsClientConnect(t, sc.randomServer())
	defer nc.Close()

	var servers []*Server
	for _, cl := range sc.clusters {
		servers = append(servers, cl.servers...)
	}

	_, err := js.AddStream(&nats.StreamConfig{
		Name:      "TEST",
		Subjects:  []string{"foo"},
		Replicas:  3,
		Placement: &nats.Placement{Cluster: "C1"},
	})
	require_NoError(t, err)

	for range 10 {
		_, err = js.Publish("foo", []byte("OK"))
		require_NoError(t, err)
	}

	ml := sc.leader()
	mjs, cc := ml.getJetStreamCluster()

	// Record desired state for a move to C2: the config holds the target placement while
	// the group stays on the peers in C1.
	mjs.mu.RLock()
	osa := mjs.streamAssignment(globalAccountName, "TEST")
	require_NotNil(t, osa)
	require_Len(t, len(osa.Group.Peers), 3)
	var desiredPeers []string
	for _, s := range sc.clusterForName("C2").servers {
		desiredPeers = append(desiredPeers, s.Node())
	}
	originPeers := copyStrings(osa.Group.Peers)
	nsa := osa.copyGroup()
	// Must not mutate the live assignment's config, it's shared by the clone.
	nsa.Config = osa.Config.clone()
	nsa.Config.Placement = &Placement{Cluster: "C2"}
	nsa.Group.Desired = &desiredRaftGroup{
		Created: time.Now().UTC(),
		ID:      "DESIRED",
		Peers:   desiredPeers,
		Cluster: "C2",
		Move:    true,
		Origin: &desiredRaftGroupOrigin{
			Peers:     originPeers,
			Cluster:   osa.Group.Cluster,
			Replicas:  osa.Config.Replicas,
			Placement: osa.Config.Placement.clone(),
		},
	}
	mjs.mu.RUnlock()

	require_NoError(t, cc.meta.Propose(cc.term, encodeUpdateStreamAssignment(nsa)))

	// Confirm the stream did not move.
	checkFor(t, 5*time.Second, 100*time.Millisecond, func() error {
		for _, s := range servers {
			sjs := s.getJetStream()
			sjs.mu.RLock()
			sa := sjs.streamAssignment(globalAccountName, "TEST")
			var frozen bool
			if sa != nil && sa.Group != nil {
				frozen = sa.Group.Desired != nil && slices.Equal(sa.Group.Peers, originPeers)
			}
			sjs.mu.RUnlock()
			if !frozen {
				return fmt.Errorf("desired move not pending on %s", s)
			}
		}
		return nil
	})

	// Ask for the placement we're reported to have.
	_, err = js.UpdateStream(&nats.StreamConfig{
		Name:      "TEST",
		Subjects:  []string{"foo"},
		Replicas:  3,
		Placement: &nats.Placement{Cluster: "C2"},
	})
	require_NoError(t, err)

	// The move must now be driven to completion.
	checkFor(t, 10*time.Second, 200*time.Millisecond, func() error {
		for _, s := range servers {
			sjs := s.getJetStream()
			sjs.mu.RLock()
			sa := sjs.streamAssignment(globalAccountName, "TEST")
			var peers []string
			var desired *desiredRaftGroup
			if sa != nil {
				peers, desired = sa.Group.Peers, sa.Group.Desired
			}
			sjs.mu.RUnlock()
			if sa == nil {
				return fmt.Errorf("no stream assignment on %s", s)
			}
			if desired != nil {
				return fmt.Errorf("desired state not cleared on %s", s)
			}
			if len(peers) != 3 {
				return fmt.Errorf("group peers on %s, got %d, want 3", s, len(peers))
			}
			for _, p := range peers {
				if !slices.Contains(desiredPeers, p) {
					return fmt.Errorf("peer %q on %s is not in C2", p, s)
				}
			}
		}
		return nil
	})

	// The data moved with it.
	sc.waitOnStreamLeader(globalAccountName, "TEST")
	si, err := js.StreamInfo("TEST")
	require_NoError(t, err)
	require_Equal(t, si.State.Msgs, 10)
	require_Equal(t, si.Cluster.Name, "C2")
}

func performStreamBackup(t *testing.T, nc *nats.Conn, streamName string) (StreamConfig, StreamState, []byte) {
	t.Helper()
	endpoint := fmt.Sprintf(JSApiStreamSnapshotT, streamName)
	inbox := nc.NewRespInbox()

	sub, err := nc.SubscribeSync(inbox)
	require_NoError(t, err)

	resp, err := nc.RequestMsg(&nats.Msg{
		Subject: endpoint,
		Data:    fmt.Appendf(nil, `{"deliver_subject": %q}`, inbox),
	}, 5*time.Second)
	require_NoError(t, err)

	var body struct {
		StreamState  `json:"state"`
		StreamConfig `json:"config"`
	}
	require_NoError(t, json.Unmarshal(resp.Data, &body))

	buf := bytes.NewBuffer(nil)
	for {
		msg, err := sub.NextMsg(5 * time.Second)
		require_NoError(t, err)
		if msg.Reply != _EMPTY_ {
			require_NoError(t, msg.Respond(nil))
		}
		if len(msg.Data) == 0 {
			break
		}
		_, err = buf.Write(msg.Data)
		require_NoError(t, err)
	}
	return body.StreamConfig, body.StreamState, buf.Bytes()
}
