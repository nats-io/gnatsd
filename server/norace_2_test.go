// Copyright 2018-2026 The NATS Authors
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

//go:build !race && !skip_no_race_tests && !skip_no_race_2_tests

package server

import (
	"bytes"
	"compress/flate"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math/rand/v2"
	"net"
	"os"
	"path/filepath"
	"reflect"
	"runtime"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	crand "crypto/rand"

	"github.com/nats-io/nats-server/v2/server/gsl"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nuid"
)

// IMPORTANT: Tests in this file are not executed when running with the -race flag.
//            The test name should be prefixed with TestNoRace so we can run only
//            those tests: go test -run=TestNoRace ...

func TestNoRaceJetStreamClusterLeafnodeConnectPerf(t *testing.T) {
	// Uncomment to run. Needs to be on a big machine. Do not want as part of Travis tests atm.
	skip(t)

	tmpl := strings.Replace(jsClusterAccountsTempl, "store_dir:", "domain: cloud, store_dir:", 1)
	c := createJetStreamCluster(t, tmpl, "CLOUD", _EMPTY_, 3, 18033, true)
	defer c.shutdown()

	nc, js := jsClientConnect(t, c.randomServer())
	defer nc.Close()

	_, err := js.AddStream(&nats.StreamConfig{
		Name:     "STATE",
		Subjects: []string{"STATE.GLOBAL.CELL1.*.>"},
		Replicas: 3,
	})
	require_NoError(t, err)

	tmpl = strings.Replace(jsClusterTemplWithSingleFleetLeafNode, "store_dir:", "domain: vehicle, store_dir:", 1)

	var vinSerial int
	genVIN := func() string {
		vinSerial++
		return fmt.Sprintf("7PDSGAALXNN%06d", vinSerial)
	}

	numVehicles := 500
	for i := 0; i < numVehicles; i++ {
		start := time.Now()
		vin := genVIN()
		ln := c.createLeafNodeWithTemplateNoSystemWithProto(vin, tmpl, "ws")
		nc, js := jsClientConnect(t, ln)
		_, err := js.AddStream(&nats.StreamConfig{
			Name:     "VEHICLE",
			Subjects: []string{"STATE.GLOBAL.LOCAL.>"},
			Sources: []*nats.StreamSource{{
				Name:          "STATE",
				FilterSubject: fmt.Sprintf("STATE.GLOBAL.CELL1.%s.>", vin),
				External: &nats.ExternalStream{
					APIPrefix:     "$JS.cloud.API",
					DeliverPrefix: fmt.Sprintf("DELIVER.STATE.GLOBAL.CELL1.%s", vin),
				},
			}},
		})
		require_NoError(t, err)
		// Create the sourced stream.
		checkLeafNodeConnectedCount(t, ln, 1)
		if elapsed := time.Since(start); elapsed > 2*time.Second {
			t.Fatalf("Took too long to create leafnode %d connection: %v", i+1, elapsed)
		}
		nc.Close()
	}
}

func TestNoRaceJetStreamClusterDifferentRTTInterestBasedStreamPreAck(t *testing.T) {
	tmpl := `
	listen: 127.0.0.1:-1
	server_name: %s
	jetstream: {max_mem_store: 256MB, max_file_store: 2GB, store_dir: '%s'}

	cluster {
		name: "F3"
		listen: 127.0.0.1:%d
		routes = [%s]
	}

	accounts {
		$SYS { users = [ { user: "admin", pass: "s3cr3t!" } ] }
	}
	`

	//  Route Ports
	//	"S1": 14622,
	//	"S2": 15622,
	//	"S3": 16622,

	// S2 (stream leader) will have a slow path to S1 (via proxy) and S3 (consumer leader) will have a fast path.

	// Do these in order, S1, S2 (proxy) then S3.
	c := &cluster{t: t, servers: make([]*Server, 3), opts: make([]*Options, 3), name: "F3"}

	// S1
	// The route connection to S2 must be through a slow proxy
	np12 := createNetProxy(10*time.Millisecond, 1024*1024*1024, 1024*1024*1024, "route://127.0.0.1:15622", true)
	routes := fmt.Sprintf("%s, route://127.0.0.1:16622", np12.routeURL())
	conf := fmt.Sprintf(tmpl, "S1", t.TempDir(), 14622, routes)
	c.servers[0], c.opts[0] = RunServerWithConfig(createConfFile(t, []byte(conf)))

	// S2
	// The route connection to S1 must be through a slow proxy
	np21 := createNetProxy(10*time.Millisecond, 1024*1024*1024, 1024*1024*1024, "route://127.0.0.1:14622", true)
	routes = fmt.Sprintf("%s, route://127.0.0.1:16622", np21.routeURL())
	conf = fmt.Sprintf(tmpl, "S2", t.TempDir(), 15622, routes)
	c.servers[1], c.opts[1] = RunServerWithConfig(createConfFile(t, []byte(conf)))

	// S3
	conf = fmt.Sprintf(tmpl, "S3", t.TempDir(), 16622, "route://127.0.0.1:14622, route://127.0.0.1:15622")
	c.servers[2], c.opts[2] = RunServerWithConfig(createConfFile(t, []byte(conf)))

	c.checkClusterFormed()
	c.waitOnClusterReady()
	defer c.shutdown()
	defer np12.stop()
	defer np21.stop()

	slow := c.servers[0] // Expecting pre-acks here.
	sl := c.servers[1]   // Stream leader, will publish here.
	cl := c.servers[2]   // Consumer leader, will consume & ack here.

	snc, sjs := jsClientConnect(t, sl)
	defer snc.Close()

	cnc, cjs := jsClientConnect(t, cl)
	defer cnc.Close()

	// Now create the stream.
	_, err := sjs.AddStream(&nats.StreamConfig{
		Name:      "EVENTS",
		Subjects:  []string{"EV.>"},
		Replicas:  3,
		Retention: nats.InterestPolicy,
	})
	require_NoError(t, err)

	// Make sure it's leader is on S2.
	checkFor(t, 20*time.Second, 200*time.Millisecond, func() error {
		c.waitOnStreamLeader(globalAccountName, "EVENTS")
		if s := c.streamLeader(globalAccountName, "EVENTS"); s != sl {
			s.JetStreamStepdownStream(globalAccountName, "EVENTS")
			return fmt.Errorf("Server %s is not stream leader yet", sl)
		}
		return nil
	})

	// Now create the consumer.
	_, err = sjs.AddConsumer("EVENTS", &nats.ConsumerConfig{
		Durable:        "C",
		AckPolicy:      nats.AckExplicitPolicy,
		DeliverSubject: "dx",
	})
	require_NoError(t, err)

	// Make sure the consumer leader is on S3.
	checkFor(t, 20*time.Second, 200*time.Millisecond, func() error {
		c.waitOnConsumerLeader(globalAccountName, "EVENTS", "C")
		if s := c.consumerLeader(globalAccountName, "EVENTS", "C"); s != cl {
			s.JetStreamStepdownConsumer(globalAccountName, "EVENTS", "C")
			return fmt.Errorf("Server %s is not consumer leader yet", sl)
		}
		return nil
	})

	_, err = cjs.Subscribe(_EMPTY_, func(msg *nats.Msg) {
		msg.Ack()
	}, nats.BindStream("EVENTS"), nats.Durable("C"), nats.ManualAck())
	require_NoError(t, err)

	// Publish directly on the stream leader to make it efficient.
	for i := 0; i < 1_000; i++ {
		_, err := sjs.PublishAsync("EV.PAID", []byte("ok"))
		require_NoError(t, err)
	}
	select {
	case <-sjs.PublishAsyncComplete():
	case <-time.After(5 * time.Second):
		t.Fatalf("Did not receive completion signal")
	}

	mset, err := slow.GlobalAccount().lookupStream("EVENTS")
	require_NoError(t, err)

	checkFor(t, 10*time.Second, 200*time.Millisecond, func() error {
		state := mset.state()
		if state.LastSeq != 1000 {
			return fmt.Errorf("Haven't received all messages yet (last seq %d)", state.LastSeq)
		}
		mset.mu.RLock()
		preAcks := mset.preAcks
		mset.mu.RUnlock()
		if preAcks == nil {
			return fmt.Errorf("Expected to have preAcks by now")
		}
		if state.Msgs == 0 {
			mset.mu.RLock()
			lp := len(mset.preAcks)
			mset.mu.RUnlock()
			if lp == 0 {
				return nil
			} else {
				t.Fatalf("Expected no preAcks with no msgs, but got %d", lp)
			}
		}
		return fmt.Errorf("Still have %d msgs left", state.Msgs)
	})

}

func TestNoRaceCheckAckFloorWithVeryLargeFirstSeqAndNewConsumers(t *testing.T) {
	s := RunBasicJetStreamServer(t)
	defer s.Shutdown()

	nc, _ := jsClientConnect(t, s)
	defer nc.Close()

	// Make sure to time bound here for the acksync call below.
	js, err := nc.JetStream(nats.MaxWait(200 * time.Millisecond))
	require_NoError(t, err)

	_, err = js.AddStream(&nats.StreamConfig{
		Name:      "TEST",
		Subjects:  []string{"wq-req"},
		Retention: nats.WorkQueuePolicy,
	})
	require_NoError(t, err)

	largeFirstSeq := uint64(1_200_000_000)
	err = js.PurgeStream("TEST", &nats.StreamPurgeRequest{Sequence: largeFirstSeq})
	require_NoError(t, err)
	si, err := js.StreamInfo("TEST")
	require_NoError(t, err)
	require_True(t, si.State.FirstSeq == largeFirstSeq)

	// Add a simple request to the stream.
	sendStreamMsg(t, nc, "wq-req", "HELP")

	sub, err := js.PullSubscribe("wq-req", "dlc")
	require_NoError(t, err)

	msgs, err := sub.Fetch(1)
	require_NoError(t, err)
	require_True(t, len(msgs) == 1)

	// The bug is around the checkAckFloor walking the sequences from current ackfloor
	// to the first sequence of the stream. We time bound the max wait with the js context
	// to 200ms. Since checkAckFloor is spinning and holding up processing of acks this will fail.
	// We will short circuit new consumers to fix this one.
	require_NoError(t, msgs[0].AckSync())

	// Now do again so we move past the new consumer with no ack floor situation.
	err = js.PurgeStream("TEST", &nats.StreamPurgeRequest{Sequence: 2 * largeFirstSeq})
	require_NoError(t, err)
	si, err = js.StreamInfo("TEST")
	require_NoError(t, err)
	require_True(t, si.State.FirstSeq == 2*largeFirstSeq)

	sendStreamMsg(t, nc, "wq-req", "MORE HELP")

	// We check this one directly for this use case.
	mset, err := s.GlobalAccount().lookupStream("TEST")
	require_NoError(t, err)
	o := mset.lookupConsumer("dlc")
	require_True(t, o != nil)

	// Purge will move the stream floor by default, so force into the situation where it is back to largeFirstSeq.
	// This will not trigger the new consumer logic, but will trigger a walk of the sequence space.
	// Fix will be to walk the lesser of the two linear spaces.
	o.mu.Lock()
	o.asflr = largeFirstSeq
	o.mu.Unlock()

	done := make(chan bool)
	go func() {
		o.checkAckFloor()
		done <- true
	}()

	select {
	case <-done:
		return
	case <-time.After(time.Second):
		t.Fatalf("Check ack floor taking too long!")
	}
}

func TestNoRaceReplicatedMirrorWithLargeStartingSequenceOverLeafnode(t *testing.T) {
	// Cluster B
	tmpl := strings.Replace(jsClusterTempl, "store_dir:", "domain: B, store_dir:", 1)
	c := createJetStreamCluster(t, tmpl, "B", _EMPTY_, 3, 22020, true)
	defer c.shutdown()

	// Cluster A
	// Domain is "A'
	lc := c.createLeafNodesWithStartPortAndDomain("A", 3, 22110, "A")
	defer lc.shutdown()

	lc.waitOnClusterReady()

	// Create a stream on B (HUB/CLOUD) and set its starting sequence very high.
	nc, js := jsClientConnect(t, c.randomServer())
	defer nc.Close()

	_, err := js.AddStream(&nats.StreamConfig{
		Name:     "TEST",
		Subjects: []string{"foo"},
		Replicas: 3,
	})
	require_NoError(t, err)

	err = js.PurgeStream("TEST", &nats.StreamPurgeRequest{Sequence: 1_000_000_000})
	require_NoError(t, err)

	// Send in a small amount of messages.
	for i := 0; i < 1000; i++ {
		sendStreamMsg(t, nc, "foo", "Hello")
	}

	si, err := js.StreamInfo("TEST")
	require_NoError(t, err)
	require_True(t, si.State.FirstSeq == 1_000_000_000)

	// Now try to create a replicated mirror on the leaf cluster.
	lnc, ljs := jsClientConnect(t, lc.randomServer())
	defer lnc.Close()

	_, err = ljs.AddStream(&nats.StreamConfig{
		Name: "TEST",
		Mirror: &nats.StreamSource{
			Name:   "TEST",
			Domain: "B",
		},
	})
	require_NoError(t, err)

	// Make sure we sync quickly.
	checkFor(t, time.Second, 200*time.Millisecond, func() error {
		si, err = ljs.StreamInfo("TEST")
		require_NoError(t, err)
		if si.State.Msgs == 1000 && si.State.FirstSeq == 1_000_000_000 {
			return nil
		}
		return fmt.Errorf("Mirror state not correct: %+v", si.State)
	})
}

func TestNoRaceBinaryStreamSnapshotEncodingBasic(t *testing.T) {
	s := RunBasicJetStreamServer(t)
	defer s.Shutdown()

	nc, js := jsClientConnect(t, s)
	defer nc.Close()

	_, err := js.AddStream(&nats.StreamConfig{
		Name:              "TEST",
		Subjects:          []string{"*"},
		MaxMsgsPerSubject: 1,
	})
	require_NoError(t, err)

	// Set first key
	sendStreamMsg(t, nc, "key:1", "hello")

	// Set Second key but keep updating it, causing a laggard pattern.
	value := bytes.Repeat([]byte("Z"), 8*1024)

	for i := 0; i <= 1000; i++ {
		_, err := js.PublishAsync("key:2", value)
		require_NoError(t, err)
	}
	select {
	case <-js.PublishAsyncComplete():
	case <-time.After(5 * time.Second):
		t.Fatalf("Did not receive completion signal")
	}

	// Now do more of swiss cheese style.
	for i := 3; i <= 1000; i++ {
		key := fmt.Sprintf("key:%d", i)
		_, err := js.PublishAsync(key, value)
		require_NoError(t, err)
		// Send it twice to create hole right behind it, like swiss cheese.
		_, err = js.PublishAsync(key, value)
		require_NoError(t, err)
	}
	select {
	case <-js.PublishAsyncComplete():
	case <-time.After(5 * time.Second):
		t.Fatalf("Did not receive completion signal")
	}

	// Make for round numbers for stream state.
	sendStreamMsg(t, nc, "key:2", "hello")
	sendStreamMsg(t, nc, "key:2", "world")

	si, err := js.StreamInfo("TEST")
	require_NoError(t, err)
	require_True(t, si.State.FirstSeq == 1)
	require_True(t, si.State.LastSeq == 3000)
	require_True(t, si.State.Msgs == 1000)
	require_True(t, si.State.NumDeleted == 2000)

	mset, err := s.GlobalAccount().lookupStream("TEST")
	require_NoError(t, err)

	snap, err := mset.store.EncodedStreamState(0, false)
	require_NoError(t, err)

	// Now decode the snapshot.
	ss, err := DecodeStreamState(snap)
	require_NoError(t, err)

	require_Equal(t, ss.FirstSeq, 1)
	require_Equal(t, ss.LastSeq, 3000)
	require_Equal(t, ss.Msgs, 1000)
	require_Equal(t, ss.Deleted.NumDeleted(), 2000)
}

func TestNoRaceFilestoreBinaryStreamSnapshotEncodingLargeGaps(t *testing.T) {
	storeDir := t.TempDir()
	fcfg := FileStoreConfig{
		StoreDir:  storeDir,
		BlockSize: 512, // Small on purpose to create a lot of blks.
	}
	fs, err := newFileStore(fcfg, StreamConfig{Name: "zzz", Subjects: []string{"zzz"}, Storage: FileStorage})
	require_NoError(t, err)
	defer fs.Stop()

	subj, msg := "zzz", bytes.Repeat([]byte("X"), 128)
	numMsgs := 20_000

	fs.StoreMsg(subj, nil, msg, 0)
	for i := 2; i < numMsgs; i++ {
		seq, _, err := fs.StoreMsg(subj, nil, nil, 0)
		require_NoError(t, err)
		fs.RemoveMsg(seq)
	}
	fs.StoreMsg(subj, nil, msg, 0)

	// The tombstones from above will only be cleaned up when syncing blocks.
	fs.syncBlocks()

	snap, err := fs.EncodedStreamState(0, false)
	require_NoError(t, err)
	require_LessThan(t, len(snap), 512)

	// Now decode the snapshot.
	ss, err := DecodeStreamState(snap)
	require_NoError(t, err)

	require_Equal(t, ss.FirstSeq, 1)
	require_Equal(t, ss.LastSeq, 20_000)
	require_Equal(t, ss.Msgs, 2)
	require_Equal(t, len(ss.Deleted), 1)
	require_Equal(t, ss.Deleted.NumDeleted(), 19_998)
}

func TestNoRaceJetStreamClusterStreamSnapshotCatchup(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "R3S", 3)
	defer c.shutdown()

	// Client based API
	nc, js := jsClientConnect(t, c.randomServer())
	defer nc.Close()

	_, err := js.AddStream(&nats.StreamConfig{
		Name:              "TEST",
		Subjects:          []string{"*"},
		MaxMsgsPerSubject: 1,
		Replicas:          3,
	})
	require_NoError(t, err)

	msg := []byte("Hello World")
	_, err = js.Publish("foo", msg)
	require_NoError(t, err)

	for i := 1; i < 1000; i++ {
		_, err := js.PublishAsync("bar", msg)
		require_NoError(t, err)
	}
	select {
	case <-js.PublishAsyncComplete():
	case <-time.After(5 * time.Second):
		t.Fatalf("Did not receive completion signal")
	}

	sr := c.randomNonStreamLeader(globalAccountName, "TEST")
	sr.Shutdown()

	// In case we were connected to sr.
	nc, js = jsClientConnect(t, c.randomServer())
	defer nc.Close()

	// Now create a large gap.
	for i := 0; i < 50_000; i++ {
		_, err := js.PublishAsync("bar", msg)
		require_NoError(t, err)
	}
	select {
	case <-js.PublishAsyncComplete():
	case <-time.After(10 * time.Second):
		t.Fatalf("Did not receive completion signal")
	}

	sl := c.streamLeader(globalAccountName, "TEST")
	sl.JetStreamSnapshotStream(globalAccountName, "TEST")

	sr = c.restartServer(sr)
	c.checkClusterFormed()
	c.waitOnServerCurrent(sr)
	c.waitOnStreamCurrent(sr, globalAccountName, "TEST")

	// Current can be reported before the leader's snapshot catchup has started, so wait for the state.
	checkCaughtUp := func(msgs, lseq uint64) {
		t.Helper()
		mset, err := sr.GlobalAccount().lookupStream("TEST")
		require_NoError(t, err)
		checkFor(t, 10*time.Second, 100*time.Millisecond, func() error {
			var state StreamState
			mset.store.FastState(&state)
			if state.Msgs != msgs || state.FirstSeq != 1 || state.LastSeq != lseq || state.NumDeleted != int(lseq-msgs) {
				return fmt.Errorf("not caught up: %+v", state)
			}
			return nil
		})
	}

	// Make sure it's caught up
	checkCaughtUp(2, 51_000)

	sr.Shutdown()

	_, err = js.Publish("baz", msg)
	require_NoError(t, err)

	sl.JetStreamSnapshotStream(globalAccountName, "TEST")

	sr = c.restartServer(sr)
	c.checkClusterFormed()
	c.waitOnServerCurrent(sr)
	c.waitOnStreamCurrent(sr, globalAccountName, "TEST")
	checkCaughtUp(3, 51_001)
}

func TestNoRaceStoreStreamEncoderDecoder(t *testing.T) {
	cfg := &StreamConfig{
		Name:       "zzz",
		Subjects:   []string{"*"},
		MaxMsgsPer: 1,
		Storage:    MemoryStorage,
	}
	ms, err := newMemStore(cfg)
	require_NoError(t, err)
	defer ms.Stop()

	fs, err := newFileStore(
		FileStoreConfig{StoreDir: t.TempDir()},
		StreamConfig{Name: "zzz", Subjects: []string{"*"}, MaxMsgsPer: 1, Storage: FileStorage},
	)
	require_NoError(t, err)
	defer fs.Stop()

	const seed = 2222222
	msg := bytes.Repeat([]byte("ABC"), 33) // ~100bytes

	maxEncodeTime := 2 * time.Second
	maxEncodeSize := 700 * 1024

	test := func(t *testing.T, gs StreamStore) {
		t.Parallel()
		prand := rand.New(rand.NewPCG(uint64(seed), 0))
		tick := time.NewTicker(time.Second)
		defer tick.Stop()
		done := time.NewTimer(10 * time.Second)

		for running := true; running; {
			select {
			case <-tick.C:
				var state StreamState
				gs.FastState(&state)
				if state.NumDeleted == 0 {
					continue
				}
				start := time.Now()
				snap, err := gs.EncodedStreamState(0, false)
				require_NoError(t, err)
				elapsed := time.Since(start)
				// Should take <1ms without race but if CI/CD is slow we will give it a bit of room.
				if elapsed > maxEncodeTime {
					t.Logf("Encode took longer then expected: %v", elapsed)
				}
				if len(snap) > maxEncodeSize {
					t.Fatalf("Expected snapshot size < %v got %v", friendlyBytes(maxEncodeSize), friendlyBytes(len(snap)))
				}
				ss, err := DecodeStreamState(snap)
				require_True(t, len(ss.Deleted) > 0)
				require_NoError(t, err)
			case <-done.C:
				running = false
			default:
				key := strconv.Itoa(prand.IntN(256_000))
				gs.StoreMsg(key, nil, msg, 0)
			}
		}
	}

	for _, gs := range []StreamStore{ms, fs} {
		switch gs.(type) {
		case *memStore:
			t.Run("MemStore", func(t *testing.T) {
				test(t, gs)
			})
		case *fileStore:
			t.Run("FileStore", func(t *testing.T) {
				test(t, gs)
			})
		}
	}
}

func TestNoRaceJetStreamClusterKVWithServerKill(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "R3S", 3)
	defer c.shutdown()

	// Setup the KV bucket and use for making assertions.
	nc, js := jsClientConnect(t, c.randomServer())
	defer nc.Close()
	_, err := js.CreateKeyValue(&nats.KeyValueConfig{
		Bucket:   "TEST",
		Replicas: 3,
		History:  10,
	})
	require_NoError(t, err)

	// Total number of keys to range over.
	numKeys := 50

	// ID is the server id to explicitly connect to.
	work := func(ctx context.Context, wg *sync.WaitGroup, id int) {
		defer wg.Done()

		nc, js := jsClientConnectEx(t, c.servers[id], []nats.JSOpt{nats.Context(ctx)})
		defer nc.Close()

		kv, err := js.KeyValue("TEST")
		require_NoError(t, err)

		// 100 messages a second for each single client.
		tk := time.NewTicker(10 * time.Millisecond)
		defer tk.Stop()

		for {
			select {
			case <-ctx.Done():
				return

			case <-tk.C:
				// Pick a random key within the range.
				k := fmt.Sprintf("key.%d", rand.IntN(numKeys))
				// Attempt to get a key.
				e, err := kv.Get(k)
				// If found, attempt to update or delete.
				if err == nil {
					if rand.IntN(10) < 3 {
						kv.Delete(k, nats.LastRevision(e.Revision()))
					} else {
						kv.Update(k, nil, e.Revision())
					}
				} else if errors.Is(err, nats.ErrKeyNotFound) {
					kv.Create(k, nil)
				}
			}
		}
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var wg sync.WaitGroup
	wg.Add(3)

	go work(ctx, &wg, 0)
	go work(ctx, &wg, 1)
	go work(ctx, &wg, 2)

	time.Sleep(time.Second)

	// Simulate server stop and restart.
	for i := 0; i < 7; i++ {
		s := c.randomServer()
		s.Shutdown()
		c.waitOnLeader()
		c.waitOnStreamLeader(globalAccountName, "KV_TEST")

		// Wait for a bit and then start the server again.
		time.Sleep(time.Duration(rand.IntN(1250)) * time.Millisecond)
		s = c.restartServer(s)
		c.waitOnServerCurrent(s)
		c.waitOnLeader()
		c.waitOnStreamLeader(globalAccountName, "KV_TEST")
		c.waitOnPeerCount(3)
	}

	// Stop the workload.
	cancel()
	wg.Wait()

	type fullState struct {
		state StreamState
		lseq  uint64
		clfs  uint64
	}

	grabState := func(mset *stream) *fullState {
		mset.mu.RLock()
		defer mset.mu.RUnlock()
		var state StreamState
		mset.store.FastState(&state)
		return &fullState{state, mset.lseq, mset.clfs}
	}

	grabStore := func(mset *stream) map[string][]uint64 {
		mset.mu.RLock()
		store := mset.store
		mset.mu.RUnlock()
		var state StreamState
		store.FastState(&state)
		storeMap := make(map[string][]uint64)
		for seq := state.FirstSeq; seq <= state.LastSeq; seq++ {
			if sm, err := store.LoadMsg(seq, nil); err == nil {
				storeMap[sm.subj] = append(storeMap[sm.subj], sm.seq)
			}
		}
		return storeMap
	}

	checkFor(t, 10*time.Second, 500*time.Millisecond, func() error {
		// Current stream leader.
		sl := c.streamLeader(globalAccountName, "KV_TEST")
		mset, err := sl.GlobalAccount().lookupStream("KV_TEST")
		require_NoError(t, err)
		lstate := grabState(mset)
		golden := grabStore(mset)

		// Report messages per server.
		for _, s := range c.servers {
			if s == sl {
				continue
			}
			mset, err := s.GlobalAccount().lookupStream("KV_TEST")
			require_NoError(t, err)
			state := grabState(mset)
			if !reflect.DeepEqual(state, lstate) {
				return fmt.Errorf("Expected follower state\n%+v\nto match leader's\n %+v", state, lstate)
			}
			sm := grabStore(mset)
			if !reflect.DeepEqual(sm, golden) {
				t.Fatalf("Expected follower store for %v\n%+v\nto match leader's %v\n %+v", s, sm, sl, golden)
			}
		}
		return nil
	})
}

func TestNoRaceFileStoreLargeMsgsAndFirstMatching(t *testing.T) {
	sd := t.TempDir()
	fs, err := newFileStore(
		FileStoreConfig{StoreDir: sd, BlockSize: 8 * 1024 * 1024},
		StreamConfig{Name: "zzz", Subjects: []string{">"}, Storage: FileStorage})
	require_NoError(t, err)
	defer fs.Stop()

	for i := 0; i < 150_000; i++ {
		fs.StoreMsg(fmt.Sprintf("foo.bar.%d", i), nil, nil, 0)
	}
	for i := 0; i < 150_000; i++ {
		fs.StoreMsg(fmt.Sprintf("foo.baz.%d", i), nil, nil, 0)
	}
	require_Equal(t, fs.numMsgBlocks(), 2)
	fs.mu.RLock()
	mb := fs.blks[1]
	fs.mu.RUnlock()
	fseq := atomic.LoadUint64(&mb.first.seq)
	// The -40 leaves enough mb.fss entries to kick in linear scan.
	for seq := fseq; seq < 300_000-40; seq++ {
		fs.RemoveMsg(uint64(seq))
	}
	start := time.Now()
	fs.LoadNextMsg("*.baz.*", true, fseq, nil)
	require_True(t, time.Since(start) < 200*time.Microsecond)
	// Now remove more to kick into non-linear logic.
	for seq := 300_000 - 40; seq < 300_000; seq++ {
		fs.RemoveMsg(uint64(seq))
	}
	start = time.Now()
	fs.LoadNextMsg("*.baz.*", true, fseq, nil)
	require_True(t, time.Since(start) < 200*time.Microsecond)
}

func TestNoRaceWSNoCorruptionWithFrameSizeLimit(t *testing.T) {
	testWSNoCorruptionWithFrameSizeLimit(t, 50000)
}

func TestNoRaceWSCompressionGrownBufferRecycling(t *testing.T) {
	opts := testWSOptions()
	opts.MaxPending = MAX_PENDING_SIZE
	s := &Server{opts: opts}
	c := &client{srv: s, ws: &websocket{compress: true}}
	c.initClient()

	// Incompressible payload filling the medium pool bucket exactly, so
	// that the flate output (payload + block overhead) is guaranteed to
	// outgrow the pooled buffer seeding the compression bytes.Buffer.
	payload := make([]byte, nbPoolSizeMedium)
	rnd := uint32(1)
	for i := range payload {
		rnd = rnd*1664525 + 1013904223
		payload[i] = byte(rnd >> 24)
	}

	// Confirm the payload no longer fits the bucket once compressed.
	var cb bytes.Buffer
	cw, _ := flate.NewWriter(&cb, flate.BestSpeed)
	cw.Write(payload)
	cw.Flush()
	if cb.Len() <= nbPoolSizeMedium {
		t.Fatalf("Expected compressed payload to exceed %v bytes, got %v", nbPoolSizeMedium, cb.Len())
	}

	nbSlice := make(net.Buffers, 1)
	collapse := func() {
		c.mu.Lock()
		data := nbPoolGet(len(payload))
		data = append(data, payload...)
		nbSlice[0] = data
		c.out.nb = nbSlice
		c.out.pb = int64(len(payload))
		c.ws.fs = 0

		// calls c.wsCollapsePtoNB internally.
		bufs, _ := c.collapsePtoNB()
		for _, buf := range bufs {
			nbPoolPut(buf)
		}
		c.out.nb = nil
		c.mu.Unlock()
	}
	// Populate the pool and initialize the compressor.
	for i := 0; i < 10; i++ {
		collapse()
	}

	// Growing the bytes.Buffer beyond the seeded pool buffer costs a few
	// allocations per iteration but the seed itself must be returned to the pool.
	allocs := testing.AllocsPerRun(500, collapse)
	if allocs > 3 {
		t.Fatalf("Too many allocs per iteration (%.1f); the compression seed buffer is likely not recycled", allocs)
	}
}

func TestNoRaceJetStreamAPIDispatchQueuePending(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "R3S", 3)
	defer c.shutdown()

	// Setup the KV bucket and use for making assertions.
	nc, js := jsClientConnect(t, c.randomServer())
	defer nc.Close()

	_, err := js.AddStream(&nats.StreamConfig{
		Name:     "TEST",
		Subjects: []string{"foo.*.*"},
	})
	require_NoError(t, err)

	// Queue up 500k messages all with different subjects.
	// We want to make num pending for a consumer expensive, so a large subject
	// space and wildcards for now does the trick.
	toks := []string{"foo", "bar", "baz"} // for second token.
	for i := 1; i <= 500_000; i++ {
		subj := fmt.Sprintf("foo.%s.%d", toks[rand.IntN(len(toks))], i)
		_, err := js.PublishAsync(subj, nil, nats.StallWait(time.Second))
		require_NoError(t, err)
	}
	select {
	case <-js.PublishAsyncComplete():
	case <-time.After(20 * time.Second):
		t.Fatalf("Did not receive completion signal")
	}

	// To back up our pending queue we will create lots of filtered, with wildcards, R1 consumers
	// from a different server then the one hosting the stream.
	// ok to share this connection here.
	sldr := c.streamLeader(globalAccountName, "TEST")
	for _, s := range c.servers {
		if s != sldr {
			nc, js = jsClientConnect(t, s)
			defer nc.Close()
			break
		}
	}

	ngr, ncons := 100, 10
	startCh, errCh := make(chan bool), make(chan error, ngr)
	var wg, swg sync.WaitGroup
	wg.Add(ngr)
	swg.Add(ngr)

	// The wildcard in the filter subject is the key.
	cfg := &nats.ConsumerConfig{FilterSubject: "foo.*.22"}
	var tt atomic.Int64

	for i := 0; i < ngr; i++ {
		go func() {
			defer wg.Done()
			swg.Done()
			// Make them all fire at once.
			<-startCh

			for i := 0; i < ncons; i++ {
				start := time.Now()
				if _, err := js.AddConsumer("TEST", cfg); err != nil {
					errCh <- err
					t.Logf("Got err creating consumer: %v", err)
				}
				elapsed := time.Since(start)
				tt.Add(int64(elapsed))
			}
		}()
	}
	swg.Wait()
	close(startCh)
	time.Sleep(time.Millisecond)
	jsz, _ := sldr.Jsz(nil)
	// This could be 0 legit, so just log, don't fail.
	if jsz.JetStreamStats.API.Inflight == 0 {
		t.Log("Expected a non-zero inflight")
	}
	wg.Wait()

	if len(errCh) > 0 {
		t.Fatalf("Expected no errors, got %d: %v", len(errCh), <-errCh)
	}
}

func TestNoRaceJetStreamMirrorAndSourceConsumerFailBackoff(t *testing.T) {
	owt := srcConsumerWaitTime
	srcConsumerWaitTime = 2 * time.Second
	defer func() { srcConsumerWaitTime = owt }()

	// Check calculations first.
	for i := 1; i <= 20; i++ {
		backoff := calculateRetryBackoff(i)
		if i < 12 {
			require_Equal(t, backoff, time.Duration(i)*10*time.Second)
		} else {
			require_Equal(t, backoff, retryMaximum)
		}
	}

	c := createJetStreamClusterExplicit(t, "R3S", 3)
	defer c.shutdown()

	nc, js := jsClientConnect(t, c.randomServer())
	defer nc.Close()

	_, err := js.AddStream(&nats.StreamConfig{
		Name:     "TEST",
		Subjects: []string{"foo.*.*"},
	})
	require_NoError(t, err)
	sl := c.streamLeader(globalAccountName, "TEST")

	// Create a mirror.
	ml := sl
	// Make sure not on the same server. Should not happened in general but possible.
	for ml == sl {
		js.DeleteStream("MIRROR")
		_, err = js.AddStream(&nats.StreamConfig{
			Name:   "MIRROR",
			Mirror: &nats.StreamSource{Name: "TEST"},
		})
		require_NoError(t, err)
		ml = c.streamLeader(globalAccountName, "MIRROR")
	}
	// Create a source.
	srcl := sl
	for srcl == sl {
		js.DeleteStream("SOURCE")
		_, err = js.AddStream(&nats.StreamConfig{
			Name:    "SOURCE",
			Sources: []*nats.StreamSource{{Name: "TEST"}},
		})
		require_NoError(t, err)
		srcl = c.streamLeader(globalAccountName, "MIRROR")
	}

	// Create sub to watch for the consumer create requests.
	nc, _ = jsClientConnect(t, ml)
	defer nc.Close()
	sub := natsSubSync(t, nc, "$JS.API.CONSUMER.CREATE.>")

	// Kill the server where the source is..
	sldr := c.streamLeader(globalAccountName, "TEST")
	sldr.Shutdown()

	// Wait for just greater than 5s. We should only see 1 request during this time.
	time.Sleep(6 * time.Second)
	// There should have been 2 requests, one for mirror, one for source
	n, _, _ := sub.Pending()
	require_Equal(t, n, 2)
	var mreq, sreq int
	for i := 0; i < 2; i++ {
		msg := natsNexMsg(t, sub, time.Second)
		if bytes.Contains(msg.Data, []byte("$JS.M.")) {
			mreq++
		} else if bytes.Contains(msg.Data, []byte("$JS.S.")) {
			sreq++
		}
	}
	if mreq != 1 || sreq != 1 {
		t.Fatalf("Consumer create captures invalid: mreq=%v sreq=%v", mreq, sreq)
	}

	// Now make sure that the fails is set properly.
	mset, err := c.streamLeader(globalAccountName, "MIRROR").GlobalAccount().lookupStream("MIRROR")
	require_NoError(t, err)
	mset.mu.RLock()
	fails := mset.mirror.fails
	mset.mu.RUnlock()
	require_Equal(t, fails, 1)

	mset, err = c.streamLeader(globalAccountName, "SOURCE").GlobalAccount().lookupStream("SOURCE")
	require_NoError(t, err)
	mset.mu.RLock()
	si := mset.sources["TEST > >"]
	mset.mu.RUnlock()
	require_True(t, si != nil)
	require_Equal(t, si.fails, 1)
}

func TestNoRaceJetStreamClusterStreamCatchupLargeInteriorDeletes(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "R3S", 3)
	defer c.shutdown()

	nc, js := jsClientConnect(t, c.randomServer())
	defer nc.Close()

	cfg := &nats.StreamConfig{
		Name:              "TEST",
		Subjects:          []string{"foo.*"},
		MaxMsgsPerSubject: 100,
		Replicas:          1,
	}

	_, err := js.AddStream(cfg)
	require_NoError(t, err)

	msg := bytes.Repeat([]byte("Z"), 2*1024)
	// We will create lots of interior deletes on our R1 then scale up.
	_, err = js.Publish("foo.0", msg)
	require_NoError(t, err)

	// Create 50k messages randomly from 1-100
	for i := 0; i < 50_000; i++ {
		subj := fmt.Sprintf("foo.%d", rand.IntN(100)+1)
		publishAsync(t, js, subj, msg)
	}
	select {
	case <-js.PublishAsyncComplete():
	case <-time.After(5 * time.Second):
		t.Fatalf("Did not receive completion signal")
	}
	// Now create a large gap.
	for i := 0; i < 100_000; i++ {
		publishAsync(t, js, "foo.2", msg)
	}
	select {
	case <-js.PublishAsyncComplete():
	case <-time.After(5 * time.Second):
		t.Fatalf("Did not receive completion signal")
	}
	// Do 50k random again at end.
	for i := 0; i < 50_000; i++ {
		subj := fmt.Sprintf("foo.%d", rand.IntN(100)+1)
		publishAsync(t, js, subj, msg)
	}
	select {
	case <-js.PublishAsyncComplete():
	case <-time.After(5 * time.Second):
		t.Fatalf("Did not receive completion signal")
	}

	si, err := js.StreamInfo("TEST")
	require_NoError(t, err)

	cfg.Replicas = 2
	_, err = js.UpdateStream(cfg)
	require_NoError(t, err)

	// Let catchup start.
	c.waitOnStreamLeader(globalAccountName, "TEST")

	nl := c.randomNonStreamLeader(globalAccountName, "TEST")
	require_True(t, nl != nil)
	mset, err := nl.GlobalAccount().lookupStream("TEST")
	require_NoError(t, err)

	checkFor(t, 10*time.Second, 500*time.Millisecond, func() error {
		state := mset.state()
		if state.Msgs == si.State.Msgs {
			return nil
		}
		return fmt.Errorf("Msgs not equal %d vs %d", state.Msgs, si.State.Msgs)
	})
}

func TestNoRaceJetStreamClusterBadRestartsWithHealthzPolling(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "R3S", 3)
	defer c.shutdown()

	nc, js := jsClientConnect(t, c.randomServer())
	defer nc.Close()

	cfg := &nats.StreamConfig{
		Name:     "TEST",
		Subjects: []string{"foo.>"},
		Replicas: 3,
	}
	_, err := js.AddStream(cfg)
	require_NoError(t, err)

	// We will poll healthz at a decent clip and make sure any restart logic works
	// correctly with assets coming and going.
	ch := make(chan struct{})
	defer close(ch)

	go func() {
		for {
			select {
			case <-ch:
				return
			case <-time.After(50 * time.Millisecond):
				for _, s := range c.servers {
					s.healthz(nil)
				}
			}
		}
	}()

	numConsumers := 500
	consumers := make([]string, 0, numConsumers)

	var wg sync.WaitGroup

	// Cap inflight consumer create requests to prevent timeout on slower CI machines.
	inflight := make(chan struct{}, 50)

	for i := 0; i < numConsumers; i++ {
		cname := fmt.Sprintf("CONS-%d", i+1)
		consumers = append(consumers, cname)
		wg.Add(1)
		inflight <- struct{}{}
		go func() {
			defer wg.Done()
			defer func() { <-inflight }()
			_, err := js.PullSubscribe("foo.>", cname, nats.BindStream("TEST"))
			require_NoError(t, err)
		}()
	}
	wg.Wait()

	// Make sure all are reported.
	c.waitOnAllCurrent()
	checkFor(t, 5*time.Second, 100*time.Millisecond, func() error {
		for _, s := range c.servers {
			jsz, _ := s.Jsz(nil)
			if jsz.Consumers != numConsumers {
				return fmt.Errorf("%v wrong number of consumers: %d vs %d", s, jsz.Consumers, numConsumers)
			}
		}
		return nil
	})

	// Now do same for streams.
	numStreams := 200
	streams := make([]string, 0, numStreams)

	// Cap number of inflight stream request creates.
	sinflight := make(chan struct{}, 25)

	for i := 0; i < numStreams; i++ {
		sname := fmt.Sprintf("TEST-%d", i+1)
		streams = append(streams, sname)
		wg.Add(1)
		sinflight <- struct{}{}
		go func() {
			defer wg.Done()
			defer func() { <-sinflight }()
			_, err := js.AddStream(&nats.StreamConfig{Name: sname, Replicas: 3})
			require_NoError(t, err)
		}()
	}
	wg.Wait()

	// Make sure all are reported.
	c.waitOnAllCurrent()
	checkFor(t, 5*time.Second, 100*time.Millisecond, func() error {
		for _, s := range c.servers {
			jsz, _ := s.Jsz(nil)
			if jsz.Streams != numStreams+1 {
				return fmt.Errorf("%v wrong number of streams: %d vs %d", s, jsz.Streams, numStreams+1)
			}
		}
		return nil
	})

	// Delete consumers.
	for _, cname := range consumers {
		err := js.DeleteConsumer("TEST", cname)
		require_NoError(t, err)
	}
	// Make sure reporting goes to zero.
	c.waitOnAllCurrent()
	checkFor(t, 5*time.Second, 100*time.Millisecond, func() error {
		for _, s := range c.servers {
			jsz, _ := s.Jsz(nil)
			if jsz.Consumers != 0 {
				return fmt.Errorf("%v still has %d consumers", s, jsz.Consumers)
			}
		}
		return nil
	})

	// Delete streams
	for _, sname := range streams {
		err := js.DeleteStream(sname)
		require_NoError(t, err)
	}
	err = js.DeleteStream("TEST")
	require_NoError(t, err)

	// Make sure reporting goes to zero.
	c.waitOnAllCurrent()
	checkFor(t, 5*time.Second, 100*time.Millisecond, func() error {
		for _, s := range c.servers {
			jsz, _ := s.Jsz(nil)
			if jsz.Streams != 0 {
				return fmt.Errorf("%v still has %d streams", s, jsz.Streams)
			}
		}
		return nil
	})
}

func TestNoRaceJetStreamKVReplaceWithServerRestart(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "R3S", 3)
	defer c.shutdown()

	nc, _ := jsClientConnect(t, c.randomServer())
	defer nc.Close()
	// Shorten wait time for disconnects.
	js, err := nc.JetStream(nats.MaxWait(time.Second))
	require_NoError(t, err)

	kv, err := js.CreateKeyValue(&nats.KeyValueConfig{
		Bucket:   "TEST",
		Replicas: 3,
	})
	require_NoError(t, err)

	// Manually disable direct get on underlying stream, since this test
	// relies on immediate consistency which we can't guarantee with direct gets
	// until we provide a solution for this.
	si, err := js.StreamInfo("KV_TEST")
	require_NoError(t, err)
	si.Config.AllowDirect = false
	_, err = js.UpdateStream(&si.Config)
	require_NoError(t, err)

	createData := func(n int) []byte {
		const letterBytes = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
		b := make([]byte, n)
		for i := range b {
			b[i] = letterBytes[rand.IntN(len(letterBytes))]
		}
		return b
	}

	_, err = kv.Create("foo", createData(160))
	require_NoError(t, err)

	// Ensure all replicas have applied the key.
	checkFor(t, 2*time.Second, 200*time.Millisecond, func() error {
		return checkState(t, c, globalAccountName, "KV_TEST")
	})

	ch := make(chan struct{})
	wg := sync.WaitGroup{}

	// For counting errors that should not happen.
	errCh := make(chan error, 1024)

	wg.Add(1)
	go func() {
		defer wg.Done()

		var lastData []byte
		var revision uint64

		for {
			select {
			case <-ch:
				close(errCh)
				return
			default:
				k, err := kv.Get("foo")
				if err == nats.ErrKeyNotFound {
					errCh <- err
				} else if k != nil {
					if lastData != nil && k.Revision() == revision && !bytes.Equal(lastData, k.Value()) {
						errCh <- fmt.Errorf("data loss [%s][rev:%d] expected:[%q] is:[%q]\n", "foo", revision, lastData, k.Value())
					}
					newData := createData(160)
					if revision, err = kv.Update("foo", newData, k.Revision()); err == nil {
						lastData = newData
					}
				}
			}
		}
	}()

	// Wait a short bit.
	time.Sleep(2 * time.Second)
	for _, s := range c.servers {
		s.Shutdown()
		// Need to leave servers down for a while to trigger bug properly.
		time.Sleep(5 * time.Second)
		s = c.restartServer(s)
		c.waitOnServerHealthz(s)
	}

	// Shutdown the go routine above.
	close(ch)
	// Wait for it to finish.
	wg.Wait()

	if len(errCh) != 0 {
		for err := range errCh {
			t.Logf("Received err %v during test", err)
		}
		t.Fatalf("Encountered errors")
	}
}

func TestNoRaceMemStoreCompactPerformance(t *testing.T) {
	//Load MemStore so that it is full
	subj, msg := "foo", make([]byte, 1000)
	storedMsgSize := memStoreMsgSize(subj, nil, msg)

	toStore := uint64(10_000)
	toStoreOnTop := uint64(1_000)
	setSeqNo := uint64(10_000_000_000)

	expectedPurge := toStore - 1
	maxBytes := storedMsgSize * toStore

	ms, err := newMemStore(&StreamConfig{Storage: MemoryStorage, MaxBytes: int64(maxBytes)})
	require_NoError(t, err)
	defer ms.Stop()

	for i := uint64(0); i < toStore; i++ {
		ms.StoreMsg(subj, nil, msg, 0)
	}
	state := ms.State()
	require_Equal(t, toStore, state.Msgs)
	require_Equal(t, state.Bytes, storedMsgSize*toStore)

	//1st run: Load additional messages then compact
	for i := uint64(0); i < toStoreOnTop; i++ {
		ms.StoreMsg(subj, nil, msg, 0)
	}
	startFirstRun := time.Now()
	purgedFirstRun, _ := ms.Compact(toStore + toStoreOnTop)
	elapsedFirstRun := time.Since(startFirstRun)
	require_Equal(t, expectedPurge, purgedFirstRun)

	//set the seq number to a very high value by compacting with a too high seq number
	purgedFull, _ := ms.Compact(setSeqNo)
	require_Equal(t, 1, purgedFull)

	//2nd run: Compact again
	for i := uint64(0); i < toStore; i++ {
		ms.StoreMsg(subj, nil, msg, 0)
	}
	startSecondRun := time.Now()
	purgedSecondRun, _ := ms.Compact(setSeqNo + toStore - 1)
	elapsedSecondRun := time.Since(startSecondRun)
	require_Equal(t, expectedPurge, purgedSecondRun)

	//Calculate delta between runs and fail if it is too high
	require_LessThan(t, elapsedSecondRun-elapsedFirstRun, time.Duration(1)*time.Second)
}

func TestNoRaceJetStreamSnapshotsWithSlowAckDontSlowConsumer(t *testing.T) {
	// Test takes less time this way.
	snapshotAckTimeout = 500 * time.Millisecond
	t.Cleanup(func() {
		snapshotAckTimeout = defaultSnapshotAckTimeout
	})

	s := RunBasicJetStreamServer(t)
	defer s.Shutdown()

	ech := make(chan error)
	ecb := func(_ *nats.Conn, _ *nats.Subscription, err error) {
		if err != nil {
			ech <- err
		}
	}
	nc, js := jsClientConnect(t, s, nats.ErrorHandler(ecb))
	defer nc.Close()

	_, err := js.AddStream(&nats.StreamConfig{
		Name:     "TEST",
		Subjects: []string{"foo"},
	})
	require_NoError(t, err)

	// Put in over 64MB.
	msg, toSend := make([]byte, 1024*1024), 80
	crand.Read(msg)

	for i := 0; i < toSend; i++ {
		_, err := js.Publish("foo", msg)
		require_NoError(t, err)
	}

	sreq := &JSApiStreamSnapshotRequest{
		DeliverSubject: nats.NewInbox(),
		ChunkSize:      1024 * 1024,
	}
	req, _ := json.Marshal(sreq)
	rmsg, err := nc.Request(fmt.Sprintf(JSApiStreamSnapshotT, "TEST"), req, time.Second)
	require_NoError(t, err)

	var resp JSApiStreamSnapshotResponse
	json.Unmarshal(rmsg.Data, &resp)
	require_True(t, resp.Error == nil)

	done := make(chan *nats.Msg)
	sub, _ := nc.Subscribe(sreq.DeliverSubject, func(m *nats.Msg) {
		// EOF
		if len(m.Data) == 0 {
			done <- m
			return
		}
	})
	defer sub.Unsubscribe()

	// Check that we do not get disconnected due to slow consumer.
	select {
	case msg := <-done:
		require_Equal(t, msg.Header.Get("Status"), "408")
		require_Equal(t, msg.Header.Get("Description"), "No Flow Response")
	case <-ech:
		t.Fatalf("Got disconnected: %v", err)
	case <-time.After(snapshotAckTimeout * 2):
		t.Fatalf("Should have received EOF with error status")
	}
}

func TestNoRaceJetStreamWQSkippedMsgsOnScaleUp(t *testing.T) {
	checkInterestStateT = 4 * time.Second
	checkInterestStateJ = 1
	defer func() {
		checkInterestStateT = defaultCheckInterestStateT
		checkInterestStateJ = defaultCheckInterestStateJ
	}()

	c := createJetStreamClusterExplicit(t, "R3S", 3)
	defer c.shutdown()

	nc, js := jsClientConnect(t, c.randomServer())
	defer nc.Close()

	const pre = "CORE_ENT_DR_OTP_22."
	wcSubj := pre + ">"

	_, err := js.AddStream(&nats.StreamConfig{
		Name:        "TEST",
		Subjects:    []string{wcSubj},
		Retention:   nats.WorkQueuePolicy,
		AllowDirect: true,
		Replicas:    3,
	})
	require_NoError(t, err)

	cfg := &nats.ConsumerConfig{
		Durable:           "dlc",
		FilterSubject:     wcSubj,
		DeliverPolicy:     nats.DeliverAllPolicy,
		AckPolicy:         nats.AckExplicitPolicy,
		MaxAckPending:     10_000,
		AckWait:           500 * time.Millisecond,
		MaxWaiting:        100,
		MaxRequestExpires: 1050 * time.Millisecond,
	}
	_, err = js.AddConsumer("TEST", cfg)
	require_NoError(t, err)

	pdone := make(chan bool)
	cdone := make(chan bool)

	// We will have 51 consumer apps and a producer app. Make sure to wait for
	// all go routines to end at the end of the test.
	wg := sync.WaitGroup{}
	wg.Add(52)

	// Publish routine
	go func() {
		defer wg.Done()

		publishSubjects := []string{
			"CORE_ENT_DR_OTP_22.P.H.TC.10011.1010.918886682066",
			"CORE_ENT_DR_OTP_22.P.H.TC.10011.1010.918886682067",
			"CORE_ENT_DR_OTP_22.P.H.TC.10011.1010.916596543211",
			"CORE_ENT_DR_OTP_22.P.H.TC.10011.1010.916596543212",
			"CORE_ENT_DR_OTP_22.P.H.TC.10011.1010.916596543213",
			"CORE_ENT_DR_OTP_22.P.H.TC.10011.1010.916596543214",
			"CORE_ENT_DR_OTP_22.P.H.TC.10011.1010.916596543215",
			"CORE_ENT_DR_OTP_22.P.H.TC.10011.1010.916596543216",
			"CORE_ENT_DR_OTP_22.P.H.TC.10011.1010.916596543217",
		}
		// ~1.7kb
		msg := bytes.Repeat([]byte("Z"), 1750)

		// 200 msgs/s
		st := time.NewTicker(5 * time.Millisecond)
		defer st.Stop()

		nc, js := jsClientConnect(t, c.randomServer())
		defer nc.Close()

		for {
			select {
			case <-st.C:
				subj := publishSubjects[rand.IntN(len(publishSubjects))]
				_, err = js.Publish(subj, msg)
				require_NoError(t, err)
			case <-pdone:
				return
			}
		}
	}()

	consumerApp := func() {
		defer wg.Done()

		nc, js := jsClientConnect(t, c.randomServer())
		defer nc.Close()

		_, err := js.ConsumerInfo("TEST", "dlc")
		require_NoError(t, err)
		_, err = js.UpdateConsumer("TEST", cfg)
		require_NoError(t, err)

		sub, err := js.PullSubscribe(wcSubj, "dlc")
		require_NoError(t, err)

		st := time.NewTicker(100 * time.Millisecond)
		defer st.Stop()

		for {
			select {
			case <-st.C:
				msgs, err := sub.Fetch(1, nats.MaxWait(100*time.Millisecond))
				if err != nil {
					continue
				}
				require_Equal(t, len(msgs), 1)
				m := msgs[0]
				if rand.IntN(10) == 1 {
					m.Nak()
				} else {
					// Wait up to 20ms to ack.
					time.Sleep(time.Duration(rand.IntN(20)) * time.Millisecond)
					// This could fail and that is ok, system should recover due to low ack wait.
					m.Ack()
				}
			case <-cdone:
				return
			}
		}
	}

	// Now consumer side single.
	go consumerApp()

	// Wait for 2s
	time.Sleep(2 * time.Second)

	// Now spin up 50 more.
	for i := 1; i <= 50; i++ {
		if i%5 == 0 {
			time.Sleep(200 * time.Millisecond)
		}
		go consumerApp()
	}

	timeout := time.Now().Add(8 * time.Second)
	for time.Now().Before(timeout) {
		time.Sleep(750 * time.Millisecond)
		if s := c.consumerLeader(globalAccountName, "TEST", "dlc"); s != nil {
			s.JetStreamStepdownConsumer(globalAccountName, "TEST", "dlc")
		}
	}

	// Close publishers and defer closing consumers.
	close(pdone)
	defer func() {
		close(cdone)
		wg.Wait()
	}()

	checkFor(t, 30*time.Second, 50*time.Millisecond, func() error {
		si, err := js.StreamInfo("TEST")
		require_NoError(t, err)
		if si.State.NumDeleted > 0 || si.State.Msgs > 0 {
			return fmt.Errorf("State not correct: %+v", si.State)
		}
		return nil
	})
}

func TestNoRaceConnectionObjectReleased(t *testing.T) {
	ob1Conf := createConfFile(t, []byte(`
		listen: "127.0.0.1:-1"
		server_name: "B1"
		accounts {
			A { users: [{user: a, password: pwd}] }
			SYS { users: [{user: sys, password: pwd}] }
		}
		cluster {
			name: "B"
			listen: "127.0.0.1:-1"
		}
		gateway {
			name: "B"
			listen: "127.0.0.1:-1"
		}
		leaf {
			listen: "127.0.0.1:-1"
		}
		system_account: "SYS"
	`))
	sb1, ob1 := RunServerWithConfig(ob1Conf)
	defer sb1.Shutdown()

	oaConf := createConfFile(t, []byte(fmt.Sprintf(`
		listen: "127.0.0.1:-1"
		server_name: "A"
		accounts {
			A { users: [{user: a, password: pwd}] }
			SYS { users: [{user: sys, password: pwd}] }
		}
		gateway {
			name: "A"
			listen: "127.0.0.1:-1"
			gateways [
				{
					name: "B"
					url: "nats://a:pwd@127.0.0.1:%d"
				}
			]
		}
		websocket {
			listen: "127.0.0.1:-1"
			no_tls: true
		}
		system_account: "SYS"
	`, ob1.Gateway.Port)))
	sa, oa := RunServerWithConfig(oaConf)
	defer sa.Shutdown()

	waitForOutboundGateways(t, sa, 1, 2*time.Second)
	waitForOutboundGateways(t, sb1, 1, 2*time.Second)

	ob2Conf := createConfFile(t, []byte(fmt.Sprintf(`
		listen: "127.0.0.1:-1"
		server_name: "B2"
		accounts {
			A { users: [{user: a, password: pwd}] }
			SYS { users: [{user: sys, password: pwd}] }
		}
		cluster {
			name: "B"
			listen: "127.0.0.1:-1"
			routes: ["nats://127.0.0.1:%d"]
		}
		gateway {
			name: "B"
			listen: "127.0.0.1:-1"
		}
		system_account: "SYS"
	`, ob1.Cluster.Port)))
	sb2, _ := RunServerWithConfig(ob2Conf)
	defer sb2.Shutdown()

	checkClusterFormed(t, sb1, sb2)
	waitForOutboundGateways(t, sb2, 1, 2*time.Second)
	waitForInboundGateways(t, sa, 2, 2*time.Second)

	leafConf := createConfFile(t, []byte(fmt.Sprintf(`
		listen: "127.0.0.1:-1"
		server_name: "C"
		accounts {
			A { users: [{user: a, password: pwd}] }
			SYS { users: [{user: sys, password: pwd}] }
		}
		leafnodes {
			remotes [
				{ url: "nats://a:pwd@127.0.0.1:%d" }
			]
		}
		system_account: "SYS"
	`, ob1.LeafNode.Port)))
	leaf, _ := RunServerWithConfig(leafConf)
	defer leaf.Shutdown()

	checkLeafNodeConnected(t, leaf)

	// Start an independent MQTT server to check MQTT client connection.
	mo := testMQTTDefaultOptions()
	mo.ServerName = "MQTTServer"
	sm := testMQTTRunServer(t, mo)
	defer testMQTTShutdownServer(sm)

	mc, mr := testMQTTConnect(t, &mqttConnInfo{cleanSess: true}, mo.MQTT.Host, mo.MQTT.Port)
	defer mc.Close()
	testMQTTCheckConnAck(t, mr, mqttConnAckRCConnectionAccepted, false)

	nc := natsConnect(t, sb1.ClientURL(), nats.UserInfo("a", "pwd"))
	defer nc.Close()
	cid, err := nc.GetClientID()
	require_NoError(t, err)
	natsSubSync(t, nc, "foo")
	natsFlush(t, nc)

	ncWS := natsConnect(t, fmt.Sprintf("ws://a:pwd@127.0.0.1:%d", oa.Websocket.Port))
	defer ncWS.Close()
	cidWS, err := ncWS.GetClientID()
	require_NoError(t, err)

	var conns []net.Conn
	var total int
	var ch chan string

	track := func(c *client) {
		total++
		c.mu.Lock()
		conns = append(conns, c.nc)
		c.mu.Unlock()
		runtime.SetFinalizer(c, func(c *client) {
			ch <- fmt.Sprintf("Server=%s - Kind=%s - Conn=%v", c.srv, c.kindString(), c)
		})
	}
	// Track the connection for the MQTT client
	sm.mu.RLock()
	for _, c := range sm.clients {
		track(c)
	}
	sm.mu.RUnlock()

	// Track the connection from the NATS client
	track(sb1.getClient(cid))
	// The outbound connection to GW "A"
	track(sb1.getOutboundGatewayConnection("A"))
	// The inbound connection from GW "A"
	var inGW []*client
	sb1.getInboundGatewayConnections(&inGW)
	track(inGW[0])
	// The routes from sb2
	sb1.forEachRoute(func(r *client) {
		track(r)
	})
	// The leaf form "LEAF"
	sb1.mu.RLock()
	for _, l := range sb1.leafs {
		track(l)
	}
	sb1.mu.RUnlock()

	// Now from sb2, the routes to sb1
	sb2.forEachRoute(func(r *client) {
		track(r)
	})
	// The outbound connection to GW "A"
	track(sb2.getOutboundGatewayConnection("A"))

	// From server "A", track the outbound GW
	track(sa.getOutboundGatewayConnection("B"))
	inGW = inGW[:0]
	// Track the inbound GW connections
	sa.getInboundGatewayConnections(&inGW)
	for _, ig := range inGW {
		track(ig)
	}
	// Track the websocket client
	track(sa.getClient(cidWS))

	// From the LEAF server, the connection to sb1
	leaf.mu.RLock()
	for _, l := range leaf.leafs {
		track(l)
	}
	leaf.mu.RUnlock()

	// Now close all connections and wait to see if all connections
	// with the finalizer set is invoked.
	ch = make(chan string, total)
	// Close the clients and then all other connections to create a disconnect.
	nc.Close()
	mc.Close()
	ncWS.Close()
	for _, conn := range conns {
		conn.Close()
	}
	// Wait and see if we get them all.
	tm := time.NewTimer(10 * time.Second)
	defer tm.Stop()
	tk := time.NewTicker(10 * time.Millisecond)
	for clients := make([]string, 0, total); len(clients) < total; {
		select {
		case <-tk.C:
			runtime.GC()
		case cs := <-ch:
			clients = append(clients, cs)
		case <-tm.C:
			// Don't fail the test since there is no guarantee that
			// finalizers are invoked.
			t.Logf("Got %v out of %v finalizers", len(clients), total)
			slices.Sort(clients)
			for _, cs := range clients {
				t.Logf("  => %s", cs)
			}
			return
		}
	}
}

func TestNoRaceFileStoreMsgLoadNextMsgMultiPerf(t *testing.T) {
	fs, err := newFileStore(
		FileStoreConfig{StoreDir: t.TempDir()},
		StreamConfig{Name: "zzz", Subjects: []string{"foo.*"}, Storage: FileStorage})
	require_NoError(t, err)
	defer fs.Stop()

	// Put 1k msgs in
	for i := 0; i < 1000; i++ {
		subj := fmt.Sprintf("foo.%d", i)
		_, _, err = fs.StoreMsg(subj, nil, []byte("ZZZ"), 0)
		require_NoError(t, err)
	}

	var smv StoreMsg

	// Now do normal load next with no filter.
	// This is baseline.
	start := time.Now()
	for i, seq := 0, uint64(1); i < 1000; i++ {
		sm, nseq, err := fs.LoadNextMsg(_EMPTY_, false, seq, &smv)
		require_NoError(t, err)
		require_Equal(t, sm.subj, fmt.Sprintf("foo.%d", i))
		require_Equal(t, nseq, seq)
		seq++
	}
	baseline := time.Since(start)
	t.Logf("Single - No filter %v", baseline)

	// Allow some additional skew.
	baseline += time.Millisecond

	// Now do normal load next with wc filter.
	start = time.Now()
	for i, seq := 0, uint64(1); i < 1000; i++ {
		sm, nseq, err := fs.LoadNextMsg("foo.>", true, seq, &smv)
		require_NoError(t, err)
		require_Equal(t, sm.subj, fmt.Sprintf("foo.%d", i))
		require_Equal(t, nseq, seq)
		seq++
	}
	elapsed := time.Since(start)
	t.Logf("Single - WC filter %v", elapsed)
	require_LessThan(t, elapsed, 2*baseline)

	// Now do multi load next with 1 wc entry.
	sl := gsl.NewSublist[struct{}]()
	require_NoError(t, sl.Insert("foo.>", struct{}{}))
	start = time.Now()
	for i, seq := 0, uint64(1); i < 1000; i++ {
		sm, nseq, err := fs.LoadNextMsgMulti(sl, seq, &smv)
		require_NoError(t, err)
		require_Equal(t, sm.subj, fmt.Sprintf("foo.%d", i))
		require_Equal(t, nseq, seq)
		seq++
	}
	elapsed = time.Since(start)
	t.Logf("Multi - Single WC filter %v", elapsed)
	require_LessThan(t, elapsed, 2*baseline)

	// Now do multi load next with 1000 literal subjects.
	sl = gsl.NewSublist[struct{}]()
	for i := 0; i < 1000; i++ {
		subj := fmt.Sprintf("foo.%d", i)
		require_NoError(t, sl.Insert(subj, struct{}{}))
	}
	start = time.Now()
	for i, seq := 0, uint64(1); i < 1000; i++ {
		sm, nseq, err := fs.LoadNextMsgMulti(sl, seq, &smv)
		require_NoError(t, err)
		require_Equal(t, sm.subj, fmt.Sprintf("foo.%d", i))
		require_Equal(t, nseq, seq)
		seq++
	}
	elapsed = time.Since(start)
	t.Logf("Multi - 1000 filters %v", elapsed)
	require_LessThan(t, elapsed, 3*baseline)
}

func TestNoRaceWQAndMultiSubjectFilters(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "R3S", 3)
	defer c.shutdown()

	nc, js := jsClientConnect(t, c.randomServer())
	defer nc.Close()

	_, err := js.AddStream(&nats.StreamConfig{
		Name:      "TEST",
		Subjects:  []string{"Z.>"},
		Retention: nats.WorkQueuePolicy,
	})
	require_NoError(t, err)

	stopPubs := make(chan bool)

	publish := func(subject string) {
		nc, js := jsClientConnect(t, c.randomServer())
		defer nc.Close()

		for {
			select {
			case <-stopPubs:
				return
			default:
				_, _ = js.Publish(subject, []byte("hello"))
			}
		}
	}

	go publish("Z.foo")
	go publish("Z.bar")
	go publish("Z.baz")

	// Cancel pubs after 10s.
	time.AfterFunc(10*time.Second, func() { close(stopPubs) })

	// Create a consumer
	_, err = js.AddConsumer("TEST", &nats.ConsumerConfig{
		Durable:        "zzz",
		AckPolicy:      nats.AckExplicitPolicy,
		AckWait:        5 * time.Second,
		FilterSubjects: []string{"Z.foo", "Z.bar", "Z.baz"},
	})
	require_NoError(t, err)

	sub, err := js.PullSubscribe(_EMPTY_, "zzz", nats.Bind("TEST", "zzz"))
	require_NoError(t, err)

	received := make([]uint64, 0, 256_000)
	batchSize := 10

	for running := true; running; {
		msgs, err := sub.Fetch(batchSize, nats.MaxWait(2*time.Second))
		if err == nats.ErrTimeout {
			running = false
		}
		for _, m := range msgs {
			meta, err := m.Metadata()
			require_NoError(t, err)
			received = append(received, meta.Sequence.Stream)
			m.Ack()
		}
	}

	slices.Sort(received)

	var pseq, gaps uint64
	for _, seq := range received {
		if pseq != 0 && pseq != seq-1 {
			gaps += seq - pseq + 1
		}
		pseq = seq
	}
	si, err := js.StreamInfo("TEST")
	require_NoError(t, err)

	if si.State.Msgs != 0 || gaps > 0 {
		t.Fatalf("Orphaned msgs %d with %d gaps detected", si.State.Msgs, gaps)
	}
}

// https://github.com/nats-io/nats-server/issues/4957
func TestNoRaceWQAndMultiSubjectFiltersRace(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "R3S", 3)
	defer c.shutdown()

	nc, js := jsClientConnect(t, c.randomServer())
	defer nc.Close()

	_, err := js.AddStream(&nats.StreamConfig{
		Name:      "TEST",
		Subjects:  []string{"Z.>"},
		Retention: nats.WorkQueuePolicy,
		Replicas:  1,
	})
	require_NoError(t, err)

	// The bug would happen when the stream was on same server as meta-leader.
	// So make that so.
	// Make sure stream leader is on S-1
	sl := c.streamLeader(globalAccountName, "TEST")
	checkFor(t, 5*time.Second, 100*time.Millisecond, func() error {
		if sl == c.leader() {
			return nil
		}
		// Move meta-leader since stream can be R1.
		snc, _ := jsClientConnect(t, c.randomServer(), nats.UserInfo("admin", "s3cr3t!"))
		defer snc.Close()
		if _, err := snc.Request(JSApiLeaderStepDown, nil, time.Second); err != nil {
			return err
		}
		return fmt.Errorf("stream leader on meta-leader")
	})

	start := make(chan struct{})
	var done, ready sync.WaitGroup

	// Create num go routines who will all race to create a consumer with the same filter subject but a different name.
	num := 10
	ready.Add(num)
	done.Add(num)

	for i := 0; i < num; i++ {
		go func(n int) {
			// Connect directly to the meta leader but with our own connection.
			s := c.leader()
			nc, js := jsClientConnect(t, s)
			defer nc.Close()

			ready.Done()
			defer done.Done()
			<-start

			js.AddConsumer("TEST", &nats.ConsumerConfig{
				Name:          fmt.Sprintf("C-%d", n),
				FilterSubject: "Z.foo",
				AckPolicy:     nats.AckExplicitPolicy,
			})
		}(i)
	}

	// Wait for requestors to be ready
	ready.Wait()
	close(start)
	done.Wait()

	checkFor(t, 5*time.Second, 100*time.Millisecond, func() error {
		si, err := js.StreamInfo("TEST")
		require_NoError(t, err)
		if si.State.Consumers != 1 {
			return fmt.Errorf("Consumer count not correct: %d vs 1", si.State.Consumers)
		}
		return nil
	})
}

func TestNoRaceFileStoreWriteFullStateUniqueSubjects(t *testing.T) {
	fcfg := FileStoreConfig{StoreDir: t.TempDir()}
	fs, err := newFileStore(fcfg,
		StreamConfig{Name: "zzz", Subjects: []string{"records.>"}, Storage: FileStorage, MaxMsgsPer: 1, MaxBytes: 15 * 1024 * 1024 * 1024})
	require_NoError(t, err)
	defer fs.Stop()

	qch := make(chan struct{})
	defer close(qch)

	go func() {
		const numThreshold = 1_000_000
		tick := time.NewTicker(1 * time.Second)
		for {
			select {
			case <-qch:
				return
			case <-tick.C:
				err := fs.writeFullState()
				var state StreamState
				fs.FastState(&state)
				if state.Msgs > numThreshold && err != nil {
					require_Error(t, err, errStateTooBig)
				}
			}
		}
	}()

	labels := []string{"AAAAA", "BBBB", "CCCC", "DD", "EEEEE"}
	msg := []byte(strings.Repeat("Z", 128))

	for i := 0; i < 100; i++ {
		partA := nuid.Next()
		for j := 0; j < 100; j++ {
			partB := nuid.Next()
			for k := 0; k < 500; k++ {
				partC := nuid.Next()
				partD := labels[rand.IntN(len(labels)-1)]
				subject := fmt.Sprintf("records.%s.%s.%s.%s.%s", partA, partB, partC, partD, nuid.Next())
				start := time.Now()
				fs.StoreMsg(subject, nil, msg, 0)
				elapsed := time.Since(start)
				if elapsed > 500*time.Millisecond {
					t.Fatalf("Slow store for %q: %v\n", subject, elapsed)
				}
			}
		}
	}
	// Make sure we do write the full state on stop.
	fs.Stop()
	fi, err := os.Stat(filepath.Join(fcfg.StoreDir, msgDir, streamStreamStateFile))
	require_NoError(t, err)
	// ~500MB, could change if we tweak encodings..
	require_True(t, fi.Size() > 500*1024*1024)
}

// When a catchup takes a long time and the ingest rate is high enough to cause us
// to drop append entries and move our first past out catchup window due to max bytes or max msgs, etc.
func TestNoRaceLargeStreamCatchups(t *testing.T) {
	// This usually takes too long on Travis.
	t.Skip()

	var jsMaxOutTempl = `
	listen: 127.0.0.1:-1
	server_name: %s
	jetstream: {max_file_store: 22GB, store_dir: '%s', max_outstanding_catchup: 128KB}
	cluster {
		name: %s
		listen: 127.0.0.1:%d
		routes = [%s]
	}
	# For access to system account.
	accounts { $SYS { users = [ { user: "admin", pass: "s3cr3t!" } ] } }
`

	c := createJetStreamClusterWithTemplate(t, jsMaxOutTempl, "R3S", 3)
	defer c.shutdown()

	nc, _ := jsClientConnect(t, c.randomServer())
	defer nc.Close()

	js, err := nc.JetStream(nats.PublishAsyncMaxPending(64 * 1024))
	require_NoError(t, err)

	cfg := &nats.StreamConfig{
		Name:     "TEST",
		Subjects: []string{"Z.>"},
		MaxBytes: 4 * 1024 * 1024 * 1024,
		Replicas: 1, // Start at R1
	}

	_, err = js.AddStream(cfg)
	require_NoError(t, err)

	// Load up to a decent size first.
	num, msg := 25_000, bytes.Repeat([]byte("Z"), 256*1024)
	for i := 0; i < num; i++ {
		_, err := js.PublishAsync("Z.Z.Z", msg)
		require_NoError(t, err)
	}
	select {
	case <-js.PublishAsyncComplete():
	case <-time.After(20 * time.Second):
		t.Fatalf("Did not receive completion signal")
	}

	var sl *Server
	time.AfterFunc(time.Second, func() {
		cfg.Replicas = 3
		_, err = js.UpdateStream(cfg)
		require_NoError(t, err)
		c.waitOnStreamLeader(globalAccountName, "TEST")
		sl = c.streamLeader(globalAccountName, "TEST")
	})

	// Run for 60 seconds sending new messages at a high rate.
	timeout := time.Now().Add(60 * time.Second)
	for time.Now().Before(timeout) {
		for i := 0; i < 5_000; i++ {
			// Not worried about each message, so not checking err here.
			// Just generating high load of new traffic while trying to catch up.
			js.PublishAsync("Z.Z.Z", msg)
		}
		// This will gate us waiting on a response.
		js.Publish("Z.Z.Z", msg)
	}

	// Make sure the leader has not changed.
	require_Equal(t, sl, c.streamLeader(globalAccountName, "TEST"))

	// Grab the leader and its state.
	mset, err := sl.GlobalAccount().lookupStream("TEST")
	require_NoError(t, err)
	expected := mset.state()

	checkFor(t, 45*time.Second, time.Second, func() error {
		for _, s := range c.servers {
			if s == sl {
				continue
			}
			mset, err := s.GlobalAccount().lookupStream("TEST")
			require_NoError(t, err)
			state := mset.state()
			if !reflect.DeepEqual(expected, state) {
				return fmt.Errorf("Follower %v state does not match: %+v vs %+v", s, state, expected)
			}
		}
		return nil
	})
}

func TestNoRaceLargeNumDeletesStreamCatchups(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "R3S", 3)
	defer c.shutdown()

	nc, _ := jsClientConnect(t, c.randomServer())
	defer nc.Close()

	js, err := nc.JetStream(nats.PublishAsyncMaxPending(16 * 1024))
	require_NoError(t, err)

	cfg := &nats.StreamConfig{
		Name:     "TEST",
		Subjects: []string{"foo"},
		Replicas: 1, // Start at R1
	}
	_, err = js.AddStream(cfg)
	require_NoError(t, err)

	// We will manipulate the stream at the lower level to achieve large number of interior deletes.
	// We will store only 2 msgs, but have 100M deletes in between.
	sl := c.streamLeader(globalAccountName, "TEST")
	mset, err := sl.GlobalAccount().lookupStream("TEST")
	require_NoError(t, err)
	mset.mu.Lock()
	mset.store.StoreMsg("foo", nil, []byte("ok"), 0)
	mset.store.SkipMsgs(2, 1_000_000_000)
	mset.store.StoreMsg("foo", nil, []byte("ok"), 0)
	mset.store.SkipMsgs(1_000_000_003, 1_000_000_000)
	var state StreamState
	mset.store.FastState(&state)
	mset.lseq = state.LastSeq
	mset.mu.Unlock()

	cfg.Replicas = 3
	_, err = js.UpdateStream(cfg)
	require_NoError(t, err)
	c.waitOnStreamLeader(globalAccountName, "TEST")
	mset, err = sl.GlobalAccount().lookupStream("TEST")
	require_NoError(t, err)
	expected := mset.state()

	// This should happen fast and not spin on all interior deletes.
	checkFor(t, 250*time.Millisecond, 50*time.Millisecond, func() error {
		for _, s := range c.servers {
			if s == sl {
				continue
			}
			mset, err := s.GlobalAccount().lookupStream("TEST")
			require_NoError(t, err)
			state := mset.state()
			// Ignore LastTime for this test since we send delete range at end.
			state.LastTime = expected.LastTime
			if !reflect.DeepEqual(expected, state) {
				return fmt.Errorf("Follower %v state does not match: %+v vs %+v", s, state, expected)
			}
		}
		return nil
	})
}

func TestNoRaceJetStreamClusterMemoryStreamLastSequenceResetAfterRestart(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "R3S", 3)
	defer c.shutdown()

	nc, js := jsClientConnect(t, c.randomServer())
	defer nc.Close()

	numStreams := 250
	var wg sync.WaitGroup
	wg.Add(numStreams)

	for i := 1; i <= numStreams; i++ {
		go func(n int) {
			defer wg.Done()
			_, err := js.AddStream(&nats.StreamConfig{
				Name:     fmt.Sprintf("TEST:%d", n),
				Storage:  nats.MemoryStorage,
				Subjects: []string{fmt.Sprintf("foo.%d.*", n)},
				Replicas: 3,
			}, nats.MaxWait(30*time.Second))
			require_NoError(t, err)
			subj := fmt.Sprintf("foo.%d.bar", n)
			for i := 0; i < 222; i++ {
				js.Publish(subj, nil)
			}
		}(i)
	}
	wg.Wait()

	// Make sure all streams have a snapshot in place to stress the snapshot logic for memory based streams.
	for _, s := range c.servers {
		for i := 1; i <= numStreams; i++ {
			stream := fmt.Sprintf("TEST:%d", i)
			mset, err := s.GlobalAccount().lookupStream(stream)
			require_NoError(t, err)
			node := mset.raftNode()
			require_NotNil(t, node)
			node.InstallSnapshot(mset.stateSnapshot(), false)
		}
	}

	// Do 5 rolling restarts waiting on healthz in between.
	for i := 0; i < 5; i++ {
		// Walk the servers and shut each down, and wipe the storage directory.
		for _, s := range c.servers {
			s.Shutdown()
			s.WaitForShutdown()
			s = c.restartServer(s)
			c.waitOnServerHealthz(s)
			c.waitOnAllCurrent()
			// Make sure all streams are current after healthz returns ok.
			for i := 1; i <= numStreams; i++ {
				stream := fmt.Sprintf("TEST:%d", i)
				mset, err := s.GlobalAccount().lookupStream(stream)
				require_NoError(t, err)
				var state StreamState
				checkFor(t, 30*time.Second, time.Second, func() error {
					mset.store.FastState(&state)
					if state.LastSeq != 222 {
						return fmt.Errorf("%v Wrong last sequence %d for %q - State  %+v", s, state.LastSeq, stream, state)
					}
					return nil
				})
			}
		}
	}
}

func TestNoRaceJetStreamClusterMemoryWorkQueueLastSequenceResetAfterRestart(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "R3S", 3)
	defer c.shutdown()

	numStreams := 50
	var wg sync.WaitGroup
	wg.Add(numStreams)

	for i := 1; i <= numStreams; i++ {
		go func(n int) {
			defer wg.Done()

			nc, js := jsClientConnect(t, c.randomServer())
			defer nc.Close()

			checkFor(t, 5*time.Second, time.Second, func() error {
				_, err := js.AddStream(&nats.StreamConfig{
					Name:      fmt.Sprintf("TEST:%d", n),
					Storage:   nats.MemoryStorage,
					Retention: nats.WorkQueuePolicy,
					Subjects:  []string{fmt.Sprintf("foo.%d.*", n)},
					Replicas:  3,
				}, nats.MaxWait(time.Second))
				return err
			})

			subj := fmt.Sprintf("foo.%d.bar", n)
			for i := 0; i < 22; i++ {
				checkFor(t, 5*time.Second, time.Second, func() error {
					_, err := js.Publish(subj, nil)
					return err
				})
			}
			// Now consume them all as well.
			var err error
			var sub *nats.Subscription
			checkFor(t, 5*time.Second, time.Second, func() error {
				sub, err = js.PullSubscribe(subj, "wq")
				return err
			})

			var msgs []*nats.Msg
			checkFor(t, 5*time.Second, time.Second, func() error {
				msgs, err = sub.Fetch(22, nats.MaxWait(time.Second))
				return err
			})
			require_Equal(t, len(msgs), 22)
			for _, m := range msgs {
				checkFor(t, 5*time.Second, time.Second, func() error {
					return m.AckSync()
				})
			}
		}(i)
	}
	wg.Wait()

	// Do 2 rolling restarts waiting on healthz in between.
	for i := 0; i < 2; i++ {
		// Walk the servers and shut each down, and wipe the storage directory.
		for _, s := range c.servers {
			s.Shutdown()
			s.WaitForShutdown()
			s = c.restartServer(s)
			c.waitOnServerHealthz(s)
			c.waitOnAllCurrent()
			// Make sure all streams are current after healthz returns ok.
			for i := 1; i <= numStreams; i++ {
				stream := fmt.Sprintf("TEST:%d", i)
				mset, err := s.GlobalAccount().lookupStream(stream)
				require_NoError(t, err)
				var state StreamState
				checkFor(t, 20*time.Second, time.Second, func() error {
					mset.store.FastState(&state)
					if state.LastSeq != 22 {
						return fmt.Errorf("%v Wrong last sequence %d for %q - State  %+v", s, state.LastSeq, stream, state)
					}
					if state.FirstSeq != 23 {
						return fmt.Errorf("%v Wrong first sequence %d for %q - State  %+v", s, state.FirstSeq, stream, state)
					}
					return nil
				})
			}
		}
	}
}

func TestNoRaceJetStreamClusterMirrorSkipSequencingBug(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "R5S", 5)
	defer c.shutdown()

	nc, js := jsClientConnect(t, c.randomServer())
	defer nc.Close()

	_, err := js.AddStream(&nats.StreamConfig{
		Name:              "ORIGIN",
		Subjects:          []string{"foo.*"},
		MaxMsgsPerSubject: 1,
		Replicas:          1,
		Storage:           nats.MemoryStorage,
	})
	require_NoError(t, err)

	// Create a mirror with R5 such that it will be much slower than the ORIGIN.
	_, err = js.AddStream(&nats.StreamConfig{
		Name:     "M",
		Replicas: 5,
		Mirror:   &nats.StreamSource{Name: "ORIGIN"},
	})
	require_NoError(t, err)

	// Connect new directly to ORIGIN
	s := c.streamLeader(globalAccountName, "ORIGIN")
	nc, js = jsClientConnect(t, s)
	defer nc.Close()

	// We are going to send at a high rate and also delete some along the way
	// via the max msgs per limit.
	for i := 0; i < 500_000; i++ {
		subj := fmt.Sprintf("foo.%d", i)
		publishAsync(t, js, subj, nil)
		// Create sequence holes every 100k.
		if i%100_000 == 0 {
			publishAsync(t, js, subj, nil)
		}
	}
	select {
	case <-js.PublishAsyncComplete():
	case <-time.After(5 * time.Second):
		t.Fatalf("Did not receive completion signal")
	}

	checkFor(t, 20*time.Second, time.Second, func() error {
		si, err := js.StreamInfo("M")
		require_NoError(t, err)
		if si.State.Msgs != 500_000 {
			return fmt.Errorf("Expected 500k msgs, got state: %+v", si.State)
		}
		return nil
	})
}

func TestNoRaceJetStreamStandaloneDontReplyToAckBeforeProcessingIt(t *testing.T) {
	s := RunBasicJetStreamServer(t)
	defer s.Shutdown()

	// Client for API requests.
	nc, js := jsClientConnect(t, s)
	defer nc.Close()

	_, err := js.AddStream(&nats.StreamConfig{
		Name:                 "WQ",
		Discard:              nats.DiscardNew,
		MaxMsgsPerSubject:    1,
		DiscardNewPerSubject: true,
		Retention:            nats.WorkQueuePolicy,
		Subjects:             []string{"queue.>"},
	})
	require_NoError(t, err)

	// Keep this low since we are going to run as many go routines
	// to consume, ack and republish the message.
	total := 10000
	// Populate the queue, one message per subject.
	for i := 0; i < total; i++ {
		js.Publish(fmt.Sprintf("queue.%d", i), []byte("hello"))
	}

	_, err = js.AddConsumer("WQ", &nats.ConsumerConfig{
		Durable:       "cons",
		AckPolicy:     nats.AckExplicitPolicy,
		MaxWaiting:    20000,
		MaxAckPending: -1,
	})
	require_NoError(t, err)

	sub, err := js.PullSubscribe("queue.>", "cons", nats.BindStream("WQ"))
	require_NoError(t, err)

	errCh := make(chan error, total)

	var wg sync.WaitGroup
	for iter := 0; iter < 3; iter++ {
		wg.Add(total)
		for i := 0; i < total; i++ {
			go func() {
				defer wg.Done()
				for {
					msgs, err := sub.Fetch(1)
					if err != nil {
						time.Sleep(5 * time.Millisecond)
						continue
					}
					msg := msgs[0]
					err = msg.AckSync()
					if err != nil {
						time.Sleep(5 * time.Millisecond)
						continue
					}
					_, err = js.Publish(msg.Subject, []byte("hello"))
					if err != nil {
						errCh <- err
						return
					}
					break
				}
			}()
		}
		wg.Wait()
		select {
		case err := <-errCh:
			t.Fatalf("Test failed, first error was: %v", err)
		default:
			// OK!
		}
	}
}

// Under certain scenarios an old index.db with a stream that has msg limits set will not restore properly
// due to an old index.db and compaction after the index.db took place which could lose per subject information.
func TestNoRaceFileStoreMsgLimitsAndOldRecoverState(t *testing.T) {
	for _, test := range []struct {
		name             string
		expectedFirstSeq uint64
		expectedLastSeq  uint64
		expectedMsgs     uint64
		transform        func(StreamConfig) StreamConfig
	}{
		{
			name:             "MaxMsgsPer",
			expectedFirstSeq: 10_001,
			expectedLastSeq:  1_010_001,
			expectedMsgs:     1_000_001,
			transform: func(config StreamConfig) StreamConfig {
				config.MaxMsgsPer = 1
				return config
			},
		},
		{
			name:             "MaxMsgs",
			expectedFirstSeq: 10_001,
			expectedLastSeq:  1_010_001,
			expectedMsgs:     1_000_001,
			transform: func(config StreamConfig) StreamConfig {
				config.MaxMsgs = 1_000_001
				return config
			},
		},
		{
			name:             "MaxBytes",
			expectedFirstSeq: 8_624,
			expectedLastSeq:  1_010_001,
			expectedMsgs:     1_001_378,
			transform: func(config StreamConfig) StreamConfig {
				config.MaxBytes = 1_065_353_216
				return config
			},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			sd := t.TempDir()
			fs, err := newFileStore(
				FileStoreConfig{StoreDir: sd},
				test.transform(StreamConfig{Name: "zzz", Subjects: []string{"foo.*"}, Storage: FileStorage}),
			)
			require_NoError(t, err)
			defer fs.Stop()

			msg := make([]byte, 1024)

			for i := 0; i < 10_000; i++ {
				subj := fmt.Sprintf("foo.%d", i)
				fs.StoreMsg(subj, nil, msg, 0)
			}

			// This will write the index.db file. We will capture this and use it to replace a new one.
			sfile := filepath.Join(fs.fcfg.StoreDir, msgDir, streamStreamStateFile)
			fs.Stop()
			_, err = os.Stat(sfile)
			require_NoError(t, err)

			// Read it in and make sure len > 0.
			buf, err := os.ReadFile(sfile)
			require_NoError(t, err)
			require_True(t, len(buf) > 0)

			// Restart
			fs, err = newFileStore(
				FileStoreConfig{StoreDir: sd},
				test.transform(StreamConfig{Name: "zzz", Subjects: []string{"foo.*"}, Storage: FileStorage}),
			)
			require_NoError(t, err)
			defer fs.Stop()

			// Put in more messages with wider range. This will compact a bunch of the previous blocks.
			for i := 0; i < 1_000_001; i++ {
				subj := fmt.Sprintf("foo.%d", i)
				fs.StoreMsg(subj, nil, msg, 0)
			}

			var ss StreamState
			fs.FastState(&ss)
			require_Equal(t, ss.FirstSeq, test.expectedFirstSeq)
			require_Equal(t, ss.LastSeq, test.expectedLastSeq)
			require_Equal(t, ss.Msgs, test.expectedMsgs)

			// Now stop again, but replace index.db with old one.
			fs.Stop()
			// Put back old stream state.
			require_NoError(t, os.WriteFile(sfile, buf, defaultFilePerms))

			// Restart
			fs, err = newFileStore(
				FileStoreConfig{StoreDir: sd},
				test.transform(StreamConfig{Name: "zzz", Subjects: []string{"foo.*"}, Storage: FileStorage}),
			)
			require_NoError(t, err)
			defer fs.Stop()

			fs.FastState(&ss)
			require_Equal(t, ss.FirstSeq, test.expectedFirstSeq)
			require_Equal(t, ss.LastSeq, test.expectedLastSeq)
			require_Equal(t, ss.Msgs, test.expectedMsgs)
		})
	}
}

func TestNoRaceJetStreamClusterCheckInterestStatePerformanceWQ(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "R3F", 3)
	defer c.shutdown()

	s := c.randomServer()
	nc, js := jsClientConnect(t, s)
	defer nc.Close()

	_, err := js.AddStream(&nats.StreamConfig{
		Name:      "TEST",
		Subjects:  []string{"foo.*"},
		Retention: nats.WorkQueuePolicy,
	})
	require_NoError(t, err)

	// Load up a bunch of messages for three different subjects.
	msg := bytes.Repeat([]byte("Z"), 4096)
	for i := 0; i < 100_000; i++ {
		publishAsync(t, js, "foo.foo", msg)
	}
	for i := 0; i < 5_000; i++ {
		publishAsync(t, js, "foo.bar", msg)
		publishAsync(t, js, "foo.baz", msg)
	}
	select {
	case <-js.PublishAsyncComplete():
	case <-time.After(5 * time.Second):
		t.Fatalf("Did not receive completion signal")
	}

	// We will not process this one and leave it as "offline".
	_, err = js.PullSubscribe("foo.foo", "A")
	require_NoError(t, err)
	subB, err := js.PullSubscribe("foo.bar", "B")
	require_NoError(t, err)
	subC, err := js.PullSubscribe("foo.baz", "C")
	require_NoError(t, err)

	// Now catch up both B and C but let A simulate being offline of very behind.
	for i := 0; i < 5; i++ {
		for _, sub := range []*nats.Subscription{subB, subC} {
			msgs, err := sub.Fetch(1000)
			require_NoError(t, err)
			require_Equal(t, len(msgs), 1000)
			for _, m := range msgs {
				m.Ack()
			}
		}
	}
	// Let acks process.
	nc.Flush()
	time.Sleep(200 * time.Millisecond)

	// Now test the check checkInterestState() on the stream.
	sl := c.streamLeader(globalAccountName, "TEST")
	mset, err := sl.GlobalAccount().lookupStream("TEST")
	require_NoError(t, err)

	expireAllBlks := func() {
		mset.mu.RLock()
		fs := mset.store.(*fileStore)
		mset.mu.RUnlock()
		fs.mu.RLock()
		for _, mb := range fs.blks {
			mb.tryForceExpireCache()
		}
		fs.mu.RUnlock()
	}

	// First expire all the blocks.
	expireAllBlks()

	start := time.Now()
	mset.checkInterestState()
	elapsed := time.Since(start)
	// This is actually ~300 microseconds but due to travis and race flags etc.
	// Was > 30 ms before fix for comparison, M2 macbook air.
	require_LessThan(t, elapsed, 5*time.Millisecond)

	// Make sure we set the chkflr correctly.
	// The chkflr should be equal to asflr+1 (unless the next message matching the filter is further ahead).
	// Otherwise, if chkflr would be set higher a subsequent call to checkInterestState will be ineffective.
	requireFloorsEqual := func(o *consumer) {
		t.Helper()
		require_True(t, o != nil)
		o.mu.RLock()
		defer o.mu.RUnlock()
		_, seq, err := mset.store.LoadNextMsg(o.cfg.FilterSubject, false, o.asflr+1, nil)
		if err == ErrStoreEOF {
			seq++
		}
		require_True(t, o.asflr+1 <= seq)
		require_Equal(t, o.chkflr, seq)
	}

	requireFloorsEqual(mset.lookupConsumer("A"))
	requireFloorsEqual(mset.lookupConsumer("B"))
	requireFloorsEqual(mset.lookupConsumer("C"))

	// Expire all the blocks again.
	expireAllBlks()

	// This checks the chkflr state.
	start = time.Now()
	mset.checkInterestState()
	elapsed = time.Since(start)
	require_LessThan(t, elapsed, 5*time.Millisecond)
}

func TestNoRaceJetStreamClusterCheckInterestStatePerformanceInterest(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "R3F", 3)
	defer c.shutdown()

	s := c.randomServer()
	nc, js := jsClientConnect(t, s)
	defer nc.Close()

	_, err := js.AddStream(&nats.StreamConfig{
		Name:      "TEST",
		Subjects:  []string{"foo.*"},
		Retention: nats.InterestPolicy,
	})
	require_NoError(t, err)

	// We will not process this one and leave it as "offline".
	_, err = js.PullSubscribe("foo.foo", "A")
	require_NoError(t, err)
	_, err = js.PullSubscribe("foo.*", "B")
	require_NoError(t, err)
	// Make subC multi-subject.
	_, err = js.AddConsumer("TEST", &nats.ConsumerConfig{
		Durable:        "C",
		FilterSubjects: []string{"foo.foo", "foo.bar", "foo.baz"},
		AckPolicy:      nats.AckExplicitPolicy,
	})
	require_NoError(t, err)

	// Load up a bunch of messages for three different subjects.
	msg := bytes.Repeat([]byte("Z"), 4096)
	for i := 0; i < 90_000; i++ {
		publishAsync(t, js, "foo.foo", msg)
	}
	for i := 0; i < 5_000; i++ {
		publishAsync(t, js, "foo.bar", msg)
		publishAsync(t, js, "foo.baz", msg)
	}
	select {
	case <-js.PublishAsyncComplete():
	case <-time.After(5 * time.Second):
		t.Fatalf("Did not receive completion signal")
	}
	// This is so we do not asynchronously update our consumer state after we set the state due to notifications
	// from new messages for the stream.
	time.Sleep(250 * time.Millisecond)

	// Now catch up both B and C but let A simulate being offline of very behind.
	// Will do this manually here to speed up tests.
	sl := c.streamLeader(globalAccountName, "TEST")
	mset, err := sl.GlobalAccount().lookupStream("TEST")
	require_NoError(t, err)

	for _, cname := range []string{"B", "C"} {
		o := mset.lookupConsumer(cname)
		o.mu.Lock()
		o.setStoreState(&ConsumerState{
			Delivered: SequencePair{100_000, 100_000},
			AckFloor:  SequencePair{100_000, 100_000},
		})
		o.mu.Unlock()
	}

	// Now test the check checkInterestState() on the stream.
	start := time.Now()
	mset.checkInterestState()
	elapsed := time.Since(start)

	// Make sure we set the chkflr correctly.
	checkFloor := func(o *consumer) uint64 {
		require_True(t, o != nil)
		o.mu.RLock()
		defer o.mu.RUnlock()
		return o.chkflr
	}

	require_Equal(t, checkFloor(mset.lookupConsumer("A")), 1)
	require_Equal(t, checkFloor(mset.lookupConsumer("B")), 90_001)
	require_Equal(t, checkFloor(mset.lookupConsumer("C")), 100_001)

	// This checks the chkflr state. For this test this should be much faster,
	// two orders of magnitude then the first time.
	start = time.Now()
	mset.checkInterestState()
	require_True(t, time.Since(start) < elapsed/100)
}

func TestNoRaceJetStreamClusterLargeMetaSnapshotTiming(t *testing.T) {
	// This test was to show improvements in speed for marshaling the meta layer with lots of assets.
	// Move to S2.Encode vs EncodeBetter which is 2x faster and actually better compression.
	// Also moved to goccy json which is faster then the default and in my tests now always matches
	// the default encoder byte for byte which last time I checked it did not.
	t.Skip()

	c := createJetStreamClusterExplicit(t, "R3F", 3)
	defer c.shutdown()

	// Create 200 streams, each with 500 consumers.
	numStreams := 200
	numConsumers := 500
	wg := sync.WaitGroup{}
	wg.Add(numStreams)
	for i := 0; i < numStreams; i++ {
		go func() {
			defer wg.Done()
			s := c.randomServer()
			nc, js := jsClientConnect(t, s)
			defer nc.Close()
			sname := fmt.Sprintf("TEST-SNAPSHOT-%d", i)
			subj := fmt.Sprintf("foo.%d", i)
			_, err := js.AddStream(&nats.StreamConfig{
				Name:     sname,
				Subjects: []string{subj},
				Replicas: 3,
			})
			require_NoError(t, err)

			// Now consumers.
			for c := 0; c < numConsumers; c++ {
				_, err = js.AddConsumer(sname, &nats.ConsumerConfig{
					Durable:       fmt.Sprintf("C-%d", c),
					FilterSubject: subj,
					AckPolicy:     nats.AckExplicitPolicy,
					Replicas:      1,
				})
				require_NoError(t, err)
			}
		}()
	}
	wg.Wait()

	s := c.leader()
	js := s.getJetStream()
	n := js.getMetaGroup()
	// Now let's see how long it takes to create a meta snapshot and how big it is.
	start := time.Now()
	snap, _, _, err := js.metaSnapshot()
	require_NoError(t, err)
	require_NoError(t, n.InstallSnapshot(snap, false))
	t.Logf("Took %v to snap meta with size of %v\n", time.Since(start), friendlyBytes(len(snap)))
}

func TestNoRaceStoreReverseWalkWithDeletesPerf(t *testing.T) {
	cfg := StreamConfig{Name: "zzz", Subjects: []string{"foo.*"}, Storage: FileStorage}

	fs, err := newFileStore(FileStoreConfig{StoreDir: t.TempDir()}, cfg)
	require_NoError(t, err)
	defer fs.Stop()

	cfg.Storage = MemoryStorage
	ms, err := newMemStore(&cfg)
	require_NoError(t, err)
	defer ms.Stop()

	msg := []byte("Hello")

	for _, store := range []StreamStore{fs, ms} {
		_, _, err = store.StoreMsg("foo.A", nil, msg, 0)
		require_NoError(t, err)
		for i := 0; i < 1_000_000; i++ {
			_, _, err = store.StoreMsg("foo.B", nil, msg, 0)
			require_NoError(t, err)
		}
		_, _, err = store.StoreMsg("foo.C", nil, msg, 0)
		require_NoError(t, err)

		var ss StreamState
		store.FastState(&ss)
		require_Equal(t, ss.Msgs, 1_000_002)

		// Create a bunch of interior deletes.
		p, err := store.PurgeEx("foo.B", 0, 0)
		require_NoError(t, err)
		require_Equal(t, p, 1_000_000)

		// Preload the caches so this test only measures the reverse walk.
		preloadCaches := func() {
			if fs, ok := store.(*fileStore); ok {
				fs.mu.Lock()
				defer fs.mu.Unlock()
				for _, mb := range fs.blks {
					require_NoError(t, mb.loadMsgs())
				}
			}
		}

		// Now simulate a walk backwards as we currently do when searching for starting sequence numbers in sourced streams.
		preloadCaches()
		start := time.Now()
		var smv StoreMsg
		for seq := ss.LastSeq; seq > 0; seq-- {
			_, err := store.LoadMsg(seq, &smv)
			if err == errDeletedMsg || err == ErrStoreMsgNotFound {
				continue
			}
			require_NoError(t, err)
		}
		elapsed := time.Since(start)

		// Now use the optimized load prev.
		preloadCaches()
		seq, seen := ss.LastSeq, 0
		start = time.Now()
		for {
			sm, _, err := store.LoadPrevMsg(_EMPTY_, false, seq, &smv)
			if err == ErrStoreEOF {
				break
			}
			require_NoError(t, err)
			seq = sm.seq - 1
			seen++
		}
		elapsedNew := time.Since(start)
		require_Equal(t, seen, 2)

		switch store.(type) {
		case *memStore:
			require_LessThan(t, elapsedNew, elapsed)
		case *fileStore:
			// Bigger gains for filestore.
			require_LessThan(t, elapsedNew*10, elapsed)
		}
	}
}

type fastProdLogger struct {
	DummyLogger
	gotIt chan struct{}
}

func (l *fastProdLogger) Debugf(format string, args ...any) {
	msg := fmt.Sprintf(format, args...)
	if strings.Contains(msg, "fast producer") {
		select {
		case l.gotIt <- struct{}{}:
		default:
		}
	}
}

func TestNoRaceNoFastProducerStall(t *testing.T) {
	tmpl := `
		listen: "127.0.0.1:-1"
		no_fast_producer_stall: %s
	`
	conf := createConfFile(t, []byte(fmt.Sprintf(tmpl, "true")))
	s, _ := RunServerWithConfig(conf)
	defer s.Shutdown()

	l := &fastProdLogger{gotIt: make(chan struct{}, 1)}
	s.SetLogger(l, true, false)

	ncSlow := natsConnect(t, s.ClientURL())
	defer ncSlow.Close()
	natsSub(t, ncSlow, "foo", func(_ *nats.Msg) {})
	natsFlush(t, ncSlow)

	nc := natsConnect(t, s.ClientURL())
	defer nc.Close()
	traceSub := natsSubSync(t, nc, "my.trace.subj")
	natsFlush(t, nc)

	ncProd := natsConnect(t, s.ClientURL())
	defer ncProd.Close()

	payload := make([]byte, 256)

	cid, err := ncSlow.GetClientID()
	require_NoError(t, err)
	c := s.GetClient(cid)
	require_True(t, c != nil)

	wg := sync.WaitGroup{}
	pub := func() {
		wg.Add(1)
		go func() {
			defer wg.Done()

			msg := nats.NewMsg("foo")
			msg.Header.Set(MsgTraceDest, traceSub.Subject)
			msg.Data = payload
			ncProd.PublishMsg(msg)
		}()
	}

	checkTraceMsg := func(err string) {
		t.Helper()
		var e MsgTraceEvent
		traceMsg := natsNexMsg(t, traceSub, time.Second)
		json.Unmarshal(traceMsg.Data, &e)
		egresses := e.Egresses()
		require_Equal(t, len(egresses), 1)
		eg := egresses[0]
		require_Equal(t, eg.CID, cid)
		if err != _EMPTY_ {
			require_Contains(t, eg.Error, err)
		} else {
			require_Equal(t, eg.Error, _EMPTY_)
		}
	}

	// Artificially set a stall channel.
	c.mu.Lock()
	c.out.stc = make(chan struct{})
	c.mu.Unlock()

	// Publish a message, it should not stall the producer.
	pub()
	// Now  make sure we did not get any fast producer debug statements.
	select {
	case <-l.gotIt:
		t.Fatal("Got debug logs about fast producer")
	case <-time.After(250 * time.Millisecond):
		// OK!
	}
	wg.Wait()

	checkTraceMsg(errMsgTraceFastProdNoStall)

	// Now we will conf reload to enable fast producer stalling.
	reloadUpdateConfig(t, s, conf, fmt.Sprintf(tmpl, "false"))

	// Publish, this time the prod should be stalled.
	pub()
	select {
	case <-l.gotIt:
		// OK!
	case <-time.After(500 * time.Millisecond):
		t.Fatal("Timed-out waiting for a warning")
	}
	wg.Wait()

	// Should have been delivered to the trace subscription.
	checkTraceMsg(_EMPTY_)
}

func TestNoRaceProducerStallLimits(t *testing.T) {
	tmpl := `
		listen: "127.0.0.1:-1"
	`
	conf := createConfFile(t, []byte(tmpl))
	s, _ := RunServerWithConfig(conf)
	defer s.Shutdown()

	ncSlow := natsConnect(t, s.ClientURL(), nats.Name("slow"))
	defer ncSlow.Close()
	natsSub(t, ncSlow, "foo", func(m *nats.Msg) { m.Respond([]byte("42")) })
	natsFlush(t, ncSlow)

	ncProd := natsConnect(t, s.ClientURL(), nats.Name("fast"))
	defer ncProd.Close()

	cid, err := ncSlow.GetClientID()
	require_NoError(t, err)
	c := s.GetClient(cid)
	require_True(t, c != nil)

	// Artificially set a stall channel on the subscriber.
	c.mu.Lock()
	c.out.stc = make(chan struct{})
	c.mu.Unlock()

	start := time.Now()
	_, err = ncProd.Request("foo", []byte("HELLO"), time.Second)
	elapsed := time.Since(start)
	require_NoError(t, err)

	// This should have not cleared on its own but should have between min and max pause.
	require_True(t, elapsed >= stallClientMinDuration)
	require_LessThan(t, elapsed, stallClientMaxDuration+5*time.Millisecond)

	// Now test total maximum by loading up a bunch of requests and measuring the last one.
	// Artificially set a stall channel again on the subscriber.
	c.mu.Lock()
	c.out.stc = make(chan struct{})
	// This will prevent us from clearing the stc.
	c.out.pb = c.out.mp/4*3 + 100
	c.mu.Unlock()

	for i := 0; i < 10; i++ {
		err = ncProd.PublishRequest("foo", "bar", []byte("HELLO"))
		require_NoError(t, err)
	}
	start = time.Now()
	_, err = ncProd.Request("foo", []byte("HELLO"), time.Second)
	elapsed = time.Since(start)
	require_NoError(t, err)

	require_True(t, elapsed >= stallTotalAllowed)
	// Should always be close to totalAllowed (e.g. 10ms), but if you run a lot of them in one go can bump up
	// just past it, hence the Max setting below to avoid a flapper.
	require_LessThan(t, elapsed, stallTotalAllowed+20*time.Millisecond)

	var tc *client
	s.mu.Lock()
	for _, client := range s.clients {
		client.mu.Lock()
		if client.opts.Name == "slow" {
			tc = client
			client.mu.Unlock()
			break
		}
		client.mu.Unlock()
	}
	s.mu.Unlock()

	// Verify counter was incremented.
	totalStalls := atomic.LoadInt64(&tc.stalls)
	if totalStalls == 0 {
		t.Fatalf("Expected stalls count to be incremented, got: %d", totalStalls)
	}

	// Verify this shows up in Connz monitoring.
	pconns := pollConnz(t, s, 1, "", nil)
	var connzStalls int64
	for _, conn := range pconns.Conns {
		if conn.Cid == tc.cid {
			connzStalls = conn.Stalls
			break
		}
	}
	if connzStalls < 1 {
		t.Fatalf("Expected connz stalls count to be incremented, got: %d", connzStalls)
	}
	if s.NumStalledClients() < 1 {
		t.Fatalf("Expected total stalled clients to be incremented, got: %d", s.NumStalledClients())
	}
}

func TestNoRaceJetStreamClusterConsumerDeleteInterestPolicyPerf(t *testing.T) {
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

	// Create 3 consumers. 1 Ack explicit, 1 AckAll and 1 AckNone
	_, err = js.AddConsumer("TEST", &nats.ConsumerConfig{
		Durable:   "C1",
		AckPolicy: nats.AckExplicitPolicy,
	})
	require_NoError(t, err)

	_, err = js.AddConsumer("TEST", &nats.ConsumerConfig{
		Durable:   "C2",
		AckPolicy: nats.AckAllPolicy,
	})
	require_NoError(t, err)

	_, err = js.AddConsumer("TEST", &nats.ConsumerConfig{
		Durable:   "C3",
		AckPolicy: nats.AckNonePolicy,
	})
	require_NoError(t, err)

	for i := 0; i < 500_000; i++ {
		js.PublishAsync("foo.bar", []byte("ok"))

		// Confirm batch.
		if i%1000 == 0 {
			select {
			case <-js.PublishAsyncComplete():
			case <-time.After(5 * time.Second):
				t.Fatalf("Did not receive completion signal")
			}
		}
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
	expectedStreamMsgs(500_000)

	// For C1 grab 100 and ack evens.
	sub, err := js.PullSubscribe("foo.bar", "C1")
	require_NoError(t, err)
	msgs, err := sub.Fetch(100)
	require_NoError(t, err)
	require_Equal(t, len(msgs), 100)
	for _, m := range msgs {
		meta, _ := m.Metadata()
		if meta.Sequence.Stream%2 == 0 {
			require_NoError(t, m.AckSync())
		}
	}

	// For C2 grab 500 and ack 100.
	sub, err = js.PullSubscribe("foo.bar", "C2")
	require_NoError(t, err)
	msgs, err = sub.Fetch(500)
	require_NoError(t, err)
	require_Equal(t, len(msgs), 500)
	require_NoError(t, msgs[99].AckSync())

	// Simulate stream viewer, get first 10 from C3
	sub, err = js.PullSubscribe("foo.bar", "C3")
	require_NoError(t, err)
	msgs, err = sub.Fetch(10)
	require_NoError(t, err)
	require_Equal(t, len(msgs), 10)

	expectedStreamMsgs(499_995)

	// This test would flake depending on if mset.checkInterestState already ran or not.
	// Manually call it here, because consumers don't retry removing messages below their ack floor.
	for _, s := range c.servers {
		acc, err := s.lookupAccount(globalAccountName)
		require_NoError(t, err)
		mset, err := acc.lookupStream("TEST")
		require_NoError(t, err)
		mset.checkInterestState()
	}

	// Before fix this was in the seconds. All the while the stream is locked.
	// This should be short now, but messages might not be cleaned up.
	start := time.Now()
	err = js.DeleteConsumer("TEST", "C3")
	require_NoError(t, err)
	if elapsed := time.Since(start); elapsed > 100*time.Millisecond {
		t.Fatalf("Deleting AckNone consumer took too long: %v", elapsed)
	}

	expectedStreamMsgs(499_950)

	// Now do AckAll
	start = time.Now()
	err = js.DeleteConsumer("TEST", "C2")
	require_NoError(t, err)
	if elapsed := time.Since(start); elapsed > 100*time.Millisecond {
		t.Fatalf("Deleting AckAll consumer took too long: %v", elapsed)
	}

	expectedStreamMsgs(499_950)

	// Now do AckExplicit
	start = time.Now()
	err = js.DeleteConsumer("TEST", "C1")
	require_NoError(t, err)
	if elapsed := time.Since(start); elapsed > 100*time.Millisecond {
		t.Fatalf("Deleting AckExplicit consumer took too long: %v", elapsed)
	}

	expectedStreamMsgs(0)
}

func TestNoRaceJetStreamClusterConsumerDeleteInterestPolicyUniqueFiltersPerf(t *testing.T) {
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

	// Create 2 consumers. 1 Ack explicit, 1 AckNone
	_, err = js.AddConsumer("TEST", &nats.ConsumerConfig{
		Durable:        "C0",
		AckPolicy:      nats.AckExplicitPolicy,
		FilterSubjects: []string{"foo.0"},
	})
	require_NoError(t, err)

	_, err = js.AddConsumer("TEST", &nats.ConsumerConfig{
		Durable:        "C1",
		AckPolicy:      nats.AckNonePolicy,
		FilterSubjects: []string{"foo.1"},
	})
	require_NoError(t, err)

	for i := 0; i < 500_000; i++ {
		subject := fmt.Sprintf("foo.%d", i%2)
		js.PublishAsync(subject, []byte("ok"))

		// Confirm batch.
		if i%1000 == 0 {
			select {
			case <-js.PublishAsyncComplete():
			case <-time.After(5 * time.Second):
				t.Fatalf("Did not receive completion signal")
			}
		}
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

	expectedStreamMsgs(500_000)

	// For C0 grab 100 and ack them.
	sub, err := js.PullSubscribe("foo.0", "C0")
	require_NoError(t, err)
	msgs, err := sub.Fetch(100)
	require_NoError(t, err)
	require_Equal(t, len(msgs), 100)
	for _, msg := range msgs {
		require_NoError(t, msg.AckSync())
	}

	expectedStreamMsgs(499_900)

	start := time.Now()
	err = js.DeleteConsumer("TEST", "C1")
	require_NoError(t, err)
	if elapsed := time.Since(start); elapsed > 100*time.Millisecond {
		t.Fatalf("Deleting AckNone consumer took too long: %v", elapsed)
	}

	expectedStreamMsgs(499_800)

	start = time.Now()
	err = js.DeleteConsumer("TEST", "C0")
	require_NoError(t, err)
	if elapsed := time.Since(start); elapsed > 100*time.Millisecond {
		t.Fatalf("Deleting AckExplicit consumer took too long: %v", elapsed)
	}
	expectedStreamMsgs(0)
}

func TestNoRaceFileStorePurgeExAsyncTombstones(t *testing.T) {
	fs, err := newFileStore(
		FileStoreConfig{StoreDir: t.TempDir()},
		StreamConfig{Name: "zzz", Subjects: []string{"*.*"}, Storage: FileStorage})
	require_NoError(t, err)
	defer fs.Stop()

	msg := []byte("zzz")

	fs.StoreMsg("foo.A", nil, msg, 0)
	fs.StoreMsg("foo.B", nil, msg, 0)
	for i := 0; i < 500; i++ {
		fs.StoreMsg("foo.C", nil, msg, 0)
	}
	fs.StoreMsg("foo.D", nil, msg, 0)

	// Load all blocks to avoid that being a factor in timing.
	fs.mu.RLock()
	for _, mb := range fs.blks {
		mb.loadMsgs()
	}
	fs.mu.RUnlock()

	// Now purge 1 that is not the first message and take note of time.
	// Since we are loaded this should mostly be the time to write / flush tombstones.
	start := time.Now()
	n, err := fs.PurgeEx("foo.B", 0, 0)
	elapsed := time.Since(start)
	require_NoError(t, err)
	require_Equal(t, n, 1)

	start = time.Now()
	n, err = fs.PurgeEx("foo.C", 0, 0)
	elapsed2 := time.Since(start)
	require_NoError(t, err)
	require_Equal(t, n, 500)

	// If we are flushing for each tombstone the second elapsed time will be a larger multiple of the single message purge.
	// In testing this is like >200x
	// With async and flush for all tombstones will be ~30x
	require_True(t, elapsed*50 > elapsed2)
}

// Chck that we do not leak any go routines from access time optimization.
func TestNoRaceAccessTimeLeakCheck(t *testing.T) {
	time.Sleep(time.Second)
	ngrp := runtime.NumGoroutine()

	s := RunBasicJetStreamServer(t)
	defer s.Shutdown()

	nc, js := jsClientConnect(t, s)
	defer nc.Close()

	_, err := js.AddStream(&nats.StreamConfig{
		Name:     "TEST",
		Subjects: []string{"foo"},
	})
	require_NoError(t, err)

	_, err = js.Publish("foo", []byte("Hello World"))
	require_NoError(t, err)

	// Close down client and server.
	nc.Close()
	s.Shutdown()
	s.WaitForShutdown()

	checkFor(t, 5*time.Second, time.Second, func() error {
		if ngra := runtime.NumGoroutine(); ngrp != ngra {
			return fmt.Errorf("expected %d, got %d", ngrp, ngra)
		}
		return nil
	})
}

func TestNoRaceQuickMultipleConfigReloadRemoteLeafNoDuplicate(t *testing.T) {
	// This test is to demonstrate that if we perform quicj disable/enable,
	// or remove/add of remote leafnodes, we don't end up with multiple
	// "connect to remote" go routines for the same remote, leading to duplicate
	// remote leafnodes.

	conf1 := createConfFile(t, []byte(`
		port: -1
		server_name: "A"
		leafnodes {
			listen: "127.0.0.1:-1"
		}
	`))
	s1, o1 := RunServerWithConfig(conf1)
	defer s1.Shutdown()
	l := &captureErrorLogger{errCh: make(chan string, 100)}
	s1.SetLogger(l, false, false)

	tmpl2 := `
		port: -1
		server_name: "B"
		leafnodes {
			reconnect_interval: "50ms"
			remotes [
				%s
			]
		}
	`

	configWithAdded := fmt.Appendf(nil, tmpl2,
		fmt.Sprintf(`{url: "nats://127.0.0.1:%d"}`, o1.LeafNode.Port))
	configWithRemoved := fmt.Appendf(nil, tmpl2, _EMPTY_)
	configWithDisabled := fmt.Appendf(nil, tmpl2,
		fmt.Sprintf(`{url: "nats://127.0.0.1:%d", disabled: true}`, o1.LeafNode.Port))
	configWithEnabled := fmt.Appendf(nil, tmpl2,
		fmt.Sprintf(`{url: "nats://127.0.0.1:%d", disabled: false}`, o1.LeafNode.Port))

	conf2 := createConfFile(t, configWithEnabled)
	s2, _ := RunServerWithConfig(conf2)
	defer s2.Shutdown()

	checkLeafNodeConnectedCount(t, s2, 1)

	for _, test := range []struct {
		name     string
		disabled bool
	}{
		{"disabled_enabled", true},
		{"removed_added", false},
	} {
		t.Run(test.name, func(t *testing.T) {
			var disabledOrRemoved bool
			// We pick a number that will end the loop with enabled/added remote.
			for range 20 {
				disabledOrRemoved = !disabledOrRemoved
				var config []byte
				if test.disabled {
					if disabledOrRemoved {
						config = configWithDisabled
					} else {
						config = configWithEnabled
					}
				} else {
					if disabledOrRemoved {
						config = configWithRemoved
					} else {
						config = configWithAdded
					}
				}
				err := os.WriteFile(conf2, config, 0666)
				require_NoError(t, err)
				deadline := time.Now().Add(1500 * time.Millisecond)
				for time.Now().Before(deadline) {
					err = s2.Reload()
					if err == nil || !strings.Contains(err.Error(), "try again") {
						break
					}
				}
				require_NoError(t, err)
			}
			checkLeafNodeConnectedCount(t, s2, 1)

			for {
				select {
				case errTxt := <-l.errCh:
					if strings.Contains(errTxt, DuplicateRemoteLeafnodeConnection.String()) {
						t.Fatalf("Duplicates detected: %s", errTxt)
					}
				default:
					// No error, all ok!
					return
				}
			}
		})
	}
}

// This test observes buffers being returned to the block buffer sync.Pools.
// Under the race detector sync.Pool randomly drops puts by design, so it
// cannot run with -race.
func TestNoRaceFileStoreWeakCacheGCRecyclesBuf(t *testing.T) {
	// Run everything on a single P so all sync.Pool puts/gets use the same
	// P-local pool: buffers recycled by the GC cleanup goroutine would
	// otherwise often land in another P's private slot, which Get on this
	// goroutine can never steal from and the test couldn't observe them.
	defer runtime.GOMAXPROCS(runtime.GOMAXPROCS(1))

	// All cache buffers in this test come from the tiny block pool, identify
	// them by their backing array so we can spot them in the pool.
	bufArr := func(buf []byte) *[defaultTinyBlockSize]byte {
		return (*[defaultTinyBlockSize]byte)(buf[0:defaultTinyBlockSize])
	}
	// Wait for the GC cleanup to recycle the buffer back into the pool.
	// GC and polling must be interleaved carefully: every GC cycle also
	// drains the sync.Pools, so polling Get while triggering GCs could
	// evict the recycled buffer before we get a chance to observe it.
	expectRecycled := func(t *testing.T, arr *[defaultTinyBlockSize]byte) {
		t.Helper()
		for attempts := 0; attempts < 10; attempts++ {
			runtime.GC()
			// Poll without triggering more GCs so the pool isn't drained
			// while we look for the buffer.
			for range 20 {
				if blkPoolTiny.Get().(*[defaultTinyBlockSize]byte) == arr {
					return
				}
				time.Sleep(5 * time.Millisecond)
			}
		}
		t.Fatalf("buffer was not recycled into the pool")
	}
	// Ensure the buffer does not (re)appear in the pool through a GC cleanup.
	expectNotRecycled := func(t *testing.T, arr *[defaultTinyBlockSize]byte) {
		t.Helper()
		for attempts := 0; attempts < 3; attempts++ {
			runtime.GC()
			// Give a (faulty) cleanup time to run, then poll without
			// triggering more GCs so we would reliably see its recycle.
			time.Sleep(20 * time.Millisecond)
			for range 20 {
				if blkPoolTiny.Get().(*[defaultTinyBlockSize]byte) == arr {
					t.Fatalf("buffer was recycled into the pool, but should not have been")
				}
				time.Sleep(2 * time.Millisecond)
			}
		}
	}

	newTestStore := func(t *testing.T) *msgBlock {
		t.Helper()
		fs, err := newFileStore(
			FileStoreConfig{StoreDir: t.TempDir(), BlockSize: 8192},
			StreamConfig{Name: "zzz", Storage: FileStorage, Subjects: []string{"foo"}},
		)
		require_NoError(t, err)
		t.Cleanup(func() { fs.Stop() })

		for range 4 {
			_, _, err = fs.StoreMsg("foo", nil, []byte("hello"), 0)
			require_NoError(t, err)
		}
		require_NoError(t, fs.FlushAllPending())

		fs.mu.Lock()
		mb := fs.lmb
		fs.mu.Unlock()
		return mb
	}

	t.Run("recycledOnGC", func(t *testing.T) {
		arr := func() *[defaultTinyBlockSize]byte {
			c := &cache{buf: getMsgBlockBuf(1)}
			c.registerRecycle()
			require_True(t, c.clean != (runtime.Cleanup{}))
			return bufArr(c.buf)
		}()
		// The cache is unreachable now, GC should recycle its buffer.
		expectRecycled(t, arr)
	})

	t.Run("stopRecycle", func(t *testing.T) {
		arr := func() *[defaultTinyBlockSize]byte {
			c := &cache{buf: getMsgBlockBuf(1)}
			c.registerRecycle()
			c.stopRecycle()
			require_True(t, c.clean == (runtime.Cleanup{}))
			return bufArr(c.buf)
		}()
		expectNotRecycled(t, arr)
	})

	t.Run("registerRecycleTwice", func(t *testing.T) {
		arr := func() *[defaultTinyBlockSize]byte {
			c := &cache{buf: getMsgBlockBuf(1)}
			// Registering again must cancel the previous registration,
			// otherwise the buffer would be recycled twice.
			c.registerRecycle()
			c.registerRecycle()
			return bufArr(c.buf)
		}()
		expectRecycled(t, arr)
		expectNotRecycled(t, arr)
	})

	t.Run("noRecycleNRA", func(t *testing.T) {
		arr := func() *[defaultTinyBlockSize]byte {
			c := &cache{buf: getMsgBlockBuf(1), nra: true}
			c.registerRecycle()
			require_True(t, c.clean == (runtime.Cleanup{}))
			return bufArr(c.buf)
		}()
		expectNotRecycled(t, arr)
	})

	t.Run("noRecycleEmptyBuf", func(t *testing.T) {
		c := &cache{}
		c.registerRecycle()
		require_True(t, c.clean == (runtime.Cleanup{}))
		// Stopping without a registration, or twice, must not panic.
		c.stopRecycle()
		c.stopRecycle()
		// Neither must registering on a nil cache.
		(*cache)(nil).registerRecycle()
	})

	t.Run("weakWriteCacheGC", func(t *testing.T) {
		mb := newTestStore(t)
		arr := func() *[defaultTinyBlockSize]byte {
			mb.mu.Lock()
			defer mb.mu.Unlock()
			// The write path registered the recycle cleanup when it grew
			// the write cache buffer from the pool.
			require_NotNil(t, mb.cache)
			require_True(t, mb.cache.clean != (runtime.Cleanup{}))
			require_Equal(t, cap(mb.cache.buf), defaultTinyBlockSize)
			arr := bufArr(mb.cache.buf)
			// Drop the strong references so only the weak pointer remains.
			mb.finishedWithCache()
			require_True(t, mb.cache == nil)
			mb.ecache.Weaken()
			require_NotNil(t, mb.ecache.Value())
			return arr
		}()

		// GC collects the weakly referenced cache and recycles its buffer.
		expectRecycled(t, arr)
		mb.mu.Lock()
		defer mb.mu.Unlock()
		require_True(t, mb.ecache.Value() == nil)
	})

	t.Run("weakLoadedCacheGC", func(t *testing.T) {
		mb := newTestStore(t)
		arr := func() *[defaultTinyBlockSize]byte {
			mb.mu.Lock()
			defer mb.mu.Unlock()
			// Reload the block from disk so indexCacheBuf registers the cleanup.
			mb.clearCache()
			require_True(t, mb.cache == nil)
			require_NoError(t, mb.loadMsgsWithLock())
			require_NotNil(t, mb.cache)
			require_True(t, mb.cache.clean != (runtime.Cleanup{}))
			require_Equal(t, cap(mb.cache.buf), defaultTinyBlockSize)
			arr := bufArr(mb.cache.buf)
			mb.finishedWithCache()
			require_True(t, mb.cache == nil)
			mb.ecache.Weaken()
			require_NotNil(t, mb.ecache.Value())
			return arr
		}()

		expectRecycled(t, arr)
		mb.mu.Lock()
		defer mb.mu.Unlock()
		require_True(t, mb.ecache.Value() == nil)
	})

	t.Run("clearCacheNoDoubleRecycle", func(t *testing.T) {
		mb := newTestStore(t)
		arr := func() *[defaultTinyBlockSize]byte {
			mb.mu.Lock()
			defer mb.mu.Unlock()
			require_NotNil(t, mb.cache)
			require_Equal(t, cap(mb.cache.buf), defaultTinyBlockSize)
			arr := bufArr(mb.cache.buf)
			mb.clearCache()
			require_True(t, mb.cache == nil)
			require_True(t, mb.ecache.Value() == nil)
			return arr
		}()

		// clearCache recycled the buffer into the pool explicitly once...
		expectRecycled(t, arr)
		// ...and GC of the disused cache must not recycle it a second time.
		expectNotRecycled(t, arr)
	})

	t.Run("writeCacheHandoffNoRecycle", func(t *testing.T) {
		mb := newTestStore(t)
		arr, buf := func() (*[defaultTinyBlockSize]byte, []byte) {
			mb.mu.Lock()
			defer mb.mu.Unlock()
			require_NotNil(t, mb.cache)
			require_Equal(t, cap(mb.cache.buf), defaultTinyBlockSize)
			arr := bufArr(mb.cache.buf)
			// Pretend the block was never loaded for reads, so the write cache
			// buffer is handed off for reuse by the next write block.
			mb.llts = 0
			buf := mb.tryExpireWriteCache()
			require_True(t, buf != nil)
			require_True(t, bufArr(buf) == arr)
			// Drop any remaining references to the old cache object.
			mb.cache = nil
			mb.ecache.Set(nil)
			return arr, buf
		}()

		// The handed-off buffer is still in use, so GC of the old cache
		// object must not recycle it into the pool.
		expectNotRecycled(t, arr)
		runtime.KeepAlive(buf)
	})

	t.Run("expiredWriteCacheNoHandoffRecycles", func(t *testing.T) {
		mb := newTestStore(t)
		arr := func() *[defaultTinyBlockSize]byte {
			mb.mu.Lock()
			defer mb.mu.Unlock()
			require_NotNil(t, mb.cache)
			require_Equal(t, cap(mb.cache.buf), defaultTinyBlockSize)
			arr := bufArr(mb.cache.buf)
			// Pretend the block was read, but long ago. The cache will
			// expire, but the stale read blocks the buffer handoff.
			mb.llts = 1
			buf := mb.tryExpireWriteCache()
			require_True(t, buf == nil)
			require_True(t, mb.cache == nil)
			return arr
		}()

		// The buffer was recycled into the pool explicitly once...
		expectRecycled(t, arr)
		// ...and GC of the disused cache must not recycle it a second time.
		expectNotRecycled(t, arr)
	})
}
