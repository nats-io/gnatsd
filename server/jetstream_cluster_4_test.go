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

//go:build !skip_js_tests && !skip_js_cluster_tests_4

package server

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"maps"
	"math"
	"math/big"
	"math/rand/v2"
	"os"
	"path"
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

	"github.com/nats-io/jwt/v2"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nuid"
)

func TestJetStreamClusterWorkQueueStreamDiscardNewDesync(t *testing.T) {
	t.Run("max msgs", func(t *testing.T) {
		testJetStreamClusterWorkQueueStreamDiscardNewDesync(t, &nats.StreamConfig{
			Name:      "WQTEST_MM",
			Subjects:  []string{"messages.*"},
			Replicas:  3,
			MaxAge:    10 * time.Minute,
			MaxMsgs:   100,
			Retention: nats.WorkQueuePolicy,
			Discard:   nats.DiscardNew,
		})
	})
	t.Run("max bytes", func(t *testing.T) {
		testJetStreamClusterWorkQueueStreamDiscardNewDesync(t, &nats.StreamConfig{
			Name:      "WQTEST_MB",
			Subjects:  []string{"messages.*"},
			Replicas:  3,
			MaxAge:    10 * time.Minute,
			MaxBytes:  1 * 1024 * 1024,
			Retention: nats.WorkQueuePolicy,
			Discard:   nats.DiscardNew,
		})
	})
}

func testJetStreamClusterWorkQueueStreamDiscardNewDesync(t *testing.T, sc *nats.StreamConfig) {
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
	    jetstream = enabled
	    users = [
	      { user: js, pass: js }
	    ]
	  }
	}`
	c := createJetStreamClusterWithTemplate(t, conf, sc.Name, 3)
	defer c.shutdown()

	nc, js := jsClientConnect(t, c.randomServer())
	defer nc.Close()

	cnc, cjs := jsClientConnect(t, c.randomServer())
	defer cnc.Close()

	_, err := js.AddStream(sc)
	require_NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	psub, err := cjs.PullSubscribe("messages.*", "consumer")
	require_NoError(t, err)

	stepDown := func() {
		_, err = nc.Request(fmt.Sprintf(JSApiStreamLeaderStepDownT, sc.Name), nil, time.Second)
	}

	// Messages will be produced and consumed in parallel, then once there are
	// enough errors a leader election will be triggered.
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
					// The consumer will continue to timeout here eventually.
					continue
				}
				for _, msg := range msgs {
					received++
					receivedMap[msg.Subject] = msg
					msg.Ack()
				}
			}
		}
	}()

	shouldDrop := make(map[string]error)
	wg.Add(1)
	go func() {
		payload := []byte(strings.Repeat("A", 1024))
		tick := time.NewTicker(1 * time.Millisecond)
		for i := 1; ; i++ {
			select {
			case <-ctx.Done():
				wg.Done()
				return
			case <-tick.C:
				subject := fmt.Sprintf("messages.%d", i)
				_, err := js.Publish(subject, payload, nats.RetryAttempts(0))
				// Capture the messages that have failed.
				if err != nil {
					// Unless it was a timeout, we can't be sure if the message was received,
					// and we just didn't get a response.
					if errors.Is(err, nats.ErrTimeout) {
						break
					}
					errCh <- err
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
		case err = <-errCh:
			errCount++
			if errCount%500 == 0 {
				stepDown()
			} else if errCount >= 2000 {
				// Stop both producing and consuming.
				cancel()
				break Setup
			}
		case <-time.After(5 * time.Second):
			// Unblock the test and continue.
			cancel()
			break Setup
		}
	}

	// Both goroutines should be exiting now..
	wg.Wait()

	// Let acks propagate for stream checks.
	time.Sleep(250 * time.Millisecond)

	// Check messages that ought to have been dropped.
	for subject := range receivedMap {
		found, ok := shouldDrop[subject]
		if ok {
			t.Errorf("Should have dropped message published on %q since got error: %v", subject, found)
		}
	}
}

// https://github.com/nats-io/nats-server/issues/5071
func TestJetStreamClusterStreamPlacementDistribution(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "R3S", 5)
	defer c.shutdown()

	s := c.randomNonLeader()
	nc, js := jsClientConnect(t, s)
	defer nc.Close()

	for i := 1; i <= 10; i++ {
		_, err := js.AddStream(&nats.StreamConfig{
			Name:     fmt.Sprintf("TEST:%d", i),
			Subjects: []string{fmt.Sprintf("foo.%d.*", i)},
			Replicas: 3,
		})
		require_NoError(t, err)
	}

	// 10 streams, 3 replicas div 5 servers.
	expectedStreams := 10 * 3 / 5
	for _, s := range c.servers {
		jsz, err := s.Jsz(nil)
		require_NoError(t, err)
		require_Equal(t, jsz.Streams, expectedStreams)
	}
}

func TestJetStreamClusterSourceWorkingQueueWithLimit(t *testing.T) {
	const (
		totalMsgs        = 300
		maxMsgs          = 100
		maxBytes         = maxMsgs * 100
		msgPayloadFormat = "%0100d" // %0100d is 100 bytes. Must match payload value above.
	)
	c := createJetStreamClusterExplicit(t, "WQ3", 3)
	defer c.shutdown()

	nc, js := jsClientConnect(t, c.randomServer())
	defer nc.Close()

	_, err := js.AddStream(&nats.StreamConfig{Name: "test", Subjects: []string{"test"}, Replicas: 3})
	require_NoError(t, err)

	_, err = js.AddStream(&nats.StreamConfig{Name: "wq", MaxMsgs: maxMsgs, Discard: nats.DiscardNew, Retention: nats.WorkQueuePolicy,
		Sources: []*nats.StreamSource{{Name: "test"}}, Replicas: 3})
	require_NoError(t, err)

	_, err = js.AddStream(&nats.StreamConfig{Name: "wq2", MaxBytes: maxBytes, Discard: nats.DiscardNew, Retention: nats.WorkQueuePolicy,
		Sources: []*nats.StreamSource{{Name: "test"}}, Replicas: 3})
	require_NoError(t, err)

	sendBatch := func(subject string, n int) {
		for i := 0; i < n; i++ {
			_, err = js.Publish(subject, []byte(fmt.Sprintf(msgPayloadFormat, i)))
			require_NoError(t, err)
		}
	}

	f := func(ss *nats.Subscription, done chan bool) {
		for i := 0; i < totalMsgs; i++ {
			m, err := ss.Fetch(1, nats.MaxWait(3*time.Second))
			require_NoError(t, err)
			p, err := strconv.Atoi(string(m[0].Data))
			require_NoError(t, err)
			require_Equal(t, p, i)
			time.Sleep(11 * time.Millisecond)
			err = m[0].Ack()
			require_NoError(t, err)
		}
		done <- true
	}

	// Populate the sourced stream.
	sendBatch("test", totalMsgs)

	checkFor(t, 3*time.Second, 250*time.Millisecond, func() error {
		si, err := js.StreamInfo("wq")
		require_NoError(t, err)
		if si.State.Msgs != maxMsgs {
			return fmt.Errorf("expected %d msgs on stream wq, got state: %+v", maxMsgs, si.State)
		}
		return nil
	})

	checkFor(t, 3*time.Second, 250*time.Millisecond, func() error {
		si, err := js.StreamInfo("wq2")
		require_NoError(t, err)
		if si.State.Bytes > maxBytes {
			return fmt.Errorf("expected no more than %d bytes on stream wq2, got state: %+v", maxBytes, si.State)
		}
		return nil
	})

	_, err = js.AddConsumer("wq", &nats.ConsumerConfig{Durable: "wqc", FilterSubject: "test", AckPolicy: nats.AckExplicitPolicy})
	require_NoError(t, err)

	ss1, err := js.PullSubscribe("test", "wqc", nats.Bind("wq", "wqc"))
	require_NoError(t, err)

	var doneChan1 = make(chan bool)
	go f(ss1, doneChan1)

	checkFor(t, 10*time.Second, 250*time.Millisecond, func() error {
		si, err := js.StreamInfo("wq")
		require_NoError(t, err)
		if si.State.Msgs > 0 && si.State.Msgs <= maxMsgs {
			return fmt.Errorf("expected 0 msgs on stream wq, got: %d", si.State.Msgs)
		} else if si.State.Msgs > maxMsgs {
			t.Fatalf("got more than our %d message limit on stream wq: %+v", maxMsgs, si.State)
		}

		return nil
	})

	select {
	case <-doneChan1:
		ss1.Drain()
	case <-time.After(10 * time.Second):
		t.Fatalf("Did not receive completion signal")
	}

	_, err = js.AddConsumer("wq2", &nats.ConsumerConfig{Durable: "wqc", FilterSubject: "test", AckPolicy: nats.AckExplicitPolicy})
	require_NoError(t, err)

	ss2, err := js.PullSubscribe("test", "wqc", nats.Bind("wq2", "wqc"))
	require_NoError(t, err)

	var doneChan2 = make(chan bool)
	go f(ss2, doneChan2)

	checkFor(t, 10*time.Second, 250*time.Millisecond, func() error {
		si, err := js.StreamInfo("wq2")
		require_NoError(t, err)
		if si.State.Bytes > 0 && si.State.Bytes <= maxBytes {
			return fmt.Errorf("expected 0 bytes on stream wq2, got: %+v", si.State)
		} else if si.State.Bytes > maxBytes {
			t.Fatalf("got more than our %d bytes limit on stream wq2: %+v", maxMsgs, si.State)
		}

		return nil
	})

	select {
	case <-doneChan2:
		ss2.Drain()
	case <-time.After(20 * time.Second):
		t.Fatalf("Did not receive completion signal")
	}
}

func TestJetStreamClusterConsumerPauseViaConfig(t *testing.T) {
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

	jsTestPause_CreateOrUpdateConsumer(t, nc, ActionCreate, "TEST", ConsumerConfig{
		Name:     "my_consumer",
		Replicas: 3,
	})

	sub, err := js.PullSubscribe("foo", "", nats.Bind("TEST", "my_consumer"))
	require_NoError(t, err)

	stepdown := func() {
		t.Helper()
		_, err := nc.Request(fmt.Sprintf(JSApiConsumerLeaderStepDownT, "TEST", "my_consumer"), nil, time.Second)
		require_NoError(t, err)
		c.waitOnConsumerLeader(globalAccountName, "TEST", "my_consumer")
	}

	publish := func(wait time.Duration) {
		t.Helper()
		for i := 0; i < 5; i++ {
			_, err = js.Publish("foo", []byte("OK"))
			require_NoError(t, err)
		}
		msgs, err := sub.Fetch(5, nats.MaxWait(wait))
		require_NoError(t, err)
		require_Equal(t, len(msgs), 5)
	}

	// This should be fast as there's no deadline.
	publish(time.Second)

	// Now we're going to set the deadline.
	deadline := jsTestPause_PauseConsumer(t, nc, "TEST", "my_consumer", time.Now().Add(time.Second*3))
	c.waitOnAllCurrent()

	// It will now take longer than 3 seconds.
	publish(time.Second * 5)
	require_True(t, time.Now().After(deadline))

	// The next set of publishes after the deadline should now be fast.
	publish(time.Second)

	// We'll kick the leader, but since we're after the deadline, this
	// should still be fast.
	stepdown()
	publish(time.Second)

	// Now we're going to do an update and then immediately kick the
	// leader. The pause should still be in effect afterwards.
	deadline = jsTestPause_PauseConsumer(t, nc, "TEST", "my_consumer", time.Now().Add(time.Second*3))
	c.waitOnAllCurrent()
	publish(time.Second * 5)
	require_True(t, time.Now().After(deadline))

	// The next set of publishes after the deadline should now be fast.
	publish(time.Second)
}

func TestJetStreamClusterConsumerPauseViaEndpoint(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "R3S", 3)
	defer c.shutdown()

	nc, js := jsClientConnect(t, c.randomServer())
	defer nc.Close()

	_, err := js.AddStream(&nats.StreamConfig{
		Name:     "TEST",
		Subjects: []string{"push", "pull"},
		Replicas: 3,
	})
	require_NoError(t, err)

	t.Run("PullConsumer", func(t *testing.T) {
		_, err := js.AddConsumer("TEST", &nats.ConsumerConfig{
			Name: "pull_consumer",
		})
		require_NoError(t, err)

		sub, err := js.PullSubscribe("pull", "", nats.Bind("TEST", "pull_consumer"))
		require_NoError(t, err)

		// This should succeed as there's no pause, so it definitely
		// shouldn't take more than a second.
		for i := 0; i < 10; i++ {
			_, err = js.Publish("pull", []byte("OK"))
			require_NoError(t, err)
		}
		msgs, err := sub.Fetch(10, nats.MaxWait(time.Second))
		require_NoError(t, err)
		require_Equal(t, len(msgs), 10)

		// Now we'll pause the consumer for 3 seconds.
		deadline := time.Now().Add(time.Second * 3)
		require_True(t, jsTestPause_PauseConsumer(t, nc, "TEST", "pull_consumer", deadline).Equal(deadline))
		c.waitOnAllCurrent()

		// This should fail as we'll wait for only half of the deadline.
		for i := 0; i < 10; i++ {
			_, err = js.Publish("pull", []byte("OK"))
			require_NoError(t, err)
		}
		_, err = sub.Fetch(10, nats.MaxWait(time.Until(deadline)/2))
		require_Error(t, err, nats.ErrTimeout)

		// This should succeed after a short wait, and when we're done,
		// we should be after the deadline.
		msgs, err = sub.Fetch(10)
		require_NoError(t, err)
		require_Equal(t, len(msgs), 10)
		require_True(t, time.Now().After(deadline))

		// This should succeed as there's no pause, so it definitely
		// shouldn't take more than a second.
		for i := 0; i < 10; i++ {
			_, err = js.Publish("pull", []byte("OK"))
			require_NoError(t, err)
		}
		msgs, err = sub.Fetch(10, nats.MaxWait(time.Second))
		require_NoError(t, err)
		require_Equal(t, len(msgs), 10)

		require_True(t, jsTestPause_PauseConsumer(t, nc, "TEST", "pull_consumer", time.Time{}).Equal(time.Time{}))
		c.waitOnAllCurrent()

		// This should succeed as there's no pause, so it definitely
		// shouldn't take more than a second.
		for i := 0; i < 10; i++ {
			_, err = js.Publish("pull", []byte("OK"))
			require_NoError(t, err)
		}
		msgs, err = sub.Fetch(10, nats.MaxWait(time.Second))
		require_NoError(t, err)
		require_Equal(t, len(msgs), 10)
	})

	t.Run("PushConsumer", func(t *testing.T) {
		ch := make(chan *nats.Msg, 100)
		_, err = js.ChanSubscribe("push", ch, nats.BindStream("TEST"), nats.ConsumerName("push_consumer"))
		require_NoError(t, err)

		// This should succeed as there's no pause, so it definitely
		// shouldn't take more than a second.
		for i := 0; i < 10; i++ {
			_, err = js.Publish("push", []byte("OK"))
			require_NoError(t, err)
		}
		for i := 0; i < 10; i++ {
			msg := require_ChanRead(t, ch, time.Second)
			require_NotEqual(t, msg, nil)
		}

		// Now we'll pause the consumer for 3 seconds.
		deadline := time.Now().Add(time.Second * 3)
		require_True(t, jsTestPause_PauseConsumer(t, nc, "TEST", "push_consumer", deadline).Equal(deadline))
		c.waitOnAllCurrent()

		// This should succeed after a short wait, and when we're done,
		// we should be after the deadline.
		for i := 0; i < 10; i++ {
			_, err = js.Publish("push", []byte("OK"))
			require_NoError(t, err)
		}
		for i := 0; i < 10; i++ {
			msg := require_ChanRead(t, ch, time.Second*5)
			require_NotEqual(t, msg, nil)
			require_True(t, time.Now().After(deadline))
		}

		// This should succeed as there's no pause, so it definitely
		// shouldn't take more than a second.
		for i := 0; i < 10; i++ {
			_, err = js.Publish("push", []byte("OK"))
			require_NoError(t, err)
		}
		for i := 0; i < 10; i++ {
			msg := require_ChanRead(t, ch, time.Second)
			require_NotEqual(t, msg, nil)
		}

		require_True(t, jsTestPause_PauseConsumer(t, nc, "TEST", "push_consumer", time.Time{}).Equal(time.Time{}))
		c.waitOnAllCurrent()

		// This should succeed as there's no pause, so it definitely
		// shouldn't take more than a second.
		for i := 0; i < 10; i++ {
			_, err = js.Publish("push", []byte("OK"))
			require_NoError(t, err)
		}
		for i := 0; i < 10; i++ {
			msg := require_ChanRead(t, ch, time.Second)
			require_NotEqual(t, msg, nil)
		}
	})
}

func TestJetStreamClusterConsumerPauseTimerFollowsLeader(t *testing.T) {
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

	deadline := time.Now().Add(time.Hour)
	jsTestPause_CreateOrUpdateConsumer(t, nc, ActionCreate, "TEST", ConsumerConfig{
		Name:       "my_consumer",
		PauseUntil: &deadline,
		Replicas:   3,
	})

	for i := 0; i < 10; i++ {
		c.waitOnConsumerLeader(globalAccountName, "TEST", "my_consumer")
		c.waitOnAllCurrent()

		for _, s := range c.servers {
			stream, err := s.gacc.lookupStream("TEST")
			require_NoError(t, err)

			consumer := stream.lookupConsumer("my_consumer")
			require_NotEqual(t, consumer, nil)

			isLeader := s.JetStreamIsConsumerLeader(globalAccountName, "TEST", "my_consumer")

			consumer.mu.RLock()
			hasTimer := consumer.uptmr != nil
			consumer.mu.RUnlock()

			require_Equal(t, isLeader, hasTimer)
		}

		_, err = nc.Request(fmt.Sprintf(JSApiConsumerLeaderStepDownT, "TEST", "my_consumer"), nil, maxElectionTimeout)
		require_NoError(t, err)
	}
}

func TestJetStreamClusterConsumerPauseResumeViaEndpoint(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "R3S", 3)
	defer c.shutdown()

	nc, js := jsClientConnect(t, c.randomServer())
	defer nc.Close()

	_, err := js.AddStream(&nats.StreamConfig{
		Name:     "TEST",
		Subjects: []string{"TEST"},
		Replicas: 3,
	})
	require_NoError(t, err)

	_, err = js.AddConsumer("TEST", &nats.ConsumerConfig{
		Name:     "CONSUMER",
		Replicas: 3,
	})
	require_NoError(t, err)

	getConsumerInfo := func() ConsumerInfo {
		var ci ConsumerInfo
		infoResp, err := nc.Request("$JS.API.CONSUMER.INFO.TEST.CONSUMER", nil, time.Second)
		require_NoError(t, err)
		err = json.Unmarshal(infoResp.Data, &ci)
		require_NoError(t, err)
		return ci
	}

	// Ensure we are not paused
	require_False(t, getConsumerInfo().Paused)

	// Use pause advisories to know when pause/resume is applied.
	ch := make(chan *nats.Msg, 10)
	_, err = nc.ChanSubscribe(JSAdvisoryConsumerPausePre+".TEST.CONSUMER", ch)
	require_NoError(t, err)

	// Now we'll pause the consumer for 30 seconds.
	deadline := time.Now().Add(time.Second * 30)
	require_True(t, jsTestPause_PauseConsumer(t, nc, "TEST", "CONSUMER", deadline).Equal(deadline))
	require_ChanRead(t, ch, time.Second*2)
	require_Len(t, len(ch), 0)

	// Ensure the consumer reflects being paused
	require_True(t, getConsumerInfo().Paused)

	subj := fmt.Sprintf("$JS.API.CONSUMER.PAUSE.%s.%s", "TEST", "CONSUMER")
	_, err = nc.Request(subj, nil, time.Second)
	require_NoError(t, err)
	require_ChanRead(t, ch, time.Second*2)
	require_Len(t, len(ch), 0)

	// Ensure the consumer reflects being resumed
	require_False(t, getConsumerInfo().Paused)
}

func TestJetStreamClusterConsumerPauseMetadataRace(t *testing.T) {
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
		Durable:  "CONSUMER",
		Replicas: 3,
	})
	require_NoError(t, err)

	const concurrency = 32
	const iterations = 50
	deadline := time.Now().Add(time.Minute)

	var wg sync.WaitGroup
	for i := 0; i < concurrency; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < iterations; j++ {
				jsTestPause_PauseConsumer(t, nc, "TEST", "CONSUMER", deadline)
			}
		}()
	}
	wg.Wait()
}

func TestJetStreamClusterConsumerPauseHeartbeats(t *testing.T) {
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

	deadline := time.Now().Add(time.Hour)
	dsubj := "deliver_subj"

	ci := jsTestPause_CreateOrUpdateConsumer(t, nc, ActionCreate, "TEST", ConsumerConfig{
		Name:           "my_consumer",
		PauseUntil:     &deadline,
		Heartbeat:      time.Millisecond * 100,
		DeliverSubject: dsubj,
	})
	require_True(t, ci.Config.PauseUntil.Equal(deadline))

	ch := make(chan *nats.Msg, 10)
	_, err = nc.ChanSubscribe(dsubj, ch)
	require_NoError(t, err)

	for i := 0; i < 20; i++ {
		msg := require_ChanRead(t, ch, time.Millisecond*200)
		require_Equal(t, msg.Header.Get("Status"), "100")
		require_Equal(t, msg.Header.Get("Description"), "Idle Heartbeat")
	}
}

func TestJetStreamClusterConsumerPauseAdvisories(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "R3S", 3)
	defer c.shutdown()

	nc, js := jsClientConnect(t, c.randomServer())
	defer nc.Close()

	checkAdvisory := func(msg *nats.Msg, shouldBePaused bool, deadline time.Time) {
		t.Helper()
		var advisory JSConsumerPauseAdvisory
		require_NoError(t, json.Unmarshal(msg.Data, &advisory))
		require_Equal(t, advisory.Stream, "TEST")
		require_Equal(t, advisory.Consumer, "my_consumer")
		require_Equal(t, advisory.Paused, shouldBePaused)
		require_True(t, advisory.PauseUntil.Equal(deadline))
	}

	_, err := js.AddStream(&nats.StreamConfig{
		Name:     "TEST",
		Subjects: []string{"foo"},
		Replicas: 3,
	})
	require_NoError(t, err)

	ch := make(chan *nats.Msg, 10)
	_, err = nc.ChanSubscribe(JSAdvisoryConsumerPausePre+".TEST.my_consumer", ch)
	require_NoError(t, err)

	deadline := time.Now().Add(time.Second)
	jsTestPause_CreateOrUpdateConsumer(t, nc, ActionCreate, "TEST", ConsumerConfig{
		Name:       "my_consumer",
		PauseUntil: &deadline,
		Replicas:   3,
	})

	// First advisory should tell us that the consumer was paused
	// on creation.
	msg := require_ChanRead(t, ch, time.Second*2)
	checkAdvisory(msg, true, deadline)
	require_Len(t, len(ch), 0) // Should only receive one advisory.

	// The second one for the unpause.
	msg = require_ChanRead(t, ch, time.Second*2)
	checkAdvisory(msg, false, deadline)
	require_Len(t, len(ch), 0) // Should only receive one advisory.

	// Now we'll pause the consumer for a second using the API.
	deadline = time.Now().Add(time.Second)
	require_True(t, jsTestPause_PauseConsumer(t, nc, "TEST", "my_consumer", deadline).Equal(deadline))

	// Third advisory should tell us about the pause via the API.
	msg = require_ChanRead(t, ch, time.Second*2)
	checkAdvisory(msg, true, deadline)
	require_Len(t, len(ch), 0) // Should only receive one advisory.

	// Finally that should unpause.
	msg = require_ChanRead(t, ch, time.Second*2)
	checkAdvisory(msg, false, deadline)
	require_Len(t, len(ch), 0) // Should only receive one advisory.

	// Now we're going to set the deadline into the future so we can
	// see what happens when we kick leaders or restart.
	deadline = time.Now().Add(time.Hour)
	require_True(t, jsTestPause_PauseConsumer(t, nc, "TEST", "my_consumer", deadline).Equal(deadline))

	// Setting the deadline should have generated an advisory.
	msg = require_ChanRead(t, ch, time.Second)
	checkAdvisory(msg, true, deadline)
	require_Len(t, len(ch), 0) // Should only receive one advisory.

	// Try to kick the consumer leader.
	srv := c.consumerLeader(globalAccountName, "TEST", "my_consumer")
	srv.JetStreamStepdownConsumer(globalAccountName, "TEST", "my_consumer")
	c.waitOnConsumerLeader(globalAccountName, "TEST", "my_consumer")

	// This shouldn't have generated an advisory.
	require_NoChanRead(t, ch, time.Second)
}

func TestJetStreamClusterConsumerPauseSurvivesRestart(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "R3S", 3)
	defer c.shutdown()

	nc, js := jsClientConnect(t, c.randomServer())
	defer nc.Close()

	checkTimer := func(s *Server) {
		stream, err := s.gacc.lookupStream("TEST")
		require_NoError(t, err)

		consumer := stream.lookupConsumer("my_consumer")
		require_NotEqual(t, consumer, nil)

		consumer.mu.RLock()
		timer := consumer.uptmr
		consumer.mu.RUnlock()
		require_True(t, timer != nil)
	}

	_, err := js.AddStream(&nats.StreamConfig{
		Name:     "TEST",
		Subjects: []string{"foo"},
		Replicas: 3,
	})
	require_NoError(t, err)

	deadline := time.Now().Add(time.Hour)
	jsTestPause_CreateOrUpdateConsumer(t, nc, ActionCreate, "TEST", ConsumerConfig{
		Name:       "my_consumer",
		PauseUntil: &deadline,
		Replicas:   3,
	})

	// First try with just restarting the consumer leader.
	srv := c.consumerLeader(globalAccountName, "TEST", "my_consumer")
	srv.Shutdown()
	c.restartServer(srv)
	c.waitOnAllCurrent()
	c.waitOnConsumerLeader(globalAccountName, "TEST", "my_consumer")
	leader := c.consumerLeader(globalAccountName, "TEST", "my_consumer")
	require_True(t, leader != nil)
	checkTimer(leader)

	// Then try restarting the entire cluster.
	c.stopAll()
	c.restartAllSamePorts()
	c.waitOnAllCurrent()
	c.waitOnConsumerLeader(globalAccountName, "TEST", "my_consumer")
	leader = c.consumerLeader(globalAccountName, "TEST", "my_consumer")
	require_True(t, leader != nil)
	checkTimer(leader)
}

func TestJetStreamClusterConsumerNRGCleanup(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "R3S", 3)
	defer c.shutdown()

	nc, js := jsClientConnect(t, c.randomServer())
	defer nc.Close()

	_, err := js.AddStream(&nats.StreamConfig{
		Name:      "TEST",
		Subjects:  []string{"foo"},
		Storage:   nats.MemoryStorage,
		Retention: nats.WorkQueuePolicy,
		Replicas:  3,
	})
	require_NoError(t, err)

	// First call is just to create the pull subscribers.
	_, err = js.PullSubscribe("foo", "dlc")
	require_NoError(t, err)

	require_NoError(t, js.DeleteConsumer("TEST", "dlc"))

	// Now delete the stream.
	require_NoError(t, js.DeleteStream("TEST"))

	// Now make sure we cleaned up the NRG directories for the stream and consumer.
	checkFor(t, 2*time.Second, 500*time.Millisecond, func() error {
		var numConsumers, numStreams int
		for _, s := range c.servers {
			sd := s.JetStreamConfig().StoreDir
			nd := filepath.Join(sd, "$SYS", "_js_")
			f, err := os.Open(nd)
			if err != nil {
				return err
			}
			dirs, err := f.ReadDir(-1)
			if err != nil {
				return err
			}
			for _, fi := range dirs {
				if strings.HasPrefix(fi.Name(), "C-") {
					numConsumers++
				} else if strings.HasPrefix(fi.Name(), "S-") {
					numStreams++
				}
			}
			f.Close()
		}

		if numConsumers != 0 {
			return fmt.Errorf("expected 0 consumers, got %d", numConsumers)
		}
		if numStreams != 0 {
			return fmt.Errorf("expected 0 streams, got %d", numStreams)
		}
		return nil
	})
}

// https://github.com/nats-io/nats-server/issues/4878
func TestClusteredInterestConsumerFilterEdit(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "R3S", 3)
	defer c.shutdown()

	s := c.randomNonLeader()

	nc, js := jsClientConnect(t, s)
	defer nc.Close()

	_, err := js.AddStream(&nats.StreamConfig{
		Name:      "INTEREST",
		Retention: nats.InterestPolicy,
		Subjects:  []string{"interest.>"},
		Replicas:  3,
	})
	require_NoError(t, err)

	_, err = js.AddConsumer("INTEREST", &nats.ConsumerConfig{
		Durable:       "C0",
		FilterSubject: "interest.>",
		AckPolicy:     nats.AckExplicitPolicy,
	})
	require_NoError(t, err)

	for i := 0; i < 10; i++ {
		_, err = js.Publish(fmt.Sprintf("interest.%d", i), []byte(strconv.Itoa(i)))
		require_NoError(t, err)
	}

	// we check we got 10 messages
	nfo, err := js.StreamInfo("INTEREST")
	require_NoError(t, err)
	if nfo.State.Msgs != 10 {
		t.Fatalf("expected 10 messages got %d", nfo.State.Msgs)
	}

	// now we lower the consumer interest from all subjects to 1,
	// then check the stream state and check if interest behavior still works
	_, err = js.UpdateConsumer("INTEREST", &nats.ConsumerConfig{
		Durable:       "C0",
		FilterSubject: "interest.1",
		AckPolicy:     nats.AckExplicitPolicy,
	})
	require_NoError(t, err)

	// we should now have only one message left
	nfo, err = js.StreamInfo("INTEREST")
	require_NoError(t, err)
	if nfo.State.Msgs != 1 {
		t.Fatalf("expected 1 message got %d", nfo.State.Msgs)
	}
}

func TestJetStreamClusterDoubleAckRedelivery(t *testing.T) {
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
		server_tags: ["test"]
		system_account: sys
		no_auth_user: js
		accounts {
			sys { users = [ { user: sys, pass: sys } ] }
			js {
				jetstream = enabled
				users = [ { user: js, pass: js } ]
		    }
		}`
	c := createJetStreamClusterWithTemplate(t, conf, "R3F", 3)
	defer c.shutdown()
	for _, s := range c.servers {
		s.optsMu.Lock()
		s.opts.LameDuckDuration = 15 * time.Second
		s.opts.LameDuckGracePeriod = -15 * time.Second
		s.optsMu.Unlock()
	}
	s := c.randomNonLeader()

	nc, js := jsClientConnect(t, s)
	defer nc.Close()

	sc, err := js.AddStream(&nats.StreamConfig{
		Name:     "LIMITS",
		Subjects: []string{"foo.>"},
		Replicas: 3,
		Storage:  nats.FileStorage,
	})
	require_NoError(t, err)

	stepDown := func() {
		_, err = nc.Request(fmt.Sprintf(JSApiStreamLeaderStepDownT, sc.Config.Name), nil, time.Second)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var wg sync.WaitGroup
	producer := func(name string) {
		wg.Add(1)
		nc, js := jsClientConnect(t, s)
		defer nc.Close()
		defer wg.Done()

		i := 0
		payload := []byte(strings.Repeat("Z", 1024))
		for range time.NewTicker(1 * time.Millisecond).C {
			select {
			case <-ctx.Done():
				return
			default:
			}
			msgID := nats.MsgId(fmt.Sprintf("%s:%d", name, i))
			js.PublishAsync("foo.bar", payload, msgID, nats.RetryAttempts(10))
			i++
		}
	}
	go producer("A")
	go producer("B")
	go producer("C")

	sub, err := js.PullSubscribe("foo.bar", "ABC", nats.AckWait(5*time.Second), nats.MaxAckPending(1000), nats.PullMaxWaiting(1000))
	if err != nil {
		t.Fatal(err)
	}

	type ackResult struct {
		ack         *nats.Msg
		original    *nats.Msg
		redelivered *nats.Msg
	}
	received := make(map[string]int64)
	acked := make(map[string]*ackResult)
	errors := make(map[string]error)
	extraRedeliveries := 0

	wg.Add(1)
	go func() {
		nc, js = jsClientConnect(t, s)
		defer nc.Close()
		defer wg.Done()

		fetch := func(t *testing.T, batchSize int) {
			msgs, err := sub.Fetch(batchSize, nats.MaxWait(500*time.Millisecond))
			if err != nil {
				return
			}

			for _, msg := range msgs {
				meta, err := msg.Metadata()
				if err != nil {
					t.Error(err)
					continue
				}

				msgID := msg.Header.Get(nats.MsgIdHdr)
				if err, ok := errors[msgID]; ok {
					t.Logf("Redelivery (num_delivered: %v) after failed Ack Sync: %+v - %+v - error: %v", meta.NumDelivered, msg.Reply, msg.Header, err)
				}
				if resp, ok := acked[msgID]; ok {
					t.Errorf("Redelivery (num_delivered: %v) after successful Ack Sync: msgID:%v - redelivered:%v - original:%+v - ack:%+v",
						meta.NumDelivered, msgID, msg.Reply, resp.original.Reply, resp.ack)
					resp.redelivered = msg
					extraRedeliveries++
				}
				received[msgID]++

				// Retry quickly a few times after there is a failed ack.
			Retries:
				for i := 0; i < 10; i++ {
					resp, err := nc.Request(msg.Reply, []byte("+ACK"), 500*time.Millisecond)
					if err != nil {
						t.Logf("Error: %v %v", msgID, err)
						errors[msgID] = err
					} else {
						acked[msgID] = &ackResult{resp, msg, nil}
						break Retries
					}
				}
			}
		}

		for {
			select {
			case <-ctx.Done():
				return
			default:
			}
			fetch(t, 1)
			fetch(t, 50)
		}
	}()

	// Cause a couple of step downs before the restarts as well.
	time.AfterFunc(5*time.Second, func() { stepDown() })
	time.AfterFunc(10*time.Second, func() { stepDown() })

	// Let messages be produced, and then restart the servers.
	<-time.After(15 * time.Second)

NextServer:
	for _, s := range c.servers {
		s.lameDuckMode()
		s.WaitForShutdown()
		s = c.restartServer(s)

		hctx, hcancel := context.WithTimeout(ctx, 60*time.Second)
		defer hcancel()
		for range time.NewTicker(2 * time.Second).C {
			select {
			case <-hctx.Done():
				t.Logf("WRN: Timed out waiting for healthz from %s", s)
				continue NextServer
			default:
			}

			status := s.healthz(nil)
			if status.StatusCode == 200 {
				continue NextServer
			}
		}
		// Pause in-between server restarts.
		time.Sleep(10 * time.Second)
	}

	// Stop all producer and consumer goroutines to check results.
	cancel()
	select {
	case <-ctx.Done():
	case <-time.After(10 * time.Second):
	}
	wg.Wait()
	if extraRedeliveries > 0 {
		t.Fatalf("Received %v redeliveries after a successful ack", extraRedeliveries)
	}
}

func TestJetStreamClusterBusyStreams(t *testing.T) {
	t.Skip("Too long for CI at the moment")
	type streamSetup struct {
		config    *nats.StreamConfig
		consumers []*nats.ConsumerConfig
		subjects  []string
	}
	type job func(t *testing.T, nc *nats.Conn, js nats.JetStreamContext, c *cluster)
	type testParams struct {
		cluster         string
		streams         []*streamSetup
		producers       int
		consumers       int
		restartAny      bool
		restartWait     time.Duration
		ldmRestart      bool
		rolloutRestart  bool
		restarts        int
		checkHealthz    bool
		jobs            []job
		expect          job
		duration        time.Duration
		producerMsgs    int
		producerMsgSize int
	}
	test := func(t *testing.T, test *testParams) {
		conf := `
                listen: 127.0.0.1:-1
                http: 127.0.0.1:-1
                server_name: %s
                jetstream: {
                        domain: "cloud"
                        store_dir: '%s',
                }
                cluster {
                        name: %s
                        listen: 127.0.0.1:%d
                        routes = [%s]
                }
                server_tags: ["test"]
                system_account: sys

                no_auth_user: js
                accounts {
                        sys { users = [ { user: sys, pass: sys } ] }

                        js  { jetstream = enabled
                              users = [ { user: js, pass: js } ]
                        }
                }`
		c := createJetStreamClusterWithTemplate(t, conf, test.cluster, 3)
		defer c.shutdown()
		for _, s := range c.servers {
			s.optsMu.Lock()
			s.opts.LameDuckDuration = 15 * time.Second
			s.opts.LameDuckGracePeriod = -15 * time.Second
			s.optsMu.Unlock()
		}

		nc, js := jsClientConnect(t, c.randomServer())
		defer nc.Close()

		var wg sync.WaitGroup
		for _, stream := range test.streams {
			stream := stream
			wg.Add(1)
			go func() {
				defer wg.Done()
				_, err := js.AddStream(stream.config)
				require_NoError(t, err)

				for _, consumer := range stream.consumers {
					_, err := js.AddConsumer(stream.config.Name, consumer)
					require_NoError(t, err)
				}
			}()
		}
		wg.Wait()

		ctx, cancel := context.WithTimeout(context.Background(), test.duration)
		defer cancel()
		for _, stream := range test.streams {
			payload := []byte(strings.Repeat("A", test.producerMsgSize))
			stream := stream
			subjects := stream.subjects

			// Create publishers on different connections that sends messages
			// to all the consumers subjects.
			var n atomic.Uint64
			for i := 0; i < test.producers; i++ {
				wg.Add(1)
				go func() {
					defer wg.Done()
					nc, js := jsClientConnect(t, c.randomServer())
					defer nc.Close()

					for range time.NewTicker(1 * time.Millisecond).C {
						select {
						case <-ctx.Done():
							return
						default:
						}

						for _, subject := range subjects {
							msgID := nats.MsgId(fmt.Sprintf("n:%d", n.Load()))
							_, err := js.Publish(subject, payload, nats.AckWait(200*time.Millisecond), msgID)
							if err == nil {
								if nn := n.Add(1); int(nn) >= test.producerMsgs {
									return
								}
							}
						}
					}
				}()
			}

			// Create multiple parallel pull subscribers per consumer config.
			for i := 0; i < test.consumers; i++ {
				for _, consumer := range stream.consumers {
					wg.Add(1)

					consumer := consumer
					go func() {
						defer wg.Done()

						for attempts := 0; attempts < 60; attempts++ {
							_, err := js.ConsumerInfo(stream.config.Name, consumer.Name)
							if err != nil {
								t.Logf("WRN: Failed creating pull subscriber: %v - %v - %v - %v",
									consumer.FilterSubject, stream.config.Name, consumer.Name, err)
							}
						}
						sub, err := js.PullSubscribe(consumer.FilterSubject, "", nats.Bind(stream.config.Name, consumer.Name))
						if err != nil {
							t.Logf("WRN: Failed creating pull subscriber: %v - %v - %v - %v",
								consumer.FilterSubject, stream.config.Name, consumer.Name, err)
							return
						}
						require_NoError(t, err)

						for range time.NewTicker(100 * time.Millisecond).C {
							select {
							case <-ctx.Done():
								return
							default:
							}

							msgs, err := sub.Fetch(1, nats.MaxWait(200*time.Millisecond))
							if err != nil {
								continue
							}
							for _, msg := range msgs {
								msg.Ack()
							}

							msgs, err = sub.Fetch(100, nats.MaxWait(200*time.Millisecond))
							if err != nil {
								continue
							}
							for _, msg := range msgs {
								msg.Ack()
							}
						}
					}()
				}
			}
		}

		for _, job := range test.jobs {
			go job(t, nc, js, c)
		}
		if test.restarts > 0 {
			wg.Add(1)
			time.AfterFunc(test.restartWait, func() {
				defer wg.Done()
				for i := 0; i < test.restarts; i++ {
					switch {
					case test.restartAny:
						s := c.servers[rand.IntN(len(c.servers))]
						if test.ldmRestart {
							s.lameDuckMode()
						} else {
							s.Shutdown()
						}
						s.WaitForShutdown()
						c.restartServer(s)
					case test.rolloutRestart:
						for _, s := range c.servers {
							if test.ldmRestart {
								s.lameDuckMode()
							} else {
								s.Shutdown()
							}
							s.WaitForShutdown()
							s = c.restartServer(s)

							if test.checkHealthz {
								hctx, hcancel := context.WithTimeout(ctx, 15*time.Second)
								defer hcancel()

							Healthz:
								for range time.NewTicker(2 * time.Second).C {
									select {
									case <-hctx.Done():
										break Healthz
									default:
									}

									status := s.healthz(nil)
									if status.StatusCode == 200 {
										break Healthz
									}
								}
							}
						}
					}
					c.waitOnClusterReady()
				}
			})
		}
		test.expect(t, nc, js, c)
		cancel()
		wg.Wait()
	}
	stepDown := func(nc *nats.Conn, streamName string) {
		nc.Request(fmt.Sprintf(JSApiStreamLeaderStepDownT, streamName), nil, time.Second)
	}
	getStreamDetails := func(t *testing.T, c *cluster, accountName, streamName string) *StreamDetail {
		t.Helper()
		srv := c.streamLeader(accountName, streamName)
		jsz, err := srv.Jsz(&JSzOptions{Accounts: true, Streams: true, Consumer: true})
		require_NoError(t, err)
		for _, acc := range jsz.AccountDetails {
			if acc.Name == accountName {
				for _, stream := range acc.Streams {
					if stream.Name == streamName {
						return &stream
					}
				}
			}
		}
		t.Error("Could not find account details")
		return nil
	}
	checkMsgsEqual := func(t *testing.T, c *cluster, accountName, streamName string) {
		state := getStreamDetails(t, c, accountName, streamName).State
		var msets []*stream
		for _, s := range c.servers {
			acc, err := s.LookupAccount(accountName)
			require_NoError(t, err)
			mset, err := acc.lookupStream(streamName)
			require_NoError(t, err)
			msets = append(msets, mset)
		}
		for seq := state.FirstSeq; seq <= state.LastSeq; seq++ {
			var msgId string
			var smv StoreMsg
			for _, mset := range msets {
				mset.mu.RLock()
				sm, err := mset.store.LoadMsg(seq, &smv)
				mset.mu.RUnlock()
				require_NoError(t, err)
				if msgId == _EMPTY_ {
					msgId = string(sm.hdr)
				} else if msgId != string(sm.hdr) {
					t.Fatalf("MsgIds do not match for seq %d: %q vs %q", seq, msgId, sm.hdr)
				}
			}
		}
	}
	checkConsumer := func(t *testing.T, c *cluster, accountName, streamName, consumerName string) {
		t.Helper()
		var leader string
		for _, s := range c.servers {
			jsz, err := s.Jsz(&JSzOptions{Accounts: true, Streams: true, Consumer: true})
			require_NoError(t, err)
			for _, acc := range jsz.AccountDetails {
				if acc.Name == accountName {
					for _, stream := range acc.Streams {
						if stream.Name == streamName {
							for _, consumer := range stream.Consumer {
								if leader == "" {
									leader = consumer.Cluster.Leader
								} else if leader != consumer.Cluster.Leader {
									t.Errorf("There are two leaders for %s/%s: %s vs %s",
										stream.Name, consumer.Name, leader, consumer.Cluster.Leader)
								}
							}
						}
					}
				}
			}
		}
	}

	t.Run("R1F/rescale/R3F/sources:10/limits", func(t *testing.T) {
		testDuration := 3 * time.Minute
		totalStreams := 10
		streams := make([]*streamSetup, totalStreams)
		sources := make([]*nats.StreamSource, totalStreams)
		for i := 0; i < totalStreams; i++ {
			name := fmt.Sprintf("test:%d", i)
			st := &streamSetup{
				config: &nats.StreamConfig{
					Name:      name,
					Subjects:  []string{fmt.Sprintf("test.%d.*", i)},
					Replicas:  1,
					Retention: nats.LimitsPolicy,
				},
			}
			st.subjects = append(st.subjects, fmt.Sprintf("test.%d.0", i))
			sources[i] = &nats.StreamSource{Name: name}
			streams[i] = st
		}

		// Create Source consumer.
		sourceSetup := &streamSetup{
			config: &nats.StreamConfig{
				Name:      "source-test",
				Replicas:  1,
				Retention: nats.LimitsPolicy,
				Sources:   sources,
			},
			consumers: make([]*nats.ConsumerConfig, 0),
		}
		cc := &nats.ConsumerConfig{
			Name:          "A",
			Durable:       "A",
			FilterSubject: "test.>",
			AckPolicy:     nats.AckExplicitPolicy,
		}
		sourceSetup.consumers = append(sourceSetup.consumers, cc)
		streams = append(streams, sourceSetup)

		scale := func(replicas int, wait time.Duration) job {
			return func(t *testing.T, nc *nats.Conn, js nats.JetStreamContext, c *cluster) {
				config := sourceSetup.config
				time.AfterFunc(wait, func() {
					config.Replicas = replicas
					for i := 0; i < 10; i++ {
						_, err := js.UpdateStream(config)
						if err == nil {
							return
						}
						time.Sleep(1 * time.Second)
					}
				})
			}
		}

		expect := func(t *testing.T, nc *nats.Conn, js nats.JetStreamContext, c *cluster) {
			// The source stream should not be stuck or be different from the other streams.
			time.Sleep(testDuration + 1*time.Minute)
			accName := "js"
			streamName := "source-test"

			// Check a few times to see if there are no changes in the number of messages.
			var changed bool
			var prevMsgs uint64
			for i := 0; i < 10; i++ {
				sinfo, err := js.StreamInfo(streamName)
				if err != nil {
					t.Logf("Error: %v", err)
					time.Sleep(2 * time.Second)
					continue
				}
				prevMsgs = sinfo.State.Msgs
			}
			for i := 0; i < 10; i++ {
				sinfo, err := js.StreamInfo(streamName)
				if err != nil {
					t.Logf("Error: %v", err)
					time.Sleep(2 * time.Second)
					continue
				}
				changed = prevMsgs != sinfo.State.Msgs
				prevMsgs = sinfo.State.Msgs
				time.Sleep(2 * time.Second)
			}
			if !changed {
				// Doing a leader step down should not cause the messages to change.
				stepDown(nc, streamName)

				for i := 0; i < 10; i++ {
					sinfo, err := js.StreamInfo(streamName)
					if err != nil {
						t.Logf("Error: %v", err)
						time.Sleep(2 * time.Second)
						continue
					}
					changed = prevMsgs != sinfo.State.Msgs
					prevMsgs = sinfo.State.Msgs
					time.Sleep(2 * time.Second)
				}
				if changed {
					t.Error("Stream msgs changed after the step down")
				}
			}

			/////////////////////////////////////////////////////////////////////////////////////////
			//                                                                                     //
			//  The number of messages sourced should match the count from all the other streams.  //
			//                                                                                     //
			/////////////////////////////////////////////////////////////////////////////////////////
			var expectedMsgs uint64
			for i := 0; i < totalStreams; i++ {
				name := fmt.Sprintf("test:%d", i)
				sinfo, err := js.StreamInfo(name)
				require_NoError(t, err)
				expectedMsgs += sinfo.State.Msgs
			}
			sinfo, err := js.StreamInfo(streamName)
			require_NoError(t, err)

			gotMsgs := sinfo.State.Msgs
			if gotMsgs != expectedMsgs {
				t.Errorf("stream with sources has %v messages, but total sourced messages should be %v", gotMsgs, expectedMsgs)
			}
			checkConsumer(t, c, accName, streamName, "A")
			checkMsgsEqual(t, c, accName, streamName)
		}
		test(t, &testParams{
			cluster:        t.Name(),
			streams:        streams,
			producers:      10,
			consumers:      10,
			restarts:       1,
			rolloutRestart: true,
			ldmRestart:     true,
			checkHealthz:   true,
			// TODO(dlc) - If this overlaps with the scale jobs this test will fail.
			// Leaders will be elected with partial state.
			restartWait: 65 * time.Second,
			jobs: []job{
				scale(3, 15*time.Second),
				scale(1, 30*time.Second),
				scale(3, 60*time.Second),
			},
			expect:          expect,
			duration:        testDuration,
			producerMsgSize: 1024,
			producerMsgs:    100_000,
		})
	})

	t.Run("rollouts", func(t *testing.T) {
		shared := func(t *testing.T, sc *nats.StreamConfig, tp *testParams) func(t *testing.T) {
			return func(t *testing.T) {
				testDuration := 3 * time.Minute
				totalStreams := 30
				consumersPerStream := 5
				streams := make([]*streamSetup, totalStreams)
				for i := 0; i < totalStreams; i++ {
					name := fmt.Sprintf("test:%d", i)
					st := &streamSetup{
						config: &nats.StreamConfig{
							Name:      name,
							Subjects:  []string{fmt.Sprintf("test.%d.*", i)},
							Replicas:  3,
							Discard:   sc.Discard,
							Retention: sc.Retention,
							Storage:   sc.Storage,
							MaxMsgs:   sc.MaxMsgs,
							MaxBytes:  sc.MaxBytes,
							MaxAge:    sc.MaxAge,
						},
						consumers: make([]*nats.ConsumerConfig, 0),
					}
					for j := 0; j < consumersPerStream; j++ {
						subject := fmt.Sprintf("test.%d.%d", i, j)
						name := fmt.Sprintf("A:%d:%d", i, j)
						cc := &nats.ConsumerConfig{
							Name:          name,
							Durable:       name,
							FilterSubject: subject,
							AckPolicy:     nats.AckExplicitPolicy,
						}
						st.consumers = append(st.consumers, cc)
						st.subjects = append(st.subjects, subject)
					}
					streams[i] = st
				}
				expect := func(t *testing.T, nc *nats.Conn, js nats.JetStreamContext, c *cluster) {
					time.Sleep(testDuration + 1*time.Minute)
					accName := "js"
					for i := 0; i < totalStreams; i++ {
						streamName := fmt.Sprintf("test:%d", i)
						checkMsgsEqual(t, c, accName, streamName)
					}
				}
				test(t, &testParams{
					cluster:         t.Name(),
					streams:         streams,
					producers:       10,
					consumers:       10,
					restarts:        tp.restarts,
					rolloutRestart:  tp.rolloutRestart,
					ldmRestart:      tp.ldmRestart,
					checkHealthz:    tp.checkHealthz,
					restartWait:     tp.restartWait,
					expect:          expect,
					duration:        testDuration,
					producerMsgSize: 1024,
					producerMsgs:    100_000,
				})
			}
		}
		for prefix, st := range map[string]nats.StorageType{"R3F": nats.FileStorage, "R3M": nats.MemoryStorage} {
			t.Run(prefix, func(t *testing.T) {
				for rolloutType, params := range map[string]*testParams{
					// Rollouts using graceful restarts and checking healthz.
					"ldm": {
						restarts:       1,
						rolloutRestart: true,
						ldmRestart:     true,
						checkHealthz:   true,
						restartWait:    45 * time.Second,
					},
					// Non graceful restarts calling Shutdown, but using healthz on startup.
					"term": {
						restarts:       1,
						rolloutRestart: true,
						ldmRestart:     false,
						checkHealthz:   true,
						restartWait:    45 * time.Second,
					},
				} {
					t.Run(rolloutType, func(t *testing.T) {
						t.Run("limits", shared(t, &nats.StreamConfig{
							Retention: nats.LimitsPolicy,
							Storage:   st,
						}, params))
						t.Run("wq", shared(t, &nats.StreamConfig{
							Retention: nats.WorkQueuePolicy,
							Storage:   st,
						}, params))
						t.Run("interest", shared(t, &nats.StreamConfig{
							Retention: nats.InterestPolicy,
							Storage:   st,
						}, params))
						t.Run("limits:dn:max-per-subject", shared(t, &nats.StreamConfig{
							Retention:         nats.LimitsPolicy,
							Storage:           st,
							MaxMsgsPerSubject: 1,
							Discard:           nats.DiscardNew,
						}, params))
						t.Run("wq:dn:max-msgs", shared(t, &nats.StreamConfig{
							Retention: nats.WorkQueuePolicy,
							Storage:   st,
							MaxMsgs:   10_000,
							Discard:   nats.DiscardNew,
						}, params))
						t.Run("wq:dn-per-subject:max-msgs", shared(t, &nats.StreamConfig{
							Retention:            nats.WorkQueuePolicy,
							Storage:              st,
							MaxMsgs:              10_000,
							MaxMsgsPerSubject:    100,
							Discard:              nats.DiscardNew,
							DiscardNewPerSubject: true,
						}, params))
					})
				}
			})
		}
	})
}

// https://github.com/nats-io/nats-server/issues/5488
func TestJetStreamClusterSingleMaxConsumerUpdate(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "R3S", 3)
	defer c.shutdown()

	nc, js := jsClientConnect(t, c.randomServer())
	defer nc.Close()

	_, err := js.AddStream(&nats.StreamConfig{
		Name:         "TEST",
		MaxConsumers: 1,
	})
	require_NoError(t, err)

	_, err = js.AddConsumer("TEST", &nats.ConsumerConfig{
		Name:          "test_consumer",
		MaxAckPending: 1000,
	})
	require_NoError(t, err)

	// This would previously return a "nats: maximum consumers limit
	// reached" (10026) error.
	_, err = js.UpdateConsumer("TEST", &nats.ConsumerConfig{
		Name:          "test_consumer",
		MaxAckPending: 1001,
	})
	require_NoError(t, err)
}

func TestJetStreamClusterStreamLastSequenceResetAfterStorageWipe(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "R3S", 3)
	defer c.shutdown()

	nc, js := jsClientConnect(t, c.randomServer())
	defer nc.Close()

	// After bug was found, number of streams and wiping store directory really did not affect.
	numStreams := 50
	var wg sync.WaitGroup
	wg.Add(numStreams)

	for i := 1; i <= numStreams; i++ {
		go func(n int) {
			defer wg.Done()
			_, err := js.AddStream(&nats.StreamConfig{
				Name:      fmt.Sprintf("TEST:%d", n),
				Retention: nats.InterestPolicy,
				Subjects:  []string{fmt.Sprintf("foo.%d.*", n)},
				Replicas:  3,
			}, nats.MaxWait(30*time.Second))
			require_NoError(t, err)
			subj := fmt.Sprintf("foo.%d.bar", n)
			for i := 0; i < 222; i++ {
				js.Publish(subj, nil)
			}
		}(i)
	}
	wg.Wait()

	for i := 0; i < 5; i++ {
		// Walk the servers and shut each down, and wipe the storage directory.
		for _, s := range c.servers {
			sd := s.JetStreamConfig().StoreDir
			s.Shutdown()
			s.WaitForShutdown()
			os.RemoveAll(sd)
			s = c.restartServer(s)
			checkFor(t, 10*time.Second, 200*time.Millisecond, func() error {
				hs := s.healthz(nil)
				if hs.Error != _EMPTY_ {
					return errors.New(hs.Error)
				}
				return nil
			})
		}

		for _, s := range c.servers {
			for i := 1; i <= numStreams; i++ {
				stream := fmt.Sprintf("TEST:%d", i)
				mset, err := s.GlobalAccount().lookupStream(stream)
				require_NoError(t, err)
				var state StreamState
				checkFor(t, 10*time.Second, 200*time.Millisecond, func() error {
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

func TestJetStreamClusterAckFloorBetweenLeaderAndFollowers(t *testing.T) {
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

	sub, err := js.PullSubscribe("foo.*", "consumer")
	require_NoError(t, err)

	// Move consumer leader to be on the same server as the stream leader.
	// Without this fetching messages right after getting a response to js.Publish could mean
	// some messages are not available to the consumer yet.
	sl := c.streamLeader(globalAccountName, "TEST")
	cl := c.consumerLeader(globalAccountName, "TEST", "consumer")
	if cl.Name() != sl.Name() {
		req := JSApiLeaderStepdownRequest{Placement: &Placement{Preferred: sl.Name()}}
		data, err := json.Marshal(req)
		require_NoError(t, err)
		_, err = nc.Request(fmt.Sprintf(JSApiConsumerLeaderStepDownT, "TEST", "consumer"), data, time.Second)
		require_NoError(t, err)
		c.waitOnConsumerLeader(globalAccountName, "TEST", "consumer")
		cl = c.consumerLeader(globalAccountName, "TEST", "consumer")
		require_Equal(t, cl.Name(), sl.Name())
	}

	// Do 25 rounds.
	for i := 1; i <= 25; i++ {
		// Send 50 msgs.
		for x := 0; x < 50; x++ {
			_, err := js.Publish("foo.bar", nil)
			require_NoError(t, err)
		}
		msgs, err := sub.Fetch(50)
		require_NoError(t, err)
		require_Equal(t, len(msgs), 50)
		// Randomize
		rand.Shuffle(len(msgs), func(i, j int) { msgs[i], msgs[j] = msgs[j], msgs[i] })
		for _, m := range msgs {
			require_NoError(t, m.AckSync())
		}

		time.Sleep(100 * time.Millisecond)
		for _, s := range c.servers {
			mset, err := s.GlobalAccount().lookupStream("TEST")
			require_NoError(t, err)
			consumer := mset.lookupConsumer("consumer")
			require_NotEqual(t, consumer, nil)
			info := consumer.info()
			require_Equal(t, info.NumAckPending, 0)
			require_Equal(t, info.AckFloor.Consumer, uint64(i*50))
			require_Equal(t, info.AckFloor.Stream, uint64(i*50))
		}
	}
}

// https://github.com/nats-io/nats-server/pull/5600
func TestJetStreamClusterConsumerLeak(t *testing.T) {
	N := 2000
	NConcurrent := 25

	clusterConf := `
	listen: 127.0.0.1:-1

	server_name: %s
	jetstream: {max_mem_store: 256MB, max_file_store: 2GB, store_dir: '%s'}

	leafnodes {
		listen: 127.0.0.1:-1
	}

	cluster {
		name: %s
		listen: 127.0.0.1:%d
		routes = [%s]
	}

	accounts {
		ONE { users = [ { user: "one", pass: "p" } ]; jetstream: enabled }
		$SYS { users = [ { user: "admin", pass: "s3cr3t!" } ] }
	}
`

	cl := createJetStreamClusterWithTemplate(t, clusterConf, "Leak-test", 3)
	defer cl.shutdown()
	cl.waitOnLeader()

	s := cl.randomNonLeader()

	// Create the test stream.
	streamName := "LEAK_TEST_STREAM"
	nc, js := jsClientConnect(t, s, nats.UserInfo("one", "p"))
	defer nc.Close()
	_, err := js.AddStream(&nats.StreamConfig{
		Name:      streamName,
		Subjects:  []string{"$SOMETHING.>"},
		Storage:   nats.FileStorage,
		Retention: nats.InterestPolicy,
		Replicas:  3,
	})
	if err != nil {
		t.Fatalf("Error creating stream: %v", err)
	}

	concurrent := make(chan struct{}, NConcurrent)
	for i := 0; i < NConcurrent; i++ {
		concurrent <- struct{}{}
	}
	errors := make(chan error, N)

	wg := sync.WaitGroup{}
	wg.Add(N)

	// Gather the stats for comparison.
	before := &runtime.MemStats{}
	runtime.GC()
	runtime.ReadMemStats(before)

	for i := 0; i < N; {
		// wait for a slot to open up
		<-concurrent
		i++
		go func() {
			defer func() {
				concurrent <- struct{}{}
				wg.Done()
			}()

			nc, js := jsClientConnect(t, s, nats.UserInfo("one", "p"))
			defer nc.Close()

			consumerName := "sessid_" + nuid.Next()
			_, err := js.AddConsumer(streamName, &nats.ConsumerConfig{
				DeliverSubject: "inbox",
				Durable:        consumerName,
				AckPolicy:      nats.AckExplicitPolicy,
				DeliverPolicy:  nats.DeliverNewPolicy,
				FilterSubject:  "$SOMETHING.ELSE.subject",
				AckWait:        30 * time.Second,
				MaxAckPending:  1024,
			})
			if err != nil {
				errors <- fmt.Errorf("Error on JetStream consumer creation: %v", err)
				return
			}
			cl.waitOnAllCurrent()

			err = js.DeleteConsumer(streamName, consumerName)
			if err != nil {
				errors <- fmt.Errorf("Error on JetStream consumer deletion: %v", err)
			}
		}()
	}

	wg.Wait()
	if len(errors) > 0 {
		for err := range errors {
			t.Fatalf("%v", err)
		}
	}

	after := &runtime.MemStats{}
	runtime.GC()
	runtime.ReadMemStats(after)

	// Before https://github.com/nats-io/nats-server/pull/5600 this test was
	// adding 180Mb+ to HeapInuse. Now it's under 40Mb (ran locally on a Mac)
	limit := before.HeapInuse + 100*1024*1024 // 100MB
	if after.HeapInuse > before.HeapInuse+limit {
		t.Fatalf("Extra memory usage too high: %v", after.HeapInuse-before.HeapInuse)
	}
}

func TestJetStreamClusterAccountNRG(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "R3S", 3)
	defer c.shutdown()

	nc, js := jsClientConnect(t, c.randomServer())
	defer nc.Close()

	snc, _ := jsClientConnect(t, c.randomServer(), nats.UserInfo("admin", "s3cr3t!"))
	defer snc.Close()

	_, err := js.AddStream(&nats.StreamConfig{
		Name:      "TEST",
		Subjects:  []string{"foo"},
		Storage:   nats.MemoryStorage,
		Retention: nats.WorkQueuePolicy,
		Replicas:  3,
	})
	require_NoError(t, err)

	leader := c.streamLeader(globalAccountName, "TEST")
	stream, err := leader.gacc.lookupStream("TEST")
	require_NoError(t, err)
	rg := stream.node.(*raft)

	t.Run("Disabled", func(t *testing.T) {
		// Switch off account NRG on all servers in the cluster.
		for _, s := range c.servers {
			s.accountNRGAllowed.Store(false)
			s.sendStatszUpdate()
		}
		time.Sleep(time.Millisecond * 100)
		for _, s := range c.servers {
			s.GlobalAccount().nrgAccount = ""
			s.updateNRGAccountStatus()
		}

		// Check account interest for the AppendEntry subject.
		checkFor(t, time.Second, time.Millisecond*25, func() error {
			for _, s := range c.servers {
				if !s.sys.account.sl.HasInterest(rg.asubj) {
					return fmt.Errorf("system account should have interest")
				}
				if s.gacc.sl.HasInterest(rg.asubj) {
					return fmt.Errorf("global account shouldn't have interest")
				}
			}
			return nil
		})

		// Check that the Raft traffic is in the system account, as we
		// haven't moved it elsewhere yet.
		{
			sub, err := snc.SubscribeSync(rg.asubj)
			require_NoError(t, err)
			require_NoError(t, sub.AutoUnsubscribe(1))

			msg, err := sub.NextMsg(time.Second * 3)
			require_NoError(t, err)
			require_True(t, msg != nil)
		}
	})

	t.Run("Mixed", func(t *testing.T) {
		// Switch on account NRG on a single server in the cluster and
		// leave it off on the rest.
		for i, s := range c.servers {
			s.accountNRGAllowed.Store(i == 0)
			s.sendStatszUpdate()
		}
		time.Sleep(time.Millisecond * 100)
		for i, s := range c.servers {
			if i == 0 {
				s.GlobalAccount().nrgAccount = globalAccountName
			} else {
				s.GlobalAccount().nrgAccount = ""
			}
			s.updateNRGAccountStatus()
		}

		// Check account interest for the AppendEntry subject.
		checkFor(t, time.Second, time.Millisecond*25, func() error {
			for _, s := range c.servers {
				if !s.sys.account.sl.HasInterest(rg.asubj) {
					return fmt.Errorf("system account should have interest")
				}
				if s.gacc.sl.HasInterest(rg.asubj) {
					return fmt.Errorf("global account shouldn't have interest")
				}
			}
			return nil
		})

		// Check that the Raft traffic is in the system account, as we
		// don't claim support for account NRG on all nodes in the group.
		{
			sub, err := snc.SubscribeSync(rg.asubj)
			require_NoError(t, err)
			require_NoError(t, sub.AutoUnsubscribe(1))

			msg, err := sub.NextMsg(time.Second * 3)
			require_NoError(t, err)
			require_True(t, msg != nil)
		}
	})

	t.Run("Enabled", func(t *testing.T) {
		// Switch on account NRG on all servers in the cluster.
		for _, s := range c.servers {
			s.accountNRGAllowed.Store(true)
			s.sendStatszUpdate()
		}
		time.Sleep(time.Millisecond * 100)
		for _, s := range c.servers {
			s.GlobalAccount().nrgAccount = globalAccountName
			s.updateNRGAccountStatus()
		}

		// Check account interest for the AppendEntry subject.
		checkFor(t, time.Second, time.Millisecond*25, func() error {
			for _, s := range c.servers {
				if s.sys.account.sl.HasInterest(rg.asubj) {
					return fmt.Errorf("system account shouldn't have interest")
				}
				if !s.gacc.sl.HasInterest(rg.asubj) {
					return fmt.Errorf("global account should have interest")
				}
			}
			return nil
		})

		// Check that the traffic moved into the global account as
		// expected.
		{
			sub, err := nc.SubscribeSync(rg.asubj)
			require_NoError(t, err)
			require_NoError(t, sub.AutoUnsubscribe(1))

			msg, err := sub.NextMsg(time.Second * 3)
			require_NoError(t, err)
			require_True(t, msg != nil)
		}
	})
}

func TestJetStreamClusterAccountNRGConfigNoPanic(t *testing.T) {
	clusterConf := `
		listen: 127.0.0.1:-1

		server_name: %s
		jetstream: {max_mem_store: 256MB, max_file_store: 2GB, store_dir: '%s'}

		cluster {
			name: %s
			listen: 127.0.0.1:%d
			routes = [%s]
		}

		accounts {
			ONE { jetstream: { cluster_traffic: system } }
			TWO { jetstream: { cluster_traffic: owner } }
		}
	`

	cl := createJetStreamClusterWithTemplate(t, clusterConf, "test", 3)
	defer cl.shutdown()

	for _, s := range cl.servers {
		acc, err := s.lookupAccount("ONE")
		require_NoError(t, err)
		require_Equal(t, acc.nrgAccount, _EMPTY_) // Empty for the system account

		acc, err = s.lookupAccount("TWO")
		require_NoError(t, err)
		require_Equal(t, acc.nrgAccount, "TWO")
	}
}

func TestJetStreamClusterWQRoundRobinSubjectRetention(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "R3S", 3)
	defer c.shutdown()

	nc, js := jsClientConnect(t, c.randomServer())
	defer nc.Close()

	_, err := js.AddStream(&nats.StreamConfig{
		Name:      "wq_stream",
		Subjects:  []string{"something.>"},
		Storage:   nats.FileStorage,
		Retention: nats.WorkQueuePolicy,
		Replicas:  3,
	})
	require_NoError(t, err)

	for i := 0; i < 100; i++ {
		n := (i % 5) + 1
		_, err := js.Publish(fmt.Sprintf("something.%d", n), nil)
		require_NoError(t, err)
	}

	sub, err := js.PullSubscribe(
		"something.5",
		"wq_consumer_5",
		nats.BindStream("wq_stream"),
		nats.ConsumerReplicas(3),
	)
	require_NoError(t, err)

	for {
		msgs, _ := sub.Fetch(5)
		if len(msgs) == 0 {
			break
		}
		for _, msg := range msgs {
			require_NoError(t, msg.AckSync())
		}
	}

	si, err := js.StreamInfo("wq_stream")
	require_NoError(t, err)
	require_Equal(t, si.State.Msgs, 80)
	require_Equal(t, si.State.NumDeleted, 20)
	require_Equal(t, si.State.NumSubjects, 4)
}

func TestJetStreamClusterMetaSyncOrphanCleanup(t *testing.T) {
	c := createJetStreamClusterWithTemplateAndModHook(t, jsClusterTempl, "R3S", 3,
		func(serverName, clusterName, storeDir, conf string) string {
			return fmt.Sprintf("%s\nserver_tags: [server:%s]", conf, serverName)
		})
	defer c.shutdown()

	nc, js := jsClientConnect(t, c.randomServer())
	defer nc.Close()

	// Create a bunch of streams on S1
	for i := 0; i < 100; i++ {
		stream := fmt.Sprintf("TEST-%d", i)
		subject := fmt.Sprintf("TEST.%d", i)
		_, err := js.AddStream(&nats.StreamConfig{
			Name:      stream,
			Subjects:  []string{subject},
			Storage:   nats.FileStorage,
			Placement: &nats.Placement{Tags: []string{"server:S-1"}},
		})
		require_NoError(t, err)
		// Put in 10 msgs to each
		for j := 0; j < 10; j++ {
			_, err := js.Publish(subject, nil)
			require_NoError(t, err)
		}
	}

	// Now we will shutdown S1 and remove all of its meta-data to trip the condition.
	s := c.serverByName("S-1")
	require_True(t, s != nil)

	sd := s.JetStreamConfig().StoreDir
	nd := filepath.Join(sd, "$SYS", "_js_", "_meta_")
	s.Shutdown()
	s.WaitForShutdown()
	os.RemoveAll(nd)
	s = c.restartServer(s)
	c.waitOnServerCurrent(s)
	jsz, err := s.Jsz(nil)
	require_NoError(t, err)
	require_Equal(t, jsz.Streams, 100)

	// These will be recreated by the meta layer, but if the orphan detection deleted them they will be empty,
	// so check all streams to make sure they still have data.
	acc := s.GlobalAccount()
	var state StreamState
	for i := 0; i < 100; i++ {
		mset, err := acc.lookupStream(fmt.Sprintf("TEST-%d", i))
		require_NoError(t, err)
		mset.store.FastState(&state)
		require_Equal(t, state.Msgs, 10)
	}
}

func TestJetStreamClusterKeyValueDesyncAfterHardKill(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "R3F", 3)
	defer c.shutdown()

	nc, js := jsClientConnect(t, c.serverByName("S-1"))
	defer nc.Close()

	kv, err := js.CreateKeyValue(&nats.KeyValueConfig{
		Bucket:   "inconsistency",
		Replicas: 3,
	})
	require_NoError(t, err)

	// First create should succeed.
	revision, err := kv.Create("key.exists", []byte("1"))
	require_NoError(t, err)
	require_Equal(t, revision, 1)

	// Second create will be rejected but bump CLFS.
	_, err = kv.Create("key.exists", []byte("2"))
	require_Error(t, err)

	// Insert a new message, should only be applied once, even if we hard kill and replay afterward.
	revision, err = kv.Put("key.put", []byte("3"))
	require_NoError(t, err)
	require_Equal(t, revision, 2)

	// Restart a server
	s3 := c.serverByName("S-3")
	// We will remove the index.db file after we shutdown.
	mset, err := s3.GlobalAccount().lookupStream("KV_inconsistency")
	require_NoError(t, err)
	fs := mset.store.(*fileStore)
	ifile := filepath.Join(fs.fcfg.StoreDir, msgDir, "index.db")

	s3.Shutdown()
	s3.WaitForShutdown()
	// Remove the index.db file to simulate a hard kill where server can not write out the index.db file.
	require_NoError(t, os.Remove(ifile))

	c.restartServer(s3)
	c.waitOnClusterReady()
	c.waitOnAllCurrent()

	err = checkState(t, c, "$G", "KV_inconsistency")
	require_NoError(t, err)
}

func TestJetStreamClusterKeyValueLastSeqMismatch(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "R3S", 3)
	defer c.shutdown()

	nc, js := jsClientConnect(t, c.randomServer())
	defer nc.Close()

	for _, r := range []int{1, 3} {
		t.Run(fmt.Sprintf("R=%d", r), func(t *testing.T) {
			kv, err := js.CreateKeyValue(&nats.KeyValueConfig{
				Bucket:   fmt.Sprintf("mismatch_%v", r),
				Replicas: r,
			})
			require_NoError(t, err)

			revision, err := kv.Create("foo", []byte("1"))
			require_NoError(t, err)
			require_Equal(t, revision, 1)

			revision, err = kv.Create("bar", []byte("2"))
			require_NoError(t, err)
			require_Equal(t, revision, 2)

			// Now say we want to update baz but iff last was revision 1.
			_, err = kv.Update("baz", []byte("3"), uint64(1))
			require_Error(t, err)
			require_True(t, errors.Is(err, nats.ErrKeyRevisionMismatch))
			var apiErr *nats.APIError
			require_True(t, errors.As(err, &apiErr))
			require_Equal(t, apiErr.Description, "wrong last sequence: 0")
		})
	}
}

func TestJetStreamClusterPubAckSequenceDupe(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "TEST_CLUSTER", 3)
	defer c.shutdown()

	nc, js := jsClientConnect(t, c.randomServer())
	defer nc.Close()

	type client struct {
		nc *nats.Conn
		js nats.JetStreamContext
	}

	clients := make([]client, len(c.servers))
	for i, server := range c.servers {
		clients[i].nc, clients[i].js = jsClientConnect(t, server)
		defer clients[i].nc.Close()
	}

	_, err := js.AddStream(&nats.StreamConfig{
		Name:       "TEST_STREAM",
		Subjects:   []string{"TEST_SUBJECT.*"},
		Replicas:   3,
		Duplicates: 1 * time.Minute,
	})
	require_NoError(t, err)

	msgData := []byte("...")

	for seq := uint64(1); seq < 10; seq++ {

		if seq%3 == 0 {
			c.restartAll()
		}

		msgSubject := "TEST_SUBJECT." + strconv.FormatUint(seq, 10)
		msgIdOpt := nats.MsgId(nuid.Next())

		firstPublisherClient := &clients[rand.IntN(len(clients))]
		secondPublisherClient := &clients[rand.IntN(len(clients))]

		pubAck1, err := firstPublisherClient.js.Publish(msgSubject, msgData, msgIdOpt)
		require_NoError(t, err)
		require_Equal(t, seq, pubAck1.Sequence)
		require_False(t, pubAck1.Duplicate)

		pubAck2, err := secondPublisherClient.js.Publish(msgSubject, msgData, msgIdOpt)
		require_NoError(t, err)
		require_Equal(t, seq, pubAck2.Sequence)
		require_True(t, pubAck2.Duplicate)

	}

}

func TestJetStreamClusterPubAckSequenceDupeAsync(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "TEST_CLUSTER", 3)
	defer c.shutdown()

	nc, js := jsClientConnect(t, c.randomServer())
	defer nc.Close()

	_, err := js.AddStream(&nats.StreamConfig{
		Name:       "TEST_STREAM",
		Subjects:   []string{"TEST_SUBJECT"},
		Replicas:   3,
		Duplicates: 1 * time.Minute,
	})
	require_NoError(t, err)

	msgData := []byte("...")

	for seq := uint64(1); seq < 10; seq++ {

		msgSubject := "TEST_SUBJECT"
		msgIdOpt := nats.MsgId(nuid.Next())
		conflictErr := &nats.APIError{ErrorCode: nats.ErrorCode(JSStreamDuplicateMessageConflict)}

		wg := sync.WaitGroup{}
		wg.Add(2)

		// Fire off 2 publish requests in parallel
		// The first one "stages" a duplicate entry before even proposing the message
		// The second one gets a pubAck with sequence zero by hitting the staged duplicated entry

		pubAcks := [2]*nats.PubAck{}
		for i := 0; i < 2; i++ {
			go func(i int) {
				defer wg.Done()
				var err error
				pubAcks[i], err = js.Publish(msgSubject, msgData, msgIdOpt)
				// Conflict on duplicate message, wait a bit before retrying to get the proper pubAck.
				if errors.Is(err, conflictErr) {
					time.Sleep(time.Millisecond * 500)
					pubAcks[i], err = js.Publish(msgSubject, msgData, msgIdOpt)
				}
				require_NoError(t, err)
			}(i)
		}

		wg.Wait()
		require_Equal(t, pubAcks[0].Sequence, seq)
		require_Equal(t, pubAcks[1].Sequence, seq)

		// Exactly one of the pubAck should be marked dupe
		require_True(t, (pubAcks[0].Duplicate || pubAcks[1].Duplicate) && (pubAcks[0].Duplicate != pubAcks[1].Duplicate))
	}
}

func TestJetStreamClusterPubAckSequenceDupeResetAfterLeaderChange(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "R3S", 3)
	defer c.shutdown()

	nc, js := jsClientConnect(t, c.randomServer())
	defer nc.Close()

	_, err := js.AddStream(&nats.StreamConfig{
		Name:       "TEST",
		Subjects:   []string{"foo"},
		Replicas:   3,
		Duplicates: 2 * time.Second,
	})
	require_NoError(t, err)

	sl := c.streamLeader(globalAccountName, "TEST")
	acc, err := sl.lookupAccount(globalAccountName)
	require_NoError(t, err)
	mset, err := acc.lookupStream("TEST")
	require_NoError(t, err)

	// Store one msg ID that needs to be preserved, and another that should be removed during leader change.
	mset.ddMu.Lock()
	mset.storeMsgIdLocked(&ddentry{"genuine", 1, time.Now().UnixNano()})
	mset.storeMsgIdLocked(&ddentry{"msgId", 0, time.Now().UnixNano()})
	mset.ddMu.Unlock()

	// Simulates the msg ID being in process.
	_, err = js.Publish("foo", nil, nats.MsgId("msgId"))
	require_Error(t, err, NewJSStreamDuplicateMessageConflictError())

	// Move stream leader to a different server.
	rs := c.randomNonStreamLeader(globalAccountName, "TEST")
	req := JSApiLeaderStepdownRequest{Placement: &Placement{Preferred: rs.Name()}}
	data, err := json.Marshal(req)
	require_NoError(t, err)
	_, err = nc.Request(fmt.Sprintf(JSApiStreamLeaderStepDownT, "TEST"), data, time.Second)
	require_NoError(t, err)
	c.waitOnStreamLeader(globalAccountName, "TEST")

	// Move stream leader back.
	req.Placement.Preferred = sl.Name()
	data, err = json.Marshal(req)
	require_NoError(t, err)
	_, err = nc.Request(fmt.Sprintf(JSApiStreamLeaderStepDownT, "TEST"), data, time.Second)
	require_NoError(t, err)
	c.waitOnStreamLeader(globalAccountName, "TEST")

	nsl := c.streamLeader(globalAccountName, "TEST")
	require_True(t, nsl == sl)

	mset.ddMu.Lock()
	lenDdmap, lenDdarr := len(mset.ddmap), len(mset.ddarr)
	mset.ddMu.Unlock()
	require_Len(t, lenDdmap, 1)
	require_Len(t, lenDdarr, 1)

	// Now the publish should pass.
	_, err = js.Publish("foo", nil, nats.MsgId("msgId"))
	require_NoError(t, err)
}

func TestJetStreamClusterStreamLeaderChangeDedupeCleanupRace(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "R3S", 3)
	defer c.shutdown()

	nc, js := jsClientConnect(t, c.randomServer())
	defer nc.Close()

	_, err := js.AddStream(&nats.StreamConfig{
		Name:       "TEST",
		Subjects:   []string{"foo"},
		Replicas:   3,
		Duplicates: 2 * time.Minute,
	})
	require_NoError(t, err)

	sl := c.streamLeader(globalAccountName, "TEST")
	require_NotNil(t, sl)
	mset, err := sl.globalAccount().lookupStream("TEST")
	require_NoError(t, err)
	sjs := sl.getJetStream()
	require_NotNil(t, sjs)

	// Simulate an inflight proposal: holds clMu and is about to plant a seq=0 placeholder.
	mset.clMu.Lock()

	// In the meantime, we become leader again. processStreamLeaderChange must block on
	// clMu so the placeholder is in ddmap by the time the cleanup runs.
	done := make(chan struct{})
	go func() {
		defer close(done)
		sjs.processStreamLeaderChange(mset, true, 0)
	}()

	// Give the goroutine time to reach the clMu acquisition.
	time.Sleep(200 * time.Millisecond)
	select {
	case <-done:
		t.Fatal("processStreamLeaderChange should be blocked on clMu")
	default:
	}

	// Plant the seq=0 placeholder, simulating a proposal's diff.commit step
	// happening while the leader change is in flight.
	mset.ddMu.Lock()
	mset.storeMsgIdLocked(&ddentry{"msgId", 0, time.Now().UnixNano()})
	mset.ddMu.Unlock()

	// Release clMu so processStreamLeaderChange can proceed with cleanup.
	mset.clMu.Unlock()

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("processStreamLeaderChange did not complete after clMu released")
	}

	// The placeholder must have been cleaned up.
	mset.ddMu.Lock()
	defer mset.ddMu.Unlock()
	require_Len(t, len(mset.ddmap), 0)
	require_Len(t, len(mset.ddarr), 0)
}

func TestJetStreamClusterConsumeWithStartSequence(t *testing.T) {

	const (
		NumMessages         = 10
		ChosenSeq           = 5
		StreamName          = "TEST"
		StreamSubject       = "ORDERS.*"
		StreamSubjectPrefix = "ORDERS."
	)

	for _, ClusterSize := range []int{
		1, // Single server
		3, // 3-node cluster
	} {
		R := ClusterSize
		t.Run(
			fmt.Sprintf("Nodes:%d,Replicas:%d", ClusterSize, R),
			func(t *testing.T) {
				// This is the success condition for all sub-tests below
				var ExpectedMsgId = ""
				checkMessage := func(t *testing.T, msg *nats.Msg) {
					t.Helper()

					msgMeta, err := msg.Metadata()
					require_NoError(t, err)

					// Check sequence number
					require_Equal(t, msgMeta.Sequence.Stream, ChosenSeq)

					// Check message id
					require_NotEqual(t, ExpectedMsgId, "")
					require_Equal(t, msg.Header.Get(nats.MsgIdHdr), ExpectedMsgId)
				}

				checkRawMessage := func(t *testing.T, msg *nats.RawStreamMsg) {
					t.Helper()

					// Check sequence number
					require_Equal(t, msg.Sequence, ChosenSeq)

					// Check message id
					require_NotEqual(t, ExpectedMsgId, "")
					require_Equal(t, msg.Header.Get(nats.MsgIdHdr), ExpectedMsgId)
				}

				// Setup: start server or cluster, connect client
				var server *Server
				if ClusterSize == 1 {
					server = RunBasicJetStreamServer(t)
					defer server.Shutdown()
				} else {
					c := createJetStreamCluster(t, jsClusterTempl, "HUB", _EMPTY_, ClusterSize, 22020, true)
					defer c.shutdown()
					server = c.randomServer()
				}

				// Setup: connect
				var nc *nats.Conn
				var js nats.JetStreamContext
				nc, js = jsClientConnect(t, server)
				defer nc.Close()

				// Setup: create stream
				_, err := js.AddStream(&nats.StreamConfig{
					Replicas: R,
					Name:     StreamName,
					Subjects: []string{StreamSubject},
				})
				require_NoError(t, err)

				// Setup: populate stream
				buf := make([]byte, 100)
				for i := uint64(1); i <= NumMessages; i++ {
					msgId := nuid.Next()
					pubAck, err := js.Publish(StreamSubjectPrefix+strconv.Itoa(int(i)), buf, nats.MsgId(msgId))
					require_NoError(t, err)

					// Verify assumption made in tests below
					require_Equal(t, pubAck.Sequence, i)

					if i == ChosenSeq {
						// Save the expected message id for the chosen message
						ExpectedMsgId = msgId
					}
				}

				// Setup: create subscriptions, needs to be after stream creation or OptStartSeq could be clipped
				var preCreatedSub, preCreatedSubDurable *nats.Subscription
				{
					preCreatedSub, err = js.PullSubscribe(
						StreamSubject,
						"",
						nats.StartSequence(ChosenSeq),
					)
					require_NoError(t, err)
					defer func() {
						require_NoError(t, preCreatedSub.Unsubscribe())
					}()

					const Durable = "dlc_pre_created"
					c, err := js.AddConsumer(StreamName, &nats.ConsumerConfig{
						Durable:       Durable,
						DeliverPolicy: nats.DeliverByStartSequencePolicy,
						OptStartSeq:   ChosenSeq,
						Replicas:      R,
					})
					require_NoError(t, err)
					defer func() {
						require_NoError(t, js.DeleteConsumer(c.Stream, c.Name))
					}()

					preCreatedSubDurable, err = js.PullSubscribe(
						"",
						"",
						nats.Bind(StreamName, Durable),
					)
					require_NoError(t, err)
					defer func() {
						require_NoError(t, preCreatedSubDurable.Unsubscribe())
					}()
				}

				// Tests various ways to consume the stream starting at the ChosenSeq sequence

				t.Run(
					"DurableConsumer",
					func(t *testing.T) {
						const Durable = "dlc"
						c, err := js.AddConsumer(StreamName, &nats.ConsumerConfig{
							Durable:       Durable,
							DeliverPolicy: nats.DeliverByStartSequencePolicy,
							OptStartSeq:   ChosenSeq,
							Replicas:      R,
						})
						require_NoError(t, err)
						defer func() {
							require_NoError(t, js.DeleteConsumer(c.Stream, c.Name))
						}()

						sub, err := js.PullSubscribe(
							StreamSubject,
							Durable,
						)
						require_NoError(t, err)
						defer func() {
							require_NoError(t, sub.Unsubscribe())
						}()

						msgs, err := sub.Fetch(1)
						require_NoError(t, err)
						require_Equal(t, len(msgs), 1)

						checkMessage(t, msgs[0])
					},
				)

				t.Run(
					"DurableConsumerWithBind",
					func(t *testing.T) {
						const Durable = "dlc_bind"
						c, err := js.AddConsumer(StreamName, &nats.ConsumerConfig{
							Durable:       Durable,
							DeliverPolicy: nats.DeliverByStartSequencePolicy,
							OptStartSeq:   ChosenSeq,
							Replicas:      R,
						})
						require_NoError(t, err)
						defer func() {
							require_NoError(t, js.DeleteConsumer(c.Stream, c.Name))
						}()

						sub, err := js.PullSubscribe(
							"",
							"",
							nats.Bind(StreamName, Durable),
						)
						require_NoError(t, err)
						defer func() {
							require_NoError(t, sub.Unsubscribe())
						}()

						msgs, err := sub.Fetch(1)
						require_NoError(t, err)
						require_Equal(t, len(msgs), 1)

						checkMessage(t, msgs[0])
					},
				)

				t.Run(
					"PreCreatedDurableConsumerWithBind",
					func(t *testing.T) {
						msgs, err := preCreatedSubDurable.Fetch(1)
						require_NoError(t, err)
						require_Equal(t, len(msgs), 1)

						checkMessage(t, msgs[0])
					},
				)

				t.Run(
					"PullConsumer",
					func(t *testing.T) {
						sub, err := js.PullSubscribe(
							StreamSubject,
							"",
							nats.StartSequence(ChosenSeq),
						)
						require_NoError(t, err)
						defer func() {
							require_NoError(t, sub.Unsubscribe())
						}()

						msgs, err := sub.Fetch(1)
						require_NoError(t, err)
						require_Equal(t, len(msgs), 1)

						checkMessage(t, msgs[0])
					},
				)

				t.Run(
					"PreCreatedPullConsumer",
					func(t *testing.T) {
						msgs, err := preCreatedSub.Fetch(1)
						require_NoError(t, err)
						require_Equal(t, len(msgs), 1)

						checkMessage(t, msgs[0])
					},
				)

				t.Run(
					"SynchronousConsumer",
					func(t *testing.T) {
						sub, err := js.SubscribeSync(
							StreamSubject,
							nats.StartSequence(ChosenSeq),
						)
						if err != nil {
							return
						}
						require_NoError(t, err)
						defer func() {
							require_NoError(t, sub.Unsubscribe())
						}()

						msg, err := sub.NextMsg(1 * time.Second)
						require_NoError(t, err)
						checkMessage(t, msg)
					},
				)

				t.Run(
					"CallbackSubscribe",
					func(t *testing.T) {
						var waitGroup sync.WaitGroup
						waitGroup.Add(1)
						// To be populated by callback
						var receivedMsg *nats.Msg

						sub, err := js.Subscribe(
							StreamSubject,
							func(msg *nats.Msg) {
								// Save first message received
								if receivedMsg == nil {
									receivedMsg = msg
									waitGroup.Done()
								}
							},
							nats.StartSequence(ChosenSeq),
						)
						require_NoError(t, err)
						defer func() {
							require_NoError(t, sub.Unsubscribe())
						}()

						waitGroup.Wait()
						require_NotNil(t, receivedMsg)
						checkMessage(t, receivedMsg)
					},
				)

				t.Run(
					"ChannelSubscribe",
					func(t *testing.T) {
						msgChannel := make(chan *nats.Msg, 1)
						sub, err := js.ChanSubscribe(
							StreamSubject,
							msgChannel,
							nats.StartSequence(ChosenSeq),
						)
						require_NoError(t, err)
						defer func() {
							require_NoError(t, sub.Unsubscribe())
						}()

						msg := <-msgChannel
						checkMessage(t, msg)
					},
				)

				t.Run(
					"GetRawStreamMessage",
					func(t *testing.T) {
						rawMsg, err := js.GetMsg(StreamName, ChosenSeq)
						require_NoError(t, err)
						checkRawMessage(t, rawMsg)
					},
				)

				t.Run(
					"GetLastMessageBySubject",
					func(t *testing.T) {
						rawMsg, err := js.GetLastMsg(
							StreamName,
							fmt.Sprintf("ORDERS.%d", ChosenSeq),
						)
						require_NoError(t, err)
						checkRawMessage(t, rawMsg)
					},
				)
			},
		)
	}
}

func TestJetStreamClusterAckDeleted(t *testing.T) {

	const (
		NumMessages         = 10
		StreamName          = "TEST"
		StreamSubject       = "ORDERS.*"
		StreamSubjectPrefix = "ORDERS."
	)

	for _, ClusterSize := range []int{
		1, // Single server
		3, // 3-node cluster
	} {
		R := ClusterSize
		t.Run(
			fmt.Sprintf("Nodes:%d,Replicas:%d", ClusterSize, R),
			func(t *testing.T) {
				// Setup: start server or cluster, connect client
				var server *Server
				if ClusterSize == 1 {
					server = RunBasicJetStreamServer(t)
					defer server.Shutdown()
				} else {
					c := createJetStreamCluster(t, jsClusterTempl, "HUB", _EMPTY_, ClusterSize, 22020, true)
					defer c.shutdown()
					server = c.randomServer()
				}

				// Setup: connect
				var nc *nats.Conn
				var js nats.JetStreamContext
				nc, js = jsClientConnect(t, server)
				defer nc.Close()

				// Setup: create stream
				_, err := js.AddStream(&nats.StreamConfig{
					Replicas:  R,
					Name:      StreamName,
					Subjects:  []string{StreamSubject},
					Retention: nats.LimitsPolicy,
					Discard:   nats.DiscardOld,
					MaxMsgs:   1, // Only keep the latest message
				})
				require_NoError(t, err)

				// Setup: create durable consumer and subscription
				const Durable = "dlc"
				c, err := js.AddConsumer(StreamName, &nats.ConsumerConfig{
					Durable:       Durable,
					Replicas:      R,
					AckPolicy:     nats.AckExplicitPolicy,
					MaxAckPending: NumMessages,
				})
				require_NoError(t, err)
				defer func() {
					require_NoError(t, js.DeleteConsumer(c.Stream, c.Name))
				}()

				// Setup: create durable consumer subscription
				sub, err := js.PullSubscribe(
					"",
					"",
					nats.Bind(StreamName, Durable),
				)
				require_NoError(t, err)
				defer func() {
					require_NoError(t, sub.Unsubscribe())
				}()

				// Collect received and non-ACKed messages
				receivedMessages := make([]*nats.Msg, 0, NumMessages)

				buf := make([]byte, 100)
				for i := uint64(1); i <= NumMessages; i++ {
					// Publish one message
					msgId := nuid.Next()
					pubAck, err := js.Publish(
						StreamSubjectPrefix+strconv.Itoa(int(i)),
						buf,
						nats.MsgId(msgId),
					)
					require_NoError(t, err)
					require_Equal(t, pubAck.Sequence, i)

					// Consume message
					msgs, err := sub.Fetch(1)
					require_NoError(t, err)
					require_Equal(t, len(msgs), 1)

					// Validate message
					msg := msgs[0]
					require_Equal(t, msgs[0].Header.Get(nats.MsgIdHdr), msgId)

					// Validate message metadata
					msgMeta, err := msg.Metadata()
					require_NoError(t, err)
					// Check sequence number
					require_Equal(t, msgMeta.Sequence.Stream, i)

					// Save for ACK later
					receivedMessages = append(receivedMessages, msg)
				}

				// Verify stream state, expecting a single message due to limits
				streamInfo, err := js.StreamInfo(StreamName)
				require_NoError(t, err)
				require_Equal(t, streamInfo.State.Msgs, 1)

				// Verify consumer state, expecting ack floor corresponding to messages dropped
				consumerInfo, err := js.ConsumerInfo(StreamName, Durable)
				require_NoError(t, err)
				require_Equal(t, consumerInfo.NumAckPending, 1)
				require_Equal(t, consumerInfo.AckFloor.Stream, 9)
				require_Equal(t, consumerInfo.AckFloor.Consumer, 9)

				// ACK all messages (all except last have been dropped from the stream)
				for _, message := range receivedMessages {
					err := message.AckSync()
					require_NoError(t, err)
				}

				// Verify consumer state, all messages ACKed
				consumerInfo, err = js.ConsumerInfo(StreamName, Durable)
				require_NoError(t, err)
				require_Equal(t, consumerInfo.NumAckPending, 0)
				require_Equal(t, consumerInfo.AckFloor.Stream, 10)
				require_Equal(t, consumerInfo.AckFloor.Consumer, 10)
			},
		)
	}
}

func TestJetStreamClusterAPILimitDefault(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "R3S", 3)
	defer c.shutdown()

	for _, s := range c.servers {
		s.optsMu.RLock()
		lim := s.opts.JetStreamRequestQueueLimit
		ilim := s.opts.JetStreamInfoQueueLimit
		s.optsMu.RUnlock()

		require_Equal(t, lim, JSDefaultRequestQueueLimit)
		require_Equal(t, ilim, JSDefaultRequestQueueLimit)
		require_Equal(t, atomic.LoadInt64(&s.getJetStream().queueLimit), JSDefaultRequestQueueLimit)
		require_Equal(t, atomic.LoadInt64(&s.getJetStream().infoQueueLimit), JSDefaultRequestQueueLimit)
	}
}

func TestJetStreamClusterAPILimitAdvisory(t *testing.T) {
	// Hit the limit straight away.
	const queueLimit = 1

	config := `
		listen: 127.0.0.1:-1
		server_name: %s
		jetstream: {
			max_mem_store: 256MB
			max_file_store: 2GB
			store_dir: '%s'
			request_queue_limit: ` + fmt.Sprintf("%d", queueLimit) + `
		}
		cluster {
			name: %s
			listen: 127.0.0.1:%d
			routes = [%s]
		}
		accounts { $SYS { users = [ { user: "admin", pass: "s3cr3t!" } ] } }
    `
	c := createJetStreamClusterWithTemplate(t, config, "R3S", 3)
	defer c.shutdown()

	c.waitOnLeader()
	s := c.randomNonLeader()

	for _, s := range c.servers {
		lim := atomic.LoadInt64(&s.getJetStream().queueLimit)
		require_Equal(t, lim, queueLimit)
	}

	nc, _ := jsClientConnect(t, s)
	defer nc.Close()

	snc, _ := jsClientConnect(t, c.randomServer(), nats.UserInfo("admin", "s3cr3t!"))
	defer snc.Close()

	sub, err := snc.SubscribeSync(JSAdvisoryAPILimitReached)
	require_NoError(t, err)

	// There's a very slim chance that a worker could pick up a request between
	// pushing to and draining the queue, so make sure we've sent enough of them
	// to reliably trigger a drain and advisory.
	inbox := nc.NewRespInbox()
	total := 100
	for range total {
		require_NoError(t, nc.PublishMsg(&nats.Msg{
			Subject: fmt.Sprintf(JSApiConsumerListT, "TEST"),
			Reply:   inbox,
		}))
	}

	for range total {
		// Wait for the advisory to come in.
		msg, err := sub.NextMsg(time.Second * 5)
		require_NoError(t, err)
		var advisory JSAPILimitReachedAdvisory
		require_NoError(t, json.Unmarshal(msg.Data, &advisory))
		require_Equal(t, advisory.Domain, _EMPTY_) // No JetStream domain was set.
		if advisory.Dropped >= 1 {
			// We are done!
			return
		}
	}
	t.Fatal("Did not get any advisory with dropped > 0")
}

func TestJetStreamClusterPendingRequestsInJsz(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "R3S", 3)
	defer c.shutdown()

	c.waitOnLeader()
	metaleader := c.leader()

	sjs := metaleader.getJetStream()
	sjs.mu.Lock()
	sub := &subscription{
		subject: []byte("$JS.API.VERY_SLOW"),
		icb: func(sub *subscription, client *client, acc *Account, subject, reply string, rmsg []byte) {
			select {
			case <-client.srv.quitCh:
			case <-time.After(time.Second * 3):
			}
		},
	}
	err := metaleader.getJetStream().apiSubs.Insert(sub)
	sjs.mu.Unlock()

	require_NoError(t, err)

	nc, _ := jsClientConnect(t, c.randomNonLeader())
	defer nc.Close()

	inbox := nc.NewRespInbox()
	msg := &nats.Msg{
		Subject: "$JS.API.VERY_SLOW",
		Reply:   inbox,
	}

	// Fall short of hitting the API limit by a little bit,
	// otherwise the requests get drained away.
	for i := 0; i < JSDefaultRequestQueueLimit-10; i++ {
		require_NoError(t, nc.PublishMsg(msg))
	}

	// We could check before above published messages are received,
	// so allow some retries for pending messages to build up.
	checkFor(t, 2*time.Second, 100*time.Millisecond, func() error {
		jsz, err := metaleader.Jsz(nil)
		if err != nil {
			return err
		}
		if jsz.Meta == nil {
			return errors.New("jsz.Meta == nil")
		}
		if jsz.Meta.Pending == 0 {
			return errors.New("jsz.Meta.Pending == 0, expected pending requests")
		}
		return nil
	})

	snc, _ := jsClientConnect(t, c.randomServer(), nats.UserInfo("admin", "s3cr3t!"))
	defer snc.Close()

	ch := make(chan *nats.Msg, 1)
	ssub, err := snc.ChanSubscribe(fmt.Sprintf(serverStatsSubj, metaleader.ID()), ch)
	require_NoError(t, err)
	require_NoError(t, ssub.AutoUnsubscribe(1))

	msg = require_ChanRead(t, ch, time.Second*5)
	var m ServerStatsMsg
	require_NoError(t, json.Unmarshal(msg.Data, &m))
	require_True(t, m.Stats.JetStream != nil)
	require_NotEqual(t, m.Stats.JetStream.Meta.Pending, 0)
}

func TestJetStreamClusterConsumerReplicasAfterScale(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "R5S", 5)
	defer c.shutdown()

	nc, js := jsClientConnect(t, c.randomNonLeader())
	defer nc.Close()

	_, err := js.AddStream(&nats.StreamConfig{
		Name:     "TEST",
		Subjects: []string{"foo"},
		Replicas: 5,
	})
	require_NoError(t, err)

	// Put some messages in to test consumer state transfer.
	for i := 0; i < 100; i++ {
		js.PublishAsync("foo", []byte("ok"))
	}
	select {
	case <-js.PublishAsyncComplete():
	case <-time.After(5 * time.Second):
		t.Fatalf("Did not receive completion signal")
	}

	// Create four different consumers.
	// Normal where we inherit replicas from parent.
	ci, err := js.AddConsumer("TEST", &nats.ConsumerConfig{
		Durable:   "dur",
		AckPolicy: nats.AckExplicitPolicy,
	})
	require_NoError(t, err)
	require_Equal(t, ci.Config.Replicas, 0)
	require_Equal(t, len(ci.Cluster.Replicas), 4)

	// Ephemeral
	ci, err = js.AddConsumer("TEST", &nats.ConsumerConfig{
		AckPolicy: nats.AckExplicitPolicy,
	})
	require_NoError(t, err)
	require_Equal(t, ci.Config.Replicas, 0) // Legacy ephemeral is 0 here too.
	require_Equal(t, len(ci.Cluster.Replicas), 0)
	eName := ci.Name

	// R1
	ci, err = js.AddConsumer("TEST", &nats.ConsumerConfig{
		Durable:   "r1",
		AckPolicy: nats.AckExplicitPolicy,
		Replicas:  1,
	})
	require_NoError(t, err)
	require_Equal(t, ci.Config.Replicas, 1)
	require_Equal(t, len(ci.Cluster.Replicas), 0)

	// R3
	ci, err = js.AddConsumer("TEST", &nats.ConsumerConfig{
		Name:      "r3",
		AckPolicy: nats.AckExplicitPolicy,
		Replicas:  3,
	})
	require_NoError(t, err)
	require_Equal(t, ci.Config.Replicas, 3)
	require_Equal(t, len(ci.Cluster.Replicas), 2)

	// Now create some state on r1 consumer.
	sub, err := js.PullSubscribe("foo", "r1")
	require_NoError(t, err)

	fetch := rand.IntN(99) + 1 // Needs to be at least 1.
	msgs, err := sub.Fetch(fetch, nats.MaxWait(10*time.Second))
	require_NoError(t, err)
	require_Equal(t, len(msgs), fetch)
	ack := rand.IntN(fetch)
	for i := 0; i <= ack; i++ {
		msgs[i].AckSync()
	}
	r1ci, err := js.ConsumerInfo("TEST", "r1")
	require_NoError(t, err)
	r1ci.Delivered.Last, r1ci.AckFloor.Last = nil, nil

	// Now scale stream to R3.
	_, err = js.UpdateStream(&nats.StreamConfig{
		Name:     "TEST",
		Subjects: []string{"foo"},
		Replicas: 3,
	})
	require_NoError(t, err)

	c.waitOnStreamLeader(globalAccountName, "TEST")

	checkConsumerReplicas := func(t *testing.T, stream, consumer string, cor, clr int) {
		checkFor(t, 5*time.Second, 500*time.Millisecond, func() error {
			if ci, err = js.ConsumerInfo(stream, consumer); err != nil {
				return err
			}
			if ci.Config.Replicas != cor {
				return fmt.Errorf("config replicas %d != %d", ci.Config.Replicas, cor)
			}
			if ci.Cluster == nil {
				return fmt.Errorf("cluster nil")
			}
			if len(ci.Cluster.Replicas) != clr {
				return fmt.Errorf("cluster replica peers %d != %d", len(ci.Cluster.Replicas), clr)
			}
			return nil
		})
	}

	// Now check each.
	c.waitOnConsumerLeader(globalAccountName, "TEST", "dur")
	checkConsumerReplicas(t, "TEST", "dur", 0, 2)

	c.waitOnConsumerLeader(globalAccountName, "TEST", eName)
	checkConsumerReplicas(t, "TEST", eName, 0, 0)

	c.waitOnConsumerLeader(globalAccountName, "TEST", "r1")
	checkConsumerReplicas(t, "TEST", "r1", 1, 0)

	// Now check that state transferred correctly.
	ci.Delivered.Last, ci.AckFloor.Last = nil, nil
	if ci.Delivered != r1ci.Delivered {
		t.Fatalf("Delivered state for R1 incorrect, wanted %+v got %+v",
			r1ci.Delivered, ci.Delivered)
	}
	if ci.AckFloor != r1ci.AckFloor {
		t.Fatalf("AckFloor state for R1 incorrect, wanted %+v got %+v",
			r1ci.AckFloor, ci.AckFloor)
	}

	c.waitOnConsumerLeader(globalAccountName, "TEST", "r3")
	c.waitOnConsumerLeader(globalAccountName, "TEST", "r1")
	checkConsumerReplicas(t, "TEST", "r3", 3, 2)
}

func TestJetStreamClusterConsumerReplicasAfterScaleMoveConsumer(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "R3", 3)
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

	sub, err := js.PullSubscribe(_EMPTY_, "CONSUMER", nats.BindStream("TEST"), nats.ConsumerReplicas(1))
	require_NoError(t, err)
	defer sub.Drain()

	msgs, err := sub.Fetch(1)
	require_NoError(t, err)
	require_Len(t, len(msgs), 1)
	require_NoError(t, msgs[0].AckSync())

	// The consumer will have received anc acked one message.
	ci, err := js.ConsumerInfo("TEST", "CONSUMER")
	require_NoError(t, err)
	require_Equal(t, ci.Delivered.Stream, 1)
	require_Equal(t, ci.AckFloor.Stream, 1)
	ci.AckFloor.Last = nil
	consumerLeader := ci.Cluster.Leader

	checkFor(t, 2*time.Second, 200*time.Millisecond, func() error {
		si, err := js.StreamInfo("TEST")
		if err != nil {
			return err
		}
		if si.Cluster.Leader != consumerLeader {
			return nil
		}
		_, err = nc.Request(fmt.Sprintf(JSApiStreamLeaderStepDownT, "TEST"), nil, time.Second)
		if err != nil {
			return err
		}
		return errors.New("expected leader to not be equal")
	})

	// Scaling the stream down to 1 replica means the R1 consumer will need to move to the current stream leader.
	cfg.Replicas = 1
	_, err = js.UpdateStream(cfg)
	require_NoError(t, err)

	// Check that the consumer state wasn't lost.
	checkFor(t, 2*time.Second, 200*time.Millisecond, func() error {
		ci2, err := js.ConsumerInfo("TEST", "CONSUMER")
		if err != nil {
			return err
		}
		ci2.AckFloor.Last = nil
		if !reflect.DeepEqual(ci.AckFloor, ci2.AckFloor) {
			return fmt.Errorf("%+v vs %+v", ci.AckFloor, ci2.AckFloor)
		}
		return nil
	})
}

func TestJetStreamClusterDesyncAfterQuitDuringCatchup(t *testing.T) {
	for title, test := range map[string]func(s *Server, rn RaftNode){
		"RAFT": func(s *Server, rn RaftNode) {
			rn.Stop()
			rn.WaitForStop()
		},
		"server": func(s *Server, rn RaftNode) {
			s.running.Store(false)
		},
	} {
		t.Run(title, func(t *testing.T) {
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

			// Wait for all servers to have applied everything up to this point.
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
					_, _, applied := mset.raftNode().Progress()
					if applied != 1 {
						return fmt.Errorf("expected applied to be %d, got %d", 1, applied)
					}
				}
				return nil
			})

			rs := c.randomNonStreamLeader(globalAccountName, "TEST")
			acc, err := rs.lookupAccount(globalAccountName)
			require_NoError(t, err)
			mset, err := acc.lookupStream("TEST")
			require_NoError(t, err)

			rn := mset.raftNode()
			snap, err := json.Marshal(streamSnapshot{Msgs: 1, Bytes: 1, FirstSeq: 100, LastSeq: 100, Failed: 0, Deleted: nil})
			require_NoError(t, err)
			esm := encodeStreamMsgAllowCompress("foo", _EMPTY_, nil, nil, 0, 0, false)

			// Lock stream so that we can go into processSnapshot but must wait for this to unlock.
			mset.mu.Lock()
			var unlocked bool
			defer func() {
				if !unlocked {
					mset.mu.Unlock()
				}
			}()

			_, err = rn.ApplyQ().push(newCommittedEntry(100, []*Entry{newEntry(EntrySnapshot, snap)}))
			require_NoError(t, err)
			_, err = rn.ApplyQ().push(newCommittedEntry(101, []*Entry{newEntry(EntryNormal, esm)}))
			require_NoError(t, err)

			// Waiting for the apply queue entry to be captured in monitorStream first.
			time.Sleep(time.Second)

			// Set commit to a very high number, just so that we allow upping Applied()
			n := rn.(*raft)
			n.Lock()
			n.commit = 1000
			n.Unlock()

			// Now stop the underlying RAFT node/server so processSnapshot must exit because of it.
			test(rs, rn)
			mset.mu.Unlock()
			unlocked = true

			// Allow some time for the applied number to be updated, in which case it's an error.
			time.Sleep(time.Second)
			_, _, applied := mset.raftNode().Progress()
			require_Equal(t, applied, 1)
		})
	}
}

func TestJetStreamClusterDesyncAfterErrorDuringCatchup(t *testing.T) {
	tests := []struct {
		title            string
		onErrorCondition func(server *Server, mset *stream)
	}{
		{
			title: "TooManyRetries",
			onErrorCondition: func(server *Server, mset *stream) {
				// Too many retries while processing snapshot is considered a cluster reset.
				// If a leader is temporarily unavailable we shouldn't blow away our state.
				require_True(t, isClusterResetErr(errCatchupTooManyRetries))
				mset.resetClusteredState(mset.raftNode(), errCatchupTooManyRetries)
			},
		},
		{
			title: "AbortedNoLeader",
			onErrorCondition: func(server *Server, mset *stream) {
				for _, n := range server.raftNodes {
					rn := n.(*raft)
					if rn.accName == "$G" {
						rn.Lock()
						rn.updateLeader(noLeader)
						rn.Unlock()
					}
				}

				// Processing a snapshot while there's no leader elected is considered a cluster reset.
				// If a leader is temporarily unavailable we shouldn't blow away our state.
				var snap StreamReplicatedState
				snap.LastSeq = 1_000      // ensure we can catchup based on the snapshot
				appliedIndex := uint64(0) // incorrect index, but doesn't matter for this test
				err := mset.processSnapshot(&snap, appliedIndex)
				require_True(t, errors.Is(err, errCatchupAbortedNoLeader))
				require_True(t, isClusterResetErr(err))
				mset.resetClusteredState(mset.raftNode(), err)
			},
		},
	}

	for _, test := range tests {
		t.Run(test.title, func(t *testing.T) {
			c := createJetStreamClusterExplicit(t, "R3S", 3)
			defer c.shutdown()

			nc, js := jsClientConnect(t, c.randomServer())
			defer nc.Close()

			si, err := js.AddStream(&nats.StreamConfig{
				Name:     "TEST",
				Subjects: []string{"foo"},
				Replicas: 3,
			})
			require_NoError(t, err)

			streamLeader := si.Cluster.Leader
			streamLeaderServer := c.serverByName(streamLeader)
			nc.Close()
			nc, js = jsClientConnect(t, streamLeaderServer)
			defer nc.Close()

			servers := slices.DeleteFunc([]string{"S-1", "S-2", "S-3"}, func(s string) bool {
				return s == streamLeader
			})

			// Publish 10 messages.
			for i := 0; i < 10; i++ {
				pubAck, err := js.Publish("foo", []byte("ok"))
				require_NoError(t, err)
				require_Equal(t, pubAck.Sequence, uint64(i+1))
			}

			outdatedServerName := servers[0]
			clusterResetServerName := servers[1]

			outdatedServer := c.serverByName(outdatedServerName)
			outdatedServer.Shutdown()
			outdatedServer.WaitForShutdown()

			// Publish 10 more messages, one server will be behind.
			for i := 0; i < 10; i++ {
				pubAck, err := js.Publish("foo", []byte("ok"))
				require_NoError(t, err)
				require_Equal(t, pubAck.Sequence, uint64(i+11))
			}

			// We will not need the client anymore.
			nc.Close()

			// Shutdown stream leader so one server remains.
			streamLeaderServer.Shutdown()
			streamLeaderServer.WaitForShutdown()

			clusterResetServer := c.serverByName(clusterResetServerName)
			acc, err := clusterResetServer.lookupAccount(globalAccountName)
			require_NoError(t, err)
			mset, err := acc.lookupStream("TEST")
			require_NoError(t, err)

			// Run error condition.
			test.onErrorCondition(clusterResetServer, mset)

			// Stream leader stays offline, we only start the server with missing stream data.
			// We expect that the reset server must not allow the outdated server to become leader, as that would result in desync.
			c.restartServer(outdatedServer)
			c.waitOnStreamLeader(globalAccountName, "TEST")

			// Outdated server must NOT become the leader.
			newStreamLeaderServer := c.streamLeader(globalAccountName, "TEST")
			require_Equal(t, newStreamLeaderServer.Name(), clusterResetServerName)
		})
	}
}

func TestJetStreamClusterConsumerDesyncAfterErrorDuringStreamCatchup(t *testing.T) {
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

	ci, err := js.AddConsumer("TEST", &nats.ConsumerConfig{
		Durable:   "CONSUMER",
		AckPolicy: nats.AckExplicitPolicy,
	})
	require_NoError(t, err)

	consumerLeader := ci.Cluster.Leader
	consumerLeaderServer := c.serverByName(consumerLeader)
	nc.Close()
	nc, js = jsClientConnect(t, consumerLeaderServer)
	defer nc.Close()

	servers := slices.DeleteFunc([]string{"S-1", "S-2", "S-3"}, func(s string) bool {
		return s == consumerLeader
	})

	// Publish 1 message, consume, and ack it.
	pubAck, err := js.Publish("foo", []byte("ok"))
	require_NoError(t, err)
	require_Equal(t, pubAck.Sequence, 1)
	checkFor(t, time.Second, 100*time.Millisecond, func() error {
		return checkState(t, c, globalAccountName, "TEST")
	})

	sub, err := js.PullSubscribe("foo", "CONSUMER")
	require_NoError(t, err)
	defer sub.Drain()

	msgs, err := sub.Fetch(1)
	require_NoError(t, err)
	require_Len(t, len(msgs), 1)
	require_NoError(t, msgs[0].AckSync())

	outdatedServerName := servers[0]
	clusterResetServerName := servers[1]

	outdatedServer := c.serverByName(outdatedServerName)
	outdatedServer.Shutdown()
	outdatedServer.WaitForShutdown()

	// Publish and ack another message, one server will be behind.
	pubAck, err = js.Publish("foo", []byte("ok"))
	require_NoError(t, err)
	require_Equal(t, pubAck.Sequence, 2)
	checkFor(t, time.Second, 100*time.Millisecond, func() error {
		return checkState(t, c, globalAccountName, "TEST")
	})

	msgs, err = sub.Fetch(1)
	require_NoError(t, err)
	require_Len(t, len(msgs), 1)
	require_NoError(t, msgs[0].AckSync())

	// We will not need the client anymore.
	nc.Close()

	// Shutdown consumer leader so one server remains.
	consumerLeaderServer.Shutdown()
	consumerLeaderServer.WaitForShutdown()

	clusterResetServer := c.serverByName(clusterResetServerName)
	acc, err := clusterResetServer.lookupAccount(globalAccountName)
	require_NoError(t, err)
	mset, err := acc.lookupStream("TEST")
	require_NoError(t, err)

	// Run error condition.
	mset.resetClusteredState(mset.raftNode(), nil)

	// Consumer leader stays offline, we only start the server with missing stream/consumer data.
	// We expect that the reset server must not allow the outdated server to become leader, as that would result in desync.
	c.restartServer(outdatedServer)
	c.waitOnConsumerLeader(globalAccountName, "TEST", "CONSUMER")

	// Outdated server must NOT become the leader.
	newConsumerLeaderServer := c.consumerLeader(globalAccountName, "TEST", "CONSUMER")
	require_Equal(t, newConsumerLeaderServer.Name(), clusterResetServerName)
}

func TestJetStreamClusterDesyncAfterEofFromOldStreamLeader(t *testing.T) {
	test := func(t *testing.T, eof bool) {
		c := createJetStreamClusterExplicit(t, "R5S", 5)
		defer c.shutdown()

		cs := c.randomServer()
		nc, js := jsClientConnect(t, cs)
		defer nc.Close()

		_, err := js.AddStream(&nats.StreamConfig{
			Name:     "TEST",
			Subjects: []string{"foo"},
			Replicas: 5,
		})
		require_NoError(t, err)

		sl := c.streamLeader(globalAccountName, "TEST")
		var rs *Server
		var catchup *Server
		for _, s := range c.servers {
			if s != sl && s != cs {
				if rs == nil {
					rs = s
				} else {
					catchup = s
					break
				}
			}
		}

		// Shutdown server that needs to catch up, so it gets a snapshot from the leader after restart.
		catchup.Shutdown()

		// One message is received and applied by all replicas.
		_, err = js.Publish("foo", nil)
		require_NoError(t, err)
		checkFor(t, 2*time.Second, 200*time.Millisecond, func() error {
			return checkState(t, c, globalAccountName, "TEST")
		})

		// Disable Raft and start cluster subs for server, simulating an old leader with an outdated log.
		acc, err := rs.lookupAccount(globalAccountName)
		require_NoError(t, err)
		mset, err := acc.lookupStream("TEST")
		require_NoError(t, err)
		mset.startClusterSubs()
		rn := mset.raftNode()
		rn.Stop()
		rn.WaitForStop()

		// Temporarily disable cluster subs for this test.
		// Normally due to multiple cluster subs responses will interleave, but this is simpler for this test.
		acc, err = sl.lookupAccount(globalAccountName)
		require_NoError(t, err)
		mset, err = acc.lookupStream("TEST")
		require_NoError(t, err)
		mset.stopClusterSubs()

		// Publish another message that the old leader will not get.
		_, err = js.Publish("foo", nil)
		require_NoError(t, err)
		require_NoError(t, sl.JetStreamSnapshotStream(globalAccountName, "TEST"))

		sa := sl.getJetStream().streamAssignment(globalAccountName, "TEST")
		require_NotNil(t, sa)

		// Send EOF immediately to requesting server, otherwise the server needs to time out and retry.
		if eof {
			snc, err := nats.Connect(rs.ClientURL(), nats.UserInfo("admin", "s3cr3t!"))
			require_NoError(t, err)
			defer snc.Close()

			sub, err := snc.Subscribe(sa.Sync, func(msg *nats.Msg) {
				// EOF
				rs.sendInternalMsgLocked(msg.Reply, _EMPTY_, nil, nil)
			})
			require_NoError(t, err)
			defer sub.Drain()
		}

		// Restart server so it starts catching up.
		catchup = c.restartServer(catchup)

		// Wait for server to start catching up.
		// This shouldn't be a problem. The server should retry catchup, recognizing it wasn't caught up fully.
		checkFor(t, 2*time.Second, 200*time.Millisecond, func() error {
			if a, err := catchup.lookupAccount(globalAccountName); err != nil {
				return err
			} else if m, err := a.lookupStream("TEST"); err != nil {
				return err
			} else if !m.isCatchingUp() {
				return errors.New("stream not catching up")
			}
			return nil
		})

		// Stop old leader, and re-enable cluster subs on proper leader.
		rs.Shutdown()
		mset.startClusterSubs()

		// Server should automatically restart catchup and get the missing data.
		checkFor(t, 10*time.Second, 200*time.Millisecond, func() error {
			return checkState(t, c, globalAccountName, "TEST")
		})
	}

	t.Run("eof", func(t *testing.T) { test(t, true) })
	t.Run("retry", func(t *testing.T) { test(t, false) })
}

func TestJetStreamClusterReservedResourcesAccountingAfterClusterReset(t *testing.T) {
	for _, clusterResetErr := range []error{errLastSeqMismatch, errFirstSequenceMismatch} {
		t.Run(clusterResetErr.Error(), func(t *testing.T) {
			c := createJetStreamClusterExplicit(t, "R3S", 3)
			defer c.shutdown()

			nc, js := jsClientConnect(t, c.randomServer())
			defer nc.Close()

			maxBytes := int64(1024 * 1024 * 1024)
			_, err := js.AddStream(&nats.StreamConfig{
				Name:     "TEST",
				Subjects: []string{"foo"},
				Replicas: 3,
				MaxBytes: maxBytes,
			})
			require_NoError(t, err)

			sl := c.streamLeader(globalAccountName, "TEST")

			mem, store, err := sl.JetStreamReservedResources()
			require_NoError(t, err)
			require_Equal(t, mem, 0)
			require_Equal(t, store, maxBytes)

			acc, err := sl.lookupAccount(globalAccountName)
			require_NoError(t, err)
			mset, err := acc.lookupStream("TEST")
			require_NoError(t, err)

			sjs := sl.getJetStream()
			rn := mset.raftNode()
			sa := mset.streamAssignment()
			sjs.mu.RLock()
			saGroupNode := sa.Group.node
			sjs.mu.RUnlock()
			require_NotNil(t, sa)
			require_Equal(t, rn, saGroupNode)

			require_True(t, mset.resetClusteredState(mset.raftNode(), clusterResetErr))

			checkFor(t, 5*time.Second, 500*time.Millisecond, func() error {
				sjs.mu.RLock()
				defer sjs.mu.RUnlock()
				if sa.Group.node == nil || sa.Group.node == saGroupNode {
					return errors.New("waiting for reset to complete")
				}
				return nil
			})

			mem, store, err = sl.JetStreamReservedResources()
			require_NoError(t, err)
			require_Equal(t, mem, 0)
			require_Equal(t, store, maxBytes)
		})
	}
}

func TestJetStreamClusterHardKillAfterStreamAdd(t *testing.T) {
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

	// Simulate being hard killed by:
	// 1. copy directories before shutdown
	copyToSrcMap := make(map[string]string)
	for _, s := range c.servers {
		sd := s.StoreDir()
		copySd := path.Join(t.TempDir(), JetStreamStoreDir)
		err = copyDir(t, copySd, sd)
		require_NoError(t, err)
		copyToSrcMap[copySd] = sd
	}

	// 2. stop all
	nc.Close()
	c.stopAll()

	// 3. revert directories to before shutdown
	for cp, dest := range copyToSrcMap {
		err = os.RemoveAll(dest)
		require_NoError(t, err)
		err = copyDir(t, dest, cp)
		require_NoError(t, err)
	}

	// 4. restart
	c.restartAll()
	c.waitOnAllCurrent()
	// The stream was added and applied before the kill, it recovers with the meta layer.
	c.waitOnStreamLeader(globalAccountName, "TEST")

	nc, js = jsClientConnect(t, c.randomServer())
	defer nc.Close()

	// Stream should exist still and not be removed after hard killing all servers, so expect no error.
	_, err = js.StreamInfo("TEST")
	require_NoError(t, err)
}

func TestJetStreamClusterDesyncAfterPublishToLeaderWithoutQuorum(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "R3S", 3)
	defer c.shutdown()

	nc, js := jsClientConnect(t, c.randomServer())
	defer nc.Close()

	si, err := js.AddStream(&nats.StreamConfig{
		Name:     "TEST",
		Subjects: []string{"foo"},
		Replicas: 3,
	})
	require_NoError(t, err)

	streamLeader := si.Cluster.Leader
	streamLeaderServer := c.serverByName(streamLeader)
	nc.Close()
	nc, js = jsClientConnect(t, streamLeaderServer)
	defer nc.Close()

	servers := slices.DeleteFunc([]string{"S-1", "S-2", "S-3"}, func(s string) bool {
		return s == streamLeader
	})

	// Stop followers so further publishes will not have quorum.
	followerName1 := servers[0]
	followerName2 := servers[1]
	followerServer1 := c.serverByName(followerName1)
	followerServer2 := c.serverByName(followerName2)
	followerServer1.Shutdown()
	followerServer2.Shutdown()
	followerServer1.WaitForShutdown()
	followerServer2.WaitForShutdown()

	// Although this request will time out, it will be added to the stream leader's WAL.
	_, err = js.Publish("foo", []byte("first"), nats.AckWait(time.Second))
	require_NotNil(t, err)
	require_Equal(t, err, nats.ErrTimeout)

	// Now shut down the leader as well.
	nc.Close()
	streamLeaderServer.Shutdown()
	streamLeaderServer.WaitForShutdown()

	// Only restart the (previous) followers.
	followerServer1 = c.restartServer(followerServer1)
	c.restartServer(followerServer2)
	c.waitOnStreamLeader(globalAccountName, "TEST")

	nc, js = jsClientConnect(t, followerServer1)
	defer nc.Close()

	// Publishing a message will now have quorum.
	pubAck, err := js.Publish("foo", []byte("first, this is a retry"))
	require_NoError(t, err)
	require_Equal(t, pubAck.Sequence, 1)

	// Bring up the previous stream leader.
	c.restartServer(streamLeaderServer)
	c.waitOnAllCurrent()
	c.waitOnStreamLeader(globalAccountName, "TEST")

	// Check all servers ended up with the last published message, which had quorum.
	checkFor(t, 3*time.Second, 250*time.Millisecond, func() error {
		for _, s := range c.servers {
			acc, err := s.lookupAccount(globalAccountName)
			if err != nil {
				return err
			}
			mset, err := acc.lookupStream("TEST")
			if err != nil {
				return err
			}
			state := mset.state()
			if state.Msgs != 1 || state.Bytes != 55 {
				return fmt.Errorf("stream state didn't match, got %d messages with %d bytes", state.Msgs, state.Bytes)
			}
		}
		return nil
	})
}

func TestJetStreamClusterPreserveWALDuringCatchupWithMatchingTerm(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "R3S", 3)
	defer c.shutdown()

	nc, js := jsClientConnect(t, c.randomServer())
	defer nc.Close()

	checkConsistency := func() {
		t.Helper()
		checkFor(t, 3*time.Second, 250*time.Millisecond, func() error {
			for _, s := range c.servers {
				acc, err := s.lookupAccount(globalAccountName)
				if err != nil {
					return err
				}
				mset, err := acc.lookupStream("TEST")
				if err != nil {
					return err
				}
				state := mset.state()
				if state.Msgs != 3 || state.Bytes != 99 {
					return fmt.Errorf("stream state didn't match, got %d messages with %d bytes", state.Msgs, state.Bytes)
				}
			}
			return nil
		})
	}

	_, err := js.AddStream(&nats.StreamConfig{
		Name:     "TEST",
		Subjects: []string{"foo"},
		Replicas: 3,
	})
	require_NoError(t, err)

	sl := c.streamLeader(globalAccountName, "TEST")
	require_NoError(t, err)
	acc, err := sl.lookupAccount(globalAccountName)
	require_NoError(t, err)
	mset, err := acc.lookupStream("TEST")
	require_NoError(t, err)
	rn := mset.raftNode().(*raft)
	leaderId := rn.ID()

	for i := 0; i < 3; i++ {
		_, err = js.Publish("foo", nil)
		require_NoError(t, err)
	}
	nc.Close()
	checkConsistency()

	// Pick one server that will only store a part of the messages in its WAL.
	rs := c.randomNonStreamLeader(globalAccountName, "TEST")
	acc, err = rs.lookupAccount(globalAccountName)
	require_NoError(t, err)
	mset, err = acc.lookupStream("TEST")
	require_NoError(t, err)
	rn = mset.raftNode().(*raft)
	index, commit, _ := rn.Progress()
	require_Equal(t, index, 4)
	require_Equal(t, index, commit)

	// We'll simulate as-if the last message was never received/stored.
	// Will need to truncate the stream, correct lseq (so the msg isn't skipped) and truncate the WAL.
	// This will simulate that the RAFT layer can restore it.
	mset.mu.Lock()
	mset.lseq--
	err = mset.store.Truncate(2)
	mset.mu.Unlock()
	require_NoError(t, err)
	rn.Lock()
	rn.truncateWAL(rn.pterm, rn.pindex-1)
	rn.Unlock()

	// Check all servers ended up with all published messages, which had quorum.
	checkConsistency()

	// Check that all entries came from the expected leader.
	for _, n := range rs.raftNodes {
		rn := n.(*raft)
		if rn.accName == globalAccountName {
			ae, err := rn.loadEntry(1)
			require_NoError(t, err)
			require_Equal(t, ae.leader, leaderId)

			ae, err = rn.loadEntry(2)
			require_NoError(t, err)
			require_Equal(t, ae.leader, leaderId)

			ae, err = rn.loadEntry(3)
			require_NoError(t, err)
			require_Equal(t, ae.leader, leaderId)
		}
	}
}

func TestJetStreamClusterDesyncAfterRestartReplacesLeaderSnapshot(t *testing.T) {
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

	// Reconnect to the leader.
	leader := c.streamLeader(globalAccountName, "TEST")
	nc.Close()
	nc, js = jsClientConnect(t, leader)
	defer nc.Close()

	lookupStream := func(s *Server) *stream {
		t.Helper()
		acc, err := s.lookupAccount(globalAccountName)
		require_NoError(t, err)
		mset, err := acc.lookupStream("TEST")
		require_NoError(t, err)
		return mset
	}

	// Stop one follower so it lags behind.
	rs := c.randomNonStreamLeader(globalAccountName, "TEST")
	mset := lookupStream(rs)
	n := mset.node.(*raft)
	followerSnapshots := path.Join(n.sd, snapshotsDir)
	rs.Shutdown()
	rs.WaitForShutdown()

	// Move the stream forward so the follower requires a snapshot.
	err = js.PurgeStream("TEST", &nats.StreamPurgeRequest{Sequence: 10})
	require_NoError(t, err)
	_, err = js.Publish("foo", nil)
	require_NoError(t, err)

	// Install a snapshot on the leader, ensuring RAFT entries are compacted and a snapshot remains.
	mset = lookupStream(leader)
	n = mset.node.(*raft)
	err = n.InstallSnapshot(mset.stateSnapshot(), false)
	require_NoError(t, err)

	c.stopAll()

	// Replace follower snapshot with the leader's.
	// This simulates the follower coming online, getting a snapshot from the leader after which it goes offline.
	leaderSnapshots := path.Join(n.sd, snapshotsDir)
	err = os.RemoveAll(followerSnapshots)
	require_NoError(t, err)
	err = copyDir(t, followerSnapshots, leaderSnapshots)
	require_NoError(t, err)

	// Start the follower, it will load the snapshot from the leader.
	rs = c.restartServer(rs)

	// Shutting down must check that the leader's snapshot is not overwritten.
	rs.Shutdown()
	rs.WaitForShutdown()

	// Now start all servers back up.
	c.restartAll()
	c.waitOnAllCurrent()

	checkFor(t, 10*time.Second, 500*time.Millisecond, func() error {
		return checkState(t, c, globalAccountName, "TEST")
	})
}

func TestJetStreamClusterKeepRaftStateIfStreamCreationFailedDuringShutdown(t *testing.T) {
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

	// Capture RAFT storage directory and JetStream handle before shutdown.
	s := c.randomNonStreamLeader(globalAccountName, "TEST")
	acc, err := s.lookupAccount(globalAccountName)
	require_NoError(t, err)
	mset, err := acc.lookupStream("TEST")
	require_NoError(t, err)
	sd := mset.node.(*raft).sd
	jss := s.getJetStream()

	// Shutdown the server.
	// Normally there are no actions taken anymore after shutdown completes,
	// but still do so to simulate actions taken while shutdown is in progress.
	s.Shutdown()
	s.WaitForShutdown()

	// Check RAFT state is kept.
	files, err := os.ReadDir(sd)
	require_NoError(t, err)
	require_True(t, len(files) > 0)

	// Simulate server shutting down, JetStream being disabled and a stream being created.
	sa := &streamAssignment{
		Config: &StreamConfig{Name: "TEST"},
		Group:  &raftGroup{node: &raft{}},
	}
	jss.processClusterCreateStream(acc, sa)

	// Check RAFT state is not deleted due to failing stream creation.
	files, err = os.ReadDir(sd)
	require_NoError(t, err)
	require_True(t, len(files) > 0)
}

func TestJetStreamClusterKeepRaftStateIfStreamUpdateFailed(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "R3S", 3)
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

	for range 10 {
		_, err = js.Publish("foo", []byte("hello"))
		require_NoError(t, err)
	}

	// Operate on a follower that is a member of the group.
	rs := c.randomNonStreamLeader(globalAccountName, "TEST")
	acc, err := rs.lookupAccount(globalAccountName)
	require_NoError(t, err)
	mset, err := acc.lookupStream("TEST")
	require_NoError(t, err)
	node := mset.raftNode()
	require_NotNil(t, node)
	rn := node.(*raft)
	sd := rn.sd
	jss := rs.getJetStream()

	// RAFT state exists before we apply the bad update.
	files, err := os.ReadDir(sd)
	require_NoError(t, err)
	require_True(t, len(files) > 0)

	// Craft a committed assignment that reuses the SAME raft group (so the node
	// gets resolved to the existing running node) but carries an invalid config
	// change (storage type) that updateWithAdvisory will reject. This simulates
	// an assignment that reaches a member without going through the API check.
	osa := mset.streamAssignment()
	require_NotNil(t, osa)
	sa := osa.copyGroup()
	sa.Group.node = nil
	ncfg := *osa.Config
	ncfg.Storage = MemoryStorage
	sa.Config = &ncfg

	jss.processClusterCreateStream(acc, sa)

	// The raft node must be stopped, NOT deleted: no data loss. Its on-disk
	// state (WAL + store dir) must be preserved so it can recover later.
	require_False(t, rn.IsDeleted())
	require_Equal(t, rn.State(), Closed)
	files, err = os.ReadDir(sd)
	require_NoError(t, err)
	require_True(t, len(files) > 0)

	// The stream must be stopped on this server rather than left running with a
	// config the rest of the cluster no longer agrees on.
	require_True(t, mset.closed.Load())
	_, err = acc.lookupStream("TEST")
	require_Error(t, err)

	// The stream remains available cluster-wide via the other peers, and its
	// data is intact.
	_, err = js.Publish("foo", []byte("after"))
	require_NoError(t, err)
	si, err := js.StreamInfo("TEST")
	require_NoError(t, err)
	require_Equal(t, si.State.Msgs, 11)
}

func TestJetStreamClusterMetaSnapshotReCreateConsistency(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "R3S", 3)
	defer c.shutdown()

	nc, js := jsClientConnect(t, c.randomServer())
	defer nc.Close()

	scfg := &nats.StreamConfig{Name: "TEST", Replicas: 3}
	_, err := js.AddStream(scfg)
	require_NoError(t, err)

	ccfg := &nats.ConsumerConfig{Name: "consumer", Replicas: 3}
	_, err = js.AddConsumer("TEST", ccfg)
	require_NoError(t, err)

	ml := c.leader()
	mjs := ml.getJetStream()
	mjs.mu.Lock()
	sa := mjs.streamAssignment(globalAccountName, "TEST")
	ca := mjs.consumerAssignment(globalAccountName, "TEST", "consumer")

	oldStreamGroup := sa.Group.Name
	oldConsumerGroup := ca.Group.Name
	streamDelete := encodeDeleteStreamAssignment(sa)

	csa := sa.copyGroup()
	cca := ca.copyGroup()
	csa.Group.Name, csa.Config.Replicas = "new-group", 1
	cca.Group.Name, cca.Config.Replicas = "new-group", 1
	streamAdd := encodeAddStreamAssignment(csa)
	consumerAdd := encodeAddConsumerAssignment(cca)
	mjs.mu.Unlock()

	// Get the snapshot before removing the stream below so we can recover fresh.
	snap, _, _, err := mjs.metaSnapshot()
	require_NoError(t, err)
	require_NoError(t, js.DeleteStream("TEST"))
	nc.Close()

	ru := &recoveryUpdates{
		removeStreams:   make(map[string]*streamAssignment),
		removeConsumers: make(map[string]map[string]*consumerAssignment),
		addStreams:      make(map[string]*streamAssignment),
		updateStreams:   make(map[string]*streamAssignment),
		updateConsumers: make(map[string]map[string]*consumerAssignment),
	}

	// Simulate recovering:
	// - snapshot with a stream and consumer
	// - normal entry deleting the stream
	// - normal entry re-adding the stream and consumer under different configs
	// This should result in a consistent state.
	mjs.mu.Lock()
	mjs.metaRecovering = true
	mjs.mu.Unlock()
	_, _, err = mjs.applyMetaEntries([]*Entry{
		newEntry(EntrySnapshot, snap),
		newEntry(EntryNormal, streamDelete),
		newEntry(EntryNormal, streamAdd),
		newEntry(EntryNormal, consumerAdd),
	}, ru)
	require_NoError(t, err)

	// Recovery should contain the stream and consumer create.
	require_Len(t, len(ru.addStreams), 1)
	require_Len(t, len(ru.updateConsumers), 1)

	// Process those updates.
	for _, sa = range ru.addStreams {
		mjs.processStreamAssignment(sa)
	}
	for _, cas := range ru.updateConsumers {
		for _, ca = range cas {
			mjs.processConsumerAssignment(ca)
		}
	}

	// Should not have created old Raft nodes during recovery.
	n1 := ml.lookupRaftNode(oldStreamGroup)
	n2 := ml.lookupRaftNode(oldConsumerGroup)
	require_True(t, n1 == nil)
	require_True(t, n2 == nil)
}

func TestJetStreamClusterMetaSnapshotConsumerDeleteConsistency(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "R3S", 3)
	defer c.shutdown()

	nc, js := jsClientConnect(t, c.randomServer())
	defer nc.Close()

	scfg := &nats.StreamConfig{Name: "TEST", Replicas: 1}
	_, err := js.AddStream(scfg)
	require_NoError(t, err)

	ccfg := &nats.ConsumerConfig{Name: "consumer", Replicas: 1}
	_, err = js.AddConsumer("TEST", ccfg)
	require_NoError(t, err)

	sl := c.streamLeader(globalAccountName, "TEST")
	mjs := sl.getJetStream()
	mjs.mu.Lock()
	ca := mjs.consumerAssignment(globalAccountName, "TEST", "consumer")
	ca.Created = time.Time{} // Simulate this consumer existed for a while already.
	deleteConsumer := encodeDeleteConsumerAssignment(ca)
	mjs.mu.Unlock()

	// Get the snapshot before removing the stream below so we can recover fresh.
	snap, _, _, err := mjs.metaSnapshot()
	require_NoError(t, err)
	require_NoError(t, js.DeleteStream("TEST"))
	nc.Close()

	ru := &recoveryUpdates{
		removeStreams:   make(map[string]*streamAssignment),
		removeConsumers: make(map[string]map[string]*consumerAssignment),
		addStreams:      make(map[string]*streamAssignment),
		updateStreams:   make(map[string]*streamAssignment),
		updateConsumers: make(map[string]map[string]*consumerAssignment),
	}

	// Simulate recovering:
	// - snapshot with a stream and consumer
	// - normal entry deleting the consumer
	// This should result in a consistent state.
	mjs.mu.Lock()
	mjs.metaRecovering = true
	mjs.mu.Unlock()
	_, _, err = mjs.applyMetaEntries([]*Entry{
		newEntry(EntrySnapshot, snap),
		newEntry(EntryNormal, deleteConsumer),
	}, ru)
	require_NoError(t, err)

	// Recovery should contain the stream create and the consumer delete.
	require_Len(t, len(ru.updateConsumers), 1)
	require_Len(t, len(ru.removeConsumers), 1)
	require_Len(t, len(ru.addStreams), 1)

	// Process those updates.
	for _, cas := range ru.updateConsumers {
		require_Len(t, len(cas), 0)
	}
	for _, cas := range ru.removeConsumers {
		for _, ca = range cas {
			mjs.processConsumerRemoval(ca)
		}
	}
	for _, sa := range ru.addStreams {
		mjs.processStreamAssignment(sa)
	}
	mjs.clearMetaRecovering()

	checkFor(t, 2*time.Second, 200*time.Millisecond, func() error {
		hs := sl.healthz(&HealthzOptions{})
		if hs.Error != _EMPTY_ {
			return errors.New(hs.Error) // Would previously error with "consumer not found".
		}
		return nil
	})
}

func TestJetStreamClusterMetaSnapshotDecodeError(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "R3S", 3)
	defer c.shutdown()

	js := c.leader().getJetStream()

	isRecovering, didSnap, err := js.applyMetaEntries(
		[]*Entry{newEntry(EntrySnapshot, []byte("not a valid snapshot"))}, nil)

	require_Error(t, err)
	require_False(t, isRecovering)
	require_False(t, didSnap)
}

func TestJetStreamClusterConsumerDontSendSnapshotOnLeaderChange(t *testing.T) {
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
		Replicas:  3,
		AckPolicy: nats.AckExplicitPolicy,
	})
	require_NoError(t, err)

	// Add a message and let the consumer ack it, this moves the consumer's RAFT applied up to 1.
	_, err = js.Publish("foo", nil)
	require_NoError(t, err)
	sub, err := js.PullSubscribe("foo", "CONSUMER")
	require_NoError(t, err)
	msgs, err := sub.Fetch(1)
	require_NoError(t, err)
	require_Len(t, len(msgs), 1)
	err = msgs[0].AckSync()
	require_NoError(t, err)

	// We don't need the client anymore.
	nc.Close()

	lookupConsumer := func(s *Server) *consumer {
		t.Helper()
		mset, err := s.lookupAccount(globalAccountName)
		require_NoError(t, err)
		acc, err := mset.lookupStream("TEST")
		require_NoError(t, err)
		o := acc.lookupConsumer("CONSUMER")
		require_NotNil(t, o)
		return o
	}

	// Grab current consumer leader before moving all into observer mode.
	cl := c.consumerLeader(globalAccountName, "TEST", "CONSUMER")
	for _, s := range c.servers {
		// Put all consumer's RAFT into observer mode, this will prevent all servers from trying to become leader.
		o := lookupConsumer(s)
		o.node.SetObserver(true)
		if s != cl {
			// For all followers, pause apply so they only store messages in WAL but not apply and possibly snapshot.
			err = o.node.PauseApply()
			require_NoError(t, err)
		}
	}

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

	updateAcksBuffer := func() []byte {
		var b [2*binary.MaxVarintLen64 + 1]byte
		b[0] = byte(updateAcksOp)
		n := 1
		n += binary.PutUvarint(b[n:], 100)
		n += binary.PutUvarint(b[n:], 100)
		return b[:n]
	}

	// Store an uncommitted entry into our WAL, which will be committed and applied later.
	co := lookupConsumer(cl)
	rn := co.node.(*raft)
	rn.Lock()
	entries := []*Entry{{EntryNormal, updateDeliveredBuffer()}, {EntryNormal, updateAcksBuffer()}}
	ae := encode(t, rn.buildAppendEntry(entries))
	err = rn.storeToWAL(ae)
	minPindex := rn.pindex
	rn.Unlock()
	require_NoError(t, err)

	// Simulate leader change, we do this so we can check what happens in the upper layer logic.
	rn.leadc <- leadChange{isLeader: true, term: rn.Term()}
	rn.SetObserver(false)

	// Since upper layer is async, we don't know whether it will or will not act on the leader change.
	// Wait for some time to check if it does.
	time.Sleep(2 * time.Second)
	rn.RLock()
	maxPindex := rn.pindex
	rn.RUnlock()

	r := c.randomNonConsumerLeader(globalAccountName, "TEST", "CONSUMER")
	ro := lookupConsumer(r)
	rn = ro.node.(*raft)

	checkFor(t, 5*time.Second, time.Second, func() error {
		rn.RLock()
		defer rn.RUnlock()
		if rn.pindex < maxPindex {
			return fmt.Errorf("rn.pindex too low, expected %d, got %d", maxPindex, rn.pindex)
		}
		return nil
	})

	// We should only have 'Normal' entries.
	// If we'd get a 'Snapshot' entry, that would mean it had incomplete state and would be reverting committed state.
	var state StreamState
	rn.wal.FastState(&state)
	for seq := minPindex; seq <= maxPindex; seq++ {
		ae, err = rn.loadEntry(seq)
		require_NoError(t, err)
		for _, entry := range ae.entries {
			require_Equal(t, entry.Type, EntryNormal)
		}
	}
}

func TestJetStreamClusterDontInstallSnapshotWhenStoppingStream(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "R3S", 3)
	defer c.shutdown()

	nc, js := jsClientConnect(t, c.randomServer())
	defer nc.Close()

	_, err := js.AddStream(&nats.StreamConfig{
		Name:      "TEST",
		Subjects:  []string{"foo"},
		Retention: nats.WorkQueuePolicy,
		Replicas:  3,
	})
	require_NoError(t, err)

	_, err = js.Publish("foo", nil)
	require_NoError(t, err)

	// Wait for all servers to have applied everything.
	var maxApplied uint64
	var tries int
	checkFor(t, 5*time.Second, 100*time.Millisecond, func() error {
		for _, s := range c.servers {
			acc, err := s.lookupAccount(globalAccountName)
			if err != nil {
				return err
			}
			mset, err := acc.lookupStream("TEST")
			if err != nil {
				return err
			}
			if _, _, applied := mset.node.Progress(); applied > maxApplied {
				maxApplied, tries = applied, 0
				return fmt.Errorf("applied upped to %d", maxApplied)
			} else if applied != maxApplied {
				return fmt.Errorf("applied doesn't match, expected %d, got %d", maxApplied, applied)
			}
		}
		tries++
		if tries < 3 {
			return fmt.Errorf("retrying for applied %d (try %d)", maxApplied, tries)
		}
		return nil
	})

	// Install a snapshot on a follower.
	s := c.randomNonStreamLeader(globalAccountName, "TEST")
	acc, err := s.lookupAccount(globalAccountName)
	require_NoError(t, err)
	mset, err := acc.lookupStream("TEST")
	require_NoError(t, err)
	err = mset.node.InstallSnapshot(mset.stateSnapshotLocked(), false)
	require_NoError(t, err)

	// Validate the snapshot reflects applied.
	validateStreamState := func(snap *snapshot) {
		t.Helper()
		require_Equal(t, snap.lastIndex, maxApplied)
		ss, err := DecodeStreamState(snap.data)
		require_NoError(t, err)
		require_Equal(t, ss.FirstSeq, 1)
		require_Equal(t, ss.LastSeq, 1)
	}
	snap, err := mset.node.(*raft).loadLastSnapshot()
	require_NoError(t, err)
	validateStreamState(snap)

	// Simulate a message being stored, but not calling Applied yet.
	err = mset.processJetStreamMsg("foo", _EMPTY_, nil, nil, 1, time.Now().UnixNano(), nil, false, true)
	require_NoError(t, err)

	// Simulate the stream being stopped before we're able to call Applied.
	// If we'd install a snapshot during this, which would be a race condition,
	// we'd store a snapshot with state that's ahead of applied.
	err = mset.stop(false, false)
	require_NoError(t, err)

	// Validate the snapshot is the same as before.
	snap, err = mset.node.(*raft).loadLastSnapshot()
	require_NoError(t, err)
	validateStreamState(snap)
}

func TestJetStreamClusterDontInstallSnapshotWhenStoppingConsumer(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "R3S", 3)
	defer c.shutdown()

	nc, js := jsClientConnect(t, c.randomServer())
	defer nc.Close()

	_, err := js.AddStream(&nats.StreamConfig{
		Name:      "TEST",
		Subjects:  []string{"foo"},
		Retention: nats.WorkQueuePolicy,
		Replicas:  3,
	})
	require_NoError(t, err)

	_, err = js.AddConsumer("TEST", &nats.ConsumerConfig{
		Durable:   "CONSUMER",
		Replicas:  3,
		AckPolicy: nats.AckExplicitPolicy,
	})
	require_NoError(t, err)

	// Add a message and let the consumer ack it, this moves the consumer's RAFT applied up.
	_, err = js.Publish("foo", nil)
	require_NoError(t, err)
	sub, err := js.PullSubscribe("foo", "CONSUMER")
	require_NoError(t, err)
	msgs, err := sub.Fetch(1)
	require_NoError(t, err)
	require_Len(t, len(msgs), 1)
	err = msgs[0].AckSync()
	require_NoError(t, err)

	// Wait for all servers to have applied everything.
	var maxApplied uint64
	var tries int
	checkFor(t, 5*time.Second, 100*time.Millisecond, func() error {
		for _, s := range c.servers {
			acc, err := s.lookupAccount(globalAccountName)
			if err != nil {
				return err
			}
			mset, err := acc.lookupStream("TEST")
			if err != nil {
				return err
			}
			o := mset.lookupConsumer("CONSUMER")
			if o == nil {
				return errors.New("consumer not found")
			}
			if _, _, applied := o.node.Progress(); applied > maxApplied {
				maxApplied, tries = applied, 0
				return fmt.Errorf("applied upped to %d", maxApplied)
			} else if applied != maxApplied {
				return fmt.Errorf("applied doesn't match, expected %d, got %d", maxApplied, applied)
			}
		}
		tries++
		if tries < 3 {
			return fmt.Errorf("retrying for applied %d (try %d)", maxApplied, tries)
		}
		return nil
	})

	// Install a snapshot on a follower.
	s := c.randomNonStreamLeader(globalAccountName, "TEST")
	acc, err := s.lookupAccount(globalAccountName)
	require_NoError(t, err)
	mset, err := acc.lookupStream("TEST")
	require_NoError(t, err)
	o := mset.lookupConsumer("CONSUMER")
	require_NotNil(t, o)
	snapBytes, err := o.store.EncodedState()
	require_NoError(t, err)
	err = o.node.InstallSnapshot(snapBytes, false)
	require_NoError(t, err)

	// Validate the snapshot reflects applied.
	validateConsumerState := func(snap *snapshot) {
		t.Helper()
		require_Equal(t, snap.lastIndex, maxApplied)
		state, err := decodeConsumerState(snap.data)
		require_NoError(t, err)
		require_Equal(t, state.Delivered.Consumer, 1)
		require_Equal(t, state.Delivered.Stream, 1)
	}
	snap, err := o.node.(*raft).loadLastSnapshot()
	require_NoError(t, err)
	validateConsumerState(snap)

	// Simulate a message being delivered, but not calling Applied yet.
	err = o.store.UpdateDelivered(2, 2, 1, time.Now().UnixNano())
	require_NoError(t, err)

	// Simulate the consumer being stopped before we're able to call Applied.
	// If we'd install a snapshot during this, which would be a race condition,
	// we'd store a snapshot with state that's ahead of applied.
	err = o.stop()
	require_NoError(t, err)

	// Validate the snapshot is the same as before.
	snap, err = o.node.(*raft).loadLastSnapshot()
	require_NoError(t, err)
	validateConsumerState(snap)
}

func TestJetStreamClusterStreamConsumerStateResetAfterRecreate(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "R3S", 3)
	defer c.shutdown()
	nc, js := jsClientConnect(t, c.randomServer())
	defer nc.Close()
	stream := "test:0"
	config := &nats.StreamConfig{
		Name:       stream,
		Subjects:   []string{"test.0.*"},
		Replicas:   3,
		Retention:  nats.WorkQueuePolicy,
		MaxMsgs:    100_000,
		Discard:    nats.DiscardNew,
		Duplicates: 5 * time.Second,
		Storage:    nats.MemoryStorage,
	}
	consumer := "A:0:0"
	subject := "test.0.0"
	var (
		duration        = 30 * time.Minute
		producerMsgs    = 200_000
		producerMsgSize = 1024
		payload         = []byte(strings.Repeat("A", producerMsgSize))
		wg              sync.WaitGroup
		n               atomic.Uint64
		canPublish      atomic.Bool
	)
	createStream := func(t *testing.T) {
		t.Helper()
		_, err := js.AddStream(config)
		require_NoError(t, err)
		consumer := &nats.ConsumerConfig{
			Durable:       consumer,
			Replicas:      3,
			MaxAckPending: 100_000,
			MaxWaiting:    100_000,
			FilterSubject: subject,
			AckPolicy:     nats.AckExplicitPolicy,
		}
		_, err = js.AddConsumer(stream, consumer)
		require_NoError(t, err)
	}
	deleteStream := func(t *testing.T) {
		err := js.DeleteStream(stream)
		require_NoError(t, err)
	}
	stopPublishing := func() {
		canPublish.Store(false)
	}
	resumePublishing := func() {
		canPublish.Store(true)
	}
	// Setup stream
	ctx, cancel := context.WithTimeout(context.Background(), duration)
	defer cancel()
	createStream(t)
	// Setup producer
	resumePublishing()
	wg.Add(1)
	go func() {
		defer wg.Done()
		nc, js := jsClientConnect(t, c.randomServer())
		defer nc.Close()
		for range time.NewTicker(1 * time.Millisecond).C {
			select {
			case <-ctx.Done():
				return
			default:
			}
			if !canPublish.Load() {
				continue
			}
			_, err := js.Publish("test.0.0", payload, nats.AckWait(200*time.Millisecond))
			if err == nil {
				if nn := n.Add(1); int(nn) >= producerMsgs {
					return
				}
			}
		}
	}()
	// Setup consumer
	acked := make(chan struct{}, 100)
	wg.Add(1)
	go func() {
		defer wg.Done()
		nc, js := jsClientConnect(t, c.randomServer())
		defer nc.Close()
	Attempts:
		for attempts := 0; attempts < 10; attempts++ {
			_, err := js.ConsumerInfo(stream, consumer)
			if err != nil {
				t.Logf("WRN: Failed creating pull subscriber: %v - %v - %v - %v",
					subject, stream, consumer, err)
				time.Sleep(200 * time.Millisecond)
				continue
			}
			break Attempts
		}
		sub, err := js.PullSubscribe(subject, "", nats.Bind(stream, consumer))
		if err != nil {
			t.Logf("WRN: Failed creating pull subscriber: %v - %v - %v - %v",
				subject, stream, consumer, err)
			return
		}
		require_NoError(t, err)
		for range time.NewTicker(100 * time.Millisecond).C {
			select {
			case <-ctx.Done():
				return
			default:
			}
			msgs, err := sub.Fetch(1, nats.MaxWait(200*time.Millisecond))
			if err != nil {
				continue
			}
			for _, msg := range msgs {
				time.AfterFunc(3*time.Second, func() {
					select {
					case <-ctx.Done():
						return
					default:
					}
					msg.Ack()
					acked <- struct{}{}
				})
			}
			msgs, err = sub.Fetch(10, nats.MaxWait(200*time.Millisecond))
			if err != nil {
				continue
			}
			for _, msg := range msgs {
				msg.Ack()
			}
		}
	}()
	// Let publish and consume to happen for a bit.
	time.Sleep(2 * time.Second)
	// Recreate the stream
	deleteStream(t)
	stopPublishing()
	createStream(t)
	for i := 0; i < 3; i++ {
		js.Publish("test.0.0", payload, nats.AckWait(200*time.Millisecond))
	}
	select {
	case <-time.After(5 * time.Second):
		t.Fatal("Timed out waiting for ack")
	case <-acked:
		time.Sleep(2 * time.Second)
	}
	sinfo, err := js.StreamInfo(stream)
	require_NoError(t, err)
	cinfo, err := js.ConsumerInfo(stream, consumer)
	require_NoError(t, err)
	cancel()
	if cinfo.Delivered.Stream > sinfo.State.LastSeq {
		t.Fatalf("Consumer Stream sequence is ahead of Stream LastSeq: consumer=%d, stream=%d", cinfo.Delivered.Stream, sinfo.State.LastSeq)
	}
	wg.Wait()
}

func TestJetStreamClusterStreamAckMsgR1SignalsRemovedMsg(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "R3S", 3)
	defer c.shutdown()

	nc, js := jsClientConnect(t, c.randomServer())
	defer nc.Close()

	_, err := js.AddStream(&nats.StreamConfig{
		Name:      "TEST",
		Subjects:  []string{"foo"},
		Retention: nats.WorkQueuePolicy,
		Replicas:  1,
	})
	require_NoError(t, err)

	_, err = js.AddConsumer("TEST", &nats.ConsumerConfig{
		Durable:   "CONSUMER",
		Replicas:  1,
		AckPolicy: nats.AckExplicitPolicy,
	})
	require_NoError(t, err)

	_, err = js.Publish("foo", nil)
	require_NoError(t, err)

	s := c.streamLeader(globalAccountName, "TEST")
	acc, err := s.lookupAccount(globalAccountName)
	require_NoError(t, err)
	mset, err := acc.lookupStream("TEST")
	require_NoError(t, err)
	o := mset.lookupConsumer("CONSUMER")
	require_NotNil(t, o)

	// Too high sequence, should register pre-ack and return true allowing for retries.
	require_True(t, mset.ackMsg(o, 100))

	var smv StoreMsg
	sm, err := mset.store.LoadMsg(1, &smv)
	require_NoError(t, err)
	require_Equal(t, sm.subj, "foo")

	// Should not remove if the message is still pending, even if we call ack.
	require_False(t, mset.ackMsg(o, 1))
	sm, err = mset.store.LoadMsg(1, &smv)
	require_NoError(t, err)
	require_Equal(t, sm.subj, "foo")

	// Now do a proper ack, should immediately remove the message since it's R1.
	o.mu.Lock()
	o.sseq, o.dseq = 2, 2
	o.asflr, o.adflr = 1, 1
	o.mu.Unlock()
	require_True(t, mset.ackMsg(o, 1))
	_, err = mset.store.LoadMsg(1, &smv)
	require_Error(t, err, ErrStoreMsgNotFound)
}

func TestJetStreamClusterStreamAckMsgR3SignalsRemovedMsg(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "R3S", 3)
	defer c.shutdown()

	nc, js := jsClientConnect(t, c.randomServer())
	defer nc.Close()

	_, err := js.AddStream(&nats.StreamConfig{
		Name:      "TEST",
		Subjects:  []string{"foo"},
		Retention: nats.WorkQueuePolicy,
		Replicas:  3,
	})
	require_NoError(t, err)

	_, err = js.AddConsumer("TEST", &nats.ConsumerConfig{
		Durable:   "CONSUMER",
		Replicas:  3,
		AckPolicy: nats.AckExplicitPolicy,
	})
	require_NoError(t, err)

	_, err = js.Publish("foo", nil)
	require_NoError(t, err)

	// Wait for all servers to know about the stream and consumer.
	checkFor(t, 2*time.Second, 100*time.Millisecond, func() error {
		for _, s := range c.servers {
			mset, err := s.globalAccount().lookupStream("TEST")
			if err != nil {
				return err
			}
			if mset.lookupConsumer("CONSUMER") == nil {
				return errors.New("consumer not found")
			}
		}
		return nil
	})

	// Also wait for the published message to be replicated.
	checkFor(t, 2*time.Second, 100*time.Millisecond, func() error {
		return checkState(t, c, globalAccountName, "TEST")
	})

	sl := c.streamLeader(globalAccountName, "TEST")
	sf := c.randomNonStreamLeader(globalAccountName, "TEST")

	msetL, err := sl.globalAccount().lookupStream("TEST")
	require_NoError(t, err)
	ol := msetL.lookupConsumer("CONSUMER")
	require_NotNil(t, ol)
	msetF, err := sf.globalAccount().lookupStream("TEST")
	require_NoError(t, err)
	of := msetF.lookupConsumer("CONSUMER")
	require_NotNil(t, of)

	// Too high sequence, should register pre-ack and return true allowing for retries.
	require_True(t, msetL.ackMsg(ol, 100))
	require_True(t, msetF.ackMsg(of, 100))

	// We're bypassing the normal ack flow, so must set these values ourselves.
	for _, s := range c.servers {
		mset, err := s.globalAccount().lookupStream("TEST")
		require_NoError(t, err)
		o := mset.lookupConsumer("CONSUMER")
		require_NotNil(t, o)

		if o.IsLeader() {
			o.mu.Lock()
			o.sseq, o.dseq = 2, 2
			o.asflr, o.adflr = 1, 1
			o.mu.Unlock()
		} else {
			require_NoError(t, o.store.Update(&ConsumerState{
				Delivered: SequencePair{1, 1},
				AckFloor:  SequencePair{1, 1},
			}))
		}
	}

	// Ack message on follower, should not remove message as that's proposed by the leader.
	// But should still signal message removal.
	require_True(t, msetF.ackMsg(of, 1))

	// Confirm all servers have the message.
	var smv StoreMsg
	for _, s := range c.servers {
		mset, err := s.globalAccount().lookupStream("TEST")
		require_NoError(t, err)
		sm, err := mset.store.LoadMsg(1, &smv)
		require_NoError(t, err)
		require_Equal(t, sm.subj, "foo")
	}

	// Now do a proper ack, should propose the message removal since it's R3.
	require_True(t, msetL.ackMsg(ol, 1))
	checkFor(t, 5*time.Second, 200*time.Millisecond, func() error {
		for _, s := range c.servers {
			mset, err := s.globalAccount().lookupStream("TEST")
			if err != nil {
				return err
			}
			_, err = mset.store.LoadMsg(1, &smv)
			if err != ErrStoreMsgNotFound {
				return fmt.Errorf("expected error, but got: %v", err)
			}
		}
		return nil
	})
}

func TestJetStreamClusterExpectedPerSubjectConsistency(t *testing.T) {
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

	s := c.streamLeader(globalAccountName, "TEST")
	acc, err := s.lookupAccount(globalAccountName)
	require_NoError(t, err)
	mset, err := acc.lookupStream("TEST")
	require_NoError(t, err)

	// Block updates when subject already in process.
	mset.clMu.Lock()
	mset.expectedPerSubjectSequence = map[uint64]string{0: "foo"}
	mset.expectedPerSubjectInProcess = map[string]struct{}{"foo": {}}
	mset.clMu.Unlock()
	_, err = js.Publish("foo", nil, nats.ExpectLastSequencePerSubject(0))
	require_Error(t, err, NewJSStreamWrongLastSequenceConstantError())

	// Block updates when subject already inflight without expected headers.
	mset.clMu.Lock()
	mset.expectedPerSubjectSequence = nil
	mset.expectedPerSubjectInProcess = nil
	mset.inflight = map[string]*inflightSubjectRunningTotal{"foo": {bytes: 33, ops: 1}}
	mset.clMu.Unlock()
	_, err = js.Publish("foo", nil, nats.ExpectLastSequencePerSubject(0))
	require_Error(t, err, NewJSStreamWrongLastSequenceConstantError())

	// Allow updates when ready and subject not already in process.
	mset.clMu.Lock()
	mset.expectedPerSubjectSequence = nil
	mset.expectedPerSubjectInProcess = nil
	mset.inflight = nil
	mset.clMu.Unlock()
	pa, err := js.Publish("foo", nil, nats.ExpectLastSequencePerSubject(0))
	require_NoError(t, err)
	require_Equal(t, pa.Sequence, 1)

	// Should be cleaned up after publish.
	mset.clMu.Lock()
	defer mset.clMu.Unlock()
	require_Len(t, len(mset.expectedPerSubjectSequence), 0)
	require_Len(t, len(mset.expectedPerSubjectInProcess), 0)
}

func TestJetStreamClusterMsgCounterRunningTotalConsistency(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "R3S", 3)
	defer c.shutdown()

	nc, js := jsClientConnect(t, c.randomServer())
	defer nc.Close()

	_, err := jsStreamCreate(t, nc, &StreamConfig{
		Name:            "TEST",
		Subjects:        []string{"foo"},
		Storage:         FileStorage,
		Retention:       LimitsPolicy,
		Replicas:        3,
		AllowMsgCounter: true,
	})
	require_NoError(t, err)

	s := c.streamLeader(globalAccountName, "TEST")
	acc, err := s.lookupAccount(globalAccountName)
	require_NoError(t, err)
	mset, err := acc.lookupStream("TEST")
	require_NoError(t, err)

	// Running total should be kept up-to-date.
	mset.clMu.Lock()
	mset.clusteredCounterTotal = map[string]*msgCounterRunningTotal{
		"foo": {total: big.NewInt(10), ops: 1},
	}
	mset.clMu.Unlock()
	m := nats.NewMsg("foo")
	m.Header.Set("Nats-Incr", "1")
	pubAck, err := js.PublishMsg(m)
	require_NoError(t, err)
	require_Equal(t, pubAck.Sequence, 1)

	rsm, err := js.GetLastMsg("TEST", "foo")
	require_NoError(t, err)
	require_Equal(t, rsm.Sequence, 1)
	var count CounterValue
	require_NoError(t, json.Unmarshal(rsm.Data, &count))
	require_Equal(t, count.Value, "11")

	// Confirm running total has properly been mutated.
	total, ops := _EMPTY_, uint64(0)
	mset.clMu.Lock()
	if l := len(mset.clusteredCounterTotal); l != 1 {
		mset.clMu.Unlock()
		require_Len(t, l, 1)
	}
	if counter, ok := mset.clusteredCounterTotal["foo"]; !ok {
		mset.clMu.Unlock()
		t.Fatal("counter not found")
	} else {
		total = counter.total.String()
		ops = counter.ops
	}
	mset.clMu.Unlock()
	require_Equal(t, total, "11")
	require_Equal(t, ops, 1)

	// Reset. Running totals should be removed once all inflight counter operations are applied.
	mset.clMu.Lock()
	mset.clusteredCounterTotal = nil
	mset.clMu.Unlock()
	pubAck, err = js.PublishMsg(m)
	require_NoError(t, err)
	require_Equal(t, pubAck.Sequence, 2)

	rsm, err = js.GetLastMsg("TEST", "foo")
	require_NoError(t, err)
	require_Equal(t, rsm.Sequence, 2)
	require_NoError(t, json.Unmarshal(rsm.Data, &count))
	require_Equal(t, count.Value, "12")

	// Should be cleaned up after publish.
	mset.clMu.Lock()
	defer mset.clMu.Unlock()
	require_Len(t, len(mset.clusteredCounterTotal), 0)
}

func TestJetStreamClusterConsistencyAfterLeaderChange(t *testing.T) {
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

	sl := c.streamLeader(globalAccountName, "TEST")
	acc, err := sl.lookupAccount(globalAccountName)
	require_NoError(t, err)
	mset, err := acc.lookupStream("TEST")
	require_NoError(t, err)
	n := mset.raftNode().(*raft)
	n.Lock()
	// Block snapshots from being made, preserving the full log so we can validate it later.
	n.progress = make(map[string]*ipQueue[uint64])
	n.progress["blockSnapshots"] = newIPQueue[uint64](n.s, "blockSnapshots")
	// Put into observer so the RAFT code doesn't switch to candidate on its own.
	n.observer = true
	n.Unlock()

	nc.Close()
	nc, js = jsClientConnect(t, sl)
	defer nc.Close()

	// Publish a message and confirm all servers are up-to-date.
	// This ensures the first message entry has lseq=0.
	pubAck, err := js.Publish("foo", nil)
	require_NoError(t, err)
	require_Equal(t, pubAck.Sequence, 1)
	checkFor(t, 2*time.Second, 500*time.Millisecond, func() error {
		return checkState(t, c, globalAccountName, "TEST")
	})

	// Shutdown all other servers so no changes get quorum.
	for _, s := range c.servers {
		if s != sl {
			s.Shutdown()
			s.WaitForShutdown()
		}
	}

	// Publish an initial set of messages to be stored in the leader's WAL, but without quorum.
	var ppindex uint64
	for n.State() == Leader && ppindex <= 10 {
		err = nc.Publish("foo", nil)
		require_NoError(t, err)

		n.RLock()
		ppindex = n.pindex
		n.RUnlock()
	}
	// Only continue if we were actually able to persist messages.
	require_LessThan(t, 10, ppindex)

	// Step down to follower, forcing a leader transition.
	n.stepdown(noLeader)
	n.RLock()
	ppindex = n.pindex
	n.RUnlock()

	// We don't know when the RAFT run loop transitions, just wait for some time.
	time.Sleep(time.Second)

	// Publish a second set of messages to be stored in the leader's WAL, but without quorum.
	var npindex uint64
	n.switchToLeader()
	for n.State() == Leader && npindex <= ppindex*2 {
		err = nc.Publish("foo", nil)
		require_NoError(t, err)

		n.RLock()
		npindex = n.pindex
		n.RUnlock()
	}

	n.RLock()
	var ss StreamState
	n.wal.FastState(&ss)
	n.RUnlock()

	// Go through all messages and confirm the last seq that's
	// proposed to the NRG layer is monotonically increasing.
	var clseq uint64
	for seq := ss.FirstSeq; seq <= ss.LastSeq; seq++ {
		ae, err := n.loadEntry(seq)
		require_NoError(t, err)
		for _, e := range ae.entries {
			_ = e
			if e.Type == EntryNormal && len(e.Data) > 0 && entryOp(e.Data[0]) == streamMsgOp {
				subject, _, _, _, lseq, _, _, err := decodeStreamMsg(e.Data[1:])
				require_NoError(t, err)
				require_Equal(t, subject, "foo")
				// Sequence must monotonically increase. If it wouldn't that would mean the new leader accepted
				// new messages before it was in the same state as the previous leader when it stepped down.
				// Or there are gaps, which would mean the JetStream layer would run into errLastSeqMismatch.
				require_Equal(t, lseq, clseq)
				clseq++
			} else if e.Type != EntryPeerState {
				t.Fatalf("Received unhandled entry type: %s\n", e.Type)
			}
		}
	}
	require_NotEqual(t, clseq, 0)

	// Remove observer flag so it can become leader on its own.
	n.Lock()
	n.observer = false
	n.Unlock()

	// Restart one other server, this should result in the previous leader
	// to become leader again, as it has the most up-to-date log.
	// If we'd bring all of them up it would be up to chance who'd become leader.
	for _, s := range c.servers {
		if s != sl {
			c.restartServer(s)
			break
		}
	}
	c.waitOnStreamLeader(globalAccountName, "TEST")

	// Confirm the sequence still monotonically increases.
	pubAck, err = js.Publish("foo", nil)
	require_NoError(t, err)
	require_Equal(t, pubAck.Sequence, clseq+1)

	for _, s := range c.servers {
		if !s.isRunning() {
			c.restartServer(s)
		}
	}
	c.waitOnAllCurrent()
	c.waitOnStreamLeader(globalAccountName, "TEST")

	// Confirm the sequence still monotonically increases.
	pubAck, err = js.Publish("foo", nil)
	require_NoError(t, err)
	require_Equal(t, pubAck.Sequence, clseq+2)
	checkFor(t, 2*time.Second, 500*time.Millisecond, func() error {
		return checkState(t, c, globalAccountName, "TEST")
	})
}

func TestJetStreamClusterMetaStepdownPreferred(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "R3S", 3)
	defer c.shutdown()

	nc, _ := jsClientConnect(t, c.randomServer(), nats.UserInfo("admin", "s3cr3t!"))
	defer nc.Close()

	// We know of the preferred server and will successfully hand over to it.
	t.Run("KnownPreferred", func(t *testing.T) {
		leader := c.leader()
		var preferred *Server
		for _, s := range c.servers {
			if s == leader {
				continue
			}
			preferred = s
			break
		}

		body, err := json.Marshal(JSApiLeaderStepdownRequest{
			Placement: &Placement{
				Preferred: preferred.Name(),
			},
		})
		require_NoError(t, err)

		resp, err := nc.Request(JSApiLeaderStepDown, body, time.Second)
		require_NoError(t, err)

		var apiresp JSApiLeaderStepDownResponse
		require_NoError(t, json.Unmarshal(resp.Data, &apiresp))
		require_True(t, apiresp.Success)
		require_Equal(t, apiresp.Error, nil)

		c.waitOnLeader()
		require_Equal(t, preferred, c.leader())
	})

	// We don't know of a server that matches that name so the stepdown fails.
	t.Run("UnknownPreferred", func(t *testing.T) {
		body, err := json.Marshal(JSApiLeaderStepdownRequest{
			Placement: &Placement{
				Preferred: "i_dont_exist",
			},
		})
		require_NoError(t, err)

		resp, err := nc.Request(JSApiLeaderStepDown, body, time.Second)
		require_NoError(t, err)

		var apiresp JSApiLeaderStepDownResponse
		require_NoError(t, json.Unmarshal(resp.Data, &apiresp))
		require_False(t, apiresp.Success)
		require_NotNil(t, apiresp.Error)
		require_Equal(t, ErrorIdentifier(apiresp.Error.ErrCode), JSClusterNoPeersErrF)
	})

	// The preferred server happens to already be the leader so the stepdown fails.
	t.Run("SamePreferred", func(t *testing.T) {
		body, err := json.Marshal(JSApiLeaderStepdownRequest{
			Placement: &Placement{
				Preferred: c.leader().Name(),
			},
		})
		require_NoError(t, err)

		resp, err := nc.Request(JSApiLeaderStepDown, body, time.Second)
		require_NoError(t, err)

		var apiresp ApiResponse
		require_NoError(t, json.Unmarshal(resp.Data, &apiresp))
		require_NotNil(t, apiresp.Error)
		require_Equal(t, ErrorIdentifier(apiresp.Error.ErrCode), JSClusterNoPeersErrF)
	})
}

func TestJetStreamClusterOnlyPublishAdvisoriesWhenInterest(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "R3S", 3)
	defer c.shutdown()

	subj := "$JS.ADVISORY.TEST"
	s1 := c.servers[0]
	s2 := c.servers[1]

	// On the first server, see if we think the advisory will be published.
	require_False(t, s1.publishAdvisory(s1.GlobalAccount(), subj, "test"))

	// On the second server, subscribe to the advisory subject.
	nc, _ := jsClientConnect(t, s2)
	defer nc.Close()

	_, err := nc.Subscribe(subj, func(_ *nats.Msg) {})
	require_NoError(t, err)

	// Wait for the interest to propagate to the first server.
	checkFor(t, time.Second, 25*time.Millisecond, func() error {
		if !s1.GlobalAccount().sl.HasInterest(subj) {
			return fmt.Errorf("expected interest in %q, not yet found", subj)
		}
		return nil
	})

	// On the first server, try and publish the advisory again. THis time
	// it should succeed.
	require_True(t, s1.publishAdvisory(s1.GlobalAccount(), subj, "test"))
}

func TestJetStreamClusterRoutedAPIRecoverPerformance(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "R3S", 3)
	defer c.shutdown()

	nc, _ := jsClientConnect(t, c.randomNonLeader())
	defer nc.Close()

	// We only run 16 JetStream API workers.
	mp := runtime.GOMAXPROCS(0)
	if mp > 16 {
		mp = 16
	}

	leader := c.leader()
	ljs := leader.js.Load()

	// Take the JS lock, which allows the JS API queue to build up.
	ljs.mu.Lock()
	defer ljs.mu.Unlock()

	count := JSDefaultRequestQueueLimit - 1
	ch := make(chan *nats.Msg, count)

	inbox := nc.NewRespInbox()
	_, err := nc.ChanSubscribe(inbox, ch)
	require_NoError(t, err)

	// To ensure a fair starting line, we need to submit as many tasks as
	// there are JS workers whilst holding the JS lock. This will ensure that
	// each JS API worker is properly wedged.
	msg := &nats.Msg{
		Subject: fmt.Sprintf(JSApiConsumerInfoT, "Doesnt", "Exist"),
		Reply:   "no_one_here",
	}
	for i := 0; i < mp; i++ {
		require_NoError(t, nc.PublishMsg(msg))
	}

	// Then we want to submit a fixed number of tasks, big enough to fill
	// the queue, so that we can measure them.
	msg = &nats.Msg{
		Subject: fmt.Sprintf(JSApiConsumerInfoT, "Doesnt", "Exist"),
		Reply:   inbox,
	}
	for i := 0; i < count; i++ {
		require_NoError(t, nc.PublishMsg(msg))
	}
	checkFor(t, 5*time.Second, 25*time.Millisecond, func() error {
		if queued := leader.jsAPIRoutedInfoReqs.len(); queued != count {
			return fmt.Errorf("expected %d queued requests, got %d", count, queued)
		}
		return nil
	})

	// Now we're going to release the lock and start timing. The workers
	// will now race to clear the queues and we'll wait to see how long
	// it takes for them all to respond.
	start := time.Now()
	ljs.mu.Unlock()
	for i := 0; i < count; i++ {
		<-ch
	}
	ljs.mu.Lock()
	t.Logf("Took %s to clear %d items", time.Since(start), count)
}

func TestJetStreamClusterMessageTTLWhenSourcing(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "R3S", 3)
	defer c.shutdown()

	nc, js := jsClientConnect(t, c.randomServer())
	defer nc.Close()

	jsStreamCreate(t, nc, &StreamConfig{
		Name:        "Origin",
		Storage:     FileStorage,
		Subjects:    []string{"test"},
		AllowMsgTTL: true,
		Replicas:    3,
	})

	jsStreamCreate(t, nc, &StreamConfig{
		Name:    "TTLEnabled",
		Storage: FileStorage,
		Sources: []*StreamSource{
			{Name: "Origin"},
		},
		AllowMsgTTL: true,
		Replicas:    3,
	})

	jsStreamCreate(t, nc, &StreamConfig{
		Name:    "TTLDisabled",
		Storage: FileStorage,
		Sources: []*StreamSource{
			{Name: "Origin"},
		},
		AllowMsgTTL: false,
		Replicas:    3,
	})

	hdr := nats.Header{}
	hdr.Add(JSMessageTTL, "1s")

	_, err := js.PublishMsg(&nats.Msg{
		Subject: "test",
		Header:  hdr,
	})
	require_NoError(t, err)

	for _, stream := range []string{"TTLEnabled", "TTLDisabled"} {
		t.Run(stream, func(t *testing.T) {
			sc, err := js.PullSubscribe("test", "consumer", nats.BindStream(stream))
			require_NoError(t, err)

			msgs, err := sc.Fetch(1)
			require_NoError(t, err)
			require_Len(t, len(msgs), 1)
			require_Equal(t, msgs[0].Header.Get(JSMessageTTL), "1s")

			time.Sleep(time.Second)

			si, err := js.StreamInfo(stream)
			require_NoError(t, err)
			if stream == "TTLDisabled" {
				require_Equal(t, si.State.Msgs, 1)
			} else {
				require_Equal(t, si.State.Msgs, 0)
			}
		})
	}
}

func TestJetStreamClusterMessageTTLWhenMirroring(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "R3S", 3)
	defer c.shutdown()

	nc, js := jsClientConnect(t, c.randomServer())
	defer nc.Close()

	jsStreamCreate(t, nc, &StreamConfig{
		Name:        "Origin",
		Storage:     FileStorage,
		Subjects:    []string{"test"},
		AllowMsgTTL: true,
		Replicas:    3,
	})

	jsStreamCreate(t, nc, &StreamConfig{
		Name:    "TTLEnabled",
		Storage: FileStorage,
		Mirror: &StreamSource{
			Name: "Origin",
		},
		AllowMsgTTL: true,
		Replicas:    3,
	})

	jsStreamCreate(t, nc, &StreamConfig{
		Name:    "TTLDisabled",
		Storage: FileStorage,
		Mirror: &StreamSource{
			Name: "Origin",
		},
		AllowMsgTTL: false,
		Replicas:    3,
	})

	hdr := nats.Header{}
	hdr.Add(JSMessageTTL, "1s")

	_, err := js.PublishMsg(&nats.Msg{
		Subject: "test",
		Header:  hdr,
	})
	require_NoError(t, err)

	for _, stream := range []string{"TTLEnabled", "TTLDisabled"} {
		t.Run(stream, func(t *testing.T) {
			sc, err := js.PullSubscribe("test", "consumer", nats.BindStream(stream))
			require_NoError(t, err)

			msgs, err := sc.Fetch(1)
			require_NoError(t, err)
			require_Len(t, len(msgs), 1)
			require_Equal(t, msgs[0].Header.Get(JSMessageTTL), "1s")

			time.Sleep(time.Second)

			si, err := js.StreamInfo(stream)
			require_NoError(t, err)
			if stream == "TTLDisabled" {
				require_Equal(t, si.State.Msgs, 1)
			} else {
				require_Equal(t, si.State.Msgs, 0)
			}
		})
	}
}

func TestJetStreamClusterMessageTTLDisabled(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "R3S", 3)
	defer c.shutdown()

	nc, js := jsClientConnect(t, c.randomServer())
	defer nc.Close()

	jsStreamCreate(t, nc, &StreamConfig{
		Name:     "TEST",
		Storage:  FileStorage,
		Subjects: []string{"test"},
		Replicas: 3,
	})

	msg := &nats.Msg{
		Subject: "test",
		Header:  nats.Header{},
	}

	msg.Header.Set(JSMessageTTL, "1s")
	_, err := js.PublishMsg(msg)
	require_Error(t, err)

	// In clustered mode we should have caught this before generating the
	// proposal, therefore the CLFS should not have been bumped by this.
	for _, s := range c.servers {
		stream, err := s.GlobalAccount().lookupStream("TEST")
		require_NoError(t, err)
		require_Equal(t, stream.getCLFS(), 0)
	}
}

func TestJetStreamClusterCreateStreamPerf(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "R3S", 3)
	defer c.shutdown()

	nc, js := jsClientConnect(t, c.randomServer())
	defer nc.Close()

	for i := 0; i < 10; i++ {
		// Check that creating a replicated asset doesn't take too long.
		start := time.Now()
		_, err := js.AddStream(&nats.StreamConfig{
			Name:      fmt.Sprintf("TEST-%d", i),
			Retention: nats.LimitsPolicy,
			Subjects:  []string{fmt.Sprintf("foo.%d", i)},
			Replicas:  3,
		})
		require_NoError(t, err)
		if elapsed := time.Since(start); elapsed > 150*time.Millisecond {
			t.Skipf("Took too long to create a R3 stream: %v", elapsed)
		}
	}
}

func TestJetStreamClusterTTLAndDedupe(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "R3S", 3)
	defer c.shutdown()

	nc, js := jsClientConnect(t, c.randomServer())
	defer nc.Close()

	cfg := &StreamConfig{
		Name:      "TEST",
		Retention: LimitsPolicy,
		Storage:   FileStorage,
		Subjects:  []string{"foo"},
		Replicas:  3,
	}
	_, err := jsStreamCreate(t, nc, cfg)
	require_NoError(t, err)

	m := nats.NewMsg("foo")
	m.Header.Add(JSMsgId, "msgId")
	m.Header.Add(JSMessageTTL, "10s")
	_, err = js.PublishMsg(m)
	require_Error(t, err, NewJSMessageTTLDisabledError())

	// Retrying should get TTL disabled still, not any other error.
	_, err = js.PublishMsg(m)
	require_Error(t, err, NewJSMessageTTLDisabledError())

	// Send without TTL should succeed.
	m.Header.Del(JSMessageTTL)
	_, err = js.PublishMsg(m)
	require_NoError(t, err)
}

func TestJetStreamClusterInvalidTTLAndDedupe(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "R3S", 3)
	defer c.shutdown()

	nc, js := jsClientConnect(t, c.randomServer())
	defer nc.Close()

	cfg := &StreamConfig{
		Name:        "TEST",
		Retention:   LimitsPolicy,
		Storage:     FileStorage,
		Subjects:    []string{"foo"},
		Replicas:    3,
		AllowMsgTTL: true,
	}
	_, err := jsStreamCreate(t, nc, cfg)
	require_NoError(t, err)

	m := nats.NewMsg("foo")
	m.Header.Add(JSMsgId, "msgId")
	m.Header.Add(JSMessageTTL, "invalid!")
	_, err = js.PublishMsg(m)
	require_Error(t, err, NewJSMessageTTLInvalidError())

	// Retry with a negative TTL.
	m.Header.Set(JSMessageTTL, "-10s")
	_, err = js.PublishMsg(m)
	require_Error(t, err, NewJSMessageTTLInvalidError())

	// Retrying with valid TTL should be successful, and not be marked as duplicate.
	m.Header.Set(JSMessageTTL, "10s")
	_, err = js.PublishMsg(m)
	require_NoError(t, err)
}

func TestJetStreamClusterServerPeerRemovePeersDrift(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "R4S", 4)
	defer c.shutdown()

	nc, js := jsClientConnect(t, c.randomServer())
	defer nc.Close()

	_, err := js.AddStream(&nats.StreamConfig{
		Name:      "TEST",
		Retention: nats.LimitsPolicy,
		Subjects:  []string{"foo"},
		Replicas:  3,
	})
	require_NoError(t, err)

	_, err = js.AddConsumer("TEST", &nats.ConsumerConfig{
		Durable:  "CONSUMER",
		Replicas: 3,
	})
	require_NoError(t, err)

	var acc *Account
	var mset *stream

	// Wait for 3 of the 4 servers to have created the stream.
	checkFor(t, 2*time.Second, 100*time.Millisecond, func() error {
		count := 0
		for _, s := range c.servers {
			acc, err = s.lookupAccount(globalAccountName)
			if err != nil {
				return err
			}
			mset, err = acc.lookupStream("TEST")
			if err != nil {
				continue
			}
			o := mset.lookupConsumer("CONSUMER")
			if o == nil {
				continue
			}
			count++
		}
		if count != 3 {
			return fmt.Errorf("expected 3 streams/consumers, got: %d", count)
		}
		return nil
	})

	sl := c.streamLeader(globalAccountName, "TEST")

	// Get a random server that:
	// - is not stream leader
	// - is not meta leader (peer-removing the meta leader has other issues)
	// - already hosts the stream so a peer-remove results in changing the stream peer set
	var rs *Server
	for _, s := range c.servers {
		acc, err = s.lookupAccount(globalAccountName)
		require_NoError(t, err)
		_, err = acc.lookupStream("TEST")
		if s == sl || s.isMetaLeader.Load() || err != nil {
			continue
		}
		rs = s
		break
	}
	if rs == nil {
		t.Fatal("No server found that's not either stream or meta leader.")
	}
	rs.Shutdown()

	// Peer-remove the selected server so the stream moves to the remaining empty server.
	sc, err := nats.Connect(sl.ClientURL(), nats.UserInfo("admin", "s3cr3t!"))
	require_NoError(t, err)
	req := &JSApiMetaServerRemoveRequest{Server: rs.Name()}
	jsreq, err := json.Marshal(req)
	require_NoError(t, err)
	_, err = sc.Request(JSApiRemoveServer, jsreq, time.Second)
	require_NoError(t, err)

	// Eventually there should again be a R3 stream and everyone should agree on the peers.
	checkFor(t, 2*time.Second, 100*time.Millisecond, func() error {
		count := 0
		var streamPeers []string
		var consumerPeers []string
		for _, s := range c.servers {
			if s == rs {
				continue
			}
			acc, err = s.lookupAccount(globalAccountName)
			if err != nil {
				return err
			}
			mset, err = acc.lookupStream("TEST")
			if err != nil {
				return err
			}
			o := mset.lookupConsumer("CONSUMER")
			if o == nil {
				return fmt.Errorf("consumer not found on %s", s.Name())
			}
			mrn := mset.raftNode().(*raft)
			mrn.RLock()
			streamPeerNames := mrn.peerNames()
			mrn.RUnlock()

			orn := o.raftNode().(*raft)
			orn.RLock()
			consumerPeerNames := orn.peerNames()
			orn.RUnlock()

			slices.Sort(streamPeerNames)
			slices.Sort(consumerPeerNames)
			if count == 0 {
				streamPeers = streamPeerNames
				consumerPeers = consumerPeerNames
			} else if !slices.Equal(streamPeers, streamPeerNames) {
				rsid := rs.NodeName()
				containsOld := slices.Contains(streamPeers, rsid) || slices.Contains(streamPeerNames, rsid)
				return fmt.Errorf("no equal stream peers, expected: %v, got: %v, contains old peer (%s): %v", streamPeers, streamPeerNames, rsid, containsOld)
			} else if !slices.Equal(consumerPeers, consumerPeerNames) {
				rsid := rs.NodeName()
				containsOld := slices.Contains(consumerPeers, rsid) || slices.Contains(consumerPeerNames, rsid)
				return fmt.Errorf("no equal consumer peers, expected: %v, got: %v, contains old peer (%s): %v", consumerPeers, consumerPeerNames, rsid, containsOld)
			}
			count++
		}
		if count != 3 {
			return fmt.Errorf("expected 3 servers hosting stream/consumer, got: %d", count)
		}
		return nil
	})
}

func TestJetStreamStreamTagPlacement(t *testing.T) {
	c := createJetStreamClusterWithTemplateAndModHook(t, jsClusterTempl, "R3S", 3,
		func(serverName, clusterName, storeDir, conf string) string {
			return fmt.Sprintf("%s\nserver_tags: [%s]", conf, serverName)
		})
	defer c.shutdown()

	nc, js := jsClientConnect(t, c.randomServer())
	defer nc.Close()

	for i, c := range []nats.StreamConfig{
		{Replicas: 1, Placement: &nats.Placement{Tags: []string{"!S-1", "!S-2", "!S-3"}}}, // exclude all servers.
		{Replicas: 2, Placement: &nats.Placement{Tags: []string{"!S-1", "!S-2"}}},         // exclude two servers.
		{Replicas: 3, Placement: &nats.Placement{Tags: []string{"!S-2"}}},                 // not enough server.
	} {
		c.Name = fmt.Sprintf("TEST%d", i)
		c.Subjects = []string{c.Name}
		_, err := js.AddStream(&c)
		require_Error(t, err)
		require_Contains(t, err.Error(), "no suitable peers for placement", "tags excluded", "S-2")
	}

	// Test adding excluded tags to an existing stream.
	cfg := &nats.StreamConfig{Name: "TEST", Subjects: []string{"foo"}, Replicas: 3}
	_, err := js.AddStream(cfg)
	require_NoError(t, err)

	cfg.Placement = &nats.Placement{Tags: []string{"!S-1"}}
	_, err = js.UpdateStream(cfg)
	require_Error(t, err)
	require_Contains(t, err.Error(), "no suitable peers for placement", "tags excluded", "S-1")

	// Test changing replicas to 2 and then add the excluded tag.
	cfg.Replicas = 2
	cfg.Placement = nil
	_, err = js.UpdateStream(cfg)
	require_NoError(t, err)

	var leaderName string
	// Wait until we have two replicas
	checkFor(t, 6*time.Second, 1*time.Second, func() error {
		s, err := js.StreamInfo("TEST")
		if err != nil {
			return err
		}

		if s.Cluster.Leader == "" {
			return fmt.Errorf("no leader yet")
		}

		if len(s.Cluster.Replicas) != 1 {
			return fmt.Errorf("expected 1 replica, got %d", len(s.Cluster.Replicas))
		}
		leaderName = s.Cluster.Leader
		return nil
	})

	// Now that we only have two servers, we can exclude one.
	cfg.Replicas = 2
	cfg.Placement = &nats.Placement{Tags: []string{"!" + leaderName}}
	_, err = js.UpdateStream(cfg)
	require_NoError(t, err)

	// Check that the stream grab the correct servers.
	// This can take a bit as we need to first add the new replica and later remove the old one.
	checkFor(t, 6*time.Second, 1*time.Second, func() error {
		s, err := js.StreamInfo("TEST")
		if err != nil {
			return err
		}

		if len(s.Cluster.Replicas) != 1 {
			return fmt.Errorf("expected 1 replica, got %d", len(s.Cluster.Replicas))
		}

		if s.Cluster.Leader == leaderName {
			return fmt.Errorf("expected leader to be different than %q", leaderName)
		}

		if s.Cluster.Replicas[0].Name == leaderName {
			return fmt.Errorf("expected replica to be different than %q", leaderName)
		}
		return nil
	})
}

func TestJetStreamClusterObserverNotElectedMetaLeader(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "R3S", 3)
	defer c.shutdown()

	c.waitOnLeader()

	getMeta := func(s *Server) *raft {
		if js := s.getJetStream(); js != nil {
			if mg := js.getMetaGroup(); mg != nil {
				return mg.(*raft)
			}
		}
		return nil
	}

	setToObserverAndStepDown := func(s *Server) {
		if meta := getMeta(s); meta != nil {
			meta.setObserver(true, extExtended)
			meta.StepDown()
		}
	}

	var wg sync.WaitGroup

	for range 10 {
		// Pick what will be the new leader since we are going to switch
		// the 2 other servers to observer mode and make them step down.
		newLeader := c.randomNonLeader()
		leader := c.leader()

		var other *Server
		for _, s := range c.servers {
			if s != newLeader && s != leader {
				other = s
				break
			}
		}

		wg.Add(1)
		go func() {
			defer wg.Done()
			// Add some random delay before changing state and stepping down.
			time.Sleep(time.Duration(rand.IntN(25)) * time.Millisecond)
			setToObserverAndStepDown(other)
		}()
		setToObserverAndStepDown(leader)
		wg.Wait()

		// Wait for the newLeader to really be elected.
		checkFor(t, 10*time.Second, 50*time.Millisecond, func() error {
			if !newLeader.JetStreamIsLeader() {
				return fmt.Errorf("Server %q is still not leader", newLeader)
			}
			return nil
		})

		// Change the observer back to false.
		for _, s := range []*Server{leader, other} {
			if meta := getMeta(s); meta != nil {
				meta.SetObserver(false)
			}
		}
	}
}

func TestJetStreamClusterParallelCreateRaftGroup(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "R3S", 3)
	defer c.shutdown()

	nc, js := jsClientConnect(t, c.randomServer())
	defer nc.Close()

	_, err := js.AddStream(&nats.StreamConfig{
		Name:     "TEST",
		Replicas: 3,
	})
	require_NoError(t, err)
	checkFor(t, time.Second, 100*time.Millisecond, func() error {
		return checkState(t, c, globalAccountName, "TEST")
	})

	ml := c.leader()
	sjs := ml.getJetStream()
	sa := sjs.streamAssignment(globalAccountName, "TEST")
	require_NotNil(t, sa)
	acc, err := ml.lookupAccount(globalAccountName)
	require_NoError(t, err)
	mset, err := acc.lookupStream("TEST")
	require_NoError(t, err)

	rg, storage := sa.Group, sa.Group.Storage
	rn := mset.raftNode().(*raft)
	// Do first half of Stop.
	require_NotEqual(t, rn.state.Swap(int32(Closed)), int32(Closed))

	var wg sync.WaitGroup
	var finish sync.WaitGroup
	wg.Add(2)
	finish.Add(2)

	var mu sync.Mutex
	var nodes []RaftNode

	// Call createRaftGroup in parallel.
	for i := 0; i < 2; i++ {
		go func() {
			wg.Done()
			defer finish.Done()
			if n, rerr := sjs.createRaftGroup(acc.GetName(), rg, false, storage, pprofLabels{}); rerr == nil {
				mu.Lock()
				nodes = append(nodes, n)
				mu.Unlock()
			}
		}()
	}

	// Wait for both goroutines, and allow some time for both to have entered createRaftGroup.
	wg.Wait()
	time.Sleep(100 * time.Millisecond)

	// Do second half of Stop while goroutines are in createRaftGroup.
	rn.leaderState.Store(false)
	rn.leaderSince.Store(nil)
	close(rn.quit)

	// Wait for node and goroutines to stop.
	rn.WaitForStop()
	finish.Wait()

	// Should only create one new node instance.
	require_Len(t, len(nodes), 2)
	require_Equal(t, nodes[0], nodes[1])
}

func TestJetStreamClusterParallelCreateRaftGroupHighConcurrency(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "R3S", 3)
	defer c.shutdown()

	nc, js := jsClientConnect(t, c.randomServer())
	defer nc.Close()

	_, err := js.AddStream(&nats.StreamConfig{
		Name:     "TEST",
		Replicas: 3,
	})
	require_NoError(t, err)
	checkFor(t, time.Second, 100*time.Millisecond, func() error {
		return checkState(t, c, globalAccountName, "TEST")
	})

	ml := c.leader()
	sjs := ml.getJetStream()
	sa := sjs.streamAssignment(globalAccountName, "TEST")
	require_NotNil(t, sa)
	acc, err := ml.lookupAccount(globalAccountName)
	require_NoError(t, err)
	mset, err := acc.lookupStream("TEST")
	require_NoError(t, err)

	rg, storage := sa.Group, sa.Group.Storage

	// Fully stop the existing node so it unregisters from s.raftNodes;
	// concurrent createRaftGroup callers must therefore reach the
	// "build a new node" path rather than the existing-node short-circuit.
	rn := mset.raftNode()
	rn.Stop()
	rn.WaitForStop()

	sjs.mu.Lock()
	rg.node = nil
	sjs.mu.Unlock()

	const N = 25
	var (
		ready sync.WaitGroup
		start = make(chan struct{})
		done  sync.WaitGroup
		mu    sync.Mutex
		nodes []RaftNode
		errs  []error
	)

	// Without the creatingRaftGroups gate, parallel callers each build
	// their own RaftNode; only the last registered stays in s.raftNodes
	// and the rest leak with goroutines + file handles, so c.shutdown()
	// hangs. Stop every distinct node we got back so the test can exit
	// cleanly even when the assertions below fail.
	defer func() {
		mu.Lock()
		defer mu.Unlock()
		seen := make(map[RaftNode]struct{}, len(nodes))
		for _, n := range nodes {
			if n == nil {
				continue
			}
			if _, ok := seen[n]; ok {
				continue
			}
			seen[n] = struct{}{}
			n.Stop()
		}
	}()

	ready.Add(N)
	done.Add(N)
	for i := 0; i < N; i++ {
		go func() {
			defer done.Done()
			ready.Done()
			<-start
			n, rerr := sjs.createRaftGroup(acc.GetName(), rg, false, storage, pprofLabels{})
			mu.Lock()
			nodes = append(nodes, n)
			errs = append(errs, rerr)
			mu.Unlock()
		}()
	}
	ready.Wait()
	close(start)
	done.Wait()

	// Every caller must have received the same RaftNode instance —
	// without the creatingRaftGroups gate, parallel callers each build
	// their own node and clobber each other in s.raftNodes, returning
	// distinct pointers.
	require_Len(t, len(nodes), N)
	for i := range N {
		require_NoError(t, errs[i])
		require_NotNil(t, nodes[i])
		require_Equal(t, nodes[0], nodes[i])
	}

	// The in-flight sentinel must be cleaned up once creation completed.
	sjs.mu.Lock()
	_, sentinelLeft := sjs.cluster.creatingRaftGroups[rg.Name]
	sjs.mu.Unlock()
	require_False(t, sentinelLeft)
}

func TestJetStreamClusterParallelCreateMaxHAAssetsLimit(t *testing.T) {
	const maxAssets = 2
	tmpl := strings.Replace(jsClusterTempl, "store_dir:", fmt.Sprintf("limits: {max_ha_assets: %d}, store_dir:", maxAssets), 1)
	c := createJetStreamClusterWithTemplateAndModHook(t, tmpl, "R3S", 3, nil)
	defer c.shutdown()

	nc, js := jsClientConnect(t, c.randomServer())
	defer nc.Close()

	// Concurrent creates must account for inflight assignments, not only applied ones.
	const N = 8
	var (
		wg      sync.WaitGroup
		start   = make(chan struct{})
		created atomic.Int32
	)
	for i := range N {
		wg.Go(func() {
			<-start
			if _, err := js.AddStream(&nats.StreamConfig{Name: fmt.Sprintf("S%d", i), Replicas: 3}); err == nil {
				created.Add(1)
			}
		})
	}
	close(start)
	wg.Wait()
	require_Equal(t, created.Load(), maxAssets)
}

func TestJetStreamClusterMaxHAAssetsAccounting(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "R3S", 3)
	defer c.shutdown()

	nc, js := jsClientConnect(t, c.randomServer())
	defer nc.Close()

	peers := func(s *Server) map[string]peerAssets {
		sjs := s.getJetStream()
		sjs.mu.RLock()
		defer sjs.mu.RUnlock()
		return maps.Clone(sjs.cluster.peerAssets)
	}
	requireCounts := func(streams, ha int) {
		t.Helper()
		ml := c.leader()
		pa := peers(ml)
		for _, s := range c.servers {
			require_Equal(t, pa[s.NodeName()], peerAssets{streams, ha})
		}
		// Only kept on the meta leader.
		for _, s := range c.servers {
			if s != ml {
				require_Len(t, len(peers(s)), 0)
			}
		}
	}

	_, err := js.AddStream(&nats.StreamConfig{Name: "TEST", Replicas: 3})
	require_NoError(t, err)
	requireCounts(1, 1)
	// Inherits the stream's replicas.
	_, err = js.AddConsumer("TEST", &nats.ConsumerConfig{Durable: "C1", AckPolicy: nats.AckExplicitPolicy})
	require_NoError(t, err)
	requireCounts(1, 2)
	// R1 consumers are not HA.
	_, err = js.AddConsumer("TEST", &nats.ConsumerConfig{Durable: "C2", AckPolicy: nats.AckExplicitPolicy, Replicas: 1})
	require_NoError(t, err)
	requireCounts(1, 2)

	// Rebuilt on the new meta leader.
	ml := c.leader()
	want := peers(ml)
	require_NoError(t, ml.getJetStream().getMetaGroup().StepDown())
	// The stepdown is async, wait for another server to take over.
	var ml2 *Server
	checkFor(t, 5*time.Second, 50*time.Millisecond, func() error {
		if ml2 = c.leader(); ml2 == nil || ml2 == ml || ml.JetStreamIsLeader() {
			return errors.New("waiting on new meta leader")
		}
		return nil
	})
	got := peers(ml2)
	require_True(t, maps.Equal(got, want))
	requireCounts(1, 2)

	// Scaling the stream down also frees up its inherited consumer.
	_, err = js.UpdateStream(&nats.StreamConfig{Name: "TEST", Replicas: 1})
	require_NoError(t, err)
	checkFor(t, 2*time.Second, 200*time.Millisecond, func() error {
		pa := peers(c.leader())
		if len(pa) != 1 {
			return fmt.Errorf("expected 1 stream peer, got %v", pa)
		}
		for _, v := range pa {
			if v != (peerAssets{streams: 1}) {
				return fmt.Errorf("expected no HA assets, got %v", pa)
			}
		}
		return nil
	})

	// Deleting the stream implicitly deletes its consumers.
	_, err = js.UpdateStream(&nats.StreamConfig{Name: "TEST", Replicas: 3})
	require_NoError(t, err)
	require_NoError(t, js.DeleteStream("TEST"))
	requireCounts(0, 0)
}

func TestJetStreamClusterMaxHAAssetsTracking(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "R3S", 3)
	defer c.shutdown()

	nc, js := jsClientConnect(t, c.randomServer())
	defer nc.Close()

	_, err := js.AddStream(&nats.StreamConfig{Name: "TEST", Replicas: 3})
	require_NoError(t, err)
	for _, name := range []string{"C", "C2"} {
		_, err = js.AddConsumer("TEST", &nats.ConsumerConfig{Durable: name, AckPolicy: nats.AckExplicitPolicy})
		require_NoError(t, err)
	}

	sjs := c.leader().getJetStream()
	sjs.mu.Lock()
	defer sjs.mu.Unlock()
	cc := sjs.cluster
	sa := sjs.streamAssignment(globalAccountName, "TEST")
	require_NotNil(t, sa)
	ca := sa.consumers["C"]
	require_NotNil(t, ca)
	requireCounts := func(peers []string, want peerAssets) {
		t.Helper()
		for _, peer := range peers {
			require_Equal(t, cc.peerAssets[peer], want)
		}
	}
	origin := sa.Group.Peers
	requireCounts(origin, peerAssets{streams: 1, ha: 3})

	// Moving the stream reserves its destination and frees up its source as soon as it's proposed.
	// The consumers stay counted on their own peers, until they're remapped onto the stream's peers.
	dest := []string{"A", "B", "C"}
	nsa := sa.copyGroup()
	nsa.Group = sa.Group.withDesired(&raftGroup{Name: sa.Group.Name, Peers: dest, Storage: sa.Group.Storage, Cluster: sa.Group.Cluster})
	nsa.Group.Desired.Move = true
	cc.trackInflightStreamProposal(globalAccountName, nsa, false)
	requireCounts(origin, peerAssets{ha: 2})
	requireCounts(dest, peerAssets{streams: 1, ha: 1})
	// Moving it would also bring along its consumers.
	require_Equal(t, cc.streamHACost(globalAccountName, sa.Config), 3)

	// Reverting restores the counts.
	cc.trackInflightStreamProposal(globalAccountName, sa, false)
	requireCounts(origin, peerAssets{streams: 1, ha: 3})
	requireCounts(dest, peerAssets{})

	// An inflight consumer update, before deleting the stream.
	cc.trackInflightConsumerProposal(globalAccountName, "TEST", ca.copyGroup(), false)
	requireCounts(origin, peerAssets{streams: 1, ha: 3})

	// Deleting the stream also drops its consumers, including inflight ones.
	cc.trackInflightStreamProposal(globalAccountName, sa, true)
	requireCounts(origin, peerAssets{})
	for _, name := range []string{"C", "C2"} {
		require_True(t, sjs.consumerAssignmentOrInflight(globalAccountName, "TEST", name) == nil)
	}
	require_Equal(t, cc.streamHACost(globalAccountName, sa.Config), 1)

	// Recreating it while the delete is inflight doesn't bring back the applied or inflight consumers.
	cc.trackInflightStreamProposal(globalAccountName, sa.copyGroup(), false)
	requireCounts(origin, peerAssets{streams: 1, ha: 1})
	require_Equal(t, cc.streamHACost(globalAccountName, sa.Config), 1)

	// A consumer with the same name is a new one, instead of replacing the stale applied one.
	cc.trackInflightConsumerProposal(globalAccountName, "TEST", ca.copyGroup(), false)
	requireCounts(origin, peerAssets{streams: 1, ha: 2})

	// A stream created and deleted inflight, its consumer applying in between is still deleted.
	cfg := *sa.Config
	cfg.Name = "NEW"
	csa := sa.copyGroup()
	csa.Config = &cfg
	cca := ca.copyGroup()
	cca.Stream = "NEW"
	cc.trackInflightStreamProposal(globalAccountName, csa, false)
	cc.trackInflightConsumerProposal(globalAccountName, "NEW", cca, false)
	cc.trackInflightStreamProposal(globalAccountName, csa, true)
	cc.streams[globalAccountName]["NEW"] = csa
	cc.removeInflightStreamProposal(globalAccountName, "NEW")
	csa.consumers = map[string]*consumerAssignment{cca.Name: cca}
	cc.removeInflightConsumerProposal(globalAccountName, "NEW", cca.Name)
	require_True(t, sjs.consumerAssignmentOrInflight(globalAccountName, "NEW", cca.Name) == nil)

	// Also when recreating it, until the delete is applied.
	cc.trackInflightStreamProposal(globalAccountName, csa.copyGroup(), false)
	require_True(t, sjs.consumerAssignmentOrInflight(globalAccountName, "NEW", cca.Name) == nil)
	delete(cc.streams[globalAccountName], "NEW")
	cc.removeInflightStreamProposal(globalAccountName, "NEW")
	require_True(t, sjs.consumerAssignmentOrInflight(globalAccountName, "NEW", cca.Name) == nil)
}

func TestJetStreamClusterMaxHAAssetsRetentionChange(t *testing.T) {
	tmpl := strings.Replace(jsClusterTempl, "store_dir:", "limits: {max_ha_assets: 2}, store_dir:", 1)
	c := createJetStreamClusterWithTemplateAndModHook(t, tmpl, "R3S", 3, nil)
	defer c.shutdown()

	nc, js := jsClientConnect(t, c.randomServer())
	defer nc.Close()

	// Every server holds 1 HA asset, R1 consumers are not HA on a limits stream.
	cfg := &nats.StreamConfig{Name: "TEST", Replicas: 3}
	_, err := js.AddStream(cfg)
	require_NoError(t, err)
	for _, name := range []string{"C1", "C2"} {
		_, err = js.AddConsumer("TEST", &nats.ConsumerConfig{Durable: name, AckPolicy: nats.AckExplicitPolicy, Replicas: 1})
		require_NoError(t, err)
	}

	// Interest streams remap their consumers onto the stream's peers, both R1 consumers would become HA.
	cfg.Retention = nats.InterestPolicy
	_, err = js.UpdateStream(cfg)
	require_Error(t, err, NewJSInsufficientResourcesError())

	// Only one consumer becomes HA, which fits.
	require_NoError(t, js.DeleteConsumer("TEST", "C2"))
	_, err = js.UpdateStream(cfg)
	require_NoError(t, err)
	checkFor(t, 5*time.Second, 200*time.Millisecond, func() error {
		sjs := c.leader().getJetStream()
		sjs.mu.RLock()
		defer sjs.mu.RUnlock()
		for _, s := range c.servers {
			if pa := sjs.cluster.peerAssets[s.NodeName()]; pa != (peerAssets{streams: 1, ha: 2}) {
				return fmt.Errorf("expected 2 HA assets for %s, got %v", s.NodeName(), pa)
			}
		}
		return nil
	})
}

func TestJetStreamClusterMaxHAAssetsRetentionChangeReserved(t *testing.T) {
	tmpl := strings.Replace(jsClusterTempl, "store_dir:", "limits: {max_ha_assets: 2}, store_dir:", 1)
	c := createJetStreamClusterWithTemplateAndModHook(t, tmpl, "R3S", 3, nil)
	defer c.shutdown()

	nc, js := jsClientConnect(t, c.randomServer())
	defer nc.Close()

	_, err := js.AddStream(&nats.StreamConfig{Name: "TEST", Replicas: 3})
	require_NoError(t, err)
	_, err = js.AddConsumer("TEST", &nats.ConsumerConfig{Durable: "C", AckPolicy: nats.AckExplicitPolicy, Replicas: 1})
	require_NoError(t, err)

	ml := c.leader()
	sjs := ml.getJetStream()
	sjs.mu.Lock()
	cc := sjs.cluster
	sa := sjs.streamAssignment(globalAccountName, "TEST")
	require_NotNil(t, sa)
	ca := sa.consumers["C"]
	require_NotNil(t, ca)
	require_Len(t, len(ca.Group.Peers), 1)
	requireCounts := func(want peerAssets) {
		t.Helper()
		for _, peer := range sa.Group.Peers {
			require_Equal(t, cc.peerAssets[peer], want)
		}
	}
	requireCounts(peerAssets{streams: 1, ha: 1})

	// The retention change reserves the consumer on all the stream's peers as soon as it's proposed,
	// before the consumer is remapped. The stream's peers are now at the limit.
	cfg := *sa.Config
	cfg.Retention = InterestPolicy
	nsa := sa.copyGroup()
	nsa.Config = &cfg
	nsa.Group = nsa.Group.withRetentionChange(sa, cfg.Retention)
	cc.trackInflightStreamProposal(globalAccountName, nsa, false)
	requireCounts(peerAssets{streams: 1, ha: 2})
	sjs.mu.Unlock()

	// Scaling up the consumer to the stream's replicas adds nothing, since it's already counted.
	creq := &CreateConsumerRequest{Stream: "TEST", Config: ConsumerConfig{Durable: "C", AckPolicy: AckExplicit, Replicas: 3}, Action: ActionUpdate}
	msg, err := json.Marshal(creq)
	require_NoError(t, err)
	ci := &ClientInfo{Account: globalAccountName, Cluster: "R3S"}
	ml.jsClusteredConsumerRequest(ci, ml.globalAccount(), JSApiDurableCreateT, _EMPTY_, nil, msg, creq)
	sjs.mu.Lock()
	defer sjs.mu.Unlock()
	nca := sjs.consumerAssignmentOrInflight(globalAccountName, "TEST", "C")
	require_NotNil(t, nca)
	require_Equal(t, nca.Config.Replicas, 3)
	requireCounts(peerAssets{streams: 1, ha: 2})

	// Reverting the consumer and the stream restores the counts.
	cc.trackInflightConsumerProposal(globalAccountName, "TEST", ca, false)
	requireCounts(peerAssets{streams: 1, ha: 2})
	cc.trackInflightStreamProposal(globalAccountName, sa, false)
	requireCounts(peerAssets{streams: 1, ha: 1})
}

func TestJetStreamClusterMaxHAAssetsScaleUp(t *testing.T) {
	tmpl := strings.Replace(jsClusterTempl, "store_dir:", "limits: {max_ha_assets: 3}, store_dir:", 1)
	c := createJetStreamClusterWithTemplateAndModHook(t, tmpl, "R3S", 3, nil)
	defer c.shutdown()

	nc, js := jsClientConnect(t, c.randomServer())
	defer nc.Close()

	// Every server holds 2 HA assets.
	_, err := js.AddStream(&nats.StreamConfig{Name: "S1", Subjects: []string{"s1"}, Replicas: 3})
	require_NoError(t, err)
	_, err = js.AddConsumer("S1", &nats.ConsumerConfig{Durable: "C1", AckPolicy: nats.AckExplicitPolicy})
	require_NoError(t, err)

	// R1 assets are not limited.
	_, err = js.AddStream(&nats.StreamConfig{Name: "S2", Subjects: []string{"s2"}, Replicas: 1})
	require_NoError(t, err)
	_, err = js.AddConsumer("S2", &nats.ConsumerConfig{Durable: "C2", AckPolicy: nats.AckExplicitPolicy})
	require_NoError(t, err)

	// Scaling up would add the stream and its inherited consumer, 2 HA assets per server.
	_, err = js.UpdateStream(&nats.StreamConfig{Name: "S2", Subjects: []string{"s2"}, Replicas: 3})
	require_Error(t, err, NewJSInsufficientResourcesError())

	// An R1 consumer isn't HA on a limits stream, but interest streams remap it onto the stream's peers.
	require_NoError(t, js.DeleteConsumer("S2", "C2"))
	_, err = js.AddConsumer("S2", &nats.ConsumerConfig{Durable: "C2", AckPolicy: nats.AckExplicitPolicy, Replicas: 1})
	require_NoError(t, err)
	_, err = js.UpdateStream(&nats.StreamConfig{Name: "S2", Subjects: []string{"s2"}, Replicas: 3, Retention: nats.InterestPolicy})
	require_Error(t, err, NewJSInsufficientResourcesError())

	// Only the stream becomes HA, which fits.
	_, err = js.UpdateStream(&nats.StreamConfig{Name: "S2", Subjects: []string{"s2"}, Replicas: 3})
	require_NoError(t, err)
	checkFor(t, 5*time.Second, 200*time.Millisecond, func() error {
		ml := c.leader()
		sjs := ml.getJetStream()
		sjs.mu.RLock()
		defer sjs.mu.RUnlock()
		if sa := sjs.streamAssignment(globalAccountName, "S2"); sa == nil || len(sa.Group.Peers) != 3 || sa.Group.Desired != nil {
			return errors.New("scale up not converged yet")
		}
		return nil
	})

	// Can't add another HA consumer, but R1 consumers are fine.
	_, err = js.AddConsumer("S2", &nats.ConsumerConfig{Durable: "C3", AckPolicy: nats.AckExplicitPolicy})
	require_Error(t, err, NewJSInsufficientResourcesError())
	_, err = js.AddConsumer("S2", &nats.ConsumerConfig{Durable: "C4", AckPolicy: nats.AckExplicitPolicy, Replicas: 1})
	require_NoError(t, err)

	// Scaling up the R1 consumer is also limited.
	_, err = js.UpdateConsumer("S2", &nats.ConsumerConfig{Durable: "C4", AckPolicy: nats.AckExplicitPolicy, Replicas: 3})
	require_Error(t, err, NewJSInsufficientResourcesError())
}

func TestJetStreamClusterMaxHAAssetsR1MoveNotLimited(t *testing.T) {
	tmpl := strings.Replace(jsClusterTempl, "store_dir:", "limits: {max_ha_assets: 1}, store_dir:", 1)
	c := createJetStreamClusterWithTemplateAndModHook(t, tmpl, "R3S", 3, nil)
	defer c.shutdown()

	nc, js := jsClientConnect(t, c.randomServer())
	defer nc.Close()

	// Every server is at the limit.
	_, err := js.AddStream(&nats.StreamConfig{Name: "S1", Subjects: []string{"s1"}, Replicas: 3})
	require_NoError(t, err)
	si, err := js.AddStream(&nats.StreamConfig{Name: "S2", Subjects: []string{"s2"}, Replicas: 1})
	require_NoError(t, err)
	origin := si.Cluster.Leader
	_, err = js.AddConsumer("S2", &nats.ConsumerConfig{Durable: "DUR", AckPolicy: nats.AckExplicitPolicy})
	require_NoError(t, err)
	const numMsgs = 10
	for range numMsgs {
		_, err = js.Publish("s2", nil)
		require_NoError(t, err)
	}

	ncsys, err := nats.Connect(c.randomServer().ClientURL(), nats.UserInfo("admin", "s3cr3t!"))
	require_NoError(t, err)
	defer ncsys.Close()

	// Moving an R1 stream and its consumer temporarily uses Raft groups, but those aren't HA assets.
	moveReq, err := json.Marshal(&JSApiMetaServerStreamMoveRequest{Server: origin})
	require_NoError(t, err)
	rmsg, err := ncsys.Request(fmt.Sprintf(JSApiServerStreamMoveT, globalAccountName, "S2"), moveReq, 5*time.Second)
	require_NoError(t, err)
	var moveResp JSApiStreamUpdateResponse
	require_NoError(t, json.Unmarshal(rmsg.Data, &moveResp))
	require_True(t, moveResp.Error == nil)

	checkFor(t, 10*time.Second, 200*time.Millisecond, func() error {
		si, err := js.StreamInfo("S2")
		if err != nil {
			return err
		}
		if si.Cluster.Leader == _EMPTY_ || si.Cluster.Leader == origin || len(si.Cluster.Replicas) > 0 {
			return fmt.Errorf("move not completed yet: %+v", si.Cluster)
		}
		ci, err := js.ConsumerInfo("S2", "DUR")
		if err != nil {
			return err
		}
		if ci.Cluster.Leader != si.Cluster.Leader || len(ci.Cluster.Replicas) > 0 {
			return fmt.Errorf("consumer move not completed yet: %+v", ci.Cluster)
		}
		return nil
	})

	// The consumer is still usable.
	sub, err := js.PullSubscribe("s2", "DUR")
	require_NoError(t, err)
	msgs, err := sub.Fetch(numMsgs, nats.MaxWait(2*time.Second))
	require_NoError(t, err)
	require_Len(t, len(msgs), numMsgs)

	// Only the R3 stream counts as an HA asset.
	sjs := c.leader().getJetStream()
	sjs.mu.RLock()
	defer sjs.mu.RUnlock()
	for _, s := range c.servers {
		require_Equal(t, sjs.cluster.peerAssets[s.NodeName()].ha, 1)
	}
}

func TestJetStreamClusterMaxHAAssetsConsumerStackedScaleUp(t *testing.T) {
	tmpl := strings.Replace(jsClusterTempl, "store_dir:", "limits: {max_ha_assets: 2}, store_dir:", 1)
	c := createJetStreamClusterWithTemplateAndModHook(t, tmpl, "R5S", 5, nil)
	defer c.shutdown()

	nc, js := jsClientConnect(t, c.randomServer())
	defer nc.Close()

	// Every server holds 1 HA asset.
	_, err := js.AddStream(&nats.StreamConfig{Name: "TEST", Replicas: 5})
	require_NoError(t, err)
	_, err = js.AddConsumer("TEST", &nats.ConsumerConfig{Durable: "C", AckPolicy: nats.AckExplicitPolicy, Replicas: 1})
	require_NoError(t, err)

	// Simulate an inflight R1 -> R3 scale up, which is counted on its desired peers.
	sjs := c.leader().getJetStream()
	sjs.mu.Lock()
	cc := sjs.cluster
	sa := sjs.streamAssignment(globalAccountName, "TEST")
	require_NotNil(t, sa)
	ca := sa.consumers["C"]
	require_NotNil(t, ca)
	desired := []string{ca.Group.Peers[0]}
	var spare []string
	for _, peer := range sa.Group.Peers {
		if slices.Contains(desired, peer) {
			continue
		} else if len(desired) < 3 {
			desired = append(desired, peer)
		} else {
			spare = append(spare, peer)
		}
	}
	nca := ca.copyGroup()
	ncfg := *ca.Config
	ncfg.Replicas = 3
	nca.Config = &ncfg
	nca.Group = ca.Group.withDesired(&raftGroup{Name: ca.Group.Name, Peers: desired, Storage: ca.Group.Storage, Cluster: ca.Group.Cluster})
	cc.trackConsumerAssets(globalAccountName, "TEST", nca, false)
	sa.consumers["C"] = nca
	for _, peer := range desired {
		require_Equal(t, cc.peerAssets[peer].ha, 2)
	}
	// One of the peers that's not part of the inflight scale up is at the limit.
	cc.adjustPeerAssets(spare[:1], 0, 1)
	sjs.mu.Unlock()

	// Stacking a scale up to R5 is limited by the peers that gain the consumer.
	_, err = js.UpdateConsumer("TEST", &nats.ConsumerConfig{Durable: "C", AckPolicy: nats.AckExplicitPolicy, Replicas: 5})
	require_Error(t, err, NewJSInsufficientResourcesError())

	// But not by the peers of the inflight scale up, which already count the consumer.
	sjs.mu.Lock()
	cc.adjustPeerAssets(spare[:1], 0, -1)
	sjs.mu.Unlock()
	_, err = js.UpdateConsumer("TEST", &nats.ConsumerConfig{Durable: "C", AckPolicy: nats.AckExplicitPolicy, Replicas: 5})
	require_NoError(t, err)
}

func TestJetStreamClusterMaxHAAssetsPlacementMoveOverlap(t *testing.T) {
	tmpl := strings.Replace(jsClusterTempl, "store_dir:", "limits: {max_ha_assets: 2}, store_dir:", 1)
	tags := map[string]string{"S-1": "a", "S-2": "a, b", "S-3": "a, b", "S-4": "b"}
	c := createJetStreamClusterWithTemplateAndModHook(t, tmpl, "R4S", 4, func(serverName, _, _, conf string) string {
		return fmt.Sprintf("%s\nserver_tags: [%s]", conf, tags[serverName])
	})
	defer c.shutdown()

	nc, js := jsClientConnect(t, c.randomServer())
	defer nc.Close()

	// The stream and its inherited consumer put every tagged 'a' server at the limit.
	cfg := &nats.StreamConfig{Name: "TEST", Replicas: 3, Placement: &nats.Placement{Tags: []string{"a"}}}
	_, err := js.AddStream(cfg)
	require_NoError(t, err)
	_, err = js.AddConsumer("TEST", &nats.ConsumerConfig{Durable: "C", AckPolicy: nats.AckExplicitPolicy})
	require_NoError(t, err)

	// Moving onto the tagged 'b' servers keeps two of its peers, which don't gain any HA assets.
	cfg.Placement.Tags = []string{"b"}
	_, err = js.UpdateStream(cfg)
	require_NoError(t, err)

	want := []string{c.serverByName("S-2").NodeName(), c.serverByName("S-3").NodeName(), c.serverByName("S-4").NodeName()}
	slices.Sort(want)
	checkFor(t, 10*time.Second, 200*time.Millisecond, func() error {
		sjs := c.leader().getJetStream()
		sjs.mu.RLock()
		defer sjs.mu.RUnlock()
		sa := sjs.streamAssignment(globalAccountName, "TEST")
		if sa == nil || sa.Group.Desired != nil {
			return errors.New("move not converged yet")
		}
		peers := slices.Sorted(slices.Values(sa.Group.Peers))
		if !slices.Equal(peers, want) {
			return fmt.Errorf("expected peers %v, got %v", want, peers)
		}
		return nil
	})

	// An interest stream on the tagged 'a' servers.
	require_NoError(t, js.DeleteStream("TEST"))
	_, err = js.AddStream(&nats.StreamConfig{Name: "INTEREST", Replicas: 3, Retention: nats.InterestPolicy, Placement: &nats.Placement{Tags: []string{"a"}}})
	require_NoError(t, err)

	// Simulate an inflight move onto the tagged 'b' servers, which is counted on its desired peers.
	sjs := c.leader().getJetStream()
	sjs.mu.Lock()
	cc := sjs.cluster
	sa := sjs.streamAssignment(globalAccountName, "INTEREST")
	require_NotNil(t, sa)
	nsa := sa.copyGroup()
	nsa.Group = sa.Group.withDesired(&raftGroup{Name: sa.Group.Name, Peers: want, Storage: sa.Group.Storage, Cluster: sa.Group.Cluster})
	cc.trackStreamAssets(globalAccountName, nsa, false)
	sa.Group = nsa.Group
	s1, s4 := c.serverByName("S-1").NodeName(), c.serverByName("S-4").NodeName()
	require_Equal(t, cc.peerAssets[s1].ha, 0)
	require_Equal(t, cc.peerAssets[s4].ha, 1)
	// The move's destination, not part of the stream's current peers, is at the limit.
	cc.adjustPeerAssets([]string{s4}, 0, 1)
	sjs.mu.Unlock()

	// A new consumer is placed on the stream's current peers, but is counted on its desired peers.
	_, err = js.AddConsumer("INTEREST", &nats.ConsumerConfig{Durable: "C", AckPolicy: nats.AckExplicitPolicy})
	require_Error(t, err, NewJSInsufficientResourcesError())

	sjs.mu.Lock()
	cc.adjustPeerAssets([]string{s4}, 0, -1)
	sjs.mu.Unlock()
	_, err = js.AddConsumer("INTEREST", &nats.ConsumerConfig{Durable: "C", AckPolicy: nats.AckExplicitPolicy})
	require_NoError(t, err)
}

func TestJetStreamClusterMaxHAAssetsLoweredBelowExisting(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "R3S", 3)
	defer c.shutdown()

	nc, js := jsClientConnect(t, c.randomServer())
	defer nc.Close()

	// The stream and its consumers put 3 HA assets on every server.
	_, err := js.AddStream(&nats.StreamConfig{Name: "TEST", Subjects: []string{"foo"}, Replicas: 3})
	require_NoError(t, err)
	for _, name := range []string{"C1", "C2"} {
		_, err = js.AddConsumer("TEST", &nats.ConsumerConfig{Durable: name, AckPolicy: nats.AckExplicitPolicy})
		require_NoError(t, err)
	}
	nc.Close()

	// Lower the limit below the existing HA assets.
	c.stopAll()
	for _, o := range c.opts {
		b, err := os.ReadFile(o.ConfigFile)
		require_NoError(t, err)
		conf := strings.Replace(string(b), "store_dir:", "limits: {max_ha_assets: 2}, store_dir:", 1)
		require_NoError(t, os.WriteFile(o.ConfigFile, []byte(conf), defaultFilePerms))
	}
	c.restartAll()
	c.waitOnClusterReady()
	for _, s := range c.servers {
		require_Equal(t, s.getOpts().JetStreamLimits.MaxHAAssets, 2)
	}

	// Existing assets must all come up, even though they exceed the limit.
	c.waitOnStreamLeader(globalAccountName, "TEST")
	c.waitOnConsumerLeader(globalAccountName, "TEST", "C1")
	c.waitOnConsumerLeader(globalAccountName, "TEST", "C2")
	nc, js = jsClientConnect(t, c.randomServer())
	defer nc.Close()
	_, err = js.Publish("foo", nil)
	require_NoError(t, err)

	// But new assets are limited.
	_, err = js.AddStream(&nats.StreamConfig{Name: "OTHER", Subjects: []string{"bar"}, Replicas: 3})
	require_Error(t, err, errors.New("no suitable peers"))
}

func TestJetStreamClusterMaxHAAssetsPeerRemoveIgnoresLimit(t *testing.T) {
	tmpl := strings.Replace(jsClusterTempl, "store_dir:", "limits: {max_ha_assets: 1}, store_dir:", 1)
	c := createJetStreamClusterWithTemplateAndModHook(t, tmpl, "R4S", 4, nil)
	defer c.shutdown()

	nc, js := jsClientConnect(t, c.randomServer())
	defer nc.Close()

	si, err := js.AddStream(&nats.StreamConfig{Name: "TEST", Replicas: 3})
	require_NoError(t, err)
	toRemove := si.Cluster.Replicas[0].Name

	// Mark the only server not hosting the stream as being at the limit.
	var spare *Server
	sjs := c.leader().getJetStream()
	sjs.mu.Lock()
	sa := sjs.streamAssignment(globalAccountName, "TEST")
	for _, s := range c.servers {
		if !slices.Contains(sa.Group.Peers, s.NodeName()) {
			spare = s
			sjs.cluster.adjustPeerAssets([]string{s.NodeName()}, 0, 1)
		}
	}
	sjs.mu.Unlock()
	require_NotNil(t, spare)

	// User-requested placement is limited.
	_, err = js.AddStream(&nats.StreamConfig{Name: "OTHER", Subjects: []string{"other"}, Replicas: 3})
	require_Error(t, err, errors.New("no suitable peers"))

	// But a peer-remove is a system-level repair, which ignores the limit.
	resp, err := nc.Request(fmt.Sprintf(JSApiStreamRemovePeerT, "TEST"), []byte(`{"peer":"`+toRemove+`"}`), time.Second)
	require_NoError(t, err)
	var rpResp JSApiStreamRemovePeerResponse
	require_NoError(t, json.Unmarshal(resp.Data, &rpResp))
	require_True(t, rpResp.Success)

	checkFor(t, 10*time.Second, 200*time.Millisecond, func() error {
		si, err := js.StreamInfo("TEST")
		if err != nil {
			return err
		}
		peers := []string{si.Cluster.Leader}
		for _, r := range si.Cluster.Replicas {
			peers = append(peers, r.Name)
		}
		if len(peers) != 3 || slices.Contains(peers, toRemove) || !slices.Contains(peers, spare.Name()) {
			return fmt.Errorf("peer not replaced yet: %v", peers)
		}
		return nil
	})
}

func TestJetStreamClusterSubjectDeleteMarkersMinimumTTL(t *testing.T) {
	for _, storageType := range []StorageType{FileStorage, MemoryStorage} {
		for _, replicas := range []int{1, 3} {
			t.Run(fmt.Sprintf("%s/R%d", storageType, replicas), func(t *testing.T) {
				c := createJetStreamClusterExplicit(t, "R3S", 3)
				defer c.shutdown()

				nc, js := jsClientConnect(t, c.randomServer())
				defer nc.Close()

				_, err := jsStreamCreate(t, nc, &StreamConfig{
					Name:                   "TEST",
					Retention:              LimitsPolicy,
					Subjects:               []string{"foo"},
					Replicas:               replicas,
					Storage:                storageType,
					SubjectDeleteMarkerTTL: 3 * time.Second,
					AllowMsgTTL:            true,
				})
				require_NoError(t, err)

				m := nats.NewMsg("foo")
				m.Header.Set(JSMessageTTL, "1s")
				_, err = js.PublishMsg(m)
				require_NoError(t, err)

				_, err = js.GetMsg("TEST", 1)
				require_NoError(t, err)

				// After the TTL expires it should still be there, because SubjectDeleteMarkerTTL is the minimum.
				time.Sleep(1500 * time.Millisecond)
				rsm, err := js.GetMsg("TEST", 1)
				require_NoError(t, err)
				require_Equal(t, rsm.Header.Get(JSMessageTTL), "3") // 3s from the SDM TTL.

				// Need to wait for the subject delete marker to be placed.
				time.Sleep(2 * time.Second)

				_, err = js.GetMsg("TEST", 1)
				require_Error(t, err, nats.ErrMsgNotFound)

				// Expect a subject delete marker based on per-message TTL.
				sm, err := js.GetMsg("TEST", 2)
				require_NoError(t, err)
				require_Equal(t, sm.Header.Get(JSMarkerReason), JSMarkerReasonMaxAge)
				require_Equal(t, sm.Subject, "foo")

				// Since we have a subject delete marker now with a higher TTL, if this
				// message's lower TTL would be respected then we would not have a new
				// subject delete marker when this message expires.
				pa, err := js.PublishMsg(m)
				require_NoError(t, err)
				require_Equal(t, pa.Sequence, 3)

				// After some time the first subject delete marker should be gone, and the new one should be placed.
				time.Sleep(3500 * time.Millisecond)

				_, err = js.GetMsg("TEST", 2)
				require_Error(t, err, nats.ErrMsgNotFound)

				sm, err = js.GetMsg("TEST", 4)
				require_NoError(t, err)
				require_Equal(t, sm.Header.Get(JSMarkerReason), JSMarkerReasonMaxAge)
				require_Equal(t, sm.Subject, "foo")
			})
		}
	}
}

func TestJetStreamClusterSubjectDeleteMarkersMinimumTTLExceptionMaxMsgsPer(t *testing.T) {
	for _, storageType := range []StorageType{FileStorage, MemoryStorage} {
		for _, replicas := range []int{1, 3} {
			t.Run(fmt.Sprintf("%s/R%d", storageType, replicas), func(t *testing.T) {
				c := createJetStreamClusterExplicit(t, "R3S", 3)
				defer c.shutdown()

				nc, js := jsClientConnect(t, c.randomServer())
				defer nc.Close()

				_, err := jsStreamCreate(t, nc, &StreamConfig{
					Name:                   "TEST",
					Retention:              LimitsPolicy,
					Subjects:               []string{"foo"},
					Replicas:               replicas,
					Storage:                storageType,
					SubjectDeleteMarkerTTL: time.Hour,
					AllowMsgTTL:            true,
					MaxMsgsPer:             1,
				})
				require_NoError(t, err)

				m := nats.NewMsg("foo")
				m.Header.Set(JSMessageTTL, "1s")
				_, err = js.PublishMsg(m)
				require_NoError(t, err)

				_, err = js.GetMsg("TEST", 1)
				require_NoError(t, err)

				// After the TTL expires the message should be gone, even though SubjectDeleteMarkerTTL is the minimum
				// normally. This works because MaxMsgsPer=1 is set.
				time.Sleep(1500 * time.Millisecond)
				_, err = js.GetMsg("TEST", 1)
				require_Error(t, err, nats.ErrMsgNotFound)

				// Expect a subject delete marker based on per-message TTL.
				sm, err := js.GetMsg("TEST", 2)
				require_NoError(t, err)
				require_Equal(t, sm.Header.Get(JSMarkerReason), JSMarkerReasonMaxAge)
				require_Equal(t, sm.Subject, "foo")

				// Since we have a subject delete marker now with a higher TTL, if this
				// message's lower TTL would be respected then we would not have a new
				// subject delete marker when this message expires. But with MaxMsgsPer=1
				// this doesn't apply, because the subject delete marker gets removed.
				pa, err := js.PublishMsg(m)
				require_NoError(t, err)
				require_Equal(t, pa.Sequence, 3)

				// After some time the first subject delete marker should be gone, and the new one should be placed.
				time.Sleep(1500 * time.Millisecond)

				_, err = js.GetMsg("TEST", 2)
				require_Error(t, err, nats.ErrMsgNotFound)

				sm, err = js.GetMsg("TEST", 4)
				require_NoError(t, err)
				require_Equal(t, sm.Header.Get(JSMarkerReason), JSMarkerReasonMaxAge)
				require_Equal(t, sm.Subject, "foo")
			})
		}
	}
}

func TestJetStreamClusterSubjectDeleteMarkersNoMsgTTLSet(t *testing.T) {
	for _, storageType := range []StorageType{FileStorage, MemoryStorage} {
		for _, replicas := range []int{1, 3} {
			t.Run(fmt.Sprintf("%s/R%d", storageType, replicas), func(t *testing.T) {
				c := createJetStreamClusterExplicit(t, "R3S", 3)
				defer c.shutdown()

				nc, js := jsClientConnect(t, c.randomServer())
				defer nc.Close()

				_, err := jsStreamCreate(t, nc, &StreamConfig{
					Name:                   "TEST",
					Retention:              LimitsPolicy,
					Subjects:               []string{"foo"},
					Replicas:               replicas,
					Storage:                storageType,
					SubjectDeleteMarkerTTL: time.Second,
					AllowMsgTTL:            true,
				})
				require_NoError(t, err)

				_, err = js.Publish("foo", nil)
				require_NoError(t, err)

				_, err = js.GetMsg("TEST", 1)
				require_NoError(t, err)

				// Message should not expire based on SubjectDeleteMarkerTTL if no TTL is set.
				time.Sleep(1500 * time.Millisecond)
				_, err = js.GetMsg("TEST", 1)
				require_NoError(t, err)
			})
		}
	}
}

func TestJetStreamClusterSDMMaxAgeOnRecover(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "R3S", 3)
	defer c.shutdown()

	nc, js := jsClientConnect(t, c.randomServer())
	defer nc.Close()

	_, err := jsStreamCreate(t, nc, &StreamConfig{
		Name:                   "TEST",
		Retention:              LimitsPolicy,
		Subjects:               []string{"foo"},
		Storage:                FileStorage,
		Replicas:               3,
		MaxAge:                 time.Second,
		SubjectDeleteMarkerTTL: time.Second,
	})
	require_NoError(t, err)

	_, err = js.Publish("foo", nil)
	require_NoError(t, err)

	// Wait for all servers to have applied the published message.
	checkFor(t, 500*time.Millisecond, 50*time.Millisecond, func() error {
		return checkState(t, c, globalAccountName, "TEST")
	})

	rs := c.randomServer()
	for _, s := range c.servers {
		s.Shutdown()
	}

	// MaxAge would expire the message on recovery, ensure that's not done because we're clustered with SDM.
	time.Sleep(1500 * time.Millisecond)

	rs = c.restartServer(rs)
	acc, err := rs.lookupAccount(globalAccountName)
	require_NoError(t, err)
	mset, err := acc.lookupStream("TEST")
	require_NoError(t, err)
	_, err = mset.getMsg(1)
	require_NoError(t, err)
}

func TestJetStreamClusterSDMMaxAgeRemoveMsgProposal(t *testing.T) {
	for _, storageType := range []StorageType{FileStorage, MemoryStorage} {
		t.Run(storageType.String(), func(t *testing.T) {
			c := createJetStreamClusterExplicit(t, "R3S", 3)
			defer c.shutdown()

			nc, js := jsClientConnect(t, c.randomServer())
			defer nc.Close()

			_, err := jsStreamCreate(t, nc, &StreamConfig{
				Name:                   "TEST",
				Retention:              LimitsPolicy,
				Subjects:               []string{"foo"},
				Storage:                storageType,
				Replicas:               3,
				MaxAge:                 time.Second,
				SubjectDeleteMarkerTTL: time.Hour,
			})
			require_NoError(t, err)

			_, err = js.Publish("foo", nil)
			require_NoError(t, err)

			// Wait for all servers to have applied the published message.
			checkFor(t, 500*time.Millisecond, 50*time.Millisecond, func() error {
				return checkState(t, c, globalAccountName, "TEST")
			})

			rs := c.randomNonStreamLeader(globalAccountName, "TEST")
			acc, err := rs.lookupAccount(globalAccountName)
			require_NoError(t, err)
			mset, err := acc.lookupStream("TEST")
			require_NoError(t, err)
			rn := mset.raftNode()
			require_NoError(t, rn.PauseApply())

			// Check we can get the message.
			_, err = mset.getMsg(1)
			require_NoError(t, err)

			// Let MaxAge expire the message, but this replica is paused so should not remove on its own.
			time.Sleep(1500 * time.Millisecond)
			_, err = mset.getMsg(1)
			require_NoError(t, err)

			// Resume and check all replicas agree on the state.
			rn.ResumeApply()
			checkFor(t, 500*time.Millisecond, 50*time.Millisecond, func() error {
				return checkState(t, c, globalAccountName, "TEST")
			})

			// Now the message should be gone.
			_, err = mset.getMsg(1)
			require_Error(t, err, ErrStoreMsgNotFound)

			_, err = js.GetMsg("TEST", 1)
			require_Error(t, err, nats.ErrMsgNotFound)

			// Expect a subject delete marker.
			sm, err := js.GetMsg("TEST", 2)
			require_NoError(t, err)
			require_Equal(t, sm.Header.Get(JSMarkerReason), JSMarkerReasonMaxAge)
			require_Equal(t, sm.Subject, "foo")
		})
	}
}

func TestJetStreamClusterSDMMaxAgeRemoveMsgProposalLimitRetries(t *testing.T) {
	for _, storageType := range []StorageType{FileStorage, MemoryStorage} {
		t.Run(storageType.String(), func(t *testing.T) {
			c := createJetStreamClusterExplicit(t, "R3S", 3)
			defer c.shutdown()

			nc, js := jsClientConnect(t, c.randomServer())
			defer nc.Close()

			_, err := jsStreamCreate(t, nc, &StreamConfig{
				Name:                   "TEST",
				Retention:              LimitsPolicy,
				Subjects:               []string{"foo"},
				Storage:                storageType,
				Replicas:               3,
				MaxAge:                 time.Second,
				SubjectDeleteMarkerTTL: time.Hour,
			})
			require_NoError(t, err)

			_, err = js.Publish("foo", nil)
			require_NoError(t, err)

			// Wait for all servers to have applied the published message.
			checkFor(t, 500*time.Millisecond, 50*time.Millisecond, func() error {
				return checkState(t, c, globalAccountName, "TEST")
			})

			sl := c.streamLeader(globalAccountName, "TEST")
			acc, err := sl.lookupAccount(globalAccountName)
			require_NoError(t, err)
			mset, err := acc.lookupStream("TEST")
			require_NoError(t, err)
			rn := mset.raftNode().(*raft)

			// Force the leader to not be able to know its remove proposals were accepted.
			rn.Lock()
			pindex, pcommit := rn.pindex, rn.commit
			rn.commit = 1_000 * pcommit
			rn.Unlock()

			// Let MaxAge expire the message.
			time.Sleep(1500 * time.Millisecond)

			// Spam a bunch of expiry calls, shouldn't spam message delete proposals.
			for i := 0; i < 100; i++ {
				if fs, ok := mset.store.(*fileStore); ok {
					fs.expireMsgs()
				} else if ms, ok := mset.store.(*memStore); ok {
					ms.expireMsgs()
				}
				// Spread them out, so that they can't be grouped into just one Raft proposal.
				time.Sleep(2 * time.Millisecond)
			}

			// Put the original commit back, so they can now be committed/applied.
			rn.Lock()
			rn.commit = pcommit
			rn.Unlock()

			// Ensures we can check the stream leader's full log has been applied.
			_, err = js.Publish("foo", nil)
			require_NoError(t, err)

			// Expect two entries: one initial removal, one publish.
			nindex, _, _ := rn.Progress()
			require_Equal(t, nindex, pindex+2)
		})
	}
}

func TestJetStreamClusterSDMTTLRemoveMsgProposal(t *testing.T) {
	for _, storageType := range []StorageType{FileStorage, MemoryStorage} {
		t.Run(storageType.String(), func(t *testing.T) {
			c := createJetStreamClusterExplicit(t, "R3S", 3)
			defer c.shutdown()

			nc, js := jsClientConnect(t, c.randomServer())
			defer nc.Close()

			_, err := jsStreamCreate(t, nc, &StreamConfig{
				Name:                   "TEST",
				Retention:              LimitsPolicy,
				Subjects:               []string{"foo"},
				Storage:                storageType,
				Replicas:               3,
				SubjectDeleteMarkerTTL: 2 * time.Second,
			})
			require_NoError(t, err)

			m := nats.NewMsg("foo")
			m.Header.Set(JSMessageTTL, "2s")
			_, err = js.PublishMsg(m)
			require_NoError(t, err)

			// Wait for all servers to have applied the published message.
			checkFor(t, 500*time.Millisecond, 50*time.Millisecond, func() error {
				return checkState(t, c, globalAccountName, "TEST")
			})

			rs := c.randomNonStreamLeader(globalAccountName, "TEST")
			acc, err := rs.lookupAccount(globalAccountName)
			require_NoError(t, err)
			mset, err := acc.lookupStream("TEST")
			require_NoError(t, err)
			rn := mset.raftNode()
			require_NoError(t, rn.PauseApply())

			// Check we can get the message.
			_, err = mset.getMsg(1)
			require_NoError(t, err)

			// Let TTL expire the message, but this replica is paused so should not remove on its own.
			time.Sleep(2500 * time.Millisecond)
			_, err = mset.getMsg(1)
			require_NoError(t, err)

			// Resume and check all replicas agree on the state.
			rn.ResumeApply()
			checkFor(t, 500*time.Millisecond, 50*time.Millisecond, func() error {
				return checkState(t, c, globalAccountName, "TEST")
			})

			// Now the message should be gone.
			_, err = mset.getMsg(1)
			require_Error(t, err, ErrStoreMsgNotFound)

			_, err = js.GetMsg("TEST", 1)
			require_Error(t, err, nats.ErrMsgNotFound)

			// Expect a subject delete marker.
			sm, err := js.GetMsg("TEST", 2)
			require_NoError(t, err)
			require_Equal(t, sm.Header.Get(JSMarkerReason), JSMarkerReasonMaxAge)
			require_Equal(t, sm.Subject, "foo")
		})
	}
}

func TestJetStreamClusterSDMInflightTTL(t *testing.T) {
	for _, storageType := range []StorageType{FileStorage, MemoryStorage} {
		t.Run(storageType.String(), func(t *testing.T) {
			c := createJetStreamClusterExplicit(t, "R3S", 3)
			defer c.shutdown()

			nc, js := jsClientConnect(t, c.randomServer())
			defer nc.Close()

			_, err := jsStreamCreate(t, nc, &StreamConfig{
				Name:                   "TEST",
				Retention:              LimitsPolicy,
				Subjects:               []string{"foo"},
				Storage:                storageType,
				Replicas:               3,
				SubjectDeleteMarkerTTL: 2 * time.Second,
			})
			require_NoError(t, err)

			for i := 0; i < 2; i++ {
				m := nats.NewMsg("foo")
				m.Header.Set(JSMessageTTL, fmt.Sprintf("%dms", 2000+500*i))
				_, err = js.PublishMsg(m)
				require_NoError(t, err)
			}

			sl := c.streamLeader(globalAccountName, "TEST")
			acc, err := sl.lookupAccount(globalAccountName)
			require_NoError(t, err)
			mset, err := acc.lookupStream("TEST")
			require_NoError(t, err)
			rn := mset.raftNode().(*raft)

			// Force the leader to not be able to know its remove proposals were accepted.
			rn.Lock()
			pcommit := rn.commit
			rn.commit = 1_000 * pcommit
			rn.Unlock()

			// Check all messages are there.
			for seq := uint64(1); seq <= 2; seq++ {
				_, err = js.GetMsg("TEST", seq)
				require_NoError(t, err)
			}

			// Let TTL expire the messages while we're not applying the removals.
			time.Sleep(3 * time.Second)

			// Put the original commit back, so they can now be committed/applied.
			rn.Lock()
			rn.commit = pcommit
			rn.Unlock()

			// Ensures we can check the stream leader's full log has been applied.
			_, err = js.Publish("foo", nil)
			require_NoError(t, err)

			// Check all messages are removed.
			for seq := uint64(1); seq <= 2; seq++ {
				_, err = js.GetMsg("TEST", seq)
				require_Error(t, err, nats.ErrMsgNotFound)
			}

			// Expect a subject delete marker.
			sm, err := js.GetMsg("TEST", 3)
			require_NoError(t, err)
			require_Equal(t, sm.Header.Get(JSMarkerReason), JSMarkerReasonMaxAge)
			require_Equal(t, sm.Subject, "foo")
		})
	}
}

func TestJetStreamClusterSDMTTLAndMaxMsgsPer(t *testing.T) {
	for _, storageType := range []StorageType{FileStorage, MemoryStorage} {
		t.Run(storageType.String(), func(t *testing.T) {
			c := createJetStreamClusterExplicit(t, "R3S", 3)
			defer c.shutdown()

			nc, js := jsClientConnect(t, c.randomServer())
			defer nc.Close()

			_, err := jsStreamCreate(t, nc, &StreamConfig{
				Name:                   "TEST",
				Retention:              LimitsPolicy,
				Subjects:               []string{"foo"},
				Storage:                storageType,
				Replicas:               3,
				SubjectDeleteMarkerTTL: 2 * time.Second,
				// Used for the second part of this test, checks proper accounting
				// with removals through MaxMsgsPer and TTL.
				MaxMsgsPer: 3,
			})
			require_NoError(t, err)

			sl := c.streamLeader(globalAccountName, "TEST")
			acc, err := sl.lookupAccount(globalAccountName)
			require_NoError(t, err)
			mset, err := acc.lookupStream("TEST")
			require_NoError(t, err)

			checkNoPending := func() {
				t.Helper()
				if fs, ok := mset.store.(*fileStore); ok {
					fs.mu.RLock()
					pending := fs.sdm.pending
					fs.mu.RUnlock()
					if len(pending) > 0 {
						t.Fatalf("Expected no pending messages, but got: %v", pending)
					}
				} else if ms, ok := mset.store.(*memStore); ok {
					ms.mu.RLock()
					pending := ms.sdm.pending
					ms.mu.RUnlock()
					if len(pending) > 0 {
						t.Fatalf("Expected no pending messages, but got: %v", pending)
					}
				}
			}

			// Publish initial batch of messages with TTLs, but wait
			// in-between so they expire as we publish.
			for i := 0; i < 6; i++ {
				m := nats.NewMsg("foo")
				m.Header.Set(JSMessageTTL, "2s")
				_, err = js.PublishMsg(m)
				require_NoError(t, err)
				time.Sleep(500 * time.Millisecond)
			}

			// Wait for all messages to be expired.
			time.Sleep(2 * time.Second)

			// Check all messages are removed. Must not have subject delete markers be placed in-between.
			for seq := uint64(1); seq <= 6; seq++ {
				sm, err := js.GetMsg("TEST", seq)
				if sm != nil && sm.Header.Get(JSMarkerReason) != _EMPTY_ {
					t.Fatalf("Got unexpected subject delete marker at sequence %d", seq)
				}
				require_Error(t, err, nats.ErrMsgNotFound)
			}

			// Expect a subject delete marker.
			sm, err := js.GetMsg("TEST", 7)
			require_NoError(t, err)
			require_Equal(t, sm.Header.Get(JSMarkerReason), JSMarkerReasonMaxAge)
			require_Equal(t, sm.Subject, "foo")

			require_NoError(t, js.DeleteMsg("TEST", 7))
			checkNoPending()

			// Publish another batch of messages with TTLs, but only
			// wait initially and have MaxMsgsPer kick in during it.
			for i := 0; i < 6; i++ {
				m := nats.NewMsg("foo")
				m.Header.Set(JSMessageTTL, "2s")
				_, err = js.PublishMsg(m)
				require_NoError(t, err)
				if i < 3 {
					time.Sleep(500 * time.Millisecond)
				}
			}

			// Wait for all messages to be expired.
			time.Sleep(2500 * time.Millisecond)

			// Check all messages are removed. Must not have subject delete markers be placed in-between.
			for seq := uint64(8); seq <= 13; seq++ {
				sm, err = js.GetMsg("TEST", seq)
				if sm != nil && sm.Header.Get(JSMarkerReason) != _EMPTY_ {
					t.Fatalf("Got unexpected subject delete marker at sequence %d", seq)
				}
				require_Error(t, err, nats.ErrMsgNotFound)
			}

			checkNoPending()

			// Expect a subject delete marker.
			sm, err = js.GetMsg("TEST", 14)
			require_NoError(t, err)
			require_Equal(t, sm.Header.Get(JSMarkerReason), JSMarkerReasonMaxAge)
			require_Equal(t, sm.Subject, "foo")
		})
	}
}

func TestJetStreamClusterSDMMsgTTLReverseExpiry(t *testing.T) {
	for _, storageType := range []StorageType{FileStorage, MemoryStorage} {
		t.Run(storageType.String(), func(t *testing.T) {
			c := createJetStreamClusterExplicit(t, "R3S", 3)
			defer c.shutdown()

			nc, js := jsClientConnect(t, c.randomServer())
			defer nc.Close()

			_, err := jsStreamCreate(t, nc, &StreamConfig{
				Name:                   "TEST",
				Retention:              LimitsPolicy,
				Subjects:               []string{"foo", "bar"},
				Storage:                storageType,
				Replicas:               3,
				SubjectDeleteMarkerTTL: 2 * time.Second,
			})
			require_NoError(t, err)

			for i := 0; i < 2; i++ {
				m := nats.NewMsg("foo")
				m.Header.Set(JSMessageTTL, fmt.Sprintf("%ds", 3-i))
				_, err = js.PublishMsg(m)
				require_NoError(t, err)
			}

			// Wait for all servers to have applied the published message.
			checkFor(t, 500*time.Millisecond, 50*time.Millisecond, func() error {
				return checkState(t, c, globalAccountName, "TEST")
			})

			sl := c.streamLeader(globalAccountName, "TEST")
			acc, err := sl.lookupAccount(globalAccountName)
			require_NoError(t, err)
			mset, err := acc.lookupStream("TEST")
			require_NoError(t, err)
			rn := mset.raftNode().(*raft)

			// Force the leader to not be able to know its remove proposals were accepted.
			rn.Lock()
			pindex, pcommit := rn.pindex, rn.commit
			rn.commit = 1_000 * pcommit
			rn.Unlock()

			// Let TTL expire the messages, but the leader does not know the proposals will go through.
			time.Sleep(3500 * time.Millisecond)

			// Put the original commit back, so they can now be committed/applied.
			rn.Lock()
			rn.commit = pcommit
			rn.Unlock()

			// Eventually the message should be gone.
			// Ensures we can check the stream leader's full log has been applied.
			_, err = js.Publish("bar", nil)
			require_NoError(t, err)

			// Expect a subject delete marker.
			sm, err := js.GetMsg("TEST", 3)
			require_NoError(t, err)
			require_Equal(t, sm.Subject, "foo")
			require_Equal(t, sm.Header.Get(JSMarkerReason), JSMarkerReasonMaxAge)

			// Expect three entries, one leader change, the subject delete marker rollup, and one published message.
			nindex, _, _ := rn.Progress()
			require_Equal(t, nindex, pindex+3)
		})
	}
}

func TestJetStreamClusterSDMResetLast(t *testing.T) {
	for _, storageType := range []StorageType{FileStorage, MemoryStorage} {
		t.Run(storageType.String(), func(t *testing.T) {
			c := createJetStreamClusterExplicit(t, "R3S", 3)
			defer c.shutdown()

			nc, js := jsClientConnect(t, c.randomServer())
			defer nc.Close()

			_, err := jsStreamCreate(t, nc, &StreamConfig{
				Name:                   "TEST",
				Retention:              LimitsPolicy,
				Subjects:               []string{"foo"},
				Storage:                storageType,
				Replicas:               3,
				MaxAge:                 2 * time.Second,
				SubjectDeleteMarkerTTL: time.Second,
			})
			require_NoError(t, err)

			_, err = js.Publish("foo", nil)
			require_NoError(t, err)

			// Wait for all servers to have applied the published message.
			checkFor(t, 500*time.Millisecond, 50*time.Millisecond, func() error {
				return checkState(t, c, globalAccountName, "TEST")
			})

			sl := c.streamLeader(globalAccountName, "TEST")

			pauseApplies := func(pause bool) {
				for _, s := range c.servers {
					if s == sl {
						continue
					}
					acc, err := s.lookupAccount(globalAccountName)
					require_NoError(t, err)
					mset, err := acc.lookupStream("TEST")
					require_NoError(t, err)
					rn := mset.raftNode().(*raft)
					if pause {
						require_NoError(t, rn.PauseApply())
					} else {
						rn.ResumeApply()
					}
				}
			}

			// Pause applies on all replicas.
			pauseApplies(true)

			// Publish message after pause, so the replicas cache the first message as needing SDM.
			// Then once applies are resumed they should not place SDM.
			m := nats.NewMsg("foo")
			m.Header.Set(JSMessageTTL, "never")
			_, err = js.PublishMsg(m)
			require_NoError(t, err)

			acc, err := sl.lookupAccount(globalAccountName)
			require_NoError(t, err)
			mset, err := acc.lookupStream("TEST")
			require_NoError(t, err)
			rn := mset.raftNode().(*raft)

			// Force the leader to not be able to make proposals.
			rn.Lock()
			rn.werr = errors.New("block proposals")
			rn.Unlock()

			// Let MaxAge expire the message, but the replicas are paused.
			time.Sleep(2500 * time.Millisecond)

			// Resume applies on all replicas.
			pauseApplies(false)

			// Shutdown and wait for new stream leader.
			sl.Shutdown()
			c.waitOnStreamLeader(globalAccountName, "TEST")

			// Wait for the new leader to have removed the first message and added the second message.
			checkFor(t, 2500*time.Millisecond, 200*time.Millisecond, func() error {
				si, err := js.StreamInfo("TEST")
				if err != nil {
					return err
				}
				if si.State.Msgs != 1 || si.State.FirstSeq != 2 {
					return fmt.Errorf("incorrect state: %v", si.State)
				}
				return nil
			})

			_, err = js.GetMsg("TEST", 1)
			require_Error(t, err, nats.ErrMsgNotFound)

			// There should be no subject delete marker. Only the "never" TTL message.
			sm, err := js.GetMsg("TEST", 2)
			require_NoError(t, err)
			require_Equal(t, sm.Subject, "foo")
			require_Equal(t, sm.Header.Get(JSMessageTTL), "never")
			require_Equal(t, sm.Header.Get(JSMarkerReason), _EMPTY_)
		})
	}
}

func TestJetStreamClusterSDMMaxAgeProposeExpiryShortRetry(t *testing.T) {
	for _, storageType := range []StorageType{FileStorage, MemoryStorage} {
		t.Run(storageType.String(), func(t *testing.T) {
			c := createJetStreamClusterExplicit(t, "R3S", 3)
			defer c.shutdown()

			nc := clientConnectToServer(t, c.randomServer())
			defer nc.Close()

			cfg := &StreamConfig{
				Name:                   "TEST",
				Retention:              LimitsPolicy,
				Subjects:               []string{"foo"},
				Storage:                storageType,
				Replicas:               3,
				MaxAge:                 2 * time.Second,
				SubjectDeleteMarkerTTL: time.Hour,
			}

			_, err := jsStreamCreate(t, nc, cfg)
			require_NoError(t, err)

			sl := c.streamLeader(globalAccountName, "TEST")
			acc, err := sl.lookupAccount(globalAccountName)
			require_NoError(t, err)
			mset, err := acc.lookupStream("TEST")
			require_NoError(t, err)

			// Manually store a message with a timestamp far in the past.
			if fs, ok := mset.store.(*fileStore); ok {
				require_NoError(t, fs.StoreRawMsg("foo", nil, nil, 1, 1, 0, false))
			} else if ms, ok := mset.store.(*memStore); ok {
				require_NoError(t, ms.StoreRawMsg("foo", nil, nil, 1, 1, 0, false))
			}
			// We'll also need to manually adjust the upper-layer last sequence.
			mset.mu.Lock()
			mset.lseq = 1
			mset.mu.Unlock()

			cfg.MaxAge = time.Hour
			_, err = jsStreamUpdate(t, nc, cfg)
			require_NoError(t, err)

			if fs, ok := mset.store.(*fileStore); ok {
				fs.mu.Lock()
				fs.resetAgeChk(0)
				fs.mu.Unlock()
			} else if ms, ok := mset.store.(*memStore); ok {
				ms.mu.Lock()
				ms.resetAgeChk(0)
				ms.mu.Unlock()
			}

			checkFor(t, 3*time.Second, 100*time.Millisecond, func() error {
				_, err = mset.getMsg(1)
				if !errors.Is(err, ErrStoreMsgNotFound) {
					return fmt.Errorf("expected msg not found error: %v", err)
				}
				return nil
			})
		})
	}
}

func TestJetStreamClusterInvalidR1Config(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "R1TEST", 3)
	defer c.shutdown()

	nc, js := jsClientConnect(t, c.servers[0])
	defer nc.Close()

	nc2, js2 := jsClientConnect(t, c.servers[2])
	defer nc2.Close()

	createStreams := func(t *testing.T, js nats.JetStreamContext, n, replicas int) {
		for i := 0; i < n; i++ {
			sname := fmt.Sprintf("S:%d", i)
			js.AddStream(&nats.StreamConfig{
				Name:              sname,
				MaxMsgsPerSubject: 5,
				Replicas:          replicas,
				Subjects:          []string{fmt.Sprintf("A.%d.>", i)},
			})
			time.Sleep(10 * time.Millisecond)
			js.AddConsumer(sname, &nats.ConsumerConfig{
				Name:          sname,
				Durable:       sname,
				FilterSubject: ">",
			})
			js.Publish(fmt.Sprintf("A.%d.foo", i), []byte("one"))
		}
	}

	// Create 5 streams in parallel with different configs, then
	// check whether one of them is in an undefined state.
	totalStreams := 5

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		createStreams(t, js, totalStreams, 1)
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		createStreams(t, js2, totalStreams, 2)
	}()
	wg.Wait()

	for i := 0; i < totalStreams; i++ {
		ci, err := js.StreamInfo(fmt.Sprintf("S:%d", i))
		require_NoError(t, err)

		// Make sure that consumer scale up when peers are missing responds with error.
		if ci.Config.Replicas == 2 {
			// Starting with a single replica should still be valid.
			js.AddConsumer(ci.Config.Name, &nats.ConsumerConfig{
				Name:     "test",
				Replicas: 1,
			})
			_, err = js.UpdateConsumer(ci.Config.Name, &nats.ConsumerConfig{
				Name:     "test",
				Replicas: 2,
			})
			if err != nil {
				require_Equal(t, err.Error(), "nats: consumer config replica count exceeds parent stream")
			}
		}
	}
}

func TestJetStreamClusterMultiLeaderR3Config(t *testing.T) {
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
		server_tags: ["test"]
		system_account: sys
		no_auth_user: js
		accounts {
			sys { users = [ { user: sys, pass: sys } ] }
			js {
				jetstream = enabled
				users = [ { user: js, pass: js } ]
		    }
		}`
	c := createJetStreamClusterWithTemplate(t, conf, "R3TEST", 3)
	defer c.shutdown()

	nc, js := jsClientConnect(t, c.servers[0])
	defer nc.Close()

	nc2, js2 := jsClientConnect(t, c.servers[2])
	defer nc2.Close()

	createStreams := func(t *testing.T, js nats.JetStreamContext, n, replicas int) {
		for i := 0; i < n; i++ {
			sname := fmt.Sprintf("S:%d", i)
			js.AddStream(&nats.StreamConfig{
				Name:              sname,
				MaxMsgsPerSubject: 5,
				Replicas:          replicas,
				Subjects:          []string{fmt.Sprintf("A.%d.>", i)},
			})
			time.Sleep(10 * time.Millisecond)
			js.AddConsumer(sname, &nats.ConsumerConfig{
				Name:          sname,
				Durable:       sname,
				FilterSubject: ">",
			})
			js.Publish(fmt.Sprintf("A.%d.foo", i), []byte("one"))
		}
	}

	// Create streams in parallel with different configs, then
	// check whether one of them is in an undefined state.
	totalStreams := 5

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		createStreams(t, js, totalStreams, 3)
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		createStreams(t, js2, totalStreams, 1)
	}()
	wg.Wait()

	checkMultiLeader := func(accountName, streamName string) error {
		leaders := make(map[string]bool)
		for _, srv := range c.servers {
			jsz, err := srv.Jsz(&JSzOptions{Accounts: true, Streams: true, Consumer: true})
			if err != nil {
				return err
			}
			for _, acc := range jsz.AccountDetails {
				if acc.Name == accountName {
					for _, stream := range acc.Streams {
						if stream.Name == streamName {
							leaders[stream.Cluster.Leader] = true
						}
					}
				}
			}
		}
		if len(leaders) > 1 {
			return fmt.Errorf("There are multiple leaders on %s stream: %+v", streamName, leaders)
		}
		return nil
	}

	var invalidStream string
	for i := 0; i < totalStreams; i++ {
		ci, err := js.StreamInfo(fmt.Sprintf("S:%d", i))
		require_NoError(t, err)
		if ci.Config.Replicas == 1 {
			if ci.Cluster != nil {
				peers := ci.Cluster.Replicas
				if len(peers) > 1 {
					invalidStream = ci.Config.Name
					t.Errorf("Unexpected stream config drift, 1 replica expected but found %v peers", len(peers))
				}
			}
		}
	}
	if len(invalidStream) > 0 {
		_, err := js.StreamInfo(invalidStream)
		require_NoError(t, err)

		// Restart server where first client is connected and almost all R1 replicas landed.
		srv := c.servers[0]
		srv.Shutdown()
		srv.WaitForShutdown()
		time.Sleep(2 * time.Second)
		c.restartServer(srv)
		c.waitOnClusterReady()
		checkFor(t, 30*time.Second, 200*time.Millisecond, func() error {
			return checkMultiLeader("js", invalidStream)
		})
	}
}

func TestJetStreamClusterAccountMaxConnectionsReconnect(t *testing.T) {
	conf := `
                listen: 127.0.0.1:-1
                http: -1
                server_name: %s
                jetstream: {
                        store_dir: '%s',
                }
                cluster {
                        name: %s
                        listen: 127.0.0.1:%d
                        routes = [%s]
                }
                server_tags: ["test"]
                system_account: sys
                no_auth_user: js
                accounts {
                     sys { users = [ { user: sys, pass: sys } ] }
                     js {
                         jetstream = enabled
                         users = [ { user: js, pass: js } ]
                         limits {
                           max_connections: 5
                         }
                     }
                }
        `
	c := createJetStreamClusterWithTemplate(t, conf, "R3CONNECT", 3)
	defer c.shutdown()
	var conns []*nats.Conn

	disconnects := make([]chan error, 0)
	for i := 1; i <= 5; i++ {
		disconnectCh := make(chan error)
		// Reconnect quickly, a kicked client that retries the same server is rejected again.
		c, _ := jsClientConnect(t, c.servers[0], nats.UserInfo("js", "js"),
			nats.ReconnectWait(50*time.Millisecond), nats.ReconnectJitter(0, 0),
			nats.DisconnectErrHandler(func(_ *nats.Conn, err error) {
				disconnectCh <- err
			}))
		defer c.Close()
		conns = append(conns, c)
		disconnects = append(disconnects, disconnectCh)
		// Small delay to ensure distinct start times.
		time.Sleep(10 * time.Millisecond)
	}
	s := c.servers[0]
	acc, err := s.lookupAccount("js")
	require_NoError(t, err)

	checkCounts := func(expectedNumConnections, expectedJSClients, expectedTotalClients int) {
		t.Helper()
		checkFor(t, 2*time.Second, 15*time.Millisecond, func() error {
			acc.mu.RLock()
			clients := acc.getClientsLocked()
			numConnections := acc.NumConnections()
			jsClients := int(acc.sysclients)
			totalClients := len(clients)
			acc.mu.RUnlock()

			if numConnections != expectedNumConnections {
				return fmt.Errorf("Expected %d connections got %d", expectedNumConnections, numConnections)
			}
			if jsClients != expectedJSClients {
				return fmt.Errorf("Expected %d js clients got %d", expectedJSClients, jsClients)
			}
			if totalClients != expectedTotalClients {
				return fmt.Errorf("Expected %d total clients got %d", expectedTotalClients, totalClients)
			}
			return nil
		})
	}

	checkCounts(5, 0, 5)

	nc := conns[0]
	js, _ := nc.JetStream()
	for i := 0; i < 10; i++ {
		_, err := js.AddStream(&nats.StreamConfig{
			Name:     fmt.Sprintf("foo:%d", i),
			Subjects: []string{fmt.Sprintf("foo.%d", i)},
			Replicas: 3, // Must be R3 to stop flaking due to stream placement
		})
		require_NoError(t, err)

		_, err = js.Publish(fmt.Sprintf("foo.%d", i), []byte("hello"), nats.AckWait(5*time.Second))
		require_NoError(t, err)
	}

	checkCounts(5, 20, 25)

	checkFor(t, 30*time.Second, 200*time.Millisecond, func() error {
		for i := 0; i < 10; i++ {
			_, err := js.Publish(fmt.Sprintf("foo.%d", i), []byte("hello"), nats.AckWait(5*time.Second))
			if err != nil {
				return err
			}
		}
		return nil
	})

	// Force account update to trigger connection limit enforcement.
	accClaims := jwt.NewAccountClaims(acc.Name)
	accClaims.Limits.Conn = 1
	accClaims.Limits.MemoryStorage = -1
	accClaims.Limits.DiskStorage = -1
	accClaims.Limits.Streams = -1
	accClaims.Limits.Consumer = -1

	// Update server, before this would have disconnected JS internal clients with
	// 'JETSTREAM - maximum account active connections exceeded'.
	s.UpdateAccountClaims(acc, accClaims)

	// Allow some time for enforcement.
	time.Sleep(100 * time.Millisecond)

	acc, err = s.lookupAccount("js")
	require_NoError(t, err)

	// JETSTREAM internal clients should still linger after reducing connections.
	checkCounts(5, 20, 20)

	// Wait for disconnections from the most recent client.
	disconnectCh := disconnects[2]
	select {
	case <-disconnectCh:
	case <-time.After(2 * time.Second):
		t.Fatal("Expected newest connection to disconnect!")
	}

	checkFor(t, 30*time.Second, 200*time.Millisecond, func() error {
		activeConnections := 0
		for _, conn := range conns {
			if !conn.IsClosed() {
				activeConnections++
			}
		}
		if activeConnections < 5 {
			return fmt.Errorf("Unexpected number of connections: %d", activeConnections)
		}
		return nil
	})

	// Force account update to trigger connection limit enforcement.
	accClaims = jwt.NewAccountClaims(acc.Name)
	accClaims.Limits.Conn = 10
	accClaims.Limits.MemoryStorage = -1
	accClaims.Limits.DiskStorage = -1
	accClaims.Limits.Streams = -1
	accClaims.Limits.Consumer = -1

	// Update all servers then confirm that internal JS clients should work
	// and clients have reconnected.
	for _, s := range c.servers {
		acc, err := s.lookupAccount("js")
		require_NoError(t, err)
		s.UpdateAccountClaims(acc, accClaims)
	}
	checkFor(t, 30*time.Second, 200*time.Millisecond, func() error {
		for _, nc := range conns {
			js, _ := nc.JetStream()
			for i := 0; i < 10; i++ {
				stream := fmt.Sprintf("foo.%d", i)
				_, err := js.Publish(stream, []byte("hello"), nats.AckWait(5*time.Second))
				if err != nil {
					return err
				}
			}
		}
		return nil
	})
}

func TestJetStreamClusterMetaCompactThreshold(t *testing.T) {
	for _, thresh := range []uint64{0, 5, 10} {
		t.Run(fmt.Sprintf("%d", thresh), func(t *testing.T) {
			c := createJetStreamClusterExplicit(t, "R1TEST", 3)
			defer c.shutdown()
			for _, s := range c.servers {
				s.optsMu.Lock()
				s.opts.JetStreamMetaCompact = thresh
				s.optsMu.Unlock()
			}

			nc, _ := jsClientConnect(t, c.servers[0])
			defer nc.Close()

			leader := c.leader()
			_, cc := leader.getJetStreamCluster()
			rg := cc.meta.(*raft)

			// Kicking the leader change channel is the easiest way to
			// trick monitorCluster() into calling doSnapshot().
			kick := func() {
				select {
				case rg.leadc <- leadChange{isLeader: true, term: rg.Term()}:
				default:
				}
			}

			// We will get nowhere near math.MaxInt, as we will hit the
			// compaction threshold and return early, but keeps "i" moving up.
			for i := range math.MaxInt {
				jsStreamCreate(t, nc, &StreamConfig{
					Name:     fmt.Sprintf("test_%d", i),
					Subjects: []string{fmt.Sprintf("test.%d", i)},
					Storage:  MemoryStorage,
				})

				entries, _ := rg.Size()
				kick()

				// Should we have compacted on this iteration?
				if entries > thresh {
					checkFor(t, 2*time.Second, 50*time.Millisecond, func() error {
						if entries, _ := rg.Size(); entries > thresh {
							kick()
							return fmt.Errorf("haven't compacted yet (%d entries)", entries)
						}
						return nil
					})
					return
				}
			}
		})
	}
}

func TestJetStreamClusterMetaCompactSizeThreshold(t *testing.T) {
	for _, othresh := range []any{int64(1), "4K", "32K", "1M"} {
		t.Run(fmt.Sprintf("%v", othresh), func(t *testing.T) {
			it, err := getStorageSize(othresh)
			require_NoError(t, err)
			thresh := uint64(it)

			c := createJetStreamClusterExplicit(t, "R1TEST", 3)
			defer c.shutdown()
			for _, s := range c.servers {
				s.optsMu.Lock()
				s.opts.JetStreamMetaCompactSize = thresh
				s.optsMu.Unlock()
			}

			nc, _ := jsClientConnect(t, c.servers[0])
			defer nc.Close()

			leader := c.leader()
			_, cc := leader.getJetStreamCluster()
			rg := cc.meta.(*raft)

			// Kicking the leader change channel is the easiest way to
			// trick monitorCluster() into calling doSnapshot().
			kick := func() {
				select {
				case rg.leadc <- leadChange{isLeader: true, term: rg.Term()}:
				default:
				}
			}

			// We will get nowhere near math.MaxInt, as we will hit the
			// compaction threshold and return early, but keeps "i" moving up.
			for i := range math.MaxInt {
				jsStreamCreate(t, nc, &StreamConfig{
					Name:     fmt.Sprintf("test_%d", i),
					Subjects: []string{fmt.Sprintf("test.%d", i)},
					Storage:  MemoryStorage,
				})

				_, size := rg.Size()
				kick()

				// Should we have compacted on this iteration?
				if size > thresh {
					checkFor(t, 2*time.Second, 50*time.Millisecond, func() error {
						if _, size := rg.Size(); size > thresh {
							kick()
							return fmt.Errorf("haven't compacted yet (%d bytes)", size)
						}
						return nil
					})
					return
				}
			}
		})
	}
}

// Test that setStoreState treats ErrStoreOldUpdate as a non-error.
// This can happen during processClusterCreateConsumer when a consumer
// assignment carries stale state (e.g. from a stream restore or meta
// snapshot), but the consumer was already recovered from disk with
// newer state. Without the fix, the error would propagate and cause
// the consumer to be stopped and its Raft node deleted.
func TestJetStreamClusterConsumerSetStoreStateOldUpdate(t *testing.T) {
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
		Replicas:  3,
		AckPolicy: nats.AckExplicitPolicy,
	})
	require_NoError(t, err)

	// Publish messages and ack them to advance the consumer state.
	for i := 0; i < 10; i++ {
		_, err = js.Publish("foo", []byte("msg"))
		require_NoError(t, err)
	}
	sub, err := js.PullSubscribe("foo", "CONSUMER")
	require_NoError(t, err)
	msgs, err := sub.Fetch(10)
	require_NoError(t, err)
	require_Len(t, len(msgs), 10)
	for _, m := range msgs {
		err = m.AckSync()
		require_NoError(t, err)
	}

	// Wait for all consumers on all servers to be caught up to ack floor 10.
	checkFor(t, 5*time.Second, 200*time.Millisecond, func() error {
		for _, s := range c.servers {
			a, err := s.lookupAccount(globalAccountName)
			if err != nil {
				return err
			}
			ms, err := a.lookupStream("TEST")
			if err != nil {
				return err
			}
			co := ms.lookupConsumer("CONSUMER")
			if co == nil {
				return errors.New("consumer not found")
			}
			info := co.info()
			if info.AckFloor.Consumer != 10 {
				return fmt.Errorf("expected ack floor 10, got %d", info.AckFloor.Consumer)
			}
		}
		return nil
	})

	// Pick a follower server to test on. This simulates the scenario where
	// a consumer already exists with advanced state and then
	// processClusterCreateConsumer is called with stale state.
	rs := c.randomNonConsumerLeader(globalAccountName, "TEST", "CONSUMER")
	require_NotNil(t, rs)

	// Grab the consumer's Raft node before the stale state is applied.
	acc, err := rs.lookupAccount(globalAccountName)
	require_NoError(t, err)
	mset, err := acc.lookupStream("TEST")
	require_NoError(t, err)
	o := mset.lookupConsumer("CONSUMER")
	require_NotNil(t, o)
	o.mu.RLock()
	raftNode := o.node
	o.mu.RUnlock()
	require_NotNil(t, raftNode)

	// Build a consumer assignment with stale state, as would happen during
	// a stream restore where the consumer info returned has older state.
	rsjs := rs.getJetStream()
	rsjs.mu.RLock()
	ca := rsjs.consumerAssignment(globalAccountName, "TEST", "CONSUMER")
	rsjs.mu.RUnlock()
	require_NotNil(t, ca)

	staleState := &ConsumerState{
		Delivered: SequencePair{Consumer: 5, Stream: 5},
		AckFloor:  SequencePair{Consumer: 5, Stream: 5},
	}

	// Check that consumer is running and healthy.
	current := mset.lookupConsumer("CONSUMER")
	if current == nil {
		t.Errorf("Expected consumer to be present on restart")
	}
	cinfo := current.info()
	require_Equal(t, cinfo.AckFloor.Consumer, 10)
	require_Equal(t, cinfo.AckFloor.Stream, 10)

	// Call processClusterCreateConsumer with the stale state, simulating
	// what happens during stream restore or meta snapshot recovery.
	// Without the fix, this would delete the consumer's Raft node and stop it.
	rsjs.processClusterCreateConsumer(ca, ca, staleState, true)

	// Verify the consumer still exists on this server.
	o = mset.lookupConsumer("CONSUMER")
	if o == nil {
		t.Fatal("Expected consumer to still be present after consumer create with stale data")
	}

	// Verify the consumer's Raft node was NOT deleted.
	o.mu.RLock()
	nodeAfter := o.node
	o.mu.RUnlock()
	require_NotNil(t, nodeAfter)
	require_Equal(t, raftNode, nodeAfter)

	// Verify the consumer retained its correct (newer) state, not the stale state.
	info := o.info()
	require_Equal(t, info.AckFloor.Consumer, 10)
	require_Equal(t, info.AckFloor.Stream, 10)

	// Verify the consumer is still fully functional by publishing and consuming more.
	for i := 0; i < 5; i++ {
		_, err = js.Publish("foo", []byte("after-stale"))
		require_NoError(t, err)
	}
	msgs, err = sub.Fetch(5)
	require_NoError(t, err)
	require_Len(t, len(msgs), 5)
	for _, m := range msgs {
		err = m.AckSync()
		require_NoError(t, err)
	}
}

func TestJetStreamClusterConsumerSetStoreStateOldUpdateRestart(t *testing.T) {
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
		Replicas:  3,
		AckPolicy: nats.AckExplicitPolicy,
	})
	require_NoError(t, err)

	// Publish messages and ack them to advance the consumer state.
	for i := 0; i < 10; i++ {
		_, err = js.Publish("foo", []byte("msg"))
		require_NoError(t, err)
	}
	sub, err := js.PullSubscribe("foo", "CONSUMER")
	require_NoError(t, err)
	msgs, err := sub.Fetch(10)
	require_NoError(t, err)
	require_Len(t, len(msgs), 10)
	for _, m := range msgs {
		err = m.AckSync()
		require_NoError(t, err)
	}

	// Wait for all consumers on all servers to be caught up to ack floor 10.
	checkFor(t, 5*time.Second, 200*time.Millisecond, func() error {
		for _, s := range c.servers {
			a, err := s.lookupAccount(globalAccountName)
			if err != nil {
				return err
			}
			ms, err := a.lookupStream("TEST")
			if err != nil {
				return err
			}
			co := ms.lookupConsumer("CONSUMER")
			if co == nil {
				return errors.New("consumer not found")
			}
			info := co.info()
			if info.AckFloor.Consumer != 10 {
				return fmt.Errorf("expected ack floor 10, got %d", info.AckFloor.Consumer)
			}
		}
		return nil
	})

	// Pick a server that is not the meta leader. We will shut it down and
	// then propose a stale consumer assignment entry to meta while it is
	// down. When it restarts and catches up on the meta Raft, it will
	// replay the entry with the stale State.
	ml := c.leader()
	require_NotNil(t, ml)
	var rs *Server
	for _, s := range c.servers {
		if s != ml {
			rs = s
			break
		}
	}
	require_NotNil(t, rs)

	rs.Shutdown()
	rs.WaitForShutdown()

	// Reconnect to a remaining server.
	nc.Close()
	nc, js = jsClientConnect(t, ml)
	defer nc.Close()

	// Propose a consumer assignment with stale State to the meta Raft.
	// This simulates what happens during a stream scale-down reassignment
	// where the consumer info returned has older state than what the
	// consumer already has on disk.
	mljs := ml.getJetStream()
	mljs.mu.RLock()
	cc := mljs.cluster
	ca := mljs.consumerAssignment(globalAccountName, "TEST", "CONSUMER")
	require_NotNil(t, ca)

	cca := ca.copyGroup()
	cca.State = &ConsumerState{
		Delivered: SequencePair{Consumer: 5, Stream: 5},
		AckFloor:  SequencePair{Consumer: 5, Stream: 5},
	}
	consumerAdd := encodeAddConsumerAssignment(cca)
	mljs.mu.RUnlock()
	require_NoError(t, cc.meta.Propose(cc.meta.Term(), consumerAdd))

	// Wait for the meta leader to apply the entry.
	time.Sleep(500 * time.Millisecond)

	// Restart the server. During meta Raft catchup, it will replay the
	// consumer assignment entry that has State set to ack floor 5, but the
	// consumer's store on disk already has ack floor 10. Without the fix,
	// setStoreState returns ErrStoreOldUpdate which triggers
	// rg.node.Delete() and o.stop(), destroying the consumer.
	rs = c.restartServer(rs)
	c.waitOnServerCurrent(rs)

	// Verify the consumer still exists on the restarted server and has
	// the correct (newer) state, not the stale state.
	checkFor(t, 5*time.Second, 200*time.Millisecond, func() error {
		a, err := rs.lookupAccount(globalAccountName)
		if err != nil {
			return err
		}
		ms, err := a.lookupStream("TEST")
		if err != nil {
			return err
		}
		co := ms.lookupConsumer("CONSUMER")
		if co == nil {
			return errors.New("consumer not found after restart - Raft node was likely deleted")
		}
		info := co.info()
		if info.AckFloor.Consumer != 10 {
			return fmt.Errorf("expected ack floor 10, got %d", info.AckFloor.Consumer)
		}
		return nil
	})

	// Verify the consumer is still fully functional by publishing and consuming more.
	for i := 0; i < 5; i++ {
		_, err = js.Publish("foo", []byte("after-restart"))
		require_NoError(t, err)
	}
	sub, err = js.PullSubscribe("foo", "CONSUMER")
	require_NoError(t, err)
	msgs, err = sub.Fetch(5)
	require_NoError(t, err)
	require_Len(t, len(msgs), 5)
	for _, m := range msgs {
		err = m.AckSync()
		require_NoError(t, err)
	}
}

func TestJetStreamClusterSourcingIntoDiscardNewPerSubject(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "R3S", 3)
	defer c.shutdown()

	nc, js := jsClientConnect(t, c.randomServer())
	defer nc.Close()

	_, err := js.AddStream(&nats.StreamConfig{
		Name:       "A",
		Subjects:   []string{"foo.*"},
		Duplicates: 100 * time.Millisecond,
		Replicas:   3,
	})
	require_NoError(t, err)

	sConfig := StreamConfig{
		Name:          "B",
		Storage:       MemoryStorage,
		Sources:       []*StreamSource{{Name: "A"}},
		MaxMsgsPer:    1,
		Discard:       DiscardNew,
		DiscardNewPer: true,
		Duplicates:    10 * time.Second,
		Replicas:      3,
	}

	req, err := json.Marshal(&sConfig)
	require_NoError(t, err)

	r, err := nc.Request("$JS.API.STREAM.CREATE.B", req, 5*time.Second)
	require_NoError(t, err)

	var resp JSApiStreamCreateResponse
	err = json.Unmarshal(r.Data, &resp)
	require_NoError(t, err)
	require_Equal(t, resp.Error, nil)

	_, err = js.Publish("foo.1", []byte("1"))
	require_NoError(t, err)

	// This second publish to the same subject should not be sourced
	// due to discard new per subject with MaxMsgsPer=1.
	_, err = js.Publish("foo.1", []byte("2"))
	require_NoError(t, err)

	checkFor(t, 4*time.Second, 200*time.Millisecond, func() error {
		si, err := js.StreamInfo("B")
		if err != nil {
			return err
		}
		if si.State.Msgs != 1 {
			return fmt.Errorf("expected 1 message, got %d", si.State.Msgs)
		}
		return nil
	})

	// Check the message content.
	msgp, err := js.GetMsg("B", uint64(1))
	require_NoError(t, err)
	require_Equal(t, msgp.Subject, "foo.1")
	require_Equal(t, string(msgp.Data), "1")

	// Purge stream B so sourcing can continue past the per-subject limit.
	require_NoError(t, js.PurgeStream("B"))

	checkFor(t, 4*time.Second, 200*time.Millisecond, func() error {
		si, err := js.StreamInfo("B")
		if err != nil {
			return err
		}
		if si.State.Msgs != 1 {
			return fmt.Errorf("expected 1 message, got %d", si.State.Msgs)
		}
		return nil
	})

	// After purge, the second message should now be sourced.
	msgp, err = js.GetMsg("B", uint64(2))
	require_NoError(t, err)
	require_Equal(t, msgp.Subject, "foo.1")
	require_Equal(t, string(msgp.Data), "2")

	// Test duplicate message ID handling: publish with explicit Nats-Msg-Id.
	msg := nats.NewMsg("foo.2")
	msg.Data = []byte("1")
	msg.Header.Set("Nats-Msg-Id", "dup1")

	_, err = js.PublishMsg(msg)
	require_NoError(t, err)

	// Must be able to source the message after the duplicate.
	checkFor(t, 4*time.Second, 200*time.Millisecond, func() error {
		si, err := js.StreamInfo("B")
		if err != nil {
			return err
		}
		if si.State.Msgs != 2 {
			return fmt.Errorf("expected 2 messages, got %d", si.State.Msgs)
		}
		return nil
	})

	msgp, err = js.GetMsg("B", uint64(3))
	require_NoError(t, err)
	require_Equal(t, msgp.Subject, "foo.2")
	require_Equal(t, string(msgp.Data), "1")

	// Wait for the dedup window on stream A (100ms) to expire.
	time.Sleep(200 * time.Millisecond)

	// Publish a message with the same Nats-Msg-Id but to a different subject.
	// Stream A's dedup window has expired, so A will accept it.
	// Stream B has a 10s dedup window, so it should detect this as a duplicate and skip it.
	msg = nats.NewMsg("foo.3")
	msg.Data = []byte("1")
	msg.Header.Set("Nats-Msg-Id", "dup1")

	_, err = js.PublishMsg(msg)
	require_NoError(t, err)

	// The duplicate should be skipped by B; message count should stay at 2.
	checkFor(t, 4*time.Second, 200*time.Millisecond, func() error {
		si, err := js.StreamInfo("B")
		if err != nil {
			return err
		}
		if si.State.Msgs != 2 {
			return fmt.Errorf("expected 2 messages, got %d", si.State.Msgs)
		}
		return nil
	})

	// Sourcing must continue processing after the skipped duplicate.
	_, err = js.Publish("foo.4", []byte("1"))
	require_NoError(t, err)

	checkFor(t, 4*time.Second, 200*time.Millisecond, func() error {
		si, err := js.StreamInfo("B")
		if err != nil {
			return err
		}
		if si.State.Msgs != 3 {
			return fmt.Errorf("expected 3 messages, got %d", si.State.Msgs)
		}
		return nil
	})

	msgp, err = js.GetMsg("B", uint64(4))
	require_NoError(t, err)
	require_Equal(t, msgp.Subject, "foo.4")
	require_Equal(t, string(msgp.Data), "1")
}

func TestJetStreamClusterSourceRetryDoesNotDuplicate(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "R3S", 3)
	defer c.shutdown()

	nc, js := jsClientConnect(t, c.randomServer())
	defer nc.Close()

	_, err := js.AddStream(&nats.StreamConfig{
		Name:     "SRC",
		Subjects: []string{"src"},
		Replicas: 3,
	})
	require_NoError(t, err)

	for range 5 {
		_, err = js.Publish("src", nil)
		require_NoError(t, err)
	}

	_, err = js.AddStream(&nats.StreamConfig{
		Name:     "DST",
		Sources:  []*nats.StreamSource{{Name: "SRC"}},
		Replicas: 3,
	})
	require_NoError(t, err)

	// Wait until DST has fully sourced SRC.
	checkFor(t, 2*time.Second, 200*time.Millisecond, func() error {
		si, err := js.StreamInfo("DST")
		if err != nil {
			return err
		}
		if si.State.Msgs != 5 {
			return fmt.Errorf("DST has %d msgs, want %d", si.State.Msgs, 5)
		}
		return nil
	})

	sl := c.streamLeader(globalAccountName, "DST")
	require_NotNil(t, sl)
	mset, err := sl.globalAccount().lookupStream("DST")
	require_NoError(t, err)

	// Simulate a leaf node reconnect: close the internal client backing the source
	// subscription so shouldRetry observes a closed client, cancels the current
	// sourceInfo, and schedules a fresh setupSourceConsumer.
	var srcClients []*client
	mset.mu.Lock()
	for _, si := range mset.sources {
		if si.sub != nil && si.sub.client != nil {
			srcClients = append(srcClients, si.sub.client)
		}
	}
	mset.mu.Unlock()
	for _, c := range srcClients {
		c.closeConnection(ClientClosed)
	}
	mset.retryDisconnectedSyncConsumers()

	// Wait for the new source consumer to be fully re-established, i.e.
	// si.sub is set again and si.dseq has been reset to 0 by the setup
	// callback. si.sseq is preserved across the cancel/retry.
	var si *sourceInfo
	checkFor(t, 10*time.Second, 50*time.Millisecond, func() error {
		mset.mu.RLock()
		defer mset.mu.RUnlock()
		for _, s := range mset.sources {
			si = s
			break
		}
		if si == nil {
			return fmt.Errorf("no source info")
		}
		if si.sub == nil || si.cname == _EMPTY_ {
			return fmt.Errorf("source consumer not yet re-established (sub=%v cname=%q)", si.sub != nil, si.cname)
		}
		return nil
	})

	mset.mu.RLock()
	origSseq, dseq, cname := si.sseq, si.dseq, si.cname
	mset.mu.RUnlock()
	require_Equal(t, origSseq, 5)
	require_Equal(t, dseq, 0)

	// Synthesize a delivery from the (just recreated) consumer with a
	// source sequence that is ALREADY stored in DST (sseq=3 <= origSseq=5)
	// and dseq=1, matching si.dseq+1 for the freshly reset consumer.
	// Reply layout: $JS.ACK.<stream>.<consumer>.<dc>.<sseq>.<dseq>.<ts>.<pending>
	replaySseq := uint64(3)
	rply := fmt.Sprintf("$JS.ACK.SRC.%s.1.%d.1.%d.0", cname, replaySseq, time.Now().UnixNano())
	mset.processInboundSourceMsg(si, &inMsg{subj: "src", rply: rply})

	// Allow any proposal through the raft pipeline.
	time.Sleep(500 * time.Millisecond)

	// DST must still have exactly N messages. Verify the source-sequence
	// being replayed is stored exactly once.
	si2, err := js.StreamInfo("DST")
	require_NoError(t, err)
	require_Equal(t, si2.State.Msgs, 5)

	state := mset.state()
	var dstSeqs []uint64
	var smv StoreMsg
	for seq := state.FirstSeq; seq <= state.LastSeq; seq++ {
		sm, err := mset.store.LoadMsg(seq, &smv)
		if err != nil {
			continue
		}
		ss := getHeader(JSStreamSource, sm.hdr)
		if len(ss) == 0 {
			continue
		}
		_, _, sseq, _ := streamAndSeq(string(ss))
		if sseq == replaySseq {
			dstSeqs = append(dstSeqs, seq)
		}
	}
	if len(dstSeqs) != 1 {
		t.Fatalf("SRC sseq=%d stored %d times in DST at seqs %v (want 1; original si.sseq=%d)",
			replaySseq, len(dstSeqs), dstSeqs, origSseq)
	}
}

func TestJetStreamClusterSourceRetryDoesNotResetHealthy(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "R3S", 3)
	defer c.shutdown()

	nc, js := jsClientConnect(t, c.randomServer())
	defer nc.Close()

	_, err := js.AddStream(&nats.StreamConfig{
		Name:     "SRC",
		Subjects: []string{"src"},
		Replicas: 3,
	})
	require_NoError(t, err)
	for range 5 {
		_, err = js.Publish("src", nil)
		require_NoError(t, err)
	}

	_, err = js.AddStream(&nats.StreamConfig{
		Name:     "DST",
		Sources:  []*nats.StreamSource{{Name: "SRC"}},
		Replicas: 3,
	})
	require_NoError(t, err)

	checkFor(t, 2*time.Second, 200*time.Millisecond, func() error {
		si, err := js.StreamInfo("DST")
		if err != nil {
			return err
		}
		if si.State.Msgs != 5 {
			return fmt.Errorf("DST has %d msgs, want %d", si.State.Msgs, 5)
		}
		return nil
	})

	sl := c.streamLeader(globalAccountName, "DST")
	require_NotNil(t, sl)
	mset, err := sl.globalAccount().lookupStream("DST")
	require_NoError(t, err)

	// Capture the original sub/client/cname for the healthy source.
	mset.mu.RLock()
	var origSub *subscription
	var origCname string
	for _, si := range mset.sources {
		origSub, origCname = si.sub, si.cname
		break
	}
	mset.mu.RUnlock()
	require_NotNil(t, origSub)
	require_True(t, origCname != _EMPTY_)

	// Healthy state: si.sub is set, client is not closed, heartbeats are
	// keeping si.last fresh. shouldRetry must leave the consumer alone.
	mset.retryDisconnectedSyncConsumers()

	mset.mu.RLock()
	var sub *subscription
	var cname string
	for _, si := range mset.sources {
		sub, cname = si.sub, si.cname
		break
	}
	mset.mu.RUnlock()
	require_Equal(t, sub, origSub)
	require_Equal(t, cname, origCname)
}

func TestJetStreamClusterSourceStartingSeqIgnoresInflight(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "R3S", 3)
	defer c.shutdown()

	nc, js := jsClientConnect(t, c.randomServer())
	defer nc.Close()

	_, err := js.AddStream(&nats.StreamConfig{
		Name:     "SRC",
		Subjects: []string{"src"},
		Replicas: 3,
	})
	require_NoError(t, err)

	stored := uint64(5)
	for range stored {
		_, err = js.Publish("src", nil)
		require_NoError(t, err)
	}

	_, err = js.AddStream(&nats.StreamConfig{
		Name:     "DST",
		Sources:  []*nats.StreamSource{{Name: "SRC"}},
		Replicas: 3,
	})
	require_NoError(t, err)

	// Wait until DST has applied all storedN sourced messages.
	checkFor(t, 2*time.Second, 200*time.Millisecond, func() error {
		si, err := js.StreamInfo("DST")
		if err != nil {
			return err
		}
		if si.State.Msgs != stored {
			return fmt.Errorf("DST has %d msgs, want %d", si.State.Msgs, stored)
		}
		return nil
	})

	sl := c.streamLeader(globalAccountName, "DST")
	require_NotNil(t, sl)
	mset, err := sl.globalAccount().lookupStream("DST")
	require_NoError(t, err)

	// Simulate N inflight proposals that are reflected in the source tracking state,
	// but are not applied to the store yet. Also, seal the stream so subsequent
	// proposals will fail.
	inflight := uint64(4)
	var si *sourceInfo
	mset.mu.Lock()
	for _, s := range mset.sources {
		si = s
		break
	}
	require_NotNil(t, si)
	require_Equal(t, si.sseq, stored)
	// Bump both the inflight sequence.
	si.sseq, si.dseq = stored+inflight, stored+inflight
	cname := si.cname
	mset.cfg.Sealed = true
	mset.mu.Unlock()

	// Simulate the next delivery, which should fail due to the sealed stream.
	// The sourcing consumer should reset, but not roll back in sequence.
	nextSeq := stored + inflight + 1
	rply := fmt.Sprintf("$JS.ACK.SRC.%s.1.%d.%d.%d.0", cname, nextSeq, nextSeq, time.Now().UnixNano())
	mset.processInboundSourceMsg(si, &inMsg{subj: "src", rply: rply})

	mset.mu.RLock()
	got := si.sseq
	mset.mu.RUnlock()
	if got != stored+inflight {
		t.Fatalf("si.sseq was rewound to %d after sealed proposal error; want %d "+
			"(storedN=%d, inflightK=%d). A retry at si.sseq+1=%d would re-source "+
			"%d in-flight messages, duplicating them once apply catches up.",
			got, stored+inflight, stored, inflight, got+1, inflight)
	}
}

func TestJetStreamClusterWorkQueueConsumerCreateRejectionNoOrphan(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "R3S", 3)
	defer c.shutdown()

	nc, js := jsClientConnect(t, c.randomServer())
	defer nc.Close()

	_, err := js.AddStream(&nats.StreamConfig{
		Name:      "WQ",
		Subjects:  []string{"wq.>"},
		Replicas:  3,
		Retention: nats.WorkQueuePolicy,
	})
	require_NoError(t, err)

	_, err = js.AddConsumer("WQ", &nats.ConsumerConfig{
		Name:          "CONSUMER",
		AckPolicy:     nats.AckExplicitPolicy,
		DeliverPolicy: nats.DeliverByStartSequencePolicy,
		OptStartSeq:   1,
	})
	require_Error(t, err, NewJSConsumerWQConsumerNotDeliverAllError())

	for _, s := range c.servers {
		sjs := s.getJetStream()
		sjs.mu.RLock()
		ca := sjs.consumerAssignment(globalAccountName, "WQ", "CONSUMER")
		sjs.mu.RUnlock()
		if ca != nil {
			t.Fatalf("orphan assignment present on %s", s.Name())
		}
	}
}

func TestJetStreamClusterDecodeStreamMsgRejectsMalformed(t *testing.T) {
	// The field lengths are deliberately large enough that every field
	// boundary lands past the 26-byte minimum.
	const (
		subjLen  = 20
		replyLen = 10
		hdrLen   = 10
		msgLen   = 10
	)
	subject := strings.Repeat("s", subjLen)
	reply := strings.Repeat("r", replyLen)
	hdr := []byte(strings.Repeat("h", hdrLen))
	body := []byte(strings.Repeat("b", msgLen))
	valid := encodeStreamMsg(subject, reply, hdr, body, 12345, 67890, true)[1:]

	// Sanity check that the valid buffer round-trips.
	gotSubj, gotReply, gotHdr, gotMsg, lseq, ts, sourced, err := decodeStreamMsg(valid)
	require_NoError(t, err)
	require_Equal(t, gotSubj, subject)
	require_Equal(t, gotReply, reply)
	require_Equal(t, string(gotHdr), string(hdr))
	require_Equal(t, string(gotMsg), string(body))
	require_Equal(t, lseq, 12345)
	require_Equal(t, ts, 67890)
	require_True(t, sourced)

	// Field boundary offsets within valid. The decoder reads, in order:
	// lseq(8) + ts(8) + subjLen(2) + subject + replyLen(2) + reply +
	// hdrLen(2) + hdr + msgLen(4) + msg + flags(uvarint).
	const (
		fixedEnd    = 8 + 8        // after lseq + ts
		subjLenEnd  = fixedEnd + 2 // after subject length prefix
		subjEnd     = subjLenEnd + subjLen
		replyLenEnd = subjEnd + 2 // after reply length prefix
		replyEnd    = replyLenEnd + replyLen
		hdrLenEnd   = replyEnd + 2 // after hdr length prefix
		hdrEnd      = hdrLenEnd + hdrLen
		msgLenEnd   = hdrEnd + 4 // after msg length prefix
		msgEnd      = msgLenEnd + msgLen
	)

	// Dropping the trailing optional flags still decodes successfully.
	_, _, _, _, _, _, sourced, err = decodeStreamMsg(valid[:msgEnd])
	require_NoError(t, err)
	require_False(t, sourced) // No flags means not sourced.

	for _, test := range []struct {
		title string
		buf   []byte
	}{
		{title: "Empty", buf: nil},
		// Smaller than the fixed-size prefix (lseq+ts+subjLen+...), trips the initial length guard.
		{title: "TooShort", buf: valid[:25]},
		// Past the guard, but the subject claims more bytes than remain.
		{title: "TruncatedSubj", buf: valid[:subjEnd-1]},
		// Subject present, but the reply length prefix is missing.
		{title: "TruncatedReplyLen", buf: valid[:subjEnd]},
		// Reply claims more bytes than remain.
		{title: "TruncatedReply", buf: valid[:replyEnd-1]},
		// Subject+reply present, but the header length prefix is missing.
		{title: "TruncatedHdrLen", buf: valid[:replyEnd]},
		// Header claims more bytes than remain.
		{title: "TruncatedHdr", buf: valid[:hdrEnd-1]},
		// Subject+reply+hdr present, but the msg length prefix (4 bytes) is missing.
		{title: "TruncatedMsgLen", buf: valid[:hdrEnd]},
		// Msg claims more bytes than remain.
		{title: "TruncatedMsg", buf: valid[:msgEnd-1]},
	} {
		t.Run(test.title, func(t *testing.T) {
			_, _, _, _, _, _, _, err = decodeStreamMsg(test.buf)
			require_Error(t, err, errBadStreamMsg)
		})
	}
}

func TestJetStreamClusterDecodeBatchMsgRejectsMalformed(t *testing.T) {
	// Build a valid encoded batch msg payload (the buffer passed to
	// decodeBatchMsg is everything after the leading batch op byte).
	full := encodeStreamMsgAllowCompressAndBatch("foo", _EMPTY_, nil, []byte("body"), 1, 2, false, "batch", 7, false)
	valid := full[1:]

	// Sanity check that the valid buffer round-trips.
	batchId, batchSeq, op, mbuf, err := decodeBatchMsg(valid)
	require_NoError(t, err)
	require_Equal(t, batchId, "batch")
	require_Equal(t, batchSeq, 7)
	require_Equal(t, op, streamMsgOp)
	require_True(t, len(mbuf) > 0)

	// The mbuf left over from a valid batch msg must itself decode as a valid stream msg.
	subject, reply, hdr, msg, lseq, ts, sourced, err := decodeStreamMsg(mbuf)
	require_NoError(t, err)
	require_Equal(t, subject, "foo")
	require_Equal(t, reply, _EMPTY_)
	require_Equal(t, len(hdr), 0)
	require_Equal(t, string(msg), "body")
	require_Equal(t, lseq, 1)
	require_Equal(t, ts, 2)
	require_False(t, sourced)

	// Field boundary offsets within valid. The decoder reads, in order:
	// batchIdLen(2) + batchId("batch") + batchSeq(uvarint, 1 byte for 7) + op(1).
	const (
		batchIdLenEnd = 2                            // after batchId length prefix
		batchIdEnd    = batchIdLenEnd + len("batch") // after batchId bytes
		batchSeqEnd   = batchIdEnd + 1               // after the single-byte seq varint
	)

	for _, test := range []struct {
		title string
		buf   []byte
	}{
		{title: "Empty", buf: nil},
		// Smaller than the 2-byte batchId length prefix.
		{title: "TooShort", buf: valid[:1]},
		// Claims a batchId longer than the remaining bytes.
		{title: "TruncatedBatchId", buf: valid[:batchIdEnd-1]},
		// batchId present, but the batchSeq varint is missing.
		{title: "TruncatedSeq", buf: valid[:batchIdEnd]},
		// batchId+batchSeq present, but the op byte is missing.
		// This previously caused an index-out-of-range panic.
		{title: "TruncatedOp", buf: valid[:batchSeqEnd]},
	} {
		t.Run(test.title, func(t *testing.T) {
			_, _, _, _, err := decodeBatchMsg(test.buf)
			require_Error(t, err, errBadStreamMsg)
		})
	}
}

func TestJetStreamClusterCatchupBadMsgStopsRetrying(t *testing.T) {
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

	// Respond to the catchup with a corrupt compressed message.
	var count atomic.Int32
	sub, err := sysNc.Subscribe(syncSubj, func(msg *nats.Msg) {
		count.Add(1)
		// {0x80} is an incomplete varint and fails s2.Decode with corrupt input.
		msg.Respond(append([]byte{byte(compressedStreamMsgOp)}, 0x80))
	})
	require_NoError(t, err)
	defer sub.Drain()
	require_NoError(t, sysNc.Flush()) // Must flush, otherwise our subscription could be too late.

	err = mset.processSnapshot(&StreamReplicatedState{FirstSeq: 1, LastSeq: 1}, 1)
	require_Error(t, err, errCatchupBadMsg)

	// We must not have retried the catchup, a bad message bails out immediately.
	require_Equal(t, count.Load(), 1)
}

func TestJetStreamClusterDecodeUpdatesRejectMalformed(t *testing.T) {
	// binary.Uvarint/Varint return n == 0 when the buffer is too short to
	// hold a full varint, so a truncated replicated update must be rejected
	// rather than silently decoded as zero-valued fields.
	t.Run("AckUpdate", func(t *testing.T) {
		valid := binary.AppendUvarint(nil, 10)
		valid = binary.AppendUvarint(valid, 20)
		if _, _, err := decodeAckUpdate(valid); err != nil {
			t.Fatalf("expected valid ack update to decode, got %v", err)
		}
		for _, buf := range [][]byte{nil, valid[:1]} {
			if _, _, err := decodeAckUpdate(buf); err != errBadAckUpdate {
				t.Fatalf("expected errBadAckUpdate for %v, got %v", buf, err)
			}
		}
	})
	t.Run("DeliveredUpdate", func(t *testing.T) {
		valid := binary.AppendUvarint(nil, 10)
		valid = binary.AppendUvarint(valid, 20)
		valid = binary.AppendUvarint(valid, 1)
		valid = binary.AppendVarint(valid, time.Now().UnixNano())
		if _, _, _, _, err := decodeDeliveredUpdate(valid); err != nil {
			t.Fatalf("expected valid delivered update to decode, got %v", err)
		}
		for _, buf := range [][]byte{nil, valid[:1], valid[:2], valid[:3]} {
			if _, _, _, _, err := decodeDeliveredUpdate(buf); err != errBadDeliveredUpdate {
				t.Fatalf("expected errBadDeliveredUpdate for %v, got %v", buf, err)
			}
		}
	})
	// updateSkipOp and resetSeqOp read a fixed-width little-endian uint64 for
	// the sequence, so a buffer shorter than 8 bytes must be rejected rather
	// than triggering an out-of-bounds slice access.
	t.Run("SkipUpdate", func(t *testing.T) {
		valid := binary.LittleEndian.AppendUint64(nil, 10)
		if sseq, err := decodeSkipUpdate(valid); err != nil || sseq != 10 {
			t.Fatalf("expected valid skip update to decode, got sseq=%d err=%v", sseq, err)
		}
		for _, buf := range [][]byte{nil, valid[:1], valid[:7]} {
			if _, err := decodeSkipUpdate(buf); err != errBadSkipUpdate {
				t.Fatalf("expected errBadSkipUpdate for %v, got %v", buf, err)
			}
		}
	})
	t.Run("ResetUpdate", func(t *testing.T) {
		valid := binary.LittleEndian.AppendUint64(nil, 10)
		valid = append(valid, "reply"...)
		if sseq, reply, err := decodeResetUpdate(valid); err != nil || sseq != 10 || reply != "reply" {
			t.Fatalf("expected valid reset update to decode, got sseq=%d reply=%q err=%v", sseq, reply, err)
		}
		for _, buf := range [][]byte{nil, valid[:1], valid[:7]} {
			if _, _, err := decodeResetUpdate(buf); err != errBadResetUpdate {
				t.Fatalf("expected errBadResetUpdate for %v, got %v", buf, err)
			}
		}
	})
}

func TestJetStreamClusterDecodeStreamMsgRejectOversizedLength(t *testing.T) {
	// A valid replicated stream msg still round trips.
	esm := encodeStreamMsg("foo", _EMPTY_, nil, []byte("hello"), 1, 0, false)
	subject, _, _, msg, lseq, _, _, err := decodeStreamMsg(esm[1:])
	require_NoError(t, err)
	require_Equal(t, subject, "foo")
	require_Equal(t, lseq, 1)
	require_Equal(t, string(msg), "hello")

	// The message length is read as a uint32 into an int. On 32-bit builds a
	// value with the high bit set turns negative and slips past the
	// len(buf) < ml check, so the following buf[:ml] panics. Such a length must
	// be rejected on every architecture rather than decoded. This mirrors what a
	// cluster peer could replicate on the applyStreamMsgOp/catchup path.
	le := binary.LittleEndian
	var buf []byte
	buf = le.AppendUint64(buf, 1) // lseq
	buf = le.AppendUint64(buf, 0) // ts
	buf = le.AppendUint16(buf, 3) // subject length
	buf = append(buf, "foo"...)
	buf = le.AppendUint16(buf, 0)          // reply length
	buf = le.AppendUint16(buf, 0)          // header length
	buf = le.AppendUint32(buf, 0xFFFFFFFF) // message length, negative as a 32-bit int
	if _, _, _, _, _, _, _, err := decodeStreamMsg(buf); err != errBadStreamMsg {
		t.Fatalf("expected errBadStreamMsg for oversized message length, got %v", err)
	}
}

func TestJetStreamClusterApplyEntriesRejectEmptyNormal(t *testing.T) {
	// The append entry wire format only requires each entry to carry its type
	// byte, so a cluster peer can replicate an EntryNormal with a zero-length
	// body and it still decodes cleanly.
	le := binary.LittleEndian
	raw := make([]byte, appendEntryBaseLen)
	le.PutUint16(raw[40:], 1)            // one entry
	raw = le.AppendUint32(raw, 1)        // entry length is the type byte only
	raw = append(raw, byte(EntryNormal)) // no data follows
	ae, err := decodeAppendEntry(raw, nil, _EMPTY_)
	require_NoError(t, err)
	require_Len(t, len(ae.entries), 1)
	require_Equal(t, ae.entries[0].Type, EntryNormal)
	require_Len(t, len(ae.entries[0].Data), 0)

	// The apply loops read the op from data[0]; an empty EntryNormal must be
	// rejected rather than triggering an out-of-bounds access.
	empty := []*Entry{{EntryNormal, nil}}
	js := &jetStream{}
	if _, _, err := js.applyMetaEntries(empty, nil); err != errBadEntryOp {
		t.Fatalf("expected errBadEntryOp from applyMetaEntries, got %v", err)
	}
	ce := &CommittedEntry{Entries: empty}
	if _, err := js.applyStreamEntries(&stream{}, nil, ce, false, nil); err != errBadEntryOp {
		t.Fatalf("expected errBadEntryOp from applyStreamEntries, got %v", err)
	}
	if err := js.applyConsumerEntries(&consumer{}, ce, false); err != errBadEntryOp {
		t.Fatalf("expected errBadEntryOp from applyConsumerEntries, got %v", err)
	}
}

func TestJetStreamClusterCreateGroupForConsumerNoPeers(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "R3S", 3)
	defer c.shutdown()

	nc, js := jsClientConnect(t, c.randomServer())
	defer nc.Close()

	_, err := js.AddStream(&nats.StreamConfig{Name: "TEST", Subjects: []string{"foo"}, Replicas: 3})
	require_NoError(t, err)
	c.waitOnStreamLeader(globalAccountName, "TEST")

	sjs, cc := c.leader().getJetStreamCluster()
	sjs.mu.Lock()
	defer sjs.mu.Unlock()

	sa := cc.streams[globalAccountName]["TEST"]
	require_True(t, sa != nil)

	// Asking for more replicas than the parent stream has peers can't be placed.
	rg, cerr := cc.createGroupForConsumer(&ConsumerConfig{Replicas: len(sa.Group.Peers) + 1}, sa)
	require_True(t, rg == nil)
	require_True(t, cerr != nil)

	// A stream group with no peers at all can't be placed either.
	empty := &streamAssignment{Config: sa.Config, Group: &raftGroup{Name: "G"}}
	rg, cerr = cc.createGroupForConsumer(&ConsumerConfig{Replicas: 1}, empty)
	require_True(t, rg == nil)
	require_True(t, cerr != nil)

	// A placeable consumer still gets a group, with no error.
	rg, cerr = cc.createGroupForConsumer(&ConsumerConfig{Replicas: 3}, sa)
	require_True(t, cerr == nil)
	require_True(t, rg != nil)
	require_Len(t, len(rg.Peers), 3)
}

func TestJetStreamClusterStreamSnapshots(t *testing.T) {
	workload := func(t *testing.T, setup func(t *testing.T, nc *nats.Conn, js nats.JetStreamContext)) {
		c := createJetStreamClusterExplicit(t, "R3C", 3)
		defer c.shutdown()

		nc, js := jsClientConnect(t, c.servers[0])
		defer nc.Close()

		setup(t, nc, js)
		cfg, state, archive := performStreamBackup(t, nc, "test_stream")

		osi, err := js.StreamInfo("test_stream")
		require_NoError(t, err)

		require_NoError(t, js.DeleteStream("test_stream"))
		require_True(t, performStreamRestore(t, nc, cfg, state, archive))

		c.waitOnAllCurrent()
		c.waitOnStreamLeader(globalAccountName, "test_stream")

		nsi, err := js.StreamInfo("test_stream")
		require_NoError(t, err)

		// Nothing should have been mangled in the round-trip. We are
		// specifically ignoring FirstTime and LastTime here though, as
		// the compact call on the restore path can influence that in
		// certain cases.
		require_True(t, reflect.DeepEqual(osi.Config, nsi.Config))
		require_Equal(t, osi.State.FirstSeq, nsi.State.FirstSeq)
		require_Equal(t, osi.State.LastSeq, nsi.State.LastSeq)
		require_Equal(t, osi.State.Msgs, nsi.State.Msgs)
		require_Equal(t, osi.State.Bytes, nsi.State.Bytes)
		require_Equal(t, osi.State.NumSubjects, nsi.State.NumSubjects)
		require_Equal(t, osi.State.NumDeleted, nsi.State.NumDeleted)
		require_Equal(t, osi.State.Consumers, nsi.State.Consumers)
	}

	for storename, storetype := range map[string]StorageType{
		"Memory": MemoryStorage,
		"File":   FileStorage,
	} {
		t.Run(storename, func(t *testing.T) {
			for name, setup := range map[string]func(t *testing.T, nc *nats.Conn, js nats.JetStreamContext){
				"LimitsNoConsumers": func(t *testing.T, nc *nats.Conn, js nats.JetStreamContext) {
					jsStreamCreate(t, nc, &StreamConfig{
						Name:     "test_stream",
						Subjects: []string{"foo.>"},
						Storage:  storetype,
						Replicas: 3,
					})
					for range 5 * 10 {
						_, err := js.Publish("foo.bar", nil)
						require_NoError(t, err)
					}
				},
				"LimitsWithR3Consumers": func(t *testing.T, nc *nats.Conn, js nats.JetStreamContext) {
					jsStreamCreate(t, nc, &StreamConfig{
						Name:     "test_stream",
						Subjects: []string{"foo.>"},
						Storage:  storetype,
						Replicas: 3,
					})
					for n := range 5 {
						_, err := js.AddConsumer("test_stream", &nats.ConsumerConfig{
							Name:          fmt.Sprintf("consumer_%d", n),
							AckPolicy:     nats.AckExplicitPolicy,
							MemoryStorage: storetype == MemoryStorage,
							Replicas:      3,
						})
						require_NoError(t, err)
					}
					for range 5 * 10 {
						_, err := js.Publish("foo.bar", nil)
						require_NoError(t, err)
					}
					for n := range 5 {
						cn := fmt.Sprintf("consumer_%d", n)
						sub, err := js.PullSubscribe(_EMPTY_, _EMPTY_, nats.Bind("test_stream", cn))
						require_NoError(t, err)
						for range n * 10 {
							msgs, err := sub.Fetch(1)
							require_NoError(t, err)
							require_Len(t, len(msgs), 1)
							require_NoError(t, msgs[0].AckSync())
						}
					}
				},
				"LimitsWithR1Consumers": func(t *testing.T, nc *nats.Conn, js nats.JetStreamContext) {
					jsStreamCreate(t, nc, &StreamConfig{
						Name:     "test_stream",
						Subjects: []string{"foo.>"},
						Storage:  storetype,
						Replicas: 3,
					})
					for n := range 20 {
						_, err := js.AddConsumer("test_stream", &nats.ConsumerConfig{
							Name:          fmt.Sprintf("consumer_%d", n),
							AckPolicy:     nats.AckExplicitPolicy,
							MemoryStorage: storetype == MemoryStorage,
							Replicas:      1,
						})
						require_NoError(t, err)
					}
					for range 20 * 10 {
						_, err := js.Publish("foo.bar", nil)
						require_NoError(t, err)
					}
					for n := range 20 {
						cn := fmt.Sprintf("consumer_%d", n)
						sub, err := js.PullSubscribe(_EMPTY_, _EMPTY_, nats.Bind("test_stream", cn))
						require_NoError(t, err)
						for range n * 10 {
							msgs, err := sub.Fetch(1)
							require_NoError(t, err)
							require_Len(t, len(msgs), 1)
							require_NoError(t, msgs[0].AckSync())
						}
					}
				},
				"InterestNoConsumers": func(t *testing.T, nc *nats.Conn, js nats.JetStreamContext) {
					jsStreamCreate(t, nc, &StreamConfig{
						Name:      "test_stream",
						Subjects:  []string{"foo.>"},
						Storage:   storetype,
						Retention: InterestPolicy,
						Replicas:  3,
					})
					for range 300 {
						_, err := js.Publish("foo.bar", nil)
						require_NoError(t, err)
					}
					// Technically since there is no interest, this should be empty.
					si, err := js.StreamInfo("test_stream")
					require_NoError(t, err)
					require_Equal(t, si.State.Msgs, 0)
					require_Equal(t, si.State.FirstSeq, 301)
				},
				"InterestWithR3Consumers": func(t *testing.T, nc *nats.Conn, js nats.JetStreamContext) {
					jsStreamCreate(t, nc, &StreamConfig{
						Name:      "test_stream",
						Subjects:  []string{"foo.>"},
						Storage:   storetype,
						Retention: InterestPolicy,
						Replicas:  3,
					})
					for n := range 5 {
						_, err := js.AddConsumer("test_stream", &nats.ConsumerConfig{
							Name:           fmt.Sprintf("consumer_%d", n),
							AckPolicy:      nats.AckExplicitPolicy,
							FilterSubjects: []string{"foo.>"},
							MemoryStorage:  storetype == MemoryStorage,
							Replicas:       3,
						})
						require_NoError(t, err)
					}
					for range 300 {
						_, err := js.Publish("foo.bar", nil)
						require_NoError(t, err)
					}
					for n := range 5 {
						cn := fmt.Sprintf("consumer_%d", n)
						sub, err := js.PullSubscribe(_EMPTY_, _EMPTY_, nats.Bind("test_stream", cn))
						require_NoError(t, err)
						for range (n + 1) * 10 {
							msgs, err := sub.Fetch(1)
							require_NoError(t, err)
							require_Len(t, len(msgs), 1)
							require_NoError(t, msgs[0].AckSync())
						}
					}
					// Consumers have made mixed progress but the first ten messages should be gone.
					si, err := js.StreamInfo("test_stream")
					require_NoError(t, err)
					require_Equal(t, si.State.Msgs, 290)
					require_Equal(t, si.State.FirstSeq, 11)
				},
				"WorkQueueNoConsumers": func(t *testing.T, nc *nats.Conn, js nats.JetStreamContext) {
					jsStreamCreate(t, nc, &StreamConfig{
						Name:      "test_stream",
						Subjects:  []string{"foo.>"},
						Storage:   storetype,
						Retention: WorkQueuePolicy,
						Replicas:  3,
					})
					for range 300 {
						_, err := js.Publish("foo.bar", nil)
						require_NoError(t, err)
					}
					// With no consumers, all messages should remain.
					si, err := js.StreamInfo("test_stream")
					require_NoError(t, err)
					require_Equal(t, si.State.Msgs, 300)
					require_Equal(t, si.State.FirstSeq, 1)
				},
				"WorkQueueWithConsumers": func(t *testing.T, nc *nats.Conn, js nats.JetStreamContext) {
					jsStreamCreate(t, nc, &StreamConfig{
						Name:      "test_stream",
						Subjects:  []string{"foo.0", "foo.1", "foo.2", "foo.3", "foo.4"},
						Storage:   storetype,
						Retention: WorkQueuePolicy,
						Replicas:  3,
					})
					// Each consumer gets a unique subject filter (WorkQueue requires non-overlapping).
					for n := range 5 {
						_, err := js.AddConsumer("test_stream", &nats.ConsumerConfig{
							Name:           fmt.Sprintf("consumer_%d", n),
							AckPolicy:      nats.AckExplicitPolicy,
							FilterSubjects: []string{fmt.Sprintf("foo.%d", n)},
							MemoryStorage:  storetype == MemoryStorage,
							Replicas:       3,
						})
						require_NoError(t, err)
					}
					for n := range 5 {
						for range 60 {
							_, err := js.Publish(fmt.Sprintf("foo.%d", n), nil)
							require_NoError(t, err)
						}
					}
					for n := range 5 {
						cn := fmt.Sprintf("consumer_%d", n)
						sub, err := js.PullSubscribe(_EMPTY_, _EMPTY_, nats.Bind("test_stream", cn))
						require_NoError(t, err)
						for range (n + 1) * 10 {
							msgs, err := sub.Fetch(1)
							require_NoError(t, err)
							require_Len(t, len(msgs), 1)
							require_NoError(t, msgs[0].AckSync())
						}
					}
					// Each consumer acked (n+1)*10 of its 60 messages.
					// Total acked: 10+20+30+40+50 = 150 of 300.
					checkFor(t, 5*time.Second, 100*time.Millisecond, func() error {
						si, err := js.StreamInfo("test_stream")
						if err != nil {
							return err
						}
						if si.State.Msgs != 150 {
							return fmt.Errorf("expected 150 messages, got %d", si.State.Msgs)
						}
						return nil
					})
				},
			} {
				t.Run(name, func(t *testing.T) {
					workload(t, setup)
				})
			}
		})
	}
}

func TestJetStreamClusterConsumerAssignmentCreateAPI(t *testing.T) {
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

	ncsys, err := nats.Connect(c.randomServer().ClientURL(), nats.UserInfo("admin", "s3cr3t!"))
	require_NoError(t, err)
	defer ncsys.Close()

	cfg, err := json.Marshal(&ConsumerConfig{
		Durable:   "CONSUMER",
		Name:      "CONSUMER",
		AckPolicy: AckExplicit,
		Replicas:  3,
	})
	require_NoError(t, err)

	created := time.Now().UTC()
	req, err := json.Marshal(&consumerAssignmentCreate{
		Account:  globalAccountName,
		Stream:   "TEST",
		Consumer: "CONSUMER",
		Created:  created,
		Config:   cfg,
	})
	require_NoError(t, err)
	require_NoError(t, ncsys.Publish(consumerAssignmentCreateSubj, req))

	// The meta leader creates the group for us and assigns the consumer.
	checkFor(t, 5*time.Second, 100*time.Millisecond, func() error {
		ci, err := js.ConsumerInfo("TEST", "CONSUMER")
		if err != nil {
			return err
		}
		if ci.Cluster == nil || len(ci.Cluster.Replicas) != 2 {
			return fmt.Errorf("expected an R3 consumer, got %+v", ci.Cluster)
		}
		return nil
	})

	ml := c.leader()
	mjs := ml.getJetStream()
	require_NotNil(t, mjs)
	getCa := func() *consumerAssignment {
		mjs.mu.RLock()
		defer mjs.mu.RUnlock()
		return mjs.consumerAssignment(globalAccountName, "TEST", "CONSUMER")
	}

	ca := getCa()
	require_NotNil(t, ca)
	require_True(t, ca.Created.Equal(created))
	require_NotNil(t, ca.Group)
	require_Len(t, len(ca.Group.Peers), 3)

	// Requests are idempotent, so the requester can simply retry until it observes the
	// assignment. A repeat must not create a new group or reset the assignment.
	require_NoError(t, ncsys.Publish(consumerAssignmentCreateSubj, req))
	require_NoError(t, ncsys.Flush())
	time.Sleep(500 * time.Millisecond)

	nca := getCa()
	require_NotNil(t, nca)
	require_NotNil(t, nca.Group)
	require_Equal(t, nca.Group.Name, ca.Group.Name)
	require_True(t, nca.Created.Equal(created))
}

func TestJetStreamClusterConsumerAssignmentCreateCompressedAPI(t *testing.T) {
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

	cfgJSON, err := json.Marshal(&ConsumerConfig{
		Durable:   "C",
		AckPolicy: AckExplicit,
		Replicas:  3,
	})
	require_NoError(t, err)

	// State is only distributed when restoring, and is the one field that can grow
	// large, so make sure a request carrying it survives being compressed and framed.
	state := &ConsumerState{
		Delivered: SequencePair{Consumer: 1000, Stream: 1000},
		AckFloor:  SequencePair{Consumer: 1, Stream: 1},
		Pending:   make(map[uint64]*Pending, 1000),
	}
	for i := uint64(2); i <= 1000; i++ {
		state.Pending[i] = &Pending{Sequence: i, Timestamp: int64(i)}
	}

	s := c.randomServer()
	s.sendConsumerAssignmentCreate(&consumerAssignmentCreate{
		Account:  globalAccountName,
		Stream:   "TEST",
		Consumer: "C",
		Created:  time.Now().UTC(),
		Config:   cfgJSON,
		State:    state,
	})

	checkFor(t, 10*time.Second, 100*time.Millisecond, func() error {
		sjs := s.getJetStream()
		sjs.mu.RLock()
		defer sjs.mu.RUnlock()
		if ca := sjs.consumerAssignment(globalAccountName, "TEST", "C"); ca == nil {
			return errors.New("consumer assignment not seen yet")
		}
		return nil
	})
}

func TestJetStreamClusterConsumerAssignmentDeleteAPI(t *testing.T) {
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
		Replicas:  3,
		AckPolicy: nats.AckExplicitPolicy,
	})
	require_NoError(t, err)

	ml := c.leader()
	mjs := ml.getJetStream()
	require_NotNil(t, mjs)
	getCa := func() *consumerAssignment {
		mjs.mu.RLock()
		defer mjs.mu.RUnlock()
		return mjs.consumerAssignment(globalAccountName, "TEST", "CONSUMER")
	}

	ca := getCa()
	require_NotNil(t, ca)
	created := ca.Created

	ncsys, err := nats.Connect(c.randomServer().ClientURL(), nats.UserInfo("admin", "s3cr3t!"))
	require_NoError(t, err)
	defer ncsys.Close()

	sendDelete := func(created time.Time) {
		req, err := json.Marshal(&consumerAssignmentDelete{
			Account:  globalAccountName,
			Stream:   "TEST",
			Consumer: "CONSUMER",
			Created:  created,
		})
		require_NoError(t, err)
		require_NoError(t, ncsys.Publish(consumerAssignmentDeleteSubj, req))
		require_NoError(t, ncsys.Flush())
	}

	// The creation time is part of the consumer's identity, so a request that doesn't
	// match must be ignored. Otherwise we'd delete a consumer that got recreated.
	sendDelete(created.Add(-time.Second))
	time.Sleep(500 * time.Millisecond)
	nca := getCa()
	require_NotNil(t, nca)
	require_True(t, nca.Created.Equal(created))

	// A matching request deletes the consumer.
	sendDelete(created)
	checkFor(t, 5*time.Second, 100*time.Millisecond, func() error {
		if getCa() != nil {
			return errors.New("consumer assignment still present")
		}
		return nil
	})
	_, err = js.ConsumerInfo("TEST", "CONSUMER")
	require_Error(t, err, nats.ErrConsumerNotFound)
}

func TestJetStreamClusterConsumerCreateResponseConsumerClosed(t *testing.T) {
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

	// An R1 consumer on a replicated stream responds through the R1 leader change path.
	_, err = js.AddConsumer("TEST", &nats.ConsumerConfig{
		Durable:   "C",
		AckPolicy: nats.AckExplicitPolicy,
		Replicas:  1,
	})
	require_NoError(t, err)

	s := c.consumerLeader(globalAccountName, "TEST", "C")
	require_NotNil(t, s)
	mset, err := s.globalAccount().lookupStream("TEST")
	require_NoError(t, err)
	o := mset.lookupConsumer("C")
	require_NotNil(t, o)

	// Redirect the create response to us, as if it had not been answered yet.
	// API responses are sent on the system account, so listen there.
	snc, _ := jsClientConnect(t, s, nats.UserInfo("admin", "s3cr3t!"))
	defer snc.Close()
	inbox := nats.NewInbox()
	sub, err := snc.SubscribeSync(inbox)
	require_NoError(t, err)
	require_NoError(t, snc.Flush())
	ca := o.consumerAssignment()
	require_NotNil(t, ca)
	ca = ca.clone()
	ca.Reply = inbox
	ca.clearResponded()

	// Simulate the consumer being torn down concurrently: the closed check at the
	// start of processConsumerLeaderChangeWithAssignment has already passed, but by
	// the time the response is built o.info() returns nil.
	o.mu.Lock()
	o.mset = nil
	o.mu.Unlock()
	defer func() {
		o.mu.Lock()
		o.mset = mset
		o.mu.Unlock()
	}()

	// Must not panic and must answer with an error rather than an empty response.
	require_NoError(t, s.getJetStream().processConsumerLeaderChangeWithAssignment(o, ca, true, 0))

	msg, err := sub.NextMsg(2 * time.Second)
	require_NoError(t, err)
	var resp JSApiConsumerCreateResponse
	require_NoError(t, json.Unmarshal(msg.Data, &resp))
	require_True(t, resp.ConsumerInfo == nil)
	require_NotNil(t, resp.Error)
	require_Equal(t, resp.Error.ErrCode, uint16(JSConsumerCreateErrF))
	require_Contains(t, resp.Error.Description, errConsumerClosed.Error())
}

func TestJetStreamClusterConsumerDeleteRacingGroupRename(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "R3S", 3)
	defer c.shutdown()

	nc, js := jsClientConnect(t, c.randomServer())
	defer nc.Close()

	_, err := js.AddStream(&nats.StreamConfig{Name: "TEST", Subjects: []string{"foo"}, Replicas: 3})
	require_NoError(t, err)
	_, err = js.AddConsumer("TEST", &nats.ConsumerConfig{Durable: "C", Replicas: 1, AckPolicy: nats.AckExplicitPolicy})
	require_NoError(t, err)

	ml := c.leader()
	mjs := ml.getJetStream()
	cc := mjs.cluster

	// Propose a rename of the consumer's group (what the meta leader does once a
	// single node group converges after a migration) and immediately run the
	// client delete request on the meta leader, before the rename is applied.
	mjs.mu.Lock()
	rca := mjs.consumerAssignment(globalAccountName, "TEST", "C").copyGroup()
	rca.Group.Name = groupNameForConsumer(rca.Group.Peers, rca.Group.Storage)
	rca.Reply = _EMPTY_
	err = cc.meta.Propose(cc.term, encodeAddConsumerAssignment(rca))
	if err == nil {
		cc.trackInflightConsumerProposal(globalAccountName, "TEST", rca, false)
	}
	mjs.mu.Unlock()
	require_NoError(t, err)
	ci := &ClientInfo{Account: globalAccountName}
	ml.jsClusteredConsumerDeleteRequest(ci, ml.GlobalAccount(), "TEST", "C", fmt.Sprintf(JSApiConsumerDeleteT, "TEST", "C"), _EMPTY_, nil)

	// The delete must have been proposed against the inflight (renamed) assignment.
	mjs.mu.RLock()
	inflight := cc.inflightConsumers[globalAccountName]["TEST"]["C"]
	mjs.mu.RUnlock()
	require_NotNil(t, inflight)
	require_True(t, inflight.deleted)
	require_Equal(t, inflight.consumerAssignment.Group.Name, rca.Group.Name)

	// Both the assignment and the consumer must be gone everywhere.
	checkFor(t, 5*time.Second, 100*time.Millisecond, func() error {
		for _, s := range c.servers {
			sjs := s.getJetStream()
			sjs.mu.RLock()
			ca := sjs.consumerAssignment(globalAccountName, "TEST", "C")
			sjs.mu.RUnlock()
			if ca != nil {
				return fmt.Errorf("server %q still has an assignment for consumer (group %q)", s, ca.Group.Name)
			}
			if mset, err := s.GlobalAccount().lookupStream("TEST"); err == nil && mset.lookupConsumer("C") != nil {
				return fmt.Errorf("server %q still has the consumer running", s)
			}
		}
		return nil
	})
	for _, s := range c.servers {
		c.waitOnServerHealthz(s)
	}
}

// Meta entries must not be applied while shutting down, and replay after restart.
func TestJetStreamClusterMetaAppliesSkippedWhileShuttingDown(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "R3S", 3)
	defer c.shutdown()

	nc, js := jsClientConnect(t, c.randomServer())
	defer nc.Close()

	_, err := js.AddStream(&nats.StreamConfig{Name: "TEST", Subjects: []string{"foo"}, Replicas: 3})
	require_NoError(t, err)
	_, err = js.AddConsumer("TEST", &nats.ConsumerConfig{Durable: "C", Replicas: 3, AckPolicy: nats.AckExplicitPolicy})
	require_NoError(t, err)
	c.waitOnConsumerLeader(globalAccountName, "TEST", "C")

	// A follower that's shutting down, as far as the meta monitor is concerned.
	s := c.randomNonConsumerLeader(globalAccountName, "TEST", "C")
	require_NotNil(t, s)
	mset, err := s.GlobalAccount().lookupStream("TEST")
	require_NoError(t, err)
	o := mset.lookupConsumer("C")
	require_NotNil(t, o)
	n := o.raftNode().(*raft)
	n.RLock()
	group, sd := n.group, n.sd
	n.RUnlock()

	sjs := s.getJetStream()
	sjs.mu.Lock()
	sjs.shuttingDown = true
	sjs.mu.Unlock()

	require_NoError(t, js.DeleteConsumer("TEST", "C"))
	// The others apply the removal.
	for _, os := range c.servers {
		if os == s {
			continue
		}
		checkFor(t, 5*time.Second, 50*time.Millisecond, func() error {
			if os.lookupRaftNode(group) != nil {
				return fmt.Errorf("%s still runs the consumer's raft node", os.Name())
			}
			return nil
		})
	}
	// The shutting down server doesn't touch what's still running.
	require_Equal(t, n.State(), Follower)
	require_True(t, s.lookupRaftNode(group) == n)
	_, err = os.Stat(sd)
	require_NoError(t, err)

	// The removal replays after a restart.
	s.Shutdown()
	s.WaitForShutdown()
	s = c.restartServer(s)
	c.waitOnServerCurrent(s)
	checkFor(t, 5*time.Second, 50*time.Millisecond, func() error {
		if s.lookupRaftNode(group) != nil {
			return fmt.Errorf("consumer's raft node still registered after restart")
		}
		if _, err := os.Stat(sd); !os.IsNotExist(err) {
			return fmt.Errorf("consumer's raft store still on disk after restart")
		}
		return nil
	})
}

// The stop gap cleanup of a consumer removal must delete a running Raft node, not just its store.
func TestJetStreamClusterConsumerRemovalStopGapDeletesRunningNode(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "R3S", 3)
	defer c.shutdown()

	nc, js := jsClientConnect(t, c.randomServer())
	defer nc.Close()

	_, err := js.AddStream(&nats.StreamConfig{Name: "TEST", Subjects: []string{"foo"}, Replicas: 3})
	require_NoError(t, err)
	_, err = js.AddConsumer("TEST", &nats.ConsumerConfig{Durable: "C", Replicas: 3, AckPolicy: nats.AckExplicitPolicy})
	require_NoError(t, err)

	s := c.consumerLeader(globalAccountName, "TEST", "C")
	require_NotNil(t, s)
	mset, err := s.GlobalAccount().lookupStream("TEST")
	require_NoError(t, err)
	o := mset.lookupConsumer("C")
	require_NotNil(t, o)
	n := o.raftNode().(*raft)
	require_Equal(t, n.State(), Leader)
	n.RLock()
	sd := n.sd
	n.RUnlock()

	// A removal for a consumer that can't be looked up, but names the running node's group.
	sjs := s.getJetStream()
	sjs.mu.RLock()
	ca := sjs.consumerAssignment(globalAccountName, "TEST", "C")
	sjs.mu.RUnlock()
	require_NotNil(t, ca)
	rca := &consumerAssignment{
		Client: ca.Client, Created: ca.Created, Stream: "TEST", Name: "GONE",
		Group: &raftGroup{Name: ca.Group.Name, Peers: copyStrings(ca.Group.Peers)},
	}
	sjs.processClusterDeleteConsumer(rca, false)

	// The node is gone along with its store, not just the store.
	require_Equal(t, n.State(), Closed)
	require_True(t, n.IsDeleted())
	_, err = os.Stat(sd)
	require_True(t, os.IsNotExist(err))
	require_True(t, s.lookupRaftNode(ca.Group.Name) == nil)
}

func TestJetStreamClusterStreamDeleteRacingGroupRename(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "R3S", 3)
	defer c.shutdown()

	nc, js := jsClientConnect(t, c.randomServer())
	defer nc.Close()

	_, err := js.AddStream(&nats.StreamConfig{Name: "TEST", Subjects: []string{"foo"}, Replicas: 1})
	require_NoError(t, err)

	ml := c.leader()
	mjs := ml.getJetStream()
	cc := mjs.cluster

	mjs.mu.Lock()
	rsa := mjs.streamAssignment(globalAccountName, "TEST").copyGroup()
	rsa.Group.Name = groupNameForStream(rsa.Group.Peers, rsa.Group.Storage)
	rsa.Reply = _EMPTY_
	err = cc.meta.Propose(cc.term, encodeUpdateStreamAssignment(rsa))
	if err == nil {
		cc.trackInflightStreamProposal(globalAccountName, rsa, false)
	}
	mjs.mu.Unlock()
	require_NoError(t, err)
	ci := &ClientInfo{Account: globalAccountName}
	ml.jsClusteredStreamDeleteRequest(ci, ml.GlobalAccount(), "TEST", fmt.Sprintf(JSApiStreamDeleteT, "TEST"), _EMPTY_, nil)

	// The delete must have been proposed against the inflight (renamed) assignment.
	mjs.mu.RLock()
	inflight := cc.inflightStreams[globalAccountName]["TEST"]
	mjs.mu.RUnlock()
	require_NotNil(t, inflight)
	require_True(t, inflight.deleted)
	require_Equal(t, inflight.streamAssignment.Group.Name, rsa.Group.Name)

	// Both the assignment and the stream must be gone everywhere.
	checkFor(t, 5*time.Second, 100*time.Millisecond, func() error {
		for _, s := range c.servers {
			sjs := s.getJetStream()
			sjs.mu.RLock()
			sa := sjs.streamAssignment(globalAccountName, "TEST")
			sjs.mu.RUnlock()
			if sa != nil {
				return fmt.Errorf("server %q still has an assignment for stream (group %q)", s, sa.Group.Name)
			}
			if _, err := s.GlobalAccount().lookupStream("TEST"); err == nil {
				return fmt.Errorf("server %q still has the stream running", s)
			}
		}
		return nil
	})
	for _, s := range c.servers {
		c.waitOnServerHealthz(s)
	}
}

func TestJetStreamClusterConsumerPauseRacingGroupRename(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "R3S", 3)
	defer c.shutdown()

	nc, js := jsClientConnect(t, c.randomServer())
	defer nc.Close()

	_, err := js.AddStream(&nats.StreamConfig{Name: "TEST", Subjects: []string{"foo"}, Replicas: 3})
	require_NoError(t, err)
	_, err = js.AddConsumer("TEST", &nats.ConsumerConfig{Durable: "C", Replicas: 1, AckPolicy: nats.AckExplicitPolicy})
	require_NoError(t, err)

	ml := c.leader()
	mjs := ml.getJetStream()
	cc := mjs.cluster

	// Track a rename of the consumer's group as an inflight proposal, as if the
	// meta leader had just proposed it, and it is not applied yet.
	mjs.mu.Lock()
	rca := mjs.consumerAssignment(globalAccountName, "TEST", "C").copyGroup()
	rca.Group.Name = groupNameForConsumer(rca.Group.Peers, rca.Group.Storage)
	rca.Reply = _EMPTY_
	cc.trackInflightConsumerProposal(globalAccountName, "TEST", rca, false)
	mjs.mu.Unlock()

	pauseUntil := time.Now().Add(time.Hour).UTC()
	req, err := json.Marshal(JSApiConsumerPauseRequest{PauseUntil: pauseUntil})
	require_NoError(t, err)
	msg, err := nc.Request(fmt.Sprintf(JSApiConsumerPauseT, "TEST", "C"), req, 5*time.Second)
	require_NoError(t, err)
	var resp JSApiConsumerPauseResponse
	require_NoError(t, json.Unmarshal(msg.Data, &resp))
	require_True(t, resp.Error == nil)
	require_True(t, resp.Paused)

	// The pause must have been proposed against the inflight (renamed) assignment,
	// so the rename sticks and the consumer is paused everywhere.
	checkFor(t, 5*time.Second, 100*time.Millisecond, func() error {
		for _, s := range c.servers {
			sjs := s.getJetStream()
			sjs.mu.RLock()
			ca := sjs.consumerAssignment(globalAccountName, "TEST", "C")
			sjs.mu.RUnlock()
			if ca == nil {
				return fmt.Errorf("server %q has no assignment for consumer", s)
			}
			if ca.Group.Name != rca.Group.Name {
				return fmt.Errorf("server %q has group %q, expected renamed group %q", s, ca.Group.Name, rca.Group.Name)
			}
			if ca.Config.PauseUntil == nil || !ca.Config.PauseUntil.Equal(pauseUntil) {
				return fmt.Errorf("server %q assignment is not paused", s)
			}
		}
		return nil
	})
}

func TestJetStreamClusterConsumerLeaderStepDownWithInflightStreamUpdate(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "R3S", 3)
	defer c.shutdown()

	nc, js := jsClientConnect(t, c.randomServer())
	defer nc.Close()

	_, err := js.AddStream(&nats.StreamConfig{Name: "TEST", Subjects: []string{"foo"}, Replicas: 3})
	require_NoError(t, err)
	_, err = js.AddConsumer("TEST", &nats.ConsumerConfig{Durable: "C", Replicas: 3, AckPolicy: nats.AckExplicitPolicy})
	require_NoError(t, err)

	ml := c.leader()
	mjs := ml.getJetStream()
	cc := mjs.cluster
	c.stepDownConsumerLeader(nc, globalAccountName, "TEST", "C", ml)

	// Track a client stream update as an inflight proposal on the meta leader. As
	// proposed by jsClusteredStreamUpdateRequestLocked it has no consumers.
	mjs.mu.Lock()
	osa := mjs.streamAssignment(globalAccountName, "TEST")
	usa := &streamAssignment{Group: osa.Group, Sync: osa.Sync, Created: osa.Created, Config: osa.Config, Client: osa.Client}
	cc.trackInflightStreamProposal(globalAccountName, usa, false)
	mjs.mu.Unlock()

	subj := fmt.Sprintf(JSApiConsumerLeaderStepDownT, "TEST", "C")
	msg, err := nc.Request(subj, nil, 5*time.Second)
	require_NoError(t, err)
	var resp JSApiConsumerLeaderStepDownResponse
	require_NoError(t, json.Unmarshal(msg.Data, &resp))
	require_True(t, resp.Error == nil)
	require_True(t, resp.Success)

	c.waitOnConsumerLeader(globalAccountName, "TEST", "C")
	require_True(t, c.consumerLeader(globalAccountName, "TEST", "C") != ml)
}

func TestJetStreamClusterStreamInfoWithInflightStreamUpdate(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "R3S", 3)
	defer c.shutdown()

	nc, js := jsClientConnect(t, c.randomServer())
	defer nc.Close()

	_, err := js.AddStream(&nats.StreamConfig{Name: "TEST", Subjects: []string{"foo"}, Replicas: 3})
	require_NoError(t, err)

	// The stream leader serves stream info, the meta leader tracks inflight proposals.
	ml := c.leader()
	c.stepDownStreamLeader(nc, globalAccountName, "TEST", ml)

	// Need an R1 consumer on another peer, so the local count is lower than the cluster-wide one.
	var consumers int
	var remote bool
	for ; !remote && consumers < 10; consumers++ {
		name := fmt.Sprintf("C%d", consumers)
		_, err = js.AddConsumer("TEST", &nats.ConsumerConfig{Durable: name, Replicas: 1, AckPolicy: nats.AckExplicitPolicy})
		require_NoError(t, err)
		remote = c.consumerLeader(globalAccountName, "TEST", name) != ml
	}
	require_True(t, remote)

	// Track a client stream update as an inflight proposal, it has no consumers.
	mjs := ml.getJetStream()
	mjs.mu.Lock()
	osa := mjs.streamAssignment(globalAccountName, "TEST")
	usa := &streamAssignment{Group: osa.Group, Sync: osa.Sync, Created: osa.Created, Config: osa.Config, Client: osa.Client}
	mjs.cluster.trackInflightStreamProposal(globalAccountName, usa, false)
	mjs.mu.Unlock()

	si, err := js.StreamInfo("TEST")
	require_NoError(t, err)
	require_Equal(t, si.State.Consumers, consumers)
}

func TestJetStreamClusterStreamInfoWithUncommittedCreate(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "R3S", 3)
	defer c.shutdown()

	ml := c.leader()
	nc, js := jsClientConnect(t, ml)
	defer nc.Close()

	// Keep the metadata leader alive, but prevent new assignments from reaching quorum.
	for _, s := range c.servers {
		if s != ml {
			s.Shutdown()
		}
	}
	sjs, _ := ml.getJetStreamCluster()

	// R1 allows placement on the surviving server, but metadata still needs quorum.
	cfg := &nats.StreamConfig{Name: "TEST", Subjects: []string{"foo"}, Replicas: 1}
	_, err := js.AddStream(cfg, nats.MaxWait(250*time.Millisecond))
	require_Error(t, err, context.DeadlineExceeded)

	checkFor(t, time.Second, 10*time.Millisecond, func() error {
		sjs.mu.RLock()
		applied := sjs.streamAssignment(globalAccountName, "TEST")
		pending := sjs.streamAssignmentOrInflight(globalAccountName, "TEST")
		sjs.mu.RUnlock()
		if applied != nil || pending == nil {
			return fmt.Errorf("expected only an inflight assignment: applied=%v pending=%v",
				applied != nil, pending != nil)
		}
		return nil
	})

	// INFO must require an actual stream, not just its inflight assignment.
	_, err = js.StreamInfo("TEST", nats.MaxWait(2*time.Second))
	require_Error(t, err, nats.ErrStreamNotFound)

	// AddStream sends CREATE directly. Retrying cannot succeed without quorum.
	_, err = js.AddStream(cfg, nats.MaxWait(2*time.Second))
	require_Error(t, err, context.DeadlineExceeded)

	// Restore quorum and verify that the stream can store messages.
	for _, s := range c.servers {
		if s != ml {
			c.restartServer(s)
		}
	}
	c.waitOnLeader()
	c.waitOnStreamLeader(globalAccountName, "TEST")
	si, err := js.StreamInfo("TEST")
	require_NoError(t, err)
	require_Equal(t, si.Config.Name, "TEST")
	ack, err := js.Publish("foo", []byte("message after committed creation"))
	require_NoError(t, err)
	msg, err := js.GetMsg("TEST", ack.Sequence)
	require_NoError(t, err)
	require_Equal(t, msg.Subject, "foo")
	require_Equal(t, string(msg.Data), "message after committed creation")
}

func TestJetStreamClusterConsumerInfoWithUncommittedCreate(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "R3S", 3)
	defer c.shutdown()

	ml := c.leader()
	nc, js := jsClientConnect(t, ml)
	defer nc.Close()

	_, err := js.AddStream(&nats.StreamConfig{
		Name: "TEST", Subjects: []string{"foo"}, Replicas: 3,
	})
	require_NoError(t, err)
	c.waitOnStreamLeader(globalAccountName, "TEST")

	// Shutdown all servers, except for the metadata leader.
	for _, s := range c.servers {
		if s != ml {
			s.Shutdown()
		}
	}
	sjs, _ := ml.getJetStreamCluster()

	// Use R1 so the consumer can be placed on the metadata.
	// Its assignment still needs a quorum of the three metadata peers.
	cfg := &nats.ConsumerConfig{
		Durable: "C", Name: "C", FilterSubject: "foo", AckPolicy: nats.AckExplicitPolicy, Replicas: 1,
	}
	_, err = js.AddConsumer("TEST", cfg, nats.MaxWait(250*time.Millisecond))
	require_Error(t, err, context.DeadlineExceeded)

	checkFor(t, time.Second, 10*time.Millisecond, func() error {
		sjs.mu.RLock()
		applied := sjs.consumerAssignment(globalAccountName, "TEST", "C")
		pending := sjs.consumerAssignmentOrInflight(globalAccountName, "TEST", "C")
		sjs.mu.RUnlock()
		if applied != nil || pending == nil {
			return fmt.Errorf("expected only an inflight assignment: applied=%v pending=%v",
				applied != nil, pending != nil)
		}
		return nil
	})

	// INFO must require an actual consumer, not just its inflight assignment.
	_, err = js.ConsumerInfo("TEST", "C", nats.MaxWait(2*time.Second))
	require_Error(t, err, nats.ErrConsumerNotFound)

	// The legacy client retries creation after INFO returns consumer not found.
	// Without quorum expect the request to timeout.
	_, err = js.AddConsumer("TEST", cfg, nats.MaxWait(2*time.Second))
	require_Error(t, err, context.DeadlineExceeded)

	// Restart servers and check that the consumer works.
	for _, s := range c.servers {
		if s != ml {
			c.restartServer(s)
		}
	}
	c.waitOnLeader()
	c.waitOnStreamLeader(globalAccountName, "TEST")
	c.waitOnConsumerLeader(globalAccountName, "TEST", "C")
	ci, err := js.ConsumerInfo("TEST", "C")
	require_NoError(t, err)
	require_Equal(t, ci.Name, "C")
	sub, err := js.PullSubscribe("foo", "C", nats.Bind("TEST", "C"))
	require_NoError(t, err)
	ack, err := js.Publish("foo", []byte("message after committed creation"))
	require_NoError(t, err)
	msgs, err := sub.Fetch(1, nats.MaxWait(5*time.Second))
	require_NoError(t, err)
	require_Equal(t, len(msgs), 1)
	meta, err := msgs[0].Metadata()
	require_NoError(t, err)
	require_Equal(t, meta.Sequence.Stream, ack.Sequence)
	require_NoError(t, msgs[0].AckSync())
}

func TestJetStreamClusterConsumerInfoWithInflightConsumerDelete(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "R3S", 3)
	defer c.shutdown()

	nc, js := jsClientConnect(t, c.randomServer())
	defer nc.Close()

	_, err := js.AddStream(&nats.StreamConfig{Name: "TEST", Subjects: []string{"foo"}, Replicas: 3})
	require_NoError(t, err)
	_, err = js.AddConsumer("TEST", &nats.ConsumerConfig{Durable: "C", Replicas: 3, AckPolicy: nats.AckExplicitPolicy})
	require_NoError(t, err)

	// The consumer leader serves consumer info, the meta leader tracks inflight proposals.
	ml := c.leader()
	c.stepDownConsumerLeader(nc, globalAccountName, "TEST", "C", ml)

	// Track a consumer delete as an inflight proposal.
	mjs := ml.getJetStream()
	mjs.mu.Lock()
	ca := mjs.consumerAssignment(globalAccountName, "TEST", "C")
	mjs.cluster.trackInflightConsumerProposal(globalAccountName, "TEST", ca, true)
	mjs.mu.Unlock()

	ci, err := js.ConsumerInfo("TEST", "C")
	require_NoError(t, err)
	require_Equal(t, ci.Name, "C")
}

func TestJetStreamClusterRetentionUpdateToInterestWithoutConsumers(t *testing.T) {
	for _, replicas := range []int{1, 3} {
		t.Run(fmt.Sprintf("R%d", replicas), func(t *testing.T) {
			c := createJetStreamClusterExplicit(t, "R3S", 3)
			defer c.shutdown()

			nc, js := jsClientConnect(t, c.randomServer())
			defer nc.Close()

			cfg := &nats.StreamConfig{
				Name:      "TEST",
				Subjects:  []string{"foo"},
				Retention: nats.LimitsPolicy,
				Replicas:  replicas,
			}
			_, err := js.AddStream(cfg)
			require_NoError(t, err)

			for range 10 {
				_, err = js.Publish("foo", []byte("msg"))
				require_NoError(t, err)
			}
			si, err := js.StreamInfo("TEST")
			require_NoError(t, err)
			require_Equal(t, si.State.Msgs, 10)
			require_Equal(t, si.State.Consumers, 0)

			// Change to Interest retention without any consumers should drop all messages.
			cfg.Retention = nats.InterestPolicy
			_, err = js.UpdateStream(cfg)
			require_NoError(t, err)

			checkFor(t, 5*time.Second, 100*time.Millisecond, func() error {
				for _, s := range c.servers {
					mset, err := s.globalAccount().lookupStream("TEST")
					if err != nil {
						// Not a replica.
						continue
					}
					if mset.config().Retention != InterestPolicy {
						return fmt.Errorf("%s: retention not yet interest", s.Name())
					}
					var ss StreamState
					mset.store.FastState(&ss)
					if ss.Msgs != 0 {
						return fmt.Errorf("%s: expected 0 messages, got %d (first %d, last %d)",
							s.Name(), ss.Msgs, ss.FirstSeq, ss.LastSeq)
					}
					if ss.LastSeq != 10 {
						return fmt.Errorf("%s: expected last sequence 10, got %d", s.Name(), ss.LastSeq)
					}
				}
				return nil
			})

			// New messages must be skipped as well.
			pa, err := js.Publish("foo", []byte("msg"))
			require_NoError(t, err)
			require_Equal(t, pa.Sequence, 11)
			si, err = js.StreamInfo("TEST")
			require_NoError(t, err)
			require_Equal(t, si.State.Msgs, 0)
			require_Equal(t, si.State.LastSeq, 11)
		})
	}
}

func TestJetStreamClusterConsumerRemovalMatchesRenamedGroup(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "R3S", 3)
	defer c.shutdown()

	nc, js := jsClientConnect(t, c.randomServer())
	defer nc.Close()

	_, err := js.AddStream(&nats.StreamConfig{Name: "TEST", Subjects: []string{"foo"}, Replicas: 3})
	require_NoError(t, err)
	_, err = js.AddConsumer("TEST", &nats.ConsumerConfig{Durable: "C", Replicas: 1, AckPolicy: nats.AckExplicitPolicy})
	require_NoError(t, err)

	// A server that only holds the assignment, not the consumer itself.
	s := c.randomNonConsumerLeader(globalAccountName, "TEST", "C")
	sjs := s.getJetStream()
	sjs.mu.RLock()
	ca := sjs.consumerAssignment(globalAccountName, "TEST", "C")
	sjs.mu.RUnlock()
	require_NotNil(t, ca)

	// A removal for a recreated consumer with the same name is not ours.
	nca := ca.copyGroup()
	nca.Group.Name = groupNameForConsumer(nca.Group.Peers, nca.Group.Storage)
	nca.Created = time.Now().UTC()
	sjs.processConsumerRemoval(nca)
	sjs.mu.RLock()
	ca = sjs.consumerAssignment(globalAccountName, "TEST", "C")
	sjs.mu.RUnlock()
	require_NotNil(t, ca)

	// A removal for our consumer under its renamed group is.
	rca := ca.copyGroup()
	rca.Group.Name = groupNameForConsumer(rca.Group.Peers, rca.Group.Storage)
	sjs.processConsumerRemoval(rca)
	sjs.mu.RLock()
	ca = sjs.consumerAssignment(globalAccountName, "TEST", "C")
	sjs.mu.RUnlock()
	require_True(t, ca == nil)
}

func TestJetStreamClusterStreamLeaderStepDownWithInflightStreamDelete(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "R3S", 3)
	defer c.shutdown()

	nc, js := jsClientConnect(t, c.randomServer())
	defer nc.Close()

	_, err := js.AddStream(&nats.StreamConfig{Name: "TEST", Subjects: []string{"foo"}, Replicas: 3})
	require_NoError(t, err)

	// The stream leader serves the request, the meta leader tracks inflight proposals.
	ml := c.leader()
	c.stepDownStreamLeader(nc, globalAccountName, "TEST", ml)

	// Track a stream delete as an inflight proposal.
	mjs := ml.getJetStream()
	mjs.mu.Lock()
	sa := mjs.streamAssignment(globalAccountName, "TEST")
	mjs.cluster.trackInflightStreamProposal(globalAccountName, sa, true)
	mjs.mu.Unlock()

	msg, err := nc.Request(fmt.Sprintf(JSApiStreamLeaderStepDownT, "TEST"), nil, 5*time.Second)
	require_NoError(t, err)
	var resp JSApiStreamLeaderStepDownResponse
	require_NoError(t, json.Unmarshal(msg.Data, &resp))
	require_True(t, resp.Error == nil)
	require_True(t, resp.Success)
	c.waitOnStreamLeader(globalAccountName, "TEST")
	require_True(t, c.streamLeader(globalAccountName, "TEST") != ml)
}

func TestJetStreamClusterMsgGetDeletePurgeWithInflightStreamDelete(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "R3S", 3)
	defer c.shutdown()

	nc, js := jsClientConnect(t, c.randomServer())
	defer nc.Close()

	_, err := js.AddStream(&nats.StreamConfig{Name: "TEST", Subjects: []string{"foo"}, Replicas: 3})
	require_NoError(t, err)
	for range 2 {
		_, err = js.Publish("foo", nil)
		require_NoError(t, err)
	}

	// The stream leader serves the requests, the meta leader tracks inflight proposals.
	ml := c.leader()
	c.stepDownStreamLeader(nc, globalAccountName, "TEST", ml)

	// Track a stream delete as an inflight proposal.
	mjs := ml.getJetStream()
	mjs.mu.Lock()
	sa := mjs.streamAssignment(globalAccountName, "TEST")
	mjs.cluster.trackInflightStreamProposal(globalAccountName, sa, true)
	mjs.mu.Unlock()

	_, err = js.GetMsg("TEST", 1)
	require_NoError(t, err)
	require_NoError(t, js.DeleteMsg("TEST", 1))
	require_NoError(t, js.PurgeStream("TEST"))
}

// Moving a stream re-proposes its consumers. That must not be treated as a
// consumer update, which for a sourcing consumer resets its delivery state
// and makes the sourcing stream re-create the consumer.
func TestJetStreamClusterSourcingConsumerMoveDoesNotReset(t *testing.T) {
	c := createJetStreamClusterWithTemplateAndModHook(t, jsClusterTempl, "R3S", 3,
		func(serverName, clusterName, storeDir, conf string) string {
			return fmt.Sprintf("%s\nserver_tags: [server:%s]", conf, serverName)
		})
	defer c.shutdown()

	nc, js := jsClientConnect(t, c.randomServer())
	defer nc.Close()

	// A sourcing consumer is only assigned through the meta layer on a non-Limits stream.
	cfg := &nats.StreamConfig{
		Name:      "ORIGIN",
		Subjects:  []string{"foo"},
		Retention: nats.InterestPolicy,
		Placement: &nats.Placement{Tags: []string{"server:S-1"}},
	}
	_, err := js.AddStream(cfg)
	require_NoError(t, err)
	_, err = js.AddStream(&nats.StreamConfig{Name: "SOURCE", Sources: []*nats.StreamSource{{Name: "ORIGIN"}}})
	require_NoError(t, err)
	// Interest retention drops messages until the sourcing consumer exists.
	var name string
	checkFor(t, 5*time.Second, 100*time.Millisecond, func() error {
		for name = range js.ConsumerNames("ORIGIN") {
			return nil
		}
		return fmt.Errorf("no sourcing consumer yet")
	})

	sourced := func(n uint64) {
		t.Helper()
		checkFor(t, 5*time.Second, 100*time.Millisecond, func() error {
			si, err := js.StreamInfo("SOURCE")
			if err != nil {
				return err
			}
			if si.State.Msgs != n {
				return fmt.Errorf("expected %d sourced messages, got %d", n, si.State.Msgs)
			}
			return nil
		})
	}
	for range 10 {
		_, err = js.Publish("foo", nil)
		require_NoError(t, err)
	}
	sourced(10)

	// The client can't decode the sourcing consumer's ack policy, use the raw API.
	consumerInfo := func() *ConsumerInfo {
		t.Helper()
		msg, err := nc.Request(fmt.Sprintf(JSApiConsumerInfoT, "ORIGIN", name), nil, 5*time.Second)
		require_NoError(t, err)
		var resp JSApiConsumerInfoResponse
		require_NoError(t, json.Unmarshal(msg.Data, &resp))
		require_True(t, resp.Error == nil)
		return resp.ConsumerInfo
	}
	require_Equal(t, consumerInfo().Delivered.Consumer, 10)

	// Move the origin, which re-proposes the sourcing consumer.
	cfg.Placement.Tags = []string{"server:S-2"}
	_, err = js.UpdateStream(cfg)
	require_NoError(t, err)
	c.waitOnStreamLeader(globalAccountName, "ORIGIN")
	require_Equal(t, c.streamLeader(globalAccountName, "ORIGIN").Name(), "S-2")

	// Give the sourcing stream a few heartbeats to notice a reset.
	time.Sleep(3 * sourceHealthHB)
	require_Equal(t, consumerInfo().Delivered.Consumer, 10)

	for range 10 {
		_, err = js.Publish("foo", nil)
		require_NoError(t, err)
	}
	sourced(20)
	require_Equal(t, consumerInfo().Delivered.Consumer, 20)
}

func TestJetStreamClusterScaleDownWaitsForPeerStateAnswer(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "R3S", 3)
	defer c.shutdown()

	nc, js := jsClientConnect(t, c.randomServer())
	defer nc.Close()

	_, err := js.AddStream(&nats.StreamConfig{Name: "TEST", Subjects: []string{"foo"}, Replicas: 3})
	require_NoError(t, err)
	for range 10 {
		sendStreamMsg(t, nc, "foo", "hello")
	}
	checkFor(t, 2*time.Second, 200*time.Millisecond, func() error {
		return checkState(t, c, globalAccountName, "TEST")
	})

	sl := c.streamLeader(globalAccountName, "TEST")
	rs := c.randomNonStreamLeader(globalAccountName, "TEST")
	mset, err := rs.globalAccount().lookupStream("TEST")
	require_NoError(t, err)

	// Temporarily stop the follower from answering with the state of its stream store.
	mset.mu.Lock()
	isubj := fmt.Sprintf(clusterStreamInfoT, mset.jsa.acc(), mset.cfg.Name)
	rs.sysUnsubscribe(mset.infoSub)
	mset.mu.Unlock()

	// Scale down to the leader and the silent follower.
	ml := c.leader()
	mjs := ml.getJetStream()
	cc := mjs.cluster
	mjs.mu.Lock()
	sa := mjs.streamAssignment(globalAccountName, "TEST")
	require_NotNil(t, sa)
	nsa := sa.copyGroup()
	cfg := *sa.Config
	cfg.Replicas = 2
	nsa.Config = &cfg
	nsa.Reply = _EMPTY_
	nsa.Group.Desired = &desiredRaftGroup{
		Created: time.Now().UTC(),
		ID:      nuid.Next(),
		Peers:   []string{sl.NodeName(), rs.NodeName()},
		Origin: &desiredRaftGroupOrigin{
			Peers:    copyStrings(sa.Group.Peers),
			Replicas: 3,
		},
	}
	err = cc.meta.Propose(cc.term, encodeUpdateStreamAssignment(nsa))
	if err == nil {
		cc.trackInflightStreamProposal(globalAccountName, nsa, false)
	}
	mjs.mu.Unlock()
	require_NoError(t, err)

	sjs := sl.getJetStream()
	assignment := func() (int, *DesiredClusterInfoStatus) {
		sjs.mu.RLock()
		defer sjs.mu.RUnlock()
		sa := sjs.streamAssignment(globalAccountName, "TEST")
		if sa == nil || sa.Group == nil {
			return 0, nil
		}
		return len(sa.Group.Peers), sa.Group.migration
	}

	// No peer may be removed while the follower stays silent, we'd be down to one copy we
	// can account for.
	var held bool
	for deadline := time.Now().Add(2 * time.Second); time.Now().Before(deadline); {
		peers, status := assignment()
		require_Equal(t, peers, 3)
		if status != nil && status.Type == MigrationStatusCatchup {
			held = true
		}
		time.Sleep(100 * time.Millisecond)
	}
	require_True(t, held)

	// Once it answers again, the scale down goes ahead, onto the peers we asked for.
	mset.mu.Lock()
	mset.infoSub, err = rs.systemSubscribe(isubj, _EMPTY_, false, mset.sysc, mset.handleClusterStreamInfoRequest)
	mset.mu.Unlock()
	require_NoError(t, err)
	c.waitOnStreamLeader(globalAccountName, "TEST")
	sjs.mu.RLock()
	defer sjs.mu.RUnlock()
	sa = sjs.streamAssignment(globalAccountName, "TEST")
	require_True(t, slices.Contains(sa.Group.Peers, rs.NodeName()))
	require_True(t, slices.Contains(sa.Group.Peers, sl.NodeName()))
}

func TestJetStreamClusterStreamMoveIgnoresStoreStateOfDownPeer(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "R4S", 4)
	defer c.shutdown()

	nc, js := jsClientConnect(t, c.randomServer())
	defer nc.Close()

	_, err := js.AddStream(&nats.StreamConfig{Name: "TEST", Subjects: []string{"foo"}, Replicas: 3})
	require_NoError(t, err)
	for range 10 {
		sendStreamMsg(t, nc, "foo", "hello")
	}
	checkFor(t, 2*time.Second, 200*time.Millisecond, func() error {
		return checkState(t, c, globalAccountName, "TEST")
	})

	// Move away from the stream leader, onto the server not hosting the stream yet.
	sl := c.streamLeader(globalAccountName, "TEST")
	var desired []string
	var followers []*Server
	var ns *Server
	for _, s := range c.servers {
		if s == sl {
			continue
		}
		desired = append(desired, s.NodeName())
		if s.JetStreamIsStreamAssigned(globalAccountName, "TEST") {
			followers = append(followers, s)
		} else {
			ns = s
		}
	}
	require_Len(t, len(followers), 2)
	require_NotNil(t, ns)

	// answer has the server answer with the state of its stream store, and counts the answers
	// given once it holds all messages.
	answer := func(s *Server, answering bool) *atomic.Int32 {
		mset, err := s.globalAccount().lookupStream("TEST")
		require_NoError(t, err)
		var answered atomic.Int32
		mset.mu.Lock()
		defer mset.mu.Unlock()
		s.sysUnsubscribe(mset.infoSub)
		if !answering {
			return &answered
		}
		isubj := fmt.Sprintf(clusterStreamInfoT, mset.jsa.acc(), mset.cfg.Name)
		mset.infoSub, err = s.systemSubscribe(isubj, _EMPTY_, false, mset.sysc, func(sub *subscription, c *client, acc *Account, subject, reply string, msg []byte) {
			var req clusterStreamInfoRequest
			if json.Unmarshal(msg, &req) != nil || !req.State {
				mset.handleClusterStreamInfoRequest(sub, c, acc, subject, reply, msg)
				return
			}
			mset.processClusterStreamStateRequest(reply)
			if mset.state().LastSeq == 10 {
				answered.Add(1)
			}
		})
		require_NoError(t, err)
		return &answered
	}
	waitAnswered := func(answered *atomic.Int32, count int32) {
		t.Helper()
		checkFor(t, 5*time.Second, 100*time.Millisecond, func() error {
			if n := answered.Load(); n < count {
				return fmt.Errorf("expected %d answers, got %d", count, n)
			}
			return nil
		})
	}

	// Stop both followers from answering with the state of their stream store.
	for _, s := range followers {
		answer(s, false)
	}

	// Have the meta leader start moving the stream onto the desired peers.
	ml := c.leader()
	mjs := ml.getJetStream()
	cc := mjs.cluster
	mjs.mu.Lock()
	sa := mjs.streamAssignment(globalAccountName, "TEST")
	require_NotNil(t, sa)
	origin := copyStrings(sa.Group.Peers)
	nsa := sa.copyGroup()
	nsa.Reply = _EMPTY_
	nsa.Group.Desired = &desiredRaftGroup{
		Created: time.Now().UTC(),
		ID:      nuid.Next(),
		Peers:   desired,
		Origin: &desiredRaftGroupOrigin{
			Peers:    origin,
			Replicas: 3,
		},
	}
	err = cc.meta.Propose(cc.term, encodeUpdateStreamAssignment(nsa))
	cc.trackInflightStreamProposal(globalAccountName, nsa, false)
	mjs.mu.Unlock()
	require_NoError(t, err)

	assignment := func(s *Server) ([]string, *DesiredClusterInfoStatus) {
		sjs := s.getJetStream()
		sjs.mu.RLock()
		defer sjs.mu.RUnlock()
		sa := sjs.streamAssignment(globalAccountName, "TEST")
		if sa == nil || sa.Group == nil {
			return nil, nil
		}
		return copyStrings(sa.Group.Peers), sa.Group.migration
	}

	// Wait for the group to be extended with the new peer.
	checkFor(t, 5*time.Second, 100*time.Millisecond, func() error {
		if peers, _ := assignment(sl); len(peers) != 4 {
			return fmt.Errorf("expected 4 peers, got %d", len(peers))
		}
		return nil
	})

	// The old peer must stay while a quorum of desired peers can't vouch for their data.
	requireHeld := func() {
		t.Helper()
		var held bool
		for deadline := time.Now().Add(2 * time.Second); time.Now().Before(deadline); {
			peers, status := assignment(sl)
			require_Len(t, len(peers), 4)
			require_True(t, slices.Contains(peers, sl.NodeName()))
			require_True(t, c.streamLeader(globalAccountName, "TEST") == sl)
			if status != nil && status.Type == MigrationStatusCatchup {
				held = true
			}
			time.Sleep(100 * time.Millisecond)
		}
		require_True(t, held)
	}

	// Only the new peer vouches for its data, which is not a quorum of the desired peers.
	checkFor(t, 5*time.Second, 100*time.Millisecond, func() error {
		_, err := ns.globalAccount().lookupStream("TEST")
		return err
	})
	waitAnswered(answer(ns, true), 2)
	requireHeld()

	// Once the new peer is down, its earlier answer must not count towards a quorum anymore.
	ns.Shutdown()
	lmset, err := sl.globalAccount().lookupStream("TEST")
	require_NoError(t, err)
	checkFor(t, 2*time.Second, 50*time.Millisecond, func() error {
		if lmset.raftNode().IsFollowerCaughtUp(ns.NodeName()) {
			return errors.New("new peer still caught up")
		}
		return nil
	})
	waitAnswered(answer(followers[0], true), 1)
	requireHeld()

	// Once the new peer is back, it forms a quorum with the answering follower and the old peer is removed.
	// The other follower still doesn't answer, but it must not block the move.
	c.restartServer(ns)
	checkFor(t, 20*time.Second, 200*time.Millisecond, func() error {
		l := c.streamLeader(globalAccountName, "TEST")
		if l == nil {
			return errors.New("no stream leader")
		}
		if peers, _ := assignment(l); len(peers) != 3 || slices.Contains(peers, sl.NodeName()) {
			return fmt.Errorf("old peer still in %v", peers)
		}
		return nil
	})
}

func TestJetStreamClusterConsumerMoveWaitsForDesiredQuorum(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "R6S", 6)
	defer c.shutdown()

	nc, js := jsClientConnect(t, c.randomServer())
	defer nc.Close()

	_, err := js.AddStream(&nats.StreamConfig{Name: "TEST", Subjects: []string{"foo"}, Replicas: 3})
	require_NoError(t, err)
	_, err = js.AddConsumer("TEST", &nats.ConsumerConfig{Durable: "CONSUMER", AckPolicy: nats.AckExplicitPolicy})
	require_NoError(t, err)
	nc.Close()

	// Move onto the servers not hosting the stream yet, while two of them are down.
	var desired []string
	var down []*Server
	for _, s := range c.servers {
		if s.JetStreamIsStreamAssigned(globalAccountName, "TEST") {
			continue
		}
		desired = append(desired, s.NodeName())
		if len(down) < 2 {
			down = append(down, s)
		}
	}
	require_Len(t, len(desired), 3)
	for _, s := range down {
		s.Shutdown()
	}
	c.waitOnLeader()

	// Have the meta leader start moving the stream, and with it the consumer, onto the desired peers.
	ml := c.leader()
	mjs := ml.getJetStream()
	cc := mjs.cluster
	mjs.mu.Lock()
	sa := mjs.streamAssignment(globalAccountName, "TEST")
	require_NotNil(t, sa)
	origin := copyStrings(sa.Group.Peers)
	nsa := sa.copyGroup()
	nsa.Reply = _EMPTY_
	nsa.Group.Desired = &desiredRaftGroup{
		Created: time.Now().UTC(),
		ID:      nuid.Next(),
		Peers:   desired,
		Origin: &desiredRaftGroupOrigin{
			Peers:    origin,
			Replicas: 3,
		},
	}
	err = cc.meta.Propose(cc.term, encodeUpdateStreamAssignment(nsa))
	cc.trackInflightStreamProposal(globalAccountName, nsa, false)
	mjs.mu.Unlock()
	require_NoError(t, err)

	assignment := func() ([]string, []string, *DesiredClusterInfoStatus) {
		cl := c.consumerLeader(globalAccountName, "TEST", "CONSUMER")
		if cl == nil {
			return nil, nil, nil
		}
		sjs := cl.getJetStream()
		sjs.mu.RLock()
		defer sjs.mu.RUnlock()
		sa := sjs.streamAssignment(globalAccountName, "TEST")
		ca := sjs.consumerAssignment(globalAccountName, "TEST", "CONSUMER")
		if sa == nil || sa.Group == nil || ca == nil || ca.Group == nil {
			return nil, nil, nil
		}
		return copyStrings(sa.Group.Peers), copyStrings(ca.Group.Peers), ca.Group.migration
	}

	// Wait for the consumer group to be extended with all desired peers.
	checkFor(t, 5*time.Second, 200*time.Millisecond, func() error {
		if _, peers, _ := assignment(); len(peers) != 6 {
			return fmt.Errorf("expected 6 consumer peers, got %d", len(peers))
		}
		return nil
	})

	// Only one desired peer is caught up, which is not a quorum of the desired peers.
	// The old peers could still form a quorum with it, but must not be removed.
	var held bool
	for deadline := time.Now().Add(2 * time.Second); time.Now().Before(deadline); {
		_, peers, status := assignment()
		require_Len(t, len(peers), 6)
		if status != nil && status.Type == MigrationStatusCatchup {
			held = true
		}
		time.Sleep(100 * time.Millisecond)
	}
	require_True(t, held)

	// Once the desired peers are back, the consumer and then the stream complete the move.
	for _, s := range down {
		c.restartServer(s)
	}
	slices.Sort(desired)
	checkFor(t, 10*time.Second, 200*time.Millisecond, func() error {
		speers, cpeers, _ := assignment()
		slices.Sort(speers)
		slices.Sort(cpeers)
		if !slices.Equal(cpeers, desired) {
			return fmt.Errorf("consumer peers %v, expected %v", cpeers, desired)
		}
		if !slices.Equal(speers, desired) {
			return fmt.Errorf("stream peers %v, expected %v", speers, desired)
		}
		return nil
	})
}

func TestJetStreamClusterStreamPositionBookkeeping(t *testing.T) {
	mset := &stream{srv: &Server{}}
	pos := &peerPositions{}

	// Without an inbox to answer on, there is nothing to ask.
	pos.request(mset, globalAccountName, "TEST")
	require_True(t, pos.asked.IsZero())
	require_False(t, pos.isCaughtUp("A"))

	// Short of the bar is not good enough.
	pos.bar = 100
	require_False(t, pos.record(&clusterStreamInfoResponse{Peer: "C", State: &StreamState{LastSeq: 99}}))
	require_False(t, pos.isCaughtUp("C"))

	require_True(t, pos.record(&clusterStreamInfoResponse{Peer: "A", State: &StreamState{LastSeq: 100}}))
	require_True(t, pos.isCaughtUp("A"))
	require_False(t, pos.record(&clusterStreamInfoResponse{Peer: "A", State: &StreamState{LastSeq: 5000}}))
	require_True(t, pos.isCaughtUp("A"))

	// An answer carrying no state says nothing and changes nothing.
	require_False(t, pos.record(&clusterStreamInfoResponse{Peer: "A"}))
	require_True(t, pos.isCaughtUp("A"))
	require_False(t, pos.record(&clusterStreamInfoResponse{Peer: "B"}))
	require_False(t, pos.isCaughtUp("B"))

	// An older server answers with a full stream info, which unmarshals into a state
	// without a peer. We can't attribute that to anyone, so it's ignored.
	require_False(t, pos.record(&clusterStreamInfoResponse{State: &StreamState{LastSeq: 5000}}))
	require_Len(t, len(pos.seqs), 2)

	// Now with an inbox to answer on, so asks actually go out.
	pos = &peerPositions{reply: syncReplySubject()}
	pos.request(mset, globalAccountName, "TEST")
	first := pos.asked
	require_False(t, first.IsZero())
	require_False(t, pos.retried)

	// The first retry waits a fraction of the interval.
	pos.request(mset, globalAccountName, "TEST")
	require_Equal(t, pos.asked, first)
	require_False(t, pos.retried)
	pos.asked = time.Now().Add(-migratePosAskInterval / 5)
	first = pos.asked
	pos.request(mset, globalAccountName, "TEST")
	require_True(t, pos.retried)
	require_True(t, pos.asked.After(first))

	// But not for the next one.
	second := pos.asked
	pos.request(mset, globalAccountName, "TEST")
	require_Equal(t, pos.asked, second)

	// Until the interval has passed.
	pos.asked = time.Now().Add(-migratePosAskInterval)
	pos.request(mset, globalAccountName, "TEST")
	require_True(t, pos.asked.After(second))
}

func TestJetStreamClusterCanRemovePeer(t *testing.T) {
	// Peers are "A" (us), then uppercase for current and heard from, lowercase for down.
	peers := func(spec string) []*Peer {
		var actual []*Peer
		for _, id := range spec {
			switch {
			case id == 'A':
				// We always count ourselves, without having heard from us.
				actual = append(actual, &Peer{ID: "A", Current: true})
			case id >= 'a' && id <= 'z':
				actual = append(actual, &Peer{ID: strings.ToUpper(string(id))})
			default:
				actual = append(actual, &Peer{ID: string(id), Current: true, Last: time.Now()})
			}
		}
		return actual
	}
	lagging := &Peer{ID: "L", Last: time.Now()}
	unheard := &Peer{ID: "U", Current: true, Last: time.Now().Add(-time.Hour)}

	s := &Server{}
	for _, test := range []struct {
		name   string
		actual []*Peer
		remove string
		ok     bool
	}{
		{"R3 all up", peers("ABC"), "B", true},
		{"R3 removing the other live peer", peers("ABc"), "B", false},
		{"R3 removing a peer that's down", peers("ABc"), "C", true},
		{"R4 removing ourselves", peers("ABCD"), "A", true},
		{"R4 removing ourselves with one live peer left", peers("ABcd"), "A", false},
		{"R5 removing a live peer leaves no quorum", peers("ABcDe"), "D", false},
		{"R5 removing a peer that's down", peers("ABcDe"), "E", true},
		{"R6 needs three voters after the removal", peers("ABCdef"), "D", true},
		{"R6 short of three voters after the removal", peers("ABCdef"), "B", false},
		{"lagging peer doesn't count", append(peers("AB"), lagging), "B", false},
		{"unheard peer doesn't count", append(peers("AB"), unheard), "B", false},
	} {
		t.Run(test.name, func(t *testing.T) {
			require_Equal(t, s.canRemovePeer("A", test.remove, test.actual), test.ok)
		})
	}
}

func TestJetStreamClusterStreamMoveHoldsWhileDesiredPeerCatchesUp(t *testing.T) {
	askInterval, stall := migratePosAskInterval, migrateCatchupStall
	migratePosAskInterval, migrateCatchupStall = 250*time.Millisecond, time.Second
	defer func() { migratePosAskInterval, migrateCatchupStall = askInterval, stall }()

	c := createJetStreamClusterExplicit(t, "R4S", 4)
	defer c.shutdown()

	nc, js := jsClientConnect(t, c.randomServer())
	defer nc.Close()

	_, err := js.AddStream(&nats.StreamConfig{Name: "TEST", Subjects: []string{"foo"}, Replicas: 3})
	require_NoError(t, err)
	for range 100 {
		sendStreamMsg(t, nc, "foo", "hello")
	}
	checkFor(t, 2*time.Second, 200*time.Millisecond, func() error {
		return checkState(t, c, globalAccountName, "TEST")
	})

	// Move away from the stream leader, onto the server not hosting the stream yet.
	sl := c.streamLeader(globalAccountName, "TEST")
	var desired []string
	var followers []*Server
	var ns *Server
	for _, s := range c.servers {
		if s == sl {
			continue
		}
		desired = append(desired, s.NodeName())
		if s.JetStreamIsStreamAssigned(globalAccountName, "TEST") {
			followers = append(followers, s)
		} else {
			ns = s
		}
	}
	require_Len(t, len(followers), 2)
	require_NotNil(t, ns)

	// answerWith has the server answer requests for the state of its stream store with h, or not at all.
	answerWith := func(s *Server, h func(mset *stream, reply string)) {
		mset, err := s.globalAccount().lookupStream("TEST")
		require_NoError(t, err)
		mset.mu.Lock()
		defer mset.mu.Unlock()
		s.sysUnsubscribe(mset.infoSub)
		if h == nil {
			return
		}
		isubj := fmt.Sprintf(clusterStreamInfoT, mset.jsa.acc(), mset.cfg.Name)
		mset.infoSub, err = s.systemSubscribe(isubj, _EMPTY_, false, mset.sysc, func(sub *subscription, c *client, acc *Account, subject, reply string, msg []byte) {
			var req clusterStreamInfoRequest
			if json.Unmarshal(msg, &req) != nil || !req.State {
				mset.handleClusterStreamInfoRequest(sub, c, acc, subject, reply, msg)
				return
			}
			h(mset, reply)
		})
		require_NoError(t, err)
	}

	// Keep the followers from vouching for their data until the new peer reports on its terms.
	for _, s := range followers {
		answerWith(s, nil)
	}

	ml := c.leader()
	mjs := ml.getJetStream()
	cc := mjs.cluster
	mjs.mu.Lock()
	sa := mjs.streamAssignment(globalAccountName, "TEST")
	require_NotNil(t, sa)
	nsa := sa.copyGroup()
	nsa.Reply = _EMPTY_
	nsa.Group.Desired = &desiredRaftGroup{
		Created: time.Now().UTC(),
		ID:      nuid.Next(),
		Peers:   desired,
		Origin: &desiredRaftGroupOrigin{
			Peers:    copyStrings(sa.Group.Peers),
			Replicas: 3,
		},
	}
	err = cc.meta.Propose(cc.term, encodeUpdateStreamAssignment(nsa))
	if err == nil {
		cc.trackInflightStreamProposal(globalAccountName, nsa, false)
	}
	mjs.mu.Unlock()
	require_NoError(t, err)

	// Have the new peer report a store that's behind, making progress while progress is set.
	var progress atomic.Bool
	progress.Store(true)
	checkFor(t, 2*time.Second, 100*time.Millisecond, func() error {
		_, err := ns.globalAccount().lookupStream("TEST")
		return err
	})
	var seq uint64
	answerWith(ns, func(mset *stream, reply string) {
		if progress.Load() {
			seq++
		}
		state := StreamState{LastSeq: seq}
		ns.sendInternalMsgLocked(reply, _EMPTY_, nil, &clusterStreamInfoResponse{Peer: mset.raftNode().ID(), State: &state})
	})

	// The followers vouch for their data, which is a quorum of the desired peers.
	for _, s := range followers {
		answerWith(s, (*stream).processClusterStreamStateRequest)
	}

	group := func() ([]string, *DesiredClusterInfoStatus) {
		sjs := sl.getJetStream()
		sjs.mu.RLock()
		defer sjs.mu.RUnlock()
		sa := sjs.streamAssignment(globalAccountName, "TEST")
		if sa == nil || sa.Group == nil {
			return nil, nil
		}
		return copyStrings(sa.Group.Peers), sa.Group.migration
	}

	// While the new peer makes progress, the old leader stays, for longer than a stall.
	checkFor(t, 10*time.Second, 100*time.Millisecond, func() error {
		if peers, _ := group(); len(peers) != 4 {
			return fmt.Errorf("expected 4 peers, got %d", len(peers))
		}
		return nil
	})
	for deadline := time.Now().Add(3 * migrateCatchupStall); time.Now().Before(deadline); {
		peers, _ := group()
		require_True(t, slices.Contains(peers, sl.NodeName()))
		require_True(t, c.streamLeader(globalAccountName, "TEST") == sl)
		time.Sleep(100 * time.Millisecond)
	}

	// Once it stalls, the move goes ahead without it.
	progress.Store(false)
	checkFor(t, 20*time.Second, 200*time.Millisecond, func() error {
		if peers, _ := group(); len(peers) != 3 || slices.Contains(peers, sl.NodeName()) {
			return fmt.Errorf("old peer still in %v", peers)
		}
		return nil
	})
}

func TestJetStreamClusterStreamMigrationHoldsForCatchup(t *testing.T) {
	now := time.Now()
	actual := []*Peer{{ID: "A"}, {ID: "B", Last: now}, {ID: "C", Last: now}, {ID: "D", Last: now}}
	desired := []string{"B", "C", "D"}
	pos := &peerPositions{bar: 100, seqs: map[string]uint64{"B": 100, "C": 100}}

	report := func(peer string, seq uint64) {
		pos.record(&clusterStreamInfoResponse{Peer: peer, State: &StreamState{LastSeq: seq}})
	}

	// Nobody is catching up.
	require_False(t, pos.holdForCatchup("A", "A", actual, desired, nil))

	// A desired peer we're catching up holds the removal, as does one that told us it's behind.
	require_True(t, pos.holdForCatchup("A", "A", actual, desired, []string{"D"}))
	report("D", 10)
	require_True(t, pos.holdForCatchup("A", "A", actual, desired, nil))

	// Once it stalls, we stop waiting for it, also if it reports the same again.
	pos.progress = time.Now().Add(-migrateCatchupStall)
	report("D", 10)
	require_False(t, pos.holdForCatchup("A", "A", actual, desired, nil))

	// Until it makes progress again.
	report("D", 20)
	require_True(t, pos.holdForCatchup("A", "A", actual, desired, nil))

	// Removing a peer that's down doesn't cost us a copy.
	require_False(t, pos.holdForCatchup("A", "X", append(actual, &Peer{ID: "X"}), desired, nil))
	require_True(t, pos.progress.IsZero())

	// A desired peer that's down, or not desired at all, doesn't hold the removal.
	actual[3].Last = time.Time{}
	require_False(t, pos.holdForCatchup("A", "A", actual, desired, []string{"D"}))
	actual[3].Last = now
	require_False(t, pos.holdForCatchup("A", "A", actual, []string{"B", "C"}, []string{"D"}))

	// Once caught up, it no longer holds the removal.
	report("D", 100)
	require_False(t, pos.holdForCatchup("A", "A", actual, desired, nil))
}

func TestJetStreamClusterPlacementPrefersCaughtUpPeers(t *testing.T) {
	c := createJetStreamClusterWithTemplateAndModHook(t, jsClusterTempl, "R3S", 3,
		func(serverName, _, _, conf string) string {
			return fmt.Sprintf("%s\nserver_tags: [server:%s]", conf, serverName)
		})
	defer c.shutdown()

	nc, js := jsClientConnect(t, c.randomServer())
	defer nc.Close()

	_, err := js.AddStream(&nats.StreamConfig{Name: "TEST", Subjects: []string{"foo"}, Replicas: 3})
	require_NoError(t, err)

	// Have the meta leader believe it's catching up rs.
	ml, rs := c.leader(), c.randomNonLeader()
	meta := ml.getJetStream().getMetaGroup().(*raft)
	rsID := rs.getJetStream().getMetaGroup().ID()
	meta.Lock()
	meta.peers[rsID].ci = math.MaxUint64
	meta.Unlock()
	require_False(t, meta.IsFollowerCaughtUp(rsID))

	// Responses come from the stream/consumer leader, the meta leader might not have applied the assignment yet.
	isMember := func(ci *nats.ClusterInfo) bool {
		if ci.Leader == rs.Name() {
			return true
		}
		return slices.ContainsFunc(ci.Replicas, func(pi *nats.PeerInfo) bool { return pi.Name == rs.Name() })
	}
	for i := range 10 {
		// R1 streams and consumers aren't placed on the peer that's catching up.
		name := fmt.Sprintf("S%d", i)
		si, err := js.AddStream(&nats.StreamConfig{Name: name, Subjects: []string{name}, Replicas: 1})
		require_NoError(t, err)
		require_False(t, isMember(si.Cluster))
		ci, err := js.AddConsumer("TEST", &nats.ConsumerConfig{Durable: fmt.Sprintf("C%d", i), Replicas: 1})
		require_NoError(t, err)
		require_False(t, isMember(ci.Cluster))
	}

	// But it's not ruled out.
	si, err := js.AddStream(&nats.StreamConfig{Name: "PINNED", Subjects: []string{"pinned"}, Replicas: 1,
		Placement: &nats.Placement{Tags: []string{"server:" + rs.Name()}}})
	require_NoError(t, err)
	require_True(t, isMember(si.Cluster))
	si, err = js.AddStream(&nats.StreamConfig{Name: "R3", Subjects: []string{"r3"}, Replicas: 3})
	require_NoError(t, err)
	require_True(t, isMember(si.Cluster))
}

// A replica restarting after its stream's group was renamed must start a monitor for the new group.
func TestJetStreamClusterStreamGroupRenamedWhileServerDownStartsMonitor(t *testing.T) {
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

	// Take down a follower that is neither the stream nor the meta leader.
	sl, ml := c.streamLeader(globalAccountName, "TEST"), c.leader()
	var rs *Server
	for _, s := range c.servers {
		if s != sl && s != ml {
			rs = s
			break
		}
	}
	require_NotNil(t, rs)
	mset, err := rs.globalAccount().lookupStream("TEST")
	require_NoError(t, err)
	oldGroup := mset.raftNode().Group()
	rs.Shutdown()
	rs.WaitForShutdown()
	nc.Close()
	nc, js = jsClientConnect(t, sl)
	defer nc.Close()

	// An idempotent create retry re-proposes the assignment as an add.
	_, err = js.AddStream(cfg)
	require_NoError(t, err)

	// Scale down to R1 and back to R3 while the server is down, renaming the group.
	cfg.Replicas = 1
	_, err = js.UpdateStream(cfg)
	require_NoError(t, err)
	c.waitOnStreamLeader(globalAccountName, "TEST")
	cfg.Replicas = 3
	_, err = js.UpdateStream(cfg)
	require_NoError(t, err)
	c.waitOnStreamLeader(globalAccountName, "TEST")

	lmset, err := c.streamLeader(globalAccountName, "TEST").globalAccount().lookupStream("TEST")
	require_NoError(t, err)
	newGroup := lmset.raftNode().Group()
	require_NotEqual(t, oldGroup, newGroup)

	rs = c.restartServer(rs)
	c.waitOnServerCurrent(rs)

	checkFor(t, 5*time.Second, 100*time.Millisecond, func() error {
		mset, err := rs.globalAccount().lookupStream("TEST")
		if err != nil {
			return err
		}
		n := mset.raftNode()
		if n == nil {
			return errors.New("no raft node")
		}
		if n.Group() != newGroup {
			return fmt.Errorf("raft group %q, expected %q", n.Group(), newGroup)
		}
		if !mset.isMonitorRunning() {
			return errors.New("stream monitor not running")
		}
		return nil
	})
	c.waitOnStreamCurrent(rs, globalAccountName, "TEST")
	c.waitOnServerHealthz(rs)
}

// A raised limit that was applied must not be undone on restart, the store would drop messages.
func TestJetStreamClusterRaisedLimitSurvivesHardKill(t *testing.T) {
	for _, replicas := range []int{1, 3} {
		t.Run(fmt.Sprintf("R%d", replicas), func(t *testing.T) {
			c := createJetStreamClusterExplicit(t, "R3S", 3)
			defer c.shutdown()

			nc, js := jsClientConnect(t, c.randomServer())
			defer nc.Close()

			cfg := &nats.StreamConfig{Name: "TEST", Subjects: []string{"foo"}, Replicas: replicas, MaxMsgs: 10}
			_, err := js.AddStream(cfg)
			require_NoError(t, err)

			// Raising the limit is the last meta entry before the kill.
			cfg.MaxMsgs = 100
			_, err = js.UpdateStream(cfg)
			require_NoError(t, err)
			for range 50 {
				_, err = js.Publish("foo", nil)
				require_NoError(t, err)
			}
			checkFor(t, 5*time.Second, 50*time.Millisecond, func() error {
				for _, s := range c.servers {
					mset, err := s.globalAccount().lookupStream("TEST")
					if err != nil {
						continue
					}
					if msgs := mset.state().Msgs; msgs != 50 {
						return fmt.Errorf("%s has %d msgs", s, msgs)
					}
				}
				return nil
			})
			nc.Close()

			// Simulate a hard kill of all servers.
			copies := make(map[string]string)
			for _, s := range c.servers {
				copySd := path.Join(t.TempDir(), JetStreamStoreDir)
				require_NoError(t, copyDir(t, copySd, s.StoreDir()))
				copies[s.StoreDir()] = copySd
			}
			c.stopAll()
			for sd, copySd := range copies {
				require_NoError(t, os.RemoveAll(sd))
				require_NoError(t, copyDir(t, sd, copySd))
			}
			c.restartAll()
			c.waitOnStreamLeader(globalAccountName, "TEST")

			nc, js = jsClientConnect(t, c.randomServer())
			defer nc.Close()
			checkFor(t, 10*time.Second, 200*time.Millisecond, func() error {
				si, err := js.StreamInfo("TEST")
				if err != nil {
					return err
				}
				if si.Config.MaxMsgs != 100 {
					return fmt.Errorf("max msgs %d", si.Config.MaxMsgs)
				}
				return nil
			})
			si, err := js.StreamInfo("TEST")
			require_NoError(t, err)
			require_Equal(t, si.State.Msgs, 50)
		})
	}
}

func TestJetStreamClusterRestartedServerKeepsLastAppliedStreamGroup(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "R3S", 3)
	defer c.shutdown()

	nc, js := jsClientConnect(t, c.randomServer())
	defer nc.Close()

	si, err := js.AddStream(&nats.StreamConfig{Name: "TEST", Subjects: []string{"foo"}, Replicas: 1})
	require_NoError(t, err)
	sl := c.serverByName(si.Cluster.Leader)
	mset, err := sl.globalAccount().lookupStream("TEST")
	require_NoError(t, err)
	r1Group := mset.raftGroup().Name

	// Scale up, the remap to the R3 group is applied first.
	cfg := &StreamConfig{Name: "TEST", Subjects: []string{"foo"}, Replicas: 3, Storage: FileStorage}
	req, err := json.Marshal(cfg)
	require_NoError(t, err)
	require_NoError(t, nc.Publish(fmt.Sprintf(JSApiStreamUpdateT, "TEST"), req))
	var r3Group string
	checkFor(t, 2*time.Second, time.Millisecond, func() error {
		if rg := mset.raftGroup(); rg != nil && rg.Name != r1Group {
			r3Group = rg.Name
			return nil
		}
		return errors.New("remap not applied yet")
	})

	// Simulate a hard kill of the stream's server and one other right after the remap was applied.
	var other *Server
	for _, s := range c.servers {
		if s != sl {
			other = s
			break
		}
	}
	copies := make(map[string]string)
	for _, s := range []*Server{sl, other} {
		copySd := path.Join(t.TempDir(), JetStreamStoreDir)
		require_NoError(t, copyDir(t, copySd, s.StoreDir()))
		copies[s.StoreDir()] = copySd
	}
	nc.Close()
	c.stopAll()
	for sd, copySd := range copies {
		require_NoError(t, os.RemoveAll(sd))
		require_NoError(t, copyDir(t, sd, copySd))
	}

	// Restart only those two, recovering before they have a meta leader.
	sl = c.restartServer(sl)
	c.restartServer(other)
	checkFor(t, 10*time.Second, time.Millisecond, func() error {
		if mset, err = sl.globalAccount().lookupStream("TEST"); err != nil {
			return err
		}
		if mset.raftGroup() == nil {
			return errors.New("stream not assigned yet")
		}
		return nil
	})
	// The remap was applied before the kill, the stream must not go back to its R1 group.
	require_Equal(t, mset.raftGroup().Name, r3Group)
}

// A group scaling up from its only member must snapshot what its store holds after a restart.
func TestJetStreamClusterScaleUpFromOneSnapshotsStoreAfterRestart(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "R3S", 3)
	defer c.shutdown()

	nc, js := jsClientConnect(t, c.randomServer())
	defer nc.Close()
	_, err := js.AddStream(&nats.StreamConfig{Name: "TEST", Subjects: []string{"foo"}, Replicas: 1})
	require_NoError(t, err)
	for range 5 {
		_, err = js.Publish("foo", nil)
		require_NoError(t, err)
	}

	// The source restarts below, so it mustn't be the meta leader.
	sl := c.streamLeader(globalAccountName, "TEST")
	var ml *Server
	checkFor(t, 10*time.Second, 100*time.Millisecond, func() error {
		if ml = c.leader(); ml == nil || ml == sl {
			if ml != nil {
				ml.getJetStream().getMetaGroup().StepDown()
			}
			return errors.New("meta leader is unknown or the source")
		}
		return nil
	})
	mljs := ml.getJetStream()
	nc, js = jsClientConnect(t, ml)
	defer nc.Close()

	// Hold the scale up before the assignment is extended onto the new peers.
	var dropExpand atomic.Bool
	dropExpand.Store(true)
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
			if json.Unmarshal(msg, &reconcile) == nil && dropExpand.Load() && len(reconcile.MetaPeers) > 1 {
				return
			}
			mljs.reconcileDesiredStreamAssignment(sub, c, acc, subject, reply, msg)
		})
	mljs.mu.Unlock()
	require_NoError(t, err)

	_, err = js.UpdateStream(&nats.StreamConfig{Name: "TEST", Subjects: []string{"foo"}, Replicas: 3})
	require_NoError(t, err)

	// The source snapshots for the scale up, and stays the only member.
	mset, err := sl.globalAccount().lookupStream("TEST")
	require_NoError(t, err)
	checkFor(t, 5*time.Second, 50*time.Millisecond, func() error {
		if n := mset.raftNode(); n == nil || !n.Leader() || n.NeedSnapshot() {
			return errors.New("source has no scale up snapshot yet")
		}
		return nil
	})

	// Store writes neither the snapshot nor the log hold.
	for range 2 {
		_, _, err = mset.store.StoreMsg("foo", nil, nil, 0)
		require_NoError(t, err)
	}
	require_NoError(t, mset.flushAllPending())

	// Hard kill the source, a clean shutdown would snapshot on the way out.
	copySd := path.Join(t.TempDir(), JetStreamStoreDir)
	require_NoError(t, copyDir(t, copySd, sl.StoreDir()))
	sl.Shutdown()
	sl.WaitForShutdown()
	require_NoError(t, os.RemoveAll(sl.StoreDir()))
	require_NoError(t, copyDir(t, sl.StoreDir(), copySd))
	c.restartServer(sl)

	// Not waitOnStreamLeader, it waits for the scale up we're still holding.
	checkFor(t, 10*time.Second, 50*time.Millisecond, func() error {
		if c.streamLeader(globalAccountName, "TEST") == nil {
			return errors.New("no stream leader yet")
		}
		return nil
	})
	dropExpand.Store(false)
	checkFor(t, 20*time.Second, 250*time.Millisecond, func() error {
		ml := c.leader()
		if ml == nil {
			return errors.New("no meta leader")
		}
		mljs := ml.getJetStream()
		mljs.mu.RLock()
		defer mljs.mu.RUnlock()
		if sa := mljs.streamAssignment(globalAccountName, "TEST"); sa == nil || sa.Group.Desired != nil {
			return errors.New("scale up not done yet")
		}
		return nil
	})
	for _, s := range c.servers {
		c.waitOnStreamCurrent(s, globalAccountName, "TEST")
	}
	// Without a new write that could make a replica notice it's behind.
	for _, s := range c.servers {
		mset, err := s.globalAccount().lookupStream("TEST")
		require_NoError(t, err)
		if state := mset.state(); state.Msgs != 7 || state.LastSeq != 7 {
			t.Fatalf("server %s has %d msgs, last %d", s.Name(), state.Msgs, state.LastSeq)
		}
	}
}

func TestJetStreamClusterPlacementPeersWhileConverging(t *testing.T) {
	origin := []string{"A1", "A2", "A3"}
	target := []string{"B1", "B2", "B3"}
	moving := append(copyStrings(origin), target...)
	for _, test := range []struct {
		name    string
		peers   []string
		desired *desiredRaftGroup
		want    []string
	}{
		{
			name:  "stable",
			peers: origin,
			want:  origin,
		},
		{
			name:    "no members recorded",
			peers:   moving,
			desired: &desiredRaftGroup{Peers: target, Move: true, Origin: &desiredRaftGroupOrigin{Peers: origin}},
			want:    moving,
		},
		{
			name:    "move before quorum",
			peers:   moving,
			desired: &desiredRaftGroup{Peers: target, Move: true, Origin: &desiredRaftGroupOrigin{Peers: origin}, Members: append(copyStrings(origin), "B1")},
			want:    origin,
		},
		{
			name:    "move after quorum",
			peers:   moving,
			desired: &desiredRaftGroup{Peers: target, Move: true, Origin: &desiredRaftGroupOrigin{Peers: origin}, Members: append(copyStrings(origin), "B1", "B2")},
			want:    target,
		},
		{
			name:    "move keeps overlapping origin peer",
			peers:   []string{"A1", "A2", "A3", "B1", "B2"},
			desired: &desiredRaftGroup{Peers: []string{"A1", "B1", "B2"}, Move: true, Origin: &desiredRaftGroupOrigin{Peers: origin}, Members: origin},
			want:    origin,
		},
		{
			name:    "overlapping move before its new peer is added",
			peers:   origin,
			desired: &desiredRaftGroup{Peers: []string{"A2", "A3", "B1"}, Move: true, Origin: &desiredRaftGroupOrigin{Peers: origin}, Members: origin},
			want:    origin,
		},
		{
			name:    "cancel move back to origin",
			peers:   moving,
			desired: &desiredRaftGroup{Peers: origin, Move: true, CancelMove: true, Origin: &desiredRaftGroupOrigin{Peers: origin}, Members: append(copyStrings(origin), "B1")},
			want:    origin,
		},
		{
			name:    "scale up before quorum",
			peers:   []string{"A1", "B1", "C1"},
			desired: &desiredRaftGroup{Peers: []string{"A1", "B1", "C1"}, Origin: &desiredRaftGroupOrigin{Peers: []string{"A1"}}, Members: []string{"A1"}},
			want:    []string{"A1", "B1", "C1"},
		},
		{
			name:    "scale up after quorum",
			peers:   []string{"A1", "B1", "C1"},
			desired: &desiredRaftGroup{Peers: []string{"A1", "B1", "C1"}, Origin: &desiredRaftGroupOrigin{Peers: []string{"A1"}}, Members: []string{"A1", "B1"}},
			want:    []string{"A1", "B1", "C1"},
		},
		{
			name:    "scale down",
			peers:   moving,
			desired: &desiredRaftGroup{Peers: moving, ScaleDown: true, Members: origin},
			want:    moving,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			sa := &streamAssignment{Group: &raftGroup{Peers: test.peers, Desired: test.desired}}
			require_True(t, slices.Equal(sa.placementPeers(), test.want))
		})
	}
}

func TestJetStreamClusterRemapConsumerHeldUntilDesiredMembersQuorum(t *testing.T) {
	origin := []string{"A1", "A2", "A3"}
	target := []string{"B1", "B2", "B3"}

	js := &jetStream{cluster: &jetStreamCluster{}}
	newAssignments := func(members, consumerPeers []string, replicas int) (*streamAssignment, *consumerAssignment) {
		sa := &streamAssignment{
			Config: &StreamConfig{Name: "TEST", Replicas: 3, Retention: LimitsPolicy},
			Group: &raftGroup{
				Name:  "S",
				Peers: append(copyStrings(origin), target...),
				Desired: &desiredRaftGroup{
					ID:      "ID",
					Peers:   target,
					Move:    true,
					Origin:  &desiredRaftGroupOrigin{Peers: origin, Replicas: 3},
					Members: members,
				},
			},
		}
		ca := &consumerAssignment{
			Name:   "CONSUMER",
			Stream: "TEST",
			Config: &ConsumerConfig{Durable: "CONSUMER", Replicas: replicas},
			Group:  &raftGroup{Name: "C", Peers: consumerPeers},
		}
		sa.consumers = map[string]*consumerAssignment{ca.Name: ca}
		js.cluster.streams = map[string]map[string]*streamAssignment{globalAccountName: {sa.Config.Name: sa}}
		return sa, ca
	}

	// Without a quorum of desired peers holding the stream's data, consumers stay put.
	sa, _ := newAssignments(append(copyStrings(origin), "B1"), []string{"A2"}, 1)
	consumers, deleted, done := js.remapConsumerAssignments(globalAccountName, sa)
	require_Len(t, len(consumers), 0)
	require_Len(t, len(deleted), 0)
	require_False(t, done)

	// A peer that must be dropped can't wait for that.
	sa, _ = newAssignments(append(copyStrings(origin), "B1"), []string{"A2"}, 1)
	sa.Group.Peers = slices.DeleteFunc(sa.Group.Peers, func(p string) bool { return p == "A2" })
	consumers, _, done = js.remapConsumerAssignments(globalAccountName, sa)
	require_Len(t, len(consumers), 1)
	require_False(t, done)
	require_False(t, slices.Contains(consumers[0].Group.Peers, "A2"))

	// A quorum of members doesn't count until all desired peers are stream peers.
	sa, _ = newAssignments(append(copyStrings(origin), "B1", "B2"), []string{"A2"}, 1)
	sa.Group.Peers = slices.DeleteFunc(sa.Group.Peers, func(p string) bool { return p == "B3" })
	consumers, _, done = js.remapConsumerAssignments(globalAccountName, sa)
	require_Len(t, len(consumers), 0)
	require_False(t, done)

	// Once a quorum are members, a R1 consumer only moves to a member.
	for range 20 {
		sa, _ = newAssignments(append(copyStrings(origin), "B1", "B2"), []string{"A2"}, 1)
		consumers, _, done = js.remapConsumerAssignments(globalAccountName, sa)
		require_Len(t, len(consumers), 1)
		require_False(t, done)
		desired := consumers[0].Group.Desired.Peers
		require_Len(t, len(desired), 1)
		require_True(t, desired[0] == "B1" || desired[0] == "B2")
		require_True(t, slices.Equal(consumers[0].Group.Peers, []string{"A2"}))
	}

	// A R3 consumer is placed on all desired peers, including the one that's still catching up.
	sa, _ = newAssignments(append(copyStrings(origin), "B1", "B2"), origin, 3)
	consumers, _, _ = js.remapConsumerAssignments(globalAccountName, sa)
	require_Len(t, len(consumers), 1)
	desired := copyStrings(consumers[0].Group.Desired.Peers)
	slices.Sort(desired)
	require_True(t, slices.Equal(desired, target))
}

func TestJetStreamClusterReconcileDesiredMembersOnlyGrow(t *testing.T) {
	newGroup := func() *raftGroup {
		return &raftGroup{
			Name:  "S",
			Peers: []string{"A", "B", "C", "D"},
			Desired: &desiredRaftGroup{
				ID:      "ID",
				Term:    1,
				Peers:   []string{"B", "C", "D"},
				Members: []string{"A", "B", "C"},
			},
		}
	}
	update := func(metaPeers, members []string) desiredAssignmentUpdate {
		return desiredAssignmentUpdate{ID: "ID", Term: 1, MetaPeers: metaPeers, Members: members}
	}

	// A member that isn't reported stays, a newly reported one is added.
	ng := newGroup().reconcileDesiredState(update([]string{"A", "B", "C", "D"}, []string{"A", "D"}), 3, false)
	require_NotNil(t, ng)
	require_True(t, slices.Equal(ng.Desired.Members, []string{"A", "B", "C", "D"}))

	// Nothing changes if no new members are reported.
	ng = newGroup().reconcileDesiredState(update([]string{"A", "B", "C", "D"}, []string{"A"}), 3, false)
	require_True(t, ng == nil)

	// A member leaves together with the assignment.
	ng = newGroup().reconcileDesiredState(update([]string{"B", "C", "D"}, []string{"B"}), 3, false)
	require_NotNil(t, ng)
	require_True(t, slices.Equal(ng.Desired.Members, []string{"B", "C"}))

	// A reported peer that isn't part of the assignment isn't added.
	ng = newGroup().reconcileDesiredState(update([]string{"A", "B", "C"}, []string{"D"}), 3, false)
	require_NotNil(t, ng)
	require_True(t, slices.Equal(ng.Desired.Members, []string{"A", "B", "C"}))
}

func TestJetStreamClusterLegacyMoveSeedsDesiredMembers(t *testing.T) {
	rg := &raftGroup{Name: "S", Peers: []string{"A1", "A2", "A3", "B1", "B2", "B3"}}
	ng := rg.reconcileDesiredState(desiredAssignmentUpdate{Term: 1}, 3, false)
	require_NotNil(t, ng)
	require_NotNil(t, ng.Desired)
	require_True(t, ng.Desired.Move)
	require_True(t, slices.Equal(ng.Desired.Peers, []string{"B1", "B2", "B3"}))
	// Only the peers the legacy move started from are known to hold the data.
	require_True(t, slices.Equal(ng.Desired.Members, []string{"A1", "A2", "A3"}))
}

func TestJetStreamClusterStepDownCandidatesAndPreferMembers(t *testing.T) {
	actual := []*Peer{
		{ID: "L", Current: true},
		{ID: "B1", Current: true},
		{ID: "B2", Current: false},
		{ID: "B3", Current: true},
		{ID: "A2", Current: true},
	}
	desired := []string{"B1", "B2", "B3"}

	// Only current desired peers holding the stream's data, never ourselves.
	require_True(t, slices.Equal(stepDownCandidates("L", actual, desired, []string{"B1", "B2"}), []string{"B1"}))
	// Without members the stream isn't converging, so every current desired peer holds its data.
	require_True(t, slices.Equal(stepDownCandidates("L", actual, desired, nil), []string{"B1", "B3"}))

	// Desired peers that are members come first, the order is kept otherwise.
	d := &desiredRaftGroup{Peers: desired, Members: []string{"A1", "B2", "B3"}}
	peers := []string{"A1", "B1", "B2", "A2", "B3"}
	d.preferMembers(peers)
	require_True(t, slices.Equal(peers, []string{"B2", "B3", "A1", "B1", "A2"}))

	// Without members the order is left alone.
	peers = []string{"A1", "B1", "B2"}
	(&desiredRaftGroup{Peers: desired}).preferMembers(peers)
	require_True(t, slices.Equal(peers, []string{"A1", "B1", "B2"}))
}

func TestJetStreamClusterSetPreferredMember(t *testing.T) {
	s := &Server{}
	for _, p := range []string{"B1", "B2", "B3"} {
		s.nodeToInfo.Store(p, nodeInfo{})
	}
	s.nodeToInfo.Store("B4", nodeInfo{offline: true})
	s.nodeToInfo.Store("B5", nodeInfo{offline: true})

	// Only members are preferred while the stream converges.
	d := &desiredRaftGroup{Peers: []string{"B1", "B2", "B3"}, Members: []string{"B1", "B2"}}
	for range 50 {
		rg := &raftGroup{Peers: []string{"B1", "B2", "B3"}}
		rg.setPreferredMember(s, d)
		require_True(t, rg.Preferred == "B1" || rg.Preferred == "B2")
	}

	// An offline member is still preferred over an online peer that isn't a member.
	rg := &raftGroup{Peers: []string{"B3", "B4", "B5"}}
	rg.setPreferredMember(s, &desiredRaftGroup{Members: []string{"B4", "B5"}})
	require_True(t, rg.Preferred == "B4" || rg.Preferred == "B5")

	// Without a member there's no preferred leader.
	rg = &raftGroup{Peers: []string{"B1", "B2"}, Preferred: "B1"}
	rg.setPreferredMember(s, &desiredRaftGroup{Members: []string{"B3"}})
	require_Equal(t, rg.Preferred, _EMPTY_)

	// Without members the stream isn't converging, so any online peer works.
	seen := make(map[string]struct{})
	for range 100 {
		rg = &raftGroup{Peers: []string{"B1", "B2", "B3"}}
		rg.setPreferredMember(s, &desiredRaftGroup{Peers: []string{"B1", "B2", "B3"}})
		seen[rg.Preferred] = struct{}{}
	}
	require_Len(t, len(seen), 3)
}

func TestJetStreamClusterStreamDeleteAfterRestoreStall(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "R3S", 3)
	defer c.shutdown()
	l := &captureWarnLogger{warn: make(chan string, 16)}
	for _, s := range c.servers {
		s.SetLogger(l, false, false)
	}
	nc, js := jsClientConnect(t, c.randomServer())
	defer nc.Close()

	r, err := nc.Request(fmt.Sprintf(JSApiStreamRestoreT, "TEST"),
		[]byte(`{"config":{"name":"TEST","num_replicas":3,"storage":"file"}}`), 5*time.Second)
	require_NoError(t, err)
	var resp JSApiStreamRestoreResponse
	require_NoError(t, json.Unmarshal(r.Data, &resp))
	require_Equal(t, resp.Type, JSApiStreamRestoreResponseType)
	require_True(t, resp.Error == nil && resp.DeliverSubject != _EMPTY_)

	// Send no chunks. Wait for the watchdog to fail the restore and stop its leader.
	deadline := time.After(10 * time.Second)
waitForStall:
	for {
		select {
		case warning := <-l.warn:
			if strings.Contains(warning, "Stream restore failed for") && strings.Contains(warning, "is stalled") {
				break waitForStall
			}
		case <-deadline:
			t.Fatal("restore did not stall")
		}
	}

	// Deleting the failed assignment must reply even though its leader has stopped.
	require_NoError(t, js.DeleteStream("TEST", nats.MaxWait(time.Second)))
}

// Non-preferred restore members must wait for the preferred receiver to start.
// After it starts, a real snapshot must restore and all replicas must catch up.
func TestJetStreamClusterRestoreWaitsForPreferredReceiver(t *testing.T) {
	const streamName = "TEST-STREAM"

	// Create a stream snapshot
	source := RunBasicJetStreamServer(t)
	defer source.Shutdown()
	sourceNC, sourceJS := jsClientConnect(t, source)
	defer sourceNC.Close()
	_, err := sourceJS.AddStream(&nats.StreamConfig{Name: streamName})
	require_NoError(t, err)
	_, err = sourceJS.Publish(streamName, []byte("restored message"))
	require_NoError(t, err)
	mset, err := source.GlobalAccount().lookupStream(streamName)
	require_NoError(t, err)
	sr, err := mset.snapshot(5*time.Second, false, true)
	require_NoError(t, err)
	defer sr.Reader.Close()
	snapshot, err := io.ReadAll(sr.Reader)
	require_NoError(t, err)

	// Restore it in a 3 node cluster
	c := createJetStreamClusterExplicit(t, "R3S", 3)
	defer c.shutdown()
	nc, client := jsClientConnect(t, c.randomServer())
	defer nc.Close()
	inbox := nats.NewInbox()
	replies, err := nc.SubscribeSync(inbox)
	require_NoError(t, err)
	defer replies.Unsubscribe()
	require_NoError(t, nc.PublishRequest(fmt.Sprintf(JSApiStreamRestoreT, streamName), inbox,
		fmt.Appendf(nil, `{"config":{"name":%q,"num_replicas":3,"storage":"file"}}`, streamName)))

	// Lock the preferred the node that is expected to become the leader
	var locked *raft
	var group string
	deadline := time.Now().Add(2 * time.Second)
	for locked == nil && time.Now().Before(deadline) {
		for _, s := range c.servers {
			js, cc := s.getJetStreamCluster()
			if !js.mu.TryRLock() {
				continue
			}
			var preferred bool
			if sa := cc.streams[globalAccountName][streamName]; sa != nil {
				group = sa.Group.Name
				preferred = sa.Group.Preferred == cc.meta.ID()
			}
			js.mu.RUnlock()
			if !preferred {
				continue
			}
			if node := s.lookupRaftNode(group); node != nil {
				n := node.(*raft)
				if n.TryLock() {
					if n.State() != Closed && !n.Leader() {
						locked = n
						break
					}
					n.Unlock()
				}
			}
		}
		if locked == nil {
			time.Sleep(100 * time.Microsecond)
		}
	}

	require_NotNil(t, locked)
	// No replica should answer until the preferred receiver is released.
	_, err = replies.NextMsg(maxElectionTimeout + time.Second)

	// Release the preferred node and make sure we can restore
	locked.Unlock()
	require_Error(t, err, nats.ErrTimeout)
	r, err := replies.NextMsg(5 * time.Second)
	require_NoError(t, err)
	var response JSApiStreamRestoreResponse
	require_NoError(t, json.Unmarshal(r.Data, &response))
	require_Equal(t, response.Type, JSApiStreamRestoreResponseType)
	require_True(t, response.Error == nil && response.DeliverSubject != "")
	chunk, err := nc.Request(response.DeliverSubject, snapshot, time.Second)
	require_NoError(t, err)
	require_Equal(t, len(chunk.Data), 0)
	final, err := nc.Request(response.DeliverSubject, nil, 5*time.Second)
	require_NoError(t, err)
	var restored JSApiStreamCreateResponse
	require_NoError(t, json.Unmarshal(final.Data, &restored))
	require_True(t, restored.Error == nil)
	for _, s := range c.servers {
		c.waitOnStreamCurrent(s, globalAccountName, streamName)
		require_False(t, s.lookupRaftNode(group).IsObserver())
	}
	message, err := client.GetMsg(streamName, 1)
	require_NoError(t, err)
	require_Equal(t, string(message.Data), "restored message")
}

type delayedRestoreFailureLogger struct {
	DummyLogger
	sawFailure    atomic.Bool
	failurePaused chan struct{}
	monitorExited chan struct{}
	resumeFailure <-chan struct{}
}

func (l *delayedRestoreFailureLogger) Debugf(format string, _ ...any) {
	switch format {
	case "Stream restore failed: %v":
		if l.sawFailure.CompareAndSwap(false, true) {
			l.failurePaused <- struct{}{}
			<-l.resumeFailure
		}
	case "Exiting stream monitor for '%s > %s' [%s]":
		if l.sawFailure.Load() {
			select {
			case l.monitorExited <- struct{}{}:
			default:
			}
		}
	}
}

func TestJetStreamClusterRestoreFailureDoesNotDeleteReplacement(t *testing.T) {
	// Create a snapshot to restore into the cluster.
	source := RunBasicJetStreamServer(t)
	defer source.Shutdown()
	sourceNC, sourceJS := jsClientConnect(t, source)
	defer sourceNC.Close()
	_, err := sourceJS.AddStream(&nats.StreamConfig{Name: "TEST"})
	require_NoError(t, err)
	_, err = sourceJS.Publish("TEST", []byte("restored message"))
	require_NoError(t, err)
	cfg, state, snapshot := performStreamBackup(t, sourceNC, "TEST")
	cfg.Replicas = 3

	c := createJetStreamClusterExplicit(t, "R3S", 3)
	defer c.shutdown()

	// Pause the failed restore before it deletes its Raft node or sends its failure report.
	failurePaused := make(chan struct{}, 1)
	monitorExited := make(chan struct{}, 1)
	resumeFailure := make(chan struct{})
	resumeFailureHandling := sync.OnceFunc(func() { close(resumeFailure) })
	defer resumeFailureHandling()
	for _, s := range c.servers {
		s.SetLogger(&delayedRestoreFailureLogger{
			failurePaused: failurePaused,
			monitorExited: monitorExited,
			resumeFailure: resumeFailure,
		}, true, false)
	}
	nc, js := jsClientConnect(t, c.randomServer())
	defer nc.Close()

	req, err := json.Marshal(&JSApiStreamRestoreRequest{Config: cfg, State: state})
	require_NoError(t, err)
	_, err = nc.Request(fmt.Sprintf(JSApiStreamRestoreT, cfg.Name), req, 5*time.Second)
	require_NoError(t, err)

	// Send no chunks so the first restore stalls.
	select {
	case <-failurePaused:
	case <-time.After(10 * time.Second):
		t.Fatal("restore did not stall")
	}

	// Delete the failed attempt, then restore the snapshot under the same name.
	require_NoError(t, js.DeleteStream(cfg.Name))
	deletionAdvisories, err := nc.SubscribeSync(JSAdvisoryStreamDeletedPre + "." + cfg.Name)
	require_NoError(t, err)

	require_True(t, performStreamRestore(t, nc, cfg, state, snapshot))
	checkFor(t, 30*time.Second, 10*time.Millisecond, func() error {
		si, err := js.StreamInfo(cfg.Name)
		if err != nil {
			return err
		}
		if si.State.Msgs != state.Msgs || si.Cluster == nil || len(si.Cluster.Replicas) != 2 {
			return fmt.Errorf("replacement not restored: %+v", si)
		}
		for _, peer := range si.Cluster.Replicas {
			if !peer.Current {
				return fmt.Errorf("replica %s not current", peer.Name)
			}
		}
		return nil
	})

	// Let the original restore report its failure after the replacement is current.
	resumeFailureHandling()
	select {
	case <-monitorExited:
	case <-time.After(5 * time.Second):
		t.Fatal("failed restore monitor did not exit")
	}

	// The replacement stream should not be deleted
	_, err = deletionAdvisories.NextMsg(time.Second)
	if err == nil {
		t.Fatal("replacement was deleted after the delayed restore failure")
	}
	require_Error(t, err, nats.ErrTimeout)
	msg, err := js.GetMsg(cfg.Name, 1)
	require_NoError(t, err)
	require_Equal(t, string(msg.Data), "restored message")
}

func TestJetStreamClusterMetaMonitorShutdownRightAfterStart(t *testing.T) {
	// The meta monitor goroutine can first run after shutdownJetStream has
	// cleared the meta group state, which used to crash a clustered server
	// shut down right after it started. A single proc makes that likely.
	defer runtime.GOMAXPROCS(runtime.GOMAXPROCS(1))

	for r := range 25 {
		var wg sync.WaitGroup
		for i := range 32 {
			wg.Add(1)
			go func() {
				defer wg.Done()
				o := DefaultTestOptions
				o.Port = -1
				o.ServerName = fmt.Sprintf("S-%d-%d", r, i)
				o.JetStream = true
				o.StoreDir = t.TempDir()
				o.Cluster.Name = "R1S"
				o.Cluster.Host = o.Host
				o.Cluster.Port = -1
				// Clustered JetStream requires a route, nothing needs to listen on it.
				o.Routes = RoutesFromStr("nats://127.0.0.1:1")
				s := RunServer(&o)
				start := time.Now()
				s.Shutdown()
				if elapsed := time.Since(start); elapsed > 5*time.Second {
					t.Errorf("Shutdown of %s took %v", o.ServerName, elapsed)
				}
			}()
		}
		wg.Wait()
	}
}

func TestJetStreamClusterMetaMonitorNotStartedShutdown(t *testing.T) {
	o := DefaultTestOptions
	o.Port = -1
	o.JetStream = true
	o.StoreDir = t.TempDir()
	s := RunServer(&o)
	defer s.Shutdown()

	// Set up the meta group after a Shutdown has stopped new goroutines from
	// starting, so the meta monitor is never started. No cluster block is
	// needed since setupMetaGroup is called directly.
	s.grMu.Lock()
	s.grRunning = false
	s.grMu.Unlock()
	require_NoError(t, s.getJetStream().setupMetaGroup())

	// Must not wait for a monitor that is not running.
	start := time.Now()
	s.shutdownJetStream()
	if elapsed := time.Since(start); elapsed > 5*time.Second {
		t.Fatalf("shutdownJetStream took %v", elapsed)
	}
}
