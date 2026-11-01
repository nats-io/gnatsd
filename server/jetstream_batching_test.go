// Copyright 2025-2026 The NATS Authors
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

//go:build !skip_js_tests

package server

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"math/big"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/klauspost/compress/s2"
	"github.com/nats-io/nats.go"
)

func TestJetStreamAtomicBatchPublish(t *testing.T) {
	test := func(
		t *testing.T,
		storage StorageType,
		retention RetentionPolicy,
		replicas int,
	) {
		c := createJetStreamClusterExplicit(t, "R3S", 3)
		defer c.shutdown()

		nc, js := jsClientConnect(t, c.randomServer())
		defer nc.Close()

		var pubAck JSPubAckResponse

		cfg := &StreamConfig{
			Name:      "TEST",
			Subjects:  []string{"foo.*"},
			Storage:   storage,
			Retention: retention,
			Replicas:  replicas,
		}

		_, err := jsStreamCreate(t, nc, cfg)
		require_NoError(t, err)

		m := nats.NewMsg("foo.0")
		m.Data = []byte("foo.0")
		m.Header.Set("Nats-Batch-Id", "uuid")

		// Publish with atomic publish disabled.
		rmsg, err := nc.RequestMsg(m, time.Second)
		require_NoError(t, err)
		require_NoError(t, json.Unmarshal(rmsg.Data, &pubAck))
		require_NotNil(t, pubAck.Error)
		require_Error(t, pubAck.Error, NewJSAtomicPublishDisabledError())

		// Enable atomic publish.
		cfg.AllowAtomicPublish = true
		_, err = jsStreamUpdate(t, nc, cfg)
		require_NoError(t, err)

		// Publish without batch sequence errors.
		rmsg, err = nc.RequestMsg(m, time.Second)
		require_NoError(t, err)
		pubAck = JSPubAckResponse{}
		require_NoError(t, json.Unmarshal(rmsg.Data, &pubAck))
		require_Error(t, pubAck.Error, NewJSAtomicPublishMissingSeqError())

		// Publish a batch, misses start.
		m.Header.Set("Nats-Batch-Id", "uuid")
		m.Header.Set("Nats-Batch-Sequence", "2")
		rmsg, err = nc.RequestMsg(m, time.Second)
		require_NoError(t, err)
		pubAck = JSPubAckResponse{}
		require_NoError(t, json.Unmarshal(rmsg.Data, &pubAck))
		require_Error(t, pubAck.Error, NewJSAtomicPublishIncompleteBatchError())

		// Publish a "batch" which immediately commits.
		m.Header.Set("Nats-Batch-Id", "uuid")
		m.Header.Set("Nats-Batch-Sequence", "1")
		m.Header.Set("Nats-Batch-Commit", "1")
		rmsg, err = nc.RequestMsg(m, time.Second)
		require_NoError(t, err)
		pubAck = JSPubAckResponse{}
		require_NoError(t, json.Unmarshal(rmsg.Data, &pubAck))
		require_Equal(t, pubAck.Sequence, 1)
		require_Equal(t, pubAck.BatchId, "uuid")
		require_Equal(t, pubAck.BatchSize, 1)

		// Reset commit.
		m.Header.Del("Nats-Batch-Commit")

		// Publish a "batch" which has gaps.
		require_NoError(t, nc.PublishMsg(m))
		m.Header.Set("Nats-Batch-Sequence", "3")
		rmsg, err = nc.RequestMsg(m, time.Second)
		require_NoError(t, err)
		pubAck = JSPubAckResponse{}
		require_NoError(t, json.Unmarshal(rmsg.Data, &pubAck))
		require_Error(t, pubAck.Error, NewJSAtomicPublishIncompleteBatchError())

		// Publish a batch of N messages.
		m.Header.Del("Nats-Batch-Commit")
		for seq, batch := uint64(1), uint64(5); seq <= batch; seq++ {
			m.Subject = fmt.Sprintf("foo.%d", seq)
			m.Data = []byte(m.Subject)
			m.Header.Set("Nats-Batch-Sequence", strconv.FormatUint(seq, 10))
			// If not commit.
			if seq != batch {
				require_NoError(t, nc.PublishMsg(m))
				continue
			}

			m.Header.Set("Nats-Batch-Commit", "1")
			rmsg, err = nc.RequestMsg(m, time.Second)
			require_NoError(t, err)

			pubAck = JSPubAckResponse{}
			require_NoError(t, json.Unmarshal(rmsg.Data, &pubAck))
			require_True(t, pubAck.Error == nil)
			require_Equal(t, pubAck.Sequence, 6)
			require_Equal(t, pubAck.BatchId, "uuid")
			require_Equal(t, pubAck.BatchSize, 5)
		}

		// Validate stream contents.
		if retention != InterestPolicy {
			for i := 0; i < 6; i++ {
				rsm, err := js.GetMsg("TEST", uint64(i+1))
				require_NoError(t, err)
				subj := fmt.Sprintf("foo.%d", i)
				require_Equal(t, rsm.Subject, subj)
				require_Equal(t, string(rsm.Data), subj)
			}
		}
	}

	for _, storage := range []StorageType{FileStorage, MemoryStorage} {
		for _, retention := range []RetentionPolicy{LimitsPolicy, InterestPolicy, WorkQueuePolicy} {
			for _, replicas := range []int{1, 3} {
				t.Run(fmt.Sprintf("%s/%s/R%d", storage, retention, replicas), func(t *testing.T) {
					test(t, storage, retention, replicas)
				})
			}
		}
	}
}

func TestJetStreamAtomicBatchPublishEmptyAck(t *testing.T) {
	test := func(
		t *testing.T,
		storage StorageType,
		retention RetentionPolicy,
		replicas int,
	) {
		c := createJetStreamClusterExplicit(t, "R3S", 3)
		defer c.shutdown()

		nc := clientConnectToServer(t, c.randomServer())
		defer nc.Close()

		var pubAck JSPubAckResponse

		_, err := jsStreamCreate(t, nc, &StreamConfig{
			Name:               "TEST",
			Subjects:           []string{"foo.*"},
			Storage:            storage,
			Retention:          retention,
			Replicas:           replicas,
			AllowAtomicPublish: true,
		})
		require_NoError(t, err)

		// Publish a batch of N messages.
		for seq, batch := uint64(1), uint64(5); seq <= batch; seq++ {
			m := nats.NewMsg(fmt.Sprintf("foo.%d", seq))
			m.Data = []byte(m.Subject)
			m.Header.Set("Nats-Batch-Id", "uuid")
			m.Header.Set("Nats-Batch-Sequence", strconv.FormatUint(seq, 10))
			commit := seq == batch
			if commit {
				m.Header.Set("Nats-Batch-Commit", "1")
			}

			rmsg, err := nc.RequestMsg(m, time.Second)
			require_NoError(t, err)
			// If not commit we should receive an empty ack for flow control.
			if !commit {
				require_Len(t, len(rmsg.Data), 0)
				continue
			}

			require_NoError(t, json.Unmarshal(rmsg.Data, &pubAck))
			require_Equal(t, pubAck.Sequence, 5)
			require_Equal(t, pubAck.BatchId, "uuid")
			require_Equal(t, pubAck.BatchSize, 5)
		}
	}

	for _, storage := range []StorageType{FileStorage, MemoryStorage} {
		for _, retention := range []RetentionPolicy{LimitsPolicy, InterestPolicy, WorkQueuePolicy} {
			for _, replicas := range []int{1, 3} {
				t.Run(fmt.Sprintf("%s/%s/R%d", storage, retention, replicas), func(t *testing.T) {
					test(t, storage, retention, replicas)
				})
			}
		}
	}
}

func TestJetStreamAtomicBatchPublishCommitEob(t *testing.T) {
	test := func(
		t *testing.T,
		storage StorageType,
		retention RetentionPolicy,
		replicas int,
	) {
		c := createJetStreamClusterExplicit(t, "R3S", 3)
		defer c.shutdown()

		nc, js := jsClientConnect(t, c.randomServer())
		defer nc.Close()

		var pubAck JSPubAckResponse
		_, err := jsStreamCreate(t, nc, &StreamConfig{
			Name:               "TEST",
			Subjects:           []string{"foo"},
			Storage:            storage,
			Retention:          retention,
			Replicas:           replicas,
			AllowAtomicPublish: true,
		})
		require_NoError(t, err)

		// A batch that immediately commits through EOB is an error.
		m := nats.NewMsg("foo")
		m.Header.Set("Nats-Batch-Id", "uuid")
		m.Header.Set("Nats-Batch-Sequence", "1")
		m.Header.Set("Nats-Batch-Commit", "eob")
		rmsg, err := nc.RequestMsg(m, time.Second)
		require_NoError(t, err)
		pubAck = JSPubAckResponse{}
		require_NoError(t, json.Unmarshal(rmsg.Data, &pubAck))
		require_Error(t, pubAck.Error, NewJSAtomicPublishIncompleteBatchError())

		m = nats.NewMsg("foo")
		m.Header.Set("Nats-Batch-Id", "uuid")
		m.Header.Set("Nats-Batch-Sequence", "1")
		require_NoError(t, nc.PublishMsg(m))

		m.Header.Set("Nats-Batch-Sequence", "2")
		require_NoError(t, nc.PublishMsg(m))

		m.Header.Set("Nats-Batch-Sequence", "3")
		m.Header.Set("Nats-Batch-Commit", "eob")
		rmsg, err = nc.RequestMsg(m, time.Second)
		require_NoError(t, err)
		pubAck = JSPubAckResponse{}
		require_NoError(t, json.Unmarshal(rmsg.Data, &pubAck))
		require_True(t, pubAck.Error == nil)
		require_Equal(t, pubAck.Sequence, 2)
		require_Equal(t, pubAck.BatchId, "uuid")
		require_Equal(t, pubAck.BatchSize, 2)

		// Validate stream contents.
		if retention != InterestPolicy {
			for seq := uint64(1); seq <= 2; seq++ {
				rsm, err := js.GetMsg("TEST", seq)
				require_NoError(t, err)
				require_Equal(t, rsm.Header.Get("Nats-Batch-Id"), "uuid")
				require_Equal(t, rsm.Header.Get("Nats-Batch-Sequence"), strconv.FormatUint(seq, 10))
				// The last message should have the commit header set, even though the commit was done via EOB.
				if seq == 2 {
					require_Equal(t, rsm.Header.Get("Nats-Batch-Commit"), "1")
				}
			}
		}
	}

	for _, storage := range []StorageType{FileStorage, MemoryStorage} {
		for _, retention := range []RetentionPolicy{LimitsPolicy, InterestPolicy, WorkQueuePolicy} {
			for _, replicas := range []int{1, 3} {
				t.Run(fmt.Sprintf("%s/%s/R%d", storage, retention, replicas), func(t *testing.T) {
					test(t, storage, retention, replicas)
				})
			}
		}
	}
}

func TestJetStreamAtomicBatchPublishCommitEobMaxBatchSize(t *testing.T) {
	streamMaxAtomicBatchSize = 3
	defer func() {
		streamMaxAtomicBatchSize = streamDefaultMaxAtomicBatchSize
	}()

	s := RunBasicJetStreamServer(t)
	defer s.Shutdown()

	nc := clientConnectToServer(t, s)
	defer nc.Close()

	cfg := &StreamConfig{
		Name:               "TEST",
		Subjects:           []string{"foo"},
		Storage:            FileStorage,
		Retention:          LimitsPolicy,
		Replicas:           1,
		AllowAtomicPublish: true,
	}
	_, err := jsStreamCreate(t, nc, cfg)
	require_NoError(t, err)

	var pubAck JSPubAckResponse

	// A batch of exactly the maximum size must be accepted when committed via an
	// "End Of Batch" marker, since the marker itself is not stored as part of the batch.
	for seq := 1; seq <= streamMaxAtomicBatchSize; seq++ {
		m := nats.NewMsg("foo")
		m.Header.Set("Nats-Batch-Id", "uuid")
		m.Header.Set("Nats-Batch-Sequence", strconv.Itoa(seq))
		require_NoError(t, nc.PublishMsg(m))
	}
	m := nats.NewMsg("foo")
	m.Header.Set("Nats-Batch-Id", "uuid")
	m.Header.Set("Nats-Batch-Sequence", strconv.Itoa(streamMaxAtomicBatchSize+1))
	m.Header.Set("Nats-Batch-Commit", "eob")
	rmsg, err := nc.RequestMsg(m, time.Second)
	require_NoError(t, err)
	require_NoError(t, json.Unmarshal(rmsg.Data, &pubAck))
	require_True(t, pubAck.Error == nil)
	require_Equal(t, pubAck.BatchSize, uint64(streamMaxAtomicBatchSize))

	// One message over the maximum size must still be rejected when committed via EOB.
	for seq := 1; seq <= streamMaxAtomicBatchSize+1; seq++ {
		m = nats.NewMsg("foo")
		m.Header.Set("Nats-Batch-Id", "uuid2")
		m.Header.Set("Nats-Batch-Sequence", strconv.Itoa(seq))
		require_NoError(t, nc.PublishMsg(m))
	}
	m = nats.NewMsg("foo")
	m.Header.Set("Nats-Batch-Id", "uuid2")
	m.Header.Set("Nats-Batch-Sequence", strconv.Itoa(streamMaxAtomicBatchSize+2))
	m.Header.Set("Nats-Batch-Commit", "eob")
	rmsg, err = nc.RequestMsg(m, time.Second)
	require_NoError(t, err)
	pubAck = JSPubAckResponse{}
	require_NoError(t, json.Unmarshal(rmsg.Data, &pubAck))
	require_Error(t, pubAck.Error, NewJSAtomicPublishTooLargeBatchError(streamMaxAtomicBatchSize))

	// An EOB commit marker for an unknown batch at sequence maxSize+1 means the batch
	// itself would have exactly maxSize messages, so it should be reported as incomplete
	// rather than too large.
	m = nats.NewMsg("foo")
	m.Header.Set("Nats-Batch-Id", "uuid3")
	m.Header.Set("Nats-Batch-Sequence", strconv.Itoa(streamMaxAtomicBatchSize+1))
	m.Header.Set("Nats-Batch-Commit", "eob")
	rmsg, err = nc.RequestMsg(m, time.Second)
	require_NoError(t, err)
	pubAck = JSPubAckResponse{}
	require_NoError(t, json.Unmarshal(rmsg.Data, &pubAck))
	require_Error(t, pubAck.Error, NewJSAtomicPublishIncompleteBatchError())
}

func TestJetStreamAtomicBatchPublishLimits(t *testing.T) {
	streamMaxAtomicBatchInflightPerStream = 1
	streamMaxAtomicBatchInflightTotal = 1
	streamMaxAtomicBatchSize = 2
	streamMaxBatchTimeout = 500 * time.Millisecond
	defer func() {
		streamMaxAtomicBatchInflightPerStream = streamDefaultMaxAtomicBatchInflightPerStream
		streamMaxAtomicBatchInflightTotal = streamDefaultMaxAtomicBatchInflightTotal
		streamMaxAtomicBatchSize = streamDefaultMaxAtomicBatchSize
		streamMaxBatchTimeout = streamDefaultMaxBatchTimeout
	}()

	test := func(t *testing.T, replicas int) {
		c := createJetStreamClusterExplicit(t, "R3S", 3)
		defer c.shutdown()

		nc := clientConnectToServer(t, c.randomServer())
		defer nc.Close()

		var pubAck JSPubAckResponse

		cfg := &StreamConfig{
			Name:               "FOO",
			Subjects:           []string{"foo"},
			Storage:            FileStorage,
			Retention:          LimitsPolicy,
			Replicas:           replicas,
			AllowAtomicPublish: true,
		}
		_, err := jsStreamCreate(t, nc, cfg)
		require_NoError(t, err)

		// For testing total server-wide maximum inflight batches.
		cfg = &StreamConfig{
			Name:               "BAR",
			Subjects:           []string{"bar"},
			Storage:            FileStorage,
			Retention:          LimitsPolicy,
			Replicas:           replicas,
			AllowAtomicPublish: true,
		}
		_, err = jsStreamCreate(t, nc, cfg)
		require_NoError(t, err)

		// A batch ID must not exceed the maximum length.
		for _, length := range []int{64, 65} {
			longBatchId := strings.Repeat("A", length)
			m := nats.NewMsg("foo")
			m.Header.Set("Nats-Batch-Id", longBatchId)
			m.Header.Set("Nats-Batch-Sequence", "1")
			m.Header.Set("Nats-Batch-Commit", "1")
			rmsg, err := nc.RequestMsg(m, time.Second)
			require_NoError(t, err)
			pubAck = JSPubAckResponse{}
			require_NoError(t, json.Unmarshal(rmsg.Data, &pubAck))
			if length <= 64 {
				require_True(t, pubAck.Error == nil)
			} else {
				require_NotNil(t, pubAck.Error)
				require_Error(t, pubAck.Error, NewJSAtomicPublishInvalidBatchIDError())
			}
		}

		// One batch is inflight.
		m := nats.NewMsg("foo")
		m.Header.Set("Nats-Batch-Id", "uuid")
		m.Header.Set("Nats-Batch-Sequence", "1")
		require_NoError(t, nc.PublishMsg(m))

		// Another batch moves over the threshold and batch is denied.
		m = nats.NewMsg("foo")
		m.Header.Set("Nats-Batch-Id", "exceeds_threshold")
		m.Header.Set("Nats-Batch-Sequence", "1")
		m.Header.Set("Nats-Batch-Commit", "1")
		rmsg, err := nc.RequestMsg(m, time.Second)
		require_NoError(t, err)
		pubAck = JSPubAckResponse{}
		require_NoError(t, json.Unmarshal(rmsg.Data, &pubAck))
		require_Error(t, pubAck.Error, NewJSAtomicPublishTooManyInflightError())

		// Another batch on a different stream moves over the server-wide threshold and batch is denied.
		m = nats.NewMsg("bar")
		m.Header.Set("Nats-Batch-Id", "bar")
		m.Header.Set("Nats-Batch-Sequence", "1")
		m.Header.Set("Nats-Batch-Commit", "1")
		rmsg, err = nc.RequestMsg(m, time.Second)
		require_NoError(t, err)
		pubAck = JSPubAckResponse{}
		require_NoError(t, json.Unmarshal(rmsg.Data, &pubAck))
		require_Error(t, pubAck.Error, NewJSAtomicPublishTooManyInflightError())

		// The first batch should now time out.
		sl := c.streamLeader(globalAccountName, "FOO")
		mset, err := sl.globalAccount().lookupStream("FOO")
		require_NoError(t, err)
		checkFor(t, 2*time.Second, 200*time.Millisecond, func() error {
			mset.mu.RLock()
			batches := mset.batches
			mset.mu.RUnlock()
			if batches == nil {
				return errors.New("batches not found")
			}
			batches.mu.Lock()
			groups := len(batches.atomic)
			batches.mu.Unlock()
			if groups != 0 {
				return fmt.Errorf("expected 0 groups, got %d", groups)
			}
			return nil
		})

		// Publishing to the batch should also error since it timed out.
		m = nats.NewMsg("foo")
		m.Header.Set("Nats-Batch-Id", "uuid")
		m.Header.Set("Nats-Batch-Sequence", "2")
		m.Header.Set("Nats-Batch-Commit", "1")
		rmsg, err = nc.RequestMsg(m, time.Second)
		require_NoError(t, err)
		pubAck = JSPubAckResponse{}
		require_NoError(t, json.Unmarshal(rmsg.Data, &pubAck))
		require_Error(t, pubAck.Error, NewJSAtomicPublishIncompleteBatchError())

		// Batch should be rejected if going over the max batch size.
		for _, size := range []int{streamMaxAtomicBatchSize, streamMaxAtomicBatchSize + 1} {
			for i := range size {
				seq := i + 1
				m = nats.NewMsg("foo")
				m.Header.Set("Nats-Batch-Id", "exceeds_threshold")
				m.Header.Set("Nats-Batch-Sequence", strconv.Itoa(seq))
				commit := seq == size
				if !commit {
					require_NoError(t, nc.PublishMsg(m))
					continue
				}

				m.Header.Set("Nats-Batch-Commit", "1")
				rmsg, err = nc.RequestMsg(m, time.Second)
				require_NoError(t, err)
				pubAck = JSPubAckResponse{}
				require_NoError(t, json.Unmarshal(rmsg.Data, &pubAck))
				if size <= streamMaxAtomicBatchSize {
					require_True(t, pubAck.Error == nil)
					require_Equal(t, pubAck.BatchSize, uint64(size))
				} else {
					require_Error(t, pubAck.Error, NewJSAtomicPublishTooLargeBatchError(streamMaxAtomicBatchSize))
				}
			}
		}
	}

	t.Run("R1", func(t *testing.T) { test(t, 1) })
	t.Run("R3", func(t *testing.T) { test(t, 3) })
}

func TestJetStreamAtomicBatchPublishDedupeNotAllowed(t *testing.T) {
	test := func(
		t *testing.T,
		nc *nats.Conn,
		js nats.JetStreamContext,
		storage StorageType,
		retention RetentionPolicy,
		replicas int,
	) {
		cfg := &StreamConfig{
			Name:               "TEST",
			Retention:          retention,
			Subjects:           []string{"foo"},
			Replicas:           replicas,
			Storage:            storage,
			AllowAtomicPublish: true,
		}

		_, err := jsStreamCreate(t, nc, cfg)
		require_NoError(t, err)

		_, err = js.Publish("foo", nil, nats.MsgId("pre-existing"))
		require_NoError(t, err)

		var pubAck JSPubAckResponse
		m := nats.NewMsg("foo")
		m.Header.Set("Nats-Msg-Id", "pre-existing")
		m.Header.Set("Nats-Batch-Id", "uuid")
		m.Header.Set("Nats-Batch-Sequence", "1")
		m.Header.Set("Nats-Batch-Commit", "1")
		rmsg, err := nc.RequestMsg(m, time.Second)
		require_NoError(t, err)
		require_NoError(t, json.Unmarshal(rmsg.Data, &pubAck))
		require_NotNil(t, pubAck.Error)
		require_Error(t, pubAck.Error, NewJSAtomicPublishContainsDuplicateMessageError())

		m = nats.NewMsg("foo")
		m.Header.Set("Nats-Batch-Id", "uuid")
		m.Header.Set("Nats-Batch-Sequence", "1")
		m.Header.Set("Nats-Msg-Id", "msgId1")
		require_NoError(t, nc.PublishMsg(m))

		pubAck = JSPubAckResponse{}
		m.Header.Set("Nats-Batch-Sequence", "2")
		m.Header.Set("Nats-Msg-Id", "msgId2")
		m.Header.Set("Nats-Batch-Commit", "1")
		rmsg, err = nc.RequestMsg(m, time.Second)
		require_NoError(t, err)
		require_NoError(t, json.Unmarshal(rmsg.Data, &pubAck))
		require_True(t, pubAck.Error == nil)
		require_Equal(t, pubAck.BatchSize, 2)
		require_Equal(t, pubAck.Sequence, 3)
		require_Equal(t, pubAck.BatchId, "uuid")
	}

	for _, storage := range []StorageType{FileStorage, MemoryStorage} {
		for _, retention := range []RetentionPolicy{LimitsPolicy, InterestPolicy, WorkQueuePolicy} {
			for _, replicas := range []int{1, 3} {
				t.Run(fmt.Sprintf("%s/%s/R%d", storage, retention, replicas), func(t *testing.T) {
					c := createJetStreamClusterExplicit(t, "R3S", 3)
					defer c.shutdown()

					nc, js := jsClientConnect(t, c.randomServer())
					defer nc.Close()

					test(t, nc, js, storage, retention, replicas)
				})
			}
		}
	}
}

func TestJetStreamAtomicBatchPublishSourceAndMirror(t *testing.T) {
	test := func(t *testing.T, replicas int) {
		c := createJetStreamClusterExplicit(t, "R3S", 3)
		defer c.shutdown()

		nc, js := jsClientConnect(t, c.randomServer())
		defer nc.Close()

		_, err := jsStreamCreate(t, nc, &StreamConfig{
			Name:               "TEST",
			Subjects:           []string{"foo"},
			Storage:            FileStorage,
			AllowAtomicPublish: true,
			Replicas:           replicas,
		})
		require_NoError(t, err)

		for seq := uint64(1); seq <= 3; seq++ {
			m := nats.NewMsg("foo")
			m.Header.Set("Nats-Batch-Id", "uuid")
			m.Header.Set("Nats-Batch-Sequence", strconv.FormatUint(seq, 10))
			commit := seq == 3
			if !commit {
				require_NoError(t, nc.PublishMsg(m))
				continue
			}
			m.Header.Set("Nats-Batch-Commit", "1")

			rmsg, err := nc.RequestMsg(m, time.Second)
			require_NoError(t, err)
			var pubAck JSPubAckResponse
			require_NoError(t, json.Unmarshal(rmsg.Data, &pubAck))
			require_Equal(t, pubAck.Sequence, 3)
			require_Equal(t, pubAck.BatchId, "uuid")
			require_Equal(t, pubAck.BatchSize, 3)
		}

		require_NoError(t, js.DeleteMsg("TEST", 2))
		checkFor(t, 2*time.Second, 200*time.Millisecond, func() error {
			return checkState(t, c, globalAccountName, "TEST")
		})

		// Mirror can source batched messages but can't do atomic batching itself.
		_, err = jsStreamCreate(t, nc, &StreamConfig{
			Name:               "M-no-batch",
			Storage:            FileStorage,
			Mirror:             &StreamSource{Name: "TEST"},
			Replicas:           replicas,
			AllowAtomicPublish: true,
		})
		require_Error(t, err, NewJSMirrorWithAtomicPublishError())

		_, err = js.AddStream(&nats.StreamConfig{
			Name:     "M",
			Mirror:   &nats.StreamSource{Name: "TEST"},
			Replicas: replicas,
		})
		require_NoError(t, err)

		_, err = jsStreamCreate(t, nc, &StreamConfig{
			Name:               "S",
			Storage:            FileStorage,
			Sources:            []*StreamSource{{Name: "TEST"}},
			Replicas:           replicas,
			AllowAtomicPublish: true,
		})
		require_NoError(t, err)

		checkFor(t, 2*time.Second, 200*time.Millisecond, func() error {
			for _, name := range []string{"M", "S"} {
				if si, err := js.StreamInfo(name); err != nil {
					return err
				} else if si.State.Msgs != 2 {
					return fmt.Errorf("expected 2 messages for stream %q, got %d", name, si.State.Msgs)
				}
			}
			return nil
		})

		// Ensure the batching headers were removed when ingested into the source/mirror.
		rsm, err := js.GetMsg("M", 1)
		require_NoError(t, err)
		require_Len(t, len(rsm.Header), 0)

		rsm, err = js.GetMsg("M", 3)
		require_NoError(t, err)
		require_Len(t, len(rsm.Header), 0)

		rsm, err = js.GetMsg("S", 1)
		require_NoError(t, err)
		require_Len(t, len(rsm.Header), 1)
		src := rsm.Header.Get(JSStreamSource)
		require_True(t, strings.HasPrefix(src, "TEST 1 > > foo"))

		rsm, err = js.GetMsg("S", 2)
		require_NoError(t, err)
		require_Len(t, len(rsm.Header), 1)
		src = rsm.Header.Get(JSStreamSource)
		require_True(t, strings.HasPrefix(src, "TEST 3 > > foo"))
	}

	t.Run("R1", func(t *testing.T) { test(t, 1) })
	t.Run("R3", func(t *testing.T) { test(t, 3) })
}

func TestJetStreamAtomicBatchPublishCleanup(t *testing.T) {
	const (
		Disable = iota
		StepDown
		Delete
		Commit
	)

	test := func(t *testing.T, mode int) {
		c := createJetStreamClusterExplicit(t, "R3S", 3)
		defer c.shutdown()

		nc, js := jsClientConnect(t, c.randomServer())
		defer nc.Close()

		cfg := &StreamConfig{
			Name:               "TEST",
			Subjects:           []string{"foo"},
			Storage:            FileStorage,
			AllowAtomicPublish: false,
			Replicas:           3,
		}
		_, err := jsStreamCreate(t, nc, cfg)
		require_NoError(t, err)

		sl := c.streamLeader(globalAccountName, "TEST")
		mset, err := sl.globalAccount().lookupStream("TEST")
		require_NoError(t, err)
		mset.mu.RLock()
		batches := mset.batches
		mset.mu.RUnlock()
		require_True(t, batches == nil)

		// Enabling doesn't need to populate the batching state.
		cfg.AllowAtomicPublish = true
		_, err = jsStreamUpdate(t, nc, cfg)
		require_NoError(t, err)
		mset.mu.RLock()
		batches = mset.batches
		mset.mu.RUnlock()
		require_True(t, batches == nil)

		// Publish a partial batch that needs to be cleaned up.
		m := nats.NewMsg("foo")
		m.Header.Set("Nats-Batch-Id", "uuid")
		m.Header.Set("Nats-Batch-Sequence", "1")
		require_NoError(t, nc.PublishMsg(m))

		// Publish another batch that commits, state should be cleaned up by default.
		m.Header.Set("Nats-Batch-Id", "commit")
		m.Header.Set("Nats-Batch-Commit", "1")
		_, err = js.PublishMsg(m)
		require_NoError(t, err)

		mset.mu.RLock()
		batches = mset.batches
		mset.mu.RUnlock()
		require_NotNil(t, batches)
		batches.mu.Lock()
		groups := len(batches.atomic)
		b := batches.atomic["uuid"]
		batches.mu.Unlock()
		require_Len(t, groups, 1)
		require_NotNil(t, b)
		store := b.store
		require_Equal(t, store.State().Msgs, 1)

		// Should fully clean up the in-progress batch.
		switch mode {
		case Disable:
			cfg.AllowAtomicPublish = false
			_, err = jsStreamUpdate(t, nc, cfg)
			require_NoError(t, err)
		case StepDown:
			require_NoError(t, sl.JetStreamStepdownStream(globalAccountName, "TEST"))
		case Delete:
			require_NoError(t, mset.delete())
		case Commit:
			m = nats.NewMsg("foo")
			m.Header.Set("Nats-Batch-Id", "uuid")
			m.Header.Set("Nats-Batch-Sequence", "2")
			m.Header.Set("Nats-Batch-Commit", "1")
			_, err = js.PublishMsg(m)
			require_NoError(t, err)
		}
		checkFor(t, 2*time.Second, 200*time.Millisecond, func() error {
			mset.mu.RLock()
			batches = mset.batches
			mset.mu.RUnlock()
			if batches != nil {
				if mode != Commit {
					return fmt.Errorf("expected no batches")
				}
				batches.mu.Lock()
				groups = len(batches.atomic)
				batches.mu.Unlock()
				if groups > 0 {
					return fmt.Errorf("expected 0 groups, got %d", groups)
				}
			}
			if msgs := store.State().Msgs; msgs != 0 {
				return fmt.Errorf("expected 0 messages, got %d", msgs)
			}
			return nil
		})
	}

	t.Run("Disable", func(t *testing.T) { test(t, Disable) })
	t.Run("StepDown", func(t *testing.T) { test(t, StepDown) })
	t.Run("Delete", func(t *testing.T) { test(t, Delete) })
	t.Run("Commit", func(t *testing.T) { test(t, Commit) })
}

func TestJetStreamAtomicBatchPublishConfigOpts(t *testing.T) {
	// Defaults.
	require_Equal(t, streamMaxBatchTimeout, 10*time.Second)
	require_Equal(t, streamMaxAtomicBatchInflightPerStream, 50)
	require_Equal(t, streamMaxAtomicBatchInflightTotal, 1000)
	require_Equal(t, streamMaxAtomicBatchSize, 1000)
	require_Equal(t, streamMaxFastBatchInflightPerStream, 1000)
	require_Equal(t, streamMaxFastBatchInflightTotal, 50_000)

	batchingConf := `
	listen: 127.0.0.1:-1
	jetstream {
		store_dir: %q
		limits {
			batch {
				max_inflight_per_stream: %d
				max_inflight_total: %d
				max_msgs: %d
				timeout: %s
			}
		}
	}`

	storeDir := t.TempDir()
	cf := createConfFile(t, []byte(fmt.Sprintf(batchingConf, storeDir, 10, 20, 100, "5s")))
	s, _ := RunServerWithConfig(cf)
	defer s.Shutdown()

	opts := s.getOpts()
	require_Equal(t, opts.JetStreamLimits.MaxBatchInflightPerStream, 10)
	require_Equal(t, opts.JetStreamLimits.MaxBatchInflightTotal, 20)
	require_Equal(t, opts.JetStreamLimits.MaxBatchSize, 100)
	require_Equal(t, opts.JetStreamLimits.MaxBatchTimeout, 5*time.Second)

	// The limits are all read at the point of use, so they can be reloaded. Raising them.
	reloadUpdateConfig(t, s, cf, fmt.Sprintf(batchingConf, storeDir, 20, 40, 200, "10s"))
	opts = s.getOpts()
	require_Equal(t, opts.JetStreamLimits.MaxBatchInflightPerStream, 20)
	require_Equal(t, opts.JetStreamLimits.MaxBatchInflightTotal, 40)
	require_Equal(t, opts.JetStreamLimits.MaxBatchSize, 200)
	require_Equal(t, opts.JetStreamLimits.MaxBatchTimeout, 10*time.Second)

	// As well as lowering them again. In-flight batches are not silently dropped, the
	// publisher is always responded to with an error and an advisory is sent.
	reloadUpdateConfig(t, s, cf, fmt.Sprintf(batchingConf, storeDir, 5, 10, 50, "2s"))
	opts = s.getOpts()
	require_Equal(t, opts.JetStreamLimits.MaxBatchInflightPerStream, 5)
	require_Equal(t, opts.JetStreamLimits.MaxBatchInflightTotal, 10)
	require_Equal(t, opts.JetStreamLimits.MaxBatchSize, 50)
	require_Equal(t, opts.JetStreamLimits.MaxBatchTimeout, 2*time.Second)

	// Changing the storage directory remains unsupported.
	changeCurrentConfigContentWithNewContent(t, cf, []byte(fmt.Sprintf(batchingConf, t.TempDir(), 5, 10, 50, "2s")))
	require_Error(t, s.Reload(), fmt.Errorf("config reload not supported for jetstream storage directory"))
}

func TestJetStreamAtomicBatchPublishDenyHeaders(t *testing.T) {
	test := func(t *testing.T, replicas int) {
		c := createJetStreamClusterExplicit(t, "R3S", 3)
		defer c.shutdown()

		nc, js := jsClientConnect(t, c.randomServer())
		defer nc.Close()

		cfg := &StreamConfig{
			Name:               "TEST",
			Subjects:           []string{"foo"},
			Storage:            FileStorage,
			AllowAtomicPublish: true,
			Replicas:           replicas,
		}
		_, err := jsStreamCreate(t, nc, cfg)
		require_NoError(t, err)

		// We might support these headers later on, but for now error.
		for key, value := range map[string]string{
			"Nats-Expected-Last-Msg-Id": "msgId",
		} {
			t.Run(key, func(t *testing.T) {
				m := nats.NewMsg("foo")
				m.Header.Set("Nats-Batch-Id", "uuid")
				m.Header.Set("Nats-Batch-Sequence", "1")
				m.Header.Set(key, value)
				require_NoError(t, nc.PublishMsg(m))

				m = nats.NewMsg("foo")
				m.Header.Set("Nats-Batch-Id", "uuid")
				m.Header.Set("Nats-Batch-Sequence", "2")
				m.Header.Set("Nats-Batch-Commit", "eob")
				_, err = js.PublishMsg(m)
				require_Error(t, err, NewJSAtomicPublishUnsupportedHeaderBatchError(key))
			})
		}
	}

	t.Run("R1", func(t *testing.T) { test(t, 1) })
	t.Run("R3", func(t *testing.T) { test(t, 3) })
}

func TestJetStreamAtomicBatchPublishStageAndCommit(t *testing.T) {
	type BatchItem struct {
		subject  string
		rsubject string
		header   nats.Header
		msg      []byte
		err      error
	}

	type BatchTest struct {
		title             string
		allowRollup       bool
		denyPurge         bool
		allowTTL          bool
		allowMsgCounter   bool
		allowMsgSchedules bool
		discardNew        bool
		discardNewPerSubj bool
		init              func(mset *stream)
		batch             []BatchItem
		validate          func(mset *stream, commit bool)
	}

	tests := []BatchTest{
		{
			title: "dedupe-distinct",
			batch: []BatchItem{
				{subject: "foo", header: nats.Header{JSMsgId: {"foo"}}},
				{subject: "bar", header: nats.Header{JSMsgId: {"bar"}}},
			},
			validate: func(mset *stream, commit bool) {
				require_Equal(t, mset.checkMsgId("foo") != nil, commit)
				require_Equal(t, mset.checkMsgId("bar") != nil, commit)
			},
		},
		{
			title: "dedupe",
			init: func(mset *stream) {
				mset.storeMsgId(&ddentry{id: "foo", seq: 0, ts: time.Now().UnixNano()})
			},
			batch: []BatchItem{
				{subject: "foo", header: nats.Header{JSMsgId: {"foo"}}, err: errMsgIdDuplicate},
			},
			validate: func(mset *stream, commit bool) {
				require_NotNil(t, mset.checkMsgId("foo"))
			},
		},
		{
			title: "dedupe-staged",
			batch: []BatchItem{
				{subject: "foo", header: nats.Header{JSMsgId: {"foo"}}},
				{subject: "foo", header: nats.Header{JSMsgId: {"foo"}}, err: errMsgIdDuplicate},
			},
			validate: func(mset *stream, commit bool) {
				require_Equal(t, mset.checkMsgId("foo") != nil, commit)
			},
		},
		{
			title:           "counter-single",
			allowMsgCounter: true,
			batch: []BatchItem{
				{subject: "foo", header: nats.Header{JSMessageIncr: {"1"}}},
			},
			validate: func(mset *stream, commit bool) {
				if !commit {
					require_True(t, mset.clusteredCounterTotal["foo"] == nil)
				} else {
					counter := mset.clusteredCounterTotal["foo"]
					require_NotNil(t, counter)
					require_Equal(t, counter.ops, 1)
					require_Equal(t, counter.total.String(), "1")
				}
			},
		},
		{
			title:           "counter-multiple",
			allowMsgCounter: true,
			batch: []BatchItem{
				{subject: "foo", header: nats.Header{JSMessageIncr: {"1"}}},
				{subject: "foo", header: nats.Header{JSMessageIncr: {"2"}}},
			},
			validate: func(mset *stream, commit bool) {
				if !commit {
					require_True(t, mset.clusteredCounterTotal["foo"] == nil)
				} else {
					counter := mset.clusteredCounterTotal["foo"]
					require_NotNil(t, counter)
					require_Equal(t, counter.ops, 2)
					require_Equal(t, counter.total.String(), "3")
				}
			},
		},
		{
			title:           "counter-pre-init",
			allowMsgCounter: true,
			init: func(mset *stream) {
				mset.clusteredCounterTotal = map[string]*msgCounterRunningTotal{
					"foo": {total: big.NewInt(1), ops: 1},
				}
			},
			batch: []BatchItem{
				{subject: "foo", header: nats.Header{JSMessageIncr: {"2"}}},
			},
			validate: func(mset *stream, commit bool) {
				counter := mset.clusteredCounterTotal["foo"]
				require_NotNil(t, counter)
				if !commit {
					require_Equal(t, counter.ops, 1)
					require_Equal(t, counter.total.String(), "1")
				} else {
					require_Equal(t, counter.ops, 2)
					require_Equal(t, counter.total.String(), "3")
				}
			},
		},
		{
			title:             "msg-schedules-disabled",
			allowMsgSchedules: false,
			batch: []BatchItem{
				{
					subject: "foo",
					header:  nats.Header{JSSchedulePattern: {"@at 1970-01-01T00:00:00Z"}},
					err:     NewJSMessageSchedulesDisabledError(),
				},
				{
					subject: "foo",
					header:  nats.Header{JSSchedulePattern: {"disabled"}},
					err:     NewJSMessageSchedulesDisabledError(),
				},
			},
		},
		{
			title:             "msg-schedules-ttl-disabled",
			allowMsgSchedules: true,
			batch: []BatchItem{
				{
					subject: "foo",
					header: nats.Header{
						JSSchedulePattern: {"@at 1970-01-01T00:00:00Z"},
						JSScheduleTTL:     {"1s"},
					},
					err: errMsgTTLDisabled,
				},
			},
		},
		{
			title:             "msg-schedules-ttl-invalid",
			allowMsgSchedules: true,
			allowTTL:          true,
			batch: []BatchItem{
				{
					subject: "foo",
					header: nats.Header{
						JSSchedulePattern: {"@at 1970-01-01T00:00:00Z"},
						JSScheduleTTL:     {"invalid"},
					},
					err: NewJSMessageSchedulesTTLInvalidError(),
				},
			},
		},
		{
			title:             "msg-schedules-rollup-invalid",
			allowMsgSchedules: true,
			batch: []BatchItem{
				{
					subject: "foo",
					header: nats.Header{
						JSSchedulePattern: {"@at 1970-01-01T00:00:00Z"},
						JSScheduleRollup:  {"invalid"},
					},
					err: NewJSMessageSchedulesRollupInvalidError(),
				},
			},
		},
		{
			title:             "msg-schedules-invalid-schedule",
			allowMsgSchedules: true,
			batch: []BatchItem{
				{
					subject: "foo",
					header:  nats.Header{JSSchedulePattern: {"invalid"}},
					err:     NewJSMessageSchedulesPatternInvalidError(),
				},
			},
		},
		{
			title:             "msg-schedules-invalid-time-zone",
			allowMsgSchedules: true,
			batch: []BatchItem{
				{
					subject: "foo",
					header: nats.Header{
						JSSchedulePattern:  {"0 * * * * *"},
						JSScheduleTimeZone: {"Not/A/Zone"},
					},
					err: NewJSMessageSchedulesTimeZoneInvalidError(),
				},
			},
		},
		{
			title:             "msg-schedules-empty-time-zone",
			allowMsgSchedules: true,
			batch: []BatchItem{
				{
					subject: "foo",
					header: nats.Header{
						JSSchedulePattern:  {"0 * * * * *"},
						JSScheduleTimeZone: {""},
					},
					err: NewJSMessageSchedulesTimeZoneInvalidError(),
				},
			},
		},
		{
			title:             "msg-schedules-target-mismatch",
			allowMsgSchedules: true,
			batch: []BatchItem{
				{
					subject: "foo",
					header: nats.Header{
						JSSchedulePattern: {"@at 1970-01-01T00:00:00Z"},
						JSScheduleTarget:  {"not.matching"},
					},
					err: NewJSMessageSchedulesTargetInvalidError(),
				},
			},
		},
		{
			title:             "msg-schedules-target-must-be-literal",
			allowMsgSchedules: true,
			batch: []BatchItem{
				{
					subject: "foo",
					header: nats.Header{
						JSSchedulePattern: {"@at 1970-01-01T00:00:00Z"},
						JSScheduleTarget:  {"foo.*"},
					},
					err: NewJSMessageSchedulesTargetInvalidError(),
				},
			},
		},
		{
			title:             "msg-schedules-target-must-be-unique",
			allowMsgSchedules: true,
			batch: []BatchItem{
				{
					subject: "foo",
					header: nats.Header{
						JSSchedulePattern: {"@at 1970-01-01T00:00:00Z"},
						JSScheduleTarget:  {"foo"},
					},
					err: NewJSMessageSchedulesTargetInvalidError(),
				},
			},
		},
		{
			title:             "msg-schedules-rollup-disabled",
			allowMsgSchedules: true,
			batch: []BatchItem{
				{
					subject: "foo",
					header: nats.Header{
						JSSchedulePattern: {"@at 1970-01-01T00:00:00Z"},
						JSScheduleTarget:  {"bar"},
						JSMsgRollup:       {JSMsgRollupSubject},
					},
					err: errors.New("rollup not permitted"),
				},
			},
		},
		{
			title:             "msg-schedules",
			allowMsgSchedules: true,
			allowRollup:       true,
			batch: []BatchItem{
				{
					subject: "foo",
					header: nats.Header{
						JSSchedulePattern: {"@at 1970-01-01T00:00:00Z"},
						JSScheduleTarget:  {"baz"},
					},
				},
				{
					subject: "bar",
					header: nats.Header{
						JSSchedulePattern: {"@at 1970-01-01T00:00:00Z"},
						JSScheduleTarget:  {"baz"},
						JSMsgRollup:       {JSMsgRollupSubject},
					},
				},
			},
		},
		{
			title:             "msg-schedules-purge-staged-not-schedule",
			allowMsgSchedules: true,
			allowRollup:       true,
			batch: []BatchItem{
				{subject: "foo"},
				{
					subject: "bar",
					header: nats.Header{
						JSScheduleNext: {JSScheduleNextPurge},
						JSScheduler:    {"foo"},
					},
					err: NewJSMessageSchedulesSchedulerInvalidError(),
				},
			},
		},
		{
			title:             "msg-schedules-purge-staged-schedule",
			allowMsgSchedules: true,
			allowRollup:       true,
			batch: []BatchItem{
				{
					subject: "foo",
					header: nats.Header{
						JSSchedulePattern: {"@at 1970-01-01T00:00:00Z"},
						JSScheduleTarget:  {"baz"},
					},
				},
				{
					subject: "bar",
					header: nats.Header{
						JSScheduleNext: {JSScheduleNextPurge},
						JSScheduler:    {"foo"},
					},
				},
			},
			validate: func(mset *stream, commit bool) {
				if !commit {
					return
				}
				require_NotNil(t, mset.inflight["foo"])
				require_True(t, mset.inflight["foo"].schedule)
			},
		},
		{
			title:             "msg-schedules-purge-staged-overwritten",
			allowMsgSchedules: true,
			allowRollup:       true,
			batch: []BatchItem{
				{
					subject: "foo",
					header: nats.Header{
						JSSchedulePattern: {"@at 1970-01-01T00:00:00Z"},
						JSScheduleTarget:  {"baz"},
					},
				},
				{subject: "foo"},
				{
					subject: "bar",
					header: nats.Header{
						JSScheduleNext: {JSScheduleNextPurge},
						JSScheduler:    {"foo"},
					},
					err: NewJSMessageSchedulesSchedulerInvalidError(),
				},
			},
		},
		{
			title:             "msg-schedules-purge-inflight-not-schedule",
			allowMsgSchedules: true,
			allowRollup:       true,
			init: func(mset *stream) {
				mset.inflight = map[string]*inflightSubjectRunningTotal{
					"foo": {bytes: 10, ops: 1, schedule: false},
				}
			},
			batch: []BatchItem{
				{
					subject: "bar",
					header: nats.Header{
						JSScheduleNext: {JSScheduleNextPurge},
						JSScheduler:    {"foo"},
					},
					err: NewJSMessageSchedulesSchedulerInvalidError(),
				},
			},
		},
		{
			title:             "msg-schedules-purge-inflight-schedule",
			allowMsgSchedules: true,
			allowRollup:       true,
			init: func(mset *stream) {
				mset.inflight = map[string]*inflightSubjectRunningTotal{
					"foo": {bytes: 10, ops: 1, schedule: true},
				}
			},
			batch: []BatchItem{
				{
					subject: "bar",
					header: nats.Header{
						JSScheduleNext: {JSScheduleNextPurge},
						JSScheduler:    {"foo"},
					},
				},
			},
		},
		{
			title:      "discard-new",
			discardNew: true,
			batch: []BatchItem{
				{subject: "foo1"},
				{subject: "foo2"},
			},
			validate: func(mset *stream, commit bool) {
				if !commit {
					require_Len(t, len(mset.inflight), 0)
				} else {
					require_Len(t, len(mset.inflight), 2)
					require_Equal(t, *mset.inflight["foo1"], inflightSubjectRunningTotal{bytes: 20, ops: 1})
					require_Equal(t, *mset.inflight["foo2"], inflightSubjectRunningTotal{bytes: 20, ops: 1})
				}
			},
		},
		{
			title:      "discard-new-max-msgs",
			discardNew: true,
			batch: []BatchItem{
				{subject: "foo"},
				{subject: "foo"},
				{subject: "foo", err: ErrMaxMsgs},
			},
			validate: func(mset *stream, commit bool) {
				if !commit {
					require_Len(t, len(mset.inflight), 0)
				} else {
					require_Len(t, len(mset.inflight), 1)
					require_Equal(t, *mset.inflight["foo"], inflightSubjectRunningTotal{bytes: 19 * 3, ops: 3})
				}
			},
		},
		{
			title:      "discard-new-max-bytes",
			discardNew: true,
			batch: []BatchItem{
				{subject: "foo1", msg: bytes.Repeat([]byte("A"), 10)},
				{subject: "foo2", msg: bytes.Repeat([]byte("A"), 11), err: ErrMaxBytes},
			},
			validate: func(mset *stream, commit bool) {
				if !commit {
					require_Len(t, len(mset.inflight), 0)
				} else {
					require_Len(t, len(mset.inflight), 2)
					require_Equal(t, *mset.inflight["foo1"], inflightSubjectRunningTotal{bytes: 30, ops: 1})
					require_Equal(t, *mset.inflight["foo2"], inflightSubjectRunningTotal{bytes: 31, ops: 1})
				}
			},
		},
		{
			title:             "discard-new-max-msgs-per-subj",
			discardNew:        true,
			discardNewPerSubj: true,
			batch: []BatchItem{
				{subject: "foo"},
			},
			validate: func(mset *stream, commit bool) {
				if !commit {
					require_Len(t, len(mset.inflight), 0)
				} else {
					require_Len(t, len(mset.inflight), 1)
					require_Equal(t, *mset.inflight["foo"], inflightSubjectRunningTotal{bytes: 19, ops: 1})
				}
			},
		},
		{
			title:             "discard-new-max-msgs-per-subj-duplicate",
			discardNew:        true,
			discardNewPerSubj: true,
			batch: []BatchItem{
				{subject: "foo"},
				{subject: "foo", err: ErrMaxMsgsPerSubject},
			},
			validate: func(mset *stream, commit bool) {
				require_Len(t, len(mset.inflight), 0)
			},
		},
		{
			title:             "discard-new-max-msgs-per-subj-inflight",
			discardNew:        true,
			discardNewPerSubj: true,
			init: func(mset *stream) {
				mset.inflight = map[string]*inflightSubjectRunningTotal{"foo": {bytes: 123, ops: 1}}
			},
			batch: []BatchItem{
				{subject: "foo", err: ErrMaxMsgsPerSubject},
			},
			validate: func(mset *stream, commit bool) {
				require_Len(t, len(mset.inflight), 1)
				require_Equal(t, *mset.inflight["foo"], inflightSubjectRunningTotal{bytes: 123, ops: 1})
			},
		},
		{
			title:             "discard-new-max-msgs-per-subj-pre-existing",
			discardNew:        true,
			discardNewPerSubj: true,
			init: func(mset *stream) {
				_, _, err := mset.store.StoreMsg("foo", nil, nil, 0)
				require_NoError(t, err)
			},
			batch: []BatchItem{
				{subject: "foo", err: ErrMaxMsgsPerSubject},
			},
			validate: func(mset *stream, commit bool) {
				require_Len(t, len(mset.inflight), 0)
			},
		},
		{
			title: "expect-last-seq",
			batch: []BatchItem{
				{subject: "foo", header: nats.Header{JSExpectedLastSeq: {"0"}}},
				{subject: "bar"},
			},
		},
		{
			title: "expect-last-seq-not-first",
			batch: []BatchItem{
				{subject: "foo"},
				{subject: "bar", header: nats.Header{JSExpectedLastSeq: {"0"}}, err: errors.New("last sequence mismatch")},
			},
		},
		{
			title: "expect-last-seq-invalid-first",
			batch: []BatchItem{
				{subject: "foo", header: nats.Header{JSExpectedLastSeq: {"1"}}, err: errors.New("last sequence mismatch: 1 vs 0")},
			},
		},
		{
			title: "expect-last-seq-invalid",
			batch: []BatchItem{
				{subject: "foo", header: nats.Header{JSExpectedLastSeq: {"0"}}},
				{subject: "bar", header: nats.Header{JSExpectedLastSeq: {"0"}}, err: errors.New("last sequence mismatch: 0 vs 1")},
			},
		},
		{
			title: "expect-per-subj-simple",
			batch: []BatchItem{
				{subject: "foo", header: nats.Header{JSExpectedLastSubjSeq: {"0"}}},
				{subject: "bar", header: nats.Header{JSExpectedLastSubjSeq: {"0"}}},
			},
			validate: func(mset *stream, commit bool) {
				if !commit {
					require_Len(t, len(mset.expectedPerSubjectSequence), 0)
					require_Len(t, len(mset.expectedPerSubjectInProcess), 0)
				} else {
					require_Len(t, len(mset.expectedPerSubjectSequence), 2)
					require_Len(t, len(mset.expectedPerSubjectInProcess), 2)
					require_Equal(t, mset.expectedPerSubjectSequence[0], "foo")
					require_Equal(t, mset.expectedPerSubjectSequence[1], "bar")
				}
			},
		},
		{
			title: "expect-per-subj-redundant-in-batch",
			batch: []BatchItem{
				{
					subject: "foo",
					header:  nats.Header{JSExpectedLastSubjSeq: {"0"}},
				},
				// Would normally fail the batch, recognize in-process.
				{
					subject: "foo",
					header:  nats.Header{JSExpectedLastSubjSeq: {"1"}},
					err:     errors.New("last sequence by subject mismatch: 1 vs 0"),
				},
				// Redundant expected check results in an error. The subject 'foo' is also updated in the batch.
				{
					subject: "foo",
					header:  nats.Header{JSExpectedLastSubjSeq: {"0"}},
					err:     errors.New("last sequence by subject mismatch"),
				},
			},
			validate: func(mset *stream, commit bool) {
				if !commit {
					require_Len(t, len(mset.expectedPerSubjectSequence), 0)
					require_Len(t, len(mset.expectedPerSubjectInProcess), 0)
				} else {
					require_Len(t, len(mset.expectedPerSubjectSequence), 1)
					require_Len(t, len(mset.expectedPerSubjectInProcess), 1)
					require_Equal(t, mset.expectedPerSubjectSequence[0], "foo")
				}
			},
		},
		{
			title: "expect-per-subj-dupe-in-change",
			batch: []BatchItem{
				{subject: "foo", header: nats.Header{JSExpectedLastSubjSeq: {"0"}, JSExpectedLastSubjSeqSubj: {"baz"}}},
				{subject: "bar", header: nats.Header{JSExpectedLastSubjSeq: {"0"}, JSExpectedLastSubjSeqSubj: {"baz"}}},
			},
			validate: func(mset *stream, commit bool) {
				if !commit {
					require_Len(t, len(mset.expectedPerSubjectSequence), 0)
					require_Len(t, len(mset.expectedPerSubjectInProcess), 0)
				} else {
					require_Len(t, len(mset.expectedPerSubjectSequence), 1)
					require_Len(t, len(mset.expectedPerSubjectInProcess), 1)
					require_Equal(t, mset.expectedPerSubjectSequence[1], "baz")
				}
			},
		},
		{
			title: "expect-per-subj-not-first",
			batch: []BatchItem{
				{subject: "foo"},
				// Mismatch because only the first 'foo' can have the expected check.
				{
					subject: "foo",
					header:  nats.Header{JSExpectedLastSubjSeq: {"0"}},
					err:     errors.New("last sequence by subject mismatch"),
				},
				// Mismatch because only the first 'foo' can have the expected check.
				{
					subject: "bar",
					header:  nats.Header{JSExpectedLastSubjSeq: {"0"}, JSExpectedLastSubjSeqSubj: {"foo"}},
					err:     errors.New("last sequence by subject mismatch"),
				},
			},
		},
		{
			title: "expect-per-subj-in-process",
			init: func(mset *stream) {
				mset.expectedPerSubjectInProcess = map[string]struct{}{"foo": {}}
			},
			batch: []BatchItem{
				{subject: "foo", header: nats.Header{JSExpectedLastSubjSeq: {"10"}}, err: errors.New("last sequence by subject mismatch")},
			},
			validate: func(mset *stream, commit bool) {
				require_Len(t, len(mset.expectedPerSubjectSequence), 0)
				require_Len(t, len(mset.expectedPerSubjectInProcess), 1)
			},
		},
		{
			title: "expect-per-subj-inflight",
			init: func(mset *stream) {
				mset.inflight = map[string]*inflightSubjectRunningTotal{"bar": {bytes: 33, ops: 1}}
			},
			batch: []BatchItem{
				{subject: "foo", header: nats.Header{JSExpectedLastSubjSeq: {"10"}, JSExpectedLastSubjSeqSubj: {"bar"}}, err: errors.New("last sequence by subject mismatch")},
			},
			validate: func(mset *stream, commit bool) {
				require_Len(t, len(mset.expectedPerSubjectSequence), 0)
				require_Len(t, len(mset.expectedPerSubjectInProcess), 0)
				require_Len(t, len(mset.inflight), 1)
			},
		},
		{
			title:       "rollup-deny-purge",
			allowRollup: true,
			denyPurge:   true,
			batch: []BatchItem{
				{subject: "foo", header: nats.Header{JSMsgRollup: {JSMsgRollupSubject}}, err: errors.New("rollup not permitted")},
			},
		},
		{
			title:       "rollup-invalid",
			allowRollup: true,
			denyPurge:   false,
			batch: []BatchItem{
				{subject: "foo", header: nats.Header{JSMsgRollup: {"invalid"}}, err: fmt.Errorf("rollup value invalid: %q", "invalid")},
			},
		},
		{
			title:       "rollup-all-first",
			allowRollup: true,
			denyPurge:   false,
			batch: []BatchItem{
				{subject: "foo", header: nats.Header{JSMsgRollup: {JSMsgRollupAll}}},
			},
		},
		{
			title:       "rollup-all-not-first",
			allowRollup: true,
			denyPurge:   false,
			batch: []BatchItem{
				{subject: "foo"},
				{subject: "bar", header: nats.Header{JSMsgRollup: {JSMsgRollupAll}}, err: errors.New("batch rollup all invalid")},
			},
		},
		{
			title:       "rollup-sub-unique",
			allowRollup: true,
			denyPurge:   false,
			batch: []BatchItem{
				{subject: "foo", header: nats.Header{JSMsgRollup: {JSMsgRollupSubject}}},
				{subject: "bar", header: nats.Header{JSMsgRollup: {JSMsgRollupSubject}}},
			},
		},
		{
			title:       "rollup-sub-overlap",
			allowRollup: true,
			denyPurge:   false,
			batch: []BatchItem{
				{subject: "foo", header: nats.Header{JSMsgRollup: {JSMsgRollupSubject}}},
				{subject: "foo", header: nats.Header{JSMsgRollup: {JSMsgRollupSubject}}, err: errors.New("batch rollup sub invalid")},
			},
		},
		{
			title: "subject-transform",
			batch: []BatchItem{
				{subject: "bar", rsubject: "foo"},
				{subject: "bar"},
			},
			validate: func(mset *stream, commit bool) {
				if !commit {
					require_Len(t, len(mset.inflight), 0)
				} else {
					require_Len(t, len(mset.inflight), 1)
					require_Equal(t, *mset.inflight["bar"], inflightSubjectRunningTotal{bytes: 38, ops: 2})
					require_Len(t, len(mset.inflightTransform), 1)
					require_Equal(t, mset.inflightTransform[0], "bar")
				}
			},
		},
	}

	for _, test := range tests {
		t.Run(test.title, func(t *testing.T) {
			s := RunBasicJetStreamServer(t)
			defer s.Shutdown()

			nc := clientConnectToServer(t, s)
			defer nc.Close()

			_, err := jsStreamCreate(t, nc, &StreamConfig{
				Name:               "TEST",
				Storage:            MemoryStorage,
				AllowAtomicPublish: true,
				Subjects:           []string{"foo", "bar", "baz"},
			})
			require_NoError(t, err)

			mset, err := s.globalAccount().lookupStream("TEST")
			require_NoError(t, err)

			if test.init != nil {
				test.init(mset)
			}

			var (
				discard       DiscardPolicy
				discardNewPer bool
				maxMsgs       int64 = -1
				maxMsgsPer    int64 = -1
				maxBytes      int64 = -1
			)
			if test.discardNew {
				discard, maxMsgs, maxBytes = DiscardNew, 2, 60
			}
			if test.discardNewPerSubj {
				require_True(t, test.discardNew)
				discardNewPer, maxMsgs, maxBytes, maxMsgsPer = true, -1, -1, 1
			}

			diff := &batchStagedDiff{}
			mset.clMu.Lock()
			defer mset.clMu.Unlock()
			var hadError bool
			for _, m := range test.batch {
				var hdr []byte
				for key, values := range m.header {
					for _, value := range values {
						hdr = genHeader(hdr, key, value)
					}
				}
				// Potential subject transform.
				rsubject := m.subject
				if m.rsubject != _EMPTY_ {
					rsubject = m.rsubject
				}
				_, _, _, _, err = checkMsgHeadersPreClusteredProposal(diff, mset, m.subject, rsubject, hdr, m.msg, false, "TEST", nil, test.allowRollup, test.denyPurge, test.allowTTL, test.allowMsgCounter, test.allowMsgSchedules, discard, discardNewPer, -1, maxMsgs, maxMsgsPer, maxBytes)
				if m.err != nil {
					require_Error(t, err, m.err)
				} else if err != nil {
					require_NoError(t, err)
				}
				if err != nil {
					hadError = true
				}
				if test.validate != nil {
					test.validate(mset, false)
				}
				mset.clseq++
			}
			if test.validate != nil && !hadError {
				diff.commit(mset)
				test.validate(mset, true)
			}
		})
	}
}

func TestJetStreamCounterNullSourcesHeaderDoesNotPanic(t *testing.T) {
	s := RunBasicJetStreamServer(t)
	defer s.Shutdown()

	nc, js := jsClientConnect(t, s)
	defer nc.Close()

	// Origin counter stream that will be sourced in below.
	_, err := jsStreamCreate(t, nc, &StreamConfig{
		Name:            "O1",
		Subjects:        []string{"foo.*"},
		Storage:         MemoryStorage,
		AllowMsgCounter: true,
	})
	require_NoError(t, err)

	m := nats.NewMsg("foo.1")
	m.Header.Set(JSMessageIncr, "5")
	_, err = js.PublishMsg(m)
	require_NoError(t, err)

	_, err = jsStreamCreate(t, nc, &StreamConfig{
		Name:            "M",
		Subjects:        []string{"foo"},
		Storage:         MemoryStorage,
		AllowMsgCounter: true,
	})
	require_NoError(t, err)

	// A sources header holding a JSON `null` for the origin stream is stored
	// verbatim, and decodes back into a present key with a nil inner map.
	m = nats.NewMsg("foo")
	m.Header.Set(JSMessageIncr, "1")
	m.Header.Set(JSMessageCounterSources, `{"O1":null}`)
	_, err = js.PublishMsg(m)
	require_NoError(t, err)

	mset, err := s.globalAccount().lookupStream("M")
	require_NoError(t, err)
	var smv StoreMsg
	sm, err := mset.store.LoadLastMsg("foo", &smv)
	require_NoError(t, err)
	require_Equal(t, string(sliceHeader(JSMessageCounterSources, sm.hdr)), `{"O1":null}`)

	// That stored record reached through the pre-clustered proposal path. The
	// sourced message has to record its count, which used to be a write into
	// the nil inner map.
	hdr := genHeader(nil, JSMessageIncr, "5")
	hdr = genHeader(hdr, JSStreamSource, "O1 1 > > foo.1")
	diff := &batchStagedDiff{}
	mset.clMu.Lock()
	_, _, _, _, err = checkMsgHeadersPreClusteredProposal(diff, mset, "foo", "foo", hdr, []byte(`{"val":"5"}`), true, "M", nil, false, false, false, true, false, DiscardOld, false, -1, -1, -1, -1)
	mset.clMu.Unlock()
	require_NoError(t, err)
	require_Equal(t, diff.counter["foo"].sources["O1"]["foo.1"], "5")

	// And now the same through the regular sourcing path.
	_, err = jsStreamUpdate(t, nc, &StreamConfig{
		Name:     "M",
		Subjects: []string{"foo"},
		Sources: []*StreamSource{{
			Name:              "O1",
			SubjectTransforms: []SubjectTransformConfig{{Source: "foo.*", Destination: "foo"}},
		}},
		Storage:         MemoryStorage,
		AllowMsgCounter: true,
	})
	require_NoError(t, err)

	checkFor(t, 5*time.Second, 50*time.Millisecond, func() error {
		si, err := js.StreamInfo("M")
		if err != nil {
			return err
		}
		if si.State.Msgs != 2 {
			return fmt.Errorf("expected 2 messages, got %d", si.State.Msgs)
		}
		return nil
	})

	// The counter aggregated the sourced increment, and the sources header was
	// rewritten with a usable inner map.
	rsm, err := js.GetMsg("M", 2)
	require_NoError(t, err)
	var count CounterValue
	require_NoError(t, json.Unmarshal(rsm.Data, &count))
	require_Equal(t, count.Value, "6")
	var sources CounterSources
	require_NoError(t, json.Unmarshal([]byte(rsm.Header.Get(JSMessageCounterSources)), &sources))
	require_Equal(t, sources["O1"]["foo.1"], "5")
}

func TestJetStreamCounterStagingDoesNotCorruptCommittedTotal(t *testing.T) {
	s := RunBasicJetStreamServer(t)
	defer s.Shutdown()

	nc := clientConnectToServer(t, s)
	defer nc.Close()

	_, err := jsStreamCreate(t, nc, &StreamConfig{
		Name:               "TEST",
		Storage:            MemoryStorage,
		AllowAtomicPublish: true,
		Subjects:           []string{"foo"},
	})
	require_NoError(t, err)

	mset, err := s.globalAccount().lookupStream("TEST")
	require_NoError(t, err)

	// Committed total of 2, built via Add so the big.Int has spare backing-array
	// capacity that a shallow copy would share (and an in-place Add corrupt).
	committed := new(big.Int)
	committed.Add(big.NewInt(1), big.NewInt(1))
	committedSources := CounterSources{"S1": {"foo": "2"}}
	mset.clusteredCounterTotal = map[string]*msgCounterRunningTotal{
		"foo": {total: committed, sources: committedSources, ops: 1},
	}

	// Stage an increment without committing the diff; this must not mutate the
	// committed total.
	hdr := genHeader(nil, JSMessageIncr, "3")
	hdr = genHeader(hdr, JSStreamSource, "S1 1 > > foo")
	msg := []byte(`{"val":"5"}`)
	diff := &batchStagedDiff{}
	mset.clMu.Lock()
	_, _, _, _, err = checkMsgHeadersPreClusteredProposal(diff, mset, "foo", "foo", hdr, msg, true, "TEST", nil, false, false, false, true, false, DiscardOld, false, -1, -1, -1, -1)
	mset.clMu.Unlock()
	require_NoError(t, err)

	// Staged total reflects the increment (2+3=5).
	require_Equal(t, diff.counter["foo"].total.String(), "5")
	// Committed total and sources are untouched.
	require_Equal(t, mset.clusteredCounterTotal["foo"].total.String(), "2")
	require_Equal(t, mset.clusteredCounterTotal["foo"].ops, 1)
	require_Equal(t, mset.clusteredCounterTotal["foo"].sources["S1"]["foo"], "2")
}

func TestJetStreamAtomicBatchPublishHighLevelRollback(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "R3S", 3)
	defer c.shutdown()

	nc, js := jsClientConnect(t, c.randomServer())
	defer nc.Close()

	_, err := jsStreamCreate(t, nc, &StreamConfig{
		Name:      "TEST",
		Subjects:  []string{"foo"},
		Replicas:  3,
		Storage:   FileStorage,
		Retention: InterestPolicy,
		MaxMsgs:   1,
	})
	require_NoError(t, err)

	sl := c.streamLeader(globalAccountName, "TEST")
	mset, err := sl.globalAccount().lookupStream("TEST")
	require_NoError(t, err)

	requireEmpty := func() {
		mset.ddMu.Lock()
		ddarr, ddmap := len(mset.ddarr), len(mset.ddmap)
		mset.ddMu.Unlock()
		require_Len(t, ddarr, 0)
		require_Len(t, ddmap, 0)

		mset.clMu.Lock()
		inflight, subjSeq, subjInProcess := len(mset.inflight), len(mset.expectedPerSubjectSequence), len(mset.expectedPerSubjectInProcess)
		mset.clMu.Unlock()
		require_Len(t, inflight, 0)
		require_Len(t, subjSeq, 0)
		require_Len(t, subjInProcess, 0)
	}
	requireEmpty()

	m := nats.NewMsg("foo")
	m.Header.Set("Nats-Msg-Id", "dedupe")
	m.Header.Set("Nats-Expected-Last-Subject-Sequence", "1")
	_, err = js.PublishMsg(m)
	require_Error(t, err, NewJSStreamWrongLastSequenceError(0))
	requireEmpty()
}

func TestJetStreamAtomicBatchPublishExpectedPerSubject(t *testing.T) {
	type TestKind int
	const (
		OnlyFirst TestKind = iota
		Redundant
		NotFirst
	)

	test := func(t *testing.T, kind TestKind) {
		c := createJetStreamClusterExplicit(t, "R3S", 3)
		defer c.shutdown()

		nc, js := jsClientConnect(t, c.randomServer())
		defer nc.Close()

		_, err := jsStreamCreate(t, nc, &StreamConfig{
			Name:               "TEST",
			Subjects:           []string{"foo"},
			Storage:            FileStorage,
			Replicas:           3,
			AllowAtomicPublish: true,
		})
		require_NoError(t, err)

		_, err = js.Publish("foo", nil)
		require_NoError(t, err)

		m := nats.NewMsg("foo")
		if kind != NotFirst {
			m.Header.Set("Nats-Expected-Last-Subject-Sequence", "1")
		}
		m.Header.Set("Nats-Batch-Id", "uuid")
		m.Header.Set("Nats-Batch-Sequence", "1")
		require_NoError(t, nc.PublishMsg(m))

		var pubAck JSPubAckResponse

		// Redundant expected headers are okay, as long as they reflect the state prior to the batch.
		m = nats.NewMsg("foo")
		if kind == Redundant || kind == NotFirst {
			m.Header.Set("Nats-Expected-Last-Subject-Sequence", "1")
		}
		m.Header.Set("Nats-Batch-Id", "uuid")
		m.Header.Set("Nats-Batch-Sequence", "2")
		m.Header.Set("Nats-Batch-Commit", "1")
		rmsg, err := nc.RequestMsg(m, time.Second)
		require_NoError(t, err)
		require_NoError(t, json.Unmarshal(rmsg.Data, &pubAck))
		if kind == Redundant || kind == NotFirst {
			require_NotNil(t, pubAck.Error)
			require_Error(t, pubAck.Error, NewJSStreamWrongLastSequenceConstantError())
			return
		}
		require_True(t, pubAck.Error == nil)
		require_Equal(t, pubAck.Sequence, 3)
		require_Equal(t, pubAck.BatchSize, 2)

		// The first message still contains the expected headers.
		rsm, err := js.GetMsg("TEST", 2)
		require_NoError(t, err)
		require_Equal(t, rsm.Header.Get("Nats-Batch-Id"), "uuid")
		require_Equal(t, rsm.Header.Get("Nats-Batch-Sequence"), "1")
		require_Equal(t, rsm.Header.Get("Nats-Expected-Last-Subject-Sequence"), "1")

		// The second message doesn't have the expected headers, as the condition was already checked
		// and seems inconsistent when getting the message afterward.
		rsm, err = js.GetMsg("TEST", 3)
		require_NoError(t, err)
		require_Equal(t, rsm.Header.Get("Nats-Batch-Id"), "uuid")
		require_Equal(t, rsm.Header.Get("Nats-Batch-Sequence"), "2")
		require_Equal(t, rsm.Header.Get("Nats-Expected-Last-Subject-Sequence"), _EMPTY_)
	}

	t.Run("single", func(t *testing.T) { test(t, OnlyFirst) })
	t.Run("redundant", func(t *testing.T) { test(t, Redundant) })
	t.Run("not-first", func(t *testing.T) { test(t, NotFirst) })
}

func TestJetStreamAtomicBatchPublishCounterSingleServer(t *testing.T) {
	test := func(t *testing.T, storage StorageType) {
		s := RunBasicJetStreamServer(t)
		defer s.Shutdown()

		nc, js := jsClientConnect(t, s)
		defer nc.Close()

		cfg := &StreamConfig{
			Name:               "TEST",
			Subjects:           []string{"foo"},
			Storage:            storage,
			Retention:          LimitsPolicy,
			Replicas:           1,
			AllowAtomicPublish: true,
			AllowMsgCounter:    true,
		}
		_, err := jsStreamCreate(t, nc, cfg)
		require_NoError(t, err)

		// Publish an atomic batch of two counter increments.
		m := nats.NewMsg("foo")
		m.Header.Set("Nats-Batch-Id", "uuid")
		m.Header.Set("Nats-Batch-Sequence", "1")
		m.Header.Set("Nats-Incr", "1")
		require_NoError(t, nc.PublishMsg(m))

		m = nats.NewMsg("foo")
		m.Header.Set("Nats-Batch-Id", "uuid")
		m.Header.Set("Nats-Batch-Sequence", "2")
		m.Header.Set("Nats-Batch-Commit", "1")
		m.Header.Set("Nats-Incr", "2")
		rmsg, err := nc.RequestMsg(m, time.Second)
		require_NoError(t, err)

		var pubAck JSPubAckResponse
		require_NoError(t, json.Unmarshal(rmsg.Data, &pubAck))
		require_True(t, pubAck.Error == nil)
		require_Equal(t, pubAck.Sequence, 2)
		require_Equal(t, pubAck.BatchId, "uuid")
		require_Equal(t, pubAck.BatchSize, 2)

		// The stored messages must contain the rewritten counter payloads.
		rsm, err := js.GetMsg("TEST", 1)
		require_NoError(t, err)
		require_Equal(t, string(rsm.Data), `{"val":"1"}`)
		rsm, err = js.GetMsg("TEST", 2)
		require_NoError(t, err)
		require_Equal(t, string(rsm.Data), `{"val":"3"}`)

		// The counter must not be broken, a normal increment should still work.
		m = nats.NewMsg("foo")
		m.Header.Set("Nats-Incr", "5")
		pa, err := js.PublishMsg(m)
		require_NoError(t, err)
		require_Equal(t, pa.Sequence, 3)
		rsm, err = js.GetMsg("TEST", 3)
		require_NoError(t, err)
		require_Equal(t, string(rsm.Data), `{"val":"8"}`)
	}

	for _, storage := range []StorageType{FileStorage, MemoryStorage} {
		t.Run(storage.String(), func(t *testing.T) {
			test(t, storage)
		})
	}
}

func TestJetStreamAtomicBatchPublishSingleServerRecovery(t *testing.T) {
	s := RunBasicJetStreamServer(t)
	defer s.Shutdown()

	nc := clientConnectToServer(t, s)
	defer nc.Close()

	cfg := &StreamConfig{
		Name:               "TEST",
		Subjects:           []string{"foo"},
		Storage:            FileStorage,
		Retention:          LimitsPolicy,
		Replicas:           1,
		AllowAtomicPublish: true,
	}
	_, err := jsStreamCreate(t, nc, cfg)
	require_NoError(t, err)

	// Manually construct and store a batch, so it doesn't immediately commit.
	mset, err := s.globalAccount().lookupStream("TEST")
	require_NoError(t, err)
	mset.mu.Lock()
	batches := &batching{}
	mset.batches = batches
	jsa := mset.jsa
	mset.mu.Unlock()
	jsa.mu.RLock()
	storeDir := jsa.storeDir
	jsa.mu.RUnlock()
	batches.mu.Lock()
	b, err := batches.newAtomicBatch(mset, "uuid", 1, FileStorage, storeDir, "TEST")
	if err != nil {
		batches.mu.Unlock()
		require_NoError(t, err)
	}
	hdr1 := genHeader(nil, "Nats-Batch-Id", "uuid")
	hdr1 = genHeader(hdr1, "Nats-Batch-Sequence", "1")
	_, _, err = b.store.StoreMsg("foo", hdr1, nil, 0)
	if err != nil {
		batches.mu.Unlock()
		require_NoError(t, err)
	}

	hdr2 := genHeader(nil, "Nats-Batch-Id", "uuid")
	hdr2 = genHeader(hdr2, "Nats-Batch-Sequence", "2")
	hdr2 = genHeader(hdr2, "Nats-Batch-Commit", "1")
	_, _, err = b.store.StoreMsg("foo", hdr2, nil, 0)
	if err != nil {
		batches.mu.Unlock()
		require_NoError(t, err)
	}
	abandonReason := b.readyForCommit()
	batches.mu.Unlock()
	require_True(t, abandonReason == nil)

	// Simulate the first message of the batch is committed.
	err = mset.processJetStreamMsg("foo", _EMPTY_, hdr1, nil, 0, 0, nil, false, true)
	require_NoError(t, err)

	// Simulate a hard kill, upon recovery the rest of the batch should be applied.
	port := s.opts.Port
	sd := s.StoreDir()
	nc.Close()
	s.Shutdown()
	s = RunJetStreamServerOnPort(port, sd)
	defer s.Shutdown()

	mset, err = s.globalAccount().lookupStream("TEST")
	require_NoError(t, err)
	state := mset.state()
	require_Equal(t, state.Msgs, 2)
	require_Equal(t, state.FirstSeq, 1)
	require_Equal(t, state.LastSeq, 2)

	for seq := state.FirstSeq; seq <= state.LastSeq; seq++ {
		sm, err := mset.getMsg(seq)
		require_NoError(t, err)
		require_Equal(t, string(getHeader("Nats-Batch-Id", sm.Header)), "uuid")
		require_Equal(t, string(getHeader("Nats-Batch-Sequence", sm.Header)), strconv.FormatUint(seq, 10))
		// The last message should have the commit header set.
		if seq == state.LastSeq {
			require_Equal(t, string(getHeader("Nats-Batch-Commit", sm.Header)), "1")
		}
	}
}

func TestJetStreamAtomicBatchStoreReadyForCommitSyncs(t *testing.T) {
	s := RunBasicJetStreamServer(t)
	defer s.Shutdown()

	nc := clientConnectToServer(t, s)
	defer nc.Close()

	_, err := jsStreamCreate(t, nc, &StreamConfig{
		Name:               "TEST",
		Subjects:           []string{"foo"},
		Storage:            FileStorage,
		Replicas:           1,
		AllowAtomicPublish: true,
	})
	require_NoError(t, err)

	mset, err := s.globalAccount().lookupStream("TEST")
	require_NoError(t, err)
	mset.jsa.mu.RLock()
	storeDir := mset.jsa.storeDir
	mset.jsa.mu.RUnlock()

	store, err := newBatchStore(mset, "durable-staging", 1, FileStorage, storeDir, "TEST")
	require_NoError(t, err)
	defer store.Delete(true)

	fs := store.(*fileStore)
	require_True(t, fs.fcfg.AsyncFlush)
	require_True(t, fs.syncOnFlush.Load())

	_, _, err = store.StoreMsg("foo", nil, []byte("payload"), 0)
	require_NoError(t, err)

	fs.mu.RLock()
	blks := append([]*msgBlock(nil), fs.blks...)
	fs.mu.RUnlock()
	require_NotEqual(t, len(blks), 0)

	var pendingOrDirty bool
	for _, mb := range blks {
		mb.mu.RLock()
		pendingOrDirty = pendingOrDirty || mb.pendingWriteSizeLocked() > 0 || mb.needSync
		mb.mu.RUnlock()
	}
	require_True(t, pendingOrDirty)

	b := &atomicBatch{timer: time.NewTimer(time.Minute), store: store}
	require_True(t, b.readyForCommit() == nil)

	for _, mb := range blks {
		mb.mu.RLock()
		pending, needsSync := mb.pendingWriteSizeLocked(), mb.needSync
		mb.mu.RUnlock()
		require_Equal(t, pending, 0)
		require_False(t, needsSync)
	}
}

func TestJetStreamAtomicBatchPublishFinalSyncState(t *testing.T) {
	type result struct {
		responseErr     error
		trace           MsgTraceJetStream
		traceBeforeSync bool
	}
	test := func(t *testing.T, mutate func(*stream, *fileStore) func()) result {
		storeDir := t.TempDir()
		conf := createConfFile(t, []byte(fmt.Sprintf(`
			listen: 127.0.0.1:-1
			jetstream: {
				store_dir: %q
				sync_interval: always
			}
		`, storeDir)))
		s, _ := RunServerWithConfig(conf)
		defer s.Shutdown()

		nc, _ := jsClientConnect(t, s)
		defer nc.Close()
		_, err := jsStreamCreate(t, nc, &StreamConfig{
			Name:               "TEST",
			Subjects:           []string{"state.*"},
			Storage:            FileStorage,
			Replicas:           1,
			AllowAtomicPublish: true,
		})
		require_NoError(t, err)
		mset, err := s.globalAccount().lookupStream("TEST")
		require_NoError(t, err)
		fs := mset.store.(*fileStore)

		reply := nats.NewInbox()
		sub, err := nc.SubscribeSync(reply)
		require_NoError(t, err)
		traceSub := natsSubSync(t, nc, nats.NewInbox())
		require_NoError(t, nc.Flush())

		first := nats.NewMsg("state.a")
		first.Header.Set(JSBatchId, "sync-state")
		first.Header.Set(JSBatchSeq, "1")
		require_NoError(t, nc.PublishMsg(first))
		require_NoError(t, nc.Flush())

		fs.syncMu.Lock()
		locked := true
		defer func() {
			if locked {
				fs.syncMu.Unlock()
			}
		}()
		commit := nats.NewMsg("state.b")
		commit.Reply = reply
		commit.Header.Set(JSBatchId, "sync-state")
		commit.Header.Set(JSBatchSeq, "2")
		commit.Header.Set(JSBatchCommit, "1")
		commit.Header.Set(MsgTraceDest, traceSub.Subject)
		require_NoError(t, nc.PublishMsg(commit))

		checkFor(t, 2*time.Second, 10*time.Millisecond, func() error {
			mset.mu.RLock()
			lseq := mset.lseq
			mset.mu.RUnlock()
			if lseq != 2 || !fs.syncOnFlush.Load() {
				return fmt.Errorf("atomic batch has not reached final sync")
			}
			return nil
		})
		mset.mu.RLock()
		batches := mset.batches
		mset.mu.RUnlock()
		batches.mu.Lock()
		batch := batches.atomic["sync-state"]
		batches.mu.Unlock()
		require_NotNil(t, batch)
		staging := batch.store.(*fileStore)
		staging.mu.RLock()
		stagingBlocks := append([]*msgBlock(nil), staging.blks...)
		staging.mu.RUnlock()
		require_NotEqual(t, len(stagingBlocks), 0)
		for _, mb := range stagingBlocks {
			mb.mu.RLock()
			pending, needsSync := mb.pendingWriteSizeLocked(), mb.needSync
			mb.mu.RUnlock()
			require_Equal(t, pending, 0)
			require_False(t, needsSync)
		}
		_, ackErr := sub.NextMsg(50 * time.Millisecond)
		require_Error(t, ackErr, nats.ErrTimeout)
		traceMsg, traceErr := traceSub.NextMsg(50 * time.Millisecond)
		traceBeforeSync := traceErr == nil
		if traceErr != nil {
			require_Error(t, traceErr, nats.ErrTimeout)
		}
		cleanup := mutate(mset, fs)
		if cleanup != nil {
			defer cleanup()
		}
		fs.syncMu.Unlock()
		locked = false
		if traceMsg == nil {
			traceMsg = natsNexMsg(t, traceSub, time.Second)
		}
		var traceEvent MsgTraceEvent
		require_NoError(t, json.Unmarshal(traceMsg.Data, &traceEvent))
		jsTrace := traceEvent.JetStream()
		require_NotNil(t, jsTrace)
		response, err := sub.NextMsg(250 * time.Millisecond)
		if err == nil {
			var ack JSPubAckResponse
			require_NoError(t, json.Unmarshal(response.Data, &ack))
			require_True(t, ack.Error == nil)
			require_Equal(t, ack.Sequence, 2)
			require_Equal(t, ack.BatchId, "sync-state")
			require_Equal(t, ack.BatchSize, 2)
		}
		require_True(t, fs.syncAlways.Load())
		require_False(t, fs.syncOnFlush.Load())
		return result{err, *jsTrace, traceBeforeSync}
	}

	t.Run("RespondsAfterSync", func(t *testing.T) {
		result := test(t, func(_ *stream, _ *fileStore) func() {
			return nil
		})
		require_NoError(t, result.responseErr)
		require_False(t, result.traceBeforeSync)
		require_Equal(t, result.trace.Error, _EMPTY_)
	})

	t.Run("RecordsSyncFailure", func(t *testing.T) {
		writeErr := errors.New("atomic batch final sync failed")
		var mset *stream
		result := test(t, func(s *stream, fs *fileStore) func() {
			mset = s
			fs.mu.RLock()
			lmb := fs.lmb
			fs.mu.RUnlock()
			lmb.mu.Lock()
			lmb.werr = writeErr
			lmb.mu.Unlock()
			return nil
		})
		require_Error(t, result.responseErr, nats.ErrTimeout)
		require_False(t, result.traceBeforeSync)
		require_Equal(t, result.trace.Error, writeErr.Error())
		checkFor(t, time.Second, 10*time.Millisecond, func() error {
			if !errors.Is(mset.getWriteErr(), writeErr) {
				return errors.New("stream did not record final sync failure")
			}
			return nil
		})
	})
}

func TestJetStreamAtomicBatchPublishTraceOnlyNoAck(t *testing.T) {
	storeDir := t.TempDir()
	conf := createConfFile(t, []byte(fmt.Sprintf(`
		listen: 127.0.0.1:-1
		jetstream: {
			store_dir: %q
			sync_interval: always
		}
	`, storeDir)))
	s, _ := RunServerWithConfig(conf)
	defer s.Shutdown()

	nc, js := jsClientConnect(t, s)
	defer nc.Close()
	_, err := jsStreamCreate(t, nc, &StreamConfig{
		Name:               "TEST",
		Subjects:           []string{"state.*"},
		Storage:            FileStorage,
		Replicas:           1,
		AllowAtomicPublish: true,
	})
	require_NoError(t, err)

	replySub := natsSubSync(t, nc, nats.NewInbox())
	traceSub := natsSubSync(t, nc, nats.NewInbox())
	require_NoError(t, nc.Flush())

	commit := nats.NewMsg("state.a")
	commit.Reply = replySub.Subject
	commit.Header.Set(JSBatchId, "trace-only")
	commit.Header.Set(JSBatchSeq, "1")
	commit.Header.Set(JSBatchCommit, "1")
	commit.Header.Set(MsgTraceDest, traceSub.Subject)
	commit.Header.Set(MsgTraceOnly, "true")
	require_NoError(t, nc.PublishMsg(commit))

	traceMsg := natsNexMsg(t, traceSub, time.Second)
	var traceEvent MsgTraceEvent
	require_NoError(t, json.Unmarshal(traceMsg.Data, &traceEvent))
	jsTrace := traceEvent.JetStream()
	require_NotNil(t, jsTrace)
	require_Equal(t, jsTrace.Error, _EMPTY_)

	_, err = replySub.NextMsg(250 * time.Millisecond)
	require_Error(t, err, nats.ErrTimeout)
	streamInfo, err := js.StreamInfo("TEST")
	require_NoError(t, err)
	require_Equal(t, streamInfo.State.Msgs, 0)
}

func TestJetStreamAtomicBatchPublishSingleServerRecoveryCommitEob(t *testing.T) {
	s := RunBasicJetStreamServer(t)
	defer s.Shutdown()

	nc := clientConnectToServer(t, s)
	defer nc.Close()

	cfg := &StreamConfig{
		Name:               "TEST",
		Subjects:           []string{"foo"},
		Storage:            FileStorage,
		Retention:          LimitsPolicy,
		Replicas:           1,
		AllowAtomicPublish: true,
	}
	_, err := jsStreamCreate(t, nc, cfg)
	require_NoError(t, err)

	// Manually construct and store a batch, so it doesn't immediately commit.
	mset, err := s.globalAccount().lookupStream("TEST")
	require_NoError(t, err)
	mset.mu.Lock()
	batches := &batching{}
	mset.batches = batches
	jsa := mset.jsa
	mset.mu.Unlock()
	jsa.mu.RLock()
	storeDir := jsa.storeDir
	jsa.mu.RUnlock()
	batches.mu.Lock()
	b, err := batches.newAtomicBatch(mset, "uuid", 1, FileStorage, storeDir, "TEST")
	if err != nil {
		batches.mu.Unlock()
		require_NoError(t, err)
	}
	hdr1 := genHeader(nil, "Nats-Batch-Id", "uuid")
	hdr1 = genHeader(hdr1, "Nats-Batch-Sequence", "1")
	_, _, err = b.store.StoreMsg("foo", hdr1, nil, 0)
	if err != nil {
		batches.mu.Unlock()
		require_NoError(t, err)
	}

	hdr2 := genHeader(nil, "Nats-Batch-Id", "uuid")
	hdr2 = genHeader(hdr2, "Nats-Batch-Sequence", "2")
	_, _, err = b.store.StoreMsg("foo", hdr2, nil, 0)
	if err != nil {
		batches.mu.Unlock()
		require_NoError(t, err)
	}

	hdr3 := genHeader(nil, "Nats-Batch-Id", "uuid")
	hdr3 = genHeader(hdr3, "Nats-Batch-Sequence", "3")
	hdr3 = genHeader(hdr3, "Nats-Batch-Commit", "eob")
	_, _, err = b.store.StoreMsg("foo", hdr3, nil, 0)
	if err != nil {
		batches.mu.Unlock()
		require_NoError(t, err)
	}
	abandonReason := b.readyForCommit()
	batches.mu.Unlock()
	require_True(t, abandonReason == nil)

	// Simulate the first message of the batch is committed.
	err = mset.processJetStreamMsg("foo", _EMPTY_, hdr1, nil, 0, 0, nil, false, true)
	require_NoError(t, err)

	// Simulate a hard kill, upon recovery the rest of the batch should be applied.
	port := s.opts.Port
	sd := s.StoreDir()
	nc.Close()
	s.Shutdown()
	s = RunJetStreamServerOnPort(port, sd)
	defer s.Shutdown()

	mset, err = s.globalAccount().lookupStream("TEST")
	require_NoError(t, err)
	state := mset.state()
	require_Equal(t, state.Msgs, 2)
	require_Equal(t, state.FirstSeq, 1)
	require_Equal(t, state.LastSeq, 2)

	for seq := state.FirstSeq; seq <= state.LastSeq; seq++ {
		sm, err := mset.getMsg(seq)
		require_NoError(t, err)
		require_Equal(t, string(getHeader("Nats-Batch-Id", sm.Header)), "uuid")
		require_Equal(t, string(getHeader("Nats-Batch-Sequence", sm.Header)), strconv.FormatUint(seq, 10))
		// The last message should have the commit header set, even though the commit was done via EOB.
		if seq == state.LastSeq {
			require_Equal(t, string(getHeader("Nats-Batch-Commit", sm.Header)), "1")
		}
	}
}

func TestJetStreamAtomicBatchPublishEncode(t *testing.T) {
	test := func(t *testing.T, commit bool, compress bool) {
		ts := time.Now().UnixNano()
		hdr := genHeader(nil, "Nats-Batch-Id", "uuid")
		hdr = genHeader(hdr, "Nats-Batch-Sequence", "1")
		msg := []byte("A")
		if compress {
			msg = bytes.Repeat(msg, compressThreshold)
		}
		esm := encodeStreamMsgAllowCompressAndBatch("foo", "reply", hdr, msg, 1, ts, false, "uuid", 1, commit)

		buf, op := esm[1:], entryOp(esm[0])
		if commit {
			require_Equal(t, op, batchCommitMsgOp)
		} else {
			require_Equal(t, op, batchMsgOp)
		}

		batchId, batchSeq, op, mbuf, err := decodeBatchMsg(buf)
		require_NoError(t, err)
		require_Equal(t, batchId, "uuid")
		require_Equal(t, batchSeq, 1)
		if compress {
			require_Equal(t, op, compressedStreamMsgOp)
			mbuf, err = s2.Decode(nil, mbuf)
			require_NoError(t, err)
		} else {
			require_Equal(t, op, streamMsgOp)
		}

		subject, reply, dhdr, dmsg, lseq, dts, sourced, err := decodeStreamMsg(mbuf)
		require_NoError(t, err)
		require_Equal(t, subject, "foo")
		require_Equal(t, reply, "reply")
		require_True(t, bytes.Equal(dhdr, hdr))
		require_True(t, bytes.Equal(dmsg, msg))
		require_Equal(t, lseq, 1)
		require_Equal(t, dts, ts)
		require_False(t, sourced)
	}

	t.Run("normal", func(t *testing.T) { test(t, false, false) })
	t.Run("normal-compress", func(t *testing.T) { test(t, false, true) })
	t.Run("commit", func(t *testing.T) { test(t, true, false) })
	t.Run("commit-compress", func(t *testing.T) { test(t, true, true) })
}

// Test a batch within a single proposal, optionally combined with messages unrelated
// to the batch but within the same proposal.
func TestJetStreamAtomicBatchPublishProposeOne(t *testing.T) {
	test := func(t *testing.T, combined bool) {
		c := createJetStreamClusterExplicit(t, "R3S", 3)
		defer c.shutdown()

		nc, js := jsClientConnect(t, c.randomServer())
		defer nc.Close()

		_, err := jsStreamCreate(t, nc, &StreamConfig{
			Name:               "TEST",
			Subjects:           []string{"foo"},
			Storage:            FileStorage,
			Replicas:           3,
			AllowAtomicPublish: true,
		})
		require_NoError(t, err)

		sl := c.streamLeader(globalAccountName, "TEST")
		mset, err := sl.globalAccount().lookupStream("TEST")
		require_NoError(t, err)

		pubAck, err := js.Publish("foo", nil)
		require_NoError(t, err)
		require_Equal(t, pubAck.Sequence, 1)

		var entries []*Entry

		mset.clMu.Lock()
		if combined {
			esm := encodeStreamMsg("foo", _EMPTY_, nil, nil, mset.clseq, 0, false)
			entries = append(entries, newEntry(EntryNormal, esm))
			mset.clseq++
		}

		msg := []byte("hello")
		hdr := genHeader(nil, "Nats-Batch-Id", "uuid")
		hdr = setHeader("Nats-Batch-Sequence", "1", hdr)
		esm := encodeStreamMsgAllowCompressAndBatch("foo", _EMPTY_, hdr, msg, mset.clseq, 0, false, "uuid", 1, false)
		entries = append(entries, newEntry(EntryNormal, esm))
		mset.clseq++

		hdr = setHeader("Nats-Batch-Sequence", "2", hdr)
		hdr = setHeader("Nats-Batch-Commit", "1", hdr)
		esm = encodeStreamMsgAllowCompressAndBatch("foo", _EMPTY_, hdr, msg, mset.clseq, 0, false, "uuid", 2, true)
		entries = append(entries, newEntry(EntryNormal, esm))
		mset.clseq++

		if combined {
			esm = encodeStreamMsg("foo", _EMPTY_, nil, nil, mset.clseq, 0, false)
			entries = append(entries, newEntry(EntryNormal, esm))
			mset.clseq++
		}
		mset.clMu.Unlock()
		n := mset.raftNode().(*raft)
		n.sendAppendEntry(entries)

		pubAck, err = js.Publish("foo", nil)
		require_NoError(t, err)
		if combined {
			require_Equal(t, pubAck.Sequence, 6)
		} else {
			require_Equal(t, pubAck.Sequence, 4)
		}
	}

	t.Run("single", func(t *testing.T) { test(t, false) })
	t.Run("combined", func(t *testing.T) { test(t, true) })
}

// Test a batch spanning multiple proposals, optionally combined with messages unrelated
// to the batch but within the same first/last proposal.
func TestJetStreamAtomicBatchPublishProposeMultiple(t *testing.T) {
	test := func(t *testing.T, partial bool, combined bool) {
		c := createJetStreamClusterExplicit(t, "R3S", 3)
		defer c.shutdown()

		nc, js := jsClientConnect(t, c.randomServer())
		defer nc.Close()

		_, err := jsStreamCreate(t, nc, &StreamConfig{
			Name:               "TEST",
			Subjects:           []string{"foo"},
			Storage:            FileStorage,
			Replicas:           3,
			AllowAtomicPublish: true,
		})
		require_NoError(t, err)

		sl := c.streamLeader(globalAccountName, "TEST")
		mset, err := sl.globalAccount().lookupStream("TEST")
		require_NoError(t, err)

		pubAck, err := js.Publish("foo", nil)
		require_NoError(t, err)
		require_Equal(t, pubAck.Sequence, 1)

		var entries []*Entry
		mset.clMu.Lock()
		hdr := genHeader(nil, "Nats-Batch-Id", "uuid")
		hdr = genHeader(hdr, "Nats-Batch-Sequence", "1")
		msg := []byte("hello")
		if combined {
			esm := encodeStreamMsg("foo", _EMPTY_, nil, nil, mset.clseq, 0, false)
			entries = append(entries, newEntry(EntryNormal, esm))
			mset.clseq++
		}
		esm := encodeStreamMsgAllowCompressAndBatch("foo", _EMPTY_, hdr, msg, mset.clseq, 0, false, "uuid", 1, false)
		entries = append(entries, newEntry(EntryNormal, esm))
		mset.clseq++
		mset.clMu.Unlock()
		n := mset.raftNode().(*raft)
		n.sendAppendEntry(entries)

		mset.clMu.Lock()
		hdr = setHeader("Nats-Batch-Sequence", "2", hdr)
		esm = encodeStreamMsgAllowCompressAndBatch("foo", _EMPTY_, hdr, msg, mset.clseq, 0, false, "uuid", 2, false)
		mset.clseq++
		mset.clMu.Unlock()
		n.sendAppendEntry([]*Entry{newEntry(EntryNormal, esm)})

		entries = nil
		mset.clMu.Lock()
		if !partial {
			hdr = setHeader("Nats-Batch-Sequence", "3", hdr)
			hdr = genHeader(hdr, "Nats-Batch-Commit", "1")
			esm = encodeStreamMsgAllowCompressAndBatch("foo", _EMPTY_, hdr, msg, mset.clseq, 0, false, "uuid", 3, true)
			entries = append(entries, newEntry(EntryNormal, esm))
			mset.clseq++
		}
		if combined {
			esm = encodeStreamMsg("foo", _EMPTY_, nil, nil, mset.clseq, 0, false)
			entries = append(entries, newEntry(EntryNormal, esm))
			mset.clseq++
		}
		mset.clMu.Unlock()
		n.sendAppendEntry(entries)

		pubAck, err = js.Publish("foo", nil)
		require_NoError(t, err)
		expectedSeq := uint64(2)
		if !partial {
			expectedSeq += 3
		}
		if combined {
			expectedSeq += 2
		}
		require_Equal(t, pubAck.Sequence, expectedSeq)
	}

	t.Run("partial", func(t *testing.T) { test(t, true, false) })
	t.Run("partial-combined", func(t *testing.T) { test(t, true, true) })
	t.Run("full", func(t *testing.T) { test(t, false, false) })
	t.Run("full-combined", func(t *testing.T) { test(t, false, true) })
}

// Test a batch that was only partially proposed.
func TestJetStreamAtomicBatchPublishProposeOnePartialBatch(t *testing.T) {
	maxEntries := 3
	for i := range maxEntries + 1 {
		t.Run(fmt.Sprintf("I-%d", i), func(t *testing.T) {
			c := createJetStreamClusterExplicit(t, "R3S", 3)
			defer c.shutdown()

			nc, js := jsClientConnect(t, c.randomServer())
			defer nc.Close()

			_, err := jsStreamCreate(t, nc, &StreamConfig{
				Name:               "TEST",
				Subjects:           []string{"foo"},
				Storage:            FileStorage,
				Replicas:           3,
				AllowAtomicPublish: true,
			})
			require_NoError(t, err)

			sl := c.streamLeader(globalAccountName, "TEST")
			mset, err := sl.globalAccount().lookupStream("TEST")
			require_NoError(t, err)

			pubAck, err := js.Publish("foo", nil)
			require_NoError(t, err)
			require_Equal(t, pubAck.Sequence, 1)

			var entries []*Entry
			mset.clMu.Lock()
			msg := []byte("hello")
			hdr := genHeader(nil, "Nats-Batch-Id", "uuid")
			for j := range 3 {
				// Skip if indices equal.
				if i == j {
					continue
				}
				bseq := uint64(j + 1)
				hdr = setHeader("Nats-Batch-Sequence", strconv.FormatUint(bseq, 10), hdr)
				commit := bseq == uint64(maxEntries)
				if commit {
					hdr = setHeader("Nats-Batch-Commit", "1", hdr)
				}
				esm := encodeStreamMsgAllowCompressAndBatch("foo", _EMPTY_, hdr, msg, mset.clseq, 0, false, "uuid", bseq, commit)
				entries = append(entries, newEntry(EntryNormal, esm))
				mset.clseq++
			}
			mset.clMu.Unlock()
			n := mset.raftNode().(*raft)
			n.sendAppendEntry(entries)

			pubAck, err = js.Publish("foo", nil)
			require_NoError(t, err)
			expectedSeq := uint64(2)
			if i >= maxEntries {
				expectedSeq += uint64(maxEntries)
			}
			require_Equal(t, pubAck.Sequence, expectedSeq)
		})
	}
}

// Test multiple sequential batches, the first batch is partially proposed.
func TestJetStreamAtomicBatchPublishProposeMultiplePartialBatches(t *testing.T) {
	test := func(t *testing.T, batchSize int, retry bool) {
		c := createJetStreamClusterExplicit(t, "R3S", 3)
		defer c.shutdown()

		nc, js := jsClientConnect(t, c.randomServer())
		defer nc.Close()

		_, err := jsStreamCreate(t, nc, &StreamConfig{
			Name:               "TEST",
			Subjects:           []string{"foo"},
			Storage:            FileStorage,
			Replicas:           3,
			AllowAtomicPublish: true,
		})
		require_NoError(t, err)

		sl := c.streamLeader(globalAccountName, "TEST")
		mset, err := sl.globalAccount().lookupStream("TEST")
		require_NoError(t, err)

		pubAck, err := js.Publish("foo", nil)
		require_NoError(t, err)
		require_Equal(t, pubAck.Sequence, 1)

		batchId1, batchId2 := "ID_1", "ID_2"
		if retry {
			batchId1, batchId2 = "uuid", "uuid"
		}

		var entries []*Entry
		mset.clMu.Lock()
		msg := []byte("hello")
		hdr := genHeader(nil, "Nats-Batch-Id", batchId1)
		hdr = setHeader("Nats-Batch-Sequence", "1", hdr)
		esm := encodeStreamMsgAllowCompressAndBatch("foo", _EMPTY_, hdr, msg, mset.clseq, 0, false, batchId1, 1, false)
		entries = append(entries, newEntry(EntryNormal, esm))
		mset.clseq++

		for j := range batchSize {
			bseq := uint64(j + 1)
			hdr = genHeader(nil, "Nats-Batch-Id", batchId2)
			hdr = setHeader("Nats-Batch-Sequence", strconv.FormatUint(bseq, 10), hdr)
			commit := bseq == uint64(batchSize)
			if commit {
				hdr = setHeader("Nats-Batch-Commit", "1", hdr)
			}
			esm = encodeStreamMsgAllowCompressAndBatch("foo", _EMPTY_, hdr, msg, mset.clseq, 0, false, batchId2, bseq, commit)
			entries = append(entries, newEntry(EntryNormal, esm))
			mset.clseq++
		}
		mset.clMu.Unlock()
		n := mset.raftNode().(*raft)
		n.sendAppendEntry(entries)

		pubAck, err = js.Publish("foo", nil)
		require_NoError(t, err)
		require_Equal(t, pubAck.Sequence, uint64(2+batchSize))

		// Validate the stream only committed the full batch.
		rsm, err := js.GetMsg("TEST", 1)
		require_NoError(t, err)
		require_True(t, rsm.Header.Get("Nats-Batch-Id") == _EMPTY_)
		for j := range batchSize {
			rsm, err = js.GetMsg("TEST", uint64(2+j))
			require_NoError(t, err)
			require_Equal(t, rsm.Header.Get("Nats-Batch-Id"), batchId2)
			require_Equal(t, rsm.Header.Get("Nats-Batch-Sequence"), strconv.Itoa(j+1))
		}
		rsm, err = js.GetMsg("TEST", uint64(2+batchSize))
		require_NoError(t, err)
		require_True(t, rsm.Header.Get("Nats-Batch-Id") == _EMPTY_)

		mset.clMu.Lock()
		clfs := mset.clfs
		mset.clMu.Unlock()
		require_Equal(t, clfs, 1)
	}
	for _, retry := range []bool{false, true} {
		for i := range 2 {
			batchSize := i + 1
			title := fmt.Sprintf("B-%d", batchSize)
			if retry {
				title += "/Retry"
			} else {
				title += "/NoRetry"
			}
			t.Run(title, func(t *testing.T) {
				test(t, batchSize, retry)
			})
		}
	}
}

// Test a continuous flow of batches spanning multiple append entries can still move applied up.
// Also, test a server can become leader if the previous leader left it with a partial batch.
func TestJetStreamAtomicBatchPublishContinuousBatchesStillMoveAppliedUp(t *testing.T) {
	test := func(t *testing.T, largePayload bool) {
		var payload []byte
		if largePayload {
			// Create a large payload that can be easily compressed.
			payload = make([]byte, 2*compressThreshold)
		}

		c := createJetStreamClusterExplicit(t, "R3S", 3)
		defer c.shutdown()

		nc, js := jsClientConnect(t, c.randomServer())
		defer nc.Close()

		_, err := jsStreamCreate(t, nc, &StreamConfig{
			Name:               "TEST",
			Subjects:           []string{"foo"},
			Storage:            FileStorage,
			Replicas:           3,
			AllowAtomicPublish: true,
		})
		require_NoError(t, err)

		sl := c.streamLeader(globalAccountName, "TEST")
		mset, err := sl.globalAccount().lookupStream("TEST")
		require_NoError(t, err)

		pubAck, err := js.Publish("foo", nil)
		require_NoError(t, err)
		require_Equal(t, pubAck.Sequence, 1)

		n := mset.raftNode().(*raft)
		index, commit, applied := n.Progress()

		// The first batch spans two append entries, but commits.
		mset.clMu.Lock()
		hdr := genHeader(nil, "Nats-Batch-Id", "ID_1")
		hdr = setHeader("Nats-Batch-Sequence", "1", hdr)
		esm := encodeStreamMsgAllowCompressAndBatch("foo", _EMPTY_, hdr, payload, mset.clseq, 0, false, "ID_1", 1, false)
		mset.clseq++
		mset.clMu.Unlock()
		n.sendAppendEntry([]*Entry{newEntry(EntryNormal, esm)})

		var entries []*Entry
		mset.clMu.Lock()
		hdr = genHeader(nil, "Nats-Batch-Id", "ID_1")
		hdr = setHeader("Nats-Batch-Sequence", "2", hdr)
		esm = encodeStreamMsgAllowCompressAndBatch("foo", _EMPTY_, hdr, payload, mset.clseq, 0, false, "ID_1", 2, false)
		entries = append(entries, newEntry(EntryNormal, esm))
		mset.clseq++

		hdr = genHeader(nil, "Nats-Batch-Id", "ID_1")
		hdr = setHeader("Nats-Batch-Sequence", "3", hdr)
		hdr = setHeader("Nats-Batch-Commit", "1", hdr)
		esm = encodeStreamMsgAllowCompressAndBatch("foo", _EMPTY_, hdr, payload, mset.clseq, 0, false, "ID_1", 3, true)
		entries = append(entries, newEntry(EntryNormal, esm))
		mset.clseq++

		// The second batch doesn't commit.
		hdr = genHeader(nil, "Nats-Batch-Id", "ID_2")
		hdr = setHeader("Nats-Batch-Sequence", "1", hdr)
		esm = encodeStreamMsgAllowCompressAndBatch("foo", _EMPTY_, hdr, payload, mset.clseq, 0, false, "ID_2", 1, false)
		mset.clseq++
		entries = append(entries, newEntry(EntryNormal, esm))
		mset.clMu.Unlock()
		n.sendAppendEntry(entries)

		checkFor(t, 2*time.Second, 200*time.Millisecond, func() error {
			n.RLock()
			nindex, ncommit, processed, napplied := n.pindex, n.commit, n.processed, n.applied
			n.RUnlock()
			if nindex == index {
				return errors.New("index not updated")
			} else if ncommit == commit {
				return errors.New("commit not updated")
			} else if napplied == applied {
				return errors.New("applied not updated")
			} else if napplied == ncommit {
				return errors.New("applied should not be able to equal commit yet")
			} else if processed != ncommit {
				return errors.New("must have processed all commits")
			}
			return checkState(t, c, globalAccountName, "TEST")
		})

		// Followers are now stranded with a partial batch, one needs to become leader
		// and have the batch be rejected since it was partially proposed.
		sl.Shutdown()
		c.waitOnStreamLeader(globalAccountName, "TEST")
		checkFor(t, 2*time.Second, 200*time.Millisecond, func() error {
			return checkState(t, c, globalAccountName, "TEST")
		})

		// Confirm the last batch gets rejected, and we are still able to publish with quorum.
		pubAck, err = js.Publish("foo", nil)
		require_NoError(t, err)
		require_Equal(t, pubAck.Sequence, 5)

		c.restartServer(sl)
		checkFor(t, 2*time.Second, 200*time.Millisecond, func() error {
			return checkState(t, c, globalAccountName, "TEST")
		})

		// Publish again, now with all servers online.
		pubAck, err = js.Publish("foo", nil)
		require_NoError(t, err)
		require_Equal(t, pubAck.Sequence, 6)
		checkFor(t, 2*time.Second, 200*time.Millisecond, func() error {
			return checkState(t, c, globalAccountName, "TEST")
		})
	}

	t.Run("Normal", func(t *testing.T) { test(t, false) })
	t.Run("Compressed", func(t *testing.T) { test(t, true) })
}

// Test a batch that's committed but only partially applied, and the server gets hard killed.
// Upon recovery the server should skip the messages that were already applied but store the
// messages that weren't applied yet.
func TestJetStreamAtomicBatchPublishPartiallyAppliedBatchOnRecovery(t *testing.T) {
	test := func(t *testing.T, largePayload bool) {
		var payload []byte
		expectedOp := streamMsgOp
		if largePayload {
			// Create a large payload that can be easily compressed.
			payload = make([]byte, 2*compressThreshold)
			expectedOp = compressedStreamMsgOp
		}

		c := createJetStreamClusterExplicit(t, "R3S", 3)
		defer c.shutdown()

		nc, js := jsClientConnect(t, c.randomServer())
		defer nc.Close()

		_, err := jsStreamCreate(t, nc, &StreamConfig{
			Name:               "TEST",
			Subjects:           []string{"foo"},
			Storage:            FileStorage,
			Replicas:           3,
			AllowAtomicPublish: true,
		})
		require_NoError(t, err)

		sl := c.streamLeader(globalAccountName, "TEST")
		mset, err := sl.globalAccount().lookupStream("TEST")
		require_NoError(t, err)

		pubAck, err := js.Publish("foo", nil)
		require_NoError(t, err)
		require_Equal(t, pubAck.Sequence, 1)

		checkFor(t, 2*time.Second, 200*time.Millisecond, func() error {
			return checkState(t, c, globalAccountName, "TEST")
		})

		// Pause applies on all followers.
		for _, s := range c.servers {
			if s == sl {
				continue
			}
			fmset, err := s.globalAccount().lookupStream("TEST")
			require_NoError(t, err)
			require_NoError(t, fmset.raftNode().PauseApply())
		}

		// Publish a batch.
		var entries []*Entry
		mset.clMu.Lock()
		for seq := uint64(1); seq <= 4; seq++ {
			batchCommit := seq == 4
			hdr := genHeader(nil, "Nats-Batch-Id", "ID")
			hdr = genHeader(hdr, "Nats-Batch-Sequence", strconv.FormatUint(seq, 10))
			esm := encodeStreamMsgAllowCompressAndBatch("foo", _EMPTY_, hdr, payload, mset.clseq, 0, false, "ID", seq, batchCommit)
			if _, _, bop, _, err := decodeBatchMsg(esm[1:]); err != nil || bop != expectedOp {
				mset.clMu.Unlock()
				t.Fatalf("Unexpected batch op %v: %v", bop, err)
			}
			entries = append(entries, newEntry(EntryNormal, esm))
			mset.clseq++
		}
		mset.clMu.Unlock()

		// Wait for the leader to have applied the batch, so the next
		// publish informs the followers that it can be committed.
		n := mset.raftNode().(*raft)
		_, _, applied := n.Progress()
		n.sendAppendEntry(entries)
		checkFor(t, 2*time.Second, 200*time.Millisecond, func() error {
			_, _, napplied := n.Progress()
			if applied == napplied {
				return errors.New("batch hasn't been applied yet")
			}
			return nil
		})

		// Publish so the commit moves up on all servers.
		pubAck, err = js.Publish("foo", nil)
		require_NoError(t, err)
		require_Equal(t, pubAck.Sequence, 6)

		// Simulate this follower only partially applied the batch and then got hard killed.
		rs := c.randomNonStreamLeader(globalAccountName, "TEST")
		mset, err = rs.globalAccount().lookupStream("TEST")
		require_NoError(t, err)

		ts := time.Now().UnixNano()
		hdr := genHeader(nil, "Nats-Batch-Id", "ID")
		hdr = genHeader(hdr, "Nats-Batch-Sequence", "1")
		require_NoError(t, mset.store.StoreRawMsg("foo", hdr, nil, 2, ts, 0, false))

		hdr = genHeader(nil, "Nats-Batch-Id", "ID")
		hdr = genHeader(hdr, "Nats-Batch-Sequence", "2")
		require_NoError(t, mset.store.StoreRawMsg("foo", hdr, nil, 3, ts, 0, false))

		// Unpause applies on the remaining follower.
		for _, s := range c.servers {
			if s == sl || s == rs {
				continue
			}
			fmset, err := s.globalAccount().lookupStream("TEST")
			require_NoError(t, err)
			fmset.raftNode().ResumeApply()
		}

		// Restart the "hard killed" follower, should apply the remainder of the batch upon recovery.
		rs.Shutdown()
		c.restartServer(rs)
		checkFor(t, 2*time.Second, 200*time.Millisecond, func() error {
			return checkState(t, c, globalAccountName, "TEST")
		})
	}

	t.Run("Normal", func(t *testing.T) { test(t, false) })
	t.Run("Compressed", func(t *testing.T) { test(t, true) })
}

func TestJetStreamRollupIsolatedRead(t *testing.T) {
	const (
		DirectGet = iota
		DirectBatchGet
		DirectMultiGet
		Consumer
	)

	test := func(t *testing.T, mode int) {
		c := createJetStreamClusterExplicit(t, "R3S", 3)
		defer c.shutdown()

		nc, js := jsClientConnect(t, c.randomServer())
		defer nc.Close()

		_, err := js.AddStream(&nats.StreamConfig{
			Name:        "TEST",
			Subjects:    []string{"foo.*"},
			AllowRollup: true,
			Replicas:    3,
		})
		require_NoError(t, err)

		rs := c.randomNonStreamLeader(globalAccountName, "TEST")
		mset, err := rs.globalAccount().lookupStream("TEST")
		require_NoError(t, err)

		// Reconnect to the selected server.
		nc.Close()
		nc, js = jsClientConnect(t, rs)
		defer nc.Close()

		m := nats.NewMsg("foo.0")
		pubAck, err := js.PublishMsg(m)
		require_NoError(t, err)
		require_Equal(t, pubAck.Sequence, 1)

		// mset.processJetStreamMsg will first store the new message, then update dedupe state, and then do the rollup.
		// We block the replica such that it can store the new message but can't do the rollup yet.
		// A read should wait for the rollup to complete.
		mset.ddMu.Lock()
		m.Subject = "foo.1"
		m.Header.Set(JSMsgRollup, JSMsgRollupAll)
		m.Header.Set(JSMsgId, "msgId")
		_, _ = js.PublishMsg(m)
		err = checkForErr(2*time.Second, 200*time.Millisecond, func() error {
			var state StreamState
			mset.store.FastState(&state)
			if state.LastSeq != 2 {
				return fmt.Errorf("expected last seq 2, got: %d", state.LastSeq)
			}
			return nil
		})
		if err != nil {
			mset.ddMu.Unlock()
			require_NoError(t, err)
		}

		// We're now subscribing and going to do a request, while the write is still halfway.
		sub, err := nc.SubscribeSync("reply")
		if err != nil {
			mset.ddMu.Unlock()
			require_NoError(t, err)
		}
		defer sub.Drain()
		if err = nc.Flush(); err != nil {
			mset.ddMu.Unlock()
			require_NoError(t, err)
		}

		// Run two goroutines, one will unblock the first write after a short sleep,
		// the second will do the read/consumer request.
		var (
			ready   sync.WaitGroup
			run     sync.WaitGroup
			cleanup sync.WaitGroup
		)
		ready.Add(2)
		run.Add(1)
		cleanup.Add(1)
		go func() {
			ready.Done()
			defer cleanup.Done()
			run.Wait()
			time.Sleep(250 * time.Millisecond)
			mset.ddMu.Unlock()
		}()
		go func() {
			ready.Done()
			run.Wait()
			switch mode {
			case DirectGet:
				mset.getDirectRequest(&JSApiMsgGetRequest{LastFor: "foo.0"}, "reply")
			case DirectBatchGet:
				mset.getDirectRequest(&JSApiMsgGetRequest{NextFor: "foo.*", Batch: 2}, "reply")
			case DirectMultiGet:
				mset.getDirectRequest(&JSApiMsgGetRequest{MultiLastFor: []string{"foo.*"}, Batch: 2}, "reply")
			case Consumer:
				mset.addConsumer(&ConsumerConfig{DeliverSubject: "reply", Direct: true})
			}
		}()

		// Wait for both to be ready, then run both of them.
		ready.Wait()
		run.Done()

		msg, err := sub.NextMsg(time.Second)
		// Make sure cleanup has happened before validating.
		cleanup.Wait()
		require_NoError(t, err)
		if mode == DirectGet {
			require_Equal(t, msg.Header.Get("Status"), "404")
		} else if mode != Consumer {
			require_Equal(t, msg.Header.Get(JSNumPending), "0")
			require_Equal(t, msg.Header.Get(JSSequence), "2")
		} else {
			require_True(t, strings.HasPrefix(msg.Reply, jsAckPre))
			sseq, _, _, _, pending := ackReplyInfo(msg.Reply)
			require_Equal(t, pending, 0)
			require_Equal(t, sseq, 2)
		}
	}

	t.Run("DirectGet", func(t *testing.T) { test(t, DirectGet) })
	t.Run("DirectBatchGet", func(t *testing.T) { test(t, DirectBatchGet) })
	t.Run("DirectMultiGet", func(t *testing.T) { test(t, DirectMultiGet) })
	t.Run("Consumer", func(t *testing.T) { test(t, Consumer) })
}

func TestJetStreamAtomicBatchPublishAdvisories(t *testing.T) {
	templ := `
	listen: 127.0.0.1:-1
	server_name: %s
	jetstream: {
		max_mem_store: 2GB
		max_file_store: 2GB
		store_dir: '%s'
		limits {
			batch {
				timeout: 2s
			}
		}
	}

	leaf {
		listen: 127.0.0.1:-1
	}

	cluster {
		name: %s
		listen: 127.0.0.1:%d
		routes = [%s]
	}

	# For access to system account.
	accounts { $SYS { users = [ { user: "admin", pass: "s3cr3t!" } ] } }
`
	test := func(t *testing.T, replicas int) {
		c := createJetStreamClusterWithTemplate(t, templ, "R3S", 3)
		defer c.shutdown()

		nc := clientConnectToServer(t, c.randomServer())
		defer nc.Close()

		cfg := &StreamConfig{
			Name:               "TEST",
			Subjects:           []string{"foo"},
			Replicas:           replicas,
			Storage:            FileStorage,
			AllowAtomicPublish: true,
		}

		_, err := jsStreamCreate(t, nc, cfg)
		require_NoError(t, err)

		sub, err := nc.SubscribeSync(fmt.Sprintf("%s.*", JSAdvisoryStreamBatchAbandonedPre))
		require_NoError(t, err)
		defer sub.Drain()
		require_NoError(t, nc.Flush())

		// Should receive an advisory if gaps are detected.
		m := nats.NewMsg("foo")
		m.Header.Set("Nats-Batch-Id", "uuid")
		m.Header.Set("Nats-Batch-Sequence", "1")
		require_NoError(t, nc.PublishMsg(m))
		m.Header.Set("Nats-Batch-Sequence", "3")
		require_NoError(t, nc.PublishMsg(m))

		msg, err := sub.NextMsg(time.Second)
		require_NoError(t, err)
		var advisory JSStreamBatchAbandonedAdvisory
		require_NoError(t, json.Unmarshal(msg.Data, &advisory))
		require_Equal(t, advisory.BatchId, "uuid")
		require_Equal(t, advisory.Reason, BatchIncomplete)

		// Should receive an advisory if the batch is too large.
		count := 1002
		for seq := 1; seq <= count; seq++ {
			m = nats.NewMsg("foo")
			m.Header.Set("Nats-Batch-Id", "uuid")
			m.Header.Set("Nats-Batch-Sequence", strconv.Itoa(seq))
			if seq != count {
				require_NoError(t, nc.PublishMsg(m))
				continue
			}
			var pubAck JSPubAckResponse
			m.Header.Set("Nats-Batch-Commit", "1")
			rmsg, err := nc.RequestMsg(m, time.Second)
			require_NoError(t, err)
			require_NoError(t, json.Unmarshal(rmsg.Data, &pubAck))
			require_NotNil(t, pubAck.Error)
			require_Error(t, pubAck.Error, NewJSAtomicPublishTooLargeBatchError(streamDefaultMaxAtomicBatchSize))
		}

		msg, err = sub.NextMsg(time.Second)
		require_NoError(t, err)
		advisory = JSStreamBatchAbandonedAdvisory{}
		require_NoError(t, json.Unmarshal(msg.Data, &advisory))
		require_Equal(t, advisory.BatchId, "uuid")
		require_Equal(t, advisory.Reason, BatchLarge)

		// Should receive an advisory if the batch times out and is abandoned.
		m = nats.NewMsg("foo")
		m.Header.Set("Nats-Batch-Id", "uuid")
		m.Header.Set("Nats-Batch-Sequence", "1")
		require_NoError(t, nc.PublishMsg(m))

		msg, err = sub.NextMsg(3 * time.Second)
		require_NoError(t, err)
		advisory = JSStreamBatchAbandonedAdvisory{}
		require_NoError(t, json.Unmarshal(msg.Data, &advisory))
		require_Equal(t, advisory.BatchId, "uuid")
		require_Equal(t, advisory.Reason, BatchTimeout)

		// Should receive an advisory if the batch has an unsupported required API level.
		m = nats.NewMsg("foo")
		m.Header.Set("Nats-Batch-Id", "uuid")
		m.Header.Set("Nats-Batch-Sequence", "1")
		m.Header.Set("Nats-Required-Api-Level", strconv.Itoa(math.MaxInt))
		require_NoError(t, nc.PublishMsg(m))

		msg, err = sub.NextMsg(3 * time.Second)
		require_NoError(t, err)
		advisory = JSStreamBatchAbandonedAdvisory{}
		require_NoError(t, json.Unmarshal(msg.Data, &advisory))
		require_Equal(t, advisory.BatchId, "uuid")
		require_Equal(t, advisory.Reason, BatchRequirementsNotMet)
	}

	for _, replicas := range []int{1, 3} {
		t.Run(fmt.Sprintf("R%d", replicas), func(t *testing.T) {
			test(t, replicas)
		})
	}
}

func TestJetStreamAtomicBatchPublishExpectedSeq(t *testing.T) {
	s := RunBasicJetStreamServer(t)
	defer s.Shutdown()

	nc, js := jsClientConnect(t, s)
	defer nc.Close()

	_, err := jsStreamCreate(t, nc, &StreamConfig{
		Name:               "TEST",
		Subjects:           []string{"foo"},
		Storage:            FileStorage,
		AllowAtomicPublish: true,
	})
	require_NoError(t, err)

	mset, err := s.globalAccount().lookupStream("TEST")
	require_NoError(t, err)

	clseqAndClfs := func() (uint64, uint64) {
		mset.clMu.Lock()
		defer mset.clMu.Unlock()
		return mset.clseq, mset.clfs
	}

	clseq, clfs := clseqAndClfs()
	require_Equal(t, clseq, 0)
	require_Equal(t, clfs, 0)

	_, err = js.Publish("foo", []byte("hello"))
	require_NoError(t, err)

	clseq, clfs = clseqAndClfs()
	require_Equal(t, clseq, 0)
	require_Equal(t, clfs, 0)

	// test expect last seq for standalone publish
	_, err = js.Publish("foo", []byte("hello"), nats.ExpectLastSequence(1))
	require_NoError(t, err)

	clseq, clfs = clseqAndClfs()
	require_Equal(t, clseq, 0)
	require_Equal(t, clfs, 0)

	// now do a single msg batch with expect last seq
	m := nats.NewMsg("foo")
	m.Header.Set("Nats-Expected-Last-Sequence", "2")
	m.Header.Set("Nats-Batch-Id", "uuid")
	m.Header.Set("Nats-Batch-Sequence", "1")
	m.Header.Set("Nats-Batch-Commit", "1")
	resp, err := nc.RequestMsg(m, time.Second)
	require_NoError(t, err)

	var pubAck JSPubAckResponse
	require_NoError(t, json.Unmarshal(resp.Data, &pubAck))
	if pubAck.Error != nil {
		t.Fatalf("Commit error: %v", pubAck.Error)
	}
	require_Equal(t, pubAck.Sequence, 3)
	require_Equal(t, pubAck.BatchSize, 1)

	clseq, clfs = clseqAndClfs()
	require_Equal(t, clseq, 0)
	require_Equal(t, clfs, 0)
}

// Test two different batches within the same append entry.
func TestJetStreamAtomicBatchPublishPartialBatchInSharedAppendEntry(t *testing.T) {
	test := func(t *testing.T, commit bool) {
		c := createJetStreamClusterExplicit(t, "R3S", 3)
		defer c.shutdown()

		nc := clientConnectToServer(t, c.randomServer())
		defer nc.Close()

		_, err := jsStreamCreate(t, nc, &StreamConfig{
			Name:               "TEST",
			Subjects:           []string{"foo"},
			Storage:            FileStorage,
			Replicas:           3,
			AllowAtomicPublish: true,
		})
		require_NoError(t, err)

		sl := c.streamLeader(globalAccountName, "TEST")
		mset, err := sl.globalAccount().lookupStream("TEST")
		require_NoError(t, err)

		js := sl.getJetStream()
		esm1 := encodeStreamMsgAllowCompressAndBatch("foo", _EMPTY_, nil, []byte("b1"), 0, 0, false, "b1", 1, false)
		esm2 := encodeStreamMsgAllowCompressAndBatch("foo", _EMPTY_, nil, []byte("b2"), 1, 0, false, "b2", 1, commit)
		ce := newCommittedEntry(2, []*Entry{
			newEntry(EntryNormal, esm1),
			newEntry(EntryNormal, esm2),
		})
		batch := &batchApply{}
		_, err = js.applyStreamEntries(mset, mset.raftNode(), ce, false, batch)
		require_NoError(t, err)
		entryStart, maxApplied := batch.entryStart, batch.maxApplied

		if !commit {
			require_Equal(t, entryStart, 1)
			require_Equal(t, maxApplied, 1)
			return
		}
		require_Equal(t, entryStart, 0)
		require_Equal(t, maxApplied, 0)
		require_Equal(t, mset.lastSeq(), 1)

		var smv StoreMsg
		sm, err := mset.store.LoadMsg(1, &smv)
		require_NoError(t, err)
		require_Equal(t, string(sm.buf), "b2")
	}

	t.Run("NoCommit", func(t *testing.T) { test(t, false) })
	t.Run("Commit", func(t *testing.T) { test(t, true) })
}

func TestJetStreamAtomicBatchPublishCatchupMarkerMidBatch(t *testing.T) {
	s := RunBasicJetStreamServer(t)
	defer s.Shutdown()

	nc := clientConnectToServer(t, s)
	defer nc.Close()

	_, err := jsStreamCreate(t, nc, &StreamConfig{
		Name:               "TEST",
		Subjects:           []string{"foo"},
		Storage:            MemoryStorage,
		Replicas:           1,
		AllowAtomicPublish: true,
	})
	require_NoError(t, err)

	mset, err := s.globalAccount().lookupStream("TEST")
	require_NoError(t, err)
	js := mset.js
	require_True(t, js != nil)

	// Non-zero timestamp so apply does not treat these as skip/no-op messages.
	ts := time.Now().UnixNano()

	// 1) Apply the start of an atomic batch (seq 1, no commit). The batch
	//    becomes "in progress" and this CommittedEntry is buffered.
	hdr1 := genHeader(nil, "Nats-Batch-Id", "uuid")
	hdr1 = genHeader(hdr1, "Nats-Batch-Sequence", "1")
	esm1 := encodeStreamMsgAllowCompressAndBatch("foo", _EMPTY_, hdr1, []byte("hello"), 0, ts, false, "uuid", 1, false)
	ce1 := newCommittedEntry(1, []*Entry{newEntry(EntryNormal, esm1)})
	batch := &batchApply{}
	_, err = js.applyStreamEntries(mset, mset.raftNode(), ce1, false, batch)
	require_NoError(t, err)

	// Confirm the batch is in progress.
	require_True(t, batch.id != _EMPTY_)

	// 2) A Raft-level catchup starts mid-batch. This pushes a marker entry with
	//    Type==EntryCatchup and Data==nil (see raft.sendCatchupSignal). The
	//    in-progress batch must remain intact and buffer this marker.
	catchupCE := newCommittedEntry(0, []*Entry{{EntryCatchup, nil}})
	_, err = js.applyStreamEntries(mset, mset.raftNode(), catchupCE, false, batch)
	require_NoError(t, err)

	// 3) Apply the batch commit (seq 2). The replay loop must skip the buffered
	//    EntryCatchup marker instead of decoding it and commit the full batch.
	hdr2 := genHeader(nil, "Nats-Batch-Id", "uuid")
	hdr2 = genHeader(hdr2, "Nats-Batch-Sequence", "2")
	hdr2 = genHeader(hdr2, "Nats-Batch-Commit", "1")
	esm2 := encodeStreamMsgAllowCompressAndBatch("foo", _EMPTY_, hdr2, []byte("world"), 1, ts, false, "uuid", 2, true)
	ce2 := newCommittedEntry(2, []*Entry{newEntry(EntryNormal, esm2)})

	// A regression would panic on entry.Data[1:] while holding mset.mu and batch.mu.
	// Recover and release the locks so the deferred shutdown does not deadlock.
	var panicked any
	func() {
		defer func() { panicked = recover() }()
		_, err = js.applyStreamEntries(mset, mset.raftNode(), ce2, false, batch)
	}()
	if panicked != nil {
		mset.mu.Unlock()
		t.Fatalf("applyStreamEntries panicked committing a batch with a buffered EntryCatchup entry: %v", panicked)
	}
	require_NoError(t, err)

	// The full batch committed: both messages must be present.
	state := mset.state()
	require_Equal(t, state.Msgs, 2)
	require_Equal(t, state.FirstSeq, 1)
	require_Equal(t, state.LastSeq, 2)
}

func TestJetStreamAtomicBatchPublishRejectPartialBatchOnLeaderChange(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "R3S", 3)
	defer c.shutdown()

	nc := clientConnectToServer(t, c.randomServer())
	defer nc.Close()

	_, err := jsStreamCreate(t, nc, &StreamConfig{
		Name:               "TEST",
		Subjects:           []string{"foo"},
		Storage:            FileStorage,
		Replicas:           3,
		AllowAtomicPublish: true,
	})
	require_NoError(t, err)

	sl := c.streamLeader(globalAccountName, "TEST")
	mset, err := sl.globalAccount().lookupStream("TEST")
	require_NoError(t, err)

	// Propose a partial batch that doesn't commit.
	var entries []*Entry
	for i := range uint64(10) {
		esm := encodeStreamMsgAllowCompressAndBatch("foo", _EMPTY_, nil, nil, i, 0, false, "b", i+1, false)
		entries = append(entries, newEntry(EntryNormal, esm))
	}
	n := mset.raftNode()
	require_NoError(t, n.ProposeMulti(n.Term(), entries))

	// Wait for all servers to have received and populated the partial batch.
	checkFor(t, 2*time.Second, 200*time.Millisecond, func() error {
		var pindexMax uint64
		for _, s := range c.servers {
			mset2, err := s.globalAccount().lookupStream("TEST")
			if err != nil {
				return err
			}
			rn := mset2.raftNode().(*raft)
			rn.RLock()
			prop, pindex, processed := rn.prop.len()+int(rn.prop.inProgress()), rn.pindex, rn.processed
			rn.RUnlock()
			if prop > 0 {
				return fmt.Errorf("still has inflight proposals")
			} else if pindex != processed {
				return fmt.Errorf("pindex %d != processed %d", pindex, processed)
			} else if pindexMax == 0 {
				pindexMax = pindex
			} else if pindex != pindexMax {
				return fmt.Errorf("pindex %d != max %d", pindex, pindexMax)
			}

		}
		return nil
	})

	// Trigger a leader change and ensure the CLFS gets accounted for.
	require_NoError(t, n.StepDown())
	checkFor(t, 2*time.Second, 200*time.Millisecond, func() error {
		for _, s := range c.servers {
			mset, err = s.globalAccount().lookupStream("TEST")
			if err != nil {
				return err
			}
			if clfs := mset.getCLFS(); clfs != 10 {
				return fmt.Errorf("expected clfs to be 10, got: %d", clfs)
			}
		}
		return nil
	})
}

func TestJetStreamAtomicBatchPublishPersistModeAsync(t *testing.T) {
	s := RunBasicJetStreamServer(t)
	defer s.Shutdown()

	nc := clientConnectToServer(t, s)
	defer nc.Close()

	cfg := &StreamConfig{
		Name:               "TEST",
		Subjects:           []string{"foo"},
		Storage:            FileStorage,
		Replicas:           1,
		AllowAtomicPublish: true,
		PersistMode:        AsyncPersistMode,
	}
	_, err := jsStreamCreate(t, nc, cfg)
	require_Error(t, err, NewJSStreamInvalidConfigError(fmt.Errorf("async persist mode is not supported with atomic batch publish")))
}

func TestJetStreamAtomicBatchPublishExpectedLastSubjectSequence(t *testing.T) {
	s := RunBasicJetStreamServer(t)
	defer s.Shutdown()

	nc, js := jsClientConnect(t, s)
	defer nc.Close()

	cfg := &StreamConfig{
		Name:               "TEST",
		Subjects:           []string{"foo.*"},
		Storage:            FileStorage,
		Replicas:           1,
		AllowAtomicPublish: true,
	}
	_, err := jsStreamCreate(t, nc, cfg)
	require_NoError(t, err)

	_, err = js.Publish("foo.A", nil)
	require_NoError(t, err)
	_, err = js.Publish("foo.B", nil)
	require_NoError(t, err)

	m := nats.NewMsg("foo.A")
	m.Header.Set("Nats-Batch-Id", "uuid")
	m.Header.Set("Nats-Batch-Sequence", "1")
	m.Header.Set("Nats-Expected-Last-Sequence", "2")
	m.Header.Set("Nats-Expected-Last-Subject-Sequence", "1")
	m.Header.Set("Nats-Expected-Last-Subject-Sequence-Subject", "foo.A")
	msg, err := nc.RequestMsg(m, time.Second)
	require_NoError(t, err)
	require_Len(t, len(msg.Data), 0) // Empty ack.

	m = nats.NewMsg("foo.B")
	m.Header.Set("Nats-Batch-Id", "uuid")
	m.Header.Set("Nats-Batch-Sequence", "2")
	m.Header.Set("Nats-Batch-Commit", "1")
	m.Header.Set("Nats-Expected-Last-Subject-Sequence", "2")
	m.Header.Set("Nats-Expected-Last-Subject-Sequence-Subject", "foo.B")
	msg, err = nc.RequestMsg(m, time.Second)
	require_NoError(t, err)
	var resp JSPubAckResponse
	require_NoError(t, json.Unmarshal(msg.Data, &resp))
	require_True(t, resp.Error == nil)
	require_Equal(t, resp.PubAck.Sequence, 4)
	require_Equal(t, resp.PubAck.BatchId, "uuid")
	require_Equal(t, resp.PubAck.BatchSize, 2)
}

func TestJetStreamAtomicBatchPublishExpectedLastSubjectSequenceOwnership(t *testing.T) {
	s := RunBasicJetStreamServer(t)
	defer s.Shutdown()

	nc, js := jsClientConnect(t, s)
	defer nc.Close()

	_, err := jsStreamCreate(t, nc, &StreamConfig{
		Name:               "EVENTS",
		Subjects:           []string{"events.>"},
		Storage:            MemoryStorage,
		Replicas:           1,
		AllowAtomicPublish: true,
	})
	require_NoError(t, err)

	entry := "diary.entry_BBBBBBBBBBBBBBBBBBBBBBBB"
	oldDay := "diary-day.day_CCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCC"
	targetDay := "diary-day.day_AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"
	mutation := "mutation.mut_DDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDD"
	entities := make([]string, 19)
	for i := range entities {
		entities[i] = fmt.Sprintf("seed.entity_%02d", i+1)
	}
	entities[10], entities[17], entities[18] = targetDay, entry, oldDay
	for i, entity := range entities {
		ack, err := js.Publish("events."+entity+".balance", nil)
		require_NoError(t, err)
		require_Equal(t, ack.Sequence, uint64(i+1))
	}

	batchEntities := []string{entry, oldDay, targetDay, mutation}
	expected := []uint64{18, 19, 11, 0}
	for i, entity := range batchEntities {
		m := nats.NewMsg("events." + entity + ".balance")
		m.Header.Set(JSExpectedLastSubjSeqSubj, "events."+entity+".*")
		m.Header.Set(JSExpectedLastSubjSeq, strconv.FormatUint(expected[i], 10))
		m.Header.Set(JSBatchId, "uuid")
		m.Header.Set(JSBatchSeq, strconv.Itoa(i+1))
		if i == len(batchEntities)-1 {
			m.Header.Set(JSBatchCommit, "1")
			msg, err := nc.RequestMsg(m, time.Second)
			require_NoError(t, err)
			var resp JSPubAckResponse
			require_NoError(t, json.Unmarshal(msg.Data, &resp))
			if resp.Error != nil {
				t.Fatalf("commit error: %v", resp.Error)
			}
			require_Equal(t, resp.PubAck.Sequence, uint64(23))
			require_Equal(t, resp.PubAck.BatchId, "uuid")
			require_Equal(t, resp.PubAck.BatchSize, 4)
		} else if i == 0 {
			msg, err := nc.RequestMsg(m, time.Second)
			require_NoError(t, err)
			require_Len(t, len(msg.Data), 0)
		} else {
			require_NoError(t, nc.PublishMsg(m))
		}
	}
}

func TestJetStreamAtomicBatchPublishCommitUnsupported(t *testing.T) {
	s := RunBasicJetStreamServer(t)
	defer s.Shutdown()

	nc := clientConnectToServer(t, s)
	defer nc.Close()

	cfg := &StreamConfig{
		Name:               "TEST",
		Subjects:           []string{"foo"},
		Storage:            MemoryStorage,
		Replicas:           1,
		AllowAtomicPublish: true,
	}
	_, err := jsStreamCreate(t, nc, cfg)
	require_NoError(t, err)

	mset, err := s.globalAccount().lookupStream("TEST")
	require_NoError(t, err)

	var resp JSPubAckResponse
	for _, unsupportedCommit := range []string{"", "unsupported", "0"} {
		m := nats.NewMsg("foo")
		m.Header.Set("Nats-Batch-Id", "uuid")
		m.Header.Set("Nats-Batch-Sequence", "1")
		_, err = nc.RequestMsg(m, time.Second)
		require_NoError(t, err)

		m.Header.Set("Nats-Batch-Sequence", "2")
		m.Header.Set("Nats-Batch-Commit", unsupportedCommit)
		msg, err := nc.RequestMsg(m, time.Second)
		require_NoError(t, err)
		resp = JSPubAckResponse{}
		require_NoError(t, json.Unmarshal(msg.Data, &resp))
		require_True(t, resp.Error != nil)
		require_Error(t, resp.Error, NewJSAtomicPublishInvalidBatchCommitError())

		// Confirm no batches are left.
		mset.mu.RLock()
		batches := mset.batches
		mset.mu.RUnlock()
		batches.mu.Lock()
		groups := len(batches.atomic)
		batches.mu.Unlock()
		require_Len(t, groups, 0)
	}

	// The required API level should allow the batch to be rejected.
	m := nats.NewMsg("foo")
	m.Header.Set("Nats-Batch-Id", "uuid")
	m.Header.Set("Nats-Batch-Sequence", "1")
	m.Header.Set("Nats-Batch-Commit", "1")
	m.Header.Set("Nats-Required-Api-Level", strconv.Itoa(math.MaxInt))
	msg, err := nc.RequestMsg(m, time.Second)
	require_NoError(t, err)
	resp = JSPubAckResponse{}
	require_NoError(t, json.Unmarshal(msg.Data, &resp))
	require_True(t, resp.Error != nil)
	require_Error(t, resp.Error, NewJSRequiredApiLevelError())

	// If required API level check passes, the header should be stripped.
	m.Header.Set("Nats-Required-Api-Level", "0")
	msg, err = nc.RequestMsg(m, time.Second)
	require_NoError(t, err)
	resp = JSPubAckResponse{}
	require_NoError(t, json.Unmarshal(msg.Data, &resp))
	require_True(t, resp.Error == nil)
	require_Equal(t, resp.PubAck.Sequence, 1)

	sm, err := mset.getMsg(1)
	require_NoError(t, err)
	require_Len(t, len(sliceHeader(JSRequiredApiLevel, sm.Header)), 0)
}

func TestJetStreamAtomicPublishGetBatchSequence(t *testing.T) {
	for _, tc := range []struct {
		name   string
		hdr    []byte
		seq    uint64
		exists bool
	}{
		{"missing", nil, 0, false},
		{"empty-value", genHeader(nil, JSBatchSeq, ""), 0, false},
		{"valid", genHeader(nil, JSBatchSeq, "42"), 42, true},
		{"non-numeric", genHeader(nil, JSBatchSeq, "abc"), 0, false},
		{"negative", genHeader(nil, JSBatchSeq, "-1"), 0, false},
		{"overflow", genHeader(nil, JSBatchSeq, "18446744073709551616"), 0, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			seq, exists := getBatchSequence(tc.hdr)
			require_Equal(t, seq, tc.seq)
			require_Equal(t, exists, tc.exists)
		})
	}
}

func generateFastBatchReply(inbox string, batchId string, batchSeq uint64, flow uint16, gap string, op int) string {
	return fmt.Sprintf("%s.%s.%d.%s.%d.%d.$FI", inbox, batchId, flow, gap, batchSeq, op)
}

func TestJetStreamFastBatchPublish(t *testing.T) {
	test := func(
		t *testing.T,
		storage StorageType,
		retention RetentionPolicy,
		replicas int,
	) {
		c := createJetStreamClusterExplicit(t, "R3S", 3)
		defer c.shutdown()

		nc, js := jsClientConnect(t, c.randomServer())
		defer nc.Close()

		cfg := &StreamConfig{
			Name:      "TEST",
			Subjects:  []string{"foo.*"},
			Storage:   storage,
			Retention: retention,
			Replicas:  replicas,
		}

		_, err := jsStreamCreate(t, nc, cfg)
		require_NoError(t, err)

		inbox := nats.NewInbox()
		sub, err := nc.SubscribeSync(fmt.Sprintf("%s.>", inbox))
		require_NoError(t, err)
		defer sub.Drain()

		m := nats.NewMsg("foo.0")
		m.Reply = generateFastBatchReply(inbox, "uuid", 0, 0, FastBatchGapFail, FastBatchOpStart)
		m.Data = []byte("foo.0")

		// Publish with batch publish disabled.
		require_NoError(t, nc.PublishMsg(m))
		rmsg, err := sub.NextMsg(time.Second)
		require_NoError(t, err)
		var pubAck JSPubAckResponse
		require_NoError(t, json.Unmarshal(rmsg.Data, &pubAck))
		require_NotNil(t, pubAck.Error)
		require_Error(t, pubAck.Error, NewJSBatchPublishDisabledError())

		// Enable batch publish.
		cfg.AllowBatchPublish = true
		_, err = jsStreamUpdate(t, nc, cfg)
		require_NoError(t, err)

		// Publish with incorrect batch sequence errors.
		m.Reply = generateFastBatchReply(inbox, "uuid", 0, 0, FastBatchGapFail, FastBatchOpStart)
		require_NoError(t, nc.PublishMsg(m))
		rmsg, err = sub.NextMsg(time.Second)
		require_NoError(t, err)
		pubAck = JSPubAckResponse{}
		require_NoError(t, json.Unmarshal(rmsg.Data, &pubAck))
		require_NotNil(t, pubAck.Error)
		require_Error(t, pubAck.Error, NewJSBatchPublishInvalidPatternError())

		// A batch that immediately commits through EOB is an error.
		m.Reply = generateFastBatchReply(inbox, "uuid", 1, 0, FastBatchGapFail, FastBatchOpCommitEob)
		require_NoError(t, nc.PublishMsg(m))
		rmsg, err = sub.NextMsg(time.Second)
		require_NoError(t, err)
		pubAck = JSPubAckResponse{}
		require_NoError(t, json.Unmarshal(rmsg.Data, &pubAck))
		require_NotNil(t, pubAck.Error)
		require_Error(t, pubAck.Error, NewJSBatchPublishInvalidPatternError())

		// A batch ID must not exceed the maximum length.
		longBatchId := strings.Repeat("A", 65)
		m.Reply = generateFastBatchReply(inbox, longBatchId, 1, 0, FastBatchGapFail, FastBatchOpStart)
		require_NoError(t, nc.PublishMsg(m))
		rmsg, err = sub.NextMsg(time.Second)
		require_NoError(t, err)
		pubAck = JSPubAckResponse{}
		require_NoError(t, json.Unmarshal(rmsg.Data, &pubAck))
		require_NotNil(t, pubAck.Error)
		require_Error(t, pubAck.Error, NewJSBatchPublishInvalidBatchIDError())

		// Publish a batch, misses start.
		m.Reply = generateFastBatchReply(inbox, "uuid", 2, 0, FastBatchGapFail, FastBatchOpAppend)
		require_NoError(t, nc.PublishMsg(m))
		rmsg, err = sub.NextMsg(time.Second)
		require_NoError(t, err)
		pubAck = JSPubAckResponse{}
		require_NoError(t, json.Unmarshal(rmsg.Data, &pubAck))
		require_Error(t, pubAck.Error, NewJSBatchPublishUnknownBatchIDError())

		// Publish a "batch" which immediately commits.
		m.Reply = generateFastBatchReply(inbox, "uuid", 1, 0, FastBatchGapFail, FastBatchOpCommit)
		require_NoError(t, nc.PublishMsg(m))
		// The PubAck comes in immediately without an intermediate BatchFlowAck.
		rmsg, err = sub.NextMsg(time.Second)
		require_NoError(t, err)
		pubAck = JSPubAckResponse{}
		require_NoError(t, json.Unmarshal(rmsg.Data, &pubAck))
		require_Equal(t, pubAck.Sequence, 1)
		require_Equal(t, pubAck.BatchId, "uuid")
		require_Equal(t, pubAck.BatchSize, 1)

		// Publish a batch of N messages.
		for seq, batch := uint64(1), uint64(5); seq <= batch; seq++ {
			m.Subject = fmt.Sprintf("foo.%d", seq)
			m.Data = []byte(m.Subject)
			if seq == batch {
				m.Reply = generateFastBatchReply(inbox, "uuid", seq, 0, FastBatchGapFail, FastBatchOpCommit)
			} else if seq == 1 {
				m.Reply = generateFastBatchReply(inbox, "uuid", seq, 0, FastBatchGapFail, FastBatchOpStart)
			} else {
				m.Reply = generateFastBatchReply(inbox, "uuid", seq, 0, FastBatchGapFail, FastBatchOpAppend)
			}
			require_NoError(t, nc.PublishMsg(m))

			// Can already pre-check receiving the first flow control message.
			if seq == 1 {
				rmsg, err = sub.NextMsg(time.Second)
				require_NoError(t, err)
				require_True(t, strings.HasPrefix(string(rmsg.Data), "{\"type\":\"ack\","))
				var batchFlowAck BatchFlowAck
				require_NoError(t, json.Unmarshal(rmsg.Data, &batchFlowAck))
				require_Equal(t, batchFlowAck.Messages, 10)
				require_Equal(t, batchFlowAck.Sequence, 0)
			}
		}
		// Should receive the PubAck upon commit.
		rmsg, err = sub.NextMsg(time.Second)
		require_NoError(t, err)
		pubAck = JSPubAckResponse{}
		require_NoError(t, json.Unmarshal(rmsg.Data, &pubAck))
		require_Equal(t, pubAck.Sequence, 6)
		require_Equal(t, pubAck.BatchId, "uuid")
		require_Equal(t, pubAck.BatchSize, 5)

		// Validate stream contents.
		if retention != InterestPolicy {
			for i := 0; i < 6; i++ {
				rsm, err := js.GetMsg("TEST", uint64(i+1))
				require_NoError(t, err)
				subj := fmt.Sprintf("foo.%d", i)
				require_Equal(t, rsm.Subject, subj)
				require_Equal(t, string(rsm.Data), subj)
			}
		}
	}

	for _, storage := range []StorageType{FileStorage, MemoryStorage} {
		for _, retention := range []RetentionPolicy{LimitsPolicy, InterestPolicy, WorkQueuePolicy} {
			for _, replicas := range []int{1, 3} {
				t.Run(fmt.Sprintf("%s/%s/R%d", storage, retention, replicas), func(t *testing.T) {
					test(t, storage, retention, replicas)
				})
			}
		}
	}
}

func TestJetStreamFastBatchPublishGapDetection(t *testing.T) {
	test := func(
		t *testing.T,
		replicas int,
		gapMode string,
	) {
		c := createJetStreamClusterExplicit(t, "R3S", 3)
		defer c.shutdown()

		nc := clientConnectToServer(t, c.randomServer())
		defer nc.Close()

		var batchFlowAck BatchFlowAck
		var pubAck JSPubAckResponse

		_, err := jsStreamCreate(t, nc, &StreamConfig{
			Name:              "TEST",
			Subjects:          []string{"foo"},
			Storage:           FileStorage,
			Replicas:          replicas,
			AllowBatchPublish: true,
		})
		require_NoError(t, err)

		inbox := nats.NewInbox()
		sub, err := nc.SubscribeSync(fmt.Sprintf("%s.>", inbox))
		require_NoError(t, err)
		defer sub.Drain()

		m := nats.NewMsg("foo")
		m.Reply = generateFastBatchReply(inbox, "uuid", 1, 0, gapMode, FastBatchOpStart)
		require_NoError(t, nc.PublishMsg(m))
		rmsg, err := sub.NextMsg(time.Second)
		require_NoError(t, err)

		if gapMode == "unknown" {
			pubAck = JSPubAckResponse{}
			require_NoError(t, json.Unmarshal(rmsg.Data, &pubAck))
			require_NotNil(t, pubAck.Error)
			require_Error(t, pubAck.Error, NewJSBatchPublishInvalidPatternError())
			return
		}

		batchFlowAck = BatchFlowAck{}
		require_NoError(t, json.Unmarshal(rmsg.Data, &batchFlowAck))
		require_Equal(t, batchFlowAck.Messages, 10)
		require_Equal(t, batchFlowAck.Sequence, 0)

		// Now a message is missed and a gap should be detected.
		m.Reply = generateFastBatchReply(inbox, "uuid", 3, 0, gapMode, FastBatchOpAppend)
		require_NoError(t, nc.PublishMsg(m))
		rmsg, err = sub.NextMsg(time.Second)
		require_NoError(t, err)

		// There will always be a flow control message with the missed sequences.
		require_True(t, strings.HasPrefix(string(rmsg.Data), "{\"type\":\"gap\","))
		var batchFlowGap BatchFlowGap
		require_NoError(t, json.Unmarshal(rmsg.Data, &batchFlowGap))
		require_Equal(t, batchFlowGap.ExpectedLastSequence, 2)
		require_Equal(t, batchFlowGap.CurrentSequence, 3)

		switch gapMode {
		case FastBatchGapFail:
			// By default, if a gap is detected, the batch is rejected.
			// A PubAck is returned with the data that has been persisted up to that point.
			rmsg, err = sub.NextMsg(time.Second)
			require_NoError(t, err)
			pubAck = JSPubAckResponse{}
			require_NoError(t, json.Unmarshal(rmsg.Data, &pubAck))
			require_Equal(t, pubAck.Sequence, 1)
			require_Equal(t, pubAck.BatchId, "uuid")
			require_Equal(t, pubAck.BatchSize, 1)
		case FastBatchGapOk:
			// If a gap is ok, the batch will continue to function.
			// An EOB commit should get us the PubAck for the third message.
			m.Reply = generateFastBatchReply(inbox, "uuid", 4, 0, gapMode, FastBatchOpCommitEob)
			require_NoError(t, nc.PublishMsg(m))
			rmsg, err = sub.NextMsg(time.Second)
			require_NoError(t, err)
			pubAck = JSPubAckResponse{}
			require_NoError(t, json.Unmarshal(rmsg.Data, &pubAck))
			require_Equal(t, pubAck.Sequence, 2)
			require_Equal(t, pubAck.BatchId, "uuid")
			require_Equal(t, pubAck.BatchSize, 3)
		default:
			t.Fatalf("unexpected gap mode: %q", gapMode)
		}
	}

	for _, replicas := range []int{1, 3} {
		for _, gapMode := range []string{"fail", "ok", "unknown"} {
			t.Run(fmt.Sprintf("R%d/%s", replicas, gapMode), func(t *testing.T) {
				test(t, replicas, gapMode)
			})
		}
	}
}

func TestJetStreamFastBatchPublishMaxUint64SequenceRejected(t *testing.T) {
	test := func(t *testing.T, commitEob bool) {
		c := createJetStreamClusterExplicit(t, "R3S", 3)
		defer c.shutdown()

		nc := clientConnectToServer(t, c.randomServer())
		defer nc.Close()

		_, err := jsStreamCreate(t, nc, &StreamConfig{
			Name:              "TEST",
			Subjects:          []string{"foo"},
			Storage:           FileStorage,
			Replicas:          1,
			AllowBatchPublish: true,
		})
		require_NoError(t, err)

		inbox := nats.NewInbox()
		sub, err := nc.SubscribeSync(fmt.Sprintf("%s.>", inbox))
		require_NoError(t, err)
		defer sub.Drain()

		m := nats.NewMsg("foo")
		m.Reply = generateFastBatchReply(inbox, "uuid", 1, 1, FastBatchGapOk, FastBatchOpStart)
		require_NoError(t, nc.PublishMsg(m))
		rmsg, err := sub.NextMsg(time.Second)
		require_NoError(t, err)

		var batchFlowAck BatchFlowAck
		require_NoError(t, json.Unmarshal(rmsg.Data, &batchFlowAck))
		require_Equal(t, batchFlowAck.Messages, 1)
		require_Equal(t, batchFlowAck.Sequence, 0)

		rmsg, err = sub.NextMsg(time.Second)
		require_NoError(t, err)
		require_True(t, strings.HasPrefix(string(rmsg.Data), "{\"type\":\"ack\","))
		batchFlowAck = BatchFlowAck{}
		require_NoError(t, json.Unmarshal(rmsg.Data, &batchFlowAck))
		require_Equal(t, batchFlowAck.Messages, 1)
		require_Equal(t, batchFlowAck.Sequence, 1)

		m.Reply = generateFastBatchReply(inbox, "uuid", math.MaxUint64-1, 1, FastBatchGapOk, FastBatchOpAppend)
		require_NoError(t, nc.PublishMsg(m))
		rmsg, err = sub.NextMsg(time.Second)
		require_NoError(t, err)
		require_True(t, strings.HasPrefix(string(rmsg.Data), "{\"type\":\"gap\","))
		var batchFlowGap BatchFlowGap
		require_NoError(t, json.Unmarshal(rmsg.Data, &batchFlowGap))
		require_Equal(t, batchFlowGap.ExpectedLastSequence, 2)
		require_Equal(t, batchFlowGap.CurrentSequence, uint64(math.MaxUint64-1))

		rmsg, err = sub.NextMsg(time.Second)
		require_NoError(t, err)
		require_True(t, strings.HasPrefix(string(rmsg.Data), "{\"type\":\"ack\","))
		batchFlowAck = BatchFlowAck{}
		require_NoError(t, json.Unmarshal(rmsg.Data, &batchFlowAck))
		require_Equal(t, batchFlowAck.Messages, 1)
		require_Equal(t, batchFlowAck.Sequence, uint64(math.MaxUint64-1))

		// math.MaxUint64 must be rejected so b.lseq cannot be jumped to a value
		// where the next b.lseq++ would wrap to zero.
		opCommit := FastBatchOpCommit
		if commitEob {
			opCommit = FastBatchOpCommitEob
		}
		m.Reply = generateFastBatchReply(inbox, "uuid", math.MaxUint64, 1, FastBatchGapOk, opCommit)
		require_NoError(t, nc.PublishMsg(m))

		rmsg, err = sub.NextMsg(time.Second)
		require_NoError(t, err)
		var pubAck JSPubAckResponse
		require_NoError(t, json.Unmarshal(rmsg.Data, &pubAck))
		require_NotNil(t, pubAck.Error)
		require_Error(t, pubAck.Error, NewJSBatchPublishInvalidPatternError())
	}

	t.Run("Commit", func(t *testing.T) { test(t, false) })
	t.Run("CommitEob", func(t *testing.T) { test(t, true) })
}

func TestJetStreamFastBatchPublishFlowControl(t *testing.T) {
	templ := `
	listen: 127.0.0.1:-1
	server_name: %s
	jetstream: {
		max_mem_store: 2GB
		max_file_store: 2GB
		store_dir: '%s'
		limits {
			batch {
				timeout: 750ms
			}
		}
	}

	leaf {
		listen: 127.0.0.1:-1
	}

	cluster {
		name: %s
		listen: 127.0.0.1:%d
		routes = [%s]
	}

	# For access to system account.
	accounts { $SYS { users = [ { user: "admin", pass: "s3cr3t!" } ] } }
`

	test := func(t *testing.T, replicas int) {
		c := createJetStreamClusterWithTemplate(t, templ, "R3S", 3)
		defer c.shutdown()

		nc := clientConnectToServer(t, c.randomServer())
		defer nc.Close()

		var batchFlowAck BatchFlowAck
		var pubAck JSPubAckResponse

		_, err := jsStreamCreate(t, nc, &StreamConfig{
			Name:              "TEST",
			Subjects:          []string{"foo"},
			Storage:           FileStorage,
			Replicas:          replicas,
			AllowBatchPublish: true,
		})
		require_NoError(t, err)

		inbox := nats.NewInbox()
		sub, err := nc.SubscribeSync(fmt.Sprintf("%s.>", inbox))
		require_NoError(t, err)
		defer sub.Drain()

		m := nats.NewMsg("foo")
		lseq := uint64(5)
		for seq := uint64(1); seq <= lseq; seq++ {
			if seq == lseq {
				m.Reply = generateFastBatchReply(inbox, "uuid", seq, 2, FastBatchGapFail, FastBatchOpCommit)
			} else if seq == 1 {
				m.Reply = generateFastBatchReply(inbox, "uuid", seq, 2, FastBatchGapFail, FastBatchOpStart)
			} else {
				m.Reply = generateFastBatchReply(inbox, "uuid", seq, 2, FastBatchGapFail, FastBatchOpAppend)
			}
			require_NoError(t, nc.PublishMsg(m))

			if seq == 1 || seq%2 == 0 {
				rmsg, err := sub.NextMsg(time.Second)
				require_NoError(t, err)
				batchFlowAck = BatchFlowAck{}
				require_NoError(t, json.Unmarshal(rmsg.Data, &batchFlowAck))
				if seq > 1 {
					require_Equal(t, batchFlowAck.Sequence, seq)
				} else {
					require_Equal(t, batchFlowAck.Sequence, 0)
				}
				require_Equal(t, batchFlowAck.Messages, 2)
			} else if seq == lseq {
				rmsg, err := sub.NextMsg(time.Second)
				require_NoError(t, err)
				pubAck = JSPubAckResponse{}
				require_NoError(t, json.Unmarshal(rmsg.Data, &pubAck))
				require_Equal(t, pubAck.Sequence, 5)
				require_Equal(t, pubAck.BatchId, "uuid")
				require_Equal(t, pubAck.BatchSize, 5)
			}

			// Sleep between messages such that we'll go over the batch timeout.
			// New messages being received should receive the timer.
			time.Sleep(250 * time.Millisecond)
		}
	}

	for _, replicas := range []int{1, 3} {
		t.Run(fmt.Sprintf("R%d", replicas), func(t *testing.T) {
			test(t, replicas)
		})
	}
}

func TestJetStreamFastBatchInitAckMessages(t *testing.T) {
	batches := &batching{}
	mset := &stream{srv: &Server{opts: &Options{}}}
	stopTimer := func(b *fastBatch) { b.timer.Stop() }

	// First batch, alone in the map: should get full window clamped to client max.
	b1 := batches.newFastBatch(mset, "b1", false, 1000)
	stopTimer(b1)
	require_Equal(t, b1.ackMessages, 500)

	// Second concurrent batch: must start at 1 to coordinate with sibling.
	b2 := batches.newFastBatch(mset, "b2", false, 1000)
	stopTimer(b2)
	require_Equal(t, b2.ackMessages, 1)

	// Resetting b1 while b2 still exists: also throttled to 1.
	b1.timer.Reset(time.Hour) // Need to reset, otherwise below is a noop.
	batches.fastBatchReset(mset, "b1", b1)
	stopTimer(b1)
	require_Equal(t, b1.ackMessages, 1)

	// Remove the sibling; a reset now returns to the full window.
	stopTimer(b2)
	delete(batches.fast, "b2")
	batches.fastBatchInit(b1)
	require_Equal(t, b1.ackMessages, 500)

	// A client-specified max below 500 is respected.
	stopTimer(b1)
	delete(batches.fast, "b1")
	b3 := batches.newFastBatch(mset, "b3", false, 50)
	stopTimer(b3)
	require_Equal(t, b3.ackMessages, 50)

	// And throttled to 1 when a sibling is present.
	b4 := batches.newFastBatch(mset, "b4", false, 50)
	stopTimer(b4)
	require_Equal(t, b4.ackMessages, 1)
}

func TestJetStreamFastBatchPublishSourceAndMirror(t *testing.T) {
	test := func(t *testing.T, replicas int) {
		c := createJetStreamClusterExplicit(t, "R3S", 3)
		defer c.shutdown()

		nc, js := jsClientConnect(t, c.randomServer())
		defer nc.Close()

		_, err := jsStreamCreate(t, nc, &StreamConfig{
			Name:              "TEST",
			Subjects:          []string{"foo"},
			Storage:           FileStorage,
			AllowBatchPublish: true,
			Replicas:          replicas,
		})
		require_NoError(t, err)

		for seq := uint64(1); seq <= 3; seq++ {
			m := nats.NewMsg("foo")
			if seq == 1 {
				m.Reply = generateFastBatchReply("", "uuid", seq, 10, FastBatchGapFail, FastBatchOpStart)
			} else {
				m.Reply = generateFastBatchReply("", "uuid", seq, 10, FastBatchGapFail, FastBatchOpAppend)
			}
			commit := seq == 3
			if !commit {
				require_NoError(t, nc.PublishMsg(m))
				continue
			}

			inbox := nats.NewInbox()
			sub, err := nc.SubscribeSync(fmt.Sprintf("%s.>", inbox))
			require_NoError(t, err)
			defer sub.Drain()

			m.Reply = generateFastBatchReply(inbox, "uuid", seq, 10, FastBatchGapFail, FastBatchOpCommit)
			require_NoError(t, nc.PublishMsg(m))
			rmsg, err := sub.NextMsg(time.Second)
			require_NoError(t, err)
			var pubAck JSPubAckResponse
			require_NoError(t, json.Unmarshal(rmsg.Data, &pubAck))
			require_Equal(t, pubAck.Sequence, 3)
			require_Equal(t, pubAck.BatchId, "uuid")
			require_Equal(t, pubAck.BatchSize, 3)
		}

		require_NoError(t, js.DeleteMsg("TEST", 2))
		checkFor(t, 2*time.Second, 200*time.Millisecond, func() error {
			return checkState(t, c, globalAccountName, "TEST")
		})

		// Mirror can source batched messages but can't do fast batching itself.
		_, err = jsStreamCreate(t, nc, &StreamConfig{
			Name:              "M-no-batch",
			Storage:           FileStorage,
			Mirror:            &StreamSource{Name: "TEST"},
			Replicas:          replicas,
			AllowBatchPublish: true,
		})
		require_Error(t, err, NewJSMirrorWithBatchPublishError())

		_, err = js.AddStream(&nats.StreamConfig{
			Name:     "M",
			Mirror:   &nats.StreamSource{Name: "TEST"},
			Replicas: replicas,
		})
		require_NoError(t, err)

		_, err = jsStreamCreate(t, nc, &StreamConfig{
			Name:              "S",
			Storage:           FileStorage,
			Sources:           []*StreamSource{{Name: "TEST"}},
			Replicas:          replicas,
			AllowBatchPublish: true,
		})
		require_NoError(t, err)

		checkFor(t, 2*time.Second, 200*time.Millisecond, func() error {
			for _, name := range []string{"M", "S"} {
				if si, err := js.StreamInfo(name); err != nil {
					return err
				} else if si.State.Msgs != 2 {
					return fmt.Errorf("expected 2 messages for stream %q, got %d", name, si.State.Msgs)
				}
			}
			return nil
		})

		// Ensure the batching headers were removed when ingested into the source/mirror.
		rsm, err := js.GetMsg("M", 1)
		require_NoError(t, err)
		require_Len(t, len(rsm.Header), 0)

		rsm, err = js.GetMsg("M", 3)
		require_NoError(t, err)
		require_Len(t, len(rsm.Header), 0)

		rsm, err = js.GetMsg("S", 1)
		require_NoError(t, err)
		require_Len(t, len(rsm.Header), 1)
		src := rsm.Header.Get(JSStreamSource)
		require_True(t, strings.HasPrefix(src, "TEST 1 > > foo"))

		rsm, err = js.GetMsg("S", 2)
		require_NoError(t, err)
		require_Len(t, len(rsm.Header), 1)
		src = rsm.Header.Get(JSStreamSource)
		require_True(t, strings.HasPrefix(src, "TEST 3 > > foo"))
	}

	t.Run("R1", func(t *testing.T) { test(t, 1) })
	t.Run("R3", func(t *testing.T) { test(t, 3) })
}

func TestJetStreamFastBatchPublishDuplicates(t *testing.T) {
	test := func(t *testing.T, replicas int) {
		c := createJetStreamClusterExplicit(t, "R3S", 3)
		defer c.shutdown()

		nc := clientConnectToServer(t, c.randomServer())
		defer nc.Close()

		var batchFlowAck BatchFlowAck
		var pubAck JSPubAckResponse

		_, err := jsStreamCreate(t, nc, &StreamConfig{
			Name:              "TEST",
			Subjects:          []string{"foo"},
			Storage:           FileStorage,
			Replicas:          replicas,
			AllowBatchPublish: true,
		})
		require_NoError(t, err)

		inbox := nats.NewInbox()
		sub, err := nc.SubscribeSync(fmt.Sprintf("%s.>", inbox))
		require_NoError(t, err)
		defer sub.Drain()

		// Simulate storing a duplicate message.
		msgId := "msgId"
		sl := c.streamLeader(globalAccountName, "TEST")
		require_NotNil(t, sl)
		mset, err := sl.globalAccount().lookupStream("TEST")
		require_NoError(t, err)
		dseq := uint64(1)
		// If replicated, we simulate it being proposed still.
		if replicas > 1 {
			dseq = 0
		}
		mset.storeMsgId(&ddentry{msgId, dseq, 0})

		// Publish a "batch" which immediately commits.
		m := nats.NewMsg("foo")
		m.Header.Set(JSMsgId, msgId)
		m.Reply = generateFastBatchReply(inbox, "uuid", 1, 0, FastBatchGapFail, FastBatchOpCommit)
		require_NoError(t, nc.PublishMsg(m))
		// The PubAck comes in immediately without an intermediate BatchFlowAck.
		rmsg, err := sub.NextMsg(time.Second)
		require_NoError(t, err)
		pubAck = JSPubAckResponse{}
		require_NoError(t, json.Unmarshal(rmsg.Data, &pubAck))
		if replicas == 1 {
			require_True(t, pubAck.Error == nil)
			require_Equal(t, pubAck.Sequence, 1)
			require_True(t, pubAck.Duplicate)
			require_Equal(t, pubAck.BatchId, "uuid")
			require_Equal(t, pubAck.BatchSize, 1)
		} else {
			require_True(t, pubAck.Error != nil)
			require_Equal(t, pubAck.Error.Error(), NewJSStreamDuplicateMessageConflictError().Error())
		}
		mset.mu.RLock()
		batches := mset.batches
		batches.mu.Lock()
		fastBatches := len(mset.batches.fast)
		batches.mu.Unlock()
		mset.mu.RUnlock()
		require_Len(t, fastBatches, 0)

		// Flow setting so we get one flow control message below.
		flow := uint16(3)

		// Publish a batch of N messages that are all duplicates, we expect
		// to receive both a flow control message and PubAck.
		for seq, batch := uint64(1), uint64(5); seq <= batch; seq++ {
			if seq == batch {
				m.Reply = generateFastBatchReply(inbox, "uuid", seq, flow, FastBatchGapFail, FastBatchOpCommit)
			} else if seq == 1 {
				m.Reply = generateFastBatchReply(inbox, "uuid", seq, flow, FastBatchGapFail, FastBatchOpStart)
			} else {
				m.Reply = generateFastBatchReply(inbox, "uuid", seq, flow, FastBatchGapFail, FastBatchOpAppend)
			}
			require_NoError(t, nc.PublishMsg(m))

			// Can already pre-check receiving the first flow control message.
			if seq == 1 {
				rmsg, err = sub.NextMsg(time.Second)
				require_NoError(t, err)
				batchFlowAck = BatchFlowAck{}
				require_NoError(t, json.Unmarshal(rmsg.Data, &batchFlowAck))
				require_Equal(t, batchFlowAck.Sequence, 0)
				require_Equal(t, batchFlowAck.Messages, flow)
			}

			// Expect one flow control message for this batch.
			if seq%uint64(flow) == 0 {
				rmsg, err = sub.NextMsg(time.Second)
				require_NoError(t, err)
				batchFlowAck = BatchFlowAck{}
				require_NoError(t, json.Unmarshal(rmsg.Data, &batchFlowAck))
				require_Equal(t, batchFlowAck.Sequence, 3)
				require_Equal(t, batchFlowAck.Messages, flow)
			}
		}
		// Should receive the PubAck upon commit.
		rmsg, err = sub.NextMsg(time.Second)
		require_NoError(t, err)
		pubAck = JSPubAckResponse{}
		require_NoError(t, json.Unmarshal(rmsg.Data, &pubAck))
		require_True(t, pubAck.Error == nil)
		require_Equal(t, pubAck.Sequence, 0)
		require_False(t, pubAck.Duplicate)
		require_Equal(t, pubAck.BatchId, "uuid")
		require_Equal(t, pubAck.BatchSize, 5)

		mset.mu.RLock()
		batches = mset.batches
		batches.mu.Lock()
		fastBatches = len(mset.batches.fast)
		batches.mu.Unlock()
		mset.mu.RUnlock()
		require_Len(t, fastBatches, 0)
	}

	for _, replicas := range []int{1, 3} {
		t.Run(fmt.Sprintf("R%d", replicas), func(t *testing.T) {
			test(t, replicas)
		})
	}
}

func TestJetStreamFastBatchPublishDuplicatesCluster(t *testing.T) {
	s := RunBasicJetStreamServer(t)
	defer s.Shutdown()

	nc := clientConnectToServer(t, s)
	defer nc.Close()

	var batchFlowAck BatchFlowAck
	var pubAck JSPubAckResponse

	_, err := jsStreamCreate(t, nc, &StreamConfig{
		Name:              "TEST",
		Subjects:          []string{"foo"},
		Storage:           FileStorage,
		AllowBatchPublish: true,
	})
	require_NoError(t, err)

	inbox := nats.NewInbox()
	sub, err := nc.SubscribeSync(fmt.Sprintf("%s.>", inbox))
	require_NoError(t, err)
	defer sub.Drain()

	msgId := "msgId"
	m := nats.NewMsg("foo")
	m.Header.Set(JSMsgId, msgId)
	m.Reply = generateFastBatchReply(inbox, "uuid", 1, 4, FastBatchGapFail, FastBatchOpStart)
	require_NoError(t, nc.PublishMsg(m))
	rmsg, err := sub.NextMsg(time.Second)
	require_NoError(t, err)
	batchFlowAck = BatchFlowAck{}
	require_NoError(t, json.Unmarshal(rmsg.Data, &batchFlowAck))
	require_Equal(t, batchFlowAck.Messages, 4)
	require_Equal(t, batchFlowAck.Sequence, 0)

	mset, err := s.globalAccount().lookupStream("TEST")
	require_NoError(t, err)
	mset.mu.Lock()
	batches := mset.batches
	mset.mu.Unlock()
	require_NotNil(t, batches)

	batches.mu.Lock()
	b := batches.fast["uuid"]

	// Simulate a pending message that will take some time to be processed due to replication.
	b.lseq++
	b.pending++

	// Simulate a bunch of messages being marked as duplicate and not being accounted for.
	// These two would trigger the first ack.
	b.lseq += 2
	// These would trigger the second ack.
	b.lseq += 4

	// Now simulate the second message we published is processed.
	reply := generateFastBatchReply(inbox, "uuid", 2, 4, FastBatchGapFail, FastBatchOpAppend)
	batches.fastBatchRegisterSequences(mset, reply, 2, true, &FastBatch{id: "uuid", seq: 2})
	batches.mu.Unlock()

	// Normally we'd receive two flow control messages, but since both were triggered due to
	// the above process finishing, we only send one out to save bandwidth.
	rmsg, err = sub.NextMsg(time.Second)
	require_NoError(t, err)
	batchFlowAck = BatchFlowAck{}
	require_NoError(t, json.Unmarshal(rmsg.Data, &batchFlowAck))
	require_Equal(t, batchFlowAck.Messages, 4)
	require_Equal(t, batchFlowAck.Sequence, 8)

	// Now commit the batch (but this message is also a duplicate).
	m.Reply = generateFastBatchReply(inbox, "uuid", 9, 0, FastBatchGapFail, FastBatchOpCommit)
	require_NoError(t, nc.PublishMsg(m))
	rmsg, err = sub.NextMsg(time.Second)
	require_NoError(t, err)
	pubAck = JSPubAckResponse{}
	require_NoError(t, json.Unmarshal(rmsg.Data, &pubAck))
	require_True(t, pubAck.Error == nil)
	require_Equal(t, pubAck.Sequence, 2)
	require_False(t, pubAck.Duplicate)
	require_Equal(t, pubAck.BatchId, "uuid")
	require_Equal(t, pubAck.BatchSize, 9)
}

func TestJetStreamFastBatchPublishDuplicatesEobCommit(t *testing.T) {
	test := func(t *testing.T, replicas int) {
		c := createJetStreamClusterExplicit(t, "R3S", 3)
		defer c.shutdown()

		nc := clientConnectToServer(t, c.randomServer())
		defer nc.Close()

		var batchFlowAck BatchFlowAck
		var pubAck JSPubAckResponse

		_, err := jsStreamCreate(t, nc, &StreamConfig{
			Name:              "TEST",
			Subjects:          []string{"foo"},
			Storage:           FileStorage,
			Replicas:          replicas,
			AllowBatchPublish: true,
		})
		require_NoError(t, err)

		inbox := nats.NewInbox()
		sub, err := nc.SubscribeSync(fmt.Sprintf("%s.>", inbox))
		require_NoError(t, err)
		defer sub.Drain()

		msgId := "msgId"
		m := nats.NewMsg("foo")
		m.Header.Set(JSMsgId, msgId)
		for seq := uint64(1); seq <= 2; seq++ {
			if seq == 1 {
				m.Reply = generateFastBatchReply(inbox, "uuid", seq, 0, FastBatchGapFail, FastBatchOpStart)
			} else {
				m.Reply = generateFastBatchReply(inbox, "uuid", seq, 0, FastBatchGapFail, FastBatchOpAppend)
			}
			require_NoError(t, nc.PublishMsg(m))
		}
		rmsg, err := sub.NextMsg(time.Second)
		require_NoError(t, err)
		batchFlowAck = BatchFlowAck{}
		require_NoError(t, json.Unmarshal(rmsg.Data, &batchFlowAck))
		require_Equal(t, batchFlowAck.Messages, 10)

		m.Reply = generateFastBatchReply(inbox, "uuid", 3, 0, FastBatchGapFail, FastBatchOpCommitEob)
		require_NoError(t, nc.PublishMsg(m))
		rmsg, err = sub.NextMsg(time.Second)
		require_NoError(t, err)
		pubAck = JSPubAckResponse{}
		require_NoError(t, json.Unmarshal(rmsg.Data, &pubAck))
		require_True(t, pubAck.Error == nil)
		require_Equal(t, pubAck.Sequence, 1)
		require_Equal(t, pubAck.BatchId, "uuid")
		require_Equal(t, pubAck.BatchSize, 2)
	}

	for _, replicas := range []int{1, 3} {
		t.Run(fmt.Sprintf("R%d", replicas), func(t *testing.T) {
			test(t, replicas)
		})
	}
}

func TestJetStreamFastBatchPublishHeaderCheckError(t *testing.T) {
	test := func(t *testing.T, replicas int, gapMode string) {
		c := createJetStreamClusterExplicit(t, "R3S", 3)
		defer c.shutdown()

		nc := clientConnectToServer(t, c.randomServer())
		defer nc.Close()

		var batchFlowAck BatchFlowAck
		var pubAck JSPubAckResponse

		_, err := jsStreamCreate(t, nc, &StreamConfig{
			Name:              "TEST",
			Subjects:          []string{"foo"},
			Storage:           FileStorage,
			Replicas:          replicas,
			AllowBatchPublish: true,
		})
		require_NoError(t, err)

		inbox := nats.NewInbox()
		sub, err := nc.SubscribeSync(fmt.Sprintf("%s.>", inbox))
		require_NoError(t, err)
		defer sub.Drain()

		// Send the first message.
		m := nats.NewMsg("foo")
		m.Reply = generateFastBatchReply(inbox, "uuid", 1, 0, gapMode, FastBatchOpStart)
		require_NoError(t, nc.PublishMsg(m))

		// Add a header that will require returning an error.
		m.Header.Set(JSExpectedLastSeq, "100")
		m.Reply = generateFastBatchReply(inbox, "uuid", 2, 0, gapMode, FastBatchOpAppend)
		require_NoError(t, nc.PublishMsg(m))

		// The first message triggered the initial flow control message.
		rmsg, err := sub.NextMsg(time.Second)
		require_NoError(t, err)
		batchFlowAck = BatchFlowAck{}
		require_NoError(t, json.Unmarshal(rmsg.Data, &batchFlowAck))
		require_Equal(t, batchFlowAck.Messages, 10)

		// The second message should always report the error based on the header.
		// But it should report about this error on that sequence.
		rmsg, err = sub.NextMsg(time.Second)
		require_NoError(t, err)
		require_True(t, strings.HasPrefix(string(rmsg.Data), "{\"type\":\"err\","))
		var batchFlowErr BatchFlowErr
		require_NoError(t, json.Unmarshal(rmsg.Data, &batchFlowErr))
		require_Equal(t, batchFlowErr.Sequence, 2)
		require_True(t, batchFlowErr.Error != nil)
		require_Equal(t, batchFlowErr.Error.Error(), NewJSStreamWrongLastSequenceError(1).Error())

		switch gapMode {
		case FastBatchGapFail:
			// Nothing to do, we simply wait for the PubAck below.
			break
		case FastBatchGapOk:
			// Commit the batch.
			m.Header.Del(JSExpectedLastSeq)
			m.Reply = generateFastBatchReply(inbox, "uuid", 3, 0, gapMode, FastBatchOpCommitEob)
			require_NoError(t, nc.PublishMsg(m))
		default:
			t.Fatalf("Unexpected gap mode: %q", gapMode)
		}

		// We always expect a PubAck containing the last persisted message data.
		rmsg, err = sub.NextMsg(time.Second)
		require_NoError(t, err)
		pubAck = JSPubAckResponse{}
		require_NoError(t, json.Unmarshal(rmsg.Data, &pubAck))
		require_True(t, pubAck.Error == nil)
		require_Equal(t, pubAck.Sequence, 1)
		require_Equal(t, pubAck.BatchId, "uuid")
		switch {
		case gapMode == FastBatchGapFail:
			require_Equal(t, pubAck.BatchSize, 1)
		case gapMode == FastBatchGapOk:
			require_Equal(t, pubAck.BatchSize, 2)
		default:
			t.Fatalf("Unexpected gap mode: %q", gapMode)
		}
	}

	for _, replicas := range []int{1, 3} {
		for _, gapMode := range []string{FastBatchGapFail, FastBatchGapOk} {
			t.Run(fmt.Sprintf("R%d/%s", replicas, gapMode), func(t *testing.T) {
				test(t, replicas, gapMode)
			})
		}
	}
}

func TestJetStreamFastBatchPublishHeaderCheckErrorOnCommit(t *testing.T) {
	test := func(t *testing.T, replicas int, gapMode string, gap bool) {
		c := createJetStreamClusterExplicit(t, "R3S", 3)
		defer c.shutdown()

		nc := clientConnectToServer(t, c.randomServer())
		defer nc.Close()

		var batchFlowAck BatchFlowAck
		var pubAck JSPubAckResponse

		_, err := jsStreamCreate(t, nc, &StreamConfig{
			Name:              "TEST",
			Subjects:          []string{"foo"},
			Storage:           FileStorage,
			Replicas:          replicas,
			AllowBatchPublish: true,
		})
		require_NoError(t, err)

		inbox := nats.NewInbox()
		sub, err := nc.SubscribeSync(fmt.Sprintf("%s.>", inbox))
		require_NoError(t, err)
		defer sub.Drain()

		// Send the first message.
		m := nats.NewMsg("foo")
		m.Reply = generateFastBatchReply(inbox, "uuid", 1, 0, gapMode, FastBatchOpStart)
		require_NoError(t, nc.PublishMsg(m))

		// The first message triggered the initial flow control message.
		rmsg, err := sub.NextMsg(time.Second)
		require_NoError(t, err)
		batchFlowAck = BatchFlowAck{}
		require_NoError(t, json.Unmarshal(rmsg.Data, &batchFlowAck))
		require_Equal(t, batchFlowAck.Messages, 10)

		// Now commit the batch, but have the commit message itself fail the header checks.
		commitSeq := uint64(2)
		if gap {
			commitSeq = 3
		}
		m.Header.Set(JSExpectedLastSeq, "100")
		m.Reply = generateFastBatchReply(inbox, "uuid", commitSeq, 0, gapMode, FastBatchOpCommit)
		require_NoError(t, nc.PublishMsg(m))

		// A forward gap is always reported first.
		if gap {
			rmsg, err = sub.NextMsg(time.Second)
			require_NoError(t, err)
			var batchFlowGap BatchFlowGap
			require_NoError(t, json.Unmarshal(rmsg.Data, &batchFlowGap))
			require_Equal(t, batchFlowGap.ExpectedLastSequence, 2)
			require_Equal(t, batchFlowGap.CurrentSequence, 3)
		}

		// The commit message should report the error based on the header.
		rmsg, err = sub.NextMsg(time.Second)
		require_NoError(t, err)
		require_True(t, strings.HasPrefix(string(rmsg.Data), "{\"type\":\"err\","))
		var batchFlowErr BatchFlowErr
		require_NoError(t, json.Unmarshal(rmsg.Data, &batchFlowErr))
		require_Equal(t, batchFlowErr.Sequence, commitSeq)
		require_True(t, batchFlowErr.Error != nil)
		require_Equal(t, batchFlowErr.Error.Error(), NewJSStreamWrongLastSequenceError(1).Error())

		// Since the commit message errored, the commit must still complete and a PubAck
		// must be sent containing the last persisted message data. Otherwise, the batch
		// would be stranded; no PubAck, the cleanup timer stopped, and inflight accounting leaked.
		rmsg, err = sub.NextMsg(time.Second)
		require_NoError(t, err)
		pubAck = JSPubAckResponse{}
		require_NoError(t, json.Unmarshal(rmsg.Data, &pubAck))
		require_True(t, pubAck.Error == nil)
		require_Equal(t, pubAck.Sequence, 1)
		require_Equal(t, pubAck.BatchId, "uuid")
		// The batch size reflects the last accepted batch sequence, which includes skipped gaps.
		require_Equal(t, pubAck.BatchSize, commitSeq-1)

		// The batch must also be unregistered and not leak inflight accounting.
		sl := c.streamLeader(globalAccountName, "TEST")
		require_NotNil(t, sl)
		mset, err := sl.globalAccount().lookupStream("TEST")
		require_NoError(t, err)
		mset.mu.RLock()
		batches := mset.batches
		mset.mu.RUnlock()
		require_NotNil(t, batches)
		checkFor(t, 2*time.Second, 100*time.Millisecond, func() error {
			batches.mu.Lock()
			fastBatches := len(batches.fast)
			batches.mu.Unlock()
			if fastBatches != 0 {
				return fmt.Errorf("expected no inflight fast batches, got %d", fastBatches)
			}
			return nil
		})
	}

	for _, replicas := range []int{1, 3} {
		for _, gapMode := range []string{FastBatchGapFail, FastBatchGapOk} {
			t.Run(fmt.Sprintf("R%d/%s", replicas, gapMode), func(t *testing.T) {
				test(t, replicas, gapMode, false)
			})
		}
		// A forward gap on the commit message is only allowed in gap-ok mode.
		t.Run(fmt.Sprintf("R%d/gap/%s", replicas, FastBatchGapOk), func(t *testing.T) {
			test(t, replicas, FastBatchGapOk, true)
		})
	}
}

func TestJetStreamFastBatchPublishPing(t *testing.T) {
	s := RunBasicJetStreamServer(t)
	defer s.Shutdown()

	nc := clientConnectToServer(t, s)
	defer nc.Close()

	var batchFlowAck BatchFlowAck
	var pubAck JSPubAckResponse

	_, err := jsStreamCreate(t, nc, &StreamConfig{
		Name:              "TEST",
		Subjects:          []string{"foo"},
		Storage:           FileStorage,
		AllowBatchPublish: true,
	})
	require_NoError(t, err)

	inbox := nats.NewInbox()
	sub, err := nc.SubscribeSync(fmt.Sprintf("%s.>", inbox))
	require_NoError(t, err)
	defer sub.Drain()

	// Send the first message.
	m := nats.NewMsg("foo")
	m.Reply = generateFastBatchReply(inbox, "uuid", 1, 2, FastBatchGapOk, FastBatchOpStart)
	require_NoError(t, nc.PublishMsg(m))
	rmsg, err := sub.NextMsg(time.Second)
	require_NoError(t, err)
	batchFlowAck = BatchFlowAck{}
	require_NoError(t, json.Unmarshal(rmsg.Data, &batchFlowAck))
	require_Equal(t, batchFlowAck.Messages, 2)

	// Send the second message that will trigger the flow control message.
	m.Reply = generateFastBatchReply(inbox, "uuid", 2, 2, FastBatchGapOk, FastBatchOpAppend)
	require_NoError(t, nc.PublishMsg(m))
	rmsg, err = sub.NextMsg(time.Second)
	require_NoError(t, err)

	// A ping will resend the flow control message.
	m.Reply = generateFastBatchReply(inbox, "uuid", 2, 2, FastBatchGapOk, FastBatchOpPing)
	require_NoError(t, nc.PublishMsg(m))
	for i := range 2 {
		if i > 0 {
			rmsg, err = sub.NextMsg(time.Second)
			require_NoError(t, err)
		}
		batchFlowAck = BatchFlowAck{}
		require_NoError(t, json.Unmarshal(rmsg.Data, &batchFlowAck))
		require_Equal(t, batchFlowAck.Messages, 2)
		require_Equal(t, batchFlowAck.Sequence, 2)
	}

	// Simulate losing one message and a ping making us aware of the gap.
	m.Reply = generateFastBatchReply(inbox, "uuid", 3, 2, FastBatchGapOk, FastBatchOpPing)
	require_NoError(t, nc.PublishMsg(m))
	// First, we get the report of the detected gap.
	rmsg, err = sub.NextMsg(time.Second)
	require_NoError(t, err)
	var batchFlowGap BatchFlowGap
	require_NoError(t, json.Unmarshal(rmsg.Data, &batchFlowGap))
	require_Equal(t, batchFlowGap.ExpectedLastSequence, 3)
	require_Equal(t, batchFlowGap.CurrentSequence, 4)
	// Second, we get the last flow control message, since we might have missed that before.
	rmsg, err = sub.NextMsg(time.Second)
	require_NoError(t, err)
	batchFlowAck = BatchFlowAck{}
	require_NoError(t, json.Unmarshal(rmsg.Data, &batchFlowAck))
	require_Equal(t, batchFlowAck.Messages, 2)
	require_Equal(t, batchFlowAck.Sequence, 2)

	// Send another message.
	m.Reply = generateFastBatchReply(inbox, "uuid", 4, 2, FastBatchGapOk, FastBatchOpAppend)
	require_NoError(t, nc.PublishMsg(m))
	rmsg, err = sub.NextMsg(time.Second)
	require_NoError(t, err)
	batchFlowAck = BatchFlowAck{}
	require_NoError(t, json.Unmarshal(rmsg.Data, &batchFlowAck))
	require_Equal(t, batchFlowAck.Messages, 2)
	require_Equal(t, batchFlowAck.Sequence, 4)

	// Simulate losing many messages and a ping making us aware of the gap as well as updating flow control.
	m.Reply = generateFastBatchReply(inbox, "uuid", 100, 2, FastBatchGapOk, FastBatchOpPing)
	require_NoError(t, nc.PublishMsg(m))
	// First, we get the report of the detected gap.
	rmsg, err = sub.NextMsg(time.Second)
	require_NoError(t, err)
	batchFlowGap = BatchFlowGap{}
	require_NoError(t, json.Unmarshal(rmsg.Data, &batchFlowGap))
	require_Equal(t, batchFlowGap.ExpectedLastSequence, 5)
	require_Equal(t, batchFlowGap.CurrentSequence, 101)
	// Second, we get the updated flow control message.
	rmsg, err = sub.NextMsg(time.Second)
	require_NoError(t, err)
	batchFlowAck = BatchFlowAck{}
	require_NoError(t, json.Unmarshal(rmsg.Data, &batchFlowAck))
	require_Equal(t, batchFlowAck.Messages, 2)
	require_Equal(t, batchFlowAck.Sequence, 100)

	m.Reply = generateFastBatchReply(inbox, "uuid", 101, 2, FastBatchGapOk, FastBatchOpCommitEob)
	require_NoError(t, nc.PublishMsg(m))
	rmsg, err = sub.NextMsg(time.Second)
	require_NoError(t, err)
	pubAck = JSPubAckResponse{}
	require_NoError(t, json.Unmarshal(rmsg.Data, &pubAck))
	require_True(t, pubAck.Error == nil)
	require_Equal(t, pubAck.Sequence, 3)
	require_Equal(t, pubAck.BatchId, "uuid")
	require_Equal(t, pubAck.BatchSize, 100)
}

func TestJetStreamFastBatchPublishPingCannotStartBatch(t *testing.T) {
	s := RunBasicJetStreamServer(t)
	defer s.Shutdown()

	nc := clientConnectToServer(t, s)
	defer nc.Close()

	_, err := jsStreamCreate(t, nc, &StreamConfig{
		Name:              "TEST",
		Subjects:          []string{"foo"},
		Storage:           FileStorage,
		AllowBatchPublish: true,
	})
	require_NoError(t, err)

	inbox := nats.NewInbox()
	sub, err := nc.SubscribeSync(fmt.Sprintf("%s.>", inbox))
	require_NoError(t, err)
	defer sub.Drain()

	// A ping at batch sequence 1 must not start a new batch.
	for _, gapMode := range []string{FastBatchGapFail, FastBatchGapOk} {
		m := nats.NewMsg("foo")
		m.Reply = generateFastBatchReply(inbox, "uuid", 1, 2, gapMode, FastBatchOpPing)
		require_NoError(t, nc.PublishMsg(m))
		rmsg, err := sub.NextMsg(time.Second)
		require_NoError(t, err)
		var pubAck JSPubAckResponse
		require_NoError(t, json.Unmarshal(rmsg.Data, &pubAck))
		require_Error(t, pubAck.Error, NewJSBatchPublishUnknownBatchIDError())
	}

	mset, err := s.globalAccount().lookupStream("TEST")
	require_NoError(t, err)
	mset.mu.RLock()
	batches := mset.batches
	mset.mu.RUnlock()
	if batches != nil {
		batches.mu.Lock()
		inflight := len(batches.fast)
		batches.mu.Unlock()
		require_Len(t, inflight, 0)
	}
	require_Equal(t, globalInflightFastBatches.Load(), 0)
	_, err = sub.NextMsg(250 * time.Millisecond)
	require_Error(t, err, nats.ErrTimeout)
}

func TestJetStreamFastBatchPublishGapOkBackwardSeq(t *testing.T) {
	s := RunBasicJetStreamServer(t)
	defer s.Shutdown()

	nc := clientConnectToServer(t, s)
	defer nc.Close()

	_, err := jsStreamCreate(t, nc, &StreamConfig{
		Name:              "TEST",
		Subjects:          []string{"foo"},
		Storage:           FileStorage,
		AllowBatchPublish: true,
	})
	require_NoError(t, err)

	inbox := nats.NewInbox()
	sub, err := nc.SubscribeSync(fmt.Sprintf("%s.>", inbox))
	require_NoError(t, err)
	defer sub.Drain()

	// Start the batch in gapOk mode.
	m := nats.NewMsg("foo")
	m.Reply = generateFastBatchReply(inbox, "uuid", 1, 0, FastBatchGapOk, FastBatchOpStart)
	require_NoError(t, nc.PublishMsg(m))
	rmsg, err := sub.NextMsg(time.Second)
	require_NoError(t, err)
	var batchFlowAck BatchFlowAck
	require_NoError(t, json.Unmarshal(rmsg.Data, &batchFlowAck))

	// Forward-jump to seq=5; the gap is accepted.
	m.Reply = generateFastBatchReply(inbox, "uuid", 5, 0, FastBatchGapOk, FastBatchOpAppend)
	require_NoError(t, nc.PublishMsg(m))
	rmsg, err = sub.NextMsg(time.Second)
	require_NoError(t, err)
	var batchFlowGap BatchFlowGap
	require_NoError(t, json.Unmarshal(rmsg.Data, &batchFlowGap))
	require_Equal(t, batchFlowGap.ExpectedLastSequence, 2)
	require_Equal(t, batchFlowGap.CurrentSequence, 5)

	// A backward batch.seq must not rewind b.lseq; it aborts the batch.
	// A BatchFlowGap is sent for the backward gap.
	m.Reply = generateFastBatchReply(inbox, "uuid", 3, 0, FastBatchGapOk, FastBatchOpAppend)
	require_NoError(t, nc.PublishMsg(m))
	rmsg, err = sub.NextMsg(time.Second)
	require_NoError(t, err)
	batchFlowGap = BatchFlowGap{}
	require_NoError(t, json.Unmarshal(rmsg.Data, &batchFlowGap))
	require_Equal(t, batchFlowGap.ExpectedLastSequence, 6)
	require_Equal(t, batchFlowGap.CurrentSequence, 3)

	// The batch is committed directly and a PubAck is delivered (would never arrive if the
	// backward seq were silently accepted and the batch kept running).
	rmsg, err = sub.NextMsg(time.Second)
	require_NoError(t, err)
	var pubAck JSPubAckResponse
	require_NoError(t, json.Unmarshal(rmsg.Data, &pubAck))
	require_True(t, pubAck.Error == nil)
	require_Equal(t, pubAck.BatchId, "uuid")

	// Ensure no other messages arrived after the PubAck.
	_, err = sub.NextMsg(200 * time.Millisecond)
	require_Error(t, err, nats.ErrTimeout)
}

func TestJetStreamFastBatchPublishBackwardGapDuplicateAndLowerSeq(t *testing.T) {
	type step struct {
		seq uint64
		op  int
	}
	test := func(t *testing.T, replicas int, gapMode string, steps []step, abort step, count uint64) {
		c := createJetStreamClusterExplicit(t, "R3S", 3)
		defer c.shutdown()

		nc, js := jsClientConnect(t, c.randomServer())
		defer nc.Close()

		_, err := jsStreamCreate(t, nc, &StreamConfig{
			Name:              "TEST",
			Subjects:          []string{"foo"},
			Storage:           FileStorage,
			Replicas:          replicas,
			AllowBatchPublish: true,
		})
		require_NoError(t, err)

		inbox := nats.NewInbox()
		sub, err := nc.SubscribeSync(fmt.Sprintf("%s.>", inbox))
		require_NoError(t, err)
		defer sub.Drain()

		m := nats.NewMsg("foo")
		for _, st := range steps {
			m.Reply = generateFastBatchReply(inbox, "uuid", st.seq, 0, gapMode, st.op)
			require_NoError(t, nc.PublishMsg(m))
			if st.seq == 1 {
				rmsg, err := sub.NextMsg(time.Second)
				require_NoError(t, err)
				var batchFlowAck BatchFlowAck
				require_NoError(t, json.Unmarshal(rmsg.Data, &batchFlowAck))
				require_Equal(t, batchFlowAck.Sequence, 0)
			}
		}

		// The duplicate or lower sequence reports a backward gap and ends the batch.
		m.Reply = generateFastBatchReply(inbox, "uuid", abort.seq, 0, gapMode, abort.op)
		require_NoError(t, nc.PublishMsg(m))
		rmsg, err := sub.NextMsg(time.Second)
		require_NoError(t, err)
		require_True(t, strings.HasPrefix(string(rmsg.Data), "{\"type\":\"gap\","))
		var batchFlowGap BatchFlowGap
		require_NoError(t, json.Unmarshal(rmsg.Data, &batchFlowGap))
		require_Equal(t, batchFlowGap.ExpectedLastSequence, count+1)
		require_Equal(t, batchFlowGap.CurrentSequence, abort.seq)

		rmsg, err = sub.NextMsg(time.Second)
		require_NoError(t, err)
		var pubAck JSPubAckResponse
		require_NoError(t, json.Unmarshal(rmsg.Data, &pubAck))
		require_True(t, pubAck.Error == nil)
		require_Equal(t, pubAck.BatchId, "uuid")
		require_Equal(t, pubAck.BatchSize, count)
		require_Equal(t, pubAck.Sequence, count)

		// No additional messages are sent.
		_, err = sub.NextMsg(200 * time.Millisecond)
		require_Error(t, err, nats.ErrTimeout)

		// The duplicate or lower sequence message is not stored.
		si, err := js.StreamInfo("TEST")
		require_NoError(t, err)
		require_Equal(t, si.State.Msgs, count)
		require_Equal(t, si.State.LastSeq, count)

		// The batch is gone, continuing it is rejected.
		m.Reply = generateFastBatchReply(inbox, "uuid", count+1, 0, gapMode, FastBatchOpAppend)
		require_NoError(t, nc.PublishMsg(m))
		rmsg, err = sub.NextMsg(time.Second)
		require_NoError(t, err)
		pubAck = JSPubAckResponse{}
		require_NoError(t, json.Unmarshal(rmsg.Data, &pubAck))
		require_Error(t, pubAck.Error, NewJSBatchPublishUnknownBatchIDError())
	}

	start := step{1, FastBatchOpStart}
	cases := []struct {
		name  string
		steps []step
		abort step
		count uint64
	}{
		{"DuplicateStart", []step{start}, start, 1},
		{"DuplicateStartAfterAppend", []step{start, {2, FastBatchOpAppend}}, start, 2},
		{"DuplicateAppend", []step{start, {2, FastBatchOpAppend}}, step{2, FastBatchOpAppend}, 2},
		{"LowerAppend", []step{start, {2, FastBatchOpAppend}, {3, FastBatchOpAppend}}, step{2, FastBatchOpAppend}, 3},
		{"DuplicateCommit", []step{start, {2, FastBatchOpAppend}}, step{2, FastBatchOpCommit}, 2},
		{"LowerCommit", []step{start, {2, FastBatchOpAppend}, {3, FastBatchOpAppend}}, step{2, FastBatchOpCommit}, 3},
		{"LowerCommitEob", []step{start, {2, FastBatchOpAppend}, {3, FastBatchOpAppend}}, step{2, FastBatchOpCommitEob}, 3},
	}
	for _, replicas := range []int{1, 3} {
		for _, gapMode := range []string{FastBatchGapOk, FastBatchGapFail} {
			for _, tc := range cases {
				t.Run(fmt.Sprintf("R%d/%s/%s", replicas, gapMode, tc.name), func(t *testing.T) {
					test(t, replicas, gapMode, tc.steps, tc.abort, tc.count)
				})
			}
		}
	}
}

func TestJetStreamFastBatchSequentialDuplicateAndErrorPubAck(t *testing.T) {
	s := RunBasicJetStreamServer(t)
	defer s.Shutdown()

	nc := clientConnectToServer(t, s)
	defer nc.Close()

	_, err := jsStreamCreate(t, nc, &StreamConfig{
		Name:              "TEST",
		Subjects:          []string{"foo"},
		Storage:           FileStorage,
		AllowBatchPublish: true,
	})
	require_NoError(t, err)

	// Publish the first message.
	msgId := "msgId"
	m := nats.NewMsg("foo")
	m.Header.Set(JSMsgId, msgId)
	_, err = nc.RequestMsg(m, time.Second)
	require_NoError(t, err)

	inbox := nats.NewInbox()
	sub, err := nc.SubscribeSync(fmt.Sprintf("%s.>", inbox))
	require_NoError(t, err)
	defer sub.Drain()

	// Send the first message, which is a duplicate.
	m.Header.Set(JSMsgId, msgId)
	m.Reply = generateFastBatchReply(inbox, "uuid", 1, 2, FastBatchGapFail, FastBatchOpStart)
	require_NoError(t, nc.PublishMsg(m))
	rmsg, err := sub.NextMsg(time.Second)
	require_NoError(t, err)
	var batchFlowAck BatchFlowAck
	require_NoError(t, json.Unmarshal(rmsg.Data, &batchFlowAck))
	require_Equal(t, batchFlowAck.Messages, 2)

	// Send the second message which triggers an error and terminates the batch.
	m.Header.Del(JSMsgId)
	m.Header.Set(JSExpectedLastSeq, "100")
	m.Reply = generateFastBatchReply(inbox, "uuid", 2, 2, FastBatchGapFail, FastBatchOpAppend)
	require_NoError(t, nc.PublishMsg(m))
	rmsg, err = sub.NextMsg(time.Second)
	require_NoError(t, err)
	var batchFlowErr BatchFlowErr
	require_NoError(t, json.Unmarshal(rmsg.Data, &batchFlowErr))
	require_Equal(t, batchFlowErr.Sequence, 2)
	require_True(t, batchFlowErr.Error != nil)
	require_Equal(t, batchFlowErr.Error.Error(), NewJSStreamWrongLastSequenceError(1).Error())

	// Lastly, we expect the PubAck.
	rmsg, err = sub.NextMsg(time.Second)
	require_NoError(t, err)
	var pubAck JSPubAckResponse
	require_NoError(t, json.Unmarshal(rmsg.Data, &pubAck))
	require_True(t, pubAck.Error == nil)
	require_Equal(t, pubAck.BatchId, "uuid")
	require_Equal(t, pubAck.BatchSize, 1)
	// The batch only had duplicate messages.
	// Technically, we shouldn't return a zero-sequence, but it's unavoidable.
	require_Equal(t, pubAck.Sequence, 0)
}

func TestJetStreamFastBatchPublishAccImportExport(t *testing.T) {
	conf := createConfFile(t, []byte(fmt.Sprintf(`
		listen: 127.0.0.1:-1
		jetstream: {max_mem_store: 1MB, max_file_store: 1MB, store_dir: %q}
		accounts: {
			A: {
				jetstream: enabled
				users: [ {user: a, password: a} ]
				exports [
					{ service: "foo" }
				]
			},
			B: {
				users: [ {user: b, password: b} ]
				imports [
					{ service: { subject: "foo", account: A } }
				]
			},
		}
	`, t.TempDir())))

	s, _ := RunServerWithConfig(conf)
	defer s.Shutdown()

	nc, err := nats.Connect(s.ClientURL(), nats.UserInfo("a", "a"))
	require_NoError(t, err)
	defer nc.Close()

	cfg := &StreamConfig{
		Name:              "TEST",
		Subjects:          []string{"foo"},
		Storage:           FileStorage,
		AllowBatchPublish: true,
	}
	_, err = jsStreamCreate(t, nc, cfg)
	require_NoError(t, err)

	checkFastBatchPublish := func(user string) {
		nc, err := nats.Connect(s.ClientURL(), nats.UserInfo(user, user))
		require_NoError(t, err)
		defer nc.Close()

		inbox := nats.NewInbox()
		sub, err := nc.SubscribeSync(fmt.Sprintf("%s.>", inbox))
		require_NoError(t, err)
		defer sub.Drain()

		m := nats.NewMsg("foo")
		m.Reply = generateFastBatchReply(inbox, "uuid", 1, 0, FastBatchGapFail, FastBatchOpCommit)
		require_NoError(t, nc.PublishMsg(m))
		rmsg, err := sub.NextMsg(time.Second)
		require_NoError(t, err)
		var pubAck JSPubAckResponse
		require_NoError(t, json.Unmarshal(rmsg.Data, &pubAck))
		require_True(t, pubAck.Error == nil)
		require_Equal(t, pubAck.BatchId, "uuid")
		require_Equal(t, pubAck.BatchSize, 1)
	}
	checkFastBatchPublish("a")
	checkFastBatchPublish("b")
}

func TestJetStreamFastBatchPublishFlowControlOnLeaderChange(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "R3S", 3)
	defer c.shutdown()

	nc := clientConnectToServer(t, c.randomServer())
	defer nc.Close()

	cfg := &StreamConfig{
		Name:              "TEST",
		Subjects:          []string{"foo"},
		Storage:           FileStorage,
		Replicas:          3,
		AllowBatchPublish: true,
	}

	_, err := jsStreamCreate(t, nc, cfg)
	require_NoError(t, err)

	inbox := nats.NewInbox()
	sub, err := nc.SubscribeSync(fmt.Sprintf("%s.>", inbox))
	require_NoError(t, err)
	defer sub.Drain()

	m := nats.NewMsg("foo")
	for seq := uint64(1); seq <= 3; seq++ {
		if seq == 1 {
			m.Reply = generateFastBatchReply(inbox, "uuid", seq, 2, FastBatchGapFail, FastBatchOpStart)
		} else {
			m.Reply = generateFastBatchReply(inbox, "uuid", seq, 2, FastBatchGapFail, FastBatchOpAppend)
		}
		require_NoError(t, nc.PublishMsg(m))
	}
	rmsg, err := sub.NextMsg(time.Second)
	require_NoError(t, err)
	var batchFlowAck BatchFlowAck
	require_NoError(t, json.Unmarshal(rmsg.Data, &batchFlowAck))
	require_Equal(t, batchFlowAck.Messages, 2)
	require_Equal(t, batchFlowAck.Sequence, 0)

	rmsg, err = sub.NextMsg(time.Second)
	require_NoError(t, err)
	batchFlowAck = BatchFlowAck{}
	require_NoError(t, json.Unmarshal(rmsg.Data, &batchFlowAck))
	require_Equal(t, batchFlowAck.Messages, 2)
	require_Equal(t, batchFlowAck.Sequence, 2)

	// Should receive a flow ack after a leader change. This sends us a ping, but also informs us which
	// messages were persisted after the leader change.
	sl := c.streamLeader(globalAccountName, "TEST")
	require_NotNil(t, sl)
	mset, err := sl.globalAccount().lookupStream("TEST")
	require_NoError(t, err)
	require_NoError(t, mset.raftNode().StepDown())

	rmsg, err = sub.NextMsg(time.Second)
	require_NoError(t, err)
	batchFlowAck = BatchFlowAck{}
	require_NoError(t, json.Unmarshal(rmsg.Data, &batchFlowAck))
	require_Equal(t, batchFlowAck.Messages, 2)
	require_Equal(t, batchFlowAck.Sequence, 3)

	checkFor(t, 2*time.Second, 200*time.Millisecond, func() error {
		if err = checkState(t, c, globalAccountName, "TEST"); err != nil {
			return err
		}
		for _, s := range c.servers {
			mset, err = s.globalAccount().lookupStream("TEST")
			if err != nil {
				return err
			}
			mset.mu.RLock()
			var batches int
			if mset.batches != nil {
				mset.batches.mu.Lock()
				batches = len(mset.batches.fast)
				mset.batches.mu.Unlock()
			}
			mset.mu.RUnlock()
			if batches != 1 {
				return fmt.Errorf("expected 1 batch on %s, got %d", s.Name(), batches)
			}
		}
		return nil
	})

	// We now expect a new flow ack after two messages.
	for seq := uint64(4); seq <= 5; seq++ {
		m.Reply = generateFastBatchReply(inbox, "uuid", seq, 2, FastBatchGapFail, FastBatchOpAppend)
		require_NoError(t, nc.PublishMsg(m))
	}
	rmsg, err = sub.NextMsg(time.Second)
	require_NoError(t, err)
	batchFlowAck = BatchFlowAck{}
	require_NoError(t, json.Unmarshal(rmsg.Data, &batchFlowAck))
	require_Equal(t, batchFlowAck.Messages, 2)
	require_Equal(t, batchFlowAck.Sequence, 5)

	// Commit the final message. We expect that the followers also clean up their state.
	m.Reply = generateFastBatchReply(inbox, "uuid", 6, 2, FastBatchGapFail, FastBatchOpCommit)
	require_NoError(t, nc.PublishMsg(m))
	rmsg, err = sub.NextMsg(time.Second)
	require_NoError(t, err)
	var pubAck JSPubAckResponse
	require_NoError(t, json.Unmarshal(rmsg.Data, &pubAck))
	require_True(t, pubAck.Error == nil)
	require_Equal(t, pubAck.BatchId, "uuid")
	require_Equal(t, pubAck.BatchSize, 6)
	require_Equal(t, pubAck.Sequence, 6)

	checkFor(t, 2*time.Second, 200*time.Millisecond, func() error {
		if err = checkState(t, c, globalAccountName, "TEST"); err != nil {
			return err
		}
		for _, s := range c.servers {
			mset, err = s.globalAccount().lookupStream("TEST")
			if err != nil {
				return err
			}
			mset.mu.RLock()
			var batches int
			if mset.batches != nil {
				mset.batches.mu.Lock()
				batches = len(mset.batches.fast)
				mset.batches.mu.Unlock()
			}
			mset.mu.RUnlock()
			if batches > 0 {
				return fmt.Errorf("expected no batches on %s, got %d", s.Name(), batches)
			}
		}
		return nil
	})
}

func TestJetStreamFastBatchPublishFlowControlOnLeaderChangeAfterFailedCommitProposal(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "R3S", 3)
	defer c.shutdown()

	nc := clientConnectToServer(t, c.randomServer())
	defer nc.Close()

	cfg := &StreamConfig{
		Name:              "TEST",
		Subjects:          []string{"foo"},
		Storage:           FileStorage,
		Replicas:          3,
		AllowBatchPublish: true,
	}

	_, err := jsStreamCreate(t, nc, cfg)
	require_NoError(t, err)

	inbox := nats.NewInbox()
	sub, err := nc.SubscribeSync(fmt.Sprintf("%s.>", inbox))
	require_NoError(t, err)
	defer sub.Drain()

	m := nats.NewMsg("foo")
	for seq := uint64(1); seq <= 3; seq++ {
		if seq == 1 {
			m.Reply = generateFastBatchReply(inbox, "uuid", seq, 0, FastBatchGapFail, FastBatchOpStart)
		} else {
			m.Reply = generateFastBatchReply(inbox, "uuid", seq, 0, FastBatchGapFail, FastBatchOpAppend)
		}
		require_NoError(t, nc.PublishMsg(m))
	}
	rmsg, err := sub.NextMsg(time.Second)
	require_NoError(t, err)
	var batchFlowAck BatchFlowAck
	require_NoError(t, json.Unmarshal(rmsg.Data, &batchFlowAck))
	require_Equal(t, batchFlowAck.Messages, 10)
	require_Equal(t, batchFlowAck.Sequence, 0)

	// Should receive a flow ack after a leader change. This sends us a ping, but also informs us which
	// messages were persisted after the leader change.
	sl := c.streamLeader(globalAccountName, "TEST")
	require_NotNil(t, sl)
	mset, err := sl.globalAccount().lookupStream("TEST")
	require_NoError(t, err)

	mset.mu.Lock()
	if mset.batches == nil {
		mset.mu.Unlock()
		t.Fatal("batches map is nil")
	}
	mset.batches.mu.Lock()
	b := mset.batches.fast["uuid"]
	if b == nil {
		mset.batches.mu.Unlock()
		mset.mu.Unlock()
		t.Fatal("batch is nil")
	}
	// Simulate one more pending message that commits, but the proposal didn't make it due to the leader change.
	b.commit = true
	b.pending++
	mset.batches.mu.Unlock()
	mset.mu.Unlock()

	// Change to any other leader.
	require_NoError(t, mset.raftNode().StepDown())

	// Change back to the previous leader.
	c.waitOnStreamLeader(globalAccountName, "TEST")
	nl := c.streamLeader(globalAccountName, "TEST")
	require_NotNil(t, nl)
	require_NotEqual(t, nl, sl)
	nmset, err := nl.globalAccount().lookupStream("TEST")
	require_NoError(t, err)
	require_NoError(t, nmset.raftNode().StepDown(mset.raftNode().ID()))

	c.waitOnStreamLeader(globalAccountName, "TEST")
	require_Equal(t, c.streamLeader(globalAccountName, "TEST"), sl)

	// The previous leader should now have reset the committed flag as the proposal failed.
	checkFor(t, 2*time.Second, 200*time.Millisecond, func() error {
		mset.mu.RLock()
		defer mset.mu.RUnlock()
		if mset.batches == nil {
			return errors.New("batches map is nil")
		}
		mset.batches.mu.Lock()
		defer mset.batches.mu.Unlock()
		b = mset.batches.fast["uuid"]
		if b == nil {
			return errors.New("batch is nil")
		}
		if b.pending != 0 {
			return errors.New("pending isn't reset")
		}
		if b.commit {
			return errors.New("commit isn't reset")
		}
		return nil
	})
}

func TestJetStreamFastBatchPublishLimits(t *testing.T) {
	streamMaxFastBatchInflightPerStream = 1
	streamMaxFastBatchInflightTotal = 1
	streamMaxBatchTimeout = 500 * time.Millisecond
	defer func() {
		streamMaxFastBatchInflightPerStream = streamDefaultMaxFastBatchInflightPerStream
		streamMaxFastBatchInflightTotal = streamDefaultMaxFastBatchInflightTotal
		streamMaxBatchTimeout = streamDefaultMaxBatchTimeout
	}()

	test := func(t *testing.T, replicas int) {
		c := createJetStreamClusterExplicit(t, "R3S", 3)
		defer c.shutdown()

		nc := clientConnectToServer(t, c.randomServer())
		defer nc.Close()

		var batchFlowAck BatchFlowAck
		var pubAck JSPubAckResponse

		cfg := &StreamConfig{
			Name:              "FOO",
			Subjects:          []string{"foo"},
			Storage:           FileStorage,
			Retention:         LimitsPolicy,
			Replicas:          replicas,
			AllowBatchPublish: true,
		}
		_, err := jsStreamCreate(t, nc, cfg)
		require_NoError(t, err)

		// For testing total server-wide maximum inflight batches.
		cfg = &StreamConfig{
			Name:              "BAR",
			Subjects:          []string{"bar"},
			Storage:           FileStorage,
			Retention:         LimitsPolicy,
			Replicas:          replicas,
			AllowBatchPublish: true,
		}
		_, err = jsStreamCreate(t, nc, cfg)
		require_NoError(t, err)

		inbox := nats.NewInbox()
		sub, err := nc.SubscribeSync(fmt.Sprintf("%s.>", inbox))
		require_NoError(t, err)
		defer sub.Drain()

		// A batch ID must not exceed the maximum length.
		for _, length := range []int{64, 65} {
			longBatchId := strings.Repeat("A", length)
			m := nats.NewMsg("foo")
			m.Reply = generateFastBatchReply(inbox, longBatchId, 1, 0, FastBatchGapFail, FastBatchOpCommit)
			require_NoError(t, nc.PublishMsg(m))

			rmsg, err := sub.NextMsg(time.Second)
			require_NoError(t, err)
			pubAck = JSPubAckResponse{}
			require_NoError(t, json.Unmarshal(rmsg.Data, &pubAck))
			if length <= 64 {
				require_True(t, pubAck.Error == nil)
			} else {
				require_NotNil(t, pubAck.Error)
				require_Error(t, pubAck.Error, NewJSBatchPublishInvalidBatchIDError())
			}
		}

		// One batch is inflight.
		m := nats.NewMsg("foo")
		m.Reply = generateFastBatchReply(inbox, "uuid", 1, 0, FastBatchGapFail, FastBatchOpStart)
		require_NoError(t, nc.PublishMsg(m))

		rmsg, err := sub.NextMsg(time.Second)
		require_NoError(t, err)
		batchFlowAck = BatchFlowAck{}
		require_NoError(t, json.Unmarshal(rmsg.Data, &batchFlowAck))
		require_Equal(t, batchFlowAck.Messages, 10)
		require_Equal(t, batchFlowAck.Sequence, 0)

		// Another batch moves over the threshold and batch is denied.
		m = nats.NewMsg("foo")
		m.Reply = generateFastBatchReply(inbox, "exceeds_threshold", 1, 0, FastBatchGapFail, FastBatchOpCommit)
		require_NoError(t, nc.PublishMsg(m))
		rmsg, err = sub.NextMsg(time.Second)
		require_NoError(t, err)
		pubAck = JSPubAckResponse{}
		require_NoError(t, json.Unmarshal(rmsg.Data, &pubAck))
		require_Error(t, pubAck.Error, NewJSBatchPublishTooManyInflightError())

		// Another batch on a different stream moves over the server-wide threshold and batch is denied.
		m = nats.NewMsg("bar")
		m.Reply = generateFastBatchReply(inbox, "bar", 1, 0, FastBatchGapFail, FastBatchOpCommit)
		require_NoError(t, nc.PublishMsg(m))
		rmsg, err = sub.NextMsg(time.Second)
		require_NoError(t, err)
		pubAck = JSPubAckResponse{}
		require_NoError(t, json.Unmarshal(rmsg.Data, &pubAck))
		require_Error(t, pubAck.Error, NewJSBatchPublishTooManyInflightError())

		// The first batch should now time out.
		sl := c.streamLeader(globalAccountName, "FOO")
		mset, err := sl.globalAccount().lookupStream("FOO")
		require_NoError(t, err)
		checkFor(t, 2*time.Second, 200*time.Millisecond, func() error {
			mset.mu.RLock()
			batches := mset.batches
			mset.mu.RUnlock()
			if batches == nil {
				return errors.New("batches not found")
			}
			batches.mu.Lock()
			groups := len(batches.fast)
			batches.mu.Unlock()
			if groups != 0 {
				return fmt.Errorf("expected 0 groups, got %d", groups)
			}
			return nil
		})

		// Publishing to the batch should also error since it timed out.
		m = nats.NewMsg("foo")
		m.Reply = generateFastBatchReply(inbox, "uuid", 2, 0, FastBatchGapFail, FastBatchOpCommit)
		require_NoError(t, nc.PublishMsg(m))

		rmsg, err = sub.NextMsg(time.Second)
		require_NoError(t, err)
		pubAck = JSPubAckResponse{}
		require_NoError(t, json.Unmarshal(rmsg.Data, &pubAck))
		require_Error(t, pubAck.Error, NewJSBatchPublishUnknownBatchIDError())
	}

	t.Run("R1", func(t *testing.T) { test(t, 1) })
	t.Run("R3", func(t *testing.T) { test(t, 3) })
}
