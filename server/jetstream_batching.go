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

package server

import (
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"math"
	"math/big"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

var (
	// Tracks the total inflight batches, across all streams and accounts that enable batching.
	globalInflightAtomicBatches atomic.Int64
	globalInflightFastBatches   atomic.Int64
)

type batching struct {
	mu     sync.Mutex
	atomic map[string]*atomicBatch
	fast   map[string]*fastBatch
}

type atomicBatch struct {
	timer *time.Timer // Inactivity timer for the batch.
	lseq  uint64      // The highest sequence for this batch.
	store StreamStore // Where the batch is staged before committing.
}

type fastBatch struct {
	timer          *time.Timer // Inactivity timer for the batch.
	lseq           uint64      // The highest sequence for this batch.
	sseq           uint64      // Last persisted stream sequence.
	pseq           uint64      // Last persisted batch sequence (is always lower or equal to lseq).
	fseq           uint64      // Sequence of when we last sent a flow message (is always lower or equal to pseq).
	pending        uint32      // Number of pending messages in the batch waiting to be persisted.
	ackMessages    uint16      // Ack will be sent every N messages.
	maxAckMessages uint16      // Maximum ackMessages value the client allows.
	reply          string      // The last reply subject seen when persisting a message.
	gapOk          bool        // Whether a gap is okay, if not, the batch would be rejected.
	commit         bool        // If the batch is committed.
}

// newAtomicBatch creates an atomic batch publish object.
// Lock should be held.
func (batches *batching) newAtomicBatch(mset *stream, batchId string, replicas int, storage StorageType, storeDir, streamName string) (*atomicBatch, error) {
	store, err := newBatchStore(mset, batchId, replicas, storage, storeDir, streamName)
	if err != nil {
		return nil, err
	}
	b := &atomicBatch{store: store}
	b.setupCleanupTimer(mset, batchId, batches)
	return b, nil
}

// setupCleanupTimer sets up a timer to clean up the batch after a timeout.
func (b *atomicBatch) setupCleanupTimer(mset *stream, batchId string, batches *batching) {
	// Create a timer to clean up after timeout.
	timeout := getCleanupTimeout(mset)
	b.timer = time.AfterFunc(timeout, func() {
		b.cleanup(batchId, batches)
		mset.sendStreamBatchAbandonedAdvisory(batchId, BatchTimeout)
	})
}

// resetCleanupTimer resets the cleanup timer, allowing to extend the lifetime of the batch.
// Returns whether the timer was reset without it having expired before.
func (b *atomicBatch) resetCleanupTimer(mset *stream) bool {
	timeout := getCleanupTimeout(mset)
	return b.timer.Reset(timeout)
}

// cleanup deletes underlying resources associated with the batch and unregisters it from the stream's batches.
func (b *atomicBatch) cleanup(batchId string, batches *batching) {
	batches.mu.Lock()
	defer batches.mu.Unlock()
	b.cleanupLocked(batchId, batches)
}

// Lock should be held.
func (b *atomicBatch) cleanupLocked(batchId string, batches *batching) {
	if b.timer == nil {
		return
	}
	globalInflightAtomicBatches.Add(-1)
	b.timer.Stop()
	b.store.Delete(true)
	delete(batches.atomic, batchId)
	// Reset so that another invocation doesn't double-account.
	b.timer = nil
}

// Lock should be held.
func (b *atomicBatch) stopLocked() {
	if b.timer == nil {
		return
	}
	globalInflightAtomicBatches.Add(-1)
	b.timer.Stop()
	b.store.Stop()
	// Reset so that another invocation doesn't double-account.
	b.timer = nil
}

func getBatchStoreDir(storeDir, streamName, batchId string) (string, string) {
	bname := getHash(batchId)
	return bname, filepath.Join(storeDir, streamsDir, streamName, batchesDir, bname)
}

func newBatchStore(mset *stream, batchId string, replicas int, storage StorageType, storeDir, streamName string) (StreamStore, error) {
	if replicas == 1 && storage == FileStorage {
		bname, storeDir := getBatchStoreDir(storeDir, streamName, batchId)
		s := mset.srv
		fcfg := FileStoreConfig{
			AsyncFlush:  true,
			SyncOnFlush: true,
			BlockSize:   defaultLargeBlockSize,
			StoreDir:    storeDir,
			srv:         s,
		}
		prf := s.jsKeyGen(s.getOpts().JetStreamKey, mset.acc.Name)
		if prf != nil {
			// We are encrypted here, fill in correct cipher selection.
			fcfg.Cipher = s.getOpts().JetStreamCipher
		}
		oldprf := s.jsKeyGen(s.getOpts().JetStreamOldKey, mset.acc.Name)
		cfg := StreamConfig{Name: bname, Storage: FileStorage}
		return newFileStoreWithCreated(fcfg, cfg, time.Time{}, prf, oldprf)
	}
	return newMemStore(&StreamConfig{Name: _EMPTY_, Storage: MemoryStorage})
}

// readyForCommit indicates the batch is ready to be committed.
// If the timer has already cleaned up the batch, we can't commit.
// Otherwise, we ensure the timer does not clean up the batch in the meantime.
// Lock should be held.
func (b *atomicBatch) readyForCommit() *BatchAbandonReason {
	if !b.timer.Stop() {
		return &BatchTimeout
	}
	if b.store.FlushAllPending() != nil {
		return &BatchIncomplete
	}
	return nil
}

// newFastBatch creates a fast batch publish object and registers it in batches.fast.
// Lock should be held.
func (batches *batching) newFastBatch(mset *stream, batchId string, gapOk bool, maxAckMessages uint16) *fastBatch {
	b := &fastBatch{gapOk: gapOk, maxAckMessages: maxAckMessages}
	if batches.fast == nil {
		batches.fast = make(map[string]*fastBatch, 1)
	}
	batches.fast[batchId] = b
	batches.fastBatchInit(b)
	b.setupCleanupTimer(mset, batchId, batches)
	return b
}

// fastBatchInit (re)initializes the ackMessages field for a fast batch.
// The batch must already be registered in batches.fast.
// Lock should be held.
func (batches *batching) fastBatchInit(b *fastBatch) {
	// If it's the only batch, just allow what the client wants, otherwise we'll
	// need to coordinate and slowly ramp up this publisher.
	// TODO(mvv): fast ingest's initial flow value improvements?
	ackMessages := min(500, b.maxAckMessages)
	if len(batches.fast) > 1 {
		ackMessages = 1
	}
	b.ackMessages = ackMessages
}

// fastBatchReset resets the fast batch to an empty state and sends a flow control message.
// Lock should be held.
func (batches *batching) fastBatchReset(mset *stream, batchId string, b *fastBatch) {
	// If the timer already stopped before we could commit, we clean it up.
	if b.timer == nil || (!b.commit && !b.timer.Stop()) {
		b.cleanupLocked(batchId, batches)
		return
	}
	// Otherwise, reset the state.
	batches.fastBatchInit(b)
	b.timer.Reset(getCleanupTimeout(mset))
	b.commit = false
	b.pending = 0
	b.fseq, b.lseq = b.pseq, b.pseq
	b.sendFlowControl(b.fseq, mset, b.reply)
}

// fastBatchRegisterSequences registers the highest stored batch and stream sequence and returns
// whether a PubAck should be sent if the batch has been committed.
// If this is called on a follower, it only registers the highest stream and persisted batch sequences.
// Lock should be held.
func (batches *batching) fastBatchRegisterSequences(mset *stream, reply string, streamSeq uint64, isLeader bool, batch *FastBatch) bool {
	b, ok := batches.fast[batch.id]
	if !ok || !isLeader {
		// If this batch has committed, we can clean it up.
		if batch.commit {
			if b != nil {
				b.cleanupLocked(batch.id, batches)
			}
			return false
		}
		// Otherwise, even as a follower, we record the latest state of this batch.
		if b == nil || !b.resetCleanupTimer(mset) {
			if b != nil {
				// The timer couldn't be reset, this means the timer already runs and is likely
				// waiting to acquire the lock. We reset the timer here so it doesn't clean up
				// this batch that we're about to overwrite.
				b.timer = nil
			} else {
				// If this is a new batch for us, even though we're a follower, we still need
				// to account toward the global inflight limit.
				globalInflightFastBatches.Add(1)
			}
			// We'll need a copy as we'll use it as a key and later for cleanup.
			batchId := copyString(batch.id)
			b = batches.newFastBatch(mset, batchId, batch.gapOk, batch.flow)
		}
		b.sseq = streamSeq
		b.pseq, b.lseq = batch.seq, batch.seq
		b.reply = reply
		return false
	}
	b.reply = reply
	if b.pending > 0 {
		b.pending--
	}
	b.sseq = streamSeq
	// Store last persisted batch sequence.
	// If we have no remaining pending writes, we might have had duplicate messages
	// and need to send additional flow control messages.
	var skipped bool
	if b.pending == 0 {
		skipped = true
		b.pseq = b.lseq
	} else {
		b.pseq = batch.seq
	}
	// If the PubAck needs to be sent now as a result of a commit.
	if b.lseq == b.pseq && b.commit {
		b.cleanupLocked(batch.id, batches)
		// If we skipped ahead due to duplicate messages, send the PubAck with the highest sequence.
		if skipped {
			var buf [256]byte
			pubAck := append(buf[:0], mset.pubAck...)
			response := append(pubAck, strconv.FormatUint(b.sseq, 10)...)
			response = append(response, fmt.Sprintf(",\"batch\":%q,\"count\":%d}", batch.id, b.lseq)...)
			if len(reply) > 0 {
				mset.outq.sendMsg(reply, response)
			}
			return false
		}
		return true
	}
	b.checkFlowControl(mset, reply, batches)
	return false
}

// checkFlowControl checks whether a flow control message should be sent.
// If so, it updates the flow values to speed up or slow down the publisher if needed.
// Returns whether a flow control message was sent.
// Lock should be held.
func (b *fastBatch) checkFlowControl(mset *stream, reply string, batches *batching) bool {
	am := uint64(b.ackMessages)
	if b.pseq < b.fseq+am {
		return false
	}
	// Instead of sending multiple flow control messages, skip ahead to only send the last.
	steps := (b.pseq - b.fseq) / am
	b.fseq += steps * am

	// TODO(mvv): fast ingest's dynamic flow value improvements?
	//  This is currently just a simple value to have a working version. Should take average
	//  message sizes into account and compare how much this client is contributing to the
	//  ingest IPQ total size and messages and have publishers share based on that.
	maxAckMessages := uint16(500 / len(batches.fast))
	if maxAckMessages < 1 {
		maxAckMessages = 1
	}
	// Limit to the client's allowed maximum.
	if maxAckMessages > b.maxAckMessages {
		maxAckMessages = b.maxAckMessages
	}

	if b.ackMessages < maxAckMessages {
		// Ramp up.
		b.ackMessages *= 2
		if b.ackMessages > maxAckMessages {
			b.ackMessages = maxAckMessages
		}
	} else if b.ackMessages > maxAckMessages {
		// Slow down.
		b.ackMessages /= 2
		if b.ackMessages <= maxAckMessages {
			b.ackMessages = maxAckMessages
		}
	}

	// Finally, send the flow control message.
	b.sendFlowControl(b.fseq, mset, reply)
	return true
}

// sendFlowControl sends a fast batch flow control message for the current highest sequence.
// Lock should be held.
func (b *fastBatch) sendFlowControl(batchSeq uint64, mset *stream, reply string) {
	if len(reply) == 0 {
		return
	}
	response, _ := BatchFlowAck{Sequence: batchSeq, Messages: b.ackMessages}.MarshalJSON()
	mset.outq.sendMsg(reply, response)
}

// fastBatchCommit ends the batch and commits the data up to that point. If all messages
// have already been persisted, a PubAck is sent immediately. Otherwise, it will be sent
// after the last message has been persisted.
// Lock should be held.
func (batches *batching) fastBatchCommit(b *fastBatch, batchId string, mset *stream, reply string) bool {
	// Either we commit now, or we clean up later, so stop the timer.
	if b.timer == nil || (!b.commit && !b.timer.Stop()) {
		// Shouldn't be possible for the timer to already be stopped if we haven't committed yet,
		// since we pre-check being able to reset the timer. But guard against it anyhow.
		return true
	}
	// Mark that this batch commits.
	b.commit = true
	// If the whole batch has been persisted, we can respond with the PubAck now.
	if b.lseq == b.pseq {
		b.cleanupLocked(batchId, batches)
		var buf [256]byte
		pubAck := append(buf[:0], mset.pubAck...)
		response := append(pubAck, strconv.FormatUint(b.sseq, 10)...)
		response = append(response, fmt.Sprintf(",\"batch\":%q,\"count\":%d}", batchId, b.lseq)...)
		if len(reply) > 0 {
			mset.outq.sendMsg(reply, response)
		}
		return true
	}
	// Otherwise, we need to wait and the PubAck will be sent when the last message is persisted.
	return false
}

// setupCleanupTimer sets up a timer to clean up the batch after a timeout.
func (b *fastBatch) setupCleanupTimer(mset *stream, batchId string, batches *batching) {
	// Create a timer to clean up after timeout.
	timeout := getCleanupTimeout(mset)
	b.timer = time.AfterFunc(timeout, func() {
		b.cleanup(batchId, batches)
	})
}

// resetCleanupTimer resets the cleanup timer, allowing to extend the lifetime of the batch.
// Returns whether the timer was reset without it having expired before.
func (b *fastBatch) resetCleanupTimer(mset *stream) bool {
	if b.commit {
		return true
	}
	if b.timer == nil {
		return false
	}
	timeout := getCleanupTimeout(mset)
	return b.timer.Reset(timeout)
}

// cleanup deletes underlying resources associated with the batch and unregisters it from the stream's batches.
func (b *fastBatch) cleanup(batchId string, batches *batching) {
	batches.mu.Lock()
	defer batches.mu.Unlock()
	b.cleanupLocked(batchId, batches)
}

// Lock should be held.
func (b *fastBatch) cleanupLocked(batchId string, batches *batching) {
	// If the timer is nil, it means this batch has been replaced with a new one.
	// This can happen on a follower depending on timing.
	if b.timer == nil {
		return
	}
	globalInflightFastBatches.Add(-1)
	b.timer.Stop()
	delete(batches.fast, batchId)
	// Reset so that another invocation doesn't double-account.
	b.timer = nil
}

// getCleanupTimeout returns the timeout for the batch, taking into account the server's limits.
func getCleanupTimeout(mset *stream) time.Duration {
	timeout := streamMaxBatchTimeout
	if maxBatchTimeout := mset.srv.getOpts().JetStreamLimits.MaxBatchTimeout; maxBatchTimeout > 0 {
		timeout = maxBatchTimeout
	}
	return timeout
}

// batchStagedDiff stages all changes for consistency checks until commit.
type batchStagedDiff struct {
	msgIds             map[string]struct{}
	counter            map[string]*msgCounterRunningTotal
	inflight           map[string]*inflightSubjectRunningTotal
	inflightTransform  map[uint64]string
	expectedPerSubject map[string]*batchExpectedPerSubject
}

type batchExpectedPerSubject struct {
	sseq  uint64 // Stream sequence.
	clseq uint64 // Clustered proposal sequence.
}

// copyCounterSources returns a deep copy of the given counter sources.
func copyCounterSources(src CounterSources) CounterSources {
	if src == nil {
		return nil
	}
	dst := make(CounterSources, len(src))
	for stream, subjects := range src {
		dst[stream] = maps.Clone(subjects)
	}
	return dst
}

func (diff *batchStagedDiff) commit(mset *stream) {
	if len(diff.msgIds) > 0 {
		ts := time.Now().UnixNano()
		mset.ddMu.Lock()
		for msgId := range diff.msgIds {
			// We stage with zero, and will update in processJetStreamMsg once we know the sequence.
			mset.storeMsgIdLocked(&ddentry{msgId, 0, ts})
		}
		mset.ddMu.Unlock()
	}

	// Store running totals for counters, we could have multiple counter increments proposed, but not applied yet.
	if len(diff.counter) > 0 {
		if mset.clusteredCounterTotal == nil {
			mset.clusteredCounterTotal = make(map[string]*msgCounterRunningTotal, len(diff.counter))
		}
		for k, c := range diff.counter {
			mset.clusteredCounterTotal[k] = c
		}
	}

	// Track inflight.
	if len(diff.inflight) > 0 {
		if mset.inflight == nil {
			mset.inflight = make(map[string]*inflightSubjectRunningTotal, len(diff.inflight))
		}
		for subj, i := range diff.inflight {
			if c, ok := mset.inflight[subj]; ok {
				c.bytes += i.bytes
				c.ops += i.ops
				c.schedule = i.schedule
			} else {
				mset.inflight[subj] = i
			}
		}
	}

	// Track inflight subject transforms.
	if len(diff.inflightTransform) > 0 {
		if mset.inflightTransform == nil {
			mset.inflightTransform = make(map[uint64]string, len(diff.inflightTransform))
		}
		for clseq, subj := range diff.inflightTransform {
			mset.inflightTransform[clseq] = subj
		}
	}

	// Track sequence and subject.
	if len(diff.expectedPerSubject) > 0 {
		if mset.expectedPerSubjectSequence == nil {
			mset.expectedPerSubjectSequence = make(map[uint64]string, len(diff.expectedPerSubject))
		}
		if mset.expectedPerSubjectInProcess == nil {
			mset.expectedPerSubjectInProcess = make(map[string]struct{}, len(diff.expectedPerSubject))
		}
		for subj, e := range diff.expectedPerSubject {
			mset.expectedPerSubjectSequence[e.clseq] = subj
			mset.expectedPerSubjectInProcess[subj] = struct{}{}
		}
	}
}

type batchApply struct {
	id         string            // ID of the current batch.
	count      uint64            // Number of entries in the batch, for consistency checks.
	entries    []*CommittedEntry // Previous entries that are part of this batch.
	entryStart int               // The index into an entry indicating the first message of the batch.
	maxApplied uint64            // Applied value before the entry containing the first message of the batch.
}

// clearBatchState clears in-memory apply-batch-related state.
func (batch *batchApply) clearBatchState() {
	batch.id = _EMPTY_
	batch.count = 0
	batch.entries = nil
	batch.entryStart = 0
	batch.maxApplied = 0
}

// rejectBatchStateLocked rejects the batch and clears in-memory apply-batch-related state.
// Corrects mset.clfs to take the failed batch into account.
func (batch *batchApply) rejectBatchState(mset *stream) {
	mset.clMu.Lock()
	mset.clfs += batch.count
	mset.clMu.Unlock()
	// We're rejecting the batch, so all entries need to be returned to the pool.
	for _, bce := range batch.entries {
		bce.ReturnToPool()
	}
	batch.clearBatchState()
}

// checkMsgHeadersPreClusteredProposal checks the message for expected/consistency headers.
// mset.mu lock must NOT be held or used.
// mset.clMu lock must be held.
func checkMsgHeadersPreClusteredProposal(
	diff *batchStagedDiff, mset *stream, subject, rsubject string, hdr []byte, msg []byte, sourced bool, name string,
	jsa *jsAccount, allowRollup, denyPurge, allowTTL, allowMsgCounter, allowMsgSchedules bool,
	discard DiscardPolicy, discardNewPer bool, maxMsgSize int, maxMsgs int64, maxMsgsPer int64, maxBytes int64,
) ([]byte, []byte, uint64, *ApiError, error) {
	var incr *big.Int
	var hasSchedule bool

	// Do this before staging any proposal state. All clustered publish paths,
	// including atomic and fast batches, use this helper.
	if mset.store.Type() == FileStorage && isFileStoreMsgTooLarge(fileStoreMsgSize(subject, hdr, msg)) {
		return hdr, msg, 0, NewJSStreamStoreFailedError(ErrMsgTooLarge), ErrMsgTooLarge
	}

	// Some header checks must be checked pre proposal.
	if len(hdr) > 0 {
		// Since we encode header len as u16 make sure we do not exceed.
		// Again this works if it goes through but better to be pre-emptive.
		if len(hdr) > math.MaxUint16 {
			err := fmt.Errorf("JetStream header size exceeds limits for '%s > %s'", jsa.acc().Name, name)
			return hdr, msg, 0, NewJSStreamHeaderExceedsMaximumError(), err
		}
		// Counter increments.
		// Only supported on counter streams, and payload must be empty (if not coming from a source).
		var ok bool
		if incr, ok = getMessageIncr(hdr); !ok {
			apiErr := NewJSMessageIncrInvalidError()
			return hdr, msg, 0, apiErr, apiErr
		} else if incr != nil && !sourced {
			// Only do checks if the message isn't sourced. Otherwise, we need to store verbatim.
			if !allowMsgCounter {
				apiErr := NewJSMessageIncrDisabledError()
				return hdr, msg, 0, apiErr, apiErr
			} else if len(msg) > 0 {
				apiErr := NewJSMessageIncrPayloadError()
				return hdr, msg, 0, apiErr, apiErr
			} else {
				// Check for incompatible headers.
				var doErr bool
				if getRollup(hdr) != _EMPTY_ ||
					getExpectedStream(hdr) != _EMPTY_ ||
					getExpectedLastMsgId(hdr) != _EMPTY_ ||
					getExpectedLastSeqPerSubjectForSubject(hdr) != _EMPTY_ {
					doErr = true
				} else if _, ok = getExpectedLastSeq(hdr); ok {
					doErr = true
				} else if _, ok = getExpectedLastSeqPerSubject(hdr); ok {
					doErr = true
				}

				if doErr {
					apiErr := NewJSMessageIncrInvalidError()
					return hdr, msg, 0, apiErr, apiErr
				}
			}
		}
		// Expected stream name can also be pre-checked.
		if sname := getExpectedStream(hdr); sname != _EMPTY_ && sname != name {
			return hdr, msg, 0, NewJSStreamNotMatchError(), errStreamMismatch
		}
		// TTL'd messages are rejected entirely if TTLs are not enabled on the stream, or if the TTL is invalid.
		if ttl, err := getMessageTTL(hdr); !sourced && (ttl != 0 || err != nil) {
			if !allowTTL {
				return hdr, msg, 0, NewJSMessageTTLDisabledError(), errMsgTTLDisabled
			} else if err != nil {
				return hdr, msg, 0, NewJSMessageTTLInvalidError(), err
			}
		}
		// Check for MsgIds here at the cluster level to avoid excessive CLFS accounting.
		// Will help during restarts.
		if msgId := getMsgId(hdr); msgId != _EMPTY_ {
			// Dedupe if staged.
			if _, ok = diff.msgIds[msgId]; ok {
				return hdr, msg, 0, NewJSAtomicPublishContainsDuplicateMessageError(), errMsgIdDuplicate
			}
			mset.ddMu.Lock()
			if dde := mset.checkMsgId(msgId); dde != nil {
				seq := dde.seq
				mset.ddMu.Unlock()
				// Should not return an invalid sequence, in that case error.
				if seq > 0 {
					return hdr, msg, seq, NewJSAtomicPublishContainsDuplicateMessageError(), errMsgIdDuplicate
				} else {
					return hdr, msg, 0, NewJSStreamDuplicateMessageConflictError(), errMsgIdDuplicate
				}
			}
			if diff.msgIds == nil {
				diff.msgIds = map[string]struct{}{msgId: {}}
			} else {
				diff.msgIds[msgId] = struct{}{}
			}
			mset.ddMu.Unlock()
		}

		// Non-sourced messages aren't allowed to have the stream source header.
		if !sourced && len(sliceHeader(JSStreamSource, hdr)) > 0 {
			apiErr := NewJSMessageSourceHdrNotAllowedError()
			return hdr, msg, 0, apiErr, apiErr
		}
	}

	// Apply increment for counter.
	// But only if it's allowed for this stream. This can happen when we store verbatim for a sourced stream.
	if incr == nil && allowMsgCounter {
		apiErr := NewJSMessageIncrMissingError()
		return hdr, msg, 0, apiErr, apiErr
	}
	if incr != nil && allowMsgCounter {
		var initial big.Int
		var sources CounterSources

		// If we've got a running total, update that, since we have inflight proposals updating the same counter.
		var ok bool
		var counter *msgCounterRunningTotal
		if counter, ok = diff.counter[subject]; ok {
			initial = *counter.total
			sources = counter.sources
		} else if counter, ok = mset.clusteredCounterTotal[subject]; ok {
			// Make an explicit copy to separate the staged data from what's committed.
			initial.Set(counter.total)
			sources = copyCounterSources(counter.sources)
			// Don't need to initialize all values, they'll be overwritten later.
			counter = &msgCounterRunningTotal{ops: counter.ops}
		} else {
			// Load last message, and store as inflight running total.
			var smv StoreMsg
			sm, err := mset.store.LoadLastMsg(subject, &smv)
			if err == nil && sm != nil {
				var val CounterValue
				// Return an error if the counter is broken somehow.
				if json.Unmarshal(sm.msg, &val) != nil {
					apiErr := NewJSMessageCounterBrokenError()
					return hdr, msg, 0, apiErr, apiErr
				}
				if ncs := sliceHeader(JSMessageCounterSources, sm.hdr); len(ncs) > 0 {
					if err := json.Unmarshal(ncs, &sources); err != nil {
						apiErr := NewJSMessageCounterBrokenError()
						return hdr, msg, 0, apiErr, apiErr
					}
				}
				initial.SetString(val.Value, 10)
			}
		}
		srchdr := sliceHeader(JSStreamSource, hdr)
		if len(srchdr) > 0 {
			// This is a sourced message, so we can't apply Nats-Incr but
			// instead should just update the source count header.
			fields := strings.Split(string(srchdr), " ")
			origStream := fields[0]
			origSubj := subject
			if len(fields) >= 5 {
				origSubj = fields[4]
			}
			var val CounterValue
			if json.Unmarshal(msg, &val) != nil {
				apiErr := NewJSMessageCounterBrokenError()
				return hdr, msg, 0, apiErr, apiErr
			}
			var sourced big.Int
			sourced.SetString(val.Value, 10)
			if sources == nil {
				sources = map[string]map[string]string{}
			}
			if sources[origStream] == nil {
				sources[origStream] = map[string]string{}
			}
			prevVal := sources[origStream][origSubj]
			sources[origStream][origSubj] = sourced.String()
			// We will also replace the Nats-Incr header with the diff
			// between our last value from this source and this one, so
			// that the arithmetic is always correct.
			var previous big.Int
			previous.SetString(prevVal, 10)
			incr.Sub(&sourced, &previous)
			hdr = setHeader(JSMessageIncr, incr.String(), hdr)
		}
		// Now make the change.
		initial.Add(&initial, incr)
		// Generate the new payload.
		var _msg [128]byte
		msg = fmt.Appendf(_msg[:0], "{%q:%q}", "val", initial.String())
		// Write the updated source count headers.
		if len(sources) > 0 {
			nhdr, err := json.Marshal(sources)
			if err != nil {
				return hdr, msg, 0, NewJSMessageCounterBrokenError(), err
			}
			hdr = setHeader(JSMessageCounterSources, string(nhdr), hdr)
		}

		// Check to see if we are over the max msg size.
		maxSize := int64(mset.srv.getOpts().MaxPayload)
		if maxMsgSize >= 0 && int64(maxMsgSize) < maxSize {
			maxSize = int64(maxMsgSize)
		}
		hdrLen, msgLen := int64(len(hdr)), int64(len(msg))
		// Subtract to prevent against overflows.
		if hdrLen > maxSize || msgLen > maxSize-hdrLen {
			return hdr, msg, 0, NewJSStreamMessageExceedsMaximumError(), ErrMaxPayload
		}

		// Keep the in-memory counters up-to-date.
		if counter == nil {
			counter = &msgCounterRunningTotal{}
		}
		counter.total = &initial
		counter.sources = sources
		counter.ops++
		if diff.counter == nil {
			diff.counter = map[string]*msgCounterRunningTotal{subject: counter}
		} else {
			diff.counter[subject] = counter
		}
	}

	if len(hdr) > 0 {
		// Expected last sequence.
		if seq, exists := getExpectedLastSeq(hdr); exists && seq != mset.clseq-mset.clfs {
			mlseq := mset.clseq - mset.clfs
			err := fmt.Errorf("last sequence mismatch: %d vs %d", seq, mlseq)
			return hdr, msg, 0, NewJSStreamWrongLastSequenceError(mlseq), err
		} else if exists && len(diff.inflight) > 0 {
			// Only the first message in a batch can contain an expected last sequence.
			err := fmt.Errorf("last sequence mismatch")
			return hdr, msg, 0, NewJSStreamWrongLastSequenceConstantError(), err
		}

		// Expected last sequence per subject.
		if seq, exists := getExpectedLastSeqPerSubject(hdr); exists {
			// Allow override of the subject used for the check.
			seqSubj := subject
			if optSubj := getExpectedLastSeqPerSubjectForSubject(hdr); optSubj != _EMPTY_ {
				seqSubj = copyString(optSubj)
			}

			// The subject is already written to in this batch, we can't allow
			// expected checks since they would be incorrect.
			if _, ok := diff.inflight[seqSubj]; ok {
				err := errors.New("last sequence by subject mismatch")
				return hdr, msg, 0, NewJSStreamWrongLastSequenceConstantError(), err
			}

			// If the subject is already in process, block as otherwise we could have
			// multiple messages inflight with the same subject.
			if _, found := mset.expectedPerSubjectInProcess[seqSubj]; found {
				err := errors.New("last sequence by subject mismatch")
				return hdr, msg, 0, NewJSStreamWrongLastSequenceConstantError(), err
			}

			// If the subject is already in process but without expected headers, block as we would have
			// multiple messages inflight with the same subject.
			if _, ok := mset.inflight[seqSubj]; ok {
				err := errors.New("last sequence by subject mismatch")
				return hdr, msg, 0, NewJSStreamWrongLastSequenceConstantError(), err
			}

			// If we've already done an expected-check on this subject, use the cached result.
			if e, ok := diff.expectedPerSubject[seqSubj]; ok {
				if e.sseq != seq {
					err := fmt.Errorf("last sequence by subject mismatch: %d vs %d", seq, e.sseq)
					return hdr, msg, 0, NewJSStreamWrongLastSequenceError(e.sseq), err
				}
				e.clseq = mset.clseq
			} else {
				fseq, err := mset.store.LoadLastSeq(seqSubj)
				if err == ErrStoreMsgNotFound && seq == 0 {
					fseq, err = 0, nil
				}
				if err != nil || fseq != seq {
					err = fmt.Errorf("last sequence by subject mismatch: %d vs %d", seq, fseq)
					return hdr, msg, 0, NewJSStreamWrongLastSequenceError(fseq), err
				}

				e = &batchExpectedPerSubject{sseq: fseq, clseq: mset.clseq}
				if diff.expectedPerSubject == nil {
					diff.expectedPerSubject = map[string]*batchExpectedPerSubject{seqSubj: e}
				} else {
					diff.expectedPerSubject[seqSubj] = e
				}
			}
		} else if getExpectedLastSeqPerSubjectForSubject(hdr) != _EMPTY_ {
			apiErr := NewJSStreamExpectedLastSeqPerSubjectInvalidError()
			return hdr, msg, 0, apiErr, apiErr
		}

		// Message scheduling.
		if sourced {
			// noop, sourced messages were already validated by the origin stream.
		} else if schedule, apiErr := getMessageSchedule(hdr); apiErr != nil {
			if !allowMsgSchedules {
				apiErr = NewJSMessageSchedulesDisabledError()
			}
			return hdr, msg, 0, apiErr, apiErr
		} else if !schedule.IsZero() {
			hasSchedule = true
			if !allowMsgSchedules {
				apiErr := NewJSMessageSchedulesDisabledError()
				return hdr, msg, 0, apiErr, apiErr
			} else if scheduleTtl, ok := getMessageScheduleTTL(hdr); !ok {
				apiErr := NewJSMessageSchedulesTTLInvalidError()
				return hdr, msg, 0, apiErr, apiErr
			} else if scheduleRollup := getMessageScheduleRollup(hdr); scheduleRollup != _EMPTY_ && scheduleRollup != JSMsgRollupSubject {
				apiErr := NewJSMessageSchedulesRollupInvalidError()
				return hdr, msg, 0, apiErr, apiErr
			} else if scheduleTtl != _EMPTY_ && !allowTTL {
				return hdr, msg, 0, NewJSMessageTTLDisabledError(), errMsgTTLDisabled
			} else if scheduleTarget := getMessageScheduleTarget(hdr); scheduleTarget == _EMPTY_ ||
				!IsValidPublishSubject(scheduleTarget) || scheduleTarget == subject {
				apiErr := NewJSMessageSchedulesTargetInvalidError()
				return hdr, msg, 0, apiErr, apiErr
			} else if scheduleSource := getMessageScheduleSource(hdr); scheduleSource != _EMPTY_ &&
				(scheduleSource == scheduleTarget || scheduleSource == subject || !IsValidPublishSubject(scheduleSource)) {
				apiErr := NewJSMessageSchedulesSourceInvalidError()
				return hdr, msg, 0, apiErr, apiErr
			} else {
				mset.cfgMu.RLock()
				match := slices.ContainsFunc(mset.cfg.Subjects, func(subj string) bool {
					return SubjectsCollide(subj, scheduleTarget)
				})
				if !match {
					mset.cfgMu.RUnlock()
					apiErr := NewJSMessageSchedulesTargetInvalidError()
					return hdr, msg, 0, apiErr, apiErr
				}
				if scheduleSource != _EMPTY_ {
					match = slices.ContainsFunc(mset.cfg.Subjects, func(subj string) bool {
						return SubjectsCollide(subj, scheduleSource)
					})
					if !match {
						mset.cfgMu.RUnlock()
						apiErr := NewJSMessageSchedulesSourceInvalidError()
						return hdr, msg, 0, apiErr, apiErr
					}
				}
				mset.cfgMu.RUnlock()

				// Add a rollup sub header if it doesn't already exist.
				// Otherwise, it must exist already as a rollup on the subject.
				if rollup := getRollup(hdr); rollup == _EMPTY_ {
					hdr = genHeader(hdr, JSMsgRollup, JSMsgRollupSubject)
				} else if rollup != JSMsgRollupSubject {
					apiErr := NewJSMessageSchedulesRollupInvalidError()
					return hdr, msg, 0, apiErr, apiErr
				}
			}
		}
		if scheduleNext := sliceHeader(JSScheduleNext, hdr); len(scheduleNext) > 0 && !sourced {
			// Clients may only use Nats-Schedule-Next to purge a schedule.
			if bytesToString(scheduleNext) != JSScheduleNextPurge {
				apiErr := NewJSMessageSchedulesSchedulerInvalidError()
				return hdr, msg, 0, apiErr, apiErr
			}
			// Nats-Scheduler must accompany the purge and:
			// - it must NOT be empty.
			// - it must NOT match the publish subject.
			if scheduler := sliceHeader(JSScheduler, hdr); len(scheduler) == 0 ||
				bytesToString(scheduler) == subject || !IsValidPublishSubject(bytesToString(scheduler)) {
				apiErr := NewJSMessageSchedulesSchedulerInvalidError()
				return hdr, msg, 0, apiErr, apiErr
			} else if !allowMsgSchedules {
				apiErr := NewJSMessageSchedulesDisabledError()
				return hdr, msg, 0, apiErr, apiErr
			} else {
				// Check that the to-be-purged subject is a schedule message.
				// We still allow this message through if there exists no message for this subject,
				// to remain backward-compatible. An "expected at sequence" check can still be
				// performed to make this stricter.
				schedSubj := bytesToString(scheduler)
				var invalid bool
				if i, ok := diff.inflight[schedSubj]; ok {
					invalid = !i.schedule
				} else if i, ok = mset.inflight[schedSubj]; ok {
					invalid = !i.schedule
				} else {
					var smv StoreMsg
					sm, _ := mset.store.LoadLastMsg(schedSubj, &smv)
					invalid = sm != nil && len(sliceHeader(JSSchedulePattern, sm.hdr)) == 0
				}
				if invalid {
					apiErr := NewJSMessageSchedulesSchedulerInvalidError()
					return hdr, msg, 0, apiErr, apiErr
				}
			}
		} else if !sourced && len(sliceHeader(JSScheduler, hdr)) > 0 {
			// Clients may only use Nats-Scheduler alongside Nats-Schedule-Next.
			apiErr := NewJSMessageSchedulesSchedulerInvalidError()
			return hdr, msg, 0, apiErr, apiErr
		}

		// Check for any rollups.
		if rollup := getRollup(hdr); rollup != _EMPTY_ {
			if (!allowRollup || denyPurge) && !sourced {
				err := errors.New("rollup not permitted")
				return hdr, msg, 0, NewJSStreamRollupFailedError(err), err
			}
			switch rollup {
			case JSMsgRollupSubject:
				// Rolling up the subject is only allowed if the first occurrence of this subject in the batch.
				if _, ok := diff.inflight[subject]; ok {
					err := errors.New("batch rollup sub invalid")
					return hdr, msg, 0, NewJSStreamRollupFailedError(err), err
				}
			case JSMsgRollupAll:
				// Rolling up the whole stream is only allowed if this is the first message of the batch.
				if len(diff.inflight) > 0 {
					err := errors.New("batch rollup all invalid")
					return hdr, msg, 0, NewJSStreamRollupFailedError(err), err
				}
			default:
				err := fmt.Errorf("rollup value invalid: %q", rollup)
				return hdr, msg, 0, NewJSStreamRollupFailedError(err), err
			}
		}
	}

	// Track inflight.
	// Store the subject to ensure other messages in this batch using
	// an expected check or rollup on the same subject fail.
	if diff.inflight == nil {
		diff.inflight = make(map[string]*inflightSubjectRunningTotal, 1)
	}
	var sz uint64
	if mset.store.Type() == FileStorage {
		sz = fileStoreMsgSizeRaw(len(subject), len(hdr), len(msg))
	} else {
		sz = memStoreMsgSizeRaw(len(subject), len(hdr), len(msg))
	}
	var (
		i   *inflightSubjectRunningTotal
		ok  bool
		err error
	)
	if i, ok = diff.inflight[subject]; ok {
		i.bytes += sz
		i.ops++
		i.schedule = hasSchedule
	} else {
		i = &inflightSubjectRunningTotal{bytes: sz, ops: 1, schedule: hasSchedule}
		diff.inflight[subject] = i
	}

	// Subject transform.
	if subject != rsubject {
		// The 'subject' is a transformed subject used for consistency checks.
		// But since we propose the original (raw) subject to our peers, we need
		// to store the transformed subject separately for when we apply.
		// TODO(mvv): since subject transforms are handled by each replica individually, this has a
		//  potential for desync given out-of-order stream subject transform updates.
		if diff.inflightTransform == nil {
			diff.inflightTransform = make(map[uint64]string, 1)
		}
		diff.inflightTransform[mset.clseq] = subject
	}

	// Check if we have discard new with max msgs or bytes.
	// We need to deny here otherwise we'd need to bump CLFS, and it could succeed on some
	// peers and not others depending on consumer ack state (if interest policy).
	// So we deny here, if we allow that means we know it would succeed on every peer.
	if discard == DiscardNew {
		if maxMsgs > 0 || maxBytes > 0 {
			// Track usual max msgs/bytes thresholds for DiscardNew.
			var state StreamState
			mset.store.FastState(&state)

			totalMsgs := state.Msgs
			totalBytes := state.Bytes
			for _, i = range mset.inflight {
				totalMsgs += i.ops
				totalBytes += i.bytes
			}
			for _, i = range diff.inflight {
				totalMsgs += i.ops
				totalBytes += i.bytes
			}

			if maxMsgs > 0 && totalMsgs > uint64(maxMsgs) {
				err = ErrMaxMsgs
			} else if maxBytes > 0 && totalBytes > uint64(maxBytes) {
				err = ErrMaxBytes
			}
			if err != nil {
				return hdr, msg, 0, NewJSStreamStoreFailedError(err, Unless(err)), err
			}
		}

		// Similarly, check DiscardNew per-subject threshold to not need to bump CLFS.
		// Allow rollup messages through since they will purge after storing.
		if discardNewPer && maxMsgsPer > 0 && len(sliceHeader(JSMsgRollup, hdr)) == 0 {
			// Get the current total for this subject.
			totalMsgsForSubject := mset.store.SubjectsTotals(subject)[subject]
			// Add inflight count in this batch and for this stream.
			totalMsgsForSubject += i.ops
			if i, ok = mset.inflight[subject]; ok {
				totalMsgsForSubject += i.ops
			}
			if totalMsgsForSubject > uint64(maxMsgsPer) {
				err = ErrMaxMsgsPerSubject
				return hdr, msg, 0, NewJSStreamStoreFailedError(err, Unless(err)), err
			}
		}
	}

	return hdr, msg, 0, nil, nil
}

// recalculateClusteredSeq initializes or updates mset.clseq, for example after a leader change.
// This is reused for normal clustered publishing into a stream, and for atomic and fast batch publishing.
// mset.clMu lock must be held.
func recalculateClusteredSeq(mset *stream, needStreamLock bool) (lseq uint64) {
	// Need to unlock and re-acquire the locks in the proper order.
	// Locking order is stream -> clMu
	if needStreamLock {
		mset.clMu.Unlock()
		mset.mu.RLock()
		mset.clMu.Lock()
	}
	// Re-capture
	lseq = mset.lseq
	mset.clseq = lseq + mset.clfs
	// Keep hold of the mset.clMu, but unlock the others.
	if needStreamLock {
		mset.mu.RUnlock()
	}
	return lseq
}

// commitSingleMsg commits and proposes a single message to the node.
// This is reused both for normal publishing into a stream, and for fast batch publishing.
// mset.clMu lock must be held.
func commitSingleMsg(
	diff *batchStagedDiff, mset *stream, subject string, reply string, hdr []byte, msg []byte, name string,
	jsa *jsAccount, mt *msgTrace, node RaftNode, term uint64, replicas int, lseq uint64,
) error {
	// Do proposal.
	esm := encodeStreamMsgAllowCompress(subject, reply, hdr, msg, mset.clseq, time.Now().UnixNano(), false)
	if err := node.Propose(term, esm); err != nil {
		return err
	}

	var mtKey uint64
	if mt != nil {
		mtKey = mset.clseq
		if mset.mt == nil {
			mset.mt = make(map[uint64]*msgTrace)
		}
		mset.mt[mtKey] = mt
	}

	diff.commit(mset)
	mset.clseq++
	mset.trackReplicationTraffic(node, len(esm), replicas)

	// Check to see if we are being overrun.
	// TODO(dlc) - Make this a limit where we drop messages to protect ourselves, but allow to be configured.
	if mset.clseq-(lseq+mset.clfs) > streamLagWarnThreshold {
		lerr := fmt.Errorf("JetStream stream '%s > %s' has high message lag", jsa.acc().Name, name)
		mset.srv.RateLimitWarnf("%s", lerr.Error())
	}
	return nil
}
