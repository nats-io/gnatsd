// Copyright 2026 The NATS Authors
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
	"container/heap"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"net"
	"path"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/klauspost/compress/s2"
	"github.com/nats-io/nats-server/v2/server/archive"
)

type SnapshotConsumerState struct {
	*ConsumerConfig `json:"config"`
	*ConsumerState  `json:"state"`
}

// Create a snapshot of this stream and its consumer's state along with messages.
// sa is passed in when the stream is clustered, so we can find child consumer assignments.
func (js *jetStream) CreateStreamSnapshotV2(store StreamStore, deadline time.Duration, includeConsumers bool, sa *streamAssignment) (*SnapshotResult, error) {
	pr, pw := net.Pipe()

	// Set a write deadline here to protect ourselves.
	if deadline > 0 {
		pw.SetWriteDeadline(time.Now().Add(deadline))
	}

	// The stream keeps taking messages while the snapshot reads it, and its limits,
	// TTLs, deletes and rollups keep removing them. A store that can hold messages
	// for a snapshot keeps a copy of each one removed before the snapshot reads it,
	// so the snapshot holds the stream exactly as this state describes it.
	var state StreamState
	var hold *snapshotHold
	if hs, ok := store.(snapshotHolder); ok {
		hold = hs.holdForSnapshot(&state, &js.snapHolds)
	} else {
		store.FastState(&state)
	}

	// Stream in separate Go routine.
	errCh := make(chan error, 1)
	go js.streamSnapshotV2(store, &state, hold, pw, includeConsumers, sa, errCh)

	return &SnapshotResult{pr, state, errCh}, nil
}

// Stream our snapshot through S2 compression and the custom archive format.
func (js *jetStream) streamSnapshotV2(store StreamStore, state *StreamState, hold *snapshotHold, w io.WriteCloser, includeConsumers bool, sa *streamAssignment, errCh chan error) {
	defer close(errCh)
	defer w.Close()
	if hold != nil {
		defer func() {
			store.(snapshotHolder).releaseSnapshotHold(hold)
			hold.release()
		}()
	}

	enc := s2.NewWriter(w)
	defer enc.Close()

	tw := archive.NewWriter(enc)
	defer tw.Close()

	now := time.Now()
	clustered := js.isClustered()

	writeGeneric := func(name string, mod int64, seq uint64, headerSize, payloadSize int64, buf []byte) error {
		hdr := &archive.Header{
			Name:        name,
			Timestamp:   mod,
			Sequence:    seq,
			HeaderSize:  headerSize,
			PayloadSize: payloadSize,
		}
		if err := tw.WriteHeader(hdr); err != nil {
			return err
		}
		_, err := tw.Write(buf)
		return err
	}

	writeStoreMsg := func(msg *StoreMsg) error {
		return writeGeneric(
			msg.subj,
			msg.ts,
			msg.seq,
			int64(len(msg.hdr)),
			int64(len(msg.msg)),
			msg.buf,
		)
	}

	writeConsumerMsg := func(scs SnapshotConsumerState) error {
		// Bound the consumer state to the stream snapshot. Consumer sequence
		// numbers are left intact since filtered consumers do not have a
		// one-to-one mapping between consumer and stream sequences.
		scs.Delivered.Stream = min(scs.Delivered.Stream, state.LastSeq)
		scs.AckFloor.Stream = min(scs.AckFloor.Stream, state.LastSeq)
		for seq := range scs.Pending {
			if seq > state.LastSeq {
				delete(scs.Pending, seq)
			}
		}
		for seq := range scs.Redelivered {
			if seq > state.LastSeq {
				delete(scs.Redelivered, seq)
			}
		}
		ssj, err := json.Marshal(scs)
		if err != nil {
			return err
		}
		return writeGeneric(
			path.Join("consumers", scs.Name),
			now.UnixNano(),
			0,
			0,
			int64(len(ssj)),
			ssj,
		)
	}

	// If we aren't including consumers here then make sure the consumer count
	// is set accordingly, this helps on the restore path.
	var consumerAssignments map[string]*consumerAssignment
	var consumerStores []ConsumerStore
	var streamState = *state
	if !includeConsumers {
		streamState.Consumers = 0
	} else if clustered {
		if sa == nil {
			errCh <- errors.New("stream assignment not present in clustered mode")
			return
		}
		js.mu.RLock()
		consumerAssignments = make(map[string]*consumerAssignment, len(sa.consumers))
		for name, ca := range sa.consumers {
			consumerAssignments[name] = ca.copyGroup()
		}
		streamState.Consumers = len(consumerAssignments)
		js.mu.RUnlock()
	} else {
		consumerStores = slices.Collect(store.Consumers())
		streamState.Consumers = len(consumerStores)
	}

	ssj, err := json.Marshal(streamState)
	if err != nil {
		errCh <- err
		return
	}
	if err := writeGeneric("state.json", now.UnixNano(), 0, 0, int64(len(ssj)), ssj); err != nil {
		errCh <- err
		return
	}

	// Do consumers first, if the stream is interest/WQ then this may be
	// important for message retention.
	if includeConsumers {
		consumerStateFromInfo := func(ci *ConsumerInfo) *ConsumerState {
			state := &ConsumerState{
				Delivered: SequencePair{
					Consumer: ci.Delivered.Consumer,
					Stream:   ci.Delivered.Stream,
				},
				AckFloor: SequencePair{
					Consumer: ci.AckFloor.Consumer,
					Stream:   ci.AckFloor.Stream,
				},
			}
			if ci.NumAckPending > 0 {
				// Cluster consumer info does not include the sparse pending state.
				// Roll back to the ack floor so all possibly unacknowledged messages
				// are delivered again instead of guessing which ones are pending.
				state.Delivered = state.AckFloor
			}
			return state
		}

		if clustered {
			for _, ca := range consumerAssignments {
				ci, err := sysRequest[ConsumerInfo](js.srv, clusterConsumerInfoT, sa.Client.serviceAccount(), sa.Config.Name, ca.Name)
				if err != nil || ci == nil {
					errCh <- fmt.Errorf("failed to get consumer state for '%s > %s'", sa.Config.Name, ca.Name)
					return
				}
				// Ephemeral clustered consumers are named by their assignment. Their
				// original config may not carry that generated name, but snapshots
				// need it both for the archive entry and for restoration.
				config := *ca.Config
				config.Name = ca.Name
				if err := writeConsumerMsg(SnapshotConsumerState{
					ConsumerConfig: &config,
					ConsumerState:  consumerStateFromInfo(ci),
				}); err != nil {
					errCh <- err
					return
				}
			}
		} else {
			for _, o := range consumerStores {
				config := o.GetConfig()
				state, err := o.State()
				if err != nil {
					errCh <- fmt.Errorf("couldn't load consumer '%s' state: %s", config.Name, err)
					return
				}
				if err := writeConsumerMsg(SnapshotConsumerState{
					ConsumerConfig: config,
					ConsumerState:  state,
				}); err != nil {
					errCh <- err
					return
				}
			}
		}
	}

	// Write the messages the store removed, after the snapshot began, from before
	// the sequence upTo, which the store can no longer load.
	writeHeld := func(upTo uint64) error {
		if hold == nil {
			return nil
		}
		held, err := hold.through(upTo)
		if err != nil {
			return err
		}
		for _, hsm := range held {
			if err := writeStoreMsg(hsm); err != nil {
				return err
			}
		}
		return nil
	}

	var sm StoreMsg
	for seq := state.FirstSeq - 1; seq < state.LastSeq; {
		if _, seq, err = store.LoadNextMsg(fwcs, true, seq+1, &sm); err != nil {
			if err == ErrStoreEOF {
				break
			}
			errCh <- fmt.Errorf("couldn't load next message after seq %d: %s", seq+1, err)
			return
		}
		// Concurrent deletions and publishes can move the next available
		// message beyond the snapshot's original sequence range.
		if seq > state.LastSeq {
			break
		}
		if err = writeHeld(seq); err != nil {
			errCh <- err
			return
		}
		if err = writeStoreMsg(&sm); err != nil {
			errCh <- err
			return
		}
	}
	// And those removed after the last message the store still had.
	if state.LastSeq > 0 {
		if err = writeHeld(state.LastSeq + 1); err != nil {
			errCh <- err
			return
		}
	}

	// End of backup sentinel. A clear marker makes it obvious when
	// a backup has been truncated or not without having to count
	// messages or from first/last sequence, which may not be possible
	// during the rewrite of a large stream backup.
	if err = writeGeneric(_EMPTY_, 0, 0, 0, 0, nil); err != nil {
		errCh <- err
		return
	}

	// Make sure final flush error reporting is propagated, as the
	// close is in a defer.
	if err = tw.Flush(); err != nil {
		errCh <- err
	}
}

// A store that can hold, for a snapshot, the messages it removes while the
// snapshot reads it.
type snapshotHolder interface {
	// holdForSnapshot fills in state as FastState does and, at the same instant,
	// starts keeping a copy of each message up to state.LastSeq the store removes,
	// until releaseSnapshotHold, charging the copies to budget.
	holdForSnapshot(state *StreamState, budget *snapshotHoldBudget) *snapshotHold
	releaseSnapshotHold(h *snapshotHold)
}

// errSnapshotPurged fails a snapshot whose messages were purged, compacted or
// truncated away in bulk before it read them, which the store does not copy.
var errSnapshotPurged = errors.New("stream was purged, compacted or truncated while the snapshot read it")

// errSnapshotHoldLimit fails a snapshot when the copies of the messages removed
// before it read them would take the server's snapshot holds past their limit.
var errSnapshotHoldLimit = errors.New("too many messages were removed while the snapshot read the stream")

// The bytes of removed messages every snapshot of a server may hold at once.
const defaultMaxSnapshotHoldBytes = int64(64 * 1024 * 1024)

// snapshotHoldBudget bounds the bytes all of a server's snapshot holds keep,
// so that snapshots falling behind a stream's removals cannot take its memory.
type snapshotHoldBudget struct {
	used atomic.Int64
	max  int64
}

// snapshotHold keeps a copy of each message a store removes before the snapshot
// reading it gets to it, so that the snapshot holds the stream exactly as it was
// when it began. Without it, a message removed by a limit, a TTL, a delete or a
// rollup while the snapshot reads is in neither the snapshot nor, when a newer
// message removed it (a key-value update, MaxMsgsPerSubject), is its replacement,
// which lies past the snapshot's last sequence.
type snapshotHold struct {
	mu     sync.Mutex
	last   uint64              // The snapshot's last sequence.
	read   uint64              // The snapshot has read every message up to here.
	held   snapshotHeld        // Copies of the messages removed, by sequence.
	bytes  int64               // The bytes of held, charged to budget.
	budget *snapshotHoldBudget // The server's limit on what its holds keep.
	err    error               // Set when messages it needs were removed in bulk or past the budget.
}

func newSnapshotHold(state *StreamState, budget *snapshotHoldBudget) *snapshotHold {
	h := &snapshotHold{last: state.LastSeq, budget: budget}
	if state.Msgs == 0 {
		h.read = state.LastSeq
	} else {
		h.read = state.FirstSeq - 1
	}
	return h
}

func snapshotHeldSize(sm *StoreMsg) int64 {
	return int64(len(sm.subj) + len(sm.hdr) + len(sm.msg))
}

// keep copies sm when the snapshot has yet to read it.
// Called by the store, with its lock held, before it removes sm.
func (h *snapshotHold) keep(sm *StoreMsg) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if sm.seq <= h.read || sm.seq > h.last || h.err != nil {
		return
	}
	sz := snapshotHeldSize(sm)
	if h.budget != nil {
		if h.budget.used.Add(sz) > h.budget.max {
			h.budget.used.Add(-sz)
			h.failLocked(errSnapshotHoldLimit)
			return
		}
	}
	buf := make([]byte, len(sm.hdr)+len(sm.msg))
	copy(buf, sm.hdr)
	copy(buf[len(sm.hdr):], sm.msg)
	heap.Push(&h.held, &StoreMsg{
		subj: copyString(sm.subj),
		hdr:  buf[:len(sm.hdr):len(sm.hdr)],
		msg:  buf[len(sm.hdr):],
		buf:  buf,
		seq:  sm.seq,
		ts:   sm.ts,
	})
	h.bytes += sz
}

// lost records that the messages from first to last were removed without copies.
// Called by the store, with its lock held.
func (h *snapshotHold) lost(first, last uint64) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if first <= last && last > h.read && first <= h.last && h.err == nil {
		h.failLocked(errSnapshotPurged)
	}
}

// failLocked fails the snapshot with err and drops what it held, which it no
// longer needs.
// Lock should be held.
func (h *snapshotHold) failLocked(err error) {
	h.err = err
	h.releaseLocked()
}

// release drops what the hold still keeps and returns its bytes to the budget.
// Called once the store no longer gives the hold messages.
func (h *snapshotHold) release() {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.releaseLocked()
}

// Lock should be held.
func (h *snapshotHold) releaseLocked() {
	if h.budget != nil {
		h.budget.used.Add(-h.bytes)
	}
	h.held, h.bytes = nil, 0
}

// through returns, in sequence order, the messages held from before upTo that
// the snapshot has not read yet, and records it has read everything before upTo.
// Each held message is visited once, when it becomes due.
func (h *snapshotHold) through(upTo uint64) ([]*StoreMsg, error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.err != nil {
		return nil, h.err
	}
	var held []*StoreMsg
	for len(h.held) > 0 && h.held[0].seq <= upTo {
		sm := heap.Pop(&h.held).(*StoreMsg)
		sz := snapshotHeldSize(sm)
		h.bytes -= sz
		if h.budget != nil {
			h.budget.used.Add(-sz)
		}
		// The message at upTo itself was read from the store after all, and a
		// message removed twice is written once.
		if sm.seq < upTo && (len(held) == 0 || held[len(held)-1].seq != sm.seq) {
			held = append(held, sm)
		}
	}
	h.read = max(h.read, upTo)
	return held, nil
}

// snapshotHeld is a hold's copies of removed messages, a min-heap by sequence.
type snapshotHeld []*StoreMsg

func (sh snapshotHeld) Len() int           { return len(sh) }
func (sh snapshotHeld) Less(i, j int) bool { return sh[i].seq < sh[j].seq }
func (sh snapshotHeld) Swap(i, j int)      { sh[i], sh[j] = sh[j], sh[i] }
func (sh *snapshotHeld) Push(x any)        { *sh = append(*sh, x.(*StoreMsg)) }
func (sh *snapshotHeld) Pop() any {
	old, n := *sh, len(*sh)-1
	sm := old[n]
	old[n] = nil
	*sh = old[:n]
	return sm
}

// RestoreStreamSnapshotV2 will restore a stream from a snapshot.
func (a *Account) RestoreStreamV2(ncfg *StreamConfig, r io.Reader) (retMset *stream, retErr error) {
	dec := s2.NewReader(r)
	tr := archive.NewReader(dec)

	var nstate StreamState

	// Load the stream state.
	hdr, err := tr.Next()
	if err != nil {
		return nil, err
	}
	if hdr.Name != "state.json" {
		return nil, fmt.Errorf("expected state.json first")
	}
	if err := json.NewDecoder(tr).Decode(&nstate); err != nil {
		return nil, fmt.Errorf("error in state.json: %w", err)
	}
	if nstate.Consumers < 0 {
		return nil, fmt.Errorf("invalid stream state: negative consumer count")
	}
	if nstate.FirstSeq == 0 {
		if nstate.LastSeq != 0 || nstate.Msgs != 0 {
			return nil, fmt.Errorf("invalid stream state: inconsistent message count or sequence range")
		}
	} else if nstate.FirstSeq > nstate.LastSeq {
		// An empty, advanced stream has FirstSeq exactly one past LastSeq.
		// Express this by subtracting from FirstSeq so a corrupt LastSeq of
		// MaxUint64 can not overflow here.
		if nstate.FirstSeq-1 != nstate.LastSeq || nstate.Msgs != 0 {
			return nil, fmt.Errorf("invalid stream state: inconsistent message count or sequence range")
		}
	} else if nstate.Msgs > nstate.LastSeq-nstate.FirstSeq+1 {
		return nil, fmt.Errorf("invalid stream state: message count exceeds sequence range")
	}

	s, jsa, err := a.checkForJetStream()
	if err != nil {
		return nil, err
	}
	js := jsa.js
	if js == nil {
		return nil, NewJSNotEnabledForAccountError()
	}
	if _, err := a.lookupStream(ncfg.Name); err == nil {
		return nil, NewJSStreamNameExistRestoreFailedError()
	}

	cfg, apiErr := s.checkStreamCfg(ncfg, a, false)
	if apiErr != nil {
		return nil, apiErr
	}

	_, isClustered := jsa.jetStreamAndClustered()
	jsa.usageMu.RLock()
	selected, tier, hasTier := jsa.selectLimits(cfg.Replicas)
	jsa.usageMu.RUnlock()
	reserved := int64(0)
	if hasTier {
		if isClustered {
			js.mu.RLock()
			_, reserved = js.tieredStreamAndReservationCount(a.Name, tier, &cfg)
			js.mu.RUnlock()
		} else {
			reserved = jsa.tieredReservation(tier, &cfg)
		}
	}

	// Hold the full restore footprint as usage before creating the stream. As
	// messages are stored, this temporary reservation is converted into actual
	// usage so other streams cannot consume the capacity needed by the restore.
	restoreBytes := nstate.Bytes
	if restoreBytes > math.MaxInt64 {
		if cfg.Storage == MemoryStorage {
			return nil, NewJSMemoryResourcesExceededError()
		}
		return nil, NewJSStorageResourcesExceededError()
	}
	restoreRemaining := int64(restoreBytes)
	jsa.updateUsage(tier, cfg.Storage, restoreRemaining)
	defer func() {
		// Inside a defer func() so that restoreRemaining is captured later.
		jsa.updateUsage(tier, cfg.Storage, -restoreRemaining)
	}()
	releaseRestoreBytes := func(bytes int64) {
		bytes = min(bytes, restoreRemaining)
		jsa.updateUsage(tier, cfg.Storage, -bytes)
		restoreRemaining -= bytes
	}
	checkUsageLimits := func() error {
		lr := int64(max(1, cfg.Replicas))
		if tier == _EMPTY_ {
			lr = 1
		}
		jsa.usageMu.RLock()
		usage := jsa.usage[tier]
		var used, limit int64
		if cfg.Storage == MemoryStorage {
			limit = selected.MaxMemory
			if usage != nil {
				used = usage.total.mem
			}
		} else {
			limit = selected.MaxStore
			if usage != nil {
				used = usage.total.store
			}
		}
		jsa.usageMu.RUnlock()
		if limit >= 0 && used > mulSaturate(limit, lr) {
			if cfg.Storage == MemoryStorage {
				return NewJSMemoryResourcesExceededError()
			}
			return NewJSStorageResourcesExceededError()
		}
		if js.limitsExceeded(cfg.Storage) {
			if cfg.Storage == MemoryStorage {
				return NewJSMemoryResourcesExceededError()
			}
			return NewJSStorageResourcesExceededError()
		}
		return nil
	}

	reserveCfg := cfg
	reserveCfg.MaxBytes = max(reserveCfg.MaxBytes, restoreRemaining)
	js.mu.RLock()
	err = js.checkAllLimits(&selected, tier, &reserveCfg, reserved, 0)
	js.mu.RUnlock()
	if err != nil {
		return nil, err
	}
	if err := checkUsageLimits(); err != nil {
		return nil, err
	}

	mset, err := a.addStreamForRestore(&cfg)
	if err != nil {
		return nil, fmt.Errorf("error adding stream: %w", err)
	}
	defer func() {
		var state StreamState
		mset.store.FastState(&state)
		mset.mu.Lock()
		mset.lseq = state.LastSeq
		mset.mu.Unlock()
		if err := mset.completeRestore(); err != nil {
			if err = fmt.Errorf("failed to activate stream %q: %w", cfg.Name, err); retErr == nil {
				retErr = err
			}
			s.Warnf("JetStream stream restore for '%s > %s' failed to activate stream: %v", a.Name, cfg.Name, err)
		}
	}()

	// Start off at the right sequence number. This is important in particular
	// when the backup contains no messages or would restore to no interest.
	if _, err = mset.store.Compact(nstate.FirstSeq); err != nil {
		return nil, fmt.Errorf("error purging stream: %w", err)
	}

	var restoredConsumers, ephemerals []*consumer
	defer func() {
		// Consumers must be unconditionally converted and completed, otherwise
		// a partial restore that fails midway through can leave assets that are
		// unusable.
		for _, o := range ephemerals {
			o.switchToEphemeral()
		}
		for _, o := range restoredConsumers {
			if err := o.completeRestore(); err != nil {
				if err = fmt.Errorf("failed to activate consumer %q: %w", o.name, err); retErr == nil {
					retErr = err
				}
				s.Warnf("JetStream stream restore for '%s > %s' failed to activate consumers: %v", a.Name, cfg.Name, err)
			}
		}
	}()
	for range nstate.Consumers {
		hdr, err := tr.Next()
		if err != nil {
			return nil, err
		}
		name, found := strings.CutPrefix(hdr.Name, "consumers/")
		if !found {
			return nil, fmt.Errorf("expected consumer, found %q", hdr.Name)
		}
		var consumer SnapshotConsumerState
		if err := json.NewDecoder(tr).Decode(&consumer); err != nil {
			return nil, fmt.Errorf("failed to decode consumer %q state: %w", name, err)
		}
		if consumer.ConsumerConfig == nil {
			return nil, fmt.Errorf("consumer %q is missing config", name)
		}
		if consumer.ConsumerState == nil {
			return nil, fmt.Errorf("consumer %q is missing state", name)
		}
		isEphemeral := !isDurableConsumer(consumer.ConsumerConfig)
		if isEphemeral {
			// Keep ephemerals alive and interested until all messages have
			// been restored, then start their normal inactivity lifecycle.
			consumer.Durable = name
		}
		o, err := mset.addConsumerForRestore(consumer.ConsumerConfig)
		if err != nil {
			return nil, fmt.Errorf("failed to add consumer %q: %w", name, err)
		}
		if isEphemeral {
			ephemerals = append(ephemerals, o)
		}
		restoredConsumers = append(restoredConsumers, o)
		o.mu.Lock()
		err = o.setStoreState(consumer.ConsumerState)
		o.mu.Unlock()
		if err != nil {
			return nil, fmt.Errorf("failed to set consumer %q state: %w", name, err)
		}
	}

	store := mset.store
	lseq := nstate.FirstSeq - 1
	eob := false
	mp := int64(s.getOpts().MaxPayload)
	for {
		hdr, err := tr.Next()
		if err != nil {
			return nil, err
		}
		seq := hdr.Sequence
		if seq == 0 {
			// Sentinel "end of backup" if all fields are zero.
			if hdr.Name == _EMPTY_ && hdr.Timestamp == 0 && hdr.HeaderSize == 0 && hdr.PayloadSize == 0 {
				eob = true
				break
			}
			return nil, fmt.Errorf("expected message sequence")
		}
		if hdr.HeaderSize < 0 || hdr.PayloadSize < 0 || hdr.PayloadSize > math.MaxInt64-hdr.HeaderSize {
			return nil, fmt.Errorf("invalid message lengths for sequence %d", seq)
		}
		declaredSize := hdr.HeaderSize + hdr.PayloadSize
		if hdr.HeaderSize > mp || hdr.PayloadSize > mp-hdr.HeaderSize {
			return nil, fmt.Errorf("message sequence %d exceeds maximum payload size", seq)
		}
		if mms := int64(cfg.MaxMsgSize); mms >= 0 && (hdr.HeaderSize > mms || hdr.PayloadSize > mms-hdr.HeaderSize) {
			return nil, fmt.Errorf("message sequence %d exceeds maximum message size", seq)
		}
		var storedSizeRaw uint64
		switch cfg.Storage {
		case MemoryStorage:
			if storedSizeRaw = memStoreMsgSizeRaw(len(hdr.Name), int(hdr.HeaderSize), int(hdr.PayloadSize)); storedSizeRaw > math.MaxInt64 {
				return nil, fmt.Errorf("snapshot message bytes exceed maximum store message size")
			}
		default:
			if storedSizeRaw = fileStoreMsgSizeRaw(len(hdr.Name), int(hdr.HeaderSize), int(hdr.PayloadSize)); storedSizeRaw > rlBadThresh {
				return nil, fmt.Errorf("snapshot message bytes exceed maximum store message size")
			}
		}
		storedSize := int64(storedSizeRaw)
		if additional := storedSize - restoreRemaining; additional > 0 {
			jsa.updateUsage(tier, cfg.Storage, additional)
			restoreRemaining += additional
			if err := checkUsageLimits(); err != nil {
				return nil, err
			}
		}
		buf, err := io.ReadAll(io.LimitReader(tr, declaredSize))
		if err != nil {
			return nil, fmt.Errorf("failed to read message sequence %d: %w", seq, err)
		}
		if hdr.HeaderSize > int64(len(buf)) {
			return nil, fmt.Errorf("failed to parse message sequence %d: invalid header length", seq)
		}
		if int64(len(buf)) != declaredSize {
			return nil, fmt.Errorf("failed to read message sequence %d: unexpected payload size", seq)
		}
		subj := hdr.Name
		mhdr := buf[:hdr.HeaderSize]
		msg := buf[hdr.HeaderSize : hdr.HeaderSize+hdr.PayloadSize]
		if seq <= lseq {
			return nil, fmt.Errorf("message sequence %d out of order", seq)
		}
		// We could have deleted messages since the last message we stored, if so
		// we should work out what the gap is and skip those sequences.
		if gap := seq - lseq - 1; gap > 0 {
			if err := store.SkipMsgs(lseq+1, gap); err != nil {
				return nil, fmt.Errorf("failed to process gap: %w", err)
			}
		}
		lseq = seq
		ttl, err := getMessageTTL(mhdr)
		if err != nil {
			return nil, fmt.Errorf("failed to parse message TTL: %w", err)
		}
		hdrTime := time.Unix(0, hdr.Timestamp)
		if ttl > 0 && time.Now().After(hdrTime.Add(time.Duration(ttl)*time.Second)) {
			// If the TTL has exceeded then there isn't much point in storing the message,
			// but we still need to preserve the sequence.
			if err := store.SkipMsgs(seq, 1); err != nil {
				return nil, fmt.Errorf("failed to process expired message sequence %d: %w", seq, err)
			}
			releaseRestoreBytes(storedSize)
			continue
		}
		if err = store.StoreRawMsg(subj, mhdr, msg, seq, hdr.Timestamp, ttl, false); err != nil {
			return nil, fmt.Errorf("failed to store message sequence %d: %w", seq, err)
		}
		releaseRestoreBytes(storedSize)
	}

	if !eob {
		return mset, fmt.Errorf("backup was truncated")
	}

	// Need to make sure that we pad out with skip msgs to preserve the last
	// sequence, otherwise trailing deleted messages could reuse sequence numbers.
	if lseq < nstate.LastSeq {
		gap := nstate.LastSeq - lseq
		if err := store.SkipMsgs(lseq+1, gap); err != nil {
			return nil, fmt.Errorf("failed to process trailing gap: %w", err)
		}
	}
	return mset, nil
}
