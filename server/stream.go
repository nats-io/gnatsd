// Copyright 2019-2020 The NATS Authors
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
	"archive/tar"
	"bytes"
	"compress/gzip"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"math"
	"os"
	"path"
	"path/filepath"
	"reflect"
	"strconv"
	"sync"
	"time"

	"github.com/nats-io/nuid"
)

// StreamConfig will determine the name, subjects and retention policy
// for a given stream. If subjects is empty the name will be used.
type StreamConfig struct {
	Name         string          `json:"name"`
	Subjects     []string        `json:"subjects,omitempty"`
	Retention    RetentionPolicy `json:"retention"`
	MaxConsumers int             `json:"max_consumers"`
	MaxMsgs      int64           `json:"max_msgs"`
	MaxBytes     int64           `json:"max_bytes"`
	Discard      DiscardPolicy   `json:"discard"`
	MaxAge       time.Duration   `json:"max_age"`
	MaxMsgSize   int32           `json:"max_msg_size,omitempty"`
	Storage      StorageType     `json:"storage"`
	Replicas     int             `json:"num_replicas"`
	NoAck        bool            `json:"no_ack,omitempty"`
	Template     string          `json:"template_owner,omitempty"`
	Duplicates   time.Duration   `json:"duplicate_window,omitempty"`
}

// PubAck is the detail you get back from a publish to a stream that was successful.
// e.g. +OK {"stream": "Orders", "seq": 22}
type PubAck struct {
	Stream    string `json:"stream"`
	Seq       uint64 `json:"seq"`
	Duplicate bool   `json:"duplicate,omitempty"`
}

// StreamInfo shows config and current state for this stream.
type StreamInfo struct {
	Config  StreamConfig `json:"config"`
	Created time.Time    `json:"created"`
	State   StreamState  `json:"state"`
}

// Stream is a jetstream stream of messages. When we receive a message internally destined
// for a Stream we will direct link from the client to this Stream structure.
type Stream struct {
	mu          sync.RWMutex
	sg          *sync.Cond
	sgw         int
	jsa         *jsAccount
	client      *client
	sid         int
	pubAck      []byte
	sendq       chan *jsPubMsg
	store       StreamStore
	consumers   map[string]*Consumer
	config      StreamConfig
	created     time.Time
	ddmap       map[string]*ddentry
	ddarr       []*ddentry
	ddindex     int
	ddtmr       *time.Timer
	streamSeq   uint64
	subjectSeqs map[string]uint64
}

// JSPubId is used for identifying published messages and performing de-duplication.
const JSPubId = "Msg-Id"
const StreamDefaultDuplicatesWindow = 2 * time.Minute

// Dedupe entry
type ddentry struct {
	id  string
	seq uint64
	ts  int64
}

// JSExpSeq is used to perform an optimistic concurrency control check
// for a published message. The value of this header should be the current
// sequence of the stream.
const JSExpSeq = "Exp-Seq"
const JSExpSeqStreamPrefix = "!"

// Replicas Range
const (
	StreamDefaultReplicas = 1
	StreamMaxReplicas     = 7
)

// AddStream adds a stream for the given account.
func (a *Account) AddStream(config *StreamConfig) (*Stream, error) {
	return a.AddStreamWithStore(config, nil)
}

// AddStreamWithStore adds a stream for the given account with custome store config options.
func (a *Account) AddStreamWithStore(config *StreamConfig, fsConfig *FileStoreConfig) (*Stream, error) {
	s, jsa, err := a.checkForJetStream()
	if err != nil {
		return nil, err
	}

	// Sensible defaults.
	cfg, err := checkStreamCfg(config)
	if err != nil {
		return nil, err
	}

	jsa.mu.Lock()
	if mset, ok := jsa.streams[cfg.Name]; ok {
		jsa.mu.Unlock()
		// Check to see if configs are same.
		ocfg := mset.Config()
		if reflect.DeepEqual(ocfg, cfg) {
			return mset, nil
		} else {
			return nil, fmt.Errorf("stream name already in use")
		}
	}
	// Check for limits.
	if err := jsa.checkLimits(&cfg); err != nil {
		jsa.mu.Unlock()
		return nil, err
	}
	// Check for template ownership if present.
	if cfg.Template != _EMPTY_ && jsa.account != nil {
		if !jsa.checkTemplateOwnership(cfg.Template, cfg.Name) {
			jsa.mu.Unlock()
			return nil, fmt.Errorf("stream not owned by template")
		}
	}

	// Check for overlapping subjects. These are not allowed for now.
	if jsa.subjectsOverlap(cfg.Subjects) {
		jsa.mu.Unlock()
		return nil, fmt.Errorf("subjects overlap with an existing stream")
	}

	// Setup the internal client.
	c := s.createInternalJetStreamClient()
	mset := &Stream{
		jsa:         jsa,
		config:      cfg,
		client:      c,
		consumers:   make(map[string]*Consumer),
		subjectSeqs: make(map[string]uint64),
	}
	mset.sg = sync.NewCond(&mset.mu)

	jsa.streams[cfg.Name] = mset
	storeDir := path.Join(jsa.storeDir, streamsDir, cfg.Name)
	jsa.mu.Unlock()

	// Bind to the account.
	c.registerWithAccount(a)

	// Create the appropriate storage
	fsCfg := fsConfig
	if fsCfg == nil {
		fsCfg = &FileStoreConfig{}
	}
	fsCfg.StoreDir = storeDir
	if err := mset.setupStore(fsCfg); err != nil {
		mset.Delete()
		return nil, err
	}
	// Setup our internal send go routine.
	mset.setupSendCapabilities()

	// Create our pubAck here. This will be reused and for +OK will contain JSON
	// for stream name and sequence.
	longestSeq := strconv.FormatUint(math.MaxUint64, 10)
	lpubAck := len(OK) + len(cfg.Name) + len("{\"stream\": ,\"seq\": }") + len(longestSeq)
	mset.pubAck = make([]byte, 0, lpubAck)
	mset.pubAck = append(mset.pubAck, OK...)
	mset.pubAck = append(mset.pubAck, fmt.Sprintf(" {\"stream\": %q, \"seq\": ", cfg.Name)...)

	// Rebuild dedupe as needed.
	mset.rebuildDedupe()
	// Rebuild seqs as needed.
	mset.rebuildSeqs()

	// Setup subscriptions
	if err := mset.subscribeToStream(); err != nil {
		mset.Delete()
		return nil, err
	}

	// Send advisory.
	mset.sendCreateAdvisory()

	return mset, nil
}

// rebuildSeqs will rebuild any dedupe structures needed after recovery of a stream.
// Lock not needed, only called during initialization.
func (mset *Stream) rebuildSeqs() {
	state := mset.store.State()
	if state.Msgs == 0 {
		return
	}

	mset.streamSeq = state.LastSeq
	if mset.subjectSeqs == nil {
		mset.subjectSeqs = make(map[string]uint64)
	}

	for seq := uint64(0); seq <= state.LastSeq; seq++ {
		sub, _, _, _, err := mset.store.LoadMsg(seq)
		if err == nil {
			mset.subjectSeqs[sub] = seq
		}
	}
}

// rebuildDedupe will rebuild any dedupe structures needed after recovery of a stream.
// Lock not needed, only called during initialization.
// TODO(dlc) - Might be good to know if this should be checked at all for streams with no
// headers and msgId in them. Would need signaling from the storage layer.
func (mset *Stream) rebuildDedupe() {
	state := mset.store.State()
	if state.Msgs == 0 {
		return
	}

	// We have some messages. Lookup starting sequence by duplicate time window.
	sseq := mset.store.GetSeqFromTime(time.Now().Add(-mset.config.Duplicates))
	if sseq == 0 {
		return
	}

	for seq := sseq; seq <= state.LastSeq; seq++ {
		_, hdr, _, ts, err := mset.store.LoadMsg(seq)
		if err == nil && len(hdr) > 0 {
			if msgId := getMsgId(hdr); msgId != "" {
				mset.storeMsgId(&ddentry{msgId, seq, ts})
			}
		}
	}
}

func (mset *Stream) sendCreateAdvisory() {
	mset.mu.Lock()
	name := mset.config.Name
	template := mset.config.Template
	sendq := mset.sendq
	mset.mu.Unlock()

	if sendq == nil {
		return
	}

	// finally send an event that this stream was created
	m := JSStreamActionAdvisory{
		TypedEvent: TypedEvent{
			Type: JSStreamActionAdvisoryType,
			ID:   nuid.Next(),
			Time: time.Now().UTC(),
		},
		Stream:   name,
		Action:   CreateEvent,
		Template: template,
	}

	j, err := json.MarshalIndent(m, "", "  ")
	if err == nil {
		subj := JSAdvisoryStreamCreatedPre + "." + name
		sendq <- &jsPubMsg{subj, subj, _EMPTY_, nil, j, nil, 0}
	}
}

func (mset *Stream) sendDeleteAdvisoryLocked() {
	if mset.sendq == nil {
		return
	}

	m := JSStreamActionAdvisory{
		TypedEvent: TypedEvent{
			Type: JSStreamActionAdvisoryType,
			ID:   nuid.Next(),
			Time: time.Now().UTC(),
		},
		Stream:   mset.config.Name,
		Action:   DeleteEvent,
		Template: mset.config.Template,
	}

	j, err := json.MarshalIndent(m, "", "  ")
	if err == nil {
		subj := JSAdvisoryStreamDeletedPre + "." + mset.config.Name
		mset.sendq <- &jsPubMsg{subj, subj, _EMPTY_, nil, j, nil, 0}
	}
}

func (mset *Stream) sendUpdateAdvisoryLocked() {
	if mset.sendq == nil {
		return
	}

	m := JSStreamActionAdvisory{
		TypedEvent: TypedEvent{
			Type: JSStreamActionAdvisoryType,
			ID:   nuid.Next(),
			Time: time.Now().UTC(),
		},
		Stream: mset.config.Name,
		Action: ModifyEvent,
	}

	j, err := json.MarshalIndent(m, "", "  ")
	if err == nil {
		subj := JSAdvisoryStreamUpdatedPre + "." + mset.config.Name
		mset.sendq <- &jsPubMsg{subj, subj, _EMPTY_, nil, j, nil, 0}
	}
}

// Created returns created time.
func (mset *Stream) Created() time.Time {
	mset.mu.RLock()
	created := mset.created
	mset.mu.RUnlock()
	return created
}

// Internal to allow creation time to be restored.
func (mset *Stream) setCreated(created time.Time) {
	mset.mu.Lock()
	mset.created = created
	mset.mu.Unlock()
}

// Check to see if these subjects overlap with existing subjects.
// Lock should be held.
func (jsa *jsAccount) subjectsOverlap(subjects []string) bool {
	for _, mset := range jsa.streams {
		for _, subj := range mset.config.Subjects {
			for _, tsubj := range subjects {
				if SubjectsCollide(tsubj, subj) {
					return true
				}
			}
		}
	}
	return false
}

func checkStreamCfg(config *StreamConfig) (StreamConfig, error) {
	if config == nil {
		return StreamConfig{}, fmt.Errorf("stream configuration invalid")
	}
	if !isValidName(config.Name) {
		return StreamConfig{}, fmt.Errorf("stream name is required and can not contain '.', '*', '>'")
	}
	if len(config.Name) > JSMaxNameLen {
		return StreamConfig{}, fmt.Errorf("stream name is too long, maximum allowed is %d", JSMaxNameLen)
	}
	cfg := *config

	// TODO(dlc) - check config for conflicts, e.g replicas > 1 in single server mode.
	if cfg.Replicas == 0 {
		cfg.Replicas = 1
	}
	// TODO(dlc) - Remove when clustering happens.
	if cfg.Replicas > 1 {
		return StreamConfig{}, fmt.Errorf("maximum replicas is 1")
	}
	if cfg.Replicas > StreamMaxReplicas {
		return cfg, fmt.Errorf("maximum replicas is %d", StreamMaxReplicas)
	}
	if cfg.MaxMsgs == 0 {
		cfg.MaxMsgs = -1
	}
	if cfg.MaxBytes == 0 {
		cfg.MaxBytes = -1
	}
	if cfg.MaxMsgSize == 0 {
		cfg.MaxMsgSize = -1
	}
	if cfg.MaxConsumers == 0 {
		cfg.MaxConsumers = -1
	}
	if cfg.Duplicates == 0 {
		if cfg.MaxAge != 0 && cfg.MaxAge < StreamDefaultDuplicatesWindow {
			cfg.Duplicates = cfg.MaxAge
		} else {
			cfg.Duplicates = StreamDefaultDuplicatesWindow
		}
	} else if cfg.Duplicates < 0 {
		return StreamConfig{}, fmt.Errorf("duplicates window can not be negative")
	}
	// Check that duplicates is not larger then age if set.
	if cfg.MaxAge != 0 && cfg.Duplicates > cfg.MaxAge {
		return StreamConfig{}, fmt.Errorf("duplicates window can not be larger then max age")
	}

	if len(cfg.Subjects) == 0 {
		cfg.Subjects = append(cfg.Subjects, cfg.Name)
	} else {
		// We can allow overlaps, but don't allow direct duplicates.
		dset := make(map[string]struct{}, len(cfg.Subjects))
		for _, subj := range cfg.Subjects {
			if _, ok := dset[subj]; ok {
				return StreamConfig{}, fmt.Errorf("duplicate subjects detected")
			}
			// Also check to make sure we do not overlap with our $JS API subjects.
			if subjectIsSubsetMatch(subj, "$JS.API.>") {
				return StreamConfig{}, fmt.Errorf("subjects overlap with jetstream api")
			}

			dset[subj] = struct{}{}
		}
	}
	return cfg, nil
}

// Config returns the stream's configuration.
func (mset *Stream) Config() StreamConfig {
	mset.mu.Lock()
	defer mset.mu.Unlock()
	return mset.config
}

// Delete deletes a stream from the owning account.
func (mset *Stream) Delete() error {
	mset.mu.Lock()
	jsa := mset.jsa
	mset.mu.Unlock()
	if jsa == nil {
		return fmt.Errorf("jetstream not enabled for account")
	}
	jsa.mu.Lock()
	delete(jsa.streams, mset.config.Name)
	jsa.mu.Unlock()

	return mset.delete()
}

// Update will allow certain configuration properties of an existing stream to be updated.
func (mset *Stream) Update(config *StreamConfig) error {
	cfg, err := checkStreamCfg(config)
	if err != nil {
		return err
	}
	o_cfg := mset.Config()

	// Name must match.
	if cfg.Name != o_cfg.Name {
		return fmt.Errorf("stream configuration name must match original")
	}
	// Can't change MaxConsumers for now.
	if cfg.MaxConsumers != o_cfg.MaxConsumers {
		return fmt.Errorf("stream configuration update can not change MaxConsumers")
	}
	// Can't change storage types.
	if cfg.Storage != o_cfg.Storage {
		return fmt.Errorf("stream configuration update can not change storage type")
	}
	// Can't change retention.
	if cfg.Retention != o_cfg.Retention {
		return fmt.Errorf("stream configuration update can not change retention policy")
	}
	// Can not have a template owner for now.
	if o_cfg.Template != "" {
		return fmt.Errorf("stream configuration update not allowed on template owned stream")
	}
	if cfg.Template != "" {
		return fmt.Errorf("stream configuration update can not be owned by a template")
	}

	// Check limits.
	mset.mu.Lock()
	jsa := mset.jsa
	mset.mu.Unlock()

	jsa.mu.Lock()
	if cfg.MaxConsumers > 0 && cfg.MaxConsumers > jsa.limits.MaxConsumers {
		jsa.mu.Unlock()
		return fmt.Errorf("stream configuration maximum consumers exceeds account limit")
	}
	if cfg.MaxBytes > 0 && cfg.MaxBytes > o_cfg.MaxBytes {
		if err := jsa.checkBytesLimits(cfg.MaxBytes*int64(cfg.Replicas), cfg.Storage); err != nil {
			jsa.mu.Unlock()
			return err
		}
	}
	jsa.mu.Unlock()

	// Now check for subject interest differences.
	current := make(map[string]struct{}, len(o_cfg.Subjects))
	for _, s := range o_cfg.Subjects {
		current[s] = struct{}{}
	}
	// Update config with new values. The store update will enforce any stricter limits.
	mset.mu.Lock()
	defer mset.mu.Unlock()

	// Now walk new subjects. All of these need to be added, but we will check
	// the originals first, since if it is in there we can skip, already added.
	for _, s := range cfg.Subjects {
		if _, ok := current[s]; !ok {
			if _, err := mset.subscribeInternal(s, mset.processInboundJetStreamMsg); err != nil {
				return err
			}
		}
		delete(current, s)
	}
	// What is left in current needs to be deleted.
	for s := range current {
		if err := mset.unsubscribeInternal(s); err != nil {
			return err
		}
	}

	// Check for the Duplicates
	if cfg.Duplicates != o_cfg.Duplicates && mset.ddtmr != nil {
		// Let it fire right away, it will adjust properly on purge.
		mset.ddtmr.Reset(time.Microsecond)
	}
	// Now update config and store's version of our config.
	mset.config = cfg
	mset.store.UpdateConfig(&cfg)

	mset.sendUpdateAdvisoryLocked()

	return nil
}

// Purge will remove all messages from the stream and underlying store.
func (mset *Stream) Purge() uint64 {
	mset.mu.Lock()
	if mset.client == nil {
		mset.mu.Unlock()
		return 0
	}
	purged := mset.store.Purge()
	stats := mset.store.State()
	var obs []*Consumer
	for _, o := range mset.consumers {
		obs = append(obs, o)
	}
	mset.streamSeq = stats.LastSeq
	mset.subjectSeqs = make(map[string]uint64)
	mset.mu.Unlock()
	for _, o := range obs {
		o.purge(stats.FirstSeq)
	}
	return purged
}

// RemoveMsg will remove a message from a stream.
// FIXME(dlc) - Should pick one and be consistent.
func (mset *Stream) RemoveMsg(seq uint64) (bool, error) {
	return mset.store.RemoveMsg(seq)
}

// DeleteMsg will remove a message from a stream.
func (mset *Stream) DeleteMsg(seq uint64) (bool, error) {
	return mset.store.RemoveMsg(seq)
}

// EraseMsg will securely remove a message and rewrite the data with random data.
func (mset *Stream) EraseMsg(seq uint64) (bool, error) {
	return mset.store.EraseMsg(seq)
}

// Will create internal subscriptions for the msgSet.
// Lock should be held.
func (mset *Stream) subscribeToStream() error {
	for _, subject := range mset.config.Subjects {
		if _, err := mset.subscribeInternal(subject, mset.processInboundJetStreamMsg); err != nil {
			return err
		}
	}
	return nil
}

// FIXME(dlc) - This only works in single server mode for the moment. Need to fix as we expand to clusters.
// Lock should be held.
func (mset *Stream) subscribeInternal(subject string, cb msgHandler) (*subscription, error) {
	c := mset.client
	if c == nil {
		return nil, fmt.Errorf("invalid stream")
	}
	if !c.srv.eventsEnabled() {
		return nil, ErrNoSysAccount
	}
	if cb == nil {
		return nil, fmt.Errorf("undefined message handler")
	}

	mset.sid++

	// Now create the subscription
	return c.processSub([]byte(subject), nil, []byte(strconv.Itoa(mset.sid)), cb, false)
}

// Helper for unlocked stream.
func (mset *Stream) subscribeInternalUnlocked(subject string, cb msgHandler) (*subscription, error) {
	mset.mu.Lock()
	defer mset.mu.Unlock()
	return mset.subscribeInternal(subject, cb)
}

// This will unsubscribe us from the exact subject given.
// We do not currently track the subs so do not have the sid.
// This should be called only on an update.
// Lock should be held.
func (mset *Stream) unsubscribeInternal(subject string) error {
	c := mset.client
	if c == nil {
		return fmt.Errorf("invalid stream")
	}
	if !c.srv.eventsEnabled() {
		return ErrNoSysAccount
	}

	var sid []byte

	c.mu.Lock()
	for _, sub := range c.subs {
		if subject == string(sub.subject) {
			sid = sub.sid
			break
		}
	}
	c.mu.Unlock()

	if sid != nil {
		return c.processUnsub(sid)
	}
	return nil
}

// Lock should be held.
func (mset *Stream) unsubscribe(sub *subscription) {
	if sub == nil || mset.client == nil {
		return
	}
	mset.client.unsubscribe(mset.client.acc, sub, true, true)
}

func (mset *Stream) unsubscribeUnlocked(sub *subscription) {
	mset.mu.Lock()
	mset.unsubscribe(sub)
	mset.mu.Unlock()
}

func (mset *Stream) setupStore(fsCfg *FileStoreConfig) error {
	mset.mu.Lock()
	defer mset.mu.Unlock()

	mset.created = time.Now().UTC()

	switch mset.config.Storage {
	case MemoryStorage:
		ms, err := newMemStore(&mset.config)
		if err != nil {
			return err
		}
		mset.store = ms
	case FileStorage:
		fs, err := newFileStoreWithCreated(*fsCfg, mset.config, mset.created)
		if err != nil {
			return err
		}
		mset.store = fs
	}
	jsa, st := mset.jsa, mset.config.Storage
	mset.store.StorageBytesUpdate(func(delta int64) { jsa.updateUsage(st, delta) })
	return nil
}

// NumMsgIds returns the number of message ids being tracked for duplicate suppression.
func (mset *Stream) NumMsgIds() int {
	mset.mu.RLock()
	defer mset.mu.RUnlock()
	return len(mset.ddmap)
}

// checkMsgId will process and check for duplicates.
// Lock should be held.
func (mset *Stream) checkMsgId(id string) *ddentry {
	if id == "" || mset.ddmap == nil {
		return nil
	}
	return mset.ddmap[id]
}

// Will purge the entries that are past the window.
// Should be called from a timer.
func (mset *Stream) purgeMsgIds() {
	mset.mu.Lock()
	defer mset.mu.Unlock()

	now := time.Now().UnixNano()
	tmrNext := mset.config.Duplicates
	window := int64(tmrNext)

	for i, dde := range mset.ddarr[mset.ddindex:] {
		if now-dde.ts >= window {
			delete(mset.ddmap, dde.id)
		} else {
			mset.ddindex += i
			// Check if we should garbage collect here if we are 1/3 total size.
			if cap(mset.ddarr) > 3*(len(mset.ddarr)-mset.ddindex) {
				mset.ddarr = append([]*ddentry(nil), mset.ddarr[mset.ddindex:]...)
				mset.ddindex = 0
			}
			tmrNext = time.Duration(window - (now - dde.ts))
			break
		}
	}
	if len(mset.ddmap) > 0 {
		// Make sure to not fire too quick
		const minFire = 50 * time.Millisecond
		if tmrNext < minFire {
			tmrNext = minFire
		}
		mset.ddtmr.Reset(tmrNext)
	} else {
		mset.ddtmr.Stop()
		mset.ddtmr = nil
	}
}

// storeMsgId will store the message id for duplicate detection.
func (mset *Stream) storeMsgId(dde *ddentry) {
	mset.mu.Lock()
	if mset.ddmap == nil {
		mset.ddmap = make(map[string]*ddentry)
	}
	if mset.ddtmr == nil {
		mset.ddtmr = time.AfterFunc(mset.config.Duplicates, mset.purgeMsgIds)
	}
	mset.ddmap[dde.id] = dde
	mset.ddarr = append(mset.ddarr, dde)
	mset.mu.Unlock()
}

// Will return the value for the header denoted by key or nil if it does not exists.
// This function ignores errors and tries to achieve speed and no additional allocations.
func getHdrVal(key string, hdr []byte) []byte {
	index := bytes.Index(hdr, []byte(key))
	if index < 0 {
		return nil
	}
	var value []byte
	for i := index + len(key) + 2; i > 0 && i < len(hdr); i++ {
		if hdr[i] == '\r' && i < len(hdr)-1 && hdr[i+1] == '\n' {
			break
		}
		value = append(value, hdr[i])
	}
	return value
}

// Fast lookup of msgId.
func getMsgId(hdr []byte) string {
	return string(getHdrVal(JSPubId, hdr))
}

// Fast lookup of expected sequence
func getExpSeq(hdr []byte) (uint64, bool, bool, error) {
	val := getHdrVal(JSExpSeq, hdr)
	if len(val) == 0 {
		return 0, false, false, nil
	}
	var stream bool
	// Character indicator stream flag.
	// TODO: this is arbitrary, but something to distinguish a stream vs.
	// subject level check is needed.
	if val[0] == JSExpSeqStreamPrefix[0] {
		stream = true
		val = val[1:]
	}
	seq, err := strconv.ParseUint(string(val), 10, 64)
	return seq, true, stream, err
}

// processInboundJetStreamMsg handles processing messages bound for a stream.
func (mset *Stream) processInboundJetStreamMsg(_ *subscription, pc *client, subject, reply string, msg []byte) {
	mset.mu.Lock()
	store := mset.store
	c := mset.client
	var accName string
	if c != nil && c.acc != nil {
		accName = c.acc.Name
	}
	doAck := !mset.config.NoAck
	pubAck := mset.pubAck
	jsa := mset.jsa
	stype := mset.config.Storage
	name := mset.config.Name
	maxMsgSize := int(mset.config.MaxMsgSize)
	numConsumers := len(mset.consumers)
	interestRetention := mset.config.Retention == InterestPolicy

	// Process msgId and expSeq if we have headers.
	var msgId string
	if pc != nil && pc.pa.hdr > 0 {
		msgId = getMsgId(msg[:pc.pa.hdr])

		if dde := mset.checkMsgId(msgId); dde != nil {
			if doAck && len(reply) > 0 {
				response := append(pubAck, strconv.FormatUint(dde.seq, 10)...)
				response = append(response, ", \"duplicate\": true}"...)
				mset.sendq <- &jsPubMsg{reply, _EMPTY_, _EMPTY_, nil, response, nil, 0}
			}
			mset.mu.Unlock()
			return
		}

		// Check if the expected sequence is present or not. If not, process
		// without a check.
		expSeq, present, streamScope, err := getExpSeq(msg[:pc.pa.hdr])
		if err != nil {
			if doAck && len(reply) > 0 {
				response := []byte("-ERR 'bad exp-seq'")
				mset.sendq <- &jsPubMsg{reply, _EMPTY_, _EMPTY_, nil, response, nil, 0}
			}
			mset.mu.Unlock()
			return
		}

		if present {
			currentSeq := mset.streamSeq
			if !streamScope {
				if seq, ok := mset.subjectSeqs[subject]; ok {
					currentSeq = seq
				}
			}

			if expSeq != currentSeq {
				if doAck && len(reply) > 0 {
					response := []byte("-ERR 'exp-seq conflict'")
					mset.sendq <- &jsPubMsg{reply, _EMPTY_, _EMPTY_, nil, response, nil, 0}
				}
				mset.mu.Unlock()
				return
			}
		}
	}

	mset.mu.Unlock()

	if c == nil {
		return
	}

	// Response Ack.
	var (
		response []byte
		seq      uint64
		err      error
		ts       int64
	)

	// Header support.
	var hdr []byte

	// Check to see if we are over the account limit.
	if maxMsgSize >= 0 && len(msg) > maxMsgSize {
		response = []byte("-ERR 'message size exceeds maximum allowed'")
	} else {
		// Headers.
		if pc != nil && pc.pa.hdr > 0 {
			hdr = msg[:pc.pa.hdr]
			msg = msg[pc.pa.hdr:]
		}
		seq, ts, err = store.StoreMsg(subject, hdr, msg)
		if err != nil {
			if err != ErrStoreClosed {
				c.Errorf("JetStream failed to store a msg on account: %q stream: %q -  %v", accName, name, err)
			}
			response = []byte(fmt.Sprintf("-ERR '%v'", err))
		} else if jsa.limitsExceeded(stype) {
			c.Warnf("JetStream resource limits exceeded for account: %q", accName)
			response = []byte("-ERR 'resource limits exceeded for account'")
			store.RemoveMsg(seq)
			seq = 0
		} else if err == nil {
			mset.mu.Lock()
			mset.streamSeq = seq
			mset.subjectSeqs[subject] = seq
			mset.mu.Unlock()

			if doAck && len(reply) > 0 {
				response = append(pubAck, strconv.FormatUint(seq, 10)...)
				response = append(response, '}')
			}

			// If we have a msgId make sure to save.
			if msgId != "" {
				mset.storeMsgId(&ddentry{msgId, seq, ts})
			}
			// If we are interest based retention and have no consumers clean that up here.
			if interestRetention && numConsumers == 0 {
				store.RemoveMsg(seq)
			}
		}
	}

	// Send response here.
	if doAck && len(reply) > 0 {
		mset.sendq <- &jsPubMsg{reply, _EMPTY_, _EMPTY_, nil, response, nil, 0}
	}

	if err == nil && numConsumers > 0 && seq > 0 {
		var needSignal bool
		mset.mu.Lock()
		for _, o := range mset.consumers {
			if !o.deliverCurrentMsg(subject, hdr, msg, seq, ts) {
				needSignal = true
			}
		}
		mset.mu.Unlock()

		if needSignal {
			mset.signalConsumers()
		}
	}
}

// Will signal all waiting consumers.
func (mset *Stream) signalConsumers() {
	mset.mu.Lock()
	if mset.sgw > 0 {
		mset.sg.Broadcast()
	}
	mset.mu.Unlock()
}

// Internal message for use by jetstream subsystem.
type jsPubMsg struct {
	subj  string
	dsubj string
	reply string
	hdr   []byte
	msg   []byte
	o     *Consumer
	seq   uint64
}

// StoredMsg is for raw access to messages in a stream.
type StoredMsg struct {
	Subject  string    `json:"subject"`
	Sequence uint64    `json:"seq"`
	Header   []byte    `json:"hdrs,omitempty"`
	Data     []byte    `json:"data,omitempty"`
	Time     time.Time `json:"time"`
}

// TODO(dlc) - Maybe look at onering instead of chan - https://github.com/pltr/onering
const msetSendQSize = 1024

// This is similar to system semantics but did not want to overload the single system sendq,
// or require system account when doing simple setup with jetstream.
func (mset *Stream) setupSendCapabilities() {
	mset.mu.Lock()
	defer mset.mu.Unlock()
	if mset.sendq != nil {
		return
	}
	mset.sendq = make(chan *jsPubMsg, msetSendQSize)
	go mset.internalSendLoop()
}

// Name returns the stream name.
func (mset *Stream) Name() string {
	mset.mu.Lock()
	defer mset.mu.Unlock()
	return mset.config.Name
}

func (mset *Stream) internalSendLoop() {
	mset.mu.Lock()
	c := mset.client
	if c == nil {
		mset.mu.Unlock()
		return
	}
	s := c.srv
	sendq := mset.sendq
	name := mset.config.Name
	mset.mu.Unlock()

	// Warn when internal send queue is backed up past 75%
	warnThresh := 3 * msetSendQSize / 4
	warnFreq := time.Second
	last := time.Now().Add(-warnFreq)

	for {
		if len(sendq) > warnThresh && time.Since(last) >= warnFreq {
			s.Warnf("Jetstream internal send queue > 75%% for account: %q stream: %q", c.acc.Name, name)
			last = time.Now()
		}
		select {
		case pm := <-sendq:
			if pm == nil {
				return
			}
			c.pa.subject = []byte(pm.subj)
			c.pa.deliver = []byte(pm.dsubj)
			c.pa.size = len(pm.msg) + len(pm.hdr)
			c.pa.szb = []byte(strconv.Itoa(c.pa.size))
			c.pa.reply = []byte(pm.reply)

			var msg []byte
			if len(pm.hdr) > 0 {
				c.pa.hdr = len(pm.hdr)
				c.pa.hdb = []byte(strconv.Itoa(c.pa.hdr))
				msg = append(pm.hdr, pm.msg...)
				msg = append(msg, _CRLF_...)
			} else {
				c.pa.hdr = -1
				c.pa.hdb = nil
				msg = append(pm.msg, _CRLF_...)
			}
			didDeliver := c.processInboundClientMsg(msg)
			c.pa.szb = nil
			c.flushClients(0)
			// Check to see if this is a delivery for an observable and
			// we failed to deliver the message. If so alert the observable.
			if pm.o != nil && pm.seq > 0 && !didDeliver {
				pm.o.didNotDeliver(pm.seq)
			}
		case <-s.quitCh:
			return
		}
	}
}

// Internal function to delete a stream.
func (mset *Stream) delete() error {
	return mset.stop(true)
}

// Internal function to stop or delete the stream.
func (mset *Stream) stop(delete bool) error {
	// Clean up consumers.
	mset.mu.Lock()
	var obs []*Consumer
	for _, o := range mset.consumers {
		obs = append(obs, o)
	}
	mset.consumers = nil
	mset.mu.Unlock()

	for _, o := range obs {
		// Second flag says do not broadcast to signal.
		// TODO(dlc) - If we have an err here we don't want to stop
		// but should we log?
		o.stop(delete, false, delete)
	}

	// Make sure we release all consumers here at once.
	mset.mu.Lock()
	mset.sg.Broadcast()

	// Send stream delete advisory after the consumers.
	if delete {
		mset.sendDeleteAdvisoryLocked()
	}

	if mset.sendq != nil {
		mset.sendq <- nil
	}

	c := mset.client
	mset.client = nil
	if c == nil {
		mset.mu.Unlock()
		return nil
	}

	// Cleanup duplicate timer if running.
	if mset.ddtmr != nil {
		mset.ddtmr.Stop()
		mset.ddtmr = nil
		mset.ddarr = nil
		mset.ddmap = nil
	}
	mset.mu.Unlock()

	c.closeConnection(ClientClosed)

	if mset.store == nil {
		return nil
	}

	if delete {
		if err := mset.store.Delete(); err != nil {
			return err
		}
	} else if err := mset.store.Stop(); err != nil {
		return err
	}

	return nil
}

func (mset *Stream) GetMsg(seq uint64) (*StoredMsg, error) {
	subj, hdr, msg, ts, err := mset.store.LoadMsg(seq)
	if err != nil {
		return nil, err
	}
	sm := &StoredMsg{
		Subject:  subj,
		Sequence: seq,
		Header:   hdr,
		Data:     msg,
		Time:     time.Unix(0, ts).UTC(),
	}
	return sm, nil
}

// Consunmers will return all the current consumers for this stream.
func (mset *Stream) Consumers() []*Consumer {
	mset.mu.Lock()
	defer mset.mu.Unlock()

	var obs []*Consumer
	for _, o := range mset.consumers {
		obs = append(obs, o)
	}
	return obs
}

// NumConsumers reports on number of active observables for this stream.
func (mset *Stream) NumConsumers() int {
	mset.mu.Lock()
	defer mset.mu.Unlock()
	return len(mset.consumers)
}

// LookupConsumer will retrieve a consumer by name.
func (mset *Stream) LookupConsumer(name string) *Consumer {
	mset.mu.Lock()
	defer mset.mu.Unlock()

	for _, o := range mset.consumers {
		if o.name == name {
			return o
		}
	}
	return nil
}

// State will return the current state for this stream.
func (mset *Stream) State() StreamState {
	mset.mu.Lock()
	c := mset.client
	mset.mu.Unlock()
	if c == nil {
		return StreamState{}
	}
	// Currently rely on store.
	// TODO(dlc) - This will need to change with clusters.
	return mset.store.State()
}

// waitForMsgs will have the stream wait for the arrival of new messages.
func (mset *Stream) waitForMsgs() {
	mset.mu.Lock()

	if mset.client == nil {
		mset.mu.Unlock()
		return
	}

	mset.sgw++
	mset.sg.Wait()
	mset.sgw--

	mset.mu.Unlock()
}

// Determines if the new proposed partition is unique amongst all observables.
// Lock should be held.
func (mset *Stream) partitionUnique(partition string) bool {
	for _, o := range mset.consumers {
		if o.config.FilterSubject == _EMPTY_ {
			return false
		}
		if subjectIsSubsetMatch(partition, o.config.FilterSubject) {
			return false
		}
	}
	return true
}

// Lock should be held.
func (mset *Stream) checkInterest(seq uint64, obs *Consumer) bool {
	for _, o := range mset.consumers {
		if o != obs && o.needAck(seq) {
			return true
		}
	}
	return false
}

// ackMsg is called into from an observable when we have a WorkQueue or Interest retention policy.
func (mset *Stream) ackMsg(obs *Consumer, seq uint64) {
	switch mset.config.Retention {
	case LimitsPolicy:
		return
	case WorkQueuePolicy:
		mset.store.RemoveMsg(seq)
	case InterestPolicy:
		mset.mu.Lock()
		hasInterest := mset.checkInterest(seq, obs)
		mset.mu.Unlock()
		if !hasInterest {
			mset.store.RemoveMsg(seq)
		}
	}
}

// Snapshot creates a snapshot for the stream and possibly consumers.
func (mset *Stream) Snapshot(deadline time.Duration, checkMsgs, includeConsumers bool) (*SnapshotResult, error) {
	mset.mu.Lock()
	if mset.client == nil || mset.store == nil {
		mset.mu.Unlock()
		return nil, fmt.Errorf("invalid stream")
	}
	store := mset.store
	var obs []*Consumer
	for _, o := range mset.consumers {
		obs = append(obs, o)
	}
	mset.mu.Unlock()

	// Make sure to sync their state.
	for _, o := range obs {
		o.writeState()
	}

	return store.Snapshot(deadline, checkMsgs, includeConsumers)
}

const snapsDir = "__snapshots__"

// RestoreStream will restore a stream from a snapshot.
func (a *Account) RestoreStream(stream string, r io.Reader) (*Stream, error) {
	_, jsa, err := a.checkForJetStream()
	if err != nil {
		return nil, err
	}

	sd := path.Join(jsa.storeDir, snapsDir)
	defer os.RemoveAll(sd)

	if _, err := os.Stat(sd); os.IsNotExist(err) {
		if err := os.MkdirAll(sd, 0755); err != nil {
			return nil, fmt.Errorf("could not create snapshots directory - %v", err)
		}
	}
	sdir, err := ioutil.TempDir(sd, "snap-")
	if err != nil {
		return nil, err
	}
	if _, err := os.Stat(sdir); os.IsNotExist(err) {
		if err := os.MkdirAll(sdir, 0755); err != nil {
			return nil, fmt.Errorf("could not create snapshots directory - %v", err)
		}
	}

	gzr, err := gzip.NewReader(r)
	if err != nil {
		return nil, err
	}
	defer gzr.Close()
	tr := tar.NewReader(gzr)

	for {
		hdr, err := tr.Next()
		if err == io.EOF {
			break // End of snapshot
		}
		if err != nil {
			return nil, err
		}
		fpath := path.Join(sdir, filepath.Clean(hdr.Name))
		pdir := filepath.Dir(fpath)
		os.MkdirAll(pdir, 0750)
		fd, err := os.OpenFile(fpath, os.O_CREATE|os.O_RDWR, 0600)
		if err != nil {
			return nil, err
		}
		_, err = io.Copy(fd, tr)
		fd.Close()
		if err != nil {
			return nil, err
		}
	}

	// Check metadata
	var cfg FileStreamInfo
	b, err := ioutil.ReadFile(path.Join(sdir, JetStreamMetaFile))
	if err != nil {
		return nil, err
	}
	if err := json.Unmarshal(b, &cfg); err != nil {
		return nil, err
	}
	// See if names match
	if cfg.Name != stream {
		return nil, fmt.Errorf("stream name [%q] does not match snapshot stream [%q]", stream, cfg.Name)
	}

	// See if this stream already exists.
	if _, err := a.LookupStream(cfg.Name); err == nil {
		return nil, fmt.Errorf("stream [%q] already exists", cfg.Name)
	}
	// Move into the correct place here.
	ndir := path.Join(jsa.storeDir, streamsDir, cfg.Name)
	if err := os.Rename(sdir, ndir); err != nil {
		return nil, err
	}
	if cfg.Template != _EMPTY_ {
		if err := jsa.addStreamNameToTemplate(cfg.Template, cfg.Name); err != nil {
			return nil, err
		}
	}
	mset, err := a.AddStream(&cfg.StreamConfig)
	if err != nil {
		return nil, err
	}
	if !cfg.Created.IsZero() {
		mset.setCreated(cfg.Created)
	}

	// Now do consumers.
	odir := path.Join(ndir, consumerDir)
	ofis, _ := ioutil.ReadDir(odir)
	for _, ofi := range ofis {
		metafile := path.Join(odir, ofi.Name(), JetStreamMetaFile)
		metasum := path.Join(odir, ofi.Name(), JetStreamMetaFileSum)
		if _, err := os.Stat(metafile); os.IsNotExist(err) {
			mset.Delete()
			return nil, fmt.Errorf("error restoring consumer [%q]: %v", ofi.Name(), err)
		}
		buf, err := ioutil.ReadFile(metafile)
		if err != nil {
			mset.Delete()
			return nil, fmt.Errorf("error restoring consumer [%q]: %v", ofi.Name(), err)
		}
		if _, err := os.Stat(metasum); os.IsNotExist(err) {
			mset.Delete()
			return nil, fmt.Errorf("error restoring consumer [%q]: %v", ofi.Name(), err)
		}
		var cfg FileConsumerInfo
		if err := json.Unmarshal(buf, &cfg); err != nil {
			mset.Delete()
			return nil, fmt.Errorf("error restoring consumer [%q]: %v", ofi.Name(), err)
		}
		isEphemeral := !isDurableConsumer(&cfg.ConsumerConfig)
		if isEphemeral {
			// This is an ephermal consumer and this could fail on restart until
			// the consumer can reconnect. We will create it as a durable and switch it.
			cfg.ConsumerConfig.Durable = ofi.Name()
		}
		obs, err := mset.AddConsumer(&cfg.ConsumerConfig)
		if err != nil {
			mset.Delete()
			return nil, fmt.Errorf("error restoring consumer [%q]: %v", ofi.Name(), err)
		}
		if isEphemeral {
			obs.switchToEphemeral()
		}
		if !cfg.Created.IsZero() {
			obs.setCreated(cfg.Created)
		}
		if err := obs.readStoredState(); err != nil {
			mset.Delete()
			return nil, fmt.Errorf("error restoring consumer [%q]: %v", ofi.Name(), err)
		}
	}
	return mset, nil
}
