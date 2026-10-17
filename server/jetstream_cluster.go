// Copyright 2020-2025 The NATS Authors
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
	"bytes"
	"cmp"
	crand "crypto/rand"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"iter"
	"maps"
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
	"time"

	"github.com/antithesishq/antithesis-sdk-go/assert"
	"github.com/klauspost/compress/s2"
	"github.com/nats-io/nuid"
)

// jetStreamCluster holds information about the meta group and stream assignments.
type jetStreamCluster struct {
	// The metacontroller raftNode.
	meta RaftNode
	// For stream and consumer assignments. All servers will have this be the same.
	// ACCOUNT -> STREAM -> Stream Assignment -> Consumers
	streams map[string]map[string]*streamAssignment
	// These are inflight proposals and used to apply limits when there are
	// concurrent requests that would otherwise be accepted.
	// We also record the assignment for the stream/consumer. This is needed since if we have
	// concurrent requests for same account and stream/consumer we need to let it process to get
	// a response but they need to be same group, peers etc. and sync subjects.
	inflightStreams   map[string]map[string]*inflightStreamInfo
	inflightConsumers map[string]map[string]map[string]*inflightConsumerInfo
	// Tracks raft groups currently being started by createRaftGroup, so that
	// concurrent callers for the same group can wait without holding js.mu
	// across the disk I/O performed during startup.
	creatingRaftGroups map[string]chan struct{}
	// Holds a map of a peer ID to the reply subject, to only respond after gaining
	// quorum on the peer-remove action.
	peerRemoveReply map[string]peerRemoveInfo
	// Raft term, used to determine if we are still the leader for the current term.
	term uint64
	// Signals meta-leader should check the stream assignments.
	streamsCheck bool
	// Server.
	s *Server
	// Internal client.
	c *client
	// Processing assignment results.
	streamResults   *subscription
	consumerResults *subscription
	// System level request to have the leader stepdown.
	stepdown *subscription
	// System level requests to remove a peer.
	peerRemove *subscription
	// System level request to move a stream
	peerStreamMove *subscription
	// System level request to cancel a stream move
	peerStreamCancelMove *subscription
	// To pop out the monitorCluster before the raft layer.
	qch chan struct{}
	// To notify others that monitorCluster has actually stopped.
	stopped chan struct{}
	// Track last meta snapshot time and duration for monitoring.
	lastMetaSnapTime     int64 // Unix nanoseconds
	lastMetaSnapDuration int64 // Duration in nanoseconds
}

// Used to track inflight stream create/update/delete requests that have been proposed but not yet applied.
type inflightStreamInfo struct {
	ops     uint64 // Inflight operations, i.e. inflight stream creates/updates/deletes.
	deleted bool   // Whether the stream has been deleted.
	*streamAssignment
}

// Used to track inflight consumer create/update/delete requests that have been proposed but not yet applied.
type inflightConsumerInfo struct {
	ops     uint64 // Inflight operations, i.e. inflight consumer creates/updates/deletes.
	deleted bool   // Whether the consumer has been deleted.
	*consumerAssignment
}

// Used to track inflight peer-remove info to respond 'success' after quorum.
type peerRemoveInfo struct {
	ci      *ClientInfo
	subject string
	reply   string
	request string
}

// Used to guide placement of streams and meta controllers in clustered JetStream.
type Placement struct {
	Cluster   string   `json:"cluster,omitempty"`
	Tags      []string `json:"tags,omitempty"`
	Preferred string   `json:"preferred,omitempty"`
}

func (p *Placement) clone() *Placement {
	if p == nil {
		return nil
	}
	cp := *p
	cp.Tags = copyStrings(p.Tags)
	return &cp
}

// Define types of the entry.
type entryOp uint8

// ONLY ADD TO THE END, DO NOT INSERT IN BETWEEN WILL BREAK SERVER INTEROP.
const (
	// Meta ops.
	assignStreamOp entryOp = iota
	assignConsumerOp
	removeStreamOp
	removeConsumerOp
	// Stream ops.
	streamMsgOp
	purgeStreamOp
	deleteMsgOp
	// Consumer ops.
	updateDeliveredOp
	updateAcksOp
	// Compressed consumer assignments.
	assignCompressedConsumerOp
	// Filtered Consumer skip.
	updateSkipOp
	// Update Stream.
	updateStreamOp
	// For updating information on pending pull requests.
	addPendingRequest
	removePendingRequest
	// For sending compressed streams, either through RAFT or catchup.
	compressedStreamMsgOp
	// For sending deleted gaps on catchups for replicas.
	deleteRangeOp
	// Batch stream ops.
	batchMsgOp
	batchCommitMsgOp
	// Consumer rest to specific starting sequence.
	resetSeqOp
)

// raftGroups are controlled by the metagroup controller.
// The raftGroups will house streams and consumers.
type raftGroup struct {
	Name      string      `json:"name"`
	Peers     []string    `json:"peers"`
	Storage   StorageType `json:"store"`
	Cluster   string      `json:"cluster,omitempty"`
	Preferred string      `json:"preferred,omitempty"`
	ScaleUp   bool        `json:"scale_up,omitempty"`
	// Excluded peers are evacuated and can't be added to this group as part of the meta
	// leader's reconciliation. Cleared when this group is at the configured replicas.
	Excluded []string `json:"excluded,omitempty"`
	// Desired holds the target placement while this group is being moved or
	// scaled; it is nil when the group is stable.
	Desired *desiredRaftGroup `json:"desired,omitempty"`
	// Internal
	node RaftNode
	// migration reports what the group leader is currently doing to move this group
	// toward its desired state. Local to the group leader and not replicated: only
	// the leader drives the migration, and only the leader answers INFO requests.
	migration *DesiredClusterInfoStatus
}

// desiredGroupPlacement specifies the desired peer set.
// A stream or consumer raftGroup can be scaled or moved safely based on this desired state.
type desiredRaftGroup struct {
	Created time.Time `json:"created"`
	ID      string    `json:"id"`
	// Term is the raft term of the group leader driving this desired state. It acts as a
	// fencing token; reconcile requests from older leadership terms are rejected.
	Term      uint64   `json:"term,omitempty"`
	Peers     []string `json:"peers"`
	Cluster   string   `json:"cluster,omitempty"`
	Preferred string   `json:"preferred,omitempty"`

	// ScaleDown marks this desired state as a scale down. The Peers field is the
	// total set that can be scaled down from.
	ScaleDown bool `json:"scale_down,omitempty"`

	// Move marks this desired state as retargeting placement.
	Move bool `json:"move,omitempty"`

	// CancelMove marks this desired state as a rollback to the recorded origin, proposed
	// by a cancel move request. The assignment it is proposed with is responded to as a
	// cancel move rather than a stream update.
	CancelMove bool `json:"cancel_move,omitempty"`

	// Removed are peers an operator peer-removed from this group. A group that
	// can't reach quorum may only evict what's recorded here.
	Removed []string `json:"removed,omitempty"`

	// Members are the Raft members known to hold the group's data, as reported by the group leader.
	// It only grows, a peer leaves it together with the assignment. A peer not in it observes until added.
	Members []string `json:"members,omitempty"`

	Origin *desiredRaftGroupOrigin `json:"origin,omitempty"`
}

// desiredRaftGroupOrigin specifies the original properties of the asset before any desired state changes were made.
// Multiple desired state changes MUST NOT update these values, only the initial values must be set, allowing to
// revert to before any changes were made.
type desiredRaftGroupOrigin struct {
	Peers     []string   `json:"peers"`
	Cluster   string     `json:"cluster,omitempty"`
	Replicas  int        `json:"replicas"`
	Placement *Placement `json:"placement,omitempty"`
	// When changing between retention policies, this is the origin retention.
	// While recorded the stream acts under Limits retention until converged.
	Retention *RetentionPolicy `json:"retention,omitempty"`
}

// desiredOrigin returns the origin the asset is still running at while a desired
// state is pending, or nil if there is none. This server persists and exposes
// desired state, it never acts on it, so an asset stays at its origin until a
// v2.15 server drives the migration.
// Lock should be held.
func (sa *streamAssignment) desiredOrigin() *desiredRaftGroupOrigin {
	if sa == nil || sa.Group == nil || sa.Group.Desired == nil {
		return nil
	}
	return sa.Group.Desired.Origin
}

// atDesiredOrigin returns a copy of the config with the desired origin applied (if any).
func (cfg *StreamConfig) atDesiredOrigin(rg *raftGroup) *StreamConfig {
	if rg.Desired == nil || rg.Desired.Origin == nil {
		return cfg
	}
	newCfg := cfg.clone()
	// The origin always records the placement it started from, nil included, so an
	// unconstrained origin must be restored just the same.
	newCfg.Placement = rg.Desired.Origin.Placement.clone()
	if rg.Desired.Origin.Retention != nil {
		// Any retention change means the stream acts under Limits until converged.
		// Either we're moving from Limits to Interest, and we only apply Interest at the end.
		// Or, we're moving from Interest to Limits, and must release the Interest restrictions prior to converging.
		newCfg.Retention = LimitsPolicy
	}
	return newCfg
}

// atDesiredTarget returns a copy of the running config with the fields that atDesiredOrigin can
// hold back restored from target, which is the config as the user requested it. While the desired
// state is pending the stream runs at its origin, but clients MUST be reported the config they
// requested. Restoring unconditionally is safe, target always holds the requested values, whether
// the desired state is still pending or was already reached.
func (cfg StreamConfig) atDesiredTarget(target *StreamConfig) StreamConfig {
	if target == nil {
		return cfg
	}
	// Must copy, the target can be owned by the meta layer and outlive its lock here.
	cfg.Placement = target.Placement.clone()
	cfg.Retention = target.Retention
	return cfg
}

// targetStreamConfig returns the stream's cfg as the user requested it, looking up the requested config
// from the meta layer. Can differ from the running config while the desired state is still pending,
// or while an update was committed but not applied onto the stream yet.
func (js *jetStream) targetStreamConfig(mset *stream, cfg StreamConfig) StreamConfig {
	// Only a clustered stream can differ from what was requested.
	if js == nil || js.cluster == nil || mset == nil {
		return cfg
	}
	accName := mset.accName()
	js.mu.RLock()
	defer js.mu.RUnlock()
	sa := js.streamAssignment(accName, cfg.Name)
	if sa == nil {
		return cfg
	}
	return cfg.atDesiredTarget(sa.Config)
}

// streamAssignment is what the meta controller uses to assign streams to peers.
type streamAssignment struct {
	Client     *ClientInfo     `json:"client,omitempty"`
	Created    time.Time       `json:"created"`
	ConfigJSON json.RawMessage `json:"stream"`
	Config     *StreamConfig   `json:"-"`
	Group      *raftGroup      `json:"group"`
	Sync       string          `json:"sync"`
	Subject    string          `json:"subject,omitempty"`
	Reply      string          `json:"reply,omitempty"`
	Restore    *StreamState    `json:"restore_state,omitempty"`
	// Internal
	consumers   map[string]*consumerAssignment
	responded   atomic.Bool // copied via clone() to satisfy go vet's noCopy check
	recovering  bool
	catchup     bool // First seen via meta catchup, the raft group still needs empty-log protection.
	reassigning bool // i.e. due to placement issues, lack of resources, etc.
	resetting   bool // i.e. there was an error, and we're stopping and starting the stream
	err         error
	unsupported *unsupportedStreamAssignment
}

func (sa *streamAssignment) hasResponded() bool {
	return sa.responded.Load()
}

// markResponded sets the responded flag and returns the prior value.
func (sa *streamAssignment) markResponded() bool {
	return sa.responded.Swap(true)
}

func (sa *streamAssignment) clearResponded() {
	sa.responded.Store(false)
}

// clone returns a copy of sa. Field-explicit (rather than `*sa`) and
// pointer-returning so the embedded atomic.Bool isn't value-copied;
// responded is transferred via Load/Store. Concurrent callers may write
// responded via markResponded/clearResponded without holding js.mu.
func (sa *streamAssignment) clone() *streamAssignment {
	csa := &streamAssignment{
		Client:      sa.Client,
		Created:     sa.Created,
		ConfigJSON:  sa.ConfigJSON,
		Config:      sa.Config,
		Group:       sa.Group,
		Sync:        sa.Sync,
		Subject:     sa.Subject,
		Reply:       sa.Reply,
		Restore:     sa.Restore,
		consumers:   sa.consumers,
		recovering:  sa.recovering,
		catchup:     sa.catchup,
		reassigning: sa.reassigning,
		resetting:   sa.resetting,
		err:         sa.err,
		unsupported: sa.unsupported,
	}
	csa.responded.Store(sa.responded.Load())
	return csa
}

type unsupportedStreamAssignment struct {
	reason  string
	info    StreamInfo
	sysc    *client
	infoSub *subscription
}

func newUnsupportedStreamAssignment(s *Server, sa *streamAssignment, err error) *unsupportedStreamAssignment {
	reason := "stopped"
	if err != nil {
		if errstr := err.Error(); strings.HasPrefix(errstr, "json:") {
			reason = fmt.Sprintf("unsupported - config error: %s", strings.TrimPrefix(err.Error(), "json: "))
		} else {
			reason = fmt.Sprintf("stopped - %s", errstr)
		}
	} else if sa.Config != nil && !supportsRequiredApiLevel(sa.Config.Metadata) {
		if req := getRequiredApiLevel(sa.Config.Metadata); req != _EMPTY_ {
			reason = fmt.Sprintf("unsupported - required API level: %s, current API level: %d", req, JSApiLevel)
		}
	}
	return &unsupportedStreamAssignment{
		reason: reason,
		info: StreamInfo{
			Created:   sa.Created,
			Config:    *setDynamicStreamMetadata(sa.Config),
			Domain:    s.getOpts().JetStreamDomain,
			TimeStamp: time.Now().UTC(),
		},
	}
}

func (usa *unsupportedStreamAssignment) setupInfoSub(s *Server, sa *streamAssignment) {
	if usa.infoSub != nil {
		return
	}

	// Bind to the system account.
	ic := s.createInternalJetStreamClient()
	ic.registerWithAccount(s.SystemAccount())
	usa.sysc = ic

	// Note below the way we subscribe here is so that we can send requests to ourselves.
	isubj := fmt.Sprintf(clusterStreamInfoT, sa.Client.serviceAccount(), sa.Config.Name)
	usa.infoSub, _ = s.systemSubscribe(isubj, _EMPTY_, false, ic, usa.handleClusterStreamInfoRequest)
}

func (usa *unsupportedStreamAssignment) handleClusterStreamInfoRequest(_ *subscription, c *client, _ *Account, _, reply string, _ []byte) {
	s, acc := c.srv, c.acc
	info := streamInfoClusterResponse{OfflineReason: usa.reason, StreamInfo: usa.info}
	s.sendDelayedErrResponse(acc, reply, nil, s.jsonResponse(&info), errRespDelay)
}

func (usa *unsupportedStreamAssignment) closeInfoSub(s *Server) {
	if usa.infoSub != nil {
		s.sysUnsubscribe(usa.infoSub)
		usa.infoSub = nil
	}
	if usa.sysc != nil {
		usa.sysc.closeConnection(ClientClosed)
		usa.sysc = nil
	}
}

// consumerAssignment is what the meta controller uses to assign consumers to streams.
type consumerAssignment struct {
	Client     *ClientInfo     `json:"client,omitempty"`
	Created    time.Time       `json:"created"`
	Name       string          `json:"name"`
	Stream     string          `json:"stream"`
	ConfigJSON json.RawMessage `json:"consumer"`
	Config     *ConsumerConfig `json:"-"`
	Group      *raftGroup      `json:"group"`
	Subject    string          `json:"subject,omitempty"`
	Reply      string          `json:"reply,omitempty"`
	State      *ConsumerState  `json:"state,omitempty"`
	// Internal
	responded   atomic.Bool // copied via clone() to satisfy go vet's noCopy check
	recovering  bool
	catchup     bool // First seen via meta catchup, the raft group still needs empty-log protection.
	err         error
	unsupported *unsupportedConsumerAssignment
}

func (ca *consumerAssignment) hasResponded() bool {
	return ca.responded.Load()
}

// markResponded sets the responded flag and returns the prior value.
func (ca *consumerAssignment) markResponded() bool {
	return ca.responded.Swap(true)
}

func (ca *consumerAssignment) clearResponded() {
	ca.responded.Store(false)
}

// sameIdentity reports whether nca refers to the same logical consumer as ca.
// Only stable identity fields (Name, Stream, Group name, Created time) are
// compared; request-routing fields like Client/Reply and transient flags are
// intentionally excluded since processClusterCreateConsumer may set the
// per-object o.ca to a clone with the original requester's Client/Reply
// preserved while the meta-layer holds the newer values.
func (ca *consumerAssignment) sameIdentity(nca *consumerAssignment) bool {
	return ca != nil && nca != nil &&
		nca.Name == ca.Name &&
		nca.Stream == ca.Stream &&
		nca.Created.Equal(ca.Created) &&
		nca.Group != nil && ca.Group != nil &&
		nca.Group.Name == ca.Group.Name
}

// clone returns a copy of ca. Field-explicit (rather than `*ca`) and
// pointer-returning so the embedded atomic.Bool isn't value-copied;
// responded is transferred via Load/Store. Concurrent callers may write
// responded via markResponded/clearResponded without holding js.mu.
func (ca *consumerAssignment) clone() *consumerAssignment {
	cca := &consumerAssignment{
		Client:      ca.Client,
		Created:     ca.Created,
		Name:        ca.Name,
		Stream:      ca.Stream,
		ConfigJSON:  ca.ConfigJSON,
		Config:      ca.Config,
		Group:       ca.Group,
		Subject:     ca.Subject,
		Reply:       ca.Reply,
		State:       ca.State,
		recovering:  ca.recovering,
		catchup:     ca.catchup,
		err:         ca.err,
		unsupported: ca.unsupported,
	}
	cca.responded.Store(ca.responded.Load())
	return cca
}

type unsupportedConsumerAssignment struct {
	reason  string
	info    ConsumerInfo
	sysc    *client
	infoSub *subscription
}

func newUnsupportedConsumerAssignment(ca *consumerAssignment, err error) *unsupportedConsumerAssignment {
	reason := "stopped"
	if err != nil {
		if errstr := err.Error(); strings.HasPrefix(errstr, "json:") {
			reason = fmt.Sprintf("unsupported - config error: %s", strings.TrimPrefix(err.Error(), "json: "))
		} else {
			reason = fmt.Sprintf("stopped - %s", errstr)
		}
	} else if ca.Config != nil && !supportsRequiredApiLevel(ca.Config.Metadata) {
		if req := getRequiredApiLevel(ca.Config.Metadata); req != _EMPTY_ {
			reason = fmt.Sprintf("unsupported - required API level: %s, current API level: %d", getRequiredApiLevel(ca.Config.Metadata), JSApiLevel)
		}
	}
	return &unsupportedConsumerAssignment{
		reason: reason,
		info: ConsumerInfo{
			Stream:    ca.Stream,
			Name:      ca.Name,
			Created:   ca.Created,
			Config:    setDynamicConsumerMetadata(ca.Config),
			TimeStamp: time.Now().UTC(),
		},
	}
}

func (uca *unsupportedConsumerAssignment) setupInfoSub(s *Server, ca *consumerAssignment) {
	if uca.infoSub != nil {
		return
	}

	// Bind to the system account.
	ic := s.createInternalJetStreamClient()
	ic.registerWithAccount(s.SystemAccount())
	uca.sysc = ic

	// Note below the way we subscribe here is so that we can send requests to ourselves.
	isubj := fmt.Sprintf(clusterConsumerInfoT, ca.Client.serviceAccount(), ca.Stream, ca.Name)
	uca.infoSub, _ = s.systemSubscribe(isubj, _EMPTY_, false, ic, uca.handleClusterConsumerInfoRequest)
}

func (uca *unsupportedConsumerAssignment) handleClusterConsumerInfoRequest(_ *subscription, c *client, _ *Account, _, reply string, _ []byte) {
	s, acc := c.srv, c.acc
	info := consumerInfoClusterResponse{OfflineReason: uca.reason, ConsumerInfo: uca.info}
	s.sendDelayedErrResponse(acc, reply, nil, s.jsonResponse(&info), errRespDelay)
}

func (uca *unsupportedConsumerAssignment) closeInfoSub(s *Server) {
	if uca.infoSub != nil {
		s.sysUnsubscribe(uca.infoSub)
		uca.infoSub = nil
	}
	if uca.sysc != nil {
		uca.sysc.closeConnection(ClientClosed)
		uca.sysc = nil
	}
}

type writeableConsumerAssignment struct {
	Client     *ClientInfo     `json:"client,omitempty"`
	Created    time.Time       `json:"created"`
	Name       string          `json:"name"`
	Stream     string          `json:"stream"`
	ConfigJSON json.RawMessage `json:"consumer"`
	Group      *raftGroup      `json:"group"`
}

// streamPurge is what the stream leader will replicate when purging a stream.
type streamPurge struct {
	Client  *ClientInfo              `json:"client,omitempty"`
	Stream  string                   `json:"stream"`
	LastSeq uint64                   `json:"last_seq"`
	Subject string                   `json:"subject"`
	Reply   string                   `json:"reply"`
	Request *JSApiStreamPurgeRequest `json:"request,omitempty"`
}

// streamMsgDelete is what the stream leader will replicate when deleting a message.
type streamMsgDelete struct {
	Client  *ClientInfo `json:"client,omitempty"`
	Stream  string      `json:"stream"`
	Seq     uint64      `json:"seq"`
	NoErase bool        `json:"no_erase,omitempty"`
	Subject string      `json:"subject"`
	Reply   string      `json:"reply"`
}

const (
	defaultStoreDirName  = "_js_"
	defaultMetaGroupName = "_meta_"
	defaultMetaFSBlkSize = 1024 * 1024
	jsExcludePlacement   = "!jetstream"
)

// Returns information useful in mixed mode.
func (s *Server) trackedJetStreamServers() (js, total int) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if !s.isRunning() || !s.eventsEnabled() {
		return -1, -1
	}
	s.nodeToInfo.Range(func(k, v any) bool {
		si := v.(nodeInfo)
		if si.js {
			js++
		}
		total++
		return true
	})
	return js, total
}

func (s *Server) getJetStreamCluster() (*jetStream, *jetStreamCluster) {
	if s.isShuttingDown() {
		return nil, nil
	}

	js := s.getJetStream()
	if js == nil {
		return nil, nil
	}

	// Only set once, do not need a lock.
	return js, js.cluster
}

func (s *Server) JetStreamIsClustered() bool {
	return s.jsClustered.Load()
}

func (s *Server) JetStreamIsLeader() bool {
	return s.isMetaLeader.Load()
}

func (s *Server) JetStreamIsCurrent() bool {
	js := s.getJetStream()
	if js == nil {
		return false
	}
	// Grab what we need and release js lock.
	js.mu.RLock()
	var meta RaftNode
	cc := js.cluster
	if cc != nil {
		meta = cc.meta
	}
	js.mu.RUnlock()

	if cc == nil {
		// Non-clustered mode
		return true
	}
	return meta.Current()
}

func (s *Server) JetStreamSnapshotMeta() error {
	js := s.getJetStream()
	if js == nil {
		return NewJSNotEnabledError()
	}
	js.mu.RLock()
	cc := js.cluster
	isLeader := cc.isLeader()
	meta := cc.meta
	js.mu.RUnlock()

	if !isLeader {
		return errNotLeader
	}

	snap, _, _, err := js.metaSnapshot()
	if err != nil {
		return err
	}

	return meta.InstallSnapshot(snap, false)
}

func (s *Server) JetStreamStepdownStream(account, stream string) error {
	js, cc := s.getJetStreamCluster()
	if js == nil {
		return NewJSNotEnabledError()
	}
	if cc == nil {
		return NewJSClusterNotActiveError()
	}
	// Grab account
	acc, err := s.LookupAccount(account)
	if err != nil {
		return err
	}
	// Grab stream
	mset, err := acc.lookupStream(stream)
	if err != nil {
		return err
	}

	if node := mset.raftNode(); node != nil {
		node.StepDown()
	}

	return nil
}

func (s *Server) JetStreamStepdownConsumer(account, stream, consumer string) error {
	js, cc := s.getJetStreamCluster()
	if js == nil {
		return NewJSNotEnabledError()
	}
	if cc == nil {
		return NewJSClusterNotActiveError()
	}
	// Grab account
	acc, err := s.LookupAccount(account)
	if err != nil {
		return err
	}
	// Grab stream
	mset, err := acc.lookupStream(stream)
	if err != nil {
		return err
	}

	o := mset.lookupConsumer(consumer)
	if o == nil {
		return NewJSConsumerNotFoundError()
	}

	if node := o.raftNode(); node != nil {
		node.StepDown()
	}

	return nil
}

func (s *Server) JetStreamSnapshotStream(account, stream string) error {
	js, cc := s.getJetStreamCluster()
	if js == nil {
		return NewJSNotEnabledForAccountError()
	}
	if cc == nil {
		return NewJSClusterNotActiveError()
	}
	// Grab account
	acc, err := s.LookupAccount(account)
	if err != nil {
		return err
	}
	// Grab stream
	mset, err := acc.lookupStream(stream)
	if err != nil {
		return err
	}

	// Hold lock when installing snapshot.
	mset.mu.Lock()
	if mset.node == nil {
		mset.mu.Unlock()
		return nil
	}
	err = mset.node.InstallSnapshot(mset.stateSnapshotLocked(), false)
	mset.mu.Unlock()

	return err
}

func (s *Server) JetStreamClusterPeers() []string {
	js := s.getJetStream()
	if js == nil {
		return nil
	}
	js.mu.RLock()
	defer js.mu.RUnlock()

	cc := js.cluster
	if !cc.isLeader() || cc.meta == nil {
		return nil
	}
	peers := cc.meta.Peers()
	var nodes []string
	for _, p := range peers {
		si, ok := s.nodeToInfo.Load(p.ID)
		if !ok || si == nil {
			continue
		}
		ni := si.(nodeInfo)
		// Ignore if offline, no JS, or no current stats have been received.
		if ni.offline || !ni.js || ni.stats == nil {
			continue
		}
		nodes = append(nodes, si.(nodeInfo).name)
	}
	return nodes
}

// Read lock should be held.
func (cc *jetStreamCluster) isLeader() bool {
	if cc == nil {
		// Non-clustered mode
		return true
	}
	return cc.meta != nil && cc.meta.Leader()
}

// streamGroupNode returns the stream's group node and whether the group should have one.
// Read lock should be held.
func (cc *jetStreamCluster) streamGroupNode(account, stream string) (RaftNode, bool, bool) {
	as := cc.streams[account]
	if as == nil {
		return nil, false, false
	}
	sa := as[stream]
	if sa == nil {
		return nil, false, false
	}
	rg := sa.Group
	if rg == nil {
		return nil, false, false
	}
	// Mirrors createRaftGroup, a single peer group only keeps a node while a desired state is pending.
	return rg.node, rg.Desired != nil || len(rg.Peers) > 1, true
}

// isStreamHealthy will determine if the stream is up to date or very close.
// For R1 it will make sure the stream is present on this server.
func (js *jetStream) isStreamHealthy(acc *Account, sa *streamAssignment) error {
	js.mu.RLock()
	if sa != nil && sa.unsupported != nil {
		js.mu.RUnlock()
		return nil
	}
	s, cc := js.srv, js.cluster
	if cc == nil {
		// Non-clustered mode
		js.mu.RUnlock()
		return nil
	}
	if sa == nil || sa.Group == nil {
		js.mu.RUnlock()
		return errors.New("stream assignment or group missing")
	}
	// Surface any persisted assignment-level error (e.g. failed create on this
	// peer due to account limits) so the health check reflects the broken state
	// instead of falling through to runtime-only checks.
	if sa.err != nil {
		err := sa.err
		js.mu.RUnlock()
		return fmt.Errorf("stream assignment error: %w", err)
	}
	streamName := sa.Config.Name
	node := sa.Group.node
	// The config holds the target replica count while a desired state is pending,
	// but the group is still at its origin and we don't act on desired state, so
	// judge the stream against the replica count it is actually running at.
	var originReplicas int
	if o := sa.desiredOrigin(); o != nil {
		originReplicas = o.Replicas
	}
	js.mu.RUnlock()

	// First lookup stream and make sure its there.
	mset, err := acc.lookupStream(streamName)
	if err != nil {
		return errors.New("stream not found")
	}

	msetNode := mset.raftNode()
	mset.cfgMu.RLock()
	replicas := mset.cfg.Replicas
	mset.cfgMu.RUnlock()
	var nrgWerr error
	if node != nil {
		nrgWerr = node.GetWriteErr()
	}
	streamWerr := mset.getWriteErr()
	switch {
	case replicas <= 1:
		return nil // No further checks for R=1 streams

	case node == nil:
		// Still running at an origin that needs no node. Only checked here, so a group
		// that does have a node is checked as usual for the rest of the migration.
		if originReplicas > 0 && originReplicas < replicas {
			return nil
		}
		return errors.New("group node missing")

	case msetNode == nil:
		// Can happen when the stream's node is not yet initialized.
		return errors.New("stream node missing")

	case node != msetNode:
		s.Warnf("Detected stream cluster node skew '%s > %s'", acc.GetName(), streamName)
		return errors.New("cluster node skew detected")

	case nrgWerr != nil:
		return fmt.Errorf("node write error: %v", nrgWerr)

	case streamWerr != nil:
		return fmt.Errorf("stream write error: %v", streamWerr)

	case !mset.isMonitorRunning():
		return errors.New("monitor goroutine not running")

	case mset.isCatchingUp():
		return errors.New("stream catching up")

	case !node.Healthy():
		return errors.New("group node unhealthy")

	default:
		return nil
	}
}

// isConsumerHealthy will determine if the consumer is up to date.
// For R1 it will make sure the consumer is present on this server.
func (js *jetStream) isConsumerHealthy(mset *stream, sa *streamAssignment, consumer string, ca *consumerAssignment) error {
	js.mu.RLock()
	if ca != nil && ca.unsupported != nil {
		js.mu.RUnlock()
		return nil
	}
	if mset == nil {
		js.mu.RUnlock()
		return errors.New("stream missing")
	}
	s, cc := js.srv, js.cluster
	if cc == nil {
		// Non-clustered mode
		js.mu.RUnlock()
		return nil
	}
	if ca == nil || ca.Group == nil {
		js.mu.RUnlock()
		return errors.New("consumer assignment or group missing")
	}
	// Surface any persisted assignment-level error (e.g. failed create on this
	// peer) so the health check reflects the broken state instead of falling
	// through to runtime-only checks.
	if ca.err != nil {
		err := ca.err
		js.mu.RUnlock()
		return fmt.Errorf("consumer assignment error: %w", err)
	}
	created := ca.Created
	node := ca.Group.node
	// The stream may be running at its origin while its config already carries the
	// target replica count. A consumer that has not been mapped onto the new peers
	// yet inherits that target, so fall back on the origin count below.
	var originReplicas int
	if o := sa.desiredOrigin(); o != nil {
		originReplicas = o.Replicas
	}
	js.mu.RUnlock()

	// Check if not running at all.
	o := mset.lookupConsumer(consumer)
	if o == nil {
		if time.Since(created) < 5*time.Second {
			// No further checks, consumer is not available yet but should be soon.
			// We'll start erroring once we're sure this consumer is actually broken.
			return nil
		}
		return errors.New("consumer not found")
	}

	oNode := o.raftNode()
	rc, _ := o.replica()
	var nrgWerr error
	if node != nil {
		nrgWerr = node.GetWriteErr()
	}
	switch {
	case rc <= 1:
		return nil // No further checks for R=1 consumers

	case node == nil:
		// Not mapped onto the new peers yet, so still running at the stream's origin
		// count. Only checked here, so a consumer that does have a node is checked as
		// usual for the rest of the migration.
		if originReplicas > 0 && originReplicas < rc {
			return nil
		}
		return errors.New("group node missing")

	case oNode == nil:
		// Can happen when the consumer's node is not yet initialized.
		return errors.New("consumer node missing")

	case node != oNode:
		mset.mu.RLock()
		accName, streamName := mset.acc.GetName(), mset.cfg.Name
		mset.mu.RUnlock()
		s.Warnf("Detected consumer cluster node skew '%s > %s > %s'", accName, streamName, consumer)
		return errors.New("cluster node skew detected")

	case nrgWerr != nil:
		return fmt.Errorf("node write error: %v", nrgWerr)

	case !o.isMonitorRunning():
		return errors.New("monitor goroutine not running")

	case !node.Healthy():
		return errors.New("group node unhealthy")

	default:
		return nil
	}
}

// subjectsOverlap checks all existing stream assignments for the account cross-cluster for subject overlap
// Use only for clustered JetStream
// Read lock should be held.
func (js *jetStream) subjectsOverlap(acc string, subjects []string, osa *streamAssignment) bool {
	for sa := range js.streamAssignmentsOrInflightSeq(acc) {
		// can't overlap yourself, assume osa pre-checked for deep equal if passed
		if osa != nil && sa.Config.Name == osa.Config.Name {
			continue
		}
		for _, subj := range sa.Config.Subjects {
			for _, tsubj := range subjects {
				if SubjectsCollide(tsubj, subj) {
					return true
				}
			}
		}
	}
	return false
}

func (a *Account) getJetStreamFromAccount() (*Server, *jetStream, *jsAccount) {
	a.mu.RLock()
	jsa := a.js
	a.mu.RUnlock()
	if jsa == nil {
		return nil, nil, nil
	}
	jsa.mu.RLock()
	js := jsa.js
	jsa.mu.RUnlock()
	if js == nil {
		return nil, nil, nil
	}
	// Lock not needed, set on creation.
	s := js.srv
	return s, js, jsa
}

func (s *Server) JetStreamIsStreamLeader(account, stream string) bool {
	js, cc := s.getJetStreamCluster()
	if js == nil || cc == nil {
		return false
	}
	js.mu.RLock()
	defer js.mu.RUnlock()
	return cc.isStreamLeader(account, stream)
}

func (a *Account) JetStreamIsStreamLeader(stream string) bool {
	s, js, jsa := a.getJetStreamFromAccount()
	if s == nil || js == nil || jsa == nil {
		return false
	}
	js.mu.RLock()
	defer js.mu.RUnlock()
	return js.cluster.isStreamLeader(a.Name, stream)
}

func (s *Server) JetStreamIsStreamCurrent(account, stream string) bool {
	js, cc := s.getJetStreamCluster()
	if js == nil {
		return false
	}
	if cc == nil {
		// Non-clustered mode
		return true
	}
	js.mu.RLock()
	node, needsNode, ok := cc.streamGroupNode(account, stream)
	js.mu.RUnlock()
	if !ok || (needsNode && node == nil) {
		return false
	}
	if node != nil && !node.Current() {
		return false
	}

	acc, err := s.LookupAccount(account)
	if err != nil {
		return false
	}
	mset, err := acc.lookupStream(stream)
	if err != nil {
		return false
	}
	// The stream only uses the group node once it's been assigned.
	if mset.raftNode() != node {
		return false
	}
	// Check if we are processing a snapshot and are catching up.
	return !mset.isCatchingUp()
}

func (a *Account) JetStreamIsConsumerLeader(stream, consumer string) bool {
	s, js, jsa := a.getJetStreamFromAccount()
	if s == nil || js == nil || jsa == nil {
		return false
	}
	js.mu.RLock()
	defer js.mu.RUnlock()
	return js.cluster.isConsumerLeader(a.Name, stream, consumer)
}

func (s *Server) JetStreamIsConsumerLeader(account, stream, consumer string) bool {
	js, cc := s.getJetStreamCluster()
	if js == nil || cc == nil {
		return false
	}
	js.mu.RLock()
	defer js.mu.RUnlock()
	return cc.isConsumerLeader(account, stream, consumer)
}

func (s *Server) enableJetStreamClustering() error {
	if !s.isRunning() {
		return nil
	}
	js := s.getJetStream()
	if js == nil {
		return NewJSNotEnabledForAccountError()
	}
	// Already set.
	if js.cluster != nil {
		return nil
	}

	s.Noticef("Starting JetStream cluster")
	// We need to determine if we have a stable cluster name and expected number of servers.
	s.Debugf("JetStream cluster checking for stable cluster name and peers")

	hasLeafNodeSystemShare := s.canExtendOtherDomain()
	if s.isClusterNameDynamic() && !hasLeafNodeSystemShare {
		return errors.New("JetStream cluster requires cluster name")
	}
	if s.configuredRoutes() == 0 && !hasLeafNodeSystemShare {
		return errors.New("JetStream cluster requires configured routes or solicited leafnode for the system account")
	}

	return js.setupMetaGroup()
}

// isClustered returns if we are clustered.
// Lock should not be held.
func (js *jetStream) isClustered() bool {
	// This is only ever set, no need for lock here.
	return js.cluster != nil
}

// isClusteredNoLock returns if we are clustered, but unlike isClustered() does
// not use the jetstream's lock, instead, uses an atomic operation.
// There are situations where some code wants to know if we are clustered but
// can't use js.isClustered() without causing a lock inversion.
func (js *jetStream) isClusteredNoLock() bool {
	return atomic.LoadInt32(&js.clustered) == 1
}

func (js *jetStream) setupMetaGroup() error {
	s := js.srv
	s.Noticef("Creating JetStream metadata controller")

	// Setup our WAL for the metagroup.
	sysAcc := s.SystemAccount()
	if sysAcc == nil {
		return ErrNoSysAccount
	}
	storeDir := filepath.Join(js.config.StoreDir, sysAcc.Name, defaultStoreDirName, defaultMetaGroupName)

	js.srv.optsMu.RLock()
	syncAlways := js.srv.opts.SyncAlways
	syncInterval := js.srv.opts.SyncInterval
	js.srv.optsMu.RUnlock()
	fs, err := newFileStoreWithCreated(
		FileStoreConfig{StoreDir: storeDir, BlockSize: defaultMetaFSBlkSize, AsyncFlush: false, SyncAlways: syncAlways, SyncInterval: syncInterval, srv: s},
		StreamConfig{Name: defaultMetaGroupName, Storage: FileStorage},
		time.Now().UTC(),
		s.jsKeyGen(s.getOpts().JetStreamKey, defaultMetaGroupName),
		s.jsKeyGen(s.getOpts().JetStreamOldKey, defaultMetaGroupName),
	)
	if err != nil {
		s.Errorf("Error creating filestore: %v", err)
		return err
	}

	cfg := &RaftConfig{Name: defaultMetaGroupName, Store: storeDir, Log: fs, Recovering: true}

	// If we are soliciting leafnode connections and we are sharing a system account and do not disable it with a hint,
	// we want to move to observer mode so that we extend the solicited cluster or supercluster but do not form our own.
	cfg.Observer = s.canExtendOtherDomain() && s.getOpts().JetStreamExtHint != jsNoExtend

	var bootstrap bool
	if ps, err := readPeerState(s.diskIOSemaphore(), storeDir); err != nil {
		s.Noticef("JetStream cluster bootstrapping")
		bootstrap = true
		peers := s.ActivePeers()
		s.Debugf("JetStream cluster initial peers: %+v", peers)
		if err := s.bootstrapRaftNode(cfg, peers, false); err != nil {
			fs.Stop()
			return err
		}
		if cfg.Observer {
			s.Noticef("Turning JetStream metadata controller Observer Mode on")
			s.Noticef("In cases where the JetStream domain is not intended to be extended through a SYS account leaf node connection")
			s.Noticef("and waiting for leader election until first contact is not acceptable,")
			s.Noticef(`manually disable Observer Mode by setting the JetStream Option "extension_hint: %s"`, jsNoExtend)
		}
	} else {
		s.Noticef("JetStream cluster recovering state")
		// correlate the value of observer with observations from a previous run.
		if cfg.Observer {
			switch ps.domainExt {
			case extExtended:
				s.Noticef("Keeping JetStream metadata controller Observer Mode on - due to previous contact")
			case extNotExtended:
				s.Noticef("Turning JetStream metadata controller Observer Mode off - due to previous contact")
				cfg.Observer = false
			case extUndetermined:
				s.Noticef("Turning JetStream metadata controller Observer Mode on - no previous contact")
				s.Noticef("In cases where the JetStream domain is not intended to be extended through a SYS account leaf node connection")
				s.Noticef("and waiting for leader election until first contact is not acceptable,")
				s.Noticef(`manually disable Observer Mode by setting the JetStream Option "extension_hint: %s"`, jsNoExtend)
			}
		} else {
			// To track possible configuration changes, responsible for an altered value of cfg.Observer,
			// set extension state to undetermined.
			ps.domainExt = extUndetermined
			if err := writePeerState(s.diskIOSemaphore(), storeDir, ps); err != nil {
				fs.Stop()
				return err
			}
		}
	}

	// Start up our meta node.
	n, err := s.startRaftNode(sysAcc.GetName(), cfg, pprofLabels{
		"type":    "metaleader",
		"account": sysAcc.Name,
	})
	if err != nil {
		s.Warnf("Could not start metadata controller: %v", err)
		return err
	}

	// If we are bootstrapped with no state, start campaign early.
	if bootstrap {
		n.Campaign()
	}

	c := s.createInternalJetStreamClient()
	qch, stopped := make(chan struct{}), make(chan struct{})

	js.mu.Lock()
	defer js.mu.Unlock()
	js.cluster = &jetStreamCluster{
		meta:    n,
		streams: make(map[string]map[string]*streamAssignment),
		s:       s,
		c:       c,
		qch:     qch,
		stopped: stopped,
	}
	atomic.StoreInt32(&js.clustered, 1)
	c.registerWithAccount(sysAcc)

	// Set to true before we start.
	js.metaRecovering = true
	// Pass the node and channels to the monitor, shutdownJetStream can clear
	// them from js.cluster before the monitor goroutine first runs.
	if !js.srv.startGoRoutine(
		func() { js.monitorCluster(n, qch, stopped) },
		pprofLabels{
			"type":    "metaleader",
			"account": sysAcc.Name,
		},
	) {
		// Shutting down, shutdownJetStream must not wait for a monitor that never ran.
		close(stopped)
	}
	return nil
}

func (js *jetStream) getMetaGroup() RaftNode {
	js.mu.RLock()
	defer js.mu.RUnlock()
	if js.cluster == nil {
		return nil
	}
	return js.cluster.meta
}

func (js *jetStream) server() *Server {
	// Lock not needed, only set once on creation.
	return js.srv
}

// Will respond if we do not think we have a metacontroller leader.
func (js *jetStream) isLeaderless() bool {
	js.mu.RLock()
	cc := js.cluster
	if cc == nil || cc.meta == nil {
		js.mu.RUnlock()
		return false
	}
	meta := cc.meta
	js.mu.RUnlock()

	// If we don't have a leader.
	// Make sure we have been running for enough time.
	if meta.Leaderless() && time.Since(meta.Created()) > lostQuorumIntervalDefault {
		return true
	}
	return false
}

// Will respond iff we are a member and we know we have no leader.
func (js *jetStream) isGroupLeaderless(rg *raftGroup) bool {
	if rg == nil || js == nil {
		return false
	}
	js.mu.RLock()
	cc := js.cluster
	started := js.started

	// If we are not a member we can not say..
	if cc.meta == nil {
		js.mu.RUnlock()
		return false
	}
	if !rg.isMember(cc.meta.ID()) {
		js.mu.RUnlock()
		return false
	}
	// Single peer groups always have a leader if we are here.
	if rg.node == nil {
		js.mu.RUnlock()
		return false
	}
	node := rg.node
	js.mu.RUnlock()
	// If we don't have a leader.
	if node.Leaderless() {
		// Threshold for jetstream startup.
		const startupThreshold = 10 * time.Second

		if node.HadPreviousLeader() {
			// Make sure we have been running long enough to intelligently determine this.
			if time.Since(started) > startupThreshold {
				return true
			}
		}
		// Make sure we have been running for enough time.
		if time.Since(node.Created()) > lostQuorumIntervalDefault {
			return true
		}
	}

	return false
}

func (s *Server) JetStreamIsStreamAssigned(account, stream string) bool {
	js, cc := s.getJetStreamCluster()
	if js == nil || cc == nil {
		return false
	}
	acc, _ := s.LookupAccount(account)
	if acc == nil {
		return false
	}
	js.mu.RLock()
	assigned := cc.isStreamAssigned(acc, stream)
	js.mu.RUnlock()
	return assigned
}

// streamAssigned informs us if this server has this stream assigned.
func (jsa *jsAccount) streamAssigned(stream string) bool {
	jsa.mu.RLock()
	js, acc := jsa.js, jsa.account
	jsa.mu.RUnlock()

	if js == nil {
		return false
	}
	js.mu.RLock()
	assigned := js.cluster.isStreamAssigned(acc, stream)
	js.mu.RUnlock()
	return assigned
}

// Read lock should be held.
func (cc *jetStreamCluster) isStreamAssigned(a *Account, stream string) bool {
	// Non-clustered mode always return true.
	if cc == nil {
		return true
	}
	if cc.meta == nil {
		return false
	}
	as := cc.streams[a.Name]
	if as == nil {
		return false
	}
	sa := as[stream]
	if sa == nil {
		return false
	}
	return sa.Group.isMember(cc.meta.ID())
}

// Read lock should be held.
func (cc *jetStreamCluster) isStreamLeader(account, stream string) bool {
	// Non-clustered mode always return true.
	if cc == nil {
		return true
	}
	if cc.meta == nil {
		return false
	}

	var sa *streamAssignment
	if as := cc.streams[account]; as != nil {
		sa = as[stream]
	}
	if sa == nil {
		return false
	}
	rg := sa.Group
	if rg == nil {
		return false
	}
	// Check if we are the leader of this raftGroup assigned to the stream.
	ourID := cc.meta.ID()
	for _, peer := range rg.Peers {
		if peer == ourID {
			if len(rg.Peers) == 1 || (rg.node != nil && rg.node.Leader()) {
				return true
			}
		}
	}
	return false
}

// Read lock should be held.
func (cc *jetStreamCluster) isConsumerLeader(account, stream, consumer string) bool {
	// Non-clustered mode always return true.
	if cc == nil {
		return true
	}
	if cc.meta == nil {
		return false
	}

	var sa *streamAssignment
	if as := cc.streams[account]; as != nil {
		sa = as[stream]
	}
	if sa == nil {
		return false
	}
	// Check if we are the leader of this raftGroup assigned to this consumer.
	ca := sa.consumers[consumer]
	if ca == nil {
		return false
	}
	rg := ca.Group
	ourID := cc.meta.ID()
	for _, peer := range rg.Peers {
		if peer == ourID {
			if len(rg.Peers) == 1 || (rg.node != nil && rg.node.Leader()) {
				return true
			}
		}
	}
	return false
}

// Track the stream for the account `accName` in the inflight proposals map.
// This is done after proposing a stream change.
// (Write) Lock held on entry.
func (cc *jetStreamCluster) trackInflightStreamProposal(accName string, sa *streamAssignment, deleted bool) {
	if cc.inflightStreams == nil {
		cc.inflightStreams = make(map[string]map[string]*inflightStreamInfo)
	}
	streams, ok := cc.inflightStreams[accName]
	if !ok {
		streams = make(map[string]*inflightStreamInfo)
		cc.inflightStreams[accName] = streams
	}
	if inflight, ok := streams[sa.Config.Name]; ok {
		inflight.ops++
		inflight.deleted = deleted
		inflight.streamAssignment = sa
	} else {
		streams[sa.Config.Name] = &inflightStreamInfo{1, deleted, sa}
	}
}

// Remove the stream `streamName` for the account `accName` from the inflight proposals map.
// This is done after successful create/update/delete.
// (Write) Lock held on entry.
func (cc *jetStreamCluster) removeInflightStreamProposal(accName, streamName string) {
	if streams, ok := cc.inflightStreams[accName]; !ok {
		return // No accounts.
	} else if inflight, ok := streams[streamName]; !ok {
		return // No changes for this stream.
	} else if inflight.ops > 1 {
		// Decrement one pending operation.
		inflight.ops--
	} else {
		// No pending operations left, clean up.
		delete(streams, streamName)
		if len(streams) == 0 {
			delete(cc.inflightStreams, accName)
		}
	}
}

// Track the consumer for the `streamName` and account `accName` in the inflight proposals map.
// This is done after proposing a consumer change.
// (Write) Lock held on entry.
func (cc *jetStreamCluster) trackInflightConsumerProposal(accName, streamName string, ca *consumerAssignment, deleted bool) {
	if cc.inflightConsumers == nil {
		cc.inflightConsumers = make(map[string]map[string]map[string]*inflightConsumerInfo)
	}
	streams, ok := cc.inflightConsumers[accName]
	if !ok {
		streams = make(map[string]map[string]*inflightConsumerInfo)
		cc.inflightConsumers[accName] = streams
	}
	consumers, ok := streams[streamName]
	if !ok {
		consumers = make(map[string]*inflightConsumerInfo)
		streams[streamName] = consumers
	}
	if inflight, ok := consumers[ca.Name]; ok {
		inflight.ops++
		inflight.deleted = deleted
		inflight.consumerAssignment = ca
	} else {
		consumers[ca.Name] = &inflightConsumerInfo{1, deleted, ca}
	}
}

// Remove the consumer `consumerName` for the `streamName` and account `accName` from the inflight proposals map.
// This is done after successful create/update/delete.
// (Write) Lock held on entry.
func (cc *jetStreamCluster) removeInflightConsumerProposal(accName, streamName, consumerName string) {
	if streams, ok := cc.inflightConsumers[accName]; !ok {
		return // No accounts.
	} else if consumers, ok := streams[streamName]; !ok {
		return // No streams.
	} else if inflight, ok := consumers[consumerName]; !ok {
		return // No changes for this consumer.
	} else if inflight.ops > 1 {
		// Decrement one pending operation.
		inflight.ops--
	} else {
		// No pending operations left, clean up.
		delete(consumers, consumerName)
		if len(consumers) == 0 {
			delete(streams, streamName)
		}
		if len(streams) == 0 {
			delete(cc.inflightConsumers, accName)
		}
	}
}

// Return the cluster quit chan.
func (js *jetStream) clusterQuitC() chan struct{} {
	js.mu.RLock()
	defer js.mu.RUnlock()
	if js.cluster != nil {
		return js.cluster.qch
	}
	return nil
}

// Mark that the meta layer is recovering.
func (js *jetStream) setMetaRecovering() {
	js.mu.Lock()
	defer js.mu.Unlock()
	if js.cluster != nil {
		// metaRecovering
		js.metaRecovering = true
	}
}

// Mark that the meta layer is no longer recovering.
func (js *jetStream) clearMetaRecovering() {
	js.mu.Lock()
	defer js.mu.Unlock()
	js.metaRecovering = false
}

// Return whether the meta layer is recovering.
func (js *jetStream) isMetaRecovering() bool {
	js.mu.RLock()
	defer js.mu.RUnlock()
	return js.metaRecovering
}

// During recovery track any stream and consumer delete and update operations.
type recoveryUpdates struct {
	removeStreams   map[string]*streamAssignment
	removeConsumers map[string]map[string]*consumerAssignment
	addStreams      map[string]*streamAssignment
	updateStreams   map[string]*streamAssignment
	updateConsumers map[string]map[string]*consumerAssignment
}

func (ru *recoveryUpdates) removeStream(sa *streamAssignment) {
	key := sa.recoveryKey()
	ru.removeStreams[key] = sa
	delete(ru.addStreams, key)
	delete(ru.updateStreams, key)
	delete(ru.updateConsumers, key)
	delete(ru.removeConsumers, key)
}

func (ru *recoveryUpdates) addStream(sa *streamAssignment) {
	key := sa.recoveryKey()
	ru.addStreams[key] = sa
	// A fresh add supersedes any earlier staged update for this stream.
	delete(ru.updateStreams, key)
}

func (ru *recoveryUpdates) updateStream(sa *streamAssignment) {
	key := sa.recoveryKey()
	ru.updateStreams[key] = sa
}

func (ru *recoveryUpdates) removeConsumer(ca *consumerAssignment) {
	key := ca.recoveryKey()
	skey := ca.streamRecoveryKey()
	if _, ok := ru.removeConsumers[skey]; !ok {
		ru.removeConsumers[skey] = map[string]*consumerAssignment{}
	}
	// Keep the first staged removal; it's for the consumer we hold. Later
	// removals are for a same-name recreate, and processConsumerRemoval
	// ignores a removal whose raft group doesn't match ours.
	if _, ok := ru.removeConsumers[skey][key]; !ok {
		ru.removeConsumers[skey][key] = ca
	}
	if consumers, ok := ru.updateConsumers[skey]; ok {
		delete(consumers, key)
	}
}

func (ru *recoveryUpdates) addOrUpdateConsumer(ca *consumerAssignment) {
	key := ca.recoveryKey()
	skey := ca.streamRecoveryKey()
	if _, ok := ru.updateConsumers[skey]; !ok {
		ru.updateConsumers[skey] = map[string]*consumerAssignment{}
	}
	ru.updateConsumers[skey][key] = ca
}

// Called after recovery of the cluster on startup to check for any orphans.
// Streams and consumers are recovered from disk, and the meta layer's mappings
// should clean them up, but under crash scenarios there could be orphans.
func (js *jetStream) checkForOrphans() {
	// Can not hold jetstream lock while trying to delete streams or consumers.
	js.mu.Lock()
	s, cc := js.srv, js.cluster
	s.Debugf("JetStream cluster checking for orphans")

	// We only want to cleanup any orphans if we know we are current with the meta-leader.
	meta := cc.meta
	if meta == nil || meta.Leaderless() {
		js.mu.Unlock()
		s.Debugf("JetStream cluster skipping check for orphans, no meta-leader")
		return
	}
	if !meta.Healthy() {
		js.mu.Unlock()
		s.Debugf("JetStream cluster skipping check for orphans, not current with the meta-leader")
		return
	}
	streams, consumers := js.getOrphans()
	js.mu.Unlock()

	for _, mset := range streams {
		mset.mu.RLock()
		accName, stream := mset.acc.Name, mset.cfg.Name
		mset.mu.RUnlock()
		s.Warnf("Detected orphaned stream '%s > %s', will cleanup", accName, stream)
		if err := mset.delete(); err != nil {
			s.Warnf("Deleting stream encountered an error: %v", err)
		}
	}
	for _, o := range consumers {
		o.mu.RLock()
		accName, mset, consumer := o.acc.Name, o.mset, o.name
		o.mu.RUnlock()
		stream := "N/A"
		if mset != nil {
			mset.mu.RLock()
			stream = mset.cfg.Name
			mset.mu.RUnlock()
		}
		if o.isDurable() {
			s.Warnf("Detected orphaned durable consumer '%s > %s > %s', will cleanup", accName, stream, consumer)
		} else {
			s.Debugf("Detected orphaned consumer '%s > %s > %s', will cleanup", accName, stream, consumer)
		}

		if err := o.delete(); err != nil {
			s.Warnf("Deleting consumer encountered an error: %v", err)
		}
	}
}

// Returns orphaned streams and consumers that were recovered from disk, but don't
// exist as clustered stream/consumer assignments.
// Lock should be held.
func (js *jetStream) getOrphans() (streams []*stream, consumers []*consumer) {
	cc := js.cluster
	for accName, jsa := range js.accounts {
		asa := cc.streams[accName]
		jsa.mu.RLock()
		for stream, mset := range jsa.streams {
			if sa := asa[stream]; sa == nil {
				streams = append(streams, mset)
			} else {
				// This one is good, check consumers now.
				for _, o := range mset.getPublicConsumers() {
					if sa.consumers[o.String()] == nil {
						consumers = append(consumers, o)
					}
				}
			}
		}
		jsa.mu.RUnlock()
	}
	return streams, consumers
}

func (js *jetStream) monitorCluster(n RaftNode, qch, stopped chan struct{}) {
	s := js.server()
	rqch, lch, aq := n.QuitC(), n.LeadChangeC(), n.ApplyQ()

	defer s.grWG.Done()
	defer close(stopped)

	s.Debugf("Starting metadata monitor")
	defer s.Debugf("Exiting metadata monitor")

	// Make sure to stop the raft group on exit to prevent accidental memory bloat.
	defer n.Stop()
	defer s.isMetaLeader.Store(false)

	const compactInterval = time.Minute
	const compactMinInterval = 15 * time.Second
	t := time.NewTicker(compactInterval)
	defer t.Stop()

	// Used to check cold boot cluster when possibly in mixed mode.
	const leaderCheckInterval = time.Second
	lt := time.NewTicker(leaderCheckInterval)
	defer lt.Stop()

	// Check the general health once an hour.
	const healthCheckInterval = 1 * time.Hour
	ht := time.NewTicker(healthCheckInterval)
	defer ht.Stop()

	// Utility to check health.
	checkHealth := func() {
		if hs := s.healthz(nil); hs.Error != _EMPTY_ {
			s.Warnf("%v", hs.Error)
		}
	}

	var (
		isLeader       bool
		lastSnapTime   time.Time
		compactSizeMin = uint64(8 * 1024 * 1024) // 8MB
		minSnapDelta   = 30 * time.Second
	)

	// Highwayhash key for generating hashes.
	key := make([]byte, 32)
	crand.Read(key)

	// Set to true to start.
	js.setMetaRecovering()
	recovering := true

	// Snapshotting function.
	var (
		snapMu       sync.Mutex
		snapshotting bool
		// The first snapshot we do after recovery should clean up most of the log.
		fallbackSnapshot = true
		// Suppress non-forced snapshots after an error. Timer-based snapshots will become forced.
		// If failures continue, we force a snapshot even if we were catching up others
		// (otherwise we might indefinitely stall and grow log size).
		failedSnapshots atomic.Uint32
	)
	doSnapshot := func(force bool) {
		// Suppress during recovery.
		// If snapshots have failed, and we're not forced to, we'll wait for the timer since it'll now be forced.
		if recovering || (!force && failedSnapshots.Load() > 0) {
			return
		}
		// Suppress if an async snapshot is already in progress.
		snapMu.Lock()
		if snapshotting {
			snapMu.Unlock()
			return
		}
		// Look up what the threshold is for compaction. Re-reading from config here as it is reloadable.
		js.srv.optsMu.RLock()
		ethresh := js.srv.opts.JetStreamMetaCompact
		szthresh := js.srv.opts.JetStreamMetaCompactSize
		// Allows reverting to sync/blocking snapshots instead of the default async snapshots.
		syncSnapshot := js.srv.opts.JetStreamMetaCompactSync
		js.srv.optsMu.RUnlock()
		// Work out our criteria for snapshotting.
		byEntries, bySize := ethresh > 0, szthresh > 0
		byNeither := !byEntries && !bySize
		// For the meta layer we want to snapshot when over the above threshold (which could be 0 by default).
		ne, nsz := n.Size()
		createSnapshot := force || byNeither || (byEntries && ne > ethresh) || (bySize && nsz > szthresh) || n.NeedSnapshot()
		if !createSnapshot {
			snapMu.Unlock()
			return
		}

		// Start a checkpoint, we can either install the snapshot sync or async from here.
		// If we had a significant number of failed snapshots, start relaxing Raft-layer checks
		// to force it through. We might have been catching up a peer for a long period, and this
		// protects our log size from growing indefinitely.
		forceSnapshot := failedSnapshots.Load() > 4
		c, err := n.CreateSnapshotCheckpoint(forceSnapshot)
		if err != nil {
			if err != errNoSnapAvailable && err != errNodeClosed {
				s.Warnf("Error snapshotting JetStream cluster state: %v", err)
				// If this is the first failure, reduce the interval of the snapshot timer.
				// This ensures we're not waiting too long for snapshotting to eventually become forced.
				if failedSnapshots.Load() == 0 {
					t.Reset(compactMinInterval)
				}
				failedSnapshots.Add(1)
			}
			snapMu.Unlock()
			return
		}

		// If we're meant to install an async snapshot, check if the underlying log has grown way too large.
		// In that case, we fall back to a synchronous/blocking snapshot after all. This is a protective
		// measure to prevent the log from growing faster than we can asynchronously compact.
		if !fallbackSnapshot && !syncSnapshot {
			if szthresh == 0 {
				szthresh = compactSizeMin
			}
			if thresh := 10 * szthresh; nsz >= thresh {
				s.rateLimitFormatWarnf("JetStream cluster metalayer log size has exceeded async threshold (%s), will fall back to blocking snapshot", friendlyBytes(thresh))
				fallbackSnapshot = true
			}
		}

		// Check if we should fall back to a blocking snapshot, either due to failures or extreme log size.
		if fallbackSnapshot || syncSnapshot {
			if !syncSnapshot {
				s.rateLimitFormatWarnf("Metalayer blocking snapshot starting")
			}
			start := time.Now()
			snap, nsa, nca, err := js.metaSnapshot()
			if err != nil {
				s.Warnf("Error generating JetStream cluster snapshot: %v", err)
				c.Abort()
			} else if csz, err := c.InstallSnapshot(snap); err == nil {
				lastSnapTime = time.Now()
				// If there was a failed snapshot before, we reduced the timer's interval.
				// Reset it back to the original interval now.
				if failedSnapshots.Load() > 0 {
					t.Reset(compactInterval)
				}
				failedSnapshots.Store(0)
				// Fallback snapshot was successful, the next one can be async again.
				fallbackSnapshot = false
				took := time.Since(start)
				if !syncSnapshot || took > 2*time.Second {
					s.rateLimitFormatWarnf("Metalayer blocking snapshot took %.3fs (streams: %d, consumers: %d, compacted: %s)",
						took.Seconds(), nsa, nca, friendlyBytes(csz))
				}
			} else {
				c.Abort()
				if err != errNoSnapAvailable && err != errNodeClosed && err != errSnapAborted {
					s.Warnf("Error snapshotting JetStream cluster state: %v", err)
					// If this is the first failure, reduce the interval of the snapshot timer.
					// This ensures we're not waiting too long for snapshotting to eventually become forced.
					if failedSnapshots.Load() == 0 {
						t.Reset(compactMinInterval)
					}
					failedSnapshots.Add(1)
				}
			}
			snapMu.Unlock()
			return
		}

		// Perform the snapshot asynchronously.
		start := time.Now()
		snapshotting = true
		snapMu.Unlock()
		s.startGoRoutine(func() {
			defer s.grWG.Done()

			abort := func(err error) {
				snapMu.Lock()
				c.Abort()
				if err != errNoSnapAvailable && err != errNodeClosed && err != errSnapAborted {
					s.Warnf("Error snapshotting JetStream cluster state: %v, will fall back to blocking snapshot", err)
					fallbackSnapshot = true
					// If this is the first failure, reduce the interval of the snapshot timer.
					// This ensures we're not waiting too long for snapshotting to eventually become forced.
					if failedSnapshots.Load() == 0 {
						t.Reset(compactMinInterval)
					}
					failedSnapshots.Add(1)
				}
				snapshotting = false
				snapMu.Unlock()
			}

			// The strategy of asynchronous snapshotting:
			// - Not holding any locks. Only minimal for the Raft node itself, importantly not the JS lock.
			// - Load the last snapshot.
			// - Replay stream/consumer changes through the assignment state machine.
			// - Encode and install as the new snapshot.
			if data, err := c.LoadLastSnapshot(); err != nil && err != errNoSnapAvailable {
				abort(err)
			} else if streams, err := js.decodeMetaSnapshot(data); err != nil {
				abort(err)
			} else if err = js.collectStreamAndConsumerChanges(c, streams); err != nil {
				abort(err)
			} else if snap, nsa, nca, err := js.encodeMetaSnapshot(streams); err != nil {
				abort(err)
			} else if csz, err := c.InstallSnapshot(snap); err != nil {
				abort(err)
			} else {
				// Successful snapshot.
				// If there was a failed snapshot before, we reduced the timer's interval.
				// Reset it back to the original interval now.
				if failedSnapshots.Load() > 0 {
					t.Reset(compactInterval)
				}
				failedSnapshots.Store(0)
				if took := time.Since(start); took > 2*time.Second {
					s.rateLimitFormatWarnf("Metalayer async snapshot took %.3fs (streams: %d, consumers: %d, compacted: %s)",
						took.Seconds(), nsa, nca, friendlyBytes(csz))
				}
				snapMu.Lock()
				lastSnapTime = time.Now()
				snapshotting = false
				snapMu.Unlock()
			}
		})
	}

	var ru *recoveryUpdates

	// Make sure to cancel any pending checkForOrphans calls if the
	// monitor goroutine exits.
	var oc *time.Timer
	defer stopAndClearTimer(&oc)

	for {
		select {
		case <-s.quitCh:
			// Server shutting down, but we might receive this before qch, so try to snapshot.
			snapMu.Lock()
			fallbackSnapshot = true
			snapMu.Unlock()
			doSnapshot(false)
			return
		case <-rqch:
			// Raft node is closed, no use in trying to snapshot.
			return
		case <-qch:
			// Clean signal from shutdown routine so do best effort attempt to snapshot meta layer.
			snapMu.Lock()
			fallbackSnapshot = true
			snapMu.Unlock()
			doSnapshot(false)
			return
		case <-aq.ch:
			ces := aq.pop()
			for _, ce := range ces {
				if recovering && ru == nil {
					ru = &recoveryUpdates{
						removeStreams:   make(map[string]*streamAssignment),
						removeConsumers: make(map[string]map[string]*consumerAssignment),
						addStreams:      make(map[string]*streamAssignment),
						updateStreams:   make(map[string]*streamAssignment),
						updateConsumers: make(map[string]map[string]*consumerAssignment),
					}
				}
				if ce == nil {
					if ru != nil {
						// Process any removes that are still valid after recovery.
						for _, cas := range ru.removeConsumers {
							for _, ca := range cas {
								js.processConsumerRemoval(ca)
							}
						}
						for _, sa := range ru.removeStreams {
							js.processStreamRemoval(sa)
						}
						// Process stream additions.
						for _, sa := range ru.addStreams {
							js.processStreamAssignment(sa)
						}
						// Process pending updates.
						for _, sa := range ru.updateStreams {
							js.processUpdateStreamAssignment(sa)
						}
						// Now consumers.
						for _, cas := range ru.updateConsumers {
							for _, ca := range cas {
								js.processConsumerAssignment(ca)
							}
						}
					}
					// Signals we have replayed all of our metadata.
					wasMetaRecovering := js.isMetaRecovering()
					js.clearMetaRecovering()
					recovering = false
					// Clear.
					ru = nil
					s.Debugf("Recovered JetStream cluster metadata")
					// Snapshot now so we start with freshly compacted log.
					doSnapshot(true)
					if wasMetaRecovering {
						// Reset, it could be we didn't need to install a snapshot. This ensures we don't degrade
						// to a blocking snapshot if we install our first snapshot during normal operations.
						snapMu.Lock()
						fallbackSnapshot = false
						snapMu.Unlock()
						oc = time.AfterFunc(30*time.Second, js.checkForOrphans)
						// Do a health check here as well.
						go checkHealth()
					}
					continue
				}
				if isRecovering, didSnap, err := js.applyMetaEntries(ce.Entries, ru); err == nil {
					var nb uint64
					// Some entries can fail without an error when shutting down, don't move applied forward.
					if !js.isShuttingDown() {
						_, nb = n.Applied(ce.Index)
					}
					if js.hasPeerEntries(ce.Entries) || (didSnap && !isLeader) {
						doSnapshot(true)
					} else if nb > compactSizeMin {
						snapMu.Lock()
						expired := time.Since(lastSnapTime) > minSnapDelta
						snapMu.Unlock()
						if expired {
							doSnapshot(false)
						}
					}
					recovering = isRecovering
				} else {
					s.Warnf("Error applying JetStream cluster entries: %v", err)
				}
				ce.ReturnToPool()
			}
			aq.recycle(&ces)

		case lc := <-lch:
			isLeader = lc.isLeader
			// Process the change.
			js.processLeaderChange(isLeader, lc.term)
			if isLeader {
				s.sendInternalMsgLocked(serverStatsPingReqSubj, _EMPTY_, nil, nil)
				// Install a snapshot as we become leader.
				js.checkClusterSize()
				doSnapshot(false)
			}

		case <-t.C:
			// Start forcing snapshots if they failed previously.
			forceIfFailed := failedSnapshots.Load() > 0
			doSnapshot(forceIfFailed)
			// Periodically check the cluster size.
			if n.Leader() {
				js.checkClusterSize()
			}
		case <-ht.C:
			// Do this in a separate go routine.
			go checkHealth()

		case <-lt.C:
			s.Debugf("Checking JetStream cluster state")
			// If we have a current leader or had one in the past we can cancel this here since the metaleader
			// will be in charge of all peer state changes.
			// For cold boot only.
			if !n.Leaderless() || n.HadPreviousLeader() {
				lt.Stop()
				continue
			}
			// If we are here we do not have a leader and we did not have a previous one, so cold start.
			// Check to see if we can adjust our cluster size down iff we are in mixed mode and we have
			// seen a total that is what our original estimate was.
			cs := n.ClusterSize()
			if js, total := s.trackedJetStreamServers(); js < total && total >= cs && js != cs {
				s.Noticef("Adjusting JetStream expected peer set size to %d from original %d", js, cs)
				n.AdjustBootClusterSize(js)
			}
		}
	}
}

// This is called on first leader transition to double check the peers and cluster set size.
func (js *jetStream) checkClusterSize() {
	s, n := js.server(), js.getMetaGroup()
	if n == nil {
		return
	}
	// We will check that we have a correct cluster set size by checking for any non-js servers
	// which can happen in mixed mode.
	ps := n.(*raft).currentPeerState()
	if len(ps.knownPeers) >= ps.clusterSize {
		return
	}

	// Grab our active peers.
	peers := s.ActivePeers()

	// If we have not registered all of our peers yet we can't do
	// any adjustments based on a mixed mode. We will periodically check back.
	if len(peers) < ps.clusterSize {
		return
	}

	s.Debugf("Checking JetStream cluster size")

	// If we are here our known set as the leader is not the same as the cluster size.
	// Check to see if we have a mixed mode setup.
	var totalJS int
	for _, p := range peers {
		if si, ok := s.nodeToInfo.Load(p); ok && si != nil {
			if si.(nodeInfo).js {
				totalJS++
			}
		}
	}
	// If we have less then our cluster size adjust that here. Can not do individual peer removals since
	// they will not be in the tracked peers.
	if totalJS < ps.clusterSize {
		s.Debugf("Adjusting JetStream cluster size from %d to %d", ps.clusterSize, totalJS)
		if err := n.AdjustClusterSize(totalJS); err != nil {
			s.Warnf("Error adjusting JetStream cluster size: %v", err)
		}
	}
}

// Represents our stable meta state that we can write out.
type writeableStreamAssignment struct {
	Client     *ClientInfo     `json:"client,omitempty"`
	Created    time.Time       `json:"created"`
	ConfigJSON json.RawMessage `json:"stream"`
	Group      *raftGroup      `json:"group"`
	Sync       string          `json:"sync"`
	Consumers  []*writeableConsumerAssignment
}

// Returns the stream config as registered in the meta layer, from an inflight
// proposal that has not been applied yet, or from an applied assignment otherwise.
func (js *jetStream) clusterStreamConfig(accName, streamName string) (StreamConfig, bool) {
	js.mu.RLock()
	defer js.mu.RUnlock()
	if sa := js.streamAssignmentOrInflight(accName, streamName); sa != nil {
		return *sa.Config, true
	}
	return StreamConfig{}, false
}

func (js *jetStream) metaSnapshot() ([]byte, int, int, error) {
	js.mu.RLock()
	defer js.mu.RUnlock()
	cc := js.cluster
	return js.encodeMetaSnapshot(cc.streams)
}

func (js *jetStream) applyMetaSnapshot(buf []byte, ru *recoveryUpdates, isRecovering bool) error {
	streams, err := js.decodeMetaSnapshot(buf)
	if err != nil {
		return err
	}

	js.mu.Lock()
	cc := js.cluster

	var saAdd, saDel, saChk []*streamAssignment
	// Walk through the old list to generate the delete list.
	for account, asa := range cc.streams {
		nasa := streams[account]
		for sn, sa := range asa {
			if nsa := nasa[sn]; nsa == nil {
				// Stream was removed.
				saDel = append(saDel, sa)
			} else if !nsa.Created.Equal(sa.Created) && (nsa.Group == nil || sa.Group == nil || nsa.Group.Name != sa.Group.Name) {
				// Stream was recreated.
				saDel = append(saDel, sa)
				saAdd = append(saAdd, nsa)
			} else {
				saChk = append(saChk, nsa)
			}
		}
	}
	// Walk through the new list to generate the add list.
	for account, nasa := range streams {
		asa := cc.streams[account]
		for sn, sa := range nasa {
			if asa[sn] == nil {
				saAdd = append(saAdd, sa)
			}
		}
	}

	// Now walk the ones to check and process consumers.
	var caAdd, caDel []*consumerAssignment
	for _, sa := range saChk {
		// Make sure to add in all the new ones from sa.
		for _, ca := range sa.consumers {
			caAdd = append(caAdd, ca)
		}
		if osa := js.streamAssignment(sa.Client.serviceAccount(), sa.Config.Name); osa != nil {
			for _, ca := range osa.consumers {
				// Consumer was either removed or recreated.
				if nca := sa.consumers[ca.Name]; nca == nil {
					caDel = append(caDel, ca)
				} else if !nca.Created.Equal(ca.Created) && (nca.Group == nil || ca.Group == nil || nca.Group.Name != ca.Group.Name) {
					caDel = append(caDel, ca)
				}
			}
		}
	}
	js.mu.Unlock()

	// Reconcile the staged recoveryUpdates against this snapshot. Any staged operations that the
	// snapshot does not contain should be dropped, otherwise deleted assets could be revived.
	if isRecovering && ru != nil {
		supersededStream := func(sa *streamAssignment) bool {
			acc := sa.Client.serviceAccount()
			return streams[acc] == nil || streams[acc][sa.Config.Name] == nil
		}
		supersededConsumer := func(ca *consumerAssignment) bool {
			nsa := streams[ca.Client.serviceAccount()][ca.Stream]
			return nsa == nil || nsa.consumers[ca.Name] == nil
		}
		for key, sa := range ru.addStreams {
			if supersededStream(sa) {
				delete(ru.addStreams, key)
				delete(ru.updateStreams, key)
				delete(ru.updateConsumers, key)
				delete(ru.removeConsumers, key)
			}
		}
		for key, sa := range ru.updateStreams {
			if supersededStream(sa) {
				delete(ru.updateStreams, key)
				delete(ru.updateConsumers, key)
				delete(ru.removeConsumers, key)
			}
		}
		for skey, consumers := range ru.updateConsumers {
			for ckey, ca := range consumers {
				if supersededConsumer(ca) {
					delete(consumers, ckey)
				}
			}
			if len(consumers) == 0 {
				delete(ru.updateConsumers, skey)
			}
		}
	}

	// Do removals first.
	for _, sa := range saDel {
		js.setStreamAssignmentRecovering(sa)
		if isRecovering {
			ru.removeStream(sa)
		} else {
			js.processStreamRemoval(sa)
		}
	}
	// Now do add for the streams. Also add in all consumers.
	for _, sa := range saAdd {
		consumers := sa.consumers
		js.setStreamAssignmentRecovering(sa)
		if isRecovering {
			// Since we're recovering and storing up changes, we'll need to clear out these consumers.
			// Some might be removed, and we'll recover those later, must not be able to remember them.
			sa.consumers = nil
			ru.addStream(sa)
		} else {
			js.processStreamAssignment(sa)
		}

		// We can simply process the consumers.
		for _, ca := range consumers {
			js.setConsumerAssignmentRecovering(ca)
			if isRecovering {
				ru.addOrUpdateConsumer(ca)
			} else {
				js.processConsumerAssignment(ca)
			}
		}
	}

	// Perform updates on those in saChk. These were existing so make
	// sure to process any changes.
	for _, sa := range saChk {
		js.setStreamAssignmentRecovering(sa)
		if isRecovering {
			ru.updateStream(sa)
		} else {
			js.processUpdateStreamAssignment(sa)
		}
	}

	// Now do the deltas for existing stream's consumers.
	for _, ca := range caDel {
		js.setConsumerAssignmentRecovering(ca)
		if isRecovering {
			ru.removeConsumer(ca)
		} else {
			js.processConsumerRemoval(ca)
		}
	}
	for _, ca := range caAdd {
		js.setConsumerAssignmentRecovering(ca)
		if isRecovering {
			ru.addOrUpdateConsumer(ca)
		} else {
			js.processConsumerAssignment(ca)
		}
	}

	// If we're not recovering, we need to check if we have any left-over streams or consumers that aren't
	// tracked in our assignments. This could happen if we've restarted and recovered streams or consumers
	// from disk, but then got sent a snapshot to catch up from the meta-leader.
	if !isRecovering {
		// This logic is similar to that of checkForOrphans. But, this cleanup is focused on aligning with
		// a snapshot from a meta leader as a result of catchup. We silently delete them here, instead of
		// logging and sending out advisories.
		js.mu.RLock()
		deleteStreams, deleteConsumers := js.getOrphans()
		js.mu.RUnlock()
		for _, mset := range deleteStreams {
			mset.stop(true, false)
		}
		for _, o := range deleteConsumers {
			o.deleteWithoutAdvisory()
		}
	}
	return nil
}

// Decode the meta snapshot from buf into the relevant stream and consumer assignments.
func (js *jetStream) decodeMetaSnapshot(buf []byte) (map[string]map[string]*streamAssignment, error) {
	var wsas []writeableStreamAssignment
	if len(buf) > 0 {
		jse, err := s2.Decode(nil, buf)
		if err != nil {
			return nil, err
		}
		if err = json.Unmarshal(jse, &wsas); err != nil {
			return nil, err
		}
	}

	// Build our new version here outside of js.
	streams := make(map[string]map[string]*streamAssignment)
	for _, wsa := range wsas {
		as := streams[wsa.Client.serviceAccount()]
		if as == nil {
			as = make(map[string]*streamAssignment)
			streams[wsa.Client.serviceAccount()] = as
		}
		sa := &streamAssignment{Client: wsa.Client, Created: wsa.Created, ConfigJSON: wsa.ConfigJSON, Group: wsa.Group, Sync: wsa.Sync}
		if err := decodeStreamAssignmentConfig(js.srv, sa); err != nil {
			return nil, err
		}
		if len(wsa.Consumers) > 0 {
			sa.consumers = make(map[string]*consumerAssignment)
			for _, wca := range wsa.Consumers {
				if wca.Stream == _EMPTY_ {
					wca.Stream = sa.Config.Name // Rehydrate from the stream name.
				}
				ca := &consumerAssignment{Client: wca.Client, Created: wca.Created, Name: wca.Name, Stream: wca.Stream, ConfigJSON: wca.ConfigJSON, Group: wca.Group}
				if err := decodeConsumerAssignmentConfig(ca); err != nil {
					return nil, err
				}
				sa.consumers[ca.Name] = ca
			}
		}
		as[sa.Config.Name] = sa
	}
	return streams, nil
}

// Encode the meta assignments into an encoded and compressed buffer.
// Returns the snapshot itself, and the amount of streams and consumers.
func (js *jetStream) encodeMetaSnapshot(streams map[string]map[string]*streamAssignment) ([]byte, int, int, error) {
	start := time.Now()
	nsa := 0
	nca := 0
	for _, asa := range streams {
		nsa += len(asa)
	}
	out := make([]writeableStreamAssignment, 0, nsa)
	for _, asa := range streams {
		for _, sa := range asa {
			wsa := writeableStreamAssignment{
				Client:     sa.Client.forAssignmentSnap(),
				Created:    sa.Created,
				ConfigJSON: sa.ConfigJSON,
				Group:      sa.Group,
				Sync:       sa.Sync,
				Consumers:  make([]*writeableConsumerAssignment, 0, len(sa.consumers)),
			}
			for _, ca := range sa.consumers {
				wca := writeableConsumerAssignment{
					Client:     ca.Client.forAssignmentSnap(),
					Created:    ca.Created,
					Name:       ca.Name,
					Stream:     ca.Stream,
					ConfigJSON: ca.ConfigJSON,
					Group:      ca.Group,
				}
				wsa.Consumers = append(wsa.Consumers, &wca)
				nca++
			}
			out = append(out, wsa)
		}
	}

	if len(out) == 0 {
		return nil, nsa, nca, nil
	}

	// Track how long it took to marshal the JSON
	mstart := time.Now()
	b, err := json.Marshal(out)
	mend := time.Since(mstart)

	// Must not be possible for a JSON marshaling error to result
	// in an empty snapshot.
	if err != nil {
		return nil, nsa, nca, err
	}

	// Track how long it took to compress the JSON.
	cstart := time.Now()
	snap := s2.Encode(nil, b)
	cend := time.Since(cstart)
	took := time.Since(start)

	if took > 2*time.Second {
		js.srv.rateLimitFormatWarnf("Metalayer snapshot generation took %.3fs (streams: %d, consumers: %d, marshal: %.3fs, s2: %.3fs, uncompressed: %s, compressed: %s)",
			took.Seconds(), nsa, nca, mend.Seconds(), cend.Seconds(), friendlyBytes(len(b)), friendlyBytes(len(snap)))
	}

	// Track in jsz monitoring as well.
	if cc := js.cluster; cc != nil {
		atomic.StoreInt64(&cc.lastMetaSnapTime, start.UnixNano())
		atomic.StoreInt64(&cc.lastMetaSnapDuration, int64(took))
	}

	return snap, nsa, nca, nil
}

// Given a checkpoint, collects all relevant append entries that can be snapshotted.
func (js *jetStream) collectStreamAndConsumerChanges(c RaftNodeCheckpoint, streams map[string]map[string]*streamAssignment) error {
	ru := &recoveryUpdates{
		removeStreams:   make(map[string]*streamAssignment),
		removeConsumers: make(map[string]map[string]*consumerAssignment),
		addStreams:      make(map[string]*streamAssignment),
		updateStreams:   make(map[string]*streamAssignment),
		updateConsumers: make(map[string]map[string]*consumerAssignment),
	}
	for ae, err := range c.AppendEntriesSeq() {
		if err != nil {
			return err
		}
		for _, e := range ae.entries {
			if e.Type == EntryNormal {
				buf := e.Data
				if len(buf) == 0 {
					return errBadEntryOp
				}
				op := entryOp(buf[0])
				switch op {
				case assignStreamOp, updateStreamOp, removeStreamOp:
					sa, err := decodeStreamAssignment(js.srv, buf[1:])
					if err != nil {
						js.srv.Errorf("JetStream cluster failed to decode stream assignment: %q", buf[1:])
						panic(err)
					}
					if op == removeStreamOp {
						ru.removeStream(sa)
					} else {
						ru.addStream(sa)
					}
				case assignConsumerOp:
					ca, err := decodeConsumerAssignment(buf[1:])
					if err != nil {
						js.srv.Errorf("JetStream cluster failed to decode consumer assignment: %q", buf[1:])
						panic(err)
					}
					ru.addOrUpdateConsumer(ca)
				case assignCompressedConsumerOp:
					ca, err := decodeConsumerAssignmentCompressed(buf[1:])
					if err != nil {
						js.srv.Errorf("JetStream cluster failed to decode compressed consumer assignment: %q", buf[1:])
						panic(err)
					}
					ru.addOrUpdateConsumer(ca)
				case removeConsumerOp:
					ca, err := decodeConsumerAssignment(buf[1:])
					if err != nil {
						js.srv.Errorf("JetStream cluster failed to decode consumer assignment: %q", buf[1:])
						panic(err)
					}
					ru.removeConsumer(ca)
				default:
					panic(fmt.Sprintf("JetStream Cluster Unknown meta entry op type: %v", entryOp(buf[0])))
				}
			}
		}
	}

	// With our recovery structure, apply the stream/consumer diff.
	for _, cas := range ru.removeConsumers {
		for _, ca := range cas {
			if asa, ok := streams[ca.Client.serviceAccount()]; ok {
				if sa, ok := asa[ca.Stream]; ok {
					delete(sa.consumers, ca.Name)
				}
			}
		}
	}
	for _, sa := range ru.removeStreams {
		if asa, ok := streams[sa.Client.serviceAccount()]; ok {
			delete(asa, sa.Config.Name)
		}
	}
	for _, sa := range ru.addStreams {
		as := streams[sa.Client.serviceAccount()]
		if as == nil {
			as = make(map[string]*streamAssignment)
			streams[sa.Client.serviceAccount()] = as
		}
		// Preserve consumers from the previous assignment.
		if osa := as[sa.Config.Name]; osa != nil {
			sa.consumers = osa.consumers
		}
		as[sa.Config.Name] = sa
	}
	for _, cas := range ru.updateConsumers {
		for _, ca := range cas {
			if asa, ok := streams[ca.Client.serviceAccount()]; ok {
				if sa, ok := asa[ca.Stream]; ok {
					if sa.consumers == nil {
						sa.consumers = make(map[string]*consumerAssignment)
					}
					sa.consumers[ca.Name] = ca
				}
			}
		}
	}
	return nil
}

// Called on recovery to make sure we do not process like original.
func (js *jetStream) setStreamAssignmentRecovering(sa *streamAssignment) {
	js.mu.Lock()
	defer js.mu.Unlock()
	sa.markResponded()
	sa.recovering = true
	sa.Restore = nil
	if sa.Group != nil {
		sa.Group.Preferred = _EMPTY_
		// Must be preserved while a migration is inflight, so peers that create
		// their raft node late keep the empty-log protection.
		if sa.Group.Desired == nil {
			sa.Group.ScaleUp = false
		}
	}
}

// setStreamAssignmentCatchup keeps the empty-log protection for an assignment first seen via
// meta catchup, while leaving its reply intact so it can still be responded to.
func (js *jetStream) setStreamAssignmentCatchup(sa *streamAssignment) {
	js.mu.Lock()
	defer js.mu.Unlock()
	sa.catchup = true
	if sa.Group != nil && sa.Group.Desired == nil {
		sa.Group.ScaleUp = false
	}
}

// Called on recovery to make sure we do not process like original.
func (js *jetStream) setConsumerAssignmentRecovering(ca *consumerAssignment) {
	js.mu.Lock()
	defer js.mu.Unlock()
	ca.markResponded()
	ca.recovering = true
	if ca.Group != nil {
		ca.Group.Preferred = _EMPTY_
		// Must be preserved while a migration is inflight, so peers that create
		// their raft node late keep the empty-log protection.
		if ca.Group.Desired == nil {
			ca.Group.ScaleUp = false
		}
	}
}

// setConsumerAssignmentCatchup keeps the empty-log protection for an assignment first seen via
// meta catchup, while leaving its reply intact so it can still be responded to.
func (js *jetStream) setConsumerAssignmentCatchup(ca *consumerAssignment) {
	js.mu.Lock()
	defer js.mu.Unlock()
	ca.catchup = true
	if ca.Group != nil && ca.Group.Desired == nil {
		ca.Group.ScaleUp = false
	}
}

// Just copies over and changes out the group so it can be encoded.
// Lock should be held.
func (sa *streamAssignment) copyGroup() *streamAssignment {
	csa, cg := sa.clone(), *sa.Group
	csa.Group = &cg
	csa.Group.Peers = copyStrings(sa.Group.Peers)
	return csa
}

// Just copies over and changes out the group so it can be encoded.
// Lock should be held.
func (ca *consumerAssignment) copyGroup() *consumerAssignment {
	cca, cg := ca.clone(), *ca.Group
	cca.Group = &cg
	cca.Group.Peers = copyStrings(ca.Group.Peers)
	return cca
}

// Lock should be held.
func (sa *streamAssignment) missingPeers() bool {
	return len(sa.Group.Peers) < sa.Config.Replicas
}

// Called when we detect a new peer. Only the leader will process checking
// for any streams, and consequently any consumers.
func (js *jetStream) processAddPeer(peer string) {
	js.mu.Lock()
	defer js.mu.Unlock()

	s, cc := js.srv, js.cluster
	if cc == nil || cc.meta == nil {
		return
	}
	isLeader := cc.isLeader()

	// Now check if we are meta-leader. We will check for any re-assignments.
	if !isLeader {
		return
	}

	sir, ok := s.nodeToInfo.Load(peer)
	if !ok || sir == nil {
		return
	}
	si := sir.(nodeInfo)

	for accName, sa := range js.streamAssignmentsOrInflightSeqAllAccounts() {
		if sa.unsupported != nil {
			continue
		}
		if sa.missingPeers() {
			// Make sure the right cluster etc.
			if si.cluster != sa.Client.Cluster {
				continue
			}
			// If we are here we can add in this peer.
			csa := sa.copyGroup()
			csa.Group.Peers = append(csa.Group.Peers, peer)
			// Send our proposal for this csa. Also use same group definition for all the consumers as well.
			if err := cc.meta.Propose(cc.term, encodeAddStreamAssignment(csa)); err != nil {
				return
			}
			cc.trackInflightStreamProposal(accName, csa, false)
			for ca := range js.consumerAssignmentsOrInflightSeq(accName, csa.Config.Name) {
				if ca.unsupported != nil {
					continue
				}
				// Ephemerals are R=1, so only auto-remap durables, or R>1.
				if ca.Config.Durable != _EMPTY_ || len(ca.Group.Peers) > 1 {
					cca := ca.copyGroup()
					cca.Group.Peers = csa.Group.Peers
					if err := cc.meta.Propose(cc.term, encodeAddConsumerAssignment(cca)); err != nil {
						return
					}
					cc.trackInflightConsumerProposal(accName, csa.Config.Name, cca, false)
				}
			}
		}
	}
}

func (js *jetStream) processRemovePeer(peer string) {
	// We may be already disabled.
	if js == nil || js.disabled.Load() {
		return
	}

	// Account usage from remote servers is tracked by node ID. Once a server is
	// removed from the meta group it will never update that entry again, so drop
	// its last reported contribution from every account total.
	js.mu.RLock()
	accounts := make([]*jsAccount, 0, len(js.accounts))
	for _, jsa := range js.accounts {
		accounts = append(accounts, jsa)
	}
	js.mu.RUnlock()
	for _, jsa := range accounts {
		jsa.removeRemoteUsage(peer)
	}

	js.mu.Lock()
	s, cc := js.srv, js.cluster
	if cc == nil || cc.meta == nil {
		js.mu.Unlock()
		return
	}
	isLeader := cc.isLeader()
	// All nodes will check if this is them.
	isUs := cc.meta.ID() == peer
	js.mu.Unlock()

	if isUs {
		s.Errorf("JetStream being DISABLED, our server was removed from the cluster")
		adv := &JSServerRemovedAdvisory{
			TypedEvent: TypedEvent{
				Type: JSServerRemovedAdvisoryType,
				ID:   nuid.Next(),
				Time: time.Now().UTC(),
			},
			Server:   s.Name(),
			ServerID: s.ID(),
			Cluster:  s.cachedClusterName(),
			Domain:   s.getOpts().JetStreamDomain,
		}
		s.publishAdvisory(nil, JSAdvisoryServerRemoved, adv)

		go s.DisableJetStream()
	}

	// Now check if we are meta-leader. We will attempt re-assignment.
	if !isLeader {
		return
	}

	js.mu.Lock()
	defer js.mu.Unlock()

	for _, sa := range js.streamAssignmentsOrInflightSeqAllAccounts() {
		if sa.unsupported != nil {
			continue
		}
		if rg := sa.Group; rg.isMember(peer) {
			js.removePeerFromStreamLocked(sa, peer)
		}
	}
}

// Assumes all checks have already been done.
func (js *jetStream) removePeerFromStream(sa *streamAssignment, peer string) bool {
	js.mu.Lock()
	defer js.mu.Unlock()
	return js.removePeerFromStreamLocked(sa, peer)
}

// Lock should be held.
func (js *jetStream) removePeerFromStreamLocked(sa *streamAssignment, peer string) bool {
	if rg := sa.Group; !rg.isMember(peer) {
		return false
	}

	s, cc, csa := js.srv, js.cluster, sa.copyGroup()
	if cc == nil || cc.meta == nil {
		return false
	}
	replaced := cc.remapStreamAssignment(csa, peer)
	accName := sa.Client.serviceAccount()
	if !replaced {
		s.Warnf("JetStream cluster could not replace peer for stream '%s > %s'", accName, sa.Config.Name)
	}

	// Send our proposal for this csa. Also use same group definition for all the consumers as well.
	if err := cc.meta.Propose(cc.term, encodeAddStreamAssignment(csa)); err != nil {
		return false
	}
	cc.trackInflightStreamProposal(accName, csa, false)
	rg := csa.Group
	for ca := range js.consumerAssignmentsOrInflightSeq(accName, sa.Config.Name) {
		if ca.unsupported != nil {
			continue
		}
		// Ephemerals are R=1, so only auto-remap durables, or R>1.
		if ca.Config.Durable != _EMPTY_ {
			cca := ca.copyGroup()
			cca.Group.Peers, cca.Group.Preferred = rg.Peers, _EMPTY_
			if err := cc.meta.Propose(cc.term, encodeAddConsumerAssignment(cca)); err != nil {
				return false
			}
			cc.trackInflightConsumerProposal(accName, csa.Config.Name, cca, false)
		} else if ca.Group.isMember(peer) {
			// These are ephemerals. Check to see if we deleted this peer.
			if err := cc.meta.Propose(cc.term, encodeDeleteConsumerAssignment(ca)); err != nil {
				return false
			}
			cc.trackInflightConsumerProposal(accName, csa.Config.Name, ca, true)
		}
	}
	return replaced
}

// Check if we have peer related entries.
func (js *jetStream) hasPeerEntries(entries []*Entry) bool {
	for _, e := range entries {
		if e.Type == EntryRemovePeer || e.Type == EntryAddPeer {
			return true
		}
	}
	return false
}

const ksep = ":"

func (sa *streamAssignment) recoveryKey() string {
	if sa == nil {
		return _EMPTY_
	}
	return sa.Client.serviceAccount() + ksep + sa.Config.Name
}

func (ca *consumerAssignment) streamRecoveryKey() string {
	if ca == nil {
		return _EMPTY_
	}
	return ca.Client.serviceAccount() + ksep + ca.Stream
}

func (ca *consumerAssignment) recoveryKey() string {
	if ca == nil {
		return _EMPTY_
	}
	return ca.Client.serviceAccount() + ksep + ca.Stream + ksep + ca.Name
}

func (js *jetStream) applyMetaEntries(entries []*Entry, ru *recoveryUpdates) (bool, bool, error) {
	var didSnap bool
	isRecovering := ru != nil
	// Unlike startup recovery, catchup entries are seen for the first time.
	isCatchup := isRecovering && !js.isMetaRecovering()

	for _, e := range entries {
		// If we received a lower-level catchup entry, mark that we're recovering.
		// We can optimize by staging all meta operations until we're caught up.
		// At that point we can apply the diff in one go.
		if e.Type == EntryCatchup {
			isRecovering = true
			// A catchup entry only contains this, so we can exit now and have the
			// recoveryUpdates struct be populated for the next invocation of applyMetaEntries.
			return isRecovering, didSnap, nil
		}

		if e.Type == EntrySnapshot {
			if err := js.applyMetaSnapshot(e.Data, ru, isRecovering); err != nil {
				return isRecovering, didSnap, err
			}
			didSnap = true
		} else if e.Type == EntryRemovePeer {
			if !js.isMetaRecovering() {
				peer := string(e.Data)
				js.processRemovePeer(peer)

				// The meta leader can now respond to the peer-removal,
				// since a quorum of nodes has this in their log.
				s := js.srv
				if s.JetStreamIsLeader() {
					var (
						info peerRemoveInfo
						ok   bool
					)
					js.mu.Lock()
					if cc := js.cluster; cc != nil && cc.peerRemoveReply != nil {
						if info, ok = cc.peerRemoveReply[peer]; ok {
							delete(cc.peerRemoveReply, peer)
						}
						if len(cc.peerRemoveReply) == 0 {
							cc.peerRemoveReply = nil
						}
					}
					js.mu.Unlock()

					if info.reply != _EMPTY_ {
						sysAcc := s.SystemAccount()
						var resp = JSApiMetaServerRemoveResponse{ApiResponse: ApiResponse{Type: JSApiMetaServerRemoveResponseType}}
						resp.Success = true
						s.sendAPIResponse(info.ci, sysAcc, info.subject, info.reply, info.request, s.jsonResponse(&resp))
					}
				}
			}
		} else if e.Type == EntryAddPeer {
			if !js.isMetaRecovering() {
				js.processAddPeer(string(e.Data))
			}
		} else {
			buf := e.Data
			if len(buf) == 0 {
				return isRecovering, didSnap, errBadEntryOp
			}
			switch entryOp(buf[0]) {
			case assignStreamOp:
				sa, err := decodeStreamAssignment(js.srv, buf[1:])
				if err != nil {
					js.srv.Errorf("JetStream cluster failed to decode stream assignment: %q", buf[1:])
					return isRecovering, didSnap, err
				}
				if isRecovering {
					if isCatchup {
						js.setStreamAssignmentCatchup(sa)
					} else {
						js.setStreamAssignmentRecovering(sa)
					}
					ru.addStream(sa)
				} else {
					js.processStreamAssignment(sa)
				}
			case removeStreamOp:
				sa, err := decodeStreamAssignment(js.srv, buf[1:])
				if err != nil {
					js.srv.Errorf("JetStream cluster failed to decode stream assignment: %q", buf[1:])
					return isRecovering, didSnap, err
				}
				if isRecovering {
					if isCatchup {
						js.setStreamAssignmentCatchup(sa)
					} else {
						js.setStreamAssignmentRecovering(sa)
					}
					ru.removeStream(sa)
				} else {
					js.processStreamRemoval(sa)
				}
			case assignConsumerOp:
				ca, err := decodeConsumerAssignment(buf[1:])
				if err != nil {
					js.srv.Errorf("JetStream cluster failed to decode consumer assignment: %q", buf[1:])
					return isRecovering, didSnap, err
				}
				if isRecovering {
					if isCatchup {
						js.setConsumerAssignmentCatchup(ca)
					} else {
						js.setConsumerAssignmentRecovering(ca)
					}
					ru.addOrUpdateConsumer(ca)
				} else {
					js.processConsumerAssignment(ca)
				}
			case assignCompressedConsumerOp:
				ca, err := decodeConsumerAssignmentCompressed(buf[1:])
				if err != nil {
					js.srv.Errorf("JetStream cluster failed to decode compressed consumer assignment: %q", buf[1:])
					return isRecovering, didSnap, err
				}
				if isRecovering {
					if isCatchup {
						js.setConsumerAssignmentCatchup(ca)
					} else {
						js.setConsumerAssignmentRecovering(ca)
					}
					ru.addOrUpdateConsumer(ca)
				} else {
					js.processConsumerAssignment(ca)
				}
			case removeConsumerOp:
				ca, err := decodeConsumerAssignment(buf[1:])
				if err != nil {
					js.srv.Errorf("JetStream cluster failed to decode consumer assignment: %q", buf[1:])
					return isRecovering, didSnap, err
				}
				if isRecovering {
					if isCatchup {
						js.setConsumerAssignmentCatchup(ca)
					} else {
						js.setConsumerAssignmentRecovering(ca)
					}
					ru.removeConsumer(ca)
				} else {
					js.processConsumerRemoval(ca)
				}
			case updateStreamOp:
				sa, err := decodeStreamAssignment(js.srv, buf[1:])
				if err != nil {
					js.srv.Errorf("JetStream cluster failed to decode stream assignment: %q", buf[1:])
					return isRecovering, didSnap, err
				}
				if isRecovering {
					if isCatchup {
						js.setStreamAssignmentCatchup(sa)
					} else {
						js.setStreamAssignmentRecovering(sa)
					}
					ru.updateStream(sa)
				} else {
					js.processUpdateStreamAssignment(sa)
				}
			default:
				panic(fmt.Sprintf("JetStream Cluster Unknown meta entry op type: %v", entryOp(buf[0])))
			}
		}
	}
	return isRecovering, didSnap, nil
}

func (rg *raftGroup) isMember(id string) bool {
	if rg == nil {
		return false
	}
	for _, peer := range rg.Peers {
		if peer == id {
			return true
		}
	}
	return false
}

func (rg *raftGroup) setPreferred(s *Server) {
	if rg == nil || len(rg.Peers) == 0 {
		return
	}
	if len(rg.Peers) == 1 {
		rg.Preferred = rg.Peers[0]
	} else {
		var online []string
		for _, p := range rg.Peers {
			si, ok := s.nodeToInfo.Load(p)
			if !ok || si == nil {
				continue
			}
			ni := si.(nodeInfo)
			if ni.offline {
				continue
			}
			online = append(online, p)
		}

		if len(online) == 0 {
			// No online servers, just randomly select a peer for the preferred.
			pi := rand.Int32N(int32(len(rg.Peers)))
			rg.Preferred = rg.Peers[pi]
		} else if len(online) == 1 {
			// Only one online server.
			rg.Preferred = online[0]
		} else {
			// Randomly select an online peer.
			pi := rand.Int32N(int32(len(online)))
			rg.Preferred = online[pi]
		}
	}
}

// createRaftGroup is called to spin up this raft group if needed.
func (js *jetStream) createRaftGroup(accName string, rg *raftGroup, recovering bool, storage StorageType, labels pprofLabels) (RaftNode, error) {
	// js.mu protects the lookup/registration of raft groups so that two parallel
	// calls for the same identifier can't end up creating duplicate instances.
	// It is released around blocking work (waiting for a previous instance to
	// shut down, and the disk I/O inside startRaftNode); concurrent callers for
	// the same rg.Name are gated through cc.creatingRaftGroups instead.
	js.mu.Lock()
	defer js.mu.Unlock()

	s, cc := js.srv, js.cluster
	if cc == nil || cc.meta == nil {
		return nil, NewJSClusterNotActiveError()
	}

	// While a desired state is pending the group keeps a node even at a single peer, so
	// scaling down doesn't lose the Raft term a newer server resumes the migration from.
	// If this is a single peer raft group or we are not a member return.
	if (rg.Desired == nil && len(rg.Peers) <= 1) || !rg.isMember(cc.meta.ID()) {
		// Nothing to do here.
		return nil, nil
	}

retry:
	// If another goroutine is mid-creation for this raft group, wait for it
	// to finish (without holding js.mu) and then retry the lookup.
	if ch, ok := cc.creatingRaftGroups[rg.Name]; ok {
		js.mu.Unlock()
		<-ch
		js.mu.Lock()
		// js.cluster could have been swapped out (shutdown).
		if js.cluster == nil || js.cluster.meta == nil {
			return nil, NewJSClusterNotActiveError()
		}
		cc = js.cluster
		goto retry
	}
	// Check if we already have this assigned.
	if node := s.lookupRaftNode(rg.Name); node != nil {
		if node.State() == Closed {
			// We're waiting for this node to finish shutting down before we replace it.
			js.mu.Unlock()
			node.WaitForStop()
			js.mu.Lock()
			goto retry
		}
		s.Debugf("JetStream cluster already has raft group %q assigned", rg.Name)
		// Check and see if the group has the same peers. If not then we
		// will update the known peers, which will send a peerstate if leader.
		groupPeerIDs := append([]string{}, rg.Peers...)
		var samePeers bool
		if nodePeers := node.Peers(); len(rg.Peers) == len(nodePeers) {
			nodePeerIDs := make([]string, 0, len(nodePeers))
			for _, n := range nodePeers {
				nodePeerIDs = append(nodePeerIDs, n.ID)
			}
			slices.Sort(groupPeerIDs)
			slices.Sort(nodePeerIDs)
			samePeers = slices.Equal(groupPeerIDs, nodePeerIDs)
		}
		if !samePeers {
			// At this point we have no way of knowing:
			// 1. Whether the group has lost enough nodes to cause a quorum
			//    loss, in which case a proposal may fail, therefore we will
			//    force a peerstate write;
			// 2. Whether nodes in the group have other applies queued up
			//    that could change the peerstate again, therefore the leader
			//    should send out a new proposal anyway too just to make sure
			//    that this change gets captured in the log.
			node.UpdateKnownPeers(groupPeerIDs)

			// If the peers changed as a result of an update by the meta layer, we must reflect that in the log of
			// this group. Otherwise, a new peer would come up and instantly reset the peer state back to whatever is
			// in the log at that time, overwriting what the meta layer told it.
			// Will need to address this properly later on, by for example having the meta layer decide the new
			// placement, but have the leader of this group propose it through its own log instead.
			if node.Leader() {
				node.ProposeKnownPeers(groupPeerIDs)
			}
		}
		rg.node = node
		return node, nil
	}

	s.Debugf("JetStream cluster creating raft group:%+v", rg)

	sysAcc := s.SystemAccount()
	if sysAcc == nil {
		s.Debugf("JetStream cluster detected shutdown processing raft group: %+v", rg)
		return nil, errors.New("shutting down")
	}

	// Check here to see if we have a max HA Assets limit set.
	if maxHaAssets := s.getOpts().JetStreamLimits.MaxHAAssets; maxHaAssets > 0 {
		if s.numRaftNodes()+len(cc.creatingRaftGroups) > maxHaAssets {
			s.Warnf("Maximum HA Assets limit reached: %d", maxHaAssets)
			// Since the meta leader assigned this, send a statsz update to them to get them up to date.
			go s.sendStatszUpdate()
			return nil, errors.New("system limit reached")
		}
	}

	// Register an in-flight sentinel so concurrent callers for the same group
	// will wait for us. Then drop js.mu around all the blocking work below
	// (file store creation, peer state read, snapshot replay, fsyncs) so we
	// don't serialize every stream/consumer assignment behind one disk fsync.
	if cc.creatingRaftGroups == nil {
		cc.creatingRaftGroups = make(map[string]chan struct{})
	}
	doneCh := make(chan struct{})
	cc.creatingRaftGroups[rg.Name] = doneCh

	// Snapshot rg fields; we drop js.mu below and rg is shared.
	rgName, rgScaleUp := rg.Name, rg.ScaleUp
	rgPeers := copyStrings(rg.Peers)
	storeDir := filepath.Join(js.config.StoreDir, sysAcc.Name, defaultStoreDirName, rg.Name)
	js.mu.Unlock()

	n, err := func() (RaftNode, error) {
		var store StreamStore
		if storage == FileStorage {
			opts := s.getOpts()
			fs, err := newFileStoreWithCreated(
				FileStoreConfig{StoreDir: storeDir, BlockSize: defaultMediumBlockSize, AsyncFlush: false, SyncAlways: opts.SyncAlways, SyncInterval: opts.SyncInterval, srv: s},
				StreamConfig{Name: rgName, Storage: FileStorage, Metadata: labels},
				time.Now().UTC(),
				s.jsKeyGen(opts.JetStreamKey, rgName),
				s.jsKeyGen(opts.JetStreamOldKey, rgName),
			)
			if err != nil {
				s.Errorf("Error creating filestore WAL: %v", err)
				return nil, err
			}
			store = fs
		} else {
			ms, err := newMemStore(&StreamConfig{Name: rgName, Storage: MemoryStorage})
			if err != nil {
				s.Errorf("Error creating memstore WAL: %v", err)
				return nil, err
			}
			store = ms
		}

		cfg := &RaftConfig{Name: rgName, Store: storeDir, Log: store, Track: true, Recovering: recovering, ScaleUp: rgScaleUp}

		if _, err := readPeerState(s.diskIOSemaphore(), storeDir); err != nil {
			s.bootstrapRaftNode(cfg, rgPeers, true)
		}

		n, err := s.startRaftNode(accName, cfg, labels)
		if err != nil || n == nil {
			s.Debugf("Error creating raft group: %v", err)
			return nil, err
		}
		return n, nil
	}()

	js.mu.Lock()
	delete(cc.creatingRaftGroups, rg.Name)
	close(doneCh)
	if err != nil || n == nil {
		return nil, err
	}
	if js.cluster == nil || js.cluster.meta == nil {
		// Cluster was torn down while we were creating; let n be reaped at shutdown.
		return nil, NewJSClusterNotActiveError()
	}
	// Need JS lock to be held for the assignment to avoid data-race reports
	rg.node = n
	preferred := rg.Preferred
	if rg.Desired != nil {
		preferred = rg.Desired.Preferred
	}
	// See if we are preferred and should start campaign immediately.
	if n.ID() == preferred && n.Term() == 0 {
		n.CampaignImmediately()
	}
	return n, nil
}

func (mset *stream) raftGroup() *raftGroup {
	if mset == nil {
		return nil
	}
	mset.mu.RLock()
	defer mset.mu.RUnlock()
	if mset.sa == nil {
		return nil
	}
	return mset.sa.Group
}

func (mset *stream) raftNode() RaftNode {
	if mset == nil {
		return nil
	}
	mset.mu.RLock()
	defer mset.mu.RUnlock()
	return mset.node
}

func (mset *stream) removeNode() {
	mset.mu.Lock()
	defer mset.mu.Unlock()
	if n := mset.node; n != nil {
		n.Delete()
		mset.node = nil
	}
}

// Helper function to generate peer info.
// lists and sets for old and new.
func genPeerInfo(peers []string, split int) (newPeers, oldPeers []string, newPeerSet, oldPeerSet map[string]bool) {
	newPeers = peers[split:]
	oldPeers = peers[:split]
	newPeerSet = make(map[string]bool, len(newPeers))
	oldPeerSet = make(map[string]bool, len(oldPeers))
	for i, peer := range peers {
		if i < split {
			oldPeerSet[peer] = true
		} else {
			newPeerSet[peer] = true
		}
	}
	return
}

// This will wait for a period of time until all consumers are registered and have
// their consumer assignments assigned.
// Should only be called from monitorStream.
func (mset *stream) waitOnConsumerAssignments() {
	mset.mu.RLock()
	s, js, acc, sa, name, replicas := mset.srv, mset.js, mset.acc, mset.sa, mset.cfg.Name, mset.cfg.Replicas
	mset.mu.RUnlock()

	if s == nil || js == nil || acc == nil || sa == nil {
		return
	}

	js.mu.RLock()
	numExpectedConsumers := len(sa.consumers)
	js.mu.RUnlock()

	// Max to wait.
	const maxWaitTime = 10 * time.Second
	const sleepTime = 500 * time.Millisecond

	// Wait up to 10s
	timeout := time.Now().Add(maxWaitTime)
	for time.Now().Before(timeout) {
		var numReady int
		for _, o := range mset.getConsumers() {
			// Make sure we are registered with our consumer assignment.
			if ca := o.consumerAssignment(); ca != nil {
				if replicas > 1 && !o.isMonitorRunning() {
					break
				}
				numReady++
			} else {
				break
			}
		}
		// Check if we are good.
		if numReady >= numExpectedConsumers {
			break
		}

		s.Debugf("Waiting for consumers for interest based stream '%s > %s'", acc.Name, name)
		select {
		case <-s.quitCh:
			return
		case <-mset.monitorQuitC():
			return
		case <-time.After(sleepTime):
		}
	}

	if actual := mset.numConsumers(); actual < numExpectedConsumers {
		s.Warnf("All consumers not online for '%s > %s': expected %d but only have %d", acc.Name, name, numExpectedConsumers, actual)
	}
}

// Monitor our stream node for this stream.
func (js *jetStream) monitorStream(mset *stream, sa *streamAssignment, sendSnapshot bool) {
	s, cc := js.server(), js.cluster
	defer s.grWG.Done()
	if mset != nil {
		defer mset.monitorWg.Done()
	}
	js.mu.RLock()
	n := sa.Group.node
	meta := cc.meta
	js.mu.RUnlock()

	if n == nil || meta == nil {
		s.Warnf("No RAFT group for '%s > %s'", sa.Client.serviceAccount(), sa.Config.Name)
		return
	}

	// Make sure only one is running.
	if mset != nil {
		if mset.checkInMonitor() {
			return
		}
		defer mset.clearMonitorRunning()
	}

	// Make sure to stop the raft group on exit to prevent accidental memory bloat.
	// This should be below the checkInMonitor call though to avoid stopping it out
	// from underneath the one that is running since it will be the same raft node.
	defer func() {
		// We might be closing during shutdown, don't pre-emptively stop here since we'll still want to install snapshots.
		if mset != nil && !mset.closed.Load() {
			n.Stop()
		}
	}()

	qch, mqch, lch, aq, uch := n.QuitC(), mset.monitorQuitC(), n.LeadChangeC(), n.ApplyQ(), mset.updateC()

	s.Debugf("Starting stream monitor for '%s > %s' [%s]", sa.Client.serviceAccount(), sa.Config.Name, n.Group())
	defer s.Debugf("Exiting stream monitor for '%s > %s' [%s]", sa.Client.serviceAccount(), sa.Config.Name, n.Group())

	// Make sure we do not leave the apply channel to fill up and block the raft layer.
	defer func() {
		if n.State() == Closed {
			return
		}
		n.StepDown()
		// Drain the commit queue...
		aq.drain()
	}()

	const (
		compactInterval    = 2 * time.Minute
		compactMinInterval = 15 * time.Second
		compactSizeMin     = 8 * 1024 * 1024
		compactNumMin      = 65536
	)

	// Spread these out for large numbers on server restart.
	rci := time.Duration(rand.Int64N(int64(time.Minute)))
	t := time.NewTicker(compactInterval + rci)
	defer t.Stop()

	js.mu.RLock()
	isLeader := cc.isStreamLeader(sa.Client.serviceAccount(), sa.Config.Name)
	isRestore := sa.Restore != nil
	js.mu.RUnlock()

	acc, err := s.LookupAccount(sa.Client.serviceAccount())
	if err != nil {
		s.Warnf("Could not retrieve account for stream '%s > %s'", sa.Client.serviceAccount(), sa.Config.Name)
		return
	}
	accName := acc.GetName()

	// Don't allow the upper layer to install snapshots until we have
	// fully recovered from disk.
	isRecovering := true

	var (
		snapMu           sync.Mutex
		snapshotting     bool
		fallbackSnapshot bool
		failedSnapshots  int
	)

	// Make sure all pending data is flushed before allowing snapshots. Returns false if
	// it couldn't be flushed, in which case we have no safe way to continue.
	flushPending := func() bool {
		err := mset.flushAllPending()
		if err == nil {
			return true
		}
		s.Errorf("Failed to flush pending data for '%s > %s' [%s]: %v", accName, mset.name(), n.Group(), err)
		assert.Unreachable("Stream snapshot flush failed", map[string]any{
			"account": accName,
			"stream":  mset.name(),
			"group":   n.Group(),
			"err":     err,
		})
		mset.setWriteErr(err)
		n.Stop()
		return false
	}

	doSnapshot := func(force bool) {
		// Suppress during recovery.
		if mset == nil || isRecovering || isRestore {
			return
		}
		snapMu.Lock()
		defer snapMu.Unlock()

		// Don't suppress a snapshot during shutdown.
		if js.isShuttingDown() {
			if !flushPending() {
				return
			}
			if err := n.InstallSnapshot(mset.stateSnapshot(), true); err != nil &&
				err != errNoSnapAvailable && err != errNodeClosed {
				s.RateLimitWarnf("Failed to install snapshot for '%s > %s' [%s]: %v",
					mset.acc.Name, mset.name(), n.Group(), err)
			}
			return
		}

		// If snapshots have failed, and we're not forced to, we'll wait for the timer since it'll now be forced.
		if !force && failedSnapshots > 0 {
			return
		}
		// Suppress if an async snapshot is already in progress.
		if snapshotting {
			return
		}

		// If we had a significant number of failed snapshots, start relaxing Raft-layer checks
		// to force it through. We might have been catching up a peer for a long period, and this
		// protects our log size from growing indefinitely.
		forceSnapshot := failedSnapshots > 4
		c, err := n.CreateSnapshotCheckpoint(forceSnapshot)
		if err != nil {
			if err != errNoSnapAvailable && err != errNodeClosed {
				s.RateLimitWarnf("Failed to install snapshot for '%s > %s' [%s]: %v",
					mset.acc.Name, mset.name(), n.Group(), err)
				// If this is the first failure, reduce the interval of the snapshot timer.
				// This ensures we're not waiting too long for snapshotting to eventually become forced.
				if failedSnapshots == 0 {
					t.Reset(compactMinInterval)
				}
				failedSnapshots++
			}
			return
		}
		if !flushPending() {
			c.Abort()
			return
		}

		snap := mset.stateSnapshot()

		handleInstallResult := func(err error) {
			snapshotting = false
			if err == nil {
				// If there was a failed snapshot before, we reduced the timer's interval.
				// Reset it back to the original interval now.
				if failedSnapshots > 0 {
					t.Reset(compactInterval + rci)
				}
				failedSnapshots = 0
				fallbackSnapshot = false
			} else {
				c.Abort()

				if err == errNoSnapAvailable || err == errNodeClosed || err == errCatchupsRunning || err == errSnapAborted {
					return
				}

				s.RateLimitWarnf("Failed to install snapshot for '%s > %s' [%s]: %v, will fall back to blocking snapshot",
					mset.acc.Name, mset.name(), n.Group(), err)
				fallbackSnapshot = true
				// If this is the first failure, reduce the interval of the snapshot timer.
				// This ensures we're not waiting too long for snapshotting to eventually become forced.
				if failedSnapshots == 0 {
					t.Reset(compactMinInterval)
				}
				failedSnapshots++
			}
		}

		snapshotting = true
		if fallbackSnapshot {
			_, err = c.InstallSnapshot(snap)
			handleInstallResult(err)
		} else {
			started := s.startGoRoutine(func() {
				defer s.grWG.Done()

				_, err := c.InstallSnapshot(snap)

				snapMu.Lock()
				defer snapMu.Unlock()
				handleInstallResult(err)
			})
			if !started {
				snapshotting = false
				c.Abort()
			}
		}
	}

	// We will establish a restoreDoneCh no matter what. Will never be triggered unless
	// we replace with the restore chan.
	restoreDoneCh := make(<-chan error)

	// For migration tracking.
	mmtd := 500 * time.Millisecond
	var mmt *time.Ticker
	var mmtc <-chan time.Time

	startMigrationMonitoring := func() {
		if mmt == nil {
			mmt = time.NewTicker(mmtd)
			mmtc = mmt.C
		}
	}

	stopMigrationMonitoring := func() {
		if mmt != nil {
			mmt.Stop()
			mmt, mmtc = nil, nil
		}
	}
	defer stopMigrationMonitoring()

	// This is to optionally track when we are ready as a non-leader for direct access participation.
	// Either direct or if we are a direct mirror, or both.
	var dat *time.Ticker
	var datc <-chan time.Time

	startDirectAccessMonitoring := func() {
		if dat == nil {
			dat = time.NewTicker(2 * time.Second)
			datc = dat.C
		}
	}

	stopDirectMonitoring := func() {
		if dat != nil {
			dat.Stop()
			dat, datc = nil, nil
		}
	}
	defer stopDirectMonitoring()

	// For checking interest state if applicable.
	var cist *time.Ticker
	var cistc <-chan time.Time

	checkInterestInterval := checkInterestStateT + time.Duration(rand.IntN(checkInterestStateJ))*time.Second

	if mset != nil && mset.isInterestRetention() {
		// Wait on our consumers to be assigned and running before proceeding.
		// This can become important when a server has lots of assets
		// since we process streams first then consumers as an asset class.
		mset.waitOnConsumerAssignments()
		// Setup our periodic check here. We will check once we have restored right away.
		cist = time.NewTicker(checkInterestInterval)
		cistc = cist.C
	}

	// State to check for atomic batch completeness before applying it.
	batch := &batchApply{}
	defer func() {
		for _, bce := range batch.entries {
			bce.ReturnToPool()
		}
		batch.clearBatchState()
	}()

	for {
		select {
		case <-s.quitCh:
			// Server shutting down, but we might receive this before qch, so try to snapshot.
			doSnapshot(false)
			return
		case <-mqch:
			// Clean signal from shutdown routine so do best effort attempt to snapshot.
			// Don't snapshot if not shutting down, monitor goroutine could be going away
			// on a scale down or a remove for example.
			if js.isShuttingDown() {
				doSnapshot(false)
			}
			return
		case <-qch:
			// Raft node is closed, no use in trying to snapshot.
			return
		case <-aq.ch:
			var ne, nb uint64
			// If we bump clfs we will want to write out snapshot if within our time window.
			pclfs := mset.getCLFS()

			ces := aq.pop()
			for _, ce := range ces {
				// No special processing needed for when we are caught up on restart.
				if ce == nil {
					if !isRecovering {
						continue
					}
					isRecovering = false
					// If we are interest based make sure to check consumers if interest retention policy.
					// This is to make sure we process any outstanding acks from all consumers.
					if mset != nil && mset.isInterestRetention() {
						fire := time.Duration(rand.IntN(5)+5) * time.Second
						time.AfterFunc(fire, mset.checkInterestState)
					}
					// If we became leader during this time and we need to send a snapshot to our
					// followers, i.e. as a result of a scale-up from R1, do it now.
					if sendSnapshot && isLeader && mset != nil && n != nil {
						n.SendSnapshot(mset.stateSnapshot())
						sendSnapshot = false
					}
					continue
				} else if len(ce.Entries) == 0 {
					// If we have a partial batch, it needs to be rejected to ensure CLFS is correct.
					if mset != nil && batch != nil && batch.id != _EMPTY_ {
						batch.rejectBatchState(mset)
					}

					// Entry could be empty on a restore when mset is nil.
					ne, nb = n.Applied(ce.Index)
					ce.ReturnToPool()
					continue
				}

				// Apply our entries.
				if maxApplied, err := js.applyStreamEntries(mset, n, ce, isRecovering, batch); err == nil {
					// Update our applied.
					if maxApplied > 0 {
						// Indicate we've processed (but not applied) everything up to this point.
						ne, nb = n.Processed(ce.Index, min(maxApplied, ce.Index))
						// Don't return entry to the pool, this is handled by the in-progress batch.
					} else {
						ne, nb = n.Applied(ce.Index)
						ce.ReturnToPool()
					}
				} else {
					// Make sure to clean up.
					ce.ReturnToPool()
					// Our stream was closed out from underneath of us, simply return here.
					if err == errStreamClosed || err == errCatchupStreamStopped || err == ErrServerNotRunning {
						aq.recycle(&ces)
						return
					}
					s.Errorf("Error applying entries to '%s > %s': %v", accName, sa.Config.Name, err)
					if isClusterResetErr(err) {
						if mset.isMirror() && mset.IsLeader() {
							mset.retryMirrorConsumer()
							continue
						}
						// If the error signals we timed out of a snapshot, we should try to replay the snapshot
						// instead of fully resetting the state. Resetting the clustered state may result in
						// race conditions and should only be used as a last effort attempt.
						if errors.Is(err, errCatchupAbortedNoLeader) || err == errCatchupTooManyRetries || err == errAlreadyLeader {
							if n.DrainAndReplaySnapshot() {
								break
							} else if n.IsDeleted() {
								// The only reason we can't replay is our node being deleted, which
								// means the meta layer is deliberately tearing this monitor down.
								// Don't fall through to a reset, that would resurrect this group.
								s.Warnf("Will not reset stream '%s > %s', raft group %q was removed",
									accName, sa.Config.Name, n.Group())
								aq.recycle(&ces)
								return
							}
						}
						// We will attempt to reset our cluster state.
						if mset.resetClusteredState(n, err) {
							aq.recycle(&ces)
							return
						}
					} else if isOutOfSpaceErr(err) {
						// If applicable this will tear all of this down, but don't assume so and return.
						s.handleOutOfSpace(mset)
					} else {
						// Encountered an unexpected error, can't continue.
						mset.setWriteErr(err)
						aq.recycle(&ces)
						return
					}
				}
			}
			aq.recycle(&ces)

			// Check about snapshotting
			// If we have at least min entries to compact, go ahead and try to snapshot/compact.
			if ne >= compactNumMin || nb > compactSizeMin || mset.getCLFS() > pclfs {
				doSnapshot(false)
			}

		case lc := <-lch:
			isLeader = lc.isLeader
			// Process our leader change.
			js.processStreamLeaderChange(mset, isLeader, lc.term)

			if isLeader {
				if mset != nil && n != nil && sendSnapshot && !isRecovering {
					// If we *are* recovering at the time then this will get done when the apply queue
					// handles the nil guard to show the catchup ended.
					n.SendSnapshot(mset.stateSnapshot())
					sendSnapshot = false
				}
				if isRestore {
					acc, _ := s.LookupAccount(sa.Client.serviceAccount())
					restoreDoneCh = s.processStreamRestore(sa.Client, acc, sa.Config, _EMPTY_, sa.Reply, _EMPTY_)
					continue
				} else if n != nil && n.NeedSnapshot() {
					doSnapshot(false)
				}
				// Always cancel if this was running.
				stopDirectMonitoring()
			} else if !n.Leaderless() {
				js.setStreamAssignmentRecovering(sa)
			}

			// We may receive a leader change after the stream assignment which would cancel us
			// monitoring for this closely. So re-assess our state here as well.
			// Or the old leader is no longer part of the set and transferred leadership
			// for this leader to resume with removal
			migrating := mset.isMigrating()

			// Check for migrations here. We set the state on the stream assignment update below.
			if isLeader && migrating {
				startMigrationMonitoring()
			} else {
				stopMigrationMonitoring()
			}

			// Here we are checking if we are not the leader but we have been asked to allow
			// direct access. We now allow non-leaders to participate in the queue group.
			if !isLeader && mset != nil {
				mset.mu.RLock()
				ad, md := mset.cfg.AllowDirect, mset.cfg.MirrorDirect
				mset.mu.RUnlock()
				if ad || md {
					startDirectAccessMonitoring()
				}
			}

		case <-cistc:
			cist.Reset(checkInterestInterval)
			// We may be adjusting some things with consumers so do this in its own go routine.
			go mset.checkInterestState()

		case <-datc:
			if mset == nil || isRecovering {
				continue
			}
			// If we are leader we can stop, we know this is setup now.
			if isLeader {
				stopDirectMonitoring()
				continue
			}

			mset.mu.Lock()
			ad, md, current := mset.cfg.AllowDirect, mset.cfg.MirrorDirect, mset.isCurrent()
			if !current {
				const syncThreshold = 90.0
				// We are not current, but current means exactly caught up. Under heavy publish
				// loads we may never reach this, so check if we are within 90% caught up.
				_, c, a := mset.node.Progress()
				if c == 0 {
					mset.mu.Unlock()
					continue
				}
				if p := float64(a) / float64(c) * 100.0; p < syncThreshold {
					mset.mu.Unlock()
					continue
				} else {
					s.Debugf("Stream '%s > %s' enabling direct gets at %.0f%% synchronized",
						sa.Client.serviceAccount(), sa.Config.Name, p)
				}
			}
			// We are current, cancel monitoring and create the direct subs as needed.
			if ad {
				mset.subscribeToDirect()
			}
			if md {
				mset.subscribeToMirrorDirect()
			}
			mset.mu.Unlock()
			// Stop direct monitoring.
			stopDirectMonitoring()

		case <-t.C:
			// Start forcing snapshots if they failed previously.
			snapMu.Lock()
			forceIfFailed := failedSnapshots > 0
			snapMu.Unlock()
			doSnapshot(forceIfFailed)

		case <-uch:
			// keep stream assignment current
			sa = mset.streamAssignment()

			// We get this when we have a new stream assignment caused by an update.
			// We want to know if we are migrating.
			migrating := mset.isMigrating()
			if isLeader && migrating {
				startMigrationMonitoring()
			} else {
				stopMigrationMonitoring()
			}
		case <-mmtc:
			if !isLeader {
				// We are no longer leader, so not our job.
				stopMigrationMonitoring()
				continue
			}
			if done := js.runStreamMigration(mset, sa, n); done {
				stopMigrationMonitoring()
				continue
			}

		case err := <-restoreDoneCh:
			// We have completed a restore from snapshot on this server. The stream assignment has
			// already been assigned but the replicas will need to catch up out of band. Consumers
			// will need to be assigned by forwarding the proposal and stamping the initial state.
			s.Debugf("Stream restore for '%s > %s' completed", sa.Client.serviceAccount(), sa.Config.Name)
			if err != nil {
				s.Debugf("Stream restore failed: %v", err)
			}
			isRestore = false
			sa.Restore = nil
			// If we were successful lookup up our stream now.
			if err == nil {
				if mset, err = acc.lookupStream(sa.Config.Name); mset != nil {
					mset.startMonitorWg()
					defer mset.monitorWg.Done()
					mset.checkInMonitor()
					mset.setStreamAssignment(sa)
					// Make sure to update our updateC which would have been nil.
					uch = mset.updateC()
					// Also update our mqch
					mqch = mset.monitorQuitC()
					// Setup a periodic check here if we are interest based as well.
					if mset.isInterestRetention() {
						cist = time.NewTicker(checkInterestInterval)
						cistc = cist.C
					}
				}
			}
			// We may have lost leadership while the restore was running.
			if err == nil && !isLeader {
				err = errNotLeader
			}
			if err != nil {
				if mset != nil {
					mset.delete()
				}
				js.mu.Lock()
				s.Warnf("Stream restore failed for '%s > %s': %v", sa.Client.serviceAccount(), sa.Config.Name, err)
				sa.err = err
				if n != nil {
					n.Delete()
				}
				result := &streamAssignmentResult{
					Account: sa.Client.serviceAccount(),
					Stream:  sa.Config.Name,
					Restore: &JSApiStreamRestoreResponse{ApiResponse: ApiResponse{Type: JSApiStreamRestoreResponseType}},
				}
				result.Restore.Error = NewJSStreamAssignmentError(err, Unless(err))
				js.mu.Unlock()
				// Send response to the metadata leader. They will forward to the user as needed.
				s.sendInternalMsgLocked(streamAssignmentSubj, _EMPTY_, nil, result)
				return
			}
			// Trigger the stream followers to catchup.
			var term uint64
			if n = mset.raftNode(); n != nil {
				n.SendSnapshot(mset.stateSnapshot())
				term = n.Term()
			}
			js.processStreamLeaderChange(mset, isLeader, term)

			// Check to see if we have restored consumers here.
			// These are not currently assigned so we will need to do so here.
			if consumers := mset.getPublicConsumers(); len(consumers) > 0 {
				// Need to read meta under the js lock, it's set to nil on shutdown.
				js.mu.RLock()
				meta := cc.meta
				js.mu.RUnlock()
				if meta == nil {
					// We are shutting down.
					continue
				}
				for _, o := range consumers {
					name, cfg := o.String(), o.config()
					rg, err := cc.createGroupForConsumer(&cfg, sa)
					if err != nil {
						s.Warnf("Could not create group for consumer '%s > %s > %s': %v",
							sa.Client.serviceAccount(), sa.Config.Name, name, err)
						continue
					}
					// Pick a preferred leader.
					rg.setPreferred(s)

					// Place our initial state here as well for assignment distribution.
					state, _ := o.store.State()
					ca := &consumerAssignment{
						Group:   rg,
						Stream:  sa.Config.Name,
						Name:    name,
						Config:  &cfg,
						Client:  sa.Client,
						Created: o.createdTime(),
						State:   state,
					}

					// We make these compressed in case state is complex.
					addEntry := encodeAddConsumerAssignmentCompressed(ca)
					meta.ForwardProposal(addEntry)

					// Check to make sure we see the assignment.
					go func() {
						ticker := time.NewTicker(time.Second)
						defer ticker.Stop()
						for range ticker.C {
							js.mu.RLock()
							ca, meta := js.consumerAssignment(ca.Client.serviceAccount(), sa.Config.Name, name), cc.meta
							js.mu.RUnlock()
							if ca == nil {
								s.Warnf("Consumer assignment has not been assigned, retrying")
								if meta != nil {
									meta.ForwardProposal(addEntry)
								} else {
									return
								}
							} else {
								return
							}
						}
					}()
				}
			}
		}
	}
}

// Migrate a stream from peer set A to peer set B. Returns true when migration is done.
func (js *jetStream) runStreamMigration(mset *stream, sa *streamAssignment, n RaftNode) bool {
	ourPeerId, cc, s := n.ID(), js.cluster, js.srv

	js.mu.RLock()
	// We are shutting down.
	if cc == nil || cc.meta == nil {
		js.mu.RUnlock()
		return true
	}
	meta := cc.meta
	accName, streamName := sa.Client.serviceAccount(), sa.Config.Name
	js.mu.RUnlock()

	// Check to see where we are..
	rg := mset.raftGroup()

	// Track the new peers and check the ones that are current.
	mset.mu.RLock()
	replicas := mset.cfg.Replicas
	mset.mu.RUnlock()
	if len(rg.Peers) <= replicas {
		// Migration no longer happening, so not our job anymore
		return true
	}

	// Make sure we have correct cluster information on the other peers.
	ci := js.clusterInfo(rg)
	mset.checkClusterInfo(ci)

	newPeers, _, newPeerSet, oldPeerSet := genPeerInfo(rg.Peers, len(rg.Peers)-replicas)

	// If we are part of the new peerset and we have been passed the baton.
	// We will handle scale down.
	if newPeerSet[ourPeerId] {
		// First need to check on any consumers and make sure they have moved properly before scaling down ourselves.
		js.mu.RLock()
		var needToWait bool
		for name, c := range sa.consumers {
			if c.unsupported != nil {
				continue
			}
			for _, peer := range c.Group.Peers {
				// If we have peers still in the old set block.
				if oldPeerSet[peer] {
					s.Debugf("Scale down of '%s > %s' blocked by consumer '%s'", accName, streamName, name)
					needToWait = true
					break
				}
			}
			if needToWait {
				break
			}
		}
		js.mu.RUnlock()
		if needToWait {
			return false
		}
		// We are good to go, can scale down here.
		n.ProposeKnownPeers(newPeers)

		csa := sa.copyGroup()
		csa.Group.Peers = newPeers
		csa.Group.Preferred = ourPeerId
		csa.Group.Cluster = s.cachedClusterName()
		meta.ForwardProposal(encodeUpdateStreamAssignment(csa))
		s.Noticef("Scaling down '%s > %s' to %+v", accName, streamName, s.peerSetToNames(newPeers))
	} else {
		// We are the old leader here, from the original peer set.
		// We are simply waiting on the new peerset to be caught up so we can transfer leadership.
		var newLeaderPeer, newLeader string
		neededCurrent, current := replicas/2+1, 0

		for _, r := range ci.Replicas {
			if r.Current && newPeerSet[r.Peer] {
				current++
				if newLeader == _EMPTY_ {
					newLeaderPeer, newLeader = r.Peer, r.Name
				}
			}
		}
		// Check if we have a quorom.
		if current >= neededCurrent {
			s.Noticef("Transfer of stream leader for '%s > %s' to '%s'", accName, streamName, newLeader)
			n.ProposeKnownPeers(newPeers)
			n.StepDown(newLeaderPeer)
		}
	}
	return false
}

// Determine if we are migrating
func (mset *stream) isMigrating() bool {
	if mset == nil {
		return false
	}

	mset.mu.RLock()
	js, sa := mset.js, mset.sa
	mset.mu.RUnlock()

	js.mu.RLock()
	defer js.mu.RUnlock()

	// During migration we will always be R>1, even when we start R1.
	// So if we do not have a group or node, we know we are not migrating.
	if sa == nil || sa.Group == nil || sa.Group.node == nil {
		return false
	}
	// Desired state is driven by a v2.15 meta leader, which reconciles the group itself.
	// We can't complete that migration, and the peer count deliberately lags the replica
	// count while it's pending, so the legacy signature below would fire and never converge.
	if sa.Group.Desired != nil {
		return false
	}
	// The sign of migration is if our group peer count != configured replica count.
	if sa.Config.Replicas != len(sa.Group.Peers) {
		return true
	}
	return false
}

// resetClusteredState is called when a clustered stream had an error (e.g sequence mismatch, bad snapshot) and needs to be reset.
func (mset *stream) resetClusteredState(n RaftNode, err error) bool {
	mset.mu.RLock()
	s, js, jsa, sa, acc, node, name := mset.srv, mset.js, mset.jsa, mset.sa, mset.acc, mset.node, mset.nameLocked(false)
	stype, tierName, replicas := mset.cfg.Storage, mset.tier, mset.cfg.Replicas
	mset.mu.RUnlock()

	// The stream might already be deleted and not assigned to us anymore.
	// In any case, don't revive the stream if it's already closed.
	// A nil node means it was removed out from underneath us, which only happens
	// when the meta layer is deliberately tearing this monitor down.
	if mset.closed.Load() || node == nil || node.IsDeleted() {
		s.Warnf("Will not reset stream '%s > %s', stream is closed", acc, mset.name())
		// Explicitly returning true here, we want the outside to break out of the monitoring loop as well.
		return true
	}

	// The node we were monitoring is no longer this stream's node. The meta layer has
	// remapped us into a different group and is tearing this monitor down, so resetting
	// here would resurrect a group that has already been replaced.
	if n != nil && n != node {
		s.Warnf("Will not reset stream '%s > %s', raft group %q was replaced by %q",
			acc, mset.name(), n.Group(), node.Group())
		// Explicitly returning true here, we want the outside to break out of the monitoring loop as well.
		return true
	}

	assert.Unreachable("Reset clustered state", map[string]any{
		"stream":  name,
		"account": acc.Name,
		"err":     err,
	})

	// Stepdown regardless if we are the leader here.
	node.StepDown()

	// If we detect we are shutting down just return.
	if js != nil && js.isShuttingDown() {
		s.Debugf("Will not reset stream '%s > %s', JetStream shutting down", acc, mset.name())
		return false
	}

	// Server
	if js.limitsExceeded(stype) {
		s.Warnf("Will not reset stream '%s > %s', server resources exceeded", acc, mset.name())
		return false
	}

	// Account
	if exceeded, _ := jsa.limitsExceeded(stype, tierName, replicas); exceeded {
		s.Warnf("Stream '%s > %s' errored, account resources exceeded", acc, mset.name())
		return false
	}

	if errors.Is(err, errCatchupAbortedNoLeader) || err == errCatchupTooManyRetries {
		// Don't delete all state, could've just been temporarily unable to reach the leader.
		node.Stop()
	} else {
		// We delete our raft state. Will recreate.
		node.Delete()
	}

	// Preserve our current state and messages unless we have a first sequence mismatch.
	shouldDelete := err == errFirstSequenceMismatch

	// Need to do the rest in a separate Go routine.
	go func() {
		mset.stopMonitoring()
		mset.resetAndWaitOnConsumers()
		// Stop our stream.
		mset.stop(shouldDelete, false)

		if sa != nil {
			js.mu.Lock()
			if js.shuttingDown {
				js.mu.Unlock()
				return
			}

			s.Warnf("Resetting stream cluster state for '%s > %s'", sa.Client.serviceAccount(), sa.Config.Name)
			// Mark stream assignment as resetting, so we don't double-account reserved resources.
			// But only if we're not also releasing the resources as part of the delete.
			sa.resetting = !shouldDelete
			// Now wipe groups from assignments.
			sa.Group.node = nil
			var consumers []*consumerAssignment
			if cc := js.cluster; cc != nil && cc.meta != nil {
				ourID := cc.meta.ID()
				for _, ca := range sa.consumers {
					if ca.unsupported != nil {
						continue
					}
					if rg := ca.Group; rg != nil && rg.isMember(ourID) {
						rg.node = nil // Erase group raft/node state.
						consumers = append(consumers, ca)
					}
				}
			}
			js.mu.Unlock()

			// This will reset the stream and consumers.
			// Reset stream.
			js.processClusterCreateStream(acc, sa)
			// Reset consumers.
			for _, ca := range consumers {
				js.processClusterCreateConsumer(nil, ca, nil, false)
			}
		}
	}()

	return true
}

func isControlHdr(hdr []byte) bool {
	return bytes.HasPrefix(hdr, []byte("NATS/1.0 100 "))
}

// Apply our stream entries.
// Return maximum allowed applied value, if currently inside a batch, zero otherwise.
func (js *jetStream) applyStreamEntries(mset *stream, n RaftNode, ce *CommittedEntry, isRecovering bool, batch *batchApply) (uint64, error) {
	for i, e := range ce.Entries {
		// Ignore if lower-level catchup is started.
		// We don't need to optimize during this, all entries are handled as normal.
		if e.Type == EntryCatchup {
			continue
		}

		// Check if a batch is abandoned.
		if e.Type != EntryNormal && batch != nil && batch.id != _EMPTY_ {
			batch.rejectBatchState(mset)
		}

		if e.Type == EntryNormal {
			if len(e.Data) == 0 {
				return 0, errBadEntryOp
			}
			buf, op := e.Data, entryOp(e.Data[0])
			if op == batchMsgOp {
				batchId, batchSeq, _, _, err := decodeBatchMsg(buf[1:])
				if err != nil {
					panic(err.Error())
				}

				// Need to grab the stream lock first.
				if isRecovering {
					mset.mu.Lock()
				}

				// Previous batch (if any) was abandoned.
				if batch.id != _EMPTY_ && batchId != batch.id {
					batch.rejectBatchState(mset)
				}
				if batchSeq == 1 {
					// If this is the first message in the batch, need to mark the start index.
					// We'll continue to check batch-completeness and try to find the commit.
					// At that point we'll commit the whole batch.
					batch.rejectBatchState(mset)
					batch.entryStart = i
					batch.maxApplied = ce.Index - 1
				}
				batch.id = batchId

				// While recovering, we could come up in the middle of a compacted batch that has already been applied.
				// This is possible if two batches are part of the same append entry, and the first batch was fully
				// applied but the second wasn't.
				// If we still see the first message of the batch, we don't skip any messages of the batch here.
				if isRecovering {
					if batchSeq > 1 && batch.count == 0 {
						if skip, err := mset.skipBatchIfRecovering(batch, buf); err != nil || skip {
							mset.mu.Unlock()
							if err != nil {
								panic(err.Error())
							}
							continue
						}
					}
					mset.mu.Unlock()
				}

				batch.count++
				// If the sequence is not monotonically increasing/we identify gaps, the batch can't be accepted.
				if batchSeq != batch.count {
					batch.rejectBatchState(mset)
					continue
				}
				continue
			} else if op == batchCommitMsgOp {
				batchId, batchSeq, _, _, err := decodeBatchMsg(buf[1:])
				if err != nil {
					panic(err.Error())
				}
				mset.mu.Lock()
				// Previous batch (if any) was abandoned.
				if batch.id != _EMPTY_ && batchId != batch.id {
					batch.rejectBatchState(mset)
				}
				if batchSeq == 1 {
					// If this is the first message in the batch, need to mark the start index.
					// This is a batch of size one that immediately commits.
					batch.rejectBatchState(mset)
					batch.entryStart = i
					batch.maxApplied = ce.Index - 1
				}
				batch.id = batchId

				// While recovering, we could come up in the middle of a compacted batch that has already been applied.
				// This is possible if two batches are part of the same append entry, and the first batch was fully
				// applied but the second wasn't.
				// If we still see the first message of the batch, we don't skip any messages of the batch here.
				if isRecovering && batchSeq > 1 && batch.count == 0 {
					if skip, err := mset.skipBatchIfRecovering(batch, buf); err != nil || skip {
						mset.mu.Unlock()
						if err != nil {
							panic(err.Error())
						}
						continue
					}
				}

				batch.count++
				// Detected a gap, reject the batch.
				if batchSeq != batch.count {
					batch.rejectBatchState(mset)
					mset.mu.Unlock()
					continue
				}

				// Ensure the whole batch is fully isolated, and reads
				// can only happen after the full batch is committed.
				mset.mu.Unlock()
				mset.isolateMu.Lock()

				// Process any entries that are part of this batch but prior to the current one.
				var entries []*Entry
				for j, bce := range batch.entries {
					if j == 0 {
						// The first needs only the entries when the batch is started.
						entries = bce.Entries[batch.entryStart:]
					} else {
						// Otherwise, all entries are used.
						entries = bce.Entries
					}
					clearAndUnlock := func() {
						// Make sure to return remaining entries to the pool on an error.
						for _, nce := range batch.entries[j:] {
							nce.ReturnToPool()
						}
						// Important to clear, otherwise we could return the entries to the pool multiple times.
						batch.clearBatchState()
						mset.isolateMu.Unlock()
					}
					for _, entry := range entries {
						// Non-normal entries (e.g. EntryCatchup) can be buffered but must be ignored.
						if entry.Type != EntryNormal {
							continue
						}
						_, _, op, buf, err = decodeBatchMsg(entry.Data[1:])
						if err != nil {
							clearAndUnlock()
							panic(err.Error())
						}
						if err = js.applyStreamMsgOp(mset, op, buf, isRecovering, false); err != nil {
							clearAndUnlock()
							return 0, err
						}
					}
					// Return the entry to the pool now.
					bce.ReturnToPool()
				}
				if len(batch.entries) == 0 {
					// Get within the same entry, but within the range of this batch.
					entries = ce.Entries[batch.entryStart : i+1]
				} else {
					// Get all entries up to and including the current one.
					entries = ce.Entries[:i+1]
				}
				clearAndUnlock := func() {
					// Important to clear, otherwise we could return the entries to the pool multiple times.
					batch.clearBatchState()
					mset.isolateMu.Unlock()
				}
				// Process remaining entries in the current entry.
				for _, entry := range entries {
					// Non-normal entries (e.g. EntryCatchup) can be buffered but must be ignored.
					if entry.Type != EntryNormal {
						continue
					}
					_, _, op, buf, err = decodeBatchMsg(entry.Data[1:])
					if err != nil {
						clearAndUnlock()
						panic(err.Error())
					}
					if err = js.applyStreamMsgOp(mset, op, buf, isRecovering, false); err != nil {
						clearAndUnlock()
						return 0, err
					}
				}
				// Clear state, batch was successful.
				batch.clearBatchState()
				mset.isolateMu.Unlock()
				continue
			} else if batch != nil && batch.id != _EMPTY_ {
				// If a batch is abandoned without a commit, reject it.
				batch.rejectBatchState(mset)
			}

			switch op {
			case streamMsgOp, compressedStreamMsgOp:
				mbuf := buf[1:]
				if err := js.applyStreamMsgOp(mset, op, mbuf, isRecovering, true); err != nil {
					return 0, err
				}

			case deleteRangeOp:
				dr, err := decodeDeleteRange(buf[1:])
				if err != nil {
					if node := mset.raftNode(); node != nil {
						s := js.srv
						s.Errorf("JetStream cluster could not decode delete range for '%s > %s' [%s]",
							mset.account(), mset.name(), node.Group())
					}
					panic(err.Error())
				}
				if dr.Num == 0 {
					continue
				}
				// Delete ranges mutate message state, so hold the isolation lock like any other write.
				mset.isolateMu.Lock()
				mset.mu.Lock()
				first, num := dr.First, dr.Num
				lseq := first + num - 1
				if mset.lseq >= lseq {
					mset.mu.Unlock()
					mset.isolateMu.Unlock()
					continue
				}
				// Trim any prefix already applied so we only skip the uncovered tail.
				if mset.lseq >= first {
					first = mset.lseq + 1
					num = lseq - mset.lseq
				}
				if err = mset.store.SkipMsgs(first, num); err != nil {
					mset.mu.Unlock()
					mset.isolateMu.Unlock()
					js.srv.RateLimitWarnf("JetStream cluster failed to apply delete range [%d..%d] for '%s > %s': %v",
						first, lseq, mset.account().Name, mset.cfg.Name, err)
					return 0, err
				}
				mset.clearAllPreAcksInRange(first, lseq)
				mset.lseq = lseq
				mset.mu.Unlock()
				mset.isolateMu.Unlock()

			case deleteMsgOp:
				md, err := decodeMsgDelete(buf[1:])
				if err != nil {
					if node := mset.raftNode(); node != nil {
						s := js.srv
						s.Errorf("JetStream cluster could not decode delete msg for '%s > %s' [%s]",
							mset.account(), mset.name(), node.Group())
					}
					panic(err.Error())
				}
				s := js.server()

				var removed bool
				if md.NoErase {
					removed, err = mset.removeMsg(md.Seq)
				} else {
					removed, err = mset.eraseMsg(md.Seq)
				}
				// Only respond for deletes that originated from a client request.
				isClientReq := md.Client != nil

				var isLeader bool
				if node := mset.raftNode(); node != nil && node.Leader() {
					isLeader = true
				}

				if err == ErrStoreEOF {
					if isClientReq && isLeader && !isRecovering {
						var resp = JSApiMsgDeleteResponse{ApiResponse: ApiResponse{Type: JSApiMsgDeleteResponseType}}
						resp.Error = NewJSStreamMsgDeleteFailedError(err, Unless(err))
						s.sendAPIErrResponse(md.Client, mset.account(), md.Subject, md.Reply, _EMPTY_, s.jsonResponse(resp))
					}
					continue
				}

				if err != nil && !isRecovering {
					s.Debugf("JetStream cluster failed to delete stream msg %d from '%s > %s': %v",
						md.Seq, md.Client.serviceAccount(), md.Stream, err)
				}

				if isClientReq && isLeader && !isRecovering {
					var resp = JSApiMsgDeleteResponse{ApiResponse: ApiResponse{Type: JSApiMsgDeleteResponseType}}
					if err != nil {
						resp.Error = NewJSStreamMsgDeleteFailedError(err, Unless(err))
						s.sendAPIErrResponse(md.Client, mset.account(), md.Subject, md.Reply, _EMPTY_, s.jsonResponse(resp))
					} else if !removed {
						resp.Error = NewJSSequenceNotFoundError(md.Seq)
						s.sendAPIErrResponse(md.Client, mset.account(), md.Subject, md.Reply, _EMPTY_, s.jsonResponse(resp))
					} else {
						resp.Success = true
						s.sendAPIResponse(md.Client, mset.account(), md.Subject, md.Reply, _EMPTY_, s.jsonResponse(resp))
					}
				}
			case purgeStreamOp:
				sp, err := decodeStreamPurge(buf[1:])
				if err != nil {
					if node := mset.raftNode(); node != nil {
						s := js.srv
						s.Errorf("JetStream cluster could not decode purge msg for '%s > %s' [%s]",
							mset.account(), mset.name(), node.Group())
					}
					panic(err.Error())
				}
				// If no explicit request, fill in with leader stamped last sequence to protect ourselves on replay during server start.
				if sp.Request == nil || sp.Request.Sequence == 0 {
					purgeSeq := sp.LastSeq + 1
					if sp.Request == nil {
						sp.Request = &JSApiStreamPurgeRequest{Sequence: purgeSeq}
					} else if sp.Request.Keep == 0 {
						sp.Request.Sequence = purgeSeq
					} else if isRecovering {
						continue
					}
				}

				s := js.server()
				purged, err := mset.purge(sp.Request)
				if err != nil {
					s.Warnf("JetStream cluster failed to purge stream %q for account %q: %v", sp.Stream, sp.Client.serviceAccount(), err)
				}

				js.mu.RLock()
				isLeader := js.cluster.isStreamLeader(sp.Client.serviceAccount(), sp.Stream)
				js.mu.RUnlock()

				if isLeader && !isRecovering {
					var resp = JSApiStreamPurgeResponse{ApiResponse: ApiResponse{Type: JSApiStreamPurgeResponseType}}
					if err != nil {
						resp.Error = NewJSStreamGeneralError(err, Unless(err))
						s.sendAPIErrResponse(sp.Client, mset.account(), sp.Subject, sp.Reply, _EMPTY_, s.jsonResponse(resp))
					} else {
						resp.Purged = purged
						resp.Success = true
						s.sendAPIResponse(sp.Client, mset.account(), sp.Subject, sp.Reply, _EMPTY_, s.jsonResponse(resp))
					}
				}
			default:
				panic(fmt.Sprintf("JetStream Cluster Unknown group entry op type: %v", op))
			}
		} else if e.Type == EntrySnapshot {
			// Everything operates on new replicated state. Will convert legacy snapshots to this for processing.
			var ss *StreamReplicatedState

			onBadState := func(err error) {
				// If we are the leader or recovering, meaning we own the snapshot,
				// we should stepdown and clear our raft state since our snapshot is bad.
				if isRecovering || mset.IsLeader() {
					mset.mu.RLock()
					s, accName, streamName := mset.srv, mset.acc.GetName(), mset.cfg.Name
					mset.mu.RUnlock()
					s.Warnf("Detected bad stream state, resetting '%s > %s'", accName, streamName)
					mset.resetClusteredState(n, err)
				}
			}

			// Check if we are the new binary encoding.
			if IsEncodedStreamState(e.Data) {
				var err error
				ss, err = DecodeStreamState(e.Data)
				if err != nil {
					onBadState(err)
					return 0, err
				}
			} else {
				var snap streamSnapshot
				if err := json.Unmarshal(e.Data, &snap); err != nil {
					onBadState(err)
					return 0, err
				}
				// Convert over to StreamReplicatedState
				ss = &StreamReplicatedState{
					Msgs:     snap.Msgs,
					Bytes:    snap.Bytes,
					FirstSeq: snap.FirstSeq,
					LastSeq:  snap.LastSeq,
					Failed:   snap.Failed,
				}
				if len(snap.Deleted) > 0 {
					ss.Deleted = append(ss.Deleted, DeleteSlice(snap.Deleted))
				}
			}

			if err := mset.processSnapshot(ss, ce.Index); err != nil {
				return 0, err
			}
		} else if e.Type == EntryRemovePeer {
			js.mu.RLock()
			var ourID string
			if js.cluster != nil && js.cluster.meta != nil {
				ourID = js.cluster.meta.ID()
			}
			js.mu.RUnlock()
			// Cancel any catchup if one was still running for this peer.
			if mset != nil {
				mset.mu.Lock()
				mset.clearCatchupPeerLocked(string(e.Data))
				mset.mu.Unlock()
			}
			// We only need to do processing if this is us.
			if peer := string(e.Data); peer == ourID && mset != nil {
				// Double check here with the registered stream assignment.
				shouldRemove := true
				if sa := mset.streamAssignment(); sa != nil && sa.Group != nil {
					js.mu.RLock()
					shouldRemove = !sa.Group.isMember(ourID)
					js.mu.RUnlock()
				}
				if shouldRemove {
					mset.stop(true, false)
				}
			}
		}
	}

	// If we're still actively processing a batch, must store the entry in-memory
	// to come back to it later once we find the commit.
	if batch != nil && batch.id != _EMPTY_ {
		if batch.entries == nil {
			batch.entries = []*CommittedEntry{ce}
		} else {
			batch.entries = append(batch.entries, ce)
		}
		maxApplied := batch.maxApplied
		return maxApplied, nil
	}
	return 0, nil
}

// skipBatchIfRecovering returns whether the batched message can be skipped because the batch was already fully applied.
// Stream lock should be held.
func (mset *stream) skipBatchIfRecovering(batch *batchApply, buf []byte) (bool, error) {
	_, _, op, mbuf, err := decodeBatchMsg(buf[1:])
	if err != nil {
		return false, err
	}

	if op == compressedStreamMsgOp {
		if mbuf, err = s2.Decode(nil, mbuf); err != nil {
			return false, err
		}
	}

	_, _, _, _, lseq, _, _, err := decodeStreamMsg(mbuf)
	if err != nil {
		return false, err
	}

	// Grab last sequence and CLFS.
	last, clfs := mset.lastSeqAndCLFS()

	// We can skip if we know this is less than what we already have.
	if lseq-clfs < last {
		mset.srv.Debugf("Apply stream entries for '%s > %s' skipping message with sequence %d with last of %d",
			mset.accountLocked(false), mset.nameLocked(false), lseq+1-clfs, last)
		// Check for any preAcks in case we are interest based.
		mset.clearAllPreAcks(lseq + 1 - clfs)
		batch.clearBatchState()
		return true, nil
	}
	return false, nil
}

// needIsolation should be false only if the caller already holds isolateMu
// across a whole atomic batch; mset.mu must NOT be held by the caller.
func (js *jetStream) applyStreamMsgOp(mset *stream, op entryOp, mbuf []byte, isRecovering bool, needIsolation bool) error {
	s := js.srv

	if op == compressedStreamMsgOp {
		var err error
		mbuf, err = s2.Decode(nil, mbuf)
		if err != nil {
			panic(err.Error())
		}
	}

	subject, reply, hdr, msg, lseq, ts, sourced, err := decodeStreamMsg(mbuf)
	if err != nil {
		// We're going to panic below, we're not holding the stream lock, so
		// we'll not deadlock when trying to get the raft node.
		if node := mset.raftNode(); node != nil {
			s.Errorf("JetStream cluster could not decode stream msg for '%s > %s' [%s]",
				mset.account(), mset.name(), node.Group())
		}
		panic(err.Error())
	}

	// Check for flowcontrol here.
	if len(msg) == 0 && len(hdr) > 0 && reply != _EMPTY_ && isControlHdr(hdr) {
		if !isRecovering {
			mset.mu.RLock()
			mset.sendFlowControlReply(reply, hdr)
			mset.mu.RUnlock()
		}
		return nil
	}

	mset.mu.RLock()
	// Grab last sequence and CLFS.
	last, clfs := mset.lastSeqAndCLFS()
	mset.mu.RUnlock()

	// We can skip if we know this is less than what we already have.
	if lseq-clfs < last {
		mset.mu.Lock()
		s.Debugf("Apply stream entries for '%s > %s' skipping message with sequence %d with last of %d",
			mset.accountLocked(false), mset.nameLocked(false), lseq+1-clfs, last)
		// Check for any preAcks in case we are interest based.
		mset.clearAllPreAcks(lseq + 1 - clfs)
		mset.mu.Unlock()
		return nil
	}

	// Skip by hand here since first msg special case.
	// Reason is sequence is unsigned and for lseq being 0
	// the lseq under stream would have to be -1.
	if lseq == 0 && last != 0 {
		return nil
	}

	// Messages to be skipped have no subject or timestamp or msg or hdr.
	if subject == _EMPTY_ && ts == 0 && len(msg) == 0 && len(hdr) == 0 {
		// Skip and update our lseq.
		last, err := mset.store.SkipMsg(0)
		if err != nil {
			return err
		}
		mset.mu.Lock()
		mset.lseq = last
		mset.clearAllPreAcks(last)
		mset.mu.Unlock()
		return nil
	}

	var mt *msgTrace
	// If not recovering, see if we find a message trace object for this
	// sequence. Only the leader that has proposed this entry will have
	// stored the trace info.
	if !isRecovering {
		mt = mset.getAndDeleteMsgTrace(lseq)
	}
	// Process the actual message here.
	err = mset.processJetStreamMsg(subject, reply, hdr, msg, lseq, ts, mt, sourced, needIsolation)

	// Take into account subject transforms, if any.
	// The untransformed subject is replicated, but the transformed subject is used for consistency checks below.
	csubject := subject
	if mset.inflightTransform != nil {
		mset.clMu.Lock()
		if subj, found := mset.inflightTransform[lseq]; found {
			csubject = subj
			delete(mset.inflightTransform, lseq)
		}
		mset.clMu.Unlock()
	}

	// If we have inflight make sure to clear after processing.
	// TODO(dlc) - technically check on inflight != nil could cause datarace.
	// But do not want to acquire lock since tracking this will be rare.
	if mset.inflight != nil {
		mset.clMu.Lock()
		if i, found := mset.inflight[csubject]; found {
			// Decrement from pending operations. Once it reaches zero, it can be deleted.
			if i.ops > 0 {
				var sz uint64
				if mset.stype == FileStorage {
					sz = fileStoreMsgSizeRaw(len(csubject), len(hdr), len(msg))
				} else {
					sz = memStoreMsgSizeRaw(len(csubject), len(hdr), len(msg))
				}
				if i.bytes >= sz {
					i.bytes -= sz
				} else {
					i.bytes = 0
				}
				i.ops--
			}
			if i.ops == 0 {
				delete(mset.inflight, csubject)
			}
		}
		mset.clMu.Unlock()
	}

	// Update running total for counter.
	if mset.clusteredCounterTotal != nil {
		mset.clMu.Lock()
		if counter, found := mset.clusteredCounterTotal[csubject]; found {
			// Decrement from pending operations. Once it reaches zero, it can be deleted.
			if counter.ops > 0 {
				counter.ops--
			}
			if counter.ops == 0 {
				delete(mset.clusteredCounterTotal, csubject)
			}
		}
		mset.clMu.Unlock()
	}

	// Clear expected per subject state after processing.
	if mset.expectedPerSubjectSequence != nil {
		mset.clMu.Lock()
		if subj, found := mset.expectedPerSubjectSequence[lseq]; found {
			delete(mset.expectedPerSubjectSequence, lseq)
			delete(mset.expectedPerSubjectInProcess, subj)
		}
		mset.clMu.Unlock()
	}

	if err != nil {
		if err == errLastSeqMismatch {

			var state StreamState
			mset.store.FastState(&state)

			// If we have no msgs and the other side is delivering us a sequence past where we
			// should be reset. This is possible if the other side has a stale snapshot and no longer
			// has those messages. So compact and retry to reset.
			if state.Msgs == 0 {
				_, err = mset.store.Compact(lseq + 1)
				if err == nil {
					// Retry
					err = mset.processJetStreamMsg(subject, reply, hdr, msg, lseq, ts, mt, sourced, needIsolation)
				}
			}
			// FIXME(dlc) - We could just run a catchup with a request defining the span between what we expected
			// and what we got.
		}

		// Only return in place if we are going to reset our stream or we are out of space, or we are closed.
		if isClusterResetErr(err) || isOutOfSpaceErr(err) || err == errStreamClosed {
			return err
		}
		s.Debugf("Apply stream entries for '%s > %s' got error processing message: %v",
			mset.accountLocked(true), mset.nameLocked(true), err)

		// There are some errors that we can't recover from.
		if err != ErrMaxMsgs && err != ErrMaxBytes && err != ErrMaxMsgsPerSubject && err != ErrMsgTooLarge && err != ErrStoreClosed {
			return err
		}
	}
	return nil
}

// Returns the PeerInfo for all replicas of a raft node. This is different than node.Peers()
// and is used for external facing advisories.
func (s *Server) replicas(node RaftNode) []*PeerInfo {
	var replicas []*PeerInfo
	for _, rp := range node.Peers() {
		if sir, ok := s.nodeToInfo.Load(rp.ID); ok && sir != nil {
			si := sir.(nodeInfo)
			pi := &PeerInfo{Peer: rp.ID, Name: si.name, Current: rp.Current, Offline: si.offline, Lag: rp.Lag}
			if !rp.Last.IsZero() {
				pi.Active = time.Since(rp.Last)
			}
			replicas = append(replicas, pi)
		}
	}
	return replicas
}

// Process a leader change for the clustered stream.
func (js *jetStream) processStreamLeaderChange(mset *stream, isLeader bool, term uint64) {
	if mset == nil {
		return
	}
	sa := mset.streamAssignment()
	if sa == nil {
		return
	}

	// Acquire clMu before ddMu so any inflight proposals finish first, and we can
	// clean up if they added new dedupe IDs.
	mset.clMu.Lock()

	// Clear inflight dedupe IDs, where seq=0.
	mset.ddMu.Lock()
	var removed int
	for i := len(mset.ddarr) - 1; i >= mset.ddindex; i-- {
		dde := mset.ddarr[i]
		if dde.seq != 0 {
			break
		}
		removed++
		delete(mset.ddmap, dde.id)
	}
	if removed > 0 {
		if len(mset.ddmap) > 0 {
			mset.ddarr = mset.ddarr[:len(mset.ddarr)-removed]
		} else {
			mset.ddmap = nil
			mset.ddarr = nil
			mset.ddindex = 0
		}
	}
	mset.ddMu.Unlock()

	// Clear inflight if we have it.
	mset.inflight = nil
	mset.inflightTransform = nil
	// Clear running counter totals.
	mset.clusteredCounterTotal = nil
	// Clear expected per subject state.
	mset.expectedPerSubjectSequence = nil
	mset.expectedPerSubjectInProcess = nil

	// Clear clseq on every leader transition. recalculateClusteredSeq
	// repopulates it on the next proposal.
	if mset.clseq > 0 {
		mset.clseq = 0
	}
	mset.clMu.Unlock()

	js.mu.RLock()
	s, account, err := js.srv, sa.Client.serviceAccount(), sa.err
	client, subject, reply := sa.Client, sa.Subject, sa.Reply
	hasResponded := sa.markResponded()
	js.mu.RUnlock()

	streamName := mset.name()

	if isLeader {
		s.Noticef("JetStream cluster new stream leader for '%s > %s'", account, streamName)
		s.sendStreamLeaderElectAdvisory(mset)
	} else {
		// We are stepping down.
		// Make sure if we are doing so because we have lost quorum that we send the appropriate advisories.
		if node := mset.raftNode(); node != nil && !node.Quorum() && time.Since(node.Created()) > 5*time.Second {
			s.sendStreamLostQuorumAdvisory(mset)
		}
	}

	// Tell stream to switch leader status.
	mset.setLeader(isLeader, term)

	if !isLeader || hasResponded {
		return
	}

	acc, _ := s.LookupAccount(account)
	if acc == nil {
		return
	}

	// Send our response.
	var resp = JSApiStreamCreateResponse{ApiResponse: ApiResponse{Type: JSApiStreamCreateResponseType}}
	if err != nil {
		resp.Error = NewJSStreamCreateError(err, Unless(err))
		s.sendAPIErrResponse(client, acc, subject, reply, _EMPTY_, s.jsonResponse(&resp))
	} else {
		// Report the config as requested, the stream can still be running at its origin.
		msetCfg := js.targetStreamConfig(mset, mset.config())
		resp.StreamInfo = &StreamInfo{
			Created:   mset.createdTime(),
			State:     mset.state(),
			Config:    *setDynamicStreamMetadata(&msetCfg),
			Cluster:   js.clusterInfo(mset.raftGroup()),
			Sources:   mset.sourcesInfo(),
			Mirror:    mset.mirrorInfo(),
			TimeStamp: time.Now().UTC(),
		}
		resp.DidCreate = true
		s.sendAPIResponse(client, acc, subject, reply, _EMPTY_, s.jsonResponse(&resp))
		if node := mset.raftNode(); node != nil {
			mset.sendCreateAdvisory()
		}
	}
}

// Fixed value ok for now.
const lostQuorumAdvInterval = 10 * time.Second

// Determines if we should send lost quorum advisory. We throttle these after first one.
func (mset *stream) shouldSendLostQuorum() bool {
	mset.mu.Lock()
	defer mset.mu.Unlock()
	if time.Since(mset.lqsent) >= lostQuorumAdvInterval {
		mset.lqsent = time.Now()
		return true
	}
	return false
}

func (s *Server) sendStreamLostQuorumAdvisory(mset *stream) {
	if mset == nil {
		return
	}
	node, stream, acc := mset.raftNode(), mset.name(), mset.account()
	if node == nil {
		return
	}
	if !mset.shouldSendLostQuorum() {
		return
	}

	s.Warnf("JetStream cluster stream '%s > %s' has NO quorum, stalled", acc.GetName(), stream)

	subj := JSAdvisoryStreamQuorumLostPre + "." + stream
	adv := &JSStreamQuorumLostAdvisory{
		TypedEvent: TypedEvent{
			Type: JSStreamQuorumLostAdvisoryType,
			ID:   nuid.Next(),
			Time: time.Now().UTC(),
		},
		Stream:   stream,
		Replicas: s.replicas(node),
		Domain:   s.getOpts().JetStreamDomain,
	}

	// Send to the user's account if not the system account.
	if acc != s.SystemAccount() {
		s.publishAdvisory(acc, subj, adv)
	}
	// Now do system level one. Place account info in adv, and nil account means system.
	adv.Account = acc.GetName()
	s.publishAdvisory(nil, subj, adv)
}

func (s *Server) sendStreamLeaderElectAdvisory(mset *stream) {
	if mset == nil {
		return
	}
	node, stream, acc := mset.raftNode(), mset.name(), mset.account()
	if node == nil {
		return
	}
	subj := JSAdvisoryStreamLeaderElectedPre + "." + stream
	adv := &JSStreamLeaderElectedAdvisory{
		TypedEvent: TypedEvent{
			Type: JSStreamLeaderElectedAdvisoryType,
			ID:   nuid.Next(),
			Time: time.Now().UTC(),
		},
		Stream:   stream,
		Leader:   s.serverNameForNode(node.GroupLeader()),
		Replicas: s.replicas(node),
		Domain:   s.getOpts().JetStreamDomain,
	}

	// Send to the user's account if not the system account.
	if acc != s.SystemAccount() {
		s.publishAdvisory(acc, subj, adv)
	}
	// Now do system level one. Place account info in adv, and nil account means system.
	adv.Account = acc.GetName()
	s.publishAdvisory(nil, subj, adv)
}

// Will look up a stream assignment.
// Lock should be held.
func (js *jetStream) streamAssignment(account, stream string) (sa *streamAssignment) {
	cc := js.cluster
	if cc == nil {
		return nil
	}

	if as := cc.streams[account]; as != nil {
		sa = as[stream]
	}
	return sa
}

// Will look up a stream assignment, either an applied or inflight assignment.
// Lock should be held.
func (js *jetStream) streamAssignmentOrInflight(account, stream string) *streamAssignment {
	cc := js.cluster
	if cc == nil {
		return nil
	}
	if streams, ok := cc.inflightStreams[account]; ok {
		if inflight, ok := streams[stream]; ok {
			if !inflight.deleted {
				return inflight.streamAssignment
			} else {
				return nil
			}
		}
	}

	if as := cc.streams[account]; as != nil {
		return as[stream]
	}
	return nil
}

// Will gather all stream assignments for a specific account, both applied and inflight assignments.
// Lock should be held.
func (js *jetStream) streamAssignmentsOrInflightSeq(account string) iter.Seq[*streamAssignment] {
	return func(yield func(*streamAssignment) bool) {
		cc := js.cluster
		if cc == nil {
			return
		}
		inflight := cc.inflightStreams[account]
		for _, i := range inflight {
			if !i.deleted && !yield(i.streamAssignment) {
				return
			}
		}
		for _, sa := range cc.streams[account] {
			// Skip if we already iterated over it as inflight.
			if _, ok := inflight[sa.Config.Name]; ok {
				continue
			}
			if !yield(sa) {
				return
			}
		}
	}
}

// Will gather all stream assignments for all accounts, both applied and inflight assignments.
// Lock should be held.
func (js *jetStream) streamAssignmentsOrInflightSeqAllAccounts() iter.Seq2[string, *streamAssignment] {
	return func(yield func(string, *streamAssignment) bool) {
		cc := js.cluster
		if cc == nil {
			return
		}
		for accName, inflight := range cc.inflightStreams {
			for _, i := range inflight {
				if !i.deleted && !yield(accName, i.streamAssignment) {
					return
				}
			}
		}
		for accName, asa := range cc.streams {
			for _, sa := range asa {
				// Skip if we already iterated over it as inflight.
				if inflight, ok := cc.inflightStreams[accName]; ok {
					if _, ok := inflight[sa.Config.Name]; ok {
						continue
					}
				}
				if !yield(accName, sa) {
					return
				}
			}
		}
	}
}

// processStreamAssignment is called when followers have replicated an assignment.
func (js *jetStream) processStreamAssignment(sa *streamAssignment) {
	js.mu.Lock()
	s, cc := js.srv, js.cluster
	accName, stream := sa.Client.serviceAccount(), sa.Config.Name
	noMeta := cc == nil || cc.meta == nil
	var ourID string
	if !noMeta {
		ourID = cc.meta.ID()
	}
	var isMember bool
	if sa.Group != nil && ourID != _EMPTY_ {
		isMember = sa.Group.isMember(ourID)
	}

	if s == nil || noMeta {
		js.mu.Unlock()
		return
	}

	// Remove this stream from the inflight proposals
	cc.removeInflightStreamProposal(accName, sa.Config.Name)

	accStreams := cc.streams[accName]
	if accStreams == nil {
		accStreams = make(map[string]*streamAssignment)
	} else if osa := accStreams[stream]; osa != nil {
		if osa != sa {
			// Copy over private existing state from former SA.
			if sa.Group != nil {
				sa.Group.node = osa.Group.node
			}
			sa.consumers = osa.consumers
			if osa.hasResponded() {
				sa.markResponded()
			}
			sa.err = osa.err
		}
		// Unsubscribe if it was previously unsupported.
		if osa.unsupported != nil {
			osa.unsupported.closeInfoSub(js.srv)
			// If we've seen unsupported once, it remains for the lifetime of this server process.
			if sa.unsupported == nil {
				sa.unsupported = osa.unsupported
			}
		}
	}

	// Update our state.
	accStreams[stream] = sa
	cc.streams[accName] = accStreams
	hasResponded := sa.hasResponded()

	// If unsupported, we can't register any further.
	if sa.unsupported != nil {
		sa.unsupported.setupInfoSub(s, sa)
		s.Warnf("Detected unsupported stream '%s > %s': %s", accName, stream, sa.unsupported.reason)
		js.mu.Unlock()

		// Need to stop the stream, we can't keep running with an old config.
		acc, err := s.lookupOrFetchAccount(accName, isMember)
		if err != nil {
			return
		}
		mset, err := acc.lookupStream(stream)
		if err != nil || mset.closed.Load() {
			return
		}
		s.Warnf("Stopping unsupported stream '%s > %s'", accName, stream)
		mset.stop(false, false)
		return
	}
	js.mu.Unlock()

	acc, err := s.lookupOrFetchAccount(accName, isMember)
	if err != nil {
		ll := fmt.Sprintf("Account [%s] lookup for stream create failed: %v", accName, err)
		if isMember {
			if !hasResponded {
				// If we can not lookup the account and we are a member, send this result back to the metacontroller leader.
				result := &streamAssignmentResult{
					Account:  accName,
					Stream:   stream,
					Response: &JSApiStreamCreateResponse{ApiResponse: ApiResponse{Type: JSApiStreamCreateResponseType}},
				}
				result.Response.Error = NewJSNoAccountError()
				s.sendInternalMsgLocked(streamAssignmentSubj, _EMPTY_, nil, result)
			}
			s.Warnf(ll)
		} else {
			s.Debugf(ll)
		}
		return
	}

	// Check if this is for us..
	if isMember {
		js.processClusterCreateStream(acc, sa)
	} else if mset, _ := acc.lookupStream(sa.Config.Name); mset != nil {
		// We have one here even though we are not a member. This can happen on re-assignment.
		s.removeStream(mset, sa)
	}

	// If this stream assignment does not have a sync subject (bug) set that the meta-leader should check when elected.
	if sa.Sync == _EMPTY_ {
		js.mu.Lock()
		cc.streamsCheck = true
		js.mu.Unlock()
	}
}

// processUpdateStreamAssignment is called when followers have replicated an updated assignment.
func (js *jetStream) processUpdateStreamAssignment(sa *streamAssignment) {
	js.mu.RLock()
	s, cc := js.srv, js.cluster
	js.mu.RUnlock()
	if s == nil || cc == nil {
		// TODO(dlc) - debug at least
		return
	}

	accName := sa.Client.serviceAccount()
	stream := sa.Config.Name

	js.mu.Lock()
	if cc.meta == nil {
		js.mu.Unlock()
		return
	}

	// Remove this stream from the inflight proposals
	cc.removeInflightStreamProposal(accName, sa.Config.Name)

	ourID := cc.meta.ID()

	var isMember bool
	if sa.Group != nil {
		isMember = sa.Group.isMember(ourID)
	}

	accStreams := cc.streams[accName]
	if accStreams == nil {
		js.mu.Unlock()
		return
	}
	osa := accStreams[stream]
	if osa == nil {
		js.mu.Unlock()
		return
	}

	// Copy over private existing state from former SA.
	if sa.Group != nil {
		sa.Group.node = osa.Group.node
	}
	sa.consumers = osa.consumers
	sa.err = osa.err

	// If we detect we are scaling down to 1, non-clustered, and we had a previous node, clear it here.
	// Note the group can converge onto one peer without the config scaling down based on peer-removes.
	if len(sa.Group.Peers) == 1 && sa.Group.node != nil && sa.Group.Desired == nil {
		sa.Group.node = nil
	}

	// Update our state.
	accStreams[stream] = sa
	cc.streams[accName] = accStreams

	// Make sure we respond if we are a member.
	if isMember {
		sa.clearResponded()
	} else {
		// Make sure to clean up any old node in case this stream moves back here.
		if sa.Group != nil {
			sa.Group.node = nil
		}
	}

	// Unsubscribe if it was previously unsupported.
	if osa.unsupported != nil {
		osa.unsupported.closeInfoSub(js.srv)
		// If we've seen unsupported once, it remains for the lifetime of this server process.
		if sa.unsupported == nil {
			sa.unsupported = osa.unsupported
		}
	}

	// If unsupported, we can't register any further.
	if sa.unsupported != nil {
		sa.unsupported.setupInfoSub(s, sa)
		s.Warnf("Detected unsupported stream '%s > %s': %s", accName, stream, sa.unsupported.reason)
		js.mu.Unlock()

		// Need to stop the stream, we can't keep running with an old config.
		acc, err := s.lookupOrFetchAccount(accName, isMember)
		if err != nil {
			return
		}
		mset, err := acc.lookupStream(stream)
		if err != nil || mset.closed.Load() {
			return
		}
		s.Warnf("Stopping unsupported stream '%s > %s'", accName, stream)
		mset.stop(false, false)
		return
	}
	js.mu.Unlock()

	acc, err := s.lookupOrFetchAccount(accName, isMember)
	if err != nil {
		ll := fmt.Sprintf("Update Stream Account %s, error on lookup: %v", accName, err)
		if isMember {
			s.Warnf(ll)
		} else {
			s.Debugf(ll)
		}
		return
	}

	// Check if this is for us..
	if isMember {
		js.processClusterUpdateStream(acc, osa, sa)
	} else if mset, _ := acc.lookupStream(sa.Config.Name); mset != nil {
		// We have one here even though we are not a member. This can happen on re-assignment.
		s.removeStream(mset, sa)
	}
}

// Common function to remove ourselves from this server.
// This can happen on re-assignment, move, etc
func (s *Server) removeStream(mset *stream, nsa *streamAssignment) {
	if mset == nil {
		return
	}
	// Make sure to use the new stream assignment, not our own.
	s.Debugf("JetStream removing stream '%s > %s' from this server", nsa.Client.serviceAccount(), nsa.Config.Name)
	if node := mset.raftNode(); node != nil {
		node.StepDown(nsa.Group.Preferred)
		// shutdown monitor by shutting down raft.
		node.Delete()
	}

	var isShuttingDown bool
	// Make sure this node is no longer attached to our stream assignment.
	if js, _ := s.getJetStreamCluster(); js != nil {
		js.mu.Lock()
		nsa.Group.node = nil
		nsa.err = nil
		isShuttingDown = js.shuttingDown
		js.mu.Unlock()
	}

	if !isShuttingDown {
		// wait for monitor to be shutdown.
		mset.stopMonitoring()
	}
	mset.stop(true, false)
}

// processClusterUpdateStream is called when we have a stream assignment that
// has been updated for an existing assignment and we are a member.
func (js *jetStream) processClusterUpdateStream(acc *Account, osa, sa *streamAssignment) {
	if sa == nil {
		return
	}

	js.mu.RLock()
	s, rg, desired := js.srv, sa.Group, sa.Group.Desired
	client, subject, reply := sa.Client, sa.Subject, sa.Reply
	alreadyRunning, numReplicas := osa.Group.node != nil, len(rg.Peers)
	// A remap below clears alreadyRunning, but we must still know whether we were
	// running clustered before, or we'd skip the cleanup we owe on a downgrade.
	wasRunning := alreadyRunning
	wasClustered := len(osa.Group.Peers) > 1 || osa.Group.Desired != nil
	needsNode := rg.node == nil
	storage, cfg := sa.Config.Storage, sa.Config
	newCfg := cfg.atDesiredOrigin(rg)
	recovering, raftRecovering := sa.recovering, sa.recovering || sa.catchup
	hasResponded := sa.markResponded()
	hadErr := sa.err != nil
	js.mu.RUnlock()

	mset, err := acc.lookupStream(cfg.Name)
	if err == nil && mset != nil {
		// Make sure we have not had a new group assigned to us.
		if osa.Group.Name != sa.Group.Name {
			s.Warnf("JetStream cluster detected stream remapping for '%s > %s' from %q to %q",
				acc, cfg.Name, osa.Group.Name, sa.Group.Name)
			mset.removeNode()
			mset.stopMonitoring()
			alreadyRunning, needsNode = false, true
			// Make sure to clear from original.
			js.mu.Lock()
			osa.Group.node = nil
			js.mu.Unlock()
		}

		if !alreadyRunning && (numReplicas > 1 || desired != nil) {
			if needsNode {
				// Must run before startClusterSubs reads mset.sa.Sync.
				mset.setStreamAssignment(sa)

				// Since we are scaling up we want to make sure our sync subject
				// is registered before we start our raft node.
				mset.mu.Lock()
				mset.startClusterSubs()
				mset.mu.Unlock()

				js.createRaftGroup(acc.GetName(), rg, raftRecovering, storage, pprofLabels{
					"type":    "stream",
					"account": mset.accName(),
					"stream":  mset.name(),
				})
			}
			mset.startMonitorWg()
			// Start monitoring..
			started := s.startGoRoutine(
				func() { js.monitorStream(mset, sa, needsNode) },
				pprofLabels{
					"type":    "stream",
					"account": mset.accName(),
					"stream":  mset.name(),
				},
			)
			if !started {
				mset.monitorWg.Done()
			}
		} else if numReplicas == 1 && desired == nil && wasRunning {
			// We downgraded to R1. Make sure we cleanup the raft node and the stream monitor.
			mset.removeNode()
			mset.stopMonitoring()
			// In case we need to shutdown the cluster specific subs, etc.
			mset.mu.Lock()
			// Stop responding to sync requests.
			mset.stopClusterSubs()
			// Clear catchup state
			mset.clearAllCatchupPeers()
			mset.mu.Unlock()
			// Remove from meta layer.
			js.mu.Lock()
			rg.node = nil
			js.mu.Unlock()
		}
		// Set the new stream assignment.
		mset.setStreamAssignment(sa)

		// Call update.
		err = mset.updateWithAdvisory(newCfg, !recovering, false)
	}

	// If not found we must be expanding into this node since if we are here we know we are a member.
	if err == ErrJetStreamStreamNotFound {
		js.processStreamAssignment(sa)
		return
	}

	if err != nil {
		js.mu.Lock()
		s.Warnf("Stream update failed for '%s > %s': %v", sa.Client.serviceAccount(), sa.Config.Name, err)
		sa.err = err
		result := &streamAssignmentResult{
			Account:  sa.Client.serviceAccount(),
			Stream:   sa.Config.Name,
			Response: &JSApiStreamCreateResponse{ApiResponse: ApiResponse{Type: JSApiStreamCreateResponseType}},
			Update:   true,
		}
		result.Response.Error = NewJSStreamGeneralError(err, Unless(err))
		js.mu.Unlock()

		// Send response to the metadata leader. They will forward to the user as needed.
		s.sendInternalMsgLocked(streamAssignmentSubj, _EMPTY_, nil, result)
		return
	} else if hadErr {
		js.mu.Lock()
		sa.err = nil
		js.mu.Unlock()
	}

	isLeader := mset.IsLeader()

	// If the stream is scaled down, there is a chance we weren't already the leader.
	if isLeader && numReplicas == 1 && desired == nil && wasClustered {
		js.processStreamLeaderChange(mset, true, 0)
	}

	// Check for missing syncSubject bug.
	if isLeader && osa != nil && osa.Sync == _EMPTY_ {
		if node := mset.raftNode(); node != nil {
			node.StepDown()
		}
		return
	}

	// If we were a single node being promoted assume leadership role for purpose of responding.
	if !hasResponded && !isLeader && !alreadyRunning {
		isLeader = true
	}

	// Check if we should bail.
	if !isLeader || hasResponded || recovering {
		return
	}

	// Send our response.
	var resp = JSApiStreamUpdateResponse{ApiResponse: ApiResponse{Type: JSApiStreamUpdateResponseType}}
	// Report the config as requested, the stream can still be running at its origin.
	// Reading cfg without js.mu is safe, an assignment's config is never changed in place.
	msetCfg := mset.config().atDesiredTarget(cfg)
	resp.StreamInfo = &StreamInfo{
		Created:   mset.createdTime(),
		State:     mset.state(),
		Config:    *setDynamicStreamMetadata(&msetCfg),
		Cluster:   js.clusterInfo(mset.raftGroup()),
		Mirror:    mset.mirrorInfo(),
		Sources:   mset.sourcesInfo(),
		TimeStamp: time.Now().UTC(),
	}

	s.sendAPIResponse(client, acc, subject, reply, _EMPTY_, s.jsonResponse(&resp))
}

// processClusterCreateStream is called when we have a stream assignment that
// has been committed and this server is a member of the peer group.
func (js *jetStream) processClusterCreateStream(acc *Account, sa *streamAssignment) {
	if sa == nil {
		return
	}

	js.mu.RLock()
	s, rg, created := js.srv, sa.Group, sa.Created
	alreadyRunning := rg.node != nil
	newCfg := sa.Config.atDesiredOrigin(rg)
	storage := sa.Config.Storage
	restore := sa.Restore
	raftRecovering := sa.recovering || sa.catchup
	hadErr := sa.err != nil
	js.mu.RUnlock()

	// Process the raft group and make sure it's running if needed.
	_, err := js.createRaftGroup(acc.GetName(), rg, raftRecovering, storage, pprofLabels{
		"type":    "stream",
		"account": acc.Name,
		"stream":  sa.Config.Name,
	})

	// If we are restoring, create the stream if we are R>1 and not the preferred who handles the
	// receipt of the snapshot itself.
	shouldCreate := true
	if restore != nil {
		if len(rg.Peers) == 1 || rg.node != nil && rg.node.ID() == rg.Preferred {
			shouldCreate = false
		} else {
			js.mu.Lock()
			sa.Restore = nil
			js.mu.Unlock()
		}
	}

	// Our stream.
	var mset *stream

	// Process here if not restoring or not the leader.
	if shouldCreate && err == nil {
		// Go ahead and create or update the stream.
		mset, err = acc.lookupStream(sa.Config.Name)
		if err == nil && mset != nil {
			osa := mset.streamAssignment()
			// If we already have a stream assignment and they are the same exact config, short circuit here.
			if osa != nil {
				// Set the index name on both to ensure the DeepEqual works.
				js.mu.Lock()
				matchSourceIndexNames(sa.Config, osa.Config)
				js.mu.Unlock()
				if reflect.DeepEqual(osa.Config, sa.Config) {
					if sa.Group.Name == osa.Group.Name && reflect.DeepEqual(sa.Group.Peers, osa.Group.Peers) &&
						reflect.DeepEqual(sa.Group.Desired, osa.Group.Desired) {
						// Since this already exists we know it succeeded, just respond to this caller.
						js.mu.RLock()
						client, subject, reply, recovering := sa.Client, sa.Subject, sa.Reply, sa.recovering
						js.mu.RUnlock()

						if !recovering {
							var resp = JSApiStreamCreateResponse{ApiResponse: ApiResponse{Type: JSApiStreamCreateResponseType}}
							// Report the config as requested, the stream can still be running at its origin.
							msetCfg := js.targetStreamConfig(mset, mset.config())
							resp.StreamInfo = &StreamInfo{
								Created:   mset.createdTime(),
								State:     mset.state(),
								Config:    *setDynamicStreamMetadata(&msetCfg),
								Cluster:   js.clusterInfo(mset.raftGroup()),
								Sources:   mset.sourcesInfo(),
								Mirror:    mset.mirrorInfo(),
								TimeStamp: time.Now().UTC(),
							}
							s.sendAPIResponse(client, acc, subject, reply, _EMPTY_, s.jsonResponse(&resp))
						}
						return
					} else if sa.Group.Name != osa.Group.Name {
						// We had a bug where we could have multiple assignments for the same
						// stream but with different group assignments, including multiple raft
						// groups. So check for that here. We can only bet on the last one being
						// consistent in the long run, so let it continue if we see this condition.
						// Only a differing group name means a duplicate group; a differing peer
						// set or desired state is a normal peer change and must fall through.
						s.Warnf("JetStream cluster detected duplicate assignment for stream %q for account %q", sa.Config.Name, acc.Name)
						if osa.Group.node != nil && osa.Group.node != sa.Group.node {
							osa.Group.node.Delete()
							osa.Group.node = nil
						}
					}
				}
			}
			mset.setStreamAssignment(sa)
			// Check if our config has really been updated.
			cfg := mset.config()
			if !reflect.DeepEqual(&cfg, newCfg) {
				if err = mset.updateWithAdvisory(newCfg, false, false); err != nil {
					if js.isShuttingDown() {
						s.Debugf("Could not update stream, JetStream shutting down")
						return
					}
					s.Warnf("JetStream cluster error updating stream %q for account %q: %v", sa.Config.Name, acc.Name, err)
					js.mu.Lock()
					sa.err = err
					// Detach the node from the assignment; we stop (not delete) it below,
					// and it unregisters itself once its goroutines exit.
					if sa.Group != nil {
						sa.Group.node = nil
					}
					js.mu.Unlock()
					// Stop the monitor and raft node without deleting any state.
					mset.stopMonitoring()
					mset.stop(false, false)
					return
				}
			}
		} else if err == NewJSStreamNotFoundError() {
			// Add in the stream here.
			mset, err = acc.addStreamWithAssignment(newCfg, nil, sa, false, true)
		}
		if mset != nil {
			mset.setCreatedTime(created)
		}
	}

	// This is an error condition.
	if err != nil {
		// If we're shutting down we could get a variety of errors, for example:
		// 'JetStream not enabled for account' when looking up the stream.
		// Normally we can continue and delete state, but need to be careful when shutting down.
		if js.isShuttingDown() {
			s.Debugf("Could not create stream, JetStream shutting down")
			return
		}

		s.Warnf("Stream create failed for '%s > %s': %v", sa.Client.serviceAccount(), sa.Config.Name, err)
		if IsNatsErr(err, JSStreamStoreFailedF) {
			err = errStreamStoreFailed
		}
		js.mu.Lock()

		sa.err = err
		hasResponded := sa.hasResponded()

		// If out of space do nothing for now.
		if isOutOfSpaceErr(err) {
			hasResponded = true
		}

		if rg.node != nil {
			rg.node.Delete()
		}

		var result *streamAssignmentResult
		if !hasResponded {
			result = &streamAssignmentResult{
				Account:  sa.Client.serviceAccount(),
				Stream:   sa.Config.Name,
				Response: &JSApiStreamCreateResponse{ApiResponse: ApiResponse{Type: JSApiStreamCreateResponseType}},
			}
			result.Response.Error = NewJSStreamCreateError(err, Unless(err))
		}
		js.mu.Unlock()

		// Send response to the metadata leader. They will forward to the user as needed.
		if result != nil {
			s.sendInternalMsgLocked(streamAssignmentSubj, _EMPTY_, nil, result)
		}
		return
	} else if hadErr {
		js.mu.Lock()
		sa.err = nil
		js.mu.Unlock()
	}

	// Re-capture node.
	js.mu.RLock()
	node := rg.node
	js.mu.RUnlock()

	// Start our monitoring routine.
	if node != nil {
		if !alreadyRunning {
			if mset != nil {
				mset.startMonitorWg()
			}
			started := s.startGoRoutine(
				func() { js.monitorStream(mset, sa, false) },
				pprofLabels{
					"type":    "stream",
					"account": mset.accName(),
					"stream":  mset.name(),
				},
			)
			if !started && mset != nil {
				mset.monitorWg.Done()
			}
		}
	} else {
		// Single replica stream, process manually here.
		// If we are restoring, process that first.
		if sa.Restore != nil {
			// We are restoring a stream here.
			restoreDoneCh := s.processStreamRestore(sa.Client, acc, sa.Config, _EMPTY_, sa.Reply, _EMPTY_)
			s.startGoRoutine(func() {
				defer s.grWG.Done()
				select {
				case err := <-restoreDoneCh:
					if err == nil {
						mset, err = acc.lookupStream(sa.Config.Name)
						if mset != nil {
							mset.setStreamAssignment(sa)
							mset.setCreatedTime(created)
						}
					}
					if err != nil {
						if mset != nil {
							mset.delete()
						}
						js.mu.Lock()
						s.Warnf("Stream restore failed for '%s > %s': %v", sa.Client.serviceAccount(), sa.Config.Name, err)
						sa.err = err
						result := &streamAssignmentResult{
							Account: sa.Client.serviceAccount(),
							Stream:  sa.Config.Name,
							Restore: &JSApiStreamRestoreResponse{ApiResponse: ApiResponse{Type: JSApiStreamRestoreResponseType}},
						}
						result.Restore.Error = NewJSStreamRestoreError(err, Unless(err))
						js.mu.Unlock()
						// Send response to the metadata leader. They will forward to the user as needed.
						b, _ := json.Marshal(result) // Avoids auto-processing and doing fancy json with newlines.
						s.sendInternalMsgLocked(streamAssignmentSubj, _EMPTY_, nil, b)
						return
					}
					js.processStreamLeaderChange(mset, true, 0)

					// Check to see if we have restored consumers here.
					// These are not currently assigned so we will need to do so here.
					if consumers := mset.getPublicConsumers(); len(consumers) > 0 {
						// Need to read meta under the js lock, it's set to nil on shutdown.
						js.mu.RLock()
						cc := js.cluster
						meta := cc.meta
						js.mu.RUnlock()
						if meta == nil {
							// We are shutting down.
							return
						}

						for _, o := range consumers {
							name, cfg := o.String(), o.config()
							rg, err := cc.createGroupForConsumer(&cfg, sa)
							if err != nil {
								s.Warnf("Could not create group for consumer '%s > %s > %s': %v",
									sa.Client.serviceAccount(), sa.Config.Name, name, err)
								continue
							}

							// Place our initial state here as well for assignment distribution.
							ca := &consumerAssignment{
								Group:   rg,
								Stream:  sa.Config.Name,
								Name:    name,
								Config:  &cfg,
								Client:  sa.Client,
								Created: o.createdTime(),
							}

							addEntry := encodeAddConsumerAssignment(ca)
							meta.ForwardProposal(addEntry)

							// Check to make sure we see the assignment.
							go func() {
								ticker := time.NewTicker(time.Second)
								defer ticker.Stop()
								for range ticker.C {
									js.mu.RLock()
									ca, meta := js.consumerAssignment(ca.Client.serviceAccount(), sa.Config.Name, name), cc.meta
									js.mu.RUnlock()
									if ca == nil {
										s.Warnf("Consumer assignment has not been assigned, retrying")
										if meta != nil {
											meta.ForwardProposal(addEntry)
										} else {
											return
										}
									} else {
										return
									}
								}
							}()
						}
					}
				case <-s.quitCh:
					return
				}
			})
		} else {
			js.processStreamLeaderChange(mset, true, 0)
		}
	}
}

// processStreamRemoval is called when followers have replicated an assignment.
func (js *jetStream) processStreamRemoval(sa *streamAssignment) {
	js.mu.Lock()
	s, cc := js.srv, js.cluster
	if s == nil || cc == nil || cc.meta == nil {
		// TODO(dlc) - debug at least
		js.mu.Unlock()
		return
	}
	accName, stream, created := sa.Client.serviceAccount(), sa.Config.Name, sa.Created
	var isMember bool
	if sa.Group != nil {
		isMember = sa.Group.isMember(cc.meta.ID())
	}
	wasLeader := cc.isStreamLeader(accName, stream)

	// Check if we already have this assigned.
	accStreams := cc.streams[accName]
	needDelete := accStreams != nil && accStreams[stream] != nil
	if needDelete {
		osa := accStreams[stream]
		if osa.Group != nil && osa.Group.node != nil {
			// Check if we were leader of a node that was already deleted
			n := osa.Group.node
			wasLeader = wasLeader || (n.IsDeleted() && n.GroupLeader() == n.ID())
		}
		if osa.unsupported != nil {
			osa.unsupported.closeInfoSub(js.srv)
			// Remember we used to be unsupported, just so we can send a successful delete response.
			if sa.unsupported == nil {
				sa.unsupported = osa.unsupported
			}
		}
		// Carry over the running node, the decoded assignment doesn't have it.
		if sa.Group != nil && osa.Group != nil {
			sa.Group.node = osa.Group.node
		}
		delete(accStreams, stream)
		if len(accStreams) == 0 {
			delete(cc.streams, accName)
		}
	}
	cc.removeInflightStreamProposal(accName, sa.Config.Name)
	js.mu.Unlock()

	// During initial/startup recovery we'll not have registered the stream assignment,
	// but might have recovered the stream from disk. We'll need to make sure that we only
	// delete the stream if it wasn't created after this delete.
	if !needDelete && !created.IsZero() {
		if acc, err := s.lookupOrFetchAccount(accName, isMember); err == nil {
			if mset, err := acc.lookupStream(stream); err == nil {
				needDelete = !mset.createdTime().After(created)
			}
		}
	}

	if needDelete {
		js.processClusterDeleteStream(sa, isMember, wasLeader)
	}
}

func (js *jetStream) processClusterDeleteStream(sa *streamAssignment, isMember, wasLeader bool) {
	if sa == nil {
		return
	}
	js.mu.RLock()
	s := js.srv
	node := sa.Group.node
	hadLeader := node == nil || !node.Leaderless()
	offline := s.allPeersOffline(sa.Group) || sa.unsupported != nil
	var isMetaLeader bool
	if cc := js.cluster; cc != nil {
		isMetaLeader = cc.isLeader()
	}
	recovering := sa.recovering
	js.mu.RUnlock()

	stopped := false
	var resp = JSApiStreamDeleteResponse{ApiResponse: ApiResponse{Type: JSApiStreamDeleteResponseType}}
	var err error
	var acc *Account

	// Go ahead and delete the stream if we have it and the account here.
	if acc, _ = s.LookupAccount(sa.Client.serviceAccount()); acc != nil {
		if mset, _ := acc.lookupStream(sa.Config.Name); mset != nil {
			// shut down monitor by shutting down raft
			if n := mset.raftNode(); n != nil {
				n.Delete()
			}
			// wait for monitor to be shut down
			mset.stopMonitoring()
			err = mset.stop(true, wasLeader)
			stopped = true
		} else if isMember {
			s.Warnf("JetStream failed to lookup running stream while removing stream '%s > %s' from this server",
				sa.Client.serviceAccount(), sa.Config.Name)
		}
	} else if isMember {
		s.Warnf("JetStream failed to lookup account while removing stream '%s > %s' from this server", sa.Client.serviceAccount(), sa.Config.Name)
	}

	// Always delete the node if present.
	if node != nil {
		node.Delete()
	}

	// This is a stop gap cleanup in case
	// 1) the account or mset does not exist and/or
	// 2) node was nil (and couldn't be deleted)
	if !stopped || node == nil {
		if sacc := s.SystemAccount(); sacc != nil {
			saccName := sacc.GetName()
			os.RemoveAll(filepath.Join(js.config.StoreDir, saccName, defaultStoreDirName, sa.Group.Name))
			// cleanup dependent consumer groups
			if !stopped {
				for _, ca := range sa.consumers {
					// Make sure we cleanup any possible running nodes for the consumers.
					if isMember && ca.Group != nil && ca.Group.node != nil {
						ca.Group.node.Delete()
					}
					os.RemoveAll(filepath.Join(js.config.StoreDir, saccName, defaultStoreDirName, ca.Group.Name))
				}
			}
		}
	}
	accDir := filepath.Join(js.config.StoreDir, sa.Client.serviceAccount())
	streamDir := filepath.Join(accDir, streamsDir)
	os.RemoveAll(filepath.Join(streamDir, sa.Config.Name))

	// no op if not empty
	os.Remove(streamDir)
	os.Remove(accDir)

	// Normally we want only the leader to respond here, but if we had no leader then all members will respond to make
	// sure we get feedback to the user.
	if !isMember || (hadLeader && !wasLeader) {
		// If all the peers are offline and we are the meta leader we will also respond, so suppress returning here.
		if !(offline && isMetaLeader) {
			return
		}
	}

	// Do not respond if the account does not exist any longer
	if acc == nil || recovering {
		return
	}

	if err != nil {
		resp.Error = NewJSStreamGeneralError(err, Unless(err))
		s.sendAPIErrResponse(sa.Client, acc, sa.Subject, sa.Reply, _EMPTY_, s.jsonResponse(resp))
	} else {
		resp.Success = true
		s.sendAPIResponse(sa.Client, acc, sa.Subject, sa.Reply, _EMPTY_, s.jsonResponse(resp))
	}
}

// processConsumerAssignment is called when followers have replicated an assignment for a consumer.
func (js *jetStream) processConsumerAssignment(ca *consumerAssignment) {
	js.mu.RLock()
	s, cc := js.srv, js.cluster
	accName, stream, consumerName := ca.Client.serviceAccount(), ca.Stream, ca.Name
	noMeta := cc == nil || cc.meta == nil
	shuttingDown := js.shuttingDown
	var ourID string
	if !noMeta {
		ourID = cc.meta.ID()
	}
	var isMember bool
	if ca.Group != nil && ourID != _EMPTY_ {
		isMember = ca.Group.isMember(ourID)
	}
	js.mu.RUnlock()

	if s == nil || noMeta || shuttingDown {
		return
	}

	js.mu.Lock()
	sa := js.streamAssignment(accName, stream)
	if sa == nil {
		js.mu.Unlock()
		s.Debugf("Consumer create failed, could not locate stream '%s > %s'", accName, stream)
		return
	}

	// Track if this existed already.
	var wasExisting bool

	// Check if we have an existing consumer assignment.
	if sa.consumers == nil {
		sa.consumers = make(map[string]*consumerAssignment)
	}
	oca := sa.consumers[ca.Name]
	if oca != nil {
		wasExisting = true
		// Copy over private existing state from former CA.
		if ca.Group != nil {
			ca.Group.node = oca.Group.node
		}
		if oca.hasResponded() {
			ca.markResponded()
		}
		ca.err = oca.err

		// Unsubscribe if it was previously unsupported.
		if oca.unsupported != nil {
			oca.unsupported.closeInfoSub(s)
			// If we've seen unsupported once, it remains for the lifetime of this server process.
			if ca.unsupported == nil {
				ca.unsupported = oca.unsupported
			}
		}
	}

	// Capture the optional state. We will pass it along if we are a member to apply.
	// This is only applicable when restoring a stream with consumers.
	state := ca.State
	ca.State = nil

	// Place into our internal map under the stream assignment.
	// Ok to replace an existing one, we check on process call below.
	sa.consumers[ca.Name] = ca
	cc.removeInflightConsumerProposal(accName, stream, consumerName)

	// If unsupported, we can't register any further.
	if ca.unsupported != nil {
		ca.unsupported.setupInfoSub(s, ca)
		s.Warnf("Detected unsupported consumer '%s > %s > %s': %s", accName, stream, ca.Name, ca.unsupported.reason)

		// Mark stream as unsupported as well
		if sa.unsupported == nil {
			sa.unsupported = newUnsupportedStreamAssignment(s, sa, fmt.Errorf("unsupported consumer %q", ca.Name))
		}
		sa.unsupported.setupInfoSub(s, sa)
		js.mu.Unlock()

		// Be conservative by protecting the whole stream, even if just one consumer is unsupported.
		// This ensures it's safe, even with Interest-based retention where it would otherwise
		// continue accepting but dropping messages.
		acc, err := s.lookupOrFetchAccount(accName, isMember)
		if err != nil {
			return
		}
		mset, err := acc.lookupStream(stream)
		if err != nil || mset.closed.Load() {
			return
		}
		s.Warnf("Stopping unsupported stream '%s > %s'", accName, stream)
		mset.stop(false, false)
		return
	}
	js.mu.Unlock()

	acc, err := s.lookupOrFetchAccount(accName, isMember)
	if err != nil {
		ll := fmt.Sprintf("Account [%s] lookup for consumer create failed: %v", accName, err)
		if isMember {
			if !js.isMetaRecovering() {
				// If we can not lookup the account and we are a member, send this result back to the metacontroller leader.
				result := &consumerAssignmentResult{
					Account:  accName,
					Stream:   stream,
					Consumer: consumerName,
					Response: &JSApiConsumerCreateResponse{ApiResponse: ApiResponse{Type: JSApiConsumerCreateResponseType}},
				}
				result.Response.Error = NewJSNoAccountError()
				s.sendInternalMsgLocked(consumerAssignmentSubj, _EMPTY_, nil, result)
			}
			s.Warnf(ll)
		} else {
			s.Debugf(ll)
		}
		return
	}

	// Check if this is for us..
	if isMember {
		js.processClusterCreateConsumer(oca, ca, state, wasExisting)
	} else if mset, _ := acc.lookupStream(sa.Config.Name); mset != nil {
		if o := mset.lookupConsumer(ca.Name); o != nil {
			// We have one here even though we are not a member. This can happen on re-assignment.
			s.removeConsumer(o, ca)
		}
	}
}

// Common function to remove ourselves from this server.
// This can happen on re-assignment, move, etc
func (s *Server) removeConsumer(o *consumer, nca *consumerAssignment) {
	if o == nil {
		return
	}
	// Make sure to use the new stream assignment, not our own.
	s.Debugf("JetStream removing consumer '%s > %s > %s' from this server", nca.Client.serviceAccount(), nca.Stream, nca.Name)
	if node := o.raftNode(); node != nil {
		node.StepDown(nca.Group.Preferred)
		// shutdown monitor by shutting down raft.
		node.Delete()
	}

	var isShuttingDown bool
	// Make sure this node is no longer attached to our consumer assignment.
	if js, _ := s.getJetStreamCluster(); js != nil {
		js.mu.Lock()
		nca.Group.node = nil
		nca.err = nil
		isShuttingDown = js.shuttingDown
		js.mu.Unlock()
	}

	if !isShuttingDown {
		// wait for monitor to be shutdown.
		o.stopMonitoring()
	}
	o.deleteWithoutAdvisory()
}

func (js *jetStream) processConsumerRemoval(ca *consumerAssignment) {
	js.mu.Lock()
	s, cc := js.srv, js.cluster
	if s == nil || cc == nil || cc.meta == nil {
		// TODO(dlc) - debug at least
		js.mu.Unlock()
		return
	}

	accName, stream, name, created := ca.Client.serviceAccount(), ca.Stream, ca.Name, ca.Created
	wasLeader := cc.isConsumerLeader(accName, stream, name)

	// Delete from our state.
	var needDelete bool
	if accStreams := cc.streams[accName]; accStreams != nil {
		if sa := accStreams[ca.Stream]; sa != nil && sa.consumers != nil && sa.consumers[ca.Name] != nil {
			oca := sa.consumers[ca.Name]
			// Make sure this removal is for what we have, otherwise ignore.
			if ca.Group != nil && oca.Group != nil && (ca.Group.Name == oca.Group.Name || (!ca.Created.IsZero() && ca.Created.Equal(oca.Created))) {
				needDelete = true
				delete(sa.consumers, ca.Name)
				// Remember we used to be unsupported, just so we can send a successful delete response.
				if ca.unsupported == nil {
					ca.unsupported = oca.unsupported
				}
				// Carry over the running node, the decoded assignment doesn't have it.
				ca.Group.node = oca.Group.node
			}
		}
	}
	cc.removeInflightConsumerProposal(accName, stream, name)
	js.mu.Unlock()

	// During initial/startup recovery we'll not have registered the consumer assignment,
	// but might have recovered the consumer from disk. We'll need to make sure that we only
	// delete the consumer if it wasn't created after this delete.
	if !needDelete && !created.IsZero() {
		if acc, err := s.LookupAccount(accName); err == nil {
			if mset, err := acc.lookupStream(stream); err == nil {
				if o := mset.lookupConsumer(name); o != nil {
					needDelete = !o.createdTime().After(created)
				}
			}
		}
	}

	if needDelete {
		js.processClusterDeleteConsumer(ca, wasLeader)
	}
}

type consumerAssignmentResult struct {
	Account  string                       `json:"account"`
	Stream   string                       `json:"stream"`
	Consumer string                       `json:"consumer"`
	Response *JSApiConsumerCreateResponse `json:"response,omitempty"`
}

// processClusterCreateConsumer is when we are a member of the group and need to create the consumer.
func (js *jetStream) processClusterCreateConsumer(oca, ca *consumerAssignment, state *ConsumerState, wasExisting bool) {
	if ca == nil {
		return
	}
	js.mu.RLock()
	s := js.srv
	rg := ca.Group
	alreadyRunning := rg != nil && rg.node != nil
	accName, stream, consumer := ca.Client.serviceAccount(), ca.Stream, ca.Name
	raftRecovering := ca.recovering || ca.catchup
	js.mu.RUnlock()

	acc, err := s.LookupAccount(accName)
	if err != nil {
		s.Warnf("JetStream cluster failed to lookup account %q: %v", accName, err)
		return
	}

	// Go ahead and create or update the consumer.
	mset, err := acc.lookupStream(stream)
	if err != nil {
		if !js.isMetaRecovering() {
			js.mu.Lock()
			s.Warnf("Consumer create failed, could not locate stream '%s > %s > %s'", ca.Client.serviceAccount(), ca.Stream, ca.Name)
			ca.err = NewJSStreamNotFoundError()
			result := &consumerAssignmentResult{
				Account:  ca.Client.serviceAccount(),
				Stream:   ca.Stream,
				Consumer: ca.Name,
				Response: &JSApiConsumerCreateResponse{ApiResponse: ApiResponse{Type: JSApiConsumerCreateResponseType}},
			}
			result.Response.Error = NewJSStreamNotFoundError()
			s.sendInternalMsgLocked(consumerAssignmentSubj, _EMPTY_, nil, result)
			js.mu.Unlock()
		}
		return
	}

	// Check if we already have this consumer running.
	o := mset.lookupConsumer(consumer)

	if o != nil && oca != nil && oca.Group.Name != ca.Group.Name {
		s.Warnf("JetStream cluster detected consumer remapping for '%s > %s' from %q to %q",
			acc, ca.Name, oca.Group.Name, ca.Group.Name)
		o.clearNode()
		o.stopMonitoring()
		alreadyRunning = false
		// Make sure to clear from original.
		js.mu.Lock()
		oca.Group.node = nil
		js.mu.Unlock()
	}

	// Process the raft group and make sure it's running if needed.
	storage := mset.config().Storage
	if ca.Config.MemoryStorage {
		storage = MemoryStorage
	}
	// No-op if R1.
	js.createRaftGroup(accName, rg, raftRecovering, storage, pprofLabels{
		"type":     "consumer",
		"account":  mset.accName(),
		"stream":   ca.Stream,
		"consumer": ca.Name,
	})

	// Check if we already have this consumer running.
	var didCreate, isConfigUpdate, needsLocalResponse bool
	var oreply string
	if o == nil {
		// Add in the consumer if needed.
		if o, err = mset.addConsumerWithAssignment(ca.Config, ca.Name, ca, js.isMetaRecovering(), ActionCreateOrUpdate, false); err == nil {
			didCreate = true
		}
	} else {
		// This consumer exists.
		// Only update if config is really different.
		cfg := o.config()
		if isConfigUpdate = !reflect.DeepEqual(&cfg, ca.Config); isConfigUpdate {
			// Call into update, ignore consumer exists error here since this means an old deliver subject is bound
			// which can happen on restart etc.
			// JS lock needed as this can mutate the consumer assignments and race with updateInactivityThreshold.
			js.mu.Lock()
			err := o.updateConfig(ca.Config)
			js.mu.Unlock()
			if err != nil && err != NewJSConsumerNameExistError() {
				// This is essentially an update that has failed. Respond back to metaleader if we are not recovering.
				js.mu.RLock()
				if !js.metaRecovering {
					result := &consumerAssignmentResult{
						Account:  accName,
						Stream:   stream,
						Consumer: consumer,
						Response: &JSApiConsumerCreateResponse{ApiResponse: ApiResponse{Type: JSApiConsumerCreateResponseType}},
					}
					result.Response.Error = NewJSConsumerNameExistError()
					s.sendInternalMsgLocked(consumerAssignmentSubj, _EMPTY_, nil, result)
				}
				s.Warnf("Consumer create failed during update for '%s > %s > %s': %v", ca.Client.serviceAccount(), ca.Stream, ca.Name, err)
				js.mu.RUnlock()
				return
			}
		}

		var sendState bool
		js.mu.RLock()
		n := rg.node
		// Check if we already had a consumer assignment and its still pending.
		cca, oca := ca, o.consumerAssignment()
		if oca != nil {
			// An internal re-proposal (scale, move, peer remove, reconcile) carries
			// the reply of the request it copied, only a client update brings a new one.
			oreply = oca.Reply
			if !oca.hasResponded() {
				// We can't override info for replying here otherwise leader once elected can not respond.
				// So copy over original client and the reply from the old ca.
				cac := ca.clone()
				cac.Client = oca.Client
				cac.Reply = oca.Reply
				cca = cac
				needsLocalResponse = true
			}
			// If we look like we are scaling up, let's send our current state to the group.
			sendState = (len(ca.Group.Peers) > len(oca.Group.Peers) || ca.Group.Desired != nil) && o.IsLeader() && n != nil
			// Signal that this is an update
			if ca.Reply != _EMPTY_ && ca.Reply != oreply {
				isConfigUpdate = true
			}
		}
		js.mu.RUnlock()

		if sendState {
			if snap, err := o.store.EncodedState(); err == nil {
				n.SendSnapshot(snap)
			}
		}

		// Set CA for our consumer.
		o.setConsumerAssignment(cca)
		s.Debugf("JetStream cluster, consumer '%s > %s > %s' was already running", ca.Client.serviceAccount(), ca.Stream, ca.Name)
	}

	// If we have an initial state set apply that now.
	if state != nil && o != nil {
		o.mu.Lock()
		err = o.setStoreState(state)
		o.mu.Unlock()
	}

	if err != nil {
		// If we're shutting down we could get a variety of errors.
		// Normally we can continue and delete state, but need to be careful when shutting down.
		if js.isShuttingDown() {
			s.Debugf("Could not create consumer, JetStream shutting down")
			return
		}

		s.Warnf("Consumer create failed for '%s > %s > %s': %v", ca.Client.serviceAccount(), ca.Stream, ca.Name, err)
		if IsNatsErr(err, JSConsumerStoreFailedErrF) {
			err = errConsumerStoreFailed
		}

		js.mu.Lock()
		ca.err = err
		hasResponded := ca.hasResponded()

		// If out of space do nothing for now.
		if isOutOfSpaceErr(err) {
			hasResponded = true
		}

		if rg.node != nil {
			rg.node.Delete()
			// Clear the node here.
			rg.node = nil
		}

		// If we did seem to create a consumer make sure to stop it.
		if o != nil {
			o.stop()
		}

		var result *consumerAssignmentResult
		if !hasResponded && !js.metaRecovering {
			result = &consumerAssignmentResult{
				Account:  ca.Client.serviceAccount(),
				Stream:   ca.Stream,
				Consumer: ca.Name,
				Response: &JSApiConsumerCreateResponse{ApiResponse: ApiResponse{Type: JSApiConsumerCreateResponseType}},
			}
			result.Response.Error = NewJSConsumerCreateError(err, Unless(err))
		} else if err == errNoInterest {
			// This is a stranded ephemeral, let's clean this one up.
			subject := fmt.Sprintf(JSApiConsumerDeleteT, ca.Stream, ca.Name)
			mset.outq.send(newJSPubMsg(subject, _EMPTY_, _EMPTY_, nil, nil, nil, 0))
		}
		js.mu.Unlock()

		if result != nil {
			// Send response to the metadata leader. They will forward to the user as needed.
			b, _ := json.Marshal(result) // Avoids auto-processing and doing fancy json with newlines.
			s.sendInternalMsgLocked(consumerAssignmentSubj, _EMPTY_, nil, b)
		}
	} else {
		js.mu.RLock()
		hadErr := ca.err != nil
		node := rg.node
		js.mu.RUnlock()
		if hadErr {
			js.mu.Lock()
			ca.err = nil
			js.mu.Unlock()
		}

		if didCreate {
			o.setCreatedTime(ca.Created)
		} else {
			// Check for scale down to 1..
			if node != nil && len(rg.Peers) == 1 && rg.Desired == nil {
				o.clearNode()
				o.stopMonitoring()
				// Need to clear from rg too.
				js.mu.Lock()
				rg.node = nil
				client, subject, reply := ca.Client, ca.Subject, ca.Reply
				js.mu.Unlock()
				// Perform the leader change in a goroutine, otherwise we could block meta operations.
				if o.shouldStartMonitor() {
					started := s.startGoRoutine(
						func() {
							defer s.grWG.Done()
							defer o.clearMonitorRunning()
							err = o.setLeader(true, 0)
							var resp = JSApiConsumerCreateResponse{ApiResponse: ApiResponse{Type: JSApiConsumerCreateResponseType}}
							if err != nil {
								resp.Error = NewJSConsumerCreateError(err, Unless(err))
								s.sendAPIErrResponse(client, acc, subject, reply, _EMPTY_, s.jsonResponse(&resp))
							} else if resp.ConsumerInfo = setDynamicConsumerInfoMetadata(o.info()); resp.ConsumerInfo == nil {
								// The consumer was closed before we could respond.
								resp.Error = NewJSConsumerCreateError(errConsumerClosed)
								s.sendAPIErrResponse(client, acc, subject, reply, _EMPTY_, s.jsonResponse(&resp))
							} else {
								s.sendAPIResponse(client, acc, subject, reply, _EMPTY_, s.jsonResponse(&resp))
							}
						},
						pprofLabels{
							"type":     "consumer",
							"account":  mset.accName(),
							"stream":   mset.name(),
							"consumer": ca.Name,
						},
					)
					if !started {
						o.clearMonitorRunning()
					}
				}
				return
			}
		}

		if node == nil {
			// Wait for the previous routine to stop running.
			o.stopMonitoring()
			// Single replica consumer, process manually here.
			// Force response in case we think this is an update.
			if !js.isMetaRecovering() && isConfigUpdate {
				ca.clearResponded()
				o.setConsumerAssignment(ca)
			}
			cca := o.consumerAssignment()
			// Perform the leader change in a goroutine, otherwise we could block meta operations.
			if o.shouldStartMonitor() {
				started := s.startGoRoutine(
					func() {
						defer s.grWG.Done()
						defer o.clearMonitorRunning()
						js.processConsumerLeaderChangeWithAssignment(o, cca, true, 0)
					},
					pprofLabels{
						"type":     "consumer",
						"account":  mset.accName(),
						"stream":   mset.name(),
						"consumer": ca.Name,
					},
				)
				if !started {
					o.clearMonitorRunning()
				}
			}
		} else {
			// Clustered consumer.
			// Start our monitoring routine if needed.
			if !alreadyRunning {
				// Wait for the previous routine to stop running.
				o.stopMonitoring()
				if o.shouldStartMonitor() {
					started := s.startGoRoutine(
						func() { js.monitorConsumer(o, ca) },
						pprofLabels{
							"type":     "consumer",
							"account":  mset.accName(),
							"stream":   mset.name(),
							"consumer": ca.Name,
						},
					)
					if !started {
						o.clearMonitorRunning()
					}
				}
			}
			// For existing consumer, only send response if not recovering.
			if wasExisting && !js.isMetaRecovering() {
				if o.IsLeader() || (!didCreate && needsLocalResponse) {
					// Process if existing as an update. Double check that this is not recovered.
					js.mu.RLock()
					client, subject, reply, recovering, sourcing := ca.Client, ca.Subject, ca.Reply, ca.recovering, ca.Config.Sourcing
					js.mu.RUnlock()
					// Only a client update needs a response, an internal re-proposal has
					// nothing to answer and must not reset a sourcing consumer.
					if !recovering && reply != _EMPTY_ && reply != oreply {
						// If it's a sourcing consumer, we need to respond after the consumer has been reset instead.
						if sourcing {
							var resp = JSApiConsumerResetResponse{ApiResponse: ApiResponse{Type: JSApiConsumerResetResponseType}}
							resetSeq, canRespond, err := o.resetStartingSeq(0, reply, true)
							if err != nil {
								resp.Error = NewJSConsumerInvalidResetError(err)
								s.sendAPIErrResponse(client, acc, subject, reply, _EMPTY_, s.jsonResponse(&resp))
							} else if canRespond {
								if resp.ConsumerInfo = setDynamicConsumerInfoMetadata(o.info()); resp.ConsumerInfo == nil {
									// The consumer was closed before we could respond.
									resp.Error = NewJSConsumerInvalidResetError(errConsumerClosed)
									s.sendAPIErrResponse(client, acc, subject, reply, _EMPTY_, s.jsonResponse(&resp))
								} else {
									resp.ResetSeq = resetSeq
									s.sendAPIResponse(client, acc, subject, reply, _EMPTY_, s.jsonResponse(&resp))
								}
							}
						} else {
							var resp = JSApiConsumerCreateResponse{ApiResponse: ApiResponse{Type: JSApiConsumerCreateResponseType}}
							if resp.ConsumerInfo = setDynamicConsumerInfoMetadata(o.info()); resp.ConsumerInfo == nil {
								// The consumer was closed before we could respond.
								resp.Error = NewJSConsumerCreateError(errConsumerClosed)
								s.sendAPIErrResponse(client, acc, subject, reply, _EMPTY_, s.jsonResponse(&resp))
							} else {
								s.sendAPIResponse(client, acc, subject, reply, _EMPTY_, s.jsonResponse(&resp))
							}
						}
					}
				}
			}
		}
	}
}

func (js *jetStream) processClusterDeleteConsumer(ca *consumerAssignment, wasLeader bool) {
	if ca == nil {
		return
	}
	js.mu.RLock()
	s := js.srv
	node := ca.Group.node
	offline := s.allPeersOffline(ca.Group) || ca.unsupported != nil
	var isMetaLeader bool
	if cc := js.cluster; cc != nil {
		isMetaLeader = cc.isLeader()
	}
	recovering := ca.recovering
	js.mu.RUnlock()

	stopped := false
	var resp = JSApiConsumerDeleteResponse{ApiResponse: ApiResponse{Type: JSApiConsumerDeleteResponseType}}
	var err error
	var acc *Account

	// Go ahead and delete the consumer if we have it and the account.
	if acc, _ = s.LookupAccount(ca.Client.serviceAccount()); acc != nil {
		if mset, _ := acc.lookupStream(ca.Stream); mset != nil {
			if o := mset.lookupConsumer(ca.Name); o != nil {
				// An R1 consumer processes its leader change in a goroutine, which responds to its create.
				// Wait for it, otherwise the consumer is already stopped and the create gets no response.
				if node == nil {
					o.stopMonitoring()
				}
				err = o.stopWithFlags(true, false, true, wasLeader)
				stopped = true
			}
		}
	}

	// Always delete the node if present.
	if node != nil {
		node.Delete()
	}

	// This is a stop gap cleanup in case
	// 1) the account, mset, or consumer does not exist and/or
	// 2) node was nil (and couldn't be deleted)
	if !stopped || node == nil {
		if sacc := s.SystemAccount(); sacc != nil {
			os.RemoveAll(filepath.Join(js.config.StoreDir, sacc.GetName(), defaultStoreDirName, ca.Group.Name))
		}
	}

	accDir := filepath.Join(js.config.StoreDir, ca.Client.serviceAccount())
	consumersDir := filepath.Join(accDir, streamsDir, ca.Stream, consumerDir)
	os.RemoveAll(filepath.Join(consumersDir, ca.Name))

	if !wasLeader || ca.Reply == _EMPTY_ {
		if !(offline && isMetaLeader) {
			return
		}
	}

	// Do not respond if the account does not exist any longer or this is during recovery.
	if acc == nil || recovering {
		return
	}

	if err != nil {
		resp.Error = NewJSConsumerNotFoundError(Unless(err))
		s.sendAPIErrResponse(ca.Client, acc, ca.Subject, ca.Reply, _EMPTY_, s.jsonResponse(resp))
	} else {
		resp.Success = true
		s.sendAPIResponse(ca.Client, acc, ca.Subject, ca.Reply, _EMPTY_, s.jsonResponse(resp))
	}
}

// Returns the consumer assignment, or nil if not present.
// Lock should be held.
func (js *jetStream) consumerAssignment(account, stream, consumer string) *consumerAssignment {
	if sa := js.streamAssignment(account, stream); sa != nil {
		return sa.consumers[consumer]
	}
	return nil
}

// Will look up a consumer assignment, either an applied or inflight assignment.
// Lock should be held.
func (js *jetStream) consumerAssignmentOrInflight(account, stream, consumer string) *consumerAssignment {
	cc := js.cluster
	if cc == nil {
		return nil
	}
	if streams, ok := cc.inflightConsumers[account]; ok {
		if consumers, ok := streams[stream]; ok {
			if inflight, ok := consumers[consumer]; ok {
				if !inflight.deleted {
					return inflight.consumerAssignment
				} else {
					return nil
				}
			}
		}
	}
	if sa := js.streamAssignment(account, stream); sa != nil {
		return sa.consumers[consumer]
	}
	return nil
}

// Will gather all consumer assignments for the specified account and stream, both applied and inflight assignments.
// Lock should be held.
func (js *jetStream) consumerAssignmentsOrInflightSeq(account, stream string) iter.Seq[*consumerAssignment] {
	return func(yield func(*consumerAssignment) bool) {
		cc := js.cluster
		if cc == nil {
			return
		}

		var inflight map[string]*inflightConsumerInfo
		if streams, ok := cc.inflightConsumers[account]; ok {
			inflight = streams[stream]
		}
		for _, i := range inflight {
			if !i.deleted && !yield(i.consumerAssignment) {
				return
			}
		}
		sa := js.streamAssignment(account, stream)
		if sa == nil {
			return
		}
		for _, ca := range sa.consumers {
			// Skip if we already iterated over it as inflight.
			if _, ok := inflight[ca.Name]; ok {
				continue
			}
			if !yield(ca) {
				return
			}
		}
	}
}

// consumerAssigned informs us if this server has this consumer assigned.
func (jsa *jsAccount) consumerAssigned(stream, consumer string) bool {
	jsa.mu.RLock()
	js, acc := jsa.js, jsa.account
	jsa.mu.RUnlock()

	if js == nil {
		return false
	}
	js.mu.RLock()
	defer js.mu.RUnlock()
	return js.cluster.isConsumerAssigned(acc, stream, consumer)
}

// Read lock should be held.
func (cc *jetStreamCluster) isConsumerAssigned(a *Account, stream, consumer string) bool {
	// Non-clustered mode always return true.
	if cc == nil {
		return true
	}
	if cc.meta == nil {
		return false
	}
	var sa *streamAssignment
	accStreams := cc.streams[a.Name]
	if accStreams != nil {
		sa = accStreams[stream]
	}
	if sa == nil {
		// TODO(dlc) - This should not happen.
		return false
	}
	ca := sa.consumers[consumer]
	if ca == nil {
		return false
	}
	return ca.Group.isMember(cc.meta.ID())
}

// Returns our stream and underlying raft node.
func (o *consumer) streamAndNode() (*stream, RaftNode) {
	if o == nil {
		return nil, nil
	}
	o.mu.RLock()
	defer o.mu.RUnlock()
	return o.mset, o.node
}

// Return the replica count for this consumer. If the consumer has been
// stopped, this will return an error.
func (o *consumer) replica() (int, error) {
	o.mu.RLock()
	oCfg := o.cfg
	mset := o.mset
	o.mu.RUnlock()
	if mset == nil {
		return 0, errBadConsumer
	}
	sCfg := mset.config()
	return oCfg.replicas(&sCfg), nil
}

func (o *consumer) raftGroup() *raftGroup {
	if o == nil {
		return nil
	}
	o.mu.RLock()
	defer o.mu.RUnlock()
	if o.ca == nil {
		return nil
	}
	return o.ca.Group
}

func (o *consumer) raftNode() RaftNode {
	if o == nil {
		return nil
	}
	o.mu.RLock()
	defer o.mu.RUnlock()
	return o.node
}

func (js *jetStream) monitorConsumer(o *consumer, ca *consumerAssignment) {
	s, n, meta := js.server(), o.raftNode(), js.getMetaGroup()
	defer s.grWG.Done()

	defer o.clearMonitorRunning()

	if n == nil || meta == nil {
		s.Warnf("No RAFT group for '%s > %s > %s'", o.acc.Name, ca.Stream, ca.Name)
		return
	}

	// Make sure to stop the raft group on exit to prevent accidental memory bloat.
	// This should be below the checkInMonitor call though to avoid stopping it out
	// from underneath the one that is running since it will be the same raft node.
	defer n.Stop()

	qch, mqch, lch, aq, uch := n.QuitC(), o.monitorQuitC(), n.LeadChangeC(), n.ApplyQ(), o.updateC()

	s.Debugf("Starting consumer monitor for '%s > %s > %s' [%s]", o.acc.Name, ca.Stream, ca.Name, n.Group())
	defer s.Debugf("Exiting consumer monitor for '%s > %s > %s' [%s]", o.acc.Name, ca.Stream, ca.Name, n.Group())

	const (
		compactInterval    = 2 * time.Minute
		compactMinInterval = 15 * time.Second
		compactSizeMin     = 64 * 1024 // What is stored here is always small for consumers.
		compactNumMin      = 1024
		minSnapDelta       = 10 * time.Second
	)

	// Spread these out for large numbers on server restart.
	rci := time.Duration(rand.Int64N(int64(time.Minute)))
	t := time.NewTicker(compactInterval + rci)
	defer t.Stop()

	// Highwayhash key for generating hashes.
	key := make([]byte, 32)
	crand.Read(key)

	var lastSnapTime time.Time

	// Don't allow the upper layer to install snapshots until we have
	// fully recovered from disk.
	recovering := true

	var failedSnapshots int
	doSnapshot := func(force bool) {
		// Bail if trying too fast and not in a forced situation.
		// If snapshots have failed, and we're not forced to, we'll wait for the timer since it'll now be forced.
		if recovering || (!force && (time.Since(lastSnapTime) < minSnapDelta || failedSnapshots > 0)) {
			return
		}

		if snap, err := o.store.EncodedState(); err == nil {
			// If we had a significant number of failed snapshots, start relaxing Raft-layer checks
			// to force it through. We might have been catching up a peer for a long period, and this
			// protects our log size from growing indefinitely.
			forceSnapshot := failedSnapshots > 4
			if err := n.InstallSnapshot(snap, forceSnapshot); err == nil {
				lastSnapTime = time.Now()
				// If there was a failed snapshot before, we reduced the timer's interval.
				// Reset it back to the original interval now.
				if failedSnapshots > 0 {
					t.Reset(compactInterval + rci)
				}
				failedSnapshots = 0
			} else if err != errNoSnapAvailable && err != errNodeClosed && err != errCatchupsRunning {
				s.RateLimitWarnf("Failed to install snapshot for '%s > %s > %s' [%s]: %v", o.acc.Name, ca.Stream, ca.Name, n.Group(), err)
				// If this is the first failure, reduce the interval of the snapshot timer.
				// This ensures we're not waiting too long for snapshotting to eventually become forced.
				if failedSnapshots == 0 {
					t.Reset(compactMinInterval)
				}
				failedSnapshots++
			}
		} else {
			s.RateLimitWarnf("Failed to install snapshot for '%s > %s > %s' [%s]: %v", o.acc.Name, ca.Stream, ca.Name, n.Group(), err)
		}
	}

	// For migration tracking.
	mmtd := 500 * time.Millisecond
	var mmt *time.Ticker
	var mmtc <-chan time.Time

	startMigrationMonitoring := func() {
		if mmt == nil {
			mmt = time.NewTicker(mmtd)
			mmtc = mmt.C
		}
	}

	stopMigrationMonitoring := func() {
		if mmt != nil {
			mmt.Stop()
			mmt, mmtc = nil, nil
		}
	}
	defer stopMigrationMonitoring()

	// Track if we are leader.
	var isLeader bool

	for {
		select {
		case <-s.quitCh:
			// Server shutting down, but we might receive this before qch, so try to snapshot.
			doSnapshot(false)
			return
		case <-mqch:
			// Clean signal from shutdown routine so do best effort attempt to snapshot.
			// Don't snapshot if not shutting down, monitor goroutine could be going away
			// on a scale down or a remove for example.
			if s.isShuttingDown() {
				doSnapshot(false)
			}
			return
		case <-qch:
			// Raft node is closed, no use in trying to snapshot.
			return
		case <-aq.ch:
			ces := aq.pop()
			for _, ce := range ces {
				// No special processing needed for when we are caught up on restart.
				if ce == nil {
					if !recovering {
						continue
					}
					recovering = false
					if n.NeedSnapshot() {
						doSnapshot(true)
					}
					continue
				}
				if err := js.applyConsumerEntries(o, ce, isLeader); err == nil {
					var ne, nb uint64
					// We can't guarantee writes are flushed while we're shutting down. Just rely on replay during recovery.
					if !js.isShuttingDown() {
						ne, nb = n.Applied(ce.Index)
					}
					// If we have at least min entries to compact, go ahead and snapshot/compact.
					if nb > 0 && ne >= compactNumMin || nb > compactSizeMin {
						doSnapshot(false)
					}
				} else if err != errConsumerClosed {
					s.Warnf("Error applying consumer entries to '%s > %s'", ca.Client.serviceAccount(), ca.Name)
				}
				ce.ReturnToPool()
			}
			aq.recycle(&ces)

		case lc := <-lch:
			isLeader = lc.isLeader
			if recovering && !isLeader {
				js.setConsumerAssignmentRecovering(ca)
			}

			// Process the change.
			if err := js.processConsumerLeaderChange(o, isLeader, lc.term); err == nil {
				// Check our state if we are under an interest based stream.
				if mset := o.getStream(); mset != nil {
					var ss StreamState
					mset.store.FastState(&ss)
					o.checkStateForInterestStream(&ss)
				}
			}

			// We may receive a leader change after the consumer assignment which would cancel us
			// monitoring for this closely. So re-assess our state here as well.
			// Or the old leader is no longer part of the set and transferred leadership
			// for this leader to resume with removal
			migrating := o.isMigrating()
			if isLeader && migrating {
				startMigrationMonitoring()
			} else {
				stopMigrationMonitoring()
			}
		case <-uch:
			// keep consumer assignment current
			ca = o.consumerAssignment()
			// We get this when we have a new consumer assignment caused by an update.
			// We want to know if we are migrating.
			migrating := o.isMigrating()
			if isLeader && migrating {
				startMigrationMonitoring()
			} else {
				stopMigrationMonitoring()
			}
		case <-mmtc:
			if !isLeader {
				// We are no longer leader, so not our job.
				stopMigrationMonitoring()
				continue
			}
			if done := js.runConsumerMigration(o, ca, n); done {
				stopMigrationMonitoring()
				continue
			}

		case <-t.C:
			// Start forcing snapshots if they failed previously.
			forceIfFailed := failedSnapshots > 0
			doSnapshot(forceIfFailed)
		}
	}
}

// Migrate a consumer from peer set A to peer set B. Returns true when migration is done.
func (js *jetStream) runConsumerMigration(o *consumer, ca *consumerAssignment, n RaftNode) bool {
	ourPeerId, cc, s := n.ID(), js.cluster, js.srv

	js.mu.RLock()
	// We are shutting down.
	if cc == nil || cc.meta == nil {
		js.mu.RUnlock()
		return true
	}
	meta := cc.meta
	accName, streamName, consumerName := ca.Client.serviceAccount(), ca.Stream, ca.Name
	js.mu.RUnlock()

	rg := o.raftGroup()
	ci := js.clusterInfo(rg)
	replicas, err := o.replica()
	if err != nil {
		return true
	}
	if len(rg.Peers) <= replicas {
		// Migration no longer happening, so not our job anymore
		return true
	}
	newPeers, _, newPeerSet, _ := genPeerInfo(rg.Peers, len(rg.Peers)-replicas)

	// If we are part of the new peerset and we have been passed the baton.
	// We will handle scale down.
	if newPeerSet[ourPeerId] {
		n.ProposeKnownPeers(newPeers)
		cca := ca.copyGroup()
		cca.Group.Peers = newPeers
		cca.Group.Cluster = s.cachedClusterName()
		meta.ForwardProposal(encodeAddConsumerAssignment(cca))
		s.Noticef("Scaling down '%s > %s > %s' to %+v", accName, streamName, consumerName, s.peerSetToNames(newPeers))

	} else {
		var newLeaderPeer, newLeader, newCluster string
		neededCurrent, current := replicas/2+1, 0
		for _, r := range ci.Replicas {
			if r.Current && newPeerSet[r.Peer] {
				current++
				if newCluster == _EMPTY_ {
					newLeaderPeer, newLeader, newCluster = r.Peer, r.Name, r.cluster
				}
			}
		}

		// Check if we have a quorom
		if current >= neededCurrent {
			s.Noticef("Transfer of consumer leader for '%s > %s > %s' to '%s'", accName, streamName, consumerName, newLeader)
			n.StepDown(newLeaderPeer)
		}
	}
	return false
}

// Determine if we are migrating
func (o *consumer) isMigrating() bool {
	if o == nil {
		return false
	}

	replicas, err := o.replica()
	if err != nil {
		return false
	}
	o.mu.RLock()
	js, ca := o.js, o.ca
	o.mu.RUnlock()

	js.mu.RLock()
	defer js.mu.RUnlock()

	// During migration we will always be R>1, even when we start R1.
	// So if we do not have a group or node, we know we are not migrating.
	if ca == nil || ca.Group == nil || ca.Group.node == nil {
		return false
	}
	// Desired state is driven by a v2.15 meta leader, which reconciles the group itself.
	// We can't complete that migration, and the peer count deliberately lags the replica
	// count while it's pending, so the legacy signature below would fire and never converge.
	if ca.Group.Desired != nil {
		return false
	}
	// The sign of migration is if our group peer count != configured replica count.
	if replicas != len(ca.Group.Peers) {
		return true
	}
	return false
}

func (js *jetStream) applyConsumerEntries(o *consumer, ce *CommittedEntry, isLeader bool) error {
	for _, e := range ce.Entries {
		// Ignore if lower-level catchup is started.
		// We don't need to optimize during this, all entries are handled as normal.
		if e.Type == EntryCatchup {
			continue
		}

		if e.Type == EntrySnapshot {
			if !isLeader {
				// No-op needed?
				state, err := decodeConsumerState(e.Data)
				if err != nil {
					if mset, node := o.streamAndNode(); mset != nil && node != nil {
						s := js.srv
						s.Errorf("JetStream cluster could not decode consumer snapshot for '%s > %s > %s' [%s]",
							mset.account(), mset.name(), o, node.Group())
					}
					panic(err.Error())
				}

				if err = o.store.Update(state); err != nil && err != ErrStoreOldUpdate {
					o.mu.RLock()
					s, acc, mset, name := o.srv, o.acc, o.mset, o.name
					o.mu.RUnlock()
					if s != nil && mset != nil {
						s.Warnf("Consumer '%s > %s > %s' error on store update from snapshot entry: %v", acc, mset.name(), name, err)
					}
				}
				// Check our interest state if applicable.
				if mset := o.getStream(); mset != nil {
					var ss StreamState
					mset.store.FastState(&ss)
					// We used to register preacks here if our ack floor was higher than the last sequence.
					// Now when streams catch up they properly call checkInterestState() and periodically run this as well.
					// If our states drift this could have allocated lots of pre-acks.
					o.checkStateForInterestStream(&ss)
				}
			}

		} else if e.Type == EntryRemovePeer {
			js.mu.RLock()
			var ourID string
			if js.cluster != nil && js.cluster.meta != nil {
				ourID = js.cluster.meta.ID()
			}
			js.mu.RUnlock()
			if peer := string(e.Data); peer == ourID {
				shouldRemove := true
				if mset := o.getStream(); mset != nil {
					if sa := mset.streamAssignment(); sa != nil && sa.Group != nil {
						js.mu.RLock()
						shouldRemove = !sa.Group.isMember(ourID)
						js.mu.RUnlock()
					}
				}
				if shouldRemove {
					o.stopWithFlags(true, false, false, false)
				}
			}
		} else if e.Type == EntryAddPeer {
			// Ignore for now.
		} else {
			buf := e.Data
			if len(buf) == 0 {
				return errBadEntryOp
			}
			switch entryOp(buf[0]) {
			case updateDeliveredOp:
				dseq, sseq, dc, ts, err := decodeDeliveredUpdate(buf[1:])
				if err != nil {
					if mset, node := o.streamAndNode(); mset != nil && node != nil {
						s := js.srv
						s.Errorf("JetStream cluster could not decode consumer delivered update for '%s > %s > %s' [%s]",
							mset.account(), mset.name(), o, node.Group())
					}
					panic(err.Error())
				}
				// Make sure to update delivered under the lock.
				o.mu.Lock()
				err = o.store.UpdateDelivered(dseq, sseq, dc, ts)
				o.ldt = time.Now()
				// Need to send message to the client, since we have quorum to do so now.
				if pmsg, ok := o.pendingDeliveries[sseq]; ok {
					// Copy delivery subject and sequence first, as the send returns it to the pool and clears it.
					dsubj, seq := pmsg.dsubj, pmsg.seq
					o.outq.send(pmsg)
					delete(o.pendingDeliveries, sseq)

					// Might need to send a request timeout after sending the last replicated delivery.
					if wd, ok := o.waitingDeliveries[dsubj]; ok && wd.seq == seq {
						if wd.pn > 0 || wd.pb > 0 {
							hdr := fmt.Appendf(nil, "NATS/1.0 408 Request Timeout\r\n%s: %d\r\n%s: %d\r\n\r\n", JSPullRequestPendingMsgs, wd.pn, JSPullRequestPendingBytes, wd.pb)
							o.outq.send(newJSPubMsg(dsubj, _EMPTY_, _EMPTY_, hdr, nil, nil, 0))
						}
						wd.recycle()
						delete(o.waitingDeliveries, dsubj)
					}
				}
				o.mu.Unlock()
				if err != nil {
					panic(err.Error())
				}
			case updateAcksOp:
				dseq, sseq, err := decodeAckUpdate(buf[1:])
				if err != nil {
					if mset, node := o.streamAndNode(); mset != nil && node != nil {
						s := js.srv
						s.Errorf("JetStream cluster could not decode consumer ack update for '%s > %s > %s' [%s]",
							mset.account(), mset.name(), o, node.Group())
					}
					panic(err.Error())
				}
				if err := o.processReplicatedAck(dseq, sseq); err == errConsumerClosed {
					return err
				}
			case updateSkipOp:
				sseq, err := decodeSkipUpdate(buf[1:])
				if err != nil {
					return err
				}
				o.mu.Lock()
				if !o.isLeader() && sseq > o.sseq {
					o.sseq = sseq
				}
				if o.dseq == 0 {
					o.dseq = 1
				}
				if o.store != nil {
					o.store.UpdateStarting(sseq - 1)
				}
				o.mu.Unlock()
			case resetSeqOp:
				sseq, reply, err := decodeResetUpdate(buf[1:])
				if err != nil {
					return err
				}
				o.mu.Lock()
				o.resetLocalStartingSeq(sseq)
				if o.store != nil {
					o.store.Reset(sseq - 1)
				}
				// Cleanup messages that lost interest.
				if o.retention == InterestPolicy {
					if mset := o.mset; mset != nil {
						o.mu.Unlock()
						ss := mset.state()
						o.checkStateForInterestStream(&ss)
						o.mu.Lock()
					}
				}
				// Recalculate pending, and re-trigger message delivery.
				if !o.isLeader() {
					o.mu.Unlock()
				} else {
					o.streamNumPending()
					o.signalNewMessages()
					s, a := o.srv, o.acc
					if reply == _EMPTY_ {
						o.mu.Unlock()
					} else if internal, ok := o.rsm[reply]; !ok {
						o.mu.Unlock()
					} else {
						delete(o.rsm, reply)
						o.mu.Unlock()

						// Check if the reset request needs to be answered on the system account.
						// This will happen for replicated sourcing consumers that get reset as part of a create/update.
						if internal {
							a = nil
						}
						var resp = JSApiConsumerResetResponse{ApiResponse: ApiResponse{Type: JSApiConsumerResetResponseType}}
						if resp.ConsumerInfo = setDynamicConsumerInfoMetadata(o.info()); resp.ConsumerInfo == nil {
							// The consumer was closed before we could respond.
							resp.Error = NewJSConsumerInvalidResetError(errConsumerClosed)
							s.sendInternalAccountMsg(a, reply, s.jsonResponse(&resp))
						} else {
							resp.ResetSeq = sseq
							s.sendInternalAccountMsg(a, reply, s.jsonResponse(&resp))
						}
					}
				}
			case addPendingRequest:
				o.mu.Lock()
				if !o.isLeader() {
					if o.prm == nil {
						o.prm = make(map[string]struct{})
					}
					o.prm[string(buf[1:])] = struct{}{}
				}
				o.mu.Unlock()
			case removePendingRequest:
				o.mu.Lock()
				if !o.isLeader() {
					if o.prm != nil {
						delete(o.prm, string(buf[1:]))
					}
				}
				o.mu.Unlock()
			default:
				panic(fmt.Sprintf("JetStream Cluster Unknown group entry op type: %v", entryOp(buf[0])))
			}
		}
	}
	return nil
}

var errConsumerClosed = errors.New("consumer closed")

func (o *consumer) processReplicatedAck(dseq, sseq uint64) error {
	o.mu.Lock()
	// Update activity.
	o.lat = time.Now()

	var ackAllSeqs []uint64
	if o.retention != LimitsPolicy && (o.cfg.AckPolicy == AckAll || o.cfg.AckPolicy == AckFlowControl) {
		// Always use the store state, as o.asflr is skipped ahead already.
		// Capture before updating store, which clears the pending below.
		state, err := o.store.BorrowState()
		if err == nil {
			// Only need to collect if the ack covers more than the sequence itself.
			if sagap := sseq - state.AckFloor.Stream; sagap > 1 {
				// At most the pending entries below the ack, don't over-allocate.
				ackAllSeqs = make([]uint64, 0, min(uint64(len(state.Pending)), sagap-1))
				for seq := range state.Pending {
					if seq < sseq {
						ackAllSeqs = append(ackAllSeqs, seq)
					}
				}
			}
		}
	}

	// Do actual ack update to store.
	// Always do this to have it recorded.
	o.store.UpdateAcks(dseq, sseq)

	mset := o.mset
	if o.closed || mset == nil {
		o.mu.Unlock()
		return errConsumerClosed
	}
	if mset.closed.Load() {
		o.mu.Unlock()
		return errStreamClosed
	}

	// Check if we have a reply that was requested.
	if reply := o.replies[sseq]; reply != _EMPTY_ {
		o.outq.sendMsg(reply, nil)
		delete(o.replies, sseq)
	}

	if o.retention == LimitsPolicy {
		o.mu.Unlock()
		return nil
	}
	o.mu.Unlock()

	mset.ackMsg(o, sseq)
	for _, seq := range ackAllSeqs {
		mset.ackMsg(o, seq)
	}
	return nil
}

var errBadEntryOp = errors.New("jetstream cluster bad replicated entry")
var errBadAckUpdate = errors.New("jetstream cluster bad replicated ack update")
var errBadDeliveredUpdate = errors.New("jetstream cluster bad replicated delivered update")
var errBadSkipUpdate = errors.New("jetstream cluster bad replicated skip update")
var errBadResetUpdate = errors.New("jetstream cluster bad replicated reset update")

func decodeAckUpdate(buf []byte) (dseq, sseq uint64, err error) {
	var bi, n int
	if dseq, n = binary.Uvarint(buf); n <= 0 {
		return 0, 0, errBadAckUpdate
	}
	bi += n
	if sseq, n = binary.Uvarint(buf[bi:]); n <= 0 {
		return 0, 0, errBadAckUpdate
	}
	return dseq, sseq, nil
}

func decodeDeliveredUpdate(buf []byte) (dseq, sseq, dc uint64, ts int64, err error) {
	var bi, n int
	if dseq, n = binary.Uvarint(buf); n <= 0 {
		return 0, 0, 0, 0, errBadDeliveredUpdate
	}
	bi += n
	if sseq, n = binary.Uvarint(buf[bi:]); n <= 0 {
		return 0, 0, 0, 0, errBadDeliveredUpdate
	}
	bi += n
	if dc, n = binary.Uvarint(buf[bi:]); n <= 0 {
		return 0, 0, 0, 0, errBadDeliveredUpdate
	}
	bi += n
	if ts, n = binary.Varint(buf[bi:]); n <= 0 {
		return 0, 0, 0, 0, errBadDeliveredUpdate
	}
	return dseq, sseq, dc, ts, nil
}

func decodeSkipUpdate(buf []byte) (sseq uint64, err error) {
	if len(buf) < 8 {
		return 0, errBadSkipUpdate
	}
	return binary.LittleEndian.Uint64(buf), nil
}

func decodeResetUpdate(buf []byte) (sseq uint64, reply string, err error) {
	if len(buf) < 8 {
		return 0, _EMPTY_, errBadResetUpdate
	}
	return binary.LittleEndian.Uint64(buf[:8]), string(buf[8:]), nil
}

func (js *jetStream) processConsumerLeaderChange(o *consumer, isLeader bool, term uint64) error {
	return js.processConsumerLeaderChangeWithAssignment(o, nil, isLeader, term)
}

func (js *jetStream) processConsumerLeaderChangeWithAssignment(o *consumer, ca *consumerAssignment, isLeader bool, term uint64) error {
	stepDownIfLeader := func() error {
		if node := o.raftNode(); node != nil && isLeader {
			node.StepDown()
		}
		return errors.New("failed to update consumer leader status")
	}

	if o == nil || o.isClosed() {
		return stepDownIfLeader()
	}

	if ca == nil {
		ca = o.consumerAssignment()
	}
	if ca == nil {
		return stepDownIfLeader()
	}
	js.mu.RLock()
	s, account, err := js.srv, ca.Client.serviceAccount(), ca.err
	client, subject, reply, streamName, consumerName, sourcing := ca.Client, ca.Subject, ca.Reply, ca.Stream, ca.Name, ca.Config.Sourcing
	hasResponded := ca.markResponded()
	js.mu.RUnlock()

	acc, _ := s.LookupAccount(account)
	if acc == nil {
		return stepDownIfLeader()
	}

	if isLeader {
		// Only log if the consumer is replicated and/or durable.
		// Logging about R1 ephemerals, like KV watchers, is mostly noise since the leader will always be known.
		o.mu.RLock()
		isReplicated, durable := o.node != nil, o.isDurable()
		o.mu.RUnlock()
		if isReplicated || durable {
			s.Noticef("JetStream cluster new consumer leader for '%s > %s > %s'", ca.Client.serviceAccount(), streamName, consumerName)
		}
		s.sendConsumerLeaderElectAdvisory(o)
	} else {
		// We are stepping down.
		// Make sure if we are doing so because we have lost quorum that we send the appropriate advisories.
		if node := o.raftNode(); node != nil && !node.Quorum() && time.Since(node.Created()) > 5*time.Second {
			s.sendConsumerLostQuorumAdvisory(o)
		}
	}

	// Tell consumer to switch leader status.
	if lerr := o.setLeader(isLeader, term); lerr != nil && err == nil {
		err = lerr
	}

	if !isLeader || hasResponded {
		if isLeader {
			o.clearInitialInfo()
		}
		return nil
	}

	var resp = JSApiConsumerCreateResponse{ApiResponse: ApiResponse{Type: JSApiConsumerCreateResponseType}}
	if err != nil {
		resp.Error = NewJSConsumerCreateError(err, Unless(err))
		s.sendAPIErrResponse(client, acc, subject, reply, _EMPTY_, s.jsonResponse(&resp))
	} else {
		// If it's a sourcing consumer, we need to respond after the consumer has been reset instead.
		if sourcing {
			var rresp = JSApiConsumerResetResponse{ApiResponse: ApiResponse{Type: JSApiConsumerResetResponseType}}
			resetSeq, canRespond, err := o.resetStartingSeq(0, reply, true)
			if err != nil {
				rresp.Error = NewJSConsumerInvalidResetError(err)
				s.sendAPIErrResponse(client, acc, subject, reply, _EMPTY_, s.jsonResponse(&rresp))
			} else if canRespond {
				if rresp.ConsumerInfo = setDynamicConsumerInfoMetadata(o.info()); rresp.ConsumerInfo == nil {
					// The consumer was closed before we could respond.
					rresp.Error = NewJSConsumerInvalidResetError(errConsumerClosed)
					s.sendAPIErrResponse(client, acc, subject, reply, _EMPTY_, s.jsonResponse(&rresp))
					return nil
				}
				rresp.ResetSeq = resetSeq
				s.sendAPIResponse(client, acc, subject, reply, _EMPTY_, s.jsonResponse(&rresp))
			}
		} else if resp.ConsumerInfo = setDynamicConsumerInfoMetadata(o.initialInfo()); resp.ConsumerInfo == nil {
			// The consumer was closed before we could respond.
			resp.Error = NewJSConsumerCreateError(errConsumerClosed)
			s.sendAPIErrResponse(client, acc, subject, reply, _EMPTY_, s.jsonResponse(&resp))
			return nil
		} else {
			s.sendAPIResponse(client, acc, subject, reply, _EMPTY_, s.jsonResponse(&resp))
		}
		o.sendCreateAdvisory()
	}

	// Only send a pause advisory on consumer create if we're
	// actually paused. The timer would have been kicked by now
	// by the call to o.setLeader() above.
	o.mu.RLock()
	if isLeader && o.cfg.PauseUntil != nil && !o.cfg.PauseUntil.IsZero() && time.Now().Before(*o.cfg.PauseUntil) {
		o.sendPauseAdvisoryLocked(&o.cfg)
	}
	o.mu.RUnlock()

	return nil
}

// Determines if we should send lost quorum advisory. We throttle these after first one.
func (o *consumer) shouldSendLostQuorum() bool {
	o.mu.Lock()
	defer o.mu.Unlock()
	if time.Since(o.lqsent) >= lostQuorumAdvInterval {
		o.lqsent = time.Now()
		return true
	}
	return false
}

func (s *Server) sendConsumerLostQuorumAdvisory(o *consumer) {
	if o == nil {
		return
	}
	node, stream, consumer, acc := o.raftNode(), o.streamName(), o.String(), o.account()
	if node == nil {
		return
	}
	if !o.shouldSendLostQuorum() {
		return
	}

	s.Warnf("JetStream cluster consumer '%s > %s > %s' has NO quorum, stalled.", acc.GetName(), stream, consumer)

	subj := JSAdvisoryConsumerQuorumLostPre + "." + stream + "." + consumer
	adv := &JSConsumerQuorumLostAdvisory{
		TypedEvent: TypedEvent{
			Type: JSConsumerQuorumLostAdvisoryType,
			ID:   nuid.Next(),
			Time: time.Now().UTC(),
		},
		Stream:   stream,
		Consumer: consumer,
		Replicas: s.replicas(node),
		Domain:   s.getOpts().JetStreamDomain,
	}

	// Send to the user's account if not the system account.
	if acc != s.SystemAccount() {
		s.publishAdvisory(acc, subj, adv)
	}
	// Now do system level one. Place account info in adv, and nil account means system.
	adv.Account = acc.GetName()
	s.publishAdvisory(nil, subj, adv)
}

func (s *Server) sendConsumerLeaderElectAdvisory(o *consumer) {
	if o == nil {
		return
	}
	node, stream, consumer, acc := o.raftNode(), o.streamName(), o.String(), o.account()
	if node == nil {
		return
	}

	subj := JSAdvisoryConsumerLeaderElectedPre + "." + stream + "." + consumer
	adv := &JSConsumerLeaderElectedAdvisory{
		TypedEvent: TypedEvent{
			Type: JSConsumerLeaderElectedAdvisoryType,
			ID:   nuid.Next(),
			Time: time.Now().UTC(),
		},
		Stream:   stream,
		Consumer: consumer,
		Leader:   s.serverNameForNode(node.GroupLeader()),
		Replicas: s.replicas(node),
		Domain:   s.getOpts().JetStreamDomain,
	}

	// Send to the user's account if not the system account.
	if acc != s.SystemAccount() {
		s.publishAdvisory(acc, subj, adv)
	}
	// Now do system level one. Place account info in adv, and nil account means system.
	adv.Account = acc.GetName()
	s.publishAdvisory(nil, subj, adv)
}

type streamAssignmentResult struct {
	Account  string                      `json:"account"`
	Stream   string                      `json:"stream"`
	Response *JSApiStreamCreateResponse  `json:"create_response,omitempty"`
	Restore  *JSApiStreamRestoreResponse `json:"restore_response,omitempty"`
	Update   bool                        `json:"is_update,omitempty"`
}

// Determine if this is an insufficient resources' error type.
func isInsufficientResourcesErr(resp *JSApiStreamCreateResponse) bool {
	return resp != nil && resp.Error != nil && IsNatsErr(resp.Error, JSInsufficientResourcesErr, JSMemoryResourcesExceededErr, JSStorageResourcesExceededErr)
}

// Process error results of stream and consumer assignments.
// Success will be handled by stream leader.
func (js *jetStream) processStreamAssignmentResults(sub *subscription, c *client, _ *Account, subject, reply string, msg []byte) {
	var result streamAssignmentResult
	if err := json.Unmarshal(msg, &result); err != nil {
		// TODO(dlc) - log
		return
	}
	acc, _ := js.srv.LookupAccount(result.Account)
	if acc == nil {
		// TODO(dlc) - log
		return
	}

	js.mu.Lock()
	defer js.mu.Unlock()

	s, cc := js.srv, js.cluster
	if cc == nil || cc.meta == nil {
		return
	}

	if sa := js.streamAssignmentOrInflight(result.Account, result.Stream); sa != nil && !sa.reassigning {
		canDelete := !result.Update && time.Since(sa.Created) < 5*time.Second

		// See if we should retry in case this cluster is full but there are others.
		if cfg, ci := sa.Config, sa.Client; cfg != nil && ci != nil && isInsufficientResourcesErr(result.Response) && canDelete {
			// If cluster is defined we can not retry.
			if cfg.Placement == nil || cfg.Placement.Cluster == _EMPTY_ {
				// If we have additional clusters to try we can retry.
				// We have already verified that ci != nil.
				if len(ci.Alternates) > 0 {
					if rg, err := js.createGroupForStream(ci, cfg); err != nil {
						s.Warnf("Retrying cluster placement for stream '%s > %s' failed due to placement error: %+v", result.Account, result.Stream, err)
					} else {
						if org := sa.Group; org != nil && len(org.Peers) > 0 {
							s.Warnf("Retrying cluster placement for stream '%s > %s' due to insufficient resources in cluster %q",
								result.Account, result.Stream, s.clusterNameForNode(org.Peers[0]))
						} else {
							s.Warnf("Retrying cluster placement for stream '%s > %s' due to insufficient resources", result.Account, result.Stream)
						}
						// Pick a new preferred leader.
						rg.setPreferred(s)
						// Get rid of previous attempt.
						if err := cc.meta.Propose(cc.term, encodeDeleteStreamAssignment(sa)); err != nil {
							return
						}
						cc.trackInflightStreamProposal(result.Account, sa, true)
						// Propose new.
						nsa := sa.copyGroup()
						nsa.Group, nsa.err = rg, nil
						if err := cc.meta.Propose(cc.term, encodeAddStreamAssignment(nsa)); err != nil {
							return
						}
						cc.trackInflightStreamProposal(result.Account, nsa, false)
						// When the new stream assignment is processed, sa.reassigning will be
						// automatically set back to false. Until then, don't process any more
						// assignment results.
						nsa.reassigning = true
						return
					}
				}
			}
		}

		// Respond to the user here.
		var resp string
		if result.Response != nil {
			resp = s.jsonResponse(result.Response)
		} else if result.Restore != nil {
			resp = s.jsonResponse(result.Restore)
		}
		if !sa.hasResponded() || result.Update {
			sa.markResponded()
			js.srv.sendAPIErrResponse(sa.Client, acc, sa.Subject, sa.Reply, _EMPTY_, resp)
		}
		// Remove this assignment if possible.
		if canDelete {
			var apiErr *ApiError
			if result.Response != nil {
				apiErr = result.Response.Error
			} else if result.Restore != nil {
				apiErr = result.Restore.Error
			}
			s.Warnf("Stream assignment for '%s > %s' rejected by assigned member: %v", sa.Client.serviceAccount(), sa.Config.Name, apiErr)
			sa.err = NewJSClusterNotAssignedError()
			if err := cc.meta.Propose(cc.term, encodeDeleteStreamAssignment(sa)); err != nil {
				return
			}
			cc.trackInflightStreamProposal(result.Account, sa, true)
		}
	}
}

func (js *jetStream) processConsumerAssignmentResults(sub *subscription, c *client, _ *Account, subject, reply string, msg []byte) {
	var result consumerAssignmentResult
	if err := json.Unmarshal(msg, &result); err != nil {
		// TODO(dlc) - log
		return
	}
	acc, _ := js.srv.LookupAccount(result.Account)
	if acc == nil {
		// TODO(dlc) - log
		return
	}

	js.mu.Lock()
	defer js.mu.Unlock()

	s, cc := js.srv, js.cluster
	if cc == nil || cc.meta == nil {
		return
	}

	if sa := js.streamAssignment(result.Account, result.Stream); sa != nil && sa.consumers != nil {
		if ca := sa.consumers[result.Consumer]; ca != nil && !ca.hasResponded() {
			js.srv.sendAPIErrResponse(ca.Client, acc, ca.Subject, ca.Reply, _EMPTY_, s.jsonResponse(result.Response))
			ca.markResponded()

			// Check if this failed.
			// TODO(dlc) - Could have mixed results, should track per peer.
			// Make sure this is recent response.
			if result.Response.Error != nil && result.Response.Error != NewJSConsumerNameExistError() && time.Since(ca.Created) < 2*time.Second {
				// Do not list in consumer names/lists.
				s.Warnf("Consumer assignment for '%s > %s > %s' rejected by assigned member: %v", ca.Client.serviceAccount(), ca.Stream, ca.Name, result.Response.Error)
				ca.err = NewJSClusterNotAssignedError()
			}
		}
	}
}

const (
	streamAssignmentSubj   = "$SYS.JSC.STREAM.ASSIGNMENT.RESULT"
	consumerAssignmentSubj = "$SYS.JSC.CONSUMER.ASSIGNMENT.RESULT"
)

// Lock should be held.
func (js *jetStream) startUpdatesSub() {
	cc, s, c := js.cluster, js.srv, js.cluster.c
	if cc.streamResults == nil {
		cc.streamResults, _ = s.systemSubscribe(streamAssignmentSubj, _EMPTY_, false, c, js.processStreamAssignmentResults)
	}
	if cc.consumerResults == nil {
		cc.consumerResults, _ = s.systemSubscribe(consumerAssignmentSubj, _EMPTY_, false, c, js.processConsumerAssignmentResults)
	}
	if cc.stepdown == nil {
		cc.stepdown, _ = s.systemSubscribe(JSApiLeaderStepDown, _EMPTY_, false, c, s.jsLeaderStepDownRequest)
	}
	if cc.peerRemove == nil {
		cc.peerRemove, _ = s.systemSubscribe(JSApiRemoveServer, _EMPTY_, false, c, s.jsLeaderServerRemoveRequest)
	}
	if cc.peerStreamMove == nil {
		cc.peerStreamMove, _ = s.systemSubscribe(JSApiServerStreamMove, _EMPTY_, false, c, s.jsLeaderServerStreamMoveRequest)
	}
	if cc.peerStreamCancelMove == nil {
		cc.peerStreamCancelMove, _ = s.systemSubscribe(JSApiServerStreamCancelMove, _EMPTY_, false, c, s.jsLeaderServerStreamCancelMoveRequest)
	}
	if js.accountPurge == nil {
		js.accountPurge, _ = s.systemSubscribe(JSApiAccountPurge, _EMPTY_, false, c, s.jsLeaderAccountPurgeRequest)
	}
}

// Lock should be held.
func (js *jetStream) stopUpdatesSub() {
	cc := js.cluster
	if cc.streamResults != nil {
		cc.s.sysUnsubscribe(cc.streamResults)
		cc.streamResults = nil
	}
	if cc.consumerResults != nil {
		cc.s.sysUnsubscribe(cc.consumerResults)
		cc.consumerResults = nil
	}
	if cc.stepdown != nil {
		cc.s.sysUnsubscribe(cc.stepdown)
		cc.stepdown = nil
	}
	if cc.peerRemove != nil {
		cc.s.sysUnsubscribe(cc.peerRemove)
		cc.peerRemove = nil
	}
	if cc.peerStreamMove != nil {
		cc.s.sysUnsubscribe(cc.peerStreamMove)
		cc.peerStreamMove = nil
	}
	if cc.peerStreamCancelMove != nil {
		cc.s.sysUnsubscribe(cc.peerStreamCancelMove)
		cc.peerStreamCancelMove = nil
	}
	if js.accountPurge != nil {
		cc.s.sysUnsubscribe(js.accountPurge)
		js.accountPurge = nil
	}
}

func (s *Server) sendDomainLeaderElectAdvisory() {
	js, cc := s.getJetStreamCluster()
	if js == nil || cc == nil {
		return
	}

	js.mu.RLock()
	node := cc.meta
	js.mu.RUnlock()

	if node == nil {
		return
	}

	adv := &JSDomainLeaderElectedAdvisory{
		TypedEvent: TypedEvent{
			Type: JSDomainLeaderElectedAdvisoryType,
			ID:   nuid.Next(),
			Time: time.Now().UTC(),
		},
		Leader:   node.GroupLeader(),
		Replicas: s.replicas(node),
		Cluster:  s.cachedClusterName(),
		Domain:   s.getOpts().JetStreamDomain,
	}

	s.publishAdvisory(nil, JSAdvisoryDomainLeaderElected, adv)
}

func (js *jetStream) processLeaderChange(isLeader bool, term uint64) {
	if js == nil {
		return
	}
	s := js.srv
	if s == nil {
		return
	}

	if isLeader {
		s.Noticef("Self is new JetStream cluster metadata leader")
		s.sendDomainLeaderElectAdvisory()
	} else {
		var node string
		if meta := js.getMetaGroup(); meta != nil {
			node = meta.GroupLeader()
		}
		if node == _EMPTY_ {
			s.Noticef("JetStream cluster no metadata leader")
		} else if srv := js.srv.serverNameForNode(node); srv == _EMPTY_ {
			s.Noticef("JetStream cluster new remote metadata leader")
		} else if clst := js.srv.clusterNameForNode(node); clst == _EMPTY_ {
			s.Noticef("JetStream cluster new metadata leader: %s", srv)
		} else {
			s.Noticef("JetStream cluster new metadata leader: %s/%s", srv, clst)
		}
	}

	js.mu.Lock()
	defer js.mu.Unlock()

	// Update our server atomic, while holding the lock to not race with API requests.
	s.isMetaLeader.Store(isLeader)
	js.cluster.term = term

	// Clear replies for peer-removes.
	js.cluster.peerRemoveReply = nil

	// Clear inflight proposal tracking.
	js.cluster.inflightStreams = nil
	js.cluster.inflightConsumers = nil

	if isLeader {
		if meta := js.cluster.meta; meta != nil && meta.IsObserver() {
			meta.StepDown()
			return
		}
	}

	if isLeader {
		js.startUpdatesSub()
	} else {
		js.stopUpdatesSub()
		// TODO(dlc) - stepdown.
	}

	// If we have been signaled to check the streams, this is for a bug that left stream
	// assignments with no sync subject after an update and no way to sync/catchup outside of the RAFT layer.
	if isLeader && js.cluster.streamsCheck {
		cc := js.cluster
		for acc, sa := range js.streamAssignmentsOrInflightSeqAllAccounts() {
			if sa.unsupported != nil {
				continue
			}
			if sa.Sync == _EMPTY_ {
				s.Warnf("Stream assignment corrupt for stream '%s > %s'", acc, sa.Config.Name)
				nsa := &streamAssignment{Group: sa.Group, Config: sa.Config, Subject: sa.Subject, Reply: sa.Reply, Client: sa.Client, Created: sa.Created}
				nsa.Sync = syncSubjForStream()
				if err := cc.meta.Propose(cc.term, encodeUpdateStreamAssignment(nsa)); err != nil {
					return
				}
				cc.trackInflightStreamProposal(acc, nsa, false)
			}
		}
		// Clear check.
		cc.streamsCheck = false
	}
}

// Lock should be held.
func (cc *jetStreamCluster) remapStreamAssignment(sa *streamAssignment, removePeer string) bool {
	// We're reassigning the peers ourselves, so any desired state is moot.
	sa.Group.Desired = nil

	// Invoke placement algo passing RG peers that stay (existing) and the peer that is being removed (ignore)
	var retain, ignore []string
	for _, v := range sa.Group.Peers {
		if v == removePeer {
			ignore = append(ignore, v)
		} else {
			retain = append(retain, v)
		}
	}

	newPeers, placementError := cc.selectPeerGroup(len(sa.Group.Peers), sa.Group.Cluster, sa.Config, retain, 0, ignore)

	if placementError == nil {
		sa.Group.Peers = newPeers
		// Don't influence preferred leader.
		sa.Group.Preferred = _EMPTY_
		return true
	}

	// If R1 just return to avoid bricking the stream.
	if sa.Group.node == nil || len(sa.Group.Peers) == 1 {
		return false
	}

	// If we are here let's remove the peer at least, as long as we are R>1
	for i, peer := range sa.Group.Peers {
		if peer == removePeer {
			sa.Group.Peers[i] = sa.Group.Peers[len(sa.Group.Peers)-1]
			sa.Group.Peers = sa.Group.Peers[:len(sa.Group.Peers)-1]
			break
		}
	}
	return false
}

type selectPeerError struct {
	excludeTag  bool
	offline     bool
	noStorage   bool
	uniqueTag   bool
	misc        bool
	noJsClust   bool
	noMatchTags map[string]struct{}
	excludeTags map[string]struct{}
}

func (e *selectPeerError) Error() string {
	b := strings.Builder{}
	writeBoolErrReason := func(hasErr bool, errMsg string) {
		if !hasErr {
			return
		}
		b.WriteString(", ")
		b.WriteString(errMsg)
	}
	b.WriteString("no suitable peers for placement")
	writeBoolErrReason(e.offline, "peer offline")
	writeBoolErrReason(e.excludeTag, "exclude tag set")
	writeBoolErrReason(e.noStorage, "insufficient storage")
	writeBoolErrReason(e.uniqueTag, "server tag not unique")
	writeBoolErrReason(e.misc, "miscellaneous issue")
	writeBoolErrReason(e.noJsClust, "jetstream not enabled in cluster")
	if len(e.noMatchTags) != 0 {
		b.WriteString(", tags not matched [")
		var firstTagWritten bool
		for tag := range e.noMatchTags {
			if firstTagWritten {
				b.WriteString(", ")
			}
			firstTagWritten = true
			b.WriteRune('\'')
			b.WriteString(tag)
			b.WriteRune('\'')
		}
		b.WriteString("]")
	}
	if len(e.excludeTags) != 0 {
		b.WriteString(", tags excluded [")
		var firstTagWritten bool
		for tag := range e.excludeTags {
			if firstTagWritten {
				b.WriteString(", ")
			}
			firstTagWritten = true
			b.WriteRune('\'')
			b.WriteString(tag)
			b.WriteRune('\'')
		}
		b.WriteString("]")
	}

	return b.String()
}

func (e *selectPeerError) addMissingTag(t string) {
	if e.noMatchTags == nil {
		e.noMatchTags = map[string]struct{}{}
	}
	e.noMatchTags[t] = struct{}{}
}

func (e *selectPeerError) addExcludeTag(t string) {
	if e.excludeTags == nil {
		e.excludeTags = map[string]struct{}{}
	}
	e.excludeTags[t] = struct{}{}
}

func (e *selectPeerError) accumulate(eAdd *selectPeerError) {
	if eAdd == nil {
		return
	}
	acc := func(val *bool, valAdd bool) {
		if valAdd {
			*val = valAdd
		}
	}
	acc(&e.offline, eAdd.offline)
	acc(&e.excludeTag, eAdd.excludeTag)
	acc(&e.noStorage, eAdd.noStorage)
	acc(&e.uniqueTag, eAdd.uniqueTag)
	acc(&e.misc, eAdd.misc)
	acc(&e.noJsClust, eAdd.noJsClust)
	for tag := range eAdd.noMatchTags {
		e.addMissingTag(tag)
	}
	for tag := range eAdd.excludeTags {
		e.addExcludeTag(tag)
	}
}

// selectPeerGroup will select a group of peers to start a raft group.
// when peers exist already the unique tag prefix check for the replaceFirstExisting will be skipped
// js lock should be held.
func (cc *jetStreamCluster) selectPeerGroup(r int, cluster string, cfg *StreamConfig, existing []string, replaceFirstExisting int, ignore []string) ([]string, *selectPeerError) {
	if cluster == _EMPTY_ || cfg == nil {
		return nil, &selectPeerError{misc: true}
	}

	var maxBytes uint64
	if cfg.MaxBytes > 0 {
		maxBytes = uint64(cfg.MaxBytes)
	}

	// Check for tags.
	type tagInfo struct {
		tag     string
		exclude bool
	}
	var ti []tagInfo
	if cfg.Placement != nil {
		ti = make([]tagInfo, 0, len(cfg.Placement.Tags))
		for _, t := range cfg.Placement.Tags {
			ti = append(ti, tagInfo{
				tag:     strings.TrimPrefix(t, "!"),
				exclude: strings.HasPrefix(t, "!"),
			})
		}
	}

	// Used for weighted sorting based on availability.
	type wn struct {
		id    string
		avail uint64
		off   bool
		lag   bool
		ha    int
		ns    int
	}
	// Prefer online servers to offline ones, and caught up servers to lagging ones.
	cmpAvailability := func(i, j wn) int {
		rank := func(n wn) int {
			if n.off {
				return 2
			} else if n.lag {
				return 1
			}
			return 0
		}
		return cmp.Compare(rank(i), rank(j))
	}

	var nodes []wn
	// peers is a randomized list
	s, peers := cc.s, cc.meta.Peers()

	uniqueTagPrefix := s.getOpts().JetStreamUniqueTag
	if uniqueTagPrefix != _EMPTY_ {
		for _, t := range ti {
			if strings.HasPrefix(t.tag, uniqueTagPrefix) {
				// disable uniqueness check if explicitly listed in tags
				uniqueTagPrefix = _EMPTY_
				break
			}
		}
	}
	var uniqueTags = make(map[string]*nodeInfo)

	checkUniqueTag := func(ni *nodeInfo) (bool, *nodeInfo) {
		for _, t := range ni.tags {
			if strings.HasPrefix(t, uniqueTagPrefix) {
				if n, ok := uniqueTags[t]; !ok {
					uniqueTags[t] = ni
					return true, ni
				} else {
					return false, n
				}
			}
		}
		// default requires the unique prefix to be present
		return false, nil
	}

	// Map existing.
	var ep map[string]struct{}
	if le := len(existing); le > 0 {
		if le >= r {
			return existing[:r], nil
		}
		ep = make(map[string]struct{})
		for i, p := range existing {
			ep[p] = struct{}{}
			if uniqueTagPrefix == _EMPTY_ {
				continue
			}
			si, ok := s.nodeToInfo.Load(p)
			if !ok || si == nil || i < replaceFirstExisting {
				continue
			}
			ni := si.(nodeInfo)
			// collect unique tags, but do not require them as this node is already part of the peerset
			checkUniqueTag(&ni)
		}
	}

	// Map ignore
	var ip map[string]struct{}
	if li := len(ignore); li > 0 {
		ip = make(map[string]struct{})
		for _, p := range ignore {
			ip[p] = struct{}{}
		}
	}

	// Grab the number of streams and HA assets currently assigned to each peer.
	// HAAssets under usage is async, so calculate here in realtime based on assignments.
	peerStreams := make(map[string]int, len(peers))
	peerHA := make(map[string]int, len(peers))
	for _, asa := range cc.streams {
		for _, sa := range asa {
			if sa.unsupported != nil {
				continue
			}
			isHA := len(sa.Group.Peers) > 1
			for _, peer := range sa.Group.Peers {
				peerStreams[peer]++
				if isHA {
					peerHA[peer]++
				}
			}
		}
	}

	maxHaAssets := s.getOpts().JetStreamLimits.MaxHAAssets

	// An error is a result of multiple individual placement decisions.
	// Which is why we keep taps on how often which one happened.
	err := selectPeerError{}

	var onlinePeers int

	// Shuffle them up.
	rand.Shuffle(len(peers), func(i, j int) { peers[i], peers[j] = peers[j], peers[i] })
	for _, p := range peers {
		si, ok := s.nodeToInfo.Load(p.ID)
		if !ok || si == nil {
			err.misc = true
			continue
		}
		ni := si.(nodeInfo)
		// Only select from the designated named cluster.
		if ni.cluster != cluster {
			s.Debugf("Peer selection: discard %s@%s reason: not target cluster %s", ni.name, ni.cluster, cluster)
			continue
		}

		// If we've never heard from a server, don't consider.
		if ni.cfg == nil || ni.stats == nil {
			s.Debugf("Peer selection: discard %s@%s reason: offline", ni.name, ni.cluster)
			err.offline = true
			continue
		}

		// If ignore skip
		if _, ok := ip[p.ID]; ok {
			continue
		}

		// If existing also skip, we will add back in to front of the list when done.
		if _, ok := ep[p.ID]; ok {
			continue
		}

		if ni.tags.Contains(jsExcludePlacement) {
			s.Debugf("Peer selection: discard %s@%s tags: %v reason: %s present",
				ni.name, ni.cluster, ni.tags, jsExcludePlacement)
			err.excludeTag = true
			continue
		}

		if len(ti) > 0 {
			matched := true
			for _, t := range ti {
				contains := ni.tags.Contains(t.tag)
				if t.exclude && contains {
					matched = false
					s.Debugf("Peer selection: discard %s@%s tags: %v reason: excluded tag %s present",
						ni.name, ni.cluster, ni.tags, t)
					err.addExcludeTag(t.tag)
					break
				} else if !t.exclude && !contains {
					matched = false
					s.Debugf("Peer selection: discard %s@%s tags: %v reason: mandatory tag %s not present",
						ni.name, ni.cluster, ni.tags, t)
					err.addMissingTag(t.tag)
					break
				}
			}
			if !matched {
				continue
			}
		}

		var available uint64
		if ni.stats != nil {
			switch cfg.Storage {
			case MemoryStorage:
				used := ni.stats.ReservedMemory
				if ni.stats.Memory > used {
					used = ni.stats.Memory
				}
				if ni.cfg.MaxMemory > int64(used) {
					available = uint64(ni.cfg.MaxMemory) - used
				}
			case FileStorage:
				used := ni.stats.ReservedStore
				if ni.stats.Store > used {
					used = ni.stats.Store
				}
				if ni.cfg.MaxStore > int64(used) {
					available = uint64(ni.cfg.MaxStore) - used
				}
			}
		}

		// Otherwise check if we have enough room if maxBytes set.
		if maxBytes > 0 && maxBytes > available {
			s.Warnf("Peer selection: discard %s@%s (Max Bytes: %d) exceeds available %s storage of %d bytes",
				ni.name, ni.cluster, maxBytes, cfg.Storage.String(), available)
			err.noStorage = true
			continue
		}
		// HAAssets contain _meta_ which we want to ignore, hence > and not >=.
		if maxHaAssets > 0 && ni.stats != nil && ni.stats.HAAssets > maxHaAssets {
			s.Warnf("Peer selection: discard %s@%s (HA Asset Count: %d) exceeds max ha asset limit of %d for stream placement",
				ni.name, ni.cluster, ni.stats.HAAssets, maxHaAssets)
			err.misc = true
			continue
		}

		if uniqueTagPrefix != _EMPTY_ {
			if unique, owner := checkUniqueTag(&ni); !unique {
				if owner != nil {
					s.Debugf("Peer selection: discard %s@%s tags:%v reason: unique prefix %s owned by %s@%s",
						ni.name, ni.cluster, ni.tags, owner.name, owner.cluster)
				} else {
					s.Debugf("Peer selection: discard %s@%s tags:%v reason: unique prefix %s not present",
						ni.name, ni.cluster, ni.tags)
				}
				err.uniqueTag = true
				continue
			}
		}
		// Add to our list of potential nodes.
		nodes = append(nodes, wn{p.ID, available, ni.offline, !cc.meta.IsFollowerCaughtUp(p.ID), peerHA[p.ID], peerStreams[p.ID]})
		if !ni.offline {
			onlinePeers++
		}
	}

	// If we could not select enough peers, fail.
	quorum := r/2 + 1
	missingQuorum := onlinePeers+len(existing) < quorum
	if missingNodes := len(nodes) < (r - len(existing)); missingNodes || missingQuorum {
		if len(peers) == 0 {
			err.noJsClust = true
		} else if !missingNodes && missingQuorum {
			err.offline = true
		}
		s.Debugf("Peer selection: required %d nodes but found %d (cluster: %s replica: %d existing: %v/%d peers: %d result-peers: %d err: %+v)",
			r-len(existing), len(nodes), cluster, r, existing, replaceFirstExisting, len(peers), len(nodes), err)
		return nil, &err
	}
	// Sort based on available from most to least, breaking ties by number of total streams assigned to the peer.
	slices.SortFunc(nodes, func(i, j wn) int {
		if c := cmpAvailability(i, j); c != 0 {
			return c
		}
		if i.avail == j.avail {
			return cmp.Compare(i.ns, j.ns)
		}
		return -cmp.Compare(i.avail, j.avail) // reverse
	})
	// If we are placing a replicated stream, let's sort based on HAAssets, as that is more important to balance.
	if cfg.Replicas > 1 {
		slices.SortStableFunc(nodes, func(i, j wn) int {
			if c := cmpAvailability(i, j); c != 0 {
				return c
			}
			return cmp.Compare(i.ha, j.ha)
		})
	}

	var results []string
	if len(existing) > 0 {
		results = append(results, existing...)
		r -= len(existing)
	}
	for _, r := range nodes[:r] {
		results = append(results, r.id)
	}
	return results, nil
}

func groupNameForStream(peers []string, storage StorageType) string {
	return groupName("S", peers, storage)
}

func groupNameForConsumer(peers []string, storage StorageType) string {
	return groupName("C", peers, storage)
}

func groupName(prefix string, peers []string, storage StorageType) string {
	gns := getHash(nuid.Next())
	return fmt.Sprintf("%s-R%d%s-%s", prefix, len(peers), storage.String()[:1], gns)
}

// returns stream count for this tier as well as applicable reservation size (not including cfg)
// jetStream read lock should be held
func (js *jetStream) tieredStreamAndReservationCount(accName, tier string, cfg *StreamConfig) (int, int64) {
	var numStreams int
	var reservation int64
	for sa := range js.streamAssignmentsOrInflightSeq(accName) {
		// Don't count the stream toward the limit if it already exists.
		if sa.Config.Name == cfg.Name {
			continue
		}
		if tier == _EMPTY_ || isSameTier(sa.Config.Replicas, cfg.Replicas) {
			numStreams++
			if sa.Config.MaxBytes > 0 && sa.Config.Storage == cfg.Storage {
				reservation = addSaturate(reservation, accountReservation(tier, sa.Config.Replicas, sa.Config.MaxBytes))
			}
		}
	}
	return numStreams, reservation
}

// createGroupForStream will create a group for assignment for the stream.
// Lock should be held.
func (js *jetStream) createGroupForStream(ci *ClientInfo, cfg *StreamConfig) (*raftGroup, *selectPeerError) {
	replicas := cfg.Replicas
	if replicas == 0 {
		replicas = 1
	}

	// Default connected cluster from the request origin.
	cc, cluster := js.cluster, ci.Cluster
	// If specified, override the default.
	clusterDefined := cfg.Placement != nil && cfg.Placement.Cluster != _EMPTY_
	if clusterDefined {
		cluster = cfg.Placement.Cluster
	}
	clusters := []string{cluster}
	if !clusterDefined {
		clusters = append(clusters, ci.Alternates...)
	}

	// Need to create a group here.
	errs := &selectPeerError{}
	for _, cn := range clusters {
		peers, err := cc.selectPeerGroup(replicas, cn, cfg, nil, 0, nil)
		if len(peers) < replicas {
			errs.accumulate(err)
			continue
		}
		return &raftGroup{Name: groupNameForStream(peers, cfg.Storage), Storage: cfg.Storage, Peers: peers, Cluster: cn}, nil
	}
	return nil, errs
}

func (acc *Account) selectLimits(replicas int) (*JetStreamAccountLimits, string, *jsAccount, *ApiError) {
	// Grab our jetstream account info.
	acc.mu.RLock()
	jsa := acc.js
	acc.mu.RUnlock()

	if jsa == nil {
		return nil, _EMPTY_, nil, NewJSNotEnabledForAccountError()
	}

	jsa.usageMu.RLock()
	selectedLimits, tierName, ok := jsa.selectLimits(replicas)
	jsa.usageMu.RUnlock()

	if !ok {
		return nil, _EMPTY_, nil, NewJSNoLimitsError()
	}
	return &selectedLimits, tierName, jsa, nil
}

// Read lock needs to be held
func (js *jetStream) jsClusteredStreamLimitsCheck(acc *Account, cfg *StreamConfig) *ApiError {
	var replicas int
	if cfg != nil {
		replicas = cfg.Replicas
	}
	selectedLimits, tier, _, apiErr := acc.selectLimits(replicas)
	if apiErr != nil {
		return apiErr
	}

	numStreams, reservations := js.tieredStreamAndReservationCount(acc.Name, tier, cfg)
	if selectedLimits.MaxStreams > 0 && numStreams >= selectedLimits.MaxStreams {
		return NewJSMaximumStreamsLimitError()
	}
	// Check for account limits here before proposing.
	if err := js.checkAccountLimits(selectedLimits, tier, cfg, reservations); err != nil {
		return NewJSStreamLimitsError(err, Unless(err))
	}
	return nil
}

func (s *Server) jsClusteredStreamRequest(ci *ClientInfo, acc *Account, subject, reply string, rmsg []byte, config *StreamConfigRequest) {
	js, cc := s.getJetStreamCluster()
	if js == nil || cc == nil {
		return
	}

	var resp = JSApiStreamCreateResponse{ApiResponse: ApiResponse{Type: JSApiStreamCreateResponseType}}

	ccfg, apiErr := s.checkStreamCfg(&config.StreamConfig, acc, config.Pedantic)
	if apiErr != nil {
		resp.Error = apiErr
		s.sendAPIErrResponse(ci, acc, subject, reply, string(rmsg), s.jsonResponse(&resp))
		return
	}
	cfg := &ccfg

	// Now process the request and proposal.
	js.mu.Lock()
	defer js.mu.Unlock()

	var self *streamAssignment
	var rg *raftGroup
	var syncSubject string

	// Capture if we have existing/inflight assignment first.
	if osa := js.streamAssignmentOrInflight(acc.Name, cfg.Name); osa != nil {
		copyStreamMetadata(cfg, osa.Config)
		// Set the index name on both to ensure the DeepEqual works
		matchSourceIndexNames(cfg, osa.Config)
		if !reflect.DeepEqual(osa.Config, cfg) {
			resp.Error = NewJSStreamNameExistError()
			s.sendAPIErrResponse(ci, acc, subject, reply, string(rmsg), s.jsonResponse(&resp))
			return
		}
		// This is an equal assignment.
		self, rg, syncSubject = osa, osa.Group, osa.Sync
	}

	if cfg.Sealed {
		resp.Error = NewJSStreamInvalidConfigError(fmt.Errorf("stream configuration for create can not be sealed"))
		s.sendAPIErrResponse(ci, acc, subject, reply, string(rmsg), s.jsonResponse(&resp))
		return
	}

	// Check for subject collisions here.
	if js.subjectsOverlap(acc.Name, cfg.Subjects, self) {
		resp.Error = NewJSStreamSubjectOverlapError()
		s.sendAPIErrResponse(ci, acc, subject, reply, string(rmsg), s.jsonResponse(&resp))
		return
	}

	apiErr = js.jsClusteredStreamLimitsCheck(acc, cfg)
	// Check for stream limits here before proposing. These need to be tracked from meta layer, not jsa.
	if apiErr != nil {
		resp.Error = apiErr
		s.sendAPIErrResponse(ci, acc, subject, reply, string(rmsg), s.jsonResponse(&resp))
		return
	}

	// Create a new one here if needed.
	if rg == nil {
		nrg, err := js.createGroupForStream(ci, cfg)
		if err != nil {
			resp.Error = NewJSClusterNoPeersError(err)
			s.sendAPIErrResponse(ci, acc, subject, reply, string(rmsg), s.jsonResponse(&resp))
			return
		}
		rg = nrg
		// Pick a preferred leader.
		rg.setPreferred(s)
	}

	if syncSubject == _EMPTY_ {
		syncSubject = syncSubjForStream()
	}
	// Preserve the original creation time on an idempotent create retry.
	created := time.Now().UTC()
	if self != nil {
		created = self.Created
	}
	// Sync subject for post snapshot sync.
	sa := &streamAssignment{Group: rg, Sync: syncSubject, Config: cfg, Subject: subject, Reply: reply, Client: ci, Created: created}
	if err := cc.meta.Propose(cc.term, encodeAddStreamAssignment(sa)); err != nil {
		return
	}
	// On success, add this as an inflight proposal so we can apply limits
	// on concurrent create requests while this stream assignment has
	// possibly not been processed yet.
	cc.trackInflightStreamProposal(acc.Name, sa, false)
}

var (
	errReqTimeout = errors.New("timeout while waiting for response")
	errReqSrvExit = errors.New("server shutdown while waiting for response")
)

// blocking utility call to perform requests on the system account
// returns (synchronized) v or error
func sysRequest[T any](s *Server, subjFormat string, args ...any) (*T, error) {
	isubj := fmt.Sprintf(subjFormat, args...)

	s.mu.Lock()
	if s.sys == nil {
		s.mu.Unlock()
		return nil, ErrNoSysAccount
	}
	inbox := s.newRespInbox()
	results := make(chan *T, 1)
	s.sys.replies[inbox] = func(_ *subscription, _ *client, _ *Account, _, _ string, msg []byte) {
		var v T
		if err := json.Unmarshal(msg, &v); err != nil {
			s.Warnf("Error unmarshalling response for request '%s':%v", isubj, err)
			return
		}
		select {
		case results <- &v:
		default:
			s.Warnf("Failed placing request response on internal channel")
		}
	}
	s.mu.Unlock()

	s.sendInternalMsgLocked(isubj, inbox, nil, nil)

	defer func() {
		s.mu.Lock()
		defer s.mu.Unlock()
		if s.sys != nil && s.sys.replies != nil {
			delete(s.sys.replies, inbox)
		}
	}()

	ttl := time.NewTimer(2 * time.Second)
	defer ttl.Stop()

	select {
	case <-s.quitCh:
		return nil, errReqSrvExit
	case <-ttl.C:
		return nil, errReqTimeout
	case data := <-results:
		return data, nil
	}
}

func (s *Server) jsClusteredStreamUpdateRequest(ci *ClientInfo, acc *Account, subject, reply string, rmsg []byte, cfg *StreamConfig, peerSet []string, pedantic bool) {
	js, cc := s.getJetStreamCluster()
	if js == nil || cc == nil {
		return
	}

	// Now process the request and proposal.
	js.mu.Lock()
	defer js.mu.Unlock()
	meta := cc.meta
	if meta == nil {
		return
	}

	var resp = JSApiStreamUpdateResponse{ApiResponse: ApiResponse{Type: JSApiStreamUpdateResponseType}}

	osa := js.streamAssignmentOrInflight(acc.Name, cfg.Name)
	if osa == nil {
		resp.Error = NewJSStreamNotFoundError()
		s.sendAPIErrResponse(ci, acc, subject, reply, string(rmsg), s.jsonResponse(&resp))
		return
	}

	// Don't allow updating if all peers are offline.
	if s.allPeersOffline(osa.Group) {
		resp.Error = NewJSStreamOfflineError()
		s.sendDelayedAPIErrResponse(ci, acc, subject, reply, string(rmsg), s.jsonResponse(&resp), nil, errRespDelay)
		return
	}

	// Update asset version metadata.
	setStaticStreamMetadata(cfg)

	var newCfg *StreamConfig
	if jsa := js.accounts[acc.Name]; jsa != nil {
		js.mu.Unlock()
		ncfg, err := jsa.configUpdateCheck(osa.Config, cfg, s, pedantic)
		js.mu.Lock()
		if err != nil {
			resp.Error = NewJSStreamUpdateError(err, Unless(err))
			s.sendAPIErrResponse(ci, acc, subject, reply, string(rmsg), s.jsonResponse(&resp))
			return
		} else {
			newCfg = ncfg
		}
	} else {
		resp.Error = NewJSNotEnabledForAccountError()
		s.sendAPIErrResponse(ci, acc, subject, reply, string(rmsg), s.jsonResponse(&resp))
		return
	}
	// Check for mirror changes which are not allowed.
	// We will allow removing the mirror config to "promote" the mirror to a normal stream.
	if newCfg.Mirror != nil && !reflect.DeepEqual(newCfg.Mirror, osa.Config.Mirror) {
		resp.Error = NewJSStreamMirrorNotUpdatableError()
		s.sendAPIErrResponse(ci, acc, subject, reply, string(rmsg), s.jsonResponse(&resp))
		return
	}

	// Check for subject collisions here.
	if js.subjectsOverlap(acc.Name, cfg.Subjects, osa) {
		resp.Error = NewJSStreamSubjectOverlapError()
		s.sendAPIErrResponse(ci, acc, subject, reply, string(rmsg), s.jsonResponse(&resp))
		return
	}

	// In the event that some of the stream-level limits have changed, yell appropriately
	// if any of the consumers exceed that limit.
	oldInactiveThreshold, newInactiveThreshold := osa.Config.ConsumerLimits.InactiveThreshold, newCfg.ConsumerLimits.InactiveThreshold
	oldMaxAckPending, newMaxAckPending := osa.Config.ConsumerLimits.MaxAckPending, newCfg.ConsumerLimits.MaxAckPending
	updateLimits := (newInactiveThreshold > 0 && oldInactiveThreshold != newInactiveThreshold) ||
		(newMaxAckPending > 0 && oldMaxAckPending != newMaxAckPending)
	if updateLimits {
		var errorConsumers []string
		for ca := range js.consumerAssignmentsOrInflightSeq(acc.Name, cfg.Name) {
			if ca.Config == nil {
				continue
			}
			if (newInactiveThreshold > 0 && ca.Config.InactiveThreshold > newInactiveThreshold) ||
				(newMaxAckPending > 0 && ca.Config.MaxAckPending > newMaxAckPending) {
				errorConsumers = append(errorConsumers, ca.Name)
			}
		}
		if len(errorConsumers) > 0 {
			err := fmt.Errorf("change to limits violates consumers: %s", strings.Join(errorConsumers, ", "))
			resp.Error = NewJSStreamUpdateError(err)
			s.sendAPIErrResponse(ci, acc, subject, reply, string(rmsg), s.jsonResponse(&resp))
			return
		}
	}

	// Make copy so to not change original.
	rg := osa.copyGroup().Group

	// We don't act on desired state recorded by a newer server, so the stream is still at
	// its origin while the config already holds the target. Compare against where it
	// actually is, or asking for the target it's reported to have would be a no-op, and
	// drop the desired state so we reconcile the group ourselves.
	oldReplicas, oldPlacement := osa.Config.Replicas, osa.Config.Placement
	if d := rg.Desired; d != nil {
		// The peer set is how far the migration got, not necessarily the origin.
		oldReplicas = len(rg.Peers)
		if d.Origin != nil {
			oldPlacement = d.Origin.Placement
		}
		rg.Desired = nil
	}

	// Check for a move request.
	var isMoveRequest, isMoveCancel bool
	if lPeerSet := len(peerSet); lPeerSet > 0 {
		isMoveRequest = true
		// check if this is a cancellation
		if lPeerSet == oldReplicas && lPeerSet <= len(rg.Peers) {
			isMoveCancel = true
			// can only be a cancellation if the peer sets overlap as expected
			for i := 0; i < lPeerSet; i++ {
				if peerSet[i] != rg.Peers[i] {
					isMoveCancel = false
					break
				}
			}
		}
	} else {
		isMoveRequest = newCfg.Placement != nil && !reflect.DeepEqual(oldPlacement, newCfg.Placement)
	}

	// Check for replica changes.
	isReplicaChange := newCfg.Replicas != oldReplicas

	// We stage consumer updates and do them after the stream update.
	var consumers []*consumerAssignment

	// Check if this is a move request, but no cancellation, and we are already moving this stream.
	if isMoveRequest && !isMoveCancel && oldReplicas != len(rg.Peers) {
		// obtain stats to include in error message
		msg := _EMPTY_
		if s.allPeersOffline(rg) {
			msg = fmt.Sprintf("all %d peers offline", len(rg.Peers))
		} else {
			// Need to release js lock.
			js.mu.Unlock()
			if si, err := sysRequest[StreamInfo](s, clusterStreamInfoT, ci.serviceAccount(), cfg.Name); err != nil {
				msg = fmt.Sprintf("error retrieving info: %s", err.Error())
			} else if si != nil {
				currentCount := 0
				if si.Cluster.Leader != _EMPTY_ {
					currentCount++
				}
				combinedLag := uint64(0)
				for _, r := range si.Cluster.Replicas {
					if r.Current {
						currentCount++
					}
					combinedLag += r.Lag
				}
				msg = fmt.Sprintf("total peers: %d, current peers: %d, combined lag: %d",
					len(rg.Peers), currentCount, combinedLag)
			}
			// Re-acquire here.
			js.mu.Lock()
		}
		resp.Error = NewJSStreamMoveInProgressError(msg)
		s.sendAPIErrResponse(ci, acc, subject, reply, string(rmsg), s.jsonResponse(&resp))
		return
	}

	// Can not move and scale at same time.
	if isMoveRequest && isReplicaChange {
		resp.Error = NewJSStreamMoveAndScaleError()
		s.sendAPIErrResponse(ci, acc, subject, reply, string(rmsg), s.jsonResponse(&resp))
		return
	}

	// Reset notion of scaling up, if this was done in a previous update.
	rg.ScaleUp = false
	if isReplicaChange {
		isScaleUp := newCfg.Replicas > len(rg.Peers)
		// We are adding new peers here.
		if isScaleUp {
			// Check that we have the allocation available.
			if err := js.jsClusteredStreamLimitsCheck(acc, newCfg); err != nil {
				resp.Error = err
				s.sendAPIErrResponse(ci, acc, subject, reply, string(rmsg), s.jsonResponse(&resp))
				return
			}
			// Check if we do not have a cluster assigned, and if we do not make sure we
			// try to pick one. This could happen with older streams that were assigned by
			// previous servers.
			if rg.Cluster == _EMPTY_ {
				// Prefer placement directives if we have them.
				if newCfg.Placement != nil && newCfg.Placement.Cluster != _EMPTY_ {
					rg.Cluster = newCfg.Placement.Cluster
				} else {
					// Fall back to the cluster assignment from the client.
					rg.Cluster = ci.Cluster
				}
			}
			peers, err := cc.selectPeerGroup(newCfg.Replicas, rg.Cluster, newCfg, rg.Peers, 0, nil)
			if err != nil {
				resp.Error = NewJSClusterNoPeersError(err)
				s.sendAPIErrResponse(ci, acc, subject, reply, string(rmsg), s.jsonResponse(&resp))
				return
			}
			// Single nodes are not recorded by the NRG layer so we can rename.
			if len(peers) == 1 || oldReplicas == 1 {
				rg.Name = groupNameForStream(peers, rg.Storage)
			}
			if len(rg.Peers) == 1 {
				// This is scale up from being a singleton, set preferred to that singleton.
				rg.Preferred = rg.Peers[0]
			}
			rg.ScaleUp = true
			rg.Peers = peers
		} else {
			// The tier we're scaling down to must exist.
			if _, _, _, err := acc.selectLimits(newCfg.Replicas); err != nil {
				resp.Error = err
				s.sendAPIErrResponse(ci, acc, subject, reply, string(rmsg), s.jsonResponse(&resp))
				return
			}
			// We are deleting nodes here. We want to do our best to preserve the current leader.
			// We have support now from above that guarantees we are in our own Go routine, so can
			// ask for stream info from the stream leader to make sure we keep the leader in the new list.
			var curLeader string
			if !s.allPeersOffline(rg) {
				// Need to release js lock.
				js.mu.Unlock()
				if si, err := sysRequest[StreamInfo](s, clusterStreamInfoT, ci.serviceAccount(), cfg.Name); err != nil {
					s.Warnf("Did not receive stream info results for '%s > %s' due to: %s", acc, cfg.Name, err)
				} else if si != nil {
					if cl := si.Cluster; cl != nil && cl.Leader != _EMPTY_ {
						curLeader = getHash(cl.Leader)
					}
				}
				// Re-acquire here.
				js.mu.Lock()
			}
			// If we identified a leader make sure its part of the new group.
			rg.Peers = s.selectScaleDownPeers(rg.Peers, curLeader, newCfg.Replicas)
			// Single nodes are not recorded by the NRG layer so we can rename.
			// MUST do this, otherwise a scaleup afterward could potentially lead to inconsistencies.
			if len(rg.Peers) == 1 {
				rg.Name = groupNameForStream(rg.Peers, rg.Storage)
			}
		}

		// Need to remap any consumers.
		for ca := range js.consumerAssignmentsOrInflightSeq(acc.Name, osa.Config.Name) {
			// Legacy ephemerals are R=1 but present as R=0, so only auto-remap named consumers, or if we are downsizing the consumer peers.
			// If stream is interest or workqueue policy always remaps since they require peer parity with stream.
			numPeers := len(ca.Group.Peers)
			isAutoScale := ca.Config.Replicas == 0 && (ca.Config.Durable != _EMPTY_ || ca.Config.Name != _EMPTY_)
			if isAutoScale || numPeers > len(rg.Peers) || cfg.Retention != LimitsPolicy {
				cca := ca.copyGroup()
				// Adjust preferred as needed.
				if numPeers == 1 && isScaleUp {
					cca.Group.Preferred = ca.Group.Peers[0]
				} else {
					cca.Group.Preferred = _EMPTY_
				}
				// Assign new peers.
				cca.Group.Peers = rg.Peers
				// Single nodes are not recorded by the NRG layer so we can rename.
				if len(cca.Group.Peers) == 1 || numPeers == 1 {
					cca.Group.Name = groupNameForConsumer(cca.Group.Peers, cca.Group.Storage)
				}
				// If the replicas was not 0 make sure it matches here.
				if cca.Config.Replicas != 0 {
					cca.Config.Replicas = len(rg.Peers)
				}
				// We can not propose here before the stream itself so we collect them.
				consumers = append(consumers, cca)

			} else if !isScaleUp {
				// We decided to leave this consumer's peer group alone but we are also scaling down.
				// We need to make sure we do not have any peers that are no longer part of the stream.
				// Note we handle down scaling of a consumer above if its number of peers were > new stream peers.
				var needReplace []string
				for _, rp := range ca.Group.Peers {
					// Check if we have an orphaned peer now for this consumer.
					if !rg.isMember(rp) {
						needReplace = append(needReplace, rp)
					}
				}
				if len(needReplace) > 0 {
					newPeers := copyStrings(rg.Peers)
					rand.Shuffle(len(newPeers), func(i, j int) { newPeers[i], newPeers[j] = newPeers[j], newPeers[i] })
					// If we had a small size then the peer set, restrict to the same number.
					if lp := len(ca.Group.Peers); lp < len(newPeers) {
						newPeers = newPeers[:lp]
					}
					cca := ca.copyGroup()
					// Assign new peers.
					cca.Group.Peers = newPeers
					// Single nodes are not recorded by the NRG layer so we can rename.
					if len(cca.Group.Peers) == 1 || numPeers == 1 {
						cca.Group.Name = groupNameForConsumer(cca.Group.Peers, cca.Group.Storage)
					}
					// If the replicas was not 0 make sure it matches here.
					if cca.Config.Replicas != 0 {
						cca.Config.Replicas = len(newPeers)
					}
					// Check if all peers are invalid. This can happen with R1 under replicated streams that are being scaled down.
					if len(needReplace) == len(ca.Group.Peers) {
						// We have to transfer state to new peers.
						// we will grab our state and attach to the new assignment.
						// TODO(dlc) - In practice we would want to make sure the consumer is paused.
						// Need to release js lock.
						js.mu.Unlock()
						if ci, err := sysRequest[ConsumerInfo](s, clusterConsumerInfoT, acc, osa.Config.Name, ca.Name); err != nil {
							s.Warnf("Did not receive consumer info results for '%s > %s > %s' due to: %s", acc, osa.Config.Name, ca.Name, err)
						} else if ci != nil {
							cca.State = &ConsumerState{
								Delivered: SequencePair{
									Consumer: ci.Delivered.Consumer,
									Stream:   ci.Delivered.Stream,
								},
								AckFloor: SequencePair{
									Consumer: ci.AckFloor.Consumer,
									Stream:   ci.AckFloor.Stream,
								},
							}
						}
						// Re-acquire here.
						js.mu.Lock()
					}
					// We can not propose here before the stream itself so we collect them.
					consumers = append(consumers, cca)
				}
			}
		}

	} else if isMoveRequest {
		if len(peerSet) == 0 {
			nrg, err := js.createGroupForStream(ci, newCfg)
			if err != nil {
				resp.Error = NewJSClusterNoPeersError(err)
				s.sendAPIErrResponse(ci, acc, subject, reply, string(rmsg), s.jsonResponse(&resp))
				return
			}
			// filter peers present in both sets
			for _, peer := range rg.Peers {
				if !slices.Contains(nrg.Peers, peer) {
					peerSet = append(peerSet, peer)
				}
			}
			peerSet = append(peerSet, nrg.Peers...)
		}
		if len(rg.Peers) == 1 {
			rg.Preferred = peerSet[0]
		}
		rg.Peers = peerSet

		for ca := range js.consumerAssignmentsOrInflightSeq(acc.Name, osa.Config.Name) {
			cca := ca.copyGroup()
			r := cca.Config.replicas(osa.Config)
			// shuffle part of cluster peer set we will be keeping
			randPeerSet := copyStrings(peerSet[len(peerSet)-newCfg.Replicas:])
			rand.Shuffle(newCfg.Replicas, func(i, j int) { randPeerSet[i], randPeerSet[j] = randPeerSet[j], randPeerSet[i] })
			// move overlapping peers at the end of randPeerSet and keep a tally of non overlapping peers
			dropPeerSet := make([]string, 0, len(cca.Group.Peers))
			for _, p := range cca.Group.Peers {
				found := false
				for i, rp := range randPeerSet {
					if p == rp {
						randPeerSet[i] = randPeerSet[newCfg.Replicas-1]
						randPeerSet[newCfg.Replicas-1] = p
						found = true
						break
					}
				}
				if !found {
					dropPeerSet = append(dropPeerSet, p)
				}
			}
			cPeerSet := randPeerSet[newCfg.Replicas-r:]
			// In case of a set or cancel simply assign
			if len(peerSet) == newCfg.Replicas {
				cca.Group.Peers = cPeerSet
			} else {
				cca.Group.Peers = append(dropPeerSet, cPeerSet...)
			}
			// make sure it overlaps with peers and remove if not
			if cca.Group.Preferred != _EMPTY_ {
				if !slices.Contains(cca.Group.Peers, cca.Group.Preferred) {
					cca.Group.Preferred = _EMPTY_
				}
			}
			// We can not propose here before the stream itself so we collect them.
			consumers = append(consumers, cca)
		}
	} else {
		// All other updates make sure no preferred is set.
		rg.Preferred = _EMPTY_
	}

	syncSubject := osa.Sync
	if syncSubject == _EMPTY_ {
		syncSubject = syncSubjForStream()
	}
	sa := &streamAssignment{Group: rg, Sync: syncSubject, Created: osa.Created, Config: newCfg, Subject: subject, Reply: reply, Client: ci}
	if err := meta.Propose(cc.term, encodeUpdateStreamAssignment(sa)); err != nil {
		return
	}
	cc.trackInflightStreamProposal(acc.Name, sa, false)

	// Process any staged consumers.
	for _, ca := range consumers {
		if err := meta.Propose(cc.term, encodeAddConsumerAssignment(ca)); err != nil {
			return
		}
		cc.trackInflightConsumerProposal(acc.Name, sa.Config.Name, ca, false)
	}
}

func (s *Server) jsClusteredStreamDeleteRequest(ci *ClientInfo, acc *Account, stream, subject, reply string, rmsg []byte) {
	js, cc := s.getJetStreamCluster()
	if js == nil || cc == nil {
		return
	}

	js.mu.Lock()
	defer js.mu.Unlock()

	if cc.meta == nil {
		return
	}

	osa := js.streamAssignmentOrInflight(acc.Name, stream)
	if osa == nil {
		var resp = JSApiStreamDeleteResponse{ApiResponse: ApiResponse{Type: JSApiStreamDeleteResponseType}}
		resp.Error = NewJSStreamNotFoundError()
		s.sendAPIErrResponse(ci, acc, subject, reply, string(rmsg), s.jsonResponse(&resp))
		return
	}

	sa := &streamAssignment{Group: osa.Group, Config: osa.Config, Subject: subject, Reply: reply, Client: ci, Created: osa.Created}
	if err := cc.meta.Propose(cc.term, encodeDeleteStreamAssignment(sa)); err != nil {
		return
	}
	cc.trackInflightStreamProposal(acc.Name, sa, true)
}

// Process a clustered purge request.
func (s *Server) jsClusteredStreamPurgeRequest(
	ci *ClientInfo,
	acc *Account,
	mset *stream,
	stream, subject, reply string,
	rmsg []byte,
	preq *JSApiStreamPurgeRequest,
) {
	js, cc := s.getJetStreamCluster()
	if js == nil || cc == nil || mset == nil {
		return
	}

	js.mu.Lock()
	sa := js.streamAssignment(acc.Name, stream)
	if sa == nil {
		resp := JSApiStreamPurgeResponse{ApiResponse: ApiResponse{Type: JSApiStreamPurgeResponseType}}
		resp.Error = NewJSStreamNotFoundError()
		s.sendAPIErrResponse(ci, acc, subject, reply, string(rmsg), s.jsonResponse(&resp))
		js.mu.Unlock()
		return
	}

	if n := sa.Group.node; n != nil {
		sp := encodeStreamPurge(&streamPurge{Stream: stream, LastSeq: mset.state().LastSeq, Subject: subject, Reply: reply, Client: ci, Request: preq})
		js.mu.Unlock()
		mset.mu.RLock()
		term := mset.term
		mset.mu.RUnlock()
		n.Propose(term, sp)
		return
	}
	js.mu.Unlock()

	var resp = JSApiStreamPurgeResponse{ApiResponse: ApiResponse{Type: JSApiStreamPurgeResponseType}}
	purged, err := mset.purge(preq)
	if err != nil {
		resp.Error = NewJSStreamGeneralError(err, Unless(err))
	} else {
		resp.Purged = purged
		resp.Success = true
	}
	s.sendAPIResponse(ci, acc, subject, reply, string(rmsg), s.jsonResponse(resp))
}

func (s *Server) jsClusteredStreamRestoreRequest(
	ci *ClientInfo,
	acc *Account,
	req *JSApiStreamRestoreRequest,
	subject, reply string, rmsg []byte) {

	js, cc := s.getJetStreamCluster()
	if js == nil || cc == nil {
		return
	}

	resp := JSApiStreamRestoreResponse{ApiResponse: ApiResponse{Type: JSApiStreamRestoreResponseType}}

	// check stream config at the start of the restore process, not at the end
	cfg, apiErr := s.checkStreamCfg(&req.Config, acc, false)
	if apiErr != nil {
		resp.Error = apiErr
		s.sendAPIErrResponse(ci, acc, subject, reply, string(rmsg), s.jsonResponse(&resp))
		return
	}

	js.mu.Lock()
	defer js.mu.Unlock()

	if cc.meta == nil {
		return
	}

	if err := js.jsClusteredStreamLimitsCheck(acc, &cfg); err != nil {
		resp.Error = err
		s.sendAPIErrResponse(ci, acc, subject, reply, string(rmsg), s.jsonResponse(&resp))
		return
	}

	if sa := js.streamAssignmentOrInflight(ci.serviceAccount(), cfg.Name); sa != nil {
		resp.Error = NewJSStreamNameExistRestoreFailedError()
		s.sendAPIErrResponse(ci, acc, subject, reply, string(rmsg), s.jsonResponse(&resp))
		return
	}

	// Raft group selection and placement.
	rg, err := js.createGroupForStream(ci, &cfg)
	if err != nil {
		resp.Error = NewJSClusterNoPeersError(err)
		s.sendAPIErrResponse(ci, acc, subject, reply, string(rmsg), s.jsonResponse(&resp))
		return
	}
	// Pick a preferred leader.
	rg.setPreferred(s)
	sa := &streamAssignment{Group: rg, Sync: syncSubjForStream(), Config: &cfg, Subject: subject, Reply: reply, Client: ci, Created: time.Now().UTC()}
	// Now add in our restore state and pre-select a peer to handle the actual receipt of the snapshot.
	sa.Restore = &req.State
	// Set ScaleUp to prevent other members from winning the initial election.
	// The preferred node clears its ScaleUp state through CampaignImmediately
	// in createRaftGroup.
	rg.ScaleUp = true
	if err := cc.meta.Propose(cc.term, encodeAddStreamAssignment(sa)); err != nil {
		return
	}
	cc.trackInflightStreamProposal(ci.serviceAccount(), sa, false)
}

// Determine if all peers for this group are offline.
func (s *Server) allPeersOffline(rg *raftGroup) bool {
	if rg == nil {
		return false
	}
	// Check to see if this stream has any servers online to respond.
	for _, peer := range rg.Peers {
		if si, ok := s.nodeToInfo.Load(peer); ok && si != nil {
			if !si.(nodeInfo).offline {
				return false
			}
		}
	}
	return true
}

// Select the peers to keep when scaling a raft group down to replicas.
// The current leader, if known and in peer set, is kept. Online peers are preferred,
// but we will fall back to offline peers to honor the requested replica count.
func (s *Server) selectScaleDownPeers(peers []string, curLeader string, replicas int) []string {
	selected := make([]string, 0, replicas)
	if curLeader != _EMPTY_ && slices.Contains(peers, curLeader) {
		selected = append(selected, curLeader)
		if len(selected) == replicas {
			return selected
		}
	}
	// Prefer online peers.
	for _, peer := range peers {
		if peer == curLeader {
			continue
		}
		if si, ok := s.nodeToInfo.Load(peer); ok && si != nil && !si.(nodeInfo).offline {
			selected = append(selected, peer)
			if len(selected) == replicas {
				return selected
			}
		}
	}

	// Fall back to offline peers for the remainder.
	for _, peer := range peers {
		if slices.Contains(selected, peer) {
			continue
		}
		selected = append(selected, peer)
		if len(selected) == replicas {
			break
		}
	}
	return selected
}

// This will do a scatter and gather operation for all streams for this account. This is only called from metadata leader.
// This will be running in a separate Go routine.
func (s *Server) jsClusteredStreamListRequest(acc *Account, ci *ClientInfo, filter string, offset int, subject, reply string, rmsg []byte) {
	defer s.grWG.Done()

	js, cc := s.getJetStreamCluster()
	if js == nil || cc == nil {
		return
	}

	js.mu.RLock()

	var streams []*streamAssignment
	for _, sa := range cc.streams[acc.Name] {
		if IsNatsErr(sa.err, JSClusterNotAssignedErr) {
			continue
		}

		if filter != _EMPTY_ {
			// These could not have subjects auto-filled in since they are raw and unprocessed.
			if len(sa.Config.Subjects) == 0 {
				if SubjectsCollide(filter, sa.Config.Name) {
					streams = append(streams, sa)
				}
			} else {
				for _, subj := range sa.Config.Subjects {
					if SubjectsCollide(filter, subj) {
						streams = append(streams, sa)
						break
					}
				}
			}
		} else {
			streams = append(streams, sa)
		}
	}

	// Needs to be sorted for offsets etc.
	if len(streams) > 1 {
		slices.SortFunc(streams, func(i, j *streamAssignment) int { return cmp.Compare(i.Config.Name, j.Config.Name) })
	}

	scnt := len(streams)
	if offset < 0 {
		offset = 0
	}
	if offset > scnt {
		offset = scnt
	}
	if offset > 0 {
		streams = streams[offset:]
	}
	if len(streams) > JSApiListLimit {
		streams = streams[:JSApiListLimit]
	}

	var resp = JSApiStreamListResponse{
		ApiResponse: ApiResponse{Type: JSApiStreamListResponseType},
		Streams:     make([]*StreamInfo, 0, len(streams)),
	}

	js.mu.RUnlock()

	if len(streams) == 0 {
		resp.Limit = JSApiListLimit
		resp.Offset = offset
		s.sendAPIResponse(ci, acc, subject, reply, string(rmsg), s.jsonResponse(resp))
		return
	}

	// Create an inbox for our responses and send out our requests.
	s.mu.Lock()
	inbox := s.newRespInbox()
	rc := make(chan *streamInfoClusterResponse, len(streams))

	// Store our handler.
	s.sys.replies[inbox] = func(sub *subscription, _ *client, _ *Account, subject, _ string, msg []byte) {
		var si streamInfoClusterResponse
		if err := json.Unmarshal(msg, &si); err != nil {
			s.Warnf("Error unmarshalling clustered stream info response:%v", err)
			return
		}
		select {
		case rc <- &si:
		default:
			s.Warnf("Failed placing remote stream info result on internal channel")
		}
	}
	s.mu.Unlock()

	// Cleanup after.
	defer func() {
		s.mu.Lock()
		if s.sys != nil && s.sys.replies != nil {
			delete(s.sys.replies, inbox)
		}
		s.mu.Unlock()
	}()

	var missingNames []string
	sent := map[string]int{}

	// Send out our requests here.
	js.mu.RLock()
	for _, sa := range streams {
		if s.allPeersOffline(sa.Group) {
			// Place offline onto our results by hand here.
			si := &StreamInfo{
				Config:    *sa.Config,
				Created:   sa.Created,
				Cluster:   js.offlineClusterInfo(sa.Group),
				TimeStamp: time.Now().UTC(),
			}
			resp.Streams = append(resp.Streams, si)
			missingNames = append(missingNames, sa.Config.Name)
		} else {
			isubj := fmt.Sprintf(clusterStreamInfoT, sa.Client.serviceAccount(), sa.Config.Name)
			s.sendInternalMsgLocked(isubj, inbox, nil, nil)
			sent[sa.Config.Name] = len(sa.consumers)
		}
	}
	// Don't hold lock.
	js.mu.RUnlock()

	const timeout = 4 * time.Second
	notActive := time.NewTimer(timeout)
	defer notActive.Stop()

LOOP:
	for len(sent) > 0 {
		select {
		case <-s.quitCh:
			return
		case <-notActive.C:
			s.Warnf("Did not receive all stream info results for %q", acc)
			for sName := range sent {
				missingNames = append(missingNames, sName)
			}
			break LOOP
		case si := <-rc:
			consCount := sent[si.Config.Name]
			if consCount > 0 {
				si.State.Consumers = consCount
			}
			delete(sent, si.Config.Name)
			if si.OfflineReason == _EMPTY_ {
				resp.Streams = append(resp.Streams, &si.StreamInfo)
			} else if _, ok := resp.Offline[si.Config.Name]; !ok {
				if resp.Offline == nil {
					resp.Offline = make(map[string]string, 1)
				}
				resp.Offline[si.Config.Name] = si.OfflineReason
				missingNames = append(missingNames, si.Config.Name)
			}
		}
	}

	// Needs to be sorted as well.
	if len(resp.Streams) > 1 {
		slices.SortFunc(resp.Streams, func(i, j *StreamInfo) int { return cmp.Compare(i.Config.Name, j.Config.Name) })
	}

	resp.Total = scnt
	resp.Limit = JSApiListLimit
	resp.Offset = offset
	resp.Missing = missingNames
	s.sendAPIResponse(ci, acc, subject, reply, string(rmsg), s.jsonResponse(resp))
}

// This will do a scatter and gather operation for all consumers for this stream and account.
// This will be running in a separate Go routine.
func (s *Server) jsClusteredConsumerListRequest(acc *Account, ci *ClientInfo, offset int, stream, subject, reply string, rmsg []byte) {
	defer s.grWG.Done()

	js, cc := s.getJetStreamCluster()
	if js == nil || cc == nil {
		return
	}

	js.mu.RLock()

	var consumers []*consumerAssignment
	if sas := cc.streams[acc.Name]; sas != nil {
		if sa := sas[stream]; sa != nil {
			// Copy over since we need to sort etc.
			for _, ca := range sa.consumers {
				consumers = append(consumers, ca)
			}
		}
	}
	// Needs to be sorted.
	if len(consumers) > 1 {
		slices.SortFunc(consumers, func(i, j *consumerAssignment) int { return cmp.Compare(i.Config.Name, j.Config.Name) })
	}

	ocnt := len(consumers)
	if offset < 0 {
		offset = 0
	}
	if offset > ocnt {
		offset = ocnt
	}
	if offset > 0 {
		consumers = consumers[offset:]
	}
	if len(consumers) > JSApiListLimit {
		consumers = consumers[:JSApiListLimit]
	}

	// Send out our requests here.
	var resp = JSApiConsumerListResponse{
		ApiResponse: ApiResponse{Type: JSApiConsumerListResponseType},
		Consumers:   []*ConsumerInfo{},
	}

	js.mu.RUnlock()

	if len(consumers) == 0 {
		resp.Limit = JSApiListLimit
		resp.Offset = offset
		s.sendAPIResponse(ci, acc, subject, reply, string(rmsg), s.jsonResponse(resp))
		return
	}

	// Create an inbox for our responses and send out requests.
	s.mu.Lock()
	inbox := s.newRespInbox()
	rc := make(chan *consumerInfoClusterResponse, len(consumers))

	// Store our handler.
	s.sys.replies[inbox] = func(sub *subscription, _ *client, _ *Account, subject, _ string, msg []byte) {
		var ci consumerInfoClusterResponse
		if err := json.Unmarshal(msg, &ci); err != nil {
			s.Warnf("Error unmarshaling clustered consumer info response:%v", err)
			return
		}
		select {
		case rc <- &ci:
		default:
			s.Warnf("Failed placing consumer info result on internal chan")
		}
	}
	s.mu.Unlock()

	// Cleanup after.
	defer func() {
		s.mu.Lock()
		if s.sys != nil && s.sys.replies != nil {
			delete(s.sys.replies, inbox)
		}
		s.mu.Unlock()
	}()

	var missingNames []string
	sent := map[string]struct{}{}

	// Send out our requests here.
	js.mu.RLock()
	for _, ca := range consumers {
		if s.allPeersOffline(ca.Group) {
			// Place offline onto our results by hand here.
			ci := &ConsumerInfo{
				Config:    ca.Config,
				Created:   ca.Created,
				Cluster:   js.offlineClusterInfo(ca.Group),
				TimeStamp: time.Now().UTC(),
			}
			resp.Consumers = append(resp.Consumers, ci)
			missingNames = append(missingNames, ca.Name)
		} else {
			isubj := fmt.Sprintf(clusterConsumerInfoT, ca.Client.serviceAccount(), stream, ca.Name)
			s.sendInternalMsgLocked(isubj, inbox, nil, nil)
			sent[ca.Name] = struct{}{}
		}
	}
	// Don't hold lock.
	js.mu.RUnlock()

	const timeout = 4 * time.Second
	notActive := time.NewTimer(timeout)
	defer notActive.Stop()

LOOP:
	for len(sent) > 0 {
		select {
		case <-s.quitCh:
			return
		case <-notActive.C:
			s.Warnf("Did not receive all consumer info results for '%s > %s'", acc, stream)
			for cName := range sent {
				missingNames = append(missingNames, cName)
			}
			break LOOP
		case ci := <-rc:
			delete(sent, ci.Name)
			if ci.OfflineReason == _EMPTY_ {
				resp.Consumers = append(resp.Consumers, &ci.ConsumerInfo)
			} else if _, ok := resp.Offline[ci.Name]; !ok {
				if resp.Offline == nil {
					resp.Offline = make(map[string]string, 1)
				}
				resp.Offline[ci.Name] = ci.OfflineReason
				missingNames = append(missingNames, ci.Name)
			}
		}
	}

	// Needs to be sorted as well.
	if len(resp.Consumers) > 1 {
		slices.SortFunc(resp.Consumers, func(i, j *ConsumerInfo) int { return cmp.Compare(i.Name, j.Name) })
	}

	resp.Total = ocnt
	resp.Limit = JSApiListLimit
	resp.Offset = offset
	resp.Missing = missingNames
	s.sendAPIResponse(ci, acc, subject, reply, string(rmsg), s.jsonResponse(resp))
}

func encodeStreamPurge(sp *streamPurge) []byte {
	var bb bytes.Buffer
	bb.WriteByte(byte(purgeStreamOp))
	json.NewEncoder(&bb).Encode(sp)
	return bb.Bytes()
}

func decodeStreamPurge(buf []byte) (*streamPurge, error) {
	var sp streamPurge
	err := json.Unmarshal(buf, &sp)
	return &sp, err
}

func (s *Server) jsClusteredConsumerDeleteRequest(ci *ClientInfo, acc *Account, stream, consumer, subject, reply string, rmsg []byte) {
	js, cc := s.getJetStreamCluster()
	if js == nil || cc == nil {
		return
	}

	js.mu.Lock()
	defer js.mu.Unlock()

	if cc.meta == nil {
		return
	}

	var resp = JSApiConsumerDeleteResponse{ApiResponse: ApiResponse{Type: JSApiConsumerDeleteResponseType}}

	sa := js.streamAssignmentOrInflight(acc.Name, stream)
	if sa == nil {
		resp.Error = NewJSStreamNotFoundError()
		s.sendAPIErrResponse(ci, acc, subject, reply, string(rmsg), s.jsonResponse(&resp))
		return
	}
	oca := js.consumerAssignmentOrInflight(acc.Name, stream, consumer)
	if oca == nil {
		resp.Error = NewJSConsumerNotFoundError()
		s.sendAPIErrResponse(ci, acc, subject, reply, string(rmsg), s.jsonResponse(&resp))
		return
	}
	ca := &consumerAssignment{Group: oca.Group, Stream: stream, Name: consumer, Config: oca.Config, Subject: subject, Reply: reply, Client: ci, Created: oca.Created}
	if err := cc.meta.Propose(cc.term, encodeDeleteConsumerAssignment(ca)); err != nil {
		return
	}
	cc.trackInflightConsumerProposal(acc.Name, stream, ca, true)
}

func encodeMsgDelete(md *streamMsgDelete) []byte {
	var bb bytes.Buffer
	bb.WriteByte(byte(deleteMsgOp))
	json.NewEncoder(&bb).Encode(md)
	return bb.Bytes()
}

func decodeMsgDelete(buf []byte) (*streamMsgDelete, error) {
	var md streamMsgDelete
	err := json.Unmarshal(buf, &md)
	return &md, err
}

func (s *Server) jsClusteredMsgDeleteRequest(ci *ClientInfo, acc *Account, mset *stream, stream, subject, reply string, req *JSApiMsgDeleteRequest, rmsg []byte) {
	js, cc := s.getJetStreamCluster()
	if js == nil || cc == nil {
		return
	}

	js.mu.Lock()
	sa := js.streamAssignment(acc.Name, stream)
	if sa == nil {
		s.Debugf("Message delete failed, could not locate stream '%s > %s'", acc.Name, stream)
		js.mu.Unlock()
		return
	}

	// Check for single replica items.
	if n := sa.Group.node; n != nil {
		md := encodeMsgDelete(&streamMsgDelete{Seq: req.Seq, NoErase: req.NoErase, Stream: stream, Subject: subject, Reply: reply, Client: ci})
		js.mu.Unlock()
		mset.mu.RLock()
		term := mset.term
		mset.mu.RUnlock()
		n.Propose(term, md)
		return
	}
	js.mu.Unlock()

	if mset == nil {
		return
	}

	var err error
	var removed bool
	if req.NoErase {
		removed, err = mset.removeMsg(req.Seq)
	} else {
		removed, err = mset.eraseMsg(req.Seq)
	}
	var resp = JSApiMsgDeleteResponse{ApiResponse: ApiResponse{Type: JSApiMsgDeleteResponseType}}
	if err != nil {
		resp.Error = NewJSStreamMsgDeleteFailedError(err, Unless(err))
	} else if !removed {
		resp.Error = NewJSSequenceNotFoundError(req.Seq)
	} else {
		resp.Success = true
	}
	s.sendAPIResponse(ci, acc, subject, reply, string(rmsg), s.jsonResponse(resp))
}

func encodeAddStreamAssignment(sa *streamAssignment) []byte {
	csa := sa.clone()
	csa.Client = csa.Client.forProposal()
	csa.ConfigJSON, _ = json.Marshal(sa.Config)
	var bb bytes.Buffer
	bb.WriteByte(byte(assignStreamOp))
	json.NewEncoder(&bb).Encode(csa)
	return bb.Bytes()
}

func encodeUpdateStreamAssignment(sa *streamAssignment) []byte {
	csa := sa.clone()
	csa.Client = csa.Client.forProposal()
	csa.ConfigJSON, _ = json.Marshal(sa.Config)
	var bb bytes.Buffer
	bb.WriteByte(byte(updateStreamOp))
	json.NewEncoder(&bb).Encode(csa)
	return bb.Bytes()
}

func encodeDeleteStreamAssignment(sa *streamAssignment) []byte {
	csa := sa.clone()
	csa.Client = csa.Client.forProposal()
	csa.ConfigJSON, _ = json.Marshal(sa.Config)
	var bb bytes.Buffer
	bb.WriteByte(byte(removeStreamOp))
	json.NewEncoder(&bb).Encode(csa)
	return bb.Bytes()
}

func decodeStreamAssignment(s *Server, buf []byte) (*streamAssignment, error) {
	var sa streamAssignment
	if err := json.Unmarshal(buf, &sa); err != nil {
		return nil, err
	}
	if err := decodeStreamAssignmentConfig(s, &sa); err != nil {
		return nil, err
	}
	return &sa, nil
}

func decodeStreamAssignmentConfig(s *Server, sa *streamAssignment) error {
	var unsupported bool
	var cfg StreamConfig
	var err error
	decoder := json.NewDecoder(bytes.NewReader(sa.ConfigJSON))
	decoder.DisallowUnknownFields()
	if err = decoder.Decode(&cfg); err != nil {
		unsupported = true
		cfg = StreamConfig{}
		if err2 := json.Unmarshal(sa.ConfigJSON, &cfg); err2 != nil {
			return err2
		}
	}
	sa.Config = &cfg
	fixCfgMirrorWithDedupWindow(sa.Config)

	if unsupported || (sa.Config != nil && !supportsRequiredApiLevel(sa.Config.Metadata)) {
		sa.unsupported = newUnsupportedStreamAssignment(s, sa, err)
	}
	return nil
}

func encodeDeleteRange(dr *DeleteRange) []byte {
	var bb bytes.Buffer
	bb.WriteByte(byte(deleteRangeOp))
	json.NewEncoder(&bb).Encode(dr)
	return bb.Bytes()
}

func decodeDeleteRange(buf []byte) (*DeleteRange, error) {
	var dr DeleteRange
	err := json.Unmarshal(buf, &dr)
	if err != nil {
		return nil, err
	}
	return &dr, err
}

// createGroupForConsumer will create a new group from same peer set as the stream.
func (cc *jetStreamCluster) createGroupForConsumer(cfg *ConsumerConfig, sa *streamAssignment) (*raftGroup, *selectPeerError) {
	if len(sa.Group.Peers) == 0 || cfg.Replicas > len(sa.Group.Peers) {
		return nil, &selectPeerError{misc: true}
	}

	replicas := cfg.replicas(sa.Config)
	peers := copyStrings(sa.Group.Peers)
	var _ss [5]string
	active := _ss[:0]

	// Calculate all active peers.
	for _, peer := range peers {
		if sir, ok := cc.s.nodeToInfo.Load(peer); ok && sir != nil {
			if !sir.(nodeInfo).offline {
				active = append(active, peer)
			}
		}
	}
	if quorum := replicas/2 + 1; quorum > len(active) {
		// Not enough active to satisfy the request.
		return nil, &selectPeerError{offline: true}
	}

	// If we want less then our parent stream, select from active.
	if replicas > 0 && replicas < len(peers) {
		// Pedantic in case stream is say R5 and consumer is R3 and 3 or more offline, etc.
		if len(active) < replicas {
			return nil, &selectPeerError{offline: true}
		}
		// First shuffle the active peers and then select to account for replica = 1.
		rand.Shuffle(len(active), func(i, j int) { active[i], active[j] = active[j], active[i] })
		// Prefer caught up peers, since lagging ones would respond late.
		n := 0
		for i, peer := range active {
			if cc.meta.IsFollowerCaughtUp(peer) {
				active[i], active[n] = active[n], active[i]
				n++
			}
		}
		peers = active[:replicas]
	}
	storage := sa.Config.Storage
	if cfg.MemoryStorage {
		storage = MemoryStorage
	}
	return &raftGroup{Name: groupNameForConsumer(peers, storage), Storage: storage, Peers: peers}, nil
}

// jsClusteredConsumerRequest is first point of entry to create a consumer in clustered mode.
func (s *Server) jsClusteredConsumerRequest(ci *ClientInfo, acc *Account, subject, reply string, rmsg []byte, stream string, cfg *ConsumerConfig, action ConsumerAction, pedantic bool) {
	js, cc := s.getJetStreamCluster()
	if js == nil || cc == nil {
		return
	}

	// If the consumer is a direct sourcing consumer, we need to "upgrade" it to be durable without AckNone.
	// We only get here if the stream is not Limits-based.
	if cfg.Direct && cfg.Sourcing && cfg.Name != _EMPTY_ {
		cfg.Direct = false
		cfg.Durable = cfg.Name
		cfg.AckPolicy = AckFlowControl
		cfg.AckWait = 0
		cfg.MaxDeliver = 0
		cfg.InactiveThreshold = 0
	}

	var resp = JSApiConsumerCreateResponse{ApiResponse: ApiResponse{Type: JSApiConsumerCreateResponseType}}

	streamCfg, ok := js.clusterStreamConfig(acc.Name, stream)
	if !ok {
		resp.Error = NewJSStreamNotFoundError()
		s.sendAPIErrResponse(ci, acc, subject, reply, string(rmsg), s.jsonResponse(&resp))
		return
	}
	selectedReplicas := cfg.replicas(&streamCfg)
	selectedLimits, selectedTier, _, apiErr := acc.selectLimits(selectedReplicas)
	if apiErr != nil {
		resp.Error = apiErr
		s.sendAPIErrResponse(ci, acc, subject, reply, string(rmsg), s.jsonResponse(&resp))
		return
	}
	srvLim := &s.getOpts().JetStreamLimits
	// Make sure we have sane defaults
	if err := setConsumerConfigDefaults(cfg, &streamCfg, srvLim, selectedLimits, pedantic); err != nil {
		resp.Error = err
		s.sendAPIErrResponse(ci, acc, subject, reply, string(rmsg), s.jsonResponse(&resp))
		return
	}

	if err := checkConsumerCfg(cfg, srvLim, &streamCfg, acc, selectedLimits, false); err != nil {
		resp.Error = err
		s.sendAPIErrResponse(ci, acc, subject, reply, string(rmsg), s.jsonResponse(&resp))
		return
	}

	js.mu.Lock()
	defer js.mu.Unlock()

	if cc.meta == nil {
		return
	}

	// Lookup the stream assignment.
	sa := js.streamAssignmentOrInflight(acc.Name, stream)
	if sa == nil {
		resp.Error = NewJSStreamNotFoundError()
		s.sendAPIErrResponse(ci, acc, subject, reply, string(rmsg), s.jsonResponse(&resp))
		return
	}

	// Was a consumer name provided?
	var oname string
	if isDurableConsumer(cfg) || cfg.Name != _EMPTY_ {
		if cfg.Name != _EMPTY_ {
			oname = cfg.Name
		} else {
			oname = cfg.Durable
		}
	}

	// Check for max consumers here to short circuit if possible.
	// Start with limit on a stream, but if one is defined at the level of the account
	// and is lower, use that limit.
	if action == ActionCreate || action == ActionCreateOrUpdate {
		// The stream limit caps every consumer of the stream. The account limit
		// caps only the consumers of the selected tier, so the two need separate
		// counts.
		streamMaxc := sa.Config.MaxConsumers
		tierMaxc := selectedLimits.MaxConsumers
		if streamMaxc > 0 || tierMaxc > 0 {
			// If the consumer name is specified and we think it already exists, then
			// we're likely updating an existing consumer, so don't count it. Otherwise
			// we will incorrectly return NewJSMaximumConsumersLimitError for an update.
			if oname == _EMPTY_ || js.consumerAssignmentOrInflight(acc.Name, stream, oname) == nil {
				// Don't count direct/sourcing consumers.
				var total, tierTotal int
				for ca := range js.consumerAssignmentsOrInflightSeq(acc.Name, stream) {
					if ca.unsupported != nil {
						continue
					}
					if ca.Config != nil && !ca.Config.Direct && !ca.Config.Sourcing {
						total++
						if selectedTier == _EMPTY_ || isSameTier(ca.Config.replicas(&streamCfg), selectedReplicas) {
							tierTotal++
						}
					}
				}
				if (streamMaxc > 0 && total >= streamMaxc) || (tierMaxc > 0 && tierTotal >= tierMaxc) {
					resp.Error = NewJSMaximumConsumersLimitError()
					s.sendAPIErrResponse(ci, acc, subject, reply, string(rmsg), s.jsonResponse(&resp))
					return
				}
			}
		}
	}

	// Also short circuit if DeliverLastPerSubject is set with no FilterSubject.
	if cfg.DeliverPolicy == DeliverLastPerSubject {
		if cfg.FilterSubject == _EMPTY_ && len(cfg.FilterSubjects) == 0 {
			resp.Error = NewJSConsumerInvalidPolicyError(fmt.Errorf("consumer delivery policy is deliver last per subject, but FilterSubject is not set"))
			s.sendAPIErrResponse(ci, acc, subject, reply, string(rmsg), s.jsonResponse(&resp))
			return
		}
	}

	// Setup proper default for ack wait if we are in explicit ack mode.
	if cfg.AckWait == 0 && (cfg.AckPolicy == AckExplicit || cfg.AckPolicy == AckAll) {
		cfg.AckWait = JsAckWaitDefault
	}
	// Setup default of -1, meaning no limit for MaxDeliver.
	if cfg.MaxDeliver == 0 {
		cfg.MaxDeliver = -1
	}
	// Set proper default for max ack pending if we are ack explicit and none has been set.
	if cfg.AckPolicy == AckExplicit && cfg.MaxAckPending == 0 {
		cfg.MaxAckPending = JsDefaultMaxAckPending
	}

	if cfg.PriorityPolicy == PriorityPinnedClient && cfg.PinnedTTL == 0 {
		cfg.PinnedTTL = JsDefaultPinnedTTL
	}

	var ca *consumerAssignment
	// See if we have an existing one already under same durable name or
	// if name was set by the user.
	if oname != _EMPTY_ {
		if ca = js.consumerAssignmentOrInflight(acc.Name, stream, oname); ca != nil {
			// Provided config might miss metadata, copy from existing config.
			copyConsumerMetadata(cfg, ca.Config)

			// If a durable sourcing consumer is used, we need to reset the deliver policy.
			if cfg.Sourcing && cfg.Durable != _EMPTY_ {
				cfg.DeliverPolicy = ca.Config.DeliverPolicy
				cfg.OptStartSeq = ca.Config.OptStartSeq
				cfg.OptStartTime = ca.Config.OptStartTime
			}

			if action == ActionCreate && !reflect.DeepEqual(cfg, ca.Config) {
				resp.Error = NewJSConsumerAlreadyExistsError()
				s.sendAPIErrResponse(ci, acc, subject, reply, string(rmsg), s.jsonResponse(&resp))
				return
			}
			// Do quick sanity check on new cfg to prevent here if possible.
			if err := acc.checkNewConsumerConfig(ca.Config, cfg); err != nil {
				resp.Error = NewJSConsumerCreateError(err, Unless(err))
				s.sendAPIErrResponse(ci, acc, subject, reply, string(rmsg), s.jsonResponse(&resp))
				return
			}
			// Don't allow updating if all peers are offline.
			if s.allPeersOffline(ca.Group) {
				resp.Error = NewJSConsumerOfflineError()
				s.sendDelayedAPIErrResponse(ci, acc, subject, reply, string(rmsg), s.jsonResponse(&resp), nil, errRespDelay)
				return
			}
		} else {
			// Initialize/update asset version metadata.
			// First time creating this consumer, or updating.
			setStaticConsumerMetadata(cfg)
		}
	}

	// Initialize/update asset version metadata.
	// But only if we're not creating, should only update it the first time
	// to be idempotent with versions where there's no versioning metadata.
	if action != ActionCreate {
		setStaticConsumerMetadata(cfg)
	}

	// If this is new consumer.
	if ca == nil {
		if action == ActionUpdate {
			resp.Error = NewJSConsumerDoesNotExistError()
			s.sendAPIErrResponse(ci, acc, subject, reply, string(rmsg), s.jsonResponse(&resp))
			return
		}
		rg, err := cc.createGroupForConsumer(cfg, sa)
		if err != nil {
			resp.Error = NewJSInsufficientResourcesError()
			s.sendAPIErrResponse(ci, acc, subject, reply, string(rmsg), s.jsonResponse(&resp))
			return
		}
		// Pick a preferred leader.
		rg.setPreferred(s)

		// Inherit cluster from stream.
		rg.Cluster = sa.Group.Cluster

		// We need to set the ephemeral here before replicating.
		if !isDurableConsumer(cfg) {
			if cfg.Name != _EMPTY_ {
				oname = cfg.Name
			} else {
				// Make sure name is unique.
				for {
					oname = createConsumerName()
					if js.consumerAssignmentOrInflight(acc.Name, stream, oname) != nil {
						continue
					}
					break
				}
			}
		}
		if len(rg.Peers) > 1 {
			if maxHaAssets := s.getOpts().JetStreamLimits.MaxHAAssets; maxHaAssets != 0 {
				for _, peer := range rg.Peers {
					if ni, ok := s.nodeToInfo.Load(peer); ok {
						ni := ni.(nodeInfo)
						if stats := ni.stats; stats != nil && stats.HAAssets > maxHaAssets {
							resp.Error = NewJSInsufficientResourcesError()
							s.sendAPIErrResponse(ci, acc, subject, reply, string(rmsg), s.jsonResponse(&resp))
							s.Warnf("%s@%s (HA Asset Count: %d) exceeds max ha asset limit of %d"+
								" for (durable) consumer %s placement on stream %s",
								ni.name, ni.cluster, ni.stats.HAAssets, maxHaAssets, oname, stream)
							return
						}
					}
				}
			}
		}

		// Check if we are work queue policy.
		// We will do pre-checks here to avoid thrashing meta layer.
		if sa.Config.Retention == WorkQueuePolicy && !cfg.Direct && !cfg.Sourcing {
			if cfg.AckPolicy != AckExplicit && cfg.AckPolicy != AckFlowControl {
				resp.Error = NewJSConsumerWQRequiresExplicitAckError()
				s.sendAPIErrResponse(ci, acc, subject, reply, string(rmsg), s.jsonResponse(&resp))
				return
			}
			if cfg.DeliverPolicy != DeliverAll {
				resp.Error = NewJSConsumerWQConsumerNotDeliverAllError()
				s.sendAPIErrResponse(ci, acc, subject, reply, string(rmsg), s.jsonResponse(&resp))
				return
			}
			subjects := gatherSubjectFilters(cfg.FilterSubject, cfg.FilterSubjects)
			for oca := range js.consumerAssignmentsOrInflightSeq(acc.Name, stream) {
				if oca.Name == oname || oca.Config.Direct || oca.Config.Sourcing {
					continue
				}
				if len(subjects) == 0 {
					resp.Error = NewJSConsumerWQMultipleUnfilteredError()
					s.sendAPIErrResponse(ci, acc, subject, reply, string(rmsg), s.jsonResponse(&resp))
					return
				}
				for _, psubj := range gatherSubjectFilters(oca.Config.FilterSubject, oca.Config.FilterSubjects) {
					for _, subj := range subjects {
						if SubjectsCollide(subj, psubj) {
							resp.Error = NewJSConsumerWQConsumerNotUniqueError()
							s.sendAPIErrResponse(ci, acc, subject, reply, string(rmsg), s.jsonResponse(&resp))
							return
						}
					}
				}
			}
		}

		ca = &consumerAssignment{
			Group:   rg,
			Stream:  stream,
			Name:    oname,
			Config:  cfg,
			Subject: subject,
			Reply:   reply,
			Client:  ci,
			Created: time.Now().UTC(),
		}
	} else {
		// If the consumer already exists then don't allow updating the PauseUntil, just set
		// it back to whatever the current configured value is.
		cfg.PauseUntil = ca.Config.PauseUntil

		nca := ca.copyGroup()

		// Reset notion of scaling up, if this was done in a previous update.
		nca.Group.ScaleUp = false

		rBefore := nca.Config.replicas(sa.Config)
		rAfter := cfg.replicas(sa.Config)

		// Not acting on desired state, so the consumer is still at its own peer set.
		if nca.Group.Desired != nil {
			rBefore = len(nca.Group.Peers)
			nca.Group.Desired = nil
		}

		var curLeader string
		if rBefore != rAfter {
			// We are modifying nodes here. We want to do our best to preserve the current leader.
			// We have support now from above that guarantees we are in our own Go routine, so can
			// ask for stream info from the stream leader to make sure we keep the leader in the new list.
			if !s.allPeersOffline(ca.Group) {
				// Need to release js lock.
				js.mu.Unlock()
				if ci, err := sysRequest[ConsumerInfo](s, clusterConsumerInfoT, ci.serviceAccount(), sa.Config.Name, oname); err != nil {
					s.Warnf("Did not receive consumer info results for '%s > %s > %s' due to: %s", acc, sa.Config.Name, oname, err)
				} else if ci != nil {
					if cl := ci.Cluster; cl != nil && cl.Leader != _EMPTY_ {
						curLeader = getHash(cl.Leader)
					}
				}
				// Re-acquire here.
				js.mu.Lock()
			}
		}

		if rBefore < rAfter {
			newPeerSet := nca.Group.Peers
			// Scale up by adding new members from the stream peer set that are not yet in the consumer peer set.
			streamPeerSet := copyStrings(sa.Group.Peers)

			// Respond with error when there is a config mismatch between the intended config and expected peer size.
			if len(streamPeerSet) < rAfter {
				resp.Error = NewJSConsumerReplicasExceedsStreamError()
				s.sendAPIErrResponse(ci, acc, subject, reply, string(rmsg), s.jsonResponse(&resp))
				return
			}
			rand.Shuffle(rAfter, func(i, j int) { streamPeerSet[i], streamPeerSet[j] = streamPeerSet[j], streamPeerSet[i] })
			for _, p := range streamPeerSet {
				found := false
				for _, sp := range newPeerSet {
					if sp == p {
						found = true
						break
					}
				}
				if !found {
					newPeerSet = append(newPeerSet, p)
					if len(newPeerSet) == rAfter {
						break
					}
				}
			}
			// Single nodes are not recorded by the NRG layer so we can rename.
			if rBefore == 1 {
				nca.Group.Name = groupNameForConsumer(newPeerSet, nca.Group.Storage)
			}
			nca.Group.Peers = newPeerSet
			nca.Group.Preferred = curLeader
			nca.Group.ScaleUp = true
		} else if rBefore > rAfter {
			// Mark the current leader as preferred, it will be kept in the new peer set.
			nca.Group.Preferred = curLeader
			nca.Group.Peers = s.selectScaleDownPeers(nca.Group.Peers, curLeader, rAfter)
			// Single nodes are not recorded by the NRG layer so we can rename.
			// MUST do this, otherwise a scaleup afterward could potentially lead to inconsistencies.
			if len(nca.Group.Peers) == 1 {
				nca.Group.Name = groupNameForConsumer(nca.Group.Peers, nca.Group.Storage)
			}
		}

		// Update config and client info on copy of existing.
		nca.Config = cfg
		nca.Client = ci
		nca.Subject = subject
		nca.Reply = reply
		ca = nca
	}

	// Do formal proposal.
	if err := cc.meta.Propose(cc.term, encodeAddConsumerAssignment(ca)); err != nil {
		return
	}
	cc.trackInflightConsumerProposal(acc.Name, stream, ca, false)
}

func encodeAddConsumerAssignment(ca *consumerAssignment) []byte {
	cca := ca.clone()
	cca.Client = cca.Client.forProposal()
	cca.ConfigJSON, _ = json.Marshal(ca.Config)
	var bb bytes.Buffer
	bb.WriteByte(byte(assignConsumerOp))
	json.NewEncoder(&bb).Encode(cca)
	return bb.Bytes()
}

func encodeDeleteConsumerAssignment(ca *consumerAssignment) []byte {
	cca := ca.clone()
	cca.Client = cca.Client.forProposal()
	cca.ConfigJSON, _ = json.Marshal(ca.Config)
	var bb bytes.Buffer
	bb.WriteByte(byte(removeConsumerOp))
	json.NewEncoder(&bb).Encode(cca)
	return bb.Bytes()
}

func decodeConsumerAssignment(buf []byte) (*consumerAssignment, error) {
	var ca consumerAssignment
	if err := json.Unmarshal(buf, &ca); err != nil {
		return nil, err
	}
	if err := decodeConsumerAssignmentConfig(&ca); err != nil {
		return nil, err
	}
	return &ca, nil
}

func decodeConsumerAssignmentConfig(ca *consumerAssignment) error {
	var unsupported bool
	var cfg ConsumerConfig
	var err error
	decoder := json.NewDecoder(bytes.NewReader(ca.ConfigJSON))
	decoder.DisallowUnknownFields()
	if err = decoder.Decode(&cfg); err != nil {
		unsupported = true
		cfg = ConsumerConfig{}
		if err2 := json.Unmarshal(ca.ConfigJSON, &cfg); err2 != nil {
			return err2
		}
	}
	ca.Config = &cfg
	if unsupported || (ca.Config != nil && !supportsRequiredApiLevel(ca.Config.Metadata)) {
		ca.unsupported = newUnsupportedConsumerAssignment(ca, err)
	}
	return nil
}

func encodeAddConsumerAssignmentCompressed(ca *consumerAssignment) []byte {
	cca := ca.clone()
	cca.Client = cca.Client.forProposal()
	cca.ConfigJSON, _ = json.Marshal(ca.Config)
	var bb bytes.Buffer
	bb.WriteByte(byte(assignCompressedConsumerOp))
	s2e := s2.NewWriter(&bb)
	json.NewEncoder(s2e).Encode(cca)
	s2e.Close()
	return bb.Bytes()
}

func decodeConsumerAssignmentCompressed(buf []byte) (*consumerAssignment, error) {
	var ca consumerAssignment
	bb := bytes.NewBuffer(buf)
	s2d := s2.NewReader(bb)
	decoder := json.NewDecoder(s2d)
	if err := decoder.Decode(&ca); err != nil {
		return nil, err
	}
	if err := decodeConsumerAssignmentConfig(&ca); err != nil {
		return nil, err
	}
	return &ca, nil
}

var errBadStreamMsg = errors.New("jetstream cluster bad replicated stream msg")

func decodeStreamMsg(buf []byte) (subject, reply string, hdr, msg []byte, lseq uint64, ts int64, sourced bool, err error) {
	var le = binary.LittleEndian
	if len(buf) < 26 {
		return _EMPTY_, _EMPTY_, nil, nil, 0, 0, false, errBadStreamMsg
	}
	lseq = le.Uint64(buf)
	buf = buf[8:]
	ts = int64(le.Uint64(buf))
	buf = buf[8:]
	sl := int(le.Uint16(buf))
	buf = buf[2:]
	if len(buf) < sl {
		return _EMPTY_, _EMPTY_, nil, nil, 0, 0, false, errBadStreamMsg
	}
	subject = string(buf[:sl])
	buf = buf[sl:]
	if len(buf) < 2 {
		return _EMPTY_, _EMPTY_, nil, nil, 0, 0, false, errBadStreamMsg
	}
	rl := int(le.Uint16(buf))
	buf = buf[2:]
	if len(buf) < rl {
		return _EMPTY_, _EMPTY_, nil, nil, 0, 0, false, errBadStreamMsg
	}
	reply = string(buf[:rl])
	buf = buf[rl:]
	if len(buf) < 2 {
		return _EMPTY_, _EMPTY_, nil, nil, 0, 0, false, errBadStreamMsg
	}
	hl := int(le.Uint16(buf))
	buf = buf[2:]
	if len(buf) < hl {
		return _EMPTY_, _EMPTY_, nil, nil, 0, 0, false, errBadStreamMsg
	}
	if hdr = buf[:hl]; len(hdr) == 0 {
		hdr = nil
	}
	buf = buf[hl:]
	if len(buf) < 4 {
		return _EMPTY_, _EMPTY_, nil, nil, 0, 0, false, errBadStreamMsg
	}
	ml := int(le.Uint32(buf))
	buf = buf[4:]
	// ml is read as a uint32 but held in an int; on 32-bit builds a length with
	// the high bit set becomes negative, which slips past len(buf) < ml and then
	// panics on buf[:ml]. Reject a negative length so the bound holds everywhere.
	if ml < 0 || len(buf) < ml {
		return _EMPTY_, _EMPTY_, nil, nil, 0, 0, false, errBadStreamMsg
	}
	if msg = buf[:ml]; len(msg) == 0 {
		msg = nil
	}
	buf = buf[ml:]
	if len(buf) > 0 {
		flags, _ := binary.Uvarint(buf)
		sourced = flags&msgFlagFromSourceOrMirror != 0
	}
	return subject, reply, hdr, msg, lseq, ts, sourced, nil
}

func decodeBatchMsg(buf []byte) (batchId string, batchSeq uint64, op entryOp, mbuf []byte, err error) {
	var le = binary.LittleEndian
	if len(buf) < 2 {
		return _EMPTY_, 0, 0, nil, errBadStreamMsg
	}
	bl := int(le.Uint16(buf))
	buf = buf[2:]
	if len(buf) < bl {
		return _EMPTY_, 0, 0, nil, errBadStreamMsg
	}
	batchId = string(buf[:bl])
	buf = buf[bl:]
	var n int
	batchSeq, n = binary.Uvarint(buf)
	if n <= 0 {
		return _EMPTY_, 0, 0, nil, errBadStreamMsg
	}
	buf = buf[n:]
	if len(buf) < 1 {
		return _EMPTY_, 0, 0, nil, errBadStreamMsg
	}
	op = entryOp(buf[0])
	mbuf = buf[1:]
	return batchId, batchSeq, op, mbuf, nil
}

// Flags for encodeStreamMsg/decodeStreamMsg.
const (
	msgFlagFromSourceOrMirror uint64 = 1 << iota
)

func encodeStreamMsg(subject, reply string, hdr, msg []byte, lseq uint64, ts int64, sourced bool) []byte {
	return encodeStreamMsgAllowCompress(subject, reply, hdr, msg, lseq, ts, sourced)
}

func encodeStreamMsgAllowCompress(subject, reply string, hdr, msg []byte, lseq uint64, ts int64, sourced bool) []byte {
	return encodeStreamMsgAllowCompressAndBatch(subject, reply, hdr, msg, lseq, ts, sourced, _EMPTY_, 0, false)
}

// Threshold for compression.
// TODO(dlc) - Eventually make configurable.
const compressThreshold = 8192 // 8k

// If allowed and contents over the threshold we will compress.
func encodeStreamMsgAllowCompressAndBatch(subject, reply string, hdr, msg []byte, lseq uint64, ts int64, sourced bool, batchId string, batchSeq uint64, batchCommit bool) []byte {
	// Clip the subject, reply, header and msgs down. Operate on
	// uint64 lengths to avoid overflowing.
	slen := min(uint64(len(subject)), math.MaxUint16)
	rlen := min(uint64(len(reply)), math.MaxUint16)
	hlen := min(uint64(len(hdr)), math.MaxUint16)
	mlen := min(uint64(len(msg)), math.MaxUint32)
	total := slen + rlen + hlen + mlen

	shouldCompress := total > compressThreshold
	elen := int(1 + 8 + 8 + total)
	elen += (2 + 2 + 2 + 4 + 8) // Encoded lengths, 4bytes, flags are up to 8 bytes

	blen := min(uint64(len(batchId)), math.MaxUint16)
	if batchId != _EMPTY_ {
		elen += int(2 + blen + 8) // length of batchId, batchId itself, batchSeq (up to 8 bytes)
	}

	var flags uint64
	if sourced {
		flags |= msgFlagFromSourceOrMirror
	}

	var le = binary.LittleEndian
	var opIndex int
	buf := make([]byte, 1, elen)
	if batchId != _EMPTY_ {
		if batchCommit {
			buf[0] = byte(batchCommitMsgOp)
		} else {
			buf[0] = byte(batchMsgOp)
		}
		buf = le.AppendUint16(buf, uint16(blen))
		buf = append(buf, batchId[:blen]...)
		buf = binary.AppendUvarint(buf, batchSeq)
		opIndex = len(buf)
		buf = append(buf, byte(streamMsgOp))
	} else {
		buf[opIndex] = byte(streamMsgOp)
	}

	buf = le.AppendUint64(buf, lseq)
	buf = le.AppendUint64(buf, uint64(ts))
	buf = le.AppendUint16(buf, uint16(slen))
	buf = append(buf, subject[:slen]...)
	buf = le.AppendUint16(buf, uint16(rlen))
	buf = append(buf, reply[:rlen]...)
	buf = le.AppendUint16(buf, uint16(hlen))
	buf = append(buf, hdr[:hlen]...)
	buf = le.AppendUint32(buf, uint32(mlen))
	buf = append(buf, msg[:mlen]...)
	buf = binary.AppendUvarint(buf, flags)

	// Check if we should compress.
	if shouldCompress {
		nbuf := make([]byte, s2.MaxEncodedLen(elen))
		if opIndex > 0 {
			copy(nbuf[:opIndex], buf[:opIndex])
		}
		nbuf[opIndex] = byte(compressedStreamMsgOp)
		ebuf := s2.Encode(nbuf[opIndex+1:], buf[opIndex+1:])
		// Only pay the cost of decode on the other side if we compressed.
		// S2 will allow us to try without major penalty for non-compressable data.
		if len(ebuf) < len(buf) {
			buf = nbuf[:len(ebuf)+opIndex+1]
		}
	}

	return buf
}

// Determine if all peers in our set support the binary snapshot.
func (mset *stream) supportsBinarySnapshot() bool {
	mset.mu.RLock()
	defer mset.mu.RUnlock()
	return mset.supportsBinarySnapshotLocked()
}

// Determine if all peers in our set support the binary snapshot.
// Lock should be held.
func (mset *stream) supportsBinarySnapshotLocked() bool {
	s, n := mset.srv, mset.node
	if s == nil || n == nil {
		return false
	}
	// Grab our peers and walk them to make sure we can all support binary stream snapshots.
	id, peers := n.ID(), n.Peers()
	for _, p := range peers {
		if p.ID == id {
			// We know we support ourselves.
			continue
		}
		// Since release 2.10.16 only deny if we know the other node does not support.
		if sir, ok := s.nodeToInfo.Load(p.ID); ok && sir != nil && !sir.(nodeInfo).binarySnapshots {
			return false
		}
	}
	return true
}

// StreamSnapshot is used for snapshotting and out of band catch up in clustered mode.
// Legacy, replace with binary stream snapshots.
type streamSnapshot struct {
	Msgs     uint64   `json:"messages"`
	Bytes    uint64   `json:"bytes"`
	FirstSeq uint64   `json:"first_seq"`
	LastSeq  uint64   `json:"last_seq"`
	Failed   uint64   `json:"clfs"`
	Deleted  []uint64 `json:"deleted,omitempty"`
}

// Grab a snapshot of a stream for clustered mode.
func (mset *stream) stateSnapshot() []byte {
	mset.mu.RLock()
	defer mset.mu.RUnlock()
	return mset.stateSnapshotLocked()
}

// Grab a snapshot of a stream for clustered mode.
// Lock should be held.
func (mset *stream) stateSnapshotLocked() []byte {
	// Decide if we can support the new style of stream snapshots.
	if mset.supportsBinarySnapshotLocked() {
		snap, err := mset.store.EncodedStreamState(mset.getCLFS())
		if err != nil {
			return nil
		}
		return snap
	}

	// Older v1 version with deleted as a sorted []uint64.
	// For a stream with millions or billions of interior deletes, this will be huge.
	// Now that all server versions 2.10.+ support binary snapshots, we should never fall back.
	assert.Unreachable("Legacy JSON stream snapshot used", map[string]any{
		"stream":  mset.cfg.Name,
		"account": mset.acc.Name,
	})

	state := mset.store.State()
	snap := &streamSnapshot{
		Msgs:     state.Msgs,
		Bytes:    state.Bytes,
		FirstSeq: state.FirstSeq,
		LastSeq:  state.LastSeq,
		Failed:   mset.getCLFS(),
		Deleted:  state.Deleted,
	}
	b, _ := json.Marshal(snap)
	return b
}

// To warn when we are getting too far behind from what has been proposed vs what has been committed.
const streamLagWarnThreshold = 10_000

// processClusteredInboundMsg will propose the inbound message to the underlying raft group.
func (mset *stream) processClusteredInboundMsg(subject, reply string, hdr, msg []byte, mt *msgTrace, sourced bool) (retErr error) {
	// For possible error response.
	var response []byte

	mset.mu.RLock()
	canRespond := !mset.cfg.NoAck && len(reply) > 0
	name, stype := mset.cfg.Name, mset.cfg.Storage
	discard, discardNewPer, maxMsgs, maxMsgsPer, maxBytes := mset.cfg.Discard, mset.cfg.DiscardNewPer, mset.cfg.MaxMsgs, mset.cfg.MaxMsgsPer, mset.cfg.MaxBytes
	s, js, jsa, st, r, tierName, outq, node, term := mset.srv, mset.js, mset.jsa, mset.cfg.Storage, mset.cfg.Replicas, mset.tier, mset.outq, mset.node, mset.term
	maxMsgSize, lseq := int(mset.cfg.MaxMsgSize), mset.lseq
	isLeader, isSealed, allowRollup, denyPurge, allowTTL, allowMsgCounter, allowMsgSchedules := mset.isLeader(), mset.cfg.Sealed, mset.cfg.AllowRollup, mset.cfg.DenyPurge, mset.cfg.AllowMsgTTL, mset.cfg.AllowMsgCounter, mset.cfg.AllowMsgSchedules

	// Apply the input subject transform if any
	csubject := subject
	if mset.itr != nil {
		ts, err := mset.itr.Match(csubject)
		if err == nil {
			// no filtering: if the subject doesn't map the source of the transform, don't change it
			csubject = ts
		}
	}
	mset.mu.RUnlock()

	// This should not happen but possible now that we allow scale up, and scale down where this could trigger.
	//
	// We also invoke this in clustering mode for message tracing when not
	// performing message delivery.
	if node == nil || mt.traceOnly() {
		return mset.processJetStreamMsg(subject, reply, hdr, msg, 0, 0, mt, sourced, true)
	}

	// If message tracing (with message delivery), we will need to send the
	// event on exit in case there was an error (if message was not proposed).
	// Otherwise, the event will be sent from processJetStreamMsg when
	// invoked by the leader (from applyStreamEntries).
	if mt != nil {
		defer func() {
			if retErr != nil {
				mt.sendEventFromJetStream(retErr)
			}
		}()
	}

	// Check that we are the leader. This can be false if we have scaled up from an R1 that had inbound queued messages.
	if !isLeader {
		return NewJSClusterNotLeaderError()
	}

	// Bail here if sealed.
	if isSealed {
		var resp = JSPubAckResponse{PubAck: &PubAck{Stream: mset.name()}, Error: NewJSStreamSealedError()}
		b, _ := json.Marshal(resp)
		mset.outq.sendMsg(reply, b)
		return NewJSStreamSealedError()
	}

	// Check here pre-emptively if we have exceeded this server limits.
	if js.limitsExceeded(stype) {
		s.resourcesExceededError(stype)
		if canRespond {
			b, _ := json.Marshal(&JSPubAckResponse{PubAck: &PubAck{Stream: name}, Error: NewJSInsufficientResourcesError()})
			outq.send(newJSPubMsg(reply, _EMPTY_, _EMPTY_, nil, b, nil, 0))
		}
		// Stepdown regardless.
		if node := mset.raftNode(); node != nil {
			node.StepDown()
		}
		return NewJSInsufficientResourcesError()
	}

	// Check here pre-emptively if we have exceeded our account limits.
	if exceeded, err := jsa.wouldExceedLimits(st, tierName, r, csubject, hdr, msg); exceeded {
		if err == nil {
			err = NewJSAccountResourcesExceededError()
		}
		s.RateLimitWarnf("JetStream account limits exceeded for '%s': %s", jsa.acc().GetName(), err.Error())
		if canRespond {
			var resp = &JSPubAckResponse{PubAck: &PubAck{Stream: name}}
			resp.Error = err
			response, _ = json.Marshal(resp)
			outq.send(newJSPubMsg(reply, _EMPTY_, _EMPTY_, nil, response, nil, 0))
		}
		return err
	}

	// Check msgSize if we have a limit set there. Again this works if it goes through but better to be pre-emptive.
	// Subtract to prevent against overflows.
	if maxMsgSize >= 0 && (len(hdr) > maxMsgSize || len(msg) > maxMsgSize-len(hdr)) {
		err := fmt.Errorf("JetStream message size exceeds limits for '%s > %s'", jsa.acc().Name, name)
		s.RateLimitWarnf("%s", err.Error())
		if canRespond {
			var resp = &JSPubAckResponse{PubAck: &PubAck{Stream: name}}
			resp.Error = NewJSStreamMessageExceedsMaximumError()
			response, _ = json.Marshal(resp)
			outq.send(newJSPubMsg(reply, _EMPTY_, _EMPTY_, nil, response, nil, 0))
		}
		return err
	}

	// Proceed with proposing this message.

	// We only use mset.clseq for clustering and in case we run ahead of actual commits.
	// Check if we need to set initial value here
	mset.clMu.Lock()
	if mset.clseq == 0 || mset.clseq < lseq+mset.clfs {
		lseq = recalculateClusteredSeq(mset, true)
	}

	var (
		dseq   uint64
		apiErr *ApiError
		err    error
	)
	diff := &batchStagedDiff{}
	if hdr, msg, dseq, apiErr, err = checkMsgHeadersPreClusteredProposal(diff, mset, csubject, subject, hdr, msg, sourced, name, jsa, allowRollup, denyPurge, allowTTL, allowMsgCounter, allowMsgSchedules, discard, discardNewPer, maxMsgSize, maxMsgs, maxMsgsPer, maxBytes); err != nil {
		mset.clMu.Unlock()
		if err == errMsgIdDuplicate && dseq > 0 {
			var buf [256]byte
			pubAck := append(buf[:0], mset.pubAck...)
			response = append(pubAck, strconv.FormatUint(dseq, 10)...)
			response = append(response, ",\"duplicate\": true}"...)
			outq.sendMsg(reply, response)
			return err
		}
		if canRespond {
			var resp = &JSPubAckResponse{PubAck: &PubAck{Stream: name}}
			resp.Error = apiErr
			response, _ = json.Marshal(resp)
			outq.sendMsg(reply, response)
		}
		return err
	}

	err = commitSingleMsg(diff, mset, subject, reply, hdr, msg, name, jsa, mt, node, term, r, lseq)
	mset.clMu.Unlock()
	return err
}

func (mset *stream) getAndDeleteMsgTrace(lseq uint64) *msgTrace {
	if mset == nil {
		return nil
	}
	mset.clMu.Lock()
	mt, ok := mset.mt[lseq]
	if ok {
		delete(mset.mt, lseq)
	}
	mset.clMu.Unlock()
	return mt
}

// For requesting messages post raft snapshot to catch up streams post server restart.
// Any deleted msgs etc will be handled inline on catchup.
type streamSyncRequest struct {
	Peer           string `json:"peer,omitempty"`
	FirstSeq       uint64 `json:"first_seq"`
	LastSeq        uint64 `json:"last_seq"`
	DeleteRangesOk bool   `json:"delete_ranges"`
	MinApplied     uint64 `json:"min_applied"`
}

// Given a stream state that represents a snapshot, calculate the sync request based on our current state.
// Stream lock must be held.
func (mset *stream) calculateSyncRequest(state *StreamState, snap *StreamReplicatedState, index uint64) *streamSyncRequest {
	// Shouldn't happen, but consequences are pretty bad if we have the lock held and
	// our caller tries to take the lock again on panic defer, as in processSnapshot.
	if state == nil || snap == nil || mset.node == nil {
		return nil
	}
	// Quick check if we are already caught up.
	if state.LastSeq >= snap.LastSeq {
		return nil
	}
	return &streamSyncRequest{FirstSeq: state.LastSeq + 1, LastSeq: snap.LastSeq, Peer: mset.node.ID(), DeleteRangesOk: true, MinApplied: index}
}

// processSnapshotDeletes will update our current store based on the snapshot
// but only processing deletes and new FirstSeq / purges.
func (mset *stream) processSnapshotDeletes(snap *StreamReplicatedState) error {
	mset.mu.Lock()
	var state StreamState
	mset.store.FastState(&state)
	// Always adjust if FirstSeq has moved beyond our state.
	var didReset bool
	if snap.FirstSeq > state.FirstSeq {
		if _, err := mset.store.Compact(snap.FirstSeq); err != nil {
			mset.mu.Unlock()
			return err
		}
		mset.store.FastState(&state)
		mset.lseq = state.LastSeq
		mset.clearAllPreAcksBelowFloor(state.FirstSeq)
		didReset = true
	}
	s := mset.srv
	mset.mu.Unlock()

	if didReset {
		s.Warnf("Catchup for stream '%s > %s' resetting first sequence: %d on catchup request",
			mset.account(), mset.name(), snap.FirstSeq)
	}

	if len(snap.Deleted) > 0 {
		return mset.store.SyncDeleted(snap.Deleted)
	}
	return nil
}

// catchupPeer tracks a peer the leader is catching up out of band.
type catchupPeer struct {
	lag  uint64        // Lag is the number of messages that still need to be sent to the peer.
	quit chan struct{} // Quit channel is closed to cancel the runCatchup serving this peer.
}

// setCatchupPeer records that this peer is catching up, and returns its quit channel.
func (mset *stream) setCatchupPeer(peer string, lag uint64) <-chan struct{} {
	if peer == _EMPTY_ {
		return nil
	}
	mset.mu.Lock()
	defer mset.mu.Unlock()
	if mset.catchups == nil {
		mset.catchups = make(map[string]*catchupPeer)
	}
	// Cancel if there was a previous catchup for this peer.
	if cp := mset.catchups[peer]; cp != nil && cp.quit != nil {
		close(cp.quit)
	}
	quit := make(chan struct{})
	mset.catchups[peer] = &catchupPeer{lag: lag, quit: quit}
	return quit
}

// Will decrement by one.
func (mset *stream) updateCatchupPeer(peer string) {
	if peer == _EMPTY_ {
		return
	}
	mset.mu.Lock()
	if cp := mset.catchups[peer]; cp != nil && cp.lag > 0 {
		cp.lag--
	}
	mset.mu.Unlock()
}

func (mset *stream) decrementCatchupPeer(peer string, num uint64) {
	if peer == _EMPTY_ {
		return
	}
	mset.mu.Lock()
	if cp := mset.catchups[peer]; cp != nil && cp.lag > 0 {
		if cp.lag >= num {
			cp.lag -= num
		} else {
			cp.lag = 0
		}
	}
	mset.mu.Unlock()
}

// detachCatchupPeer detaches the quit channel for the peer, leaving it tracked after exiting runCatchup.
func (mset *stream) detachCatchupPeer(peer string, quit <-chan struct{}) {
	mset.mu.Lock()
	if cp := mset.catchups[peer]; cp != nil && cp.quit != nil && cp.quit == quit {
		cp.quit = nil
	}
	mset.mu.Unlock()
}

// clearCatchupPeer stops tracking the peer if its quit channel matches.
func (mset *stream) clearCatchupPeer(peer string, quit <-chan struct{}) {
	mset.mu.Lock()
	if cp := mset.catchups[peer]; cp != nil && cp.quit != nil && cp.quit == quit {
		mset.clearCatchupPeerLocked(peer)
	}
	mset.mu.Unlock()
}

// Lock should be held.
func (mset *stream) clearCatchupPeerLocked(peer string) {
	if cp := mset.catchups[peer]; cp != nil {
		if cp.quit != nil {
			close(cp.quit)
			cp.quit = nil
		}
		delete(mset.catchups, peer)
	}
	if len(mset.catchups) == 0 {
		mset.catchups = nil
	}
}

// Lock should be held.
func (mset *stream) clearAllCatchupPeers() {
	for _, cp := range mset.catchups {
		if cp.quit != nil {
			close(cp.quit)
			cp.quit = nil
		}
	}
	mset.catchups = nil
}

func (mset *stream) lagForCatchupPeer(peer string) uint64 {
	mset.mu.RLock()
	defer mset.mu.RUnlock()
	if cp := mset.catchups[peer]; cp != nil {
		return cp.lag
	}
	return 0
}

func (mset *stream) hasCatchupPeers() bool {
	mset.mu.RLock()
	defer mset.mu.RUnlock()
	return len(mset.catchups) > 0
}

func (mset *stream) catchupPeers() []string {
	mset.mu.RLock()
	defer mset.mu.RUnlock()
	return slices.Collect(maps.Keys(mset.catchups))
}

func (mset *stream) setCatchingUp() {
	mset.catchup.Store(true)
}

func (mset *stream) clearCatchingUp() {
	mset.catchup.Store(false)
}

func (mset *stream) isCatchingUp() bool {
	return mset.catchup.Load()
}

// Determine if a non-leader is current.
// Lock should be held.
func (mset *stream) isCurrent() bool {
	if mset.node == nil {
		return true
	}
	return mset.node.Current() && !mset.catchup.Load()
}

// Maximum requests for the whole server that can be in flight at the same time.
const maxConcurrentSyncRequests = 32

var (
	errCatchupCorruptSnapshot = errors.New("corrupt stream snapshot detected")
	errCatchupStalled         = errors.New("catchup stalled")
	errCatchupStreamStopped   = errors.New("stream has been stopped") // when a catchup is terminated due to the stream going away.
	errCatchupBadMsg          = errors.New("bad catchup msg")
	errCatchupWrongSeqForSkip = errors.New("wrong sequence for skipped msg")
	errCatchupAbortedNoLeader = errors.New("catchup aborted, no leader")
	errCatchupTooManyRetries  = errors.New("catchup failed, too many retries")
)

// Catchup inactivity timers.
const (
	defaultStreamCatchupStartInterval    = 5 * time.Second
	defaultStreamCatchupActivityInterval = 30 * time.Second
)

var (
	streamCatchupStartInterval    = defaultStreamCatchupStartInterval
	streamCatchupActivityInterval = defaultStreamCatchupActivityInterval
)

// Process a stream snapshot.
func (mset *stream) processSnapshot(snap *StreamReplicatedState, index uint64) (e error) {
	// Update any deletes, etc.
	if err := mset.processSnapshotDeletes(snap); err != nil {
		return err
	}
	mset.setCLFS(snap.Failed)

	mset.mu.Lock()
	var state StreamState
	mset.store.FastState(&state)
	sreq := mset.calculateSyncRequest(&state, snap, index)

	if mset.sa == nil || mset.node == nil {
		mset.mu.Unlock()
		return errCatchupStreamStopped
	}
	s, js, subject, n, st := mset.srv, mset.js, mset.sa.Sync, mset.node, mset.cfg.Storage
	qname := fmt.Sprintf("[ACC:%s] stream '%s' snapshot", mset.acc.Name, mset.cfg.Name)
	mset.mu.Unlock()

	// Always try to resume applies, we might be paused already if we timed out of processing the snapshot previously.
	defer func() {
		// Don't bother resuming if server or stream is gone.
		if e != errCatchupStreamStopped && e != ErrServerNotRunning {
			n.ResumeApply()
		}
	}()

	// Bug that would cause this to be empty on stream update.
	if subject == _EMPTY_ {
		return errCatchupCorruptSnapshot
	}

	// Just return if up to date.
	if sreq == nil {
		return nil
	}

	// We need to catch up, but are already exceeding limits.
	if js.limitsExceeded(st) {
		s.resourcesExceededError(st)
		return NewJSInsufficientResourcesError()
	}

	// Pause the apply channel for our raft group while we catch up.
	if err := n.PauseApply(); err != nil {
		// The only reason PauseApply can fail is due to errAlreadyLeader.
		// We step down to get someone else to become the leader that can catch us up.
		// Ignore the error since we could have already stepped down before us doing so here.
		_ = n.StepDown()
		// Now try pausing again and continue to catchup.
		if err = n.PauseApply(); err != nil {
			return err
		}
	}

	// Set our catchup state.
	mset.setCatchingUp()
	defer mset.clearCatchingUp()

	var sub *subscription
	var err error

	startInterval := streamCatchupStartInterval
	activityInterval := streamCatchupActivityInterval
	notActive := time.NewTimer(startInterval)
	defer notActive.Stop()

	defer func() {
		if sub != nil {
			s.sysUnsubscribe(sub)
		}
		// Make sure any consumers are updated for the pending amounts.
		mset.mu.Lock()
		for _, o := range mset.consumers {
			o.mu.Lock()
			o.streamNumPending()
			o.mu.Unlock()
		}
		mset.mu.Unlock()

		// If we are interest based make sure to check our ack floor state.
		// We will delay a bit to allow consumer states to also catchup.
		if mset.isInterestRetention() {
			fire := time.Duration(rand.IntN(10)+5) * time.Second
			time.AfterFunc(fire, mset.checkInterestState)
		}
	}()

	var releaseSem bool
	releaseSyncOutSem := func() {
		if !releaseSem {
			return
		}
		// Need to use select for the server shutdown case.
		select {
		case s.syncOutSem <- struct{}{}:
		default:
		}
		releaseSem = false
	}
	// On exit, we will release our semaphore if we acquired it.
	defer releaseSyncOutSem()

	// Do not let this go on forever.
	start := time.Now()
	const maxRetries = 3
	var numRetries int

RETRY:
	// On retry, we need to release the semaphore we got. Call will be no-op
	// if releaseSem boolean has not been set to true on successfully getting
	// the semaphore.
	releaseSyncOutSem()

	if n.Leaderless() {
		// Prevent us from spinning if we've installed a snapshot from a leader but there's no leader online.
		// We wait a bit to check if a leader has come online in the meantime, if so we can continue.
		var canContinue bool
		if numRetries == 0 {
			time.Sleep(startInterval)
			canContinue = !n.Leaderless()
		}
		if !canContinue {
			return fmt.Errorf("%w for stream '%s > %s'", errCatchupAbortedNoLeader, mset.account(), mset.name())
		}
	}

	// If we have a sub clear that here.
	if sub != nil {
		s.sysUnsubscribe(sub)
		sub = nil
	}

	if !s.isRunning() {
		return ErrServerNotRunning
	}

	numRetries++
	if numRetries > maxRetries {
		// Force a hard reset here.
		return errCatchupTooManyRetries
	}

	// Block here if we have too many requests in flight.
	<-s.syncOutSem
	releaseSem = true

	// We may have been blocked for a bit, so the reset needs to ensure that we
	// consume the already fired timer.
	if !notActive.Stop() {
		select {
		case <-notActive.C:
		default:
		}
	}
	notActive.Reset(startInterval)

	// Grab sync request again on failures.
	if sreq == nil {
		mset.mu.RLock()
		var state StreamState
		mset.store.FastState(&state)
		sreq = mset.calculateSyncRequest(&state, snap, index)
		mset.mu.RUnlock()
		if sreq == nil {
			return nil
		}
	}

	// Used to transfer message from the wire to another Go routine internally.
	type im struct {
		msg   []byte
		reply string
	}
	// This is used to notify the leader that it should stop the runCatchup
	// because we are either bailing out or going to retry due to an error.
	var lastReply string
	notifyLeaderStopCatchup := func(reply string, err error) error {
		if reply == _EMPTY_ {
			return err
		}
		s.sendInternalMsgLocked(reply, _EMPTY_, nil, err.Error())
		return err
	}

	msgsQ := newIPQueue[*im](s, qname)
	defer msgsQ.unregister()

	// Send our catchup request here.
	reply := syncReplySubject()
	sub, err = s.sysSubscribe(reply, func(_ *subscription, _ *client, _ *Account, _, reply string, msg []byte) {
		// Make copy since we are using a buffer from the inbound client/route.
		msgsQ.push(&im{copyBytes(msg), reply})
	})
	if err != nil {
		s.Errorf("Could not subscribe to stream catchup: %v", err)
		goto RETRY
	}

	// Send our sync request.
	b, _ := json.Marshal(sreq)
	s.sendInternalMsgLocked(subject, reply, nil, b)

	// Remember when we sent this out to avoid loop spins on errors below.
	reqSendTime := time.Now()

	// Clear our sync request.
	sreq = nil

	// Run our own select loop here.
	for qch := n.QuitC(); ; {
		select {
		case <-msgsQ.ch:
			notActive.Reset(activityInterval)

			mrecs := msgsQ.pop()
			for _, mrec := range mrecs {
				msg := mrec.msg
				if mrec.reply != _EMPTY_ {
					lastReply = mrec.reply
				}
				// Check for eof signaling.
				if len(msg) == 0 {
					msgsQ.recycle(&mrecs)

					// Sanity check that we've received all data expected by the snapshot.
					mset.mu.RLock()
					lseq := mset.lseq
					mset.mu.RUnlock()
					if lseq >= snap.LastSeq {
						// We MUST ensure all data is flushed up to this point, if the store hadn't already.
						// Because the snapshot needs to represent what has been persisted.
						err = mset.flushAllPending()
						if err == nil {
							s.Noticef("Catchup for stream '%s > %s' complete (took %v)", mset.account(), mset.name(), time.Since(start).Round(time.Millisecond))
						} else {
							s.Noticef("Catchup for stream '%s > %s' errored: %v (took %v)", mset.account(), mset.name(), err, time.Since(start).Round(time.Millisecond))
						}
						return err
					}

					// Make sure we do not spin and make things worse.
					const minRetryWait = 2 * time.Second
					elapsed := time.Since(reqSendTime)
					if elapsed < minRetryWait {
						select {
						case <-s.quitCh:
							return notifyLeaderStopCatchup(lastReply, ErrServerNotRunning)
						case <-qch:
							return notifyLeaderStopCatchup(lastReply, errCatchupStreamStopped)
						case <-time.After(minRetryWait - elapsed):
						}
					}
					goto RETRY
				}
				if _, err := mset.processCatchupMsg(msg); err == nil {
					if mrec.reply != _EMPTY_ {
						s.sendInternalMsgLocked(mrec.reply, _EMPTY_, nil, nil)
					}
				} else if isOutOfSpaceErr(err) {
					_ = notifyLeaderStopCatchup(mrec.reply, err)
					msgsQ.recycle(&mrecs)
					return err
				} else if err == NewJSInsufficientResourcesError() {
					_ = notifyLeaderStopCatchup(mrec.reply, err)
					if mset.js.limitsExceeded(st) {
						s.resourcesExceededError(st)
					} else {
						s.Warnf("Catchup for stream '%s > %s' errored, account resources exceeded: %v", mset.account(), mset.name(), err)
					}
					msgsQ.recycle(&mrecs)
					return err
				} else {
					_ = notifyLeaderStopCatchup(mrec.reply, err)
					s.Warnf("Catchup for stream '%s > %s' errored, will retry: %v", mset.account(), mset.name(), err)
					msgsQ.recycle(&mrecs)

					// Make sure we do not spin and make things worse.
					const minRetryWait = 2 * time.Second
					elapsed := time.Since(reqSendTime)
					if elapsed < minRetryWait {
						select {
						case <-s.quitCh:
							return ErrServerNotRunning
						case <-qch:
							return errCatchupStreamStopped
						case <-time.After(minRetryWait - elapsed):
						}
					}
					goto RETRY
				}
			}
			notActive.Reset(activityInterval)
			msgsQ.recycle(&mrecs)
		case <-notActive.C:
			if mrecs := msgsQ.pop(); len(mrecs) > 0 {
				mrec := mrecs[0]
				_ = notifyLeaderStopCatchup(mrec.reply, errCatchupStalled)
				msgsQ.recycle(&mrecs)
			}
			s.Warnf("Catchup for stream '%s > %s' stalled", mset.account(), mset.name())
			// Sanity check that we've not become leader. Shouldn't be possible
			// since we haven't applied the snapshot yet.
			if n.State() == Leader {
				n.StepDown()
			}
			goto RETRY
		case <-s.quitCh:
			return notifyLeaderStopCatchup(lastReply, ErrServerNotRunning)
		case <-qch:
			return notifyLeaderStopCatchup(lastReply, errCatchupStreamStopped)
		}
	}
}

// processCatchupMsg will be called to process out of band catchup msgs from a sync request.
func (mset *stream) processCatchupMsg(msg []byte) (uint64, error) {
	if len(msg) == 0 {
		return 0, errCatchupBadMsg
	}
	op := entryOp(msg[0])
	if op != streamMsgOp && op != compressedStreamMsgOp && op != deleteRangeOp {
		return 0, errCatchupBadMsg
	}

	mbuf := msg[1:]
	if op == deleteRangeOp {
		dr, err := decodeDeleteRange(mbuf)
		if err != nil {
			return 0, errCatchupBadMsg
		}
		// Handle the delete range.
		// Make sure the sequences match up properly.
		mset.mu.Lock()
		lseq := dr.First + dr.Num - 1
		if err = mset.store.SkipMsgs(dr.First, dr.Num); err != nil {
			mset.mu.Unlock()
			return 0, errCatchupWrongSeqForSkip
		}
		mset.clearAllPreAcksInRange(dr.First, lseq)
		mset.lseq = lseq
		mset.mu.Unlock()
		return lseq, nil
	}

	if op == compressedStreamMsgOp {
		var err error
		mbuf, err = s2.Decode(nil, mbuf)
		if err != nil {
			panic(err.Error())
		}
	}

	subj, _, hdr, msg, seq, ts, _, err := decodeStreamMsg(mbuf)
	if err != nil {
		return 0, errCatchupBadMsg
	}

	mset.mu.Lock()
	st := mset.cfg.Storage
	if mset.hasAllPreAcks(seq, subj) {
		mset.clearAllPreAcks(seq)
		// Mark this to be skipped
		subj, ts = _EMPTY_, 0
	}
	mset.mu.Unlock()

	// Since we're clustered we do not want to check limits based on tier here and possibly introduce skew.
	if mset.js.limitsExceeded(st) {
		return 0, NewJSInsufficientResourcesError()
	}

	// Find the message TTL if any.
	// TODO(nat): If the TTL isn't valid by this stage then there isn't really a
	// lot we can do about it, as we'd break the catchup if we reject the message.
	ttl, _ := getMessageTTL(hdr)

	// Put into our store
	// Messages to be skipped have no subject or timestamp.
	// TODO(dlc) - formalize with skipMsgOp
	if subj == _EMPTY_ && ts == 0 {
		if _, err = mset.store.SkipMsg(seq); err != nil {
			return 0, errCatchupWrongSeqForSkip
		}
	} else if err := mset.store.StoreRawMsg(subj, hdr, msg, seq, ts, ttl, false); err != nil {
		return 0, err
	}

	mset.mu.Lock()
	defer mset.mu.Unlock()
	// Update our lseq.
	mset.lseq = seq

	// Check for MsgId and if we have one here make sure to update our internal map.
	if len(hdr) > 0 {
		if msgId := getMsgId(hdr); msgId != _EMPTY_ {
			mset.ddMu.Lock()
			mset.storeMsgIdLocked(&ddentry{msgId, seq, ts})
			mset.ddMu.Unlock()
		}
	}

	return seq, nil
}

// flushAllPending will flush any pending writes as a result of installing a snapshot or performing catchup.
func (mset *stream) flushAllPending() error {
	return mset.store.FlushAllPending()
}

func (mset *stream) handleClusterSyncRequest(sub *subscription, c *client, _ *Account, subject, reply string, msg []byte) {
	var sreq streamSyncRequest
	if err := json.Unmarshal(msg, &sreq); err != nil {
		// Log error.
		return
	}
	mset.srv.startGoRoutine(func() { mset.runCatchup(reply, &sreq) })
}

// Lock should be held.
func (js *jetStream) offlineClusterInfo(rg *raftGroup) *ClusterInfo {
	s := js.srv

	ci := &ClusterInfo{Name: s.ClusterName(), RaftGroup: rg.Name}
	for _, peer := range rg.Peers {
		if sir, ok := s.nodeToInfo.Load(peer); ok && sir != nil {
			si := sir.(nodeInfo)
			pi := &PeerInfo{Peer: peer, Name: si.name, Current: false, Offline: true}
			ci.Replicas = append(ci.Replicas, pi)
		}
	}
	return ci
}

// clusterInfo will report on the status of the raft group.
func (js *jetStream) clusterInfo(rg *raftGroup) *ClusterInfo {
	if js == nil {
		return nil
	}
	js.mu.RLock()
	s := js.srv
	if rg == nil || (rg.node == nil && rg.Desired == nil) {
		js.mu.RUnlock()
		return &ClusterInfo{
			Name:   s.cachedClusterName(),
			Leader: s.Name(),
		}
	}
	// Use v2.15-compatible cluster info if desired state is known.
	if rg.Desired != nil {
		js.mu.RUnlock()
		return js.clusterInfoDesired(rg)
	}

	// Capture what we need and let go of the lock to ensure that
	// contention on Raft locks can't happen while holding JS lock.
	n := rg.node
	rgName := rg.Name
	rgPeers := slices.Clone(rg.Peers)
	js.mu.RUnlock()

	ci := &ClusterInfo{
		Name:        s.cachedClusterName(),
		Leader:      s.serverNameForNode(n.GroupLeader()),
		LeaderSince: n.LeaderSince(),
		SystemAcc:   n.IsSystemAccount(),
		TrafficAcc:  n.GetTrafficAccountName(),
		RaftGroup:   rgName,
	}

	now := time.Now()
	id, peers := n.ID(), n.Peers()

	// If we are leaderless, do not suppress putting us in the peer list.
	if ci.Leader == _EMPTY_ {
		id = _EMPTY_
	}

	for _, rp := range peers {
		if rp.ID != id && slices.Contains(rgPeers, rp.ID) {
			var lastSeen time.Duration
			if now.After(rp.Last) && !rp.Last.IsZero() {
				lastSeen = now.Sub(rp.Last)
			}
			current := rp.Current
			if current && lastSeen > lostQuorumInterval {
				current = false
			}
			// Create a peer info with common settings if the peer has not been seen
			// yet (which can happen after the whole cluster is stopped and only some
			// of the nodes are restarted).
			pi := &PeerInfo{
				Current: current,
				Offline: true,
				Active:  lastSeen,
				Lag:     rp.Lag,
				Peer:    rp.ID,
			}
			// If node is found, complete/update the settings.
			if sir, ok := s.nodeToInfo.Load(rp.ID); ok && sir != nil {
				si := sir.(nodeInfo)
				pi.Name, pi.Offline, pi.cluster = si.name, si.offline, si.cluster
			} else {
				// If not, then add a name that indicates that the server name
				// is unknown at this time, and clear the lag since it is misleading
				// (the node may not have that much lag).
				// Note: We return now the Peer ID in PeerInfo, so the "(peerID: %s)"
				// would technically not be required, but keeping it for now.
				pi.Name, pi.Lag = fmt.Sprintf("Server name unknown at this time (peerID: %s)", rp.ID), 0
			}
			ci.Replicas = append(ci.Replicas, pi)
		}
	}
	// Order the result based on the name so that we get something consistent
	// when doing repeated stream info in the CLI, etc...
	slices.SortFunc(ci.Replicas, func(i, j *PeerInfo) int { return cmp.Compare(i.Name, j.Name) })
	return ci
}

// clusterInfoDesired returns v2.15-compatible cluster info based on desired state existing.
func (js *jetStream) clusterInfoDesired(rg *raftGroup) *ClusterInfo {
	if js == nil {
		return nil
	}
	js.mu.RLock()
	s := js.srv
	if rg == nil || (rg.node == nil && rg.Desired == nil) {
		js.mu.RUnlock()
		return &ClusterInfo{
			Name:   s.cachedClusterName(),
			Leader: s.Name(),
		}
	}
	// Capture what we need and let go of the lock to ensure that
	// contention on Raft locks can't happen while holding JS lock.
	n := rg.node
	rgName := rg.Name
	rgPeers := copyStrings(rg.Peers)
	var (
		desired      *DesiredClusterInfo
		desiredPeers []string
	)
	if d := rg.Desired; d != nil {
		desired = &DesiredClusterInfo{
			Created: d.Created,
			Name:    d.Cluster,
			Status: &DesiredClusterInfoStatus{
				Type:        MigrationStatusBlocked,
				Description: "upgrade to v2.15 or higher",
			},
		}
		// If desired state can be rolled back, include what can be rolled back to.
		// Must copy, the origin is owned by the meta layer but reported without its lock.
		if d.Origin != nil {
			desired.Origin = &DesiredClusterInfoOrigin{
				Replicas:  d.Origin.Replicas,
				Placement: d.Origin.Placement.clone(),
			}
			if r := d.Origin.Retention; r != nil {
				retention := *r
				desired.Origin.Retention = &retention
			}
		}
		// Don't populate peers if scaling down, since the peers aren't the desired set, it's
		// the set that the group leader selects the peers to scale down to from.
		if !d.ScaleDown {
			desiredPeers = copyStrings(d.Peers)
		}
	}
	// The group leader can have a status before desired state exists, most importantly
	// when it's still requesting it from the meta leader. Report that on its own.
	if status := rg.migration; status != nil {
		if desired == nil {
			desired = &DesiredClusterInfo{}
		}
		cp := *status
		desired.Status = &cp
	}
	js.mu.RUnlock()

	ci := &ClusterInfo{
		Name:      s.cachedClusterName(),
		RaftGroup: rgName,
	}

	id := s.Node()
	if n != nil {
		ci.Leader = s.serverNameForNode(n.GroupLeader())
		ci.LeaderSince = n.LeaderSince()
		ci.SystemAcc = n.IsSystemAccount()
		ci.TrafficAcc = n.GetTrafficAccountName()

		// If we are leaderless, do not suppress putting us in the peer list.
		if ci.Leader == _EMPTY_ {
			id = _EMPTY_
		}

		now := time.Now()
		for _, rp := range n.Peers() {
			// The peer is either in the actual or desired peer set.
			if rp.ID != id && (slices.Contains(rgPeers, rp.ID) || slices.Contains(desiredPeers, rp.ID)) {
				var lastSeen time.Duration
				if now.After(rp.Last) && !rp.Last.IsZero() {
					lastSeen = now.Sub(rp.Last)
				}
				current := rp.Current
				if current && lastSeen > lostQuorumInterval {
					current = false
				}
				// Create a peer info with common settings if the peer has not been seen
				// yet (which can happen after the whole cluster is stopped and only some
				// of the nodes are restarted).
				pi := &PeerInfo{
					Current: current,
					Offline: true,
					Active:  lastSeen,
					Lag:     rp.Lag,
					Peer:    rp.ID,
				}
				// If node is found, complete/update the settings.
				if sir, ok := s.nodeToInfo.Load(rp.ID); ok && sir != nil {
					si := sir.(nodeInfo)
					pi.Name, pi.Offline, pi.cluster = si.name, si.offline, si.cluster
				} else {
					// If not, then add a name that indicates that the server name
					// is unknown at this time, and clear the lag since it is misleading
					// (the node may not have that much lag).
					// Note: We return now the Peer ID in PeerInfo, so the "(peerID: %s)"
					// would technically not be required, but keeping it for now.
					pi.Name, pi.Lag = fmt.Sprintf("Server name unknown at this time (peerID: %s)", rp.ID), 0
				}
				ci.Replicas = append(ci.Replicas, pi)
			}
		}
	}

	generatePeer := func(peer string) *DesiredPeerInfo {
		pi := &DesiredPeerInfo{Offline: true, Peer: peer}
		if sir, ok := s.nodeToInfo.Load(peer); ok && sir != nil {
			si := sir.(nodeInfo)
			pi.Name, pi.Offline = si.name, si.offline
		} else {
			pi.Name = fmt.Sprintf("Server name unknown at this time (peerID: %s)", peer)
		}
		return pi
	}
	if desired != nil {
		ci.Desired = desired
		for _, peer := range desiredPeers {
			ci.Desired.Replicas = append(ci.Desired.Replicas, generatePeer(peer))
		}
	}
	for _, peer := range rgPeers {
		// Skip if the peer is already present.
		if peer == id || slices.ContainsFunc(ci.Replicas, func(info *PeerInfo) bool { return info.Peer == peer }) {
			continue
		}
		pi := generatePeer(peer)
		p := &PeerInfo{Name: pi.Name, Offline: pi.Offline, Peer: pi.Peer}
		// We know the peer is part of the assignment, but if we have a Raft node it
		// wasn't reported as one of its peers, so it hasn't joined the group (yet).
		p.Pending = n != nil
		ci.Replicas = append(ci.Replicas, p)
	}
	// Order the result based on the name so that we get something consistent
	// when doing repeated stream info in the CLI, etc...
	slices.SortFunc(ci.Replicas, func(i, j *PeerInfo) int { return cmp.Compare(i.Name, j.Name) })
	return ci
}

func (mset *stream) checkClusterInfo(ci *ClusterInfo) {
	for _, r := range ci.Replicas {
		peer := getHash(r.Name)
		if lag := mset.lagForCatchupPeer(peer); lag > 0 {
			r.Current = false
			r.Lag = lag
		}
	}
}

// Return a list of alternates, ranked by preference order to the request, of stream mirrors.
// This allows clients to select or get more information about read replicas that could be a
// better option to connect to versus the original source.
func (js *jetStream) streamAlternates(ci *ClientInfo, stream string) []StreamAlternate {
	if js == nil {
		return nil
	}

	js.mu.RLock()
	defer js.mu.RUnlock()

	s, cc := js.srv, js.cluster
	// Track our domain.
	domain := s.getOpts().JetStreamDomain

	// No clustering just return nil.
	if cc == nil {
		return nil
	}
	acc, _ := s.LookupAccount(ci.serviceAccount())
	if acc == nil {
		return nil
	}

	// Collect our ordering first for clusters.
	weights := make(map[string]int)
	all := []string{ci.Cluster}
	all = append(all, ci.Alternates...)

	for i := 0; i < len(all); i++ {
		weights[all[i]] = len(all) - i
	}

	var alts []StreamAlternate
	for _, sa := range cc.streams[acc.Name] {
		if sa.unsupported != nil {
			continue
		}
		// Add in ourselves and any mirrors.
		if sa.Config.Name == stream || (sa.Config.Mirror != nil && sa.Config.Mirror.Name == stream) {
			alts = append(alts, StreamAlternate{Name: sa.Config.Name, Domain: domain, Cluster: sa.Group.Cluster})
		}
	}
	// If just us don't fill in.
	if len(alts) == 1 {
		return nil
	}

	// Sort based on our weights that originate from the request itself.
	// reverse sort
	slices.SortFunc(alts, func(i, j StreamAlternate) int { return -cmp.Compare(weights[i.Cluster], weights[j.Cluster]) })

	return alts
}

// Internal request for stream info, this is coming on the wire so do not block here.
func (mset *stream) handleClusterStreamInfoRequest(_ *subscription, c *client, _ *Account, subject, reply string, _ []byte) {
	go mset.processClusterStreamInfoRequest(reply)
}

func (mset *stream) processClusterStreamInfoRequest(reply string) {
	mset.mu.RLock()
	sysc, js, sa, config := mset.sysc, mset.srv.js.Load(), mset.sa, mset.cfg
	isLeader := mset.isLeader()
	mset.mu.RUnlock()

	// By design all members will receive this. Normally we only want the leader answering.
	// But if we have stalled and lost quorom all can respond.
	if sa != nil && !js.isGroupLeaderless(sa.Group) && !isLeader {
		return
	}

	// If we are not the leader let someone else possibly respond first.
	if !isLeader {
		time.Sleep(500 * time.Millisecond)
	}

	// Report the config as requested, the stream can still be running at its origin.
	config = js.targetStreamConfig(mset, config)

	si := &StreamInfo{
		Created:   mset.createdTime(),
		State:     mset.state(),
		Config:    config,
		Cluster:   js.clusterInfo(mset.raftGroup()),
		Sources:   mset.sourcesInfo(),
		Mirror:    mset.mirrorInfo(),
		TimeStamp: time.Now().UTC(),
	}

	// Check for out of band catchups.
	if mset.hasCatchupPeers() {
		mset.checkClusterInfo(si.Cluster)
	}

	sysc.sendInternalMsg(reply, _EMPTY_, nil, si)
}

// 64MB for now, for the total server. This is max we will blast out if asked to
// do so to another server for purposes of catchups.
// This number should be ok on 1Gbit interface.
const defaultMaxTotalCatchupOutBytes = int64(64 * 1024 * 1024)

// Current total outstanding catchup bytes.
func (s *Server) gcbTotal() int64 {
	s.gcbMu.RLock()
	defer s.gcbMu.RUnlock()
	return s.gcbOut
}

// Returns true if Current total outstanding catchup bytes is below
// the maximum configured.
func (s *Server) gcbBelowMax() bool {
	s.gcbMu.RLock()
	defer s.gcbMu.RUnlock()
	return s.gcbOut <= s.gcbOutMax
}

// Adds `sz` to the server's total outstanding catchup bytes and to `localsz`
// under the gcbMu lock. The `localsz` points to the local outstanding catchup
// bytes of the runCatchup go routine of a given stream.
func (s *Server) gcbAdd(localsz *int64, sz int64) {
	s.gcbMu.Lock()
	atomic.AddInt64(localsz, sz)
	s.gcbOut += sz
	if s.gcbOut >= s.gcbOutMax && s.gcbKick == nil {
		s.gcbKick = make(chan struct{})
	}
	s.gcbMu.Unlock()
}

// Removes `sz` from the server's total outstanding catchup bytes and from
// `localsz`, but only if `localsz` is non 0, which would signal that gcSubLast
// has already been invoked. See that function for details.
// Must be invoked under the gcbMu lock.
func (s *Server) gcbSubLocked(localsz *int64, sz int64) {
	if atomic.LoadInt64(localsz) == 0 {
		return
	}
	atomic.AddInt64(localsz, -sz)
	s.gcbOut -= sz
	if s.gcbKick != nil && s.gcbOut < s.gcbOutMax {
		close(s.gcbKick)
		s.gcbKick = nil
	}
}

// Locked version of gcbSubLocked()
func (s *Server) gcbSub(localsz *int64, sz int64) {
	s.gcbMu.Lock()
	s.gcbSubLocked(localsz, sz)
	s.gcbMu.Unlock()
}

// Similar to gcbSub() but reset `localsz` to 0 at the end under the gcbMu lock.
// This will signal further calls to gcbSub() for this `localsz` pointer that
// nothing should be done because runCatchup() has exited and any remaining
// outstanding bytes value has already been decremented.
func (s *Server) gcbSubLast(localsz *int64) {
	s.gcbMu.Lock()
	s.gcbSubLocked(localsz, *localsz)
	*localsz = 0
	s.gcbMu.Unlock()
}

// Returns our kick chan, or nil if it does not exist.
func (s *Server) cbKickChan() <-chan struct{} {
	s.gcbMu.RLock()
	defer s.gcbMu.RUnlock()
	return s.gcbKick
}

func (mset *stream) runCatchup(sendSubject string, sreq *streamSyncRequest) {
	s := mset.srv
	defer s.grWG.Done()

	const maxOutBytes = int64(64 * 1024 * 1024) // 64MB for now, these are all internal, from server to server
	const maxOutMsgs = int32(256 * 1024)        // 256k in case we have lots of small messages or skip msgs.
	outb := int64(0)
	outm := int32(0)

	// On abnormal exit make sure to update global total.
	defer s.gcbSubLast(&outb)

	// Flow control processing.
	ackReplySize := func(subj string) int64 {
		if li := strings.LastIndexByte(subj, btsep); li > 0 && li < len(subj) {
			return parseAckReplyNum(subj[li+1:])
		}
		return 0
	}

	nextBatchC := make(chan struct{}, 4)
	nextBatchC <- struct{}{}
	remoteQuitCh := make(chan struct{})
	var remoteQuitOnce sync.Once

	activityInterval := streamCatchupActivityInterval
	notActive := time.NewTimer(activityInterval)
	defer notActive.Stop()

	// Setup ackReply for flow control.
	ackReply := syncAckSubject()
	ackSub, _ := s.sysSubscribe(ackReply, func(sub *subscription, c *client, _ *Account, subject, reply string, msg []byte) {
		if len(msg) > 0 {
			s.Warnf("Catchup for stream '%s > %s' was aborted on the remote due to: %q",
				mset.account(), mset.name(), msg)
			s.sysUnsubscribe(sub)
			// The unsubscribe is skipped during shutdown, so guard against a repeated abort.
			remoteQuitOnce.Do(func() { close(remoteQuitCh) })
			return
		}
		sz := ackReplySize(subject)
		s.gcbSub(&outb, sz)
		atomic.AddInt32(&outm, -1)
		mset.updateCatchupPeer(sreq.Peer)
		// Kick ourselves and anyone else who might have stalled on global state.
		select {
		case nextBatchC <- struct{}{}:
		default:
		}
		// Reset our activity
		notActive.Reset(activityInterval)
	})
	defer s.sysUnsubscribe(ackSub)
	ackReplyT := strings.ReplaceAll(ackReply, ".*", ".%d")

	// Grab our state.
	var state StreamState
	// mset.store never changes after being set, don't need lock.
	mset.store.FastState(&state)

	// Setup sequences to walk through.
	seq, last := sreq.FirstSeq, sreq.LastSeq

	// The follower received a snapshot from another leader, and we've become leader since.
	// We have an up-to-date log but could be behind on applies. We must wait until we've reached the minimum required.
	// The follower will automatically retry after a timeout, so we can safely return here.
	if node := mset.raftNode(); node != nil {
		index, _, applied := node.Progress()
		// Only skip if our log has enough entries, and they could be applied in the future.
		if index >= sreq.MinApplied && applied < sreq.MinApplied {
			return
		}
		// We know here we've either applied enough entries, or our log doesn't have enough entries.
		// In the latter case the request expects us to have more. Just continue and value availability here.
		// This should only be possible if the logs have already desynced, and we shouldn't have become leader
		// in the first place. Not much we can do here in this (hypothetical) scenario.

		// Do another quick sanity check that we actually have enough data to satisfy the request.
		// If not, let's step down and hope a new leader can correct this.
		if state.LastSeq < last {
			s.Warnf("Catchup for stream '%s > %s' skipped, requested sequence %d was larger than current state: %+v",
				mset.account(), mset.name(), seq, state)
			node.StepDown()
			return
		}
	}

	start := time.Now()
	quit := mset.setCatchupPeer(sreq.Peer, last-seq)

	var spb int
	const minWait = 5 * time.Second

	sendNextBatchAndContinue := func(qch chan struct{}) bool {
		// Check if we know we will not enter the loop because we are done.
		if seq > last {
			s.Noticef("Catchup for stream '%s > %s' complete (took %v)", mset.account(), mset.name(), time.Since(start).Round(time.Millisecond))
			// EOF
			s.sendInternalMsgLocked(sendSubject, _EMPTY_, nil, nil)
			return false
		}

		// If we already sent a batch, we will try to make sure we can at least send a minimum
		// batch before sending the next batch.
		if spb > 0 {
			// Wait til we can send at least 4k
			const minBatchWait = int32(4 * 1024)
			mw := time.NewTimer(minWait)
			for done := maxOutMsgs-atomic.LoadInt32(&outm) > minBatchWait; !done; {
				select {
				case <-nextBatchC:
					done = maxOutMsgs-atomic.LoadInt32(&outm) > minBatchWait
					if !done {
						// Wait for a small bit.
						time.Sleep(100 * time.Millisecond)
					} else {
						// GC friendly.
						mw.Stop()
					}
				case <-mw.C:
					done = true
				case <-s.quitCh:
					return false
				case <-qch:
					return false
				case <-remoteQuitCh:
					return false
				case <-quit:
					return false
				}
			}
			spb = 0
		}

		// Send an encoded msg.
		sendEM := func(em []byte) {
			// Place size in reply subject for flow control.
			l := int64(len(em))
			reply := fmt.Sprintf(ackReplyT, l)
			s.gcbAdd(&outb, l)
			atomic.AddInt32(&outm, 1)
			s.sendInternalMsgLocked(sendSubject, reply, nil, em)
			spb++
		}

		// If we support gap markers.
		var dr DeleteRange
		drOk := sreq.DeleteRangesOk

		// Will send our delete range.
		// Should already be checked for being valid.
		sendDR := func() {
			if dr.Num == 1 {
				// Send like a normal skip msg.
				sendEM(encodeStreamMsg(_EMPTY_, _EMPTY_, nil, nil, dr.First, 0, false))
			} else {
				// We have a run, send a gap record. We send these without reply or tracking.
				s.sendInternalMsgLocked(sendSubject, _EMPTY_, nil, encodeDeleteRange(&dr))
				// Clear out the pending for catchup.
				mset.decrementCatchupPeer(sreq.Peer, dr.Num)
			}
			// Reset always.
			dr.First, dr.Num = 0, 0
		}

		// See if we should use LoadNextMsg instead of walking sequence by sequence if we have an order magnitude more interior deletes.
		// Only makes sense with delete range capabilities.
		useLoadNext := drOk && (uint64(state.NumDeleted) > 2*state.Msgs || state.NumDeleted > 1_000_000)

		var smv StoreMsg
		for ; seq <= last && atomic.LoadInt64(&outb) <= maxOutBytes && atomic.LoadInt32(&outm) <= maxOutMsgs && s.gcbBelowMax(); seq++ {
			var sm *StoreMsg
			var err error
			// If we should use load next do so here.
			if useLoadNext {
				var nseq uint64
				sm, nseq, err = mset.store.LoadNextMsg(fwcs, true, seq, &smv)
				if err == nil && nseq > seq {
					// If we jumped over the requested last sequence, clamp it down.
					// Otherwise, we would send too much to the follower.
					if nseq > last {
						nseq = last
						sm = nil
					}
					dr.First, dr.Num = seq, nseq-seq
					// Jump ahead
					seq = nseq
				} else if err == ErrStoreEOF {
					dr.First, dr.Num = seq, last-seq
					// Clear EOF here for normal processing.
					err = nil
					// Jump ahead
					seq = last
				}
			} else {
				sm, err = mset.store.LoadMsg(seq, &smv)
			}

			// if this is not a deleted msg, bail out.
			if err != nil && err != ErrStoreMsgNotFound && err != errDeletedMsg {
				if err == ErrStoreEOF {
					var state StreamState
					mset.store.FastState(&state)
					if seq > state.LastSeq {
						// The snapshot has a larger last sequence then we have. This could be due to a truncation
						// when trying to recover after corruption, still not 100% sure. Could be off by 1 too somehow,
						// but tested a ton of those with no success.
						s.Warnf("Catchup for stream '%s > %s' completed (took %v), but requested sequence %d was larger than current state: %+v",
							mset.account(), mset.name(), time.Since(start).Round(time.Millisecond), seq, state)
						// Try our best to redo our invalidated snapshot as well.
						if n := mset.raftNode(); n != nil {
							if snap := mset.stateSnapshot(); snap != nil {
								n.InstallSnapshot(snap, true)
							}
						}
						// If we allow gap markers check if we have one pending.
						if drOk && dr.First > 0 {
							sendDR()
						}
						// Signal EOF
						s.sendInternalMsgLocked(sendSubject, _EMPTY_, nil, nil)
						return false
					}
				}
				s.Warnf("Error loading message for catchup '%s > %s': %v", mset.account(), mset.name(), err)
				return false
			}

			if sm != nil {
				// If we allow gap markers check if we have one pending.
				if drOk && dr.First > 0 {
					sendDR()
				}
				// Send the normal message now.
				sendEM(encodeStreamMsgAllowCompress(sm.subj, _EMPTY_, sm.hdr, sm.msg, sm.seq, sm.ts, false))
			} else {
				if drOk {
					if dr.First == 0 {
						dr.First, dr.Num = seq, 1
					} else {
						dr.Num++
					}
				} else {
					// Skip record for deleted msg.
					sendEM(encodeStreamMsg(_EMPTY_, _EMPTY_, nil, nil, seq, 0, false))
				}
			}

			// Check if we are done.
			if seq == last {
				// Need to see if we have a pending delete range.
				if drOk && dr.First > 0 {
					sendDR()
				}
				s.Noticef("Catchup for stream '%s > %s' complete (took %v)", mset.account(), mset.name(), time.Since(start).Round(time.Millisecond))
				// EOF
				s.sendInternalMsgLocked(sendSubject, _EMPTY_, nil, nil)
				return false
			}
			select {
			case <-remoteQuitCh:
				return false
			case <-quit:
				return false
			default:
			}
		}
		if drOk && dr.First > 0 {
			sendDR()
		}
		return true
	}

	// Check is this stream got closed.
	mset.mu.RLock()
	qch := mset.qch
	mset.mu.RUnlock()
	if qch == nil {
		return
	}

	// Run as long as we are still active and need catchup.
	// FIXME(dlc) - Purge event? Stream delete?
	retryTimer := time.NewTimer(500 * time.Millisecond)
	defer stopAndClearTimer(&retryTimer)
	for {
		// Get this each time, will be non-nil if globally blocked and we will close to wake everyone up.
		cbKick := s.cbKickChan()

		select {
		case <-s.quitCh:
			return
		case <-qch:
			return
		case <-remoteQuitCh:
			mset.clearCatchupPeer(sreq.Peer, quit)
			return
		case <-quit:
			// Superseded by a newer request from this peer, or the peer left the group.
			return
		case <-notActive.C:
			s.Warnf("Catchup for stream '%s > %s' stalled", mset.account(), mset.name())
			// Do NOT clear the catchup peer on a transient inactivity stall, this allows the
			// follower to retry without us losing track of it requiring catchup.
			mset.detachCatchupPeer(sreq.Peer, quit)
			return
		case <-nextBatchC:
			if !sendNextBatchAndContinue(qch) {
				mset.clearCatchupPeer(sreq.Peer, quit)
				return
			}
		case <-cbKick:
			if !sendNextBatchAndContinue(qch) {
				mset.clearCatchupPeer(sreq.Peer, quit)
				return
			}
		case <-retryTimer.C:
			if !sendNextBatchAndContinue(qch) {
				mset.clearCatchupPeer(sreq.Peer, quit)
				return
			}
		}
		retryTimer.Reset(500 * time.Millisecond)
	}
}

const jscAllSubj = "$JSC.>"

func syncSubjForStream() string {
	return syncSubject("$JSC.SYNC")
}

func syncReplySubject() string {
	return syncSubject("$JSC.R")
}

func infoReplySubject() string {
	return syncSubject("$JSC.R")
}

func syncAckSubject() string {
	return syncSubject("$JSC.ACK") + ".*"
}

func syncSubject(pre string) string {
	var sb strings.Builder
	sb.WriteString(pre)
	sb.WriteByte(btsep)

	var b [replySuffixLen]byte
	rn := rand.Int64()
	for i, l := 0, rn; i < len(b); i++ {
		b[i] = digits[l%base]
		l /= base
	}

	sb.Write(b[:])
	return sb.String()
}

const (
	clusterStreamInfoT   = "$JSC.SI.%s.%s"
	clusterConsumerInfoT = "$JSC.CI.%s.%s.%s"
	jsaUpdatesSubT       = "$JSC.ARU.%s.*"
	jsaUpdatesPubT       = "$JSC.ARU.%s.%s"
)
