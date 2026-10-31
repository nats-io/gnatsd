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

package server

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"iter"
	"math"
	"math/rand/v2"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/antithesishq/antithesis-sdk-go/assert"

	"github.com/minio/highwayhash"
)

type RaftNode interface {
	Propose(term uint64, entry []byte) error
	ProposeMulti(term uint64, entries []*Entry) error
	ForwardProposal(entry []byte) error
	InstallSnapshot(snap []byte, force bool) error
	CreateSnapshotCheckpoint(force bool) (RaftNodeCheckpoint, error)
	SendSnapshot(snap []byte) error
	NeedSnapshot() bool
	Applied(index uint64) (entries uint64, bytes uint64)
	Processed(index uint64, applied uint64) (entries uint64, bytes uint64)
	State() RaftState
	Size() (entries, bytes uint64)
	Progress() (index, commit, applied uint64)
	Leader() bool
	LeaderSince() *time.Time
	Quorum() bool
	Current() bool
	Healthy() bool
	Term() uint64
	Leaderless() bool
	GroupLeader() string
	HadPreviousLeader() bool
	StepDown(preferred ...string) error
	SetObserver(isObserver bool)
	IsObserver() bool
	Campaign() error
	CampaignImmediately() error
	ID() string
	Group() string
	Peers() []*Peer
	PeerNames() []string
	ProposeKnownPeers(knownPeers []string)
	UpdateKnownPeers(knownPeers []string)
	LastHeardFromFollower(peer string) time.Time
	IsFollowerCaughtUp(peer string) bool
	ProposeAddPeer(peer string) error
	ProposeRemovePeer(peer string) error
	MembershipChangeInProgress() bool
	AdjustClusterSize(csz int) error
	AdjustBootClusterSize(csz int) error
	ClusterSize() int
	ApplyQ() *ipQueue[*CommittedEntry]
	PauseApply() error
	ResumeApply()
	DrainAndReplaySnapshot() bool
	LeadChangeC() <-chan leadChange
	QuitC() <-chan struct{}
	Created() time.Time
	Stop()
	WaitForStop()
	Delete()
	IsDeleted() bool
	Reset()
	RecreateInternalSubs() error
	IsSystemAccount() bool
	GetTrafficAccountName() string
	GetWriteErr() error
}

// RaftNodeCheckpoint is used as an alternative to a direct InstallSnapshot.
// A checkpoint is created from CreateSnapshotCheckpoint and allows installing snapshots asynchronously,
// as well as loading the last snapshot or entries between the last snapshot and the one we're about to create.
// Abort can be called to cancel the snapshot installation at any time, or InstallSnapshot to install it.
type RaftNodeCheckpoint interface {
	LoadLastSnapshot() (snap []byte, err error)
	AppendEntriesSeq() iter.Seq2[*appendEntry, error]
	Abort()
	InstallSnapshot(data []byte) (uint64, error)
}

type WAL interface {
	Type() StorageType
	StoreMsg(subj string, hdr, msg []byte, ttl int64) (uint64, int64, error)
	LoadMsg(index uint64, sm *StoreMsg) (*StoreMsg, error)
	RemoveMsg(index uint64) (bool, error)
	Compact(index uint64) (uint64, error)
	Purge() (uint64, error)
	PurgeEx(subject string, seq, keep uint64) (uint64, error)
	Truncate(seq uint64) error
	State() StreamState
	FastState(*StreamState)
	Stop() error
	Delete(inline bool) error
}

type Peer struct {
	ID      string
	Current bool
	Last    time.Time
	Lag     uint64
}

type RaftState uint8

// Allowable states for a NATS Consensus Group.
const (
	Follower RaftState = iota
	Leader
	Candidate
	Closed
)

func (state RaftState) String() string {
	switch state {
	case Follower:
		return "FOLLOWER"
	case Candidate:
		return "CANDIDATE"
	case Leader:
		return "LEADER"
	case Closed:
		return "CLOSED"
	}
	return "UNKNOWN"
}

type raft struct {
	sync.RWMutex
	// Serializes writing snapshot files, since async snapshots are written with the Raft lock released.
	// MUST be acquired while already holding the Raft lock. But the Raft lock may then be released.
	snapLock sync.Mutex

	created time.Time      // Time that the group was created
	accName string         // Account name of the asset this raft group is for
	group   string         // Raft group
	sd      string         // Store directory
	id      string         // Node ID
	wg      sync.WaitGroup // Wait for running goroutines to exit on shutdown

	dios  *diskIOSemaphore
	wal   WAL         // WAL store (filestore or memstore)
	wtype StorageType // WAL type, e.g. FileStorage or MemoryStorage
	bytes uint64      // Total amount of bytes stored in the WAL. (Saves us from needing to call wal.FastState very often)
	werr  error       // Last write error

	state       atomic.Int32              // RaftState
	leaderState atomic.Bool               // Is in (complete) leader state.
	leaderSince atomic.Pointer[time.Time] // How long since becoming leader.
	hh          *highwayhash.Digest64     // Highwayhash, used for snapshots
	snapfile    string                    // Snapshot filename

	csz   int             // Cluster size
	qn    int             // Number of nodes needed to establish quorum
	peers map[string]*lps // Other peers in the Raft group

	removed  map[string]time.Time           // Peers that were removed from the group
	acks     map[uint64]map[string]struct{} // Append entry responses/acks, map of entry index -> peer ID
	pae      map[uint64]*appendEntry        // Pending append entries
	paeBytes uint64                         // Total byte size of the pending append entries in pae

	elect  *time.Timer // Election timer, normally accessed via electTimer
	etlr   time.Time   // Election timer last reset time, for unit tests only
	active time.Time   // Last activity time, i.e. for heartbeats
	llqrt  time.Time   // Last quorum lost time
	lsut   time.Time   // Last scale-up time
	nlt    time.Time   // No leader time, since when we've been without a leader
	lgt    time.Time   // Leader grace time, since when we're healthy while the leader changes

	term      uint64 // The current vote term
	pterm     uint64 // Previous term from the last snapshot
	pindex    uint64 // Previous index from the last snapshot
	commit    uint64 // Index of the most recent commit
	processed uint64 // Index of the most recently processed commit
	applied   uint64 // Index of the most recently applied commit
	papplied  uint64 // First sequence of our log, matches when we last installed a snapshot.

	membChange *membChange // Uncommitted membership change entry at a specific log index.

	aflr uint64 // Index when to signal initial messages have been applied after becoming leader. 0 means signaling is disabled.

	leader string // The ID of the leader
	vote   string // Our current vote state

	s  *Server    // Reference to top-level server
	js *jetStream // JetStream, if running, to see if we are out of resources

	hasleader atomic.Bool // Is there a group leader right now?
	pleader   atomic.Bool // Has the group ever had a leader?
	isSysAcc  atomic.Bool // Are we utilizing the system account?

	extSt extensionState // Extension state

	track bool // Whether out of resources checking is enabled.
	dflag bool // Debug flag

	psubj  string // Proposals subject
	rpsubj string // Remove peers subject
	vsubj  string // Vote requests subject
	vreply string // Vote responses subject
	asubj  string // Append entries subject
	areply string // Append entries responses subject

	t     raftTransport // Transport that handles Raft messaging
	aesub *subscription // Subscription for handleAppendEntry callbacks

	wtv []byte // Term and vote to be written
	wps []byte // Peer state to be written

	catchup  *catchupState               // For when we need to catch up as a follower.
	progress map[string]*ipQueue[uint64] // For leader or server catching up a follower.

	hcommit uint64 // The commit at the time that applies were paused
	hcterm  uint64 // Term in which hcindex was recorded
	hcindex uint64 // Leader's last index when we first heard from it in hcterm, must be processed to be healthy

	prop  *ipQueue[*proposedEntry]       // Proposals
	entry *ipQueue[*appendEntry]         // Append entries
	resp  *ipQueue[*appendEntryResponse] // Append entries responses
	apply *ipQueue[*CommittedEntry]      // Apply queue (committed entries to be passed to upper layer)
	reqs  *ipQueue[*voteRequest]         // Vote requests
	votes *ipQueue[*voteResponse]        // Vote responses
	leadc chan leadChange                // Leader changes
	quit  chan struct{}                  // Raft group shutdown

	lxfer        bool // Are we doing a leadership transfer?
	hcbehind     bool // Were we falling behind at the last health check? (see: isCurrent)
	maybeLeader  bool // The group had a preferred leader. And is maybe already acting as leader prior to scale up.
	paused       bool // Whether or not applies are paused
	observer     bool // The node is observing, i.e. not able to become leader
	initializing bool // The node is new, and "empty log" checks can be temporarily relaxed.
	scaleUp      bool // The node is part of a scale up, puts us in observer mode until the log contains data.
	deleted      bool // If the node was deleted.
	snapshotting bool // Snapshot is in progress.
	quorumPaused bool // Pause replication and quorum participation to prevent log growth during slow applies.

	overrunCount uint64 // Counter of how many times we were overrun, either as follower or as leader.
}

type proposedEntry struct {
	*Entry
	reply string // Optional, to respond once proposal handled
}

// catchupState structure that holds our subscription, and catchup term and index
// as well as starting term and index and how many updates we have seen.
type catchupState struct {
	sub    *subscription // Subscription that catchup messages will arrive on
	cterm  uint64        // Catchup term
	cindex uint64        // Catchup index
	pterm  uint64        // Starting term
	pindex uint64        // Starting index
	active time.Time     // Last time we received a message for this catchup
	signal bool          // Whether the EntryCatchup signal was sent.
}

// lps holds peer state of last time and last index replicated.
type lps struct {
	ts time.Time // Last timestamp
	li uint64    // Last index replicated
	ci uint64    // Index the follower must reach to be caught up
}

type membChange struct {
	index uint64 // Which entry in the log is the membership change.
	peer  string // The peer whose membership is being changed.
	prev  *lps   // The previous membership state if removing, unset if adding.
}

const (
	minElectionTimeoutDefault      = 4 * time.Second
	maxElectionTimeoutDefault      = 9 * time.Second
	minCampaignTimeoutDefault      = 100 * time.Millisecond
	maxCampaignTimeoutDefault      = 8 * minCampaignTimeoutDefault
	hbIntervalDefault              = 1 * time.Second
	lostQuorumIntervalDefault      = hbIntervalDefault * 10 // 10 seconds
	lostQuorumCheckIntervalDefault = hbIntervalDefault * 10 // 10 seconds
	observerModeIntervalDefault    = 48 * time.Hour
	peerRemoveTimeoutDefault       = 5 * time.Minute
	leaderlessHealthyDefault       = time.Minute
)

var (
	minElectionTimeout   = minElectionTimeoutDefault
	maxElectionTimeout   = maxElectionTimeoutDefault
	minCampaignTimeout   = minCampaignTimeoutDefault
	maxCampaignTimeout   = maxCampaignTimeoutDefault
	hbInterval           = hbIntervalDefault
	lostQuorumInterval   = lostQuorumIntervalDefault
	lostQuorumCheck      = lostQuorumCheckIntervalDefault
	observerModeInterval = observerModeIntervalDefault
	peerRemoveTimeout    = peerRemoveTimeoutDefault
	leaderlessHealthy    = leaderlessHealthyDefault
)

type RaftConfig struct {
	Name     string
	Store    string
	Log      WAL
	Track    bool
	Observer bool

	// Recovering must be set for a Raft group that's recovering after a restart, or if it's
	// first seen after a catchup from another server. If a server recovers with an empty log,
	// we know to protect against data loss.
	Recovering bool

	// ScaleUp identifies the Raft peer set is being scaled up.
	// We need to protect against losing state due to the new peers starting with an empty log.
	// Therefore, these empty servers can't try to become leader until they at least have _some_ state.
	ScaleUp bool

	// NewTransport creates the transport used for Raft node communication.
	// This is mainly for tests to inject a custom transport.
	// If nil, the default transport is used.
	NewTransport newTransportFunc
}

var (
	errNotLeader         = errors.New("raft: not leader")
	errAlreadyLeader     = errors.New("raft: already leader")
	errNilCfg            = errors.New("raft: no config given")
	errCorruptPeers      = errors.New("raft: corrupt peer state")
	errEntryLoadFailed   = errors.New("raft: could not load entry from WAL")
	errEntryStoreFailed  = errors.New("raft: could not store entry to WAL")
	errNodeClosed        = errors.New("raft: node is closed")
	errNodeRemoved       = errors.New("raft: peer was removed")
	errBadSnapName       = errors.New("raft: snapshot name could not be parsed")
	errNoSnapAvailable   = errors.New("raft: no snapshot available")
	errSnapInProgress    = errors.New("raft: snapshot is already in progress")
	errSnapAborted       = errors.New("raft: snapshot was aborted")
	errCatchupsRunning   = errors.New("raft: snapshot can not be installed while catchups running")
	errSnapshotCorrupt   = errors.New("raft: snapshot corrupt")
	errTooManyPrefs      = errors.New("raft: stepdown requires at most one preferred new leader")
	errNoPeerState       = errors.New("raft: no peerstate")
	errAdjustBootCluster = errors.New("raft: can not adjust boot peer size on established group")
	errLeaderLen         = fmt.Errorf("raft: leader should be exactly %d bytes", idLen)
	errTooManyEntries    = errors.New("raft: append entry can contain a max of 64k entries")
	errBadAppendEntry    = errors.New("raft: append entry corrupt")
	errNoInternalClient  = errors.New("raft: no internal client")
	errMembershipChange  = errors.New("raft: membership change in progress")
	errRemoveLastNode    = errors.New("raft: cannot remove the last peer")
	errPeerNotFound      = errors.New("raft: peer not found")
)

// This will bootstrap a raftNode by writing its config into the store directory.
func (s *Server) bootstrapRaftNode(cfg *RaftConfig, knownPeers []string, allPeersKnown bool) error {
	if cfg == nil {
		return errNilCfg
	}
	// Check validity of peers if presented.
	for _, p := range knownPeers {
		if len(p) != idLen {
			return fmt.Errorf("raft: illegal peer: %q", p)
		}
	}
	expected := len(knownPeers)
	// We need to adjust this is all peers are not known.
	if !allPeersKnown {
		s.Debugf("Determining expected peer size for JetStream meta group")
		if expected < 2 {
			expected = 2
		}
		opts := s.getOpts()
		nrs := len(opts.Routes)

		cn := s.ClusterName()
		ngwps := 0
		for _, gw := range opts.Gateway.Gateways {
			// Ignore our own cluster if specified.
			if gw.Name == cn {
				continue
			}
			// Each configured gateway URL represents one remote endpoint, so
			// count it as a single peer. We must not resolve the host and add
			// one per returned address: a hostname on a dual-stack host (e.g.
			// "localhost" -> 127.0.0.1 + ::1) would then count the same server
			// multiple times, inflating the expected meta-group size above the
			// real node count and preventing meta leader election.
			ngwps += len(gw.URLs)
		}

		if expected < nrs+ngwps {
			expected = nrs + ngwps
			s.Debugf("Adjusting expected peer set size to %d with %d known", expected, len(knownPeers))
		}
	}

	// Check the store directory. If we have a memory based WAL we need to make sure the directory is setup.
	if stat, err := os.Stat(cfg.Store); os.IsNotExist(err) {
		if err := os.MkdirAll(cfg.Store, defaultDirPerms); err != nil {
			return fmt.Errorf("raft: could not create storage directory - %v", err)
		}
	} else if stat == nil || !stat.IsDir() {
		return fmt.Errorf("raft: storage directory is not a directory")
	}
	tmpfile, err := os.CreateTemp(cfg.Store, "_test_")
	if err != nil {
		return fmt.Errorf("raft: storage directory is not writable")
	}
	tmpfile.Close()
	os.Remove(tmpfile.Name())

	return writePeerState(s.diskIOSemaphore(), cfg.Store, &peerState{knownPeers, expected, extUndetermined})
}

// initRaftNode will initialize the raft node, to be used by startRaftNode or when testing to not run the Go routine.
func (s *Server) initRaftNode(accName string, cfg *RaftConfig, labels pprofLabels) (*raft, error) {
	restorePeerState := func(n *raft) error {
		ps, err := readPeerState(s.diskIOSemaphore(), cfg.Store)
		if err != nil {
			return err
		}
		if ps == nil {
			return errNoPeerState
		}
		n.processPeerState(ps)
		n.extSt = ps.domainExt
		return nil
	}

	if cfg == nil {
		return nil, errNilCfg
	}
	s.mu.RLock()
	if s.sys == nil {
		s.mu.RUnlock()
		return nil, ErrNoSysAccount
	}
	hash := s.sys.shash
	s.mu.RUnlock()

	qpfx := fmt.Sprintf("[ACC:%s] RAFT '%s' ", accName, cfg.Name)
	now := time.Now()
	n := &raft{
		created:  now,
		nlt:      now,
		id:       hash[:idLen],
		group:    cfg.Name,
		sd:       cfg.Store,
		wal:      cfg.Log,
		wtype:    cfg.Log.Type(),
		dios:     s.diskIOSemaphore(),
		track:    cfg.Track,
		peers:    make(map[string]*lps),
		acks:     make(map[uint64]map[string]struct{}),
		pae:      make(map[uint64]*appendEntry),
		s:        s,
		js:       s.getJetStream(),
		quit:     make(chan struct{}),
		reqs:     newIPQueue[*voteRequest](s, qpfx+"vreq"),
		votes:    newIPQueue[*voteResponse](s, qpfx+"vresp"),
		prop:     newIPQueue[*proposedEntry](s, qpfx+"entry"),
		entry:    newIPQueue[*appendEntry](s, qpfx+"appendEntry"),
		resp:     newIPQueue[*appendEntryResponse](s, qpfx+"appendEntryResponse"),
		apply:    newIPQueue[*CommittedEntry](s, qpfx+"committedEntry"),
		accName:  accName,
		leadc:    make(chan leadChange, 1),
		observer: cfg.Observer,
	}

	if cfg.NewTransport != nil {
		n.t = cfg.NewTransport(s, n)
	} else {
		n.t = defaultRaftTransport(s, n)
	}

	// Setup our internal subscriptions for proposals, votes and append entries.
	// If we fail to do this for some reason then this is fatal — we cannot
	// continue setting up or the Raft node may be partially/totally isolated.
	if err := n.RecreateInternalSubs(); err != nil {
		n.shutdown()
		return nil, err
	}

	if atomic.LoadInt32(&s.logging.debug) > 0 {
		n.dflag = true
	}

	// Set up the highwayhash for the snapshots.
	key := sha256.Sum256([]byte(n.group))
	n.hh, _ = highwayhash.NewDigest64(key[:])

	// If we have a term and vote file (tav.idx on the filesystem) then read in
	// what we think the term and vote was. It's possible these are out of date
	// so a catch-up may be required.
	if term, vote, err := n.readTermVote(); err == nil && term > 0 {
		n.term = term
		n.vote = vote
	}

	// Can't recover snapshots if memory based since wal will be reset.
	// We will inherit from the current leader.
	n.papplied = 0
	if _, ok := n.wal.(*memStore); ok {
		_ = os.RemoveAll(filepath.Join(n.sd, snapshotsDir))
	} else if err := n.setupLastSnapshot(); err != nil && err != errNoSnapAvailable {
		// If we failed to recover from the snapshot, then we should surface
		// the error upwards, otherwise we can complete recovery but have only
		// a partial view of the world.
		n.shutdown()
		return nil, err
	}

	// We may have restored the peer state from the
	// snapshot above. If not, we restore peers from
	// the peer state file.
	if len(n.peers) == 0 {
		if err := restorePeerState(n); err != nil {
			return nil, err
		}
	}

	// Make sure that the snapshots directory exists.
	if err := os.MkdirAll(filepath.Join(n.sd, snapshotsDir), defaultDirPerms); err != nil {
		n.shutdown()
		return nil, fmt.Errorf("could not create snapshots directory - %v", err)
	}

	truncateAndErr := func(index uint64) {
		if err := n.wal.Truncate(index); err != nil {
			n.setWriteErr(err)
		}
	}

	// Retrieve the stream state from the WAL. If there are pending append
	// entries that were committed but not applied before we last shut down,
	// we will try to replay them and process them here.
	var state StreamState
	n.wal.FastState(&state)
	n.bytes = state.Bytes

	if state.Msgs > 0 {
		n.debug("Replaying state of %d entries", state.Msgs)

		// This process will queue up entries on our applied queue but prior to the upper
		// state machine running. So we will monitor how much we have queued and if we
		// reach a limit will pause the apply queue and resume inside of run() go routine.
		const maxQsz = 32 * 1024 * 1024 // 32MB max

		// It looks like there are entries we have committed but not applied
		// yet. Replay them.
		for index, qsz := state.FirstSeq, 0; index <= state.LastSeq; index++ {
			ae, err := n.loadEntry(index)
			// The first entry in our WAL initializes state but must align with our snapshot if we had one.
			// Importantly, check this first, as we might need to truncate the WAL further than the index.
			if index == state.FirstSeq {
				// If the entry is missing, corrupt, or doesn't align with the snapshot, truncate the WAL.
				if err != nil || ae == nil || ae.pindex != index-1 || n.pindex != ae.pindex {
					if err != nil {
						n.warn("Could not load %d from WAL [%+v]: %v", index, state, err)
					} else {
						n.warn("Misaligned WAL, will truncate")
					}
					// Truncate to the snapshot or beginning if there is none.
					truncateAndErr(n.pindex)
					break
				}
				n.pterm, n.pindex = ae.pterm, ae.pindex
				if ae.commit > 0 && ae.commit > n.commit {
					n.commit = ae.commit
				}
			}
			if err != nil {
				n.warn("Could not load %d from WAL [%+v]: %v", index, state, err)
				// Truncate to the previous correct entry.
				truncateAndErr(index - 1)
				break
			}
			if ae.pindex != index-1 {
				n.warn("Corrupt WAL, will truncate")
				// Truncate to the previous correct entry.
				truncateAndErr(index - 1)
				break
			}
			n.processAppendEntry(ae, nil)
			// Check how much we have queued up so far to determine if we should pause.
			for _, e := range ae.entries {
				qsz += len(e.Data)
				if qsz > maxQsz && !n.paused {
					n.PauseApply()
				}
			}
		}
	}

	n.debug("Started (cluster size %d, quorum %d)", n.csz, n.qn)

	// Check if we need to start in observer mode due to lame duck status.
	// This will stop us from taking on the leader role when we're about to
	// shutdown anyway.
	if s.isLameDuckMode() {
		n.debug("Will start in observer mode due to lame duck status")
		n.SetObserver(true)
	}

	// Set the election timer and lost quorum timers to now, so that we
	// won't accidentally trigger either state without knowing the real state
	// of the other nodes.
	n.Lock()
	n.resetElectionTimeout()
	n.llqrt = time.Now()

	// If our log is empty, and we're initializing, relax the "empty log" checks temporarily.
	if !cfg.Recovering && n.pindex == 0 {
		n.initializing = true
		// If we're scaling up and our log is empty, must put ourselves into observer
		// and wait for data from the leader.
		if !cfg.Observer && cfg.ScaleUp {
			n.scaleUp = true
			n.setObserverLocked(true, extUndetermined)
		}
	}
	n.Unlock()

	// Register the Raft group.
	labels["group"] = n.group
	s.registerRaftNode(n.group, n)

	return n, nil
}

// startRaftNode will start the raft node.
func (s *Server) startRaftNode(accName string, cfg *RaftConfig, labels pprofLabels) (RaftNode, error) {
	n, err := s.initRaftNode(accName, cfg, labels)
	if err != nil {
		return nil, err
	}

	// Start the run goroutine for the Raft state machine.
	n.wg.Add(1)
	s.startGoRoutine(n.run, labels)

	return n, nil
}

// Returns whether peers within this group claim to support
// moving NRG traffic into the asset account.
// Lock must be held.
func (n *raft) checkAccountNRGStatus() bool {
	if !n.s.accountNRGAllowed.Load() {
		return false
	}
	enabled := true
	for pn := range n.peers {
		if si, ok := n.s.nodeToInfo.Load(pn); ok && si != nil {
			enabled = enabled && si.(nodeInfo).accountNRG
		}
	}
	return enabled
}

// Whether we are using the system account or not.
func (n *raft) IsSystemAccount() bool {
	return n.isSysAcc.Load()
}

// GetTrafficAccountName returns the account name of the account used for replication traffic.
func (n *raft) GetTrafficAccountName() string {
	n.RLock()
	defer n.RUnlock()
	if n.t == nil {
		return (*Account)(nil).GetName()
	}
	return n.t.Account().GetName()
}

func (n *raft) RecreateInternalSubs() error {
	n.Lock()
	defer n.Unlock()
	return n.recreateInternalSubsLocked()
}

func (n *raft) recreateInternalSubsLocked() error {
	// Sanity check for system account, as it can disappear when
	// the system is shutting down.
	if n.s == nil {
		return fmt.Errorf("server not found")
	}
	n.s.mu.RLock()
	sys := n.s.sys
	n.s.mu.RUnlock()
	if sys == nil {
		return fmt.Errorf("system account not found")
	}

	// Default is the system account.
	nrgAcc := sys.account
	n.isSysAcc.Store(true)

	// Is account NRG enabled in this account and do all group
	// peers claim to also support account NRG?
	if n.checkAccountNRGStatus() {
		// Check whether the account that the asset belongs to
		// has volunteered a different NRG account.
		target := nrgAcc.Name
		if a, _ := n.s.lookupAccount(n.accName); a != nil {
			a.mu.RLock()
			if a.js != nil {
				target = a.nrgAccount
			}
			a.mu.RUnlock()
		}

		// If the target account exists, then we'll use that.
		if target != _EMPTY_ {
			if a, _ := n.s.lookupAccount(target); a != nil {
				nrgAcc = a
				if a != sys.account {
					n.isSysAcc.Store(false)
				}
			}
		}
	}
	if n.aesub != nil && n.t.Account() == nrgAcc {
		// Subscriptions already exist and the account NRG state
		// hasn't changed.
		return nil
	}

	// Need to cancel any in-progress catch-ups, otherwise the
	// inboxes are about to be pulled out from underneath it in
	// the next step...
	n.cancelCatchup()

	if n.t.Account() != nrgAcc {
		n.debug("Subscribing in '%s'", nrgAcc.GetName())
	}

	n.t.Reset(nrgAcc)

	// Recreate any internal subscriptions for voting, append
	// entries etc in the new account.
	return n.createInternalSubs()
}

// outOfResources checks to see if we are out of resources.
func (n *raft) outOfResources() bool {
	js := n.js
	if !n.track || js == nil {
		return false
	}
	return js.limitsExceeded(n.wtype)
}

// Maps node names back to server names.
func (s *Server) serverNameForNode(node string) string {
	if si, ok := s.nodeToInfo.Load(node); ok && si != nil {
		return si.(nodeInfo).name
	}
	return _EMPTY_
}

// Maps node names back to cluster names.
func (s *Server) clusterNameForNode(node string) string {
	if si, ok := s.nodeToInfo.Load(node); ok && si != nil {
		return si.(nodeInfo).cluster
	}
	return _EMPTY_
}

// Registers the Raft node with the server, as it will track all of the Raft
// nodes.
func (s *Server) registerRaftNode(group string, n RaftNode) {
	s.rnMu.Lock()
	defer s.rnMu.Unlock()
	if s.raftNodes == nil {
		s.raftNodes = make(map[string]RaftNode)
	}
	s.raftNodes[group] = n
}

// Unregisters the Raft node from the server, i.e. at shutdown.
func (s *Server) unregisterRaftNode(group string) {
	s.rnMu.Lock()
	defer s.rnMu.Unlock()
	if s.raftNodes != nil {
		delete(s.raftNodes, group)
	}
}

// Returns how many Raft nodes are running in this server instance.
func (s *Server) numRaftNodes() int {
	s.rnMu.RLock()
	defer s.rnMu.RUnlock()
	return len(s.raftNodes)
}

// Finds the Raft node for a given Raft group, if any. If there is no Raft node
// running for this group then it can return nil.
func (s *Server) lookupRaftNode(group string) RaftNode {
	s.rnMu.RLock()
	defer s.rnMu.RUnlock()
	var n RaftNode
	if s.raftNodes != nil {
		n = s.raftNodes[group]
	}
	return n
}

// Reloads the debug state for all running Raft nodes. This is necessary when
// the configuration has been reloaded and the debug log level has changed.
func (s *Server) reloadDebugRaftNodes(debug bool) {
	if s == nil {
		return
	}
	s.rnMu.RLock()
	for _, ni := range s.raftNodes {
		n := ni.(*raft)
		n.Lock()
		n.dflag = debug
		n.Unlock()
	}
	s.rnMu.RUnlock()
}

// Requests that all Raft nodes on this server step down and place them into
// observer mode. This is called when the server is shutting down.
func (s *Server) stepdownRaftNodes() {
	if s == nil {
		return
	}
	s.rnMu.RLock()
	if len(s.raftNodes) == 0 {
		s.rnMu.RUnlock()
		return
	}
	s.Debugf("Stepping down all leader raft nodes")
	nodes := make([]RaftNode, 0, len(s.raftNodes))
	for _, n := range s.raftNodes {
		nodes = append(nodes, n)
	}
	s.rnMu.RUnlock()

	for _, node := range nodes {
		node.StepDown()
		node.SetObserver(true)
	}
}

// Shuts down all Raft nodes on this server. This is called either when the
// server is either entering lame duck mode, shutting down or when JetStream
// has been disabled.
func (s *Server) shutdownRaftNodes() {
	if s == nil {
		return
	}
	s.rnMu.RLock()
	if len(s.raftNodes) == 0 {
		s.rnMu.RUnlock()
		return
	}
	nodes := make([]RaftNode, 0, len(s.raftNodes))
	s.Debugf("Shutting down all raft nodes")
	for _, n := range s.raftNodes {
		nodes = append(nodes, n)
	}
	s.rnMu.RUnlock()

	for _, node := range nodes {
		node.Stop()
	}
}

// Used in lameduck mode to move off the leaders.
// We also put all nodes in observer mode so new leaders
// can not be placed on this server.
func (s *Server) transferRaftLeaders() bool {
	if s == nil {
		return false
	}
	s.rnMu.RLock()
	if len(s.raftNodes) == 0 {
		s.rnMu.RUnlock()
		return false
	}
	nodes := make([]RaftNode, 0, len(s.raftNodes))
	for _, n := range s.raftNodes {
		nodes = append(nodes, n)
	}
	s.rnMu.RUnlock()

	var didTransfer bool
	for _, node := range nodes {
		if err := node.StepDown(); err == nil {
			didTransfer = true
		}
		node.SetObserver(true)
	}
	return didTransfer
}

// Formal API

// Propose will propose a new entry to the group.
// This should only be called on the leader.
func (n *raft) Propose(term uint64, data []byte) error {
	n.Lock()
	defer n.Unlock()
	return n.proposeLocked(term, data)
}

func (n *raft) proposeLocked(term uint64, data []byte) error {
	// Check state under lock, we might not be leader anymore.
	if state := n.State(); state != Leader || term != n.term {
		n.debug("Proposal ignored, not leader (state: %v, cterm: %d, term: %d)", state, term, n.term)
		return errNotLeader
	}

	// Error if we had a previous write error.
	if werr := n.werr; werr != nil {
		return werr
	}

	if n.isLeaderOverrun() {
		var state StreamState
		n.wal.FastState(&state)
		n.warn("Leader falling behind, stepping down: pindex %d, commit %d, applied %d, WAL size %s", n.pindex, n.commit, n.applied, friendlyBytes(state.Bytes))
		// Stepdown without leader transfer, likely all replicas will be overrun, and we need time to recover.
		n.stepdownLocked(noLeader)
		n.overrunCount++
		return errNotLeader
	}
	n.prop.push(newProposedEntry(newEntry(EntryNormal, data), _EMPTY_))
	return nil
}

// ProposeMulti will propose multiple entries at once.
// This should only be called on the leader.
func (n *raft) ProposeMulti(term uint64, entries []*Entry) error {
	n.Lock()
	defer n.Unlock()
	// Check state under lock, we might not be leader anymore.
	if state := n.State(); state != Leader || term != n.term {
		n.debug("Multi proposal ignored, not leader (state: %v, cterm: %d, term: %d)", state, term, n.term)
		return errNotLeader
	}

	// Error if we had a previous write error.
	if werr := n.werr; werr != nil {
		return werr
	}

	if n.isLeaderOverrun() {
		var state StreamState
		n.wal.FastState(&state)
		n.warn("Leader falling behind, stepping down: pindex %d, commit %d, applied %d, WAL size %s", n.pindex, n.commit, n.applied, friendlyBytes(state.Bytes))
		// Stepdown without leader transfer, likely all replicas will be overrun, and we need time to recover.
		n.stepdownLocked(noLeader)
		n.overrunCount++
		return errNotLeader
	}
	for _, e := range entries {
		n.prop.push(newProposedEntry(e, _EMPTY_))
	}
	return nil
}

// isLeaderOverrun returns whether we are overrun and should step down due to continuously increasing
// uncommitted or unapplied entries. If triggered, this means we're being severely overrun by
// incoming proposals or the system is degraded such that it's too slow (or unable) to process them.
// Stepping down means the system gets to "breathe" for a bit, until a new leader can be elected.
// Lock should be held.
func (n *raft) isLeaderOverrun() bool {
	applied := max(n.applied, n.papplied)
	commit := max(n.commit, n.papplied)
	// We only do this past a high threshold to protect ourselves.
	// Worst-case we'll have 2x the threshold, once in uncommitted and once in unapplied entries.
	// Either the number of uncommitted entries is over the threshold: we're not getting quorum from our followers.
	uncommittedThreshold := n.pindex > commit && n.overrun(n.pindex-commit, pauseQuorumThreshold, pauseQuorumBytes)
	// Or, the number of in-memory committed but not yet applied entries is over the threshold: we're slow to apply.
	unappliedThreshold := commit > applied && n.overrun(commit-applied, pauseQuorumThreshold, pauseQuorumBytes)
	return uncommittedThreshold || unappliedThreshold
}

// entryBytes estimates the byte size of the given number of entries, based on the
// average size of the entries we are currently holding in our WAL.
// Lock should be held.
func (n *raft) entryBytes(entries uint64) uint64 {
	msgs := n.pindex - n.papplied
	if msgs == 0 || n.bytes == 0 {
		return 0
	}
	return entries * n.bytes / msgs
}

// overrun returns whether the given number of entries is over either the entry count
// threshold or the estimated byte threshold.
// Lock should be held.
func (n *raft) overrun(entries, maxEntries, maxBytes uint64) bool {
	return entries > maxEntries || n.entryBytes(entries) > maxBytes
}

// ForwardProposal will forward the proposal to the leader if known.
// If we are the leader this is the same as calling propose.
func (n *raft) ForwardProposal(entry []byte) error {
	if n.State() == Leader {
		n.Lock()
		defer n.Unlock()
		// We pass the node's term so the proposal goes through. This is unavoidable with forwarded
		// proposals, normally the passed term MUST be that of the process triggering the proposals.
		// So a stale process that is still running isn't allowed to make new proposals past its term.
		return n.proposeLocked(n.term, entry)
	}

	// TODO: Currently we do not set a reply subject, even though we are
	// now capable of responding. Do this once enough time has passed,
	// i.e. maybe in 2.12.
	n.sendRPC(n.psubj, _EMPTY_, entry)
	return nil
}

// ProposeAddPeer is called to add a peer to the group.
func (n *raft) ProposeAddPeer(peer string) error {
	n.RLock()
	// Check state under lock, we might not be leader anymore.
	if n.State() != Leader {
		n.RUnlock()
		return errNotLeader
	}
	// Error if we had a previous write error.
	if werr := n.werr; werr != nil {
		n.RUnlock()
		return werr
	}
	if n.membChange != nil {
		n.RUnlock()
		return errMembershipChange
	}
	prop := n.prop
	n.RUnlock()

	prop.push(newProposedEntry(newEntry(EntryAddPeer, []byte(peer)), _EMPTY_))
	return nil
}

// ProposeRemovePeer is called to remove a peer from the group.
func (n *raft) ProposeRemovePeer(peer string) error {
	n.RLock()

	// Error if we had a previous write error.
	if werr := n.werr; werr != nil {
		n.RUnlock()
		return werr
	}

	if n.State() != Leader {
		subj := n.rpsubj
		n.RUnlock()

		// Forward the proposal to the leader
		n.sendRPC(subj, _EMPTY_, []byte(peer))
		return nil
	}

	if n.membChange != nil {
		n.RUnlock()
		return errMembershipChange
	}
	if _, ok := n.peers[peer]; !ok {
		n.RUnlock()
		return errPeerNotFound
	}
	if len(n.peers) <= 1 {
		n.RUnlock()
		return errRemoveLastNode
	}

	prop := n.prop
	n.RUnlock()

	prop.push(newProposedEntry(newEntry(EntryRemovePeer, []byte(peer)), _EMPTY_))
	return nil
}

func (n *raft) MembershipChangeInProgress() bool {
	n.RLock()
	defer n.RUnlock()
	return n.membChange != nil
}

// ClusterSize reports back the total cluster size.
// This effects quorum etc.
func (n *raft) ClusterSize() int {
	n.Lock()
	defer n.Unlock()
	return n.csz
}

// AdjustBootClusterSize can be called to adjust the boot cluster size.
// Will error if called on a group with a leader or a previous leader.
// This can be helpful in mixed mode.
func (n *raft) AdjustBootClusterSize(csz int) error {
	n.Lock()
	defer n.Unlock()

	if n.leader != noLeader || n.pleader.Load() {
		return errAdjustBootCluster
	}
	// Same floor as bootstrap.
	if csz < 2 {
		csz = 2
	}
	// Adjust the cluster size and the number of nodes needed to establish
	// a quorum.
	n.csz = csz
	n.qn = n.csz/2 + 1

	return nil
}

// AdjustClusterSize will change the cluster set size.
// Must be the leader.
func (n *raft) AdjustClusterSize(csz int) error {
	n.Lock()
	defer n.Unlock()

	// Check state under lock, we might not be leader anymore.
	if n.State() != Leader {
		return errNotLeader
	}
	// Same floor as bootstrap.
	if csz < 2 {
		csz = 2
	}

	// Adjust the cluster size and the number of nodes needed to establish
	// a quorum.
	n.csz = csz
	n.qn = n.csz/2 + 1

	n.sendPeerState()
	return nil
}

// PauseApply will allow us to pause processing of append entries onto our
// external apply queue. In effect this means that the upper layer will no longer
// receive any new entries from the Raft group.
func (n *raft) PauseApply() error {
	if n.State() == Leader {
		return errAlreadyLeader
	}
	n.Lock()
	defer n.Unlock()
	n.pauseApplyLocked()
	return nil
}

func (n *raft) pauseApplyLocked() {
	// If we are currently not a follower, make sure we step down.
	if n.State() != Follower {
		n.stepdownLocked(noLeader)
	}

	n.debug("Pausing our apply channel")
	n.paused = true
	if n.hcommit < n.commit {
		n.hcommit = n.commit
	}
	// Also prevent us from trying to become a leader while paused and catching up.
	n.resetElect(observerModeInterval)
}

// ResumeApply will resume sending applies to the external apply queue. This
// means that we will start sending new entries to the upper layer.
func (n *raft) ResumeApply() {
	n.Lock()
	defer n.Unlock()

	if !n.paused {
		return
	}

	n.debug("Resuming our apply channel")

	// Reset before we start.
	n.resetElectionTimeout()

	// Run catchup..
	if n.hcommit > n.commit {
		n.debug("Resuming %d replays", n.hcommit+1-n.commit)
		for index := n.commit + 1; index <= n.hcommit; index++ {
			if err := n.applyCommit(index); err != nil {
				n.warn("Got error on apply commit during replay: %v", err)
				break
			}
			// We want to unlock here to allow the upper layers to call Applied() without blocking.
			n.Unlock()
			// Give hint to let other Go routines run.
			// Might not be necessary but seems to make it more fine grained interleaving.
			runtime.Gosched()
			// Simply re-acquire
			n.Lock()
			// Need to check if we got closed.
			if n.State() == Closed {
				return
			}
		}
	}

	// Commits held while paused must be processed before being healthy again.
	n.hcindex = max(n.hcindex, n.hcommit)

	// Clear our paused state after we apply.
	n.paused = false
	n.hcommit = 0

	// If we had been selected to be the next leader campaign here now that we have resumed.
	if n.lxfer {
		n.xferCampaign()
	} else {
		n.resetElectionTimeout()
	}
}

// DrainAndReplaySnapshot will drain the apply queue and replay the snapshot.
// Our highest known commit will be preserved by pausing applies. The caller
// should make sure to call ResumeApply() when handling the snapshot from the
// queue, which will populate the rest of the committed entries in the queue.
func (n *raft) DrainAndReplaySnapshot() bool {
	n.Lock()
	defer n.Unlock()
	// If the node was deleted, we can't replay the snapshot.
	if n.deleted {
		return false
	}
	snap, err := n.loadLastSnapshot()
	if err != nil {
		return false
	}
	n.warn("Draining and replaying snapshot")
	n.pauseApplyLocked()
	n.apply.drain()
	// Cancel after draining, we might have sent EntryCatchup and need to get them the nil entry.
	n.cancelCatchup()
	n.commit = snap.lastIndex
	n.apply.push(newCommittedEntry(n.commit, []*Entry{{EntrySnapshot, snap.data}}))
	return true
}

// Applied is a callback that must be called by the upper layer when it
// has successfully applied the committed entries that it received from the
// apply queue. It will return the number of entries and an estimation of the
// byte size that could be removed with a snapshot/compact.
func (n *raft) Applied(index uint64) (entries uint64, bytes uint64) {
	return n.Processed(index, index)
}

// Processed is a callback that must be called by the upper layer when it
// has processed the committed entries that it received from the apply queue,
// but it (maybe) hasn't applied all the processed entries yet.
// Used to indicate a commit was processed, even if it wasn't applied yet and
// can't be compacted away by a snapshot just yet. Which allows us to try to
// become leader if we've processed all commits, even if they're not all applied.
func (n *raft) Processed(index uint64, applied uint64) (entries uint64, bytes uint64) {
	n.Lock()
	defer n.Unlock()

	// Ignore if not applicable. This can happen during a reset.
	if index > n.commit {
		return 0, 0
	}

	// Ignore if already processed.
	if index > n.processed {
		n.processed = index
	}

	// Ignore if already applied.
	if applied > index {
		applied = index
	}
	if applied > n.applied {
		n.applied = applied
	}

	// If it was set, and we reached the minimum processed index, reset and send signal to upper layer.
	// We're not waiting for processed AND applied, because applying could take longer.
	if n.aflr > 0 && n.processed >= n.aflr {
		n.aflr = 0
		// Quick sanity-check to confirm we're still leader.
		// In which case we must signal, since switchToLeader would not have done so already.
		if n.State() == Leader {
			if !n.leaderState.Swap(true) {
				// Only update timestamp if leader state actually changed.
				nowts := time.Now().UTC()
				n.leaderSince.Store(&nowts)
			}
			n.updateLeadChange(true)
		}
	}

	// If we're a R1 node, and we've processed all that we needed to commit, make sure we can become
	// leader as quickly as possible, so we don't wait out the election timeout.
	if n.processed == n.commit && n.leader == _EMPTY_ && len(n.peers) == 1 && !n.pleader.Load() {
		// Need to lower the election timeout, since only the run loop can transition.
		n.resetElect(0)
	}

	// Calculate the number of entries and estimate the byte size that
	// we can now remove with a compaction/snapshot.
	if n.applied > n.papplied {
		entries = n.applied - n.papplied
	}
	if msgs := n.pindex - n.papplied; msgs > 0 {
		bytes = entries * n.bytes / msgs
	}
	return entries, bytes
}

// For capturing data needed by snapshot.
type snapshot struct {
	lastTerm  uint64
	lastIndex uint64
	peerstate []byte
	data      []byte
}

const minSnapshotLen = 28

// Encodes a snapshot into a buffer for storage.
// Lock should be held.
func (n *raft) encodeSnapshot(snap *snapshot) []byte {
	if snap == nil {
		return nil
	}
	le := binary.LittleEndian
	buf := make([]byte, minSnapshotLen+len(snap.peerstate)+len(snap.data))
	le.PutUint64(buf[0:], snap.lastTerm)
	le.PutUint64(buf[8:], snap.lastIndex)
	// Peer state
	le.PutUint32(buf[16:], uint32(len(snap.peerstate)))
	wi := 20
	copy(buf[wi:], snap.peerstate)
	wi += len(snap.peerstate)
	// data itself.
	copy(buf[wi:], snap.data)
	wi += len(snap.data)

	// Now do the hash for the end.
	n.hh.Reset()
	n.hh.Write(buf[:wi])
	var hb [highwayhash.Size64]byte
	checksum := n.hh.Sum(hb[:0])
	copy(buf[wi:], checksum)
	wi += len(checksum)
	return buf[:wi]
}

// SendSnapshot will send the latest snapshot as a normal AE.
// Should only be used when the upper layers know this is most recent.
// Used when restoring streams, moving a stream from R1 to R>1, etc.
func (n *raft) SendSnapshot(data []byte) error {
	n.Lock()
	defer n.Unlock()
	// Don't check if we're leader before sending and storing, this is used on scaleup.
	n.sendAppendEntryLocked([]*Entry{{EntrySnapshot, data}}, false)
	return nil
}

// Used to install a snapshot for the given term and applied index. This will release
// all of the log entries up to and including index. This should not be called with
// entries that have been applied to the FSM but have not been applied to the raft state.
func (n *raft) InstallSnapshot(data []byte, force bool) error {
	n.Lock()
	defer n.Unlock()

	c, err := n.createSnapshotCheckpointLocked(force)
	if err != nil {
		return err
	}
	c.n.debug("Installing snapshot of %d bytes [%d:%d]", len(data), c.term, c.applied)
	snap := &snapshot{
		lastTerm:  c.term,
		lastIndex: c.applied,
		peerstate: c.peerstate,
		data:      data,
	}
	return c.n.installSnapshot(snap)
}

// Install the snapshot.
// Lock should be held.
func (n *raft) installSnapshot(snap *snapshot) error {
	// Always reset, regardless of success or error.
	// This is done even though this doesn't come from a checkpoint. We do this so we can
	// interrupt/abort an asynchronously running snapshot (if it exists). Ensures the upper layer
	// can't overwrite a snapshot that we installed here with an old asynchronously created one.
	defer func() {
		n.snapshotting = false
	}()

	snapDir := filepath.Join(n.sd, snapshotsDir)
	sn := fmt.Sprintf(snapFileT, snap.lastTerm, snap.lastIndex)
	sfile := filepath.Join(snapDir, sn)

	n.snapLock.Lock()
	err := writeFileWithSync(n.dios, sfile, n.encodeSnapshot(snap), defaultFilePerms)
	n.snapLock.Unlock()
	if err != nil {
		// We could set write err here, but if this is a temporary situation, too many open files etc.
		// we want to retry and snapshots are not fatal.
		return err
	}

	// Delete our previous snapshot file if it exists.
	if n.snapfile != _EMPTY_ && n.snapfile != sfile {
		os.Remove(n.snapfile)
	}
	// Remember our latest snapshot file.
	n.snapfile = sfile
	if _, err := n.wal.Compact(snap.lastIndex + 1); err != nil {
		n.setWriteErrLocked(err)
		return err
	}

	// If installing a snapshot past our commits, clear the cache.
	if snap.lastIndex > n.commit {
		n.resetPendingEntries()
	}

	var state StreamState
	n.wal.FastState(&state)
	n.papplied = snap.lastIndex
	n.bytes = state.Bytes
	return nil
}

// CreateSnapshotCheckpoint creates a checkpoint to allow installing a snapshot asynchronously.
// Caller MUST make sure it only ever has one checkpoint handle at most, and either installs or
// aborts the checkpoint.
// See also: RaftNodeCheckpoint
func (n *raft) CreateSnapshotCheckpoint(force bool) (RaftNodeCheckpoint, error) {
	n.Lock()
	defer n.Unlock()
	return n.createSnapshotCheckpointLocked(force)
}

func (n *raft) createSnapshotCheckpointLocked(force bool) (*checkpoint, error) {
	if n.State() == Closed {
		return nil, errNodeClosed
	}
	if !force && n.snapshotting {
		return nil, errSnapInProgress
	}

	// If a write error has occurred already then stop here.
	if werr := n.werr; werr != nil {
		return nil, werr
	}

	// Check that a catchup isn't already taking place. If it is then we won't
	// allow installing snapshots until it is done.
	// Unless we're forced to snapshot. We might have been catching up a peer for
	// a long period, and this protects our log size from growing indefinitely.
	if !force && len(n.progress) > 0 {
		return nil, errCatchupsRunning
	}

	if n.applied == 0 {
		n.debug("Not snapshotting as there are no applied entries")
		return nil, errNoSnapAvailable
	}

	var term uint64
	if ae, _ := n.loadEntry(n.applied); ae != nil {
		term = ae.term
		ae.returnToPool()
	} else {
		n.debug("Not snapshotting as entry %d is not available", n.applied)
		return nil, errNoSnapAvailable
	}

	// Snapshot the committed peer state for the current applied index, we'll need it in the snapshot.
	peerstate := encodePeerState(n.committedPeerStateLocked(n.applied))
	snapDir := filepath.Join(n.sd, snapshotsDir)
	snapFile := filepath.Join(snapDir, fmt.Sprintf(snapFileT, term, n.applied))

	n.snapshotting = true
	c := &checkpoint{
		n:         n,
		term:      term,
		applied:   n.applied,
		papplied:  n.papplied,
		snapFile:  snapFile,
		peerstate: peerstate,
	}
	return c, nil
}

type checkpoint struct {
	n         *raft  // Reference to the RaftNode.
	term      uint64 // The term of the entry at applied.
	applied   uint64 // What applied value the snapshot will represent and what the log can be compacted to.
	papplied  uint64 // Previous applied value of the previous snapshot.
	snapFile  string // Where the snapshot should be installed.
	peerstate []byte // Encoded peerstate generated when creating this checkpoint.
}

// LoadLastSnapshot loads the last snapshot from disk when using a RaftNodeCheckpoint.
func (c *checkpoint) LoadLastSnapshot() ([]byte, error) {
	c.n.Lock()
	defer c.n.Unlock()
	if c.n.State() == Closed {
		return nil, errNodeClosed
	}
	if !c.n.snapshotting {
		// The checkpoint can be aborted at any time, don't continue if that happened.
		return nil, errSnapAborted
	}
	snap, err := c.n.loadLastSnapshot()
	if err != nil {
		return nil, err
	}
	if snap.lastIndex != c.papplied {
		// Another snapshot was installed in the meantime. This invalidates our checkpoint.
		return nil, errors.New("snapshot index mismatch")
	}
	return snap.data, nil
}

// AppendEntriesSeq allows iterating over entries that can be compacted as part of a snapshot.
func (c *checkpoint) AppendEntriesSeq() iter.Seq2[*appendEntry, error] {
	return func(yield func(*appendEntry, error) bool) {
		for index := c.papplied + 1; index <= c.applied; index++ {
			c.n.Lock()
			if c.n.State() == Closed {
				c.n.Unlock()
				yield(nil, errNodeClosed)
				return
			}
			if !c.n.snapshotting {
				c.n.Unlock()
				// The checkpoint can be aborted at any time, don't continue if that happened.
				yield(nil, errSnapAborted)
				return
			}
			// Load entry and yield to the caller while unlocked.
			ae, err := c.n.loadEntry(index)
			c.n.Unlock()
			if err != nil {
				yield(nil, err)
				return
			}
			hasMore := yield(ae, nil)
			ae.returnToPool()
			if !hasMore {
				return
			}
		}
	}
}

// Abort can be called to cancel the snapshot installation at any time.
func (c *checkpoint) Abort() {
	c.n.Lock()
	defer c.n.Unlock()
	c.n.snapshotting = false
}

// InstallSnapshot allows asynchronous installation of a snapshot by unlocking when
// performing operations that don't strictly need to be locked. When the lock is re-acquired
// it's checked that the node is not closed and n.snapshotting is still set to ensure we're
// still meant to install the snapshot.
// Async snapshots can only be used when using CreateSnapshotCheckpoint.
// Lock should be held.
func (c *checkpoint) InstallSnapshot(data []byte) (uint64, error) {
	n := c.n
	n.Lock()
	defer n.Unlock()
	if n.State() == Closed {
		return 0, errNodeClosed
	}
	if !n.snapshotting {
		// The checkpoint can be aborted at any time, don't continue if that happened.
		return 0, errSnapAborted
	}

	// Always reset, regardless of success or error.
	defer func() {
		n.snapshotting = false
	}()

	n.debug("Installing snapshot of %d bytes [%d:%d]", len(data), c.term, c.applied)
	snap := &snapshot{
		lastTerm:  c.term,
		lastIndex: c.applied,
		peerstate: c.peerstate,
		data:      data,
	}
	encoded := n.encodeSnapshot(snap)

	// Before releasing the Raft lock, we acquire the snapshot lock to make
	// sure snapshot writes remain serial.
	n.snapLock.Lock()
	// Unlock while writing.
	n.Unlock()
	err := writeFileWithSync(n.dios, c.snapFile, encoded, defaultFilePerms)
	n.snapLock.Unlock()
	n.Lock()
	// On any failure path, drop the file we just wrote so it doesn't get
	// picked up by setupLastSnapshot on restart. Skip the remove if it's the
	// snapshot already adopted into n.snapfile for this term/applied, a direct
	// install that was serialized after our write will have overwritten it.
	if closed := n.State() == Closed; closed || !n.snapshotting || err != nil {
		if c.snapFile != n.snapfile {
			os.Remove(c.snapFile)
		}
		// If the node was closed/deleted while we were writing, the write error
		// is just a consequence of that, report the close instead.
		if closed {
			return 0, errNodeClosed
		} else if !n.snapshotting {
			return 0, errSnapAborted
		}

		// We could set write err here, but if this is a temporary situation, too many open files etc.
		// we want to retry and snapshots are not fatal.
		return 0, err
	}

	// Delete our previous snapshot file if it exists.
	if n.snapfile != _EMPTY_ && n.snapfile != c.snapFile {
		os.Remove(n.snapfile)
	}
	// Remember our latest snapshot file.
	n.snapfile = c.snapFile

	// Unlock while compacting.
	n.Unlock()
	_, err = n.wal.Compact(snap.lastIndex + 1)
	n.Lock()
	if n.State() == Closed {
		return 0, errNodeClosed
	} else if !n.snapshotting {
		// The checkpoint can be aborted at any time, don't continue if that happened.
		return 0, errSnapAborted
	} else if err != nil {
		n.setWriteErrLocked(err)
		return 0, err
	}

	compacted := n.bytes
	var state StreamState
	n.wal.FastState(&state)
	n.papplied = snap.lastIndex
	n.bytes = state.Bytes

	// Expose compacted size.
	if n.bytes > compacted {
		compacted = 0
	} else {
		compacted -= n.bytes
	}
	return compacted, nil
}

// NeedSnapshot returns true if it is necessary to try to install a snapshot, i.e.
// after we have finished recovering/replaying at startup, on a regular interval or
// as a part of cleaning up when shutting down.
func (n *raft) NeedSnapshot() bool {
	n.RLock()
	defer n.RUnlock()
	return n.snapfile == _EMPTY_ && n.applied > 1
}

const (
	snapshotsDir = "snapshots"
	snapFileT    = "snap.%d.%d"
)

// termAndIndexFromSnapfile tries to load the snapshot file and returns the term
// and index from that snapshot.
func termAndIndexFromSnapFile(sn string) (term, index uint64, err error) {
	if sn == _EMPTY_ {
		return 0, 0, errBadSnapName
	}
	fn := filepath.Base(sn)
	if n, err := fmt.Sscanf(fn, snapFileT, &term, &index); err != nil || n != 2 {
		return 0, 0, errBadSnapName
	}
	if fn != fmt.Sprintf(snapFileT, term, index) {
		return 0, 0, errBadSnapName
	}
	return term, index, nil
}

// setupLastSnapshot is called at startup to try and recover the last snapshot from
// the disk if possible. We will try to recover the term, index and commit/applied
// indices and then notify the upper layer what we found. Compacts the WAL if needed.
func (n *raft) setupLastSnapshot() error {
	snapDir := filepath.Join(n.sd, snapshotsDir)
	psnaps, err := os.ReadDir(snapDir)
	if err != nil {
		if os.IsNotExist(err) {
			return errNoSnapAvailable
		}
		return err
	}

	var lterm, lindex uint64
	var latest string
	for _, sf := range psnaps {
		sfile := filepath.Join(snapDir, sf.Name())
		var term, index uint64
		term, index, err := termAndIndexFromSnapFile(sf.Name())
		if err == nil {
			if term > lterm {
				lterm, lindex = term, index
				latest = sfile
			} else if term == lterm && index > lindex {
				lindex = index
				latest = sfile
			}
		} else {
			// Clean this up, can't parse the name.
			// TODO(dlc) - We could read in and check actual contents.
			n.debug("Removing snapshot, can't parse name: %q", sf.Name())
			os.Remove(sfile)
		}
	}
	if latest == _EMPTY_ {
		return nil
	}

	// Set latest snapshot we have.
	n.Lock()
	defer n.Unlock()

	n.snapfile = latest
	snap, err := n.loadLastSnapshot()
	if err != nil {
		return err
	}

	// We successfully recovered the last snapshot from the disk.
	// Recover state from the snapshot and then notify the upper layer.
	// Compact the WAL when we're done if needed.
	n.pindex = snap.lastIndex
	n.pterm = snap.lastTerm
	// Explicitly only set commit, and not applied.
	// Applied will move up when the snapshot is actually applied.
	n.commit = snap.lastIndex
	n.papplied = snap.lastIndex
	// Restore the peerState
	ps, err := decodePeerState(snap.peerstate)
	if err != nil {
		return err
	}
	n.processPeerState(ps)
	n.extSt = ps.domainExt

	n.apply.push(newCommittedEntry(n.commit, []*Entry{{EntrySnapshot, snap.data}}))
	if _, err := n.wal.Compact(snap.lastIndex + 1); err != nil {
		n.setWriteErrLocked(err)
		return err
	}

	// Now cleanup any old entries. We only do this once we know that the
	// latest snapshot was OK.
	for _, sf := range psnaps {
		if sfile := filepath.Join(snapDir, sf.Name()); sfile != latest {
			n.debug("Removing old snapshot: %q", sfile)
			os.Remove(sfile)
		}
	}

	return nil
}

// loadLastSnapshot will load and return our last snapshot.
// Lock should be held.
func (n *raft) loadLastSnapshot() (*snapshot, error) {
	if n.snapfile == _EMPTY_ {
		return nil, errNoSnapAvailable
	}

	n.dios.acquire()
	buf, err := os.ReadFile(n.snapfile)
	n.dios.release()

	if err != nil {
		n.warn("Error reading snapshot: %v", err)
		return nil, err
	}
	if len(buf) < minSnapshotLen {
		n.warn("Snapshot corrupt, too short")
		return nil, errSnapshotCorrupt
	}

	// Check to make sure hash is consistent.
	hoff := len(buf) - 8
	lchk := buf[hoff:]
	n.hh.Reset()
	n.hh.Write(buf[:hoff])
	var hb [highwayhash.Size64]byte
	if !bytes.Equal(lchk[:], n.hh.Sum(hb[:0])) {
		n.warn("Snapshot corrupt, checksums did not match")
		return nil, errSnapshotCorrupt
	}

	le := binary.LittleEndian
	lps := le.Uint32(buf[16:])
	snap := &snapshot{
		lastTerm:  le.Uint64(buf[0:]),
		lastIndex: le.Uint64(buf[8:]),
		peerstate: buf[20 : 20+lps],
		data:      buf[20+lps : hoff],
	}

	// We had a bug in 2.9.12 that would allow snapshots on last index of 0.
	// Detect that and continue anyway, nothing else we can do about it.
	if snap.lastIndex == 0 {
		n.warn("Snapshot with last index 0 is invalid, cleaning up")
		os.Remove(n.snapfile)
		n.snapfile = _EMPTY_
		return nil, errNoSnapAvailable
	}

	return snap, nil
}

// Leader returns if we are the leader for our group.
// We use an atomic here now vs acquiring the read lock.
func (n *raft) Leader() bool {
	if n == nil {
		return false
	}
	return n.leaderState.Load()
}

// LeaderSince returns how long we have been leader for,
// if applicable.
func (n *raft) LeaderSince() *time.Time {
	if n == nil {
		return nil
	}
	return n.leaderSince.Load()
}

// stepdown immediately steps down the Raft node to the
// follower state. This will take the lock itself.
func (n *raft) stepdown(newLeader string) {
	n.Lock()
	defer n.Unlock()
	n.stepdownLocked(newLeader)
}

// stepdownLocked immediately steps down the Raft node to the
// follower state. This requires the lock is already held.
func (n *raft) stepdownLocked(newLeader string) {
	n.debug("Stepping down")
	n.switchToFollowerLocked(newLeader)
}

// isCatchingUp returns true if a catchup is currently taking place.
func (n *raft) isCatchingUp() bool {
	n.RLock()
	defer n.RUnlock()
	return n.catchup != nil
}

// isCurrent is called from the healthchecks and returns true if we believe
// that the upper layer is current with the Raft layer, i.e. that it has applied
// all of the commits that we have given it.
// If healthy is set, we must instead have processed everything the leader had when
// we first heard from it in this term. This allows lagging behind a bit under load.
// If quiet is set, nothing is logged.
// Lock should be held.
func (n *raft) isCurrent(healthy, quiet bool) bool {
	debug, warn := n.debug, n.warn
	if quiet {
		debug, warn = func(string, ...any) {}, func(string, ...any) {}
	}

	// Check if we are closed.
	if n.State() == Closed {
		debug("Not current, node is closed")
		return false
	}

	// Check whether we've made progress on any state, 0 is invalid so not healthy.
	if n.commit == 0 {
		debug("Not current, no commits")
		return false
	}

	// If we were previously logging about falling behind, also log when the problem
	// was cleared.
	clearBehindState := func() {
		if n.hcbehind {
			warn("Health check OK, no longer falling behind")
			n.hcbehind = false
		}
	}

	// Make sure we are the leader or we know we have heard from the leader recently.
	if n.State() == Leader {
		// A new leader is only healthy once it processed all entries from prior terms.
		if healthy && !n.leaderState.Load() {
			debug("Not current, leader still processing prior terms")
			return false
		}
		clearBehindState()
		return true
	}

	// Check to see that we have heard from the current leader lately.
	if n.leader != noLeader && n.leader != n.id && n.catchup == nil {
		okInterval := hbInterval * 2
		if ps := n.peers[n.leader]; ps == nil || time.Since(ps.ts) > okInterval {
			debug("Not current, no recent leader contact")
			return false
		}
	}
	if cs := n.catchup; cs != nil {
		// We're actively catching up, can't mark current even if commit==applied.
		debug("Not current, still catching up pindex=%d, cindex=%d", n.pindex, cs.cindex)
		return false
	}

	if n.paused && n.hcommit > n.commit {
		// We're currently paused, waiting to be resumed to apply pending commits.
		debug("Not current, waiting to resume applies commit=%d, hcommit=%d", n.commit, n.hcommit)
		return false
	}

	if !healthy {
		// At this point if we are current, we can return saying so.
		if n.commit == n.applied {
			clearBehindState()
			return true
		}
		return false
	}

	// Already logged when quorum was paused.
	// Only checked with a leader, since nothing resumes the quorum without one.
	if n.quorumPaused && n.leader != noLeader {
		debug("Not current, quorum paused commit=%d, applied=%d", n.commit, n.applied)
		return false
	}

	// We were healthy right before the leader changed.
	// Only skips waiting for a new leader and for reaching its index.
	if time.Since(n.lgt) < hbInterval*2 {
		clearBehindState()
		return true
	}

	if n.leader == noLeader {
		// Don't block forever if the group can't elect a leader.
		// It may need this server to be healthy before the other servers are restarted.
		if time.Since(n.nlt) < leaderlessHealthy {
			debug("Not current, no leader")
			return false
		}
		// Then we can only check against what we know locally.
		if n.processed >= n.commit {
			clearBehindState()
			return true
		}
		return false
	}

	// Should be up to date enough with the leader.
	if n.hcterm != n.term {
		debug("Not current, no append entry from the leader in this term")
		return false
	}
	if n.processed < n.hcindex {
		n.hcbehind = true
		warn("Falling behind in health check, processed %d < leader index %d", n.processed, n.hcindex)
		return false
	}
	clearBehindState()
	return true
}

// startLeaderChangeGrace keeps us healthy for a short while if we were healthy right before the leader changes.
// Must be called before raising the term, since being healthy requires the leader's index of our term.
// Lock should be held.
func (n *raft) startLeaderChangeGrace() {
	// Not when healthy due to a previous grace period, or the fallback for having no leader.
	if time.Since(n.lgt) < hbInterval*2 || n.leader == noLeader {
		return
	}
	// Not an actual health check, so don't log or change whether we're falling behind.
	hcbehind := n.hcbehind
	if n.isCurrent(true, true) {
		n.lgt = time.Now()
	}
	n.hcbehind = hcbehind
}

// Current returns if we are the leader for our group or an up to date follower.
func (n *raft) Current() bool {
	if n == nil {
		return false
	}
	n.Lock()
	defer n.Unlock()
	return n.isCurrent(false, false)
}

// Healthy returns if we are the leader for our group or a follower that's caught up.
func (n *raft) Healthy() bool {
	if n == nil {
		return false
	}
	n.Lock()
	defer n.Unlock()
	return n.isCurrent(true, false)
}

// HadPreviousLeader indicates if this group ever had a leader.
func (n *raft) HadPreviousLeader() bool {
	return n.pleader.Load()
}

// GroupLeader returns the current leader of the group.
func (n *raft) GroupLeader() string {
	if n == nil {
		return noLeader
	}
	n.RLock()
	defer n.RUnlock()
	return n.leader
}

// Leaderless is a lockless way of finding out if the group has a
// leader or not. Use instead of GroupLeader in hot paths.
func (n *raft) Leaderless() bool {
	if n == nil {
		return true
	}
	// Negated because we want the default state of hasLeader to be
	// false until the first setLeader() call.
	return !n.hasleader.Load()
}

// Guess the best next leader. Stepdown will check more thoroughly.
// Lock should be held.
func (n *raft) selectNextLeader() string {
	nextLeader, hli := noLeader, uint64(0)
	for peer, ps := range n.peers {
		if peer == n.id || ps.li <= hli {
			continue
		}
		hli = ps.li
		nextLeader = peer
	}
	return nextLeader
}

// StepDown will have a leader stepdown and optionally do a leader transfer.
func (n *raft) StepDown(preferred ...string) error {
	n.Lock()
	// Check state under lock, we might not be leader anymore.
	if n.State() != Leader {
		n.Unlock()
		return errNotLeader
	}
	if len(preferred) > 1 {
		n.Unlock()
		return errTooManyPrefs
	}

	n.debug("Being asked to stepdown")

	// See if we have up to date followers.
	maybeLeader := noLeader
	if len(preferred) > 0 {
		if preferred[0] != _EMPTY_ {
			maybeLeader = preferred[0]
		} else {
			preferred = nil
		}
	}

	// Can't pick ourselves.
	if maybeLeader == n.id {
		maybeLeader = noLeader
		preferred = nil
	}

	// If we have a preferred check it first.
	if maybeLeader != noLeader {
		var isHealthy bool
		if ps, ok := n.peers[maybeLeader]; ok {
			si, ok := n.s.nodeToInfo.Load(maybeLeader)
			isHealthy = ok && !si.(nodeInfo).offline && time.Since(ps.ts) < hbInterval*3
		}
		if !isHealthy {
			maybeLeader = noLeader
		}
	}

	// If we do not have a preferred at this point pick a healthy one.
	// Make sure not ourselves.
	if maybeLeader == noLeader {
		var fallback string
		for peer, ps := range n.peers {
			if peer == n.id {
				continue
			}
			si, ok := n.s.nodeToInfo.Load(peer)
			isHealthy := ok && !si.(nodeInfo).offline && time.Since(ps.ts) < hbInterval*3
			if !isHealthy {
				continue
			}
			// Prefer a caught up peer. One we're still catching up is slower to take over.
			if n.isFollowerCaughtUpLocked(peer) {
				maybeLeader = peer
				break
			}
			if fallback == noLeader {
				fallback = peer
			}
		}
		if maybeLeader == noLeader {
			maybeLeader = fallback
		}
	}
	n.Unlock()

	if len(preferred) > 0 && maybeLeader == noLeader {
		n.debug("Can not transfer to preferred peer %q", preferred[0])
	}

	// If we have a new leader selected, transfer over to them.
	// Send the append entry directly rather than via the proposals queue,
	// as we will switch to follower state immediately and will blow away
	// the contents of the proposal queue in the process.
	if maybeLeader != noLeader {
		n.debug("Selected %q for new leader, stepping down due to leadership transfer", maybeLeader)
		ae := newEntry(EntryLeaderTransfer, []byte(maybeLeader))
		n.sendAppendEntry([]*Entry{ae})
	}

	// Force us to stepdown here.
	n.stepdown(noLeader)

	return nil
}

// Campaign will have our node start a leadership vote.
func (n *raft) Campaign() error {
	n.Lock()
	defer n.Unlock()
	return n.campaign(randCampaignTimeout())
}

// CampaignImmediately will have our node start a leadership vote after minimal delay.
func (n *raft) CampaignImmediately() error {
	n.Lock()
	defer n.Unlock()
	n.maybeLeader = true
	n.resetInitializing()
	return n.campaign(minCampaignTimeout / 2)
}

func randCampaignTimeout() time.Duration {
	delta := rand.Int64N(int64(maxCampaignTimeout - minCampaignTimeout))
	return (minCampaignTimeout + time.Duration(delta))
}

// Campaign will have our node start a leadership vote.
// Lock should be held.
func (n *raft) campaign(et time.Duration) error {
	n.debug("Starting campaign")
	if n.State() == Leader {
		return errAlreadyLeader
	}
	n.resetElect(et)
	return nil
}

// xferCampaign will have our node start an immediate leadership vote.
// Lock should be held.
func (n *raft) xferCampaign() error {
	n.debug("Starting transfer campaign")
	if n.State() == Leader {
		n.lxfer = false
		return errAlreadyLeader
	}
	n.resetElect(10 * time.Millisecond)
	return nil
}

// State returns the current state for this node.
// Upper layers should not check State to check if we're Leader, use n.Leader() instead.
func (n *raft) State() RaftState {
	return RaftState(n.state.Load())
}

// Progress returns the current index, commit and applied values.
func (n *raft) Progress() (index, commit, applied uint64) {
	n.RLock()
	defer n.RUnlock()
	return n.pindex, n.commit, n.applied
}

// Size returns number of entries and total bytes for our WAL.
func (n *raft) Size() (entries uint64, bytes uint64) {
	n.RLock()
	entries = n.pindex - n.papplied
	bytes = n.bytes
	n.RUnlock()
	return entries, bytes
}

func (n *raft) ID() string {
	if n == nil {
		return _EMPTY_
	}
	// Lock not needed as n.id is never changed after creation.
	return n.id
}

func (n *raft) Group() string {
	// Lock not needed as n.group is never changed after creation.
	return n.group
}

func (n *raft) Peers() []*Peer {
	n.RLock()
	defer n.RUnlock()

	var peers []*Peer
	for id, ps := range n.peers {
		var current bool
		var lag uint64
		if id == n.id {
			// We are current and have no lag when compared with ourselves.
			current = true
		} else if n.id == n.leader {
			// We are the leader, we know how many entries this replica has persisted.
			// Lag is determined by how many entries we have quorum on in our log that haven't yet
			// been persisted on the replica. They are current if there's no lag.
			// This will show all peers that are part of quorum as "current".
			if n.commit > ps.li {
				lag = n.commit - ps.li
			}
			current = lag == 0
		} else if id == n.leader {
			// This peer is the leader, we don't know our lag, but we can report
			// on whether we've seen the leader recently.
			okInterval := hbInterval * 2
			current = time.Since(ps.ts) <= okInterval
		} else {
			// The remaining condition is another follower that we're not in contact with.
			// We intentionally leave current and lag as empty.
			current, lag = false, 0
		}

		p := &Peer{
			ID:      id,
			Current: current,
			Last:    ps.ts,
			Lag:     lag,
		}
		peers = append(peers, p)
	}
	return peers
}

// Update and propose our known set of peers.
func (n *raft) ProposeKnownPeers(knownPeers []string) {
	n.Lock()
	defer n.Unlock()
	// If we are the leader update and send this update out.
	if n.State() != Leader {
		return
	}
	n.updateKnownPeersLocked(knownPeers)
	n.sendPeerState()
}

// Update our known set of peers.
func (n *raft) UpdateKnownPeers(knownPeers []string) {
	n.Lock()
	n.updateKnownPeersLocked(knownPeers)
	n.Unlock()
}

func (n *raft) updateKnownPeersLocked(knownPeers []string) {
	// Process like peer state update.
	ps := &peerState{knownPeers, len(knownPeers), n.extSt}
	n.processPeerState(ps)
}

// ApplyQ returns the apply queue that new commits will be sent to for the
// upper layer to apply.
func (n *raft) ApplyQ() *ipQueue[*CommittedEntry] { return n.apply }

// LeadChangeC returns the leader change channel, notifying when the Raft
// leader role has moved.
func (n *raft) LeadChangeC() <-chan leadChange { return n.leadc }

// QuitC returns the quit channel, notifying when the Raft group has shut down.
func (n *raft) QuitC() <-chan struct{} { return n.quit }

func (n *raft) Created() time.Time {
	// Lock not needed as n.created is never changed after creation.
	return n.created
}

func (n *raft) Stop() {
	n.shutdown()
}

func (n *raft) WaitForStop() {
	if n.state.Load() == int32(Closed) {
		n.wg.Wait()
	}
}

func (n *raft) Delete() {
	n.shutdown()
	n.wg.Wait()

	n.Lock()
	defer n.Unlock()

	n.deleted = true
	if wal := n.wal; wal != nil {
		wal.Delete(false)
	}
	os.RemoveAll(n.sd)
	n.snapfile = _EMPTY_
	n.debug("Deleted")
}

func (n *raft) IsDeleted() bool {
	n.RLock()
	defer n.RUnlock()
	return n.deleted
}

func (n *raft) shutdown() {
	// First call to Stop or Delete should close the quit chan
	// to notify the runAs goroutines to stop what they're doing.
	if n.state.Swap(int32(Closed)) != int32(Closed) {
		n.leaderState.Store(false)
		n.leaderSince.Store(nil)
		close(n.quit)
	}
}

// Reset discards this node's local raft state (log, snapshots, peer set,
// term/vote) so it can be caught up cleanly by another group with the same
// name. The caller is responsible for parking the node first (typically via
// SetObserver) if it should not compete for leadership immediately after;
// Reset itself steps the node down but does not flip observer mode.
func (n *raft) Reset() {
	n.Lock()
	defer n.Unlock()

	n.debug("Resetting Raft state")

	n.stepdownLocked(_EMPTY_)

	// Drop proposals and inbound entries; they are no longer meaningful
	// against whatever log this node ends up following.
	n.prop.drain()
	n.entry.drain()
	n.resp.drain()
	n.apply.drain()
	n.reqs.drain()
	n.votes.drain()

	// Cancel any in-flight catchup so it does not race the reset.
	// Cancel after draining, we might have sent EntryCatchup and need to get them the nil entry.
	n.cancelCatchup()

	// Remove every snapshot under our snapshots dir, not just the one referenced
	// by n.snapfile. Orphans (e.g. from a crash between install and the previous
	// file's removal) would otherwise be picked up by setupLastSnapshot on the
	// next restart and reseed the state we are discarding here.
	snapDir := filepath.Join(n.sd, snapshotsDir)
	if err := os.RemoveAll(snapDir); err != nil {
		n.warn("Error removing snapshots directory during reset: %v", err)
	}
	if err := os.MkdirAll(snapDir, defaultDirPerms); err != nil {
		n.warn("Error recreating snapshots directory during reset: %v", err)
	}
	n.snapfile = _EMPTY_

	// Abort any inflight async snapshot checkpoint.
	n.snapshotting = false

	// Reset the WAL, but reset these first to not trip the assertion.
	n.commit, n.hcommit, n.applied, n.processed, n.papplied = 0, 0, 0, 0, 0
	n.resetWAL()

	// Reset peer set to just ourselves; a new leader will fold us back into
	// the cluster's membership view via processPeerState.
	n.peers = map[string]*lps{n.id: {}}
	n.removed = nil
	n.adjustClusterSizeAndQuorum()

	n.term, n.vote = 0, _EMPTY_
	n.writeTermVote()

	// Persist the cleared peer state so a restart picks up the reset.
	n.writePeerState(n.currentPeerStateLocked())
}

const (
	raftAllSubj        = "$NRG.>"
	raftVoteSubj       = "$NRG.V.%s"
	raftAppendSubj     = "$NRG.AE.%s"
	raftPropSubj       = "$NRG.P.%s"
	raftRemovePeerSubj = "$NRG.RP.%s"
	raftReply          = "$NRG.R.%s"
	raftCatchupReply   = "$NRG.CR.%s"
	// Catchup progress replies happen in their own subject space for
	// flow control to the leader, but not counting toward quorum.
	raftCatchupProgressReply = "$NRG.CP.%s"
	raftCatchupProgressPre   = "$NRG.CP."
)

// Lock should be held (due to use of random generator)
func (n *raft) newCatchupInbox() string {
	var b [replySuffixLen]byte
	rn := rand.Uint64()
	for i, l := 0, rn; i < len(b); i++ {
		b[i] = digits[l%base]
		l /= base
	}
	return fmt.Sprintf(raftCatchupReply, b[:])
}

// Lock should be held (due to use of random generator)
func (n *raft) newCatchupProgressInbox() string {
	var b [replySuffixLen]byte
	rn := rand.Uint64()
	for i, l := 0, rn; i < len(b); i++ {
		b[i] = digits[l%base]
		l /= base
	}
	return fmt.Sprintf(raftCatchupProgressReply, b[:])
}

func (n *raft) newInbox() string {
	var b [replySuffixLen]byte
	rn := rand.Uint64()
	for i, l := 0, rn; i < len(b); i++ {
		b[i] = digits[l%base]
		l /= base
	}
	return fmt.Sprintf(raftReply, b[:])
}

// Our internal subscribe.
// Lock should be held.
func (n *raft) subscribe(subject string, cb msgHandler) (*subscription, error) {
	return n.t.Subscribe(subject, cb)
}

// Lock should be held.
func (n *raft) unsubscribe(sub *subscription) {
	n.t.Unsubscribe(sub)
}

// Lock should be held.
func (n *raft) createInternalSubs() error {
	n.vsubj, n.vreply = fmt.Sprintf(raftVoteSubj, n.group), n.newInbox()
	n.asubj, n.areply = fmt.Sprintf(raftAppendSubj, n.group), n.newInbox()
	n.psubj = fmt.Sprintf(raftPropSubj, n.group)
	n.rpsubj = fmt.Sprintf(raftRemovePeerSubj, n.group)

	// Votes
	if _, err := n.subscribe(n.vreply, n.handleVoteResponse); err != nil {
		return err
	}
	if _, err := n.subscribe(n.vsubj, n.handleVoteRequest); err != nil {
		return err
	}
	// AppendEntry
	if _, err := n.subscribe(n.areply, n.handleAppendEntryResponse); err != nil {
		return err
	}
	if sub, err := n.subscribe(n.asubj, n.handleAppendEntry); err != nil {
		return err
	} else {
		n.aesub = sub
	}

	return nil
}

func randElectionTimeout() time.Duration {
	delta := rand.Int64N(int64(maxElectionTimeout - minElectionTimeout))
	return (minElectionTimeout + time.Duration(delta))
}

// Lock should be held.
func (n *raft) resetElectionTimeout() {
	n.resetElect(randElectionTimeout())
}

func (n *raft) resetElectionTimeoutWithLock() {
	n.resetElectWithLock(randElectionTimeout())
}

// Lock should be held.
func (n *raft) resetElect(et time.Duration) {
	n.etlr = time.Now()
	if n.elect == nil {
		n.elect = time.NewTimer(et)
	} else {
		if !n.elect.Stop() {
			select {
			case <-n.elect.C:
			default:
			}
		}
		n.elect.Reset(et)
	}
}

func (n *raft) resetElectWithLock(et time.Duration) {
	n.Lock()
	n.resetElect(et)
	n.Unlock()
}

// run is the top-level runner for the Raft state machine. Depending on the
// state of the node (leader, follower, candidate, observer), this will call
// through to other functions. It is expected that this function will run for
// the entire life of the Raft node once started.
func (n *raft) run() {
	s := n.s
	defer s.grWG.Done()
	defer n.wg.Done()

	// We want to wait for some routing to be enabled, so we will wait for
	// at least a route, leaf or gateway connection to be established before
	// starting the run loop.
	for gw := s.gateway; ; {
		s.mu.RLock()
		ready, gwEnabled := s.numRemotes()+len(s.leafs) > 0, gw.enabled
		s.mu.RUnlock()
		if !ready && gwEnabled {
			gw.RLock()
			ready = len(gw.out)+len(gw.in) > 0
			gw.RUnlock()
		}
		if !ready {
			select {
			case <-s.quitCh:
				return
			case <-time.After(100 * time.Millisecond):
				s.RateLimitWarnf("Waiting for routing to be established...")
			}
		} else {
			break
		}
	}

	// We may have paused adding entries to apply queue, resume here.
	// No-op if not paused.
	n.ResumeApply()

	// Send nil entry to signal the upper layers we are done doing replay/restore.
	n.apply.push(nil)

runner:
	for {
		switch n.State() {
		case Follower:
			n.runAsFollower()
		case Candidate:
			n.runAsCandidate()
		case Leader:
			n.runAsLeader()
		case Closed:
			break runner
		}
	}

	// If we've reached this point then we're shutting down, either because
	// the server is stopping or because the Raft group is closing/closed.
	n.Lock()
	defer n.Unlock()

	n.t.Close()

	// Unregistering ipQueues do not prevent them from push/pop
	// just will remove them from the central monitoring map
	queues := []interface {
		unregister()
		drain() int
	}{n.reqs, n.votes, n.prop, n.entry, n.resp, n.apply}
	for _, q := range queues {
		q.drain()
		q.unregister()
	}

	n.s.unregisterRaftNode(n.group)

	if wal := n.wal; wal != nil {
		wal.Stop()
	}

	n.debug("Shutdown")
}

func (n *raft) debug(format string, args ...any) {
	if n.dflag {
		nf := fmt.Sprintf("RAFT [%s - %s] %s", n.id, n.group, format)
		n.s.Debugf(nf, args...)
	}
}

func (n *raft) warn(format string, args ...any) {
	nf := fmt.Sprintf("RAFT [%s - %s] %s", n.id, n.group, format)
	n.s.RateLimitWarnf(nf, args...)
}

func (n *raft) error(format string, args ...any) {
	nf := fmt.Sprintf("RAFT [%s - %s] %s", n.id, n.group, format)
	n.s.Errorf(nf, args...)
}

func (n *raft) electTimer() *time.Timer {
	n.RLock()
	defer n.RUnlock()
	return n.elect
}

func (n *raft) IsObserver() bool {
	n.RLock()
	defer n.RUnlock()
	return n.observer
}

// Sets the state to observer only.
func (n *raft) SetObserver(isObserver bool) {
	n.setObserver(isObserver, extUndetermined)
}

func (n *raft) setObserver(isObserver bool, extSt extensionState) {
	n.Lock()
	defer n.Unlock()
	n.setObserverLocked(isObserver, extSt)
}

func (n *raft) setObserverLocked(isObserver bool, extSt extensionState) {
	wasObserver := n.observer
	n.observer = isObserver
	n.extSt = extSt

	// If we're leaving observer state then reset the election timer or
	// we might end up waiting for up to the observerModeInterval.
	if wasObserver && !isObserver {
		n.resetElect(randElectionTimeout())
	}
}

// processAppendEntries is called by the Raft state machine when there are
// new append entries to be committed and sent to the upper state machine.
func (n *raft) processAppendEntries() {
	canProcess := true
	if n.isClosed() {
		n.debug("AppendEntry not processing inbound, closed")
		canProcess = false
	}
	if n.outOfResources() {
		n.debug("AppendEntry not processing inbound, no resources")
		canProcess = false
	}
	// Always pop the entries, but check if we can process them. If we can't
	// then the entries are effectively dropped.
	aes := n.entry.pop()
	if canProcess {
		for _, ae := range aes {
			n.processAppendEntry(ae, ae.sub)
		}
	}
	n.entry.recycle(&aes)
}

// runAsFollower is called by run and will block for as long as the node is
// running in the follower state.
func (n *raft) runAsFollower() {
	for n.State() == Follower {
		elect := n.electTimer()

		select {
		case <-n.entry.ch:
			// New append entries have arrived over the network.
			n.processAppendEntries()
		case <-n.s.quitCh:
			// The server is shutting down.
			return
		case <-n.quit:
			// The Raft node is shutting down.
			return
		case <-elect.C:
			// The election timer has fired so we think it's time to call an election.
			// If we are out of resources we just want to stay in this state for the moment.
			if n.outOfResources() {
				n.resetElectionTimeoutWithLock()
				n.debug("Not switching to candidate, no resources")
			} else if n.IsObserver() {
				n.resetElectWithLock(observerModeInterval)
				n.debug("Not switching to candidate, observer only")
			} else if n.isCatchingUp() {
				n.debug("Not switching to candidate, catching up")
				// Check to see if our catchup has stalled.
				n.Lock()
				if n.catchupStalled() {
					n.cancelCatchup()
				}
				n.resetElectionTimeout()
				n.Unlock()
			} else {
				n.switchToCandidate()
				return
			}
		case <-n.votes.ch:
			// We're receiving votes from the network, probably because we have only
			// just stepped down and they were already in flight. Ignore them.
			n.debug("Ignoring old vote response, we have stepped down")
			n.votes.popOne()
		case <-n.resp.ch:
			// Ignore append entry responses received from before the state change.
			n.resp.drain()
		case <-n.prop.ch:
			// Ignore proposals received from before the state change.
			n.prop.drain()
		case <-n.reqs.ch:
			// We've just received a vote request from the network.
			// Because of drain() it is possible that we get nil from popOne().
			if voteReq, ok := n.reqs.popOne(); ok {
				n.processVoteRequest(voteReq)
			}
		}
	}
}

// Pool for CommittedEntry re-use.
var cePool = sync.Pool{
	New: func() any {
		return &CommittedEntry{}
	},
}

// CommittedEntry is handed back to the user to apply a commit to their upper layer.
type CommittedEntry struct {
	Index   uint64
	Entries []*Entry
}

// Create a new CommittedEntry. When the returned entry is no longer needed, it
// should be returned to the pool by calling ReturnToPool.
func newCommittedEntry(index uint64, entries []*Entry) *CommittedEntry {
	ce := cePool.Get().(*CommittedEntry)
	ce.Index, ce.Entries = index, entries
	return ce
}

// ReturnToPool returns the CommittedEntry to the pool, after which point it is
// no longer safe to reuse.
func (ce *CommittedEntry) ReturnToPool() {
	if ce == nil {
		return
	}
	if len(ce.Entries) > 0 {
		for _, e := range ce.Entries {
			entryPool.Put(e)
		}
	}
	ce.Index, ce.Entries = 0, nil
	cePool.Put(ce)
}

// Pool for Entry re-use.
var entryPool = sync.Pool{
	New: func() any {
		return &Entry{}
	},
}

// Helper to create new entries. When the returned entry is no longer needed, it
// should be returned to the entryPool pool.
func newEntry(t EntryType, data []byte) *Entry {
	entry := entryPool.Get().(*Entry)
	entry.Type, entry.Data = t, data
	return entry
}

// Pool for appendEntry re-use.
var aePool = sync.Pool{
	New: func() any {
		return &appendEntry{}
	},
}

// appendEntry is the main struct that is used to sync raft peers.
type appendEntry struct {
	leader  string   // The leader that this append entry came from.
	term    uint64   // The term when this entry was stored.
	commit  uint64   // The commit index of the leader when this append entry was sent.
	pterm   uint64   // The previous term, for checking consistency.
	pindex  uint64   // The previous commit index, for checking consistency.
	entries []*Entry // Entries to process.
	// Below fields are for internal use only:
	lterm uint64        // The highest term for catchups only, as the leader understands it. (If lterm=0, use term instead)
	reply string        // Reply subject to respond to once committed.
	sub   *subscription // The subscription that the append entry came in on.
	buf   []byte
}

// Create a new appendEntry.
func newAppendEntry(leader string, term, commit, pterm, pindex uint64, entries []*Entry) *appendEntry {
	ae := aePool.Get().(*appendEntry)
	ae.leader, ae.term, ae.commit, ae.pterm, ae.pindex, ae.entries = leader, term, commit, pterm, pindex, entries
	ae.lterm, ae.reply, ae.sub, ae.buf = 0, _EMPTY_, nil, nil
	return ae
}

// Will return this append entry, and its interior entries to their respective pools.
func (ae *appendEntry) returnToPool() {
	ae.entries, ae.buf, ae.sub, ae.reply = nil, nil, nil, _EMPTY_
	aePool.Put(ae)
}

// Pool for proposedEntry re-use.
var pePool = sync.Pool{
	New: func() any {
		return &proposedEntry{}
	},
}

// Create a new proposedEntry.
func newProposedEntry(entry *Entry, reply string) *proposedEntry {
	pe := pePool.Get().(*proposedEntry)
	pe.Entry, pe.reply = entry, reply
	return pe
}

// Will return this proosed entry.
func (pe *proposedEntry) returnToPool() {
	pe.Entry, pe.reply = nil, _EMPTY_
	pePool.Put(pe)
}

type EntryType uint8

const (
	EntryNormal EntryType = iota
	EntryOldSnapshot
	EntryPeerState
	EntryAddPeer
	EntryRemovePeer
	EntryLeaderTransfer
	EntrySnapshot
	// EntryCatchup signals an internal type used to signal a Raft-level catchup has started.
	// After the catchup completes (or is canceled), a nil entry will be sent to signal this.
	// This type of entry is purely internal and not transmitted between peers or stored in the log.
	EntryCatchup
)

func (t EntryType) String() string {
	switch t {
	case EntryNormal:
		return "Normal"
	case EntryOldSnapshot:
		return "OldSnapshot"
	case EntryPeerState:
		return "PeerState"
	case EntryAddPeer:
		return "AddPeer"
	case EntryRemovePeer:
		return "RemovePeer"
	case EntryLeaderTransfer:
		return "LeaderTransfer"
	case EntrySnapshot:
		return "Snapshot"
	}
	return fmt.Sprintf("Unknown [%d]", uint8(t))
}

type Entry struct {
	Type EntryType
	Data []byte
}

func (e *Entry) ChangesMembership() bool {
	switch e.Type {
	case EntryAddPeer, EntryRemovePeer:
		return true
	default:
		return false
	}
}

func (ae *appendEntry) String() string {
	return fmt.Sprintf("&{leader:%s term:%d commit:%d pterm:%d pindex:%d entries: %d}",
		ae.leader, ae.term, ae.commit, ae.pterm, ae.pindex, len(ae.entries))
}

const appendEntryBaseLen = idLen + 4*8 + 2

func (ae *appendEntry) encode(b []byte) ([]byte, error) {
	if ll := len(ae.leader); ll != idLen && ll != 0 {
		return nil, errLeaderLen
	}
	if len(ae.entries) > math.MaxUint16 {
		return nil, errTooManyEntries
	}

	var elen uint64
	for _, e := range ae.entries {
		// MaxInt32 instead of MaxUint32 deliberate here to stop int
		// overflow on 32-bit platforms, still gives us ~2GB limit.
		ulen := uint64(len(e.Data))
		if ulen > math.MaxInt32 {
			return nil, errBadAppendEntry
		}
		elen += ulen + 1 + 4 // 1 is type, 4 is for size.
	}
	// Uvarint for lterm can be a maximum 10 bytes for a uint64.
	var _lterm [10]byte
	lterm := _lterm[:binary.PutUvarint(_lterm[:], ae.lterm)]
	tlen := appendEntryBaseLen + elen + uint64(len(lterm))

	var buf []byte
	if uint64(cap(b)) >= tlen {
		buf = b[:idLen]
	} else {
		buf = make([]byte, idLen, tlen)
	}

	le := binary.LittleEndian
	copy(buf[:idLen], ae.leader)
	buf = le.AppendUint64(buf, ae.term)
	buf = le.AppendUint64(buf, ae.commit)
	buf = le.AppendUint64(buf, ae.pterm)
	buf = le.AppendUint64(buf, ae.pindex)
	buf = le.AppendUint16(buf, uint16(len(ae.entries)))
	for _, e := range ae.entries {
		// The +1 is safe here as we've already checked len(e.Data)
		// is not greater than MaxInt32, which is less than MaxUint32.
		buf = le.AppendUint32(buf, uint32(1+len(e.Data)))
		buf = append(buf, byte(e.Type))
		buf = append(buf, e.Data...)
	}
	// This is safe because old nodes will ignore bytes after the
	// encoded messages. Nodes that are aware of this will decode
	// it correctly.
	buf = append(buf, lterm...)
	return buf, nil
}

// This can not be used post the wire level callback since we do not copy.
func decodeAppendEntry(msg []byte, sub *subscription, reply string) (*appendEntry, error) {
	if len(msg) < appendEntryBaseLen {
		return nil, errBadAppendEntry
	}

	le := binary.LittleEndian

	ae := newAppendEntry(string(msg[:idLen]), le.Uint64(msg[8:]), le.Uint64(msg[16:]), le.Uint64(msg[24:]), le.Uint64(msg[32:]), nil)
	ae.reply, ae.sub = reply, sub

	// Decode Entries.
	ne, ri := int(le.Uint16(msg[40:])), uint64(appendEntryBaseLen)
	for i, max := 0, uint64(len(msg)); i < ne; i++ {
		if max-ri < 4 {
			return nil, errBadAppendEntry
		}
		ml := uint64(le.Uint32(msg[ri:]))
		ri += 4
		if ml <= 0 || ri+ml > max {
			return nil, errBadAppendEntry
		}
		entry := newEntry(EntryType(msg[ri]), msg[ri+1:ri+ml])
		ae.entries = append(ae.entries, entry)
		ri += ml
	}
	if len(msg[ri:]) > 0 {
		if lterm, n := binary.Uvarint(msg[ri:]); n > 0 {
			ae.lterm = lterm
		}
	}
	ae.buf = msg
	return ae, nil
}

// Pool for appendEntryResponse re-use.
var arPool = sync.Pool{
	New: func() any {
		return &appendEntryResponse{}
	},
}

// We want to make sure this does not change from system changing length of syshash.
const (
	idLen                  = 8
	appendEntryResponseLen = 24 + 1
)

// appendEntryResponse is our response to a received appendEntry.
type appendEntryResponse struct {
	term    uint64
	index   uint64
	peer    string
	reply   string // internal usage.
	success bool
}

// Create a new appendEntryResponse.
func newAppendEntryResponse(term, index uint64, peer string, success bool) *appendEntryResponse {
	ar := arPool.Get().(*appendEntryResponse)
	ar.term, ar.index, ar.peer, ar.success = term, index, peer, success
	// Always empty out.
	ar.reply = _EMPTY_
	return ar
}

func (ar *appendEntryResponse) encode(b []byte) []byte {
	var buf []byte
	if cap(b) >= appendEntryResponseLen {
		buf = b[:appendEntryResponseLen]
	} else {
		buf = make([]byte, appendEntryResponseLen)
	}
	le := binary.LittleEndian
	le.PutUint64(buf[0:], ar.term)
	le.PutUint64(buf[8:], ar.index)
	copy(buf[16:16+idLen], ar.peer)
	if ar.success {
		buf[24] = 1
	} else {
		buf[24] = 0
	}
	return buf[:appendEntryResponseLen]
}

// Track all peers we may have ever seen to use an string interns for appendEntryResponse decoding.
var peers sync.Map

func decodeAppendEntryResponse(msg []byte) *appendEntryResponse {
	if len(msg) != appendEntryResponseLen {
		return nil
	}
	le := binary.LittleEndian
	ar := arPool.Get().(*appendEntryResponse)
	ar.term = le.Uint64(msg[0:])
	ar.index = le.Uint64(msg[8:])

	peer, ok := peers.Load(string(msg[16 : 16+idLen]))
	if !ok {
		// We missed so store inline here.
		peer = string(msg[16 : 16+idLen])
		peers.Store(peer, peer)
	}
	ar.peer = peer.(string)
	ar.success = msg[24] == 1
	return ar
}

// Called when a remove peer proposal has been forwarded
func (n *raft) handleForwardedRemovePeerProposal(sub *subscription, c *client, _ *Account, _, reply string, msg []byte) {
	n.debug("Received forwarded remove peer proposal: %q", msg)

	if len(msg) != idLen {
		n.warn("Received invalid peer name for remove proposal: %q", msg)
		return
	}

	n.RLock()
	// Check state under lock, we might not be leader anymore.
	if n.State() != Leader || !n.leaderState.Load() {
		n.debug("Ignoring forwarded peer removal proposal, not leader")
		n.RUnlock()
		return
	}
	// Error if we had a previous write error.
	if werr := n.werr; werr != nil {
		n.RUnlock()
		return
	}
	if n.membChange != nil {
		n.debug("Ignoring forwarded peer removal proposal, membership changing")
		n.RUnlock()
		return
	}
	if _, ok := n.peers[string(msg)]; !ok {
		n.debug("Ignoring forwarded peer removal proposal, peer not found")
		n.RUnlock()
		return
	}
	if len(n.peers) <= 1 {
		n.debug("Ignoring forwarded peer removal proposal, remove last node")
		n.RUnlock()
		return
	}
	prop := n.prop
	n.RUnlock()

	// Need to copy since this is underlying client/route buffer.
	peer := copyBytes(msg)
	prop.push(newProposedEntry(newEntry(EntryRemovePeer, peer), reply))
}

// Called when a peer has forwarded a proposal.
func (n *raft) handleForwardedProposal(sub *subscription, c *client, _ *Account, _, reply string, msg []byte) {
	// Need to copy since this is underlying client/route buffer.
	msg = copyBytes(msg)

	n.RLock()
	prop := n.prop
	// Check state under lock, we might not be leader anymore.
	if n.State() != Leader || !n.leaderState.Load() {
		n.debug("Ignoring forwarded proposal, not leader")
		n.RUnlock()
		return
	}

	// Ignore if we have had a write error previous.
	if n.werr != nil {
		n.RUnlock()
		return
	}

	if n.isLeaderOverrun() {
		n.RUnlock()
		n.Lock()
		defer n.Unlock()
		// Now that we've reacquired as write lock, we need to make sure that everything we
		// believed before is still true. Otherwise we've either stepped down already from
		// another goroutine or we've stopped being overrun and shouldn't drop the entry.
		if n.State() != Leader || !n.leaderState.Load() {
			return
		} else if !n.isLeaderOverrun() {
			prop.push(newProposedEntry(newEntry(EntryNormal, msg), reply))
			return
		}
		var state StreamState
		n.wal.FastState(&state)
		n.warn("Leader falling behind, stepping down: pindex %d, commit %d, applied %d, WAL size %s", n.pindex, n.commit, n.applied, friendlyBytes(state.Bytes))
		// Stepdown without leader transfer, likely all replicas will be overrun, and we need time to recover.
		n.stepdownLocked(noLeader)
		n.overrunCount++
		return
	}
	// Possible that we could fall through to here from multiple connections but if
	// one does end up stepping down then the proposal queue gets drained anyway.
	n.RUnlock()
	prop.push(newProposedEntry(newEntry(EntryNormal, msg), reply))
}

// Adds peer with the given id to our membership,
// and adjusts cluster size and quorum accordingly.
// Lock should be held.
func (n *raft) addPeer(peer string) {
	// If we were on the removed list reverse that here.
	if n.removed != nil {
		delete(n.removed, peer)
		if len(n.removed) == 0 {
			n.removed = nil
		}
	}

	if _, ok := n.peers[peer]; !ok {
		// We are not tracking this one automatically so we need
		// to bump cluster size.
		n.peers[peer] = &lps{}
	}
	// Adjust cluster size and quorum if needed.
	n.adjustClusterSizeAndQuorum()
	// Write out our new state.
	n.writePeerState(&peerState{n.peerNames(), n.csz, n.extSt})
}

// Remove the peer with the given id from our membership,
// and adjusts cluster size and quorum accordingly.
// Lock should be held.
func (n *raft) removePeer(peer string) {
	if n.removed == nil {
		n.removed = map[string]time.Time{}
	}
	n.removed[peer] = time.Now()

	delete(n.peers, peer)
	n.adjustClusterSizeAndQuorum()
	n.writePeerState(&peerState{n.peerNames(), n.csz, n.extSt})
}

// Build and send appendEntry request for the given entry that changes
// membership (EntryAddPeer / EntryRemovePeer).
// Returns true if the entry made it to the WAL and was sent to the followers
func (n *raft) sendMembershipChange(e *Entry) bool {
	n.Lock()
	defer n.Unlock()

	// Only makes sense to call this with entries that change membership.
	// Also, ignore if we're already changing membership.
	if !e.ChangesMembership() || n.membChange != nil {
		return false
	}

	// Set to the index where we will store the membership change.
	// It needs to be before we send, since if we're cluster size 1 we try to commit immediately.
	peer := string(e.Data)
	n.membChange = &membChange{index: n.pindex + 1, peer: peer}
	ps := n.peers[peer]
	// Ignore if the peer already exists (add) or is not found (remove).
	if (e.Type == EntryAddPeer && ps != nil) || (e.Type == EntryRemovePeer && ps == nil) {
		n.membChange = nil
		return false
	}
	if e.Type == EntryRemovePeer {
		n.membChange.prev = ps
	}
	err := n.sendAppendEntryLocked([]*Entry{e}, true)
	if err != nil {
		n.membChange = nil
		return false
	}

	if e.Type == EntryAddPeer {
		// Track directly, but wait for commit to be official
		if _, ok := n.peers[peer]; !ok {
			n.peers[peer] = &lps{}
			n.adjustClusterSizeAndQuorum()
		}
	}

	if e.Type == EntryRemovePeer {
		// Track directly, but wait for commit to be official
		if _, ok := n.peers[peer]; ok {
			delete(n.peers, peer)
			n.adjustClusterSizeAndQuorum()
		}
		if n.csz == 1 {
			n.tryCommit(n.pindex)
			return true
		}
	}
	return true
}

func (n *raft) runAsLeader() {
	if n.State() == Closed {
		return
	}

	n.Lock()
	psubj, rpsubj := n.psubj, n.rpsubj

	// For forwarded proposals, both normal and remove peer proposals.
	fsub, err := n.subscribe(psubj, n.handleForwardedProposal)
	if err != nil {
		n.warn("Error subscribing to forwarded proposals: %v", err)
		n.stepdownLocked(noLeader)
		n.Unlock()
		return
	}
	rpsub, err := n.subscribe(rpsubj, n.handleForwardedRemovePeerProposal)
	if err != nil {
		n.warn("Error subscribing to forwarded remove peer proposals: %v", err)
		n.unsubscribe(fsub)
		n.stepdownLocked(noLeader)
		n.Unlock()
		return
	}

	// Cleanup our subscription when we leave.
	defer func() {
		n.Lock()
		n.unsubscribe(fsub)
		n.unsubscribe(rpsub)
		n.Unlock()
	}()
	n.Unlock()

	hb := time.NewTicker(hbInterval)
	defer hb.Stop()

	lq := time.NewTicker(lostQuorumCheck)
	defer lq.Stop()

	for n.State() == Leader {
		select {
		case <-n.s.quitCh:
			return
		case <-n.quit:
			return
		case <-n.resp.ch:
			ars := n.resp.pop()
			for _, ar := range ars {
				n.processAppendEntryResponse(ar)
			}
			n.resp.recycle(&ars)
		case <-n.prop.ch:
			const maxBatch = 256 * 1024
			const maxEntries = 512
			var entries []*Entry

			es, sz := n.prop.pop(), 0
			for _, b := range es {
				if b.ChangesMembership() {
					n.sendMembershipChange(b.Entry)
					continue
				}
				entries = append(entries, b.Entry)
				// Increment size.
				sz += len(b.Data) + 1
				// If below thresholds go ahead and send.
				if sz < maxBatch && len(entries) < maxEntries {
					continue
				}
				n.sendAppendEntry(entries)
				// Reset our sz and entries.
				// We need to re-create `entries` because there is a reference
				// to it in the node's pae map.
				sz, entries = 0, nil
			}
			if len(entries) > 0 {
				n.sendAppendEntry(entries)
			}
			// Respond to any proposals waiting for a confirmation.
			for _, pe := range es {
				if pe.reply != _EMPTY_ {
					n.sendReply(pe.reply, nil)
				}
				pe.returnToPool()
			}
			n.prop.recycle(&es)

		case <-hb.C:
			if n.notActive() {
				n.sendHeartbeat()
			}
		case <-lq.C:
			if n.lostQuorum() {
				n.stepdown(noLeader)
				return
			}
		case <-n.votes.ch:
			// Because of drain() it is possible that we get nil from popOne().
			vresp, ok := n.votes.popOne()
			if !ok {
				continue
			}
			if vresp.term > n.Term() {
				n.stepdown(noLeader)
				return
			}
		case <-n.reqs.ch:
			// Because of drain() it is possible that we get nil from popOne().
			if voteReq, ok := n.reqs.popOne(); ok {
				n.processVoteRequest(voteReq)
			}
		case <-n.entry.ch:
			n.processAppendEntries()
		}
	}
}

// Quorum reports the quorum status. Will be called on former leaders.
func (n *raft) Quorum() bool {
	n.RLock()
	defer n.RUnlock()

	nc := 0
	for id, peer := range n.peers {
		if id == n.id || time.Since(peer.ts) < lostQuorumInterval {
			if nc++; nc >= n.qn {
				return true
			}
		}
	}
	return false
}

func (n *raft) lostQuorum() bool {
	n.RLock()
	defer n.RUnlock()
	return n.lostQuorumLocked()
}

func (n *raft) lostQuorumLocked() bool {
	// In order to avoid false positives that can happen in heavily loaded systems
	// make sure nothing is queued up that we have not processed yet.
	// Also make sure we let any scale up actions settle before deciding.
	if n.resp.len() != 0 || (!n.lsut.IsZero() && time.Since(n.lsut) < lostQuorumInterval) {
		return false
	}

	nc := 0
	for id, peer := range n.peers {
		if id == n.id || time.Since(peer.ts) < lostQuorumInterval {
			if nc++; nc >= n.qn {
				return false
			}
		}
	}
	return true
}

// Check for being not active in terms of sending entries.
// Used in determining if we need to send a heartbeat.
func (n *raft) notActive() bool {
	n.RLock()
	defer n.RUnlock()
	return time.Since(n.active) > hbInterval
}

// Return our current term.
func (n *raft) Term() uint64 {
	n.RLock()
	defer n.RUnlock()
	return n.term
}

// Lock should be held.
func (n *raft) loadFirstEntry() (ae *appendEntry, err error) {
	var state StreamState
	n.wal.FastState(&state)
	return n.loadEntry(state.FirstSeq)
}

func (n *raft) runCatchup(ar *appendEntryResponse, indexUpdatesQ *ipQueue[uint64]) {
	n.Lock()
	s := n.s
	peer, subj, term, pterm, last := ar.peer, ar.reply, n.term, n.pterm, n.pindex
	leader := n.State() == Leader // Grab while holding lock, to not race.
	// Catchup progress responses for flow control happen on its own subscription,
	// not counting toward quorum.
	reply := n.newCatchupProgressInbox()
	psub, err := n.subscribe(reply, func(_ *subscription, _ *client, _ *Account, _, _ string, msg []byte) {
		if pr := decodeAppendEntryResponse(msg); pr != nil {
			n.trackPeer(pr.peer)
			if pr.success && pr.peer == peer {
				indexUpdatesQ.push(pr.index)
			}
			arPool.Put(pr)
		}
	})
	n.Unlock()

	defer s.grWG.Done()
	defer arPool.Put(ar)

	defer func() {
		n.Lock()
		if psub != nil {
			n.unsubscribe(psub)
		}
		// Only remove our own progress entry.
		if q, ok := n.progress[peer]; ok && q == indexUpdatesQ {
			delete(n.progress, peer)
			if len(n.progress) == 0 {
				n.progress = nil
			}
		}
		// Check if this is a new peer and if so go ahead and propose adding them.
		_, exists := n.peers[peer]
		n.Unlock()
		if !exists {
			n.debug("Catchup done for %q, will add into peers", peer)
			n.ProposeAddPeer(peer)
		}
		indexUpdatesQ.unregister()
	}()

	if err != nil {
		// Without the progress subscription, there's no flow control, so catchup would stall.
		n.warn("Canceling catchup for %q, could not subscribe to progress inbox: %v", peer, err)
		return
	}
	if !leader {
		n.debug("Canceling catchup for %q, not leader anymore", peer)
		return
	}
	n.debug("Running catchup for %q [%d:%d] to [%d:%d]", peer, ar.term, ar.index, pterm, last)

	const maxOutstanding = 2 * 1024 * 1024 // 2MB for now.
	next, total, om := uint64(0), 0, make(map[uint64]int)

	sendNext := func() bool {
		for total <= maxOutstanding {
			next++
			if next > last {
				return true
			}
			ae, err := n.loadEntry(next)
			if err != nil {
				if err != ErrStoreEOF {
					n.warn("Got an error loading %d index: %v", next, err)
				}
				return true
			}
			// Re-encode with the lterm if needed
			if ae.lterm != term {
				ae.lterm = term
				if ae.buf, err = ae.encode(ae.buf[:0]); err != nil {
					n.warn("Got an error re-encoding append entry: %v", err)
					return true
				}
			}
			// Update our tracking total.
			om[next] = len(ae.buf)
			total += len(ae.buf)
			n.sendRPC(subj, reply, ae.buf)
		}
		return false
	}

	const activityInterval = 2 * time.Second
	timeout := time.NewTimer(activityInterval)
	defer timeout.Stop()

	stepCheck := time.NewTicker(100 * time.Millisecond)
	defer stepCheck.Stop()

	// Run as long as we are leader and still not caught up.
	for n.State() == Leader {
		select {
		case <-n.s.quitCh:
			return
		case <-n.quit:
			return
		case <-stepCheck.C:
			if n.State() != Leader {
				n.debug("Catching up canceled, no longer leader")
				return
			}
		case <-timeout.C:
			n.debug("Catching up for %q stalled", peer)
			return
		case <-indexUpdatesQ.ch:
			if index, ok := indexUpdatesQ.popOne(); ok {
				// Update our activity timer.
				timeout.Reset(activityInterval)
				// Update outstanding total.
				total -= om[index]
				delete(om, index)
				if next == 0 {
					next = index
				}
				// Check if we are done.
				if index > last || sendNext() {
					n.debug("Finished catching up")
					return
				}
			}
		}
	}
}

// Lock should be held.
func (n *raft) sendSnapshotToFollower(subject string) (uint64, error) {
	snap, err := n.loadLastSnapshot()
	if err != nil {
		// We need to stepdown here when this happens.
		n.stepdownLocked(noLeader)
		return 0, err
	}
	// Go ahead and send the snapshot and peerstate here as first append entry to the catchup follower.
	ae := n.buildAppendEntry([]*Entry{{EntrySnapshot, snap.data}, {EntryPeerState, snap.peerstate}})
	ae.pterm, ae.pindex = snap.lastTerm, snap.lastIndex
	var state StreamState
	n.wal.FastState(&state)

	fpIndex := state.FirstSeq - 1
	if snap.lastIndex < fpIndex && state.FirstSeq != 0 {
		snap.lastIndex = fpIndex
		ae.pindex = fpIndex
	}

	encoding, err := ae.encode(nil)
	if err != nil {
		return 0, err
	}
	n.sendRPC(subject, n.areply, encoding)
	return snap.lastIndex, nil
}

func (n *raft) catchupFollower(ar *appendEntryResponse) {
	n.debug("Being asked to catch up follower: %q", ar.peer)
	n.Lock()
	if n.progress == nil {
		n.progress = make(map[string]*ipQueue[uint64])
	} else if q, ok := n.progress[ar.peer]; ok {
		n.debug("Will cancel existing entry for catching up %q", ar.peer)
		delete(n.progress, ar.peer)
		q.push(n.pindex)
	}

	// The follower may have lost state, so only trust the index it reports now,
	// and it's not caught up until it confirms everything we have.
	if ps := n.peers[ar.peer]; ps != nil {
		ps.li, ps.ci = ar.index, n.pindex
	}

	// Check to make sure we have this entry.
	start := ar.index + 1
	var state StreamState
	n.wal.FastState(&state)

	if start < state.FirstSeq || (state.Msgs == 0 && start <= state.LastSeq) {
		n.debug("Need to send snapshot to follower")
		if lastIndex, err := n.sendSnapshotToFollower(ar.reply); err != nil {
			n.error("Error sending snapshot to follower [%s]: %v", ar.peer, err)
			n.Unlock()
			arPool.Put(ar)
			return
		} else {
			start = lastIndex + 1
			// If no other entries, we can just return here.
			if state.Msgs == 0 || start > state.LastSeq {
				n.debug("Finished catching up")
				n.Unlock()
				arPool.Put(ar)
				return
			}
			n.debug("Snapshot sent, reset first catchup entry to %d", lastIndex)
		}
	}

	ae, err := n.loadEntry(start)
	if err != nil {
		n.warn("Request from follower for entry at index [%d] errored for state %+v - %v", start, state, err)
		if err == ErrStoreEOF {
			// If we are here we are seeing a request for an item beyond our state, meaning we should stepdown.
			n.stepdownLocked(noLeader)
			n.Unlock()
			arPool.Put(ar)
			return
		}
		ae, err = n.loadFirstEntry()
	}
	if err != nil || ae == nil {
		n.warn("Could not find a starting entry for catchup request: %v", err)
		// If we are here we are seeing a request for an item we do not have, meaning we should stepdown.
		// This is possible on a reset of our WAL but the other side has a snapshot already.
		// If we do not stepdown this can cycle.
		n.stepdownLocked(noLeader)
		n.Unlock()
		arPool.Put(ar)
		return
	}
	if ae.pindex != ar.index || ae.pterm != ar.term {
		n.debug("Our first entry [%d:%d] does not match request from follower [%d:%d]", ae.pterm, ae.pindex, ar.term, ar.index)
	}
	// Create a queue for delivering updates from responses.
	indexUpdates := newIPQueue[uint64](n.s, fmt.Sprintf("[ACC:%s] RAFT '%s' indexUpdates", n.accName, n.group))
	indexUpdates.push(ae.pindex)
	n.progress[ar.peer] = indexUpdates
	n.wg.Add(1)
	n.Unlock()
	started := n.s.startGoRoutine(func() {
		defer n.wg.Done()
		n.runCatchup(ar, indexUpdates)
	})
	if !started {
		n.wg.Done()
		n.Lock()
		if n.progress != nil && n.progress[ar.peer] == indexUpdates {
			delete(n.progress, ar.peer)
		}
		n.Unlock()
		indexUpdates.unregister()
		arPool.Put(ar)
	}
}

func (n *raft) loadEntry(index uint64) (*appendEntry, error) {
	var smp StoreMsg
	sm, err := n.wal.LoadMsg(index, &smp)
	if err != nil {
		return nil, err
	}
	return decodeAppendEntry(sm.msg, nil, _EMPTY_)
}

// applyCommit will update our commit index and apply the entry to the apply queue.
// lock should be held.
func (n *raft) applyCommit(index uint64) error {
	if n.State() == Closed {
		return errNodeClosed
	}
	if index <= n.commit {
		n.debug("Ignoring apply commit for %d, already processed", index)
		return nil
	}

	if n.State() == Leader {
		delete(n.acks, index)
	}

	if index <= n.papplied {
		return nil
	}

	ae := n.pae[index]
	if ae == nil {
		var err error
		if ae, err = n.loadEntry(index); err != nil {
			if err != ErrStoreClosed && err != ErrStoreEOF {
				n.warn("Got an error loading %d index: %v - will reset", index, err)
				if n.State() == Leader {
					n.stepdownLocked(n.selectNextLeader())
				}
				// Reset and cancel any catchup.
				n.resetWAL()
				n.cancelCatchup()
			}
			return errEntryLoadFailed
		}
	} else {
		// Size it now, the buffer is cleared below.
		sz := n.entryStoreSize(ae)
		defer func() {
			if _, ok := n.pae[index]; ok {
				n.paeBytes -= sz
				delete(n.pae, index)
			}
		}()
	}

	n.commit = index
	ae.buf = nil
	var committed []*Entry

	defer func() {
		// Pass to the upper layers if we have normal entries. It is
		// entirely possible that 'committed' might be an empty slice here,
		// which will happen if we've processed updates inline (like peer
		// states). In which case the upper layer will just call down with
		// Applied() with no further action.
		n.apply.push(newCommittedEntry(index, committed))
		// Place back in the pool.
		ae.returnToPool()
	}()

	for _, e := range ae.entries {
		switch e.Type {
		case EntryNormal:
			committed = append(committed, e)
		case EntryOldSnapshot:
			// For old snapshots in our WAL.
			committed = append(committed, newEntry(EntrySnapshot, e.Data))
		case EntrySnapshot:
			committed = append(committed, e)
			// If we have no snapshot, install the leader's snapshot as our own.
			if len(ae.entries) == 1 && n.snapfile == _EMPTY_ && ae.commit > 0 {
				n.installSnapshot(&snapshot{
					lastTerm:  ae.pterm,
					lastIndex: ae.commit,
					peerstate: encodePeerState(n.committedPeerStateLocked(ae.commit)),
					data:      e.Data,
				})
			}
		case EntryPeerState:
			if n.State() != Leader {
				if ps, err := decodePeerState(e.Data); err == nil {
					n.processPeerState(ps)
				}
			}
		case EntryAddPeer:
			newPeer := string(e.Data)
			n.debug("Added peer %q", newPeer)

			// Store our peer in our global peer map for all peers.
			peers.LoadOrStore(newPeer, newPeer)

			n.addPeer(newPeer)

			// We pass these up as well.
			committed = append(committed, e)

			// We are done with this membership change
			n.membChange = nil

		case EntryRemovePeer:
			peer := string(e.Data)
			n.debug("Removing peer %q", peer)

			n.removePeer(peer)

			// Remove from string intern map.
			peers.Delete(peer)

			// We pass these up as well.
			committed = append(committed, e)

			// We are done with this membership change
			n.membChange = nil

			// If this is us and we are the leader signal the caller
			// to attempt to stepdown.
			if peer == n.id && n.State() == Leader {
				return errNodeRemoved
			}
		}
	}
	return nil
}

// Check if there is a quorum for the given index, and if
// so, commit the corresponding entry.
// Return true if the index was committed, false otherwise.
// Lock should be held.
func (n *raft) tryCommit(index uint64) (bool, error) {
	acks := len(n.acks[index])
	// Count the leader if it's still part of membership
	if n.peers[n.ID()] != nil {
		acks += 1
	}
	if acks < n.qn {
		return false, nil
	}
	// We have a quorum
	for i := n.commit + 1; i <= index; i++ {
		if err := n.applyCommit(i); err != nil {
			if err != errNodeClosed && err != errNodeRemoved {
				n.error("Got an error applying commit for %d: %v", i, err)
			}
			return false, err
		}
	}
	return true, nil
}

// Used to track a success response. Returns true if the
// response was tracked, false if the response was ignored
// (the response is old, the index is already committed, ...)
// Lock should be held.
func (n *raft) trackResponse(ar *appendEntryResponse) bool {
	// Check state under lock, we might not be leader anymore.
	if n.State() != Leader {
		return false
	}

	ps := n.peers[ar.peer]

	// Update peer's last index.
	if ps != nil && ar.index > ps.li {
		ps.li = ar.index
	}

	// Ignore items already committed, or skip if this is not about an entry that matches our current term.
	if ar.index <= n.commit || ar.term != n.term {
		assert.AlwaysOrUnreachable(ar.term <= n.term, "Raft response term mismatch", map[string]any{
			"n.accName": n.accName,
			"n.group":   n.group,
			"n.id":      n.id,
			"n.term":    n.term,
			"ar.term":   ar.term,
		})
		return false
	}

	// Not a peer, can't count this message towards quorum
	if ps == nil {
		return false
	}

	// Keep track of the response
	results := n.acks[ar.index]
	if results == nil {
		results = make(map[string]struct{})
		n.acks[ar.index] = results
	}
	results[ar.peer] = struct{}{}

	return true
}

// Used to adjust cluster size and peer count based on added official peers.
// lock should be held.
func (n *raft) adjustClusterSizeAndQuorum() {
	pcsz, ncsz := n.csz, len(n.peers)
	n.csz = ncsz
	n.qn = n.csz/2 + 1

	if ncsz > pcsz {
		n.debug("Expanding our clustersize: %d -> %d", pcsz, ncsz)
		n.lsut = time.Now()
	} else if ncsz < pcsz {
		n.debug("Decreasing our clustersize: %d -> %d", pcsz, ncsz)
		if n.State() == Leader {
			go n.sendHeartbeat()
		}
	}
	if ncsz != pcsz {
		n.recreateInternalSubsLocked()
	}
}

// Returns true if we should count vote responses from this peer.
// Lock should be held.
func (n *raft) shouldCountVoteFromPeer(peer string) bool {
	if _, ok := n.peers[peer]; ok {
		return true
	}
	// During bootstrap, we may know fewer peer ids
	// than the declared cluster size. Initially, we
	// may need to accept votes from peers that are
	// not yet in the peer set.
	return len(n.peers) < n.csz
}

// Track interactions with this peer.
func (n *raft) trackPeer(peer string) error {
	n.Lock()
	var needPeerAdd, isRemoved bool
	var rts time.Time
	if n.removed != nil {
		rts, isRemoved = n.removed[peer]
		// Removed peers can rejoin after timeout.
		if isRemoved && time.Since(rts) >= peerRemoveTimeout {
			isRemoved = false
		}
	}
	if n.State() == Leader {
		if _, ok := n.peers[peer]; !ok {
			// Check if this peer had been removed previously.
			needPeerAdd = !isRemoved
		}
	}
	if ps := n.peers[peer]; ps != nil {
		ps.ts = time.Now()
	}
	n.Unlock()

	if needPeerAdd {
		n.ProposeAddPeer(peer)
	}
	return nil
}

// LastHeardFromFollower returns when we last heard from the given peer.
// Zero if we never heard from it. The upper layer uses this to judge if
// a meta-assigned peer is up before adding it.
// Only known by the leader.
func (n *raft) LastHeardFromFollower(peer string) time.Time {
	n.RLock()
	defer n.RUnlock()
	if ps := n.peers[peer]; ps != nil {
		return ps.ts
	}
	return time.Time{}
}

// IsFollowerCaughtUp returns whether we're not catching up this peer, it confirmed its last catchup,
// and we have heard from it recently.
// Only known by the leader.
func (n *raft) IsFollowerCaughtUp(peer string) bool {
	n.RLock()
	defer n.RUnlock()
	return n.isFollowerCaughtUpLocked(peer)
}

// Lock should be held.
func (n *raft) isFollowerCaughtUpLocked(peer string) bool {
	if peer == n.id {
		return true
	}
	// Covers catchup that started before the peer was tracked.
	if _, ok := n.progress[peer]; ok {
		return false
	}
	// Requires an ack since becoming leader, so lagging voters don't count right after an election.
	ps := n.peers[peer]
	return ps != nil && ps.li > 0 && ps.li >= ps.ci && time.Since(ps.ts) <= hbInterval*3
}

func (n *raft) runAsCandidate() {
	n.Lock()
	// Drain old responses.
	n.votes.drain()
	// An empty log only wins on quorum while initializing or as the preferred peer, see processVoteRequest.
	maybeLeader := n.maybeLeader
	n.Unlock()

	// Send out our request for votes.
	n.requestVote()

	// We vote for ourselves.
	n.votes.push(&voteResponse{term: n.term, peer: n.ID(), granted: true})

	votes := map[string]struct{}{}
	emptyVotes := map[string]struct{}{}

	// The preferred peer of a new group re-sends its vote request, as peers may not have created their node yet.
	var resendC <-chan time.Time
	resendDelay := minCampaignTimeout
	if maybeLeader {
		resendC = time.After(resendDelay)
	}

	for n.State() == Candidate {
		elect := n.electTimer()
		select {
		case <-resendC:
			n.resendVoteRequest()
			// Back off linearly, so a slow vote response triggers few duplicate requests.
			resendDelay = min(resendDelay+minCampaignTimeout, maxCampaignTimeout)
			resendC = time.After(resendDelay)
		case <-n.entry.ch:
			n.processAppendEntries()
		case <-n.resp.ch:
			// Ignore append entry responses received from before the state change.
			n.resp.drain()
		case <-n.prop.ch:
			// Ignore proposals received from before the state change.
			n.prop.drain()
		case <-n.s.quitCh:
			return
		case <-n.quit:
			return
		case <-elect.C:
			n.switchToCandidate()
			return
		case <-n.votes.ch:
			// Because of drain() it is possible that we get nil from popOne().
			vresp, ok := n.votes.popOne()
			if !ok {
				continue
			}
			n.RLock()
			nterm := n.term
			csz := n.csz
			countVote := n.shouldCountVoteFromPeer(vresp.peer)
			n.RUnlock()

			if vresp.granted && nterm == vresp.term {
				// only track peers that would be our followers
				n.trackPeer(vresp.peer)
				if countVote {
					if !vresp.empty {
						votes[vresp.peer] = struct{}{}
					} else {
						emptyVotes[vresp.peer] = struct{}{}
					}
				}
				if n.wonElection(len(votes)) {
					// Become LEADER if we have won and gotten a quorum with everyone we should hear from.
					n.switchToLeader()
					return
				} else if len(votes)+len(emptyVotes) == csz {
					// Become LEADER if we've got voted in by ALL servers.
					// We couldn't get quorum based on just our normal votes.
					// But, we have heard from the full cluster, and some servers came up empty.
					// We know for sure we have the most up-to-date log.
					n.switchToLeader()
					return
				}
			} else if vresp.term > nterm {
				// if we observe a bigger term, we should start over again or risk forming a quorum fully knowing
				// someone with a better term exists. This is even the right thing to do if won == true.
				n.Lock()
				n.debug("Stepping down from candidate, detected higher term: %d vs %d", vresp.term, n.term)
				n.term = vresp.term
				n.vote = noVote
				n.writeTermVote()
				n.lxfer = false
				n.stepdownLocked(noLeader)
				n.Unlock()
			}
		case <-n.reqs.ch:
			// Because of drain() it is possible that we get nil from popOne().
			if voteReq, ok := n.reqs.popOne(); ok {
				n.processVoteRequest(voteReq)
			}
		}
	}
}

// handleAppendEntry handles an append entry from the wire. This function
// is an internal callback from the "asubj" append entry subscription.
func (n *raft) handleAppendEntry(sub *subscription, c *client, _ *Account, _, reply string, msg []byte) {
	msg = copyBytes(msg)
	if ae, err := decodeAppendEntry(msg, sub, reply); err == nil {
		// Push to the new entry channel. From here one of the worker
		// goroutines (runAsLeader, runAsFollower, runAsCandidate) will
		// pick it up.
		n.entry.push(ae)
	} else {
		n.warn("AppendEntry failed to be placed on internal channel: corrupt entry")
	}
}

// cancelCatchup will stop an in-flight catchup by unsubscribing from the
// catchup subscription.
// Lock should be held.
func (n *raft) cancelCatchup() {
	n.debug("Canceling catchup subscription since we are now up to date")

	if n.catchup != nil && n.catchup.sub != nil {
		n.unsubscribe(n.catchup.sub)
	}
	n.cancelCatchupSignal()
	n.catchup = nil
}

// catchupStalled will try to determine if we are stalled. This is called
// on a new entry from our leader.
// Lock should be held.
func (n *raft) catchupStalled() bool {
	if n.catchup == nil {
		return false
	}
	if n.catchup.pindex == n.pindex {
		return time.Since(n.catchup.active) > 2*time.Second
	}
	n.catchup.pindex = n.pindex
	n.catchup.active = time.Now()
	return false
}

// createCatchup will create the state needed to track a catchup as it
// runs. It then creates a unique inbox for this catchup and subscribes
// to it. The remote side will stream entries to that subject.
// Lock should be held.
func (n *raft) createCatchup(ae *appendEntry) string {
	// Cleanup any old ones, but preserve whether we signaled the upper layer.
	var signal bool
	if n.catchup != nil {
		if n.catchup.sub != nil {
			n.unsubscribe(n.catchup.sub)
		}
		signal = n.catchup.signal
	}
	// Snapshot term and index.
	n.catchup = &catchupState{
		cterm:  ae.pterm,
		cindex: ae.pindex,
		pterm:  n.pterm,
		pindex: n.pindex,
		active: time.Now(),
		signal: signal,
	}
	inbox := n.newCatchupInbox()
	sub, _ := n.subscribe(inbox, n.handleAppendEntry)
	n.catchup.sub = sub
	return inbox
}

// Lock should be held.
func (n *raft) sendCatchupSignal() {
	if n.catchup == nil || n.catchup.signal {
		return
	}
	n.catchup.signal = true
	// Signal to the upper layer that the following entries are catchup entries, up until the nil guard.
	n.apply.push(newCommittedEntry(0, []*Entry{{EntryCatchup, nil}}))
}

// Lock should be held.
func (n *raft) cancelCatchupSignal() {
	if n.catchup == nil || !n.catchup.signal {
		return
	}
	// Send nil entry to signal the upper layers we are done catching up.
	n.apply.push(nil)
}

// Truncate our WAL and reset.
// Lock should be held.
func (n *raft) truncateWAL(term, index uint64) {
	n.debug("Truncating and repairing WAL to Term %d Index %d", term, index)

	if term == 0 && index == 0 {
		if n.commit > 0 {
			n.warn("Resetting WAL state")
		} else {
			n.debug("Clearing WAL state (no commits)")
		}
	}
	if index < n.commit {
		assert.Unreachable("WAL truncate lost commits", map[string]any{
			"n.accName": n.accName,
			"n.group":   n.group,
			"n.id":      n.id,
			"term":      term,
			"index":     index,
			"commit":    n.commit,
			"applied":   n.applied,
		})
	}

	defer func() {
		// Check to see if we invalidated any snapshots that might have held state
		// from the entries we are truncating.
		if snap, _ := n.loadLastSnapshot(); snap != nil && snap.lastIndex > index {
			os.Remove(n.snapfile)
			n.snapfile = _EMPTY_
		}
		// Make sure to reset commit and applied if above
		if n.commit > n.pindex {
			n.commit = n.pindex
		}
		if n.processed > n.commit {
			n.processed = n.commit
		}
		if n.applied > n.processed {
			n.applied = n.processed
		}
		// Refresh bytes count after truncate.
		var state StreamState
		n.wal.FastState(&state)
		n.bytes = state.Bytes
	}()

	if err := n.wal.Truncate(index); err != nil {
		n.warn("Error truncating WAL: %v", err)
		n.setWriteErrLocked(err)
		return
	}
	// Set after we know we have truncated properly.
	n.pterm, n.pindex = term, index

	// Invalidate cached entries the WAL no longer has.
	if index == 0 {
		n.resetPendingEntries()
	} else {
		for k, ae := range n.pae {
			if k > index {
				n.paeBytes -= n.entryStoreSize(ae)
				delete(n.pae, k)
			}
		}
	}

	// Check if we're truncating an uncommitted membership change.
	if n.membChange != nil && n.membChange.index > index {
		peer := n.membChange.peer
		if ps := n.membChange.prev; ps != nil {
			// This was a removal, add it back.
			n.peers[peer] = ps
			// If we were on the removed list, reverse that here.
			if n.removed != nil {
				delete(n.removed, peer)
				if len(n.removed) == 0 {
					n.removed = nil
				}
			}
		} else {
			// This was an addition, remove it.
			delete(n.peers, peer)
		}
		n.adjustClusterSizeAndQuorum()
		n.writePeerState(&peerState{n.peerNames(), n.csz, n.extSt})
		n.membChange = nil
	}
}

// Reset our WAL. This is equivalent to truncating all data from the log.
// Lock should be held.
func (n *raft) resetWAL() {
	n.truncateWAL(0, 0)
}

// Lock should be held
func (n *raft) updateLeader(newLeader string) {
	wasLeader := n.leader == n.id
	if newLeader == noLeader && n.hasleader.Load() {
		n.nlt = time.Now()
	}
	n.leader = newLeader
	n.hasleader.Store(newLeader != _EMPTY_)
	if !n.pleader.Load() && newLeader != noLeader {
		n.pleader.Store(true)
		// If we were preferred to become the first leader, but didn't end up successful.
		// Ensure to call lead change. When scaling from R1 to R3 we've optimized for a scale up
		// not flipping leader/non-leader/leader status if the leader remains the same. But we need to
		// correct that if the first leader turns out to be different.
		if n.maybeLeader {
			n.maybeLeader = false
			if n.id != newLeader {
				n.updateLeadChange(false)
			}
		}
	}
	// Reset last seen timestamps and indices.
	// If we are (or were) the leader we track(ed) everyone, and don't reset.
	// But if we're a follower we only track the leader, and reset all others.
	if newLeader != n.id && !wasLeader {
		for peer, ps := range n.peers {
			// Always reset last replicated and catchup index.
			ps.li, ps.ci = 0, 0
			if peer == newLeader {
				continue
			}
			// Only reset the last seen timestamp if this peer is not the leader.
			ps.ts = time.Time{}
		}
	}
}

// processAppendEntry will process an appendEntry. This is called either
// during recovery or from processAppendEntries when there are new entries
// to be committed.
func (n *raft) processAppendEntry(ae *appendEntry, sub *subscription) {
	n.Lock()
	// Don't reset here if we have been asked to assume leader position.
	if !n.lxfer {
		n.resetElectionTimeout()
	}

	// Just return if closed or we had previous write error.
	if n.State() == Closed || n.werr != nil {
		n.Unlock()
		return
	}

	// Scratch buffer for responses.
	var scratch [appendEntryResponseLen]byte
	arbuf := scratch[:]

	// Grab term from append entry. But if leader explicitly defined its term, use that instead.
	// This is required during catchup if the leader catches us up on older items from previous terms.
	// While still allowing us to confirm they're matching our highest known term.
	lterm := ae.term
	if ae.lterm != 0 {
		lterm = ae.lterm
	}

	// Are we receiving from another leader.
	if n.State() == Leader {
		if lterm >= n.term {
			// If the append entry term is newer than the current term, erase our
			// vote.
			if lterm > n.term {
				n.term = lterm
				n.vote = noVote
				n.writeTermVote()
			} else {
				assert.Unreachable(
					"Two leaders using the same term",
					map[string]any{
						"n.accName": n.accName,
						"n.group":   n.group,
						"n.id":      n.id,
						"n.term":    n.term,
						"ae.leader": ae.leader,
						"ae.term":   ae.term,
						"ae.lterm":  ae.lterm,
					},
				)
			}
			n.debug("Received append entry from another leader, stepping down to %q", ae.leader)
			n.stepdownLocked(ae.leader)
		} else {
			// Let them know we are the leader.
			ar := newAppendEntryResponse(n.term, n.pindex, n.id, false)
			n.debug("AppendEntry ignoring old term from another leader")
			n.sendRPC(ae.reply, _EMPTY_, ar.encode(arbuf))
			arPool.Put(ar)
			n.Unlock()
			return
		}
	}

	// If we received an append entry as a candidate then it would appear that
	// another node has taken on the leader role already, so we should convert
	// to a follower of that node instead.
	if n.State() == Candidate {
		// If we have a leader in the current term or higher, we should stepdown,
		// write the term and vote if the term of the request is higher.
		if lterm >= n.term {
			// If the append entry term is newer than the current term, erase our
			// vote.
			if lterm > n.term {
				n.term = lterm
				n.vote = noVote
				n.writeTermVote()
			}
			n.debug("Received append entry in candidate state from %q, converting to follower", ae.leader)
			n.stepdownLocked(ae.leader)
			n.updateLeadChange(false)
		}
	}

	// Catching up state.
	catchingUp := n.catchup != nil
	// Is this a new entry? New entries will be delivered on the append entry
	// sub, rather than a catch-up sub.
	isNew := sub != nil && sub == n.aesub
	// Entries arriving on any other subscription are catchup entries being streamed
	// to our dedicated catchup inbox.
	isCatchup := sub != nil && !isNew

	// If we are/were catching up ignore old catchup subs, but only if catching up from an older server
	// that doesn't send the leader term when catching up or if we would truncate as a result.
	// We can reject old catchups from newer subs later, just by checking the append entry is on the correct term.
	if !isNew && sub != nil && (ae.lterm == 0 || ae.pindex < n.pindex) && (!catchingUp || sub != n.catchup.sub) {
		n.Unlock()
		n.debug("AppendEntry ignoring old entry from previous catchup")
		return
	}

	// If this term is greater than ours.
	if lterm > n.term {
		n.startLeaderChangeGrace()
		n.term = lterm
		n.vote = noVote
		if isNew {
			n.writeTermVote()
		}
		if n.State() != Follower {
			n.debug("Term higher than ours and we are not a follower: %v, stepping down to %q", n.State(), ae.leader)
			n.stepdownLocked(ae.leader)
		}
	} else if lterm < n.term && sub != nil && (isNew || ae.lterm != 0) {
		// Anything that's below our expected highest term needs to be rejected.
		// Unless we're replaying (sub=nil), in which case we'll always continue.
		// For backward-compatibility we shouldn't reject if we're being caught up by an old server.
		if !isNew {
			n.debug("AppendEntry ignoring old entry from previous catchup")
			n.Unlock()
			return
		}
		n.debug("Rejected AppendEntry from a leader (%s) with term %d which is less than ours", ae.leader, lterm)
		ar := newAppendEntryResponse(n.term, n.pindex, n.id, false)
		n.Unlock()
		n.sendRPC(ae.reply, _EMPTY_, ar.encode(arbuf))
		arPool.Put(ar)
		return
	}

	// Remember the first leader index we see in this term, to process before being healthy.
	// Only raised while catching up.
	if isNew && (n.hcterm != n.term || catchingUp) {
		n.hcterm, n.hcindex = n.term, ae.pindex
		// Need to wait until all entries in this entry (and below) were applied.
		// Unless it's a heartbeat, in which case it's just all entries below this one.
		if ae.shouldStore() {
			n.hcindex++
		}
	}

	// Check state if we are catching up.
	if catchingUp {
		if cs := n.catchup; cs != nil && n.pterm >= cs.cterm && n.pindex >= cs.cindex {
			// If we are here we are good, so if we have a catchup pending we can cancel.
			n.cancelCatchup()
			// Reset our notion of catching up.
			catchingUp = false
		} else if isNew {
			var ar *appendEntryResponse
			var inbox string
			// Check to see if we are stalled. If so recreate our catchup state and resend response.
			if n.catchupStalled() {
				n.debug("Catchup may be stalled, will request again")
				inbox = n.createCatchup(ae)
				ar = newAppendEntryResponse(n.pterm, n.pindex, n.id, false)
			}
			n.Unlock()
			if ar != nil {
				n.sendRPC(ae.reply, inbox, ar.encode(arbuf))
				arPool.Put(ar)
			}
			// Ignore new while catching up or replaying.
			return
		}
	}

	if isNew && n.leader != ae.leader && n.State() == Follower {
		n.debug("AppendEntry updating leader to %q", ae.leader)
		n.lxfer = false
		n.updateLeader(ae.leader)
		n.writeTermVote()
		n.resetElectionTimeout()
		n.updateLeadChange(false)
	}

	// Track leader directly
	// But, do so after all consistency checks so we don't track an old leader.
	if isNew && ae.leader != noLeader && ae.leader == n.leader {
		if ps := n.peers[ae.leader]; ps != nil {
			ps.ts = time.Now()
		}
	}

	// If commits are outpacing our applies, temporarily stop accepting new entries to avoid falling further behind.
	// This encourages the leader to sync us via a snapshot instead. We use max(applied, papplied) to avoid
	// incorrectly triggering this pause immediately after receiving a snapshot.
	applied := max(n.applied, n.papplied)
	commit := max(n.commit, n.papplied)
	if sub != nil && (commit > applied || n.quorumPaused) {
		diff := commit - applied
		if n.quorumPaused {
			if n.overrun(diff, paeWarnThreshold, paeWarnBytes) {
				if catchingUp {
					n.cancelCatchup()
				}
				n.Unlock()
				return
			}
			// Once we're sufficiently below the threshold, we continue again. We'll likely receive a snapshot
			// from the leader.
			n.quorumPaused = false
			var state StreamState
			n.wal.FastState(&state)
			n.warn("Quorum resumed: commit %d, applied %d, WAL size %s", commit, applied, friendlyBytes(state.Bytes))
		} else if n.overrun(diff, pauseQuorumThreshold, pauseQuorumBytes) {
			// It takes a while until we reach the pause threshold, but once we do we enter a "cooldown period".
			n.quorumPaused = true
			n.overrunCount++
			var state StreamState
			n.wal.FastState(&state)
			n.warn("Quorum paused, falling behind: commit %d != applied %d, WAL size %s", commit, applied, friendlyBytes(state.Bytes))
			if catchingUp {
				n.cancelCatchup()
			}
			n.Unlock()
			return
		}
	}

	if ae.pterm != n.pterm || ae.pindex != n.pindex {
		// Check if this is a lower or equal index than what we were expecting.
		if ae.pindex <= n.pindex {
			n.debug("AppendEntry detected pindex less than/equal to ours: [%d:%d] vs [%d:%d]", ae.pterm, ae.pindex, n.pterm, n.pindex)
			var success bool

			if ae.pindex < n.commit {
				// If we have already committed this entry, just mark success.
				success = true
				n.debug("AppendEntry pindex %d below commit %d, marking success", ae.pindex, n.commit)
			} else if eae, _ := n.loadEntry(ae.pindex); eae == nil {
				// If terms are equal, and we are not catching up, we have simply already processed this message.
				// This can happen on server restarts based on timings of snapshots.
				if ae.pterm == n.pterm && isNew {
					success = true
					n.debug("AppendEntry pindex %d already processed, marking success", ae.pindex)
				} else if ae.pindex == n.pindex {
					// Check if only our terms do not match here.
					// Make sure pterms match and we take on the leader's.
					// This prevents constant spinning.
					n.truncateWAL(ae.pterm, ae.pindex)
				} else {
					snap, err := n.loadLastSnapshot()
					if err == nil && snap.lastIndex == ae.pindex && snap.lastTerm == ae.pterm {
						// Entry can't be found, this is normal because we have a snapshot at this index.
						// Truncate back to where we've created the snapshot.
						n.truncateWAL(snap.lastTerm, snap.lastIndex)
						// Only continue if truncation was successful, and we ended up such that we can safely continue.
						if ae.pterm == n.pterm && ae.pindex == n.pindex {
							goto CONTINUE
						}
					} else {
						// Otherwise, something has gone very wrong and we need to reset.
						n.resetWAL()
					}
				}
			} else if eae.term == ae.pterm {
				// If terms match we can delete all entries past this one, and then continue storing the current entry.
				n.truncateWAL(ae.pterm, ae.pindex)
				// Only continue if truncation was successful, and we ended up such that we can safely continue.
				if ae.pterm == n.pterm && ae.pindex == n.pindex {
					goto CONTINUE
				}
			} else {
				// If terms mismatched, delete that entry and all others past it.
				// But only if we haven't already committed past this point.
				if eae.pindex < n.commit {
					success = true
					assert.Unreachable("Truncate to earlier entry would lose commits", map[string]any{
						"n.accName":  n.accName,
						"n.group":    n.group,
						"n.id":       n.id,
						"n.term":     n.term,
						"n.pindex":   n.pindex,
						"n.commit":   n.commit,
						"n.applied":  n.applied,
						"ae.pindex":  ae.pindex,
						"ae.pterm":   ae.pterm,
						"ae.commit":  ae.commit,
						"eae.pterm":  eae.pterm,
						"eae.pindex": eae.pindex,
					})
				} else {
					n.truncateWAL(eae.pterm, eae.pindex)
				}
			}
			// Cancel regardless if unsuccessful.
			if !success {
				n.cancelCatchup()
			}
			// Intentionally not responding. Otherwise, we could erroneously report "success". Reporting
			// non-success is not needed either, and would only be wasting messages.
			// For example, if we got partial catchup, and then the "real-time" messages came in very delayed.
			// If we reported "success" on those "real-time" messages, we'd wrongfully be providing
			// quorum while not having an up-to-date log.
			n.Unlock()
			return
		}

		// Check if we are catching up. If we are here we know the leader did not have all of the entries
		// so make sure this is a snapshot entry. If it is not start the catchup process again since it
		// means we may have missed additional messages.
		if catchingUp {
			// This means we already entered into a catchup state but what the leader sent us did not match what we expected.
			// Snapshots and peerstate will always be together when a leader is catching us up in this fashion.
			if len(ae.entries) != 2 || ae.entries[0].Type != EntrySnapshot || ae.entries[1].Type != EntryPeerState {
				n.warn("Expected first catchup entry to be a snapshot and peerstate, will retry")
				n.cancelCatchup()
				n.Unlock()
				return
			}

			if ps, err := decodePeerState(ae.entries[1].Data); err == nil {
				n.processPeerState(ps)
				// Also need to copy from client's buffer.
				ae.entries[0].Data = copyBytes(ae.entries[0].Data)
			} else {
				n.warn("Could not parse snapshot peerstate correctly")
				n.cancelCatchup()
				n.Unlock()
				return
			}

			// Inherit state from appendEntry with the leader's snapshot.
			hadPreviousSnapshot := n.snapfile != _EMPTY_

			snap := &snapshot{
				lastTerm:  ae.pterm,
				lastIndex: ae.pindex,
				peerstate: encodePeerState(&peerState{n.peerNames(), n.csz, n.extSt}),
				data:      ae.entries[0].Data,
			}
			// Install the leader's snapshot as our own.
			if err := n.installSnapshot(snap); err != nil {
				n.setWriteErrLocked(err)
				n.Unlock()
				return
			}
			n.pindex = ae.pindex
			n.pterm = ae.pterm
			n.commit = ae.pindex
			n.resetInitializing()

			if !hadPreviousSnapshot {
				// If the first snapshot we install is received from another server, then we immediately signal
				// to the upper-layer it can coalesce catchup entries.
				n.sendCatchupSignal()
			}
			// Now send snapshot to upper levels. Only send the snapshot, not the peerstate entry.
			n.apply.push(newCommittedEntry(n.commit, ae.entries[:1]))
			if hadPreviousSnapshot {
				// Signal catchup only after we've sent the snapshot. That ensures the upper-layer processes the snapshot
				// as-is and can only coalesce other catchup entries after this one.
				n.sendCatchupSignal()
			}
			n.Unlock()
			return
		}

		// Setup our state for catching up.
		n.debug("AppendEntry did not match [%d:%d] with [%d:%d]", ae.pterm, ae.pindex, n.pterm, n.pindex)
		inbox := n.createCatchup(ae)
		ar := newAppendEntryResponse(n.pterm, n.pindex, n.id, false)
		n.Unlock()
		n.sendRPC(ae.reply, inbox, ar.encode(arbuf))
		arPool.Put(ar)
		return
	}

CONTINUE:
	// Save to our WAL if we have entries.
	if ae.shouldStore() {
		// Only store if an original which will have sub != nil
		if sub != nil {
			if err := n.storeToWAL(ae); err != nil {
				if err != ErrStoreClosed {
					n.warn("Error storing entry to WAL: %v", err)
				}
				n.Unlock()
				return
			}
			n.cachePendingEntry(ae)
			n.resetInitializing()
		} else {
			// This is a replay on startup so just take the appendEntry version.
			n.pterm = ae.term
			n.pindex = ae.pindex + 1
		}
	}

	// Check to see if we have any related entries to process here.
	for _, e := range ae.entries {
		switch e.Type {
		case EntryLeaderTransfer:
			// Only process these if they are new, so no replays or catchups.
			if isNew {
				maybeLeader := string(e.Data)
				// This is us. We need to check if we can become the leader.
				if maybeLeader == n.id {
					// If not an observer and not paused we are good to go.
					if !n.observer && !n.paused {
						n.lxfer = true
						n.xferCampaign()
					} else if n.paused {
						// Here we can become a leader but need to wait for resume of the apply queue.
						n.lxfer = true
					}
				} else if !n.observer {
					// Campaign early in case the successor is slow to take over.
					// Randomized so followers don't split the vote.
					n.resetElect(minElectionTimeout/4 + randCampaignTimeout())
				}
			}
		case EntryAddPeer:
			// When receiving or restoring, mark membership as changing.
			// Set to the index where this entry was stored (pindex is now this entry's index)
			if newPeer := string(e.Data); len(newPeer) == idLen {
				// Track directly, but wait for commit to be official
				n.membChange = &membChange{index: n.pindex, peer: newPeer}
				if _, ok := n.peers[newPeer]; !ok {
					n.peers[newPeer] = &lps{}
				}
				n.adjustClusterSizeAndQuorum()
				// Store our peer in our global peer map for all peers.
				peers.LoadOrStore(newPeer, newPeer)
			}
		case EntryRemovePeer:
			// When receiving or restoring, mark membership as changing.
			// Set to the index where this entry was stored (pindex is now this entry's index)
			if oldPeer := string(e.Data); len(oldPeer) == idLen {
				// Track directly, but wait for commit to be official
				ps, ok := n.peers[oldPeer]
				if !ok {
					ps = &lps{}
				}
				n.membChange = &membChange{index: n.pindex, peer: oldPeer, prev: ps}
				delete(n.peers, oldPeer)
				n.adjustClusterSizeAndQuorum()
			}
		}
	}

	// Make a copy of these values, as the AppendEntry might be cached and returned to the pool in applyCommit.
	aeCommit := ae.commit
	aeReply := ae.reply

	// Apply anything we need here.
	if aeCommit > n.commit {
		// If we're catching up, we might need to signal that it's okay to potentially coalesce entries from here.
		if catchingUp {
			n.sendCatchupSignal()
		}
		if n.paused {
			n.hcommit = aeCommit
			n.debug("Paused, not applying %d", aeCommit)
		} else {
			for index := n.commit + 1; index <= aeCommit; index++ {
				if err := n.applyCommit(index); err != nil {
					break
				}
			}
		}
	}

	// The only way for the leader to receive "success" MUST be through this path.
	// Only new entries are allowed to provide quorum, since providing quorum based
	// on catchup is unsafe. Catchup entries are acked through here, but only on a
	// dedicated progress inbox.
	var ar *appendEntryResponse
	if isNew || (isCatchup && strings.HasPrefix(aeReply, raftCatchupProgressPre)) {
		ar = newAppendEntryResponse(n.pterm, n.pindex, n.id, true)
	}
	n.Unlock()

	// Success. Send our response.
	if ar != nil {
		n.sendRPC(aeReply, _EMPTY_, ar.encode(arbuf))
		arPool.Put(ar)
	}
}

// resetInitializing resets the notion of initializing.
// If we were scaling up, also leaves observer mode.
// Lock should be held.
func (n *raft) resetInitializing() {
	n.initializing = false
	if n.scaleUp {
		n.scaleUp = false
		n.setObserverLocked(false, extUndetermined)
	}
}

// processPeerState is called when a peer state entry is received
// over the wire or when we're updating known peers.
// Lock should be held.
func (n *raft) processPeerState(ps *peerState) {
	// Update our version of peers to that of the leader. Calculate
	// the number of nodes needed to establish a quorum.
	n.csz = ps.clusterSize
	n.qn = n.csz/2 + 1

	old := n.peers
	n.peers = make(map[string]*lps)
	for _, peer := range ps.knownPeers {
		if lp := old[peer]; lp != nil {
			n.peers[peer] = lp
			delete(old, peer)
		} else {
			n.peers[peer] = &lps{}
		}
		// If we were on the removed list reverse that here.
		if n.removed != nil {
			delete(n.removed, peer)
			if len(n.removed) == 0 {
				n.removed = nil
			}
		}
	}
	// Any remaining old nodes are marked as removed, so they can't be
	// re-added via automatic peer tracking.
	if len(old) > 0 {
		if n.removed == nil {
			n.removed = map[string]time.Time{}
		}
		now := time.Now()
		for peer := range old {
			n.removed[peer] = now
		}
	}
	n.debug("Update peers from leader to %+v", n.peers)
	n.writePeerState(ps)
}

// processAppendEntryResponse is called when we receive an append entry
// response from another node. They will send a confirmation to tell us
// whether they successfully committed the entry or not.
func (n *raft) processAppendEntryResponse(ar *appendEntryResponse) {
	n.trackPeer(ar.peer)

	if ar.success {
		// The remote node successfully committed the append entry.
		// They agree with our leadership and are happy with the state of the log.
		// In this case ar.term was populated with the remote's pterm. If this matches
		// our term, we can use it to check for quorum and up our commit.
		var err error
		var committed bool

		n.Lock()
		if n.trackResponse(ar) {
			committed, err = n.tryCommit(ar.index)
		}
		n.Unlock()

		// Leader was peer-removed. Attempt a step-down to
		// a new leader before shutting down.
		if err == errNodeRemoved {
			n.StepDown()
			n.Stop()
		}

		// Send a heartbeat if there is no other message lined
		// up, so that followers can apply without waiting for
		// the next message.
		if committed && n.prop.len() == 0 {
			n.sendHeartbeat()
		}

		arPool.Put(ar)
	} else if ar.reply != _EMPTY_ {
		// The remote node didn't commit the append entry, and they believe they
		// are behind and have specified a reply subject, so let's try to catch them up.
		// In this case ar.term was populated with the remote's pterm.
		n.catchupFollower(ar)
	} else {
		n.Lock()
		if ar.term > n.term {
			// The remote node didn't commit the append entry, it looks like
			// they are on a newer term than we are. Step down.
			// In this case ar.term was populated with the remote's term.
			n.term = ar.term
			n.vote = noVote
			n.writeTermVote()
			n.warn("Detected another leader with higher term, will stepdown")
			n.stepdownLocked(noLeader)
		}
		n.Unlock()
		// Either way, return back to pool.
		arPool.Put(ar)
	}
}

// handleAppendEntryResponse processes responses to append entries.
func (n *raft) handleAppendEntryResponse(sub *subscription, c *client, _ *Account, subject, reply string, msg []byte) {
	ar := decodeAppendEntryResponse(msg)
	if ar == nil {
		n.error("Received malformed append entry response")
		return
	}
	ar.reply = reply
	n.resp.push(ar)
}

func (n *raft) buildAppendEntry(entries []*Entry) *appendEntry {
	return newAppendEntry(n.id, n.term, n.commit, n.pterm, n.pindex, entries)
}

// Determine if we should store an entry. This stops us from storing
// heartbeat messages.
func (ae *appendEntry) shouldStore() bool {
	return ae != nil && len(ae.entries) > 0
}

// Store our append entry to our WAL.
// lock should be held.
func (n *raft) storeToWAL(ae *appendEntry) error {
	if ae == nil {
		return fmt.Errorf("raft: Missing append entry for storage")
	}
	if n.werr != nil {
		return n.werr
	}

	seq, _, err := n.wal.StoreMsg(_EMPTY_, nil, ae.buf, 0)
	if err != nil {
		n.setWriteErrLocked(err)
		return err
	}

	// Sanity checking for now.
	if index := ae.pindex + 1; index != seq {
		n.warn("Wrong index, ae is %+v, index stored was %d, n.pindex is %d, will reset", ae, seq, n.pindex)
		if n.State() == Leader {
			n.stepdownLocked(n.selectNextLeader())
		}
		// Reset and cancel any catchup.
		n.resetWAL()
		n.cancelCatchup()
		return errEntryStoreFailed
	}

	n.bytes += n.entryStoreSize(ae)
	n.pterm = ae.term
	n.pindex = seq
	return nil
}

const (
	pauseQuorumThreshold = 100_000
	pauseQuorumBytes     = 1 * 1024 * 1024 * 1024 // 1GB

	paeDropThreshold = 20_000
	paeDropBytes     = 256 * 1024 * 1024 // 256MB

	paeWarnThreshold   = 10_000
	paeWarnModulo      = 5_000
	paeWarnBytes       = 128 * 1024 * 1024 // 128MB
	paeWarnBytesModulo = 32 * 1024 * 1024  // 32MB
)

func (n *raft) sendAppendEntry(entries []*Entry) {
	n.Lock()
	defer n.Unlock()
	n.sendAppendEntryLocked(entries, true)
}

// Returns nil if an appendEntry was appended to our WAL and sent to followers,
// an error otherwise.
func (n *raft) sendAppendEntryLocked(entries []*Entry, checkLeader bool) error {
	// Safeguard against sending an append entry right after a stepdown from a different goroutine.
	// Specifically done while holding the lock to not race.
	if checkLeader && n.State() != Leader {
		n.debug("Not sending append entry, not leader")
		return errNotLeader
	}
	ae := n.buildAppendEntry(entries)

	var err error
	var scratch [1024]byte
	ae.buf, err = ae.encode(scratch[:])
	if err != nil {
		return err
	}

	// If we have entries store this in our wal.
	shouldStore := ae.shouldStore()
	if shouldStore {
		if err := n.storeToWAL(ae); err != nil {
			return err
		}
		n.active = time.Now()
		n.cachePendingEntry(ae)
	}
	n.sendRPC(n.asubj, n.areply, ae.buf)
	if !shouldStore {
		ae.returnToPool()
	}
	if n.csz == 1 {
		n.tryCommit(n.pindex)
	}
	return nil
}

// cachePendingEntry saves append entries in memory for faster processing during applyCommit.
// Only save so many however to avoid memory bloat.
func (n *raft) cachePendingEntry(ae *appendEntry) {
	// Invalidate any cache entry at this index, we might have
	// stored it previously with a different value.
	if pae, ok := n.pae[n.pindex]; ok {
		n.paeBytes -= n.entryStoreSize(pae)
		delete(n.pae, n.pindex)
	}
	if l := uint64(len(n.pae)); l < paeDropThreshold && n.paeBytes < paeDropBytes {
		prev := n.paeBytes
		n.pae[n.pindex], l = ae, l+1
		n.paeBytes += n.entryStoreSize(ae)
		if l >= paeWarnThreshold && l%paeWarnModulo == 0 {
			n.warn("%d append entries pending", len(n.pae))
		} else if b := n.paeBytes; b >= paeWarnBytes {
			// Only log on transition past another paeWarnBytesModulo.
			if b/paeWarnBytesModulo != prev/paeWarnBytesModulo {
				n.warn("%d append entries pending, %s", l, friendlyBytes(b))
			}
		}
	}
}

// entryStoreSize returns the byte size of the given append entry as stored in our WAL.
// Lock should be held.
func (n *raft) entryStoreSize(ae *appendEntry) uint64 {
	if ae == nil || ae.buf == nil {
		return 0
	}
	if n.wtype == FileStorage {
		return fileStoreMsgSize(_EMPTY_, nil, ae.buf)
	}
	return memStoreMsgSize(_EMPTY_, nil, ae.buf)
}

// resetPendingEntries clears our append entry cache.
// Lock should be held.
func (n *raft) resetPendingEntries() {
	if len(n.pae) > 0 {
		n.pae = make(map[uint64]*appendEntry)
	}
	n.paeBytes = 0
}

type extensionState uint16

const (
	extUndetermined = extensionState(iota)
	extExtended
	extNotExtended
)

type peerState struct {
	knownPeers  []string
	clusterSize int
	domainExt   extensionState
}

func peerStateBufSize(ps *peerState) int {
	return 4 + 4 + (idLen * len(ps.knownPeers)) + 2
}

func encodePeerState(ps *peerState) []byte {
	le := binary.LittleEndian
	buf := make([]byte, peerStateBufSize(ps))
	le.PutUint32(buf[0:], uint32(ps.clusterSize))
	le.PutUint32(buf[4:], uint32(len(ps.knownPeers)))
	wi := 8
	for _, peer := range ps.knownPeers {
		copy(buf[wi:], peer)
		wi += idLen
	}
	le.PutUint16(buf[wi:], uint16(ps.domainExt))
	return buf
}

func decodePeerState(buf []byte) (*peerState, error) {
	if len(buf) < 8 {
		return nil, errCorruptPeers
	}
	le := binary.LittleEndian
	ps := &peerState{clusterSize: int(le.Uint32(buf[0:]))}
	expectedPeers := int(le.Uint32(buf[4:]))
	buf = buf[8:]
	ri := 0
	for i, n := 0, expectedPeers; i < n && ri+idLen <= len(buf); i++ {
		ps.knownPeers = append(ps.knownPeers, string(buf[ri:ri+idLen]))
		ri += idLen
	}
	if len(ps.knownPeers) != expectedPeers {
		return nil, errCorruptPeers
	}
	if len(buf[ri:]) >= 2 {
		ps.domainExt = extensionState(le.Uint16(buf[ri:]))
	}
	return ps, nil
}

func (n *raft) PeerNames() []string {
	n.RLock()
	defer n.RUnlock()
	return n.peerNames()
}

// Lock should be held.
func (n *raft) peerNames() []string {
	peers := make([]string, 0, len(n.peers))
	for name := range n.peers {
		peers = append(peers, name)
	}
	return peers
}

func (n *raft) currentPeerState() *peerState {
	n.RLock()
	ps := n.currentPeerStateLocked()
	n.RUnlock()
	return ps
}

func (n *raft) currentPeerStateLocked() *peerState {
	return &peerState{n.peerNames(), n.csz, n.extSt}
}

// committedPeerStateLocked builds peer state from the committed membership as of the given index.
// Lock should be held.
func (n *raft) committedPeerStateLocked(index uint64) *peerState {
	// No pending change or index already covers the pending change.
	if n.membChange == nil || n.membChange.index <= index {
		return &peerState{n.peerNames(), n.csz, n.extSt}
	}

	// Reconstruct the committed peer set by reverting the speculative change.
	peer := n.membChange.peer
	if n.membChange.prev != nil {
		// This was a removal; the peer is still a committed member, add it back.
		names := make([]string, 0, len(n.peers)+1)
		for name := range n.peers {
			names = append(names, name)
		}
		if _, ok := n.peers[peer]; !ok {
			names = append(names, peer)
		}
		return &peerState{names, len(names), n.extSt}
	}

	// This was an addition; exclude the not-yet-committed peer.
	names := make([]string, 0, len(n.peers))
	for name := range n.peers {
		if name == peer {
			continue
		}
		names = append(names, name)
	}
	return &peerState{names, len(names), n.extSt}
}

// sendPeerState will send our current peer state to the cluster.
// Lock should be held.
func (n *raft) sendPeerState() {
	n.sendAppendEntryLocked([]*Entry{{EntryPeerState, encodePeerState(n.currentPeerStateLocked())}}, true)
}

// Send a heartbeat.
func (n *raft) sendHeartbeat() {
	n.sendAppendEntry(nil)
}

type voteRequest struct {
	term      uint64
	lastTerm  uint64
	lastIndex uint64
	candidate string
	// internal only.
	reply string
}

const voteRequestLen = 24 + idLen

func (vr *voteRequest) encode() []byte {
	var buf [voteRequestLen]byte
	le := binary.LittleEndian
	le.PutUint64(buf[0:], vr.term)
	le.PutUint64(buf[8:], vr.lastTerm)
	le.PutUint64(buf[16:], vr.lastIndex)
	copy(buf[24:24+idLen], vr.candidate)

	return buf[:voteRequestLen]
}

func decodeVoteRequest(msg []byte, reply string) *voteRequest {
	if len(msg) != voteRequestLen {
		return nil
	}

	le := binary.LittleEndian
	return &voteRequest{
		term:      le.Uint64(msg[0:]),
		lastTerm:  le.Uint64(msg[8:]),
		lastIndex: le.Uint64(msg[16:]),
		candidate: string(copyBytes(msg[24 : 24+idLen])),
		reply:     reply,
	}
}

const peerStateFile = "peers.idx"

// Lock should be held.
func (n *raft) writePeerState(ps *peerState) {
	pse := encodePeerState(ps)
	if bytes.Equal(n.wps, pse) {
		return
	}
	// Stamp latest and write the peer state file.
	n.wps = pse
	if err := writePeerState(n.dios, n.sd, ps); err != nil && !n.isClosed() {
		n.setWriteErrLocked(err)
		n.warn("Error writing peer state file for %q: %v", n.group, err)
	}
}

// Writes out our peer state outside of a specific raft context.
func writePeerState(dios *diskIOSemaphore, sd string, ps *peerState) error {
	psf := filepath.Join(sd, peerStateFile)
	if _, err := os.Stat(psf); err != nil && !os.IsNotExist(err) {
		return err
	}
	return writeFileWithSync(dios, psf, encodePeerState(ps), defaultFilePerms)
}

func readPeerState(dios *diskIOSemaphore, sd string) (ps *peerState, err error) {
	dios.acquire()
	buf, err := os.ReadFile(filepath.Join(sd, peerStateFile))
	dios.release()

	if err != nil {
		return nil, err
	}
	return decodePeerState(buf)
}

const (
	termVoteFile = "tav.idx"
	termLen      = 8 // uint64
	termVoteLen  = idLen + termLen
)

// Writes out our term & vote outside of a specific raft context.
func writeTermVote(dios *diskIOSemaphore, sd string, wtv []byte) error {
	psf := filepath.Join(sd, termVoteFile)
	if _, err := os.Stat(psf); err != nil && !os.IsNotExist(err) {
		return err
	}
	return writeFileWithSync(dios, psf, wtv, defaultFilePerms)
}

// readTermVote will read the largest term and who we voted from to stable storage.
// Lock should be held.
func (n *raft) readTermVote() (term uint64, voted string, err error) {
	n.dios.acquire()
	buf, err := os.ReadFile(filepath.Join(n.sd, termVoteFile))
	n.dios.release()

	if err != nil {
		return 0, noVote, err
	}
	if len(buf) < termLen {
		// Not enough bytes for the uint64 below, so avoid a panic.
		return 0, noVote, nil
	}
	le := binary.LittleEndian
	term = le.Uint64(buf[0:])
	if len(buf) < termVoteLen {
		return term, noVote, nil
	}
	voted = string(buf[8:])
	return term, voted, nil
}

// Lock should be held.
func (n *raft) setWriteErrLocked(err error) {
	// Check if we are closed already.
	if n.State() == Closed {
		return
	}
	// Ignore if already set.
	if n.werr == err || err == nil {
		return
	}
	// Ignore non-write errors.
	if err == ErrStoreClosed ||
		err == ErrStoreEOF ||
		err == ErrStoreMsgNotFound ||
		err == errNoPending ||
		err == errPartialCache {
		return
	}
	// If this is a not found report but do not disable.
	if os.IsNotExist(err) {
		n.warn("Resource not found: %v", err)
		return
	}
	n.error("Critical write error: %v", err)
	n.werr = err
	// Abort any inflight async snapshot checkpoint.
	n.snapshotting = false
	n.shutdown()
	assert.Unreachable("Raft encountered write error", map[string]any{
		"n.accName": n.accName,
		"n.group":   n.group,
		"n.id":      n.id,
		"err":       err,
	})

	if isPermissionError(err) {
		go n.s.handleWritePermissionError()
	}

	if isOutOfSpaceErr(err) {
		// For now since this can be happening all under the covers, we will call up and disable JetStream.
		go n.s.handleOutOfSpace(nil)
	}
}

// Helper to check if we are closed when we do not hold a lock already.
func (n *raft) isClosed() bool {
	return n.State() == Closed
}

// GetWriteErr returns the write error (if any).
func (n *raft) GetWriteErr() error {
	n.RLock()
	defer n.RUnlock()
	return n.werr
}

// Capture our write error if any and hold.
func (n *raft) setWriteErr(err error) {
	n.Lock()
	defer n.Unlock()
	n.setWriteErrLocked(err)
}

// writeTermVote will record the largest term and who we voted for to stable
// storage. It returns the write error, if any, so that callers which need the
// term/vote to be durable before acting on it (e.g. granting or requesting a
// vote) can avoid doing so when the persist fails.
// Lock should be held.
func (n *raft) writeTermVote() error {
	if n.werr != nil {
		return n.werr
	}

	var buf [termVoteLen]byte
	le := binary.LittleEndian
	le.PutUint64(buf[0:], n.term)
	copy(buf[8:], n.vote)
	b := buf[:8+len(n.vote)]

	// If the term and vote hasn't changed then don't rewrite to disk.
	if bytes.Equal(n.wtv, b) {
		return nil
	}
	// Stamp latest and write the term & vote file.
	n.wtv = b
	if err := writeTermVote(n.dios, n.sd, n.wtv); err != nil && !n.isClosed() {
		// Clear wtv since we failed.
		n.wtv = nil
		n.setWriteErrLocked(err)
		n.warn("Error writing term and vote file for %q: %v", n.group, err)
		return err
	}
	return nil
}

// voteResponse is a response to a vote request.
type voteResponse struct {
	term    uint64
	peer    string
	granted bool
	empty   bool // "Empty vote", whether this peer's log is empty.
}

const voteResponseLen = 8 + 8 + 1

func (vr *voteResponse) encode() []byte {
	var buf [voteResponseLen]byte
	le := binary.LittleEndian
	le.PutUint64(buf[0:], vr.term)
	copy(buf[8:], vr.peer)
	if vr.granted {
		buf[16] |= 1
	}
	if vr.empty {
		buf[16] |= 2
	}
	return buf[:voteResponseLen]
}

func decodeVoteResponse(msg []byte) *voteResponse {
	if len(msg) != voteResponseLen {
		return nil
	}
	le := binary.LittleEndian
	vr := &voteResponse{term: le.Uint64(msg[0:]), peer: string(msg[8:16])}
	vr.granted = msg[16]&1 != 0
	vr.empty = msg[16]&2 != 0
	return vr
}

func (n *raft) handleVoteResponse(sub *subscription, c *client, _ *Account, _, reply string, msg []byte) {
	vr := decodeVoteResponse(msg)
	n.debug("Received a voteResponse %+v", vr)
	if vr == nil {
		n.error("Received malformed vote response for %q", n.group)
		return
	}

	if state := n.State(); state != Candidate && state != Leader {
		n.debug("Ignoring old vote response, we have stepped down")
		return
	}

	n.votes.push(vr)
}

func (n *raft) processVoteRequest(vr *voteRequest) error {
	// To simplify calling code, we can possibly pass `nil` to this function.
	// If that is the case, does not consider it an error.
	if vr == nil {
		return nil
	}
	n.debug("Received a voteRequest %+v", vr)

	n.Lock()

	vresp := &voteResponse{n.term, n.id, false, n.pindex == 0}
	defer n.debug("Sending a voteResponse %+v -> %q", vresp, vr.reply)

	// Ignore if we are newer. This is important so that we don't accidentally process
	// votes from a previous term if they were still in flight somewhere.
	if vr.term < n.term {
		n.Unlock()
		n.sendReply(vr.reply, vresp.encode())
		return nil
	}

	// If this is a higher term go ahead and stepdown.
	newTerm := vr.term > n.term
	if newTerm {
		n.startLeaderChangeGrace()
		if n.State() != Follower {
			n.debug("Stepping down from %s, detected higher term: %d vs %d",
				strings.ToLower(n.State().String()), vr.term, n.term)
			n.stepdownLocked(noLeader)
		}
		n.cancelCatchup()
		// Persisted below, with our vote if granted.
		n.term = vr.term
		n.vote = noVote
	}

	// Only way we get to yes is through here.
	voteOk := n.vote == noVote || n.vote == vr.candidate

	// If we have an empty log, but are initializing.
	if voteOk && vresp.empty && n.initializing {
		// Reset notion of having an empty log if we're voting during initialization/scale up.
		// Ensures they only need quorum, and not need to hear from all servers.
		vresp.empty = false
	}

	// Other server's log needs to be equal or more up-to-date than ours.
	if voteOk && (vr.lastTerm > n.pterm || vr.lastTerm == n.pterm && vr.lastIndex >= n.pindex) {
		// Persist our vote before granting it. Raft requires the vote to be durable before
		// we respond, otherwise a restart could forget the vote and let us grant a second
		// vote in the same term, which would break election safety.
		n.term = vr.term
		n.vote = vr.candidate
		if err := n.writeTermVote(); err != nil {
			n.vote = noVote
			n.warn("Not granting vote for %q, could not persist term and vote: %v", n.group, err)
		} else {
			vresp.granted = true
			n.resetElectionTimeout()
		}
	} else if newTerm {
		// Persist the new term before responding.
		n.writeTermVote()
		if n.vote == noVote && n.State() != Candidate {
			// We have a more up-to-date log, and haven't voted yet.
			// Start campaigning earlier, but only if not candidate already, as that would short-circuit us.
			// Only on a new term, as re-sent vote requests would otherwise keep postponing our campaign.
			n.resetElect(randCampaignTimeout())
		}
	}

	// Term might have changed, make sure response has the most current
	vresp.term = n.term

	n.Unlock()

	n.sendReply(vr.reply, vresp.encode())

	return nil
}

func (n *raft) handleVoteRequest(sub *subscription, c *client, _ *Account, subject, reply string, msg []byte) {
	vr := decodeVoteRequest(msg, reply)
	if vr == nil {
		n.error("Received malformed vote request for %q", n.group)
		return
	}
	n.reqs.push(vr)
}

func (n *raft) requestVote() {
	n.Lock()
	if n.State() != Candidate {
		n.Unlock()
		return
	}
	n.vote = n.id
	if err := n.writeTermVote(); err != nil {
		// Make sure that our self-vote is persisted durably before campaigning, otherwise
		// we could vote for someone else in the same term after a restart.
		n.vote = noVote
		n.Unlock()
		n.warn("Not requesting votes for %q, could not persist term and vote: %v", n.group, err)
		return
	}
	vr := voteRequest{n.term, n.pterm, n.pindex, n.id, _EMPTY_}
	subj, reply := n.vsubj, n.vreply
	n.Unlock()

	n.debug("Sending out voteRequest %+v", vr)

	// Now send it out.
	n.sendRPC(subj, reply, vr.encode())
}

// resendVoteRequest re-sends the vote request for the current term, our self-vote is already persisted.
func (n *raft) resendVoteRequest() {
	n.RLock()
	if n.State() != Candidate || n.vote != n.id {
		n.RUnlock()
		return
	}
	vr := voteRequest{n.term, n.pterm, n.pindex, n.id, _EMPTY_}
	subj, reply := n.vsubj, n.vreply
	n.RUnlock()

	n.debug("Re-sending voteRequest %+v", vr)
	n.sendRPC(subj, reply, vr.encode())
}

func (n *raft) sendRPC(subject, reply string, msg []byte) {
	n.t.Publish(subject, reply, msg)
}

func (n *raft) sendReply(subject string, msg []byte) {
	n.t.Publish(subject, _EMPTY_, msg)
}

func (n *raft) wonElection(votes int) bool {
	return votes >= n.quorumNeeded()
}

// Return the quorum size for a given cluster config.
func (n *raft) quorumNeeded() int {
	n.RLock()
	qn := n.qn
	n.RUnlock()
	return qn
}

// leadChange signals a leadership change to the upper layer. The term
// identifies the leadership epoch the signal belongs to.
type leadChange struct {
	isLeader bool
	term     uint64
}

// Lock should be held.
func (n *raft) updateLeadChange(isLeader bool) {
	lc := leadChange{isLeader: isLeader, term: n.term}
	// We don't care about values that have not been consumed (transitory states),
	// so we dequeue any state that is pending and push the new one.
	for {
		select {
		case n.leadc <- lc:
			return
		default:
			select {
			case <-n.leadc:
			default:
				// May have been consumed by the "reader" go routine, so go back
				// to the top of the loop and try to send again.
			}
		}
	}
}

// Lock should be held.
func (n *raft) switchState(state RaftState) bool {
retry:
	pstate := n.State()
	if pstate == Closed {
		return false
	}

	// Set our state. If something else has changed our state
	// then retry, this will either be a Stop or Delete call.
	if !n.state.CompareAndSwap(int32(pstate), int32(state)) {
		goto retry
	}

	// Reset the election timer.
	n.resetElectionTimeout()

	var leadChanged bool
	if pstate == Leader && state != Leader {
		leadChanged = true
		n.updateLeadChange(false)
		// Drain the append entry response and proposal queues.
		n.resp.drain()
		n.prop.drain()
	} else if state == Leader && pstate != Leader {
		// Don't updateLeadChange here, it will be done in switchToLeader or after initial messages are applied.
		leadChanged = true
		n.resetPendingEntries()
	}

	n.writeTermVote()
	return leadChanged
}

const (
	noLeader = _EMPTY_
	noVote   = _EMPTY_
)

func (n *raft) switchToFollower(leader string) {
	n.Lock()
	defer n.Unlock()

	n.switchToFollowerLocked(leader)
}

func (n *raft) switchToFollowerLocked(leader string) {
	if n.State() == Closed {
		return
	}

	n.debug("Switching to follower")

	n.startLeaderChangeGrace()
	n.aflr = 0
	n.leaderState.Store(false)
	n.leaderSince.Store(nil)
	n.lxfer = false

	// Reset acks, we can't assume acks from a previous term are still valid in another term.
	if len(n.acks) > 0 {
		n.acks = make(map[uint64]map[string]struct{})
	}
	n.updateLeader(leader)
	n.switchState(Follower)
}

func (n *raft) switchToCandidate() {
	if n.State() == Closed {
		return
	}

	n.Lock()
	defer n.Unlock()

	// If we are catching up or are in observer mode we can not switch.
	// Avoid petitioning to become leader if we're behind on applies.
	if n.observer || n.paused || n.processed < n.commit {
		if n.lxfer {
			// A transfer target retries quickly, before the other followers campaign.
			n.resetElect(minCampaignTimeout)
		} else {
			n.resetElect(minElectionTimeout / 4)
		}
		return
	}

	// Do not campaign with an uncommitted membership change about us,
	// otherwise we would try to become leader outside our peer set.
	if n.membChange != nil && n.membChange.peer == n.id {
		n.resetElect(minElectionTimeout)
		return
	}

	if n.State() != Candidate {
		n.debug("Switching to candidate")
	} else {
		if n.lostQuorumLocked() && time.Since(n.llqrt) > 20*time.Second {
			// We signal to the upper layers such that can alert on quorum lost.
			n.updateLeadChange(false)
			n.llqrt = time.Now()
		}
	}
	// Increment the term.
	n.startLeaderChangeGrace()
	n.term++
	n.vote = noVote
	// Reset quorum paused. If it was previously set, we checked above that we've applied all committed entries.
	n.quorumPaused = false
	// Clear current Leader.
	n.updateLeader(noLeader)
	n.switchState(Candidate)
}

func (n *raft) switchToLeader() {
	if n.State() == Closed {
		return
	}

	n.Lock()
	defer n.Unlock()

	n.debug("Switching to leader")

	n.lxfer = false
	n.updateLeader(n.id)
	n.switchState(Leader)

	// To send out our initial peer state.
	// In our implementation this is equivalent to sending a NOOP-entry upon becoming leader.
	// Wait for this message (and potentially more) to be applied.
	// It's important to wait signaling we're leader if we're not up-to-date yet, as that
	// would mean we're in a consistent state compared with the previous leader.
	n.sendPeerState()
	n.aflr = n.pindex
}
