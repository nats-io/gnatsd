// Copyright 2020-2021 The NATS Authors
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
	"hash"
	"io/ioutil"
	"math/rand"
	"net"
	"os"
	"path"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"

	"github.com/minio/highwayhash"
)

type RaftNode interface {
	Propose(entry []byte) error
	ForwardProposal(entry []byte) error
	InstallSnapshot(snap []byte) error
	SendSnapshot(snap []byte) error
	Applied(index uint64) (entries uint64, bytes uint64)
	Compact(index uint64) error
	State() RaftState
	Size() (entries, bytes uint64)
	Progress() (index, commit, applied uint64)
	Leader() bool
	Quorum() bool
	Current() bool
	GroupLeader() string
	StepDown(preferred ...string) error
	Campaign() error
	ID() string
	Group() string
	Peers() []*Peer
	ProposeAddPeer(peer string) error
	ProposeRemovePeer(peer string) error
	ApplyC() <-chan *CommittedEntry
	PauseApply()
	ResumeApply()
	LeadChangeC() <-chan bool
	// returns quit channel to receive quit from and wait group to decrement afterwards
	QuitC() (<-chan struct{}, *sync.WaitGroup)
	Created() time.Time
	Stop()
	Delete()
}

type WAL interface {
	StoreMsg(subj string, hdr, msg []byte) (uint64, int64, error)
	LoadMsg(index uint64) (subj string, hdr, msg []byte, ts int64, err error)
	RemoveMsg(index uint64) (bool, error)
	Compact(index uint64) (uint64, error)
	Purge() (uint64, error)
	Truncate(seq uint64) error
	State() StreamState
	FastState(*StreamState)
	Stop() error
	Delete() error
}

type LeadChange struct {
	Leader   bool
	Previous string
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
	Observer
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
	case Observer:
		return "OBSERVER"
	case Closed:
		return "CLOSED"
	}
	return "UNKNOWN"
}

type raft struct {
	sync.RWMutex
	created  time.Time
	group    string
	sd       string
	id       string
	wal      WAL
	werr     error
	state    RaftState
	hh       hash.Hash64
	snapfile string
	csz      int
	qn       int
	peers    map[string]*lps
	acks     map[uint64]map[string]struct{}
	pae      map[uint64]*appendEntry
	elect    *time.Timer
	active   time.Time
	llqrt    time.Time
	term     uint64
	pterm    uint64
	pindex   uint64
	commit   uint64
	applied  uint64
	leader   string
	vote     string
	hash     string
	s        *Server
	c        *client
	dflag    bool

	// Subjects for votes, updates, replays.
	psubj  string
	rpsubj string
	vsubj  string
	vreply string
	asubj  string
	areply string

	sq    *sendq
	aesub *subscription

	// For holding term and vote and peerstate to be written.
	wtv   []byte
	wps   []byte
	wtvch chan struct{}
	wpsch chan struct{}

	// For when we need to catch up as a follower.
	catchup *catchupState

	// For leader or server catching up a follower.
	progress map[string]chan uint64

	// For when we have paused our applyC.
	paused  bool
	hcommit uint64

	// Channels
	propc    chan *Entry
	entryc   chan *appendEntry
	respc    chan *appendEntryResponse
	applyc   chan *CommittedEntry
	quit     chan struct{}
	reqs     chan *voteRequest
	votes    chan *voteResponse
	leadc    chan bool
	stepdown chan string

	// wait group to clean up raft specific go routines
	wgQuit sync.WaitGroup
}

// cacthupState structure that holds our subscription, and catchup term and index
// as well as starting term and index and how many updates we have seen.
type catchupState struct {
	sub    *subscription
	cterm  uint64
	cindex uint64
	pterm  uint64
	pindex uint64
	active time.Time
}

// lps holds peer state of last time and last index replicated.
type lps struct {
	ts int64
	li uint64
}

const (
	minElectionTimeout = 1500 * time.Millisecond
	maxElectionTimeout = 5 * minElectionTimeout
	minCampaignTimeout = 100 * time.Millisecond
	maxCampaignTimeout = 4 * minCampaignTimeout
	hbInterval         = 500 * time.Millisecond
	lostQuorumInterval = hbInterval * 5
)

type RaftConfig struct {
	Name  string
	Store string
	Log   WAL
}

var (
	errProposalFailed  = errors.New("raft: proposal failed")
	errNotLeader       = errors.New("raft: not leader")
	errAlreadyLeader   = errors.New("raft: already leader")
	errNilCfg          = errors.New("raft: no config given")
	errUnknownPeer     = errors.New("raft: unknown peer")
	errCorruptPeers    = errors.New("raft: corrupt peer state")
	errStepdownFailed  = errors.New("raft: stepdown failed")
	errEntryLoadFailed = errors.New("raft: could not load entry from WAL")
	errNodeClosed      = errors.New("raft: node is closed")
	errBadSnapName     = errors.New("raft: snapshot name could not be parsed")
	errNoSnapAvailable = errors.New("raft: no snapshot available")
	errSnapshotCorrupt = errors.New("raft: snapshot corrupt")
	errTooManyPrefs    = errors.New("raft: stepdown requires at most one preferred new leader")
	errStepdownNoPeer  = errors.New("raft: stepdown failed, could not match new leader")
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
		s.Debugf("Determining expected peer size for JetStream metacontroller")
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
			for _, u := range gw.URLs {
				host := u.Hostname()
				// If this is an IP just add one.
				if net.ParseIP(host) != nil {
					ngwps++
				} else {
					addrs, _ := net.LookupHost(host)
					ngwps += len(addrs)
				}
			}
		}

		if expected < nrs+ngwps {
			expected = nrs + ngwps
			s.Debugf("Adjusting expected peer set size to %d with %d known", expected, len(knownPeers))
		}
	}

	// Check the store directory. If we have a memory based WAL we need to make sure the directory is setup.
	if stat, err := os.Stat(cfg.Store); os.IsNotExist(err) {
		if err := os.MkdirAll(cfg.Store, 0750); err != nil {
			return fmt.Errorf("raft: could not create storage directory - %v", err)
		}
	} else if stat == nil || !stat.IsDir() {
		return fmt.Errorf("raft: storage directory is not a directory")
	}
	tmpfile, err := ioutil.TempFile(cfg.Store, "_test_")
	if err != nil {
		return fmt.Errorf("raft: storage directory is not writable")
	}
	os.Remove(tmpfile.Name())

	return writePeerState(cfg.Store, &peerState{knownPeers, expected})
}

// startRaftNode will start the raft node.
func (s *Server) startRaftNode(cfg *RaftConfig) (RaftNode, error) {
	if cfg == nil {
		return nil, errNilCfg
	}
	s.mu.Lock()
	if s.sys == nil {
		s.mu.Unlock()
		return nil, ErrNoSysAccount
	}
	sq := s.sys.sq
	sacc := s.sys.account
	hash := s.sys.shash
	s.mu.Unlock()

	ps, err := readPeerState(cfg.Store)
	if err != nil {
		return nil, err
	}
	if ps == nil || (len(ps.knownPeers) < 2 && ps.clusterSize < 2) {
		return nil, errors.New("raft: cluster too small")
	}

	n := &raft{
		created:  time.Now(),
		id:       hash[:idLen],
		group:    cfg.Name,
		sd:       cfg.Store,
		wal:      cfg.Log,
		state:    Follower,
		csz:      ps.clusterSize,
		qn:       ps.clusterSize/2 + 1,
		hash:     hash,
		peers:    make(map[string]*lps),
		acks:     make(map[uint64]map[string]struct{}),
		pae:      make(map[uint64]*appendEntry),
		s:        s,
		c:        s.createInternalSystemClient(),
		sq:       sq,
		quit:     make(chan struct{}),
		wgQuit:   sync.WaitGroup{},
		wtvch:    make(chan struct{}, 1),
		wpsch:    make(chan struct{}, 1),
		reqs:     make(chan *voteRequest, 8),
		votes:    make(chan *voteResponse, 32),
		propc:    make(chan *Entry, 8192),
		entryc:   make(chan *appendEntry, 32768),
		respc:    make(chan *appendEntryResponse, 32768),
		applyc:   make(chan *CommittedEntry, 8192),
		leadc:    make(chan bool, 8),
		stepdown: make(chan string, 8),
	}
	n.c.registerWithAccount(sacc)

	if atomic.LoadInt32(&s.logging.debug) > 0 {
		n.dflag = true
	}

	key := sha256.Sum256([]byte(n.group))
	n.hh, _ = highwayhash.New64(key[:])

	if term, vote, err := n.readTermVote(); err != nil && term > 0 {
		n.term = term
		n.vote = vote
	}

	if err := os.MkdirAll(path.Join(cfg.Store, snapshotsDir), 0750); err != nil {
		return nil, fmt.Errorf("could not create snapshots directory - %v", err)
	}

	// Can't recover snapshots if memory based.
	if _, ok := n.wal.(*memStore); ok {
		snapDir := path.Join(n.sd, snapshotsDir, "*")
		os.RemoveAll(snapDir)
	} else {
		// See if we have any snapshots and if so load and process on startup.
		n.setupLastSnapshot()
	}

	var state StreamState
	n.wal.FastState(&state)
	if state.Msgs > 0 {
		// TODO(dlc) - Recover our state here.
		if first, err := n.loadFirstEntry(); err == nil {
			n.pterm, n.pindex = first.pterm, first.pindex
			if first.commit > 0 && first.commit > n.commit {
				n.commit = first.commit
			}
		}

		// Replay the log.
		// Since doing this in place we need to make sure we have enough room on the applyc.
		needed := state.Msgs + 1 // 1 is for nil to mark end of replay.
		if uint64(cap(n.applyc)) < needed {
			n.applyc = make(chan *CommittedEntry, needed)
		}

		for index := state.FirstSeq; index <= state.LastSeq; index++ {
			ae, err := n.loadEntry(index)
			if err != nil {
				panic("err loading entry from WAL")
			}
			if ae.pindex != index-1 {
				n.warn("Corrupt WAL, truncating and fixing")
				n.truncateWal(ae)
				break
			}
			n.processAppendEntry(ae, nil)
		}
	}

	// Send nil entry to signal the upper layers we are done doing replay/restore.
	n.applyc <- nil

	// Setup our internal subscriptions.
	if err := n.createInternalSubs(); err != nil {
		n.shutdown(true)
		return nil, err
	}

	// Make sure to track ourselves.
	n.trackPeer(n.id)
	// Track known peers
	for _, peer := range ps.knownPeers {
		// Set these to 0 to start.
		if peer != n.id {
			n.peers[peer] = &lps{0, 0}
		}
	}

	n.debug("Started")

	n.Lock()
	n.resetElectionTimeout()
	n.llqrt = time.Now()
	n.Unlock()

	s.registerRaftNode(n.group, n)
	s.startGoRoutine(n.run)
	s.startGoRoutine(n.fileWriter)

	return n, nil
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

// Server will track all raft nodes.
func (s *Server) registerRaftNode(group string, n RaftNode) {
	s.rnMu.Lock()
	defer s.rnMu.Unlock()
	if s.raftNodes == nil {
		s.raftNodes = make(map[string]RaftNode)
	}
	s.raftNodes[group] = n
}

func (s *Server) unregisterRaftNode(group string) {
	s.rnMu.Lock()
	defer s.rnMu.Unlock()
	if s.raftNodes != nil {
		delete(s.raftNodes, group)
	}
}

func (s *Server) lookupRaftNode(group string) RaftNode {
	s.rnMu.RLock()
	defer s.rnMu.RUnlock()
	var n RaftNode
	if s.raftNodes != nil {
		n = s.raftNodes[group]
	}
	return n
}

func (s *Server) reloadDebugRaftNodes() {
	if s == nil {
		return
	}
	debug := atomic.LoadInt32(&s.logging.debug) > 0
	s.rnMu.RLock()
	for _, ni := range s.raftNodes {
		n := ni.(*raft)
		n.Lock()
		n.dflag = debug
		n.Unlock()
	}
	s.rnMu.RUnlock()
}

func (s *Server) shutdownRaftNodes() {
	if s == nil {
		return
	}
	var nodes []RaftNode
	s.rnMu.RLock()
	if len(s.raftNodes) > 0 {
		s.Debugf("Shutting down all raft nodes")
	}
	for _, n := range s.raftNodes {
		nodes = append(nodes, n)
	}
	s.rnMu.RUnlock()

	for _, node := range nodes {
		if node.Leader() {
			node.StepDown()
		}
		node.Stop()
	}
}

func (s *Server) transferRaftLeaders() bool {
	if s == nil {
		return false
	}
	var nodes []RaftNode
	s.rnMu.RLock()
	if len(s.raftNodes) > 0 {
		s.Debugf("Transferring any raft leaders")
	}
	for _, n := range s.raftNodes {
		nodes = append(nodes, n)
	}
	s.rnMu.RUnlock()

	var didTransfer bool
	for _, node := range nodes {
		if node.Leader() {
			node.StepDown()
			didTransfer = true
		}
	}
	return didTransfer
}

// Formal API

// Propose will propose a new entry to the group.
// This should only be called on the leader.
func (n *raft) Propose(data []byte) error {
	n.RLock()
	if n.state != Leader {
		n.RUnlock()
		n.debug("Proposal ignored, not leader")
		return errNotLeader
	}
	// Error if we had a previous write error.
	if werr := n.werr; werr != nil {
		n.RUnlock()
		return werr
	}
	propc := n.propc
	n.RUnlock()

	// For entering and exiting the system, proposals and apply we
	// will block.
	propc <- &Entry{EntryNormal, data}
	return nil
}

// ForwardProposal will forward the proposal to the leader if known.
// If we are the leader this is the same as calling propose.
// FIXME(dlc) - We could have a reply subject and wait for a response
// for retries, but would need to not block and be in separate Go routine.
func (n *raft) ForwardProposal(entry []byte) error {
	if n.Leader() {
		return n.Propose(entry)
	}

	n.sendRPC(n.psubj, _EMPTY_, entry)
	return nil
}

// ProposeAddPeer is called to add a peer to the group.
func (n *raft) ProposeAddPeer(peer string) error {
	n.RLock()
	if n.state != Leader {
		n.RUnlock()
		return errNotLeader
	}
	// Error if we had a previous write error.
	if werr := n.werr; werr != nil {
		n.RUnlock()
		return werr
	}
	propc := n.propc
	n.RUnlock()

	select {
	case propc <- &Entry{EntryAddPeer, []byte(peer)}:
	default:
		return errProposalFailed
	}
	return nil
}

// ProposeRemovePeer is called to remove a peer from the group.
func (n *raft) ProposeRemovePeer(peer string) error {
	n.RLock()
	propc, subj := n.propc, n.rpsubj
	isUs, isLeader := peer == n.id, n.state == Leader
	n.RUnlock()

	if isLeader {
		if isUs {
			n.StepDown()
		} else {
			select {
			case propc <- &Entry{EntryRemovePeer, []byte(peer)}:
			default:
				return errProposalFailed
			}
		}
		return nil
	}

	// Need to forward.
	n.sendRPC(subj, _EMPTY_, []byte(peer))
	return nil
}

// PauseApply will allow us to pause processing of append entries onto our
// external apply chan.
func (n *raft) PauseApply() {
	n.Lock()
	defer n.Unlock()

	n.debug("Pausing apply channel")
	n.paused = true
	n.hcommit = n.commit
}

func (n *raft) ResumeApply() {
	n.Lock()
	defer n.Unlock()

	n.debug("Resuming apply channel")
	n.paused = false
	// Run catchup..
	if n.hcommit > n.commit {
		n.debug("Resuming %d replays", n.hcommit+1-n.commit)
		for index := n.commit + 1; index <= n.hcommit; index++ {
			if err := n.applyCommit(index); err != nil {
				break
			}
		}
	}
	n.hcommit = 0
}

// Compact will compact our WAL.
// This is for when we know we have our state on stable storage.
// E.g. snapshots.
func (n *raft) Compact(index uint64) error {
	n.Lock()
	defer n.Unlock()
	// Error if we had a previous write error.
	if n.werr != nil {
		return n.werr
	}
	_, err := n.wal.Compact(index)
	if err != nil {
		n.setWriteErrLocked(err)
	}
	return err
}

// Applied is to be called when the FSM has applied the committed entries.
// Applied will return the number of entries and an estimation of the
// byte size that could be removed with a snapshot/compact.
func (n *raft) Applied(index uint64) (entries uint64, bytes uint64) {
	n.Lock()
	defer n.Unlock()

	// Ignore if already applied.
	if index > n.applied {
		n.applied = index
	}
	var state StreamState
	n.wal.FastState(&state)
	entries = n.applied - state.FirstSeq
	if state.Msgs > 0 {
		bytes = entries * state.Bytes / state.Msgs
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
	var le = binary.LittleEndian
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
	checksum := n.hh.Sum(nil)
	copy(buf[wi:], checksum)
	wi += len(checksum)
	return buf[:wi]
}

// SendSnapshot will send the latest snapshot as a normal AE.
// Should only be used when the upper layers know this is most recent.
// Used when restoring streams etc.
func (n *raft) SendSnapshot(data []byte) error {
	n.sendAppendEntry([]*Entry{&Entry{EntrySnapshot, data}})
	return nil
}

// Used to install a snapshot for the given term and applied index. This will release
// all of the log entries up to and including index. This should not be called with
// entries that have been applied to the FSM but have not been applied to the raft state.
func (n *raft) InstallSnapshot(data []byte) error {
	n.Lock()
	if n.state == Closed {
		n.Unlock()
		return errNodeClosed
	}

	if werr := n.werr; werr != nil {
		n.Unlock()
		return werr
	}

	var state StreamState
	n.wal.FastState(&state)
	if state.FirstSeq >= n.applied {
		n.Unlock()
		return nil
	}

	n.debug("Installing snapshot of %d bytes", len(data))

	var term uint64
	if ae, _ := n.loadEntry(n.applied); ae != nil {
		term = ae.term
	} else if ae, _ = n.loadFirstEntry(); ae != nil {
		term = ae.term
	} else {
		term = n.pterm
	}

	snap := &snapshot{
		lastTerm:  term,
		lastIndex: n.applied,
		peerstate: encodePeerState(&peerState{n.peerNames(), n.csz}),
		data:      data,
	}

	snapDir := path.Join(n.sd, snapshotsDir)
	sn := fmt.Sprintf(snapFileT, snap.lastTerm, snap.lastIndex)
	sfile := path.Join(snapDir, sn)
	// Remember our latest snapshot file.
	n.snapfile = sfile
	n.Unlock()

	if err := ioutil.WriteFile(sfile, n.encodeSnapshot(snap), 0640); err != nil {
		n.setWriteErr(err)
		return err
	}

	if _, err := n.wal.Compact(snap.lastIndex); err != nil {
		n.setWriteErr(err)
		return err
	}

	psnaps, _ := ioutil.ReadDir(snapDir)
	// Remove any old snapshots.
	for _, fi := range psnaps {
		pn := fi.Name()
		if pn != sn {
			os.Remove(path.Join(snapDir, pn))
		}
	}

	return nil
}

const (
	snapshotsDir = "snapshots"
	snapFileT    = "snap.%d.%d"
)

func termAndIndexFromSnapFile(sn string) (term, index uint64, err error) {
	if sn == _EMPTY_ {
		return 0, 0, errBadSnapName
	}
	fn := filepath.Base(sn)
	if n, err := fmt.Sscanf(fn, snapFileT, &term, &index); err != nil || n != 2 {
		return 0, 0, errBadSnapName
	}
	return term, index, nil
}

func (n *raft) setupLastSnapshot() {
	snapDir := path.Join(n.sd, snapshotsDir)
	psnaps, err := ioutil.ReadDir(snapDir)
	if err != nil {
		return
	}

	var lterm, lindex uint64
	var latest string
	for _, sf := range psnaps {
		sfile := path.Join(snapDir, sf.Name())
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

	// Now cleanup any old entries
	for _, sf := range psnaps {
		sfile := path.Join(snapDir, sf.Name())
		if sfile != latest {
			n.debug("Removing old snapshot: %q", sfile)
			os.Remove(sfile)
		}
	}

	if latest == _EMPTY_ {
		return
	}

	// Set latest snapshot we have.
	n.Lock()
	defer n.Unlock()

	n.snapfile = latest
	snap, err := n.loadLastSnapshot()
	if err != nil {
		os.Remove(n.snapfile)
		n.snapfile = _EMPTY_
	} else {
		n.pindex = snap.lastIndex
		n.pterm = snap.lastTerm
		n.commit = snap.lastIndex
		n.applyc <- &CommittedEntry{n.commit, []*Entry{&Entry{EntrySnapshot, snap.data}}}
		if _, err := n.wal.Compact(snap.lastIndex + 1); err != nil {
			n.setWriteErrLocked(err)
		}
	}
}

// loadLastSnapshot will load and return our last snapshot.
// Lock should be held.
func (n *raft) loadLastSnapshot() (*snapshot, error) {
	if n.snapfile == _EMPTY_ {
		return nil, errNoSnapAvailable
	}
	buf, err := ioutil.ReadFile(n.snapfile)
	if err != nil {
		n.warn("Error reading snapshot: %v", err)
		os.Remove(n.snapfile)
		n.snapfile = _EMPTY_
		return nil, err
	}
	if len(buf) < minSnapshotLen {
		n.warn("Snapshot corrupt, too short")
		os.Remove(n.snapfile)
		n.snapfile = _EMPTY_
		return nil, errSnapshotCorrupt
	}

	// Check to make sure hash is consistent.
	hoff := len(buf) - 8
	lchk := buf[hoff:]
	n.hh.Reset()
	n.hh.Write(buf[:hoff])
	if !bytes.Equal(lchk[:], n.hh.Sum(nil)) {
		n.warn("Snapshot corrupt, checksums did not match")
		os.Remove(n.snapfile)
		n.snapfile = _EMPTY_
		return nil, errSnapshotCorrupt
	}

	var le = binary.LittleEndian
	lps := le.Uint32(buf[16:])
	snap := &snapshot{
		lastTerm:  le.Uint64(buf[0:]),
		lastIndex: le.Uint64(buf[8:]),
		peerstate: buf[20 : 20+lps],
		data:      buf[20+lps : hoff],
	}

	return snap, nil
}

// Leader returns if we are the leader for our group.
func (n *raft) Leader() bool {
	if n == nil {
		return false
	}
	n.RLock()
	isLeader := n.state == Leader
	n.RUnlock()
	return isLeader
}

// Lock should be held.
func (n *raft) isCurrent() bool {
	// First check if we match commit and applied.
	if n.commit != n.applied {
		n.debug("Not current, commit %d != applied %d", n.commit, n.applied)
		return false
	}
	// Make sure we are the leader or we know we have heard from the leader recently.
	if n.state == Leader {
		return true
	}

	// Check here on catchup status.
	if cs := n.catchup; cs != nil && n.pterm >= cs.cterm && n.pindex >= cs.cindex {
		n.cancelCatchup()
	}

	// Check to see that we have heard from the current leader lately.
	if n.leader != noLeader && n.leader != n.id && n.catchup == nil {
		const okInterval = int64(hbInterval) * 2
		ts := time.Now().UnixNano()
		if ps := n.peers[n.leader]; ps != nil && ps.ts > 0 && (ts-ps.ts) <= okInterval {
			return true
		}
		n.debug("Not current, no recent leader contact")
	}
	if cs := n.catchup; cs != nil {
		n.debug("Not current, still catching up pindex=%d, cindex=%d", n.pindex, cs.cindex)
	}
	return false
}

// Current returns if we are the leader for our group or an up to date follower.
func (n *raft) Current() bool {
	if n == nil {
		return false
	}
	n.RLock()
	defer n.RUnlock()
	return n.isCurrent()
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

// StepDown will have a leader stepdown and optionally do a leader transfer.
func (n *raft) StepDown(preferred ...string) error {
	n.Lock()

	if len(preferred) > 1 {
		n.Unlock()
		return errTooManyPrefs
	}

	if n.state != Leader {
		n.Unlock()
		return errNotLeader
	}

	n.debug("Being asked to stepdown")

	// See if we have up to date followers.
	nowts := time.Now().UnixNano()
	maybeLeader := noLeader
	if len(preferred) > 0 {
		maybeLeader = preferred[0]
	}
	for peer, ps := range n.peers {
		// If not us and alive and caughtup.
		if peer != n.id && (nowts-ps.ts) < int64(hbInterval*3) {
			if maybeLeader != noLeader && maybeLeader != peer {
				continue
			}
			if si, ok := n.s.nodeToInfo.Load(peer); !ok || si.(nodeInfo).offline {
				continue
			}
			n.debug("Looking at %q which is %v behind", peer, time.Duration(nowts-ps.ts))
			maybeLeader = peer
			break
		}
	}
	stepdown := n.stepdown
	n.Unlock()

	if len(preferred) > 0 && maybeLeader == noLeader {
		return errStepdownNoPeer
	}

	if maybeLeader != noLeader {
		n.debug("Stepping down, selected %q for new leader", maybeLeader)
		n.sendAppendEntry([]*Entry{&Entry{EntryLeaderTransfer, []byte(maybeLeader)}})
	}
	// Force us to stepdown here.
	select {
	case stepdown <- noLeader:
	default:
		return errStepdownFailed
	}
	return nil
}

// Campaign will have our node start a leadership vote.
func (n *raft) Campaign() error {
	n.Lock()
	defer n.Unlock()
	return n.campaign()
}

func randCampaignTimeout() time.Duration {
	delta := rand.Int63n(int64(maxCampaignTimeout - minCampaignTimeout))
	return (minCampaignTimeout + time.Duration(delta))
}

// Campaign will have our node start a leadership vote.
// Lock should be held.
func (n *raft) campaign() error {
	n.debug("Starting campaign")
	if n.state == Leader {
		return errAlreadyLeader
	}
	n.resetElect(randCampaignTimeout())
	return nil
}

// State returns the current state for this node.
func (n *raft) State() RaftState {
	n.RLock()
	defer n.RUnlock()
	return n.state
}

// Progress returns the current index, commit and applied values.
func (n *raft) Progress() (index, commit, applied uint64) {
	n.RLock()
	defer n.RUnlock()
	return n.pindex + 1, n.commit, n.applied
}

// Size returns number of entries and total bytes for our WAL.
func (n *raft) Size() (uint64, uint64) {
	n.RLock()
	var state StreamState
	n.wal.FastState(&state)
	n.RUnlock()
	return state.Msgs, state.Bytes
}

func (n *raft) ID() string {
	if n == nil {
		return _EMPTY_
	}
	n.RLock()
	defer n.RUnlock()
	return n.id
}

func (n *raft) Group() string {
	n.RLock()
	defer n.RUnlock()
	return n.group
}

func (n *raft) Peers() []*Peer {
	n.RLock()
	defer n.RUnlock()

	var peers []*Peer
	for id, ps := range n.peers {
		p := &Peer{
			ID:      id,
			Current: id == n.leader || ps.li >= n.applied,
			Last:    time.Unix(0, ps.ts),
			Lag:     n.commit - ps.li,
		}
		peers = append(peers, p)
	}
	return peers
}

func (n *raft) ApplyC() <-chan *CommittedEntry            { return n.applyc }
func (n *raft) LeadChangeC() <-chan bool                  { return n.leadc }
func (n *raft) QuitC() (<-chan struct{}, *sync.WaitGroup) { return n.quit, &n.wgQuit }

func (n *raft) Created() time.Time {
	n.RLock()
	defer n.RUnlock()
	return n.created
}

func (n *raft) Stop() {
	n.shutdown(false)
}

func (n *raft) Delete() {
	n.shutdown(true)
}

func (n *raft) shutdown(shouldDelete bool) {
	n.Lock()
	if n.state == Closed {
		n.Unlock()
		return
	}
	close(n.quit)
	if c := n.c; c != nil {
		var subs []*subscription
		c.mu.Lock()
		for _, sub := range c.subs {
			subs = append(subs, sub)
		}
		c.mu.Unlock()
		for _, sub := range subs {
			n.unsubscribe(sub)
		}
		c.closeConnection(InternalClient)
	}
	n.state = Closed
	s, g, wal := n.s, n.group, n.wal

	// Delete our peer state and vote state and any snapshots.
	if shouldDelete {
		os.Remove(path.Join(n.sd, peerStateFile))
		os.Remove(path.Join(n.sd, termVoteFile))
		os.RemoveAll(path.Join(n.sd, snapshotsDir))
	}
	n.Unlock()

	n.wgQuit.Wait()

	s.unregisterRaftNode(g)
	if shouldDelete {
		n.debug("Deleted")
	} else {
		n.debug("Shutdown")
	}
	if wal != nil {
		if shouldDelete {
			wal.Delete()
		} else {
			wal.Stop()
		}
	}
}

func (n *raft) newInbox() string {
	var b [replySuffixLen]byte
	rn := rand.Int63()
	for i, l := 0, rn; i < len(b); i++ {
		b[i] = digits[l%base]
		l /= base
	}
	return fmt.Sprintf(raftReplySubj, b[:])
}

const (
	raftVoteSubj       = "$NRG.V.%s"
	raftAppendSubj     = "$NRG.AE.%s"
	raftPropSubj       = "$NRG.P.%s"
	raftRemovePeerSubj = "$NRG.RP.%s"
	raftReplySubj      = "$NRG.R.%s"
)

// Our internal subscribe.
// Lock should be held.
func (n *raft) subscribe(subject string, cb msgHandler) (*subscription, error) {
	return n.s.systemSubscribe(subject, _EMPTY_, false, n.c, cb)
}

// Lock should be held.
func (n *raft) unsubscribe(sub *subscription) {
	if sub != nil {
		n.c.processUnsub(sub.sid)
	}
}

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
	delta := rand.Int63n(int64(maxElectionTimeout - minElectionTimeout))
	return (minElectionTimeout + time.Duration(delta))
}

// Lock should be held.
func (n *raft) resetElectionTimeout() {
	n.resetElect(randElectionTimeout())
}

// Lock should be held.
func (n *raft) resetElect(et time.Duration) {
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

func (n *raft) run() {
	s := n.s
	defer s.grWG.Done()

	for s.isRunning() {
		switch n.State() {
		case Follower:
			n.runAsFollower()
		case Candidate:
			n.runAsCandidate()
		case Leader:
			n.runAsLeader()
		case Observer:
			// TODO(dlc) - fix.
			n.runAsFollower()
		case Closed:
			return
		}
	}
}

func (n *raft) debug(format string, args ...interface{}) {
	if n.dflag {
		nf := fmt.Sprintf("RAFT [%s - %s] %s", n.id, n.group, format)
		n.s.Debugf(nf, args...)
	}
}

func (n *raft) warn(format string, args ...interface{}) {
	nf := fmt.Sprintf("RAFT [%s - %s] %s", n.id, n.group, format)
	n.s.Warnf(nf, args...)
}

func (n *raft) error(format string, args ...interface{}) {
	nf := fmt.Sprintf("RAFT [%s - %s] %s", n.id, n.group, format)
	n.s.Errorf(nf, args...)
}

func (n *raft) electTimer() *time.Timer {
	n.RLock()
	defer n.RUnlock()
	return n.elect
}

func (n *raft) runAsFollower() {
	for {
		elect := n.electTimer()
		select {
		case ae := <-n.entryc:
			n.processAppendEntry(ae, ae.sub)
		case <-n.s.quitCh:
			return
		case <-n.quit:
			return
		case <-elect.C:
			n.switchToCandidate()
			return
		case <-n.respc:
			// Ignore
		case vreq := <-n.reqs:
			n.processVoteRequest(vreq)
		case newLeader := <-n.stepdown:
			n.switchToFollower(newLeader)
			return
		}
	}
}

// CommitEntry is handed back to the user to apply a commit to their FSM.
type CommittedEntry struct {
	Index   uint64
	Entries []*Entry
}

type appendEntry struct {
	leader  string
	term    uint64
	commit  uint64
	pterm   uint64
	pindex  uint64
	entries []*Entry
	// internal use only.
	reply string
	sub   *subscription
	buf   []byte
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

func (ae *appendEntry) String() string {
	return fmt.Sprintf("&{leader:%s term:%d commit:%d pterm:%d pindex:%d entries: %d}",
		ae.leader, ae.term, ae.commit, ae.pterm, ae.pindex, len(ae.entries))
}

const appendEntryBaseLen = idLen + 4*8 + 2

func (ae *appendEntry) encode() []byte {
	var elen int
	for _, e := range ae.entries {
		elen += len(e.Data) + 1 + 4 // 1 is type, 4 is for size.
	}
	var le = binary.LittleEndian
	buf := make([]byte, appendEntryBaseLen+elen)
	copy(buf[:idLen], ae.leader)
	le.PutUint64(buf[8:], ae.term)
	le.PutUint64(buf[16:], ae.commit)
	le.PutUint64(buf[24:], ae.pterm)
	le.PutUint64(buf[32:], ae.pindex)
	le.PutUint16(buf[40:], uint16(len(ae.entries)))
	wi := 42
	for _, e := range ae.entries {
		le.PutUint32(buf[wi:], uint32(len(e.Data)+1))
		wi += 4
		buf[wi] = byte(e.Type)
		wi++
		copy(buf[wi:], e.Data)
		wi += len(e.Data)
	}
	return buf[:wi]
}

// This can not be used post the wire level callback since we do not copy.
func (n *raft) decodeAppendEntry(msg []byte, sub *subscription, reply string) *appendEntry {
	if len(msg) < appendEntryBaseLen {
		return nil
	}

	var le = binary.LittleEndian
	ae := &appendEntry{
		leader: string(msg[:idLen]),
		term:   le.Uint64(msg[8:]),
		commit: le.Uint64(msg[16:]),
		pterm:  le.Uint64(msg[24:]),
		pindex: le.Uint64(msg[32:]),
	}
	// Decode Entries.
	ne, ri := int(le.Uint16(msg[40:])), 42
	for i := 0; i < ne; i++ {
		le := int(le.Uint32(msg[ri:]))
		ri += 4
		etype := EntryType(msg[ri])
		ae.entries = append(ae.entries, &Entry{etype, msg[ri+1 : ri+le]})
		ri += int(le)
	}
	ae.reply = reply
	ae.sub = sub
	ae.buf = msg
	return ae
}

// appendEntryResponse is our response to a received appendEntry.
type appendEntryResponse struct {
	term    uint64
	index   uint64
	peer    string
	success bool
	// internal
	reply string
}

// We want to make sure this does not change from system changing length of syshash.
const idLen = 8
const appendEntryResponseLen = 24 + 1

func (ar *appendEntryResponse) encode() []byte {
	var buf [appendEntryResponseLen]byte
	var le = binary.LittleEndian
	le.PutUint64(buf[0:], ar.term)
	le.PutUint64(buf[8:], ar.index)
	copy(buf[16:], ar.peer)

	if ar.success {
		buf[24] = 1
	} else {
		buf[24] = 0
	}
	return buf[:appendEntryResponseLen]
}

func (n *raft) decodeAppendEntryResponse(msg []byte) *appendEntryResponse {
	if len(msg) != appendEntryResponseLen {
		return nil
	}
	var le = binary.LittleEndian
	ar := &appendEntryResponse{
		term:  le.Uint64(msg[0:]),
		index: le.Uint64(msg[8:]),
		peer:  string(msg[16 : 16+idLen]),
	}
	ar.success = msg[24] == 1
	return ar
}

// Called when a remove peer proposal has been forwarded
func (n *raft) handleForwardedRemovePeerProposal(sub *subscription, c *client, _, reply string, msg []byte) {
	n.debug("Received forwarded remove peer proposal: %q", msg)

	if !n.Leader() {
		n.debug("Ignoring forwarded peer removal proposal, not leader")
		return
	}
	if len(msg) != idLen {
		n.warn("Received invalid peer name for remove proposal: %q", msg)
		return
	}
	// Need to copy since this is underlying client/route buffer.
	peer := string(append(msg[:0:0], msg...))

	n.RLock()
	propc := n.propc
	n.RUnlock()

	select {
	case propc <- &Entry{EntryRemovePeer, []byte(peer)}:
	default:
		n.warn("Failed to place peer removal proposal onto propose channel")
	}
}

// Called when a peer has forwarded a proposal.
func (n *raft) handleForwardedProposal(sub *subscription, c *client, _, reply string, msg []byte) {
	if !n.Leader() {
		n.debug("Ignoring forwarded proposal, not leader")
		return
	}
	// Need to copy since this is underlying client/route buffer.
	msg = append(msg[:0:0], msg...)
	if err := n.Propose(msg); err != nil {
		n.warn("Got error processing forwarded proposal: %v", err)
	}
}

func (n *raft) runAsLeader() {
	n.RLock()
	if n.state == Closed {
		n.RUnlock()
		return
	}
	psubj, rpsubj := n.psubj, n.rpsubj
	n.RUnlock()

	// For forwarded proposals, both normal and remove peer proposals.
	fsub, err := n.subscribe(psubj, n.handleForwardedProposal)
	if err != nil {
		n.debug("Error subscribing to forwarded proposals: %v", err)
		return
	}
	rpsub, err := n.subscribe(rpsubj, n.handleForwardedRemovePeerProposal)
	if err != nil {
		n.debug("Error subscribing to forwarded proposals: %v", err)
		return
	}

	// Cleanup our subscription when we leave.
	defer func() {
		n.Lock()
		n.unsubscribe(fsub)
		n.unsubscribe(rpsub)
		n.Unlock()
	}()

	n.sendPeerState()

	hb := time.NewTicker(hbInterval)
	defer hb.Stop()

	for {
		select {
		case <-n.s.quitCh:
			return
		case <-n.quit:
			return
		case ar := <-n.respc:
			n.processAppendEntryResponse(ar)
		case b := <-n.propc:
			entries := []*Entry{b}
			if b.Type == EntryNormal {
				const maxBatch = 256 * 1024
			gather:
				for sz := 0; sz < maxBatch; {
					select {
					case e := <-n.propc:
						entries = append(entries, e)
						sz += len(e.Data) + 1
					default:
						break gather
					}
				}
			}
			n.sendAppendEntry(entries)
		case <-hb.C:
			if n.notActive() {
				n.sendHeartbeat()
			}
			if n.lostQuorum() {
				n.switchToFollower(noLeader)
				return
			}
		case vresp := <-n.votes:
			if vresp.term > n.currentTerm() {
				n.switchToFollower(noLeader)
				return
			}
			n.trackPeer(vresp.peer)
		case vreq := <-n.reqs:
			n.processVoteRequest(vreq)
		case newLeader := <-n.stepdown:
			n.switchToFollower(newLeader)
			return
		case ae := <-n.entryc:
			n.processAppendEntry(ae, ae.sub)
		}
	}
}

// Quorum reports the quorum status. Will be called on former leaders.
func (n *raft) Quorum() bool {
	n.RLock()
	defer n.RUnlock()

	now, nc := time.Now().UnixNano(), 1
	for _, peer := range n.peers {
		if now-peer.ts < int64(lostQuorumInterval) {
			nc++
			if nc >= n.qn {
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
	now, nc := time.Now().UnixNano(), 1
	for _, peer := range n.peers {
		if now-peer.ts < int64(lostQuorumInterval) {
			nc++
			if nc >= n.qn {
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
func (n *raft) currentTerm() uint64 {
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

func (n *raft) runCatchup(ar *appendEntryResponse, indexUpdatesC <-chan uint64) {
	n.RLock()
	s, reply := n.s, n.areply
	peer, subj, last := ar.peer, ar.reply, n.pindex
	n.RUnlock()

	defer s.grWG.Done()

	defer func() {
		n.Lock()
		delete(n.progress, peer)
		if len(n.progress) == 0 {
			n.progress = nil
		}
		// Check if this is a new peer and if so go ahead and propose adding them.
		_, exists := n.peers[peer]
		n.Unlock()
		if !exists {
			n.debug("Catchup done for %q, will add into peers", peer)
			n.ProposeAddPeer(peer)
		}
	}()

	n.debug("Running catchup for %q", peer)

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
					n.debug("Got an error loading %d index: %v", next, err)
				}
				return true
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
	for n.Leader() {
		select {
		case <-n.s.quitCh:
			return
		case <-n.quit:
			return
		case <-stepCheck.C:
			if !n.Leader() {
				n.debug("Catching up canceled, no longer leader")
				return
			}
		case <-timeout.C:
			n.debug("Catching up for %q stalled", peer)
			return
		case index := <-indexUpdatesC:
			// Update our activity timer.
			timeout.Reset(activityInterval)
			// Update outstanding total.
			total -= om[index]
			delete(om, index)
			// Still have more catching up to do.
			if next < index {
				n.debug("Adjusting next to %d from %d", index, next)
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

// Lock should be held.
func (n *raft) sendSnapshotToFollower(subject string) (uint64, error) {
	snap, err := n.loadLastSnapshot()
	if err != nil {
		return 0, err
	}
	// Go ahead and send the snapshot and peerstate here as first append entry to the catchup follower.
	ae := n.buildAppendEntry([]*Entry{&Entry{EntrySnapshot, snap.data}, &Entry{EntryPeerState, snap.peerstate}})
	ae.pterm, ae.pindex = snap.lastTerm, snap.lastIndex
	n.sendRPC(subject, n.areply, ae.encode())
	return snap.lastIndex, nil
}

func (n *raft) catchupFollower(ar *appendEntryResponse) {
	n.debug("Being asked to catch up follower: %q", ar.peer)
	n.Lock()
	if n.progress == nil {
		n.progress = make(map[string]chan uint64)
	}

	if ch, ok := n.progress[ar.peer]; ok {
		n.debug("Will cancel existing entry for catching up %q", ar.peer)
		delete(n.progress, ar.peer)
		// Try to pop them out but make sure to not block.
		select {
		case ch <- n.pindex:
		default:
		}
	}
	// Check to make sure we have this entry.
	start := ar.index + 1
	var state StreamState
	n.wal.FastState(&state)

	if start < state.FirstSeq {
		n.debug("Need to send snapshot to follower")
		if lastIndex, err := n.sendSnapshotToFollower(ar.reply); err != nil {
			n.error("Error sending snapshot to follower [%s]: %v", ar.peer, err)
			n.attemptStepDown(noLeader)
			n.Unlock()
			return
		} else {
			n.debug("Snapshot sent, reset first entry to %d", lastIndex)
			start = lastIndex
		}
	}

	ae, err := n.loadEntry(start)
	if err != nil {
		ae, err = n.loadFirstEntry()
	}
	if err != nil || ae == nil {
		n.debug("Could not find a starting entry: %v", err)
		n.Unlock()
		return
	}
	if ae.pindex != ar.index || ae.pterm != ar.term {
		n.debug("Our first entry does not match request from follower")
	}
	// Create a chan for delivering updates from responses.
	isz := 64
	if ar.index > ae.pindex && ar.index-ae.pindex > uint64(isz) {
		isz = int(ar.index - ae.pindex)
	}
	indexUpdates := make(chan uint64, isz)
	indexUpdates <- ae.pindex
	n.progress[ar.peer] = indexUpdates
	n.Unlock()

	n.s.startGoRoutine(func() { n.runCatchup(ar, indexUpdates) })
}

func (n *raft) loadEntry(index uint64) (*appendEntry, error) {
	_, _, msg, _, err := n.wal.LoadMsg(index)
	if err != nil {
		return nil, err
	}
	return n.decodeAppendEntry(msg, nil, _EMPTY_), nil
}

// applyCommit will update our commit index and apply the entry to the apply chan.
// lock should be held.
func (n *raft) applyCommit(index uint64) error {
	if n.state == Closed {
		return errNodeClosed
	}
	if index <= n.commit {
		n.debug("Ignoring apply commit for %d, already processed", index)
		return nil
	}
	original := n.commit
	n.commit = index

	if n.state == Leader {
		delete(n.acks, index)
	}

	var fpae bool

	ae := n.pae[index]
	if ae == nil {
		var state StreamState
		n.wal.FastState(&state)
		if index < state.FirstSeq {
			return nil
		}
		var err error
		if ae, err = n.loadEntry(index); err != nil {
			if err != ErrStoreClosed && err != ErrStoreEOF {
				if err == errBadMsg {
					n.setWriteErrLocked(err)
				}
				n.warn("Got an error loading %d index: %v", index, err)
			}
			n.commit = original
			return errEntryLoadFailed
		}
	} else {
		fpae = true
	}

	ae.buf = nil

	var committed []*Entry
	for _, e := range ae.entries {
		switch e.Type {
		case EntryNormal:
			committed = append(committed, e)
		case EntryOldSnapshot:
			// For old snapshots in our WAL.
			committed = append(committed, &Entry{EntrySnapshot, e.Data})
		case EntrySnapshot:
			committed = append(committed, e)
		case EntryPeerState:
			if n.state != Leader {
				if ps, err := decodePeerState(e.Data); err == nil {
					n.processPeerState(ps)
				}
			}
		case EntryAddPeer:
			newPeer := string(e.Data)
			n.debug("Added peer %q", newPeer)
			if _, ok := n.peers[newPeer]; !ok {
				// We are not tracking this one automatically so we need to bump cluster size.
				n.debug("Expanding our clustersize: %d -> %d", n.csz, n.csz+1)
				n.csz++
				n.qn = n.csz/2 + 1
				n.peers[newPeer] = &lps{time.Now().UnixNano(), 0}
			}
			n.writePeerState(&peerState{n.peerNames(), n.csz})
		case EntryRemovePeer:
			oldPeer := string(e.Data)
			n.debug("Removing peer %q", oldPeer)

			// FIXME(dlc) - Check if this is us??
			if _, ok := n.peers[oldPeer]; ok {
				// We should decrease our cluster size since we are tracking this peer.
				if n.csz > 1 {
					n.debug("Decreasing our clustersize: %d -> %d", n.csz, n.csz-1)
					n.csz--
					n.qn = n.csz/2 + 1
				} else {
					n.warn("Not decreasing further our clustersize: %d", n.csz)
				}
				delete(n.peers, oldPeer)
			}
			n.writePeerState(&peerState{n.peerNames(), n.csz})
			// We pass these up as well.
			committed = append(committed, e)
		}
	}
	// Pass to the upper layers if we have normal entries.
	if len(committed) > 0 {
		if fpae {
			delete(n.pae, index)
		}
		// For entering and exiting the system, proposals and apply we
		// will block.
		closed := n.state == Closed
		n.Unlock()
		if !closed {
			n.applyc <- &CommittedEntry{index, committed}
		}
		n.Lock()
	} else {
		// If we processed inline update our applied index.
		n.applied = index
	}
	return nil
}

// Used to track a success response and apply entries.
func (n *raft) trackResponse(ar *appendEntryResponse) {
	n.Lock()

	// Update peer's last index.
	if ps := n.peers[ar.peer]; ps != nil && ar.index > ps.li {
		ps.li = ar.index
	}

	// If we are tracking this peer as a catchup follower, update that here.
	if indexUpdateC := n.progress[ar.peer]; indexUpdateC != nil {
		select {
		case indexUpdateC <- ar.index:
		default:
			n.warn("TrackResponse failed to place progress update on internal channel")
		}
	}

	// Ignore items already committed.
	if ar.index <= n.commit {
		n.Unlock()
		return
	}

	// See if we have items to apply.
	var sendHB bool

	if results := n.acks[ar.index]; results != nil {
		results[ar.peer] = struct{}{}
		if nr := len(results); nr >= n.qn {
			// We have a quorum.
			for index := n.commit + 1; index <= ar.index; index++ {
				if err := n.applyCommit(index); err != nil {
					break
				}
			}
			sendHB = len(n.propc) == 0
		}
	}

	n.Unlock()

	if sendHB {
		n.sendHeartbeat()
	}
}

// Track interactions with this peer.
func (n *raft) trackPeer(peer string) error {
	n.Lock()
	var needPeerUpdate bool
	if n.state == Leader {
		if _, ok := n.peers[peer]; !ok {
			// This is someone new, if we have registered all of the peers already
			// this is an error.
			if len(n.peers) >= n.csz {
				n.Unlock()
				return errUnknownPeer
			}
			needPeerUpdate = true
		}
	}
	if ps := n.peers[peer]; ps != nil {
		ps.ts = time.Now().UnixNano()
	} else {
		n.peers[peer] = &lps{time.Now().UnixNano(), 0}
	}
	n.Unlock()

	if needPeerUpdate {
		n.sendPeerState()
	}
	return nil
}

func (n *raft) runAsCandidate() {
	n.Lock()
	// Drain old responses.
	for len(n.votes) > 0 {
		<-n.votes
	}
	n.Unlock()

	// Send out our request for votes.
	n.requestVote()

	// We vote for ourselves.
	votes := 1
	won := false

	for {
		elect := n.electTimer()
		select {
		case ae := <-n.entryc:
			n.processAppendEntry(ae, ae.sub)
		case <-n.respc:
			// Ignore
		case <-n.s.quitCh:
			return
		case <-n.quit:
			return
		case <-elect.C:
			if won {
				// we are here if we won the election but some server did not respond
				n.switchToLeader()
			} else {
				n.switchToCandidate()
			}
			return
		case vresp := <-n.votes:
			if vresp.granted && n.term >= vresp.term {
				// only track peers that would be our followers
				n.trackPeer(vresp.peer)
				votes++
				if n.wonElection(votes) {
					// TODO If this server was also leader in n.term-1, then we could skip the timer as well.
					// This would be ok as we'd be guaranteed to have the latest history.
					if len(n.peers) == votes {
						// Become LEADER if we have won and gotten a quorum with everyone
						n.switchToLeader()
						return
					} else {
						// Not everyone is in this quorum, yet?
						// Wait for the remaining responses and become leader once everyone did.
						// Or Wait until after the election timeout and become leader then.
						// In case another server responds with vresp.granted==false and vresp.term > n.term,
						// we will start all over again.
						won = true
					}
				}
			} else if vresp.term > n.term {
				// if we observe a bigger term, we should start over again or risk forming a quorum fully knowing
				// someone with a better term exists. This is even the right thing to do if won == true.
				n.switchToCandidate()
				return
			}
		case vreq := <-n.reqs:
			n.processVoteRequest(vreq)
		case newLeader := <-n.stepdown:
			n.switchToFollower(newLeader)
			return
		}
	}
}

// handleAppendEntry handles an append entry from the wire.
func (n *raft) handleAppendEntry(sub *subscription, c *client, subject, reply string, msg []byte) {
	msg = append(msg[:0:0], msg...)
	if ae := n.decodeAppendEntry(msg, sub, reply); ae != nil {
		select {
		case n.entryc <- ae:
		default:
			n.warn("AppendEntry failed to be placed on internal channel")
		}
	}
}

// Lock should be held.
func (n *raft) cancelCatchup() {
	n.debug("Canceling catchup subscription since we are now up to date")
	if n.catchup != nil && n.catchup.sub != nil {
		n.unsubscribe(n.catchup.sub)
	}
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

// Lock should be held.
func (n *raft) createCatchup(ae *appendEntry) string {
	// Cleanup any old ones.
	if n.catchup != nil && n.catchup.sub != nil {
		n.unsubscribe(n.catchup.sub)
	}
	// Snapshot term and index.
	n.catchup = &catchupState{
		cterm:  ae.pterm,
		cindex: ae.pindex,
		pterm:  n.pterm,
		pindex: n.pindex,
		active: time.Now(),
	}
	inbox := n.newInbox()
	sub, _ := n.subscribe(inbox, n.handleAppendEntry)
	n.catchup.sub = sub

	return inbox
}

// Attempt to stepdown, lock should be held.
func (n *raft) attemptStepDown(newLeader string) {
	select {
	case n.stepdown <- newLeader:
	default:
		n.debug("Failed to place stepdown for new leader %q for %q", newLeader, n.group)
	}
}

func (n *raft) truncateWal(ae *appendEntry) {
	n.debug("Truncating and repairing WAL")

	// Special case if already at 0.
	if ae.pindex == 0 {
		n.pindex = ae.pindex
		n.pterm = ae.pterm
		n.wal.Purge()
		return
	}

	tindex := ae.pindex - 1
	if err := n.wal.Truncate(tindex); err != nil {
		n.setWriteErrLocked(err)
		return
	}
	n.pindex = tindex
	if nae, _ := n.loadEntry(tindex); nae != nil {
		n.pterm = nae.term
	} else {
		n.pterm = ae.term
	}
}

// processAppendEntry will process an appendEntry.
func (n *raft) processAppendEntry(ae *appendEntry, sub *subscription) {
	n.Lock()

	// Just return if closed or we had previous write error.
	if n.state == Closed || n.werr != nil {
		n.Unlock()
		return
	}

	// Are we receiving from another leader.
	if n.state == Leader {
		if ae.term > n.term {
			n.term = ae.term
			n.vote = noVote
			n.writeTermVote()
			n.debug("Received append entry from another leader, stepping down to %q", ae.leader)
			n.attemptStepDown(ae.leader)
		} else {
			// Let them know we are the leader.
			ar := &appendEntryResponse{n.term, n.pindex, n.id, false, _EMPTY_}
			n.Unlock()
			n.debug("AppendEntry ignoring old term from another leader")
			n.sendRPC(ae.reply, _EMPTY_, ar.encode())
			return
		}
	}

	// If we received an append entry as a candidate we should convert to a follower.
	if n.state == Candidate {
		n.debug("Received append entry in candidate state from %q, converting to follower", ae.leader)
		if n.term < ae.term {
			n.term = ae.term
			n.vote = noVote
			n.writeTermVote()
		}
		n.attemptStepDown(ae.leader)
	}

	n.resetElectionTimeout()

	// Catching up state.
	catchingUp := n.catchup != nil
	// Is this a new entry?
	isNew := sub != nil && sub == n.aesub

	// Track leader directly
	if isNew && ae.leader != noLeader {
		if ps := n.peers[ae.leader]; ps != nil {
			ps.ts = time.Now().UnixNano()
		} else {
			n.peers[ae.leader] = &lps{time.Now().UnixNano(), 0}
		}
	}

	// Ignore old terms.
	if isNew && ae.term < n.term {
		ar := &appendEntryResponse{n.term, n.pindex, n.id, false, _EMPTY_}
		n.Unlock()
		n.debug("AppendEntry ignoring old term")
		n.sendRPC(ae.reply, _EMPTY_, ar.encode())
		return
	}

	// If we are catching up ignore old catchup subs.
	// This could happen when we stall or cancel a catchup.
	if !isNew && catchingUp && sub != n.catchup.sub {
		n.Unlock()
		n.debug("AppendEntry ignoring old entry from previous catchup")
		return
	}

	// Check state if we are catching up.
	if catchingUp {
		if cs := n.catchup; cs != nil && n.pterm >= cs.cterm && n.pindex >= cs.cindex {
			// If we are here we are good, so if we have a catchup pending we can cancel.
			n.cancelCatchup()
		} else if isNew {
			var ar *appendEntryResponse
			var inbox string
			// Check to see if we are stalled. If so recreate our catchup state and resend response.
			if n.catchupStalled() {
				n.debug("Catchup may be stalled, will request again")
				inbox = n.createCatchup(ae)
				ar = &appendEntryResponse{n.pterm, n.pindex, n.id, false, _EMPTY_}
			}
			n.Unlock()
			if ar != nil {
				n.sendRPC(ae.reply, inbox, ar.encode())
			}
			// Ignore new while catching up or replaying.
			return
		}
	}

	// If this term is greater than ours.
	if ae.term > n.term {
		n.term = ae.term
		n.vote = noVote
		if isNew {
			n.writeTermVote()
		}
		if n.state != Follower {
			n.debug("Term higher than ours and we are not a follower: %v, stepping down to %q", n.state, ae.leader)
			n.attemptStepDown(ae.leader)
		}
	}

	if isNew && n.leader != ae.leader && n.state == Follower {
		n.debug("AppendEntry updating leader to %q", ae.leader)
		n.leader = ae.leader
		n.writeTermVote()
		n.resetElectionTimeout()
		n.updateLeadChange(false)
	}

	if ae.pterm != n.pterm || ae.pindex != n.pindex {
		// If this is a lower index than what we were expecting.
		if ae.pindex < n.pindex {
			var ar *appendEntryResponse
			if eae, err := n.loadEntry(ae.pindex); err == nil && eae != nil {
				// If terms mismatched, delete that entry and all others past it.
				if ae.pterm > eae.pterm {
					n.truncateWal(ae)
					ar = &appendEntryResponse{n.pterm, n.pindex, n.id, false, _EMPTY_}
				} else {
					ar = &appendEntryResponse{ae.pterm, ae.pindex, n.id, true, _EMPTY_}
				}
			}
			n.Unlock()
			if ar != nil {
				n.sendRPC(ae.reply, _EMPTY_, ar.encode())
			}
			return
		}

		// Check if we are catching up. If we are here we know the leader did not have all of the entries
		// so make sure this is a snapshot entry. If it is not start the catchup process again since it
		// means we may have missed additional messages.
		if catchingUp {
			// Check if only our terms do not match here.
			if ae.pindex == n.pindex {
				n.truncateWal(ae)
				n.cancelCatchup()
				n.Unlock()
				return
			}
			// Snapshots and peerstate will always be together when a leader is catching us up.
			if len(ae.entries) != 2 || ae.entries[0].Type != EntrySnapshot || ae.entries[1].Type != EntryPeerState {
				n.warn("Expected first catchup entry to be a snapshot and peerstate, will retry")
				n.cancelCatchup()
				n.Unlock()
				return
			}

			if ps, err := decodePeerState(ae.entries[1].Data); err == nil {
				n.processPeerState(ps)
				// Also need to copy from client's buffer.
				ae.entries[0].Data = append(ae.entries[0].Data[:0:0], ae.entries[0].Data...)
			} else {
				n.warn("Could not parse snapshot peerstate correctly")
				n.cancelCatchup()
				n.Unlock()
				return
			}
			n.pindex = ae.pindex
			n.pterm = ae.pterm
			n.commit = ae.pindex
			_, err := n.wal.Compact(n.pindex + 1)
			if err != nil {
				n.setWriteErrLocked(err)
				n.Unlock()
				return
			}

			// Now send snapshot to upper levels. Only send the snapshot, not the peerstate entry.
			select {
			case n.applyc <- &CommittedEntry{n.commit, ae.entries[:1]}:
			default:
				n.debug("Failed to place snapshot entry onto our apply channel")
				n.commit--
			}
			n.Unlock()
			return

		} else {
			n.debug("AppendEntry did not match %d %d with %d %d", ae.pterm, ae.pindex, n.pterm, n.pindex)
			// Reset our term.
			n.term = n.pterm
			if ae.pindex > n.pindex {
				// Setup our state for catching up.
				inbox := n.createCatchup(ae)
				ar := &appendEntryResponse{n.pterm, n.pindex, n.id, false, _EMPTY_}
				n.Unlock()
				n.sendRPC(ae.reply, inbox, ar.encode())
				return
			}
		}
	}

	// Heartbeat or do we have entries.
	isHeartbeat := len(ae.entries) == 0

	// Save to our WAL if we have entries.
	if !isHeartbeat {
		// Only store if an original which will have sub != nil
		if sub != nil {
			if err := n.storeToWAL(ae); err != nil {
				if err != ErrStoreClosed {
					n.warn("Error storing entry to WAL: %v", err)
				}
				n.Unlock()
				return
			}
			// Save in memory for faster processing during applyCommit.
			n.pae[n.pindex] = ae
			if len(n.pae) > paeWarnThreshold {
				n.warn("%d append entries pending", len(n.pae))
			}

		} else {
			// This is a replay on startup so just take the appendEntry version.
			n.pterm = ae.term
			n.pindex = ae.pindex + 1
		}

		// Check to see if we have any related entries to process here.
		for _, e := range ae.entries {
			switch e.Type {
			case EntryLeaderTransfer:
				if isNew {
					maybeLeader := string(e.Data)
					if maybeLeader == n.id {
						n.campaign()
					}
				}
			case EntryAddPeer:
				if newPeer := string(e.Data); len(newPeer) == idLen {
					// Track directly
					if ps := n.peers[newPeer]; ps != nil {
						ps.ts = time.Now().UnixNano()
					} else {
						n.peers[newPeer] = &lps{time.Now().UnixNano(), 0}
					}
				}
			}
		}
	}

	// Apply anything we need here.
	if ae.commit > n.commit {
		if n.paused {
			n.hcommit = ae.commit
			n.debug("Paused, not applying %d", ae.commit)
		} else {
			for index := n.commit + 1; index <= ae.commit; index++ {
				if err := n.applyCommit(index); err != nil {
					break
				}
			}
		}
	}

	ar := appendEntryResponse{n.pterm, n.pindex, n.id, true, _EMPTY_}
	n.Unlock()

	// Success. Send our response.
	n.sendRPC(ae.reply, _EMPTY_, ar.encode())
}

// Lock should be held.
func (n *raft) processPeerState(ps *peerState) {
	// Update our version of peers to that of the leader.
	n.csz = ps.clusterSize
	old := n.peers
	n.peers = make(map[string]*lps)
	for _, peer := range ps.knownPeers {
		if lp := old[peer]; lp != nil {
			n.peers[peer] = lp
		} else {
			n.peers[peer] = &lps{0, 0}
		}
	}
	n.debug("Update peers from leader to %+v", n.peers)
	n.writePeerState(ps)
}

// Process a response.
func (n *raft) processAppendEntryResponse(ar *appendEntryResponse) {
	n.trackPeer(ar.peer)

	if ar.success {
		n.trackResponse(ar)
	} else {
		// False here, check to make sure they do not have a higher term.
		if ar.term > n.term {
			n.term = ar.term
			n.vote = noVote
			n.writeTermVote()
			n.Lock()
			n.attemptStepDown(noLeader)
			n.Unlock()
		} else if ar.reply != _EMPTY_ {
			n.catchupFollower(ar)
		}
	}
}

// handleAppendEntryResponse processes responses to append entries.
func (n *raft) handleAppendEntryResponse(sub *subscription, c *client, subject, reply string, msg []byte) {
	msg = append(msg[:0:0], msg...)
	ar := n.decodeAppendEntryResponse(msg)
	ar.reply = reply

	select {
	case n.respc <- ar:
	default:
		n.warn("AppendEntryResponse failed to be placed on internal channel")
	}
}

func (n *raft) buildAppendEntry(entries []*Entry) *appendEntry {
	return &appendEntry{n.id, n.term, n.commit, n.pterm, n.pindex, entries, _EMPTY_, nil, nil}
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
	seq, _, err := n.wal.StoreMsg(_EMPTY_, nil, ae.buf)
	if err != nil {
		n.setWriteErrLocked(err)
		return err
	}

	// Sanity checking for now.
	if ae.pindex != seq-1 {
		fmt.Printf("[%s] n is %+v\n\n", n.s, n)
		fmt.Printf("[%s] n.catchup is %+v\n", n.s, n.catchup)
		fmt.Printf("[%s] n.wal is %+v\n", n.s, n.wal.State())
		if state := n.wal.State(); state.Msgs > 0 {
			for index := state.FirstSeq; index <= state.LastSeq; index++ {
				if nae, _ := n.loadEntry(index); nae != nil {
					fmt.Printf("INDEX %d is %+v\n", index, nae)
					for _, e := range nae.entries {
						fmt.Printf("Entry type is %v\n", e.Type)
					}
				}
			}
		}
		n.Unlock()
		panic(fmt.Sprintf("[%s-%s] Placed an entry at the wrong index, ae is %+v, seq is %d, n.pindex is %d\n\n", n.s, n.group, ae, seq, n.pindex))
	}

	n.pterm = ae.term
	n.pindex = seq
	return nil
}

const paeWarnThreshold = 32 * 1024

func (n *raft) sendAppendEntry(entries []*Entry) {
	n.Lock()
	defer n.Unlock()
	ae := n.buildAppendEntry(entries)
	ae.buf = ae.encode()
	// If we have entries store this in our wal.
	if len(entries) > 0 {
		if err := n.storeToWAL(ae); err != nil {
			n.setWriteErrLocked(err)
			return
		}
		// We count ourselves.
		n.acks[n.pindex] = map[string]struct{}{n.id: struct{}{}}
		n.active = time.Now()

		// Save in memory for faster processing during applyCommit.
		n.pae[n.pindex] = ae
		if len(n.pae) > paeWarnThreshold {
			n.warn("%d append entries pending", len(n.pae))
		}
	}
	n.sendRPC(n.asubj, n.areply, ae.buf)
}

type peerState struct {
	knownPeers  []string
	clusterSize int
}

func peerStateBufSize(ps *peerState) int {
	return 4 + 4 + (8 * len(ps.knownPeers))
}

func encodePeerState(ps *peerState) []byte {
	var le = binary.LittleEndian
	buf := make([]byte, peerStateBufSize(ps))
	le.PutUint32(buf[0:], uint32(ps.clusterSize))
	le.PutUint32(buf[4:], uint32(len(ps.knownPeers)))
	wi := 8
	for _, peer := range ps.knownPeers {
		copy(buf[wi:], peer)
		wi += idLen
	}
	return buf
}

func decodePeerState(buf []byte) (*peerState, error) {
	if len(buf) < 8 {
		return nil, errCorruptPeers
	}
	var le = binary.LittleEndian
	ps := &peerState{clusterSize: int(le.Uint32(buf[0:]))}
	expectedPeers := int(le.Uint32(buf[4:]))
	buf = buf[8:]
	for i, ri, n := 0, 0, expectedPeers; i < n && ri < len(buf); i++ {
		ps.knownPeers = append(ps.knownPeers, string(buf[ri:ri+idLen]))
		ri += idLen
	}
	if len(ps.knownPeers) != expectedPeers {
		return nil, errCorruptPeers
	}
	return ps, nil
}

// Lock should be held.
func (n *raft) peerNames() []string {
	var peers []string
	for peer := range n.peers {
		peers = append(peers, peer)
	}
	return peers
}

func (n *raft) currentPeerState() *peerState {
	n.RLock()
	ps := &peerState{n.peerNames(), n.csz}
	n.RUnlock()
	return ps
}

// sendPeerState will send our current peer state to the cluster.
func (n *raft) sendPeerState() {
	n.sendAppendEntry([]*Entry{&Entry{EntryPeerState, encodePeerState(n.currentPeerState())}})
}

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
	var le = binary.LittleEndian
	le.PutUint64(buf[0:], vr.term)
	le.PutUint64(buf[8:], vr.lastTerm)
	le.PutUint64(buf[16:], vr.lastIndex)
	copy(buf[24:24+idLen], vr.candidate)

	return buf[:voteRequestLen]
}

func (n *raft) decodeVoteRequest(msg []byte, reply string) *voteRequest {
	if len(msg) != voteRequestLen {
		return nil
	}
	// Need to copy for now b/c of candidate.
	msg = append(msg[:0:0], msg...)

	var le = binary.LittleEndian
	return &voteRequest{
		term:      le.Uint64(msg[0:]),
		lastTerm:  le.Uint64(msg[8:]),
		lastIndex: le.Uint64(msg[16:]),
		candidate: string(msg[24 : 24+idLen]),
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
	// Stamp latest and kick writer.
	n.wps = pse
	select {
	case n.wpsch <- struct{}{}:
	default:
	}
}

// Writes out our peer state outside of a specific raft context.
func writePeerState(sd string, ps *peerState) error {
	psf := path.Join(sd, peerStateFile)
	if _, err := os.Stat(psf); err != nil && !os.IsNotExist(err) {
		return err
	}
	if err := ioutil.WriteFile(psf, encodePeerState(ps), 0640); err != nil {
		return err
	}
	return nil
}

func readPeerState(sd string) (ps *peerState, err error) {
	buf, err := ioutil.ReadFile(path.Join(sd, peerStateFile))
	if err != nil {
		return nil, err
	}
	return decodePeerState(buf)
}

const termVoteFile = "tav.idx"
const termVoteLen = idLen + 8

// readTermVote will read the largest term and who we voted from to stable storage.
// Lock should be held.
func (n *raft) readTermVote() (term uint64, voted string, err error) {
	buf, err := ioutil.ReadFile(path.Join(n.sd, termVoteFile))
	if err != nil {
		return 0, noVote, err
	}
	if len(buf) < termVoteLen {
		return 0, noVote, nil
	}
	var le = binary.LittleEndian
	term = le.Uint64(buf[0:])
	voted = string(buf[8:])
	return term, voted, nil
}

// Lock should be held.
func (n *raft) setWriteErrLocked(err error) {
	// Ignore non-write errors.
	if err != nil {
		if err == ErrStoreClosed || err == ErrStoreEOF || err == ErrInvalidSequence || err == ErrStoreMsgNotFound || err == errNoPending {
			return
		}
	}
	n.werr = err

	// For now since this can be happening all under the covers, we will call up and disable JetStream.
	n.Unlock()
	n.s.handleOutOfSpace("")
	n.Lock()
}

// Capture our write error if any and hold.
func (n *raft) setWriteErr(err error) {
	n.Lock()
	defer n.Unlock()
	n.setWriteErrLocked(err)
}

func (n *raft) fileWriter() {
	s := n.s
	defer s.grWG.Done()

	n.RLock()
	tvf := path.Join(n.sd, termVoteFile)
	psf := path.Join(n.sd, peerStateFile)
	n.RUnlock()

	for s.isRunning() {
		select {
		case <-n.quit:
			return
		case <-n.wtvch:
			var buf [termVoteLen]byte
			n.RLock()
			copy(buf[0:], n.wtv)
			n.RUnlock()
			if err := ioutil.WriteFile(tvf, buf[:], 0640); err != nil {
				n.setWriteErr(err)
			}
		case <-n.wpsch:
			n.RLock()
			buf := append(n.wps[:0:0], n.wps...)
			n.RUnlock()
			if err := ioutil.WriteFile(psf, buf, 0640); err != nil {
				n.setWriteErr(err)
			}
		}
	}
}

// writeTermVote will record the largest term and who we voted for to stable storage.
// Lock should be held.
func (n *raft) writeTermVote() {
	var buf [termVoteLen]byte
	var le = binary.LittleEndian
	le.PutUint64(buf[0:], n.term)
	copy(buf[8:], n.vote)
	b := buf[:8+len(n.vote)]

	// If same as what we have we can ignore.
	if bytes.Equal(n.wtv, b) {
		return
	}
	// Stamp latest and kick writer.
	n.wtv = b
	select {
	case n.wtvch <- struct{}{}:
	default:
	}
}

// voteResponse is a response to a vote request.
type voteResponse struct {
	term    uint64
	peer    string
	granted bool
}

const voteResponseLen = 8 + 8 + 1

func (vr *voteResponse) encode() []byte {
	var buf [voteResponseLen]byte
	var le = binary.LittleEndian
	le.PutUint64(buf[0:], vr.term)
	copy(buf[8:], vr.peer)
	if vr.granted {
		buf[16] = 1
	} else {
		buf[16] = 0
	}
	return buf[:voteResponseLen]
}

func (n *raft) decodeVoteResponse(msg []byte) *voteResponse {
	if len(msg) != voteResponseLen {
		return nil
	}
	var le = binary.LittleEndian
	vr := &voteResponse{term: le.Uint64(msg[0:]), peer: string(msg[8:16])}
	vr.granted = msg[16] == 1
	return vr
}

func (n *raft) handleVoteResponse(sub *subscription, c *client, _, reply string, msg []byte) {
	vr := n.decodeVoteResponse(msg)
	n.debug("Received a voteResponse %+v", vr)
	if vr == nil {
		n.error("Received malformed vote response for %q", n.group)
		return
	}
	if state := n.State(); state != Candidate && state != Leader {
		n.debug("Ignoring old vote response, we have stepped down")
		return
	}

	select {
	case n.votes <- vr:
	default:
		// FIXME(dlc)
		n.error("Failed to place vote response on chan for %q", n.group)
	}
}

func (n *raft) processVoteRequest(vr *voteRequest) error {
	n.debug("Received a voteRequest %+v", vr)

	if err := n.trackPeer(vr.candidate); err != nil {
		return err
	}

	n.Lock()
	n.resetElectionTimeout()

	vresp := &voteResponse{n.term, n.id, false}
	defer n.debug("Sending a voteResponse %+v -> %q", vresp, vr.reply)

	// Ignore if we are newer.
	if vr.term < n.term {
		n.Unlock()
		n.sendReply(vr.reply, vresp.encode())
		return nil
	}

	// If this is a higher term go ahead and stepdown.
	if vr.term > n.term {
		n.term = vr.term
		n.vote = noVote
		n.writeTermVote()
		if n.state != Follower {
			n.debug("Stepping down from candidate, detected higher term: %d vs %d", vr.term, n.term)
			n.attemptStepDown(noLeader)
		}
	}

	// Only way we get to yes is through here.
	voteOk := n.vote == noVote || n.vote == vr.candidate
	if voteOk && vr.lastTerm >= n.pterm && vr.lastIndex >= n.pindex {
		vresp.granted = true
		n.vote = vr.candidate
		n.writeTermVote()
	}
	n.Unlock()

	n.sendReply(vr.reply, vresp.encode())

	return nil
}

func (n *raft) handleVoteRequest(sub *subscription, c *client, subject, reply string, msg []byte) {
	vr := n.decodeVoteRequest(msg, reply)
	if vr == nil {
		n.error("Received malformed vote request for %q", n.group)
		return
	}
	select {
	case n.reqs <- vr:
	default:
		n.error("Failed to place vote request on chan for %q", n.group)
	}
}

func (n *raft) requestVote() {
	n.Lock()
	if n.state != Candidate {
		n.Unlock()
		return
	}
	n.vote = n.id
	n.writeTermVote()
	vr := voteRequest{n.term, n.pterm, n.pindex, n.id, _EMPTY_}
	subj, reply := n.vsubj, n.vreply
	n.Unlock()

	n.debug("Sending out voteRequest %+v", vr)

	// Now send it out.
	n.sendRPC(subj, reply, vr.encode())
}

func (n *raft) sendRPC(subject, reply string, msg []byte) {
	n.sq.send(subject, reply, nil, msg)
}

func (n *raft) sendReply(subject string, msg []byte) {
	n.sq.send(subject, _EMPTY_, nil, msg)
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

// Lock should be held.
func (n *raft) updateLeadChange(isLeader bool) {
	select {
	case n.leadc <- isLeader:
	case <-n.leadc:
		// We had an old value not consumed.
		select {
		case n.leadc <- isLeader:
		default:
			n.error("Failed to post lead change to %v for %q", isLeader, n.group)
		}
	}
}

// Lock should be held.
func (n *raft) switchState(state RaftState) {
	if n.state == Closed {
		return
	}

	// Reset the election timer.
	n.resetElectionTimeout()

	if n.state == Leader && state != Leader {
		n.updateLeadChange(false)
	} else if state == Leader && n.state != Leader {
		if len(n.pae) > 0 {
			n.pae = make(map[uint64]*appendEntry)
		}
		n.updateLeadChange(true)
	}

	n.state = state
	n.writeTermVote()
}

const (
	noLeader = _EMPTY_
	noVote   = _EMPTY_
)

func (n *raft) switchToFollower(leader string) {
	n.Lock()
	defer n.Unlock()
	if n.state == Closed {
		return
	}
	n.debug("Switching to follower")
	n.leader = leader
	n.switchState(Follower)
}

func (n *raft) switchToCandidate() {
	n.Lock()
	defer n.Unlock()
	if n.state == Closed {
		return
	}
	if n.state != Candidate {
		n.debug("Switching to candidate")
	} else if n.lostQuorumLocked() {
		if time.Since(n.llqrt) > 20*time.Second {
			// We signal to the upper layers such that can alert on quorum lost.
			n.updateLeadChange(false)
			n.llqrt = time.Now()
		}
	}
	// Increment the term.
	n.term++
	// Clear current Leader.
	n.leader = noLeader
	n.switchState(Candidate)
}

func (n *raft) switchToLeader() {
	n.Lock()
	defer n.Unlock()
	if n.state == Closed {
		return
	}
	n.debug("Switching to leader")
	n.leader = n.id
	n.switchState(Leader)
}
