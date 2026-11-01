// Copyright 2023-2026 The NATS Authors
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

// Do not exlude this file with the !skip_js_tests since those helpers
// are also used by MQTT.

package server

import (
	"encoding/binary"
	"errors"
	"fmt"
	"math/rand/v2"
	"os"
	"sync"
	"testing"
	"time"
)

type stateMachine interface {
	server() *Server
	node() RaftNode
	waitGroup() *sync.WaitGroup
	// This will call forward as needed so can be called on any node.
	propose(data []byte)
	// When entries have been committed and can be applied.
	applyEntry(ce *CommittedEntry)
	// When a leader change happens.
	leaderChange(isLeader bool)
	// Stop the raft group.
	stop()
	// Restart
	restart()
}

// Factory function needed for constructor.
type smFactory func(s *Server, cfg *RaftConfig, node RaftNode) stateMachine

type smGroup []stateMachine

// Leader of the group.
func (sg smGroup) leader() stateMachine {
	for _, sm := range sg {
		if sm.node().Leader() {
			return sm
		}
	}
	return nil
}

func (sg smGroup) followers() smGroup {
	var f []stateMachine
	for _, sm := range sg {
		if sm.node().Leader() {
			continue
		}
		f = append(f, sm)
	}
	return f
}

// Wait on a leader to be elected.
func (sg smGroup) waitOnLeader() stateMachine {
	expires := time.Now().Add(10 * time.Second)
	for time.Now().Before(expires) {
		for _, sm := range sg {
			if sm.node().Leader() {
				return sm
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
	return nil
}

// Pick a random member.
func (sg smGroup) randomMember() stateMachine {
	return sg[rand.IntN(len(sg))]
}

// Return a non-leader
func (sg smGroup) nonLeader() stateMachine {
	for _, sm := range sg {
		if !sm.node().Leader() {
			return sm
		}
	}
	return nil
}

// Take out the lock on all nodes.
func (sg smGroup) lockAll() {
	for _, sm := range sg {
		sm.node().(*raft).Lock()
	}
}

// Release the lock on all nodes.
func (sg smGroup) unlockAll() {
	for _, sm := range sg {
		sm.node().(*raft).Unlock()
	}
}

// Acquire the lock on all follower nodes.
func (sg smGroup) lockFollowers() []stateMachine {
	var locked []stateMachine
	for _, sm := range sg {
		if !sm.node().Leader() {
			locked = append(locked, sm)
			sm.node().(*raft).Lock()
		}
	}
	return locked[:]
}

// Create a raft group and place on numMembers servers at random.
// Filestore based.
func (c *cluster) createRaftGroup(name string, numMembers int, smf smFactory) smGroup {
	return c.createRaftGroupEx(name, numMembers, smf, defaultRaftTransport, FileStorage)
}

func (c *cluster) createMemRaftGroup(name string, numMembers int, smf smFactory) smGroup {
	return c.createRaftGroupEx(name, numMembers, smf, defaultRaftTransport, MemoryStorage)
}

func (c *cluster) createMockMemRaftGroup(name string, members int, smf smFactory) (*raftTransportHub, smGroup) {
	hub := newRaftTransportHub()
	return hub, c.createRaftGroupEx(name, members, smf, hub.newTransport, MemoryStorage)
}

func (c *cluster) createRaftGroupEx(name string, numMembers int, smf smFactory, rtf newTransportFunc, st StorageType) smGroup {
	c.t.Helper()
	if numMembers > len(c.servers) {
		c.t.Fatalf("Members > Peers: %d vs  %d", numMembers, len(c.servers))
	}
	servers := append([]*Server{}, c.servers...)
	rand.Shuffle(len(servers), func(i, j int) { servers[i], servers[j] = servers[j], servers[i] })
	return c.createRaftGroupWithPeers(name, servers[:numMembers], smf, rtf, st)
}

func (c *cluster) createWAL(name string, st StorageType) WAL {
	c.t.Helper()
	var err error
	var store WAL
	if st == FileStorage {
		store, err = newFileStore(
			FileStoreConfig{
				StoreDir:     c.t.TempDir(),
				BlockSize:    defaultMediumBlockSize,
				AsyncFlush:   false,
				SyncInterval: 5 * time.Minute},
			StreamConfig{
				Name:    name,
				Storage: FileStorage})
	} else {
		store, err = newMemStore(
			&StreamConfig{
				Name:    name,
				Storage: MemoryStorage})
	}
	require_NoError(c.t, err)
	return store
}

func serverPeerNames(servers []*Server) []string {
	var peers []string

	for _, s := range servers {
		// generate peer names.
		s.mu.RLock()
		peers = append(peers, s.sys.shash)
		s.mu.RUnlock()
	}

	return peers
}

func (c *cluster) createStateMachine(s *Server, cfg *RaftConfig, peers []string, smf smFactory) stateMachine {
	s.bootstrapRaftNode(cfg, peers, true)
	n, err := s.startRaftNode(globalAccountName, cfg, pprofLabels{})
	require_NoError(c.t, err)
	sm := smf(s, cfg, n)
	go smLoop(sm)
	return sm
}

func (c *cluster) createRaftGroupWithPeers(name string, servers []*Server, smf smFactory, rtf newTransportFunc, st StorageType) smGroup {
	c.t.Helper()

	var sg smGroup
	peers := serverPeerNames(servers)

	for _, s := range servers {
		cfg := &RaftConfig{
			Name:         name,
			Store:        c.t.TempDir(),
			Log:          c.createWAL(name, st),
			NewTransport: rtf}
		sg = append(sg, c.createStateMachine(s, cfg, peers, smf))
	}

	// Start campaigning early to speed up bootstrap leader election.
	sg[0].node().CampaignImmediately()
	return sg
}

func (c *cluster) addNodeEx(name string, smf smFactory, rtf newTransportFunc, st StorageType) stateMachine {
	c.t.Helper()

	server := c.addInNewServer()

	cfg := &RaftConfig{
		Name:         name,
		Store:        c.t.TempDir(),
		Log:          c.createWAL(name, st),
		NewTransport: rtf}

	peers := serverPeerNames(c.servers)
	return c.createStateMachine(server, cfg, peers, smf)
}

func (c *cluster) addRaftNode(name string, smf smFactory) stateMachine {
	return c.addNodeEx(name, smf, defaultRaftTransport, FileStorage)
}

func (c *cluster) addMemRaftNode(name string, smf smFactory) stateMachine {
	return c.addNodeEx(name, smf, defaultRaftTransport, MemoryStorage)
}

func (c *cluster) addMockMemRaftNode(name string, hub *raftTransportHub, smf smFactory) stateMachine {
	return c.addNodeEx(name, smf, hub.newTransport, MemoryStorage)
}

// Driver program for the state machine.
// Should be run in its own go routine.
func smLoop(sm stateMachine) {
	s, n, wg := sm.server(), sm.node(), sm.waitGroup()
	qch, lch, aq := n.QuitC(), n.LeadChangeC(), n.ApplyQ()

	// Wait group used to allow waiting until we exit from here.
	wg.Add(1)
	defer wg.Done()

	for {
		select {
		case <-s.quitCh:
			return
		case <-qch:
			return
		case <-aq.ch:
			ces := aq.pop()
			for _, ce := range ces {
				sm.applyEntry(ce)
			}
			aq.recycle(&ces)

		case lc := <-lch:
			sm.leaderChange(lc.isLeader)
		}
	}
}

// Simple implementation of a replicated state.
// The adder state just sums up int64 values.
type stateAdder struct {
	sync.Mutex
	s   *Server
	n   RaftNode
	wg  sync.WaitGroup
	cfg *RaftConfig
	sum int64
	lch chan bool
}

// Simple getters for server and the raft node.
func (a *stateAdder) server() *Server {
	a.Lock()
	defer a.Unlock()
	return a.s
}

func (a *stateAdder) node() RaftNode {
	a.Lock()
	defer a.Unlock()
	return a.n
}

func (a *stateAdder) waitGroup() *sync.WaitGroup {
	a.Lock()
	defer a.Unlock()
	return &a.wg
}

func (a *stateAdder) propose(data []byte) {
	// Don't hold state machine lock as we could deadlock if the node was locked as part of the test.
	n := a.node()
	n.ForwardProposal(data)
}

func (a *stateAdder) applyEntry(ce *CommittedEntry) {
	a.Lock()
	if ce == nil {
		// This means initial state is done/replayed.
		a.Unlock()
		return
	}
	for _, e := range ce.Entries {
		if e.Type == EntryNormal {
			delta, _ := binary.Varint(e.Data)
			a.sum += delta
		} else if e.Type == EntrySnapshot {
			a.sum, _ = binary.Varint(e.Data)
		}
	}
	// Update applied.
	// But don't hold state machine lock as we could deadlock if the node was locked as part of the test.
	n := a.n
	a.Unlock()
	n.Applied(ce.Index)
}

func (a *stateAdder) leaderChange(isLeader bool) {
	select {
	case a.lch <- isLeader:
	default:
	}
}

// Adder specific to change the total.
func (a *stateAdder) proposeDelta(delta int64) {
	data := make([]byte, binary.MaxVarintLen64)
	n := binary.PutVarint(data, int64(delta))
	a.propose(data[:n])
}

// Stop the group.
func (a *stateAdder) stop() {
	n, wg := a.node(), a.waitGroup()
	n.Stop()
	n.WaitForStop()
	wg.Wait()
}

// Restart the group
func (a *stateAdder) restart() {
	a.Lock()
	defer a.Unlock()

	if a.n.State() != Closed {
		return
	}

	// The filestore is stopped as well, so need to extract the parts to recreate it.
	rn := a.n.(*raft)
	var err error

	switch rn.wal.(type) {
	case *fileStore:
		fs := rn.wal.(*fileStore)
		a.cfg.Log, err = newFileStore(fs.fcfg, fs.cfg.StreamConfig)
	case *memStore:
		ms := rn.wal.(*memStore)
		a.cfg.Log, err = newMemStore(&ms.cfg)
	}
	if err != nil {
		panic(err)
	}

	// Must reset in-memory state.
	// A real restart would not preserve it, but more importantly we have no way to detect if we
	// already applied an entry. So, the sum must only be updated based on append entries or snapshots.
	a.sum = 0

	a.n, err = a.s.startRaftNode(globalAccountName, a.cfg, pprofLabels{})
	if err != nil {
		panic(err)
	}
	// Finally restart the driver.
	go smLoop(a)
}

// Total for the adder state machine.
func (a *stateAdder) total() int64 {
	a.Lock()
	defer a.Unlock()
	return a.sum
}

// Install a snapshot.
func (a *stateAdder) snapshot(t *testing.T) {
	// Don't hold state machine lock as we could deadlock if the node was locked as part of the test.
	a.Lock()
	sum := a.sum
	rn := a.n
	a.Unlock()

	data := make([]byte, binary.MaxVarintLen64)
	n := binary.PutVarint(data, sum)
	snap := data[:n]
	require_NoError(t, rn.InstallSnapshot(snap, false))
}

// Helper to wait for a certain state.
func (rg smGroup) waitOnTotal(t *testing.T, expected int64) {
	t.Helper()
	checkFor(t, 5*time.Second, 200*time.Millisecond, func() error {
		var err error
		for _, sm := range rg {
			if sm.node().State() == Closed {
				continue
			}
			asm := sm.(*stateAdder)
			if total := asm.total(); total != expected {
				err = errors.Join(err, fmt.Errorf("Adder on %v has wrong total: %d vs %d",
					asm.server(), total, expected))
			}
		}
		return err
	})
}

// Factory function.
func newStateAdder(s *Server, cfg *RaftConfig, n RaftNode) stateMachine {
	return &stateAdder{s: s, n: n, cfg: cfg, lch: make(chan bool, 1)}
}

func initSingleMemRaftNode(t *testing.T) (*raft, func()) {
	t.Helper()
	n, c := initSingleMemRaftNodeWithCluster(t)
	cleanup := func() {
		c.shutdown()
	}
	return n, cleanup
}

func initSingleMemRaftNodeWithCluster(t *testing.T) (*raft, *cluster) {
	t.Helper()
	c := createJetStreamClusterExplicit(t, "R3S", 3)
	s := c.servers[0] // RunBasicJetStreamServer not available

	ms, err := newMemStore(&StreamConfig{Name: "TEST", Storage: MemoryStorage})
	require_NoError(t, err)
	cfg := &RaftConfig{Name: "TEST", Store: t.TempDir(), Log: ms}

	id := s.sys.shash[:idLen]
	err = s.bootstrapRaftNode(cfg, []string{id}, true)
	require_NoError(t, err)
	n, err := s.initRaftNode(globalAccountName, cfg, pprofLabels{})
	require_NoError(t, err)

	return n, c
}

// Encode an AppendEntry.
// An AppendEntry is encoded into a buffer and that's stored into the WAL.
// This is a helper function to generate that buffer.
func encode(t *testing.T, ae *appendEntry) *appendEntry {
	t.Helper()
	buf, err := ae.encode(nil)
	require_NoError(t, err)
	ae.buf = buf
	return ae
}

// runRaftTestServer runs a JetStream server to host Raft nodes, as
// RunBasicJetStreamServer isn't built with skip_js_tests.
func runRaftTestServer(t *testing.T) *Server {
	t.Helper()
	o := DefaultTestOptions
	o.Port = -1
	o.JetStream = true
	o.StoreDir = t.TempDir()
	return RunServer(&o)
}

// runServerWaitingForRouting starts a single-member clustered JetStream server
// whose route never connects, so its meta group keeps waiting for routing.
func runServerWaitingForRouting(t *testing.T) *Server {
	t.Helper()
	o := DefaultTestOptions
	o.Port = -1
	o.ServerName = "S"
	o.JetStream = true
	o.StoreDir = t.TempDir()
	o.Cluster.Name = "R1S"
	o.Cluster.Host = o.Host
	o.Cluster.Port = -1
	o.Routes = RoutesFromStr("nats://127.0.0.1:1")
	return RunServer(&o)
}

// requireRaftNodeReleased checks that a stopped node closed its WAL and its
// commit file, if it had one, and is no longer registered with the server.
func requireRaftNodeReleased(t *testing.T, s *Server, n *raft) {
	t.Helper()
	n.RLock()
	fs, cf := n.wal.(*fileStore), n.cf
	n.RUnlock()
	require_True(t, fs.isClosed())
	if cf != nil {
		_, err := cf.Stat()
		require_Error(t, err, os.ErrClosed)
	}
	require_True(t, s.lookupRaftNode(n.group) == nil)
}
