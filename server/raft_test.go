// Copyright 2021-2026 The NATS Authors
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
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"math/rand/v2"
	"net/url"
	"os"
	"path"
	"path/filepath"
	"reflect"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
)

func TestNRGSimple(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "R3S", 3)
	defer c.shutdown()

	rg := c.createRaftGroup("TEST", 3, newStateAdder)
	rg.waitOnLeader()
	// Do several state transitions.
	rg.randomMember().(*stateAdder).proposeDelta(22)
	rg.randomMember().(*stateAdder).proposeDelta(-11)
	rg.randomMember().(*stateAdder).proposeDelta(-10)
	// Wait for all members to have the correct state.
	rg.waitOnTotal(t, 1)
}

func TestNRGSnapshotAndRestart(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "R3S", 3)
	defer c.shutdown()

	rg := c.createRaftGroup("TEST", 3, newStateAdder)
	lsm := rg.waitOnLeader()
	require_NotNil(t, lsm)
	leader := lsm.(*stateAdder)

	var expectedTotal int64

	sm := rg.nonLeader().(*stateAdder)

	for i := 0; i < 1000; i++ {
		delta := rand.Int64N(222)
		expectedTotal += delta
		leader.proposeDelta(delta)

		if i == 250 {
			// Let some things catchup.
			time.Sleep(50 * time.Millisecond)
			// Snapshot leader and stop and snapshot a member.
			leader.snapshot(t)
			sm.snapshot(t)
			sm.stop()
		}
	}
	// Restart.
	sm.restart()
	// Wait for all members to have the correct state.
	rg.waitOnTotal(t, expectedTotal)
}

func TestNRGAppendEntryEncode(t *testing.T) {
	ae := &appendEntry{
		term:   1,
		pindex: 0,
	}

	// Test leader should be _EMPTY_ or exactly idLen long
	ae.leader = "foo_bar_baz"
	_, err := ae.encode(nil)
	require_Error(t, err, errLeaderLen)

	// Empty ok (noLeader)
	ae.leader = noLeader // _EMPTY_
	_, err = ae.encode(nil)
	require_NoError(t, err)

	ae.leader = "DEREK123"
	_, err = ae.encode(nil)
	require_NoError(t, err)

	// Buffer reuse
	var rawSmall [32]byte
	var rawBigger [64]byte

	b := rawSmall[:]
	ae.encode(b)
	if b[0] != 0 {
		t.Fatalf("Expected arg buffer to not be used")
	}
	b = rawBigger[:]
	ae.encode(b)
	if b[0] == 0 {
		t.Fatalf("Expected arg buffer to be used")
	}

	// Test max number of entries.
	for i := 0; i < math.MaxUint16+1; i++ {
		ae.entries = append(ae.entries, &Entry{EntryNormal, nil})
	}
	_, err = ae.encode(b)
	require_Error(t, err, errTooManyEntries)
}

func TestNRGAppendEntryDecode(t *testing.T) {
	ae := &appendEntry{
		leader: "12345678",
		term:   1,
		pindex: 0,
	}
	for i := 0; i < math.MaxUint16; i++ {
		ae.entries = append(ae.entries, &Entry{EntryNormal, nil})
	}
	buf, err := ae.encode(nil)
	require_NoError(t, err)

	// Truncate buffer first.
	short := buf[0 : len(buf)-1025]
	_, err = decodeAppendEntry(short, nil, _EMPTY_)
	require_Error(t, err, errBadAppendEntry)

	for i := 0; i < 100; i++ {
		b := copyBytes(buf)
		// modifying the header (idx < 42) will not result in an error by decodeAppendEntry
		bi := 42 + rand.IntN(len(b)-42)
		if b[bi] != 0 && bi != 40 {
			b[bi] = 0
			_, err = decodeAppendEntry(b, nil, _EMPTY_)
			require_Error(t, err, errBadAppendEntry)
		}
	}
}

func TestNRGAppendEntryDecodeTruncatedEntryLength(t *testing.T) {
	ae := &appendEntry{
		leader:  "12345678",
		term:    1,
		entries: []*Entry{newEntry(EntryNormal, nil)},
	}

	buf, err := ae.encode(nil)
	require_NoError(t, err)

	// Truncate the 4 byte length field of the first entry.
	// The decoder wants to read 4 bytes, finds only 2 bytes left.
	truncated := buf[:appendEntryBaseLen+2]
	_, err = decodeAppendEntry(truncated, nil, _EMPTY_)
	require_Error(t, err, errBadAppendEntry)
}

func TestNRGPeerStateDecodeTruncated(t *testing.T) {
	ps := &peerState{knownPeers: []string{"12345678", "abcdefgh"}, clusterSize: 2}
	buf := encodePeerState(ps)

	// Sanity check the round trip.
	dps, err := decodePeerState(buf)
	require_NoError(t, err)
	require_Equal(t, len(dps.knownPeers), 2)
	require_Equal(t, dps.clusterSize, 2)

	// Header claims one peer but only a partial id (idLen-5 bytes) follows.
	// len == cap so the partial id can not be read past the slice.
	truncated := make([]byte, 8+idLen-5)
	le := binary.LittleEndian
	le.PutUint32(truncated[0:], 1)
	le.PutUint32(truncated[4:], 1)
	_, err = decodePeerState(truncated)
	require_Error(t, err, errCorruptPeers)
}

func TestNRGRecoverFromFollowingNoLeader(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "R3S", 3)
	defer c.shutdown()

	rg := c.createRaftGroup("TEST", 3, newStateAdder)
	rg.waitOnLeader()

	// Find out what term we are on.
	term := rg.leader().node().Term()

	// Start by pausing all of the nodes. This will stop them from
	// processing new entries.
	for _, n := range rg {
		n.node().PauseApply()
	}

	// Now drain all of the ApplyQ entries from them, which will stop
	// them from automatically trying to follow a previous leader if
	// they happened to have received an apply entry from one. Then
	// we're going to force them into a state where they are all
	// followers but they don't have a leader.
	for _, n := range rg {
		rn := n.node().(*raft)
		rn.ApplyQ().drain()
		rn.switchToFollower(noLeader)
	}

	// Resume the nodes.
	for _, n := range rg {
		n.node().ResumeApply()
	}

	// Wait a while. The nodes should notice that they haven't heard
	// from a leader lately and will switch to voting. After an
	// election we should find a new leader and be on a new term.
	rg.waitOnLeader()
	require_True(t, rg.leader() != nil)
	require_NotEqual(t, rg.leader().node().Term(), term)
}

func TestNRGInlineStepdown(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "R3S", 3)
	defer c.shutdown()

	rg := c.createMemRaftGroup("TEST", 3, newStateAdder)
	rg.waitOnLeader()

	// When StepDown() completes, we should not be the leader. Before,
	// this would not be guaranteed as the stepdown could be processed
	// some time later.
	n := rg.leader().node().(*raft)
	require_NoError(t, n.StepDown())
	require_NotEqual(t, n.State(), Leader)
}

func TestNRGMalformedAppendEntryResponse(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "R3S", 3)
	defer c.shutdown()

	rg := c.createMemRaftGroup("TEST", 3, newStateAdder)
	rg.waitOnLeader()

	n := rg.leader().node().(*raft)

	// A message whose length does not match appendEntryResponseLen decodes to
	// nil. This must not cause a nil pointer dereference/panic.
	n.handleAppendEntryResponse(nil, nil, nil, "subject", "reply", []byte{})
	n.handleAppendEntryResponse(nil, nil, nil, "subject", "reply", []byte{1, 2, 3})
}

func TestNRGObserverMode(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "R3S", 3)
	defer c.shutdown()

	rg := c.createRaftGroup("TEST", 3, newStateAdder)
	rg.waitOnLeader()

	// Put all of the followers into observer mode. In this state
	// they will not participate in an election but they will continue
	// to apply incoming commits.
	for _, n := range rg {
		if n.node().Leader() {
			continue
		}
		n.node().SetObserver(true)
	}

	// Propose a change from the leader.
	adder := rg.leader().(*stateAdder)
	adder.proposeDelta(1)
	adder.proposeDelta(2)
	adder.proposeDelta(3)

	// Wait for the followers to apply it.
	rg.waitOnTotal(t, 6)

	// Confirm the followers are still just observers and weren't
	// reset out of that state for some reason.
	for _, n := range rg {
		if n.node().Leader() {
			continue
		}
		require_True(t, n.node().IsObserver())
	}
}

func TestNRGAEFromOldLeader(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "R3S", 3)
	defer c.shutdown()

	nc, _ := jsClientConnect(t, c.leader(), nats.UserInfo("admin", "s3cr3t!"))
	defer nc.Close()

	rg := c.createMemRaftGroup("TEST", 3, newStateAdder)
	rg.waitOnLeader()

	// Listen out for catchup requests.
	ch := make(chan *nats.Msg, 16)
	_, err := nc.ChanSubscribe(fmt.Sprintf(raftCatchupReply, ">"), ch)
	require_NoError(t, err)

	// Start next term so that we can reuse term 1 in the next step.
	leader := rg.leader().node().(*raft)
	leader.StepDown()
	time.Sleep(time.Millisecond * 100)
	rg.waitOnLeader()
	require_Equal(t, leader.Term(), 2)
	leader = rg.leader().node().(*raft)

	// Send an append entry with an outdated term. Beforehand, doing
	// so would have caused a WAL reset and then would have triggered
	// a Raft-level catchup.
	ae := &appendEntry{
		term:   1,
		pindex: 0,
		leader: leader.id,
		reply:  nc.NewRespInbox(),
	}
	payload, err := ae.encode(nil)
	require_NoError(t, err)
	resp, err := nc.Request(leader.asubj, payload, time.Second)
	require_NoError(t, err)

	// Wait for the response, the server should have rejected it.
	ar := decodeAppendEntryResponse(resp.Data)
	require_NotNil(t, ar)
	require_Equal(t, ar.success, false)

	// No catchup should happen at this point because no reset should
	// have happened.
	require_NoChanRead(t, ch, time.Second*2)
}

// TestNRGSimpleElection tests that a simple election succeeds. It is
// simple because the group hasn't processed any entries and hasn't
// suffered any interruptions of any kind, therefore there should be
// no way that the conditions for granting the votes can fail.
func TestNRGSimpleElection(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "R3S", 9)
	defer c.shutdown()
	c.waitOnLeader()

	nc, _ := jsClientConnect(t, c.leader(), nats.UserInfo("admin", "s3cr3t!"))
	defer nc.Close()

	rg := c.createRaftGroup("TEST", 9, newStateAdder)
	rg.waitOnLeader()

	voteReqs := make(chan *nats.Msg, 1)
	voteResps := make(chan *nats.Msg, len(rg)-1)

	// Keep a record of the term when we started.
	leader := rg.leader().node().(*raft)
	startTerm := leader.term

	// Subscribe to the vote request subject, this should be the
	// same across all nodes in the group.
	_, err := nc.ChanSubscribe(leader.vsubj, voteReqs)
	require_NoError(t, err)

	// Subscribe to all of the vote response inboxes for all nodes
	// in the Raft group, as they can differ.
	for _, n := range rg {
		rn := n.node().(*raft)
		_, err = nc.ChanSubscribe(rn.vreply, voteResps)
		require_NoError(t, err)
	}

	// Step down, this will start a new voting session.
	require_NoError(t, rg.leader().node().StepDown())

	// Wait for a vote request to come in.
	msg := require_ChanRead(t, voteReqs, time.Second)
	vr := decodeVoteRequest(msg.Data, msg.Reply)
	require_True(t, vr != nil)
	require_NotEqual(t, vr.candidate, _EMPTY_)

	// The leader should have bumped their term in order to start
	// an election.
	require_Equal(t, vr.term, startTerm+1)
	require_Equal(t, vr.lastTerm, startTerm)

	// Wait for all of the vote responses to come in. There should
	// be as many vote responses as there are followers.
	for i := 0; i < len(rg)-1; i++ {
		msg := require_ChanRead(t, voteResps, time.Second)
		re := decodeVoteResponse(msg.Data)
		require_True(t, re != nil)

		// Ignore old vote responses that could be in-flight.
		if re.term < vr.term {
			i--
			continue
		}

		// The vote should have been granted.
		require_Equal(t, re.granted, true)

		// The node granted the vote, therefore the term in the vote
		// response should have advanced as well.
		require_Equal(t, re.term, vr.term)
		require_Equal(t, re.term, startTerm+1)
	}

	// Everyone in the group should have voted for our candidate
	// and arrived at the term from the vote request.
	rg.lockAll()
	defer rg.unlockAll()
	for _, n := range rg {
		rn := n.node().(*raft)
		require_Equal(t, rn.term, vr.term)
		require_Equal(t, rn.term, startTerm+1)
		require_Equal(t, rn.vote, vr.candidate)
	}
}

// TestNRGLeaderTransfer verifies that a Raft group correctly transfers
// leadership to a chosen preferred node when the current leader steps down.
func TestNRGLeaderTransfer(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "R3S", 3)
	defer c.shutdown()

	nc, _ := jsClientConnect(t, c.leader(), nats.UserInfo("admin", "s3cr3t!"))
	defer nc.Close()

	rg := c.createMemRaftGroup("Test", 3, newStateAdder)
	rg.waitOnLeader()

	leader := rg.leader()
	sub, err := nc.SubscribeSync(leader.node().(*raft).asubj)
	require_NoError(t, err)
	defer sub.Drain()
	require_NoError(t, nc.Flush())

	preferredID := rg.nonLeader().node().ID()
	leader.node().StepDown(preferredID)
	newLeader := rg.waitOnLeader()

	require_Equal(t, newLeader.node().ID(), preferredID)

	// Expect to see a EntryLeader message
	checkFor(t, time.Second, 0, func() (err error) {
		msg, err := sub.NextMsg(time.Second)
		if err != nil {
			return err
		}

		ae, err := decodeAppendEntry(msg.Data, nil, msg.Reply)
		if err != nil {
			return err
		}

		if len(ae.entries) == 1 {
			e := ae.entries[0]
			if e.Type == EntryLeaderTransfer && string(e.Data) == preferredID {
				return nil
			}
		}

		return fmt.Errorf("Expect EntryLeaderTransfer")
	})
}

func TestNRGSwitchStateClearsQueues(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "R3S", 3)
	defer c.shutdown()
	s := c.servers[0] // RunBasicJetStreamServer not available

	n := &raft{
		prop:  newIPQueue[*proposedEntry](s, "prop"),
		resp:  newIPQueue[*appendEntryResponse](s, "resp"),
		leadc: make(chan leadChange, 1), // for switchState
		sd:    t.TempDir(),
		dios:  defaultDiskIOSemaphore(),
	}
	n.state.Store(int32(Leader))
	require_Equal(t, n.prop.len(), 0)
	require_Equal(t, n.resp.len(), 0)

	n.prop.push(&proposedEntry{&Entry{}, _EMPTY_})
	n.resp.push(&appendEntryResponse{})
	require_Equal(t, n.prop.len(), 1)
	require_Equal(t, n.resp.len(), 1)

	n.switchState(Follower)
	require_Equal(t, n.prop.len(), 0)
	require_Equal(t, n.resp.len(), 0)
}

func TestNRGStepDownOnSameTermDoesntClearVote(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "R3S", 3)
	defer c.shutdown()
	c.waitOnLeader()

	nc, _ := jsClientConnect(t, c.leader(), nats.UserInfo("admin", "s3cr3t!"))
	defer nc.Close()

	rg := c.createRaftGroup("TEST", 3, newStateAdder)
	rg.waitOnLeader()

	lsm := rg.leader().(*stateAdder)
	leader := lsm.node().(*raft)
	follower := rg.nonLeader().node().(*raft)

	// Make sure we handle the leader change notification from above.
	require_ChanRead(t, lsm.lch, time.Second)

	// Subscribe to the append entry subject.
	sub, err := nc.SubscribeSync(leader.asubj)
	require_NoError(t, err)

	// Get the first append entry that we receive.
	msg, err := sub.NextMsg(time.Second)
	require_NoError(t, err)
	require_NoError(t, sub.Unsubscribe())

	// We're going to modify the append entry that we received so that
	// we can send it again with modifications.
	ae, err := decodeAppendEntry(msg.Data, nil, msg.Reply)
	require_NoError(t, err)

	// First of all we're going to try sending an append entry that
	// has an old term and a fake leader.
	msg.Reply = follower.areply
	ae.leader = follower.id
	ae.term = leader.term - 1
	msg.Data, err = ae.encode(msg.Data[:0])
	require_NoError(t, err)
	require_NoError(t, nc.PublishMsg(msg))

	// Because the term was old, the fake leader shouldn't matter as
	// the current leader should ignore it.
	require_NoChanRead(t, lsm.lch, time.Second)

	// Now we're going to send it on the same term that the current leader
	// is on. What we expect to happen is that the leader will step down
	// but it *shouldn't* clear the vote.
	ae.term = leader.term
	msg.Data, err = ae.encode(msg.Data[:0])
	require_NoError(t, err)
	require_NoError(t, nc.PublishMsg(msg))

	// Wait for the leader transition and ensure that the vote wasn't
	// cleared.
	require_ChanRead(t, lsm.lch, time.Second)
	require_NotEqual(t, leader.vote, noVote)
}

func TestNRGUnsuccessfulVoteRequestDoesntResetElectionTimer(t *testing.T) {
	// This test relies on nodes not hitting their election timer too often,
	// otherwise the step later where we capture the election time before and
	// after the failed vote request will flake.
	origMinTimeout, origMaxTimeout, origHBInterval := minElectionTimeout, maxElectionTimeout, hbInterval
	minElectionTimeout, maxElectionTimeout, hbInterval = time.Second*5, time.Second*10, time.Second*10
	defer func() {
		minElectionTimeout, maxElectionTimeout, hbInterval = origMinTimeout, origMaxTimeout, origHBInterval
	}()

	c := createJetStreamClusterExplicit(t, "R3S", 3)
	defer c.shutdown()
	c.waitOnLeader()

	nc, _ := jsClientConnect(t, c.leader(), nats.UserInfo("admin", "s3cr3t!"))
	defer nc.Close()

	rg := c.createRaftGroup("TEST", 3, newStateAdder)

	// Because the election timer is quite high, we want to kick a node into
	// campaigning before it naturally needs to, otherwise the test takes a
	// long time just to pick a leader.
	for _, n := range rg {
		n.node().Campaign()
		break
	}
	rg.waitOnLeader()
	rgLeader := rg.leader()
	leader := rgLeader.node().(*raft)
	follower := rg.nonLeader().node().(*raft)

	// Send one message to ensure heartbeats are not sent during the remainder of this test.
	rgLeader.(*stateAdder).proposeDelta(1)
	rg.waitOnTotal(t, 1)

	// Set up a new inbox for the vote responses to go to.
	vsubj, vreply := leader.vsubj, nc.NewInbox()
	ch := make(chan *nats.Msg, 3)
	_, err := nc.ChanSubscribe(vreply, ch)
	require_NoError(t, err)

	// Keep a track of the last time the election timer was reset before this.
	// Also build up a vote request that's obviously behind so that the other
	// nodes should not do anything with it. All locks are taken at the same
	// time so that it guarantees that both the leader and the follower aren't
	// operating at the time we take the etlr snapshots.
	rg.lockAll()
	leader.resetElect(maxElectionTimeout)
	follower.resetElect(maxElectionTimeout)
	leaderOriginal := leader.etlr
	followerOriginal := follower.etlr
	vr := &voteRequest{
		term:      follower.term - 1,
		lastTerm:  follower.term - 1,
		lastIndex: 0,
		candidate: follower.id,
	}
	rg.unlockAll()

	// Now send a vote request that's obviously behind.
	require_NoError(t, nc.PublishMsg(&nats.Msg{
		Subject: vsubj,
		Reply:   vreply,
		Data:    vr.encode(),
	}))

	// Wait for everyone to respond.
	require_ChanRead(t, ch, time.Second)
	require_ChanRead(t, ch, time.Second)
	require_ChanRead(t, ch, time.Second)

	// Neither the leader nor our chosen follower should have updated their
	// election timer as a result of this.
	rg.lockAll()
	leaderEqual := leaderOriginal.Equal(leader.etlr)
	followerEqual := followerOriginal.Equal(follower.etlr)
	rg.unlockAll()
	require_True(t, leaderEqual)
	require_True(t, followerEqual)
}

func TestNRGUnsuccessfulVoteRequestCampaignEarly(t *testing.T) {
	n, cleanup := initSingleMemRaftNode(t)
	defer cleanup()

	nats0 := "S1Nunr6R" // "nats-0"
	n.etlr = time.Time{}

	// Simple case: we are follower and vote for a candidate.
	require_NoError(t, n.processVoteRequest(&voteRequest{term: 1, lastTerm: 0, lastIndex: 0, candidate: nats0}))
	require_Equal(t, n.term, 1)
	require_Equal(t, n.vote, nats0)
	require_NotEqual(t, n.etlr, time.Time{}) // Resets election timer as it voted.
	n.etlr = time.Time{}

	// We are follower and deny vote for outdated candidate.
	n.pterm, n.pindex = 1, 100
	require_NoError(t, n.processVoteRequest(&voteRequest{term: 2, lastTerm: 1, lastIndex: 2, candidate: nats0}))
	require_Equal(t, n.term, 2)
	require_Equal(t, n.vote, noVote)
	require_NotEqual(t, n.etlr, time.Time{}) // Resets election timer as it starts campaigning.
	n.etlr = time.Time{}

	// We are follower and deny a re-sent vote for outdated candidate. Don't postpone campaigning.
	require_NoError(t, n.processVoteRequest(&voteRequest{term: 2, lastTerm: 1, lastIndex: 2, candidate: nats0}))
	require_Equal(t, n.term, 2)
	require_Equal(t, n.vote, noVote)
	require_Equal(t, n.etlr, time.Time{})

	// Switch to candidate.
	n.pterm, n.pindex = 2, 200
	n.switchToCandidate()
	require_Equal(t, n.term, 3)
	require_Equal(t, n.State(), Candidate)
	require_NotEqual(t, n.etlr, time.Time{}) // Resets election timer as part of switching state.
	n.etlr = time.Time{}

	// We are candidate and deny vote for outdated candidate. But they were on a more recent term, restart campaign.
	require_NoError(t, n.processVoteRequest(&voteRequest{term: 4, lastTerm: 1, lastIndex: 2, candidate: nats0}))
	require_Equal(t, n.term, 4)
	require_Equal(t, n.vote, noVote)
	require_NotEqual(t, n.etlr, time.Time{}) // Resets election timer as it restarts campaigning.
	n.etlr = time.Time{}

	// Switch to candidate.
	n.pterm, n.pindex = 4, 400
	n.switchToCandidate()
	require_Equal(t, n.term, 5)
	require_Equal(t, n.State(), Candidate)
	require_NotEqual(t, n.etlr, time.Time{}) // Resets election timer as part of switching state.
	n.etlr = time.Time{}

	// We are candidate and deny vote for outdated candidate. Don't start campaigning early.
	require_NoError(t, n.processVoteRequest(&voteRequest{term: 5, lastTerm: 1, lastIndex: 2, candidate: nats0}))
	require_Equal(t, n.term, 5)
	require_Equal(t, n.vote, noVote)
	// Election timer must NOT be updated as that would mean another candidate that we don't vote
	// for can short-circuit us by making us restart elections, denying us the ability to become leader.
	require_Equal(t, n.etlr, time.Time{})
}

func TestNRGInvalidTAVDoesntPanic(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "R3S", 3)
	defer c.shutdown()
	c.waitOnLeader()

	rg := c.createRaftGroup("TEST", 3, newStateAdder)
	leader := rg.waitOnLeader()
	require_NotNil(t, leader)

	// Mangle the TAV file to a short length (less than uint64).
	tav := filepath.Join(leader.node().(*raft).sd, termVoteFile)
	require_NoError(t, os.WriteFile(tav, []byte{1, 2, 3, 4}, 0644))

	// Restart the node.
	leader.stop()
	leader.restart()

	// Before the fix, a crash would have happened before this point.
	c.waitOnAllCurrent()
}

func TestNRGAssumeHighTermAfterCandidateIsolation(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "R3S", 3)
	defer c.shutdown()
	c.waitOnLeader()

	nc, _ := jsClientConnect(t, c.leader(), nats.UserInfo("admin", "s3cr3t!"))
	defer nc.Close()

	rg := c.createRaftGroup("TEST", 3, newStateAdder)
	rg.waitOnLeader()

	// Bump the term up on one of the follower nodes by a considerable
	// amount and force it into the candidate state. This is what happens
	// after a period of time in isolation.
	follower := rg.nonLeader().node().(*raft)
	follower.Lock()
	follower.term += 100
	follower.switchState(Candidate)
	follower.Unlock()

	follower.requestVote()
	time.Sleep(time.Millisecond * 100)

	// The candidate will shortly send a vote request. When that happens,
	// the rest of the nodes in the cluster should move up to that term,
	// even though they will not grant the vote.
	nterm := follower.Term()
	for _, n := range rg {
		require_Equal(t, n.node().Term(), nterm)
	}

	// Have the leader send out a proposal, which will force the candidate
	// back into follower state.
	rg.waitOnLeader()
	rg.leader().(*stateAdder).proposeDelta(1)
	rg.waitOnTotal(t, 1)

	// The candidate should have switched to a follower on a term equal to
	// or newer than the candidate had.
	for _, n := range rg {
		require_NotEqual(t, n.node().State(), Candidate)
		require_True(t, n.node().Term() >= nterm)
	}
}

// Test to make sure this does not cause us to truncate our wal or enter catchup state.
func TestNRGHeartbeatOnLeaderChange(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "R3S", 3)
	defer c.shutdown()

	rg := c.createMemRaftGroup("TEST", 3, newStateAdder)
	rg.waitOnLeader()

	for i := 0; i < 10; i++ {
		// Restart the leader.
		leader := rg.leader().(*stateAdder)
		leader.proposeDelta(22)
		leader.proposeDelta(-11)
		leader.proposeDelta(-10)
		// Must observe forward progress, so each iteration will check +1 total.
		rg.waitOnTotal(t, int64(i+1))
		leader.stop()
		leader.restart()
		rg.waitOnLeader()
	}
}

func TestNRGElectionTimerAfterObserver(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "R3S", 3)
	defer c.shutdown()

	rg := c.createMemRaftGroup("TEST", 3, newStateAdder)
	rg.waitOnLeader()

	for _, n := range rg {
		n.node().SetObserver(true)
	}

	time.Sleep(maxElectionTimeout)
	before := time.Now()

	for _, n := range rg {
		n.node().SetObserver(false)
	}

	time.Sleep(maxCampaignTimeout)

	for _, n := range rg {
		rn := n.node().(*raft)
		rn.RLock()
		etlr := rn.etlr
		rn.RUnlock()
		require_True(t, etlr.After(before))
	}
}

func TestNRGSystemClientCleanupFromAccount(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "R3S", 3)
	defer c.shutdown()

	s := c.randomServer()
	sacc := s.SystemAccount()

	numClients := func() int {
		sacc.mu.RLock()
		defer sacc.mu.RUnlock()
		return len(sacc.clients)
	}

	start := numClients()

	var all []smGroup
	for i := 0; i < 5; i++ {
		rgName := fmt.Sprintf("TEST-%d", i)
		rg := c.createRaftGroup(rgName, 3, newStateAdder)
		all = append(all, rg)
		rg.waitOnLeader()
	}
	for _, rg := range all {
		for _, sm := range rg {
			sm.node().Stop()
		}
		for _, sm := range rg {
			sm.node().WaitForStop()
		}
	}
	finish := numClients()
	require_Equal(t, start, finish)
}

func TestNRGCandidateDoesntRevertTermAfterOldAE(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "R3S", 3)
	defer c.shutdown()

	rg := c.createMemRaftGroup("TEST", 3, newStateAdder)
	rg.waitOnLeader()

	// Bump the term up a few times.
	for i := 0; i < 3; i++ {
		rg.leader().node().StepDown()
		time.Sleep(time.Millisecond * 50) // Needed because stepdowns not synchronous
		rg.waitOnLeader()
	}

	leader := rg.leader().node().(*raft)
	follower := rg.nonLeader().node().(*raft)

	// Sanity check that we are where we expect to be.
	require_Equal(t, leader.term, 4)
	require_Equal(t, follower.term, 4)

	// At this point the active term is 4 and pterm is 4, force the
	// term up to 9. This won't bump the pterm.
	rg.lockAll()
	for _, n := range rg {
		n.node().(*raft).term += 5
	}
	rg.unlockAll()

	// Build an AE that has a term newer than the pterm but older than
	// the term. Give it to the follower in candidate state.
	ae := newAppendEntry(leader.id, 6, leader.commit, leader.pterm, leader.pindex, nil)
	follower.switchToCandidate()
	follower.processAppendEntry(ae, nil)

	// The candidate must not have reverted back to term 6.
	require_NotEqual(t, follower.term, 6)
}

func TestNRGTermDoesntRollBackToPtermOnCatchup(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "R3S", 3)
	defer c.shutdown()

	nc, _ := jsClientConnect(t, c.leader(), nats.UserInfo("admin", "s3cr3t!"))
	defer nc.Close()

	rg := c.createMemRaftGroup("TEST", 3, newStateAdder)
	rg.waitOnLeader()

	// Propose some entries so that we have entries in the log that have pterm 1.
	lsm := rg.leader().(*stateAdder)
	for i := 0; i < 5; i++ {
		lsm.proposeDelta(1)
		rg.waitOnTotal(t, int64(i)+1)
	}

	// Check that everyone is where they are supposed to be.
	rg.lockAll()
	for _, n := range rg {
		rn := n.node().(*raft)
		require_Equal(t, rn.term, 1)
		require_Equal(t, rn.pterm, 1)
		require_Equal(t, rn.pindex, 6)
	}
	rg.unlockAll()

	// Force a stepdown so that we move up to term 2.
	rg.leader().node().(*raft).switchToFollower(noLeader)
	rg.waitOnLeader()
	leader := rg.leader().node().(*raft)

	// Now make sure everyone has moved up to term 2. Additionally we're
	// going to prune back the follower logs to term 1 as this is what will
	// create the right conditions for the catchup.
	rg.lockAll()
	for _, n := range rg {
		rn := n.node().(*raft)
		require_Equal(t, rn.term, 2)

		if !rn.Leader() {
			rn.truncateWAL(1, 6)
			require_Equal(t, rn.term, 2) // rn.term must stay the same
			require_Equal(t, rn.pterm, 1)
			require_Equal(t, rn.pindex, 6)
		}
	}
	// This will make followers run a catchup.
	ae := newAppendEntry(leader.id, leader.term, leader.commit, leader.pterm, leader.pindex, nil)
	rg.unlockAll()

	arInbox := nc.NewRespInbox()
	arCh := make(chan *nats.Msg, 2)
	_, err := nc.ChanSubscribe(arInbox, arCh)
	require_NoError(t, err)

	// Ensure the subscription is known by the server we're connected to,
	// but also by the other servers in the cluster.
	require_NoError(t, nc.Flush())
	time.Sleep(100 * time.Millisecond)

	// In order to trip this condition, we need to send an append entry that
	// will trick the followers into running a catchup. In the process they
	// were setting the term back to pterm which is incorrect.
	b, err := ae.encode(nil)
	require_NoError(t, err)
	require_NoError(t, nc.PublishMsg(&nats.Msg{
		Subject: fmt.Sprintf(raftAppendSubj, "TEST"),
		Reply:   arInbox,
		Data:    b,
	}))

	// Wait for both followers to respond to the append entry and then verify
	// that none of the nodes should have reverted back to term 1.
	require_ChanRead(t, arCh, time.Second*5) // First follower
	require_ChanRead(t, arCh, time.Second*5) // Second follower
	for _, n := range rg {
		require_NotEqual(t, n.node().Term(), 1)
	}
}

func TestNRGNoResetOnAppendEntryResponse(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "R3S", 3)
	defer c.shutdown()

	nc, _ := jsClientConnect(t, c.leader(), nats.UserInfo("admin", "s3cr3t!"))
	defer nc.Close()

	rg := c.createRaftGroup("TEST", 3, newStateAdder)
	rg.waitOnLeader()
	c.waitOnAllCurrent()

	leader := rg.leader().node().(*raft)
	follower := rg.nonLeader().node().(*raft)
	lsm := rg.leader().(*stateAdder)
	isLeader := require_ChanRead(t, lsm.lch, 5*time.Second)
	require_True(t, isLeader)

	// Subscribe for append entries that aren't heartbeats and respond to
	// each of them as though it's a non-success and with a higher term.
	// The higher term in this case is what would cause the leader previously
	// to reset the entire log which it shouldn't do.
	_, err := nc.Subscribe(fmt.Sprintf(raftAppendSubj, "TEST"), func(msg *nats.Msg) {
		if ae, err := decodeAppendEntry(msg.Data, nil, msg.Reply); err == nil && len(ae.entries) > 0 {
			ar := newAppendEntryResponse(ae.term+1, ae.commit, follower.id, false)
			require_NoError(t, msg.Respond(ar.encode(nil)))
		}
	})
	require_NoError(t, err)

	c.waitOnAllCurrent()

	// Temporarily prevent followers to respond
	locked := rg.lockFollowers()

	// Generate an append entry that the subscriber above can respond to.
	lsm.proposeDelta(5)

	// Expect the leader to step down
	checkFor(t, 10*time.Second, 0, func() error {
		isLeader = require_ChanRead(t, lsm.lch, 5*time.Second)
		if isLeader {
			return fmt.Errorf("Node is still leader!")
		}
		return nil
	})

	// Unlock the followers
	for _, l := range locked {
		l.node().(*raft).Unlock()
	}

	// The was-leader should now have stepped down, make sure that it
	// didn't blow away its log in the process.
	rg.lockAll()
	defer rg.unlockAll()
	require_Equal(t, leader.State(), Follower)
	require_NotEqual(t, leader.pterm, 0)
	require_NotEqual(t, leader.pindex, 0)
}

func TestNRGCandidateDontStepdownDueToLeaderOfPreviousTerm(t *testing.T) {
	// This test relies on nodes not hitting their election timer too often.
	origMinTimeout, origMaxTimeout, origHBInterval := minElectionTimeout, maxElectionTimeout, hbInterval
	minElectionTimeout, maxElectionTimeout, hbInterval = time.Second*5, time.Second*10, time.Second
	defer func() {
		minElectionTimeout, maxElectionTimeout, hbInterval = origMinTimeout, origMaxTimeout, origHBInterval
	}()

	c := createJetStreamClusterExplicit(t, "R3S", 3)
	defer c.shutdown()
	c.waitOnLeader()

	nc, _ := jsClientConnect(t, c.leader(), nats.UserInfo("admin", "s3cr3t!"))
	defer nc.Close()

	rg := c.createRaftGroup("TEST", 3, newStateAdder)
	rg.waitOnLeader()

	var (
		candidatePterm  uint64 = 50
		candidatePindex uint64 = 70
		candidateTerm   uint64 = 100
	)

	// Create a candidate that has received entries while they were a follower in a previous term
	candidate := rg.nonLeader().node().(*raft)
	candidate.Lock()
	candidate.switchState(Candidate)
	candidate.pterm = candidatePterm
	candidate.pindex = candidatePindex
	candidate.term = candidateTerm
	candidate.Unlock()

	// Leader term is behind candidate
	leader := rg.leader().node().(*raft)
	leader.Lock()
	leader.term = candidatePterm
	leader.pterm = candidatePterm
	leader.pindex = candidatePindex
	leader.Unlock()

	// Subscribe to the append entry subject.
	sub, err := nc.SubscribeSync(leader.asubj)
	require_NoError(t, err)

	// Get the first append entry that we receive, should be heartbeat from leader of prev term
	msg, err := sub.NextMsg(5 * time.Second)
	require_NoError(t, err)

	// Stop nodes from progressing so we can check state
	rg.lockAll()
	defer rg.unlockAll()

	// Decode the append entry
	ae, err := decodeAppendEntry(msg.Data, nil, msg.Reply)
	require_NoError(t, err)

	// Check that the append entry is from the leader
	require_Equal(t, ae.leader, leader.id)
	// Check that it came from the leader before it updated its term with the response from the candidate
	require_Equal(t, ae.term, candidatePterm)

	// Check that the candidate hasn't stepped down
	require_Equal(t, candidate.State(), Candidate)
	// Check that the candidate's term is still ahead of the leader's term
	require_True(t, candidate.term > ae.term)
}

func TestNRGRemoveLeaderPeerDeadlockBug(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "R3S", 3)
	defer c.shutdown()

	rg := c.createMemRaftGroup("TEST", 3, newStateAdder)
	rg.waitOnLeader()

	n := rg.leader().node().(*raft)
	leader := n.ID()

	// Propose to remove the leader as a peer. Will lead to a deadlock with bug.
	require_NoError(t, n.ProposeRemovePeer(leader))
	rg.waitOnLeader()

	checkFor(t, 10*time.Second, 200*time.Millisecond, func() error {
		nl := n.GroupLeader()
		if nl != leader {
			return nil
		}
		return errors.New("Leader has not moved")
	})
}

func TestNRGWALEntryWithoutQuorumMustTruncate(t *testing.T) {
	tests := []struct {
		title  string
		modify func(rg smGroup)
	}{
		{
			// state equals, only need to remove the entry
			title:  "equal",
			modify: func(rg smGroup) {},
		},
		{
			// state diverged, need to replace the entry
			title: "diverged",
			modify: func(rg smGroup) {
				rg.leader().(*stateAdder).proposeDelta(11)
			},
		},
	}

	for _, test := range tests {
		t.Run(test.title, func(t *testing.T) {
			c := createJetStreamClusterExplicit(t, "R3S", 3)
			defer c.shutdown()

			rg := c.createRaftGroup("TEST", 3, newStateAdder)
			rg.waitOnLeader()

			var err error
			var scratch [1024]byte

			// Simulate leader storing an AppendEntry in WAL but being hard killed before it can propose to its peers.
			n := rg.leader().node().(*raft)
			esm := encodeStreamMsgAllowCompress("foo", "_INBOX.foo", nil, nil, 0, 0, true)
			entries := []*Entry{newEntry(EntryNormal, esm)}
			n.Lock()
			ae := n.buildAppendEntry(entries)
			ae.buf, err = ae.encode(scratch[:])
			require_NoError(t, err)
			err = n.storeToWAL(ae)
			n.Unlock()
			require_NoError(t, err)

			// Stop the leader so it moves to another one.
			n.shutdown()

			// Wait for another leader to be picked
			rg.waitOnLeader()

			// Make a modification, specific to this test.
			test.modify(rg)

			// Restart the previous leader that contains the stored AppendEntry without quorum.
			for _, a := range rg {
				if a.node().ID() == n.ID() {
					sa := a.(*stateAdder)
					sa.restart()
					break
				}
			}

			// The previous leader's WAL should truncate to remove the AppendEntry only it has.
			// Eventually all WALs for all peers must match.
			checkFor(t, 5*time.Second, 200*time.Millisecond, func() error {
				var expected []*appendEntry
				for _, a := range rg {
					an := a.node().(*raft)
					var state StreamState
					an.wal.FastState(&state)
					if len(expected) > 0 && int(state.LastSeq-state.FirstSeq+1) != len(expected) {
						return fmt.Errorf("WAL is different: too many entries")
					}
					// Loop over all entries in the WAL, checking if the contents for all RAFT nodes are equal.
					for index := state.FirstSeq; index <= state.LastSeq; index++ {
						ae, err := an.loadEntry(index)
						if err != nil {
							return err
						}
						ae.buf = nil // ... as we'll deeply check everything else in the AE.
						ae.lterm = 0 // ... as lterm can differ if one node caught up another.
						seq := int(index)
						if len(expected) < seq {
							expected = append(expected, ae)
						} else if !reflect.DeepEqual(expected[seq-1], ae) {
							return fmt.Errorf("WAL is different: stored AEs differ")
						}
					}
				}
				return nil
			})
		})
	}
}

func TestNRGTermNoDecreaseAfterWALReset(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "R3S", 3)
	defer c.shutdown()

	rg := c.createRaftGroup("TEST", 3, newStateAdder)
	rg.waitOnLeader()

	l := rg.leader().node().(*raft)
	l.Lock()
	l.term = 20
	l.Unlock()

	esm := encodeStreamMsgAllowCompress("foo", "_INBOX.foo", nil, nil, 0, 0, true)
	entries := []*Entry{newEntry(EntryNormal, esm)}
	l.Lock()
	ae := l.buildAppendEntry(entries)
	l.Unlock()

	for _, f := range rg {
		if f.node().ID() != l.ID() {
			fn := f.node().(*raft)
			fn.processAppendEntry(ae, fn.aesub)
			require_Equal(t, fn.term, 20) // Follower's term gets upped as expected.
		}
	}

	// Lower the term, simulating the followers receiving a message from an old term/leader.
	ae.term = 3
	for _, f := range rg {
		if f.node().ID() != l.ID() {
			fn := f.node().(*raft)
			fn.processAppendEntry(ae, fn.aesub)
			require_Equal(t, fn.term, 20) // Follower should reject and the term stays the same.

			fn.Lock()
			fn.resetWAL()
			fn.Unlock()
			fn.processAppendEntry(ae, fn.aesub)
			require_Equal(t, fn.term, 20) // Follower should reject again, even after reset, term stays the same.
		}
	}
}

func TestNRGPendingAppendEntryCacheInvalidation(t *testing.T) {
	for _, test := range []struct {
		title   string
		entries int
	}{
		{title: "empty", entries: 1},
		{title: "at limit", entries: paeDropThreshold},
		{title: "full", entries: paeDropThreshold + 1},
	} {
		t.Run(test.title, func(t *testing.T) {
			c := createJetStreamClusterExplicit(t, "R3S", 3)
			defer c.shutdown()

			rg := c.createMemRaftGroup("TEST", 3, newStateAdder)
			rg.waitOnLeader()
			l := rg.leader()

			l.(*stateAdder).proposeDelta(1)
			rg.waitOnTotal(t, 1)

			// Fill up the cache with N entries.
			// The contents don't matter as they should never be applied.
			rg.lockAll()
			for _, s := range rg {
				n := s.node().(*raft)
				for i := 0; i < test.entries; i++ {
					n.pae[n.pindex+uint64(1+i)] = newAppendEntry("", 0, 0, 0, 0, nil)
				}
			}
			rg.unlockAll()

			l.(*stateAdder).proposeDelta(1)
			rg.waitOnTotal(t, 2)
		})
	}
}

func TestNRGTruncateWALClearsPendingAppendEntryCache(t *testing.T) {
	n, cleanup := initSingleMemRaftNode(t)
	defer cleanup()

	esm := encodeStreamMsgAllowCompress("foo", "_INBOX.foo", nil, nil, 0, 0, true)
	entries := []*Entry{newEntry(EntryNormal, esm)}

	nats0 := "S1Nunr6R" // "nats-0"

	// Store three entries through the normal path so both the WAL and n.pae
	// are populated by cachePendingEntry.
	ae1 := encode(t, &appendEntry{leader: nats0, term: 1, commit: 0, pterm: 0, pindex: 0, entries: entries})
	ae2 := encode(t, &appendEntry{leader: nats0, term: 1, commit: 0, pterm: 1, pindex: 1, entries: entries})
	ae3 := encode(t, &appendEntry{leader: nats0, term: 1, commit: 0, pterm: 1, pindex: 2, entries: entries})

	n.processAppendEntry(ae1, n.aesub)
	n.processAppendEntry(ae2, n.aesub)
	n.processAppendEntry(ae3, n.aesub)

	n.Lock()
	defer n.Unlock()

	// Sanity check: WAL and cache both have all three entries.
	require_Equal(t, n.pindex, 3)
	require_Equal(t, n.wal.State().Msgs, 3)
	require_NotNil(t, n.pae[1])
	require_NotNil(t, n.pae[2])
	require_NotNil(t, n.pae[3])

	// Truncate WAL back to index 1, dropping entries 2 and 3 from the log.
	n.truncateWAL(1, 1)
	require_Equal(t, n.pindex, 1)
	require_Equal(t, n.wal.State().Msgs, 1)

	// The cache must not retain entries for indices the WAL no longer holds,
	// otherwise applyCommit could later apply stale entries straight from
	// the cache (bypassing the WAL load path).
	require_Len(t, len(n.pae), 1)
	require_NotNil(t, n.pae[1])
}

func TestNRGTruncateWALRevertsUncommittedAddPeer(t *testing.T) {
	const (
		KindLeader = iota
		KindFollower
		KindRestart
	)
	test := func(kind int) {
		n, cleanup := initSingleMemRaftNode(t)
		defer cleanup()

		nats0 := "S1Nunr6R"   // "nats-0"
		newPeer := "yrzKKRBu" // "nats-1"
		n.addPeer(nats0)
		require_Len(t, len(n.peers), 2)

		esm := encodeStreamMsgAllowCompress("foo", "_INBOX.foo", nil, nil, 0, 0, true)
		normal := []*Entry{newEntry(EntryNormal, esm)}
		addPeer := newEntry(EntryAddPeer, []byte(newPeer))

		if kind == KindLeader {
			n.switchToLeader()
			n.sendMembershipChange(addPeer)
		} else {
			// Store a normal entry, then an uncommitted AddPeer entry.
			ae1 := encode(t, &appendEntry{leader: nats0, term: 1, commit: 0, pterm: 0, pindex: 0, entries: normal})
			ae2 := encode(t, &appendEntry{leader: nats0, term: 1, commit: 0, pterm: 1, pindex: 1, entries: []*Entry{addPeer}})
			aesub := n.aesub
			if kind == KindRestart {
				aesub = nil
			}
			n.processAppendEntry(ae1, aesub)
			n.processAppendEntry(ae2, aesub)
		}

		// The peer is tracked speculatively, and a membership change is now inflight.
		require_Equal(t, n.pindex, 2)
		_, ok := n.peers[newPeer]
		require_True(t, ok)
		require_Equal(t, n.csz, 3)
		require_Equal(t, n.qn, 2)
		require_NotNil(t, n.membChange)
		require_Equal(t, n.membChange.index, 2)
		require_Equal(t, n.membChange.peer, newPeer)
		require_True(t, n.membChange.prev == nil)

		// Raftz should report the new peer as not known/committed yet.
		rz := n.s.Raftz(&RaftzOptions{AccountFilter: globalAccountName})
		require_NotNil(t, rz)
		gz := (*rz)[globalAccountName]["TEST"]
		require_NotNil(t, gz)
		require_Len(t, len(gz.Peers), 2)
		require_True(t, gz.Peers[nats0].Known)
		require_False(t, gz.Peers[newPeer].Known)

		// Truncate the uncommitted AddPeer entry. Its speculative effects must be reverted.
		n.Lock()
		n.truncateWAL(1, 1)
		n.Unlock()
		require_Equal(t, n.pindex, 1)
		_, ok = n.peers[newPeer]
		require_False(t, ok)
		require_Equal(t, n.csz, 2)
		require_Equal(t, n.qn, 2)
		require_True(t, n.membChange == nil)

		// Raftz should also be reverted.
		rz = n.s.Raftz(&RaftzOptions{AccountFilter: globalAccountName})
		require_NotNil(t, rz)
		gz = (*rz)[globalAccountName]["TEST"]
		require_NotNil(t, gz)
		require_Len(t, len(gz.Peers), 1)
		require_True(t, gz.Peers[nats0].Known)
	}

	t.Run("Leader", func(t *testing.T) { test(KindLeader) })
	t.Run("Follower", func(t *testing.T) { test(KindFollower) })
	t.Run("Restart", func(t *testing.T) { test(KindRestart) })
}

func TestNRGTruncateWALRevertsUncommittedRemovePeer(t *testing.T) {
	const (
		KindLeader = iota
		KindFollower
		KindRestart
	)
	test := func(kind int) {
		n, cleanup := initSingleMemRaftNode(t)
		defer cleanup()

		nats0 := "S1Nunr6R"   // "nats-0"
		oldPeer := "yrzKKRBu" // "nats-1"
		n.addPeer(nats0)
		n.addPeer(oldPeer)
		require_Len(t, len(n.peers), 3)

		esm := encodeStreamMsgAllowCompress("foo", "_INBOX.foo", nil, nil, 0, 0, true)
		normal := []*Entry{newEntry(EntryNormal, esm)}
		removePeer := newEntry(EntryRemovePeer, []byte(oldPeer))

		if kind == KindLeader {
			n.switchToLeader()
			n.sendMembershipChange(removePeer)
		} else {
			// Store a normal entry, then an uncommitted AddPeer entry.
			ae1 := encode(t, &appendEntry{leader: nats0, term: 1, commit: 0, pterm: 0, pindex: 0, entries: normal})
			ae2 := encode(t, &appendEntry{leader: nats0, term: 1, commit: 0, pterm: 1, pindex: 1, entries: []*Entry{removePeer}})
			aesub := n.aesub
			if kind == KindRestart {
				aesub = nil
			}
			n.processAppendEntry(ae1, aesub)
			n.processAppendEntry(ae2, aesub)
		}

		// The peer is gone and quorum shrank.
		_, ok := n.peers[oldPeer]
		require_False(t, ok)
		require_Equal(t, n.csz, 2)
		require_Equal(t, n.qn, 2)
		_, ok = n.removed[oldPeer]
		require_False(t, ok)
		require_NotNil(t, n.membChange)
		require_Equal(t, n.membChange.index, 2)
		require_Equal(t, n.membChange.peer, oldPeer)
		require_NotNil(t, n.membChange.prev)

		// Raftz shouldn't report the to-be-removed peer anymore.
		rz := n.s.Raftz(&RaftzOptions{AccountFilter: globalAccountName})
		require_NotNil(t, rz)
		gz := (*rz)[globalAccountName]["TEST"]
		require_NotNil(t, gz)
		require_Len(t, len(gz.Peers), 1)
		require_True(t, gz.Peers[nats0].Known)

		// Truncate the uncommitted RemovePeer entry. Its speculative effects must be reverted.
		n.Lock()
		n.truncateWAL(1, 1)
		n.Unlock()
		_, ok = n.peers[oldPeer]
		require_True(t, ok)
		require_Equal(t, n.csz, 3)
		require_Equal(t, n.qn, 2)
		_, ok = n.removed[oldPeer]
		require_False(t, ok)
		require_True(t, n.membChange == nil)

		// Raftz should also be reverted.
		rz = n.s.Raftz(&RaftzOptions{AccountFilter: globalAccountName})
		require_NotNil(t, rz)
		gz = (*rz)[globalAccountName]["TEST"]
		require_NotNil(t, gz)
		require_Len(t, len(gz.Peers), 2)
		require_True(t, gz.Peers[nats0].Known)
		require_True(t, gz.Peers[oldPeer].Known)
	}

	t.Run("Leader", func(t *testing.T) { test(KindLeader) })
	t.Run("Follower", func(t *testing.T) { test(KindFollower) })
	t.Run("Restart", func(t *testing.T) { test(KindRestart) })
}

func TestNRGResetWALClearsPendingAppendEntryCache(t *testing.T) {
	n, cleanup := initSingleMemRaftNode(t)
	defer cleanup()

	esm := encodeStreamMsgAllowCompress("foo", "_INBOX.foo", nil, nil, 0, 0, true)
	entries := []*Entry{newEntry(EntryNormal, esm)}

	nats0 := "S1Nunr6R" // "nats-0"

	ae1 := encode(t, &appendEntry{leader: nats0, term: 1, commit: 0, pterm: 0, pindex: 0, entries: entries})
	ae2 := encode(t, &appendEntry{leader: nats0, term: 1, commit: 0, pterm: 1, pindex: 1, entries: entries})
	ae3 := encode(t, &appendEntry{leader: nats0, term: 1, commit: 0, pterm: 1, pindex: 2, entries: entries})

	n.processAppendEntry(ae1, n.aesub)
	n.processAppendEntry(ae2, n.aesub)
	n.processAppendEntry(ae3, n.aesub)

	n.Lock()
	defer n.Unlock()

	require_Equal(t, n.pindex, 3)
	require_Equal(t, len(n.pae), 3)

	n.resetWAL()
	require_Equal(t, n.pindex, 0)
	require_Equal(t, n.wal.State().Msgs, 0)

	// All cached pending append entries should be gone after a full reset.
	require_Equal(t, len(n.pae), 0)
}

func TestNRGInstallSnapshotClearsPendingAppendEntryCache(t *testing.T) {
	n, cleanup := initSingleMemRaftNode(t)
	defer cleanup()

	esm := encodeStreamMsgAllowCompress("foo", "_INBOX.foo", nil, nil, 0, 0, true)
	entries := []*Entry{newEntry(EntryNormal, esm)}

	nats0 := "S1Nunr6R" // "nats-0"

	// Three appendEntries arrive with commit lagging at 0, so applyCommit
	// never runs and all three remain cached in n.pae.
	ae1 := encode(t, &appendEntry{leader: nats0, term: 1, commit: 0, pterm: 0, pindex: 0, entries: entries})
	ae2 := encode(t, &appendEntry{leader: nats0, term: 1, commit: 0, pterm: 1, pindex: 1, entries: entries})
	ae3 := encode(t, &appendEntry{leader: nats0, term: 1, commit: 0, pterm: 1, pindex: 2, entries: entries})

	n.processAppendEntry(ae1, n.aesub)
	n.processAppendEntry(ae2, n.aesub)
	n.processAppendEntry(ae3, n.aesub)

	n.Lock()
	defer n.Unlock()

	require_Equal(t, n.pindex, 3)
	require_Equal(t, n.commit, 0)
	require_Equal(t, len(n.pae), 3)

	// Install a snapshot covering the entire log. The WAL gets compacted
	// past index 3, but cached entries 1-3 would otherwise stay in n.pae
	// since applyCommit never ran for them and cachePendingEntry only
	// overwrites at the current n.pindex (which won't revisit 1-3).
	snap := &snapshot{
		lastTerm:  1,
		lastIndex: 3,
		peerstate: encodePeerState(&peerState{n.peerNames(), n.csz, n.extSt}),
	}
	require_NoError(t, n.installSnapshot(snap))

	require_Equal(t, n.wal.State().Msgs, 0)
	require_Equal(t, len(n.pae), 0)
}

func TestNRGCatchupFromSnapshotClearsPendingAppendEntryCache(t *testing.T) {
	n, cleanup := initSingleMemRaftNode(t)
	defer cleanup()

	esm := encodeStreamMsgAllowCompress("foo", "_INBOX.foo", nil, nil, 0, 0, true)
	entries := []*Entry{newEntry(EntryNormal, esm)}
	snapshotEntries := []*Entry{
		newEntry(EntrySnapshot, nil),
		newEntry(EntryPeerState, encodePeerState(&peerState{n.peerNames(), n.csz, n.extSt})),
	}

	nats0 := "S1Nunr6R" // "nats-0"

	// Receive two appendEntries normally; commit lags so both stay cached.
	ae1 := encode(t, &appendEntry{leader: nats0, term: 1, commit: 0, pterm: 0, pindex: 0, entries: entries})
	ae2 := encode(t, &appendEntry{leader: nats0, term: 1, commit: 0, pterm: 1, pindex: 1, entries: entries})
	n.processAppendEntry(ae1, n.aesub)
	n.processAppendEntry(ae2, n.aesub)
	require_Equal(t, n.pindex, 2)
	require_Equal(t, len(n.pae), 2)

	// Leader is way ahead; this triggers catchup.
	aeTriggerCatchup := encode(t, &appendEntry{leader: nats0, term: 1, commit: 100, pterm: 1, pindex: 100, entries: nil})
	n.processAppendEntry(aeTriggerCatchup, n.aesub)
	require_True(t, n.catchup != nil)

	// Leader sends a snapshot at the catchup point. n.pindex/n.commit jump
	// to 100 without applyCommit running for indices 1-2, so the cached
	// entries from before catchup would otherwise leak.
	aeCatchupSnapshot := encode(t, &appendEntry{leader: nats0, term: 1, commit: 100, pterm: 1, pindex: 100, entries: snapshotEntries})
	n.processAppendEntry(aeCatchupSnapshot, n.catchup.sub)

	n.Lock()
	defer n.Unlock()
	require_Equal(t, n.pindex, 100)
	require_Equal(t, n.commit, 100)
	require_Equal(t, len(n.pae), 0)
}

func TestNRGCatchupDoesNotTruncateUncommittedEntriesWithQuorum(t *testing.T) {
	n, cleanup := initSingleMemRaftNode(t)
	defer cleanup()

	// Create a sample entry, the content doesn't matter, just that it's stored.
	esm := encodeStreamMsgAllowCompress("foo", "_INBOX.foo", nil, nil, 0, 0, true)
	entries := []*Entry{newEntry(EntryNormal, esm)}

	nats0 := "S1Nunr6R" // "nats-0"
	nats1 := "yrzKKRBu" // "nats-1"

	// Timeline, for first leader
	aeInitial := encode(t, &appendEntry{leader: nats0, term: 1, commit: 0, pterm: 0, pindex: 0, entries: entries})
	aeHeartbeat := encode(t, &appendEntry{leader: nats0, term: 1, commit: 1, pterm: 1, pindex: 1, entries: nil})
	aeUncommitted := encode(t, &appendEntry{leader: nats0, term: 1, commit: 1, pterm: 1, pindex: 1, entries: entries})
	aeNoQuorum := encode(t, &appendEntry{leader: nats0, term: 1, commit: 1, pterm: 1, pindex: 2, entries: entries})

	// Timeline, after leader change
	aeMissed := encode(t, &appendEntry{leader: nats1, term: 2, commit: 1, pterm: 1, pindex: 2, entries: entries})
	aeCatchupTrigger := encode(t, &appendEntry{leader: nats1, term: 2, commit: 1, pterm: 2, pindex: 3, entries: entries})
	aeHeartbeat2 := encode(t, &appendEntry{leader: nats1, term: 2, commit: 2, pterm: 2, pindex: 4, entries: nil})
	aeHeartbeat3 := encode(t, &appendEntry{leader: nats1, term: 2, commit: 4, pterm: 2, pindex: 4, entries: nil})

	// Initial case is simple, just store the entry.
	n.processAppendEntry(aeInitial, n.aesub)
	require_Equal(t, n.wal.State().Msgs, 1)
	entry, err := n.loadEntry(1)
	require_NoError(t, err)
	require_Equal(t, entry.leader, nats0)

	// Heartbeat, makes sure commit moves up.
	n.processAppendEntry(aeHeartbeat, n.aesub)
	require_Equal(t, n.commit, 1)

	// We get one entry that has quorum (but we don't know that yet), so it stays uncommitted for a bit.
	n.processAppendEntry(aeUncommitted, n.aesub)
	require_Equal(t, n.wal.State().Msgs, 2)
	entry, err = n.loadEntry(2)
	require_NoError(t, err)
	require_Equal(t, entry.leader, nats0)

	// We get one entry that has NO quorum (but we don't know that yet).
	n.processAppendEntry(aeNoQuorum, n.aesub)
	require_Equal(t, n.wal.State().Msgs, 3)
	entry, err = n.loadEntry(3)
	require_NoError(t, err)
	require_Equal(t, entry.leader, nats0)

	// We've just had a leader election, and we missed one message from the previous leader.
	// We should truncate the last message.
	n.processAppendEntry(aeCatchupTrigger, n.aesub)
	require_Equal(t, n.wal.State().Msgs, 2)
	require_True(t, n.catchup == nil)

	// We get a heartbeat that prompts us to catchup.
	n.processAppendEntry(aeHeartbeat2, n.aesub)
	require_Equal(t, n.wal.State().Msgs, 2)
	require_Equal(t, n.commit, 1) // Commit should not change, as we missed an item.
	require_True(t, n.catchup != nil)
	require_Equal(t, n.catchup.pterm, 1)  // n.pterm
	require_Equal(t, n.catchup.pindex, 2) // n.pindex

	// We now notice the leader indicated a different entry at the (no quorum) index, should save that.
	n.processAppendEntry(aeMissed, n.catchup.sub)
	require_Equal(t, n.wal.State().Msgs, 3)
	require_True(t, n.catchup != nil)

	// We now get the entry that initially triggered us to catchup, it should be added.
	n.processAppendEntry(aeCatchupTrigger, n.catchup.sub)
	require_Equal(t, n.wal.State().Msgs, 4)
	require_True(t, n.catchup != nil)
	entry, err = n.loadEntry(4)
	require_NoError(t, err)
	require_Equal(t, entry.leader, nats1)

	// Heartbeat, makes sure we commit (and reset catchup, as we're now up-to-date).
	n.processAppendEntry(aeHeartbeat3, n.aesub)
	require_Equal(t, n.commit, 4)
	require_True(t, n.catchup == nil)
}

func TestNRGCatchupCanTruncateMultipleEntriesWithoutQuorum(t *testing.T) {
	n, cleanup := initSingleMemRaftNode(t)
	defer cleanup()

	// Create a sample entry, the content doesn't matter, just that it's stored.
	esm := encodeStreamMsgAllowCompress("foo", "_INBOX.foo", nil, nil, 0, 0, true)
	entries := []*Entry{newEntry(EntryNormal, esm)}

	nats0 := "S1Nunr6R" // "nats-0"
	nats1 := "yrzKKRBu" // "nats-1"

	// Timeline, for first leader
	aeInitial := encode(t, &appendEntry{leader: nats0, term: 1, commit: 0, pterm: 0, pindex: 0, entries: entries})
	aeHeartbeat := encode(t, &appendEntry{leader: nats0, term: 1, commit: 1, pterm: 1, pindex: 1, entries: nil})
	aeNoQuorum1 := encode(t, &appendEntry{leader: nats0, term: 1, commit: 1, pterm: 1, pindex: 1, entries: entries})
	aeNoQuorum2 := encode(t, &appendEntry{leader: nats0, term: 1, commit: 1, pterm: 1, pindex: 2, entries: entries})

	// Timeline, after leader change
	aeMissed1 := encode(t, &appendEntry{leader: nats1, term: 2, commit: 1, pterm: 1, pindex: 1, entries: entries})
	aeMissed2 := encode(t, &appendEntry{leader: nats1, term: 2, commit: 1, pterm: 2, pindex: 2, entries: entries})
	aeCatchupTrigger := encode(t, &appendEntry{leader: nats1, term: 2, commit: 1, pterm: 2, pindex: 3, entries: entries})
	aeHeartbeat2 := encode(t, &appendEntry{leader: nats1, term: 2, commit: 2, pterm: 2, pindex: 4, entries: nil})
	aeHeartbeat3 := encode(t, &appendEntry{leader: nats1, term: 2, commit: 4, pterm: 2, pindex: 4, entries: nil})

	// Initial case is simple, just store the entry.
	n.processAppendEntry(aeInitial, n.aesub)
	require_Equal(t, n.wal.State().Msgs, 1)
	entry, err := n.loadEntry(1)
	require_NoError(t, err)
	require_Equal(t, entry.leader, nats0)

	// Heartbeat, makes sure commit moves up.
	n.processAppendEntry(aeHeartbeat, n.aesub)
	require_Equal(t, n.commit, 1)

	// We get one entry that has NO quorum (but we don't know that yet).
	n.processAppendEntry(aeNoQuorum1, n.aesub)
	require_Equal(t, n.wal.State().Msgs, 2)
	entry, err = n.loadEntry(2)
	require_NoError(t, err)
	require_Equal(t, entry.leader, nats0)

	// We get another entry that has NO quorum (but we don't know that yet).
	n.processAppendEntry(aeNoQuorum2, n.aesub)
	require_Equal(t, n.wal.State().Msgs, 3)
	entry, err = n.loadEntry(3)
	require_NoError(t, err)
	require_Equal(t, entry.leader, nats0)

	// We've just had a leader election, and we missed messages from the previous leader.
	// We should truncate the last message.
	n.processAppendEntry(aeCatchupTrigger, n.aesub)
	require_Equal(t, n.wal.State().Msgs, 2)
	require_True(t, n.catchup == nil)

	// We get a heartbeat that prompts us to catchup.
	n.processAppendEntry(aeHeartbeat2, n.aesub)
	require_Equal(t, n.wal.State().Msgs, 2)
	require_Equal(t, n.commit, 1) // Commit should not change, as we missed an item.
	require_True(t, n.catchup != nil)
	require_Equal(t, n.catchup.pterm, 1)  // n.pterm
	require_Equal(t, n.catchup.pindex, 2) // n.pindex

	// We now notice the leader indicated a different entry at the (no quorum) index. We should truncate again.
	n.processAppendEntry(aeMissed2, n.catchup.sub)
	require_Equal(t, n.wal.State().Msgs, 1)
	require_True(t, n.catchup == nil)

	// We get a heartbeat that prompts us to catchup.
	n.processAppendEntry(aeHeartbeat2, n.aesub)
	require_Equal(t, n.wal.State().Msgs, 1)
	require_Equal(t, n.commit, 1) // Commit should not change, as we missed an item.
	require_True(t, n.catchup != nil)
	require_Equal(t, n.catchup.pterm, 1)  // n.pterm
	require_Equal(t, n.catchup.pindex, 1) // n.pindex

	// We now get caught up with the missed messages.
	n.processAppendEntry(aeMissed1, n.catchup.sub)
	require_Equal(t, n.wal.State().Msgs, 2)
	require_True(t, n.catchup != nil)

	n.processAppendEntry(aeMissed2, n.catchup.sub)
	require_Equal(t, n.wal.State().Msgs, 3)
	require_True(t, n.catchup != nil)

	// We now get the entry that initially triggered us to catchup, it should be added.
	n.processAppendEntry(aeCatchupTrigger, n.catchup.sub)
	require_Equal(t, n.wal.State().Msgs, 4)
	require_True(t, n.catchup != nil)
	entry, err = n.loadEntry(4)
	require_NoError(t, err)
	require_Equal(t, entry.leader, nats1)

	// Heartbeat, makes sure we commit (and reset catchup, as we're now up-to-date).
	n.processAppendEntry(aeHeartbeat3, n.aesub)
	require_Equal(t, n.commit, 4)
	require_True(t, n.catchup == nil)
}

func TestNRGCatchupDoesNotTruncateCommittedEntriesDuringRedelivery(t *testing.T) {
	n, cleanup := initSingleMemRaftNode(t)
	defer cleanup()

	// Create a sample entry, the content doesn't matter, just that it's stored.
	esm := encodeStreamMsgAllowCompress("foo", "_INBOX.foo", nil, nil, 0, 0, true)
	entries := []*Entry{newEntry(EntryNormal, esm)}

	nats0 := "S1Nunr6R" // "nats-0"

	// Timeline.
	aeMsg1 := encode(t, &appendEntry{leader: nats0, term: 1, commit: 0, pterm: 0, pindex: 0, entries: entries})
	aeMsg2 := encode(t, &appendEntry{leader: nats0, term: 1, commit: 1, pterm: 1, pindex: 1, entries: entries})
	aeHeartbeat1 := encode(t, &appendEntry{leader: nats0, term: 1, commit: 2, pterm: 1, pindex: 2, entries: nil})
	aeMsg3 := encode(t, &appendEntry{leader: nats0, term: 1, commit: 1, pterm: 1, pindex: 2, entries: entries})
	aeHeartbeat2 := encode(t, &appendEntry{leader: nats0, term: 1, commit: 3, pterm: 1, pindex: 3, entries: nil})

	// Initial case is simple, just store the entry.
	n.processAppendEntry(aeMsg1, n.aesub)
	require_Equal(t, n.wal.State().Msgs, 1)
	entry, err := n.loadEntry(1)
	require_NoError(t, err)
	require_Equal(t, entry.leader, nats0)

	// Deliver a message.
	n.processAppendEntry(aeMsg2, n.aesub)
	require_Equal(t, n.wal.State().Msgs, 2)
	entry, err = n.loadEntry(2)
	require_NoError(t, err)
	require_Equal(t, entry.leader, nats0)

	// Heartbeat, makes sure commit moves up.
	n.processAppendEntry(aeHeartbeat1, n.aesub)
	require_Equal(t, n.commit, 2)

	// Deliver another message.
	n.processAppendEntry(aeMsg3, n.aesub)
	require_Equal(t, n.wal.State().Msgs, 3)
	entry, err = n.loadEntry(3)
	require_NoError(t, err)
	require_Equal(t, entry.leader, nats0)

	// Simulate receiving an old entry as a redelivery. We should not truncate as that lowers our commit.
	n.processAppendEntry(aeMsg1, n.aesub)
	require_Equal(t, n.commit, 2)

	// Heartbeat, makes sure we commit.
	n.processAppendEntry(aeHeartbeat2, n.aesub)
	require_Equal(t, n.commit, 3)
}

func TestNRGCatchupFromNewLeaderWithIncorrectPterm(t *testing.T) {
	n, cleanup := initSingleMemRaftNode(t)
	defer cleanup()

	// Create a sample entry, the content doesn't matter, just that it's stored.
	esm := encodeStreamMsgAllowCompress("foo", "_INBOX.foo", nil, nil, 0, 0, true)
	entries := []*Entry{newEntry(EntryNormal, esm)}

	nats0 := "S1Nunr6R" // "nats-0"

	// Timeline.
	aeMsg := encode(t, &appendEntry{leader: nats0, term: 1, commit: 0, pterm: 1, pindex: 0, entries: entries})
	aeHeartbeat := encode(t, &appendEntry{leader: nats0, term: 1, commit: 1, pterm: 1, pindex: 1, entries: nil})

	// Heartbeat, triggers catchup.
	n.processAppendEntry(aeHeartbeat, n.aesub)
	require_Equal(t, n.commit, 0) // Commit should not change, as we missed an item.
	require_True(t, n.catchup != nil)
	require_Equal(t, n.catchup.pterm, 0)  // n.pterm
	require_Equal(t, n.catchup.pindex, 0) // n.pindex

	// First catchup message has the incorrect pterm, stop catchup and re-trigger later with the correct pterm.
	n.processAppendEntry(aeMsg, n.catchup.sub)
	require_True(t, n.catchup == nil)
	require_Equal(t, n.pterm, 1)
	require_Equal(t, n.pindex, 0)

	// Heartbeat, triggers catchup.
	n.processAppendEntry(aeHeartbeat, n.aesub)
	require_Equal(t, n.commit, 0) // Commit should not change, as we missed an item.
	require_True(t, n.catchup != nil)
	require_Equal(t, n.catchup.pterm, 1)  // n.pterm
	require_Equal(t, n.catchup.pindex, 0) // n.pindex

	// Now we get the message again and can continue to store it.
	n.processAppendEntry(aeMsg, n.catchup.sub)
	require_Equal(t, n.wal.State().Msgs, 1)
	entry, err := n.loadEntry(1)
	require_NoError(t, err)
	require_Equal(t, entry.leader, nats0)

	// Now heartbeat is able to commit the entry.
	n.processAppendEntry(aeHeartbeat, n.aesub)
	require_Equal(t, n.commit, 1)
}

func TestNRGDontRemoveSnapshotIfTruncateToApplied(t *testing.T) {
	n, cleanup := initSingleMemRaftNode(t)
	defer cleanup()

	// Create a sample entry, the content doesn't matter, just that it's stored.
	esm := encodeStreamMsgAllowCompress("foo", "_INBOX.foo", nil, nil, 0, 0, true)
	entries := []*Entry{newEntry(EntryNormal, esm)}

	nats0 := "S1Nunr6R" // "nats-0"

	// Timeline.
	aeMsg := encode(t, &appendEntry{leader: nats0, term: 1, commit: 0, pterm: 0, pindex: 0, entries: entries})
	aeHeartbeat := encode(t, &appendEntry{leader: nats0, term: 1, commit: 1, pterm: 1, pindex: 1, entries: nil})

	// Initial case is simple, just store the entry.
	n.processAppendEntry(aeMsg, n.aesub)
	require_Equal(t, n.wal.State().Msgs, 1)
	entry, err := n.loadEntry(1)
	require_NoError(t, err)
	require_Equal(t, entry.leader, nats0)

	// Heartbeat, makes sure commit moves up.
	n.processAppendEntry(aeHeartbeat, n.aesub)
	require_Equal(t, n.commit, 1)
	require_Equal(t, n.pterm, 1)

	// Simulate upper layer calling down to apply.
	n.Applied(1)

	// Install snapshot and check it exists.
	err = n.InstallSnapshot(nil, false)
	require_NoError(t, err)

	snapshots := path.Join(n.sd, snapshotsDir)
	files, err := os.ReadDir(snapshots)
	require_NoError(t, err)
	require_Equal(t, len(files), 1)

	// Truncate and check snapshot is kept.
	n.truncateWAL(n.pterm, n.applied)

	files, err = os.ReadDir(snapshots)
	require_NoError(t, err)
	require_Equal(t, len(files), 1)
}

func TestNRGSnapshotAndTruncateToApplied(t *testing.T) {
	n, cleanup := initSingleMemRaftNode(t)
	defer cleanup()

	// Create a sample entry, the content doesn't matter, just that it's stored.
	esm := encodeStreamMsgAllowCompress("foo", "_INBOX.foo", nil, nil, 0, 0, true)
	entries := []*Entry{newEntry(EntryNormal, esm)}

	nats1 := "yrzKKRBu" // "nats-1"
	nats0 := "S1Nunr6R" // "nats-0"

	n.Lock()
	n.addPeer(nats1)
	n.Unlock()

	// Timeline, other leader
	aeMsg1 := encode(t, &appendEntry{leader: nats1, term: 1, commit: 0, pterm: 0, pindex: 0, entries: entries})
	aeMsg2 := encode(t, &appendEntry{leader: nats1, term: 1, commit: 1, pterm: 1, pindex: 1, entries: entries})

	// Timeline, we temporarily became leader
	aeHeartbeat1 := encode(t, &appendEntry{leader: nats0, term: 2, commit: 3, pterm: 1, pindex: 3, entries: nil})
	aeMsg3 := encode(t, &appendEntry{leader: nats0, term: 2, commit: 3, pterm: 1, pindex: 3, entries: entries})

	// Timeline, old leader is back.
	aeHeartbeat2 := encode(t, &appendEntry{leader: nats1, term: 3, commit: 3, pterm: 1, pindex: 3, entries: nil})

	// Simply receive first message.
	n.processAppendEntry(aeMsg1, n.aesub)
	require_Equal(t, n.commit, 0)
	require_Equal(t, n.wal.State().Msgs, 1)
	entry, err := n.loadEntry(1)
	require_NoError(t, err)
	require_Equal(t, entry.leader, nats1)

	// Receive second message, which commits the first message.
	n.processAppendEntry(aeMsg2, n.aesub)
	require_Equal(t, n.commit, 1)
	require_Equal(t, n.wal.State().Msgs, 2)
	entry, err = n.loadEntry(2)
	require_NoError(t, err)
	require_Equal(t, entry.leader, nats1)

	// Simulate upper layer calling down to apply.
	n.Applied(1)

	// Send heartbeat, which commits the second message.
	n.switchToLeader()
	n.term = aeHeartbeat1.term
	n.processAppendEntryResponse(&appendEntryResponse{
		term:    aeHeartbeat1.term,
		index:   aeHeartbeat1.pindex,
		peer:    nats1,
		reply:   _EMPTY_,
		success: true,
	})
	require_Equal(t, n.commit, 3)

	// Simulate upper layer calling down to apply.
	n.Applied(3)

	// Install snapshot and check it exists.
	err = n.InstallSnapshot(nil, false)
	require_NoError(t, err)

	snapshots := path.Join(n.sd, snapshotsDir)
	files, err := os.ReadDir(snapshots)
	require_NoError(t, err)
	require_Equal(t, len(files), 1)
	require_Equal(t, n.wal.State().Msgs, 0)

	// Store a third message, it stays uncommitted.
	require_NoError(t, n.storeToWAL(aeMsg3))
	require_Equal(t, n.commit, 3)
	require_Equal(t, n.wal.State().Msgs, 1)
	entry, err = n.loadEntry(4)
	require_NoError(t, err)
	require_Equal(t, entry.leader, nats0)

	// Receive heartbeat from new leader, should not lose commits.
	n.stepdown(noLeader)
	n.processAppendEntry(aeHeartbeat2, n.aesub)
	require_Equal(t, n.wal.State().Msgs, 0)
	require_Equal(t, n.commit, 3)
	require_Equal(t, n.applied, 3)
}

func TestNRGIgnoreDoubleSnapshot(t *testing.T) {
	n, cleanup := initSingleMemRaftNode(t)
	defer cleanup()

	// Create a sample entry, the content doesn't matter, just that it's stored.
	esm := encodeStreamMsgAllowCompress("foo", "_INBOX.foo", nil, nil, 0, 0, true)
	entries := []*Entry{newEntry(EntryNormal, esm)}

	nats0 := "S1Nunr6R" // "nats-0"

	// Timeline
	aeMsg1 := encode(t, &appendEntry{leader: nats0, term: 1, commit: 0, pterm: 0, pindex: 0, entries: entries})
	aeHeartbeat := encode(t, &appendEntry{leader: nats0, term: 1, commit: 1, pterm: 1, pindex: 1, entries: nil})
	aeMsg2 := encode(t, &appendEntry{leader: nats0, term: 2, commit: 1, pterm: 1, pindex: 1, entries: entries})

	// Simply receive first message.
	n.processAppendEntry(aeMsg1, n.aesub)
	require_Equal(t, n.pindex, 1)
	require_Equal(t, n.commit, 0)

	// Heartbeat moves commit up.
	n.processAppendEntry(aeHeartbeat, n.aesub)
	require_Equal(t, n.commit, 1)

	// Manually call back down to applied.
	n.Applied(1)

	// Second message just for upping the pterm.
	n.processAppendEntry(aeMsg2, n.aesub)
	require_Equal(t, n.pindex, 2)
	require_Equal(t, n.commit, 1)
	require_Equal(t, n.pterm, 2)
	require_Equal(t, n.term, 2)

	// Snapshot, and confirm state.
	err := n.InstallSnapshot(nil, false)
	require_NoError(t, err)
	snap, err := n.loadLastSnapshot()
	require_NoError(t, err)
	require_Equal(t, snap.lastTerm, 1)
	require_Equal(t, snap.lastIndex, 1)

	// Snapshot again, should not overwrite previous snapshot.
	err = n.InstallSnapshot(nil, false)
	require_Error(t, err, errNoSnapAvailable)
	snap, err = n.loadLastSnapshot()
	require_NoError(t, err)
	require_Equal(t, snap.lastTerm, 1)
	require_Equal(t, snap.lastIndex, 1)
}

func TestNRGDontSwitchToCandidateWithInflightSnapshot(t *testing.T) {
	n, cleanup := initSingleMemRaftNode(t)
	defer cleanup()

	// Create a sample snapshot entry, the content doesn't matter.
	snapshotEntries := []*Entry{
		newEntry(EntrySnapshot, nil),
		newEntry(EntryPeerState, encodePeerState(&peerState{n.peerNames(), n.csz, n.extSt})),
	}

	nats0 := "S1Nunr6R" // "nats-0"

	// Timeline.
	aeTriggerCatchup := encode(t, &appendEntry{leader: nats0, term: 1, commit: 1, pterm: 1, pindex: 1, entries: nil})
	aeCatchupSnapshot := encode(t, &appendEntry{leader: nats0, term: 1, commit: 1, pterm: 1, pindex: 1, entries: snapshotEntries})

	// Switch follower into catchup.
	n.processAppendEntry(aeTriggerCatchup, n.aesub)
	require_True(t, n.catchup != nil)
	require_Equal(t, n.catchup.pterm, 0)  // n.pterm
	require_Equal(t, n.catchup.pindex, 0) // n.pindex

	// Follower receives a snapshot, marking a snapshot as inflight as the apply queue is async.
	n.processAppendEntry(aeCatchupSnapshot, n.catchup.sub)
	require_Equal(t, n.pindex, 1)
	require_Equal(t, n.commit, 1)

	// Try to switch to candidate, it should be blocked since the snapshot is not processed yet.
	n.switchToCandidate()
	require_Equal(t, n.State(), Follower)

	// Simulate snapshot being processed by the upper layer.
	n.Applied(1)

	// Retry becoming candidate, snapshot is processed so can now do so.
	n.switchToCandidate()
	require_Equal(t, n.State(), Candidate)
}

func TestNRGDontSwitchToCandidateWithMultipleInflightSnapshots(t *testing.T) {
	n, cleanup := initSingleMemRaftNode(t)
	defer cleanup()

	// Create a sample snapshot entry, the content doesn't matter.
	snapshotEntries := []*Entry{
		newEntry(EntrySnapshot, nil),
		newEntry(EntryPeerState, encodePeerState(&peerState{n.peerNames(), n.csz, n.extSt})),
	}

	nats0 := "S1Nunr6R" // "nats-0"

	// Timeline.
	aeSnapshot1 := encode(t, &appendEntry{leader: nats0, term: 1, commit: 0, pterm: 0, pindex: 0, entries: snapshotEntries})
	aeSnapshot2 := encode(t, &appendEntry{leader: nats0, term: 1, commit: 1, pterm: 1, pindex: 1, entries: snapshotEntries})
	aeHeartbeat := encode(t, &appendEntry{leader: nats0, term: 1, commit: 2, pterm: 1, pindex: 2, entries: nil})

	// Simulate snapshots being sent to us.
	n.processAppendEntry(aeSnapshot1, n.aesub)
	require_Equal(t, n.pindex, 1)
	require_Equal(t, n.commit, 0)
	require_Equal(t, n.applied, 0)

	n.processAppendEntry(aeSnapshot2, n.aesub)
	require_Equal(t, n.pindex, 2)
	require_Equal(t, n.commit, 1)
	require_Equal(t, n.applied, 0)

	n.processAppendEntry(aeHeartbeat, n.aesub)
	require_Equal(t, n.pindex, 2)
	require_Equal(t, n.commit, 2)
	require_Equal(t, n.applied, 0)

	for i := uint64(1); i <= 2; i++ {
		// Try to switch to candidate, it should be blocked since the snapshot is not processed yet.
		n.switchToCandidate()
		require_Equal(t, n.State(), Follower)

		// Simulate snapshot being processed by the upper layer.
		n.Applied(i)
	}

	// Retry becoming candidate, all snapshots processed so can now do so.
	n.switchToCandidate()
	require_Equal(t, n.State(), Candidate)
}

func TestNRGRecoverPindexPtermOnlyIfLogNotEmpty(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "R3S", 3)
	defer c.shutdown()

	nc, _ := jsClientConnect(t, c.leader(), nats.UserInfo("admin", "s3cr3t!"))
	defer nc.Close()

	rg := c.createRaftGroup("TEST", 3, newStateAdder)
	rg.waitOnLeader()

	gn := rg[0].(*stateAdder)
	rn := rg[0].node().(*raft)

	// Delete the msgs and snapshots, leaving the only remaining trace
	// of the term in the TAV file.
	store := filepath.Join(gn.cfg.Store)
	require_NoError(t, rn.wal.Truncate(0))
	require_NoError(t, os.RemoveAll(filepath.Join(store, "msgs")))
	require_NoError(t, os.RemoveAll(filepath.Join(store, "snapshots")))

	for _, gn := range rg {
		gn.stop()
	}
	rg[0].restart()
	rn = rg[0].node().(*raft)

	// Both should be zero as, without any snapshots or log entries,
	// the log is considered empty and therefore we account as such.
	require_Equal(t, rn.pterm, 0)
	require_Equal(t, rn.pindex, 0)
}

func TestNRGCancelCatchupWhenDetectingHigherTermDuringVoteRequest(t *testing.T) {
	n, cleanup := initSingleMemRaftNode(t)
	defer cleanup()

	// Create a sample entry, the content doesn't matter, just that it's stored.
	esm := encodeStreamMsgAllowCompress("foo", "_INBOX.foo", nil, nil, 0, 0, true)
	entries := []*Entry{newEntry(EntryNormal, esm)}

	nats0 := "S1Nunr6R" // "nats-0"

	// Timeline.
	aeCatchupTrigger := encode(t, &appendEntry{leader: nats0, term: 1, commit: 1, pterm: 1, pindex: 1, entries: entries})
	aeMsg1 := encode(t, &appendEntry{leader: nats0, term: 1, commit: 0, pterm: 0, pindex: 0, entries: entries})

	// Truncate to simulate we missed one message and need to catchup.
	n.processAppendEntry(aeCatchupTrigger, n.aesub)
	require_True(t, n.catchup != nil)
	require_Equal(t, n.catchup.pterm, 0)  // n.pterm
	require_Equal(t, n.catchup.pindex, 0) // n.pindex

	// Process first message as part of the catchup.
	catchupSub := n.catchup.sub
	n.processAppendEntry(aeMsg1, catchupSub)
	require_True(t, n.catchup != nil)

	// Receiving a vote request should cancel our catchup.
	// Otherwise, we could receive catchup messages after this that provides the previous leader with quorum.
	// If the new leader doesn't have these entries, the previous leader would desync since it would commit them.
	err := n.processVoteRequest(&voteRequest{2, 1, 1, nats0, "reply"})
	require_NoError(t, err)
	require_True(t, n.catchup == nil)
}

func TestNRGMultipleStopsDontPanic(t *testing.T) {
	n, cleanup := initSingleMemRaftNode(t)
	defer cleanup()

	defer func() {
		p := recover()
		require_True(t, p == nil)
	}()

	for i := 0; i < 10; i++ {
		n.Stop()
	}
}

func TestNRGTruncateDownToCommitted(t *testing.T) {
	n, cleanup := initSingleMemRaftNode(t)
	defer cleanup()

	// Create a sample entry, the content doesn't matter, just that it's stored.
	esm := encodeStreamMsgAllowCompress("foo", "_INBOX.foo", nil, nil, 0, 0, true)
	entries := []*Entry{newEntry(EntryNormal, esm)}

	nats0 := "S1Nunr6R" // "nats-0"
	nats1 := "yrzKKRBu" // "nats-1"

	// Timeline, we are leader
	aeMsg1 := encode(t, &appendEntry{leader: nats0, term: 1, commit: 0, pterm: 0, pindex: 0, entries: entries})
	aeMsg2 := encode(t, &appendEntry{leader: nats0, term: 1, commit: 1, pterm: 1, pindex: 1, entries: entries})

	// Timeline, after leader change
	aeMsg3 := encode(t, &appendEntry{leader: nats1, term: 2, commit: 1, pterm: 1, pindex: 1, entries: entries})
	aeHeartbeat := encode(t, &appendEntry{leader: nats1, term: 2, commit: 2, pterm: 2, pindex: 2, entries: nil})

	// Simply receive first message.
	n.processAppendEntry(aeMsg1, n.aesub)
	require_Equal(t, n.commit, 0)
	require_Equal(t, n.wal.State().Msgs, 1)
	entry, err := n.loadEntry(1)
	require_NoError(t, err)
	require_Equal(t, entry.leader, nats0)

	// Receive second message, which commits the first message.
	n.processAppendEntry(aeMsg2, n.aesub)
	require_Equal(t, n.commit, 1)
	require_Equal(t, n.wal.State().Msgs, 2)
	entry, err = n.loadEntry(2)
	require_NoError(t, err)
	require_Equal(t, entry.leader, nats0)

	// We receive an entry from another leader, should truncate down to commit / remove the second message.
	// After doing so, we should also be able to immediately store the message after.
	n.processAppendEntry(aeMsg3, n.aesub)
	require_Equal(t, n.commit, 1)
	require_Equal(t, n.wal.State().Msgs, 2)
	entry, err = n.loadEntry(2)
	require_NoError(t, err)
	require_Equal(t, entry.leader, nats1)

	// Heartbeat moves commit up.
	n.processAppendEntry(aeHeartbeat, n.aesub)
	require_Equal(t, n.commit, 2)
}

type mockWALTruncateAlwaysFails struct {
	WAL
}

func (m mockWALTruncateAlwaysFails) Truncate(seq uint64) error {
	return errors.New("test: truncate always fails")
}

func TestNRGTruncateDownToCommittedWhenTruncateFails(t *testing.T) {
	n, cleanup := initSingleMemRaftNode(t)
	defer cleanup()

	n.Lock()
	n.wal = mockWALTruncateAlwaysFails{n.wal}
	n.Unlock()

	// Create a sample entry, the content doesn't matter, just that it's stored.
	esm := encodeStreamMsgAllowCompress("foo", "_INBOX.foo", nil, nil, 0, 0, true)
	entries := []*Entry{newEntry(EntryNormal, esm)}

	nats0 := "S1Nunr6R" // "nats-0"
	nats1 := "yrzKKRBu" // "nats-1"

	// Timeline, we are leader
	aeMsg1 := encode(t, &appendEntry{leader: nats0, term: 1, commit: 0, pterm: 0, pindex: 0, entries: entries})
	aeMsg2 := encode(t, &appendEntry{leader: nats0, term: 1, commit: 1, pterm: 1, pindex: 1, entries: entries})

	// Timeline, after leader change
	aeMsg3 := encode(t, &appendEntry{leader: nats1, term: 2, commit: 1, pterm: 1, pindex: 1, entries: entries})

	// Simply receive first message.
	n.processAppendEntry(aeMsg1, n.aesub)
	require_Equal(t, n.commit, 0)
	require_Equal(t, n.wal.State().Msgs, 1)
	entry, err := n.loadEntry(1)
	require_NoError(t, err)
	require_Equal(t, entry.leader, nats0)

	// Receive second message, which commits the first message.
	n.processAppendEntry(aeMsg2, n.aesub)
	require_Equal(t, n.commit, 1)
	require_Equal(t, n.wal.State().Msgs, 2)
	entry, err = n.loadEntry(2)
	require_NoError(t, err)
	require_Equal(t, entry.leader, nats0)

	// We receive an entry from another leader, should truncate down to commit / remove the second message.
	// But, truncation fails so should register that and not change pindex/pterm.
	bindex, bterm := n.pindex, n.pterm
	n.processAppendEntry(aeMsg3, n.aesub)
	require_Error(t, n.werr, errors.New("test: truncate always fails"))
	require_Equal(t, bindex, n.pindex)
	require_Equal(t, bterm, n.pterm)
}

func TestNRGForwardProposalResponse(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "R3S", 3)
	defer c.shutdown()

	nc, _ := jsClientConnect(t, c.leader(), nats.UserInfo("admin", "s3cr3t!"))
	defer nc.Close()

	rg := c.createRaftGroup("TEST", 3, newStateAdder)
	rg.waitOnLeader()

	n := rg.nonLeader().node().(*raft)
	psubj := n.psubj

	data := make([]byte, binary.MaxVarintLen64)
	dn := binary.PutVarint(data, int64(123))

	_, err := nc.Request(psubj, data[:dn], time.Second*5)
	require_NoError(t, err)

	rg.waitOnTotal(t, 123)
}

func TestNRGMemoryWALEmptiesSnapshotsDir(t *testing.T) {
	n, c := initSingleMemRaftNodeWithCluster(t)
	defer c.shutdown()

	// Create a sample entry, the content doesn't matter, just that it's stored.
	esm := encodeStreamMsgAllowCompress("foo", "_INBOX.foo", nil, nil, 0, 0, true)
	entries := []*Entry{newEntry(EntryNormal, esm)}

	nats0 := "S1Nunr6R" // "nats-0"

	// Timeline
	aeMsg := encode(t, &appendEntry{leader: nats0, term: 1, commit: 0, pterm: 0, pindex: 0, entries: entries})
	aeHeartbeat := encode(t, &appendEntry{leader: nats0, term: 1, commit: 1, pterm: 1, pindex: 1, entries: nil})

	// Simply receive first message.
	n.processAppendEntry(aeMsg, n.aesub)
	require_Equal(t, n.pindex, 1)
	require_Equal(t, n.commit, 0)

	// Heartbeat moves commit up.
	n.processAppendEntry(aeHeartbeat, n.aesub)
	require_Equal(t, n.commit, 1)

	// Manually call back down to applied, and then snapshot.
	n.Applied(1)
	err := n.InstallSnapshot(nil, false)
	require_NoError(t, err)

	// Stop current node and restart it.
	n.Stop()
	n.WaitForStop()

	s := c.servers[0]
	ms, err := newMemStore(&StreamConfig{Name: "TEST", Storage: MemoryStorage})
	require_NoError(t, err)
	cfg := &RaftConfig{Name: "TEST", Store: n.sd, Log: ms}
	n, err = s.initRaftNode(globalAccountName, cfg, pprofLabels{})
	require_NoError(t, err)

	// Since the WAL is in-memory, the snapshots dir should've been emptied upon restart.
	files, err := os.ReadDir(filepath.Join(n.sd, snapshotsDir))
	require_NoError(t, err)
	require_Len(t, len(files), 0)
}

func TestNRGHealthCheckWaitForCatchup(t *testing.T) {
	n, cleanup := initSingleMemRaftNode(t)
	defer cleanup()

	// Create a sample entry, the content doesn't matter, just that it's stored.
	esm := encodeStreamMsgAllowCompress("foo", "_INBOX.foo", nil, nil, 0, 0, true)
	entries := []*Entry{newEntry(EntryNormal, esm)}

	nats0 := "S1Nunr6R" // "nats-0"

	n.Lock()
	n.addPeer(nats0)
	n.Unlock()

	// Timeline
	aeMsg1 := encode(t, &appendEntry{leader: nats0, term: 1, commit: 0, pterm: 0, pindex: 0, entries: entries})
	aeMsg2 := encode(t, &appendEntry{leader: nats0, term: 1, commit: 1, pterm: 1, pindex: 1, entries: entries})
	aeMsg3 := encode(t, &appendEntry{leader: nats0, term: 1, commit: 2, pterm: 1, pindex: 2, entries: entries})
	aeHeartbeat := encode(t, &appendEntry{leader: nats0, term: 1, commit: 3, pterm: 1, pindex: 3, entries: nil})

	// Switch follower into catchup.
	n.processAppendEntry(aeHeartbeat, n.aesub)
	require_True(t, n.catchup != nil)
	require_Equal(t, n.catchup.pterm, 0)  // n.pterm
	require_Equal(t, n.catchup.pindex, 0) // n.pindex
	require_Equal(t, n.catchup.cterm, aeHeartbeat.term)
	require_Equal(t, n.catchup.cindex, aeHeartbeat.pindex)

	// Catchup first message.
	n.processAppendEntry(aeMsg1, n.catchup.sub)
	require_Equal(t, n.pindex, 1)
	require_False(t, n.Healthy())

	// Catchup second message.
	n.processAppendEntry(aeMsg2, n.catchup.sub)
	require_Equal(t, n.pindex, 2)
	require_Equal(t, n.commit, 1)
	require_False(t, n.Healthy())

	// If we apply the entry sooner than we receive the next catchup message,
	// should not mark as healthy since we're still in catchup.
	n.Applied(1)
	require_False(t, n.Healthy())

	// Catchup third message.
	n.processAppendEntry(aeMsg3, n.catchup.sub)
	require_Equal(t, n.pindex, 3)
	require_Equal(t, n.commit, 2)
	n.Applied(2)
	require_False(t, n.Healthy())

	// Heartbeat stops catchup.
	n.processAppendEntry(aeHeartbeat, n.aesub)
	require_True(t, n.catchup == nil)
	require_Equal(t, n.pindex, 3)
	require_Equal(t, n.commit, 3)
	require_False(t, n.Healthy())

	// Still need to wait for the last entry to be applied.
	n.Applied(3)
	require_True(t, n.Healthy())
}

func TestNRGHealthCheckWaitForDoubleCatchup(t *testing.T) {
	n, cleanup := initSingleMemRaftNode(t)
	defer cleanup()

	// Create a sample entry, the content doesn't matter, just that it's stored.
	esm := encodeStreamMsgAllowCompress("foo", "_INBOX.foo", nil, nil, 0, 0, true)
	entries := []*Entry{newEntry(EntryNormal, esm)}

	nats0 := "S1Nunr6R" // "nats-0"

	n.Lock()
	n.addPeer(nats0)
	n.Unlock()

	// Timeline
	aeMsg1 := encode(t, &appendEntry{leader: nats0, term: 1, commit: 0, pterm: 0, pindex: 0, entries: entries})
	aeMsg2 := encode(t, &appendEntry{leader: nats0, term: 1, commit: 1, pterm: 1, pindex: 1, entries: entries})
	aeHeartbeat1 := encode(t, &appendEntry{leader: nats0, term: 1, commit: 2, pterm: 1, pindex: 2, entries: nil})
	aeMsg3 := encode(t, &appendEntry{leader: nats0, term: 1, commit: 2, pterm: 1, pindex: 2, entries: entries})
	aeHeartbeat2 := encode(t, &appendEntry{leader: nats0, term: 1, commit: 3, pterm: 1, pindex: 3, entries: nil})

	// Switch follower into catchup.
	n.processAppendEntry(aeHeartbeat1, n.aesub)
	require_True(t, n.catchup != nil)
	require_Equal(t, n.catchup.pterm, 0)  // n.pterm
	require_Equal(t, n.catchup.pindex, 0) // n.pindex
	require_Equal(t, n.catchup.cterm, aeHeartbeat1.term)
	require_Equal(t, n.catchup.cindex, aeHeartbeat1.pindex)

	// Catchup first message.
	n.processAppendEntry(aeMsg1, n.catchup.sub)
	require_Equal(t, n.pindex, 1)
	require_False(t, n.Healthy())

	// We miss this message, since we're catching up.
	n.processAppendEntry(aeMsg3, n.aesub)
	require_True(t, n.catchup != nil)
	require_Equal(t, n.pindex, 1)
	require_False(t, n.Healthy())

	// We also miss the heartbeat, since we're catching up.
	n.processAppendEntry(aeHeartbeat2, n.aesub)
	require_True(t, n.catchup != nil)
	require_Equal(t, n.pindex, 1)
	require_False(t, n.Healthy())

	// Catchup second message, this will stop catchup.
	n.processAppendEntry(aeMsg2, n.catchup.sub)
	require_Equal(t, n.pindex, 2)
	require_Equal(t, n.commit, 1)
	n.Applied(1)
	require_False(t, n.Healthy())

	// We expect to still be in catchup, waiting for a heartbeat or new append entry to reset.
	require_True(t, n.catchup != nil)
	require_Equal(t, n.catchup.cterm, aeHeartbeat1.term)
	require_Equal(t, n.catchup.cindex, aeHeartbeat1.pindex)

	// We now get a 'future' heartbeat, should restart catchup.
	n.processAppendEntry(aeHeartbeat2, n.aesub)
	require_True(t, n.catchup != nil)
	require_Equal(t, n.catchup.pterm, 1)  // n.pterm
	require_Equal(t, n.catchup.pindex, 2) // n.pindex
	require_Equal(t, n.catchup.cterm, aeHeartbeat2.term)
	require_Equal(t, n.catchup.cindex, aeHeartbeat2.pindex)
	require_False(t, n.Healthy())

	// Catchup third message.
	n.processAppendEntry(aeMsg3, n.catchup.sub)
	require_Equal(t, n.pindex, 3)
	require_Equal(t, n.commit, 2)
	n.Applied(2)
	require_False(t, n.Healthy())

	// Heartbeat stops catchup.
	n.processAppendEntry(aeHeartbeat2, n.aesub)
	require_True(t, n.catchup == nil)
	require_Equal(t, n.pindex, 3)
	require_Equal(t, n.commit, 3)
	require_False(t, n.Healthy())

	// Still need to wait for the last entry to be applied.
	n.Applied(3)
	require_True(t, n.Healthy())
}

func TestNRGHealthCheckWaitForPendingCommitsWhenPaused(t *testing.T) {
	n, cleanup := initSingleMemRaftNode(t)
	defer cleanup()

	// Create a sample entry, the content doesn't matter, just that it's stored.
	esm := encodeStreamMsgAllowCompress("foo", "_INBOX.foo", nil, nil, 0, 0, true)
	entries := []*Entry{newEntry(EntryNormal, esm)}

	nats0 := "S1Nunr6R" // "nats-0"

	n.Lock()
	n.addPeer(nats0)
	n.Unlock()

	// Timeline
	aeMsg1 := encode(t, &appendEntry{leader: nats0, term: 1, commit: 0, pterm: 0, pindex: 0, entries: entries})
	aeMsg2 := encode(t, &appendEntry{leader: nats0, term: 1, commit: 1, pterm: 1, pindex: 1, entries: entries})
	aeHeartbeat := encode(t, &appendEntry{leader: nats0, term: 1, commit: 2, pterm: 1, pindex: 2, entries: nil})

	// Process first message.
	n.processAppendEntry(aeMsg1, n.aesub)
	require_Equal(t, n.pindex, 1)
	require_False(t, n.Healthy())

	// Process second message, moves commit up.
	n.processAppendEntry(aeMsg2, n.aesub)
	require_Equal(t, n.pindex, 2)
	require_False(t, n.Healthy())

	// We're healthy once we've applied the first message.
	n.Applied(1)
	require_True(t, n.Healthy())

	// If we're paused we still are healthy if there are no pending commits.
	err := n.PauseApply()
	require_NoError(t, err)
	require_True(t, n.Healthy())

	// Heartbeat marks second message to be committed.
	n.processAppendEntry(aeHeartbeat, n.aesub)
	require_Equal(t, n.pindex, 2)
	require_False(t, n.Healthy())

	// Resuming apply commits the message.
	n.ResumeApply()
	require_NoError(t, err)
	require_False(t, n.Healthy())

	// But still waiting for it to be applied before marking healthy.
	n.Applied(2)
	require_True(t, n.Healthy())
}

func TestNRGHealthy(t *testing.T) {
	// The leader grace period is twice the heartbeat interval, needs to be long enough to time it.
	origHBInterval := hbInterval
	hbInterval = 200 * time.Millisecond
	defer func() { hbInterval = origHBInterval }()

	s := runRaftTestServer(t)
	defer s.Shutdown()

	nats0 := "S1Nunr6R" // "nats-0"
	esm := encodeStreamMsgAllowCompress("foo", "_INBOX.foo", nil, nil, 0, 0, true)
	entries := []*Entry{newEntry(EntryNormal, esm)}
	processAE := func(n *raft, term, commit, pterm, pindex uint64, entries []*Entry) {
		ae := &appendEntry{leader: nats0, term: term, commit: commit, pterm: pterm, pindex: pindex, entries: entries}
		n.processAppendEntry(encode(t, ae), n.aesub)
	}
	leaderless := func(n *raft) {
		n.updateLeader(noLeader)
		n.nlt, n.lgt = time.Now().Add(-leaderlessHealthy), time.Time{}
	}
	// A vote request for a newer term clears the leader.
	requestVote := func(n *raft) {
		require_NoError(t, n.processVoteRequest(&voteRequest{term: n.term + 1, lastTerm: n.pterm, lastIndex: n.pindex, candidate: nats0}))
	}

	for i, test := range []struct {
		title   string
		setup   func(n *raft)
		healthy bool
		behind  bool // Whether falling behind was logged.
	}{
		{title: "follower processed leader index", healthy: true},
		{title: "closed", setup: func(n *raft) { n.state.Store(int32(Closed)) }},
		{title: "no commits", setup: func(n *raft) { n.commit = 0 }},
		{title: "leader processing prior terms", setup: func(n *raft) { n.state.Store(int32(Leader)) }},
		{title: "leader processed prior terms", healthy: true, setup: func(n *raft) {
			n.state.Store(int32(Leader))
			n.leaderState.Store(true)
		}},
		{title: "follower below leader index", behind: true, setup: func(n *raft) { n.hcindex = 2 }},
		{title: "follower with pending commits past leader index", healthy: true, setup: func(n *raft) { n.pindex, n.commit = 2, 2 }},
		{title: "catching up", setup: func(n *raft) { n.catchup = &catchupState{} }},
		{title: "paused with pending commits", setup: func(n *raft) { n.paused, n.hcommit = true, 2 }},
		{title: "paused without pending commits", healthy: true, setup: func(n *raft) { n.paused, n.hcommit = true, 1 }},
		{title: "falling behind on applies", setup: func(n *raft) { n.quorumPaused = true }},
		{title: "no recent leader contact", setup: func(n *raft) { n.peers[nats0].ts = time.Now().Add(-4 * hbInterval) }},
		{title: "leader contact over two heartbeats ago", healthy: true, setup: func(n *raft) { n.peers[nats0].ts = time.Now().Add(-5 * hbInterval / 2) }},
		{title: "new term without leader contact", setup: func(n *raft) { n.term = 2 }},
		{title: "new term records leader index", behind: true, setup: func(n *raft) {
			processAE(n, 2, 1, 1, 1, entries)
			n.lgt = time.Time{}
		}},
		{title: "same term keeps leader index", healthy: true, setup: func(n *raft) { processAE(n, 1, 1, 1, 1, entries) }},
		{title: "multiple entries record a single index", healthy: true, setup: func(n *raft) {
			processAE(n, 2, 1, 1, 1, []*Entry{newEntry(EntryNormal, esm), newEntry(EntryNormal, esm), newEntry(EntryNormal, esm)})
			processAE(n, 2, 2, 2, 2, nil)
			n.Applied(2)
			n.lgt = time.Time{}
		}},
		{title: "new term replaces leader index of truncated entry", healthy: true, setup: func(n *raft) {
			processAE(n, 2, 1, 1, 1, entries)
			processAE(n, 3, 1, 1, 1, nil)
			n.lgt = time.Time{}
		}},
		{title: "new term within leader grace", healthy: true, setup: func(n *raft) { processAE(n, 2, 1, 1, 1, entries) }},
		{title: "vote request within leader grace", healthy: true, setup: requestVote},
		{title: "vote request past leader grace", setup: func(n *raft) {
			requestVote(n)
			n.lgt = time.Now().Add(-2 * hbInterval)
		}},
		{title: "vote request without grace while behind", setup: func(n *raft) {
			n.hcindex = 2
			requestVote(n)
			require_False(t, n.hcbehind)
		}},
		{title: "vote request without grace while quorum paused", setup: func(n *raft) {
			n.quorumPaused = true
			requestVote(n)
		}},
		{title: "vote request without grace after no leader past timeout", behind: true, setup: func(n *raft) {
			leaderless(n)
			requestVote(n)
			processAE(n, n.term, 1, 1, 1, entries)
		}},
		{title: "vote request without extending grace", setup: func(n *raft) {
			n.hcindex = 2
			n.lgt = time.Now().Add(-hbInterval)
			requestVote(n)
			time.Sleep(hbInterval * 3 / 2)
		}},
		{title: "vote request without grace after losing leader contact", setup: func(n *raft) {
			n.peers[nats0].ts = time.Now().Add(-4 * hbInterval)
			requestVote(n)
		}},
		{title: "election timeout without grace after losing leader contact", setup: func(n *raft) {
			// The election timeout is at least four heartbeats, so leader contact is already lost.
			n.peers[nats0].ts = time.Now().Add(-4 * hbInterval)
			// Skip the peer interest check, nats-0 isn't a real server.
			n.qn = 1
			n.switchToCandidate()
			require_Equal(t, n.term, 2)
		}},
		{title: "leader stepping down within leader grace", healthy: true, setup: func(n *raft) {
			n.state.Store(int32(Leader))
			n.leaderState.Store(true)
			n.switchToFollowerLocked(noLeader)
		}},
		{title: "leader processing prior terms stepping down without grace", setup: func(n *raft) {
			n.state.Store(int32(Leader))
			n.switchToFollowerLocked(noLeader)
		}},
		{title: "no leader", setup: func(n *raft) {
			n.updateLeader(noLeader)
			n.lgt = time.Time{}
		}},
		{title: "no leader past timeout", healthy: true, setup: leaderless},
		{title: "no leader past timeout with pending commits", setup: func(n *raft) {
			leaderless(n)
			n.pindex, n.commit = 2, 2
		}},
		{title: "no leader past timeout while quorum paused", healthy: true, setup: func(n *raft) {
			leaderless(n)
			n.quorumPaused = true
		}},
	} {
		t.Run(test.title, func(t *testing.T) {
			ms, err := newMemStore(&StreamConfig{Name: "TEST", Storage: MemoryStorage})
			require_NoError(t, err)
			cfg := &RaftConfig{Name: fmt.Sprintf("TEST-%d", i), Store: t.TempDir(), Log: ms}
			require_NoError(t, s.bootstrapRaftNode(cfg, []string{s.sys.shash[:idLen]}, true))
			n, err := s.initRaftNode(globalAccountName, cfg, pprofLabels{})
			require_NoError(t, err)
			n.addPeer(nats0)

			// Follower that processed the leader's first entry.
			processAE(n, 1, 0, 0, 0, entries)
			processAE(n, 1, 1, 1, 1, nil)
			n.Applied(1)

			if test.setup != nil {
				test.setup(n)
			}
			require_Equal(t, n.Healthy(), test.healthy)
			require_Equal(t, n.hcbehind, test.behind)
		})
	}
}

func TestNRGAppendEntryCanEstablishQuorumAfterLeaderChange(t *testing.T) {
	n, cleanup := initSingleMemRaftNode(t)
	defer cleanup()

	// Create a sample entry, the content doesn't matter, just that it's stored.
	esm := encodeStreamMsgAllowCompress("foo", "_INBOX.foo", nil, nil, 0, 0, true)
	entries := []*Entry{newEntry(EntryNormal, esm)}

	nats0 := "S1Nunr6R" // "nats-0"

	n.Lock()
	n.addPeer(nats0)
	n.Unlock()

	// Timeline
	aeMsg := encode(t, &appendEntry{leader: nats0, term: 1, commit: 0, pterm: 0, pindex: 0, entries: entries})
	aeHeartbeatResponse := &appendEntryResponse{term: 1, index: 2, peer: nats0, success: true}

	// Process first message.
	n.processAppendEntry(aeMsg, n.aesub)
	require_Equal(t, n.pindex, 1)
	require_Equal(t, n.aflr, 0)

	// Simulate becoming leader, and not knowing if the stored entry has quorum and can be committed.
	// Switching to leader should send a heartbeat.
	n.switchToLeader()
	require_Equal(t, n.aflr, 2)
	require_Equal(t, n.commit, 0)

	// We simulate receiving the successful heartbeat response here. It should move the commit up.
	n.processAppendEntryResponse(aeHeartbeatResponse)
	require_Equal(t, n.commit, 2)
	require_Equal(t, n.aflr, 2)

	// Once the entry is applied, it should reset the applied floor.
	n.Applied(2)
	require_Equal(t, n.aflr, 0)
}

func TestNRGQuorumAccounting(t *testing.T) {
	n, cleanup := initSingleMemRaftNode(t)
	defer cleanup()

	nats1 := "yrzKKRBu" // "nats-1"
	nats2 := "cnrtt3eg" // "nats-2"

	n.Lock()
	n.addPeer(nats1)
	n.addPeer(nats2)
	n.Unlock()

	// Timeline
	aeHeartbeat1Response := &appendEntryResponse{term: 1, index: 1, peer: nats1, success: true}
	aeHeartbeat2Response := &appendEntryResponse{term: 1, index: 1, peer: nats2, success: true}

	// Adjust cluster size, so we need at least 2 responses from other servers to establish quorum.
	require_NoError(t, n.AdjustBootClusterSize(5))
	require_Equal(t, n.csz, 5)
	require_Equal(t, n.qn, 3)

	// Switch this node to leader which sends an entry.
	n.switchToLeader()
	n.term = 1
	require_Equal(t, n.pindex, 1)

	// The first response MUST NOT indicate quorum has been reached.
	n.processAppendEntryResponse(aeHeartbeat1Response)
	require_Equal(t, n.commit, 0)

	// The second response means we have reached quorum and can move commit up.
	n.processAppendEntryResponse(aeHeartbeat2Response)
	require_Equal(t, n.commit, 1)
}

func TestNRGRevalidateQuorumAfterLeaderChange(t *testing.T) {
	n, cleanup := initSingleMemRaftNode(t)
	defer cleanup()

	nats1 := "yrzKKRBu" // "nats-1"
	nats2 := "cnrtt3eg" // "nats-2"

	n.Lock()
	n.addPeer(nats1)
	n.addPeer(nats2)
	n.Unlock()

	// Timeline
	aeHeartbeat1Response := &appendEntryResponse{term: 1, index: 1, peer: nats1, success: true}
	aeHeartbeat2Response := &appendEntryResponse{term: 6, index: 2, peer: nats2, success: true}

	// Adjust cluster size, so we need at least 2 responses from other servers to establish quorum.
	require_NoError(t, n.AdjustBootClusterSize(5))
	require_Equal(t, n.csz, 5)
	require_Equal(t, n.qn, 3)

	// Switch this node to leader which sends an entry.
	n.term++
	n.switchToLeader()
	require_Equal(t, n.term, 1)
	require_Equal(t, n.pindex, 1)

	// We have one server that signals the message was stored. The leader will add 1 to the acks count.
	n.processAppendEntryResponse(aeHeartbeat1Response)
	require_Equal(t, n.commit, 0)
	require_Len(t, len(n.acks), 1)

	// We stepdown now and don't know if we will have quorum on the first entry.
	n.stepdown(noLeader)

	// Let's assume there are a bunch of leader elections now, data being added to the log, being truncated, etc.
	// We don't know what happened, maybe we were partitioned, but we can't know for sure if the first entry has quorum.

	// We now become leader again.
	n.term = 6
	n.switchToLeader()
	require_Equal(t, n.term, 6)

	// We now receive a successful response from another server saying they have stored it.
	// Anything can have happened to the replica that said success before, we can't assume that's still valid.
	// So our commit must stay the same and we restart counting for quorum.
	n.processAppendEntryResponse(aeHeartbeat2Response)
	require_Equal(t, n.commit, 0)
	require_Len(t, len(n.acks), 1)
}

func TestNRGSignalLeadChangeFalseIfCampaignImmediately(t *testing.T) {
	tests := []struct {
		title      string
		switchNode func(n *raft)
	}{
		{
			title: "Follower",
		},
		{
			title: "Candidate",
			switchNode: func(n *raft) {
				n.switchToCandidate()
			},
		},
		{
			title: "Leader",
			switchNode: func(n *raft) {
				n.switchToCandidate()
				n.switchToLeader()
				select {
				case <-n.LeadChangeC():
					t.Error("Expected no leadChange signal")
				default:
					// Expecting no signal yet.
				}
			},
		},
	}
	for _, test := range tests {
		t.Run(test.title, func(t *testing.T) {
			n, cleanup := initSingleMemRaftNode(t)
			defer cleanup()

			// Create a sample entry, the content doesn't matter, just that it's stored.
			esm := encodeStreamMsgAllowCompress("foo", "_INBOX.foo", nil, nil, 0, 0, true)
			entries := []*Entry{newEntry(EntryNormal, esm)}

			nats0 := "S1Nunr6R" // "nats-0"
			aeMsg1 := encode(t, &appendEntry{leader: nats0, term: 1, commit: 0, pterm: 0, pindex: 0, entries: entries})

			// Campaigning immediately signals we're the preferred leader.
			require_NoError(t, n.CampaignImmediately())
			if test.switchNode != nil {
				test.switchNode(n)
			}

			n.processAppendEntry(aeMsg1, n.aesub)

			select {
			case lc := <-n.LeadChangeC():
				require_False(t, lc.isLeader)
			default:
				t.Error("Expected leadChange signal")
			}
			require_Equal(t, n.State(), Follower)
			require_Equal(t, n.leader, nats0)
			require_Equal(t, n.term, 1)
		})
	}
}

func TestNRGCatchupDontCountTowardQuorum(t *testing.T) {
	n, cleanup := initSingleMemRaftNode(t)
	defer cleanup()

	// Create a sample entry, the content doesn't matter, just that it's stored.
	esm := encodeStreamMsgAllowCompress("foo", "_INBOX.foo", nil, nil, 0, 0, true)
	entries := []*Entry{newEntry(EntryNormal, esm)}

	nats0 := "S1Nunr6R" // "nats-0"

	aeReply := "$TEST"
	nc, err := nats.Connect(n.s.ClientURL(), nats.UserInfo("admin", "s3cr3t!"))
	require_NoError(t, err)
	defer nc.Close()

	sub, err := nc.SubscribeSync(aeReply)
	require_NoError(t, err)
	defer sub.Drain()
	require_NoError(t, nc.Flush())

	// Timeline
	aeMissedMsg := encode(t, &appendEntry{leader: nats0, term: 1, commit: 0, pterm: 0, pindex: 0, entries: entries, reply: aeReply})
	ae := appendEntry{leader: nats0, term: 1, commit: 0, pterm: 1, pindex: 1, entries: entries, reply: aeReply}
	aeCatchupTrigger := encode(t, &ae)
	aeHeartbeat := encode(t, &appendEntry{leader: nats0, term: 1, commit: 0, pterm: 1, pindex: 2, entries: nil, reply: aeReply})

	// Simulate we missed all messages up to this point.
	n.processAppendEntry(aeCatchupTrigger, n.aesub)
	require_True(t, n.catchup != nil)
	require_Equal(t, n.catchup.pterm, 0)  // n.pterm
	require_Equal(t, n.catchup.pindex, 0) // n.pindex
	require_Equal(t, n.catchup.cterm, ae.pterm)
	require_Equal(t, n.catchup.cindex, ae.pindex)

	// Should reply we require catchup.
	msg, err := sub.NextMsg(time.Second)
	require_NoError(t, err)
	ar := decodeAppendEntryResponse(msg.Data)
	require_Equal(t, ar.index, 0)
	require_False(t, ar.success)
	require_True(t, strings.HasPrefix(msg.Reply, "$NRG.CR"))

	// Should NEVER respond to catchup messages.
	n.processAppendEntry(aeMissedMsg, n.catchup.sub)
	_, err = sub.NextMsg(time.Second)
	require_Error(t, err, nats.ErrTimeout)

	n.processAppendEntry(aeCatchupTrigger, n.catchup.sub)
	_, err = sub.NextMsg(time.Second)
	require_Error(t, err, nats.ErrTimeout)

	// Now we've received all messages, stop catchup, and respond success to new message.
	n.processAppendEntry(aeHeartbeat, n.aesub)
	msg, err = sub.NextMsg(time.Second)
	require_NoError(t, err)
	ar = decodeAppendEntryResponse(msg.Data)
	require_Equal(t, ar.index, aeHeartbeat.pindex)
	require_True(t, ar.success)
	require_Equal(t, msg.Reply, _EMPTY_)
}

func TestNRGCatchupAcksProgressWhenGivenProgressInbox(t *testing.T) {
	n, cleanup := initSingleMemRaftNode(t)
	defer cleanup()

	// Create a sample entry, the content doesn't matter, just that it's stored.
	esm := encodeStreamMsgAllowCompress("foo", "_INBOX.foo", nil, nil, 0, 0, true)
	entries := []*Entry{newEntry(EntryNormal, esm)}

	nats0 := "S1Nunr6R" // "nats-0"

	aeReply := "$TEST"
	progressReply := fmt.Sprintf(raftCatchupProgressReply, "test")

	nc, err := nats.Connect(n.s.ClientURL(), nats.UserInfo("admin", "s3cr3t!"))
	require_NoError(t, err)
	defer nc.Close()

	sub, err := nc.SubscribeSync(aeReply)
	require_NoError(t, err)
	defer sub.Drain()
	psub, err := nc.SubscribeSync(progressReply)
	require_NoError(t, err)
	defer psub.Drain()
	require_NoError(t, nc.Flush())

	// Timeline
	aeMissedMsg := encode(t, &appendEntry{leader: nats0, term: 1, commit: 0, pterm: 0, pindex: 0, entries: entries, reply: progressReply})
	ae := appendEntry{leader: nats0, term: 1, commit: 0, pterm: 1, pindex: 1, entries: entries, reply: aeReply}
	aeCatchupTrigger := encode(t, &ae)

	// Simulate we missed all messages up to this point.
	n.processAppendEntry(aeCatchupTrigger, n.aesub)
	require_True(t, n.catchup != nil)

	// Should reply we require catchup.
	msg, err := sub.NextMsg(time.Second)
	require_NoError(t, err)
	ar := decodeAppendEntryResponse(msg.Data)
	require_False(t, ar.success)
	require_True(t, strings.HasPrefix(msg.Reply, "$NRG.CR"))

	// A catchup entry that carries a dedicated progress inbox MUST be acked, that
	// response is what lets the leader move its send window forward. Without it the
	// leader can only ever have one window in flight before it gives up and stalls.
	n.processAppendEntry(aeMissedMsg, n.catchup.sub)
	msg, err = psub.NextMsg(time.Second)
	require_NoError(t, err)
	ar = decodeAppendEntryResponse(msg.Data)
	require_Equal(t, ar.index, 1)
	require_True(t, ar.success)

	// It must NOT also show up on the general append entry inbox, since that is the
	// path that feeds quorum.
	_, err = sub.NextMsg(250 * time.Millisecond)
	require_Error(t, err, nats.ErrTimeout)
}

func TestNRGCatchupSendsProgressInboxAndRefillsWindow(t *testing.T) {
	n, cleanup := initSingleMemRaftNode(t)
	defer cleanup()

	nats0 := "S1Nunr6R" // "nats-0"
	nats1 := "yrzKKRBu" // "nats-1"

	n.Lock()
	n.addPeer(nats1)
	n.Unlock()

	// Entries large enough that only a handful fit in the leader's 2MB send window,
	// so catching this log up must span several windows.
	const (
		entrySize  = 512 * 1024
		numEntries = 12
	)
	entries := []*Entry{newEntry(EntryNormal, make([]byte, entrySize))}

	// Fill our WAL, so we have something to catch a follower up on.
	for i := range numEntries {
		pterm := uint64(1)
		if i == 0 {
			pterm = 0
		}
		ae := encode(t, &appendEntry{leader: nats0, term: 1, commit: 0, pterm: pterm, pindex: uint64(i), entries: entries})
		n.processAppendEntry(ae, n.aesub)
	}
	require_Equal(t, n.pindex, numEntries)

	catchupSubj := "$TEST.catchup"
	nc, err := nats.Connect(n.s.ClientURL(), nats.UserInfo("admin", "s3cr3t!"))
	require_NoError(t, err)
	defer nc.Close()

	sub, err := nc.SubscribeSync(catchupSubj)
	require_NoError(t, err)
	defer sub.Drain()
	sub.SetPendingLimits(-1, -1)
	require_NoError(t, nc.Flush())

	n.switchToLeader()

	// Pretend we haven't heard from the follower in a while. It's the only other peer,
	// so as far as we're concerned right now we've lost quorum.
	n.Lock()
	n.lsut = time.Time{}
	n.peers[nats1].ts = time.Now().Add(-2 * lostQuorumInterval)
	n.Unlock()
	require_True(t, n.lostQuorum())

	// Ask to be caught up from the very beginning.
	ar := newAppendEntryResponse(0, 0, nats1, false)
	ar.reply = catchupSubj
	n.catchupFollower(ar)

	// Act as the follower. Every catchup entry is acknowledged on the reply subject
	// the leader handed us, which is what refills its send window.
	var received int
	for received < numEntries {
		msg, err := sub.NextMsg(time.Second)
		if err != nil {
			t.Fatalf("Only received %d of %d catchup entries before stalling: %v", received, numEntries, err)
		}
		// The leader must point catchup responses at a dedicated progress inbox, never
		// at its general append entry inbox, so they can't be counted towards quorum.
		require_True(t, strings.HasPrefix(msg.Reply, raftCatchupProgressPre))

		cae, err := decodeAppendEntry(msg.Data, nil, msg.Reply)
		require_NoError(t, err)
		received++

		par := newAppendEntryResponse(cae.term, cae.pindex+1, nats1, true)
		require_NoError(t, nc.Publish(msg.Reply, par.encode(nil)))
		require_NoError(t, nc.Flush())
		arPool.Put(par)

		// These acks don't go through processAppendEntryResponse, but they must still
		// count as having heard from the peer. Otherwise, a catchup outlasting
		// lostQuorumInterval makes us step down, while the follower is responding to us.
		checkFor(t, time.Second, 10*time.Millisecond, func() error {
			if n.lostQuorum() {
				return errors.New("should not have lost quorum, peer is acking catchup progress")
			}
			return nil
		})
	}
}

func TestNRGCatchupLargeEntriesProgressesAcrossWindows(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "R3S", 3)
	defer c.shutdown()

	rg := c.createMemRaftGroup("TEST", 3, newStateAdder)
	rg.waitOnLeader()

	// The leader keeps at most maxOutstanding (2MB) of catchup entries in flight, so
	// entries this large mean a catchup spans many send windows. A window can only be
	// refilled once the follower reports progress on what it already received.
	const (
		entrySize  = 512 * 1024
		numEntries = 64
	)

	// Take a follower down so it misses everything that follows.
	follower := rg.nonLeader().(*stateAdder)
	follower.stop()

	leader := rg.leader().(*stateAdder)
	for range numEntries {
		data := make([]byte, entrySize)
		binary.PutVarint(data, 1)
		leader.propose(data)
	}
	rg.waitOnTotal(t, numEntries)

	// Bring it back with an empty log, it must now be caught up from scratch.
	follower.restart()

	// Without working catchup flow control the leader pushes one 2MB window and then
	// stalls out for a couple of seconds before the follower re-requests, which for a
	// log this size takes the better part of a minute.
	checkFor(t, 2*time.Second, 100*time.Millisecond, func() error {
		var err error
		for _, sm := range rg {
			asm := sm.(*stateAdder)
			if total := asm.total(); total != numEntries {
				err = errors.Join(err, fmt.Errorf("Adder on %v has wrong total: %d vs %d",
					asm.server(), total, numEntries))
			}
		}
		return err
	})
}

func TestNRGIgnoreTrackResponseWhenNotLeader(t *testing.T) {
	n, cleanup := initSingleMemRaftNode(t)
	defer cleanup()

	nats1 := "yrzKKRBu" // "nats-1"
	n.Lock()
	n.addPeer(nats1)
	// Keep quorum at 2 so the initial leader NOOP does not auto-commit.
	n.lsut = time.Time{}
	n.Unlock()

	// Switch this node to leader which sends an entry.
	n.term++
	n.switchToLeader()
	require_Equal(t, n.term, 1)
	require_Equal(t, n.pindex, 1)
	require_Equal(t, n.pterm, 1)
	require_Equal(t, n.commit, 0)

	// Step down
	n.stepdown(noLeader)
	require_Equal(t, n.pindex, 1)
	require_Equal(t, n.pterm, 1)
	require_Equal(t, n.commit, 0)

	// Normally would commit the entry, but since we're not leader anymore we should ignore it.
	n.trackResponse(&appendEntryResponse{term: 1, index: 1, peer: "peer", success: true})
	require_Equal(t, n.commit, 0)
}

func TestNRGRejectNewAppendEntryFromPreviousLeader(t *testing.T) {
	n, cleanup := initSingleMemRaftNode(t)
	defer cleanup()

	// Create a sample entry, the content doesn't matter, just that it's stored.
	esm := encodeStreamMsgAllowCompress("foo", "_INBOX.foo", nil, nil, 0, 0, true)
	entries := []*Entry{newEntry(EntryNormal, esm)}

	nats0 := "S1Nunr6R" // "nats-0"

	// Timeline
	aeMsg1 := encode(t, &appendEntry{leader: nats0, term: 1, commit: 0, pterm: 0, pindex: 0, entries: entries})
	aeMsg2 := encode(t, &appendEntry{leader: nats0, term: 1, commit: 1, pterm: 1, pindex: 1, entries: entries})

	// Accept first message because it equals our term.
	n.term = 1
	n.processAppendEntry(aeMsg1, n.aesub)
	require_Equal(t, n.pterm, 1)
	require_Equal(t, n.pindex, 1)

	// We are part of the successful vote for a new leader under a new term.
	require_NoError(t, n.processVoteRequest(&voteRequest{term: 5, lastTerm: 1, lastIndex: 2}))

	// Must reject entry from a previous term.
	n.processAppendEntry(aeMsg2, n.aesub)
	require_Equal(t, n.pterm, 1)
	require_Equal(t, n.pindex, 1)
}

func TestNRGRejectAppendEntryDuringCatchupFromPreviousLeader(t *testing.T) {
	test := func(t *testing.T, isCatchingUp, oldBehavior bool) {
		n, cleanup := initSingleMemRaftNode(t)
		defer cleanup()

		// Create a sample entry, the content doesn't matter, just that it's stored.
		esm := encodeStreamMsgAllowCompress("foo", "_INBOX.foo", nil, nil, 0, 0, true)
		entries := []*Entry{newEntry(EntryNormal, esm)}

		nats0 := "S1Nunr6R" // "nats-0"

		// Timeline
		aeMsg1 := encode(t, &appendEntry{leader: nats0, term: 1, commit: 0, pterm: 0, pindex: 0, entries: entries})
		aeMsg2 := encode(t, &appendEntry{leader: nats0, term: 1, commit: 1, pterm: 1, pindex: 1, entries: entries})

		// Accept first message because it equals our term.
		n.term = 1
		n.processAppendEntry(aeMsg2, n.aesub)
		require_True(t, n.catchup != nil)
		require_Equal(t, n.catchup.pterm, 0)  // n.pterm
		require_Equal(t, n.catchup.pindex, 0) // n.pindex
		require_Equal(t, n.catchup.cterm, aeMsg2.pterm)
		require_Equal(t, n.catchup.cindex, aeMsg2.pindex)

		// Under the new behavior the term of the leader that's doing the catchup is included.
		if !oldBehavior {
			aeMsg1.lterm = 1
			aeMsg2.lterm = 1
		}

		// First catchup message is accepted.
		catchup := n.catchup
		n.processAppendEntry(aeMsg1, catchup.sub)
		require_Equal(t, n.pterm, 1)
		require_Equal(t, n.pindex, 1)

		// We are part of the successful vote for a new leader under a new term.
		require_NoError(t, n.processVoteRequest(&voteRequest{term: 5, lastTerm: 1, lastIndex: 2}))

		// Voting cancels catchup. For testing, revert that so we can test
		// what a catchup message after upping the term does.
		nsub := n.aesub
		if isCatchingUp {
			n.catchup = catchup
			nsub = catchup.sub
		}

		// Now send the second catchup entry.
		n.processAppendEntry(aeMsg2, nsub)
		require_Equal(t, n.pterm, 1)

		// Under the old behavior this entry is wrongly accepted.
		// A new server will also know to be backward-compatible if being caught up by an old server.
		if isCatchingUp && oldBehavior {
			require_Equal(t, n.pindex, 2)
		} else {
			// Under the new behavior the entry is correctly accepted.
			require_Equal(t, n.pindex, 1)
		}
	}

	for _, isCatchingUp := range []bool{false, true} {
		for _, oldBehavior := range []bool{false, true} {
			title := "new-entry"
			if isCatchingUp {
				title = "catchup"
			}
			if oldBehavior {
				title += "-backward-compatible"
			}
			t.Run(title, func(t *testing.T) { test(t, isCatchingUp, oldBehavior) })
		}
	}
}

func TestNRGDontRejectAppendEntryFromReplay(t *testing.T) {
	test := func(t *testing.T, restart bool) {
		c := createJetStreamClusterExplicit(t, "R3S", 3)
		defer c.shutdown()

		rg := c.createRaftGroup("TEST", 3, newStateAdder)
		rg.waitOnLeader()

		// Stop all servers except the leader.
		l := rg.leader().(*stateAdder)
		rs := rg.nonLeader()
		for _, sm := range rg {
			if sm != l {
				sm.stop()
			}
		}

		// Propose a new entry to the leader and confirm it's stored in its log.
		pindex, _, _ := l.node().Progress()
		l.proposeDelta(10)
		checkFor(t, 2*time.Second, 200*time.Millisecond, func() error {
			if index, _, _ := l.node().Progress(); index == pindex {
				return errors.New("proposal not stored yet")
			}
			return nil
		})

		// Optionally restart the leader, which will require replay of the entries in the log.
		if restart {
			l.stop()
			l.restart()
		}

		// Shutdown server should be caught up.
		rs.restart()

		// Wait for both online servers to have applied the delta.
		checkFor(t, 5*time.Second, 200*time.Millisecond, func() (err error) {
			for _, sm := range rg {
				if sm != l && sm != rs {
					break
				}
				if total := sm.(*stateAdder).total(); total != 10 {
					err = errors.Join(err, fmt.Errorf("expected 10 total, got: %d", total))
				}
			}
			return err
		})
	}

	t.Run("no-restart", func(t *testing.T) { test(t, false) })
	t.Run("with-restart", func(t *testing.T) { test(t, true) })
}

func TestNRGSimpleCatchup(t *testing.T) {
	test := func(t *testing.T, leaderChange bool) {
		c := createJetStreamClusterExplicit(t, "R3S", 3)
		defer c.shutdown()

		rg := c.createRaftGroup("TEST", 3, newStateAdder)
		rg.waitOnLeader()

		// Shutdown a single group, we'll start it later for catchup.
		nl := rg.nonLeader()
		nl.stop()

		l := rg.leader().(*stateAdder)
		l.proposeDelta(10)

		// Wait for remaining servers to have applied the delta.
		checkFor(t, 2*time.Second, 200*time.Millisecond, func() (err error) {
			for _, sm := range rg {
				total := sm.(*stateAdder).total()
				if sm == nl && total != 0 {
					err = errors.Join(err, errors.New("expected shutdown server to not get data"))
				} else if sm != nl && total != 10 {
					err = errors.Join(err, fmt.Errorf("expected 10 total, got: %d", total))
				}
			}
			return err
		})

		if leaderChange {
			require_NoError(t, l.node().StepDown())
		}

		// Shutdown server should be caught up.
		nl.restart()
		rg.waitOnTotal(t, 10)
	}

	t.Run("same-leader", func(t *testing.T) { test(t, false) })
	t.Run("change-leader", func(t *testing.T) { test(t, true) })
}

func TestNRGSnapshotCatchup(t *testing.T) {
	test := func(t *testing.T, restart bool) {
		c := createJetStreamClusterExplicit(t, "R3S", 3)
		defer c.shutdown()

		rg := c.createRaftGroup("TEST", 3, newStateAdder)
		rg.waitOnLeader()

		l := rg.leader().(*stateAdder)
		var s1 stateMachine
		var s2 stateMachine
		for _, sm := range rg {
			if sm == l {
				continue
			}
			if s1 == nil {
				s1 = sm
			} else {
				s2 = sm
			}
		}

		// Stop one non-leader server.
		s1.stop()

		// Wait for both online servers to have applied the delta.
		l.proposeDelta(10)
		checkFor(t, 5*time.Second, 200*time.Millisecond, func() (err error) {
			for _, sm := range rg {
				if sm == s1 {
					continue
				}
				if total := sm.(*stateAdder).total(); total != 10 {
					err = errors.Join(err, fmt.Errorf("expected 10 total, got: %d", total))
				}
			}
			return err
		})

		// Shutdown last non-leader server.
		s2.stop()

		// Snapshot so outdated server needs to catchup based on it.
		l.snapshot(t)

		// Propose a new entry to the leader and confirm it's stored in its log.
		pindex, _, _ := l.node().Progress()
		l.proposeDelta(10)
		checkFor(t, 2*time.Second, 200*time.Millisecond, func() error {
			if index, _, _ := l.node().Progress(); index == pindex {
				return errors.New("proposal not stored yet")
			}
			return nil
		})

		// Optionally restart the leader, which will require replay of the entries in the log.
		if restart {
			l.stop()
			l.restart()
		}

		// Shutdown server should be caught up.
		s1.restart()

		// Wait for both online servers to have applied the delta.
		checkFor(t, 5*time.Second, 200*time.Millisecond, func() (err error) {
			for _, sm := range rg {
				if sm == s2 {
					break
				}
				if total := sm.(*stateAdder).total(); total != 20 {
					err = errors.Join(err, fmt.Errorf("expected 20 total, got: %d", total))
				}
			}
			return err
		})
	}

	t.Run("no-restart", func(t *testing.T) { test(t, false) })
	t.Run("with-restart", func(t *testing.T) { test(t, true) })
}

func TestNRGSnapshotRecovery(t *testing.T) {
	n, cleanup := initSingleMemRaftNode(t)
	defer cleanup()

	// Create a sample entry, the content doesn't matter, just that it's stored.
	esm := encodeStreamMsgAllowCompress("foo", "_INBOX.foo", nil, nil, 0, 0, true)
	entries := []*Entry{newEntry(EntryNormal, esm)}

	nats0 := "S1Nunr6R" // "nats-0"

	// Timeline
	aeMsg := encode(t, &appendEntry{leader: nats0, term: 1, commit: 1, pterm: 0, pindex: 0, entries: entries})

	// Store one entry.
	n.processAppendEntry(aeMsg, n.aesub)
	require_Equal(t, n.pindex, 1)
	require_Equal(t, n.commit, 1)
	require_Equal(t, n.applied, 0)

	// Apply it.
	n.Applied(1)
	require_Equal(t, n.applied, 1)

	// Install the snapshot.
	require_NoError(t, n.InstallSnapshot(nil, false))

	// Restoring the snapshot should not up applied, because the apply queue is async.
	n.pindex, n.commit, n.processed, n.applied = 0, 0, 0, 0
	n.setupLastSnapshot()
	require_Equal(t, n.pindex, 1)
	require_Equal(t, n.commit, 1)
	require_Equal(t, n.processed, 0)
	require_Equal(t, n.applied, 0)
}

func TestNRGLoadLastSnapshotCleansLegacyZeroIndexSnapshot(t *testing.T) {
	n, cleanup := initSingleMemRaftNode(t)
	defer cleanup()

	snapDir := filepath.Join(n.sd, snapshotsDir)
	require_NoError(t, os.MkdirAll(snapDir, defaultDirPerms))

	legacy := &snapshot{
		lastTerm:  1,
		lastIndex: 0,
		peerstate: encodePeerState(&peerState{n.peerNames(), n.csz, n.extSt}),
		data:      []byte("legacy"),
	}
	sfile := filepath.Join(snapDir, fmt.Sprintf(snapFileT, legacy.lastTerm, legacy.lastIndex))
	require_NoError(t, writeFileWithSync(n.dios, sfile, n.encodeSnapshot(legacy), defaultFilePerms))

	n.Lock()
	n.snapfile = sfile
	snap, err := n.loadLastSnapshot()
	snapfile := n.snapfile
	n.Unlock()

	require_Error(t, err, errNoSnapAvailable)
	require_True(t, snap == nil)
	require_Equal(t, snapfile, _EMPTY_)
	_, err = os.Stat(sfile)
	require_True(t, os.IsNotExist(err))
}

func TestNRGSetupLastSnapshotIgnoresTmpFile(t *testing.T) {
	n, cleanup := initSingleMemRaftNode(t)
	defer cleanup()

	// Make a snapshot file
	snapDir := filepath.Join(n.sd, snapshotsDir)
	require_NoError(t, os.MkdirAll(snapDir, defaultDirPerms))

	snap := &snapshot{
		lastTerm:  1,
		lastIndex: 1,
		peerstate: encodePeerState(n.currentPeerState()),
		data:      []byte("snapshot"),
	}

	sfile := filepath.Join(snapDir, fmt.Sprintf(snapFileT, snap.lastTerm, snap.lastIndex))
	require_NoError(t, os.WriteFile(sfile, n.encodeSnapshot(snap), defaultFilePerms))

	// Make a empty snapshot file with a higher index, and extension
	// .tmp to simulate the intermediate step in writeAtomically.
	tmpSnap := filepath.Join(snapDir, fmt.Sprintf(snapFileT, uint64(1), uint64(2))+".tmp")
	require_NoError(t, os.WriteFile(tmpSnap, nil, defaultFilePerms))

	// If bug is present: setupLastSnapshot() selects
	// the higher numbered snapshot file, even if it
	// has the .tmp extension. The presence of a .tmp
	// snapshot file means the server crashed, preventing
	// writeAtomically() to finish. The contents of a
	// .tmp snapshot file may be incomplete.
	// Verify that setupLastSnapshot selects the "good"
	// snapshot.
	err := n.setupLastSnapshot()
	require_NoError(t, err)
	require_Equal(t, n.snapfile, sfile)
}

func TestNRGKeepRunningOnServerShutdown(t *testing.T) {
	n, cleanup := initSingleMemRaftNode(t)
	defer cleanup()

	n.RLock()
	s := n.s
	wal := n.wal.(*memStore)
	n.RUnlock()

	n.wg.Add(1)
	s.startGoRoutine(n.run, nil)

	s.running.Store(false)
	time.Sleep(time.Second)

	wal.mu.RLock()
	msgs := wal.msgs
	wal.mu.RUnlock()
	require_NotNil(t, msgs)

	n.Stop()
	n.WaitForStop()

	wal.mu.RLock()
	msgs = wal.msgs
	wal.mu.RUnlock()
	require_True(t, msgs == nil)
}

func TestNRGVoteResponseEncoding(t *testing.T) {
	vr := &voteResponse{term: 1, peer: "S1Nunr6R"}
	vr.granted, vr.empty = false, false
	res := vr.encode()
	require_Equal(t, res[16], 0)
	require_True(t, reflect.DeepEqual(decodeVoteResponse(res), vr))

	vr.granted, vr.empty = true, false
	res = vr.encode()
	require_Equal(t, res[16], 1)
	require_True(t, reflect.DeepEqual(decodeVoteResponse(res), vr))

	vr.granted, vr.empty = false, true
	res = vr.encode()
	require_Equal(t, res[16], 2)
	require_True(t, reflect.DeepEqual(decodeVoteResponse(res), vr))

	vr.granted, vr.empty = true, true
	res = vr.encode()
	require_Equal(t, res[16], 3)
	require_True(t, reflect.DeepEqual(decodeVoteResponse(res), vr))
}

func TestNRGDoesntRequestVoteOnWriteError(t *testing.T) {
	n, cleanup := initSingleMemRaftNode(t)
	defer cleanup()

	// Snoop on the vote request subject so we can tell whether we campaign.
	nc, err := nats.Connect(n.s.ClientURL(), nats.UserInfo("admin", "s3cr3t!"))
	require_NoError(t, err)
	defer nc.Close()

	sub, err := nc.SubscribeSync(n.vsubj)
	require_NoError(t, err)
	defer sub.Unsubscribe()
	require_NoError(t, nc.Flush())

	// Specifically not using setWriteError here because that shuts down the node
	// and causes problems with the defer cleanup.
	n.werr = errors.New("test write error")
	n.state.Store(int32(Candidate))

	// We can't persist our own vote, so asking for votes must be a no-op,
	// otherwise after a restart we could vote for someone else in this term.
	n.requestVote()

	// Nothing should have been published to the vote subject.
	_, err = sub.NextMsg(250 * time.Millisecond)
	require_Error(t, err, nats.ErrTimeout)
}

func TestNRGInitializeAndScaleUp(t *testing.T) {
	n, cleanup := initSingleMemRaftNode(t)
	defer cleanup()

	// Create a sample entry, the content doesn't matter, just that it's stored.
	esm := encodeStreamMsgAllowCompress("foo", "_INBOX.foo", nil, nil, 0, 0, true)
	entries := []*Entry{newEntry(EntryNormal, esm)}

	nats0 := "S1Nunr6R" // "nats-0"

	// Timeline
	aeMsg := encode(t, &appendEntry{leader: nats0, term: 1, commit: 1, pterm: 0, pindex: 0, entries: entries})

	require_True(t, n.initializing)
	n.scaleUp = true
	n.SetObserver(true)
	require_Equal(t, n.term, 0)

	voteReply := "$TEST"
	nc, err := nats.Connect(n.s.ClientURL(), nats.UserInfo("admin", "s3cr3t!"))
	require_NoError(t, err)
	defer nc.Close()

	sub, err := nc.SubscribeSync(voteReply)
	require_NoError(t, err)
	defer sub.Drain()
	require_NoError(t, nc.Flush())

	// Votes on a new leader, and resets notion of "empty vote" to ease getting quorum.
	require_NoError(t, n.processVoteRequest(&voteRequest{term: 1, candidate: nats0, reply: voteReply}))
	require_Equal(t, n.term, 1)
	require_Equal(t, n.vote, nats0)
	require_True(t, n.initializing)
	require_True(t, n.scaleUp)
	require_True(t, n.observer)

	msg, err := sub.NextMsg(time.Second)
	require_NoError(t, err)
	vr := decodeVoteResponse(msg.Data)
	require_True(t, vr.granted)
	require_False(t, vr.empty)

	// Processing an append entry resets scale up and puts us out of observer mode.
	n.processAppendEntry(aeMsg, n.aesub)
	require_Equal(t, n.pindex, 1)
	require_False(t, n.initializing)
	require_False(t, n.scaleUp)
	require_False(t, n.observer)

	// Simulate a reset.
	n.resetWAL()
	require_Equal(t, n.pindex, 0)

	// Vote after a WAL reset must be an "empty vote".
	require_NoError(t, n.processVoteRequest(&voteRequest{term: 2, candidate: "_random_", reply: voteReply}))
	require_Equal(t, n.term, 2)
	require_Equal(t, n.vote, "_random_")

	// Vote is still granted, but now marked as "empty".
	msg, err = sub.NextMsg(time.Second)
	require_NoError(t, err)
	vr = decodeVoteResponse(msg.Data)
	require_True(t, vr.granted)
	require_True(t, vr.empty)

	// Reset to check if snapshot on catchup can also reset this.
	n.initializing = true
	n.scaleUp = true
	n.SetObserver(true)
	snapshotEntries := []*Entry{
		newEntry(EntrySnapshot, nil),
		newEntry(EntryPeerState, encodePeerState(&peerState{n.peerNames(), n.csz, n.extSt})),
	}
	aeSnapshot := encode(t, &appendEntry{leader: nats0, term: 2, commit: 1, pterm: 1, pindex: 1, entries: snapshotEntries})
	n.createCatchup(aeSnapshot)
	n.processAppendEntry(aeSnapshot, n.catchup.sub)
	require_False(t, n.initializing)
	require_False(t, n.scaleUp)
	require_False(t, n.observer)

	// Simulate a reset.
	n.resetWAL()
	require_Equal(t, n.pindex, 0)

	// Vote when initializing but not scaling up, must also NOT be an "empty vote".
	n.initializing = true
	require_NoError(t, n.processVoteRequest(&voteRequest{term: 3, candidate: "_random_", reply: voteReply}))
	require_Equal(t, n.term, 3)
	require_Equal(t, n.vote, "_random_")

	// Vote is still granted, but now marked as "empty".
	msg, err = sub.NextMsg(time.Second)
	require_NoError(t, err)
	vr = decodeVoteResponse(msg.Data)
	require_True(t, vr.granted)
	require_False(t, vr.empty)
}

func TestNRGReplayOnSnapshotSameTerm(t *testing.T) {
	n, cleanup := initSingleMemRaftNode(t)
	defer cleanup()

	// Create a sample entry, the content doesn't matter, just that it's stored.
	esm := encodeStreamMsgAllowCompress("foo", "_INBOX.foo", nil, nil, 0, 0, true)
	entries := []*Entry{newEntry(EntryNormal, esm)}

	nats0 := "S1Nunr6R" // "nats-0"

	// Timeline
	aeMsg1 := encode(t, &appendEntry{leader: nats0, term: 1, commit: 1, pterm: 0, pindex: 0, entries: entries})
	aeMsg2 := encode(t, &appendEntry{leader: nats0, term: 1, commit: 1, pterm: 1, pindex: 1, entries: entries})
	aeMsg3 := encode(t, &appendEntry{leader: nats0, term: 1, commit: 1, pterm: 1, pindex: 2, entries: entries})

	// Process the first append entry.
	n.processAppendEntry(aeMsg1, n.aesub)
	require_Equal(t, n.pindex, 1)

	// Commit and apply.
	require_NoError(t, n.applyCommit(1))
	require_Equal(t, n.commit, 1)
	n.Applied(1)
	require_Equal(t, n.applied, 1)

	// Install snapshot.
	require_NoError(t, n.InstallSnapshot(nil, false))
	snap, err := n.loadLastSnapshot()
	require_NoError(t, err)
	require_Equal(t, snap.lastIndex, 1)

	// Process other messages.
	n.processAppendEntry(aeMsg2, n.aesub)
	require_Equal(t, n.pindex, 2)
	n.processAppendEntry(aeMsg3, n.aesub)
	require_Equal(t, n.pindex, 3)

	// Replay the append entry that matches our snapshot.
	// This can happen as a repeated entry, or a delayed append entry after having already received it in a catchup.
	// Should be recognized as a replay with the same term, marked as success, and not truncate.
	n.processAppendEntry(aeMsg2, n.aesub)
	require_Equal(t, n.pindex, 3)
}

func TestNRGReplayOnSnapshotDifferentTerm(t *testing.T) {
	n, cleanup := initSingleMemRaftNode(t)
	defer cleanup()

	// Create a sample entry, the content doesn't matter, just that it's stored.
	esm := encodeStreamMsgAllowCompress("foo", "_INBOX.foo", nil, nil, 0, 0, true)
	entries := []*Entry{newEntry(EntryNormal, esm)}

	nats0 := "S1Nunr6R" // "nats-0"

	// Timeline
	aeMsg1 := encode(t, &appendEntry{leader: nats0, term: 1, commit: 1, pterm: 0, pindex: 0, entries: entries, lterm: 2})
	aeMsg2 := encode(t, &appendEntry{leader: nats0, term: 2, commit: 1, pterm: 1, pindex: 1, entries: entries, lterm: 2})
	aeMsg3 := encode(t, &appendEntry{leader: nats0, term: 2, commit: 1, pterm: 2, pindex: 2, entries: entries, lterm: 2})

	// Process the first append entry.
	n.processAppendEntry(aeMsg1, n.aesub)
	require_Equal(t, n.pindex, 1)

	// Commit and apply.
	require_NoError(t, n.applyCommit(1))
	require_Equal(t, n.commit, 1)
	n.Applied(1)
	require_Equal(t, n.applied, 1)

	// Install snapshot.
	require_NoError(t, n.InstallSnapshot(nil, false))
	snap, err := n.loadLastSnapshot()
	require_NoError(t, err)
	require_Equal(t, snap.lastIndex, 1)

	// Reset applied to simulate having received the snapshot from
	// another leader, and we didn't apply yet since it's async.
	n.applied = 0

	// Process other messages.
	n.processAppendEntry(aeMsg2, n.aesub)
	require_Equal(t, n.pindex, 2)
	n.processAppendEntry(aeMsg3, n.aesub)
	require_Equal(t, n.pindex, 3)

	// Replay the append entry that matches our snapshot.
	// This can happen as a repeated entry, or a delayed append entry after having already received it in a catchup.
	// Should be recognized as truncating back to the installed snapshot, not reset the WAL fully.
	// Since all is aligned after truncation, should also be able to apply the entry.
	n.processAppendEntry(aeMsg2, n.aesub)
	require_Equal(t, n.pindex, 2)

	// Should now also be able to apply the third entry.
	n.processAppendEntry(aeMsg3, n.aesub)
	require_Equal(t, n.pindex, 3)
}

func TestNRGSizeAndApplied(t *testing.T) {
	n, cleanup := initSingleMemRaftNode(t)
	defer cleanup()

	// Create a sample entry, the content doesn't matter, just that it's stored.
	esm := encodeStreamMsgAllowCompress("foo", "_INBOX.foo", nil, nil, 0, 0, true)

	nats0 := "S1Nunr6R" // "nats-0"
	aeMsg1 := encode(t, &appendEntry{leader: nats0, term: 1, commit: 0, pterm: 0, pindex: 0, entries: []*Entry{newEntry(EntryNormal, esm)}})
	aeMsg2 := encode(t, &appendEntry{leader: nats0, term: 1, commit: 0, pterm: 1, pindex: 1, entries: []*Entry{newEntry(EntryNormal, esm)}})

	var (
		entries uint64
		bytes   uint64
	)

	// Initially our WAL is empty.
	entries, bytes = n.Size()
	require_Equal(t, entries, 0)
	require_Equal(t, bytes, 0)

	// Store the first append entry.
	require_NoError(t, n.storeToWAL(aeMsg1))
	entries, bytes = n.Size()
	require_Equal(t, entries, 1)
	require_Equal(t, bytes, 105)

	// Store the second append entry.
	require_NoError(t, n.storeToWAL(aeMsg2))
	entries, bytes = n.Size()
	require_Equal(t, entries, 2)
	require_Equal(t, bytes, 210)

	// Applying should return what part of the WAL can be compacted.
	n.commit = 1
	entries, bytes = n.Applied(1)
	require_Equal(t, entries, 1)
	require_Equal(t, bytes, 105)

	// After applying all should return our whole WAL can be compacted.
	n.commit = 2
	entries, bytes = n.Applied(2)
	require_Equal(t, entries, 2)
	require_Equal(t, bytes, 210)

	// Installing a snapshot should properly correct n.papplied and n.bytes
	n.applied = 1 // Reset just for testing.
	require_NoError(t, n.InstallSnapshot(nil, false))
	require_Equal(t, n.papplied, 1)
	require_Equal(t, n.bytes, 105)
	entries, bytes = n.Size()
	require_Equal(t, entries, 1)
	require_Equal(t, bytes, 105)

	entries, bytes = n.Applied(2)
	require_Equal(t, entries, 1)
	require_Equal(t, bytes, 105)

	require_NoError(t, n.InstallSnapshot(nil, false))
	require_Equal(t, n.papplied, 2)
	require_Equal(t, n.bytes, 0)
	entries, bytes = n.Size()
	require_Equal(t, entries, 0)
	require_Equal(t, bytes, 0)
}

func TestNRGIgnoreEntryAfterCanceledCatchup(t *testing.T) {
	n, cleanup := initSingleMemRaftNode(t)
	defer cleanup()

	// Create a sample entry, the content doesn't matter, just that it's stored.
	esm := encodeStreamMsgAllowCompress("foo", "_INBOX.foo", nil, nil, 0, 0, true)
	entries := []*Entry{newEntry(EntryNormal, esm)}

	nats0 := "S1Nunr6R" // "nats-0"

	aeMsg1 := encode(t, &appendEntry{leader: nats0, term: 1, commit: 0, pterm: 0, pindex: 0, entries: entries})
	aeMsg2 := encode(t, &appendEntry{leader: nats0, term: 1, commit: 0, pterm: 1, pindex: 1, entries: entries})

	n.processAppendEntry(aeMsg2, n.aesub)
	require_True(t, n.catchup != nil)

	csub := n.catchup.sub
	n.cancelCatchup()

	// Catchup was canceled, a message on this canceled catchup should not be stored.
	n.processAppendEntry(aeMsg1, csub)
	require_Equal(t, n.pindex, 0)
}

func TestNRGDelayedMessagesAfterCatchupDontCountTowardQuorum(t *testing.T) {
	n, cleanup := initSingleMemRaftNode(t)
	defer cleanup()

	// Create a sample entry, the content doesn't matter, just that it's stored.
	esm := encodeStreamMsgAllowCompress("foo", "_INBOX.foo", nil, nil, 0, 0, true)
	entries := []*Entry{newEntry(EntryNormal, esm)}

	aeReply := "$TEST"
	nats0 := "S1Nunr6R" // "nats-0"
	aeMsg1 := encode(t, &appendEntry{leader: nats0, term: 1, commit: 0, pterm: 0, pindex: 0, entries: entries, reply: aeReply})
	aeMsg2 := encode(t, &appendEntry{leader: nats0, term: 1, commit: 1, pterm: 1, pindex: 1, entries: entries, reply: aeReply})
	aeMsg3 := encode(t, &appendEntry{leader: nats0, term: 1, commit: 1, pterm: 1, pindex: 2, entries: entries, reply: aeReply})

	// Triggers catchup.
	n.processAppendEntry(aeMsg3, n.aesub)
	require_True(t, n.catchup != nil)

	// Catchup runs partially.
	n.processAppendEntry(aeMsg1, n.catchup.sub)
	n.processAppendEntry(aeMsg2, n.catchup.sub)
	require_Equal(t, n.pindex, 2)
	require_Equal(t, n.commit, 1)
	n.Applied(1)
	require_Equal(t, n.applied, 1)
	require_NoError(t, n.InstallSnapshot(nil, false))

	nc, err := nats.Connect(n.s.ClientURL(), nats.UserInfo("admin", "s3cr3t!"))
	require_NoError(t, err)
	defer nc.Close()

	sub, err := nc.SubscribeSync(aeReply)
	require_NoError(t, err)
	defer sub.Drain()
	require_NoError(t, nc.Flush())

	// We now receive delayed "real-time" messages.
	// The first message needs to be a copy, because we've committed it before and returned it to the pool.
	aeMsg1Copy := encode(t, &appendEntry{leader: nats0, term: 1, commit: 0, pterm: 0, pindex: 0, entries: entries, reply: aeReply})
	n.processAppendEntry(aeMsg1Copy, n.aesub)
	require_Equal(t, n.pindex, 2)
	// Should NOT reply "success", otherwise we would wrongfully provide quorum while not having an up-to-date log.
	_, err = sub.NextMsg(500 * time.Millisecond)
	require_Error(t, err, nats.ErrTimeout)

	n.processAppendEntry(aeMsg2, n.aesub)
	require_Equal(t, n.pindex, 2)
	// Should NOT reply "success", otherwise we would wrongfully provide quorum while not having an up-to-date log.
	_, err = sub.NextMsg(500 * time.Millisecond)
	require_Error(t, err, nats.ErrTimeout)

	n.processAppendEntry(aeMsg3, n.aesub)
	require_Equal(t, n.pindex, 3)
	// Should reply "success", this is the latest message.
	msg, err := sub.NextMsg(500 * time.Millisecond)
	require_NoError(t, err)
	ar := decodeAppendEntryResponse(msg.Data)
	require_Equal(t, ar.index, 3)
	require_True(t, ar.success)
	require_Equal(t, msg.Reply, _EMPTY_)
}

func TestNRGStepdownWithHighestTermDuringCatchup(t *testing.T) {
	n, cleanup := initSingleMemRaftNode(t)
	defer cleanup()

	// Create a sample entry, the content doesn't matter, just that it's stored.
	esm := encodeStreamMsgAllowCompress("foo", "_INBOX.foo", nil, nil, 0, 0, true)
	entries := []*Entry{newEntry(EntryNormal, esm)}

	nats0 := "S1Nunr6R" // "nats-0"
	aeMsg1 := encode(t, &appendEntry{leader: nats0, lterm: 10, term: 1, commit: 0, pterm: 0, pindex: 0, entries: entries})
	aeMsg2 := encode(t, &appendEntry{leader: nats0, lterm: 20, term: 1, commit: 0, pterm: 1, pindex: 1, entries: entries})

	// Need to store the message, stepdown, and up term.
	n.switchToCandidate()
	require_Equal(t, n.term, 1)
	n.processAppendEntry(aeMsg1, n.aesub)
	require_Equal(t, n.term, 10)
	require_Equal(t, n.pindex, 1)

	// Need to store the message, stepdown, and up term.
	n.switchToLeader()
	n.processAppendEntry(aeMsg2, n.aesub)
	require_Equal(t, n.term, 20)
	require_Equal(t, n.pindex, 2)
}

func TestNRGTruncateOnStartup(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "R3S", 3)
	s := c.servers[0] // RunBasicJetStreamServer not available
	defer c.shutdown()

	storeDir := t.TempDir()
	fcfg := FileStoreConfig{StoreDir: storeDir, BlockSize: defaultMediumBlockSize, AsyncFlush: false, srv: s}
	scfg := StreamConfig{Name: "RAFT", Storage: FileStorage}
	fs, err := newFileStore(fcfg, scfg)
	require_NoError(t, err)

	cfg := &RaftConfig{Name: "TEST", Store: storeDir, Log: fs}

	err = s.bootstrapRaftNode(cfg, nil, false)
	require_NoError(t, err)
	n, err := s.initRaftNode(globalAccountName, cfg, pprofLabels{})
	require_NoError(t, err)

	// Create a sample entry, the content doesn't matter, just that it's stored.
	esm := encodeStreamMsgAllowCompress("foo", "_INBOX.foo", nil, nil, 0, 0, true)
	entries := []*Entry{newEntry(EntryNormal, esm)}

	nats0 := "S1Nunr6R" // "nats-0"
	aeMsg1 := encode(t, &appendEntry{leader: nats0, term: 1, commit: 0, pterm: 0, pindex: 0, entries: entries})
	aeMsg2 := encode(t, &appendEntry{leader: nats0, term: 1, commit: 0, pterm: 1, pindex: 1, entries: entries})

	// Store two messages the normal way.
	n.processAppendEntry(aeMsg1, n.aesub)
	n.processAppendEntry(aeMsg2, n.aesub)
	require_Equal(t, n.pindex, 2)

	state := n.wal.State()
	require_Equal(t, state.Msgs, 2)
	require_Equal(t, state.FirstSeq, 1)
	require_Equal(t, state.LastSeq, 2)
	require_Equal(t, state.NumDeleted, 0)

	// Simulate a truncation that's only performed halfway, and we got hard killed at some point.
	removed, err := n.wal.RemoveMsg(2)
	require_True(t, removed)
	require_NoError(t, err)

	state = n.wal.State()
	require_Equal(t, state.Msgs, 1)
	require_Equal(t, state.FirstSeq, 1)
	require_Equal(t, state.LastSeq, 2)
	require_Equal(t, state.NumDeleted, 1)

	// Restart.
	n.Stop()
	n.WaitForStop()
	require_NoError(t, fs.Stop())
	fs, err = newFileStore(fcfg, scfg)
	require_NoError(t, err)
	cfg = &RaftConfig{Name: "TEST", Store: storeDir, Log: fs}
	n, err = s.initRaftNode(globalAccountName, cfg, pprofLabels{})
	require_NoError(t, err)

	// Should truncate the WAL on startup, the message was removed.
	state = n.wal.State()
	require_Equal(t, state.Msgs, 1)
	require_Equal(t, state.FirstSeq, 1)
	require_Equal(t, state.LastSeq, 1)
	require_Equal(t, state.NumDeleted, 0)

	// Store an invalid append entry manually.
	aeMsg2.pindex = 0
	aeMsg2 = encode(t, aeMsg2)
	_, _, err = n.wal.StoreMsg(_EMPTY_, nil, aeMsg2.buf, 0)
	require_NoError(t, err)

	state = n.wal.State()
	require_Equal(t, state.Msgs, 2)
	require_Equal(t, state.FirstSeq, 1)
	require_Equal(t, state.LastSeq, 2)
	require_Equal(t, state.NumDeleted, 0)

	// Restart.
	n.Stop()
	n.WaitForStop()
	require_NoError(t, fs.Stop())
	fs, err = newFileStore(fcfg, scfg)
	require_NoError(t, err)
	cfg = &RaftConfig{Name: "TEST", Store: storeDir, Log: fs}
	n, err = s.initRaftNode(globalAccountName, cfg, pprofLabels{})
	require_NoError(t, err)

	// Should truncate the WAL on startup, the append entry is invalid.
	state = n.wal.State()
	require_Equal(t, state.Msgs, 1)
	require_Equal(t, state.FirstSeq, 1)
	require_Equal(t, state.LastSeq, 1)
	require_Equal(t, state.NumDeleted, 0)
}

func TestNRGLeaderCatchupHandling(t *testing.T) {
	n, cleanup := initSingleMemRaftNode(t)
	defer cleanup()

	// Create a sample entry, the content doesn't matter, just that it's stored.
	esm := encodeStreamMsgAllowCompress("foo", "_INBOX.foo", nil, nil, 0, 0, true)
	entries := []*Entry{newEntry(EntryNormal, esm)}

	nats0 := "S1Nunr6R" // "nats-0"
	aeMsg1 := encode(t, &appendEntry{leader: nats0, term: 1, commit: 0, pterm: 0, pindex: 0, entries: entries})
	aeMsg2 := encode(t, &appendEntry{leader: nats0, term: 1, commit: 0, pterm: 1, pindex: 1, entries: entries})
	aeMsg3 := encode(t, &appendEntry{leader: nats0, term: 1, commit: 0, pterm: 1, pindex: 2, entries: entries})

	n.processAppendEntry(aeMsg1, n.aesub)
	n.processAppendEntry(aeMsg2, n.aesub)
	n.processAppendEntry(aeMsg3, n.aesub)
	require_Equal(t, n.pindex, 3)

	n.switchToLeader()

	catchupReply := "$TEST"
	nc, err := nats.Connect(n.s.ClientURL(), nats.UserInfo("admin", "s3cr3t!"))
	require_NoError(t, err)
	defer nc.Close()

	sub, err := nc.SubscribeSync(catchupReply)
	require_NoError(t, err)
	defer sub.Drain()
	require_NoError(t, nc.Flush())

	// Simulate a follower that's up-to-date with only the first message.
	n.catchupFollower(&appendEntryResponse{success: false, term: 1, index: 1, reply: catchupReply})

	// Should receive all messages the leader knows up to this point.
	msg, err := sub.NextMsg(500 * time.Millisecond)
	require_NoError(t, err)
	ae, err := decodeAppendEntry(msg.Data, nil, _EMPTY_)
	require_NoError(t, err)
	require_Equal(t, ae.pterm, 1)
	require_Equal(t, ae.pindex, 1)

	msg, err = sub.NextMsg(500 * time.Millisecond)
	require_NoError(t, err)
	ae, err = decodeAppendEntry(msg.Data, nil, _EMPTY_)
	require_NoError(t, err)
	require_Equal(t, ae.pterm, 1)
	require_Equal(t, ae.pindex, 2)
}

func TestNRGSnapshotOnlyCatchupNotCaughtUpUntilConfirmed(t *testing.T) {
	n, cleanup := initSingleMemRaftNode(t)
	defer cleanup()

	// Create a sample entry, the content doesn't matter, just that it's stored.
	esm := encodeStreamMsgAllowCompress("foo", "_INBOX.foo", nil, nil, 0, 0, true)
	entries := []*Entry{newEntry(EntryNormal, esm)}

	nats0 := "S1Nunr6R" // "nats-0"
	nats1 := "yrzKKRBu" // "nats-1"
	for i := range uint64(3) {
		ae := encode(t, &appendEntry{leader: nats0, term: 1, commit: 0, pterm: min(i, 1), pindex: i, entries: entries})
		n.processAppendEntry(ae, n.aesub)
	}
	require_Equal(t, n.pindex, 3)

	// Switching to leader stores a peer state entry at index 4.
	n.term = 1
	n.switchToLeader()
	require_Equal(t, n.pindex, 4)

	// Fully compact the log, so catchup only requires sending the snapshot.
	n.Lock()
	n.commit = n.pindex
	n.Unlock()
	n.Applied(4)
	require_NoError(t, n.InstallSnapshot(nil, false))
	var state StreamState
	n.wal.FastState(&state)
	require_Equal(t, state.Msgs, 0)

	// The follower was fully caught up.
	n.Lock()
	n.peers[nats1] = &lps{ts: time.Now(), li: 4}
	n.Unlock()
	require_True(t, n.IsFollowerCaughtUp(nats1))

	// But lost its state and requests a catchup, which only sends the snapshot.
	n.catchupFollower(newAppendEntryResponse(0, 0, nats1, false))
	n.RLock()
	_, ok := n.progress[nats1]
	n.RUnlock()
	require_False(t, ok)
	require_False(t, n.IsFollowerCaughtUp(nats1))

	// Only caught up once the follower confirms it has the snapshot.
	n.processAppendEntryResponse(newAppendEntryResponse(1, 4, nats1, true))
	require_True(t, n.IsFollowerCaughtUp(nats1))
}

func TestNRGNewEntriesFromOldLeaderResetsWALDuringCatchup(t *testing.T) {
	n, cleanup := initSingleMemRaftNode(t)
	defer cleanup()

	// Create a sample entry, the content doesn't matter, just that it's stored.
	esm := encodeStreamMsgAllowCompress("foo", "_INBOX.foo", nil, nil, 0, 0, true)
	entries := []*Entry{newEntry(EntryNormal, esm)}

	nats0 := "S1Nunr6R" // "nats-0"
	aeMsg1 := encode(t, &appendEntry{leader: nats0, lterm: 20, term: 20, commit: 0, pterm: 0, pindex: 0, entries: entries})
	aeMsg2 := encode(t, &appendEntry{leader: nats0, lterm: 20, term: 20, commit: 0, pterm: 20, pindex: 1, entries: entries})
	aeMsg3 := encode(t, &appendEntry{leader: nats0, lterm: 20, term: 20, commit: 0, pterm: 20, pindex: 2, entries: entries})

	aeReply := "$TEST"
	aeMsg1Fork := encode(t, &appendEntry{leader: nats0, term: 1, commit: 0, pterm: 0, pindex: 0, entries: entries, reply: aeReply})
	aeMsg2Fork := encode(t, &appendEntry{leader: nats0, term: 1, commit: 0, pterm: 1, pindex: 1, entries: entries})

	// Trigger a catchup.
	n.processAppendEntry(aeMsg2, n.aesub)
	validateCatchup := func() {
		t.Helper()
		require_True(t, n.catchup != nil)
		require_Equal(t, n.catchup.cterm, 20)
		require_Equal(t, n.catchup.cindex, 1)
	}
	validateCatchup()

	// Catchup the first missed entry.
	csub := n.catchup.sub
	n.processAppendEntry(aeMsg1, csub)
	require_Equal(t, n.pindex, 1)
	require_Equal(t, n.pterm, 20)

	nc, err := nats.Connect(n.s.ClientURL(), nats.UserInfo("admin", "s3cr3t!"))
	require_NoError(t, err)
	defer nc.Close()

	sub, err := nc.SubscribeSync(aeReply)
	require_NoError(t, err)
	defer sub.Drain()
	require_NoError(t, nc.Flush())

	// Would previously stall the catchup and restart it with a previous leader.
	n.catchup.pindex = aeMsg1.pindex + 1
	n.catchup.active = time.Time{}
	n.processAppendEntry(aeMsg1Fork, n.aesub)
	require_Equal(t, n.pindex, 1)
	require_Equal(t, n.pterm, 20)
	validateCatchup()

	// Should reply we have a higher term, prompting the server to step down.
	msg, err := sub.NextMsg(time.Second)
	require_NoError(t, err)
	ar := decodeAppendEntryResponse(msg.Data)
	require_False(t, ar.success)
	require_Equal(t, ar.index, 1)
	require_Equal(t, ar.term, 20)

	// Would previously reset the WAL.
	n.processAppendEntry(aeMsg2Fork, n.aesub)
	require_Equal(t, n.pindex, 1)
	require_Equal(t, n.pterm, 20)
	validateCatchup()

	// Now the catchup should continue, undisturbed by an old leader sending append entries.
	n.processAppendEntry(aeMsg2, csub)
	require_Equal(t, n.pindex, 2)
	require_Equal(t, n.pterm, 20)
	require_True(t, n.catchup == nil)

	// A remaining catchup entry can still be ingested, even if the catchup state itself is gone.
	n.processAppendEntry(aeMsg3, csub)
	require_Equal(t, n.pindex, 3)
	require_Equal(t, n.pterm, 20)
}

func TestNRGProcessed(t *testing.T) {
	n, cleanup := initSingleMemRaftNode(t)
	defer cleanup()

	// Create a sample entry, the content doesn't matter, just that it's stored.
	esm := encodeStreamMsgAllowCompress("foo", "_INBOX.foo", nil, nil, 0, 0, true)
	entries := []*Entry{newEntry(EntryNormal, esm)}

	nats0 := "S1Nunr6R" // "nats-0"

	// Timeline
	aeMsg1 := encode(t, &appendEntry{leader: nats0, term: 1, commit: 1, pterm: 0, pindex: 0, entries: entries})
	aeMsg2 := encode(t, &appendEntry{leader: nats0, term: 1, commit: 2, pterm: 1, pindex: 1, entries: entries})
	aeMsg3 := encode(t, &appendEntry{leader: nats0, term: 1, commit: 3, pterm: 1, pindex: 2, entries: entries})
	aeMsg4 := encode(t, &appendEntry{leader: nats0, term: 1, commit: 4, pterm: 1, pindex: 3, entries: entries})
	aeMsg5 := encode(t, &appendEntry{leader: nats0, term: 1, commit: 5, pterm: 1, pindex: 4, entries: entries})

	// Store three entries.
	for i, aeMsg := range []*appendEntry{aeMsg1, aeMsg2, aeMsg3} {
		n.processAppendEntry(aeMsg, n.aesub)
		require_Equal(t, n.pindex, uint64(i+1))
		require_Equal(t, n.commit, uint64(i+1))
		require_Equal(t, n.processed, 0)
		require_Equal(t, n.applied, 0)
	}

	// n.Processed can move both n.processed and n.applied up, with different values.
	n.Processed(1, 0)
	require_Equal(t, n.pindex, 3)
	require_Equal(t, n.commit, 3)
	require_Equal(t, n.processed, 1)
	require_Equal(t, n.applied, 0)

	n.Processed(2, 1)
	require_Equal(t, n.pindex, 3)
	require_Equal(t, n.commit, 3)
	require_Equal(t, n.processed, 2)
	require_Equal(t, n.applied, 1)

	// n.Applied moves both n.processed and n.applied up to the same value.
	n.Applied(3)
	require_Equal(t, n.pindex, 3)
	require_Equal(t, n.commit, 3)
	require_Equal(t, n.processed, 3)
	require_Equal(t, n.applied, 3)

	// Store the remaining messages.
	for i, aeMsg := range []*appendEntry{aeMsg4, aeMsg5} {
		n.processAppendEntry(aeMsg, n.aesub)
		require_Equal(t, n.pindex, uint64(i+4))
		require_Equal(t, n.commit, uint64(i+4))
		require_Equal(t, n.processed, 3)
		require_Equal(t, n.applied, 3)
	}

	// An invalid processed call with a higher applied should be clamped back down to the processed index.
	n.Processed(4, 5)
	require_Equal(t, n.pindex, 5)
	require_Equal(t, n.commit, 5)
	require_Equal(t, n.processed, 4)
	require_Equal(t, n.applied, 4)
}

func TestNRGSendAppendEntryNotLeader(t *testing.T) {
	n, cleanup := initSingleMemRaftNode(t)
	defer cleanup()

	require_Equal(t, n.State(), Follower)
	require_Equal(t, n.pindex, 0)

	nc, err := nats.Connect(n.s.ClientURL(), nats.UserInfo("admin", "s3cr3t!"))
	require_NoError(t, err)
	defer nc.Close()

	sub, err := nc.SubscribeSync(n.asubj)
	require_NoError(t, err)
	defer sub.Drain()
	require_NoError(t, nc.Flush())

	// sendAppendEntry acquires the lock by itself, so it must also protect against us not being leader anymore.
	n.sendAppendEntry([]*Entry{newEntry(EntryNormal, nil)})

	// We're a follower, so we should not be able to send or store a message.
	require_Equal(t, n.pindex, 0)
	msg, err := sub.NextMsg(250 * time.Millisecond)
	require_Error(t, err, nats.ErrTimeout)
	require_True(t, msg == nil)
}

func TestNRGDrainAndReplaySnapshot(t *testing.T) {
	n, cleanup := initSingleMemRaftNode(t)
	defer cleanup()

	// Create a sample entry, the content doesn't matter, just that it's stored.
	esm := encodeStreamMsgAllowCompress("foo", "_INBOX.foo", nil, nil, 0, 0, true)
	entries := []*Entry{newEntry(EntryNormal, esm)}

	nats0 := "S1Nunr6R" // "nats-0"

	// Timeline
	aeMsg1 := encode(t, &appendEntry{leader: nats0, term: 1, commit: 0, pterm: 0, pindex: 0, entries: entries})
	aeMsg2 := encode(t, &appendEntry{leader: nats0, term: 1, commit: 0, pterm: 1, pindex: 1, entries: entries})
	aeMsg3 := encode(t, &appendEntry{leader: nats0, term: 1, commit: 0, pterm: 1, pindex: 2, entries: entries})
	aeHeartbeat1 := encode(t, &appendEntry{leader: nats0, term: 1, commit: 3, pterm: 1, pindex: 3, entries: nil})
	aeMsg4 := encode(t, &appendEntry{leader: nats0, term: 1, commit: 3, pterm: 1, pindex: 3, entries: entries})
	aeHeartbeat2 := encode(t, &appendEntry{leader: nats0, term: 1, commit: 4, pterm: 1, pindex: 4, entries: nil})

	// Stage some entries as normal.
	require_Len(t, n.apply.len(), 0)
	n.processAppendEntry(aeMsg1, n.aesub)
	n.processAppendEntry(aeMsg2, n.aesub)
	n.processAppendEntry(aeMsg3, n.aesub)
	n.processAppendEntry(aeHeartbeat1, n.aesub)
	require_Equal(t, n.pindex, 3)
	require_Len(t, n.apply.len(), 3)
	require_Equal(t, n.commit, 3)
	require_Equal(t, n.hcommit, 0)

	// Just a sanity-check, if we have no snapshot then this should fail.
	require_False(t, n.DrainAndReplaySnapshot())
	require_Len(t, n.apply.len(), 3)
	require_Equal(t, n.commit, 3)
	require_Equal(t, n.hcommit, 0)

	// Simulate this server processing a snapshot that requires upper layer catchup.
	// This catchup timed out and we would then call into DrainAndReplaySnapshot.
	snap := []byte("snapshot")
	n.Applied(1)
	require_NoError(t, n.InstallSnapshot(snap, false))

	require_True(t, n.DrainAndReplaySnapshot())
	require_True(t, n.paused)
	require_Len(t, n.apply.len(), 1)
	require_Equal(t, n.commit, 1)
	require_Equal(t, n.hcommit, 3)

	// Simulate snapshot processing being successful and restoring the apply queue when resuming.
	n.ResumeApply()
	require_False(t, n.paused)
	require_Len(t, n.apply.len(), 3)
	require_Equal(t, n.commit, 3)
	require_Equal(t, n.hcommit, 0)

	// Now simulate another case where the snapshot processing times out multiple times.
	require_True(t, n.DrainAndReplaySnapshot())
	require_True(t, n.paused)
	require_Len(t, n.apply.len(), 1)
	require_Equal(t, n.commit, 1)
	require_Equal(t, n.hcommit, 3)

	// Could receive new messages in the meantime and need to keep tracking the highest known commit properly.
	n.processAppendEntry(aeMsg4, n.aesub)
	n.processAppendEntry(aeHeartbeat2, n.aesub)
	require_Equal(t, n.pindex, 4)
	require_True(t, n.paused)
	require_Len(t, n.apply.len(), 1)
	require_Equal(t, n.commit, 1)
	require_Equal(t, n.hcommit, 4)

	// Replaying again should preserve the highest known commit.
	require_True(t, n.DrainAndReplaySnapshot())
	require_True(t, n.paused)
	require_Len(t, n.apply.len(), 1)
	require_Equal(t, n.commit, 1)
	require_Equal(t, n.hcommit, 4)

	// Resume applies, and ensure correct state.
	n.ResumeApply()
	require_False(t, n.paused)
	require_Len(t, n.apply.len(), 4)
	require_Equal(t, n.commit, 4)
	require_Equal(t, n.hcommit, 0)
}

func TestNRGTrackPeerActive(t *testing.T) {
	// The leader should track timestamps for all peers.
	// Each follower should only track the leader, otherwise we would get outdated timestamps.
	checkLastSeen := func(peers map[string]RaftzGroupPeer) {
		for _, peer := range peers {
			if peer.LastSeen == _EMPTY_ {
				continue
			}
			elapsed, err := time.ParseDuration(peer.LastSeen)
			require_NoError(t, err)
			require_LessThan(t, elapsed, time.Second)
		}
	}

	c := createJetStreamClusterExplicit(t, "R3S", 3)
	defer c.shutdown()

	ml := c.leader()
	rs := c.randomNonLeader()
	var preferred *Server
	for _, s := range c.servers {
		if s == ml || s == rs {
			continue
		}
		preferred = s
		break
	}
	require_NotNil(t, preferred)

	time.Sleep(2 * time.Second)
	before := (*rs.Raftz(&RaftzOptions{}))[DEFAULT_SYSTEM_ACCOUNT][defaultMetaGroupName].Peers
	checkLastSeen(before)

	js := ml.getJetStream()
	n := js.getMetaGroup()
	require_NoError(t, n.StepDown(preferred.NodeName()))

	time.Sleep(2 * time.Second)
	after := (*rs.Raftz(&RaftzOptions{}))[DEFAULT_SYSTEM_ACCOUNT][defaultMetaGroupName].Peers
	checkLastSeen(after)
}

func TestNRGLostQuorum(t *testing.T) {
	n, cleanup := initSingleMemRaftNode(t)
	defer cleanup()

	nats0 := "S1Nunr6R" // "nats-0"

	nats1 := "yrzKKRBu" // "nats-1"
	n.Lock()
	n.addPeer(nats1)
	// Ignore scale-up grace period so this
	// test focuses on lost-quorum tracking.
	n.lsut = time.Time{}
	n.Unlock()

	require_Equal(t, n.State(), Follower)
	require_False(t, n.Quorum())
	require_True(t, n.lostQuorum())

	nc, err := nats.Connect(n.s.ClientURL(), nats.UserInfo("admin", "s3cr3t!"))
	require_NoError(t, err)
	defer nc.Close()

	// Respond to a vote request.
	sub, err := nc.Subscribe(n.vsubj, func(m *nats.Msg) {
		req := decodeVoteRequest(m.Data, m.Reply)
		resp := voteResponse{term: req.term, peer: nats0, granted: true}
		m.Respond(resp.encode())
	})
	require_NoError(t, err)
	defer sub.Drain()
	require_NoError(t, nc.Flush())

	// Switch to candidate and make sure we properly track the peer as active.
	n.switchToCandidate()
	require_Equal(t, n.State(), Candidate)
	require_False(t, n.Quorum())
	require_True(t, n.lostQuorum())

	n.Lock()
	n.addPeer(nats0)
	n.Unlock()

	n.runAsCandidate()
	require_Equal(t, n.State(), Leader)
	require_True(t, n.Quorum())
	require_False(t, n.lostQuorum())
}

func TestNRGParallelCatchupRollback(t *testing.T) {
	n, cleanup := initSingleMemRaftNode(t)
	defer cleanup()

	// Create a sample entry, the content doesn't matter, just that it's stored.
	esm := encodeStreamMsgAllowCompress("foo", "_INBOX.foo", nil, nil, 0, 0, true)
	entries := []*Entry{newEntry(EntryNormal, esm)}

	nats0 := "S1Nunr6R" // "nats-0"

	aeReply := "$TEST"
	nc, err := nats.Connect(n.s.ClientURL(), nats.UserInfo("admin", "s3cr3t!"))
	require_NoError(t, err)
	defer nc.Close()

	sub, err := nc.SubscribeSync(aeReply)
	require_NoError(t, err)
	defer sub.Drain()
	require_NoError(t, nc.Flush())

	// Timeline
	aeMsg1 := encode(t, &appendEntry{leader: nats0, term: 1, lterm: 1, commit: 0, pterm: 0, pindex: 0, entries: entries})
	aeMsg2 := encode(t, &appendEntry{leader: nats0, term: 1, lterm: 1, commit: 0, pterm: 1, pindex: 1, entries: entries})
	aeHeartbeat := encode(t, &appendEntry{leader: nats0, term: 1, commit: 0, pterm: 1, pindex: 2, entries: nil, reply: aeReply})

	// Trigger a catchup.
	n.processAppendEntry(aeMsg2, n.aesub)
	require_Equal(t, n.pindex, 0)
	require_NotNil(t, n.catchup)
	require_Equal(t, n.catchup.cterm, aeMsg2.pterm)
	require_Equal(t, n.catchup.cindex, aeMsg2.pindex)
	csub := n.catchup.sub

	// Receive the missed messages.
	n.processAppendEntry(aeMsg1, csub)
	require_Equal(t, n.pindex, 1)
	n.processAppendEntry(aeMsg2, csub)
	require_Equal(t, n.pindex, 2)
	require_True(t, n.catchup == nil)

	// Should respond to the heartbeat and allow the leader to commit.
	n.processAppendEntry(aeHeartbeat, n.aesub)
	msg, err := sub.NextMsg(time.Second)
	require_NoError(t, err)
	ar := decodeAppendEntryResponse(msg.Data)
	require_NotNil(t, ar)
	require_True(t, ar.success)
	require_Equal(t, ar.term, aeHeartbeat.term)
	require_Equal(t, ar.index, aeHeartbeat.pindex)

	// Now replay a message that was already received as a catchup entry.
	// Likely due to running multiple catchups in parallel.
	// Since our WAL is already ahead, we should not truncate based on this.
	n.processAppendEntry(aeMsg1, csub)
	require_Equal(t, n.pindex, 2)
}

func TestNRGReportLeaderAfterNoopEntry(t *testing.T) {
	n, cleanup := initSingleMemRaftNode(t)
	defer cleanup()

	require_Equal(t, n.State(), Follower)
	require_Equal(t, n.term, 0)
	require_False(t, n.Leader())

	n.switchToCandidate()
	require_Equal(t, n.State(), Candidate)
	require_Equal(t, n.term, 1)
	require_False(t, n.Leader())

	// Switching to leader will put us into Leader state,
	// but we're not necessarily an up-to-date leader yet.
	n.switchToLeader()
	require_Equal(t, n.State(), Leader)
	require_Equal(t, n.term, 1)
	require_Equal(t, n.pindex, 1) // Should've sent a NOOP-entry to establish leadership.
	require_Equal(t, n.applied, 0)
	require_False(t, n.Leader())

	// Once we commit and apply the final entry, we should starting to report we're leader.
	n.commit = 1
	n.Applied(1)
	require_Equal(t, n.applied, 1)
	require_True(t, n.Leader())
}

func TestNRGSendSnapshotInstallsSnapshot(t *testing.T) {
	n, cleanup := initSingleMemRaftNode(t)
	defer cleanup()

	nats0 := "S1Nunr6R" // "nats-0"
	nats1 := "yrzKKRBu" // "nats-1"

	n.Lock()
	n.addPeer(nats0)
	n.addPeer(nats1)
	n.Unlock()

	require_Equal(t, n.pindex, 0)
	require_Equal(t, n.snapfile, _EMPTY_)

	// Switch to candidate, to become leader.
	require_Equal(t, n.term, 0)
	n.switchToCandidate()
	require_Equal(t, n.term, 1)

	// When switching to leader a NOOP-entry is sent.
	n.switchToLeader()
	require_Equal(t, n.pindex, 1)
	require_Equal(t, n.snapfile, _EMPTY_)
	require_NoError(t, n.applyCommit(1))
	require_Equal(t, n.snapfile, _EMPTY_)

	// On scaleup, we send a snapshot.
	require_NoError(t, n.SendSnapshot([]byte("snapshot_data")))
	require_Equal(t, n.pindex, 2)
	require_Equal(t, n.snapfile, _EMPTY_)

	// When applying the entry, the sent snapshot should be installed.
	require_NoError(t, n.applyCommit(2))
	require_NotEqual(t, n.snapfile, _EMPTY_)

	snap, err := n.loadLastSnapshot()
	require_NoError(t, err)
	require_NotNil(t, snap)
	require_Equal(t, snap.lastTerm, 1)
	require_Equal(t, snap.lastIndex, 1)
	require_Equal(t, string(snap.data), "snapshot_data")

	// Draining and replaying the snapshot should work.
	require_True(t, n.DrainAndReplaySnapshot())
}

func TestNRGQuorumAfterLeaderStepdown(t *testing.T) {
	origMinTimeout, origMaxTimeout, origHBInterval := minElectionTimeout, maxElectionTimeout, hbInterval
	minElectionTimeout, maxElectionTimeout, hbInterval = minElectionTimeoutDefault, maxElectionTimeoutDefault, hbIntervalDefault
	defer func() {
		minElectionTimeout, maxElectionTimeout, hbInterval = origMinTimeout, origMaxTimeout, origHBInterval
	}()

	n, cleanup := initSingleMemRaftNode(t)
	defer cleanup()

	nats0 := "S1Nunr6R" // "nats-0"
	nats1 := "yrzKKRBu" // "nats-1"

	n.Lock()
	n.addPeer(nats0)
	n.addPeer(nats1)
	n.Unlock()

	// Become leader.
	n.switchToCandidate()
	n.switchToLeader()

	// Should not report having quorum, unless at least 1 peer is seen.
	require_False(t, n.Quorum())
	n.s.nodeToInfo.Store(nats0, nodeInfo{})
	require_NoError(t, n.trackPeer(nats0))
	require_True(t, n.Quorum())
	n.s.nodeToInfo.Store(nats1, nodeInfo{})
	require_NoError(t, n.trackPeer(nats1))
	require_True(t, n.Quorum())
	require_Len(t, len(n.peers), 3)

	// If we hand off leadership to another server, we should
	// still be reporting we have quorum.
	require_NoError(t, n.StepDown())
	require_True(t, n.Quorum())
	require_Len(t, len(n.peers), 3)
	for peer, ps := range n.peers {
		if peer == n.id {
			continue
		}
		// All peer timestamps should be preserved.
		require_False(t, ps.ts.IsZero())
	}

	// After our new leader comes online, we should still have quorum,
	// but the other follower's timestamp should be cleared.
	n.updateLeader(nats0)
	require_Equal(t, n.leader, nats0)
	require_True(t, n.Quorum())
	require_Len(t, len(n.peers), 3)
	for peer, ps := range n.peers {
		if peer == n.id {
			continue
		}
		if peer == nats0 {
			require_False(t, ps.ts.IsZero()) // Leader is preserved.
		} else {
			require_True(t, ps.ts.IsZero()) // Other follower is cleared.
		}
	}
}

func TestNRGNoLogResetOnCorruptedSendToFollower(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "R3S", 2)
	defer c.shutdown()

	rg := c.createRaftGroup("TEST", 2, newStateAdder)
	rg.waitOnLeader()

	leader := rg.leader().(*stateAdder)
	follower := rg.nonLeader().(*stateAdder)

	leaderrg := leader.node().(*raft)
	followerrg := follower.node().(*raft)

	for i := range int64(10) {
		leader.proposeDelta(i + 1)
		time.Sleep(50 * time.Millisecond) // ... for multiple AEs.
	}

	// Snapshot and compact.
	rg.waitOnTotal(t, 55)
	leader.snapshot(t)

	// Above snapshot should have resulted in compaction of the WAL.
	var ss StreamState
	leaderrg.wal.FastState(&ss)
	require_Equal(t, ss.Msgs, 0)

	// Stop the follower, we'll flatten their state so they have to
	// request a snapshot.
	require_NoError(t, followerrg.wal.Truncate(0))
	follower.stop()

	// Now we're going to subtly corrupt the snapshot on the leader.
	stat, err := os.Stat(leaderrg.snapfile)
	require_NoError(t, err)
	require_NoError(t, os.Truncate(leaderrg.snapfile, stat.Size()-1))

	// Now we'll bring the follower back. It should request a snapshot
	// from the leader. Previously this would have caused the leader to
	// blow away the entire log, but in reality we will probably just
	// install a new snapshot at some point soon anyway.
	follower.restart()
	c.waitOnAllCurrent()
	leaderrg.wal.FastState(&ss)
	require_NotEqual(t, ss.LastSeq, 0)
}

func TestNRGTruncateLogWithMisalignedSnapshotGap(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "R3S", 3)
	s := c.servers[0] // RunBasicJetStreamServer not available
	defer c.shutdown()

	storeDir := t.TempDir()
	fcfg := FileStoreConfig{StoreDir: storeDir, BlockSize: defaultMediumBlockSize, AsyncFlush: false, srv: s}
	scfg := StreamConfig{Name: "RAFT", Storage: FileStorage}
	fs, err := newFileStore(fcfg, scfg)
	require_NoError(t, err)

	cfg := &RaftConfig{Name: "TEST", Store: storeDir, Log: fs}

	err = s.bootstrapRaftNode(cfg, nil, false)
	require_NoError(t, err)
	n, err := s.initRaftNode(globalAccountName, cfg, pprofLabels{})
	require_NoError(t, err)

	// Create a sample entry, the content doesn't matter, just that it's stored.
	esm := encodeStreamMsgAllowCompress("foo", "_INBOX.foo", nil, nil, 0, 0, true)
	entries := []*Entry{newEntry(EntryNormal, esm)}

	nats0 := "S1Nunr6R" // "nats-0"

	// Timeline
	aeMsg1 := encode(t, &appendEntry{leader: nats0, term: 1, commit: 0, pterm: 0, pindex: 0, entries: entries})
	aeMsg2 := encode(t, &appendEntry{leader: nats0, term: 1, commit: 0, pterm: 1, pindex: 1, entries: entries})
	aeMsg3 := encode(t, &appendEntry{leader: nats0, term: 1, commit: 2, pterm: 1, pindex: 2, entries: entries})

	for i, ae := range []*appendEntry{aeMsg1, aeMsg2, aeMsg3} {
		n.processAppendEntry(ae, n.aesub)
		require_Equal(t, n.pindex, uint64(i+1))
	}

	// Manually call back down to applied, and then snapshot.
	n.Applied(1)
	require_NoError(t, n.InstallSnapshot(nil, false))

	state := n.wal.State()
	require_Equal(t, state.FirstSeq, 2)
	require_Equal(t, state.LastSeq, 3)

	// Manually compact the WAL further so it doesn't align anymore with the snapshot.
	_, err = n.wal.Compact(3)
	require_NoError(t, err)
	state = n.wal.State()
	require_Equal(t, state.FirstSeq, 3)
	require_Equal(t, state.LastSeq, 3)

	// Restart.
	n.Stop()
	n.WaitForStop()
	require_NoError(t, fs.Stop())
	fs, err = newFileStore(fcfg, scfg)
	require_NoError(t, err)
	cfg = &RaftConfig{Name: "TEST", Store: storeDir, Log: fs}
	n, err = s.initRaftNode(globalAccountName, cfg, pprofLabels{})
	require_NoError(t, err)

	// The gap between the snapshot and the WAL should be detected, and the WAL should truncate.
	// Can't continue normally with missing entries.
	state = n.wal.State()
	require_Equal(t, state.Msgs, 0)
	require_Equal(t, state.FirstSeq, 2)
	require_Equal(t, state.LastSeq, 1)
	require_Equal(t, n.pindex, 1)

	// Should be able to re-populate the missing messages.
	// Need to re-encode them, though, since they would have been returned to the pool before.
	aeMsg2 = encode(t, &appendEntry{leader: nats0, term: 1, commit: 0, pterm: 1, pindex: 1, entries: entries})
	aeMsg3 = encode(t, &appendEntry{leader: nats0, term: 1, commit: 2, pterm: 1, pindex: 2, entries: entries})
	for i, ae := range []*appendEntry{aeMsg2, aeMsg3} {
		n.processAppendEntry(ae, n.aesub)
		require_Equal(t, n.pindex, uint64(i+2))
	}
}

func TestNRGTruncateLogWithMissingSnapshot(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "R3S", 3)
	s := c.servers[0] // RunBasicJetStreamServer not available
	defer c.shutdown()

	storeDir := t.TempDir()
	fcfg := FileStoreConfig{StoreDir: storeDir, BlockSize: defaultMediumBlockSize, AsyncFlush: false, srv: s}
	scfg := StreamConfig{Name: "RAFT", Storage: FileStorage}
	fs, err := newFileStore(fcfg, scfg)
	require_NoError(t, err)

	cfg := &RaftConfig{Name: "TEST", Store: storeDir, Log: fs}

	err = s.bootstrapRaftNode(cfg, nil, false)
	require_NoError(t, err)
	n, err := s.initRaftNode(globalAccountName, cfg, pprofLabels{})
	require_NoError(t, err)

	// Create a sample entry, the content doesn't matter, just that it's stored.
	esm := encodeStreamMsgAllowCompress("foo", "_INBOX.foo", nil, nil, 0, 0, true)
	entries := []*Entry{newEntry(EntryNormal, esm)}

	nats0 := "S1Nunr6R" // "nats-0"

	// Timeline
	aeMsg1 := encode(t, &appendEntry{leader: nats0, term: 1, commit: 0, pterm: 0, pindex: 0, entries: entries})
	aeMsg2 := encode(t, &appendEntry{leader: nats0, term: 1, commit: 0, pterm: 1, pindex: 1, entries: entries})
	aeMsg3 := encode(t, &appendEntry{leader: nats0, term: 1, commit: 2, pterm: 1, pindex: 2, entries: entries})

	for i, ae := range []*appendEntry{aeMsg1, aeMsg2, aeMsg3} {
		n.processAppendEntry(ae, n.aesub)
		require_Equal(t, n.pindex, uint64(i+1))
	}

	// Manually simulate a snapshot being made, only compacting but not actually installing a snapshot.
	_, err = n.wal.Compact(2)
	require_NoError(t, err)

	state := n.wal.State()
	require_Equal(t, state.FirstSeq, 2)
	require_Equal(t, state.LastSeq, 3)

	// Restart.
	n.Stop()
	n.WaitForStop()
	require_NoError(t, fs.Stop())
	fs, err = newFileStore(fcfg, scfg)
	require_NoError(t, err)
	cfg = &RaftConfig{Name: "TEST", Store: storeDir, Log: fs}
	n, err = s.initRaftNode(globalAccountName, cfg, pprofLabels{})
	require_NoError(t, err)

	// The gap between the snapshot and the WAL should be detected, and the WAL should truncate.
	// Can't continue normally with missing entries.
	state = n.wal.State()
	require_Equal(t, state.Msgs, 0)
	require_Equal(t, state.FirstSeq, 0)
	require_Equal(t, state.LastSeq, 0)
	require_Equal(t, n.pindex, 0)
}

// This is a RaftChainOfBlocks test where a block is proposed and then we wait for all replicas to apply it before
// proposing the next one.
// The test may fail if:
//   - Replicas hash diverge
//   - One replica never applies the N-th block applied by the rest
//   - The given number of blocks cannot be applied within some amount of time
func TestNRGChainOfBlocksRunInLockstep(t *testing.T) {
	const iterations = 50
	const applyTimeout = 3 * time.Second
	const testTimeout = iterations * time.Second

	c := createJetStreamClusterExplicit(t, "R3S", 3)
	defer c.shutdown()

	rg := c.createRaftGroup("TEST", 3, newRaftChainStateMachine)
	rg.waitOnLeader()

	testTimer := time.NewTimer(testTimeout)

	for iteration := uint64(1); iteration <= iterations; iteration++ {
		select {
		case <-testTimer.C:
			t.Fatalf("Timeout, completed %d/%d iterations", iteration-1, iterations)
		default:
			// Continue
		}

		// Propose the next block (this test assumes the proposal always goes through)
		rg.randomMember().(*RCOBStateMachine).proposeBlock()

		var currentHash string

		// Wait on participants to converge
		checkFor(t, applyTimeout, 500*time.Millisecond, func() error {
			var previousNodeName string
			var previousNodeHash string
			for _, sm := range rg {
				stateMachine := sm.(*RCOBStateMachine)
				nodeName := fmt.Sprintf(
					"%s/%s",
					stateMachine.server().Name(),
					stateMachine.node().ID(),
				)

				running, blocksCount, hash := stateMachine.getCurrentHash()
				// All nodes always running
				if !running {
					return fmt.Errorf(
						"node %s is not running",
						nodeName,
					)
				}
				// Node is behind
				if blocksCount != iteration {
					return fmt.Errorf(
						"node %s applied %d blocks out of %d expected",
						nodeName,
						blocksCount,
						iteration,
					)
				}
				// Make sure hash is not empty
				if hash == "" {
					return fmt.Errorf(
						"node %s has empty hash after applying %d blocks",
						nodeName,
						blocksCount,
					)
				}
				// Check against previous node hash, unless this is the first node and we don't have anyone to compare
				if previousNodeHash != "" && previousNodeHash != hash {
					return fmt.Errorf(
						"hash mismatch after %d blocks: %s hash: %s != %s hash: %s",
						iteration,
						nodeName,
						hash,
						previousNodeName,
						previousNodeHash,
					)
				}
				// Set node name and hash for next node to compare against
				previousNodeName, previousNodeHash = nodeName, hash

			}
			// All replicas applied the last block and their hashes match
			currentHash = previousNodeHash
			return nil
		})
		if RCOBOptions.verbose {
			t.Logf(
				"Verified chain hash %s for %d/%d nodes after %d/%d iterations",
				currentHash,
				len(rg),
				len(rg),
				iteration,
				iterations,
			)
		}
	}
}

// This is a RaftChainOfBlocks test where one of the replicas is stopped before proposing a short burst of blocks.
// Upon resuming the replica, we check it is able to catch up to the rest.
// The test may fail if:
//   - Replicas hash diverge
//   - One replica never applies the N-th block applied by the rest
//   - The given number of blocks cannot be applied within some amount of time
func TestNRGChainOfBlocksStopAndCatchUp(t *testing.T) {
	const iterations = 50
	const blocksPerIteration = 3
	const applyTimeout = 3 * time.Second
	const testTimeout = 2 * iterations * time.Second

	c := createJetStreamClusterExplicit(t, "R3S", 3)
	defer c.shutdown()

	rg := c.createRaftGroup("TEST", 3, newRaftChainStateMachine)
	rg.waitOnLeader()

	testTimer := time.NewTimer(testTimeout)
	highestBlockSeen := uint64(0)

	for iteration := uint64(1); iteration <= iterations; iteration++ {

		select {
		case <-testTimer.C:
			t.Fatalf("Timeout, completed %d/%d iterations", iteration-1, iterations)
		default:
			// Continue
		}

		// Stop a random node
		stoppedNode := rg.randomMember()
		leader := stoppedNode.node().Leader()
		stoppedNode.stop()

		// Snapshot a random (non-stopped) node
		snapshotNode := rg.randomMember()
		for snapshotNode == stoppedNode {
			snapshotNode = rg.randomMember()
		}
		snapshotNode.(*RCOBStateMachine).createSnapshot()

		if RCOBOptions.verbose {
			t.Logf(
				"Iteration %d/%d: stopping node: %s/%s (leader: %v)",
				iteration,
				iterations,
				stoppedNode.server().Name(),
				stoppedNode.node().ID(),
				leader,
			)
		}

		// Propose some new blocks
		rg.waitOnLeader()
		for i := 0; i < blocksPerIteration; i++ {
			proposer := rg.randomMember()
			// Pick again if we randomly chose the stopped node
			for proposer == stoppedNode {
				proposer = rg.randomMember()
			}

			proposer.(*RCOBStateMachine).proposeBlock()
		}

		// Restart the stopped node
		stoppedNode.restart()

		// Wait on participants to converge
		expectedBlocks := iteration * blocksPerIteration
		var currentHash string
		checkFor(t, applyTimeout, 250*time.Millisecond, func() error {
			var previousNodeName string
			var previousNodeHash string
			for _, sm := range rg {
				stateMachine := sm.(*RCOBStateMachine)
				nodeName := fmt.Sprintf(
					"%s/%s",
					stateMachine.server().Name(),
					stateMachine.node().ID(),
				)
				running, blocksCount, currentHash := stateMachine.getCurrentHash()
				// Track the highest block seen by any replica
				if blocksCount > highestBlockSeen {
					highestBlockSeen = blocksCount
					// Must check all replicas again
					return fmt.Errorf("updated highest block to %d (%s)", highestBlockSeen, nodeName)
				}
				// All nodes should be running
				if !running {
					return fmt.Errorf(
						"node %s not running",
						nodeName,
					)
				}
				// Node is behind
				if blocksCount != expectedBlocks {
					return fmt.Errorf(
						"node %s applied %d blocks out of %d expected",
						nodeName,
						blocksCount,
						expectedBlocks,
					)
				}
				// Make sure hash is not empty
				if currentHash == "" {
					return fmt.Errorf(
						"node %s has empty hash after applying %d blocks",
						nodeName,
						blocksCount,
					)
				}
				// Check against previous node hash, unless this is the first node to be checked
				if previousNodeHash != "" && previousNodeHash != currentHash {
					return fmt.Errorf(
						"hash mismatch after %d blocks: %s hash: %s != %s hash: %s",
						expectedBlocks,
						nodeName,
						currentHash,
						previousNodeName,
						previousNodeHash,
					)
				}
				// Set node name and hash for next node to compare against
				previousNodeName, previousNodeHash = nodeName, currentHash
			}
			// All is well
			currentHash = previousNodeHash
			return nil
		})

		if RCOBOptions.verbose {
			t.Logf(
				"Verified chain hash %s for %d/%d nodes after %d blocks, %d/%d iterations (%d lost proposals)",
				currentHash,
				len(rg),
				len(rg),
				highestBlockSeen,
				iteration,
				iterations,
				expectedBlocks-highestBlockSeen,
			)
		}
	}
}

func TestNRGProposeRemovePeer(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "R3S", 3)
	defer c.shutdown()

	rg := c.createMemRaftGroup("TEST", 3, newStateAdder)
	rg.waitOnLeader()

	n := rg.leader().node()
	rg.waitOnLeader()

	peerId := rg.nonLeader().node().ID()
	require_NoError(t, n.ProposeRemovePeer(peerId))

	checkFor(t, 10*time.Second, 200*time.Millisecond, func() error {
		var err error
		for _, r := range rg {
			if len(r.node().Peers()) != 2 {
				err = errors.New("has not removed peer")
				break
			}

		}
		return err
	})
}

func TestNRGProposeRemovePeerNonExistent(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "R3S", 3)
	defer c.shutdown()

	rg := c.createMemRaftGroup("TEST", 3, newStateAdder)
	rg.waitOnLeader()

	leader := rg.leader().node().(*raft)

	const bogus = "deadbeef" // 8 chars, matches idLen
	leader.RLock()
	_, exists := leader.peers[bogus]
	leader.RUnlock()
	require_False(t, exists)

	// Removing a peer not in the group should fail synchronously and have no side effects.
	require_Error(t, leader.ProposeRemovePeer(bogus), errPeerNotFound)
	require_False(t, leader.MembershipChangeInProgress())

	for _, r := range rg {
		n := r.node().(*raft)
		n.RLock()
		_, removed := n.removed[bogus]
		n.RUnlock()
		require_False(t, removed)
		require_Equal(t, len(r.node().Peers()), 3)
	}

	// A real removal still works afterward.
	realPeer := rg.nonLeader().node().ID()
	require_NoError(t, leader.ProposeRemovePeer(realPeer))
	checkFor(t, 2*time.Second, 200*time.Millisecond, func() error {
		for _, r := range rg {
			if len(r.node().Peers()) != 2 {
				return errors.New("has not removed peer")
			}
		}
		return nil
	})
}

func TestNRGForwardedRemovePeerProposalNonExistent(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "R3S", 3)
	defer c.shutdown()

	rg := c.createMemRaftGroup("TEST", 3, newStateAdder)
	rg.waitOnLeader()

	leader := rg.leader().node().(*raft)
	follower := rg.nonLeader().node()

	const bogus = "deadbeef" // 8 chars, matches idLen
	leader.RLock()
	_, exists := leader.peers[bogus]
	leader.RUnlock()
	require_False(t, exists)

	// ProposeRemovePeer on a follower forwards to the leader's
	// handleForwardedRemovePeerProposal. The follower returns nil regardless,
	// so we have to inspect the leader directly for the side effects.
	require_NoError(t, follower.ProposeRemovePeer(bogus))

	// Give the forwarded RPC plenty of time to arrive and (incorrectly) take
	// effect if the existence check were missing. With the check, no
	// membership change is queued and the removed map stays clean.
	time.Sleep(500 * time.Millisecond)

	require_False(t, leader.MembershipChangeInProgress())
	for _, r := range rg {
		n := r.node().(*raft)
		n.RLock()
		_, removed := n.removed[bogus]
		n.RUnlock()
		require_False(t, removed)
		require_Equal(t, len(r.node().Peers()), 3)
	}

	// A real forwarded removal still works.
	realPeer := follower.ID()
	require_NoError(t, follower.ProposeRemovePeer(realPeer))
	checkFor(t, 2*time.Second, 200*time.Millisecond, func() error {
		for _, r := range rg {
			if len(r.node().Peers()) != 2 {
				return errors.New("has not removed peer")
			}
		}
		return nil
	})
}

func TestNRGProposeRemovePeerConcurrent(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "R3S", 3)
	defer c.shutdown()

	rg := c.createMemRaftGroup("TEST", 3, newStateAdder)
	rg.waitOnLeader()

	n := rg.leader().node()

	locked := rg.lockFollowers()

	// Attempt to remove the first follower, should succeed.
	err := n.ProposeRemovePeer(locked[0].node().ID())
	require_NoError(t, err)

	// Check that membership change is in progress.
	checkFor(t, 2*time.Second, 200*time.Millisecond, func() error {
		if n.MembershipChangeInProgress() {
			return nil
		} else {
			return errors.New("membership not in progress")
		}
	})

	// Attempt to remove the second follower, should fail.
	err = n.ProposeRemovePeer(locked[1].node().ID())
	require_Error(t, err, errMembershipChange)

	for _, l := range locked {
		l.node().(*raft).Unlock()
	}

	// Expect only one peer removal to succeed
	checkFor(t, 10*time.Second, 200*time.Millisecond, func() error {
		var err error
		for _, r := range rg {
			if len(r.node().Peers()) != 2 {
				err = errors.New("has not removed peer")
				break
			}

		}
		return err
	})
}

func TestNRGUncommittedMembershipChangeOnNewLeader(t *testing.T) {
	n, cleanup := initSingleMemRaftNode(t)
	defer cleanup()

	nats1 := "yrzKKRBu" // "nats-1"
	nats2 := "cnrtt3eg" // "nats-2"

	n.Lock()
	n.addPeer(nats1)
	n.addPeer(nats2)
	n.Unlock()

	entries := []*Entry{newEntry(EntryRemovePeer, []byte(nats2))}
	aeRemovePeer := encode(t, &appendEntry{leader: nats1, term: 1, commit: 0, pterm: 0, pindex: 0, entries: entries})

	// plant a EntryRemovePeer in the log
	n.processAppendEntry(aeRemovePeer, n.aesub)
	require_Equal(t, n.pindex, 1)
	require_False(t, n.Healthy())

	// become the new leader
	n.term = 2
	n.switchToLeader()

	err := n.ProposeRemovePeer(nats1)
	require_Error(t, err, errMembershipChange)
}

func TestNRGUncommittedMembershipChangeGetsTruncated(t *testing.T) {
	n, cleanup := initSingleMemRaftNode(t)
	defer cleanup()

	nats1 := "yrzKKRBu" // "nats-1"

	esm := encodeStreamMsgAllowCompress("foo", "_INBOX.foo", nil, nil, 0, 0, true)
	entries := []*Entry{newEntry(EntryNormal, esm)}
	aeMsg := encode(t, &appendEntry{leader: nats1, term: 1, commit: 0, pterm: 0, pindex: 0, entries: entries})
	entries = []*Entry{newEntry(EntryAddPeer, []byte(nats1))}
	aeAddPeer := encode(t, &appendEntry{leader: nats1, term: 1, commit: 0, pterm: 1, pindex: 1, entries: entries})

	// Set up a membership change.
	n.processAppendEntry(aeMsg, n.aesub)
	n.processAppendEntry(aeAddPeer, n.aesub)
	require_Equal(t, n.pindex, 2)
	require_True(t, n.MembershipChangeInProgress())
	require_Equal(t, n.membChange.index, 2)

	// If the entry containing the membership change isn't truncated, it should remain in progress.
	n.truncateWAL(n.pterm, n.pindex)
	require_True(t, n.MembershipChangeInProgress())
	require_Equal(t, n.membChange.index, 2)

	// If the entry IS truncated, then it shouldn't be in progress anymore.
	n.truncateWAL(n.pterm, n.pindex-1)
	require_False(t, n.MembershipChangeInProgress())
	require_True(t, n.membChange == nil)
}

func TestNRGUncommittedMembershipChangeOnNewLeaderForwardedRemovePeerProposal(t *testing.T) {
	n, cleanup := initSingleMemRaftNode(t)
	defer cleanup()

	nats1 := "yrzKKRBu" // "nats-1"
	nats2 := "cnrtt3eg" // "nats-2"

	n.Lock()
	n.addPeer(nats1)
	n.addPeer(nats2)
	n.Unlock()

	entries := []*Entry{newEntry(EntryRemovePeer, []byte(nats2))}
	aeRemovePeer := encode(t, &appendEntry{leader: nats1, term: 1, commit: 0, pterm: 0, pindex: 0, entries: entries})

	// plant a EntryRemovePeer in the log
	n.processAppendEntry(aeRemovePeer, n.aesub)
	require_Equal(t, n.pindex, 1)
	require_False(t, n.Healthy())

	// become the new leader
	n.term = 2
	n.switchToLeader()
	n.leaderState.Store(true)
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		n.runAsLeader()
	}()
	defer wg.Wait()

	n.RLock()
	rpsubj := n.rpsubj
	n.RUnlock()

	nc, _ := jsClientConnect(t, n.s, nats.UserInfo("admin", "s3cr3t!"))
	defer nc.Close()

	// Forward a peer-remove proposal to the new leader.
	before, _, _ := n.Progress()
	require_True(t, n.MembershipChangeInProgress())
	checkFor(t, 2*time.Second, 200*time.Millisecond, func() error {
		_, err := nc.Request(rpsubj, []byte(nats1), 100*time.Millisecond)
		// Wait for the server to be subscribed and either respond or time out.
		if err == nil || errors.Is(err, nats.ErrTimeout) {
			return nil
		}
		return err
	})
	// The forwarded peer-remove should not be accepted.
	time.Sleep(200 * time.Millisecond)
	after, _, _ := n.Progress()
	require_Equal(t, before, after)
	require_True(t, n.MembershipChangeInProgress())
}

func TestNRGIgnoreForwardedProposalIfNotCaughtUpLeader(t *testing.T) {
	n, cleanup := initSingleMemRaftNode(t)
	defer cleanup()

	nats1 := "yrzKKRBu" // "nats-1"
	nats2 := "cnrtt3eg" // "nats-2"

	n.Lock()
	n.addPeer(nats1)
	n.addPeer(nats2)
	n.Unlock()

	n.term = 1
	n.switchToLeader()
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		n.runAsLeader()
	}()
	defer wg.Wait()

	n.RLock()
	// Although this server is the leader, mark it as not caught up yet.
	n.leaderState.Store(false)
	psubj := n.psubj
	n.RUnlock()

	nc, _ := jsClientConnect(t, n.s, nats.UserInfo("admin", "s3cr3t!"))
	defer nc.Close()

	// Forward a normal proposal to the new leader.
	before, _, _ := n.Progress()
	checkFor(t, 2*time.Second, 200*time.Millisecond, func() error {
		_, err := nc.Request(psubj, nil, 100*time.Millisecond)
		// Wait for the server to be subscribed and either respond or time out.
		if err == nil || errors.Is(err, nats.ErrTimeout) {
			return nil
		}
		return err
	})
	// The forwarded proposal should not be accepted.
	time.Sleep(200 * time.Millisecond)
	after, _, _ := n.Progress()
	require_Equal(t, before, after)
}

func TestNRGProposeRemovePeerQuorum(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "R3S", 3)
	defer c.shutdown()

	rg := c.createMemRaftGroup("TEST", 3, newStateAdder)
	rg.waitOnLeader()

	leader := rg.leader().node()
	followers := rg.followers()

	require_True(t, len(followers) == 2)

	// Block follower 0 and remove follower 1
	followers[0].node().(*raft).Lock()
	err := leader.ProposeRemovePeer(followers[1].node().ID())
	require_NoError(t, err)

	// Should not be able to make progress
	time.Sleep(time.Second)
	require_True(t, leader.MembershipChangeInProgress())

	// Unlock the other follower and expect the membership
	// change to eventually finish
	followers[0].node().(*raft).Unlock()
	checkFor(t, 10*time.Second, 200*time.Millisecond, func() error {
		if leader.MembershipChangeInProgress() {
			return errors.New("membership still in progress")
		} else {
			return nil
		}
	})
}

// Test outline:
//   - In a R3 cluster, PeerRemove the leader and block on of
//     the followers.
//   - Verify that the membership change can't make progress,
//     the leader should not count its own ack towards quorum.
//   - Release the previously blocked follower and expect the
//     membership change to take place.
func TestNRGProposeRemovePeerLeader(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "R3S", 3)
	defer c.shutdown()

	rg := c.createMemRaftGroup("TEST", 3, newStateAdder)
	rg.waitOnLeader()

	leader := rg.leader().node()
	followers := rg.followers()
	leaderID := leader.ID()
	require_True(t, len(followers) == 2)

	// Block follower 0 and remove the leader
	followers[0].node().(*raft).Lock()
	err := leader.ProposeRemovePeer(leader.ID())
	require_NoError(t, err)

	// Should not be able to make progress
	time.Sleep(time.Second)
	require_True(t, leader.MembershipChangeInProgress())

	// Unlock the follower and expect the membership
	// change to eventually finish
	followers[0].node().(*raft).Unlock()
	checkFor(t, 10*time.Second, 200*time.Millisecond, func() error {
		if leader.MembershipChangeInProgress() {
			return errors.New("membership still in progress")
		} else {
			return nil
		}
	})

	// Old leader steps down
	checkFor(t, 10*time.Second, 200*time.Millisecond, func() error {
		newLeader := rg.waitOnLeader()
		if newLeader.node().ID() == leaderID {
			return errors.New("leader has not changed yet")
		}
		return nil
	})

	newLeader := rg.waitOnLeader()
	require_Equal(t, leader.State(), Closed)
	require_NotEqual(t, leader.ID(), newLeader.node().ID())
	require_Equal(t, len(newLeader.node().Peers()), 2)
	require_False(t, newLeader.node().MembershipChangeInProgress())
}

func TestNRGProposeRemovePeerAll(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "R3S", 3)
	defer c.shutdown()

	rg := c.createMemRaftGroup("TEST", 3, newStateAdder)
	rg.waitOnLeader()

	leader := rg.leader()
	followers := rg.followers()
	require_Equal(t, len(followers), 2)

	peers := leader.node().Peers()
	require_Equal(t, len(peers), 3)
	for i, follower := range followers {
		require_NoError(t, leader.node().ProposeRemovePeer(follower.node().ID()))
		checkFor(t, 2*time.Second, 10*time.Millisecond, func() error {
			if n := leader.node(); len(n.Peers()) == 2-i && !n.MembershipChangeInProgress() {
				return nil
			}
			return errors.New("membership still in progress")
		})
	}

	peers = leader.node().Peers()
	leaderID := leader.node().ID()

	// The leader is the only one left...
	require_Equal(t, len(peers), 1)
	require_Equal(t, peers[0].ID, leaderID)
	// and we can't remove it
	require_Error(t, leader.node().ProposeRemovePeer(leaderID), errRemoveLastNode)
}

func TestNRGLeaderResurrectsRemovedPeers(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "R3S", 3)
	defer c.shutdown()

	rg := c.createMemRaftGroup("TEST", 3, newStateAdder)
	rg.waitOnLeader()

	leader := rg.leader()
	followers := rg.followers()
	require_Equal(t, len(followers), 2)

	// Remove one follower
	require_NoError(t, leader.node().ProposeRemovePeer(followers[0].node().ID()))
	checkFor(t, 2*time.Second, 10*time.Millisecond, func() error {
		if peers := leader.node().Peers(); len(peers) == 2 {
			return nil
		}
		return errors.New("membership still in progress")
	})

	// Stop the leader and restart it.
	// If bug is present: the leader resurrects the previously removed peer.
	leader.stop()
	followers[1].stop()

	leader.restart()
	require_Equal(t, len(leader.node().Peers()), 2)

	followers[1].restart()
	require_Equal(t, len(leader.node().Peers()), 2)
}

func TestNRGAddPeers(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "R3S", 3)
	defer c.shutdown()

	rg := c.createMemRaftGroup("TEST", 3, newStateAdder)
	leader := rg.waitOnLeader()

	require_Equal(t, leader.node().ClusterSize(), 3)

	for range 6 {
		rg = append(rg, c.addMemRaftNode("TEST", newStateAdder))
	}

	checkFor(t, 1*time.Second, 10*time.Millisecond, func() error {
		if leader.node().ClusterSize() != 9 {
			return errors.New("node additions still in progress")
		}
		return nil
	})

	require_Equal(t, leader.node().ClusterSize(), 9)
}

func TestNRGDisjointMajorities(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "R3S", 3)
	defer c.shutdown()

	rg := c.createMemRaftGroup("TEST", 3, newStateAdder)
	rg.waitOnLeader()

	leader := rg.leader()
	followers := rg.followers()
	require_Equal(t, len(followers), 2)

	// Lock all followers, a majority.
	locked := rg.lockFollowers()
	require_Equal(t, len(locked), 2)

	defer func() {
		for _, l := range locked {
			l.node().(*raft).Unlock()
		}
	}()

	// Add one node (cluster size is 4)
	c.addMemRaftNode("TEST", newStateAdder)

	checkFor(t, 1*time.Second, 10*time.Millisecond, func() error {
		if leader.node().ClusterSize() != 4 {
			return errors.New("node addition still in progress")
		}
		return nil
	})

	// Attempt another node addition
	c.addMemRaftNode("TEST", newStateAdder)

	// If bug is present:
	// The leader is able to form a majority because it would
	// add peers immediately, and readjust cluster size and
	// quorum only after committing EntryAddPeer. This allowed
	// the leader to commit the entries using only newly added
	// nodes (the original followers are locked and not
	// acknowledging the EntryAddPeer proposals!)
	// This should not never happen. In a real scenario the
	// followers could be partitioned, and they actually have
	// a majority... so they could diverge and we end up with
	// two different histories.
	//
	// Here wait a little bit, and check that the leader is
	// unable to make any progess.
	time.Sleep(time.Second)

	require_Equal(t, leader.node().ClusterSize(), 4)
	require_Equal(t, leader.node().MembershipChangeInProgress(), true)
}

func TestNRGAppendEntryResurrectsLeader(t *testing.T) {
	n, cleanup := initSingleMemRaftNode(t)
	defer cleanup()

	S2 := "z3WIzPtj" // S-2

	n.Lock()
	n.addPeer(S2)
	n.Unlock()
	require_Len(t, len(n.Peers()), 2)
	require_Equal(t, n.ClusterSize(), 2)

	// PeerRemove S2
	entries := []*Entry{newEntry(EntryRemovePeer, []byte(S2))}
	aeRemovePeer := encode(t, &appendEntry{
		leader: S2, term: 1, commit: 0, pterm: 0, pindex: 0, entries: entries})
	n.processAppendEntry(aeRemovePeer, n.aesub)

	// Heartbeat commits the PeerRemove
	aeHeartBeat := encode(t, &appendEntry{
		leader: S2, term: 1, commit: 1, pterm: 1, pindex: 1, entries: nil})
	n.processAppendEntry(aeHeartBeat, n.aesub)

	require_Len(t, len(n.Peers()), 1)
	require_Equal(t, n.ClusterSize(), 1)

	// If bug is present: receiving a appendEntry from the old leader
	// will resurrect it. In practice, this has been observed with
	// LeaderTransfer entries, but any type of entry will do...
	aeHeartBeat2 := encode(t, &appendEntry{
		leader: S2, term: 1, commit: 1, pterm: 1, pindex: 1, entries: nil})
	n.processAppendEntry(aeHeartBeat2, n.aesub)

	// Expect the cluster size to be unchanged
	require_Len(t, len(n.Peers()), 1)
	require_Equal(t, n.ClusterSize(), 1)
}

func TestNRGSingleNodeElection(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "R3S", 3)
	defer c.shutdown()

	rg := c.createMemRaftGroup("TEST", 3, newStateAdder)

	// Remove the cluster leader, and then again
	for range 2 {
		leader := rg.waitOnLeader().node()
		require_NoError(t, leader.ProposeRemovePeer(leader.ID()))
		checkFor(t, 1*time.Second, 10*time.Millisecond, func() error {
			if leader.State() == Leader {
				return errors.New("Removed node is still leader")
			}
			return nil
		})
		require_False(t, leader.MembershipChangeInProgress())
	}

	// The remaining follower must be able to become leader
	// on its own
	newLeader := rg.waitOnLeader()
	require_Equal(t, len(newLeader.node().Peers()), 1)
	require_Equal(t, newLeader.node().ClusterSize(), 1)
	require_False(t, newLeader.node().MembershipChangeInProgress())

	adder := newLeader.(*stateAdder)
	adder.proposeDelta(1)
	adder.proposeDelta(10)
	adder.proposeDelta(100)

	rg.waitOnTotal(t, 111)

	// Add two nodes back
	rg = append(rg, c.addMemRaftNode("TEST", newStateAdder))
	rg = append(rg, c.addMemRaftNode("TEST", newStateAdder))

	checkFor(t, 1*time.Second, 10*time.Millisecond, func() error {
		if newLeader.node().ClusterSize() != 3 {
			return errors.New("node additions still in progress")
		}
		return nil
	})

	checkFor(t, 1*time.Second, 10*time.Millisecond, func() error {
		if newLeader.node().MembershipChangeInProgress() {
			return errors.New("membership still in progress")
		}
		return nil
	})

	rg.waitOnTotal(t, 111)
	require_Equal(t, newLeader.node().ClusterSize(), 3)
	require_False(t, newLeader.node().MembershipChangeInProgress())
}

func TestNRGMustNotResetVoteOnStepDownOrLeaderTransfer(t *testing.T) {
	n, cleanup := initSingleMemRaftNode(t)
	defer cleanup()

	nats0 := "S1Nunr6R" // "nats-0"

	// Vote for a new leader.
	require_NoError(t, n.processVoteRequest(&voteRequest{term: 1, candidate: nats0}))
	require_Equal(t, n.term, 1)
	require_Equal(t, n.vote, nats0)

	// Stepping down as leader must NOT reset the vote info.
	n.state.Store(int32(Leader))
	require_NoError(t, n.StepDown())
	require_Equal(t, n.vote, nats0)

	// A leader transfer must NOT reset the vote info.
	// This is automatically cleared once the intended leader starts a new election with a higher term.
	entries := []*Entry{newEntry(EntryLeaderTransfer, []byte(nats0))}
	aeLeaderTransfer := encode(t, &appendEntry{leader: nats0, term: 1, commit: 0, pterm: 0, pindex: 0, entries: entries})
	n.processAppendEntry(aeLeaderTransfer, n.aesub)
	require_Equal(t, n.term, 1)
	require_Equal(t, n.vote, nats0)
}

func TestNRGAppendEntriesSeqStopsOnRangeBreak(t *testing.T) {
	n, cleanup := initSingleMemRaftNode(t)
	defer cleanup()

	esm := encodeStreamMsgAllowCompress("foo", "_INBOX.foo", nil, nil, 0, 0, true)
	entries := []*Entry{newEntry(EntryNormal, esm)}
	nats0 := "S1Nunr6R" // "nats-0"

	// Store and commit two entries
	aeMsg1 := encode(t, &appendEntry{leader: nats0, term: 1, commit: 0, pterm: 0, pindex: 0, entries: entries})
	aeMsg2 := encode(t, &appendEntry{leader: nats0, term: 1, commit: 1, pterm: 1, pindex: 1, entries: entries})
	aeHeartbeat := encode(t, &appendEntry{leader: nats0, term: 1, commit: 2, pterm: 1, pindex: 2})

	n.processAppendEntry(aeMsg1, n.aesub)
	n.processAppendEntry(aeMsg2, n.aesub)
	n.processAppendEntry(aeHeartbeat, n.aesub)
	n.Applied(2)

	c, err := n.CreateSnapshotCheckpoint(false)
	require_NoError(t, err)
	defer c.Abort()

	var calls int
	for ae, err := range c.AppendEntriesSeq() {
		calls++
		require_NoError(t, err)
		require_NotNil(t, ae)
		// If bug is present: the iterator keeps
		// yielding even if we break, causing a
		// runtime panic.
		break
	}
	require_Equal(t, calls, 1)
}

func TestNRGInstallSnapshotFromCheckpoint(t *testing.T) {
	n, cleanup := initSingleMemRaftNode(t)
	defer cleanup()

	// Create a sample entry, the content doesn't matter, just that it's stored.
	esm := encodeStreamMsgAllowCompress("foo", "_INBOX.foo", nil, nil, 0, 0, true)
	entries := []*Entry{newEntry(EntryNormal, esm)}

	nats0 := "S1Nunr6R" // "nats-0"

	// Timeline
	aeMsg1 := encode(t, &appendEntry{leader: nats0, term: 1, commit: 0, pterm: 0, pindex: 0, entries: entries})
	aeMsg2 := encode(t, &appendEntry{leader: nats0, term: 1, commit: 1, pterm: 1, pindex: 1, entries: entries})
	aeMsg3 := encode(t, &appendEntry{leader: nats0, term: 1, commit: 2, pterm: 1, pindex: 2, entries: entries})
	aeMsg4 := encode(t, &appendEntry{leader: nats0, term: 1, commit: 3, pterm: 1, pindex: 3, entries: entries})
	aeHeartbeat := encode(t, &appendEntry{leader: nats0, term: 1, commit: 4, pterm: 1, pindex: 4, entries: nil})

	// Process the first set of messages.
	n.processAppendEntry(aeMsg1, n.aesub)
	require_Equal(t, n.pindex, 1)
	require_Equal(t, n.commit, 0)
	n.processAppendEntry(aeMsg2, n.aesub)
	require_Equal(t, n.pindex, 2)
	require_Equal(t, n.commit, 1)
	n.Applied(1)
	require_Equal(t, n.applied, 1)

	// Start the first checkpoint, creating the first snapshot.
	c, err := n.CreateSnapshotCheckpoint(false)
	require_NoError(t, err)

	// There's no previous snapshot.
	buf, err := c.LoadLastSnapshot()
	require_Error(t, err, errNoSnapAvailable)
	require_True(t, buf == nil)

	// Since only 1 entry was applied, we should be able to retrieve that here.
	var count int
	for ae, err := range c.AppendEntriesSeq() {
		count++
		require_NoError(t, err)
		require_Equal(t, ae.pindex, 0)
	}
	require_Equal(t, count, 1)

	// Install the snapshot and confirm we can retrieve it.
	_, err = c.InstallSnapshot([]byte("old"))
	require_NoError(t, err)
	snap, err := n.loadLastSnapshot()
	require_NoError(t, err)
	require_True(t, bytes.Equal(snap.data, []byte("old")))

	// Checkpoint was installed, so all should now abort.
	_, err = c.LoadLastSnapshot()
	require_Error(t, err, errSnapAborted)
	count = 0
	for _, err = range c.AppendEntriesSeq() {
		count++
		require_Error(t, err, errSnapAborted)
	}
	require_Equal(t, count, 1)
	_, err = c.InstallSnapshot(nil)
	require_Error(t, err, errSnapAborted)

	// Process the next set of messages, we'll be able to compact two append entries now.
	n.processAppendEntry(aeMsg3, n.aesub)
	require_Equal(t, n.pindex, 3)
	require_Equal(t, n.commit, 2)
	n.processAppendEntry(aeMsg4, n.aesub)
	require_Equal(t, n.pindex, 4)
	require_Equal(t, n.commit, 3)
	n.Applied(3)
	require_Equal(t, n.applied, 3)

	// Check a couple edge cases now, a second checkpoint should fail noting a snapshot is already in progress.
	c, err = n.CreateSnapshotCheckpoint(false)
	require_NoError(t, err)
	_, err = n.CreateSnapshotCheckpoint(false)
	require_Error(t, err, errSnapInProgress)

	// Abort checkpoint, so all should now abort.
	c.Abort()
	_, err = c.LoadLastSnapshot()
	require_Error(t, err, errSnapAborted)
	count = 0
	for _, err = range c.AppendEntriesSeq() {
		count++
		require_Error(t, err, errSnapAborted)
	}
	require_Equal(t, count, 1) // Must always iterate at least once to retrieve the error.
	_, err = c.InstallSnapshot(nil)
	require_Error(t, err, errSnapAborted)

	// Start the second checkpoint, creating the second snapshot.
	c, err = n.CreateSnapshotCheckpoint(false)
	require_NoError(t, err)

	// Should load the previous snapshot.
	buf, err = c.LoadLastSnapshot()
	require_NoError(t, err)
	require_True(t, bytes.Equal(buf, []byte("old")))

	// Iterate over the two append entries part of this new snapshot.
	count = 0
	for ae, err := range c.AppendEntriesSeq() {
		count++
		require_NoError(t, err)
		require_Equal(t, ae.pindex, uint64(count))
	}
	require_Equal(t, count, 2)

	// Install the snapshot and confirm we can retrieve it.
	_, err = c.InstallSnapshot([]byte("new"))
	require_NoError(t, err)
	snap, err = n.loadLastSnapshot()
	require_NoError(t, err)
	require_True(t, bytes.Equal(snap.data, []byte("new")))

	// Commit and apply the last entry to test for the final edge case.
	n.processAppendEntry(aeHeartbeat, n.aesub)
	require_Equal(t, n.pindex, 4)
	require_Equal(t, n.commit, 4)
	n.Applied(4)
	require_Equal(t, n.applied, 4)

	// Start an async snapshot.
	c, err = n.CreateSnapshotCheckpoint(false)
	require_NoError(t, err)

	// Installing a snapshot normally should be denied, as one is already in progress.
	require_Error(t, n.InstallSnapshot(nil, false), errSnapInProgress)

	// The checkpoint should still work successfully.
	buf, err = c.LoadLastSnapshot()
	require_NoError(t, err)
	require_True(t, bytes.Equal(buf, []byte("new")))

	// However, if we install using the internal API, this should be allowed and abort our checkpoint.
	// This API will be used as a result of receiving a snapshot as part of catchup, so it's really important
	// we aren't allowed to replace this newest snapshot with an older snapshot from an earlier checkpoint.
	snap.lastIndex++
	require_NoError(t, n.installSnapshot(snap))

	// Check it's aborted.
	_, err = c.LoadLastSnapshot()
	require_Error(t, err, errSnapAborted)

	// Check if the last snapshot was updated from underneath of us.
	n.Lock()
	n.snapshotting = true
	n.Unlock()
	_, err = c.LoadLastSnapshot()
	require_Error(t, err, errors.New("snapshot index mismatch"))
}

// A forced install must be able to take over from an async snapshot that's still in
// progress, otherwise a shutdown snapshot gets suppressed.
func TestNRGForcedInstallSnapshotAbortsInProgressCheckpoint(t *testing.T) {
	n, cleanup := initSingleMemRaftNode(t)
	defer cleanup()

	// Create a sample entry, the content doesn't matter, just that it's stored.
	esm := encodeStreamMsgAllowCompress("foo", "_INBOX.foo", nil, nil, 0, 0, true)
	entries := []*Entry{newEntry(EntryNormal, esm)}

	nats0 := "S1Nunr6R" // "nats-0"

	aeMsg1 := encode(t, &appendEntry{leader: nats0, term: 1, commit: 0, pterm: 0, pindex: 0, entries: entries})
	aeMsg2 := encode(t, &appendEntry{leader: nats0, term: 1, commit: 1, pterm: 1, pindex: 1, entries: entries})
	aeHeartbeat := encode(t, &appendEntry{leader: nats0, term: 1, commit: 2, pterm: 1, pindex: 2})

	n.processAppendEntry(aeMsg1, n.aesub)
	n.processAppendEntry(aeMsg2, n.aesub)
	n.processAppendEntry(aeHeartbeat, n.aesub)
	n.Applied(2)

	// An async snapshot is in progress.
	c, err := n.CreateSnapshotCheckpoint(false)
	require_NoError(t, err)

	// Without forcing we can't snapshot, this is what would suppress a shutdown snapshot.
	require_Error(t, n.InstallSnapshot([]byte("blocked"), false), errSnapInProgress)

	// Forcing installs directly, which aborts the checkpoint that was in progress.
	require_NoError(t, n.InstallSnapshot([]byte("fresh"), true))

	// The aborted checkpoint must not be able to install or read anything anymore.
	_, err = c.LoadLastSnapshot()
	require_Error(t, err, errSnapAborted)
	_, err = c.InstallSnapshot([]byte("stale"))
	require_Error(t, err, errSnapAborted)

	// And its late abort must not cancel anything installed after it.
	c.Abort()

	snap, err := n.loadLastSnapshot()
	require_NoError(t, err)
	require_True(t, bytes.Equal(snap.data, []byte("fresh")))

	// The aborted checkpoint targets the same file for the same term/applied index, so
	// it must not have written anything, not even a temporary file it never renamed.
	files, err := os.ReadDir(filepath.Join(n.sd, snapshotsDir))
	require_NoError(t, err)
	require_Len(t, len(files), 1)
	require_Equal(t, files[0].Name(), fmt.Sprintf(snapFileT, 1, 2))
}

// A direct install must wait for a snapshot write that's already in flight. The async
// install path releases the Raft lock while writing, and both would target the same
// file, and the same temporary file it's renamed from.
func TestNRGDirectInstallSnapshotWaitsForInFlightWrite(t *testing.T) {
	n, cleanup := initSingleMemRaftNode(t)
	defer cleanup()

	// Create a sample entry, the content doesn't matter, just that it's stored.
	esm := encodeStreamMsgAllowCompress("foo", "_INBOX.foo", nil, nil, 0, 0, true)
	entries := []*Entry{newEntry(EntryNormal, esm)}

	nats0 := "S1Nunr6R" // "nats-0"

	aeMsg1 := encode(t, &appendEntry{leader: nats0, term: 1, commit: 0, pterm: 0, pindex: 0, entries: entries})
	aeMsg2 := encode(t, &appendEntry{leader: nats0, term: 1, commit: 1, pterm: 1, pindex: 1, entries: entries})
	aeHeartbeat := encode(t, &appendEntry{leader: nats0, term: 1, commit: 2, pterm: 1, pindex: 2})

	n.processAppendEntry(aeMsg1, n.aesub)
	n.processAppendEntry(aeMsg2, n.aesub)
	n.processAppendEntry(aeHeartbeat, n.aesub)
	n.Applied(2)

	// Stand in for a checkpoint that's in the middle of writing its snapshot file.
	n.snapLock.Lock()

	installed := make(chan error, 1)
	go func() {
		installed <- n.InstallSnapshot([]byte("fresh"), true)
	}()

	select {
	case err := <-installed:
		t.Fatalf("Expected install to wait for the in-flight snapshot write, got %v", err)
	case <-time.After(250 * time.Millisecond):
	}

	n.snapLock.Unlock()
	require_NoError(t, <-installed)

	snap, err := n.loadLastSnapshot()
	require_NoError(t, err)
	require_True(t, bytes.Equal(snap.data, []byte("fresh")))
}

// An async install must claim the snapshot write lock before it gives up the Raft lock,
// otherwise a direct install could slip in between its checks and its write.
func TestNRGCheckpointInstallSnapshotWaitsForInFlightWrite(t *testing.T) {
	n, cleanup := initSingleMemRaftNode(t)
	defer cleanup()

	// Create a sample entry, the content doesn't matter, just that it's stored.
	esm := encodeStreamMsgAllowCompress("foo", "_INBOX.foo", nil, nil, 0, 0, true)
	entries := []*Entry{newEntry(EntryNormal, esm)}

	nats0 := "S1Nunr6R" // "nats-0"

	aeMsg1 := encode(t, &appendEntry{leader: nats0, term: 1, commit: 0, pterm: 0, pindex: 0, entries: entries})
	aeMsg2 := encode(t, &appendEntry{leader: nats0, term: 1, commit: 1, pterm: 1, pindex: 1, entries: entries})
	aeHeartbeat := encode(t, &appendEntry{leader: nats0, term: 1, commit: 2, pterm: 1, pindex: 2})

	n.processAppendEntry(aeMsg1, n.aesub)
	n.processAppendEntry(aeMsg2, n.aesub)
	n.processAppendEntry(aeHeartbeat, n.aesub)
	n.Applied(2)

	c, err := n.CreateSnapshotCheckpoint(false)
	require_NoError(t, err)

	snapDir := filepath.Join(n.sd, snapshotsDir)

	// Stand in for another snapshot write that's already in flight.
	n.snapLock.Lock()

	installed := make(chan error, 1)
	go func() {
		_, err := c.InstallSnapshot([]byte("async"))
		installed <- err
	}()

	// Must not have written anything yet, not even a temporary file.
	time.Sleep(250 * time.Millisecond)
	files, err := os.ReadDir(snapDir)
	require_NoError(t, err)
	require_Len(t, len(files), 0)

	n.snapLock.Unlock()
	require_NoError(t, <-installed)

	snap, err := n.loadLastSnapshot()
	require_NoError(t, err)
	require_True(t, bytes.Equal(snap.data, []byte("async")))
}

func TestNRGSnapshotCheckpointNodeClosed(t *testing.T) {
	for _, test := range []struct {
		title string
		close func(n *raft)
	}{
		{title: "Stop", close: func(n *raft) { n.Stop() }},
		{title: "Delete", close: func(n *raft) { n.Delete() }},
	} {
		t.Run(test.title, func(t *testing.T) {
			n, cleanup := initSingleMemRaftNode(t)
			defer cleanup()

			// Create a sample entry, the content doesn't matter, just that it's stored.
			esm := encodeStreamMsgAllowCompress("foo", "_INBOX.foo", nil, nil, 0, 0, true)
			entries := []*Entry{newEntry(EntryNormal, esm)}

			nats0 := "S1Nunr6R" // "nats-0"

			aeMsg := encode(t, &appendEntry{leader: nats0, term: 1, commit: 0, pterm: 0, pindex: 0, entries: entries})
			aeHeartbeat := encode(t, &appendEntry{leader: nats0, term: 1, commit: 1, pterm: 1, pindex: 1, entries: nil})

			n.processAppendEntry(aeMsg, n.aesub)
			n.processAppendEntry(aeHeartbeat, n.aesub)
			require_Equal(t, n.commit, 1)
			n.Applied(1)
			require_Equal(t, n.applied, 1)

			// Create the checkpoint while the node is still healthy.
			c, err := n.CreateSnapshotCheckpoint(false)
			require_NoError(t, err)

			// Stop/delete the node.
			test.close(n)

			// All checkpoint paths must now report the node as closed.
			_, err = c.LoadLastSnapshot()
			require_Error(t, err, errNodeClosed)
			var count int
			for _, err = range c.AppendEntriesSeq() {
				count++
				require_Error(t, err, errNodeClosed)
			}
			require_Equal(t, count, 1) // Must always iterate at least once to retrieve the error.
			_, err = c.InstallSnapshot(nil)
			require_Error(t, err, errNodeClosed)
		})
	}
}

func TestNRGDrainAndReplaySnapshotNodeClosed(t *testing.T) {
	for _, test := range []struct {
		title  string
		close  func(n *raft)
		replay bool
	}{
		{title: "Stop", close: func(n *raft) { n.Stop() }, replay: true},
		{title: "Delete", close: func(n *raft) { n.Delete() }, replay: false},
	} {
		t.Run(test.title, func(t *testing.T) {
			n, cleanup := initSingleMemRaftNode(t)
			defer cleanup()

			// Create a sample entry, the content doesn't matter, just that it's stored.
			esm := encodeStreamMsgAllowCompress("foo", "_INBOX.foo", nil, nil, 0, 0, true)
			entries := []*Entry{newEntry(EntryNormal, esm)}

			nats0 := "S1Nunr6R" // "nats-0"

			// Timeline, the second message ups the pterm so we can snapshot on the first.
			aeMsg1 := encode(t, &appendEntry{leader: nats0, term: 1, commit: 0, pterm: 0, pindex: 0, entries: entries})
			aeHeartbeat := encode(t, &appendEntry{leader: nats0, term: 1, commit: 1, pterm: 1, pindex: 1, entries: nil})
			aeMsg2 := encode(t, &appendEntry{leader: nats0, term: 2, commit: 1, pterm: 1, pindex: 1, entries: entries})

			n.processAppendEntry(aeMsg1, n.aesub)
			n.processAppendEntry(aeHeartbeat, n.aesub)
			require_Equal(t, n.commit, 1)
			n.Applied(1)
			n.processAppendEntry(aeMsg2, n.aesub)

			// Snapshot, so we have something to replay.
			require_NoError(t, n.InstallSnapshot(nil, false))
			require_True(t, n.snapfile != _EMPTY_)
			sfile := n.snapfile
			sdata, err := os.ReadFile(sfile)
			require_NoError(t, err)

			test.close(n)

			if test.replay {
				require_True(t, n.snapfile != _EMPTY_)
				require_True(t, n.DrainAndReplaySnapshot())
				return
			}

			// Delete must not leave us pointing at a file it just removed.
			require_Equal(t, n.snapfile, _EMPTY_)
			require_False(t, n.DrainAndReplaySnapshot())

			// Snapshot filenames are only "snap.<term>.<index>", so a later
			// incarnation of a same-named group can recreate this exact path.
			// Even then a deleted node must refuse to replay, or we'd adopt
			// another generation's state.
			require_NoError(t, os.MkdirAll(filepath.Dir(sfile), defaultDirPerms))
			require_NoError(t, os.WriteFile(sfile, sdata, defaultFilePerms))
			n.snapfile = sfile
			require_False(t, n.DrainAndReplaySnapshot())
		})
	}
}

func TestNRGInstallSnapshotForce(t *testing.T) {
	n, cleanup := initSingleMemRaftNode(t)
	defer cleanup()

	// Create a sample entry, the content doesn't matter, just that it's stored.
	esm := encodeStreamMsgAllowCompress("foo", "_INBOX.foo", nil, nil, 0, 0, true)
	entries := []*Entry{newEntry(EntryNormal, esm)}

	nats0 := "S1Nunr6R" // "nats-0"

	// Timeline
	aeMsg := encode(t, &appendEntry{leader: nats0, term: 1, commit: 0, pterm: 0, pindex: 0, entries: entries})
	aeHeartbeat := encode(t, &appendEntry{leader: nats0, term: 1, commit: 1, pterm: 1, pindex: 1, entries: nil})

	n.processAppendEntry(aeMsg, n.aesub)
	require_Equal(t, n.pindex, 1)
	require_Equal(t, n.commit, 0)
	n.processAppendEntry(aeHeartbeat, n.aesub)
	require_Equal(t, n.pindex, 1)
	require_Equal(t, n.commit, 1)
	n.Applied(1)
	require_Equal(t, n.applied, 1)

	// Block snapshots unless they're forced to skip in-progress catchups.
	n.progress = make(map[string]*ipQueue[uint64])
	n.progress["blockSnapshots"] = newIPQueue[uint64](n.s, "blockSnapshots")

	require_Error(t, n.InstallSnapshot(nil, false), errCatchupsRunning)
	require_Equal(t, n.papplied, 0)

	require_NoError(t, n.InstallSnapshot(nil, true))
	require_Equal(t, n.papplied, 1)
}

func TestNRGInstallSnapshotFromCheckpointAfterTruncateToSnapshot(t *testing.T) {
	n, cleanup := initSingleMemRaftNode(t)
	defer cleanup()

	// Create a sample entry, the content doesn't matter, just that it's stored.
	esm := encodeStreamMsgAllowCompress("foo", "_INBOX.foo", nil, nil, 0, 0, true)
	entries := []*Entry{newEntry(EntryNormal, esm)}

	nats0 := "S1Nunr6R" // "nats-0"

	// Timeline
	aeMsg1 := encode(t, &appendEntry{leader: nats0, term: 1, commit: 0, pterm: 0, pindex: 0, entries: entries})
	aeHeartbeat1 := encode(t, &appendEntry{leader: nats0, term: 1, commit: 1, pterm: 1, pindex: 1, entries: nil})
	aeMsg2 := encode(t, &appendEntry{leader: nats0, term: 1, commit: 100, pterm: 1, pindex: 100, entries: entries})
	aeHeartbeat2 := encode(t, &appendEntry{leader: nats0, term: 1, commit: 101, pterm: 1, pindex: 101, entries: nil})

	// Process the first message.
	n.processAppendEntry(aeMsg1, n.aesub)
	require_Equal(t, n.pindex, 1)
	require_Equal(t, n.commit, 0)
	n.processAppendEntry(aeHeartbeat1, n.aesub)
	require_Equal(t, n.pindex, 1)
	require_Equal(t, n.commit, 1)
	n.Applied(1)
	require_Equal(t, n.applied, 1)

	// Install a snapshot as normal to set up papplied.
	require_NoError(t, n.InstallSnapshot(nil, false))
	require_Equal(t, n.papplied, 1)

	// Simulate being caught up from a leader with a snapshot.
	snap := &snapshot{
		lastTerm:  1,
		lastIndex: 100,
		peerstate: encodePeerState(n.currentPeerState()),
		data:      []byte("catchup"),
	}
	n.pterm = snap.lastTerm
	n.pindex = snap.lastIndex
	n.commit = snap.lastIndex
	require_NoError(t, n.installSnapshot(snap))
	require_Equal(t, n.papplied, 100)
	require_Equal(t, n.applied, 1)

	// Simulate uncommitted entries being truncated up to the above snapshot.
	n.truncateWAL(1, 100)
	require_Equal(t, n.papplied, 100)

	// We haven't applied anything since the previous snapshot, so should error.
	_, err := n.CreateSnapshotCheckpoint(false)
	require_Error(t, err, errNoSnapAvailable)

	// We have only applied the previous snapshot, so should still error as there's nothing (new) to snapshot.
	n.Applied(100)
	require_Equal(t, n.applied, 100)
	_, err = n.CreateSnapshotCheckpoint(false)
	require_Error(t, err, errNoSnapAvailable)

	// Process a second message such that we can snapshot.
	n.processAppendEntry(aeMsg2, n.aesub)
	require_Equal(t, n.pindex, 101)
	require_Equal(t, n.commit, 100)
	n.processAppendEntry(aeHeartbeat2, n.aesub)
	require_Equal(t, n.pindex, 101)
	require_Equal(t, n.commit, 101)
	n.Applied(101)
	require_Equal(t, n.applied, 101)

	// The checkpoint should function, load the previous/catchup snapshot and load above entry.
	c, err := n.CreateSnapshotCheckpoint(false)
	require_NoError(t, err)

	buf, err := c.LoadLastSnapshot()
	require_NoError(t, err)
	require_True(t, bytes.Equal(buf, []byte("catchup")))

	var count int
	for ae, err := range c.AppendEntriesSeq() {
		count++
		require_NoError(t, err)
		require_Equal(t, ae.pindex, 100)
	}
	require_Equal(t, count, 1)
}

func TestNRGCheckpointInstallSnapshotAbortDuringWrite(t *testing.T) {
	type tc struct {
		name       string
		preinstall func(n *raft, cp *checkpoint)
		check      func(t *testing.T, n *raft, cp *checkpoint)
	}
	cases := []tc{
		{
			name:       "RemoveOrphan",
			preinstall: func(*raft, *checkpoint) {},
			check: func(t *testing.T, _ *raft, cp *checkpoint) {
				if _, err := os.Stat(cp.snapFile); !os.IsNotExist(err) {
					t.Fatalf("orphan snapshot file remains at %q (stat err=%v)", cp.snapFile, err)
				}
			},
		},
		{
			name: "KeepAdoptedSnapshot",
			preinstall: func(n *raft, cp *checkpoint) {
				n.Lock()
				n.snapfile = cp.snapFile
				n.Unlock()
			},
			check: func(t *testing.T, n *raft, cp *checkpoint) {
				n.RLock()
				sf := n.snapfile
				n.RUnlock()
				require_Equal(t, sf, cp.snapFile)
				if _, err := os.Stat(cp.snapFile); err != nil {
					t.Fatalf("adopted snapshot file was removed: %v", err)
				}
			},
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			n, cleanup := initSingleMemRaftNode(t)
			defer cleanup()

			esm := encodeStreamMsgAllowCompress("foo", "_INBOX.foo", nil, nil, 0, 0, true)
			entries := []*Entry{newEntry(EntryNormal, esm)}
			nats0 := "S1Nunr6R" // "nats-0"

			aeMsg := encode(t, &appendEntry{leader: nats0, term: 1, commit: 0, pterm: 0, pindex: 0, entries: entries})
			aeHB := encode(t, &appendEntry{leader: nats0, term: 1, commit: 1, pterm: 1, pindex: 1, entries: nil})
			n.processAppendEntry(aeMsg, n.aesub)
			n.processAppendEntry(aeHB, n.aesub)
			n.Applied(1)
			require_Equal(t, n.applied, 1)

			ck, err := n.CreateSnapshotCheckpoint(false)
			require_NoError(t, err)
			cp := ck.(*checkpoint)
			c.preinstall(n, cp)

			// Drain dios so writeFileWithSync parks inside writeAtomically.
			drained := 0
		drain:
			for {
				select {
				case <-n.dios.ch:
					drained++
				default:
					break drain
				}
			}
			refill := func() {
				for i := 0; i < drained; i++ {
					n.dios.ch <- struct{}{}
				}
				drained = 0
			}
			defer refill()

			errCh := make(chan error, 1)
			go func() {
				_, err := ck.InstallSnapshot([]byte("data"))
				errCh <- err
			}()

			// 100ms is empirical: enough for the writer to reach <-dios on any
			// reasonable host. With dios drained it cannot progress past this
			// point, so the sanity check below would catch a too-short wait.
			time.Sleep(100 * time.Millisecond)
			select {
			case err = <-errCh:
				t.Fatalf("writer returned before dios refill: %v", err)
			default:
			}

			n.Lock()
			require_True(t, n.snapshotting)
			n.snapshotting = false
			n.Unlock()

			refill()

			select {
			case err = <-errCh:
				require_Error(t, err, errSnapAborted)
			case <-time.After(5 * time.Second):
				t.Fatal("c.InstallSnapshot did not return")
			}

			c.check(t, n, cp)
		})
	}
}

func TestNRGSwitchToCandidateResetsVote(t *testing.T) {
	n, cleanup := initSingleMemRaftNode(t)
	defer cleanup()

	nats0 := "S1Nunr6R" // "nats-0"

	require_Equal(t, n.term, 0)
	n.vote = nats0
	n.switchToCandidate()

	// Confirm the term was incremented and the vote reset.
	require_Equal(t, n.term, 1)
	require_Equal(t, n.vote, _EMPTY_)

	// Confirm this was written to disk.
	term, voted, err := n.readTermVote()
	require_NoError(t, err)
	require_Equal(t, term, 1)
	require_Equal(t, voted, _EMPTY_)
}

func TestNRGInitSingleMemRaftNodeDefaults(t *testing.T) {
	n, cleanup := initSingleMemRaftNode(t)
	defer cleanup()
	require_Equal(t, n.ID(), "esFhDys3")
	require_Equal(t, len(n.Peers()), 1)
	require_Equal(t, n.Peers()[0].ID, "esFhDys3")
	require_Equal(t, n.ClusterSize(), 1)
	require_True(t, n.Quorum())
}

func TestNRGReplayAddPeerKeepsClusterSize(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "R3S", 3)
	s := c.servers[0]
	defer c.shutdown()

	storeDir := t.TempDir()
	fcfg := FileStoreConfig{
		StoreDir:   storeDir,
		BlockSize:  defaultMediumBlockSize,
		AsyncFlush: false,
		srv:        s,
	}
	scfg := StreamConfig{Name: "RAFT", Storage: FileStorage}
	fs, err := newFileStore(fcfg, scfg)
	require_NoError(t, err)

	cfg := &RaftConfig{Name: "TEST", Store: storeDir, Log: fs}

	// Seed peer state as a 3-node group.
	s.mu.RLock()
	self := s.sys.shash[:idLen]
	s.mu.RUnlock()
	peerA, peerB := "yrzKKRBu", "cnrtt3eg"
	require_NoError(t, s.bootstrapRaftNode(cfg, []string{self, peerA, peerB}, true))

	n, err := s.initRaftNode(globalAccountName, cfg, pprofLabels{})
	require_NoError(t, err)
	require_Equal(t, n.ClusterSize(), 3)

	// Store a normal entry and a EntryAddPeer and commit them.
	// The latter triggered the bug at restart.
	esm := encodeStreamMsgAllowCompress("foo", "_INBOX.foo", nil, nil, 0, 0, true)
	aeMsg1 := encode(t, &appendEntry{
		leader:  self,
		term:    2,
		commit:  0,
		pterm:   0,
		pindex:  0,
		entries: []*Entry{newEntry(EntryNormal, esm)},
	})
	aeMsg2 := encode(t, &appendEntry{
		leader:  self,
		term:    2,
		commit:  2,
		pterm:   2,
		pindex:  1,
		entries: []*Entry{newEntry(EntryAddPeer, []byte(peerA))},
	})
	n.processAppendEntry(aeMsg1, n.aesub)
	n.processAppendEntry(aeMsg2, n.aesub)

	// Restart from disk, forcing replay of the two WAL entries.
	n.Stop()
	n.WaitForStop()
	fs.Stop()

	fs, err = newFileStore(fcfg, scfg)
	require_NoError(t, err)
	cfg = &RaftConfig{Name: "TEST", Store: storeDir, Log: fs}
	n, err = s.initRaftNode(globalAccountName, cfg, pprofLabels{})
	require_NoError(t, err)

	// If bug is present, cluster size is 1 instead of 3.
	// EntryAddPeer would increases the initial cluster
	// of size of 0 to 1. initRaftNode() would then add
	// the remaining peers from peer state file, but would
	// not adjust cluster size and quorum accordingly.
	// This could lead to erroneously create singleton
	// clusters after restart.
	require_Equal(t, n.ClusterSize(), 3)
	require_Equal(t, n.qn, 2)

	n.Stop()
	n.WaitForStop()
	fs.Stop()
}

func TestNRGSnapshotExcludesUncommittedMembershipChange(t *testing.T) {
	esm := encodeStreamMsgAllowCompress("foo", "_INBOX.foo", nil, nil, 0, 0, true)

	t.Run("AddPeer", func(t *testing.T) {
		n, cleanup := initSingleMemRaftNode(t)
		defer cleanup()

		nats0 := "S1Nunr6R"   // "nats-0"
		newPeer := "yrzKKRBu" // "nats-1"

		n.Lock()
		n.addPeer(nats0)
		n.Unlock()
		require_Equal(t, len(n.peers), 2)

		aeMsg := encode(t, &appendEntry{leader: nats0, term: 1, commit: 0, pterm: 0, pindex: 0, entries: []*Entry{newEntry(EntryNormal, esm)}})
		aeAddPeer := encode(t, &appendEntry{leader: nats0, term: 1, commit: 1, pterm: 1, pindex: 1, entries: []*Entry{newEntry(EntryAddPeer, []byte(newPeer))}})
		n.processAppendEntry(aeMsg, n.aesub)
		n.processAppendEntry(aeAddPeer, n.aesub)

		// The peer is tracked speculatively and the change is inflight at index 2.
		require_Equal(t, n.commit, 1)
		_, ok := n.peers[newPeer]
		require_True(t, ok)
		require_Equal(t, n.csz, 3)
		require_NotNil(t, n.membChange)
		require_Equal(t, n.membChange.index, 2)

		// Apply the committed normal entry, then snapshot at applied (index 1).
		n.Applied(1)
		require_NoError(t, n.InstallSnapshot(nil, false))

		// The snapshot's peer state must reflect committed membership only,
		// i.e. it must not contain the not-yet-committed peer.
		snap, err := n.loadLastSnapshot()
		require_NoError(t, err)
		require_Equal(t, snap.lastIndex, 1)
		ps, err := decodePeerState(snap.peerstate)
		require_NoError(t, err)
		require_Equal(t, ps.clusterSize, 2)
		require_Len(t, len(ps.knownPeers), 2)
		require_False(t, slices.Contains(ps.knownPeers, newPeer))
	})

	t.Run("RemovePeer", func(t *testing.T) {
		n, cleanup := initSingleMemRaftNode(t)
		defer cleanup()

		nats0 := "S1Nunr6R"   // "nats-0"
		oldPeer := "yrzKKRBu" // "nats-1"

		n.Lock()
		n.addPeer(nats0)
		n.addPeer(oldPeer)
		n.Unlock()
		require_Equal(t, len(n.peers), 3)

		aeMsg := encode(t, &appendEntry{leader: nats0, term: 1, commit: 0, pterm: 0, pindex: 0, entries: []*Entry{newEntry(EntryNormal, esm)}})
		aeRemovePeer := encode(t, &appendEntry{leader: nats0, term: 1, commit: 1, pterm: 1, pindex: 1, entries: []*Entry{newEntry(EntryRemovePeer, []byte(oldPeer))}})
		n.processAppendEntry(aeMsg, n.aesub)
		n.processAppendEntry(aeRemovePeer, n.aesub)

		// The peer is removed speculatively and the change is inflight at index 2.
		require_Equal(t, n.commit, 1)
		_, ok := n.peers[oldPeer]
		require_False(t, ok)
		require_Equal(t, n.csz, 2)
		require_NotNil(t, n.membChange)
		require_Equal(t, n.membChange.index, 2)

		// Apply the committed normal entry, then snapshot at applied (index 1).
		n.Applied(1)
		require_NoError(t, n.InstallSnapshot(nil, false))

		// The snapshot's peer state must reflect committed membership only,
		// i.e. it must still include the not-yet-removed peer.
		snap, err := n.loadLastSnapshot()
		require_NoError(t, err)
		require_Equal(t, snap.lastIndex, 1)
		ps, err := decodePeerState(snap.peerstate)
		require_NoError(t, err)
		require_Equal(t, ps.clusterSize, 3)
		require_Len(t, len(ps.knownPeers), 3)
		require_True(t, slices.Contains(ps.knownPeers, oldPeer))
	})
}

func TestNRGDontSwitchToCandidateWithInflightSelfMembershipChange(t *testing.T) {
	n, cleanup := initSingleMemRaftNode(t)
	defer cleanup()

	esm := encodeStreamMsgAllowCompress("foo", "_INBOX.foo", nil, nil, 0, 0, true)

	nats0 := "S1Nunr6R" // "nats-0"
	nats1 := "yrzKKRBu" // "nats-1"

	n.Lock()
	n.addPeer(nats0)
	n.addPeer(nats1)
	n.Unlock()
	require_Equal(t, len(n.peers), 3)

	// Receive an uncommitted EntryRemovePeer that targets us.
	aeMsg := encode(t, &appendEntry{leader: nats0, term: 1, commit: 0, pterm: 0, pindex: 0, entries: []*Entry{newEntry(EntryNormal, esm)}})
	aeRemoveSelf := encode(t, &appendEntry{leader: nats0, term: 1, commit: 1, pterm: 1, pindex: 1, entries: []*Entry{newEntry(EntryRemovePeer, []byte(n.id))}})
	n.processAppendEntry(aeMsg, n.aesub)
	n.processAppendEntry(aeRemoveSelf, n.aesub)

	// We've speculatively removed ourselves, and the change is inflight at index 2.
	_, ok := n.peers[n.id]
	require_False(t, ok)
	require_NotNil(t, n.membChange)
	require_Equal(t, n.membChange.peer, n.id)
	require_Equal(t, n.membChange.index, 2)

	// We must not become a candidate while our own removal is uncommitted.
	n.Applied(1)
	n.switchToCandidate()
	require_Equal(t, n.State(), Follower)

	// Truncate the uncommitted removal back to the committed index. This
	// restores us to our own peer set and clears the inflight change.
	n.Lock()
	n.truncateWAL(1, 1)
	n.Unlock()
	_, ok = n.peers[n.id]
	require_True(t, ok)
	require_True(t, n.membChange == nil)

	// Now that we're a settled member again, we can campaign.
	n.switchToCandidate()
	require_Equal(t, n.State(), Candidate)
}

func TestNRGTrackPeerLag(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "R3S", 3)
	defer c.shutdown()

	rg := c.createRaftGroup("TEST", 3, newStateAdder)
	lsm := rg.waitOnLeader()
	require_NotNil(t, lsm)
	ls := lsm.(*stateAdder)

	checkPeerData := func() {
		for _, sm := range rg {
			rn := sm.node().(*raft)
			rn.RLock()
			leader := rn.leader
			isLeader := RaftState(rn.state.Load()) == Leader
			for id, ps := range rn.peers {
				// If this peer is the leader (but not ourselves), then these values need to be populated.
				if isLeader && id != rn.id {
					if li := ps.li; li == 0 {
						rn.RUnlock()
						t.Fatal("require last replicated index to be set")
					}
					if ts := ps.ts; ts.IsZero() {
						rn.RUnlock()
						t.Fatal("require last seen timestamp to be set")
					}
					continue
				}
				if li := ps.li; li != 0 {
					rn.RUnlock()
					t.Fatalf("require equal, but got: %v != %v", ps.li, 0)
				}
				if ts := ps.ts; id != rn.leader && !ts.IsZero() {
					rn.RUnlock()
					t.Fatalf("require zero ts, but got: %v", ts)
				}
			}
			rn.RUnlock()

			// This test expects no lag and all nodes as current (except for followers as seen by other followers).
			for _, ps := range rn.Peers() {
				require_Equal(t, ps.Lag, 0)
				require_Equal(t, ps.Current, ps.ID == rn.id || ps.ID == leader || isLeader)
			}
		}
	}

	for i := range 5 {
		ls.proposeDelta(10)
		rg.waitOnTotal(t, int64(10*(i+1)))
	}
	checkPeerData()

	require_NoError(t, lsm.node().StepDown())
	lsm = rg.waitOnLeader()
	require_NotNil(t, lsm)
	ls = lsm.(*stateAdder)

	for i := range 5 {
		ls.proposeDelta(10)
		rg.waitOnTotal(t, int64(50+10*(i+1)))
	}
	checkPeerData()
}

func TestNRGPeersResponse(t *testing.T) {
	// As a leader.
	rn := &raft{
		id:     "me",
		leader: "me",
		peers: map[string]*lps{
			"me":    {li: 0},
			"peer1": {li: 10},
			"peer2": {li: 3},
			"peer3": {li: 0},
		},
		pindex:  10,
		commit:  10,
		applied: 10,
	}
	rn.state.Store(int32(Leader))
	for _, ps := range rn.Peers() {
		switch ps.ID {
		case "me":
			require_Equal(t, ps.Lag, 0)
			require_True(t, ps.Current)
		case "peer1":
			require_Equal(t, ps.Lag, 0)
			require_True(t, ps.Current)
		case "peer2":
			require_Equal(t, ps.Lag, 7) // We persisted 10 entries, they have only 3.
			require_False(t, ps.Current)
		case "peer3":
			require_Equal(t, ps.Lag, 10) // We persisted 10 entries, they have none.
			require_False(t, ps.Current)
		default:
			t.Fatalf("unexpected peer ID: %s", ps.ID)
		}
	}

	// As a follower.
	rn = &raft{
		id:     "me",
		leader: "leader",
		peers: map[string]*lps{
			"me":     {li: 0},
			"leader": {li: 0},
			"peer":   {li: 0},
		},
	}
	rn.state.Store(int32(Follower))
	for _, ps := range rn.Peers() {
		switch ps.ID {
		case "me":
			require_Equal(t, ps.Lag, 0)
			require_True(t, ps.Current)
		case "leader":
			require_Equal(t, ps.Lag, 0)
			require_False(t, ps.Current)
		default:
			// As a follower ourselves, we don't know about the state of other followers.
			require_Equal(t, ps.Lag, 0)
			require_False(t, ps.Current)
		}
	}

	// If we've heard from the leader recently, we report it as current.
	rn.peers["leader"].ts = time.Now()
	for _, ps := range rn.Peers() {
		if ps.ID == "leader" {
			require_Equal(t, ps.Lag, 0)
			require_True(t, ps.Current)
		}
	}
}

func TestNRGOnlyCommitIfCurrentTerm(t *testing.T) {
	n, cleanup := initSingleMemRaftNode(t)
	defer cleanup()

	// Create a sample entry, the content doesn't matter, just that it's stored.
	esm := encodeStreamMsgAllowCompress("foo", "_INBOX.foo", nil, nil, 0, 0, true)
	entries := []*Entry{newEntry(EntryNormal, esm)}

	s1 := getHash("S-1")
	s2 := getHash("S-2")
	s3 := getHash("S-3")

	n.Lock()
	n.addPeer(s2)
	n.addPeer(s3)
	n.Unlock()
	require_Len(t, len(n.Peers()), 3)

	// The below timeline describes a test ensuring a leader doesn't commit entries from previous terms.

	// This server became leader twice and stored two entries.
	aeMsg1 := encode(t, &appendEntry{leader: s1, term: 1, commit: 0, pterm: 0, pindex: 0, entries: entries})
	require_NoError(t, n.storeToWAL(aeMsg1))
	aeMsg2 := encode(t, &appendEntry{leader: s1, term: 2, commit: 0, pterm: 1, pindex: 1, entries: entries})
	require_NoError(t, n.storeToWAL(aeMsg2))

	// Another server became leader for term 3 without us knowing and stored a different entry at index 2.
	// We now become leader again and store a new entry at index 3, but unaware of the above term 3 entry.
	aeMsg3 := encode(t, &appendEntry{leader: s1, term: 4, commit: 0, pterm: 2, pindex: 2, entries: entries})
	require_NoError(t, n.storeToWAL(aeMsg3))

	// Set up leader state for below response handling.
	n.switchToLeader()
	n.term = 4
	require_Equal(t, n.commit, 0)

	// When we get quorum on the first two entries this doesn't count toward upping the commit yet.
	n.processAppendEntryResponse(&appendEntryResponse{term: 1, index: 1, peer: s2, success: true})
	require_Equal(t, n.commit, 0)
	n.processAppendEntryResponse(&appendEntryResponse{term: 2, index: 2, peer: s2, success: true})
	require_Equal(t, n.commit, 0)
	// Only once we receive quorum on an entry of our current term do we count this toward upping the commit.
	// If we wouldn't have waited to up our commit until now, the server that was leader during term 3
	// could still have overwritten the entry at index 2, resulting in a desync.
	n.processAppendEntryResponse(&appendEntryResponse{term: 4, index: 3, peer: s2, success: true})
	require_Equal(t, n.commit, 3)
}

func TestNRGTermFencedProposals(t *testing.T) {
	n, cleanup := initSingleMemRaftNode(t)
	defer cleanup()

	// Simulate winning an election.
	n.switchToCandidate()
	n.switchToLeader()
	require_Equal(t, n.State(), Leader)
	term := n.Term()
	require_Equal(t, term, 1)

	// Proposals with the current term are accepted.
	require_NoError(t, n.Propose(term, nil))
	require_NoError(t, n.ProposeMulti(term, []*Entry{newEntry(EntryNormal, nil)}))

	// Proposals with a stale or future term are rejected, even though we are leader.
	// Term 0 can never be a valid proposing term, elected leaders always have term >= 1.
	require_Error(t, n.Propose(0, nil), errNotLeader)
	require_Error(t, n.Propose(term+1, nil), errNotLeader)
	require_Error(t, n.ProposeMulti(0, []*Entry{newEntry(EntryNormal, nil)}), errNotLeader)
	require_Error(t, n.ProposeMulti(term+1, []*Entry{newEntry(EntryNormal, nil)}), errNotLeader)
	require_Equal(t, n.State(), Leader)

	// Losing leadership signals the upper layer with the term the loss happened in.
	n.stepdown(noLeader)
	select {
	case lc := <-n.LeadChangeC():
		require_False(t, lc.isLeader)
		require_Equal(t, lc.term, term)
	default:
		t.Fatal("Expected leadChange signal")
	}

	// Get re-elected at a higher term.
	checkFor(t, 2*time.Second, 100*time.Millisecond, func() error {
		n.RLock()
		prop, commit, processed := n.prop.len(), n.commit, n.processed
		n.RUnlock()
		if prop > 0 {
			return fmt.Errorf("expected prop to be 0, got %d", prop)
		}
		if processed >= commit {
			return nil
		}
		n.Applied(commit)
		return fmt.Errorf("processed %d < commit %d", processed, commit)
	})
	n.switchToCandidate()
	n.switchToLeader()
	newTerm := n.Term()
	require_Equal(t, newTerm, term+1)

	// A stale process still holding the previous epoch's term must not be able
	// to propose into the new epoch.
	require_Error(t, n.Propose(term, nil), errNotLeader)
	require_Error(t, n.ProposeMulti(term, []*Entry{newEntry(EntryNormal, nil)}), errNotLeader)

	// Proposals with the new term are accepted, and forwarded proposals always
	// use the node's current term.
	require_NoError(t, n.Propose(newTerm, nil))
	require_NoError(t, n.ForwardProposal(nil))
}

func TestNRGLeaderStepsDownIfOverrun(t *testing.T) {
	n, cleanup := initSingleMemRaftNode(t)
	defer cleanup()

	// Proposals are only accepted if we're the leader.
	term := n.Term()
	require_Equal(t, term, 0)
	require_Error(t, n.Propose(term, nil), errNotLeader)
	require_Error(t, n.ProposeMulti(term, nil), errNotLeader)

	// Simulate winning an election.
	n.switchToCandidate()
	n.switchToLeader()
	term = n.Term()
	require_Equal(t, term, 1)
	require_NoError(t, n.Propose(term, nil))
	require_NoError(t, n.ProposeMulti(term, nil))
	require_Equal(t, n.State(), Leader)

	// Too many uncommitted entries in our log.
	t.Run("Uncommitted", func(t *testing.T) {
		n.state.Store(int32(Leader))

		// Proposing while we're exactly at the threshold still succeeds.
		n.pindex, n.commit, n.applied = pauseQuorumThreshold, 0, 0
		require_NoError(t, n.Propose(term, nil))
		require_NoError(t, n.ProposeMulti(term, nil))
		require_Equal(t, n.State(), Leader)

		// While we're over the threshold, we temporarily don't send proposals.
		n.pindex, n.commit, n.applied = pauseQuorumThreshold+1, 0, 0
		require_Error(t, n.Propose(term, nil), errNotLeader)
		require_Equal(t, n.State(), Follower)

		n.state.Store(int32(Leader))
		require_Error(t, n.ProposeMulti(term, nil), errNotLeader)
		require_Equal(t, n.State(), Follower)
	})

	// Too many unapplied entries in our log and in-memory.
	t.Run("Unapplied", func(t *testing.T) {
		n.state.Store(int32(Leader))

		// Proposing while we're exactly at the threshold still succeeds.
		n.pindex, n.commit, n.applied = pauseQuorumThreshold, pauseQuorumThreshold, 0
		require_NoError(t, n.Propose(term, nil))
		require_NoError(t, n.ProposeMulti(term, nil))
		require_Equal(t, n.State(), Leader)

		// While we're over the threshold, we temporarily don't send proposals.
		n.pindex, n.commit, n.applied = pauseQuorumThreshold+1, pauseQuorumThreshold+1, 0
		require_Error(t, n.Propose(term, nil), errNotLeader)
		require_Equal(t, n.State(), Follower)

		n.state.Store(int32(Leader))
		require_Error(t, n.ProposeMulti(term, nil), errNotLeader)
		require_Equal(t, n.State(), Follower)
	})
}

func TestNRGFollowerPausesQuorumIfOverrun(t *testing.T) {
	test := func(t *testing.T, new bool) {
		n, cleanup := initSingleMemRaftNode(t)
		defer cleanup()

		aeReply := "$TEST"
		nc, err := nats.Connect(n.s.ClientURL(), nats.UserInfo("admin", "s3cr3t!"))
		require_NoError(t, err)
		defer nc.Close()

		sub, err := nc.SubscribeSync(aeReply)
		require_NoError(t, err)
		defer sub.Drain()
		require_NoError(t, nc.Flush())

		// Timeline
		nats0 := "S1Nunr6R" // "nats-0"
		aeHeartbeat := encode(t, &appendEntry{leader: nats0, term: 1, commit: 0, pterm: 0, pindex: 0, entries: nil, reply: aeReply})

		aesub := n.aesub
		if !new {
			aesub = nil
		}

		n.processAppendEntry(aeHeartbeat, aesub)
		msg, err := sub.NextMsg(200 * time.Millisecond)
		if new {
			require_NoError(t, err)
			ar := decodeAppendEntryResponse(msg.Data)
			require_True(t, ar.success)
			require_Equal(t, msg.Reply, _EMPTY_)
		} else {
			require_Error(t, err, nats.ErrTimeout)
		}

		// Processing while we're exactly at the threshold still succeeds.
		n.commit, n.applied = pauseQuorumThreshold, 0
		n.processAppendEntry(aeHeartbeat, aesub)
		msg, err = sub.NextMsg(200 * time.Millisecond)
		if new {
			require_NoError(t, err)
			ar := decodeAppendEntryResponse(msg.Data)
			require_True(t, ar.success)
			require_Equal(t, msg.Reply, _EMPTY_)
		} else {
			require_Error(t, err, nats.ErrTimeout)
		}

		// When we're over the threshold, we pause processing until we're sufficiently below it again.
		n.commit, n.applied = pauseQuorumThreshold+1, 0
		n.processAppendEntry(aeHeartbeat, aesub)
		_, err = sub.NextMsg(200 * time.Millisecond)
		require_Error(t, err, nats.ErrTimeout)
		require_Equal(t, n.quorumPaused, new)

		// Confirm we remain paused while we're almost at the unpause threshold.
		n.applied = n.commit - paeWarnThreshold - 1
		n.processAppendEntry(aeHeartbeat, aesub)
		_, err = sub.NextMsg(200 * time.Millisecond)
		require_Error(t, err, nats.ErrTimeout)
		require_Equal(t, n.quorumPaused, new)

		// Once we're at the lower threshold, we should be unpaused and accept new entries again.
		// Afterward we'd likely catch up from a snapshot instead of normal append entries.
		n.applied = n.commit - paeWarnThreshold
		n.processAppendEntry(aeHeartbeat, aesub)
		require_False(t, n.quorumPaused)

		msg, err = sub.NextMsg(200 * time.Millisecond)
		if new {
			require_NoError(t, err)
			ar := decodeAppendEntryResponse(msg.Data)
			require_True(t, ar.success)
			require_Equal(t, msg.Reply, _EMPTY_)
		} else {
			require_Error(t, err, nats.ErrTimeout)
		}

		if new {
			// Guard against an underflow. We should unpause shortly after snapshot install.
			n.quorumPaused = true
			n.applied, n.papplied = paeWarnThreshold, paeWarnThreshold
			n.commit = 0
			n.processAppendEntry(aeHeartbeat, aesub)
			require_False(t, n.quorumPaused)

			msg, err = sub.NextMsg(200 * time.Millisecond)
			require_NoError(t, err)
			ar := decodeAppendEntryResponse(msg.Data)
			require_True(t, ar.success)
			require_Equal(t, msg.Reply, _EMPTY_)
		}
	}

	t.Run("Replay", func(t *testing.T) { test(t, false) })
	t.Run("New", func(t *testing.T) { test(t, true) })
}

func TestNRGSwitchToCandidateResetsQuorumPaused(t *testing.T) {
	n, cleanup := initSingleMemRaftNode(t)
	defer cleanup()

	// Manually set the quorumPaused flag as if the follower was overrun.
	n.quorumPaused = true
	require_Equal(t, n.term, 0)

	// Switching to a candidate has a similar prerequisite to check all committed entries
	// have been applied. We can therefore reset quorumPaused.
	n.switchToCandidate()
	require_Equal(t, n.term, 1)
	require_False(t, n.quorumPaused)
}

func TestNRGPausingQuorumCancelsCatchup(t *testing.T) {
	// If a follower is already catching up and it also falls far enough behind to
	// hit the quorum-pause threshold, the in-flight catchup should be canceled
	// so the leader re-sends a fresher snapshot rather than continuing to stream
	// entries we can't absorb.
	test := func(t *testing.T, alreadyPaused bool) {
		n, cleanup := initSingleMemRaftNode(t)
		defer cleanup()

		esm := encodeStreamMsgAllowCompress("foo", "_INBOX.foo", nil, nil, 0, 0, true)
		entries := []*Entry{newEntry(EntryNormal, esm)}

		nats0 := "S1Nunr6R" // "nats-0"
		nats1 := "yrzKKRBu" // "nats-1"

		// Initial entry and heartbeat from nats0 so the follower has a committed baseline.
		aeInitial := encode(t, &appendEntry{leader: nats0, term: 1, commit: 0, pterm: 0, pindex: 0, entries: entries})
		aeHeartbeat := encode(t, &appendEntry{leader: nats0, term: 1, commit: 1, pterm: 1, pindex: 1, entries: nil})

		n.processAppendEntry(aeInitial, n.aesub)
		n.processAppendEntry(aeHeartbeat, n.aesub)
		require_Equal(t, n.commit, 1)

		// Leader change. The new leader references pindex=3, which the follower
		// doesn't have, so the follower requests a catchup.
		aeCatchupTrigger := encode(t, &appendEntry{leader: nats1, term: 2, commit: 1, pterm: 2, pindex: 3, entries: entries})
		n.processAppendEntry(aeCatchupTrigger, n.aesub)
		require_True(t, n.catchup != nil)
		require_True(t, n.catchup.sub != nil)
		catchupSub := n.catchup.sub

		// Put the follower into a state where the next append entry will trip the
		// pause logic: either already paused with a backlog above the warn threshold,
		// or fresh but with a backlog above the pause threshold.
		if alreadyPaused {
			n.quorumPaused = true
			n.commit, n.applied, n.papplied = paeWarnThreshold+1, 0, 0
		} else {
			n.commit, n.applied, n.papplied = pauseQuorumThreshold+1, 0, 0
		}

		// Deliver an append entry on the catchup sub. Its contents don't matter –
		// the pause check runs before the entry is applied.
		aeCatchup := encode(t, &appendEntry{leader: nats1, term: 2, commit: 1, pterm: 1, pindex: 1, lterm: 2, entries: entries})
		n.processAppendEntry(aeCatchup, catchupSub)

		// The catchup should have been canceled, so the leader will re-send a fresher snapshot.
		require_True(t, n.catchup == nil)
		require_True(t, n.quorumPaused)
	}

	t.Run("EnteringPause", func(t *testing.T) { test(t, false) })
	t.Run("AlreadyPaused", func(t *testing.T) { test(t, true) })
}

func TestNRGPausedFollowerOverrunPausesQuorum(t *testing.T) {
	n, cleanup := initSingleMemRaftNode(t)
	defer cleanup()

	esm := encodeStreamMsgAllowCompress("foo", _EMPTY_, nil, nil, 0, 0, true)
	entries := []*Entry{newEntry(EntryNormal, esm)}
	nats0 := "S1Nunr6R" // "nats-0"

	// Applies are paused, like during a stream catchup.
	require_NoError(t, n.PauseApply())

	// The leader keeps replicating and committing well past the threshold.
	total := uint64(pauseQuorumThreshold + 10_000)
	for index := uint64(1); index <= total; index++ {
		ae := encode(t, &appendEntry{leader: nats0, term: 1, commit: index - 1, pterm: 1, pindex: index - 1, entries: entries})
		if index == 1 {
			ae.pterm = 0
		}
		n.processAppendEntry(ae, n.aesub)
	}

	n.RLock()
	pindex, commit, quorumPaused := n.pindex, n.commit, n.quorumPaused
	n.RUnlock()

	// The held commits must stop the follower from storing more.
	// The check sees the previous entry's commit, which itself lags by one.
	require_Equal(t, commit, 0)
	require_True(t, quorumPaused)
	require_Equal(t, pindex, pauseQuorumThreshold+2)

	nc, err := nats.Connect(n.s.ClientURL(), nats.UserInfo("admin", "s3cr3t!"))
	require_NoError(t, err)
	defer nc.Close()
	aeReply := "$TEST"
	sub, err := nc.SubscribeSync(aeReply)
	require_NoError(t, err)
	defer sub.Drain()
	require_NoError(t, nc.Flush())

	// While quorum is paused, a new entry still gets us to request a newer snapshot.
	next := encode(t, &appendEntry{leader: nats0, term: 1, commit: total, pterm: 1, pindex: total, entries: entries, reply: aeReply})
	n.processAppendEntry(next, n.aesub)
	msg, err := sub.NextMsg(time.Second)
	require_NoError(t, err)
	require_True(t, decodeAppendEntryResponse(msg.Data).snapshot)

	n.RLock()
	peerstate := encodePeerState(&peerState{n.peerNames(), n.csz, n.extSt})
	snapsub := n.snapsub
	n.RUnlock()
	snapEntries := []*Entry{{EntrySnapshot, []byte("snap")}, {EntryPeerState, peerstate}}
	leaderSnapshot := func(index uint64) *appendEntry {
		return encode(t, &appendEntry{leader: nats0, term: 1, commit: total, pterm: 1, pindex: index, entries: snapEntries})
	}

	// A snapshot within our log can keep quorum paused, while holding less than we'd normally request at.
	snapIndex := pindex - paeWarnThreshold - 5_000
	n.processAppendEntry(leaderSnapshot(snapIndex), snapsub)
	n.Lock()
	papplied := n.papplied
	// Skip waiting for the request interval.
	n.snapreq = time.Time{}
	n.Unlock()
	require_Equal(t, papplied, snapIndex)
	require_LessThan(t, pindex-papplied, paeDropThreshold)

	// We keep requesting while quorum is paused.
	next = encode(t, &appendEntry{leader: nats0, term: 1, commit: total, pterm: 1, pindex: total, entries: entries, reply: aeReply})
	n.processAppendEntry(next, n.aesub)
	msg, err = sub.NextMsg(time.Second)
	require_NoError(t, err)
	ar := decodeAppendEntryResponse(msg.Data)
	require_True(t, ar.snapshot)
	require_Equal(t, ar.index, snapIndex)
	n.RLock()
	quorumPaused = n.quorumPaused
	n.RUnlock()
	require_True(t, quorumPaused)

	// The leader's snapshot is past our log, our log restarts at it.
	n.processAppendEntry(leaderSnapshot(total), snapsub)

	// The next entry resumes quorum and follows the snapshot.
	next = encode(t, &appendEntry{leader: nats0, term: 1, commit: total, pterm: 1, pindex: total, entries: entries, reply: aeReply})
	n.processAppendEntry(next, n.aesub)
	msg, err = sub.NextMsg(time.Second)
	require_NoError(t, err)
	require_True(t, decodeAppendEntryResponse(msg.Data).success)
	n.RLock()
	pindex, quorumPaused = n.pindex, n.quorumPaused
	n.RUnlock()
	require_False(t, quorumPaused)
	require_Equal(t, pindex, total+1)

	// Resuming applies the snapshot instead of replaying the held commits.
	n.ResumeApply()
	ces := n.apply.pop()
	require_Len(t, len(ces), 1)
	require_Equal(t, ces[0].Index, total)
	require_Equal(t, ces[0].Entries[0].Type, EntrySnapshot)
}

func TestNRGPausedFollowerRequestsLeaderSnapshot(t *testing.T) {
	n, cleanup := initSingleMemRaftNode(t)
	defer cleanup()

	esm := encodeStreamMsgAllowCompress("foo", _EMPTY_, nil, nil, 0, 0, true)
	entries := []*Entry{newEntry(EntryNormal, esm)}
	nats0 := "S1Nunr6R" // "nats-0"

	nc, err := nats.Connect(n.s.ClientURL(), nats.UserInfo("admin", "s3cr3t!"))
	require_NoError(t, err)
	defer nc.Close()

	aeReply, snapReply := "$TEST", "$TEST.snap"
	sub, err := nc.SubscribeSync(aeReply)
	require_NoError(t, err)
	defer sub.Drain()
	ssub, err := nc.SubscribeSync(snapReply)
	require_NoError(t, err)
	defer ssub.Drain()
	require_NoError(t, nc.Flush())

	// Applies are paused, like during a stream catchup.
	require_NoError(t, n.PauseApply())

	// The leader keeps replicating and committing past the request threshold.
	// Only the last entry gets a reply, which carries our request.
	total := uint64(paeDropThreshold + 1)
	for index := uint64(1); index <= total; index++ {
		ae := &appendEntry{leader: nats0, term: 1, commit: index - 1, pterm: 1, pindex: index - 1, entries: entries}
		if index == 1 {
			ae.pterm = 0
		}
		if index == total {
			ae.reply = aeReply
		}
		n.processAppendEntry(encode(t, ae), n.aesub)
	}

	// We ask for a snapshot past ours, to be sent to our inbox.
	msg, err := sub.NextMsg(time.Second)
	require_NoError(t, err)
	require_Equal(t, len(msg.Data), snapshotRequestLen)
	ar := decodeAppendEntryResponse(msg.Data)
	require_True(t, ar.snapshot)
	require_False(t, ar.success)
	require_Equal(t, ar.index, 0)
	n.RLock()
	snapInbox := string(n.snapsub.subject)
	n.RUnlock()
	require_Equal(t, msg.Reply, snapInbox)

	// The entry's own response still follows.
	msg, err = sub.NextMsg(time.Second)
	require_NoError(t, err)
	require_True(t, decodeAppendEntryResponse(msg.Data).success)

	// The leader's snapshot arrives on our inbox and is installed.
	n.RLock()
	peerstate := encodePeerState(&peerState{n.peerNames(), n.csz, n.extSt})
	snapsub := n.snapsub
	n.RUnlock()
	snapIndex := total / 2
	snapEntries := []*Entry{{EntrySnapshot, []byte("snap")}, {EntryPeerState, peerstate}}
	n.processAppendEntry(encode(t, &appendEntry{leader: nats0, term: 1, commit: total - 1, pterm: 1, pindex: snapIndex, entries: snapEntries}), snapsub)
	n.RLock()
	papplied := n.papplied
	n.RUnlock()
	require_Equal(t, papplied, snapIndex)

	// Resuming stops listening for snapshots.
	n.ResumeApply()
	n.RLock()
	snapsub = n.snapsub
	n.RUnlock()
	require_True(t, snapsub == nil)

	// As leader, a request only gets our snapshot if it's past theirs.
	n.switchToLeader()
	n.RLock()
	term := n.term
	n.RUnlock()
	request := func(index uint64) {
		ar := newAppendEntryResponse(term, index, nats0, false)
		ar.snapshot, ar.reply = true, snapReply
		n.processAppendEntryResponse(ar)
	}
	request(snapIndex)
	_, err = ssub.NextMsg(250 * time.Millisecond)
	require_Error(t, err, nats.ErrTimeout)

	request(0)
	msg, err = ssub.NextMsg(time.Second)
	require_NoError(t, err)
	sae, err := decodeAppendEntry(msg.Data, nil, _EMPTY_)
	require_NoError(t, err)
	require_Equal(t, sae.pterm, 1)
	require_Equal(t, sae.pindex, snapIndex)
	require_Len(t, len(sae.entries), 2)
	require_Equal(t, sae.entries[0].Type, EntrySnapshot)
	require_Equal(t, string(sae.entries[0].Data), "snap")
}

func TestNRGPausedFollowerProcessSnapshotResponse(t *testing.T) {
	for _, test := range []struct {
		title    string
		pterm    uint64
		pindex   uint64
		resumed  bool
		papplied uint64   // Zero if the snapshot is ignored.
		queued   []uint64 // Indexes queued once we resume.
	}{
		{title: "CommittedTermMismatch", pterm: 2, pindex: 7, queued: []uint64{5, 6, 7, 8, 9}},
		// Entry 10 is past our held commit of 9, so it's replaced by the snapshot.
		{title: "UncommittedTermMismatch", pterm: 2, pindex: 10, papplied: 10, queued: []uint64{10}},
		{title: "AtCommit", pterm: 1, pindex: 4, queued: []uint64{5, 6, 7, 8, 9}},
		// Past our commit of 9 after resuming.
		{title: "NotPaused", pterm: 1, pindex: 10, resumed: true},
		{title: "WithinLog", pterm: 1, pindex: 7, papplied: 7, queued: []uint64{7, 8, 9}},
		{title: "PastLog", pterm: 1, pindex: 100, papplied: 100, queued: []uint64{100}},
	} {
		t.Run(test.title, func(t *testing.T) {
			n, cleanup := initSingleMemRaftNode(t)
			defer cleanup()

			esm := encodeStreamMsgAllowCompress("foo", _EMPTY_, nil, nil, 0, 0, true)
			entries := []*Entry{newEntry(EntryNormal, esm)}
			nats0 := "S1Nunr6R" // "nats-0"

			// Stores the entry after pindex, the leader's commit lags it by one.
			store := func(pindex uint64) {
				ae := &appendEntry{leader: nats0, term: 1, commit: pindex, pterm: 1, pindex: pindex, entries: entries}
				// There's no term before the first entry.
				if pindex == 0 {
					ae.pterm = 0
				}
				n.processAppendEntry(encode(t, ae), n.aesub)
			}
			// Entries 1-5 with commits up to 4 are applied.
			for pindex := range uint64(5) {
				store(pindex)
			}
			// Entries 6-10 with commits up to 9 are held while paused.
			require_NoError(t, n.PauseApply())
			for pindex := uint64(5); pindex < 10; pindex++ {
				store(pindex)
			}
			if test.resumed {
				n.ResumeApply()
			}
			n.apply.drain()

			n.RLock()
			peerstate := encodePeerState(&peerState{n.peerNames(), n.csz, n.extSt})
			n.RUnlock()
			snapEntries := []*Entry{{EntrySnapshot, []byte("snap")}, {EntryPeerState, peerstate}}
			sae := encode(t, &appendEntry{leader: nats0, term: 1, commit: 9, pterm: test.pterm, pindex: test.pindex, entries: snapEntries})

			var state StreamState
			n.Lock()
			n.processSnapshotResponseLocked(sae)
			papplied, pindex := n.papplied, n.pindex
			n.wal.FastState(&state)
			n.Unlock()

			// Our log still ends at 10, unless it restarts at a snapshot past it.
			// An installed snapshot compacts it up to the snapshot.
			require_Equal(t, papplied, test.papplied)
			require_Equal(t, pindex, max(10, test.papplied))
			require_Equal(t, state.FirstSeq, test.papplied+1)
			if test.resumed {
				return
			}

			// Resuming applies an installed snapshot first, then replays the held commits after it.
			n.ResumeApply()
			var queued []uint64
			for _, ce := range n.apply.pop() {
				queued = append(queued, ce.Index)
			}
			require_True(t, slices.Equal(queued, test.queued))
		})
	}
}

func TestNRGApplyCommitDoesNotResetWALAtSnapshotBoundary(t *testing.T) {
	n, cleanup := initSingleMemRaftNode(t)
	defer cleanup()

	nats0 := "S1Nunr6R" // "nats-0"
	esm := encodeStreamMsgAllowCompress("foo", "_INBOX.foo", nil, nil, 0, 0, true)
	entry := []*Entry{newEntry(EntryNormal, esm)}

	// Build a WAL with 3 entries.
	ae1 := encode(t, &appendEntry{leader: nats0, term: 1, commit: 0, pterm: 0, pindex: 0, entries: entry})
	ae2 := encode(t, &appendEntry{leader: nats0, term: 1, commit: 0, pterm: 1, pindex: 1, entries: entry})
	ae3 := encode(t, &appendEntry{leader: nats0, term: 1, commit: 0, pterm: 1, pindex: 2, entries: entry})
	n.processAppendEntry(ae1, n.aesub)
	n.processAppendEntry(ae2, n.aesub)
	n.processAppendEntry(ae3, n.aesub)
	require_Equal(t, n.pindex, 3)
	require_Equal(t, n.commit, 0)

	// Install a snapshot past our commit, not overwriting commit here.
	snap := &snapshot{
		lastTerm:  1,
		lastIndex: 2,
		peerstate: encodePeerState(&peerState{n.peerNames(), n.csz, n.extSt}),
	}
	n.Lock()
	require_NoError(t, n.installSnapshot(snap))
	n.Unlock()
	require_Equal(t, n.papplied, 2)
	require_Equal(t, n.commit, 0)

	var before StreamState
	n.wal.FastState(&before)
	require_Equal(t, before.FirstSeq, 3)
	require_Equal(t, before.Msgs, 1)

	// A heartbeat advances commit through the snapshot boundary. The apply loop
	// should skip entries that are already compacted away.
	aeHeartbeat := encode(t, &appendEntry{leader: nats0, term: 1, commit: 3, pterm: 1, pindex: 3, entries: nil})
	n.processAppendEntry(aeHeartbeat, n.aesub)

	// The WAL must be intact: entry 3 still present, no reset to [0,0].
	var after StreamState
	n.wal.FastState(&after)
	require_Equal(t, after.FirstSeq, 3)
	require_Equal(t, after.Msgs, 1)
	// And we should have applied past the snapshot boundary up to the new commit.
	require_Equal(t, n.commit, 3)
}

func TestNRGProcessAppendEntryTermMismatchDoesNotRegressCommit(t *testing.T) {
	n, cleanup := initSingleMemRaftNode(t)
	defer cleanup()

	// Create a sample entry
	esm := encodeStreamMsgAllowCompress("foo", "_INBOX.foo", nil, nil, 0, 0, true)
	entries := []*Entry{newEntry(EntryNormal, esm)}

	nats0 := "S1Nunr6R" // "nats-0"

	// Timeline
	aeMsg1 := encode(t, &appendEntry{leader: nats0, term: 1, commit: 0, pterm: 0, pindex: 0, entries: entries})
	aeMsg2 := encode(t, &appendEntry{leader: nats0, term: 1, commit: 1, pterm: 1, pindex: 1, entries: entries})
	aeMsg3 := encode(t, &appendEntry{leader: nats0, term: 1, commit: 2, pterm: 1, pindex: 2, entries: entries})
	aeMsg4 := encode(t, &appendEntry{leader: nats0, term: 1, commit: 3, pterm: 1, pindex: 3, entries: entries})
	aeMsg5 := encode(t, &appendEntry{leader: nats0, term: 1, commit: 4, pterm: 1, pindex: 4, entries: entries})
	aeHeartbeat := encode(t, &appendEntry{leader: nats0, term: 1, commit: 5, pterm: 1, pindex: 5, entries: nil})

	// Store five entries and commit.
	n.processAppendEntry(aeMsg1, n.aesub)
	n.processAppendEntry(aeMsg2, n.aesub)
	n.processAppendEntry(aeMsg3, n.aesub)
	n.processAppendEntry(aeMsg4, n.aesub)
	n.processAppendEntry(aeMsg5, n.aesub)
	n.processAppendEntry(aeHeartbeat, n.aesub)
	require_Equal(t, n.commit, 5)
	require_Equal(t, n.pindex, 5)

	// Send an append entry with a mismatched pterm. This will trigger
	// the term mismatch path in processAppendEntry which calls
	// truncateWAL. The commit must not regress below what was
	// already committed.
	aeBadPterm := encode(t, &appendEntry{leader: nats0, term: 2, commit: 5, pterm: 2, pindex: 5, entries: entries})
	n.processAppendEntry(aeBadPterm, n.aesub)
	require_Equal(t, n.commit, 5)
}

func TestNRGReset(t *testing.T) {
	n, cleanup := initSingleMemRaftNode(t)
	defer cleanup()

	// Simulate this node having operated as its own group: WAL entries,
	// extra peers, advanced commit/applied, a snapshot file, and a known
	// leader. Reset must clear all of it.
	nats0 := "S1Nunr6R" // "nats-0"
	nats1 := "yrzKKRBu" // "nats-1"
	esm := encodeStreamMsgAllowCompress("foo", "_INBOX.foo", nil, nil, 0, 0, true)
	aeMsg1 := encode(t, &appendEntry{leader: nats0, term: 1, commit: 0, pterm: 0, pindex: 0, entries: []*Entry{newEntry(EntryNormal, esm)}})
	aeMsg2 := encode(t, &appendEntry{leader: nats0, term: 1, commit: 1, pterm: 1, pindex: 1, entries: []*Entry{newEntry(EntryNormal, esm)}})
	require_NoError(t, n.storeToWAL(aeMsg1))
	require_NoError(t, n.storeToWAL(aeMsg2))
	require_Equal(t, n.pindex, 2)

	// Populate the cache, to check that it gets reset below.
	n.pae[1] = &appendEntry{leader: nats0, term: 1, pindex: 0}
	n.pae[2] = &appendEntry{leader: nats0, term: 1, pindex: 1}

	n.commit = 2
	n.applied = 2
	n.processed = 2
	n.papplied = 1
	n.hcommit = 2
	n.term = 7
	n.vote = nats1
	n.writeTermVote()

	// Add another peer in addition to ourselves.
	other := nats1
	n.peers[other] = &lps{}
	n.adjustClusterSizeAndQuorum()
	n.updateLeader(other)

	// Drop a fake snapshot file plus an orphan to verify the whole
	// snapshots directory is wiped, not just n.snapfile.
	snapDir := filepath.Join(n.sd, snapshotsDir)
	require_NoError(t, os.MkdirAll(snapDir, defaultDirPerms))
	sfile := filepath.Join(snapDir, "snap.fake")
	require_NoError(t, os.WriteFile(sfile, []byte("stale"), defaultFilePerms))
	orphan := filepath.Join(snapDir, "snap.orphan")
	require_NoError(t, os.WriteFile(orphan, []byte("orphan"), defaultFilePerms))
	n.snapfile = sfile

	// Push something onto each queue so we can verify they are drained.
	_, err := n.prop.push(&proposedEntry{&Entry{}, _EMPTY_})
	require_NoError(t, err)
	_, err = n.entry.push(&appendEntry{})
	require_NoError(t, err)
	_, err = n.resp.push(&appendEntryResponse{})
	require_NoError(t, err)
	_, err = n.apply.push(newCommittedEntry(1, nil))
	require_NoError(t, err)
	_, err = n.reqs.push(&voteRequest{})
	require_NoError(t, err)
	_, err = n.votes.push(&voteResponse{})
	require_NoError(t, err)

	// Start an async checkpoint so Reset has to clear n.snapshotting too.
	_, err = n.CreateSnapshotCheckpoint(false)
	require_NoError(t, err)
	require_True(t, n.snapshotting)

	// Park the node before resetting, mirroring how callers (e.g. the SYS-account
	// leaf extension path) use Reset: observer mode is set first so the
	// node doesn't compete for leadership immediately after step-down.
	n.setObserver(true, extExtended)
	n.Reset()

	// All log state cleared.
	require_Equal(t, n.pterm, 0)
	require_Equal(t, n.pindex, 0)
	require_Equal(t, n.commit, 0)
	require_Equal(t, n.applied, 0)
	require_Equal(t, n.processed, 0)
	require_Equal(t, n.papplied, 0)
	require_Equal(t, n.hcommit, 0)

	// Peer set reset to just self, cluster size and quorum adjusted.
	require_Equal(t, len(n.peers), 1)
	if _, ok := n.peers[n.id]; !ok {
		t.Fatalf("expected self %q to remain in peers after reset, got %v", n.id, n.peers)
	}
	require_Equal(t, n.csz, 1)
	require_Equal(t, n.qn, 1)

	// Leader forgotten so the parent leader can re-establish via heartbeat.
	require_Equal(t, n.leader, noLeader)

	// State transitions: stepped down to follower, observer mode left as the
	// caller set it, term/vote cleared so the new leader's term wins.
	require_Equal(t, n.State(), Follower)
	require_True(t, n.observer)
	require_Equal(t, n.extSt, extExtended)
	require_Equal(t, n.term, 0)
	require_Equal(t, n.vote, noVote)

	// Snapshot reference cleared, in-flight checkpoint flag cleared, and the
	// entire snapshots directory was wiped (both the referenced file and the
	// orphan), then recreated empty.
	require_Equal(t, n.snapfile, _EMPTY_)
	require_False(t, n.snapshotting)
	if _, err = os.Stat(sfile); !os.IsNotExist(err) {
		t.Fatalf("expected snapshot file %q to be removed, stat err=%v", sfile, err)
	}
	if _, err = os.Stat(orphan); !os.IsNotExist(err) {
		t.Fatalf("expected orphan snapshot %q to be removed, stat err=%v", orphan, err)
	}
	snapEntries, err := os.ReadDir(snapDir)
	require_NoError(t, err)
	require_Equal(t, len(snapEntries), 0)

	// Queues drained.
	require_Equal(t, n.prop.len(), 0)
	require_Equal(t, n.entry.len(), 0)
	require_Equal(t, n.resp.len(), 0)
	require_Equal(t, n.apply.len(), 0)
	require_Equal(t, n.reqs.len(), 0)
	require_Equal(t, n.votes.len(), 0)

	// WAL is empty.
	walEntries, _ := n.Size()
	require_Equal(t, walEntries, 0)

	// Append entry cache cleared.
	require_Equal(t, len(n.pae), 0)
}

func TestNRGBootstrapExpectedClusterSize(t *testing.T) {
	urls := func(host string, ports ...int) []*url.URL {
		t.Helper()
		urls := make([]*url.URL, 0, len(ports))
		for _, p := range ports {
			u, err := url.Parse(fmt.Sprintf("nats-route://%s:%d", host, p))
			require_NoError(t, err)
			urls = append(urls, u)
		}
		return urls
	}

	for _, test := range []struct {
		name     string
		routes   int
		gateways []*RemoteGatewayOpts
		expected int
	}{
		{
			name:   "ip literal gateway urls",
			routes: 3,
			gateways: []*RemoteGatewayOpts{
				{Name: "C1", URLs: urls("127.0.0.1", 6222, 6223, 6224)}, // own cluster, skipped
				{Name: "C2", URLs: urls("127.0.0.1", 7222, 7223, 7224)},
			},
			expected: 6, // 3 routes + 3 remote gateway urls
		},
		{
			name:   "hostname gateway urls are not resolved",
			routes: 3,
			gateways: []*RemoteGatewayOpts{
				{Name: "C1", URLs: urls("localhost", 6222, 6223, 6224)}, // own cluster, skipped
				// "localhost" may resolve to both 127.0.0.1 and ::1; each url
				// must still count as a single peer.
				{Name: "C2", URLs: urls("localhost", 7222, 7223, 7224)},
			},
			expected: 6, // 3 routes + 3 remote gateway urls (not 9)
		},
		{
			name:   "multiple remote clusters",
			routes: 3,
			gateways: []*RemoteGatewayOpts{
				{Name: "C1", URLs: urls("localhost", 6222, 6223, 6224)}, // own cluster, skipped
				{Name: "C2", URLs: urls("localhost", 7222, 7223, 7224)},
				{Name: "C3", URLs: urls("localhost", 8222, 8223, 8224)},
			},
			expected: 9, // 3 routes + 3 + 3 remote gateway urls
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			opts := &Options{}
			for i := 0; i < test.routes; i++ {
				u, err := url.Parse(fmt.Sprintf("nats-route://127.0.0.1:%d", 5000+i))
				require_NoError(t, err)
				opts.Routes = append(opts.Routes, u)
			}
			opts.Gateway.Gateways = test.gateways

			s := &Server{opts: opts}
			s.info.Cluster = "C1"

			cfg := &RaftConfig{Name: "_meta_", Store: t.TempDir()}
			// allPeersKnown=false forces the estimate path.
			require_NoError(t, s.bootstrapRaftNode(cfg, nil, false))

			ps, err := readPeerState(s.diskIOSemaphore(), cfg.Store)
			require_NoError(t, err)
			require_Equal(t, ps.clusterSize, test.expected)
		})
	}
}

func TestNRGCatchupRerequestDoesNotOrphanProgressQueue(t *testing.T) {
	n, cleanup := initSingleMemRaftNode(t)
	defer cleanup()

	// Use large entries so a catchup run exceeds its outstanding limit and
	// must block waiting for index updates instead of finishing immediately.
	msg := make([]byte, 1024*1024)
	entries := []*Entry{newEntry(EntryNormal, msg)}

	nats0 := "S1Nunr6R" // "nats-0"
	nats1 := "yrzKKRBu" // "nats-1"

	aeMsg1 := encode(t, &appendEntry{leader: nats0, term: 1, commit: 0, pterm: 0, pindex: 0, entries: entries})
	aeMsg2 := encode(t, &appendEntry{leader: nats0, term: 1, commit: 0, pterm: 1, pindex: 1, entries: entries})
	aeMsg3 := encode(t, &appendEntry{leader: nats0, term: 1, commit: 0, pterm: 1, pindex: 2, entries: entries})
	require_NoError(t, n.storeToWAL(aeMsg1))
	require_NoError(t, n.storeToWAL(aeMsg2))
	require_NoError(t, n.storeToWAL(aeMsg3))

	// Switching to leader stores a peer state entry at index 4.
	n.term = 1
	n.switchToLeader()
	require_Equal(t, n.pindex, 4)

	// Follower requests a catchup, which starts a catchup run that blocks
	// waiting for index updates after sending up to 2MB of entries.
	n.catchupFollower(&appendEntryResponse{term: 1, index: 0, peer: nats1, reply: "$NRG.CR.TEST.1"})
	n.RLock()
	q1 := n.progress[nats1]
	n.RUnlock()
	require_True(t, q1 != nil)

	// Wait for the first run to consume the initial index update, so we know
	// it has started and captured its view of the last index (4) before we
	// store more entries below.
	checkFor(t, 2*time.Second, 10*time.Millisecond, func() error {
		if q1.len() > 0 {
			return errors.New("first catchup run has not started yet")
		}
		return nil
	})

	// Store another entry so the cancel signal pushed by the re-request below
	// (n.pindex) exceeds the first run's view of the last index, making the
	// first run return promptly instead of waiting for its stall timeout.
	aeMsg4 := encode(t, &appendEntry{leader: nats0, term: 1, commit: 0, pterm: 1, pindex: 4, entries: entries})
	require_NoError(t, n.storeToWAL(aeMsg4))

	// Follower re-requests the catchup while the first run is still active.
	// This cancels the first run and installs a fresh progress queue.
	n.catchupFollower(&appendEntryResponse{term: 1, index: 0, peer: nats1, reply: "$NRG.CR.TEST.2"})
	n.RLock()
	q2 := n.progress[nats1]
	n.RUnlock()
	require_True(t, q2 != nil)
	require_True(t, q1 != q2)

	// Wait for the first run's deferred cleanup. On exit it proposes adding
	// the (unknown) peer, which is observable on the proposal queue since the
	// run loop isn't started in this test.
	checkFor(t, 2*time.Second, 10*time.Millisecond, func() error {
		if n.prop.len() == 0 {
			return errors.New("first catchup run still active")
		}
		return nil
	})

	// The first run's cleanup must only remove its own progress queue. If it
	// removed the re-requested run's queue, index updates would no longer
	// reach that run and the catchup would stall.
	n.RLock()
	q := n.progress[nats1]
	n.RUnlock()
	require_True(t, q == q2)
}

func TestNRGCatchupRerequestLosesCatchupSignal(t *testing.T) {
	test := func(t *testing.T, stall bool) {
		n, cleanup := initSingleMemRaftNode(t)
		defer cleanup()

		esm := encodeStreamMsgAllowCompress("foo", "_INBOX.foo", nil, nil, 0, 0, true)
		entries := []*Entry{newEntry(EntryNormal, esm)}

		nats0 := "S1Nunr6R" // "nats-0"

		// One committed entry, so we have some log.
		n.processAppendEntry(encode(t, &appendEntry{leader: nats0, term: 1, commit: 0, pterm: 0, pindex: 0, entries: entries}), n.aesub)
		n.processAppendEntry(encode(t, &appendEntry{leader: nats0, term: 1, commit: 1, pterm: 1, pindex: 1}), n.aesub)
		require_Equal(t, n.commit, 1)

		// Leader is at 5, we're at 1, so request a catchup.
		n.processAppendEntry(encode(t, &appendEntry{leader: nats0, term: 1, commit: 5, pterm: 1, pindex: 5, entries: entries}), n.aesub)
		require_True(t, n.catchup != nil)
		require_False(t, n.catchup.signal)
		require_Equal(t, n.catchup.cindex, 5)

		// Snapshot at 3 installs and signals the upper layer.
		n.RLock()
		ps := encodePeerState(&peerState{n.peerNames(), n.csz, n.extSt})
		n.RUnlock()
		aeSnap := encode(t, &appendEntry{leader: nats0, term: 1, commit: 3, pterm: 1, pindex: 3,
			entries: []*Entry{{EntrySnapshot, []byte("snapshot")}, {EntryPeerState, ps}}})
		n.processAppendEntry(aeSnap, n.catchup.sub)
		require_Equal(t, n.pindex, 3)
		require_True(t, n.catchup != nil)
		require_True(t, n.catchup.signal)

		if stall {
			// No progress for 2s, so the catchup is stalled and re-requested.
			hb := encode(t, &appendEntry{leader: nats0, term: 1, commit: 5, pterm: 1, pindex: 5})
			n.processAppendEntry(hb, n.aesub)
			require_True(t, n.catchup != nil)
			n.Lock()
			n.catchup.active = time.Now().Add(-3 * time.Second)
			n.Unlock()
			n.processAppendEntry(hb, n.aesub)
			require_True(t, n.catchup != nil)
			require_True(t, n.catchup.signal)
		}

		// Catchup entries 4 and 5 carry the leader's old commit, so we don't re-signal.
		n.processAppendEntry(encode(t, &appendEntry{leader: nats0, term: 1, commit: 3, pterm: 1, pindex: 3, entries: entries}), n.catchup.sub)
		n.processAppendEntry(encode(t, &appendEntry{leader: nats0, term: 1, commit: 3, pterm: 1, pindex: 4, entries: entries}), n.catchup.sub)
		require_Equal(t, n.pindex, 5)
		require_Equal(t, n.commit, 3)

		// We're current now, the catchup is canceled.
		n.processAppendEntry(encode(t, &appendEntry{leader: nats0, term: 1, commit: 5, pterm: 1, pindex: 5}), n.aesub)
		require_True(t, n.catchup == nil)
		require_Equal(t, n.commit, 5)

		// Must have seen EntryCatchup followed by nil.
		var sawCatchup, sawDone bool
		for _, ce := range n.apply.pop() {
			if ce == nil {
				if sawCatchup {
					sawDone = true
				}
				continue
			}
			for _, e := range ce.Entries {
				if e.Type == EntryCatchup {
					sawCatchup = true
				}
			}
		}
		require_True(t, sawCatchup)
		require_True(t, sawDone)
	}

	t.Run("NoStall", func(t *testing.T) { test(t, false) })
	t.Run("Stall", func(t *testing.T) { test(t, true) })
}

func TestNRGCatchupFollowerNoWaitGroupLeakWhenGoRoutineFails(t *testing.T) {
	n, cleanup := initSingleMemRaftNode(t)
	defer cleanup()

	// Store a single entry to the WAL so catchupFollower can load a valid starting entry
	// and proceed to the point where it launches the catchup goroutine.
	var scratch [1024]byte
	n.Lock()
	entries := []*Entry{newEntry(EntryNormal, []byte("hello"))}
	ae := n.buildAppendEntry(entries)
	buf, err := ae.encode(scratch[:])
	require_NoError(t, err)
	ae.buf = buf
	err = n.storeToWAL(ae)
	n.Unlock()
	require_NoError(t, err)

	// Simulate server shutdown: goroutines are no longer started.
	s := n.s
	s.grMu.Lock()
	s.grRunning = false
	s.grMu.Unlock()

	// Ask to catch up a follower.
	ar := newAppendEntryResponse(ae.term, 0, "follower-peer", true)
	n.catchupFollower(ar)

	// startGoRoutine returned false, so the catchup goroutine never ran. With the bug,
	// the wg counter is still +1 (and progress/indexUpdates leaked). Verify Delete() (which
	// does n.wg.Wait()) returns promptly instead of hanging forever.
	done := make(chan struct{})
	go func() {
		n.Delete()
		close(done)
	}()

	select {
	case <-done:
		// Good: wg reached zero, no leak.
	case <-time.After(2 * time.Second):
		t.Fatal("raft.Delete() hung: n.wg leaked because startGoRoutine failed but wg.Done() was never called")
	}

	// Also confirm the per-catchup bookkeeping was cleaned up.
	n.RLock()
	_, progressLeaked := n.progress["follower-peer"]
	n.RUnlock()
	if progressLeaked {
		t.Fatal("progress map entry for the follower leaked after failed catchup")
	}
}

// Must be run with -race.
func TestNRGProcessAppendEntryResponseTermRace(t *testing.T) {
	n, cleanup := initSingleMemRaftNode(t)
	defer cleanup()

	// Use this node's own id as the peer so trackPeer is happy.
	peer := n.id

	var wg sync.WaitGroup
	stop := make(chan struct{})

	// Writer goroutine: write n.term under n.Lock, exactly as raft.Reset does.
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := uint64(0); ; i++ {
			select {
			case <-stop:
				return
			default:
			}
			n.Lock()
			n.term = i
			n.Unlock()
		}
	}()

	// Reader goroutine: call processAppendEntryResponse with an ar that reaches
	// the unlocked `ar.term > n.term` comparison. ar.success=false and
	// ar.reply=="" route us past the earlier branches to that read.
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-stop:
				return
			default:
			}
			ar := &appendEntryResponse{
				term:    0, // 0 > n.term is false, so the heavy stepdown branch is skipped.
				index:   0,
				peer:    peer,
				reply:   _EMPTY_,
				success: false,
			}
			n.processAppendEntryResponse(ar)
		}
	}()

	time.Sleep(300 * time.Millisecond)
	close(stop)
	wg.Wait()
}

func TestNRGPartitionedPeerRemove(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "R2S", 2)
	defer c.shutdown()

	hub, rg := c.createMockMemRaftGroup("MOCK", 2, newStateAdder)
	defer hub.healPartitions()

	leader := rg.waitOnLeader().node()
	require_Equal(t, leader.ClusterSize(), 2)
	require_Equal(t, len(rg.followers()), 1)
	follower := rg.followers()[0].node()

	var hookCalls atomic.Int64
	hub.setAfterMsgHook(func(subject, reply string, msg []byte) {
		hookCalls.Add(1)
	})

	// Remove the follower while the leader is partitioned away
	hub.partition(leader.ID(), 1)
	leader.ProposeRemovePeer(follower.ID())

	// Follower can't get elected as leader, but let's try anyway
	follower.CampaignImmediately()

	// Expect progress on the leader side
	checkFor(t, 1*time.Second, 10*time.Millisecond, func() error {
		if leader.ClusterSize() != 1 {
			return errors.New("node removal still in progress")
		}
		return nil
	})

	checkFor(t, 1*time.Second, 10*time.Millisecond, func() error {
		if leader.MembershipChangeInProgress() {
			return errors.New("membership still in progress")
		}
		return nil
	})

	require_Equal(t, leader.ClusterSize(), 1)
	require_False(t, leader.MembershipChangeInProgress())
	require_True(t, hookCalls.Load() > 0)

	// Follower has not become leader and its membership has not changed.
	require_NotEqual(t, follower.State(), Leader)
	require_Equal(t, follower.ClusterSize(), 2)
	require_False(t, follower.MembershipChangeInProgress())

	// Heal the partition, and expect the follower to get the bad news...
	hub.heal(leader.ID())

	checkFor(t, 1*time.Second, 10*time.Millisecond, func() error {
		if follower.ClusterSize() != 1 {
			return errors.New("node removal still in progress")
		}
		return nil
	})
}

func TestNRGPeerAddAndPartitionLeader(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "R3S", 3)
	defer c.shutdown()

	hub, rg := c.createMockMemRaftGroup("MOCK", 3, newStateAdder)

	leader := rg.waitOnLeader()
	followers := rg.followers()

	// When the leader sends a EntryAddPeer, isolate it from
	// the rest of the cluster.
	hub.setAfterMsgHook(func(subject, reply string, msg []byte) {
		if subject != "$NRG.AE.MOCK" {
			return
		}
		ae, _ := decodeAppendEntry(msg, nil, reply)
		if ae == nil || len(ae.entries) != 1 {
			return
		}
		if ae.leader != leader.node().ID() {
			return
		}
		if ae.entries[0].Type == EntryAddPeer {
			hub.partition(leader.node().ID(), 1)
		}
	})

	// Add a new node and expect a new leader to be elected
	newNode := c.addMockMemRaftNode("MOCK", hub, newStateAdder)
	newGroup := append(followers, newNode)
	newLeader := newGroup.waitOnLeader()
	require_True(t, newLeader != nil)

	// If bug is present: The new leader has not yet committed the
	// appendEntry containing EntryAddPeer. The new leader (and
	// followers) would not update their cluster size until the
	// EntryAddPeer was committed, allowing the following
	// sequence of events:
	// 1) the new leader has initially cluster size of 3
	// 2) it sends a peerState message with cluster size 3
	// 3) the leader commits the EntryAddPeer from the previous
	//    leader, and adjusts its cluster size to 4.
	// 4) the followers commit the EntryAddPeer, incrementing
	//    their cluster size to 4. Next, the EntryPeerState is
	//    committed and the cluster size goes back to 3.
	// 5) At this point cluster size from the leader has diverged
	//    from the cluster size of its followers.

	// Expect all nodes to report cluster size of 4
	for _, n := range newGroup {
		checkFor(t, 1*time.Second, 10*time.Millisecond, func() error {
			if n.node().ClusterSize() != 4 {
				return errors.New("node addition still in progress")
			}
			return nil
		})
	}

	// Finally bring back the old leader
	hub.healPartitions()
	checkFor(t, 1*time.Second, 10*time.Millisecond, func() error {
		if leader.node().MembershipChangeInProgress() {
			return errors.New("membership still in progress")
		}
		if leader.node().ClusterSize() != 4 {
			return errors.New("node addition still in progress")
		}
		return nil
	})

	for _, n := range newGroup {
		checkFor(t, 1*time.Second, 10*time.Millisecond, func() error {
			if n.node().ClusterSize() != 4 {
				return errors.New("node addition still in progress")
			}
			return nil
		})
	}

}

func TestNRGLeaderWithoutQuorumAfterPeerAdd(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "R3S", 3)
	defer c.shutdown()

	hub, rg := c.createMockMemRaftGroup("MOCK", 3, newStateAdder)
	defer hub.healPartitions()

	leader := rg.waitOnLeader()
	followers := rg.followers()

	// Setup a after message hook to create a partition as soon as
	// the leader publishes a EntryAddPeer. The partition will
	// prevent committing the entry.
	hub.setAfterMsgHook(func(subject, reply string, msg []byte) {
		if subject != "$NRG.AE.MOCK" {
			return
		}
		ae, _ := decodeAppendEntry(msg, nil, reply)
		if ae == nil || len(ae.entries) != 1 {
			return
		}
		if ae.leader != leader.node().ID() {
			return
		}

		// After EntryAddPeer is published, partition the
		// leader and one of the followers. This partition
		// can't commit the entry.
		if ae.entries[0].Type == EntryAddPeer {
			hub.partition(leader.node().ID(), 1)
			hub.partition(followers[0].node().ID(), 1)
		}
	})

	newNode := c.addMockMemRaftNode("MOCK", hub, newStateAdder)

	// At some point here the cluster gets partitioned in two
	// parts: {leader, followers[0]} and {newNode, followers[1]}.
	// Neither side should be able to make progress.
	newGroup := smGroup{newNode, followers[1]}
	newLeader := newGroup.waitOnLeader()

	// If the bug is present: we managed to elect a new leader,
	// in a 4 node cluster, with only two nodes in the partition!
	// This is because of the following sequence of events:
	// 1) the follower has received the EntryPeerAdd
	// 2) the leader and the other follower have partitioned away
	// 3) the entry is uncommitted, however the follower has added
	///   the new peer to its peer set, but won't adjust cluster
	//    size and quorum until after the entry is committed.
	// 4) follower becomes a canditate and will become leader with
	//    a single vote from the new node
	require_Equal(t, newLeader, nil)

	// Check that node addition completes after healing the partition
	hub.healPartitions()
	rg = append(rg, newNode)
	newLeader = rg.waitOnLeader()
	require_True(t, newLeader != nil)
	for _, n := range rg {
		checkFor(t, 1*time.Second, 10*time.Millisecond, func() error {
			if n.node().ClusterSize() != 4 {
				return errors.New("node addition still in progress")
			}
			return nil
		})
	}
}

func TestNRGRemovedPeerVoteDoesNotElectLeader(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "R4S", 4)
	defer c.shutdown()

	hub, rg := c.createMockMemRaftGroup("MOCK", 4, newStateAdder)
	defer hub.healPartitions()

	nodeA := rg.waitOnLeader()
	require_NotNil(t, nodeA)

	followers := rg.followers()
	require_Equal(t, len(followers), 3)
	nodeB, nodeC, nodeD := followers[0], followers[1], followers[2]

	// Partition D away and then remove it.
	// D never learns that it is no longer part of the group.
	hub.partition(nodeD.node().ID(), 1)

	require_NoError(t, nodeA.node().ProposeRemovePeer(nodeD.node().ID()))
	valid := smGroup{nodeA, nodeB, nodeC}
	checkFor(t, time.Second, 10*time.Millisecond, func() error {
		for _, sm := range valid {
			node := sm.node()
			if size := node.ClusterSize(); size != 3 {
				return fmt.Errorf("%q has cluster size %d", node.ID(), size)
			}
		}
		return nil
	})

	// Partition C with D, leaving two partitions:
	//  - {A, B} which has a valid quorum
	//  - {C, D} which is not supposed to have a leader
	hub.partition(nodeC.node().ID(), 1)
	require_NoError(t, nodeC.node().CampaignImmediately())

	err := checkForErr(time.Second, 10*time.Millisecond, func() error {
		if nodeC.node().State() == Leader {
			return nil
		}
		return fmt.Errorf("%q has not become leader", nodeC.node().ID())
	})
	if err == nil {
		t.Fatalf("removed peer %q helped elect second leader %q while %q was leader",
			nodeD.node().ID(), nodeC.node().ID(), nodeA.node().ID())
	}
}

func TestNRGEntryBytesEstimate(t *testing.T) {
	n, cleanup := initSingleMemRaftNode(t)
	defer cleanup()

	n.Lock()
	defer n.Unlock()

	const largeEntryBytes = 512 * 1024
	for _, test := range []struct {
		name     string
		pindex   uint64
		papplied uint64
		bytes    uint64
		entries  uint64
		expected uint64
	}{
		{"no entries in the wal", 0, 0, 0, 100, 0},
		{"no bytes in the wal", 1000, 0, 0, 100, 0},
		{"pindex equals papplied", 1000, 1000, 1024, 100, 0},
		{"large entries", 1000, 0, 1000 * largeEntryBytes, 100, 100 * largeEntryBytes},
		{"small entries", 1000, 0, 1000 * 1024, 100, 100 * 1024},
		{"offset by papplied", 1500, 500, 1000 * largeEntryBytes, 100, 100 * largeEntryBytes},
		{"asking for more than we hold", 1000, 0, 1000 * largeEntryBytes, 2000, 2000 * largeEntryBytes},
	} {
		t.Run(test.name, func(t *testing.T) {
			n.pindex, n.papplied, n.bytes = test.pindex, test.papplied, test.bytes
			require_Equal(t, n.entryBytes(test.entries), test.expected)
		})
	}
}

func TestNRGOverrunUsesBytesWhenEntryCountIsLow(t *testing.T) {
	n, cleanup := initSingleMemRaftNode(t)
	defer cleanup()

	n.Lock()
	defer n.Unlock()

	// A backlog of large entries. The entry count stays two orders of magnitude
	// below the count threshold the whole time, so only the byte threshold can
	// catch this.
	const largeEntryBytes = 512 * 1024
	setAverage := func(entries, avg uint64) {
		n.papplied, n.pindex, n.bytes = 0, entries, entries*avg
	}

	// 2500 entries of 512KiB is ~1.22GB, over pauseQuorumBytes.
	setAverage(2500, largeEntryBytes)
	require_True(t, 2500 < uint64(pauseQuorumThreshold))
	require_True(t, n.overrun(2500, pauseQuorumThreshold, pauseQuorumBytes))

	// 900 entries of 512KiB is ~460MB, under pauseQuorumBytes.
	setAverage(900, largeEntryBytes)
	require_False(t, n.overrun(900, pauseQuorumThreshold, pauseQuorumBytes))

	// The entry count path must still work for small entries.
	setAverage(2500, 1024)
	require_False(t, n.overrun(2500, pauseQuorumThreshold, pauseQuorumBytes))
	setAverage(pauseQuorumThreshold+1, 1024)
	require_True(t, n.overrun(pauseQuorumThreshold+1, pauseQuorumThreshold, pauseQuorumBytes))
}

func TestNRGLeaderOverrunOnLargeEntries(t *testing.T) {
	const largeEntryBytes = 512 * 1024
	for _, test := range []struct {
		name     string
		lagging  string // which counter lags behind
		expected bool
		avg      uint64
	}{
		{"uncommitted large entries", "commit", true, largeEntryBytes},
		{"uncommitted small entries", "commit", false, 1024},
		{"unapplied large entries", "applied", true, largeEntryBytes},
		{"unapplied small entries", "applied", false, 1024},
	} {
		t.Run(test.name, func(t *testing.T) {
			n, cleanup := initSingleMemRaftNode(t)
			defer cleanup()

			n.Lock()
			defer n.Unlock()

			// 2500 entries is far below pauseQuorumThreshold, so whether we are
			// overrun depends entirely on the byte estimate.
			const entries = 2500
			n.papplied, n.pindex, n.bytes = 0, entries, entries*test.avg
			if test.lagging == "commit" {
				// We are not getting quorum from our followers.
				n.commit, n.applied = 0, 0
			} else {
				// We are committing but too slow to apply.
				n.commit, n.applied = entries, 0
			}

			require_Equal(t, n.isLeaderOverrun(), test.expected)
		})
	}
}

func TestNRGCachePendingEntryBoundedByBytes(t *testing.T) {
	for _, test := range []struct {
		name        string
		payloadSize int
		inserts     int
		capped      bool
	}{
		// Large entries: the cache fills up to paeDropBytes and stops
		// there, nowhere near paeDropThreshold.
		{"large entries stop at the byte cap", 512 * 1024, 1000, true},
		// Small entries: 1000 of them is ~1MB, so nothing is dropped.
		{"small entries are all cached", 1024, 1000, false},
	} {
		t.Run(test.name, func(t *testing.T) {
			n, cleanup := initSingleMemRaftNode(t)
			defer cleanup()

			entries := []*Entry{newEntry(EntryNormal, make([]byte, test.payloadSize))}
			ae := encode(t, &appendEntry{leader: "S1Nunr6R", term: 1, entries: entries})

			n.Lock()
			defer n.Unlock()

			sz := n.entryStoreSize(ae)
			require_True(t, sz > 0)

			// We stop caching once we'd go over paeDropBytes.
			expected := test.inserts
			if test.capped {
				expected = int((paeDropBytes + sz - 1) / sz)
			}

			// A long history of entries that were applied but not snapshotted yet
			// must not dilute what we account for, only what's cached counts.
			n.papplied, n.bytes = 0, 100*1024*1024

			for i := 1; i <= test.inserts; i++ {
				n.pindex = uint64(i)
				n.cachePendingEntry(ae)
			}

			require_Len(t, len(n.pae), expected)
			// Whatever we dropped, it was not because of the entry count.
			require_True(t, len(n.pae) < paeDropThreshold)
			// We must account for exactly what we've cached.
			require_Equal(t, n.paeBytes, uint64(len(n.pae))*sz)
		})
	}
}

func TestNRGCachePendingEntryBytesAccounting(t *testing.T) {
	n, cleanup := initSingleMemRaftNode(t)
	defer cleanup()

	nats0 := "S1Nunr6R" // "nats-0"
	newAE := func(index uint64, payload int) *appendEntry {
		entries := []*Entry{newEntry(EntryNormal, make([]byte, payload))}
		return encode(t, &appendEntry{leader: nats0, term: 1, pterm: 1, pindex: index - 1, entries: entries})
	}

	n.Lock()
	defer n.Unlock()

	large := newAE(3, 4*1024)
	ssz, lsz := n.entryStoreSize(newAE(1, 128)), n.entryStoreSize(large)
	require_True(t, lsz > ssz)

	// Cache a couple of entries.
	for i := uint64(1); i <= 3; i++ {
		n.pindex = i
		n.cachePendingEntry(newAE(i, 128))
	}
	require_Len(t, len(n.pae), 3)
	require_Equal(t, n.paeBytes, 3*ssz)

	// Re-caching an index with a different value must not account for it twice.
	n.pindex = 3
	n.cachePendingEntry(large)
	require_Len(t, len(n.pae), 3)
	require_Equal(t, n.paeBytes, 2*ssz+lsz)

	// Truncating must remove the bytes of the entries the WAL no longer has.
	n.pterm, n.pindex = 1, 3
	n.truncateWAL(1, 2)
	require_Len(t, len(n.pae), 2)
	require_Equal(t, n.paeBytes, 2*ssz)

	// Resetting the cache clears our accounting.
	n.resetPendingEntries()
	require_Len(t, len(n.pae), 0)
	require_Equal(t, n.paeBytes, 0)
}

// createClusterWithoutJetStream creates a routed cluster without a meta group, which would be slow to elect under long election timeouts.
func createClusterWithoutJetStream(t *testing.T) *cluster {
	modify := func(_, _, _, conf string) string { return strings.Replace(conf, "jetstream:", "#jetstream:", 1) }
	return createJetStreamClusterAndModHook(t, jsClusterTempl, "R3S", _EMPTY_, 3, 14_622, false, modify)
}

func TestNRGPreferredCandidateResendsVoteRequest(t *testing.T) {
	// A long election timeout ensures any retries happen within a single term.
	omin, omax := minElectionTimeout, maxElectionTimeout
	minElectionTimeout, maxElectionTimeout = time.Minute, 2*time.Minute
	defer func() { minElectionTimeout, maxElectionTimeout = omin, omax }()

	for _, preferred := range []bool{true, false} {
		t.Run(fmt.Sprintf("Preferred=%v", preferred), func(t *testing.T) {
			c := createClusterWithoutJetStream(t)
			defer c.shutdown()

			// Only the first peer has created its node, the others have not yet.
			peers := serverPeerNames(c.servers)
			cfg := &RaftConfig{Name: "TEST", Store: t.TempDir(), Log: c.createWAL("TEST", MemoryStorage)}
			n := c.createStateMachine(c.servers[0], cfg, peers, newStateAdder).node().(*raft)

			// Listen for vote requests in place of the other peers.
			nc, err := nats.Connect(c.servers[1].ClientURL(), nats.UserInfo("admin", "s3cr3t!"))
			require_NoError(t, err)
			defer nc.Close()
			sub, err := nc.SubscribeSync(n.vsubj)
			require_NoError(t, err)
			require_NoError(t, nc.Flush())
			sacc := c.servers[0].SystemAccount()
			checkFor(t, 2*time.Second, 10*time.Millisecond, func() error {
				if np, _ := sacc.sl.NumInterest(n.vsubj); np < 2 {
					return errors.New("vote subject interest not propagated yet")
				}
				return nil
			})

			if preferred {
				require_NoError(t, n.CampaignImmediately())
			} else {
				require_NoError(t, n.Campaign())
			}
			msg, err := sub.NextMsg(2 * time.Second)
			require_NoError(t, err)
			vr := decodeVoteRequest(msg.Data, msg.Reply)
			require_NotNil(t, vr)
			require_Equal(t, vr.candidate, n.ID())

			if !preferred {
				// A regular candidate relies on its election timer to retry.
				_, err = sub.NextMsg(time.Second)
				require_Error(t, err, nats.ErrTimeout)
				require_Equal(t, n.State(), Candidate)
				return
			}

			// The preferred candidate re-sends the same vote request, without bumping its term.
			for range 2 {
				msg, err = sub.NextMsg(2 * time.Second)
				require_NoError(t, err)
				rvr := decodeVoteRequest(msg.Data, msg.Reply)
				require_NotNil(t, rvr)
				require_Equal(t, *rvr, *vr)
			}

			// A peer that came up late grants the re-sent vote request.
			vresp := &voteResponse{term: vr.term, peer: peers[1], granted: true}
			require_NoError(t, msg.Respond(vresp.encode()))
			checkFor(t, 2*time.Second, 10*time.Millisecond, func() error {
				if n.State() != Leader {
					return errors.New("not leader yet")
				}
				return nil
			})
			require_Equal(t, n.Term(), vr.term)
		})
	}
}

func TestNRGLeaderTransferTargetRetriesQuickly(t *testing.T) {
	n, cleanup := initSingleMemRaftNode(t)
	defer cleanup()

	n.Lock()
	n.lxfer = true
	n.commit = n.processed + 1
	n.Unlock()

	n.switchToCandidate()
	require_Equal(t, n.State(), Follower)
	select {
	case <-n.electTimer().C:
	case <-time.After(2 * minCampaignTimeout):
		t.Fatal("Transfer target didn't retry quickly")
	}
}

func TestNRGLeaderTransferTargetFollowsNewLeader(t *testing.T) {
	n, cleanup := initSingleMemRaftNode(t)
	defer cleanup()

	nats0 := "S1Nunr6R" // "nats-0"
	nats1 := "yrzKKRBu" // "nats-1"

	// The old leader names us as its successor.
	entries := []*Entry{newEntry(EntryLeaderTransfer, []byte(n.id))}
	n.processAppendEntry(encode(t, &appendEntry{leader: nats0, term: 1, commit: 0, pterm: 0, pindex: 0, entries: entries}), n.aesub)
	require_True(t, n.lxfer)

	// Granting another candidate our vote moves us to its term.
	require_NoError(t, n.processVoteRequest(&voteRequest{term: 2, lastTerm: 1, lastIndex: 1, candidate: nats1}))
	require_Equal(t, n.term, 2)
	require_True(t, n.lxfer)

	// Following the new leader in that same term clears the transfer.
	n.processAppendEntry(encode(t, &appendEntry{leader: nats1, term: 2, commit: 0, pterm: 1, pindex: 1, entries: nil}), n.aesub)
	require_Equal(t, n.leader, nats1)
	require_False(t, n.lxfer)
}

func TestNRGStepDownToStalledPeerElectsQuickly(t *testing.T) {
	for _, replicas := range []int{3, 5} {
		t.Run(fmt.Sprintf("R%d", replicas), func(t *testing.T) {
			c := createJetStreamClusterExplicit(t, fmt.Sprintf("R%dS", replicas), replicas)
			defer c.shutdown()

			rg := c.createRaftGroup("TEST", replicas, newStateAdder)
			leader := rg.waitOnLeader()
			require_NotNil(t, leader)

			// Stand-in for a successor stuck in writeTermVote.
			stalled := rg.nonLeader().node().(*raft)
			stalled.Lock()
			unlock := time.AfterFunc(10*time.Second, stalled.Unlock)
			defer func() {
				if unlock.Stop() {
					stalled.Unlock()
				}
			}()

			require_NoError(t, leader.node().StepDown(stalled.ID()))
			// Like in lame duck mode, the old leader only votes.
			leader.node().SetObserver(true)

			// Without shortening the followers' election timers, this would take at least 1.5s.
			checkFor(t, minElectionTimeout-100*time.Millisecond, 10*time.Millisecond, func() error {
				if rg.leader() == nil {
					return errors.New("no leader yet")
				}
				return nil
			})
		})
	}
}

func TestNRGVoteRequestPersistsNewTerm(t *testing.T) {
	n, cleanup := initSingleMemRaftNode(t)
	defer cleanup()

	nats0 := "S1Nunr6R" // "nats-0"

	// Granting persists the new term with the vote.
	require_NoError(t, n.processVoteRequest(&voteRequest{term: 1, lastTerm: 0, lastIndex: 0, candidate: nats0}))
	term, voted, err := n.readTermVote()
	require_NoError(t, err)
	require_Equal(t, term, 1)
	require_Equal(t, voted, nats0)

	// Denying an outdated candidate still persists the new term.
	n.pterm, n.pindex = 1, 100
	require_NoError(t, n.processVoteRequest(&voteRequest{term: 2, lastTerm: 1, lastIndex: 2, candidate: nats0}))
	term, voted, err = n.readTermVote()
	require_NoError(t, err)
	require_Equal(t, term, 2)
	require_Equal(t, voted, noVote)
}

func TestNRGStepDownPrefersCaughtUpPeer(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "R3S", 3)
	defer c.shutdown()

	rg := c.createRaftGroup("TEST", 3, newStateAdder)

	// Repeat, as peers are visited in random order.
	for range 10 {
		leader := rg.waitOnLeader()
		require_NotNil(t, leader)
		ln := leader.node().(*raft)

		// Followers only count as caught up once they've acked since the election.
		leader.(*stateAdder).proposeDelta(1)
		followers := rg.followers()
		checkFor(t, 2*time.Second, 10*time.Millisecond, func() error {
			for _, f := range followers {
				if !ln.IsFollowerCaughtUp(f.node().ID()) {
					return fmt.Errorf("follower %s not caught up", f.node().ID())
				}
			}
			return nil
		})

		// Pretend we're catching up one of the followers.
		catchingUp := followers[0].node().ID()
		ln.Lock()
		if ln.progress == nil {
			ln.progress = make(map[string]*ipQueue[uint64])
		}
		q := newIPQueue[uint64](ln.s, "test")
		ln.progress[catchingUp] = q
		ln.Unlock()

		require_NoError(t, ln.StepDown())
		ln.Lock()
		delete(ln.progress, catchingUp)
		ln.Unlock()
		q.unregister()

		nl := rg.waitOnLeader()
		require_NotNil(t, nl)
		require_NotEqual(t, nl.node().ID(), catchingUp)
	}
}
