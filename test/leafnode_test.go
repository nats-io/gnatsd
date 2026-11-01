// Copyright 2019-2026 The NATS Authors
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

package test

import (
	"bytes"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"fmt"
	"math/rand/v2"
	"net"
	"net/url"
	"os"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nats-io/jwt/v2"
	"github.com/nats-io/nats-server/v2/logger"
	"github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nkeys"
	"github.com/nats-io/nuid"
)

func createLeafConn(t tLogger, host string, port int) net.Conn {
	return createClientConn(t, host, port)
}

func testDefaultOptionsForLeafNodes() *server.Options {
	o := DefaultTestOptions
	o.Port = -1
	o.LeafNode.Host = o.Host
	o.LeafNode.Port = -1
	o.NoSystemAccount = true
	return &o
}

func runLeafServer() (*server.Server, *server.Options) {
	o := testDefaultOptionsForLeafNodes()
	return RunServer(o), o
}

func runLeafServerOnPort(port int) (*server.Server, *server.Options) {
	o := testDefaultOptionsForLeafNodes()
	o.LeafNode.Port = port
	return RunServer(o), o
}

func runSolicitLeafServer(lso *server.Options) (*server.Server, *server.Options) {
	return runSolicitLeafServerToURL(fmt.Sprintf("nats-leaf://%s:%d", lso.LeafNode.Host, lso.LeafNode.Port))
}

func runSolicitLeafServerToURL(surl string) (*server.Server, *server.Options) {
	o := DefaultTestOptions
	o.Port = -1
	o.NoSystemAccount = true
	rurl, _ := url.Parse(surl)
	o.LeafNode.Remotes = []*server.RemoteLeafOpts{{URLs: []*url.URL{rurl}}}
	o.LeafNode.ReconnectInterval = 100 * time.Millisecond
	return RunServer(&o), &o
}

func TestLeafNodeInfo(t *testing.T) {
	s, opts := runLeafServer()
	defer s.Shutdown()

	lc := createLeafConn(t, opts.LeafNode.Host, opts.LeafNode.Port)
	defer lc.Close()

	info := checkInfoMsg(t, lc)
	if !info.AuthRequired {
		t.Fatalf("AuthRequired should always be true for leaf nodes")
	}
	// By default headers should be true.
	if !info.Headers {
		t.Fatalf("Expected to have headers on by default")
	}

	sendProto(t, lc, "CONNECT {}\r\n")
	checkLeafNodeConnected(t, s)

	// Now close connection, make sure we are doing the right accounting in the server.
	lc.Close()

	checkLeafNodeConnections(t, s, 0)
}

func TestLeafNodeSplitBuffer(t *testing.T) {
	s, opts := runLeafServer()
	defer s.Shutdown()

	nc, err := nats.Connect(s.ClientURL())
	if err != nil {
		t.Fatalf("Error on connect: %v", err)
	}
	defer nc.Close()

	nc.QueueSubscribe("foo", "bar", func(m *nats.Msg) {
		m.Respond([]byte("ok"))
	})
	nc.Flush()

	lc := createLeafConn(t, opts.LeafNode.Host, opts.LeafNode.Port)
	defer lc.Close()
	sendProto(t, lc, "CONNECT {}\r\n")
	checkLeafNodeConnected(t, s)

	leafSend, leafExpect := setupLeaf(t, lc, 2)

	leafSend("LS+ reply\r\nPING\r\n")
	leafExpect(pongRe)

	leafSend("LMSG foo ")
	time.Sleep(time.Millisecond)
	leafSend("+ reply bar 2\r\n")
	time.Sleep(time.Millisecond)
	leafSend("OK\r")
	time.Sleep(time.Millisecond)
	leafSend("\n")
	leafExpect(lmsgRe)
}

func TestNumLeafNodes(t *testing.T) {
	s, opts := runLeafServer()
	defer s.Shutdown()

	createNewLeafNode := func() net.Conn {
		t.Helper()
		lc := createLeafConn(t, opts.LeafNode.Host, opts.LeafNode.Port)
		checkInfoMsg(t, lc)
		sendProto(t, lc, "CONNECT {}\r\n")
		return lc
	}
	checkLeafNodeConnections(t, s, 0)

	lc1 := createNewLeafNode()
	defer lc1.Close()
	checkLeafNodeConnections(t, s, 1)

	lc2 := createNewLeafNode()
	defer lc2.Close()
	checkLeafNodeConnections(t, s, 2)

	// Now test remove works.
	lc1.Close()
	checkLeafNodeConnections(t, s, 1)

	lc2.Close()
	checkLeafNodeConnections(t, s, 0)
}

func TestLeafNodeRequiresConnect(t *testing.T) {
	opts := testDefaultOptionsForLeafNodes()
	opts.LeafNode.AuthTimeout = 0.001
	s := RunServer(opts)
	defer s.Shutdown()

	lc := createLeafConn(t, opts.LeafNode.Host, opts.LeafNode.Port)
	defer lc.Close()

	info := checkInfoMsg(t, lc)
	if !info.AuthRequired {
		t.Fatalf("Expected AuthRequired to force CONNECT")
	}
	if info.TLSRequired {
		t.Fatalf("Expected TLSRequired to be false")
	}
	if info.TLSVerify {
		t.Fatalf("Expected TLSVerify to be false")
	}

	// Now wait and make sure we get disconnected.
	errBuf := expectResult(t, lc, errRe)

	if !strings.Contains(string(errBuf), "Authentication Timeout") {
		t.Fatalf("Authentication Timeout response incorrect: %q", errBuf)
	}
	expectDisconnect(t, lc)
}

func setupLeaf(t *testing.T, lc net.Conn, expectedSubs int) (sendFun, expectFun) {
	t.Helper()
	send, expect := setupConn(t, lc)
	// A loop detection subscription is sent, so consume this here, along
	// with the ones that caller expect on setup.
	expectNumberOfProtos(t, expect, lsubRe, expectedSubs, infoStartRe, pingRe)
	return send, expect
}

func TestLeafNodeSendsSubsAfterConnect(t *testing.T) {
	s, opts := runLeafServer()
	defer s.Shutdown()

	c := createClientConn(t, opts.Host, opts.Port)
	defer c.Close()

	send, expect := setupConn(t, c)
	send("SUB foo 1\r\n")
	send("SUB bar 2\r\n")
	send("SUB foo baz 3\r\n")
	send("SUB foo baz 4\r\n")
	send("SUB bar 5\r\n")
	send("PING\r\n")
	expect(pongRe)

	lc := createLeafConn(t, opts.LeafNode.Host, opts.LeafNode.Port)
	defer lc.Close()

	// This should compress down to 1 for foo, 1 for bar, and 1 for foo [baz]
	// and one for the loop detection subject.
	setupLeaf(t, lc, 4)
}

func TestLeafNodeSendsSubsOngoing(t *testing.T) {
	s, opts := runLeafServer()
	defer s.Shutdown()

	c := createClientConn(t, opts.Host, opts.Port)
	defer c.Close()

	send, expect := setupConn(t, c)
	send("PING\r\n")
	expect(pongRe)

	lc := createLeafConn(t, opts.LeafNode.Host, opts.LeafNode.Port)
	defer lc.Close()

	leafSend, leafExpect := setupLeaf(t, lc, 1)
	leafSend("PING\r\n")
	leafExpect(pongRe)

	send("SUB foo 1\r\n")
	leafExpect(lsubRe)

	// Check queues send updates each time.
	// TODO(dlc) - If we decide to suppress this with a timer approach this test will break.
	send("SUB foo bar 2\r\n")
	leafExpect(lsubRe)
	send("SUB foo bar 3\r\n")
	leafExpect(lsubRe)
	send("SUB foo bar 4\r\n")
	leafExpect(lsubRe)

	// Now check more normal subs do nothing.
	send("SUB foo 5\r\n")
	expectNothing(t, lc)

	// Check going back down does nothing til we hit 0.
	send("UNSUB 5\r\n")
	expectNothing(t, lc)
	send("UNSUB 1\r\n")
	leafExpect(lunsubRe)

	// Queues going down should always send updates.
	send("UNSUB 2\r\n")
	leafExpect(lsubRe)
	send("UNSUB 3\r\n")
	leafExpect(lsubRe)
	send("UNSUB 4\r\n")
	leafExpect(lunsubRe)
}

func TestLeafNodeSubs(t *testing.T) {
	s, opts := runLeafServer()
	defer s.Shutdown()

	lc := createLeafConn(t, opts.LeafNode.Host, opts.LeafNode.Port)
	defer lc.Close()

	leafSend, leafExpect := setupLeaf(t, lc, 1)

	leafSend("PING\r\n")
	leafExpect(pongRe)

	leafSend("LS+ foo\r\n")
	expectNothing(t, lc)
	leafSend("PING\r\n")
	leafExpect(pongRe)

	c := createClientConn(t, opts.Host, opts.Port)
	defer c.Close()

	send, expect := setupConn(t, c)
	send("PING\r\n")
	expect(pongRe)

	send("PUB foo 2\r\nOK\r\n")
	matches := lmsgRe.FindAllSubmatch(leafExpect(lmsgRe), -1)
	if len(matches) != 1 {
		t.Fatalf("Expected only 1 msg, got %d", len(matches))
	}
	checkLmsg(t, matches[0], "foo", "", "2", "OK")

	// Second sub should not change delivery
	leafSend("LS+ foo\r\n")
	expectNothing(t, lc)
	leafSend("PING\r\n")
	leafExpect(pongRe)

	send("PUB foo 3\r\nOK!\r\n")
	matches = lmsgRe.FindAllSubmatch(leafExpect(lmsgRe), -1)
	if len(matches) != 1 {
		t.Fatalf("Expected only 1 msg, got %d", len(matches))
	}
	checkLmsg(t, matches[0], "foo", "", "3", "OK!")

	// Now add in a queue sub with weight 4.
	leafSend("LS+ foo bar 4\r\n")
	expectNothing(t, lc)
	leafSend("PING\r\n")
	leafExpect(pongRe)

	send("PUB foo 4\r\nOKOK\r\n")
	matches = lmsgRe.FindAllSubmatch(leafExpect(lmsgRe), -1)
	if len(matches) != 1 {
		t.Fatalf("Expected only 1 msg, got %d", len(matches))
	}
	checkLmsg(t, matches[0], "foo", "| bar", "4", "OKOK")

	// Now add in a queue sub with weight 4.
	leafSend("LS+ foo baz 2\r\n")
	expectNothing(t, lc)
	leafSend("PING\r\n")
	leafExpect(pongRe)

	send("PUB foo 5\r\nHELLO\r\n")
	matches = lmsgRe.FindAllSubmatch(leafExpect(lmsgRe), -1)
	if len(matches) != 1 {
		t.Fatalf("Expected only 1 msg, got %d", len(matches))
	}
	checkLmsg(t, matches[0], "foo", "| bar baz", "5", "HELLO")

	// Test Unsub
	leafSend("LS- foo\r\n")
	leafSend("LS- foo bar\r\n")
	leafSend("LS- foo baz\r\n")
	expectNothing(t, lc)
	leafSend("PING\r\n")
	leafExpect(pongRe)

	send("PUB foo 5\r\nHELLO\r\n")
	expectNothing(t, lc)
}

func TestLeafNodeMsgDelivery(t *testing.T) {
	s, opts := runLeafServer()
	defer s.Shutdown()

	lc := createLeafConn(t, opts.LeafNode.Host, opts.LeafNode.Port)
	defer lc.Close()

	leafSend, leafExpect := setupLeaf(t, lc, 1)

	leafSend("PING\r\n")
	leafExpect(pongRe)

	c := createClientConn(t, opts.Host, opts.Port)
	defer c.Close()

	send, expect := setupConn(t, c)
	send("PING\r\n")
	expect(pongRe)

	send("SUB foo 1\r\nPING\r\n")
	expect(pongRe)
	leafExpect(lsubRe)

	// Now send from leaf side.
	leafSend("LMSG foo 2\r\nOK\r\n")
	expectNothing(t, lc)

	matches := msgRe.FindAllSubmatch(expect(msgRe), -1)
	if len(matches) != 1 {
		t.Fatalf("Expected only 1 msg, got %d", len(matches))
	}
	checkMsg(t, matches[0], "foo", "1", "", "2", "OK")

	send("UNSUB 1\r\nPING\r\n")
	expect(pongRe)
	leafExpect(lunsubRe)
	send("SUB foo bar 2\r\nPING\r\n")
	expect(pongRe)
	leafExpect(lsubRe)

	// Now send again from leaf side. This is targeted so this should
	// not be delivered.
	leafSend("LMSG foo 2\r\nOK\r\n")
	expectNothing(t, lc)
	expectNothing(t, c)

	// Now send targeted, and we should receive it.
	leafSend("LMSG foo | bar 2\r\nOK\r\n")
	expectNothing(t, lc)

	matches = msgRe.FindAllSubmatch(expect(msgRe), -1)
	if len(matches) != 1 {
		t.Fatalf("Expected only 1 msg, got %d", len(matches))
	}
	checkMsg(t, matches[0], "foo", "2", "", "2", "OK")

	// Check reply + queues
	leafSend("LMSG foo + myreply bar 2\r\nOK\r\n")
	expectNothing(t, lc)

	matches = msgRe.FindAllSubmatch(expect(msgRe), -1)
	if len(matches) != 1 {
		t.Fatalf("Expected only 1 msg, got %d", len(matches))
	}
	checkMsg(t, matches[0], "foo", "2", "myreply", "2", "OK")
}

func TestLeafNodeAndRoutes(t *testing.T) {
	optsA := LoadConfig("./configs/srv_a_leaf.conf")
	optsA.DisableShortFirstPing = true
	optsB := LoadConfig("./configs/srv_b.conf")
	optsB.DisableShortFirstPing = true
	srvA := RunServer(optsA)
	defer srvA.Shutdown()
	srvB := RunServer(optsB)
	defer srvB.Shutdown()
	checkClusterFormed(t, srvA, srvB)

	lc := createLeafConn(t, optsA.LeafNode.Host, optsA.LeafNode.Port)
	defer lc.Close()

	leafSend, leafExpect := setupLeaf(t, lc, 6)
	leafSend("PING\r\n")
	leafExpect(pongRe)

	c := createClientConn(t, optsB.Host, optsB.Port)
	defer c.Close()

	send, expect := setupConn(t, c)
	send("PING\r\n")
	expect(pongRe)

	send("SUB foo 1\r\nPING\r\n")
	expect(pongRe)
	leafExpect(lsubRe)

	send("SUB foo 2\r\nPING\r\n")
	expect(pongRe)
	expectNothing(t, lc)

	send("UNSUB 2\r\nPING\r\n")
	expect(pongRe)
	expectNothing(t, lc)
	send("UNSUB 1\r\nPING\r\n")
	expect(pongRe)
	leafExpect(lunsubRe)

	// Now put it back and test msg flow.
	send("SUB foo 1\r\nPING\r\n")
	expect(pongRe)
	leafExpect(lsubRe)

	leafSend("LMSG foo + myreply bar 2\r\nOK\r\n")
	expectNothing(t, lc)

	matches := msgRe.FindAllSubmatch(expect(msgRe), -1)
	if len(matches) != 1 {
		t.Fatalf("Expected only 1 msg, got %d", len(matches))
	}
	checkMsg(t, matches[0], "foo", "1", "myreply", "2", "OK")

	// Now check reverse.
	leafSend("LS+ bar\r\n")
	expectNothing(t, lc)
	leafSend("PING\r\n")
	leafExpect(pongRe)

	send("PUB bar 2\r\nOK\r\n")
	matches = lmsgRe.FindAllSubmatch(leafExpect(lmsgRe), -1)
	if len(matches) != 1 {
		t.Fatalf("Expected only 1 msg, got %d", len(matches))
	}
	checkLmsg(t, matches[0], "bar", "", "2", "OK")
}

// Helper function to check that a leaf node has connected to our server.
func checkLeafNodeConnected(t *testing.T, s *server.Server) {
	t.Helper()
	checkLeafNodeConnections(t, s, 1)
}

func checkLeafNodeConnections(t *testing.T, s *server.Server, expected int) {
	t.Helper()
	checkFor(t, 5*time.Second, 100*time.Millisecond, func() error {
		if nln := s.NumLeafNodes(); nln != expected {
			return fmt.Errorf("Expected a connected leafnode for server %q, got %d", s.ID(), nln)
		}
		return nil
	})
}

func TestLeafNodeSolicit(t *testing.T) {
	s, opts := runLeafServer()
	defer s.Shutdown()

	sl, _ := runSolicitLeafServer(opts)
	defer sl.Shutdown()

	checkLeafNodeConnected(t, s)

	// Now test reconnect.
	s.Shutdown()
	// Need to restart it on the same port.
	s, _ = runLeafServerOnPort(opts.LeafNode.Port)
	defer s.Shutdown()
	checkLeafNodeConnected(t, s)
}

func TestLeafNodeNoEcho(t *testing.T) {
	s, opts := runLeafServer()
	defer s.Shutdown()

	c := createClientConn(t, opts.Host, opts.Port)
	defer c.Close()

	send, expect := setupConn(t, c)
	send("PING\r\n")
	expect(pongRe)

	lc := createLeafConn(t, opts.LeafNode.Host, opts.LeafNode.Port)
	defer lc.Close()

	leafSend, leafExpect := setupLeaf(t, lc, 1)
	leafSend("PING\r\n")
	leafExpect(pongRe)

	// We should not echo back to ourselves. Set up 'foo' subscriptions
	// on both sides and send message across the leafnode connection. It
	// should not come back.

	send("SUB foo 1\r\n")
	leafExpect(lsubRe)

	leafSend("LS+ foo\r\n")
	expectNothing(t, lc)
	leafSend("PING\r\n")
	leafExpect(pongRe)

	leafSend("LMSG foo 2\r\nOK\r\n")
	expectNothing(t, lc)
}

func TestLeafNodeHeaderSupport(t *testing.T) {
	srvA, optsA := runLeafServer()
	defer srvA.Shutdown()

	srvB, optsB := runSolicitLeafServer(optsA)
	defer srvB.Shutdown()

	clientA := createClientConn(t, optsA.Host, optsA.Port)
	defer clientA.Close()

	clientB := createClientConn(t, optsB.Host, optsB.Port)
	defer clientB.Close()

	sendA, expectA := setupHeaderConn(t, clientA)
	sendA("SUB foo bar 22\r\n")
	sendA("SUB bar 11\r\n")
	sendA("PING\r\n")
	expectA(pongRe)

	if err := checkExpectedSubs(3, srvB); err != nil {
		t.Fatalf("%v", err)
	}

	sendB, expectB := setupHeaderConn(t, clientB)
	// Can not have \r\n in payload fyi for regex.
	// With reply
	sendB("HPUB foo reply 12 14\r\nK1:V1,K2:V2 ok\r\n")
	sendB("PING\r\n")
	expectB(pongRe)

	expectHeaderMsgs := expectHeaderMsgsCommand(t, expectA)
	matches := expectHeaderMsgs(1)
	checkHmsg(t, matches[0], "foo", "22", "reply", "12", "14", "K1:V1,K2:V2 ", "ok")

	// Without reply
	sendB("HPUB foo 12 14\r\nK1:V1,K2:V2 ok\r\n")
	sendB("PING\r\n")
	expectB(pongRe)

	matches = expectHeaderMsgs(1)
	checkHmsg(t, matches[0], "foo", "22", "", "12", "14", "K1:V1,K2:V2 ", "ok")

	// Without queues or reply
	sendB("HPUB bar 12 14\r\nK1:V1,K2:V2 ok\r\n")
	sendB("PING\r\n")
	expectB(pongRe)

	matches = expectHeaderMsgs(1)
	checkHmsg(t, matches[0], "bar", "11", "", "12", "14", "K1:V1,K2:V2 ", "ok")

	// Without queues but with reply
	sendB("HPUB bar reply 12 14\r\nK1:V1,K2:V2 ok\r\n")
	sendB("PING\r\n")
	expectB(pongRe)

	matches = expectHeaderMsgs(1)
	checkHmsg(t, matches[0], "bar", "11", "reply", "12", "14", "K1:V1,K2:V2 ", "ok")
}

// Used to setup clusters of clusters for tests.
type cluster struct {
	servers []*server.Server
	opts    []*server.Options
	name    string
	t       *testing.T
}

func testDefaultClusterOptionsForLeafNodes() *server.Options {
	o := DefaultTestOptions
	o.Port = -1
	o.Cluster.Host = o.Host
	o.Cluster.Port = -1
	o.Gateway.Host = o.Host
	o.Gateway.Port = -1
	o.LeafNode.Host = o.Host
	o.LeafNode.Port = -1
	return &o
}

func (c *cluster) shutdown() {
	if c == nil {
		return
	}
	for i, s := range c.servers {
		if cf := c.opts[i].ConfigFile; cf != "" {
			os.RemoveAll(cf)
		}
		if sd := s.StoreDir(); sd != "" {
			os.RemoveAll(sd)
		}
		s.Shutdown()
	}
}

func shutdownCluster(c *cluster) {
	c.shutdown()
}

func (c *cluster) totalSubs() int {
	totalSubs := 0
	for _, s := range c.servers {
		totalSubs += int(s.NumSubscriptions())
	}
	return totalSubs
}

// Wait for the expected number of outbound gateways, or fails.
func waitForOutboundGateways(t *testing.T, s *server.Server, expected int, timeout time.Duration) {
	t.Helper()
	if timeout < 2*time.Second {
		timeout = 2 * time.Second
	}
	checkFor(t, timeout, 15*time.Millisecond, func() error {
		if n := s.NumOutboundGateways(); n != expected {
			return fmt.Errorf("Expected %v outbound gateway(s), got %v (ulimit -n too low?)",
				expected, n)
		}
		return nil
	})
}

// Creates a full cluster with numServers and given name and makes sure its well formed.
// Will have Gateways and Leaf Node connections active.
func createClusterWithName(t *testing.T, clusterName string, numServers int, connectTo ...*cluster) *cluster {
	t.Helper()
	return createClusterEx(t, false, 5*time.Millisecond, true, clusterName, numServers, connectTo...)
}

// Creates a cluster and optionally additional accounts and users.
// Will have Gateways and Leaf Node connections active.
func createClusterEx(t *testing.T, doAccounts bool, gwSolicit time.Duration, waitOnGWs bool, clusterName string, numServers int, connectTo ...*cluster) *cluster {
	t.Helper()

	if clusterName == "" || numServers < 1 {
		t.Fatalf("Bad params")
	}

	// Setup some accounts and users.
	// $SYS is always the system account. And we have default FOO and BAR accounts, as well
	// as DLC and NGS which do a service import.
	createAccountsAndUsers := func() ([]*server.Account, []*server.User) {
		if !doAccounts {
			return []*server.Account{server.NewAccount("$SYS")}, nil
		}

		sys := server.NewAccount("$SYS")
		ngs := server.NewAccount("NGS")
		dlc := server.NewAccount("DLC")
		foo := server.NewAccount("FOO")
		bar := server.NewAccount("BAR")

		accounts := []*server.Account{sys, foo, bar, ngs, dlc}

		ngs.AddServiceExport("ngs.usage.*", nil)
		dlc.AddServiceImport(ngs, "ngs.usage", "ngs.usage.dlc")

		// Setup users
		users := []*server.User{
			{Username: "dlc", Password: "pass", Permissions: nil, Account: dlc},
			{Username: "ngs", Password: "pass", Permissions: nil, Account: ngs},
			{Username: "foo", Password: "pass", Permissions: nil, Account: foo},
			{Username: "bar", Password: "pass", Permissions: nil, Account: bar},
			{Username: "sys", Password: "pass", Permissions: nil, Account: sys},
		}
		return accounts, users
	}

	bindGlobal := func(s *server.Server) {
		ngs, err := s.LookupAccount("NGS")
		if err != nil {
			return
		}
		// Bind global to service import
		gacc, _ := s.LookupAccount("$G")
		gacc.AddServiceImport(ngs, "ngs.usage", "ngs.usage.$G")
	}

	// If we are going to connect to another cluster set that up now for options.
	var gws []*server.RemoteGatewayOpts
	for _, c := range connectTo {
		// Gateways autodiscover here too, so just need one address from the set.
		gwAddr := fmt.Sprintf("nats-gw://%s:%d", c.opts[0].Gateway.Host, c.opts[0].Gateway.Port)
		gwurl, _ := url.Parse(gwAddr)
		gws = append(gws, &server.RemoteGatewayOpts{Name: c.name, URLs: []*url.URL{gwurl}})
	}

	// Make the GWs form faster for the tests.
	server.SetGatewaysSolicitDelay(gwSolicit)
	defer server.ResetGatewaysSolicitDelay()

	// Create seed first.
	o := testDefaultClusterOptionsForLeafNodes()
	o.Gateway.Name = clusterName
	o.Gateway.Gateways = gws
	// All of these need system accounts.
	o.Accounts, o.Users = createAccountsAndUsers()
	o.SystemAccount = "$SYS"
	o.ServerName = fmt.Sprintf("%s1", clusterName)
	// Run the server
	s := RunServer(o)
	bindGlobal(s)

	c := &cluster{servers: make([]*server.Server, 0, numServers), opts: make([]*server.Options, 0, numServers), name: clusterName}
	c.servers = append(c.servers, s)
	c.opts = append(c.opts, o)

	// For connecting to seed server above.
	routeAddr := fmt.Sprintf("nats-route://%s:%d", o.Cluster.Host, o.Cluster.Port)
	rurl, _ := url.Parse(routeAddr)
	routes := []*url.URL{rurl}

	for i := 1; i < numServers; i++ {
		o := testDefaultClusterOptionsForLeafNodes()
		o.Gateway.Name = clusterName
		o.Gateway.Gateways = gws
		o.Routes = routes
		// All of these need system accounts.
		o.Accounts, o.Users = createAccountsAndUsers()
		o.SystemAccount = "$SYS"
		o.ServerName = fmt.Sprintf("%s%d", clusterName, i+1)
		s := RunServer(o)
		bindGlobal(s)

		c.servers = append(c.servers, s)
		c.opts = append(c.opts, o)
	}
	checkClusterFormed(t, c.servers...)

	if waitOnGWs {
		// Wait on gateway connections if we were asked to connect to other gateways.
		if numGWs := len(connectTo); numGWs > 0 {
			for _, s := range c.servers {
				waitForOutboundGateways(t, s, numGWs, 2*time.Second)
			}
		}
	}
	c.t = t
	return c
}

func TestLeafNodeGatewayRequiresSystemAccount(t *testing.T) {
	o := testDefaultClusterOptionsForLeafNodes()
	o.Gateway.Name = "CLUSTER-A"
	_, err := server.NewServer(o)
	if err == nil {
		t.Fatalf("Expected an error with no system account defined")
	}
}

func TestLeafNodeGatewaySendsSystemEvent(t *testing.T) {
	server.SetGatewaysSolicitDelay(50 * time.Millisecond)
	defer server.ResetGatewaysSolicitDelay()

	ca := createClusterWithName(t, "A", 1)
	defer shutdownCluster(ca)
	cb := createClusterWithName(t, "B", 1, ca)
	defer shutdownCluster(cb)

	// Create client on a server in cluster A
	opts := ca.opts[0]
	c := createClientConn(t, opts.Host, opts.Port)
	defer c.Close()

	// Listen for the leaf node event.
	send, expect := setupConnWithAccount(t, ca.servers[0], c, "$SYS")
	send("SUB $SYS.ACCOUNT.$G.LEAFNODE.CONNECT 1\r\nPING\r\n")
	expect(pongRe)

	opts = cb.opts[0]
	lc := createLeafConn(t, opts.LeafNode.Host, opts.LeafNode.Port)
	defer lc.Close()

	// This is for our global responses since we are setting up GWs above.
	leafSend, leafExpect := setupLeaf(t, lc, 7)
	leafSend("PING\r\n")
	leafExpect(pongRe)

	matches := rawMsgRe.FindAllSubmatch(expect(rawMsgRe), -1)
	if len(matches) != 1 {
		t.Fatalf("Expected only 1 msg, got %d", len(matches))
	}
	m := matches[0]
	if string(m[subIndex]) != "$SYS.ACCOUNT.$G.LEAFNODE.CONNECT" {
		t.Fatalf("Got wrong subject for leaf node event, got %q, wanted %q",
			m[subIndex], "$SYS.ACCOUNT.$G.LEAFNODE.CONNECT")
	}
}

func TestLeafNodeGatewayInterestPropagation(t *testing.T) {
	server.SetGatewaysSolicitDelay(10 * time.Millisecond)
	defer server.ResetGatewaysSolicitDelay()

	ca := createClusterWithName(t, "A", 3)
	defer shutdownCluster(ca)
	cb := createClusterWithName(t, "B", 3, ca)
	defer shutdownCluster(cb)

	sl1, sl1Opts := runSolicitLeafServer(ca.opts[1])
	defer sl1.Shutdown()

	c := createClientConn(t, sl1Opts.Host, sl1Opts.Port)
	defer c.Close()

	send, expect := setupConn(t, c)
	send("SUB foo 1\r\n")
	send("PING\r\n")
	expect(pongRe)

	// Now we will create a new leaf node on cluster B, expect to get the
	// interest for "foo".
	opts := cb.opts[0]
	lc := createLeafConn(t, opts.LeafNode.Host, opts.LeafNode.Port)
	defer lc.Close()
	_, leafExpect := setupConn(t, lc)
	buf := leafExpect(infoStartRe)
	buf = infoStartRe.ReplaceAll(buf, []byte(nil))

	foundFoo := false
	for count := 0; count < 9; {
		// skip first time if we still have data (buf from above may already have some left)
		if count != 0 || len(buf) == 0 {
			buf = append(buf, leafExpect(anyRe)...)
		}
		count += len(lsubRe.FindAllSubmatch(buf, -1))
		if count > 9 {
			t.Fatalf("Expected %v matches, got %v (buf=%s)", 9, count, buf)
		}
		if strings.Contains(string(buf), "foo") {
			foundFoo = true
		}
		buf = lsubRe.ReplaceAll(buf, []byte(nil))
	}
	if len(buf) != 0 {
		t.Fatalf("did not consume everything, left with: %q", buf)
	}
	if !foundFoo {
		t.Fatalf("Expected interest for 'foo' as 'LS+ foo\\r\\n', got %q", buf)
	}
}

func TestLeafNodeAuthSystemEventNoCrash(t *testing.T) {
	ca := createClusterWithName(t, "A", 1)
	defer shutdownCluster(ca)

	opts := ca.opts[0]
	lc := createLeafConn(t, opts.LeafNode.Host, opts.LeafNode.Port)
	defer lc.Close()

	leafSend := sendCommand(t, lc)
	leafSend("LS+ foo\r\n")
	checkInfoMsg(t, lc)
}

func TestLeafNodeWithRouteAndGateway(t *testing.T) {
	server.SetGatewaysSolicitDelay(50 * time.Millisecond)
	defer server.ResetGatewaysSolicitDelay()

	ca := createClusterWithName(t, "A", 3)
	defer shutdownCluster(ca)
	cb := createClusterWithName(t, "B", 3, ca)
	defer shutdownCluster(cb)

	// Create client on a server in cluster A
	opts := ca.opts[0]
	c := createClientConn(t, opts.Host, opts.Port)
	defer c.Close()

	send, expect := setupConn(t, c)
	send("PING\r\n")
	expect(pongRe)

	// Create a leaf node connection on a server in cluster B
	opts = cb.opts[0]
	lc := createLeafConn(t, opts.LeafNode.Host, opts.LeafNode.Port)
	defer lc.Close()

	// This is for our global responses since we are setting up GWs above.
	leafSend, leafExpect := setupLeaf(t, lc, 7)
	leafSend("PING\r\n")
	leafExpect(pongRe)

	// Make sure we see interest graph propagation on the leaf node
	// connection. This is required since leaf nodes only send data
	// in the presence of interest.
	send("SUB foo 1\r\nPING\r\n")
	expect(pongRe)
	leafExpect(lsubRe)

	send("SUB foo 2\r\nPING\r\n")
	expect(pongRe)
	expectNothing(t, lc)

	send("UNSUB 2\r\n")
	expectNothing(t, lc)
	send("UNSUB 1\r\n")
	leafExpect(lunsubRe)

	// Now put it back and test msg flow.
	send("SUB foo 1\r\nPING\r\n")
	expect(pongRe)
	leafExpect(lsubRe)

	leafSend("LMSG foo 2\r\nOK\r\n")
	expectNothing(t, lc)

	matches := msgRe.FindAllSubmatch(expect(msgRe), -1)
	if len(matches) != 1 {
		t.Fatalf("Expected only 1 msg, got %d", len(matches))
	}
	checkMsg(t, matches[0], "foo", "1", "", "2", "OK")

	// Now check reverse.
	leafSend("LS+ bar\r\n")
	expectNothing(t, lc)
	leafSend("PING\r\n")
	leafExpect(pongRe)

	send("PUB bar 2\r\nOK\r\n")
	matches = lmsgRe.FindAllSubmatch(leafExpect(lmsgRe), -1)
	if len(matches) != 1 {
		t.Fatalf("Expected only 1 msg, got %d", len(matches))
	}
	checkLmsg(t, matches[0], "bar", "", "2", "OK")
}

// This will test that we propagate interest only mode after a leafnode
// has been established and a new server joins a remote cluster.
func TestLeafNodeWithGatewaysAndStaggeredStart(t *testing.T) {
	ca := createClusterWithName(t, "A", 3)
	defer shutdownCluster(ca)

	// Create the leafnode on a server in cluster A.
	opts := ca.opts[0]
	lc := createLeafConn(t, opts.LeafNode.Host, opts.LeafNode.Port)
	defer lc.Close()

	leafSend, leafExpect := setupLeaf(t, lc, 7)
	leafSend("PING\r\n")
	leafExpect(pongRe)

	// Now setup the cluster B.
	cb := createClusterWithName(t, "B", 3, ca)
	defer shutdownCluster(cb)

	// Create client on a server in cluster B
	opts = cb.opts[0]
	c := createClientConn(t, opts.Host, opts.Port)
	defer c.Close()

	send, expect := setupConn(t, c)
	send("PING\r\n")
	expect(pongRe)

	// Make sure we see interest graph propagation on the leaf node
	// connection. This is required since leaf nodes only send data
	// in the presence of interest.
	send("SUB foo 1\r\nPING\r\n")
	expect(pongRe)
	leafExpect(lsubRe)
}

// This will test that we propagate interest only mode after a leafnode
// has been established and a server is restarted..
func TestLeafNodeWithGatewaysServerRestart(t *testing.T) {
	ca := createClusterWithName(t, "A", 3)
	defer shutdownCluster(ca)

	// Now setup the cluster B.
	cb := createClusterWithName(t, "B", 3, ca)
	defer shutdownCluster(cb)

	// Create the leafnode on a server in cluster B.
	opts := cb.opts[1]
	lc := createLeafConn(t, opts.LeafNode.Host, opts.LeafNode.Port)
	defer lc.Close()

	leafSend, leafExpect := setupLeaf(t, lc, 7)
	leafSend("PING\r\n")
	leafExpect(pongRe)

	// Create client on a server in cluster A
	opts = ca.opts[1]
	c := createClientConn(t, opts.Host, opts.Port)
	defer c.Close()

	send, expect := setupConn(t, c)
	send("PING\r\n")
	expect(pongRe)

	// Make sure we see interest graph propagation on the leaf node
	// connection. This is required since leaf nodes only send data
	// in the presence of interest.
	send("SUB foo 1\r\nPING\r\n")
	expect(pongRe)
	leafExpect(lsubRe)

	// Close old leaf connection and simulate a reconnect.
	lc.Close()

	// Shutdown and recreate B and the leafnode connection to it.
	shutdownCluster(cb)

	// Create new cluster with longer solicit and don't wait for GW connect.
	cb = createClusterEx(t, false, 500*time.Millisecond, false, "B", 1, ca)
	defer shutdownCluster(cb)

	opts = cb.opts[0]
	lc = createLeafConn(t, opts.LeafNode.Host, opts.LeafNode.Port)
	defer lc.Close()

	_, leafExpect = setupLeaf(t, lc, 7)

	// Now wait on GW solicit to fire
	time.Sleep(500 * time.Millisecond)

	// We should see the interest for 'foo' here.
	leafExpect(lsubRe)
}

func TestLeafNodeLocalizedDQ(t *testing.T) {
	s, opts := runLeafServer()
	defer s.Shutdown()

	sl, slOpts := runSolicitLeafServer(opts)
	defer sl.Shutdown()

	checkLeafNodeConnected(t, s)

	c := createClientConn(t, slOpts.Host, slOpts.Port)
	defer c.Close()

	send, expect := setupConn(t, c)
	send("SUB foo bar 1\r\n")
	send("SUB foo bar 2\r\n")
	send("SUB foo bar 3\r\n")
	send("SUB foo bar 4\r\n")
	send("PING\r\n")
	expect(pongRe)

	// Now create another client on the main leaf server.
	sc := createClientConn(t, opts.Host, opts.Port)
	defer sc.Close()

	sendL, expectL := setupConn(t, sc)
	sendL("SUB foo bar 11\r\n")
	sendL("SUB foo bar 12\r\n")
	sendL("SUB foo bar 13\r\n")
	sendL("SUB foo bar 14\r\n")
	sendL("PING\r\n")
	expectL(pongRe)

	for i := 0; i < 10; i++ {
		send("PUB foo 2\r\nOK\r\n")
	}
	expectNothing(t, sc)

	matches := msgRe.FindAllSubmatch(expect(msgRe), -1)
	if len(matches) != 10 {
		t.Fatalf("Expected 10 msgs, got %d", len(matches))
	}
	for i := 0; i < 10; i++ {
		checkMsg(t, matches[i], "foo", "", "", "2", "OK")
	}
}

func TestLeafNodeBasicAuth(t *testing.T) {
	content := `
    listen: "127.0.0.1:-1"

	leafnodes {
		listen: "127.0.0.1:-1"
		authorization {
			user: "derek"
			password: "s3cr3t!"
			timeout: 2.2
		}
	}
	`
	conf := createConfFile(t, []byte(content))

	s, opts := RunServerWithConfig(conf)
	defer s.Shutdown()

	lc := createLeafConn(t, opts.LeafNode.Host, opts.LeafNode.Port)
	defer lc.Close()

	// This should fail since we want u/p
	setupConn(t, lc)
	errBuf := expectResult(t, lc, errRe)
	if !strings.Contains(string(errBuf), "Authorization Violation") {
		t.Fatalf("Authentication Timeout response incorrect: %q", errBuf)
	}
	expectDisconnect(t, lc)

	// Try bad password as well.
	lc = createLeafConn(t, opts.LeafNode.Host, opts.LeafNode.Port)
	defer lc.Close()

	// This should fail since we want u/p
	setupConnWithUserPass(t, lc, "derek", "badpassword")
	errBuf = expectResult(t, lc, errRe)
	if !strings.Contains(string(errBuf), "Authorization Violation") {
		t.Fatalf("Authentication Timeout response incorrect: %q", errBuf)
	}
	expectDisconnect(t, lc)

	// This one should work.
	lc = createLeafConn(t, opts.LeafNode.Host, opts.LeafNode.Port)
	defer lc.Close()
	leafSend, leafExpect := setupConnWithUserPass(t, lc, "derek", "s3cr3t!")
	leafExpect(infoRe)
	leafExpect(lsubRe)
	leafSend("PING\r\n")
	expectResult(t, lc, pongRe)

	checkLeafNodeConnected(t, s)
}

func runTLSSolicitLeafServer(lso *server.Options) (*server.Server, *server.Options) {
	o := DefaultTestOptions
	o.Port = -1
	rurl, _ := url.Parse(fmt.Sprintf("nats-leaf://%s:%d", lso.LeafNode.Host, lso.LeafNode.Port))
	remote := &server.RemoteLeafOpts{URLs: []*url.URL{rurl}}
	remote.TLSConfig = &tls.Config{MinVersion: tls.VersionTLS12}
	host, _, _ := net.SplitHostPort(lso.LeafNode.Host)
	remote.TLSConfig.ServerName = host
	remote.TLSConfig.InsecureSkipVerify = true
	remote.Compression.Mode = server.CompressionOff
	o.LeafNode.Remotes = []*server.RemoteLeafOpts{remote}
	o.LeafNode.Compression.Mode = server.CompressionOff
	return RunServer(&o), &o
}

func TestLeafNodeTLS(t *testing.T) {
	content := `
	listen: "127.0.0.1:-1"

	leafnodes {
		listen: "127.0.0.1:-1"
		tls {
			cert_file: "./configs/certs/server-cert.pem"
			key_file: "./configs/certs/server-key.pem"
			timeout: 0.1
		}
	}
	`
	conf := createConfFile(t, []byte(content))

	s, opts := RunServerWithConfig(conf)
	defer s.Shutdown()

	lc := createLeafConn(t, opts.LeafNode.Host, opts.LeafNode.Port)
	defer lc.Close()

	info := checkInfoMsg(t, lc)
	if !info.TLSRequired {
		t.Fatalf("Expected TLSRequired to be true")
	}
	if info.TLSVerify {
		t.Fatalf("Expected TLSVerify to be false")
	}
	// We should get a disconnect here since we have not upgraded to TLS.
	expectDisconnect(t, lc)

	// This should work ok.
	sl, _ := runTLSSolicitLeafServer(opts)
	defer sl.Shutdown()

	checkLeafNodeConnected(t, s)
}

func TestLeafNodeTLSConnCloseEarly(t *testing.T) {
	content := `
	listen: "127.0.0.1:-1"

	leafnodes {
		listen: "127.0.0.1:-1"
		tls {
			cert_file: "./configs/certs/server-cert.pem"
			key_file: "./configs/certs/server-key.pem"
			timeout: 2.0
		}
	}
	`
	conf := createConfFile(t, []byte(content))

	s, opts := RunServerWithConfig(conf)
	defer s.Shutdown()

	lc, err := net.Dial("tcp", fmt.Sprintf("127.0.0.1:%d", opts.LeafNode.Port))
	if err != nil {
		t.Fatalf("Unable to connect: %v", err)
	}
	// Then close right away
	lc.Close()

	// Check server does not crash...
	time.Sleep(250 * time.Millisecond)
	if s.ID() == "" {
		t.Fatalf("should not happen")
	}
}

type captureLeafNodeErrLogger struct {
	dummyLogger
	ch chan string
}

func (c *captureLeafNodeErrLogger) Errorf(format string, v ...any) {
	msg := fmt.Sprintf(format, v...)
	select {
	case c.ch <- msg:
	default:
	}
}

func TestLeafNodeTLSMixIP(t *testing.T) {
	content := `
	listen: "127.0.0.1:-1"
	leafnodes {
		listen: "127.0.0.1:-1"
        authorization {
			user: dlc
			pass: monkey
		}
		tls {
			cert_file: "./configs/certs/server-noip.pem"
			key_file:  "./configs/certs/server-key-noip.pem"
			ca_file:   "./configs/certs/ca.pem"
		}
	}
	`
	conf := createConfFile(t, []byte(content))

	s, opts := RunServerWithConfig(conf)
	defer s.Shutdown()

	slContent := `
	listen: "127.0.0.1:-1"
	leafnodes {
		reconnect: 1
		remotes: [
			{
				url: [tls://127.0.0.1:%d, "tls://localhost:%d"]
				tls { ca_file: "./configs/certs/ca.pem" }
			}
		]
	}
	`
	slconf := createConfFile(t, []byte(fmt.Sprintf(slContent, opts.LeafNode.Port, opts.LeafNode.Port)))

	// This will fail but we want to make sure in the correct way, not with
	// TLS issue because we used an IP for serverName.
	sl, _ := RunServerWithConfig(slconf)
	defer sl.Shutdown()

	ll := &captureLeafNodeErrLogger{ch: make(chan string, 2)}
	sl.SetLogger(ll, false, false)

	// We may or may not get an error depending on timing. For the handshake bug
	// we would always get it, so make sure if we have anything it is not that.
	select {
	case msg := <-ll.ch:
		if strings.Contains(msg, "TLS handshake error") && strings.Contains(msg, "doesn't contain any IP SANs") {
			t.Fatalf("Got bad error about TLS handshake")
		}
	default:
	}
}

func runLeafNodeOperatorServer(t *testing.T) (*server.Server, *server.Options, string) {
	t.Helper()
	content := `
	port: -1
	server_name: OP
	operator = "./configs/nkeys/op.jwt"
	resolver = MEMORY
	listen: "127.0.0.1:-1"
	leafnodes {
		listen: "127.0.0.1:-1"
	}
	`
	conf := createConfFile(t, []byte(content))
	s, opts := RunServerWithConfig(conf)
	return s, opts, conf
}

func genCredsFile(t *testing.T, jwt string, seed []byte) string {
	creds := `
		-----BEGIN NATS USER JWT-----
		%s
		------END NATS USER JWT------

		************************* IMPORTANT *************************
		NKEY Seed printed below can be used to sign and prove identity.
		NKEYs are sensitive and should be treated as secrets.

		-----BEGIN USER NKEY SEED-----
		%s
		------END USER NKEY SEED------

		*************************************************************
		`
	return createConfFile(t, []byte(strings.Replace(fmt.Sprintf(creds, jwt, seed), "\t\t", "", -1)))
}

func runSolicitWithCredentials(t *testing.T, opts *server.Options, creds string) (*server.Server, *server.Options, string) {
	content := `
		port: -1
		leafnodes {
			remotes = [
				{
					url: nats-leaf://127.0.0.1:%d
					credentials: '%s'
				}
			]
		}
		`
	config := fmt.Sprintf(content, opts.LeafNode.Port, creds)
	conf := createConfFile(t, []byte(config))
	s, opts := RunServerWithConfig(conf)
	return s, opts, conf
}

func TestLeafNodeOperatorModel(t *testing.T) {
	s, opts, _ := runLeafNodeOperatorServer(t)
	defer s.Shutdown()

	// Make sure we get disconnected without proper credentials etc.
	lc := createLeafConn(t, opts.LeafNode.Host, opts.LeafNode.Port)
	defer lc.Close()

	// This should fail since we want user jwt, signed nonce etc.
	setupConn(t, lc)
	errBuf := expectResult(t, lc, errRe)
	if !strings.Contains(string(errBuf), "Authorization Violation") {
		t.Fatalf("Authentication Timeout response incorrect: %q", errBuf)
	}
	expectDisconnect(t, lc)

	// Setup account and a user that will be used by the remote leaf node server.
	// createAccount automatically registers with resolver etc..
	_, akp := createAccount(t, s)
	kp, _ := nkeys.CreateUser()
	pub, _ := kp.PublicKey()
	nuc := jwt.NewUserClaims(pub)
	ujwt, err := nuc.Encode(akp)
	if err != nil {
		t.Fatalf("Error generating user JWT: %v", err)
	}
	seed, _ := kp.Seed()
	mycreds := genCredsFile(t, ujwt, seed)

	sl, _, _ := runSolicitWithCredentials(t, opts, mycreds)
	defer sl.Shutdown()

	checkLeafNodeConnected(t, s)
}

func TestLeafNodeUserPermsForConnection(t *testing.T) {
	s, opts, _ := runLeafNodeOperatorServer(t)
	defer s.Shutdown()

	// Setup account and a user that will be used by the remote leaf node server.
	// createAccount automatically registers with resolver etc..
	acc, akp := createAccount(t, s)
	kp, _ := nkeys.CreateUser()
	pub, _ := kp.PublicKey()
	nuc := jwt.NewUserClaims(pub)
	nuc.Permissions.Pub.Allow.Add("foo.>")
	nuc.Permissions.Pub.Allow.Add("baz.>")
	nuc.Permissions.Sub.Allow.Add("foo.>")
	// we would be immediately disconnected if that would not work
	nuc.Permissions.Sub.Deny.Add("$SYS.>")
	ujwt, err := nuc.Encode(akp)
	if err != nil {
		t.Fatalf("Error generating user JWT: %v", err)
	}
	seed, _ := kp.Seed()
	mycreds := genCredsFile(t, ujwt, seed)

	content := `
		port: -1
		leafnodes {
			remotes = [
				{
					url: nats-leaf://127.0.0.1:%d
					credentials: '%s'
					deny_import: "foo.33"
					deny_export: "foo.33"
				}
			]
		}
		`
	config := fmt.Sprintf(content, opts.LeafNode.Port, mycreds)
	lnconf := createConfFile(t, []byte(config))
	sl, _ := RunServerWithConfig(lnconf)
	defer sl.Shutdown()

	checkLeafNodeConnected(t, s)

	// Create credentials for a normal unrestricted user that we will connect to the op server.
	url := fmt.Sprintf("nats://%s:%d", opts.Host, opts.Port)
	nc, err := nats.Connect(url, createUserCreds(t, s, akp))
	if err != nil {
		t.Fatalf("Error on connect: %v", err)
	}
	defer nc.Close()

	// Create a user on the leafnode server that solicited.
	nc2, err := nats.Connect(sl.ClientURL())
	if err != nil {
		t.Fatalf("Error on connect: %v", err)
	}
	defer nc2.Close()

	// Make sure subscriptions properly do or do not make it to the hub.
	nc2.SubscribeSync("bar")
	checkNoSubInterest(t, s, acc.GetName(), "bar", 20*time.Millisecond)
	// This one should.
	nc2.SubscribeSync("foo.22")
	checkSubInterest(t, s, acc.GetName(), "foo.22", 100*time.Millisecond)

	// Capture everything.
	sub, _ := nc.SubscribeSync(">")
	nc.Flush()

	checkSubInterest(t, sl, "$G", ">", 100*time.Millisecond)
	// Now check local pubs are not forwarded.
	nc2.Publish("baz.22", nil)
	m, err := sub.NextMsg(1 * time.Second)
	if err != nil || m.Subject != "baz.22" {
		t.Fatalf("Expected to received this message")
	}
	nc2.Publish("bar.22", nil)
	if _, err := sub.NextMsg(100 * time.Millisecond); err == nil {
		t.Fatalf("Did not expect to receive this message")
	}

	// Check local overrides work.
	nc2.Publish("foo.33", nil)
	if _, err := sub.NextMsg(100 * time.Millisecond); err == nil {
		t.Fatalf("Did not expect to receive this message")
	}

	// This would trigger the sub interest below.
	sub.Unsubscribe()
	nc.Flush()

	nc2.SubscribeSync("foo.33")
	checkNoSubInterest(t, s, acc.GetName(), "foo.33", 20*time.Millisecond)
}

func TestLeafNodeMultipleAccounts(t *testing.T) {
	// So we will create a main server with two accounts. The remote server, acting as a leaf node, will simply have
	// the $G global account and no auth. Make sure things work properly here.
	s, opts, _ := runLeafNodeOperatorServer(t)
	defer s.Shutdown()

	// Setup the two accounts for this server.
	a, akp1 := createAccount(t, s)
	kp1, _ := nkeys.CreateUser()
	pub1, _ := kp1.PublicKey()
	nuc1 := jwt.NewUserClaims(pub1)
	ujwt1, err := nuc1.Encode(akp1)
	if err != nil {
		t.Fatalf("Error generating user JWT: %v", err)
	}

	// Create second account.
	createAccount(t, s)

	// Create the leaf node server using the first account.
	seed, _ := kp1.Seed()
	mycreds := genCredsFile(t, ujwt1, seed)

	sl, lopts, _ := runSolicitWithCredentials(t, opts, mycreds)
	defer sl.Shutdown()

	checkLeafNodeConnected(t, s)

	// To connect to main server.
	url := fmt.Sprintf("nats://%s:%d", opts.Host, opts.Port)

	nc1, err := nats.Connect(url, createUserCreds(t, s, akp1))
	if err != nil {
		t.Fatalf("Error on connect: %v", err)
	}
	defer nc1.Close()

	// This is a client connected to the leaf node with no auth,
	// binding to account1 via leafnode connection.
	// To connect to leafnode server.
	lurl := fmt.Sprintf("nats://%s:%d", lopts.Host, lopts.Port)
	ncl, err := nats.Connect(lurl)
	if err != nil {
		t.Fatalf("Error on connect: %v", err)
	}
	defer ncl.Close()

	lsub, _ := ncl.SubscribeSync("foo.test")

	// Wait for the subs to propagate. LDS + foo.test
	checkSubInterest(t, s, a.GetName(), "foo.test", 2*time.Second)

	// Now send from nc1 with account 1, should be received by our leafnode subscriber.
	nc1.Publish("foo.test", nil)

	_, err = lsub.NextMsg(1 * time.Second)
	if err != nil {
		t.Fatalf("Error during wait for next message: %s", err)
	}
}

func TestLeafNodeOperatorAndPermissions(t *testing.T) {
	s, opts, conf := runLeafNodeOperatorServer(t)
	defer os.Remove(conf)
	defer s.Shutdown()

	_, akp := createAccount(t, s)
	kp, _ := nkeys.CreateUser()
	pub, _ := kp.PublicKey()

	// Create SRV user, with no limitations
	srvnuc := jwt.NewUserClaims(pub)
	srvujwt, err := srvnuc.Encode(akp)
	if err != nil {
		t.Fatalf("Error generating user JWT: %v", err)
	}
	seed, _ := kp.Seed()
	srvcreds := genCredsFile(t, srvujwt, seed)
	defer os.Remove(srvcreds)

	// Create connection for SRV
	srvnc, err := nats.Connect(s.ClientURL(), nats.UserCredentials(srvcreds))
	if err != nil {
		t.Fatalf("Error on connect: %v", err)
	}
	defer srvnc.Close()

	// Create on the server "s" a subscription on "*" and on "foo".
	// We check that the subscription on "*" will be able to receive
	// messages since LEAF has publish permissions on "foo", so msg
	// should be received.
	srvsubStar, err := srvnc.SubscribeSync("*")
	if err != nil {
		t.Fatalf("Error on subscribe: %v", err)
	}
	srvsubFoo, err := srvnc.SubscribeSync("foo")
	if err != nil {
		t.Fatalf("Error on subscribe: %v", err)
	}
	srvnc.Flush()

	// Create LEAF user, with pub perms on "foo" and sub perms on "bar"
	leafnuc := jwt.NewUserClaims(pub)
	leafnuc.Permissions.Pub.Allow.Add("foo")
	leafnuc.Permissions.Sub.Allow.Add("bar")
	leafnuc.Permissions.Sub.Allow.Add("baz")
	leafujwt, err := leafnuc.Encode(akp)
	if err != nil {
		t.Fatalf("Error generating user JWT: %v", err)
	}
	leafcreds := genCredsFile(t, leafujwt, seed)
	defer os.Remove(leafcreds)

	content := `
		port: -1
		server_name: LN
		leafnodes {
			remotes = [
				{
					url: nats-leaf://127.0.0.1:%d
					credentials: '%s'
				}
			]
		}
		`
	config := fmt.Sprintf(content, opts.LeafNode.Port, leafcreds)
	lnconf := createConfFile(t, []byte(config))
	defer os.Remove(lnconf)
	sl, _ := RunServerWithConfig(lnconf)
	defer sl.Shutdown()

	checkLeafNodeConnected(t, s)

	// Check that interest makes it to "sl" server.
	// This helper does not check for wildcard interest...
	checkSubInterest(t, sl, "$G", "foo", time.Second)

	// Create connection for LEAF and subscribe on "bar"
	leafnc, err := nats.Connect(sl.ClientURL(), nats.UserCredentials(leafcreds))
	if err != nil {
		t.Fatalf("Error on connect: %v", err)
	}
	defer leafnc.Close()

	leafsub, err := leafnc.SubscribeSync("bar")
	if err != nil {
		t.Fatalf("Error on subscribe: %v", err)
	}

	// To check that we can pull in 'baz'.
	leafsubpwc, err := leafnc.SubscribeSync("*")
	if err != nil {
		t.Fatalf("Error on subscribe: %v", err)
	}
	leafnc.Flush()

	// Make sure the interest on "bar" and "*" from "sl" server makes it to the "s" server.
	// Can't use checkSubInterest since the local sub on "*" on "s" already matches.
	checkFor(t, time.Second, 15*time.Millisecond, func() error {
		leafz, err := s.Leafz(&server.LeafzOptions{Subscriptions: true})
		if err != nil {
			return err
		}
		if len(leafz.Leafs) != 1 {
			return fmt.Errorf("expected 1 leaf, got %d", len(leafz.Leafs))
		}
		subs := leafz.Leafs[0].Subs
		if !slices.Contains(subs, "bar") || !slices.Contains(subs, "*") {
			return fmt.Errorf("leaf interest not registered yet: %v", subs)
		}
		return nil
	})
	// Check for local interest too.
	checkSubInterest(t, sl, "$G", "bar", time.Second)

	// Now that we know that "s" has received interest on "bar", create
	// the sub on "bar" locally on "s"
	srvsub, err := srvnc.SubscribeSync("bar")
	if err != nil {
		t.Fatalf("Error on subscribe: %v", err)
	}

	srvnc.Publish("bar", []byte("hello"))
	if _, err := srvsub.NextMsg(time.Second); err != nil {
		t.Fatalf("SRV did not get message: %v", err)
	}
	if _, err := leafsub.NextMsg(time.Second); err != nil {
		t.Fatalf("LEAF did not get message: %v", err)
	}
	if _, err := leafsubpwc.NextMsg(time.Second); err != nil {
		t.Fatalf("LEAF did not get message: %v", err)
	}

	// The leafnode has a sub on '*', that should pull in a publish to 'baz'.
	srvnc.Publish("baz", []byte("hello"))
	if _, err := leafsubpwc.NextMsg(time.Second); err != nil {
		t.Fatalf("LEAF did not get message: %v", err)
	}

	// User LEAF user on "sl" server, publish on "foo"
	leafnc.Publish("foo", []byte("hello"))
	// The user SRV on "s" receives it because the LN connection
	// is allowed to publish on "foo".
	if _, err := srvsubFoo.NextMsg(time.Second); err != nil {
		t.Fatalf("SRV did not get message: %v", err)
	}
	// The wildcard subscription should get it too.
	if _, err := srvsubStar.NextMsg(time.Second); err != nil {
		t.Fatalf("SRV did not get message: %v", err)
	}

	// However, even when using an unrestricted user connects to "sl" and
	// publishes on "bar", the user SRV on "s" should not receive it because
	// the LN connection is not allowed to publish (send msg over) on "bar".
	nc, err := nats.Connect(sl.ClientURL(), nats.UserCredentials(srvcreds))
	if err != nil {
		t.Fatalf("Error on connect: %v", err)
	}
	defer nc.Close()
	nc.Publish("bar", []byte("should not received"))
	if _, err := srvsub.NextMsg(250 * time.Millisecond); err == nil {
		t.Fatal("Should not have received message")
	}
}

func TestLeafNodeSignerUser(t *testing.T) {
	s, opts, _ := runLeafNodeOperatorServer(t)
	defer s.Shutdown()

	// Setup the two accounts for this server.
	_, akp1 := createAccount(t, s)
	apk1, _ := akp1.PublicKey()

	// add a signing key to the account
	akp2, err := nkeys.CreateAccount()
	if err != nil {
		t.Fatal(err)
	}
	apk2, _ := akp2.PublicKey()

	token, err := s.AccountResolver().Fetch(apk1)
	if err != nil {
		t.Fatal(err)
	}
	ac, err := jwt.DecodeAccountClaims(token)
	if err != nil {
		t.Fatal(err)
	}

	ac.SigningKeys.Add(apk2)
	okp, _ := nkeys.FromSeed(oSeed)
	token, err = ac.Encode(okp)
	if err != nil {
		t.Fatal(err)
	}
	// update the resolver
	account, _ := s.LookupAccount(apk1)
	err = s.AccountResolver().Store(apk1, token)
	if err != nil {
		t.Fatal(err)
	}
	s.UpdateAccountClaims(account, ac)

	tt, err := s.AccountResolver().Fetch(apk1)
	if err != nil {
		t.Fatal(err)
	}
	ac2, err := jwt.DecodeAccountClaims(tt)
	if err != nil {
		t.Fatal(err)
	}
	if len(ac2.SigningKeys) != 1 {
		t.Fatal("signing key is not added")
	}
	if _, ok := ac2.SigningKeys[apk2]; !ok {
		t.Fatal("signing key is not added")
	}

	// create an user signed by the signing key
	kp1, _ := nkeys.CreateUser()
	pub1, _ := kp1.PublicKey()
	nuc1 := jwt.NewUserClaims(pub1)
	nuc1.IssuerAccount = apk1
	ujwt1, err := nuc1.Encode(akp2)
	if err != nil {
		t.Fatalf("Error generating user JWT: %v", err)
	}

	// Create the leaf node server using the first account.
	seed, _ := kp1.Seed()
	mycreds := genCredsFile(t, ujwt1, seed)

	sl, _, _ := runSolicitWithCredentials(t, opts, mycreds)
	defer sl.Shutdown()

	checkLeafNodeConnected(t, s)
}

func TestLeafNodeExportsImports(t *testing.T) {
	// So we will create a main server with two accounts. The remote server, acting as a leaf node, will simply have
	// the $G global account and no auth. Make sure things work properly here.
	s, opts, _ := runLeafNodeOperatorServer(t)
	defer s.Shutdown()

	// Setup the two accounts for this server.
	okp, _ := nkeys.FromSeed(oSeed)

	// Create second account with exports
	acc2, akp2 := createAccount(t, s)
	akp2Pub, _ := akp2.PublicKey()
	akp2AC := jwt.NewAccountClaims(akp2Pub)
	streamExport := &jwt.Export{Subject: "foo.stream", Type: jwt.Stream}
	serviceExport := &jwt.Export{Subject: "req.echo", Type: jwt.Service}
	akp2AC.Exports.Add(streamExport, serviceExport)
	akp2ACJWT, err := akp2AC.Encode(okp)
	if err != nil {
		t.Fatalf("Error generating account JWT: %v", err)
	}

	if err := s.AccountResolver().Store(akp2Pub, akp2ACJWT); err != nil {
		t.Fatalf("Account Resolver returned an error: %v", err)
	}
	s.UpdateAccountClaims(acc2, akp2AC)

	// Now create the first account and add on the imports. This will be what is used in the leafnode.
	acc1, akp1 := createAccount(t, s)
	akp1Pub, _ := akp1.PublicKey()
	akp1AC := jwt.NewAccountClaims(akp1Pub)
	streamImport := &jwt.Import{Account: akp2Pub, Subject: "foo.stream", To: "import", Type: jwt.Stream}
	serviceImport := &jwt.Import{Account: akp2Pub, Subject: "import.request", To: "req.echo", Type: jwt.Service}
	akp1AC.Imports.Add(streamImport, serviceImport)
	akp1ACJWT, err := akp1AC.Encode(okp)
	if err != nil {
		t.Fatalf("Error generating account JWT: %v", err)
	}
	if err := s.AccountResolver().Store(akp1Pub, akp1ACJWT); err != nil {
		t.Fatalf("Account Resolver returned an error: %v", err)
	}
	s.UpdateAccountClaims(acc1, akp1AC)

	// Create the user will we use to connect the leafnode.
	kp1, _ := nkeys.CreateUser()
	pub1, _ := kp1.PublicKey()
	nuc1 := jwt.NewUserClaims(pub1)
	ujwt1, err := nuc1.Encode(akp1)
	if err != nil {
		t.Fatalf("Error generating user JWT: %v", err)
	}

	// Create the leaf node server using the first account.
	seed, _ := kp1.Seed()
	mycreds := genCredsFile(t, ujwt1, seed)

	sl, lopts, _ := runSolicitWithCredentials(t, opts, mycreds)
	defer sl.Shutdown()

	checkLeafNodeConnected(t, s)

	// To connect to main server.
	url := fmt.Sprintf("nats://%s:%d", opts.Host, opts.Port)

	// Imported
	nc1, err := nats.Connect(url, createUserCreds(t, s, akp1))
	if err != nil {
		t.Fatalf("Error on connect: %v", err)
	}
	defer nc1.Close()

	// Exported
	nc2, err := nats.Connect(url, createUserCreds(t, s, akp2))
	if err != nil {
		t.Fatalf("Error on connect: %v", err)
	}
	defer nc2.Close()

	// Leaf node connection.
	lurl := fmt.Sprintf("nats://%s:%d", lopts.Host, lopts.Port)
	ncl, err := nats.Connect(lurl)
	if err != nil {
		t.Fatalf("Error on connect: %v", err)
	}
	defer ncl.Close()

	// So everything should be setup here. So let's test streams first.
	lsub, _ := ncl.SubscribeSync("import.foo.stream")

	// Wait for all subs to propagate.
	checkSubInterest(t, s, acc1.GetName(), "import.foo.stream", time.Second)

	// Pub to other account with export on original subject.
	nc2.Publish("foo.stream", nil)

	_, err = lsub.NextMsg(1 * time.Second)
	if err != nil {
		t.Fatalf("Error during wait for next message: %s", err)
	}

	// Services
	// Create listener on nc2
	nc2.Subscribe("req.echo", func(msg *nats.Msg) {
		nc2.Publish(msg.Reply, []byte("WORKED"))
	})
	nc2.Flush()

	// Now send the request on the leaf node client.
	if _, err := ncl.Request("import.request", []byte("fingers crossed"), 500*time.Millisecond); err != nil {
		t.Fatalf("Did not receive response: %v", err)
	}
}

func TestLeafNodeExportImportComplexSetup(t *testing.T) {
	content := `
	port: -1
	operator = "./configs/nkeys/op.jwt"
	resolver = MEMORY
	cluster {
		port: -1
		name: xyz
	}
	leafnodes {
		listen: "127.0.0.1:-1"
	}
	`
	conf := createConfFile(t, []byte(content))
	s1, s1Opts := RunServerWithConfig(conf)
	defer s1.Shutdown()

	content = fmt.Sprintf(`
	port: -1
	operator = "./configs/nkeys/op.jwt"
	resolver = MEMORY
	cluster {
		port: -1
		name: xyz
		routes: ["nats://%s:%d"]
	}
	leafnodes {
		listen: "127.0.0.1:-1"
	}
	`, s1Opts.Cluster.Host, s1Opts.Cluster.Port)
	conf = createConfFile(t, []byte(content))
	s2, s2Opts := RunServerWithConfig(conf)
	defer s2.Shutdown()

	// Setup the two accounts for this server.
	okp, _ := nkeys.FromSeed(oSeed)

	// Create second account with exports
	acc2, akp2 := createAccount(t, s1)
	akp2Pub, _ := akp2.PublicKey()
	akp2AC := jwt.NewAccountClaims(akp2Pub)
	streamExport := &jwt.Export{Subject: "foo.stream", Type: jwt.Stream}
	serviceExport := &jwt.Export{Subject: "req.echo", Type: jwt.Service}
	akp2AC.Exports.Add(streamExport, serviceExport)
	akp2ACJWT, err := akp2AC.Encode(okp)
	if err != nil {
		t.Fatalf("Error generating account JWT: %v", err)
	}

	if err := s1.AccountResolver().Store(akp2Pub, akp2ACJWT); err != nil {
		t.Fatalf("Account Resolver returned an error: %v", err)
	}
	s1.UpdateAccountClaims(acc2, akp2AC)

	// Now create the first account and add on the imports. This will be what is used in the leafnode.
	acc1, akp1 := createAccount(t, s1)
	akp1Pub, _ := akp1.PublicKey()
	akp1AC := jwt.NewAccountClaims(akp1Pub)
	streamImport := &jwt.Import{Account: akp2Pub, Subject: "foo.stream", To: "import", Type: jwt.Stream}
	serviceImport := &jwt.Import{Account: akp2Pub, Subject: "import.request", To: "req.echo", Type: jwt.Service}
	akp1AC.Imports.Add(streamImport, serviceImport)
	akp1ACJWT, err := akp1AC.Encode(okp)
	if err != nil {
		t.Fatalf("Error generating account JWT: %v", err)
	}
	if err := s1.AccountResolver().Store(akp1Pub, akp1ACJWT); err != nil {
		t.Fatalf("Account Resolver returned an error: %v", err)
	}
	s1.UpdateAccountClaims(acc1, akp1AC)

	if err := s2.AccountResolver().Store(akp2Pub, akp2ACJWT); err != nil {
		t.Fatalf("Account Resolver returned an error: %v", err)
	}
	// Just make sure that account object registered in S2 is not acc2
	if a, err := s2.LookupAccount(acc2.Name); err != nil || a == acc2 {
		t.Fatalf("Lookup account error: %v - accounts are same: %v", err, a == acc2)
	}

	if err := s2.AccountResolver().Store(akp1Pub, akp1ACJWT); err != nil {
		t.Fatalf("Account Resolver returned an error: %v", err)
	}
	// Just make sure that account object registered in S2 is not acc1
	if a, err := s2.LookupAccount(acc1.Name); err != nil || a == acc1 {
		t.Fatalf("Lookup account error: %v - accounts are same: %v", err, a == acc1)
	}

	// Create the user will we use to connect the leafnode.
	kp1, _ := nkeys.CreateUser()
	pub1, _ := kp1.PublicKey()
	nuc1 := jwt.NewUserClaims(pub1)
	ujwt1, err := nuc1.Encode(akp1)
	if err != nil {
		t.Fatalf("Error generating user JWT: %v", err)
	}

	// Create the leaf node server using the first account.
	seed, _ := kp1.Seed()
	mycreds := genCredsFile(t, ujwt1, seed)

	sl, lopts, _ := runSolicitWithCredentials(t, s1Opts, mycreds)
	defer sl.Shutdown()

	checkLeafNodeConnected(t, s1)

	// Url to server s2
	s2URL := fmt.Sprintf("nats://%s:%d", s2Opts.Host, s2Opts.Port)

	// Imported
	nc1, err := nats.Connect(s2URL, createUserCreds(t, s2, akp1))
	if err != nil {
		t.Fatalf("Error on connect: %v", err)
	}
	defer nc1.Close()

	// Exported
	nc2, err := nats.Connect(s2URL, createUserCreds(t, s2, akp2))
	if err != nil {
		t.Fatalf("Error on connect: %v", err)
	}
	defer nc2.Close()

	// Leaf node connection.
	lurl := fmt.Sprintf("nats://%s:%d", lopts.Host, lopts.Port)
	ncl, err := nats.Connect(lurl)
	if err != nil {
		t.Fatalf("Error on connect: %v", err)
	}
	defer ncl.Close()

	// So everything should be setup here. So let's test streams first.
	lsub, _ := ncl.SubscribeSync("import.foo.stream")

	// Wait for the sub to propagate to s2. LDS + subject above.
	checkFor(t, 2*time.Second, 15*time.Millisecond, func() error {
		if acc1.RoutedSubs() != 6 {
			return fmt.Errorf("Still no routed subscription: %d", acc1.RoutedSubs())
		}
		return nil
	})

	// Pub to other account with export on original subject.
	nc2.Publish("foo.stream", nil)

	if _, err = lsub.NextMsg(1 * time.Second); err != nil {
		t.Fatalf("Did not receive stream message: %s", err)
	}

	// Services
	// Create listener on nc2 (which connects to s2)
	gotIt := int32(0)
	nc2.Subscribe("req.echo", func(msg *nats.Msg) {
		atomic.AddInt32(&gotIt, 1)
		nc2.Publish(msg.Reply, []byte("WORKED"))
	})
	nc2.Flush()

	// Wait for it to make it across.
	time.Sleep(250 * time.Millisecond)

	// Now send the request on the leaf node client.
	if _, err := ncl.Request("import.request", []byte("fingers crossed"), 5500*time.Millisecond); err != nil {
		if atomic.LoadInt32(&gotIt) == 0 {
			t.Fatalf("Request was not received")
		}
		t.Fatalf("Did not receive response: %v", err)
	}
}

func TestLeafNodeInfoURLs(t *testing.T) {
	for _, test := range []struct {
		name         string
		useAdvertise bool
	}{
		{
			"without advertise",
			false,
		},
		{
			"with advertise",
			true,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			opts := testDefaultOptionsForLeafNodes()
			opts.Cluster.Name = "A"
			opts.Cluster.Port = -1
			opts.LeafNode.Host = "127.0.0.1"
			if test.useAdvertise {
				opts.LeafNode.Advertise = "me:1"
			}
			s1 := RunServer(opts)
			defer s1.Shutdown()

			lc := createLeafConn(t, opts.LeafNode.Host, opts.LeafNode.Port)
			defer lc.Close()
			info := checkInfoMsg(t, lc)
			if sz := len(info.LeafNodeURLs); sz != 1 {
				t.Fatalf("Expected LeafNodeURLs array to be size 1, got %v", sz)
			}
			var s1LNURL string
			if test.useAdvertise {
				s1LNURL = "me:1"
			} else {
				s1LNURL = net.JoinHostPort(opts.LeafNode.Host, strconv.Itoa(opts.LeafNode.Port))
			}
			if url := info.LeafNodeURLs[0]; url != s1LNURL {
				t.Fatalf("Expected URL to be %s, got %s", s1LNURL, url)
			}
			lc.Close()

			opts2 := testDefaultOptionsForLeafNodes()
			opts2.Cluster.Name = "A"
			opts2.Cluster.Port = -1
			opts2.Routes = server.RoutesFromStr(fmt.Sprintf("nats://%s:%d", opts.Cluster.Host, opts.Cluster.Port))
			opts2.LeafNode.Host = "127.0.0.1"
			if test.useAdvertise {
				opts2.LeafNode.Advertise = "me:2"
			}
			s2 := RunServer(opts2)
			defer s2.Shutdown()

			checkClusterFormed(t, s1, s2)

			lc = createLeafConn(t, opts.LeafNode.Host, opts.LeafNode.Port)
			defer lc.Close()
			info = checkInfoMsg(t, lc)
			if sz := len(info.LeafNodeURLs); sz != 2 {
				t.Fatalf("Expected LeafNodeURLs array to be size 2, got %v", sz)
			}
			var s2LNURL string
			if test.useAdvertise {
				s2LNURL = "me:2"
			} else {
				s2LNURL = net.JoinHostPort(opts2.LeafNode.Host, strconv.Itoa(opts2.LeafNode.Port))
			}
			var ok [2]int
			for _, url := range info.LeafNodeURLs {
				if url == s1LNURL {
					ok[0]++
				} else if url == s2LNURL {
					ok[1]++
				}
			}
			for i, res := range ok {
				if res != 1 {
					t.Fatalf("URL from server %v was found %v times", i+1, res)
				}
			}
			lc.Close()

			// Remove s2, and wait for route to be lost on s1.
			s2.Shutdown()
			checkNumRoutes(t, s1, 0)

			// Now check that s1 returns only itself in the URLs array.
			lc = createLeafConn(t, opts.LeafNode.Host, opts.LeafNode.Port)
			defer lc.Close()
			info = checkInfoMsg(t, lc)
			if sz := len(info.LeafNodeURLs); sz != 1 {
				t.Fatalf("Expected LeafNodeURLs array to be size 1, got %v", sz)
			}
			if url := info.LeafNodeURLs[0]; url != s1LNURL {
				t.Fatalf("Expected URL to be %s, got %s", s1LNURL, url)
			}
			lc.Close()

			s1.Shutdown()

			// Now we need a configuration where both s1 and s2 have a route
			// to each other, so we need explicit configuration. We are trying
			// to get S1->S2 and S2->S1 so one of the route is dropped. This
			// should not affect the number of URLs reported in INFO.

			opts.Cluster.Port = 5223
			opts.Routes = server.RoutesFromStr(fmt.Sprintf("nats://%s:5224", opts2.Host))
			s1, _ = server.NewServer(opts)
			defer s1.Shutdown()

			opts2.Cluster.Port = 5224
			opts2.Routes = server.RoutesFromStr(fmt.Sprintf("nats://%s:5223", opts.Host))
			s2, _ = server.NewServer(opts2)
			defer s2.Shutdown()

			// Start this way to increase chance of having the two connect
			// to each other at the same time. This will cause one of the
			// route to be dropped.
			wg := &sync.WaitGroup{}
			wg.Add(2)
			go func() {
				s1.Start()
				wg.Done()
			}()
			go func() {
				s2.Start()
				wg.Done()
			}()

			checkClusterFormed(t, s1, s2)

			lc = createLeafConn(t, opts.LeafNode.Host, opts.LeafNode.Port)
			defer lc.Close()
			info = checkInfoMsg(t, lc)
			if sz := len(info.LeafNodeURLs); sz != 2 {
				t.Fatalf("Expected LeafNodeURLs array to be size 2, got %v", sz)
			}
			ok[0], ok[1] = 0, 0
			for _, url := range info.LeafNodeURLs {
				if url == s1LNURL {
					ok[0]++
				} else if url == s2LNURL {
					ok[1]++
				}
			}
			for i, res := range ok {
				if res != 1 {
					t.Fatalf("URL from server %v was found %v times", i+1, res)
				}
			}
		})
	}
}

func TestLeafNodeFailover(t *testing.T) {
	server.SetGatewaysSolicitDelay(50 * time.Millisecond)
	defer server.ResetGatewaysSolicitDelay()

	ca := createClusterWithName(t, "A", 2)
	defer shutdownCluster(ca)

	cb := createClusterWithName(t, "B", 1, ca)
	defer shutdownCluster(cb)

	// Start a server that creates LeafNode connection to first
	// server in cluster A.
	s, opts := runSolicitLeafServer(ca.opts[0])
	defer s.Shutdown()

	// Shutdown that server on A.
	ca.servers[0].Shutdown()

	// Make sure that s reconnects its LN connection
	checkLeafNodeConnected(t, ca.servers[1])

	// Verify that LeafNode info protocol is sent to the server `s`
	// with list of new servers. To do that, we will restart
	// ca[0] but with a different LN listen port.
	ca.opts[0].Port = -1
	ca.opts[0].Cluster.Port = -1
	ca.opts[0].Routes = server.RoutesFromStr(fmt.Sprintf("nats://%s:%d", ca.opts[1].Cluster.Host, ca.opts[1].Cluster.Port))
	ca.opts[0].LeafNode.Port = -1
	newa0 := RunServer(ca.opts[0])
	defer newa0.Shutdown()

	checkClusterFormed(t, newa0, ca.servers[1])

	// Shutdown the server the LN is currently connected to. It should
	// reconnect to newa0.
	ca.servers[1].Shutdown()
	checkLeafNodeConnected(t, newa0)

	// Now shutdown newa0 and make sure `s` does not reconnect
	// to server in gateway.
	newa0.Shutdown()

	// Wait for more than the reconnect attempts.
	time.Sleep(opts.LeafNode.ReconnectInterval + 50*time.Millisecond)

	checkLeafNodeConnections(t, cb.servers[0], 0)
}

func TestLeafNodeAdvertise(t *testing.T) {
	// Create a dummy listener which will we use for the advertise address.
	// We will then stop the server and the test will be a success if
	// this listener accepts a connection.
	ch := make(chan struct{}, 1)
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("Error starting listener: %v", err)
	}
	defer l.Close()

	wg := sync.WaitGroup{}
	wg.Add(1)
	go func() {
		defer wg.Done()
		c, _ := l.Accept()
		if c != nil {
			c.Close()
		}
		l.Close()
		ch <- struct{}{}
	}()

	port := l.Addr().(*net.TCPAddr).Port

	o2 := testDefaultOptionsForLeafNodes()
	o2.LeafNode.Advertise = fmt.Sprintf("127.0.0.1:%d", port)
	o2.Cluster.Name = "A"
	o2.Cluster.Port = -1
	s2 := RunServer(o2)
	defer s2.Shutdown()

	o1 := testDefaultOptionsForLeafNodes()
	o1.Cluster.Name = "A"
	o1.Cluster.Port = -1
	o1.Routes = server.RoutesFromStr(fmt.Sprintf("nats://127.0.0.1:%d", o2.Cluster.Port))
	s1 := RunServer(o1)
	defer s1.Shutdown()

	checkClusterFormed(t, s1, s2)

	// Start a server that connects to s1. It should be made aware
	// of s2 (and its advertise address).
	s, _ := runSolicitLeafServer(o1)
	defer s.Shutdown()

	// Wait for leaf node connection to be established on s1.
	checkLeafNodeConnected(t, s1)

	// Shutdown s1. The listener that we created should be the one
	// receiving the connection from s.
	s1.Shutdown()

	select {
	case <-ch:
	case <-time.After(5 * time.Second):
		t.Fatalf("Server did not reconnect to advertised address")
	}
	wg.Wait()
}

func TestLeafNodeConnectionLimitsSingleServer(t *testing.T) {
	s, opts, _ := runLeafNodeOperatorServer(t)
	defer s.Shutdown()

	// Setup account and a user that will be used by the remote leaf node server.
	// createAccount automatically registers with resolver etc..
	acc, akp := createAccount(t, s)

	// Now update with limits for lead node connections.
	const maxleafs = 2

	apub, _ := akp.PublicKey()
	nac := jwt.NewAccountClaims(apub)
	nac.Limits.LeafNodeConn = maxleafs
	s.UpdateAccountClaims(acc, nac)

	// Make sure we have the limits updated in acc.
	if mleafs := acc.MaxActiveLeafNodes(); mleafs != maxleafs {
		t.Fatalf("Expected to have max leafnodes of %d, got %d", maxleafs, mleafs)
	}

	// Create the user credentials for the leadnode connection.
	kp, _ := nkeys.CreateUser()
	pub, _ := kp.PublicKey()
	nuc := jwt.NewUserClaims(pub)
	ujwt, err := nuc.Encode(akp)
	if err != nil {
		t.Fatalf("Error generating user JWT: %v", err)
	}
	seed, _ := kp.Seed()
	mycreds := genCredsFile(t, ujwt, seed)

	checkAccConnectionCounts := func(t *testing.T, expected int) {
		t.Helper()
		checkFor(t, 2*time.Second, 15*time.Millisecond, func() error {
			// Make sure we are accounting properly here.
			if nln := acc.NumLeafNodes(); nln != expected {
				return fmt.Errorf("expected %v leaf node, got %d", expected, nln)
			}
			// clients and leafnodes counted together.
			if nc := acc.NumConnections(); nc != expected {
				return fmt.Errorf("expected %v for total connections, got %d", expected, nc)
			}
			return nil
		})
	}

	checkAccNLF := func(n int) {
		t.Helper()
		checkFor(t, time.Second, 10*time.Millisecond, func() error {
			if nln := acc.NumLeafNodes(); nln != n {
				return fmt.Errorf("Expected %d leaf node, got %d", n, nln)
			}
			return nil
		})
	}

	sl, _, _ := runSolicitWithCredentials(t, opts, mycreds)
	defer sl.Shutdown()

	checkLeafNodeConnections(t, s, 1)

	// Make sure we are accounting properly here.
	checkAccNLF(1)
	// clients and leafnodes counted together.
	if nc := acc.NumConnections(); nc != 1 {
		t.Fatalf("Expected 1 for total connections, got %d", nc)
	}

	s2, _, _ := runSolicitWithCredentials(t, opts, mycreds)
	defer s2.Shutdown()
	checkLeafNodeConnections(t, s, 2)
	checkAccConnectionCounts(t, 2)

	s2.Shutdown()
	checkLeafNodeConnections(t, s, 1)
	checkAccConnectionCounts(t, 1)

	// Make sure we are accounting properly here.
	checkAccNLF(1)
	// clients and leafnodes counted together.
	if nc := acc.NumConnections(); nc != 1 {
		t.Fatalf("Expected 1 for total connections, got %d", nc)
	}

	// Now add back the second one as #3.
	s3, _, _ := runSolicitWithCredentials(t, opts, mycreds)
	defer s3.Shutdown()
	checkLeafNodeConnections(t, s, 2)

	checkAccConnectionCounts(t, 2)

	// Once we are here we should not be able to create anymore. Limit == 2.
	s4, _, _ := runSolicitWithCredentials(t, opts, mycreds)
	defer s4.Shutdown()

	checkAccConnectionCounts(t, 2)

	// Make sure s4 has 0 still. We need checkFor because it is possible
	// that when we check we have actually the connection registered for
	// a short period before it is closed due to limit.
	checkFor(t, time.Second, 15*time.Millisecond, func() error {
		if nln := s4.NumLeafNodes(); nln != 0 {
			return fmt.Errorf("Expected no leafnodes accounted for in s4, got %d", nln)
		}
		return nil
	})

	// Make sure this is still 2.
	checkLeafNodeConnections(t, s, 2)
}

func TestLeafNodeConnectionLimitsCluster(t *testing.T) {
	content := `
	port: -1
	operator = "./configs/nkeys/op.jwt"
    system_account = "AD2VB6C25DQPEUUQ7KJBUFX2J4ZNVBPOHSCBISC7VFZXVWXZA7VASQZG"
	resolver = MEMORY
	cluster {
		port: -1
		name: xyz
	}
	leafnodes {
		listen: "127.0.0.1:-1"
	}
    resolver_preload = {
        AD2VB6C25DQPEUUQ7KJBUFX2J4ZNVBPOHSCBISC7VFZXVWXZA7VASQZG : "eyJ0eXAiOiJqd3QiLCJhbGciOiJlZDI1NTE5In0.eyJqdGkiOiJDSzU1UERKSUlTWU5QWkhLSUpMVURVVTdJT1dINlM3UkE0RUc2TTVGVUQzUEdGQ1RWWlJRIiwiaWF0IjoxNTQzOTU4NjU4LCJpc3MiOiJPQ0FUMzNNVFZVMlZVT0lNR05HVU5YSjY2QUgyUkxTREFGM01VQkNZQVk1UU1JTDY1TlFNNlhRRyIsInN1YiI6IkFEMlZCNkMyNURRUEVVVVE3S0pCVUZYMko0Wk5WQlBPSFNDQklTQzdWRlpYVldYWkE3VkFTUVpHIiwidHlwZSI6ImFjY291bnQiLCJuYXRzIjp7ImxpbWl0cyI6e319fQ.7m1fysYUsBw15Lj88YmYoHxOI4HlOzu6qgP8Zg-1q9mQXUURijuDGVZrtb7gFYRlo-nG9xZyd2ZTRpMA-b0xCQ"
    }
	`
	conf := createConfFile(t, []byte(content))
	s1, s1Opts := RunServerWithConfig(conf)
	defer s1.Shutdown()

	content = fmt.Sprintf(`
	port: -1
	operator = "./configs/nkeys/op.jwt"
    system_account = "AD2VB6C25DQPEUUQ7KJBUFX2J4ZNVBPOHSCBISC7VFZXVWXZA7VASQZG"
	resolver = MEMORY
	cluster {
		port: -1
		name: xyz
		routes: ["nats://%s:%d"]
	}
	leafnodes {
		listen: "127.0.0.1:-1"
	}
    resolver_preload = {
        AD2VB6C25DQPEUUQ7KJBUFX2J4ZNVBPOHSCBISC7VFZXVWXZA7VASQZG : "eyJ0eXAiOiJqd3QiLCJhbGciOiJlZDI1NTE5In0.eyJqdGkiOiJDSzU1UERKSUlTWU5QWkhLSUpMVURVVTdJT1dINlM3UkE0RUc2TTVGVUQzUEdGQ1RWWlJRIiwiaWF0IjoxNTQzOTU4NjU4LCJpc3MiOiJPQ0FUMzNNVFZVMlZVT0lNR05HVU5YSjY2QUgyUkxTREFGM01VQkNZQVk1UU1JTDY1TlFNNlhRRyIsInN1YiI6IkFEMlZCNkMyNURRUEVVVVE3S0pCVUZYMko0Wk5WQlBPSFNDQklTQzdWRlpYVldYWkE3VkFTUVpHIiwidHlwZSI6ImFjY291bnQiLCJuYXRzIjp7ImxpbWl0cyI6e319fQ.7m1fysYUsBw15Lj88YmYoHxOI4HlOzu6qgP8Zg-1q9mQXUURijuDGVZrtb7gFYRlo-nG9xZyd2ZTRpMA-b0xCQ"
    }
	`, s1Opts.Cluster.Host, s1Opts.Cluster.Port)
	conf = createConfFile(t, []byte(content))
	s2, s2Opts := RunServerWithConfig(conf)
	defer s2.Shutdown()

	// Setup the two accounts for this server.
	okp, _ := nkeys.FromSeed(oSeed)

	// Setup account and a user that will be used by the remote leaf node server.
	// createAccount automatically registers with resolver etc..
	acc, akp := createAccount(t, s1)

	// Now update with limits for lead node connections.
	const maxleafs = 10

	apub, _ := akp.PublicKey()
	nac := jwt.NewAccountClaims(apub)
	nac.Limits.LeafNodeConn = maxleafs

	ajwt, err := nac.Encode(okp)
	if err != nil {
		t.Fatalf("Error generating account JWT: %v", err)
	}
	if err := s1.AccountResolver().Store(apub, ajwt); err != nil {
		t.Fatalf("Account Resolver returned an error: %v", err)
	}
	s1.UpdateAccountClaims(acc, nac)

	if err := s2.AccountResolver().Store(apub, ajwt); err != nil {
		t.Fatalf("Account Resolver returned an error: %v", err)
	}
	// Make sure that account object registered in S2 is not acc2
	acc2, err := s2.LookupAccount(acc.Name)
	if err != nil || acc == acc2 {
		t.Fatalf("Lookup account error: %v - accounts are same: %v", err, acc == acc2)
	}

	// Create the user credentials for the leadnode connection.
	kp, _ := nkeys.CreateUser()
	pub, _ := kp.PublicKey()
	nuc := jwt.NewUserClaims(pub)
	ujwt, err := nuc.Encode(akp)
	if err != nil {
		t.Fatalf("Error generating user JWT: %v", err)
	}
	seed, _ := kp.Seed()
	mycreds := genCredsFile(t, ujwt, seed)

	loop := maxleafs / 2

	// Now create maxleafs/2 leaf node servers on each operator server.
	for i := 0; i < loop; i++ {
		sl1, _, _ := runSolicitWithCredentials(t, s1Opts, mycreds)
		defer sl1.Shutdown()

		sl2, _, _ := runSolicitWithCredentials(t, s2Opts, mycreds)
		defer sl2.Shutdown()
	}

	checkLeafNodeConnections(t, s1, loop)
	checkLeafNodeConnections(t, s2, loop)

	// Now check that we have the remotes registered. This will prove we are sending
	// and processing the leaf node connect events properly etc.
	checkAccLFCount := func(acc *server.Account, remote bool, n int) {
		t.Helper()
		checkFor(t, time.Second, 10*time.Millisecond, func() error {
			str := ""
			var nln int
			if remote {
				nln = acc.NumRemoteLeafNodes()
				str = "remote"
			} else {
				nln = acc.NumLeafNodes()
			}
			if nln != n {
				return fmt.Errorf("number of expected %sleaf nodes is %v, got %v", str, n, nln)
			}
			return nil
		})
	}
	checkAccLFCount(acc, true, loop)
	checkAccLFCount(acc2, true, loop)

	// Now that we are here we should not be allowed anymore leaf nodes.
	l, _, _ := runSolicitWithCredentials(t, s1Opts, mycreds)
	defer l.Shutdown()

	checkAccLFCount(acc, false, maxleafs)
	// Should still be at loop size.
	checkLeafNodeConnections(t, s1, loop)

	l, _, _ = runSolicitWithCredentials(t, s2Opts, mycreds)
	defer l.Shutdown()
	checkAccLFCount(acc2, false, maxleafs)
	// Should still be at loop size.
	checkLeafNodeConnections(t, s2, loop)
}

func TestLeafNodeSwitchGatewayToInterestModeOnly(t *testing.T) {
	server.SetGatewaysSolicitDelay(50 * time.Millisecond)
	defer server.ResetGatewaysSolicitDelay()

	ca := createClusterWithName(t, "A", 3)
	defer shutdownCluster(ca)
	cb := createClusterWithName(t, "B", 3, ca)
	defer shutdownCluster(cb)

	// Create client on a server in cluster A
	opts := ca.opts[0]
	c := createClientConn(t, opts.Host, opts.Port)
	defer c.Close()

	send, expect := setupConn(t, c)
	send("PING\r\n")
	expect(pongRe)

	// Send a message from this client on "foo" so that B
	// registers a no-interest for account "$G"
	send("PUB foo 2\r\nok\r\nPING\r\n")
	expect(pongRe)

	// Create a leaf node connection on a server in cluster B
	opts = cb.opts[0]
	lc := createLeafConn(t, opts.LeafNode.Host, opts.LeafNode.Port)
	defer lc.Close()

	// This is for our global responses since we are setting up GWs above.
	leafSend, leafExpect := setupLeaf(t, lc, 7)
	leafSend("PING\r\n")
	leafExpect(pongRe)
}

// The MSG proto for routes and gateways is RMSG, and we have an
// optimization that a scratch buffer has RMSG and when doing a
// client we just start at scratch[1]. For leaf nodes its LMSG and we
// rewrite scratch[0], but never reset it which causes protocol
// errors when used with routes or gateways after use to send
// to a leafnode.
// We will create a server with a leafnode connection and a route
// and a gateway connection.

// route connections to simulate.
func TestLeafNodeResetsMSGProto(t *testing.T) {
	server.GatewayDoNotForceInterestOnlyMode(true)
	defer server.GatewayDoNotForceInterestOnlyMode(false)

	opts := testDefaultOptionsForLeafNodes()
	opts.Cluster.Name = "xyz"
	opts.Cluster.Host = opts.Host
	opts.Cluster.Port = -1
	opts.Gateway.Name = "xyz"
	opts.Gateway.Host = opts.Host
	opts.Gateway.Port = -1
	opts.Accounts = []*server.Account{server.NewAccount("$SYS")}
	opts.SystemAccount = "$SYS"

	s := RunServer(opts)
	defer s.Shutdown()

	lc := createLeafConn(t, opts.LeafNode.Host, opts.LeafNode.Port)
	defer lc.Close()

	leafSend, leafExpect := setupConn(t, lc)

	// To avoid possible INFO when switching to interest mode only,
	// delay start of gateway.
	time.Sleep(500 * time.Millisecond)

	gw := createGatewayConn(t, opts.Gateway.Host, opts.Gateway.Port)
	defer gw.Close()

	gwSend, gwExpect := setupGatewayConn(t, gw, "A", "xyz")
	gwSend("PING\r\n")
	gwExpect(pongRe)

	// This is for our global responses since we are setting up GWs above.
	leafExpect(lsubRe)

	// Now setup interest in the leaf node for 'foo'.
	leafSend("LS+ foo\r\nPING\r\n")
	leafExpect(pongRe)

	// Send msg from the gateway.
	gwSend("RMSG $G foo 2\r\nok\r\nPING\r\n")
	gwExpect(pongRe)

	leafExpect(lmsgRe)

	// At this point the gw inside our main server's scratch buffer is LMSG. When we do
	// same with a connected route with interest it should fail.
	rc := createRouteConn(t, opts.Cluster.Host, opts.Cluster.Port)
	defer rc.Close()
	checkInfoMsg(t, rc)
	routeSend, routeExpect := setupRouteEx(t, rc, opts, "RC")

	routeSend("RS+ $G foo\r\nPING\r\n")
	routeExpect(pongRe)

	// This is for the route interest we just created.
	leafExpect(lsubRe)

	// Send msg from the gateway.
	gwSend("RMSG $G foo 2\r\nok\r\nPING\r\n")
	gwExpect(pongRe)

	leafExpect(lmsgRe)

	// Now make sure we get it on route. This will fail with the proto bug.
	routeExpect(rmsgRe)
}

// We need to make sure that as a remote server we also send our local subs on connect.
func TestLeafNodeSendsRemoteSubsOnConnect(t *testing.T) {
	s, opts := runLeafServer()
	defer s.Shutdown()

	sl, slOpts := runSolicitLeafServer(opts)
	defer sl.Shutdown()

	checkLeafNodeConnected(t, s)
	s.Shutdown()

	c := createClientConn(t, slOpts.Host, slOpts.Port)
	defer c.Close()

	send, expect := setupConn(t, c)
	send("SUB foo 1\r\n")
	send("PING\r\n")
	expect(pongRe)

	// Need to restart it on the same port.
	s, _ = runLeafServerOnPort(opts.LeafNode.Port)
	defer s.Shutdown()
	checkLeafNodeConnected(t, s)

	lc := createLeafConn(t, opts.LeafNode.Host, opts.LeafNode.Port)
	defer lc.Close()

	setupLeaf(t, lc, 3)
}

func TestLeafNodeServiceImportLikeNGS(t *testing.T) {
	gwSolicit := 10 * time.Millisecond
	ca := createClusterEx(t, true, gwSolicit, true, "A", 3)
	defer shutdownCluster(ca)
	cb := createClusterEx(t, true, gwSolicit, true, "B", 3, ca)
	defer shutdownCluster(cb)

	// Hang a responder off of cluster A.
	opts := ca.opts[0]
	url := fmt.Sprintf("nats://ngs:pass@%s:%d", opts.Host, opts.Port)
	nc, err := nats.Connect(url)
	if err != nil {
		t.Fatalf("Error on connect: %v", err)
	}
	defer nc.Close()

	// Create a queue subscriber to send results
	nc.QueueSubscribe("ngs.usage.*", "ngs", func(m *nats.Msg) {
		m.Respond([]byte("22"))
	})
	nc.Flush()

	// Now create a leafnode server on B.
	opts = cb.opts[1]
	sl, slOpts := runSolicitLeafServerToURL(fmt.Sprintf("nats-leaf://dlc:pass@%s:%d", opts.LeafNode.Host, opts.LeafNode.Port))
	defer sl.Shutdown()

	checkLeafNodeConnected(t, sl)

	// Create a normal direct connect client on B.
	url = fmt.Sprintf("nats://dlc:pass@%s:%d", opts.Host, opts.Port)
	nc2, err := nats.Connect(url)
	if err != nil {
		t.Fatalf("Error on connect: %v", err)
	}
	defer nc2.Close()

	if _, err := nc2.Request("ngs.usage", []byte("fingers crossed"), 500*time.Millisecond); err != nil {
		t.Fatalf("Did not receive response: %v", err)
	}

	// Now create a client on the leafnode.
	url = fmt.Sprintf("nats://%s:%d", slOpts.Host, slOpts.Port)
	ncl, err := nats.Connect(url)
	if err != nil {
		t.Fatalf("Error on connect: %v", err)
	}
	defer ncl.Close()

	if _, err := ncl.Request("ngs.usage", []byte("fingers crossed"), 500*time.Millisecond); err != nil {
		t.Fatalf("Did not receive response: %v", err)
	}
}

func TestLeafNodeServiceImportResponderOnLeaf(t *testing.T) {
	gwSolicit := 10 * time.Millisecond
	ca := createClusterEx(t, true, gwSolicit, true, "A", 3)
	defer shutdownCluster(ca)

	// Now create a leafnode server on A that will bind to the NGS account.
	opts := ca.opts[1]
	sl, slOpts := runSolicitLeafServerToURL(fmt.Sprintf("nats-leaf://ngs:pass@%s:%d", opts.LeafNode.Host, opts.LeafNode.Port))
	defer sl.Shutdown()

	checkLeafNodeConnected(t, sl)

	// Now create a client on the leafnode.
	ncl, err := nats.Connect(fmt.Sprintf("nats://%s:%d", slOpts.Host, slOpts.Port))
	if err != nil {
		t.Fatalf("Error on connect: %v", err)
	}
	defer ncl.Close()

	// Create a queue subscriber to send results
	ncl.QueueSubscribe("ngs.usage.*", "ngs", func(m *nats.Msg) {
		m.Respond([]byte("22"))
	})
	ncl.Flush()

	// Create a normal direct connect client on A. Needs to be same server as leafnode.
	opts = ca.opts[1]
	nc, err := nats.Connect(fmt.Sprintf("nats://dlc:pass@%s:%d", opts.Host, opts.Port))
	if err != nil {
		t.Fatalf("Error on connect: %v", err)
	}
	defer nc.Close()

	if _, err := nc.Request("ngs.usage", []byte("fingers crossed"), 500*time.Millisecond); err != nil {
		t.Fatalf("Did not receive response: %v", err)
	}
}

func TestLeafNodeSendsAccountingEvents(t *testing.T) {
	s, opts, _ := runLeafNodeOperatorServer(t)
	defer s.Shutdown()

	// System account
	acc, akp := createAccount(t, s)
	if err := s.SetSystemAccount(acc.Name); err != nil {
		t.Fatalf("Expected this succeed, got %v", err)
	}

	// Leafnode Account
	lacc, lakp := createAccount(t, s)

	// Create a system account user and connect a client to listen for the events.
	url := fmt.Sprintf("nats://%s:%d", opts.Host, opts.Port)
	nc, err := nats.Connect(url, createUserCreds(t, s, akp))
	if err != nil {
		t.Fatalf("Error on connect: %v", err)
	}
	defer nc.Close()

	// Watch only for our leaf node account.
	cSub, _ := nc.SubscribeSync(fmt.Sprintf("$SYS.ACCOUNT.%s.CONNECT", lacc.Name))
	dSub, _ := nc.SubscribeSync(fmt.Sprintf("$SYS.ACCOUNT.%s.DISCONNECT", lacc.Name))
	nc.Flush()

	// Now create creds for the leafnode and connect the server.
	kp, _ := nkeys.CreateUser()
	pub, _ := kp.PublicKey()
	nuc := jwt.NewUserClaims(pub)
	ujwt, err := nuc.Encode(lakp)
	if err != nil {
		t.Fatalf("Error generating user JWT: %v", err)
	}
	seed, _ := kp.Seed()
	mycreds := genCredsFile(t, ujwt, seed)

	sl, _, _ := runSolicitWithCredentials(t, opts, mycreds)
	defer sl.Shutdown()

	// Wait for connect event
	msg, err := cSub.NextMsg(time.Second)
	if err != nil {
		t.Fatalf("Error waiting for account connect event: %v", err)
	}
	m := server.ConnectEventMsg{}
	if err := json.Unmarshal(msg.Data, &m); err != nil {
		t.Fatal("Did not get correctly formatted event")
	}

	// Shutdown leafnode to generate disconnect event.
	sl.Shutdown()

	msg, err = dSub.NextMsg(time.Second)
	if err != nil {
		t.Fatalf("Error waiting for account disconnect event: %v", err)
	}
	dm := server.DisconnectEventMsg{}
	if err := json.Unmarshal(msg.Data, &dm); err != nil {
		t.Fatal("Did not get correctly formatted event")
	}
}

func TestLeafNodeDistributedQueueAcrossGWs(t *testing.T) {
	gwSolicit := 10 * time.Millisecond
	ca := createClusterEx(t, true, gwSolicit, true, "A", 3)
	defer shutdownCluster(ca)
	cb := createClusterEx(t, true, gwSolicit, true, "B", 3, ca)
	defer shutdownCluster(cb)
	cc := createClusterEx(t, true, gwSolicit, true, "C", 3, ca, cb)
	defer shutdownCluster(cc)

	// Create queue subscribers
	createQS := func(c *cluster) *nats.Conn {
		t.Helper()
		opts := c.opts[rand.IntN(len(c.opts))]
		url := fmt.Sprintf("nats://ngs:pass@%s:%d", opts.Host, opts.Port)
		nc, err := nats.Connect(url)
		if err != nil {
			t.Fatalf("Error on connect: %v", err)
		}
		nc.QueueSubscribe("ngs.usage.*", "dq", func(m *nats.Msg) {
			m.Respond([]byte(c.name))
		})
		nc.Flush()
		return nc
	}

	ncA := createQS(ca)
	defer ncA.Close()
	ncB := createQS(cb)
	defer ncB.Close()
	ncC := createQS(cc)
	defer ncC.Close()

	connectAndRequest := func(url, clusterName string, nreqs int) {
		t.Helper()
		nc, err := nats.Connect(url)
		if err != nil {
			t.Fatalf("Error on connect: %v", err)
		}
		defer nc.Close()
		for i := 0; i < nreqs; i++ {
			m, err := nc.Request("ngs.usage", nil, 500*time.Millisecond)
			if err != nil {
				t.Fatalf("Did not receive a response: %v", err)
			}
			if string(m.Data) != clusterName {
				t.Fatalf("Expected to prefer %q, but got response from %q", clusterName, m.Data)
			}
		}
	}

	checkClientDQ := func(c *cluster, nreqs int) {
		t.Helper()
		// Pick one at random.
		opts := c.opts[rand.IntN(len(c.opts))]
		url := fmt.Sprintf("nats://dlc:pass@%s:%d", opts.Host, opts.Port)
		connectAndRequest(url, c.name, nreqs)
	}

	// First check that this works with direct connected clients.
	checkClientDQ(ca, 100)
	checkClientDQ(cb, 100)
	checkClientDQ(cc, 100)

	createLNS := func(c *cluster) (*server.Server, *server.Options) {
		t.Helper()
		// Pick one at random.
		copts := c.opts[rand.IntN(len(c.servers))]
		s, opts := runSolicitLeafServerToURL(fmt.Sprintf("nats-leaf://dlc:pass@%s:%d", copts.LeafNode.Host, copts.LeafNode.Port))
		checkLeafNodeConnected(t, s)
		return s, opts
	}

	checkLeafDQ := func(opts *server.Options, clusterName string, nreqs int) {
		t.Helper()
		url := fmt.Sprintf("nats://%s:%d", opts.Host, opts.Port)
		connectAndRequest(url, clusterName, nreqs)
	}

	// Test leafnodes to all clusters.
	for _, c := range []*cluster{ca, cb, cc} {
		// Now create a leafnode on cluster.
		sl, slOpts := createLNS(c)
		defer sl.Shutdown()
		// Now connect to the leafnode server and run test.
		checkLeafDQ(slOpts, c.name, 100)
	}
}

func TestLeafNodeDistributedQueueEvenly(t *testing.T) {
	gwSolicit := 10 * time.Millisecond
	ca := createClusterEx(t, true, gwSolicit, true, "A", 3)
	defer shutdownCluster(ca)
	cb := createClusterEx(t, true, gwSolicit, true, "B", 3, ca)
	defer shutdownCluster(cb)

	// Create queue subscribers
	createQS := func(c *cluster) *nats.Conn {
		t.Helper()
		opts := c.opts[rand.IntN(len(c.opts))]
		url := fmt.Sprintf("nats://ngs:pass@%s:%d", opts.Host, opts.Port)
		nc, err := nats.Connect(url)
		if err != nil {
			t.Fatalf("Error on connect: %v", err)
		}
		cid, _ := nc.GetClientID()
		response := []byte(fmt.Sprintf("%s:%d:%d", c.name, opts.Port, cid))
		nc.QueueSubscribe("ngs.usage.*", "dq", func(m *nats.Msg) {
			m.Respond(response)
		})
		nc.Flush()
		return nc
	}

	ncA1 := createQS(ca)
	defer ncA1.Close()

	ncA2 := createQS(ca)
	defer ncA2.Close()

	ncA3 := createQS(ca)
	defer ncA3.Close()

	resp := make(map[string]int)

	connectAndRequest := func(url, clusterName string, nreqs int) {
		t.Helper()
		nc, err := nats.Connect(url)
		if err != nil {
			t.Fatalf("Error on connect: %v", err)
		}
		defer nc.Close()
		for i := 0; i < nreqs; i++ {
			m, err := nc.Request("ngs.usage", nil, 500*time.Millisecond)
			if err != nil {
				t.Fatalf("Did not receive a response: %v", err)
			}
			if string(m.Data[0]) != clusterName {
				t.Fatalf("Expected to prefer %q, but got response from %q", clusterName, m.Data[0])
			}
			resp[string(m.Data)]++
		}
	}

	createLNS := func(c *cluster) (*server.Server, *server.Options) {
		t.Helper()
		// Pick one at random.
		copts := c.opts[rand.IntN(len(c.servers))]
		s, opts := runSolicitLeafServerToURL(fmt.Sprintf("nats-leaf://dlc:pass@%s:%d", copts.LeafNode.Host, copts.LeafNode.Port))
		checkLeafNodeConnected(t, s)
		return s, opts
	}

	checkLeafDQ := func(opts *server.Options, clusterName string, nreqs int) {
		t.Helper()
		url := fmt.Sprintf("nats://%s:%d", opts.Host, opts.Port)
		connectAndRequest(url, clusterName, nreqs)
	}

	// Now create a leafnode on cluster A.
	sl, slOpts := createLNS(ca)
	defer sl.Shutdown()

	// Now connect to the leafnode server and run test.
	checkLeafDQ(slOpts, ca.name, 100)

	// Should have some for all 3 QS [ncA1, ncA2, ncA3]
	if lr := len(resp); lr != 3 {
		t.Fatalf("Expected all 3 queue subscribers to have received some messages, only got %d", lr)
	}
	// Now check that we have at least 10% for each subscriber.
	for _, r := range resp {
		if r < 10 {
			t.Fatalf("Got a subscriber with less than 10 responses: %d", r)
		}
	}
}

func TestLeafNodeDefaultPort(t *testing.T) {
	o := testDefaultOptionsForLeafNodes()
	o.LeafNode.Port = server.DEFAULT_LEAFNODE_PORT
	s := RunServer(o)
	defer s.Shutdown()

	conf := createConfFile(t, []byte(`
		port: -1
		leaf {
			remotes = [
				{
					url: "leafnode://127.0.0.1"
				}
			]
		}
	`))

	sl, _ := RunServerWithConfig(conf)
	defer sl.Shutdown()

	checkLeafNodeConnected(t, s)
}

// Since leafnode's are true interest only we need to make sure that we
// register the proper interest with global routing $GR.xxxxxx._INBOX.>
func TestLeafNodeAndGatewayGlobalRouting(t *testing.T) {
	server.SetGatewaysSolicitDelay(50 * time.Millisecond)
	defer server.ResetGatewaysSolicitDelay()

	ca := createClusterWithName(t, "A", 3)
	defer shutdownCluster(ca)
	cb := createClusterWithName(t, "B", 3, ca)
	defer shutdownCluster(cb)

	sl, slOpts := runSolicitLeafServer(ca.opts[1])
	defer sl.Shutdown()

	checkLeafNodeConnected(t, ca.servers[1])

	// Create a client on the leafnode. This will listen for requests.
	ncl, err := nats.Connect(fmt.Sprintf("nats://%s:%d", slOpts.Host, slOpts.Port))
	if err != nil {
		t.Fatalf("Error on connect: %v", err)
	}
	defer ncl.Close()

	ncl.Subscribe("foo", func(m *nats.Msg) {
		m.Respond([]byte("World"))
	})
	ncl.Flush()

	// Since for leafnodes the account becomes interest-only mode,
	// let's make sure that the interest on "foo" has time to propagate
	// to cluster B.
	time.Sleep(250 * time.Millisecond)

	// Create a direct connect requestor. Try with all possible
	// servers in cluster B to make sure that we also receive the
	// reply when the accepting leafnode server does not have
	// its outbound GW connection to the requestor's server.
	for i := 0; i < 3; i++ {
		opts := cb.opts[i]
		url := fmt.Sprintf("nats://%s:%d", opts.Host, opts.Port)
		nc, err := nats.Connect(url)
		if err != nil {
			t.Fatalf("Error on connect: %v", err)
		}
		defer nc.Close()

		// We don't use an INBOX here because we had a bug where
		// leafnode would subscribe to _GR_.*.*.*.> instead of
		// _GR_.>, and inbox masked that because of their number
		// of tokens.
		reply := nuid.Next()
		sub, err := nc.SubscribeSync(reply)
		if err != nil {
			t.Fatalf("Error on subscribe: %v", err)
		}
		if err := nc.PublishRequest("foo", reply, []byte("Hello")); err != nil {
			t.Fatalf("Failed to send request: %v", err)
		}
		if _, err := sub.NextMsg(250 * time.Millisecond); err != nil {
			t.Fatalf("Did not get reply from server %d: %v", i, err)
		}
		nc.Close()
	}
}

func checkLeafNode2Connected(t *testing.T, s *server.Server) {
	t.Helper()
	checkFor(t, 5*time.Second, 100*time.Millisecond, func() error {
		if nln := s.NumLeafNodes(); nln != 2 {
			return fmt.Errorf("Expected a connected leafnode for server %q, got none", s.ID())
		}
		return nil
	})
}

func TestLeafNodesStaggeredSubPub(t *testing.T) {
	s, opts := runLeafServer()
	defer s.Shutdown()

	sl1, sl1Opts := runSolicitLeafServer(opts)
	defer sl1.Shutdown()

	checkLeafNodeConnected(t, s)

	// Create a client on the leafnode and a subscription.
	ncl, err := nats.Connect(fmt.Sprintf("nats://%s:%d", sl1Opts.Host, sl1Opts.Port))
	if err != nil {
		t.Fatalf("Error on connect: %v", err)
	}
	defer ncl.Close()

	sub, err := ncl.SubscribeSync("foo")
	if err != nil {
		t.Fatalf("Failed to subscribe: %v", err)
	}

	sl2, sl2Opts := runSolicitLeafServer(opts)
	defer sl2.Shutdown()

	checkLeafNode2Connected(t, s)

	// Create a client on the second leafnode and publish a message.
	ncl2, err := nats.Connect(fmt.Sprintf("nats://%s:%d", sl2Opts.Host, sl2Opts.Port))
	if err != nil {
		t.Fatalf("Error on connect: %v", err)
	}
	defer ncl2.Close()

	ncl2.Publish("foo", nil)

	checkFor(t, 250*time.Millisecond, 10*time.Millisecond, func() error {
		if nmsgs, _, err := sub.Pending(); err != nil || nmsgs != 1 {
			return fmt.Errorf("Did not receive the message: %v", err)
		}
		return nil
	})
}

func TestLeafNodeMultipleRemoteURLs(t *testing.T) {
	s, opts := runLeafServer()
	defer s.Shutdown()

	content := `
		port: -1
		leafnodes {
			remotes = [
				{
					urls: [nats-leaf://127.0.0.1:%d,nats-leaf://localhost:%d]
				}
			]
		}
		`

	config := fmt.Sprintf(content, opts.LeafNode.Port, opts.LeafNode.Port)
	conf := createConfFile(t, []byte(config))
	sl, _ := RunServerWithConfig(conf)
	defer sl.Shutdown()

	checkLeafNodeConnected(t, s)
}

func runSolicitLeafCluster(t *testing.T, clusterName string, d1, d2 *cluster) *cluster {
	c := &cluster{servers: make([]*server.Server, 0, 2), opts: make([]*server.Options, 0, 2), name: clusterName}

	// Who we will solicit for server 1
	ci := rand.IntN(len(d1.opts))
	opts := d1.opts[ci]
	surl := fmt.Sprintf("nats-leaf://%s:%d", opts.LeafNode.Host, opts.LeafNode.Port)

	o := DefaultTestOptions
	o.Port = -1
	rurl, _ := url.Parse(surl)
	o.LeafNode.Remotes = []*server.RemoteLeafOpts{{URLs: []*url.URL{rurl}}}
	o.LeafNode.ReconnectInterval = 100 * time.Millisecond
	o.Cluster.Name = clusterName
	o.Cluster.Host = o.Host
	o.Cluster.Port = -1
	s := RunServer(&o)
	checkLeafNodeConnected(t, d1.servers[ci])

	c.servers = append(c.servers, s)
	c.opts = append(c.opts, &o)

	// Grab route info
	routeAddr := fmt.Sprintf("nats-route://%s:%d", o.Cluster.Host, o.Cluster.Port)
	curl, _ := url.Parse(routeAddr)

	// Who we will solicit for server 2
	ci = rand.IntN(len(d2.opts))
	opts = d2.opts[ci]
	surl = fmt.Sprintf("nats-leaf://%s:%d", opts.LeafNode.Host, opts.LeafNode.Port)

	// This is for the case were d1 == d2 and we select the same server.
	plfn := d2.servers[ci].NumLeafNodes()

	o2 := DefaultTestOptions
	o2.Port = -1
	rurl, _ = url.Parse(surl)
	o2.LeafNode.Remotes = []*server.RemoteLeafOpts{{URLs: []*url.URL{rurl}}}
	o2.LeafNode.ReconnectInterval = 100 * time.Millisecond
	o2.Cluster.Name = clusterName
	o2.Cluster.Host = o.Host
	o2.Cluster.Port = -1
	o2.Routes = []*url.URL{curl}
	s = RunServer(&o2)

	if plfn == 0 {
		checkLeafNodeConnected(t, d2.servers[ci])
	} else {
		checkLeafNode2Connected(t, d2.servers[ci])
	}

	c.servers = append(c.servers, s)
	c.opts = append(c.opts, &o2)

	checkClusterFormed(t, c.servers...)

	return c
}

func clientForCluster(t *testing.T, c *cluster) *nats.Conn {
	t.Helper()
	opts := c.opts[rand.IntN(len(c.opts))]
	url := fmt.Sprintf("nats://%s:%d", opts.Host, opts.Port)
	nc, err := nats.Connect(url)
	if err != nil {
		t.Fatalf("Error on connect: %v", err)
	}
	return nc
}

func TestLeafNodeCycleWithSolicited(t *testing.T) {
	server.SetGatewaysSolicitDelay(10 * time.Millisecond)
	defer server.ResetGatewaysSolicitDelay()

	// Accepting leafnode cluster, e.g. NGS
	ca := createClusterWithName(t, "A", 3)
	defer shutdownCluster(ca)
	cb := createClusterWithName(t, "B", 3, ca)
	defer shutdownCluster(cb)

	// Create the responders.
	requestsReceived := int32(0)

	nc := clientForCluster(t, ca)
	defer nc.Close()
	nc.QueueSubscribe("request", "cycles", func(m *nats.Msg) {
		atomic.AddInt32(&requestsReceived, 1)
		m.Respond([]byte("22"))
	})

	nc = clientForCluster(t, cb)
	defer nc.Close()
	nc.QueueSubscribe("request", "cycles", func(m *nats.Msg) {
		atomic.AddInt32(&requestsReceived, 1)
		m.Respond([]byte("33"))
	})

	// Soliciting cluster, both solicited connected to the "A" cluster
	sc := runSolicitLeafCluster(t, "SC", ca, ca)
	defer shutdownCluster(sc)

	checkInterest := func(s *server.Server, subject string) bool {
		t.Helper()
		acc, _ := s.LookupAccount("$G")
		return acc.SubscriptionInterest(subject)
	}

	waitForInterest := func(subject string, servers ...*server.Server) {
		t.Helper()
		checkFor(t, time.Second, 10*time.Millisecond, func() error {
			for _, s := range servers {
				if !checkInterest(s, subject) {
					return fmt.Errorf("No interest")
				}
			}
			return nil
		})
	}

	waitForInterest("request",
		sc.servers[0], sc.servers[1],
		ca.servers[0], ca.servers[1], ca.servers[2],
		cb.servers[0], cb.servers[1], cb.servers[2],
	)

	// Connect a client to a random server in sc
	createClientAndRequest := func(c *cluster) (*nats.Conn, *nats.Subscription) {
		nc := clientForCluster(t, c)
		reply := nats.NewInbox()
		sub, err := nc.SubscribeSync(reply)
		if err != nil {
			t.Fatalf("Could not subscribe: %v", err)
		}
		if err := nc.PublishRequest("request", reply, []byte("fingers crossed")); err != nil {
			t.Fatalf("Error sending request: %v", err)
		}
		return nc, sub
	}

	verifyOneResponse := func(sub *nats.Subscription) {
		time.Sleep(250 * time.Millisecond)
		m, _, err := sub.Pending()
		if err != nil {
			t.Fatalf("Error calling Pending(): %v", err)
		}
		if m > 1 {
			t.Fatalf("Received more then one response, cycle indicated: %d", m)
		}
	}

	verifyRequestTotal := func(nre int32) {
		if nr := atomic.LoadInt32(&requestsReceived); nr != nre {
			t.Fatalf("Expected %d requests received, got %d", nre, nr)
		}
	}

	// This should pass to here, but if we have a cycle things will be spinning and we will receive
	// too many responses when it should only be 1.
	nc, rsub := createClientAndRequest(sc)
	defer nc.Close()
	verifyOneResponse(rsub)
	verifyRequestTotal(1)

	// Do a solicit across GW, so shut this one down.
	nc.Close()
	shutdownCluster(sc)

	// Soliciting cluster, connect to different clusters across a GW.
	sc = runSolicitLeafCluster(t, "SC", ca, cb)
	defer shutdownCluster(sc)

	nc, rsub = createClientAndRequest(sc)
	defer nc.Close()
	verifyOneResponse(rsub)
	verifyRequestTotal(2) // This is total since use same responders.
}

func TestLeafNodeNoRaceGeneratingNonce(t *testing.T) {
	opts := testDefaultOptionsForLeafNodes()
	opts.Cluster.Port = -1
	opts.Cluster.Name = "xyz"
	s := RunServer(opts)
	defer s.Shutdown()

	quitCh := make(chan struct{})
	wg := sync.WaitGroup{}
	wg.Add(2)

	go func() {
		defer wg.Done()
		for {
			lc := createLeafConn(t, opts.LeafNode.Host, opts.LeafNode.Port)
			checkInfoMsg(t, lc)
			lc.Close()
			select {
			case <-quitCh:
				return
			case <-time.After(time.Millisecond):
			}
		}
	}()

	go func() {
		defer wg.Done()
		for {
			rc := createRouteConn(t, opts.Cluster.Host, opts.Cluster.Port)
			checkInfoMsg(t, rc)
			rc.Close()
			select {
			case <-quitCh:
				return
			case <-time.After(time.Millisecond):
			}
		}
	}()

	// Let this run for a bit to see if we get data race
	time.Sleep(100 * time.Millisecond)
	close(quitCh)
	wg.Wait()
}

func runSolicitAndAcceptLeafServer(lso *server.Options) (*server.Server, *server.Options) {
	surl := fmt.Sprintf("nats-leaf://%s:%d", lso.LeafNode.Host, lso.LeafNode.Port)
	o := testDefaultOptionsForLeafNodes()
	o.Port = -1
	rurl, _ := url.Parse(surl)
	o.LeafNode.Remotes = []*server.RemoteLeafOpts{{URLs: []*url.URL{rurl}}}
	o.LeafNode.ReconnectInterval = 100 * time.Millisecond
	return RunServer(o), o
}

func TestLeafNodeDaisyChain(t *testing.T) {
	// To quickly enable trace and debug logging
	// doLog, doTrace, doDebug = true, true, true
	s1, opts1 := runLeafServer()
	defer s1.Shutdown()

	s2, opts2 := runSolicitAndAcceptLeafServer(opts1)
	defer s2.Shutdown()
	checkLeafNodeConnected(t, s1)

	s3, _ := runSolicitLeafServer(opts2)
	defer s3.Shutdown()
	checkLeafNodeConnections(t, s2, 2)

	// Make so we can tell the two apart since in same PID.
	if doLog {
		s1.SetLogger(logger.NewTestLogger("[S-1] - ", false), true, true)
		s2.SetLogger(logger.NewTestLogger("[S-2] - ", false), true, true)
		s3.SetLogger(logger.NewTestLogger("[S-3] - ", false), true, true)
	}

	nc1, err := nats.Connect(s1.ClientURL())
	if err != nil {
		t.Fatalf("Error creating client: %v", err)
	}
	defer nc1.Close()

	nc1.Subscribe("ngs.usage", func(msg *nats.Msg) {
		msg.Respond([]byte("22 msgs"))
	})
	nc1.Flush()

	checkSubInterest(t, s3, "$G", "ngs.usage", time.Second)

	nc2, err := nats.Connect(s3.ClientURL())
	if err != nil {
		t.Fatalf("Error creating client: %v", err)
	}
	defer nc2.Close()

	if _, err = nc2.Request("ngs.usage", []byte("1h"), time.Second); err != nil {
		t.Fatalf("Expected a response")
	}
}

// This will test failover to a server with a cert with only an IP after successfully connecting
// to a server with a cert with both.
func TestClusterTLSMixedIPAndDNS(t *testing.T) {
	confA := createConfFile(t, []byte(`
		listen: 127.0.0.1:-1
		leafnodes {
			listen: "127.0.0.1:-1"
			tls {
				cert_file: "./configs/certs/server-iponly.pem"
				key_file:  "./configs/certs/server-key-iponly.pem"
				ca_file:   "./configs/certs/ca.pem"
				timeout: 2
			}
		}
		cluster {
			listen: "127.0.0.1:-1"
			name: xyz
		}
	`))
	srvA, optsA := RunServerWithConfig(confA)
	defer srvA.Shutdown()

	bConfigTemplate := `
		listen: 127.0.0.1:-1
		leafnodes {
			listen: "127.0.0.1:-1"
			tls {
				cert_file: "./configs/certs/server-cert.pem"
				key_file:  "./configs/certs/server-key.pem"
				ca_file:   "./configs/certs/ca.pem"
				timeout: 2
			}
		}
		cluster {
			listen: "127.0.0.1:-1"
			name: xyz
			routes [
				"nats://%s:%d"
			]
		}
	`
	confB := createConfFile(t, []byte(fmt.Sprintf(bConfigTemplate,
		optsA.Cluster.Host, optsA.Cluster.Port)))
	srvB, optsB := RunServerWithConfig(confB)
	defer srvB.Shutdown()

	checkClusterFormed(t, srvA, srvB)

	// Solicit a leafnode server here. Don't use the helper since we need verification etc.
	o := DefaultTestOptions
	o.Port = -1
	rurl, _ := url.Parse(fmt.Sprintf("nats-leaf://%s:%d", optsB.LeafNode.Host, optsB.LeafNode.Port))
	o.LeafNode.ReconnectInterval = 10 * time.Millisecond
	remote := &server.RemoteLeafOpts{URLs: []*url.URL{rurl}}
	remote.TLSConfig = &tls.Config{MinVersion: tls.VersionTLS12}
	pool := x509.NewCertPool()
	rootPEM, err := os.ReadFile("./configs/certs/ca.pem")
	if err != nil || rootPEM == nil {
		t.Fatalf("Error loading or parsing rootCA file: %v", err)
	}
	ok := pool.AppendCertsFromPEM(rootPEM)
	if !ok {
		t.Fatalf("Failed to parse root certificate from %q", "./configs/certs/ca.pem")
	}
	remote.TLSConfig.RootCAs = pool
	o.LeafNode.Remotes = []*server.RemoteLeafOpts{remote}
	sl, _ := RunServer(&o), &o
	defer sl.Shutdown()

	checkLeafNodeConnected(t, srvB)

	// Now kill off srvB and force client to connect to srvA.
	srvB.Shutdown()

	// Make sure this works.
	checkLeafNodeConnected(t, srvA)
}

// This will test for a bug in stream export/import with leafnodes.
// https://github.com/nats-io/nats-server/issues/1332
func TestStreamExportWithMultipleAccounts(t *testing.T) {
	confA := createConfFile(t, []byte(`
		listen: 127.0.0.1:-1
		leafnodes {
			listen: "127.0.0.1:-1"
		}
	`))
	srvA, optsA := RunServerWithConfig(confA)
	defer srvA.Shutdown()

	bConfigTemplate := `
		listen: 127.0.0.1:-1
		leafnodes {
			listen: "127.0.0.1:-1"
			remotes = [
			{
				url:"nats://127.0.0.1:%d"
				account:"EXTERNAL"
			}
			]
		}
		accounts: {
		    INTERNAL: {
		        users: [
		            {user: good, password: pwd}
		        ]
			    imports: [
		            {
		                stream: { account: EXTERNAL, subject: "foo"}, prefix: "bar"
		            }
		        ]
		    },
		    EXTERNAL: {
		        users: [
		            {user: bad, password: pwd}
		        ]
		        exports: [{stream: "foo"}]
		    },
		}
	`

	confB := createConfFile(t, []byte(fmt.Sprintf(bConfigTemplate, optsA.LeafNode.Port)))
	srvB, optsB := RunServerWithConfig(confB)
	defer srvB.Shutdown()

	nc, err := nats.Connect(fmt.Sprintf("nats://good:pwd@%s:%d", optsB.Host, optsB.Port))
	if err != nil {
		t.Fatalf("Error on connect: %v", err)
	}
	defer nc.Close()

	wcsub, err := nc.SubscribeSync(">")
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	defer wcsub.Unsubscribe()
	nc.Flush()

	nc2, err := nats.Connect(fmt.Sprintf("nats://%s:%d", optsA.Host, optsA.Port))
	if err != nil {
		t.Fatalf("Error on connect: %v", err)
	}
	defer nc2.Close()

	nc2.Publish("foo", nil)

	msg, err := wcsub.NextMsg(1 * time.Second)
	if err != nil {
		t.Fatalf("Did not receive the message: %v", err)
	}
	if msg.Subject != "bar.foo" {
		t.Fatalf("Received on wrong subject: %q", msg.Subject)
	}
}

// This will test for a bug in service export/import with leafnodes.
// https://github.com/nats-io/nats-server/issues/1336
func TestServiceExportWithMultipleAccounts(t *testing.T) {
	confA := createConfFile(t, []byte(`
		server_name: A
		listen: 127.0.0.1:-1
		leafnodes {
			listen: "127.0.0.1:-1"
		}
	`))

	srvA, optsA := RunServerWithConfig(confA)
	defer srvA.Shutdown()

	bConfigTemplate := `
		server_name: B
		listen: 127.0.0.1:-1
		leafnodes {
			listen: "127.0.0.1:-1"
			remotes = [
			{
				url:"nats://127.0.0.1:%d"
				account:"EXTERNAL"
			}
			]
		}
		accounts: {
			INTERNAL: {
				users: [
					{user: good, password: pwd}
				]
				imports: [
					{
						service: {
							account: EXTERNAL
							subject: "foo"
						}
					}
				]
			},
			EXTERNAL: {
				users: [
					{user: evil, password: pwd}
				]
				exports: [{service: "foo"}]
			},
		}
	`

	confB := createConfFile(t, []byte(fmt.Sprintf(bConfigTemplate, optsA.LeafNode.Port)))

	srvB, optsB := RunServerWithConfig(confB)
	defer srvB.Shutdown()

	checkLeafNodeConnected(t, srvB)

	// connect to confA, and offer a service
	nc2, err := nats.Connect(fmt.Sprintf("nats://%s:%d", optsA.Host, optsA.Port))
	if err != nil {
		t.Fatalf("Error on connect: %v", err)
	}
	defer nc2.Close()

	nc2.Subscribe("foo", func(msg *nats.Msg) {
		if err := msg.Respond([]byte("world")); err != nil {
			t.Fatalf("Error on respond: %v", err)
		}
	})
	nc2.Flush()

	checkSubInterest(t, srvB, "INTERNAL", "foo", time.Second)

	nc, err := nats.Connect(fmt.Sprintf("nats://good:pwd@%s:%d", optsB.Host, optsB.Port))
	if err != nil {
		t.Fatalf("Error on connect: %v", err)
	}
	defer nc.Close()

	resp, err := nc.Request("foo", []byte("hello"), 2*time.Second)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if resp == nil || strings.Compare("world", string(resp.Data)) != 0 {
		t.Fatal("Did not receive the correct message")
	}
}

// This will test for a bug in service export/import with leafnode restart.
// https://github.com/nats-io/nats-server/issues/1344
func TestServiceExportWithLeafnodeRestart(t *testing.T) {
	confG := createConfFile(t, []byte(`
		server_name: G
		listen: 127.0.0.1:-1
		leafnodes {
			listen: "127.0.0.1:-1"
			authorization {
				account:"EXTERNAL"
				user: ln
				password: pass
			}
		}

		accounts: {
		    INTERNAL: {
		        users: [
		            {user: good, password: pwd}
		        ]
		        exports: [{service: "foo", response: singleton}]
			    imports: [
		            {
		                service: {
		                    account: EXTERNAL
		                    subject: "evilfoo"
		                }, to: from_evilfoo
		            }
		        ]
		    },
		    EXTERNAL: {
		        users: [
		            {user: evil, password: pwd}
		        ]
		        exports: [{service: "evilfoo", response: singleton}]
			    imports: [
		            {
		                service: {
		                    account: INTERNAL
		                    subject: "foo"
		                }, to: goodfoo
		            }
		        ]
		    }
		}
	`))

	srvG, optsG := RunServerWithConfig(confG)
	defer srvG.Shutdown()

	eConfigTemplate := `
		server_name: E
		listen: 127.0.0.1:-1
		leafnodes {
			listen: "127.0.0.1:-1"
			remotes = [
			{
				url:"nats://ln:pass@127.0.0.1:%d"
				account:"EXTERNAL_GOOD"
			}
			]
		}

		accounts: {
		    INTERNAL_EVILINC: {
		        users: [
		            {user: evil, password: pwd}
		        ]
		        exports: [{service: "foo", response: singleton}]
			    imports: [
		            {
		                service: {
		                    account: EXTERNAL_GOOD
		                    subject: "goodfoo"
		                }, to: from_goodfoo
		            }
		        ]
		    },
		    EXTERNAL_GOOD: {
		        users: [
		            {user: good, password: pwd}
		        ]
		        exports: [{service: "goodfoo", response: singleton}]
			    imports: [
		            {
		                service: {
		                    account: INTERNAL_EVILINC
		                    subject: "foo"
		                }, to: evilfoo
		            }
		        ]
		    },
		}
	`

	confE := createConfFile(t, []byte(fmt.Sprintf(eConfigTemplate, optsG.LeafNode.Port)))

	srvE, optsE := RunServerWithConfig(confE)
	defer srvE.Shutdown()

	// connect to confE, and offer a service
	nc2, err := nats.Connect(fmt.Sprintf("nats://evil:pwd@%s:%d", optsE.Host, optsE.Port))
	if err != nil {
		t.Fatalf("Error on connect: %v", err)
	}
	defer nc2.Close()

	nc2.Subscribe("foo", func(msg *nats.Msg) {
		if err := msg.Respond([]byte("world")); err != nil {
			t.Fatalf("Error on respond: %v", err)
		}
	})
	nc2.Flush()

	nc, err := nats.Connect(fmt.Sprintf("nats://good:pwd@%s:%d", optsG.Host, optsG.Port))
	if err != nil {
		t.Fatalf("Error on connect: %v", err)
	}
	defer nc.Close()

	resp, err := nc.Request("from_evilfoo", []byte("hello"), 2*time.Second)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if resp == nil || strings.Compare("world", string(resp.Data)) != 0 {
		t.Fatal("Did not receive the correct message")
	}

	// Now restart server E and requestor and replier.
	nc.Close()
	nc2.Close()
	srvE.Shutdown()

	srvE, optsE = RunServerWithConfig(confE)
	defer srvE.Shutdown()

	nc2, err = nats.Connect(fmt.Sprintf("nats://evil:pwd@%s:%d", optsE.Host, optsE.Port))
	if err != nil {
		t.Fatalf("Error on connect: %v", err)
	}
	defer nc2.Close()

	nc2.Subscribe("foo", func(msg *nats.Msg) {
		if err := msg.Respond([]byte("world")); err != nil {
			t.Fatalf("Error on respond: %v", err)
		}
	})
	nc2.Flush()

	nc, err = nats.Connect(fmt.Sprintf("nats://good:pwd@%s:%d", optsG.Host, optsG.Port))
	if err != nil {
		t.Fatalf("Error on connect: %v", err)
	}
	defer nc.Close()

	resp, err = nc.Request("from_evilfoo", []byte("hello"), 2*time.Second)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if resp == nil || strings.Compare("world", string(resp.Data)) != 0 {
		t.Fatal("Did not receive the correct message")
	}
}

func TestLeafNodeQueueSubscriberUnsubscribe(t *testing.T) {
	s, opts := runLeafServer()
	defer s.Shutdown()

	lc := createLeafConn(t, opts.LeafNode.Host, opts.LeafNode.Port)
	defer lc.Close()

	leafSend, leafExpect := setupLeaf(t, lc, 1)
	leafSend("PING\r\n")
	leafExpect(pongRe)

	// Create a client on leaf server and create a queue sub
	c1 := createClientConn(t, opts.Host, opts.Port)
	defer c1.Close()

	send1, expect1 := setupConn(t, c1)
	send1("SUB foo bar 1\r\nPING\r\n")
	expect1(pongRe)

	// Leaf should receive an LS+ foo bar 1
	leafExpect(lsubRe)

	// Create a second client on leaf server and create queue sub on same group.
	c2 := createClientConn(t, opts.Host, opts.Port)
	defer c2.Close()

	send2, expect2 := setupConn(t, c2)
	send2("SUB foo bar 1\r\nPING\r\n")
	expect2(pongRe)

	// Leaf should receive an LS+ foo bar 2
	leafExpect(lsubRe)

	// Now close c1
	c1.Close()

	// Leaf should receive an indication that the queue group went to 1.
	// Which means LS+ foo bar 1.
	buf := leafExpect(lsubRe)
	if matches := lsubRe.FindAllSubmatch(buf, -1); len(matches) != 1 {
		t.Fatalf("Expected only 1 LS+, got %v", len(matches))
	}
	// Make sure that we did not get a LS- at the same time.
	if bytes.Contains(buf, []byte("LS-")) {
		t.Fatalf("Unexpected LS- in response: %q", buf)
	}
	// Make sure we receive nothing...
	expectNothing(t, lc)
}

func TestLeafNodeOriginClusterSingleHub(t *testing.T) {
	s, opts := runLeafServer()
	defer s.Shutdown()

	c1 := `
	listen: 127.0.0.1:-1
	cluster { name: ln22, listen: 127.0.0.1:-1 }
	leafnodes { remotes = [{ url: nats-leaf://127.0.0.1:%d }] }
	`
	lconf1 := createConfFile(t, []byte(fmt.Sprintf(c1, opts.LeafNode.Port)))

	ln1, lopts1 := RunServerWithConfig(lconf1)
	defer ln1.Shutdown()

	c2 := `
	listen: 127.0.0.1:-1
	cluster { name: ln22, listen: 127.0.0.1:-1, routes = [ nats-route://127.0.0.1:%d] }
	leafnodes { remotes = [{ url: nats-leaf://127.0.0.1:%d }] }
	`
	lconf2 := createConfFile(t, []byte(fmt.Sprintf(c2, lopts1.Cluster.Port, opts.LeafNode.Port)))

	ln2, _ := RunServerWithConfig(lconf2)
	defer ln2.Shutdown()

	ln3, _ := RunServerWithConfig(lconf2)
	defer ln3.Shutdown()

	checkClusterFormed(t, ln1, ln2, ln3)
	checkLeafNodeConnections(t, s, 3)

	// So now we are setup with 3 solicited leafnodes all connected to a hub.
	// We will create two clients, one on each leafnode server.
	nc1, err := nats.Connect(ln1.ClientURL())
	if err != nil {
		t.Fatalf("Error on connect: %v", err)
	}
	defer nc1.Close()

	nc2, err := nats.Connect(ln2.ClientURL())
	if err != nil {
		t.Fatalf("Error on connect: %v", err)
	}
	defer nc2.Close()

	checkInterest := func(s *server.Server, subject string) bool {
		t.Helper()
		acc, _ := s.LookupAccount("$G")
		return acc.SubscriptionInterest(subject)
	}

	waitForInterest := func(subject string, servers ...*server.Server) {
		t.Helper()
		checkFor(t, time.Second, 10*time.Millisecond, func() error {
			for _, s := range servers {
				if !checkInterest(s, subject) {
					return fmt.Errorf("No interest")
				}
			}
			return nil
		})
	}

	subj := "foo.bar"

	sub, _ := nc2.SubscribeSync(subj)
	waitForInterest(subj, ln1, ln2, ln3, s)

	// Make sure we truncated the subscription bouncing through the hub and back to other leafnodes.
	for _, s := range []*server.Server{ln1, ln3} {
		acc, _ := s.LookupAccount("$G")
		if nms := acc.Interest(subj); nms != 1 {
			t.Fatalf("Expected only one active subscription, got %d", nms)
		}
	}

	// Send a message.
	nc1.Publish(subj, nil)
	nc1.Flush()
	// Wait to propagate
	time.Sleep(25 * time.Millisecond)

	// Make sure we only get it once.
	if n, _, _ := sub.Pending(); n != 1 {
		t.Fatalf("Expected only one message, got %d", n)
	}
}

func TestLeafNodeOriginCluster(t *testing.T) {
	ca := createClusterWithName(t, "A", 3)
	defer shutdownCluster(ca)

	c1 := `
	server_name: L1
	listen: 127.0.0.1:-1
	cluster { name: ln22, listen: 127.0.0.1:-1 }
	leafnodes { remotes = [{ url: nats-leaf://127.0.0.1:%d }] }
	`
	lconf1 := createConfFile(t, []byte(fmt.Sprintf(c1, ca.opts[0].LeafNode.Port)))

	ln1, lopts1 := RunServerWithConfig(lconf1)
	defer ln1.Shutdown()

	c2 := `
	server_name: L2
	listen: 127.0.0.1:-1
	cluster { name: ln22, listen: 127.0.0.1:-1, routes = [ nats-route://127.0.0.1:%d] }
	leafnodes { remotes = [{ url: nats-leaf://127.0.0.1:%d }] }
	`
	lconf2 := createConfFile(t, []byte(fmt.Sprintf(c2, lopts1.Cluster.Port, ca.opts[1].LeafNode.Port)))

	ln2, _ := RunServerWithConfig(lconf2)
	defer ln2.Shutdown()

	c3 := `
	server_name: L3
	listen: 127.0.0.1:-1
	cluster { name: ln22, listen: 127.0.0.1:-1, routes = [ nats-route://127.0.0.1:%d] }
	leafnodes { remotes = [{ url: nats-leaf://127.0.0.1:%d }] }
	`
	lconf3 := createConfFile(t, []byte(fmt.Sprintf(c3, lopts1.Cluster.Port, ca.opts[2].LeafNode.Port)))

	ln3, _ := RunServerWithConfig(lconf3)
	defer ln3.Shutdown()

	checkClusterFormed(t, ln1, ln2, ln3)
	checkLeafNodeConnections(t, ca.servers[0], 1)
	checkLeafNodeConnections(t, ca.servers[1], 1)
	checkLeafNodeConnections(t, ca.servers[2], 1)

	// So now we are setup with 3 solicited leafnodes connected to different servers in the hub cluster.
	// We will create two clients, one on each leafnode server.
	nc1, err := nats.Connect(ln1.ClientURL())
	if err != nil {
		t.Fatalf("Error on connect: %v", err)
	}
	defer nc1.Close()

	nc2, err := nats.Connect(ln2.ClientURL())
	if err != nil {
		t.Fatalf("Error on connect: %v", err)
	}
	defer nc2.Close()

	checkInterest := func(s *server.Server, subject string) bool {
		t.Helper()
		acc, _ := s.LookupAccount("$G")
		return acc.SubscriptionInterest(subject)
	}

	waitForInterest := func(subject string, servers ...*server.Server) {
		t.Helper()
		checkFor(t, time.Second, 10*time.Millisecond, func() error {
			for _, s := range servers {
				if !checkInterest(s, subject) {
					return fmt.Errorf("No interest")
				}
			}
			return nil
		})
	}

	subj := "foo.bar"

	sub, _ := nc2.SubscribeSync(subj)
	waitForInterest(subj, ln1, ln2, ln3, ca.servers[0], ca.servers[1], ca.servers[2])

	// Make sure we truncated the subscription bouncing through the hub and back to other leafnodes.
	for _, s := range []*server.Server{ln1, ln3} {
		acc, _ := s.LookupAccount("$G")
		if nms := acc.Interest(subj); nms != 1 {
			t.Fatalf("Expected only one active subscription, got %d", nms)
		}
	}

	// Send a message.
	nc1.Publish(subj, nil)
	nc1.Flush()
	// Wait to propagate
	time.Sleep(25 * time.Millisecond)

	// Make sure we only get it once.
	if n, _, _ := sub.Pending(); n != 1 {
		t.Fatalf("Expected only one message, got %d", n)
	}
	// eat the msg
	sub.NextMsg(time.Second)

	// Now create interest on the hub side. This will draw the message from a leafnode
	// to the hub. We want to make sure that message does not bounce back to other leafnodes.
	nc3, err := nats.Connect(ca.servers[0].ClientURL())
	if err != nil {
		t.Fatalf("Error on connect: %v", err)
	}
	defer nc3.Close()

	wcSubj := "foo.*"
	wcsub, _ := nc3.SubscribeSync(wcSubj)
	// This is a placeholder that we can use to check all interest has propagated.
	nc3.SubscribeSync("bar")
	waitForInterest("bar", ln1, ln2, ln3, ca.servers[0], ca.servers[1], ca.servers[2])

	// Send another message.
	m := nats.NewMsg(subj)
	m.Header.Add("Accept-Encoding", "json")
	m.Header.Add("Authorization", "s3cr3t")
	m.Data = []byte("Hello Headers!")

	nc1.PublishMsg(m)
	nc1.Flush()
	// Wait to propagate
	time.Sleep(25 * time.Millisecond)

	// Make sure we only get it once.
	if n, _, _ := sub.Pending(); n != 1 {
		t.Fatalf("Expected only one message, got %d", n)
	}
	// Also for wc
	if n, _, _ := wcsub.Pending(); n != 1 {
		t.Fatalf("Expected only one message, got %d", n)
	}

	// grab the msg
	msg, _ := sub.NextMsg(time.Second)
	if !bytes.Equal(m.Data, msg.Data) {
		t.Fatalf("Expected the payloads to match, wanted %q, got %q", m.Data, msg.Data)
	}
	if len(msg.Header) != 2 {
		t.Fatalf("Expected 2 header entries, got %d", len(msg.Header))
	}
	if msg.Header.Get("Authorization") != "s3cr3t" {
		t.Fatalf("Expected auth header to match, wanted %q, got %q", "s3cr3t", msg.Header.Get("Authorization"))
	}
}

func TestLeafNodeAdvertiseInCluster(t *testing.T) {
	o1 := testDefaultOptionsForLeafNodes()
	o1.Cluster.Name = "abc"
	o1.Cluster.Host = "127.0.0.1"
	o1.Cluster.Port = -1
	s1 := runGatewayServer(o1)
	defer s1.Shutdown()

	lc := createLeafConn(t, o1.LeafNode.Host, o1.LeafNode.Port)
	defer lc.Close()

	leafSend, leafExpect := setupLeaf(t, lc, 1)
	leafSend("PING\r\n")
	leafExpect(pongRe)

	o2 := testDefaultOptionsForLeafNodes()
	o2.Cluster.Name = "abc"
	o2.Cluster.Host = "127.0.0.1"
	o2.Cluster.Port = -1
	o2.Routes = server.RoutesFromStr(fmt.Sprintf("nats://127.0.0.1:%d", o1.Cluster.Port))
	o2.LeafNode.Advertise = "srvB:7222"
	s2 := runGatewayServer(o2)
	defer s2.Shutdown()

	checkClusterFormed(t, s1, s2)

	buf := leafExpect(infoRe)
	si := &server.Info{}
	json.Unmarshal(buf[5:], si)
	var ok bool
	for _, u := range si.LeafNodeURLs {
		if u == "srvB:7222" {
			ok = true
			break
		}
	}
	if !ok {
		t.Fatalf("Url srvB:7222 was not found: %q", si.GatewayURLs)
	}

	o3 := testDefaultOptionsForLeafNodes()
	o3.Cluster.Name = "abc"
	o3.Cluster.Host = "127.0.0.1"
	o3.Cluster.Port = -1
	o3.Routes = server.RoutesFromStr(fmt.Sprintf("nats://127.0.0.1:%d", o1.Cluster.Port))
	o3.LeafNode.Advertise = "srvB:7222"
	s3 := runGatewayServer(o3)
	defer s3.Shutdown()

	checkClusterFormed(t, s1, s2, s3)

	// Since it is the save srvB:7222 url, we should not get an update.
	expectNothing(t, lc)

	// Now shutdown s2 and make sure that we are not getting an update
	// with srvB:7222 missing.
	s2.Shutdown()
	expectNothing(t, lc)
}

func TestLeafNodeAndGatewaysStreamAndShadowSubs(t *testing.T) {
	server.SetGatewaysSolicitDelay(10 * time.Millisecond)
	defer server.ResetGatewaysSolicitDelay()
	conf1 := createConfFile(t, []byte(`
		port: -1
		system_account: SYS
		accounts {
			SYS {}
			A: {
				users: [{ user: a, password: pwd, permissions: {publish: [A.b.>]} }]
				exports: [{ stream: A.b.>, accounts: [B] }]
			},
			B: {
				users: [{ user: b, password: pwd, permissions: {subscribe: [ A.b.> ]}}]
				imports: [{ stream: { account: A, subject: A.b.> } }]
			}
		}
		gateway {
			name: "A"
			port: -1
		}
		leafnodes {
			port: -1
			authorization: {
				users: [
					{user: a, password: pwd, account: A}
				]
			}
		}
	`))
	s1, o1 := RunServerWithConfig(conf1)
	defer s1.Shutdown()

	conf2 := createConfFile(t, []byte(fmt.Sprintf(`
		port: -1
		system_account: SYS
		accounts {
			SYS {}
			A: {
				users: [{ user: a, password: pwd, permissions: {publish: [A.b.>]} }]
				exports: [{ stream: A.b.>, accounts: [B] }]
			},
			B: {
				users: [{ user: b, password: pwd, permissions: {subscribe: [ A.b.> ]}}]
				imports: [{ stream: { account: A, subject: A.b.> } }]
			}
		}
		gateway {
			name: "B"
			port: -1
			gateways [
				{
					name: "A"
					urls: ["nats://127.0.0.1:%d"]
				}
			]
		}
	`, o1.Gateway.Port)))
	s2, o2 := RunServerWithConfig(conf2)
	defer s2.Shutdown()

	waitForOutboundGateways(t, s1, 1, 2*time.Second)
	waitForOutboundGateways(t, s2, 1, 2*time.Second)

	nc, err := nats.Connect(fmt.Sprintf("nats://b:pwd@127.0.0.1:%d", o2.Port))
	if err != nil {
		t.Fatalf("Error on connect: %v", err)
	}
	defer nc.Close()

	sub, err := nc.SubscribeSync("A.b.>")
	if err != nil {
		t.Fatalf("Error on subscibe: %v", err)
	}
	defer sub.Unsubscribe()

	conf3 := createConfFile(t, []byte(fmt.Sprintf(`
		port: -1
		system_account: SYS
		accounts: {
			SYS {}
			C: {
				imports: [{ stream: { account: D, subject: b.> }, prefix: A }]
			}
			D: {
				users: [{ user: d, password: pwd, permissions: {publish: [ b.> ]} }]
				exports: [{ stream: b.>, accounts: [C] }]
			}
		}
		leafnodes {
			remotes [
				{
					url: "nats://a:pwd@127.0.0.1:%d"
					account: C
				}
			]
		}
	`, o1.LeafNode.Port)))
	s3, o3 := RunServerWithConfig(conf3)
	defer s3.Shutdown()

	checkLeafNodeConnected(t, s1)
	checkLeafNodeConnected(t, s3)

	ncl, err := nats.Connect(fmt.Sprintf("nats://d:pwd@127.0.0.1:%d", o3.Port))
	if err != nil {
		t.Fatalf("Error connecting: %v", err)
	}
	defer ncl.Close()

	ncl.Publish("b.c", []byte("test"))
	if _, err := sub.NextMsg(time.Second); err != nil {
		t.Fatalf("Did not receive message: %v", err)
	}
}

func TestLeafnodeHeaders(t *testing.T) {
	srv, opts := runLeafServer()
	defer srv.Shutdown()
	leaf, _ := runSolicitLeafServer(opts)
	defer leaf.Shutdown()

	checkLeafNodeConnected(t, srv)
	checkLeafNodeConnected(t, leaf)

	snc, err := nats.Connect(srv.ClientURL())
	if err != nil {
		t.Fatal(err.Error())
	}
	defer snc.Close()

	lnc, err := nats.Connect(leaf.ClientURL())
	if err != nil {
		t.Fatal(err.Error())
	}
	defer lnc.Close()

	// Start with subscription on leaf so that we check that srv has the interest
	// (since we are going to publish from srv)
	lsub, err := lnc.SubscribeSync("test")
	if err != nil {
		t.Fatalf("subscribe failed: %s", err)
	}
	lnc.Flush()
	checkSubInterest(t, srv, "$G", "test", time.Second)

	ssub, err := snc.SubscribeSync("test")
	if err != nil {
		t.Fatalf("subscribe failed: %s", err)
	}

	msg := nats.NewMsg("test")
	msg.Header.Add("Test", "Header")
	if len(msg.Header) == 0 {
		t.Fatalf("msg header is empty")
	}
	err = snc.PublishMsg(msg)
	if err != nil {
		t.Fatal(err.Error())
	}

	smsg, err := ssub.NextMsg(time.Second)
	if err != nil {
		t.Fatalf("next failed: %s", err)
	}
	if len(smsg.Header) == 0 {
		t.Fatalf("server msgs header is empty")
	}

	lmsg, err := lsub.NextMsg(time.Second)
	if err != nil {
		t.Fatalf("next failed: %s", err)
	}
	if len(lmsg.Header) == 0 {
		t.Fatalf("leaf msg header is empty")
	}
}

func TestLeafNodeClusterNameWithSpacesRejected(t *testing.T) {
	s, opts := runLeafServer()
	defer s.Shutdown()

	lc := createLeafConn(t, opts.LeafNode.Host, opts.LeafNode.Port)
	defer lc.Close()

	checkInfoMsg(t, lc)
	sendProto(t, lc, "CONNECT {\"cluster\":\"my cluster\"}\r\n")
	expect := expectCommand(t, lc)
	expect(errRe)
	expectDisconnect(t, lc)

	lc = createLeafConn(t, opts.LeafNode.Host, opts.LeafNode.Port)
	defer lc.Close()
	leafSend, leafExpect := setupLeaf(t, lc, 1)
	// The accept side does expect an INFO from an handshake,
	// so it will "ignore" the first one.
	leafSend("INFO {\"server\":\"server\"}\r\n")
	leafSend("INFO {\"cluster\":\"my cluster\"}\r\n")
	leafExpect(errRe)
	expectDisconnect(t, lc)
}

func TestLeafNodeConnectInfo(t *testing.T) {
	for _, test := range []struct {
		name   string
		sys    string
		hasSys bool
	}{
		{"with explicit system account", "system_account: SYS", true},
		{"without explicit system account", "", false},
	} {
		t.Run(test.name, func(t *testing.T) {
			conf := createConfFile(t, []byte(fmt.Sprintf(`
				port: -1
				%s
				accounts {
					SYS: { users: [{ user: sys, password: pwd}] }
					A:   { users: [{ user: a, password: pwd}] }
					B:   { users: [{ user: b, password: pwd}] }
				}
				leafnodes {
					port: -1
				}
			`, test.sys)))
			hub, oHub := RunServerWithConfig(conf)
			defer hub.Shutdown()

			checkInfoOnConnect := func(user, acc string, isSys bool) {
				t.Helper()
				lc := createLeafConn(t, oHub.LeafNode.Host, oHub.LeafNode.Port)
				defer lc.Close()

				checkInfoMsg(t, lc)

				sendProto(t, lc, fmt.Sprintf("CONNECT {\"user\":%q,\"pass\":\"pwd\"}\r\n", user))
				info := checkInfoMsg(t, lc)
				if !info.ConnectInfo {
					t.Fatal("Expected ConnectInfo to be true")
				}
				if an := info.RemoteAccount; an != acc {
					t.Fatalf("Expected account %q, got %q", acc, info.RemoteAccount)
				}
				if ais := info.IsSystemAccount; ais != isSys {
					t.Fatalf("Expected IsSystemAccount to be %v, got %v", isSys, ais)
				}
				checkLeafNodeConnected(t, hub)
			}
			checkInfoOnConnect("a", "A", false)
			checkInfoOnConnect("sys", "SYS", test.hasSys)
			checkInfoOnConnect("b", "B", false)
		})
	}
}
