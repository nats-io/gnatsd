// Copyright 2018-2026 The NATS Authors
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
	"encoding/json"
	"errors"
	"fmt"
	"math/rand/v2"
	"net/http"
	"net/http/httptest"
	"os"
	"reflect"
	"runtime"
	"runtime/debug"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nats-io/jwt/v2"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nkeys"
	"github.com/nats-io/nuid"
)

func createAccount(s *Server) (*Account, nkeys.KeyPair) {
	okp, _ := nkeys.FromSeed(oSeed)
	akp, _ := nkeys.CreateAccount()
	pub, _ := akp.PublicKey()
	nac := jwt.NewAccountClaims(pub)
	jwt, _ := nac.Encode(okp)
	addAccountToMemResolver(s, pub, jwt)
	acc, err := s.LookupAccount(pub)
	if err != nil {
		panic(err)
	}
	return acc, akp
}

func createUserCredsEx(t *testing.T, nuc *jwt.UserClaims, akp nkeys.KeyPair) nats.Option {
	t.Helper()
	kp, _ := nkeys.CreateUser()
	nuc.Subject, _ = kp.PublicKey()
	ujwt, err := nuc.Encode(akp)
	if err != nil {
		t.Fatalf("Error generating user JWT: %v", err)
	}
	userCB := func() (string, error) {
		return ujwt, nil
	}
	sigCB := func(nonce []byte) ([]byte, error) {
		sig, _ := kp.Sign(nonce)
		return sig, nil
	}
	return nats.UserJWT(userCB, sigCB)
}

func createUserCreds(t *testing.T, _ *Server, akp nkeys.KeyPair) nats.Option {
	return createUserCredsEx(t, jwt.NewUserClaims("test"), akp)
}

func runTrustedServer(t *testing.T) (*Server, *Options) {
	t.Helper()
	opts := DefaultOptions()
	kp, _ := nkeys.FromSeed(oSeed)
	pub, _ := kp.PublicKey()
	opts.TrustedKeys = []string{pub}
	opts.AccountResolver = &MemAccResolver{}
	s := RunServer(opts)
	return s, opts
}

func runTrustedCluster(t *testing.T) (*Server, *Options, *Server, *Options, nkeys.KeyPair) {
	t.Helper()

	kp, _ := nkeys.FromSeed(oSeed)
	pub, _ := kp.PublicKey()

	mr := &MemAccResolver{}

	// Now create a system account.
	// NOTE: This can NOT be shared directly between servers.
	// Set via server options.
	okp, _ := nkeys.FromSeed(oSeed)
	akp, _ := nkeys.CreateAccount()
	apub, _ := akp.PublicKey()
	nac := jwt.NewAccountClaims(apub)
	jwt, _ := nac.Encode(okp)

	mr.Store(apub, jwt)

	optsA := DefaultOptions()
	optsA.Cluster.Name = "TEST_CLUSTER_22"
	optsA.Cluster.Host = "127.0.0.1"
	optsA.TrustedKeys = []string{pub}
	optsA.AccountResolver = mr
	optsA.SystemAccount = apub
	optsA.ServerName = "A_SRV"
	// Add in dummy gateway
	optsA.Gateway.Name = "TEST_CLUSTER_22"
	optsA.Gateway.Host = "127.0.0.1"
	optsA.Gateway.Port = -1
	optsA.gatewaysSolicitDelay = 30 * time.Second

	sa := RunServer(optsA)

	optsB := nextServerOpts(optsA)
	optsB.ServerName = "B_SRV"
	optsB.Routes = RoutesFromStr(fmt.Sprintf("nats://%s:%d", optsA.Cluster.Host, optsA.Cluster.Port))
	sb := RunServer(optsB)

	checkClusterFormed(t, sa, sb)

	return sa, optsA, sb, optsB, akp
}

func runTrustedGateways(t *testing.T) (*Server, *Options, *Server, *Options, nkeys.KeyPair) {
	t.Helper()

	kp, _ := nkeys.FromSeed(oSeed)
	pub, _ := kp.PublicKey()

	mr := &MemAccResolver{}

	// Now create a system account.
	// NOTE: This can NOT be shared directly between servers.
	// Set via server options.
	okp, _ := nkeys.FromSeed(oSeed)
	akp, _ := nkeys.CreateAccount()
	apub, _ := akp.PublicKey()
	nac := jwt.NewAccountClaims(apub)
	jwt, _ := nac.Encode(okp)

	mr.Store(apub, jwt)

	optsA := testDefaultOptionsForGateway("A")
	optsA.Cluster.Name = "A"
	optsA.Cluster.Host = "127.0.0.1"
	optsA.TrustedKeys = []string{pub}
	optsA.AccountResolver = mr
	optsA.SystemAccount = apub

	sa := RunServer(optsA)

	optsB := testGatewayOptionsFromToWithServers(t, "B", "A", sa)
	optsB.Cluster.Name = "B"
	optsB.TrustedKeys = []string{pub}
	optsB.AccountResolver = mr
	optsB.SystemAccount = apub

	sb := RunServer(optsB)

	waitForInboundGateways(t, sa, 1, time.Second)
	waitForOutboundGateways(t, sa, 1, time.Second)
	waitForInboundGateways(t, sb, 1, time.Second)
	waitForOutboundGateways(t, sb, 1, time.Second)

	return sa, optsA, sb, optsB, akp
}

func TestSystemAccount(t *testing.T) {
	s, _ := runTrustedServer(t)
	defer s.Shutdown()

	acc, _ := createAccount(s)
	s.setSystemAccount(acc)

	s.mu.Lock()
	defer s.mu.Unlock()

	if s.sys == nil || s.sys.account == nil {
		t.Fatalf("Expected sys.account to be non-nil")
	}
	if s.sys.client == nil {
		t.Fatalf("Expected sys.client to be non-nil")
	}

	s.sys.client.mu.Lock()
	defer s.sys.client.mu.Unlock()
	if s.sys.client.echo {
		t.Fatalf("Internal clients should always have echo false")
	}
}

func TestSystemAccountNewConnection(t *testing.T) {
	s, opts := runTrustedServer(t)
	defer s.Shutdown()

	acc, akp := createAccount(s)
	s.setSystemAccount(acc)

	url := fmt.Sprintf("nats://%s:%d", opts.Host, opts.Port)
	ncs, err := nats.Connect(url, createUserCreds(t, s, akp))
	if err != nil {
		t.Fatalf("Error on connect: %v", err)
	}
	defer ncs.Close()

	// We may not be able to hear ourselves (if the event is processed
	// before we create the sub), so we need to create a second client to
	// trigger the connect/disconnect events.
	acc2, akp2 := createAccount(s)

	// Be explicit to only receive the event for acc2.
	sub, _ := ncs.SubscribeSync(fmt.Sprintf("$SYS.ACCOUNT.%s.>", acc2.Name))
	defer sub.Unsubscribe()
	ncs.Flush()

	nc, err := nats.Connect(url, createUserCreds(t, s, akp2), nats.Name("TEST EVENTS"))
	if err != nil {
		t.Fatalf("Error on connect: %v", err)
	}
	defer nc.Close()

	msg, err := sub.NextMsg(time.Second)
	if err != nil {
		t.Fatalf("Error receiving msg: %v", err)
	}
	connsMsg, err := sub.NextMsg(time.Second)
	if err != nil {
		t.Fatalf("Error receiving msg: %v", err)
	}
	if strings.HasPrefix(msg.Subject, fmt.Sprintf("$SYS.ACCOUNT.%s.SERVER.CONNS", acc2.Name)) {
		msg, connsMsg = connsMsg, msg
	}
	if !strings.HasPrefix(connsMsg.Subject, fmt.Sprintf("$SYS.ACCOUNT.%s.SERVER.CONNS", acc2.Name)) {
		t.Fatalf("Expected subject to start with %q, got %q", "$SYS.ACCOUNT.<account>.CONNECT", msg.Subject)
	}
	conns := AccountNumConns{}
	if err := json.Unmarshal(connsMsg.Data, &conns); err != nil {
		t.Fatalf("Error unmarshalling conns event message: %v", err)
	} else if conns.Account != acc2.Name {
		t.Fatalf("Wrong account in conns message: %v", conns)
	} else if conns.Conns != 1 || conns.TotalConns != 1 || conns.LeafNodes != 0 {
		t.Fatalf("Wrong counts in conns message: %v", conns)
	}
	if !strings.HasPrefix(msg.Subject, fmt.Sprintf("$SYS.ACCOUNT.%s.CONNECT", acc2.Name)) {
		t.Fatalf("Expected subject to start with %q, got %q", "$SYS.ACCOUNT.<account>.CONNECT", msg.Subject)
	}
	tokens := strings.Split(msg.Subject, ".")
	if len(tokens) < 4 {
		t.Fatalf("Expected 4 tokens, got %d", len(tokens))
	}
	account := tokens[2]
	if account != acc2.Name {
		t.Fatalf("Expected %q for account, got %q", acc2.Name, account)
	}

	cem := ConnectEventMsg{}
	if err := json.Unmarshal(msg.Data, &cem); err != nil {
		t.Fatalf("Error unmarshalling connect event message: %v", err)
	}
	if cem.Type != ConnectEventMsgType {
		t.Fatalf("Incorrect schema in connect event: %s", cem.Type)
	}
	if cem.Time.IsZero() {
		t.Fatalf("Event time is not set")
	}
	if len(cem.ID) != 22 {
		t.Fatalf("Event ID is incorrectly set to len %d", len(cem.ID))
	}
	if cem.Server.ID != s.ID() {
		t.Fatalf("Expected server to be %q, got %q", s.ID(), cem.Server.ID)
	}
	if cem.Server.Seq == 0 {
		t.Fatalf("Expected sequence to be non-zero")
	}
	if cem.Client.Name != "TEST EVENTS" {
		t.Fatalf("Expected client name to be %q, got %q", "TEST EVENTS", cem.Client.Name)
	}
	if cem.Client.Lang != "go" {
		t.Fatalf("Expected client lang to be \"go\", got %q", cem.Client.Lang)
	}

	// Now close the other client. Should fire a disconnect event.
	// First send and receive some messages.
	sub2, _ := nc.SubscribeSync("foo")
	defer sub2.Unsubscribe()
	sub3, _ := nc.SubscribeSync("*")
	defer sub3.Unsubscribe()

	for i := 0; i < 10; i++ {
		nc.Publish("foo", []byte("HELLO WORLD"))
	}
	nc.Flush()
	nc.Close()

	msg, err = sub.NextMsg(time.Second)
	if err != nil {
		t.Fatalf("Error receiving msg: %v", err)
	}
	connsMsg, err = sub.NextMsg(time.Second)
	if err != nil {
		t.Fatalf("Error receiving msg: %v", err)
	}
	if strings.HasPrefix(msg.Subject, fmt.Sprintf("$SYS.ACCOUNT.%s.SERVER.CONNS", acc2.Name)) {
		msg, connsMsg = connsMsg, msg
	}
	if !strings.HasPrefix(connsMsg.Subject, fmt.Sprintf("$SYS.ACCOUNT.%s.SERVER.CONNS", acc2.Name)) {
		t.Fatalf("Expected subject to start with %q, got %q", "$SYS.ACCOUNT.<account>.CONNECT", msg.Subject)
	} else if !strings.Contains(string(connsMsg.Data), `"total_conns":0`) {
		t.Fatalf("Expected event to reflect created connection, got: %s", string(connsMsg.Data))
	}
	conns = AccountNumConns{}
	if err := json.Unmarshal(connsMsg.Data, &conns); err != nil {
		t.Fatalf("Error unmarshalling conns event message: %v", err)
	} else if conns.Account != acc2.Name {
		t.Fatalf("Wrong account in conns message: %v", conns)
	} else if conns.Conns != 0 || conns.TotalConns != 0 || conns.LeafNodes != 0 {
		t.Fatalf("Wrong counts in conns message: %v", conns)
	}
	if !strings.HasPrefix(msg.Subject, fmt.Sprintf("$SYS.ACCOUNT.%s.DISCONNECT", acc2.Name)) {
		t.Fatalf("Expected subject to start with %q, got %q", "$SYS.ACCOUNT.<account>.DISCONNECT", msg.Subject)
	}
	tokens = strings.Split(msg.Subject, ".")
	if len(tokens) < 4 {
		t.Fatalf("Expected 4 tokens, got %d", len(tokens))
	}
	account = tokens[2]
	if account != acc2.Name {
		t.Fatalf("Expected %q for account, got %q", acc2.Name, account)
	}

	dem := DisconnectEventMsg{}
	if err := json.Unmarshal(msg.Data, &dem); err != nil {
		t.Fatalf("Error unmarshalling disconnect event message: %v", err)
	}
	if dem.Type != DisconnectEventMsgType {
		t.Fatalf("Incorrect schema in connect event: %s", cem.Type)
	}
	if dem.Time.IsZero() {
		t.Fatalf("Event time is not set")
	}
	if len(dem.ID) != 22 {
		t.Fatalf("Event ID is incorrectly set to len %d", len(cem.ID))
	}
	if dem.Server.ID != s.ID() {
		t.Fatalf("Expected server to be %q, got %q", s.ID(), dem.Server.ID)
	}
	if dem.Server.Seq == 0 {
		t.Fatalf("Expected sequence to be non-zero")
	}
	if dem.Server.Seq <= cem.Server.Seq {
		t.Fatalf("Expected sequence to be increasing")
	}

	if cem.Client.Name != "TEST EVENTS" {
		t.Fatalf("Expected client name to be %q, got %q", "TEST EVENTS", dem.Client.Name)
	}
	if dem.Client.Lang != "go" {
		t.Fatalf("Expected client lang to be \"go\", got %q", dem.Client.Lang)
	}

	if dem.Sent.Msgs != 10 {
		t.Fatalf("Expected 10 msgs sent, got %d", dem.Sent.Msgs)
	}
	if dem.Sent.Bytes != 110 {
		t.Fatalf("Expected 110 bytes sent, got %d", dem.Sent.Bytes)
	}
	if dem.Received.Msgs != 20 {
		t.Fatalf("Expected 20 msgs received, got %d", dem.Sent.Msgs)
	}
	if dem.Received.Bytes != 220 {
		t.Fatalf("Expected 220 bytes sent, got %d", dem.Sent.Bytes)
	}
}

func runTrustedLeafServer(t *testing.T) (*Server, *Options) {
	t.Helper()
	opts := DefaultOptions()
	kp, _ := nkeys.FromSeed(oSeed)
	pub, _ := kp.PublicKey()
	opts.TrustedKeys = []string{pub}
	opts.AccountResolver = &MemAccResolver{}
	opts.LeafNode.Port = -1
	s := RunServer(opts)
	return s, opts
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

func runSolicitWithCredentials(t *testing.T, opts *Options, creds string) (*Server, *Options, string) {
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

// Helper function to check that a leaf node has connected to our server.
func checkLeafNodeConnected(t testing.TB, s *Server) {
	t.Helper()
	checkLeafNodeConnectedCount(t, s, 1)
}

// Helper function to check that a leaf node has connected to n server.
func checkLeafNodeConnectedCount(t testing.TB, s *Server, lnCons int) {
	t.Helper()
	checkFor(t, 5*time.Second, 15*time.Millisecond, func() error {
		if nln := s.NumLeafNodes(); nln != lnCons {
			return fmt.Errorf("Expected %d connected leafnode(s) for server %v, got %d",
				lnCons, s, nln)
		}
		return nil
	})
}

func TestSystemAccountingWithLeafNodes(t *testing.T) {
	s, opts := runTrustedLeafServer(t)
	defer s.Shutdown()

	acc, akp := createAccount(s)
	s.setSystemAccount(acc)

	url := fmt.Sprintf("nats://%s:%d", opts.Host, opts.Port)
	ncs, err := nats.Connect(url, createUserCreds(t, s, akp))
	if err != nil {
		t.Fatalf("Error on connect: %v", err)
	}
	defer ncs.Close()

	acc2, akp2 := createAccount(s)

	// Be explicit to only receive the event for acc2 account.
	sub, _ := ncs.SubscribeSync(fmt.Sprintf("$SYS.ACCOUNT.%s.DISCONNECT", acc2.Name))
	defer sub.Unsubscribe()
	ncs.Flush()

	kp, _ := nkeys.CreateUser()
	pub, _ := kp.PublicKey()
	nuc := jwt.NewUserClaims(pub)
	ujwt, err := nuc.Encode(akp2)
	if err != nil {
		t.Fatalf("Error generating user JWT: %v", err)
	}
	seed, _ := kp.Seed()
	mycreds := genCredsFile(t, ujwt, seed)

	// Create a server that solicits a leafnode connection.
	sl, slopts, _ := runSolicitWithCredentials(t, opts, mycreds)
	defer sl.Shutdown()

	checkLeafNodeConnected(t, s)

	// Compute the expected number of subs on "sl" based on number
	// of existing subs before creating the sub on "s".
	expected := int(sl.NumSubscriptions() + 1)

	nc, err := nats.Connect(url, createUserCreds(t, s, akp2), nats.Name("TEST LEAFNODE EVENTS"))
	if err != nil {
		t.Fatalf("Error on connect: %v", err)
	}
	defer nc.Close()
	fooSub := natsSubSync(t, nc, "foo")
	natsFlush(t, nc)

	checkExpectedSubs(t, expected, sl)

	surl := fmt.Sprintf("nats://%s:%d", slopts.Host, slopts.Port)
	nc2, err := nats.Connect(surl, nats.Name("TEST LEAFNODE EVENTS"))
	if err != nil {
		t.Fatalf("Error on connect: %v", err)
	}
	defer nc2.Close()

	// Compute the expected number of subs on "s" based on number
	// of existing subs before creating the sub on "sl".
	expected = int(s.NumSubscriptions() + 1)

	m := []byte("HELLO WORLD")

	// Now generate some traffic
	starSub := natsSubSync(t, nc2, "*")
	for i := 0; i < 10; i++ {
		nc2.Publish("foo", m)
		nc2.Publish("bar", m)
	}
	natsFlush(t, nc2)

	checkExpectedSubs(t, expected, s)

	// Now send some from the cluster side too.
	for i := 0; i < 10; i++ {
		nc.Publish("foo", m)
		nc.Publish("bar", m)
	}
	nc.Flush()

	// Make sure all messages are received
	for i := 0; i < 20; i++ {
		if _, err := fooSub.NextMsg(time.Second); err != nil {
			t.Fatalf("Did not get message: %v", err)
		}
	}
	for i := 0; i < 40; i++ {
		if _, err := starSub.NextMsg(time.Second); err != nil {
			t.Fatalf("Did not get message: %v", err)
		}
	}

	// Now shutdown the leafnode server since this is where the event tracking should
	// happen. Right now we do not track local clients to the leafnode server that
	// solicited to the cluster, but we should track usage once the leafnode connection stops.
	sl.Shutdown()

	// Make sure we get disconnect event and that tracking is correct.
	msg, err := sub.NextMsg(time.Second)
	if err != nil {
		t.Fatalf("Error receiving msg: %v", err)
	}

	dem := DisconnectEventMsg{}
	if err := json.Unmarshal(msg.Data, &dem); err != nil {
		t.Fatalf("Error unmarshalling disconnect event message: %v", err)
	}
	if dem.Sent.Msgs != 10 {
		t.Fatalf("Expected 10 msgs sent, got %d", dem.Sent.Msgs)
	}
	if dem.Sent.Bytes != 110 {
		t.Fatalf("Expected 110 bytes sent, got %d", dem.Sent.Bytes)
	}
	if dem.Received.Msgs != 20 {
		t.Fatalf("Expected 20 msgs received, got %d", dem.Received.Msgs)
	}
	if dem.Received.Bytes != 220 {
		t.Fatalf("Expected 220 bytes sent, got %d", dem.Received.Bytes)
	}
}

func TestSystemAccountDisconnectBadLogin(t *testing.T) {
	s, opts := runTrustedServer(t)
	defer s.Shutdown()

	acc, akp := createAccount(s)
	s.setSystemAccount(acc)

	url := fmt.Sprintf("nats://%s:%d", opts.Host, opts.Port)
	ncs, err := nats.Connect(url, createUserCreds(t, s, akp))
	if err != nil {
		t.Fatalf("Error on connect: %v", err)
	}
	defer ncs.Close()

	// Listen for auth error events though.
	asub, _ := ncs.SubscribeSync("$SYS.SERVER.*.CLIENT.AUTH.ERR")
	defer asub.Unsubscribe()

	ncs.Flush()

	nats.Connect(url, nats.Name("TEST BAD LOGIN"))

	m, err := asub.NextMsg(100 * time.Millisecond)
	if err != nil {
		t.Fatalf("Should have heard an auth error event")
	}
	dem := DisconnectEventMsg{}
	if err := json.Unmarshal(m.Data, &dem); err != nil {
		t.Fatalf("Error unmarshalling disconnect event message: %v", err)
	}
	if dem.Reason != "Authentication Failure" {
		t.Fatalf("Expected auth error, got %q", dem.Reason)
	}
}

func TestSysSubscribeRace(t *testing.T) {
	s, opts := runTrustedServer(t)
	defer s.Shutdown()

	acc, akp := createAccount(s)
	s.setSystemAccount(acc)

	url := fmt.Sprintf("nats://%s:%d", opts.Host, opts.Port)

	nc, err := nats.Connect(url, createUserCreds(t, s, akp))
	if err != nil {
		t.Fatalf("Error on connect: %v", err)
	}
	defer nc.Close()

	done := make(chan struct{})
	wg := sync.WaitGroup{}
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			nc.Publish("foo", []byte("hello"))
			select {
			case <-done:
				return
			default:
			}
		}
	}()

	time.Sleep(10 * time.Millisecond)

	received := make(chan struct{})
	// Create message callback handler.
	cb := func(sub *subscription, producer *client, _ *Account, subject, reply string, msg []byte) {
		select {
		case received <- struct{}{}:
		default:
		}
	}
	// Now create an internal subscription
	sub, err := s.sysSubscribe("foo", cb)
	if sub == nil || err != nil {
		t.Fatalf("Expected to subscribe, got %v", err)
	}
	select {
	case <-received:
		close(done)
	case <-time.After(time.Second):
		t.Fatalf("Did not receive the message")
	}
	wg.Wait()
}

func TestSystemAccountInternalSubscriptions(t *testing.T) {
	s, opts := runTrustedServer(t)
	defer s.Shutdown()

	sub, err := s.sysSubscribe("foo", nil)
	if sub != nil || err != ErrNoSysAccount {
		t.Fatalf("Expected to get proper error, got %v", err)
	}

	acc, akp := createAccount(s)
	s.setSystemAccount(acc)

	url := fmt.Sprintf("nats://%s:%d", opts.Host, opts.Port)

	nc, err := nats.Connect(url, createUserCreds(t, s, akp))
	if err != nil {
		t.Fatalf("Error on connect: %v", err)
	}
	defer nc.Close()

	sub, err = s.sysSubscribe("foo", nil)
	if sub != nil || err == nil {
		t.Fatalf("Expected to get error for no handler, got %v", err)
	}

	received := make(chan *nats.Msg)
	// Create message callback handler.
	cb := func(sub *subscription, _ *client, _ *Account, subject, reply string, msg []byte) {
		copy := append([]byte(nil), msg...)
		received <- &nats.Msg{Subject: subject, Reply: reply, Data: copy}
	}

	// Now create an internal subscription
	sub, err = s.sysSubscribe("foo", cb)
	if sub == nil || err != nil {
		t.Fatalf("Expected to subscribe, got %v", err)
	}
	// Now send out a message from our normal client.
	nc.Publish("foo", []byte("HELLO WORLD"))

	var msg *nats.Msg

	select {
	case msg = <-received:
		if msg.Subject != "foo" {
			t.Fatalf("Expected \"foo\" as subject, got %q", msg.Subject)
		}
		if msg.Reply != "" {
			t.Fatalf("Expected no reply, got %q", msg.Reply)
		}
		if !bytes.Equal(msg.Data, []byte("HELLO WORLD")) {
			t.Fatalf("Got the wrong msg payload: %q", msg.Data)
		}
		break
	case <-time.After(time.Second):
		t.Fatalf("Did not receive the message")
	}
	s.sysUnsubscribe(sub)

	// Now send out a message from our normal client.
	// We should not see this one.
	nc.Publish("foo", []byte("You There?"))

	select {
	case <-received:
		t.Fatalf("Received a message when we should not have")
	case <-time.After(100 * time.Millisecond):
		break
	}

	// Now make sure we do not hear ourselves. We optimize this for internally
	// generated messages.
	s.mu.Lock()
	s.sendInternalMsg("foo", "", nil, msg.Data)
	s.mu.Unlock()

	select {
	case <-received:
		t.Fatalf("Received a message when we should not have")
	case <-time.After(100 * time.Millisecond):
		break
	}
}

func TestSystemAccountConnectionUpdatesStopAfterNoLocal(t *testing.T) {
	sa, _, sb, optsB, _ := runTrustedCluster(t)
	defer sa.Shutdown()
	defer sb.Shutdown()

	// Normal Account
	okp, _ := nkeys.FromSeed(oSeed)
	akp, _ := nkeys.CreateAccount()
	pub, _ := akp.PublicKey()
	nac := jwt.NewAccountClaims(pub)
	nac.Limits.Conn = 4 // Limit to 4 connections.
	jwt, _ := nac.Encode(okp)

	addAccountToMemResolver(sa, pub, jwt)

	// Listen for updates to the new account connection activity.
	received := make(chan *nats.Msg, 10)
	cb := func(sub *subscription, _ *client, _ *Account, subject, reply string, msg []byte) {
		copy := append([]byte(nil), msg...)
		received <- &nats.Msg{Subject: subject, Reply: reply, Data: copy}
	}
	subj := fmt.Sprintf(accConnsEventSubjNew, pub)
	sub, err := sa.sysSubscribe(subj, cb)
	if sub == nil || err != nil {
		t.Fatalf("Expected to subscribe, got %v", err)
	}
	defer sa.sysUnsubscribe(sub)

	// Create a few users on the new account.
	clients := []*nats.Conn{}

	url := fmt.Sprintf("nats://%s:%d", optsB.Host, optsB.Port)
	for i := 0; i < 4; i++ {
		nc, err := nats.Connect(url, createUserCreds(t, sb, akp))
		if err != nil {
			t.Fatalf("Error on connect: %v", err)
		}
		defer nc.Close()
		clients = append(clients, nc)
	}

	// Wait for all 4 notifications.
	checkFor(t, time.Second, 50*time.Millisecond, func() error {
		if len(received) == 4 {
			return nil
		}
		return fmt.Errorf("Not enough messages, %d vs 4", len(received))
	})

	// Now lookup the account doing the events on sb.
	acc, _ := sb.LookupAccount(pub)
	// Make sure we have the timer running.
	acc.mu.RLock()
	ctmr := acc.ctmr
	acc.mu.RUnlock()
	if ctmr == nil {
		t.Fatalf("Expected event timer for acc conns to be running")
	}

	// Now close all of the connections.
	for _, nc := range clients {
		nc.Close()
	}

	// Wait for the 4 new notifications, 8 total (4 for connect, 4 for disconnect)
	checkFor(t, time.Second, 50*time.Millisecond, func() error {
		if len(received) == 8 {
			return nil
		}
		return fmt.Errorf("Not enough messages, %d vs 4", len(received))
	})
	// Drain the messages.
	for i := 0; i < 7; i++ {
		<-received
	}
	// Check last one.
	msg := <-received
	m := AccountNumConns{}
	if err := json.Unmarshal(msg.Data, &m); err != nil {
		t.Fatalf("Error unmarshalling account connections request message: %v", err)
	}
	if m.Conns != 0 {
		t.Fatalf("Expected Conns to be 0, got %d", m.Conns)
	}

	// Should not receive any more messages..
	select {
	case <-received:
		t.Fatalf("Did not expect a message here")
	case <-time.After(50 * time.Millisecond):
		break
	}

	// Make sure we have the timer is NOT running.
	acc.mu.RLock()
	ctmr = acc.ctmr
	acc.mu.RUnlock()
	if ctmr != nil {
		t.Fatalf("Expected event timer for acc conns to NOT be running after reaching zero local clients")
	}
}

func TestSystemAccountConnectionLimits(t *testing.T) {
	sa, optsA, sb, optsB, _ := runTrustedCluster(t)
	defer sa.Shutdown()
	defer sb.Shutdown()

	// We want to test that we are limited to a certain number of active connections
	// across multiple servers.

	// Let's create a user account.
	okp, _ := nkeys.FromSeed(oSeed)
	akp, _ := nkeys.CreateAccount()
	pub, _ := akp.PublicKey()
	nac := jwt.NewAccountClaims(pub)
	nac.Limits.Conn = 4 // Limit to 4 connections.
	jwt, _ := nac.Encode(okp)

	addAccountToMemResolver(sa, pub, jwt)

	urlA := fmt.Sprintf("nats://%s:%d", optsA.Host, optsA.Port)
	urlB := fmt.Sprintf("nats://%s:%d", optsB.Host, optsB.Port)

	// Create a user on each server. Break on first failure.
	for {
		nca1, err := nats.Connect(urlA, createUserCreds(t, sa, akp))
		if err != nil {
			break
		}
		defer nca1.Close()
		ncb1, err := nats.Connect(urlB, createUserCreds(t, sb, akp))
		if err != nil {
			break
		}
		defer ncb1.Close()
	}

	checkFor(t, 5*time.Second, 50*time.Millisecond, func() error {
		total := sa.NumClients() + sb.NumClients()
		if total > int(nac.Limits.Conn) {
			return fmt.Errorf("Expected only %d connections, was allowed to connect %d", nac.Limits.Conn, total)
		}
		return nil
	})
}

func TestBadAccountUpdate(t *testing.T) {
	sa, _ := runTrustedServer(t)
	defer sa.Shutdown()
	akp1, _ := nkeys.CreateAccount()
	pub, _ := akp1.PublicKey()
	nac := jwt.NewAccountClaims(pub)
	ajwt1, err := nac.Encode(oKp)
	require_NoError(t, err)
	addAccountToMemResolver(sa, pub, ajwt1)
	akp2, _ := nkeys.CreateAccount()
	pub2, _ := akp2.PublicKey()
	nac.Subject = pub2 // maliciously use a different subject but pretend to remain pub
	ajwt2, err := nac.Encode(oKp)
	require_NoError(t, err)
	acc, err := sa.fetchAccount(pub)
	require_NoError(t, err)
	if err := sa.updateAccountWithClaimJWT(acc, ajwt2); err != ErrAccountValidation {
		t.Fatalf("expected %v but got %v", ErrAccountValidation, err)
	}
}

// Make sure connection limits apply to the system account itself.
func TestSystemAccountSystemConnectionLimitsHonored(t *testing.T) {
	sa, optsA, sb, optsB, sakp := runTrustedCluster(t)
	defer sa.Shutdown()
	defer sb.Shutdown()

	okp, _ := nkeys.FromSeed(oSeed)
	// Update system account to have 10 connections
	pub, _ := sakp.PublicKey()
	nac := jwt.NewAccountClaims(pub)
	nac.Limits.Conn = 10
	ajwt, _ := nac.Encode(okp)

	addAccountToMemResolver(sa, pub, ajwt)
	addAccountToMemResolver(sb, pub, ajwt)

	// Update the accounts on each server with new claims to force update.
	sysAccA := sa.SystemAccount()
	sa.updateAccountWithClaimJWT(sysAccA, ajwt)
	sysAccB := sb.SystemAccount()
	sb.updateAccountWithClaimJWT(sysAccB, ajwt)

	// Check system here first, with no external it should be zero.
	sacc := sa.SystemAccount()
	if nlc := sacc.NumLocalConnections(); nlc != 0 {
		t.Fatalf("Expected no local connections, got %d", nlc)
	}

	urlA := fmt.Sprintf("nats://%s:%d", optsA.Host, optsA.Port)
	urlB := fmt.Sprintf("nats://%s:%d", optsB.Host, optsB.Port)

	// Create a user on each server. Break on first failure.
	tc := 0
	for {
		nca1, err := nats.Connect(urlA, createUserCreds(t, sa, sakp))
		if err != nil {
			break
		}
		defer nca1.Close()
		tc++

		ncb1, err := nats.Connect(urlB, createUserCreds(t, sb, sakp))
		if err != nil {
			break
		}
		defer ncb1.Close()
		tc++

		// The account's connection count is exchanged between servers
		// so that the local count on each server reflects the total count.
		// Pause a bit to give a chance to each server to process the update.
		time.Sleep(15 * time.Millisecond)
	}
	if tc != 10 {
		t.Fatalf("Expected to get 10 external connections, got %d", tc)
	}

	checkFor(t, 1*time.Second, 50*time.Millisecond, func() error {
		total := sa.NumClients() + sb.NumClients()
		if total > int(nac.Limits.Conn) {
			return fmt.Errorf("Expected only %d connections, was allowed to connect %d", nac.Limits.Conn, total)
		}
		return nil
	})
}

// Test that the remote accounting works when a server is started some time later.
func TestSystemAccountConnectionLimitsServersStaggered(t *testing.T) {
	sa, optsA, sb, optsB, _ := runTrustedCluster(t)
	defer sa.Shutdown()
	sb.Shutdown()

	// Let's create a user account.
	okp, _ := nkeys.FromSeed(oSeed)
	akp, _ := nkeys.CreateAccount()
	pub, _ := akp.PublicKey()
	nac := jwt.NewAccountClaims(pub)
	nac.Limits.Conn = 4 // Limit to 4 connections.
	jwt, _ := nac.Encode(okp)

	addAccountToMemResolver(sa, pub, jwt)

	urlA := fmt.Sprintf("nats://%s:%d", optsA.Host, optsA.Port)
	// Create max connections on sa.
	for i := 0; i < int(nac.Limits.Conn); i++ {
		nc, err := nats.Connect(urlA, createUserCreds(t, sa, akp))
		if err != nil {
			t.Fatalf("Unexpected error on #%d try: %v", i+1, err)
		}
		defer nc.Close()
	}

	// Restart server B.
	optsB.AccountResolver = sa.AccountResolver()
	optsB.SystemAccount = sa.SystemAccount().Name
	sb = RunServer(optsB)
	defer sb.Shutdown()
	checkClusterFormed(t, sa, sb)

	// Trigger a load of the user account on the new server
	// NOTE: If we do not load the user, the user can be the first
	// to request this account, hence the connection will succeed.
	checkFor(t, time.Second, 15*time.Millisecond, func() error {
		if acc, err := sb.LookupAccount(pub); acc == nil || err != nil {
			return fmt.Errorf("LookupAccount did not return account or failed, err=%v", err)
		}
		return nil
	})

	// Expect this to fail.
	urlB := fmt.Sprintf("nats://%s:%d", optsB.Host, optsB.Port)
	if _, err := nats.Connect(urlB, createUserCreds(t, sb, akp)); err == nil {
		t.Fatalf("Expected connection to fail due to max limit")
	}
}

// Test that the remote accounting works when a server is shutdown.
func TestSystemAccountConnectionLimitsServerShutdownGraceful(t *testing.T) {
	sa, optsA, sb, optsB, _ := runTrustedCluster(t)
	defer sa.Shutdown()
	defer sb.Shutdown()

	// Let's create a user account.
	okp, _ := nkeys.FromSeed(oSeed)
	akp, _ := nkeys.CreateAccount()
	pub, _ := akp.PublicKey()
	nac := jwt.NewAccountClaims(pub)
	nac.Limits.Conn = 10 // Limit to 10 connections.
	jwt, _ := nac.Encode(okp)

	addAccountToMemResolver(sa, pub, jwt)
	addAccountToMemResolver(sb, pub, jwt)

	urlA := fmt.Sprintf("nats://%s:%d", optsA.Host, optsA.Port)
	urlB := fmt.Sprintf("nats://%s:%d", optsB.Host, optsB.Port)

	for i := 0; i < 5; i++ {
		nc, err := nats.Connect(urlA, nats.NoReconnect(), createUserCreds(t, sa, akp))
		if err != nil {
			t.Fatalf("Expected to connect, got %v", err)
		}
		defer nc.Close()
		nc, err = nats.Connect(urlB, nats.NoReconnect(), createUserCreds(t, sb, akp))
		if err != nil {
			t.Fatalf("Expected to connect, got %v", err)
		}
		defer nc.Close()
	}

	// We are at capacity so both of these should fail.
	if _, err := nats.Connect(urlA, createUserCreds(t, sa, akp)); err == nil {
		t.Fatalf("Expected connection to fail due to max limit")
	}
	if _, err := nats.Connect(urlB, createUserCreds(t, sb, akp)); err == nil {
		t.Fatalf("Expected connection to fail due to max limit")
	}

	// Now shutdown Server B.
	sb.Shutdown()

	// Now we should be able to create more on A now.
	for i := 0; i < 5; i++ {
		nc, err := nats.Connect(urlA, createUserCreds(t, sa, akp))
		if err != nil {
			t.Fatalf("Expected to connect on %d, got %v", i, err)
		}
		defer nc.Close()
	}
}

// Test that the remote accounting works when a server goes away.
func TestSystemAccountConnectionLimitsServerShutdownForced(t *testing.T) {
	sa, optsA, sb, optsB, _ := runTrustedCluster(t)
	defer sa.Shutdown()

	// Let's create a user account.
	okp, _ := nkeys.FromSeed(oSeed)
	akp, _ := nkeys.CreateAccount()
	pub, _ := akp.PublicKey()
	nac := jwt.NewAccountClaims(pub)
	nac.Limits.Conn = 20 // Limit to 20 connections.
	jwt, _ := nac.Encode(okp)

	addAccountToMemResolver(sa, pub, jwt)
	addAccountToMemResolver(sb, pub, jwt)

	urlA := fmt.Sprintf("nats://%s:%d", optsA.Host, optsA.Port)
	urlB := fmt.Sprintf("nats://%s:%d", optsB.Host, optsB.Port)

	for i := 0; i < 10; i++ {
		c, err := nats.Connect(urlA, nats.NoReconnect(), createUserCreds(t, sa, akp))
		if err != nil {
			t.Fatalf("Expected to connect, got %v", err)
		}
		defer c.Close()
		c, err = nats.Connect(urlB, nats.NoReconnect(), createUserCreds(t, sb, akp))
		if err != nil {
			t.Fatalf("Expected to connect, got %v", err)
		}
		defer c.Close()
	}

	// Now shutdown Server B. Do so such that no communications go out.
	sb.mu.Lock()
	sb.sys = nil
	sb.mu.Unlock()
	sb.Shutdown()

	if _, err := nats.Connect(urlA, createUserCreds(t, sa, akp)); err == nil {
		t.Fatalf("Expected connection to fail due to max limit")
	}

	// Let's speed up the checking process.
	sa.mu.Lock()
	sa.sys.chkOrph = 10 * time.Millisecond
	sa.sys.orphMax = 30 * time.Millisecond
	sa.sys.sweeper.Reset(sa.sys.chkOrph)
	sa.mu.Unlock()

	// We should eventually be able to connect.
	checkFor(t, 2*time.Second, 50*time.Millisecond, func() error {
		if c, err := nats.Connect(urlA, createUserCreds(t, sa, akp)); err != nil {
			return err
		} else {
			c.Close()
		}
		return nil
	})
}

func TestSystemAccountFromConfig(t *testing.T) {
	kp, _ := nkeys.FromSeed(oSeed)
	opub, _ := kp.PublicKey()
	akp, _ := nkeys.CreateAccount()
	apub, _ := akp.PublicKey()
	nac := jwt.NewAccountClaims(apub)
	ajwt, err := nac.Encode(kp)
	if err != nil {
		t.Fatalf("Error generating account JWT: %v", err)
	}
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte(ajwt))
	}))
	defer ts.Close()

	confTemplate := `
		listen: -1
		trusted: %s
		system_account: %s
		resolver: URL("%s/jwt/v1/accounts/")
    `

	conf := createConfFile(t, []byte(fmt.Sprintf(confTemplate, opub, apub, ts.URL)))

	s, _ := RunServerWithConfig(conf)
	defer s.Shutdown()

	if acc := s.SystemAccount(); acc == nil || acc.Name != apub {
		t.Fatalf("System Account not properly set")
	}
}

func TestAccountClaimsUpdates(t *testing.T) {
	test := func(subj string) {
		s, opts := runTrustedServer(t)
		defer s.Shutdown()

		sacc, sakp := createAccount(s)
		s.setSystemAccount(sacc)

		// Let's create a normal account with limits we can update.
		okp, _ := nkeys.FromSeed(oSeed)
		akp, _ := nkeys.CreateAccount()
		pub, _ := akp.PublicKey()
		nac := jwt.NewAccountClaims(pub)
		nac.Limits.Conn = 4
		ajwt, _ := nac.Encode(okp)

		addAccountToMemResolver(s, pub, ajwt)

		acc, _ := s.LookupAccount(pub)
		if acc.MaxActiveConnections() != 4 {
			t.Fatalf("Expected to see a limit of 4 connections")
		}

		// Simulate a systems publisher so we can do an account claims update.
		url := fmt.Sprintf("nats://%s:%d", opts.Host, opts.Port)
		nc, err := nats.Connect(url, createUserCreds(t, s, sakp))
		if err != nil {
			t.Fatalf("Error on connect: %v", err)
		}
		defer nc.Close()

		// Update the account
		nac = jwt.NewAccountClaims(pub)
		nac.Limits.Conn = 8
		issAt := time.Now().Add(-30 * time.Second).Unix()
		nac.IssuedAt = issAt
		expires := time.Now().Add(2 * time.Second).Unix()
		nac.Expires = expires
		ajwt, _ = nac.Encode(okp)

		// Publish to the system update subject.
		claimUpdateSubj := fmt.Sprintf(subj, pub)
		nc.Publish(claimUpdateSubj, []byte(ajwt))
		nc.Flush()
		time.Sleep(200 * time.Millisecond)

		acc, _ = s.LookupAccount(pub)
		if acc.MaxActiveConnections() != 8 {
			t.Fatalf("Account was not updated")
		}
	}
	t.Run("new", func(t *testing.T) {
		test(accUpdateEventSubjNew)
	})
	t.Run("old", func(t *testing.T) {
		test(accUpdateEventSubjOld)
	})
}

func TestAccountReqMonitoring(t *testing.T) {
	s, opts := runTrustedServer(t)
	defer s.Shutdown()
	sacc, sakp := createAccount(s)
	s.setSystemAccount(sacc)
	s.EnableJetStream(&JetStreamConfig{StoreDir: t.TempDir()})
	unusedAcc, _ := createAccount(s)
	acc, akp := createAccount(s)
	acc.EnableJetStream(nil, nil)
	subsz := fmt.Sprintf(accDirectReqSubj, acc.Name, "SUBSZ")
	connz := fmt.Sprintf(accDirectReqSubj, acc.Name, "CONNZ")
	jsz := fmt.Sprintf(accDirectReqSubj, acc.Name, "JSZ")

	pStatz := fmt.Sprintf(accPingReqSubj, "STATZ")
	statz := func(name string) string { return fmt.Sprintf(accDirectReqSubj, name, "STATZ") }
	// Create system account connection to query
	url := fmt.Sprintf("nats://%s:%d", opts.Host, opts.Port)
	ncSys, err := nats.Connect(url, createUserCreds(t, s, sakp))
	if err != nil {
		t.Fatalf("Error on connect: %v", err)
	}
	defer ncSys.Close()
	// Create a connection that we can query
	nc, err := nats.Connect(url, createUserCreds(t, s, akp))
	if err != nil {
		t.Fatalf("Error on connect: %v", err)
	}
	defer nc.Close()
	// query SUBSZ for account
	resp, err := ncSys.Request(subsz, nil, time.Second)
	require_NoError(t, err)
	require_Contains(t, string(resp.Data), `"num_subscriptions":5,`)
	// create a subscription
	sub, err := nc.Subscribe("foo", func(msg *nats.Msg) {})
	require_NoError(t, err)
	defer sub.Unsubscribe()

	require_NoError(t, nc.Flush())
	// query SUBSZ for account
	resp, err = ncSys.Request(subsz, nil, time.Second)
	require_NoError(t, err)
	require_Contains(t, string(resp.Data), `"num_subscriptions":6,`, `"subject":"foo"`)
	// query connections for account
	resp, err = ncSys.Request(connz, nil, time.Second)
	require_NoError(t, err)
	require_Contains(t, string(resp.Data), `"num_connections":1,`, `"total":1,`)
	// query connections for js account
	resp, err = ncSys.Request(jsz, nil, time.Second)
	require_NoError(t, err)
	require_Contains(t, string(resp.Data), `"memory":0,`, `"storage":0,`)
	// query statz/conns for account
	resp, err = ncSys.Request(statz(acc.Name), nil, time.Second)
	require_NoError(t, err)
	respContentAcc := []string{`"conns":1,`, `"total_conns":1`, `"slow_consumers":0`, `"sent":{"msgs":0,"bytes":0`,
		`"received":{"msgs":0,"bytes":0`, `"num_subscriptions":`, fmt.Sprintf(`"acc":"%s"`, acc.Name)}
	require_Contains(t, string(resp.Data), respContentAcc...)

	rIb := ncSys.NewRespInbox()
	rSub, err := ncSys.SubscribeSync(rIb)
	require_NoError(t, err)
	require_NoError(t, ncSys.PublishRequest(pStatz, rIb, nil))
	minRespContentForBothAcc := []string{`"conns":1,`, `"total_conns":1`, `"slow_consumers":0`, `"acc":"`, `"num_subscriptions":`}
	resp, err = rSub.NextMsg(time.Second)
	require_NoError(t, err)
	require_Contains(t, string(resp.Data), minRespContentForBothAcc...)
	// expect one entry per account
	require_Contains(t, string(resp.Data), fmt.Sprintf(`"acc":"%s"`, acc.Name), fmt.Sprintf(`"acc":"%s"`, sacc.Name))

	// Test ping with filter by account name
	require_NoError(t, ncSys.PublishRequest(pStatz, rIb, []byte(fmt.Sprintf(`{"accounts":["%s"]}`, sacc.Name))))
	m, err := rSub.NextMsg(time.Second)
	require_NoError(t, err)
	require_Contains(t, string(m.Data), minRespContentForBothAcc...)

	require_NoError(t, ncSys.PublishRequest(pStatz, rIb, []byte(fmt.Sprintf(`{"accounts":["%s"]}`, acc.Name))))
	m, err = rSub.NextMsg(time.Second)
	require_NoError(t, err)
	require_Contains(t, string(m.Data), respContentAcc...)

	// Test include unused for statz and ping of statz
	unusedContent := []string{`"conns":0,`, `"total_conns":0`, `"slow_consumers":0`,
		fmt.Sprintf(`"acc":"%s"`, unusedAcc.Name)}

	resp, err = ncSys.Request(statz(unusedAcc.Name),
		[]byte(fmt.Sprintf(`{"accounts":["%s"], "include_unused":true}`, unusedAcc.Name)),
		time.Second)
	require_NoError(t, err)
	require_Contains(t, string(resp.Data), unusedContent...)

	require_NoError(t, ncSys.PublishRequest(pStatz, rIb,
		[]byte(fmt.Sprintf(`{"accounts":["%s"], "include_unused":true}`, unusedAcc.Name))))
	resp, err = rSub.NextMsg(time.Second)
	require_NoError(t, err)
	require_Contains(t, string(resp.Data), unusedContent...)

	require_NoError(t, ncSys.PublishRequest(pStatz, rIb, []byte(fmt.Sprintf(`{"accounts":["%s"]}`, unusedAcc.Name))))
	_, err = rSub.NextMsg(200 * time.Millisecond)
	require_Error(t, err)

	// Test ping from within account, send extra message to check counters.
	require_NoError(t, nc.Publish("foo", nil))
	require_NoError(t, nc.Flush())
	checkFor(t, time.Second, 10*time.Millisecond, func() error {
		acc.mu.RLock()
		stats := acc.statz()
		acc.mu.RUnlock()
		if stats.Received.Msgs == 0 {
			return fmt.Errorf("publish not accounted for yet")
		}
		return nil
	})

	ib := nc.NewRespInbox()
	rSub, err = nc.SubscribeSync(ib)
	require_NoError(t, err)
	require_NoError(t, nc.PublishRequest(pStatz, ib, nil))
	require_NoError(t, nc.Flush())
	resp, err = rSub.NextMsg(time.Second)
	require_NoError(t, err)

	// Since we now have processed our own message, sent msgs will be at least 1.
	payload := string(resp.Data)
	respContentAcc = []string{`"conns":1,`, `"total_conns":1`, `"slow_consumers":0`, `"sent":{"msgs":1,"bytes":0`, fmt.Sprintf(`"acc":"%s"`, acc.Name)}
	require_Contains(t, payload, respContentAcc...)

	// Depending on timing, statz message could be accounted too.
	receivedOK := strings.Contains(payload, `"received":{"msgs":1,"bytes":0`) || strings.Contains(payload, `"received":{"msgs":2,"bytes":0`)
	require_True(t, receivedOK)
	_, err = rSub.NextMsg(200 * time.Millisecond)
	require_Error(t, err)
}

func TestAccountReqInfo(t *testing.T) {
	s, opts := runTrustedServer(t)
	defer s.Shutdown()
	sacc, sakp := createAccount(s)
	s.setSystemAccount(sacc)
	// Let's create an account with service export.
	akp, _ := nkeys.CreateAccount()
	pub1, _ := akp.PublicKey()
	nac1 := jwt.NewAccountClaims(pub1)
	nac1.Exports.Add(&jwt.Export{Subject: "req.*", Type: jwt.Service})
	ajwt1, _ := nac1.Encode(oKp)
	addAccountToMemResolver(s, pub1, ajwt1)
	s.LookupAccount(pub1)
	info1 := fmt.Sprintf(accDirectReqSubj, pub1, "INFO")
	// Now add an account with service imports.
	akp2, _ := nkeys.CreateAccount()
	pub2, _ := akp2.PublicKey()
	nac2 := jwt.NewAccountClaims(pub2)
	nac2.Imports.Add(&jwt.Import{Account: pub1, Subject: "req.1", Type: jwt.Service})
	ajwt2, _ := nac2.Encode(oKp)
	addAccountToMemResolver(s, pub2, ajwt2)
	s.LookupAccount(pub2)
	info2 := fmt.Sprintf(accDirectReqSubj, pub2, "INFO")
	// Create system account connection to query
	url := fmt.Sprintf("nats://%s:%d", opts.Host, opts.Port)
	ncSys, err := nats.Connect(url, createUserCreds(t, s, sakp))
	if err != nil {
		t.Fatalf("Error on connect: %v", err)
	}
	defer ncSys.Close()
	checkCommon := func(info *AccountInfo, srv *ServerInfo, pub, jwt string) {
		if info.Complete != true {
			t.Fatalf("Unexpected value: %v", info.Complete)
		} else if info.Expired != false {
			t.Fatalf("Unexpected value: %v", info.Expired)
		} else if info.JetStream != false {
			t.Fatalf("Unexpected value: %v", info.JetStream)
		} else if info.ClientCnt != 0 {
			t.Fatalf("Unexpected value: %v", info.ClientCnt)
		} else if info.AccountName != pub {
			t.Fatalf("Unexpected value: %v", info.AccountName)
		} else if info.LeafCnt != 0 {
			t.Fatalf("Unexpected value: %v", info.LeafCnt)
		} else if info.Jwt != jwt {
			t.Fatalf("Unexpected value: %v", info.Jwt)
		} else if srv.Cluster != "abc" {
			t.Fatalf("Unexpected value: %v", srv.Cluster)
		} else if srv.Name != s.Name() {
			t.Fatalf("Unexpected value: %v", srv.Name)
		} else if srv.Host != opts.Host {
			t.Fatalf("Unexpected value: %v", srv.Host)
		} else if srv.Seq < 1 {
			t.Fatalf("Unexpected value: %v", srv.Seq)
		}
	}
	info := AccountInfo{}
	srv := ServerInfo{}
	msg := struct {
		Data *AccountInfo `json:"data"`
		Srv  *ServerInfo  `json:"server"`
	}{
		&info,
		&srv,
	}
	if resp, err := ncSys.Request(info1, nil, time.Second); err != nil {
		t.Fatalf("Error on request: %v", err)
	} else if err := json.Unmarshal(resp.Data, &msg); err != nil {
		t.Fatalf("Unmarshalling failed: %v", err)
	} else if len(info.Exports) != 1 {
		t.Fatalf("Unexpected value: %v", info.Exports)
	} else if len(info.Imports) != 4 {
		t.Fatalf("Unexpected value: %+v", info.Imports)
	} else if info.Exports[0].Subject != "req.*" {
		t.Fatalf("Unexpected value: %v", info.Exports)
	} else if info.Exports[0].Type != jwt.Service {
		t.Fatalf("Unexpected value: %v", info.Exports)
	} else if info.Exports[0].ResponseType != jwt.ResponseTypeSingleton {
		t.Fatalf("Unexpected value: %v", info.Exports)
	} else if info.SubCnt != 4 {
		t.Fatalf("Unexpected value: %v", info.SubCnt)
	} else {
		checkCommon(&info, &srv, pub1, ajwt1)
	}
	info = AccountInfo{}
	srv = ServerInfo{}
	if resp, err := ncSys.Request(info2, nil, time.Second); err != nil {
		t.Fatalf("Error on request: %v", err)
	} else if err := json.Unmarshal(resp.Data, &msg); err != nil {
		t.Fatalf("Unmarshalling failed: %v", err)
	} else if len(info.Exports) != 0 {
		t.Fatalf("Unexpected value: %v", info.Exports)
	} else if len(info.Imports) != 5 {
		t.Fatalf("Unexpected value: %+v", info.Imports)
	}
	// Here we need to find our import
	var si *ExtImport
	for _, im := range info.Imports {
		if im.Subject == "req.1" {
			si = &im
			break
		}
	}
	if si == nil {
		t.Fatalf("Could not find our import")
	}
	if si.Type != jwt.Service {
		t.Fatalf("Unexpected value: %+v", si)
	} else if si.Account != pub1 {
		t.Fatalf("Unexpected value: %+v", si)
	} else if info.SubCnt != 5 {
		t.Fatalf("Unexpected value: %+v", si)
	} else {
		checkCommon(&info, &srv, pub2, ajwt2)
	}
}

func TestAccountClaimsUpdatesWithServiceImports(t *testing.T) {
	s, opts := runTrustedServer(t)
	defer s.Shutdown()

	sacc, sakp := createAccount(s)
	s.setSystemAccount(sacc)

	okp, _ := nkeys.FromSeed(oSeed)

	// Let's create an account with service export.
	akp, _ := nkeys.CreateAccount()
	pub, _ := akp.PublicKey()
	nac := jwt.NewAccountClaims(pub)
	nac.Exports.Add(&jwt.Export{Subject: "req.*", Type: jwt.Service})
	ajwt, _ := nac.Encode(okp)
	addAccountToMemResolver(s, pub, ajwt)
	s.LookupAccount(pub)

	// Now add an account with multiple service imports.
	akp2, _ := nkeys.CreateAccount()
	pub2, _ := akp2.PublicKey()
	nac2 := jwt.NewAccountClaims(pub2)
	nac2.Imports.Add(&jwt.Import{Account: pub, Subject: "req.1", Type: jwt.Service})
	ajwt2, _ := nac2.Encode(okp)

	addAccountToMemResolver(s, pub2, ajwt2)
	s.LookupAccount(pub2)

	startSubs := s.NumSubscriptions()

	// Simulate a systems publisher so we can do an account claims update.
	url := fmt.Sprintf("nats://%s:%d", opts.Host, opts.Port)
	nc, err := nats.Connect(url, createUserCreds(t, s, sakp))
	if err != nil {
		t.Fatalf("Error on connect: %v", err)
	}
	defer nc.Close()

	// Update the account several times
	for i := 1; i <= 10; i++ {
		nac2 = jwt.NewAccountClaims(pub2)
		nac2.Limits.Conn = int64(i)
		nac2.Imports.Add(&jwt.Import{Account: pub, Subject: "req.1", Type: jwt.Service})
		ajwt2, _ = nac2.Encode(okp)

		// Publish to the system update subject.
		claimUpdateSubj := fmt.Sprintf(accUpdateEventSubjNew, pub2)
		nc.Publish(claimUpdateSubj, []byte(ajwt2))
	}
	nc.Flush()
	time.Sleep(50 * time.Millisecond)

	if startSubs < s.NumSubscriptions() {
		t.Fatalf("Subscriptions leaked: %d vs %d", startSubs, s.NumSubscriptions())
	}
}

func TestAccountConnsLimitExceededAfterUpdate(t *testing.T) {
	s, opts := runTrustedServer(t)
	defer s.Shutdown()

	sacc, _ := createAccount(s)
	s.setSystemAccount(sacc)

	// Let's create a normal  account with limits we can update.
	okp, _ := nkeys.FromSeed(oSeed)
	akp, _ := nkeys.CreateAccount()
	pub, _ := akp.PublicKey()
	nac := jwt.NewAccountClaims(pub)
	nac.Limits.Conn = 10
	ajwt, _ := nac.Encode(okp)

	addAccountToMemResolver(s, pub, ajwt)
	acc, _ := s.LookupAccount(pub)

	// Now create the max connections.
	url := fmt.Sprintf("nats://%s:%d", opts.Host, opts.Port)
	for {
		nc, err := nats.Connect(url, createUserCreds(t, s, akp))
		if err != nil {
			break
		}
		defer nc.Close()
	}

	// We should have max here.
	checkFor(t, 2*time.Second, 50*time.Millisecond, func() error {
		if total := s.NumClients(); total != acc.MaxActiveConnections() {
			return fmt.Errorf("Expected %d connections, got %d", acc.MaxActiveConnections(), total)
		}
		return nil
	})

	// Now change limits to make current connections over the limit.
	nac = jwt.NewAccountClaims(pub)
	nac.Limits.Conn = 2
	ajwt, _ = nac.Encode(okp)

	s.updateAccountWithClaimJWT(acc, ajwt)
	if acc.MaxActiveConnections() != 2 {
		t.Fatalf("Expected max connections to be set to 2, got %d", acc.MaxActiveConnections())
	}
	// We should have closed the excess connections.
	checkClientsCount(t, s, acc.MaxActiveConnections())
}

func TestAccountConnsLimitExceededAfterUpdateDisconnectNewOnly(t *testing.T) {
	s, opts := runTrustedServer(t)
	defer s.Shutdown()

	sacc, _ := createAccount(s)
	s.setSystemAccount(sacc)

	// Let's create a normal  account with limits we can update.
	okp, _ := nkeys.FromSeed(oSeed)
	akp, _ := nkeys.CreateAccount()
	pub, _ := akp.PublicKey()
	nac := jwt.NewAccountClaims(pub)
	nac.Limits.Conn = 10
	ajwt, _ := nac.Encode(okp)

	addAccountToMemResolver(s, pub, ajwt)
	acc, _ := s.LookupAccount(pub)

	// Now create the max connections.
	// We create half then we will wait and then create the rest.
	// Will test that we disconnect the newest ones.
	url := fmt.Sprintf("nats://%s:%d", opts.Host, opts.Port)
	for i := 0; i < 5; i++ {
		nc, err := nats.Connect(url, nats.Name("OLD"), nats.NoReconnect(), createUserCreds(t, s, akp))
		require_NoError(t, err)
		defer nc.Close()
	}
	time.Sleep(500 * time.Millisecond)
	for i := 0; i < 5; i++ {
		nc, err := nats.Connect(url, nats.Name("NEW"), nats.NoReconnect(), createUserCreds(t, s, akp))
		require_NoError(t, err)
		defer nc.Close()
	}

	// We should have max here.
	checkClientsCount(t, s, acc.MaxActiveConnections())

	// Now change limits to make current connections over the limit.
	nac = jwt.NewAccountClaims(pub)
	nac.Limits.Conn = 5
	ajwt, _ = nac.Encode(okp)

	s.updateAccountWithClaimJWT(acc, ajwt)
	if acc.MaxActiveConnections() != 5 {
		t.Fatalf("Expected max connections to be set to 2, got %d", acc.MaxActiveConnections())
	}
	// We should have closed the excess connections.
	checkClientsCount(t, s, acc.MaxActiveConnections())

	connz, err := s.Connz(nil)
	require_NoError(t, err)

	// There should only be OLD connections.
	for _, c := range connz.Conns {
		require_Equal(t, c.Name, "OLD")
	}
}

func TestSystemAccountWithBadRemoteLatencyUpdate(t *testing.T) {
	s, _ := runTrustedServer(t)
	defer s.Shutdown()

	acc, _ := createAccount(s)
	s.setSystemAccount(acc)

	rl := remoteLatency{
		Account: "NONSENSE",
		ReqId:   "_INBOX.22",
	}
	b, _ := json.Marshal(&rl)
	s.remoteLatencyUpdate(nil, nil, nil, "foo", _EMPTY_, nil, b)
}

func TestSystemAccountWithGateways(t *testing.T) {
	sa, oa, sb, ob, akp := runTrustedGateways(t)
	defer sa.Shutdown()
	defer sb.Shutdown()

	// Create a client on A that will subscribe on $SYS.ACCOUNT.>
	urla := fmt.Sprintf("nats://%s:%d", oa.Host, oa.Port)
	nca := natsConnect(t, urla, createUserCreds(t, sa, akp), nats.Name("SYS"))
	defer nca.Close()
	nca.Flush()

	sub, _ := nca.SubscribeSync("$SYS.ACCOUNT.>")
	defer sub.Unsubscribe()
	nca.Flush()

	// If this tests fails with wrong number after 10 seconds we may have
	// added a new initial subscription for the eventing system.
	checkExpectedSubs(t, 62, sa)

	// Create a client on B and see if we receive the event
	urlb := fmt.Sprintf("nats://%s:%d", ob.Host, ob.Port)
	ncb := natsConnect(t, urlb, createUserCreds(t, sb, akp), nats.Name("TEST EVENTS"))
	defer ncb.Close()

	// space for .CONNECT and .CONNS from SYS and $G as well as one extra message
	msgs := [4]*nats.Msg{}
	var err error
	msgs[0], err = sub.NextMsg(time.Second)
	require_NoError(t, err)
	msgs[1], err = sub.NextMsg(time.Second)
	require_NoError(t, err)
	// TODO: There is a race currently that can cause the server to process the
	// system event *after* the subscription on "A" has been registered, and so
	// the "nca" client would receive its own CONNECT message.
	msgs[2], _ = sub.NextMsg(250 * time.Millisecond)

	findMsgs := func(sub string) []*nats.Msg {
		rMsgs := []*nats.Msg{}
		for _, m := range msgs {
			if m == nil {
				continue
			}
			if m.Subject == sub {
				rMsgs = append(rMsgs, m)
			}
		}
		return rMsgs
	}

	msg := findMsgs(fmt.Sprintf("$SYS.ACCOUNT.%s.CONNECT", sa.SystemAccount().Name))
	var bMsg *nats.Msg
	if len(msg) < 1 {
		t.Fatal("Expected at least one message")
	}
	bMsg = msg[len(msg)-1]

	require_Contains(t, string(bMsg.Data), sb.ID())
	require_Contains(t, string(bMsg.Data), `"cluster":"B"`)
	require_Contains(t, string(bMsg.Data), `"name":"TEST EVENTS"`)

	connsMsgA := findMsgs(fmt.Sprintf("$SYS.ACCOUNT.%s.SERVER.CONNS", sa.SystemAccount().Name))
	if len(connsMsgA) != 1 {
		t.Fatal("Expected a message")
	}
}

func TestSystemAccountNoAuthUser(t *testing.T) {
	conf := createConfFile(t, []byte(`
		listen: "127.0.0.1:-1"
		accounts {
			$SYS {
				users [{user: "admin", password: "pwd"}]
			}
		}
	`))
	defer os.Remove(conf)
	s, o := RunServerWithConfig(conf)
	defer s.Shutdown()

	for _, test := range []struct {
		name    string
		usrInfo string
		ok      bool
		account string
	}{
		{"valid user/pwd", "admin:pwd@", true, "$SYS"},
		{"invalid pwd", "admin:wrong@", false, _EMPTY_},
		{"some token", "sometoken@", false, _EMPTY_},
		{"user used without pwd", "admin@", false, _EMPTY_}, // will be treated as a token
		{"user with empty password", "admin:@", false, _EMPTY_},
		{"no user means global account", _EMPTY_, true, globalAccountName},
	} {
		t.Run(test.name, func(t *testing.T) {
			url := fmt.Sprintf("nats://%s127.0.0.1:%d", test.usrInfo, o.Port)
			nc, err := nats.Connect(url)
			if err != nil {
				if test.ok {
					t.Fatalf("Unexpected error: %v", err)
				}
				return
			} else if !test.ok {
				nc.Close()
				t.Fatalf("Should have failed, did not")
			}
			var accName string
			s.mu.Lock()
			for _, c := range s.clients {
				c.mu.Lock()
				if c.acc != nil {
					accName = c.acc.Name
				}
				c.mu.Unlock()
				break
			}
			s.mu.Unlock()
			nc.Close()
			checkClientsCount(t, s, 0)
			if accName != test.account {
				t.Fatalf("The account should have been %q, got %q", test.account, accName)
			}
		})
	}
}

func TestServerAccountConns(t *testing.T) {
	// speed up hb
	orgHBInterval := eventsHBInterval
	eventsHBInterval = time.Millisecond * 100
	defer func() { eventsHBInterval = orgHBInterval }()
	conf := createConfFile(t, []byte(`
	   host: 127.0.0.1
	   port: -1
	   system_account: SYS
	   accounts: {
			   SYS: {users: [{user: s, password: s}]}
			   ACC: {users: [{user: a, password: a}]}
	   }`))
	s, _ := RunServerWithConfig(conf)
	defer s.Shutdown()

	nc := natsConnect(t, s.ClientURL(), nats.UserInfo("a", "a"))
	defer nc.Close()

	subOut, err := nc.SubscribeSync("foo")
	require_NoError(t, err)
	hw := "HELLO WORLD"
	nc.Publish("foo", []byte(hw))
	nc.Publish("bar", []byte(hw)) // will only count towards received
	nc.Flush()
	m, err := subOut.NextMsg(time.Second)
	require_NoError(t, err)
	require_Equal(t, string(m.Data), hw)

	ncs := natsConnect(t, s.ClientURL(), nats.UserInfo("s", "s"))
	defer ncs.Close()
	subs, err := ncs.SubscribeSync("$SYS.ACCOUNT.ACC.SERVER.CONNS")
	require_NoError(t, err)

	m, err = subs.NextMsg(time.Second)
	require_NoError(t, err)
	accConns := &AccountNumConns{}
	err = json.Unmarshal(m.Data, accConns)
	require_NoError(t, err)

	require_True(t, accConns.Received.Msgs == 2)
	require_True(t, accConns.Received.Bytes == 2*int64(len(hw)))
	require_True(t, accConns.Sent.Msgs == 1)
	require_True(t, accConns.Sent.Bytes == int64(len(hw)))
}

func TestServerEventsStatsZ(t *testing.T) {
	serverStatsReqSubj := "$SYS.REQ.SERVER.%s.STATSZ"
	preStart := time.Now().UTC()
	// Add little bit of delay to make sure that time check
	// between pre-start and actual start does not fail.
	time.Sleep(5 * time.Millisecond)
	sa, optsA, sb, _, akp := runTrustedCluster(t)
	defer sa.Shutdown()
	defer sb.Shutdown()
	// Same between actual start and post start.
	time.Sleep(5 * time.Millisecond)
	postStart := time.Now().UTC()

	url := fmt.Sprintf("nats://%s:%d", optsA.Host, optsA.Port)
	ncs, err := nats.Connect(url, createUserCreds(t, sa, akp))
	if err != nil {
		t.Fatalf("Error on connect: %v", err)
	}
	defer ncs.Close()

	subj := fmt.Sprintf(serverStatsSubj, sa.ID())
	sub, _ := ncs.SubscribeSync(subj)
	defer sub.Unsubscribe()
	ncs.Publish("foo", []byte("HELLO WORLD"))
	ncs.Flush()

	// Let's speed up the checking process.
	sa.mu.Lock()
	sa.sys.statsz = 10 * time.Millisecond
	sa.sys.stmr.Reset(sa.sys.statsz)
	sa.mu.Unlock()

	_, err = sub.NextMsg(time.Second)
	if err != nil {
		t.Fatalf("Error receiving msg: %v", err)
	}
	// Get it the second time so we can check some stats
	msg, err := sub.NextMsg(time.Second)
	if err != nil {
		t.Fatalf("Error receiving msg: %v", err)
	}
	m := ServerStatsMsg{}
	if err := json.Unmarshal(msg.Data, &m); err != nil {
		t.Fatalf("Error unmarshalling the statz json: %v", err)
	}
	if m.Server.ID != sa.ID() {
		t.Fatalf("Did not match IDs")
	}
	if m.Server.Cluster != "TEST_CLUSTER_22" {
		t.Fatalf("Did not match cluster name")
	}
	if m.Server.Version != VERSION {
		t.Fatalf("Did not match server version")
	}
	if !m.Stats.Start.After(preStart) && m.Stats.Start.Before(postStart) {
		t.Fatalf("Got a wrong start time for the server %v", m.Stats.Start)
	}
	if m.Stats.Connections != 1 {
		t.Fatalf("Did not match connections of 1, got %d", m.Stats.Connections)
	}
	if m.Stats.ActiveAccounts != 1 {
		t.Fatalf("Did not match active accounts of 1, got %d", m.Stats.ActiveAccounts)
	}
	if m.Stats.Sent.Msgs < 1 {
		t.Fatalf("Did not match sent msgs of >=1, got %d", m.Stats.Sent.Msgs)
	}
	if m.Stats.Received.Msgs < 1 {
		t.Fatalf("Did not match received msgs of >=1, got %d", m.Stats.Received.Msgs)
	}
	if m.Stats.ReceivedFromClients.Msgs < 1 {
		t.Fatalf("Did not match received from client msgs of >=1, got %d", m.Stats.ReceivedFromClients.Msgs)
	}
	if m.Stats.ReceivedFromClients.Bytes < 1 {
		t.Fatalf("Did not match received from client bytes of >=1, got %d", m.Stats.ReceivedFromClients.Bytes)
	}
	if m.Stats.SentToClients.Msgs < 1 {
		t.Fatalf("Did not match sent to client msgs of >= 1, got %d", m.Stats.SentToClients.Msgs)
	}
	if m.Stats.SentToClients.Bytes < 1 {
		t.Fatalf("Did not match sent to client bytes of >= 1, got %d", m.Stats.SentToClients.Bytes)
	}

	// Default pool size + 1 for system account
	expectedRoutes := DEFAULT_ROUTE_POOL_SIZE + 1
	if lr := len(m.Stats.Routes); lr != expectedRoutes {
		t.Fatalf("Expected %d routes, but got %d", expectedRoutes, lr)
	}

	// Now let's prompt this server to send us the statsz
	subj = fmt.Sprintf(serverStatsReqSubj, sa.ID())
	msg, err = ncs.Request(subj, nil, time.Second)
	if err != nil {
		t.Fatalf("Error trying to request statsz: %v", err)
	}
	m2 := ServerStatsMsg{}
	if err := json.Unmarshal(msg.Data, &m2); err != nil {
		t.Fatalf("Error unmarshalling the statz json: %v", err)
	}
	if m2.Server.ID != sa.ID() {
		t.Fatalf("Did not match IDs")
	}
	if m2.Stats.Connections != 1 {
		t.Fatalf("Did not match connections of 1, got %d", m2.Stats.Connections)
	}
	if m2.Stats.ActiveAccounts != 1 {
		t.Fatalf("Did not match active accounts of 1, got %d", m2.Stats.ActiveAccounts)
	}
	if m2.Stats.Sent.Msgs < 3 {
		t.Fatalf("Did not match sent msgs of >= 3, got %d", m2.Stats.Sent.Msgs)
	}
	if m2.Stats.Received.Msgs < 1 {
		t.Fatalf("Did not match received msgs of >= 1, got %d", m2.Stats.Received.Msgs)
	}
	if lr := len(m2.Stats.Routes); lr != expectedRoutes {
		t.Fatalf("Expected %d routes, but got %d", expectedRoutes, lr)
	}

	msg, err = ncs.Request(subj, nil, time.Second)
	if err != nil {
		t.Fatalf("Error trying to request statsz: %v", err)
	}
	m3 := ServerStatsMsg{}
	if err := json.Unmarshal(msg.Data, &m3); err != nil {
		t.Fatalf("Error unmarshalling the statz json: %v", err)
	}
	if m3.Server.ID != sa.ID() {
		t.Fatalf("Did not match IDs")
	}
	if m3.Stats.Connections != 1 {
		t.Fatalf("Did not match connections of 1, got %d", m3.Stats.Connections)
	}
	if m3.Stats.ActiveAccounts != 1 {
		t.Fatalf("Did not match active accounts of 1, got %d", m3.Stats.ActiveAccounts)
	}
	if m3.Stats.Sent.Msgs < 4 {
		t.Fatalf("Did not match sent msgs of >= 4, got %d", m3.Stats.Sent.Msgs)
	}
	if m3.Stats.Received.Msgs < 2 {
		t.Fatalf("Did not match received msgs of >= 2, got %d", m3.Stats.Received.Msgs)
	}
	if lr := len(m3.Stats.Routes); lr != expectedRoutes {
		t.Fatalf("Expected %d routes, but got %d", expectedRoutes, lr)
	}
	for _, sr := range m3.Stats.Routes {
		if sr.Name != "B_SRV" {
			t.Fatalf("Expected server A's route to B to have Name set to %q, got %q", "B", sr.Name)
		}
	}
	// Increment stalls to confirm they are reported.
	atomic.AddInt64(&sb.stalls, 3)

	// Now query B and check that route's name is "A"
	subj = fmt.Sprintf(serverStatsReqSubj, sb.ID())
	ncs.SubscribeSync(subj)
	msg, err = ncs.Request(subj, nil, time.Second)
	if err != nil {
		t.Fatalf("Error trying to request statsz: %v", err)
	}
	m = ServerStatsMsg{}
	if err := json.Unmarshal(msg.Data, &m); err != nil {
		t.Fatalf("Error unmarshalling the statz json: %v", err)
	}
	if lr := len(m.Stats.Routes); lr != expectedRoutes {
		t.Fatalf("Expected %d routes, but got %d", expectedRoutes, lr)
	}
	for _, sr := range m.Stats.Routes {
		if sr.Name != "A_SRV" {
			t.Fatalf("Expected server B's route to A to have Name set to %q, got %q", "A_SRV", sr.Name)
		}
	}
	require_Equal(t, m.Stats.StalledClients, 3)
}

func TestServerEventsStatsZForcedBroadcastNotRateLimited(t *testing.T) {
	// Tests will set this generally faster, but here we want the rate limiter
	// to actually be active.
	original := statszRateLimit
	statszRateLimit = time.Hour
	defer func() { statszRateLimit = original }()

	s, opts := runTrustedServer(t)
	defer s.Shutdown()

	acc, akp := createAccount(s)
	require_NoError(t, s.setSystemAccount(acc))

	url := fmt.Sprintf("nats://%s:%d", opts.Host, opts.Port)
	nc, err := nats.Connect(url, createUserCreds(t, s, akp))
	require_NoError(t, err)
	defer nc.Close()

	subj := fmt.Sprintf(serverStatsSubj, s.ID())

	// Arrange so that the rate limiter would suppress any broadcast:
	// stop the periodic heartbeat timer so it cannot produce a broadcast,
	// and set lastStatsz to "now" so we are well within the interval.
	s.mu.Lock()
	if s.sys.stmr != nil {
		s.sys.stmr.Stop()
	}
	s.sys.cstatsz = time.Hour
	s.sys.statsz = time.Hour
	s.sys.lastStatsz = time.Now()
	s.mu.Unlock()

	// Let any inflight startup statsz goroutines finish before we subscribe,
	// so they cannot land in our subscription as a false positive.
	time.Sleep(300 * time.Millisecond)

	// From here on the only thing that can publish to this subject is an
	// un-suppressed broadcast.
	sub, err := nc.SubscribeSync(subj)
	require_NoError(t, err)
	defer sub.Unsubscribe()
	require_NoError(t, nc.Flush())

	// Re-arm the suppression window right before the request.
	s.mu.Lock()
	s.sys.lastStatsz = time.Now()
	s.mu.Unlock()

	// Issue a forced (no-reply) STATSZ request. This is the path a new meta
	// leader uses to force an immediate broadcast. It must reset lastStatsz
	// and broadcast immediately even though we are within the rate-limit
	// interval.
	s.statszReq(nil, nil, nil, subj, _EMPTY_, nil, nil)

	// Make sure it is a statsz from our server.
	msg, err := sub.NextMsg(2 * time.Second)
	require_NoError(t, err)
	var m ServerStatsMsg
	require_NoError(t, json.Unmarshal(msg.Data, &m))
	require_Equal(t, m.Server.ID, s.ID())
	require_Equal(t, m.Stats.Connections, 1)
}

func TestServerEventsHealthZSingleServer(t *testing.T) {
	type healthzResp struct {
		Healthz HealthStatus `json:"data"`
		Server  ServerInfo   `json:"server"`
	}
	cfg := fmt.Sprintf(`listen: 127.0.0.1:-1

	jetstream: {max_mem_store: 256MB, max_file_store: 2GB, store_dir: '%s'}

	no_auth_user: one

	accounts {
		ONE { users = [ { user: "one", pass: "p" } ]; jetstream: enabled }
		$SYS { users = [ { user: "admin", pass: "s3cr3t!" } ] }
	}`, t.TempDir())

	serverHealthzReqSubj := "$SYS.REQ.SERVER.%s.HEALTHZ"
	s, _ := RunServerWithConfig(createConfFile(t, []byte(cfg)))
	defer s.Shutdown()

	ncs, err := nats.Connect(s.ClientURL(), nats.UserInfo("admin", "s3cr3t!"))
	if err != nil {
		t.Fatalf("Error connecting to cluster: %v", err)
	}

	defer ncs.Close()
	ncAcc, err := nats.Connect(s.ClientURL())
	if err != nil {
		t.Fatalf("Error on connect: %v", err)
	}
	defer ncAcc.Close()
	js, err := ncAcc.JetStream()
	if err != nil {
		t.Fatalf("Error creating JetStream context: %v", err)
	}
	_, err = js.AddStream(&nats.StreamConfig{
		Name:     "test",
		Subjects: []string{"foo"},
	})
	if err != nil {
		t.Fatalf("Error creating stream: %v", err)
	}
	_, err = js.AddConsumer("test", &nats.ConsumerConfig{
		Name: "cons",
	})
	if err != nil {
		t.Fatalf("Error creating consumer: %v", err)
	}

	subj := fmt.Sprintf(serverHealthzReqSubj, s.ID())

	tests := []struct {
		name     string
		req      *HealthzEventOptions
		expected HealthStatus
	}{
		{
			name:     "no parameters",
			expected: HealthStatus{Status: "ok", StatusCode: 200},
		},
		{
			name: "with js enabled only",
			req: &HealthzEventOptions{
				HealthzOptions: HealthzOptions{
					JSEnabledOnly: true,
				},
			},
			expected: HealthStatus{Status: "ok", StatusCode: 200},
		},
		{
			name: "with server only",
			req: &HealthzEventOptions{
				HealthzOptions: HealthzOptions{
					JSServerOnly: true,
				},
			},
			expected: HealthStatus{Status: "ok", StatusCode: 200},
		},
		{
			name: "with js meta only",
			req: &HealthzEventOptions{
				HealthzOptions: HealthzOptions{
					JSMetaOnly: true,
				},
			},
			expected: HealthStatus{Status: "ok", StatusCode: 200},
		},
		{
			name: "with account name",
			req: &HealthzEventOptions{
				HealthzOptions: HealthzOptions{
					Account: "ONE",
				},
			},
			expected: HealthStatus{Status: "ok", StatusCode: 200},
		},
		{
			name: "with account name and stream",
			req: &HealthzEventOptions{
				HealthzOptions: HealthzOptions{
					Account: "ONE",
					Stream:  "test",
				},
			},
			expected: HealthStatus{Status: "ok", StatusCode: 200},
		},
		{
			name: "with account name, stream and consumer",
			req: &HealthzEventOptions{
				HealthzOptions: HealthzOptions{
					Account:  "ONE",
					Stream:   "test",
					Consumer: "cons",
				},
			},
			expected: HealthStatus{Status: "ok", StatusCode: 200},
		},
		{
			name: "with stream only",
			req: &HealthzEventOptions{
				HealthzOptions: HealthzOptions{
					Stream: "test",
				},
			},
			expected: HealthStatus{
				Status:     "error",
				StatusCode: 400,
				Error:      `"account" must not be empty when checking stream health`,
			},
		},
		{
			name: "with stream only, detailed",
			req: &HealthzEventOptions{
				HealthzOptions: HealthzOptions{
					Details: true,
					Stream:  "test",
				},
			},
			expected: HealthStatus{
				Status:     "error",
				StatusCode: 400,
				Errors: []HealthzError{
					{
						Type:  HealthzErrorBadRequest,
						Error: `"account" must not be empty when checking stream health`,
					},
				},
			},
		},
		{
			name: "with account and consumer",
			req: &HealthzEventOptions{
				HealthzOptions: HealthzOptions{
					Account:  "ONE",
					Consumer: "cons",
				},
			},
			expected: HealthStatus{
				Status:     "error",
				StatusCode: 400,
				Error:      `"stream" must not be empty when checking consumer health`,
			},
		},
		{
			name: "with account and consumer, detailed",
			req: &HealthzEventOptions{
				HealthzOptions: HealthzOptions{
					Account:  "ONE",
					Consumer: "cons",
					Details:  true,
				},
			},
			expected: HealthStatus{
				Status:     "error",
				StatusCode: 400,
				Errors: []HealthzError{
					{
						Type:  HealthzErrorBadRequest,
						Error: `"stream" must not be empty when checking consumer health`,
					},
				},
			},
		},
		{
			name: "account not found",
			req: &HealthzEventOptions{
				HealthzOptions: HealthzOptions{
					Account: "abc",
				},
			},
			expected: HealthStatus{
				Status:     "unavailable",
				StatusCode: 404,
				Error:      `JetStream account "abc" not found`,
			},
		},
		{
			name: "account not found, detailed",
			req: &HealthzEventOptions{
				HealthzOptions: HealthzOptions{
					Account: "abc",
					Details: true,
				},
			},
			expected: HealthStatus{
				Status:     "error",
				StatusCode: 404,
				Errors: []HealthzError{
					{
						Type:    HealthzErrorAccount,
						Account: "abc",
						Error:   `JetStream account "abc" not found`,
					},
				},
			},
		},
		{
			name: "stream not found",
			req: &HealthzEventOptions{
				HealthzOptions: HealthzOptions{
					Account: "ONE",
					Stream:  "abc",
				},
			},
			expected: HealthStatus{
				Status:     "unavailable",
				StatusCode: 404,
				Error:      `JetStream stream "abc" not found on account "ONE"`,
			},
		},
		{
			name: "stream not found, detailed",
			req: &HealthzEventOptions{
				HealthzOptions: HealthzOptions{
					Account: "ONE",
					Stream:  "abc",
					Details: true,
				},
			},
			expected: HealthStatus{
				Status:     "error",
				StatusCode: 404,
				Errors: []HealthzError{
					{
						Type:    HealthzErrorStream,
						Account: "ONE",
						Stream:  "abc",
						Error:   `JetStream stream "abc" not found on account "ONE"`,
					},
				},
			},
		},
		{
			name: "consumer not found",
			req: &HealthzEventOptions{
				HealthzOptions: HealthzOptions{
					Account:  "ONE",
					Stream:   "test",
					Consumer: "abc",
				},
			},
			expected: HealthStatus{
				Status:     "unavailable",
				StatusCode: 404,
				Error:      `JetStream consumer "abc" not found for stream "test" on account "ONE"`,
			},
		},
		{
			name: "consumer not found, detailed",
			req: &HealthzEventOptions{
				HealthzOptions: HealthzOptions{
					Account:  "ONE",
					Stream:   "test",
					Consumer: "abc",
					Details:  true,
				},
			},
			expected: HealthStatus{
				Status:     "error",
				StatusCode: 404,
				Errors: []HealthzError{
					{
						Type:     HealthzErrorConsumer,
						Account:  "ONE",
						Stream:   "test",
						Consumer: "abc",
						Error:    `JetStream consumer "abc" not found for stream "test" on account "ONE"`,
					},
				},
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			var body []byte
			var err error
			if test.req != nil {
				body, err = json.Marshal(test.req)
				if err != nil {
					t.Fatalf("Error marshaling request body: %v", err)
				}
			}
			msg, err := ncs.Request(subj, body, 1*time.Second)
			if err != nil {
				t.Fatalf("Error trying to request healthz: %v", err)
			}
			var health healthzResp
			if err := json.Unmarshal(msg.Data, &health); err != nil {
				t.Fatalf("Error unmarshalling the statz json: %v", err)
			}
			if !reflect.DeepEqual(health.Healthz, test.expected) {
				t.Errorf("Invalid healthz status; want: %+v; got: %+v", test.expected, health.Healthz)
			}
		})
	}
}

func TestServerEventsHealthZClustered(t *testing.T) {
	type healthzResp struct {
		Healthz HealthStatus `json:"data"`
		Server  ServerInfo   `json:"server"`
	}
	serverHealthzReqSubj := "$SYS.REQ.SERVER.%s.HEALTHZ"
	c := createJetStreamClusterWithTemplate(t, jsClusterAccountsTempl, "JSC", 3)
	defer c.shutdown()

	ncs, err := nats.Connect(c.randomServer().ClientURL(), nats.UserInfo("admin", "s3cr3t!"))
	if err != nil {
		t.Fatalf("Error connecting to cluster: %v", err)
	}

	defer ncs.Close()
	ncAcc, err := nats.Connect(c.randomServer().ClientURL())
	if err != nil {
		t.Fatalf("Error on connect: %v", err)
	}
	defer ncAcc.Close()
	js, err := ncAcc.JetStream()
	if err != nil {
		t.Fatalf("Error creating JetStream context: %v", err)
	}
	_, err = js.AddStream(&nats.StreamConfig{
		Name:     "test",
		Subjects: []string{"foo"},
		Replicas: 3,
	})
	if err != nil {
		t.Fatalf("Error creating stream: %v", err)
	}
	_, err = js.AddConsumer("test", &nats.ConsumerConfig{
		Name:     "cons",
		Replicas: 3,
	})
	if err != nil {
		t.Fatalf("Error creating consumer: %v", err)
	}

	c.waitOnStreamLeader("ONE", "test")
	c.waitOnConsumerLeader("ONE", "test", "cons")
	c.waitOnAllCurrent()

	subj := fmt.Sprintf(serverHealthzReqSubj, c.servers[0].ID())
	pingSubj := fmt.Sprintf(serverHealthzReqSubj, "PING")

	tests := []struct {
		name          string
		req           *HealthzEventOptions
		expected      HealthStatus
		expectedError string
	}{
		{
			name:     "no parameters",
			expected: HealthStatus{Status: "ok", StatusCode: 200},
		},
		{
			name: "with js enabled only",
			req: &HealthzEventOptions{
				HealthzOptions: HealthzOptions{
					JSEnabledOnly: true,
				},
			},
			expected: HealthStatus{Status: "ok", StatusCode: 200},
		},
		{
			name: "with server only",
			req: &HealthzEventOptions{
				HealthzOptions: HealthzOptions{
					JSServerOnly: true,
				},
			},
			expected: HealthStatus{Status: "ok", StatusCode: 200},
		},
		{
			name: "with account name",
			req: &HealthzEventOptions{
				HealthzOptions: HealthzOptions{
					Account: "ONE",
				},
			},
			expected: HealthStatus{Status: "ok", StatusCode: 200},
		},
		{
			name: "with account name and stream",
			req: &HealthzEventOptions{
				HealthzOptions: HealthzOptions{
					Account: "ONE",
					Stream:  "test",
				},
			},
			expected: HealthStatus{Status: "ok", StatusCode: 200},
		},
		{
			name: "with account name, stream and consumer",
			req: &HealthzEventOptions{
				HealthzOptions: HealthzOptions{
					Account:  "ONE",
					Stream:   "test",
					Consumer: "cons",
				},
			},
			expected: HealthStatus{Status: "ok", StatusCode: 200},
		},
		{
			name: "with stream only",
			req: &HealthzEventOptions{
				HealthzOptions: HealthzOptions{
					Stream: "test",
				},
			},
			expected: HealthStatus{
				Status:     "error",
				StatusCode: 400,
				Error:      `"account" must not be empty when checking stream health`,
			},
			expectedError: "Bad request:",
		},
		{
			name: "with stream only, detailed",
			req: &HealthzEventOptions{
				HealthzOptions: HealthzOptions{
					Details: true,
					Stream:  "test",
				},
			},
			expected: HealthStatus{
				Status:     "error",
				StatusCode: 400,
				Errors: []HealthzError{
					{
						Type:  HealthzErrorBadRequest,
						Error: `"account" must not be empty when checking stream health`,
					},
				},
			},
		},
		{
			name: "account not found",
			req: &HealthzEventOptions{
				HealthzOptions: HealthzOptions{
					Account: "abc",
				},
			},
			expected: HealthStatus{
				Status:     "unavailable",
				StatusCode: 404,
				Error:      `JetStream account "abc" not found`,
			},
			expectedError: `account "abc" not found`,
		},
		{
			name: "account not found, detailed",
			req: &HealthzEventOptions{
				HealthzOptions: HealthzOptions{
					Account: "abc",
					Details: true,
				},
			},
			expected: HealthStatus{
				Status:     "error",
				StatusCode: 404,
				Errors: []HealthzError{
					{
						Type:    HealthzErrorAccount,
						Account: "abc",
						Error:   `JetStream account "abc" not found`,
					},
				},
			},
		},
		{
			name: "stream not found",
			req: &HealthzEventOptions{
				HealthzOptions: HealthzOptions{
					Account: "ONE",
					Stream:  "abc",
				},
			},
			expected: HealthStatus{
				Status:     "unavailable",
				StatusCode: 404,
				Error:      `JetStream stream "abc" not found on account "ONE"`,
			},
			expectedError: `stream "abc" not found`,
		},
		{
			name: "stream not found, detailed",
			req: &HealthzEventOptions{
				HealthzOptions: HealthzOptions{
					Account: "ONE",
					Stream:  "abc",
					Details: true,
				},
			},
			expected: HealthStatus{
				Status:     "error",
				StatusCode: 404,
				Errors: []HealthzError{
					{
						Type:    HealthzErrorStream,
						Account: "ONE",
						Stream:  "abc",
						Error:   `JetStream stream "abc" not found on account "ONE"`,
					},
				},
			},
		},
		{
			name: "consumer not found",
			req: &HealthzEventOptions{
				HealthzOptions: HealthzOptions{
					Account:  "ONE",
					Stream:   "test",
					Consumer: "abc",
				},
			},
			expected: HealthStatus{
				Status:     "unavailable",
				StatusCode: 404,
				Error:      `JetStream consumer "abc" not found for stream "test" on account "ONE"`,
			},
			expectedError: `consumer "abc" not found for stream "test"`,
		},
		{
			name: "consumer not found, detailed",
			req: &HealthzEventOptions{
				HealthzOptions: HealthzOptions{
					Account:  "ONE",
					Stream:   "test",
					Consumer: "abc",
					Details:  true,
				},
			},
			expected: HealthStatus{
				Status:     "error",
				StatusCode: 404,
				Errors: []HealthzError{
					{
						Type:     HealthzErrorConsumer,
						Account:  "ONE",
						Stream:   "test",
						Consumer: "abc",
						Error:    `JetStream consumer "abc" not found for stream "test" on account "ONE"`,
					},
				},
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			var body []byte
			var err error
			if test.req != nil {
				body, err = json.Marshal(test.req)
				if err != nil {
					t.Fatalf("Error marshaling request body: %v", err)
				}
			}
			msg, err := ncs.Request(subj, body, 1*time.Second)
			if err != nil {
				t.Fatalf("Error trying to request healthz: %v", err)
			}
			var health healthzResp
			if err := json.Unmarshal(msg.Data, &health); err != nil {
				t.Fatalf("Error unmarshalling the statz json: %v", err)
			}
			if !reflect.DeepEqual(health.Healthz, test.expected) {
				t.Errorf("Invalid healthz status; want: %+v; got: %+v", test.expected, health.Healthz)
			}

			reply := ncs.NewRespInbox()
			sub, err := ncs.SubscribeSync(reply)
			if err != nil {
				t.Fatalf("Error creating subscription: %v", err)
			}
			defer sub.Unsubscribe()

			// now PING all servers
			if err := ncs.PublishRequest(pingSubj, reply, body); err != nil {
				t.Fatalf("Publish error: %v", err)
			}
			for i := 0; i < 3; i++ {
				msg, err := sub.NextMsg(1 * time.Second)
				if err != nil {
					t.Fatalf("Error fetching healthz PING response: %v", err)
				}
				var health healthzResp
				if err := json.Unmarshal(msg.Data, &health); err != nil {
					t.Fatalf("Error unmarshalling the statz json: %v", err)
				}
				if !reflect.DeepEqual(health.Healthz, test.expected) {
					t.Errorf("Invalid healthz status; want: %+v; got: %+v", test.expected, health.Healthz)
				}
			}
			if _, err := sub.NextMsg(50 * time.Millisecond); !errors.Is(err, nats.ErrTimeout) {
				t.Fatalf("Expected timeout error; got: %v", err)
			}
		})
	}
}

func TestServerEventsHealthZClustered_NoReplicas(t *testing.T) {
	type healthzResp struct {
		Healthz HealthStatus `json:"data"`
		Server  ServerInfo   `json:"server"`
	}
	serverHealthzReqSubj := "$SYS.REQ.SERVER.%s.HEALTHZ"
	c := createJetStreamClusterWithTemplate(t, jsClusterAccountsTempl, "JSC", 3)
	defer c.shutdown()

	ncs, err := nats.Connect(c.randomServer().ClientURL(), nats.UserInfo("admin", "s3cr3t!"))
	if err != nil {
		t.Fatalf("Error connecting to cluster: %v", err)
	}

	defer ncs.Close()
	ncAcc, err := nats.Connect(c.randomServer().ClientURL())
	if err != nil {
		t.Fatalf("Error on connect: %v", err)
	}
	defer ncAcc.Close()
	js, err := ncAcc.JetStream()
	if err != nil {
		t.Fatalf("Error creating JetStream context: %v", err)
	}

	pingSubj := fmt.Sprintf(serverHealthzReqSubj, "PING")

	t.Run("non-replicated stream", func(t *testing.T) {
		_, err = js.AddStream(&nats.StreamConfig{
			Name:     "test",
			Subjects: []string{"foo"},
		})
		if err != nil {
			t.Fatalf("Error creating stream: %v", err)
		}
		_, err = js.AddConsumer("test", &nats.ConsumerConfig{
			Name: "cons",
		})
		if err != nil {
			t.Fatalf("Error creating consumer: %v", err)
		}
		body, err := json.Marshal(HealthzEventOptions{
			HealthzOptions: HealthzOptions{
				Account: "ONE",
				Stream:  "test",
			},
		})
		if err != nil {
			t.Fatalf("Error marshaling request body: %v", err)
		}

		reply := ncs.NewRespInbox()
		sub, err := ncs.SubscribeSync(reply)
		if err != nil {
			t.Fatalf("Error creating subscription: %v", err)
		}
		defer sub.Unsubscribe()

		// now PING all servers
		if err := ncs.PublishRequest(pingSubj, reply, body); err != nil {
			t.Fatalf("Publish error: %v", err)
		}
		var healthy int
		for i := 0; i < 3; i++ {
			msg, err := sub.NextMsg(1 * time.Second)
			if err != nil {
				t.Fatalf("Error fetching healthz PING response: %v", err)
			}
			var health healthzResp
			if err := json.Unmarshal(msg.Data, &health); err != nil {
				t.Fatalf("Error unmarshalling the statz json: %v", err)
			}
			if health.Healthz.Status == "ok" {
				healthy++
				continue
			}
			if !strings.Contains(health.Healthz.Error, `stream "test" not found`) {
				t.Errorf("Expected error to contain: %q, got: %s", `stream "test" not found`, health.Healthz.Error)
			}
		}
		if healthy != 1 {
			t.Fatalf("Expected 1 healthy server; got: %d", healthy)
		}
		if _, err := sub.NextMsg(50 * time.Millisecond); !errors.Is(err, nats.ErrTimeout) {
			t.Fatalf("Expected timeout error; got: %v", err)
		}
	})

	t.Run("non-replicated consumer", func(t *testing.T) {
		_, err = js.AddStream(&nats.StreamConfig{
			Name:     "test-repl",
			Subjects: []string{"bar"},
			Replicas: 3,
		})
		if err != nil {
			t.Fatalf("Error creating stream: %v", err)
		}
		_, err = js.AddConsumer("test-repl", &nats.ConsumerConfig{
			Name: "cons-single",
		})
		if err != nil {
			t.Fatalf("Error creating consumer: %v", err)
		}
		body, err := json.Marshal(HealthzEventOptions{
			HealthzOptions: HealthzOptions{
				Account:  "ONE",
				Stream:   "test-repl",
				Consumer: "cons-single",
			},
		})
		if err != nil {
			t.Fatalf("Error marshaling request body: %v", err)
		}

		reply := ncs.NewRespInbox()
		sub, err := ncs.SubscribeSync(reply)
		if err != nil {
			t.Fatalf("Error creating subscription: %v", err)
		}
		defer sub.Unsubscribe()

		// now PING all servers
		if err := ncs.PublishRequest(pingSubj, reply, body); err != nil {
			t.Fatalf("Publish error: %v", err)
		}
		var healthy int
		for i := 0; i < 3; i++ {
			msg, err := sub.NextMsg(1 * time.Second)
			if err != nil {
				t.Fatalf("Error fetching healthz PING response: %v", err)
			}
			var health healthzResp
			if err := json.Unmarshal(msg.Data, &health); err != nil {
				t.Fatalf("Error unmarshalling the statz json: %v", err)
			}
			if health.Healthz.Status == "ok" {
				healthy++
				continue
			}
			if !strings.Contains(health.Healthz.Error, `consumer "cons-single" not found`) {
				t.Errorf("Expected error to contain: %q, got: %s", `consumer "cons-single" not found`, health.Healthz.Error)
			}
		}
		if healthy != 1 {
			t.Fatalf("Expected 1 healthy server; got: %d", healthy)
		}
		if _, err := sub.NextMsg(50 * time.Millisecond); !errors.Is(err, nats.ErrTimeout) {
			t.Fatalf("Expected timeout error; got: %v", err)
		}
	})

}

func TestServerEventsHealthZJetStreamNotEnabled(t *testing.T) {
	type healthzResp struct {
		Healthz HealthStatus `json:"data"`
		Server  ServerInfo   `json:"server"`
	}
	cfg := `listen: 127.0.0.1:-1

	accounts {
		$SYS { users = [ { user: "admin", pass: "s3cr3t!" } ] }
	}`

	serverHealthzReqSubj := "$SYS.REQ.SERVER.%s.HEALTHZ"
	s, _ := RunServerWithConfig(createConfFile(t, []byte(cfg)))
	defer s.Shutdown()

	ncs, err := nats.Connect(s.ClientURL(), nats.UserInfo("admin", "s3cr3t!"))
	if err != nil {
		t.Fatalf("Error connecting to cluster: %v", err)
	}

	defer ncs.Close()

	subj := fmt.Sprintf(serverHealthzReqSubj, s.ID())

	msg, err := ncs.Request(subj, nil, 1*time.Second)
	if err != nil {
		t.Fatalf("Error trying to request healthz: %v", err)
	}
	var health healthzResp
	if err := json.Unmarshal(msg.Data, &health); err != nil {
		t.Fatalf("Error unmarshalling the statz json: %v", err)
	}
	if health.Healthz.Status != "ok" {
		t.Errorf("Invalid healthz status; want: %q; got: %q", "ok", health.Healthz.Status)
	}
	if health.Healthz.Error != "" {
		t.Errorf("HealthZ error: %s", health.Healthz.Error)
	}
}

func TestServerEventsPingStatsZ(t *testing.T) {
	sa, _, sb, optsB, akp := runTrustedCluster(t)
	defer sa.Shutdown()
	defer sb.Shutdown()
	url := fmt.Sprintf("nats://%s:%d", optsB.Host, optsB.Port)
	nc, err := nats.Connect(url, createUserCreds(t, sb, akp))
	if err != nil {
		t.Fatalf("Error on connect: %v", err)
	}
	defer nc.Close()
	test := func(req []byte) {
		reply := nc.NewRespInbox()
		sub, _ := nc.SubscribeSync(reply)
		nc.PublishRequest(serverStatsPingReqSubj, reply, req)
		// Make sure its a statsz
		m := ServerStatsMsg{}
		// Receive both manually.
		msg, err := sub.NextMsg(time.Second)
		if err != nil {
			t.Fatalf("Error receiving msg: %v", err)
		}
		if err := json.Unmarshal(msg.Data, &m); err != nil {
			t.Fatalf("Error unmarshalling the statz json: %v", err)
		}
		msg, err = sub.NextMsg(time.Second)
		if err != nil {
			t.Fatalf("Error receiving msg: %v", err)
		}
		if err := json.Unmarshal(msg.Data, &m); err != nil {
			t.Fatalf("Error unmarshalling the statz json: %v", err)
		}
	}
	strRequestTbl := []string{
		`{"cluster":"TEST"}`,
		`{"cluster":"CLUSTER"}`,
		`{"server_name":"SRV"}`,
		`{"server_name":"_"}`,
		fmt.Sprintf(`{"host":"%s"}`, optsB.Host),
		fmt.Sprintf(`{"host":"%s", "cluster":"CLUSTER", "name":"SRV"}`, optsB.Host),
	}
	for i, opt := range strRequestTbl {
		t.Run(fmt.Sprintf("%s-%d", t.Name(), i), func(t *testing.T) {
			test([]byte(opt))
		})
	}
	requestTbl := []StatszEventOptions{
		{EventFilterOptions: EventFilterOptions{Cluster: "TEST"}},
		{EventFilterOptions: EventFilterOptions{Cluster: "CLUSTER"}},
		{EventFilterOptions: EventFilterOptions{Name: "SRV"}},
		{EventFilterOptions: EventFilterOptions{Name: "_"}},
		{EventFilterOptions: EventFilterOptions{Host: optsB.Host}},
		{EventFilterOptions: EventFilterOptions{Host: optsB.Host, Cluster: "CLUSTER", Name: "SRV"}},
	}
	for i, opt := range requestTbl {
		t.Run(fmt.Sprintf("%s-%d", t.Name(), i), func(t *testing.T) {
			msg, _ := json.MarshalIndent(&opt, "", "  ")
			test(msg)
		})
	}
}

func TestServerEventsPingStatsZDedicatedRecvQ(t *testing.T) {
	sa, _, sb, optsB, akp := runTrustedCluster(t)
	defer sa.Shutdown()
	defer sb.Shutdown()
	url := fmt.Sprintf("nats://%s:%d", optsB.Host, optsB.Port)
	nc, err := nats.Connect(url, createUserCreds(t, sb, akp))
	require_NoError(t, err)
	defer nc.Close()
	// We need to wait a little bit for the $SYS.SERVER.ACCOUNT.%s.CONNS
	// event to be pushed in the mux'ed queue.
	time.Sleep(300 * time.Millisecond)

	testReq := func(t *testing.T, subj string, expectTwo bool) {
		for _, s := range []*Server{sa, sb} {
			s.mu.RLock()
			recvq := s.sys.recvq
			s.mu.RUnlock()
			recvq.Lock()
			defer recvq.Unlock()
		}
		reply := nc.NewRespInbox()
		sub := natsSubSync(t, nc, reply)
		nc.PublishRequest(subj, reply, nil)
		msg := natsNexMsg(t, sub, time.Second)
		if len(msg.Data) == 0 {
			t.Fatal("Unexpected empty response")
		}
		// Make sure its a statsz
		m := ServerStatsMsg{}
		err := json.Unmarshal(msg.Data, &m)
		require_NoError(t, err)
		require_False(t, m.Stats.Start.IsZero())
		if expectTwo {
			msg = natsNexMsg(t, sub, time.Second)
			err = json.Unmarshal(msg.Data, &m)
			require_NoError(t, err)
			require_False(t, m.Stats.Start.IsZero())
		}
	}
	const statsz = "STATSZ"
	for _, test := range []struct {
		name      string
		f         func() string
		expectTwo bool
	}{
		{"server stats ping request subject", func() string { return serverStatsPingReqSubj }, true},
		{"server ping request subject", func() string { return fmt.Sprintf(serverPingReqSubj, statsz) }, true},
		{"server a direct request subject", func() string { return fmt.Sprintf(serverDirectReqSubj, sa.ID(), statsz) }, false},
		{"server b direct request subject", func() string { return fmt.Sprintf(serverDirectReqSubj, sb.ID(), statsz) }, false},
	} {
		t.Run(test.name, func(t *testing.T) {
			testReq(t, test.f(), test.expectTwo)
		})
	}
}

func TestServerEventsPingStatsZFilter(t *testing.T) {
	sa, _, sb, optsB, akp := runTrustedCluster(t)
	defer sa.Shutdown()
	defer sb.Shutdown()

	url := fmt.Sprintf("nats://%s:%d", optsB.Host, optsB.Port)
	nc, err := nats.Connect(url, createUserCreds(t, sb, akp))
	if err != nil {
		t.Fatalf("Error on connect: %v", err)
	}
	defer nc.Close()

	requestTbl := []string{
		`{"cluster":"DOESNOTEXIST"}`,
		`{"host":"DOESNOTEXIST"}`,
		`{"server_name":"DOESNOTEXIST"}`,
	}
	for i, msg := range requestTbl {
		t.Run(fmt.Sprintf("%s-%d", t.Name(), i), func(t *testing.T) {
			// Receive both manually.
			if _, err := nc.Request(serverStatsPingReqSubj, []byte(msg), time.Second/4); err != nats.ErrTimeout {
				t.Fatalf("Error, expected timeout: %v", err)
			}
		})
	}
	requestObjTbl := []EventFilterOptions{
		{Cluster: "DOESNOTEXIST"},
		{Host: "DOESNOTEXIST"},
		{Name: "DOESNOTEXIST"},
	}
	for i, opt := range requestObjTbl {
		t.Run(fmt.Sprintf("%s-%d", t.Name(), i), func(t *testing.T) {
			msg, _ := json.MarshalIndent(&opt, "", "  ")
			// Receive both manually.
			if _, err := nc.Request(serverStatsPingReqSubj, []byte(msg), time.Second/4); err != nats.ErrTimeout {
				t.Fatalf("Error, expected timeout: %v", err)
			}
		})
	}
}

func TestServerEventsPingStatsZFailFilter(t *testing.T) {
	sa, _, sb, optsB, akp := runTrustedCluster(t)
	defer sa.Shutdown()
	defer sb.Shutdown()

	url := fmt.Sprintf("nats://%s:%d", optsB.Host, optsB.Port)
	nc, err := nats.Connect(url, createUserCreds(t, sb, akp))
	if err != nil {
		t.Fatalf("Error on connect: %v", err)
	}
	defer nc.Close()

	// Receive both manually.
	if msg, err := nc.Request(serverStatsPingReqSubj, []byte(`{MALFORMEDJSON`), time.Second/4); err != nil {
		t.Fatalf("Error: %v", err)
	} else {
		resp := make(map[string]map[string]any)
		if err := json.Unmarshal(msg.Data, &resp); err != nil {
			t.Fatalf("Error unmarshalling the response json: %v", err)
		}
		if resp["error"]["code"].(float64) != http.StatusBadRequest {
			t.Fatal("bad error code")
		}
	}
}

func TestServerEventsPingMonitorz(t *testing.T) {
	sa, _, sb, optsB, akp := runTrustedCluster(t)
	defer sa.Shutdown()
	defer sb.Shutdown()
	sysAcc, _ := akp.PublicKey()
	url := fmt.Sprintf("nats://%s:%d", optsB.Host, optsB.Port)
	nc, err := nats.Connect(url, createUserCreds(t, sb, akp))
	if err != nil {
		t.Fatalf("Error on connect: %v", err)
	}
	defer nc.Close()

	nc.Flush()

	tests := []struct {
		endpoint  string
		opt       any
		resp      any
		respField []string
	}{
		{"VARZ", nil, &Varz{},
			[]string{"now", "cpu", "system_account"}},
		{"SUBSZ", nil, &Subsz{},
			[]string{"num_subscriptions", "num_cache"}},
		{"CONNZ", nil, &Connz{},
			[]string{"now", "connections"}},
		{"ROUTEZ", nil, &Routez{},
			[]string{"now", "routes"}},
		{"GATEWAYZ", nil, &Gatewayz{},
			[]string{"now", "outbound_gateways", "inbound_gateways"}},
		{"LEAFZ", nil, &Leafz{},
			[]string{"now", "leafs"}},

		{"SUBSZ", &SubszOptions{}, &Subsz{},
			[]string{"num_subscriptions", "num_cache"}},
		{"CONNZ", &ConnzOptions{}, &Connz{},
			[]string{"now", "connections"}},
		{"ROUTEZ", &RoutezOptions{}, &Routez{},
			[]string{"now", "routes"}},
		{"GATEWAYZ", &GatewayzOptions{}, &Gatewayz{},
			[]string{"now", "outbound_gateways", "inbound_gateways"}},
		{"LEAFZ", &LeafzOptions{}, &Leafz{},
			[]string{"now", "leafs"}},
		{"ACCOUNTZ", &AccountzOptions{}, &Accountz{},
			[]string{"now", "accounts"}},

		{"SUBSZ", &SubszOptions{Limit: 5}, &Subsz{},
			[]string{"num_subscriptions", "num_cache"}},
		{"CONNZ", &ConnzOptions{Limit: 5}, &Connz{},
			[]string{"now", "connections"}},
		{"ROUTEZ", &RoutezOptions{SubscriptionsDetail: true}, &Routez{},
			[]string{"now", "routes"}},
		{"GATEWAYZ", &GatewayzOptions{Accounts: true}, &Gatewayz{},
			[]string{"now", "outbound_gateways", "inbound_gateways"}},
		{"LEAFZ", &LeafzOptions{Subscriptions: true}, &Leafz{},
			[]string{"now", "leafs"}},
		{"ACCOUNTZ", &AccountzOptions{Account: sysAcc}, &Accountz{},
			[]string{"now", "account_detail"}},
		{"LEAFZ", &LeafzOptions{Account: sysAcc}, &Leafz{},
			[]string{"now", "leafs"}},

		{"ROUTEZ", json.RawMessage(`{"cluster":""}`), &Routez{},
			[]string{"now", "routes"}},
		{"ROUTEZ", json.RawMessage(`{"name":""}`), &Routez{},
			[]string{"now", "routes"}},
		{"ROUTEZ", json.RawMessage(`{"cluster":"TEST_CLUSTER_22"}`), &Routez{},
			[]string{"now", "routes"}},
		{"ROUTEZ", json.RawMessage(`{"cluster":"CLUSTER"}`), &Routez{},
			[]string{"now", "routes"}},
		{"ROUTEZ", json.RawMessage(`{"cluster":"TEST_CLUSTER_22", "subscriptions":true}`), &Routez{},
			[]string{"now", "routes"}},

		{"JSZ", nil, &JSzOptions{}, []string{"now", "disabled"}},

		{"HEALTHZ", nil, &JSzOptions{}, []string{"status"}},
		{"HEALTHZ", &HealthzOptions{JSEnabledOnly: true}, &JSzOptions{}, []string{"status"}},
		{"HEALTHZ", &HealthzOptions{JSServerOnly: true}, &JSzOptions{}, []string{"status"}},
		{"HEALTHZ", &HealthzOptions{JSMetaOnly: true}, &JSzOptions{}, []string{"status"}},
		{"EXPVARZ", nil, &ExpvarzStatus{}, []string{"memstats", "cmdline"}},
	}

	for i, test := range tests {
		t.Run(fmt.Sprintf("%s-%d", test.endpoint, i), func(t *testing.T) {
			var opt []byte
			if test.opt != nil {
				opt, err = json.Marshal(test.opt)
				if err != nil {
					t.Fatalf("Error marshaling opts: %v", err)
				}
			}
			reply := nc.NewRespInbox()
			replySubj, _ := nc.SubscribeSync(reply)

			// set a header to make sure request parsing knows to ignore them
			nc.PublishMsg(&nats.Msg{
				Subject: fmt.Sprintf("%s.%s", serverStatsPingReqSubj, test.endpoint),
				Reply:   reply,
				Header:  nats.Header{"header": []string{"for header sake"}},
				Data:    opt,
			})

			// Receive both manually.
			msg, err := replySubj.NextMsg(time.Second)
			if err != nil {
				t.Fatalf("Error receiving msg: %v", err)
			}
			response1 := make(map[string]map[string]any)

			if err := json.Unmarshal(msg.Data, &response1); err != nil {
				t.Fatalf("Error unmarshalling response1 json: %v", err)
			}

			serverName := ""
			if response1["server"]["name"] == "A_SRV" {
				serverName = "B_SRV"
			} else if response1["server"]["name"] == "B_SRV" {
				serverName = "A_SRV"
			} else {
				t.Fatalf("Error finding server in %s", string(msg.Data))
			}
			if resp, ok := response1["data"]; !ok {
				t.Fatalf("Error finding: %s in %s",
					strings.ToLower(test.endpoint), string(msg.Data))
			} else {
				for _, respField := range test.respField {
					if _, ok := resp[respField]; !ok {
						t.Fatalf("Error finding: %s in %s", respField, resp)
					}
				}
			}

			msg, err = replySubj.NextMsg(time.Second)
			if err != nil {
				t.Fatalf("Error receiving msg: %v", err)
			}
			response2 := make(map[string]map[string]any)
			if err := json.Unmarshal(msg.Data, &response2); err != nil {
				t.Fatalf("Error unmarshalling the response2 json: %v", err)
			}
			if response2["server"]["name"] != serverName {
				t.Fatalf("Error finding server %s in %s", serverName, string(msg.Data))
			}
			if resp, ok := response2["data"]; !ok {
				t.Fatalf("Error finding: %s in %s",
					strings.ToLower(test.endpoint), string(msg.Data))
			} else {
				for _, respField := range test.respField {
					if val, ok := resp[respField]; !ok {
						t.Fatalf("Error finding: %s in %s", respField, resp)
					} else if val == nil {
						t.Fatalf("Nil value found: %s in %s", respField, resp)
					}
				}
			}
		})
	}
}

func TestGatewayNameClientInfo(t *testing.T) {
	sa, _, sb, _, _ := runTrustedCluster(t)
	defer sa.Shutdown()
	defer sb.Shutdown()

	c, _, l := newClientForServer(sa)
	defer c.close()

	var info Info
	err := json.Unmarshal([]byte(l[5:]), &info)
	if err != nil {
		t.Fatalf("Could not parse INFO json: %v\n", err)
	}
	if info.Cluster != "TEST_CLUSTER_22" {
		t.Fatalf("Expected a cluster name of 'TEST_CLUSTER_22', got %q", info.Cluster)
	}
}

type slowAccResolver struct {
	sync.Mutex
	AccountResolver
	acc string
}

func (sr *slowAccResolver) Fetch(name string) (string, error) {
	sr.Lock()
	delay := sr.acc == name
	sr.Unlock()
	if delay {
		time.Sleep(200 * time.Millisecond)
	}
	return sr.AccountResolver.Fetch(name)
}

func TestConnectionUpdatesTimerProperlySet(t *testing.T) {
	origEventsHBInterval := eventsHBInterval
	eventsHBInterval = 50 * time.Millisecond
	defer func() { eventsHBInterval = origEventsHBInterval }()

	sa, _, sb, optsB, _ := runTrustedCluster(t)
	defer sa.Shutdown()
	defer sb.Shutdown()

	// Normal Account
	okp, _ := nkeys.FromSeed(oSeed)
	akp, _ := nkeys.CreateAccount()
	pub, _ := akp.PublicKey()
	nac := jwt.NewAccountClaims(pub)
	nac.Limits.Conn = 10 // set any limit...
	jwt, _ := nac.Encode(okp)

	addAccountToMemResolver(sa, pub, jwt)

	// Listen for HB updates...
	count := int32(0)
	cb := func(sub *subscription, _ *client, _ *Account, subject, reply string, msg []byte) {
		atomic.AddInt32(&count, 1)
	}
	subj := fmt.Sprintf(accConnsEventSubjNew, pub)
	sub, err := sa.sysSubscribe(subj, cb)
	if sub == nil || err != nil {
		t.Fatalf("Expected to subscribe, got %v", err)
	}
	defer sa.sysUnsubscribe(sub)

	url := fmt.Sprintf("nats://%s:%d", optsB.Host, optsB.Port)
	nc := natsConnect(t, url, createUserCreds(t, sb, akp))
	defer nc.Close()

	time.Sleep(500 * time.Millisecond)
	// After waiting 500ms with HB interval of 50ms, we should get
	// about 10 updates, no much more
	if n := atomic.LoadInt32(&count); n > 15 {
		t.Fatalf("Expected about 10 updates, got %v", n)
	}

	// Now lookup the account doing the events on sb.
	acc, _ := sb.LookupAccount(pub)
	// Make sure we have the timer running.
	acc.mu.RLock()
	ctmr := acc.ctmr
	acc.mu.RUnlock()
	if ctmr == nil {
		t.Fatalf("Expected event timer for acc conns to be running")
	}

	nc.Close()

	checkFor(t, 2*time.Second, 15*time.Millisecond, func() error {
		// Make sure we have the timer is NOT running.
		acc.mu.RLock()
		ctmr = acc.ctmr
		acc.mu.RUnlock()
		if ctmr != nil {
			return fmt.Errorf("Expected event timer for acc conns to NOT be running after reaching zero local clients")
		}
		return nil
	})
}

func TestServerEventsReceivedByQSubs(t *testing.T) {
	s, opts := runTrustedServer(t)
	defer s.Shutdown()

	acc, akp := createAccount(s)
	s.setSystemAccount(acc)

	url := fmt.Sprintf("nats://%s:%d", opts.Host, opts.Port)
	ncs, err := nats.Connect(url, createUserCreds(t, s, akp))
	if err != nil {
		t.Fatalf("Error on connect: %v", err)
	}
	defer ncs.Close()

	// Listen for auth error events.
	qsub, _ := ncs.QueueSubscribeSync("$SYS.SERVER.*.CLIENT.AUTH.ERR", "queue")
	defer qsub.Unsubscribe()

	ncs.Flush()

	nats.Connect(url, nats.Name("TEST BAD LOGIN"))

	m, err := qsub.NextMsg(time.Second)
	if err != nil {
		t.Fatalf("Should have heard an auth error event")
	}
	dem := DisconnectEventMsg{}
	if err := json.Unmarshal(m.Data, &dem); err != nil {
		t.Fatalf("Error unmarshalling disconnect event message: %v", err)
	}
	if dem.Reason != "Authentication Failure" {
		t.Fatalf("Expected auth error, got %q", dem.Reason)
	}
}

func TestServerEventsFilteredByTag(t *testing.T) {
	confA := createConfFile(t, []byte(`
		listen: -1
		server_name: srv-A
		server_tags: ["foo", "bar"]
		cluster {
			name: clust
			listen: -1
			no_advertise: true
		}
		system_account: SYS
		accounts: {
			SYS: {
				users: [
					{user: b, password: b}
				]
			}
		}
		no_auth_user: b
    `))
	sA, _ := RunServerWithConfig(confA)
	defer sA.Shutdown()
	confB := createConfFile(t, []byte(fmt.Sprintf(`
		listen: -1
		server_name: srv-B
		server_tags: ["bar", "baz"]
		cluster {
			name: clust
			listen: -1
			no_advertise: true
			routes [
				nats-route://127.0.0.1:%d
			]
		}
		system_account: SYS
		accounts: {
			SYS: {
				users: [
					{user: b, password: b}
				]
			}
		}
		no_auth_user: b
    `, sA.opts.Cluster.Port)))
	sB, _ := RunServerWithConfig(confB)
	defer sB.Shutdown()
	checkClusterFormed(t, sA, sB)
	nc := natsConnect(t, sA.ClientURL())
	defer nc.Close()

	ib := nats.NewInbox()
	req := func(tags ...string) {
		t.Helper()
		r, err := json.Marshal(VarzEventOptions{EventFilterOptions: EventFilterOptions{Tags: tags}})
		require_NoError(t, err)
		err = nc.PublishRequest(fmt.Sprintf(serverPingReqSubj, "VARZ"), ib, r)
		require_NoError(t, err)
	}

	msgs := make(chan *nats.Msg, 10)
	defer close(msgs)
	_, err := nc.ChanSubscribe(ib, msgs)
	require_NoError(t, err)
	req("none")
	select {
	case <-msgs:
		t.Fatalf("no message expected")
	case <-time.After(200 * time.Millisecond):
	}
	req("foo")
	m := <-msgs
	require_Contains(t, string(m.Data), "srv-A", "foo", "bar")
	req("foo", "bar")
	m = <-msgs
	require_Contains(t, string(m.Data), "srv-A", "foo", "bar")
	req("baz")
	m = <-msgs
	require_Contains(t, string(m.Data), "srv-B", "bar", "baz")
	req("bar")
	m1 := <-msgs
	m2 := <-msgs
	require_Contains(t, string(m1.Data)+string(m2.Data), "srv-A", "srv-B", "foo", "bar", "baz")
	require_Len(t, len(msgs), 0)
}

func TestServerUnstableEventFilterMatch(t *testing.T) {
	confA := createConfFile(t, []byte(`
		listen: -1
		server_name: srv1
		server_tags: ["foo", "bar"]
		cluster {
			name: clust
			listen: -1
			no_advertise: true
		}
		system_account: SYS
		accounts: {
			SYS: {
				users: [
					{user: b, password: b}
				]
			}
		}
		no_auth_user: b
    `))
	sA, _ := RunServerWithConfig(confA)
	defer sA.Shutdown()
	confB := createConfFile(t, []byte(fmt.Sprintf(`
		listen: -1
		server_name: srv10
		server_tags: ["bar", "baz"]
		cluster {
			name: clust
			listen: -1
			no_advertise: true
			routes [
				nats-route://127.0.0.1:%d
			]
		}
		system_account: SYS
		accounts: {
			SYS: {
				users: [
					{user: b, password: b}
				]
			}
		}
		no_auth_user: b
    `, sA.opts.Cluster.Port)))
	sB, _ := RunServerWithConfig(confB)
	defer sB.Shutdown()
	checkClusterFormed(t, sA, sB)

	tester := func(t *testing.T, nc *nats.Conn, name string, count int) {
		t.Helper()

		r, err := json.Marshal(VarzEventOptions{EventFilterOptions: EventFilterOptions{Name: name, ExactMatch: true}})
		require_NoError(t, err)

		for i := 0; i < count; i++ {
			res, err := nc.Request(fmt.Sprintf(serverPingReqSubj, "VARZ"), r, time.Second)
			require_NoError(t, err)

			var vz ServerAPIVarzResponse
			err = json.Unmarshal(res.Data, &vz)
			require_NoError(t, err)

			if vz.Server.Name != name {
				t.Fatalf("Expected server name to be %q, got %q", name, vz.Server.Name)
			}
		}
	}

	// Connects to srv1 so it's most likely to respond first while we are asking srv10 to respond.
	nc := natsConnect(t, sA.ClientURL())
	defer nc.Close()
	tester(t, nc, "srv10", 10)

	// Connects to srv10 so it's most likely to respond first while we are asking srv1 to respond.
	nc.Close()
	nc = natsConnect(t, sB.ClientURL())
	defer nc.Close()
	tester(t, nc, "srv1", 10)
}

// https://github.com/nats-io/nats-server/issues/3177
func TestServerEventsAndDQSubscribers(t *testing.T) {
	c := createJetStreamClusterWithTemplate(t, jsClusterAccountsTempl, "DDQ", 3)
	defer c.shutdown()

	nc, err := nats.Connect(c.randomServer().ClientURL(), nats.UserInfo("admin", "s3cr3t!"))
	require_NoError(t, err)
	defer nc.Close()

	sub, err := nc.QueueSubscribeSync("$SYS.ACCOUNT.*.DISCONNECT", "qq")
	require_NoError(t, err)
	nc.Flush()

	// Create and disconnect 10 random connections.
	for i := 0; i < 10; i++ {
		nc, err := nats.Connect(c.randomServer().ClientURL())
		require_NoError(t, err)
		nc.Close()
	}

	checkSubsPending(t, sub, 10)
}

func TestServerEventsStatszSingleServer(t *testing.T) {
	conf := createConfFile(t, []byte(`
		listen: "127.0.0.1:-1"
		accounts { $SYS { users [{user: "admin", password: "p1d"}]} }
	`))
	s, _ := RunServerWithConfig(conf)
	defer s.Shutdown()

	// Grab internal system client.
	s.mu.RLock()
	sysc := s.sys.client
	wait := s.sys.cstatsz + 25*time.Millisecond
	s.mu.RUnlock()

	// Wait for when first statsz would have gone out..
	time.Sleep(wait)

	sysc.mu.Lock()
	outMsgs := sysc.stats.outMsgs
	sysc.mu.Unlock()

	require_True(t, outMsgs == 0)

	// Connect as a system user and make sure if there is
	// subscription interest that we will receive updates.
	nc, _ := jsClientConnect(t, s, nats.UserInfo("admin", "p1d"))
	defer nc.Close()

	sub, err := nc.SubscribeSync(fmt.Sprintf(serverStatsSubj, "*"))
	require_NoError(t, err)

	checkSubsPending(t, sub, 1)
}

func TestServerEventsReload(t *testing.T) {
	conf := createConfFile(t, []byte(`
		listen: "127.0.0.1:-1"
		accounts: {
			$SYS { users [{user: "admin", password: "p1d"}]}
			test { users [{user: "foo", password: "bar"}]}
		}
		ping_interval: "100ms"
	`))
	opts := LoadConfig(conf)
	s := RunServer(opts)
	defer s.Shutdown()
	subject := fmt.Sprintf(serverReloadReqSubj, s.info.ID)

	// Connect as a test user and make sure the reload endpoint is not
	// accessible.
	ncTest, _ := jsClientConnect(t, s, nats.UserInfo("foo", "bar"))
	defer ncTest.Close()
	testReply := ncTest.NewRespInbox()
	sub, err := ncTest.SubscribeSync(testReply)
	require_NoError(t, err)
	err = ncTest.PublishRequest(subject, testReply, nil)
	require_NoError(t, err)
	_, err = sub.NextMsg(time.Second)
	require_Error(t, err)

	require_True(t, s.getOpts().PingInterval == 100*time.Millisecond)

	// Connect as a system user.
	nc, _ := jsClientConnect(t, s, nats.UserInfo("admin", "p1d"))
	defer nc.Close()

	// rewrite the config file with a different ping interval
	err = os.WriteFile(conf, []byte(`
		listen: "127.0.0.1:-1"
		accounts: {
			$SYS { users [{user: "admin", password: "p1d"}]}
			test { users [{user: "foo", password: "bar"}]}
		}
		ping_interval: "200ms"
	`), 0666)
	require_NoError(t, err)

	msg, err := nc.Request(subject, nil, time.Second)
	require_NoError(t, err)

	var apiResp = ServerAPIResponse{}
	err = json.Unmarshal(msg.Data, &apiResp)
	require_NoError(t, err)

	require_True(t, apiResp.Data == nil)
	require_True(t, apiResp.Error == nil)

	// See that the ping interval has changed.
	require_True(t, s.getOpts().PingInterval == 200*time.Millisecond)

	// rewrite the config file with a different ping interval
	err = os.WriteFile(conf, []byte(`garbage and nonsense`), 0666)
	require_NoError(t, err)

	// Request the server to reload and wait for the response.
	msg, err = nc.Request(subject, nil, time.Second)
	require_NoError(t, err)

	apiResp = ServerAPIResponse{}
	err = json.Unmarshal(msg.Data, &apiResp)
	require_NoError(t, err)

	require_True(t, apiResp.Data == nil)
	require_Error(t, apiResp.Error, fmt.Errorf("Parse error on line 1: 'Expected a top-level value to end with a new line, comment or EOF, but got 'n' instead.'"))

	// See that the ping interval has not changed.
	require_True(t, s.getOpts().PingInterval == 200*time.Millisecond)
}

func TestServerEventsLDMKick(t *testing.T) {
	ldmed := make(chan bool, 1)
	disconnected := make(chan bool, 1)

	s, opts := runTrustedServer(t)
	defer s.Shutdown()

	acc, akp := createAccount(s)
	s.setSystemAccount(acc)

	url := fmt.Sprintf("nats://%s:%d", opts.Host, opts.Port)
	ncs, err := nats.Connect(url, createUserCreds(t, s, akp))
	if err != nil {
		t.Fatalf("Error on connect: %v", err)
	}
	defer ncs.Close()

	_, akp2 := createAccount(s)

	nc, err := nats.Connect(url, createUserCreds(t, s, akp2), nats.Name("TEST EVENTS LDM+KICK"), nats.LameDuckModeHandler(func(_ *nats.Conn) { ldmed <- true }))
	if err != nil {
		t.Fatalf("Error on connect: %v", err)
	}
	defer nc.Close()

	nc.SetDisconnectErrHandler(func(_ *nats.Conn, err error) { disconnected <- true })

	cid, err := nc.GetClientID()
	if err != nil {
		t.Fatalf("Error on getting the CID: %v", err)
	}

	reqldm := LDMClientReq{CID: cid}
	reqldmpayload, _ := json.Marshal(reqldm)
	reqkick := KickClientReq{CID: cid}
	reqkickpayload, _ := json.Marshal(reqkick)

	// Test for data races when getting the client by ID
	uc := createUserCreds(t, s, akp2)
	totalClients := 100
	someClients := make([]*nats.Conn, 0, totalClients)
	for i := 0; i < totalClients; i++ {
		nc, err := nats.Connect(s.ClientURL(), uc)
		require_NoError(t, err)
		defer nc.Close()
		someClients = append(someClients, nc)
	}
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < totalClients; i++ {
			someClients[i].Close()
		}
	}()
	defer wg.Wait()

	_, err = ncs.Request(fmt.Sprintf("$SYS.REQ.SERVER.%s.LDM", s.ID()), reqldmpayload, time.Second)
	require_NoError(t, err)

	select {
	case <-ldmed:
	case <-time.After(time.Second):
		t.Fatalf("timeout waiting for the connection to receive the LDM signal")
	}

	_, err = ncs.Request(fmt.Sprintf("$SYS.REQ.SERVER.%s.KICK", s.ID()), reqkickpayload, time.Second)
	require_NoError(t, err)

	select {
	case <-disconnected:
	case <-time.After(time.Second):
		t.Fatalf("timeout waiting for the client to get disconnected")
	}
}

func Benchmark_GetHash(b *testing.B) {
	b.StopTimer()
	// Get 100 random names
	names := make([]string, 0, 100)
	for i := 0; i < 100; i++ {
		names = append(names, nuid.Next())
	}
	hashes := make([]string, 0, 100)
	for j := 0; j < 100; j++ {
		sha := sha256.New()
		sha.Write([]byte(names[j]))
		b := sha.Sum(nil)
		for i := 0; i < 8; i++ {
			b[i] = digits[int(b[i]%base)]
		}
		hashes = append(hashes, string(b[:8]))
	}
	wg := sync.WaitGroup{}
	wg.Add(8)
	errCh := make(chan error, 8)
	b.StartTimer()
	for i := 0; i < 8; i++ {
		go func() {
			defer wg.Done()
			for i := 0; i < b.N; i++ {
				idx := rand.IntN(100)
				if h := getHash(names[idx]); h != hashes[idx] {
					errCh <- fmt.Errorf("Hash for name %q was %q, but should be %q", names[idx], h, hashes[idx])
					return
				}
			}
		}()
	}
	wg.Wait()
	select {
	case err := <-errCh:
		b.Fatal(err.Error())
	default:
	}
}

func TestClusterSetupMsgs(t *testing.T) {
	// Tests will set this general faster, but here we want default for production.
	original := statszRateLimit
	statszRateLimit = defaultStatszRateLimit
	defer func() { statszRateLimit = original }()

	numServers := 10
	c := createClusterEx(t, false, 0, false, "cluster", numServers)
	defer shutdownCluster(c)

	checkFor(t, 3*time.Second, 500*time.Millisecond, func() error {
		var totalOut int
		for _, server := range c.servers {
			totalOut += int(atomic.LoadInt64(&server.outMsgs))
		}
		totalExpected := numServers * numServers
		if totalOut >= totalExpected {
			return fmt.Errorf("Total outMsgs is %d, expected < %d\n", totalOut, totalExpected)
		}
		return nil
	})
}

func TestServerEventsProfileZNotBlockingRecvQ(t *testing.T) {
	sa, _, sb, optsB, akp := runTrustedCluster(t)
	defer sa.Shutdown()
	defer sb.Shutdown()
	// For this test, we will run a single server because the profiling
	// would fail for the second server since it would detect that
	// one profiling is already running (2 servers, but same process).
	sa.Shutdown()
	url := fmt.Sprintf("nats://%s:%d", optsB.Host, optsB.Port)
	nc, err := nats.Connect(url, createUserCreds(t, sb, akp))
	require_NoError(t, err)
	defer nc.Close()
	// We need to wait a little bit for the $SYS.SERVER.ACCOUNT.%s.CONNS
	// event to be pushed in the mux'ed queue.
	time.Sleep(300 * time.Millisecond)

	po := ProfilezOptions{Name: "cpu", Duration: 1 * time.Second}
	req, err := json.Marshal(po)
	require_NoError(t, err)

	testReq := func(t *testing.T, subj string) {
		// Block the recvQ by locking it for the duration of this test.
		sb.mu.RLock()
		recvq := sb.sys.recvq
		sb.mu.RUnlock()
		recvq.Lock()
		defer recvq.Unlock()

		// Send the profilez request on the given subject.
		reply := nc.NewRespInbox()
		sub := natsSubSync(t, nc, reply)
		nc.PublishRequest(subj, reply, req)
		msg := natsNexMsg(t, sub, 10*time.Second)
		if len(msg.Data) == 0 {
			t.Fatal("Unexpected empty response")
		}
		// Make sure its a ServerAPIResponse
		resp := ServerAPIResponse{Data: &ProfilezStatus{}}
		err := json.Unmarshal(msg.Data, &resp)
		require_NoError(t, err)
		// Check profile status to make sure that we got something.
		ps := resp.Data.(*ProfilezStatus)
		if ps.Error != _EMPTY_ {
			t.Fatalf("%s", ps.Error)
		}
		require_True(t, len(ps.Profile) > 0)
	}
	const profilez = "PROFILEZ"
	for _, test := range []struct {
		name string
		f    func() string
	}{
		{"server profilez request subject", func() string { return fmt.Sprintf(serverPingReqSubj, profilez) }},
		{"server direct request subject", func() string { return fmt.Sprintf(serverDirectReqSubj, sb.ID(), profilez) }},
	} {
		t.Run(test.name, func(t *testing.T) {
			testReq(t, test.f())
		})
	}
}

func TestServerEventsStatsZJetStreamApiLevel(t *testing.T) {
	s, opts := runTrustedServer(t)
	defer s.Shutdown()

	acc, akp := createAccount(s)
	s.setSystemAccount(acc)
	s.EnableJetStream(&JetStreamConfig{StoreDir: t.TempDir()})

	url := fmt.Sprintf("nats://%s:%d", opts.Host, opts.Port)
	ncs, err := nats.Connect(url, createUserCreds(t, s, akp))
	require_NoError(t, err)
	defer ncs.Close()

	msg, err := ncs.Request("$SYS.REQ.SERVER.PING.STATSZ", nil, time.Second)
	require_NoError(t, err)

	var stats ServerStatsMsg
	err = json.Unmarshal(msg.Data, &stats)
	require_NoError(t, err)

	require_Equal(t, stats.Stats.JetStream.Stats.API.Level, JSApiLevel)
}

func TestServerEventsPingStatsSlowConsumersStats(t *testing.T) {
	s, _ := runTrustedServer(t)
	defer s.Shutdown()

	acc, akp := createAccount(s)
	s.setSystemAccount(acc)
	ncs, err := nats.Connect(s.ClientURL(), createUserCreds(t, s, akp))
	require_NoError(t, err)
	defer ncs.Close()

	const statsz = "STATSZ"
	for _, test := range []struct {
		name      string
		f         func() string
		expectTwo bool
	}{
		{"server stats ping request subject", func() string { return serverStatsPingReqSubj }, true},
		{"server ping request subject", func() string { return fmt.Sprintf(serverPingReqSubj, statsz) }, true},
		{"server direct request subject", func() string { return fmt.Sprintf(serverDirectReqSubj, s.ID(), statsz) }, false},
	} {
		t.Run(test.name, func(t *testing.T) {
			// Clear all slow consumers values
			s.scStats.clients.Store(0)
			s.scStats.routes.Store(0)
			s.scStats.gateways.Store(0)
			s.scStats.leafs.Store(0)

			msg, err := ncs.Request(test.f(), nil, time.Second)
			require_NoError(t, err)

			var ssm ServerStatsMsg
			err = json.Unmarshal(msg.Data, &ssm)
			require_NoError(t, err)

			// No slow consumer stats, so should be nil
			require_True(t, ssm.Stats.SlowConsumersStats == nil)

			// Now set some values
			s.scStats.clients.Store(1)
			s.scStats.routes.Store(2)
			s.scStats.gateways.Store(3)
			s.scStats.leafs.Store(4)

			msg, err = ncs.Request(test.f(), nil, time.Second)
			require_NoError(t, err)

			ssm = ServerStatsMsg{}
			err = json.Unmarshal(msg.Data, &ssm)
			require_NoError(t, err)

			require_NotNil(t, ssm.Stats.SlowConsumersStats)
			scs := ssm.Stats.SlowConsumersStats
			require_Equal(t, scs.Clients, 1)
			require_Equal(t, scs.Routes, 2)
			require_Equal(t, scs.Gateways, 3)
			require_Equal(t, scs.Leafs, 4)
		})
	}
}

func TestServerEventsPingStatsStaleConnectionStats(t *testing.T) {
	templ := `
                        listen: "127.0.0.1:-1"
                        system_account = sys
                        accounts {
                          a {
                            users = [{ user: a,  pass: a  }]
                          }
                          b {
                            users = [{ user: b,  pass: b  }]
                          }
                          sys {
                            users = [{ user: sys, pass: sys }]
                          }
                        }
                        `
	conf := createConfFile(t, []byte(templ))
	s, _ := RunServerWithConfig(conf)
	defer s.Shutdown()

	// 3 different ways to get statz:
	const statsz = "STATSZ"

	// $SYS.REQ.SERVER.NCBT6MBA7Q7ZF4R4WVXTSZTCZDPPXX2ALLN3XM75VCBCNMPIKQFPFLKV.STATSZ
	dirReqSubject := func(s *Server) string {
		return fmt.Sprintf(serverDirectReqSubj, s.ID(), statsz)
	}
	// $SYS.REQ.SERVER.PING.STATSZ
	pingReqSubject := func(s *Server) string {
		return fmt.Sprintf(serverPingReqSubj, statsz)
	}
	// $SYS.REQ.SERVER.PING
	statsPingSubject := func(s *Server) string {
		return serverStatsPingReqSubj
	}
	subjects := []string{dirReqSubject(s), pingReqSubject(s), statsPingSubject(s)}

	ncs, err := nats.Connect(s.ClientURL(), nats.UserInfo("sys", "sys"))
	require_NoError(t, err)

	for _, subject := range subjects {
		msg, err := ncs.Request(subject, nil, time.Second)
		require_NoError(t, err)

		var ssm ServerStatsMsg
		err = json.Unmarshal(msg.Data, &ssm)
		require_NoError(t, err)

		// No stale connection stats.
		require_True(t, ssm.Stats.StaleConnectionStats == nil)
		require_Equal(t, s.NumStaleConnections(), int64(0))
		require_Equal(t, s.NumStaleConnectionsClients(), uint64(0))
		require_Equal(t, s.NumStaleConnectionsRoutes(), uint64(0))
		require_Equal(t, s.NumStaleConnectionsGateways(), uint64(0))
		require_Equal(t, s.NumStaleConnectionsLeafs(), uint64(0))
	}

	// Set some values and confirm.
	s.staleStats.clients.Store(1)
	s.staleStats.routes.Store(2)
	s.staleStats.gateways.Store(3)
	s.staleStats.leafs.Store(4)
	atomic.StoreInt64(&s.staleConnections, 10)
	require_Equal(t, s.NumStaleConnections(), int64(10))
	require_Equal(t, s.NumStaleConnectionsClients(), uint64(1))
	require_Equal(t, s.NumStaleConnectionsRoutes(), uint64(2))
	require_Equal(t, s.NumStaleConnectionsGateways(), uint64(3))
	require_Equal(t, s.NumStaleConnectionsLeafs(), uint64(4))

	for _, subject := range subjects {
		msg, err := ncs.Request(subject, nil, time.Second)
		require_NoError(t, err)

		ssm := ServerStatsMsg{}
		err = json.Unmarshal(msg.Data, &ssm)
		require_NoError(t, err)

		require_NotNil(t, ssm.Stats.StaleConnectionStats)
		stcs := ssm.Stats.StaleConnectionStats
		require_Equal(t, stcs.Clients, 1)
		require_Equal(t, stcs.Routes, 2)
		require_Equal(t, stcs.Gateways, 3)
		require_Equal(t, stcs.Leafs, 4)
		require_Equal(t, ssm.Stats.StaleConnections, int64(10))
	}
}

func TestServerEventsStatszMaxProcsMemLimit(t *testing.T) {
	// We want to prove that our set values are reflected in STATSZ,
	// so we can't use constants that might match the system that
	// the test is run on.
	omp, omm := runtime.GOMAXPROCS(-1), debug.SetMemoryLimit(-1)
	mp, mm := runtime.GOMAXPROCS(omp*2)*2, debug.SetMemoryLimit(omm/2)/2

	// When we're done, put everything back.
	defer runtime.GOMAXPROCS(omp)
	defer debug.SetMemoryLimit(omm)

	s, opts := runTrustedServer(t)
	defer s.Shutdown()

	acc, akp := createAccount(s)
	s.setSystemAccount(acc)

	url := fmt.Sprintf("nats://%s:%d", opts.Host, opts.Port)
	ncs, err := nats.Connect(url, createUserCreds(t, s, akp))
	require_NoError(t, err)
	defer ncs.Close()

	msg, err := ncs.Request("$SYS.REQ.SERVER.PING.STATSZ", nil, time.Second)
	require_NoError(t, err)

	var stats ServerStatsMsg
	require_NoError(t, json.Unmarshal(msg.Data, &stats))
	require_Equal(t, stats.Stats.MaxProcs, mp)
	require_Equal(t, stats.Stats.MemLimit, mm)
}

func TestSubszPagination(t *testing.T) {
	type subszResp struct {
		Subsz  Subsz      `json:"data"`
		Server ServerInfo `json:"server"`
	}
	s, opts := runTrustedServer(t)
	defer s.Shutdown()

	sysAcc, sysAkp := createAccount(s)
	s.setSystemAccount(sysAcc)

	acc, akp := createAccount(s)

	url := fmt.Sprintf("nats://%s:%d", opts.Host, opts.Port)
	ncSys, err := nats.Connect(url, createUserCreds(t, s, sysAkp))
	require_NoError(t, err)
	defer ncSys.Close()

	nc, err := nats.Connect(url, createUserCreds(t, s, akp))
	if err != nil {
		t.Fatalf("Error on connect: %v", err)
	}
	defer nc.Close()

	// Create 100 subscriptions.
	for i := range 100 {
		nc.Subscribe(fmt.Sprintf("foo.%d", i), func(_ *nats.Msg) {})
	}

	time.Sleep(100 * time.Millisecond)

	reqSubject := fmt.Sprintf(accDirectReqSubj, acc.Name, "SUBSZ")

	// Request the first page.
	subszReq := SubszOptions{Subscriptions: true, Limit: 10}
	req, _ := json.Marshal(subszReq)
	msg, err := ncSys.Request(reqSubject, req, time.Second)
	require_NoError(t, err)

	var subsz subszResp
	require_NoError(t, json.Unmarshal(msg.Data, &subsz))
	require_Equal(t, len(subsz.Subsz.Subs), 10)

	// we cannot check for equality since we have to account for the monitoring subscriptions
	if subsz.Subsz.Total < 100 || subsz.Subsz.Total > 110 {
		t.Fatalf("Expected total subscriptions to be more than 100 and less than 110, got %d", subsz.Subsz.Total)
	}

	// Now test with a sub filter

	// create 10 subs on "bar.*"
	for range 10 {
		nc.Subscribe("bar.*", func(_ *nats.Msg) {})
	}

	time.Sleep(100 * time.Millisecond)

	subszReq = SubszOptions{Subscriptions: true, Limit: 5, Test: "bar.A"}
	req, _ = json.Marshal(subszReq)
	msg, err = ncSys.Request(reqSubject, req, time.Second)
	require_NoError(t, err)

	var subszFiltered subszResp
	require_NoError(t, json.Unmarshal(msg.Data, &subszFiltered))
	require_Equal(t, len(subszFiltered.Subsz.Subs), 5)
	require_Equal(t, subszFiltered.Subsz.Total, 10)
}

func TestServerEventsConnectDisconnectForGlobalAcc(t *testing.T) {
	conf := createConfFile(t, []byte(`
		listen: "127.0.0.1:-1"
		accounts {
			$SYS {
				users [{user: "admin", password: "pwd"}]
			}
		}
	`))
	defer os.Remove(conf)

	s, opts := RunServerWithConfig(conf)
	defer s.Shutdown()

	url := fmt.Sprintf("nats://%s:%d", opts.Host, opts.Port)
	ncs, err := nats.Connect(url, nats.UserInfo("admin", "pwd"))
	require_NoError(t, err)
	defer ncs.Close()

	s1, err := ncs.SubscribeSync(fmt.Sprintf(connectEventSubj, globalAccountName))
	require_NoError(t, err)
	s2, err := ncs.SubscribeSync(fmt.Sprintf(disconnectEventSubj, globalAccountName))
	require_NoError(t, err)

	// Flush to make sure subscriptions are established
	require_NoError(t, ncs.Flush())

	// Connect to global account
	ncg, err := nats.Connect(url, nats.UserInfo("", ""))
	require_NoError(t, err)

	// System account should get a connect event
	msg, err := s1.NextMsg(5 * time.Second)
	require_NoError(t, err)
	require_Equal(t, msg.Subject, fmt.Sprintf(connectEventSubj, globalAccountName))

	// Disconnect from global account
	ncg.Close()

	// System account should get a disconnect event
	msg, err = s2.NextMsg(5 * time.Second)
	require_NoError(t, err)
	require_Equal(t, msg.Subject, fmt.Sprintf(disconnectEventSubj, globalAccountName))
}

func TestSendInternalAccountMsgWithReplyDoesNotMutateHeaderSlice(t *testing.T) {
	s := RunServer(DefaultOptions())
	defer s.Shutdown()

	nc, err := nats.Connect(s.ClientURL())
	require_NoError(t, err)
	defer nc.Close()

	sub, err := nc.SubscribeSync("foo")
	require_NoError(t, err)
	require_NoError(t, nc.Flush())

	// Build a header slice with significant spare capacity, like callers
	// constructing headers with append/genHeader naturally do.
	hdr := make([]byte, 0, 256)
	hdr = append(hdr, "NATS/1.0\r\nFoo: bar\r\n\r\n"...)

	// Snapshot the entire backing array so we can detect any mutation,
	// including writes past len(hdr) into spare capacity.
	snapshot := make([]byte, cap(hdr))
	copy(snapshot, hdr[:cap(hdr)])

	body := []byte("REPRO_LEAK_BODY")
	err = s.sendInternalAccountMsgWithReply(s.globalAccount(), "foo", _EMPTY_, hdr, body, false)
	require_NoError(t, err)

	msg, err := sub.NextMsg(2 * time.Second)
	require_NoError(t, err)
	require_Equal(t, string(msg.Data), "REPRO_LEAK_BODY")
	require_Equal(t, msg.Header.Get("Foo"), "bar")

	// The caller's hdr slice (and its full backing array) must be untouched.
	got := hdr[:cap(hdr)]
	if !bytes.Equal(got, snapshot) {
		t.Fatalf("hdr backing array was mutated by send:\n want: %q\n  got: %q", snapshot, got)
	}
}

func TestEventsResetChSendNoPanicAfterShutdownEventing(t *testing.T) {
	s := RunServer(DefaultOptions())
	defer s.Shutdown()

	// Simulate a sender that grabbed the channel reference before it was nil-ed,
	// exactly as the production senders do under s.mu.
	s.mu.Lock()
	if s.sys == nil || s.sys.resetCh == nil {
		s.mu.Unlock()
		t.Fatal("expected system eventing to be running")
	}
	rc := s.sys.resetCh
	s.mu.Unlock()

	// Drive eventing shutdown to completion. With the old code this closed rc.
	s.shutdownEventing()

	// Stand in for the (now exited) internal send loop so that, on a healthy
	// open channel, the send can complete.
	go func() { <-rc }()

	// Perform the send using the same select-against-quitCh pattern as the
	// production senders. On the buggy code rc is closed and this panics.
	done := make(chan any, 1)
	go func() {
		defer func() { done <- recover() }()
		select {
		case rc <- struct{}{}:
		case <-s.quitCh:
		}
	}()

	select {
	case r := <-done:
		if r != nil {
			t.Fatalf("send on resetCh panicked during shutdown: %v", r)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("send on resetCh blocked unexpectedly")
	}
}

func TestEventsRemoteLatencyUpdateNilLatency(t *testing.T) {
	s, _ := runTrustedServer(t)
	defer s.Shutdown()

	// Enable internal events by setting a system account.
	sacc, _ := createAccount(s)
	require_NoError(t, s.setSystemAccount(sacc))

	// Target account that will own the tracked response serviceImport.
	acc, _ := createAccount(s)

	// Construct the offending state directly: a response serviceImport with a
	// .T-suffixed tracked reply, tracking enabled, but a nil latency.
	reply := "_R_.abcdef.T"
	si := &serviceImport{
		acc:      acc,
		from:     reply,
		to:       reply,
		response: true,
		tracking: true, // tracking on, but...
		latency:  nil,  // ...no latency object -> nil deref at si.latency.subject
	}
	acc.mu.Lock()
	if acc.exports.responses == nil {
		acc.exports.responses = make(map[string]*serviceImport)
	}
	acc.exports.responses[reply] = si
	acc.mu.Unlock()

	// Craft a remote latency measurement that resolves to this serviceImport.
	rl := remoteLatency{
		Account: acc.Name,
		ReqId:   reply,
	}
	msg, err := json.Marshal(&rl)
	require_NoError(t, err)

	// On unfixed code this panics with a nil pointer dereference on
	// si.latency.subject. With the fix it returns cleanly.
	var recovered any
	func() {
		defer func() {
			if r := recover(); r != nil {
				recovered = r
				// The panic occurred while holding acc.mu.RLock() (taken just
				// before the offending deref and released just after). Release
				// it so the deferred s.Shutdown does not deadlock.
				acc.mu.RUnlock()
			}
		}()
		s.remoteLatencyUpdate(nil, nil, nil, "$SYS.LATENCY.M2."+reply, "", nil, msg)
	}()

	if recovered != nil {
		t.Fatalf("remoteLatencyUpdate panicked on nil si.latency: %v", recovered)
	}
}
