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
	"bufio"
	"bytes"
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"net"
	"net/url"
	"os"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nats-io/nats-server/v2/logger"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nkeys"
	"golang.org/x/crypto/ocsp"

	. "github.com/nats-io/nats-server/v2/internal/ocsp"
)

func init() {
	gatewayConnectDelay = 15 * time.Millisecond
	gatewayConnectMaxDelay = 15 * time.Millisecond
	gatewayReconnectDelay = 15 * time.Millisecond
}

// Wait for the expected number of outbound gateways, or fails.
func waitForOutboundGateways(t *testing.T, s *Server, expected int, timeout time.Duration) {
	t.Helper()
	if timeout < 2*time.Second {
		timeout = 2 * time.Second
	}
	checkFor(t, timeout, 15*time.Millisecond, func() error {
		if n := s.numOutboundGateways(); n != expected {
			return fmt.Errorf("Expected %v outbound gateway(s), got %v", expected, n)
		}
		return nil
	})
}

// Wait for the expected number of inbound gateways, or fails.
func waitForInboundGateways(t *testing.T, s *Server, expected int, timeout time.Duration) {
	t.Helper()
	if timeout < 2*time.Second {
		timeout = 2 * time.Second
	}
	checkFor(t, timeout, 15*time.Millisecond, func() error {
		if n := s.numInboundGateways(); n != expected {
			return fmt.Errorf("Expected %v inbound gateway(s), got %v", expected, n)
		}
		return nil
	})
}

func waitForGatewayFailedConnect(t *testing.T, s *Server, gwName string, expectFailure bool, timeout time.Duration) {
	t.Helper()
	checkFor(t, timeout, 15*time.Millisecond, func() error {
		var c int
		cfg := s.getRemoteGateway(gwName)
		if cfg != nil {
			c = cfg.getConnAttempts()
		}
		if expectFailure && c <= 1 {
			return fmt.Errorf("Expected several attempts to connect, got %v", c)
		} else if !expectFailure && c > 1 {
			return fmt.Errorf("Expected single attempt to connect, got %v", c)
		}
		return nil
	})
}

func checkForRegisteredQSubInterest(t *testing.T, s *Server, gwName, acc, subj string, expected int, timeout time.Duration) {
	t.Helper()
	checkFor(t, timeout, 15*time.Millisecond, func() error {
		count := 0
		c := s.getOutboundGatewayConnection(gwName)
		ei, _ := c.gw.outsim.Load(acc)
		if ei != nil {
			sl := ei.(*outsie).sl
			r := sl.Match(subj)
			for _, qsubs := range r.qsubs {
				count += len(qsubs)
			}
		}
		if count == expected {
			return nil
		}
		return fmt.Errorf("Expected %v qsubs in sublist, got %v", expected, count)
	})
}

func checkForSubjectNoInterest(t *testing.T, c *client, account, subject string, expectNoInterest bool, timeout time.Duration) {
	t.Helper()
	checkFor(t, timeout, 15*time.Millisecond, func() error {
		ei, _ := c.gw.outsim.Load(account)
		if ei == nil {
			return fmt.Errorf("Did not receive subject no-interest")
		}
		e := ei.(*outsie)
		e.RLock()
		_, inMap := e.ni[subject]
		e.RUnlock()
		if expectNoInterest {
			if inMap {
				return nil
			}
			return fmt.Errorf("Did not receive subject no-interest on %q", subject)
		}
		if inMap {
			return fmt.Errorf("No-interest on subject %q was not cleared", subject)
		}
		return nil
	})
}

func checkForAccountNoInterest(t *testing.T, c *client, account string, expectedNoInterest bool, timeout time.Duration) {
	t.Helper()
	checkFor(t, timeout, 15*time.Millisecond, func() error {
		ei, ok := c.gw.outsim.Load(account)
		if !ok && expectedNoInterest {
			return fmt.Errorf("No-interest for account %q not yet registered", account)
		} else if ok && !expectedNoInterest {
			return fmt.Errorf("Account %q should not have a no-interest", account)
		}
		if ei != nil {
			return fmt.Errorf("Account %q should have a global no-interest, not subject no-interest", account)
		}
		return nil
	})
}

func checkGWInterestOnlyMode(t *testing.T, s *Server, outboundGWName, accName string) {
	t.Helper()
	checkGWInterestOnlyModeOrNotPresent(t, s, outboundGWName, accName, false)
}

func checkGWInterestOnlyModeOrNotPresent(t *testing.T, s *Server, outboundGWName, accName string, notPresentOk bool) {
	t.Helper()
	checkFor(t, 2*time.Second, 15*time.Millisecond, func() error {
		gwc := s.getOutboundGatewayConnection(outboundGWName)
		if gwc == nil {
			return fmt.Errorf("No outbound gateway connection %q for server %v", outboundGWName, s)
		}
		gwc.mu.Lock()
		defer gwc.mu.Unlock()
		out, ok := gwc.gw.outsim.Load(accName)
		if !ok {
			if notPresentOk {
				return nil
			} else {
				return fmt.Errorf("Server %v - outbound gateway connection %q: no account %q found in map",
					s, outboundGWName, accName)
			}
		}
		if out == nil {
			return fmt.Errorf("Server %v - outbound gateway connection %q: interest map not found for account %q",
				s, outboundGWName, accName)
		}
		e := out.(*outsie)
		e.RLock()
		defer e.RUnlock()
		if e.mode != InterestOnly {
			return fmt.Errorf(
				"Server %v - outbound gateway connection %q: account %q mode shoule be InterestOnly but is %v",
				s, outboundGWName, accName, e.mode)
		}
		return nil
	})
}

func checkGWInterestOnlyModeInterestOn(t *testing.T, s *Server, outboundGWName, accName, subject string) {
	t.Helper()
	checkFor(t, 2*time.Second, 15*time.Millisecond, func() error {
		c := s.getOutboundGatewayConnection(outboundGWName)
		outsiei, _ := c.gw.outsim.Load(accName)
		if outsiei == nil {
			return fmt.Errorf("Server %s - outbound gateway connection %q: no map entry found for account %q",
				s, outboundGWName, accName)
		}
		outsie := outsiei.(*outsie)
		r := outsie.sl.Match(subject)
		if len(r.psubs) == 0 {
			return fmt.Errorf("Server %s - outbound gateway connection %q - account %q: no subject interest for %q",
				s, outboundGWName, accName, subject)
		}
		return nil
	})
}

func waitCh(t *testing.T, ch chan bool, errTxt string) {
	t.Helper()
	select {
	case <-ch:
		return
	case <-time.After(5 * time.Second):
		t.Fatal(errTxt)
	}
}

var noOpErrHandler = func(_ *nats.Conn, _ *nats.Subscription, _ error) {}

func natsConnect(t testing.TB, url string, options ...nats.Option) *nats.Conn {
	t.Helper()
	opts := nats.GetDefaultOptions()
	for _, opt := range options {
		if err := opt(&opts); err != nil {
			t.Fatalf("Error applying client option: %v", err)
		}
	}
	nc, err := nats.Connect(url, options...)
	if err != nil {
		t.Fatalf("Error on connect: %v", err)
	}
	if opts.AsyncErrorCB == nil {
		// Set this up to not pollute the logs when running tests.
		nc.SetErrorHandler(noOpErrHandler)
	}

	return nc
}

func natsSub(t *testing.T, nc *nats.Conn, subj string, cb nats.MsgHandler) *nats.Subscription {
	t.Helper()
	sub, err := nc.Subscribe(subj, cb)
	if err != nil {
		t.Fatalf("Error on subscribe: %v", err)
	}
	return sub
}

func natsSubSync(t *testing.T, nc *nats.Conn, subj string) *nats.Subscription {
	t.Helper()
	sub, err := nc.SubscribeSync(subj)
	if err != nil {
		t.Fatalf("Error on subscribe: %v", err)
	}
	return sub
}

func natsNexMsg(t *testing.T, sub *nats.Subscription, timeout time.Duration) *nats.Msg {
	t.Helper()
	msg, err := sub.NextMsg(timeout)
	if err != nil {
		t.Fatalf("Failed getting next message: %v", err)
	}
	return msg
}

func natsQueueSub(t *testing.T, nc *nats.Conn, subj, queue string, cb nats.MsgHandler) *nats.Subscription {
	t.Helper()
	sub, err := nc.QueueSubscribe(subj, queue, cb)
	if err != nil {
		t.Fatalf("Error on subscribe: %v", err)
	}
	return sub
}

func natsQueueSubSync(t *testing.T, nc *nats.Conn, subj, queue string) *nats.Subscription {
	t.Helper()
	sub, err := nc.QueueSubscribeSync(subj, queue)
	if err != nil {
		t.Fatalf("Error on subscribe: %v", err)
	}
	return sub
}

func natsFlush(t *testing.T, nc *nats.Conn) {
	t.Helper()
	if err := nc.Flush(); err != nil {
		t.Fatalf("Error on flush: %v", err)
	}
}

func natsPub(t testing.TB, nc *nats.Conn, subj string, payload []byte) {
	t.Helper()
	if err := nc.Publish(subj, payload); err != nil {
		t.Fatalf("Error on publish: %v", err)
	}
}

func natsPubReq(t *testing.T, nc *nats.Conn, subj, reply string, payload []byte) {
	t.Helper()
	if err := nc.PublishRequest(subj, reply, payload); err != nil {
		t.Fatalf("Error on publish: %v", err)
	}
}

func natsUnsub(t *testing.T, sub *nats.Subscription) {
	t.Helper()
	if err := sub.Unsubscribe(); err != nil {
		t.Fatalf("Error on unsubscribe: %v", err)
	}
}

func testDefaultOptionsForGateway(name string) *Options {
	o := DefaultOptions()
	o.NoSystemAccount = true
	o.Cluster.Name = name
	o.Gateway.Name = name
	o.Gateway.Host = "127.0.0.1"
	o.Gateway.Port = -1
	o.gatewaysSolicitDelay = 15 * time.Millisecond
	return o
}

func runGatewayServer(o *Options) *Server {
	s := RunServer(o)
	s.SetLogger(&DummyLogger{}, true, true)
	return s
}

func testGatewayOptionsFromToWithServers(t *testing.T, org, dst string, servers ...*Server) *Options {
	t.Helper()
	o := testDefaultOptionsForGateway(org)
	gw := &RemoteGatewayOpts{Name: dst}
	for _, s := range servers {
		us := fmt.Sprintf("nats://127.0.0.1:%d", s.GatewayAddr().Port)
		u, err := url.Parse(us)
		if err != nil {
			t.Fatalf("Error parsing url: %v", err)
		}
		gw.URLs = append(gw.URLs, u)
	}
	o.Gateway.Gateways = append(o.Gateway.Gateways, gw)
	return o
}

func testAddGatewayURLs(t *testing.T, o *Options, dst string, urls []string) {
	t.Helper()
	gw := &RemoteGatewayOpts{Name: dst}
	for _, us := range urls {
		u, err := url.Parse(us)
		if err != nil {
			t.Fatalf("Error parsing url: %v", err)
		}
		gw.URLs = append(gw.URLs, u)
	}
	o.Gateway.Gateways = append(o.Gateway.Gateways, gw)
}

func testGatewayOptionsFromToWithURLs(t *testing.T, org, dst string, urls []string) *Options {
	o := testDefaultOptionsForGateway(org)
	testAddGatewayURLs(t, o, dst, urls)
	return o
}

func testGatewayOptionsWithTLS(t *testing.T, name string) *Options {
	t.Helper()
	o := testDefaultOptionsForGateway(name)
	var (
		tc  = &TLSConfigOpts{}
		err error
	)
	if name == "A" {
		tc.CertFile = "../test/configs/certs/srva-cert.pem"
		tc.KeyFile = "../test/configs/certs/srva-key.pem"
	} else {
		tc.CertFile = "../test/configs/certs/srvb-cert.pem"
		tc.KeyFile = "../test/configs/certs/srvb-key.pem"
	}
	tc.CaFile = "../test/configs/certs/ca.pem"
	o.Gateway.TLSConfig, err = GenTLSConfig(tc)
	if err != nil {
		t.Fatalf("Error generating TLS config: %v", err)
	}
	o.Gateway.TLSConfig.ClientAuth = tls.RequireAndVerifyClientCert
	o.Gateway.TLSConfig.RootCAs = o.Gateway.TLSConfig.ClientCAs
	o.Gateway.TLSTimeout = 2.0
	return o
}

func testGatewayOptionsFromToWithTLS(t *testing.T, org, dst string, urls []string) *Options {
	o := testGatewayOptionsWithTLS(t, org)
	testAddGatewayURLs(t, o, dst, urls)
	return o
}

func TestGatewayBasic(t *testing.T) {
	o2 := testDefaultOptionsForGateway("B")
	o2.Gateway.ConnectRetries = 0
	s2 := runGatewayServer(o2)
	defer s2.Shutdown()

	o1 := testGatewayOptionsFromToWithServers(t, "A", "B", s2)
	s1 := runGatewayServer(o1)
	defer s1.Shutdown()

	// s1 should have an outbound gateway to s2.
	waitForOutboundGateways(t, s1, 1, time.Second)
	// s2 should have an inbound gateway
	waitForInboundGateways(t, s2, 1, time.Second)
	// and an outbound too
	waitForOutboundGateways(t, s2, 1, time.Second)

	// Stop s2 server
	s2.Shutdown()

	// gateway should go away
	waitForOutboundGateways(t, s1, 0, time.Second)
	waitForInboundGateways(t, s1, 0, time.Second)

	// Restart server
	s2 = runGatewayServer(o2)
	defer s2.Shutdown()

	// gateway should reconnect
	waitForOutboundGateways(t, s1, 1, 2*time.Second)
	waitForOutboundGateways(t, s2, 1, 2*time.Second)
	waitForInboundGateways(t, s1, 1, 2*time.Second)
	waitForInboundGateways(t, s2, 1, 2*time.Second)

	// Shutdown s1, remove the gateway from A to B and restart.
	s1.Shutdown()
	// When s2 detects the connection is closed, it will attempt
	// to reconnect once (even if the route is implicit).
	// We need to wait more than a dial timeout to make sure
	// s1 does not restart too quickly and s2 can actually reconnect.
	time.Sleep(DEFAULT_ROUTE_DIAL + 250*time.Millisecond)
	// Restart s1 without gateway to B.
	o1.Gateway.Gateways = nil
	s1 = runGatewayServer(o1)
	defer s1.Shutdown()

	// s1 should not have any outbound nor inbound
	waitForOutboundGateways(t, s1, 0, 2*time.Second)
	waitForInboundGateways(t, s1, 0, 2*time.Second)

	// Same for s2
	waitForOutboundGateways(t, s2, 0, 2*time.Second)
	waitForInboundGateways(t, s2, 0, 2*time.Second)

	// Verify that s2 no longer has A gateway in its list
	checkFor(t, time.Second, 15*time.Millisecond, func() error {
		if s2.getRemoteGateway("A") != nil {
			return fmt.Errorf("Gateway A should have been removed from s2")
		}
		return nil
	})
}

func TestGatewayIgnoreSelfReference(t *testing.T) {
	o := testDefaultOptionsForGateway("A")
	// To create a reference to itself before running the server
	// it means that we have to assign an explicit port
	o.Gateway.Port = 5222
	o.gatewaysSolicitDelay = 0
	u, _ := url.Parse(fmt.Sprintf("nats://%s:%d", o.Gateway.Host, o.Gateway.Port))
	cfg := &RemoteGatewayOpts{
		Name: "A",
		URLs: []*url.URL{u},
	}
	o.Gateway.Gateways = append(o.Gateway.Gateways, cfg)
	o.NoSystemAccount = true
	s := runGatewayServer(o)
	defer s.Shutdown()

	// Wait a bit to make sure that there is no attempt to connect.
	time.Sleep(20 * time.Millisecond)

	// No outbound connection expected, and no attempt to connect.
	if s.getRemoteGateway("A") != nil {
		t.Fatalf("Should not have a remote gateway config for A")
	}
	if s.getOutboundGatewayConnection("A") != nil {
		t.Fatalf("Should not have a gateway connection to A")
	}
	s.Shutdown()

	// Now try with config files and include
	s1, _ := RunServerWithConfig("configs/gwa.conf")
	defer s1.Shutdown()

	s2, _ := RunServerWithConfig("configs/gwb.conf")
	defer s2.Shutdown()

	waitForOutboundGateways(t, s1, 1, 2*time.Second)
	waitForOutboundGateways(t, s2, 1, 2*time.Second)
	waitForInboundGateways(t, s1, 1, 2*time.Second)
	waitForInboundGateways(t, s2, 1, 2*time.Second)

	if s1.getRemoteGateway("A") != nil {
		t.Fatalf("Should not have a remote gateway config for A")
	}
	if s1.getOutboundGatewayConnection("A") != nil {
		t.Fatalf("Should not have a gateway connection to A")
	}
	if s2.getRemoteGateway("B") != nil {
		t.Fatalf("Should not have a remote gateway config for B")
	}
	if s2.getOutboundGatewayConnection("B") != nil {
		t.Fatalf("Should not have a gateway connection to B")
	}
}

func TestGatewayHeaderInfo(t *testing.T) {
	o := testDefaultOptionsForGateway("A")
	s := runGatewayServer(o)
	defer s.Shutdown()

	gwconn, err := net.Dial("tcp", net.JoinHostPort(o.Gateway.Host, fmt.Sprintf("%d", o.Gateway.Port)))
	if err != nil {
		t.Fatalf("Error dialing server: %v\n", err)
	}
	defer gwconn.Close()
	client := bufio.NewReaderSize(gwconn, maxBufSize)
	l, err := client.ReadString('\n')
	if err != nil {
		t.Fatalf("Error receiving info from server: %v\n", err)
	}
	var info serverInfo
	if err = json.Unmarshal([]byte(l[5:]), &info); err != nil {
		t.Fatalf("Could not parse INFO json: %v\n", err)
	}
	if !info.Headers {
		t.Fatalf("Expected by default for header support to be enabled")
	}

	s.Shutdown()
	gwconn.Close()

	// Now turn headers off.
	o.NoHeaderSupport = true
	s = runGatewayServer(o)
	defer s.Shutdown()

	gwconn, err = net.Dial("tcp", net.JoinHostPort(o.Gateway.Host, fmt.Sprintf("%d", o.Gateway.Port)))
	if err != nil {
		t.Fatalf("Error dialing server: %v\n", err)
	}
	defer gwconn.Close()
	client = bufio.NewReaderSize(gwconn, maxBufSize)
	l, err = client.ReadString('\n')
	if err != nil {
		t.Fatalf("Error receiving info from server: %v\n", err)
	}
	if err = json.Unmarshal([]byte(l[5:]), &info); err != nil {
		t.Fatalf("Could not parse INFO json: %v\n", err)
	}
	if info.Headers {
		t.Fatalf("Expected header support to be disabled")
	}
}

func TestGatewayRejectsInfoBeforeConnect(t *testing.T) {
	o := testDefaultOptionsForGateway("A")
	s := runGatewayServer(o)
	defer s.Shutdown()

	gwconn, err := net.Dial("tcp", net.JoinHostPort(o.Gateway.Host, strconv.Itoa(o.Gateway.Port)))
	if err != nil {
		t.Fatalf("Error dialing server: %v", err)
	}
	defer gwconn.Close()

	client := bufio.NewReaderSize(gwconn, maxBufSize)
	if _, err := client.ReadString('\n'); err != nil {
		t.Fatalf("Error receiving initial INFO from server: %v", err)
	}

	if _, err := fmt.Fprint(gwconn, "INFO {}\r\n"); err != nil {
		t.Fatalf("Error sending gateway INFO: %v", err)
	}

	if err := gwconn.SetReadDeadline(time.Now().Add(time.Second)); err != nil {
		t.Fatalf("Error setting read deadline: %v", err)
	}
	l, err := client.ReadString('\n')
	if err != nil {
		t.Fatalf("Expected authorization failure, got read error: %v", err)
	}
	if !strings.Contains(l, "Authorization Violation") {
		t.Fatalf("Expected authorization violation, got %q", l)
	}
}

// Verify that receiving A-/A+ on an inbound gateway connection closes the
// connection with a protocol violation.
func TestGatewayAccountSubUnsubOnInboundProtocolViolation(t *testing.T) {
	o := testDefaultOptionsForGateway("A")
	s := runGatewayServer(o)
	defer s.Shutdown()

	for _, proto := range []string{"A- foo\r\n", "A+ foo\r\n"} {
		t.Run(strings.TrimSpace(proto), func(t *testing.T) {
			gwconn, err := net.Dial("tcp", net.JoinHostPort(o.Gateway.Host, strconv.Itoa(o.Gateway.Port)))
			if err != nil {
				t.Fatalf("Error dialing server: %v", err)
			}
			defer gwconn.Close()

			br := bufio.NewReaderSize(gwconn, maxBufSize)
			if _, err := br.ReadString('\n'); err != nil {
				t.Fatalf("Error receiving initial INFO from server: %v", err)
			}

			// Send a CONNECT so that the connection is registered as an
			// inbound gateway, then the A-/A+ protocol.
			if _, err := fmt.Fprintf(gwconn, "CONNECT {\"gateway\":\"B\",\"name\":\"x\"}\r\n%s", proto); err != nil {
				t.Fatalf("Error sending gateway CONNECT: %v", err)
			}

			// The server should close the connection (protocol violation).
			if err := gwconn.SetReadDeadline(time.Now().Add(2 * time.Second)); err != nil {
				t.Fatalf("Error setting read deadline: %v", err)
			}
			var rerr error
			for rerr == nil {
				_, rerr = br.ReadString('\n')
			}
			if ne, ok := rerr.(net.Error); ok && ne.Timeout() {
				t.Fatalf("Expected connection to be closed by the server, got timeout")
			}
			waitForInboundGateways(t, s, 0, 2*time.Second)
		})
	}
}

// Verify that receiving a message (RMSG/HMSG) on an outbound gateway connection
// closes the connection with a protocol violation before buffering any payload.
func TestGatewayMsgOnOutboundProtocolViolation(t *testing.T) {
	ob := testDefaultOptionsForGateway("B")
	sb := runGatewayServer(ob)
	defer sb.Shutdown()

	oa := testGatewayOptionsFromToWithServers(t, "A", "B", sb)
	sa := runGatewayServer(oa)
	defer sa.Shutdown()

	waitForOutboundGateways(t, sa, 1, 2*time.Second)
	waitForInboundGateways(t, sb, 1, 2*time.Second)

	for _, proto := range []string{"RMSG $G foo 2\r\n", "HMSG $G foo 0 2\r\n"} {
		t.Run(strings.TrimSpace(proto), func(t *testing.T) {
			// A's outbound connection to B.
			gwa := sa.getOutboundGatewayConnection("B")
			if gwa == nil {
				t.Fatalf("Outbound gateway connection to B not found")
			}
			// B's inbound connection from A: what it writes is read by A's
			// outbound connection, which should never receive an RMSG/HMSG.
			var gws []*client
			sb.getInboundGatewayConnections(&gws)
			if len(gws) != 1 {
				t.Fatalf("Expected 1 inbound gateway on B, got %v", len(gws))
			}
			gwb := gws[0]
			gwb.mu.Lock()
			gwb.enqueueProto([]byte(proto))
			gwb.mu.Unlock()

			// A's outbound connection should be closed immediately upon
			// receiving the header line, without waiting for the payload.
			checkFor(t, 2*time.Second, 15*time.Millisecond, func() error {
				gwa.mu.Lock()
				closed := gwa.isClosed()
				gwa.mu.Unlock()
				if !closed {
					return fmt.Errorf("Outbound gateway connection should have been closed")
				}
				return nil
			})
			// A should reconnect to B.
			checkFor(t, 2*time.Second, 15*time.Millisecond, func() error {
				if gw := sa.getOutboundGatewayConnection("B"); gw == nil || gw == gwa {
					return fmt.Errorf("Outbound gateway connection to B not re-established")
				}
				return nil
			})
			waitForInboundGateways(t, sb, 1, 2*time.Second)
		})
	}
}

func TestGatewayHeaderSupport(t *testing.T) {
	o2 := testDefaultOptionsForGateway("B")
	o2.Gateway.ConnectRetries = 0
	s2 := runGatewayServer(o2)
	defer s2.Shutdown()

	o1 := testGatewayOptionsFromToWithServers(t, "A", "B", s2)
	s1 := runGatewayServer(o1)
	defer s1.Shutdown()

	// s1 should have an outbound gateway to s2.
	waitForOutboundGateways(t, s1, 1, time.Second)
	// and an inbound too
	waitForInboundGateways(t, s1, 1, time.Second)
	// s2 should have an inbound gateway
	waitForInboundGateways(t, s2, 1, time.Second)
	// and an outbound too
	waitForOutboundGateways(t, s2, 1, time.Second)

	c, cr, _ := newClientForServer(s1)
	defer c.close()

	connect := "CONNECT {\"headers\":true}"
	subOp := "SUB foo 1"
	pingOp := "PING\r\n"
	cmd := strings.Join([]string{connect, subOp, pingOp}, "\r\n")
	c.parseAsync(cmd)
	if _, err := cr.ReadString('\n'); err != nil {
		t.Fatalf("Error receiving msg from server: %v\n", err)
	}

	// Wait for interest to be registered on s2
	checkGWInterestOnlyModeInterestOn(t, s2, "A", globalAccountName, "foo")

	b, _, _ := newClientForServer(s2)
	defer b.close()

	pubOp := "HPUB foo 12 14\r\nName:Derek\r\nOK\r\n"
	cmd = strings.Join([]string{connect, pubOp}, "\r\n")
	b.parseAsync(cmd)

	l, err := cr.ReadString('\n')
	if err != nil {
		t.Fatalf("Error receiving msg from server: %v\n", err)
	}

	am := hmsgPat.FindAllStringSubmatch(l, -1)
	if len(am) == 0 {
		t.Fatalf("Did not get a match for %q", l)
	}
	matches := am[0]
	if len(matches) != 7 {
		t.Fatalf("Did not get correct # matches: %d vs %d\n", len(matches), 7)
	}
	if matches[SUB_INDEX] != "foo" {
		t.Fatalf("Did not get correct subject: '%s'\n", matches[SUB_INDEX])
	}
	if matches[SID_INDEX] != "1" {
		t.Fatalf("Did not get correct sid: '%s'\n", matches[SID_INDEX])
	}
	if matches[HDR_INDEX] != "12" {
		t.Fatalf("Did not get correct msg length: '%s'\n", matches[HDR_INDEX])
	}
	if matches[TLEN_INDEX] != "14" {
		t.Fatalf("Did not get correct msg length: '%s'\n", matches[TLEN_INDEX])
	}
	checkPayload(cr, []byte("Name:Derek\r\nOK\r\n"), t)
}

func TestGatewayHeaderDeliverStrippedMsg(t *testing.T) {
	o2 := testDefaultOptionsForGateway("B")
	o2.Gateway.ConnectRetries = 0
	s2 := runGatewayServer(o2)
	defer s2.Shutdown()

	o1 := testGatewayOptionsFromToWithServers(t, "A", "B", s2)
	o1.NoHeaderSupport = true
	s1 := runGatewayServer(o1)
	defer s1.Shutdown()

	// s1 should have an outbound gateway to s2.
	waitForOutboundGateways(t, s1, 1, time.Second)
	// and an inbound too
	waitForInboundGateways(t, s1, 1, time.Second)
	// s2 should have an inbound gateway
	waitForInboundGateways(t, s2, 1, time.Second)
	// and an outbound too
	waitForOutboundGateways(t, s2, 1, time.Second)

	c, cr, _ := newClientForServer(s1)
	defer c.close()

	connect := "CONNECT {\"headers\":true}"
	subOp := "SUB foo 1"
	pingOp := "PING\r\n"
	cmd := strings.Join([]string{connect, subOp, pingOp}, "\r\n")
	c.parseAsync(cmd)
	if _, err := cr.ReadString('\n'); err != nil {
		t.Fatalf("Error receiving msg from server: %v\n", err)
	}

	// Wait for interest to be registered on s2
	checkGWInterestOnlyModeInterestOn(t, s2, "A", globalAccountName, "foo")

	b, _, _ := newClientForServer(s2)
	defer b.close()

	pubOp := "HPUB foo 12 14\r\nName:Derek\r\nOK\r\n"
	cmd = strings.Join([]string{connect, pubOp}, "\r\n")
	b.parseAsync(cmd)

	l, err := cr.ReadString('\n')
	if err != nil {
		t.Fatalf("Error receiving msg from server: %v\n", err)
	}
	am := smsgPat.FindAllStringSubmatch(l, -1)
	if len(am) == 0 {
		t.Fatalf("Did not get a correct match for %q", l)
	}
	matches := am[0]
	if len(matches) != 6 {
		t.Fatalf("Did not get correct # matches: %d vs %d\n", len(matches), 6)
	}
	if matches[SUB_INDEX] != "foo" {
		t.Fatalf("Did not get correct subject: '%s'\n", matches[SUB_INDEX])
	}
	if matches[SID_INDEX] != "1" {
		t.Fatalf("Did not get correct sid: '%s'\n", matches[SID_INDEX])
	}
	if matches[LEN_INDEX] != "2" {
		t.Fatalf("Did not get correct msg length: '%s'\n", matches[LEN_INDEX])
	}
	checkPayload(cr, []byte("OK\r\n"), t)
	if cr.Buffered() != 0 {
		t.Fatalf("Expected no extra bytes to be buffered, got %d", cr.Buffered())
	}
}

func TestGatewaySolicitDelay(t *testing.T) {
	o2 := testDefaultOptionsForGateway("B")
	s2 := runGatewayServer(o2)
	defer s2.Shutdown()

	o1 := testGatewayOptionsFromToWithServers(t, "A", "B", s2)
	// Set the solicit delay to 0. This tests that server will use its
	// default value, currently set at 1 sec.
	o1.gatewaysSolicitDelay = 0
	start := time.Now()
	s1 := runGatewayServer(o1)
	defer s1.Shutdown()

	// After 500ms, check outbound gateway. Should not be there.
	time.Sleep(500 * time.Millisecond)
	if time.Since(start) < defaultSolicitGatewaysDelay {
		if s1.numOutboundGateways() > 0 {
			t.Fatalf("The outbound gateway was initiated sooner than expected (%v)", time.Since(start))
		}
	}
	// Ultimately, s1 should have an outbound gateway to s2.
	waitForOutboundGateways(t, s1, 1, 2*time.Second)
	// s2 should have an inbound gateway
	waitForInboundGateways(t, s2, 1, 2*time.Second)

	s1.Shutdown()
	// Make sure that server can be shutdown while waiting
	// for that initial solicit delay
	o1.gatewaysSolicitDelay = 2 * time.Second
	s1 = runGatewayServer(o1)
	start = time.Now()
	s1.Shutdown()
	if dur := time.Since(start); dur >= 2*time.Second {
		t.Fatalf("Looks like shutdown was delayed: %v", dur)
	}
}

func TestGatewaySolicitDelayWithImplicitOutbounds(t *testing.T) {
	// Cause a situation where A connects to B, and because of
	// delay of solicit gateways set on B, we want to make sure
	// that B does not end-up with 2 connections to A.
	o2 := testDefaultOptionsForGateway("B")
	o2.gatewaysSolicitDelay = 500 * time.Millisecond
	s2 := runGatewayServer(o2)
	defer s2.Shutdown()

	o1 := testGatewayOptionsFromToWithServers(t, "A", "B", s2)
	s1 := runGatewayServer(o1)
	defer s1.Shutdown()

	// s1 should have an outbound and inbound gateway to s2.
	waitForOutboundGateways(t, s1, 1, 2*time.Second)
	// s2 should have an inbound gateway
	waitForInboundGateways(t, s2, 1, 2*time.Second)
	// Wait for more than s2 solicit delay
	time.Sleep(750 * time.Millisecond)
	// The way we store outbound (map key'ed by gw name), we would
	// not know if we had created 2 (since the newer would replace
	// the older in the map). But if a second connection was made,
	// then s1 would have 2 inbounds. So check it has only 1.
	waitForInboundGateways(t, s1, 1, time.Second)
}

type slowResolver struct {
	inLookupCh chan struct{}
	releaseCh  chan struct{}
}

func (r *slowResolver) LookupHost(ctx context.Context, h string) ([]string, error) {
	if r.inLookupCh != nil {
		select {
		case r.inLookupCh <- struct{}{}:
		default:
		}
		<-r.releaseCh
	} else {
		time.Sleep(500 * time.Millisecond)
	}
	return []string{h}, nil
}

func TestGatewaySolicitShutdown(t *testing.T) {
	var urls []string
	for i := 0; i < 5; i++ {
		u := fmt.Sprintf("nats://localhost:%d", 1234+i)
		urls = append(urls, u)
	}
	o1 := testGatewayOptionsFromToWithURLs(t, "A", "B", urls)
	o1.Gateway.resolver = &slowResolver{}
	s1 := runGatewayServer(o1)
	defer s1.Shutdown()

	time.Sleep(o1.gatewaysSolicitDelay + 10*time.Millisecond)

	start := time.Now()
	s1.Shutdown()
	if dur := time.Since(start); dur > 1200*time.Millisecond {
		t.Fatalf("Took too long to shutdown: %v", dur)
	}
}

func testFatalErrorOnStart(t *testing.T, o *Options, errTxt string) {
	t.Helper()
	s := New(o)
	defer s.Shutdown()
	l := &captureFatalLogger{fatalCh: make(chan string, 1)}
	s.SetLogger(l, false, false)
	// This does not block
	s.Start()
	select {
	case e := <-l.fatalCh:
		if !strings.Contains(e, errTxt) {
			t.Fatalf("Error should contain %q, got %s", errTxt, e)
		}
	case <-time.After(time.Second):
		t.Fatal("Should have got a fatal error")
	}
	s.Shutdown()
	s.WaitForShutdown()
}

func TestGatewayListenError(t *testing.T) {
	o2 := testDefaultOptionsForGateway("B")
	s2 := runGatewayServer(o2)
	defer s2.Shutdown()

	o1 := testDefaultOptionsForGateway("A")
	o1.Gateway.Port = s2.GatewayAddr().Port
	testFatalErrorOnStart(t, o1, "listening on")
}

func TestGatewayWithListenToAny(t *testing.T) {
	confB1 := createConfFile(t, []byte(`
		listen: "127.0.0.1:-1"
		cluster {
			listen: "127.0.0.1:-1"
		}
		gateway {
			name: "B"
			listen: "0.0.0.0:-1"
		}
	`))
	sb1, ob1 := RunServerWithConfig(confB1)
	defer sb1.Shutdown()

	confB2 := createConfFile(t, []byte(fmt.Sprintf(`
		listen: "127.0.0.1:-1"
		cluster {
			listen: "127.0.0.1:-1"
			routes: ["%s"]
		}
		gateway {
			name: "B"
			listen: "0.0.0.0:-1"
		}
	`, fmt.Sprintf("nats://127.0.0.1:%d", sb1.ClusterAddr().Port))))
	sb2, ob2 := RunServerWithConfig(confB2)
	defer sb2.Shutdown()

	checkClusterFormed(t, sb1, sb2)

	confA := createConfFile(t, []byte(fmt.Sprintf(`
		listen: "127.0.0.1:-1"
		cluster {
			listen: "127.0.0.1:-1"
		}
		gateway {
			name: "A"
			listen: "0.0.0.0:-1"
			gateways [
				{
					name: "B"
					urls: ["%s", "%s"]
				}
			]
		}
	`, fmt.Sprintf("nats://127.0.0.1:%d", ob1.Gateway.Port), fmt.Sprintf("nats://127.0.0.1:%d", ob2.Gateway.Port))))
	oa := LoadConfig(confA)
	oa.gatewaysSolicitDelay = 15 * time.Millisecond
	sa := runGatewayServer(oa)
	defer sa.Shutdown()

	waitForOutboundGateways(t, sa, 1, time.Second)
	waitForOutboundGateways(t, sb1, 1, time.Second)
	waitForOutboundGateways(t, sb2, 1, time.Second)
	waitForInboundGateways(t, sa, 2, time.Second)

	checkAll := func(t *testing.T) {
		t.Helper()
		checkURL := func(t *testing.T, s *Server) {
			t.Helper()
			url := s.getGatewayURL()
			if strings.HasPrefix(url, "0.0.0.0") {
				t.Fatalf("URL still references 0.0.0.0")
			}
			s.gateway.RLock()
			for url := range s.gateway.URLs {
				if strings.HasPrefix(url, "0.0.0.0") {
					s.gateway.RUnlock()
					t.Fatalf("URL still references 0.0.0.0")
				}
			}
			s.gateway.RUnlock()

			var cfg *gatewayCfg
			if s.getGatewayName() == "A" {
				cfg = s.getRemoteGateway("B")
			} else {
				cfg = s.getRemoteGateway("A")
			}
			urls := cfg.getURLs()
			for _, url := range urls {
				if strings.HasPrefix(url.Host, "0.0.0.0") {
					t.Fatalf("URL still references 0.0.0.0")
				}
			}
		}
		checkURL(t, sb1)
		checkURL(t, sb2)
		checkURL(t, sa)
	}
	checkAll(t)
	// Perform a reload and ensure that nothing has changed
	servers := []*Server{sb1, sb2, sa}
	for _, s := range servers {
		if err := s.Reload(); err != nil {
			t.Fatalf("Error on reload: %v", err)
		}
		checkAll(t)
	}
}

func TestGatewayAdvertise(t *testing.T) {
	o3 := testDefaultOptionsForGateway("C")
	s3 := runGatewayServer(o3)
	defer s3.Shutdown()

	o2 := testDefaultOptionsForGateway("B")
	s2 := runGatewayServer(o2)
	defer s2.Shutdown()

	o1 := testGatewayOptionsFromToWithServers(t, "A", "B", s2)
	// Set the advertise so that this points to C
	o1.Gateway.Advertise = fmt.Sprintf("127.0.0.1:%d", s3.GatewayAddr().Port)
	s1 := runGatewayServer(o1)
	defer s1.Shutdown()

	// We should have outbound from s1 to s2
	waitForOutboundGateways(t, s1, 1, time.Second)
	// But no inbound from s2
	waitForInboundGateways(t, s1, 0, time.Second)

	// And since B tries to connect to A but reaches C, it should fail to connect,
	// and without connect retries, stop trying. So no outbound for s2, and no
	// inbound/outbound for s3.
	waitForInboundGateways(t, s2, 1, time.Second)
	waitForOutboundGateways(t, s2, 0, time.Second)
	waitForInboundGateways(t, s3, 0, time.Second)
	waitForOutboundGateways(t, s3, 0, time.Second)
}

func TestGatewayAdvertiseErr(t *testing.T) {
	o1 := testDefaultOptionsForGateway("A")
	o1.Gateway.Advertise = "wrong:address"
	testFatalErrorOnStart(t, o1, "Gateway.Advertise")
}

func TestGatewayAuth(t *testing.T) {
	o2 := testDefaultOptionsForGateway("B")
	o2.Gateway.Username = "me"
	o2.Gateway.Password = "pwd"
	s2 := runGatewayServer(o2)
	defer s2.Shutdown()

	o1 := testGatewayOptionsFromToWithURLs(t, "A", "B", []string{fmt.Sprintf("nats://me:pwd@127.0.0.1:%d", s2.GatewayAddr().Port)})
	s1 := runGatewayServer(o1)
	defer s1.Shutdown()

	// s1 should have an outbound gateway to s2.
	waitForOutboundGateways(t, s1, 1, time.Second)
	// s2 should have an inbound gateway
	waitForInboundGateways(t, s2, 1, time.Second)

	s2.Shutdown()
	s1.Shutdown()

	o2.Gateway.Username = "me"
	o2.Gateway.Password = "wrong"
	s2 = runGatewayServer(o2)
	defer s2.Shutdown()

	s1 = runGatewayServer(o1)
	defer s1.Shutdown()

	// Connection should fail...
	waitForGatewayFailedConnect(t, s1, "B", true, 2*time.Second)

	s2.Shutdown()
	s1.Shutdown()
	o2.Gateway.Username = "wrong"
	o2.Gateway.Password = "pwd"
	s2 = runGatewayServer(o2)
	defer s2.Shutdown()

	s1 = runGatewayServer(o1)
	defer s1.Shutdown()

	// Connection should fail...
	waitForGatewayFailedConnect(t, s1, "B", true, 2*time.Second)
}

func TestGatewayTLS(t *testing.T) {
	o2 := testGatewayOptionsWithTLS(t, "B")
	s2 := runGatewayServer(o2)
	defer s2.Shutdown()

	o1 := testGatewayOptionsFromToWithTLS(t, "A", "B", []string{fmt.Sprintf("nats://127.0.0.1:%d", s2.GatewayAddr().Port)})
	s1 := runGatewayServer(o1)
	defer s1.Shutdown()

	// s1 should have an outbound gateway to s2.
	waitForOutboundGateways(t, s1, 1, time.Second)
	// s2 should have an inbound gateway
	waitForInboundGateways(t, s2, 1, time.Second)
	// and vice-versa
	waitForOutboundGateways(t, s2, 1, time.Second)
	waitForInboundGateways(t, s1, 1, time.Second)

	// Stop s2 server
	s2.Shutdown()

	// gateway should go away
	waitForOutboundGateways(t, s1, 0, time.Second)
	waitForInboundGateways(t, s1, 0, time.Second)
	waitForOutboundGateways(t, s2, 0, time.Second)
	waitForInboundGateways(t, s2, 0, time.Second)

	// Restart server
	s2 = runGatewayServer(o2)
	defer s2.Shutdown()

	// gateway should reconnect
	waitForOutboundGateways(t, s1, 1, 2*time.Second)
	waitForOutboundGateways(t, s2, 1, 2*time.Second)
	waitForInboundGateways(t, s1, 1, 2*time.Second)
	waitForInboundGateways(t, s2, 1, 2*time.Second)

	s1.Shutdown()
	// Wait for s2 to lose connections to s1.
	waitForOutboundGateways(t, s2, 0, 2*time.Second)
	waitForInboundGateways(t, s2, 0, 2*time.Second)

	// Make an explicit TLS config for remote gateway config "B"
	// on cluster A.
	o1.Gateway.Gateways[0].TLSConfig = o1.Gateway.TLSConfig.Clone()
	u, _ := url.Parse(fmt.Sprintf("tls://localhost:%d", s2.GatewayAddr().Port))
	o1.Gateway.Gateways[0].URLs = []*url.URL{u}
	// Make the TLSTimeout so small that it should fail to connect.
	smallTimeout := 0.00000001
	o1.Gateway.Gateways[0].TLSTimeout = smallTimeout
	s1 = runGatewayServer(o1)
	defer s1.Shutdown()

	// Check that s1 reports connection failures
	waitForGatewayFailedConnect(t, s1, "B", true, 2*time.Second)

	// Check that TLSConfig from s1's remote "B" is based on
	// what we have configured.
	cfg := s1.getRemoteGateway("B")
	cfg.RLock()
	tlsName := cfg.tlsName
	timeout := cfg.TLSTimeout
	cfg.RUnlock()
	if tlsName != "localhost" {
		t.Fatalf("Expected server name to be localhost, got %v", tlsName)
	}
	if timeout != smallTimeout {
		t.Fatalf("Expected tls timeout to be %v, got %v", smallTimeout, timeout)
	}
	s1.Shutdown()
	// Wait for s2 to lose connections to s1.
	waitForOutboundGateways(t, s2, 0, 2*time.Second)
	waitForInboundGateways(t, s2, 0, 2*time.Second)

	// Remove explicit TLSTimeout from gateway "B" and check that
	// we use the A's spec one.
	o1.Gateway.Gateways[0].TLSTimeout = 0
	s1 = runGatewayServer(o1)
	defer s1.Shutdown()

	waitForOutboundGateways(t, s1, 1, 2*time.Second)
	waitForOutboundGateways(t, s2, 1, 2*time.Second)
	waitForInboundGateways(t, s1, 1, 2*time.Second)
	waitForInboundGateways(t, s2, 1, 2*time.Second)

	cfg = s1.getRemoteGateway("B")
	cfg.RLock()
	timeout = cfg.TLSTimeout
	cfg.RUnlock()
	if timeout != o1.Gateway.TLSTimeout {
		t.Fatalf("Expected tls timeout to be %v, got %v", o1.Gateway.TLSTimeout, timeout)
	}
}

func TestGatewayTLSErrors(t *testing.T) {
	o2 := testDefaultOptionsForGateway("B")
	s2 := runGatewayServer(o2)
	defer s2.Shutdown()

	o1 := testGatewayOptionsFromToWithTLS(t, "A", "B", []string{fmt.Sprintf("nats://127.0.0.1:%d", s2.ClusterAddr().Port)})
	s1 := runGatewayServer(o1)
	defer s1.Shutdown()

	// Expect s1 to have a failed to connect count > 0
	waitForGatewayFailedConnect(t, s1, "B", true, 2*time.Second)
}

func TestGatewayServerNameInTLSConfig(t *testing.T) {
	o2 := testDefaultOptionsForGateway("B")
	var (
		tc  = &TLSConfigOpts{}
		err error
	)
	tc.CertFile = "../test/configs/certs/server-noip.pem"
	tc.KeyFile = "../test/configs/certs/server-key-noip.pem"
	tc.CaFile = "../test/configs/certs/ca.pem"
	o2.Gateway.TLSConfig, err = GenTLSConfig(tc)
	if err != nil {
		t.Fatalf("Error generating TLS config: %v", err)
	}
	o2.Gateway.TLSConfig.ClientAuth = tls.RequireAndVerifyClientCert
	o2.Gateway.TLSConfig.RootCAs = o2.Gateway.TLSConfig.ClientCAs
	o2.Gateway.TLSTimeout = 2.0
	s2 := runGatewayServer(o2)
	defer s2.Shutdown()

	o1 := testGatewayOptionsFromToWithTLS(t, "A", "B", []string{fmt.Sprintf("nats://127.0.0.1:%d", s2.GatewayAddr().Port)})
	s1 := runGatewayServer(o1)
	defer s1.Shutdown()

	// s1 should fail to connect since we don't have proper expected hostname.
	waitForGatewayFailedConnect(t, s1, "B", true, 2*time.Second)

	// Now set server name, and it should work.
	s1.Shutdown()
	o1.Gateway.TLSConfig.ServerName = "localhost"
	s1 = runGatewayServer(o1)
	defer s1.Shutdown()

	waitForOutboundGateways(t, s1, 1, 2*time.Second)
}

func TestGatewayWrongDestination(t *testing.T) {
	// Start a server with a gateway named "C"
	o2 := testDefaultOptionsForGateway("C")
	s2 := runGatewayServer(o2)
	defer s2.Shutdown()

	// Configure a gateway to "B", but since we are connecting to "C"...
	o1 := testGatewayOptionsFromToWithServers(t, "A", "B", s2)
	s1 := runGatewayServer(o1)
	defer s1.Shutdown()

	// we should not be able to connect.
	waitForGatewayFailedConnect(t, s1, "B", true, time.Second)

	// Shutdown s2 and fix the gateway name.
	// s1 should then connect ok and failed connect should be cleared.
	s2.Shutdown()

	// Reset the conn attempts
	cfg := s1.getRemoteGateway("B")
	cfg.resetConnAttempts()

	o2.Gateway.Name = "B"
	o2.Cluster.Name = "B"
	s2 = runGatewayServer(o2)
	defer s2.Shutdown()

	// At some point, the number of failed connect count should be reset to 0.
	waitForGatewayFailedConnect(t, s1, "B", false, 2*time.Second)
}

func TestGatewayConnectToWrongPort(t *testing.T) {
	o2 := testDefaultOptionsForGateway("B")
	s2 := runGatewayServer(o2)
	defer s2.Shutdown()

	// Configure a gateway to "B", but connect to the wrong port
	urls := []string{fmt.Sprintf("nats://127.0.0.1:%d", s2.Addr().(*net.TCPAddr).Port)}
	o1 := testGatewayOptionsFromToWithURLs(t, "A", "B", urls)
	s1 := runGatewayServer(o1)
	defer s1.Shutdown()

	// we should not be able to connect.
	waitForGatewayFailedConnect(t, s1, "B", true, time.Second)

	s1.Shutdown()

	// Repeat with route port
	urls = []string{fmt.Sprintf("nats://127.0.0.1:%d", s2.ClusterAddr().Port)}
	o1 = testGatewayOptionsFromToWithURLs(t, "A", "B", urls)
	s1 = runGatewayServer(o1)
	defer s1.Shutdown()

	// we should not be able to connect.
	waitForGatewayFailedConnect(t, s1, "B", true, time.Second)

	s1.Shutdown()

	// Now have a client connect to s2's gateway port.
	nc, err := nats.Connect(fmt.Sprintf("nats://127.0.0.1:%d", s2.GatewayAddr().Port))
	if err == nil {
		nc.Close()
		t.Fatal("Expected error, got none")
	}
}

func TestGatewayCreateImplicit(t *testing.T) {
	// Create a regular cluster of 2 servers
	o2 := testDefaultOptionsForGateway("B")
	s2 := runGatewayServer(o2)
	defer s2.Shutdown()

	o3 := testDefaultOptionsForGateway("B")
	o3.Routes = RoutesFromStr(fmt.Sprintf("nats://127.0.0.1:%d", s2.ClusterAddr().Port))
	s3 := runGatewayServer(o3)
	defer s3.Shutdown()

	checkClusterFormed(t, s2, s3)

	// Now start s1 that creates a Gateway connection to s2 or s3
	o1 := testGatewayOptionsFromToWithServers(t, "A", "B", s2, s3)
	s1 := runGatewayServer(o1)
	defer s1.Shutdown()

	// We should have an outbound gateway connection on ALL servers.
	waitForOutboundGateways(t, s1, 1, 2*time.Second)
	waitForOutboundGateways(t, s2, 1, 2*time.Second)
	waitForOutboundGateways(t, s3, 1, 2*time.Second)

	// Server s1 must have 2 inbound ones
	waitForInboundGateways(t, s1, 2, 2*time.Second)

	// However, s1 may have created the outbound to s2 or s3. It is possible that
	// either s2 or s3 does not an inbound connection.
	s2Inbound := s2.numInboundGateways()
	s3Inbound := s3.numInboundGateways()
	if (s2Inbound == 1 && s3Inbound != 0) || (s3Inbound == 1 && s2Inbound != 0) {
		t.Fatalf("Unexpected inbound for s2/s3: %v/%v", s2Inbound, s3Inbound)
	}
}

func TestGatewayCreateImplicitOnNewRoute(t *testing.T) {
	// Start with only 2 clusters of 1 server each
	o2 := testDefaultOptionsForGateway("B")
	s2 := runGatewayServer(o2)
	defer s2.Shutdown()

	// Now start s1 that creates a Gateway connection to s2
	o1 := testGatewayOptionsFromToWithServers(t, "A", "B", s2)
	s1 := runGatewayServer(o1)
	defer s1.Shutdown()

	// Check outbounds
	waitForOutboundGateways(t, s1, 1, 2*time.Second)
	waitForOutboundGateways(t, s2, 1, 2*time.Second)

	// Now add a server to cluster B
	o3 := testDefaultOptionsForGateway("B")
	o3.Routes = RoutesFromStr(fmt.Sprintf("nats://127.0.0.1:%d", s2.ClusterAddr().Port))
	s3 := runGatewayServer(o3)
	defer s3.Shutdown()

	// Wait for cluster between s2/s3 to form
	checkClusterFormed(t, s2, s3)

	// s3 should have been notified about existence of A and create its gateway to A.
	waitForOutboundGateways(t, s1, 1, 2*time.Second)
	waitForOutboundGateways(t, s2, 1, 2*time.Second)
	waitForOutboundGateways(t, s3, 1, 2*time.Second)
}

func TestGatewayImplicitReconnect(t *testing.T) {
	o2 := testDefaultOptionsForGateway("B")
	o2.Gateway.ConnectRetries = 5
	s2 := runGatewayServer(o2)
	defer s2.Shutdown()

	o1 := testGatewayOptionsFromToWithServers(t, "A", "B", s2)
	s1 := runGatewayServer(o1)
	defer s1.Shutdown()

	// s1 should have an outbound gateway to s2.
	waitForOutboundGateways(t, s1, 1, time.Second)
	// s2 should have an inbound gateway
	waitForInboundGateways(t, s2, 1, time.Second)
	// It will have also created an implicit outbound connection to s1.
	// We need to wait for that implicit outbound connection to be made
	// to show that it will try to reconnect when we stop/restart s1
	// (without config to connect to B).
	waitForOutboundGateways(t, s2, 1, time.Second)

	// Shutdown s1, remove the gateway from A to B and restart.
	s1.Shutdown()
	o1.Gateway.Gateways = o1.Gateway.Gateways[:0]
	// Only the gateway port needs to be reused, others could be taken in the meantime.
	o1.Port, o1.Cluster.Port = -1, -1
	s1 = runGatewayServer(o1)
	defer s1.Shutdown()

	// s1 should have both outbound and inbound to s2
	waitForOutboundGateways(t, s1, 1, 2*time.Second)
	waitForInboundGateways(t, s1, 1, 2*time.Second)

	// Same for s2
	waitForOutboundGateways(t, s2, 1, 2*time.Second)
	waitForInboundGateways(t, s2, 1, 2*time.Second)

	// Verify that s2 still has "A" in its gateway config
	if s2.getRemoteGateway("A") == nil {
		t.Fatal("Gateway A should be in s2")
	}
}

func TestGatewayImplicitReconnectRace(t *testing.T) {
	ob := testDefaultOptionsForGateway("B")
	resolver := &slowResolver{
		inLookupCh: make(chan struct{}, 1),
		releaseCh:  make(chan struct{}),
	}
	ob.Gateway.resolver = resolver
	sb := runGatewayServer(ob)
	defer sb.Shutdown()

	oa1 := testGatewayOptionsFromToWithServers(t, "A", "B", sb)
	sa1 := runGatewayServer(oa1)
	defer sa1.Shutdown()

	// Wait for the proper connections
	waitForOutboundGateways(t, sa1, 1, time.Second)
	waitForOutboundGateways(t, sb, 1, time.Second)
	waitForInboundGateways(t, sa1, 1, time.Second)
	waitForInboundGateways(t, sb, 1, time.Second)

	// On sb, change the URL to sa1 so that it is a name, instead of an IP,
	// so that we hit the slow resolver.
	cfg := sb.getRemoteGateway("A")
	cfg.updateURLs([]string{fmt.Sprintf("localhost:%d", sa1.GatewayAddr().Port)})

	// Shutdown sa1 now...
	sa1.Shutdown()

	// Wait to be notified that B has detected the connection close
	// and it is trying to resolve the host during the reconnect.
	<-resolver.inLookupCh

	// Start a new "A" server (sa2).
	oa2 := testGatewayOptionsFromToWithServers(t, "A", "B", sb)
	sa2 := runGatewayServer(oa2)
	defer sa2.Shutdown()

	// Make sure we have our outbound to sb registered on sa2 and inbound
	// from sa2 on sb before releasing the resolver.
	waitForOutboundGateways(t, sa2, 1, 2*time.Second)
	waitForInboundGateways(t, sb, 1, 2*time.Second)

	// Now release the resolver and ensure we have all connections.
	close(resolver.releaseCh)

	waitForOutboundGateways(t, sb, 1, 2*time.Second)
	waitForInboundGateways(t, sa2, 1, 2*time.Second)
}

type gwReconnAttemptLogger struct {
	DummyLogger
	errCh chan string
}

func (l *gwReconnAttemptLogger) Errorf(format string, v ...any) {
	msg := fmt.Sprintf(format, v...)
	if strings.Contains(msg, `Error connecting to implicit gateway "A"`) {
		select {
		case l.errCh <- msg:
		default:
		}
	}
}

func TestGatewayImplicitReconnectHonorConnectRetries(t *testing.T) {
	ob := testDefaultOptionsForGateway("B")
	ob.ReconnectErrorReports = 1
	ob.Gateway.ConnectRetries = 2
	sb := runGatewayServer(ob)
	defer sb.Shutdown()

	l := &gwReconnAttemptLogger{errCh: make(chan string, 3)}
	sb.SetLogger(l, true, false)

	oa := testGatewayOptionsFromToWithServers(t, "A", "B", sb)
	sa := runGatewayServer(oa)
	defer sa.Shutdown()

	// Wait for the proper connections
	waitForOutboundGateways(t, sa, 1, time.Second)
	waitForOutboundGateways(t, sb, 1, time.Second)
	waitForInboundGateways(t, sa, 1, time.Second)
	waitForInboundGateways(t, sb, 1, time.Second)

	// Now have C connect to B.
	oc := testGatewayOptionsFromToWithServers(t, "C", "B", sb)
	sc := runGatewayServer(oc)
	defer sc.Shutdown()

	// Wait for the proper connections
	waitForOutboundGateways(t, sa, 2, time.Second)
	waitForOutboundGateways(t, sb, 2, time.Second)
	waitForOutboundGateways(t, sc, 2, time.Second)
	waitForInboundGateways(t, sa, 2, time.Second)
	waitForInboundGateways(t, sb, 2, time.Second)
	waitForInboundGateways(t, sc, 2, time.Second)

	// Shutdown sa now...
	sa.Shutdown()

	// B will try to reconnect to A 3 times (we stop after attempts > ConnectRetries)
	timeout := time.NewTimer(time.Second)
	for i := 0; i < 3; i++ {
		select {
		case <-l.errCh:
			// OK
		case <-timeout.C:
			t.Fatal("Did not get debug trace about reconnect")
		}
	}
	// If we get 1 more, we have an issue!
	select {
	case e := <-l.errCh:
		t.Fatalf("Should not have attempted to reconnect: %q", e)
	case <-time.After(250 * time.Millisecond):
		// OK!
	}

	waitForOutboundGateways(t, sb, 1, 2*time.Second)
	waitForInboundGateways(t, sb, 1, 2*time.Second)
	waitForOutboundGateways(t, sc, 1, 2*time.Second)
	waitForInboundGateways(t, sc, 1, 2*time.Second)
}

func TestGatewayReconnectExponentialBackoff(t *testing.T) {
	oGatewayConnectDelay := gatewayConnectDelay
	oGatewayConnectMaxDelay := gatewayConnectMaxDelay
	gatewayConnectDelay = 500 * time.Millisecond
	gatewayConnectMaxDelay = 2 * time.Second
	defer func() {
		gatewayConnectDelay = oGatewayConnectDelay
		gatewayConnectMaxDelay = oGatewayConnectMaxDelay
	}()

	ob := testDefaultOptionsForGateway("B")
	ob.ReconnectErrorReports = 1
	ob.Gateway.ConnectRetries = 3
	ob.Gateway.ConnectBackoff = true
	sb := runGatewayServer(ob)
	defer sb.Shutdown()

	l := &gwReconnAttemptLogger{errCh: make(chan string, 3)}
	sb.SetLogger(l, true, false)

	oa := testGatewayOptionsFromToWithServers(t, "A", "B", sb)
	sa := runGatewayServer(oa)
	defer sa.Shutdown()

	// Wait for the proper connections
	waitForOutboundGateways(t, sa, 1, time.Second)
	waitForOutboundGateways(t, sb, 1, time.Second)
	waitForInboundGateways(t, sa, 1, time.Second)
	waitForInboundGateways(t, sb, 1, time.Second)

	// Remove initial delay before reconnect, and allow for some skew.
	now := time.Now().Add(gatewayReconnectDelay).Add(-100 * time.Millisecond)
	var delay time.Duration

	// B will try to reconnect to A 3 times (we stop after attempts > ConnectRetries)
	sa.Shutdown()
	for i := 0; i < ob.Gateway.ConnectRetries+1; i++ {
		select {
		case <-l.errCh:
			if since := time.Since(now); since < delay {
				t.Fatalf("Expected delay to take %v, took %v", delay, since)
			}
			if delay == 0 {
				delay = gatewayConnectDelay
			} else {
				delay *= 2
			}
			if delay > gatewayConnectMaxDelay {
				delay = gatewayConnectMaxDelay
			}
		case <-time.After(gatewayConnectMaxDelay + time.Second):
			t.Fatal("Did not attempt to reconnect")
		}
	}
}

func TestGatewayURLsFromClusterSentInINFO(t *testing.T) {
	o2 := testDefaultOptionsForGateway("B")
	s2 := runGatewayServer(o2)
	defer s2.Shutdown()

	o3 := testDefaultOptionsForGateway("B")
	o3.Routes = RoutesFromStr(fmt.Sprintf("nats://127.0.0.1:%d", s2.ClusterAddr().Port))
	s3 := runGatewayServer(o3)
	defer s3.Shutdown()

	checkClusterFormed(t, s2, s3)

	// Now start s1 that creates a Gateway connection to s2
	o1 := testGatewayOptionsFromToWithServers(t, "A", "B", s2)
	s1 := runGatewayServer(o1)
	defer s1.Shutdown()

	// Make sure we have proper outbound/inbound
	waitForOutboundGateways(t, s1, 1, time.Second)
	waitForOutboundGateways(t, s2, 1, time.Second)
	waitForOutboundGateways(t, s3, 1, time.Second)

	// Although s1 connected to s2 and knew only about s2, it should have
	// received the list of gateway URLs in the B cluster. So if we shutdown
	// server s2, it should be able to reconnect to s3.
	s2.Shutdown()
	// Wait for s3 to register that there s2 is gone.
	checkNumRoutes(t, s3, 0)
	// s1 should have reconnected to s3 because it learned about it
	// when connecting earlier to s2.
	waitForOutboundGateways(t, s1, 1, 2*time.Second)
	// Also make sure that the gateway's urls map has 2 urls.
	gw := s1.getRemoteGateway("B")
	if gw == nil {
		t.Fatal("Did not find gateway B")
	}
	gw.RLock()
	l := len(gw.urls)
	gw.RUnlock()
	if l != 2 {
		t.Fatalf("S1 should have 2 urls, got %v", l)
	}
}

func TestGatewayUseUpdatedURLs(t *testing.T) {
	// For this test, we have cluster B with an explicit gateway to cluster A
	// on a given URL. Then we create cluster A with a gateway to B with server B's
	// GW url, and we expect server B to ultimately create an outbound GW connection
	// to server A (with the URL it will get from server A connecting to it).

	ob := testGatewayOptionsFromToWithURLs(t, "B", "A", []string{"nats://127.0.0.1:1234"})
	sb := runGatewayServer(ob)
	defer sb.Shutdown()

	// Add a delay before starting server A to make sure that server B start
	// initiating the connection to A on inexistant server at :1234.
	time.Sleep(100 * time.Millisecond)

	oa := testGatewayOptionsFromToWithServers(t, "A", "B", sb)
	sa := runGatewayServer(oa)
	defer sa.Shutdown()

	// sa should have no problem creating outbound connection to sb
	waitForOutboundGateways(t, sa, 1, time.Second)

	// Make sure that since sb learns about sa's GW URL, it can successfully
	// connect to it.
	waitForOutboundGateways(t, sb, 1, 3*time.Second)
	waitForInboundGateways(t, sb, 1, time.Second)
	waitForInboundGateways(t, sa, 1, time.Second)
}

func TestGatewayAutoDiscovery(t *testing.T) {
	o4 := testDefaultOptionsForGateway("D")
	s4 := runGatewayServer(o4)
	defer s4.Shutdown()

	o3 := testGatewayOptionsFromToWithServers(t, "C", "D", s4)
	s3 := runGatewayServer(o3)
	defer s3.Shutdown()

	o2 := testGatewayOptionsFromToWithServers(t, "B", "C", s3)
	s2 := runGatewayServer(o2)
	defer s2.Shutdown()

	o1 := testGatewayOptionsFromToWithServers(t, "A", "B", s2)
	s1 := runGatewayServer(o1)
	defer s1.Shutdown()

	// Each server should have 3 outbound gateway connections.
	waitForOutboundGateways(t, s1, 3, 2*time.Second)
	waitForOutboundGateways(t, s2, 3, 2*time.Second)
	waitForOutboundGateways(t, s3, 3, 2*time.Second)
	waitForOutboundGateways(t, s4, 3, 2*time.Second)

	s1.Shutdown()
	s2.Shutdown()
	s3.Shutdown()
	s4.Shutdown()

	o2 = testDefaultOptionsForGateway("B")
	s2 = runGatewayServer(o2)
	defer s2.Shutdown()

	o4 = testGatewayOptionsFromToWithServers(t, "D", "B", s2)
	s4 = runGatewayServer(o4)
	defer s4.Shutdown()

	o3 = testGatewayOptionsFromToWithServers(t, "C", "B", s2)
	s3 = runGatewayServer(o3)
	defer s3.Shutdown()

	o1 = testGatewayOptionsFromToWithServers(t, "A", "B", s2)
	s1 = runGatewayServer(o1)
	defer s1.Shutdown()

	// Each server should have 3 outbound gateway connections.
	waitForOutboundGateways(t, s1, 3, 2*time.Second)
	waitForOutboundGateways(t, s2, 3, 2*time.Second)
	waitForOutboundGateways(t, s3, 3, 2*time.Second)
	waitForOutboundGateways(t, s4, 3, 2*time.Second)

	s1.Shutdown()
	s2.Shutdown()
	s3.Shutdown()
	s4.Shutdown()

	o1 = testDefaultOptionsForGateway("A")
	s1 = runGatewayServer(o1)
	defer s1.Shutdown()

	o2 = testDefaultOptionsForGateway("A")
	o2.Routes = RoutesFromStr(fmt.Sprintf("nats://127.0.0.1:%d", s1.ClusterAddr().Port))
	s2 = runGatewayServer(o2)
	defer s2.Shutdown()

	o3 = testDefaultOptionsForGateway("A")
	o3.Routes = RoutesFromStr(fmt.Sprintf("nats://127.0.0.1:%d", s1.ClusterAddr().Port))
	s3 = runGatewayServer(o3)
	defer s3.Shutdown()

	checkClusterFormed(t, s1, s2, s3)

	o4 = testGatewayOptionsFromToWithServers(t, "B", "A", s1)
	s4 = runGatewayServer(o4)
	defer s4.Shutdown()

	waitForOutboundGateways(t, s1, 1, 2*time.Second)
	waitForOutboundGateways(t, s2, 1, 2*time.Second)
	waitForOutboundGateways(t, s3, 1, 2*time.Second)
	waitForOutboundGateways(t, s4, 1, 2*time.Second)
	waitForInboundGateways(t, s4, 3, 2*time.Second)

	o5 := testGatewayOptionsFromToWithServers(t, "C", "B", s4)
	s5 := runGatewayServer(o5)
	defer s5.Shutdown()

	waitForOutboundGateways(t, s1, 2, 2*time.Second)
	waitForOutboundGateways(t, s2, 2, 2*time.Second)
	waitForOutboundGateways(t, s3, 2, 2*time.Second)
	waitForOutboundGateways(t, s4, 2, 2*time.Second)
	waitForInboundGateways(t, s4, 4, 2*time.Second)
	waitForOutboundGateways(t, s5, 2, 2*time.Second)
	waitForInboundGateways(t, s5, 4, 2*time.Second)
}

func TestGatewayRejectUnknown(t *testing.T) {
	o2 := testDefaultOptionsForGateway("B")
	s2 := runGatewayServer(o2)
	defer s2.Shutdown()

	// Create a gateway from A to B, but configure B to reject non configured ones.
	o1 := testGatewayOptionsFromToWithServers(t, "A", "B", s2)
	o1.Gateway.RejectUnknown = true
	s1 := runGatewayServer(o1)
	defer s1.Shutdown()

	// Wait for outbound/inbound to be created.
	waitForOutboundGateways(t, s1, 1, time.Second)
	waitForOutboundGateways(t, s2, 1, time.Second)
	waitForInboundGateways(t, s1, 1, time.Second)
	waitForInboundGateways(t, s2, 1, time.Second)

	// Create gateway C to B. B will tell C to connect to A,
	// which A should reject.
	o3 := testGatewayOptionsFromToWithServers(t, "C", "B", s2)
	s3 := runGatewayServer(o3)
	defer s3.Shutdown()

	// s3 should have outbound to B, but not to A
	waitForOutboundGateways(t, s3, 1, time.Second)
	// s2 should have 2 inbounds (one from s1 one from s3)
	waitForInboundGateways(t, s2, 2, time.Second)

	// s1 should have single outbound/inbound with s2.
	waitForOutboundGateways(t, s1, 1, time.Second)
	waitForInboundGateways(t, s1, 1, time.Second)

	// It should not have a registered remote gateway with C (s3)
	if s1.getOutboundGatewayConnection("C") != nil {
		t.Fatalf("A should not have outbound gateway to C")
	}
	if s1.getRemoteGateway("C") != nil {
		t.Fatalf("A should not have a registered remote gateway to C")
	}

	// Restart s1 and this time, B will tell A to connect to C.
	// But A will not even attempt that since it does not have
	// C configured.
	s1.Shutdown()
	waitForOutboundGateways(t, s2, 1, time.Second)
	waitForInboundGateways(t, s2, 1, time.Second)
	s1 = runGatewayServer(o1)
	defer s1.Shutdown()
	waitForOutboundGateways(t, s2, 2, time.Second)
	waitForInboundGateways(t, s2, 2, time.Second)
	waitForOutboundGateways(t, s1, 1, time.Second)
	waitForInboundGateways(t, s1, 1, time.Second)
	waitForOutboundGateways(t, s3, 1, time.Second)
	waitForInboundGateways(t, s3, 1, time.Second)
	// It should not have a registered remote gateway with C (s3)
	if s1.getOutboundGatewayConnection("C") != nil {
		t.Fatalf("A should not have outbound gateway to C")
	}
	if s1.getRemoteGateway("C") != nil {
		t.Fatalf("A should not have a registered remote gateway to C")
	}
}

func TestGatewayNoReconnectOnClose(t *testing.T) {
	o2 := testDefaultOptionsForGateway("B")
	s2 := runGatewayServer(o2)
	defer s2.Shutdown()

	o1 := testGatewayOptionsFromToWithServers(t, "A", "B", s2)
	s1 := runGatewayServer(o1)
	defer s1.Shutdown()

	waitForOutboundGateways(t, s1, 1, time.Second)
	waitForOutboundGateways(t, s2, 1, time.Second)

	// Shutdown s1, and check that there is no attempt to reconnect.
	s1.Shutdown()
	time.Sleep(250 * time.Millisecond)
	waitForOutboundGateways(t, s1, 0, time.Second)
	waitForOutboundGateways(t, s2, 0, time.Second)
	waitForInboundGateways(t, s2, 0, time.Second)
}

func TestGatewayDontSendSubInterest(t *testing.T) {
	o2 := testDefaultOptionsForGateway("B")
	s2 := runGatewayServer(o2)
	defer s2.Shutdown()

	o1 := testGatewayOptionsFromToWithServers(t, "A", "B", s2)
	s1 := runGatewayServer(o1)
	defer s1.Shutdown()

	waitForOutboundGateways(t, s1, 1, time.Second)
	waitForOutboundGateways(t, s2, 1, time.Second)

	s2Url := fmt.Sprintf("nats://127.0.0.1:%d", o2.Port)
	subnc := natsConnect(t, s2Url)
	defer subnc.Close()
	natsSub(t, subnc, "foo", func(_ *nats.Msg) {})
	natsFlush(t, subnc)

	checkExpectedSubs(t, 1, s2)
	// Subscription should not be sent to s1
	checkExpectedSubs(t, 0, s1)

	// Restart s1
	s1.Shutdown()
	s1 = runGatewayServer(o1)
	defer s1.Shutdown()
	waitForOutboundGateways(t, s1, 1, time.Second)
	waitForOutboundGateways(t, s2, 1, time.Second)

	checkExpectedSubs(t, 1, s2)
	checkExpectedSubs(t, 0, s1)
}

func setAccountUserPassInOptions(o *Options, accName, username, password string) {
	acc := NewAccount(accName)
	o.Accounts = append(o.Accounts, acc)
	o.Users = append(o.Users, &User{Username: username, Password: password, Account: acc})
}

func TestGatewayAccountInterest(t *testing.T) {
	GatewayDoNotForceInterestOnlyMode(true)
	defer GatewayDoNotForceInterestOnlyMode(false)

	o2 := testDefaultOptionsForGateway("B")
	// Add users to cause s2 to require auth. Will add an account with user later.
	o2.Users = append([]*User(nil), &User{Username: "test", Password: "pwd"})
	s2 := runGatewayServer(o2)
	defer s2.Shutdown()

	o1 := testGatewayOptionsFromToWithServers(t, "A", "B", s2)
	setAccountUserPassInOptions(o1, "$foo", "ivan", "password")
	s1 := runGatewayServer(o1)
	defer s1.Shutdown()

	// Make this server initiate connection to A, so it is faster
	// when restarting it at the end of this test.
	o3 := testGatewayOptionsFromToWithServers(t, "C", "A", s1)
	setAccountUserPassInOptions(o3, "$foo", "ivan", "password")
	s3 := runGatewayServer(o3)
	defer s3.Shutdown()

	waitForOutboundGateways(t, s1, 2, time.Second)
	waitForOutboundGateways(t, s2, 2, time.Second)
	waitForOutboundGateways(t, s3, 2, time.Second)

	s1Url := fmt.Sprintf("nats://ivan:password@127.0.0.1:%d", o1.Port)
	nc := natsConnect(t, s1Url)
	defer nc.Close()
	natsPub(t, nc, "foo", []byte("hello"))
	natsFlush(t, nc)

	// On first send, the message should be sent.
	checkCount := func(t *testing.T, c *client, expected int) {
		t.Helper()
		c.mu.Lock()
		out := c.outMsgs
		c.mu.Unlock()
		if int(out) != expected {
			t.Fatalf("Expected %d message(s) to be sent over, got %v", expected, out)
		}
	}
	gwcb := s1.getOutboundGatewayConnection("B")
	checkCount(t, gwcb, 1)
	gwcc := s1.getOutboundGatewayConnection("C")
	checkCount(t, gwcc, 1)

	// S2 and S3 should have sent a protocol indicating no account interest.
	checkForAccountNoInterest(t, gwcb, "$foo", true, 2*time.Second)
	checkForAccountNoInterest(t, gwcc, "$foo", true, 2*time.Second)
	// Second send should not go to B nor C.
	natsPub(t, nc, "foo", []byte("hello"))
	natsFlush(t, nc)
	checkCount(t, gwcb, 1)
	checkCount(t, gwcc, 1)

	// Add account to S2 and a client, this should clear the no-interest
	// for that account.
	s2FooAcc, err := s2.RegisterAccount("$foo")
	if err != nil {
		t.Fatalf("Error registering account: %v", err)
	}
	s2.mu.Lock()
	s2.users["ivan"] = &User{Account: s2FooAcc, Username: "ivan", Password: "password"}
	s2.mu.Unlock()
	s2Url := fmt.Sprintf("nats://ivan:password@127.0.0.1:%d", o2.Port)
	ncS2 := natsConnect(t, s2Url)
	defer ncS2.Close()
	// Any subscription should cause s2 to send an A+
	natsSubSync(t, ncS2, "asub")
	// Wait for the A+
	checkForAccountNoInterest(t, gwcb, "$foo", false, 2*time.Second)

	// Now publish a message that should go to B
	natsPub(t, nc, "foo", []byte("hello"))
	natsFlush(t, nc)
	checkCount(t, gwcb, 2)
	// Still won't go to C since there is no sub interest
	checkCount(t, gwcc, 1)

	// We should have received a subject no interest for foo
	checkForSubjectNoInterest(t, gwcb, "$foo", "foo", true, 2*time.Second)

	// Now if we close the client, which removed the sole subscription,
	// and publish to a new subject, we should then get an A-
	ncS2.Close()
	// Wait a bit...
	time.Sleep(20 * time.Millisecond)
	// Publish on new subject
	natsPub(t, nc, "bar", []byte("hello"))
	natsFlush(t, nc)
	// It should go out to B...
	checkCount(t, gwcb, 3)
	// But then we should get a A-
	checkForAccountNoInterest(t, gwcb, "$foo", true, 2*time.Second)

	// Restart C and that should reset the no-interest
	s3.Shutdown()
	s3 = runGatewayServer(o3)
	defer s3.Shutdown()

	waitForOutboundGateways(t, s1, 2, 2*time.Second)
	waitForOutboundGateways(t, s2, 2, 2*time.Second)
	waitForOutboundGateways(t, s3, 2, 2*time.Second)

	// First refresh gwcc
	gwcc = s1.getOutboundGatewayConnection("C")
	// Verify that it's count is 0
	checkCount(t, gwcc, 0)
	// Publish and now...
	natsPub(t, nc, "foo", []byte("hello"))
	natsFlush(t, nc)
	// it should not go to B (no sub interest)
	checkCount(t, gwcb, 3)
	// but will go to C
	checkCount(t, gwcc, 1)
}

func TestGatewayAccountUnsub(t *testing.T) {
	ob := testDefaultOptionsForGateway("B")
	sb := runGatewayServer(ob)
	defer sb.Shutdown()

	oa := testGatewayOptionsFromToWithServers(t, "A", "B", sb)
	sa := runGatewayServer(oa)
	defer sa.Shutdown()

	waitForOutboundGateways(t, sa, 1, time.Second)
	waitForOutboundGateways(t, sb, 1, time.Second)
	waitForInboundGateways(t, sa, 1, time.Second)
	waitForInboundGateways(t, sb, 1, time.Second)

	// Connect on B
	ncb := natsConnect(t, fmt.Sprintf("nats://%s:%d", ob.Host, ob.Port))
	defer ncb.Close()
	// Create subscription
	natsSub(t, ncb, "foo", func(m *nats.Msg) {
		ncb.Publish(m.Reply, []byte("reply"))
	})
	natsFlush(t, ncb)

	// Connect on A
	nca := natsConnect(t, fmt.Sprintf("nats://%s:%d", oa.Host, oa.Port))
	defer nca.Close()
	// Send a request
	if _, err := nca.Request("foo", []byte("req"), time.Second); err != nil {
		t.Fatalf("Error getting reply: %v", err)
	}

	// Now close connection on B
	ncb.Close()

	// Publish lots of messages on "foo" from A.
	// We should receive an A- shortly and the number
	// of outbound messages from A to B should not be
	// close to the number of messages sent here.
	total := 5000
	for i := 0; i < total; i++ {
		natsPub(t, nca, "foo", []byte("hello"))
		// Try to slow down things a bit to give a chance
		// to srvB to send the A- and to srvA to be able
		// to process it, which will then suppress the sends.
		if i%100 == 0 {
			natsFlush(t, nca)
		}
	}
	natsFlush(t, nca)

	c := sa.getOutboundGatewayConnection("B")
	c.mu.Lock()
	out := c.outMsgs
	c.mu.Unlock()

	if out >= int64(80*total)/100 {
		t.Fatalf("Unexpected number of messages sent from A to B: %v", out)
	}
}

func TestGatewaySubjectInterest(t *testing.T) {
	GatewayDoNotForceInterestOnlyMode(true)
	defer GatewayDoNotForceInterestOnlyMode(false)

	o1 := testDefaultOptionsForGateway("A")
	setAccountUserPassInOptions(o1, "$foo", "ivan", "password")
	s1 := runGatewayServer(o1)
	defer s1.Shutdown()

	o2 := testGatewayOptionsFromToWithServers(t, "B", "A", s1)
	setAccountUserPassInOptions(o2, "$foo", "ivan", "password")
	s2 := runGatewayServer(o2)
	defer s2.Shutdown()

	waitForOutboundGateways(t, s1, 1, time.Second)
	waitForOutboundGateways(t, s2, 1, time.Second)

	// We will create a subscription that we are not testing so
	// that we don't get an A- in this test.
	s2Url := fmt.Sprintf("nats://ivan:password@127.0.0.1:%d", o2.Port)
	ncb := natsConnect(t, s2Url)
	defer ncb.Close()
	natsSubSync(t, ncb, "not.used")
	checkExpectedSubs(t, 1, s2)

	s1Url := fmt.Sprintf("nats://ivan:password@127.0.0.1:%d", o1.Port)
	nc := natsConnect(t, s1Url)
	defer nc.Close()
	natsPub(t, nc, "foo", []byte("hello"))
	natsFlush(t, nc)

	// On first send, the message should be sent.
	checkCount := func(t *testing.T, c *client, expected int) {
		t.Helper()
		c.mu.Lock()
		out := c.outMsgs
		c.mu.Unlock()
		if int(out) != expected {
			t.Fatalf("Expected %d message(s) to be sent over, got %v", expected, out)
		}
	}
	gwcb := s1.getOutboundGatewayConnection("B")
	checkCount(t, gwcb, 1)

	// S2 should have sent a protocol indicating no subject interest.
	checkNoInterest := func(t *testing.T, subject string, expectedNoInterest bool) {
		t.Helper()
		checkForSubjectNoInterest(t, gwcb, "$foo", subject, expectedNoInterest, 2*time.Second)
	}
	checkNoInterest(t, "foo", true)
	// Second send should not go through to B
	natsPub(t, nc, "foo", []byte("hello"))
	natsFlush(t, nc)
	checkCount(t, gwcb, 1)

	// Now create subscription interest on B (s2)
	ch := make(chan bool, 1)
	sub := natsSub(t, ncb, "foo", func(_ *nats.Msg) {
		ch <- true
	})
	natsFlush(t, ncb)
	checkExpectedSubs(t, 2, s2)
	checkExpectedSubs(t, 0, s1)

	// This should clear the no interest for this subject
	checkNoInterest(t, "foo", false)
	// Third send should go to B
	natsPub(t, nc, "foo", []byte("hello"))
	natsFlush(t, nc)
	checkCount(t, gwcb, 2)

	// Make sure message is received
	waitCh(t, ch, "Did not get our message")
	// Now unsubscribe, there won't be an UNSUB sent to the gateway.
	natsUnsub(t, sub)
	natsFlush(t, ncb)
	checkExpectedSubs(t, 1, s2)
	checkExpectedSubs(t, 0, s1)

	// So now sending a message should go over, but then we should get an RS-
	natsPub(t, nc, "foo", []byte("hello"))
	natsFlush(t, nc)
	checkCount(t, gwcb, 3)

	checkNoInterest(t, "foo", true)

	// Send one more time and now it should not go to B
	natsPub(t, nc, "foo", []byte("hello"))
	natsFlush(t, nc)
	checkCount(t, gwcb, 3)

	// Send on bar, message should go over.
	natsPub(t, nc, "bar", []byte("hello"))
	natsFlush(t, nc)
	checkCount(t, gwcb, 4)

	// But now we should have receives an RS- on bar.
	checkNoInterest(t, "bar", true)

	// Check that wildcards are supported. Create a subscription on '*' on B.
	// This should clear the no-interest on both "foo" and "bar"
	natsSub(t, ncb, "*", func(_ *nats.Msg) {})
	natsFlush(t, ncb)
	checkExpectedSubs(t, 2, s2)
	checkExpectedSubs(t, 0, s1)
	checkNoInterest(t, "foo", false)
	checkNoInterest(t, "bar", false)
	// Publish on message on foo and one on bar and they should go.
	natsPub(t, nc, "foo", []byte("hello"))
	natsPub(t, nc, "bar", []byte("hello"))
	natsFlush(t, nc)
	checkCount(t, gwcb, 6)

	// Restart B and that should clear everything on A
	ncb.Close()
	s2.Shutdown()
	s2 = runGatewayServer(o2)
	defer s2.Shutdown()

	waitForOutboundGateways(t, s1, 1, time.Second)
	waitForOutboundGateways(t, s2, 1, time.Second)

	ncb = natsConnect(t, s2Url)
	defer ncb.Close()
	natsSubSync(t, ncb, "not.used")
	checkExpectedSubs(t, 1, s2)

	gwcb = s1.getOutboundGatewayConnection("B")
	checkCount(t, gwcb, 0)
	natsPub(t, nc, "foo", []byte("hello"))
	natsFlush(t, nc)
	checkCount(t, gwcb, 1)

	checkNoInterest(t, "foo", true)

	natsPub(t, nc, "foo", []byte("hello"))
	natsFlush(t, nc)
	checkCount(t, gwcb, 1)

	// Add a node to B cluster and subscribe there.
	// We want to ensure that the no-interest is cleared
	// when s2 receives remote SUB from s2bis
	o2bis := testGatewayOptionsFromToWithServers(t, "B", "A", s1)
	setAccountUserPassInOptions(o2bis, "$foo", "ivan", "password")
	o2bis.Routes = RoutesFromStr(fmt.Sprintf("nats://127.0.0.1:%d", s2.ClusterAddr().Port))
	s2bis := runGatewayServer(o2bis)
	defer s2bis.Shutdown()

	checkClusterFormed(t, s2, s2bis)

	// Make sure all outbound gateway connections are setup
	waitForOutboundGateways(t, s1, 1, time.Second)
	waitForOutboundGateways(t, s2, 1, time.Second)
	waitForOutboundGateways(t, s2bis, 1, time.Second)

	// A should have 2 inbound
	waitForInboundGateways(t, s1, 2, time.Second)

	// Create sub on s2bis
	ncb2bis := natsConnect(t, fmt.Sprintf("nats://ivan:password@127.0.0.1:%d", o2bis.Port))
	defer ncb2bis.Close()
	natsSub(t, ncb2bis, "foo", func(_ *nats.Msg) {})
	natsFlush(t, ncb2bis)

	// Wait for subscriptions to be registered locally on s2bis and remotely on s2
	checkExpectedSubs(t, 2, s2, s2bis)

	// Check that subject no-interest on A was cleared.
	checkNoInterest(t, "foo", false)

	// Now publish. Remember, s1 has outbound gateway to s2, and s2 does not
	// have a local subscription and has previously sent a no-interest on "foo".
	// We check that this has been cleared due to the interest on s2bis.
	natsPub(t, nc, "foo", []byte("hello"))
	natsFlush(t, nc)
	checkCount(t, gwcb, 2)
}

func TestGatewayDoesntSendBackToItself(t *testing.T) {
	o2 := testDefaultOptionsForGateway("B")
	s2 := runGatewayServer(o2)
	defer s2.Shutdown()

	o1 := testGatewayOptionsFromToWithServers(t, "A", "B", s2)
	s1 := runGatewayServer(o1)
	defer s1.Shutdown()

	waitForOutboundGateways(t, s1, 1, time.Second)
	waitForOutboundGateways(t, s2, 1, time.Second)

	s2Url := fmt.Sprintf("nats://127.0.0.1:%d", o2.Port)
	nc2 := natsConnect(t, s2Url)
	defer nc2.Close()

	count := int32(0)
	cb := func(_ *nats.Msg) {
		atomic.AddInt32(&count, 1)
	}
	natsSub(t, nc2, "foo", cb)
	natsFlush(t, nc2)

	s1Url := fmt.Sprintf("nats://127.0.0.1:%d", o1.Port)
	nc1 := natsConnect(t, s1Url)
	defer nc1.Close()

	natsSub(t, nc1, "foo", cb)
	natsFlush(t, nc1)

	// Now send 1 message. If there is a cycle, after few ms we
	// should have tons of messages...
	natsPub(t, nc1, "foo", []byte("cycle"))
	natsFlush(t, nc1)
	time.Sleep(100 * time.Millisecond)
	if c := atomic.LoadInt32(&count); c != 2 {
		t.Fatalf("Expected only 2 messages, got %v", c)
	}
}

func TestGatewayOrderedOutbounds(t *testing.T) {
	o2 := testDefaultOptionsForGateway("B")
	s2 := runGatewayServer(o2)
	defer s2.Shutdown()

	o1 := testGatewayOptionsFromToWithServers(t, "A", "B", s2)
	s1 := runGatewayServer(o1)
	defer s1.Shutdown()

	o3 := testGatewayOptionsFromToWithServers(t, "C", "B", s2)
	s3 := runGatewayServer(o3)
	defer s3.Shutdown()

	waitForOutboundGateways(t, s1, 2, time.Second)
	waitForOutboundGateways(t, s2, 2, time.Second)
	waitForOutboundGateways(t, s3, 2, time.Second)

	gws := make([]*client, 0, 2)
	s2.getOutboundGatewayConnections(&gws)

	// RTTs are expected to be initially 0. So update RTT of first
	// in the array so that its value is no longer 0, this should
	// cause order to be flipped.
	c := gws[0]
	c.mu.Lock()
	c.sendPing()
	c.mu.Unlock()

	// Wait a tiny but
	time.Sleep(15 * time.Millisecond)
	// Get the ordering again.
	gws = gws[:0]
	s2.getOutboundGatewayConnections(&gws)
	// Verify order is correct.
	fRTT := gws[0].getRTTValue()
	sRTT := gws[1].getRTTValue()
	if fRTT > sRTT {
		t.Fatalf("Wrong ordering: %v, %v", fRTT, sRTT)
	}

	// What is the first in the array?
	gws[0].mu.Lock()
	gwName := gws[0].gw.name
	gws[0].mu.Unlock()
	if gwName == "A" {
		s1.Shutdown()
	} else {
		s3.Shutdown()
	}
	waitForOutboundGateways(t, s2, 1, time.Second)
	gws = gws[:0]
	s2.getOutboundGatewayConnections(&gws)
	if len(gws) != 1 {
		t.Fatalf("Expected size of outo to be 1, got %v", len(gws))
	}
	gws[0].mu.Lock()
	name := gws[0].gw.name
	gws[0].mu.Unlock()
	if gwName == name {
		t.Fatalf("Gateway %q should have been removed", gwName)
	}
	// Stop the remaining gateway
	if gwName == "A" {
		s3.Shutdown()
	} else {
		s1.Shutdown()
	}
	waitForOutboundGateways(t, s2, 0, time.Second)
	gws = gws[:0]
	s2.getOutboundGatewayConnections(&gws)
	if len(gws) != 0 {
		t.Fatalf("Expected size of outo to be 0, got %v", len(gws))
	}
}

func TestGatewayQueueSub(t *testing.T) {
	o2 := testDefaultOptionsForGateway("B")
	s2 := runGatewayServer(o2)
	defer s2.Shutdown()

	o1 := testGatewayOptionsFromToWithServers(t, "A", "B", s2)
	s1 := runGatewayServer(o1)
	defer s1.Shutdown()

	waitForOutboundGateways(t, s1, 1, time.Second)
	waitForOutboundGateways(t, s2, 1, time.Second)

	sBUrl := fmt.Sprintf("nats://127.0.0.1:%d", o2.Port)
	ncB := natsConnect(t, sBUrl)
	defer ncB.Close()

	count2 := int32(0)
	cb2 := func(_ *nats.Msg) {
		atomic.AddInt32(&count2, 1)
	}
	qsubOnB := natsQueueSub(t, ncB, "foo", "bar", cb2)
	natsFlush(t, ncB)

	sAUrl := fmt.Sprintf("nats://127.0.0.1:%d", o1.Port)
	ncA := natsConnect(t, sAUrl)
	defer ncA.Close()

	count1 := int32(0)
	cb1 := func(_ *nats.Msg) {
		atomic.AddInt32(&count1, 1)
	}
	qsubOnA := natsQueueSub(t, ncA, "foo", "bar", cb1)
	natsFlush(t, ncA)

	// Make sure subs are registered on each server
	checkExpectedSubs(t, 1, s1, s2)
	checkForRegisteredQSubInterest(t, s1, "B", globalAccountName, "foo", 1, time.Second)
	checkForRegisteredQSubInterest(t, s2, "A", globalAccountName, "foo", 1, time.Second)

	total := 100
	send := func(t *testing.T, nc *nats.Conn) {
		t.Helper()
		for i := 0; i < total; i++ {
			// Alternate with adding a reply
			if i%2 == 0 {
				natsPubReq(t, nc, "foo", "reply", []byte("msg"))
			} else {
				natsPub(t, nc, "foo", []byte("msg"))
			}
		}
		natsFlush(t, nc)
	}
	// Send from client connecting to S1 (cluster A)
	send(t, ncA)

	check := func(t *testing.T, count *int32, expected int) {
		t.Helper()
		checkFor(t, 2*time.Second, 15*time.Millisecond, func() error {
			if n := int(atomic.LoadInt32(count)); n != expected {
				return fmt.Errorf("Expected to get %v messages, got %v", expected, n)
			}
			return nil
		})
	}
	// Check that all messages stay on S1 (cluster A)
	check(t, &count1, total)
	check(t, &count2, 0)

	// Now send from the other side
	send(t, ncB)
	check(t, &count1, total)
	check(t, &count2, total)

	// Reset counters
	atomic.StoreInt32(&count1, 0)
	atomic.StoreInt32(&count2, 0)

	// Add different queue group and make sure that messages are received
	count3 := int32(0)
	cb3 := func(_ *nats.Msg) {
		atomic.AddInt32(&count3, 1)
	}
	batQSub := natsQueueSub(t, ncB, "foo", "bat", cb3)
	natsFlush(t, ncB)
	checkExpectedSubs(t, 2, s2)

	checkForRegisteredQSubInterest(t, s1, "B", globalAccountName, "foo", 2, time.Second)

	send(t, ncA)
	check(t, &count1, total)
	check(t, &count2, 0)
	check(t, &count3, total)

	// Reset counters
	atomic.StoreInt32(&count1, 0)
	atomic.StoreInt32(&count2, 0)
	atomic.StoreInt32(&count3, 0)

	natsUnsub(t, batQSub)
	natsFlush(t, ncB)
	checkExpectedSubs(t, 1, s2)

	checkForRegisteredQSubInterest(t, s1, "B", globalAccountName, "foo", 1, time.Second)

	// Stop qsub on A, and send messages to A, they should
	// be routed to B.
	qsubOnA.Unsubscribe()
	checkExpectedSubs(t, 0, s1)
	send(t, ncA)
	check(t, &count1, 0)
	check(t, &count2, total)

	// Reset counters
	atomic.StoreInt32(&count1, 0)
	atomic.StoreInt32(&count2, 0)

	// Create a C gateway now
	o3 := testGatewayOptionsFromToWithServers(t, "C", "B", s2)
	s3 := runGatewayServer(o3)
	defer s3.Shutdown()

	waitForOutboundGateways(t, s1, 2, time.Second)
	waitForOutboundGateways(t, s2, 2, time.Second)
	waitForOutboundGateways(t, s3, 2, time.Second)

	waitForInboundGateways(t, s1, 2, time.Second)
	waitForInboundGateways(t, s2, 2, time.Second)
	waitForInboundGateways(t, s3, 2, time.Second)

	// Create another qsub "bar"
	sCUrl := fmt.Sprintf("nats://127.0.0.1:%d", o3.Port)
	ncC := natsConnect(t, sCUrl)
	defer ncC.Close()
	// Associate this with count1 (since A qsub is no longer running)
	natsQueueSub(t, ncC, "foo", "bar", cb1)
	natsFlush(t, ncC)
	checkExpectedSubs(t, 1, s3)
	checkForRegisteredQSubInterest(t, s1, "C", globalAccountName, "foo", 1, time.Second)
	checkForRegisteredQSubInterest(t, s2, "C", globalAccountName, "foo", 1, time.Second)

	// Artificially bump the RTT from A to C so that
	// the code should favor sending to B.
	gwcC := s1.getOutboundGatewayConnection("C")
	gwcC.mu.Lock()
	gwcC.rtt = 10 * time.Second
	gwcC.mu.Unlock()
	s1.gateway.orderOutboundConnections()

	send(t, ncA)
	check(t, &count1, 0)
	check(t, &count2, total)

	// Add a new group on s3 that should receive all messages
	natsQueueSub(t, ncC, "foo", "baz", cb3)
	natsFlush(t, ncC)
	checkExpectedSubs(t, 2, s3)
	checkForRegisteredQSubInterest(t, s1, "C", globalAccountName, "foo", 2, time.Second)
	checkForRegisteredQSubInterest(t, s2, "C", globalAccountName, "foo", 2, time.Second)

	// Reset counters
	atomic.StoreInt32(&count1, 0)
	atomic.StoreInt32(&count2, 0)

	// Make the RTTs equal
	gwcC.mu.Lock()
	gwcC.rtt = time.Second
	gwcC.mu.Unlock()

	gwcB := s1.getOutboundGatewayConnection("B")
	gwcB.mu.Lock()
	gwcB.rtt = time.Second
	gwcB.mu.Unlock()

	s1.gateway.Lock()
	s1.gateway.orderOutboundConnectionsLocked()
	destName := s1.gateway.outo[0].gw.name
	s1.gateway.Unlock()

	send(t, ncA)
	// Group baz should receive all messages
	check(t, &count3, total)

	// Ordering is normally re-evaluated when processing PONGs,
	// but rest of the time order will remain the same.
	// Since RTT are equal, messages will go to the first
	// GW in the array.
	if destName == "B" {
		check(t, &count2, total)
	} else if destName == "C" && int(atomic.LoadInt32(&count2)) != total {
		check(t, &count1, total)
	}

	// Unsubscribe qsub on B and C should receive
	// all messages on count1 and count3.
	qsubOnB.Unsubscribe()
	checkExpectedSubs(t, 0, s2)

	// gwcB should have the qsubs interest map empty now.
	checkFor(t, 2*time.Second, 15*time.Millisecond, func() error {
		ei, _ := gwcB.gw.outsim.Load(globalAccountName)
		if ei != nil {
			sl := ei.(*outsie).sl
			if sl.Count() == 0 {
				return nil
			}
		}
		return fmt.Errorf("Qsub interest for account should have been removed")
	})

	// Reset counters
	atomic.StoreInt32(&count1, 0)
	atomic.StoreInt32(&count2, 0)
	atomic.StoreInt32(&count3, 0)

	send(t, ncA)
	check(t, &count1, total)
	check(t, &count3, total)
}

func TestGatewayQueueSubNotSuppressedByDeniedLeafQueue(t *testing.T) {
	ob := testDefaultOptionsForGateway("B")
	sb := runGatewayServer(ob)
	defer sb.Shutdown()

	oa := testGatewayOptionsFromToWithServers(t, "A", "B", sb)
	oa.NoSystemAccount = false
	oa.Accounts = []*Account{NewAccount("SYS")}
	oa.SystemAccount = "SYS"
	oa.LeafNode.Host = "127.0.0.1"
	oa.LeafNode.Port = -1
	oa.Users = []*User{
		{
			Username: "leaf",
			Password: "pwd",
			Permissions: &Permissions{
				Publish: &SubjectPermission{Allow: []string{">"}},
				Subscribe: &SubjectPermission{
					Allow: []string{">"},
					Deny:  []string{"admin.secret workers"},
				},
			},
		},
	}
	sa := runGatewayServer(oa)
	defer sa.Shutdown()

	waitForOutboundGateways(t, sa, 1, time.Second)
	waitForOutboundGateways(t, sb, 1, time.Second)

	leafURL, err := url.Parse(fmt.Sprintf("nats://leaf:pwd@127.0.0.1:%d", oa.LeafNode.Port))
	require_NoError(t, err)
	ol := DefaultOptions()
	ol.LeafNode.Remotes = []*RemoteLeafOpts{{URLs: []*url.URL{leafURL}}}
	sl := RunServer(ol)
	defer sl.Shutdown()
	checkLeafNodeConnected(t, sa)
	checkLeafNodeConnected(t, sl)

	ncLeaf := natsConnect(t, sl.ClientURL())
	defer ncLeaf.Close()
	leafSub := natsQueueSubSync(t, ncLeaf, "admin.secret", "workers")
	natsFlush(t, ncLeaf)

	ncB := natsConnect(t, sb.ClientURL())
	defer ncB.Close()
	gatewaySub := natsQueueSubSync(t, ncB, "admin.secret", "workers")
	natsFlush(t, ncB)

	checkForRegisteredQSubInterest(t, sa, "B", globalAccountName, "admin.secret", 1, time.Second)
	checkFor(t, time.Second, 10*time.Millisecond, func() error {
		if !sa.globalAccount().sl.HasInterest("admin.secret") {
			return fmt.Errorf("leaf queue interest has not propagated")
		}
		return nil
	})
	checkFor(t, time.Second, 10*time.Millisecond, func() error {
		sa.mu.Lock()
		leafs := make([]*client, 0, len(sa.leafs))
		for _, lc := range sa.leafs {
			leafs = append(leafs, lc)
		}
		sa.mu.Unlock()
		for _, lc := range leafs {
			lc.mu.Lock()
			isGlobal := lc.acc != nil && lc.acc.Name == globalAccountName
			denied := isGlobal && lc.mperms != nil && lc.checkDenySub("admin.secret", "workers")
			lc.mu.Unlock()
			if denied {
				return nil
			}
		}
		return fmt.Errorf("leaf queue deny filter has not been initialized")
	})

	ncA := natsConnect(t, sa.ClientURL())
	defer ncA.Close()
	natsPub(t, ncA, "admin.secret", []byte("gateway"))
	natsFlush(t, ncA)

	msg := natsNexMsg(t, gatewaySub, time.Second)
	require_Equal(t, "gateway", string(msg.Data))
	if _, err := leafSub.NextMsg(100 * time.Millisecond); err != nats.ErrTimeout {
		t.Fatalf("Expected leaf queue subscription to be denied, got %v", err)
	}
}

func TestGatewayTotalQSubs(t *testing.T) {
	ob1 := testDefaultOptionsForGateway("B")
	sb1 := runGatewayServer(ob1)
	defer sb1.Shutdown()

	ob2 := testDefaultOptionsForGateway("B")
	ob2.Routes = RoutesFromStr(fmt.Sprintf("nats://127.0.0.1:%d", sb1.ClusterAddr().Port))
	sb2 := runGatewayServer(ob2)
	defer sb2.Shutdown()

	checkClusterFormed(t, sb1, sb2)

	sb1URL := fmt.Sprintf("nats://%s:%d", ob1.Host, ob1.Port)
	ncb1 := natsConnect(t, sb1URL, nats.ReconnectWait(50*time.Millisecond))
	defer ncb1.Close()

	sb2URL := fmt.Sprintf("nats://%s:%d", ob2.Host, ob2.Port)
	ncb2 := natsConnect(t, sb2URL, nats.ReconnectWait(50*time.Millisecond))
	defer ncb2.Close()

	oa := testGatewayOptionsFromToWithServers(t, "A", "B", sb1)
	sa := runGatewayServer(oa)
	defer sa.Shutdown()

	waitForOutboundGateways(t, sa, 1, 2*time.Second)
	waitForOutboundGateways(t, sb1, 1, 2*time.Second)
	waitForOutboundGateways(t, sb2, 1, 2*time.Second)
	waitForInboundGateways(t, sa, 2, 2*time.Second)
	waitForInboundGateways(t, sb1, 1, 2*time.Second)

	checkTotalQSubs := func(t *testing.T, s *Server, expected int) {
		t.Helper()
		checkFor(t, time.Second, 15*time.Millisecond, func() error {
			if n := int(atomic.LoadInt64(&s.gateway.totalQSubs)); n != expected {
				return fmt.Errorf("Expected TotalQSubs to be %v, got %v", expected, n)
			}
			return nil
		})
	}

	cb := func(_ *nats.Msg) {}

	natsQueueSub(t, ncb1, "foo", "bar", cb)
	checkTotalQSubs(t, sa, 1)
	qsub2 := natsQueueSub(t, ncb1, "foo", "baz", cb)
	checkTotalQSubs(t, sa, 2)
	qsub3 := natsQueueSub(t, ncb1, "foo", "baz", cb)
	checkTotalQSubs(t, sa, 2)

	// Shutdown sb1, there should be a failover from clients
	// to sb2. sb2 will then send the queue subs to sa.
	sb1.Shutdown()

	checkClientsCount(t, sb2, 2)
	checkExpectedSubs(t, 3, sb2)

	waitForOutboundGateways(t, sa, 1, 2*time.Second)
	waitForOutboundGateways(t, sb2, 1, 2*time.Second)
	waitForInboundGateways(t, sa, 1, 2*time.Second)
	waitForInboundGateways(t, sb2, 1, 2*time.Second)

	// When sb1 is shutdown, the total qsubs on sa should fall
	// down to 0, but will be updated as soon as sa and sb2
	// connect to each other. So instead we will verify by
	// making sure that the count is 2 instead of 4 if there
	// was a bug.
	// (note that there are 2 qsubs on same group, so only
	// 1 RS+ would have been sent for that group)
	checkTotalQSubs(t, sa, 2)

	// Restart sb1
	sb1 = runGatewayServer(ob1)
	defer sb1.Shutdown()

	checkClusterFormed(t, sb1, sb2)

	waitForOutboundGateways(t, sa, 1, 2*time.Second)
	waitForOutboundGateways(t, sb1, 1, 2*time.Second)
	waitForOutboundGateways(t, sb2, 1, 2*time.Second)
	waitForInboundGateways(t, sa, 2, 2*time.Second)
	waitForInboundGateways(t, sb1, 0, 2*time.Second)
	waitForInboundGateways(t, sb2, 1, 2*time.Second)

	// Now start unsubscribing. Start with one of the duplicate
	// and check that count stays same.
	natsUnsub(t, qsub3)
	checkTotalQSubs(t, sa, 2)
	// Now the other, which would cause an RS-
	natsUnsub(t, qsub2)
	checkTotalQSubs(t, sa, 1)
	// Now test that if connections are closed, things are updated
	// properly.
	ncb1.Close()
	ncb2.Close()
	checkTotalQSubs(t, sa, 0)
}

func TestGatewaySendQSubsOnGatewayConnect(t *testing.T) {
	o2 := testDefaultOptionsForGateway("B")
	s2 := runGatewayServer(o2)
	defer s2.Shutdown()

	s2Url := fmt.Sprintf("nats://127.0.0.1:%d", o2.Port)
	subnc := natsConnect(t, s2Url)
	defer subnc.Close()

	ch := make(chan bool, 1)
	cb := func(_ *nats.Msg) {
		ch <- true
	}
	natsQueueSub(t, subnc, "foo", "bar", cb)
	natsFlush(t, subnc)

	// Now start s1 that creates a gateway to s2
	o1 := testGatewayOptionsFromToWithServers(t, "A", "B", s2)
	s1 := runGatewayServer(o1)
	defer s1.Shutdown()

	waitForOutboundGateways(t, s1, 1, time.Second)
	waitForOutboundGateways(t, s2, 1, time.Second)

	checkForRegisteredQSubInterest(t, s1, "B", globalAccountName, "foo", 1, time.Second)

	// Publish from s1, message should be received on s2.
	pubnc := natsConnect(t, fmt.Sprintf("nats://127.0.0.1:%d", o1.Port))
	defer pubnc.Close()
	// Publish 1 message
	natsPub(t, pubnc, "foo", []byte("hello"))
	waitCh(t, ch, "Did not get out message")
	pubnc.Close()

	s1.Shutdown()
	s1 = runGatewayServer(o1)
	defer s1.Shutdown()

	waitForOutboundGateways(t, s1, 1, time.Second)
	waitForOutboundGateways(t, s2, 1, time.Second)

	checkForRegisteredQSubInterest(t, s1, "B", globalAccountName, "foo", 1, time.Second)

	pubnc = natsConnect(t, fmt.Sprintf("nats://127.0.0.1:%d", o1.Port))
	defer pubnc.Close()
	// Publish 1 message
	natsPub(t, pubnc, "foo", []byte("hello"))
	waitCh(t, ch, "Did not get out message")
}

func TestGatewaySendRemoteQSubs(t *testing.T) {
	GatewayDoNotForceInterestOnlyMode(true)
	defer GatewayDoNotForceInterestOnlyMode(false)

	ob1 := testDefaultOptionsForGateway("B")
	sb1 := runGatewayServer(ob1)
	defer sb1.Shutdown()

	ob2 := testDefaultOptionsForGateway("B")
	ob2.Routes = RoutesFromStr(fmt.Sprintf("nats://%s:%d", ob1.Cluster.Host, ob1.Cluster.Port))
	sb2 := runGatewayServer(ob2)
	defer sb2.Shutdown()

	checkClusterFormed(t, sb1, sb2)

	sbURL := fmt.Sprintf("nats://127.0.0.1:%d", ob2.Port)
	subnc := natsConnect(t, sbURL)
	defer subnc.Close()

	ch := make(chan bool, 1)
	cb := func(_ *nats.Msg) {
		ch <- true
	}
	qsub1 := natsQueueSub(t, subnc, "foo", "bar", cb)
	qsub2 := natsQueueSub(t, subnc, "foo", "bar", cb)
	natsFlush(t, subnc)

	// There will be 2 local qsubs on the sb2 server where the client is connected
	checkExpectedSubs(t, 2, sb2)
	// But only 1 remote on sb1
	checkExpectedSubs(t, 1, sb1)

	// Now start s1 that creates a gateway to sb1 (the one that does not have the local QSub)
	oa := testGatewayOptionsFromToWithServers(t, "A", "B", sb1)
	sa := runGatewayServer(oa)
	defer sa.Shutdown()

	waitForOutboundGateways(t, sa, 1, time.Second)
	waitForOutboundGateways(t, sb1, 1, time.Second)
	waitForOutboundGateways(t, sb2, 1, time.Second)

	checkForRegisteredQSubInterest(t, sa, "B", globalAccountName, "foo", 1, time.Second)

	// Publish from s1, message should be received on s2.
	saURL := fmt.Sprintf("nats://127.0.0.1:%d", oa.Port)
	pubnc := natsConnect(t, saURL)
	defer pubnc.Close()
	// Publish 1 message
	natsPub(t, pubnc, "foo", []byte("hello"))
	natsFlush(t, pubnc)
	waitCh(t, ch, "Did not get out message")

	// Note that since cluster B has no plain sub, an "RS- $G foo" will have been sent.
	// Wait for the no interest to be received by A
	checkFor(t, time.Second, 15*time.Millisecond, func() error {
		gw := sa.getOutboundGatewayConnection("B").gw
		ei, _ := gw.outsim.Load(globalAccountName)
		if ei != nil {
			e := ei.(*outsie)
			e.RLock()
			defer e.RUnlock()
			if _, inMap := e.ni["foo"]; inMap {
				return nil
			}
		}
		return fmt.Errorf("No-interest still not registered")
	})

	// Unsubscribe 1 qsub
	natsUnsub(t, qsub1)
	natsFlush(t, subnc)
	// There should be only 1 local qsub on sb2 now, and the remote should still exist on sb1
	checkExpectedSubs(t, 1, sb1, sb2)

	// Publish 1 message
	natsPub(t, pubnc, "foo", []byte("hello"))
	natsFlush(t, pubnc)
	waitCh(t, ch, "Did not get out message")

	// Unsubscribe the remaining
	natsUnsub(t, qsub2)
	natsFlush(t, subnc)

	// No more subs now on both sb1 and sb2
	checkExpectedSubs(t, 0, sb1, sb2)

	// Server sb1 should not have qsub in its sub interest map
	checkFor(t, time.Second, 15*time.Millisecond, func() error {
		var entry *sitally
		var err error
		sb1.gateway.pasi.Lock()
		asim := sb1.gateway.pasi.m[globalAccountName]
		if asim != nil {
			entry = asim["foo bar"]
		}
		if entry != nil {
			err = fmt.Errorf("Map should not have an entry, got %#v", entry)
		}
		sb1.gateway.pasi.Unlock()
		return err
	})

	// Let's wait for A to receive the unsubscribe
	checkFor(t, time.Second, 15*time.Millisecond, func() error {
		gw := sa.getOutboundGatewayConnection("B").gw
		ei, _ := gw.outsim.Load(globalAccountName)
		if ei != nil {
			sl := ei.(*outsie).sl
			if sl.Count() == 0 {
				return nil
			}
		}
		return fmt.Errorf("Interest still present")
	})

	// Now send a message, it won't be sent because A received an RS-
	// on the first published message since there was no plain sub interest.
	natsPub(t, pubnc, "foo", []byte("hello"))
	natsFlush(t, pubnc)

	// Get the gateway connection from A (sa) to B (sb1)
	gw := sa.getOutboundGatewayConnection("B")
	gw.mu.Lock()
	out := gw.outMsgs
	gw.mu.Unlock()
	if out != 2 {
		t.Fatalf("Expected 2 out messages, got %v", out)
	}

	// Restart A
	pubnc.Close()
	sa.Shutdown()
	sa = runGatewayServer(oa)
	defer sa.Shutdown()

	waitForOutboundGateways(t, sa, 1, time.Second)

	// Check qsubs interest should be empty
	checkFor(t, time.Second, 15*time.Millisecond, func() error {
		gw := sa.getOutboundGatewayConnection("B").gw
		if ei, _ := gw.outsim.Load(globalAccountName); ei == nil {
			return nil
		}
		return fmt.Errorf("Interest still present")
	})
}

func TestGatewayComplexSetup(t *testing.T) {
	doLog := false

	// This test will have the following setup:
	// --- means route connection
	// === means gateway connection
	// [o] is outbound
	// [i] is inbound
	// Each server as an outbound connection to the other cluster.
	// It may have 0 or more inbound connection(s).
	//
	//  Cluster A		Cluster B
	//   sa1 [o]===========>[i]
	//    |  [i]<===========[o]
	//    |            		sb1 ------- sb2
	//    |					[i]			[o]
	//   sa2 [o]=============^   		 ||
	//		 [i]<========================||
	ob1 := testDefaultOptionsForGateway("B")
	sb1 := runGatewayServer(ob1)
	defer sb1.Shutdown()
	if doLog {
		sb1.SetLogger(logger.NewTestLogger("[B1] - ", true), true, true)
	}

	oa1 := testGatewayOptionsFromToWithServers(t, "A", "B", sb1)
	sa1 := runGatewayServer(oa1)
	defer sa1.Shutdown()
	if doLog {
		sa1.SetLogger(logger.NewTestLogger("[A1] - ", true), true, true)
	}

	waitForOutboundGateways(t, sa1, 1, time.Second)
	waitForOutboundGateways(t, sb1, 1, time.Second)

	waitForInboundGateways(t, sa1, 1, time.Second)
	waitForInboundGateways(t, sb1, 1, time.Second)

	oa2 := testGatewayOptionsFromToWithServers(t, "A", "B", sb1)
	oa2.Routes = RoutesFromStr(fmt.Sprintf("nats://127.0.0.1:%d", sa1.ClusterAddr().Port))
	sa2 := runGatewayServer(oa2)
	defer sa2.Shutdown()
	if doLog {
		sa2.SetLogger(logger.NewTestLogger("[A2] - ", true), true, true)
	}

	checkClusterFormed(t, sa1, sa2)

	waitForOutboundGateways(t, sa2, 1, time.Second)
	waitForInboundGateways(t, sb1, 2, time.Second)

	ob2 := testGatewayOptionsFromToWithServers(t, "B", "A", sa2)
	ob2.Routes = RoutesFromStr(fmt.Sprintf("nats://127.0.0.1:%d", sb1.ClusterAddr().Port))
	var sb2 *Server
	for {
		sb2 = runGatewayServer(ob2)
		defer sb2.Shutdown()

		checkClusterFormed(t, sb1, sb2)

		waitForOutboundGateways(t, sb2, 1, time.Second)
		waitForInboundGateways(t, sb2, 0, time.Second)
		// For this test, we want the outbound to be to sa2, so if we don't have that,
		// restart sb2 until we get lucky.
		time.Sleep(100 * time.Millisecond)
		if sa2.numInboundGateways() == 0 {
			sb2.Shutdown()
			sb2 = nil
		} else {
			break
		}
	}
	if doLog {
		sb2.SetLogger(logger.NewTestLogger("[B2] - ", true), true, true)
	}

	ch := make(chan bool, 1)
	cb := func(_ *nats.Msg) {
		ch <- true
	}

	// Create a subscription on sa1 and sa2.
	ncsa1 := natsConnect(t, fmt.Sprintf("nats://127.0.0.1:%d", oa1.Port))
	defer ncsa1.Close()
	sub1 := natsSub(t, ncsa1, "foo", cb)
	natsFlush(t, ncsa1)

	ncsa2 := natsConnect(t, fmt.Sprintf("nats://127.0.0.1:%d", oa2.Port))
	defer ncsa2.Close()
	sub2 := natsSub(t, ncsa2, "foo", cb)
	natsFlush(t, ncsa2)

	// sa1 will have 1 local, one remote (from sa2), same for sa2.
	checkExpectedSubs(t, 2, sa1, sa2)

	// Connect to sb2 and send 1 message
	ncsb2 := natsConnect(t, fmt.Sprintf("nats://127.0.0.1:%d", ob2.Port))
	defer ncsb2.Close()
	natsPub(t, ncsb2, "foo", []byte("hello"))
	natsFlush(t, ncsb2)

	for i := 0; i < 2; i++ {
		waitCh(t, ch, "Did not get our message")
	}

	// Unsubscribe sub2, and send 1, should still get it.
	natsUnsub(t, sub2)
	natsFlush(t, ncsa2)
	natsPub(t, ncsb2, "foo", []byte("hello"))
	natsFlush(t, ncsb2)
	waitCh(t, ch, "Did not get our message")

	// Unsubscribe sub1, all server's sublist should be empty
	sub1.Unsubscribe()
	natsFlush(t, ncsa1)

	checkExpectedSubs(t, 0, sa1, sa2, sb1, sb2)

	// Create queue subs
	total := 100
	c1 := int32(0)
	c2 := int32(0)
	c3 := int32(0)
	tc := int32(0)
	natsQueueSub(t, ncsa1, "foo", "bar", func(_ *nats.Msg) {
		atomic.AddInt32(&c1, 1)
		if c := atomic.AddInt32(&tc, 1); int(c) == total {
			ch <- true
		}
	})
	natsFlush(t, ncsa1)
	natsQueueSub(t, ncsa2, "foo", "bar", func(_ *nats.Msg) {
		atomic.AddInt32(&c2, 1)
		if c := atomic.AddInt32(&tc, 1); int(c) == total {
			ch <- true
		}
	})
	natsFlush(t, ncsa2)
	checkExpectedSubs(t, 2, sa1, sa2)

	qsubOnB2 := natsQueueSub(t, ncsb2, "foo", "bar", func(_ *nats.Msg) {
		atomic.AddInt32(&c3, 1)
		if c := atomic.AddInt32(&tc, 1); int(c) == total {
			ch <- true
		}
	})
	natsFlush(t, ncsb2)
	checkExpectedSubs(t, 1, sb2)

	checkForRegisteredQSubInterest(t, sb1, "A", globalAccountName, "foo", 1, time.Second)

	// Publish all messages. The queue sub on cluster B should receive all
	// messages.
	for i := 0; i < total; i++ {
		natsPub(t, ncsb2, "foo", []byte("msg"))
	}
	natsFlush(t, ncsb2)

	waitCh(t, ch, "Did not get all our queue messages")
	if n := int(atomic.LoadInt32(&c1)); n != 0 {
		t.Fatalf("No message should have been received by qsub1, got %v", n)
	}
	if n := int(atomic.LoadInt32(&c2)); n != 0 {
		t.Fatalf("No message should have been received by qsub2, got %v", n)
	}
	if n := int(atomic.LoadInt32(&c3)); n != total {
		t.Fatalf("All messages should have been delivered to qsub on B, got %v", n)
	}

	// Reset counters
	atomic.StoreInt32(&c1, 0)
	atomic.StoreInt32(&c2, 0)
	atomic.StoreInt32(&c3, 0)
	atomic.StoreInt32(&tc, 0)

	// Now send from cluster A, messages should be distributed to qsubs on A.
	for i := 0; i < total; i++ {
		natsPub(t, ncsa1, "foo", []byte("msg"))
	}
	natsFlush(t, ncsa1)

	expectedLow := int(float32(total/2) * 0.6)
	expectedHigh := int(float32(total/2) * 1.4)
	checkCount := func(t *testing.T, count *int32) {
		t.Helper()
		c := int(atomic.LoadInt32(count))
		if c < expectedLow || c > expectedHigh {
			t.Fatalf("Expected value to be between %v/%v, got %v", expectedLow, expectedHigh, c)
		}
	}
	waitCh(t, ch, "Did not get all our queue messages")
	checkCount(t, &c1)
	checkCount(t, &c2)

	// Now unsubscribe sub on B and reset counters
	natsUnsub(t, qsubOnB2)
	checkExpectedSubs(t, 0, sb2)
	atomic.StoreInt32(&c1, 0)
	atomic.StoreInt32(&c2, 0)
	atomic.StoreInt32(&c3, 0)
	atomic.StoreInt32(&tc, 0)
	// Publish from cluster B, messages should be delivered to cluster A.
	for i := 0; i < total; i++ {
		natsPub(t, ncsb2, "foo", []byte("msg"))
	}
	natsFlush(t, ncsb2)

	waitCh(t, ch, "Did not get all our queue messages")
	if n := int(atomic.LoadInt32(&c3)); n != 0 {
		t.Fatalf("There should not have been messages on unsubscribed sub, got %v", n)
	}
	checkCount(t, &c1)
	checkCount(t, &c2)
}

func TestGatewayMsgSentOnlyOnce(t *testing.T) {
	o2 := testDefaultOptionsForGateway("B")
	s2 := runGatewayServer(o2)
	defer s2.Shutdown()

	o1 := testGatewayOptionsFromToWithServers(t, "A", "B", s2)
	s1 := runGatewayServer(o1)
	defer s1.Shutdown()

	waitForOutboundGateways(t, s1, 1, time.Second)
	waitForOutboundGateways(t, s2, 1, time.Second)

	s2Url := fmt.Sprintf("nats://127.0.0.1:%d", o2.Port)
	nc2 := natsConnect(t, s2Url)
	defer nc2.Close()

	s1Url := fmt.Sprintf("nats://127.0.0.1:%d", o1.Port)
	nc1 := natsConnect(t, s1Url)
	defer nc1.Close()

	ch := make(chan bool, 1)
	count := int32(0)
	expected := int32(4)
	cb := func(_ *nats.Msg) {
		if c := atomic.AddInt32(&count, 1); c == expected {
			ch <- true
		}
	}

	// On s1, create 2 plain subs, 2 queue members for group
	// "bar" and 1 for group "baz".
	natsSub(t, nc1, ">", cb)
	natsSub(t, nc1, "foo", cb)
	natsQueueSub(t, nc1, "foo", "bar", cb)
	natsQueueSub(t, nc1, "foo", "bar", cb)
	natsQueueSub(t, nc1, "foo", "baz", cb)
	natsFlush(t, nc1)

	// Ensure subs registered in S1
	checkExpectedSubs(t, 5, s1)

	// Also need to wait for qsubs to be registered on s2.
	checkForRegisteredQSubInterest(t, s2, "A", globalAccountName, "foo", 2, time.Second)

	// From s2, send 1 message, s1 should receive 1 only,
	// and total we should get the callback notified 4 times.
	natsPub(t, nc2, "foo", []byte("hello"))
	natsFlush(t, nc2)

	waitCh(t, ch, "Did not get our messages")
	// Verifiy that count is still 4
	if c := atomic.LoadInt32(&count); c != expected {
		t.Fatalf("Expected %v messages, got %v", expected, c)
	}
	// Check s2 outbound connection stats. It should say that it
	// sent only 1 message.
	c := s2.getOutboundGatewayConnection("A")
	if c == nil {
		t.Fatalf("S2 outbound gateway not found")
	}
	c.mu.Lock()
	out := c.outMsgs
	c.mu.Unlock()
	if out != 1 {
		t.Fatalf("Expected s2's outbound gateway to have sent a single message, got %v", out)
	}
	// Now check s1's inbound gateway
	s1.gateway.RLock()
	c = nil
	for _, ci := range s1.gateway.in {
		c = ci
		break
	}
	s1.gateway.RUnlock()
	if c == nil {
		t.Fatalf("S1 inbound gateway not found")
	}
	if in := atomic.LoadInt64(&c.inMsgs); in != 1 {
		t.Fatalf("Expected s1's inbound gateway to have received a single message, got %v", in)
	}
}

type checkErrorLogger struct {
	DummyLogger
	checkErrorStr string
	gotError      bool
}

func (l *checkErrorLogger) Errorf(format string, args ...any) {
	l.DummyLogger.Errorf(format, args...)
	l.Lock()
	if strings.Contains(l.Msg, l.checkErrorStr) {
		l.gotError = true
	}
	l.Unlock()
}

func TestGatewayRoutedServerWithoutGatewayConfigured(t *testing.T) {
	o2 := testDefaultOptionsForGateway("B")
	s2 := runGatewayServer(o2)
	defer s2.Shutdown()

	o1 := testGatewayOptionsFromToWithServers(t, "A", "B", s2)
	s1 := runGatewayServer(o1)
	defer s1.Shutdown()

	waitForOutboundGateways(t, s1, 1, time.Second)
	waitForOutboundGateways(t, s2, 1, time.Second)

	o3 := DefaultOptions()
	o3.NoSystemAccount = true
	o3.Cluster.Name = "B"
	o3.Routes = RoutesFromStr(fmt.Sprintf("nats://127.0.0.1:%d", s2.ClusterAddr().Port))
	s3 := New(o3)
	defer s3.Shutdown()
	l := &checkErrorLogger{checkErrorStr: "not configured"}
	s3.SetLogger(l, true, true)
	wg := sync.WaitGroup{}
	wg.Add(1)
	go func() {
		s3.Start()
		wg.Done()
	}()

	checkClusterFormed(t, s2, s3)

	// Check that server s3 does not panic when being notified
	// about the A gateway, but report an error.
	deadline := time.Now().Add(2 * time.Second)
	gotIt := false
	for time.Now().Before(deadline) {
		l.Lock()
		gotIt = l.gotError
		l.Unlock()
		if gotIt {
			break
		}
		time.Sleep(15 * time.Millisecond)
	}
	if !gotIt {
		t.Fatalf("Should have reported error about gateway not configured")
	}

	s3.Shutdown()
	wg.Wait()
}

func TestGatewaySendsToNonLocalSubs(t *testing.T) {
	ob1 := testDefaultOptionsForGateway("B")
	sb1 := runGatewayServer(ob1)
	defer sb1.Shutdown()

	oa1 := testGatewayOptionsFromToWithServers(t, "A", "B", sb1)
	sa1 := runGatewayServer(oa1)
	defer sa1.Shutdown()

	waitForOutboundGateways(t, sa1, 1, time.Second)
	waitForOutboundGateways(t, sb1, 1, time.Second)

	waitForInboundGateways(t, sa1, 1, time.Second)
	waitForInboundGateways(t, sb1, 1, time.Second)

	oa2 := testGatewayOptionsFromToWithServers(t, "A", "B", sb1)
	oa2.Routes = RoutesFromStr(fmt.Sprintf("nats://127.0.0.1:%d", sa1.ClusterAddr().Port))
	sa2 := runGatewayServer(oa2)
	defer sa2.Shutdown()

	checkClusterFormed(t, sa1, sa2)

	waitForOutboundGateways(t, sa2, 1, time.Second)
	waitForInboundGateways(t, sb1, 2, time.Second)

	ch := make(chan bool, 1)
	// Create an interest of sa2
	ncSub := natsConnect(t, fmt.Sprintf("nats://127.0.0.1:%d", oa2.Port))
	defer ncSub.Close()
	natsSub(t, ncSub, "foo", func(_ *nats.Msg) { ch <- true })
	natsFlush(t, ncSub)
	checkExpectedSubs(t, 1, sa1, sa2)

	// Produce a message from sb1, make sure it can be received.
	ncPub := natsConnect(t, fmt.Sprintf("nats://127.0.0.1:%d", ob1.Port))
	defer ncPub.Close()
	natsPub(t, ncPub, "foo", []byte("hello"))
	waitCh(t, ch, "Did not get our message")

	ncSub.Close()
	ncPub.Close()
	checkExpectedSubs(t, 0, sa1, sa2)

	// Now create sb2 that has a route to sb1 and gateway connects to sa2.
	ob2 := testGatewayOptionsFromToWithServers(t, "B", "A", sa2)
	ob2.Routes = RoutesFromStr(fmt.Sprintf("nats://127.0.0.1:%d", sb1.ClusterAddr().Port))
	sb2 := runGatewayServer(ob2)
	defer sb2.Shutdown()

	checkClusterFormed(t, sb1, sb2)
	waitForOutboundGateways(t, sb2, 1, time.Second)

	ncSub = natsConnect(t, fmt.Sprintf("nats://127.0.0.1:%d", oa1.Port))
	defer ncSub.Close()
	natsSub(t, ncSub, "foo", func(_ *nats.Msg) { ch <- true })
	natsFlush(t, ncSub)
	checkExpectedSubs(t, 1, sa1, sa2)

	ncPub = natsConnect(t, fmt.Sprintf("nats://127.0.0.1:%d", ob2.Port))
	defer ncPub.Close()
	natsPub(t, ncPub, "foo", []byte("hello"))
	waitCh(t, ch, "Did not get our message")
}

func TestGatewayUnknownGatewayCommand(t *testing.T) {
	o1 := testDefaultOptionsForGateway("A")
	s1 := runGatewayServer(o1)
	defer s1.Shutdown()

	l := &checkErrorLogger{checkErrorStr: "Unknown command"}
	s1.SetLogger(l, true, true)

	o2 := testDefaultOptionsForGateway("A")
	o2.Routes = RoutesFromStr(fmt.Sprintf("nats://127.0.0.1:%d", s1.ClusterAddr().Port))
	s2 := runGatewayServer(o2)
	defer s2.Shutdown()

	checkClusterFormed(t, s1, s2)

	var route *client
	s2.mu.Lock()
	if r := getFirstRoute(s2); r != nil {
		route = r
	}
	s2.mu.Unlock()

	route.mu.Lock()
	info := &Info{
		Gateway:    "B",
		GatewayCmd: 255,
	}
	b, _ := json.Marshal(info)
	route.enqueueProto([]byte(fmt.Sprintf(InfoProto, b)))
	route.mu.Unlock()

	checkFor(t, time.Second, 15*time.Millisecond, func() error {
		l.Lock()
		gotIt := l.gotError
		l.Unlock()
		if gotIt {
			return nil
		}
		return fmt.Errorf("Did not get expected error")
	})
}

func TestGatewayRandomIP(t *testing.T) {
	ob := testDefaultOptionsForGateway("B")
	sb := runGatewayServer(ob)
	defer sb.Shutdown()

	oa := testGatewayOptionsFromToWithURLs(t, "A", "B",
		[]string{
			"nats://noport",
			fmt.Sprintf("nats://localhost:%d", sb.GatewayAddr().Port),
		})
	// Create a dummy resolver that returns error since we
	// don't provide any IP. The code should then use the configured
	// url (localhost:port) and try with that, which in this case
	// should work.
	oa.Gateway.resolver = &myDummyDNSResolver{}
	sa := runGatewayServer(oa)
	defer sa.Shutdown()

	waitForOutboundGateways(t, sa, 1, 2*time.Second)
	waitForOutboundGateways(t, sb, 1, 2*time.Second)
}

func TestGatewaySendQSubsBufSize(t *testing.T) {
	for _, test := range []struct {
		name    string
		bufSize int
	}{
		{
			name:    "Bufsize 45, more than one at a time",
			bufSize: 45,
		},
		{
			name:    "Bufsize 15, one at a time",
			bufSize: 15,
		},
		{
			name:    "Bufsize 0, default to maxBufSize, all at once",
			bufSize: 0,
		},
	} {
		t.Run(test.name, func(t *testing.T) {

			o2 := testDefaultOptionsForGateway("B")
			o2.Gateway.sendQSubsBufSize = test.bufSize
			s2 := runGatewayServer(o2)
			defer s2.Shutdown()

			s2Url := fmt.Sprintf("nats://%s:%d", o2.Host, o2.Port)
			nc := natsConnect(t, s2Url)
			defer nc.Close()
			natsQueueSub(t, nc, "foo", "bar", func(_ *nats.Msg) {})
			natsQueueSub(t, nc, "foo", "baz", func(_ *nats.Msg) {})
			natsQueueSub(t, nc, "foo", "bat", func(_ *nats.Msg) {})
			natsQueueSub(t, nc, "foo", "bax", func(_ *nats.Msg) {})

			checkExpectedSubs(t, 4, s2)

			o1 := testGatewayOptionsFromToWithServers(t, "A", "B", s2)
			s1 := runGatewayServer(o1)
			defer s1.Shutdown()

			waitForOutboundGateways(t, s1, 1, time.Second)
			waitForOutboundGateways(t, s2, 1, time.Second)

			checkForRegisteredQSubInterest(t, s1, "B", globalAccountName, "foo", 4, time.Second)

			// Make sure we have the 4 we expected
			c := s1.getOutboundGatewayConnection("B")
			ei, _ := c.gw.outsim.Load(globalAccountName)
			if ei == nil {
				t.Fatalf("No interest found")
			}
			sl := ei.(*outsie).sl
			r := sl.Match("foo")
			if len(r.qsubs) != 4 {
				t.Fatalf("Expected 4 groups, got %v", len(r.qsubs))
			}
			var gotBar, gotBaz, gotBat, gotBax bool
			for _, qs := range r.qsubs {
				if len(qs) != 1 {
					t.Fatalf("Unexpected number of subs for group %s: %v", qs[0].queue, len(qs))
				}
				q := qs[0].queue
				switch string(q) {
				case "bar":
					gotBar = true
				case "baz":
					gotBaz = true
				case "bat":
					gotBat = true
				case "bax":
					gotBax = true
				default:
					t.Fatalf("Unexpected group name: %s", q)
				}
			}
			if !gotBar || !gotBaz || !gotBat || !gotBax {
				t.Fatalf("Did not get all we wanted: bar=%v baz=%v bat=%v bax=%v",
					gotBar, gotBaz, gotBat, gotBax)
			}

			nc.Close()
			s1.Shutdown()
			s2.Shutdown()

			waitForOutboundGateways(t, s1, 0, time.Second)
			waitForOutboundGateways(t, s2, 0, time.Second)
		})
	}
}

func TestGatewayRaceBetweenPubAndSub(t *testing.T) {
	o2 := testDefaultOptionsForGateway("B")
	s2 := runGatewayServer(o2)
	defer s2.Shutdown()

	o1 := testGatewayOptionsFromToWithServers(t, "A", "B", s2)
	s1 := runGatewayServer(o1)
	defer s1.Shutdown()

	waitForOutboundGateways(t, s1, 1, time.Second)
	waitForOutboundGateways(t, s2, 1, time.Second)

	s2Url := fmt.Sprintf("nats://127.0.0.1:%d", o2.Port)
	nc2 := natsConnect(t, s2Url)
	defer nc2.Close()

	s1Url := fmt.Sprintf("nats://127.0.0.1:%d", o1.Port)
	var ncaa [5]*nats.Conn
	var nca = ncaa[:0]
	for i := 0; i < 5; i++ {
		nc := natsConnect(t, s1Url)
		defer nc.Close()
		nca = append(nca, nc)
	}

	ch := make(chan bool, 1)
	wg := sync.WaitGroup{}
	wg.Add(5)
	for _, nc := range nca {
		nc := nc
		go func(n *nats.Conn) {
			defer wg.Done()
			for {
				n.Publish("foo", []byte("hello"))
				select {
				case <-ch:
					return
				default:
				}
			}
		}(nc)
	}
	time.Sleep(100 * time.Millisecond)
	natsQueueSub(t, nc2, "foo", "bar", func(m *nats.Msg) {
		natsUnsub(t, m.Sub)
		close(ch)
	})
	wg.Wait()
}

// Returns the first (if any) of the inbound connections for this name.
func getInboundGatewayConnection(s *Server, name string) *client {
	var gwsa [4]*client
	var gws = gwsa[:0]
	s.getInboundGatewayConnections(&gws)
	for _, gw := range gws {
		gw.mu.Lock()
		ok := gw.gw.name == name
		gw.mu.Unlock()
		if ok {
			return gw
		}
	}
	return nil
}

func TestGatewaySendAllSubs(t *testing.T) {
	GatewayDoNotForceInterestOnlyMode(true)
	defer GatewayDoNotForceInterestOnlyMode(false)

	gatewayMaxRUnsubBeforeSwitch = 100
	defer func() { gatewayMaxRUnsubBeforeSwitch = defaultGatewayMaxRUnsubBeforeSwitch }()

	ob := testDefaultOptionsForGateway("B")
	sb := runGatewayServer(ob)
	defer sb.Shutdown()

	oa := testGatewayOptionsFromToWithServers(t, "A", "B", sb)
	sa := runGatewayServer(oa)
	defer sa.Shutdown()

	oc := testGatewayOptionsFromToWithServers(t, "C", "B", sb)
	sc := runGatewayServer(oc)
	defer sc.Shutdown()

	waitForOutboundGateways(t, sa, 2, time.Second)
	waitForOutboundGateways(t, sb, 2, time.Second)
	waitForOutboundGateways(t, sc, 2, time.Second)
	waitForInboundGateways(t, sa, 2, time.Second)
	waitForInboundGateways(t, sb, 2, time.Second)
	waitForInboundGateways(t, sc, 2, time.Second)

	// On A, create a sub to register some interest
	aURL := fmt.Sprintf("nats://%s:%d", oa.Host, oa.Port)
	ncA := natsConnect(t, aURL)
	defer ncA.Close()
	natsSub(t, ncA, "sub.on.a.*", func(m *nats.Msg) {})
	natsFlush(t, ncA)
	checkExpectedSubs(t, 1, sa)

	// On C, have some sub activity while it receives
	// unwanted messages and switches to interestOnly mode.
	cURL := fmt.Sprintf("nats://%s:%d", oc.Host, oc.Port)
	ncC := natsConnect(t, cURL)
	defer ncC.Close()
	wg := sync.WaitGroup{}
	wg.Add(2)
	done := make(chan bool)
	consCount := 0
	accsCount := 0
	go func() {
		defer wg.Done()
		for i := 0; ; i++ {
			// Create subs and qsubs on same subject
			natsSub(t, ncC, fmt.Sprintf("foo.%d", i+1), func(_ *nats.Msg) {})
			natsQueueSub(t, ncC, fmt.Sprintf("foo.%d", i+1), fmt.Sprintf("bar.%d", i+1), func(_ *nats.Msg) {})
			// Create psubs and qsubs on unique subjects
			natsSub(t, ncC, fmt.Sprintf("foox.%d", i+1), func(_ *nats.Msg) {})
			natsQueueSub(t, ncC, fmt.Sprintf("fooy.%d", i+1), fmt.Sprintf("bar.%d", i+1), func(_ *nats.Msg) {})
			consCount += 4
			// Register account
			sc.RegisterAccount(fmt.Sprintf("acc.%d", i+1))
			accsCount++
			select {
			case <-done:
				return
			case <-time.After(15 * time.Millisecond):
			}
		}
	}()

	// From B publish on subjects for which C has an interest
	bURL := fmt.Sprintf("nats://%s:%d", ob.Host, ob.Port)
	ncB := natsConnect(t, bURL)
	defer ncB.Close()

	go func() {
		defer wg.Done()
		time.Sleep(10 * time.Millisecond)
		for {
			for i := 0; i < 10; i++ {
				natsPub(t, ncB, fmt.Sprintf("foo.%d", i+1), []byte("hello"))
			}
			select {
			case <-done:
				return
			case <-time.After(5 * time.Millisecond):
			}
		}
	}()

	// From B, send a lot of messages that A is interested in,
	// but not C.
	// TODO(ik): May need to change that if we change the threshold
	// for when the switch happens.
	total := 300
	for i := 0; i < total; i++ {
		if err := ncB.Publish(fmt.Sprintf("sub.on.a.%d", i), []byte("hi")); err != nil {
			t.Fatalf("Error waiting for reply: %v", err)
		}
	}
	close(done)

	// Normally, C would receive a message for each req inbox and
	// would send and RS- on that to B, making both have an unbounded
	// growth of the no-interest map. But after a certain amount
	// of RS-, C will send all its sub for the given account and
	// instruct B to send only if there is explicit interest.
	checkFor(t, 2*time.Second, 50*time.Millisecond, func() error {
		// Check C inbound connection from B
		c := getInboundGatewayConnection(sc, "B")
		c.mu.Lock()
		var switchedMode bool
		e := c.gw.insim[globalAccountName]
		if e != nil {
			switchedMode = e.ni == nil && e.mode == InterestOnly
		}
		c.mu.Unlock()
		if !switchedMode {
			return fmt.Errorf("C has still not switched mode")
		}
		return nil
	})
	checkGWInterestOnlyMode(t, sb, "C", globalAccountName)
	wg.Wait()

	// Check consCount and accsCount on C
	checkFor(t, 2*time.Second, 15*time.Millisecond, func() error {
		sc.gateway.pasi.Lock()
		scount := len(sc.gateway.pasi.m[globalAccountName])
		sc.gateway.pasi.Unlock()
		if scount != consCount {
			return fmt.Errorf("Expected %v consumers for global account, got %v", consCount, scount)
		}
		acount := sc.numAccounts()
		if acount != accsCount+1 {
			return fmt.Errorf("Expected %v accounts, got %v", accsCount+1, acount)
		}
		return nil
	})

	// Also, after all that, if a sub is created on C, it should
	// be sent to B (but not A). Check that this is the case.
	// So first send from A on the subject that we are going to
	// use for this new sub.
	natsPub(t, ncA, "newsub", []byte("hello"))
	natsFlush(t, ncA)
	aOutboundToC := sa.getOutboundGatewayConnection("C")
	checkForSubjectNoInterest(t, aOutboundToC, globalAccountName, "newsub", true, 2*time.Second)

	newSubSub := natsSub(t, ncC, "newsub", func(_ *nats.Msg) {})
	natsFlush(t, ncC)
	checkExpectedSubs(t, consCount+1)
	checkFor(t, time.Second, 15*time.Millisecond, func() error {
		c := sb.getOutboundGatewayConnection("C")
		ei, _ := c.gw.outsim.Load(globalAccountName)
		if ei != nil {
			sl := ei.(*outsie).sl
			r := sl.Match("newsub")
			if len(r.psubs) == 1 {
				return nil
			}
		}
		return fmt.Errorf("Newsub not registered on B")
	})
	checkForSubjectNoInterest(t, aOutboundToC, globalAccountName, "newsub", false, 2*time.Second)

	natsUnsub(t, newSubSub)
	natsFlush(t, ncC)
	checkExpectedSubs(t, consCount)
	checkFor(t, time.Second, 15*time.Millisecond, func() error {
		c := sb.getOutboundGatewayConnection("C")
		ei, _ := c.gw.outsim.Load(globalAccountName)
		if ei != nil {
			sl := ei.(*outsie).sl
			r := sl.Match("newsub")
			if len(r.psubs) == 0 {
				return nil
			}
		}
		return fmt.Errorf("Newsub still registered on B")
	})
}

func TestGatewaySendAllSubsBadProtocol(t *testing.T) {
	ob := testDefaultOptionsForGateway("B")
	sb := runGatewayServer(ob)
	defer sb.Shutdown()

	oa := testGatewayOptionsFromToWithServers(t, "A", "B", sb)
	sa := runGatewayServer(oa)
	defer sa.Shutdown()

	waitForOutboundGateways(t, sa, 1, time.Second)
	waitForOutboundGateways(t, sb, 1, time.Second)
	waitForInboundGateways(t, sa, 1, time.Second)
	waitForInboundGateways(t, sb, 1, time.Second)

	// For this test, make sure to use inbound from A so
	// A will reconnect when we send bad proto that
	// causes connection to be closed.
	c := getInboundGatewayConnection(sa, "B")
	// Mock an invalid protocol (account name missing)
	info := &Info{
		Gateway:    "B",
		GatewayCmd: gatewayCmdAllSubsStart,
	}
	b, _ := json.Marshal(info)
	c.mu.Lock()
	c.enqueueProto([]byte(fmt.Sprintf("INFO %s\r\n", b)))
	c.mu.Unlock()

	orgConn := c
	checkFor(t, 3*time.Second, 100*time.Millisecond, func() error {
		curConn := getInboundGatewayConnection(sa, "B")
		if orgConn == curConn {
			return fmt.Errorf("Not reconnected")
		}
		return nil
	})

	waitForOutboundGateways(t, sa, 1, 2*time.Second)
	waitForOutboundGateways(t, sb, 1, 2*time.Second)

	// Refresh
	c = nil
	checkFor(t, 3*time.Second, 15*time.Millisecond, func() error {
		c = getInboundGatewayConnection(sa, "B")
		if c == nil {
			return fmt.Errorf("Did not reconnect")
		}
		return nil
	})
	// Do correct start
	info.GatewayCmdPayload = []byte(globalAccountName)
	b, _ = json.Marshal(info)
	c.mu.Lock()
	c.enqueueProto([]byte(fmt.Sprintf("INFO %s\r\n", b)))
	c.mu.Unlock()
	// But incorrect end.
	info.GatewayCmd = gatewayCmdAllSubsComplete
	info.GatewayCmdPayload = nil
	b, _ = json.Marshal(info)
	c.mu.Lock()
	c.enqueueProto([]byte(fmt.Sprintf("INFO %s\r\n", b)))
	c.mu.Unlock()

	orgConn = c
	checkFor(t, 3*time.Second, 100*time.Millisecond, func() error {
		curConn := getInboundGatewayConnection(sa, "B")
		if orgConn == curConn {
			return fmt.Errorf("Not reconnected")
		}
		return nil
	})
}

func TestGatewayRaceOnClose(t *testing.T) {
	ob := testDefaultOptionsForGateway("B")
	sb := runGatewayServer(ob)
	defer sb.Shutdown()

	oa := testGatewayOptionsFromToWithServers(t, "A", "B", sb)
	sa := runGatewayServer(oa)
	defer sa.Shutdown()

	waitForOutboundGateways(t, sa, 1, time.Second)
	waitForOutboundGateways(t, sb, 1, time.Second)
	waitForInboundGateways(t, sa, 1, time.Second)
	waitForInboundGateways(t, sb, 1, time.Second)

	bURL := fmt.Sprintf("nats://%s:%d", ob.Host, ob.Port)
	ncB := natsConnect(t, bURL, nats.NoReconnect())
	defer ncB.Close()

	wg := sync.WaitGroup{}
	wg.Add(1)
	go func() {
		defer wg.Done()
		cb := func(_ *nats.Msg) {}
		for {
			// Expect failure at one point and just return.
			qsub, err := ncB.QueueSubscribe("foo", "bar", cb)
			if err != nil {
				return
			}
			if err := qsub.Unsubscribe(); err != nil {
				return
			}
		}
	}()
	// Wait a bit and kill B
	time.Sleep(200 * time.Millisecond)
	sb.Shutdown()
	wg.Wait()
}

// Similar to TestNewRoutesServiceImport but with 2 GW servers instead
// of a cluster of 2 servers.
func TestGatewayServiceImport(t *testing.T) {
	GatewayDoNotForceInterestOnlyMode(true)
	defer GatewayDoNotForceInterestOnlyMode(false)

	oa := testDefaultOptionsForGateway("A")
	setAccountUserPassInOptions(oa, "$foo", "clientA", "password")
	setAccountUserPassInOptions(oa, "$bar", "yyyyyyy", "password")
	sa := runGatewayServer(oa)
	defer sa.Shutdown()

	ob := testGatewayOptionsFromToWithServers(t, "B", "A", sa)
	setAccountUserPassInOptions(ob, "$foo", "clientBFoo", "password")
	setAccountUserPassInOptions(ob, "$bar", "clientB", "password")
	sb := runGatewayServer(ob)
	defer sb.Shutdown()

	waitForOutboundGateways(t, sa, 1, time.Second)
	waitForOutboundGateways(t, sb, 1, time.Second)
	waitForInboundGateways(t, sa, 1, time.Second)
	waitForInboundGateways(t, sb, 1, time.Second)

	// Get accounts
	fooA, _ := sa.LookupAccount("$foo")
	barA, _ := sa.LookupAccount("$bar")
	fooB, _ := sb.LookupAccount("$foo")
	barB, _ := sb.LookupAccount("$bar")

	// Add in the service export for the requests. Make it public.
	fooA.AddServiceExport("test.request", nil)
	fooB.AddServiceExport("test.request", nil)

	// Add import abilities to server B's bar account from foo.
	if err := barB.AddServiceImport(fooB, "foo.request", "test.request"); err != nil {
		t.Fatalf("Error adding service import: %v", err)
	}
	// Same on A.
	if err := barA.AddServiceImport(fooA, "foo.request", "test.request"); err != nil {
		t.Fatalf("Error adding service import: %v", err)
	}

	// clientA will be connected to srvA and be the service endpoint and responder.
	aURL := fmt.Sprintf("nats://clientA:password@127.0.0.1:%d", oa.Port)
	clientA := natsConnect(t, aURL)
	defer clientA.Close()

	subA := natsSubSync(t, clientA, "test.request")
	natsFlush(t, clientA)

	// Now setup client B on srvB who will do a sub from account $bar
	// that should map account $foo's foo subject.
	bURL := fmt.Sprintf("nats://clientB:password@127.0.0.1:%d", ob.Port)
	clientB := natsConnect(t, bURL)
	defer clientB.Close()

	subB := natsSubSync(t, clientB, "reply")
	natsFlush(t, clientB)

	for i := 1; i <= 2; i++ {
		// Send the request from clientB on foo.request,
		natsPubReq(t, clientB, "foo.request", "reply", []byte("hi"))
		natsFlush(t, clientB)

		// Expect the request on A
		msg, err := subA.NextMsg(time.Second)
		if err != nil {
			t.Fatalf("subA failed to get request: %v", err)
		}
		if msg.Subject != "test.request" || string(msg.Data) != "hi" {
			t.Fatalf("Unexpected message: %v", msg)
		}
		if msg.Reply == "reply" {
			t.Fatalf("Expected randomized reply, but got original")
		}

		// Check for duplicate message
		if msg, err := subA.NextMsg(250 * time.Millisecond); err != nats.ErrTimeout {
			t.Fatalf("Unexpected msg: %v", msg)
		}

		// Send reply
		natsPub(t, clientA, msg.Reply, []byte("ok"))
		natsFlush(t, clientA)

		msg, err = subB.NextMsg(time.Second)
		if err != nil {
			t.Fatalf("subB failed to get reply: %v", err)
		}
		if msg.Subject != "reply" || string(msg.Data) != "ok" {
			t.Fatalf("Unexpected message: %v", msg)
		}

		// Check for duplicate message
		if msg, err := subB.NextMsg(250 * time.Millisecond); err != nats.ErrTimeout {
			t.Fatalf("Unexpected msg: %v", msg)
		}

		expected := int64(i * 2)
		vz, _ := sa.Varz(nil)
		if vz.OutMsgs != expected {
			t.Fatalf("Expected %d outMsgs for A, got %v", expected, vz.OutMsgs)
		}

		// For B, we expect it to send to gateway on the two subjects: test.request
		// and foo.request then send the reply to the client and optimistically
		// to the other gateway.
		if i == 1 {
			expected = 4
		} else {
			// The second time, one of the accounts will be suppressed and the reply going
			// back so we should only get 2 more messages.
			expected = 6
		}
		vz, _ = sb.Varz(nil)
		if vz.OutMsgs != expected {
			t.Fatalf("Expected %d outMsgs for B, got %v", expected, vz.OutMsgs)
		}
	}

	checkFor(t, 2*time.Second, 15*time.Millisecond, func() error {
		if ts := fooA.TotalSubs(); ts != 1 {
			return fmt.Errorf("Expected one sub to be left on fooA, but got %d", ts)
		}
		return nil
	})

	// Speed up exiration
	err := fooA.SetServiceExportResponseThreshold("test.request", 50*time.Millisecond)
	if err != nil {
		t.Fatalf("Error setting response threshold: %v", err)
	}

	// Send 100 requests from clientB on foo.request,
	for i := 0; i < 100; i++ {
		natsPubReq(t, clientB, "foo.request", "reply", []byte("hi"))
	}
	natsFlush(t, clientB)

	// Consume the requests, but don't reply to them...
	for i := 0; i < 100; i++ {
		if _, err := subA.NextMsg(time.Second); err != nil {
			t.Fatalf("subA did not receive request: %v", err)
		}
	}

	// These reply subjects will be dangling off of $foo account on serverA.
	// Remove our service endpoint and wait for the dangling replies to go to zero.
	natsUnsub(t, subA)
	natsFlush(t, clientA)

	checkFor(t, 2*time.Second, 10*time.Millisecond, func() error {
		if ts := fooA.TotalSubs(); ts != 0 {
			return fmt.Errorf("Number of subs is %d, should be zero", ts)
		}
		return nil
	})

	// Repeat similar test but without the small TTL and verify
	// that if B is shutdown, the dangling subs for replies are
	// cleared from the account sublist.
	err = fooA.SetServiceExportResponseThreshold("test.request", 10*time.Second)
	if err != nil {
		t.Fatalf("Error setting response threshold: %v", err)
	}

	subA = natsSubSync(t, clientA, "test.request")
	natsFlush(t, clientA)

	// Send 100 requests from clientB on foo.request,
	for i := 0; i < 100; i++ {
		natsPubReq(t, clientB, "foo.request", "reply", []byte("hi"))
	}
	natsFlush(t, clientB)

	// Consume the requests, but don't reply to them...
	for i := 0; i < 100; i++ {
		if _, err := subA.NextMsg(time.Second); err != nil {
			t.Fatalf("subA did not receive request: %v", err)
		}
	}

	// Shutdown B
	clientB.Close()
	sb.Shutdown()

	// Close our last sub
	natsUnsub(t, subA)
	natsFlush(t, clientA)

	// Verify that they are gone before the 10 sec TTL
	checkFor(t, 2*time.Second, 10*time.Millisecond, func() error {
		if ts := fooA.TotalSubs(); ts != 0 {
			return fmt.Errorf("Number of subs is %d, should be zero", ts)
		}
		return nil
	})

	// Check that this all work in interest-only mode
	sb = runGatewayServer(ob)
	defer sb.Shutdown()

	fooB, _ = sb.LookupAccount("$foo")
	barB, _ = sb.LookupAccount("$bar")

	// Add in the service export for the requests. Make it public.
	fooB.AddServiceExport("test.request", nil)
	// Add import abilities to server B's bar account from foo.
	if err := barB.AddServiceImport(fooB, "foo.request", "test.request"); err != nil {
		t.Fatalf("Error adding service import: %v", err)
	}

	waitForOutboundGateways(t, sa, 1, 2*time.Second)
	waitForOutboundGateways(t, sb, 1, 2*time.Second)
	waitForInboundGateways(t, sa, 1, 2*time.Second)
	waitForInboundGateways(t, sb, 1, 2*time.Second)

	// We need at least a subscription on A otherwise when publishing
	// to subjects with no interest we would simply get an A-
	natsSubSync(t, clientA, "not.used")

	// Create a client on B that will use account $foo
	bURL = fmt.Sprintf("nats://clientBFoo:password@127.0.0.1:%d", ob.Port)
	clientB = natsConnect(t, bURL)
	defer clientB.Close()

	// First flood with subjects that remote gw is not interested
	// so we switch to interest-only.
	for i := 0; i < 1100; i++ {
		natsPub(t, clientB, fmt.Sprintf("no.interest.%d", i), []byte("hello"))
	}
	natsFlush(t, clientB)

	checkGWInterestOnlyMode(t, sb, "A", "$foo")

	// Go back to clientB on $bar.
	clientB.Close()
	bURL = fmt.Sprintf("nats://clientB:password@127.0.0.1:%d", ob.Port)
	clientB = natsConnect(t, bURL)
	defer clientB.Close()

	subA = natsSubSync(t, clientA, "test.request")
	natsFlush(t, clientA)

	subB = natsSubSync(t, clientB, "reply")
	natsFlush(t, clientB)

	// Sine it is interest-only, B should receive an interest
	// on $foo test.request
	checkGWInterestOnlyModeInterestOn(t, sb, "A", "$foo", "test.request")

	// Send the request from clientB on foo.request,
	natsPubReq(t, clientB, "foo.request", "reply", []byte("hi"))
	natsFlush(t, clientB)

	// Expect the request on A
	msg, err := subA.NextMsg(time.Second)
	if err != nil {
		t.Fatalf("subA failed to get request: %v", err)
	}
	if msg.Subject != "test.request" || string(msg.Data) != "hi" {
		t.Fatalf("Unexpected message: %v", msg)
	}
	if msg.Reply == "reply" {
		t.Fatalf("Expected randomized reply, but got original")
	}

	// Check for duplicate message
	if msg, err := subA.NextMsg(100 * time.Millisecond); err != nats.ErrTimeout {
		t.Fatalf("Unexpected msg: %v", msg)
	}

	// Send reply
	natsPub(t, clientA, msg.Reply, []byte("ok"))
	natsFlush(t, clientA)

	msg, err = subB.NextMsg(time.Second)
	if err != nil {
		t.Fatalf("subB failed to get reply: %v", err)
	}
	if msg.Subject != "reply" || string(msg.Data) != "ok" {
		t.Fatalf("Unexpected message: %v", msg)
	}

	// Check for duplicate message
	if msg, err := subB.NextMsg(100 * time.Millisecond); err != nats.ErrTimeout {
		t.Fatalf("Unexpected msg: %v", msg)
	}
}

func TestGatewayServiceImportWithQueue(t *testing.T) {
	GatewayDoNotForceInterestOnlyMode(true)
	defer GatewayDoNotForceInterestOnlyMode(false)

	oa := testDefaultOptionsForGateway("A")
	setAccountUserPassInOptions(oa, "$foo", "clientA", "password")
	setAccountUserPassInOptions(oa, "$bar", "yyyyyyy", "password")
	sa := runGatewayServer(oa)
	defer sa.Shutdown()

	ob := testGatewayOptionsFromToWithServers(t, "B", "A", sa)
	setAccountUserPassInOptions(ob, "$foo", "clientBFoo", "password")
	setAccountUserPassInOptions(ob, "$bar", "clientB", "password")
	sb := runGatewayServer(ob)
	defer sb.Shutdown()

	waitForOutboundGateways(t, sa, 1, time.Second)
	waitForOutboundGateways(t, sb, 1, time.Second)
	waitForInboundGateways(t, sa, 1, time.Second)
	waitForInboundGateways(t, sb, 1, time.Second)

	// Get accounts
	fooA, _ := sa.LookupAccount("$foo")
	barA, _ := sa.LookupAccount("$bar")
	fooB, _ := sb.LookupAccount("$foo")
	barB, _ := sb.LookupAccount("$bar")

	// Add in the service export for the requests. Make it public.
	fooA.AddServiceExport("test.request", nil)
	fooB.AddServiceExport("test.request", nil)

	// Add import abilities to server B's bar account from foo.
	if err := barB.AddServiceImport(fooB, "foo.request", "test.request"); err != nil {
		t.Fatalf("Error adding service import: %v", err)
	}
	// Same on A.
	if err := barA.AddServiceImport(fooA, "foo.request", "test.request"); err != nil {
		t.Fatalf("Error adding service import: %v", err)
	}

	// clientA will be connected to srvA and be the service endpoint and responder.
	aURL := fmt.Sprintf("nats://clientA:password@127.0.0.1:%d", oa.Port)
	clientA := natsConnect(t, aURL)
	defer clientA.Close()

	subA := natsQueueSubSync(t, clientA, "test.request", "queue")
	natsFlush(t, clientA)

	// Now setup client B on srvB who will do a sub from account $bar
	// that should map account $foo's foo subject.
	bURL := fmt.Sprintf("nats://clientB:password@127.0.0.1:%d", ob.Port)
	clientB := natsConnect(t, bURL)
	defer clientB.Close()

	subB := natsQueueSubSync(t, clientB, "reply", "queue2")
	natsFlush(t, clientB)

	// Wait for queue interest on test.request from A to be registered
	// on server B.
	checkForRegisteredQSubInterest(t, sb, "A", "$foo", "test.request", 1, time.Second)

	for i := 0; i < 2; i++ {
		// Send the request from clientB on foo.request,
		natsPubReq(t, clientB, "foo.request", "reply", []byte("hi"))
		natsFlush(t, clientB)

		// Expect the request on A
		msg, err := subA.NextMsg(time.Second)
		if err != nil {
			t.Fatalf("subA failed to get request: %v", err)
		}
		if msg.Subject != "test.request" || string(msg.Data) != "hi" {
			t.Fatalf("Unexpected message: %v", msg)
		}
		if msg.Reply == "reply" {
			t.Fatalf("Expected randomized reply, but got original")
		}
		// Check for duplicate message
		if msg, err := subA.NextMsg(100 * time.Millisecond); err != nats.ErrTimeout {
			t.Fatalf("Unexpected msg: %v", msg)
		}

		// Send reply
		natsPub(t, clientA, msg.Reply, []byte("ok"))
		natsFlush(t, clientA)

		msg, err = subB.NextMsg(time.Second)
		if err != nil {
			t.Fatalf("subB failed to get reply: %v", err)
		}
		if msg.Subject != "reply" || string(msg.Data) != "ok" {
			t.Fatalf("Unexpected message: %v", msg)
		}
		// Check for duplicate message
		if msg, err := subB.NextMsg(250 * time.Millisecond); err != nats.ErrTimeout {
			t.Fatalf("Unexpected msg: %v", msg)
		}

		expected := int64((i + 1) * 2)
		vz, _ := sa.Varz(nil)
		if vz.OutMsgs != expected {
			t.Fatalf("Expected %d outMsgs for A, got %v", expected, vz.OutMsgs)
		}

		// For B, we expect it to send to gateway on the two subjects: test.request
		// and foo.request then send the reply to the client and optimistically
		// to the other gateway.
		if i == 0 {
			expected = 4
		} else {
			// The second time, one of the accounts will be suppressed and the reply going
			// back so we should get only 2 more messages.
			expected = 6
		}
		vz, _ = sb.Varz(nil)
		if vz.OutMsgs != expected {
			t.Fatalf("Expected %d outMsgs for B, got %v", expected, vz.OutMsgs)
		}
	}

	checkFor(t, 2*time.Second, 15*time.Millisecond, func() error {
		if ts := fooA.TotalSubs(); ts != 1 {
			return fmt.Errorf("Expected one sub to be left on fooA, but got %d", ts)
		}
		return nil
	})

	// Speed up exiration
	err := fooA.SetServiceExportResponseThreshold("test.request", 10*time.Millisecond)
	if err != nil {
		t.Fatalf("Error setting response threshold: %v", err)
	}

	// Send 100 requests from clientB on foo.request,
	for i := 0; i < 100; i++ {
		natsPubReq(t, clientB, "foo.request", "reply", []byte("hi"))
	}
	natsFlush(t, clientB)

	// Consume the requests, but don't reply to them...
	for i := 0; i < 100; i++ {
		if _, err := subA.NextMsg(time.Second); err != nil {
			t.Fatalf("subA did not receive request: %v", err)
		}
	}

	// These reply subjects will be dangling off of $foo account on serverA.
	// Remove our service endpoint and wait for the dangling replies to go to zero.
	natsUnsub(t, subA)
	natsFlush(t, clientA)

	checkFor(t, 2*time.Second, 10*time.Millisecond, func() error {
		if ts := fooA.TotalSubs(); ts != 0 {
			return fmt.Errorf("Number of subs is %d, should be zero", ts)
		}
		return nil
	})
	checkForRegisteredQSubInterest(t, sb, "A", "$foo", "test.request", 0, time.Second)

	// Repeat similar test but without the small TTL and verify
	// that if B is shutdown, the dangling subs for replies are
	// cleared from the account sublist.
	err = fooA.SetServiceExportResponseThreshold("test.request", 10*time.Second)
	if err != nil {
		t.Fatalf("Error setting response threshold: %v", err)
	}

	subA = natsQueueSubSync(t, clientA, "test.request", "queue")
	natsFlush(t, clientA)
	checkForRegisteredQSubInterest(t, sb, "A", "$foo", "test.request", 1, time.Second)

	// Send 100 requests from clientB on foo.request,
	for i := 0; i < 100; i++ {
		natsPubReq(t, clientB, "foo.request", "reply", []byte("hi"))
	}
	natsFlush(t, clientB)

	// Consume the requests, but don't reply to them...
	for i := 0; i < 100; i++ {
		if _, err := subA.NextMsg(time.Second); err != nil {
			t.Fatalf("subA did not receive request %d: %v", i+1, err)
		}
	}

	// Shutdown B
	clientB.Close()
	sb.Shutdown()

	// Close our last sub
	natsUnsub(t, subA)
	natsFlush(t, clientA)

	// Verify that they are gone before the 10 sec TTL
	checkFor(t, 2*time.Second, 10*time.Millisecond, func() error {
		if ts := fooA.TotalSubs(); ts != 0 {
			return fmt.Errorf("Number of subs is %d, should be zero", ts)
		}
		return nil
	})

	// Check that this all work in interest-only mode
	sb = runGatewayServer(ob)
	defer sb.Shutdown()

	fooB, _ = sb.LookupAccount("$foo")
	barB, _ = sb.LookupAccount("$bar")

	// Add in the service export for the requests. Make it public.
	fooB.AddServiceExport("test.request", nil)
	// Add import abilities to server B's bar account from foo.
	if err := barB.AddServiceImport(fooB, "foo.request", "test.request"); err != nil {
		t.Fatalf("Error adding service import: %v", err)
	}

	waitForOutboundGateways(t, sa, 1, 2*time.Second)
	waitForOutboundGateways(t, sb, 1, 2*time.Second)
	waitForInboundGateways(t, sa, 1, 2*time.Second)
	waitForInboundGateways(t, sb, 1, 2*time.Second)

	// We need at least a subscription on A otherwise when publishing
	// to subjects with no interest we would simply get an A-
	natsSubSync(t, clientA, "not.used")

	// Create a client on B that will use account $foo
	bURL = fmt.Sprintf("nats://clientBFoo:password@127.0.0.1:%d", ob.Port)
	clientB = natsConnect(t, bURL)
	defer clientB.Close()

	// First flood with subjects that remote gw is not interested
	// so we switch to interest-only.
	for i := 0; i < 1100; i++ {
		natsPub(t, clientB, fmt.Sprintf("no.interest.%d", i), []byte("hello"))
	}
	natsFlush(t, clientB)

	checkGWInterestOnlyMode(t, sb, "A", "$foo")

	// Go back to clientB on $bar.
	clientB.Close()
	bURL = fmt.Sprintf("nats://clientB:password@127.0.0.1:%d", ob.Port)
	clientB = natsConnect(t, bURL)
	defer clientB.Close()

	subA = natsSubSync(t, clientA, "test.request")
	natsFlush(t, clientA)

	subB = natsSubSync(t, clientB, "reply")
	natsFlush(t, clientB)

	// Sine it is interest-only, B should receive an interest
	// on $foo test.request
	checkGWInterestOnlyModeInterestOn(t, sb, "A", "$foo", "test.request")

	// Send the request from clientB on foo.request,
	natsPubReq(t, clientB, "foo.request", "reply", []byte("hi"))
	natsFlush(t, clientB)

	// Expect the request on A
	msg, err := subA.NextMsg(time.Second)
	if err != nil {
		t.Fatalf("subA failed to get request: %v", err)
	}
	if msg.Subject != "test.request" || string(msg.Data) != "hi" {
		t.Fatalf("Unexpected message: %v", msg)
	}
	if msg.Reply == "reply" {
		t.Fatalf("Expected randomized reply, but got original")
	}

	// Check for duplicate message
	if msg, err := subA.NextMsg(100 * time.Millisecond); err != nats.ErrTimeout {
		t.Fatalf("Unexpected msg: %v", msg)
	}

	// Send reply
	natsPub(t, clientA, msg.Reply, []byte("ok"))
	natsFlush(t, clientA)

	msg, err = subB.NextMsg(time.Second)
	if err != nil {
		t.Fatalf("subB failed to get reply: %v", err)
	}
	if msg.Subject != "reply" || string(msg.Data) != "ok" {
		t.Fatalf("Unexpected message: %v", msg)
	}

	// Check for duplicate message
	if msg, err := subB.NextMsg(100 * time.Millisecond); err != nats.ErrTimeout {
		t.Fatalf("Unexpected msg: %v", msg)
	}
}

func ensureGWConnectTo(t *testing.T, s *Server, remoteGWName string, remoteGWServer *Server) {
	t.Helper()
	var good bool
	for i := 0; !good && (i < 3); i++ {
		checkFor(t, 2*time.Second, 15*time.Millisecond, func() error {
			if s.numOutboundGateways() == 0 {
				return fmt.Errorf("Still no gw outbound connection")
			}
			return nil
		})
		ogc := s.getOutboundGatewayConnection(remoteGWName)
		ogc.mu.Lock()
		name := ogc.opts.Name
		nc := ogc.nc
		ogc.mu.Unlock()
		if name != remoteGWServer.ID() {
			rg := s.getRemoteGateway(remoteGWName)
			goodURL := remoteGWServer.getGatewayURL()
			rg.Lock()
			for u := range rg.urls {
				if u != goodURL {
					delete(rg.urls, u)
				}
			}
			rg.Unlock()
			if nc != nil {
				nc.Close()
			}
		} else {
			good = true
		}
	}
	if !good {
		t.Fatalf("Could not ensure that server connects to remote gateway %q at URL %q",
			remoteGWName, remoteGWServer.getGatewayURL())
	}
}

func TestGatewayServiceImportComplexSetup(t *testing.T) {
	// This test will have following setup:
	//
	//						     |- responder (subs  to "$foo test.request")
	//                           |            (sends to "$foo _R_.xxxx")
	//              route        v
	//   [A1]<----------------->[A2]
	//   ^  |^                    |
	//   |gw| \______gw________ gw|
	//   |  v                  \  v
	//   [B1]<----------------->[B2]
	//    ^         route
	//    |
	//    |_ requestor (sends "$bar foo.request reply")
	//

	// Setup first A1 and B1 to ensure that they have GWs
	// connections as described above.

	oa1 := testDefaultOptionsForGateway("A")
	setAccountUserPassInOptions(oa1, "$foo", "clientA", "password")
	setAccountUserPassInOptions(oa1, "$bar", "yyyyyyy", "password")
	sa1 := runGatewayServer(oa1)
	defer sa1.Shutdown()

	ob1 := testGatewayOptionsFromToWithServers(t, "B", "A", sa1)
	setAccountUserPassInOptions(ob1, "$foo", "xxxxxxx", "password")
	setAccountUserPassInOptions(ob1, "$bar", "clientB", "password")
	sb1 := runGatewayServer(ob1)
	defer sb1.Shutdown()

	waitForOutboundGateways(t, sa1, 1, time.Second)
	waitForOutboundGateways(t, sb1, 1, time.Second)

	waitForInboundGateways(t, sa1, 1, time.Second)
	waitForInboundGateways(t, sb1, 1, time.Second)

	ob2 := testGatewayOptionsFromToWithServers(t, "B", "A", sa1)
	ob2.Routes = RoutesFromStr(fmt.Sprintf("nats://127.0.0.1:%d", sb1.ClusterAddr().Port))
	setAccountUserPassInOptions(ob2, "$foo", "clientBFoo", "password")
	setAccountUserPassInOptions(ob2, "$bar", "clientB", "password")
	ob2.gatewaysSolicitDelay = time.Nanosecond // 0 would be default, so nano to connect asap
	sb2 := runGatewayServer(ob2)
	defer sb2.Shutdown()

	waitForOutboundGateways(t, sa1, 1, time.Second)
	waitForOutboundGateways(t, sb1, 1, time.Second)
	waitForOutboundGateways(t, sb2, 1, 2*time.Second)

	waitForInboundGateways(t, sa1, 2, time.Second)
	waitForInboundGateways(t, sb1, 1, time.Second)
	waitForInboundGateways(t, sb2, 0, time.Second)

	oa2 := testGatewayOptionsFromToWithServers(t, "A", "B", sb2)
	oa2.Routes = RoutesFromStr(fmt.Sprintf("nats://127.0.0.1:%d", sa1.ClusterAddr().Port))
	setAccountUserPassInOptions(oa2, "$foo", "clientA", "password")
	setAccountUserPassInOptions(oa2, "$bar", "yyyyyyy", "password")
	oa2.gatewaysSolicitDelay = time.Nanosecond // 0 would be default, so nano to connect asap
	sa2 := runGatewayServer(oa2)
	defer sa2.Shutdown()

	ensureGWConnectTo(t, sa2, "B", sb2)

	checkClusterFormed(t, sa1, sa2)
	checkClusterFormed(t, sb1, sb2)

	waitForOutboundGateways(t, sa1, 1, time.Second)
	waitForOutboundGateways(t, sb1, 1, time.Second)
	waitForOutboundGateways(t, sb2, 1, time.Second)
	waitForOutboundGateways(t, sa2, 1, 2*time.Second)

	waitForInboundGateways(t, sa1, 2, time.Second)
	waitForInboundGateways(t, sb1, 1, time.Second)
	waitForInboundGateways(t, sb2, 1, 2*time.Second)
	waitForInboundGateways(t, sa2, 0, time.Second)

	// Verification that we have what we wanted
	c := sa2.getOutboundGatewayConnection("B")
	if c == nil || c.opts.Name != sb2.ID() {
		t.Fatalf("A2 does not have outbound to B2")
	}
	c = getInboundGatewayConnection(sa2, "B")
	if c != nil {
		t.Fatalf("Bad setup")
	}
	c = sb2.getOutboundGatewayConnection("A")
	if c == nil || c.opts.Name != sa1.ID() {
		t.Fatalf("B2 does not have outbound to A1")
	}
	c = getInboundGatewayConnection(sb2, "A")
	if c == nil || c.opts.Name != sa2.ID() {
		t.Fatalf("Bad setup")
	}

	// Ok, so now that we have proper setup, do actual test!

	// Get accounts
	fooA1, _ := sa1.LookupAccount("$foo")
	barA1, _ := sa1.LookupAccount("$bar")
	fooA2, _ := sa2.LookupAccount("$foo")
	barA2, _ := sa2.LookupAccount("$bar")

	fooB1, _ := sb1.LookupAccount("$foo")
	barB1, _ := sb1.LookupAccount("$bar")
	fooB2, _ := sb2.LookupAccount("$foo")
	barB2, _ := sb2.LookupAccount("$bar")

	// Add in the service export for the requests. Make it public.
	fooA1.AddServiceExport("test.request", nil)
	fooA2.AddServiceExport("test.request", nil)
	fooB1.AddServiceExport("test.request", nil)
	fooB2.AddServiceExport("test.request", nil)

	// Add import abilities to server B's bar account from foo.
	if err := barB1.AddServiceImport(fooB1, "foo.request", "test.request"); err != nil {
		t.Fatalf("Error adding service import: %v", err)
	}
	if err := barB2.AddServiceImport(fooB2, "foo.request", "test.request"); err != nil {
		t.Fatalf("Error adding service import: %v", err)
	}
	// Same on A.
	if err := barA1.AddServiceImport(fooA1, "foo.request", "test.request"); err != nil {
		t.Fatalf("Error adding service import: %v", err)
	}
	if err := barA2.AddServiceImport(fooA2, "foo.request", "test.request"); err != nil {
		t.Fatalf("Error adding service import: %v", err)
	}

	// clientA will be connected to A2 and be the service endpoint and responder.
	a2URL := fmt.Sprintf("nats://clientA:password@127.0.0.1:%d", oa2.Port)
	clientA := natsConnect(t, a2URL)
	defer clientA.Close()

	subA := natsSubSync(t, clientA, "test.request")
	natsFlush(t, clientA)

	// Now setup client B on B1 who will do a sub from account $bar
	// that should map account $foo's foo subject.
	b1URL := fmt.Sprintf("nats://clientB:password@127.0.0.1:%d", ob1.Port)
	clientB := natsConnect(t, b1URL)
	defer clientB.Close()

	subB := natsSubSync(t, clientB, "reply")
	natsFlush(t, clientB)

	var msg *nats.Msg
	var err error
	for attempts := 1; attempts <= 2; attempts++ {
		// Send the request from clientB on foo.request,
		natsPubReq(t, clientB, "foo.request", "reply", []byte("hi"))
		natsFlush(t, clientB)

		// Expect the request on A
		msg, err = subA.NextMsg(time.Second)
		if err != nil {
			if attempts == 1 {
				// Since we are in interestOnly mode, it is possible
				// that server B did not receive the subscription
				// interest yet, so try again.
				continue
			}
			t.Fatalf("subA failed to get request: %v", err)
		}
		if msg.Subject != "test.request" || string(msg.Data) != "hi" {
			t.Fatalf("Unexpected message: %v", msg)
		}
		if msg.Reply == "reply" {
			t.Fatalf("Expected randomized reply, but got original")
		}
	}
	// Make sure we don't receive a second copy
	if msg, err := subA.NextMsg(100 * time.Millisecond); err != nats.ErrTimeout {
		t.Fatalf("Received unexpected message: %v", msg)
	}

	// Send reply
	natsPub(t, clientA, msg.Reply, []byte("ok"))
	natsFlush(t, clientA)

	msg, err = subB.NextMsg(time.Second)
	if err != nil {
		t.Fatalf("subB failed to get reply: %v", err)
	}
	if msg.Subject != "reply" || string(msg.Data) != "ok" {
		t.Fatalf("Unexpected message: %v", msg)
	}
	// Make sure we don't receive a second copy
	if msg, err := subB.NextMsg(100 * time.Millisecond); err != nats.ErrTimeout {
		t.Fatalf("Received unexpected message: %v", msg)
	}

	checkSubs := func(t *testing.T, acc *Account, srvName string, expected int) {
		t.Helper()
		checkFor(t, 2*time.Second, 10*time.Millisecond, func() error {
			if ts := acc.TotalSubs(); ts != expected {
				return fmt.Errorf("Number of subs is %d on acc=%s srv=%s, should be %v", ts, acc.Name, srvName, expected)
			}
			return nil
		})
	}
	checkSubs(t, fooA1, "A1", 1)
	checkSubs(t, barA1, "A1", 1)
	checkSubs(t, fooA2, "A2", 1)
	checkSubs(t, barA2, "A2", 1)
	checkSubs(t, fooB1, "B1", 1)
	checkSubs(t, barB1, "B1", 2)
	checkSubs(t, fooB2, "B2", 1)
	checkSubs(t, barB2, "B2", 2)

	// Speed up exiration
	err = fooA2.SetServiceExportResponseThreshold("test.request", 10*time.Millisecond)
	if err != nil {
		t.Fatalf("Error setting response threshold: %v", err)
	}
	err = fooB1.SetServiceExportResponseThreshold("test.request", 10*time.Millisecond)
	if err != nil {
		t.Fatalf("Error setting response threshold: %v", err)
	}

	// Send 100 requests from clientB on foo.request,
	for i := 0; i < 100; i++ {
		natsPubReq(t, clientB, "foo.request", "reply", []byte("hi"))
	}
	natsFlush(t, clientB)

	// Consume the requests, but don't reply to them...
	for i := 0; i < 100; i++ {
		if _, err := subA.NextMsg(time.Second); err != nil {
			t.Fatalf("subA did not receive request: %v", err)
		}
	}

	// Unsubsribe all and ensure counts go to 0.
	natsUnsub(t, subA)
	natsFlush(t, clientA)
	natsUnsub(t, subB)
	natsFlush(t, clientB)

	// We should expire because ttl.
	checkFor(t, 2*time.Second, 10*time.Millisecond, func() error {
		if nr := len(fooA1.exports.responses); nr != 0 {
			return fmt.Errorf("Number of responses is %d", nr)
		}
		return nil
	})

	checkSubs(t, fooA1, "A1", 0)
	checkSubs(t, fooA2, "A2", 0)
	checkSubs(t, fooB1, "B1", 1)
	checkSubs(t, fooB2, "B2", 1)

	checkSubs(t, barA1, "A1", 1)
	checkSubs(t, barA2, "A2", 1)
	checkSubs(t, barB1, "B1", 1)
	checkSubs(t, barB2, "B2", 1)

	// Check that this all work in interest-only mode.

	// We need at least a subscription on B2 otherwise when publishing
	// to subjects with no interest we would simply get an A-
	b2URL := fmt.Sprintf("nats://clientBFoo:password@127.0.0.1:%d", ob2.Port)
	clientB2 := natsConnect(t, b2URL)
	defer clientB2.Close()
	natsSubSync(t, clientB2, "not.used")
	natsFlush(t, clientB2)

	// Make A2 flood B2 with subjects that B2 is not interested in.
	for i := 0; i < 1100; i++ {
		natsPub(t, clientA, fmt.Sprintf("no.interest.%d", i), []byte("hello"))
	}
	natsFlush(t, clientA)
	// Wait for B2 to switch to interest-only
	checkGWInterestOnlyMode(t, sa2, "B", "$foo")

	subA = natsSubSync(t, clientA, "test.request")
	natsFlush(t, clientA)

	subB = natsSubSync(t, clientB, "reply")
	natsFlush(t, clientB)

	for attempts := 1; attempts <= 2; attempts++ {
		// Send the request from clientB on foo.request,
		natsPubReq(t, clientB, "foo.request", "reply", []byte("hi"))
		natsFlush(t, clientB)

		// Expect the request on A
		msg, err = subA.NextMsg(time.Second)
		if err != nil {
			if attempts == 1 {
				// Since we are in interestOnly mode, it is possible
				// that server B did not receive the subscription
				// interest yet, so try again.
				continue
			}
			t.Fatalf("subA failed to get request: %v", err)
		}
		if msg.Subject != "test.request" || string(msg.Data) != "hi" {
			t.Fatalf("Unexpected message: %v", msg)
		}
		if msg.Reply == "reply" {
			t.Fatalf("Expected randomized reply, but got original")
		}
	}

	// Check for duplicate message
	if msg, err := subA.NextMsg(100 * time.Millisecond); err != nats.ErrTimeout {
		t.Fatalf("Unexpected msg: %v", msg)
	}

	// Send reply
	natsPub(t, clientA, msg.Reply, []byte("ok"))
	natsFlush(t, clientA)

	msg, err = subB.NextMsg(time.Second)
	if err != nil {
		t.Fatalf("subB failed to get reply: %v", err)
	}
	if msg.Subject != "reply" || string(msg.Data) != "ok" {
		t.Fatalf("Unexpected message: %v", msg)
	}

	// Check for duplicate message
	if msg, err := subB.NextMsg(100 * time.Millisecond); err != nats.ErrTimeout {
		t.Fatalf("Unexpected msg: %v", msg)
	}
}

func TestGatewayServiceExportWithWildcards(t *testing.T) {
	// This test will have following setup:
	//
	//						     |- responder
	//                           |
	//              route        v
	//   [A1]<----------------->[A2]
	//   ^  |^                    |
	//   |gw| \______gw________ gw|
	//   |  v                  \  v
	//   [B1]<----------------->[B2]
	//    ^         route
	//    |
	//    |_ requestor
	//

	for _, test := range []struct {
		name   string
		public bool
	}{
		{
			name:   "public",
			public: true,
		},
		{
			name:   "private",
			public: false,
		},
	} {
		t.Run(test.name, func(t *testing.T) {

			// Setup first A1 and B1 to ensure that they have GWs
			// connections as described above.

			oa1 := testDefaultOptionsForGateway("A")
			setAccountUserPassInOptions(oa1, "$foo", "clientA", "password")
			setAccountUserPassInOptions(oa1, "$bar", "yyyyyyy", "password")
			sa1 := runGatewayServer(oa1)
			defer sa1.Shutdown()

			ob1 := testGatewayOptionsFromToWithServers(t, "B", "A", sa1)
			setAccountUserPassInOptions(ob1, "$foo", "xxxxxxx", "password")
			setAccountUserPassInOptions(ob1, "$bar", "clientB", "password")
			sb1 := runGatewayServer(ob1)
			defer sb1.Shutdown()

			waitForOutboundGateways(t, sa1, 1, time.Second)
			waitForOutboundGateways(t, sb1, 1, time.Second)

			waitForInboundGateways(t, sa1, 1, time.Second)
			waitForInboundGateways(t, sb1, 1, time.Second)

			ob2 := testGatewayOptionsFromToWithServers(t, "B", "A", sa1)
			ob2.Routes = RoutesFromStr(fmt.Sprintf("nats://127.0.0.1:%d", sb1.ClusterAddr().Port))
			setAccountUserPassInOptions(ob2, "$foo", "clientBFoo", "password")
			setAccountUserPassInOptions(ob2, "$bar", "clientB", "password")
			ob2.gatewaysSolicitDelay = time.Nanosecond // 0 would be default, so nano to connect asap
			sb2 := runGatewayServer(ob2)
			defer sb2.Shutdown()

			waitForOutboundGateways(t, sa1, 1, time.Second)
			waitForOutboundGateways(t, sb1, 1, time.Second)
			waitForOutboundGateways(t, sb2, 1, 2*time.Second)

			waitForInboundGateways(t, sa1, 2, time.Second)
			waitForInboundGateways(t, sb1, 1, time.Second)
			waitForInboundGateways(t, sb2, 0, time.Second)

			oa2 := testGatewayOptionsFromToWithServers(t, "A", "B", sb2)
			oa2.Routes = RoutesFromStr(fmt.Sprintf("nats://127.0.0.1:%d", sa1.ClusterAddr().Port))
			setAccountUserPassInOptions(oa2, "$foo", "clientA", "password")
			setAccountUserPassInOptions(oa2, "$bar", "yyyyyyy", "password")
			oa2.gatewaysSolicitDelay = time.Nanosecond // 0 would be default, so nano to connect asap
			sa2 := runGatewayServer(oa2)
			defer sa2.Shutdown()

			ensureGWConnectTo(t, sa2, "B", sb2)

			checkClusterFormed(t, sa1, sa2)
			checkClusterFormed(t, sb1, sb2)

			waitForOutboundGateways(t, sa1, 1, time.Second)
			waitForOutboundGateways(t, sb1, 1, time.Second)
			waitForOutboundGateways(t, sb2, 1, time.Second)
			waitForOutboundGateways(t, sa2, 1, 2*time.Second)

			waitForInboundGateways(t, sa1, 2, time.Second)
			waitForInboundGateways(t, sb1, 1, time.Second)
			waitForInboundGateways(t, sb2, 1, 2*time.Second)
			waitForInboundGateways(t, sa2, 0, time.Second)

			// Verification that we have what we wanted
			c := sa2.getOutboundGatewayConnection("B")
			if c == nil || c.opts.Name != sb2.ID() {
				t.Fatalf("A2 does not have outbound to B2")
			}
			c = getInboundGatewayConnection(sa2, "B")
			if c != nil {
				t.Fatalf("Bad setup")
			}
			c = sb2.getOutboundGatewayConnection("A")
			if c == nil || c.opts.Name != sa1.ID() {
				t.Fatalf("B2 does not have outbound to A1")
			}
			c = getInboundGatewayConnection(sb2, "A")
			if c == nil || c.opts.Name != sa2.ID() {
				t.Fatalf("Bad setup")
			}

			// Ok, so now that we have proper setup, do actual test!

			// Get accounts
			fooA1, _ := sa1.LookupAccount("$foo")
			barA1, _ := sa1.LookupAccount("$bar")
			fooA2, _ := sa2.LookupAccount("$foo")
			barA2, _ := sa2.LookupAccount("$bar")

			fooB1, _ := sb1.LookupAccount("$foo")
			barB1, _ := sb1.LookupAccount("$bar")
			fooB2, _ := sb2.LookupAccount("$foo")
			barB2, _ := sb2.LookupAccount("$bar")

			var accs []*Account
			// Add in the service export for the requests.
			if !test.public {
				accs = []*Account{barA1}
			}
			require_NoError(t, fooA1.AddServiceExport("ngs.update.*", accs))
			if !test.public {
				accs = []*Account{barA2}
			}
			require_NoError(t, fooA2.AddServiceExport("ngs.update.*", accs))
			if !test.public {
				accs = []*Account{barB1}
			}
			require_NoError(t, fooB1.AddServiceExport("ngs.update.*", accs))
			if !test.public {
				accs = []*Account{barB2}
			}
			require_NoError(t, fooB2.AddServiceExport("ngs.update.*", accs))

			// Add import abilities to server B's bar account from foo.
			if err := barB1.AddServiceImport(fooB1, "ngs.update", "ngs.update.$bar"); err != nil {
				t.Fatalf("Error adding service import: %v", err)
			}
			if err := barB2.AddServiceImport(fooB2, "ngs.update", "ngs.update.$bar"); err != nil {
				t.Fatalf("Error adding service import: %v", err)
			}
			// Same on A.
			if err := barA1.AddServiceImport(fooA1, "ngs.update", "ngs.update.$bar"); err != nil {
				t.Fatalf("Error adding service import: %v", err)
			}
			if err := barA2.AddServiceImport(fooA2, "ngs.update", "ngs.update.$bar"); err != nil {
				t.Fatalf("Error adding service import: %v", err)
			}

			// clientA will be connected to A2 and be the service endpoint and responder.
			a2URL := fmt.Sprintf("nats://clientA:password@127.0.0.1:%d", oa2.Port)
			clientA := natsConnect(t, a2URL)
			defer clientA.Close()

			subA := natsSubSync(t, clientA, "ngs.update.$bar")
			natsFlush(t, clientA)

			// Now setup client B on B1 who will do a sub from account $bar
			// that should map account $foo's foo subject.
			b1URL := fmt.Sprintf("nats://clientB:password@127.0.0.1:%d", ob1.Port)
			clientB := natsConnect(t, b1URL)
			defer clientB.Close()

			subB := natsSubSync(t, clientB, "reply")
			natsFlush(t, clientB)

			// Ensure the subscription is known by the server we're connected to.
			time.Sleep(100 * time.Millisecond)

			var msg *nats.Msg
			var err error
			for attempts := 1; attempts <= 2; attempts++ {
				// Send the request from clientB on foo.request,
				natsPubReq(t, clientB, "ngs.update", "reply", []byte("hi"))
				natsFlush(t, clientB)

				// Expect the request on A
				msg, err = subA.NextMsg(time.Second)
				if err != nil {
					if attempts == 1 {
						// Since we are in interestOnly mode, it is possible
						// that server B did not receive the subscription
						// interest yet, so try again.
						continue
					}
					t.Fatalf("subA failed to get request: %v", err)
				}
				if msg.Subject != "ngs.update.$bar" || string(msg.Data) != "hi" {
					t.Fatalf("Unexpected message: %v", msg)
				}
				if msg.Reply == "reply" {
					t.Fatalf("Expected randomized reply, but got original")
				}
			}
			// Make sure we don't receive a second copy
			if msg, err := subA.NextMsg(100 * time.Millisecond); err != nats.ErrTimeout {
				t.Fatalf("Received unexpected message: %v", msg)
			}

			// Send reply
			natsPub(t, clientA, msg.Reply, []byte("ok"))
			natsFlush(t, clientA)

			msg, err = subB.NextMsg(time.Second)
			if err != nil {
				t.Fatalf("subB failed to get reply: %v", err)
			}
			if msg.Subject != "reply" || string(msg.Data) != "ok" {
				t.Fatalf("Unexpected message: %v", msg)
			}
			// Make sure we don't receive a second copy
			if msg, err := subB.NextMsg(100 * time.Millisecond); err != nats.ErrTimeout {
				t.Fatalf("Received unexpected message: %v", msg)
			}

			checkSubs := func(t *testing.T, acc *Account, srvName string, expected int) {
				t.Helper()
				checkFor(t, 2*time.Second, 10*time.Millisecond, func() error {
					if ts := acc.TotalSubs(); ts != expected {
						return fmt.Errorf("Number of subs is %d on acc=%s srv=%s, should be %v", ts, acc.Name, srvName, expected)
					}
					return nil
				})
			}
			checkSubs(t, fooA1, "A1", 1)
			checkSubs(t, barA1, "A1", 1)
			checkSubs(t, fooA2, "A2", 1)
			checkSubs(t, barA2, "A2", 1)
			checkSubs(t, fooB1, "B1", 1)
			checkSubs(t, barB1, "B1", 2)
			checkSubs(t, fooB2, "B2", 1)
			checkSubs(t, barB2, "B2", 2)

			// Speed up expiration
			err = fooA1.SetServiceExportResponseThreshold("ngs.update.*", 10*time.Millisecond)
			if err != nil {
				t.Fatalf("Error setting response threshold: %v", err)
			}
			err = fooB1.SetServiceExportResponseThreshold("ngs.update.*", 10*time.Millisecond)
			if err != nil {
				t.Fatalf("Error setting response threshold: %v", err)
			}

			// Send 100 requests from clientB on foo.request,
			for i := 0; i < 100; i++ {
				natsPubReq(t, clientB, "ngs.update", "reply", []byte("hi"))
			}
			natsFlush(t, clientB)

			// Consume the requests, but don't reply to them...
			for i := 0; i < 100; i++ {
				if _, err := subA.NextMsg(time.Second); err != nil {
					t.Fatalf("subA did not receive request: %v", err)
				}
			}

			// Unsubscribe all and ensure counts go to 0.
			natsUnsub(t, subA)
			natsFlush(t, clientA)
			natsUnsub(t, subB)
			natsFlush(t, clientB)

			// We should expire because ttl.
			checkFor(t, 2*time.Second, 10*time.Millisecond, func() error {
				if nr := len(fooA1.exports.responses); nr != 0 {
					return fmt.Errorf("Number of responses is %d", nr)
				}
				return nil
			})

			checkSubs(t, fooA1, "A1", 0)
			checkSubs(t, fooA2, "A2", 0)
			checkSubs(t, fooB1, "B1", 1)
			checkSubs(t, fooB2, "B2", 1)

			checkSubs(t, barA1, "A1", 1)
			checkSubs(t, barA2, "A2", 1)
			checkSubs(t, barB1, "B1", 1)
			checkSubs(t, barB2, "B2", 1)

			// Check that this all work in interest-only mode.

			// We need at least a subscription on B2 otherwise when publishing
			// to subjects with no interest we would simply get an A-
			b2URL := fmt.Sprintf("nats://clientBFoo:password@127.0.0.1:%d", ob2.Port)
			clientB2 := natsConnect(t, b2URL)
			defer clientB2.Close()
			natsSubSync(t, clientB2, "not.used")
			natsFlush(t, clientB2)

			// Make A2 flood B2 with subjects that B2 is not interested in.
			for i := 0; i < 1100; i++ {
				natsPub(t, clientA, fmt.Sprintf("no.interest.%d", i), []byte("hello"))
			}
			natsFlush(t, clientA)

			// Wait for B2 to switch to interest-only
			checkGWInterestOnlyMode(t, sa2, "B", "$foo")

			subA = natsSubSync(t, clientA, "ngs.update.*")
			natsFlush(t, clientA)

			subB = natsSubSync(t, clientB, "reply")
			natsFlush(t, clientB)

			// Ensure the subscription is known by the server we're connected to.
			time.Sleep(100 * time.Millisecond)

			for attempts := 1; attempts <= 2; attempts++ {
				// Send the request from clientB on foo.request,
				natsPubReq(t, clientB, "ngs.update", "reply", []byte("hi"))
				natsFlush(t, clientB)

				// Expect the request on A
				msg, err = subA.NextMsg(time.Second)
				if err != nil {
					if attempts == 1 {
						// Since we are in interestOnly mode, it is possible
						// that server B did not receive the subscription
						// interest yet, so try again.
						continue
					}
					t.Fatalf("subA failed to get request: %v", err)
				}
				if msg.Subject != "ngs.update.$bar" || string(msg.Data) != "hi" {
					t.Fatalf("Unexpected message: %v", msg)
				}
				if msg.Reply == "reply" {
					t.Fatalf("Expected randomized reply, but got original")
				}
			}

			// Check for duplicate message
			if msg, err := subA.NextMsg(100 * time.Millisecond); err != nats.ErrTimeout {
				t.Fatalf("Unexpected msg: %v", msg)
			}

			// Send reply
			natsPub(t, clientA, msg.Reply, []byte("ok"))
			natsFlush(t, clientA)

			msg, err = subB.NextMsg(time.Second)
			if err != nil {
				t.Fatalf("subB failed to get reply: %v", err)
			}
			if msg.Subject != "reply" || string(msg.Data) != "ok" {
				t.Fatalf("Unexpected message: %v", msg)
			}

			// Check for duplicate message
			if msg, err := subB.NextMsg(100 * time.Millisecond); err != nats.ErrTimeout {
				t.Fatalf("Unexpected msg: %v", msg)
			}
		})
	}
}

// NOTE: if this fails for you and says only has <10 outbound, make sure ulimit for open files > 256.
func TestGatewayMemUsage(t *testing.T) {
	// Try to clean up.
	runtime.GC()
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	pta := m.TotalAlloc

	o := testDefaultOptionsForGateway("A")
	s := runGatewayServer(o)
	defer s.Shutdown()

	var servers []*Server
	servers = append(servers, s)

	numServers := 10
	for i := 0; i < numServers; i++ {
		rn := fmt.Sprintf("RG_%d", i+1)
		o := testGatewayOptionsFromToWithServers(t, rn, "A", s)
		s := runGatewayServer(o)
		defer s.Shutdown()
		servers = append(servers, s)
	}

	// Each server should have an outbound
	for _, s := range servers {
		waitForOutboundGateways(t, s, numServers, 2*time.Second)
	}
	// The first started server should have numServers inbounds (since
	// they all connect to it).
	waitForInboundGateways(t, s, numServers, 2*time.Second)

	// Calculate in MB what we are using now.
	const max = 50 * 1024 * 1024 // 50MB
	runtime.ReadMemStats(&m)
	used := m.TotalAlloc - pta
	if used > max {
		t.Fatalf("Cluster using too much memory, expect < 50MB, got %dMB", used/(1024*1024))
	}

	for _, s := range servers {
		s.Shutdown()
	}
}

func TestGatewayMapReplyOnlyForRecentSub(t *testing.T) {
	o2 := testDefaultOptionsForGateway("B")
	s2 := runGatewayServer(o2)
	defer s2.Shutdown()

	o1 := testGatewayOptionsFromToWithServers(t, "A", "B", s2)
	s1 := runGatewayServer(o1)
	defer s1.Shutdown()

	waitForOutboundGateways(t, s1, 1, time.Second)
	waitForOutboundGateways(t, s2, 1, time.Second)

	// Change s1's recent sub expiration default value
	s1.mu.Lock()
	s1.gateway.pasi.Lock()
	s1.gateway.recSubExp = 100 * time.Millisecond
	s1.gateway.pasi.Unlock()
	s1.mu.Unlock()

	// Setup a replier on s2
	nc2 := natsConnect(t, fmt.Sprintf("nats://%s:%d", o2.Host, o2.Port))
	defer nc2.Close()
	errCh := make(chan error, 1)
	natsSub(t, nc2, "foo", func(m *nats.Msg) {
		// Send reply regardless..
		nc2.Publish(m.Reply, []byte("reply"))
		// Check that reply given to application is not mapped.
		if !strings.HasPrefix(m.Reply, nats.InboxPrefix) {
			errCh <- fmt.Errorf("Reply expected to have normal inbox, got %v", m.Reply)
			return
		}
		errCh <- nil
	})
	natsFlush(t, nc2)
	checkExpectedSubs(t, 1, s2)

	// Create requestor on s1
	nc1 := natsConnect(t, fmt.Sprintf("nats://%s:%d", o1.Host, o1.Port))
	defer nc1.Close()
	// Send first request, reply should be mapped
	nc1.Request("foo", []byte("msg1"), time.Second)
	// Wait more than the recent sub expiration (that we have set to 100ms)
	time.Sleep(200 * time.Millisecond)
	// Send second request (reply should not be mapped)
	nc1.Request("foo", []byte("msg2"), time.Second)

	select {
	case e := <-errCh:
		if e != nil {
			t.Fatal(e.Error())
		}
	case <-time.After(time.Second):
		t.Fatalf("Did not get replies")
	}
}

type delayedWriteConn struct {
	sync.Mutex
	net.Conn
	bytes [][]byte
	delay bool
	wg    sync.WaitGroup
}

func (c *delayedWriteConn) Write(b []byte) (int, error) {
	c.Lock()
	defer c.Unlock()
	if c.delay || len(c.bytes) > 0 {
		c.bytes = append(c.bytes, append([]byte(nil), b...))
		c.wg.Add(1)
		go func() {
			defer c.wg.Done()
			c.Lock()
			defer c.Unlock()
			if c.delay {
				c.Unlock()
				time.Sleep(100 * time.Millisecond)
				c.Lock()
			}
			if len(c.bytes) > 0 {
				b = c.bytes[0]
				c.bytes = c.bytes[1:]
				c.Conn.Write(b)
			}
		}()
		return len(b), nil
	}
	return c.Conn.Write(b)
}

// This test uses a single account and makes sure that when
// a reply subject is prefixed with $GR it comes back to
// the origin cluster and delivered to proper reply subject
// there, but also to subscribers on that reply subject
// on the other cluster.
func TestGatewaySendReplyAcrossGateways(t *testing.T) {
	for _, test := range []struct {
		name     string
		poolSize int
		peracc   bool
	}{
		{"no pooling", -1, false},
		{"pooling", 5, false},
		{"per account", 0, true},
	} {
		t.Run(test.name, func(t *testing.T) {
			ob := testDefaultOptionsForGateway("B")
			ob.Accounts = []*Account{NewAccount("ACC")}
			ob.Users = []*User{{Username: "user", Password: "pwd", Account: ob.Accounts[0]}}
			sb := runGatewayServer(ob)
			defer sb.Shutdown()

			oa1 := testGatewayOptionsFromToWithServers(t, "A", "B", sb)
			oa1.Cluster.PoolSize = test.poolSize
			if test.peracc {
				oa1.Cluster.PinnedAccounts = []string{"ACC"}
			}
			oa1.Accounts = []*Account{NewAccount("ACC")}
			oa1.Users = []*User{{Username: "user", Password: "pwd", Account: oa1.Accounts[0]}}
			sa1 := runGatewayServer(oa1)
			defer sa1.Shutdown()

			waitForOutboundGateways(t, sb, 1, time.Second)
			waitForInboundGateways(t, sb, 1, time.Second)
			waitForOutboundGateways(t, sa1, 1, time.Second)
			waitForInboundGateways(t, sa1, 1, time.Second)

			// Now start another server in cluster "A". This will allow us
			// to test the reply from cluster "B" coming back directly to
			// the server where the request originates, and indirectly through
			// route.
			oa2 := testGatewayOptionsFromToWithServers(t, "A", "B", sb)
			oa2.Cluster.PoolSize = test.poolSize
			if test.peracc {
				oa2.Cluster.PinnedAccounts = []string{"ACC"}
			}
			oa2.Accounts = []*Account{NewAccount("ACC")}
			oa2.Users = []*User{{Username: "user", Password: "pwd", Account: oa2.Accounts[0]}}
			oa2.Routes = RoutesFromStr(fmt.Sprintf("nats://%s:%d", oa1.Cluster.Host, oa1.Cluster.Port))
			sa2 := runGatewayServer(oa2)
			defer sa2.Shutdown()

			waitForOutboundGateways(t, sa2, 1, time.Second)
			waitForInboundGateways(t, sb, 2, time.Second)
			checkClusterFormed(t, sa1, sa2)

			replySubj := "bar"

			// Setup a responder on sb
			ncb := natsConnect(t, fmt.Sprintf("nats://user:pwd@%s:%d", ob.Host, ob.Port))
			defer ncb.Close()
			natsSub(t, ncb, "foo", func(m *nats.Msg) {
				m.Respond([]byte("reply"))
			})
			// Set a subscription on the reply subject on sb
			subSB := natsSubSync(t, ncb, replySubj)
			natsFlush(t, ncb)
			checkExpectedSubs(t, 2, sb)

			testReqReply := func(t *testing.T, host string, port int, createSubOnA bool) {
				t.Helper()
				nca := natsConnect(t, fmt.Sprintf("nats://user:pwd@%s:%d", host, port))
				defer nca.Close()
				if createSubOnA {
					subSA := natsSubSync(t, nca, replySubj)
					natsPubReq(t, nca, "foo", replySubj, []byte("hello"))
					natsNexMsg(t, subSA, time.Second)
					// Check for duplicates
					if _, err := subSA.NextMsg(50 * time.Millisecond); err == nil {
						t.Fatalf("Received duplicate message on subSA!")
					}
				} else {
					natsPubReq(t, nca, "foo", replySubj, []byte("hello"))
				}
				natsNexMsg(t, subSB, time.Second)
				// Check for duplicates
				if _, err := subSB.NextMsg(50 * time.Millisecond); err == nil {
					t.Fatalf("Received duplicate message on subSB!")
				}
			}
			// Create requestor on sa1 to check for direct reply from GW:
			testReqReply(t, oa1.Host, oa1.Port, true)
			// Wait for subscription to be gone...
			checkExpectedSubs(t, 0, sa1)
			// Now create requestor on sa2, it will receive reply through sa1.
			testReqReply(t, oa2.Host, oa2.Port, true)
			checkExpectedSubs(t, 0, sa1)
			checkExpectedSubs(t, 0, sa2)

			// Now issue requests but without any interest in the requestor's
			// origin cluster and make sure the other cluster gets the reply.
			testReqReply(t, oa1.Host, oa1.Port, false)
			testReqReply(t, oa2.Host, oa2.Port, false)

			// There is a possible race between sa2 sending the RS+ for the
			// subscription on the reply subject, and the GW reply making it
			// to sa1 before the RS+ is processed there.
			// We are going to force this race by making the route connection
			// block as needed.

			acc, _ := sa2.LookupAccount("ACC")
			acc.mu.RLock()
			api := acc.routePoolIdx
			acc.mu.RUnlock()

			var route *client
			sa2.mu.Lock()
			if test.peracc {
				if conns, ok := sa2.accRoutes["ACC"]; ok {
					for _, r := range conns {
						route = r
						break
					}
				}
			} else if test.poolSize > 0 {
				sa2.forEachRoute(func(r *client) {
					r.mu.Lock()
					if r.route.poolIdx == api {
						route = r
					}
					r.mu.Unlock()
				})
			} else if r := getFirstRoute(sa2); r != nil {
				route = r
			}
			sa2.mu.Unlock()
			route.mu.Lock()
			routeConn := &delayedWriteConn{
				Conn: route.nc,
				wg:   sync.WaitGroup{},
			}
			route.nc = routeConn
			route.mu.Unlock()

			delayRoute := func() {
				routeConn.Lock()
				routeConn.delay = true
				routeConn.Unlock()
			}
			stopDelayRoute := func() {
				routeConn.Lock()
				routeConn.delay = false
				wg := &routeConn.wg
				routeConn.Unlock()
				wg.Wait()
			}

			delayRoute()
			testReqReply(t, oa2.Host, oa2.Port, true)
			stopDelayRoute()

			// Same test but now we have a local interest on the reply subject
			// on sa1 to make sure that interest there does not prevent sending
			// the RMSG to sa2, which is the origin of the request.
			checkExpectedSubs(t, 0, sa1)
			checkExpectedSubs(t, 0, sa2)
			nca1 := natsConnect(t, fmt.Sprintf("nats://user:pwd@%s:%d", oa1.Host, oa1.Port))
			defer nca1.Close()
			subSA1 := natsSubSync(t, nca1, replySubj)
			natsFlush(t, nca1)
			checkExpectedSubs(t, 1, sa1)
			checkExpectedSubs(t, 1, sa2)

			delayRoute()
			testReqReply(t, oa2.Host, oa2.Port, true)
			stopDelayRoute()

			natsNexMsg(t, subSA1, time.Second)
		})
	}
}

// This test will have a requestor on cluster A and responder
// on cluster B, but when the responder sends the response,
// it will also have a reply subject to receive a response
// for the response.
func TestGatewayPingPongReplyAcrossGateways(t *testing.T) {
	ob := testDefaultOptionsForGateway("B")
	ob.Accounts = []*Account{NewAccount("ACC")}
	ob.Users = []*User{{Username: "user", Password: "pwd", Account: ob.Accounts[0]}}
	sb := runGatewayServer(ob)
	defer sb.Shutdown()

	oa1 := testGatewayOptionsFromToWithServers(t, "A", "B", sb)
	oa1.Accounts = []*Account{NewAccount("ACC")}
	oa1.Users = []*User{{Username: "user", Password: "pwd", Account: oa1.Accounts[0]}}
	sa1 := runGatewayServer(oa1)
	defer sa1.Shutdown()

	waitForOutboundGateways(t, sb, 1, time.Second)
	waitForInboundGateways(t, sb, 1, time.Second)
	waitForOutboundGateways(t, sa1, 1, time.Second)
	waitForInboundGateways(t, sa1, 1, time.Second)

	// Now start another server in cluster "A". This will allow us
	// to test the reply from cluster "B" coming back directly to
	// the server where the request originates, and indirectly through
	// route.
	oa2 := testGatewayOptionsFromToWithServers(t, "A", "B", sb)
	oa2.Accounts = []*Account{NewAccount("ACC")}
	oa2.Users = []*User{{Username: "user", Password: "pwd", Account: oa2.Accounts[0]}}
	oa2.Routes = RoutesFromStr(fmt.Sprintf("nats://%s:%d", oa1.Cluster.Host, oa1.Cluster.Port))
	sa2 := runGatewayServer(oa2)
	defer sa2.Shutdown()

	waitForOutboundGateways(t, sa2, 1, time.Second)
	waitForInboundGateways(t, sb, 2, time.Second)
	checkClusterFormed(t, sa1, sa2)

	// Setup a responder on sb
	ncb := natsConnect(t, fmt.Sprintf("nats://user:pwd@%s:%d", ob.Host, ob.Port))
	defer ncb.Close()
	sbReplySubj := "sbreply"
	subSB := natsSubSync(t, ncb, sbReplySubj)
	natsSub(t, ncb, "foo", func(m *nats.Msg) {
		ncb.PublishRequest(m.Reply, sbReplySubj, []byte("sb reply"))
	})
	natsFlush(t, ncb)
	checkExpectedSubs(t, 2, sb)

	testReqReply := func(t *testing.T, host string, port int) {
		t.Helper()
		nca := natsConnect(t, fmt.Sprintf("nats://user:pwd@%s:%d", host, port))
		defer nca.Close()
		msg, err := nca.Request("foo", []byte("sa request"), time.Second)
		if err != nil {
			t.Fatalf("Did not get response: %v", err)
		}
		// Check response from sb, it should have content "sb reply" and
		// reply subject should not have GW prefix
		if string(msg.Data) != "sb reply" || msg.Reply != sbReplySubj {
			t.Fatalf("Unexpected message from sb: %+v", msg)
		}
		// Now send our own reply:
		nca.Publish(msg.Reply, []byte("sa reply"))
		// And make sure that subS2 receives it...
		msg = natsNexMsg(t, subSB, time.Second)
		if string(msg.Data) != "sa reply" || msg.Reply != _EMPTY_ {
			t.Fatalf("Unexpected message from sa: %v", msg)
		}
	}
	// Create requestor on sa1 to check for direct reply from GW:
	testReqReply(t, oa1.Host, oa1.Port)
	// Now from sa2 to see reply coming from route (sa1)
	testReqReply(t, oa2.Host, oa2.Port)
}

// Similar to TestGatewaySendReplyAcrossGateways, but this time
// with service import.
func TestGatewaySendReplyAcrossGatewaysServiceImport(t *testing.T) {
	ob := testDefaultOptionsForGateway("B")
	setAccountUserPassInOptions(ob, "$foo", "clientBFoo", "password")
	setAccountUserPassInOptions(ob, "$bar", "clientBBar", "password")
	sb := runGatewayServer(ob)
	defer sb.Shutdown()

	oa1 := testGatewayOptionsFromToWithServers(t, "A", "B", sb)
	oa1.Cluster.PoolSize = 1
	setAccountUserPassInOptions(oa1, "$foo", "clientAFoo", "password")
	setAccountUserPassInOptions(oa1, "$bar", "clientABar", "password")
	sa1 := runGatewayServer(oa1)
	defer sa1.Shutdown()

	waitForOutboundGateways(t, sb, 1, time.Second)
	waitForInboundGateways(t, sb, 1, time.Second)
	waitForOutboundGateways(t, sa1, 1, time.Second)
	waitForInboundGateways(t, sa1, 1, time.Second)

	oa2 := testGatewayOptionsFromToWithServers(t, "A", "B", sb)
	oa2.Cluster.PoolSize = 1
	setAccountUserPassInOptions(oa2, "$foo", "clientAFoo", "password")
	setAccountUserPassInOptions(oa2, "$bar", "clientABar", "password")
	oa2.Routes = RoutesFromStr(fmt.Sprintf("nats://%s:%d", oa1.Cluster.Host, oa1.Cluster.Port))
	sa2 := runGatewayServer(oa2)
	defer sa2.Shutdown()

	oa3 := testGatewayOptionsFromToWithServers(t, "A", "B", sb)
	oa3.Cluster.PoolSize = 1
	setAccountUserPassInOptions(oa3, "$foo", "clientAFoo", "password")
	setAccountUserPassInOptions(oa3, "$bar", "clientABar", "password")
	oa3.Routes = RoutesFromStr(fmt.Sprintf("nats://%s:%d", oa1.Cluster.Host, oa1.Cluster.Port))
	sa3 := runGatewayServer(oa3)
	defer sa3.Shutdown()

	waitForOutboundGateways(t, sa2, 1, time.Second)
	waitForOutboundGateways(t, sa3, 1, time.Second)
	waitForInboundGateways(t, sb, 3, time.Second)
	checkClusterFormed(t, sa1, sa2, sa3)

	// Setup account on B
	fooB, _ := sb.LookupAccount("$foo")
	// Add in the service export for the requests. Make it public.
	fooB.AddServiceExport("foo.request", nil)

	// Setup accounts on sa1, sa2 and sa3
	setupAccsOnA := func(s *Server) {
		// Get accounts
		fooA, _ := s.LookupAccount("$foo")
		barA, _ := s.LookupAccount("$bar")
		// Add in the service export for the requests. Make it public.
		fooA.AddServiceExport("foo.request", nil)
		// Add import abilities to server A's bar account from foo.
		if err := barA.AddServiceImport(fooA, "bar.request", "foo.request"); err != nil {
			t.Fatalf("Error adding service import: %v", err)
		}
	}
	setupAccsOnA(sa1)
	setupAccsOnA(sa2)
	setupAccsOnA(sa3)

	// clientB will be connected to sb and be the service endpoint and responder.
	bURL := fmt.Sprintf("nats://clientBFoo:password@127.0.0.1:%d", ob.Port)
	clientBFoo := natsConnect(t, bURL)
	defer clientBFoo.Close()
	subBFoo := natsSubSync(t, clientBFoo, "foo.request")
	natsFlush(t, clientBFoo)

	// Create another client on B for account $bar that will listen to
	// the reply subject.
	bURL = fmt.Sprintf("nats://clientBBar:password@127.0.0.1:%d", ob.Port)
	clientBBar := natsConnect(t, bURL)
	defer clientBBar.Close()
	replySubj := "reply"
	subBReply := natsSubSync(t, clientBBar, replySubj)
	natsFlush(t, clientBBar)

	testServiceImport := func(t *testing.T, host string, port int) {
		t.Helper()
		bURL := fmt.Sprintf("nats://clientABar:password@%s:%d", host, port)
		clientABar := natsConnect(t, bURL)
		defer clientABar.Close()
		subAReply := natsSubSync(t, clientABar, replySubj)
		natsFlush(t, clientABar)

		// Send the request from clientA on bar.request, which
		// will be translated to foo.request and sent over.
		natsPubReq(t, clientABar, "bar.request", replySubj, []byte("hi"))
		natsFlush(t, clientABar)

		// Expect the request to be received on subAFoo
		msg, err := subBFoo.NextMsg(time.Second)
		if err != nil {
			t.Fatalf("subBFoo failed to get request: %v", err)
		}
		if msg.Subject != "foo.request" || string(msg.Data) != "hi" {
			t.Fatalf("Unexpected message: %v", msg)
		}
		if msg.Reply == replySubj {
			t.Fatalf("Expected randomized reply, but got original")
		}

		// Check for duplicate message
		if msg, err := subBFoo.NextMsg(100 * time.Millisecond); err != nats.ErrTimeout {
			t.Fatalf("Unexpected msg: %v", msg)
		}

		// Send reply
		natsPub(t, clientBFoo, msg.Reply, []byte("ok-42"))
		natsFlush(t, clientBFoo)

		// Now check that the subscription on the reply receives the message...
		checkReply := func(t *testing.T, sub *nats.Subscription) {
			t.Helper()
			msg, err = sub.NextMsg(time.Second)
			if err != nil {
				t.Fatalf("sub failed to get reply: %v", err)
			}
			if msg.Subject != replySubj || string(msg.Data) != "ok-42" {
				t.Fatalf("Unexpected message: %v", msg)
			}
		}
		// Check subscription on A (where the request originated)
		checkReply(t, subAReply)
		// And the subscription on B (where the responder is located)
		checkReply(t, subBReply)
	}

	// We check the service import with GW working ok with either
	// direct connection between the responder's server to the
	// requestor's server and also through routes.
	testServiceImport(t, oa1.Host, oa1.Port)
	testServiceImport(t, oa2.Host, oa2.Port)
	// sa1 is the one receiving the reply from GW between B and A.
	// Check that the server routes directly to the server
	// with the interest.
	checkRoute := func(t *testing.T, s *Server, expected int64) {
		t.Helper()
		s.mu.Lock()
		defer s.mu.Unlock()
		s.forEachRoute(func(r *client) {
			r.mu.Lock()
			if r.route.remoteID != sa1.ID() {
				r.mu.Unlock()
				return
			}
			inMsgs := atomic.LoadInt64(&r.inMsgs)
			r.mu.Unlock()
			if inMsgs != expected {
				t.Fatalf("Expected %v incoming msgs, got %v", expected, inMsgs)
			}
		})
	}
	// Wait a bit to make sure that we don't have a loop that
	// cause messages to be routed more than needed.
	time.Sleep(100 * time.Millisecond)
	checkRoute(t, sa2, 1)
	checkRoute(t, sa3, 0)

	testServiceImport(t, oa3.Host, oa3.Port)
	// Wait a bit to make sure that we don't have a loop that
	// cause messages to be routed more than needed.
	time.Sleep(100 * time.Millisecond)
	checkRoute(t, sa2, 1)
	checkRoute(t, sa3, 1)
}

func TestGatewayClientsDontReceiveMsgsOnGWPrefix(t *testing.T) {
	ob := testDefaultOptionsForGateway("B")
	sb := runGatewayServer(ob)
	defer sb.Shutdown()

	oa := testGatewayOptionsFromToWithServers(t, "A", "B", sb)
	sa := runGatewayServer(oa)
	defer sa.Shutdown()

	waitForOutboundGateways(t, sa, 1, time.Second)
	waitForInboundGateways(t, sa, 1, time.Second)
	waitForOutboundGateways(t, sb, 1, time.Second)
	waitForInboundGateways(t, sb, 1, time.Second)

	// Setup a responder on sb
	ncb := natsConnect(t, fmt.Sprintf("nats://%s:%d", ob.Host, ob.Port))
	defer ncb.Close()
	natsSub(t, ncb, "foo", func(m *nats.Msg) {
		if strings.HasPrefix(m.Reply, gwReplyPrefix) {
			m.Respond([]byte(fmt.Sprintf("-ERR: received request with mapped reply subject %q", m.Reply)))
		} else {
			m.Respond([]byte("+OK: reply"))
		}
	})
	// And create a sub on ">" that should not get the $GR reply.
	subSB := natsSubSync(t, ncb, ">")
	natsFlush(t, ncb)
	checkExpectedSubs(t, 2, sb)

	nca := natsConnect(t, fmt.Sprintf("nats://%s:%d", oa.Host, oa.Port))
	defer nca.Close()
	msg, err := nca.Request("foo", []byte("request"), time.Second)
	if err != nil {
		t.Fatalf("Did not get response: %v", err)
	}
	if string(msg.Data) != "+OK: reply" {
		t.Fatalf("Error from responder: %q", msg.Data)
	}

	// subSB would have also received the request, so drop that one.
	msg = natsNexMsg(t, subSB, time.Second)
	if string(msg.Data) != "request" {
		t.Fatalf("Wrong request: %q", msg.Data)
	}
	// Once sa gets the direct reply, it should resend the reply
	// with normal subject. So subSB should get the message with
	// a subject that does not start with $GNR prefix.
	msg = natsNexMsg(t, subSB, time.Second)
	if string(msg.Data) != "+OK: reply" || strings.HasPrefix(msg.Subject, gwReplyPrefix) {
		t.Fatalf("Unexpected message from sa: %v", msg)
	}
	// Check no more message...
	if m, err := subSB.NextMsg(100 * time.Millisecond); m != nil || err == nil {
		t.Fatalf("Expected only 1 message, got %+v", m)
	}
}

func TestGatewayNoAccInterestThenQSubThenRegularSub(t *testing.T) {
	GatewayDoNotForceInterestOnlyMode(true)
	defer GatewayDoNotForceInterestOnlyMode(false)

	ob := testDefaultOptionsForGateway("B")
	sb := runGatewayServer(ob)
	defer sb.Shutdown()

	oa := testGatewayOptionsFromToWithServers(t, "A", "B", sb)
	sa := runGatewayServer(oa)
	defer sa.Shutdown()

	waitForOutboundGateways(t, sa, 1, time.Second)
	waitForInboundGateways(t, sa, 1, time.Second)
	waitForOutboundGateways(t, sb, 1, time.Second)
	waitForInboundGateways(t, sb, 1, time.Second)

	// Connect on A and send a message
	ncA := natsConnect(t, fmt.Sprintf("nats://%s:%d", oa.Host, oa.Port))
	defer ncA.Close()
	natsPub(t, ncA, "foo", []byte("hello"))
	natsFlush(t, ncA)

	// expect an A- on return
	gwb := sa.getOutboundGatewayConnection("B")
	checkForAccountNoInterest(t, gwb, globalAccountName, true, time.Second)

	// Create a connection o B, and create a queue sub first
	ncB := natsConnect(t, fmt.Sprintf("nats://%s:%d", ob.Host, ob.Port))
	defer ncB.Close()
	qsub := natsQueueSubSync(t, ncB, "bar", "queue")
	natsFlush(t, ncB)

	// A should have received a queue interest
	checkForRegisteredQSubInterest(t, sa, "B", globalAccountName, "bar", 1, time.Second)

	// Now on B, create a regular sub
	sub := natsSubSync(t, ncB, "baz")
	natsFlush(t, ncB)

	// From A now, produce a message on each subject and
	// expect both subs to receive their message.
	msgForQSub := []byte("msg_qsub")
	natsPub(t, ncA, "bar", msgForQSub)
	natsFlush(t, ncA)

	if msg := natsNexMsg(t, qsub, time.Second); !bytes.Equal(msgForQSub, msg.Data) {
		t.Fatalf("Expected msg for queue sub to be %q, got %q", msgForQSub, msg.Data)
	}

	// Publish for the regular sub
	msgForSub := []byte("msg_sub")
	natsPub(t, ncA, "baz", msgForSub)
	natsFlush(t, ncA)

	if msg := natsNexMsg(t, sub, time.Second); !bytes.Equal(msgForSub, msg.Data) {
		t.Fatalf("Expected msg for sub to be %q, got %q", msgForSub, msg.Data)
	}
}

// Similar to TestGatewayNoAccInterestThenQSubThenRegularSub but simulate
// older incorrect behavior.
func TestGatewayHandleUnexpectedASubUnsub(t *testing.T) {
	GatewayDoNotForceInterestOnlyMode(true)
	defer GatewayDoNotForceInterestOnlyMode(false)

	ob := testDefaultOptionsForGateway("B")
	sb := runGatewayServer(ob)
	defer sb.Shutdown()

	oa := testGatewayOptionsFromToWithServers(t, "A", "B", sb)
	sa := runGatewayServer(oa)
	defer sa.Shutdown()

	waitForOutboundGateways(t, sa, 1, time.Second)
	waitForInboundGateways(t, sa, 1, time.Second)
	waitForOutboundGateways(t, sb, 1, time.Second)
	waitForInboundGateways(t, sb, 1, time.Second)

	// Connect on A and send a message
	ncA := natsConnect(t, fmt.Sprintf("nats://%s:%d", oa.Host, oa.Port))
	defer ncA.Close()
	natsPub(t, ncA, "foo", []byte("hello"))
	natsFlush(t, ncA)

	// expect an A- on return
	gwb := sa.getOutboundGatewayConnection("B")
	checkForAccountNoInterest(t, gwb, globalAccountName, true, time.Second)

	// Create a connection o B, and create a queue sub first
	ncB := natsConnect(t, fmt.Sprintf("nats://%s:%d", ob.Host, ob.Port))
	defer ncB.Close()
	qsub := natsQueueSubSync(t, ncB, "bar", "queue")
	natsFlush(t, ncB)

	// A should have received a queue interest
	checkForRegisteredQSubInterest(t, sa, "B", globalAccountName, "bar", 1, time.Second)

	// Now on B, create a regular sub
	sub := natsSubSync(t, ncB, "baz")
	natsFlush(t, ncB)
	// and reproduce old, wrong, behavior that would have resulted in sending an A-
	gwA := getInboundGatewayConnection(sb, "A")
	gwA.mu.Lock()
	gwA.enqueueProto([]byte("A- $G\r\n"))
	gwA.mu.Unlock()

	// From A now, produce a message on each subject and
	// expect both subs to receive their message.
	msgForQSub := []byte("msg_qsub")
	natsPub(t, ncA, "bar", msgForQSub)
	natsFlush(t, ncA)

	if msg := natsNexMsg(t, qsub, time.Second); !bytes.Equal(msgForQSub, msg.Data) {
		t.Fatalf("Expected msg for queue sub to be %q, got %q", msgForQSub, msg.Data)
	}

	// Publish for the regular sub
	msgForSub := []byte("msg_sub")
	natsPub(t, ncA, "baz", msgForSub)
	natsFlush(t, ncA)

	if msg := natsNexMsg(t, sub, time.Second); !bytes.Equal(msgForSub, msg.Data) {
		t.Fatalf("Expected msg for sub to be %q, got %q", msgForSub, msg.Data)
	}

	// Remove all subs on B.
	qsub.Unsubscribe()
	sub.Unsubscribe()
	ncB.Flush()

	// Produce a message from A expect A-
	natsPub(t, ncA, "foo", []byte("hello"))
	natsFlush(t, ncA)

	// expect an A- on return
	checkForAccountNoInterest(t, gwb, globalAccountName, true, time.Second)

	// Simulate B sending another A-, on A account no interest should remain same.
	gwA.mu.Lock()
	gwA.enqueueProto([]byte("A- $G\r\n"))
	gwA.mu.Unlock()

	checkForAccountNoInterest(t, gwb, globalAccountName, true, time.Second)

	// Create a queue sub on B
	qsub = natsQueueSubSync(t, ncB, "bar", "queue")
	natsFlush(t, ncB)

	checkForRegisteredQSubInterest(t, sa, "B", globalAccountName, "bar", 1, time.Second)

	// Make B send an A+ and verify that we sitll have the registered qsub interest
	gwA.mu.Lock()
	gwA.enqueueProto([]byte("A+ $G\r\n"))
	gwA.mu.Unlock()

	// Give a chance to A to possibly misbehave when receiving this proto
	time.Sleep(250 * time.Millisecond)
	// Now check interest is still there
	checkForRegisteredQSubInterest(t, sa, "B", globalAccountName, "bar", 1, time.Second)

	qsub.Unsubscribe()
	natsFlush(t, ncB)
	checkForRegisteredQSubInterest(t, sa, "B", globalAccountName, "bar", 0, time.Second)

	// Send A-, server A should set entry to nil
	gwA.mu.Lock()
	gwA.enqueueProto([]byte("A- $G\r\n"))
	gwA.mu.Unlock()
	checkForAccountNoInterest(t, gwb, globalAccountName, true, time.Second)

	// Send A+ and entry should be removed since there is no longer reason to
	// keep the entry.
	gwA.mu.Lock()
	gwA.enqueueProto([]byte("A+ $G\r\n"))
	gwA.mu.Unlock()
	checkForAccountNoInterest(t, gwb, globalAccountName, false, time.Second)

	// Last A+ should not change because account already removed from map.
	gwA.mu.Lock()
	gwA.enqueueProto([]byte("A+ $G\r\n"))
	gwA.mu.Unlock()
	checkForAccountNoInterest(t, gwb, globalAccountName, false, time.Second)
}

type captureGWInterestSwitchLogger struct {
	DummyLogger
	imss []string
}

func (l *captureGWInterestSwitchLogger) Debugf(format string, args ...any) {
	l.Lock()
	msg := fmt.Sprintf(format, args...)
	if strings.Contains(msg, fmt.Sprintf("switching account %q to %s mode", globalAccountName, InterestOnly)) ||
		strings.Contains(msg, fmt.Sprintf("switching account %q to %s mode complete", globalAccountName, InterestOnly)) {
		l.imss = append(l.imss, msg)
	}
	l.Unlock()
}

func TestGatewayLogAccountInterestModeSwitch(t *testing.T) {
	GatewayDoNotForceInterestOnlyMode(true)
	defer GatewayDoNotForceInterestOnlyMode(false)

	ob := testDefaultOptionsForGateway("B")
	sb := runGatewayServer(ob)
	defer sb.Shutdown()

	logB := &captureGWInterestSwitchLogger{}
	sb.SetLogger(logB, true, true)

	oa := testGatewayOptionsFromToWithServers(t, "A", "B", sb)
	sa := runGatewayServer(oa)
	defer sa.Shutdown()

	logA := &captureGWInterestSwitchLogger{}
	sa.SetLogger(logA, true, true)

	waitForOutboundGateways(t, sa, 1, 2*time.Second)
	waitForInboundGateways(t, sa, 1, 2*time.Second)
	waitForOutboundGateways(t, sb, 1, 2*time.Second)
	waitForInboundGateways(t, sb, 1, 2*time.Second)

	ncB := natsConnect(t, fmt.Sprintf("nats://127.0.0.1:%d", ob.Port))
	defer ncB.Close()
	natsSubSync(t, ncB, "foo")
	natsFlush(t, ncB)

	ncA := natsConnect(t, fmt.Sprintf("nats://127.0.0.1:%d", oa.Port))
	defer ncA.Close()
	for i := 0; i < gatewayMaxRUnsubBeforeSwitch+10; i++ {
		subj := fmt.Sprintf("bar.%d", i)
		natsPub(t, ncA, subj, []byte("hello"))
	}
	natsFlush(t, ncA)

	gwA := getInboundGatewayConnection(sb, "A")
	checkFor(t, 2*time.Second, 15*time.Millisecond, func() error {
		mode := Optimistic
		gwA.mu.Lock()
		e := gwA.gw.insim[globalAccountName]
		if e != nil {
			mode = e.mode
		}
		gwA.mu.Unlock()
		if mode != InterestOnly {
			return fmt.Errorf("not switched yet")
		}
		return nil
	})

	checkGWInterestOnlyMode(t, sa, "B", globalAccountName)

	checkLog := func(t *testing.T, l *captureGWInterestSwitchLogger) {
		t.Helper()
		l.Lock()
		logs := append([]string(nil), l.imss...)
		l.Unlock()

		if len(logs) != 2 {
			t.Fatalf("Expected 2 logs about switching to interest-only, got %v", logs)
		}
		if !strings.Contains(logs[0], "switching account") {
			t.Fatalf("First log statement should have been about switching, got %v", logs[0])
		}
		if !strings.Contains(logs[1], "complete") {
			t.Fatalf("Second log statement should have been about having switched, got %v", logs[1])
		}
	}
	checkLog(t, logB)
	checkLog(t, logA)

	// Clear log of server B
	logB.Lock()
	logB.imss = nil
	logB.Unlock()

	// Force a switch on B to inbound gateway from A and make sure that it is
	// a no-op since this gateway connection has already been switched.
	sb.switchAccountToInterestMode(globalAccountName)

	logB.Lock()
	didSwitch := len(logB.imss) > 0
	logB.Unlock()
	if didSwitch {
		t.Fatalf("Attempted to switch while it was already in interest mode only")
	}
}

func TestGatewayAccountInterestModeSwitchOnlyOncePerAccount(t *testing.T) {
	GatewayDoNotForceInterestOnlyMode(true)
	defer GatewayDoNotForceInterestOnlyMode(false)

	ob := testDefaultOptionsForGateway("B")
	sb := runGatewayServer(ob)
	defer sb.Shutdown()

	logB := &captureGWInterestSwitchLogger{}
	sb.SetLogger(logB, true, true)

	nc := natsConnect(t, sb.ClientURL())
	defer nc.Close()
	natsSubSync(t, nc, "foo")
	natsQueueSubSync(t, nc, "bar", "baz")

	oa := testGatewayOptionsFromToWithServers(t, "A", "B", sb)
	sa := runGatewayServer(oa)
	defer sa.Shutdown()

	waitForOutboundGateways(t, sa, 1, 2*time.Second)
	waitForInboundGateways(t, sa, 1, 2*time.Second)
	waitForOutboundGateways(t, sb, 1, 2*time.Second)
	waitForInboundGateways(t, sb, 1, 2*time.Second)

	wg := sync.WaitGroup{}
	total := 20
	wg.Add(total)
	for i := 0; i < total; i++ {
		go func() {
			sb.switchAccountToInterestMode(globalAccountName)
			wg.Done()
		}()
	}
	wg.Wait()
	time.Sleep(50 * time.Millisecond)
	logB.Lock()
	nl := len(logB.imss)
	logB.Unlock()
	// There should be a trace for switching and when switch is complete
	if nl != 2 {
		t.Fatalf("Attempted to switch account too many times, number lines=%v", nl)
	}
}

func TestGatewaySingleOutbound(t *testing.T) {
	l, err := natsListen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("Error on listen: %v", err)
	}
	defer l.Close()
	port := l.Addr().(*net.TCPAddr).Port

	oa := testGatewayOptionsFromToWithTLS(t, "A", "B", []string{fmt.Sprintf("nats://127.0.0.1:%d", port)})
	oa.Gateway.TLSTimeout = 0.1
	sa := runGatewayServer(oa)
	defer sa.Shutdown()

	// Wait a bit for reconnections
	time.Sleep(500 * time.Millisecond)

	// Now prepare gateway B to take place of the bare listener.
	ob := testGatewayOptionsWithTLS(t, "B")
	// There is a risk that when stopping the listener and starting
	// the actual server, that port is being reused by some other process.
	ob.Gateway.Port = port
	l.Close()
	sb := runGatewayServer(ob)
	defer sb.Shutdown()

	// To make sure that we don't fail, bump the TLSTimeout now.
	cfg := sa.getRemoteGateway("B")
	cfg.Lock()
	cfg.TLSTimeout = 2.0
	cfg.Unlock()

	waitForOutboundGateways(t, sa, 1, time.Second)
	sa.gateway.Lock()
	lm := len(sa.gateway.out)
	sa.gateway.Unlock()
	if lm != 1 {
		t.Fatalf("Expected 1 outbound, got %v", lm)
	}
}

func TestGatewayLegacyGRPrefixReservedOnClientIngress(t *testing.T) {
	ob := testDefaultOptionsForGateway("B")
	sb := runGatewayServer(ob)
	defer sb.Shutdown()

	oa := testGatewayOptionsFromToWithServers(t, "A", "B", sb)
	oa.Users = []*User{{
		Username:    "denied",
		Password:    "pwd",
		Permissions: &Permissions{Publish: &SubjectPermission{Deny: []string{"foo"}}},
	}}
	sa := runGatewayServer(oa)
	defer sa.Shutdown()

	waitForOutboundGateways(t, sa, 1, 2*time.Second)
	waitForOutboundGateways(t, sb, 1, 2*time.Second)

	ncb := natsConnect(t, fmt.Sprintf("nats://%s:%d", ob.Host, ob.Port))
	defer ncb.Close()
	subB := natsSubSync(t, ncb, "foo")
	natsFlush(t, ncb)

	errCh := make(chan string, 8)
	nca := natsConnect(t, fmt.Sprintf("nats://denied:pwd@%s:%d", oa.Host, oa.Port),
		nats.ErrorHandler(func(_ *nats.Conn, _ *nats.Subscription, err error) { errCh <- err.Error() }))
	defer nca.Close()
	// Cluster B re-sends un-prefixed replies to gateways with interest, so
	// also make sure nothing comes back to A.
	subA := natsSubSync(t, nca, "foo")
	natsFlush(t, nca)

	expectViolation := func(what string) {
		t.Helper()
		select {
		case e := <-errCh:
			if !strings.Contains(e, "Permissions Violation") {
				t.Fatalf("Unexpected error: %v", e)
			}
		case <-time.After(time.Second):
			t.Fatalf("Expected permissions violation for %s", what)
		}
		for _, sub := range []*nats.Subscription{subB, subA} {
			if m, err := sub.NextMsg(100 * time.Millisecond); err == nil {
				t.Fatalf("Unexpected delivery on %q: %q", m.Subject, m.Data)
			}
		}
	}

	legacy := oldGWReplyPrefix + "abcd.foo"
	for _, subj := range []string{"foo", string(sb.gateway.replyPfx) + "foo", legacy} {
		natsPub(t, nca, subj, []byte("msg"))
		natsFlush(t, nca)
		expectViolation("publish on " + subj)
	}
	// The legacy prefix must also be rejected as a reply subject.
	natsPubReq(t, nca, "bar", legacy, []byte("req"))
	natsFlush(t, nca)
	expectViolation("reply on " + legacy)
}

func TestGatewayReplyMapTracking(t *testing.T) {
	// Increase the recSubExp value on servers so we have time
	// to check the replies mapping structures.
	subExp := 400 * time.Millisecond
	setRecSub := func(s *Server) {
		s.gateway.pasi.Lock()
		s.gateway.recSubExp = subExp
		s.gateway.pasi.Unlock()
	}

	ob := testDefaultOptionsForGateway("B")
	sb := runGatewayServer(ob)
	defer sb.Shutdown()
	setRecSub(sb)

	oa := testGatewayOptionsFromToWithServers(t, "A", "B", sb)
	sa := runGatewayServer(oa)
	defer sa.Shutdown()
	setRecSub(sa)

	waitForOutboundGateways(t, sa, 1, 2*time.Second)
	waitForInboundGateways(t, sa, 1, 2*time.Second)
	waitForOutboundGateways(t, sb, 1, 2*time.Second)
	waitForInboundGateways(t, sb, 1, 2*time.Second)

	ncb := natsConnect(t, sb.ClientURL())
	defer ncb.Close()
	count := 0
	total := 100
	ch := make(chan bool, 1)
	natsSub(t, ncb, "foo", func(m *nats.Msg) {
		m.Respond([]byte("reply"))
		if count++; count == total {
			ch <- true
		}
	})
	natsFlush(t, ncb)

	var bc *client
	sb.mu.Lock()
	for _, c := range sb.clients {
		bc = c
		break
	}
	sb.mu.Unlock()

	nca := natsConnect(t, sa.ClientURL())
	defer nca.Close()

	replySub := natsSubSync(t, nca, "bar.>")
	for i := 0; i < total; i++ {
		nca.PublishRequest("foo", fmt.Sprintf("bar.%d", i), []byte("request"))
	}

	waitCh(t, ch, "Did not receive all requests")

	check := func(t *testing.T, expectedIndicator int32, expectLenMap int, expectedSrvMapEmpty bool) {
		t.Helper()
		bc.mu.Lock()
		mapIndicator := atomic.LoadInt32(&bc.gwReplyMapping.check)
		var lenMap int
		if bc.gwReplyMapping.mapping != nil {
			lenMap = len(bc.gwReplyMapping.mapping)
		}
		bc.mu.Unlock()
		if mapIndicator != expectedIndicator {
			t.Fatalf("Client should map indicator should be %v, got %v", expectedIndicator, mapIndicator)
		}
		if lenMap != expectLenMap {
			t.Fatalf("Client map should have %v entries, got %v", expectLenMap, lenMap)
		}
		srvMapEmpty := true
		sb.gwrm.m.Range(func(_, _ any) bool {
			srvMapEmpty = false
			return false
		})
		if srvMapEmpty != expectedSrvMapEmpty {
			t.Fatalf("Expected server map to be empty=%v, got %v", expectedSrvMapEmpty, srvMapEmpty)
		}
	}
	// Check that indicator is set and that there "total" entries in the map
	// and that srv map is not empty
	check(t, 1, total, false)

	// Receive all replies
	for i := 0; i < total; i++ {
		natsNexMsg(t, replySub, time.Second)
	}

	// Wait until entries expire
	time.Sleep(2*subExp + 100*time.Millisecond)

	// Now check again.
	check(t, 0, 0, true)
}

func TestGatewayNoAccountUnsubWhenServiceReplyInUse(t *testing.T) {
	oa := testDefaultOptionsForGateway("A")
	setAccountUserPassInOptions(oa, "$foo", "clientFoo", "password")
	setAccountUserPassInOptions(oa, "$bar", "clientBar", "password")
	sa := runGatewayServer(oa)
	defer sa.Shutdown()

	ob := testGatewayOptionsFromToWithServers(t, "B", "A", sa)
	setAccountUserPassInOptions(ob, "$foo", "clientFoo", "password")
	setAccountUserPassInOptions(ob, "$bar", "clientBar", "password")
	sb := runGatewayServer(ob)
	defer sb.Shutdown()

	waitForOutboundGateways(t, sa, 1, time.Second)
	waitForOutboundGateways(t, sb, 1, time.Second)
	waitForInboundGateways(t, sa, 1, time.Second)
	waitForInboundGateways(t, sb, 1, time.Second)

	// Get accounts
	fooA, _ := sa.LookupAccount("$foo")
	barA, _ := sa.LookupAccount("$bar")
	fooB, _ := sb.LookupAccount("$foo")
	barB, _ := sb.LookupAccount("$bar")

	// Add in the service export for the requests. Make it public.
	fooA.AddServiceExport("test.request", nil)
	fooB.AddServiceExport("test.request", nil)

	// Add import abilities to server B's bar account from foo.
	if err := barB.AddServiceImport(fooB, "foo.request", "test.request"); err != nil {
		t.Fatalf("Error adding service import: %v", err)
	}
	// Same on A.
	if err := barA.AddServiceImport(fooA, "foo.request", "test.request"); err != nil {
		t.Fatalf("Error adding service import: %v", err)
	}

	// clientA will be connected to srvA and be the service endpoint and responder.
	aURL := fmt.Sprintf("nats://clientFoo:password@127.0.0.1:%d", oa.Port)
	clientA := natsConnect(t, aURL)
	defer clientA.Close()

	natsSub(t, clientA, "test.request", func(m *nats.Msg) {
		m.Respond([]byte("reply"))
	})
	natsFlush(t, clientA)

	// Now setup client B on srvB who will send the requests.
	bURL := fmt.Sprintf("nats://clientBar:password@127.0.0.1:%d", ob.Port)
	clientB := natsConnect(t, bURL)
	defer clientB.Close()

	if _, err := clientB.Request("foo.request", []byte("request"), time.Second); err != nil {
		t.Fatalf("Did not get the reply: %v", err)
	}

	quitCh := make(chan bool, 1)
	wg := sync.WaitGroup{}
	wg.Add(1)
	go func() {
		defer wg.Done()

		for {
			select {
			case <-quitCh:
				return
			default:
				clientA.Publish("any.subject", []byte("any message"))
				time.Sleep(time.Millisecond)
			}
		}
	}()
	for i := 0; i < 1000; i++ {
		if _, err := clientB.Request("foo.request", []byte("request"), time.Second); err != nil {
			t.Fatalf("Did not get the reply: %v", err)
		}
	}
	close(quitCh)
	wg.Wait()
}

func TestGatewayCloseTLSConnection(t *testing.T) {
	oa := testGatewayOptionsWithTLS(t, "A")
	oa.DisableShortFirstPing = true
	oa.Gateway.TLSConfig.ClientAuth = tls.NoClientCert
	oa.Gateway.TLSTimeout = 100
	sa := runGatewayServer(oa)
	defer sa.Shutdown()

	ob1 := testGatewayOptionsFromToWithTLS(t, "B", "A", []string{fmt.Sprintf("nats://127.0.0.1:%d", sa.GatewayAddr().Port)})
	sb1 := runGatewayServer(ob1)
	defer sb1.Shutdown()

	waitForOutboundGateways(t, sa, 1, 2*time.Second)
	waitForInboundGateways(t, sa, 1, 2*time.Second)
	waitForOutboundGateways(t, sb1, 1, 2*time.Second)
	waitForInboundGateways(t, sb1, 1, 2*time.Second)

	endpoint := net.JoinHostPort(oa.Gateway.Host, fmt.Sprintf("%d", oa.Gateway.Port))
	conn, err := net.DialTimeout("tcp", endpoint, 2*time.Second)
	if err != nil {
		t.Fatalf("Unexpected error on dial: %v", err)
	}
	defer conn.Close()

	tlsConn := tls.Client(conn, &tls.Config{InsecureSkipVerify: true})
	defer tlsConn.Close()
	if err := tlsConn.Handshake(); err != nil {
		t.Fatalf("Unexpected error during handshake: %v", err)
	}
	connectOp := []byte("CONNECT {\"name\":\"serverID\",\"verbose\":false,\"pedantic\":false,\"tls_required\":true,\"gateway\":\"B\"}\r\n")
	if _, err := tlsConn.Write(connectOp); err != nil {
		t.Fatalf("Unexpected error writing CONNECT: %v", err)
	}
	infoOp := []byte("INFO {\"server_id\":\"serverID\",\"tls_required\":true,\"gateway\":\"B\",\"gateway_nrp\":true}\r\n")
	if _, err := tlsConn.Write(infoOp); err != nil {
		t.Fatalf("Unexpected error writing CONNECT: %v", err)
	}
	if _, err := tlsConn.Write([]byte("PING\r\n")); err != nil {
		t.Fatalf("Unexpected error writing PING: %v", err)
	}

	// Get gw connection
	var gw *client
	checkFor(t, time.Second, 15*time.Millisecond, func() error {
		sa.gateway.RLock()
		for _, g := range sa.gateway.in {
			g.mu.Lock()
			if g.opts.Name == "serverID" {
				gw = g
			}
			g.mu.Unlock()
			break
		}
		sa.gateway.RUnlock()
		if gw == nil {
			return fmt.Errorf("No gw registered yet")
		}
		return nil
	})
	// Fill the buffer. We want to timeout on write so that nc.Close()
	// would block due to a write that cannot complete.
	buf := make([]byte, 64*1024)
	done := false
	for !done {
		gw.nc.SetWriteDeadline(time.Now().Add(time.Second))
		if _, err := gw.nc.Write(buf); err != nil {
			done = true
		}
		gw.nc.SetWriteDeadline(time.Time{})
	}
	ch := make(chan bool)
	go func() {
		select {
		case <-ch:
			return
		case <-time.After(3 * time.Second):
			fmt.Println("!!!! closeConnection is blocked, test will hang !!!")
			return
		}
	}()
	// Close the gateway
	gw.closeConnection(SlowConsumerWriteDeadline)
	ch <- true
}

func TestGatewayNoCrashOnInvalidSubject(t *testing.T) {
	ob := testDefaultOptionsForGateway("B")
	sb := runGatewayServer(ob)
	defer sb.Shutdown()

	oa := testGatewayOptionsFromToWithServers(t, "A", "B", sb)
	sa := runGatewayServer(oa)
	defer sa.Shutdown()

	waitForOutboundGateways(t, sa, 1, 2*time.Second)
	waitForInboundGateways(t, sa, 1, 2*time.Second)
	waitForOutboundGateways(t, sb, 1, 2*time.Second)
	waitForInboundGateways(t, sb, 1, 2*time.Second)

	ncB := natsConnect(t, sb.ClientURL())
	defer ncB.Close()

	natsSubSync(t, ncB, "foo")
	natsFlush(t, ncB)

	ncA := natsConnect(t, sa.ClientURL())
	defer ncA.Close()

	// Send on an invalid subject. Since there is interest on B,
	// we will receive an RS- instead of A-
	natsPub(t, ncA, "bar..baz", []byte("bad subject"))
	natsFlush(t, ncA)

	// Now create on B a sub on a wildcard subject
	sub := natsSubSync(t, ncB, "bar.*")
	natsFlush(t, ncB)

	// Server should not have crashed...
	natsPub(t, ncA, "bar.baz", []byte("valid subject"))
	if _, err := sub.NextMsg(time.Second); err != nil {
		t.Fatalf("Error getting message: %v", err)
	}
}

func TestGatewayUpdateURLsFromRemoteCluster(t *testing.T) {
	ob1 := testDefaultOptionsForGateway("B")
	sb1 := RunServer(ob1)
	defer sb1.Shutdown()

	oa := testGatewayOptionsFromToWithServers(t, "A", "B", sb1)
	sa := RunServer(oa)
	defer sa.Shutdown()

	waitForOutboundGateways(t, sa, 1, 2*time.Second)
	waitForOutboundGateways(t, sb1, 1, 2*time.Second)

	// Add a server to cluster B.
	ob2 := testDefaultOptionsForGateway("B")
	ob2.Routes = RoutesFromStr(fmt.Sprintf("nats://127.0.0.1:%d", ob1.Cluster.Port))
	sb2 := RunServer(ob2)
	defer sb2.Shutdown()

	checkClusterFormed(t, sb1, sb2)
	waitForOutboundGateways(t, sb2, 1, 2*time.Second)
	waitForInboundGateways(t, sa, 2, 2*time.Second)

	pmap := make(map[int]string)
	pmap[ob1.Gateway.Port] = "B1"
	pmap[ob2.Gateway.Port] = "B2"

	checkURLs := func(eurls map[string]string) {
		t.Helper()
		checkFor(t, 2*time.Second, 15*time.Millisecond, func() error {
			rg := sa.getRemoteGateway("B")
			urls := rg.getURLsAsStrings()
			for _, u := range urls {
				if _, ok := eurls[u]; !ok {
					_, sport, _ := net.SplitHostPort(u)
					port, _ := strconv.Atoi(sport)
					return fmt.Errorf("URL %q (%s) should not be in the list of urls (%q)", u, pmap[port], eurls)
				}
			}
			return nil
		})
	}
	expected := make(map[string]string)
	expected[fmt.Sprintf("127.0.0.1:%d", ob1.Gateway.Port)] = "B1"
	expected[fmt.Sprintf("127.0.0.1:%d", ob2.Gateway.Port)] = "B2"
	checkURLs(expected)

	// Add another in cluster B
	ob3 := testDefaultOptionsForGateway("B")
	ob3.Routes = RoutesFromStr(fmt.Sprintf("nats://127.0.0.1:%d", ob1.Cluster.Port))
	sb3 := RunServer(ob3)
	defer sb3.Shutdown()

	checkClusterFormed(t, sb1, sb2, sb3)
	waitForOutboundGateways(t, sb3, 1, 2*time.Second)
	waitForInboundGateways(t, sa, 3, 2*time.Second)

	pmap[ob3.Gateway.Port] = "B3"

	expected = make(map[string]string)
	expected[fmt.Sprintf("127.0.0.1:%d", ob1.Gateway.Port)] = "B1"
	expected[fmt.Sprintf("127.0.0.1:%d", ob2.Gateway.Port)] = "B2"
	expected[fmt.Sprintf("127.0.0.1:%d", ob3.Gateway.Port)] = "B3"
	checkURLs(expected)

	// Now stop server SB2, which should cause SA to remove it from its list.
	sb2.Shutdown()

	expected = make(map[string]string)
	expected[fmt.Sprintf("127.0.0.1:%d", ob1.Gateway.Port)] = "B1"
	expected[fmt.Sprintf("127.0.0.1:%d", ob3.Gateway.Port)] = "B3"
	checkURLs(expected)
}

type capturePingConn struct {
	net.Conn
	ch chan struct{}
}

func (c *capturePingConn) Write(b []byte) (int, error) {
	if bytes.Contains(b, []byte(pingProto)) {
		select {
		case c.ch <- struct{}{}:
		default:
		}
	}
	return c.Conn.Write(b)
}

func TestGatewayPings(t *testing.T) {
	gatewayMaxPingInterval = 50 * time.Millisecond
	defer func() { gatewayMaxPingInterval = gwMaxPingInterval }()

	ob := testDefaultOptionsForGateway("B")
	sb := RunServer(ob)
	defer sb.Shutdown()

	oa := testGatewayOptionsFromToWithServers(t, "A", "B", sb)
	sa := RunServer(oa)
	defer sa.Shutdown()

	waitForInboundGateways(t, sa, 1, 2*time.Second)
	waitForOutboundGateways(t, sa, 1, 2*time.Second)
	waitForInboundGateways(t, sb, 1, 2*time.Second)
	waitForOutboundGateways(t, sb, 1, 2*time.Second)

	c := sa.getOutboundGatewayConnection("B")
	ch := make(chan struct{}, 1)
	c.mu.Lock()
	c.nc = &capturePingConn{c.nc, ch}
	c.mu.Unlock()

	for i := 0; i < 5; i++ {
		select {
		case <-ch:
		case <-time.After(250 * time.Millisecond):
			t.Fatalf("Did not send PING")
		}
	}
}

func TestGatewayTLSConfigReload(t *testing.T) {
	template := `
		listen: 127.0.0.1:-1
		gateway {
			name: "A"
			listen: "127.0.0.1:-1"
			tls {
				cert_file: "../test/configs/certs/server-cert.pem"
				key_file:  "../test/configs/certs/server-key.pem"
				%s
				timeout: 2
			}
		}
	`
	confA := createConfFile(t, []byte(fmt.Sprintf(template, "")))

	srvA, optsA := RunServerWithConfig(confA)
	defer srvA.Shutdown()

	optsB := testGatewayOptionsFromToWithTLS(t, "B", "A", []string{fmt.Sprintf("nats://127.0.0.1:%d", optsA.Gateway.Port)})
	srvB := runGatewayServer(optsB)
	defer srvB.Shutdown()

	waitForGatewayFailedConnect(t, srvB, "A", true, time.Second)

	reloadUpdateConfig(t, srvA, confA, fmt.Sprintf(template, `ca_file:   "../test/configs/certs/ca.pem"`))

	waitForInboundGateways(t, srvA, 1, time.Second)
	waitForOutboundGateways(t, srvA, 1, time.Second)
	waitForInboundGateways(t, srvB, 1, time.Second)
	waitForOutboundGateways(t, srvB, 1, time.Second)
}

func TestGatewayTLSConfigReloadForRemote(t *testing.T) {
	SetGatewaysSolicitDelay(5 * time.Millisecond)
	defer ResetGatewaysSolicitDelay()

	optsA := testGatewayOptionsWithTLS(t, "A")
	srvA := runGatewayServer(optsA)
	defer srvA.Shutdown()

	template := `
		listen: 127.0.0.1:-1
		gateway {
			name: "B"
			listen: "127.0.0.1:-1"
			tls {
				cert_file: "../test/configs/certs/server-cert.pem"
				key_file:  "../test/configs/certs/server-key.pem"
				ca_file:   "../test/configs/certs/ca.pem"
				timeout: 2
			}
			gateways [
				{
					name: "A"
					url: "nats://127.0.0.1:%d"
					tls {
						cert_file: "../test/configs/certs/server-cert.pem"
						key_file:  "../test/configs/certs/server-key.pem"
						%s
						timeout: 2
					}
				}
			]
		}
	`
	confB := createConfFile(t, []byte(fmt.Sprintf(template, optsA.Gateway.Port, "")))

	srvB, _ := RunServerWithConfig(confB)
	defer srvB.Shutdown()

	waitForGatewayFailedConnect(t, srvB, "A", true, time.Second)

	reloadUpdateConfig(t, srvB, confB, fmt.Sprintf(template, optsA.Gateway.Port, `ca_file: "../test/configs/certs/ca.pem"`))

	waitForInboundGateways(t, srvA, 1, time.Second)
	waitForOutboundGateways(t, srvA, 1, time.Second)
	waitForInboundGateways(t, srvB, 1, time.Second)
	waitForOutboundGateways(t, srvB, 1, time.Second)
}

func TestGatewayTLSConfigReloadForImplicitRemote(t *testing.T) {
	SetGatewaysSolicitDelay(5 * time.Millisecond)
	defer ResetGatewaysSolicitDelay()

	template := `
		listen: 127.0.0.1:-1
		gateway {
			name: "A"
			listen: "127.0.0.1:-1"
			tls {
				cert_file: "../test/configs/certs/srva-cert.pem"
				key_file:  "../test/configs/certs/srva-key.pem"
				%s
				verify: true
			}
		}
	`
	confA := createConfFile(t, fmt.Appendf(nil, template, `ca_file:   "../test/configs/certs/ca.pem"`))
	srvA, optsA := RunServerWithConfig(confA)
	defer srvA.Shutdown()

	optsB := testGatewayOptionsFromToWithTLS(t, "B", "A", []string{fmt.Sprintf("nats://127.0.0.1:%d", optsA.Gateway.Port)})
	srvB := runGatewayServer(optsB)
	defer srvB.Shutdown()

	waitForInboundGateways(t, srvA, 1, time.Second)
	waitForOutboundGateways(t, srvA, 1, time.Second)
	waitForInboundGateways(t, srvB, 1, time.Second)
	waitForOutboundGateways(t, srvB, 1, time.Second)

	// We will verify that the config reload of the tls{} block is applied to
	// the implicit remote (from A to B) by removing the ca_file.
	reloadUpdateConfig(t, srvA, confA, fmt.Sprintf(template, ""))

	// Get the remote from A to B
	cfg := srvA.getRemoteGateway("B")
	require_NotNil(t, cfg)
	cfg.Lock()
	tc := cfg.TLSConfig
	cfg.Unlock()
	require_NotNil(t, tc)
	// The CA should have been removed.
	require_True(t, tc.ClientCAs == nil)

	// Reset the connection attempts, since we are going to close the connection
	// from A to B and make sure that connection keeps failing.
	cfg.resetConnAttempts()

	// Get the outbound connection and close it.
	c := srvA.getOutboundGatewayConnection("B")
	require_NotNil(t, c)
	c.mu.Lock()
	c.nc.Close()
	c.mu.Unlock()

	// Verify that we fail to connect from A to B now.
	waitForGatewayFailedConnect(t, srvA, "B", true, time.Second)
}

func TestGatewayTLSPinnedCertsReloadDisconnectsPeers(t *testing.T) {
	SetGatewaysSolicitDelay(5 * time.Millisecond)
	defer ResetGatewaysSolicitDelay()

	const (
		pinnedSrvA = "[\"c5d58200425185accb67cdca400d823ef20300a1ee09e5b38c668be8bd20256e\"]"
		pinnedSrvB = "[\"0e35d0ab7dee2683105f841c4a51953ec493ceb62503d26f38902d301111562c\"]"
	)

	template := `
		listen: 127.0.0.1:-1
		gateway {
			name: "A"
			listen: "127.0.0.1:-1"
			tls {
				cert_file: "../test/configs/certs/srva-cert.pem"
				key_file:  "../test/configs/certs/srva-key.pem"
				ca_file:   "../test/configs/certs/ca.pem"
				verify: true
				pinned_certs: %s
				timeout: 2
			}
		}
	`
	confA := createConfFile(t, []byte(fmt.Sprintf(template, pinnedSrvB)))
	srvA, optsA := RunServerWithConfig(confA)
	defer srvA.Shutdown()

	optsB := testGatewayOptionsFromToWithTLS(t, "B", "A", []string{fmt.Sprintf("nats://127.0.0.1:%d", optsA.Gateway.Port)})
	srvB := runGatewayServer(optsB)
	defer srvB.Shutdown()

	waitForInboundGateways(t, srvA, 1, time.Second)
	waitForOutboundGateways(t, srvA, 1, time.Second)
	waitForInboundGateways(t, srvB, 1, time.Second)
	waitForOutboundGateways(t, srvB, 1, time.Second)

	// Change the pinned cert. Expect the gateway to disconnect
	reloadUpdateConfig(t, srvA, confA, fmt.Sprintf(template, pinnedSrvA))
	waitForInboundGateways(t, srvA, 0, time.Second)
	waitForOutboundGateways(t, srvA, 0, time.Second)
	waitForInboundGateways(t, srvB, 0, time.Second)
	waitForOutboundGateways(t, srvB, 0, time.Second)
	waitForGatewayFailedConnect(t, srvB, "A", true, time.Second)

	// Reload with empty pinned cert. Expect gateway to reconnect.
	reloadUpdateConfig(t, srvA, confA, fmt.Sprintf(template, "[]"))
	waitForInboundGateways(t, srvA, 1, time.Second)
	waitForOutboundGateways(t, srvA, 1, time.Second)
	waitForInboundGateways(t, srvB, 1, time.Second)
	waitForOutboundGateways(t, srvB, 1, time.Second)

	// Reset pinned cert to server B. Expect gate to remain connected.
	reloadUpdateConfig(t, srvA, confA, fmt.Sprintf(template, pinnedSrvB))
	waitForInboundGateways(t, srvA, 1, time.Second)
	waitForOutboundGateways(t, srvA, 1, time.Second)
	waitForInboundGateways(t, srvB, 1, time.Second)
	waitForOutboundGateways(t, srvB, 1, time.Second)

}

func TestGatewayAuthDiscovered(t *testing.T) {
	SetGatewaysSolicitDelay(5 * time.Millisecond)
	defer ResetGatewaysSolicitDelay()

	confA := createConfFile(t, []byte(`
		listen: 127.0.0.1:-1
		gateway {
			name: "A"
			listen: 127.0.0.1:-1
			authorization: { user: gwuser, password: changeme }
		}
	`))
	srvA, optsA := RunServerWithConfig(confA)
	defer srvA.Shutdown()

	confB := createConfFile(t, []byte(fmt.Sprintf(`
		listen: 127.0.0.1:-1
		gateway {
			name: "B"
			listen: 127.0.0.1:-1
			authorization: { user: gwuser, password: changeme }
			gateways: [
				{ name: A, url: nats://gwuser:changeme@127.0.0.1:%d }
			]
		}
	`, optsA.Gateway.Port)))
	srvB, _ := RunServerWithConfig(confB)
	defer srvB.Shutdown()

	waitForInboundGateways(t, srvA, 1, time.Second)
	waitForOutboundGateways(t, srvA, 1, time.Second)
	waitForInboundGateways(t, srvB, 1, time.Second)
	waitForOutboundGateways(t, srvB, 1, time.Second)
}

func TestGatewayTLSCertificateImplicitAllowPass(t *testing.T) {
	testGatewayTLSCertificateImplicitAllow(t, true)
}

func TestGatewayTLSCertificateImplicitAllowFail(t *testing.T) {
	testGatewayTLSCertificateImplicitAllow(t, false)
}

func testGatewayTLSCertificateImplicitAllow(t *testing.T, pass bool) {
	// Base config for the servers
	cfg := createTempFile(t, "cfg")
	cfg.WriteString(fmt.Sprintf(`
		gateway {
		  tls {
			cert_file = "../test/configs/certs/tlsauth/server.pem"
			key_file = "../test/configs/certs/tlsauth/server-key.pem"
			ca_file = "../test/configs/certs/tlsauth/ca.pem"
			verify_cert_and_check_known_urls = true
			insecure = %t
			timeout = 1
		  }
		}
	`, !pass)) // set insecure to skip verification on the outgoing end
	if err := cfg.Sync(); err != nil {
		t.Fatal(err)
	}
	cfg.Close()

	optsA := LoadConfig(cfg.Name())
	optsB := LoadConfig(cfg.Name())

	urlA := "nats://localhost:9995"
	urlB := "nats://localhost:9996"
	if !pass {
		urlA = "nats://127.0.0.1:9995"
		urlB = "nats://127.0.0.1:9996"
	}

	gwA, err := url.Parse(urlA)
	if err != nil {
		t.Fatal(err)
	}
	gwB, err := url.Parse(urlB)
	if err != nil {
		t.Fatal(err)
	}

	optsA.Host = "127.0.0.1"
	optsA.Port = -1
	optsA.Gateway.Name = "A"
	optsA.Gateway.Port = 9995
	optsA.Gateway.resolver = &localhostResolver{}

	optsB.Host = "127.0.0.1"
	optsB.Port = -1
	optsB.Gateway.Name = "B"
	optsB.Gateway.Port = 9996
	optsB.Gateway.resolver = &localhostResolver{}

	gateways := make([]*RemoteGatewayOpts, 2)
	gateways[0] = &RemoteGatewayOpts{
		Name: optsA.Gateway.Name,
		URLs: []*url.URL{gwA},
	}
	gateways[1] = &RemoteGatewayOpts{
		Name: optsB.Gateway.Name,
		URLs: []*url.URL{gwB},
	}

	optsA.Gateway.Gateways = gateways
	optsB.Gateway.Gateways = gateways

	SetGatewaysSolicitDelay(100 * time.Millisecond)
	defer ResetGatewaysSolicitDelay()

	srvA := RunServer(optsA)
	defer srvA.Shutdown()

	srvB := RunServer(optsB)
	defer srvB.Shutdown()

	if pass {
		waitForOutboundGateways(t, srvA, 1, 5*time.Second)
		waitForOutboundGateways(t, srvB, 1, 5*time.Second)
	} else {
		time.Sleep(1 * time.Second) // the fail case uses the IP, so a short wait is sufficient
		checkFor(t, 2*time.Second, 15*time.Millisecond, func() error {
			if srvA.NumOutboundGateways() != 0 || srvB.NumOutboundGateways() != 0 {
				return fmt.Errorf("No outbound gateway connection expected")
			}
			return nil
		})
	}
}

func TestGatewayURLsNotRemovedOnDuplicateRoute(t *testing.T) {
	// For this test, we need to have servers in cluster B creating routes
	// to each other to help produce the "duplicate route" situation, so
	// we are forced to use deterministic ports.
	getEphemeralPort := func() int {
		t.Helper()
		l, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatalf("Error getting a port: %v", err)
		}
		p := l.Addr().(*net.TCPAddr).Port
		l.Close()
		return p
	}
	p1 := getEphemeralPort()
	p2 := getEphemeralPort()
	routeURLs := fmt.Sprintf("nats://127.0.0.1:%d,nats://127.0.0.1:%d", p1, p2)

	ob1 := testDefaultOptionsForGateway("B")
	ob1.Cluster.Port = p1
	ob1.Routes = RoutesFromStr(routeURLs)
	sb1 := RunServer(ob1)
	defer sb1.Shutdown()

	ob2 := testDefaultOptionsForGateway("B")
	ob2.Cluster.Port = p2
	ob2.Routes = RoutesFromStr(routeURLs)
	sb2 := RunServer(ob2)
	defer sb2.Shutdown()

	checkClusterFormed(t, sb1, sb2)

	oa := testGatewayOptionsFromToWithServers(t, "A", "B", sb1)
	sa := RunServer(oa)
	defer sa.Shutdown()

	waitForOutboundGateways(t, sb1, 1, 2*time.Second)
	waitForOutboundGateways(t, sb2, 1, 2*time.Second)
	waitForOutboundGateways(t, sa, 1, 2*time.Second)
	waitForInboundGateways(t, sa, 2, 2*time.Second)

	checkURLs := func(s *Server) {
		t.Helper()
		s.mu.Lock()
		urls := s.gateway.URLs.getAsStringSlice()
		s.mu.Unlock()
		if len(urls) != 2 {
			t.Fatalf("Expected 2 urls, got %v", urls)
		}
	}
	checkURLs(sb1)
	checkURLs(sb2)

	// As for sa, we should have both sb1 and sb2 urls in its outbound urls map
	c := sa.getOutboundGatewayConnection("B")
	if c == nil {
		t.Fatal("No outound connection found!")
	}
	c.mu.Lock()
	urls := c.gw.cfg.urls
	c.mu.Unlock()
	if len(urls) != 2 {
		t.Fatalf("Expected 2 urls to B, got %v", urls)
	}
}

func TestGatewayDuplicateServerName(t *testing.T) {
	// We will have 2 servers per cluster names "nats1" and "nats2", and have
	// the servers in the second cluster with the same name, but we will make
	// sure to connect "A/nats1" to "B/nats2" and "A/nats2" to "B/nats1" and
	// verify that we still discover the duplicate names.
	ob1 := testDefaultOptionsForGateway("B")
	ob1.ServerName = "nats1"
	sb1 := RunServer(ob1)
	defer sb1.Shutdown()

	ob2 := testDefaultOptionsForGateway("B")
	ob2.ServerName = "nats2"
	ob2.Routes = RoutesFromStr(fmt.Sprintf("nats://127.0.0.1:%d", ob1.Cluster.Port))
	sb2 := RunServer(ob2)
	defer sb2.Shutdown()

	checkClusterFormed(t, sb1, sb2)

	oa1 := testGatewayOptionsFromToWithServers(t, "A", "B", sb2)
	oa1.ServerName = "nats1"
	// Needed later in the test
	oa1.Gateway.RejectUnknown = true
	sa1 := RunServer(oa1)
	defer sa1.Shutdown()
	sa1l := &captureErrorLogger{errCh: make(chan string, 100)}
	sa1.SetLogger(sa1l, false, false)

	oa2 := testGatewayOptionsFromToWithServers(t, "A", "B", sb1)
	oa2.ServerName = "nats2"
	// Needed later in the test
	oa2.Gateway.RejectUnknown = true
	oa2.Routes = RoutesFromStr(fmt.Sprintf("nats://127.0.0.1:%d", oa1.Cluster.Port))
	sa2 := RunServer(oa2)
	defer sa2.Shutdown()
	sa2l := &captureErrorLogger{errCh: make(chan string, 100)}
	sa2.SetLogger(sa2l, false, false)

	checkClusterFormed(t, sa1, sa2)

	checkForDupError := func(errCh chan string) {
		t.Helper()
		timeout := time.NewTimer(time.Second)
		for done := false; !done; {
			select {
			case err := <-errCh:
				if strings.Contains(err, "server has a duplicate name") {
					done = true
				}
			case <-timeout.C:
				t.Fatal("Did not get error about servers in super-cluster with same name")
			}
		}
	}

	// Since only servers from "A" have configured outbound to
	// cluster "B", only servers on "A" are expected to report error.
	for _, errCh := range []chan string{sa1l.errCh, sa2l.errCh} {
		checkForDupError(errCh)
	}

	// So now we are going to fix names and wait for the super cluster to form.
	sa2.Shutdown()
	sa1.Shutdown()

	// Drain the error channels
	for _, errCh := range []chan string{sa1l.errCh, sa2l.errCh} {
		for done := false; !done; {
			select {
			case <-errCh:
			default:
				done = true
			}
		}
	}

	oa1.ServerName = "a_nats1"
	oa2.ServerName = "a_nats2"
	sa1 = RunServer(oa1)
	defer sa1.Shutdown()
	sa2 = RunServer(oa2)
	defer sa2.Shutdown()

	checkClusterFormed(t, sa1, sa2)

	waitForOutboundGateways(t, sa1, 1, 2*time.Second)
	waitForOutboundGateways(t, sa2, 1, 2*time.Second)
	waitForOutboundGateways(t, sb1, 1, 2*time.Second)
	waitForOutboundGateways(t, sb2, 1, 2*time.Second)

	// Now add a server on cluster B (that does not have outbound
	// gateway connections explicitly defined) and use the name
	// of one of the cluster A's server. We should get an error.
	ob3 := testDefaultOptionsForGateway("B")
	ob3.ServerName = "a_nats2"
	ob3.Accounts = []*Account{NewAccount("sys")}
	ob3.Routes = RoutesFromStr(fmt.Sprintf("nats://127.0.0.1:%d", ob2.Cluster.Port))
	sb3 := RunServer(ob3)
	defer sb3.Shutdown()
	sb3l := &captureErrorLogger{errCh: make(chan string, 100)}
	sb3.SetLogger(sb3l, false, false)

	checkClusterFormed(t, sb1, sb2, sb3)

	// It should report the error when trying to create the GW connection
	checkForDupError(sb3l.errCh)

	// Stop this node
	sb3.Shutdown()
	checkClusterFormed(t, sb1, sb2)

	// Now create a GW "C" with a server that uses the same name than one of
	// the server on "A", say "a_nats2".
	// This server will connect to "B", and "B" will gossip "A" back to "C"
	// and "C" will then try to connect to "A", but "A" rejects unknown, so
	// connection will be refused. However, we want to make sure that the
	// duplicate server name is still detected.
	oc := testGatewayOptionsFromToWithServers(t, "C", "B", sb1)
	oc.ServerName = "a_nats2"
	oc.Accounts = []*Account{NewAccount("sys")}
	sc := RunServer(oc)
	defer sc.Shutdown()
	scl := &captureErrorLogger{errCh: make(chan string, 100)}
	sc.SetLogger(scl, false, false)

	// It should report the error when trying to create the GW connection
	// to cluster "A"
	checkForDupError(scl.errCh)
}

func TestGatewayNoPanicOnStartupWithMonitoring(t *testing.T) {
	o := testDefaultOptionsForGateway("B")
	o.HTTPHost = "127.0.0.1"
	o.HTTPPort = 8888
	s, err := NewServer(o)
	require_NoError(t, err)

	wg := sync.WaitGroup{}
	wg.Add(1)
	go func() {
		defer wg.Done()

		time.Sleep(50 * time.Millisecond)
		s.Start()
		s.WaitForShutdown()
	}()

	for {
		g, err := s.Gatewayz(nil)
		if err != nil {
			continue
		}
		if g.Port != 0 && g.Port != s.GatewayAddr().Port {
			t.Fatalf("Unexpected port: %v vs %v", g.Port, s.GatewayAddr().Port)
		}
		break
	}
	s.Shutdown()
	wg.Wait()
}

func TestGatewaySwitchToInterestOnlyModeImmediately(t *testing.T) {
	o2 := testDefaultOptionsForGateway("B")
	// Add users to cause s2 to require auth. Will add an account with user later.
	o2.Users = append([]*User(nil), &User{Username: "test", Password: "pwd"})
	s2 := runGatewayServer(o2)
	defer s2.Shutdown()

	o1 := testGatewayOptionsFromToWithServers(t, "A", "B", s2)
	setAccountUserPassInOptions(o1, "$foo", "ivan", "password")
	s1 := runGatewayServer(o1)
	defer s1.Shutdown()

	waitForOutboundGateways(t, s1, 1, time.Second)
	waitForOutboundGateways(t, s2, 1, time.Second)

	s1Url := fmt.Sprintf("nats://ivan:password@127.0.0.1:%d", o1.Port)
	nc := natsConnect(t, s1Url)
	defer nc.Close()
	natsPub(t, nc, "foo", []byte("hello"))
	natsFlush(t, nc)

	checkCount := func(t *testing.T, c *client, expected int) {
		t.Helper()
		c.mu.Lock()
		out := c.outMsgs
		c.mu.Unlock()
		if int(out) != expected {
			t.Fatalf("Expected %d message(s) to be sent over, got %v", expected, out)
		}
	}
	// No message should be sent
	gwcb := s1.getOutboundGatewayConnection("B")
	checkCount(t, gwcb, 0)

	// Check that we are in interest-only mode, but in this case, since s2 does
	// have the account, we should have the account not even present in the map.
	checkGWInterestOnlyModeOrNotPresent(t, s1, "B", "$foo", true)

	// Add account to S2 and a client.
	s2FooAcc, err := s2.RegisterAccount("$foo")
	if err != nil {
		t.Fatalf("Error registering account: %v", err)
	}
	s2.mu.Lock()
	s2.users["ivan"] = &User{Account: s2FooAcc, Username: "ivan", Password: "password"}
	s2.mu.Unlock()
	s2Url := fmt.Sprintf("nats://ivan:password@127.0.0.1:%d", o2.Port)
	ncS2 := natsConnect(t, s2Url)
	defer ncS2.Close()
	natsSubSync(t, ncS2, "asub")
	// This time we will have the account in the map and it will be interest-only
	checkGWInterestOnlyMode(t, s1, "B", "$foo")

	// Now publish a message, still should not go because the sub is on "asub"
	natsPub(t, nc, "foo", []byte("hello"))
	natsFlush(t, nc)
	checkCount(t, gwcb, 0)

	natsSubSync(t, ncS2, "foo")
	natsFlush(t, ncS2)

	checkGWInterestOnlyModeInterestOn(t, s1, "B", "$foo", "foo")

	// Publish on foo
	natsPub(t, nc, "foo", []byte("hello"))
	natsFlush(t, nc)
	checkCount(t, gwcb, 1)
}

func TestGatewaySlowConsumer(t *testing.T) {
	gatewayMaxPingInterval = 50 * time.Millisecond
	defer func() { gatewayMaxPingInterval = gwMaxPingInterval }()

	ob := testDefaultOptionsForGateway("B")
	sb := RunServer(ob)
	defer sb.Shutdown()

	oa := testGatewayOptionsFromToWithServers(t, "A", "B", sb)
	sa := RunServer(oa)
	defer sa.Shutdown()

	waitForInboundGateways(t, sa, 1, 2*time.Second)
	waitForOutboundGateways(t, sa, 1, 2*time.Second)
	waitForInboundGateways(t, sb, 1, 2*time.Second)
	waitForOutboundGateways(t, sb, 1, 2*time.Second)

	c := sa.getOutboundGatewayConnection("B")
	c.mu.Lock()
	c.out.wdl = time.Nanosecond
	c.mu.Unlock()

	<-time.After(250 * time.Millisecond)
	got := sa.NumSlowConsumersGateways()
	expected := uint64(1)
	if got != 1 {
		t.Errorf("got: %d, expected: %d", got, expected)
	}
	got = sb.NumSlowConsumersGateways()
	expected = 0
	if got != expected {
		t.Errorf("got: %d, expected: %d", got, expected)
	}
}

// https://github.com/nats-io/nats-server/issues/5187
func TestGatewayConnectEvents(t *testing.T) {
	checkEvents := func(t *testing.T, name string, queue bool) {
		t.Run(name, func(t *testing.T) {
			ca := createClusterEx(t, true, 5*time.Millisecond, true, "A", 2)
			defer shutdownCluster(ca)

			cb := createClusterEx(t, true, 5*time.Millisecond, true, "B", 2, ca)
			defer shutdownCluster(cb)

			sysA, err := nats.Connect(ca.randomServer().ClientURL(), nats.UserInfo("sys", "pass"))
			require_NoError(t, err)
			defer sysA.Close()

			var sub1 *nats.Subscription
			if queue {
				sub1, err = sysA.QueueSubscribeSync("$SYS.ACCOUNT.FOO.CONNECT", "myqueue")
			} else {
				sub1, err = sysA.SubscribeSync("$SYS.ACCOUNT.FOO.CONNECT")
			}
			require_NoError(t, err)

			cA, err := nats.Connect(ca.randomServer().ClientURL(), nats.UserInfo("foo", "pass"))
			require_NoError(t, err)
			defer cA.Close()

			msg, err := sub1.NextMsg(time.Second)
			require_NoError(t, err)
			require_Equal(t, msg.Subject, "$SYS.ACCOUNT.FOO.CONNECT")

			cB, err := nats.Connect(cb.randomServer().ClientURL(), nats.UserInfo("foo", "pass"))
			require_NoError(t, err)
			defer cB.Close()

			msg, err = sub1.NextMsg(time.Second)
			require_NoError(t, err)
			require_Equal(t, msg.Subject, "$SYS.ACCOUNT.FOO.CONNECT")
		})
	}

	checkEvents(t, "Unqueued", false)
	checkEvents(t, "Queued", true)
}

func disconnectInboundGateways(s *Server) {
	s.gateway.RLock()
	in := s.gateway.in
	s.gateway.RUnlock()

	s.gateway.RLock()
	for _, client := range in {
		s.gateway.RUnlock()
		client.closeConnection(ClientClosed)
		s.gateway.RLock()
	}
	s.gateway.RUnlock()
}

type testMissingOCSPStapleLogger struct {
	DummyLogger
	ch chan string
}

func (l *testMissingOCSPStapleLogger) Errorf(format string, v ...any) {
	msg := fmt.Sprintf(format, v...)
	if strings.Contains(msg, "peer missing OCSP Staple") {
		select {
		case l.ch <- msg:
		default:
		}
	}
}

func TestGatewayOCSPMissingPeerStapleIssue(t *testing.T) {
	const (
		caCert = "../test/configs/certs/ocsp/ca-cert.pem"
		caKey  = "../test/configs/certs/ocsp/ca-key.pem"
	)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ocspr := NewOCSPResponderCustomTimeout(t, caCert, caKey, 10*time.Minute)
	defer ocspr.Shutdown(ctx)
	addr := fmt.Sprintf("http://%s", ocspr.Addr)

	// Node A
	SetOCSPStatus(t, addr, "../test/configs/certs/ocsp/server-status-request-url-01-cert.pem", ocsp.Good)
	SetOCSPStatus(t, addr, "../test/configs/certs/ocsp/server-status-request-url-02-cert.pem", ocsp.Good)

	// Node B
	SetOCSPStatus(t, addr, "../test/configs/certs/ocsp/server-status-request-url-03-cert.pem", ocsp.Good)
	SetOCSPStatus(t, addr, "../test/configs/certs/ocsp/server-status-request-url-04-cert.pem", ocsp.Good)

	// Node C
	SetOCSPStatus(t, addr, "../test/configs/certs/ocsp/server-status-request-url-05-cert.pem", ocsp.Good)
	SetOCSPStatus(t, addr, "../test/configs/certs/ocsp/server-status-request-url-06-cert.pem", ocsp.Good)

	// Node A rotated certs
	SetOCSPStatus(t, addr, "../test/configs/certs/ocsp/server-status-request-url-07-cert.pem", ocsp.Good)
	SetOCSPStatus(t, addr, "../test/configs/certs/ocsp/server-status-request-url-08-cert.pem", ocsp.Good)

	// Store Dirs
	storeDirA := t.TempDir()
	storeDirB := t.TempDir()
	storeDirC := t.TempDir()

	// Gateway server configuration
	srvConfA := `
		host: "127.0.0.1"
		port: -1

		server_name: "AAA"

		ocsp { mode = always }

                system_account = sys
                accounts {
                  sys   { users = [{ user: sys, pass: sys }]}
                  guest { users = [{ user: guest, pass: guest }]}
                }
                no_auth_user = guest

		store_dir: '%s'
		gateway {
			name: A
			host: "127.0.0.1"
			port: -1
			advertise: "127.0.0.1"

			tls {
				cert_file: "../test/configs/certs/ocsp/server-status-request-url-02-cert.pem"
				key_file: "../test/configs/certs/ocsp/server-status-request-url-02-key.pem"
				ca_file: "../test/configs/certs/ocsp/ca-cert.pem"
				timeout: 5
			}
		}
	`
	srvConfA = fmt.Sprintf(srvConfA, storeDirA)
	sconfA := createConfFile(t, []byte(srvConfA))
	srvA, optsA := RunServerWithConfig(sconfA)
	defer srvA.Shutdown()

	// Gateway B connects to Gateway A.
	srvConfB := `
		host: "127.0.0.1"
		port: -1

		server_name: "BBB"

		ocsp { mode = always }

                system_account = sys
                accounts {
                  sys   { users = [{ user: sys, pass: sys }]}
                  guest { users = [{ user: guest, pass: guest }]}
                }
                no_auth_user = guest

		store_dir: '%s'
		gateway {
			name: B
			host: "127.0.0.1"
			advertise: "127.0.0.1"
			port: -1
			gateways: [{
				name: "A"
				url: "nats://127.0.0.1:%d"
			}]

			tls {
				cert_file: "../test/configs/certs/ocsp/server-status-request-url-04-cert.pem"
				key_file: "../test/configs/certs/ocsp/server-status-request-url-04-key.pem"
				ca_file: "../test/configs/certs/ocsp/ca-cert.pem"
				timeout: 5
			}
		}
	`
	srvConfB = fmt.Sprintf(srvConfB, storeDirB, optsA.Gateway.Port)
	conf := createConfFile(t, []byte(srvConfB))
	srvB, optsB := RunServerWithConfig(conf)
	defer srvB.Shutdown()

	// Client connects to server A.
	cA, err := nats.Connect(fmt.Sprintf("nats://127.0.0.1:%d", optsA.Port),
		nats.ErrorHandler(noOpErrHandler),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer cA.Close()

	// Wait for connectivity between A and B.
	waitForOutboundGateways(t, srvB, 1, 5*time.Second)

	// Gateway C also connects to Gateway A.
	srvConfC := `
		host: "127.0.0.1"
		port: -1

		server_name: "CCC"

		ocsp { mode = always }

                system_account = sys
                accounts {
                  sys   { users = [{ user: sys, pass: sys }]}
                  guest { users = [{ user: guest, pass: guest }]}
                }
                no_auth_user = guest

		store_dir: '%s'
		gateway {
			name: C
			host: "127.0.0.1"
			advertise: "127.0.0.1"
			port: -1
			gateways: [{name: "A", url: "nats://127.0.0.1:%d" }]

			tls {
				cert_file: "../test/configs/certs/ocsp/server-status-request-url-06-cert.pem"
				key_file: "../test/configs/certs/ocsp/server-status-request-url-06-key.pem"
				ca_file: "../test/configs/certs/ocsp/ca-cert.pem"
				timeout: 5
			}
		}
	`
	srvConfC = fmt.Sprintf(srvConfC, storeDirC, optsA.Gateway.Port)
	conf = createConfFile(t, []byte(srvConfC))
	srvC, optsC := RunServerWithConfig(conf)
	defer srvC.Shutdown()

	////////////////////////////////////////////////////////////////////////////
	//                                                                        //
	//  A and B are connected at this point and A is starting with certs that //
	//  will be rotated.
	//                                                                        //
	////////////////////////////////////////////////////////////////////////////
	cB, err := nats.Connect(fmt.Sprintf("nats://127.0.0.1:%d", optsB.Port),
		nats.ErrorHandler(noOpErrHandler),
	)
	require_NoError(t, err)
	defer cB.Close()

	cC, err := nats.Connect(fmt.Sprintf("nats://127.0.0.1:%d", optsC.Port),
		nats.ErrorHandler(noOpErrHandler),
	)
	require_NoError(t, err)
	defer cC.Close()

	_, err = cA.Subscribe("foo", func(m *nats.Msg) {
		m.Respond(nil)
	})
	require_NoError(t, err)

	cA.Flush()

	_, err = cB.Subscribe("bar", func(m *nats.Msg) {
		m.Respond(nil)
	})
	require_NoError(t, err)
	cB.Flush()

	waitForOutboundGateways(t, srvB, 1, 10*time.Second)
	waitForOutboundGateways(t, srvC, 2, 10*time.Second)

	/////////////////////////////////////////////////////////////////////////////////
	//                                                                             //
	//  Switch all the certs from server A, all OCSP monitors should be restarted  //
	//  so it should have new staples.                                             //
	//                                                                             //
	/////////////////////////////////////////////////////////////////////////////////
	srvConfA = `
		host: "127.0.0.1"
		port: -1

		server_name: "AAA"

		ocsp { mode = always }

                system_account = sys
                accounts {
                  sys   { users = [{ user: sys, pass: sys }]}
                  guest { users = [{ user: guest, pass: guest }]}
                }
                no_auth_user = guest

		store_dir: '%s'
		gateway {
			name: A
			host: "127.0.0.1"
			port: -1
			advertise: "127.0.0.1"

			tls {
				cert_file: "../test/configs/certs/ocsp/server-status-request-url-08-cert.pem"
				key_file: "../test/configs/certs/ocsp/server-status-request-url-08-key.pem"
				ca_file: "../test/configs/certs/ocsp/ca-cert.pem"
				timeout: 5

			}
		}
	`

	srvConfA = fmt.Sprintf(srvConfA, storeDirA)
	if err := os.WriteFile(sconfA, []byte(srvConfA), 0666); err != nil {
		t.Fatalf("Error writing config: %v", err)
	}
	if err := srvA.Reload(); err != nil {
		t.Fatal(err)
	}
	waitForOutboundGateways(t, srvA, 2, 5*time.Second)
	waitForOutboundGateways(t, srvB, 2, 5*time.Second)
	waitForOutboundGateways(t, srvC, 2, 5*time.Second)

	// Now clients connect to C can communicate with B and A.
	_, err = cC.Request("foo", nil, 2*time.Second)
	require_NoError(t, err)

	_, err = cC.Request("bar", nil, 2*time.Second)
	require_NoError(t, err)

	// Reload and disconnect very fast trying to produce the race.
	ctx, cancel = context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	// Swap logger from server to capture the missing peer log.
	lA := &testMissingOCSPStapleLogger{ch: make(chan string, 30)}
	srvA.SetLogger(lA, false, false)

	lB := &testMissingOCSPStapleLogger{ch: make(chan string, 30)}
	srvB.SetLogger(lB, false, false)

	lC := &testMissingOCSPStapleLogger{ch: make(chan string, 30)}
	srvC.SetLogger(lC, false, false)

	// Start with a reload from the last server that connected directly to A.
	err = srvC.Reload()
	require_NoError(t, err)

	// Stress reconnections and reloading servers without getting
	// missing OCSP peer staple errors.
	var wg sync.WaitGroup

	wg.Add(1)
	go func() {
		for range time.NewTicker(500 * time.Millisecond).C {
			select {
			case <-ctx.Done():
				wg.Done()
				return
			default:
			}
			disconnectInboundGateways(srvA)
		}
	}()

	wg.Add(1)
	go func() {
		for range time.NewTicker(500 * time.Millisecond).C {
			select {
			case <-ctx.Done():
				wg.Done()
				return
			default:
			}
			disconnectInboundGateways(srvB)
		}
	}()

	wg.Add(1)
	go func() {
		for range time.NewTicker(500 * time.Millisecond).C {
			select {
			case <-ctx.Done():
				wg.Done()
				return
			default:
			}
			disconnectInboundGateways(srvC)
		}
	}()

	wg.Add(1)
	go func() {
		for range time.NewTicker(700 * time.Millisecond).C {
			select {
			case <-ctx.Done():
				wg.Done()
				return
			default:
			}
			srvC.Reload()
		}
	}()

	wg.Add(1)
	go func() {
		for range time.NewTicker(800 * time.Millisecond).C {
			select {
			case <-ctx.Done():
				wg.Done()
				return
			default:
			}
			srvB.Reload()
		}
	}()

	wg.Add(1)
	go func() {
		for range time.NewTicker(900 * time.Millisecond).C {
			select {
			case <-ctx.Done():
				wg.Done()
				return
			default:
			}
			srvA.Reload()
		}
	}()

	select {
	case <-ctx.Done():
	case msg := <-lA.ch:
		t.Fatalf("Server A: Got OCSP Staple error: %v", msg)
	case msg := <-lB.ch:
		t.Fatalf("Server B: Got OCSP Staple error: %v", msg)
	case msg := <-lC.ch:
		t.Fatalf("Server C: Got OCSP Staple error: %v", msg)
	}
	waitForOutboundGateways(t, srvA, 2, 5*time.Second)
	waitForOutboundGateways(t, srvB, 2, 5*time.Second)
	waitForOutboundGateways(t, srvC, 2, 5*time.Second)
	wg.Wait()
}

func TestGatewayOutboundDetectsStaleConnectionIfNoInfo(t *testing.T) {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	require_NoError(t, err)
	defer l.Close()

	ch := make(chan struct{})
	wg := sync.WaitGroup{}
	wg.Add(1)
	go func() {
		defer wg.Done()
		c, err := l.Accept()
		if err != nil {
			return
		}
		defer c.Close()
		<-ch
	}()

	url := fmt.Sprintf("nats://%s", l.Addr())
	o := testGatewayOptionsFromToWithURLs(t, "A", "B", []string{url})
	o.gatewaysSolicitDelay = time.Millisecond
	o.DisableShortFirstPing = false
	o.PingInterval = 50 * time.Millisecond
	o.MaxPingsOut = 3
	o.NoLog = false
	s, err := NewServer(o)
	require_NoError(t, err)
	defer s.Shutdown()

	log := &captureDebugLogger{dbgCh: make(chan string, 100)}
	s.SetLogger(log, true, false)
	s.Start()

	timeout := time.NewTimer(time.Second)
	defer timeout.Stop()
	for done := false; !done; {
		select {
		case dbg := <-log.dbgCh:
			// The server should not send PING because the accept side expects
			// the CONNECT as the first protocol (otherwise it would be a parse
			// error if that were to happen).
			if strings.Contains(dbg, "Ping Timer") {
				t.Fatalf("The server should not have sent a ping, got %q", dbg)
			}
			// However, it should detect at one point that the connection is
			// stale and close it.
			if strings.Contains(dbg, "Stale") {
				done = true
			}
		case <-timeout.C:
			t.Fatalf("Did not capture the stale connection condition")
		}
	}

	s.Shutdown()
	close(ch)
	wg.Wait()
	s.WaitForShutdown()
}

func TestGatewayConfigureWriteDeadline(t *testing.T) {
	o1 := testDefaultOptionsForGateway("B")
	o1.Gateway.WriteDeadline = 5 * time.Second
	s1 := runGatewayServer(o1)
	defer s1.Shutdown()

	o2 := testGatewayOptionsFromToWithServers(t, "A", "B", s1)
	s2 := runGatewayServer(o2)
	defer s2.Shutdown()

	waitForOutboundGateways(t, s2, 1, time.Second)
	waitForInboundGateways(t, s1, 1, time.Second)
	waitForOutboundGateways(t, s1, 1, time.Second)

	s1.mu.RLock()
	defer s1.mu.RUnlock()

	s1.gateway.RLock()
	defer s1.gateway.RUnlock()

	for _, r := range s1.gateway.out {
		r.mu.Lock()
		wdl := r.out.wdl
		r.mu.Unlock()
		require_Equal(t, wdl, 5*time.Second)
	}

	for _, r := range s1.gateway.in {
		r.mu.Lock()
		wdl := r.out.wdl
		r.mu.Unlock()
		require_Equal(t, wdl, 5*time.Second)
	}
}

func TestGatewayConfigureWriteTimeoutPolicy(t *testing.T) {
	for name, policy := range map[string]WriteTimeoutPolicy{
		"Default": WriteTimeoutPolicyDefault,
		"Retry":   WriteTimeoutPolicyRetry,
		"Close":   WriteTimeoutPolicyClose,
	} {
		t.Run(name, func(t *testing.T) {
			o1 := testDefaultOptionsForGateway("B")
			o1.Gateway.WriteTimeout = policy
			s1 := runGatewayServer(o1)
			defer s1.Shutdown()

			o2 := testGatewayOptionsFromToWithServers(t, "A", "B", s1)
			s2 := runGatewayServer(o2)
			defer s2.Shutdown()

			waitForOutboundGateways(t, s2, 1, time.Second)
			waitForInboundGateways(t, s1, 1, time.Second)
			waitForOutboundGateways(t, s1, 1, time.Second)

			s1.mu.RLock()
			defer s1.mu.RUnlock()

			s1.gateway.RLock()
			defer s1.gateway.RUnlock()

			for _, r := range s1.gateway.out {
				if policy == WriteTimeoutPolicyDefault {
					require_Equal(t, r.out.wtp, WriteTimeoutPolicyRetry)
				} else {
					require_Equal(t, r.out.wtp, policy)
				}
			}

			for _, r := range s1.gateway.in {
				if policy == WriteTimeoutPolicyDefault {
					require_Equal(t, r.out.wtp, WriteTimeoutPolicyRetry)
				} else {
					require_Equal(t, r.out.wtp, policy)
				}
			}
		})
	}
}

func TestGatewayProcessRSubNoBlockingAccountFetch(t *testing.T) {
	createAccountPubKey := func() string {
		kp, err := nkeys.CreateAccount()
		require_NoError(t, err)
		pubkey, err := kp.PublicKey()
		require_NoError(t, err)
		return pubkey
	}
	sysPub := createAccountPubKey()
	accPub := createAccountPubKey()
	dir := t.TempDir()
	conf := createConfFile(t, []byte(fmt.Sprintf(`
		listen: 127.0.0.1:-1
		server_name: srv
		operator: %s
		system_account: %s
		resolver: {
			type: cache
			dir: '%s'
			timeout: "2s"
		}
		gateway: {
			name: "clust-B"
			listen: 127.0.0.1:-1
		}
       `, ojwt, sysPub, dir)))
	s, _ := RunServerWithConfig(conf)
	defer s.Shutdown()

	// Set up a mock gateway client.
	c := s.createInternalAccountClient()
	c.mu.Lock()
	c.gw = &gateway{}
	c.gw.outsim = &sync.Map{}
	c.nc = &net.IPConn{}
	c.mu.Unlock()

	// Receiving a R+ should not be blocking, since we're in the gateway's readLoop.
	start := time.Now()
	require_NoError(t, c.processGatewayRSub(fmt.Appendf(nil, "%s subj queue 0", accPub)))
	ei, ok := c.gw.outsim.Load(accPub)
	require_True(t, ok)
	require_True(t, ei.(*outsie).sl.CacheEnabled())
	c.mu.Lock()
	subs := len(c.subs)
	c.mu.Unlock()
	require_Len(t, subs, 1)
	require_LessThan(t, time.Since(start), 100*time.Millisecond)

	// Receiving a R- should not be blocking, since we're in the gateway's readLoop.
	start = time.Now()
	require_NoError(t, c.processGatewayRUnsub(fmt.Appendf(nil, "%s subj queue", accPub)))
	c.mu.Lock()
	subs = len(c.subs)
	c.mu.Unlock()
	require_Len(t, subs, 0)
	require_LessThan(t, time.Since(start), 100*time.Millisecond)

	c2 := s.createInternalAccountClient()
	c2.mu.Lock()
	c2.gw = &gateway{}
	c2.gw.outsim = &sync.Map{}
	c2.nc = &net.IPConn{}
	c2.mu.Unlock()

	require_NoError(t, c2.processGatewayRUnsub(fmt.Appendf(nil, "%s subj", accPub)))
	ei, ok = c2.gw.outsim.Load(accPub)
	require_True(t, ok)
	require_True(t, ei.(*outsie).sl.CacheEnabled())
}

func TestGatewayLogsRemoteURL(t *testing.T) {
	o2 := testDefaultOptionsForGateway("B")
	o2.ServerName = "gw-b"
	o2.Gateway.Username = remoteURLLogUser
	o2.Gateway.Password = remoteURLLogPassword
	s2 := runGatewayServer(o2)
	defer s2.Shutdown()

	gwHost := fmt.Sprintf("localhost:%d", s2.GatewayAddr().Port)
	u := &url.URL{Scheme: "nats", Host: gwHost, User: url.UserPassword(remoteURLLogUser, remoteURLLogPassword)}
	o1 := testGatewayOptionsFromToWithURLs(t, "A", "B", []string{u.String()})
	o1.Gateway.resolver = &localhostResolver{}
	s1, l := runServerWithNoticeLogger(t, o1)
	defer s1.Shutdown()

	waitForOutboundGateways(t, s1, 1, 2*time.Second)
	checkNoticeContains(t, l, `Creating outbound gateway connection to "B" (`+gwHost+")")

	s2.Shutdown()
	checkNoticeContains(t, l, "Gateway connection closed: "+ClientClosed.String()+" - Remote: "+s2.Name()+", Host: "+gwHost)
	s1.Shutdown()
	checkNoticeNotContains(t, l, remoteURLLogUser, remoteURLLogPassword)
}
