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

package server

import (
	"bufio"
	"bytes"
	"compress/flate"
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"io"
	"math/rand/v2"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nats-io/nkeys"

	"github.com/klauspost/compress/s2"
	jwt "github.com/nats-io/jwt/v2"
	"github.com/nats-io/nats.go"

	"github.com/nats-io/nats-server/v2/internal/testhelper"
)

type captureLeafNodeRandomIPLogger struct {
	DummyLogger
	ch  chan struct{}
	ips [3]int
}

func (c *captureLeafNodeRandomIPLogger) Debugf(format string, v ...any) {
	msg := fmt.Sprintf(format, v...)
	if strings.Contains(msg, "hostname_to_resolve") {
		ippos := strings.Index(msg, "127.0.0.")
		if ippos != -1 {
			n := int(msg[ippos+8] - '1')
			c.ips[n]++
			for _, v := range c.ips {
				if v < 2 {
					return
				}
			}
			// All IPs got at least some hit, we are done.
			c.ch <- struct{}{}
		}
	}
}

func TestLeafNodeRandomIP(t *testing.T) {
	u, err := url.Parse("nats://hostname_to_resolve:1234")
	if err != nil {
		t.Fatalf("Error parsing: %v", err)
	}

	resolver := &myDummyDNSResolver{ips: []string{"127.0.0.1", "127.0.0.2", "127.0.0.3"}}

	o := DefaultOptions()
	o.Host = "127.0.0.1"
	o.Port = -1
	o.LeafNode.Port = 0
	o.LeafNode.Remotes = []*RemoteLeafOpts{{URLs: []*url.URL{u}}}
	o.LeafNode.ReconnectInterval = 50 * time.Millisecond
	o.LeafNode.resolver = resolver
	o.LeafNode.DialTimeout = 15 * time.Millisecond
	s := RunServer(o)
	defer s.Shutdown()

	l := &captureLeafNodeRandomIPLogger{ch: make(chan struct{})}
	s.SetLogger(l, true, true)

	select {
	case <-l.ch:
	case <-time.After(3 * time.Second):
		t.Fatalf("Does not seem to have used random IPs")
	}
}

func TestLeafNodeRandomRemotes(t *testing.T) {
	// 16! possible permutations.
	orderedURLs := make([]*url.URL, 0, 16)
	for i := 0; i < cap(orderedURLs); i++ {
		orderedURLs = append(orderedURLs, &url.URL{
			Scheme: "nats-leaf",
			Host:   fmt.Sprintf("host%d:7422", i),
		})
	}

	o := DefaultOptions()
	o.Accounts = append(o.Accounts, NewAccount("A"), NewAccount("B"))
	o.LeafNode.Remotes = []*RemoteLeafOpts{
		{NoRandomize: true, LocalAccount: "A"},
		{NoRandomize: false, LocalAccount: "B"},
	}
	o.LeafNode.Remotes[0].URLs = make([]*url.URL, cap(orderedURLs))
	copy(o.LeafNode.Remotes[0].URLs, orderedURLs)
	o.LeafNode.Remotes[1].URLs = make([]*url.URL, cap(orderedURLs))
	copy(o.LeafNode.Remotes[1].URLs, orderedURLs)

	s := RunServer(o)
	defer s.Shutdown()

	var r1, r2 *leafNodeCfg
	s.mu.RLock()
	for r := range s.leafRemoteCfgs {
		// Verify that the order of urls in the options have not been changed.
		require_True(t, reflect.DeepEqual(r.URLs, orderedURLs))
		// Capture the `leafNodeCfg` based on the NoRandomize option.
		if r.NoRandomize {
			r1 = r
		} else {
			r2 = r
		}
	}
	s.mu.RUnlock()

	r1.RLock()
	gotOrdered := r1.urls
	r1.RUnlock()
	if got, want := len(gotOrdered), len(orderedURLs); got != want {
		t.Fatalf("Unexpected rem0 len URLs, got %d, want %d", got, want)
	}

	// These should be IN order.
	for i := range orderedURLs {
		if got, want := gotOrdered[i].String(), orderedURLs[i].String(); got != want {
			t.Fatalf("Unexpected ordered url, got %s, want %s", got, want)
		}
	}

	r2.RLock()
	gotRandom := r2.urls
	r2.RUnlock()
	if got, want := len(gotRandom), len(orderedURLs); got != want {
		t.Fatalf("Unexpected rem1 len URLs, got %d, want %d", got, want)
	}

	// These should be OUT of order.
	var random bool
	for i := range orderedURLs {
		if gotRandom[i].String() != orderedURLs[i].String() {
			random = true
			break
		}
	}
	if !random {
		t.Fatal("Expected urls to be random")
	}
}

type testLoopbackResolver struct{}

func (r *testLoopbackResolver) LookupHost(ctx context.Context, host string) ([]string, error) {
	return []string{"127.0.0.1"}, nil
}

func TestLeafNodeTLSWithCerts(t *testing.T) {
	conf1 := createConfFile(t, []byte(`
		port: -1
		leaf {
			listen: "127.0.0.1:-1"
			tls {
				ca_file: "../test/configs/certs/tlsauth/ca.pem"
				cert_file: "../test/configs/certs/tlsauth/server.pem"
				key_file:  "../test/configs/certs/tlsauth/server-key.pem"
				timeout: 2
			}
		}
	`))
	s1, o1 := RunServerWithConfig(conf1)
	defer s1.Shutdown()

	u, err := url.Parse(fmt.Sprintf("nats://localhost:%d", o1.LeafNode.Port))
	if err != nil {
		t.Fatalf("Error parsing url: %v", err)
	}
	conf2 := createConfFile(t, []byte(fmt.Sprintf(`
		port: -1
		leaf {
			remotes [
				{
					url: "%s"
					tls {
						ca_file: "../test/configs/certs/tlsauth/ca.pem"
						cert_file: "../test/configs/certs/tlsauth/client.pem"
						key_file:  "../test/configs/certs/tlsauth/client-key.pem"
						timeout: 2
					}
				}
			]
		}
	`, u.String())))
	o2, err := ProcessConfigFile(conf2)
	if err != nil {
		t.Fatalf("Error processing config file: %v", err)
	}
	o2.NoLog, o2.NoSigs = true, true
	o2.LeafNode.resolver = &testLoopbackResolver{}
	s2 := RunServer(o2)
	defer s2.Shutdown()

	checkFor(t, 3*time.Second, 10*time.Millisecond, func() error {
		if nln := s1.NumLeafNodes(); nln != 1 {
			return fmt.Errorf("Number of leaf nodes is %d", nln)
		}
		return nil
	})
}

func TestLeafNodeTLSRemoteWithNoCerts(t *testing.T) {
	conf1 := createConfFile(t, []byte(`
		port: -1
		leaf {
			listen: "127.0.0.1:-1"
			tls {
				ca_file: "../test/configs/certs/tlsauth/ca.pem"
				cert_file: "../test/configs/certs/tlsauth/server.pem"
				key_file:  "../test/configs/certs/tlsauth/server-key.pem"
				timeout: 2
			}
		}
	`))
	s1, o1 := RunServerWithConfig(conf1)
	defer s1.Shutdown()

	u, err := url.Parse(fmt.Sprintf("nats://localhost:%d", o1.LeafNode.Port))
	if err != nil {
		t.Fatalf("Error parsing url: %v", err)
	}
	conf2 := createConfFile(t, []byte(fmt.Sprintf(`
		port: -1
		leaf {
			remotes [
				{
					url: "%s"
					tls {
						ca_file: "../test/configs/certs/tlsauth/ca.pem"
						timeout: 5
					}
				}
			]
		}
	`, u.String())))
	o2, err := ProcessConfigFile(conf2)
	if err != nil {
		t.Fatalf("Error processing config file: %v", err)
	}

	if len(o2.LeafNode.Remotes) == 0 {
		t.Fatal("Expected at least a single leaf remote")
	}

	var (
		got      float64 = o2.LeafNode.Remotes[0].TLSTimeout
		expected float64 = 5
	)
	if got != expected {
		t.Fatalf("Expected %v, got: %v", expected, got)
	}
	o2.NoLog, o2.NoSigs = true, true
	o2.LeafNode.resolver = &testLoopbackResolver{}
	s2 := RunServer(o2)
	defer s2.Shutdown()

	checkFor(t, 3*time.Second, 10*time.Millisecond, func() error {
		if nln := s1.NumLeafNodes(); nln != 1 {
			return fmt.Errorf("Number of leaf nodes is %d", nln)
		}
		return nil
	})

	// Here we only process options without starting the server
	// and without a root CA for the remote.
	conf3 := createConfFile(t, []byte(fmt.Sprintf(`
		port: -1
		leaf {
			remotes [
				{
					url: "%s"
					tls {
						timeout: 10
					}
				}
			]
		}
	`, u.String())))
	o3, err := ProcessConfigFile(conf3)
	if err != nil {
		t.Fatalf("Error processing config file: %v", err)
	}

	if len(o3.LeafNode.Remotes) == 0 {
		t.Fatal("Expected at least a single leaf remote")
	}
	got = o3.LeafNode.Remotes[0].TLSTimeout
	expected = 10
	if got != expected {
		t.Fatalf("Expected %v, got: %v", expected, got)
	}

	// Here we only process options without starting the server
	// and check the default for leafnode remotes.
	conf4 := createConfFile(t, []byte(fmt.Sprintf(`
		port: -1
		leaf {
			remotes [
				{
					url: "%s"
					tls {
						ca_file: "../test/configs/certs/tlsauth/ca.pem"
					}
				}
			]
		}
	`, u.String())))
	o4, err := ProcessConfigFile(conf4)
	if err != nil {
		t.Fatalf("Error processing config file: %v", err)
	}

	if len(o4.LeafNode.Remotes) == 0 {
		t.Fatal("Expected at least a single leaf remote")
	}
	got = o4.LeafNode.Remotes[0].TLSTimeout
	expected = float64(DEFAULT_LEAF_TLS_TIMEOUT) / float64(time.Second)
	if int(got) != int(expected) {
		t.Fatalf("Expected %v, got: %v", expected, got)
	}
}

type captureErrorLogger struct {
	DummyLogger
	errCh chan string
}

func (l *captureErrorLogger) Errorf(format string, v ...any) {
	select {
	case l.errCh <- fmt.Sprintf(format, v...):
	default:
	}
}

func TestLeafNodeAccountNotFound(t *testing.T) {
	ob := DefaultOptions()
	ob.LeafNode.Host = "127.0.0.1"
	ob.LeafNode.Port = -1
	sb := RunServer(ob)
	defer sb.Shutdown()

	u, _ := url.Parse(fmt.Sprintf("nats://127.0.0.1:%d", ob.LeafNode.Port))

	oa := DefaultOptions()
	oa.Cluster.Name = "xyz"
	oa.LeafNode.ReconnectInterval = 10 * time.Millisecond
	oa.LeafNode.Remotes = []*RemoteLeafOpts{
		{
			LocalAccount: "foo",
			URLs:         []*url.URL{u},
		},
	}
	// Expected to fail
	if _, err := NewServer(oa); err == nil || !strings.Contains(err.Error(), "local account") {
		t.Fatalf("Expected server to fail with error about no local account, got %v", err)
	}
	oa.Accounts = []*Account{NewAccount("foo")}
	sa := RunServer(oa)
	defer sa.Shutdown()

	l := &captureErrorLogger{errCh: make(chan string, 1)}
	sa.SetLogger(l, false, false)

	checkLeafNodeConnected(t, sa)

	// Now simulate account is removed with config reload, or it expires.
	sa.accounts.Delete("foo")

	// Restart B (with same Port)
	sb.Shutdown()
	sb = RunServer(ob)
	defer sb.Shutdown()

	// Wait for report of error
	select {
	case e := <-l.errCh:
		if !strings.Contains(e, "Unable to lookup account") {
			t.Fatalf("Expected error about no local account, got %s", e)
		}
	case <-time.After(2 * time.Second):
		t.Fatalf("Did not get the error")
	}

	// TODO below test is bogus. Instead add the account, do a reload, and make sure the connection works.

	// For now, sa would try to recreate the connection for ever.
	// Check that lid is increasing...
	checkFor(t, 2*time.Second, 100*time.Millisecond, func() error {
		if lid := atomic.LoadUint64(&sa.gcid); lid < 3 {
			return fmt.Errorf("Seems like connection was not retried, lid currently only at %d", lid)
		}
		return nil
	})
}

// This test ensures that we can connect using proper user/password
// to a LN URL that was discovered through the INFO protocol.
// We also check that the password doesn't leak to debug/trace logs.
func TestLeafNodeBasicAuthFailover(t *testing.T) {
	// Something a little longer than "pwd" to prevent false positives amongst many log lines;
	// don't make it complex enough to be subject to %-escaping, we want a simple needle search.
	fatalPassword := "pwdfatal"

	content := `
	listen: "127.0.0.1:-1"
	cluster {
		name: "abc"
		listen: "127.0.0.1:-1"
		%s
	}
	leafnodes {
		listen: "127.0.0.1:-1"
		authorization {
			user: foo
			password: %s
			timeout: 1
		}
	}
	`
	conf := createConfFile(t, []byte(fmt.Sprintf(content, "", fatalPassword)))

	sb1, ob1 := RunServerWithConfig(conf)
	defer sb1.Shutdown()

	conf = createConfFile(t, []byte(fmt.Sprintf(content, fmt.Sprintf("routes: [nats://127.0.0.1:%d]", ob1.Cluster.Port), fatalPassword)))

	sb2, _ := RunServerWithConfig(conf)
	defer sb2.Shutdown()

	checkClusterFormed(t, sb1, sb2)

	content = `
	port: -1
	accounts {
		foo {}
	}
	leafnodes {
		listen: "127.0.0.1:-1"
		remotes [
			{
				account: "foo"
				url: "nats://foo:%s@127.0.0.1:%d"
			}
		]
	}
	`
	conf = createConfFile(t, []byte(fmt.Sprintf(content, fatalPassword, ob1.LeafNode.Port)))

	sa, _ := RunServerWithConfig(conf)
	defer sa.Shutdown()

	l := testhelper.NewDummyLogger(100)
	sa.SetLogger(l, true, true) // we want debug & trace logs, to check for passwords in them

	checkLeafNodeConnected(t, sa)

	// Shutdown sb1, sa should reconnect to sb2
	sb1.Shutdown()

	// Wait a bit to make sure there was a disconnect and attempt to reconnect.
	time.Sleep(250 * time.Millisecond)

	// Should be able to reconnect
	checkLeafNodeConnected(t, sa)

	// Look at all our logs for the password; at time of writing it doesn't appear
	// but we want to safe-guard against it.
	l.CheckForProhibited(t, "fatal password", fatalPassword)
}

func TestLeafNodeRTT(t *testing.T) {
	ob := DefaultOptions()
	ob.PingInterval = 15 * time.Millisecond
	ob.LeafNode.Host = "127.0.0.1"
	ob.LeafNode.Port = -1
	sb := RunServer(ob)
	defer sb.Shutdown()

	lnBURL, _ := url.Parse(fmt.Sprintf("nats://127.0.0.1:%d", ob.LeafNode.Port))
	oa := DefaultOptions()
	oa.Cluster.Name = "xyz"
	oa.PingInterval = 15 * time.Millisecond
	oa.LeafNode.Remotes = []*RemoteLeafOpts{{URLs: []*url.URL{lnBURL}}}
	sa := RunServer(oa)
	defer sa.Shutdown()

	checkLeafNodeConnected(t, sa)

	checkRTT := func(t *testing.T, s *Server) time.Duration {
		t.Helper()
		var ln *client
		s.mu.Lock()
		for _, l := range s.leafs {
			ln = l
			break
		}
		s.mu.Unlock()
		var rtt time.Duration
		checkFor(t, 2*firstPingInterval, 15*time.Millisecond, func() error {
			ln.mu.Lock()
			rtt = ln.rtt
			ln.mu.Unlock()
			if rtt == 0 {
				return fmt.Errorf("RTT not tracked")
			}
			return nil
		})
		return rtt
	}

	prevA := checkRTT(t, sa)
	prevB := checkRTT(t, sb)

	// Wait to see if RTT is updated
	checkUpdated := func(t *testing.T, s *Server, prev time.Duration) {
		attempts := 0
		timeout := time.Now().Add(2 * firstPingInterval)
		for time.Now().Before(timeout) {
			if rtt := checkRTT(t, s); rtt != prev {
				return
			}
			attempts++
			if attempts == 5 {
				s.mu.Lock()
				for _, ln := range s.leafs {
					ln.mu.Lock()
					ln.rtt = 0
					ln.mu.Unlock()
					break
				}
				s.mu.Unlock()
			}
			time.Sleep(15 * time.Millisecond)
		}
		t.Fatalf("RTT probably not updated")
	}
	checkUpdated(t, sa, prevA)
	checkUpdated(t, sb, prevB)

	sa.Shutdown()
	sb.Shutdown()

	// Now check that initial RTT is computed prior to first PingInterval
	// Get new options to avoid possible race changing the ping interval.
	ob = DefaultOptions()
	ob.Cluster.Name = "xyz"
	ob.PingInterval = time.Minute
	ob.LeafNode.Host = "127.0.0.1"
	ob.LeafNode.Port = -1
	sb = RunServer(ob)
	defer sb.Shutdown()

	lnBURL, _ = url.Parse(fmt.Sprintf("nats://127.0.0.1:%d", ob.LeafNode.Port))
	oa = DefaultOptions()
	oa.PingInterval = time.Minute
	oa.LeafNode.Remotes = []*RemoteLeafOpts{{URLs: []*url.URL{lnBURL}}}
	sa = RunServer(oa)
	defer sa.Shutdown()

	checkLeafNodeConnected(t, sa)

	checkRTT(t, sa)
	checkRTT(t, sb)
}

func TestLeafNodeCustomPing(t *testing.T) {
	pingInterval := 50 * time.Millisecond

	ob := DefaultOptions()
	ob.LeafNode.Host = "127.0.0.1"
	ob.LeafNode.Port = -1
	ob.LeafNode.PingInterval = pingInterval
	ob.LeafNode.MaxPingsOut = 2
	sb := RunServer(ob)
	defer sb.Shutdown()

	lnBURL, _ := url.Parse(fmt.Sprintf("nats://127.0.0.1:%d", ob.LeafNode.Port))
	oa := DefaultOptions()
	oa.Cluster.Name = "xyz"
	oa.LeafNode.Remotes = []*RemoteLeafOpts{{URLs: []*url.URL{lnBURL}}}
	oa.LeafNode.PingInterval = pingInterval
	sa := RunServer(oa)
	defer sa.Shutdown()

	checkLeafNodeConnected(t, sa)

	var ln *client
	sa.mu.Lock()
	for _, l := range sa.leafs {
		ln = l
		break
	}
	sa.mu.Unlock()

	ch := make(chan struct{}, 1)
	ln.mu.Lock()
	ln.nc = &capturePingConn{ln.nc, ch}
	ln.mu.Unlock()

	for i := 0; i < 5; i++ {
		select {
		case <-ch:
		case <-time.After(250 * time.Millisecond):
			t.Fatalf("Did not send PING")
		}
	}
}

func TestLeafNodeValidateAuthOptions(t *testing.T) {
	opts := DefaultOptions()
	opts.LeafNode.Username = "user1"
	opts.LeafNode.Password = "pwd"
	opts.LeafNode.Users = []*User{{Username: "user", Password: "pwd"}}
	if _, err := NewServer(opts); err == nil || !strings.Contains(err.Error(),
		"can not have a single user/pass and a users array") {
		t.Fatalf("Expected error about mixing single/multi users, got %v", err)
	}

	// Check duplicate user names
	opts.LeafNode.Username = _EMPTY_
	opts.LeafNode.Password = _EMPTY_
	opts.LeafNode.Users = append(opts.LeafNode.Users, &User{Username: "user", Password: "pwd"})
	if _, err := NewServer(opts); err == nil || !strings.Contains(err.Error(), "duplicate user") {
		t.Fatalf("Expected error about duplicate user, got %v", err)
	}
}

func TestLeafNodeBasicAuthSingleton(t *testing.T) {
	opts := DefaultOptions()
	opts.LeafNode.Port = -1
	opts.LeafNode.Account = "unknown"
	if s, err := NewServer(opts); err == nil || !strings.Contains(err.Error(), "cannot find") {
		if s != nil {
			s.Shutdown()
		}
		t.Fatalf("Expected error about account not found, got %v", err)
	}

	template := `
		port: -1
		accounts: {
			ACC1: { users = [{user: "user1", password: "user1"}] }
			ACC2: { users = [{user: "user2", password: "user2"}] }
		}
		leafnodes: {
			port: -1
			authorization {
			  %s
              account: "ACC1"
            }
		}
	`
	for iter, test := range []struct {
		name       string
		userSpec   string
		lnURLCreds string
		shouldFail bool
	}{
		{"user creds required and no user so fails", "", "", true},
		{"user creds required and pick user2 associated to ACC2", "", "user2:user2@", false},
		{"user creds required and unknown user should fail", "", "unknown:user@", true},
		{"user creds required so binds to ACC1", "user: \"ln\"\npass: \"pwd\"", "ln:pwd@", false},
	} {
		t.Run(test.name, func(t *testing.T) {
			conf := createConfFile(t, []byte(fmt.Sprintf(template, test.userSpec)))
			s1, o1 := RunServerWithConfig(conf)
			defer s1.Shutdown()

			// Create a sub on "foo" for account ACC1 (user user1), which is the one
			// bound to the accepted LN connection.
			ncACC1 := natsConnect(t, fmt.Sprintf("nats://user1:user1@%s:%d", o1.Host, o1.Port))
			defer ncACC1.Close()
			sub1 := natsSubSync(t, ncACC1, "foo")
			natsFlush(t, ncACC1)

			// Create a sub on "foo" for account ACC2 (user user2). This one should
			// not receive any message.
			ncACC2 := natsConnect(t, fmt.Sprintf("nats://user2:user2@%s:%d", o1.Host, o1.Port))
			defer ncACC2.Close()
			sub2 := natsSubSync(t, ncACC2, "foo")
			natsFlush(t, ncACC2)

			conf = createConfFile(t, []byte(fmt.Sprintf(`
				port: -1
				leafnodes: {
					remotes = [ { url: "nats-leaf://%s%s:%d" } ]
				}
			`, test.lnURLCreds, o1.LeafNode.Host, o1.LeafNode.Port)))
			s2, _ := RunServerWithConfig(conf)
			defer s2.Shutdown()

			if test.shouldFail {
				// Wait a bit and ensure that there is no leaf node connection
				time.Sleep(100 * time.Millisecond)
				checkFor(t, time.Second, 15*time.Millisecond, func() error {
					if n := s1.NumLeafNodes(); n != 0 {
						return fmt.Errorf("Expected no leafnode connection, got %v", n)
					}
					return nil
				})
				return
			}

			checkLeafNodeConnected(t, s2)

			nc := natsConnect(t, s2.ClientURL())
			defer nc.Close()
			natsPub(t, nc, "foo", []byte("hello"))
			// If url contains known user, even when there is no credentials
			// required, the connection will be bound to the user's account.
			if iter == 1 {
				// Should not receive on "ACC1", but should on "ACC2"
				if _, err := sub1.NextMsg(100 * time.Millisecond); err != nats.ErrTimeout {
					t.Fatalf("Expected timeout error, got %v", err)
				}
				natsNexMsg(t, sub2, time.Second)
			} else {
				// Should receive on "ACC1"...
				natsNexMsg(t, sub1, time.Second)
				// but not received on "ACC2" since leafnode bound to account "ACC1".
				if _, err := sub2.NextMsg(100 * time.Millisecond); err != nats.ErrTimeout {
					t.Fatalf("Expected timeout error, got %v", err)
				}
			}
		})
	}
}

func TestLeafNodeBasicAuthMultiple(t *testing.T) {
	conf := createConfFile(t, []byte(`
		port: -1
		accounts: {
			S1ACC1: { users = [{user: "user1", password: "user1"}] }
			S1ACC2: { users = [{user: "user2", password: "user2"}] }
		}
		leafnodes: {
			port: -1
			authorization {
			  users = [
				  {user: "ln1", password: "ln1", account: "S1ACC1"}
				  {user: "ln2", password: "ln2", account: "S1ACC2"}
				  {user: "ln3", password: "ln3"}
			  ]
            }
		}
	`))
	s1, o1 := RunServerWithConfig(conf)
	defer s1.Shutdown()

	// Make sure that we reject a LN connection if user does not match
	conf = createConfFile(t, []byte(fmt.Sprintf(`
		port: -1
		leafnodes: {
			remotes = [{url: "nats-leaf://wron:user@%s:%d"}]
		}
	`, o1.LeafNode.Host, o1.LeafNode.Port)))
	s2, _ := RunServerWithConfig(conf)
	defer s2.Shutdown()
	// Give a chance for s2 to attempt to connect and make sure that s1
	// did not register a LN connection.
	time.Sleep(100 * time.Millisecond)
	if n := s1.NumLeafNodes(); n != 0 {
		t.Fatalf("Expected no leafnode connection, got %v", n)
	}
	s2.Shutdown()

	ncACC1 := natsConnect(t, fmt.Sprintf("nats://user1:user1@%s:%d", o1.Host, o1.Port))
	defer ncACC1.Close()
	sub1 := natsSubSync(t, ncACC1, "foo")
	natsFlush(t, ncACC1)

	ncACC2 := natsConnect(t, fmt.Sprintf("nats://user2:user2@%s:%d", o1.Host, o1.Port))
	defer ncACC2.Close()
	sub2 := natsSubSync(t, ncACC2, "foo")
	natsFlush(t, ncACC2)

	// We will start s2 with 2 LN connections that should bind local account S2ACC1
	// to account S1ACC1 and S2ACC2 to account S1ACC2 on s1.
	conf = createConfFile(t, []byte(fmt.Sprintf(`
		port: -1
		accounts {
			S2ACC1 { users = [{user: "user1", password: "user1"}] }
			S2ACC2 { users = [{user: "user2", password: "user2"}] }
		}
		leafnodes: {
			remotes = [
				{
					url: "nats-leaf://ln1:ln1@%s:%d"
					account: "S2ACC1"
				}
				{
					url: "nats-leaf://ln2:ln2@%s:%d"
					account: "S2ACC2"
				}
			]
		}
	`, o1.LeafNode.Host, o1.LeafNode.Port, o1.LeafNode.Host, o1.LeafNode.Port)))
	s2, o2 := RunServerWithConfig(conf)
	defer s2.Shutdown()

	checkFor(t, 5*time.Second, 100*time.Millisecond, func() error {
		if nln := s2.NumLeafNodes(); nln != 2 {
			return fmt.Errorf("Expected 2 connected leafnodes for server %q, got %d", s2.ID(), nln)
		}
		return nil
	})

	// Create a user connection on s2 that binds to S2ACC1 (use user1).
	nc1 := natsConnect(t, fmt.Sprintf("nats://user1:user1@%s:%d", o2.Host, o2.Port))
	defer nc1.Close()

	// Create an user connection on s2 that binds to S2ACC2 (use user2).
	nc2 := natsConnect(t, fmt.Sprintf("nats://user2:user2@%s:%d", o2.Host, o2.Port))
	defer nc2.Close()

	// Now if a message is published from nc1, sub1 should receive it since
	// their account are bound together.
	natsPub(t, nc1, "foo", []byte("hello"))
	natsNexMsg(t, sub1, time.Second)
	// But sub2 should not receive it since different account.
	if _, err := sub2.NextMsg(100 * time.Millisecond); err != nats.ErrTimeout {
		t.Fatalf("Expected timeout error, got %v", err)
	}

	// Now use nc2 (S2ACC2) to publish
	natsPub(t, nc2, "foo", []byte("hello"))
	// Expect sub2 to receive and sub1 not to.
	natsNexMsg(t, sub2, time.Second)
	if _, err := sub1.NextMsg(100 * time.Millisecond); err != nats.ErrTimeout {
		t.Fatalf("Expected timeout error, got %v", err)
	}

	// Now check that we don't panic if no account is specified for
	// a given user.
	conf = createConfFile(t, []byte(fmt.Sprintf(`
		port: -1
		leafnodes: {
			remotes = [
				{ url: "nats-leaf://ln3:ln3@%s:%d" }
			]
		}
	`, o1.LeafNode.Host, o1.LeafNode.Port)))
	s3, _ := RunServerWithConfig(conf)
	defer s3.Shutdown()
}

type loopDetectedLogger struct {
	DummyLogger
	ch chan string
}

func (l *loopDetectedLogger) Errorf(format string, v ...any) {
	msg := fmt.Sprintf(format, v...)
	if strings.Contains(msg, "Loop") {
		select {
		case l.ch <- msg:
		default:
		}
	}
}

func TestLeafNodeLoop(t *testing.T) {
	test := func(t *testing.T, cluster bool) {
		// This test requires that we set the port to known value because
		// we want A point to B and B to A.
		oa := DefaultOptions()
		oa.ServerName = "A"
		if !cluster {
			oa.Cluster.Port = 0
			oa.Cluster.Name = _EMPTY_
		}
		oa.LeafNode.ReconnectInterval = 10 * time.Millisecond
		oa.LeafNode.Port = 1234
		ub, _ := url.Parse("nats://127.0.0.1:5678")
		oa.LeafNode.Remotes = []*RemoteLeafOpts{{URLs: []*url.URL{ub}}}
		oa.LeafNode.connDelay = 50 * time.Millisecond
		sa := RunServer(oa)
		defer sa.Shutdown()

		la := &loopDetectedLogger{ch: make(chan string, 1)}
		sa.SetLogger(la, false, false)

		ob := DefaultOptions()
		ob.ServerName = "B"
		if !cluster {
			ob.Cluster.Port = 0
			ob.Cluster.Name = _EMPTY_
		} else {
			ob.Cluster.Name = "xyz"
		}
		ob.LeafNode.ReconnectInterval = 10 * time.Millisecond
		ob.LeafNode.Port = 5678
		ua, _ := url.Parse("nats://127.0.0.1:1234")
		ob.LeafNode.Remotes = []*RemoteLeafOpts{{URLs: []*url.URL{ua}}}
		ob.LeafNode.connDelay = 50 * time.Millisecond
		sb := RunServer(ob)
		defer sb.Shutdown()

		lb := &loopDetectedLogger{ch: make(chan string, 1)}
		sb.SetLogger(lb, false, false)

		select {
		case <-la.ch:
			// OK!
		case <-lb.ch:
			// OK!
		case <-time.After(5 * time.Second):
			t.Fatalf("Did not get any error regarding loop")
		}

		sb.Shutdown()
		ob.Port = -1
		ob.Cluster.Port = -1
		ob.LeafNode.Remotes = nil
		sb = RunServer(ob)
		defer sb.Shutdown()

		checkLeafNodeConnected(t, sa)
	}
	t.Run("standalone", func(t *testing.T) { test(t, false) })
	t.Run("cluster", func(t *testing.T) { test(t, true) })
}

func TestLeafNodeLoopFromDAG(t *testing.T) {
	// We want B & C to point to A, A itself does not point to any other server.
	// We need to cancel clustering since now this will suppress on its own.
	oa := DefaultOptions()
	oa.ServerName = "A"
	oa.LeafNode.connDelay = 50 * time.Millisecond
	oa.LeafNode.ReconnectInterval = 10 * time.Millisecond
	oa.LeafNode.Port = -1
	oa.Cluster = ClusterOpts{}
	sa := RunServer(oa)
	defer sa.Shutdown()

	ua, _ := url.Parse(fmt.Sprintf("nats://127.0.0.1:%d", oa.LeafNode.Port))

	// B will point to A
	ob := DefaultOptions()
	ob.ServerName = "B"
	ob.LeafNode.connDelay = 50 * time.Millisecond
	ob.LeafNode.ReconnectInterval = 10 * time.Millisecond
	ob.LeafNode.Port = -1
	ob.LeafNode.Remotes = []*RemoteLeafOpts{{URLs: []*url.URL{ua}}}
	ob.Cluster = ClusterOpts{}
	sb := RunServer(ob)
	defer sb.Shutdown()

	ub, _ := url.Parse(fmt.Sprintf("nats://127.0.0.1:%d", ob.LeafNode.Port))

	checkLeafNodeConnected(t, sa)
	checkLeafNodeConnected(t, sb)

	// C will point to A and B
	oc := DefaultOptions()
	oc.ServerName = "C"
	oc.LeafNode.connDelay = 50 * time.Millisecond
	oc.LeafNode.ReconnectInterval = 10 * time.Millisecond
	oc.LeafNode.Remotes = []*RemoteLeafOpts{{URLs: []*url.URL{ua}}, {URLs: []*url.URL{ub}}}
	oc.LeafNode.connDelay = 100 * time.Millisecond // Allow logger to be attached before connecting.
	oc.Cluster = ClusterOpts{}
	sc := RunServer(oc)

	lc := &loopDetectedLogger{ch: make(chan string, 1)}
	sc.SetLogger(lc, false, false)

	// We should get an error.
	select {
	case <-lc.ch:
		// OK
	case <-time.After(2 * time.Second):
		t.Fatalf("Did not get any error regarding loop")
	}

	// C should not be connected to anything.
	checkLeafNodeConnectedCount(t, sc, 0)
	// A and B are connected to each other.
	checkLeafNodeConnectedCount(t, sa, 1)
	checkLeafNodeConnectedCount(t, sb, 1)

	// Shutdown C and restart without the loop.
	sc.Shutdown()
	oc.LeafNode.Remotes = []*RemoteLeafOpts{{URLs: []*url.URL{ub}}}

	sc = RunServer(oc)
	defer sc.Shutdown()

	checkLeafNodeConnectedCount(t, sa, 1)
	checkLeafNodeConnectedCount(t, sb, 2)
	checkLeafNodeConnectedCount(t, sc, 1)
}

func TestLeafNodeCloseTLSConnection(t *testing.T) {
	opts := DefaultOptions()
	opts.DisableShortFirstPing = true
	opts.LeafNode.Host = "127.0.0.1"
	opts.LeafNode.Port = -1
	opts.LeafNode.TLSTimeout = 100
	tc := &TLSConfigOpts{
		CertFile: "./configs/certs/server.pem",
		KeyFile:  "./configs/certs/key.pem",
		Insecure: true,
	}
	tlsConf, err := GenTLSConfig(tc)
	if err != nil {
		t.Fatalf("Error generating tls config: %v", err)
	}
	opts.LeafNode.TLSConfig = tlsConf
	opts.NoLog = true
	opts.NoSigs = true
	s := RunServer(opts)
	defer s.Shutdown()

	endpoint := net.JoinHostPort(opts.LeafNode.Host, fmt.Sprintf("%d", opts.LeafNode.Port))
	conn, err := net.DialTimeout("tcp", endpoint, 2*time.Second)
	if err != nil {
		t.Fatalf("Unexpected error on dial: %v", err)
	}
	defer conn.Close()

	br := bufio.NewReaderSize(conn, 100)
	if _, err := br.ReadString('\n'); err != nil {
		t.Fatalf("Unexpected error reading INFO: %v", err)
	}

	tlsConn := tls.Client(conn, &tls.Config{InsecureSkipVerify: true})
	defer tlsConn.Close()
	if err := tlsConn.Handshake(); err != nil {
		t.Fatalf("Unexpected error during handshake: %v", err)
	}
	connectOp := []byte("CONNECT {\"name\":\"leaf\",\"verbose\":false,\"pedantic\":false}\r\n")
	if _, err := tlsConn.Write(connectOp); err != nil {
		t.Fatalf("Unexpected error writing CONNECT: %v", err)
	}
	if _, err := tlsConn.Write([]byte("PING\r\n")); err != nil {
		t.Fatalf("Unexpected error writing PING: %v", err)
	}

	checkLeafNodeConnected(t, s)

	// Get leaf connection
	var leaf *client
	s.mu.Lock()
	for _, l := range s.leafs {
		leaf = l
		break
	}
	s.mu.Unlock()
	// Fill the buffer. We want to timeout on write so that nc.Close()
	// would block due to a write that cannot complete.
	buf := make([]byte, 64*1024)
	done := false
	for !done {
		leaf.nc.SetWriteDeadline(time.Now().Add(time.Second))
		if _, err := leaf.nc.Write(buf); err != nil {
			done = true
		}
		leaf.nc.SetWriteDeadline(time.Time{})
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
	// Close the route
	leaf.closeConnection(SlowConsumerWriteDeadline)
	ch <- true
}

func TestLeafNodeTLSSaveName(t *testing.T) {
	opts := DefaultOptions()
	opts.LeafNode.Host = "127.0.0.1"
	opts.LeafNode.Port = -1
	tc := &TLSConfigOpts{
		CertFile: "../test/configs/certs/server-noip.pem",
		KeyFile:  "../test/configs/certs/server-key-noip.pem",
		Insecure: true,
	}
	tlsConf, err := GenTLSConfig(tc)
	if err != nil {
		t.Fatalf("Error generating tls config: %v", err)
	}
	opts.LeafNode.TLSConfig = tlsConf
	s := RunServer(opts)
	defer s.Shutdown()

	lo := DefaultOptions()
	u, _ := url.Parse(fmt.Sprintf("nats://localhost:%d", opts.LeafNode.Port))
	lo.LeafNode.Remotes = []*RemoteLeafOpts{{URLs: []*url.URL{u}}}
	lo.LeafNode.ReconnectInterval = 15 * time.Millisecond
	ln := RunServer(lo)
	defer ln.Shutdown()

	// We know connection will fail, but it should not fail because of error such as:
	// "cannot validate certificate for 127.0.0.1 because it doesn't contain any IP SANs"
	// This would mean that we are not saving the hostname to use during the TLS handshake.

	le := &captureErrorLogger{errCh: make(chan string, 100)}
	ln.SetLogger(le, false, false)

	tm := time.NewTimer(time.Second)
	var done bool
	for !done {
		select {
		case err := <-le.errCh:
			if strings.Contains(err, "doesn't contain any IP SANs") {
				t.Fatalf("Got this error: %q", err)
			}
		case <-tm.C:
			done = true
		}
	}
}

func TestLeafNodeRemoteWrongPort(t *testing.T) {
	for _, test1 := range []struct {
		name              string
		clusterAdvertise  bool
		leafnodeAdvertise bool
	}{
		{"advertise_on", false, false},
		{"cluster_no_advertise", true, false},
		{"leafnode_no_advertise", false, true},
	} {
		t.Run(test1.name, func(t *testing.T) {
			oa := DefaultOptions()
			// Make sure we have all ports (client, route, gateway) and we will try
			// to create a leafnode to connection to each and make sure we get the error.
			oa.Cluster.NoAdvertise = test1.clusterAdvertise
			oa.Cluster.Name = "A"
			oa.Cluster.Host = "127.0.0.1"
			oa.Cluster.Port = -1
			oa.Gateway.Host = "127.0.0.1"
			oa.Gateway.Port = -1
			oa.Gateway.Name = "A"
			oa.LeafNode.Host = "127.0.0.1"
			oa.LeafNode.Port = -1
			oa.LeafNode.NoAdvertise = test1.leafnodeAdvertise
			oa.Accounts = []*Account{NewAccount("sys")}
			oa.SystemAccount = "sys"
			sa := RunServer(oa)
			defer sa.Shutdown()

			ob := DefaultOptions()
			ob.Cluster.NoAdvertise = test1.clusterAdvertise
			ob.Cluster.Name = "A"
			ob.Cluster.Host = "127.0.0.1"
			ob.Cluster.Port = -1
			ob.Routes = RoutesFromStr(fmt.Sprintf("nats://%s:%d", oa.Cluster.Host, oa.Cluster.Port))
			ob.Gateway.Host = "127.0.0.1"
			ob.Gateway.Port = -1
			ob.Gateway.Name = "A"
			ob.LeafNode.Host = "127.0.0.1"
			ob.LeafNode.Port = -1
			ob.LeafNode.NoAdvertise = test1.leafnodeAdvertise
			ob.Accounts = []*Account{NewAccount("sys")}
			ob.SystemAccount = "sys"
			sb := RunServer(ob)
			defer sb.Shutdown()

			checkClusterFormed(t, sa, sb)

			for _, test := range []struct {
				name string
				port int
			}{
				{"client", oa.Port},
				{"cluster", oa.Cluster.Port},
				{"gateway", oa.Gateway.Port},
			} {
				t.Run(test.name, func(t *testing.T) {
					oc := DefaultOptions()
					// Server with the wrong config against non leafnode port.
					leafURL, _ := url.Parse(fmt.Sprintf("nats://127.0.0.1:%d", test.port))
					oc.LeafNode.Remotes = []*RemoteLeafOpts{{URLs: []*url.URL{leafURL}}}
					oc.LeafNode.ReconnectInterval = 5 * time.Millisecond
					sc := RunServer(oc)
					defer sc.Shutdown()
					l := &captureErrorLogger{errCh: make(chan string, 10)}
					sc.SetLogger(l, true, true)

					select {
					case e := <-l.errCh:
						if strings.Contains(e, ErrConnectedToWrongPort.Error()) {
							return
						}
					case <-time.After(2 * time.Second):
						t.Fatalf("Did not get any error about connecting to wrong port for %q - %q",
							test1.name, test.name)
					}
				})
			}
		})
	}
}

func TestLeafNodeNoAdvertiseDoesNotAdvertiseListener(t *testing.T) {
	hubConf := createConfFile(t, []byte(`
		listen: "127.0.0.1:-1"
		leafnodes {
			listen: "127.0.0.1:-1"
			no_advertise: true
		}
	`))
	hub, hubOpts := RunServerWithConfig(hubConf)
	defer hub.Shutdown()

	leafConf := createConfFile(t, []byte(fmt.Sprintf(`
		listen: "127.0.0.1:-1"
		leafnodes {
			reconnect: "50ms"
			remotes: [
				{
					url: "nats://localhost:%d"
				}
			]
		}
	`, hubOpts.LeafNode.Port)))
	leaf, _ := RunServerWithConfig(leafConf)
	defer leaf.Shutdown()

	checkLeafNodeConnected(t, hub)
	checkLeafNodeConnected(t, leaf)

	configURL := fmt.Sprintf("nats://localhost:%d", hubOpts.LeafNode.Port)

	checkFor(t, time.Second, 10*time.Millisecond, func() error {
		leaf.mu.RLock()
		defer leaf.mu.RUnlock()
		var remote *leafNodeCfg
		for cfg := range leaf.leafRemoteCfgs {
			remote = cfg
			break
		}
		if remote == nil {
			return fmt.Errorf("expected a leaf remote config")
		}
		remote.RLock()
		defer remote.RUnlock()
		if got, want := len(remote.urls), 1; got != want {
			return fmt.Errorf("expected %d reconnect URL, got %d: %v", want, got, remote.urls)
		}
		if got := remote.urls[0].String(); got != configURL {
			return fmt.Errorf("expected reconnect URL %q, got %q", configURL, got)
		}
		return nil
	})
}

func TestLeafNodeRemoteIsHub(t *testing.T) {
	oa := testDefaultOptionsForGateway("A")
	oa.Accounts = []*Account{NewAccount("sys")}
	oa.SystemAccount = "sys"
	sa := RunServer(oa)
	defer sa.Shutdown()

	lno := DefaultOptions()
	lno.LeafNode.Host = "127.0.0.1"
	lno.LeafNode.Port = -1
	ln := RunServer(lno)
	defer ln.Shutdown()

	ob1 := testGatewayOptionsFromToWithServers(t, "B", "A", sa)
	ob1.Accounts = []*Account{NewAccount("sys")}
	ob1.SystemAccount = "sys"
	ob1.Cluster.Host = "127.0.0.1"
	ob1.Cluster.Port = -1
	ob1.LeafNode.Host = "127.0.0.1"
	ob1.LeafNode.Port = -1
	u, _ := url.Parse(fmt.Sprintf("nats://127.0.0.1:%d", lno.LeafNode.Port))
	ob1.LeafNode.Remotes = []*RemoteLeafOpts{
		{
			URLs: []*url.URL{u},
			Hub:  true,
		},
	}
	sb1 := RunServer(ob1)
	defer sb1.Shutdown()

	waitForOutboundGateways(t, sb1, 1, 2*time.Second)
	waitForInboundGateways(t, sb1, 1, 2*time.Second)
	waitForOutboundGateways(t, sa, 1, 2*time.Second)
	waitForInboundGateways(t, sa, 1, 2*time.Second)

	checkLeafNodeConnected(t, sb1)

	// For now, due to issue 977, let's restart the leafnode so that the
	// leafnode connect is propagated in the super-cluster.
	ln.Shutdown()
	ln = RunServer(lno)
	defer ln.Shutdown()
	checkLeafNodeConnected(t, sb1)

	// Connect another server in cluster B
	ob2 := testGatewayOptionsFromToWithServers(t, "B", "A", sa)
	ob2.Accounts = []*Account{NewAccount("sys")}
	ob2.SystemAccount = "sys"
	ob2.Cluster.Host = "127.0.0.1"
	ob2.Cluster.Port = -1
	ob2.Routes = RoutesFromStr(fmt.Sprintf("nats://127.0.0.1:%d", ob1.Cluster.Port))
	sb2 := RunServer(ob2)
	defer sb2.Shutdown()

	checkClusterFormed(t, sb1, sb2)
	waitForOutboundGateways(t, sb2, 1, 2*time.Second)

	expectedSubs := ln.NumSubscriptions() + 2

	// Create sub on "foo" connected to sa
	ncA := natsConnect(t, sa.ClientURL())
	defer ncA.Close()
	subFoo := natsSubSync(t, ncA, "foo")

	// Create sub on "bar" connected to sb2
	ncB2 := natsConnect(t, sb2.ClientURL())
	defer ncB2.Close()
	subBar := natsSubSync(t, ncB2, "bar")

	// Make sure subscriptions have propagated to the leafnode.
	checkFor(t, time.Second, 10*time.Millisecond, func() error {
		if subs := ln.NumSubscriptions(); subs < expectedSubs {
			return fmt.Errorf("Number of subs is %d", subs)
		}
		return nil
	})

	// Create pub connection on leafnode
	ncLN := natsConnect(t, ln.ClientURL())
	defer ncLN.Close()

	// Publish on foo and make sure it is received.
	natsPub(t, ncLN, "foo", []byte("msg"))
	natsNexMsg(t, subFoo, time.Second)

	// Publish on foo and make sure it is received.
	natsPub(t, ncLN, "bar", []byte("msg"))
	natsNexMsg(t, subBar, time.Second)
}

func TestLeafNodePermissions(t *testing.T) {
	lo1 := DefaultOptions()
	lo1.LeafNode.Host = "127.0.0.1"
	lo1.LeafNode.Port = -1
	ln1 := RunServer(lo1)
	defer ln1.Shutdown()

	errLog := &captureErrorLogger{errCh: make(chan string, 1)}
	ln1.SetLogger(errLog, false, false)

	u, _ := url.Parse(fmt.Sprintf("nats://%s:%d", lo1.LeafNode.Host, lo1.LeafNode.Port))
	lo2 := DefaultOptions()
	lo2.Cluster.Name = "xyz"
	lo2.LeafNode.ReconnectInterval = 5 * time.Millisecond
	lo2.LeafNode.connDelay = 100 * time.Millisecond
	lo2.LeafNode.Remotes = []*RemoteLeafOpts{
		{
			URLs:        []*url.URL{u},
			DenyExports: []string{"export.*", "export"},
			DenyImports: []string{"import.*", "import"},
		},
	}
	ln2 := RunServer(lo2)
	defer ln2.Shutdown()

	checkLeafNodeConnected(t, ln1)

	// Create clients on ln1 and ln2
	nc1, err := nats.Connect(ln1.ClientURL())
	if err != nil {
		t.Fatalf("Error creating client: %v", err)
	}
	defer nc1.Close()
	nc2, err := nats.Connect(ln2.ClientURL())
	if err != nil {
		t.Fatalf("Error creating client: %v", err)
	}
	defer nc2.Close()

	checkSubs := func(acc *Account, expected int) {
		t.Helper()
		checkFor(t, time.Second, 15*time.Millisecond, func() error {
			if n := acc.TotalSubs(); n != expected {
				return fmt.Errorf("Expected %d subs, got %v", expected, n)
			}
			return nil
		})
	}

	// Create a sub on ">" on LN1
	subAll := natsSubSync(t, nc1, ">")
	// this should be registered in LN2 (there is 1 sub for LN1 $LDS subject) + SYS IMPORTS
	checkSubs(ln2.globalAccount(), 12)

	// Check deny export clause from messages published from LN2
	for _, test := range []struct {
		name     string
		subject  string
		received bool
	}{
		{"do not send on export.bat", "export.bat", false},
		{"do not send on export", "export", false},
		{"send on foo", "foo", true},
		{"send on export.this.one", "export.this.one", true},
	} {
		t.Run(test.name, func(t *testing.T) {
			nc2.Publish(test.subject, []byte("msg"))
			if test.received {
				natsNexMsg(t, subAll, time.Second)
			} else {
				if _, err := subAll.NextMsg(50 * time.Millisecond); err == nil {
					t.Fatalf("Should not have received message on %q", test.subject)
				}
			}
		})
	}

	subAll.Unsubscribe()
	// Goes down by 1.
	checkSubs(ln2.globalAccount(), 11)

	// We used to make sure we would not do subscriptions however that
	// was incorrect. We need to check publishes, not the subscriptions.
	// For instance if we can publish across a leafnode to foo, and the
	// other side has a subsxcription for '*' the message should cross
	// the leafnode. The old way would not allow this.

	// Now check deny import clause.
	// As of now, we don't suppress forwarding of subscriptions on LN2 that
	// match the deny import clause to be forwarded to LN1. However, messages
	// should still not be able to come back to LN2.
	for _, test := range []struct {
		name       string
		subSubject string
		pubSubject string
		ok         bool
	}{
		{"reject import on import.*", "import.*", "import.bad", false},
		{"reject import on import", "import", "import", false},
		{"accepts import on foo", "foo", "foo", true},
		{"accepts import on import.this.one.ok", "import.*.>", "import.this.one.ok", true},
	} {
		t.Run(test.name, func(t *testing.T) {
			sub := natsSubSync(t, nc2, test.subSubject)
			checkSubs(ln2.globalAccount(), 12)

			if !test.ok {
				nc1.Publish(test.pubSubject, []byte("msg"))
				if _, err := sub.NextMsg(50 * time.Millisecond); err == nil {
					t.Fatalf("Did not expect to get the message")
				}
			} else {
				checkSubs(ln1.globalAccount(), 11)
				nc1.Publish(test.pubSubject, []byte("msg"))
				natsNexMsg(t, sub, time.Second)
			}
			sub.Unsubscribe()
			checkSubs(ln1.globalAccount(), 10)
		})
	}
}

func TestLeafNodePermissionsConcurrentAccess(t *testing.T) {
	lo1 := DefaultOptions()
	lo1.LeafNode.Host = "127.0.0.1"
	lo1.LeafNode.Port = -1
	ln1 := RunServer(lo1)
	defer ln1.Shutdown()

	nc1 := natsConnect(t, ln1.ClientURL())
	defer nc1.Close()

	natsSub(t, nc1, "_INBOX.>", func(_ *nats.Msg) {})
	natsFlush(t, nc1)

	ch := make(chan struct{}, 1)
	wg := sync.WaitGroup{}
	wg.Add(2)

	publish := func(nc *nats.Conn) {
		defer wg.Done()

		for {
			select {
			case <-ch:
				return
			default:
				nc.Publish(nats.NewInbox(), []byte("hello"))
			}
		}
	}

	go publish(nc1)

	u, _ := url.Parse(fmt.Sprintf("nats://%s:%d", lo1.LeafNode.Host, lo1.LeafNode.Port))
	lo2 := DefaultOptions()
	lo2.Cluster.Name = "xyz"
	lo2.LeafNode.ReconnectInterval = 5 * time.Millisecond
	lo2.LeafNode.connDelay = 500 * time.Millisecond
	lo2.LeafNode.Remotes = []*RemoteLeafOpts{
		{
			URLs:        []*url.URL{u},
			DenyExports: []string{"foo"},
			DenyImports: []string{"bar"},
		},
	}
	ln2 := RunServer(lo2)
	defer ln2.Shutdown()

	nc2 := natsConnect(t, ln2.ClientURL())
	defer nc2.Close()

	natsSub(t, nc2, "_INBOX.>", func(_ *nats.Msg) {})
	natsFlush(t, nc2)

	go publish(nc2)

	checkLeafNodeConnected(t, ln1)
	checkLeafNodeConnected(t, ln2)

	time.Sleep(50 * time.Millisecond)
	close(ch)
	wg.Wait()
}

func TestLeafNodeInfoPermissionUpdatesRequireSolicitedLeaf(t *testing.T) {
	s := &Server{}
	s.setOpts(DefaultOptions())
	cli, srv := net.Pipe()
	defer cli.Close()
	defer srv.Close()

	c := &client{
		kind: LEAF,
		srv:  s,
		nc:   cli,
		leaf: &leaf{},
	}
	c.flags.set(infoReceived)
	c.flags.set(compressionNegotiated)
	c.flags.set(handshakeComplete)

	c.processLeafnodeInfo(&Info{
		Import: &SubjectPermission{Allow: []string{"import.>"}},
		Export: &SubjectPermission{Allow: []string{"export.>"}},
	})

	require_Equal(t, c.perms, (*permissions)(nil))
}

func TestLeafNodeInfoPermissionUpdatesAllowedForSolicitedLeaf(t *testing.T) {
	s := &Server{}
	s.setOpts(DefaultOptions())
	cli, srv := net.Pipe()
	defer cli.Close()
	defer srv.Close()

	c := &client{
		kind: LEAF,
		srv:  s,
		nc:   cli,
		leaf: &leaf{
			remote: &leafNodeCfg{RemoteLeafOpts: &RemoteLeafOpts{}},
		},
	}
	c.flags.set(infoReceived)
	c.flags.set(compressionNegotiated)
	c.flags.set(handshakeComplete)

	c.processLeafnodeInfo(&Info{
		Import: &SubjectPermission{Allow: []string{"import.>"}},
		Export: &SubjectPermission{Allow: []string{"export.>"}},
	})

	require_True(t, c.perms != nil)
	require_True(t, c.pubAllowed("export.foo"))
	require_True(t, c.canSubscribe("import.foo"))
	require_False(t, c.pubAllowed("other.foo"))
	require_False(t, c.canSubscribe("other.foo"))
}

func TestLeafNodeInfoOnConnectWithoutLeafNodeURLsAccepted(t *testing.T) {
	s := &Server{}
	s.setOpts(DefaultOptions())
	cli, srv := net.Pipe()
	defer cli.Close()
	defer srv.Close()

	c := &client{
		kind: LEAF,
		srv:  s,
		nc:   cli,
		acc:  NewAccount("TEST"),
		leaf: &leaf{
			remote: func() *leafNodeCfg {
				cfg := newLeafNodeCfg(&RemoteLeafOpts{})
				cfg.curURL = &url.URL{}
				return cfg
			}(),
		},
	}
	c.initClient()
	c.flags.set(handshakeComplete)

	c.processLeafnodeInfo(&Info{
		ID:            "hub",
		Name:          "hub",
		CID:           1,
		InfoOnConnect: true,
		Nonce:         "nonce",
	})

	require_False(t, c.isClosed())
	require_True(t, c.flags.isSet(infoReceived))
	require_Equal(t, "hub", c.leaf.remoteServer)
}

func TestLeafNodePubAllowedPruning(t *testing.T) {
	c := &client{}
	c.setPermissions(&Permissions{Publish: &SubjectPermission{Allow: []string{"foo"}}})

	gr := 100
	wg := sync.WaitGroup{}
	wg.Add(gr)
	for i := 0; i < gr; i++ {
		go func() {
			defer wg.Done()
			for j := 0; j < 100; j++ {
				c.pubAllowed(nats.NewInbox())
			}
		}()
	}

	wg.Wait()
	// The cache prune function does try for a bit to make sure the cache
	// is below the maxPermCacheSize value, but depending on the machine
	// this runs on, it may be that it is still a bit over. If so, run
	// pubAllowed one more time and we must get below.
	if n := int(atomic.LoadInt32(&c.perms.pcsz)); n > maxPermCacheSize {
		c.pubAllowed(nats.NewInbox())
	}
	if n := int(atomic.LoadInt32(&c.perms.pcsz)); n > maxPermCacheSize {
		t.Fatalf("Expected size to be less than %v, got %v", maxPermCacheSize, n)
	}
	if n := atomic.LoadInt32(&c.perms.prun); n != 0 {
		t.Fatalf("c.perms.prun should be 0, was %v", n)
	}
}

func TestLeafNodeExportPermissionsNotForSpecialSubs(t *testing.T) {
	lo1 := DefaultOptions()
	lo1.Accounts = []*Account{NewAccount("SYS")}
	lo1.SystemAccount = "SYS"
	lo1.Cluster.Name = "A"
	lo1.Gateway.Name = "A"
	lo1.Gateway.Port = -1
	lo1.LeafNode.Host = "127.0.0.1"
	lo1.LeafNode.Port = -1
	ln1 := RunServer(lo1)
	defer ln1.Shutdown()

	u, _ := url.Parse(fmt.Sprintf("nats://%s:%d", lo1.LeafNode.Host, lo1.LeafNode.Port))
	lo2 := DefaultOptions()
	lo2.Cluster.Name = "LN"
	lo2.LeafNode.Remotes = []*RemoteLeafOpts{
		{
			URLs:        []*url.URL{u},
			DenyExports: []string{">"},
		},
	}
	ln2 := RunServer(lo2)
	defer ln2.Shutdown()

	checkLeafNodeConnected(t, ln1)

	// The deny is totally restrictive, but make sure that we still accept the $LDS and _GR_ go from LN1.
	checkFor(t, time.Second, 15*time.Millisecond, func() error {
		// We should have registered the 2 subs from the accepting leafnode.
		if n := ln2.globalAccount().TotalSubs(); n != 8 {
			return fmt.Errorf("Expected %d subs, got %v", 8, n)
		}
		return nil
	})
}

// Make sure that if the node that detects the loop (and sends the error and
// close the connection) is the accept side, the remote node (the one that solicits)
// properly use the reconnect delay.
func TestLeafNodeLoopDetectedOnAcceptSide(t *testing.T) {
	bo := DefaultOptions()
	bo.LeafNode.Host = "127.0.0.1"
	bo.LeafNode.Port = -1
	b := RunServer(bo)
	defer b.Shutdown()

	l := &loopDetectedLogger{ch: make(chan string, 1)}
	b.SetLogger(l, false, false)

	u, _ := url.Parse(fmt.Sprintf("nats://127.0.0.1:%d", bo.LeafNode.Port))

	ao := testDefaultOptionsForGateway("A")
	ao.Accounts = []*Account{NewAccount("SYS")}
	ao.SystemAccount = "SYS"
	ao.LeafNode.ReconnectInterval = 5 * time.Millisecond
	ao.LeafNode.Remotes = []*RemoteLeafOpts{
		{
			URLs: []*url.URL{u},
			Hub:  true,
		},
	}
	a := RunServer(ao)
	defer a.Shutdown()

	co := testGatewayOptionsFromToWithServers(t, "C", "A", a)
	co.Accounts = []*Account{NewAccount("SYS")}
	co.SystemAccount = "SYS"
	co.LeafNode.ReconnectInterval = 5 * time.Millisecond
	co.LeafNode.Remotes = []*RemoteLeafOpts{
		{
			URLs: []*url.URL{u},
			Hub:  true,
		},
	}
	c := RunServer(co)
	defer c.Shutdown()

	for i := 0; i < 2; i++ {
		select {
		case <-l.ch:
			// OK
		case <-time.After(200 * time.Millisecond):
			// We are likely to detect from each A and C servers,
			// but consider a failure if we did not receive any.
			if i == 0 {
				t.Fatalf("Should have detected loop")
			}
		}
	}

	// The reconnect attempt is set to 5ms, but the default loop delay
	// is 30 seconds, so we should not get any new error for that long.
	// Check if we are getting more errors..
	select {
	case e := <-l.ch:
		t.Fatalf("Should not have gotten another error, got %q", e)
	case <-time.After(50 * time.Millisecond):
		// OK!
	}
}

func TestLeafNodeHubWithGateways(t *testing.T) {
	ao := DefaultOptions()
	ao.ServerName = "A"
	ao.LeafNode.Host = "127.0.0.1"
	ao.LeafNode.Port = -1
	a := RunServer(ao)
	defer a.Shutdown()

	ua, _ := url.Parse(fmt.Sprintf("nats://127.0.0.1:%d", ao.LeafNode.Port))

	bo := testDefaultOptionsForGateway("B")
	bo.ServerName = "B"
	bo.Accounts = []*Account{NewAccount("SYS")}
	bo.SystemAccount = "SYS"
	bo.LeafNode.ReconnectInterval = 5 * time.Millisecond
	bo.LeafNode.Remotes = []*RemoteLeafOpts{
		{
			URLs: []*url.URL{ua},
			Hub:  true,
		},
	}
	b := RunServer(bo)
	defer b.Shutdown()

	do := DefaultOptions()
	do.ServerName = "D"
	do.LeafNode.Host = "127.0.0.1"
	do.LeafNode.Port = -1
	d := RunServer(do)
	defer d.Shutdown()

	ud, _ := url.Parse(fmt.Sprintf("nats://127.0.0.1:%d", do.LeafNode.Port))

	co := testGatewayOptionsFromToWithServers(t, "C", "B", b)
	co.ServerName = "C"
	co.Accounts = []*Account{NewAccount("SYS")}
	co.SystemAccount = "SYS"
	co.LeafNode.ReconnectInterval = 5 * time.Millisecond
	co.LeafNode.Remotes = []*RemoteLeafOpts{
		{
			URLs: []*url.URL{ud},
			Hub:  true,
		},
	}
	c := RunServer(co)
	defer c.Shutdown()

	waitForInboundGateways(t, b, 1, 2*time.Second)
	waitForInboundGateways(t, c, 1, 2*time.Second)
	checkLeafNodeConnected(t, a)
	checkLeafNodeConnected(t, d)

	// Create a responder on D
	ncD := natsConnect(t, d.ClientURL())
	defer ncD.Close()

	ncD.Subscribe("service", func(m *nats.Msg) {
		m.Respond([]byte("reply"))
	})
	ncD.Flush()

	checkFor(t, time.Second, 15*time.Millisecond, func() error {
		acc := a.globalAccount()
		if r := acc.sl.Match("service"); r != nil && len(r.psubs) == 1 {
			return nil
		}
		return fmt.Errorf("subscription still not registered")
	})

	// Create requestor on A and send the request, expect a reply.
	ncA := natsConnect(t, a.ClientURL())
	defer ncA.Close()
	if msg, err := ncA.Request("service", []byte("request"), time.Second); err != nil {
		t.Fatalf("Failed to get reply: %v", err)
	} else if string(msg.Data) != "reply" {
		t.Fatalf("Unexpected reply: %q", msg.Data)
	}
}

func TestLeafNodeTmpClients(t *testing.T) {
	ao := DefaultOptions()
	ao.LeafNode.Host = "127.0.0.1"
	ao.LeafNode.Port = -1
	a := RunServer(ao)
	defer a.Shutdown()

	c, err := net.Dial("tcp", fmt.Sprintf("127.0.0.1:%d", ao.LeafNode.Port))
	if err != nil {
		t.Fatalf("Error connecting: %v", err)
	}
	defer c.Close()
	// Read info
	br := bufio.NewReader(c)
	br.ReadLine()

	checkTmp := func(expected int) {
		t.Helper()
		checkFor(t, time.Second, 15*time.Millisecond, func() error {
			a.grMu.Lock()
			l := len(a.grTmpClients)
			a.grMu.Unlock()
			if l != expected {
				return fmt.Errorf("Expected tmp map to have %v entries, got %v", expected, l)
			}
			return nil
		})
	}
	checkTmp(1)

	// Close client and wait check that it is removed.
	c.Close()
	checkTmp(0)

	// Check with normal leafnode connection that once connected,
	// the tmp map is also emptied.
	bo := DefaultOptions()
	bo.Cluster.Name = "xyz"
	bo.LeafNode.ReconnectInterval = 5 * time.Millisecond
	u, err := url.Parse(fmt.Sprintf("nats://127.0.0.1:%d", ao.LeafNode.Port))
	if err != nil {
		t.Fatalf("Error creating url: %v", err)
	}
	bo.LeafNode.Remotes = []*RemoteLeafOpts{{URLs: []*url.URL{u}}}
	b := RunServer(bo)
	defer b.Shutdown()

	checkLeafNodeConnected(t, b)
	checkTmp(0)
}

func TestLeafNodeTLSVerifyAndMap(t *testing.T) {
	accName := "MyAccount"
	acc := NewAccount(accName)
	certUserName := "CN=example.com,OU=NATS.io"
	users := []*User{{Username: certUserName, Account: acc}}

	for _, test := range []struct {
		name        string
		leafUsers   bool
		provideCert bool
	}{
		{"no users override, provides cert", false, true},
		{"no users override, does not provide cert", false, false},
		{"users override, provides cert", true, true},
		{"users override, does not provide cert", true, false},
	} {
		t.Run(test.name, func(t *testing.T) {
			o := DefaultOptions()
			o.Accounts = []*Account{acc}
			o.LeafNode.Host = "127.0.0.1"
			o.LeafNode.Port = -1
			if test.leafUsers {
				o.LeafNode.Users = users
			} else {
				o.Users = users
			}
			tc := &TLSConfigOpts{
				CertFile: "../test/configs/certs/tlsauth/server.pem",
				KeyFile:  "../test/configs/certs/tlsauth/server-key.pem",
				CaFile:   "../test/configs/certs/tlsauth/ca.pem",
				Verify:   true,
			}
			tlsc, err := GenTLSConfig(tc)
			if err != nil {
				t.Fatalf("Error creating tls config: %v", err)
			}
			o.LeafNode.TLSConfig = tlsc
			o.LeafNode.TLSMap = true
			s := RunServer(o)
			defer s.Shutdown()

			slo := DefaultOptions()
			slo.Cluster.Name = "xyz"

			sltlsc := &tls.Config{}
			if test.provideCert {
				tc := &TLSConfigOpts{
					CertFile: "../test/configs/certs/tlsauth/client.pem",
					KeyFile:  "../test/configs/certs/tlsauth/client-key.pem",
				}
				var err error
				sltlsc, err = GenTLSConfig(tc)
				if err != nil {
					t.Fatalf("Error generating tls config: %v", err)
				}
			}
			sltlsc.InsecureSkipVerify = true
			u, _ := url.Parse(fmt.Sprintf("nats://%s:%d", o.LeafNode.Host, o.LeafNode.Port))
			slo.LeafNode.Remotes = []*RemoteLeafOpts{
				{
					TLSConfig: sltlsc,
					URLs:      []*url.URL{u},
				},
			}
			sl := RunServer(slo)
			defer sl.Shutdown()

			if !test.provideCert {
				// Wait a bit and make sure we are not connecting
				time.Sleep(100 * time.Millisecond)
				checkLeafNodeConnectedCount(t, s, 0)
				return
			}
			checkLeafNodeConnected(t, s)

			var uname string
			var accname string
			s.mu.Lock()
			for _, c := range s.leafs {
				c.mu.Lock()
				uname = c.opts.Username
				if c.acc != nil {
					accname = c.acc.GetName()
				}
				c.mu.Unlock()
			}
			s.mu.Unlock()
			if uname != certUserName {
				t.Fatalf("Expected username %q, got %q", certUserName, uname)
			}
			if accname != accName {
				t.Fatalf("Expected account %q, got %v", accName, accname)
			}
		})
	}
}

type chanLogger struct {
	DummyLogger
	triggerChan chan string
}

func (l *chanLogger) Warnf(format string, v ...any) {
	l.triggerChan <- fmt.Sprintf(format, v...)
}

func (l *chanLogger) Errorf(format string, v ...any) {
	l.triggerChan <- fmt.Sprintf(format, v...)
}

const (
	testLeafNodeTLSVerifyAndMapSrvA = `
listen: 127.0.0.1:-1
leaf {
	listen: "127.0.0.1:-1"
	tls {
		cert_file: "../test/configs/certs/server-cert.pem"
		key_file:  "../test/configs/certs/server-key.pem"
		ca_file:   "../test/configs/certs/ca.pem"
		timeout: 2
		verify_and_map: true
	}
	authorization {
		users [{
			user: "%s"
		}]
	}
}
`
	testLeafNodeTLSVerifyAndMapSrvB = `
listen: -1
leaf {
	remotes [
		{
			url: "tls://user-provided-in-url@localhost:%d"
			tls {
				cert_file: "../test/configs/certs/server-cert.pem"
				key_file:  "../test/configs/certs/server-key.pem"
				ca_file:   "../test/configs/certs/ca.pem"
			}
		}
	]
}`
)

func TestLeafNodeTLSVerifyAndMapCfgPass(t *testing.T) {
	l := &chanLogger{triggerChan: make(chan string, 100)}
	defer close(l.triggerChan)

	confA := createConfFile(t, []byte(fmt.Sprintf(testLeafNodeTLSVerifyAndMapSrvA, "localhost")))
	srvA, optsA := RunServerWithConfig(confA)
	defer srvA.Shutdown()
	srvA.SetLogger(l, true, true)

	confB := createConfFile(t, []byte(fmt.Sprintf(testLeafNodeTLSVerifyAndMapSrvB, optsA.LeafNode.Port)))
	ob := LoadConfig(confB)
	ob.LeafNode.ReconnectInterval = 50 * time.Millisecond
	srvB := RunServer(ob)
	defer srvB.Shutdown()

	// Now make sure that the leaf node connection is up and the correct account was picked
	checkFor(t, 10*time.Second, 10*time.Millisecond, func() error {
		for _, srv := range []*Server{srvA, srvB} {
			if nln := srv.NumLeafNodes(); nln != 1 {
				return fmt.Errorf("Number of leaf nodes is %d", nln)
			}
			if leafz, err := srv.Leafz(nil); err != nil {
				if len(leafz.Leafs) != 1 {
					return fmt.Errorf("Number of leaf nodes returned by LEAFZ is not one: %d", len(leafz.Leafs))
				} else if leafz.Leafs[0].Account != DEFAULT_GLOBAL_ACCOUNT {
					return fmt.Errorf("Account used is not $G: %s", leafz.Leafs[0].Account)
				}
			}
		}
		return nil
	})
	// Make sure that the user name in the url was ignored and a warning printed
	for {
		select {
		case w := <-l.triggerChan:
			if w == `User "user-provided-in-url" found in connect proto, but user required from cert` {
				return
			}
		case <-time.After(2 * time.Second):
			t.Fatal("Did not get expected warning")
		}
	}
}

func TestLeafNodeTLSVerifyAndMapCfgFail(t *testing.T) {
	l := &chanLogger{triggerChan: make(chan string, 100)}
	defer close(l.triggerChan)

	// use certificate with SAN localhost, but configure the server to not accept it
	// instead provide a name matching the user (to be matched by failed
	confA := createConfFile(t, []byte(fmt.Sprintf(testLeafNodeTLSVerifyAndMapSrvA, "user-provided-in-url")))
	srvA, optsA := RunServerWithConfig(confA)
	defer srvA.Shutdown()
	srvA.SetLogger(l, true, true)

	confB := createConfFile(t, []byte(fmt.Sprintf(testLeafNodeTLSVerifyAndMapSrvB, optsA.LeafNode.Port)))
	ob := LoadConfig(confB)
	ob.LeafNode.ReconnectInterval = 50 * time.Millisecond
	srvB := RunServer(ob)
	defer srvB.Shutdown()

	// Now make sure that the leaf node connection is down
	checkFor(t, 10*time.Second, 10*time.Millisecond, func() error {
		for _, srv := range []*Server{srvA, srvB} {
			if nln := srv.NumLeafNodes(); nln != 0 {
				return fmt.Errorf("Number of leaf nodes is %d", nln)
			}
		}
		return nil
	})
	// Make sure that the connection was closed for the right reason
	for {
		select {
		case w := <-l.triggerChan:
			if strings.Contains(w, ErrAuthentication.Error()) {
				return
			}
		case <-time.After(2 * time.Second):
			t.Fatal("Did not get expected warning")
		}
	}
}

func TestLeafNodeOriginClusterInfo(t *testing.T) {
	hopts := DefaultOptions()
	hopts.ServerName = "hub"
	hopts.LeafNode.Port = -1

	hub := RunServer(hopts)
	defer hub.Shutdown()

	conf := createConfFile(t, []byte(fmt.Sprintf(`
		port: -1
		leaf {
			remotes [ { url: "nats://127.0.0.1:%d" } ]
		}
	`, hopts.LeafNode.Port)))

	opts, err := ProcessConfigFile(conf)
	if err != nil {
		t.Fatalf("Error processing config file: %v", err)
	}
	opts.NoLog, opts.NoSigs = true, true

	s := RunServer(opts)
	defer s.Shutdown()

	checkLeafNodeConnected(t, s)

	// Check the info on the leadnode client in the hub.
	grabLeaf := func() *client {
		var l *client
		hub.mu.Lock()
		for _, l = range hub.leafs {
			break
		}
		hub.mu.Unlock()
		return l
	}

	l := grabLeaf()
	if rc := l.remoteCluster(); rc != "" {
		t.Fatalf("Expected an empty remote cluster, got %q", rc)
	}

	s.Shutdown()

	// Now make our leafnode part of a cluster.
	conf = createConfFile(t, []byte(fmt.Sprintf(`
		port: -1
		leaf {
			remotes [ { url: "nats://127.0.0.1:%d" } ]
		}
		cluster {
			name: "xyz"
			listen: "127.0.0.1:-1"
		}
	`, hopts.LeafNode.Port)))

	opts, err = ProcessConfigFile(conf)
	if err != nil {
		t.Fatalf("Error processing config file: %v", err)
	}
	opts.NoLog, opts.NoSigs = true, true

	s = RunServer(opts)
	defer s.Shutdown()

	checkLeafNodeConnected(t, s)

	l = grabLeaf()
	if rc := l.remoteCluster(); rc != "xyz" {
		t.Fatalf("Expected a remote cluster name of \"xyz\", got %q", rc)
	}
	pcid := l.cid

	// Now make sure that if we update our cluster name, simulating the settling
	// of dynamic cluster names between competing servers.
	s.setClusterName("xyz")
	// Make sure we disconnect and reconnect.
	checkLeafNodeConnectedCount(t, s, 0)
	checkLeafNodeConnected(t, s)
	checkLeafNodeConnected(t, hub)

	l = grabLeaf()
	if rc := l.remoteCluster(); rc != "xyz" {
		t.Fatalf("Expected a remote cluster name of \"xyz\", got %q", rc)
	}
	// Make sure we reconnected and have a new CID.
	if l.cid == pcid {
		t.Fatalf("Expected a different id, got the same")
	}
}

type proxyAcceptDetectFailureLate struct {
	sync.Mutex
	wg         sync.WaitGroup
	acceptPort int
	l          net.Listener
	srvs       []net.Conn
	leaf       net.Conn
	startChan  chan struct{}
}

func (p *proxyAcceptDetectFailureLate) run(t *testing.T) int {
	return p.runEx(t, false)
}

func (p *proxyAcceptDetectFailureLate) runEx(t *testing.T, needStart bool) int {
	l, err := natsListen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("Error on listen: %v", err)
	}
	p.Lock()
	var startChan chan struct{}
	if needStart {
		startChan = make(chan struct{})
		p.startChan = startChan
	}
	p.l = l
	p.Unlock()
	port := l.Addr().(*net.TCPAddr).Port
	p.wg.Add(1)
	go func() {
		defer p.wg.Done()
		defer l.Close()
		defer func() {
			p.Lock()
			for _, c := range p.srvs {
				c.Close()
			}
			p.Unlock()
		}()
		if startChan != nil {
			<-startChan
		}
		for {
			c, err := l.Accept()
			if err != nil {
				return
			}
			srv, err := net.Dial("tcp", fmt.Sprintf("127.0.0.1:%d", p.acceptPort))
			if err != nil {
				return
			}
			p.Lock()
			p.leaf = c
			p.srvs = append(p.srvs, srv)
			p.Unlock()

			transfer := func(c1, c2 net.Conn) {
				var buf [1024]byte
				for {
					n, err := c1.Read(buf[:])
					if err != nil {
						return
					}
					if _, err := c2.Write(buf[:n]); err != nil {
						return
					}
				}
			}

			go transfer(srv, c)
			go transfer(c, srv)
		}
	}()
	return port
}

func (p *proxyAcceptDetectFailureLate) start() {
	p.Lock()
	if p.startChan != nil {
		close(p.startChan)
		p.startChan = nil
	}
	p.Unlock()
}

func (p *proxyAcceptDetectFailureLate) close() {
	p.Lock()
	if p.startChan != nil {
		close(p.startChan)
		p.startChan = nil
	}
	p.l.Close()
	p.Unlock()

	p.wg.Wait()
}

type oldConnReplacedLogger struct {
	DummyLogger
	errCh  chan string
	warnCh chan string
}

func (l *oldConnReplacedLogger) Errorf(format string, v ...any) {
	select {
	case l.errCh <- fmt.Sprintf(format, v...):
	default:
	}
}

func (l *oldConnReplacedLogger) Warnf(format string, v ...any) {
	select {
	case l.warnCh <- fmt.Sprintf(format, v...):
	default:
	}
}

// This test will simulate that the accept side does not detect the connection
// has been closed early enough. The soliciting side will attempt to reconnect
// and we should not be getting the "loop detected" error.
func TestLeafNodeLoopDetectedDueToReconnect(t *testing.T) {
	o := DefaultOptions()
	o.LeafNode.Host = "127.0.0.1"
	o.LeafNode.Port = -1
	s := RunServer(o)
	defer s.Shutdown()

	l := &oldConnReplacedLogger{errCh: make(chan string, 10), warnCh: make(chan string, 10)}
	s.SetLogger(l, false, false)

	p := &proxyAcceptDetectFailureLate{acceptPort: o.LeafNode.Port}
	defer p.close()
	port := p.run(t)

	aurl, err := url.Parse(fmt.Sprintf("nats://127.0.0.1:%d", port))
	if err != nil {
		t.Fatalf("Error parsing url: %v", err)
	}
	ol := DefaultOptions()
	ol.Cluster.Name = "cde"
	ol.LeafNode.ReconnectInterval = 50 * time.Millisecond
	ol.LeafNode.Remotes = []*RemoteLeafOpts{{URLs: []*url.URL{aurl}}}
	sl := RunServer(ol)
	defer sl.Shutdown()

	checkLeafNodeConnected(t, s)
	checkLeafNodeConnected(t, sl)

	// Cause disconnect client side...
	p.Lock()
	p.leaf.Close()
	p.Unlock()

	// Make sure we did not get the loop detected error
	select {
	case e := <-l.errCh:
		if strings.Contains(e, "Loop detected") {
			t.Fatalf("Loop detected: %v", e)
		}
	case <-time.After(250 * time.Millisecond):
		// We are ok
	}

	// Now make sure we got the warning
	select {
	case w := <-l.warnCh:
		if !strings.Contains(w, "Replacing connection from same server") {
			t.Fatalf("Unexpected warning: %v", w)
		}
	case <-time.After(time.Second):
		t.Fatal("Did not get expected warning")
	}

	checkLeafNodeConnected(t, s)
	checkLeafNodeConnected(t, sl)
}

func TestLeafNodeDetectDuplicateRemotesInOptions(t *testing.T) {
	u, err := url.Parse("nats://user1:pwd@127.0.0.1:1234")
	require_NoError(t, err)
	urls := []*url.URL{u}
	for _, test := range []struct {
		name   string
		remote *RemoteLeafOpts
	}{
		{"same urls", &RemoteLeafOpts{URLs: urls}},
		{"same urls/acc", &RemoteLeafOpts{URLs: urls, LocalAccount: "A"}},
		{"same urls/acc/creds", &RemoteLeafOpts{URLs: urls, LocalAccount: "A", Credentials: "credsfile"}},
	} {
		t.Run(test.name, func(t *testing.T) {
			opts := DefaultOptions()
			opts.Accounts = append(opts.Accounts, NewAccount("A"))
			opts.LeafNode.Port = -1
			u, err := url.Parse("nats://user2:pwd@127.0.0.1:1235")
			require_NoError(t, err)
			rlo := &RemoteLeafOpts{URLs: []*url.URL{u}}
			opts.LeafNode.Remotes = append(opts.LeafNode.Remotes, test.remote, rlo, test.remote)
			_, err = NewServer(opts)
			require_Error(t, err)
			require_Contains(t, err.Error(), "duplicate remote")
			// Ensure url is redacted
			require_Contains(t, err.Error(), "user1:xxxxx")
		})
	}
}

func TestLeafNodeHubRejectDuplicateRemotes(t *testing.T) {
	opts := DefaultOptions()
	opts.ServerName = "A"
	opts.LeafNode.Host = "127.0.0.1"
	opts.LeafNode.Port = -1
	s := RunServer(opts)
	defer s.Shutdown()
	l := &captureErrorLogger{errCh: make(chan string, 10)}
	s.SetLogger(l, false, false)

	tmpl := `
		listen: 127.0.0.1:-1
		server_name: "B"
		leafnodes {
			remotes = [{url: nats-leaf://127.0.0.1:%d}]
		}
	`
	conf := createConfFile(t, fmt.Appendf(nil, tmpl, opts.LeafNode.Port))
	ln, lno := RunServerWithConfig(conf)
	defer ln.Shutdown()

	// Since we correctly reject duplicate remotes, we have to manually
	// add a duplicate and start soliciting here.
	r := *lno.LeafNode.Remotes[0]
	ln.solicitLeafNodeRemotes([]*RemoteLeafOpts{&r})

	select {
	case err := <-l.errCh:
		if !strings.Contains(err, DuplicateRemoteLeafnodeConnection.String()) {
			t.Fatalf("Did not get expected duplicate connection error: %v", err)
		}
	case <-time.After(250 * time.Millisecond):
		t.Fatal("Did not get expected error")
	}
}

func TestLeafNodeNoDuplicateWithinCluster(t *testing.T) {
	// This set the cluster name to "abc"
	oSrv1 := DefaultOptions()
	oSrv1.ServerName = "srv1"
	oSrv1.LeafNode.Host = "127.0.0.1"
	oSrv1.LeafNode.Port = -1
	srv1 := RunServer(oSrv1)
	defer srv1.Shutdown()

	u, err := url.Parse(fmt.Sprintf("nats://127.0.0.1:%d", oSrv1.LeafNode.Port))
	if err != nil {
		t.Fatalf("Error parsing url: %v", err)
	}

	oLeaf1 := DefaultOptions()
	oLeaf1.ServerName = "leaf1"
	oLeaf1.Cluster.Name = "xyz"
	oLeaf1.LeafNode.Remotes = []*RemoteLeafOpts{{URLs: []*url.URL{u}}}
	leaf1 := RunServer(oLeaf1)
	defer leaf1.Shutdown()

	leaf1ClusterURL := fmt.Sprintf("nats://127.0.0.1:%d", oLeaf1.Cluster.Port)

	oLeaf2 := DefaultOptions()
	oLeaf2.ServerName = "leaf2"
	oLeaf2.Cluster.Name = "xyz"
	oLeaf2.LeafNode.Remotes = []*RemoteLeafOpts{{URLs: []*url.URL{u}}}
	oLeaf2.Routes = RoutesFromStr(leaf1ClusterURL)
	leaf2 := RunServer(oLeaf2)
	defer leaf2.Shutdown()

	checkClusterFormed(t, leaf1, leaf2)

	checkLeafNodeConnectedCount(t, srv1, 2)
	checkLeafNodeConnected(t, leaf1)
	checkLeafNodeConnected(t, leaf2)

	ncSrv1 := natsConnect(t, srv1.ClientURL())
	defer ncSrv1.Close()
	natsQueueSub(t, ncSrv1, "foo", "queue", func(m *nats.Msg) {
		m.Data = []byte("from srv1")
		m.RespondMsg(m)
	})

	ncLeaf1 := natsConnect(t, leaf1.ClientURL())
	defer ncLeaf1.Close()
	natsQueueSub(t, ncLeaf1, "foo", "queue", func(m *nats.Msg) {
		m.Data = []byte("from leaf1")
		m.RespondMsg(m)
	})

	ncLeaf2 := natsConnect(t, leaf2.ClientURL())
	defer ncLeaf2.Close()

	// Check that "foo" interest is available everywhere.
	for _, s := range []*Server{srv1, leaf1, leaf2} {
		gacc := s.GlobalAccount()
		checkFor(t, time.Second, 15*time.Millisecond, func() error {
			if n := gacc.Interest("foo"); n != 2 {
				return fmt.Errorf("Expected interest for %q to be 2, got %v", "foo", n)
			}
			return nil
		})
	}

	// Send requests (from leaf2). For this test to make sure that
	// there is no duplicate, we want to make sure that we check for
	// multiple replies and that the reply subject subscription has
	// been propagated everywhere.
	sub := natsSubSync(t, ncLeaf2, "reply_subj")
	natsFlush(t, ncLeaf2)

	// Here we have a single sub on "reply_subj" so using checkSubInterest is ok.
	checkSubInterest(t, srv1, globalAccountName, "reply_subj", time.Second)
	checkSubInterest(t, leaf1, globalAccountName, "reply_subj", time.Second)
	checkSubInterest(t, leaf2, globalAccountName, "reply_subj", time.Second)

	for i := 0; i < 100; i++ {
		// Now send the request
		reqID := fmt.Sprintf("req.%d", i)
		msg := nats.NewMsg("foo")
		msg.Data = []byte("req")
		msg.Header.Set("ReqId", reqID)
		msg.Reply = sub.Subject
		if err := ncLeaf2.PublishMsg(msg); err != nil {
			t.Fatalf("Error on publish: %v", err)
		}
		// Check that we get the reply
		replyMsg := natsNexMsg(t, sub, time.Second)
		// But make sure no duplicate. We do so by checking that the reply's
		// header ReqId matches our current reqID.
		if respReqID := replyMsg.Header.Get("ReqId"); respReqID != reqID {
			t.Fatalf("Current request is %q, got duplicate with %q", reqID, respReqID)
		}
		// We also should have preferred the queue sub that is in the leaf cluster.
		if string(replyMsg.Data) != "from leaf1" {
			t.Fatalf("Expected reply from leaf1, got %q", replyMsg.Data)
		}
	}
}

func TestLeafNodeLMsgSplit(t *testing.T) {
	// This set the cluster name to "abc"
	oSrv1 := DefaultOptions()
	oSrv1.LeafNode.Host = "127.0.0.1"
	oSrv1.LeafNode.Port = -1
	srv1 := RunServer(oSrv1)
	defer srv1.Shutdown()

	oSrv2 := DefaultOptions()
	oSrv2.LeafNode.Host = "127.0.0.1"
	oSrv2.LeafNode.Port = -1
	oSrv2.Routes = RoutesFromStr(fmt.Sprintf("nats://127.0.0.1:%d", oSrv1.Cluster.Port))
	srv2 := RunServer(oSrv2)
	defer srv2.Shutdown()

	checkClusterFormed(t, srv1, srv2)

	u1, err := url.Parse(fmt.Sprintf("nats://127.0.0.1:%d", oSrv1.LeafNode.Port))
	if err != nil {
		t.Fatalf("Error parsing url: %v", err)
	}
	u2, err := url.Parse(fmt.Sprintf("nats://127.0.0.1:%d", oSrv2.LeafNode.Port))
	if err != nil {
		t.Fatalf("Error parsing url: %v", err)
	}

	oLeaf1 := DefaultOptions()
	oLeaf1.Cluster.Name = "xyz"
	oLeaf1.LeafNode.Remotes = []*RemoteLeafOpts{{URLs: []*url.URL{u1, u2}}}
	leaf1 := RunServer(oLeaf1)
	defer leaf1.Shutdown()

	oLeaf2 := DefaultOptions()
	oLeaf2.Cluster.Name = "xyz"
	oLeaf2.LeafNode.Remotes = []*RemoteLeafOpts{{URLs: []*url.URL{u1, u2}}}
	oLeaf2.Routes = RoutesFromStr(fmt.Sprintf("nats://127.0.0.1:%d", oLeaf1.Cluster.Port))
	leaf2 := RunServer(oLeaf2)
	defer leaf2.Shutdown()

	checkClusterFormed(t, leaf1, leaf2)

	checkLeafNodeConnected(t, leaf1)
	checkLeafNodeConnected(t, leaf2)

	ncSrv2 := natsConnect(t, srv2.ClientURL())
	defer ncSrv2.Close()
	natsQueueSub(t, ncSrv2, "foo", "queue", func(m *nats.Msg) {
		m.Respond([]byte("from srv2"))
	})

	// Check that "foo" interest is available everywhere.
	checkSubInterest(t, srv1, globalAccountName, "foo", time.Second)
	checkSubInterest(t, srv2, globalAccountName, "foo", time.Second)
	checkSubInterest(t, leaf1, globalAccountName, "foo", time.Second)
	checkSubInterest(t, leaf2, globalAccountName, "foo", time.Second)

	// Not required, but have a request payload that is more than 100 bytes
	reqPayload := make([]byte, 150)
	for i := 0; i < len(reqPayload); i++ {
		reqPayload[i] = byte((i % 26)) + 'A'
	}

	// Send repeated requests (from scratch) from leaf-2:
	sendReq := func() {
		t.Helper()

		ncLeaf2 := natsConnect(t, leaf2.ClientURL())
		defer ncLeaf2.Close()

		if _, err := ncLeaf2.Request("foo", reqPayload, time.Second); err != nil {
			t.Fatalf("Did not receive reply: %v", err)
		}
	}
	for i := 0; i < 100; i++ {
		sendReq()
	}
}

type parseRouteLSUnsubLogger struct {
	DummyLogger
	gotTrace chan struct{}
	gotErr   chan error
}

func (l *parseRouteLSUnsubLogger) Errorf(format string, v ...any) {
	err := fmt.Errorf(format, v...)
	select {
	case l.gotErr <- err:
	default:
	}
}

func (l *parseRouteLSUnsubLogger) Tracef(format string, v ...any) {
	trace := fmt.Sprintf(format, v...)
	if strings.Contains(trace, "LS- xyz $G foo bar") {
		l.gotTrace <- struct{}{}
	}
}

func TestLeafNodeRouteParseLSUnsub(t *testing.T) {
	// This set the cluster name to "abc"
	oSrv1 := DefaultOptions()
	oSrv1.LeafNode.Host = "127.0.0.1"
	oSrv1.LeafNode.Port = -1
	srv1 := RunServer(oSrv1)
	defer srv1.Shutdown()

	l := &parseRouteLSUnsubLogger{gotTrace: make(chan struct{}, 1), gotErr: make(chan error, 1)}
	srv1.SetLogger(l, true, true)

	oSrv2 := DefaultOptions()
	oSrv2.LeafNode.Host = "127.0.0.1"
	oSrv2.LeafNode.Port = -1
	oSrv2.Routes = RoutesFromStr(fmt.Sprintf("nats://127.0.0.1:%d", oSrv1.Cluster.Port))
	srv2 := RunServer(oSrv2)
	defer srv2.Shutdown()

	checkClusterFormed(t, srv1, srv2)

	u2, err := url.Parse(fmt.Sprintf("nats://127.0.0.1:%d", oSrv2.LeafNode.Port))
	if err != nil {
		t.Fatalf("Error parsing url: %v", err)
	}

	oLeaf2 := DefaultOptions()
	oLeaf2.Cluster.Name = "xyz"
	oLeaf2.LeafNode.Remotes = []*RemoteLeafOpts{{URLs: []*url.URL{u2}}}
	leaf2 := RunServer(oLeaf2)
	defer leaf2.Shutdown()

	checkLeafNodeConnected(t, srv2)
	checkLeafNodeConnected(t, leaf2)

	ncLeaf2 := natsConnect(t, leaf2.ClientURL())
	defer ncLeaf2.Close()

	sub := natsQueueSubSync(t, ncLeaf2, "foo", "bar")
	// The issue was with the unsubscribe of this queue subscription
	natsUnsub(t, sub)

	// We should get the trace
	select {
	case <-l.gotTrace:
		// OK!
	case <-time.After(100 * time.Millisecond):
		t.Fatalf("Did not get LS- trace")
	}
	// And no error...
	select {
	case e := <-l.gotErr:
		t.Fatalf("There was an error on server 1: %q", e.Error())
	case <-time.After(100 * time.Millisecond):
		// OK!
	}
}

func TestLeafNodeOperatorBadCfg(t *testing.T) {
	sysAcc, err := nkeys.CreateAccount()
	require_NoError(t, err)
	sysAccPk, err := sysAcc.PublicKey()
	require_NoError(t, err)
	tmpDir := t.TempDir()

	configTmpl := `
		port: -1
		operator: %s
		system_account: %s
		resolver: {
			type: cache
			dir: '%s'
		}
		leafnodes: {
			%s
		}
	`

	cases := []struct {
		name      string
		errorText string
		cfg       string
	}{
		{
			name:      "Operator with Leafnode",
			errorText: "operator mode does not allow specifying users in leafnode config",
			cfg: `
			port: -1
			authorization {
				users = [{user: "u", password: "p"}]
			}`,
		},
		{
			name:      "Operator with NKey",
			errorText: "operator mode and non account nkeys are incompatible",
			cfg: `
			port: -1
			authorization {
				account: notankey
			}`,
		},
		{
			name: "Operator remote account NKeys",
			errorText: "operator mode requires account nkeys in remotes. " +
				"Please add an `account` key to each remote in your `leafnodes` section, to assign it to an account. " +
				"Each account value should be a 56 character public key, starting with the letter 'A'",
			cfg: `remotes: [{url: u}]`,
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			conf := createConfFile(t, []byte(fmt.Sprintf(configTmpl, ojwt, sysAccPk, tmpDir, c.cfg)))
			opts := LoadConfig(conf)
			s, err := NewServer(opts)
			if err == nil {
				s.Shutdown()
				t.Fatal("Expected an error")
			}
			// Since the server cannot be stopped, since it did not start,
			// let's manually close the account resolver to avoid leaking go routines.
			opts.AccountResolver.Close()
			if err.Error() != c.errorText {
				t.Fatalf("Expected error %q but got %q", c.errorText, err)
			}
		})
	}
}

func TestLeafNodeTLSConfigReload(t *testing.T) {
	template := `
		listen: 127.0.0.1:-1
		leaf {
			listen: "127.0.0.1:-1"
			tls {
				cert_file: "../test/configs/certs/server-cert.pem"
				key_file:  "../test/configs/certs/server-key.pem"
				%s
				timeout: 2
				verify: true
			}
		}
	`
	confA := createConfFile(t, []byte(fmt.Sprintf(template, "")))

	srvA, optsA := RunServerWithConfig(confA)
	defer srvA.Shutdown()

	lg := &captureErrorLogger{errCh: make(chan string, 10)}
	srvA.SetLogger(lg, false, false)

	confB := createConfFile(t, []byte(fmt.Sprintf(`
		listen: -1
		leaf {
			remotes [
				{
					url: "tls://127.0.0.1:%d"
					tls {
						cert_file: "../test/configs/certs/server-cert.pem"
						key_file:  "../test/configs/certs/server-key.pem"
						ca_file:   "../test/configs/certs/ca.pem"
					}
				}
			]
		}
	`, optsA.LeafNode.Port)))

	optsB, err := ProcessConfigFile(confB)
	if err != nil {
		t.Fatalf("Error processing config file: %v", err)
	}
	optsB.LeafNode.ReconnectInterval = 50 * time.Millisecond
	optsB.NoLog, optsB.NoSigs = true, true

	srvB := RunServer(optsB)
	defer srvB.Shutdown()

	// Wait for the error
	select {
	case err := <-lg.errCh:
		// Since Go 1.18, we had to regenerate certs to not have to use GODEBUG="x509sha1=1"
		// But on macOS, with our test CA certs, no SCTs included, it will fail
		// for the reason "x509: “localhost” certificate is not standards compliant"
		// instead of "unknown authority".
		if !strings.Contains(err, "unknown") && !strings.Contains(err, "compliant") {
			t.Fatalf("Unexpected error: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatalf("Did not get TLS error")
	}

	// Add the CA to srvA
	reloadUpdateConfig(t, srvA, confA, fmt.Sprintf(template, `ca_file: "../test/configs/certs/ca.pem"`))

	// Now make sure that srvB can create a LN connection.
	checkFor(t, 3*time.Second, 10*time.Millisecond, func() error {
		if nln := srvB.NumLeafNodes(); nln != 1 {
			return fmt.Errorf("Number of leaf nodes is %d", nln)
		}
		return nil
	})
}

func TestLeafNodeTLSConfigReloadForRemote(t *testing.T) {
	confA := createConfFile(t, []byte(`
		listen: 127.0.0.1:-1
		leaf {
			listen: "127.0.0.1:-1"
			tls {
				cert_file: "../test/configs/certs/server-cert.pem"
				key_file:  "../test/configs/certs/server-key.pem"
				ca_file: "../test/configs/certs/ca.pem"
				timeout: 2
				verify: true
			}
		}
	`))

	srvA, optsA := RunServerWithConfig(confA)
	defer srvA.Shutdown()

	lg := &captureErrorLogger{errCh: make(chan string, 10)}
	srvA.SetLogger(lg, false, false)

	template := `
		listen: -1
		leaf {
			remotes [
				{
					url: "tls://127.0.0.1:%d"
					tls {
						cert_file: "../test/configs/certs/server-cert.pem"
						key_file:  "../test/configs/certs/server-key.pem"
						%s
					}
				}
			]
		}
	`
	confB := createConfFile(t, []byte(fmt.Sprintf(template, optsA.LeafNode.Port, "")))

	srvB, _ := RunServerWithConfig(confB)
	defer srvB.Shutdown()

	// Wait for the error
	select {
	case err := <-lg.errCh:
		if !strings.Contains(err, "bad certificate") {
			t.Fatalf("Unexpected error: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatalf("Did not get TLS error")
	}

	// Add the CA to srvB
	reloadUpdateConfig(t, srvB, confB, fmt.Sprintf(template, optsA.LeafNode.Port, `ca_file: "../test/configs/certs/ca.pem"`))

	// Now make sure that srvB can create a LN connection.
	checkFor(t, 2*time.Second, 10*time.Millisecond, func() error {
		if nln := srvB.NumLeafNodes(); nln != 1 {
			return fmt.Errorf("Number of leaf nodes is %d", nln)
		}
		return nil
	})
}

func testDefaultLeafNodeWSOptions() *Options {
	o := DefaultOptions()
	o.Websocket.Host = "127.0.0.1"
	o.Websocket.Port = -1
	o.Websocket.NoTLS = true
	o.LeafNode.Host = "127.0.0.1"
	o.LeafNode.Port = -1
	return o
}

func testDefaultRemoteLeafNodeWSOptions(t *testing.T, o *Options, tls bool) *Options {
	// Use some path in the URL.. we don't use that, but internally
	// the server will prefix the path with /leafnode so that the
	// WS webserver knows that it needs to create a LEAF connection.
	u, _ := url.Parse(fmt.Sprintf("ws://127.0.0.1:%d/some/path", o.Websocket.Port))
	lo := DefaultOptions()
	lo.Cluster.Name = "LN"
	remote := &RemoteLeafOpts{URLs: []*url.URL{u}}
	if tls {
		tc := &TLSConfigOpts{
			CertFile: "../test/configs/certs/server-cert.pem",
			KeyFile:  "../test/configs/certs/server-key.pem",
			CaFile:   "../test/configs/certs/ca.pem",
		}
		tlsConf, err := GenTLSConfig(tc)
		if err != nil {
			t.Fatalf("Error generating TLS config: %v", err)
		}
		// GenTLSConfig sets the CA in ClientCAs, but since here we act
		// as a client, set RootCAs...
		tlsConf.RootCAs = tlsConf.ClientCAs
		remote.TLSConfig = tlsConf
	}
	lo.LeafNode.Remotes = []*RemoteLeafOpts{remote}
	return lo
}

func TestLeafNodeWSMixURLs(t *testing.T) {
	for _, test := range []struct {
		name string
		urls []string
	}{
		{"mix 1", []string{"nats://127.0.0.1:1234", "ws://127.0.0.1:5678", "wss://127.0.0.1:9012"}},
		{"mix 2", []string{"ws://127.0.0.1:1234", "nats://127.0.0.1:5678", "wss://127.0.0.1:9012"}},
		{"mix 3", []string{"wss://127.0.0.1:1234", "ws://127.0.0.1:5678", "nats://127.0.0.1:9012"}},
		{"mix 4", []string{"ws://127.0.0.1:1234", "nats://127.0.0.1:9012"}},
		{"mix 5", []string{"nats://127.0.0.1:1234", "ws://127.0.0.1:9012"}},
		{"mix 6", []string{"wss://127.0.0.1:1234", "nats://127.0.0.1:9012"}},
		{"mix 7", []string{"nats://127.0.0.1:1234", "wss://127.0.0.1:9012"}},
	} {
		t.Run(test.name, func(t *testing.T) {
			o := DefaultOptions()
			remote := &RemoteLeafOpts{}
			urls := make([]*url.URL, 0, 3)
			for _, ustr := range test.urls {
				u, err := url.Parse(ustr)
				if err != nil {
					t.Fatalf("Error parsing url: %v", err)
				}
				urls = append(urls, u)
			}
			remote.URLs = urls
			o.LeafNode.Remotes = []*RemoteLeafOpts{remote}
			s, err := NewServer(o)
			if err == nil || !strings.Contains(err.Error(), "mix") {
				if s != nil {
					s.Shutdown()
				}
				t.Fatalf("Unexpected error: %v", err)
			}
		})
	}
}

type testConnTrackSize struct {
	sync.Mutex
	net.Conn
	sz int
}

func (c *testConnTrackSize) Write(p []byte) (int, error) {
	c.Lock()
	defer c.Unlock()
	n, err := c.Conn.Write(p)
	c.sz += n
	return n, err
}

func TestLeafNodeWSBasic(t *testing.T) {
	for _, test := range []struct {
		name              string
		masking           bool
		tls               bool
		acceptCompression bool
		remoteCompression bool
	}{
		{"masking plain no compression", true, false, false, false},
		{"masking plain compression", true, false, true, true},
		{"masking plain compression disagree", true, false, false, true},
		{"masking plain compression disagree 2", true, false, true, false},
		{"masking tls no compression", true, true, false, false},
		{"masking tls compression", true, true, true, true},
		{"masking tls compression disagree", true, true, false, true},
		{"masking tls compression disagree 2", true, true, true, false},
		{"no masking plain no compression", false, false, false, false},
		{"no masking plain compression", false, false, true, true},
		{"no masking plain compression disagree", false, false, false, true},
		{"no masking plain compression disagree 2", false, false, true, false},
		{"no masking tls no compression", false, true, false, false},
		{"no masking tls compression", false, true, true, true},
		{"no masking tls compression disagree", false, true, false, true},
		{"no masking tls compression disagree 2", false, true, true, false},
	} {
		t.Run(test.name, func(t *testing.T) {
			o := testDefaultLeafNodeWSOptions()
			o.Websocket.NoTLS = !test.tls
			if test.tls {
				tc := &TLSConfigOpts{
					CertFile: "../test/configs/certs/server-cert.pem",
					KeyFile:  "../test/configs/certs/server-key.pem",
					CaFile:   "../test/configs/certs/ca.pem",
				}
				tlsConf, err := GenTLSConfig(tc)
				if err != nil {
					t.Fatalf("Error generating TLS config: %v", err)
				}
				o.Websocket.TLSConfig = tlsConf
			}
			o.Websocket.Compression = test.acceptCompression
			s := RunServer(o)
			defer s.Shutdown()

			lo := testDefaultRemoteLeafNodeWSOptions(t, o, test.tls)
			lo.LeafNode.Remotes[0].Websocket.Compression = test.remoteCompression
			lo.LeafNode.Remotes[0].Websocket.NoMasking = !test.masking
			ln := RunServer(lo)
			defer ln.Shutdown()

			checkLeafNodeConnected(t, s)
			checkLeafNodeConnected(t, ln)

			var trackSizeConn *testConnTrackSize
			if !test.tls {
				var cln *client
				ln.mu.Lock()
				for _, l := range ln.leafs {
					cln = l
					break
				}
				ln.mu.Unlock()
				cln.mu.Lock()
				trackSizeConn = &testConnTrackSize{Conn: cln.nc}
				cln.nc = trackSizeConn
				cln.mu.Unlock()
			}

			nc1 := natsConnect(t, s.ClientURL())
			defer nc1.Close()
			sub1 := natsSubSync(t, nc1, "foo")
			natsFlush(t, nc1)

			checkSubInterest(t, ln, globalAccountName, "foo", time.Second)

			nc2 := natsConnect(t, ln.ClientURL())
			defer nc2.Close()
			msg1Payload := make([]byte, 2048)
			for i := 0; i < len(msg1Payload); i++ {
				msg1Payload[i] = 'A'
			}
			natsPub(t, nc2, "foo", msg1Payload)

			msg := natsNexMsg(t, sub1, time.Second)
			if !bytes.Equal(msg.Data, msg1Payload) {
				t.Fatalf("Invalid message: %q", msg.Data)
			}

			sub2 := natsSubSync(t, nc2, "bar")
			natsFlush(t, nc2)

			checkSubInterest(t, s, globalAccountName, "bar", time.Second)

			msg2Payload := make([]byte, 2048)
			for i := 0; i < len(msg2Payload); i++ {
				msg2Payload[i] = 'B'
			}
			natsPub(t, nc1, "bar", msg2Payload)

			msg = natsNexMsg(t, sub2, time.Second)
			if !bytes.Equal(msg.Data, msg2Payload) {
				t.Fatalf("Invalid message: %q", msg.Data)
			}

			if !test.tls {
				trackSizeConn.Lock()
				size := trackSizeConn.sz
				trackSizeConn.Unlock()

				if test.acceptCompression && test.remoteCompression {
					if size >= 1024 {
						t.Fatalf("Seems that there was no compression: size=%v", size)
					}
				} else if size < 2048 {
					t.Fatalf("Seems compression was on while it should not: size=%v", size)
				}
			}
		})
	}
}

func TestLeafNodeWSRemoteCompressAndMaskingOptions(t *testing.T) {
	for _, test := range []struct {
		name      string
		compress  bool
		compStr   string
		noMasking bool
		noMaskStr string
	}{
		{"compression masking", true, "true", false, "false"},
		{"compression no masking", true, "true", true, "true"},
		{"no compression masking", false, "false", false, "false"},
		{"no compression no masking", false, "false", true, "true"},
	} {
		t.Run(test.name, func(t *testing.T) {
			conf := createConfFile(t, []byte(fmt.Sprintf(`
				port: -1
				leafnodes {
					remotes [
						{url: "ws://127.0.0.1:1234", ws_compression: %s, ws_no_masking: %s}
					]
				}
			`, test.compStr, test.noMaskStr)))
			o, err := ProcessConfigFile(conf)
			if err != nil {
				t.Fatalf("Error loading conf: %v", err)
			}
			if nr := len(o.LeafNode.Remotes); nr != 1 {
				t.Fatalf("Expected 1 remote, got %v", nr)
			}
			r := o.LeafNode.Remotes[0]
			if cur := r.Websocket.Compression; cur != test.compress {
				t.Fatalf("Expected compress to be %v, got %v", test.compress, cur)
			}
			if cur := r.Websocket.NoMasking; cur != test.noMasking {
				t.Fatalf("Expected ws_masking to be %v, got %v", test.noMasking, cur)
			}
		})
	}
}

func TestLeafNodeWSNoMaskingRejected(t *testing.T) {
	wsTestRejectNoMasking = true
	defer func() { wsTestRejectNoMasking = false }()

	o := testDefaultLeafNodeWSOptions()
	s := RunServer(o)
	defer s.Shutdown()

	lo := testDefaultRemoteLeafNodeWSOptions(t, o, false)
	lo.LeafNode.Remotes[0].Websocket.NoMasking = true
	ln := RunServer(lo)
	defer ln.Shutdown()

	checkLeafNodeConnected(t, s)
	checkLeafNodeConnected(t, ln)

	var cln *client
	ln.mu.Lock()
	for _, l := range ln.leafs {
		cln = l
		break
	}
	ln.mu.Unlock()

	cln.mu.Lock()
	maskWrite := cln.ws.maskwrite
	cln.mu.Unlock()

	if !maskWrite {
		t.Fatal("Leafnode remote connection should mask writes, it does not")
	}
}

func TestLeafNodeWSSubPath(t *testing.T) {
	o := testDefaultLeafNodeWSOptions()
	s := RunServer(o)
	defer s.Shutdown()

	lo := testDefaultRemoteLeafNodeWSOptions(t, o, false)
	ln := RunServer(lo)
	defer ln.Shutdown()

	// Confirm that it can connect using the subpath.
	checkLeafNodeConnected(t, s)
	checkLeafNodeConnected(t, ln)

	// Add another leafnode that tries to connect to the subpath
	// but intercept the attempt for the test.
	o2 := testDefaultLeafNodeWSOptions()
	lo2 := testDefaultRemoteLeafNodeWSOptions(t, o2, false)
	attempts := make(chan string, 2)
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		attempts <- r.URL.String()
	}))
	defer ts.Close()
	u, _ := url.Parse(fmt.Sprintf("%v/some/path", ts.URL))
	u.Scheme = "ws"
	lo2.LeafNode.Remotes = []*RemoteLeafOpts{
		{
			URLs: []*url.URL{u},
		},
	}
	ln2 := RunServer(lo2)
	defer ln2.Shutdown()

	expected := "/some/path/leafnode"
	select {
	case got := <-attempts:
		if got != expected {
			t.Fatalf("Expected: %v, got: %v", expected, got)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Timed out waiting for leaf ws connect attempt")
	}
}

func TestLeafNodeWSFailedConnection(t *testing.T) {
	o := testDefaultLeafNodeWSOptions()
	s := RunServer(o)
	defer s.Shutdown()

	lo := testDefaultRemoteLeafNodeWSOptions(t, o, true)
	lo.LeafNode.ReconnectInterval = 100 * time.Millisecond
	ln := RunServer(lo)
	defer ln.Shutdown()

	el := &captureErrorLogger{errCh: make(chan string, 100)}
	ln.SetLogger(el, false, false)

	select {
	case err := <-el.errCh:
		if !strings.Contains(err, "Error soliciting websocket connection") {
			t.Fatalf("Unexpected error: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("No error reported!")
	}
	ln.Shutdown()
	s.Shutdown()

	lst, err := natsListen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("Error starting listener: %v", err)
	}
	defer lst.Close()

	wg := sync.WaitGroup{}
	wg.Add(2)

	go func() {
		defer wg.Done()

		for i := 0; i < 10; i++ {
			c, err := lst.Accept()
			if err != nil {
				return
			}
			time.Sleep(time.Duration(rand.Uint32N(100)) * time.Millisecond)
			if rand.Uint32N(2) == 1 {
				c.Write([]byte("something\r\n"))
			}
			c.Close()
		}
	}()

	time.Sleep(100 * time.Millisecond)

	port := lst.Addr().(*net.TCPAddr).Port
	u, _ := url.Parse(fmt.Sprintf("ws://127.0.0.1:%d", port))
	lo = DefaultOptions()
	lo.LeafNode.Remotes = []*RemoteLeafOpts{{URLs: []*url.URL{u}}}
	lo.LeafNode.ReconnectInterval = 10 * time.Millisecond
	ln, _ = NewServer(lo)
	el = &captureErrorLogger{errCh: make(chan string, 100)}
	ln.SetLogger(el, false, false)

	go func() {
		ln.Start()
		wg.Done()
	}()

	timeout := time.NewTimer(time.Second)
	for i := 0; i < 10; i++ {
		select {
		case err := <-el.errCh:
			if !strings.Contains(err, "Error soliciting") {
				t.Fatalf("Unexpected error: %v", err)
			}
		case <-timeout.C:
			t.Fatal("No error reported!")
		}
	}
	ln.Shutdown()
	lst.Close()
	wg.Wait()
}

func TestLeafNodeWSAuth(t *testing.T) {
	template := `
		port: -1
		authorization {
			users [
				{user: "user", pass: "puser", connection_types: ["%s"]}
				{user: "leaf", pass: "pleaf", connection_types: ["%s"%s]}
			]
		}
		websocket {
			port: -1
			no_tls: true
		}
		leafnodes {
			port: -1
		}
	`
	s, o, conf := runReloadServerWithContent(t,
		[]byte(fmt.Sprintf(template, jwt.ConnectionTypeStandard, jwt.ConnectionTypeLeafnode, "")))
	defer s.Shutdown()

	l := &captureErrorLogger{errCh: make(chan string, 10)}
	s.SetLogger(l, false, false)

	lo := testDefaultRemoteLeafNodeWSOptions(t, o, false)
	u, _ := url.Parse(fmt.Sprintf("ws://leaf:pleaf@127.0.0.1:%d", o.Websocket.Port))
	remote := &RemoteLeafOpts{URLs: []*url.URL{u}}
	lo.LeafNode.Remotes = []*RemoteLeafOpts{remote}
	lo.LeafNode.ReconnectInterval = 50 * time.Millisecond
	ln := RunServer(lo)
	defer ln.Shutdown()

	var lasterr string
	tm := time.NewTimer(2 * time.Second)
	for done := false; !done; {
		select {
		case lasterr = <-l.errCh:
			if strings.Contains(lasterr, "authentication") {
				done = true
			}
		case <-tm.C:
			t.Fatalf("Expected auth error, got %v", lasterr)
		}
	}

	ws := fmt.Sprintf(`, "%s"`, jwt.ConnectionTypeLeafnodeWS)
	reloadUpdateConfig(t, s, conf, fmt.Sprintf(template,
		jwt.ConnectionTypeStandard, jwt.ConnectionTypeLeafnode, ws))

	checkLeafNodeConnected(t, s)
	checkLeafNodeConnected(t, ln)

	nc1 := natsConnect(t, fmt.Sprintf("nats://user:puser@127.0.0.1:%d", o.Port))
	defer nc1.Close()

	sub := natsSubSync(t, nc1, "foo")
	natsFlush(t, nc1)

	checkSubInterest(t, ln, globalAccountName, "foo", time.Second)

	nc2 := natsConnect(t, ln.ClientURL())
	defer nc2.Close()

	natsPub(t, nc2, "foo", []byte("msg1"))
	msg := natsNexMsg(t, sub, time.Second)

	if md := string(msg.Data); md != "msg1" {
		t.Fatalf("Invalid message: %q", md)
	}
}

func TestLeafNodeWSGossip(t *testing.T) {
	o1 := testDefaultLeafNodeWSOptions()
	s1 := RunServer(o1)
	defer s1.Shutdown()

	// Now connect from a server that knows only about s1
	lo := testDefaultRemoteLeafNodeWSOptions(t, o1, false)
	lo.LeafNode.ReconnectInterval = 15 * time.Millisecond
	ln := RunServer(lo)
	defer ln.Shutdown()

	checkLeafNodeConnected(t, s1)
	checkLeafNodeConnected(t, ln)

	// Now add a routed server to s1
	o2 := testDefaultLeafNodeWSOptions()
	o2.Routes = RoutesFromStr(fmt.Sprintf("nats://127.0.0.1:%d", o1.Cluster.Port))
	s2 := RunServer(o2)
	defer s2.Shutdown()

	// Wait for cluster to form
	checkClusterFormed(t, s1, s2)

	// Now shutdown s1 and check that ln is able to reconnect to s2.
	s1.Shutdown()

	checkLeafNodeConnected(t, s2)
	checkLeafNodeConnected(t, ln)

	// Make sure that the reconnection was as a WS connection, not simply to
	// the regular LN port.
	var s2lc *client
	s2.mu.Lock()
	for _, l := range s2.leafs {
		s2lc = l
		break
	}
	s2.mu.Unlock()

	s2lc.mu.Lock()
	isWS := s2lc.isWebsocket()
	s2lc.mu.Unlock()

	if !isWS {
		t.Fatal("Leafnode connection is not websocket!")
	}
}

// This test was showing an issue if one set maxBufSize to very small value,
// such as maxBufSize = 10. With such small value, we would get a corruption
// in that LMSG would arrive with missing bytes. We are now always making
// a copy when dealing with messages that are bigger than maxBufSize.
func TestLeafNodeWSNoBufferCorruption(t *testing.T) {
	o := testDefaultLeafNodeWSOptions()
	s := RunServer(o)
	defer s.Shutdown()

	lo1 := testDefaultRemoteLeafNodeWSOptions(t, o, false)
	lo1.LeafNode.ReconnectInterval = 15 * time.Millisecond
	ln1 := RunServer(lo1)
	defer ln1.Shutdown()

	lo2 := DefaultOptions()
	lo2.Cluster.Name = "LN"
	lo2.Routes = RoutesFromStr(fmt.Sprintf("nats://127.0.0.1:%d", lo1.Cluster.Port))
	ln2 := RunServer(lo2)
	defer ln2.Shutdown()

	checkClusterFormed(t, ln1, ln2)

	checkLeafNodeConnected(t, s)
	checkLeafNodeConnected(t, ln1)

	nc := natsConnect(t, s.ClientURL())
	defer nc.Close()
	sub := natsSubSync(t, nc, "foo")

	nc1 := natsConnect(t, ln1.ClientURL())
	defer nc1.Close()

	nc2 := natsConnect(t, ln2.ClientURL())
	defer nc2.Close()
	sub2 := natsSubSync(t, nc2, "foo")

	checkSubInterest(t, s, globalAccountName, "foo", time.Second)
	checkSubInterest(t, ln2, globalAccountName, "foo", time.Second)
	checkSubInterest(t, ln1, globalAccountName, "foo", time.Second)

	payload := make([]byte, 100*1024)
	for i := 0; i < len(payload); i++ {
		payload[i] = 'A'
	}
	natsPub(t, nc1, "foo", payload)

	checkMsgRcv := func(sub *nats.Subscription) {
		msg := natsNexMsg(t, sub, time.Second)
		if !bytes.Equal(msg.Data, payload) {
			t.Fatalf("Invalid message content: %q", msg.Data)
		}
	}
	checkMsgRcv(sub2)
	checkMsgRcv(sub)
}

func TestLeafNodeWSRemoteNoTLSBlockWithWSSProto(t *testing.T) {
	o := testDefaultLeafNodeWSOptions()
	o.Websocket.NoTLS = false
	tc := &TLSConfigOpts{
		CertFile: "../test/configs/certs/server-cert.pem",
		KeyFile:  "../test/configs/certs/server-key.pem",
		CaFile:   "../test/configs/certs/ca.pem",
	}
	tlsConf, err := GenTLSConfig(tc)
	if err != nil {
		t.Fatalf("Error generating TLS config: %v", err)
	}
	o.Websocket.TLSConfig = tlsConf
	s := RunServer(o)
	defer s.Shutdown()

	// The test will make sure that if the protocol is "wss://", a TLS handshake must
	// be initiated, regardless of the presence of a TLS config block in config file
	// or here directly.
	// A bug was causing the absence of TLS config block to initiate a non TLS connection
	// even if "wss://" proto was specified, which would lead to "invalid websocket connection"
	// errors in the log.
	// With the fix, the connection will fail because the remote will fail to verify
	// the root CA, but at least, we will make sure that this is not an "invalid websocket connection"

	u, _ := url.Parse(fmt.Sprintf("wss://127.0.0.1:%d/some/path", o.Websocket.Port))
	lo := DefaultOptions()
	lo.Cluster.Name = "LN"
	remote := &RemoteLeafOpts{URLs: []*url.URL{u}}
	lo.LeafNode.Remotes = []*RemoteLeafOpts{remote}
	lo.LeafNode.ReconnectInterval = 100 * time.Millisecond
	ln := RunServer(lo)
	defer ln.Shutdown()

	l := &captureErrorLogger{errCh: make(chan string, 10)}
	ln.SetLogger(l, false, false)

	select {
	case e := <-l.errCh:
		if strings.Contains(e, "invalid websocket connection") {
			t.Fatalf("The remote did not try to create a TLS connection: %v", e)
		}
		// OK!
		return
	case <-time.After(2 * time.Second):
		t.Fatal("Connection should fail")
	}
}

func TestLeafNodeWSNoAuthUser(t *testing.T) {
	conf := createConfFile(t, []byte(`
	port: -1
	accounts {
		A { users [ {user: a, password: a} ]}
		B { users [ {user: b, password: b} ]}
	}
	websocket {
		port: -1
		no_tls: true
		no_auth_user: a
	}
	leafnodes {
		port: -1
	}
	`))
	s, o := RunServerWithConfig(conf)
	defer s.Shutdown()

	nc1 := natsConnect(t, fmt.Sprintf("nats://a:a@127.0.0.1:%d", o.Port))
	defer nc1.Close()

	lconf := createConfFile(t, []byte(fmt.Sprintf(`
	port: -1
	accounts {
		A { users [ {user: a, password: a} ]}
		B { users [ {user: b, password: b} ]}
	}
	leafnodes {
		remotes [
			{
				url: "ws://127.0.0.1:%d"
				account: A
			}
		]
	}
	`, o.Websocket.Port)))

	ln, lo := RunServerWithConfig(lconf)
	defer ln.Shutdown()

	checkLeafNodeConnected(t, s)
	checkLeafNodeConnected(t, ln)

	nc2 := natsConnect(t, fmt.Sprintf("nats://a:a@127.0.0.1:%d", lo.Port))
	defer nc2.Close()

	sub := natsSubSync(t, nc2, "foo")
	natsFlush(t, nc2)

	checkSubInterest(t, s, "A", "foo", time.Second)

	natsPub(t, nc1, "foo", []byte("msg1"))
	msg := natsNexMsg(t, sub, time.Second)

	if md := string(msg.Data); md != "msg1" {
		t.Fatalf("Invalid message: %q", md)
	}
}

func TestLeafNodeStreamImport(t *testing.T) {
	o1 := DefaultOptions()
	o1.LeafNode.Port = -1
	accA := NewAccount("A")
	o1.Accounts = []*Account{accA}
	o1.Users = []*User{{Username: "a", Password: "a", Account: accA}}
	o1.LeafNode.Account = "A"
	o1.NoAuthUser = "a"
	s1 := RunServer(o1)
	defer s1.Shutdown()

	o2 := DefaultOptions()
	o2.LeafNode.Port = -1
	o2.Cluster.Name = "xyz"

	accB := NewAccount("B")
	if err := accB.AddStreamExport(">", nil); err != nil {
		t.Fatalf("Error adding stream export: %v", err)
	}

	accC := NewAccount("C")
	if err := accC.AddStreamImport(accB, ">", ""); err != nil {
		t.Fatalf("Error adding stream import: %v", err)
	}

	o2.Accounts = []*Account{accB, accC}
	o2.Users = []*User{{Username: "b", Password: "b", Account: accB}, {Username: "c", Password: "c", Account: accC}}
	o2.NoAuthUser = "b"
	u, err := url.Parse(fmt.Sprintf("nats://127.0.0.1:%d", o1.LeafNode.Port))
	if err != nil {
		t.Fatalf("Error parsing url: %v", err)
	}
	o2.LeafNode.Remotes = []*RemoteLeafOpts{{URLs: []*url.URL{u}, LocalAccount: "C"}}
	s2 := RunServer(o2)
	defer s2.Shutdown()

	nc1 := natsConnect(t, s1.ClientURL())
	defer nc1.Close()

	sub := natsSubSync(t, nc1, "a")

	checkSubInterest(t, s2, "C", "a", time.Second)

	nc2 := natsConnect(t, s2.ClientURL())
	defer nc2.Close()

	natsPub(t, nc2, "a", []byte("hello?"))

	natsNexMsg(t, sub, time.Second)
}

func TestLeafNodeRouteSubWithOrigin(t *testing.T) {
	lo1 := DefaultOptions()
	lo1.LeafNode.Host = "127.0.0.1"
	lo1.LeafNode.Port = -1
	lo1.Cluster.Name = "local"
	lo1.Cluster.Host = "127.0.0.1"
	lo1.Cluster.Port = -1
	l1 := RunServer(lo1)
	defer l1.Shutdown()

	lo2 := DefaultOptions()
	lo2.LeafNode.Host = "127.0.0.1"
	lo2.LeafNode.Port = -1
	lo2.Cluster.Name = "local"
	lo2.Cluster.Host = "127.0.0.1"
	lo2.Cluster.Port = -1
	lo2.Routes = RoutesFromStr(fmt.Sprintf("nats://127.0.0.1:%d", lo1.Cluster.Port))
	l2 := RunServer(lo2)
	defer l2.Shutdown()

	checkClusterFormed(t, l1, l2)

	u1, _ := url.Parse(fmt.Sprintf("nats://127.0.0.1:%d", lo1.LeafNode.Port))
	urls := []*url.URL{u1}

	ro1 := DefaultOptions()
	ro1.Cluster.Name = "remote"
	ro1.Cluster.Host = "127.0.0.1"
	ro1.Cluster.Port = -1
	ro1.LeafNode.ReconnectInterval = 50 * time.Millisecond
	ro1.LeafNode.Remotes = []*RemoteLeafOpts{{URLs: urls}}
	r1 := RunServer(ro1)
	defer r1.Shutdown()

	checkLeafNodeConnected(t, r1)

	nc := natsConnect(t, r1.ClientURL(), nats.NoReconnect())
	defer nc.Close()
	natsSubSync(t, nc, "foo")
	natsQueueSubSync(t, nc, "bar", "baz")
	checkSubInterest(t, l2, globalAccountName, "foo", time.Second)
	checkSubInterest(t, l2, globalAccountName, "bar", time.Second)

	// Now shutdown the leafnode and check that any subscription for $G on l2 are gone.
	r1.Shutdown()
	checkFor(t, time.Second, 15*time.Millisecond, func() error {
		acc := l2.GlobalAccount()
		if n := acc.TotalSubs(); n != 5 {
			return fmt.Errorf("Account %q should have 5 subs, got %v", acc.GetName(), n)
		}
		return nil
	})
}

func TestLeafNodeLoopDetectionWithMultipleClusters(t *testing.T) {
	lo1 := DefaultOptions()
	lo1.LeafNode.Host = "127.0.0.1"
	lo1.LeafNode.Port = -1
	lo1.Cluster.Name = "local"
	lo1.Cluster.Host = "127.0.0.1"
	lo1.Cluster.Port = -1
	l1 := RunServer(lo1)
	defer l1.Shutdown()

	lo2 := DefaultOptions()
	lo2.LeafNode.Host = "127.0.0.1"
	lo2.LeafNode.Port = -1
	lo2.Cluster.Name = "local"
	lo2.Cluster.Host = "127.0.0.1"
	lo2.Cluster.Port = -1
	lo2.Routes = RoutesFromStr(fmt.Sprintf("nats://127.0.0.1:%d", lo1.Cluster.Port))
	l2 := RunServer(lo2)
	defer l2.Shutdown()

	checkClusterFormed(t, l1, l2)

	ro1 := DefaultOptions()
	ro1.Cluster.Name = "remote"
	ro1.Cluster.Host = "127.0.0.1"
	ro1.Cluster.Port = -1
	ro1.LeafNode.ReconnectInterval = 50 * time.Millisecond
	ro1.LeafNode.Remotes = []*RemoteLeafOpts{{URLs: []*url.URL{
		{Scheme: "nats", Host: fmt.Sprintf("127.0.0.1:%d", lo1.LeafNode.Port)},
		{Scheme: "nats", Host: fmt.Sprintf("127.0.0.1:%d", lo2.LeafNode.Port)},
	}}}
	r1 := RunServer(ro1)
	defer r1.Shutdown()

	l := &captureErrorLogger{errCh: make(chan string, 100)}
	r1.SetLogger(l, false, false)

	ro2 := DefaultOptions()
	ro2.Cluster.Name = "remote"
	ro2.Cluster.Host = "127.0.0.1"
	ro2.Cluster.Port = -1
	ro2.Routes = RoutesFromStr(fmt.Sprintf("nats://127.0.0.1:%d", ro1.Cluster.Port))
	ro2.LeafNode.ReconnectInterval = 50 * time.Millisecond
	ro2.LeafNode.Remotes = []*RemoteLeafOpts{{URLs: []*url.URL{
		{Scheme: "nats", Host: fmt.Sprintf("127.0.0.1:%d", lo1.LeafNode.Port)},
		{Scheme: "nats", Host: fmt.Sprintf("127.0.0.1:%d", lo2.LeafNode.Port)},
	}}}
	r2 := RunServer(ro2)
	defer r2.Shutdown()

	checkClusterFormed(t, r1, r2)
	checkLeafNodeConnected(t, r1)
	checkLeafNodeConnected(t, r2)

	l1.Shutdown()

	// Now wait for r1 and r2 to reconnect, they should not have a problem of loop detection.
	checkLeafNodeConnected(t, r1)
	checkLeafNodeConnected(t, r2)

	// Wait and make sure we don't have a loop error
	timeout := time.NewTimer(500 * time.Millisecond)
	for {
		select {
		case err := <-l.errCh:
			if strings.Contains(err, "Loop detected") {
				t.Fatal(err)
			}
		case <-timeout.C:
			// OK, we are done.
			return
		}
	}
}

func TestLeafNodeUnsubOnRouteDisconnect(t *testing.T) {
	lo1 := DefaultOptions()
	lo1.LeafNode.Host = "127.0.0.1"
	lo1.LeafNode.Port = -1
	lo1.Cluster.Name = "local"
	lo1.Cluster.Host = "127.0.0.1"
	lo1.Cluster.Port = -1
	l1 := RunServer(lo1)
	defer l1.Shutdown()

	lo2 := DefaultOptions()
	lo2.LeafNode.Host = "127.0.0.1"
	lo2.LeafNode.Port = -1
	lo2.Cluster.Name = "local"
	lo2.Cluster.Host = "127.0.0.1"
	lo2.Cluster.Port = -1
	lo2.Routes = RoutesFromStr(fmt.Sprintf("nats://127.0.0.1:%d", lo1.Cluster.Port))
	l2 := RunServer(lo2)
	defer l2.Shutdown()

	checkClusterFormed(t, l1, l2)

	u1, _ := url.Parse(fmt.Sprintf("nats://127.0.0.1:%d", lo1.LeafNode.Port))
	u2, _ := url.Parse(fmt.Sprintf("nats://127.0.0.1:%d", lo2.LeafNode.Port))
	urls := []*url.URL{u1, u2}

	ro1 := DefaultOptions()
	// DefaultOptions sets a cluster name, so make sure they are different.
	// Also, we don't have r1 and r2 clustered in this test, so set port to 0.
	ro1.Cluster.Name = _EMPTY_
	ro1.Cluster.Port = 0
	ro1.LeafNode.ReconnectInterval = 50 * time.Millisecond
	ro1.LeafNode.Remotes = []*RemoteLeafOpts{{URLs: urls}}
	r1 := RunServer(ro1)
	defer r1.Shutdown()

	ro2 := DefaultOptions()
	ro1.Cluster.Name = _EMPTY_
	ro2.Cluster.Port = 0
	ro2.LeafNode.ReconnectInterval = 50 * time.Millisecond
	// Have this one point only to l2
	ro2.LeafNode.Remotes = []*RemoteLeafOpts{{URLs: []*url.URL{u2}}}
	r2 := RunServer(ro2)
	defer r2.Shutdown()

	checkLeafNodeConnected(t, r1)
	checkLeafNodeConnected(t, r2)

	// Create a subscription on r1.
	nc := natsConnect(t, r1.ClientURL())
	defer nc.Close()
	sub := natsSubSync(t, nc, "foo")
	natsFlush(t, nc)

	checkSubInterest(t, l2, globalAccountName, "foo", time.Second)
	checkSubInterest(t, r2, globalAccountName, "foo", time.Second)

	nc2 := natsConnect(t, r2.ClientURL())
	defer nc2.Close()
	natsPub(t, nc, "foo", []byte("msg1"))

	// Check message received
	natsNexMsg(t, sub, time.Second)

	// Now shutdown l1, l2 should update subscription interest to r2.
	// When r1 reconnects to l2, subscription should be updated too.
	l1.Shutdown()

	// Wait a bit (so that the check of interest is not OK just because
	// the route would not have been yet detected as broken), and check
	// interest still present on r2, l2.
	time.Sleep(100 * time.Millisecond)
	checkSubInterest(t, l2, globalAccountName, "foo", time.Second)
	checkSubInterest(t, r2, globalAccountName, "foo", time.Second)

	// Check again that message received ok
	natsPub(t, nc, "foo", []byte("msg2"))
	natsNexMsg(t, sub, time.Second)

	// Now close client. Interest should disappear on r2. Due to a bug,
	// it was not.
	nc.Close()

	checkFor(t, time.Second, 15*time.Millisecond, func() error {
		acc := r2.GlobalAccount()
		if n := acc.Interest("foo"); n != 0 {
			return fmt.Errorf("Still interest on subject: %v", n)
		}
		return nil
	})
}

func TestLeafNodeNoPingBeforeConnect(t *testing.T) {
	o := DefaultOptions()
	o.LeafNode.Port = -1
	o.LeafNode.AuthTimeout = 0.5
	// For this test we need to disable compression, because we do use
	// the ping timer instead of the auth timer before the negotiation
	// is complete.
	o.LeafNode.Compression.Mode = CompressionOff
	s := RunServer(o)
	defer s.Shutdown()

	addr := fmt.Sprintf("127.0.0.1:%d", o.LeafNode.Port)
	c, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatalf("Error on dial: %v", err)
	}
	defer c.Close()

	// Read the info
	br := bufio.NewReader(c)
	c.SetReadDeadline(time.Now().Add(time.Second))
	l, _, err := br.ReadLine()
	if err != nil {
		t.Fatalf("Error on read: %v", err)
	}
	if !strings.HasPrefix(string(l), "INFO") {
		t.Fatalf("Wrong proto: %q", l)
	}

	var leaf *client
	checkFor(t, time.Second, 15*time.Millisecond, func() error {
		s.grMu.Lock()
		for _, l := range s.grTmpClients {
			leaf = l
			break
		}
		s.grMu.Unlock()
		if leaf == nil {
			return fmt.Errorf("No leaf connection found")
		}
		return nil
	})

	// Make sure that ping timer is not set
	leaf.mu.Lock()
	ptmrSet := leaf.ping.tmr != nil
	leaf.mu.Unlock()

	if ptmrSet {
		t.Fatal("Ping timer was set before CONNECT was processed")
	}

	// Send CONNECT
	if _, err := c.Write([]byte("CONNECT {}\r\n")); err != nil {
		t.Fatalf("Error writing connect: %v", err)
	}

	// Check that we correctly set the timer now
	checkFor(t, time.Second, 15*time.Millisecond, func() error {
		leaf.mu.Lock()
		ptmrSet := leaf.ping.tmr != nil
		leaf.mu.Unlock()
		if !ptmrSet {
			return fmt.Errorf("Timer still not set")
		}
		return nil
	})

	// Reduce the first ping..
	leaf.mu.Lock()
	leaf.ping.tmr.Reset(15 * time.Millisecond)
	leaf.mu.Unlock()

	// Now consume that PING (we may get LS+, etc..)
	for {
		c.SetReadDeadline(time.Now().Add(time.Second))
		l, _, err = br.ReadLine()
		if err != nil {
			t.Fatalf("Error on read: %v", err)
		}
		if strings.HasPrefix(string(l), "PING") {
			checkLeafNodeConnected(t, s)
			return
		}
	}
}

func TestLeafNodeNoMsgLoop(t *testing.T) {
	hubConf := `
		listen: "127.0.0.1:-1"
		accounts {
			FOO {
				users [
					{username: leaf, password: pass}
					{username: user, password: pass}
				]
			}
		}
		cluster {
			name: "hub"
			listen: "127.0.0.1:-1"
			%s
		}
		leafnodes {
			listen: "127.0.0.1:-1"
			authorization {
				account: FOO
			}
		}
	`
	configS1 := createConfFile(t, []byte(fmt.Sprintf(hubConf, "")))
	s1, o1 := RunServerWithConfig(configS1)
	defer s1.Shutdown()

	configS2S3 := createConfFile(t, []byte(fmt.Sprintf(hubConf, fmt.Sprintf(`routes: ["nats://127.0.0.1:%d"]`, o1.Cluster.Port))))
	s2, o2 := RunServerWithConfig(configS2S3)
	defer s2.Shutdown()

	s3, _ := RunServerWithConfig(configS2S3)
	defer s3.Shutdown()

	checkClusterFormed(t, s1, s2, s3)

	contentLN := `
		listen: "127.0.0.1:%d"
		accounts {
			FOO {
				users [
					{username: leaf, password: pass}
					{username: user, password: pass}
				]
			}
		}
		leafnodes {
			remotes = [
				{
					url: "nats://leaf:pass@127.0.0.1:%d"
					account: FOO
				}
			]
		}
	`
	lnconf := createConfFile(t, []byte(fmt.Sprintf(contentLN, -1, o1.LeafNode.Port)))
	sl1, slo1 := RunServerWithConfig(lnconf)
	defer sl1.Shutdown()

	sl2, slo2 := RunServerWithConfig(lnconf)
	defer sl2.Shutdown()

	checkLeafNodeConnected(t, sl1)
	checkLeafNodeConnected(t, sl2)

	// Create users on each leafnode
	nc1, err := nats.Connect(fmt.Sprintf("nats://user:pass@127.0.0.1:%d", slo1.Port))
	if err != nil {
		t.Fatalf("Error on connect: %v", err)
	}
	defer nc1.Close()

	rch := make(chan struct{}, 1)
	nc2, err := nats.Connect(
		fmt.Sprintf("nats://user:pass@127.0.0.1:%d", slo2.Port),
		nats.ReconnectWait(50*time.Millisecond),
		nats.ReconnectHandler(func(_ *nats.Conn) {
			rch <- struct{}{}
		}),
	)
	if err != nil {
		t.Fatalf("Error on connect: %v", err)
	}
	defer nc2.Close()

	// Create queue subs on sl2
	nc2.QueueSubscribe("foo", "bar", func(_ *nats.Msg) {})
	nc2.QueueSubscribe("foo", "bar", func(_ *nats.Msg) {})
	nc2.Flush()

	// Wait for interest to propagate to sl1
	checkSubInterest(t, sl1, "FOO", "foo", 250*time.Millisecond)

	// Create sub on sl1
	ch := make(chan *nats.Msg, 10)
	nc1.Subscribe("foo", func(m *nats.Msg) {
		select {
		case ch <- m:
		default:
		}
	})
	nc1.Flush()

	checkSubInterest(t, sl2, "FOO", "foo", 250*time.Millisecond)

	// Produce from sl1
	nc1.Publish("foo", []byte("msg1"))

	// Check message is received by plain sub
	select {
	case <-ch:
	case <-time.After(time.Second):
		t.Fatalf("Did not receive message")
	}

	// Restart leaf node, this time make sure we connect to 2nd server.
	sl2.Shutdown()

	// Use config file but this time reuse the client port and set the 2nd server for
	// the remote leaf node port.
	lnconf = createConfFile(t, []byte(fmt.Sprintf(contentLN, slo2.Port, o2.LeafNode.Port)))
	sl2, _ = RunServerWithConfig(lnconf)
	defer sl2.Shutdown()

	checkLeafNodeConnected(t, sl2)

	// Wait for client to reconnect
	select {
	case <-rch:
	case <-time.After(time.Second):
		t.Fatalf("Did not reconnect")
	}

	// Produce a new messages
	for i := 0; i < 10; i++ {
		nc1.Publish("foo", []byte(fmt.Sprintf("msg%d", 2+i)))

		// Check sub receives 1 message
		select {
		case <-ch:
		case <-time.After(time.Second):
			t.Fatalf("Did not receive message")
		}
		// Check that there is no more...
		select {
		case m := <-ch:
			t.Fatalf("Loop: received second message %s", m.Data)
		case <-time.After(50 * time.Millisecond):
			// OK
		}
	}
}

func TestLeafNodeInterestPropagationDaisychain(t *testing.T) {
	aTmpl := `
		port: %d
		leafnodes {
		  port: %d
		}
		`

	confA := createConfFile(t, []byte(fmt.Sprintf(aTmpl, -1, -1)))
	sA, _ := RunServerWithConfig(confA)
	defer sA.Shutdown()

	aPort := sA.opts.Port
	aLeafPort := sA.opts.LeafNode.Port

	confB := createConfFile(t, []byte(fmt.Sprintf(`
		port: -1
		leafnodes {
			port: -1
			remotes = [{
				url:"nats://127.0.0.1:%d"
			}]
		}`, aLeafPort)))
	sB, _ := RunServerWithConfig(confB)
	defer sB.Shutdown()

	confC := createConfFile(t, []byte(fmt.Sprintf(`
		port: -1
		leafnodes {
			port: -1
			remotes = [{url:"nats://127.0.0.1:%d"}]
		}`, sB.opts.LeafNode.Port)))
	sC, _ := RunServerWithConfig(confC)
	defer sC.Shutdown()

	checkLeafNodeConnectedCount(t, sC, 1)
	checkLeafNodeConnectedCount(t, sB, 2)
	checkLeafNodeConnectedCount(t, sA, 1)

	ncC := natsConnect(t, sC.ClientURL())
	defer ncC.Close()
	_, err := ncC.SubscribeSync("foo")
	require_NoError(t, err)
	require_NoError(t, ncC.Flush())

	checkSubInterest(t, sC, "$G", "foo", time.Second)
	checkSubInterest(t, sB, "$G", "foo", time.Second)
	checkSubInterest(t, sA, "$G", "foo", time.Second)

	ncA := natsConnect(t, sA.ClientURL())
	defer ncA.Close()

	sA.Shutdown()
	sA.WaitForShutdown()

	confAA := createConfFile(t, []byte(fmt.Sprintf(aTmpl, aPort, aLeafPort)))
	sAA, _ := RunServerWithConfig(confAA)
	defer sAA.Shutdown()

	checkLeafNodeConnectedCount(t, sAA, 1)
	checkLeafNodeConnectedCount(t, sB, 2)
	checkLeafNodeConnectedCount(t, sC, 1)

	checkSubInterest(t, sC, "$G", "foo", time.Second)
	checkSubInterest(t, sB, "$G", "foo", time.Second)
	checkSubInterest(t, sAA, "$G", "foo", time.Second) // failure issue 2448
}

func TestLeafNodeQueueGroupDistribution(t *testing.T) {
	hc := createClusterWithName(t, "HUB", 3)
	defer hc.shutdown()

	// Now have a cluster of leafnodes with each one connecting to corresponding HUB(n) node.
	c1 := `
	server_name: LEAF1
	listen: 127.0.0.1:-1
	cluster { name: ln22, listen: 127.0.0.1:-1 }
	leafnodes { remotes = [{ url: nats-leaf://127.0.0.1:%d }] }
	`
	lconf1 := createConfFile(t, []byte(fmt.Sprintf(c1, hc.opts[0].LeafNode.Port)))
	ln1, lopts1 := RunServerWithConfig(lconf1)
	defer ln1.Shutdown()

	c2 := `
	server_name: LEAF2
	listen: 127.0.0.1:-1
	cluster { name: ln22, listen: 127.0.0.1:-1, routes = [ nats-route://127.0.0.1:%d] }
	leafnodes { remotes = [{ url: nats-leaf://127.0.0.1:%d }] }
	`
	lconf2 := createConfFile(t, []byte(fmt.Sprintf(c2, lopts1.Cluster.Port, hc.opts[1].LeafNode.Port)))
	ln2, _ := RunServerWithConfig(lconf2)
	defer ln2.Shutdown()

	c3 := `
	server_name: LEAF3
	listen: 127.0.0.1:-1
	cluster { name: ln22, listen: 127.0.0.1:-1, routes = [ nats-route://127.0.0.1:%d] }
	leafnodes { remotes = [{ url: nats-leaf://127.0.0.1:%d }] }
	`
	lconf3 := createConfFile(t, []byte(fmt.Sprintf(c3, lopts1.Cluster.Port, hc.opts[2].LeafNode.Port)))
	ln3, _ := RunServerWithConfig(lconf3)
	defer ln3.Shutdown()

	// Check leaf cluster is formed and all connected to the HUB.
	lnServers := []*Server{ln1, ln2, ln3}
	checkClusterFormed(t, lnServers...)
	for _, s := range lnServers {
		checkLeafNodeConnected(t, s)
	}
	// Check each node in the hub has 1 connection from the leaf cluster.
	for i := 0; i < 3; i++ {
		checkLeafNodeConnectedCount(t, hc.servers[i], 1)
	}

	// Create a client and qsub on LEAF1 and LEAF2.
	nc1 := natsConnect(t, ln1.ClientURL())
	defer nc1.Close()
	var qsub1Count atomic.Int32
	natsQueueSub(t, nc1, "foo", "queue1", func(_ *nats.Msg) {
		qsub1Count.Add(1)
	})
	natsFlush(t, nc1)

	nc2 := natsConnect(t, ln2.ClientURL())
	defer nc2.Close()
	var qsub2Count atomic.Int32
	natsQueueSub(t, nc2, "foo", "queue1", func(_ *nats.Msg) {
		qsub2Count.Add(1)
	})
	natsFlush(t, nc2)

	// Make sure that the propagation interest is done before sending.
	for _, s := range hc.servers {
		gacc := s.GlobalAccount()
		checkFor(t, time.Second, 15*time.Millisecond, func() error {
			if n := gacc.Interest("foo"); n != 2 {
				return fmt.Errorf("Expected interest for %q to be 2, got %v", "foo", n)
			}
			return nil
		})
	}

	sendAndCheck := func(idx int) {
		t.Helper()
		nchub := natsConnect(t, hc.servers[idx].ClientURL())
		defer nchub.Close()
		total := 1000
		for i := 0; i < total; i++ {
			natsPub(t, nchub, "foo", []byte("from hub"))
		}
		checkFor(t, time.Second, 15*time.Millisecond, func() error {
			if trecv := int(qsub1Count.Load() + qsub2Count.Load()); trecv != total {
				return fmt.Errorf("Expected %v messages, got %v", total, trecv)
			}
			return nil
		})
		// Now that we have made sure that all messages were received,
		// check that qsub1 and qsub2 are getting at least some.
		if n := int(qsub1Count.Load()); n <= total/10 {
			t.Fatalf("Expected qsub1 to get some messages, but got %v", n)
		}
		if n := int(qsub2Count.Load()); n <= total/10 {
			t.Fatalf("Expected qsub2 to get some messages, but got %v", n)
		}
		// Reset the counters.
		qsub1Count.Store(0)
		qsub2Count.Store(0)
	}
	// Send from HUB1
	sendAndCheck(0)
	// Send from HUB2
	sendAndCheck(1)
	// Send from HUB3
	sendAndCheck(2)
}

func TestLeafNodeQueueGroupDistributionVariant(t *testing.T) {
	hc := createClusterWithName(t, "HUB", 3)
	defer hc.shutdown()

	// Now have a cluster of leafnodes with LEAF1 and LEAF2 connecting to HUB1.
	c1 := `
	server_name: LEAF1
	listen: 127.0.0.1:-1
	cluster { name: ln22, listen: 127.0.0.1:-1 }
	leafnodes { remotes = [{ url: nats-leaf://127.0.0.1:%d }] }
	`
	lconf1 := createConfFile(t, []byte(fmt.Sprintf(c1, hc.opts[0].LeafNode.Port)))
	ln1, lopts1 := RunServerWithConfig(lconf1)
	defer ln1.Shutdown()

	c2 := `
	server_name: LEAF2
	listen: 127.0.0.1:-1
	cluster { name: ln22, listen: 127.0.0.1:-1, routes = [ nats-route://127.0.0.1:%d] }
	leafnodes { remotes = [{ url: nats-leaf://127.0.0.1:%d }] }
	`
	lconf2 := createConfFile(t, []byte(fmt.Sprintf(c2, lopts1.Cluster.Port, hc.opts[0].LeafNode.Port)))
	ln2, _ := RunServerWithConfig(lconf2)
	defer ln2.Shutdown()

	// And LEAF3 to HUB3
	c3 := `
	server_name: LEAF3
	listen: 127.0.0.1:-1
	cluster { name: ln22, listen: 127.0.0.1:-1, routes = [ nats-route://127.0.0.1:%d] }
	leafnodes { remotes = [{ url: nats-leaf://127.0.0.1:%d }] }
	`
	lconf3 := createConfFile(t, []byte(fmt.Sprintf(c3, lopts1.Cluster.Port, hc.opts[2].LeafNode.Port)))
	ln3, _ := RunServerWithConfig(lconf3)
	defer ln3.Shutdown()

	// Check leaf cluster is formed and all connected to the HUB.
	lnServers := []*Server{ln1, ln2, ln3}
	checkClusterFormed(t, lnServers...)
	for _, s := range lnServers {
		checkLeafNodeConnected(t, s)
	}
	// Check that HUB1 has 2 leaf connections, HUB2 has 0 and HUB3 has 1.
	checkLeafNodeConnectedCount(t, hc.servers[0], 2)
	checkLeafNodeConnectedCount(t, hc.servers[1], 0)
	checkLeafNodeConnectedCount(t, hc.servers[2], 1)

	// Create a client and qsub on LEAF1 and LEAF2.
	nc1 := natsConnect(t, ln1.ClientURL())
	defer nc1.Close()
	var qsub1Count atomic.Int32
	natsQueueSub(t, nc1, "foo", "queue1", func(_ *nats.Msg) {
		qsub1Count.Add(1)
	})
	natsFlush(t, nc1)

	nc2 := natsConnect(t, ln2.ClientURL())
	defer nc2.Close()
	var qsub2Count atomic.Int32
	natsQueueSub(t, nc2, "foo", "queue1", func(_ *nats.Msg) {
		qsub2Count.Add(1)
	})
	natsFlush(t, nc2)

	// Make sure that the propagation interest is done before sending.
	for i, s := range hc.servers {
		gacc := s.GlobalAccount()
		var ei int
		switch i {
		case 0:
			ei = 2
		default:
			ei = 1
		}
		checkFor(t, time.Second, 15*time.Millisecond, func() error {
			if n := gacc.Interest("foo"); n != ei {
				return fmt.Errorf("Expected interest for %q to be %d, got %v", "foo", ei, n)
			}
			return nil
		})
	}

	sendAndCheck := func(idx int) {
		t.Helper()
		nchub := natsConnect(t, hc.servers[idx].ClientURL())
		defer nchub.Close()
		total := 1000
		for i := 0; i < total; i++ {
			natsPub(t, nchub, "foo", []byte("from hub"))
		}
		checkFor(t, time.Second, 15*time.Millisecond, func() error {
			if trecv := int(qsub1Count.Load() + qsub2Count.Load()); trecv != total {
				return fmt.Errorf("Expected %v messages, got %v", total, trecv)
			}
			return nil
		})
		// Now that we have made sure that all messages were received,
		// check that qsub1 and qsub2 are getting at least some.
		if n := int(qsub1Count.Load()); n <= total/10 {
			t.Fatalf("Expected qsub1 to get some messages, but got %v (qsub2=%v)", n, qsub2Count.Load())
		}
		if n := int(qsub2Count.Load()); n <= total/10 {
			t.Fatalf("Expected qsub2 to get some messages, but got %v (qsub1=%v)", n, qsub1Count.Load())
		}
		// Reset the counters.
		qsub1Count.Store(0)
		qsub2Count.Store(0)
	}
	// Send from HUB1
	sendAndCheck(0)
	// Send from HUB2
	sendAndCheck(1)
	// Send from HUB3
	sendAndCheck(2)
}

func TestLeafNodeQueueGroupDistributionWithDaisyChainAndGateway(t *testing.T) {
	SetGatewaysSolicitDelay(0)
	defer ResetGatewaysSolicitDelay()

	// We create a sort of a ladder of servers with connections that look like this:
	//
	// D1 <--- route ---> D2
	//  |                 |
	// GW                GW
	//  |                 |
	// C1 <--- route ---> C2
	//  |                 |
	// Leaf              Leaf
	//  |                 |
	// B1 <--- route ---> B2
	//  |                 |
	// Leaf              Leaf
	//  |                 |
	// A1 <--- route ---> A2
	//
	// We will then place queue subscriptions (different sub-tests) on A1, A2
	// B1, B2, D1 and D2.

	accs := `
		accounts {
			SYS: {users: [{user:sys, password: pwd}]}
			USER: {users: [{user:user, password: pwd}]}
		}
		system_account: SYS
	`
	dConf := `
		%s
		server_name: %s
		port: -1
		cluster {
			name: "D"
			port: -1
			%s
		}
		gateway {
			name: "D"
			port: -1
		}
	`
	d1Conf := createConfFile(t, []byte(fmt.Sprintf(dConf, accs, "GW1", _EMPTY_)))
	d1, d1Opts := RunServerWithConfig(d1Conf)
	defer d1.Shutdown()

	d2Conf := createConfFile(t, []byte(fmt.Sprintf(dConf, accs, "GW2",
		fmt.Sprintf("routes: [\"nats://127.0.0.1:%d\"]", d1Opts.Cluster.Port))))
	d2, d2Opts := RunServerWithConfig(d2Conf)
	defer d2.Shutdown()

	checkClusterFormed(t, d1, d2)

	leafCConf := `
		%s
		server_name: %s
		port: -1
		cluster {
			name: C
			port: -1
			%s
		}
		leafnodes {
			port: -1
		}
		gateway {
			name: C
			port: -1
			gateways [
				{
					name: D
					url: "nats://127.0.0.1:%d"
				}
			]
		}
	`
	c1Conf := createConfFile(t, []byte(fmt.Sprintf(leafCConf, accs, "C1", _EMPTY_, d1Opts.Gateway.Port)))
	c1, c1Opts := RunServerWithConfig(c1Conf)
	defer c1.Shutdown()

	waitForOutboundGateways(t, c1, 1, time.Second)
	waitForInboundGateways(t, d1, 1, time.Second)

	c2Conf := createConfFile(t, []byte(fmt.Sprintf(leafCConf, accs, "C2",
		fmt.Sprintf("routes: [\"nats://127.0.0.1:%d\"]", c1Opts.Cluster.Port), d2Opts.Gateway.Port)))
	c2, c2Opts := RunServerWithConfig(c2Conf)
	defer c2.Shutdown()

	waitForOutboundGateways(t, c2, 1, time.Second)
	waitForInboundGateways(t, d2, 1, time.Second)

	checkClusterFormed(t, c1, c2)

	leafABConf := `
		%s
		server_name: %s
		port: -1
		cluster {
			name: %s
			port: -1
			%s
		}
		leafnodes {
			port: -1
			remotes [
				{
					url: "nats://user:pwd@127.0.0.1:%d"
					account: USER
				}
			]
		}
	`
	b1Conf := createConfFile(t, []byte(fmt.Sprintf(leafABConf, accs, "B1", "B", _EMPTY_, c1Opts.LeafNode.Port)))
	b1, b1Opts := RunServerWithConfig(b1Conf)
	defer b1.Shutdown()

	checkLeafNodeConnected(t, b1)
	checkLeafNodeConnected(t, c1)

	b2Conf := createConfFile(t, []byte(fmt.Sprintf(leafABConf, accs, "B2", "B",
		fmt.Sprintf("routes: [\"nats://127.0.0.1:%d\"]", b1Opts.Cluster.Port), c2Opts.LeafNode.Port)))
	b2, b2Opts := RunServerWithConfig(b2Conf)
	defer b2.Shutdown()

	checkLeafNodeConnected(t, b2)
	checkLeafNodeConnected(t, c2)
	checkClusterFormed(t, b1, b2)

	a1Conf := createConfFile(t, []byte(fmt.Sprintf(leafABConf, accs, "A1", "A", _EMPTY_, b1Opts.LeafNode.Port)))
	a1, a1Opts := RunServerWithConfig(a1Conf)
	defer a1.Shutdown()

	checkLeafNodeConnectedCount(t, b1, 2)
	checkLeafNodeConnected(t, a1)

	a2Conf := createConfFile(t, []byte(fmt.Sprintf(leafABConf, accs, "A2", "A",
		fmt.Sprintf("routes: [\"nats://127.0.0.1:%d\"]", a1Opts.Cluster.Port), b2Opts.LeafNode.Port)))
	a2, _ := RunServerWithConfig(a2Conf)
	defer a2.Shutdown()

	checkLeafNodeConnectedCount(t, b2, 2)
	checkLeafNodeConnected(t, a2)
	checkClusterFormed(t, a1, a2)

	// Create our client connections to all servers where we may need to have
	// queue subscriptions.
	ncD1 := natsConnect(t, d1.ClientURL(), nats.UserInfo("user", "pwd"))
	defer ncD1.Close()
	ncD2 := natsConnect(t, d2.ClientURL(), nats.UserInfo("user", "pwd"))
	defer ncD2.Close()
	ncB1 := natsConnect(t, b1.ClientURL(), nats.UserInfo("user", "pwd"))
	defer ncB1.Close()
	ncB2 := natsConnect(t, b2.ClientURL(), nats.UserInfo("user", "pwd"))
	defer ncB2.Close()
	ncA1 := natsConnect(t, a1.ClientURL(), nats.UserInfo("user", "pwd"))
	defer ncA1.Close()
	ncA2 := natsConnect(t, a2.ClientURL(), nats.UserInfo("user", "pwd"))
	defer ncA2.Close()

	// Helper to check that the interest is propagated to all servers
	checkInterest := func(t *testing.T, subj string) {
		t.Helper()
		for _, s := range []*Server{a1, a2, b1, b2, c1, c2, d1, d2} {
			acc, err := s.LookupAccount("USER")
			require_NoError(t, err)
			checkFor(t, time.Second, 10*time.Millisecond, func() error {
				if acc.Interest(subj) != 0 {
					return nil
				}
				return fmt.Errorf("Still no interest on %q in server %q", subj, s)
			})
		}
	}

	// Helper to send messages on given subject. We are always sending
	// from cluster B in this test, but we pick randomly between B1 and B2.
	total := 1000
	send := func(t *testing.T, subj string) {
		for i := 0; i < total; i++ {
			var nc *nats.Conn
			if rand.Uint32N(2) == 0 {
				nc = ncB1
			} else {
				nc = ncB2
			}
			natsPub(t, nc, subj, []byte(fmt.Sprintf("msg_%d", i+1)))
		}
	}

	const queue = "queue"

	for i, test := range []struct {
		name string
		a1   bool
		a2   bool
		b1   bool
		b2   bool
		d1   bool
		d2   bool
	}{
		// Cases with QSubs in A, B and D
		{"A1 __ B1 __ D1 __", true, false, true, false, true, false},
		{"A1 __ B1 __ __ D2", true, false, true, false, false, true},
		{"A1 __ B1 __ D1 D2", true, false, true, false, true, true},

		{"A1 __ __ B2 D1 __", true, false, false, true, true, false},
		{"A1 __ __ B2 __ D2", true, false, false, true, false, true},
		{"A1 __ __ B2 D1 D2", true, false, false, true, true, true},

		{"A1 __ B1 B2 D1 __", true, false, true, true, true, false},
		{"A1 __ B1 B2 __ D2", true, false, true, true, false, true},
		{"A1 __ B1 B2 D1 D2", true, false, true, true, true, true},

		{"__ A2 B1 __ D1 __", false, true, true, false, true, false},
		{"__ A2 B1 __ __ D2", false, true, true, false, false, true},
		{"__ A2 B1 __ D1 D2", false, true, true, false, true, true},

		{"__ A2 __ B2 D1 __", false, true, false, true, true, false},
		{"__ A2 __ B2 __ D2", false, true, false, true, false, true},
		{"__ A2 __ B2 D1 D2", false, true, false, true, true, true},

		{"__ A2 B1 B2 D1 __", false, true, true, true, true, false},
		{"__ A2 B1 B2 __ D2", false, true, true, true, false, true},
		{"__ A2 B1 B2 D1 D2", false, true, true, true, true, true},

		{"A1 A2 B1 __ D1 __", true, true, true, false, true, false},
		{"A1 A2 B1 __ __ D2", true, true, true, false, false, true},
		{"A1 A2 B1 __ D1 D2", true, true, true, false, true, true},

		{"A1 A2 __ B2 D1 __", true, true, false, true, true, false},
		{"A1 A2 __ B2 __ D2", true, true, false, true, false, true},
		{"A1 A2 __ B2 D1 D2", true, true, false, true, true, true},

		{"A1 A2 B1 B2 D1 __", true, true, true, true, true, false},
		{"A1 A2 B1 B2 __ D2", true, true, true, true, false, true},
		{"A1 A2 B1 B2 D1 D2", true, true, true, true, true, true},

		// Now without any QSub in B cluster (so just A and D)
		{"A1 __ __ __ D1 __", true, false, false, false, true, false},
		{"A1 __ __ __ __ D2", true, false, false, false, false, true},
		{"A1 __ __ __ D1 D2", true, false, false, false, true, true},

		{"__ A2 __ __ D1 __", false, true, false, false, true, false},
		{"__ A2 __ __ __ D2", false, true, false, false, false, true},
		{"__ A2 __ __ D1 D2", false, true, false, false, true, true},

		{"A1 A2 __ __ D1 __", true, true, false, false, true, false},
		{"A1 A2 __ __ __ D2", true, true, false, false, false, true},
		{"A1 A2 __ __ D1 D2", true, true, false, false, true, true},
	} {
		t.Run(test.name, func(t *testing.T) {
			subj := fmt.Sprintf("foo.%d", i+1)
			var aCount, bCount, dCount atomic.Int32
			if test.a1 {
				qsA1 := natsQueueSub(t, ncA1, subj, queue, func(_ *nats.Msg) {
					aCount.Add(1)
				})
				defer qsA1.Unsubscribe()
			}
			if test.a2 {
				qsA2 := natsQueueSub(t, ncA2, subj, queue, func(_ *nats.Msg) {
					aCount.Add(1)
				})
				defer qsA2.Unsubscribe()
			}
			if test.b1 {
				qsB1 := natsQueueSub(t, ncB1, subj, queue, func(_ *nats.Msg) {
					bCount.Add(1)
				})
				defer qsB1.Unsubscribe()
			}
			if test.b2 {
				qsB2 := natsQueueSub(t, ncB2, subj, queue, func(_ *nats.Msg) {
					bCount.Add(1)
				})
				defer qsB2.Unsubscribe()
			}
			if test.d1 {
				qsD1 := natsQueueSub(t, ncD1, subj, queue, func(_ *nats.Msg) {
					dCount.Add(1)
				})
				defer qsD1.Unsubscribe()
			}
			if test.d2 {
				qsD2 := natsQueueSub(t, ncD2, subj, queue, func(_ *nats.Msg) {
					dCount.Add(1)
				})
				defer qsD2.Unsubscribe()
			}
			checkInterest(t, subj)

			// Now send messages
			send(t, subj)

			// Check that appropriate queue subs receive all messages.
			checkFor(t, 2*time.Second, 10*time.Millisecond, func() error {
				n := aCount.Load() + bCount.Load() + dCount.Load()
				if n == int32(total) {
					return nil
				}
				return fmt.Errorf("Got only %v/%v messages (a=%v b=%v d=%v)", n, total, aCount.Load(), bCount.Load(), dCount.Load())
			})
			// When there is (are) qsub(s) on b, then only B should
			// get the messages. Otherwise, it should be between A and D
			if test.b1 || test.b2 {
				require_Equal(t, aCount.Load(), 0)
				require_Equal(t, dCount.Load(), 0)
			} else {
				require_Equal(t, bCount.Load(), 0)
				// We should have receive some on A and D
				require_True(t, aCount.Load() > 0)
				require_True(t, dCount.Load() > 0)
			}
		})
	}
}

func TestLeafNodeAndGatewaysSingleMsgPerQueueGroup(t *testing.T) {
	SetGatewaysSolicitDelay(0)
	defer ResetGatewaysSolicitDelay()

	accs := `
		accounts {
			SYS: {users: [{user:sys, password: pwd}]}
			USER: {users: [{user:user, password: pwd}]}
		}
		system_account: SYS
	`
	gwUSConfTmpl := `
		%s
		server_name: GW_US
		listen: "127.0.0.1:-1"
		gateway {
			name: US
			listen: "127.0.0.1:-1"
		}
		leafnodes {
			listen: "127.0.0.1:-1"
		}
	`
	gwUSConf := createConfFile(t, []byte(fmt.Sprintf(gwUSConfTmpl, accs)))
	gwUS, gwUSOpts := RunServerWithConfig(gwUSConf)
	defer gwUS.Shutdown()

	gwEUConfTmpl := `
		%s
		server_name: GW_EU
		listen: "127.0.0.1:-1"
		gateway {
			name: EU
			listen: "127.0.0.1:-1"
			gateways [
				{
					name: US
					url: "nats://127.0.0.1:%d"
				}
			]
		}
		leafnodes {
			listen: "127.0.0.1:-1"
		}
	`
	gwEUConf := createConfFile(t, []byte(fmt.Sprintf(gwEUConfTmpl, accs, gwUSOpts.Gateway.Port)))
	gwEU, gwEUOpts := RunServerWithConfig(gwEUConf)
	defer gwEU.Shutdown()

	waitForOutboundGateways(t, gwUS, 1, time.Second)
	waitForOutboundGateways(t, gwEU, 1, time.Second)
	waitForInboundGateways(t, gwUS, 1, time.Second)
	waitForInboundGateways(t, gwEU, 1, time.Second)

	leafConfTmpl := `
		%s
		server_name: %s
		listen: "127.0.0.1:-1"
		leafnodes {
			remotes [
				{
					url: "nats://user:pwd@127.0.0.1:%d"
					account: USER
				}
			]
		}
	`
	leafUSConf := createConfFile(t, []byte(fmt.Sprintf(leafConfTmpl, accs, "LEAF_US", gwUSOpts.LeafNode.Port)))
	leafUS, _ := RunServerWithConfig(leafUSConf)
	defer leafUS.Shutdown()
	checkLeafNodeConnected(t, leafUS)

	leafEUConf := createConfFile(t, []byte(fmt.Sprintf(leafConfTmpl, accs, "LEAF_EU", gwEUOpts.LeafNode.Port)))
	leafEU, _ := RunServerWithConfig(leafEUConf)
	defer leafEU.Shutdown()
	checkLeafNodeConnected(t, leafEU)

	// Order is important! (see rest of test to understand why)
	var usLeafQ, usLeafPS, usGWQ, usGWPS, euGWQ, euGWPS, euLeafQ, euLeafPS, euLeafQBaz atomic.Int32
	counters := []*atomic.Int32{&usLeafQ, &usLeafPS, &usGWQ, &usGWPS, &euGWQ, &euGWPS, &euLeafQ, &euLeafPS, &euLeafQBaz}
	counterNames := []string{"usLeafQ", "usLeafPS", "usGWQ", "usGWPS", "euGWQ", "euGWPS", "euLeafQ", "euLeafPS", "euLeafQBaz"}
	if len(counters) != len(counterNames) {
		panic("Fix test!")
	}
	resetCounters := func() {
		for _, a := range counters {
			a.Store(0)
		}
	}

	// This test will always produce from the US leaf.
	ncProd := natsConnect(t, leafUS.ClientURL(), nats.UserInfo("user", "pwd"))
	defer ncProd.Close()

	total := int32(1)
	check := func(t *testing.T, expected []int32) {
		time.Sleep(50 * time.Millisecond)
		resetCounters()
		for i := 0; i < int(total); i++ {
			natsPub(t, ncProd, "foo.1", []byte("hello"))
		}
		checkFor(t, 2*time.Second, 15*time.Millisecond, func() error {
			for i := 0; i < len(expected); i++ {
				if n := counters[i].Load(); n != expected[i] {
					return fmt.Errorf("Expected counter %q to be %v, got %v", counterNames[i], expected[i], n)
				}
			}
			return nil
		})
	}

	// "usLeafQ", "usLeafPS", "usGWQ", "usGWPS", "euGWQ", "euGWPS", "euLeafQ", "euLeafPS", "euLeafQBaz"
	for _, test := range []struct {
		subs     []int
		expected []int32
	}{
		// We will always have the qsub on leaf EU, and have some permutations
		// of queue and plain subs on other server(s) and check we get the
		// expected distribution.

		// Simple test firs, qsubs on leaf US and leaf EU, all messages stay in leaf US.
		{
			[]int{1, 0, 0, 0, 0, 0, 1, 0, 0},
			[]int32{total, 0, 0, 0, 0, 0, 0, 0, 0},
		},
		// Move the queue sub from leaf US to GW US.
		{
			[]int{0, 0, 1, 0, 0, 0, 1, 0, 0},
			[]int32{0, 0, total, 0, 0, 0, 0, 0, 0},
		},
		// Now move it to GW EU.
		{
			[]int{0, 0, 0, 0, 1, 0, 1, 0, 0},
			[]int32{0, 0, 0, 0, total, 0, 0, 0, 0},
		},

		// More combinations...
		{
			[]int{1, 1, 0, 0, 0, 0, 1, 0, 0},
			[]int32{total, total, 0, 0, 0, 0, 0, 0, 0},
		},
		{
			[]int{0, 1, 1, 0, 0, 0, 1, 0, 0},
			[]int32{0, total, total, 0, 0, 0, 0, 0, 0},
		},
		{
			[]int{0, 1, 1, 1, 0, 0, 1, 0, 0},
			[]int32{0, total, total, total, 0, 0, 0, 0, 0},
		},
		{
			[]int{0, 1, 0, 1, 1, 0, 1, 0, 0},
			[]int32{0, total, 0, total, total, 0, 0, 0, 0},
		},
		{
			[]int{0, 1, 0, 1, 1, 1, 1, 0, 0},
			[]int32{0, total, 0, total, total, total, 0, 0, 0},
		},
		// If we have the qsub in leaf US, does not matter if we have
		// qsubs in GW US and EU, only leaf US should receive the messages,
		// but plain sub in GW servers should get them too.
		{
			[]int{1, 1, 1, 0, 0, 0, 1, 0, 0},
			[]int32{total, total, 0, 0, 0, 0, 0, 0, 0},
		},
		{
			[]int{1, 1, 1, 1, 0, 0, 1, 0, 0},
			[]int32{total, total, 0, total, 0, 0, 0, 0, 0},
		},
		{
			[]int{1, 1, 1, 1, 1, 0, 1, 0, 0},
			[]int32{total, total, 0, total, 0, 0, 0, 0, 0},
		},
		{
			[]int{1, 1, 1, 1, 1, 1, 1, 0, 0},
			[]int32{total, total, 0, total, 0, total, 0, 0, 0},
		},
		// Now back to a qsub on leaf US and leaf EU, but introduce plain sub
		// interest in leaf EU
		{
			[]int{1, 0, 0, 0, 0, 0, 1, 1, 0},
			[]int32{total, 0, 0, 0, 0, 0, 0, total, 0},
		},
		// And add a different queue group in leaf EU and it should get the messages too.
		{
			[]int{1, 0, 0, 0, 0, 0, 1, 1, 1},
			[]int32{total, 0, 0, 0, 0, 0, 0, total, total},
		},
		// Keep plain and baz queue sub interests in leaf EU and add more combinations.
		{
			[]int{1, 1, 0, 0, 0, 0, 1, 1, 1},
			[]int32{total, total, 0, 0, 0, 0, 0, total, total},
		},
		{
			[]int{1, 1, 1, 0, 0, 0, 1, 1, 1},
			[]int32{total, total, 0, 0, 0, 0, 0, total, total},
		},
		{
			[]int{1, 1, 1, 1, 0, 0, 1, 1, 1},
			[]int32{total, total, 0, total, 0, 0, 0, total, total},
		},
		{
			[]int{1, 1, 1, 1, 1, 0, 1, 1, 1},
			[]int32{total, total, 0, total, 0, 0, 0, total, total},
		},
		{
			[]int{1, 1, 1, 1, 1, 1, 1, 1, 1},
			[]int32{total, total, 0, total, 0, total, 0, total, total},
		},
	} {
		t.Run(_EMPTY_, func(t *testing.T) {
			if len(test.subs) != len(counters) || len(test.expected) != len(counters) {
				panic("Fix test")
			}

			ncUS := natsConnect(t, leafUS.ClientURL(), nats.UserInfo("user", "pwd"))
			defer ncUS.Close()

			ncGWUS := natsConnect(t, gwUS.ClientURL(), nats.UserInfo("user", "pwd"))
			defer ncGWUS.Close()

			ncGWEU := natsConnect(t, gwEU.ClientURL(), nats.UserInfo("user", "pwd"))
			defer ncGWEU.Close()

			ncEU := natsConnect(t, leafEU.ClientURL(), nats.UserInfo("user", "pwd"))
			defer ncEU.Close()

			if test.subs[0] > 0 {
				natsQueueSub(t, ncUS, "foo.*", "bar", func(_ *nats.Msg) {
					usLeafQ.Add(1)
				})
			}
			if test.subs[1] > 0 {
				natsSub(t, ncUS, "foo.>", func(_ *nats.Msg) {
					usLeafPS.Add(1)
				})
			}
			natsFlush(t, ncUS)
			if test.subs[2] > 0 {
				natsQueueSub(t, ncGWUS, "foo.*", "bar", func(_ *nats.Msg) {
					usGWQ.Add(1)
				})
			}
			if test.subs[3] > 0 {
				natsSub(t, ncGWUS, "foo.>", func(_ *nats.Msg) {
					usGWPS.Add(1)
				})
			}
			natsFlush(t, ncGWUS)
			if test.subs[4] > 0 {
				natsQueueSub(t, ncGWEU, "foo.*", "bar", func(_ *nats.Msg) {
					euGWQ.Add(1)
				})
			}
			if test.subs[5] > 0 {
				natsSub(t, ncGWEU, "foo.>", func(_ *nats.Msg) {
					euGWPS.Add(1)
				})
			}
			natsFlush(t, ncGWEU)
			if test.subs[6] > 0 {
				natsQueueSub(t, ncEU, "foo.*", "bar", func(_ *nats.Msg) {
					euLeafQ.Add(1)
				})
			}
			if test.subs[7] > 0 {
				natsSub(t, ncEU, "foo.>", func(_ *nats.Msg) {
					euLeafPS.Add(1)
				})
			}
			if test.subs[8] > 0 {
				// Create on different group, called "baz"
				natsQueueSub(t, ncEU, "foo.*", "baz", func(_ *nats.Msg) {
					euLeafQBaz.Add(1)
				})
			}
			natsFlush(t, ncEU)

			// Check that we have what we expect.
			check(t, test.expected)
		})
	}
}

func TestLeafNodeQueueGroupWeightCorrectOnConnectionCloseInSuperCluster(t *testing.T) {
	SetGatewaysSolicitDelay(0)
	defer ResetGatewaysSolicitDelay()

	//
	//          D
	//          |
	//         Leaf
	//          |
	//          v
	//          C
	//       ^    ^
	//      /       \
	//    GW         GW
	//   /             \
	//  v               \
	// B1 <--- route ---> B2 <----*----------*
	//  ^ <---*                   |          |
	//  |     |                 Leaf        Leaf
	// Leaf   *-- Leaf ---*       |          |
	//  |                 |       |          |
	// A1 <--- route ---> A2    OTHER1     OTHER2
	//

	accs := `
		accounts {
			SYS: {users: [{user:sys, password: pwd}]}
			USER: {users: [{user:user, password: pwd}]}
		}
		system_account: SYS
	`
	bConf := `
		%s
		server_name: %s
		listen: "127.0.0.1:-1"
		cluster {
			name: "B"
			listen: "127.0.0.1:-1"
			no_advertise: true
			%s
		}
		gateway {
			name: "B"
			listen: "127.0.0.1:-1"
		}
		leafnodes {
			listen: "127.0.0.1:-1"
		}
	`
	sb1Conf := createConfFile(t, []byte(fmt.Sprintf(bConf, accs, "B1", _EMPTY_)))
	sb1, sb1o := RunServerWithConfig(sb1Conf)
	defer sb1.Shutdown()

	sb2Conf := createConfFile(t, []byte(fmt.Sprintf(bConf, accs, "B2",
		fmt.Sprintf("routes: [\"nats://127.0.0.1:%d\"]", sb1o.Cluster.Port))))
	sb2, sb2o := RunServerWithConfig(sb2Conf)
	defer sb2.Shutdown()

	checkClusterFormed(t, sb1, sb2)

	cConf := `
		%s
		server_name: C
		listen: "127.0.0.1:-1"
		cluster {
			name: "C"
			listen: "127.0.0.1:-1"
		}
		gateway {
			name: "C"
			listen: "127.0.0.1:-1"
			gateways [
				{
					name: B
					url: "nats://127.0.0.1:%d"
				}
			]
		}
		leafnodes {
			listen: "127.0.0.1:-1"
		}
	`
	scConf := createConfFile(t, []byte(fmt.Sprintf(cConf, accs, sb1o.Gateway.Port)))
	sc, sco := RunServerWithConfig(scConf)
	defer sc.Shutdown()

	waitForOutboundGateways(t, sc, 1, 2*time.Second)
	waitForOutboundGateways(t, sb1, 1, 2*time.Second)
	waitForOutboundGateways(t, sb2, 1, 2*time.Second)
	waitForInboundGateways(t, sc, 2, 2*time.Second)
	waitForInboundGateways(t, sb1, 1, 2*time.Second)

	dConf := `
		%s
		server_name: D
		listen: "127.0.0.1:-1"
		cluster {
			name: "D"
			listen: "127.0.0.1:-1"
		}
		leafnodes {
			remotes [
				{
					url: "nats://user:pwd@127.0.0.1:%d"
					account: USER
				}
			]
		}
	`
	sdConf := createConfFile(t, []byte(fmt.Sprintf(dConf, accs, sco.LeafNode.Port)))
	sd, _ := RunServerWithConfig(sdConf)
	defer sd.Shutdown()

	checkLeafNodeConnected(t, sc)
	checkLeafNodeConnected(t, sd)

	aConf := `
		%s
		server_name: %s
		listen: "127.0.0.1:-1"
		cluster {
			name: A
			listen: "127.0.0.1:-1"
			no_advertise: true
			%s
		}
		leafnodes {
			reconnect: "10ms"
			remotes [
				{
					url: "nats://user:pwd@127.0.0.1:%d"
					account: USER
				}
			]
		}
	`
	a1Conf := createConfFile(t, []byte(fmt.Sprintf(aConf, accs, "A1", _EMPTY_, sb1o.LeafNode.Port)))
	sa1, sa1o := RunServerWithConfig(a1Conf)
	defer sa1.Shutdown()

	checkLeafNodeConnected(t, sa1)
	checkLeafNodeConnected(t, sb1)

	a2Conf := createConfFile(t, []byte(fmt.Sprintf(aConf, accs, "A2",
		fmt.Sprintf("routes: [\"nats://127.0.0.1:%d\"]", sa1o.Cluster.Port), sb1o.LeafNode.Port)))
	sa2, _ := RunServerWithConfig(a2Conf)
	defer sa2.Shutdown()

	checkClusterFormed(t, sa1, sa2)
	checkLeafNodeConnected(t, sa2)
	checkLeafNodeConnectedCount(t, sb1, 2)

	otherLeafsConf := `
		%s
		server_name: %s
		listen: "127.0.0.1:-1"
		leafnodes {
			remotes [
				{
					url: "nats://user:pwd@127.0.0.1:%d"
					account: USER
				}
			]
		}
	`
	o1Conf := createConfFile(t, []byte(fmt.Sprintf(otherLeafsConf, accs, "OTHERLEAF1", sb2o.LeafNode.Port)))
	so1, _ := RunServerWithConfig(o1Conf)
	defer so1.Shutdown()
	checkLeafNodeConnected(t, so1)
	checkLeafNodeConnectedCount(t, sb2, 1)

	o2Conf := createConfFile(t, []byte(fmt.Sprintf(otherLeafsConf, accs, "OTHERLEAF2", sb2o.LeafNode.Port)))
	so2, _ := RunServerWithConfig(o2Conf)
	defer so2.Shutdown()
	checkLeafNodeConnected(t, so2)
	checkLeafNodeConnectedCount(t, sb2, 2)

	// Helper to check that the interest is propagated to all servers
	checkInterest := func(t *testing.T, expected []int, expectedGW int64) {
		t.Helper()
		subj := "foo"
		for i, s := range []*Server{sa1, sa2, so1, so2, sb1, sb2, sc, sd} {
			if s == sc || !s.isRunning() {
				continue
			}
			acc, err := s.LookupAccount("USER")
			require_NoError(t, err)
			checkFor(t, 2*time.Second, 10*time.Millisecond, func() error {
				n := acc.Interest(subj)
				if n == expected[i] {
					return nil
				}
				return fmt.Errorf("Expected interest count for server %q to be %v, got %v", s, expected[i], n)
			})
		}
		// For server C, need to check in gateway's account.
		checkForRegisteredQSubInterest(t, sc, "B", "USER", "foo", expected[6], time.Second)

		// For server B1 and B2, check that we have the proper counts in the map.
		for _, s := range []*Server{sb1, sb2} {
			if !s.isRunning() {
				continue
			}
			checkFor(t, 2*time.Second, 10*time.Millisecond, func() error {
				s.gateway.pasi.Lock()
				accMap := s.gateway.pasi.m
				st := accMap["USER"]
				var n int64
				entry, ok := st["foo bar"]
				if ok {
					n = entry.n
				}
				s.gateway.pasi.Unlock()
				if n == expectedGW {
					return nil
				}
				return fmt.Errorf("Expected GW interest count for server %q to be %v, got %v", s, expectedGW, n)
			})
		}
	}

	ncA1 := natsConnect(t, sa1.ClientURL(), nats.UserInfo("user", "pwd"))
	defer ncA1.Close()
	for i := 0; i < 3; i++ {
		natsQueueSubSync(t, ncA1, "foo", "bar")
	}
	natsFlush(t, ncA1)
	// With 3 queue subs on A1, we should have for servers (in order checked in checkInterest)
	// for A1: 3 locals, for all others, 1 for the remote sub from A1.
	// B1 and B2 GW map will be 3 (1 for each sub)
	checkInterest(t, []int{3, 1, 1, 1, 1, 1, 1, 1}, 3)

	ncA2 := natsConnect(t, sa2.ClientURL(), nats.UserInfo("user", "pwd"))
	defer ncA2.Close()
	ncA2qsub1 := natsQueueSubSync(t, ncA2, "foo", "bar")
	ncA2qsub2 := natsQueueSubSync(t, ncA2, "foo", "bar")
	natsFlush(t, ncA2)
	// A1 will have 1 more for remote sub, same for A2 (2 locals + 1 remote).
	// B1 will have 2 interest (1 per leaf connection)
	// B1 and B2 GW map goes to 5.
	checkInterest(t, []int{4, 3, 1, 1, 2, 1, 1, 1}, 5)

	ncOther1 := natsConnect(t, so1.ClientURL(), nats.UserInfo("user", "pwd"))
	defer ncOther1.Close()
	natsQueueSubSync(t, ncOther1, "foo", "bar")
	natsQueueSubSync(t, ncOther1, "foo", "bar")
	natsFlush(t, ncOther1)
	// A1, A2 will have one more because of routed interest
	// O1 will have 3 (2 locals + 1 for remote interest)
	// O2 has still 1 for remote interest
	// B1 has 1 more because of new leaf interest and B2 because of routed interest.
	// B1 and B2 GW map goes to 7.
	checkInterest(t, []int{5, 4, 3, 1, 3, 2, 1, 1}, 7)

	ncOther2 := natsConnect(t, so2.ClientURL(), nats.UserInfo("user", "pwd"))
	defer ncOther2.Close()
	natsQueueSubSync(t, ncOther2, "foo", "bar")
	natsFlush(t, ncOther2)
	// O2 1 more for local interest
	// B2 1 more for the new leaf interest
	// B1 and B2 GW map goes to 8.
	checkInterest(t, []int{5, 4, 3, 2, 3, 3, 1, 1}, 8)

	// Stop the server so1.
	so1.Shutdown()
	so1.WaitForShutdown()
	checkLeafNodeConnectedCount(t, sb2, 1)
	// Now check interest still valid, but wait a little bit to make sure that
	// even with the bug where we would send an RS- through the gateway, there
	// would be enough time for it to propagate before we check for interest.
	time.Sleep(250 * time.Millisecond)
	// O1 is stopped, so expect 0
	// B2 has 1 less because leaf connection went away.
	// B1 and B2 GW map goes down to 6.
	checkInterest(t, []int{5, 4, 0, 2, 3, 2, 1, 1}, 6)

	// Store server sa1.
	sa1.Shutdown()
	sa1.WaitForShutdown()
	checkLeafNodeConnectedCount(t, sb1, 1)
	time.Sleep(250 * time.Millisecond)
	// A1 and O1 are gone, so 0
	// A2 has 1 less due to loss of routed interest
	// B1 has 1 less because 1 leaf connection went away.
	// B1 and B2 GW map goes down to 3.
	checkInterest(t, []int{0, 3, 0, 2, 2, 2, 1, 1}, 3)

	// Now remove the queue subs from A2
	ncA2qsub1.Unsubscribe()
	natsFlush(t, ncA2)
	// A2 has 1 less
	checkInterest(t, []int{0, 2, 0, 2, 2, 2, 1, 1}, 2)

	ncA2qsub2.Unsubscribe()
	natsFlush(t, ncA2)
	// A2 has 1 (no more locals but still interest for O2).
	// O2 has 1 (no more for remote interest, only local).
	// B1, B2 has 1 less since no interest from any of its leaf connections.
	checkInterest(t, []int{0, 1, 0, 1, 1, 1, 1, 1}, 1)

	// Removing (closing connection) of the sub on O2 will remove
	// interest globally.
	ncOther2.Close()
	checkInterest(t, []int{0, 0, 0, 0, 0, 0, 0, 0}, 0)

	// Resubscribe now, and again, interest should be propagated.
	natsQueueSubSync(t, ncA2, "foo", "bar")
	natsFlush(t, ncA2)
	checkInterest(t, []int{0, 1, 0, 1, 1, 1, 1, 1}, 1)

	natsQueueSubSync(t, ncA2, "foo", "bar")
	natsFlush(t, ncA2)
	checkInterest(t, []int{0, 2, 0, 1, 1, 1, 1, 1}, 2)

	// Close the client connection that has the 2 queue subs.
	ncA2.Close()
	checkInterest(t, []int{0, 0, 0, 0, 0, 0, 0, 0}, 0)

	// Now we will test when a route is lost on a server that has gateway enabled
	// that we update counts properly.
	ncB2 := natsConnect(t, sb2.ClientURL(), nats.UserInfo("user", "pwd"))
	defer ncB2.Close()
	natsQueueSubSync(t, ncB2, "foo", "bar")
	natsQueueSubSync(t, ncB2, "foo", "bar")
	natsQueueSubSync(t, ncB2, "foo", "bar")
	natsFlush(t, ncB2)
	checkInterest(t, []int{0, 1, 0, 1, 1, 3, 1, 1}, 3)

	ncB1 := natsConnect(t, sb1.ClientURL(), nats.UserInfo("user", "pwd"))
	defer ncB1.Close()
	natsQueueSubSync(t, ncB1, "foo", "bar")
	natsQueueSubSync(t, ncB1, "foo", "bar")
	checkInterest(t, []int{0, 1, 0, 1, 3, 4, 1, 1}, 5)

	// Now shutdown B2
	sb2.Shutdown()
	sa1.WaitForShutdown()
	time.Sleep(250 * time.Millisecond)
	checkInterest(t, []int{0, 1, 0, 0, 2, 0, 1, 1}, 2)

	ncB1.Close()
	checkInterest(t, []int{0, 0, 0, 0, 0, 0, 0, 0}, 0)

	// Now we will create 2 qsubs to sa2 that is still running.
	ncA2 = natsConnect(t, sa2.ClientURL(), nats.UserInfo("user", "pwd"))
	defer ncA2.Close()
	qsub1 := natsQueueSubSync(t, ncA2, "foo", "bar")
	qsub2 := natsQueueSubSync(t, ncA2, "foo", "bar")
	natsFlush(t, ncA2)
	// sa1 is down, so 0, sa2 has 2 queue subs, so1 is down, so 0, so2 is up, so
	// 1 for remote interest, same for sb1. Server sb2 is down, so 0, then 1 for
	// remote interest for sc and sd. The expected tally for the gateway to "C"
	// should be 2.
	checkInterest(t, []int{0, 2, 0, 1, 1, 0, 1, 1}, 2)

	// Close the leaf connection between sb1 and sa2
	sa2.mu.Lock()
	for _, l := range sa2.leafs {
		l.mu.Lock()
		l.nc.Close()
		l.mu.Unlock()
	}
	sa2.mu.Unlock()
	time.Sleep(50 * time.Millisecond)
	checkLeafNodeConnected(t, sa2)
	// Should be the same counts
	checkInterest(t, []int{0, 2, 0, 1, 1, 0, 1, 1}, 2)

	// Unsubscribe one of the queue sub.
	qsub2.Unsubscribe()
	natsFlush(t, ncA2)
	time.Sleep(50 * time.Millisecond)
	// One less for the local interest on sa2 and 1 less for the expected GW count.
	checkInterest(t, []int{0, 1, 0, 1, 1, 0, 1, 1}, 1)

	// Verify that interest works by publishing from server "C".
	ncC := natsConnect(t, sc.ClientURL(), nats.UserInfo("user", "pwd"))
	defer ncC.Close()
	natsPub(t, ncC, "foo", []byte("hello"))

	// Message should be received by qsub1.
	natsNexMsg(t, qsub1, time.Second)

	// Now stop sa2 and instead start sb2.
	ncA2.Close()
	sa2.Shutdown()
	sa2.WaitForShutdown()

	// Wait for counts to go down to 0
	checkInterest(t, []int{0, 0, 0, 0, 0, 0, 0, 0}, 0)

	// Restart sb2 to form route to sb1.
	sb2, _ = RunServerWithConfig(sb2Conf)
	defer sb2.Shutdown()

	checkClusterFormed(t, sb1, sb2)
	waitForOutboundGateways(t, sb2, 1, time.Second)

	// Create 2 queue subs on sb2.
	ncB2 = natsConnect(t, sb2.ClientURL(), nats.UserInfo("user", "pwd"))
	defer ncB2.Close()

	qsub1 = natsQueueSubSync(t, ncB2, "foo", "bar")
	qsub2 = natsQueueSubSync(t, ncB2, "foo", "bar")
	natsFlush(t, ncB2)
	// sa1 and sa2 are down, so 0, so1 is down, so 0, so2 is up, so 1 for remote interest,
	// same for sb1. Server sb2 has 2 local queue subs, so 2. Servers sc and sd have 1 for
	// remote interest. The expected tally for the gateway to "C" should be 2.
	checkInterest(t, []int{0, 0, 0, 1, 1, 2, 1, 1}, 2)

	// Now close the route(s) between sb1 and sb2.
	sb2.mu.Lock()
	sb2.forEachRoute(func(r *client) {
		r.mu.Lock()
		r.nc.Close()
		r.mu.Unlock()
	})
	sb2.mu.Unlock()
	time.Sleep(50 * time.Millisecond)
	checkClusterFormed(t, sb1, sb2)
	// Should be the same counts
	checkInterest(t, []int{0, 0, 0, 1, 1, 2, 1, 1}, 2)

	// Unsubscribe one of the queue sub.
	qsub2.Unsubscribe()
	natsFlush(t, ncB2)
	time.Sleep(50 * time.Millisecond)
	// One less for the local interest on sb2 and 1 less for the expected GW count.
	checkInterest(t, []int{0, 0, 0, 1, 1, 1, 1, 1}, 1)

	// Verify that interest works by publishing from server "C".
	natsPub(t, ncC, "foo", []byte("hello"))

	// Message should be received by qsub1.
	natsNexMsg(t, qsub1, time.Second)
}

func TestLeafNodeQueueInterestAndWeightCorrectAfterServerRestartOrConnectionClose(t *testing.T) {
	// Note that this is not what a normal configuration should be. Users should
	// configure each leafnode to have the URLs of both B1 and B2 so that when
	// a server fails, the leaf can reconnect to the other running server. But
	// we force it to be this way to demonstrate what the issue was and see that
	// it is now fixed.
	//
	// B1 <--- route ---> B2
	//  |                 |
	// Leaf              Leaf
	//  |                 |
	// A1 <--- route ---> A2
	//

	for _, test := range []struct {
		name          string
		pinnedAccount string
	}{
		{"without pinned account", _EMPTY_},
		{"with pinned account", "accounts: [\"A\"]"},
	} {
		t.Run(test.name, func(t *testing.T) {
			leafBConf := `
				accounts { A { users: [{user:a, password: pwd}] } }
				server_name: %s
				listen: "127.0.0.1:-1"
				cluster {
					name: HUB
					listen: "127.0.0.1:-1"
					%s
					%s
				}
				leafnodes {
					listen: "127.0.0.1:-1"
					no_advertise: true
				}
			`
			b1Conf := createConfFile(t, []byte(fmt.Sprintf(leafBConf, "B1", _EMPTY_, test.pinnedAccount)))
			b1, b1Opts := RunServerWithConfig(b1Conf)
			defer b1.Shutdown()

			b2Conf := createConfFile(t, []byte(fmt.Sprintf(leafBConf, "B2",
				fmt.Sprintf("routes: [\"nats://127.0.0.1:%d\"]", b1Opts.Cluster.Port), test.pinnedAccount)))
			b2, b2Opts := RunServerWithConfig(b2Conf)
			defer b2.Shutdown()

			checkClusterFormed(t, b1, b2)

			leafAConf := `
				accounts { A { users: [{user:a, password: pwd}] } }
				server_name: %s
				listen: "127.0.0.1:-1"
				cluster {
					name: LEAF
					listen: "127.0.0.1:-1"
					%s
					%s
				}
				leafnodes {
					listen: "127.0.0.1:-1"
					remotes: [
						{
							url: "nats://a:pwd@127.0.0.1:%d"
							account: A
						}
					]
					no_advertise: true
				}
			`
			a1Conf := createConfFile(t, []byte(fmt.Sprintf(leafAConf, "A1", _EMPTY_, test.pinnedAccount, b1Opts.LeafNode.Port)))
			a1, a1Opts := RunServerWithConfig(a1Conf)
			defer a1.Shutdown()

			checkLeafNodeConnected(t, b1)
			checkLeafNodeConnected(t, a1)

			a2Conf := createConfFile(t, []byte(fmt.Sprintf(leafAConf, "A2",
				fmt.Sprintf("routes: [\"nats://127.0.0.1:%d\"]", a1Opts.Cluster.Port), test.pinnedAccount, b2Opts.LeafNode.Port)))
			a2, _ := RunServerWithConfig(a2Conf)
			defer a2.Shutdown()

			checkLeafNodeConnected(t, b2)
			checkLeafNodeConnected(t, a2)
			checkClusterFormed(t, a1, a2)

			// Create a client on A2 and 3 queue subs.
			ncA2 := natsConnect(t, a2.ClientURL(), nats.UserInfo("a", "pwd"))
			defer ncA2.Close()

			var qsubs []*nats.Subscription
			for i := 0; i < 3; i++ {
				qsubs = append(qsubs, natsQueueSub(t, ncA2, "foo", "queue", func(_ *nats.Msg) {}))
			}
			natsFlush(t, ncA2)

			subj := "foo"
			checkInterest := func(expected bool) {
				t.Helper()
				for _, s := range []*Server{a1, a2, b1, b2} {
					acc, err := s.LookupAccount("A")
					require_NoError(t, err)
					checkFor(t, time.Second, 100*time.Millisecond, func() error {
						i := acc.Interest(subj)
						if expected && i == 0 {
							return fmt.Errorf("Still no interest on %q in server %q", subj, s)
						} else if !expected && i > 0 {
							return fmt.Errorf("Still interest on %q in server %q", subj, s)
						}
						return nil
					})
				}
			}
			checkInterest(true)

			// Check that Leafz from A1 (which connects to B1) has the expected sub
			// interest on "foo".
			checkLeafA1 := func(expected bool) {
				t.Helper()
				// We will wait a bit before checking Leafz since with the bug, it would
				// take a bit of time after the action to reproduce the issue for the
				// LS+ to be sent to the wrong cluster, or the interest to not be removed.
				time.Sleep(100 * time.Millisecond)
				// Now check Leafz
				leafsz, err := a1.Leafz(&LeafzOptions{Subscriptions: true})
				require_NoError(t, err)
				require_Equal(t, leafsz.NumLeafs, 1)
				require_True(t, leafsz.Leafs[0] != nil)
				lz := leafsz.Leafs[0]
				require_Equal(t, lz.Name, "B1")
				require_Equal(t, lz.NumSubs, uint32(len(lz.Subs)))
				var ok bool
				for _, sub := range lz.Subs {
					if sub == "foo" {
						if expected {
							ok = true
							break
						}
						t.Fatalf("Did not expect to have the %q subscription", sub)
					}
				}
				if expected && !ok {
					t.Fatalf("Expected to have the %q subscription", "foo")
				}
			}
			checkLeafA1(false)

			// Now restart server "B1". We need to create a conf file with the ports
			// that it used.
			restartBConf := createConfFile(t, []byte(fmt.Sprintf(`
				accounts { A { users: [{user:a, password: pwd}] } }
				server_name: B1
				listen: "127.0.0.1:%d"
				cluster {
					name: HUB
					listen: "127.0.0.1:%d"
					%s
				}
				leafnodes {
					listen: "127.0.0.1:%d"
					no_advertise: true
				}
			`, b1Opts.Port, b1Opts.Cluster.Port, test.pinnedAccount, b1Opts.LeafNode.Port)))
			b1.Shutdown()
			b1, _ = RunServerWithConfig(restartBConf)
			defer b1.Shutdown()

			checkLeafNodeConnected(t, b1)
			checkLeafNodeConnected(t, a1)

			// Stop one of the queue sub.
			qsubs[0].Unsubscribe()
			natsFlush(t, ncA2)

			// Check that "foo" does not show up in the subscription list
			// for the leaf from A1 to B1.
			checkLeafA1(false)

			// Now stop the other 2 and check again.
			qsubs[1].Unsubscribe()
			qsubs[2].Unsubscribe()
			natsFlush(t, ncA2)
			checkInterest(false)

			checkLeafA1(false)

			// Now recreate 3 queue subs.
			for i := 0; i < 3; i++ {
				natsQueueSub(t, ncA2, "foo", "queue", func(_ *nats.Msg) {})
			}
			// Check interest is present in all servers
			checkInterest(true)
			// But A1's leaf to B1 should still not have a sub interest for "foo".
			checkLeafA1(false)

			// Now stop the client connection instead of removing queue sub
			// one at a time. This will ensure that we properly handle an LS-
			// on B2 with an interest with a queue weight more than 1 still
			// present at the time of processing.
			ncA2.Close()
			checkInterest(false)

			checkLeafA1(false)

			// We will now test that if the queue subs are created on B2,
			// we have proper interest on A1, but when we close the connection,
			// the interest disappears.
			ncB2 := natsConnect(t, b2.ClientURL(), nats.UserInfo("a", "pwd"))
			defer ncB2.Close()

			for i := 0; i < 3; i++ {
				natsQueueSub(t, ncB2, "foo", "queue", func(_ *nats.Msg) {})
			}
			checkInterest(true)
			checkLeafA1(true)
			// Close the connection, so all queue subs should be removed at once.
			ncB2.Close()
			checkInterest(false)
			checkLeafA1(false)
		})
	}
}

func TestLeafNodeQueueWeightCorrectOnRestart(t *testing.T) {
	leafBConf := `
		server_name: %s
		listen: "127.0.0.1:-1"
		cluster {
			name: HUB
			listen: "127.0.0.1:-1"
			%s
		}
		leafnodes {
			listen: "127.0.0.1:-1"
			no_advertise: true
		}
	`
	b1Conf := createConfFile(t, []byte(fmt.Sprintf(leafBConf, "B1", _EMPTY_)))
	b1, b1Opts := RunServerWithConfig(b1Conf)
	defer b1.Shutdown()

	b2Conf := createConfFile(t, []byte(fmt.Sprintf(leafBConf, "B2",
		fmt.Sprintf("routes: [\"nats://127.0.0.1:%d\"]", b1Opts.Cluster.Port))))
	b2, b2Opts := RunServerWithConfig(b2Conf)
	defer b2.Shutdown()

	checkClusterFormed(t, b1, b2)

	leafAConf := `
		server_name: LEAF
		listen: "127.0.0.1:-1"
		leafnodes {
			remotes: [{url: "nats://127.0.0.1:%d"}]
			reconnect: "50ms"
		}
	`
	aConf := createConfFile(t, []byte(fmt.Sprintf(leafAConf, b2Opts.LeafNode.Port)))
	a, _ := RunServerWithConfig(aConf)
	defer a.Shutdown()

	checkLeafNodeConnected(t, b2)
	checkLeafNodeConnected(t, a)

	nc := natsConnect(t, a.ClientURL())
	defer nc.Close()

	for i := 0; i < 2; i++ {
		natsQueueSubSync(t, nc, "foo", "queue")
	}
	natsFlush(t, nc)

	checkQueueWeight := func() {
		for _, s := range []*Server{b1, b2} {
			gacc := s.GlobalAccount()
			gacc.mu.RLock()
			sl := gacc.sl
			gacc.mu.RUnlock()
			checkFor(t, time.Second, 10*time.Millisecond, func() error {
				// For remote queue interest, Match() will expand to queue weight.
				// So we should have 1 group and 2 queue subs present.
				res := sl.Match("foo")
				for _, qsubs := range res.qsubs {
					for _, sub := range qsubs {
						if string(sub.subject) == "foo" && string(sub.queue) == "queue" && atomic.LoadInt32(&sub.qw) == 2 {
							return nil
						}
					}
				}
				return fmt.Errorf("Server %q does not have expected queue interest with expected weight", s)
			})
		}
	}
	checkQueueWeight()

	// Now restart server "B2". We need to create a conf file with the ports
	// that it used.
	restartBConf := createConfFile(t, []byte(fmt.Sprintf(`
		server_name: B2
		listen: "127.0.0.1:%d"
		cluster {
			name: HUB
			listen: "127.0.0.1:%d"
			%s
		}
		leafnodes {
			listen: "127.0.0.1:%d"
			no_advertise: true
		}
	`, b2Opts.Port, b2Opts.Cluster.Port, fmt.Sprintf("routes: [\"nats://127.0.0.1:%d\"]", b1Opts.Cluster.Port), b2Opts.LeafNode.Port)))
	b2.Shutdown()
	b2, _ = RunServerWithConfig(restartBConf)
	defer b2.Shutdown()

	checkLeafNodeConnected(t, b2)
	checkLeafNodeConnected(t, a)
	checkQueueWeight()
}

func TestLeafNodeRoutedSubKeyDifferentBetweenLeafSubAndRoutedSub(t *testing.T) {
	for _, test := range []struct {
		name          string
		pinnedAccount string
		lnocu         bool
	}{
		{"without pinned account", _EMPTY_, true},
		{"with pinned account", "accounts: [\"XYZ\"]", true},
		{"old server without pinned account", _EMPTY_, false},
		{"old server with pinned account", "accounts: [\"XYZ\"]", false},
	} {
		t.Run(test.name, func(t *testing.T) {
			leafBConf := `
				accounts: {XYZ {users:[{user:a, password:pwd}]}}
				server_name: %s
				listen: "127.0.0.1:-1"
				cluster {
					name: HUB
					listen: "127.0.0.1:-1"
					%s
					%s
				}
				leafnodes {
					listen: "127.0.0.1:-1"
					no_advertise: true
				}
			`
			b1Conf := createConfFile(t, []byte(fmt.Sprintf(leafBConf, "B1", _EMPTY_, test.pinnedAccount)))
			b1, b1Opts := RunServerWithConfig(b1Conf)
			defer b1.Shutdown()

			b2Conf := createConfFile(t, []byte(fmt.Sprintf(leafBConf, "B2",
				fmt.Sprintf("routes: [\"nats://127.0.0.1:%d\"]", b1Opts.Cluster.Port), test.pinnedAccount)))
			b2, b2Opts := RunServerWithConfig(b2Conf)
			defer b2.Shutdown()

			checkClusterFormed(t, b1, b2)

			// To make route connections behave like if the server was connected
			// to an older server, change the routes' lnocu field.
			if !test.lnocu {
				for _, s := range []*Server{b1, b2} {
					s.mu.RLock()
					s.forEachRoute(func(r *client) {
						r.mu.Lock()
						r.route.lnocu = false
						r.mu.Unlock()
					})
					s.mu.RUnlock()
				}
			}

			// This leaf will have a cluster name that matches an account name.
			// The idea is to make sure that hub servers are not using incorrect
			// keys to differentiate a routed queue interest on subject "A" with
			// queue name "foo" for account "A"  in their cluster: "RS+ A A foo"
			// with a leafnode plain subscription, which since there is an origin
			// would be: "LS+ A A foo", that is, origin is "A", account is "A"
			// and subject is "foo".
			leafAConf := `
				accounts: {XYZ {users:[{user:a, password:pwd}]}}
				server_name: LEAF
				listen: "127.0.0.1:-1"
				cluster {
					name: XYZ
					listen: "127.0.0.1:-1"
				}
				leafnodes {
					remotes: [
						{
							url: "nats://a:pwd@127.0.0.1:%d"
							account: "XYZ"
						}
					]
				}
			`
			aConf := createConfFile(t, []byte(fmt.Sprintf(leafAConf, b2Opts.LeafNode.Port)))
			a, _ := RunServerWithConfig(aConf)
			defer a.Shutdown()

			checkLeafNodeConnected(t, b2)
			checkLeafNodeConnected(t, a)

			ncB2 := natsConnect(t, b2.ClientURL(), nats.UserInfo("a", "pwd"))
			defer ncB2.Close()
			// Create a plain sub on "foo"
			natsSubSync(t, ncB2, "foo")
			// And a queue sub on "XYZ" with queue name "foo"
			natsQueueSubSync(t, ncB2, "XYZ", "foo")
			natsFlush(t, ncB2)

			ncA := natsConnect(t, a.ClientURL(), nats.UserInfo("a", "pwd"))
			defer ncA.Close()
			// From the leafnode, create a plain sub on "foo"
			natsSubSync(t, ncA, "foo")
			// And a queue sub on "XYZ" with queue name "foo"
			natsQueueSubSync(t, ncA, "XYZ", "foo")
			natsFlush(t, ncA)

			// Check the acc.rm on B2
			b2Acc, err := b2.LookupAccount("XYZ")
			require_NoError(t, err)

			rsubKey := keyFromSubWithOrigin(&subscription{subject: []byte("foo")})
			rqsubKey := keyFromSubWithOrigin(&subscription{subject: []byte("XYZ"), queue: []byte("foo")})
			rlsubKey := keyFromSubWithOrigin(&subscription{origin: []byte("XYZ"), subject: []byte("foo")})
			rlqsubKey := keyFromSubWithOrigin(&subscription{origin: []byte("XYZ"), subject: []byte("XYZ"), queue: []byte("foo")})
			// Ensure all keys are different
			require_True(t, rsubKey != rqsubKey && rqsubKey != rlsubKey && rlsubKey != rlqsubKey)

			checkFor(t, time.Second, 10*time.Millisecond, func() error {
				b2Acc.mu.RLock()
				defer b2Acc.mu.RUnlock()
				for _, key := range []string{rsubKey, rqsubKey, rlsubKey, rlqsubKey} {
					v, ok := b2Acc.rm[key]
					if !ok {
						return fmt.Errorf("Did not find key %q for sub: %+v", key, sub)
					}
					if v != 1 {
						return fmt.Errorf("Key %q v=%v for sub: %+v", key, v, sub)
					}
				}
				return nil
			})

			// Now check that on B1, we have 2 distinct subs for the route.
			b1Acc, err := b1.LookupAccount("XYZ")
			require_NoError(t, err)

			var route *client

			if test.pinnedAccount == _EMPTY_ {
				b1Acc.mu.RLock()
				rIdx := b1Acc.routePoolIdx
				b1Acc.mu.RUnlock()
				b1.mu.RLock()
				b1.forEachRouteIdx(rIdx, func(r *client) bool {
					route = r
					return false
				})
				b1.mu.RUnlock()
			} else {
				b1.mu.RLock()
				remotes := b1.accRoutes["XYZ"]
				for _, r := range remotes {
					route = r
					break
				}
				b1.mu.RUnlock()
			}

			checkFor(t, time.Second, 10*time.Millisecond, func() error {
				// Check that route.subs has 4 entries for the subs we
				// created in this test.
				var entries []string
				route.mu.Lock()
				for key := range route.subs {
					if strings.Contains(key, "foo") {
						entries = append(entries, key)
					}
				}
				route.mu.Unlock()
				// With new servers, we expect 4 entries, but with older servers,
				// we have collisions and have only 2.
				var expected int
				if test.lnocu {
					expected = 4
				} else {
					expected = 2
				}
				if len(entries) != expected {
					return fmt.Errorf("Expected %d entries with %q, got this: %q", expected, "foo", entries)
				}
				return nil
			})

			// Close the connections and expect all gone.
			ncB2.Close()
			ncA.Close()

			checkFor(t, time.Second, 10*time.Millisecond, func() error {
				b2Acc.mu.RLock()
				defer b2Acc.mu.RUnlock()
				for _, key := range []string{rsubKey, rqsubKey, rlsubKey, rlqsubKey} {
					if _, ok := b2Acc.rm[key]; ok {
						return fmt.Errorf("Key %q still present", key)
					}
				}
				return nil
			})
			checkFor(t, time.Second, 10*time.Millisecond, func() error {
				var entries []string
				route.mu.Lock()
				for key := range route.subs {
					if strings.Contains(key, "foo") {
						entries = append(entries, key)
					}
				}
				route.mu.Unlock()
				if len(entries) != 0 {
					return fmt.Errorf("Still routed subscriptions on %q: %q", "foo", entries)
				}
				return nil
			})
		})
	}
}

// Connects a raw leaf to the given port, sends the given protocol and waits for the PONG.
func createRawLeafConn(t *testing.T, port int, proto string) (net.Conn, *bufio.Reader) {
	t.Helper()
	nc, err := net.Dial("tcp", fmt.Sprintf("127.0.0.1:%d", port))
	require_NoError(t, err)
	t.Cleanup(func() { nc.Close() })
	br := bufio.NewReader(nc)
	nc.SetReadDeadline(time.Now().Add(2 * time.Second))
	defer nc.SetReadDeadline(time.Time{})
	_, err = br.ReadString('\n')
	require_NoError(t, err)
	_, err = nc.Write([]byte("CONNECT {\"verbose\":false,\"pedantic\":false}\r\n" + proto + "PING\r\n"))
	require_NoError(t, err)
	for {
		l, err := br.ReadString('\n')
		require_NoError(t, err)
		if strings.HasPrefix(l, "PONG") {
			return nc, br
		}
	}
}

// Runs a two server hub cluster with leafnode listeners and optionally a gateway.
func createHubClusterWithLeafListeners(t *testing.T, gateway bool) (*Server, *Server, *Options) {
	t.Helper()
	conf := `
		server_name: %s
		listen: "127.0.0.1:-1"
		cluster {
			name: HUB
			listen: "127.0.0.1:-1"
			%s
		}
		leafnodes {
			listen: "127.0.0.1:-1"
		}
		%s
	`
	var gw string
	if gateway {
		gw = `gateway { name: HUB, listen: "127.0.0.1:-1" }
		accounts { SYS {} }
		system_account: SYS`
	}
	s1Conf := createConfFile(t, []byte(fmt.Sprintf(conf, "S1", _EMPTY_, gw)))
	s1, s1Opts := RunServerWithConfig(s1Conf)
	t.Cleanup(s1.Shutdown)

	s2Conf := createConfFile(t, []byte(fmt.Sprintf(conf, "S2",
		fmt.Sprintf("routes: [\"nats://127.0.0.1:%d\"]", s1Opts.Cluster.Port), gw)))
	s2, _ := RunServerWithConfig(s2Conf)
	t.Cleanup(s2.Shutdown)

	checkClusterFormed(t, s1, s2)
	return s1, s2, s1Opts
}

func TestLeafNodeQueueWeightRoutedMsgPicksLeafByWeight(t *testing.T) {
	s1, s2, _ := createHubClusterWithLeafListeners(t, false)

	// Two leafs on s2 with queue weights 9 and 1, and no local queue members.
	port := s2.getOpts().LeafNode.Port
	var received [2]atomic.Int64
	for i, proto := range []string{"LS+ foo Q1 9\r\n", "LS+ foo Q1 1\r\n"} {
		_, br := createRawLeafConn(t, port, proto)
		go func() {
			for {
				l, err := br.ReadString('\n')
				if err != nil {
					return
				}
				if strings.HasPrefix(l, "LMSG ") {
					received[i].Add(1)
				}
			}
		}()
	}

	// s1 sees the combined weight from s2 over the route.
	checkSubInterest(t, s1, globalAccountName, "foo", time.Second)

	// Messages published on s1 reach s2 over the route, where a leaf is picked by weight.
	nc := natsConnect(t, s1.ClientURL())
	defer nc.Close()
	const n = 2000
	for range n {
		natsPub(t, nc, "foo", []byte("x"))
	}
	natsFlush(t, nc)

	checkFor(t, 2*time.Second, 20*time.Millisecond, func() error {
		if total := received[0].Load() + received[1].Load(); total != n {
			return fmt.Errorf("expected %d messages, got %d", n, total)
		}
		return nil
	})
	if pct := float64(received[0].Load()) / n; pct < 0.85 || pct > 0.95 {
		t.Fatalf("Expected the leaf with weight 9 to get ~90%%, got %.2f%%", pct*100)
	}
}

func TestLeafNodeQueueWeightOverflowKeepsRouteInterest(t *testing.T) {
	s1, s2, s1Opts := createHubClusterWithLeafListeners(t, true)

	leafConn := func(proto string) net.Conn {
		t.Helper()
		nc, _ := createRawLeafConn(t, s1Opts.LeafNode.Port, proto)
		return nc
	}

	// Returns the weight total s1 tracks for the gateway queue interest.
	gatewayWeight := func(subj string) int64 {
		s1.gateway.pasi.Lock()
		defer s1.gateway.pasi.Unlock()
		if entry, ok := s1.gateway.pasi.m[globalAccountName][subj+" Q1"]; ok {
			return entry.n
		}
		return 0
	}

	// Returns the weight total s1 tracks for the leaf queue interest.
	totalWeight := func(subj string) int64 {
		key := keyFromSubWithOrigin(&subscription{leaf: true, subject: []byte(subj), queue: []byte("Q1")})
		acc := s1.GlobalAccount()
		acc.mu.RLock()
		defer acc.mu.RUnlock()
		return acc.rm[key]
	}

	// Returns the weight s2 received from s1 for the leaf queue interest.
	routeWeight := func(subj string) (qw int32, found bool) {
		s2.forEachRoute(func(r *client) {
			r.mu.Lock()
			defer r.mu.Unlock()
			for _, sub := range r.subs {
				if sub.leaf && string(sub.subject) == subj && string(sub.queue) == "Q1" {
					qw, found = atomic.LoadInt32(&sub.qw), true
				}
			}
		})
		return qw, found
	}

	checkWeights := func(subj string, total int64, expected int32) {
		t.Helper()
		checkFor(t, 2*time.Second, 20*time.Millisecond, func() error {
			if n := totalWeight(subj); n != total {
				return fmt.Errorf("expected total weight %d, got %d", total, n)
			}
			if n := gatewayWeight(subj); n != total {
				return fmt.Errorf("expected gateway weight %d, got %d", total, n)
			}
			qw, found := routeWeight(subj)
			if expected == 0 && found {
				return fmt.Errorf("expected no route interest, got weight %d", qw)
			} else if expected > 0 && qw != expected {
				return fmt.Errorf("expected route weight %d, got %d (found=%v)", expected, qw, found)
			}
			return nil
		})
	}

	// The total stays exact past int32 and the parseable weight limit, peers get the max weight.
	var conns []net.Conn
	for i := int64(1); i <= 3; i++ {
		conns = append(conns, leafConn("LS+ foo Q1 999999999\r\n"))
		checkWeights("foo", i*maxQueueWeight, maxQueueWeight)
	}

	// Interest is kept until the last leaf is gone.
	for i := int64(2); i >= 0; i-- {
		conns[i].Close()
		expected := int32(maxQueueWeight)
		if i == 0 {
			expected = 0
		}
		checkWeights("foo", i*maxQueueWeight, expected)
	}

	// A malformed weight counts as a single member.
	leafConn("LS+ bar Q1 1234567890\r\n")
	checkWeights("bar", 1, 1)
}

func TestLeafNodeQueueGroupWithLateLNJoin(t *testing.T) {
	/*

		Topology: A cluster of leafnodes LN2 and LN3, connect
		to a cluster C1, C2.

		sub(foo)     sub(foo)
		    \         /
		    C1  <->  C2
		    ^        ^
		    |        |
		    LN2 <-> LN3
		    /         \
		sub(foo)     sub(foo)

		Once the above is set, start LN1 that connects to C1.

		    sub(foo)     sub(foo)
		        \         /
		LN1 ->  C1  <->  C2
		        ^        ^
		        |        |
		        LN2 <-> LN3
		        /         \
		    sub(foo)     sub(foo)

		Remove subs to LN3, C2 and C1.

		LN1 ->  C1  <->  C2
		        ^        ^
		        |        |
		        LN2 <-> LN3
		        /
		    sub(foo)

		Publish from LN1 and verify message is received by sub on LN2.

		pub(foo)
		\
		LN1 -> C1  <->  C2
		        ^        ^
		        |        |
		        LN2 <-> LN3
		        /
		    sub(foo)
	*/
	co1 := DefaultOptions()
	co1.LeafNode.Host = "127.0.0.1"
	co1.LeafNode.Port = -1
	co1.Cluster.Name = "ngs"
	co1.Cluster.Host = "127.0.0.1"
	co1.Cluster.Port = -1
	c1 := RunServer(co1)
	defer c1.Shutdown()

	co2 := DefaultOptions()
	co2.LeafNode.Host = "127.0.0.1"
	co2.LeafNode.Port = -1
	co2.Cluster.Name = "ngs"
	co2.Cluster.Host = "127.0.0.1"
	co2.Cluster.Port = -1
	co2.Routes = RoutesFromStr(fmt.Sprintf("nats://127.0.0.1:%d", co1.Cluster.Port))
	c2 := RunServer(co2)
	defer c2.Shutdown()

	checkClusterFormed(t, c1, c2)

	lo2 := DefaultOptions()
	lo2.Cluster.Name = "local"
	lo2.Cluster.Host = "127.0.0.1"
	lo2.Cluster.Port = -1
	lo2.LeafNode.ReconnectInterval = 50 * time.Millisecond
	lo2.LeafNode.Remotes = []*RemoteLeafOpts{{URLs: []*url.URL{{Scheme: "nats", Host: fmt.Sprintf("127.0.0.1:%d", co1.LeafNode.Port)}}}}
	ln2 := RunServer(lo2)
	defer ln2.Shutdown()

	lo3 := DefaultOptions()
	lo3.Cluster.Name = "local"
	lo3.Cluster.Host = "127.0.0.1"
	lo3.Cluster.Port = -1
	lo3.Routes = RoutesFromStr(fmt.Sprintf("nats://127.0.0.1:%d", lo2.Cluster.Port))
	lo3.LeafNode.ReconnectInterval = 50 * time.Millisecond
	lo3.LeafNode.Remotes = []*RemoteLeafOpts{{URLs: []*url.URL{{Scheme: "nats", Host: fmt.Sprintf("127.0.0.1:%d", co2.LeafNode.Port)}}}}
	ln3 := RunServer(lo3)
	defer ln3.Shutdown()

	checkClusterFormed(t, ln2, ln3)
	checkLeafNodeConnected(t, ln2)
	checkLeafNodeConnected(t, ln3)

	cln2 := natsConnect(t, ln2.ClientURL())
	defer cln2.Close()
	sln2 := natsQueueSubSync(t, cln2, "foo", "qgroup")
	natsFlush(t, cln2)

	cln3 := natsConnect(t, ln3.ClientURL())
	defer cln3.Close()
	sln3 := natsQueueSubSync(t, cln3, "foo", "qgroup")
	natsFlush(t, cln3)

	cc1 := natsConnect(t, c1.ClientURL())
	defer cc1.Close()
	sc1 := natsQueueSubSync(t, cc1, "foo", "qgroup")
	natsFlush(t, cc1)

	cc2 := natsConnect(t, c2.ClientURL())
	defer cc2.Close()
	sc2 := natsQueueSubSync(t, cc2, "foo", "qgroup")
	natsFlush(t, cc2)

	checkSubInterest(t, c1, globalAccountName, "foo", time.Second)
	checkSubInterest(t, c2, globalAccountName, "foo", time.Second)
	checkSubInterest(t, ln2, globalAccountName, "foo", time.Second)
	checkSubInterest(t, ln3, globalAccountName, "foo", time.Second)

	lo1 := DefaultOptions()
	lo1.LeafNode.ReconnectInterval = 50 * time.Millisecond
	lo1.LeafNode.Remotes = []*RemoteLeafOpts{{URLs: []*url.URL{{Scheme: "nats", Host: fmt.Sprintf("127.0.0.1:%d", co1.LeafNode.Port)}}}}
	ln1 := RunServer(lo1)
	defer ln1.Shutdown()

	checkLeafNodeConnected(t, ln1)
	checkSubInterest(t, ln1, globalAccountName, "foo", time.Second)

	sln3.Unsubscribe()
	natsFlush(t, cln3)
	sc2.Unsubscribe()
	natsFlush(t, cc2)
	sc1.Unsubscribe()
	natsFlush(t, cc1)

	cln1 := natsConnect(t, ln1.ClientURL())
	defer cln1.Close()

	natsPub(t, cln1, "foo", []byte("hello"))
	natsNexMsg(t, sln2, time.Second)
}

func TestLeafNodeJetStreamDomainMapCrossTalk(t *testing.T) {
	accs := `
accounts :{
    A:{   jetstream: enable, users:[ {user:a1,password:a1}]},
    SYS:{ users:[ {user:s1,password:s1}]},
}
system_account: SYS
`

	sd1 := t.TempDir()
	confA := createConfFile(t, []byte(fmt.Sprintf(`
listen: 127.0.0.1:-1
%s
jetstream: { domain: da, store_dir: '%s', max_mem: 50Mb, max_file: 50Mb }
leafnodes: {
	listen: 127.0.0.1:-1
	no_advertise: true
	authorization: {
		timeout: 0.5
	}
}
`, accs, sd1)))
	sA, _ := RunServerWithConfig(confA)
	defer sA.Shutdown()

	sd2 := t.TempDir()
	confL := createConfFile(t, []byte(fmt.Sprintf(`
listen: 127.0.0.1:-1
%s
jetstream: { domain: dl, store_dir: '%s', max_mem: 50Mb, max_file: 50Mb }
leafnodes:{
	no_advertise: true
    remotes:[{url:nats://a1:a1@127.0.0.1:%d, account: A},
		     {url:nats://s1:s1@127.0.0.1:%d, account: SYS}]
}
`, accs, sd2, sA.opts.LeafNode.Port, sA.opts.LeafNode.Port)))
	sL, _ := RunServerWithConfig(confL)
	defer sL.Shutdown()

	ncA := natsConnect(t, sA.ClientURL(), nats.UserInfo("a1", "a1"))
	defer ncA.Close()
	ncL := natsConnect(t, sL.ClientURL(), nats.UserInfo("a1", "a1"))
	defer ncL.Close()

	test := func(jsA, jsL nats.JetStreamContext) {
		kvA, err := jsA.CreateKeyValue(&nats.KeyValueConfig{Bucket: "bucket"})
		require_NoError(t, err)
		kvL, err := jsL.CreateKeyValue(&nats.KeyValueConfig{Bucket: "bucket"})
		require_NoError(t, err)

		_, err = kvA.Put("A", nil)
		require_NoError(t, err)
		_, err = kvL.Put("L", nil)
		require_NoError(t, err)

		// check for unwanted cross talk
		_, err = kvA.Get("A")
		require_NoError(t, err)
		_, err = kvA.Get("l")
		require_Error(t, err)
		require_True(t, err == nats.ErrKeyNotFound)

		_, err = kvL.Get("A")
		require_Error(t, err)
		require_True(t, err == nats.ErrKeyNotFound)
		_, err = kvL.Get("L")
		require_NoError(t, err)

		err = jsA.DeleteKeyValue("bucket")
		require_NoError(t, err)
		err = jsL.DeleteKeyValue("bucket")
		require_NoError(t, err)
	}

	jsA, err := ncA.JetStream()
	require_NoError(t, err)
	jsL, err := ncL.JetStream()
	require_NoError(t, err)
	test(jsA, jsL)

	jsAL, err := ncA.JetStream(nats.Domain("dl"))
	require_NoError(t, err)
	jsLA, err := ncL.JetStream(nats.Domain("da"))
	require_NoError(t, err)
	test(jsAL, jsLA)

	jsAA, err := ncA.JetStream(nats.Domain("da"))
	require_NoError(t, err)
	jsLL, err := ncL.JetStream(nats.Domain("dl"))
	require_NoError(t, err)
	test(jsAA, jsLL)
}

type checkLeafMinVersionLogger struct {
	DummyLogger
	errCh  chan time.Time
	connCh chan time.Time
}

func (l *checkLeafMinVersionLogger) Errorf(format string, args ...any) {
	msg := fmt.Sprintf(format, args...)
	if strings.Contains(msg, "minimum version") {
		select {
		case l.errCh <- time.Now():
		default:
		}
	}
}

func (l *checkLeafMinVersionLogger) Noticef(format string, args ...any) {
	msg := fmt.Sprintf(format, args...)
	if strings.Contains(msg, "Leafnode connection created") {
		select {
		case l.connCh <- time.Now():
		default:
		}
	}
}

func TestLeafNodeMinVersion(t *testing.T) {
	conf := createConfFile(t, []byte(`
		port: -1
		leafnodes {
			port: -1
			min_version: 2.8.0
		}
	`))
	s, o := RunServerWithConfig(conf)
	defer s.Shutdown()

	rconf := createConfFile(t, []byte(fmt.Sprintf(`
		port: -1
		leafnodes {
			remotes [
				{url: "nats://127.0.0.1:%d" }
			]
		}
	`, o.LeafNode.Port)))
	ln, _ := RunServerWithConfig(rconf)
	defer ln.Shutdown()

	checkLeafNodeConnected(t, s)
	checkLeafNodeConnected(t, ln)

	ln.Shutdown()
	s.Shutdown()

	// Now makes sure we validate options, not just config file.
	for _, test := range []struct {
		name    string
		version string
		err     string
	}{
		{"invalid version", "abc", "semver"},
		{"version too low", "2.7.9", "the minimum version should be at least 2.8.0"},
	} {
		t.Run(test.name, func(t *testing.T) {
			o.Port = -1
			o.LeafNode.Port = -1
			o.LeafNode.MinVersion = test.version
			if s, err := NewServer(o); err == nil || !strings.Contains(err.Error(), test.err) {
				if s != nil {
					s.Shutdown()
				}
				t.Fatalf("Expected error to contain %q, got %v", test.err, err)
			}
		})
	}

	// Ok, so now to verify that a server rejects a leafnode connection
	// we will set the min_version above our current VERSION. So first
	// decompose the version:
	major, minor, _, err := versionComponents(VERSION)
	if err != nil {
		t.Fatalf("The current server version %q is not valid: %v", VERSION, err)
	}
	// Let's make our minimum server an minor version above
	mv := fmt.Sprintf("%d.%d.0", major, minor+1)
	conf = createConfFile(t, []byte(fmt.Sprintf(`
		port: -1
		leafnodes {
			port: -1
			min_version: "%s"
		}
	`, mv)))
	s, o = RunServerWithConfig(conf)
	defer s.Shutdown()

	l := &checkLeafMinVersionLogger{errCh: make(chan time.Time, 1), connCh: make(chan time.Time, 1)}
	s.SetLogger(l, false, false)

	rconf = createConfFile(t, []byte(fmt.Sprintf(`
		port: -1
		leafnodes {
			remotes [
				{url: "nats://127.0.0.1:%d" }
			]
		}
	`, o.LeafNode.Port)))
	lo := LoadConfig(rconf)
	lo.LeafNode.ReconnectInterval = 50 * time.Millisecond
	ln = RunServer(lo)
	defer ln.Shutdown()

	select {
	case <-l.connCh:
	case <-time.After(time.Second):
		t.Fatal("Remote did not try to connect")
	}

	var rejectAt time.Time
	select {
	case rejectAt = <-l.errCh:
	case <-time.After(time.Second):
		t.Fatal("Did not get the minimum version required error")
	}

	// Since we have a very small reconnect interval, the next attempt should be
	// delayed by the dedicated minimum version reconnect delay on the soliciting
	// side, not by the normal reconnect interval.
	select {
	case secondAttemptAt := <-l.connCh:
		if elapsed := secondAttemptAt.Sub(rejectAt); elapsed < leafNodeMinVersionReconnectDelay {
			t.Fatalf("Expected reconnect attempt after at least %v, got %v", leafNodeMinVersionReconnectDelay, elapsed)
		}
	case <-time.After(7 * time.Second):
		t.Fatal("Did not get the reconnect attempt")
	}
}

func TestLeafNodeStreamAndShadowSubs(t *testing.T) {
	hubConf := createConfFile(t, []byte(`
		port: -1
		leafnodes {
			port: -1
			authorization: {
			  user: leaf
			  password: leaf
			  account: B
			}
		}
		accounts: {
			A: {
			  users = [{user: usrA, password: usrA}]
			  exports: [{stream: foo.*.>}]
			}
			B: {
			  imports: [{stream: {account: A, subject: foo.*.>}}]
			}
		}
	`))
	hub, hubo := RunServerWithConfig(hubConf)
	defer hub.Shutdown()

	leafConfContet := fmt.Sprintf(`
		port: -1
		leafnodes {
			remotes = [
				{
					url: "nats-leaf://leaf:leaf@127.0.0.1:%d"
					account: B
				}
			]
		}
		accounts: {
			B: {
				exports: [{stream: foo.*.>}]
			}
			C: {
				users: [{user: usrC, password: usrC}]
				imports: [{stream: {account: B, subject: foo.bar.>}}]
			}
		}
	`, hubo.LeafNode.Port)
	leafConf := createConfFile(t, []byte(leafConfContet))
	leafo := LoadConfig(leafConf)
	leafo.LeafNode.ReconnectInterval = 50 * time.Millisecond
	leaf := RunServer(leafo)
	defer leaf.Shutdown()

	checkLeafNodeConnected(t, hub)
	checkLeafNodeConnected(t, leaf)

	subPubAndCheck := func() {
		t.Helper()

		ncl, err := nats.Connect(leaf.ClientURL(), nats.UserInfo("usrC", "usrC"))
		if err != nil {
			t.Fatalf("Error connecting: %v", err)
		}
		defer ncl.Close()

		// This will send an LS+ to the "hub" server.
		sub, err := ncl.SubscribeSync("foo.*.baz")
		if err != nil {
			t.Fatalf("Error subscribing: %v", err)
		}
		ncl.Flush()

		ncm, err := nats.Connect(hub.ClientURL(), nats.UserInfo("usrA", "usrA"))
		if err != nil {
			t.Fatalf("Error connecting: %v", err)
		}
		defer ncm.Close()

		// Try a few times in case subject interest has not propagated yet
		for i := 0; i < 5; i++ {
			ncm.Publish("foo.bar.baz", []byte("msg"))
			if _, err := sub.NextMsg(time.Second); err == nil {
				// OK, done!
				return
			}
		}
		t.Fatal("Message was not received")
	}
	subPubAndCheck()

	// Now cause a restart of the accepting side so that the leaf connection
	// is recreated.
	hub.Shutdown()
	hub = RunServer(hubo)
	defer hub.Shutdown()

	checkLeafNodeConnected(t, hub)
	checkLeafNodeConnected(t, leaf)

	subPubAndCheck()

	// Issue a config reload even though we make no modification. There was
	// a defect that caused the interest propagation to break.
	// Set the ReconnectInterval to the default value so that reload does not complain.
	leaf.getOpts().LeafNode.ReconnectInterval = DEFAULT_LEAF_NODE_RECONNECT
	reloadUpdateConfig(t, leaf, leafConf, leafConfContet)

	// Check again
	subPubAndCheck()
}

func TestLeafNodeAuthConfigReload(t *testing.T) {
	template := `
		listen: 127.0.0.1:-1
		accounts { test: {} }
		leaf {
			listen: "127.0.0.1:7422"
			tls {
				cert_file: "../test/configs/certs/server-cert.pem"
				key_file:  "../test/configs/certs/server-key.pem"
				ca_file:   "../test/configs/certs/ca.pem"
			}
			authorization {
				# These are only fields allowed atm.
				users = [ { user: test, password: "s3cret1", account: "test"  } ]
			}
		}
	`
	conf := createConfFile(t, []byte(template))

	s, _ := RunServerWithConfig(conf)
	defer s.Shutdown()

	lg := &captureErrorLogger{errCh: make(chan string, 10)}
	s.SetLogger(lg, false, false)

	// Reload here should work ok.
	reloadUpdateConfig(t, s, conf, template)
}

func TestLeafNodeSignatureCB(t *testing.T) {
	content := `
		port: -1
		server_name: OP
		operator = "../test/configs/nkeys/op.jwt"
		resolver = MEMORY
		listen: "127.0.0.1:-1"
		leafnodes {
			listen: "127.0.0.1:-1"
		}
	`
	conf := createConfFile(t, []byte(content))
	s, opts := RunServerWithConfig(conf)
	defer s.Shutdown()

	_, akp := createAccount(s)
	kp, _ := nkeys.CreateUser()
	pub, _ := kp.PublicKey()
	nuc := jwt.NewUserClaims(pub)
	ujwt, err := nuc.Encode(akp)
	if err != nil {
		t.Fatalf("Error generating user JWT: %v", err)
	}

	dto := DefaultTestOptions
	lopts := &dto
	u, err := url.Parse(fmt.Sprintf("nats://%s:%d", opts.LeafNode.Host, opts.LeafNode.Port))
	if err != nil {
		t.Fatalf("Error parsing url: %v", err)
	}
	remote := &RemoteLeafOpts{URLs: []*url.URL{u}}
	remote.SignatureCB = func(nonce []byte) (string, []byte, error) {
		return "", nil, fmt.Errorf("on purpose")
	}
	lopts.LeafNode.Remotes = []*RemoteLeafOpts{remote}
	lopts.LeafNode.ReconnectInterval = 100 * time.Millisecond
	sl := RunServer(lopts)
	defer sl.Shutdown()

	slog := &captureErrorLogger{errCh: make(chan string, 10)}
	sl.SetLogger(slog, false, false)

	// Now check that the leafnode got the error that the callback returned.
	select {
	case err := <-slog.errCh:
		if !strings.Contains(err, "on purpose") {
			t.Fatalf("Expected error from cb, got %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("Did not get expected error")
	}

	sl.Shutdown()
	// Now check what happens if the connection is closed while in the callback.
	blockCh := make(chan struct{})
	inCB := make(chan struct{}, 1)
	remote.SignatureCB = func(nonce []byte) (string, []byte, error) {
		select {
		case inCB <- struct{}{}:
		default:
		}
		<-blockCh
		sig, err := kp.Sign(nonce)
		return ujwt, sig, err
	}
	sl = RunServer(lopts)
	defer sl.Shutdown()

	// Recreate the logger so that we are sure not to have possible previous errors
	slog = &captureErrorLogger{errCh: make(chan string, 10)}
	sl.SetLogger(slog, false, false)

	// Wait until we're in the callback.
	select {
	case <-inCB:
	case <-time.After(2 * time.Second):
		t.Fatal("Signature callback was not invoked")
	}

	// Get the leaf connection from the temp clients map and close it.
	checkFor(t, time.Second, 15*time.Millisecond, func() error {
		var c *client
		sl.grMu.Lock()
		for _, cli := range sl.grTmpClients {
			c = cli
		}
		sl.grMu.Unlock()
		if c == nil {
			return fmt.Errorf("Client still not found in temp map")
		}
		c.closeConnection(ClientClosed)
		return nil
	})

	// Release the callback, and check we get the appropriate error.
	close(blockCh)
	select {
	case err := <-slog.errCh:
		if !strings.Contains(err, ErrConnectionClosed.Error()) {
			t.Fatalf("Expected error that connection was closed, got %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("Did not get expected error")
	}

	sl.Shutdown()
	// Change to a good CB and now it should work
	remote.SignatureCB = func(nonce []byte) (string, []byte, error) {
		sig, err := kp.Sign(nonce)
		return ujwt, sig, err
	}
	sl = RunServer(lopts)
	defer sl.Shutdown()
	checkLeafNodeConnected(t, sl)
}

type testLeafTraceLogger struct {
	DummyLogger
	ch chan string
}

func (l *testLeafTraceLogger) Tracef(format string, v ...any) {
	msg := fmt.Sprintf(format, v...)
	// We will sub to 'baz' and to 'bar', so filter on 'ba' prefix.
	if strings.Contains(msg, "[LS+ ba") {
		select {
		case l.ch <- msg:
		default:
		}
	}
}

// Make sure permissioned denied subs do not make it to the leafnode even if existing.
func TestLeafNodePermsSuppressSubs(t *testing.T) {
	conf := createConfFile(t, []byte(`
		listen: 127.0.0.1:-1
		authorization {
		  PERMS = {
		    publish = "foo"
		    subscribe = ["_INBOX.>"]
		  }
		  users = [
		    {user: "user", password: "pass"}
		    {user: "ln",  password: "pass" , permissions: $PERMS }
		  ]
		}
		no_auth_user: user

		leafnodes {
		  listen: 127.0.0.1:7422
		}
	`))

	lconf := createConfFile(t, []byte(`
		listen: 127.0.0.1:-1
		leafnodes {
		  remotes = [ { url: "nats://ln:pass@127.0.0.1" } ]
		}
		trace = true
	`))

	s, _ := RunServerWithConfig(conf)
	defer s.Shutdown()

	// Connect client to the hub.
	nc, err := nats.Connect(s.ClientURL())
	require_NoError(t, err)
	defer nc.Close()

	// This should not be seen on leafnode side since we only allow pub to "foo"
	_, err = nc.SubscribeSync("baz")
	require_NoError(t, err)

	ln, _ := RunServerWithConfig(lconf)
	defer ln.Shutdown()

	// Setup logger to capture trace events.
	l := &testLeafTraceLogger{ch: make(chan string, 10)}
	ln.SetLogger(l, true, true)

	checkLeafNodeConnected(t, ln)

	// Need to have ot reconnect to trigger since logger attaches too late.
	ln.mu.Lock()
	for _, c := range ln.leafs {
		c.mu.Lock()
		c.nc.Close()
		c.mu.Unlock()
	}
	ln.mu.Unlock()
	checkLeafNodeConnectedCount(t, ln, 0)
	checkLeafNodeConnectedCount(t, ln, 1)

	select {
	case msg := <-l.ch:
		t.Fatalf("Unexpected LS+ seen on leafnode: %s", msg)
	case <-time.After(50 * time.Millisecond):
		// OK
	}

	// Now double check that new subs also do not propagate.
	// This behavior was working already.
	_, err = nc.SubscribeSync("bar")
	require_NoError(t, err)

	select {
	case msg := <-l.ch:
		t.Fatalf("Unexpected LS+ seen on leafnode: %s", msg)
	case <-time.After(50 * time.Millisecond):
		// OK
	}
}

func TestLeafNodeDuplicateMsg(t *testing.T) {
	// This involves 2 clusters with leafnodes to each other with a different
	// account, and those accounts import/export a subject that caused
	// duplicate messages. This test requires static ports since we need to
	// have A->B and B->A.
	a1Conf := createConfFile(t, []byte(`
	cluster : {
		name : A
		port : -1
	}
	leafnodes : {
		port : 14333
		remotes : [{
			account : A
			urls : [nats://leafa:pwd@127.0.0.1:24333]
		}]
	}
	port : -1
	server_name : A_1

	accounts:{
		A:{
			users:[
				{user: leafa, password: pwd},
				{user: usera, password: usera, permissions: {
					publish:{ allow:["iot.b.topic"] }
					subscribe:{ allow:["iot.a.topic"] }
				}}
			]
			imports:[
				{stream:{account:"B", subject:"iot.a.topic"}}
			]
		},
		B:{
			users:[
				{user: leafb, password: pwd},
			]
			exports:[
				{stream: "iot.a.topic", accounts: ["A"]}
			]
		}
	}
	`))
	a1, oa1 := RunServerWithConfig(a1Conf)
	defer a1.Shutdown()

	a2Conf := createConfFile(t, []byte(fmt.Sprintf(`
	cluster : {
		name : A
		port : -1
		routes : [nats://127.0.0.1:%d]
	}
	leafnodes : {
		port : 14334
		remotes : [{
			account : A
			urls : [nats://leafa:pwd@127.0.0.1:24334]
		}]
	}
	port : -1
	server_name : A_2

	accounts:{
		A:{
			users:[
				{user: leafa, password: pwd},
				{user: usera, password: usera, permissions: {
					publish:{ allow:["iot.b.topic"] }
					subscribe:{ allow:["iot.a.topic"] }
				}}
			]
			imports:[
				{stream:{account:"B", subject:"iot.a.topic"}}
			]
		},
		B:{
			users:[
				{user: leafb, password: pwd},
			]
			exports:[
				{stream: "iot.a.topic", accounts: ["A"]}
			]
		}
	}`, oa1.Cluster.Port)))
	a2, _ := RunServerWithConfig(a2Conf)
	defer a2.Shutdown()

	checkClusterFormed(t, a1, a2)

	b1Conf := createConfFile(t, []byte(`
	cluster : {
		name : B
		port : -1
	}
	leafnodes : {
		port : 24333
		remotes : [{
			account : B
			urls : [nats://leafb:pwd@127.0.0.1:14333]
		}]
	}
	port : -1
	server_name : B_1

	accounts:{
		A:{
			users:[
				{user: leafa, password: pwd},
			]
			exports:[
				{stream: "iot.b.topic", accounts: ["B"]}
			]
		},
		B:{
			users:[
				{user: leafb, password: pwd},
				{user: userb, password: userb, permissions: {
					publish:{ allow:["iot.a.topic"] },
					subscribe:{ allow:["iot.b.topic"] }
				}}
			]
			imports:[
				{stream:{account:"A", subject:"iot.b.topic"}}
			]
		}
	}`))
	b1, ob1 := RunServerWithConfig(b1Conf)
	defer b1.Shutdown()

	b2Conf := createConfFile(t, []byte(fmt.Sprintf(`
	cluster : {
		name : B
		port : -1
		routes : [nats://127.0.0.1:%d]
	}
	leafnodes : {
		port : 24334
		remotes : [{
			account : B
			urls : [nats://leafb:pwd@127.0.0.1:14334]
		}]
	}
	port : -1
	server_name : B_2

	accounts:{
		A:{
			users:[
				{user: leafa, password: pwd},
			]
			exports:[
				{stream: "iot.b.topic", accounts: ["B"]}
			]
		},
		B:{
			users:[
				{user: leafb, password: pwd},
				{user: userb, password: userb, permissions: {
					publish:{ allow:["iot.a.topic"] },
					subscribe:{ allow:["iot.b.topic"] }
				}}
			]
			imports:[
				{stream:{account:"A", subject:"iot.b.topic"}}
			]
		}
	}`, ob1.Cluster.Port)))
	b2, _ := RunServerWithConfig(b2Conf)
	defer b2.Shutdown()

	checkClusterFormed(t, b1, b2)

	checkLeafNodeConnectedCount(t, a1, 2)
	checkLeafNodeConnectedCount(t, a2, 2)
	checkLeafNodeConnectedCount(t, b1, 2)
	checkLeafNodeConnectedCount(t, b2, 2)

	check := func(t *testing.T, subSrv *Server, pubSrv *Server) {
		sc := natsConnect(t, subSrv.ClientURL(), nats.UserInfo("userb", "userb"))
		defer sc.Close()

		subject := "iot.b.topic"
		sub := natsSubSync(t, sc, subject)

		// Wait for this to be available in A cluster
		checkSubInterest(t, a1, "A", subject, time.Second)
		checkSubInterest(t, a2, "A", subject, time.Second)

		pb := natsConnect(t, pubSrv.ClientURL(), nats.UserInfo("usera", "usera"))
		defer pb.Close()

		natsPub(t, pb, subject, []byte("msg"))
		natsNexMsg(t, sub, time.Second)
		// Should be only 1
		if msg, err := sub.NextMsg(100 * time.Millisecond); err == nil {
			t.Fatalf("Received duplicate on %q: %s", msg.Subject, msg.Data)
		}
	}
	t.Run("sub_b1_pub_a1", func(t *testing.T) { check(t, b1, a1) })
	t.Run("sub_b1_pub_a2", func(t *testing.T) { check(t, b1, a2) })
	t.Run("sub_b2_pub_a1", func(t *testing.T) { check(t, b2, a1) })
	t.Run("sub_b2_pub_a2", func(t *testing.T) { check(t, b2, a2) })
}

func TestLeafNodeTLSHandshakeFirstVerifyNoInfoSent(t *testing.T) {
	confHub := createConfFile(t, []byte(`
		port : -1
		leafnodes : {
			port : -1
			tls {
				cert_file: "../test/configs/certs/server-cert.pem"
				key_file:  "../test/configs/certs/server-key.pem"
				ca_file: "../test/configs/certs/ca.pem"
				timeout: 2
				handshake_first: true
			}
		}
	`))
	s1, o1 := RunServerWithConfig(confHub)
	defer s1.Shutdown()

	c, err := net.DialTimeout("tcp", fmt.Sprintf("127.0.0.1:%d", o1.LeafNode.Port), 2*time.Second)
	require_NoError(t, err)
	defer c.Close()

	buf := make([]byte, 1024)
	// We will wait for up to 500ms to see if the server is sending (incorrectly)
	// the INFO.
	c.SetReadDeadline(time.Now().Add(500 * time.Millisecond))
	n, err := c.Read(buf)
	c.SetReadDeadline(time.Time{})
	// If we did not get an error, this is an issue...
	if err == nil {
		t.Fatalf("Should not have received anything, got n=%v buf=%s", n, buf[:n])
	}
	// We expect a timeout error
	if ne, ok := err.(net.Error); !ok || !ne.Timeout() {
		t.Fatalf("Expected a timeout error, got %v", err)
	}
}

func TestLeafNodeTLSHandshakeFirstFallbackDelayConfigValues(t *testing.T) {
	tmpl := `
		listen: "127.0.0.1:-1"
		leafnodes {
			listen: "127.0.0.1:-1"
			tls {
				cert_file: 	"../test/configs/certs/server-cert.pem"
				key_file:  	"../test/configs/certs/server-key.pem"
				timeout: 	1
				first: 		%s
			}
		}
	`
	for _, test := range []struct {
		name  string
		val   string
		first bool
		delay time.Duration
	}{
		{"first as boolean true", "true", true, 0},
		{"first as boolean false", "false", false, 0},
		{"first as string true", "\"true\"", true, 0},
		{"first as string false", "\"false\"", false, 0},
		{"first as string on", "on", true, 0},
		{"first as string off", "off", false, 0},
		{"first as string auto", "auto", true, DEFAULT_TLS_HANDSHAKE_FIRST_FALLBACK_DELAY},
		{"first as string auto_fallback", "auto_fallback", true, DEFAULT_TLS_HANDSHAKE_FIRST_FALLBACK_DELAY},
		{"first as fallback duration", "300ms", true, 300 * time.Millisecond},
	} {
		t.Run(test.name, func(t *testing.T) {
			conf := createConfFile(t, []byte(fmt.Sprintf(tmpl, test.val)))
			s, o := RunServerWithConfig(conf)
			defer s.Shutdown()

			if test.first {
				if !o.LeafNode.TLSHandshakeFirst {
					t.Fatal("Expected tls first to be true, was not")
				}
				if test.delay != o.LeafNode.TLSHandshakeFirstFallback {
					t.Fatalf("Expected fallback delay to be %v, got %v", test.delay, o.LeafNode.TLSHandshakeFirstFallback)
				}
			} else {
				if o.LeafNode.TLSHandshakeFirst {
					t.Fatal("Expected tls first to be false, was not")
				}
				if o.LeafNode.TLSHandshakeFirstFallback != 0 {
					t.Fatalf("Expected fallback delay to be 0, got %v", o.LeafNode.TLSHandshakeFirstFallback)
				}
			}
		})
	}
}

func TestLeafNodeTLSHandshakeFirst(t *testing.T) {
	tmpl1 := `
		port : -1
		leafnodes : {
			port : -1
			tls {
				cert_file: "../test/configs/certs/server-cert.pem"
				key_file:  "../test/configs/certs/server-key.pem"
				ca_file: "../test/configs/certs/ca.pem"
				timeout: 2
				handshake_first: %s
			}
		}
	`
	confHub := createConfFile(t, []byte(fmt.Sprintf(tmpl1, "true")))
	s1, o1 := RunServerWithConfig(confHub)
	defer s1.Shutdown()

	tmpl2 := `
		port: -1
		leafnodes : {
			port : -1
			remotes : [
				{
					urls : [tls://127.0.0.1:%d]
					tls {
						cert_file: "../test/configs/certs/client-cert.pem"
						key_file:  "../test/configs/certs/client-key.pem"
						ca_file: "../test/configs/certs/ca.pem"
						timeout: 2
						first: %s
					}
				}
			]
		}
	`
	confSpoke := createConfFile(t, []byte(fmt.Sprintf(tmpl2, o1.LeafNode.Port, "true")))
	s2, _ := RunServerWithConfig(confSpoke)
	defer s2.Shutdown()

	checkLeafNodeConnected(t, s2)

	s2.Shutdown()

	// Now check that there will be a failure if the remote does not ask for
	// handshake first since the hub is configured that way.
	// Set a logger on s1 to capture errors
	l := &captureErrorLogger{errCh: make(chan string, 10)}
	s1.SetLogger(l, false, false)

	confSpoke = createConfFile(t, []byte(fmt.Sprintf(tmpl2, o1.LeafNode.Port, "false")))
	s2, _ = RunServerWithConfig(confSpoke)
	defer s2.Shutdown()

	select {
	case err := <-l.errCh:
		if !strings.Contains(err, "handshake error") {
			t.Fatalf("Unexpected error: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Did not get TLS handshake failure")
	}

	// Check configuration reload for this remote
	reloadUpdateConfig(t, s2, confSpoke, fmt.Sprintf(tmpl2, o1.LeafNode.Port, "true"))
	checkLeafNodeConnected(t, s2)
	s2.Shutdown()

	// Drain the logger error channel
	for done := false; !done; {
		select {
		case <-l.errCh:
		default:
			done = true
		}
	}

	// Now change the config on the hub
	reloadUpdateConfig(t, s1, confHub, fmt.Sprintf(tmpl1, "false"))
	// Restart s2
	s2, _ = RunServerWithConfig(confSpoke)
	defer s2.Shutdown()

	select {
	case err := <-l.errCh:
		if !strings.Contains(err, "handshake error") {
			t.Fatalf("Unexpected error: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Did not get TLS handshake failure")
	}

	// Reload again with "true"
	reloadUpdateConfig(t, s1, confHub, fmt.Sprintf(tmpl1, "true"))
	checkLeafNodeConnected(t, s2)

	// Shutdown the remote
	s2.Shutdown()

	// Reload the hub with handshake_first with a duration
	reloadUpdateConfig(t, s1, confHub, fmt.Sprintf(tmpl1, `"250ms"`))

	// Now start s2 with a remote that does not use handshake_first (set to false)
	confSpoke = createConfFile(t, []byte(fmt.Sprintf(tmpl2, o1.LeafNode.Port, "false")))
	s2, _ = RunServerWithConfig(confSpoke)
	defer s2.Shutdown()
	checkLeafNodeConnected(t, s2)
}

func TestLeafNodeTLSHandshakeEvenForRemoteWithNoTLSBlock(t *testing.T) {
	confHub := createConfFile(t, []byte(`
		port : -1
		leafnodes : {
			port : -1
			tls {
				cert_file: "../test/configs/certs/server-cert.pem"
				key_file:  "../test/configs/certs/server-key.pem"
				ca_file: "../test/configs/certs/ca.pem"
				timeout: 2
			}
		}
	`))
	s1, o1 := RunServerWithConfig(confHub)
	defer s1.Shutdown()

	tmpl2 := `
		port: -1
		leafnodes : {
			port : -1
			remotes : [
				{
					urls : [tls://127.0.0.1:%d]
				}
			]
		}
	`
	confSpoke := createConfFile(t, []byte(fmt.Sprintf(tmpl2, o1.LeafNode.Port)))
	s2, _ := RunServerWithConfig(confSpoke)
	defer s2.Shutdown()

	l := &captureDebugLogger{dbgCh: make(chan string, 100)}
	s2.SetLogger(l, true, false)

	tm := time.NewTimer(2 * time.Second)
	defer tm.Stop()
	for {
		select {
		case l := <-l.dbgCh:
			if strings.Contains(l, "Starting TLS") {
				// OK!
				return
			}
		case <-tm.C:
			t.Fatalf("Did not perform a TLS handshake")
		}
	}
}

func TestLeafNodeCompressionOptions(t *testing.T) {
	org := testDefaultLeafNodeCompression
	testDefaultLeafNodeCompression = _EMPTY_
	defer func() { testDefaultLeafNodeCompression = org }()

	tmpl := `
		port: -1
		leafnodes {
			port: -1
			compression: %s
		}
	`
	for _, test := range []struct {
		name     string
		mode     string
		rttVals  []int
		expected string
		rtts     []time.Duration
	}{
		{"boolean enabled", "true", nil, CompressionS2Auto, defaultCompressionS2AutoRTTThresholds},
		{"string enabled", "enabled", nil, CompressionS2Auto, defaultCompressionS2AutoRTTThresholds},
		{"string EnaBled", "EnaBled", nil, CompressionS2Auto, defaultCompressionS2AutoRTTThresholds},
		{"string on", "on", nil, CompressionS2Auto, defaultCompressionS2AutoRTTThresholds},
		{"string ON", "ON", nil, CompressionS2Auto, defaultCompressionS2AutoRTTThresholds},
		{"string fast", "fast", nil, CompressionS2Fast, nil},
		{"string Fast", "Fast", nil, CompressionS2Fast, nil},
		{"string s2_fast", "s2_fast", nil, CompressionS2Fast, nil},
		{"string s2_Fast", "s2_Fast", nil, CompressionS2Fast, nil},
		{"boolean disabled", "false", nil, CompressionOff, nil},
		{"string disabled", "disabled", nil, CompressionOff, nil},
		{"string DisableD", "DisableD", nil, CompressionOff, nil},
		{"string off", "off", nil, CompressionOff, nil},
		{"string OFF", "OFF", nil, CompressionOff, nil},
		{"better", "better", nil, CompressionS2Better, nil},
		{"Better", "Better", nil, CompressionS2Better, nil},
		{"s2_better", "s2_better", nil, CompressionS2Better, nil},
		{"S2_BETTER", "S2_BETTER", nil, CompressionS2Better, nil},
		{"best", "best", nil, CompressionS2Best, nil},
		{"BEST", "BEST", nil, CompressionS2Best, nil},
		{"s2_best", "s2_best", nil, CompressionS2Best, nil},
		{"S2_BEST", "S2_BEST", nil, CompressionS2Best, nil},
		{"auto no rtts", "auto", nil, CompressionS2Auto, defaultCompressionS2AutoRTTThresholds},
		{"s2_auto no rtts", "s2_auto", nil, CompressionS2Auto, defaultCompressionS2AutoRTTThresholds},
		{"auto", "{mode: auto, rtt_thresholds: [%s]}", []int{1}, CompressionS2Auto, []time.Duration{time.Millisecond}},
		{"Auto", "{Mode: Auto, thresholds: [%s]}", []int{1, 2}, CompressionS2Auto, []time.Duration{time.Millisecond, 2 * time.Millisecond}},
		{"s2_auto", "{mode: s2_auto, thresholds: [%s]}", []int{1, 2, 3}, CompressionS2Auto, []time.Duration{time.Millisecond, 2 * time.Millisecond, 3 * time.Millisecond}},
		{"s2_AUTO", "{mode: s2_AUTO, thresholds: [%s]}", []int{1, 2, 3, 4}, CompressionS2Auto, []time.Duration{time.Millisecond, 2 * time.Millisecond, 3 * time.Millisecond, 4 * time.Millisecond}},
		{"s2_auto:-10,5,10", "{mode: s2_auto, thresholds: [%s]}", []int{-10, 5, 10}, CompressionS2Auto, []time.Duration{0, 5 * time.Millisecond, 10 * time.Millisecond}},
		{"s2_auto:5,10,15", "{mode: s2_auto, thresholds: [%s]}", []int{5, 10, 15}, CompressionS2Auto, []time.Duration{5 * time.Millisecond, 10 * time.Millisecond, 15 * time.Millisecond}},
		{"s2_auto:0,5,10", "{mode: s2_auto, thresholds: [%s]}", []int{0, 5, 10}, CompressionS2Auto, []time.Duration{0, 5 * time.Millisecond, 10 * time.Millisecond}},
		{"s2_auto:5,10,0,20", "{mode: s2_auto, thresholds: [%s]}", []int{5, 10, 0, 20}, CompressionS2Auto, []time.Duration{5 * time.Millisecond, 10 * time.Millisecond, 0, 20 * time.Millisecond}},
		{"s2_auto:0,10,0,20", "{mode: s2_auto, thresholds: [%s]}", []int{0, 10, 0, 20}, CompressionS2Auto, []time.Duration{0, 10 * time.Millisecond, 0, 20 * time.Millisecond}},
		{"s2_auto:0,0,0,20", "{mode: s2_auto, thresholds: [%s]}", []int{0, 0, 0, 20}, CompressionS2Auto, []time.Duration{0, 0, 0, 20 * time.Millisecond}},
		{"s2_auto:0,10,0,0", "{mode: s2_auto, rtt_thresholds: [%s]}", []int{0, 10, 0, 0}, CompressionS2Auto, []time.Duration{0, 10 * time.Millisecond}},
	} {
		t.Run(test.name, func(t *testing.T) {
			var val string
			if len(test.rttVals) > 0 {
				var rtts string
				for i, v := range test.rttVals {
					if i > 0 {
						rtts += ", "
					}
					rtts += fmt.Sprintf("%dms", v)
				}
				val = fmt.Sprintf(test.mode, rtts)
			} else {
				val = test.mode
			}
			conf := createConfFile(t, []byte(fmt.Sprintf(tmpl, val)))
			s, o := RunServerWithConfig(conf)
			defer s.Shutdown()

			if cm := o.LeafNode.Compression.Mode; cm != test.expected {
				t.Fatalf("Expected compression value to be %q, got %q", test.expected, cm)
			}
			if !reflect.DeepEqual(test.rtts, o.LeafNode.Compression.RTTThresholds) {
				t.Fatalf("Expected RTT tresholds to be %+v, got %+v", test.rtts, o.LeafNode.Compression.RTTThresholds)
			}
			port := s.getOpts().Port
			leafNodePort := s.getOpts().LeafNode.Port
			s.Shutdown()

			o.Port = port
			o.LeafNode.Port = leafNodePort
			o.LeafNode.Compression.Mode = test.mode
			if len(test.rttVals) > 0 {
				o.LeafNode.Compression.Mode = CompressionS2Auto
				o.LeafNode.Compression.RTTThresholds = o.LeafNode.Compression.RTTThresholds[:0]
				for _, v := range test.rttVals {
					o.LeafNode.Compression.RTTThresholds = append(o.LeafNode.Compression.RTTThresholds, time.Duration(v)*time.Millisecond)
				}
			}
			s = RunServer(o)
			defer s.Shutdown()
			if cm := o.LeafNode.Compression.Mode; cm != test.expected {
				t.Fatalf("Expected compression value to be %q, got %q", test.expected, cm)
			}
			if !reflect.DeepEqual(test.rtts, o.LeafNode.Compression.RTTThresholds) {
				t.Fatalf("Expected RTT tresholds to be %+v, got %+v", test.rtts, o.LeafNode.Compression.RTTThresholds)
			}
		})
	}

	// Same, but with remotes
	tmpl = `
		port: -1
		leafnodes {
			port: -1
			remotes [
				{
					url: "nats://127.0.0.1:1234"
					compression: %s
				}
			]
		}
	`
	for _, test := range []struct {
		name     string
		mode     string
		rttVals  []int
		expected string
		rtts     []time.Duration
	}{
		{"boolean enabled", "true", nil, CompressionS2Auto, defaultCompressionS2AutoRTTThresholds},
		{"string enabled", "enabled", nil, CompressionS2Auto, defaultCompressionS2AutoRTTThresholds},
		{"string EnaBled", "EnaBled", nil, CompressionS2Auto, defaultCompressionS2AutoRTTThresholds},
		{"string on", "on", nil, CompressionS2Auto, defaultCompressionS2AutoRTTThresholds},
		{"string ON", "ON", nil, CompressionS2Auto, defaultCompressionS2AutoRTTThresholds},
		{"string fast", "fast", nil, CompressionS2Fast, nil},
		{"string Fast", "Fast", nil, CompressionS2Fast, nil},
		{"string s2_fast", "s2_fast", nil, CompressionS2Fast, nil},
		{"string s2_Fast", "s2_Fast", nil, CompressionS2Fast, nil},
		{"boolean disabled", "false", nil, CompressionOff, nil},
		{"string disabled", "disabled", nil, CompressionOff, nil},
		{"string DisableD", "DisableD", nil, CompressionOff, nil},
		{"string off", "off", nil, CompressionOff, nil},
		{"string OFF", "OFF", nil, CompressionOff, nil},
		{"better", "better", nil, CompressionS2Better, nil},
		{"Better", "Better", nil, CompressionS2Better, nil},
		{"s2_better", "s2_better", nil, CompressionS2Better, nil},
		{"S2_BETTER", "S2_BETTER", nil, CompressionS2Better, nil},
		{"best", "best", nil, CompressionS2Best, nil},
		{"BEST", "BEST", nil, CompressionS2Best, nil},
		{"s2_best", "s2_best", nil, CompressionS2Best, nil},
		{"S2_BEST", "S2_BEST", nil, CompressionS2Best, nil},
		{"auto no rtts", "auto", nil, CompressionS2Auto, defaultCompressionS2AutoRTTThresholds},
		{"s2_auto no rtts", "s2_auto", nil, CompressionS2Auto, defaultCompressionS2AutoRTTThresholds},
		{"auto", "{mode: auto, rtt_thresholds: [%s]}", []int{1}, CompressionS2Auto, []time.Duration{time.Millisecond}},
		{"Auto", "{Mode: Auto, thresholds: [%s]}", []int{1, 2}, CompressionS2Auto, []time.Duration{time.Millisecond, 2 * time.Millisecond}},
		{"s2_auto", "{mode: s2_auto, thresholds: [%s]}", []int{1, 2, 3}, CompressionS2Auto, []time.Duration{time.Millisecond, 2 * time.Millisecond, 3 * time.Millisecond}},
		{"s2_AUTO", "{mode: s2_AUTO, thresholds: [%s]}", []int{1, 2, 3, 4}, CompressionS2Auto, []time.Duration{time.Millisecond, 2 * time.Millisecond, 3 * time.Millisecond, 4 * time.Millisecond}},
		{"s2_auto:-10,5,10", "{mode: s2_auto, thresholds: [%s]}", []int{-10, 5, 10}, CompressionS2Auto, []time.Duration{0, 5 * time.Millisecond, 10 * time.Millisecond}},
		{"s2_auto:5,10,15", "{mode: s2_auto, thresholds: [%s]}", []int{5, 10, 15}, CompressionS2Auto, []time.Duration{5 * time.Millisecond, 10 * time.Millisecond, 15 * time.Millisecond}},
		{"s2_auto:0,5,10", "{mode: s2_auto, thresholds: [%s]}", []int{0, 5, 10}, CompressionS2Auto, []time.Duration{0, 5 * time.Millisecond, 10 * time.Millisecond}},
		{"s2_auto:5,10,0,20", "{mode: s2_auto, thresholds: [%s]}", []int{5, 10, 0, 20}, CompressionS2Auto, []time.Duration{5 * time.Millisecond, 10 * time.Millisecond, 0, 20 * time.Millisecond}},
		{"s2_auto:0,10,0,20", "{mode: s2_auto, thresholds: [%s]}", []int{0, 10, 0, 20}, CompressionS2Auto, []time.Duration{0, 10 * time.Millisecond, 0, 20 * time.Millisecond}},
		{"s2_auto:0,0,0,20", "{mode: s2_auto, thresholds: [%s]}", []int{0, 0, 0, 20}, CompressionS2Auto, []time.Duration{0, 0, 0, 20 * time.Millisecond}},
		{"s2_auto:0,10,0,0", "{mode: s2_auto, rtt_thresholds: [%s]}", []int{0, 10, 0, 0}, CompressionS2Auto, []time.Duration{0, 10 * time.Millisecond}},
	} {
		t.Run("remote leaf "+test.name, func(t *testing.T) {
			var val string
			if len(test.rttVals) > 0 {
				var rtts string
				for i, v := range test.rttVals {
					if i > 0 {
						rtts += ", "
					}
					rtts += fmt.Sprintf("%dms", v)
				}
				val = fmt.Sprintf(test.mode, rtts)
			} else {
				val = test.mode
			}
			conf := createConfFile(t, []byte(fmt.Sprintf(tmpl, val)))
			s, o := RunServerWithConfig(conf)
			defer s.Shutdown()

			r := o.LeafNode.Remotes[0]

			if cm := r.Compression.Mode; cm != test.expected {
				t.Fatalf("Expected compression value to be %q, got %q", test.expected, cm)
			}
			if !reflect.DeepEqual(test.rtts, r.Compression.RTTThresholds) {
				t.Fatalf("Expected RTT tresholds to be %+v, got %+v", test.rtts, r.Compression.RTTThresholds)
			}
			port := s.getOpts().Port
			leafNodePort := s.getOpts().LeafNode.Port
			s.Shutdown()

			o.Port = port
			o.LeafNode.Port = leafNodePort
			o.LeafNode.Remotes[0].Compression.Mode = test.mode
			if len(test.rttVals) > 0 {
				o.LeafNode.Remotes[0].Compression.Mode = CompressionS2Auto
				o.LeafNode.Remotes[0].Compression.RTTThresholds = o.LeafNode.Remotes[0].Compression.RTTThresholds[:0]
				for _, v := range test.rttVals {
					o.LeafNode.Remotes[0].Compression.RTTThresholds = append(o.LeafNode.Remotes[0].Compression.RTTThresholds, time.Duration(v)*time.Millisecond)
				}
			}
			s = RunServer(o)
			defer s.Shutdown()
			if cm := o.LeafNode.Remotes[0].Compression.Mode; cm != test.expected {
				t.Fatalf("Expected compression value to be %q, got %q", test.expected, cm)
			}
			if !reflect.DeepEqual(test.rtts, o.LeafNode.Remotes[0].Compression.RTTThresholds) {
				t.Fatalf("Expected RTT tresholds to be %+v, got %+v", test.rtts, o.LeafNode.Remotes[0].Compression.RTTThresholds)
			}
		})
	}

	// Test that with no compression specified, we default to "s2_auto"
	conf := createConfFile(t, []byte(`
		port: -1
		leafnodes {
			port: -1
		}
	`))
	s, o := RunServerWithConfig(conf)
	defer s.Shutdown()
	if o.LeafNode.Compression.Mode != CompressionS2Auto {
		t.Fatalf("Expected compression value to be %q, got %q", CompressionAccept, o.LeafNode.Compression.Mode)
	}
	if !reflect.DeepEqual(defaultCompressionS2AutoRTTThresholds, o.LeafNode.Compression.RTTThresholds) {
		t.Fatalf("Expected RTT tresholds to be %+v, got %+v", defaultCompressionS2AutoRTTThresholds, o.LeafNode.Compression.RTTThresholds)
	}
	// Same for remotes
	conf = createConfFile(t, []byte(`
		port: -1
		leafnodes {
			port: -1
			remotes [ { url: "nats://127.0.0.1:1234" } ]
		}
	`))
	s, o = RunServerWithConfig(conf)
	defer s.Shutdown()
	if cm := o.LeafNode.Remotes[0].Compression.Mode; cm != CompressionS2Auto {
		t.Fatalf("Expected compression value to be %q, got %q", CompressionAccept, cm)
	}
	if !reflect.DeepEqual(defaultCompressionS2AutoRTTThresholds, o.LeafNode.Remotes[0].Compression.RTTThresholds) {
		t.Fatalf("Expected RTT tresholds to be %+v, got %+v", defaultCompressionS2AutoRTTThresholds, o.LeafNode.Remotes[0].Compression.RTTThresholds)
	}
	for _, test := range []struct {
		name string
		mode string
		rtts []time.Duration
		err  string
	}{
		{"unsupported mode", "gzip", nil, "unsupported"},
		{"not ascending order", "s2_auto", []time.Duration{
			5 * time.Millisecond,
			10 * time.Millisecond,
			2 * time.Millisecond,
		}, "ascending"},
		{"too many thresholds", "s2_auto", []time.Duration{
			5 * time.Millisecond,
			10 * time.Millisecond,
			20 * time.Millisecond,
			40 * time.Millisecond,
			60 * time.Millisecond,
		}, "more than 4"},
		{"all 0", "s2_auto", []time.Duration{0, 0, 0, 0}, "at least one"},
		{"single 0", "s2_auto", []time.Duration{0}, "at least one"},
	} {
		t.Run(test.name, func(t *testing.T) {
			o := DefaultOptions()
			o.LeafNode.Port = -1
			o.LeafNode.Compression = CompressionOpts{test.mode, test.rtts}
			if _, err := NewServer(o); err == nil || !strings.Contains(err.Error(), test.err) {
				t.Fatalf("Unexpected error: %v", err)
			}
			// Same with remotes
			o.LeafNode.Compression = CompressionOpts{}
			o.LeafNode.Remotes = []*RemoteLeafOpts{{Compression: CompressionOpts{test.mode, test.rtts}}}
			if _, err := NewServer(o); err == nil || !strings.Contains(err.Error(), test.err) {
				t.Fatalf("Unexpected error: %v", err)
			}
		})
	}
}

func TestLeafNodeCompression(t *testing.T) {
	conf1 := createConfFile(t, []byte(`
		port: -1
		server_name: "Hub"
		accounts {
			A { users: [{user: a, password: pwd}] }
			B { users: [{user: b, password: pwd}] }
			C { users: [{user: c, password: pwd}] }
		}
		leafnodes {
			port: -1
			compression: s2_fast
		}
	`))
	s1, o1 := RunServerWithConfig(conf1)
	defer s1.Shutdown()

	port := o1.LeafNode.Port
	conf2 := createConfFile(t, []byte(fmt.Sprintf(`
		port: -1
		server_name: "Spoke"
		accounts {
			A { users: [{user: a, password: pwd}] }
			B { users: [{user: b, password: pwd}] }
			C { users: [{user: c, password: pwd}] }
		}
		leafnodes {
			remotes [
				{ url: "nats://a:pwd@127.0.0.1:%d", account: "A", compression: s2_better }
				{ url: "nats://b:pwd@127.0.0.1:%d", account: "B", compression: s2_best }
				{ url: "nats://c:pwd@127.0.0.1:%d", account: "C", compression: off }
			]
		}
	`, port, port, port)))
	s2, _ := RunServerWithConfig(conf2)
	defer s2.Shutdown()

	checkLeafNodeConnectedCount(t, s1, 3)
	checkLeafNodeConnectedCount(t, s2, 3)

	s1.mu.RLock()
	for _, l := range s1.leafs {
		l.mu.Lock()
		l.nc = &testConnSentBytes{Conn: l.nc}
		l.mu.Unlock()
	}
	s1.mu.RUnlock()

	var payloads [][]byte
	totalPayloadSize := 0
	count := 26
	for i := 0; i < count; i++ {
		n := rand.Uint32N(2048) + 1
		p := make([]byte, n)
		for j := uint32(0); j < n; j++ {
			p[j] = byte(i) + 'A'
		}
		totalPayloadSize += len(p)
		payloads = append(payloads, p)
	}

	check := func(acc, user, subj string) {
		t.Helper()
		nc2 := natsConnect(t, s2.ClientURL(), nats.UserInfo(user, "pwd"))
		defer nc2.Close()
		sub := natsSubSync(t, nc2, subj)
		natsFlush(t, nc2)
		checkSubInterest(t, s1, acc, subj, time.Second)

		nc1 := natsConnect(t, s1.ClientURL(), nats.UserInfo(user, "pwd"))
		defer nc1.Close()

		for i := 0; i < count; i++ {
			natsPub(t, nc1, subj, payloads[i])
		}
		for i := 0; i < count; i++ {
			m := natsNexMsg(t, sub, time.Second)
			if !bytes.Equal(m.Data, payloads[i]) {
				t.Fatalf("Expected payload %q - got %q", payloads[i], m.Data)
			}
		}

		// Also check that the leafnode stats shows that compression likely occurred
		var out int
		s1.mu.RLock()
		for _, l := range s1.leafs {
			l.mu.Lock()
			if l.acc.Name == acc && l.nc != nil {
				nc := l.nc.(*testConnSentBytes)
				nc.Lock()
				out = nc.sent
				nc.sent = 0
				nc.Unlock()
			}
			l.mu.Unlock()
		}
		s1.mu.RUnlock()
		// Except for account "C", where compression should be off,
		// "out" should at least be smaller than totalPayloadSize, use 20%.
		if acc == "C" {
			if int(out) < totalPayloadSize {
				t.Fatalf("Expected s1's sent bytes to be at least payload size (%v), got %v", totalPayloadSize, out)
			}
		} else {
			limit := totalPayloadSize * 80 / 100
			if int(out) > limit {
				t.Fatalf("Expected s1's sent bytes to be less than %v, got %v (total payload was %v)", limit, out, totalPayloadSize)
			}
		}
	}
	check("A", "a", "foo")
	check("B", "b", "bar")
	check("C", "c", "baz")

	// Check compression settings. S1 should always be s2_fast, except for account "C"
	// since "C" wanted compression "off"
	l, err := s1.Leafz(nil)
	require_NoError(t, err)
	for _, r := range l.Leafs {
		switch r.Account {
		case "C":
			if r.Compression != CompressionOff {
				t.Fatalf("Expected compression of remote for C account to be %q, got %q", CompressionOff, r.Compression)
			}
		default:
			if r.Compression != CompressionS2Fast {
				t.Fatalf("Expected compression of remote for %s account to be %q, got %q", r.Account, CompressionS2Fast, r.Compression)
			}
		}
	}

	l, err = s2.Leafz(nil)
	require_NoError(t, err)
	for _, r := range l.Leafs {
		switch r.Account {
		case "A":
			if r.Compression != CompressionS2Better {
				t.Fatalf("Expected compression for A account to be %q, got %q", CompressionS2Better, r.Compression)
			}
		case "B":
			if r.Compression != CompressionS2Best {
				t.Fatalf("Expected compression for B account to be %q, got %q", CompressionS2Best, r.Compression)
			}
		case "C":
			if r.Compression != CompressionOff {
				t.Fatalf("Expected compression for C account to be %q, got %q", CompressionOff, r.Compression)
			}
		}
	}
}

func BenchmarkLeafNodeCompression(b *testing.B) {
	conf1 := createConfFile(b, []byte(`
		port: -1
		server_name: "Hub"
		accounts {
			A { users: [{user: a, password: pwd}] }
			B { users: [{user: b, password: pwd}] }
			C { users: [{user: c, password: pwd}] }
			D { users: [{user: d, password: pwd}] }
		}
		leafnodes {
			port: -1
		}
	`))
	s1, o1 := RunServerWithConfig(conf1)
	defer s1.Shutdown()

	port := o1.LeafNode.Port
	conf2 := createConfFile(b, []byte(fmt.Sprintf(`
		port: -1
		server_name: "Spoke"
		accounts {
			A { users: [{user: a, password: pwd}] }
			B { users: [{user: b, password: pwd}] }
			C { users: [{user: c, password: pwd}] }
			D { users: [{user: d, password: pwd}] }
		}
		leafnodes {
			remotes [
				{ url: "nats://a:pwd@127.0.0.1:%d", account: "A", compression: s2_better }
				{ url: "nats://b:pwd@127.0.0.1:%d", account: "B", compression: s2_best }
				{ url: "nats://c:pwd@127.0.0.1:%d", account: "C", compression: s2_fast }
				{ url: "nats://d:pwd@127.0.0.1:%d", account: "D", compression: off }
			]
		}
	`, port, port, port, port)))
	s2, _ := RunServerWithConfig(conf2)
	defer s2.Shutdown()

	checkLeafNodeConnectedCount(b, s1, 4)
	checkLeafNodeConnectedCount(b, s2, 4)

	l, err := s2.Leafz(nil)
	require_NoError(b, err)
	for _, r := range l.Leafs {
		switch {
		case r.Account == "A" && r.Compression == CompressionS2Better:
		case r.Account == "B" && r.Compression == CompressionS2Best:
		case r.Account == "C" && r.Compression == CompressionS2Fast:
		case r.Account == "D" && r.Compression == CompressionOff:
		default:
			b.Fatalf("Account %q had incorrect compression mode %q on leaf connection", r.Account, r.Compression)
		}
	}

	msg := make([]byte, 1024)
	for _, p := range []struct {
		algo string
		user string
	}{
		{"Better", "a"},
		{"Best", "b"},
		{"Fast", "c"},
		{"Off", "d"},
	} {
		nc1 := natsConnect(b, s1.ClientURL(), nats.UserInfo(p.user, "pwd"))
		nc2 := natsConnect(b, s2.ClientURL(), nats.UserInfo(p.user, "pwd"))

		sub, err := nc1.SubscribeSync("foo")
		require_NoError(b, err)

		time.Sleep(time.Second)

		b.Run(p.algo, func(b *testing.B) {
			start := time.Now()

			for i := 0; i < b.N; i++ {
				err = nc2.Publish("foo", msg)
				require_NoError(b, err)

				_, err = sub.NextMsg(time.Second)
				require_NoError(b, err)
			}

			b.ReportMetric(float64(len(msg)*b.N)/1024/1024, "MB")
			b.ReportMetric(float64(len(msg)*b.N)/1024/1024/float64(time.Since(start).Seconds()), "MB/sec")
		})

		nc1.Close()
		nc2.Close()
	}
}

func TestLeafNodeCompressionMatrixModes(t *testing.T) {
	for _, test := range []struct {
		name       string
		s1         string
		s2         string
		s1Expected string
		s2Expected string
	}{
		{"off off", "off", "off", CompressionOff, CompressionOff},
		{"off accept", "off", "accept", CompressionOff, CompressionOff},
		{"off on", "off", "on", CompressionOff, CompressionOff},
		{"off better", "off", "better", CompressionOff, CompressionOff},
		{"off best", "off", "best", CompressionOff, CompressionOff},

		{"accept off", "accept", "off", CompressionOff, CompressionOff},
		{"accept accept", "accept", "accept", CompressionOff, CompressionOff},
		// Note: "on", means s2_auto, which will mean uncompressed since RTT is low.
		{"accept on", "accept", "on", CompressionS2Fast, CompressionS2Uncompressed},
		{"accept better", "accept", "better", CompressionS2Better, CompressionS2Better},
		{"accept best", "accept", "best", CompressionS2Best, CompressionS2Best},

		{"on off", "on", "off", CompressionOff, CompressionOff},
		{"on accept", "on", "accept", CompressionS2Uncompressed, CompressionS2Fast},
		{"on on", "on", "on", CompressionS2Uncompressed, CompressionS2Uncompressed},
		{"on better", "on", "better", CompressionS2Uncompressed, CompressionS2Better},
		{"on best", "on", "best", CompressionS2Uncompressed, CompressionS2Best},

		{"better off", "better", "off", CompressionOff, CompressionOff},
		{"better accept", "better", "accept", CompressionS2Better, CompressionS2Better},
		{"better on", "better", "on", CompressionS2Better, CompressionS2Uncompressed},
		{"better better", "better", "better", CompressionS2Better, CompressionS2Better},
		{"better best", "better", "best", CompressionS2Better, CompressionS2Best},

		{"best off", "best", "off", CompressionOff, CompressionOff},
		{"best accept", "best", "accept", CompressionS2Best, CompressionS2Best},
		{"best on", "best", "on", CompressionS2Best, CompressionS2Uncompressed},
		{"best better", "best", "better", CompressionS2Best, CompressionS2Better},
		{"best best", "best", "best", CompressionS2Best, CompressionS2Best},
	} {
		t.Run(test.name, func(t *testing.T) {
			conf1 := createConfFile(t, []byte(fmt.Sprintf(`
				port: -1
				server_name: "A"
				leafnodes {
					port: -1
					compression: %s
				}
			`, test.s1)))
			s1, o1 := RunServerWithConfig(conf1)
			defer s1.Shutdown()

			conf2 := createConfFile(t, []byte(fmt.Sprintf(`
				port: -1
				server_name: "B"
				leafnodes {
					remotes: [
						{url: "nats://127.0.0.1:%d", compression: %s}
					]
				}
			`, o1.LeafNode.Port, test.s2)))
			s2, _ := RunServerWithConfig(conf2)
			defer s2.Shutdown()

			checkLeafNodeConnected(t, s2)

			nc1 := natsConnect(t, s1.ClientURL())
			defer nc1.Close()

			nc2 := natsConnect(t, s2.ClientURL())
			defer nc2.Close()

			payload := make([]byte, 128)
			check := func(ncp, ncs *nats.Conn, subj string, s *Server) {
				t.Helper()
				sub := natsSubSync(t, ncs, subj)
				checkSubInterest(t, s, globalAccountName, subj, time.Second)
				natsPub(t, ncp, subj, payload)
				natsNexMsg(t, sub, time.Second)

				for _, srv := range []*Server{s1, s2} {
					lz, err := srv.Leafz(nil)
					require_NoError(t, err)
					var expected string
					if srv == s1 {
						expected = test.s1Expected
					} else {
						expected = test.s2Expected
					}
					if cm := lz.Leafs[0].Compression; cm != expected {
						t.Fatalf("Server %s - expected compression %q, got %q", srv, expected, cm)
					}
				}
			}
			check(nc1, nc2, "foo", s1)
			check(nc2, nc1, "bar", s2)
		})
	}
}

func TestLeafNodeCompressionWithOlderServer(t *testing.T) {
	tmpl1 := `
		port: -1
		server_name: "A"
		leafnodes {
			port: -1
			compression: "%s"
		}
	`
	conf1 := createConfFile(t, []byte(fmt.Sprintf(tmpl1, CompressionS2Fast)))
	s1, o1 := RunServerWithConfig(conf1)
	defer s1.Shutdown()

	tmpl2 := `
		port: -1
		server_name: "B"
		leafnodes {
			remotes [
				{url: "nats://127.0.0.1:%d", compression: "%s"}
			]
		}
	`
	conf2 := createConfFile(t, []byte(fmt.Sprintf(tmpl2, o1.LeafNode.Port, CompressionNotSupported)))
	s2, _ := RunServerWithConfig(conf2)
	defer s2.Shutdown()

	checkLeafNodeConnected(t, s2)

	getLeafCompMode := func(s *Server) string {
		var cm string
		s.mu.RLock()
		defer s.mu.RUnlock()
		for _, l := range s1.leafs {
			l.mu.Lock()
			cm = l.leaf.compression
			l.mu.Unlock()
			return cm
		}
		return _EMPTY_
	}
	for _, s := range []*Server{s1, s2} {
		if cm := getLeafCompMode(s); cm != CompressionNotSupported {
			t.Fatalf("Expected compression not supported, got %q", cm)
		}
	}

	s2.Shutdown()
	s1.Shutdown()

	conf1 = createConfFile(t, []byte(fmt.Sprintf(tmpl1, CompressionNotSupported)))
	s1, o1 = RunServerWithConfig(conf1)
	defer s1.Shutdown()

	conf2 = createConfFile(t, []byte(fmt.Sprintf(tmpl2, o1.LeafNode.Port, CompressionS2Fast)))
	s2, _ = RunServerWithConfig(conf2)
	defer s2.Shutdown()

	checkLeafNodeConnected(t, s2)
	for _, s := range []*Server{s1, s2} {
		if cm := getLeafCompMode(s); cm != CompressionNotSupported {
			t.Fatalf("Expected compression not supported, got %q", cm)
		}
	}
}

func TestLeafNodeNoCompressionWithWebsocket(t *testing.T) {
	for _, test := range []struct {
		name         string
		acceptWSComp bool
		remoteWSComp bool
		expectS2     bool
	}{
		{"both ws compression", true, true, false},
		{"only accept ws compression", true, false, true},
		{"only remote ws compression", false, true, true},
		{"neither ws compression", false, false, true},
	} {
		t.Run(test.name, func(t *testing.T) {
			conf1 := createConfFile(t, []byte(fmt.Sprintf(`
				port: -1
				server_name: "A"
				websocket {
					listen: "127.0.0.1:-1"
					no_tls: true
					compression: %v
				}
				leafnodes {
					port: -1
					compression: "s2_fast"
				}
			`, test.acceptWSComp)))
			s1, o1 := RunServerWithConfig(conf1)
			defer s1.Shutdown()

			conf2 := createConfFile(t, []byte(fmt.Sprintf(`
				port: -1
				server_name: "B"
				leafnodes {
					remotes [
						{url: "ws://127.0.0.1:%d", ws_compression: %v, compression: "s2_fast"}
					]
				}
			`, o1.Websocket.Port, test.remoteWSComp)))
			s2, _ := RunServerWithConfig(conf2)
			defer s2.Shutdown()

			checkLeafNodeConnected(t, s2)

			getLeafState := func(s *Server) (string, bool) {
				var cm string
				var wsComp bool
				s.mu.RLock()
				defer s.mu.RUnlock()
				for _, l := range s.leafs {
					l.mu.Lock()
					cm = l.leaf.compression
					if l.ws != nil {
						wsComp = l.ws.compress
					}
					l.mu.Unlock()
					return cm, wsComp
				}
				return _EMPTY_, false
			}
			expected := CompressionOff
			if test.expectS2 {
				expected = CompressionS2Fast
			}
			// WS compression is only actually used when both sides opted in.
			wantWSComp := test.acceptWSComp && test.remoteWSComp
			for _, s := range []*Server{s1, s2} {
				cm, wsComp := getLeafState(s)
				if cm != expected {
					t.Fatalf("Expected leaf compression to be %q, got %q", expected, cm)
				}
				if wsComp != wantWSComp {
					t.Fatalf("Expected ws.compress to be %v, got %v", wantWSComp, wsComp)
				}
			}
		})
	}
}

// Verifies that the initial INFO sent by the accept side over a WS leaf
// connection does not advertise leafnode compression when WS compression is
// negotiated. An older soliciting peer (without the WS-aware patch) would
// honor an advertised mode, switch to S2, and then deadlock waiting for a
// compressed INFO response that we never send.
func TestLeafNodeWSAcceptInitialInfoSuppressesCompression(t *testing.T) {
	for _, test := range []struct {
		name         string
		negotiatePMC bool
		expectedComp string
	}{
		{"ws compression negotiated", true, _EMPTY_},
		{"ws compression not negotiated", false, CompressionS2Fast},
	} {
		t.Run(test.name, func(t *testing.T) {
			conf := createConfFile(t, []byte(`
				port: -1
				server_name: "A"
				websocket {
					listen: "127.0.0.1:-1"
					no_tls: true
					compression: true
				}
				leafnodes {
					port: -1
					compression: "s2_fast"
				}
			`))
			s, o := RunServerWithConfig(conf)
			defer s.Shutdown()

			addr := fmt.Sprintf("127.0.0.1:%d", o.Websocket.Port)
			c, err := net.Dial("tcp", addr)
			require_NoError(t, err)
			defer c.Close()

			wsKey, err := wsMakeChallengeKey()
			require_NoError(t, err)

			var reqBuf bytes.Buffer
			fmt.Fprintf(&reqBuf, "GET %s HTTP/1.1\r\n", leafNodeWSPath)
			fmt.Fprintf(&reqBuf, "Host: %s\r\n", addr)
			reqBuf.WriteString("Upgrade: websocket\r\n")
			reqBuf.WriteString("Connection: Upgrade\r\n")
			fmt.Fprintf(&reqBuf, "Sec-WebSocket-Key: %s\r\n", wsKey)
			reqBuf.WriteString("Sec-WebSocket-Version: 13\r\n")
			if test.negotiatePMC {
				fmt.Fprintf(&reqBuf, "Sec-WebSocket-Extensions: %s\r\n", wsPMCReqHeaderValue)
			}
			reqBuf.WriteString("\r\n")

			require_NoError(t, c.SetDeadline(time.Now().Add(2*time.Second)))
			_, err = c.Write(reqBuf.Bytes())
			require_NoError(t, err)

			br := bufio.NewReader(c)
			resp, err := http.ReadResponse(br, nil)
			require_NoError(t, err)
			resp.Body.Close()
			require_True(t, resp.StatusCode == http.StatusSwitchingProtocols)

			pmcAccepted := strings.Contains(resp.Header.Get("Sec-WebSocket-Extensions"), wsPMCExtension)
			if test.negotiatePMC != pmcAccepted {
				t.Fatalf("Expected PMC negotiated=%v, got %v", test.negotiatePMC, pmcAccepted)
			}

			// Read the first WS frame header (binary, possibly compressed).
			b0, err := br.ReadByte()
			require_NoError(t, err)
			require_True(t, b0&wsFinalBit != 0)
			require_True(t, wsOpCode(b0&0xF) == wsBinaryMessage)
			require_True(t, (b0&wsRsv1Bit != 0) == pmcAccepted)

			b1, err := br.ReadByte()
			require_NoError(t, err)
			require_True(t, b1&wsMaskBit == 0)
			plen := int(b1 & 0x7F)
			switch plen {
			case 126:
				var b [2]byte
				_, err = io.ReadFull(br, b[:])
				require_NoError(t, err)
				plen = int(b[0])<<8 | int(b[1])
			case 127:
				t.Fatalf("Frame too large")
			}
			payload := make([]byte, plen)
			_, err = io.ReadFull(br, payload)
			require_NoError(t, err)

			if pmcAccepted {
				// Append the 4-byte trailer stripped by permessage-deflate plus
				// a final empty block so flate.NewReader sees end-of-stream.
				payload = append(payload, 0x00, 0x00, 0xff, 0xff, 0x01, 0x00, 0x00, 0xff, 0xff)
				fr := flate.NewReader(bytes.NewReader(payload))
				payload, err = io.ReadAll(fr)
				require_NoError(t, err)
				fr.Close()
			}

			require_True(t, bytes.HasPrefix(payload, []byte("INFO ")))
			payload = bytes.TrimSuffix(payload, []byte(_CRLF_))
			var info Info
			require_NoError(t, json.Unmarshal(payload[5:], &info))
			if info.Compression != test.expectedComp {
				t.Fatalf("Expected info.Compression=%q, got %q", test.expectedComp, info.Compression)
			}
		})
	}
}

func TestLeafNodeCompressionAuto(t *testing.T) {
	for _, test := range []struct {
		name          string
		s1Ping        string
		s1Compression string
		s2Ping        string
		s2Compression string
		checkS1       bool
	}{
		{"remote side", "10s", CompressionS2Fast, "100ms", "{mode: s2_auto, rtt_thresholds: [10ms, 20ms, 30ms]}", false},
		{"accept side", "100ms", "{mode: s2_auto, rtt_thresholds: [10ms, 20ms, 30ms]}", "10s", CompressionS2Fast, true},
	} {
		t.Run(test.name, func(t *testing.T) {
			conf1 := createConfFile(t, []byte(fmt.Sprintf(`
				port: -1
				server_name: "A"
				ping_interval: "%s"
				leafnodes {
					port: -1
					compression: %s
				}
			`, test.s1Ping, test.s1Compression)))
			s1, o1 := RunServerWithConfig(conf1)
			defer s1.Shutdown()

			// Start with 0ms RTT
			np := createNetProxy(0, 1024*1024*1024, 1024*1024*1024, fmt.Sprintf("nats://127.0.0.1:%d", o1.LeafNode.Port), true)

			conf2 := createConfFile(t, []byte(fmt.Sprintf(`
				port: -1
				server_name: "B"
				ping_interval: "%s"
				leafnodes {
					remotes [
						{url: %s, compression %s}
					]
				}
			`, test.s2Ping, np.routeURL(), test.s2Compression)))
			s2, _ := RunServerWithConfig(conf2)
			defer s2.Shutdown()
			defer np.stop()

			checkLeafNodeConnected(t, s2)

			checkComp := func(expected string) {
				t.Helper()
				var s *Server
				if test.checkS1 {
					s = s1
				} else {
					s = s2
				}
				checkFor(t, 2*time.Second, 15*time.Millisecond, func() error {
					s.mu.RLock()
					defer s.mu.RUnlock()
					for _, l := range s.leafs {
						l.mu.Lock()
						cm := l.leaf.compression
						l.mu.Unlock()
						if cm != expected {
							return fmt.Errorf("Leaf %v compression mode expected to be %q, got %q", l, expected, cm)
						}
					}
					return nil
				})
			}
			checkComp(CompressionS2Uncompressed)

			// Change the proxy RTT and we should get compression "fast"
			np.updateRTT(15 * time.Millisecond)
			checkComp(CompressionS2Fast)

			// Now 25ms, and get "better"
			np.updateRTT(25 * time.Millisecond)
			checkComp(CompressionS2Better)

			// Above 35 and we should get "best"
			np.updateRTT(35 * time.Millisecond)
			checkComp(CompressionS2Best)

			// Down to 1ms and again should get "uncompressed"
			np.updateRTT(1 * time.Millisecond)
			checkComp(CompressionS2Uncompressed)
		})
	}

	// Make sure that if compression is off on one side, the update of RTT does
	// not trigger a compression change.
	conf1 := createConfFile(t, []byte(`
		port: -1
		server_name: "A"
		leafnodes {
			port: -1
			compression: off
		}
	`))
	s1, o1 := RunServerWithConfig(conf1)
	defer s1.Shutdown()

	// Start with 0ms RTT
	np := createNetProxy(0, 1024*1024*1024, 1024*1024*1024, fmt.Sprintf("nats://127.0.0.1:%d", o1.LeafNode.Port), true)

	conf2 := createConfFile(t, []byte(fmt.Sprintf(`
		port: -1
		server_name: "B"
		ping_interval: "50ms"
		leafnodes {
			remotes [
				{url: %s, compression s2_auto}
			]
		}
	`, np.routeURL())))
	s2, _ := RunServerWithConfig(conf2)
	defer s2.Shutdown()
	defer np.stop()

	checkLeafNodeConnected(t, s2)

	// Even with a bug of updating compression level while it should have been
	// off, the check done below would almost always pass because after
	// reconnecting, there could be a chance to get at first compression set
	// to "off". So we will double check that the leaf node CID did not change
	// at the end of the test.
	getCID := func() uint64 {
		s2.mu.RLock()
		defer s2.mu.RUnlock()
		for _, l := range s2.leafs {
			l.mu.Lock()
			cid := l.cid
			l.mu.Unlock()
			return cid
		}
		return 0
	}
	oldCID := getCID()

	checkCompOff := func() {
		t.Helper()
		checkFor(t, 2*time.Second, 15*time.Millisecond, func() error {
			s2.mu.RLock()
			defer s2.mu.RUnlock()
			if len(s2.leafs) != 1 {
				return fmt.Errorf("Leaf not currently connected")
			}
			for _, l := range s2.leafs {
				l.mu.Lock()
				cm := l.leaf.compression
				l.mu.Unlock()
				if cm != CompressionOff {
					return fmt.Errorf("Leaf %v compression mode expected to be %q, got %q", l, CompressionOff, cm)
				}
			}
			return nil
		})
	}
	checkCompOff()

	// Now change RTT and again, make sure that it is still off
	np.updateRTT(20 * time.Millisecond)
	time.Sleep(100 * time.Millisecond)
	checkCompOff()
	if cid := getCID(); cid != oldCID {
		t.Fatalf("Leafnode has reconnected, cid was %v, now %v", oldCID, cid)
	}
}

func TestLeafNodeCompressionWithWSCompression(t *testing.T) {
	conf1 := createConfFile(t, []byte(`
		port: -1
		server_name: "A"
		websocket {
			port: -1
			no_tls: true
			compression: true
		}
		leafnodes {
			port: -1
			compression: s2_fast
		}
	`))
	s1, o1 := RunServerWithConfig(conf1)
	defer s1.Shutdown()

	conf2 := createConfFile(t, []byte(fmt.Sprintf(`
		port: -1
		server_name: "B"
		leafnodes {
			remotes [
				{
					url: "ws://127.0.0.1:%d"
					ws_compression: true
					compression: s2_fast
				}
			]
		}
	`, o1.Websocket.Port)))
	s2, _ := RunServerWithConfig(conf2)
	defer s2.Shutdown()

	checkLeafNodeConnected(t, s2)

	nc1 := natsConnect(t, s1.ClientURL())
	defer nc1.Close()

	sub := natsSubSync(t, nc1, "foo")
	checkSubInterest(t, s2, globalAccountName, "foo", time.Second)

	nc2 := natsConnect(t, s2.ClientURL())
	defer nc2.Close()

	payload := make([]byte, 1024)
	for i := 0; i < len(payload); i++ {
		payload[i] = 'A'
	}
	natsPub(t, nc2, "foo", payload)
	msg := natsNexMsg(t, sub, time.Second)
	require_True(t, len(msg.Data) == 1024)
	for i := 0; i < len(msg.Data); i++ {
		if msg.Data[i] != 'A' {
			t.Fatalf("Invalid msg: %s", msg.Data)
		}
	}
}

func TestLeafNodeCompressionWithWSGetNeedsData(t *testing.T) {
	conf1 := createConfFile(t, []byte(`
		port: -1
		server_name: "A"
		websocket {
			port: -1
			no_tls: true
		}
		leafnodes {
			port: -1
			compression: s2_fast
		}
	`))
	srv1, o1 := RunServerWithConfig(conf1)
	defer srv1.Shutdown()

	conf2 := createConfFile(t, []byte(fmt.Sprintf(`
		port: -1
		server_name: "B"
		leafnodes {
			remotes [
				{
					url: "ws://127.0.0.1:%d"
					ws_no_masking: true
					compression: s2_fast
				}
			]
		}
	`, o1.Websocket.Port)))
	srv2, _ := RunServerWithConfig(conf2)
	defer srv2.Shutdown()

	checkLeafNodeConnected(t, srv2)

	nc1 := natsConnect(t, srv1.ClientURL())
	defer nc1.Close()

	sub := natsSubSync(t, nc1, "foo")
	checkSubInterest(t, srv2, globalAccountName, "foo", time.Second)

	// We want to have the payload more than 126 bytes so that the websocket
	// code need to read 2 bytes for the length. See below.
	payload := "ABCDEFGHIJKLMNOPQRSTUVWXYZABCDEFGHIJKLMNOPQRSTUVWXYZABCDEFGHIJKLMNOPQRSTUVWXYZABCDEFGHIJKLMNOPQRSTUVWXYZABCDEFGHIJKLMNOPQRSTUVWXYZABCDEFGHIJKLMNOPQRSTUVWXYZ"
	sentBytes := []byte("LMSG foo 156\r\n" + payload + "\r\n")
	h, _ := wsCreateFrameHeader(false, false, wsBinaryMessage, len(sentBytes))
	combined := &bytes.Buffer{}
	combined.Write(h)
	combined.Write(sentBytes)
	toSend := combined.Bytes()

	// We will make a compressed block that cuts the websocket header that
	// makes the reader want to read bytes directly from the connection.
	// We want to make sure that we are not going to get compressed data
	// without going through the (de)compress library. So for that, compress
	// the first 3 bytes.
	b := &bytes.Buffer{}
	w := s2.NewWriter(b)
	w.Write(toSend[:3])
	w.Close()

	var nc net.Conn
	srv2.mu.RLock()
	for _, l := range srv2.leafs {
		l.mu.Lock()
		nc = l.nc
		l.mu.Unlock()
	}
	srv2.mu.RUnlock()

	nc.Write(b.Bytes())

	// Pause to make sure other side just gets a partial of the whole WS frame.
	time.Sleep(100 * time.Millisecond)

	b.Reset()
	w.Reset(b)
	w.Write(toSend[3:])
	w.Close()

	nc.Write(b.Bytes())

	msg := natsNexMsg(t, sub, time.Second)
	require_True(t, len(msg.Data) == 156)
	require_Equal(t, string(msg.Data), payload)
}

func TestLeafNodeCompressionAuthTimeout(t *testing.T) {
	hconf := createConfFile(t, []byte(`
		port: -1
		server_name: "hub"
		leafnodes {
			port: -1
			authorization {
				timeout: 0.75
			}
		}
	`))
	sh, oh := RunServerWithConfig(hconf)
	defer sh.Shutdown()

	sconfTmpl := `
		port: -1
		server_name: "%s"
		cluster {
			port: -1
			name: "spoke"
			%s
		}
		leafnodes {
			port: -1
			remotes [
				{ url: "nats://127.0.0.1:%d" }
			]
		}
	`
	s1conf := createConfFile(t, []byte(fmt.Sprintf(sconfTmpl, "SP1", _EMPTY_, oh.LeafNode.Port)))
	s1, o1 := RunServerWithConfig(s1conf)
	defer s1.Shutdown()

	s2conf := createConfFile(t, []byte(fmt.Sprintf(sconfTmpl, "SP2", fmt.Sprintf("routes: [\"nats://127.0.0.1:%d\"]", o1.Cluster.Port), oh.LeafNode.Port)))
	s2, _ := RunServerWithConfig(s2conf)
	defer s2.Shutdown()

	checkClusterFormed(t, s1, s2)

	checkLeafNodeConnected(t, s1)
	checkLeafNodeConnected(t, s2)

	getCID := func(s *Server) uint64 {
		s.mu.RLock()
		defer s.mu.RUnlock()
		var cid uint64
		for _, l := range s.leafs {
			l.mu.Lock()
			cid = l.cid
			l.mu.Unlock()
		}
		return cid
	}
	leaf1 := getCID(s1)
	leaf2 := getCID(s2)

	// Wait for more than auth timeout
	time.Sleep(time.Second)

	checkLeafNodeConnected(t, s1)
	checkLeafNodeConnected(t, s2)
	if l1 := getCID(s1); l1 != leaf1 {
		t.Fatalf("Leaf connection first connection had CID %v, now %v", leaf1, l1)
	}
	if l2 := getCID(s2); l2 != leaf2 {
		t.Fatalf("Leaf connection first connection had CID %v, now %v", leaf2, l2)
	}
}

func TestLeafNodeWithWeightedDQRequestsToSuperClusterWithSeparateAccounts(t *testing.T) {
	sc := createJetStreamSuperClusterWithTemplate(t, jsClusterAccountsTempl, 3, 2)
	defer sc.shutdown()

	// Now create a leafnode cluster that has 2 LNs, one to each cluster but on separate accounts, ONE and TWO.
	lnTmpl := `
		listen: 127.0.0.1:-1
		server_name: %s
		jetstream: {max_mem_store: 256MB, max_file_store: 2GB, store_dir: '%s'}

		{{leaf}}

		cluster {
			name: %s
			listen: 127.0.0.1:%d
			routes = [%s]
		}

		accounts { $SYS { users = [ { user: "admin", pass: "s3cr3t!" } ] }}
		`

	leafFrag := `
		leaf {
			listen: 127.0.0.1:-1
			remotes [
				{ urls: [ %s ] }
				{ urls: [ %s ] }
			]
		}`

	// We want to have two leaf node connections that join to the same local account on the leafnode servers,
	// but connect to different accounts in different clusters.
	c1 := sc.clusters[0] // Will connect to account ONE
	c2 := sc.clusters[1] // Will connect to account TWO

	genLeafTmpl := func(tmpl string) string {
		t.Helper()

		var ln1, ln2 []string
		for _, s := range c1.servers {
			if s.ClusterName() != c1.name {
				continue
			}
			ln := s.getOpts().LeafNode
			ln1 = append(ln1, fmt.Sprintf("nats://one:p@%s:%d", ln.Host, ln.Port))
		}

		for _, s := range c2.servers {
			if s.ClusterName() != c2.name {
				continue
			}
			ln := s.getOpts().LeafNode
			ln2 = append(ln2, fmt.Sprintf("nats://two:p@%s:%d", ln.Host, ln.Port))
		}
		return strings.Replace(tmpl, "{{leaf}}", fmt.Sprintf(leafFrag, strings.Join(ln1, ", "), strings.Join(ln2, ", ")), 1)
	}

	tmpl := strings.Replace(lnTmpl, "store_dir:", fmt.Sprintf(`domain: "%s", store_dir:`, "SA"), 1)
	tmpl = genLeafTmpl(tmpl)

	ln := createJetStreamCluster(t, tmpl, "SA", "SA-", 3, 22280, false)
	ln.waitOnClusterReady()
	defer ln.shutdown()

	for _, s := range ln.servers {
		checkLeafNodeConnectedCount(t, s, 2)
	}

	// Now connect DQ subscribers to each cluster and they separate accounts, and make sure we get the right behavior, balanced between
	// them when requests originate from the leaf cluster.

	// Create 5 clients for each cluster / account
	var c1c, c2c []*nats.Conn
	for i := 0; i < 5; i++ {
		nc1, _ := jsClientConnect(t, c1.randomServer(), nats.UserInfo("one", "p"))
		defer nc1.Close()
		c1c = append(c1c, nc1)
		nc2, _ := jsClientConnect(t, c2.randomServer(), nats.UserInfo("two", "p"))
		defer nc2.Close()
		c2c = append(c2c, nc2)
	}

	createSubs := func(num int, conns []*nats.Conn) (subs []*nats.Subscription) {
		for i := 0; i < num; i++ {
			nc := conns[rand.IntN(len(conns))]
			sub, err := nc.QueueSubscribeSync("REQUEST", "MC")
			require_NoError(t, err)
			subs = append(subs, sub)
			nc.Flush()
		}
		// Let subs propagate.
		time.Sleep(100 * time.Millisecond)
		return subs
	}
	closeSubs := func(subs []*nats.Subscription) {
		for _, sub := range subs {
			sub.Unsubscribe()
		}
	}

	// Simple test first.
	subs1 := createSubs(1, c1c)
	defer closeSubs(subs1)
	subs2 := createSubs(1, c2c)
	defer closeSubs(subs2)

	sendRequests := func(num int) {
		t.Helper()
		// Now connect to the leaf cluster and send some requests.
		nc, _ := jsClientConnect(t, ln.randomServer())
		defer nc.Close()

		for i := 0; i < num; i++ {
			require_NoError(t, nc.Publish("REQUEST", []byte("HELP")))
		}
		nc.Flush()
	}

	pending := func(subs []*nats.Subscription) (total int) {
		t.Helper()
		for _, sub := range subs {
			n, _, err := sub.Pending()
			require_NoError(t, err)
			total += n
		}
		return total
	}

	num := 1000
	checkAllReceived := func() error {
		total := pending(subs1) + pending(subs2)
		if total == num {
			return nil
		}
		return fmt.Errorf("Not all received: %d vs %d", total, num)
	}

	checkBalanced := func(total, pc1, pc2 int) {
		t.Helper()
		tf := float64(total)
		e1 := tf * (float64(pc1) / 100.00)
		e2 := tf * (float64(pc2) / 100.00)
		delta := tf / 10
		p1 := float64(pending(subs1))
		if p1 < e1-delta || p1 > e1+delta {
			t.Fatalf("Value out of range for subs1, expected %v got %v", e1, p1)
		}
		p2 := float64(pending(subs2))
		if p2 < e2-delta || p2 > e2+delta {
			t.Fatalf("Value out of range for subs2, expected %v got %v", e2, p2)
		}
	}

	// Now connect to the leaf cluster and send some requests.

	// Simple 50/50
	sendRequests(num)
	checkFor(t, time.Second, 200*time.Millisecond, checkAllReceived)
	checkBalanced(num, 50, 50)

	closeSubs(subs1)
	closeSubs(subs2)

	// Now test unbalanced. 10/90
	subs1 = createSubs(1, c1c)
	defer closeSubs(subs1)
	subs2 = createSubs(9, c2c)
	defer closeSubs(subs2)

	sendRequests(num)
	checkFor(t, time.Second, 200*time.Millisecond, checkAllReceived)
	checkBalanced(num, 10, 90)

	// Now test draining the subs as we are sending from an initial balanced situation simulating a draining of a cluster.

	closeSubs(subs1)
	closeSubs(subs2)
	subs1, subs2 = nil, nil

	// These subs slightly different.
	var r1, r2 atomic.Uint64
	for i := 0; i < 20; i++ {
		nc := c1c[rand.IntN(len(c1c))]
		sub, err := nc.QueueSubscribe("REQUEST", "MC", func(m *nats.Msg) { r1.Add(1) })
		require_NoError(t, err)
		subs1 = append(subs1, sub)
		nc.Flush()

		nc = c2c[rand.IntN(len(c2c))]
		sub, err = nc.QueueSubscribe("REQUEST", "MC", func(m *nats.Msg) { r2.Add(1) })
		require_NoError(t, err)
		subs2 = append(subs2, sub)
		nc.Flush()
	}
	defer closeSubs(subs1)
	defer closeSubs(subs2)

	nc, _ := jsClientConnect(t, ln.randomServer())
	defer nc.Close()

	for i, dindex := 0, 1; i < num; i++ {
		require_NoError(t, nc.Publish("REQUEST", []byte("HELP")))
		// Check if we have more to simulate draining.
		// Will drain within first ~100 requests using 20% rand test below.
		// Will leave 1 behind.
		if dindex < len(subs1)-1 && rand.IntN(6) > 4 {
			sub := subs1[dindex]
			dindex++
			sub.Drain()
		}
	}
	nc.Flush()

	checkFor(t, time.Second, 200*time.Millisecond, func() error {
		total := int(r1.Load() + r2.Load())
		if total == num {
			return nil
		}
		return fmt.Errorf("Not all received: %d vs %d", total, num)
	})
	require_True(t, r2.Load() > r1.Load())
}

func TestLeafNodeWithWeightedDQRequestsToSuperClusterWithStreamImportAccounts(t *testing.T) {
	tmpl := `
	listen: 127.0.0.1:-1

	server_name: %s
	jetstream: {max_mem_store: 256MB, max_file_store: 2GB, store_dir: '%s'}

	leaf { listen: 127.0.0.1:-1 }

	cluster {
		name: %s
		listen: 127.0.0.1:%d
		routes = [%s]
	}

	accounts {
		EFG {
			users = [ { user: "efg", pass: "p" } ]
			jetstream: enabled
			imports [
				{ stream: { account: STL, subject: "REQUEST"} }
				{ stream: { account: KSC, subject: "REQUEST"} }
			]
			exports [ { stream: "RESPONSE" } ]
		}
		STL {
			users = [ { user: "stl", pass: "p" } ]
			exports [ { stream: "REQUEST" } ]
			imports [ { stream: { account: EFG, subject: "RESPONSE"} } ]
		}
		KSC {
			users = [ { user: "ksc", pass: "p" } ]
			exports [ { stream: "REQUEST" } ]
			imports [ { stream: { account: EFG, subject: "RESPONSE"} } ]
		}
		$SYS { users = [ { user: "admin", pass: "s3cr3t!" } ] }
	}`

	sc := createJetStreamSuperClusterWithTemplate(t, tmpl, 5, 2)
	defer sc.shutdown()

	// Now create a leafnode cluster that has 2 LNs, one to each cluster but on separate accounts, STL and KSC.
	lnTmpl := `
		listen: 127.0.0.1:-1
		server_name: %s
		jetstream: {max_mem_store: 256MB, max_file_store: 2GB, store_dir: '%s'}

		{{leaf}}

		cluster {
			name: %s
			listen: 127.0.0.1:%d
			routes = [%s]
		}

		accounts { $SYS { users = [ { user: "admin", pass: "s3cr3t!" } ] }}
		`

	leafFrag := `
		leaf {
			listen: 127.0.0.1:-1
			remotes [
				{ urls: [ %s ] }
				{ urls: [ %s ] }
				{ urls: [ %s ] ; deny_export: [REQUEST, RESPONSE], deny_import: RESPONSE }
			]
		}`

	// We want to have two leaf node connections that join to the same local account on the leafnode servers,
	// but connect to different accounts in different clusters.
	c1 := sc.clusters[0] // Will connect to account KSC
	c2 := sc.clusters[1] // Will connect to account STL

	genLeafTmpl := func(tmpl string) string {
		t.Helper()

		var ln1, ln2, ln3 []string
		for _, s := range c1.servers {
			if s.ClusterName() != c1.name {
				continue
			}
			ln := s.getOpts().LeafNode
			ln1 = append(ln1, fmt.Sprintf("nats://ksc:p@%s:%d", ln.Host, ln.Port))
		}

		for _, s := range c2.servers {
			if s.ClusterName() != c2.name {
				continue
			}
			ln := s.getOpts().LeafNode
			ln2 = append(ln2, fmt.Sprintf("nats://stl:p@%s:%d", ln.Host, ln.Port))
			ln3 = append(ln3, fmt.Sprintf("nats://efg:p@%s:%d", ln.Host, ln.Port))
		}
		return strings.Replace(tmpl, "{{leaf}}", fmt.Sprintf(leafFrag, strings.Join(ln1, ", "), strings.Join(ln2, ", "), strings.Join(ln3, ", ")), 1)
	}

	tmpl = strings.Replace(lnTmpl, "store_dir:", fmt.Sprintf(`domain: "%s", store_dir:`, "SA"), 1)
	tmpl = genLeafTmpl(tmpl)

	ln := createJetStreamCluster(t, tmpl, "SA", "SA-", 3, 22280, false)
	ln.waitOnClusterReady()
	defer ln.shutdown()

	for _, s := range ln.servers {
		checkLeafNodeConnectedCount(t, s, 3)
	}

	// Now connect DQ subscribers to each cluster but to the global account.

	// Create 5 clients for each cluster / account
	var c1c, c2c []*nats.Conn
	for i := 0; i < 5; i++ {
		nc1, _ := jsClientConnect(t, c1.randomServer(), nats.UserInfo("efg", "p"))
		defer nc1.Close()
		c1c = append(c1c, nc1)
		nc2, _ := jsClientConnect(t, c2.randomServer(), nats.UserInfo("efg", "p"))
		defer nc2.Close()
		c2c = append(c2c, nc2)
	}

	createSubs := func(num int, conns []*nats.Conn) (subs []*nats.Subscription) {
		for i := 0; i < num; i++ {
			nc := conns[rand.IntN(len(conns))]
			sub, err := nc.QueueSubscribeSync("REQUEST", "MC")
			require_NoError(t, err)
			subs = append(subs, sub)
			nc.Flush()
		}
		// Let subs propagate.
		time.Sleep(100 * time.Millisecond)
		return subs
	}
	closeSubs := func(subs []*nats.Subscription) {
		for _, sub := range subs {
			sub.Unsubscribe()
		}
	}

	// Simple test first.
	subs1 := createSubs(1, c1c)
	defer closeSubs(subs1)
	subs2 := createSubs(1, c2c)
	defer closeSubs(subs2)

	sendRequests := func(num int) {
		t.Helper()
		// Now connect to the leaf cluster and send some requests.
		nc, _ := jsClientConnect(t, ln.randomServer())
		defer nc.Close()

		for i := 0; i < num; i++ {
			require_NoError(t, nc.Publish("REQUEST", []byte("HELP")))
		}
		nc.Flush()
	}

	pending := func(subs []*nats.Subscription) (total int) {
		t.Helper()
		for _, sub := range subs {
			n, _, err := sub.Pending()
			require_NoError(t, err)
			total += n
		}
		return total
	}

	num := 1000
	checkAllReceived := func() error {
		total := pending(subs1) + pending(subs2)
		if total == num {
			return nil
		}
		return fmt.Errorf("Not all received: %d vs %d", total, num)
	}

	checkBalanced := func(total, pc1, pc2 int) {
		t.Helper()
		tf := float64(total)
		e1 := tf * (float64(pc1) / 100.00)
		e2 := tf * (float64(pc2) / 100.00)
		delta := tf / 10
		p1 := float64(pending(subs1))
		if p1 < e1-delta || p1 > e1+delta {
			t.Fatalf("Value out of range for subs1, expected %v got %v", e1, p1)
		}
		p2 := float64(pending(subs2))
		if p2 < e2-delta || p2 > e2+delta {
			t.Fatalf("Value out of range for subs2, expected %v got %v", e2, p2)
		}
	}

	// Now connect to the leaf cluster and send some requests.

	// Simple 50/50
	sendRequests(num)
	checkFor(t, time.Second, 200*time.Millisecond, checkAllReceived)
	checkBalanced(num, 50, 50)

	closeSubs(subs1)
	closeSubs(subs2)

	// Now test unbalanced. 10/90
	subs1 = createSubs(1, c1c)
	defer closeSubs(subs1)
	subs2 = createSubs(9, c2c)
	defer closeSubs(subs2)

	sendRequests(num)
	checkFor(t, time.Second, 200*time.Millisecond, checkAllReceived)
	checkBalanced(num, 10, 90)

	closeSubs(subs1)
	closeSubs(subs2)

	// Now test unbalanced. 80/20
	subs1 = createSubs(80, c1c)
	defer closeSubs(subs1)
	subs2 = createSubs(20, c2c)
	defer closeSubs(subs2)

	sendRequests(num)
	checkFor(t, time.Second, 200*time.Millisecond, checkAllReceived)
	checkBalanced(num, 80, 20)

	// Now test draining the subs as we are sending from an initial balanced situation simulating a draining of a cluster.

	closeSubs(subs1)
	closeSubs(subs2)
	subs1, subs2 = nil, nil

	// These subs slightly different.
	var r1, r2 atomic.Uint64
	for i := 0; i < 20; i++ {
		nc := c1c[rand.IntN(len(c1c))]
		sub, err := nc.QueueSubscribe("REQUEST", "MC", func(m *nats.Msg) { r1.Add(1) })
		require_NoError(t, err)
		subs1 = append(subs1, sub)
		nc.Flush()

		nc = c2c[rand.IntN(len(c2c))]
		sub, err = nc.QueueSubscribe("REQUEST", "MC", func(m *nats.Msg) { r2.Add(1) })
		require_NoError(t, err)
		subs2 = append(subs2, sub)
		nc.Flush()
	}
	// Let's them propagate
	time.Sleep(100 * time.Millisecond)
	defer closeSubs(subs1)
	defer closeSubs(subs2)

	nc, _ := jsClientConnect(t, ln.randomServer())
	defer nc.Close()

	for i, dindex := 0, 1; i < num; i++ {
		require_NoError(t, nc.Publish("REQUEST", []byte("HELP")))
		// Check if we have more to simulate draining.
		// Will drain within first ~100 requests using 20% rand test below.
		// Will leave 1 behind.
		if dindex < len(subs1)-1 && rand.IntN(6) > 4 {
			sub := subs1[dindex]
			dindex++
			sub.Drain()
		}
	}
	nc.Flush()

	// Drain can lose some messages since it only checks pending messages known to the client,
	// and is not aware of other messages that are inflight on other servers.
	numMin := num * 99 / 100
	checkFor(t, time.Second, 200*time.Millisecond, func() error {
		total := int(r1.Load() + r2.Load())
		if total >= numMin {
			return nil
		}
		return fmt.Errorf("Not all received: %d vs %d (minimum %d)", total, num, numMin)
	})
	require_True(t, r2.Load() > r1.Load())

	// Now check opposite flow for responses.

	// Create 10 subscribers.
	var rsubs []*nats.Subscription

	for i := 0; i < 10; i++ {
		nc, _ := jsClientConnect(t, ln.randomServer())
		defer nc.Close()
		sub, err := nc.QueueSubscribeSync("RESPONSE", "SA")
		require_NoError(t, err)
		nc.Flush()
		rsubs = append(rsubs, sub)
	}
	// Let's them propagate
	time.Sleep(100 * time.Millisecond)

	nc, _ = jsClientConnect(t, ln.randomServer())
	defer nc.Close()
	_, err := nc.SubscribeSync("RESPONSE")
	require_NoError(t, err)
	nc.Flush()

	// Now connect and send responses from EFG in cloud.
	nc, _ = jsClientConnect(t, sc.randomServer(), nats.UserInfo("efg", "p"))
	defer nc.Close()

	for i := 0; i < 100; i++ {
		require_NoError(t, nc.Publish("RESPONSE", []byte("OK")))
	}
	nc.Flush()

	checkAllRespReceived := func() error {
		p := pending(rsubs)
		if p == 100 {
			return nil
		}
		return fmt.Errorf("Not all responses received: %d vs %d", p, 100)
	}

	checkFor(t, time.Second, 200*time.Millisecond, checkAllRespReceived)
}

func TestLeafNodeWithWeightedDQResponsesWithStreamImportAccountsWithUnsub(t *testing.T) {
	tmpl := `
	listen: 127.0.0.1:-1

	server_name: %s
	jetstream: {max_mem_store: 256MB, max_file_store: 2GB, store_dir: '%s'}

	leaf { listen: 127.0.0.1:-1 }

	cluster {
		name: %s
		listen: 127.0.0.1:%d
		routes = [%s]
	}

	accounts {
		EFG {
			users = [ { user: "efg", pass: "p" } ]
			jetstream: enabled
			exports [ { stream: "RESPONSE" } ]
		}
		STL {
			users = [ { user: "stl", pass: "p" } ]
			imports [ { stream: { account: EFG, subject: "RESPONSE"} } ]
		}
		KSC {
			users = [ { user: "ksc", pass: "p" } ]
			imports [ { stream: { account: EFG, subject: "RESPONSE"} } ]
		}
		$SYS { users = [ { user: "admin", pass: "s3cr3t!" } ] }
	}`

	c := createJetStreamClusterWithTemplate(t, tmpl, "US-CENTRAL", 3)
	defer c.shutdown()

	// Now create a leafnode cluster that has 2 LNs, one to each cluster but on separate accounts, STL and KSC.
	lnTmpl := `
		listen: 127.0.0.1:-1
		server_name: %s
		jetstream: {max_mem_store: 256MB, max_file_store: 2GB, store_dir: '%s'}

		{{leaf}}

		cluster {
			name: %s
			listen: 127.0.0.1:%d
			routes = [%s]
		}

		accounts { $SYS { users = [ { user: "admin", pass: "s3cr3t!" } ] }}
		`

	leafFrag := `
		leaf {
			listen: 127.0.0.1:-1
			remotes [ { urls: [ %s ] } ]
		}`

	genLeafTmpl := func(tmpl string) string {
		t.Helper()

		var ln []string
		for _, s := range c.servers {
			lno := s.getOpts().LeafNode
			ln = append(ln, fmt.Sprintf("nats://ksc:p@%s:%d", lno.Host, lno.Port))
		}
		return strings.Replace(tmpl, "{{leaf}}", fmt.Sprintf(leafFrag, strings.Join(ln, ", ")), 1)
	}

	tmpl = strings.Replace(lnTmpl, "store_dir:", fmt.Sprintf(`domain: "%s", store_dir:`, "SA"), 1)
	tmpl = genLeafTmpl(tmpl)

	ln := createJetStreamCluster(t, tmpl, "SA", "SA-", 3, 22280, false)
	ln.waitOnClusterReady()
	defer ln.shutdown()

	for _, s := range ln.servers {
		checkLeafNodeConnectedCount(t, s, 1)
	}

	// Create 10 subscribers.
	var rsubs []*nats.Subscription

	closeSubs := func(subs []*nats.Subscription) {
		for _, sub := range subs {
			sub.Unsubscribe()
		}
	}

	checkAllRespReceived := func() error {
		t.Helper()
		var total int
		for _, sub := range rsubs {
			n, _, err := sub.Pending()
			require_NoError(t, err)
			total += n
		}
		if total == 100 {
			return nil
		}
		return fmt.Errorf("Not all responses received: %d vs %d", total, 100)
	}

	s := ln.randomServer()
	for i := 0; i < 4; i++ {
		nc, _ := jsClientConnect(t, s)
		defer nc.Close()
		sub, err := nc.QueueSubscribeSync("RESPONSE", "SA")
		require_NoError(t, err)
		nc.Flush()
		rsubs = append(rsubs, sub)
	}

	// Now connect and send responses from EFG in cloud.
	nc, _ := jsClientConnect(t, c.randomServer(), nats.UserInfo("efg", "p"))
	defer nc.Close()
	for i := 0; i < 100; i++ {
		require_NoError(t, nc.Publish("RESPONSE", []byte("OK")))
	}
	nc.Flush()

	// Make sure all received.
	checkFor(t, time.Second, 200*time.Millisecond, checkAllRespReceived)

	checkAccountInterest := func(s *Server, accName string) *SublistResult {
		t.Helper()
		acc, err := s.LookupAccount(accName)
		require_NoError(t, err)
		acc.mu.RLock()
		r := acc.sl.Match("RESPONSE")
		acc.mu.RUnlock()
		return r
	}

	checkInterest := func() error {
		t.Helper()
		for _, s := range c.servers {
			if r := checkAccountInterest(s, "KSC"); len(r.psubs)+len(r.qsubs) > 0 {
				return fmt.Errorf("Subs still present for %q: %+v", "KSC", r)
			}
			if r := checkAccountInterest(s, "EFG"); len(r.psubs)+len(r.qsubs) > 0 {
				return fmt.Errorf("Subs still present for %q: %+v", "EFG", r)
			}
		}
		return nil
	}

	// Now unsub them and create new ones on a different server.
	closeSubs(rsubs)
	rsubs = rsubs[:0]

	// Also restart the server that we had all the rsubs on.
	s.Shutdown()
	s.WaitForShutdown()
	s = ln.restartServer(s)
	ln.waitOnClusterReady()
	ln.waitOnServerCurrent(s)

	checkFor(t, time.Second, 200*time.Millisecond, checkInterest)

	for i := 0; i < 4; i++ {
		nc, _ := jsClientConnect(t, s)
		defer nc.Close()
		sub, err := nc.QueueSubscribeSync("RESPONSE", "SA")
		require_NoError(t, err)
		nc.Flush()
		rsubs = append(rsubs, sub)
	}

	for i := 0; i < 100; i++ {
		require_NoError(t, nc.Publish("RESPONSE", []byte("OK")))
	}
	nc.Flush()

	// Make sure all received.
	checkFor(t, time.Second, 200*time.Millisecond, checkAllRespReceived)

	closeSubs(rsubs)
	checkFor(t, time.Second, 200*time.Millisecond, checkInterest)
}

func TestLeafNodeTwoRemotesToSameHubAccount(t *testing.T) {
	conf1 := createConfFile(t, []byte(`
		port: -1
		server_name: "hub"
		accounts {
			HA { users: [{user: ha, password: pwd}] }
		}
		leafnodes {
			port: -1
		}
	`))
	s1, o1 := RunServerWithConfig(conf1)
	defer s1.Shutdown()

	conf2 := createConfFile(t, []byte(fmt.Sprintf(`
		port: -1
		server_name: "spoke"
		accounts {
			A { users: [{user: A, password: pwd}] }
			B { users: [{user: B, password: pwd}] }
			C { users: [{user: C, password: pwd}] }
		}
		leafnodes {
			remotes [
				{
					url: "nats://ha:pwd@127.0.0.1:%d"
					local: "A"
				}
				{
					url: "nats://ha:pwd@127.0.0.1:%d"
					local: "C"
				}
			]
		}
	`, o1.LeafNode.Port, o1.LeafNode.Port)))
	s2, _ := RunServerWithConfig(conf2)
	defer s2.Shutdown()

	l := &captureErrorLogger{errCh: make(chan string, 10)}
	s2.SetLogger(l, false, false)

	checkLeafNodeConnectedCount(t, s2, 2)

	// Make sure we don't get duplicate leafnode connection errors
	deadline := time.NewTimer(1500 * time.Millisecond)
	for done := false; !done; {
		select {
		case err := <-l.errCh:
			if strings.Contains(err, DuplicateRemoteLeafnodeConnection.String()) {
				t.Fatalf("Got error: %v", err)
			}
		case <-deadline.C:
			done = true
		}
	}

	nca := natsConnect(t, s2.ClientURL(), nats.UserInfo("A", "pwd"))
	defer nca.Close()
	suba := natsSubSync(t, nca, "A")
	ncb := natsConnect(t, s2.ClientURL(), nats.UserInfo("B", "pwd"))
	defer ncb.Close()
	subb := natsSubSync(t, ncb, "B")
	ncc := natsConnect(t, s2.ClientURL(), nats.UserInfo("C", "pwd"))
	defer ncc.Close()
	subc := natsSubSync(t, ncc, "C")
	subs := map[string]*nats.Subscription{"A": suba, "B": subb, "C": subc}

	for _, subj := range []string{"A", "C"} {
		checkSubInterest(t, s1, "HA", subj, time.Second)
	}

	nc := natsConnect(t, s1.ClientURL(), nats.UserInfo("ha", "pwd"))
	defer nc.Close()

	for _, subj := range []string{"A", "B", "C"} {
		natsPub(t, nc, subj, []byte("hello"))
	}

	for _, subj := range []string{"A", "B", "C"} {
		var expected bool
		if subj != "B" {
			expected = true
		}
		sub := subs[subj]
		if expected {
			natsNexMsg(t, sub, time.Second)
		} else {
			if _, err := sub.NextMsg(50 * time.Millisecond); err != nats.ErrTimeout {
				t.Fatalf("Expected timeout error, got %v", err)
			}
		}
	}
}

func TestLeafNodeTwoRemotesToSameHubAccountWithClusters(t *testing.T) {
	hubTmpl := `
		port: -1
		server_name: "%s"
		accounts {
			HA { users: [{user: HA, password: pwd}] }
		}
		cluster {
			name: "hub"
			port: -1
			%s
		}
		leafnodes {
			port: -1
		}
	`
	confH1 := createConfFile(t, []byte(fmt.Sprintf(hubTmpl, "H1", _EMPTY_)))
	sh1, oh1 := RunServerWithConfig(confH1)
	defer sh1.Shutdown()

	confH2 := createConfFile(t, []byte(fmt.Sprintf(hubTmpl, "H2", fmt.Sprintf("routes: [\"nats://127.0.0.1:%d\"]", oh1.Cluster.Port))))
	sh2, oh2 := RunServerWithConfig(confH2)
	defer sh2.Shutdown()

	checkClusterFormed(t, sh1, sh2)

	spokeTmpl := `
		port: -1
		server_name: "%s"
		accounts {
			A { users: [{user: A, password: pwd}] }
			B { users: [{user: B, password: pwd}] }
		}
		cluster {
			name: "spoke"
			port: -1
			%s
		}
		leafnodes {
			remotes [
				{
					url: "nats://HA:pwd@127.0.0.1:%d"
					local: "A"
				}
				{
					url: "nats://HA:pwd@127.0.0.1:%d"
					local: "B"
				}
			]
		}
	`
	for _, test := range []struct {
		name        string
		sp2Leafport int
	}{
		{"connect to different hub servers", oh2.LeafNode.Port},
		{"connect to same hub server", oh1.LeafNode.Port},
	} {
		t.Run(test.name, func(t *testing.T) {
			confSP1 := createConfFile(t, []byte(fmt.Sprintf(spokeTmpl, "SP1", _EMPTY_, oh1.LeafNode.Port, oh1.LeafNode.Port)))
			sp1, osp1 := RunServerWithConfig(confSP1)
			defer sp1.Shutdown()

			confSP2 := createConfFile(t, []byte(fmt.Sprintf(spokeTmpl, "SP2",
				fmt.Sprintf("routes: [\"nats://127.0.0.1:%d\"]", osp1.Cluster.Port), test.sp2Leafport, test.sp2Leafport)))
			sp2, _ := RunServerWithConfig(confSP2)
			defer sp2.Shutdown()

			checkClusterFormed(t, sp1, sp2)
			checkLeafNodeConnectedCount(t, sp1, 2)
			checkLeafNodeConnectedCount(t, sp2, 2)

			var conns []*nats.Conn
			createConn := func(s *Server, user string) {
				t.Helper()
				nc := natsConnect(t, s.ClientURL(), nats.UserInfo(user, "pwd"))
				conns = append(conns, nc)
			}
			createConn(sh1, "HA")
			createConn(sh2, "HA")
			createConn(sp1, "A")
			createConn(sp2, "A")
			createConn(sp1, "B")
			createConn(sp2, "B")
			for _, nc := range conns {
				defer nc.Close()
			}

			check := func(subConn *nats.Conn, subj string, checkA, checkB bool) {
				t.Helper()
				sub := natsSubSync(t, subConn, subj)
				defer sub.Unsubscribe()

				checkSubInterest(t, sh1, "HA", subj, time.Second)
				checkSubInterest(t, sh2, "HA", subj, time.Second)
				if checkA {
					checkSubInterest(t, sp1, "A", subj, time.Second)
					checkSubInterest(t, sp2, "A", subj, time.Second)
				}
				if checkB {
					checkSubInterest(t, sp1, "B", subj, time.Second)
					checkSubInterest(t, sp2, "B", subj, time.Second)
				}

				for i, ncp := range conns {
					// Don't publish from account "A" connections if we are
					// dealing with account "B", and vice-versa.
					if !checkA && i >= 2 && i <= 3 {
						continue
					}
					if !checkB && i >= 4 {
						continue
					}
					natsPub(t, ncp, subj, []byte("hello"))
					natsNexMsg(t, sub, time.Second)
					// Make sure we don't get a duplicate
					if msg, err := sub.NextMsg(50 * time.Millisecond); err != nats.ErrTimeout {
						t.Fatalf("Unexpected message or error: msg=%v - err=%v", msg, err)
					}
				}
			}
			check(conns[0], "HA.1", true, true)
			check(conns[1], "HA.2", true, true)
			check(conns[2], "SPA.1", true, false)
			check(conns[3], "SPA.2", true, false)
			check(conns[4], "SPB.1", false, true)
			check(conns[5], "SPB.2", false, true)
		})
	}
}

func TestLeafNodeSameLocalAccountToMultipleHubs(t *testing.T) {
	hub1Conf := createConfFile(t, []byte(`
		port: -1
		server_name: hub1
		accounts {
			hub1 { users: [{user: hub1, password: pwd}] }
		}
		leafnodes {
			port: -1
		}
	`))
	sh1, oh1 := RunServerWithConfig(hub1Conf)
	defer sh1.Shutdown()

	hub2Conf := createConfFile(t, []byte(`
		port: -1
		server_name: hub2
		accounts {
			hub2 { users: [{user: hub2, password: pwd}] }
		}
		leafnodes {
			port: -1
		}
	`))
	sh2, oh2 := RunServerWithConfig(hub2Conf)
	defer sh2.Shutdown()

	lconf := createConfFile(t, []byte(fmt.Sprintf(`
		port: -1
		server_name: leaf
		accounts {
			A { users: [{user: A, password: pwd}] }
			B { users: [{user: B, password: pwd}] }
			C { users: [{user: C, password: pwd}] }
		}
		leafnodes {
			port: -1
			remotes [
				{
					url: nats://hub1:pwd@127.0.0.1:%[1]d
					local: "A"
				}
				{
					url: nats://hub1:pwd@127.0.0.1:%[1]d
					local: "C"
				}
				{
					url: nats://hub2:pwd@127.0.0.1:%[2]d
					local: "A"
				}
				{
					url: nats://hub2:pwd@127.0.0.1:%[2]d
					local: "B"
				}
			]
		}
	`, oh1.LeafNode.Port, oh2.LeafNode.Port)))
	s, _ := RunServerWithConfig(lconf)
	defer s.Shutdown()

	// The leafnode to hub1 should have 2 connections (A and C)
	// while the one to hub2 should have 2 connections (A and B)
	checkLeafNodeConnectedCount(t, sh1, 2)
	checkLeafNodeConnectedCount(t, sh2, 2)
	checkLeafNodeConnectedCount(t, s, 4)

	nca := natsConnect(t, s.ClientURL(), nats.UserInfo("A", "pwd"))
	defer nca.Close()
	suba := natsSubSync(t, nca, "A")
	ncb := natsConnect(t, s.ClientURL(), nats.UserInfo("B", "pwd"))
	defer ncb.Close()
	subb := natsSubSync(t, ncb, "B")
	ncc := natsConnect(t, s.ClientURL(), nats.UserInfo("C", "pwd"))
	defer ncc.Close()
	subc := natsSubSync(t, ncc, "C")

	checkSubInterest(t, sh1, "hub1", "A", time.Second)
	checkSubNoInterest(t, sh1, "hub1", "B", time.Second)
	checkSubInterest(t, sh1, "hub1", "C", time.Second)

	checkSubInterest(t, sh2, "hub2", "A", time.Second)
	checkSubInterest(t, sh2, "hub2", "B", time.Second)
	checkSubNoInterest(t, sh2, "hub2", "C", time.Second)

	nch1 := natsConnect(t, sh1.ClientURL(), nats.UserInfo("hub1", "pwd"))
	defer nch1.Close()
	nch2 := natsConnect(t, sh2.ClientURL(), nats.UserInfo("hub2", "pwd"))
	defer nch2.Close()

	checkNoMsg := func(sub *nats.Subscription) {
		t.Helper()
		if msg, err := sub.NextMsg(50 * time.Millisecond); err != nats.ErrTimeout {
			t.Fatalf("Unexpected message: %s", msg.Data)
		}
	}

	checkSub := func(sub *nats.Subscription, subj, payload string) {
		t.Helper()
		msg := natsNexMsg(t, sub, time.Second)
		require_Equal(t, subj, msg.Subject)
		require_Equal(t, payload, string(msg.Data))
		// Make sure we don't get duplicates
		checkNoMsg(sub)
	}

	natsPub(t, nch1, "A", []byte("msgA1"))
	checkSub(suba, "A", "msgA1")
	natsPub(t, nch1, "B", []byte("msgB1"))
	checkNoMsg(subb)
	natsPub(t, nch1, "C", []byte("msgC1"))
	checkSub(subc, "C", "msgC1")

	natsPub(t, nch2, "A", []byte("msgA2"))
	checkSub(suba, "A", "msgA2")
	natsPub(t, nch2, "B", []byte("msgB2"))
	checkSub(subb, "B", "msgB2")
	natsPub(t, nch2, "C", []byte("msgC2"))
	checkNoMsg(subc)
}

func TestLeafNodeSlowConsumer(t *testing.T) {
	ao := DefaultOptions()
	ao.LeafNode.Host = "127.0.0.1"
	ao.LeafNode.Port = -1
	ao.WriteDeadline = 1 * time.Millisecond
	a := RunServer(ao)
	defer a.Shutdown()

	c, err := net.Dial("tcp", fmt.Sprintf("127.0.0.1:%d", ao.LeafNode.Port))
	if err != nil {
		t.Fatalf("Error connecting: %v", err)
	}
	time.Sleep(5 * time.Millisecond)
	a.mu.Lock()
	checkFor(t, 2*time.Second, 15*time.Millisecond, func() error {
		a.grMu.Lock()
		defer a.grMu.Unlock()
		for _, cli := range a.grTmpClients {
			cli.out.wdl = time.Nanosecond
			return nil
		}
		return nil
	})
	a.mu.Unlock()

	// Only leafnode slow consumers that made it past connect are tracked
	// in the slow consumers counter.
	if _, err := c.Write([]byte("CONNECT {}\r\n")); err != nil {
		t.Fatalf("Error writing connect: %v", err)
	}
	// Read info
	br := bufio.NewReader(c)
	br.ReadLine()
	for i := 0; i < 10; i++ {
		// The server may already have closed us as a slow consumer.
		if _, err := c.Write([]byte("PING\r\n")); err != nil {
			break
		}
	}
	defer c.Close()
	timeout := time.Now().Add(time.Second)
	var (
		got      uint64
		expected uint64 = 1
	)
	for time.Now().Before(timeout) {
		got = a.NumSlowConsumersLeafs()
		if got == expected {
			return
		}
		time.Sleep(1 * time.Millisecond)
	}
	t.Fatalf("Timed out waiting for slow consumer leafnodes, got: %v, expected: %v", got, expected)
}

// https://github.com/nats-io/nats-server/issues/4367
func TestLeafNodeDQMultiAccountExportImport(t *testing.T) {
	bConf := createConfFile(t, []byte(`
		listen: 127.0.0.1:-1
		server_name: cluster-b-0
		accounts {
			$SYS: { users: [ { user: admin, password: pwd } ] },
			AGG: {
				exports: [ { service: "PING.>" } ]
				users: [ { user: agg, password: agg } ]
			}
		}
		leaf { listen: 127.0.0.1:-1 }
	`))

	sb, ob := RunServerWithConfig(bConf)
	defer sb.Shutdown()

	tmpl := `
		listen: 127.0.0.1:-1
		server_name: %s
		jetstream: { store_dir: '%s' }
		cluster {
			name: %s
			listen: 127.0.0.1:%d
			routes = [%s]
		}
		accounts {
			$SYS: { users: [ { user: admin, password: pwd } ] },
			A: {
				mappings: { "A.>" : ">" }
				exports: [ { service: A.> } ]
				users: [ { user: a, password: a } ]
			},
			AGG: {
				imports: [ { service: { subject: A.>, account: A } } ]
				users: [ { user: agg, password: agg } ]
			},
		}
		leaf {
			remotes: [ {
				urls: [ nats-leaf://agg:agg@127.0.0.1:{LEAF_PORT} ]
				account: AGG
			} ]
		}
	`
	tmpl = strings.Replace(tmpl, "{LEAF_PORT}", fmt.Sprintf("%d", ob.LeafNode.Port), 1)
	c := createJetStreamCluster(t, tmpl, "cluster-a", "cluster-a-", 3, 22110, false)
	defer c.shutdown()

	// Make sure all servers are connected via leafnode to the hub, the b server.
	for _, s := range c.servers {
		checkLeafNodeConnectedCount(t, s, 1)
	}

	// Connect to a server in the cluster and create a DQ listener.
	nc, _ := jsClientConnect(t, c.randomServer(), nats.UserInfo("a", "a"))
	defer nc.Close()

	var got atomic.Int32

	natsQueueSub(t, nc, "PING", "Q", func(m *nats.Msg) {
		got.Add(1)
		m.Respond([]byte("REPLY"))
	})

	// Now connect to B and send the request.
	ncb, _ := jsClientConnect(t, sb, nats.UserInfo("agg", "agg"))
	defer ncb.Close()

	_, err := ncb.Request("A.PING", []byte("REQUEST"), time.Second)
	require_NoError(t, err)
	require_Equal(t, got.Load(), 1)
}

// https://github.com/nats-io/nats-server/issues/4934
func TestLeafNodeServerReloadSubjectMappings(t *testing.T) {
	stmpl := `
		listen: 127.0.0.1:-1
		server_name: test-server
		mappings = { "source1": "target" }
		leaf { listen: 127.0.0.1:-1 }
	`
	conf := createConfFile(t, []byte(stmpl))
	s, o := RunServerWithConfig(conf)
	defer s.Shutdown()

	tmpl := `
		listen: 127.0.0.1:-1
		server_name: test-leaf
		leaf {
			remotes: [ {
				urls: [ nats-leaf://127.0.0.1:{LEAF_PORT} ]
			} ]
		}
	`
	tmpl = strings.Replace(tmpl, "{LEAF_PORT}", fmt.Sprintf("%d", o.LeafNode.Port), 1)
	lConf := createConfFile(t, []byte(tmpl))
	l, _ := RunServerWithConfig(lConf)
	defer l.Shutdown()

	checkLeafNodeConnected(t, l)

	// Create our subscriber.
	nc := natsConnect(t, s.ClientURL())
	defer nc.Close()
	sub := natsSubSync(t, nc, "target")
	natsFlush(t, nc)

	// Create our publisher.
	ncl := natsConnect(t, l.ClientURL())
	defer ncl.Close()
	// Publish our message.
	ncl.Publish("source1", []byte("OK"))

	// Make sure we receive it.
	checkSubsPending(t, sub, 1)

	// Now change mapping.
	reloadUpdateConfig(t, s, conf, strings.Replace(stmpl, "source1", "source2", 1))
	// Also make sure we do not have subscription interest for source1 on leaf anymore.
	checkSubInterest(t, l, globalAccountName, "source2", 2*time.Second)

	// Publish our new message.
	ncl.Publish("source2", []byte("OK"))

	// Make sure we receive it.
	checkSubsPending(t, sub, 2)

	// Also make sure we do not have subscription interest for source1 on leaf anymore.
	checkSubNoInterest(t, l, globalAccountName, "source1", 2*time.Second)
}

// https://github.com/nats-io/nats-server/issues/5099
func TestLeafNodeServerReloadSubjectMappingsWithSameSubject(t *testing.T) {
	stmpl := `
		listen: 127.0.0.1:-1
		server_name: test-server
		mappings = { "source": "target1" }
		leaf { listen: 127.0.0.1:-1 }
	`
	conf := createConfFile(t, []byte(stmpl))
	s, o := RunServerWithConfig(conf)
	defer s.Shutdown()

	tmpl := `
		listen: 127.0.0.1:-1
		server_name: test-leaf
		leaf {
			remotes: [ { urls: [ nats-leaf://127.0.0.1:{LEAF_PORT} ] } ]
		}
	`
	tmpl = strings.Replace(tmpl, "{LEAF_PORT}", fmt.Sprintf("%d", o.LeafNode.Port), 1)
	lConf := createConfFile(t, []byte(tmpl))
	l, _ := RunServerWithConfig(lConf)
	defer l.Shutdown()

	checkLeafNodeConnected(t, l)

	// Create our subscriber.
	nc := natsConnect(t, s.ClientURL())
	defer nc.Close()
	sub1 := natsSubSync(t, nc, "target1")
	sub2 := natsSubSync(t, nc, "target2")
	natsFlush(t, nc)

	// Create our publisher.
	ncl := natsConnect(t, l.ClientURL())
	defer ncl.Close()
	// Publish our message.
	ncl.Publish("source", []byte("OK"))

	// Make sure we receive it.
	checkSubsPending(t, sub1, 1)
	// Make sure the other does not.
	checkSubsPending(t, sub2, 0)

	// Now change mapping, but only the "to" subject, keeping same "from"
	reloadUpdateConfig(t, s, conf, strings.Replace(stmpl, "target1", "target2", 1))
	checkLeafNodeConnected(t, l)

	// Publish our new message.
	ncl.Publish("source", []byte("OK"))

	// Make sure we receive it.
	checkSubsPending(t, sub2, 1)
	// Make sure the other does not.
	checkSubsPending(t, sub1, 1)
}

func TestLeafNodeNkeyAuth(t *testing.T) {
	conf := createConfFile(t, []byte(`
		listen: 127.0.0.1:-1
		server_name: test-server
		leaf {
			listen: 127.0.0.1:-1
			authorization: { nkey: UCSTG5CRF5GEJERAFKUUYRODGABTBVWY2NPE4GGKRQVQOH74PIAKTVKO }
		}
	`))
	s, o := RunServerWithConfig(conf)
	defer s.Shutdown()

	tmpl := `
		listen: 127.0.0.1:-1
		server_name: test-leaf
		leaf {
			remotes: [ {
				url:  nats-leaf://127.0.0.1:{LEAF_PORT}
				seed: SUACJN3OSKWWPQXME4JUNFJ3PARXPO657GGNWNU7PK7G3AUQQYHLW26XH4
			} ]
		}
	`
	tmpl = strings.Replace(tmpl, "{LEAF_PORT}", fmt.Sprintf("%d", o.LeafNode.Port), 1)
	lConf := createConfFile(t, []byte(tmpl))
	l, _ := RunServerWithConfig(lConf)
	defer l.Shutdown()

	checkLeafNodeConnected(t, l)
}

func TestLeafNodeAccountNkeysAuth(t *testing.T) {
	conf := createConfFile(t, []byte(`
		listen: 127.0.0.1:-1
		server_name: test-server
		leaf {
			listen: 127.0.0.1:-1
		}
		accounts {
            A { users [ {nkey: UCSTG5CRF5GEJERAFKUUYRODGABTBVWY2NPE4GGKRQVQOH74PIAKTVKO } ] }
		}
	`))
	s, o := RunServerWithConfig(conf)
	defer s.Shutdown()

	tmpl := `
		listen: 127.0.0.1:-1
		server_name: test-leaf
		leaf {
			remotes: [ {
				url:  nats-leaf://127.0.0.1:{LEAF_PORT}
				seed: SUACJN3OSKWWPQXME4JUNFJ3PARXPO657GGNWNU7PK7G3AUQQYHLW26XH4
			} ]
		}
	`
	tmpl = strings.Replace(tmpl, "{LEAF_PORT}", fmt.Sprintf("%d", o.LeafNode.Port), 1)
	lConf := createConfFile(t, []byte(tmpl))
	l, _ := RunServerWithConfig(lConf)
	defer l.Shutdown()

	checkLeafNodeConnected(t, l)
}

// https://github.com/nats-io/nats-server/issues/5117
func TestLeafNodeLoopDetectionOnActualLoop(t *testing.T) {
	// Setup:  B --[leaf]--> A    C --[leaf]--> A    C --[leaf] --> B
	accConf := `
		accounts: {
			APP: {
				users: [ { user:u, password: u,
					permissions: { publish = "u.>", subscribe = "u.>" }} ]
			}
			$SYS: { users = [ {user: "s", password: "s"} ] }
		}`

	confA := createConfFile(t, []byte(fmt.Sprintf(`
		server_name: a1
		port: -1
		cluster: { name: A }
		leafnodes {
			port: 17422
		}
		%s`, accConf)))

	confB := createConfFile(t, []byte(fmt.Sprintf(`
		server_name: b1
		port: -1
		cluster: { name: B }
		leafnodes {
			port: 17432
			remotes [
				{ urls: ["nats-leaf://u:u@localhost:17422"], account: "APP" }
			]
			reconnect: "2s"
		}
		%s`, accConf)))

	confC := createConfFile(t, []byte(fmt.Sprintf(`
		server_name: c1
		port: -1
		cluster: { name: C }
		leafnodes {
			port: 17442
			remotes [
				{ urls: ["nats-leaf://u:u@localhost:17422"], account: "APP" }
				# This one creates the loop
				{ urls: ["nats-leaf://u:u@localhost:17432"], account: "APP" }
			]
			reconnect: "0.5s"
		}
		%s`, accConf)))

	// Start order will be B -> C -> A
	// We will force C to connect to A first before B using different reconnect intervals.
	// If B connects first we detect loops fine. If C connects first we do not.

	srvB, _ := RunServerWithConfig(confB)
	defer srvB.Shutdown()
	lb := &loopDetectedLogger{ch: make(chan string, 1)}
	srvB.SetLogger(lb, false, false)

	srvC, _ := RunServerWithConfig(confC)
	defer srvC.Shutdown()
	lc := &loopDetectedLogger{ch: make(chan string, 1)}
	srvC.SetLogger(lc, false, false)

	// C should connect to B
	checkLeafNodeConnectedCount(t, srvC, 1)

	srvA, _ := RunServerWithConfig(confA)
	defer srvA.Shutdown()
	la := &loopDetectedLogger{ch: make(chan string, 1)}
	srvA.SetLogger(la, false, false)

	select {
	case <-la.ch:
	case <-lb.ch:
	case <-lc.ch:
	case <-time.After(5 * time.Second):
		t.Fatalf("Did not get any error regarding loop")
	}
}

func TestLeafNodeConnectionSucceedsEvenWithDelayedFirstINFO(t *testing.T) {
	for _, test := range []struct {
		name      string
		websocket bool
	}{
		{"regular", false},
		{"websocket", true},
	} {
		t.Run(test.name, func(t *testing.T) {
			ob := DefaultOptions()
			ob.ServerName = "HUB"
			ob.LeafNode.Host = "127.0.0.1"
			ob.LeafNode.Port = -1
			ob.LeafNode.AuthTimeout = 10
			if test.websocket {
				ob.Websocket.Host = "127.0.0.1"
				ob.Websocket.Port = -1
				ob.Websocket.HandshakeTimeout = 10 * time.Second
				ob.Websocket.AuthTimeout = 10
				ob.Websocket.NoTLS = true
			}
			sb := RunServer(ob)
			defer sb.Shutdown()

			var port int
			var scheme string
			if test.websocket {
				port = ob.Websocket.Port
				scheme = wsSchemePrefix
			} else {
				port = ob.LeafNode.Port
				scheme = "nats"
			}

			urlStr := fmt.Sprintf("%s://127.0.0.1:%d", scheme, port)
			proxy := createNetProxy(1100*time.Millisecond, 1024*1024*1024, 1024*1024*1024, urlStr, true)
			defer proxy.stop()
			proxyURL := proxy.clientURL()
			_, proxyPort, err := net.SplitHostPort(proxyURL[len(scheme)+3:])
			require_NoError(t, err)

			lnBURL, err := url.Parse(fmt.Sprintf("%s://127.0.0.1:%s", scheme, proxyPort))
			require_NoError(t, err)

			oa := DefaultOptions()
			oa.ServerName = "SPOKE"
			oa.Cluster.Name = "xyz"
			remote := &RemoteLeafOpts{
				URLs:             []*url.URL{lnBURL},
				FirstInfoTimeout: 3 * time.Second,
			}
			oa.LeafNode.Remotes = []*RemoteLeafOpts{remote}
			sa := RunServer(oa)
			defer sa.Shutdown()

			checkLeafNodeConnected(t, sa)
		})
	}
}

type captureLeafConnClosed struct {
	DummyLogger
	ch chan struct{}
}

func (l *captureLeafConnClosed) Noticef(format string, v ...any) {
	msg := fmt.Sprintf(format, v...)
	if strings.Contains(msg, "Leafnode connection closed: Read Error") {
		select {
		case l.ch <- struct{}{}:
		default:
		}
	}
}

func TestLeafNodeDetectsStaleConnectionIfNoInfo(t *testing.T) {
	for _, test := range []struct {
		name      string
		websocket bool
	}{
		{"regular", false},
		{"websocket", true},
	} {
		t.Run(test.name, func(t *testing.T) {
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

			var scheme string
			if test.websocket {
				scheme = wsSchemePrefix
			} else {
				scheme = "nats"
			}
			urlStr := fmt.Sprintf("%s://%s", scheme, l.Addr())
			lnBURL, err := url.Parse(urlStr)
			require_NoError(t, err)

			oa := DefaultOptions()
			oa.ServerName = "SPOKE"
			oa.Cluster.Name = "xyz"
			remote := &RemoteLeafOpts{
				URLs:             []*url.URL{lnBURL},
				FirstInfoTimeout: 250 * time.Millisecond,
			}
			oa.LeafNode.Remotes = []*RemoteLeafOpts{remote}
			oa.DisableShortFirstPing = false
			oa.NoLog = false
			sa, err := NewServer(oa)
			require_NoError(t, err)
			defer sa.Shutdown()

			log := &captureLeafConnClosed{ch: make(chan struct{}, 1)}
			sa.SetLogger(log, false, false)
			sa.Start()

			select {
			case <-log.ch:
				// OK
			case <-time.After(750 * time.Millisecond):
				t.Fatalf("Connection was not closed")
			}

			sa.Shutdown()
			close(ch)
			wg.Wait()
			sa.WaitForShutdown()
		})
	}
}

// https://github.com/nats-io/nats-server/issues/5473
func TestLeafNodeDupeDeliveryQueueSubAndPlainSub(t *testing.T) {
	clusterCommonConf := `
		accounts: {
			tenant: {
				users: [ { user:t, password: t } ]
				exports: [{stream: system-a.events.>}]
			}
			system-a: {
				users: [ { user:sa, password: sa } ]
				imports: [
					{stream: {subject: system-a.events.>, account: tenant}, prefix: tenant}
				]
			}
			$SYS: { users = [ {user: "s", password: "s"} ] }
		}
		leafnodes {
			remotes: [{
				urls: [ "nats-leaf://sa:sa@127.0.0.1:17422" ]
				account: system-a
			}]
		}`

	confCluster0 := createConfFile(t, []byte(fmt.Sprintf(`
		server_name: a-0
		port: -1
		cluster: {
			name: cluster-a
			listen: 127.0.0.1:16122
			routes = [ nats://127.0.0.1:16123 ]
			pool_size: -1
		}
		%s`, clusterCommonConf)))

	confCluster1 := createConfFile(t, []byte(fmt.Sprintf(`
		server_name: a-1
		port: -1
		cluster: {
			name: cluster-a
			listen: 127.0.0.1:16123
			routes = [ nats://127.0.0.1:16122 ]
			pool_size: -1
		}
		%s`, clusterCommonConf)))

	serverB := createConfFile(t, []byte(`
		server_name: b
		port: -1
		leafnodes: { port: 17422 }
		accounts: {
			system-a: {
				users: [ { user: sa, password: sa } ]
				exports: [{stream: *.system-a.>}]
			}
			system-b: {
				users: [ { user: sb, password: sb } ]
				imports: [ {stream: {subject: *.system-a.>, account: system-a }}]
			}
			$SYS: { users = [ {user: "s", password: "s"} ] }
		}`))

	// Start server B
	srvB, _ := RunServerWithConfig(serverB)
	defer srvB.Shutdown()

	// Start the cluster servers.
	srvA0, _ := RunServerWithConfig(confCluster0)
	defer srvA0.Shutdown()
	// Make sure this is connected first before starting the second server in cluster A.
	checkLeafNodeConnectedCount(t, srvB, 1)
	// Start second A server.
	srvA1, _ := RunServerWithConfig(confCluster1)
	defer srvA1.Shutdown()
	// Make sure they are routed together.
	checkNumRoutes(t, srvA0, 1)
	checkNumRoutes(t, srvA1, 1)
	// Make sure each cluster server is connected to server B.
	checkLeafNodeConnectedCount(t, srvB, 2)

	// Create plain subscriber on server B attached to system-b account.
	ncB := natsConnect(t, srvB.ClientURL(), nats.UserInfo("sb", "sb"))
	defer ncB.Close()
	sub := natsSubSync(t, ncB, "*.system-a.events.>")
	subq := natsQueueSubSync(t, ncB, "*.system-a.events.objectnotfound", "SBQ")
	natsFlush(t, ncB)

	// Create a subscription on SA1 (we will send from SA0). We want to make sure that
	// when subscription on B is removed, this does not affect the subject interest
	// in SA0 on behalf of SA1.
	ncSAA1 := natsConnect(t, srvA1.ClientURL(), nats.UserInfo("sa", "sa"))
	defer ncSAA1.Close()
	sub2 := natsSubSync(t, ncSAA1, "*.system-a.events.>")
	subq2 := natsQueueSubSync(t, ncSAA1, "*.system-a.events.objectnotfound", "SBQ")
	natsFlush(t, ncSAA1)

	time.Sleep(250 * time.Millisecond)

	// Connect to cluster A on SA0.
	ncA := natsConnect(t, srvA0.ClientURL(), nats.UserInfo("t", "t"))
	defer ncA.Close()

	natsPub(t, ncA, "system-a.events.objectnotfound", []byte("EventA"))
	natsFlush(t, ncA)

	natsNexMsg(t, sub, time.Second)
	natsNexMsg(t, sub2, time.Second)
	if _, err := subq.NextMsg(250 * time.Millisecond); err != nil {
		natsNexMsg(t, subq2, time.Second)
	}

	// Unsubscribe the subscriptions from server B.
	natsUnsub(t, sub)
	natsUnsub(t, subq)
	natsFlush(t, ncB)

	// Wait for subject propagation.
	time.Sleep(250 * time.Millisecond)

	// Publish again, subscriptions on SA1 should receive it.
	natsPub(t, ncA, "system-a.events.objectnotfound", []byte("EventA"))
	natsFlush(t, ncA)

	natsNexMsg(t, sub2, time.Second)
	natsNexMsg(t, subq2, time.Second)
}

func TestLeafNodeServerKickClient(t *testing.T) {
	stmpl := `
		listen: 127.0.0.1:-1
		server_name: test-server
		leaf { listen: 127.0.0.1:-1 }
	`
	conf := createConfFile(t, []byte(stmpl))
	s, o := RunServerWithConfig(conf)
	defer s.Shutdown()

	tmpl := `
		listen: 127.0.0.1:-1
		server_name: test-leaf
		leaf { remotes: [ { urls: [ nats-leaf://127.0.0.1:{LEAF_PORT} ] } ] }
	`
	tmpl = strings.Replace(tmpl, "{LEAF_PORT}", fmt.Sprintf("%d", o.LeafNode.Port), 1)
	lConf := createConfFile(t, []byte(tmpl))
	l, _ := RunServerWithConfig(lConf)
	defer l.Shutdown()

	checkLeafNodeConnected(t, l)

	// We want to make sure we can kick the leafnode connections as well as client connections.
	conns, err := s.Connz(&ConnzOptions{Account: globalAccountName})
	require_NoError(t, err)
	require_Equal(t, len(conns.Conns), 1)
	lid := conns.Conns[0].Cid

	disconnectTime := time.Now()
	err = s.DisconnectClientByID(lid)
	require_NoError(t, err)

	// Wait until we are reconnected.
	checkLeafNodeConnected(t, s)

	// Look back up again and make sure start time indicates a restart, meaning kick worked.
	conns, err = s.Connz(&ConnzOptions{Account: globalAccountName})
	require_NoError(t, err)
	require_Equal(t, len(conns.Conns), 1)
	ln := conns.Conns[0]
	require_True(t, lid != ln.Cid)
	require_True(t, ln.Start.After(disconnectTime))
}

func TestLeafNodeBannerNoClusterNameIfNoCluster(t *testing.T) {
	u, err := url.Parse("nats://127.0.0.1:1234")
	require_NoError(t, err)

	opts := DefaultOptions()
	opts.ServerName = "LEAF_SERVER"
	opts.Cluster.Name = _EMPTY_
	opts.Cluster.Port = 0
	opts.LeafNode.Remotes = []*RemoteLeafOpts{{URLs: []*url.URL{u}}}
	opts.NoLog = false

	s, err := NewServer(opts)
	require_NoError(t, err)
	defer func() {
		s.Shutdown()
		s.WaitForShutdown()
	}()
	l := &captureNoticeLogger{}
	s.SetLogger(l, false, false)
	s.Start()

	if !s.ReadyForConnections(time.Second) {
		t.Fatal("Server not ready!")
	}

	l.Lock()
	for _, n := range l.notices {
		if strings.Contains(n, "Cluster: ") {
			l.Unlock()
			t.Fatalf("Cluster name should not be displayed, got %q", n)
		}
	}
	l.Unlock()
}

func TestLeafNodeCredFormatting(t *testing.T) {
	// create the operator/sys/account tree
	oKP, err := nkeys.CreateOperator()
	require_NoError(t, err)
	oPK, err := oKP.PublicKey()
	require_NoError(t, err)

	oc := jwt.NewOperatorClaims(oPK)
	oc.Name = "O"
	oJWT, err := oc.Encode(oKP)
	require_NoError(t, err)

	sysKP, err := nkeys.CreateAccount()
	require_NoError(t, err)
	sysPK, err := sysKP.PublicKey()
	require_NoError(t, err)

	sys := jwt.NewAccountClaims(sysPK)
	sys.Name = "SYS"
	sysJWT, err := sys.Encode(oKP)
	require_NoError(t, err)

	aKP, err := nkeys.CreateAccount()
	require_NoError(t, err)
	aPK, err := aKP.PublicKey()
	require_NoError(t, err)

	ac := jwt.NewAccountClaims(aPK)
	ac.Name = "A"
	aJWT, err := ac.Encode(oKP)
	require_NoError(t, err)

	uKP, err := nkeys.CreateUser()
	require_NoError(t, err)
	uSeed, err := uKP.Seed()
	require_NoError(t, err)
	uPK, err := uKP.PublicKey()
	require_NoError(t, err)

	// build the config
	stmpl := fmt.Sprintf(`
		listen: 127.0.0.1:-1
		operator: %s
		system_account: %s
		resolver: MEM
		resolver_preload: {
			%s: %s
			%s: %s
		}
		leaf { listen: 127.0.0.1:-1 }
	`, oJWT, sysPK, sysPK, sysJWT, aPK, aJWT)
	conf := createConfFile(t, []byte(stmpl))
	s, o := RunServerWithConfig(conf)
	defer s.Shutdown()

	// create the leaf node
	// generate the user credentials
	uc := jwt.NewUserClaims(uPK)
	uc.Name = "U"
	uc.Limits.Data = -1
	uc.Limits.Payload = -1
	uc.Permissions.Pub.Allow.Add(">")
	uc.Permissions.Sub.Allow.Add(">")
	uJWT, err := uc.Encode(aKP)
	require_NoError(t, err)

	runLeaf := func(t *testing.T, creds []byte) {
		file, err := os.CreateTemp("", "tmp-*.creds")
		require_NoError(t, err)
		_, err = file.Write(creds)
		require_NoError(t, err)
		require_NoError(t, file.Close())

		template := fmt.Sprintf(`
			listen: 127.0.0.1:-1
			leafnodes {
				remotes: [
					{
						urls: [ nats-leaf://127.0.0.1:%d ]
						credentials: "%s"
					}
				]
			}`, o.LeafNode.Port, file.Name())
		conf := createConfFile(t, []byte(template))
		leaf, _ := RunServerWithConfig(conf)
		defer leaf.Shutdown()
		defer os.Remove(file.Name())
		checkLeafNodeConnected(t, leaf)
	}

	creds, err := jwt.FormatUserConfig(uJWT, uSeed)
	require_NoError(t, err)

	runLeaf(t, creds)
	runLeaf(t, bytes.ReplaceAll(creds, []byte{'\n'}, []byte{'\r', '\n'}))
}

func TestLeafNodePermissionWithLiteralSubjectAndQueueInterest(t *testing.T) {
	hconf := createConfFile(t, []byte(`
		server_name: "HUB"
		listen: "127.0.0.1:-1"
		leafnodes {
			listen: "127.0.0.1:-1"
		}
		accounts {
			A {
				users: [
					{ user: "user", password: "pwd",
						permissions: {
							subscribe: { allow: ["_INBOX.>", "my.subject"] }
							publish: {allow: [">"]}
						}
					}
				]
			}
		}
	`))
	hub, ohub := RunServerWithConfig(hconf)
	defer hub.Shutdown()

	lconf := createConfFile(t, []byte(fmt.Sprintf(`
		server_name: "LEAF"
		listen: "127.0.0.1:-1"
		leafnodes {
			remotes: [
				{url: "nats://user:pwd@127.0.0.1:%d", account: A}
			]
		}
		accounts {
			A { users: [{user: user, password: pwd}] }
		}
	`, ohub.LeafNode.Port)))
	leaf, _ := RunServerWithConfig(lconf)
	defer leaf.Shutdown()

	checkLeafNodeConnected(t, hub)
	checkLeafNodeConnected(t, leaf)

	ncLeaf := natsConnect(t, leaf.ClientURL(), nats.UserInfo("user", "pwd"))
	defer ncLeaf.Close()
	natsQueueSub(t, ncLeaf, "my.subject", "queue", func(m *nats.Msg) {
		m.Respond([]byte("OK"))
	})
	natsFlush(t, ncLeaf)

	ncHub := natsConnect(t, hub.ClientURL(), nats.UserInfo("user", "pwd"))
	defer ncHub.Close()

	var resp *nats.Msg
	var err error
	checkFor(t, 5*time.Second, time.Second, func() error {
		// Make sure we don't fail the test on the first "no responders", might
		// take time for the sub to propagate.
		resp, err = ncHub.Request("my.subject", []byte("hello"), time.Second)
		return err
	})
	require_Equal(t, "OK", string(resp.Data))
}

func TestLeafNodeQueueScopedImportDenyFiltersBroadQueueInterest(t *testing.T) {
	hconf := createConfFile(t, []byte(`
		server_name: "HUB"
		listen: "127.0.0.1:-1"
		leafnodes {
			listen: "127.0.0.1:-1"
		}
		accounts {
			A {
				users: [
					{
						user: "leaf"
						password: "pwd"
						permissions: {
							subscribe: {
								allow: [">"]
								deny: [
									"broad.secret workers"
									"exact.secret workers"
								]
							}
							publish: { allow: [">"] }
						}
					}
					{ user: "hub", password: "pwd" }
				]
			}
		}
	`))
	hub, ohub := RunServerWithConfig(hconf)
	defer hub.Shutdown()

	lconf := createConfFile(t, []byte(fmt.Sprintf(`
		server_name: "LEAF"
		listen: "127.0.0.1:-1"
		leafnodes {
			remotes: [
				{ url: "nats://leaf:pwd@127.0.0.1:%d", account: A }
			]
		}
		accounts {
			A { users: [{ user: "client", password: "pwd" }] }
		}
	`, ohub.LeafNode.Port)))
	leaf, _ := RunServerWithConfig(lconf)
	defer leaf.Shutdown()

	checkLeafNodeConnected(t, hub)
	checkLeafNodeConnected(t, leaf)

	ncLeaf := natsConnect(t, leaf.ClientURL(), nats.UserInfo("client", "pwd"))
	defer ncLeaf.Close()
	broadWorkers := natsQueueSubSync(t, ncLeaf, "broad.*", "workers")
	broadAuditors := natsQueueSubSync(t, ncLeaf, "broad.*", "auditors")
	exactWorkers := natsQueueSubSync(t, ncLeaf, "exact.secret", "workers")
	exactAuditors := natsQueueSubSync(t, ncLeaf, "exact.secret", "auditors")
	natsFlush(t, ncLeaf)

	acc, err := hub.LookupAccount("A")
	require_NoError(t, err)
	checkFor(t, time.Second, 10*time.Millisecond, func() error {
		if !acc.sl.HasInterest("broad.public") || !acc.sl.HasInterest("exact.secret") {
			return fmt.Errorf("leaf queue interest has not propagated")
		}
		return nil
	})

	ncHub := natsConnect(t, hub.ClientURL(), nats.UserInfo("hub", "pwd"))
	defer ncHub.Close()
	natsPub(t, ncHub, "broad.secret", []byte("broad blocked"))
	natsPub(t, ncHub, "broad.public", []byte("broad ok"))
	natsPub(t, ncHub, "exact.secret", []byte("exact blocked"))
	natsFlush(t, ncHub)

	msg := natsNexMsg(t, broadWorkers, time.Second)
	require_Equal(t, "broad.public", msg.Subject)
	require_Equal(t, "broad ok", string(msg.Data))

	msg = natsNexMsg(t, broadAuditors, time.Second)
	require_Equal(t, "broad.secret", msg.Subject)
	require_Equal(t, "broad blocked", string(msg.Data))
	msg = natsNexMsg(t, broadAuditors, time.Second)
	require_Equal(t, "broad.public", msg.Subject)
	require_Equal(t, "broad ok", string(msg.Data))

	if _, err := exactWorkers.NextMsg(100 * time.Millisecond); err != nats.ErrTimeout {
		t.Fatalf("Expected exact workers queue subscription to be denied, got %v", err)
	}
	msg = natsNexMsg(t, exactAuditors, time.Second)
	require_Equal(t, "exact.secret", msg.Subject)
	require_Equal(t, "exact blocked", string(msg.Data))
}

// A remote's deny_imports only limits what the leaf receives from the hub, and
// deny_exports only what it sends. Replies to a hub requester must not be dropped
// when interest on either side overlaps an import deny.
func TestLeafNodeDenyImportsDoesNotDropRepliesToHub(t *testing.T) {
	// In each case a single kind of interest overlaps the deny. All but the first use the
	// old request style, so the hub's reply inbox is literal and doesn't overlap it.
	for _, test := range []struct {
		name            string
		oldRequestStyle bool
		hubQueue        bool
		leafWildcard    bool
	}{
		// The hub requester's response inbox (_INBOX.<id>.*) overlaps the deny.
		{"hub wildcard inbox", false, false, false},
		// A hub queue sub on a literal subject overlaps the deny.
		{"hub queue sub", true, true, false},
		// A leaf wildcard sub overlaps the deny.
		{"leaf wildcard sub", true, false, true},
	} {
		t.Run(test.name, func(t *testing.T) {
			hconf := createConfFile(t, []byte(`
				server_name: "HUB"
				listen: "127.0.0.1:-1"
				leafnodes {
					listen: "127.0.0.1:-1"
				}
			`))
			hub, ohub := RunServerWithConfig(hconf)
			defer hub.Shutdown()

			lconf := createConfFile(t, []byte(fmt.Sprintf(`
				server_name: "LEAF"
				listen: "127.0.0.1:-1"
				leafnodes {
					remotes: [
						{ url: "nats://127.0.0.1:%d", deny_imports: ["_INBOX.>"], deny_exports: ["out.secret"] }
					]
				}
			`, ohub.LeafNode.Port)))
			leaf, _ := RunServerWithConfig(lconf)
			defer leaf.Shutdown()

			checkLeafNodeConnected(t, hub)
			checkLeafNodeConnected(t, leaf)

			var opts []nats.Option
			if test.oldRequestStyle {
				opts = append(opts, nats.UseOldRequestStyle())
			}
			ncHub := natsConnect(t, hub.ClientURL(), opts...)
			defer ncHub.Close()
			out := natsSubSync(t, ncHub, "out.*")
			if test.hubQueue {
				natsQueueSubSync(t, ncHub, "_INBOX.queue", "q")
			}
			natsFlush(t, ncHub)
			checkSubInterest(t, leaf, globalAccountName, "out.public", time.Second)
			if test.hubQueue {
				checkSubInterest(t, leaf, globalAccountName, "_INBOX.queue", time.Second)
			}

			ncLeaf := natsConnect(t, leaf.ClientURL())
			defer ncLeaf.Close()
			natsSub(t, ncLeaf, "svc", func(m *nats.Msg) {
				m.Respond([]byte("OK"))
			})
			var wildcard *nats.Subscription
			if test.leafWildcard {
				wildcard = natsSubSync(t, ncLeaf, ">")
			}
			denied := natsSubSync(t, ncLeaf, "_INBOX.leaf")
			natsFlush(t, ncLeaf)
			checkSubInterest(t, hub, globalAccountName, "svc", time.Second)

			resp, err := ncHub.Request("svc", []byte("hello"), 2*time.Second)
			require_NoError(t, err)
			require_Equal(t, "OK", string(resp.Data))

			// The export deny still applies to what the leaf sends.
			natsPub(t, ncLeaf, "out.secret", nil)
			natsPub(t, ncLeaf, "out.public", nil)
			natsFlush(t, ncLeaf)
			require_Equal(t, "out.public", natsNexMsg(t, out, time.Second).Subject)

			// The import deny still keeps _INBOX traffic from the hub off the leaf.
			natsPub(t, ncHub, "_INBOX.leaf", nil)
			natsFlush(t, ncHub)
			if _, err := denied.NextMsg(100 * time.Millisecond); err != nats.ErrTimeout {
				t.Fatalf("Expected _INBOX.leaf to be denied by deny_imports, got %v", err)
			}
			// The wildcard sub also gets the leaf's own traffic, but not _INBOX.leaf.
			for wildcard != nil {
				msg, err := wildcard.NextMsg(100 * time.Millisecond)
				if err == nats.ErrTimeout {
					break
				}
				require_NoError(t, err)
				if msg.Subject == "_INBOX.leaf" {
					t.Fatalf("Expected _INBOX.leaf to be denied by deny_imports for the wildcard sub")
				}
			}
		})
	}
}

// A leafnode's subs hold the remote's interest. An account update on either side must
// not remove it based on the leafnode's subscribe perms.
func TestLeafNodeAccountUpdateKeepsRemoteInterest(t *testing.T) {
	sysKp, sysPub := createKey(t)
	sysJwt := encodeClaim(t, jwt.NewAccountClaims(sysPub), sysPub)
	sysCreds := newUser(t, sysKp)

	_, expPub := createKey(t)
	expClaim := jwt.NewAccountClaims(expPub)
	expClaim.Exports.Add(&jwt.Export{Subject: "feed", Type: jwt.Stream})
	expJwt := encodeClaim(t, expClaim, expPub)

	aKp, aPub := createKey(t)
	aClaim := jwt.NewAccountClaims(aPub)
	aJwt := encodeClaim(t, aClaim, aPub)
	aCreds := newUser(t, aKp)

	// The leaf user may receive anything, but may not publish "events".
	ukp, _ := nkeys.CreateUser()
	seed, _ := ukp.Seed()
	upub, _ := ukp.PublicKey()
	uclaim := newJWTTestUserClaims()
	uclaim.Subject = upub
	uclaim.Pub.Deny.Add("events")
	ujwt, err := uclaim.Encode(aKp)
	require_NoError(t, err)
	leafCreds := genCredsFile(t, ujwt, seed)

	tmpl := `
		server_name: %s
		listen: "127.0.0.1:-1"
		operator: %s
		system_account: %s
		resolver: MEMORY
		resolver_preload: {
			%s: %s
			%s: %s
			%s: %s
		}
		leafnodes {
			%s
		}
	`
	conf := func(name, leafnodes string) string {
		return createConfFile(t, []byte(fmt.Sprintf(tmpl, name, ojwt, sysPub,
			sysPub, sysJwt, expPub, expJwt, aPub, aJwt, leafnodes)))
	}
	hub, ohub := RunServerWithConfig(conf("HUB", `listen: "127.0.0.1:-1"`))
	defer hub.Shutdown()

	leaf, _ := RunServerWithConfig(conf("LEAF", fmt.Sprintf(`
		remotes: [
			{ url: "nats://127.0.0.1:%d", account: %s, credentials: '%s', deny_imports: ["_INBOX.>"] }
		]
	`, ohub.LeafNode.Port, aPub, leafCreds)))
	defer leaf.Shutdown()

	checkLeafNodeConnected(t, hub)
	checkLeafNodeConnected(t, leaf)

	ncLeaf := natsConnect(t, leaf.ClientURL(), nats.UserCredentials(aCreds))
	defer ncLeaf.Close()
	natsSub(t, ncLeaf, "svc", func(m *nats.Msg) {
		m.Respond([]byte("OK"))
	})
	sub := natsSubSync(t, ncLeaf, "events")
	natsFlush(t, ncLeaf)
	checkSubInterest(t, hub, aPub, "svc", time.Second)
	checkSubInterest(t, hub, aPub, "events", time.Second)

	ncHub := natsConnect(t, hub.ClientURL(), nats.UserCredentials(aCreds))
	defer ncHub.Close()

	check := func() {
		t.Helper()
		// The leaf holds the hub requester's _INBOX interest, overlapping deny_imports.
		resp, err := ncHub.Request("svc", []byte("hello"), 2*time.Second)
		require_NoError(t, err)
		require_Equal(t, "OK", string(resp.Data))

		// The hub holds the leaf's interest in "events", which the leaf user may not publish.
		natsPub(t, ncHub, "events", nil)
		natsFlush(t, ncHub)
		natsNexMsg(t, sub, time.Second)
	}
	check()

	// Adding a stream import reprocesses the subs of all connections in the account.
	aClaim.Imports.Add(&jwt.Import{Subject: "feed", Account: expPub, Type: jwt.Stream})
	aJwt = encodeClaim(t, aClaim, aPub)
	for _, s := range []*Server{hub, leaf} {
		ncSys := natsConnect(t, s.ClientURL(), nats.UserCredentials(sysCreds))
		msg, err := ncSys.Request(fmt.Sprintf(accUpdateEventSubjNew, aPub), []byte(aJwt), time.Second)
		ncSys.Close()
		require_NoError(t, err)
		require_Contains(t, string(msg.Data), "jwt updated")
	}
	check()
}

func TestLeafNodeAccountDefaultPermsUpdateKeepsLeafPerms(t *testing.T) {
	sysKp, sysPub := createKey(t)
	sysJwt := encodeClaim(t, jwt.NewAccountClaims(sysPub), sysPub)
	sysCreds := newUser(t, sysKp)

	// Users without perms of their own may only receive "feed" and send "up".
	aKp, aPub := createKey(t)
	aClaim := jwt.NewAccountClaims(aPub)
	aClaim.DefaultPermissions.Sub.Allow.Add("feed")
	aClaim.DefaultPermissions.Pub.Allow.Add("up")
	aJwt := encodeClaim(t, aClaim, aPub)

	userCreds := func(allowAll bool) string {
		ukp, _ := nkeys.CreateUser()
		seed, _ := ukp.Seed()
		upub, _ := ukp.PublicKey()
		uclaim := newJWTTestUserClaims()
		uclaim.Subject = upub
		if allowAll {
			uclaim.Pub.Allow.Add(">")
			uclaim.Sub.Allow.Add(">")
		}
		ujwt, err := uclaim.Encode(aKp)
		require_NoError(t, err)
		return genCredsFile(t, ujwt, seed)
	}
	leafCreds := userCreds(false)
	clientCreds := userCreds(true)

	tmpl := `
		server_name: %s
		listen: "127.0.0.1:-1"
		operator: %s
		system_account: %s
		resolver: MEMORY
		resolver_preload: {
			%s: %s
			%s: %s
		}
		leafnodes {
			%s
		}
	`
	conf := func(name, leafnodes string) string {
		return createConfFile(t, []byte(fmt.Sprintf(tmpl, name, ojwt, sysPub,
			sysPub, sysJwt, aPub, aJwt, leafnodes)))
	}
	hub, ohub := RunServerWithConfig(conf("HUB", `listen: "127.0.0.1:-1"`))
	defer hub.Shutdown()

	leaf, _ := RunServerWithConfig(conf("LEAF", fmt.Sprintf(`
		remotes: [
			{ url: "nats://127.0.0.1:%d", account: %s, credentials: '%s' }
		]
	`, ohub.LeafNode.Port, aPub, leafCreds)))
	defer leaf.Shutdown()

	checkLeafNodeConnected(t, hub)
	checkLeafNodeConnected(t, leaf)

	ncLeaf := natsConnect(t, leaf.ClientURL(), nats.UserCredentials(clientCreds))
	defer ncLeaf.Close()
	sub := natsSubSync(t, ncLeaf, "feed")
	natsFlush(t, ncLeaf)
	checkSubInterest(t, hub, aPub, "feed", time.Second)

	ncHub := natsConnect(t, hub.ClientURL(), nats.UserCredentials(clientCreds))
	defer ncHub.Close()
	natsPub(t, ncHub, "feed", nil)
	natsNexMsg(t, sub, time.Second)

	// Changing the default perms must not reset the leaf's reversed perms.
	aClaim.DefaultPermissions.Pub.Allow.Add("up2")
	aJwt = encodeClaim(t, aClaim, aPub)
	ncSys := natsConnect(t, hub.ClientURL(), nats.UserCredentials(sysCreds))
	defer ncSys.Close()
	msg, err := ncSys.Request(fmt.Sprintf(accUpdateEventSubjNew, aPub), []byte(aJwt), time.Second)
	require_NoError(t, err)
	require_Contains(t, string(msg.Data), "jwt updated")

	natsPub(t, ncHub, "feed", nil)
	natsNexMsg(t, sub, time.Second)
}

func TestLeafNodePermissionWithGateways(t *testing.T) {
	usConf := createConfFile(t, []byte(`
		server_name: "US"
		listen: "127.0.0.1:-1"
		gateway {
			name: "US"
			listen: "127.0.0.1:-1"
		}
		accounts {
			sys { users: [{user: sys, password: sys}] }
			leaf { users: [{user: leaf, password: leaf}] }
		}
		system_account: sys
	`))
	us, ous := RunServerWithConfig(usConf)
	defer us.Shutdown()

	euConf := createConfFile(t, fmt.Appendf(nil, `
		server_name: "EU"
		listen: "127.0.0.1:-1"
		gateway {
			name: "EU"
			listen: "127.0.0.1:-1"
			gateways: [
				{
					name: "US"
					urls: ["nats://127.0.0.1:%d"]
				}
			]
		}
		leafnodes {
			listen: "127.0.0.1:-1"
		}
		accounts {
			sys { users: [{user: sys, password: sys}] }
			leaf {
				users: [
					{
						user: leaf
						password: leaf
						permissions: {
							publish: "bar"
							subscribe: "foo"
						}
					}
				]
			}
		}
		system_account: sys
		`, ous.Gateway.Port))
	eu, oeu := RunServerWithConfig(euConf)
	defer eu.Shutdown()

	waitForOutboundGateways(t, us, 1, 2*time.Second)
	waitForOutboundGateways(t, eu, 1, 2*time.Second)
	waitForInboundGateways(t, us, 1, 2*time.Second)
	waitForInboundGateways(t, eu, 1, 2*time.Second)

	leafConf := createConfFile(t, fmt.Appendf(nil, `
		server_name: "LEAF"
		listen: "127.0.0.1:-1"
		leafnodes {
			remotes: [
				{ url: "nats://leaf:leaf@127.0.0.1:%d" }
			]
		}
	`, oeu.LeafNode.Port))
	leaf, _ := RunServerWithConfig(leafConf)
	defer leaf.Shutdown()

	checkLeafNodeConnected(t, leaf)

	// Run the service from EU leafnode.
	ncEU := natsConnect(t, leaf.ClientURL())
	defer ncEU.Close()
	natsSub(t, ncEU, "foo", func(m *nats.Msg) {
		m.Respond([]byte("response"))
	})
	natsFlush(t, ncEU)

	// Wait for subject interest to propagate.
	checkGWInterestOnlyModeInterestOn(t, us, "EU", "leaf", "foo")

	// Connect to the US server
	ncUS := natsConnect(t, us.ClientURL(), nats.UserInfo("leaf", "leaf"))
	defer ncUS.Close()

	// Create a subscription on "bar" and send request on "foo"
	sub := natsSubSync(t, ncUS, "bar")
	// Wait for subject to propagate so we know that EU server
	// would know about the "bar" subscription.
	checkGWInterestOnlyModeInterestOn(t, eu, "US", "leaf", "bar")
	// Send the request and make sure we receive the reply.
	natsPubReq(t, ncUS, "foo", "bar", []byte("request"))
	reply := natsNexMsg(t, sub, time.Second)
	if string(reply.Data) != "response" {
		t.Fatalf("Invalid response: %q", reply.Data)
	}
	// Make sure that we don't blindly accept any reply because there
	// is the routing protocol. So create a sub on "baz" that the leaf
	// would not be allowed to reply to. We should not get the reply
	// to our request.
	sub2 := natsSubSync(t, ncUS, "baz")
	checkGWInterestOnlyModeInterestOn(t, eu, "US", "leaf", "baz")
	// Send the request. We should not get the message since the
	// leaf server does not have permission to publish on "baz".
	natsPubReq(t, ncUS, "foo", "baz", []byte("request2"))
	if msg, err := sub2.NextMsg(250 * time.Millisecond); err == nil {
		t.Fatalf("Should not have received the reply, got %q", msg.Data)
	}
}

// Test that the inbound LMSG path checks for subject permissions.
// Normally, a leaf node will check for ACLs before sending LMSG messages.
// If it doesn't we still need to perform ACL checks on the hub side.
func TestLeafNodeLMSGHonorsPublishPermissions(t *testing.T) {
	hubConf := createConfFile(t, []byte(`
		server_name: "HUB"
		listen: "127.0.0.1:-1"
		accounts {
			A {
				users = [
					{ user: "a", password: "a" }
					{
						user: "leaf"
						password: "pwd"
						permissions: {
							publish: { allow: ["pub.ok"] }
							subscribe: { allow: [">"] }
						}
					}
				]
			}
		}
		leafnodes {
			listen: "127.0.0.1:-1"
			compression: off
		}
	`))
	hub, ohub := RunServerWithConfig(hubConf)
	defer hub.Shutdown()

	hlog := &captureErrorLogger{errCh: make(chan string, 4)}
	hub.SetLogger(hlog, false, false)

	leafConn, err := net.DialTimeout("tcp", net.JoinHostPort(ohub.LeafNode.Host, fmt.Sprintf("%d", ohub.LeafNode.Port)), 2*time.Second)
	require_NoError(t, err)
	defer leafConn.Close()

	br := bufio.NewReader(leafConn)
	_, err = br.ReadString('\n')
	require_NoError(t, err)

	connectOp := fmt.Appendf(nil, "CONNECT {\"name\":\"leaf\",\"user\":%q,\"pass\":%q}\r\n", "leaf", "pwd")
	_, err = leafConn.Write(connectOp)
	require_NoError(t, err)
	_, err = leafConn.Write([]byte("PING\r\n"))
	require_NoError(t, err)

	checkLeafNodeConnected(t, hub)

	ncHub := natsConnect(t, hub.ClientURL(), nats.UserInfo("a", "a"))
	defer ncHub.Close()

	subOK := natsSubSync(t, ncHub, "pub.ok")
	subBlocked := natsSubSync(t, ncHub, "pub.blocked")
	natsFlush(t, ncHub)

	_, err = fmt.Fprintf(leafConn, "LMSG %s %d\r\n%s\r\n", "pub.ok", len("ok"), "ok")
	require_NoError(t, err)
	msg := natsNexMsg(t, subOK, time.Second)
	require_Equal(t, "ok", string(msg.Data))

	_, err = fmt.Fprintf(leafConn, "LMSG %s %d\r\n%s\r\n", "pub.blocked", len("blocked"), "blocked")
	require_NoError(t, err)
	if msg, err := subBlocked.NextMsg(250 * time.Millisecond); err == nil {
		t.Fatalf("Should not have received the blocked publish, got %q", msg.Data)
	}

	select {
	case errMsg := <-hlog.errCh:
		if !strings.Contains(errMsg, `Publish Violation on "pub.blocked"`) {
			t.Fatalf("Expected publish violation log, got %q", errMsg)
		}
	case <-time.After(time.Second):
		t.Fatal("Did not get expected publish violation log")
	}
}

// Test that the hub side inbound leaf check still honors tracked reply
// permissions such as allow_responses after the hub has tracked a reply
// subject for a request sent to the leaf.
func TestLeafNodeHubSideLMSGHonorsAllowResponses(t *testing.T) {
	hubConf := createConfFile(t, []byte(`
		server_name: "HUB"
		listen: "127.0.0.1:-1"
		accounts {
			A {
				users = [
					{ user: "a", password: "a" }
					{
						user: "leaf"
						password: "pwd"
						permissions: {
							publish: { allow: ["svc.req"] }
							subscribe: { allow: ["svc.req"] }
							allow_responses: true
						}
					}
				]
			}
		}
		leafnodes {
			listen: "127.0.0.1:-1"
			compression: off
		}
	`))
	hub, ohub := RunServerWithConfig(hubConf)
	defer hub.Shutdown()

	leafConf := createConfFile(t, fmt.Appendf(nil, `
		server_name: "LEAF"
		listen: "127.0.0.1:-1"
		accounts {
			A {
				users = [ { user: "a", password: "a" } ]
			}
		}
		leafnodes {
			compression: off
			remotes = [
				{
					url: "nats://leaf:pwd@127.0.0.1:%d"
					account: A
					compression: off
				}
			]
		}
	`, ohub.LeafNode.Port))
	leaf, _ := RunServerWithConfig(leafConf)
	defer leaf.Shutdown()

	checkLeafNodeConnected(t, hub)
	checkLeafNodeConnected(t, leaf)

	ncLeaf := natsConnect(t, leaf.ClientURL(), nats.UserInfo("a", "a"))
	defer ncLeaf.Close()
	reqCh := make(chan string, 1)
	natsSub(t, ncLeaf, "svc.req", func(m *nats.Msg) {
		select {
		case reqCh <- m.Reply:
		default:
		}
	})
	natsFlush(t, ncLeaf)

	ncHub := natsConnect(t, hub.ClientURL(), nats.UserInfo("a", "a"))
	defer ncHub.Close()

	reply := ncHub.NewRespInbox()
	sub := natsSubSync(t, ncHub, reply)
	natsFlush(t, ncHub)

	natsPubReq(t, ncHub, "svc.req", reply, []byte("request"))

	var trackedReply string
	select {
	case trackedReply = <-reqCh:
	case <-time.After(time.Second):
		t.Fatal("Did not receive request on leaf side")
	}
	require_Equal(t, reply, trackedReply)

	var leafConn *client
	leaf.mu.Lock()
	for _, leafConn = range leaf.leafs {
		break
	}
	leaf.mu.Unlock()
	if leafConn == nil {
		t.Fatal("Expected a leaf connection on leaf side")
	}

	_, err := fmt.Fprintf(leafConn.nc, "LMSG %s %d\r\n%s\r\n", reply, len("ok"), "ok")
	require_NoError(t, err)

	resp := natsNexMsg(t, sub, time.Second)
	require_Equal(t, "ok", string(resp.Data))
}

// Test that allow_responses works when the tracked reply subject has a
// gateway routing prefix ($GNR...). deliverMsg tracks the reply under
// the GW-routed form, so processInboundLeafMsg must also check
// responseAllowed with the original (non-stripped) subject.
func TestLeafNodeHubSideLMSGHonorsAllowResponsesGatewayRouted(t *testing.T) {
	hubConf := createConfFile(t, []byte(`
		server_name: "HUB"
		listen: "127.0.0.1:-1"
		accounts {
			A {
				users = [
					{ user: "a", password: "a" }
					{
						user: "leaf"
						password: "pwd"
						permissions: {
							publish: { allow: ["svc.req"] }
							subscribe: { allow: ["svc.req"] }
							allow_responses: true
						}
					}
				]
			}
		}
		leafnodes {
			listen: "127.0.0.1:-1"
			compression: off
		}
	`))
	hub, ohub := RunServerWithConfig(hubConf)
	defer hub.Shutdown()

	leafConn, err := net.DialTimeout("tcp",
		net.JoinHostPort(ohub.LeafNode.Host, fmt.Sprintf("%d", ohub.LeafNode.Port)),
		2*time.Second)
	require_NoError(t, err)
	defer leafConn.Close()

	br := bufio.NewReader(leafConn)
	_, err = br.ReadString('\n')
	require_NoError(t, err)

	connectOp := fmt.Appendf(nil,
		"CONNECT {\"name\":\"leaf\",\"user\":%q,\"pass\":%q}\r\n", "leaf", "pwd")
	_, err = leafConn.Write(connectOp)
	require_NoError(t, err)
	_, err = leafConn.Write([]byte("PING\r\n"))
	require_NoError(t, err)

	checkLeafNodeConnected(t, hub)

	// Simulate the tracking that deliverMsg would perform when
	// delivering a gateway-routed request to the leaf: it stores
	// the reply under the full GW-routed form ($GNR...).
	gwRoutedReply := gwReplyPrefix + "AAAAAAA1.BBBBBBB2._INBOX.test"

	var hubLeaf *client
	hub.mu.Lock()
	for _, hubLeaf = range hub.leafs {
		break
	}
	hub.mu.Unlock()
	require_NotNil(t, hubLeaf)

	hubLeaf.mu.Lock()
	hubLeaf.replies[gwRoutedReply] = &resp{t: time.Now()}
	hubLeaf.mu.Unlock()

	// Subscribe on the hub so we can verify delivery.
	ncHub := natsConnect(t, hub.ClientURL(), nats.UserInfo("a", "a"))
	defer ncHub.Close()
	sub := natsSubSync(t, ncHub, gwRoutedReply)
	natsFlush(t, ncHub)

	// The leaf sends LMSG with the GW-routed reply as subject.
	// processInboundLeafMsg strips the $GNR prefix before the
	// permission check but must still find the tracked reply.
	_, err = fmt.Fprintf(leafConn, "LMSG %s %d\r\n%s\r\n",
		gwRoutedReply, len("ok"), "ok")
	require_NoError(t, err)

	resp := natsNexMsg(t, sub, time.Second)
	require_Equal(t, "ok", string(resp.Data))
}

// A leaf user with publish: { allow: [] } and allow_responses should
// deny arbitrary publishes but still permit tracked reply responses.
func TestLeafNodeHubSideLMSGEmptyAllowWithAllowResponses(t *testing.T) {
	hubConf := createConfFile(t, []byte(`
		server_name: "HUB"
		listen: "127.0.0.1:-1"
		accounts {
			A {
				users = [
					{ user: "a", password: "a" }
					{
						user: "leaf"
						password: "pwd"
						permissions: {
							publish: { allow: [] }
							subscribe: { allow: [">"] }
							allow_responses: true
						}
					}
				]
			}
		}
		leafnodes {
			listen: "127.0.0.1:-1"
			compression: off
		}
	`))
	hub, ohub := RunServerWithConfig(hubConf)
	defer hub.Shutdown()

	leafConn, err := net.DialTimeout("tcp",
		net.JoinHostPort(ohub.LeafNode.Host, fmt.Sprintf("%d", ohub.LeafNode.Port)),
		2*time.Second)
	require_NoError(t, err)
	defer leafConn.Close()

	br := bufio.NewReader(leafConn)
	_, err = br.ReadString('\n')
	require_NoError(t, err)

	connectOp := fmt.Appendf(nil,
		"CONNECT {\"name\":\"leaf\",\"user\":%q,\"pass\":%q}\r\n", "leaf", "pwd")
	_, err = leafConn.Write(connectOp)
	require_NoError(t, err)
	_, err = leafConn.Write([]byte("PING\r\n"))
	require_NoError(t, err)

	checkLeafNodeConnected(t, hub)

	// Simulate a tracked reply: inject a reply entry on the
	// hub's leaf connection, then verify the leaf can respond on it.
	reply := "_INBOX.test123"

	var hubLeaf *client
	hub.mu.Lock()
	for _, hubLeaf = range hub.leafs {
		break
	}
	hub.mu.Unlock()
	require_NotNil(t, hubLeaf)

	hubLeaf.mu.Lock()
	hubLeaf.replies[reply] = &resp{t: time.Now()}
	hubLeaf.mu.Unlock()

	ncHub := natsConnect(t, hub.ClientURL(), nats.UserInfo("a", "a"))
	defer ncHub.Close()

	subReply := natsSubSync(t, ncHub, reply)
	natsFlush(t, ncHub)

	_, err = fmt.Fprintf(leafConn, "LMSG %s %d\r\n%s\r\n",
		reply, len("ok"), "ok")
	require_NoError(t, err)

	msg := natsNexMsg(t, subReply, time.Second)
	require_Equal(t, "ok", string(msg.Data))

	// Arbitrary publish should be denied (and closes the connection).
	subBlocked := natsSubSync(t, ncHub, "not.allowed")
	natsFlush(t, ncHub)

	_, err = fmt.Fprintf(leafConn, "LMSG %s %d\r\n%s\r\n",
		"not.allowed", len("blocked"), "blocked")
	require_NoError(t, err)
	if msg, err := subBlocked.NextMsg(250 * time.Millisecond); err == nil {
		t.Fatalf("Should not have received arbitrary publish, got %q", msg.Data)
	}
}

// Test that the hub side allows JetStream API traffic.
// This guards the regression where the hub rejected $JS.API.*
func TestLeafNodeHubSideLMSGAllowsJSAPI(t *testing.T) {
	hubConf := createConfFile(t, []byte(fmt.Sprintf(`
		server_name: "HUB"
		listen: "127.0.0.1:-1"
		jetstream: { store_dir: %q }
		accounts {
			A {
				jetstream: enabled
				users = [
					{ user: "a", password: "a" }
					{ user: "leaf", password: "pwd" }
				]
			}
		}
		leafnodes {
			listen: "127.0.0.1:-1"
			compression: off
		}
	`, t.TempDir())))
	hub, ohub := RunServerWithConfig(hubConf)
	defer hub.Shutdown()

	leafConf := createConfFile(t, fmt.Appendf(nil, `
		server_name: "LEAF"
		listen: "127.0.0.1:-1"
		default_js_domain: {A:""}
		accounts {
			A {
				users = [ { user: "a", password: "a" } ]
			}
		}
		leafnodes {
			compression: off
			remotes = [
				{
					url: "nats://leaf:pwd@127.0.0.1:%d"
					account: A
					compression: off
				}
			]
		}
	`, ohub.LeafNode.Port))
	leaf, _ := RunServerWithConfig(leafConf)
	defer leaf.Shutdown()

	checkLeafNodeConnected(t, hub)
	checkLeafNodeConnected(t, leaf)

	var l *client
	hub.mu.Lock()
	for _, l = range hub.leafs {
		break
	}
	hub.mu.Unlock()
	if l == nil {
		t.Fatal("Expected a leaf connection")
	}

	l.mu.Lock()
	r := l.perms.sub.deny.Match(jsAllAPI)
	hasSyntheticJSDeny := len(r.psubs)+len(r.qsubs) > 0
	l.mu.Unlock()
	if !hasSyntheticJSDeny {
		t.Fatal("Expected synthetic JetStream deny on accepted leaf connection")
	}

	checkSubInterest(t, leaf, "A", JSApiAccountInfo, time.Second)
	ncLeaf := natsConnect(t, leaf.ClientURL(), nats.UserInfo("a", "a"))
	defer ncLeaf.Close()

	msg, err := ncLeaf.Request(JSApiAccountInfo, nil, time.Second)
	require_NoError(t, err)
	if !bytes.Contains(msg.Data, []byte(JSApiAccountInfoResponseType)) {
		t.Fatalf("Expected JetStream account info response, got %q", msg.Data)
	}
}

// When the hub has a subject mapping "pub.ok" -> "internal.pub.ok",
// a leaf allowed to publish "pub.ok" must not be rejected just because
// "internal.pub.ok" is not in the allow list. The permission check
// should evaluate the subject the leaf actually sent, not where the
// hub remaps it locally.
func TestLeafNodeLMSGPermissionsUseWireSubjectNotMapped(t *testing.T) {
	hubConf := createConfFile(t, []byte(`
		server_name: "HUB"
		listen: "127.0.0.1:-1"
		accounts {
			A {
				users = [
					{ user: "a", password: "a" }
					{
						user: "leaf"
						password: "pwd"
						permissions: {
							publish: { allow: ["pub.ok"] }
							subscribe: { allow: [">"] }
						}
					}
				]
				mappings = { "pub.ok": "internal.pub.ok" }
			}
		}
		leafnodes {
			listen: "127.0.0.1:-1"
			compression: off
		}
	`))
	hub, ohub := RunServerWithConfig(hubConf)
	defer hub.Shutdown()

	leafConf := createConfFile(t, fmt.Appendf(nil, `
		server_name: "LEAF"
		listen: "127.0.0.1:-1"
		accounts {
			A {
				users = [ { user: "a", password: "a" } ]
			}
		}
		leafnodes {
			compression: off
			remotes = [
				{
					url: "nats://leaf:pwd@127.0.0.1:%d"
					account: A
					compression: off
				}
			]
		}
	`, ohub.LeafNode.Port))
	leaf, _ := RunServerWithConfig(leafConf)
	defer leaf.Shutdown()

	checkLeafNodeConnected(t, hub)
	checkLeafNodeConnected(t, leaf)

	ncHub := natsConnect(t, hub.ClientURL(), nats.UserInfo("a", "a"))
	defer ncHub.Close()

	// Subscribe to the mapped destination.
	sub := natsSubSync(t, ncHub, "internal.pub.ok")
	natsFlush(t, ncHub)

	ncLeaf := natsConnect(t, leaf.ClientURL(), nats.UserInfo("a", "a"))
	defer ncLeaf.Close()

	// The leaf publishes on the wire subject "pub.ok" which is in
	// the export allow list. The hub maps it to "internal.pub.ok"
	// for local delivery. The ACL check must use the wire subject.
	natsPub(t, ncLeaf, "pub.ok", []byte("hello"))
	natsFlush(t, ncLeaf)

	msg := natsNexMsg(t, sub, time.Second)
	require_Equal(t, "internal.pub.ok", msg.Subject)
	require_Equal(t, "hello", string(msg.Data))
}

// Test that a solicited leaf configured with hub: true still uses the
// hub-side export ACLs on inbound LMSG.
func TestLeafNodeSolicitedHubLMSGHonorsExportPermissions(t *testing.T) {
	spokeConf := createConfFile(t, []byte(`
		server_name: "SPOKE"
		listen: "127.0.0.1:-1"
		accounts {
			A {
				users = [
					{ user: "a", password: "a" }
					{ user: "leaf", password: "pwd" }
				]
			}
		}
		leafnodes {
			listen: "127.0.0.1:-1"
			compression: off
		}
	`))
	spoke, ospoke := RunServerWithConfig(spokeConf)
	defer spoke.Shutdown()

	hubConf := createConfFile(t, fmt.Appendf(nil, `
		server_name: "HUB"
		listen: "127.0.0.1:-1"
		accounts {
			A {
				users = [ { user: "a", password: "a" } ]
			}
		}
		leafnodes {
			compression: off
			remotes = [
				{
					url: "nats://leaf:pwd@127.0.0.1:%d"
					account: A
					compression: off
					hub: true
					deny_exports: ["pub.blocked"]
				}
			]
		}
	`, ospoke.LeafNode.Port))
	hub, _ := RunServerWithConfig(hubConf)
	defer hub.Shutdown()

	hlog := &captureErrorLogger{errCh: make(chan string, 4)}
	hub.SetLogger(hlog, false, false)

	checkLeafNodeConnected(t, hub)
	checkLeafNodeConnected(t, spoke)

	var hubLeaf *client
	hub.mu.Lock()
	for _, hubLeaf = range hub.leafs {
		break
	}
	hub.mu.Unlock()
	if hubLeaf == nil {
		t.Fatal("Expected a leaf connection on hub")
	}
	hubLeaf.mu.Lock()
	require_True(t, hubLeaf.isSolicitedLeafNode())
	require_True(t, hubLeaf.isHubLeafNode())
	hubLeaf.mu.Unlock()

	ncHub := natsConnect(t, hub.ClientURL(), nats.UserInfo("a", "a"))
	defer ncHub.Close()

	sub := natsSubSync(t, ncHub, "pub.blocked")
	natsFlush(t, ncHub)

	var spokeLeaf *client
	spoke.mu.Lock()
	for _, spokeLeaf = range spoke.leafs {
		break
	}
	spoke.mu.Unlock()
	if spokeLeaf == nil {
		t.Fatal("Expected a leaf connection on spoke")
	}

	_, err := fmt.Fprintf(spokeLeaf.nc, "LMSG %s %d\r\n%s\r\n", "pub.blocked", len("blocked"), "blocked")
	require_NoError(t, err)

	if msg, err := sub.NextMsg(250 * time.Millisecond); err == nil {
		t.Fatalf("Should not have received the blocked publish, got %q", msg.Data)
	}

	select {
	case errMsg := <-hlog.errCh:
		if !strings.Contains(errMsg, `Publish Violation on "pub.blocked"`) {
			t.Fatalf("Expected publish violation log, got %q", errMsg)
		}
	case <-time.After(time.Second):
		t.Fatal("Did not get expected publish violation log")
	}
}

// Test that when a solicited remote is configured with hub: true, direct LMSG
// sent from that hub still respects the local leaf permissions on the accepting
// spoke server.
func TestLeafNodeSpokeSideLMSGHonorsLocalPermissions(t *testing.T) {
	spokeConf := createConfFile(t, []byte(`
		server_name: "SPOKE"
		listen: "127.0.0.1:-1"
		accounts {
			A {
				users = [
					{ user: "a", password: "a" }
					{
						user: "leaf"
						password: "pwd"
						permissions: {
							publish: { allow: ["pub.ok"] }
							subscribe: { allow: [">"] }
						}
					}
				]
			}
		}
		leafnodes {
			listen: "127.0.0.1:-1"
			compression: off
		}
	`))
	spoke, ospoke := RunServerWithConfig(spokeConf)
	defer spoke.Shutdown()

	hubConf := createConfFile(t, fmt.Appendf(nil, `
		server_name: "HUB"
		listen: "127.0.0.1:-1"
		accounts {
			A {
				users = [ { user: "a", password: "a" } ]
			}
		}
		leafnodes {
			compression: off
			remotes = [
				{
					url: "nats://leaf:pwd@127.0.0.1:%d"
					account: A
					compression: off
					hub: true
				}
			]
		}
	`, ospoke.LeafNode.Port))
	hub, _ := RunServerWithConfig(hubConf)
	defer hub.Shutdown()

	checkLeafNodeConnected(t, hub)
	checkLeafNodeConnected(t, spoke)

	var hubLeaf *client
	hub.mu.Lock()
	for _, hubLeaf = range hub.leafs {
		break
	}
	hub.mu.Unlock()
	if hubLeaf == nil {
		t.Fatal("Expected a leaf connection on hub")
	}
	hubLeaf.mu.Lock()
	require_True(t, hubLeaf.isSolicitedLeafNode())
	require_True(t, hubLeaf.isHubLeafNode())
	hubLeaf.mu.Unlock()

	var spokeLeaf *client
	spoke.mu.Lock()
	for _, spokeLeaf = range spoke.leafs {
		break
	}
	spoke.mu.Unlock()
	if spokeLeaf == nil {
		t.Fatal("Expected a leaf connection on spoke")
	}
	spokeLeaf.mu.Lock()
	require_False(t, spokeLeaf.isSolicitedLeafNode())
	require_True(t, spokeLeaf.isSpokeLeafNode())
	spokeLeaf.mu.Unlock()

	ncSpoke := natsConnect(t, spoke.ClientURL(), nats.UserInfo("a", "a"))
	defer ncSpoke.Close()

	sub := natsSubSync(t, ncSpoke, "pub.*")
	natsFlush(t, ncSpoke)

	_, err := fmt.Fprintf(hubLeaf.nc, "LMSG %s %d\r\n%s\r\n", "pub.blocked", len("blocked"), "blocked")
	require_NoError(t, err)
	_, err = fmt.Fprintf(hubLeaf.nc, "LMSG %s %d\r\n%s\r\n", "pub.ok", len("ok"), "ok")
	require_NoError(t, err)

	msg := natsNexMsg(t, sub, time.Second)
	require_Equal(t, "pub.ok", msg.Subject)
	require_Equal(t, "ok", string(msg.Data))
}

// Test that the inbound LMSG path checks for import permissions.
// Normally, the sender side will check ACLs before sending LMSG messages.
// If it doesn't we still need to perform ACL checks on the receiving leaf side.
func TestLeafNodeLMSGHonorsImportPermissions(t *testing.T) {
	hubConf := createConfFile(t, []byte(`
		server_name: "HUB"
		listen: "127.0.0.1:-1"
		accounts {
			A {
				users = [
					{ user: "a", password: "a" }
					{ user: "leaf", password: "pwd" }
				]
			}
		}
		leafnodes {
			listen: "127.0.0.1:-1"
			compression: off
		}
	`))
	hub, ohub := RunServerWithConfig(hubConf)
	defer hub.Shutdown()

	leafConf := createConfFile(t, fmt.Appendf(nil, `
		server_name: "LEAF"
		listen: "127.0.0.1:-1"
		accounts {
			A {
				users = [ { user: "a", password: "a" } ]
			}
		}
		leafnodes {
			compression: off
			remotes = [
				{
					url: "nats://leaf:pwd@127.0.0.1:%d"
					account: A
					compression: off
					deny_imports: ["imp.blocked"]
				}
			]
		}
	`, ohub.LeafNode.Port))
	leaf, _ := RunServerWithConfig(leafConf)
	defer leaf.Shutdown()

	checkLeafNodeConnected(t, hub)
	checkLeafNodeConnected(t, leaf)

	ncLeaf := natsConnect(t, leaf.ClientURL(), nats.UserInfo("a", "a"))
	defer ncLeaf.Close()

	sub := natsSubSync(t, ncLeaf, "imp.*")
	natsFlush(t, ncLeaf)

	leafConn := func() net.Conn {
		var l *client
		hub.mu.Lock()
		for _, l = range hub.leafs {
			break
		}
		hub.mu.Unlock()
		return l.nc
	}

	ncHubLeaf := leafConn()

	_, err := fmt.Fprintf(ncHubLeaf, "LMSG %s %d\r\n%s\r\n", "imp.blocked", len("blocked"), "blocked")
	require_NoError(t, err)
	checkLeafNodeConnected(t, hub)
	checkLeafNodeConnected(t, leaf)
	_, err = fmt.Fprintf(ncHubLeaf, "LMSG %s %d\r\n%s\r\n", "imp.ok", len("ok"), "ok")
	require_NoError(t, err)

	msg := natsNexMsg(t, sub, time.Second)
	require_Equal(t, "imp.ok", msg.Subject)
	require_Equal(t, "ok", string(msg.Data))
}

func TestLeafNodesDisableRemote(t *testing.T) {
	hubConf := createConfFile(t, []byte(`
		server_name: "HUB"
		listen: "127.0.0.1:-1"
		accounts {
			leaf1 { users: [{user: leaf1, password: pwd}] }
			leaf2 { users: [{user: leaf2, password: pwd}] }
		}
		leafnodes {
			listen: "127.0.0.1:-1"
		}
	`))
	hub, ohub := RunServerWithConfig(hubConf)
	defer hub.Shutdown()

	port := ohub.LeafNode.Port
	leafTmpl := `
		server_name: "LEAF"
		listen: "127.0.0.1:-1"
		leafnodes {
			reconnect_interval: "50ms"
			remotes: [
				{
					url: "nats://leaf1:pwd@127.0.0.1:%d"
					disabled: %v
				}
				{
					url: "nats://leaf2:pwd@127.0.0.1:%d"
				}
			]
		}
	`
	// Start with "disabled: true" to make sure that we don't solicit it
	// when starting the server.
	leafConf := createConfFile(t, fmt.Appendf(nil, leafTmpl, port, true, port))
	leaf, _ := RunServerWithConfig(leafConf)
	defer leaf.Shutdown()

	// Wait for more than the reconnect interval to make sure that we don't
	// reconnect the connection that should have been disabled.
	time.Sleep(100 * time.Millisecond)

	// Verify that we have only 1 leaf
	checkLeafNodeConnected(t, hub)
	checkLeafNodeConnected(t, leaf)

	reconnectCh := make(chan struct{}, 2)
	ncl1 := natsConnect(t, hub.ClientURL(), nats.UserInfo("leaf1", "pwd"),
		nats.ReconnectWait(50*time.Millisecond),
		nats.ReconnectJitter(time.Millisecond, time.Millisecond),
		nats.ReconnectHandler(func(_ *nats.Conn) {
			reconnectCh <- struct{}{}
		}))
	defer ncl1.Close()
	sub1 := natsSubSync(t, ncl1, "foo")
	natsFlush(t, ncl1)

	ncl2 := natsConnect(t, hub.ClientURL(), nats.UserInfo("leaf2", "pwd"),
		nats.ReconnectWait(50*time.Millisecond),
		nats.ReconnectJitter(time.Millisecond, time.Millisecond),
		nats.ReconnectHandler(func(_ *nats.Conn) {
			reconnectCh <- struct{}{}
		}))
	defer ncl2.Close()
	sub2 := natsSubSync(t, ncl2, "foo")
	natsFlush(t, ncl2)

	checkSubInterest(t, leaf, globalAccountName, "foo", time.Second)

	nc := natsConnect(t, leaf.ClientURL())
	defer nc.Close()

	natsPub(t, nc, "foo", []byte("hello"))
	natsFlush(t, nc)

	// We should not receive on leaf1
	_, err := sub1.NextMsg(100 * time.Millisecond)
	require_Error(t, err, nats.ErrTimeout)

	// But should receive on leaf2.
	msg := natsNexMsg(t, sub2, time.Second)
	require_Equal(t, string(msg.Data), "hello")

	// Enable leaf1, which means set "disabled" to false.
	reloadUpdateConfig(t, leaf, leafConf, fmt.Sprintf(leafTmpl, port, false, port))

	// Check that we have 2 leaf node connections now.
	checkLeafNodeConnectedCount(t, hub, 2)
	checkLeafNodeConnectedCount(t, leaf, 2)

	// Verify connectivity.
	natsPub(t, nc, "foo", []byte("hello2"))

	msg = natsNexMsg(t, sub1, time.Second)
	require_Equal(t, string(msg.Data), "hello2")

	msg = natsNexMsg(t, sub2, time.Second)
	require_Equal(t, string(msg.Data), "hello2")

	// Disable again.
	reloadUpdateConfig(t, leaf, leafConf, fmt.Sprintf(leafTmpl, port, true, port))

	// Wait for more than the reconnect interval to make sure that we don't
	// reconnect the connection that should have been disabled.
	time.Sleep(100 * time.Millisecond)
	// Verify that we have only 1 leaf
	checkLeafNodeConnected(t, hub)
	checkLeafNodeConnected(t, leaf)

	// Now send a message again.
	natsPub(t, nc, "foo", []byte("hello3"))
	natsFlush(t, nc)
	// We should not receive on leaf1
	_, err = sub1.NextMsg(100 * time.Millisecond)
	require_Error(t, err, nats.ErrTimeout)

	// We should still receive for leaf2
	msg = natsNexMsg(t, sub2, time.Second)
	require_Equal(t, string(msg.Data), "hello3")

	// Enable again.
	reloadUpdateConfig(t, leaf, leafConf, fmt.Sprintf(leafTmpl, port, false, port))

	checkLeafNodeConnectedCount(t, hub, 2)
	checkLeafNodeConnectedCount(t, leaf, 2)

	// Now shutdown hub.
	hub.Shutdown()

	// Wait to be disconnected
	checkLeafNodeConnectedCount(t, leaf, 0)

	// Wait at least some reconnect interval.
	time.Sleep(100 * time.Millisecond)

	// Now disable leaf1 one more time to make sure we stop soliciting when we were
	// not currently connected.
	reloadUpdateConfig(t, leaf, leafConf, fmt.Sprintf(leafTmpl, port, true, port))

	// Restart hub
	hub = RunServer(ohub)
	defer hub.Shutdown()

	// Verify that we have only 1 leaf
	checkLeafNodeConnected(t, hub)
	checkLeafNodeConnected(t, leaf)

	// Wait for clients to reconnect
	for range 2 {
		select {
		case <-reconnectCh:
		case <-time.After(time.Second):
			t.Fatal("Client failed to reconnect")
		}
	}

	// Wait for subject propagation
	checkSubInterest(t, leaf, globalAccountName, "foo", time.Second)

	// Now send a message again.
	natsPub(t, nc, "foo", []byte("hello4"))
	natsFlush(t, nc)
	// We should not receive on leaf1
	_, err = sub1.NextMsg(100 * time.Millisecond)
	require_Error(t, err, nats.ErrTimeout)

	// We should still receive for leaf2
	msg = natsNexMsg(t, sub2, time.Second)
	require_Equal(t, string(msg.Data), "hello4")
}

func TestLeafNodeIsolatedLeafSubjectPropagationGlobal(t *testing.T) {
	for tname, isolated := range map[string]bool{
		"Isolated": true,
		"Normal":   false,
	} {
		t.Run(tname, func(t *testing.T) {
			hubTmpl := `
				port: -1
				server_name: "%s"
				accounts {
					HA { users: [{user: HA, password: pwd}] }
				}
				leafnodes {
					port: -1
					isolate_leafnode_interest: %v
				}
			`
			confH := createConfFile(t, []byte(fmt.Sprintf(hubTmpl, "H1", isolated)))
			sh, oh := RunServerWithConfig(confH)
			defer sh.Shutdown()

			spokeTmpl := `
				port: -1
				server_name: "%s"
				accounts {
					A { users: [{user: A, password: pwd}] }
				}
				leafnodes {
					remotes [
						{
							url: "nats://HA:pwd@127.0.0.1:%d"
							local: "A"
						}
					]
				}
			`

			confSP1 := createConfFile(t, []byte(fmt.Sprintf(spokeTmpl, "SP1", oh.LeafNode.Port)))
			sp1, _ := RunServerWithConfig(confSP1)
			defer sp1.Shutdown()

			confSP2 := createConfFile(t, []byte(fmt.Sprintf(spokeTmpl, "SP2", oh.LeafNode.Port)))
			sp2, _ := RunServerWithConfig(confSP2)
			defer sp2.Shutdown()

			checkLeafNodeConnectedCount(t, sh, 2)
			checkLeafNodeConnectedCount(t, sp1, 1)
			checkLeafNodeConnectedCount(t, sp2, 1)

			// We expect that the hub side will have answered the request from the spoke for
			// isolation if needed, but the spokes themselves will not themselves isolate
			// subscription interest in the other direction unless also configured to do so.
			for _, c := range sh.leafs {
				require_Equal(t, c.leaf.isolated, isolated)
			}
			for _, c := range sp1.leafs {
				require_False(t, c.leaf.isolated)
			}
			for _, c := range sp2.leafs {
				require_False(t, c.leaf.isolated)
			}

			nch := natsConnect(t, sh.ClientURL(), nats.UserInfo("HA", "pwd"))
			nc1 := natsConnect(t, sp1.ClientURL(), nats.UserInfo("A", "pwd"))

			// Create a north-south subscription on the hub that should be visible to both spokes.
			nssub, err := nch.SubscribeSync("northsouth")
			require_NoError(t, err)
			checkSubInterest(t, sh, "HA", "northsouth", time.Second) // Visible to the hub.
			checkSubInterest(t, sp1, "A", "northsouth", time.Second) // Visible to both spokes.
			checkSubInterest(t, sp2, "A", "northsouth", time.Second) // Visible to both spokes.

			// The spoke subscriptions should be visible to the hub in all cases, but only
			// visible to other spokes if they are not isolated.
			ewsub, err := nc1.SubscribeSync("eastwest")
			require_NoError(t, err)
			checkSubInterest(t, sh, "HA", "eastwest", time.Second) // Visible to the hub.
			checkSubInterest(t, sp1, "A", "eastwest", time.Second) // Visible to the spoke with the sub.
			if isolated {
				checkSubNoInterest(t, sp2, "A", "eastwest", time.Second) // Not visible to the other spoke.
			} else {
				checkSubInterest(t, sp2, "A", "eastwest", time.Second) // Visible to the other spoke.
			}
			require_NoError(t, ewsub.Unsubscribe())
			checkSubNoInterest(t, sh, "HA", "eastwest", time.Second)
			checkSubNoInterest(t, sp1, "A", "eastwest", time.Second)
			checkSubNoInterest(t, sp2, "A", "eastwest", time.Second)

			// ... but a subscription from the hub should be visible to both.
			require_NoError(t, nssub.Unsubscribe())
			checkSubNoInterest(t, sh, "HA", "northsouth", time.Second)
			checkSubNoInterest(t, sp1, "A", "northsouth", time.Second)
			checkSubNoInterest(t, sp2, "A", "northsouth", time.Second)
		})
	}
}

func TestLeafNodeIsolatedCheckBeforeSubPermCheck(t *testing.T) {
	confH := createConfFile(t, []byte(`
		port: -1
		server_name: "HUB"
		accounts {
			HA { users: [{user: HA, password: pwd, permissions: {pub: ["$SYS.>", "$LDS.>", "foo"]} }] }
		}
		leafnodes {
			port: -1
			isolate_leafnode_interest: true
		}
	`))
	sh, oh := RunServerWithConfig(confH)
	defer sh.Shutdown()

	l := &captureDebugLogger{dbgCh: make(chan string, 100)}
	sh.SetLogger(l, true, false)

	spokeTmpl := `
		port: -1
		server_name: "%s"
		leafnodes {
			remotes [{url: "nats://HA:pwd@127.0.0.1:%d"}]
		}
	`
	confSP1 := createConfFile(t, []byte(fmt.Sprintf(spokeTmpl, "SP1", oh.LeafNode.Port)))
	sp1, _ := RunServerWithConfig(confSP1)
	defer sp1.Shutdown()

	checkLeafNodeConnectedCount(t, sh, 1)
	checkLeafNodeConnectedCount(t, sp1, 1)

	nc := natsConnect(t, sp1.ClientURL())
	defer nc.Close()

	natsSubSync(t, nc, "bar")
	natsFlush(t, nc)

	// This should be visible to the hub.
	checkSubInterest(t, sh, "HA", "bar", time.Second)

	confSP2 := createConfFile(t, []byte(fmt.Sprintf(spokeTmpl, "SP2", oh.LeafNode.Port)))
	sp2, _ := RunServerWithConfig(confSP2)
	defer sp2.Shutdown()

	checkLeafNodeConnectedCount(t, sh, 2)
	checkLeafNodeConnectedCount(t, sp1, 1)
	checkLeafNodeConnectedCount(t, sp2, 1)

	// This subscription should not be visible to sp2.
	checkSubNoInterest(t, sp2, globalAccountName, "bar", time.Second)

	// Make sure that there was no debug statement about not being permitted to subscribe
	// to bar on behalf of HA.
	for {
		select {
		case dbg := <-l.dbgCh:
			if strings.Contains(dbg, "Not permitted") {
				t.Fatalf("Should not have had the permission check done: %s", dbg)
			}
		default:
			// No debug, we are done!
			return
		}
	}
}

func TestLeafNodeIsolatedLeafSubjectPropagationRequestIsolation(t *testing.T) {
	for tname, isolated := range map[string]bool{
		"Isolated": true,
		"Normal":   false,
	} {
		t.Run(tname, func(t *testing.T) {
			hubTmpl := `
				port: -1
				server_name: "%s"
				accounts {
					HA { users: [{user: HA, password: pwd}] }
				}
				leafnodes {
					port: -1
				}
			`
			confH := createConfFile(t, []byte(fmt.Sprintf(hubTmpl, "H1")))
			sh, oh := RunServerWithConfig(confH)
			defer sh.Shutdown()

			spokeTmpl := `
				port: -1
				server_name: "%s"
				accounts {
					A { users: [{user: A, password: pwd}] }
				}
				leafnodes {
					remotes [
						{
							url: "nats://HA:pwd@127.0.0.1:%d"
							local: "A"
							request_isolation: %v
						}
					]
				}
			`

			confSP1 := createConfFile(t, []byte(fmt.Sprintf(spokeTmpl, "SP1", oh.LeafNode.Port, isolated)))
			sp1, _ := RunServerWithConfig(confSP1)
			defer sp1.Shutdown()

			confSP2 := createConfFile(t, []byte(fmt.Sprintf(spokeTmpl, "SP2", oh.LeafNode.Port, isolated)))
			sp2, _ := RunServerWithConfig(confSP2)
			defer sp2.Shutdown()

			checkLeafNodeConnectedCount(t, sh, 2)
			checkLeafNodeConnectedCount(t, sp1, 1)
			checkLeafNodeConnectedCount(t, sp2, 1)

			// We expect that the hub side will have answered the request from the spoke for
			// isolation if needed, but the spokes themselves will not themselves isolate
			// subscription interest in the other direction unless also configured to do so.
			for _, c := range sh.leafs {
				require_Equal(t, c.leaf.isolated, isolated)
			}
			for _, c := range sp1.leafs {
				require_False(t, c.leaf.isolated)
			}
			for _, c := range sp2.leafs {
				require_False(t, c.leaf.isolated)
			}

			nch := natsConnect(t, sh.ClientURL(), nats.UserInfo("HA", "pwd"))
			nc1 := natsConnect(t, sp1.ClientURL(), nats.UserInfo("A", "pwd"))

			// Create a north-south subscription on the hub that should be visible to both spokes.
			nssub, err := nch.SubscribeSync("northsouth")
			require_NoError(t, err)
			checkSubInterest(t, sh, "HA", "northsouth", time.Second) // Visible to the hub.
			checkSubInterest(t, sp1, "A", "northsouth", time.Second) // Visible to both spokes.
			checkSubInterest(t, sp2, "A", "northsouth", time.Second) // Visible to both spokes.

			// The spoke subscriptions should be visible to the hub in all cases, but only
			// visible to other spokes if they are not isolated.
			ewsub, err := nc1.SubscribeSync("eastwest")
			require_NoError(t, err)
			checkSubInterest(t, sh, "HA", "eastwest", time.Second) // Visible to the hub.
			checkSubInterest(t, sp1, "A", "eastwest", time.Second) // Visible to the spoke with the sub.
			if isolated {
				checkSubNoInterest(t, sp2, "A", "eastwest", time.Second) // Not visible to the other spoke.
			} else {
				checkSubInterest(t, sp2, "A", "eastwest", time.Second) // Visible to the other spoke.
			}
			require_NoError(t, ewsub.Unsubscribe())
			checkSubNoInterest(t, sh, "HA", "eastwest", time.Second)
			checkSubNoInterest(t, sp1, "A", "eastwest", time.Second)
			checkSubNoInterest(t, sp2, "A", "eastwest", time.Second)

			// ... but a subscription from the hub should be visible to both.
			require_NoError(t, nssub.Unsubscribe())
			checkSubNoInterest(t, sh, "HA", "northsouth", time.Second)
			checkSubNoInterest(t, sp1, "A", "northsouth", time.Second)
			checkSubNoInterest(t, sp2, "A", "northsouth", time.Second)
		})
	}
}

func TestLeafNodeIsolatedLeafSubjectPropagationLocalIsolation(t *testing.T) {
	for tname, isolated := range map[string]bool{
		"Isolated": true,
		"Normal":   false,
	} {
		t.Run(tname, func(t *testing.T) {
			spokeTmpl := `
				port: -1
				server_name: "%s"
				accounts {
					A { users: [{user: A, password: pwd}] }
				}
				leafnodes {
					port: -1
				}
			`

			confSP1 := createConfFile(t, []byte(fmt.Sprintf(spokeTmpl, "SP1")))
			sp1, osp1 := RunServerWithConfig(confSP1)
			defer sp1.Shutdown()

			confSP2 := createConfFile(t, []byte(fmt.Sprintf(spokeTmpl, "SP2")))
			sp2, osp2 := RunServerWithConfig(confSP2)
			defer sp2.Shutdown()

			hubTmpl := `
				port: -1
				server_name: "%s"
				accounts {
					HA { users: [{user: HA, password: pwd}] }
				}
				leafnodes {
					port: -1
					remotes [
						{
							url: "nats://A:pwd@127.0.0.1:%d"
							local: "HA"
							isolate: %v
							hub: true
						},
						{
							url: "nats://A:pwd@127.0.0.1:%d"
							local: "HA"
							isolate: %v
							hub: true
						}
					]
				}
			`
			confH := createConfFile(t, []byte(fmt.Sprintf(hubTmpl, "H1", osp1.LeafNode.Port, isolated, osp2.LeafNode.Port, isolated)))
			sh, _ := RunServerWithConfig(confH)
			defer sh.Shutdown()

			checkLeafNodeConnectedCount(t, sh, 2)
			checkLeafNodeConnectedCount(t, sp1, 1)
			checkLeafNodeConnectedCount(t, sp2, 1)

			// We expect that the hub side will have answered the request from the spoke for
			// isolation if needed, but the spokes themselves will not themselves isolate
			// subscription interest in the other direction unless also configured to do so.
			for _, c := range sh.leafs {
				require_Equal(t, c.leaf.isolated, isolated)
			}
			for _, c := range sp1.leafs {
				require_False(t, c.leaf.isolated)
			}
			for _, c := range sp2.leafs {
				require_False(t, c.leaf.isolated)
			}

			nch := natsConnect(t, sh.ClientURL(), nats.UserInfo("HA", "pwd"))
			nc1 := natsConnect(t, sp1.ClientURL(), nats.UserInfo("A", "pwd"))

			// Create a north-south subscription on the hub that should be visible to both spokes.
			nssub, err := nch.SubscribeSync("northsouth")
			require_NoError(t, err)
			checkSubInterest(t, sh, "HA", "northsouth", time.Second) // Visible to the hub.
			checkSubInterest(t, sp1, "A", "northsouth", time.Second) // Visible to both spokes.
			checkSubInterest(t, sp2, "A", "northsouth", time.Second) // Visible to both spokes.

			// The spoke subscriptions should be visible to the hub in all cases, but only
			// visible to other spokes if they are not isolated.
			ewsub, err := nc1.SubscribeSync("eastwest")
			require_NoError(t, err)
			checkSubInterest(t, sh, "HA", "eastwest", time.Second) // Visible to the hub.
			checkSubInterest(t, sp1, "A", "eastwest", time.Second) // Visible to the spoke with the sub.
			if isolated {
				checkSubNoInterest(t, sp2, "A", "eastwest", time.Second) // Not visible to the other spoke.
			} else {
				checkSubInterest(t, sp2, "A", "eastwest", time.Second) // Visible to the other spoke.
			}
			require_NoError(t, ewsub.Unsubscribe())
			checkSubNoInterest(t, sh, "HA", "eastwest", time.Second)
			checkSubNoInterest(t, sp1, "A", "eastwest", time.Second)
			checkSubNoInterest(t, sp2, "A", "eastwest", time.Second)

			// ... but a subscription from the hub should be visible to both.
			require_NoError(t, nssub.Unsubscribe())
			checkSubNoInterest(t, sh, "HA", "northsouth", time.Second)
			checkSubNoInterest(t, sp1, "A", "northsouth", time.Second)
			checkSubNoInterest(t, sp2, "A", "northsouth", time.Second)
		})
	}
}

func TestLeafNodeIsolatedSpokeInitialSubs(t *testing.T) {
	for tname, test := range map[string]struct{ global, remote, hub, request bool }{
		"IsolatedGlobal":     {global: true},
		"IsolatedRemote":     {remote: true},
		"Normal":             {},
		"HubIsolatedGlobal":  {hub: true, global: true},
		"HubIsolatedRequest": {hub: true, request: true},
		"HubNormal":          {hub: true},
	} {
		t.Run(tname, func(t *testing.T) {
			isolated := test.global || test.remote || test.request
			topTmpl := `
				port: -1
				server_name: "TOP"
				accounts {
					HA { users: [{user: HA, password: pwd}] }
				}
				leafnodes {
					port: -1
					reconnect: "50ms"
					%s
				}
			`
			midTmpl := `
				port: -1
				server_name: "MID"
				accounts {
					A { users: [{user: A, password: pwd}] }
				}
				leafnodes {
					port: -1
					reconnect: "50ms"
					isolate_leafnode_interest: %v
					%s
				}
			`
			var (
				top, mid   *Server
				otop, omid *Options
			)
			if test.hub {
				// TOP solicits MID as a hub, so MID takes on the spoke role.
				confMid := createConfFile(t, []byte(fmt.Sprintf(midTmpl, test.global, _EMPTY_)))
				mid, omid = RunServerWithConfig(confMid)
				defer mid.Shutdown()

				remotes := fmt.Sprintf(`remotes [
					{
						url: "nats://A:pwd@127.0.0.1:%d"
						local: "HA"
						hub: true
						request_isolation: %v
					}
				]`, omid.LeafNode.Port, test.request)
				confTop := createConfFile(t, []byte(fmt.Sprintf(topTmpl, remotes)))
				top, otop = RunServerWithConfig(confTop)
				defer top.Shutdown()
			} else {
				// MID solicits TOP, so MID is the spoke.
				confTop := createConfFile(t, []byte(fmt.Sprintf(topTmpl, _EMPTY_)))
				top, otop = RunServerWithConfig(confTop)
				defer top.Shutdown()

				remotes := fmt.Sprintf(`remotes [
					{
						url: "nats://HA:pwd@127.0.0.1:%d"
						local: "A"
						isolate: %v
					}
				]`, otop.LeafNode.Port, test.remote)
				confMid := createConfFile(t, []byte(fmt.Sprintf(midTmpl, test.global, remotes)))
				mid, omid = RunServerWithConfig(confMid)
				defer mid.Shutdown()
			}

			botTmpl := `
				port: -1
				server_name: "BOTTOM"
				accounts {
					B { users: [{user: B, password: pwd}] }
				}
				leafnodes {
					remotes [
						{
							url: "nats://A:pwd@127.0.0.1:%d"
							local: "B"
						}
					]
				}
			`
			confBot := createConfFile(t, []byte(fmt.Sprintf(botTmpl, omid.LeafNode.Port)))
			bot, _ := RunServerWithConfig(confBot)
			defer bot.Shutdown()

			checkLeafNodeConnectedCount(t, top, 1)
			checkLeafNodeConnectedCount(t, mid, 2)
			checkLeafNodeConnectedCount(t, bot, 1)

			// Stop TOP so that the subscriptions below are only sent in MID's initial subs.
			top.Shutdown()
			checkLeafNodeConnectedCount(t, mid, 1)

			ncm := natsConnect(t, mid.ClientURL(), nats.UserInfo("A", "pwd"))
			defer ncm.Close()
			natsSubSync(t, ncm, "local")
			natsFlush(t, ncm)

			ncb := natsConnect(t, bot.ClientURL(), nats.UserInfo("B", "pwd"))
			defer ncb.Close()
			lsub := natsSubSync(t, ncb, "leaf")
			natsFlush(t, ncb)
			checkSubInterest(t, mid, "A", "leaf", time.Second)

			top = RunServer(otop)
			defer top.Shutdown()
			checkLeafNodeConnectedCount(t, top, 1)
			checkLeafNodeConnectedCount(t, mid, 2)

			// MID's local interest should be visible to TOP in all cases, but BOTTOM's
			// interest should only be visible if not isolated.
			checkSubInterest(t, top, "HA", "local", time.Second)
			if isolated {
				checkSubNoInterest(t, top, "HA", "leaf", time.Second)
			} else {
				checkSubInterest(t, top, "HA", "leaf", time.Second)
			}

			// New subscriptions from BOTTOM should follow the same rules.
			natsSubSync(t, ncb, "leaf2")
			natsFlush(t, ncb)
			checkSubInterest(t, mid, "A", "leaf2", time.Second)
			// MID sends updates in order, so "leaf2" would arrive before "local2".
			natsSubSync(t, ncm, "local2")
			natsFlush(t, ncm)
			checkSubInterest(t, top, "HA", "local2", time.Second)
			if isolated {
				checkSubNoInterest(t, top, "HA", "leaf2", time.Second)
			} else {
				checkSubInterest(t, top, "HA", "leaf2", time.Second)
			}

			// Once BOTTOM unsubscribes, TOP must not keep stale interest.
			natsUnsub(t, lsub)
			natsFlush(t, ncb)
			checkSubNoInterest(t, mid, "A", "leaf", time.Second)
			checkSubNoInterest(t, top, "HA", "leaf", time.Second)
		})
	}
}

func BenchmarkLeafNodeIsolatedSpokeInitialSubs(b *testing.B) {
	for _, leafSubs := range []int{0, 10_000, 100_000} {
		b.Run(fmt.Sprintf("LeafSubs=%d", leafSubs), func(b *testing.B) {
			s := RunServer(DefaultOptions())
			defer s.Shutdown()
			acc, err := s.RegisterAccount("A")
			require_NoError(b, err)

			newClient := func(kind int, l *leaf) *client {
				c := &client{srv: s, kind: kind, acc: acc, leaf: l}
				c.initClient()
				return c
			}
			local := newClient(CLIENT, nil)
			downstream := newClient(LEAF, &leaf{})
			for i := range 1_000 {
				require_NoError(b, acc.sl.Insert(&subscription{client: local, subject: fmt.Appendf(nil, "local.%d", i)}))
			}
			for i := range leafSubs {
				require_NoError(b, acc.sl.Insert(&subscription{client: downstream, subject: fmt.Appendf(nil, "leaf.%d", i), leaf: true}))
			}

			c := newClient(LEAF, &leaf{isSpoke: true, isolated: true})
			b.ReportAllocs()
			b.ResetTimer()
			for range b.N {
				s.initLeafNodeSmapAndSendSubs(c)

				// Reset state, as there's no write loop or connection.
				b.StopTimer()
				c.mu.Lock()
				for sub := range c.leaf.tsub {
					require_NotEqual(b, sub.client, downstream)
				}
				c.leaf.tsubt.Stop()
				c.leaf.tsub, c.leaf.tsubt = nil, nil
				c.out.nb, c.out.pb, c.out.stc = nil, 0, nil
				s.gwLeafSubs.Remove(c.leaf.gwSub)
				c.mu.Unlock()
				b.StartTimer()
			}
		})
	}
}

func TestLeafNodeClusterIsolatedLeafSubjectPropagation(t *testing.T) {
	spokeTmpl := `
		port: -1
		server_name: "%s"
		accounts {
			A { users: [{user: A, password: pwd}] }
		}
		leafnodes { port: -1 }
	`

	sp1Conf := createConfFile(t, []byte(fmt.Sprintf(spokeTmpl, "SP1")))
	sp1, sp1Opts := RunServerWithConfig(sp1Conf)
	defer sp1.Shutdown()

	sp2Conf := createConfFile(t, []byte(fmt.Sprintf(spokeTmpl, "SP2")))
	sp2, sp2Opts := RunServerWithConfig(sp2Conf)
	defer sp2.Shutdown()

	hubTmpl := `
		port: -1
		server_name: "%s"
		accounts {
			HA { users: [{user: HA, password: pwd}] }
		}
		cluster {
			name: HUB
			listen: 127.0.0.1:-1
			%s
		}
		leafnodes {
			port: -1
			%s
		}
	`
	remoteTmpl := `
		remotes: [{
			url: "nats://A:pwd@127.0.0.1:%d"
			local: "HA"
			isolate: %v
			hub: true
		}]
	`

	h1Conf := createConfFile(t, []byte(fmt.Sprintf(hubTmpl, "H1", _EMPTY_, fmt.Sprintf(remoteTmpl, sp1Opts.LeafNode.Port, false))))
	h1, h1Opts := RunServerWithConfig(h1Conf)
	defer h1.Shutdown()

	routeToH1 := fmt.Sprintf("routes: [\"nats://127.0.0.1:%d\"]", h1Opts.Cluster.Port)
	h2Conf := createConfFile(t, []byte(fmt.Sprintf(hubTmpl, "H2", routeToH1, _EMPTY_)))
	h2, _ := RunServerWithConfig(h2Conf)
	defer h2.Shutdown()

	h3Conf := createConfFile(t, []byte(fmt.Sprintf(hubTmpl, "H3", routeToH1, fmt.Sprintf(remoteTmpl, sp2Opts.LeafNode.Port, true))))
	h3, _ := RunServerWithConfig(h3Conf)
	defer h3.Shutdown()

	checkClusterFormed(t, h1, h2, h3)
	checkLeafNodeConnected(t, h1)
	checkLeafNodeConnected(t, h3)
	checkLeafNodeConnected(t, sp1)
	checkLeafNodeConnected(t, sp2)

	// Hub-originated interest remains north-south even when the leaf connection
	// is isolated. Connect the clients to different members of the hub cluster.
	nch1 := natsConnect(t, h1.ClientURL(), nats.UserInfo("HA", "pwd"))
	defer nch1.Close()

	nch3 := natsConnect(t, h3.ClientURL(), nats.UserInfo("HA", "pwd"))
	defer nch3.Close()

	ns1, err := nch1.SubscribeSync("northsouth.h1")
	require_NoError(t, err)

	ns3, err := nch3.SubscribeSync("northsouth.h3")
	require_NoError(t, err)

	checkSubInterest(t, sp1, "A", "northsouth.h1", time.Second)
	checkSubInterest(t, sp2, "A", "northsouth.h1", time.Second)
	checkSubInterest(t, sp1, "A", "northsouth.h3", time.Second)
	checkSubInterest(t, sp2, "A", "northsouth.h3", time.Second)

	// Interest originating from SP1 enters the cluster through H1 and reaches
	// H3 over a route. It must not be forwarded through H3's isolated leaf.
	nc1 := natsConnect(t, sp1.ClientURL(), nats.UserInfo("A", "pwd"))
	defer nc1.Close()

	ew, err := nc1.SubscribeSync("eastwest")
	require_NoError(t, err)

	checkSubInterest(t, h3, "HA", "eastwest", time.Second)
	checkSubNoInterest(t, sp2, "A", "eastwest", time.Second)

	require_NoError(t, ew.Unsubscribe())

	checkSubNoInterest(t, h1, "HA", "eastwest", time.Second)
	checkSubNoInterest(t, h2, "HA", "eastwest", time.Second)
	checkSubNoInterest(t, h3, "HA", "eastwest", time.Second)

	require_NoError(t, ns1.Unsubscribe())
	require_NoError(t, ns3.Unsubscribe())
}

func TestLeafNodeDaisyChainWithAccountImportExport(t *testing.T) {
	hubConf := createConfFile(t, []byte(`
		server_name: hub
		listen: "127.0.0.1:-1"

		leafnodes {
			listen: "127.0.0.1:-1"
		}
		accounts {
			SYS: {
				users: [{ user: s, password: s}],
			},
			ODC: {
				jetstream: enabled
				users: [
					{
						user: u, password: u,
						permissions: {
							publish: {deny: ["local.>","hub2leaf.>"]}
							subscribe: {deny: ["local.>","leaf2leaf.>"]}
						}
					}
				]
			}
		}
	`))
	hub, ohub := RunServerWithConfig(hubConf)
	defer hub.Shutdown()

	storeDir := t.TempDir()
	leafJSConf := createConfFile(t, fmt.Appendf(nil, `
		server_name: leaf-js
		listen: "127.0.0.1:-1"

		jetstream {
			store_dir="%s/leaf-js"
			domain=leaf-js
		}
		accounts {
			ODC: {
				jetstream: enabled
				users: [{ user: u, password: u}]
			},
		}
		leafnodes {
			remotes [
				{
					urls: ["leaf://u:u@127.0.0.1:%d"] # connects to hub
					account: ODC
				}
			]
		}
	`, storeDir, ohub.LeafNode.Port))
	leafJS, _ := RunServerWithConfig(leafJSConf)
	defer leafJS.Shutdown()

	checkLeafNodeConnected(t, hub)
	checkLeafNodeConnected(t, leafJS)

	otherConf := createConfFile(t, []byte(`
		server_name: other
		listen: "127.0.0.1:-1"
		leafnodes {
			listen: "127.0.0.1:-1"
		}
	`))
	other, oother := RunServerWithConfig(otherConf)
	defer other.Shutdown()

	tmpl := `
		server_name: %s
		listen: "127.0.0.1:-1"

		leafnodes {
			listen: "127.0.0.1:-1"
			remotes [
				{
					urls: ["leaf://u:u@127.0.0.1:%d"]
					account: ODC_DEV
				}
				{
					urls: ["leaf://127.0.0.1:%d"]
					account: ODC_DEV
				}
			]
		}
		cluster {
			name: "hubsh"
			listen: "127.0.0.1:-1"
			%s
		}
		accounts: {
			ODC_DEV: {
				users: [
					{user: o, password: o}
				]
				imports: [
					{service: {account: "SH1", subject: "$JS.leaf-sh.API.>"}}
					{stream: {account: "SH1", subject: "sync.leaf-sh.jspush.>"}}
				]
				exports: [
					{stream: ">"}
					{service: ">", response_type: "Singleton"}
				]
			}
			SH1: {
				users: [
					{user: s, password: s}
				]
				exports: [
					{service: "$JS.leaf-sh.API.>", response_type: "Stream"}
					{stream: "sync.leaf-sh.jspush.>"}
				]
			}
		}
	`
	hubSh1Conf := createConfFile(t, fmt.Appendf(nil, tmpl, "hubsh1", ohub.LeafNode.Port, oother.LeafNode.Port, _EMPTY_))
	hubSh1, ohubSh1 := RunServerWithConfig(hubSh1Conf)
	defer hubSh1.Shutdown()

	hubSh2Conf := createConfFile(t, fmt.Appendf(nil, tmpl, "hubsh2", ohub.LeafNode.Port, oother.LeafNode.Port,
		fmt.Sprintf("routes: [\"nats://127.0.0.1:%d\"]", ohubSh1.Cluster.Port)))
	hubSh2, ohubSh2 := RunServerWithConfig(hubSh2Conf)
	defer hubSh2.Shutdown()

	checkClusterFormed(t, hubSh1, hubSh2)

	checkLeafNodeConnectedCount(t, hub, 3)
	checkLeafNodeConnectedCount(t, hubSh1, 2)
	checkLeafNodeConnectedCount(t, hubSh2, 2)

	leafShConf := createConfFile(t, fmt.Appendf(nil, `
		server_name: leafsh
		listen: "127.0.0.1:-1"

		jetstream {
			store_dir="%s/leafsh"
			domain=leaf-sh
		}
		accounts {
			SH: {
				jetstream: enabled
				users: [{user: u, password: u}]
			}
		}
		leafnodes {
			remotes [
				{
					urls: ["leaf://s:s@127.0.0.1:%d"]
					account: SH
				}
			]
		}
	`, storeDir, ohubSh2.LeafNode.Port))
	leafSh, _ := RunServerWithConfig(leafShConf)
	defer leafSh.Shutdown()

	checkLeafNodeConnectedCount(t, hubSh2, 3)
	checkLeafNodeConnected(t, leafSh)

	ncLeafSh, jsLeafSh := jsClientConnect(t, leafSh, nats.UserInfo("u", "u"))
	defer ncLeafSh.Close()

	sc := &nats.StreamConfig{
		Name:        "leaf-sh",
		Subjects:    []string{"leaf2leaf.>"},
		Retention:   nats.LimitsPolicy,
		Storage:     nats.FileStorage,
		AllowRollup: true,
		AllowDirect: true,
	}
	_, err := jsLeafSh.AddStream(sc)
	require_NoError(t, err)

	ncLeafJS, jsLeafJS := jsClientConnect(t, leafJS, nats.UserInfo("u", "u"))
	defer ncLeafJS.Close()

	sc = &nats.StreamConfig{
		Name:        "leaf-js",
		Retention:   nats.LimitsPolicy,
		Storage:     nats.FileStorage,
		AllowRollup: true,
		AllowDirect: true,
		Sources: []*nats.StreamSource{
			{
				Name: "leaf-sh",
				External: &nats.ExternalStream{
					APIPrefix:     "$JS.leaf-sh.API",
					DeliverPrefix: "sync.leaf-sh.jspush",
				},
			},
		},
	}
	_, err = jsLeafJS.AddStream(sc)
	require_NoError(t, err)

	for range 10 {
		_, err = jsLeafSh.Publish("leaf2leaf.v1.test", []byte("hello"))
		require_NoError(t, err)
	}

	check := func(js nats.JetStreamContext, stream string) {
		t.Helper()
		checkFor(t, 2*time.Second, 50*time.Millisecond, func() error {
			si, err := js.StreamInfo(stream)
			if err != nil {
				return err
			}
			if n := si.State.Msgs; n != 10 {
				return fmt.Errorf("Expected 10 messages, got %v", n)
			}
			return nil
		})
	}
	check(jsLeafSh, "leaf-sh")
	check(jsLeafJS, "leaf-js")

	acc := other.GlobalAccount()
	acc.mu.RLock()
	sr := acc.sl.ReverseMatch("sync.leaf-sh.jspush.>")
	acc.mu.RUnlock()
	require_Len(t, len(sr.psubs), 0)
}

func TestLeafNodeConfigureWriteDeadline(t *testing.T) {
	o1, o2 := DefaultOptions(), DefaultOptions()

	o1.LeafNode.WriteDeadline = 5 * time.Second
	o1.LeafNode.Host = "127.0.0.1"
	o1.LeafNode.Port = -1
	s1 := RunServer(o1)
	defer s1.Shutdown()

	s1URL, _ := url.Parse(fmt.Sprintf("nats://127.0.0.1:%d", o1.LeafNode.Port))
	o2.Cluster.Name = "somethingelse"
	o2.LeafNode.Remotes = []*RemoteLeafOpts{{URLs: []*url.URL{s1URL}}}
	s2 := RunServer(o2)
	defer s2.Shutdown()

	checkLeafNodeConnected(t, s2)

	s1.mu.RLock()
	defer s1.mu.RUnlock()

	for _, r := range s1.leafs {
		require_Equal(t, r.out.wdl, 5*time.Second)
	}
}

func TestLeafNodeConfigureWriteTimeoutPolicy(t *testing.T) {
	for name, policy := range map[string]WriteTimeoutPolicy{
		"Default": WriteTimeoutPolicyDefault,
		"Retry":   WriteTimeoutPolicyRetry,
		"Close":   WriteTimeoutPolicyClose,
	} {
		t.Run(name, func(t *testing.T) {
			o1 := DefaultOptions()
			o1.LeafNode.Host = "127.0.0.1"
			o1.LeafNode.Port = -1
			o1.LeafNode.WriteTimeout = policy
			s1 := RunServer(o1)
			defer s1.Shutdown()

			s1URL, err := url.Parse(fmt.Sprintf("nats://127.0.0.1:%d", o1.LeafNode.Port))
			require_NoError(t, err)
			o2 := DefaultOptions()
			o2.Cluster.Name = "leaf"
			o2.LeafNode.Remotes = []*RemoteLeafOpts{{URLs: []*url.URL{s1URL}}}
			o2.LeafNode.WriteTimeout = policy
			s2 := RunServer(o2)
			defer s2.Shutdown()

			for side, s := range map[string]*Server{"accepting": s1, "soliciting": s2} {
				t.Run(side, func(t *testing.T) {
					checkLeafNodeConnected(t, s)
					s.mu.RLock()
					defer s.mu.RUnlock()
					require_Len(t, len(s.leafs), 1)
					for _, leaf := range s.leafs {
						leaf.mu.Lock()
						defer leaf.mu.Unlock()
						if policy == WriteTimeoutPolicyDefault {
							require_Equal(t, leaf.out.wtp, WriteTimeoutPolicyRetry)
						} else {
							require_Equal(t, leaf.out.wtp, policy)
						}
					}
				})
			}
		})
	}
}

// https://github.com/nats-io/nats-server/issues/7441
func TestLeafNodesBasicTokenAuth(t *testing.T) {
	hubConf := createConfFile(t, []byte(`
		server_name: "HUB"
		listen: "127.0.0.1:-1"
		authorization {
			token: secret
		}
		leafnodes {
			listen: "127.0.0.1:-1"
		}
	`))
	hub, ohub := RunServerWithConfig(hubConf)
	defer hub.Shutdown()

	port := ohub.LeafNode.Port
	leafTmpl := `
		server_name: "LEAF"
		listen: "127.0.0.1:-1"
		leafnodes {
			remotes: [
				{ url: "nats://secret@127.0.0.1:%d" }
			]
		}
	`
	leafConf := createConfFile(t, fmt.Appendf(nil, leafTmpl, port))
	leaf, _ := RunServerWithConfig(leafConf)
	defer leaf.Shutdown()

	// Verify that we have only 1 leaf
	checkLeafNodeConnected(t, hub)
	checkLeafNodeConnected(t, leaf)
}

func TestLeafNodeServiceImportRebuildsRequestInfoHeader(t *testing.T) {
	hubConf := createConfFile(t, []byte(`
		server_name: "HUB"
		listen: "127.0.0.1:-1"
		accounts {
			A {
				users = [ { user: "a", pass: "a" }, { user: "leaf", pass: "pwd" } ]
				imports = [ { service: { account: B, subject: "svc" } } ]
			}
			B {
				users = [ { user: "b", pass: "b" } ]
				exports = [ { service: "svc", accounts: [A] } ]
			}
		}
		leafnodes {
			listen: "127.0.0.1:-1"
			authorization {
				account: A
			}
		}
	`))
	hub, ohub := RunServerWithConfig(hubConf)
	defer hub.Shutdown()

	leafConf := createConfFile(t, []byte(fmt.Sprintf(`
		server_name: "LEAF"
		listen: "127.0.0.1:-1"
		accounts {
			A {
				users = [ { user: "a", pass: "a" } ]
			}
		}
		leafnodes {
			remotes = [
				{
					url: "nats-leaf://leaf:pwd@127.0.0.1:%d"
					account: A
				}
			]
		}
	`, ohub.LeafNode.Port)))
	leaf, _ := RunServerWithConfig(leafConf)
	defer leaf.Shutdown()

	checkLeafNodeConnected(t, hub)
	checkLeafNodeConnected(t, leaf)

	ncSvc := natsConnect(t, hub.ClientURL(), nats.UserInfo("b", "b"))
	defer ncSvc.Close()

	rcvCh := make(chan *nats.Msg, 1)
	_, err := ncSvc.Subscribe("svc", func(m *nats.Msg) {
		rcvCh <- m
		require_NoError(t, m.Respond([]byte("ok")))
	})
	require_NoError(t, err)
	require_NoError(t, ncSvc.Flush())

	ncLeaf := natsConnect(t, leaf.ClientURL(), nats.UserInfo("a", "a"))
	defer ncLeaf.Close()

	req := nats.NewMsg("svc")
	req.Header = nats.Header{}
	req.Header.Set(ClientInfoHdr, `{"acc":"EVIL","user":"mallory","host":"badhost"}`)

	_, err = ncLeaf.RequestMsg(req, time.Second)
	require_NoError(t, err)

	msg := require_ChanRead(t, rcvCh, time.Second)
	hdr := msg.Header.Get(ClientInfoHdr)
	require_NotEqual(t, hdr, _EMPTY_)

	var ci ClientInfo
	require_NoError(t, json.Unmarshal([]byte(hdr), &ci))
	require_Equal(t, ci.Account, "A")
	require_Equal(t, ci.User, _EMPTY_)
	require_Equal(t, ci.Host, _EMPTY_)
}

func TestLeafNodeNoAccPanicOnLeafSubBeforeConnect(t *testing.T) {
	o := DefaultOptions()
	o.LeafNode.Port = -1
	// Default compression is s2_auto. This used to bypass the parser's
	// pre-CONNECT guard because the handshake timeout was tracked outside
	// c.atmr, allowing LS+ through before CONNECT set c.acc.
	s := RunServer(o)
	defer s.Shutdown()

	addr := fmt.Sprintf("127.0.0.1:%d", o.LeafNode.Port)
	c, err := net.Dial("tcp", addr)
	require_NoError(t, err)
	defer c.Close()

	// Read the INFO.
	br := bufio.NewReader(c)
	c.SetReadDeadline(time.Now().Add(2 * time.Second))
	l, _, err := br.ReadLine()
	require_NoError(t, err)
	if !strings.HasPrefix(string(l), "INFO") {
		t.Fatalf("Expected INFO, got %q", l)
	}

	// Send LS+ without CONNECT first. This should not panic the server.
	_, err = c.Write([]byte("LS+ test\r\n"))
	require_NoError(t, err)

	// The server should close the connection.
	c.SetReadDeadline(time.Now().Add(2 * time.Second))
	buf := make([]byte, 256)
	for {
		_, err = c.Read(buf)
		if err != nil {
			break
		}
	}

	// Make sure the server is still running.
	s.mu.Lock()
	shutdown := s.isShuttingDown()
	s.mu.Unlock()
	if shutdown {
		t.Fatal("Server should not have shutdown")
	}
}

func TestLeafNodeNoAccPanicOnLeafUnsubBeforeConnect(t *testing.T) {
	o := DefaultOptions()
	o.LeafNode.Port = -1
	s := RunServer(o)
	defer s.Shutdown()

	addr := fmt.Sprintf("127.0.0.1:%d", o.LeafNode.Port)
	c, err := net.Dial("tcp", addr)
	require_NoError(t, err)
	defer c.Close()

	// Read the INFO.
	br := bufio.NewReader(c)
	c.SetReadDeadline(time.Now().Add(2 * time.Second))
	l, _, err := br.ReadLine()
	require_NoError(t, err)
	if !strings.HasPrefix(string(l), "INFO") {
		t.Fatalf("Expected INFO, got %q", l)
	}

	// Send LS- without CONNECT first. This should not panic the server.
	_, err = c.Write([]byte("LS- test\r\n"))
	require_NoError(t, err)

	// The server should close the connection.
	c.SetReadDeadline(time.Now().Add(2 * time.Second))
	buf := make([]byte, 256)
	for {
		_, err = c.Read(buf)
		if err != nil {
			break
		}
	}

	// Make sure the server is still running.
	s.mu.Lock()
	shutdown := s.isShuttingDown()
	s.mu.Unlock()
	if shutdown {
		t.Fatal("Server should not have shutdown")
	}
}

func TestLeafNodeNoAccPanicOnLeafErrLoopDetectedBeforeConnect(t *testing.T) {
	conf := createConfFile(t, []byte(`
		listen: "127.0.0.1:-1"
		leafnodes {
			listen: "127.0.0.1:-1"
		}
	`))
	s, opts := RunServerWithConfig(conf)
	defer s.Shutdown()

	addr := fmt.Sprintf("127.0.0.1:%d", opts.LeafNode.Port)
	c, err := net.Dial("tcp", addr)
	require_NoError(t, err)
	defer c.Close()

	// Read the INFO.
	br := bufio.NewReader(c)
	c.SetReadDeadline(time.Now().Add(2 * time.Second))
	l, _, err := br.ReadLine()
	require_NoError(t, err)
	if !strings.HasPrefix(string(l), "INFO") {
		t.Fatalf("Expected INFO, got %q", l)
	}

	// Send the loop-detected error before CONNECT. This should not panic the server.
	_, err = c.Write([]byte("-ERR 'Loop detected'\r\n"))
	require_NoError(t, err)

	// The server should close the connection.
	c.SetReadDeadline(time.Now().Add(2 * time.Second))
	buf := make([]byte, 256)
	for {
		_, err = c.Read(buf)
		if err != nil {
			break
		}
	}

	// Make sure the server is still running.
	s.mu.Lock()
	shutdown := s.isShuttingDown()
	s.mu.Unlock()
	if shutdown {
		t.Fatal("Server should not have shutdown")
	}
}

func TestLeafNodeLeafSubBeforeConnectCompressionEffect(t *testing.T) {
	for _, test := range []struct {
		name        string
		compression string
	}{
		{"compression off", CompressionOff},
		{"compression s2_auto", CompressionS2Auto},
	} {
		t.Run(test.name, func(t *testing.T) {
			o := DefaultOptions()
			o.LeafNode.Port = -1
			o.LeafNode.Compression.Mode = test.compression
			s := RunServer(o)
			defer s.Shutdown()

			addr := fmt.Sprintf("127.0.0.1:%d", o.LeafNode.Port)
			c, err := net.Dial("tcp", addr)
			require_NoError(t, err)
			defer c.Close()

			br := bufio.NewReader(c)
			c.SetReadDeadline(time.Now().Add(2 * time.Second))
			l, _, err := br.ReadLine()
			require_NoError(t, err)
			if !strings.HasPrefix(string(l), "INFO") {
				t.Fatalf("Expected INFO, got %q", l)
			}

			// Send LS+ without CONNECT.
			_, err = c.Write([]byte("LS+ test\r\n"))
			require_NoError(t, err)

			// Read the error response before the connection is closed.
			c.SetReadDeadline(time.Now().Add(2 * time.Second))
			l, _, err = br.ReadLine()
			require_NoError(t, err)
			errMsg := string(l)

			if !strings.Contains(errMsg, "Authorization Violation") {
				t.Fatalf("Expected auth violation error, got %q", errMsg)
			}

			// Make sure the server is still running.
			s.mu.Lock()
			shutdown := s.isShuttingDown()
			s.mu.Unlock()
			if shutdown {
				t.Fatal("Server should not have shutdown")
			}
		})
	}
}

func TestLeafNodeInfoBeforeConnectStillRequiresConnect(t *testing.T) {
	o := DefaultOptions()
	o.LeafNode.Port = -1
	o.LeafNode.Compression.Mode = CompressionS2Auto
	s := RunServer(o)
	defer s.Shutdown()

	addr := fmt.Sprintf("127.0.0.1:%d", o.LeafNode.Port)
	c, err := net.Dial("tcp", addr)
	require_NoError(t, err)
	defer c.Close()

	br := bufio.NewReader(c)
	c.SetReadDeadline(time.Now().Add(2 * time.Second))
	l, _, err := br.ReadLine()
	require_NoError(t, err)
	require_True(t, strings.HasPrefix(string(l), "INFO"))

	// INFO is allowed before CONNECT for compressed leaf-node negotiation.
	_, err = c.Write([]byte("INFO {\"compression\":\"off\"}\r\n"))
	require_NoError(t, err)

	// CONNECT is still required after INFO; other protocols must be rejected.
	_, err = c.Write([]byte("LS+ test\r\n"))
	require_NoError(t, err)

	require_NoError(t, c.SetReadDeadline(time.Now().Add(2*time.Second)))
	l, _, err = br.ReadLine()
	require_NoError(t, err)
	require_True(t, strings.Contains(string(l), "Authorization Violation"))
}

func TestLeafNodeInfoBeforeConnectRejectedWhenCompressionOff(t *testing.T) {
	o := DefaultOptions()
	o.LeafNode.Port = -1
	o.LeafNode.Compression.Mode = CompressionOff
	s := RunServer(o)
	defer s.Shutdown()

	addr := fmt.Sprintf("127.0.0.1:%d", o.LeafNode.Port)
	c, err := net.Dial("tcp", addr)
	require_NoError(t, err)
	defer c.Close()

	br := bufio.NewReader(c)
	c.SetReadDeadline(time.Now().Add(2 * time.Second))
	l, _, err := br.ReadLine()
	require_NoError(t, err)
	require_True(t, strings.HasPrefix(string(l), "INFO"))

	_, err = c.Write([]byte("INFO {\"nonce\":\"attacker\"}\r\n"))
	require_NoError(t, err)

	require_NoError(t, c.SetReadDeadline(time.Now().Add(2*time.Second)))
	l, _, err = br.ReadLine()
	require_NoError(t, err)
	require_True(t, strings.Contains(string(l), "Authorization Violation"))
}

func TestLeafNodeInboundInfoDoesNotOverrideNonce(t *testing.T) {
	o := DefaultOptions()
	o.LeafNode.Compression.Mode = CompressionS2Auto
	s := New(o)

	nonce := []byte("server-issued-nonce")
	c := &client{
		srv:   s,
		kind:  LEAF,
		leaf:  &leaf{},
		nonce: append([]byte(nil), nonce...),
	}
	c.flags.set(compressionNegotiated)

	c.processLeafnodeInfo(&Info{Nonce: "attacker-controlled"})

	require_True(t, bytes.Equal(c.nonce, nonce))
}

func TestLeafNodeSecondInfoBeforeConnectDoesNotPanic(t *testing.T) {
	for _, test := range []struct {
		name       string
		secondInfo string
	}{
		{"remote_account", `INFO {"remote_account":"attacker"}`},
		{"connect_info", `INFO {"connect_info":true}`},
	} {
		t.Run(test.name, func(t *testing.T) {
			o := DefaultOptions()
			o.LeafNode.Port = -1
			o.LeafNode.Compression.Mode = CompressionS2Auto
			s := RunServer(o)
			defer s.Shutdown()

			addr := fmt.Sprintf("127.0.0.1:%d", o.LeafNode.Port)
			c, err := net.Dial("tcp", addr)
			require_NoError(t, err)
			defer c.Close()

			br := bufio.NewReader(c)
			require_NoError(t, c.SetReadDeadline(time.Now().Add(2*time.Second)))
			l, _, err := br.ReadLine()
			require_NoError(t, err)
			require_True(t, strings.HasPrefix(string(l), "INFO"))

			// The first INFO is accepted so inbound compression can negotiate.
			_, err = c.Write([]byte("INFO {}\r\n"))
			require_NoError(t, err)

			// A second INFO before CONNECT used to panic the server.
			_, err = c.Write([]byte(test.secondInfo + "\r\n"))
			require_NoError(t, err)

			// Reject the connection, but keep the server alive.
			require_NoError(t, c.SetReadDeadline(time.Now().Add(2*time.Second)))
			l, _, err = br.ReadLine()
			require_NoError(t, err)
			require_True(t, strings.Contains(string(l), "Authorization Violation"))

			s.mu.Lock()
			shutdown := s.isShuttingDown()
			s.mu.Unlock()
			if shutdown {
				t.Fatal("Server should not have shutdown")
			}
		})
	}
}

func TestLeafNodeNoAccPanicOnLeafSubBeforeConnectOperatorMode(t *testing.T) {
	// Setup operator JWT-based server with leafnode port.
	// This confirms that even with full operator/JWT auth configured,
	// a raw TCP connection can bypass auth and trigger the panic.
	sysAcc, _ := nkeys.CreateAccount()
	sysAccPub, _ := sysAcc.PublicKey()

	okp, _ := nkeys.FromSeed(oSeed)
	opub, _ := okp.PublicKey()

	// Create operator claim with system account.
	oc := jwt.NewOperatorClaims(opub)
	oc.SystemAccount = sysAccPub
	operatorJwt, err := oc.Encode(okp)
	require_NoError(t, err)

	// Create the system account JWT.
	sysAccClaim := jwt.NewAccountClaims(sysAccPub)
	sysAccJwt, err := sysAccClaim.Encode(okp)
	require_NoError(t, err)

	conf := createConfFile(t, []byte(fmt.Sprintf(`
		port: -1
		server_name: OP
		operator: %s
		system_account: %s
		resolver: MEMORY
		resolver_preload: {
			%s: %s
		}
		leafnodes {
			listen: "127.0.0.1:-1"
		}
	`, operatorJwt, sysAccPub, sysAccPub, sysAccJwt)))
	s, opts := RunServerWithConfig(conf)
	defer s.Shutdown()

	addr := fmt.Sprintf("127.0.0.1:%d", opts.LeafNode.Port)
	c, err := net.Dial("tcp", addr)
	require_NoError(t, err)
	defer c.Close()

	// Read the INFO.
	br := bufio.NewReader(c)
	c.SetReadDeadline(time.Now().Add(2 * time.Second))
	l, _, err := br.ReadLine()
	require_NoError(t, err)
	if !strings.HasPrefix(string(l), "INFO") {
		t.Fatalf("Expected INFO, got %q", l)
	}

	// Send LS+ without CONNECT first, bypassing JWT auth entirely.
	// Without the fix this would panic on nil c.acc dereference.
	_, err = c.Write([]byte("LS+ test\r\n"))
	require_NoError(t, err)

	// The server should close the connection.
	c.SetReadDeadline(time.Now().Add(2 * time.Second))
	buf := make([]byte, 256)
	for {
		_, err = c.Read(buf)
		if err != nil {
			break
		}
	}

	// Make sure the server is still running.
	s.mu.Lock()
	shutdown := s.isShuttingDown()
	s.mu.Unlock()
	if shutdown {
		t.Fatal("Server should not have shutdown")
	}
}

func TestLeafNodeNoAccPanicOnProcessLeafNodeConnect(t *testing.T) {
	o := DefaultOptions()
	o.LeafNode.Port = -1
	o.LeafNode.AuthTimeout = 0.001 // Very short auth timeout (1ms)
	s := RunServer(o)
	defer s.Shutdown()

	addr := fmt.Sprintf("127.0.0.1:%d", o.LeafNode.Port)
	c, err := net.Dial("tcp", addr)
	require_NoError(t, err)
	defer c.Close()

	// Read the INFO.
	br := bufio.NewReader(c)
	c.SetReadDeadline(time.Now().Add(2 * time.Second))
	l, _, err := br.ReadLine()
	require_NoError(t, err)
	if !strings.HasPrefix(string(l), "INFO") {
		t.Fatalf("Expected INFO, got %q", l)
	}

	// Wait for the auth timeout to fire, then send CONNECT with cluster.
	// This races with the auth timeout closing the connection, which is
	// the scenario from #7989 where c.acc ends up nil.
	time.Sleep(5 * time.Millisecond)

	connect := `CONNECT {"verbose":false,"pedantic":false,"cluster":"test-cluster"}`
	c.Write([]byte(connect + "\r\n"))

	// The server should close the connection without panicking.
	c.SetReadDeadline(time.Now().Add(2 * time.Second))
	buf := make([]byte, 256)
	for {
		_, err = c.Read(buf)
		if err != nil {
			break
		}
	}

	// Make sure the server is still running (not panicked).
	s.mu.Lock()
	shutdown := s.isShuttingDown()
	s.mu.Unlock()
	if shutdown {
		t.Fatal("Server should not have shutdown")
	}
}

func TestLeafNodeSpokeConnectAdvisory(t *testing.T) {
	confHub := createConfFile(t, []byte(`
        listen: 127.0.0.1:-1
        accounts {
            SYS: {users: [{user: sys, password: pwd}]}
            USER: {users: [{user: user, password: pwd}]}
        }
        system_account: SYS
        leafnodes {
            listen: 127.0.0.1:-1
            no_advertise: true
            authorization { timeout: 0.5 }
        }
    `))
	hub, hubOpts := RunServerWithConfig(confHub)
	defer hub.Shutdown()
	hubPort := hubOpts.Port
	hubLeafPort := hubOpts.LeafNode.Port

	confSpoke := createConfFile(t, []byte(fmt.Sprintf(`
        listen: 127.0.0.1:-1
        accounts {
            SYS: {users: [{user: sys, password: pwd}]}
            USER: {users: [{user: user, password: pwd}]}
        }
        system_account: SYS
        leafnodes {
            reconnect: "50ms"
            remotes: [
                {url: "nats://user:pwd@127.0.0.1:%d", account: USER},
                {url: "nats://sys:pwd@127.0.0.1:%d", account: SYS},
            ]
        }
    `, hubLeafPort, hubLeafPort)))
	spoke, spokeOpts := RunServerWithConfig(confSpoke)
	defer spoke.Shutdown()

	checkLeafNodeConnectedCount(t, hub, 2)
	checkLeafNodeConnectedCount(t, spoke, 2)

	// Subscribe on spoke's system account for USER account events
	nc := natsConnect(t, fmt.Sprintf("nats://sys:pwd@127.0.0.1:%d", spokeOpts.Port))
	defer nc.Close()
	cSub := natsSubSync(t, nc, "$SYS.ACCOUNT.USER.CONNECT")
	dSub := natsSubSync(t, nc, "$SYS.ACCOUNT.USER.DISCONNECT")
	nc.Flush()

	// Shut down hub, spoke should emit DISCONNECT
	hub.Shutdown()
	checkLeafNodeConnectedCount(t, spoke, 0)
	msg, err := dSub.NextMsg(time.Second)
	require_NoError(t, err)
	var dm DisconnectEventMsg
	require_NoError(t, json.Unmarshal(msg.Data, &dm))
	require_Equal(t, dm.Client.Kind, "Leafnode")

	// Restart hub on the same ports so spoke can reconnect
	hubOpts.Port = hubPort
	hubOpts.LeafNode.Port = hubLeafPort
	hub = RunServer(hubOpts)
	defer hub.Shutdown()
	checkLeafNodeConnectedCount(t, spoke, 2)

	// Spoke should emit CONNECT advisory on reconnect
	msg, err = cSub.NextMsg(time.Second)
	require_NoError(t, err)
	var cm ConnectEventMsg
	require_NoError(t, json.Unmarshal(msg.Data, &cm))
	require_Equal(t, cm.Client.Kind, "Leafnode")
}

func TestLeafNodeRemoteIgnoreGossip(t *testing.T) {
	tmpl := `
		listen: "127.0.0.1:-1"
		accounts {
			A { users: [{user: "a", password: "pwd"}] }
			B { users: [{user: "b", password: "pwd"}] }
		}
		cluster {
			name: "abc"
			listen: "127.0.0.1:-1"
			%s
		}
		leafnodes {
			listen: "127.0.0.1:-1"
		}
	`
	conf1 := createConfFile(t, fmt.Appendf(nil, tmpl, _EMPTY_))
	sb1, ob1 := RunServerWithConfig(conf1)
	defer sb1.Shutdown()

	conf2 := createConfFile(t, fmt.Appendf(nil, tmpl,
		fmt.Sprintf("routes: [nats://127.0.0.1:%d]", ob1.Cluster.Port)))
	sb2, _ := RunServerWithConfig(conf2)
	defer sb2.Shutdown()

	checkClusterFormed(t, sb1, sb2)

	nc1 := natsConnect(t, sb1.ClientURL(), nats.UserInfo("a", "pwd"))
	defer nc1.Close()
	natsSubSync(t, nc1, "foo")
	natsFlush(t, nc1)

	nc2 := natsConnect(t, sb2.ClientURL(), nats.UserInfo("b", "pwd"))
	defer nc2.Close()
	natsSubSync(t, nc2, "bar")
	natsFlush(t, nc2)

	conf := createConfFile(t, fmt.Appendf(nil, `
		listen: "127.0.0.1:-1"
		leafnodes {
			reconnect_interval: "100ms"
			remotes: [
				{ url: "nats://a:pwd@127.0.0.1:%d", ignore_discovered_servers: true }
				{ url: "nats://b:pwd@127.0.0.1:%d" }
			]
		}
	`, ob1.LeafNode.Port, ob1.LeafNode.Port))
	leaf, _ := RunServerWithConfig(conf)
	defer leaf.Shutdown()

	checkLeafNodeConnectedCount(t, leaf, 2)

	checkSubInterest(t, leaf, globalAccountName, "foo", time.Second)
	checkSubInterest(t, leaf, globalAccountName, "bar", time.Second)

	// Now shutdown server sb1. The remote that connects to account "A" should
	// fail to reconnect (because it is asked to ignore gossip protocol), while
	// the remote that connects to account "B" should be able to reconnect
	// because it learned about sb2 thanks to gossip protocol (default behavior).
	sb1.Shutdown()

	// Wait a bit for the disconnect and reconnect interval to make sure there
	// is only one connected remote.
	time.Sleep(150 * time.Millisecond)
	checkLeafNodeConnectedCount(t, leaf, 1)

	checkSubNoInterest(t, leaf, globalAccountName, "foo", time.Second)
	checkSubInterest(t, leaf, globalAccountName, "bar", time.Second)
}

// Verify that the dial timeout handed to the dialer is resolved in this order:
// remote's `dial_timeout`, then the leafnodes{} block `dial_timeout`, then
// DEFAULT_ROUTE_DIAL.
//
// We install a test dialer instead of relying on a genuinely slow network,
// because a userspace listener/proxy completes the TCP handshake immediately
// and would therefore never exercise the dial timeout at all. Reproducing a
// real dial timeout requires delaying or dropping the SYN in the kernel (for
// instance with tc/netem), which is not something a unit test can rely on.
func TestLeafNodeDialTimeoutOption(t *testing.T) {
	// The remote does not need to exist: the test dialer never connects, it
	// just records the timeout it was given and returns an error, which sends
	// the connect loop into its normal retry path.
	u, err := url.Parse("nats-leaf://127.0.0.1:1234")
	require_NoError(t, err)

	for _, test := range []struct {
		name     string
		server   time.Duration
		remote   time.Duration
		expected time.Duration
	}{
		{"not set", 0, 0, DEFAULT_ROUTE_DIAL},
		{"server level", 3 * time.Second, 0, 3 * time.Second},
		{"remote level", 0, 4 * time.Second, 4 * time.Second},
		{"remote overrides server", 3 * time.Second, 4 * time.Second, 4 * time.Second},
		{"invalid server level", -1, 0, DEFAULT_ROUTE_DIAL},
		{"invalid remote level", 3 * time.Second, -1, 3 * time.Second},
	} {
		t.Run(test.name, func(t *testing.T) {
			ch := make(chan time.Duration, 1)

			o := DefaultOptions()
			o.Host = "127.0.0.1"
			o.Port = -1
			o.LeafNode.Port = 0
			o.LeafNode.ReconnectInterval = 50 * time.Millisecond
			o.LeafNode.DialTimeout = test.server
			o.LeafNode.Remotes = []*RemoteLeafOpts{{URLs: []*url.URL{u}, DialTimeout: test.remote}}
			o.LeafNode.dialer = func(network, address string, timeout time.Duration) (net.Conn, error) {
				// Non-blocking send: the connect loop will keep retrying and
				// we only care about the first invocation.
				select {
				case ch <- timeout:
				default:
				}
				return nil, fmt.Errorf("test dialer does not connect")
			}
			s := RunServer(o)
			defer s.Shutdown()

			select {
			case got := <-ch:
				require_Equal(t, got, test.expected)
			case <-time.After(2 * time.Second):
				t.Fatal("Leafnode dialer was never invoked")
			}
		})
	}
}

// Verify that `dial_timeout` is accepted both in the leafnodes{} block and on
// an individual remote.
func TestLeafNodeDialTimeoutConfig(t *testing.T) {
	conf := createConfFile(t, []byte(`
		leafnodes {
			dial_timeout: "5s"
			remotes = [
				{ url: "nats-leaf://127.0.0.1:1234" }
				{ url: "nats-leaf://127.0.0.1:5678", dial_timeout: "15s" }
			]
		}
	`))
	o, err := ProcessConfigFile(conf)
	require_NoError(t, err)

	require_Equal(t, o.LeafNode.DialTimeout, 5*time.Second)
	require_Len(t, len(o.LeafNode.Remotes), 2)
	// Not set on this remote, so the leafnodes{} block value applies.
	require_Equal(t, o.LeafNode.Remotes[0].DialTimeout, time.Duration(0))
	require_Equal(t, o.LeafNode.Remotes[1].DialTimeout, 15*time.Second)
}

// An invalid duration should be reported as a config error rather than being
// silently ignored.
func TestLeafNodeDialTimeoutConfigInvalid(t *testing.T) {
	for _, test := range []struct {
		name string
		conf string
	}{
		{"server level", `
			leafnodes {
				dial_timeout: abc
			}
		`},
		{"remote level", `
			leafnodes {
				remotes = [
					{ url: "nats-leaf://127.0.0.1:1234", dial_timeout: abc }
				]
			}
		`},
	} {
		t.Run(test.name, func(t *testing.T) {
			conf := createConfFile(t, []byte(test.conf))
			if _, err := ProcessConfigFile(conf); err == nil ||
				!strings.Contains(err.Error(), "error parsing dial_timeout") {
				t.Fatalf("Expected error parsing dial_timeout, got %v", err)
			}
		})
	}
}

// A shadow subscription created by a stream import delivers the import's local
// (post-transform) subject to the leafnode; the pre-transform subject exists
// only in the exporting account and is never sent on the wire. The leafnode's
// publish permissions must therefore be evaluated against the post-transform
// subject.
func TestLeafNodePermsWithImportSubjectTransform(t *testing.T) {
	for _, test := range []struct {
		name      string
		pub       string
		delivered bool
	}{
		{"post transform form", `"leaf.>"`, true},
		{"pre transform form", `"data.>"`, false},
		{"unrelated form", `"other.>"`, false},
	} {
		t.Run(test.name, func(t *testing.T) {
			hubConf := createConfFile(t, []byte(fmt.Sprintf(`
				listen: 127.0.0.1:-1
				accounts {
				  sync_account: {
				    users = [
				      {user: "sync", password: "pass"}
				      {user: "leaf-user", password: "pass", permissions: {
				        publish = [%s]
				      }}
				    ]
				  }
				}
				leafnodes { listen: 127.0.0.1:-1 }
			`, test.pub)))
			hub, hubOpts := RunServerWithConfig(hubConf)
			defer hub.Shutdown()

			leafConf := createConfFile(t, []byte(fmt.Sprintf(`
				listen: 127.0.0.1:-1
				accounts {
				  local_account: {
				    exports = [ { stream: "data.>" } ]
				    users = [ {user: "local", password: "pass"} ]
				  }
				  sync_account: {
				    imports = [
				      { stream: { account: local_account, subject: "data.>" }, to: "leaf.data.>" }
				    ]
				  }
				}
				leafnodes {
				  remotes = [ { url: "nats://leaf-user:pass@127.0.0.1:%d", account: "sync_account" } ]
				}
			`, hubOpts.LeafNode.Port)))
			leaf, _ := RunServerWithConfig(leafConf)
			defer leaf.Shutdown()

			checkLeafNodeConnected(t, leaf)

			ncHub := natsConnect(t, hub.ClientURL(), nats.UserInfo("sync", "pass"))
			defer ncHub.Close()
			sub := natsSubSync(t, ncHub, "leaf.>")
			natsFlush(t, ncHub)

			if test.delivered {
				// The interest has to reach the exporting account on the leaf side.
				checkSubInterest(t, leaf, "local_account", "data.foo", 2*time.Second)
			} else {
				// The hub may not even propagate the interest; give it a moment
				// either way, the assertion below is that nothing is delivered.
				time.Sleep(300 * time.Millisecond)
			}

			// Publish in the exporting account, on the pre-transform subject.
			ncLeaf := natsConnect(t, leaf.ClientURL(), nats.UserInfo("local", "pass"))
			defer ncLeaf.Close()
			natsPub(t, ncLeaf, "data.foo", []byte("hello"))
			natsFlush(t, ncLeaf)

			if test.delivered {
				msg := natsNexMsg(t, sub, 2*time.Second)
				require_Equal(t, msg.Subject, "leaf.data.foo")
			} else {
				if msg, err := sub.NextMsg(500 * time.Millisecond); err == nil {
					t.Fatalf("Should not have received %q", msg.Subject)
				}
			}
		})
	}
}

// Same as above, but the message is published on a gateway routed subject
// (_GR_...). The header path transforms the complete subject, so the gateway
// prefix ends up inside the delivered subject. The permission check has to be
// done on that form and not on the one obtained by stripping the prefix first,
// otherwise the leafnode authorizes a subject that differs from the one it
// puts on the wire.
func TestLeafNodePermsWithImportSubjectTransformGatewayRouted(t *testing.T) {
	// Cluster hash and server hash are gwHashLen characters each, see the
	// replyPfx built in newGateway().
	gwPrefix := gwReplyPrefix + strings.Repeat("A", gwHashLen) + "." +
		strings.Repeat("B", gwHashLen) + "."
	// The import prepends "leaf." to the complete subject, so this is what the
	// leafnode puts on the wire.
	wireSubject := "leaf." + gwPrefix + "data.foo"
	// Stripping the gateway prefix before applying the transform would yield
	// this unrelated subject instead.
	strippedSubject := "leaf.data.foo"

	for _, test := range []struct {
		name      string
		pub       string
		delivered bool
	}{
		{"wire form", wireSubject, true},
		{"stripped form", strippedSubject, false},
	} {
		t.Run(test.name, func(t *testing.T) {
			hubConf := createConfFile(t, fmt.Appendf(nil, `
				listen: 127.0.0.1:-1
				accounts {
				  sync_account: {
				    users = [
				      {user: "sync", password: "pass"}
				      {user: "leaf-user", password: "pass", permissions: {
				        publish = [%q]
				      }}
				    ]
				  }
				}
				leafnodes { listen: 127.0.0.1:-1 }
			`, test.pub))
			hub, hubOpts := RunServerWithConfig(hubConf)
			defer hub.Shutdown()

			// Export and import cover ">" so that the gateway routed subject is
			// matched as well.
			leafConf := createConfFile(t, fmt.Appendf(nil, `
				listen: 127.0.0.1:-1
				accounts {
				  local_account: {
				    exports = [ { stream: ">" } ]
				    users = [ {user: "origin", password: "pass"} ]
				  }
				  sync_account: {
				    imports = [
				      { stream: { account: local_account, subject: ">" }, to: "leaf.>" }
				    ]
				  }
				}
				leafnodes {
				  listen: 127.0.0.1:-1
				  remotes = [ { url: "nats://leaf-user:pass@127.0.0.1:%d", account: "sync_account" } ]
				}
			`, hubOpts.LeafNode.Port))
			leaf, leafOpts := RunServerWithConfig(leafConf)
			defer leaf.Shutdown()

			checkLeafNodeConnected(t, leaf)

			ncHub := natsConnect(t, hub.ClientURL(), nats.UserInfo("sync", "pass"))
			defer ncHub.Close()
			sub := natsSubSync(t, ncHub, "leaf.>")
			natsFlush(t, ncHub)

			// The hub forwards the wildcard interest to the leafnode for both
			// permission sets, see the ReverseMatch in canSubscribeInternal(),
			// and relies on deliverMsg() to prune what may not be published.
			// The shadow subscription is on ">", so any subject matches.
			checkSubInterest(t, leaf, "local_account", "data.foo", 2*time.Second)

			// A client is not allowed to publish on the gateway prefix, so
			// inject the message over a leafnode connection instead.
			originConn, err := net.DialTimeout("tcp",
				net.JoinHostPort(leafOpts.LeafNode.Host, fmt.Sprintf("%d", leafOpts.LeafNode.Port)),
				2*time.Second)
			require_NoError(t, err)
			defer originConn.Close()
			if _, err := bufio.NewReader(originConn).ReadString('\n'); err != nil {
				t.Fatalf("Error reading INFO: %v", err)
			}
			_, err = originConn.Write([]byte(
				"CONNECT {\"name\":\"origin\",\"user\":\"origin\",\"pass\":\"pass\"}\r\nPING\r\n"))
			require_NoError(t, err)
			checkLeafNodeConnectedCount(t, leaf, 2)

			// Messages the leafnode has put on the wire towards the hub, and
			// whether that connection still exists. Both are needed below: a
			// subject the leafnode may not publish makes the hub log a publish
			// violation and close the connection, see leafPermViolation().
			hubBoundMsgs := func() (int64, bool) {
				var lns []*client
				leaf.mu.Lock()
				for _, ln := range leaf.leafs {
					lns = append(lns, ln)
				}
				leaf.mu.Unlock()
				for _, ln := range lns {
					ln.mu.Lock()
					solicited, msgs := ln.isSolicitedLeafNode(), ln.outMsgs
					ln.mu.Unlock()
					if solicited {
						return msgs, true
					}
				}
				return 0, false
			}
			before, ok := hubBoundMsgs()
			require_True(t, ok)

			_, err = fmt.Fprintf(originConn, "LMSG %s %d\r\n%s\r\n",
				gwPrefix+"data.foo", len("hello"), "hello")
			require_NoError(t, err)

			if test.delivered {
				msg := natsNexMsg(t, sub, 2*time.Second)
				require_Equal(t, msg.Subject, wireSubject)
			} else {
				if msg, err := sub.NextMsg(500 * time.Millisecond); err == nil {
					t.Fatalf("Should not have received %q", msg.Subject)
				}
				// The message must be held back by the leafnode itself, not by
				// the hub, which would tear the connection down.
				after, ok := hubBoundMsgs()
				if !ok {
					t.Fatal("Hub closed the leafnode connection, it rejected the subject the leafnode sent")
				}
				require_Equal(t, after, before)
			}
		})
	}
}

func TestLeafNodeDenyAccountFunctions(t *testing.T) {
	hconf := createConfFile(t, []byte(`
		listen: 127.0.0.1:-1
		accounts { HUB { users [ {user: hub, password: pwd} ] } }
		leafnodes { listen: 127.0.0.1:-1 }
	`))
	hub, hopts := RunServerWithConfig(hconf)
	defer hub.Shutdown()

	sconf := createConfFile(t, []byte(fmt.Sprintf(`
		listen: 127.0.0.1:-1
		accounts { SPOKE { users [ {user: spoke, password: pwd} ] } }
		leafnodes {
			remotes [ {
				url: "nats://hub:pwd@127.0.0.1:%d"
				account: SPOKE
				deny_exports: [ "export.{{account-hash()}}", "export.{{account-hash-leaf-remote()}}" ]
				deny_imports: [ "import.{{account-hash()}}", "import.{{account-hash-leaf-remote()}}" ]
			} ]
		}
	`, hopts.LeafNode.Port)))
	spoke, _ := RunServerWithConfig(sconf)
	defer spoke.Shutdown()

	checkLeafNodeConnected(t, hub)
	checkLeafNodeConnected(t, spoke)

	hubHash, spokeHash := getHash("HUB"), getHash("SPOKE")

	leafClient := func(s *Server) *client {
		t.Helper()
		s.mu.RLock()
		defer s.mu.RUnlock()
		for _, c := range s.leafs {
			return c
		}
		t.Fatalf("No leafnode connection on %s", s.Name())
		return nil
	}
	denies := func(c *client, pub bool, subject string) bool {
		c.mu.Lock()
		defer c.mu.Unlock()
		if c.perms == nil {
			return false
		}
		sl := c.perms.sub.deny
		if pub {
			sl = c.perms.pub.deny
		}
		return sl != nil && sl.HasInterest(subject)
	}

	// The spoke resolves its own account right away and the hub's account once it is known.
	sc := leafClient(spoke)
	checkFor(t, 2*time.Second, 25*time.Millisecond, func() error {
		if !denies(sc, true, "export."+hubHash) {
			return fmt.Errorf("deny_exports not yet resolved with the remote account")
		}
		return nil
	})
	require_True(t, denies(sc, true, "export."+spokeHash))
	require_True(t, denies(sc, false, "import."+spokeHash))
	require_True(t, denies(sc, false, "import."+hubHash))
	require_False(t, denies(sc, true, "export.{{account-hash()}}"))

	// The hub receives the spoke's deny_imports and resolves them from the spoke's perspective.
	hc := leafClient(hub)
	checkFor(t, 2*time.Second, 25*time.Millisecond, func() error {
		if !denies(hc, true, "import."+spokeHash) || !denies(hc, true, "import."+hubHash) {
			return fmt.Errorf("deny_imports not yet resolved on the hub")
		}
		return nil
	})

	// Message flow: exports denied by the spoke never reach the hub.
	ncHub := natsConnect(t, hub.ClientURL(), nats.UserInfo("hub", "pwd"))
	defer ncHub.Close()
	ncSpoke := natsConnect(t, spoke.ClientURL(), nats.UserInfo("spoke", "pwd"))
	defer ncSpoke.Close()

	hubSub := natsSubSync(t, ncHub, "export.>")
	natsFlush(t, ncHub)
	checkSubInterest(t, spoke, "SPOKE", "export.>", time.Second)
	natsPub(t, ncSpoke, "export."+spokeHash, []byte("denied"))
	natsPub(t, ncSpoke, "export."+hubHash, []byte("denied"))
	natsPub(t, ncSpoke, "export.other", []byte("ok"))
	m := natsNexMsg(t, hubSub, time.Second)
	require_Equal(t, m.Subject, "export.other")
	if m, err := hubSub.NextMsg(100 * time.Millisecond); err == nil {
		t.Fatalf("Did not expect to receive %q", m.Subject)
	}

	// And denied imports never reach the spoke.
	spokeSub := natsSubSync(t, ncSpoke, "import.>")
	natsFlush(t, ncSpoke)
	checkSubInterest(t, hub, "HUB", "import.>", time.Second)
	natsPub(t, ncHub, "import."+spokeHash, []byte("denied"))
	natsPub(t, ncHub, "import."+hubHash, []byte("denied"))
	natsPub(t, ncHub, "import.other", []byte("ok"))
	m = natsNexMsg(t, spokeSub, time.Second)
	require_Equal(t, m.Subject, "import.other")
	if m, err := spokeSub.NextMsg(100 * time.Millisecond); err == nil {
		t.Fatalf("Did not expect to receive %q", m.Subject)
	}
}

func TestLeafNodeDenyAccountFunctionsConfigErrors(t *testing.T) {
	conf := createConfFile(t, []byte(`
		listen: 127.0.0.1:-1
		leafnodes {
			remotes [ { url: "nats://127.0.0.1:1234", deny_imports: [ "x.{{account-hash(a,b)}}" ] } ]
		}
	`))
	opts, err := ProcessConfigFile(conf)
	require_NoError(t, err)
	require_Error(t, validateLeafNode(opts), ErrMappingDestinationTooManyArgs)

	// The import function never applies to a leafnode deny list, while the leaf remote
	// function is only resolved once connected and passes validation.
	conf = createConfFile(t, []byte(`
		listen: 127.0.0.1:-1
		leafnodes {
			remotes [ { url: "nats://127.0.0.1:1234", deny_exports: [ "x.{{account-hash-leaf-remote()}}", "x.{{account-hash-import()}}" ] } ]
		}
	`))
	opts, err = ProcessConfigFile(conf)
	require_NoError(t, err)
	err = validateLeafNode(opts)
	require_Error(t, err, ErrMappingFunctionNotAvailable)
	require_Contains(t, err.Error(), "{{account-hash-import()}}")
}

func TestLeafNodeRemoteDenyPermissionsAccountFunctions(t *testing.T) {
	s := RunServer(DefaultOptions())
	defer s.Shutdown()
	l := &captureErrorLogger{errCh: make(chan string, 1)}
	s.SetLogger(l, false, false)

	perms := &Permissions{Publish: &SubjectPermission{Deny: []string{"deny.{{account-hash-leaf-remote()}}", "deny.{{account-hash()}}"}}}
	hash := getHash(s.globalAccount().Name)

	// Accepted (hub side) connection: these are the permissions of the leafnode user, in which
	// account scoped mapping functions are not resolved.
	c := &client{srv: s, kind: LEAF, leaf: &leaf{}, acc: s.globalAccount()}
	c.mu.Lock()
	c.setPermissions(perms)
	require_False(t, c.perms.pub.deny.HasInterest("deny."+hash))
	require_True(t, c.perms.pub.deny.HasInterest("deny.{{account-hash()}}"))
	require_True(t, c.perms.pub.deny.HasInterest("deny.{{account-hash-leaf-remote()}}"))
	c.mu.Unlock()

	// Solicited (spoke side) connection: the deny lists of the remote configuration are resolved,
	// the remote account one being deferred until the hub's INFO, without error.
	c = &client{srv: s, kind: LEAF, leaf: &leaf{remote: &leafNodeCfg{perms: perms}}, acc: s.globalAccount()}
	c.mu.Lock()
	c.setPermissions(c.leafRemoteDenyPermissions())
	c.mu.Unlock()
	select {
	case e := <-l.errCh:
		t.Fatalf("Unexpected error: %s", e)
	case <-time.After(100 * time.Millisecond):
	}
	c.mu.Lock()
	require_True(t, c.perms.pub.deny.HasInterest("deny."+hash))
	require_True(t, c.perms.pub.deny.HasInterest("deny.{{account-hash-leaf-remote()}}"))
	// Once the remote account is known, applying the permissions again resolves it.
	c.leaf.remoteAccName = "REMOTE"
	c.setPermissions(c.leafRemoteDenyPermissions())
	require_True(t, c.perms.pub.deny.HasInterest("deny."+getHash("REMOTE")))
	require_False(t, c.perms.pub.deny.HasInterest("deny.{{account-hash-leaf-remote()}}"))
	c.mu.Unlock()

	// The remote configuration is left untouched.
	require_Equal(t, perms.Publish.Deny[0], "deny.{{account-hash-leaf-remote()}}")
	require_Equal(t, perms.Publish.Deny[1], "deny.{{account-hash()}}")
}

func TestLeafNodeAccountLeafListReleasesClosedConnections(t *testing.T) {
	o := DefaultOptions()
	o.LeafNode.Host = "127.0.0.1"
	o.LeafNode.Port = -1
	hub := RunServer(o)
	defer hub.Shutdown()

	acc := hub.globalAccount()
	checkLeafList := func(n int) {
		t.Helper()
		checkFor(t, 5*time.Second, 15*time.Millisecond, func() error {
			acc.lmu.RLock()
			defer acc.lmu.RUnlock()
			if l := len(acc.lleafs); l != n {
				return fmt.Errorf("expected %d leafnodes, got %d", n, l)
			}
			return nil
		})
	}

	u := &url.URL{Scheme: "nats", Host: fmt.Sprintf("127.0.0.1:%d", o.LeafNode.Port)}
	var leafs []*Server
	for i := 0; i < 4; i++ {
		lo := DefaultOptions()
		lo.Cluster.Name = "edge"
		lo.LeafNode.Remotes = []*RemoteLeafOpts{{URLs: []*url.URL{u}}}
		l := RunServer(lo)
		defer l.Shutdown()
		leafs = append(leafs, l)
		checkLeafList(i + 1)
	}

	leafs[0].Shutdown()
	leafs[0].WaitForShutdown()
	checkLeafList(3)

	leafs[3].Shutdown()
	leafs[3].WaitForShutdown()
	checkLeafList(2)

	acc.lmu.RLock()
	defer acc.lmu.RUnlock()
	for _, c := range acc.lleafs[len(acc.lleafs):cap(acc.lleafs)] {
		require_True(t, c == nil)
	}
}

func TestLeafNodeWSLeafzReportsWebsocket(t *testing.T) {
	check := func(t *testing.T, s *Server, want bool) {
		t.Helper()
		lz, err := s.Leafz(nil)
		require_NoError(t, err)
		require_Equal(t, len(lz.Leafs), 1)
		require_Equal(t, lz.Leafs[0].Websocket, want)
	}

	o := testDefaultLeafNodeWSOptions()
	s := RunServer(o)
	defer s.Shutdown()
	lo := testDefaultRemoteLeafNodeWSOptions(t, o, false)
	ln := RunServer(lo)
	defer ln.Shutdown()
	checkLeafNodeConnected(t, s)
	checkLeafNodeConnected(t, ln)
	// Both the accepting and the soliciting side know the link is websocket.
	check(t, s, true)
	check(t, ln, true)

	o2 := DefaultOptions()
	o2.LeafNode.Host = "127.0.0.1"
	o2.LeafNode.Port = -1
	s2 := RunServer(o2)
	defer s2.Shutdown()
	u, _ := url.Parse(fmt.Sprintf("nats://127.0.0.1:%d", s2.getOpts().LeafNode.Port))
	lo2 := DefaultOptions()
	lo2.Cluster.Name = "LN"
	lo2.LeafNode.Remotes = []*RemoteLeafOpts{{URLs: []*url.URL{u}}}
	ln2 := RunServer(lo2)
	defer ln2.Shutdown()
	checkLeafNodeConnected(t, s2)
	checkLeafNodeConnected(t, ln2)
	check(t, s2, false)
	check(t, ln2, false)
}

// A leaf with solicited remotes and no cluster block takes its cluster name from server_name, and the hub tags
// subscriptions from its links with that origin. The shadow subscription for an import must not keep it, or the hub
// never sends the derived interest down the leaf's other links, which share the cluster name.
func TestLeafNodeImportedInterestOnSameNamedLeaf(t *testing.T) {
	hubConf := createConfFile(t, []byte(`
		port: -1
		leafnodes { port: -1 }
		accounts: {
			TEN: {
				users: [{user: ten, password: ten}]
				exports: [{stream: "ten.>", accounts: [SS]}]
			}
			SS: {
				users: [{user: ss, password: ss}]
				imports: [{stream: {account: TEN, subject: "ten.>"}}]
			}
		}
	`))
	hub := RunServer(LoadConfig(hubConf))
	defer hub.Shutdown()

	leafConf := createConfFile(t, []byte(fmt.Sprintf(`
		server_name: leaf
		port: -1
		leafnodes {
			remotes: [
				{url: "nats-leaf://ten:ten@127.0.0.1:%[1]d", account: TEN}
				{url: "nats-leaf://ss:ss@127.0.0.1:%[1]d", account: SS}
			]
		}
		accounts: {
			TEN: {users: [{user: ten, password: ten}]}
			SS: {users: [{user: ss, password: ss}]}
		}
	`, hub.getOpts().LeafNode.Port)))
	leaf := RunServer(LoadConfig(leafConf))
	defer leaf.Shutdown()

	checkLeafNodeConnectedCount(t, hub, 2)

	// An SS subscriber on the same leaf, for a subject TEN exports to SS.
	nc := natsConnect(t, leaf.ClientURL(), nats.UserInfo("ss", "ss"))
	defer nc.Close()
	if _, err := nc.SubscribeSync("ten.event"); err != nil {
		t.Fatalf("Error subscribing: %v", err)
	}
	natsFlush(t, nc)

	// The leaf's TEN account needs the interest the hub derives from that subscription.
	acc, err := leaf.LookupAccount("TEN")
	if err != nil {
		t.Fatalf("Error looking up TEN on the leaf: %v", err)
	}
	checkFor(t, 2*time.Second, 15*time.Millisecond, func() error {
		if acc.Interest("ten.event") == 0 {
			return fmt.Errorf("leaf account TEN has no interest for %q", "ten.event")
		}
		return nil
	})
}

// Control for the test above: with the subscriber on a leaf that has a different cluster name, the derived interest does
// reach the other leaf's TEN link.
func TestLeafNodeImportedInterestOnOtherNamedLeaf(t *testing.T) {
	hubConf := createConfFile(t, []byte(`
		port: -1
		leafnodes { port: -1 }
		accounts: {
			TEN: {
				users: [{user: ten, password: ten}]
				exports: [{stream: "ten.>", accounts: [SS]}]
			}
			SS: {
				users: [{user: ss, password: ss}]
				imports: [{stream: {account: TEN, subject: "ten.>"}}]
			}
		}
	`))
	hub := RunServer(LoadConfig(hubConf))
	defer hub.Shutdown()

	runLeaf := func(name string) *Server {
		t.Helper()
		conf := createConfFile(t, []byte(fmt.Sprintf(`
			server_name: %s
			port: -1
			leafnodes {
				remotes: [
					{url: "nats-leaf://ten:ten@127.0.0.1:%[2]d", account: TEN}
					{url: "nats-leaf://ss:ss@127.0.0.1:%[2]d", account: SS}
				]
			}
			accounts: {
				TEN: {users: [{user: ten, password: ten}]}
				SS: {users: [{user: ss, password: ss}]}
			}
		`, name, hub.getOpts().LeafNode.Port)))
		return RunServer(LoadConfig(conf))
	}
	leafA, leafB := runLeaf("leafa"), runLeaf("leafb")
	defer leafA.Shutdown()
	defer leafB.Shutdown()

	checkLeafNodeConnectedCount(t, hub, 4)

	nc := natsConnect(t, leafA.ClientURL(), nats.UserInfo("ss", "ss"))
	defer nc.Close()
	if _, err := nc.SubscribeSync("ten.event"); err != nil {
		t.Fatalf("Error subscribing: %v", err)
	}
	natsFlush(t, nc)

	acc, err := leafB.LookupAccount("TEN")
	if err != nil {
		t.Fatalf("Error looking up TEN on the other leaf: %v", err)
	}
	checkFor(t, 2*time.Second, 15*time.Millisecond, func() error {
		if acc.Interest("ten.event") == 0 {
			return fmt.Errorf("other leaf account TEN has no interest for %q", "ten.event")
		}
		return nil
	})
}

// Request and reply across an account import with the responder on the same named leaf as the requester. The request
// reaches the responder through a stream import and the reply comes back through a service import, and both must be
// delivered although the leaf links of both accounts belong to the same leaf cluster.
func TestLeafNodeImportedRequestReplyOnSameNamedLeaf(t *testing.T) {
	for _, queue := range []bool{false, true} {
		t.Run(fmt.Sprintf("queue=%v", queue), func(t *testing.T) {
			hubConf := createConfFile(t, []byte(`
				port: -1
				leafnodes { port: -1 }
				accounts: {
					TEN: {
						users: [{user: ten, password: ten}]
						exports: [
							{stream: "ten.>", accounts: [SS]}
							{service: "REPLY.ten.>", accounts: [SS]}
						]
					}
					SS: {
						users: [{user: ss, password: ss}]
						imports: [
							{stream: {account: TEN, subject: "ten.>"}}
							{service: {account: TEN, subject: "REPLY.ten.>"}}
						]
					}
				}
			`))
			hub := RunServer(LoadConfig(hubConf))
			defer hub.Shutdown()

			leafConf := createConfFile(t, []byte(fmt.Sprintf(`
				server_name: leaf
				port: -1
				leafnodes {
					remotes: [
						{url: "nats-leaf://ten:ten@127.0.0.1:%[1]d", account: TEN}
						{url: "nats-leaf://ss:ss@127.0.0.1:%[1]d", account: SS}
					]
				}
				accounts: {
					TEN: {users: [{user: ten, password: ten}]}
					SS: {users: [{user: ss, password: ss}]}
				}
			`, hub.getOpts().LeafNode.Port)))
			leaf := RunServer(LoadConfig(leafConf))
			defer leaf.Shutdown()

			checkLeafNodeConnectedCount(t, hub, 2)

			responder := natsConnect(t, leaf.ClientURL(), nats.UserInfo("ss", "ss"))
			defer responder.Close()
			respond := func(m *nats.Msg) { m.Respond([]byte("pong")) }
			var err error
			if queue {
				_, err = responder.QueueSubscribe("ten.event", "workers", respond)
			} else {
				_, err = responder.Subscribe("ten.event", respond)
			}
			if err != nil {
				t.Fatalf("Error subscribing: %v", err)
			}
			natsFlush(t, responder)

			acc, err := leaf.LookupAccount("TEN")
			if err != nil {
				t.Fatalf("Error looking up TEN on the leaf: %v", err)
			}
			checkFor(t, 2*time.Second, 15*time.Millisecond, func() error {
				if acc.Interest("ten.event") == 0 {
					return fmt.Errorf("leaf account TEN has no interest for %q", "ten.event")
				}
				return nil
			})

			requester := natsConnect(t, leaf.ClientURL(), nats.UserInfo("ten", "ten"), nats.CustomInboxPrefix("REPLY.ten"))
			defer requester.Close()
			resp, err := requester.Request("ten.event", []byte("ping"), 2*time.Second)
			if err != nil {
				t.Fatalf("Request failed: %v", err)
			}
			if string(resp.Data) != "pong" {
				t.Fatalf("Unexpected response %q", resp.Data)
			}
		})
	}
}

// leafImportCycleAccounts defines TEN and SS, where one shares subject with the other as a stream or service.
func leafImportCycleAccounts(kind, subject string, tenExports bool) string {
	export := func(to string) string {
		return fmt.Sprintf("exports: [{%s: %q, accounts: [%s]}]", kind, subject, to)
	}
	imprt := func(from string) string {
		return fmt.Sprintf("imports: [{%s: {account: %s, subject: %q}}]", kind, from, subject)
	}
	ten, ss := export("SS"), imprt("TEN")
	if !tenExports {
		ten, ss = imprt("SS"), export("TEN")
	}
	return fmt.Sprintf(`
		accounts: {
			TEN: {
				users: [{user: ten, password: ten}]
				%s
			}
			SS: {
				users: [{user: ss, password: ss}]
				%s
			}
		}
	`, ten, ss)
}

// runLeafImportCycleLeaf starts a leaf whose SS->TEN import reverses the hub's TEN->SS import.
func runLeafImportCycleLeaf(t *testing.T, kind, subject string, tenPort, ssPort int) *Server {
	t.Helper()
	conf := createConfFile(t, []byte(fmt.Sprintf(`
		server_name: leaf
		port: -1
		leafnodes {
			remotes: [
				{url: "nats-leaf://ten:ten@127.0.0.1:%d", account: TEN}
				{url: "nats-leaf://ss:ss@127.0.0.1:%d", account: SS}
			]
		}
		%s
	`, tenPort, ssPort, leafImportCycleAccounts(kind, subject, false))))
	return RunServer(LoadConfig(conf))
}

func checkAccountLeafInterest(t *testing.T, s *Server, accName, subject string) {
	t.Helper()
	acc, err := s.LookupAccount(accName)
	if err != nil {
		t.Fatalf("Error looking up %s on %s: %v", accName, s.Name(), err)
	}
	checkFor(t, 2*time.Second, 15*time.Millisecond, func() error {
		for _, sub := range acc.sl.Match(subject).psubs {
			if sub.client.kind == LEAF {
				return nil
			}
		}
		return fmt.Errorf("%s account %s has no leafnode interest for %q", s.Name(), accName, subject)
	})
}

func checkLeafImportCycleDoesNotLoop(t *testing.T, leaf *Server, accName, subject string) {
	t.Helper()
	nc := natsConnect(t, leaf.ClientURL(), nats.UserInfo("ss", "ss"))
	defer nc.Close()
	sub := natsSubSync(t, nc, subject)
	natsFlush(t, nc)

	checkAccountLeafInterest(t, leaf, accName, subject)

	natsPub(t, nc, subject, []byte("hello"))
	natsFlush(t, nc)

	// The first copy is local; the second has been through the hub's import, which proves the hub forwarded it.
	natsNexMsg(t, sub, time.Second)
	if _, err := sub.NextMsg(time.Second); err != nil {
		t.Fatalf("Message on %q did not come back through the hub: %v", subject, err)
	}
	for n := 2; ; n++ {
		if _, err := sub.NextMsg(250 * time.Millisecond); err != nil {
			return
		}
		if n == 10 {
			t.Fatalf("Message on %q is looping between the hub and the leaf", subject)
		}
	}
}

// Each server accepts its own imports, but together the hub's TEN->SS and the leaf's SS->TEN imports form a cycle.
func TestLeafNodeImportCycleAcrossHubAndLeafDoesNotLoop(t *testing.T) {
	hub := RunServer(LoadConfig(createConfFile(t, []byte(`
		port: -1
		leafnodes { port: -1 }
	`+leafImportCycleAccounts("stream", "ten.>", true)))))
	defer hub.Shutdown()

	port := hub.getOpts().LeafNode.Port
	leaf := runLeafImportCycleLeaf(t, "stream", "ten.>", port, port)
	defer leaf.Shutdown()

	checkLeafNodeConnectedCount(t, hub, 2)

	checkLeafImportCycleDoesNotLoop(t, leaf, "TEN", "ten.event")
}

func TestLeafNodeServiceImportCycleAcrossHubAndLeafDoesNotLoop(t *testing.T) {
	hub := RunServer(LoadConfig(createConfFile(t, []byte(`
		port: -1
		leafnodes { port: -1 }
	`+leafImportCycleAccounts("service", "svc.>", true)))))
	defer hub.Shutdown()

	port := hub.getOpts().LeafNode.Port
	leaf := runLeafImportCycleLeaf(t, "service", "svc.>", port, port)
	defer leaf.Shutdown()

	checkLeafNodeConnectedCount(t, hub, 2)

	checkLeafImportCycleDoesNotLoop(t, leaf, "SS", "svc.x")
}

// The leaf's links reach different servers of a named hub cluster, so only the cluster name identifies the hub.
func TestLeafNodeImportCycleAcrossHubClusterAndLeafDoesNotLoop(t *testing.T) {
	hubTmpl := `
		server_name: %s
		listen: 127.0.0.1:-1
		leafnodes { listen: 127.0.0.1:-1 }
		cluster {
			name: hub
			listen: 127.0.0.1:-1
			%s
		}
	` + leafImportCycleAccounts("stream", "ten.>", true)
	hub1 := RunServer(LoadConfig(createConfFile(t, []byte(fmt.Sprintf(hubTmpl, "hub1", _EMPTY_)))))
	defer hub1.Shutdown()
	routes := fmt.Sprintf("routes: [nats://127.0.0.1:%d]", hub1.ClusterAddr().Port)
	hub2 := RunServer(LoadConfig(createConfFile(t, []byte(fmt.Sprintf(hubTmpl, "hub2", routes)))))
	defer hub2.Shutdown()
	checkClusterFormed(t, hub1, hub2)

	leaf := runLeafImportCycleLeaf(t, "stream", "ten.>", hub1.getOpts().LeafNode.Port, hub2.getOpts().LeafNode.Port)
	defer leaf.Shutdown()

	checkLeafNodeConnectedCount(t, hub1, 1)
	checkLeafNodeConnectedCount(t, hub2, 1)

	checkLeafImportCycleDoesNotLoop(t, leaf, "TEN", "ten.event")
}

// Unnamed hub servers advertise no cluster name to leafs, even once clustered, so only the empty-name rule
// identifies the hub.
func TestLeafNodeImportCycleAcrossDynamicHubClusterAndLeafDoesNotLoop(t *testing.T) {
	hubTmpl := `
		server_name: %s
		listen: 127.0.0.1:-1
		leafnodes { listen: 127.0.0.1:-1 }
		cluster {
			listen: 127.0.0.1:-1
			%s
		}
	` + leafImportCycleAccounts("stream", "ten.>", true)
	hub1 := RunServer(LoadConfig(createConfFile(t, []byte(fmt.Sprintf(hubTmpl, "hub1", _EMPTY_)))))
	defer hub1.Shutdown()
	hub2Conf := createConfFile(t, []byte(fmt.Sprintf(hubTmpl, "hub2", _EMPTY_)))
	hub2 := RunServer(LoadConfig(hub2Conf))
	defer hub2.Shutdown()

	leaf := runLeafImportCycleLeaf(t, "stream", "ten.>", hub1.getOpts().LeafNode.Port, hub2.getOpts().LeafNode.Port)
	defer leaf.Shutdown()

	checkLeafNodeConnectedCount(t, hub1, 1)
	checkLeafNodeConnectedCount(t, hub2, 1)

	routes := fmt.Sprintf("routes: [nats://127.0.0.1:%d]", hub1.ClusterAddr().Port)
	reloadUpdateConfig(t, hub2, hub2Conf, fmt.Sprintf(hubTmpl, "hub2", routes))
	checkClusterFormed(t, hub1, hub2)

	leaf.mu.Lock()
	for _, l := range leaf.leafs {
		l.mu.Lock()
		rc, rs := l.leaf.remoteCluster, l.leaf.remoteServer
		l.mu.Unlock()
		if rc != _EMPTY_ {
			leaf.mu.Unlock()
			t.Fatalf("Expected the link to %s to have no hub cluster name, got %q", rs, rc)
		}
	}
	leaf.mu.Unlock()

	checkLeafImportCycleDoesNotLoop(t, leaf, "TEN", "ten.event")
}

// The leaf's links reach two different named hubs, so a local import may still bridge them.
func TestLeafNodeServiceImportBridgesDistinctHubs(t *testing.T) {
	hubTmpl := `
		server_name: %[1]s
		listen: 127.0.0.1:-1
		leafnodes { listen: 127.0.0.1:-1 }
		cluster {
			name: %[1]s
			listen: 127.0.0.1:-1
		}
		accounts: {
			%[2]s: { users: [{user: %[3]s, password: %[3]s}] }
		}
	`
	hubA := RunServer(LoadConfig(createConfFile(t, []byte(fmt.Sprintf(hubTmpl, "hubA", "TEN", "ten")))))
	defer hubA.Shutdown()
	hubB := RunServer(LoadConfig(createConfFile(t, []byte(fmt.Sprintf(hubTmpl, "hubB", "SS", "ss")))))
	defer hubB.Shutdown()

	leafConf := createConfFile(t, []byte(fmt.Sprintf(`
		server_name: leaf
		port: -1
		leafnodes {
			remotes: [
				{url: "nats-leaf://ten:ten@127.0.0.1:%d", account: TEN}
				{url: "nats-leaf://ss:ss@127.0.0.1:%d", account: SS}
			]
		}
		accounts: {
			TEN: {
				users: [{user: ten, password: ten}]
				imports: [{service: {account: SS, subject: "svc.>"}}]
			}
			SS: {
				users: [{user: ss, password: ss}]
				exports: [{service: "svc.>", accounts: [TEN]}]
			}
		}
	`, hubA.getOpts().LeafNode.Port, hubB.getOpts().LeafNode.Port)))
	leaf := RunServer(LoadConfig(leafConf))
	defer leaf.Shutdown()

	checkLeafNodeConnectedCount(t, hubA, 1)
	checkLeafNodeConnectedCount(t, hubB, 1)

	nc := natsConnect(t, hubB.ClientURL(), nats.UserInfo("ss", "ss"))
	defer nc.Close()
	natsSub(t, nc, "svc.x", func(m *nats.Msg) { m.Respond([]byte("ok")) })
	natsFlush(t, nc)

	checkAccountLeafInterest(t, hubA, "TEN", "svc.x")
	checkAccountLeafInterest(t, leaf, "SS", "svc.x")

	ncA := natsConnect(t, hubA.ClientURL(), nats.UserInfo("ten", "ten"))
	defer ncA.Close()
	resp, err := ncA.Request("svc.x", []byte("hello"), 2*time.Second)
	if err != nil {
		t.Fatalf("Request failed: %v", err)
	}
	if string(resp.Data) != "ok" {
		t.Fatalf("Unexpected response %q", resp.Data)
	}
}

// The leaf's TEN and SS links land on different servers of the hub cluster, so interest and messages for the imports
// cross the hub's route.
func TestLeafNodeImportedRequestReplyAcrossHubCluster(t *testing.T) {
	for _, queue := range []bool{false, true} {
		t.Run(fmt.Sprintf("queue=%v", queue), func(t *testing.T) {
			hubTmpl := `
				server_name: %s
				listen: 127.0.0.1:-1
				leafnodes { listen: 127.0.0.1:-1 }
				cluster {
					name: hub
					listen: 127.0.0.1:%d
					%s
				}
				accounts: {
					TEN: {
						users: [{user: ten, password: ten}]
						exports: [
							{stream: "ten.>", accounts: [SS]}
							{service: "REPLY.ten.>", accounts: [SS]}
						]
					}
					SS: {
						users: [{user: ss, password: ss}]
						imports: [
							{stream: {account: TEN, subject: "ten.>"}}
							{service: {account: TEN, subject: "REPLY.ten.>"}}
						]
					}
				}
			`
			hub1 := RunServer(LoadConfig(createConfFile(t, []byte(fmt.Sprintf(hubTmpl, "hub1", -1, _EMPTY_)))))
			defer hub1.Shutdown()
			routes := fmt.Sprintf("routes: [nats://127.0.0.1:%d]", hub1.ClusterAddr().Port)
			hub2 := RunServer(LoadConfig(createConfFile(t, []byte(fmt.Sprintf(hubTmpl, "hub2", -1, routes)))))
			defer hub2.Shutdown()
			checkClusterFormed(t, hub1, hub2)

			leafConf := createConfFile(t, []byte(fmt.Sprintf(`
				server_name: leaf
				port: -1
				leafnodes {
					remotes: [
						{url: "nats-leaf://ten:ten@127.0.0.1:%d", account: TEN}
						{url: "nats-leaf://ss:ss@127.0.0.1:%d", account: SS}
					]
				}
				accounts: {
					TEN: {users: [{user: ten, password: ten}]}
					SS: {users: [{user: ss, password: ss}]}
				}
			`, hub1.getOpts().LeafNode.Port, hub2.getOpts().LeafNode.Port)))
			leaf := RunServer(LoadConfig(leafConf))
			defer leaf.Shutdown()

			checkLeafNodeConnectedCount(t, hub1, 1)
			checkLeafNodeConnectedCount(t, hub2, 1)

			responder := natsConnect(t, leaf.ClientURL(), nats.UserInfo("ss", "ss"))
			defer responder.Close()
			respond := func(m *nats.Msg) { m.Respond([]byte("pong")) }
			var err error
			if queue {
				_, err = responder.QueueSubscribe("ten.event", "workers", respond)
			} else {
				_, err = responder.Subscribe("ten.event", respond)
			}
			if err != nil {
				t.Fatalf("Error subscribing: %v", err)
			}
			natsFlush(t, responder)

			acc, err := leaf.LookupAccount("TEN")
			if err != nil {
				t.Fatalf("Error looking up TEN on the leaf: %v", err)
			}
			checkFor(t, 2*time.Second, 15*time.Millisecond, func() error {
				if acc.Interest("ten.event") == 0 {
					return fmt.Errorf("leaf account TEN has no interest for %q", "ten.event")
				}
				return nil
			})

			requester := natsConnect(t, leaf.ClientURL(), nats.UserInfo("ten", "ten"), nats.CustomInboxPrefix("REPLY.ten"))
			defer requester.Close()
			resp, err := requester.Request("ten.event", []byte("ping"), 2*time.Second)
			if err != nil {
				t.Fatalf("Request failed: %v", err)
			}
			if string(resp.Data) != "pong" {
				t.Fatalf("Unexpected response %q", resp.Data)
			}
		})
	}
}

// Interest a hub learns over a gateway is propagated to its leafnodes and has
// to be retracted from them when that gateway connection closes, as
// removeRemoteSubs does for routes. Interest the hub also holds locally for
// the same subjects must survive.
func TestLeafNodeInterestLeakOnGatewayClose(t *testing.T) {
	const numSubjects, numLocal = 10, 2

	ob := testDefaultOptionsForGateway("B")
	b := runGatewayServer(ob)
	defer b.Shutdown()

	// Gateways and leafnodes on one server require a system account. The
	// leafnode also switches the gateway to interest-only mode, which is
	// where every RS+ is propagated to leafnodes.
	oa := testGatewayOptionsFromToWithServers(t, "A", "B", b)
	oa.NoSystemAccount = false
	oa.Accounts = []*Account{NewAccount("SYS")}
	oa.SystemAccount = "SYS"
	oa.LeafNode.Host = "127.0.0.1"
	oa.LeafNode.Port = -1
	a := runGatewayServer(oa)
	defer a.Shutdown()
	waitForOutboundGateways(t, a, 1, 2*time.Second)
	waitForOutboundGateways(t, b, 1, 2*time.Second)

	ln, _ := runSolicitLeafServer(oa)
	defer ln.Shutdown()
	checkLeafNodeConnected(t, ln)
	checkGWInterestOnlyMode(t, a, "B", globalAccountName)

	// A leafnode reconnect rebuilds its interest from scratch and would hide
	// a leak, so pin the connection being checked.
	lz, err := ln.Leafz(nil)
	require_NoError(t, err)
	require_Equal(t, lz.NumLeafs, 1)
	leafID := lz.Leafs[0].ID

	// The leafnode must hold every subject if gw is true, otherwise only the
	// ones also subscribed to on "A".
	checkInterest := func(gw bool) error {
		lz, err := ln.Leafz(&LeafzOptions{Subscriptions: true})
		if err != nil {
			return err
		}
		if lz.NumLeafs != 1 || lz.Leafs[0].ID != leafID {
			return fmt.Errorf("Leafnode connection changed")
		}
		subs := make(map[string]struct{}, len(lz.Leafs[0].Subs))
		for _, sub := range lz.Leafs[0].Subs {
			subs[sub] = struct{}{}
		}
		for i := range numSubjects {
			expected := gw || i < numLocal
			for _, subj := range []string{fmt.Sprintf("foo.%d", i), fmt.Sprintf("bar.%d", i)} {
				if _, ok := subs[subj]; ok != expected {
					return fmt.Errorf("Interest on %q is %v, expected %v", subj, ok, expected)
				}
			}
		}
		return nil
	}

	// NoReconnect keeps the client from failing over to "A" when "B" shuts
	// down, where its re-created subscriptions would be real interest.
	ncb := natsConnect(t, b.ClientURL(), nats.NoReconnect())
	defer ncb.Close()
	for i := range numSubjects {
		natsSubSync(t, ncb, fmt.Sprintf("foo.%d", i))
		natsQueueSubSync(t, ncb, fmt.Sprintf("bar.%d", i), "workers")
	}
	natsFlush(t, ncb)

	nca := natsConnect(t, a.ClientURL())
	defer nca.Close()
	for i := range numLocal {
		natsSubSync(t, nca, fmt.Sprintf("foo.%d", i))
		natsQueueSubSync(t, nca, fmt.Sprintf("bar.%d", i), "workers")
	}
	natsFlush(t, nca)
	checkFor(t, 5*time.Second, 25*time.Millisecond, func() error { return checkInterest(true) })

	b.Shutdown()
	waitForOutboundGateways(t, a, 0, 2*time.Second)
	checkFor(t, time.Second, 25*time.Millisecond, func() error { return checkInterest(false) })

	// Retractions go out in map order, so a wrong one for a local subject may
	// still be in flight. Check again once a newer subscription has arrived.
	natsSubSync(t, nca, "barrier")
	natsFlush(t, nca)
	checkSubInterest(t, ln, globalAccountName, "barrier", time.Second)
	require_NoError(t, checkInterest(false))
}

func TestLeafNodeLogsRemoteURL(t *testing.T) {
	o1 := DefaultOptions()
	o1.LeafNode.Host = "127.0.0.1"
	o1.LeafNode.Port = -1
	o1.LeafNode.Username = remoteURLLogUser
	o1.LeafNode.Password = remoteURLLogPassword
	s1 := RunServer(o1)
	defer s1.Shutdown()

	leafHost := fmt.Sprintf("localhost:%d", o1.LeafNode.Port)
	o2 := DefaultOptions()
	o2.Cluster.Name = _EMPTY_
	o2.Cluster.Port = 0
	o2.LeafNode.resolver = &localhostResolver{}
	u := &url.URL{Scheme: "nats-leaf", Host: leafHost, User: url.UserPassword(remoteURLLogUser, remoteURLLogPassword)}
	o2.LeafNode.Remotes = []*RemoteLeafOpts{{URLs: []*url.URL{u}}}
	s2, l := runServerWithNoticeLogger(t, o2)
	defer s2.Shutdown()

	checkLeafNodeConnected(t, s2)
	checkNoticeContains(t, l, "Leafnode connection created for account: $G ("+leafHost+")")

	s1.Shutdown()
	checkNoticeContains(t, l, "Leafnode connection closed: "+ClientClosed.String()+" - Remote: "+s1.Name()+", Host: "+leafHost)
	s2.Shutdown()
	checkNoticeNotContains(t, l, remoteURLLogUser, remoteURLLogPassword)
}

// A leafnode can send a pull request with an _R_ reply and its own Nats-Request-Info.
// It may only speak for its own account, so the consumer must not track interest of another account.
func TestLeafNodeJetStreamPullForgedRequestInfo(t *testing.T) {
	conf := createConfFile(t, fmt.Appendf(nil, `
		listen: 127.0.0.1:-1
		jetstream: {max_mem_store: 64MB, max_file_store: 64MB, store_dir: %q}
		accounts {
			JS {
				jetstream: enabled
				users = [ { user: js, password: js }, { user: leaf, password: leaf } ]
			}
			V { users = [ { user: v, password: v } ] }
		}
		leafnodes { listen: 127.0.0.1:-1 }
	`, t.TempDir()))
	s, o := RunServerWithConfig(conf)
	defer s.Shutdown()

	nc, js := jsClientConnect(t, s, nats.UserInfo("js", "js"))
	defer nc.Close()
	_, err := js.AddStream(&nats.StreamConfig{Name: "TEST", Subjects: []string{"foo"}})
	require_NoError(t, err)
	_, err = js.AddConsumer("TEST", &nats.ConsumerConfig{Durable: "C", AckPolicy: nats.AckExplicitPolicy})
	require_NoError(t, err)

	// Another account has interest in a subject.
	ncV := natsConnect(t, s.ClientURL(), nats.UserInfo("v", "v"))
	defer ncV.Close()
	natsSubSync(t, ncV, "victim.inbox")
	natsFlush(t, ncV)

	leafConn, err := net.DialTimeout("tcp", net.JoinHostPort(o.LeafNode.Host, fmt.Sprintf("%d", o.LeafNode.Port)), 2*time.Second)
	require_NoError(t, err)
	defer leafConn.Close()
	_, err = bufio.NewReader(leafConn).ReadString('\n')
	require_NoError(t, err)
	_, err = leafConn.Write([]byte("CONNECT {\"name\":\"leaf\",\"user\":\"leaf\",\"pass\":\"leaf\",\"headers\":true}\r\nPING\r\n"))
	require_NoError(t, err)
	checkLeafNodeConnected(t, s)

	// The leafnode claims the request came from that account.
	hdr := "NATS/1.0\r\n" + ClientInfoHdr + ": {\"acc\":\"V\",\"reply\":\"victim.inbox\"}\r\n\r\n"
	req := `{"batch":1,"expires":30000000000}`
	subj := fmt.Sprintf(JSApiRequestNextT, "TEST", "C")
	_, err = fmt.Fprintf(leafConn, "HMSG %s _R_.forged %d %d\r\n%s%s\r\n", subj, len(hdr), len(hdr)+len(req), hdr, req)
	require_NoError(t, err)

	jsAcc, err := s.LookupAccount("JS")
	require_NoError(t, err)
	mset, err := jsAcc.lookupStream("TEST")
	require_NoError(t, err)
	oc := mset.lookupConsumer("C")
	require_NotNil(t, oc)
	checkFor(t, 2*time.Second, 50*time.Millisecond, func() error {
		oc.mu.RLock()
		defer oc.mu.RUnlock()
		if oc.waiting.len() != 1 {
			return fmt.Errorf("expected 1 waiting pull request, got %d", oc.waiting.len())
		}
		return nil
	})
	oc.mu.RLock()
	acc, interest := oc.waiting.head.acc.Name, oc.waiting.head.interest
	oc.mu.RUnlock()
	require_Equal(t, acc, "JS")
	require_Equal(t, interest, "_R_.forged")
}

func TestLeafNodeConnectInfoNotDelayedBySendingSubs(t *testing.T) {
	o := DefaultOptions()
	o.LeafNode.Port = -1
	s := RunServer(o)
	defer s.Shutdown()

	// Block sending subs, like an account with many subs would.
	sl := s.globalAccount().sl
	sl.Lock()
	defer sl.Unlock()

	addr := fmt.Sprintf("127.0.0.1:%d", o.LeafNode.Port)
	c, err := net.Dial("tcp", addr)
	require_NoError(t, err)
	defer c.Close()

	br := bufio.NewReader(c)
	require_NoError(t, c.SetReadDeadline(time.Now().Add(2*time.Second)))
	l, _, err := br.ReadLine()
	require_NoError(t, err)
	require_True(t, strings.HasPrefix(string(l), "INFO"))

	_, err = c.Write([]byte("CONNECT {}\r\n"))
	require_NoError(t, err)

	// The soliciting side times out if the INFO response to CONNECT is delayed.
	require_NoError(t, c.SetReadDeadline(time.Now().Add(2*time.Second)))
	l, _, err = br.ReadLine()
	require_NoError(t, err)
	require_True(t, strings.Contains(string(l), `"connect_info":true`))
}
