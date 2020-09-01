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
	"bufio"
	"context"
	"crypto/tls"
	"fmt"
	"net"
	"net/url"
	"os"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
)

type captureLeafNodeRandomIPLogger struct {
	DummyLogger
	ch  chan struct{}
	ips [3]int
}

func (c *captureLeafNodeRandomIPLogger) Debugf(format string, v ...interface{}) {
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
	o.LeafNode.dialTimeout = 15 * time.Millisecond
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
	defer os.Remove(conf1)
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
	defer os.Remove(conf2)
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
	defer os.Remove(conf1)
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
	defer os.Remove(conf2)
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
	defer os.Remove(conf3)
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
	defer os.Remove(conf4)
	o4, err := ProcessConfigFile(conf4)
	if err != nil {
		t.Fatalf("Error processing config file: %v", err)
	}

	if len(o4.LeafNode.Remotes) == 0 {
		t.Fatal("Expected at least a single leaf remote")
	}
	got = o4.LeafNode.Remotes[0].TLSTimeout
	expected = float64(DEFAULT_LEAF_TLS_TIMEOUT)
	if int(got) != int(expected) {
		t.Fatalf("Expected %v, got: %v", expected, got)
	}
}

type captureErrorLogger struct {
	DummyLogger
	errCh chan string
}

func (l *captureErrorLogger) Errorf(format string, v ...interface{}) {
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

	logFileName := createConfFile(t, []byte(""))
	defer os.Remove(logFileName)

	oa := DefaultOptions()
	oa.LeafNode.ReconnectInterval = 15 * time.Millisecond
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
		if !strings.Contains(e, "No local account") {
			t.Fatalf("Expected error about no local account, got %s", e)
		}
	case <-time.After(2 * time.Second):
		t.Fatalf("Did not get the error")
	}

	// For now, sa would try to recreate the connection for ever.
	// Check that lid is increasing...
	time.Sleep(100 * time.Millisecond)
	lid := atomic.LoadUint64(&sa.gcid)
	if lid < 4 {
		t.Fatalf("Seems like connection was not retried")
	}
}

// This test ensures that we can connect using proper user/password
// to a LN URL that was discovered through the INFO protocol.
func TestLeafNodeBasicAuthFailover(t *testing.T) {
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
			password: pwd
			timeout: 1
		}
	}
	`
	conf := createConfFile(t, []byte(fmt.Sprintf(content, "")))
	defer os.Remove(conf)

	sb1, ob1 := RunServerWithConfig(conf)
	defer sb1.Shutdown()

	conf = createConfFile(t, []byte(fmt.Sprintf(content, fmt.Sprintf("routes: [nats://127.0.0.1:%d]", ob1.Cluster.Port))))
	defer os.Remove(conf)

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
				url: "nats://foo:pwd@127.0.0.1:%d"
			}
		]
	}
	`
	conf = createConfFile(t, []byte(fmt.Sprintf(content, ob1.LeafNode.Port)))
	defer os.Remove(conf)

	sa, _ := RunServerWithConfig(conf)
	defer sa.Shutdown()

	checkLeafNodeConnected(t, sa)

	// Shutdown sb1, sa should reconnect to sb2
	sb1.Shutdown()

	// Wait a bit to make sure there was a disconnect and attempt to reconnect.
	time.Sleep(250 * time.Millisecond)

	// Should be able to reconnect
	checkLeafNodeConnected(t, sa)
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

func TestLeafNodeValidateAuthOptions(t *testing.T) {
	opts := DefaultOptions()
	opts.LeafNode.Username = "user1"
	opts.LeafNode.Password = "pwd"
	opts.LeafNode.Users = []*User{&User{Username: "user", Password: "pwd"}}
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
		{"no user creds required and no user so binds to ACC1", "", "", false},
		{"no user creds required and pick user2 associated to ACC2", "", "user2:user2@", false},
		{"no user creds required and unknown user should fail", "", "unknown:user@", true},
		{"user creds required so binds to ACC1", "user: \"ln\"\npass: \"pwd\"", "ln:pwd@", false},
	} {
		t.Run(test.name, func(t *testing.T) {

			conf := createConfFile(t, []byte(fmt.Sprintf(template, test.userSpec)))
			defer os.Remove(conf)
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
			defer os.Remove(conf)
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
	defer os.Remove(conf)
	s1, o1 := RunServerWithConfig(conf)
	defer s1.Shutdown()

	// Make sure that we reject a LN connection if user does not match
	conf = createConfFile(t, []byte(fmt.Sprintf(`
		port: -1
		leafnodes: {
			remotes = [{url: "nats-leaf://wron:user@%s:%d"}]
		}
	`, o1.LeafNode.Host, o1.LeafNode.Port)))
	defer os.Remove(conf)
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
	defer os.Remove(conf)
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
	defer os.Remove(conf)
	s3, _ := RunServerWithConfig(conf)
	defer s3.Shutdown()
}

func TestLeafNodeLoop(t *testing.T) {
	// This test requires that we set the port to known value because
	// we want A point to B and B to A.
	oa := DefaultOptions()
	oa.LeafNode.ReconnectInterval = 10 * time.Millisecond
	oa.LeafNode.Port = 1234
	ub, _ := url.Parse("nats://127.0.0.1:5678")
	oa.LeafNode.Remotes = []*RemoteLeafOpts{{URLs: []*url.URL{ub}}}
	oa.LeafNode.connDelay = 50 * time.Millisecond
	sa := RunServer(oa)
	defer sa.Shutdown()

	l := &captureErrorLogger{errCh: make(chan string, 10)}
	sa.SetLogger(l, false, false)

	ob := DefaultOptions()
	ob.LeafNode.ReconnectInterval = 10 * time.Millisecond
	ob.LeafNode.Port = 5678
	ua, _ := url.Parse("nats://127.0.0.1:1234")
	ob.LeafNode.Remotes = []*RemoteLeafOpts{{URLs: []*url.URL{ua}}}
	ob.LeafNode.connDelay = 50 * time.Millisecond
	sb := RunServer(ob)
	defer sb.Shutdown()

	select {
	case e := <-l.errCh:
		if !strings.Contains(e, "Loop") {
			t.Fatalf("Expected error about loop, got %v", e)
		}
	case <-time.After(2 * time.Second):
		t.Fatalf("Did not get any error regarding loop")
	}

	sb.Shutdown()
	ob.LeafNode.Remotes = nil
	sb = RunServer(ob)
	defer sb.Shutdown()

	checkLeafNodeConnected(t, sa)
}

func TestLeafNodeLoopFromDAG(t *testing.T) {
	// We want B & C to point to A, A itself does not point to any other server.
	// We need to cancel clustering since now this will suppress on its own.
	oa := DefaultOptions()
	oa.ServerName = "A"
	oa.LeafNode.ReconnectInterval = 10 * time.Millisecond
	oa.LeafNode.Port = -1
	oa.Cluster = ClusterOpts{}
	sa := RunServer(oa)
	defer sa.Shutdown()

	ua, _ := url.Parse(fmt.Sprintf("nats://127.0.0.1:%d", oa.LeafNode.Port))

	// B will point to A
	ob := DefaultOptions()
	ob.ServerName = "B"
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
	oc.LeafNode.ReconnectInterval = 10 * time.Millisecond
	oc.LeafNode.Remotes = []*RemoteLeafOpts{{URLs: []*url.URL{ua}}, {URLs: []*url.URL{ub}}}
	oc.LeafNode.connDelay = 100 * time.Millisecond // Allow logger to be attached before connecting.
	oc.Cluster = ClusterOpts{}
	sc := RunServer(oc)

	lc := &captureErrorLogger{errCh: make(chan string, 10)}
	sc.SetLogger(lc, false, false)

	// We should get an error.
	select {
	case e := <-lc.errCh:
		if !strings.Contains(e, "Loop") {
			t.Fatalf("Expected error about loop, got %v", e)
		}
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

func TestLeafCloseTLSConnection(t *testing.T) {
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

	endpoint := fmt.Sprintf("%s:%d", opts.LeafNode.Host, opts.LeafNode.Port)
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
	connectOp := []byte("CONNECT {\"name\":\"leaf\",\"verbose\":false,\"pedantic\":false,\"tls_required\":true}\r\n")
	if _, err := tlsConn.Write(connectOp); err != nil {
		t.Fatalf("Unexpected error writing CONNECT: %v", err)
	}
	infoOp := []byte("INFO {\"server_id\":\"leaf\",\"tls_required\":true}\r\n")
	if _, err := tlsConn.Write(infoOp); err != nil {
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
		&RemoteLeafOpts{
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
	// this should be registered in LN2 (there is 1 sub for LN1 $LDS subject)
	checkSubs(ln2.globalAccount(), 2)

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
	// Goes down to 1 (the $LDS one)
	checkSubs(ln2.globalAccount(), 1)

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
			checkSubs(ln2.globalAccount(), 2)

			if !test.ok {
				nc1.Publish(test.pubSubject, []byte("msg"))
				if _, err := sub.NextMsg(50 * time.Millisecond); err == nil {
					t.Fatalf("Did not expect to get the message")
				}
			} else {
				checkSubs(ln1.globalAccount(), 2)
				nc1.Publish(test.pubSubject, []byte("msg"))
				natsNexMsg(t, sub, time.Second)
			}
			sub.Unsubscribe()
			checkSubs(ln1.globalAccount(), 1)

		})
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
	lo2.LeafNode.Remotes = []*RemoteLeafOpts{
		{
			URLs:        []*url.URL{u},
			DenyExports: []string{">"},
		},
	}
	ln2 := RunServer(lo2)
	defer ln2.Shutdown()

	checkLeafNodeConnected(t, ln1)

	// The deny is totally restrictive, but make sure that we still accept the $LDS, $GR and _GR_ go from LN1.
	checkFor(t, time.Second, 15*time.Millisecond, func() error {
		// We should have registered the 3 subs from the accepting leafnode.
		if n := ln2.globalAccount().TotalSubs(); n != 3 {
			return fmt.Errorf("Expected %d subs, got %v", 3, n)
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

	l := &captureErrorLogger{errCh: make(chan string, 10)}
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
		case e := <-l.errCh:
			if !strings.Contains(e, "Loop detected") {
				t.Fatalf("Unexpected error: %q", e)
			}
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
	case e := <-l.errCh:
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
	users := []*User{&User{Username: certUserName, Account: acc}}

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

	defer os.Remove(conf)
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
			name: "abc"
			listen: "127.0.0.1:-1"
		}
	`, hopts.LeafNode.Port)))

	defer os.Remove(conf)
	opts, err = ProcessConfigFile(conf)
	if err != nil {
		t.Fatalf("Error processing config file: %v", err)
	}
	opts.NoLog, opts.NoSigs = true, true

	s = RunServer(opts)
	defer s.Shutdown()

	checkLeafNodeConnected(t, s)

	l = grabLeaf()
	if rc := l.remoteCluster(); rc != "abc" {
		t.Fatalf("Expected a remote cluster name of \"abc\", got %q", rc)
	}
	pcid := l.cid

	// Now make sure that if we update our cluster name, simulating the settling
	// of dynamic cluster names between competing servers.
	s.setClusterName("xyz", false)

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
