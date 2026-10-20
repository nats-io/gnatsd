// Copyright 2013-2026 The NATS Authors
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
	"math/rand/v2"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"reflect"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nats-io/jwt/v2"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nkeys"
)

func init() {
	routeConnectDelay = 15 * time.Millisecond
	routeConnectMaxDelay = 15 * time.Millisecond
	routeReconnectDelay = 15 * time.Millisecond
}

func checkNumRoutes(t *testing.T, s *Server, expected int) {
	t.Helper()
	checkFor(t, 5*time.Second, 15*time.Millisecond, func() error {
		if nr := s.NumRoutes(); nr != expected {
			return fmt.Errorf("Expected %v routes, got %v", expected, nr)
		}
		return nil
	})
}

func checkSubInterest(t *testing.T, s *Server, accName, subject string, timeout time.Duration) {
	t.Helper()
	checkFor(t, timeout, 15*time.Millisecond, func() error {
		acc, err := s.LookupAccount(accName)
		if err != nil {
			return fmt.Errorf("error looking up account %q: %v", accName, err)
		}
		if acc.SubscriptionInterest(subject) {
			return nil
		}
		return fmt.Errorf("no subscription interest for account %q on %q", accName, subject)
	})
}

func checkSubNoInterest(t *testing.T, s *Server, accName, subject string, timeout time.Duration) {
	t.Helper()
	checkFor(t, timeout, 15*time.Millisecond, func() error {
		acc, err := s.LookupAccount(accName)
		if err != nil {
			return fmt.Errorf("error looking up account %q: %v", accName, err)
		}
		if acc.SubscriptionInterest(subject) {
			return fmt.Errorf("unexpected subscription interest for account %q on %q", accName, subject)
		}
		return nil
	})
}

func TestRouteConfig(t *testing.T) {
	opts, err := ProcessConfigFile("./configs/cluster.conf")
	if err != nil {
		t.Fatalf("Received an error reading route config file: %v\n", err)
	}

	golden := &Options{
		ConfigFile:  "./configs/cluster.conf",
		Host:        "127.0.0.1",
		Port:        4242,
		Username:    "derek",
		Password:    "porkchop",
		AuthTimeout: 1.0,
		Cluster: ClusterOpts{
			Name:           "abc",
			Host:           "127.0.0.1",
			Port:           4244,
			Username:       "route_user",
			Password:       "top_secret",
			AuthTimeout:    1.0,
			NoAdvertise:    true,
			ConnectRetries: 2,
			ConnectBackoff: true,
		},
		PidFile:          "/tmp/nats-server/nats_cluster_test.pid",
		configDigest:     "sha256:b2f6d54063e43a85f5a09d198204f044bc154f9f9ccfb84e47d5762091aec487",
		authBlockDefined: true,
	}

	// Setup URLs
	r1, _ := url.Parse("nats-route://foo:bar@127.0.0.1:4245")
	r2, _ := url.Parse("nats-route://foo:bar@127.0.0.1:4246")

	golden.Routes = []*url.URL{r1, r2}

	checkOptionsEqual(t, golden, opts)
}

func TestClusterAdvertise(t *testing.T) {
	lst, err := natsListen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("Error starting listener: %v", err)
	}
	ch := make(chan error)
	go func() {
		c, err := lst.Accept()
		if err != nil {
			ch <- err
			return
		}
		c.Close()
		ch <- nil
	}()

	optsA, err := ProcessConfigFile("./configs/seed.conf")
	require_NoError(t, err)
	optsA.NoSigs, optsA.NoLog = true, true
	srvA := RunServer(optsA)
	defer srvA.Shutdown()

	srvARouteURL := fmt.Sprintf("nats://%s:%d", optsA.Cluster.Host, srvA.ClusterAddr().Port)
	optsB := nextServerOpts(optsA)
	optsB.Routes = RoutesFromStr(srvARouteURL)

	srvB := RunServer(optsB)
	defer srvB.Shutdown()

	// Wait for these 2 to connect to each other
	checkClusterFormed(t, srvA, srvB)

	// Now start server C that connects to A. A should ask B to connect to C,
	// based on C's URL. But since C configures a Cluster.Advertise, it will connect
	// to our listener.
	optsC := nextServerOpts(optsB)
	optsC.Cluster.Advertise = lst.Addr().String()
	optsC.ClientAdvertise = "me:1"
	optsC.Routes = RoutesFromStr(srvARouteURL)

	srvC := RunServer(optsC)
	defer srvC.Shutdown()

	select {
	case e := <-ch:
		if e != nil {
			t.Fatalf("Error: %v", e)
		}
	case <-time.After(2 * time.Second):
		t.Fatalf("Test timed out")
	}
}

func TestClusterAdvertiseErrorOnStartup(t *testing.T) {
	opts := DefaultOptions()
	// Set invalid address
	opts.Cluster.Advertise = "addr:::123"
	testFatalErrorOnStart(t, opts, "Cluster.Advertise")
}

func TestClientAdvertise(t *testing.T) {
	optsA, err := ProcessConfigFile("./configs/seed.conf")
	require_NoError(t, err)
	optsA.NoSigs, optsA.NoLog = true, true

	srvA := RunServer(optsA)
	defer srvA.Shutdown()

	optsB := nextServerOpts(optsA)
	optsB.Routes = RoutesFromStr(fmt.Sprintf("nats://%s:%d", optsA.Cluster.Host, optsA.Cluster.Port))
	optsB.ClientAdvertise = "me:1"
	srvB := RunServer(optsB)
	defer srvB.Shutdown()

	checkClusterFormed(t, srvA, srvB)

	nc, err := nats.Connect(fmt.Sprintf("nats://%s:%d", optsA.Host, optsA.Port))
	if err != nil {
		t.Fatalf("Error on connect: %v", err)
	}
	defer nc.Close()
	checkFor(t, time.Second, 15*time.Millisecond, func() error {
		ds := nc.DiscoveredServers()
		if len(ds) == 1 {
			if ds[0] == "nats://me:1" {
				return nil
			}
		}
		return fmt.Errorf("Did not get expected discovered servers: %v", nc.DiscoveredServers())
	})
}

func TestServerRoutesWithClients(t *testing.T) {
	optsA, err := ProcessConfigFile("./configs/srv_a.conf")
	require_NoError(t, err)
	optsB, err := ProcessConfigFile("./configs/srv_b.conf")
	require_NoError(t, err)

	optsA.NoSigs, optsA.NoLog = true, true
	optsB.NoSigs, optsB.NoLog = true, true

	srvA := RunServer(optsA)
	defer srvA.Shutdown()

	urlA := fmt.Sprintf("nats://%s:%d/", optsA.Host, optsA.Port)
	urlB := fmt.Sprintf("nats://%s:%d/", optsB.Host, optsB.Port)

	nc1, err := nats.Connect(urlA)
	if err != nil {
		t.Fatalf("Error creating client: %v\n", err)
	}
	defer nc1.Close()

	ch := make(chan bool)
	sub, _ := nc1.Subscribe("foo", func(m *nats.Msg) { ch <- true })
	nc1.QueueSubscribe("foo", "bar", func(m *nats.Msg) {})
	nc1.Publish("foo", []byte("Hello"))
	// Wait for message
	<-ch
	sub.Unsubscribe()

	srvB := RunServer(optsB)
	defer srvB.Shutdown()

	// Wait for route to form.
	checkClusterFormed(t, srvA, srvB)

	nc2, err := nats.Connect(urlB)
	if err != nil {
		t.Fatalf("Error creating client: %v\n", err)
	}
	defer nc2.Close()
	nc2.Publish("foo", []byte("Hello"))
	nc2.Flush()
}

func TestServerRoutesWithAuthAndBCrypt(t *testing.T) {
	optsA, err := ProcessConfigFile("./configs/srv_a_bcrypt.conf")
	require_NoError(t, err)
	optsB, err := ProcessConfigFile("./configs/srv_b_bcrypt.conf")
	require_NoError(t, err)

	optsA.NoSigs, optsA.NoLog = true, true
	optsB.NoSigs, optsB.NoLog = true, true

	srvA := RunServer(optsA)
	defer srvA.Shutdown()

	srvB := RunServer(optsB)
	defer srvB.Shutdown()

	// Wait for route to form.
	checkClusterFormed(t, srvA, srvB)

	urlA := fmt.Sprintf("nats://%s:%s@%s:%d/", optsA.Username, optsA.Password, optsA.Host, optsA.Port)
	urlB := fmt.Sprintf("nats://%s:%s@%s:%d/", optsB.Username, optsB.Password, optsB.Host, optsB.Port)

	nc1, err := nats.Connect(urlA)
	if err != nil {
		t.Fatalf("Error creating client: %v\n", err)
	}
	defer nc1.Close()

	// Test that we are connected.
	ch := make(chan bool)
	sub, err := nc1.Subscribe("foo", func(m *nats.Msg) { ch <- true })
	if err != nil {
		t.Fatalf("Error creating subscription: %v\n", err)
	}
	nc1.Flush()
	defer sub.Unsubscribe()

	checkSubInterest(t, srvB, globalAccountName, "foo", time.Second)

	nc2, err := nats.Connect(urlB)
	if err != nil {
		t.Fatalf("Error creating client: %v\n", err)
	}
	defer nc2.Close()
	nc2.Publish("foo", []byte("Hello"))
	nc2.Flush()

	// Wait for message
	select {
	case <-ch:
	case <-time.After(2 * time.Second):
		t.Fatal("Timeout waiting for message across route")
	}
}

// Helper function to check that a cluster is formed
func checkClusterFormed(t testing.TB, servers ...*Server) {
	t.Helper()
	var _enr [8]int
	enr := _enr[:0]
	for _, a := range servers {
		if a.getOpts().Cluster.PoolSize < 0 {
			enr = append(enr, len(servers)-1)
		} else {
			a.mu.RLock()
			nr := a.routesPoolSize + len(a.accRoutes)
			a.mu.RUnlock()
			total := 0
			for _, b := range servers {
				if a == b {
					continue
				}
				bo := b.getOpts()
				if ps := bo.Cluster.PoolSize; ps < 0 {
					total++
				} else {
					bps := ps + len(bo.Cluster.PinnedAccounts)
					total += max(nr, bps)
				}
			}
			enr = append(enr, total)
		}
	}
	checkFor(t, 10*time.Second, 15*time.Millisecond, func() error {
		for i, s := range servers {
			if numRoutes := s.NumRoutes(); numRoutes != enr[i] {
				return fmt.Errorf("Expected %d routes for server %q, got %d", enr[i], s, numRoutes)
			}
		}
		return nil
	})
}

// Helper function to generate next opts to make sure no port conflicts etc.
func nextServerOpts(opts *Options) *Options {
	nopts := *opts
	nopts.Port = -1
	nopts.Cluster.Port = -1
	nopts.HTTPPort = -1
	if nopts.Gateway.Name != "" {
		nopts.Gateway.Port = -1
	}
	nopts.ServerName = ""
	return &nopts
}

func TestSeedSolicitWorks(t *testing.T) {
	optsSeed, err := ProcessConfigFile("./configs/seed.conf")
	require_NoError(t, err)

	optsSeed.NoSigs, optsSeed.NoLog = true, true
	optsSeed.NoSystemAccount = true

	srvSeed := RunServer(optsSeed)
	defer srvSeed.Shutdown()

	optsA := nextServerOpts(optsSeed)
	optsA.Routes = RoutesFromStr(fmt.Sprintf("nats://%s:%d", optsSeed.Cluster.Host,
		srvSeed.ClusterAddr().Port))

	srvA := RunServer(optsA)
	defer srvA.Shutdown()

	urlA := fmt.Sprintf("nats://%s:%d/", optsA.Host, srvA.ClusterAddr().Port)

	nc1, err := nats.Connect(urlA)
	if err != nil {
		t.Fatalf("Error creating client: %v\n", err)
	}
	defer nc1.Close()

	// Test that we are connected.
	ch := make(chan bool)
	nc1.Subscribe("foo", func(m *nats.Msg) { ch <- true })
	nc1.Flush()

	optsB := nextServerOpts(optsA)
	optsB.Routes = RoutesFromStr(fmt.Sprintf("nats://%s:%d", optsSeed.Cluster.Host,
		srvSeed.ClusterAddr().Port))

	srvB := RunServer(optsB)
	defer srvB.Shutdown()

	urlB := fmt.Sprintf("nats://%s:%d/", optsB.Host, srvB.ClusterAddr().Port)

	nc2, err := nats.Connect(urlB)
	if err != nil {
		t.Fatalf("Error creating client: %v\n", err)
	}
	defer nc2.Close()

	checkClusterFormed(t, srvSeed, srvA, srvB)
	checkExpectedSubs(t, 1, srvB)

	nc2.Publish("foo", []byte("Hello"))

	// Wait for message
	select {
	case <-ch:
	case <-time.After(2 * time.Second):
		t.Fatal("Timeout waiting for message across route")
	}
}

func TestTLSSeedSolicitWorks(t *testing.T) {
	optsSeed, err := ProcessConfigFile("./configs/seed_tls.conf")
	require_NoError(t, err)

	optsSeed.NoSigs, optsSeed.NoLog = true, true
	optsSeed.NoSystemAccount = true

	srvSeed := RunServer(optsSeed)
	defer srvSeed.Shutdown()

	seedRouteURL := fmt.Sprintf("nats://%s:%d", optsSeed.Cluster.Host,
		srvSeed.ClusterAddr().Port)
	optsA := nextServerOpts(optsSeed)
	optsA.Routes = RoutesFromStr(seedRouteURL)

	srvA := RunServer(optsA)
	defer srvA.Shutdown()

	urlA := fmt.Sprintf("nats://%s:%d/", optsA.Host, srvA.Addr().(*net.TCPAddr).Port)

	nc1, err := nats.Connect(urlA)
	if err != nil {
		t.Fatalf("Error creating client: %v\n", err)
	}
	defer nc1.Close()

	// Test that we are connected.
	ch := make(chan bool)
	nc1.Subscribe("foo", func(m *nats.Msg) { ch <- true })
	nc1.Flush()

	optsB := nextServerOpts(optsA)
	optsB.Routes = RoutesFromStr(seedRouteURL)

	srvB := RunServer(optsB)
	defer srvB.Shutdown()

	urlB := fmt.Sprintf("nats://%s:%d/", optsB.Host, srvB.Addr().(*net.TCPAddr).Port)

	nc2, err := nats.Connect(urlB)
	if err != nil {
		t.Fatalf("Error creating client: %v\n", err)
	}
	defer nc2.Close()

	checkClusterFormed(t, srvSeed, srvA, srvB)
	checkExpectedSubs(t, 1, srvB)

	nc2.Publish("foo", []byte("Hello"))

	// Wait for message
	select {
	case <-ch:
	case <-time.After(2 * time.Second):
		t.Fatal("Timeout waiting for message across route")
	}
}

func TestChainedSolicitWorks(t *testing.T) {
	optsSeed, err := ProcessConfigFile("./configs/seed.conf")
	require_NoError(t, err)

	optsSeed.NoSigs, optsSeed.NoLog = true, true
	optsSeed.NoSystemAccount = true

	srvSeed := RunServer(optsSeed)
	defer srvSeed.Shutdown()

	seedRouteURL := fmt.Sprintf("nats://%s:%d", optsSeed.Cluster.Host,
		srvSeed.ClusterAddr().Port)
	optsA := nextServerOpts(optsSeed)
	optsA.Routes = RoutesFromStr(seedRouteURL)

	srvA := RunServer(optsA)
	defer srvA.Shutdown()

	urlSeed := fmt.Sprintf("nats://%s:%d/", optsSeed.Host, srvA.Addr().(*net.TCPAddr).Port)

	nc1, err := nats.Connect(urlSeed)
	if err != nil {
		t.Fatalf("Error creating client: %v\n", err)
	}
	defer nc1.Close()

	// Test that we are connected.
	ch := make(chan bool)
	nc1.Subscribe("foo", func(m *nats.Msg) { ch <- true })
	nc1.Flush()

	optsB := nextServerOpts(optsA)
	// Server B connects to A
	optsB.Routes = RoutesFromStr(fmt.Sprintf("nats://%s:%d", optsA.Cluster.Host,
		srvA.ClusterAddr().Port))

	srvB := RunServer(optsB)
	defer srvB.Shutdown()

	urlB := fmt.Sprintf("nats://%s:%d/", optsB.Host, srvB.Addr().(*net.TCPAddr).Port)

	nc2, err := nats.Connect(urlB)
	if err != nil {
		t.Fatalf("Error creating client: %v\n", err)
	}
	defer nc2.Close()

	checkClusterFormed(t, srvSeed, srvA, srvB)
	checkExpectedSubs(t, 1, srvB)

	nc2.Publish("foo", []byte("Hello"))

	// Wait for message
	select {
	case <-ch:
	case <-time.After(2 * time.Second):
		t.Fatal("Timeout waiting for message across route")
	}
}

// Helper function to check that a server (or list of servers) have the
// expected number of subscriptions.
func checkExpectedSubs(t *testing.T, expected int, servers ...*Server) {
	t.Helper()
	checkFor(t, 4*time.Second, 10*time.Millisecond, func() error {
		for _, s := range servers {
			if numSubs := int(s.NumSubscriptions()); numSubs != expected {
				return fmt.Errorf("Expected %d subscriptions for server %q, got %d", expected, s.ID(), numSubs)
			}
		}
		return nil
	})
}

func TestTLSChainedSolicitWorks(t *testing.T) {
	optsSeed, err := ProcessConfigFile("./configs/seed_tls.conf")
	require_NoError(t, err)

	optsSeed.NoSigs, optsSeed.NoLog = true, true
	optsSeed.NoSystemAccount = true

	srvSeed := RunServer(optsSeed)
	defer srvSeed.Shutdown()

	urlSeedRoute := fmt.Sprintf("nats://%s:%d", optsSeed.Cluster.Host,
		srvSeed.ClusterAddr().Port)
	optsA := nextServerOpts(optsSeed)
	optsA.Routes = RoutesFromStr(urlSeedRoute)

	srvA := RunServer(optsA)
	defer srvA.Shutdown()

	urlSeed := fmt.Sprintf("nats://%s:%d/", optsSeed.Host, srvSeed.Addr().(*net.TCPAddr).Port)

	nc1, err := nats.Connect(urlSeed)
	if err != nil {
		t.Fatalf("Error creating client: %v\n", err)
	}
	defer nc1.Close()

	// Test that we are connected.
	ch := make(chan bool)
	nc1.Subscribe("foo", func(m *nats.Msg) { ch <- true })
	nc1.Flush()

	optsB := nextServerOpts(optsA)
	// Server B connects to A
	optsB.Routes = RoutesFromStr(fmt.Sprintf("nats://%s:%d", optsA.Cluster.Host,
		srvA.ClusterAddr().Port))

	srvB := RunServer(optsB)
	defer srvB.Shutdown()

	checkClusterFormed(t, srvSeed, srvA, srvB)
	checkExpectedSubs(t, 1, srvA, srvB)

	urlB := fmt.Sprintf("nats://%s:%d/", optsB.Host, srvB.Addr().(*net.TCPAddr).Port)

	nc2, err := nats.Connect(urlB)
	if err != nil {
		t.Fatalf("Error creating client: %v\n", err)
	}
	defer nc2.Close()

	nc2.Publish("foo", []byte("Hello"))

	// Wait for message
	select {
	case <-ch:
	case <-time.After(2 * time.Second):
		t.Fatal("Timeout waiting for message across route")
	}
}

func TestRouteTLSHandshakeError(t *testing.T) {
	optsSeed, err := ProcessConfigFile("./configs/seed_tls.conf")
	require_NoError(t, err)
	optsSeed.NoLog = true
	optsSeed.NoSigs = true
	srvSeed := RunServer(optsSeed)
	defer srvSeed.Shutdown()

	opts := DefaultOptions()
	opts.Routes = RoutesFromStr(fmt.Sprintf("nats://%s:%d", optsSeed.Cluster.Host, optsSeed.Cluster.Port))

	srv := RunServer(opts)
	defer srv.Shutdown()

	time.Sleep(500 * time.Millisecond)

	checkNumRoutes(t, srv, 0)
}

func TestBlockedShutdownOnRouteAcceptLoopFailure(t *testing.T) {
	opts := DefaultOptions()
	opts.Cluster.Host = "x.x.x.x"
	opts.Cluster.Port = 7222

	s := New(opts)
	s.Start()
	// Wait a second
	time.Sleep(time.Second)
	ch := make(chan bool)
	go func() {
		s.Shutdown()
		ch <- true
	}()

	timeout := time.NewTimer(5 * time.Second)
	select {
	case <-ch:
		return
	case <-timeout.C:
		t.Fatal("Shutdown did not complete")
	}
}

func TestRouteUseIPv6(t *testing.T) {
	opts := DefaultOptions()
	opts.Cluster.Host = "::"
	opts.Cluster.Port = 6222

	// I believe that there is no IPv6 support on Travis...
	// Regardless, cannot have this test fail simply because IPv6 is disabled
	// on the host.
	hp := net.JoinHostPort(opts.Cluster.Host, strconv.Itoa(opts.Cluster.Port))
	_, err := net.ResolveTCPAddr("tcp", hp)
	if err != nil {
		t.Skipf("Skipping this test since there is no IPv6 support on this host: %v", err)
	}

	s := RunServer(opts)
	defer s.Shutdown()

	routeUp := false
	timeout := time.Now().Add(5 * time.Second)
	for time.Now().Before(timeout) && !routeUp {
		// We know that the server is local and listening to
		// all IPv6 interfaces. Try connect using IPv6 loopback.
		if conn, err := net.Dial("tcp", "[::1]:6222"); err != nil {
			// Travis seem to have the server actually listening to 0.0.0.0,
			// so try with 127.0.0.1
			if conn, err := net.Dial("tcp", "127.0.0.1:6222"); err != nil {
				time.Sleep(time.Second)
				continue
			} else {
				conn.Close()
			}
		} else {
			conn.Close()
		}
		routeUp = true
	}
	if !routeUp {
		t.Fatal("Server failed to start route accept loop")
	}
}

func TestClientConnectToRoutePort(t *testing.T) {
	opts := DefaultOptions()

	// Since client will first connect to the route listen port, set the
	// cluster's Host to 127.0.0.1 so it works on Windows too, since on
	// Windows, a client can't use 0.0.0.0 in a connect.
	opts.Cluster.Host = "127.0.0.1"
	s := RunServer(opts)
	defer s.Shutdown()

	url := fmt.Sprintf("nats://%s:%d", opts.Cluster.Host, s.ClusterAddr().Port)
	clientURL := fmt.Sprintf("nats://%s:%d", opts.Host, opts.Port)
	// When connecting to the ROUTE port, the client library will receive the
	// CLIENT port in the INFO protocol. This URL is added to the client's pool
	// and will be tried after the initial connect failure. So all those
	// nats.Connect() should succeed.
	// The only reason for a failure would be if there are too many FDs in time-wait
	// which would delay the creation of TCP connection. So keep the total of
	// attempts rather small.
	total := 10
	for i := 0; i < total; i++ {
		nc, err := nats.Connect(url)
		if err != nil {
			t.Fatalf("Unexpected error on connect: %v", err)
		}
		defer nc.Close()
		if nc.ConnectedUrl() != clientURL {
			t.Fatalf("Expected client to be connected to %v, got %v", clientURL, nc.ConnectedUrl())
		}
	}

	s.Shutdown()
	// Try again with NoAdvertise and this time, the client should fail to connect.
	opts.Cluster.NoAdvertise = true
	s = RunServer(opts)
	defer s.Shutdown()

	for i := 0; i < total; i++ {
		nc, err := nats.Connect(url)
		if err == nil {
			nc.Close()
			t.Fatal("Expected error on connect, got none")
		}
	}
}

type checkDuplicateRouteLogger struct {
	DummyLogger
	gotDuplicate bool
}

func (l *checkDuplicateRouteLogger) Debugf(format string, v ...any) {
	l.Lock()
	defer l.Unlock()
	msg := fmt.Sprintf(format, v...)
	if strings.Contains(msg, "duplicate remote route") {
		l.gotDuplicate = true
	}
}

func TestRoutesToEachOther(t *testing.T) {
	optsA := DefaultOptions()
	optsA.Cluster.Port = 7246
	optsA.Routes = RoutesFromStr("nats://127.0.0.1:7247")

	optsB := DefaultOptions()
	optsB.Cluster.Port = 7247
	optsB.Routes = RoutesFromStr("nats://127.0.0.1:7246")

	srvALogger := &checkDuplicateRouteLogger{}
	srvA := New(optsA)
	srvA.SetLogger(srvALogger, true, false)
	defer srvA.Shutdown()

	srvBLogger := &checkDuplicateRouteLogger{}
	srvB := New(optsB)
	srvB.SetLogger(srvBLogger, true, false)
	defer srvB.Shutdown()

	go srvA.Start()
	go srvB.Start()

	start := time.Now()
	checkClusterFormed(t, srvA, srvB)
	end := time.Now()

	srvALogger.Lock()
	gotIt := srvALogger.gotDuplicate
	srvALogger.Unlock()
	if !gotIt {
		srvBLogger.Lock()
		gotIt = srvBLogger.gotDuplicate
		srvBLogger.Unlock()
	}
	if gotIt {
		dur := end.Sub(start)
		// It should not take too long to have a successful connection
		// between the 2 servers.
		if dur > 5*time.Second {
			t.Logf("Cluster formed, but took a long time: %v", dur)
		}
	} else {
		t.Log("Was not able to get duplicate route this time!")
	}
}

func wait(ch chan bool) error {
	select {
	case <-ch:
		return nil
	case <-time.After(5 * time.Second):
	}
	return fmt.Errorf("timeout")
}

func TestServerPoolUpdatedWhenRouteGoesAway(t *testing.T) {
	s1Opts := DefaultOptions()
	s1Opts.ServerName = "A"
	s1Opts.Host = "127.0.0.1"
	s1Opts.Port = 4222
	s1Opts.Cluster.Host = "127.0.0.1"
	s1Opts.Cluster.Port = 6222
	s1Opts.Routes = RoutesFromStr("nats://127.0.0.1:6223,nats://127.0.0.1:6224")
	s1 := RunServer(s1Opts)
	defer s1.Shutdown()

	s1Url := "nats://127.0.0.1:4222"
	s2Url := "nats://127.0.0.1:4223"
	s3Url := "nats://127.0.0.1:4224"

	ch := make(chan bool, 1)
	chch := make(chan bool, 1)
	connHandler := func(_ *nats.Conn) {
		chch <- true
	}
	nc, err := nats.Connect(s1Url,
		nats.ReconnectWait(50*time.Millisecond),
		nats.ReconnectHandler(connHandler),
		nats.DiscoveredServersHandler(func(_ *nats.Conn) {
			ch <- true
		}))
	if err != nil {
		t.Fatalf("Error on connect")
	}
	defer nc.Close()

	s2Opts := DefaultOptions()
	s2Opts.ServerName = "B"
	s2Opts.Host = "127.0.0.1"
	s2Opts.Port = s1Opts.Port + 1
	s2Opts.Cluster.Host = "127.0.0.1"
	s2Opts.Cluster.Port = 6223
	s2Opts.Routes = RoutesFromStr("nats://127.0.0.1:6222,nats://127.0.0.1:6224")
	s2 := RunServer(s2Opts)
	defer s2.Shutdown()

	// Wait to be notified
	if err := wait(ch); err != nil {
		t.Fatal("New server callback was not invoked")
	}

	checkPool := func(expected []string) {
		t.Helper()
		// Don't use discovered here, but Servers to have the full list.
		// Also, there may be cases where the mesh is not formed yet,
		// so try again on failure.
		checkFor(t, 5*time.Second, 50*time.Millisecond, func() error {
			ds := nc.Servers()
			if len(ds) == len(expected) {
				m := make(map[string]struct{}, len(ds))
				for _, url := range ds {
					m[url] = struct{}{}
				}
				ok := true
				for _, url := range expected {
					if _, present := m[url]; !present {
						ok = false
						break
					}
				}
				if ok {
					return nil
				}
			}
			return fmt.Errorf("Expected %v, got %v", expected, ds)
		})
	}
	// Verify that we now know about s2
	checkPool([]string{s1Url, s2Url})

	s3Opts := DefaultOptions()
	s3Opts.ServerName = "C"
	s3Opts.Host = "127.0.0.1"
	s3Opts.Port = s2Opts.Port + 1
	s3Opts.Cluster.Host = "127.0.0.1"
	s3Opts.Cluster.Port = 6224
	s3Opts.Routes = RoutesFromStr("nats://127.0.0.1:6222,nats://127.0.0.1:6223")
	s3 := RunServer(s3Opts)
	defer s3.Shutdown()

	// Wait to be notified
	if err := wait(ch); err != nil {
		t.Fatal("New server callback was not invoked")
	}
	// Verify that we now know about s3
	checkPool([]string{s1Url, s2Url, s3Url})

	// Stop s1. Since this was passed to the Connect() call, this one should
	// still be present.
	s1.Shutdown()
	// Wait for reconnect
	if err := wait(chch); err != nil {
		t.Fatal("Reconnect handler not invoked")
	}
	checkPool([]string{s1Url, s2Url, s3Url})

	// Check the server we reconnected to.
	reConnectedTo := nc.ConnectedUrl()
	expected := []string{s1Url}
	if reConnectedTo == s2Url {
		s2.Shutdown()
		expected = append(expected, s3Url)
	} else if reConnectedTo == s3Url {
		s3.Shutdown()
		expected = append(expected, s2Url)
	} else {
		t.Fatalf("Unexpected server client has reconnected to: %v", reConnectedTo)
	}
	// Wait for reconnect
	if err := wait(chch); err != nil {
		t.Fatal("Reconnect handler not invoked")
	}
	// The implicit server that we just shutdown should have been removed from the pool
	checkPool(expected)
	nc.Close()
}

func TestRouteFailedConnRemovedFromTmpMap(t *testing.T) {
	for _, test := range []struct {
		name     string
		poolSize int
	}{
		{"no pooling", -1},
		{"pool 1", 1},
		{"pool 3", 3},
	} {
		t.Run(test.name, func(t *testing.T) {
			optsA, err := ProcessConfigFile("./configs/srv_a.conf")
			require_NoError(t, err)
			optsA.NoSigs, optsA.NoLog = true, true
			optsA.Cluster.PoolSize = test.poolSize

			optsB, err := ProcessConfigFile("./configs/srv_b.conf")
			require_NoError(t, err)
			optsB.NoSigs, optsB.NoLog = true, true
			optsB.Cluster.PoolSize = test.poolSize

			srvA := New(optsA)
			defer srvA.Shutdown()
			srvB := New(optsB)
			defer srvB.Shutdown()

			// Start this way to increase chance of having the two connect
			// to each other at the same time. This will cause one of the
			// route to be dropped.
			wg := &sync.WaitGroup{}
			wg.Add(2)
			go func() {
				srvA.Start()
				wg.Done()
			}()
			go func() {
				srvB.Start()
				wg.Done()
			}()

			checkClusterFormed(t, srvA, srvB)

			// Ensure that maps are empty
			checkMap := func(s *Server) {
				checkFor(t, 2*time.Second, 15*time.Millisecond, func() error {
					s.grMu.Lock()
					l := len(s.grTmpClients)
					s.grMu.Unlock()
					if l != 0 {
						return fmt.Errorf("grTmpClients map should be empty, got %v", l)
					}
					return nil
				})
			}
			checkMap(srvA)
			checkMap(srvB)

			srvB.Shutdown()
			srvA.Shutdown()
			wg.Wait()
		})
	}
}

func getFirstRoute(s *Server) *client {
	for _, conns := range s.routes {
		for _, r := range conns {
			if r != nil {
				return r
			}
		}
	}
	return nil
}

func TestRoutePermsAppliedOnInboundAndOutboundRoute(t *testing.T) {

	perms := &RoutePermissions{
		Import: &SubjectPermission{
			Allow: []string{"imp.foo"},
			Deny:  []string{"imp.bar"},
		},
		Export: &SubjectPermission{
			Allow: []string{"exp.foo"},
			Deny:  []string{"exp.bar"},
		},
	}

	optsA, err := ProcessConfigFile("./configs/seed.conf")
	require_NoError(t, err)
	optsA.NoLog = true
	optsA.NoSigs = true
	optsA.Cluster.Permissions = perms
	srva := RunServer(optsA)
	defer srva.Shutdown()

	optsB := DefaultOptions()
	optsB.Routes = RoutesFromStr(fmt.Sprintf("nats://%s:%d", optsA.Cluster.Host, optsA.Cluster.Port))
	srvb := RunServer(optsB)
	defer srvb.Shutdown()

	checkClusterFormed(t, srva, srvb)

	// Ensure permission is properly set
	check := func(t *testing.T, s *Server) {
		t.Helper()
		var route *client
		s.mu.Lock()
		route = getFirstRoute(s)
		s.mu.Unlock()
		route.mu.Lock()
		perms := route.perms
		route.mu.Unlock()
		if perms == nil {
			t.Fatal("Expected perms to be set")
		}
		if perms.pub.allow == nil || perms.pub.allow.Count() != 1 {
			t.Fatal("unexpected pub allow perms")
		}
		if r := perms.pub.allow.Match("imp.foo"); len(r.psubs) != 1 {
			t.Fatal("unexpected pub allow match")
		}
		if perms.pub.deny == nil || perms.pub.deny.Count() != 1 {
			t.Fatal("unexpected pub deny perms")
		}
		if r := perms.pub.deny.Match("imp.bar"); len(r.psubs) != 1 {
			t.Fatal("unexpected pub deny match")
		}
		if perms.sub.allow == nil || perms.sub.allow.Count() != 1 {
			t.Fatal("unexpected sub allow perms")
		}
		if r := perms.sub.allow.Match("exp.foo"); len(r.psubs) != 1 {
			t.Fatal("unexpected sub allow match")
		}
		if perms.sub.deny == nil || perms.sub.deny.Count() != 1 {
			t.Fatal("unexpected sub deny perms")
		}
		if r := perms.sub.deny.Match("exp.bar"); len(r.psubs) != 1 {
			t.Fatal("unexpected sub deny match")
		}
	}

	// First check when permissions are set on the server accepting the route connection
	check(t, srva)

	srvb.Shutdown()
	srva.Shutdown()

	optsA.Cluster.Permissions = nil
	optsB.Cluster.Permissions = perms

	srva = RunServer(optsA)
	defer srva.Shutdown()

	srvb = RunServer(optsB)
	defer srvb.Shutdown()

	checkClusterFormed(t, srva, srvb)

	// Now check for permissions set on server initiating the route connection
	check(t, srvb)
}

func TestRouteSendLocalSubsWithLowMaxPending(t *testing.T) {
	optsA := DefaultOptions()
	optsA.MaxPayload = 1024
	optsA.MaxPending = 1024
	optsA.NoSystemAccount = true
	srvA := RunServer(optsA)
	defer srvA.Shutdown()

	nc, err := nats.Connect(fmt.Sprintf("nats://%s:%d", optsA.Host, optsA.Port))
	if err != nil {
		t.Fatalf("Error on connect: %v", err)
	}
	defer nc.Close()
	numSubs := 1000
	for i := 0; i < numSubs; i++ {
		subj := fmt.Sprintf("fo.bar.%d", i)
		nc.Subscribe(subj, func(_ *nats.Msg) {})
	}
	checkExpectedSubs(t, numSubs, srvA)

	// Now create a route between B and A
	optsB := DefaultOptions()
	optsB.Routes = RoutesFromStr(fmt.Sprintf("nats://%s:%d", optsA.Cluster.Host, optsA.Cluster.Port))
	optsB.NoSystemAccount = true
	srvB := RunServer(optsB)
	defer srvB.Shutdown()

	checkClusterFormed(t, srvA, srvB)

	// Check that all subs have been sent ok
	checkExpectedSubs(t, numSubs, srvA, srvB)
}

func TestRouteNoCrashOnAddingSubToRoute(t *testing.T) {
	opts := DefaultOptions()
	s := RunServer(opts)
	defer s.Shutdown()

	numRoutes := routeTargetInit + 2
	total := int32(numRoutes)
	count := int32(0)
	ch := make(chan bool, 1)
	cb := func(_ *nats.Msg) {
		if n := atomic.AddInt32(&count, 1); n == total {
			ch <- true
		}
	}

	var servers []*Server
	servers = append(servers, s)

	seedURL := fmt.Sprintf("nats://%s:%d", opts.Cluster.Host, opts.Cluster.Port)
	for i := 0; i < numRoutes; i++ {
		ropts := DefaultOptions()
		ropts.Routes = RoutesFromStr(seedURL)
		rs := RunServer(ropts)
		defer rs.Shutdown()
		servers = append(servers, rs)

		// Confirm routes are active before clients connect.
		for _, srv := range servers {
			rz, err := srv.Routez(nil)
			require_NoError(t, err)
			for i, route := range rz.Routes {
				if route.LastActivity.IsZero() {
					t.Errorf("Expected LastActivity to be valid (%d)", i)
				}
			}
		}

		// Create a sub on each routed server.
		nc := natsConnect(t, fmt.Sprintf("nats://%s:%d", ropts.Host, ropts.Port))
		defer nc.Close()
		natsSub(t, nc, "foo", cb)
	}
	checkClusterFormed(t, servers...)

	// Make sure all subs are registered in s.
	gacc := s.globalAccount()
	gacc.mu.RLock()
	sl := gacc.sl
	gacc.mu.RUnlock()
	checkFor(t, time.Second, 15*time.Millisecond, func() error {
		var _subs [64]*subscription
		subs := _subs[:0]
		sl.All(&subs)
		var ts int
		for _, sub := range subs {
			if string(sub.subject) == "foo" {
				ts++
			}
		}
		if ts != int(numRoutes) {
			return fmt.Errorf("Not all %d routed subs were registered: %d", numRoutes, ts)
		}
		return nil
	})

	pubNC := natsConnect(t, fmt.Sprintf("nats://%s:%d", opts.Host, opts.Port))
	defer pubNC.Close()
	natsPub(t, pubNC, "foo", []byte("hello world!"))

	waitCh(t, ch, "Did not get all messages")
}

func TestRouteRTT(t *testing.T) {
	ob := DefaultOptions()
	ob.PingInterval = 15 * time.Millisecond
	sb := RunServer(ob)
	defer sb.Shutdown()

	oa := DefaultOptions()
	oa.PingInterval = 15 * time.Millisecond
	oa.Routes = RoutesFromStr(fmt.Sprintf("nats://%s:%d", ob.Cluster.Host, ob.Cluster.Port))
	sa := RunServer(oa)
	defer sa.Shutdown()

	checkClusterFormed(t, sa, sb)

	checkRTT := func(t *testing.T, s *Server) time.Duration {
		t.Helper()
		var route *client
		s.mu.Lock()
		route = getFirstRoute(s)
		s.mu.Unlock()

		var rtt time.Duration
		checkFor(t, 2*firstPingInterval, 15*time.Millisecond, func() error {
			route.mu.Lock()
			rtt = route.rtt
			route.mu.Unlock()
			if rtt == 0 {
				return fmt.Errorf("RTT not tracked")
			}
			return nil
		})
		return rtt
	}

	prevA := checkRTT(t, sa)
	prevB := checkRTT(t, sb)

	checkUpdated := func(t *testing.T, s *Server, prev time.Duration) {
		t.Helper()
		attempts := 0
		timeout := time.Now().Add(2 * firstPingInterval)
		for time.Now().Before(timeout) {
			if rtt := checkRTT(t, s); rtt != prev {
				return
			}
			attempts++
			if attempts == 5 {
				// If could be that we are very unlucky
				// and the RTT is constant. So override
				// the route's RTT to 0 to see if it gets
				// updated.
				s.mu.Lock()
				if r := getFirstRoute(s); r != nil {
					r.mu.Lock()
					r.rtt = 0
					r.mu.Unlock()
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
	sb = RunServer(ob)
	defer sb.Shutdown()

	oa = DefaultOptions()
	oa.PingInterval = time.Minute
	oa.Routes = RoutesFromStr(fmt.Sprintf("nats://%s:%d", ob.Cluster.Host, ob.Cluster.Port))
	sa = RunServer(oa)
	defer sa.Shutdown()

	checkClusterFormed(t, sa, sb)
	checkRTT(t, sa)
	checkRTT(t, sb)
}

func TestRouteCloseTLSConnection(t *testing.T) {
	opts := DefaultOptions()
	opts.DisableShortFirstPing = true
	opts.Cluster.Name = "A"
	opts.Cluster.Host = "127.0.0.1"
	opts.Cluster.Port = -1
	opts.Cluster.TLSTimeout = 100
	tc := &TLSConfigOpts{
		CertFile: "./configs/certs/server.pem",
		KeyFile:  "./configs/certs/key.pem",
		Insecure: true,
	}
	tlsConf, err := GenTLSConfig(tc)
	if err != nil {
		t.Fatalf("Error generating tls config: %v", err)
	}
	opts.Cluster.TLSConfig = tlsConf
	opts.NoLog = true
	opts.NoSigs = true
	s := RunServer(opts)
	defer s.Shutdown()

	endpoint := net.JoinHostPort(opts.Cluster.Host, fmt.Sprintf("%d", opts.Cluster.Port))
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
	connectOp := []byte("CONNECT {\"name\":\"route\",\"verbose\":false,\"pedantic\":false,\"tls_required\":true,\"cluster\":\"A\"}\r\n")
	if _, err := tlsConn.Write(connectOp); err != nil {
		t.Fatalf("Unexpected error writing CONNECT: %v", err)
	}
	infoOp := []byte("INFO {\"server_id\":\"route\",\"tls_required\":true}\r\n")
	if _, err := tlsConn.Write(infoOp); err != nil {
		t.Fatalf("Unexpected error writing CONNECT: %v", err)
	}
	if _, err := tlsConn.Write([]byte("PING\r\n")); err != nil {
		t.Fatalf("Unexpected error writing PING: %v", err)
	}

	checkFor(t, time.Second, 15*time.Millisecond, func() error {
		if s.NumRoutes() != 1 {
			return fmt.Errorf("No route registered yet")
		}
		return nil
	})

	// Get route connection
	var route *client
	s.mu.Lock()
	route = getFirstRoute(s)
	s.mu.Unlock()
	// Fill the buffer. We want to timeout on write so that nc.Close()
	// would block due to a write that cannot complete.
	buf := make([]byte, 64*1024)
	done := false
	for !done {
		route.nc.SetWriteDeadline(time.Now().Add(time.Second))
		if _, err := route.nc.Write(buf); err != nil {
			done = true
		}
		route.nc.SetWriteDeadline(time.Time{})
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
	route.closeConnection(SlowConsumerWriteDeadline)
	ch <- true
}

func TestRouteNoPoolRejectsRoutePoolIdx(t *testing.T) {
	for _, test := range []struct {
		name     string
		poolSize int
		info     string
	}{
		{"remote out of range", 0, `"route_pool_size":0,"route_pool_idx":3`},
		{"remote in range", 0, `"route_pool_size":0,"route_pool_idx":2`},
		{"remote negative", 0, `"route_pool_size":0,"route_pool_idx":-1`},
		{"local out of range", -1, `"route_pool_size":3,"route_pool_idx":3`},
	} {
		t.Run(test.name, func(t *testing.T) {
			o := DefaultOptions()
			o.Cluster.Name = "A"
			o.Cluster.Host = "127.0.0.1"
			o.Cluster.Port = -1
			o.Cluster.PoolSize = test.poolSize
			s := RunServer(o)
			defer s.Shutdown()

			conn, err := net.DialTimeout("tcp", net.JoinHostPort(o.Cluster.Host, fmt.Sprintf("%d", o.Cluster.Port)), 2*time.Second)
			require_NoError(t, err)
			defer conn.Close()

			_, err = conn.Write([]byte("CONNECT {\"name\":\"route\",\"verbose\":false,\"pedantic\":false,\"cluster\":\"A\"}\r\n"))
			require_NoError(t, err)
			_, err = conn.Write(fmt.Appendf(nil, "INFO {\"server_id\":\"route\",\"name\":\"route\",\"cluster\":\"A\",%s}\r\n", test.info))
			require_NoError(t, err)

			// The route must be rejected as a protocol violation.
			require_NoError(t, conn.SetReadDeadline(time.Now().Add(2*time.Second)))
			br := bufio.NewReader(conn)
			for {
				line, err := br.ReadString('\n')
				require_NoError(t, err)
				if strings.HasPrefix(line, "-ERR") {
					require_Contains(t, line, "Route pool index should not be set")
					break
				}
			}
			require_True(t, s.Running())
			require_Equal(t, s.NumRoutes(), 0)
		})
	}
}

func TestRouteClusterNameConflictBetweenStaticAndDynamic(t *testing.T) {
	o1 := DefaultOptions()
	o1.Cluster.Name = "AAAAAAAAAAAAAAAAAAAA" // make it alphabetically the "smallest"
	s1 := RunServer(o1)
	defer s1.Shutdown()

	o2 := DefaultOptions()
	o2.Cluster.Name = "" // intentional, let it be assigned dynamically
	o2.Routes = RoutesFromStr(fmt.Sprintf("nats://127.0.0.1:%d", o1.Cluster.Port))
	s2 := RunServer(o2)
	defer s2.Shutdown()

	checkClusterFormed(t, s1, s2)
}

type testRouteResolver struct{}

func (r *testRouteResolver) LookupHost(ctx context.Context, host string) ([]string, error) {
	return []string{"127.0.0.1", "other.host.in.cluster"}, nil
}

type routeHostLookupLogger struct {
	DummyLogger
	errCh chan string
	ch    chan bool
	count int
}

func (l *routeHostLookupLogger) Debugf(format string, v ...any) {
	l.Lock()
	defer l.Unlock()
	msg := fmt.Sprintf(format, v...)
	if strings.Contains(msg, "127.0.0.1:1234") {
		l.errCh <- msg
	} else if strings.Contains(msg, "other.host.in.cluster") {
		if l.count++; l.count == 10 {
			l.ch <- true
		}
	}
}

func TestRouteIPResolutionAndRouteToSelf(t *testing.T) {
	o := DefaultOptions()
	o.Cluster.Port = 1234
	r := &testRouteResolver{}
	o.Cluster.resolver = r
	o.Routes = RoutesFromStr("nats://routehost:1234")
	o.Debug = true
	o.NoLog = false
	s, err := NewServer(o)
	if err != nil {
		t.Fatalf("Error creating server: %v", err)
	}
	defer s.Shutdown()
	l := &routeHostLookupLogger{errCh: make(chan string, 1), ch: make(chan bool, 1)}
	s.SetLogger(l, true, true)
	s.Start()
	if err := s.readyForConnections(time.Second); err != nil {
		t.Fatal(err)
	}

	select {
	case e := <-l.errCh:
		t.Fatalf("Unexpected trace: %q", e)
	case <-l.ch:
		// Ok
		return
	}
}

func TestRouteDuplicateServerName(t *testing.T) {
	o := DefaultOptions()
	o.ServerName = "A"
	s := RunServer(o)
	defer s.Shutdown()

	l := &captureErrorLogger{errCh: make(chan string, 1)}
	s.SetLogger(l, false, false)

	o2 := DefaultOptions()
	// Set the same server name on purpose
	o2.ServerName = "A"
	o2.Routes = RoutesFromStr(fmt.Sprintf("nats://127.0.0.1:%d", o.Cluster.Port))
	s2 := RunServer(o2)
	defer s2.Shutdown()

	// This is an error now so can't wait on cluster formed.
	select {
	case w := <-l.errCh:
		if !strings.Contains(w, "Remote server has a duplicate name") {
			t.Fatalf("Expected warning about same name, got %q", w)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Should have gotten a warning regarding duplicate server name")
	}
}

func TestRouteLockReleasedOnTLSFailure(t *testing.T) {
	o1 := DefaultOptions()
	o1.Cluster.Name = "abc"
	o1.Cluster.Host = "127.0.0.1"
	o1.Cluster.Port = -1
	o1.Cluster.TLSTimeout = 0.25
	tc := &TLSConfigOpts{
		CertFile: "./configs/certs/server.pem",
		KeyFile:  "./configs/certs/key.pem",
		Insecure: true,
	}
	tlsConf, err := GenTLSConfig(tc)
	if err != nil {
		t.Fatalf("Error generating tls config: %v", err)
	}
	o1.Cluster.TLSConfig = tlsConf
	s1 := RunServer(o1)
	defer s1.Shutdown()

	l := &captureErrorLogger{errCh: make(chan string, 10)}
	s1.SetLogger(l, false, false)

	o2 := DefaultOptions()
	o2.Routes = RoutesFromStr(fmt.Sprintf("nats://127.0.0.1:%d", o1.Cluster.Port))
	s2 := RunServer(o2)
	defer s2.Shutdown()

	select {
	case err := <-l.errCh:
		if !strings.Contains(err, "TLS") {
			t.Fatalf("Unexpected error: %v", err)
		}
	case <-time.After(time.Second):
	}

	s2.Shutdown()

	// Wait for longer than the TLS timeout and check that tlsTimeout is not stuck
	time.Sleep(500 * time.Millisecond)

	buf := make([]byte, 10000)
	n := runtime.Stack(buf, true)
	if bytes.Contains(buf[:n], []byte("tlsTimeout")) {
		t.Fatal("Seem connection lock was not released")
	}
}

type localhostResolver struct{}

func (r *localhostResolver) LookupHost(ctx context.Context, host string) ([]string, error) {
	return []string{"127.0.0.1"}, nil
}

func TestTLSRoutesCertificateImplicitAllowPass(t *testing.T) {
	testTLSRoutesCertificateImplicitAllow(t, true)
}

func TestTLSRoutesCertificateImplicitAllowFail(t *testing.T) {
	testTLSRoutesCertificateImplicitAllow(t, false)
}

func testTLSRoutesCertificateImplicitAllow(t *testing.T, pass bool) {
	// Base config for the servers
	cfg := createTempFile(t, "cfg")
	cfg.WriteString(fmt.Sprintf(`
		cluster {
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

	routeURLs := "nats://localhost:9935, nats://localhost:9936"
	if !pass {
		routeURLs = "nats://127.0.0.1:9935, nats://127.0.0.1:9936"
	}
	optsA.Host = "127.0.0.1"
	optsA.Port = 9335
	optsA.Cluster.Name = "xyz"
	optsA.Cluster.Host = optsA.Host
	optsA.Cluster.Port = 9935
	optsA.Cluster.resolver = &localhostResolver{}
	optsA.Routes = RoutesFromStr(routeURLs)
	optsA.NoSystemAccount = true
	srvA := RunServer(optsA)
	defer srvA.Shutdown()

	optsB.Host = "127.0.0.1"
	optsB.Port = 9336
	optsB.Cluster.Name = "xyz"
	optsB.Cluster.Host = optsB.Host
	optsB.Cluster.Port = 9936
	optsB.Cluster.resolver = &localhostResolver{}
	optsB.Routes = RoutesFromStr(routeURLs)
	optsB.NoSystemAccount = true
	srvB := RunServer(optsB)
	defer srvB.Shutdown()

	if pass {
		checkNumRoutes(t, srvA, DEFAULT_ROUTE_POOL_SIZE)
		checkNumRoutes(t, srvB, DEFAULT_ROUTE_POOL_SIZE)
	} else {
		time.Sleep(1 * time.Second) // the fail case uses the IP, so a short wait is sufficient
		checkFor(t, 2*time.Second, 15*time.Millisecond, func() error {
			if srvA.NumRoutes() != 0 || srvB.NumRoutes() != 0 {
				return fmt.Errorf("No route connection expected")
			}
			return nil
		})
	}
}

func TestSubjectRenameViaJetStreamAck(t *testing.T) {
	s := RunRandClientPortServer(t)
	defer s.Shutdown()
	errChan := make(chan error)
	defer close(errChan)
	ncPub := natsConnect(t, s.ClientURL(), nats.UserInfo("client", "pwd"),
		nats.ErrorHandler(func(conn *nats.Conn, s *nats.Subscription, err error) {
			errChan <- err
		}))
	defer ncPub.Close()
	require_NoError(t, ncPub.PublishRequest("SVC.ALLOWED", "$JS.ACK.whatever@ADMIN", nil))
	select {
	case err := <-errChan:
		require_Contains(t, err.Error(), "Permissions Violation for Publish with Reply of")
	case <-time.After(time.Second):
		t.Fatalf("Expected error")
	}
}

func TestClusterQueueGroupWeightTrackingLeak(t *testing.T) {
	o := DefaultOptions()
	o.ServerName = "A"
	s := RunServer(o)
	defer s.Shutdown()

	o2 := DefaultOptions()
	o2.ServerName = "B"
	o2.Routes = RoutesFromStr(fmt.Sprintf("nats://127.0.0.1:%d", o.Cluster.Port))
	s2 := RunServer(o2)
	defer s2.Shutdown()

	nc := natsConnect(t, s.ClientURL())
	defer nc.Close()

	// Create a queue subscription
	sub := natsQueueSubSync(t, nc, "foo", "bar")

	// Check on s0 that we have the proper queue weight info
	acc := s.GlobalAccount()

	check := func(present bool, expected int32) {
		t.Helper()
		sub := subscription{subject: []byte("foo"), queue: []byte("bar")}
		key := keyFromSubWithOrigin(&sub)
		checkFor(t, time.Second, 15*time.Millisecond, func() error {
			acc.mu.RLock()
			v, ok := acc.lws[key]
			acc.mu.RUnlock()
			if present {
				if !ok {
					return fmt.Errorf("the key is not present")
				}
				if v != expected {
					return fmt.Errorf("lws does not contain expected value of %v: %v", expected, v)
				}
			} else if ok {
				return fmt.Errorf("the key is present with value %v and should not be", v)
			}
			return nil
		})
	}
	check(true, 1)

	// Now unsub, and it should be removed, not just be 0
	sub.Unsubscribe()
	check(false, 0)

	// Still make sure that the subject interest is gone from both servers.
	checkSubGone := func(s *Server) {
		t.Helper()
		checkFor(t, time.Second, 15*time.Millisecond, func() error {
			acc := s.GlobalAccount()
			acc.mu.RLock()
			res := acc.sl.Match("foo")
			acc.mu.RUnlock()
			if res != nil && len(res.qsubs) > 0 {
				return fmt.Errorf("Found queue sub on foo for server %v", s)
			}
			return nil
		})
	}
	checkSubGone(s)
	checkSubGone(s2)
}

type testRouteReconnectLogger struct {
	DummyLogger
	ch chan string
}

func (l *testRouteReconnectLogger) Debugf(format string, v ...any) {
	msg := fmt.Sprintf(format, v...)
	if strings.Contains(msg, "Trying to connect to route") {
		select {
		case l.ch <- msg:
		default:
		}
	}
}

func TestRouteSolicitedReconnectsEvenIfImplicit(t *testing.T) {
	o1 := DefaultOptions()
	o1.ServerName = "A"
	s1 := RunServer(o1)
	defer s1.Shutdown()

	o2 := DefaultOptions()
	o2.ServerName = "B"
	o2.Routes = RoutesFromStr(fmt.Sprintf("nats://127.0.0.1:%d", o1.Cluster.Port))
	// Not strictly required to reconnect, but if the reconnect were to fail for any reason
	// then the server would retry only once and then stops. So set it to some higher value
	// and then we will check that the server does not try more than that.
	o2.Cluster.ConnectRetries = 3
	s2 := RunServer(o2)
	defer s2.Shutdown()

	o3 := DefaultOptions()
	o3.ServerName = "C"
	o3.Routes = RoutesFromStr(fmt.Sprintf("nats://127.0.0.1:%d", o1.Cluster.Port))
	o3.Cluster.ConnectRetries = 3
	s3 := RunServer(o3)
	defer s3.Shutdown()

	checkClusterFormed(t, s1, s2, s3)

	s2.mu.Lock()
	s2.forEachRoute(func(r *client) {
		r.mu.Lock()
		// Close the route between S2 and S3 (that do not have explicit route to each other)
		if r.route.remoteID == s3.ID() {
			r.nc.Close()
		}
		r.mu.Unlock()
	})
	s2.mu.Unlock()
	// Wait a bit to make sure that we don't check for cluster formed too soon (need to make
	// sure that connection is really removed and reconnect mechanism starts).
	time.Sleep(500 * time.Millisecond)
	checkClusterFormed(t, s1, s2, s3)

	// Now shutdown server 3 and make sure that s2 stops trying to reconnect to s3 at one point
	l := &testRouteReconnectLogger{ch: make(chan string, 10)}
	s2.SetLogger(l, true, false)
	s3.Shutdown()
	// S2 should retry ConnectRetries+1 times and then stop
	// Take into account default route pool size and system account dedicated route
	for i := 0; i < (DEFAULT_ROUTE_POOL_SIZE+1)*(o2.Cluster.ConnectRetries+1); i++ {
		select {
		case <-l.ch:
		case <-time.After(2 * time.Second):
			t.Fatal("Did not attempt to reconnect")
		}
	}
	// Now it should have stopped (in tests, reconnect delay is down to 15ms, so we don't need
	// to wait for too long).
	select {
	case msg := <-l.ch:
		t.Fatalf("Unexpected attempt to reconnect: %s", msg)
	case <-time.After(50 * time.Millisecond):
		// OK
	}
}

func TestRouteReconnectExponentialBackoff(t *testing.T) {
	oRouteConnectDelay := routeConnectDelay
	oRouteConnectMaxDelay := routeConnectMaxDelay
	routeConnectDelay = 500 * time.Millisecond
	routeConnectMaxDelay = 2 * time.Second
	defer func() {
		routeConnectDelay = oRouteConnectDelay
		routeConnectMaxDelay = oRouteConnectMaxDelay
	}()

	o1 := DefaultOptions()
	o1.ServerName = "A"
	s1 := RunServer(o1)
	defer s1.Shutdown()

	o2 := DefaultOptions()
	o2.ServerName = "B"
	o2.Routes = RoutesFromStr(fmt.Sprintf("nats://127.0.0.1:%d", o1.Cluster.Port))
	o2.Cluster.ConnectRetries = 3
	o2.Cluster.ConnectBackoff = true
	s2 := RunServer(o2)
	defer s2.Shutdown()

	checkClusterFormed(t, s1, s2)

	// Now shutdown server 1 and make sure that s2 stops trying to reconnect to s1 at one point
	l := &testRouteReconnectLogger{ch: make(chan string, 10)}
	s2.SetLogger(l, true, false)

	// Remove initial delay before reconnect, and allow for some skew.
	now := time.Now().Add(routeReconnectDelay).Add(-100 * time.Millisecond)
	var delay time.Duration

	// S2 should retry ConnectRetries+1 times and then stop
	// Take into account default route pool size and system account dedicated route
	s1.Shutdown()
	for i := 0; i < (DEFAULT_ROUTE_POOL_SIZE+1)*(o2.Cluster.ConnectRetries+1); i++ {
		select {
		case <-l.ch:
			if since := time.Since(now); since < delay {
				t.Fatalf("Expected delay to take %v, took %v", delay, since)
			}
			if i > 0 && (i+1)%(DEFAULT_ROUTE_POOL_SIZE+1) == 0 {
				if delay == 0 {
					delay = routeConnectDelay
				} else {
					delay *= 2
				}
				if delay > routeConnectMaxDelay {
					delay = routeConnectMaxDelay
				}
			}
		case <-time.After(routeConnectMaxDelay + time.Second):
			t.Fatal("Did not attempt to reconnect")
		}
	}
}

func TestRouteSaveTLSName(t *testing.T) {
	c1Conf := createConfFile(t, []byte(`
		port: -1
		cluster {
			name: "abc"
			port: -1
			pool_size: -1
			tls {
				cert_file: '../test/configs/certs/server-noip.pem'
				key_file: '../test/configs/certs/server-key-noip.pem'
				ca_file: '../test/configs/certs/ca.pem'
			}
		}
	`))
	s1, o1 := RunServerWithConfig(c1Conf)
	defer s1.Shutdown()

	tmpl := `
	port: -1
	cluster {
		name: "abc"
		port: -1
		pool_size: -1
		routes: ["nats://%s:%d"]
		tls {
			cert_file: '../test/configs/certs/server-noip.pem'
			key_file: '../test/configs/certs/server-key-noip.pem'
			ca_file: '../test/configs/certs/ca.pem'
		}
	}
	`
	c2And3Conf := createConfFile(t, []byte(fmt.Sprintf(tmpl, "localhost", o1.Cluster.Port)))
	s2, _ := RunServerWithConfig(c2And3Conf)
	defer s2.Shutdown()

	checkClusterFormed(t, s1, s2)

	s3, _ := RunServerWithConfig(c2And3Conf)
	defer s3.Shutdown()

	checkClusterFormed(t, s1, s2, s3)

	reloadUpdateConfig(t, s2, c2And3Conf, fmt.Sprintf(tmpl, "127.0.0.1", o1.Cluster.Port))

	s2.mu.RLock()
	s2.forEachRoute(func(r *client) {
		r.mu.Lock()
		if r.route.routeType == Implicit {
			r.nc.Close()
		}
		r.mu.Unlock()
	})
	s2.mu.RUnlock()

	checkClusterFormed(t, s1, s2, s3)

	// Set a logger to capture errors trying to connect after clearing
	// the routeTLSName and causing a disconnect
	l := &captureErrorLogger{errCh: make(chan string, 1)}
	s2.SetLogger(l, false, false)

	var gotIt bool
	for i := 0; !gotIt && i < 5; i++ {
		s2.mu.Lock()
		s2.routeTLSName = _EMPTY_
		s2.forEachRoute(func(r *client) {
			r.mu.Lock()
			if r.route.routeType == Implicit {
				r.nc.Close()
			}
			r.mu.Unlock()
		})
		s2.mu.Unlock()
		select {
		case <-l.errCh:
			gotIt = true
		case <-time.After(time.Second):
			// Try again
		}
	}
	if !gotIt {
		t.Fatal("Did not get the handshake error")
	}

	// Now get back to localhost in config and reload config and
	// it should start to work again.
	reloadUpdateConfig(t, s2, c2And3Conf, fmt.Sprintf(tmpl, "localhost", o1.Cluster.Port))
	checkClusterFormed(t, s1, s2, s3)
}

func TestRouteImplicitUsesAdvertisedURLForTLSName(t *testing.T) {
	c1Conf := createConfFile(t, []byte(`
		port: -1
		cluster {
			name: "abc"
			port: -1
			pool_size: -1
			advertise: "localhost"
			tls {
				cert_file: '../test/configs/certs/server-noip.pem'
				key_file: '../test/configs/certs/server-key-noip.pem'
				ca_file: '../test/configs/certs/ca.pem'
			}
		}
	`))
	s1, o1 := RunServerWithConfig(c1Conf)
	defer s1.Shutdown()

	tmpl := `
	port: -1
	cluster {
		name: "abc"
		port: -1
		pool_size: -1
		routes: ["nats://localhost:%d"]
		%s
		tls {
			cert_file: '../test/configs/certs/server-noip.pem'
			key_file: '../test/configs/certs/server-key-noip.pem'
			ca_file: '../test/configs/certs/ca.pem'
		}
	}
	`
	c2And3Conf := createConfFile(t, fmt.Appendf(nil, tmpl, o1.Cluster.Port, _EMPTY_))
	s2, _ := RunServerWithConfig(c2And3Conf)
	defer s2.Shutdown()

	checkClusterFormed(t, s1, s2)

	// For this test, we need to clear the saved TLS name to make sure
	// that the implicit route fails since the connection will be made with
	// an IP instead of a hostname.
	s2.mu.Lock()
	s2.routeTLSName = _EMPTY_
	s2.mu.Unlock()

	// Set a logger to capture error indicating that s2 can't connect to the
	// discovered server s3 because it connects using an IP and the certs
	// don't allow for that.
	l := &captureErrorLogger{errCh: make(chan string, 1)}
	s2.SetLogger(l, false, false)

	s3, _ := RunServerWithConfig(c2And3Conf)
	defer s3.Shutdown()

	var gotIt bool
	for i := 0; !gotIt && i < 5; i++ {
		select {
		case err := <-l.errCh:
			if strings.Contains(err, "handshake") {
				gotIt = true
			}
		case <-time.After(2 * time.Second):
			t.Fatal("Timed-out waiting for handshake error")
		}
	}
	if !gotIt {
		t.Fatal("Did not get the handshake error")
	}

	// Stop the servers and update the configuration to have the advertise
	// address set to "localhost".
	s2.Shutdown()
	s3.Shutdown()
	checkFor(t, time.Second, 15*time.Millisecond, func() error {
		if n := s1.NumRoutes(); n != 0 {
			return fmt.Errorf("Server s1 has still %v routes", n)
		}
		return nil
	})

	c2And3Conf = createConfFile(t, fmt.Appendf(nil, tmpl, o1.Cluster.Port, "advertise: localhost"))
	s2, _ = RunServerWithConfig(c2And3Conf)
	defer s2.Shutdown()

	checkClusterFormed(t, s1, s2)

	s2.mu.Lock()
	s2.routeTLSName = _EMPTY_
	s2.mu.Unlock()

	s3, _ = RunServerWithConfig(c2And3Conf)
	defer s3.Shutdown()

	checkClusterFormed(t, s1, s2, s3)
}

func TestRoutePoolAndPerAccountErrors(t *testing.T) {
	conf := createConfFile(t, []byte(`
		port: -1
		cluster {
			port: -1
			accounts: ["abc", "def", "abc"]
		}
	`))
	o := LoadConfig(conf)
	if _, err := NewServer(o); err == nil || !strings.Contains(err.Error(), "duplicate") {
		t.Fatalf("Expected error about duplicate, got %v", err)
	}

	conf1 := createConfFile(t, []byte(`
		port: -1
		accounts {
			abc { users: [{user:abc, password: pwd}] }
			def { users: [{user:def, password: pwd}] }
		}
		cluster {
			port: -1
			name: "local"
			accounts: ["abc"]
		}
	`))
	s1, o1 := RunServerWithConfig(conf1)
	defer s1.Shutdown()

	l := &captureErrorLogger{errCh: make(chan string, 10)}
	s1.SetLogger(l, false, false)

	conf2 := createConfFile(t, []byte(fmt.Sprintf(`
		port: -1
		accounts {
			abc { users: [{user:abc, password: pwd}] }
			def { users: [{user:def, password: pwd}] }
		}
		cluster {
			port: -1
			name: "local"
			routes: ["nats://127.0.0.1:%d"]
			accounts: ["def"]
		}
	`, o1.Cluster.Port)))
	s2, _ := RunServerWithConfig(conf2)
	defer s2.Shutdown()

	for i := 0; i < 2; i++ {
		select {
		case e := <-l.errCh:
			if !strings.Contains(e, "No route for account \"def\"") {
				t.Fatalf("Expected error about no route for account, got %v", e)
			}
		case <-time.After(2 * time.Second):
			t.Fatalf("Did not get expected error regarding no route for account")
		}
		time.Sleep(routeReconnectDelay + 100*time.Millisecond)
	}
}

func TestRoutePool(t *testing.T) {
	tmpl := `
		port: -1
		accounts {
			A { users: [{user: "a", password: "a"}] }
			B { users: [{user: "b", password: "b"}] }
		}
		cluster {
			port: -1
			name: "local"
			%s
			pool_size: 2
		}
	`
	conf1 := createConfFile(t, []byte(fmt.Sprintf(tmpl, _EMPTY_)))
	s1, o1 := RunServerWithConfig(conf1)
	defer s1.Shutdown()

	conf2 := createConfFile(t, []byte(fmt.Sprintf(tmpl,
		fmt.Sprintf("routes: [\"nats://127.0.0.1:%d\"]", o1.Cluster.Port))))
	s2, _ := RunServerWithConfig(conf2)
	defer s2.Shutdown()

	checkClusterFormed(t, s1, s2)

	checkRoutePoolIdx := func(s *Server, accName string, expected int) {
		t.Helper()
		a, err := s.LookupAccount(accName)
		require_NoError(t, err)
		require_True(t, a != nil)
		a.mu.RLock()
		rpi := a.routePoolIdx
		a.mu.RUnlock()
		require_True(t, rpi == expected)
	}
	checkRoutePoolIdx(s1, "A", 0)
	checkRoutePoolIdx(s2, "A", 0)
	checkRoutePoolIdx(s2, "B", 1)
	checkRoutePoolIdx(s2, "B", 1)

	sendAndRecv := func(acc, user, pwd string) {
		t.Helper()
		s2nc := natsConnect(t, s2.ClientURL(), nats.UserInfo(user, pwd))
		defer s2nc.Close()

		sub := natsSubSync(t, s2nc, "foo")
		natsFlush(t, s2nc)

		s1nc := natsConnect(t, s1.ClientURL(), nats.UserInfo(user, pwd))
		defer s1nc.Close()

		checkSubInterest(t, s1, acc, "foo", time.Second)

		for i := 0; i < 1000; i++ {
			natsPub(t, s1nc, "foo", []byte("hello"))
		}
		for i := 0; i < 1000; i++ {
			natsNexMsg(t, sub, time.Second)
		}
		// Make sure we don't receive more
		if msg, err := sub.NextMsg(150 * time.Millisecond); err == nil {
			t.Fatalf("Unexpected message: %+v", msg)
		}
	}

	sendAndRecv("A", "a", "a")
	sendAndRecv("B", "b", "b")

	checkStats := func(s *Server, isOut bool) {
		t.Helper()
		s.mu.RLock()
		defer s.mu.RUnlock()
		for _, conns := range s.routes {
			for i, r := range conns {
				r.mu.Lock()
				if isOut {
					if v := r.stats.outMsgs; v < 1000 {
						r.mu.Unlock()
						t.Fatalf("Expected at least 1000 in out msgs for route %v, got %v", i+1, v)
					}
				} else {
					if v := r.stats.inMsgs; v < 1000 {
						r.mu.Unlock()
						t.Fatalf("Expected at least 1000 in msgs for route %v, got %v", i+1, v)
					}
				}
				r.mu.Unlock()
			}
		}
	}
	checkStats(s1, true)
	checkStats(s2, false)

	disconnectRoute := func(s *Server, idx int) {
		t.Helper()
		attempts := 0
	TRY_AGAIN:
		s.mu.RLock()
		for _, conns := range s.routes {
			for i, r := range conns {
				if i != idx {
					continue
				}
				if r != nil {
					r.mu.Lock()
					nc := r.nc
					r.mu.Unlock()
					if nc == nil {
						s.mu.RUnlock()
						if attempts++; attempts < 10 {
							time.Sleep(250 * time.Millisecond)
							goto TRY_AGAIN
						}
						t.Fatalf("Route %v net.Conn is nil", i)
					}
					nc.Close()
				} else {
					s.mu.RUnlock()
					if attempts++; attempts < 10 {
						time.Sleep(250 * time.Millisecond)
						goto TRY_AGAIN
					}
					t.Fatalf("Route %v connection is nil", i)
				}
			}
		}
		s.mu.RUnlock()
		time.Sleep(250 * time.Millisecond)
		checkClusterFormed(t, s1, s2)
	}
	disconnectRoute(s1, 0)
	disconnectRoute(s2, 1)
}

func TestRoutePoolConnectRace(t *testing.T) {
	for _, test := range []struct {
		name     string
		poolSize int
	}{
		{"no pool", -1},
		{"pool size 1", 1},
		{"pool size 5", 5},
	} {
		t.Run(test.name, func(t *testing.T) {
			// This test will have each server point to each other and that is causing
			// each one to attempt to connect routes to each other which should lead
			// to connections needing to be dropped. We make sure that there is still
			// resolution and there is the expected number of routes.
			createSrv := func(name string, port int) *Server {
				o := DefaultOptions()
				o.Port = -1
				o.ServerName = name
				o.Cluster.PoolSize = test.poolSize
				o.Cluster.Name = "local"
				o.Cluster.Port = port
				o.Routes = RoutesFromStr("nats://127.0.0.1:1234,nats://127.0.0.1:1235,nats://127.0.0.1:1236")
				s, err := NewServer(o)
				if err != nil {
					t.Fatalf("Error creating server: %v", err)
				}
				return s
			}
			s1 := createSrv("A", 1234)
			s2 := createSrv("B", 1235)
			s3 := createSrv("C", 1236)

			l := &captureDebugLogger{dbgCh: make(chan string, 100)}
			s1.SetLogger(l, true, false)

			servers := []*Server{s1, s2, s3}

			for _, s := range servers {
				go s.Start()
				defer s.Shutdown()
			}

			checkClusterFormed(t, s1, s2, s3)

			for done, duplicate := false, 0; !done; {
				select {
				case e := <-l.dbgCh:
					if strings.Contains(e, "duplicate") {
						if duplicate++; duplicate > 20 {
							t.Fatalf("Routes are constantly reconnecting: %v", e)
						}
					}
				case <-time.After(routeReconnectDelay + 500*time.Millisecond):
					// More than reconnect and some, and no reconnect, so we are good.
					done = true
				}
			}

			// Also, check that they all report as solicited and configured in monitoring.
			for _, s := range servers {
				routes, err := s.Routez(nil)
				require_NoError(t, err)
				for _, r := range routes.Routes {
					if !r.DidSolicit {
						t.Fatalf("All routes should have been marked as solicited, this one was not: %+v", r)
					}
					if !r.IsConfigured {
						t.Fatalf("All routes should have been marked as configured, this one was not: %+v", r)
					}
				}
			}

			for _, s := range servers {
				s.Shutdown()
				s.WaitForShutdown()
			}
		})
	}
}

func TestRoutePoolRouteStoredSameIndexBothSides(t *testing.T) {
	tmpl := `
		port: -1
		accounts {
			A { users: [{user: "a", password: "a"}] }
			B { users: [{user: "b", password: "b"}] }
			C { users: [{user: "c", password: "c"}] }
			D { users: [{user: "d", password: "d"}] }
		}
		cluster {
			port: -1
			name: "local"
			%s
			pool_size: 4
		}
		no_sys_acc: true
	`
	conf1 := createConfFile(t, []byte(fmt.Sprintf(tmpl, _EMPTY_)))
	s1, o1 := RunServerWithConfig(conf1)
	defer s1.Shutdown()

	conf2 := createConfFile(t, []byte(fmt.Sprintf(tmpl,
		fmt.Sprintf("routes: [\"nats://127.0.0.1:%d\"]", o1.Cluster.Port))))
	s2, _ := RunServerWithConfig(conf2)
	defer s2.Shutdown()

	for i := 0; i < 20; i++ {
		checkClusterFormed(t, s1, s2)

		collect := func(s *Server, checkRemoteAddr bool) []string {
			addrs := make([]string, 0, 4)
			s.mu.RLock()
			s.forEachRoute(func(r *client) {
				var addr string
				r.mu.Lock()
				if r.nc != nil {
					if checkRemoteAddr {
						addr = r.nc.RemoteAddr().String()
					} else {
						addr = r.nc.LocalAddr().String()
					}
					addrs = append(addrs, addr)
				}
				r.mu.Unlock()
			})
			s.mu.RUnlock()
			return addrs
		}

		addrsS1 := collect(s1, true)
		addrsS2 := collect(s2, false)
		if len(addrsS1) != 4 || len(addrsS2) != 4 {
			// It could be that connections were not ready (r.nc is nil in collect())
			// if that is the case, try again.
			i--
			continue
		}

		if !reflect.DeepEqual(addrsS1, addrsS2) {
			t.Fatalf("Connections not stored at same index:\ns1=%v\ns2=%v", addrsS1, addrsS2)
		}

		s1.mu.RLock()
		s1.forEachRoute(func(r *client) {
			r.mu.Lock()
			if r.nc != nil {
				r.nc.Close()
			}
			r.mu.Unlock()
		})
		s1.mu.RUnlock()
	}
}

func TestRoutePoolSizeDifferentOnEachServer(t *testing.T) {
	tmpl := `
		port: -1
		server_name: "%s"
		accounts {
			A { users: [{user: "A", password: "pwd"}] }
			B { users: [{user: "B", password: "pwd"}] }
			C { users: [{user: "C", password: "pwd"}] }
			D { users: [{user: "D", password: "pwd"}] }
		}
		cluster {
			port: -1
			name: "local"
			%s
			pool_size: %d
			accounts: [%s]
		}
		no_sys_acc: true
	`
	conf1 := createConfFile(t, fmt.Appendf(nil, tmpl, "S1", _EMPTY_, 3, `"A"`))
	s1, o1 := RunServerWithConfig(conf1)
	defer s1.Shutdown()

	conf2 := createConfFile(t, fmt.Appendf(nil, tmpl, "S2",
		fmt.Sprintf("routes: [\"nats://127.0.0.1:%d\"]", o1.Cluster.Port), 2, `"A"`))
	s2, o2 := RunServerWithConfig(conf2)
	defer s2.Shutdown()

	conf3 := createConfFile(t, fmt.Appendf(nil, tmpl, "S3",
		fmt.Sprintf("routes: [\"nats://127.0.0.1:%d\"]", o1.Cluster.Port), 4, `"A"`))
	s3, o3 := RunServerWithConfig(conf3)
	defer s3.Shutdown()

	// Since each one has a different configured pool size, we are not going
	// to use checkClusterFormed, but use a low level checFor here.
	servers := []*Server{s1, s2, s3}

	// Both S2 and S3 solicit connections to S1, and with the order the servers
	// are started, S2 will implicitly connect to S3. With that in mind, the
	// expected number of routes for each server will be as such:
	// S1: when S2 solicits 2+1 routes, it will detect that S1 is configured
	// for 3+1, so S1<->S2 will have the max of the two, which is 4 routes,
	// then when S3 solicits its 4+1 routes, the total will be 4+5=9 routes.
	// S2: it solicits 2+1 but since S1 is configured for 3+1, it will have
	// 3+1 routes to S1, and when being told to connect to S3, it will start
	// with 2+1 but since S3 is configured for 4+1, it will have that many
	// connections, so total is 4+5=9 too.
	// S3: it solicits 4+1 routes to S1, so S1 although configured for 3+1
	// will accept the 4+1, and when S2 implicitly connects to S3, it will
	// force S2 to create 4+1 connections too. So here the total is 5+5=10.
	expected := []int{9, 9, 10}
	checkCluster := func() {
		t.Helper()
		// In most case, we will do a config reload and want to give time
		// for connections to be closed before checking.
		time.Sleep(50 * time.Millisecond)
		for i, s := range servers {
			if !s.isRunning() {
				continue
			}
			checkFor(t, 10*time.Second, 25*time.Millisecond, func() error {
				if numRoutes := s.NumRoutes(); numRoutes != expected[i] {
					return fmt.Errorf("Expected %d routes for server %q, got %d", expected[i], s, numRoutes)
				}
				return nil
			})
		}
	}
	checkCluster()

	// We will create a subscription per account per server.
	accs := []string{"A", "B", "C", "D"}
	var aconns [][]*nats.Conn
	var asubs [][]*nats.Subscription
	for _, acc := range accs {
		var subs []*nats.Subscription
		var conns []*nats.Conn
		for _, s := range servers {
			nc := natsConnect(t, s.ClientURL(), nats.UserInfo(acc, "pwd"))
			defer nc.Close()
			conns = append(conns, nc)
			sub := natsSubSync(t, nc, "foo")
			subs = append(subs, sub)
		}
		asubs = append(asubs, subs)
		aconns = append(aconns, conns)
	}

	// Now we will check that a message produced on each account from
	// each server reaches all subs.
	checkRecv := func(payload string) {
		t.Helper()
		// We first to check the interest on all servers for each account
		for _, s := range servers {
			for _, acc := range accs {
				checkSubInterest(t, s, acc, "foo", time.Second)
			}
		}

		// Now from each server, and each account, send a message and check
		// that all subs for this account receive the message.
		for _, s := range servers {
			for i, acc := range accs {
				nc := natsConnect(t, s.ClientURL(), nats.UserInfo(acc, "pwd"))
				defer nc.Close()

				natsPub(t, nc, "foo", []byte(payload))
				for j, sub := range asubs[i] {
					if _, err := sub.NextMsg(time.Second); err != nil {
						t.Fatalf("Producer on %q - account=%q - sub on server %q - err=%v",
							s, acc, servers[j], err)
					}
					// Wait a tiny bit and check that there is not a duplicate
					if msg, err := sub.NextMsg(10 * time.Millisecond); err == nil {
						t.Fatalf("Producer on %q - account=%q - sub on server %q received a duplicate msg=%s",
							s, acc, servers[j], msg.Data)
					}
				}
			}
		}
	}
	checkRecv("hello1")

	// Now that the 3 servers are up and we know their route port, we will
	// update the config of each server so that they have routes that point
	// to each other (so each server will solicit) and for routes to be
	// recreated, then test again.
	reloadUpdateConfig(t, s1, conf1, fmt.Sprintf(tmpl, "S1",
		fmt.Sprintf("routes:[\"nats://127.0.0.1:%d\",\"nats://127.0.0.1:%d\"]", o2.Cluster.Port, o3.Cluster.Port), 3, `"A"`))
	reloadUpdateConfig(t, s2, conf2, fmt.Sprintf(tmpl, "S2",
		fmt.Sprintf("routes:[\"nats://127.0.0.1:%d\",\"nats://127.0.0.1:%d\"]", o1.Cluster.Port, o3.Cluster.Port), 2, `"A"`))
	reloadUpdateConfig(t, s3, conf3, fmt.Sprintf(tmpl, "S3",
		fmt.Sprintf("routes:[\"nats://127.0.0.1:%d\",\"nats://127.0.0.1:%d\"]", o1.Cluster.Port, o2.Cluster.Port), 4, `"A"`))

	time.Sleep(50 * time.Millisecond)

	// Collect all route TCP connections so we close them all as fast as possible
	var rcs []net.Conn
	for _, s := range servers {
		s.mu.RLock()
		s.forEachRoute(func(r *client) {
			r.mu.Lock()
			rcs = append(rcs, r.nc)
			r.mu.Unlock()
		})
		s.mu.RUnlock()
	}
	// Close all connections
	for _, c := range rcs {
		c.Close()
	}

	checkCluster()
	checkRecv("hello2")

	// Now we will make account "B" have a dedicated route, and remove the one for "A"
	// Now upgrade one of the account to a dedicated route.
	reloadUpdateConfig(t, s1, conf1, fmt.Sprintf(tmpl, "S1",
		fmt.Sprintf("routes:[\"nats://127.0.0.1:%d\",\"nats://127.0.0.1:%d\"]", o2.Cluster.Port, o3.Cluster.Port), 3, `"B"`))
	reloadUpdateConfig(t, s2, conf2, fmt.Sprintf(tmpl, "S2",
		fmt.Sprintf("routes:[\"nats://127.0.0.1:%d\",\"nats://127.0.0.1:%d\"]", o1.Cluster.Port, o3.Cluster.Port), 2, `"B"`))
	reloadUpdateConfig(t, s3, conf3, fmt.Sprintf(tmpl, "S3",
		fmt.Sprintf("routes:[\"nats://127.0.0.1:%d\",\"nats://127.0.0.1:%d\"]", o1.Cluster.Port, o2.Cluster.Port), 4, `"B"`))

	checkCluster()
	checkRecv("hello3")

	// Since we are going to shutdown and not restart S3,
	// close client connections to this server.
	for _, conns := range aconns {
		conns[2].Close()
	}
	// Get S3 server ID and shut it down.
	s3ID := s3.ID()
	s3.Shutdown()

	// Wait for routes to be gone from S1 and S2.
	expected = []int{4, 4}
	servers = servers[:2]
	checkCluster()

	// Check that both S1 and S2 have cleaned-up the remoteRoutePoolSize for S3.
	for _, s := range servers {
		s.mu.RLock()
		rps, ok := s.remoteRoutePoolSize[s3ID]
		s.mu.RUnlock()
		if ok {
			t.Fatalf("On server %q, found remote pool size of %v for S3", s, rps)
		}
	}

	s1.Shutdown()
	s2.Shutdown()

	// Now start 2 servers with different pool size.
	conf1 = createConfFile(t, fmt.Appendf(nil, tmpl, "S1", _EMPTY_, 3, `"A"`))
	s1, o1 = RunServerWithConfig(conf1)
	defer s1.Shutdown()

	conf2 = createConfFile(t, fmt.Appendf(nil, tmpl, "S2",
		fmt.Sprintf("routes: [\"nats://127.0.0.1:%d\"]", o1.Cluster.Port), 2, `"A"`))
	s2, _ = RunServerWithConfig(conf2)
	defer s2.Shutdown()

	servers = []*Server{s1, s2}
	expected = []int{4, 4}
	checkCluster()

	// Now check that S1 and S2 have the proper remoteRoutePoolSize to each other.
	expectedRPS := func(s *Server, otherID string, expected int) {
		t.Helper()
		s.mu.RLock()
		defer s.mu.RUnlock()
		rps, ok := s.remoteRoutePoolSize[otherID]
		require_True(t, ok)
		require_Equal(t, rps, expected)
	}
	s2ID := s2.ID()
	expectedRPS(s1, s2ID, 2)
	expectedRPS(s2, s1.ID(), 3)

	// We will now config reload S2 with an increased pool size, but we want
	// to have S1 in a condition where the update does not go through the
	// process of re-creating the connections slice completely, which could
	// happen when the config reload reconnect from S2 to S1 happens in a way
	// that the earlier connections in the slice are replaced before the later
	// are, which prevents the situation in removeRoute() where all slots are
	// empty. To do so with certainty, we are going to replace the last
	// connection in s1's routes with a "fake" one that is not going to close
	// when s2 reloads.
	s1.mu.Lock()
	conns, ok := s1.routes[s2ID]
	if ok {
		rc := conns[2]
		conns[2] = &client{kind: ROUTER, route: &route{}}
		// Empty the slot in a bit and close the old connection.
		time.AfterFunc(150*time.Millisecond, func() {
			s1.mu.Lock()
			conns, ok := s1.routes[s2ID]
			if ok {
				conns[2] = nil
			}
			s1.mu.Unlock()
			rc.closeConnection(ReadError)
		})
	}
	s1.mu.Unlock()
	require_True(t, ok)

	// Increase the pool size.
	reloadUpdateConfig(t, s2, conf2, fmt.Sprintf(tmpl, "S2",
		fmt.Sprintf("routes:[\"nats://127.0.0.1:%d\"]", o1.Cluster.Port), 4, `"A"`))
	expected = []int{5, 5}
	checkCluster()

	expectedRPS(s1, s2ID, 4)
	expectedRPS(s2, s1.ID(), 3)

	// Try to decrease now...
	reloadUpdateConfig(t, s2, conf2, fmt.Sprintf(tmpl, "S2",
		fmt.Sprintf("routes:[\"nats://127.0.0.1:%d\"]", o1.Cluster.Port), 2, `"A"`))
	expected = []int{4, 4}
	checkCluster()

	expectedRPS(s1, s2ID, 2)
	expectedRPS(s2, s1.ID(), 3)
}

type captureRMsgTrace struct {
	DummyLogger
	sync.Mutex
	traces *bytes.Buffer
	out    []string
}

func (l *captureRMsgTrace) Tracef(format string, args ...any) {
	l.Lock()
	defer l.Unlock()
	msg := fmt.Sprintf(format, args...)
	if strings.Contains(msg, "[RMSG ") {
		l.traces.WriteString(msg)
		l.out = append(l.out, msg)
	}
}

func TestRoutePerAccount(t *testing.T) {

	akp1, _ := nkeys.CreateAccount()
	acc1, _ := akp1.PublicKey()

	akp2, _ := nkeys.CreateAccount()
	acc2, _ := akp2.PublicKey()

	tmpl := `
		port: -1
		accounts {
			%s { users: [{user: "a", password: "a"}] }
			%s { users: [{user: "b", password: "b"}] }
		}
		cluster {
			port: -1
			name: "local"
			%s
			accounts: ["%s"]
		}
	`
	conf1 := createConfFile(t, []byte(fmt.Sprintf(tmpl, acc1, acc2, _EMPTY_, acc2)))
	s1, o1 := RunServerWithConfig(conf1)
	defer s1.Shutdown()

	conf2 := createConfFile(t, []byte(fmt.Sprintf(tmpl,
		acc1, acc2,
		fmt.Sprintf("routes: [\"nats://127.0.0.1:%d\"]", o1.Cluster.Port),
		acc2)))
	s2, _ := RunServerWithConfig(conf2)
	defer s2.Shutdown()

	l := &captureRMsgTrace{traces: &bytes.Buffer{}}
	s2.SetLogger(l, false, true)

	checkClusterFormed(t, s1, s2)

	disconnectRoute := func(s *Server) {
		t.Helper()
		attempts := 0
	TRY_AGAIN:
		s.mu.RLock()
		if conns, ok := s.accRoutes[acc2]; ok {
			for _, r := range conns {
				if r != nil {
					r.mu.Lock()
					nc := r.nc
					r.mu.Unlock()
					if nc == nil {
						s.mu.RUnlock()
						if attempts++; attempts < 10 {
							time.Sleep(250 * time.Millisecond)
							goto TRY_AGAIN
						}
						t.Fatal("Route net.Conn is nil")
					}
					nc.Close()
				} else {
					s.mu.RUnlock()
					if attempts++; attempts < 10 {
						time.Sleep(250 * time.Millisecond)
						goto TRY_AGAIN
					}
					t.Fatal("Route connection is nil")
				}
			}
		}
		s.mu.RUnlock()
		time.Sleep(250 * time.Millisecond)
		checkClusterFormed(t, s1, s2)
	}
	disconnectRoute(s1)
	disconnectRoute(s2)

	sendAndRecv := func(acc, user, pwd string) {
		t.Helper()
		s2nc := natsConnect(t, s2.ClientURL(), nats.UserInfo(user, pwd))
		defer s2nc.Close()

		sub := natsSubSync(t, s2nc, "foo")
		natsFlush(t, s2nc)

		s1nc := natsConnect(t, s1.ClientURL(), nats.UserInfo(user, pwd))
		defer s1nc.Close()

		checkSubInterest(t, s1, acc, "foo", time.Second)

		for i := 0; i < 10; i++ {
			natsPub(t, s1nc, "foo", []byte("hello"))
		}
		for i := 0; i < 10; i++ {
			natsNexMsg(t, sub, time.Second)
		}
		// Make sure we don't receive more
		if msg, err := sub.NextMsg(150 * time.Millisecond); err == nil {
			t.Fatalf("Unexpected message: %+v", msg)
		}
	}

	sendAndRecv(acc1, "a", "a")
	sendAndRecv(acc2, "b", "b")

	l.Lock()
	traces := l.traces.String()
	out := append([]string(nil), l.out...)
	l.Unlock()
	// We should not have any "[RMSG <acc2>"
	if strings.Contains(traces, fmt.Sprintf("[RMSG %s", acc2)) {
		var outStr string
		for _, l := range out {
			outStr += l + "\r\n"
		}
		t.Fatalf("Should not have included account %q in protocol, got:\n%s", acc2, outStr)
	}
}

func TestRoutePerAccountImplicit(t *testing.T) {
	tmpl := `
		port: -1
		accounts {
			A { users: [{user: "a", password: "a"}] }
			B { users: [{user: "b", password: "b"}] }
		}
		cluster {
			port: -1
			name: "local"
			accounts: ["A"]
			%s
		}
	`
	conf1 := createConfFile(t, []byte(fmt.Sprintf(tmpl, _EMPTY_)))
	s1, o1 := RunServerWithConfig(conf1)
	defer s1.Shutdown()

	conf2And3 := createConfFile(t, []byte(fmt.Sprintf(tmpl,
		fmt.Sprintf("routes: [\"nats://127.0.0.1:%d\"]", o1.Cluster.Port))))
	s2, _ := RunServerWithConfig(conf2And3)
	defer s2.Shutdown()

	checkClusterFormed(t, s1, s2)

	s3, _ := RunServerWithConfig(conf2And3)
	defer s3.Shutdown()

	checkClusterFormed(t, s1, s2, s3)

	// On s3, close the per-account routes from s2
	s3.mu.RLock()
	for _, conns := range s3.accRoutes {
		for rem, r := range conns {
			if rem != s2.ID() {
				continue
			}
			r.mu.Lock()
			if r.nc != nil {
				r.nc.Close()
			}
			r.mu.Unlock()
		}
	}
	s3.mu.RUnlock()
	// Wait a bit to make sure there is a disconnect, then check the cluster is ok
	time.Sleep(250 * time.Millisecond)
	checkClusterFormed(t, s1, s2, s3)
}

func TestRoutePerAccountInboundStatsCountedOnce(t *testing.T) {
	tmpl := `
		port: -1
		accounts {
			A { users: [{user: "a", password: "a"}] }
			B { users: [{user: "b", password: "b"}] }
		}
		cluster {
			port: -1
			name: "local"
			accounts: ["A"]
			%s
		}
	`
	conf1 := createConfFile(t, []byte(fmt.Sprintf(tmpl, _EMPTY_)))
	s1, o1 := RunServerWithConfig(conf1)
	defer s1.Shutdown()

	conf2 := createConfFile(t, []byte(fmt.Sprintf(tmpl,
		fmt.Sprintf("routes: [\"nats://127.0.0.1:%d\"]", o1.Cluster.Port))))
	s2, _ := RunServerWithConfig(conf2)
	defer s2.Shutdown()

	checkClusterFormed(t, s1, s2)

	const n = 10
	payload := []byte("hello")

	// Account A has a dedicated route, account B uses the pool. Messages that
	// reach s2 only over a route must count once in the account's received
	// totals either way, matching the route breakdown and the sender's count.
	for _, acc := range []string{"A", "B"} {
		t.Run(acc, func(t *testing.T) {
			user := strings.ToLower(acc)
			s2nc := natsConnect(t, s2.ClientURL(), nats.UserInfo(user, user))
			defer s2nc.Close()
			sub := natsSubSync(t, s2nc, "foo")
			natsFlush(t, s2nc)

			s1nc := natsConnect(t, s1.ClientURL(), nats.UserInfo(user, user))
			defer s1nc.Close()
			checkSubInterest(t, s1, acc, "foo", time.Second)

			for i := 0; i < n; i++ {
				natsPub(t, s1nc, "foo", payload)
			}
			for i := 0; i < n; i++ {
				natsNexMsg(t, sub, time.Second)
			}

			checkFor(t, time.Second, 15*time.Millisecond, func() error {
				stz, err := s2.AccountStatz(&AccountStatzOptions{Accounts: []string{acc}})
				if err != nil {
					return err
				}
				if len(stz.Accounts) != 1 {
					return fmt.Errorf("expected 1 account, got %d", len(stz.Accounts))
				}
				recv := stz.Accounts[0].Received
				if recv.Routes == nil || recv.Routes.Msgs != n {
					return fmt.Errorf("expected %d route msgs received, got %+v", n, recv.Routes)
				}
				if recv.Msgs != n || recv.Bytes != int64(n*len(payload)) {
					return fmt.Errorf("expected %d msgs and %d bytes received, got %d and %d",
						n, n*len(payload), recv.Msgs, recv.Bytes)
				}
				return nil
			})
		})
	}
}

func TestRoutePerAccountDefaultForSysAccount(t *testing.T) {
	tmpl := `
		port: -1
		accounts {
			A { users: [{user: "a", password: "a"}] }
			B { users: [{user: "b", password: "b"}] }
		}
		cluster {
			port: -1
			name: "local"
			%s
			%s
			%s
		}
		%s
	`
	for _, test := range []struct {
		name     string
		accounts string
		sysAcc   string
		noSysAcc bool
	}{
		{"default sys no accounts", _EMPTY_, _EMPTY_, false},
		{"default sys in accounts", "accounts: [\"$SYS\"]", _EMPTY_, false},
		{"default sys with other accounts", "accounts: [\"A\",\"$SYS\"]", _EMPTY_, false},
		{"explicit sys no accounts", _EMPTY_, "system_account: B", false},
		{"explicit sys in accounts", "accounts: [\"B\"]", "system_account: B", false},
		{"explicit sys with other accounts", "accounts: [\"B\",\"A\"]", "system_account: B", false},
		{"no system account no accounts", _EMPTY_, "no_sys_acc: true", true},
		{"no system account with accounts", "accounts: [\"A\"]", "no_sys_acc: true", true},
	} {
		t.Run(test.name, func(t *testing.T) {
			conf1 := createConfFile(t, []byte(fmt.Sprintf(tmpl, _EMPTY_, test.accounts,
				_EMPTY_, test.sysAcc)))
			s1, o1 := RunServerWithConfig(conf1)
			defer s1.Shutdown()

			conf2 := createConfFile(t, []byte(fmt.Sprintf(tmpl, _EMPTY_, test.accounts,
				fmt.Sprintf("routes: [\"nats://127.0.0.1:%d\"]", o1.Cluster.Port), test.sysAcc)))
			s2, _ := RunServerWithConfig(conf2)
			defer s2.Shutdown()

			checkClusterFormed(t, s1, s2)

			checkSysAccRoute := func(s *Server) {
				t.Helper()
				var name string
				acc := s.SystemAccount()
				if test.noSysAcc {
					if acc != nil {
						t.Fatalf("Should not be any system account, got %q", acc.GetName())
					}
					// We will check that there is no accRoutes for the default
					// system account name
					name = DEFAULT_SYSTEM_ACCOUNT
				} else {
					acc.mu.RLock()
					pi := acc.routePoolIdx
					name = acc.Name
					acc.mu.RUnlock()
					if pi != -1 {
						t.Fatalf("System account %q should have route pool index==-1, got %v", name, pi)
					}
				}
				s.mu.RLock()
				_, ok := s.accRoutes[name]
				s.mu.RUnlock()
				if test.noSysAcc {
					if ok {
						t.Fatalf("System account %q should not have its own route, since NoSystemAccount was specified", name)
					}
				} else if !ok {
					t.Fatalf("System account %q should be present in accRoutes, it was not", name)
				}
			}
			checkSysAccRoute(s1)
			checkSysAccRoute(s2)

			// Check that this is still the case after a config reload
			reloadUpdateConfig(t, s1, conf1, fmt.Sprintf(tmpl, "pool_size: 4", test.accounts,
				_EMPTY_, test.sysAcc))
			reloadUpdateConfig(t, s2, conf2, fmt.Sprintf(tmpl, "pool_size: 4", test.accounts,
				fmt.Sprintf("routes: [\"nats://127.0.0.1:%d\"]", o1.Cluster.Port), test.sysAcc))

			checkSysAccRoute(s1)
			checkSysAccRoute(s2)
		})
	}
}

func TestRoutePerAccountConnectRace(t *testing.T) {
	// This test will have each server point to each other and that is causing
	// each one to attempt to connect routes to each other which should lead
	// to connections needing to be dropped. We make sure that there is still
	// resolution and there is the expected number of routes.
	createSrv := func(name string, port int) *Server {
		o := DefaultOptions()
		o.Port = -1
		o.ServerName = name
		o.Accounts = []*Account{NewAccount("A")}
		o.NoSystemAccount = true
		o.Cluster.PoolSize = 1
		o.Cluster.PinnedAccounts = []string{"A"}
		o.Cluster.Name = "local"
		o.Cluster.Port = port
		o.Routes = RoutesFromStr("nats://127.0.0.1:1234,nats://127.0.0.1:1235,nats://127.0.0.1:1236")
		s, err := NewServer(o)
		if err != nil {
			t.Fatalf("Error creating server: %v", err)
		}
		return s
	}
	s1 := createSrv("A", 1234)
	s2 := createSrv("B", 1235)
	s3 := createSrv("C", 1236)

	l := &captureDebugLogger{dbgCh: make(chan string, 100)}
	s1.SetLogger(l, true, false)

	servers := []*Server{s1, s2, s3}

	for _, s := range servers {
		go s.Start()
		defer s.Shutdown()
	}

	checkClusterFormed(t, s1, s2, s3)

	for done, duplicate := false, 0; !done; {
		select {
		case e := <-l.dbgCh:
			if strings.Contains(e, "duplicate") {
				if duplicate++; duplicate > 10 {
					t.Fatalf("Routes are constantly reconnecting: %v", e)
				}
			}
		case <-time.After(routeReconnectDelay + 500*time.Millisecond):
			// More than reconnect and some, and no reconnect, so we are good.
			done = true
		}
	}

	// Also, check that they all report as solicited and configured in monitoring.
	for _, s := range servers {
		routes, err := s.Routez(nil)
		require_NoError(t, err)
		for _, r := range routes.Routes {
			if !r.DidSolicit {
				t.Fatalf("All routes should have been marked as solicited, this one was not: %+v", r)
			}
			if !r.IsConfigured {
				t.Fatalf("All routes should have been marked as configured, this one was not: %+v", r)
			}
		}
	}

	for _, s := range servers {
		s.Shutdown()
		s.WaitForShutdown()
	}
}

func TestRoutePerAccountGossipWorks(t *testing.T) {
	tmplA := `
		port: -1
		server_name: "A"
		accounts {
			A { users: [{user: "A", password: "pwd"}] }
			B { users: [{user: "B", password: "pwd"}] }
		}
		cluster {
			port: %d
			name: "local"
			accounts: ["A"]
			%s
		}
	`
	conf1 := createConfFile(t, []byte(fmt.Sprintf(tmplA, -1, _EMPTY_)))
	s1, o1 := RunServerWithConfig(conf1)
	defer s1.Shutdown()

	tmplBC := `
		port: -1
		server_name: "%s"
		accounts {
			A { users: [{user: "A", password: "pwd"}] }
			B { users: [{user: "B", password: "pwd"}] }
		}
		cluster {
			port: -1
			name: "local"
			%s
			accounts: ["A"]
		}
	`
	conf2 := createConfFile(t, []byte(fmt.Sprintf(tmplBC, "B",
		fmt.Sprintf("routes: [\"nats://127.0.0.1:%d\"]", o1.Cluster.Port))))
	s2, _ := RunServerWithConfig(conf2)
	defer s2.Shutdown()

	checkClusterFormed(t, s1, s2)

	// Now connect s3 to s1 and make sure that s2 connects properly to s3.
	conf3 := createConfFile(t, []byte(fmt.Sprintf(tmplBC, "C",
		fmt.Sprintf("routes: [\"nats://127.0.0.1:%d\"]", o1.Cluster.Port))))
	s3, _ := RunServerWithConfig(conf3)
	defer s3.Shutdown()

	checkClusterFormed(t, s1, s2, s3)

	s3.Shutdown()
	s2.Shutdown()
	s1.Shutdown()

	// Slightly different version where s2 is connecting to s1, while s1
	// connects to s3 (and s3 does not solicit connections).

	conf1 = createConfFile(t, []byte(fmt.Sprintf(tmplA, -1, _EMPTY_)))
	s1, o1 = RunServerWithConfig(conf1)
	defer s1.Shutdown()

	conf2 = createConfFile(t, []byte(fmt.Sprintf(tmplBC, "B",
		fmt.Sprintf("routes: [\"nats://127.0.0.1:%d\"]", o1.Cluster.Port))))
	s2, _ = RunServerWithConfig(conf2)
	defer s2.Shutdown()

	checkClusterFormed(t, s1, s2)

	// Start s3 first that will simply accept connections
	conf3 = createConfFile(t, []byte(fmt.Sprintf(tmplBC, "C", _EMPTY_)))
	s3, o3 := RunServerWithConfig(conf3)
	defer s3.Shutdown()

	// Now config reload s1 so that it points to s3.
	reloadUpdateConfig(t, s1, conf1,
		fmt.Sprintf(tmplA, o1.Cluster.Port, fmt.Sprintf("routes: [\"nats://127.0.0.1:%d\"]", o3.Cluster.Port)))

	checkClusterFormed(t, s1, s2, s3)
}

func TestRoutePerAccountGossipWorksWithOldServerNotSeed(t *testing.T) {
	tmplA := `
		port: -1
		server_name: "A"
		accounts {
			A { users: [{user: "A", password: "pwd"}] }
			B { users: [{user: "B", password: "pwd"}] }
		}
		cluster {
			port: %d
			name: "local"
			accounts: ["A"]
		}
	`
	conf1 := createConfFile(t, []byte(fmt.Sprintf(tmplA, -1)))
	s1, o1 := RunServerWithConfig(conf1)
	defer s1.Shutdown()

	// Here, server "B" will have no pooling/accounts.
	tmplB := `
		port: -1
		server_name: "B"
		accounts {
			A { users: [{user: "A", password: "pwd"}] }
			B { users: [{user: "B", password: "pwd"}] }
		}
		cluster {
			port: -1
			name: "local"
			routes: ["nats://127.0.0.1:%d"]
			pool_size: -1
		}
	`
	conf2 := createConfFile(t, []byte(fmt.Sprintf(tmplB, o1.Cluster.Port)))
	s2, _ := RunServerWithConfig(conf2)
	defer s2.Shutdown()

	checkClusterFormed(t, s1, s2)

	l := &captureErrorLogger{errCh: make(chan string, 100)}
	s2.SetLogger(l, false, false)

	// Now connect s3 to s1. Server s1 should not gossip to s2 information
	// about pinned-account routes or extra routes from the pool.
	tmplC := `
		port: -1
		server_name: "C"
		accounts {
			A { users: [{user: "A", password: "pwd"}] }
			B { users: [{user: "B", password: "pwd"}] }
		}
		cluster {
			port: -1
			name: "local"
			routes: ["nats://127.0.0.1:%d"]
			accounts: ["A"]
		}
	`
	conf3 := createConfFile(t, []byte(fmt.Sprintf(tmplC, o1.Cluster.Port)))
	s3, _ := RunServerWithConfig(conf3)
	defer s3.Shutdown()

	checkClusterFormed(t, s1, s2, s3)

	// We should not have had s2 try to create dedicated routes for "A" or "$SYS"
	tm := time.NewTimer(time.Second)
	defer tm.Stop()
	for {
		select {
		case err := <-l.errCh:
			if strings.Contains(err, "dedicated route") {
				t.Fatalf("Server s2 should not have tried to create a dedicated route: %s", err)
			}
		case <-tm.C:
			return
		}
	}
}

func TestRoutePerAccountGossipWorksWithOldServerSeed(t *testing.T) {
	tmplA := `
		port: -1
		server_name: "A"
		accounts {
			A { users: [{user: "A", password: "pwd"}] }
			B { users: [{user: "B", password: "pwd"}] }
		}
		cluster {
			port: %d
			name: "local"
			pool_size: %d
			%s
		}
	`
	// Start with s1 being an "old" server, which does not support pooling/pinned-accounts.
	conf1 := createConfFile(t, []byte(fmt.Sprintf(tmplA, -1, -1, _EMPTY_)))
	s1, o1 := RunServerWithConfig(conf1)
	defer s1.Shutdown()

	tmplBC := `
		port: -1
		server_name: "%s"
		accounts {
			A { users: [{user: "A", password: "pwd"}] }
			B { users: [{user: "B", password: "pwd"}] }
		}
		cluster {
			port: -1
			name: "local"
			pool_size: 3
			routes: ["nats://127.0.0.1:%d"]
			accounts: ["A"]
		}
	`
	conf2 := createConfFile(t, []byte(fmt.Sprintf(tmplBC, "B", o1.Cluster.Port)))
	s2, _ := RunServerWithConfig(conf2)
	defer s2.Shutdown()

	checkClusterFormed(t, s1, s2)

	// Now connect s3 to s1 and make sure that s2 connects properly to s3.
	conf3 := createConfFile(t, []byte(fmt.Sprintf(tmplBC, "C", o1.Cluster.Port)))
	s3, _ := RunServerWithConfig(conf3)
	defer s3.Shutdown()

	checkClusterFormed(t, s1, s2, s3)

	checkRoutes := func(s *Server, expected int) {
		t.Helper()
		checkFor(t, 2*time.Second, 50*time.Millisecond, func() error {
			if nr := s.NumRoutes(); nr != expected {
				return fmt.Errorf("Server %q should have %v routes, got %v", s.Name(), expected, nr)
			}
			return nil
		})
	}
	// Since s1 has no pooling/pinned-accounts, there should be only 2 routes,
	// one to s2 and one to s3.
	checkRoutes(s1, 2)
	// s2 and s3 should have 1 route to s1 and 3(pool)+"A"+"$SYS" == 6
	checkRoutes(s2, 6)
	checkRoutes(s3, 6)

	s1.Shutdown()

	// The server s1 will now support pooling and accounts pinning.
	// Restart the server s1 with the same cluster port otherwise
	// s2/s3 would not be able to reconnect.
	conf1 = createConfFile(t, []byte(fmt.Sprintf(tmplA, o1.Cluster.Port, 3, "accounts: [\"A\"]")))
	s1, _ = RunServerWithConfig(conf1)
	defer s1.Shutdown()
	// Make sure reconnect occurs. We should now have 5 routes.
	checkClusterFormed(t, s1, s2, s3)
	// Now all servers should have 3+2 to each other, so 10 total.
	checkRoutes(s1, 10)
	checkRoutes(s2, 10)
	checkRoutes(s3, 10)
}

func TestRoutePoolPerAccountSubUnsubProtoParsing(t *testing.T) {
	for _, test := range []struct {
		name  string
		extra string
	}{
		{"regular", _EMPTY_},
		{"pooling", "pool_size: 5"},
		{"per-account", "accounts: [\"A\"]"},
	} {
		t.Run(test.name, func(t *testing.T) {
			confATemplate := `
				port: -1
				accounts {
					A { users: [{user: "user1", password: "pwd"}] }
				}
				cluster {
					listen: 127.0.0.1:-1
					%s
				}
			`
			confA := createConfFile(t, []byte(fmt.Sprintf(confATemplate, test.extra)))
			srva, optsA := RunServerWithConfig(confA)
			defer srva.Shutdown()

			confBTemplate := `
				port: -1
				accounts {
					A { users: [{user: "user1", password: "pwd"}] }
				}
				cluster {
					listen: 127.0.0.1:-1
					routes = [
						"nats://127.0.0.1:%d"
					]
					%s
				}
			`
			confB := createConfFile(t, []byte(fmt.Sprintf(confBTemplate, optsA.Cluster.Port, test.extra)))
			srvb, _ := RunServerWithConfig(confB)
			defer srvb.Shutdown()

			checkClusterFormed(t, srva, srvb)

			ncA := natsConnect(t, srva.ClientURL(), nats.UserInfo("user1", "pwd"))
			defer ncA.Close()

			for i := 0; i < 2; i++ {
				var sub *nats.Subscription
				if i == 0 {
					sub = natsSubSync(t, ncA, "foo")
				} else {
					sub = natsQueueSubSync(t, ncA, "foo", "bar")
				}

				checkSubInterest(t, srvb, "A", "foo", 2*time.Second)

				checkSubs := func(s *Server, queue, expected bool) {
					t.Helper()
					acc, err := s.LookupAccount("A")
					if err != nil {
						t.Fatalf("Error looking account: %v", err)
					}
					checkFor(t, time.Second, 15*time.Millisecond, func() error {
						acc.mu.RLock()
						res := acc.sl.Match("foo")
						acc.mu.RUnlock()
						if expected {
							if queue && (len(res.qsubs) == 0 || len(res.psubs) != 0) {
								return fmt.Errorf("Expected queue sub, did not find it")
							} else if !queue && (len(res.psubs) == 0 || len(res.qsubs) != 0) {
								return fmt.Errorf("Expected psub, did not find it")
							}
						} else if len(res.psubs)+len(res.qsubs) != 0 {
							return fmt.Errorf("Unexpected subscription: %+v", res)
						}
						return nil
					})
				}

				checkSubs(srva, i == 1, true)
				checkSubs(srvb, i == 1, true)

				sub.Unsubscribe()
				natsFlush(t, ncA)

				checkSubs(srva, i == 1, false)
				checkSubs(srvb, i == 1, false)
			}
		})
	}
}

func TestRoutePoolPerAccountStreamImport(t *testing.T) {
	for _, test := range []struct {
		name  string
		route string
	}{
		{"regular", _EMPTY_},
		{"pooled", "pool_size: 5"},
		{"one per account", "accounts: [\"A\"]"},
		{"both per account", "accounts: [\"A\", \"B\"]"},
	} {
		t.Run(test.name, func(t *testing.T) {
			tmplA := `
				server_name: "A"
				port: -1
				accounts {
					A {
						users: [{user: "user1", password: "pwd"}]
						exports: [{stream: "foo"}]
					}
					B {
						users: [{user: "user2", password: "pwd"}]
						imports: [{stream: {subject: "foo", account: "A"}}]
					}
					C { users: [{user: "user3", password: "pwd"}] }
					D { users: [{user: "user4", password: "pwd"}] }
				}
				cluster {
					name: "local"
					listen: 127.0.0.1:-1
					%s
				}
			`
			confA := createConfFile(t, []byte(fmt.Sprintf(tmplA, test.route)))
			srva, optsA := RunServerWithConfig(confA)
			defer srva.Shutdown()

			tmplB := `
				server_name: "B"
				port: -1
				accounts {
					A {
						users: [{user: "user1", password: "pwd"}]
						exports: [{stream: "foo"}]
					}
					B {
						users: [{user: "user2", password: "pwd"}]
						imports: [{stream: {subject: "foo", account: "A"}}]
					}
					C { users: [{user: "user3", password: "pwd"}] }
					D { users: [{user: "user4", password: "pwd"}] }
				}
				cluster {
					name: "local"
					listen: 127.0.0.1:-1
					%s
					%s
				}
			`
			confB := createConfFile(t, []byte(fmt.Sprintf(tmplB,
				fmt.Sprintf("routes: [\"nats://127.0.0.1:%d\"]", optsA.Cluster.Port),
				test.route)))
			srvb, _ := RunServerWithConfig(confB)
			defer srvb.Shutdown()

			checkClusterFormed(t, srva, srvb)

			ncB := natsConnect(t, srvb.ClientURL(), nats.UserInfo("user2", "pwd"))
			defer ncB.Close()

			sub := natsSubSync(t, ncB, "foo")

			checkSubInterest(t, srva, "B", "foo", time.Second)
			checkSubInterest(t, srva, "A", "foo", time.Second)

			ncA := natsConnect(t, srva.ClientURL(), nats.UserInfo("user1", "pwd"))
			defer ncA.Close()

			natsPub(t, ncA, "foo", []byte("hello"))
			natsNexMsg(t, sub, time.Second)

			natsUnsub(t, sub)
			natsFlush(t, ncB)

			checkFor(t, time.Second, 15*time.Millisecond, func() error {
				for _, acc := range []string{"A", "B"} {
					a, err := srva.LookupAccount(acc)
					if err != nil {
						return err
					}
					a.mu.RLock()
					r := a.sl.Match("foo")
					a.mu.RUnlock()
					if len(r.psubs) != 0 {
						return fmt.Errorf("Subscription not unsubscribed")
					}
				}
				return nil
			})
		})
	}
}

func TestRoutePoolAndPerAccountWithServiceLatencyNoDataRace(t *testing.T) {
	// For this test, we want the system (SYS) and SERVICE accounts to be bound
	// to different routes. So the names and pool size have been chosen accordingly.
	for _, test := range []struct {
		name    string
		poolStr string
	}{
		{"pool", "pool_size: 5"},
		{"per account", "accounts: [\"SYS\", \"SERVICE\", \"REQUESTOR\"]"},
	} {
		t.Run(test.name, func(t *testing.T) {
			tmpl := `
			port: -1
			accounts {
				SYS {
					users [{user: "sys", password: "pwd"}]
				}
				SERVICE {
					users [{user: "svc", password: "pwd"}]
					exports = [
						{service: "req.*", latency: {subject: "results"}}
					]
				}
				REQUESTOR {
					users [{user: "req", password: "pwd"}]
					imports = [
						{service: {account: "SERVICE", subject: "req.echo"}, to: "request"}
					]
				}
			}
			system_account: "SYS"
			cluster {
				name: "local"
				port: -1
				%s
				%s
			}
		`
			conf1 := createConfFile(t, []byte(fmt.Sprintf(tmpl, test.poolStr, _EMPTY_)))
			s1, opts1 := RunServerWithConfig(conf1)
			defer s1.Shutdown()

			conf2 := createConfFile(t, []byte(fmt.Sprintf(tmpl, test.poolStr,
				fmt.Sprintf("routes: [\"nats://127.0.0.1:%d\"]", opts1.Cluster.Port))))
			s2, _ := RunServerWithConfig(conf2)
			defer s2.Shutdown()

			checkClusterFormed(t, s1, s2)

			// Create service provider.
			nc := natsConnect(t, s1.ClientURL(), nats.UserInfo("svc", "pwd"))
			defer nc.Close()

			// The service listener.
			natsSub(t, nc, "req.echo", func(msg *nats.Msg) {
				msg.Respond(msg.Data)
			})

			// Listen for metrics
			rsub := natsSubSync(t, nc, "results")
			natsFlush(t, nc)
			checkSubInterest(t, s2, "SERVICE", "results", time.Second)

			// Create second client and send request from this one.
			nc2 := natsConnect(t, s2.ClientURL(), nats.UserInfo("req", "pwd"))
			defer nc2.Close()

			for i := 0; i < 5; i++ {
				// Send the request.
				_, err := nc2.Request("request", []byte("hello"), time.Second)
				require_NoError(t, err)
				// Get the latency result
				natsNexMsg(t, rsub, time.Second)
			}
		})
	}
}

func TestRouteParseOriginClusterMsgArgs(t *testing.T) {
	for _, test := range []struct {
		racc    bool
		args    string
		pacache string
		reply   string
		queues  [][]byte
	}{
		{true, "ORIGIN foo 12 345\r\n", "MY_ACCOUNT foo", _EMPTY_, nil},
		{true, "ORIGIN foo bar 12 345\r\n", "MY_ACCOUNT foo", "bar", nil},
		{true, "ORIGIN foo + bar queue1 queue2 12 345\r\n", "MY_ACCOUNT foo", "bar", [][]byte{[]byte("queue1"), []byte("queue2")}},
		{true, "ORIGIN foo | queue1 queue2 12 345\r\n", "MY_ACCOUNT foo", _EMPTY_, [][]byte{[]byte("queue1"), []byte("queue2")}},

		{false, "ORIGIN MY_ACCOUNT foo 12 345\r\n", "MY_ACCOUNT foo", _EMPTY_, nil},
		{false, "ORIGIN MY_ACCOUNT foo bar 12 345\r\n", "MY_ACCOUNT foo", "bar", nil},
		{false, "ORIGIN MY_ACCOUNT foo + bar queue1 queue2 12 345\r\n", "MY_ACCOUNT foo", "bar", [][]byte{[]byte("queue1"), []byte("queue2")}},
		{false, "ORIGIN MY_ACCOUNT foo | queue1 queue2 12 345\r\n", "MY_ACCOUNT foo", _EMPTY_, [][]byte{[]byte("queue1"), []byte("queue2")}},
	} {
		t.Run(test.args, func(t *testing.T) {
			c := &client{kind: ROUTER, route: &route{}}
			if test.racc {
				c.route.accName = []byte("MY_ACCOUNT")
			}
			if err := c.processRoutedOriginClusterMsgArgs([]byte(test.args)); err != nil {
				t.Fatalf("Error processing: %v", err)
			}
			if string(c.pa.origin) != "ORIGIN" {
				t.Fatalf("Invalid origin: %q", c.pa.origin)
			}
			if string(c.pa.account) != "MY_ACCOUNT" {
				t.Fatalf("Invalid account: %q", c.pa.account)
			}
			if string(c.pa.subject) != "foo" {
				t.Fatalf("Invalid subject: %q", c.pa.subject)
			}
			if string(c.pa.reply) != test.reply {
				t.Fatalf("Invalid reply: %q", c.pa.reply)
			}
			if !reflect.DeepEqual(c.pa.queues, test.queues) {
				t.Fatalf("Invalid queues: %v", c.pa.queues)
			}
			if c.pa.hdr != 12 {
				t.Fatalf("Invalid header size: %v", c.pa.hdr)
			}
			if c.pa.size != 345 {
				t.Fatalf("Invalid size: %v", c.pa.size)
			}
		})
	}
}

func TestRoutePoolAndPerAccountOperatorMode(t *testing.T) {
	_, spub := createKey(t)
	sysClaim := jwt.NewAccountClaims(spub)
	sysClaim.Name = "SYS"
	sysJwt := encodeClaim(t, sysClaim, spub)

	akp, apub := createKey(t)
	claima := jwt.NewAccountClaims(apub)
	ajwt := encodeClaim(t, claima, apub)

	bkp, bpub := createKey(t)
	claimb := jwt.NewAccountClaims(bpub)
	bjwt := encodeClaim(t, claimb, bpub)

	ckp, cpub := createKey(t)
	claimc := jwt.NewAccountClaims(cpub)
	cjwt := encodeClaim(t, claimc, cpub)

	_, dpub := createKey(t)

	basePath := "/ngs/v1/accounts/jwt/"
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == basePath {
			w.Write([]byte("ok"))
		} else if strings.HasSuffix(r.URL.Path, spub) {
			w.Write([]byte(sysJwt))
		} else if strings.HasSuffix(r.URL.Path, apub) {
			w.Write([]byte(ajwt))
		} else if strings.HasSuffix(r.URL.Path, bpub) {
			w.Write([]byte(bjwt))
		} else if strings.HasSuffix(r.URL.Path, cpub) {
			w.Write([]byte(cjwt))
		}
	}))
	defer ts.Close()

	operator := fmt.Sprintf(`
		operator: %s
		system_account: %s
		resolver: URL("%s%s")
	`, ojwt, spub, ts.URL, basePath)

	tmpl := `
		listen: 127.0.0.1:-1
		server_name: %s
		cluster {
			port: -1
			name: "local"
			%s
			accounts: ["` + apub + `"%s]
		}
	` + operator

	conf1 := createConfFile(t, []byte(fmt.Sprintf(tmpl, "A", _EMPTY_, _EMPTY_)))
	s1, o1 := RunServerWithConfig(conf1)
	defer s1.Shutdown()

	conf2 := createConfFile(t, []byte(fmt.Sprintf(tmpl, "B", fmt.Sprintf("routes: [\"nats://127.0.0.1:%d\"]", o1.Cluster.Port),
		_EMPTY_)))
	s2, _ := RunServerWithConfig(conf2)
	defer s2.Shutdown()

	checkClusterFormed(t, s1, s2)

	checkRoute := func(s *Server, acc string, perAccount bool) {
		t.Helper()
		checkFor(t, 2*time.Second, 50*time.Millisecond, func() error {
			s.mu.RLock()
			_, ok := s.accRoutes[acc]
			s.mu.RUnlock()
			if perAccount && !ok {
				return fmt.Errorf("No dedicated route for account %q on server %q", acc, s)
			} else if !perAccount && ok {
				return fmt.Errorf("Dedicated route for account %q on server %q", acc, s)
			}
			return nil
		})
	}
	// Route for accounts "apub" and "spub" should be a dedicated route
	checkRoute(s1, apub, true)
	checkRoute(s2, apub, true)
	checkRoute(s1, spub, true)
	checkRoute(s2, spub, true)
	// Route for account "bpub" should not
	checkRoute(s1, bpub, false)
	checkRoute(s2, bpub, false)

	checkComm := func(acc string, kp nkeys.KeyPair, subj string) {
		t.Helper()
		usr := createUserCreds(t, nil, kp)
		ncAs2 := natsConnect(t, s2.ClientURL(), usr)
		defer ncAs2.Close()
		sub := natsSubSync(t, ncAs2, subj)
		checkSubInterest(t, s1, acc, subj, time.Second)

		ncAs1 := natsConnect(t, s1.ClientURL(), usr)
		defer ncAs1.Close()
		natsPub(t, ncAs1, subj, nil)
		natsNexMsg(t, sub, time.Second)
	}
	checkComm(apub, akp, "foo")
	checkComm(bpub, bkp, "bar")

	// Add account "bpub" in accounts doing a configuration reload
	reloadUpdateConfig(t, s1, conf1, fmt.Sprintf(tmpl, "A", _EMPTY_, fmt.Sprintf(",\"%s\"", bpub)))
	// Already the route should be moved to a dedicated route, even
	// before doing the config reload on s2.
	checkRoute(s1, bpub, true)
	checkRoute(s2, bpub, true)
	// Account "apub" should still have its dedicated route
	checkRoute(s1, apub, true)
	checkRoute(s2, apub, true)
	// So the system account
	checkRoute(s1, spub, true)
	checkRoute(s2, spub, true)
	// Let's complete the config reload on srvb
	reloadUpdateConfig(t, s2, conf2, fmt.Sprintf(tmpl, "B", fmt.Sprintf("routes: [\"nats://127.0.0.1:%d\"]", o1.Cluster.Port),
		fmt.Sprintf(",\"%s\"", bpub)))
	checkClusterFormed(t, s1, s2)
	// Check communication on account bpub again.
	checkComm(bpub, bkp, "baz")

	// Now add with config reload an account that has not been used yet (cpub).
	// We will also remove account bpub from the account list.
	reloadUpdateConfig(t, s1, conf1, fmt.Sprintf(tmpl, "A", _EMPTY_, fmt.Sprintf(",\"%s\"", cpub)))
	// Again, check before reloading s2.
	checkRoute(s1, cpub, true)
	checkRoute(s2, cpub, true)
	// Now reload s2 and do other checks.
	reloadUpdateConfig(t, s2, conf2, fmt.Sprintf(tmpl, "B", fmt.Sprintf("routes: [\"nats://127.0.0.1:%d\"]", o1.Cluster.Port),
		fmt.Sprintf(",\"%s\"", cpub)))
	checkClusterFormed(t, s1, s2)
	checkRoute(s1, bpub, false)
	checkRoute(s2, bpub, false)
	checkComm(cpub, ckp, "bat")

	// Finally, let's try to add an account that the account server rejects.
	err := os.WriteFile(conf1, []byte(fmt.Sprintf(tmpl, "A", _EMPTY_, fmt.Sprintf(",\"%s\",\"%s\"", cpub, dpub))), 0666)
	require_NoError(t, err)
	if err := s1.Reload(); err == nil || !strings.Contains(err.Error(), dpub) {
		t.Fatalf("Expected error about not being able to lookup this account, got %q", err)
	}
}

func TestRoutePoolAndPerAccountWithOlderServer(t *testing.T) {
	tmpl := `
		port: -1
		server_name: "%s"
		accounts {
			A { users: [{user: "A", password: "pwd"}] }
			B { users: [{user: "B", password: "pwd"}] }
		}
		cluster {
			port: -1
			name: "local"
			pool_size: 5
			accounts: ["A"]
			%s
		}
	`
	conf1 := createConfFile(t, []byte(fmt.Sprintf(tmpl, "A", _EMPTY_)))
	s1, o1 := RunServerWithConfig(conf1)
	defer s1.Shutdown()

	conf2 := createConfFile(t, []byte(fmt.Sprintf(tmpl, "B",
		fmt.Sprintf("routes: [\"nats://127.0.0.1:%d\"]", o1.Cluster.Port))))
	s2, _ := RunServerWithConfig(conf2)
	defer s2.Shutdown()

	// Have s3 explicitly disable pooling (to behave as an old server)
	conf3 := createConfFile(t, []byte(fmt.Sprintf(`
		port: -1
		server_name: "C"
		accounts {
			A { users: [{user: "A", password: "pwd"}] }
			B { users: [{user: "B", password: "pwd"}] }
		}
		cluster {
			port: -1
			name: "local"
			pool_size: -1
			routes: ["nats://127.0.0.1:%d"]
		}
	`, o1.Cluster.Port)))
	s3, _ := RunServerWithConfig(conf3)
	defer s3.Shutdown()

	checkClusterFormed(t, s1, s2, s3)

	check := func(acc, subj string, subSrv, pubSrv1, pubSrv2 *Server) {
		t.Helper()

		ncSub := natsConnect(t, subSrv.ClientURL(), nats.UserInfo(acc, "pwd"))
		defer ncSub.Close()
		sub := natsSubSync(t, ncSub, subj)

		checkSubInterest(t, pubSrv1, acc, subj, time.Second)
		checkSubInterest(t, pubSrv2, acc, subj, time.Second)

		pub := func(s *Server) {
			t.Helper()
			nc := natsConnect(t, s.ClientURL(), nats.UserInfo(acc, "pwd"))
			defer nc.Close()

			natsPub(t, nc, subj, []byte("hello"))
			natsNexMsg(t, sub, time.Second)
		}
		pub(pubSrv1)
		pub(pubSrv2)
	}
	check("A", "subj1", s1, s2, s3)
	check("A", "subj2", s2, s1, s3)
	check("A", "subj3", s3, s1, s2)
	check("B", "subj4", s1, s2, s3)
	check("B", "subj5", s2, s1, s3)
	check("B", "subj6", s3, s1, s2)
}

type testDuplicateRouteLogger struct {
	DummyLogger
	ch    chan struct{}
	count int
}

func (l *testDuplicateRouteLogger) Noticef(format string, args ...any) {
	msg := fmt.Sprintf(format, args...)
	if !strings.Contains(msg, DuplicateRoute.String()) {
		return
	}
	select {
	case l.ch <- struct{}{}:
	default:
	}
	l.Mutex.Lock()
	l.count++
	l.Mutex.Unlock()
}

// This test will make sure that a server with pooling does not
// keep trying to connect to a non pooled (for instance old) server.
// Will also make sure that if the old server is simply accepting
// connections, and restarted, the server with pooling will connect.
func TestRoutePoolWithOlderServerConnectAndReconnect(t *testing.T) {
	tmplA := `
		port: -1
		server_name: "A"
		cluster {
			port: -1
			name: "local"
			pool_size: 3
			%s
		}
	`
	conf1 := createConfFile(t, []byte(fmt.Sprintf(tmplA, _EMPTY_)))
	s1, o1 := RunServerWithConfig(conf1)
	defer s1.Shutdown()

	l := &testDuplicateRouteLogger{ch: make(chan struct{}, 50)}
	s1.SetLogger(l, false, false)

	tmplB := `
		port: -1
		server_name: "B"
		cluster {
			port: %d
			name: "local"
			pool_size: -1
			%s
		}
	`
	conf2 := createConfFile(t, []byte(fmt.Sprintf(tmplB, -1,
		fmt.Sprintf("routes: [\"nats://127.0.0.1:%d\"]", o1.Cluster.Port))))
	s2, o2 := RunServerWithConfig(conf2)
	defer s2.Shutdown()

	checkClusterFormed(t, s1, s2)

	// Now reload configuration of s1 to point to s2.
	reloadUpdateConfig(t, s1, conf1, fmt.Sprintf(tmplA, fmt.Sprintf("routes: [\"nats://127.0.0.1:%d\"]", o2.Cluster.Port)))
	checkClusterFormed(t, s1, s2)

	// We could get some, but it should settle.
	checkRepeatConnect := func() {
		t.Helper()
		tm := time.NewTimer(4 * routeConnectDelay)
		var last time.Time
		for done := false; !done; {
			select {
			case <-l.ch:
				last = time.Now()
			case <-tm.C:
				done = true
			}
		}
		if dur := time.Since(last); dur <= routeConnectDelay {
			t.Fatalf("Still attempted to connect %v ago", dur)
		}
	}
	checkRepeatConnect()

	// Now shutdown s2 and restart it without active route to s1.
	// Check that cluster can still be formed: that is, s1 is
	// still trying to connect to s2.
	s2.Shutdown()
	// Wait for more than a regular reconnect delay attempt.
	// Note that in test it is set to 15ms.
	time.Sleep(50 * time.Millisecond)
	// Restart the server s2 with the same cluster port otherwise
	// s1 would not be able to reconnect.
	conf2 = createConfFile(t, []byte(fmt.Sprintf(tmplB, o2.Cluster.Port, _EMPTY_)))
	s2, _ = RunServerWithConfig(conf2)
	defer s2.Shutdown()
	// Make sure reconnect occurs
	checkClusterFormed(t, s1, s2)
	// And again, make sure there is no repeat-connect
	checkRepeatConnect()
}

func TestRoutePoolBadAuthNoRunawayCreateRoute(t *testing.T) {
	// This test checks the reconnect rate, so use the default reconnect delay.
	oRouteReconnectDelay := routeReconnectDelay
	routeReconnectDelay = DEFAULT_ROUTE_RECONNECT
	defer func() { routeReconnectDelay = oRouteReconnectDelay }()

	conf1 := createConfFile(t, []byte(`
		server_name: "S1"
		listen: "127.0.0.1:-1"
		cluster {
			name: "local"
			listen: "127.0.0.1:-1"
			pool_size: 4
			authorization {
				user: "correct"
				password: "correct"
				timeout: 5
			}
		}
	`))
	s1, o1 := RunServerWithConfig(conf1)
	defer s1.Shutdown()

	l := &captureErrorLogger{errCh: make(chan string, 100)}
	s1.SetLogger(l, false, false)

	tmpl := `
		server_name: "S2"
		listen: "127.0.0.1:-1"
		cluster {
			name: "local"
			listen: "127.0.0.1:-1"
			pool_size: 5
			routes: ["nats://%s@127.0.0.1:%d"]
		}
	`
	conf2 := createConfFile(t, fmt.Appendf(nil, tmpl, "incorrect:incorrect", o1.Cluster.Port))
	s2, _ := RunServerWithConfig(conf2)
	defer s2.Shutdown()

	deadline := time.Now().Add(2 * time.Second)
	var errors int
	for time.Now().Before(deadline) {
		select {
		case <-l.errCh:
			errors++
		default:
		}
	}
	// We should not get that many errors now. In the past, we would get more
	// than 200 for the 2 sec wait.
	if errors > 10 {
		t.Fatalf("Unexpected number of errors: %v", errors)
	}

	// Reload with proper credentials.
	reloadUpdateConfig(t, s2, conf2, fmt.Sprintf(tmpl, "correct:correct", o1.Cluster.Port))
	// Ensure we can connect.
	checkClusterFormed(t, s1, s2)
}

func TestRouteCompressionOptions(t *testing.T) {
	org := testDefaultClusterCompression
	testDefaultClusterCompression = _EMPTY_
	defer func() { testDefaultClusterCompression = org }()

	tmpl := `
		port: -1
		cluster {
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
		{"boolean enabled", "true", nil, CompressionS2Fast, nil},
		{"string enabled", "enabled", nil, CompressionS2Fast, nil},
		{"string EnaBled", "EnaBled", nil, CompressionS2Fast, nil},
		{"string on", "on", nil, CompressionS2Fast, nil},
		{"string ON", "ON", nil, CompressionS2Fast, nil},
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

			if cm := o.Cluster.Compression.Mode; cm != test.expected {
				t.Fatalf("Expected compression value to be %q, got %q", test.expected, cm)
			}
			if !reflect.DeepEqual(test.rtts, o.Cluster.Compression.RTTThresholds) {
				t.Fatalf("Expected RTT tresholds to be %+v, got %+v", test.rtts, o.Cluster.Compression.RTTThresholds)
			}
			s.Shutdown()

			o.Cluster.Port = -1
			o.Cluster.Compression.Mode = test.mode
			if len(test.rttVals) > 0 {
				o.Cluster.Compression.Mode = CompressionS2Auto
				o.Cluster.Compression.RTTThresholds = o.Cluster.Compression.RTTThresholds[:0]
				for _, v := range test.rttVals {
					o.Cluster.Compression.RTTThresholds = append(o.Cluster.Compression.RTTThresholds, time.Duration(v)*time.Millisecond)
				}
			}
			s = RunServer(o)
			defer s.Shutdown()
			if cm := o.Cluster.Compression.Mode; cm != test.expected {
				t.Fatalf("Expected compression value to be %q, got %q", test.expected, cm)
			}
			if !reflect.DeepEqual(test.rtts, o.Cluster.Compression.RTTThresholds) {
				t.Fatalf("Expected RTT tresholds to be %+v, got %+v", test.rtts, o.Cluster.Compression.RTTThresholds)
			}
		})
	}
	// Test that with no compression specified, we default to "accept"
	conf := createConfFile(t, []byte(`
		port: -1
		cluster {
			port: -1
		}
	`))
	s, o := RunServerWithConfig(conf)
	defer s.Shutdown()
	if cm := o.Cluster.Compression.Mode; cm != CompressionAccept {
		t.Fatalf("Expected compression value to be %q, got %q", CompressionAccept, cm)
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
			o.Cluster.Port = -1
			o.Cluster.Compression = CompressionOpts{test.mode, test.rtts}
			if _, err := NewServer(o); err == nil || !strings.Contains(err.Error(), test.err) {
				t.Fatalf("Unexpected error: %v", err)
			}
		})
	}
}

func TestS2WriterOptionsForFastCompression(t *testing.T) {
	opts := s2WriterOptions(CompressionS2Fast)
	if len(opts) == 0 {
		t.Fatal("Expected non-empty writer options for fast compression mode")
	}
}

type testConnSentBytes struct {
	net.Conn
	sync.RWMutex
	sent int
}

func (c *testConnSentBytes) Write(p []byte) (int, error) {
	n, err := c.Conn.Write(p)
	c.Lock()
	c.sent += n
	c.Unlock()
	return n, err
}

func TestRouteCompression(t *testing.T) {
	tmpl := `
		port: -1
		server_name: "%s"
		accounts {
			A { users: [{user: "a", pass: "pwd"}] }
		}
		cluster {
			name: "local"
			port: -1
			compression: true
			pool_size: %d
			%s
			%s
		}
	`
	for _, test := range []struct {
		name     string
		poolSize int
		accounts string
	}{
		{"no pooling", -1, _EMPTY_},
		{"pooling", 3, _EMPTY_},
		{"per account", 1, "accounts: [\"A\"]"},
	} {
		t.Run(test.name, func(t *testing.T) {
			conf1 := createConfFile(t, []byte(fmt.Sprintf(tmpl, "S1", test.poolSize, test.accounts, _EMPTY_)))
			s1, o1 := RunServerWithConfig(conf1)
			defer s1.Shutdown()

			conf2 := createConfFile(t, []byte(fmt.Sprintf(tmpl, "S2", test.poolSize, test.accounts,
				fmt.Sprintf("routes: [\"nats://127.0.0.1:%d\"]", o1.Cluster.Port))))
			s2, _ := RunServerWithConfig(conf2)
			defer s2.Shutdown()

			checkClusterFormed(t, s1, s2)

			s1.mu.RLock()
			s1.forEachRoute(func(r *client) {
				r.mu.Lock()
				r.nc = &testConnSentBytes{Conn: r.nc}
				r.mu.Unlock()
			})
			s1.mu.RUnlock()

			nc2 := natsConnect(t, s2.ClientURL(), nats.UserInfo("a", "pwd"))
			defer nc2.Close()
			sub := natsSubSync(t, nc2, "foo")
			natsFlush(t, nc2)
			checkSubInterest(t, s1, "A", "foo", time.Second)

			nc1 := natsConnect(t, s1.ClientURL(), nats.UserInfo("a", "pwd"))
			defer nc1.Close()

			var payloads [][]byte
			count := 26
			for i := 0; i < count; i++ {
				n := rand.IntN(2048) + 1
				p := make([]byte, n)
				for j := 0; j < n; j++ {
					p[j] = byte(i) + 'A'
				}
				payloads = append(payloads, p)
				natsPub(t, nc1, "foo", p)
			}

			totalPayloadSize := 0
			for i := 0; i < count; i++ {
				m := natsNexMsg(t, sub, time.Second)
				if !bytes.Equal(m.Data, payloads[i]) {
					t.Fatalf("Expected payload %q - got %q", payloads[i], m.Data)
				}
				totalPayloadSize += len(m.Data)
			}

			// Also check that the route stats shows that compression likely occurred
			var out int
			s1.mu.RLock()
			if len(test.accounts) > 0 {
				rems := s1.accRoutes["A"]
				if rems == nil {
					t.Fatal("Did not find route for account")
				}
				for _, r := range rems {
					r.mu.Lock()
					if r.nc != nil {
						nc := r.nc.(*testConnSentBytes)
						nc.RLock()
						out = nc.sent
						nc.RUnlock()
					}
					r.mu.Unlock()
					break
				}
			} else {
				ai, _ := s1.accounts.Load("A")
				acc := ai.(*Account)
				acc.mu.RLock()
				pi := acc.routePoolIdx
				acc.mu.RUnlock()
				s1.forEachRouteIdx(pi, func(r *client) bool {
					r.mu.Lock()
					if r.nc != nil {
						nc := r.nc.(*testConnSentBytes)
						nc.RLock()
						out = nc.sent
						nc.RUnlock()
					}
					r.mu.Unlock()
					return false
				})
			}
			s1.mu.RUnlock()
			// Should at least be smaller than totalPayloadSize, use 20%.
			limit := totalPayloadSize * 80 / 100
			if int(out) > limit {
				t.Fatalf("Expected s1's outBytes to be less than %v, got %v", limit, out)
			}
		})
	}
}

func TestRouteCompressionMatrixModes(t *testing.T) {
	tmpl := `
			port: -1
			server_name: "%s"
			cluster {
				name: "local"
				port: -1
				compression: %s
				pool_size: -1
				%s
			}
		`
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
		{"accept on", "accept", "on", CompressionS2Fast, CompressionS2Fast},
		{"accept better", "accept", "better", CompressionS2Better, CompressionS2Better},
		{"accept best", "accept", "best", CompressionS2Best, CompressionS2Best},

		{"on off", "on", "off", CompressionOff, CompressionOff},
		{"on accept", "on", "accept", CompressionS2Fast, CompressionS2Fast},
		{"on on", "on", "on", CompressionS2Fast, CompressionS2Fast},
		{"on better", "on", "better", CompressionS2Fast, CompressionS2Better},
		{"on best", "on", "best", CompressionS2Fast, CompressionS2Best},

		{"better off", "better", "off", CompressionOff, CompressionOff},
		{"better accept", "better", "accept", CompressionS2Better, CompressionS2Better},
		{"better on", "better", "on", CompressionS2Better, CompressionS2Fast},
		{"better better", "better", "better", CompressionS2Better, CompressionS2Better},
		{"better best", "better", "best", CompressionS2Better, CompressionS2Best},

		{"best off", "best", "off", CompressionOff, CompressionOff},
		{"best accept", "best", "accept", CompressionS2Best, CompressionS2Best},
		{"best on", "best", "on", CompressionS2Best, CompressionS2Fast},
		{"best better", "best", "better", CompressionS2Best, CompressionS2Better},
		{"best best", "best", "best", CompressionS2Best, CompressionS2Best},
	} {
		t.Run(test.name, func(t *testing.T) {
			conf1 := createConfFile(t, []byte(fmt.Sprintf(tmpl, "A", test.s1, _EMPTY_)))
			s1, o1 := RunServerWithConfig(conf1)
			defer s1.Shutdown()

			conf2 := createConfFile(t, []byte(fmt.Sprintf(tmpl, "B", test.s2, fmt.Sprintf("routes: [\"nats://127.0.0.1:%d\"]", o1.Cluster.Port))))
			s2, _ := RunServerWithConfig(conf2)
			defer s2.Shutdown()

			checkClusterFormed(t, s1, s2)

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
					rz, err := srv.Routez(nil)
					require_NoError(t, err)
					var expected string
					if srv == s1 {
						expected = test.s1Expected
					} else {
						expected = test.s2Expected
					}
					if cm := rz.Routes[0].Compression; cm != expected {
						t.Fatalf("Server %s - expected compression %q, got %q", srv, expected, cm)
					}
				}
			}
			check(nc1, nc2, "foo", s1)
			check(nc2, nc1, "bar", s2)
		})
	}
}

func TestRouteCompressionWithOlderServer(t *testing.T) {
	tmpl := `
		port: -1
		server_name: "%s"
		cluster {
			port: -1
			name: "local"
			%s
			%s
		}
	`
	conf1 := createConfFile(t, []byte(fmt.Sprintf(tmpl, "A", _EMPTY_, "compression: \"on\"")))
	s1, o1 := RunServerWithConfig(conf1)
	defer s1.Shutdown()

	routes := fmt.Sprintf("routes: [\"nats://127.0.0.1:%d\"]", o1.Cluster.Port)
	conf2 := createConfFile(t, []byte(fmt.Sprintf(tmpl, "B", routes, "compression: \"not supported\"")))
	s2, _ := RunServerWithConfig(conf2)
	defer s2.Shutdown()

	checkClusterFormed(t, s1, s2)

	// Make sure that s1 route's compression is "off"
	s1.mu.RLock()
	s1.forEachRoute(func(r *client) {
		r.mu.Lock()
		cm := r.route.compression
		r.mu.Unlock()
		if cm != CompressionNotSupported {
			s1.mu.RUnlock()
			t.Fatalf("Compression should be %q, got %q", CompressionNotSupported, cm)
		}
	})
	s1.mu.RUnlock()
}

func TestRouteCompressionImplicitRoute(t *testing.T) {
	tmpl := `
		port: -1
		server_name: "%s"
		cluster {
			port: -1
			name: "local"
			%s
			%s
		}
	`
	conf1 := createConfFile(t, []byte(fmt.Sprintf(tmpl, "A", _EMPTY_, _EMPTY_)))
	s1, o1 := RunServerWithConfig(conf1)
	defer s1.Shutdown()

	routes := fmt.Sprintf("routes: [\"nats://127.0.0.1:%d\"]", o1.Cluster.Port)
	conf2 := createConfFile(t, []byte(fmt.Sprintf(tmpl, "B", routes, "compression: \"fast\"")))
	s2, _ := RunServerWithConfig(conf2)
	defer s2.Shutdown()

	conf3 := createConfFile(t, []byte(fmt.Sprintf(tmpl, "C", routes, "compression: \"best\"")))
	s3, _ := RunServerWithConfig(conf3)
	defer s3.Shutdown()

	checkClusterFormed(t, s1, s2, s3)

	checkComp := func(s *Server, remoteID, expected string) {
		t.Helper()
		s.mu.RLock()
		defer s.mu.RUnlock()
		var err error
		s.forEachRoute(func(r *client) {
			if err != nil {
				return
			}
			var cm string
			ok := true
			r.mu.Lock()
			if r.route.remoteID == remoteID {
				cm = r.route.compression
				ok = cm == expected
			}
			r.mu.Unlock()
			if !ok {
				err = fmt.Errorf("Server %q - expected route to %q to use compression %q, got %q",
					s, remoteID, expected, cm)
			}
		})
	}
	checkComp(s1, s2.ID(), CompressionS2Fast)
	checkComp(s1, s3.ID(), CompressionS2Best)
	checkComp(s2, s1.ID(), CompressionS2Fast)
	checkComp(s2, s3.ID(), CompressionS2Best)
	checkComp(s3, s1.ID(), CompressionS2Best)
	checkComp(s3, s2.ID(), CompressionS2Best)
}

func TestRouteCompressionAuto(t *testing.T) {
	tmpl := `
		port: -1
		server_name: "%s"
		ping_interval: "%s"
		cluster {
			port: -1
			name: "local"
			compression: %s
			%s
		}
	`
	conf1 := createConfFile(t, []byte(fmt.Sprintf(tmpl, "A", "10s", "s2_fast", _EMPTY_)))
	s1, o1 := RunServerWithConfig(conf1)
	defer s1.Shutdown()

	// Start with 0ms RTT
	np := createNetProxy(0, 1024*1024*1024, 1024*1024*1024, fmt.Sprintf("nats://127.0.0.1:%d", o1.Cluster.Port), true)
	routes := fmt.Sprintf("routes: [\"%s\"]", np.routeURL())

	rtts := "{mode: s2_auto, rtt_thresholds: [100ms, 200ms, 300ms]}"
	conf2 := createConfFile(t, []byte(fmt.Sprintf(tmpl, "B", "500ms", rtts, routes)))
	s2, _ := RunServerWithConfig(conf2)
	defer s2.Shutdown()
	defer np.stop()

	checkClusterFormed(t, s1, s2)

	checkComp := func(expected string) {
		t.Helper()
		checkFor(t, 4*time.Second, 50*time.Millisecond, func() error {
			s2.mu.RLock()
			defer s2.mu.RUnlock()
			if n := s2.numRoutes(); n != 4 {
				return fmt.Errorf("Cluster not formed properly, got %v routes", n)
			}
			var err error
			s2.forEachRoute(func(r *client) {
				if err != nil {
					return
				}
				r.mu.Lock()
				cm := r.route.compression
				r.mu.Unlock()
				if cm != expected {
					err = fmt.Errorf("Route %v compression mode expected to be %q, got %q", r, expected, cm)
				}
			})
			return err
		})
	}
	checkComp(CompressionS2Uncompressed)

	// Change the proxy RTT and we should get compression "fast"
	np.updateRTT(150 * time.Millisecond)
	checkComp(CompressionS2Fast)

	// Now 250ms, and get "better"
	np.updateRTT(250 * time.Millisecond)
	checkComp(CompressionS2Better)

	// Above 350 and we should get "best"
	np.updateRTT(350 * time.Millisecond)
	checkComp(CompressionS2Best)

	// Down to 1ms and again should get "uncompressed"
	np.updateRTT(1 * time.Millisecond)
	checkComp(CompressionS2Uncompressed)

	// Do a config reload with disabling uncompressed
	reloadUpdateConfig(t, s2, conf2, fmt.Sprintf(tmpl, "B", "500ms", "{mode: s2_auto, rtt_thresholds: [0ms, 100ms, 0ms, 300ms]}", routes))
	// Change the RTT back down to 1ms, but we should not go uncompressed,
	// we should have "fast" compression.
	np.updateRTT(1 * time.Millisecond)
	checkComp(CompressionS2Fast)
	// Now bump to 150ms and we should be using "best", not the "better" mode
	np.updateRTT(150 * time.Millisecond)
	checkComp(CompressionS2Best)
	// Try 400ms and we should still be using "best"
	np.updateRTT(400 * time.Millisecond)
	checkComp(CompressionS2Best)

	// Try other variations
	reloadUpdateConfig(t, s2, conf2, fmt.Sprintf(tmpl, "B", "500ms", "{mode: s2_auto, rtt_thresholds: [50ms, 150ms, 0ms, 0ms]}", routes))
	np.updateRTT(0 * time.Millisecond)
	checkComp(CompressionS2Uncompressed)
	np.updateRTT(100 * time.Millisecond)
	checkComp(CompressionS2Fast)
	// Since we expect the same compression level, just wait before doing
	// the update and the next check.
	time.Sleep(100 * time.Millisecond)
	np.updateRTT(250 * time.Millisecond)
	checkComp(CompressionS2Fast)

	// Now disable compression on s1
	reloadUpdateConfig(t, s1, conf1, fmt.Sprintf(tmpl, "A", "10s", "off", _EMPTY_))
	// Wait a bit to make sure we don't check for cluster too soon since
	// we expect a disconnect.
	time.Sleep(100 * time.Millisecond)
	checkClusterFormed(t, s1, s2)
	// Now change the RTT values in the proxy.
	np.updateRTT(0 * time.Millisecond)
	// Now check that s2 also shows as "off". Wait for some ping intervals.
	time.Sleep(200 * time.Millisecond)
	checkComp(CompressionOff)
}

func TestRoutePings(t *testing.T) {
	routeMaxPingInterval = 50 * time.Millisecond
	defer func() { routeMaxPingInterval = defaultRouteMaxPingInterval }()

	o1 := DefaultOptions()
	s1 := RunServer(o1)
	defer s1.Shutdown()

	o2 := DefaultOptions()
	o2.Routes = RoutesFromStr(fmt.Sprintf("nats://127.0.0.1:%d", o1.Cluster.Port))
	s2 := RunServer(o2)
	defer s2.Shutdown()

	checkClusterFormed(t, s1, s2)

	ch := make(chan struct{}, 1)
	s1.mu.RLock()
	s1.forEachRemote(func(r *client) {
		r.mu.Lock()
		r.nc = &capturePingConn{r.nc, ch}
		r.mu.Unlock()
	})
	s1.mu.RUnlock()

	for i := 0; i < 5; i++ {
		select {
		case <-ch:
		case <-time.After(250 * time.Millisecond):
			t.Fatalf("Did not send PING")
		}
	}
}

func TestRouteCustomPing(t *testing.T) {
	pingInterval := 50 * time.Millisecond
	o1 := DefaultOptions()
	o1.Cluster.PingInterval = pingInterval
	o1.Cluster.MaxPingsOut = 2
	s1 := RunServer(o1)
	defer s1.Shutdown()

	o2 := DefaultOptions()
	o2.Cluster.PingInterval = pingInterval
	o2.Routes = RoutesFromStr(fmt.Sprintf("nats://127.0.0.1:%d", o1.Cluster.Port))
	s2 := RunServer(o2)
	defer s2.Shutdown()

	checkClusterFormed(t, s1, s2)

	ch := make(chan struct{}, 1)
	s1.mu.RLock()
	s1.forEachRemote(func(r *client) {
		r.mu.Lock()
		r.nc = &capturePingConn{r.nc, ch}
		r.mu.Unlock()
	})
	s1.mu.RUnlock()

	for i := 0; i < 5; i++ {
		select {
		case <-ch:
		case <-time.After(250 * time.Millisecond):
			t.Fatalf("Did not send PING")
		}
	}
}

func TestRouteCompressionStaleConnectionUsesClusterPingMax(t *testing.T) {
	mockListener, err := net.Listen("tcp", "127.0.0.1:0")
	require_NoError(t, err)
	defer mockListener.Close()
	mockPort := mockListener.Addr().(*net.TCPAddr).Port

	// The mock peer never sends INFO, so the soliciting route never completes
	// compression negotiation. The connection can only be closed by the stale
	// watch installed in createRoute.
	connCh := make(chan net.Conn, 1)
	go func() {
		conn, err := mockListener.Accept()
		if err != nil {
			return
		}
		connCh <- conn
	}()

	// Cluster.MaxPingsOut and MaxPingsOut differ so the stale timer's deadline
	// distinguishes which is used: 50ms*2 = 100ms vs 50ms*101 ≈ 5s.
	o := DefaultOptions()
	o.Cluster.Compression.Mode = CompressionS2Fast
	o.Cluster.PingInterval = 50 * time.Millisecond
	o.Cluster.MaxPingsOut = 1
	o.MaxPingsOut = 100
	o.Routes = RoutesFromStr(fmt.Sprintf("nats://127.0.0.1:%d", mockPort))
	s := RunServer(o)
	defer s.Shutdown()

	var conn net.Conn
	select {
	case conn = <-connCh:
	case <-time.After(2 * time.Second):
		t.Fatalf("Mock listener did not accept route connection")
	}
	defer conn.Close()

	require_NoError(t, conn.SetReadDeadline(time.Now().Add(1500*time.Millisecond)))
	buf := make([]byte, 256)
	start := time.Now()
	for {
		if _, err = conn.Read(buf); err != nil {
			break
		}
	}
	if elapsed := time.Since(start); elapsed > time.Second {
		t.Fatalf("Stale-connection timer used wrong ping_max: closed after %v, expected <1s (Cluster.MaxPingsOut=1)", elapsed)
	}
}

func TestRouteNoLeakOnSlowConsumer(t *testing.T) {
	o1 := DefaultOptions()
	o1.Cluster.PoolSize = -1
	s1 := RunServer(o1)
	defer s1.Shutdown()

	o2 := DefaultOptions()
	o2.Cluster.PoolSize = -1
	o2.Routes = RoutesFromStr(fmt.Sprintf("nats://127.0.0.1:%d", o1.Cluster.Port))
	s2 := RunServer(o2)
	defer s2.Shutdown()

	checkClusterFormed(t, s1, s2)

	// For any route connections on the first server, drop the write
	// deadline down and then get the client to try sending something.
	// This should result in an effectively immediate write timeout,
	// which will surface as a slow consumer.
	s1.mu.Lock()
	for _, cl := range s1.routes {
		for _, c := range cl {
			c.mu.Lock()
			c.out.wdl = time.Nanosecond
			c.mu.Unlock()
			c.sendRTTPing()
		}
	}
	s1.mu.Unlock()

	// By now the routes should have gone down, so check that there
	// aren't any routes listed still.
	checkFor(t, time.Millisecond*500, time.Millisecond*25, func() error {
		if nc := s1.NumRoutes(); nc != 0 {
			return fmt.Errorf("Server 1 should have no route connections, got %v", nc)
		}
		if nc := s2.NumRoutes(); nc != 0 {
			return fmt.Errorf("Server 2 should have no route connections, got %v", nc)
		}
		return nil
	})
	var got, expected int64
	got = s1.NumSlowConsumers()
	expected = 1
	if got != expected {
		t.Errorf("got: %d, expected: %d", got, expected)
	}
	got = int64(s1.NumSlowConsumersRoutes())
	if got != expected {
		t.Errorf("got: %d, expected: %d", got, expected)
	}
	got = int64(s1.NumSlowConsumersClients())
	expected = 0
	if got != expected {
		t.Errorf("got: %d, expected: %d", got, expected)
	}
	varz, err := s1.Varz(nil)
	if err != nil {
		t.Fatal(err)
	}
	if varz.SlowConsumersStats.Clients != 0 {
		t.Error("Expected no slow consumer clients")
	}
	if varz.SlowConsumersStats.Routes != 1 {
		t.Error("Expected a slow consumer route")
	}
}

func TestRouteNoAppSubLeakOnSlowConsumer(t *testing.T) {
	o1 := DefaultOptions()
	o1.MaxPending = 1024
	o1.MaxPayload = int32(o1.MaxPending)
	s1 := RunServer(o1)
	defer s1.Shutdown()

	o2 := DefaultOptions()
	o2.MaxPending = 1024
	o2.MaxPayload = int32(o2.MaxPending)
	o2.Routes = RoutesFromStr(fmt.Sprintf("nats://127.0.0.1:%d", o1.Cluster.Port))
	s2 := RunServer(o2)
	defer s2.Shutdown()

	checkClusterFormed(t, s1, s2)

	checkSub := func(expected bool) {
		t.Helper()
		checkFor(t, 2*time.Second, 50*time.Millisecond, func() error {
			subsz, err := s1.Subsz(&SubszOptions{Subscriptions: true, Account: globalAccountName})
			require_NoError(t, err)
			for _, sub := range subsz.Subs {
				if sub.Subject == "foo" {
					if expected {
						return nil
					}
					return fmt.Errorf("Subscription should not have been found: %+v", sub)
				}
			}
			if expected {
				return fmt.Errorf("Subscription on `foo` not found")
			}
			return nil
		})
	}

	checkRoutedSub := func(expected bool) {
		t.Helper()
		checkFor(t, 2*time.Second, 50*time.Millisecond, func() error {
			routez, err := s2.Routez(&RoutezOptions{Subscriptions: true})
			require_NoError(t, err)
			for _, route := range routez.Routes {
				if route.Account != _EMPTY_ {
					continue
				}
				if len(route.Subs) == 1 && route.Subs[0] == "foo" {
					if expected {
						return nil
					}
					return fmt.Errorf("Subscription should not have been found: %+v", route.Subs)
				}
			}
			if expected {
				return fmt.Errorf("Did not find `foo` subscription")
			}
			return nil
		})
	}

	checkClosed := func(cid uint64) {
		t.Helper()
		checkFor(t, 2*time.Second, 50*time.Millisecond, func() error {
			connz, err := s1.Connz(&ConnzOptions{State: ConnClosed, CID: cid, Subscriptions: true})
			if err != nil {
				return err
			}
			require_Len(t, len(connz.Conns), 1)
			conn := connz.Conns[0]
			require_Equal(t, conn.Reason, SlowConsumerPendingBytes.String())
			subs := conn.Subs
			require_Len(t, len(subs), 1)
			require_Equal[string](t, subs[0], "foo")
			return nil
		})
	}

	for i := 0; i < 5; i++ {
		nc := natsConnect(t, s1.ClientURL())
		defer nc.Close()

		natsSubSync(t, nc, "foo")
		natsFlush(t, nc)

		checkSub(true)
		checkRoutedSub(true)

		cid, err := nc.GetClientID()
		require_NoError(t, err)
		c := s1.getClient(cid)
		payload := make([]byte, 2048)
		c.mu.Lock()
		c.queueOutbound([]byte(fmt.Sprintf("MSG foo 1 2048\r\n%s\r\n", payload)))
		closed := c.isClosed()
		c.mu.Unlock()

		require_True(t, closed)
		checkSub(false)
		checkRoutedSub(false)
		checkClosed(cid)

		nc.Close()
	}
}

func TestRouteSlowConsumerRecover(t *testing.T) {
	o1 := DefaultOptions()
	o1.Cluster.PoolSize = -1
	s1 := RunServer(o1)
	defer s1.Shutdown()

	rtt := 1500 * time.Nanosecond
	upRate := 1024 * 1024
	downRate := 128 * 1024
	np := createNetProxy(rtt, upRate, downRate, fmt.Sprintf("nats://127.0.0.1:%d", o1.Cluster.Port), true)
	defer np.stop()

	o2 := DefaultOptions()
	o2.Cluster.PoolSize = -1
	o2.Routes = RoutesFromStr(np.routeURL())
	s2 := RunServer(o2)
	defer s2.Shutdown()

	checkClusterFormed(t, s1, s2)

	changeWriteDeadline := func(s *Server, duration time.Duration) {
		s.mu.Lock()
		for _, cl := range s.routes {
			for _, c := range cl {
				c.mu.Lock()
				c.out.wdl = duration
				c.mu.Unlock()
			}
		}
		s.mu.Unlock()
	}
	hasSlowConsumerRoutes := func(s *Server) bool {
		var sc bool
		s.mu.Lock()
	Loop:
		for _, cl := range s.routes {
			for _, c := range cl {
				c.mu.Lock()
				sc = c.flags.isSet(isSlowConsumer)
				c.mu.Unlock()
				if sc {
					break Loop
				}
			}
		}
		s.mu.Unlock()
		return sc
	}

	// Start with a shorter write deadline to cause errors
	// then bump it again later to let it recover.
	changeWriteDeadline(s1, 1*time.Second)

	ncA, err := nats.Connect(s1.Addr().String())
	require_NoError(t, err)
	defer ncA.Close()

	ncB, err := nats.Connect(s2.Addr().String())
	require_NoError(t, err)
	defer ncB.Close()

	var wg sync.WaitGroup
	ncB.Subscribe("test", func(*nats.Msg) {
		ncB.Close()
	})
	ncB.Flush()

	ctx, cancel := context.WithTimeout(context.Background(), 800*time.Millisecond)
	defer cancel()

	go func() {
		var total int
		payload := bytes.Repeat([]byte("A"), 132*1024)
		for range time.NewTicker(30 * time.Millisecond).C {
			select {
			case <-ctx.Done():
				wg.Done()
				return
			default:
			}
			ncA.Publish("test", payload)
			ncA.Flush()
			total++
		}
	}()
	wg.Add(1)

	checkFor(t, 20*time.Second, 2*time.Millisecond, func() error {
		if s1.NumRoutes() < 1 {
			return fmt.Errorf("No routes connected")
		}
		if !hasSlowConsumerRoutes(s1) {
			if s1.NumSlowConsumersRoutes() > 0 {
				// In case it has recovered already.
				return nil
			}
			return fmt.Errorf("Expected Slow Consumer routes")
		}
		return nil
	})
	cancel()
	changeWriteDeadline(s1, 5*time.Second)
	np.updateRTT(0)
	checkFor(t, 20*time.Second, 10*time.Millisecond, func() error {
		if s1.NumRoutes() < 1 {
			return fmt.Errorf("No routes connected")
		}
		if hasSlowConsumerRoutes(s1) {
			return fmt.Errorf("Expected Slow Consumer routes to recover")
		}
		return nil
	})

	checkFor(t, 20*time.Second, 100*time.Millisecond, func() error {
		var got, expected int64
		got = int64(s1.NumSlowConsumersRoutes())
		expected = 1
		if got != expected {
			return fmt.Errorf("got: %d, expected: %d", got, expected)
		}
		return nil
	})
	wg.Wait()
}

func TestRouteNoLeakOnAuthTimeout(t *testing.T) {
	opts := DefaultOptions()
	opts.Cluster.Username = "foo"
	opts.Cluster.Password = "bar"
	opts.AuthTimeout = 0.01 // Deliberately short timeout
	s := RunServer(opts)
	defer s.Shutdown()

	c, err := net.Dial("tcp", net.JoinHostPort(opts.Host, fmt.Sprintf("%d", opts.Cluster.Port)))
	if err != nil {
		t.Fatalf("Error connecting: %v", err)
	}
	defer c.Close()

	cr := bufio.NewReader(c)

	// Wait for INFO...
	line, _, _ := cr.ReadLine()
	var info serverInfo
	if err = json.Unmarshal(line[5:], &info); err != nil {
		t.Fatalf("Could not parse INFO json: %v\n", err)
	}

	// Wait out the clock so we hit the auth timeout
	time.Sleep(secondsToDuration(opts.AuthTimeout) * 2)
	line, _, _ = cr.ReadLine()
	if string(line) != "-ERR 'Authentication Timeout'" {
		t.Fatalf("Expected '-ERR 'Authentication Timeout'' but got %q", line)
	}

	// There shouldn't be a route entry as we didn't set up.
	if nc := s.NumRoutes(); nc != 0 {
		t.Fatalf("Server should have no route connections, got %v", nc)
	}
}

func TestRouteNoRaceOnClusterNameNegotiation(t *testing.T) {
	// Running the test 5 times was consistently producing the race.
	for i := 0; i < 5; i++ {
		o1 := DefaultOptions()
		// Set this cluster name as dynamic
		o1.Cluster.Name = _EMPTY_
		// Increase number of routes and pinned accounts to increase
		// the number of processRouteConnect() happening in parallel
		// to produce the race.
		o1.Cluster.PoolSize = 5
		o1.Cluster.PinnedAccounts = []string{"A", "B", "C", "D"}
		o1.Accounts = []*Account{NewAccount("A"), NewAccount("B"), NewAccount("C"), NewAccount("D")}
		s1 := RunServer(o1)
		defer s1.Shutdown()

		route := RoutesFromStr(fmt.Sprintf("nats://127.0.0.1:%d", o1.Cluster.Port))

		o2 := DefaultOptions()
		// Set this one as explicit. Use name that is likely to be "higher"
		// than the dynamic one.
		o2.Cluster.Name = "ZZZZZZZZZZZZZZZZZZZZZZZZZZZZZ"
		o2.Cluster.PoolSize = 5
		o2.Cluster.PinnedAccounts = []string{"A", "B", "C", "D"}
		o2.Accounts = []*Account{NewAccount("A"), NewAccount("B"), NewAccount("C"), NewAccount("D")}
		o2.Routes = route
		s2 := RunServer(o2)
		defer s2.Shutdown()
		checkClusterFormed(t, s1, s2)
		s2.Shutdown()
		s1.Shutdown()
	}
}

func TestRouteImplicitNotTooManyDuplicates(t *testing.T) {
	for _, test := range []struct {
		name        string
		pooling     bool
		compression bool
	}{
		{"no pooling-no compression", false, false},
		{"no pooling-compression", false, true},
		{"pooling-no compression", true, false},
		{"pooling-compression", true, true},
	} {
		t.Run(test.name, func(t *testing.T) {
			o := DefaultOptions()
			o.ServerName = "SEED"
			if !test.pooling {
				o.Cluster.PoolSize = -1
			}
			if !test.compression {
				o.Cluster.Compression.Mode = CompressionOff
			}
			seed := RunServer(o)
			defer seed.Shutdown()

			dl := &testDuplicateRouteLogger{}

			servers := make([]*Server, 0, 10)
			for i := 0; i < cap(servers); i++ {
				io := DefaultOptions()
				io.ServerName = fmt.Sprintf("IMPLICIT_%d", i+1)
				io.NoLog = false
				io.Routes = RoutesFromStr(fmt.Sprintf("nats://127.0.0.1:%d", o.Cluster.Port))
				if !test.pooling {
					io.Cluster.PoolSize = -1
				}
				if !test.compression {
					io.Cluster.Compression.Mode = CompressionOff
				}
				is, err := NewServer(io)
				require_NoError(t, err)
				// Will do defer of shutdown later.
				is.SetLogger(dl, true, false)
				is.Start()
				servers = append(servers, is)
			}

			allServers := make([]*Server, 0, len(servers)+1)
			allServers = append(allServers, seed)
			allServers = append(allServers, servers...)

			// Let's make sure that we wait for each server to be ready.
			for _, s := range allServers {
				if !s.ReadyForConnections(2 * time.Second) {
					t.Fatalf("Server %q is not ready for connections", s)
				}
			}

			// Do the defer of shutdown of all servers this way instead of individual
			// defers when starting them. It takes less time for the servers to shutdown
			// this way.
			defer func() {
				for _, s := range allServers {
					s.Shutdown()
				}
			}()
			checkClusterFormed(t, allServers...)

			dl.Mutex.Lock()
			count := dl.count
			dl.Mutex.Unlock()
			// Getting duplicates should not be considered fatal, it is an optimization
			// to reduce the occurrences of those. But to make sure we don't have a
			// regression, we will fail the test if we get say more than 20 or so (
			// without the code change, we would get more than 500 of duplicates).
			if count > 20 {
				t.Fatalf("Got more duplicates than anticipated: %v", count)
			}
		})
	}
}

func TestRouteImplicitJoinsSeparateGroups(t *testing.T) {
	// The test TestRouteImplicitNotTooManyDuplicates makes sure that we do
	// not have too many duplicate routes cases when processing implicit routes.
	// This test is to ensure that the code changes to reduce the number
	// of duplicate routes does not prevent the formation of the cluster
	// with the given setup (which is admittedly not good since a disconnect
	// between some routes would not result in a reconnect leading to a full mesh).
	// Still, original code was able to create the original full mesh, so we want
	// to make sure that this is still possible.
	for _, test := range []struct {
		name        string
		pooling     bool
		compression bool
	}{
		{"no pooling-no compression", false, false},
		{"no pooling-compression", false, true},
		{"pooling-no compression", true, false},
		{"pooling-compression", true, true},
	} {
		t.Run(test.name, func(t *testing.T) {
			setOpts := func(o *Options) {
				if !test.pooling {
					o.Cluster.PoolSize = -1
				}
				if !test.compression {
					o.Cluster.Compression.Mode = CompressionOff
				}
			}

			// Create a cluster s1/s2/s3
			o1 := DefaultOptions()
			o1.ServerName = "S1"
			setOpts(o1)
			s1 := RunServer(o1)
			defer s1.Shutdown()

			o2 := DefaultOptions()
			o2.ServerName = "S2"
			setOpts(o2)
			o2.Routes = RoutesFromStr(fmt.Sprintf("nats://127.0.0.1:%d", o1.Cluster.Port))
			s2 := RunServer(o2)
			defer s2.Shutdown()

			tmpl := `
				server_name: "S3"
				listen: "127.0.0.1:-1"
				cluster {
					name: "abc"
					listen: "127.0.0.1:-1"
					%s
					%s
					routes: ["nats://127.0.0.1:%d"%s]
				}
			`
			var poolCfg string
			var compressionCfg string
			if !test.pooling {
				poolCfg = "pool_size: -1"
			}
			if !test.compression {
				compressionCfg = "compression: off"
			}
			conf := createConfFile(t, []byte(fmt.Sprintf(tmpl, poolCfg, compressionCfg, o1.Cluster.Port, _EMPTY_)))
			s3, _ := RunServerWithConfig(conf)
			defer s3.Shutdown()

			checkClusterFormed(t, s1, s2, s3)

			// Now s4 and s5 connected to each other, but not linked to s1/s2/s3
			o4 := DefaultOptions()
			o4.ServerName = "S4"
			setOpts(o4)
			s4 := RunServer(o4)
			defer s4.Shutdown()

			o5 := DefaultOptions()
			o5.ServerName = "S5"
			setOpts(o5)
			o5.Routes = RoutesFromStr(fmt.Sprintf("nats://127.0.0.1:%d", o4.Cluster.Port))
			s5 := RunServer(o5)
			defer s5.Shutdown()

			checkClusterFormed(t, s4, s5)

			// Now add a route from s3 to s4 and make sure that we have a full mesh.
			routeToS4 := fmt.Sprintf(`, "nats://127.0.0.1:%d"`, o4.Cluster.Port)
			reloadUpdateConfig(t, s3, conf, fmt.Sprintf(tmpl, poolCfg, compressionCfg, o1.Cluster.Port, routeToS4))

			checkClusterFormed(t, s1, s2, s3, s4, s5)
		})
	}
}

// Extra regression test for issue fixed via:
//
// https://github.com/nats-io/nats-server/commit/d5ef552ae46d7c284e4bfdf01211f97893b2f38f
func TestRouteMsgWithManyHeadersDoesNotCorruptJSON(t *testing.T) {
	tmpl := `
		port: -1
		server_name: "%s"
		accounts {
			A {
				users: [ { user: a, password: pwd } ]
				exports: [ { service: "svc.>" } ]
			}
			B {
				users: [ { user: b, password: pwd } ]
				imports: [ { service: { account: A, subject: "svc.>" } } ]
			}
		}
		cluster {
			name: "TEST"
			port: -1
			compression: s2_fast
			%s
		}
	`
	conf1 := createConfFile(t, []byte(fmt.Sprintf(tmpl, "S1", "")))
	s1, o1 := RunServerWithConfig(conf1)
	defer s1.Shutdown()

	conf2 := createConfFile(t, []byte(fmt.Sprintf(tmpl, "S2",
		fmt.Sprintf("routes: [\"nats://127.0.0.1:%d\"]", o1.Cluster.Port))))
	s2, _ := RunServerWithConfig(conf2)
	defer s2.Shutdown()

	checkClusterFormed(t, s1, s2)

	// Account A responder on S2 — the destination of the service import.
	ncAResp := natsConnect(t, s2.ClientURL(), nats.UserInfo("a", "pwd"))
	defer ncAResp.Close()
	respCount := atomic.Int64{}
	_, err := ncAResp.Subscribe("svc.>", func(m *nats.Msg) {
		respCount.Add(1)
	})
	require_NoError(t, err)
	require_NoError(t, ncAResp.Flush())

	// Account B subscriber on S2 — receives over the route from S1.
	ncB2 := natsConnect(t, s2.ClientURL(), nats.UserInfo("b", "pwd"))
	defer ncB2.Close()
	subB2, err := ncB2.SubscribeSync("svc.>")
	require_NoError(t, err)
	defer subB2.Unsubscribe()
	require_NoError(t, subB2.SetPendingLimits(-1, -1))
	require_NoError(t, ncB2.Flush())

	checkSubInterest(t, s1, "A", "svc.foo", time.Second)
	checkSubInterest(t, s1, "B", "svc.foo", time.Second)
	checkSubInterest(t, s2, "B", "svc.foo", time.Second)

	total := 3000
	bodies := make([][]byte, total)
	for i := 0; i < total; i++ {
		filler := strings.Repeat("x", 32+rand.IntN(12*1024))
		body := fmt.Sprintf(
			`{"branch":"main","idx":%d,`+
				`"some_tag":"tag-aaaaaaaaaaaaaaaaaaaaaaaa",`+
				`"data":{"context":{"commit":"0123456789abcdef0123456789abcdef01234567",`+
				`"scope":"api","filler":%q},`+
				`"payload":{"client_ip_address":"2001:db8::1","seq":%d}}}`,
			i, filler, i)
		bodies[i] = []byte(body)
	}

	producers := 8
	var pwg sync.WaitGroup
	for p := 0; p < producers; p++ {
		pwg.Add(1)
		go func(p int) {
			defer pwg.Done()
			pnc := natsConnect(t, s1.ClientURL(), nats.UserInfo("b", "pwd"))
			defer pnc.Close()
			for i := p; i < total; i += producers {
				m := nats.NewMsg(fmt.Sprintf("svc.%d", i%64))
				m.Data = bodies[i]
				// Many headers, varying sizes, in a mix that sorts before and after "Nats-Request-Info"
				// so that it has both leading and trailing neighbors.
				m.Header.Set("Idx", strconv.Itoa(i))
				m.Header.Set("Nats-Msg-Id", fmt.Sprintf("msg-%06d", i))
				// Extra header which will be removed on the fly.
				m.Header.Set("Nats-Request-Info", `{"acc":"B","user":"b","host":"prefilled"}`)
				m.Header.Set("Trace-Id", "trace-aaaaaaaaaaaaaaaaaaaaaaaa")
				m.Header.Set("Span-Id", "span-bbbbbbbbbbbbbbbbbbbbbbbb")
				m.Header.Set("Request-Id", "request-cccccccccccccccccccccccc")
				m.Header.Set("Source-App", "test-app")
				m.Header.Set("X-Filler", strings.Repeat("filler-", 16)+strconv.Itoa(rand.Int()))
				m.Header.Set("X-Tail-Header", fmt.Sprintf("idx=%d-stable-tail-data", i))
				if err := pnc.PublishMsg(m); err != nil {
					t.Errorf("publish %d: %v", i, err)
					return
				}
			}
			_ = pnc.Flush()
		}(p)
	}
	pwg.Wait()

	seen := make(map[int]bool, total)
	deadline := time.Now().Add(120 * time.Second)
	for len(seen) < total {
		if time.Now().After(deadline) {
			t.Fatalf("subB2: timed out: got %d/%d", len(seen), total)
		}
		m, err := subB2.NextMsg(2 * time.Second)
		if err != nil {
			continue
		}
		var parsed struct {
			Branch string `json:"branch"`
			Idx    int    `json:"idx"`
			Tag    string `json:"some_tag"`
			Data   struct {
				Context struct {
					Commit string `json:"commit"`
					Scope  string `json:"scope"`
					Filler string `json:"filler"`
				} `json:"context"`
				Payload struct {
					ClientIPAddress string `json:"client_ip_address"`
					Seq             int    `json:"seq"`
				} `json:"payload"`
			} `json:"data"`
		}
		if err := json.Unmarshal(m.Data, &parsed); err != nil {
			t.Fatalf("subB2 json.Unmarshal failed: %v: data[:256]=%q",
				err, m.Data[:min(256, len(m.Data))])
		}
		// Ensure all headers are still present.
		idxStr := m.Header.Get("Idx")
		if idxStr == "" {
			keys := make([]string, 0, len(m.Header))
			for k := range m.Header {
				keys = append(keys, k)
			}
			t.Fatalf("subB2: received message with no/empty Idx header (corruption): subj=%s data[:128]=%q allHeaderKeys=%v",
				m.Subject, m.Data[:min(128, len(m.Data))], keys)
		}
		// Detect garbage header keys (e.g., 'c"' from leftover '"acc":"B"...')
		// and silently dropped expected keys.
		expectedKeys := map[string]bool{
			"Idx": true, "Nats-Msg-Id": true, "Nats-Request-Info": true,
			"Trace-Id": true, "Span-Id": true, "Request-Id": true,
			"Source-App": true, "X-Filler": true, "X-Tail-Header": true,
		}
		gotKeys := make(map[string]bool, len(m.Header))
		for k := range m.Header {
			gotKeys[k] = true
		}
		for k := range gotKeys {
			if !expectedKeys[k] {
				keys := make([]string, 0, len(gotKeys))
				for kk := range gotKeys {
					keys = append(keys, kk)
				}
				t.Fatalf("subB2 idx=%s: unexpected header key %q (corruption); allKeys=%v",
					idxStr, k, keys)
			}
		}
		for k := range expectedKeys {
			if !gotKeys[k] {
				keys := make([]string, 0, len(gotKeys))
				for kk := range gotKeys {
					keys = append(keys, kk)
				}
				t.Fatalf("subB2 idx=%s: expected header key %q missing (corruption); allKeys=%v",
					idxStr, k, keys)
			}
		}
		idx, err := strconv.Atoi(idxStr)
		require_NoError(t, err)
		// Bounds check so corruption that rewrites Idx to an out-of-range value
		// cannot inflate len(seen) and produce a false negative.
		if idx < 0 || idx >= total {
			t.Fatalf("subB2: Idx header out of range (corruption): idx=%d total=%d data[:128]=%q",
				idx, total, m.Data[:min(128, len(m.Data))])
		}

		// Check that parsed JSON contains the expected data.
		if parsed.Idx != idx {
			t.Fatalf("subB2 idx=%d: parsed idx mismatch got=%d", idx, parsed.Idx)
		}
		if parsed.Data.Payload.Seq != idx {
			t.Fatalf("subB2 idx=%d: parsed payload.seq mismatch got=%d", idx, parsed.Data.Payload.Seq)
		}
		// Headers should round-trip intact.
		if got := m.Header.Get("Idx"); got != idxStr {
			t.Fatalf("subB2 idx=%d: Idx header corrupted, got=%q", idx, got)
		}
		if got := m.Header.Get("Nats-Msg-Id"); got != fmt.Sprintf("msg-%06d", idx) {
			t.Fatalf("subB2 idx=%d: Nats-Msg-Id corrupted, got=%q", idx, got)
		}
		wantTail := fmt.Sprintf("idx=%d-stable-tail-data", idx)
		if got := m.Header.Get("X-Tail-Header"); got != wantTail {
			t.Fatalf("subB2 idx=%d: X-Tail-Header corrupted, got=%q want=%q", idx, got, wantTail)
		}
		seen[idx] = true
	}

	// Check that all messages were received.
	checkFor(t, 5*time.Second, 50*time.Millisecond, func() error {
		if got := respCount.Load(); got != int64(total) {
			return fmt.Errorf("Account A responder: got %d/%d messages", got, total)
		}
		return nil
	})
}

func TestRouteConfigureWriteDeadline(t *testing.T) {
	o1, o2 := DefaultOptions(), DefaultOptions()

	o1.Cluster.WriteDeadline = 5 * time.Second
	s1 := RunServer(o1)
	defer s1.Shutdown()

	o2.Cluster.WriteDeadline = 6 * time.Second
	o2.Routes = RoutesFromStr(fmt.Sprintf("nats://127.0.0.1:%d", o1.Cluster.Port))
	s2 := RunServer(o2)
	defer s2.Shutdown()

	checkClusterFormed(t, s1, s2)

	s1.mu.RLock()
	s2.mu.RLock()
	defer s1.mu.RUnlock()
	defer s2.mu.RUnlock()

	s1.forEachRoute(func(r *client) {
		require_Equal(t, r.out.wdl, 5*time.Second)
	})

	s2.forEachRoute(func(r *client) {
		require_Equal(t, r.out.wdl, 6*time.Second)
	})
}

func TestRouteConfigureWriteTimeoutPolicy(t *testing.T) {
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

			s1.forEachRoute(func(r *client) {
				if policy == WriteTimeoutPolicyDefault {
					require_Equal(t, r.out.wtp, WriteTimeoutPolicyRetry)
				} else {
					require_Equal(t, r.out.wtp, policy)
				}
			})
		})
	}
}

func TestRoutePoolFirstPongBlocksChain(t *testing.T) {
	// Use a very short ping interval so the timer fires quickly, before we (the mock server) send our INFO.
	orgMaxPing := routeMaxPingInterval
	routeMaxPingInterval = 200 * time.Millisecond
	defer func() { routeMaxPingInterval = orgMaxPing }()

	// Start a TCP listener acting as a mock route server. S2 will solicit a connection to this listener.
	mockListener, err := net.Listen("tcp", "127.0.0.1:0")
	require_NoError(t, err)
	defer mockListener.Close()

	mockPort := mockListener.Addr().(*net.TCPAddr).Port

	o2 := DefaultOptions()
	o2.ServerName = "S2"
	o2.Cluster.Name = "local"
	o2.Cluster.PoolSize = 3
	o2.Cluster.Compression.Mode = CompressionOff
	o2.Cluster.MaxPingsOut = 10
	o2.Routes = RoutesFromStr(fmt.Sprintf("nats://127.0.0.1:%d", mockPort))
	s2 := RunServer(o2)
	defer s2.Shutdown()

	var ready sync.WaitGroup
	var keepAlive sync.WaitGroup
	mockListener.(*net.TCPListener).SetDeadline(time.Now().Add(5 * time.Second))
	handleConnection := func() {
		conn, err := mockListener.Accept()
		if err != nil {
			ready.Done()
			require_NoError(t, err)
		}
		defer conn.Close()
		br := bufio.NewReader(conn)

		// Ensure we call these before conn.Close() above.
		// We have to wait for both connections to be ready and then wait for the test to finish.
		defer keepAlive.Wait()
		defer ready.Done()

		// S2 sends CONNECT immediately (solicited route).
		// Read it but do NOT send our INFO yet — we want S2's timer to fire first.
		require_NoError(t, conn.SetReadDeadline(time.Now().Add(5*time.Second)))

		// Read S2's CONNECT line.
		line, err := br.ReadString('\n')
		require_NoError(t, err)
		require_True(t, strings.HasPrefix(line, "CONNECT"))

		// Wait for S2's timer PING to arrive.
		// The readLoop on S2 is blocked waiting for our INFO, so the
		// timer goroutine enqueues the PING on S2's outbound buffer.
		checkFor(t, 5*time.Second, 100*time.Millisecond, func() error {
			if _, err = conn.Write([]byte("PING\r\n")); err != nil {
				return err
			}
			if line, err = br.ReadString('\n'); err != nil {
				return err
			}
			t.Logf("S2 received: %q", line)
			if !strings.HasPrefix(line, "PING") {
				return fmt.Errorf("expected PING, got %q", line)
			}
			return nil
		})

		// Respond with PONG — this sets firstPong on S2's route connection
		// BEFORE addRoute has run (since we haven't sent INFO yet).
		_, err = conn.Write([]byte("PONG\r\n"))
		require_NoError(t, err)

		// NOW send our INFO. This triggers S2's processRouteInfo → addRoute,
		// which sets startNewRoute and sends another PING.
		mockInfo := Info{
			ID:            "MOCK_SERVER_ID",
			Name:          "mock-server",
			Host:          "127.0.0.1",
			Port:          mockPort,
			Cluster:       "local",
			Headers:       true,
			Proto:         1,
			RoutePoolSize: 3,
		}
		infoJSON, err := json.Marshal(mockInfo)
		require_NoError(t, err)
		_, err = fmt.Fprintf(conn, "INFO %s\r\n", infoJSON)
		require_NoError(t, err)

		// Read S2's delayed INFO (sent during addRoute) + subscription data + PING.
		// We need to consume everything up to and including the PING.
		for {
			line, err = br.ReadString('\n')
			require_NoError(t, err)
			if strings.HasPrefix(line, "PING") {
				break
			}
		}

		// Respond to addRoute's PING with PONG.
		_, err = conn.Write([]byte("PONG\r\n"))
		require_NoError(t, err)
	}

	// Keep the below connections alive until the test is done.
	// This allows us to check that S2 creates the next pool connection.
	keepAlive.Add(1)
	defer keepAlive.Done()

	ready.Add(2)
	for range 2 {
		go handleConnection()
	}
	ready.Wait()

	// Check if S2 attempts to create a second pool connection by
	// trying to accept another connection on our mock listener.
	secondConnCh := make(chan struct{}, 1)
	go func() {
		mockListener.(*net.TCPListener).SetDeadline(time.Now().Add(2 * time.Second))
		if c2, err := mockListener.Accept(); err == nil {
			c2.Close()
			secondConnCh <- struct{}{}
		}
	}()

	select {
	case <-secondConnCh:
		// Good — S2 tried to create the next pool connection.
		// The fix works: startNewRoute was consumed despite firstPong.
	case <-time.After(2 * time.Second):
		t.Fatalf("S2 did not attempt to create next pool connection; " +
			"firstPong blocked startNewRoute consumption, pool chain is broken")
	}
}

// https://github.com/nats-io/nats-server/issues/8233
func TestRouteSubUnsubRaceLosesRemoteInterest(t *testing.T) {
	oa := DefaultOptions()
	sa := RunServer(oa)
	defer sa.Shutdown()

	ob := DefaultOptions()
	ob.Routes = RoutesFromStr(fmt.Sprintf("nats://127.0.0.1:%d", oa.Cluster.Port))
	sb := RunServer(ob)
	defer sb.Shutdown()

	checkClusterFormed(t, sa, sb)

	// A client on node B, used to confirm the user-visible symptom: a request to
	// the subject gets "no responders" even though a live responder exists on A.
	ncReq := natsConnect(t, sb.ClientURL())
	defer ncReq.Close()

	// Many subjects raced at once so they all contend the same global-account
	// acc.mu and the single pinned A->B route.mu.
	const (
		conns = 100
		waves = 10
	)

	// Connection pools, reused across waves. ncMinus owns the original subscription
	// that gets removed, ncPlus adds the new responder that must survive. They are
	// distinct connections so their operations run on different readLoop goroutines
	// on node A and can race.
	ncMinus := make([]*nats.Conn, conns)
	ncPlus := make([]*nats.Conn, conns)
	for i := 0; i < conns; i++ {
		ncMinus[i] = natsConnect(t, sa.ClientURL())
		defer ncMinus[i].Close()
		ncPlus[i] = natsConnect(t, sa.ClientURL())
		defer ncPlus[i].Close()
	}

	accB := sb.globalAccount()
	for w := range waves {
		subjects := make([]string, conns)
		minusSubs := make([]*nats.Subscription, conns)
		for i := range conns {
			subjects[i] = fmt.Sprintf("foo.%d.%d", w, i)
			// Original interest, mirrored to B via RS+. This is the "live"
			// subscription on B that makes the reordered RS+ a no-op.
			minusSubs[i] = natsSubSync(t, ncMinus[i], subjects[i])
		}
		for i := range conns {
			natsFlush(t, ncMinus[i])
		}
		// Make sure B sees all of the original interest before we start, so its
		// route already tracks each key.
		for i := range conns {
			checkSubInterest(t, sb, globalAccountName, subjects[i], 5*time.Second)
		}

		// Fire the wave: for each subject, simultaneously remove the original
		// interest (-1) and add a replacement responder (+1) on node A.
		start := make(chan struct{})
		var wg sync.WaitGroup
		wg.Add(2 * conns)
		for i := range conns {
			go func() {
				defer wg.Done()
				<-start
				minusSubs[i].Unsubscribe()
				ncMinus[i].Flush()
			}()
			go func() {
				defer wg.Done()
				<-start
				ncPlus[i].Subscribe(subjects[i], func(m *nats.Msg) {
					m.Respond([]byte("ok"))
				})
				ncPlus[i].Flush()
			}()
		}
		close(start)
		wg.Wait()

		// Barrier: subscribe a marker on the same (pinned) route and wait for B
		// to observe it. Because all of the wave's RS+/RS- protos were enqueued
		// on that same route connection before this one, FIFO ordering
		// guarantees that once B has the marker it has processed every proto
		// from the wave.
		marker := fmt.Sprintf("marker.%d", w)
		natsSubSync(t, ncPlus[0], marker)
		natsFlush(t, ncPlus[0])
		checkSubInterest(t, sb, globalAccountName, marker, 5*time.Second)

		// Every subject still has a live local responder on A (the ncPlus sub),
		// so B must still have interest. If the race fired, B silently lost it.
		for i := range conns {
			if accB.SubscriptionInterest(subjects[i]) {
				continue
			}
			// Confirm the exact reported symptom: a request from a client on B
			// gets "no responders" although A has a live responder.
			_, rerr := ncReq.Request(subjects[i], []byte("ping"), time.Second)
			t.Fatalf("wave %d: node B lost interest in %q while node A still has a "+
				"live local responder; request from B returned %v (want a reply). "+
				"RS+/RS- were reordered on the A->B route by the "+
				"updateRouteSubscriptionMap race", w, subjects[i], rerr)
		}
	}
}

// Benchmarks for message arg processing functions to measure heap allocations.
// These functions parse incoming protocol messages and split arguments.

func BenchmarkProcessRoutedMsgArgs(b *testing.B) {
	// RMSG format: account subject [reply] size
	arg := []byte("$G foo.bar _INBOX.xxx 1024")
	c := &client{kind: ROUTER, route: &route{}}
	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		if err := c.processRoutedMsgArgs(arg); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkProcessRoutedHeaderMsgArgs(b *testing.B) {
	// HMSG format: account subject [reply] headerSize totalSize
	arg := []byte("$G foo.bar 12 1024")
	c := &client{kind: ROUTER, route: &route{}}
	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		if err := c.processRoutedHeaderMsgArgs(arg); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkProcessRoutedOriginClusterMsgArgs(b *testing.B) {
	// Origin cluster HMSG format: origin account subject [reply] headerSize totalSize
	arg := []byte("ORIGIN MY_ACCOUNT foo.bar 12 345")
	c := &client{kind: ROUTER, route: &route{}}
	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		if err := c.processRoutedOriginClusterMsgArgs(arg); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkProcessLeafMsgArgs(b *testing.B) {
	// LMSG format: subject [reply] size
	arg := []byte("foo.bar _INBOX.xxx 1024")
	c := &client{kind: LEAF}
	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		if err := c.processLeafMsgArgs(arg); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkProcessLeafHeaderMsgArgs(b *testing.B) {
	// Leaf HMSG format: subject headerSize totalSize
	arg := []byte("foo.bar 12 1024")
	c := &client{kind: LEAF}
	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		if err := c.processLeafHeaderMsgArgs(arg); err != nil {
			b.Fatal(err)
		}
	}
}

// Benchmarks with queue subscribers to exercise the larger arg paths.

func BenchmarkProcessRoutedMsgArgs_Queues(b *testing.B) {
	// RMSG format with queues: account subject replyIndicator reply queue1 queue2 size
	arg := []byte("$G foo.bar + _INBOX.xxx queue1 queue2 1024")
	c := &client{kind: ROUTER, route: &route{}}
	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		if err := c.processRoutedMsgArgs(arg); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkProcessRoutedHeaderMsgArgs_Queues(b *testing.B) {
	// HMSG format with queues: account subject replyIndicator reply queue1 queue2 headerSize totalSize
	arg := []byte("$G foo.bar + _INBOX.xxx queue1 queue2 12 1024")
	c := &client{kind: ROUTER, route: &route{}}
	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		if err := c.processRoutedHeaderMsgArgs(arg); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkProcessLeafMsgArgs_Queues(b *testing.B) {
	// LMSG format with queues: subject replyIndicator reply queue1 queue2 size
	arg := []byte("foo.bar + _INBOX.xxx queue1 queue2 1024")
	c := &client{kind: LEAF}
	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		if err := c.processLeafMsgArgs(arg); err != nil {
			b.Fatal(err)
		}
	}
}

func TestRouteReconnectAfterSolicitedUpgradeAdoptsConfiguredURL(t *testing.T) {
	// Default pool size + pinned $SYS route, like a production setup, with a
	// hostname-based route URL like production configs use.
	ob := DefaultOptions()
	sb := RunServer(ob)
	defer sb.Shutdown()

	oa := DefaultOptions()
	oa.Routes = RoutesFromStr(fmt.Sprintf("nats://localhost:%d", sb.ClusterAddr().Port))
	sa := RunServer(oa)
	defer sa.Shutdown()
	checkClusterFormed(t, sa, sb)

	// Find A's pooled route connection for the slot that carries the global
	// account's interest.
	gSlot := computeRoutePoolIdx(sa.getOpts().Cluster.PoolSize, globalAccountName)
	var route *client
	sa.mu.RLock()
	sa.forEachRouteIdx(gSlot, func(r *client) bool {
		route = r
		return false
	})
	sa.mu.RUnlock()
	require_NotNil(t, route)

	// Put the connection in the pre-upgrade gossip-dial state, then run the
	// actual upgrade with the configured URL, as duplicate-route resolution
	// does. The upgrade must adopt the configured URL so the later reconnect
	// passes routeStillValid.
	gossipURL, err := url.Parse(fmt.Sprintf("nats-route://127.0.0.1:%d/", sb.ClusterAddr().Port))
	require_NoError(t, err)
	route.mu.Lock()
	route.route.didSolicit = true
	route.route.routeType = Implicit
	route.route.url = gossipURL
	route.mu.Unlock()

	configured := oa.Routes[0]
	upgradeRouteToSolicited(route, configured, Explicit)

	route.mu.Lock()
	upURL := route.route.url
	route.mu.Unlock()
	require_Equal(t, upURL, configured)

	// Drop the connection the way a fault would (slow consumer). The sibling
	// pool slots and the pinned $SYS route stay up, so the remote is never
	// treated as new and only this reconnect can re-establish the slot.
	route.closeConnection(SlowConsumerWriteDeadline)
	checkClusterFormed(t, sa, sb)

	// Interest must flow across the re-established slot again.
	nca := natsConnect(t, sa.ClientURL())
	defer nca.Close()
	ncb := natsConnect(t, sb.ClientURL())
	defer ncb.Close()
	natsSub(t, ncb, "reconnect.echo", func(m *nats.Msg) { m.Respond(m.Data) })
	natsFlush(t, ncb)
	checkSubInterest(t, sa, globalAccountName, "reconnect.echo", 2*time.Second)
	_, err = nca.Request("reconnect.echo", []byte("ping"), time.Second)
	require_NoError(t, err)
}

func TestRouteReconnectAfterDuplicateRouteAdoptsConfiguredURL(t *testing.T) {
	// handleDuplicateRoute is the other path that promotes a route to Explicit,
	// used for duplicate per-account (pinned) routes and for any duplicate
	// route in non-pool mode. It must adopt the configured URL too, otherwise
	// routeStillValid rejects the gossiped one and the reconnect is abandoned.
	// The global account is pinned here so interest can use ordinary pub/sub.
	ob := DefaultOptions()
	ob.Cluster.PinnedAccounts = []string{globalAccountName}
	sb := RunServer(ob)
	defer sb.Shutdown()

	oa := DefaultOptions()
	oa.Cluster.PinnedAccounts = []string{globalAccountName}
	oa.Routes = RoutesFromStr(fmt.Sprintf("nats://localhost:%d", sb.ClusterAddr().Port))
	sa := RunServer(oa)
	defer sa.Shutdown()
	checkClusterFormed(t, sa, sb)

	// Grab A's pinned route for the global account.
	var pinned *client
	sa.mu.RLock()
	for _, r := range sa.accRoutes[globalAccountName] {
		pinned = r
	}
	sa.mu.RUnlock()
	require_NotNil(t, pinned)

	// Put the connection in the pre-upgrade gossip-dial state, then resolve it
	// as a duplicate against the configured explicit route, as addRoute does
	// for a pinned account. The upgrade must adopt the configured URL so the
	// later reconnect passes routeStillValid.
	gossipURL, err := url.Parse(fmt.Sprintf("nats-route://127.0.0.1:%d/", sb.ClusterAddr().Port))
	require_NoError(t, err)
	pinned.mu.Lock()
	pinned.route.didSolicit = true
	pinned.route.routeType = Implicit
	pinned.route.url = gossipURL
	pinned.mu.Unlock()

	configured := oa.Routes[0]
	dup := &client{}
	dup.route = &route{
		url:        configured,
		didSolicit: true,
		routeType:  Explicit,
		accName:    []byte(globalAccountName),
	}
	handleDuplicateRoute(pinned, dup, true)

	pinned.mu.Lock()
	upURL := pinned.route.url
	pinned.mu.Unlock()
	require_Equal(t, upURL, configured)

	// Drop the connection the way a fault would (slow consumer). The pinned
	// route must come back, otherwise this account's mesh stays severed while
	// every pooled route looks healthy.
	pinned.closeConnection(SlowConsumerWriteDeadline)
	checkClusterFormed(t, sa, sb)

	checkFor(t, 5*time.Second, 50*time.Millisecond, func() error {
		sa.mu.RLock()
		defer sa.mu.RUnlock()
		for _, r := range sa.accRoutes[globalAccountName] {
			if r != nil && r != pinned {
				return nil
			}
		}
		return fmt.Errorf("pinned route for %q did not reconnect", globalAccountName)
	})

	// Interest must flow across the re-established pinned route again.
	nca := natsConnect(t, sa.ClientURL())
	defer nca.Close()
	ncb := natsConnect(t, sb.ClientURL())
	defer ncb.Close()
	natsSub(t, ncb, "pinned.echo", func(m *nats.Msg) { m.Respond(m.Data) })
	natsFlush(t, ncb)
	checkSubInterest(t, sa, globalAccountName, "pinned.echo", 2*time.Second)
	_, err = nca.Request("pinned.echo", []byte("ping"), time.Second)
	require_NoError(t, err)
}
