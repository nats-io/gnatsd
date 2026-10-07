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
	"crypto/tls"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"math/rand/v2"
	"net"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"reflect"
	"runtime"
	"slices"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"
	"unicode"

	"github.com/nats-io/jwt/v2"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nkeys"
)

const CLIENT_PORT = -1
const MONITOR_PORT = -1
const CLUSTER_PORT = -1

func DefaultMonitorOptions() *Options {
	return &Options{
		Host:         "127.0.0.1",
		Port:         CLIENT_PORT,
		HTTPHost:     "127.0.0.1",
		HTTPPort:     MONITOR_PORT,
		HTTPBasePath: "/",
		ServerName:   "monitor_server",
		NoLog:        true,
		NoSigs:       true,
		Tags:         []string{"tag"},
		Metadata:     map[string]string{"key1": "value1", "key2": "value2"},
		FeatureFlags: map[string]bool{"feature": false, "fix": true, "revert_fix": true},
	}
}

func runMonitorJSServer(t *testing.T, clientPort int, monitorPort int, clusterPort int, routePort int) (*Server, *Options) {
	resetPreviousHTTPConnections()
	tmpDir := t.TempDir()
	cf := createConfFile(t, []byte(fmt.Sprintf(`
		listen: 127.0.0.1:%d
		http: 127.0.0.1:%d
		system_account: SYS
		accounts {
			SYS {
				users [{user: sys, password: pwd}]
			}
			ACC {
				users [{user: usr, password: pwd}]
				// In clustered mode, these reservations will not impact any one server.
				jetstream: {max_store: 4Mb, max_memory: 5Mb}
			}
			BCC_TO_HAVE_ONE_EXTRA {
				users [{user: usr2, password: pwd}]
				jetstream: enabled
			}
		}
		jetstream: {
			max_mem_store: 10Mb
			max_file_store: 10Mb
			store_dir: '%s'
			unique_tag: az
			limits: {
				max_ha_assets: 1000
			}
		}
		cluster {
			name: cluster_name
			listen: 127.0.0.1:%d
			routes: [nats-route://127.0.0.1:%d]
		}
		server_name: server_%d
		server_tags: [ "az:%d", "tag" ] `, clientPort, monitorPort, tmpDir, clusterPort, routePort, clientPort, clientPort)))

	return RunServerWithConfig(cf)
}

func runMonitorServer() *Server {
	resetPreviousHTTPConnections()
	opts := DefaultMonitorOptions()
	opts.NoSystemAccount = true
	return RunServer(opts)
}

func runMonitorServerWithAccounts() *Server {
	resetPreviousHTTPConnections()
	opts := DefaultMonitorOptions()
	opts.NoSystemAccount = true
	aA := NewAccount("A")
	aB := NewAccount("B")
	opts.Accounts = append(opts.Accounts, aA, aB)
	opts.Users = append(opts.Users,
		&User{Username: "a", Password: "a", Account: aA},
		&User{Username: "b", Password: "b", Account: aB})
	return RunServer(opts)
}

func runMonitorServerNoHTTPPort() *Server {
	resetPreviousHTTPConnections()
	opts := DefaultMonitorOptions()
	opts.NoSystemAccount = true
	opts.HTTPPort = 0
	return RunServer(opts)
}

func resetPreviousHTTPConnections() {
	http.DefaultTransport.(*http.Transport).CloseIdleConnections()
}

func TestMyUptime(t *testing.T) {
	// Make sure we print this stuff right.
	var d time.Duration
	var s string

	d = 22 * time.Second
	s = myUptime(d)
	if s != "22s" {
		t.Fatalf("Expected `22s`, go ``%s`", s)
	}
	d = 4*time.Minute + d
	s = myUptime(d)
	if s != "4m22s" {
		t.Fatalf("Expected `4m22s`, go ``%s`", s)
	}
	d = 4*time.Hour + d
	s = myUptime(d)
	if s != "4h4m22s" {
		t.Fatalf("Expected `4h4m22s`, go ``%s`", s)
	}
	d = 32*24*time.Hour + d
	s = myUptime(d)
	if s != "32d4h4m22s" {
		t.Fatalf("Expected `32d4h4m22s`, go ``%s`", s)
	}
	d = 22*365*24*time.Hour + d
	s = myUptime(d)
	if s != "22y32d4h4m22s" {
		t.Fatalf("Expected `22y32d4h4m22s`, go ``%s`", s)
	}
}

// Make sure that we do not run the http server for monitoring unless asked.
func TestMonitorNoPort(t *testing.T) {
	s := runMonitorServerNoHTTPPort()
	defer s.Shutdown()

	// this test might be meaningless now that we're testing with random ports?
	url := fmt.Sprintf("http://127.0.0.1:%d/", 11245)
	if resp, err := http.Get(url + "varz"); err == nil {
		t.Fatalf("Expected error: Got %+v\n", resp)
	}
	if resp, err := http.Get(url + "healthz"); err == nil {
		t.Fatalf("Expected error: Got %+v\n", resp)
	}
	if resp, err := http.Get(url + "connz"); err == nil {
		t.Fatalf("Expected error: Got %+v\n", resp)
	}
}

var (
	appJSONContent = "application/json"
	textPlain      = "text/plain; charset=utf-8"
	textHTML       = "text/html; charset=utf-8"
)

func readBodyEx(t *testing.T, url string, status int, content string) []byte {
	t.Helper()
	resp, err := http.Get(url)
	if err != nil {
		t.Fatalf("Expected no error: Got %v\n", err)
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("Got an error reading the body: %v\n", err)
	}
	if resp.StatusCode != status {
		t.Fatalf("Expected a %d response, got %d\n%s", status, resp.StatusCode, string(body))
	}
	ct := resp.Header.Get("Content-Type")
	if ct != content {
		t.Fatalf("Expected %q content-type, got %q\n", content, ct)
	}
	// Check the CORS header for "application/json" requests only.
	if ct == appJSONContent {
		acao := resp.Header.Get("Access-Control-Allow-Origin")
		if acao != "*" {
			t.Fatalf("Expected with %q Content-Type an Access-Control-Allow-Origin header with value %q, got %q\n", appJSONContent, "*", acao)
		}
	}
	return body
}

func TestMonitorHTTPBasePath(t *testing.T) {
	resetPreviousHTTPConnections()
	opts := DefaultMonitorOptions()
	opts.NoSystemAccount = true
	opts.HTTPBasePath = "/nats"

	s := RunServer(opts)
	defer s.Shutdown()

	url := fmt.Sprintf("http://127.0.0.1:%d/nats", s.MonitorAddr().Port)
	readBodyEx(t, url, http.StatusOK, textHTML)
}

func readBody(t *testing.T, url string) []byte {
	t.Helper()
	return readBodyEx(t, url, http.StatusOK, appJSONContent)
}

func pollVarz(t *testing.T, s *Server, mode int, url string, opts *VarzOptions) *Varz {
	t.Helper()
	if mode == 0 {
		v := &Varz{}
		body := readBody(t, url)
		if err := json.Unmarshal(body, v); err != nil {
			t.Fatalf("Got an error unmarshalling the body: %v\n", err)
		}
		return v
	}
	v, err := s.Varz(opts)
	if err != nil {
		t.Fatalf("Error on Varz: %v", err)
	}
	return v
}

// https://github.com/nats-io/nats-server/issues/2170
// Just the ever increasing subs part.
func TestMonitorVarzSubscriptionsResetProperly(t *testing.T) {
	// Run with JS to create a bunch of subs to start.
	resetPreviousHTTPConnections()
	opts := DefaultMonitorOptions()
	opts.JetStream = true
	s := RunServer(opts)
	defer s.Shutdown()

	// This bug seems to only happen via the http endpoint, not direct calls.
	// Every time you call it doubles.
	url := fmt.Sprintf("http://127.0.0.1:%d/varz", s.MonitorAddr().Port)
	osubs := pollVarz(t, s, 0, url, nil).Subscriptions
	// Make sure we get same number back.
	if v := pollVarz(t, s, 0, url, nil); v.Subscriptions != osubs {
		t.Fatalf("Expected subs to stay the same, %d vs %d", osubs, v.Subscriptions)
	}
}

// Must be run with -race.
func TestMonitorVarzReloadRace(t *testing.T) {
	resetPreviousHTTPConnections()
	opts := DefaultMonitorOptions()
	opts.NoSystemAccount = true
	s := RunServer(opts)
	defer s.Shutdown()

	url := fmt.Sprintf("http://127.0.0.1:%d/varz", s.MonitorAddr().Port)

	// Ensure s.varz is populated so the marshal path reads the shared object.
	_, err := http.Get(url)
	require_NoError(t, err)

	var wg sync.WaitGroup
	stop := make(chan struct{})

	// Reader: hit /varz, which marshals s.varz under s.varzMu.
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-stop:
				return
			default:
				resp, err := http.Get(url)
				if err == nil {
					io.Copy(io.Discard, resp.Body)
					resp.Body.Close()
				}
			}
		}
	}()

	// Writer: reload, which mutates s.varz via updateVarzConfigReloadableFields.
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-stop:
				return
			default:
				if err := s.ReloadOptions(s.getOpts().Clone()); err != nil {
					t.Errorf("Error on reload: %v", err)
					return
				}
			}
		}
	}()

	time.Sleep(500 * time.Millisecond)
	close(stop)
	wg.Wait()
}

func TestMonitorHandleVarz(t *testing.T) {
	s, _ := runMonitorJSServer(t, -1, -1, 0, 0)
	defer s.Shutdown()

	url := fmt.Sprintf("http://127.0.0.1:%d/", s.MonitorAddr().Port)

	for mode := 0; mode < 2; mode++ {
		v := pollVarz(t, s, mode, url+"varz", nil)

		// Do some sanity checks on values
		if time.Since(v.Start) > 10*time.Second {
			t.Fatal("Expected start time to be within 10 seconds.")
		}
	}

	time.Sleep(100 * time.Millisecond)

	nc := createClientConnWithUserSubscribeAndPublish(t, s, "sys", "pwd")
	defer nc.Close()

	for mode := 0; mode < 2; mode++ {
		v := pollVarz(t, s, mode, url+"varz", nil)

		if v.Connections != 1 {
			t.Fatalf("Expected Connections of 1, got %v\n", v.Connections)
		}
		if v.TotalConnections < 1 {
			t.Fatalf("Expected Total Connections of at least 1, got %v\n", v.TotalConnections)
		}
		if v.InMsgs != 1 {
			t.Fatalf("Expected InMsgs of 1, got %v\n", v.InMsgs)
		}
		if v.InBytes != 5 {
			t.Fatalf("Expected InBytes of 5, got %v\n", v.InBytes)
		}
		if v.OutMsgs != 1 {
			t.Fatalf("Expected OutMsgs of 1, got %v\n", v.OutMsgs)
		}
		if v.OutBytes != 5 {
			t.Fatalf("Expected OutBytes of 5, got %v\n", v.OutBytes)
		}
		if v.InClientMsgs != 1 {
			t.Fatalf("Expected InClientMsgs of 1, got %v\n", v.InClientMsgs)
		}
		if v.InClientBytes != 5 {
			t.Fatalf("Expected InClientBytes of 5, got %v\n", v.InClientBytes)
		}
		if v.OutClientMsgs != 1 {
			t.Fatalf("Expected OutClientMsgs of 1, got %v\n", v.OutClientMsgs)
		}
		if v.OutClientBytes != 5 {
			t.Fatalf("Expected OutClientBytes of 5, got %v\n", v.OutClientBytes)
		}
		if v.Subscriptions <= 10 {
			t.Fatalf("Expected Subscriptions of at least 10, got %v\n", v.Subscriptions)
		}
		if v.Name != "server_-1" {
			t.Fatalf("Expected ServerName to be 'monitor_server' got %q", v.Name)
		}
		if !v.Tags.Contains("tag") {
			t.Fatalf("Expected tags to be 'tag' got %v", v.Tags)
		}
		if v.JetStream.Config == nil {
			t.Fatalf("JS Config not set")
		}
		sd := filepath.Join(s.opts.StoreDir, "jetstream")
		if v.JetStream.Config.StoreDir != sd {
			t.Fatalf("JS Config is invalid expected %q got %q", sd, v.JetStream.Config.StoreDir)
		}
		if v.JetStream.Stats == nil {
			t.Fatalf("JS Stats not set")
		}
		if v.JetStream.Stats.Accounts != 2 {
			t.Fatalf("Invalid stats expected 2 accounts got %d", v.JetStream.Stats.Accounts)
		}
		if v.JetStream.Limits == nil {
			t.Fatalf("JS limits not set")
		}
		if v.JetStream.Limits.MaxHAAssets != 1000 {
			t.Fatalf("Expected 1000 max_ha_assets got %v", v.JetStream.Limits.MaxHAAssets)
		}
	}
}

func pollConnz(t *testing.T, s *Server, mode int, url string, opts *ConnzOptions) *Connz {
	t.Helper()
	if mode == 0 {
		body := readBody(t, url)
		c := &Connz{}
		if err := json.Unmarshal(body, &c); err != nil {
			t.Fatalf("Got an error unmarshalling the body: %v\n", err)
		}
		return c
	}
	c, err := s.Connz(opts)
	if err != nil {
		t.Fatalf("Error on Connz(): %v", err)
	}
	return c
}

func TestMonitorConnz(t *testing.T) {
	s := runMonitorServerWithAccounts()
	defer s.Shutdown()

	url := fmt.Sprintf("http://127.0.0.1:%d/", s.MonitorAddr().Port)

	testConnz := func(mode int) {
		c := pollConnz(t, s, mode, url+"connz", nil)

		// Test contents..
		require_Equal(t, c.NumConns, 0)
		require_Equal(t, c.Total, 0)
		require_Equal(t, len(c.Conns), 0)

		// Test with connections.
		nc := createClientConnWithUserSubscribeAndPublish(t, s, "a", "a")
		defer nc.Close()

		time.Sleep(50 * time.Millisecond)

		c = pollConnz(t, s, mode, url+"connz?auth=1", &ConnzOptions{Username: true})

		require_Equal(t, c.NumConns, 1)
		require_Equal(t, c.Total, 1)
		require_Equal(t, len(c.Conns), 1)
		require_Equal(t, c.Limit, DefaultConnListSize)
		require_Equal(t, c.Offset, 0)

		// Test inside details of each connection
		ci := c.Conns[0]

		require_NotEqual(t, ci.Cid, 0)
		require_Equal(t, ci.IP, "127.0.0.1")
		require_NotEqual(t, ci.Port, 0)
		require_Equal(t, ci.NumSubs, 0)
		require_Equal(t, len(ci.Subs), 0)
		require_Equal(t, len(ci.SubsDetail), 0)
		require_Equal(t, ci.InMsgs, 1)
		require_Equal(t, ci.OutMsgs, 1)
		require_Equal(t, ci.InBytes, 5)
		require_Equal(t, ci.OutBytes, 5)
		require_False(t, ci.Start.IsZero())
		require_NotEqual(t, ci.Uptime, "")
		require_False(t, ci.LastActivity.IsZero())
		require_False(t, ci.LastActivity.UnixNano() < ci.Start.UnixNano())
		require_NotEqual(t, ci.Idle, "")
		// This is a change, we now expect them to be set for connections when the
		// client sends a connect.
		require_NotEqual(t, ci.RTT, "")

		require_Equal(t, ci.Account, "A")
		require_Equal(t, ci.NameTag, "A")
	}

	for mode := 0; mode < 2; mode++ {
		testConnz(mode)
		checkClientsCount(t, s, 0)
	}
}

func TestMonitorConnzBadParams(t *testing.T) {
	s := runMonitorServer()
	defer s.Shutdown()

	url := fmt.Sprintf("http://127.0.0.1:%d/connz?", s.MonitorAddr().Port)
	readBodyEx(t, url+"auth=xxx", http.StatusBadRequest, textPlain)
	readBodyEx(t, url+"subs=xxx", http.StatusBadRequest, textPlain)
	readBodyEx(t, url+"offset=xxx", http.StatusBadRequest, textPlain)
	readBodyEx(t, url+"limit=xxx", http.StatusBadRequest, textPlain)
	readBodyEx(t, url+"state=xxx", http.StatusBadRequest, textPlain)
}

func TestMonitorConnzWithSubs(t *testing.T) {
	s := runMonitorServer()
	defer s.Shutdown()

	nc := createClientConnSubscribeAndPublish(t, s)
	defer nc.Close()

	nc.Subscribe("hello.foo", func(m *nats.Msg) {})
	ensureServerActivityRecorded(t, nc)

	url := fmt.Sprintf("http://127.0.0.1:%d/", s.MonitorAddr().Port)
	for mode := 0; mode < 2; mode++ {
		c := pollConnz(t, s, mode, url+"connz?subs=1", &ConnzOptions{Subscriptions: true})
		// Test inside details of each connection
		ci := c.Conns[0]
		if len(ci.Subs) != 1 || ci.Subs[0] != "hello.foo" {
			t.Fatalf("Expected subs of 1, got %v\n", ci.Subs)
		}
	}
}

func TestMonitorConnzWithSubsDetail(t *testing.T) {
	s := runMonitorServer()
	defer s.Shutdown()

	nc := createClientConnSubscribeAndPublish(t, s)
	defer nc.Close()

	nc.Subscribe("hello.foo", func(m *nats.Msg) {})
	ensureServerActivityRecorded(t, nc)

	url := fmt.Sprintf("http://127.0.0.1:%d/", s.MonitorAddr().Port)
	for mode := 0; mode < 2; mode++ {
		c := pollConnz(t, s, mode, url+"connz?subs=detail", &ConnzOptions{SubscriptionsDetail: true})
		// Test inside details of each connection
		ci := c.Conns[0]
		if len(ci.SubsDetail) != 1 || ci.SubsDetail[0].Subject != "hello.foo" {
			t.Fatalf("Expected subsdetail of 1, got %v\n", ci.Subs)
		}
	}
}

func TestMonitorClosedConnzWithSubsDetail(t *testing.T) {
	s := runMonitorServer()
	defer s.Shutdown()

	nc := createClientConnSubscribeAndPublish(t, s)

	nc.Subscribe("hello.foo", func(m *nats.Msg) {})
	ensureServerActivityRecorded(t, nc)
	nc.Close()

	s.mu.Lock()
	for len(s.clients) != 0 {
		s.mu.Unlock()
		<-time.After(100 * time.Millisecond)
		s.mu.Lock()
	}
	s.mu.Unlock()

	url := fmt.Sprintf("http://127.0.0.1:%d/", s.MonitorAddr().Port)
	for mode := 0; mode < 2; mode++ {
		c := pollConnz(t, s, mode, url+"connz?state=closed&subs=detail", &ConnzOptions{State: ConnClosed,
			SubscriptionsDetail: true})
		// Test inside details of each connection
		ci := c.Conns[0]
		if len(ci.SubsDetail) != 1 || ci.SubsDetail[0].Subject != "hello.foo" {
			t.Fatalf("Expected subsdetail of 1, got %v\n", ci.Subs)
		}
	}
}

func TestMonitorConnzWithCID(t *testing.T) {
	s := runMonitorServer()
	defer s.Shutdown()

	// The one we will request
	cid := 5
	total := 10

	// Create 10
	for i := 1; i <= total; i++ {
		nc := createClientConnSubscribeAndPublish(t, s)
		defer nc.Close()
		if i == cid {
			nc.Subscribe("hello.foo", func(m *nats.Msg) {})
			nc.Subscribe("hello.bar", func(m *nats.Msg) {})
			ensureServerActivityRecorded(t, nc)
		}
	}

	url := fmt.Sprintf("http://127.0.0.1:%d/connz?cid=%d", s.MonitorAddr().Port, cid)
	for mode := 0; mode < 2; mode++ {
		c := pollConnz(t, s, mode, url, &ConnzOptions{CID: uint64(cid)})
		// Test inside details of each connection
		if len(c.Conns) != 1 {
			t.Fatalf("Expected only one connection, but got %d\n", len(c.Conns))
		}
		if c.NumConns != 1 {
			t.Fatalf("Expected NumConns to be 1, but got %d\n", c.NumConns)
		}
		ci := c.Conns[0]
		if ci.Cid != uint64(cid) {
			t.Fatalf("Expected to receive connection %v, but received %v\n", cid, ci.Cid)
		}
		if ci.NumSubs != 2 {
			t.Fatalf("Expected to receive connection with %d subs, but received %d\n", 2, ci.NumSubs)
		}
		// Now test a miss
		badUrl := fmt.Sprintf("http://127.0.0.1:%d/connz?cid=%d", s.MonitorAddr().Port, 100)
		c = pollConnz(t, s, mode, badUrl, &ConnzOptions{CID: uint64(100)})
		if len(c.Conns) != 0 {
			t.Fatalf("Expected no connections, got %d\n", len(c.Conns))
		}
		if c.NumConns != 0 {
			t.Fatalf("Expected NumConns of 0, got %d\n", c.NumConns)
		}
	}
}

func TestMonitorConnzWithCIDAndAccountFilter(t *testing.T) {
	conf := createConfFile(t, []byte(`
		listen: "127.0.0.1:-1"
		http: "127.0.0.1:-1"
		system_account: SYS
		accounts {
			A { users = [{ user: a, password: a }] }
			B { users = [{ user: b, password: b }] }
			SYS { users = [{ user: sys, password: sys }] }
		}
	`))
	s, _ := RunServerWithConfig(conf)
	defer s.Shutdown()

	ncA, err := nats.Connect(s.ClientURL(), nats.UserInfo("a", "a"))
	require_NoError(t, err)
	defer ncA.Close()
	ncB, err := nats.Connect(s.ClientURL(), nats.UserInfo("b", "b"))
	require_NoError(t, err)
	defer ncB.Close()

	cidA, err := ncA.GetClientID()
	require_NoError(t, err)
	cidB, err := ncB.GetClientID()
	require_NoError(t, err)

	checkConns := func(t *testing.T, c *Connz, expected int, cid uint64) {
		t.Helper()
		require_Len(t, len(c.Conns), expected)
		require_Equal(t, c.NumConns, expected)
		if expected > 0 {
			require_Equal(t, c.Conns[0].Cid, cid)
		}
	}

	for _, state := range []ConnState{ConnOpen, ConnAll} {
		// Selecting a CID that belongs to the filtered account must work.
		c, err := s.Connz(&ConnzOptions{Account: "A", CID: cidA, State: state})
		require_NoError(t, err)
		checkConns(t, c, 1, cidA)

		// Selecting a CID from another account must not leak that connection.
		c, err = s.Connz(&ConnzOptions{Account: "A", CID: cidB, State: state})
		require_NoError(t, err)
		checkConns(t, c, 0, 0)

		// The user filter must also apply when selecting by CID.
		c, err = s.Connz(&ConnzOptions{User: "b", CID: cidA, State: state})
		require_NoError(t, err)
		checkConns(t, c, 0, 0)
		c, err = s.Connz(&ConnzOptions{User: "a", CID: cidA, State: state})
		require_NoError(t, err)
		checkConns(t, c, 1, cidA)

		// Same for the MQTT client ID filter.
		c, err = s.Connz(&ConnzOptions{MQTTClient: "nope", CID: cidA, State: state})
		require_NoError(t, err)
		checkConns(t, c, 0, 0)
	}

	// An account user can request its own account's CONNZ through the system
	// import. It must not be able to see other accounts' connections by CID.
	sysReq := func(t *testing.T, nc *nats.Conn, cid uint64) *Connz {
		t.Helper()
		req, err := json.Marshal(&ConnzOptions{CID: cid})
		require_NoError(t, err)
		resp, err := nc.Request("$SYS.REQ.ACCOUNT.PING.CONNZ", req, time.Second)
		require_NoError(t, err)
		var r ServerAPIConnzResponse
		require_NoError(t, json.Unmarshal(resp.Data, &r))
		require_True(t, r.Error == nil)
		require_True(t, r.Data != nil)
		return r.Data
	}
	checkConns(t, sysReq(t, ncA, cidA), 1, cidA)
	checkConns(t, sysReq(t, ncA, cidB), 0, 0)
	checkConns(t, sysReq(t, ncB, cidB), 1, cidB)
	checkConns(t, sysReq(t, ncB, cidA), 0, 0)
}

// Helper to map to connection name
func createConnMap(cz *Connz) map[string]*ConnInfo {
	cm := make(map[string]*ConnInfo)
	for _, c := range cz.Conns {
		cm[c.Name] = c
	}
	return cm
}

func getFooAndBar(cm map[string]*ConnInfo) (*ConnInfo, *ConnInfo) {
	return cm["foo"], cm["bar"]
}

func ensureServerActivityRecorded(t *testing.T, nc *nats.Conn) {
	nc.Flush()
	err := nc.Flush()
	if err != nil {
		t.Fatalf("Error flushing: %v\n", err)
	}
}

func TestMonitorConnzRTT(t *testing.T) {
	s := runMonitorServer()
	defer s.Shutdown()

	url := fmt.Sprintf("http://127.0.0.1:%d/", s.MonitorAddr().Port)

	testRTT := func(mode int) {
		// Test with connections.
		nc := createClientConnSubscribeAndPublish(t, s)
		defer nc.Close()

		c := pollConnz(t, s, mode, url+"connz", nil)

		if c.NumConns != 1 {
			t.Fatalf("Expected 1 connection, got %d\n", c.NumConns)
		}

		// Send a server side PING to record RTT
		s.mu.Lock()
		ci := c.Conns[0]
		sc := s.clients[ci.Cid]
		if sc == nil {
			t.Fatalf("Error looking up client %v\n", ci.Cid)
		}
		s.mu.Unlock()
		sc.mu.Lock()
		sc.sendPing()
		sc.mu.Unlock()

		// Wait for client to respond with PONG
		time.Sleep(20 * time.Millisecond)

		// Repoll for updated information.
		c = pollConnz(t, s, mode, url+"connz", nil)
		ci = c.Conns[0]

		rtt, err := time.ParseDuration(ci.RTT)
		if err != nil {
			t.Fatalf("Could not parse RTT properly, %v (ci.RTT=%v)", err, ci.RTT)
		}
		if rtt <= 0 {
			t.Fatal("Expected RTT to be valid and non-zero\n")
		}
		if (runtime.GOOS == "windows" && rtt > 20*time.Millisecond) ||
			rtt > 20*time.Millisecond || rtt < 100*time.Nanosecond {
			t.Fatalf("Invalid RTT of %s\n", ci.RTT)
		}
	}

	for mode := 0; mode < 2; mode++ {
		testRTT(mode)
		checkClientsCount(t, s, 0)
	}
}

func TestMonitorConnzLastActivity(t *testing.T) {
	s := runMonitorServer()
	defer s.Shutdown()

	url := fmt.Sprintf("http://127.0.0.1:%d/", s.MonitorAddr().Port)
	url += "connz?subs=1"
	opts := &ConnzOptions{Subscriptions: true}

	var sleepTime time.Duration
	if runtime.GOOS == "windows" {
		sleepTime = 10 * time.Millisecond
	}

	testActivity := func(mode int) {
		ncFoo := createClientConnWithName(t, "foo", s)
		defer ncFoo.Close()

		ncBar := createClientConnWithName(t, "bar", s)
		defer ncBar.Close()

		// Test inside details of each connection
		ciFoo, ciBar := getFooAndBar(createConnMap(pollConnz(t, s, mode, url, opts)))

		// Test that LastActivity is non-zero
		if ciFoo.LastActivity.IsZero() {
			t.Fatalf("Expected LastActivity for connection '%s'to be valid\n", ciFoo.Name)
		}
		if ciBar.LastActivity.IsZero() {
			t.Fatalf("Expected LastActivity for connection '%s'to be valid\n", ciBar.Name)
		}
		// Foo should be older than Bar
		if ciFoo.LastActivity.After(ciBar.LastActivity) {
			t.Fatal("Expected connection 'foo' to be older than 'bar'\n")
		}

		fooLA := ciFoo.LastActivity
		barLA := ciBar.LastActivity

		ensureServerActivityRecorded(t, ncFoo)
		ensureServerActivityRecorded(t, ncBar)

		time.Sleep(sleepTime)

		// Sub should trigger update.
		sub, _ := ncFoo.Subscribe("hello.world", func(m *nats.Msg) {})
		ensureServerActivityRecorded(t, ncFoo)

		ciFoo, _ = getFooAndBar(createConnMap(pollConnz(t, s, mode, url, opts)))
		nextLA := ciFoo.LastActivity
		if fooLA.Equal(nextLA) {
			t.Fatalf("Subscribe should have triggered update to LastActivity %+v\n", ciFoo)
		}
		fooLA = nextLA

		time.Sleep(sleepTime)

		// Publish and Message Delivery should trigger as well. So both connections
		// should have updates.
		ncBar.Publish("hello.world", []byte("Hello"))

		ensureServerActivityRecorded(t, ncFoo)
		ensureServerActivityRecorded(t, ncBar)

		ciFoo, ciBar = getFooAndBar(createConnMap(pollConnz(t, s, mode, url, opts)))
		nextLA = ciBar.LastActivity
		if barLA.Equal(nextLA) {
			t.Fatalf("Publish should have triggered update to LastActivity\n")
		}

		// Message delivery on ncFoo should have triggered as well.
		nextLA = ciFoo.LastActivity
		if fooLA.Equal(nextLA) {
			t.Fatalf("Message delivery should have triggered update to LastActivity\n")
		}
		fooLA = nextLA

		time.Sleep(sleepTime)

		// Unsub should trigger as well
		sub.Unsubscribe()
		ensureServerActivityRecorded(t, ncFoo)

		ciFoo, _ = getFooAndBar(createConnMap(pollConnz(t, s, mode, url, opts)))
		nextLA = ciFoo.LastActivity
		if fooLA.Equal(nextLA) {
			t.Fatalf("Message delivery should have triggered update to LastActivity\n")
		}
	}

	for mode := 0; mode < 2; mode++ {
		testActivity(mode)
	}
}

func TestMonitorConnzWithOffsetAndLimit(t *testing.T) {
	s := runMonitorServer()
	defer s.Shutdown()

	url := fmt.Sprintf("http://127.0.0.1:%d/", s.MonitorAddr().Port)

	for mode := 0; mode < 2; mode++ {
		c := pollConnz(t, s, mode, url+"connz?offset=1&limit=1", &ConnzOptions{Offset: 1, Limit: 1})
		if c.Conns == nil || len(c.Conns) != 0 {
			t.Fatalf("Expected 0 connections in array, got %p\n", c.Conns)
		}

		// Test that when given negative values, 0 or default is used
		c = pollConnz(t, s, mode, url+"connz?offset=-1&limit=-1", &ConnzOptions{Offset: -11, Limit: -11})
		if c.Conns == nil || len(c.Conns) != 0 {
			t.Fatalf("Expected 0 connections in array, got %p\n", c.Conns)
		}
		if c.Offset != 0 {
			t.Fatalf("Expected offset to be 0, and limit to be %v, got %v and %v",
				DefaultConnListSize, c.Offset, c.Limit)
		}
	}

	cl1 := createClientConnSubscribeAndPublish(t, s)
	defer cl1.Close()

	cl2 := createClientConnSubscribeAndPublish(t, s)
	defer cl2.Close()

	for mode := 0; mode < 2; mode++ {
		c := pollConnz(t, s, mode, url+"connz?offset=1&limit=1", &ConnzOptions{Offset: 1, Limit: 1})
		if c.Limit != 1 {
			t.Fatalf("Expected limit of 1, got %v\n", c.Limit)
		}

		if c.Offset != 1 {
			t.Fatalf("Expected offset of 1, got %v\n", c.Offset)
		}

		if len(c.Conns) != 1 {
			t.Fatalf("Expected conns of 1, got %v\n", len(c.Conns))
		}

		if c.NumConns != 1 {
			t.Fatalf("Expected NumConns to be 1, got %v\n", c.NumConns)
		}

		if c.Total != 2 {
			t.Fatalf("Expected Total to be at least 2, got %v", c.Total)
		}

		c = pollConnz(t, s, mode, url+"connz?offset=2&limit=1", &ConnzOptions{Offset: 2, Limit: 1})
		if c.Limit != 1 {
			t.Fatalf("Expected limit of 1, got %v\n", c.Limit)
		}

		if c.Offset != 2 {
			t.Fatalf("Expected offset of 2, got %v\n", c.Offset)
		}

		if len(c.Conns) != 0 {
			t.Fatalf("Expected conns of 0, got %v\n", len(c.Conns))
		}

		if c.NumConns != 0 {
			t.Fatalf("Expected NumConns to be 0, got %v\n", c.NumConns)
		}

		if c.Total != 2 {
			t.Fatalf("Expected Total to be 2, got %v", c.Total)
		}
	}
}

func TestMonitorConnzOffsetOverflow(t *testing.T) {
	s := runMonitorServer()
	defer s.Shutdown()

	// Need at least one open connection so we get past the empty-result early
	// return and reach the pagination slicing.
	cl := createClientConnSubscribeAndPublish(t, s)
	defer cl.Close()

	url := fmt.Sprintf("http://127.0.0.1:%d/", s.MonitorAddr().Port)

	for mode := 0; mode < 2; mode++ {
		// Offset = math.MaxInt64, Limit = 1 makes Offset+Limit overflow to math.MinInt64.
		c := pollConnz(t, s, mode, url+fmt.Sprintf("connz?offset=%d&limit=1", math.MaxInt64),
			&ConnzOptions{Offset: math.MaxInt64, Limit: 1})
		if c.Conns == nil || len(c.Conns) != 0 {
			t.Fatalf("Expected 0 connections in array, got %v\n", len(c.Conns))
		}
		if c.NumConns != 0 {
			t.Fatalf("Expected NumConns to be 0, got %v", c.NumConns)
		}
	}

	// Both Offset and Limit at the max also overflows the sum.
	_, err := s.Connz(&ConnzOptions{Offset: math.MaxInt64, Limit: math.MaxInt64})
	require_NoError(t, err)
}

func TestMonitorConnzDefaultSorted(t *testing.T) {
	s := runMonitorServer()
	defer s.Shutdown()

	clients := make([]*nats.Conn, 4)
	for i := range clients {
		clients[i] = createClientConnSubscribeAndPublish(t, s)
		defer clients[i].Close()
	}

	url := fmt.Sprintf("http://127.0.0.1:%d/", s.MonitorAddr().Port)
	for mode := 0; mode < 2; mode++ {
		c := pollConnz(t, s, mode, url+"connz", nil)
		if c.Conns[0].Cid > c.Conns[1].Cid ||
			c.Conns[1].Cid > c.Conns[2].Cid ||
			c.Conns[2].Cid > c.Conns[3].Cid {
			t.Fatalf("Expected conns sorted in ascending order by cid, got %v < %v\n", c.Conns[0].Cid, c.Conns[3].Cid)
		}
	}
}

func TestMonitorConnzSortedByCid(t *testing.T) {
	s := runMonitorServer()
	defer s.Shutdown()

	clients := make([]*nats.Conn, 4)
	for i := range clients {
		clients[i] = createClientConnSubscribeAndPublish(t, s)
		defer clients[i].Close()
	}

	url := fmt.Sprintf("http://127.0.0.1:%d/", s.MonitorAddr().Port)
	for mode := 0; mode < 2; mode++ {
		c := pollConnz(t, s, mode, url+"connz?sort=cid", &ConnzOptions{Sort: ByCid})
		if c.Conns[0].Cid > c.Conns[1].Cid ||
			c.Conns[1].Cid > c.Conns[2].Cid ||
			c.Conns[2].Cid > c.Conns[3].Cid {
			t.Fatalf("Expected conns sorted in ascending order by cid, got [%v, %v, %v, %v]\n",
				c.Conns[0].Cid, c.Conns[1].Cid, c.Conns[2].Cid, c.Conns[3].Cid)
		}
	}
}

func TestMonitorConnzSortedByStart(t *testing.T) {
	s := runMonitorServer()
	defer s.Shutdown()

	clients := make([]*nats.Conn, 4)
	for i := range clients {
		clients[i] = createClientConnSubscribeAndPublish(t, s)
		defer clients[i].Close()
	}

	url := fmt.Sprintf("http://127.0.0.1:%d/", s.MonitorAddr().Port)
	for mode := 0; mode < 2; mode++ {
		c := pollConnz(t, s, mode, url+"connz?sort=start", &ConnzOptions{Sort: ByStart})
		if c.Conns[0].Start.After(c.Conns[1].Start) ||
			c.Conns[1].Start.After(c.Conns[2].Start) ||
			c.Conns[2].Start.After(c.Conns[3].Start) {
			t.Fatalf("Expected conns sorted in ascending order by startime, got [%v, %v, %v, %v]\n",
				c.Conns[0].Start, c.Conns[1].Start, c.Conns[2].Start, c.Conns[3].Start)
		}
	}
}

func TestMonitorConnzSortedByBytesAndMsgs(t *testing.T) {
	s := runMonitorServer()
	defer s.Shutdown()

	// Create a connection and make it send more messages than others
	firstClient := createClientConnSubscribeAndPublish(t, s)
	for i := 0; i < 100; i++ {
		firstClient.Publish("foo", []byte("Hello World"))
	}
	defer firstClient.Close()
	firstClient.Flush()

	clients := make([]*nats.Conn, 3)
	for i := range clients {
		clients[i] = createClientConnSubscribeAndPublish(t, s)
		defer clients[i].Close()
	}

	url := fmt.Sprintf("http://127.0.0.1:%d/", s.MonitorAddr().Port)
	for mode := 0; mode < 2; mode++ {
		c := pollConnz(t, s, mode, url+"connz?sort=bytes_to", &ConnzOptions{Sort: ByOutBytes})
		if c.Conns[0].OutBytes < c.Conns[1].OutBytes ||
			c.Conns[0].OutBytes < c.Conns[2].OutBytes ||
			c.Conns[0].OutBytes < c.Conns[3].OutBytes {
			t.Fatalf("Expected conns sorted in descending order by bytes to, got %v < one of [%v, %v, %v]\n",
				c.Conns[0].OutBytes, c.Conns[1].OutBytes, c.Conns[2].OutBytes, c.Conns[3].OutBytes)
		}

		c = pollConnz(t, s, mode, url+"connz?sort=msgs_to", &ConnzOptions{Sort: ByOutMsgs})
		if c.Conns[0].OutMsgs < c.Conns[1].OutMsgs ||
			c.Conns[0].OutMsgs < c.Conns[2].OutMsgs ||
			c.Conns[0].OutMsgs < c.Conns[3].OutMsgs {
			t.Fatalf("Expected conns sorted in descending order by msgs from, got %v < one of [%v, %v, %v]\n",
				c.Conns[0].OutMsgs, c.Conns[1].OutMsgs, c.Conns[2].OutMsgs, c.Conns[3].OutMsgs)
		}

		c = pollConnz(t, s, mode, url+"connz?sort=bytes_from", &ConnzOptions{Sort: ByInBytes})
		if c.Conns[0].InBytes < c.Conns[1].InBytes ||
			c.Conns[0].InBytes < c.Conns[2].InBytes ||
			c.Conns[0].InBytes < c.Conns[3].InBytes {
			t.Fatalf("Expected conns sorted in descending order by bytes from, got %v < one of [%v, %v, %v]\n",
				c.Conns[0].InBytes, c.Conns[1].InBytes, c.Conns[2].InBytes, c.Conns[3].InBytes)
		}

		c = pollConnz(t, s, mode, url+"connz?sort=msgs_from", &ConnzOptions{Sort: ByInMsgs})
		if c.Conns[0].InMsgs < c.Conns[1].InMsgs ||
			c.Conns[0].InMsgs < c.Conns[2].InMsgs ||
			c.Conns[0].InMsgs < c.Conns[3].InMsgs {
			t.Fatalf("Expected conns sorted in descending order by msgs from, got %v < one of [%v, %v, %v]\n",
				c.Conns[0].InMsgs, c.Conns[1].InMsgs, c.Conns[2].InMsgs, c.Conns[3].InMsgs)
		}
	}
}

func TestMonitorConnzSortedByPending(t *testing.T) {
	s := runMonitorServer()
	defer s.Shutdown()

	firstClient := createClientConnSubscribeAndPublish(t, s)
	firstClient.Subscribe("hello.world", func(m *nats.Msg) {})
	clients := make([]*nats.Conn, 3)
	for i := range clients {
		clients[i] = createClientConnSubscribeAndPublish(t, s)
		defer clients[i].Close()
	}
	defer firstClient.Close()

	url := fmt.Sprintf("http://127.0.0.1:%d/", s.MonitorAddr().Port)
	for mode := 0; mode < 2; mode++ {
		c := pollConnz(t, s, mode, url+"connz?sort=pending", &ConnzOptions{Sort: ByPending})
		if c.Conns[0].Pending < c.Conns[1].Pending ||
			c.Conns[0].Pending < c.Conns[2].Pending ||
			c.Conns[0].Pending < c.Conns[3].Pending {
			t.Fatalf("Expected conns sorted in descending order by number of pending, got %v < one of [%v, %v, %v]\n",
				c.Conns[0].Pending, c.Conns[1].Pending, c.Conns[2].Pending, c.Conns[3].Pending)
		}
	}
}

func TestMonitorConnzSortedBySubs(t *testing.T) {
	s := runMonitorServer()
	defer s.Shutdown()

	firstClient := createClientConnSubscribeAndPublish(t, s)
	firstClient.Subscribe("hello.world", func(m *nats.Msg) {})
	defer firstClient.Close()

	clients := make([]*nats.Conn, 3)
	for i := range clients {
		clients[i] = createClientConnSubscribeAndPublish(t, s)
		defer clients[i].Close()
	}

	url := fmt.Sprintf("http://127.0.0.1:%d/", s.MonitorAddr().Port)
	for mode := 0; mode < 2; mode++ {
		c := pollConnz(t, s, mode, url+"connz?sort=subs", &ConnzOptions{Sort: BySubs})
		if c.Conns[0].NumSubs < c.Conns[1].NumSubs ||
			c.Conns[0].NumSubs < c.Conns[2].NumSubs ||
			c.Conns[0].NumSubs < c.Conns[3].NumSubs {
			t.Fatalf("Expected conns sorted in descending order by number of subs, got %v < one of [%v, %v, %v]\n",
				c.Conns[0].NumSubs, c.Conns[1].NumSubs, c.Conns[2].NumSubs, c.Conns[3].NumSubs)
		}
	}
}

func TestMonitorConnzSortedByLast(t *testing.T) {
	resetPreviousHTTPConnections()
	opts := DefaultMonitorOptions()
	opts.NoSystemAccount = true
	s := RunServer(opts)
	defer s.Shutdown()

	firstClient := createClientConnSubscribeAndPublish(t, s)
	defer firstClient.Close()
	firstClient.Subscribe("hello.world", func(m *nats.Msg) {})
	firstClient.Flush()

	clients := make([]*nats.Conn, 3)
	for i := range clients {
		clients[i] = createClientConnSubscribeAndPublish(t, s)
		defer clients[i].Close()
		clients[i].Flush()
	}

	url := fmt.Sprintf("http://127.0.0.1:%d/", s.MonitorAddr().Port)
	for mode := 0; mode < 2; mode++ {
		c := pollConnz(t, s, mode, url+"connz?sort=last", &ConnzOptions{Sort: ByLast})
		if c.Conns[0].LastActivity.UnixNano() < c.Conns[1].LastActivity.UnixNano() ||
			c.Conns[1].LastActivity.UnixNano() < c.Conns[2].LastActivity.UnixNano() ||
			c.Conns[2].LastActivity.UnixNano() < c.Conns[3].LastActivity.UnixNano() {
			t.Fatalf("Expected conns sorted in descending order by lastActivity, got %v < one of [%v, %v, %v]\n",
				c.Conns[0].LastActivity, c.Conns[1].LastActivity, c.Conns[2].LastActivity, c.Conns[3].LastActivity)
		}
	}
}

func TestMonitorConnzSortedByUptime(t *testing.T) {
	s := runMonitorServer()
	defer s.Shutdown()

	for i := 0; i < 4; i++ {
		client := createClientConnSubscribeAndPublish(t, s)
		defer client.Close()
		// Since we check times (now-start) does not have to be big.
		time.Sleep(50 * time.Millisecond)
	}

	url := fmt.Sprintf("http://127.0.0.1:%d/", s.MonitorAddr().Port)
	for mode := 0; mode < 2; mode++ {
		c := pollConnz(t, s, mode, url+"connz?sort=uptime", &ConnzOptions{Sort: ByUptime})
		now := time.Now()
		ups := make([]int, 4)
		for i := 0; i < 4; i++ {
			ups[i] = int(now.Sub(c.Conns[i].Start))
		}
		if !sort.IntsAreSorted(ups) {
			d := make([]time.Duration, 4)
			for i := 0; i < 4; i++ {
				d[i] = time.Duration(ups[i])
			}
			t.Fatalf("Expected conns sorted in ascending order by uptime (now-Start), got %+v\n", d)
		}
	}
}

func TestMonitorConnzSortedByUptimeClosedConn(t *testing.T) {
	s := runMonitorServer()
	defer s.Shutdown()

	for i := time.Duration(1); i <= 4; i++ {
		c := createClientConnSubscribeAndPublish(t, s)

		// Grab client and asjust start time such that
		client := s.getClient(uint64(i))
		if client == nil {
			t.Fatalf("Could nopt retrieve client for %d\n", i)
		}
		client.mu.Lock()
		client.start = client.start.Add(-10 * (4 - i) * time.Second)
		client.mu.Unlock()

		c.Close()
	}

	checkClosedConns(t, s, 4, time.Second)

	url := fmt.Sprintf("http://127.0.0.1:%d/", s.MonitorAddr().Port)
	for mode := 0; mode < 2; mode++ {
		c := pollConnz(t, s, mode, url+"connz?state=closed&sort=uptime", &ConnzOptions{State: ConnClosed, Sort: ByUptime})
		ups := make([]int, 4)
		for i := 0; i < 4; i++ {
			ups[i] = int(c.Conns[i].Stop.Sub(c.Conns[i].Start))
		}
		if !sort.IntsAreSorted(ups) {
			d := make([]time.Duration, 4)
			for i := 0; i < 4; i++ {
				d[i] = time.Duration(ups[i])
			}
			t.Fatalf("Expected conns sorted in ascending order by uptime, got %+v\n", d)
		}
	}
}

func TestMonitorConnzSortedByStopOnOpen(t *testing.T) {
	s := runMonitorServer()
	defer s.Shutdown()

	opts := s.getOpts()
	url := fmt.Sprintf("nats://%s:%d", opts.Host, opts.Port)

	// 4 clients
	for i := 0; i < 4; i++ {
		c, err := nats.Connect(url)
		if err != nil {
			t.Fatalf("Could not create client: %v\n", err)
		}
		defer c.Close()
	}

	c, err := s.Connz(&ConnzOptions{Sort: ByStop})
	if err == nil {
		t.Fatalf("Expected err to be non-nil, got %+v\n", c)
	}
}

func TestMonitorConnzSortedByStopTimeClosedConn(t *testing.T) {
	s := runMonitorServer()
	defer s.Shutdown()

	opts := s.getOpts()
	url := fmt.Sprintf("nats://%s:%d", opts.Host, opts.Port)

	// 4 clients
	for i := 0; i < 4; i++ {
		c, err := nats.Connect(url)
		if err != nil {
			t.Fatalf("Could not create client: %v\n", err)
		}
		c.Close()
	}
	checkClosedConns(t, s, 4, time.Second)

	// Now adjust the Stop times for these with some random values.
	now := time.Now().UTC()
	ccs := s.closed.closedClients()
	for _, cc := range ccs {
		newStop := now.Add(time.Duration(rand.Int()%120) * -time.Minute)
		cc.Stop = &newStop
	}

	url = fmt.Sprintf("http://127.0.0.1:%d/", s.MonitorAddr().Port)
	for mode := 0; mode < 2; mode++ {
		c := pollConnz(t, s, mode, url+"connz?state=closed&sort=stop", &ConnzOptions{State: ConnClosed, Sort: ByStop})
		ups := make([]int, 4)
		nowU := time.Now().UnixNano()
		for i := 0; i < 4; i++ {
			ups[i] = int(nowU - c.Conns[i].Stop.UnixNano())
		}
		if !sort.IntsAreSorted(ups) {
			d := make([]time.Duration, 4)
			for i := 0; i < 4; i++ {
				d[i] = time.Duration(ups[i])
			}
			t.Fatalf("Expected conns sorted in ascending order by stop time, got %+v\n", d)
		}
	}
}

func TestMonitorConnzSortedByReason(t *testing.T) {
	s := runMonitorServer()
	defer s.Shutdown()

	opts := s.getOpts()
	url := fmt.Sprintf("nats://%s:%d", opts.Host, opts.Port)

	// 20 clients
	for i := 0; i < 20; i++ {
		c, err := nats.Connect(url)
		if err != nil {
			t.Fatalf("Could not create client: %v\n", err)
		}
		c.Close()
	}
	checkClosedConns(t, s, 20, time.Second)

	// Now adjust the Reasons for these with some random values.
	ccs := s.closed.closedClients()
	max := int(ServerShutdown)
	for _, cc := range ccs {
		cc.Reason = ClosedState(rand.Int() % max).String()
	}

	url = fmt.Sprintf("http://127.0.0.1:%d/", s.MonitorAddr().Port)
	for mode := 0; mode < 2; mode++ {
		c := pollConnz(t, s, mode, url+"connz?state=closed&sort=reason", &ConnzOptions{State: ConnClosed, Sort: ByReason})
		rs := make([]string, 20)
		for i := 0; i < 20; i++ {
			rs[i] = c.Conns[i].Reason
		}
		if !sort.StringsAreSorted(rs) {
			t.Fatalf("Expected conns sorted in order by stop reason, got %#v\n", rs)
		}
	}
}

func TestMonitorConnzSortedByReasonOnOpen(t *testing.T) {
	s := runMonitorServer()
	defer s.Shutdown()

	opts := s.getOpts()
	url := fmt.Sprintf("nats://%s:%d", opts.Host, opts.Port)

	// 4 clients
	for i := 0; i < 4; i++ {
		c, err := nats.Connect(url)
		if err != nil {
			t.Fatalf("Could not create client: %v\n", err)
		}
		defer c.Close()
	}

	c, err := s.Connz(&ConnzOptions{Sort: ByReason})
	if err == nil {
		t.Fatalf("Expected err to be non-nil, got %+v\n", c)
	}
}

func TestMonitorConnzSortedByIdle(t *testing.T) {
	s := runMonitorServer()
	defer s.Shutdown()

	url := fmt.Sprintf("http://%s/connz?sort=idle", s.MonitorAddr())
	now := time.Now()

	clients := []struct {
		start time.Time // Client start time.
		last  time.Time // Client last activity time.
	}{
		{start: now.Add(-10 * time.Second), last: now.Add(-5 * time.Second)},
		{start: now.Add(-20 * time.Second), last: now.Add(-10 * time.Second)},
		{start: now.Add(-3 * time.Second), last: now.Add(-2 * time.Second)},
		{start: now.Add(-30 * time.Second), last: now.Add(-20 * time.Second)},
	}

	testIdle := func(mode int) {
		// Connect the specified number of clients.
		for _, c := range clients {
			clientConn := createClientConnSubscribeAndPublish(t, s)
			defer clientConn.Close()

			cid, err := clientConn.GetClientID()
			if err != nil {
				t.Fatalf("error getting the client CID: %v", err)
			}

			client := s.getClient(cid)
			if client == nil {
				t.Fatalf("error looking up client %d", cid)
			}

			// Change the client's start and last activity times.
			client.mu.Lock()
			client.start = c.start
			client.last = c.last
			client.mu.Unlock()
		}

		connz := pollConnz(t, s, mode, url, &ConnzOptions{Sort: ByIdle})

		wantConns := len(clients)
		gotConns := len(connz.Conns)

		if gotConns != wantConns {
			t.Fatalf("want %d connections, got %d", wantConns, gotConns)
		}

		idleDurations := getConnsIdleDurations(t, connz.Conns)

		if !sortedDurationsDesc(idleDurations) {
			t.Errorf("want durations sorted in descending order, got %v", idleDurations)
		}
	}

	for mode := 0; mode < 2; mode++ {
		testIdle(mode)
	}
}

// getConnsIdleDurations returns a slice of parsed idle durations from a connection info slice.
func getConnsIdleDurations(t *testing.T, conns []*ConnInfo) []time.Duration {
	t.Helper()

	durations := make([]time.Duration, 0, len(conns))

	for _, conn := range conns {
		idle, err := time.ParseDuration(conn.Idle)
		if err != nil {
			t.Fatalf("error parsing duration %q: %v", conn.Idle, err)
		}
		durations = append(durations, idle)
	}

	return durations
}

// sortedDurationsDesc checks if a time.Duration slice is sorted in descending order.
func sortedDurationsDesc(durations []time.Duration) bool {
	return sort.SliceIsSorted(durations, func(i, j int) bool {
		// Must be longer than the next duration.
		return durations[i] > durations[j]
	})
}

func TestMonitorConnzSortByIdleTime(t *testing.T) {
	now := time.Now().UTC()

	cases := map[string]ConnInfos{
		"zero values": {{}, {}, {}, {}},
		"equal last activity times": {
			{Start: now.Add(-50 * time.Minute), LastActivity: now.Add(-time.Minute)},
			{Start: now.Add(-30 * time.Minute), LastActivity: now.Add(-time.Minute)},
			{Start: now.Add(-10 * time.Second), LastActivity: now.Add(-time.Minute)},
			{Start: now.Add(-2 * time.Hour), LastActivity: now.Add(-time.Minute)},
		},
		"last activity in the future": {
			{Start: now.Add(-50 * time.Minute), LastActivity: now.Add(10 * time.Minute)}, // +10m
			{Start: now.Add(-30 * time.Minute), LastActivity: now.Add(5 * time.Minute)},  // +5m
			{Start: now.Add(-24 * time.Hour), LastActivity: now.Add(2 * time.Second)},    // +2s
			{Start: now.Add(-10 * time.Second), LastActivity: now.Add(15 * time.Minute)}, // +15m
			{Start: now.Add(-2 * time.Hour), LastActivity: now.Add(time.Minute)},         // +1m
		},
		"unsorted": {
			{Start: now.Add(-50 * time.Minute), LastActivity: now.Add(-10 * time.Minute)}, // 10m ago
			{Start: now.Add(-30 * time.Minute), LastActivity: now.Add(-5 * time.Minute)},  // 5m ago
			{Start: now.Add(-24 * time.Hour), LastActivity: now.Add(-2 * time.Second)},    // 2s ago
			{Start: now.Add(-10 * time.Second), LastActivity: now.Add(-15 * time.Minute)}, // 15m ago
			{Start: now.Add(-2 * time.Hour), LastActivity: now.Add(-time.Minute)},         // 1m ago
		},
		"unsorted with zero value start time": {
			{LastActivity: now.Add(-10 * time.Minute)}, // 10m ago
			{LastActivity: now.Add(-5 * time.Minute)},  // 5m ago
			{LastActivity: now.Add(-2 * time.Second)},  // 2s ago
			{LastActivity: now.Add(-15 * time.Minute)}, // 15m ago
			{LastActivity: now.Add(-time.Minute)},      // 1m ago
		},
		"sorted": {
			{Start: now.Add(-24 * time.Hour), LastActivity: now.Add(-2 * time.Second)},    // 2s ago
			{Start: now.Add(-2 * time.Hour), LastActivity: now.Add(-time.Minute)},         // 1m ago
			{Start: now.Add(-30 * time.Minute), LastActivity: now.Add(-5 * time.Minute)},  // 5m ago
			{Start: now.Add(-50 * time.Minute), LastActivity: now.Add(-10 * time.Minute)}, // 10m ago
			{Start: now.Add(-10 * time.Second), LastActivity: now.Add(-15 * time.Minute)}, // 15m ago
		},
		"sorted with zero value start time": {
			{LastActivity: now.Add(-2 * time.Second)},  // 2s ago
			{LastActivity: now.Add(-time.Minute)},      // 1m ago
			{LastActivity: now.Add(-5 * time.Minute)},  // 5m ago
			{LastActivity: now.Add(-10 * time.Minute)}, // 10m ago
			{LastActivity: now.Add(-15 * time.Minute)}, // 15m ago
		},
	}

	for name, conns := range cases {
		t.Run(name, func(t *testing.T) {
			sort.Sort(SortByIdle{conns, now})

			idleDurations := getIdleDurations(conns, now)

			if !sortedDurationsAsc(idleDurations) {
				t.Errorf("want durations sorted in ascending order, got %v", idleDurations)
			}
		})
	}
}

// getIdleDurations returns a slice of idle durations from a connection info list up until now time.
func getIdleDurations(conns ConnInfos, now time.Time) []time.Duration {
	durations := make([]time.Duration, 0, len(conns))

	for _, conn := range conns {
		durations = append(durations, now.Sub(conn.LastActivity))
	}

	return durations
}

// sortedDurationsAsc checks if a time.Duration slice is sorted in ascending order.
func sortedDurationsAsc(durations []time.Duration) bool {
	return sort.SliceIsSorted(durations, func(i, j int) bool {
		return durations[i] < durations[j]
	})
}

func TestMonitorConnzSortBadRequest(t *testing.T) {
	s := runMonitorServer()
	defer s.Shutdown()

	firstClient := createClientConnSubscribeAndPublish(t, s)
	firstClient.Subscribe("hello.world", func(m *nats.Msg) {})
	clients := make([]*nats.Conn, 3)
	for i := range clients {
		clients[i] = createClientConnSubscribeAndPublish(t, s)
		defer clients[i].Close()
	}
	defer firstClient.Close()

	url := fmt.Sprintf("http://127.0.0.1:%d/", s.MonitorAddr().Port)
	readBodyEx(t, url+"connz?sort=foo", http.StatusBadRequest, textPlain)

	if _, err := s.Connz(&ConnzOptions{Sort: "foo"}); err == nil {
		t.Fatal("Expected error, got none")
	}
}

func pollRoutez(t *testing.T, s *Server, mode int, url string, opts *RoutezOptions) *Routez {
	t.Helper()
	if mode == 0 {
		rz := &Routez{}
		body := readBody(t, url)
		if err := json.Unmarshal(body, rz); err != nil {
			t.Fatalf("Got an error unmarshalling the body: %v\n", err)
		}
		return rz
	}
	rz, err := s.Routez(opts)
	if err != nil {
		t.Fatalf("Error on Routez: %v", err)
	}
	return rz
}

func TestMonitorConnzWithRoutes(t *testing.T) {
	resetPreviousHTTPConnections()
	opts := DefaultMonitorOptions()
	opts.NoSystemAccount = true
	opts.Cluster.Name = "A"
	opts.Cluster.Host = "127.0.0.1"
	opts.Cluster.Port = CLUSTER_PORT

	s := RunServer(opts)
	defer s.Shutdown()

	opts = &Options{
		Host: "127.0.0.1",
		Port: -1,
		Cluster: ClusterOpts{
			Name: "A",
			Host: "127.0.0.1",
			Port: -1,
		},
		NoLog:           true,
		NoSigs:          true,
		NoSystemAccount: true,
	}
	routeURL, _ := url.Parse(fmt.Sprintf("nats-route://127.0.0.1:%d", s.ClusterAddr().Port))
	opts.Routes = []*url.URL{routeURL}

	start := time.Now()
	sc := RunServer(opts)
	defer sc.Shutdown()

	checkClusterFormed(t, s, sc)

	url := fmt.Sprintf("http://127.0.0.1:%d/", s.MonitorAddr().Port)
	for mode := 0; mode < 2; mode++ {
		c := pollConnz(t, s, mode, url+"connz", nil)
		// Test contents..
		// Make sure routes don't show up under connz, but do under routez
		if c.NumConns != 0 {
			t.Fatalf("Expected 0 connections, got %d", c.NumConns)
		}
		if c.Conns == nil || len(c.Conns) != 0 {
			t.Fatalf("Expected 0 connections in array, got %p", c.Conns)
		}
	}

	nc := createClientConnSubscribeAndPublish(t, sc)
	defer nc.Close()

	nc.Subscribe("hello.bar", func(m *nats.Msg) {})
	nc.Flush()
	checkExpectedSubs(t, 1, s, sc)

	// Now check routez
	urls := []string{"routez", "routez?subs=1", "routez?subs=detail"}
	for subs, urlSuffix := range urls {
		for mode := 0; mode < 2; mode++ {
			rz := pollRoutez(t, s, mode, url+urlSuffix, &RoutezOptions{Subscriptions: subs == 1, SubscriptionsDetail: subs == 2})

			if rz.NumRoutes != DEFAULT_ROUTE_POOL_SIZE {
				t.Fatalf("Expected %d route, got %d", DEFAULT_ROUTE_POOL_SIZE, rz.NumRoutes)
			}

			if len(rz.Routes) != DEFAULT_ROUTE_POOL_SIZE {
				t.Fatalf("Expected route array of %d, got %v", DEFAULT_ROUTE_POOL_SIZE, len(rz.Routes))
			}

			route := rz.Routes[0]

			if route.DidSolicit {
				t.Fatalf("Expected unsolicited route, got %v", route.DidSolicit)
			}

			if route.Start.IsZero() {
				t.Fatalf("Expected Start to be set, got %+v", route)
			} else if route.Start.Before(start) {
				t.Fatalf("Unexpected start time: route was started around %v, got %v", start, route.Start)
			}
			if route.LastActivity.IsZero() {
				t.Fatalf("Expected LastActivity to be set, got %+v", route)
			}
			if route.Uptime == _EMPTY_ {
				t.Fatalf("Expected Uptime to be set, it was not")
			}
			if route.Idle == _EMPTY_ {
				t.Fatalf("Expected Idle to be set, it was not")
			}

			// Don't ask for subs, so there should not be any
			if subs == 0 {
				if len(route.Subs) != 0 {
					t.Fatalf("There should not be subs, got %v", len(route.Subs))
				}
			} else if subs == 1 {
				if len(route.Subs) != 1 && len(route.SubsDetail) != 0 {
					t.Fatalf("There should be 1 sub, got %v", len(route.Subs))
				}
			} else if subs == 2 {
				if len(route.SubsDetail) != 1 && len(route.Subs) != 0 {
					t.Fatalf("There should be 1 sub, got %v", len(route.SubsDetail))
				}
			}
		}
	}
}

func TestMonitorRoutezWithBadParams(t *testing.T) {
	s := runMonitorServer()
	defer s.Shutdown()

	url := fmt.Sprintf("http://127.0.0.1:%d/routez?", s.MonitorAddr().Port)
	readBodyEx(t, url+"subs=xxx", http.StatusBadRequest, textPlain)
}

func pollSubsz(t *testing.T, s *Server, mode int, url string, opts *SubszOptions) *Subsz {
	t.Helper()
	if mode == 0 {
		body := readBody(t, url)
		sz := &Subsz{}
		if err := json.Unmarshal(body, sz); err != nil {
			t.Fatalf("Got an error unmarshalling the body: %v\n", err)
		}
		return sz
	}
	sz, err := s.Subsz(opts)
	if err != nil {
		t.Fatalf("Error on Subsz: %v", err)
	}
	return sz
}

func TestSubsz(t *testing.T) {
	s := runMonitorServer()
	defer s.Shutdown()

	nc := createClientConnSubscribeAndPublish(t, s)
	defer nc.Close()

	url := fmt.Sprintf("http://127.0.0.1:%d/", s.MonitorAddr().Port)

	for mode := 0; mode < 2; mode++ {
		sl := pollSubsz(t, s, mode, url+"subsz", nil)

		require_Equal(t, sl.NumSubs, 0)
		require_Equal(t, sl.NumInserts, 1)
		require_Equal(t, sl.NumMatches, 1)

		for _, s := range sl.Subs {
			switch s.Account {
			case DEFAULT_GLOBAL_ACCOUNT:
				require_Equal(t, s.AccountTag, DEFAULT_GLOBAL_ACCOUNT)
			case DEFAULT_SYSTEM_ACCOUNT:
				require_Equal(t, s.AccountTag, DEFAULT_SYSTEM_ACCOUNT)
			default:
				t.Fatalf("Unknown account: %q", s.Account)
			}
		}
	}
}

func TestSubszOperatorMode(t *testing.T) {
	sysName := "SYS"
	accName := "APP"

	srvs, sysKp, accKp := runMonitorServerWithOperator(t, sysName, accName)
	for _, s := range srvs {
		defer s.Shutdown()
	}
	s := srvs[0]

	sysPub, _ := sysKp.PublicKey()
	accPub, _ := accKp.PublicKey()

	_, aCreds := createUser(t, accKp)

	nc, err := nats.Connect(s.ClientURL(), nats.UserCredentials(aCreds))
	require_NoError(t, err)
	defer nc.Close()

	url := fmt.Sprintf("http://127.0.0.1:%d/", s.MonitorAddr().Port)

	for mode := 0; mode < 2; mode++ {
		sl := pollSubsz(t, s, mode, url+"subsz", nil)

		for _, s := range sl.Subs {
			switch s.Account {
			case DEFAULT_GLOBAL_ACCOUNT:
				require_Equal(t, s.AccountTag, DEFAULT_GLOBAL_ACCOUNT)
			case sysPub:
				require_Equal(t, s.AccountTag, sysName)
			case accPub:
				require_Equal(t, s.AccountTag, accName)
			default:
				t.Fatalf("Unknown account: %q", s.Account)
			}
		}
	}
}

func TestMonitorSubszDetails(t *testing.T) {
	s := runMonitorServer()
	defer s.Shutdown()

	nc := createClientConnSubscribeAndPublish(t, s)
	defer nc.Close()

	nc.Subscribe("foo.*", func(m *nats.Msg) {})
	nc.Subscribe("foo.bar", func(m *nats.Msg) {})
	nc.Subscribe("foo.foo", func(m *nats.Msg) {})

	nc.Publish("foo.bar", []byte("Hello"))
	nc.Publish("foo.baz", []byte("Hello"))
	nc.Publish("foo.foo", []byte("Hello"))

	nc.Flush()

	url := fmt.Sprintf("http://127.0.0.1:%d/", s.MonitorAddr().Port)

	for mode := 0; mode < 2; mode++ {
		sl := pollSubsz(t, s, mode, url+"subsz?subs=1", &SubszOptions{Subscriptions: true})
		if sl.NumSubs != 3 {
			t.Fatalf("Expected NumSubs of 3, got %d\n", sl.NumSubs)
		}
		if sl.Total != 3 {
			t.Fatalf("Expected Total of 3, got %d\n", sl.Total)
		}
		if len(sl.Subs) != 3 {
			t.Fatalf("Expected subscription details for 3 subs, got %d\n", len(sl.Subs))
		}
	}
}

func TestMonitorSubszWithOffsetAndLimit(t *testing.T) {
	s := runMonitorServer()
	defer s.Shutdown()

	nc := createClientConnSubscribeAndPublish(t, s)
	defer nc.Close()

	for i := 0; i < 200; i++ {
		nc.Subscribe(fmt.Sprintf("foo.%d", i), func(m *nats.Msg) {})
	}
	nc.Flush()

	url := fmt.Sprintf("http://127.0.0.1:%d/", s.MonitorAddr().Port)
	for mode := 0; mode < 2; mode++ {
		sl := pollSubsz(t, s, mode, url+"subsz?subs=1&offset=10&limit=100", &SubszOptions{Subscriptions: true, Offset: 10, Limit: 100})
		if sl.NumSubs != 200 {
			t.Fatalf("Expected NumSubs of 200, got %d\n", sl.NumSubs)
		}
		if sl.Total != 200 {
			t.Fatalf("Expected Total of 200, got %d\n", sl.Total)
		}
		if sl.Offset != 10 {
			t.Fatalf("Expected Offset of 10, got %d\n", sl.Offset)
		}
		if sl.Limit != 100 {
			t.Fatalf("Expected Total of 100, got %d\n", sl.Limit)
		}
		if len(sl.Subs) != 100 {
			t.Fatalf("Expected subscription details for 100 subs, got %d\n", len(sl.Subs))
		}
	}
}

func TestMonitorSubszOffsetOverflow(t *testing.T) {
	s := runMonitorServer()
	defer s.Shutdown()

	nc := createClientConnSubscribeAndPublish(t, s)
	defer nc.Close()

	_, err := nc.Subscribe("foo.*", func(m *nats.Msg) {})
	require_NoError(t, err)
	_, err = nc.Subscribe("foo.bar", func(m *nats.Msg) {})
	require_NoError(t, err)
	_, err = nc.Subscribe("foo.foo", func(m *nats.Msg) {})
	require_NoError(t, err)
	require_NoError(t, nc.Flush())

	url := fmt.Sprintf("http://127.0.0.1:%d/", s.MonitorAddr().Port)

	for mode := 0; mode < 2; mode++ {
		// Offset = math.MaxInt64, Limit = 1 makes Offset+Limit overflow to math.MinInt64.
		sl := pollSubsz(t, s, mode, url+fmt.Sprintf("subsz?subs=1&offset=%d&limit=1", math.MaxInt64),
			&SubszOptions{Subscriptions: true, Offset: math.MaxInt64, Limit: 1})
		if len(sl.Subs) != 0 {
			t.Fatalf("Expected 0 subscription details, got %d", len(sl.Subs))
		}
	}

	// Both Offset and Limit at the max also overflows the sum.
	_, err = s.Subsz(&SubszOptions{Subscriptions: true, Offset: math.MaxInt64, Limit: math.MaxInt64})
	require_NoError(t, err)
}

func TestMonitorSubszTestPubSubject(t *testing.T) {
	s := runMonitorServer()
	defer s.Shutdown()

	nc := createClientConnSubscribeAndPublish(t, s)
	defer nc.Close()

	nc.Subscribe("foo.*", func(m *nats.Msg) {})
	nc.Subscribe("foo.bar", func(m *nats.Msg) {})
	nc.Subscribe("foo.foo", func(m *nats.Msg) {})
	nc.Flush()

	url := fmt.Sprintf("http://127.0.0.1:%d/", s.MonitorAddr().Port)
	for mode := 0; mode < 2; mode++ {
		sl := pollSubsz(t, s, mode, url+"subsz?subs=1&test=foo.foo", &SubszOptions{Subscriptions: true, Test: "foo.foo"})
		if sl.Total != 2 {
			t.Fatalf("Expected Total of 2 match, got %d\n", sl.Total)
		}
		if len(sl.Subs) != 2 {
			t.Fatalf("Expected subscription details for 2 matching subs, got %d\n", len(sl.Subs))
		}
		sl = pollSubsz(t, s, mode, url+"subsz?subs=1&test=foo", &SubszOptions{Subscriptions: true, Test: "foo"})
		if len(sl.Subs) != 0 {
			t.Fatalf("Expected no matching subs, got %d\n", len(sl.Subs))
		}
	}
	// Make sure we get an error with invalid test subject.
	testUrl := url + "subsz?subs=1&"
	readBodyEx(t, testUrl+"test=*", http.StatusBadRequest, textPlain)
	readBodyEx(t, testUrl+"test=foo.*", http.StatusBadRequest, textPlain)
	readBodyEx(t, testUrl+"test=foo.>", http.StatusBadRequest, textPlain)
	readBodyEx(t, testUrl+"test=foo..bar", http.StatusBadRequest, textPlain)
}

func TestMonitorSubszMultiAccount(t *testing.T) {
	s := runMonitorServerWithAccounts()
	defer s.Shutdown()

	ncA := createClientConnWithUserSubscribeAndPublish(t, s, "a", "a")
	defer ncA.Close()

	ncA.Subscribe("foo.*", func(m *nats.Msg) {})
	ncA.Subscribe("foo.bar", func(m *nats.Msg) {})
	ncA.Subscribe("foo.foo", func(m *nats.Msg) {})

	ncA.Publish("foo.bar", []byte("Hello"))
	ncA.Publish("foo.baz", []byte("Hello"))
	ncA.Publish("foo.foo", []byte("Hello"))

	ncA.Flush()

	ncB := createClientConnWithUserSubscribeAndPublish(t, s, "b", "b")
	defer ncB.Close()

	ncB.Subscribe("foo.*", func(m *nats.Msg) {})
	ncB.Subscribe("foo.bar", func(m *nats.Msg) {})
	ncB.Subscribe("foo.foo", func(m *nats.Msg) {})

	ncB.Publish("foo.bar", []byte("Hello"))
	ncB.Publish("foo.baz", []byte("Hello"))
	ncB.Publish("foo.foo", []byte("Hello"))

	ncB.Flush()

	url := fmt.Sprintf("http://127.0.0.1:%d/", s.MonitorAddr().Port)

	for mode := 0; mode < 2; mode++ {
		sl := pollSubsz(t, s, mode, url+"subsz?subs=1", &SubszOptions{Subscriptions: true})
		if sl.NumSubs != 6 {
			t.Fatalf("Expected NumSubs of 6, got %d\n", sl.NumSubs)
		}
		if sl.Total != 6 {
			t.Fatalf("Expected Total of 6, got %d\n", sl.Total)
		}
		if len(sl.Subs) != 6 {
			t.Fatalf("Expected subscription details for 6 subs, got %d\n", len(sl.Subs))
		}
		for _, sd := range sl.Subs {
			if sd.Account != "A" && sd.Account != "B" {
				t.Fatalf("Expected account information to be present and be 'A' or 'B', got %q", sd.Account)
			}
		}

		// Now make sure we can filter on account.
		sl = pollSubsz(t, s, mode, url+"subsz?subs=1&acc=A", &SubszOptions{Account: "A", Subscriptions: true})
		if sl.NumSubs != 3 {
			t.Fatalf("Expected NumSubs of 3, got %d\n", sl.NumSubs)
		}
		if sl.Total != 3 {
			t.Fatalf("Expected Total of 6, got %d\n", sl.Total)
		}
		if len(sl.Subs) != 3 {
			t.Fatalf("Expected subscription details for 6 subs, got %d\n", len(sl.Subs))
		}
		for _, sd := range sl.Subs {
			if sd.Account != "A" {
				t.Fatalf("Expected account information to be present and be 'A', got %q", sd.Account)
			}
		}
	}
}

func TestMonitorSubszMultiAccountWithOffsetAndLimit(t *testing.T) {
	s := runMonitorServer()
	defer s.Shutdown()

	ncA := createClientConnWithUserSubscribeAndPublish(t, s, "a", "a")
	defer ncA.Close()

	for i := 0; i < 200; i++ {
		ncA.Subscribe(fmt.Sprintf("foo.%d", i), func(m *nats.Msg) {})
	}
	ncA.Flush()

	ncB := createClientConnWithUserSubscribeAndPublish(t, s, "b", "b")
	defer ncB.Close()

	for i := 0; i < 200; i++ {
		ncB.Subscribe(fmt.Sprintf("foo.%d", i), func(m *nats.Msg) {})
	}
	ncB.Flush()

	url := fmt.Sprintf("http://127.0.0.1:%d/", s.MonitorAddr().Port)
	for mode := 0; mode < 2; mode++ {
		sl := pollSubsz(t, s, mode, url+"subsz?subs=1&offset=10&limit=100", &SubszOptions{Subscriptions: true, Offset: 10, Limit: 100})
		if sl.NumSubs != 400 {
			t.Fatalf("Expected NumSubs of 200, got %d\n", sl.NumSubs)
		}
		if sl.Total != 400 {
			t.Fatalf("Expected Total of 400, got %d\n", sl.Total)
		}
		if sl.Offset != 10 {
			t.Fatalf("Expected Offset of 10, got %d\n", sl.Offset)
		}
		if sl.Limit != 100 {
			t.Fatalf("Expected Total of 100, got %d\n", sl.Limit)
		}
		if len(sl.Subs) != 100 {
			t.Fatalf("Expected subscription details for 100 subs, got %d\n", len(sl.Subs))
		}
	}
}

// Tests handle root
func TestMonitorHandleRoot(t *testing.T) {
	s := runMonitorServer()
	defer s.Shutdown()

	nc := createClientConnSubscribeAndPublish(t, s)
	defer nc.Close()

	resp, err := http.Get(fmt.Sprintf("http://127.0.0.1:%d/", s.MonitorAddr().Port))
	if err != nil {
		t.Fatalf("Expected no error: Got %v\n", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("Expected a %d response, got %d\n", http.StatusOK, resp.StatusCode)
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("Expected no error reading body: Got %v\n", err)
	}
	for _, b := range body {
		if b > unicode.MaxASCII {
			t.Fatalf("Expected body to contain only ASCII characters, but got %v\n", b)
		}
	}

	ct := resp.Header.Get("Content-Type")
	if !strings.Contains(ct, "text/html") {
		t.Fatalf("Expected text/html response, got %s\n", ct)
	}
}

func TestMonitorConnzWithNamedClient(t *testing.T) {
	s := runMonitorServer()
	defer s.Shutdown()

	clientName := "test-client"
	nc := createClientConnWithName(t, clientName, s)
	defer nc.Close()

	url := fmt.Sprintf("http://127.0.0.1:%d/", s.MonitorAddr().Port)
	for mode := 0; mode < 2; mode++ {
		// Confirm server is exposing client name in monitoring endpoint.
		c := pollConnz(t, s, mode, url+"connz", nil)
		got := len(c.Conns)
		expected := 1
		if got != expected {
			t.Fatalf("Expected %d connection in array, got %d\n", expected, got)
		}

		conn := c.Conns[0]
		if conn.Name != clientName {
			t.Fatalf("Expected client to have name %q. got %q", clientName, conn.Name)
		}
	}
}

func TestMonitorConnzWithStateForClosedConns(t *testing.T) {
	s := runMonitorServer()
	defer s.Shutdown()

	numEach := 10
	// Create 10 closed, and 10 to leave open.
	for i := 0; i < numEach; i++ {
		nc := createClientConnSubscribeAndPublish(t, s)
		nc.Subscribe("hello.closed.conns", func(m *nats.Msg) {})
		nc.Close()
		nc = createClientConnSubscribeAndPublish(t, s)
		nc.Subscribe("hello.open.conns", func(m *nats.Msg) {})
		defer nc.Close()
	}

	url := fmt.Sprintf("http://127.0.0.1:%d/", s.MonitorAddr().Port)

	for mode := 0; mode < 2; mode++ {
		checkFor(t, 2*time.Second, 10*time.Millisecond, func() error {
			// Look at all open
			c := pollConnz(t, s, mode, url+"connz?state=open", &ConnzOptions{State: ConnOpen})
			if lc := len(c.Conns); lc != numEach {
				return fmt.Errorf("Expected %d connections in array, got %d", numEach, lc)
			}
			// Look at all closed
			c = pollConnz(t, s, mode, url+"connz?state=closed", &ConnzOptions{State: ConnClosed})
			if lc := len(c.Conns); lc != numEach {
				return fmt.Errorf("Expected %d connections in array, got %d", numEach, lc)
			}
			// Look at all
			c = pollConnz(t, s, mode, url+"connz?state=ALL", &ConnzOptions{State: ConnAll})
			if lc := len(c.Conns); lc != numEach*2 {
				return fmt.Errorf("Expected %d connections in array, got %d", 2*numEach, lc)
			}
			// Look at CID #1, which is in closed.
			c = pollConnz(t, s, mode, url+"connz?cid=1&state=open", &ConnzOptions{CID: 1, State: ConnOpen})
			if lc := len(c.Conns); lc != 0 {
				return fmt.Errorf("Expected no connections in open array, got %d", lc)
			}
			c = pollConnz(t, s, mode, url+"connz?cid=1&state=closed", &ConnzOptions{CID: 1, State: ConnClosed})
			if lc := len(c.Conns); lc != 1 {
				return fmt.Errorf("Expected a connection in closed array, got %d", lc)
			}
			c = pollConnz(t, s, mode, url+"connz?cid=1&state=ALL", &ConnzOptions{CID: 1, State: ConnAll})
			if lc := len(c.Conns); lc != 1 {
				return fmt.Errorf("Expected a connection in closed array, got %d", lc)
			}
			c = pollConnz(t, s, mode, url+"connz?cid=1&state=closed&subs=true",
				&ConnzOptions{CID: 1, State: ConnClosed, Subscriptions: true})
			if lc := len(c.Conns); lc != 1 {
				return fmt.Errorf("Expected a connection in closed array, got %d", lc)
			}
			ci := c.Conns[0]
			if ci.NumSubs != 1 {
				return fmt.Errorf("Expected NumSubs to be 1, got %d", ci.NumSubs)
			}
			if len(ci.Subs) != 1 {
				return fmt.Errorf("Expected len(ci.Subs) to be 1 also, got %d", len(ci.Subs))
			}
			// Now ask for same thing without subs and make sure they are not returned.
			c = pollConnz(t, s, mode, url+"connz?cid=1&state=closed&subs=false",
				&ConnzOptions{CID: 1, State: ConnClosed, Subscriptions: false})
			if lc := len(c.Conns); lc != 1 {
				return fmt.Errorf("Expected a connection in closed array, got %d", lc)
			}
			ci = c.Conns[0]
			if ci.NumSubs != 1 {
				return fmt.Errorf("Expected NumSubs to be 1, got %d", ci.NumSubs)
			}
			if len(ci.Subs) != 0 {
				return fmt.Errorf("Expected len(ci.Subs) to be 0 since subs=false, got %d", len(ci.Subs))
			}

			// CID #2 is in open
			c = pollConnz(t, s, mode, url+"connz?cid=2&state=open", &ConnzOptions{CID: 2, State: ConnOpen})
			if lc := len(c.Conns); lc != 1 {
				return fmt.Errorf("Expected a connection in open array, got %d", lc)
			}
			// It should also work if we ask for "state=all"
			c = pollConnz(t, s, mode, url+"connz?cid=2&state=all", &ConnzOptions{CID: 2, State: ConnAll})
			if lc := len(c.Conns); lc != 1 {
				return fmt.Errorf("Expected a connection in open array, got %d", lc)
			}
			// But not for "state=closed"
			c = pollConnz(t, s, mode, url+"connz?cid=2&state=closed", &ConnzOptions{CID: 2, State: ConnClosed})
			if lc := len(c.Conns); lc != 0 {
				return fmt.Errorf("Expected no connections in closed array, got %d", lc)
			}
			return nil
		})
	}
}

// Make sure options for ConnInfo like subs=1, authuser, etc do not cause a race.
func TestMonitorConnzClosedConnsRace(t *testing.T) {
	s := runMonitorServer()
	defer s.Shutdown()

	// Create 100 closed connections.
	for i := 0; i < 100; i++ {
		nc := createClientConnSubscribeAndPublish(t, s)
		nc.Close()
	}

	urlWithoutSubs := fmt.Sprintf("http://127.0.0.1:%d/connz?state=closed", s.MonitorAddr().Port)
	urlWithSubs := urlWithoutSubs + "&subs=true"

	checkClosedConns(t, s, 100, 2*time.Second)

	wg := &sync.WaitGroup{}

	fn := func(url string) {
		deadline := time.Now().Add(1 * time.Second)
		for time.Now().Before(deadline) {
			c := pollConnz(t, s, 0, url, nil)
			if len(c.Conns) != 100 {
				t.Errorf("Incorrect Results: %+v\n", c)
			}
		}
		wg.Done()
	}

	wg.Add(2)
	go fn(urlWithSubs)
	go fn(urlWithoutSubs)
	wg.Wait()
}

// Make sure a bad client that is disconnected right away has proper values.
func TestMonitorConnzClosedConnsBadClient(t *testing.T) {
	s := runMonitorServer()
	defer s.Shutdown()

	opts := s.getOpts()

	rc, err := net.Dial("tcp", net.JoinHostPort(opts.Host, fmt.Sprintf("%d", opts.Port)))
	if err != nil {
		t.Fatalf("Error on dial: %v", err)
	}
	rc.Close()

	checkClosedConns(t, s, 1, 2*time.Second)

	c := pollConnz(t, s, 1, "", &ConnzOptions{State: ConnClosed})
	if len(c.Conns) != 1 {
		t.Errorf("Incorrect Results: %+v\n", c)
	}
	ci := c.Conns[0]

	uptime := ci.Stop.Sub(ci.Start)
	idle, err := time.ParseDuration(ci.Idle)
	if err != nil {
		t.Fatalf("Could not parse Idle: %v\n", err)
	}
	if idle > uptime {
		t.Fatalf("Idle can't be larger then uptime, %v vs %v\n", idle, uptime)
	}
	if ci.LastActivity.IsZero() {
		t.Fatalf("LastActivity should not be Zero\n")
	}
}

// Make sure a bad client that tries to connect plain to TLS has proper values.
func TestMonitorConnzClosedConnsBadTLSClient(t *testing.T) {
	resetPreviousHTTPConnections()

	tc := &TLSConfigOpts{}
	tc.CertFile = "configs/certs/server.pem"
	tc.KeyFile = "configs/certs/key.pem"

	var err error
	opts := DefaultMonitorOptions()
	opts.NoSystemAccount = true
	opts.TLSTimeout = 1.5 // 1.5 seconds
	opts.TLSConfig, err = GenTLSConfig(tc)
	if err != nil {
		t.Fatalf("Error creating TSL config: %v", err)
	}

	s := RunServer(opts)
	defer s.Shutdown()

	opts = s.getOpts()

	rc, err := net.Dial("tcp", net.JoinHostPort(opts.Host, fmt.Sprintf("%d", opts.Port)))
	if err != nil {
		t.Fatalf("Error on dial: %v", err)
	}
	rc.Write([]byte("CONNECT {}\r\n"))
	rc.Close()

	checkClosedConns(t, s, 1, 2*time.Second)

	c := pollConnz(t, s, 1, "", &ConnzOptions{State: ConnClosed})
	if len(c.Conns) != 1 {
		t.Errorf("Incorrect Results: %+v\n", c)
	}
	ci := c.Conns[0]

	uptime := ci.Stop.Sub(ci.Start)
	idle, err := time.ParseDuration(ci.Idle)
	if err != nil {
		t.Fatalf("Could not parse Idle: %v\n", err)
	}
	if idle > uptime {
		t.Fatalf("Idle can't be larger then uptime, %v vs %v\n", idle, uptime)
	}
	if ci.LastActivity.IsZero() {
		t.Fatalf("LastActivity should not be Zero\n")
	}
}

// Create a connection to test ConnInfo
func createClientConnWithUserSubscribeAndPublish(t *testing.T, s *Server, user, pwd string) *nats.Conn {
	natsURL := ""
	if user == "" {
		natsURL = fmt.Sprintf("nats://127.0.0.1:%d", s.Addr().(*net.TCPAddr).Port)
	} else {
		natsURL = fmt.Sprintf("nats://%s:%s@127.0.0.1:%d", user, pwd, s.Addr().(*net.TCPAddr).Port)
	}
	client := nats.GetDefaultOptions()
	client.Servers = []string{natsURL}
	nc, err := client.Connect()
	if err != nil {
		t.Fatalf("Error creating client: %v to: %s\n", err, natsURL)
	}

	ch := make(chan bool)
	inbox := nats.NewInbox()
	sub, err := nc.Subscribe(inbox, func(m *nats.Msg) { ch <- true })
	if err != nil {
		t.Fatalf("Error subscribing to `%s`: %v\n", inbox, err)
	}
	nc.Publish(inbox, []byte("Hello"))
	// Wait for message
	<-ch
	sub.Unsubscribe()
	close(ch)
	nc.Flush()
	return nc
}

func createClientConnSubscribeAndPublish(t *testing.T, s *Server) *nats.Conn {
	return createClientConnWithUserSubscribeAndPublish(t, s, "", "")
}

func createClientConnWithName(t *testing.T, name string, s *Server) *nats.Conn {
	natsURI := fmt.Sprintf("nats://127.0.0.1:%d", s.Addr().(*net.TCPAddr).Port)

	client := nats.GetDefaultOptions()
	client.Servers = []string{natsURI}
	client.Name = name
	nc, err := client.Connect()
	if err != nil {
		t.Fatalf("Error creating client: %v\n", err)
	}
	return nc
}

func TestMonitorStacksz(t *testing.T) {
	s := runMonitorServer()
	defer s.Shutdown()

	url := fmt.Sprintf("http://127.0.0.1:%d/", s.MonitorAddr().Port)
	body := readBody(t, url+"stacksz")
	// Check content
	str := string(body)
	if !strings.Contains(str, "HandleStacksz") {
		t.Fatalf("Result does not seem to contain server's stacks:\n%v", str)
	}
}

func TestMonitorConcurrentMonitoring(t *testing.T) {
	s := runMonitorServer()
	defer s.Shutdown()

	url := fmt.Sprintf("http://127.0.0.1:%d/", s.MonitorAddr().Port)
	// Get some endpoints. Make sure we have at least varz,
	// and the more the merrier.
	endpoints := []string{"varz", "varz", "varz", "connz", "connz", "subsz", "subsz", "routez", "routez"}
	wg := &sync.WaitGroup{}
	wg.Add(len(endpoints))
	ech := make(chan string, len(endpoints))

	for _, e := range endpoints {
		go func(endpoint string) {
			defer wg.Done()
			for i := 0; i < 50; i++ {
				resp, err := http.Get(url + endpoint)
				if err != nil {
					ech <- fmt.Sprintf("Expected no error: Got %v\n", err)
					return
				}
				defer resp.Body.Close()
				if resp.StatusCode != http.StatusOK {
					ech <- fmt.Sprintf("Expected a %v response, got %d\n", http.StatusOK, resp.StatusCode)
					return
				}
				ct := resp.Header.Get("Content-Type")
				if ct != "application/json" {
					ech <- fmt.Sprintf("Expected application/json content-type, got %s\n", ct)
					return
				}
				if _, err := io.ReadAll(resp.Body); err != nil {
					ech <- fmt.Sprintf("Got an error reading the body: %v\n", err)
					return
				}
				resp.Body.Close()
			}
		}(e)
	}
	wg.Wait()
	// Check for any errors
	select {
	case err := <-ech:
		t.Fatal(err)
	default:
	}
}

func TestMonitorHandler(t *testing.T) {
	s := runMonitorServer()
	defer s.Shutdown()
	handler := s.HTTPHandler()
	if handler == nil {
		t.Fatal("HTTP Handler should be set")
	}
	s.Shutdown()
	handler = s.HTTPHandler()
	if handler != nil {
		t.Fatal("HTTP Handler should be nil")
	}
}

func TestMonitorRoutezRace(t *testing.T) {
	resetPreviousHTTPConnections()
	srvAOpts := DefaultMonitorOptions()
	srvAOpts.NoSystemAccount = true
	srvAOpts.Cluster.Name = "B"
	srvAOpts.Cluster.Port = -1
	srvA := RunServer(srvAOpts)
	defer srvA.Shutdown()

	srvBOpts := nextServerOpts(srvAOpts)
	srvBOpts.Routes = RoutesFromStr(fmt.Sprintf("nats://127.0.0.1:%d", srvA.ClusterAddr().Port))

	doneCh := make(chan struct{})
	go func() {
		defer func() {
			doneCh <- struct{}{}
		}()
		for i := 0; i < 10; i++ {
			time.Sleep(10 * time.Millisecond)
			// Reset ports
			srvBOpts.Port = -1
			srvBOpts.Cluster.Port = -1
			srvB := RunServer(srvBOpts)
			time.Sleep(20 * time.Millisecond)
			srvB.Shutdown()
		}
	}()
	done := false
	for !done {
		if _, err := srvA.Routez(nil); err != nil {
			time.Sleep(10 * time.Millisecond)
		}
		select {
		case <-doneCh:
			done = true
		default:
		}
	}
}

func TestMonitorConnzTLSInHandshake(t *testing.T) {
	resetPreviousHTTPConnections()

	tc := &TLSConfigOpts{}
	tc.CertFile = "configs/certs/server.pem"
	tc.KeyFile = "configs/certs/key.pem"

	var err error
	opts := DefaultMonitorOptions()
	opts.NoSystemAccount = true
	opts.TLSTimeout = 1.5 // 1.5 seconds
	opts.TLSConfig, err = GenTLSConfig(tc)
	if err != nil {
		t.Fatalf("Error creating TSL config: %v", err)
	}

	s := RunServer(opts)
	defer s.Shutdown()

	// Create bare TCP connection to delay client TLS handshake
	c, err := net.Dial("tcp", net.JoinHostPort(opts.Host, fmt.Sprintf("%d", opts.Port)))
	if err != nil {
		t.Fatalf("Error on dial: %v", err)
	}
	defer c.Close()

	// Wait for the connection to be registered
	checkClientsCount(t, s, 1)

	start := time.Now()
	endpoint := fmt.Sprintf("http://%s:%d/connz", opts.HTTPHost, s.MonitorAddr().Port)
	for mode := 0; mode < 2; mode++ {
		connz := pollConnz(t, s, mode, endpoint, nil)
		duration := time.Since(start)
		if duration >= 1500*time.Millisecond {
			t.Fatalf("Looks like connz blocked on handshake, took %v", duration)
		}
		if len(connz.Conns) != 1 {
			t.Fatalf("Expected 1 conn, got %v", len(connz.Conns))
		}
		conn := connz.Conns[0]
		// TLS fields should be not set
		if conn.TLSVersion != "" || conn.TLSCipher != "" {
			t.Fatalf("Expected TLS fields to not be set, got version:%v cipher:%v", conn.TLSVersion, conn.TLSCipher)
		}
	}
}

func TestMonitorConnzTLSCfg(t *testing.T) {
	resetPreviousHTTPConnections()

	tc := &TLSConfigOpts{}
	tc.CertFile = "configs/certs/server.pem"
	tc.KeyFile = "configs/certs/key.pem"

	var err error
	opts := DefaultMonitorOptions()
	opts.NoSystemAccount = true
	opts.TLSTimeout = 1.5 // 1.5 seconds
	opts.TLSConfig, err = GenTLSConfig(tc)
	require_NoError(t, err)
	opts.TLSConfig.ClientAuth = tls.RequireAndVerifyClientCert
	opts.Gateway.TLSConfig, err = GenTLSConfig(tc)
	require_NoError(t, err)
	opts.Gateway.TLSTimeout = 1.5
	opts.LeafNode.TLSConfig, err = GenTLSConfig(tc)
	require_NoError(t, err)
	opts.LeafNode.TLSConfig.ClientAuth = tls.RequireAndVerifyClientCert
	opts.LeafNode.TLSTimeout = 1.5
	opts.Cluster.TLSConfig, err = GenTLSConfig(tc)
	require_NoError(t, err)
	opts.Cluster.TLSTimeout = 1.5

	s := RunServer(opts)
	defer s.Shutdown()

	check := func(verify, required bool, timeout float64) {
		t.Helper()
		if !verify {
			t.Fatalf("Expected tls_verify to be true")
		}
		if !required {
			t.Fatalf("Expected tls_required to be true")
		}
		if timeout != 1.5 {
			t.Fatalf("Expected tls_timeout to be 1.5")
		}
	}

	start := time.Now()
	endpoint := fmt.Sprintf("http://%s:%d/varz", opts.HTTPHost, s.MonitorAddr().Port)
	for mode := 0; mode < 2; mode++ {
		varz := pollVarz(t, s, mode, endpoint, nil)
		duration := time.Since(start)
		if duration >= 1500*time.Millisecond {
			t.Fatalf("Looks like varz blocked on handshake, took %v", duration)
		}
		check(varz.TLSVerify, varz.TLSRequired, varz.TLSTimeout)
		check(varz.Cluster.TLSVerify, varz.Cluster.TLSRequired, varz.Cluster.TLSTimeout)
		check(varz.Gateway.TLSVerify, varz.Gateway.TLSRequired, varz.Gateway.TLSTimeout)
		check(varz.LeafNode.TLSVerify, varz.LeafNode.TLSRequired, varz.LeafNode.TLSTimeout)
	}
}

func TestMonitorConnzTLSPeerCerts(t *testing.T) {
	resetPreviousHTTPConnections()

	tc := &TLSConfigOpts{}
	tc.CertFile = "../test/configs/certs/server-cert.pem"
	tc.KeyFile = "../test/configs/certs/server-key.pem"
	tc.CaFile = "../test/configs/certs/ca.pem"
	tc.Verify = true
	tc.Timeout = 2.0

	var err error
	opts := DefaultMonitorOptions()
	opts.TLSConfig, err = GenTLSConfig(tc)
	require_NoError(t, err)

	s := RunServer(opts)
	defer s.Shutdown()

	nc := natsConnect(t, s.ClientURL(),
		nats.ClientCert("../test/configs/certs/client-cert.pem", "../test/configs/certs/client-key.pem"),
		nats.RootCAs("../test/configs/certs/ca.pem"))
	defer nc.Close()

	endpoint := fmt.Sprintf("http://%s:%d/connz", opts.HTTPHost, s.MonitorAddr().Port)
	for mode := 0; mode < 2; mode++ {
		// Without "auth" option, we should not get the details
		connz := pollConnz(t, s, mode, endpoint, nil)
		require_True(t, len(connz.Conns) == 1)
		c := connz.Conns[0]
		if c.TLSPeerCerts != nil {
			t.Fatalf("Did not expect TLSPeerCerts when auth is not specified: %+v", c.TLSPeerCerts)
		}
		// Now specify "auth" option
		connz = pollConnz(t, s, mode, endpoint+"?auth=1", &ConnzOptions{Username: true})
		require_True(t, len(connz.Conns) == 1)
		c = connz.Conns[0]
		if c.TLSPeerCerts == nil {
			t.Fatal("Expected TLSPeerCerts to be set, was not")
		} else if len(c.TLSPeerCerts) != 1 {
			t.Fatalf("Unexpected peer certificates: %+v", c.TLSPeerCerts)
		} else {
			for _, d := range c.TLSPeerCerts {
				if d.Subject != "CN=localhost,OU=nats.io,O=Synadia,ST=California,C=US" {
					t.Fatalf("Unexpected subject: %s", d.Subject)
				}
				if len(d.SubjectPKISha256) != 64 {
					t.Fatalf("Unexpected spki_sha256: %s", d.SubjectPKISha256)
				}
				if len(d.CertSha256) != 64 {
					t.Fatalf("Unexpected cert_sha256: %s", d.CertSha256)
				}
			}
		}
	}
}

func TestMonitorServerIDs(t *testing.T) {
	s := runMonitorServer()
	defer s.Shutdown()

	murl := fmt.Sprintf("http://127.0.0.1:%d/", s.MonitorAddr().Port)

	for mode := 0; mode < 2; mode++ {
		v := pollVarz(t, s, mode, murl+"varz", nil)
		if v.ID == _EMPTY_ {
			t.Fatal("Varz ID is empty")
		}
		c := pollConnz(t, s, mode, murl+"connz", nil)
		if c.ID == _EMPTY_ {
			t.Fatal("Connz ID is empty")
		}
		r := pollRoutez(t, s, mode, murl+"routez", nil)
		if r.ID == _EMPTY_ {
			t.Fatal("Routez ID is empty")
		}
		if v.ID != c.ID || v.ID != r.ID {
			t.Fatalf("Varz ID [%s] is not equal to Connz ID [%s] or Routez ID [%s]", v.ID, c.ID, r.ID)
		}
	}
}

func TestMonitorHttpStatsNoUpdatedWhenUsingServerFuncs(t *testing.T) {
	s := runMonitorServer()
	defer s.Shutdown()

	for i := 0; i < 10; i++ {
		s.Varz(nil)
		s.Connz(nil)
		s.Routez(nil)
		s.Subsz(nil)
	}

	v, _ := s.Varz(nil)
	endpoints := []string{VarzPath, ConnzPath, RoutezPath, SubszPath}
	for _, e := range endpoints {
		stats := v.HTTPReqStats[e]
		if stats != 0 {
			t.Fatalf("Expected HTTPReqStats for %q to be 0, got %v", e, stats)
		}
	}
}

func TestMonitorClusterEmptyWhenNotDefined(t *testing.T) {
	s := runMonitorServer()
	defer s.Shutdown()

	body := readBody(t, fmt.Sprintf("http://127.0.0.1:%d/varz", s.MonitorAddr().Port))
	var v map[string]any
	if err := json.Unmarshal(body, &v); err != nil {
		t.Fatalf("Got an error unmarshalling the body: %v\n", err)
	}
	// Cluster can empty, or be defined but that needs to be empty.
	c, ok := v["cluster"]
	if !ok {
		return
	}
	if len(c.(map[string]any)) != 0 {
		t.Fatalf("Expected an empty cluster definition, instead got %+v\n", c)
	}
}

func TestMonitorRoutezPermissions(t *testing.T) {
	resetPreviousHTTPConnections()
	opts := DefaultMonitorOptions()
	opts.NoSystemAccount = true
	opts.Cluster.Name = "A"
	opts.Cluster.Host = "127.0.0.1"
	opts.Cluster.Port = -1
	opts.Cluster.Permissions = &RoutePermissions{
		Import: &SubjectPermission{
			Allow: []string{"foo"},
		},
		Export: &SubjectPermission{
			Allow: []string{"*"},
			Deny:  []string{"foo", "nats"},
		},
	}

	s1 := RunServer(opts)
	defer s1.Shutdown()

	opts = DefaultMonitorOptions()
	opts.NoSystemAccount = true
	opts.ServerName = "monitor_server_2"
	opts.Cluster.Host = "127.0.0.1"
	opts.Cluster.Name = "A"
	opts.Cluster.Port = -1
	routeURL, _ := url.Parse(fmt.Sprintf("nats-route://127.0.0.1:%d", s1.ClusterAddr().Port))
	opts.Routes = []*url.URL{routeURL}
	opts.HTTPPort = -1

	s2 := RunServer(opts)
	defer s2.Shutdown()

	checkClusterFormed(t, s1, s2)

	urls := []string{
		fmt.Sprintf("http://127.0.0.1:%d/routez", s1.MonitorAddr().Port),
		fmt.Sprintf("http://127.0.0.1:%d/routez", s2.MonitorAddr().Port),
	}
	servers := []*Server{s1, s2}

	for i, url := range urls {
		for mode := 0; mode < 2; mode++ {
			rz := pollRoutez(t, servers[i], mode, url, nil)
			// For server 1, we expect to see imports and exports
			if i == 0 {
				if rz.Import == nil || rz.Import.Allow == nil ||
					len(rz.Import.Allow) != 1 || rz.Import.Allow[0] != "foo" ||
					rz.Import.Deny != nil {
					t.Fatalf("Unexpected Import %v", rz.Import)
				}
				if rz.Export == nil || rz.Export.Allow == nil || rz.Export.Deny == nil ||
					len(rz.Export.Allow) != 1 || rz.Export.Allow[0] != "*" ||
					len(rz.Export.Deny) != 2 || rz.Export.Deny[0] != "foo" || rz.Export.Deny[1] != "nats" {
					t.Fatalf("Unexpected Export %v", rz.Export)
				}
			} else {
				// We expect to see NO imports and exports for server B by default.
				if rz.Import != nil {
					t.Fatal("Routez body should NOT contain \"import\" information.")
				}
				if rz.Export != nil {
					t.Fatal("Routez body should NOT contain \"export\" information.")
				}
				// We do expect to see them show up for the information we have on Server A though.
				if len(rz.Routes) != DEFAULT_ROUTE_POOL_SIZE {
					t.Fatalf("Expected route array of %d, got %v\n", DEFAULT_ROUTE_POOL_SIZE, len(rz.Routes))
				}
				route := rz.Routes[0]
				if route.Import == nil || route.Import.Allow == nil ||
					len(route.Import.Allow) != 1 || route.Import.Allow[0] != "foo" ||
					route.Import.Deny != nil {
					t.Fatalf("Unexpected Import %v", route.Import)
				}
				if route.Export == nil || route.Export.Allow == nil || route.Export.Deny == nil ||
					len(route.Export.Allow) != 1 || route.Export.Allow[0] != "*" ||
					len(route.Export.Deny) != 2 || route.Export.Deny[0] != "foo" || route.Export.Deny[1] != "nats" {
					t.Fatalf("Unexpected Export %v", route.Export)
				}
			}
		}
	}
}

// Benchmark our Connz generation. Don't use HTTP here, just measure server endpoint.
func Benchmark_Connz(b *testing.B) {
	runtime.MemProfileRate = 0

	s := runMonitorServerNoHTTPPort()
	defer s.Shutdown()

	opts := s.getOpts()
	url := fmt.Sprintf("nats://%s:%d", opts.Host, opts.Port)

	// Create 250 connections with 100 subs each.
	for i := 0; i < 250; i++ {
		nc, err := nats.Connect(url)
		if err != nil {
			b.Fatalf("Error on connection[%d] to %s: %v", i, url, err)
		}
		for x := 0; x < 100; x++ {
			subj := fmt.Sprintf("foo.%d", x)
			nc.Subscribe(subj, func(m *nats.Msg) {})
		}
		nc.Flush()
		defer nc.Close()
	}

	b.ResetTimer()
	runtime.MemProfileRate = 1

	copts := &ConnzOptions{Subscriptions: false}
	for i := 0; i < b.N; i++ {
		_, err := s.Connz(copts)
		if err != nil {
			b.Fatalf("Error on Connz(): %v", err)
		}
	}
}

func Benchmark_Varz(b *testing.B) {
	runtime.MemProfileRate = 0

	s := runMonitorServerNoHTTPPort()
	defer s.Shutdown()

	b.ResetTimer()
	runtime.MemProfileRate = 1

	for i := 0; i < b.N; i++ {
		_, err := s.Varz(nil)
		if err != nil {
			b.Fatalf("Error on Connz(): %v", err)
		}
	}
}

func Benchmark_VarzHttp(b *testing.B) {
	runtime.MemProfileRate = 0

	s := runMonitorServer()
	defer s.Shutdown()

	murl := fmt.Sprintf("http://127.0.0.1:%d/varz", s.MonitorAddr().Port)

	b.ResetTimer()
	runtime.MemProfileRate = 1

	for i := 0; i < b.N; i++ {
		v := &Varz{}
		resp, err := http.Get(murl)
		if err != nil {
			b.Fatalf("Expected no error: Got %v\n", err)
		}
		defer resp.Body.Close()
		body, err := io.ReadAll(resp.Body)
		if err != nil {
			b.Fatalf("Got an error reading the body: %v\n", err)
		}
		if err := json.Unmarshal(body, v); err != nil {
			b.Fatalf("Got an error unmarshalling the body: %v\n", err)
		}
		resp.Body.Close()
	}
}

func TestMonitorVarzRaces(t *testing.T) {
	s := runMonitorServer()
	defer s.Shutdown()

	murl := fmt.Sprintf("http://127.0.0.1:%d/varz", s.MonitorAddr().Port)
	done := make(chan struct{})
	wg := sync.WaitGroup{}
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			for i := 0; i < 2; i++ {
				v := pollVarz(t, s, i, murl, nil)
				// Check the field that we are setting in main thread
				// to ensure that we have a copy and there is no
				// race with fields set in s.info and s.opts
				if v.ID == "abc" || v.MaxConn == -1 {
					// We will not get there. Need to have something
					// otherwise staticcheck will report empty branch
					return
				}

				select {
				case <-done:
					return
				default:
				}
			}
		}
	}()

	for i := 0; i < 1000; i++ {
		// Simulate a change in server's info and options
		// by changing something.
		s.mu.Lock()
		s.info.ID = fmt.Sprintf("serverid_%d", i)
		s.opts.MaxConn = 100 + i
		s.mu.Unlock()
		time.Sleep(time.Nanosecond)
	}
	close(done)
	wg.Wait()

	// Now check that there is no race doing parallel polling
	wg.Add(3)
	done = make(chan struct{})
	poll := func() {
		defer wg.Done()
		for {
			for mode := 0; mode < 2; mode++ {
				pollVarz(t, s, mode, murl, nil)
			}
			select {
			case <-done:
				return
			default:
			}
		}
	}
	for i := 0; i < 3; i++ {
		go poll()
	}
	time.Sleep(500 * time.Millisecond)
	close(done)
	wg.Wait()
}

func testMonitorStructPresent(t *testing.T, tag string) {
	t.Helper()

	resetPreviousHTTPConnections()
	opts := DefaultMonitorOptions()
	opts.NoSystemAccount = true
	s := RunServer(opts)
	defer s.Shutdown()

	varzURL := fmt.Sprintf("http://127.0.0.1:%d/varz", s.MonitorAddr().Port)
	body := readBody(t, varzURL)
	if !bytes.Contains(body, []byte(`"`+tag+`": {}`)) {
		t.Fatalf("%s should be present and empty, got %s", tag, body)
	}
}

func TestMonitorCluster(t *testing.T) {
	testMonitorStructPresent(t, "cluster")

	resetPreviousHTTPConnections()
	opts := DefaultMonitorOptions()
	opts.NoSystemAccount = true
	opts.Cluster.Name = "A"
	opts.Cluster.Port = -1
	opts.Cluster.AuthTimeout = 1
	opts.Routes = RoutesFromStr("nats://127.0.0.1:1234")
	s := RunServer(opts)
	defer s.Shutdown()

	expected := ClusterOptsVarz{
		"A",
		opts.Cluster.Host,
		opts.Cluster.Port,
		opts.Cluster.AuthTimeout,
		[]string{"127.0.0.1:1234"},
		opts.Cluster.TLSTimeout,
		opts.Cluster.TLSConfig != nil,
		opts.Cluster.TLSConfig != nil,
		DEFAULT_ROUTE_POOL_SIZE,
		0,
		_EMPTY_,
		time.Time{},
	}

	varzURL := fmt.Sprintf("http://127.0.0.1:%d/varz", s.MonitorAddr().Port)
	for mode := 0; mode < 2; mode++ {
		check := func(t *testing.T, v *Varz) {
			t.Helper()
			if !reflect.DeepEqual(v.Cluster, expected) {
				t.Fatalf("mode=%v - expected %+v, got %+v", mode, expected, v.Cluster)
			}
		}
		v := pollVarz(t, s, mode, varzURL, nil)
		check(t, v)

		// Having this here to make sure that if fields are added in ClusterOptsVarz,
		// we make sure to update this test (compiler will report an error if we don't)
		_ = ClusterOptsVarz{"", "", 0, 0, nil, 2, false, false, 0, 0, _EMPTY_, time.Time{}}

		// Alter the fields to make sure that we have a proper deep copy
		// of what may be stored in the server. Anything we change here
		// should not affect the next returned value.
		v.Cluster.Name = "wrong"
		v.Cluster.Host = "wrong"
		v.Cluster.Port = 0
		v.Cluster.AuthTimeout = 0
		v.Cluster.URLs = []string{"wrong"}
		v = pollVarz(t, s, mode, varzURL, nil)
		check(t, v)
	}
}

func TestMonitorClusterURLs(t *testing.T) {
	resetPreviousHTTPConnections()

	o2 := DefaultOptions()
	o2.Cluster.Host = "127.0.0.1"
	o2.Cluster.Name = "A"

	s2 := RunServer(o2)
	defer s2.Shutdown()

	s2ClusterHostPort := fmt.Sprintf("127.0.0.1:%d", s2.ClusterAddr().Port)

	template := `
		port: -1
		http: -1
		cluster: {
			name: "A"
			port: -1
			routes [
				%s
				%s
			]
		}
	`
	conf := createConfFile(t, []byte(fmt.Sprintf(template, "nats://"+s2ClusterHostPort, "")))
	s1, _ := RunServerWithConfig(conf)
	defer s1.Shutdown()

	checkClusterFormed(t, s1, s2)

	// Check /varz cluster{} to see the URLs from s1 to s2
	varzURL := fmt.Sprintf("http://127.0.0.1:%d/varz", s1.MonitorAddr().Port)
	for mode := 0; mode < 2; mode++ {
		v := pollVarz(t, s1, mode, varzURL, nil)
		if n := len(v.Cluster.URLs); n != 1 {
			t.Fatalf("mode=%v - Expected 1 URL, got %v", mode, n)
		}
		if v.Cluster.URLs[0] != s2ClusterHostPort {
			t.Fatalf("mode=%v - Expected url %q, got %q", mode, s2ClusterHostPort, v.Cluster.URLs[0])
		}
	}

	otherClusterHostPort := "127.0.0.1:1234"
	// Now update the config and add a route
	changeCurrentConfigContentWithNewContent(t, conf, []byte(fmt.Sprintf(template, "nats://"+s2ClusterHostPort, "nats://"+otherClusterHostPort)))

	if err := s1.Reload(); err != nil {
		t.Fatalf("Error on reload: %v", err)
	}

	// Verify cluster still ok
	checkClusterFormed(t, s1, s2)

	// Now verify that s1 reports in /varz the new URL
	checkFor(t, 2*time.Second, 15*time.Millisecond, func() error {
		for mode := 0; mode < 2; mode++ {
			v := pollVarz(t, s1, mode, varzURL, nil)
			if n := len(v.Cluster.URLs); n != 2 {
				t.Fatalf("mode=%v - Expected 2 URL, got %v", mode, n)
			}
			gotS2 := false
			gotOther := false
			for _, u := range v.Cluster.URLs {
				if u == s2ClusterHostPort {
					gotS2 = true
				} else if u == otherClusterHostPort {
					gotOther = true
				} else {
					t.Fatalf("mode=%v - Incorrect url: %q", mode, u)
				}
			}
			if !gotS2 {
				t.Fatalf("mode=%v - Did not get cluster URL for s2", mode)
			}
			if !gotOther {
				t.Fatalf("mode=%v - Did not get the new cluster URL", mode)
			}
		}
		return nil
	})

	// Remove all routes from config
	changeCurrentConfigContentWithNewContent(t, conf, []byte(fmt.Sprintf(template, "", "")))

	if err := s1.Reload(); err != nil {
		t.Fatalf("Error on reload: %v", err)
	}

	// Now verify that s1 reports no ULRs in /varz
	checkFor(t, 2*time.Second, 15*time.Millisecond, func() error {
		for mode := 0; mode < 2; mode++ {
			v := pollVarz(t, s1, mode, varzURL, nil)
			if n := len(v.Cluster.URLs); n != 0 {
				t.Fatalf("mode=%v - Expected 0 URL, got %v", mode, n)
			}
		}
		return nil
	})
}

func TestMonitorGateway(t *testing.T) {
	testMonitorStructPresent(t, "gateway")

	resetPreviousHTTPConnections()
	opts := DefaultMonitorOptions()
	opts.NoSystemAccount = true
	opts.Gateway.Name = "A"
	opts.Gateway.Port = -1
	opts.Gateway.AuthTimeout = 1
	opts.Gateway.TLSTimeout = 1
	opts.Gateway.Advertise = "127.0.0.1"
	opts.Gateway.ConnectRetries = 1
	opts.Gateway.RejectUnknown = false
	u1, _ := url.Parse("nats://ivan:pwd@localhost:1234")
	u2, _ := url.Parse("nats://localhost:1235")
	opts.Gateway.Gateways = []*RemoteGatewayOpts{
		{
			Name:       "B",
			TLSTimeout: 1,
			URLs: []*url.URL{
				u1,
				u2,
			},
		},
	}
	s := RunServer(opts)
	defer s.Shutdown()

	expected := GatewayOptsVarz{
		"A",
		opts.Gateway.Host,
		opts.Gateway.Port,
		opts.Gateway.AuthTimeout,
		opts.Gateway.TLSTimeout,
		opts.Gateway.TLSConfig != nil,
		opts.Gateway.TLSConfig != nil,
		opts.Gateway.Advertise,
		opts.Gateway.ConnectRetries,
		[]RemoteGatewayOptsVarz{{"B", 1, nil}},
		opts.Gateway.RejectUnknown,
		0,
		_EMPTY_,
		time.Time{},
	}
	// Since URLs array is not guaranteed to be always the same order,
	// we don't add it in the expected GatewayOptsVarz, instead we
	// maintain here.
	expectedURLs := []string{"localhost:1234", "localhost:1235"}

	varzURL := fmt.Sprintf("http://127.0.0.1:%d/varz", s.MonitorAddr().Port)
	for mode := 0; mode < 2; mode++ {
		check := func(t *testing.T, v *Varz) {
			t.Helper()
			var urls []string
			if len(v.Gateway.Gateways) == 1 {
				urls = v.Gateway.Gateways[0].URLs
				v.Gateway.Gateways[0].URLs = nil
			}
			if !reflect.DeepEqual(v.Gateway, expected) {
				t.Fatalf("mode=%v - expected %+v, got %+v", mode, expected, v.Gateway)
			}
			// Now compare urls
			for _, u := range expectedURLs {
				ok := false
				for _, u2 := range urls {
					if u == u2 {
						ok = true
						break
					}
				}
				if !ok {
					t.Fatalf("mode=%v - expected urls to be %v, got %v", mode, expected.Gateways[0].URLs, urls)
				}
			}
		}
		v := pollVarz(t, s, mode, varzURL, nil)
		check(t, v)

		// Having this here to make sure that if fields are added in GatewayOptsVarz,
		// we make sure to update this test (compiler will report an error if we don't)
		_ = GatewayOptsVarz{"", "", 0, 0, 0, false, false, "", 0, []RemoteGatewayOptsVarz{{"", 0, nil}}, false, 0, "default", time.Time{}}

		// Alter the fields to make sure that we have a proper deep copy
		// of what may be stored in the server. Anything we change here
		// should not affect the next returned value.
		v.Gateway.Name = "wrong"
		v.Gateway.Host = "wrong"
		v.Gateway.Port = 0
		v.Gateway.AuthTimeout = 1234.5
		v.Gateway.TLSTimeout = 1234.5
		v.Gateway.Advertise = "wrong"
		v.Gateway.ConnectRetries = 1234
		v.Gateway.Gateways[0].Name = "wrong"
		v.Gateway.Gateways[0].TLSTimeout = 1234.5
		v.Gateway.Gateways[0].URLs = []string{"wrong"}
		v.Gateway.RejectUnknown = true
		v = pollVarz(t, s, mode, varzURL, nil)
		check(t, v)
	}
}

func TestMonitorGatewayURLsUpdated(t *testing.T) {
	resetPreviousHTTPConnections()

	ob1 := testDefaultOptionsForGateway("B")
	sb1 := runGatewayServer(ob1)
	defer sb1.Shutdown()

	// Start a1 that has a single URL to sb1.
	oa := testGatewayOptionsFromToWithServers(t, "A", "B", sb1)
	oa.HTTPHost = "127.0.0.1"
	oa.HTTPPort = MONITOR_PORT
	sa := runGatewayServer(oa)
	defer sa.Shutdown()

	waitForOutboundGateways(t, sa, 1, 2*time.Second)

	varzURL := fmt.Sprintf("http://127.0.0.1:%d/varz", sa.MonitorAddr().Port)
	// Check the /varz gateway's URLs
	for mode := 0; mode < 2; mode++ {
		v := pollVarz(t, sa, mode, varzURL, nil)
		if n := len(v.Gateway.Gateways); n != 1 {
			t.Fatalf("mode=%v - Expected 1 remote gateway, got %v", mode, n)
		}
		gw := v.Gateway.Gateways[0]
		if n := len(gw.URLs); n != 1 {
			t.Fatalf("mode=%v - Expected 1 url, got %v", mode, n)
		}
		expected := oa.Gateway.Gateways[0].URLs[0].Host
		if u := gw.URLs[0]; u != expected {
			t.Fatalf("mode=%v - Expected URL %q, got %q", mode, expected, u)
		}
	}

	// Now start sb2 that clusters with sb1. sa should add to its list of URLs
	// sb2 gateway's connect URL.
	ob2 := testDefaultOptionsForGateway("B")
	ob2.Routes = RoutesFromStr(fmt.Sprintf("nats://127.0.0.1:%d", sb1.ClusterAddr().Port))
	sb2 := runGatewayServer(ob2)
	defer sb2.Shutdown()

	// Wait for sb1 and sb2 to connect
	checkClusterFormed(t, sb1, sb2)
	// sb2 should be made aware of gateway A and connect to sa
	waitForInboundGateways(t, sa, 2, 2*time.Second)
	// Now check that URLs in /varz get updated
	checkFor(t, 2*time.Second, 15*time.Millisecond, func() error {
		for mode := 0; mode < 2; mode++ {
			v := pollVarz(t, sa, mode, varzURL, nil)
			if n := len(v.Gateway.Gateways); n != 1 {
				return fmt.Errorf("mode=%v - Expected 1 remote gateway, got %v", mode, n)
			}
			gw := v.Gateway.Gateways[0]
			if n := len(gw.URLs); n != 2 {
				return fmt.Errorf("mode=%v - Expected 2 urls, got %v", mode, n)
			}

			gotSB1 := false
			gotSB2 := false
			for _, u := range gw.URLs {
				if u == fmt.Sprintf("127.0.0.1:%d", sb1.GatewayAddr().Port) {
					gotSB1 = true
				} else if u == fmt.Sprintf("127.0.0.1:%d", sb2.GatewayAddr().Port) {
					gotSB2 = true
				} else {
					return fmt.Errorf("mode=%v - Incorrect URL to gateway B: %v", mode, u)
				}
			}
			if !gotSB1 {
				return fmt.Errorf("mode=%v - Did not get URL to sb1", mode)
			}
			if !gotSB2 {
				return fmt.Errorf("mode=%v - Did not get URL to sb2", mode)
			}
		}
		return nil
	})

	// Now stop sb2 and make sure that its removal is reflected in varz.
	sb2.Shutdown()
	// Wait for it to disappear from sa.
	waitForInboundGateways(t, sa, 1, 2*time.Second)
	// Now check that URLs in /varz get updated.
	checkFor(t, 2*time.Second, 15*time.Millisecond, func() error {
		for mode := 0; mode < 2; mode++ {
			v := pollVarz(t, sa, mode, varzURL, nil)
			if n := len(v.Gateway.Gateways); n != 1 {
				return fmt.Errorf("mode=%v - Expected 1 remote gateway, got %v", mode, n)
			}
			gw := v.Gateway.Gateways[0]
			if n := len(gw.URLs); n != 1 {
				return fmt.Errorf("mode=%v - Expected 1 url, got %v", mode, n)
			}
			u := gw.URLs[0]
			if u != fmt.Sprintf("127.0.0.1:%d", sb1.GatewayAddr().Port) {
				return fmt.Errorf("mode=%v - Did not get URL to sb1", mode)
			}
		}
		return nil
	})
}

func TestMonitorGatewayReportItsOwnURLs(t *testing.T) {
	resetPreviousHTTPConnections()

	// In this test, we show that if a server has its own gateway information
	// as a remote (which is the case when remote gateway definitions is copied
	// on all clusters), we display the defined URLs.
	oa := testGatewayOptionsFromToWithURLs(t, "A", "A", []string{"nats://127.0.0.1:1234", "nats://127.0.0.1:1235"})
	oa.HTTPHost = "127.0.0.1"
	oa.HTTPPort = MONITOR_PORT
	sa := runGatewayServer(oa)
	defer sa.Shutdown()

	varzURL := fmt.Sprintf("http://127.0.0.1:%d/varz", sa.MonitorAddr().Port)
	// Check the /varz gateway's URLs
	for mode := 0; mode < 2; mode++ {
		v := pollVarz(t, sa, mode, varzURL, nil)
		if n := len(v.Gateway.Gateways); n != 1 {
			t.Fatalf("mode=%v - Expected 1 remote gateway, got %v", mode, n)
		}
		gw := v.Gateway.Gateways[0]
		if n := len(gw.URLs); n != 2 {
			t.Fatalf("mode=%v - Expected 2 urls, got %v", mode, gw.URLs)
		}
		expected := []string{"127.0.0.1:1234", "127.0.0.1:1235"}
		if !reflect.DeepEqual(gw.URLs, expected) {
			t.Fatalf("mode=%v - Expected URLs %q, got %q", mode, expected, gw.URLs)
		}
	}
}

func TestMonitorLeafNode(t *testing.T) {
	testMonitorStructPresent(t, "leaf")

	resetPreviousHTTPConnections()
	opts := DefaultMonitorOptions()
	opts.NoSystemAccount = true
	opts.LeafNode.Port = -1
	opts.LeafNode.AuthTimeout = 1
	opts.LeafNode.TLSTimeout = 1
	opts.Accounts = []*Account{NewAccount("acc")}
	u, _ := url.Parse("nats://ivan:pwd@localhost:1234")
	opts.LeafNode.Remotes = []*RemoteLeafOpts{
		{
			LocalAccount: "acc",
			URLs:         []*url.URL{u},
			TLSTimeout:   1,
		},
	}
	s := RunServer(opts)
	defer s.Shutdown()

	expected := LeafNodeOptsVarz{
		opts.LeafNode.Host,
		opts.LeafNode.Port,
		opts.LeafNode.AuthTimeout,
		opts.LeafNode.TLSTimeout,
		opts.LeafNode.TLSConfig != nil,
		opts.LeafNode.TLSConfig != nil,
		[]RemoteLeafOptsVarz{
			{
				"acc", 1, []string{"localhost:1234"}, nil, false,
			},
		},
		false,
		0,
		_EMPTY_,
		time.Time{},
	}

	varzURL := fmt.Sprintf("http://127.0.0.1:%d/varz", s.MonitorAddr().Port)

	for mode := 0; mode < 2; mode++ {
		check := func(t *testing.T, v *Varz) {
			t.Helper()
			// Issue 5913. When we have solicited leafnodes but no clustering
			// and no clustername, we may need a stable clustername so we use
			// the server name as cluster name. However, we should not expose
			// it in /varz.
			if v.Cluster.Name != _EMPTY_ {
				t.Fatalf("mode=%v - unexpected cluster name: %s", mode, v.Cluster.Name)
			}
			// Check rest is as expected.
			if !reflect.DeepEqual(v.LeafNode, expected) {
				t.Fatalf("mode=%v - expected %+v, got %+v", mode, expected, v.LeafNode)
			}
		}
		v := pollVarz(t, s, mode, varzURL, nil)
		check(t, v)

		// Having this here to make sure that if fields are added in ClusterOptsVarz,
		// we make sure to update this test (compiler will report an error if we don't)
		_ = LeafNodeOptsVarz{"", 0, 0, 0, false, false, []RemoteLeafOptsVarz{{"", 0, nil, nil, false}}, false, 0, _EMPTY_, time.Time{}}

		// Alter the fields to make sure that we have a proper deep copy
		// of what may be stored in the server. Anything we change here
		// should not affect the next returned value.
		v.LeafNode.Host = "wrong"
		v.LeafNode.Port = 0
		v.LeafNode.AuthTimeout = 1234.5
		v.LeafNode.TLSTimeout = 1234.5
		v.LeafNode.Remotes[0].LocalAccount = "wrong"
		v.LeafNode.Remotes[0].URLs = append(v.LeafNode.Remotes[0].URLs, "wrong")
		v.LeafNode.Remotes[0].TLSTimeout = 1234.5
		v = pollVarz(t, s, mode, varzURL, nil)
		check(t, v)
	}
}

func pollGatewayz(t *testing.T, s *Server, mode int, url string, opts *GatewayzOptions) *Gatewayz {
	t.Helper()
	if mode == 0 {
		g := &Gatewayz{}
		body := readBody(t, url)
		if err := json.Unmarshal(body, g); err != nil {
			t.Fatalf("Got an error unmarshalling the body: %v\n", err)
		}
		return g
	}
	g, err := s.Gatewayz(opts)
	if err != nil {
		t.Fatalf("Error on Gatewayz: %v", err)
	}
	return g
}

func TestMonitorGatewayz(t *testing.T) {
	resetPreviousHTTPConnections()

	// First check that without gateway configured
	s := runMonitorServer()
	defer s.Shutdown()
	url := fmt.Sprintf("http://127.0.0.1:%d/gatewayz", s.MonitorAddr().Port)
	for pollMode := 0; pollMode < 2; pollMode++ {
		g := pollGatewayz(t, s, pollMode, url, nil)
		// Expect Name and port to be empty
		if g.Name != _EMPTY_ || g.Port != 0 {
			t.Fatalf("Expected no gateway, got %+v", g)
		}
	}
	s.Shutdown()

	ob1 := testDefaultOptionsForGateway("B")
	sb1 := runGatewayServer(ob1)
	defer sb1.Shutdown()

	// Start a1 that has a single URL to sb1.
	oa := testGatewayOptionsFromToWithServers(t, "A", "B", sb1)
	oa.HTTPHost = "127.0.0.1"
	oa.HTTPPort = MONITOR_PORT
	sa := runGatewayServer(oa)
	defer sa.Shutdown()

	waitForOutboundGateways(t, sa, 1, 2*time.Second)
	waitForInboundGateways(t, sa, 1, 2*time.Second)

	waitForOutboundGateways(t, sb1, 1, 2*time.Second)
	waitForInboundGateways(t, sb1, 1, 2*time.Second)

	gatewayzURL := fmt.Sprintf("http://127.0.0.1:%d/gatewayz", sa.MonitorAddr().Port)
	for pollMode := 0; pollMode < 2; pollMode++ {
		g := pollGatewayz(t, sa, pollMode, gatewayzURL, nil)
		if g.Host != oa.Gateway.Host {
			t.Fatalf("mode=%v - Expected host to be %q, got %q", pollMode, oa.Gateway.Host, g.Host)
		}
		if g.Port != oa.Gateway.Port {
			t.Fatalf("mode=%v - Expected port to be %v, got %v", pollMode, oa.Gateway.Port, g.Port)
		}
		if n := len(g.OutboundGateways); n != 1 {
			t.Fatalf("mode=%v - Expected outbound to 1 gateway, got %v", pollMode, n)
		}
		if n := len(g.InboundGateways); n != 1 {
			t.Fatalf("mode=%v - Expected inbound from 1 gateway, got %v", pollMode, n)
		}
		og := g.OutboundGateways["B"]
		if og == nil {
			t.Fatalf("mode=%v - Expected to find outbound connection to B, got none", pollMode)
		}
		if !og.IsConfigured {
			t.Fatalf("mode=%v - Expected gw connection to be configured, was not", pollMode)
		}
		if og.Connection == nil {
			t.Fatalf("mode=%v - Expected outbound connection to B to be set, wat not", pollMode)
		}
		if og.Connection.Name != sb1.ID() {
			t.Fatalf("mode=%v - Expected outbound connection to B to have name %q, got %q", pollMode, sb1.ID(), og.Connection.Name)
		}
		if n := len(og.Accounts); n != 0 {
			t.Fatalf("mode=%v - Expected no account, got %v", pollMode, n)
		}
		ig := g.InboundGateways["B"]
		if ig == nil {
			t.Fatalf("mode=%v - Expected to find inbound connection from B, got none", pollMode)
		}
		if n := len(ig); n != 1 {
			t.Fatalf("mode=%v - Expected 1 inbound connection, got %v", pollMode, n)
		}
		igc := ig[0]
		if igc.Connection == nil {
			t.Fatalf("mode=%v - Expected inbound connection to B to be set, wat not", pollMode)
		}
		if igc.Connection.Name != sb1.ID() {
			t.Fatalf("mode=%v - Expected inbound connection to B to have name %q, got %q", pollMode, sb1.ID(), igc.Connection.Name)
		}
	}

	// Now start sb2 that clusters with sb1. sa should add to its list of URLs
	// sb2 gateway's connect URL.
	ob2 := testDefaultOptionsForGateway("B")
	ob2.Routes = RoutesFromStr(fmt.Sprintf("nats://127.0.0.1:%d", sb1.ClusterAddr().Port))
	sb2 := runGatewayServer(ob2)
	defer sb2.Shutdown()

	// Wait for sb1 and sb2 to connect
	checkClusterFormed(t, sb1, sb2)
	// sb2 should be made aware of gateway A and connect to sa
	waitForInboundGateways(t, sa, 2, 2*time.Second)
	// Now check that URLs in /varz get updated
	checkGatewayB := func(t *testing.T, url string, opts *GatewayzOptions) {
		t.Helper()
		checkFor(t, 2*time.Second, 15*time.Millisecond, func() error {
			for pollMode := 0; pollMode < 2; pollMode++ {
				g := pollGatewayz(t, sa, pollMode, url, opts)
				if n := len(g.OutboundGateways); n != 1 {
					t.Fatalf("mode=%v - Expected outbound to 1 gateway, got %v", pollMode, n)
				}
				// The InboundGateways is a map with key the gateway names,
				// then value is array of connections. So should be 1 here.
				if n := len(g.InboundGateways); n != 1 {
					t.Fatalf("mode=%v - Expected inbound from 1 gateway, got %v", pollMode, n)
				}
				ig := g.InboundGateways["B"]
				if ig == nil {
					t.Fatalf("mode=%v - Expected to find inbound connection from B, got none", pollMode)
				}
				if n := len(ig); n != 2 {
					t.Fatalf("mode=%v - Expected 2 inbound connections from gateway B, got %v", pollMode, n)
				}
				gotSB1 := false
				gotSB2 := false
				for _, rg := range ig {
					if rg.Connection != nil {
						if rg.Connection.Name == sb1.ID() {
							gotSB1 = true
						} else if rg.Connection.Name == sb2.ID() {
							gotSB2 = true
						}
					}
				}
				if !gotSB1 {
					t.Fatalf("mode=%v - Missing inbound connection from sb1", pollMode)
				}
				if !gotSB2 {
					t.Fatalf("mode=%v - Missing inbound connection from sb2", pollMode)
				}
			}
			return nil
		})
	}
	checkGatewayB(t, gatewayzURL, nil)

	// Start a new cluser C that connects to B. A should see it as
	// a non-configured gateway.
	oc := testGatewayOptionsFromToWithServers(t, "C", "B", sb1)
	sc := runGatewayServer(oc)
	defer sc.Shutdown()

	// All servers should have 2 outbound connections (one for each other cluster)
	waitForOutboundGateways(t, sa, 2, 2*time.Second)
	waitForOutboundGateways(t, sb1, 2, 2*time.Second)
	waitForOutboundGateways(t, sb2, 2, 2*time.Second)
	waitForOutboundGateways(t, sc, 2, 2*time.Second)

	// Server sa should have 3 inbounds now
	waitForInboundGateways(t, sa, 3, 2*time.Second)

	// Check gatewayz again to see that we have C now.
	checkFor(t, 2*time.Second, 15*time.Millisecond, func() error {
		for pollMode := 0; pollMode < 2; pollMode++ {
			g := pollGatewayz(t, sa, pollMode, gatewayzURL, nil)
			if n := len(g.OutboundGateways); n != 2 {
				t.Fatalf("mode=%v - Expected outbound to 2 gateways, got %v", pollMode, n)
			}
			// The InboundGateways is a map with key the gateway names,
			// then value is array of connections. So should be 2 here.
			if n := len(g.InboundGateways); n != 2 {
				t.Fatalf("mode=%v - Expected inbound from 2 gateways, got %v", pollMode, n)
			}
			og := g.OutboundGateways["C"]
			if og == nil {
				t.Fatalf("mode=%v - Expected to find outbound connection to C, got none", pollMode)
			}
			if og.IsConfigured {
				t.Fatalf("mode=%v - Expected IsConfigured for gateway C to be false, was true", pollMode)
			}
			if og.Connection == nil {
				t.Fatalf("mode=%v - Expected connection to C, got none", pollMode)
			}
			if og.Connection.Name != sc.ID() {
				t.Fatalf("mode=%v - Expected outbound connection to C to have name %q, got %q", pollMode, sc.ID(), og.Connection.Name)
			}
			ig := g.InboundGateways["C"]
			if ig == nil {
				t.Fatalf("mode=%v - Expected to find inbound connection from C, got none", pollMode)
			}
			if n := len(ig); n != 1 {
				t.Fatalf("mode=%v - Expected 1 inbound connections from gateway C, got %v", pollMode, n)
			}
			igc := ig[0]
			if igc.Connection == nil {
				t.Fatalf("mode=%v - Expected connection to C, got none", pollMode)
			}
			if igc.Connection.Name != sc.ID() {
				t.Fatalf("mode=%v - Expected outbound connection to C to have name %q, got %q", pollMode, sc.ID(), og.Connection.Name)
			}
		}
		return nil
	})

	// Select only 1 gateway by passing the name to option/url
	opts := &GatewayzOptions{Name: "B"}
	checkGatewayB(t, gatewayzURL+"?gw_name=B", opts)

	// Stop gateway C and check that we have only B, with and without filter.
	sc.Shutdown()
	checkGatewayB(t, gatewayzURL+"?gw_name=B", opts)
	checkGatewayB(t, gatewayzURL, nil)
}

func TestMonitorGatewayzAccounts(t *testing.T) {
	GatewayDoNotForceInterestOnlyMode(true)
	defer GatewayDoNotForceInterestOnlyMode(false)

	resetPreviousHTTPConnections()

	// Create bunch of Accounts
	totalAccounts := 15
	accounts := ""
	for i := 0; i < totalAccounts; i++ {
		acc := fmt.Sprintf("	acc_%d: { users=[{user:user_%d, password: pwd}] }\n", i, i)
		accounts += acc
	}

	bConf := createConfFile(t, []byte(fmt.Sprintf(`
		accounts {
			%s
		}
		port: -1
		http: -1
		gateway: {
			name: "B"
			port: -1
		}
		no_sys_acc = true
	`, accounts)))

	sb, ob := RunServerWithConfig(bConf)
	defer sb.Shutdown()
	sb.SetLogger(&DummyLogger{}, true, true)

	// Start a1 that has a single URL to sb1.
	aConf := createConfFile(t, []byte(fmt.Sprintf(`
		accounts {
			%s
		}
		port: -1
		http: -1
		gateway: {
			name: "A"
			port: -1
			gateways [
				{
					name: "B"
					url: "nats://127.0.0.1:%d"
				}
			]
		}
		no_sys_acc = true
	`, accounts, sb.GatewayAddr().Port)))

	sa, oa := RunServerWithConfig(aConf)
	defer sa.Shutdown()
	sa.SetLogger(&DummyLogger{}, true, true)

	waitForOutboundGateways(t, sa, 1, 2*time.Second)
	waitForInboundGateways(t, sa, 1, 2*time.Second)
	waitForOutboundGateways(t, sb, 1, 2*time.Second)
	waitForInboundGateways(t, sb, 1, 2*time.Second)

	// Create clients for each account on A and publish a message
	// so that list of accounts appear in gatewayz
	produceMsgsFromA := func(t *testing.T) {
		t.Helper()
		for i := 0; i < totalAccounts; i++ {
			nc, err := nats.Connect(fmt.Sprintf("nats://user_%d:pwd@%s:%d", i, oa.Host, oa.Port))
			if err != nil {
				t.Fatalf("Error on connect: %v", err)
			}
			nc.Publish("foo", []byte("hello"))
			nc.Flush()
			nc.Close()
		}
	}
	produceMsgsFromA(t)

	// Wait for A- for all accounts
	gwc := sa.getOutboundGatewayConnection("B")
	for i := 0; i < totalAccounts; i++ {
		checkForAccountNoInterest(t, gwc, fmt.Sprintf("acc_%d", i), true, 2*time.Second)
	}

	// Check accounts...
	gatewayzURL := fmt.Sprintf("http://127.0.0.1:%d/gatewayz", sa.MonitorAddr().Port)
	for pollMode := 0; pollMode < 2; pollMode++ {
		// First, without asking for it, they should not be present.
		g := pollGatewayz(t, sa, pollMode, gatewayzURL, nil)
		og := g.OutboundGateways["B"]
		if og == nil {
			t.Fatalf("mode=%v - Expected outbound gateway to B, got none", pollMode)
		}
		if n := len(og.Accounts); n != 0 {
			t.Fatalf("mode=%v - Expected accounts list to not be present by default, got %v", pollMode, n)
		}
		// Now ask for the accounts
		g = pollGatewayz(t, sa, pollMode, gatewayzURL+"?accs=1", &GatewayzOptions{Accounts: true})
		og = g.OutboundGateways["B"]
		if og == nil {
			t.Fatalf("mode=%v - Expected outbound gateway to B, got none", pollMode)
		}
		if n := len(og.Accounts); n != totalAccounts {
			t.Fatalf("mode=%v - Expected to get all %d accounts, got %v", pollMode, totalAccounts, n)
		}
		// Now account details
		for _, acc := range og.Accounts {
			if acc.InterestMode != Optimistic.String() {
				t.Fatalf("mode=%v - Expected optimistic mode, got %q", pollMode, acc.InterestMode)
			}
			// Since there is no interest at all on B, the publish
			// will have resulted in total account no interest, so
			// the number of no interest (subject wise) should be 0
			if acc.NoInterestCount != 0 {
				t.Fatalf("mode=%v - Expected 0 no-interest, got %v", pollMode, acc.NoInterestCount)
			}
			if acc.NumQueueSubscriptions != 0 || acc.TotalSubscriptions != 0 {
				t.Fatalf("mode=%v - Expected total subs to be 0, got %v - and num queue subs to be 0, got %v",
					pollMode, acc.TotalSubscriptions, acc.NumQueueSubscriptions)
			}
		}
	}

	// Check inbound on B
	gwURLServerB := fmt.Sprintf("http://127.0.0.1:%d/gatewayz", sb.MonitorAddr().Port)
	checkFor(t, 2*time.Second, 15*time.Millisecond, func() error {
		for pollMode := 0; pollMode < 2; pollMode++ {
			// First, without asking for it, they should not be present.
			g := pollGatewayz(t, sb, pollMode, gwURLServerB, nil)
			igs := g.InboundGateways["A"]
			if igs == nil {
				return fmt.Errorf("mode=%v - Expected inbound gateway to A, got none", pollMode)
			}
			if len(igs) != 1 {
				return fmt.Errorf("mode=%v - Expected single inbound, got %v", pollMode, len(igs))
			}
			ig := igs[0]
			if n := len(ig.Accounts); n != 0 {
				return fmt.Errorf("mode=%v - Expected no account, got %v", pollMode, n)
			}
			// Check that list of accounts
			g = pollGatewayz(t, sb, pollMode, gwURLServerB+"?accs=1", &GatewayzOptions{Accounts: true})
			igs = g.InboundGateways["A"]
			if igs == nil {
				return fmt.Errorf("mode=%v - Expected inbound gateway to A, got none", pollMode)
			}
			if len(igs) != 1 {
				return fmt.Errorf("mode=%v - Expected single inbound, got %v", pollMode, len(igs))
			}
			ig = igs[0]
			if ig.Connection == nil {
				return fmt.Errorf("mode=%v - Expected inbound connection from A to be set, wat not", pollMode)
			}
			if ig.Connection.Name != sa.ID() {
				t.Fatalf("mode=%v - Expected inbound connection from A to have name %q, got %q", pollMode, sa.ID(), ig.Connection.Name)
			}
			if n := len(ig.Accounts); n != totalAccounts {
				return fmt.Errorf("mode=%v - Expected to get all %d accounts, got %v", pollMode, totalAccounts, n)
			}
			// Now account details
			for _, acc := range ig.Accounts {
				if acc.InterestMode != Optimistic.String() {
					return fmt.Errorf("mode=%v - Expected optimistic mode, got %q", pollMode, acc.InterestMode)
				}
				// Since there is no interest at all on B, the publish
				// will have resulted in total account no interest, so
				// the number of no interest (subject wise) should be 0
				if acc.NoInterestCount != 0 {
					t.Fatalf("mode=%v - Expected 0 no-interest, got %v", pollMode, acc.NoInterestCount)
				}
				// For inbound gateway, NumQueueSubscriptions and TotalSubscriptions
				// are not relevant.
				if acc.NumQueueSubscriptions != 0 || acc.TotalSubscriptions != 0 {
					return fmt.Errorf("mode=%v - For inbound connection, expected num queue subs and total subs to be 0, got %v and %v",
						pollMode, acc.TotalSubscriptions, acc.NumQueueSubscriptions)
				}
			}
		}
		return nil
	})

	// Now create subscriptions on B to prevent A- and check on subject no interest
	for i := 0; i < totalAccounts; i++ {
		nc, err := nats.Connect(fmt.Sprintf("nats://user_%d:pwd@%s:%d", i, ob.Host, ob.Port))
		if err != nil {
			t.Fatalf("Error on connect: %v", err)
		}
		defer nc.Close()
		// Create a queue sub so it shows up in gatewayz
		nc.QueueSubscribeSync("bar", "queue")
		// Create plain subscriptions on baz.0, baz.1 and baz.2.
		// Create to for each subject. Since gateways will send
		// only once per subject, the number of subs should be 3, not 6.
		for j := 0; j < 3; j++ {
			subj := fmt.Sprintf("baz.%d", j)
			nc.SubscribeSync(subj)
			nc.SubscribeSync(subj)
		}
		nc.Flush()
	}

	for i := 0; i < totalAccounts; i++ {
		accName := fmt.Sprintf("acc_%d", i)
		checkForRegisteredQSubInterest(t, sa, "B", accName, "bar", 1, 2*time.Second)
	}

	// Resend msgs from A on foo, on all accounts. There will be no interest on this subject.
	produceMsgsFromA(t)

	for i := 0; i < totalAccounts; i++ {
		accName := fmt.Sprintf("acc_%d", i)
		checkForSubjectNoInterest(t, gwc, accName, "foo", true, 2*time.Second)
		// Verify that we still have the queue interest registered
		checkForRegisteredQSubInterest(t, sa, "B", accName, "bar", 1, 2*time.Second)
	}

	// Check accounts...
	checkFor(t, 2*time.Second, 15*time.Millisecond, func() error {
		for pollMode := 0; pollMode < 2; pollMode++ {
			g := pollGatewayz(t, sa, pollMode, gatewayzURL+"?accs=1", &GatewayzOptions{Accounts: true})
			og := g.OutboundGateways["B"]
			if og == nil {
				return fmt.Errorf("mode=%v - Expected outbound gateway to B, got none", pollMode)
			}
			if n := len(og.Accounts); n != totalAccounts {
				return fmt.Errorf("mode=%v - Expected to get all %d accounts, got %v", pollMode, totalAccounts, n)
			}
			// Now account details
			for _, acc := range og.Accounts {
				if acc.InterestMode != Optimistic.String() {
					return fmt.Errorf("mode=%v - Expected optimistic mode, got %q", pollMode, acc.InterestMode)
				}
				if acc.NoInterestCount != 1 {
					return fmt.Errorf("mode=%v - Expected 1 no-interest, got %v", pollMode, acc.NoInterestCount)
				}
				if acc.NumQueueSubscriptions != 1 || acc.TotalSubscriptions != 1 {
					return fmt.Errorf("mode=%v - Expected total subs to be 1, got %v - and num queue subs to be 1, got %v",
						pollMode, acc.TotalSubscriptions, acc.NumQueueSubscriptions)
				}
			}
		}
		return nil
	})

	// Check inbound on server B
	checkFor(t, 2*time.Second, 15*time.Millisecond, func() error {
		for pollMode := 0; pollMode < 2; pollMode++ {
			// Ask for accounts list
			g := pollGatewayz(t, sb, pollMode, gwURLServerB+"?accs=1", &GatewayzOptions{Accounts: true})
			igs := g.InboundGateways["A"]
			if igs == nil {
				return fmt.Errorf("mode=%v - Expected inbound gateway to A, got none", pollMode)
			}
			if len(igs) != 1 {
				return fmt.Errorf("mode=%v - Expected single inbound, got %v", pollMode, len(igs))
			}
			ig := igs[0]
			if ig.Connection == nil {
				return fmt.Errorf("mode=%v - Expected inbound connection from A to be set, wat not", pollMode)
			}
			if ig.Connection.Name != sa.ID() {
				t.Fatalf("mode=%v - Expected inbound connection from A to have name %q, got %q", pollMode, sa.ID(), ig.Connection.Name)
			}
			if n := len(ig.Accounts); n != totalAccounts {
				return fmt.Errorf("mode=%v - Expected to get all %d accounts, got %v", pollMode, totalAccounts, n)
			}
			// Now account details
			for _, acc := range ig.Accounts {
				if acc.InterestMode != Optimistic.String() {
					return fmt.Errorf("mode=%v - Expected optimistic mode, got %q", pollMode, acc.InterestMode)
				}
				if acc.NoInterestCount != 1 {
					return fmt.Errorf("mode=%v - Expected 1 no-interest, got %v", pollMode, acc.NoInterestCount)
				}
				// For inbound gateway, NumQueueSubscriptions and TotalSubscriptions
				// are not relevant.
				if acc.NumQueueSubscriptions != 0 || acc.TotalSubscriptions != 0 {
					return fmt.Errorf("mode=%v - For inbound connection, expected num queue subs and total subs to be 0, got %v and %v",
						pollMode, acc.TotalSubscriptions, acc.NumQueueSubscriptions)
				}
			}
		}
		return nil
	})

	// Make one of the account to switch to interest only
	nc, err := nats.Connect(fmt.Sprintf("nats://user_1:pwd@%s:%d", oa.Host, oa.Port))
	if err != nil {
		t.Fatalf("Error on connect: %v", err)
	}
	defer nc.Close()
	for i := 0; i < 1100; i++ {
		nc.Publish(fmt.Sprintf("foo.%d", i), []byte("hello"))
	}
	nc.Flush()
	nc.Close()

	// Check that we can select single account
	checkFor(t, 2*time.Second, 15*time.Millisecond, func() error {
		for pollMode := 0; pollMode < 2; pollMode++ {
			g := pollGatewayz(t, sa, pollMode, gatewayzURL+"?gw_name=B&acc_name=acc_1", &GatewayzOptions{Name: "B", AccountName: "acc_1"})
			og := g.OutboundGateways["B"]
			if og == nil {
				return fmt.Errorf("mode=%v - Expected outbound gateway to B, got none", pollMode)
			}
			if n := len(og.Accounts); n != 1 {
				return fmt.Errorf("mode=%v - Expected to get 1 account, got %v", pollMode, n)
			}
			// Now account details
			acc := og.Accounts[0]
			if acc.InterestMode != InterestOnly.String() {
				return fmt.Errorf("mode=%v - Expected interest-only mode, got %q", pollMode, acc.InterestMode)
			}
			// Since we switched, this should be set to 0
			if acc.NoInterestCount != 0 {
				return fmt.Errorf("mode=%v - Expected 0 no-interest, got %v", pollMode, acc.NoInterestCount)
			}
			// We have created 3 subs on that account on B, and 1 queue sub.
			// So total should be 4 and 1 for queue sub.
			if acc.NumQueueSubscriptions != 1 {
				return fmt.Errorf("mode=%v - Expected num queue subs to be 1, got %v",
					pollMode, acc.NumQueueSubscriptions)
			}
			if acc.TotalSubscriptions != 4 {
				return fmt.Errorf("mode=%v - Expected total subs to be 4, got %v",
					pollMode, acc.TotalSubscriptions)
			}
		}
		return nil
	})

	// Check inbound on B now...
	checkFor(t, 2*time.Second, 15*time.Millisecond, func() error {
		for pollMode := 0; pollMode < 2; pollMode++ {
			g := pollGatewayz(t, sb, pollMode, gwURLServerB+"?gw_name=A&acc_name=acc_1", &GatewayzOptions{Name: "A", AccountName: "acc_1"})
			igs := g.InboundGateways["A"]
			if igs == nil {
				return fmt.Errorf("mode=%v - Expected inbound gateway from A, got none", pollMode)
			}
			if len(igs) != 1 {
				return fmt.Errorf("mode=%v - Expected single inbound, got %v", pollMode, len(igs))
			}
			ig := igs[0]
			if n := len(ig.Accounts); n != 1 {
				return fmt.Errorf("mode=%v - Expected to get 1 account, got %v", pollMode, n)
			}
			// Now account details
			acc := ig.Accounts[0]
			if acc.InterestMode != InterestOnly.String() {
				return fmt.Errorf("mode=%v - Expected interest-only mode, got %q", pollMode, acc.InterestMode)
			}
			if acc.InterestMode != InterestOnly.String() {
				return fmt.Errorf("Should be in %q mode, got %q", InterestOnly.String(), acc.InterestMode)
			}
			// Since we switched, this should be set to 0
			if acc.NoInterestCount != 0 {
				return fmt.Errorf("mode=%v - Expected 0 no-interest, got %v", pollMode, acc.NoInterestCount)
			}
			// Again, for inbound, these should be always 0.
			if acc.NumQueueSubscriptions != 0 || acc.TotalSubscriptions != 0 {
				return fmt.Errorf("mode=%v - For inbound connection, expected num queue subs and total subs to be 0, got %v and %v",
					pollMode, acc.TotalSubscriptions, acc.NumQueueSubscriptions)
			}
		}
		return nil
	})
}

func TestMonitorGatewayzWithSubs(t *testing.T) {
	resetPreviousHTTPConnections()

	ob := testDefaultOptionsForGateway("B")
	aA := NewAccount("A")
	aB := NewAccount("B")
	ob.Accounts = append(ob.Accounts, aA, aB)
	ob.Users = append(ob.Users,
		&User{Username: "a", Password: "a", Account: aA},
		&User{Username: "b", Password: "b", Account: aB})
	sb := runGatewayServer(ob)
	defer sb.Shutdown()

	oa := testGatewayOptionsFromToWithServers(t, "A", "B", sb)
	oa.HTTPHost = "127.0.0.1"
	oa.HTTPPort = MONITOR_PORT
	aA = NewAccount("A")
	aB = NewAccount("B")
	oa.Accounts = append(oa.Accounts, aA, aB)
	oa.Users = append(oa.Users,
		&User{Username: "a", Password: "a", Account: aA},
		&User{Username: "b", Password: "b", Account: aB})
	sa := runGatewayServer(oa)
	defer sa.Shutdown()

	waitForOutboundGateways(t, sa, 1, 2*time.Second)
	waitForInboundGateways(t, sa, 1, 2*time.Second)

	waitForOutboundGateways(t, sb, 1, 2*time.Second)
	waitForInboundGateways(t, sb, 1, 2*time.Second)

	ncA := natsConnect(t, sb.ClientURL(), nats.UserInfo("a", "a"))
	defer ncA.Close()
	natsSubSync(t, ncA, "foo")
	natsFlush(t, ncA)

	ncB := natsConnect(t, sb.ClientURL(), nats.UserInfo("b", "b"))
	defer ncB.Close()
	natsSubSync(t, ncB, "foo")
	natsQueueSubSync(t, ncB, "bar", "baz")
	natsFlush(t, ncB)

	checkGWInterestOnlyModeInterestOn(t, sa, "B", "A", "foo")
	checkGWInterestOnlyModeInterestOn(t, sa, "B", "B", "foo")
	checkForRegisteredQSubInterest(t, sa, "B", "B", "bar", 1, time.Second)

	for _, test := range []struct {
		url     string
		allAccs bool
		opts    *GatewayzOptions
	}{
		{"accs=1&subs=1", true, &GatewayzOptions{Accounts: true, AccountSubscriptions: true}},
		{"accs=1&subs=detail", true, &GatewayzOptions{Accounts: true, AccountSubscriptionsDetail: true}},
		{"acc_name=B&subs=1", false, &GatewayzOptions{AccountName: "B", AccountSubscriptions: true}},
		{"acc_name=B&subs=detail", false, &GatewayzOptions{AccountName: "B", AccountSubscriptionsDetail: true}},
	} {
		t.Run(test.url, func(t *testing.T) {
			gatewayzURL := fmt.Sprintf("http://127.0.0.1:%d/gatewayz?%s", sa.MonitorAddr().Port, test.url)
			for pollMode := 0; pollMode < 2; pollMode++ {
				gw := pollGatewayz(t, sa, pollMode, gatewayzURL, test.opts)
				require_Equal(t, len(gw.OutboundGateways), 1)
				ogw, ok := gw.OutboundGateways["B"]
				require_True(t, ok)
				require_NotNil(t, ogw)
				var expected int
				if test.allAccs {
					expected = 3 // A + B + $G
				} else {
					expected = 1 // B
				}
				require_Len(t, len(ogw.Accounts), expected)
				accs := map[string]*AccountGatewayz{}
				for _, a := range ogw.Accounts {
					// Do not include the global account there.
					if a.Name == globalAccountName {
						continue
					}
					accs[a.Name] = a
				}
				// Update the expected number of accounts if we asked for all accounts.
				if test.allAccs {
					expected--
				}
				// The account B should always be present.
				_, ok = accs["B"]
				require_True(t, ok)
				if expected == 2 {
					_, ok = accs["A"]
					require_True(t, ok)
				}
				// Now that we know we have the proper account(s), check the content.
				for n, a := range accs {
					require_NotNil(t, a)
					require_Equal(t, a.Name, n)
					totalSubs := 1
					var numQueueSubs int
					if n == "B" {
						totalSubs++
						numQueueSubs = 1
					}
					require_Equal(t, a.TotalSubscriptions, totalSubs)
					require_Equal(t, a.NumQueueSubscriptions, numQueueSubs)

					m := map[string]*SubDetail{}
					if test.opts.AccountSubscriptions {
						require_Len(t, len(a.Subs), totalSubs)
						require_Len(t, len(a.SubsDetail), 0)
						for _, sub := range a.Subs {
							m[sub] = nil
						}
					} else {
						require_Len(t, len(a.Subs), 0)
						require_Len(t, len(a.SubsDetail), totalSubs)
						for _, sub := range a.SubsDetail {
							m[sub.Subject] = &sub
						}
					}
					sd, ok := m["foo"]
					require_True(t, ok)
					if test.opts.AccountSubscriptionsDetail {
						require_NotNil(t, sd)
						require_Equal(t, sd.Queue, _EMPTY_)
					} else {
						require_True(t, sd == nil)
					}
					sd, ok = m["bar"]
					if numQueueSubs == 1 {
						require_True(t, ok)
						if test.opts.AccountSubscriptionsDetail {
							require_NotNil(t, sd)
							require_Equal(t, sd.Queue, "baz")
						} else {
							require_True(t, sd == nil)
						}
					} else {
						require_False(t, ok)
					}
				}
			}
		})
	}
}

func TestMonitorRoutezRTT(t *testing.T) {
	// Do not change default PingInterval and expect RTT to still be reported

	ob := DefaultOptions()
	sb := RunServer(ob)
	defer sb.Shutdown()

	oa := DefaultOptions()
	oa.Routes = RoutesFromStr(fmt.Sprintf("nats://%s:%d", ob.Cluster.Host, ob.Cluster.Port))
	sa := RunServer(oa)
	defer sa.Shutdown()

	checkClusterFormed(t, sa, sb)

	checkRouteInfo := func(t *testing.T, s *Server) {
		t.Helper()
		routezURL := fmt.Sprintf("http://127.0.0.1:%d/routez", s.MonitorAddr().Port)
		for pollMode := 0; pollMode < 2; pollMode++ {
			checkFor(t, 2*firstPingInterval, 15*time.Millisecond, func() error {
				rz := pollRoutez(t, s, pollMode, routezURL, nil)
				// Pool size + 1 for system account
				if len(rz.Routes) != DEFAULT_ROUTE_POOL_SIZE+1 {
					return fmt.Errorf("Expected %d route, got %v", DEFAULT_ROUTE_POOL_SIZE+1, len(rz.Routes))
				}
				for _, ri := range rz.Routes {
					if ri.RTT == _EMPTY_ {
						return fmt.Errorf("Route's RTT not reported")
					}
				}
				return nil
			})
		}
	}
	checkRouteInfo(t, sa)
	checkRouteInfo(t, sb)
}

func pollLeafz(t *testing.T, s *Server, mode int, url string, opts *LeafzOptions) *Leafz {
	t.Helper()
	if mode == 0 {
		l := &Leafz{}
		body := readBody(t, url)
		if err := json.Unmarshal(body, l); err != nil {
			t.Fatalf("Got an error unmarshalling the body: %v\n", err)
		}
		return l
	}
	l, err := s.Leafz(opts)
	if err != nil {
		t.Fatalf("Error on Leafz: %v", err)
	}
	return l
}

func TestMonitorOpJWT(t *testing.T) {
	content := `
	listen: "127.0.0.1:-1"
	http: "127.0.0.1:-1"
	operator = "../test/configs/nkeys/op.jwt"
	resolver = MEMORY
	`
	conf := createConfFile(t, []byte(content))
	sa, _ := RunServerWithConfig(conf)
	defer sa.Shutdown()

	theJWT, err := os.ReadFile("../test/configs/nkeys/op.jwt")
	require_NoError(t, err)
	theJWT = []byte(strings.Split(string(theJWT), "\n")[1])
	claim, err := jwt.DecodeOperatorClaims(string(theJWT))
	require_NoError(t, err)

	pollURL := fmt.Sprintf("http://127.0.0.1:%d/varz", sa.MonitorAddr().Port)
	for pollMode := 1; pollMode < 2; pollMode++ {
		l := pollVarz(t, sa, pollMode, pollURL, nil)

		if len(l.TrustedOperatorsJwt) != 1 {
			t.Fatalf("Expected one operator jwt")
		}
		if len(l.TrustedOperatorsClaim) != 1 {
			t.Fatalf("Expected one operator claim")
		}
		if l.TrustedOperatorsJwt[0] != string(theJWT) {
			t.Fatalf("Expected operator to be identical to configuration")
		}
		if !reflect.DeepEqual(l.TrustedOperatorsClaim[0], claim) {
			t.Fatal("claims need to be equal")
		}
	}
}

func TestMonitorLeafz(t *testing.T) {
	content := `
	server_name: "hub"
	listen: "127.0.0.1:-1"
	http: "127.0.0.1:-1"
	operator = "../test/configs/nkeys/op.jwt"
	resolver = MEMORY
	ping_interval = 1
	leafnodes {
		listen: "127.0.0.1:-1"
	}
	`
	conf := createConfFile(t, []byte(content))
	sb, ob := RunServerWithConfig(conf)
	defer sb.Shutdown()

	createAcc := func(t *testing.T) (*Account, string) {
		t.Helper()
		acc, akp := createAccount(sb)
		kp, _ := nkeys.CreateUser()
		pub, _ := kp.PublicKey()
		nuc := jwt.NewUserClaims(pub)
		ujwt, err := nuc.Encode(akp)
		if err != nil {
			t.Fatalf("Error generating user JWT: %v", err)
		}
		seed, _ := kp.Seed()
		creds := genCredsFile(t, ujwt, seed)
		return acc, creds
	}
	acc1, mycreds1 := createAcc(t)
	acc2, mycreds2 := createAcc(t)
	leafName := "my-leaf-node"

	content = `
		port: -1
		http: "127.0.0.1:-1"
		ping_interval = 1
		server_name: %s
		accounts {
			%s {
				users [
					{user: user1, password: pwd}
				]
			}
			%s {
				users [
					{user: user2, password: pwd}
				]
			}
		}
		leafnodes {
			isolate_leafnode_interest: true
			remotes = [
				{
					account: "%s"
					url: nats-leaf://127.0.0.1:%d
					credentials: '%s'
				}
				{
					account: "%s"
					url: nats-leaf://127.0.0.1:%d
					credentials: '%s'
				}
			]
		}
		`
	config := fmt.Sprintf(content,
		leafName,
		acc1.Name, acc2.Name,
		acc1.Name, ob.LeafNode.Port, mycreds1,
		acc2.Name, ob.LeafNode.Port, mycreds2)
	conf = createConfFile(t, []byte(config))
	sa, oa := RunServerWithConfig(conf)
	defer sa.Shutdown()

	checkFor(t, time.Second, 15*time.Millisecond, func() error {
		if n := sa.NumLeafNodes(); n != 2 {
			return fmt.Errorf("Expected 2 leaf connections, got %v", n)
		}
		return nil
	})

	// Wait for initial RTT to be computed
	time.Sleep(firstPingInterval + 500*time.Millisecond)

	ch := make(chan bool, 1)
	nc1B := natsConnect(t, fmt.Sprintf("nats://127.0.0.1:%d", ob.Port), nats.UserCredentials(mycreds1))
	defer nc1B.Close()
	natsSub(t, nc1B, "foo", func(_ *nats.Msg) { ch <- true })
	natsSub(t, nc1B, "bar", func(_ *nats.Msg) {})
	natsFlush(t, nc1B)

	nc2B := natsConnect(t, fmt.Sprintf("nats://127.0.0.1:%d", ob.Port), nats.UserCredentials(mycreds2))
	defer nc2B.Close()
	natsSub(t, nc2B, "bar", func(_ *nats.Msg) { ch <- true })
	natsSub(t, nc2B, "foo", func(_ *nats.Msg) {})
	natsFlush(t, nc2B)

	checkSubInterest(t, sa, acc1.GetName(), "foo", time.Second)
	checkSubInterest(t, sa, acc2.GetName(), "bar", time.Second)

	nc1A := natsConnect(t, fmt.Sprintf("nats://user1:pwd@127.0.0.1:%d", oa.Port))
	defer nc1A.Close()
	natsPub(t, nc1A, "foo", []byte("hello"))
	natsFlush(t, nc1A)

	waitCh(t, ch, "Did not get the message")

	nc2A := natsConnect(t, fmt.Sprintf("nats://user2:pwd@127.0.0.1:%d", oa.Port))
	defer nc2A.Close()
	natsPub(t, nc2A, "bar", []byte("hello"))
	natsPub(t, nc2A, "bar", []byte("hello"))
	natsFlush(t, nc2A)

	waitCh(t, ch, "Did not get the message")
	waitCh(t, ch, "Did not get the message")

	// Let's poll server A
	pollURL := fmt.Sprintf("http://127.0.0.1:%d/leafz?subs=1", sa.MonitorAddr().Port)
	for pollMode := 1; pollMode < 2; pollMode++ {
		l := pollLeafz(t, sa, pollMode, pollURL, &LeafzOptions{Subscriptions: true})
		if l.ID != sa.ID() {
			t.Fatalf("Expected ID to be %q, got %q", sa.ID(), l.ID)
		}
		if l.Now.IsZero() {
			t.Fatalf("Expected Now to be set, was not")
		}
		if l.NumLeafs != 2 {
			t.Fatalf("Expected NumLeafs to be 2, got %v", l.NumLeafs)
		}
		if len(l.Leafs) != 2 {
			t.Fatalf("Expected array to be len 2, got %v", len(l.Leafs))
		}
		for _, ln := range l.Leafs {
			if ln.Account == acc1.Name {
				if ln.OutMsgs != 1 || ln.OutBytes == 0 || ln.InMsgs != 0 || ln.InBytes != 0 {
					t.Fatalf("Expected 1 OutMsgs/Bytes and 0 InMsgs/Bytes, got %+v", ln)
				}
			} else if ln.Account == acc2.Name {
				if ln.OutMsgs != 2 || ln.OutBytes == 0 || ln.InMsgs != 0 || ln.InBytes != 0 {
					t.Fatalf("Expected 2 OutMsgs/Bytes and 0 InMsgs/Bytes, got %+v", ln)
				}
			} else {
				t.Fatalf("Expected account to be %q or %q, got %q", acc1.Name, acc2.Name, ln.Account)
			}
			if ln.Name != "hub" {
				t.Fatalf("Expected name to be %q, got %q", "hub", ln.Name)
			}
			if !ln.IsSpoke {
				t.Fatal("Expected leafnode connection to be spoke")
			}
			if !ln.IsIsolated {
				t.Fatal("Expected leafnode connection to be isolated")
			}
			if ln.RTT == "" {
				t.Fatalf("RTT not tracked?")
			}
			if ln.NumSubs != 3 {
				t.Fatalf("Expected 3 subs, got %v", ln.NumSubs)
			}
			if len(ln.Subs) != 3 {
				t.Fatalf("Expected subs to be returned, got %v", len(ln.Subs))
			}
			var foundFoo bool
			var foundBar bool
			for _, sub := range ln.Subs {
				if sub == "foo" {
					foundFoo = true
				} else if sub == "bar" {
					foundBar = true
				}
			}
			if !foundFoo {
				t.Fatal("Did not find subject foo")
			}
			if !foundBar {
				t.Fatal("Did not find subject bar")
			}
		}
	}
	// Make sure that if we don't ask for subs, we don't get them
	pollURL = fmt.Sprintf("http://127.0.0.1:%d/leafz", sa.MonitorAddr().Port)
	for pollMode := 1; pollMode < 2; pollMode++ {
		l := pollLeafz(t, sa, pollMode, pollURL, nil)
		for _, ln := range l.Leafs {
			if ln.NumSubs != 3 {
				t.Fatalf("Number of subs should be 3, got %v", ln.NumSubs)
			}
			if len(ln.Subs) != 0 {
				t.Fatalf("Subs should not have been returned, got %v", ln.Subs)
			}
		}
	}
	// Make sure that we can request per account - existing account
	pollURL = fmt.Sprintf("http://127.0.0.1:%d/leafz?acc=%s", sa.MonitorAddr().Port, acc1.Name)
	for pollMode := 1; pollMode < 2; pollMode++ {
		l := pollLeafz(t, sa, pollMode, pollURL, &LeafzOptions{Account: acc1.Name})
		for _, ln := range l.Leafs {
			if ln.Account != acc1.Name {
				t.Fatalf("Expected leaf node to be from account %s, got: %v", acc1.Name, ln)
			}
		}
		if len(l.Leafs) != 1 {
			t.Fatalf("Expected only two leaf node for this account, got: %v", len(l.Leafs))
		}
	}
	// Make sure that we can request per account - non existing account
	pollURL = fmt.Sprintf("http://127.0.0.1:%d/leafz?acc=%s", sa.MonitorAddr().Port, "DOESNOTEXIST")
	for pollMode := 1; pollMode < 2; pollMode++ {
		l := pollLeafz(t, sa, pollMode, pollURL, &LeafzOptions{Account: "DOESNOTEXIST"})
		if len(l.Leafs) != 0 {
			t.Fatalf("Expected no leaf node for this account, got: %v", len(l.Leafs))
		}
	}
	// Now polling server B.
	pollURL = fmt.Sprintf("http://127.0.0.1:%d/leafz?subs=1", sb.MonitorAddr().Port)
	for pollMode := 1; pollMode < 2; pollMode++ {
		l := pollLeafz(t, sb, pollMode, pollURL, &LeafzOptions{Subscriptions: true})
		if l.ID != sb.ID() {
			t.Fatalf("Expected ID to be %q, got %q", sb.ID(), l.ID)
		}
		if l.Now.IsZero() {
			t.Fatalf("Expected Now to be set, was not")
		}
		if l.NumLeafs != 2 {
			t.Fatalf("Expected NumLeafs to be 1, got %v", l.NumLeafs)
		}
		if len(l.Leafs) != 2 {
			t.Fatalf("Expected array to be len 2, got %v", len(l.Leafs))
		}
		for _, ln := range l.Leafs {
			if ln.Account == acc1.Name {
				if ln.OutMsgs != 0 || ln.OutBytes != 0 || ln.InMsgs != 1 || ln.InBytes == 0 {
					t.Fatalf("Expected 1 InMsgs/Bytes and 0 OutMsgs/Bytes, got %+v", ln)
				}
			} else if ln.Account == acc2.Name {
				if ln.OutMsgs != 0 || ln.OutBytes != 0 || ln.InMsgs != 2 || ln.InBytes == 0 {
					t.Fatalf("Expected 2 InMsgs/Bytes and 0 OutMsgs/Bytes, got %+v", ln)
				}
			} else {
				t.Fatalf("Expected account to be %q or %q, got %q", acc1.Name, acc2.Name, ln.Account)
			}
			if ln.Name != leafName {
				t.Fatalf("Expected name to be %q, got %q", leafName, ln.Name)
			}
			if ln.IsSpoke {
				t.Fatal("Expected leafnode connection to be hub")
			}
			if ln.RTT == "" {
				t.Fatalf("RTT not tracked?")
			}
			// LDS should be only one.
			if ln.NumSubs != 5 || len(ln.Subs) != 5 {
				t.Fatalf("Expected 5 subs, got %v (%v)", ln.NumSubs, ln.Subs)
			}
		}
	}
}

func pollAccountz(t *testing.T, s *Server, mode int, url string, opts *AccountzOptions) *Accountz {
	t.Helper()
	if mode == 0 {
		body := readBody(t, url)
		a := &Accountz{}
		if err := json.Unmarshal(body, a); err != nil {
			t.Fatalf("Got an error unmarshalling the body: %v\n", err)
		}
		return a
	}
	a, err := s.Accountz(opts)
	if err != nil {
		t.Fatalf("Error on Accountz(): %v", err)
	}
	return a
}

func pollAccountStatz(t *testing.T, s *Server, mode int, url string, opts *AccountStatzOptions) *AccountStatz {
	t.Helper()
	if mode == 0 {
		body := readBody(t, url)
		as := &AccountStatz{}
		if err := json.Unmarshal(body, as); err != nil {
			t.Fatalf("Got an error unmarshalling the body: %v\n", err)
		}
		return as
	}
	as, err := s.AccountStatz(opts)
	if err != nil {
		t.Fatalf("Error on AccountStatz(): %v", err)
	}
	return as
}

func TestMonitorAccountz(t *testing.T) {
	s := RunServer(DefaultMonitorOptions())
	defer s.Shutdown()

	for pollMode := 0; pollMode < 2; pollMode++ {
		a := pollAccountz(t, s, pollMode, fmt.Sprintf("http://127.0.0.1:%d%s", s.MonitorAddr().Port, AccountzPath), nil)

		require_Equal(t, a.ID, s.ID())
		require_Equal(t, len(a.Accounts), 2)
		for _, acc := range a.Accounts {
			switch acc {
			case DEFAULT_SYSTEM_ACCOUNT, DEFAULT_GLOBAL_ACCOUNT:
			default:
				t.Fatalf("Unexpected account: %s", acc)
			}
		}
		require_Equal(t, a.SystemAccount, DEFAULT_SYSTEM_ACCOUNT)
	}

	for pollMode := 0; pollMode < 2; pollMode++ {
		a := pollAccountz(t, s, pollMode, fmt.Sprintf("http://127.0.0.1:%d%s?acc=$SYS", s.MonitorAddr().Port, AccountzPath), &AccountzOptions{Account: DEFAULT_SYSTEM_ACCOUNT})

		require_NotNil(t, a.Account)
		require_Equal(t, a.Account.AccountName, DEFAULT_SYSTEM_ACCOUNT)
		require_Equal(t, a.Account.NameTag, DEFAULT_SYSTEM_ACCOUNT)
		require_True(t, a.Account.IsSystem)
		require_Equal(t, a.SystemAccount, DEFAULT_SYSTEM_ACCOUNT)
	}
}

func TestMonitorAccountStatz(t *testing.T) {
	s := RunServer(DefaultMonitorOptions())
	defer s.Shutdown()

	for pollMode := 0; pollMode < 2; pollMode++ {
		a := pollAccountStatz(t, s, pollMode, fmt.Sprintf("http://127.0.0.1:%d%s?unused=1", s.MonitorAddr().Port, AccountStatzPath), &AccountStatzOptions{IncludeUnused: true})

		require_Equal(t, a.ID, s.ID())
		require_Equal(t, len(a.Accounts), 2)

		// Check accounts.
		for _, acc := range a.Accounts {
			switch acc.Account {
			case DEFAULT_GLOBAL_ACCOUNT:
				require_Equal(t, acc.Name, DEFAULT_GLOBAL_ACCOUNT)
				require_Equal(t, acc.Conns, 0)
			case DEFAULT_SYSTEM_ACCOUNT:
				require_Equal(t, acc.Name, DEFAULT_SYSTEM_ACCOUNT)
				require_Equal(t, acc.Conns, 0)
			default:
				t.Fatalf("Unexpected account: %+v", acc)
			}
		}
	}
}

// https://github.com/nats-io/nats-server/issues/8251
func TestMonitorAccountStatzLeafNodes(t *testing.T) {
	hubConf := createConfFile(t, []byte(`
		server_name: "hub"
		listen: "127.0.0.1:-1"
		http: "127.0.0.1:-1"
		accounts {
			LEAF_ACC { users [{user: leaf, password: pwd}] }
			CLIENT_ACC { users [{user: client, password: pwd}] }
		}
		leafnodes {
			listen: "127.0.0.1:-1"
		}
	`))
	hub, hubOpts := RunServerWithConfig(hubConf)
	defer hub.Shutdown()

	leafConf := createConfFile(t, []byte(fmt.Sprintf(`
		server_name: "leaf"
		listen: "127.0.0.1:-1"
		leafnodes {
			remotes = [
				{url: "nats-leaf://leaf:pwd@127.0.0.1:%d"}
			]
		}
	`, hubOpts.LeafNode.Port)))
	leaf, _ := RunServerWithConfig(leafConf)
	defer leaf.Shutdown()

	checkLeafNodeConnected(t, hub)

	// Connect a client to CLIENT_ACC, it will show up in the reporting.
	nc := natsConnect(t, hub.ClientURL(), nats.UserInfo("client", "pwd"))
	defer nc.Close()

	checkAccounts := func(t *testing.T, stz *AccountStatz) {
		t.Helper()
		accounts := make(map[string]*AccountStat, len(stz.Accounts))
		for _, acc := range stz.Accounts {
			accounts[acc.Account] = acc
		}

		// The account with only a leaf node connection should be reported.
		leafAcc, ok := accounts["LEAF_ACC"]
		if !ok {
			t.Fatalf("Expected account LEAF_ACC to be present, got %+v", stz.Accounts)
		}
		require_Equal(t, leafAcc.Conns, 0)
		require_Equal(t, leafAcc.LeafNodes, 1)
		require_Equal(t, leafAcc.TotalConns, 1)

		// The account with a client connection should be reported.
		clientAcc, ok := accounts["CLIENT_ACC"]
		if !ok {
			t.Fatalf("Expected account CLIENT_ACC to be present, got %+v", stz.Accounts)
		}
		require_Equal(t, clientAcc.Conns, 1)
		require_Equal(t, clientAcc.LeafNodes, 0)
		require_Equal(t, clientAcc.TotalConns, 1)

		// Unused accounts should still be excluded.
		if _, ok := accounts[DEFAULT_GLOBAL_ACCOUNT]; ok {
			t.Fatalf("Did not expect unused account %q to be present, got %+v", DEFAULT_GLOBAL_ACCOUNT, stz.Accounts)
		}
	}

	// Check without unused=true and without account filtering.
	for pollMode := 0; pollMode < 2; pollMode++ {
		stz := pollAccountStatz(t, hub, pollMode, fmt.Sprintf("http://127.0.0.1:%d%s", hub.MonitorAddr().Port, AccountStatzPath), &AccountStatzOptions{})
		checkAccounts(t, stz)
	}

	// Check with explicit account filtering. Only available through AccountStatz() directly.
	stz, err := hub.AccountStatz(&AccountStatzOptions{Accounts: []string{"LEAF_ACC", "CLIENT_ACC", DEFAULT_GLOBAL_ACCOUNT}})
	require_NoError(t, err)
	checkAccounts(t, stz)
}

func runMonitorServerWithOperator(t *testing.T, sysName, accName string) ([]*Server, nkeys.KeyPair, nkeys.KeyPair) {
	t.Helper()

	resetPreviousHTTPConnections()

	sysKp, sysPub := createKey(t)
	sysClaim := jwt.NewAccountClaims(sysPub)
	sysClaim.Name = sysName
	sysJwt := encodeClaim(t, sysClaim, sysPub)

	accKp, accPub := createKey(t)
	accClaim := jwt.NewAccountClaims(accPub)
	accClaim.Name = accName
	accClaim.Limits.JetStreamLimits.DiskStorage = -1
	accClaim.Limits.JetStreamLimits.MemoryStorage = -1

	accJwt := encodeClaim(t, accClaim, accPub)

	var servers []*Server

	// Main cluster
	for i, test := range []struct {
		port     int
		mport    int
		cport    int
		route1   int
		gport    int
		gateway1 int
		lport    int
	}{
		{7500, 7501, 7502, 5502, 8500, 8503, 7433},
		{5500, 5501, 5502, 7502, 8501, 8503, 7434},
		{6050, 6051, 6052, 7502, 8502, 8503, 7435},
	} {
		dir := t.TempDir()
		conf := createConfFile(t, []byte(fmt.Sprintf(`
			listen: 127.0.0.1:%d
			http: 127.0.0.1:%d
			jetstream: {
				max_mem_store: 10Mb
				max_file_store: 10Mb
				store_dir: '%s'
			}
			cluster {
				name: c1
				listen: %d
				routes: [
					nats-route://127.0.0.1:%d,
				]
			}
			gateway {
				name: c1
				port: %d
				gateways: [
					{name: c2, urls: [nats://127.0.0.1:%d]},
				]
			}
			leafnodes {
				listen: %d
			}
			server_name: %s
			operator: %s
			resolver: MEMORY
			system_account: %s
			resolver_preload {
				%s : %s
				%s : %s
			}
		`, test.port, test.mport, dir, test.cport, test.route1, test.gport, test.gateway1, test.lport, fmt.Sprintf("n%d", i), ojwt, sysPub, accPub, accJwt, sysPub, sysJwt)))

		s, _ := RunServerWithConfig(conf)
		servers = append(servers, s)
	}

	// Gateway
	for i, test := range []struct {
		port     int
		mport    int
		cport    int
		gport    int
		gateway1 int
	}{
		{7503, 7504, 6053, 8503, 8500},
	} {
		conf := createConfFile(t, []byte(fmt.Sprintf(`
			listen: 127.0.0.1:%d
			http: 127.0.0.1:%d
			cluster {
				name: c2
				listen: %d
			}
			gateway {
				name: c2
				port: %d
				gateways: [
					{name: c1, urls: [nats://127.0.0.1:%d]},
				]
			}
			server_name: %s
			operator: %s
			resolver: MEMORY
			system_account: %s
			resolver_preload {
				%s : %s
				%s : %s
			}
		`, test.port, test.mport, test.cport, test.gport, test.gateway1, fmt.Sprintf("n%d", i+3), ojwt, sysPub, accPub, accJwt, sysPub, sysJwt)))

		s, _ := RunServerWithConfig(conf)
		servers = append(servers, s)
	}

	_, credsFile := createUser(t, accKp)

	// Leafnode
	for _, test := range []struct {
		port  int
		mport int
		lport int
	}{
		{7505, 7506, 7433},
	} {
		conf := createConfFile(t, []byte(fmt.Sprintf(`
			listen: 127.0.0.1:%d
			http: 127.0.0.1:%d
			leafnodes: {
				remotes: [
					{url: "nats://127.0.0.1:%d", credentials: "%s", account: "APP"},
				]
			}
			accounts {
				SYS: {
					users: [
						{user: "sys", password: "sys"},
					]
				}
				APP: {
					users: [
						{user: "app", password: "app"},
					]
				}
			}
			no_auth_user: "app"
		`, test.port, test.mport, test.lport, credsFile)))

		s, _ := RunServerWithConfig(conf)
		servers = append(servers, s)
	}

	checkForJSClusterUp(t, servers[:3]...)
	waitForOutboundGateways(t, servers[0], 1, 2*time.Second)
	waitForOutboundGateways(t, servers[3], 1, 2*time.Second)

	return servers, sysKp, accKp
}

func createUser(t *testing.T, accKp nkeys.KeyPair) (string, string) {
	t.Helper()

	ukp, _ := nkeys.CreateUser()
	seed, _ := ukp.Seed()
	upub, _ := ukp.PublicKey()
	uclaim := newJWTTestUserClaims()
	uclaim.Subject = upub
	ujwt, err := uclaim.Encode(accKp)
	require_NoError(t, err)
	return upub, genCredsFile(t, ujwt, seed)
}

func TestMonitorAccountzOperatorMode(t *testing.T) {
	sysName := "SYS"
	accName := "APP"

	srvs, sysKp, accKp := runMonitorServerWithOperator(t, sysName, accName)
	for _, s := range srvs {
		defer s.Shutdown()
	}
	s := srvs[0]

	sysPub, _ := sysKp.PublicKey()
	accPub, _ := accKp.PublicKey()

	_, aCreds := createUser(t, accKp)

	nc, err := nats.Connect(s.ClientURL(), nats.UserCredentials(aCreds))
	require_NoError(t, err)
	defer nc.Close()

	for pollMode := 0; pollMode < 2; pollMode++ {
		a := pollAccountz(t, s, pollMode, fmt.Sprintf("http://127.0.0.1:%d%s", s.MonitorAddr().Port, AccountzPath), nil)

		require_Equal(t, a.ID, s.ID())
		require_Equal(t, len(a.Accounts), 3)
		for _, acc := range a.Accounts {
			switch acc {
			case sysPub, accPub, DEFAULT_GLOBAL_ACCOUNT:
			default:
				t.Fatalf("Unexpected account: %s", acc)
			}
		}
		require_Equal(t, a.SystemAccount, sysPub)
	}

	for pollMode := 0; pollMode < 2; pollMode++ {
		a := pollAccountz(t, s, pollMode, fmt.Sprintf("http://127.0.0.1:%d%s?acc=%s", s.MonitorAddr().Port, AccountzPath, sysPub), &AccountzOptions{Account: sysPub})

		require_NotNil(t, a.Account)
		require_Equal(t, a.Account.AccountName, sysPub)
		require_Equal(t, a.Account.NameTag, sysName)
		require_True(t, a.Account.IsSystem)
		require_Equal(t, a.SystemAccount, sysPub)
	}

}

func TestMonitorAccountStatzOperatorMode(t *testing.T) {
	sysName := "SYS"
	accName := "APP"

	srvs, sysKp, accKp := runMonitorServerWithOperator(t, sysName, accName)
	for _, s := range srvs {
		defer s.Shutdown()
	}
	s := srvs[0]

	sysPub, _ := sysKp.PublicKey()
	accPub, _ := accKp.PublicKey()

	_, aCreds := createUser(t, accKp)

	nc, err := nats.Connect(s.ClientURL(), nats.UserCredentials(aCreds))
	require_NoError(t, err)
	defer nc.Close()

	for pollMode := 0; pollMode < 2; pollMode++ {
		a := pollAccountStatz(t, s, pollMode, fmt.Sprintf("http://127.0.0.1:%d%s?unused=1", s.MonitorAddr().Port, AccountStatzPath), &AccountStatzOptions{IncludeUnused: true})

		require_Equal(t, a.ID, s.ID())
		require_Equal(t, len(a.Accounts), 3)

		// Check accounts.
		for _, acc := range a.Accounts {
			switch acc.Account {
			case accPub:
				require_Equal(t, acc.Name, accName)
				require_True(t, acc.NumSubs > 0)
				require_Equal(t, acc.Conns, 1)
			case sysPub:
				require_Equal(t, acc.Name, sysName)
				require_Equal(t, acc.Conns, 0)
			case DEFAULT_GLOBAL_ACCOUNT:
				require_Equal(t, acc.Name, DEFAULT_GLOBAL_ACCOUNT)
			default:
				t.Fatalf("Unexpected account: %+v", acc)
			}
		}
	}
}

func TestMonitorAccountStatzDataStatsOperatorMode(t *testing.T) {
	sysName := "SYS"
	accName := "APP"

	srvs, _, accKp := runMonitorServerWithOperator(t, sysName, accName)
	for _, s := range srvs {
		defer s.Shutdown()
	}
	// First three servers are the cluster.
	n0 := srvs[0]
	n1 := srvs[1]

	// Gateway server.
	n3 := srvs[3]

	// Leafnode server.
	n4 := srvs[4]

	accPub, _ := accKp.PublicKey()

	_, aCreds := createUser(t, accKp)

	n0c, err := nats.Connect(n0.ClientURL(), nats.UserCredentials(aCreds))
	require_NoError(t, err)
	defer n0c.Close()

	n1c, err := nats.Connect(n1.ClientURL(), nats.UserCredentials(aCreds))
	require_NoError(t, err)
	defer n1c.Close()

	n3c, err := nats.Connect(n3.ClientURL(), nats.UserCredentials(aCreds))
	require_NoError(t, err)
	defer n3c.Close()

	// No auth user for leafnode.
	n4c, err := nats.Connect(n4.ClientURL())
	require_NoError(t, err)
	defer n4c.Close()

	// Subscription over a route.
	_, err = n1c.Subscribe("foo", func(m *nats.Msg) {})
	require_NoError(t, err)

	// Subscription over a gateway.
	_, err = n3c.Subscribe("bar", func(m *nats.Msg) {})
	require_NoError(t, err)

	// Subscription over a leafnode.
	_, err = n4c.Subscribe("baz", func(m *nats.Msg) {})
	require_NoError(t, err)

	// Subscription propagation.
	time.Sleep(10 * time.Millisecond)

	err = n0c.Publish("foo", []byte("Hello"))
	require_NoError(t, err)

	err = n0c.Publish("bar", []byte("Hello"))
	require_NoError(t, err)

	err = n0c.Publish("baz", []byte("Hello"))
	require_NoError(t, err)

	// Publish propagation.
	time.Sleep(10 * time.Millisecond)

	for pollMode := 0; pollMode < 2; pollMode++ {
		for _, s := range srvs {
			a := pollAccountStatz(t, s, pollMode, fmt.Sprintf("http://127.0.0.1:%d%s?unused=1", s.MonitorAddr().Port, AccountStatzPath), &AccountStatzOptions{IncludeUnused: true})

			for _, acc := range a.Accounts {
				if acc.Account != accPub {
					continue
				}

				switch s.Name() {
				case "n0":
					// Should have received three messages due to the three publishes.
					// Should have sent one over a route to n1 for foo.
					// Should have sent one over a gateway to n3 for bar.
					// Should have sent one over a leaf node to n4 for baz.
					require_Equal(t, acc.Sent.Msgs, 3)
					require_Equal(t, acc.Sent.Routes.Msgs, 1)
					require_Equal(t, acc.Sent.Gateways.Msgs, 1)
					require_Equal(t, acc.Sent.Leafs.Msgs, 1)
					require_Equal(t, acc.Received.Msgs, 3)
				case "n1":
					// Should have sent 1 message to a client.
					// Should have received 1 message from n0.
					require_Equal(t, acc.Sent.Msgs, 1)
					require_Equal(t, acc.Sent.Routes.Msgs, 0)
					require_Equal(t, acc.Received.Msgs, 1)
					require_Equal(t, acc.Received.Routes.Msgs, 1)
				case "n2":
					// Should have not received anything.
					require_Equal(t, acc.Sent.Msgs, 0)
					require_Equal(t, acc.Sent.Bytes, 0)
				// Gateway, connected to n0
				case "n3":
					// Should have received 1 message from n0.
					// Should have sent 1 message to a client.
					require_Equal(t, acc.Sent.Msgs, 1)
					require_Equal(t, acc.Sent.Gateways.Msgs, 0)
					require_Equal(t, acc.Received.Msgs, 1)
					require_Equal(t, acc.Received.Gateways.Msgs, 1)
				// Leafnode, connected to n0
				case "n4":
					// Should have received 1 message from n0.
					// Should have sent 1 message to a client.
					require_Equal(t, acc.Sent.Msgs, 1)
					require_Equal(t, acc.Received.Msgs, 1)
					require_Equal(t, acc.Received.Leafs.Msgs, 1)
				}
			}
		}
	}
}

func TestMonitorAccountzAccountIssuerUpdate(t *testing.T) {
	// create an operator set of keys
	okp, err := nkeys.CreateOperator()
	require_NoError(t, err)
	opk, err := okp.PublicKey()
	require_NoError(t, err)

	// create the system account
	_, sysPK := createKey(t)
	sysAc := jwt.NewAccountClaims(sysPK)
	sysAc.Name = "SYS"
	sysJwt, err := sysAc.Encode(okp)
	require_NoError(t, err)

	// create the operator with the system
	oc := jwt.NewOperatorClaims(opk)
	oc.Name = "O"
	// add a signing keys
	osk1, err := nkeys.CreateOperator()
	require_NoError(t, err)
	opk1, err := osk1.PublicKey()
	require_NoError(t, err)
	// add a second signing key
	osk2, err := nkeys.CreateOperator()
	require_NoError(t, err)
	opk2, err := osk2.PublicKey()
	require_NoError(t, err)
	oc.SigningKeys.Add(opk1, opk2)
	// set the system account
	oc.SystemAccount = sysPK
	// generate
	oJWT, err := oc.Encode(okp)
	require_NoError(t, err)

	// create an account
	akp, apk := createKey(t)
	ac := jwt.NewAccountClaims(apk)
	ac.Name = "A"
	// sign with the signing key
	aJWT, err := ac.Encode(osk1)
	require_NoError(t, err)

	// build the mem-resolver
	conf := createConfFile(t, []byte(fmt.Sprintf(`
		listen: 127.0.0.1:-1
		http: 127.0.0.1:-1
		operator = %s
		resolver = MEMORY
		system_account: %s
		resolver_preload = {
			%s : %s
			%s : %s
		}
	`, oJWT, sysPK, sysPK, sysJwt, apk, aJWT)))

	// start the server
	s, _ := RunServerWithConfig(conf)
	defer s.Shutdown()

	// create an user for account A, or we don't see
	// the account in accountsz
	createUser := func() (string, string) {
		ukp, _ := nkeys.CreateUser()
		seed, _ := ukp.Seed()
		upub, _ := ukp.PublicKey()
		uclaim := newJWTTestUserClaims()
		uclaim.Subject = upub
		ujwt, err := uclaim.Encode(akp)
		require_NoError(t, err)
		return upub, genCredsFile(t, ujwt, seed)
	}

	_, aCreds := createUser()
	// connect the user
	nc, err := nats.Connect(s.ClientURL(), nats.UserCredentials(aCreds))
	require_NoError(t, err)
	defer nc.Close()

	// lookup the account
	data := readBody(t, fmt.Sprintf("http://127.0.0.1:%d%s?acc=%s", s.MonitorAddr().Port, AccountzPath, apk))
	var ci Accountz
	require_NoError(t, json.Unmarshal(data, &ci))
	require_Equal(t, ci.Account.IssuerKey, opk1)

	// now update the account
	aJWT, err = ac.Encode(osk2)
	require_NoError(t, err)

	updatedConf := []byte(fmt.Sprintf(`
		listen: 127.0.0.1:-1
		http: 127.0.0.1:-1
		operator = %s
		resolver = MEMORY
		system_account: %s
		resolver_preload = {
			%s : %s
			%s : %s
		}
	`, oJWT, sysPK, sysPK, sysJwt, apk, aJWT))
	// update the configuration file
	require_NoError(t, os.WriteFile(conf, updatedConf, 0666))
	// reload
	require_NoError(t, s.Reload())

	data = readBody(t, fmt.Sprintf("http://127.0.0.1:%d%s?acc=%s", s.MonitorAddr().Port, AccountzPath, apk))
	require_NoError(t, json.Unmarshal(data, &ci))
	require_Equal(t, ci.Account.IssuerKey, opk2)
}

func TestMonitorAuthorizedUsers(t *testing.T) {
	kp, _ := nkeys.FromSeed(seed)
	usrNKey, _ := kp.PublicKey()
	opts := DefaultMonitorOptions()
	opts.Nkeys = []*NkeyUser{{Nkey: string(usrNKey)}}
	opts.Users = []*User{{Username: "user", Password: "pwd"}}
	s := RunServer(opts)
	defer s.Shutdown()

	checkAuthUser := func(expected string) {
		t.Helper()
		resetPreviousHTTPConnections()
		url := fmt.Sprintf("http://127.0.0.1:%d/connz?auth=true", s.MonitorAddr().Port)
		for mode := 0; mode < 2; mode++ {
			connz := pollConnz(t, s, mode, url, &ConnzOptions{Username: true})
			if l := len(connz.Conns); l != 1 {
				t.Fatalf("Expected 1, got %v", l)
			}
			conn := connz.Conns[0]
			au := conn.AuthorizedUser
			if au == _EMPTY_ {
				t.Fatal("AuthorizedUser is empty!")
			}
			if au != expected {
				t.Fatalf("Expected %q, got %q", expected, au)
			}
		}
	}

	c := natsConnect(t, fmt.Sprintf("nats://user:pwd@127.0.0.1:%d", opts.Port))
	defer c.Close()
	checkAuthUser("user")
	c.Close()

	c = natsConnect(t, fmt.Sprintf("nats://127.0.0.1:%d", opts.Port),
		nats.Nkey(usrNKey, func(nonce []byte) ([]byte, error) {
			return kp.Sign(nonce)
		}))
	defer c.Close()
	// we should get the user's NKey
	checkAuthUser(usrNKey)
	c.Close()

	s.Shutdown()
	opts = DefaultMonitorOptions()
	opts.Authorization = "sometoken"
	s = RunServer(opts)
	defer s.Shutdown()

	c = natsConnect(t, fmt.Sprintf("nats://127.0.0.1:%d", opts.Port),
		nats.Token("sometoken"))
	defer c.Close()
	// We should get the token specified by the user
	checkAuthUser("[REDACTED]")
	c.Close()
	s.Shutdown()

	opts = DefaultMonitorOptions()
	// User an operator seed
	kp, _ = nkeys.FromSeed(oSeed)
	pub, _ := kp.PublicKey()
	opts.TrustedKeys = []string{pub}
	s = RunServer(opts)
	defer s.Shutdown()

	akp, _ := nkeys.CreateAccount()
	apub, _ := akp.PublicKey()
	nac := jwt.NewAccountClaims(apub)
	ajwt, err := nac.Encode(oKp)
	if err != nil {
		t.Fatalf("Error generating account JWT: %v", err)
	}

	nkp, _ := nkeys.CreateUser()
	upub, _ := nkp.PublicKey()
	nuc := jwt.NewUserClaims(upub)
	jwt, err := nuc.Encode(akp)
	if err != nil {
		t.Fatalf("Error generating user JWT: %v", err)
	}

	buildMemAccResolver(s)
	addAccountToMemResolver(s, apub, ajwt)

	c = natsConnect(t, fmt.Sprintf("nats://127.0.0.1:%d", opts.Port),
		nats.UserJWT(
			func() (string, error) { return jwt, nil },
			func(nonce []byte) ([]byte, error) { return nkp.Sign(nonce) }))
	defer c.Close()
	// we should get the user's pubkey
	checkAuthUser(upub)
}

func testMonitorConnzJWTVisibility(t *testing.T, bearer bool) {
	t.Helper()

	opts := DefaultMonitorOptions()
	kp, _ := nkeys.FromSeed(oSeed)
	pub, _ := kp.PublicKey()
	opts.TrustedKeys = []string{pub}
	s := RunServer(opts)
	defer s.Shutdown()

	akp, _ := nkeys.CreateAccount()
	apub, _ := akp.PublicKey()
	nac := jwt.NewAccountClaims(apub)
	ajwt, err := nac.Encode(oKp)
	require_NoError(t, err)

	nkp, _ := nkeys.CreateUser()
	upub, _ := nkp.PublicKey()
	nuc := jwt.NewUserClaims(upub)
	nuc.BearerToken = bearer
	userJWT, err := nuc.Encode(akp)
	require_NoError(t, err)

	buildMemAccResolver(s)
	addAccountToMemResolver(s, apub, ajwt)

	conn, err := net.Dial("tcp", s.Addr().String())
	require_NoError(t, err)
	defer func() {
		if conn != nil {
			conn.Close()
		}
	}()
	require_NoError(t, conn.SetDeadline(time.Now().Add(5*time.Second)))
	cr := bufio.NewReaderSize(conn, maxBufSize)
	info, err := cr.ReadString('\n')
	require_NoError(t, err)

	var sigField string
	if !bearer {
		var ni nonceInfo
		require_NoError(t, json.Unmarshal([]byte(info[5:]), &ni))
		sigraw, err := nkp.Sign([]byte(ni.Nonce))
		require_NoError(t, err)
		sig := base64.RawURLEncoding.EncodeToString(sigraw)
		sigField = fmt.Sprintf(",\"sig\":\"%s\"", sig)
	}
	cs := fmt.Sprintf("CONNECT {\"jwt\":%q,\"verbose\":true,\"pedantic\":true%s}\r\n", userJWT, sigField)
	_, err = conn.Write([]byte(cs))
	require_NoError(t, err)
	l, err := cr.ReadString('\n')
	require_NoError(t, err)
	if !strings.HasPrefix(l, "+OK") {
		t.Fatalf("Expected +OK, got %q", l)
	}

	expectedJWT := userJWT
	if bearer {
		expectedJWT = _EMPTY_
	}

	checkConnzJWT := func(url, expectedJWT string) {
		t.Helper()
		var connz *Connz
		checkFor(t, 2*time.Second, 15*time.Millisecond, func() error {
			connz = pollConnz(t, s, 0, url, nil)
			if len(connz.Conns) != 1 {
				return fmt.Errorf("expected 1 connection, got %d", len(connz.Conns))
			}
			return nil
		})
		ci := connz.Conns[0]
		require_Equal(t, ci.JWT, expectedJWT)
	}

	baseURL := fmt.Sprintf("http://127.0.0.1:%d/connz", s.MonitorAddr().Port)
	checkConnzJWT(baseURL+"?auth=1", expectedJWT)

	require_NoError(t, conn.Close())
	conn = nil

	checkConnzJWT(baseURL+"?state=closed", expectedJWT)
	checkConnzJWT(baseURL+"?auth=1&state=closed", expectedJWT)
}

func TestMonitorConnzJWTVisibility(t *testing.T) {
	for _, test := range []struct {
		name   string
		bearer bool
	}{
		{name: "bearer", bearer: true},
		{name: "non-bearer", bearer: false},
	} {
		t.Run(test.name, func(t *testing.T) {
			testMonitorConnzJWTVisibility(t, test.bearer)
		})
	}
}

// Helper function to check that a JS cluster is formed
func checkForJSClusterUp(t *testing.T, servers ...*Server) {
	t.Helper()
	// We will use the other JetStream helpers here.
	c := &cluster{t: t, servers: servers}
	c.checkClusterFormed()
	c.waitOnClusterReady()
}

func pollJsz(t *testing.T, s *Server, mode int, url string, opts *JSzOptions) *JSInfo {
	t.Helper()

	if mode == 0 {
		body := readBody(t, url)
		info := &JSInfo{}
		err := json.Unmarshal(body, info)
		require_NoError(t, err)
		return info
	}

	info, err := s.Jsz(opts)
	require_NoError(t, err)
	return info
}

func TestMonitorJszNonJszServer(t *testing.T) {
	srv := RunServer(DefaultOptions())
	defer srv.Shutdown()

	if !srv.ReadyForConnections(5 * time.Second) {
		t.Fatalf("server did not become ready")
	}

	jsi, err := srv.Jsz(&JSzOptions{})
	if err != nil {
		t.Fatalf("jsi failed: %v", err)
	}
	if jsi.ID != srv.ID() {
		t.Fatalf("did not receive valid info")
	}

	jsi, err = srv.Jsz(&JSzOptions{LeaderOnly: true})
	if !errors.Is(err, errSkipZreq) {
		t.Fatalf("expected a skip z req error: %v", err)
	}
	if jsi != nil {
		t.Fatalf("expected no jsi: %v", jsi)
	}
}

func TestMonitorJsz(t *testing.T) {
	readJsInfo := func(url string) *JSInfo {
		t.Helper()
		body := readBody(t, url)
		info := &JSInfo{}
		err := json.Unmarshal(body, info)
		require_NoError(t, err)
		return info
	}
	srvs := []*Server{}
	for _, test := range []struct {
		port   int
		mport  int
		cport  int
		routed int
	}{
		{7500, 7501, 7502, 5502},
		{5500, 5501, 5502, 7502},
	} {
		s, _ := runMonitorJSServer(t, test.port, test.mport, test.cport, test.routed)
		defer s.Shutdown()
		srvs = append(srvs, s)
	}
	checkClusterFormed(t, srvs...)
	checkForJSClusterUp(t, srvs...)

	nc := natsConnect(t, "nats://usr:pwd@127.0.0.1:7500")
	defer nc.Close()
	js, err := nc.JetStream(nats.MaxWait(5 * time.Second))
	require_NoError(t, err)
	_, err = js.AddStream(&nats.StreamConfig{
		Name:     "my-stream-replicated",
		Subjects: []string{"foo", "bar"},
		Replicas: 2,
	})
	require_NoError(t, err)
	_, err = js.AddStream(&nats.StreamConfig{
		Name:     "my-stream-non-replicated",
		Subjects: []string{"baz"},
		Replicas: 1,
	})
	require_NoError(t, err)
	_, err = js.AddStream(&nats.StreamConfig{
		Name:     "my-stream-mirror",
		Replicas: 2,
		Mirror: &nats.StreamSource{
			Name: "my-stream-replicated",
		},
	})
	require_NoError(t, err)
	_, err = js.AddConsumer("my-stream-replicated", &nats.ConsumerConfig{
		Durable:   "my-consumer-replicated",
		AckPolicy: nats.AckExplicitPolicy,
	})
	require_NoError(t, err)
	_, err = js.AddConsumer("my-stream-non-replicated", &nats.ConsumerConfig{
		Durable:   "my-consumer-non-replicated",
		AckPolicy: nats.AckExplicitPolicy,
	})
	require_NoError(t, err)
	_, err = js.AddConsumer("my-stream-mirror", &nats.ConsumerConfig{
		Durable:   "my-consumer-mirror",
		AckPolicy: nats.AckExplicitPolicy,
	})
	require_NoError(t, err)
	nc.Flush()
	_, err = js.Publish("foo", nil)
	require_NoError(t, err)
	// Wait for mirror replication
	time.Sleep(200 * time.Millisecond)

	monUrl1 := fmt.Sprintf("http://127.0.0.1:%d/jsz", 7501)
	monUrl2 := fmt.Sprintf("http://127.0.0.1:%d/jsz", 5501)

	t.Run("default", func(t *testing.T) {
		for _, url := range []string{monUrl1, monUrl2} {
			info := readJsInfo(url)
			if len(info.AccountDetails) != 0 {
				t.Fatalf("expected no account to be returned by %s but got %v", url, info)
			}
			if info.Streams == 0 {
				t.Fatalf("expected stream count to be 3 but got %d", info.Streams)
			}
			if info.Consumers == 0 {
				t.Fatalf("expected consumer count to be 3 but got %d", info.Consumers)
			}
			if info.Messages != 2 {
				t.Fatalf("expected two message but got %d", info.Messages)
			}
			if info.Limits.MaxHAAssets != 1000 {
				t.Fatalf("expected max_ha_assets limit to be 1000 got %v", info.Limits)
			}
			if info.Total != 2 {
				t.Fatalf("expected total to be 2 but got %d", info.Total)
			}
		}
	})
	t.Run("accounts", func(t *testing.T) {
		for _, url := range []string{monUrl1, monUrl2} {
			info := readJsInfo(url + "?accounts=true")

			require_Equal(t, len(info.AccountDetails), 2)

			for _, acc := range info.AccountDetails {
				switch acc.Id {
				case "ACC":
					require_Equal(t, acc.Name, "ACC")
				case "BCC_TO_HAVE_ONE_EXTRA":
					require_Equal(t, acc.Name, "BCC_TO_HAVE_ONE_EXTRA")
				default:
					t.Fatalf("Unexpected account: %s", acc.Name)
				}
			}
		}
	})
	t.Run("accounts reserved metrics", func(t *testing.T) {
		for _, url := range []string{monUrl1, monUrl2} {
			info := readJsInfo(url + "?accounts=true&acc=ACC")
			if len(info.AccountDetails) != 1 {
				t.Fatalf("expected single account")
			}
			acc := info.AccountDetails[0]
			got := int(acc.ReservedMemory)
			expected := 5242880
			if got != expected {
				t.Errorf("Expected: %v, got: %v", expected, got)
			}
			got = int(acc.ReservedStore)
			expected = 4194304
			if got != expected {
				t.Errorf("Expected: %v, got: %v", expected, got)
			}

			info = readJsInfo(url + "?accounts=true&acc=BCC_TO_HAVE_ONE_EXTRA")
			if len(info.AccountDetails) != 1 {
				t.Fatalf("expected single account")
			}
			acc = info.AccountDetails[0]
			got = int(acc.ReservedMemory)
			expected = -1
			if got != expected {
				t.Errorf("Expected: %v, got: %v", expected, got)
			}
			got = int(acc.ReservedStore)
			expected = -1
			if got != expected {
				t.Errorf("Expected: %v, got: %v", expected, got)
			}
		}
	})
	t.Run("offset-too-big", func(t *testing.T) {
		for _, url := range []string{monUrl1, monUrl2} {
			info := readJsInfo(url + "?accounts=true&offset=10")
			if len(info.AccountDetails) != 0 {
				t.Fatalf("expected no accounts to be returned by %s but got %v", url, info)
			}
		}
	})
	t.Run("limit", func(t *testing.T) {
		for _, url := range []string{monUrl1, monUrl2} {
			info := readJsInfo(url + "?accounts=true&limit=1")
			if len(info.AccountDetails) != 1 {
				t.Fatalf("expected one account to be returned by %s but got %v", url, info)
			}
			if info := readJsInfo(url + "?accounts=true&offset=1&limit=1"); len(info.AccountDetails) != 1 {
				t.Fatalf("expected one account to be returned by %s but got %v", url, info)
			}
		}
	})
	t.Run("offset-stable", func(t *testing.T) {
		for _, url := range []string{monUrl1, monUrl2} {
			info1 := readJsInfo(url + "?accounts=true&offset=0&limit=1")
			if len(info1.AccountDetails) != 1 {
				t.Fatalf("expected one account to be returned by %s but got %v", url, info1)
			}
			info2 := readJsInfo(url + "?accounts=true&offset=0&limit=1")
			if len(info2.AccountDetails) != 1 {
				t.Fatalf("expected one account to be returned by %s but got %v", url, info2)
			}
			if info1.AccountDetails[0].Name != info2.AccountDetails[0].Name {
				t.Fatalf("absent changes, same offset should result in same account but got: %v %v",
					info1.AccountDetails[0].Name, info2.AccountDetails[0].Name)
			}

			info1 = readJsInfo(url + "?accounts=true&offset=1&limit=1")
			if len(info1.AccountDetails) != 1 {
				t.Fatalf("expected one account to be returned by %s but got %v", url, info1)
			}
			info2 = readJsInfo(url + "?accounts=true&offset=1&limit=1")
			if len(info2.AccountDetails) != 1 {
				t.Fatalf("expected one account to be returned by %s but got %v", url, info2)
			}
			if info1.AccountDetails[0].Name != info2.AccountDetails[0].Name {
				t.Fatalf("absent changes, same offset should result in same account but gut: %v %v",
					info1.AccountDetails[0].Name, info2.AccountDetails[0].Name)
			}
		}
	})
	t.Run("filter-account", func(t *testing.T) {
		for _, url := range []string{monUrl1, monUrl2} {
			info := readJsInfo(url + "?acc=ACC")
			if len(info.AccountDetails) != 1 {
				t.Fatalf("expected account ACC to be returned by %s but got %v", url, info)
			}
			if info.AccountDetails[0].Name != "ACC" {
				t.Fatalf("expected account ACC to be returned by %s but got %v", url, info)
			}
			if len(info.AccountDetails[0].Streams) != 0 {
				t.Fatalf("expected account ACC to be returned by %s but got %v", url, info)
			}
		}
	})
	t.Run("streams", func(t *testing.T) {
		for _, url := range []string{monUrl1, monUrl2} {
			info := readJsInfo(url + "?acc=ACC&streams=true")
			if len(info.AccountDetails) != 1 {
				t.Fatalf("expected account ACC to be returned by %s but got %v", url, info)
			}
			if len(info.AccountDetails[0].Streams) == 0 {
				t.Fatalf("expected streams to be returned by %s but got %v", url, info)
			}
			if len(info.AccountDetails[0].Streams[0].Consumer) != 0 {
				t.Fatalf("expected no consumers to be returned by %s but got %v", url, info)
			}
		}
	})
	t.Run("stream-leader-only", func(t *testing.T) {
		// First server
		info := readJsInfo(monUrl1 + "?streams=true&stream-leader-only=1")
		for _, a := range info.AccountDetails {
			for _, s := range a.Streams {
				if s.Cluster.Leader != srvs[0].serverName() {
					t.Fatalf("expected stream leader to be %s but got %s", srvs[0].serverName(), s.Cluster.Leader)
				}
			}
		}
		info = readJsInfo(monUrl2 + "?streams=true&stream-leader-only=1")
		for _, a := range info.AccountDetails {
			for _, s := range a.Streams {
				if s.Cluster.Leader != srvs[1].serverName() {
					t.Fatalf("expected stream leader to be %s but got %s", srvs[0].serverName(), s.Cluster.Leader)
				}
			}
		}
	})
	t.Run("consumers", func(t *testing.T) {
		for _, url := range []string{monUrl1, monUrl2} {
			info := readJsInfo(url + "?acc=ACC&consumers=true")
			if len(info.AccountDetails) != 1 {
				t.Fatalf("expected account ACC to be returned by %s but got %v", url, info)
			}
			if len(info.AccountDetails[0].Streams[0].Consumer) == 0 {
				t.Fatalf("expected consumers to be returned by %s but got %v", url, info)
			}
			if info.AccountDetails[0].Streams[0].Config != nil {
				t.Fatal("Config expected to not be present")
			}
			if info.AccountDetails[0].Streams[0].Consumer[0].Config != nil {
				t.Fatal("Config expected to not be present")
			}
			if len(info.AccountDetails[0].Streams[0].ConsumerRaftGroups) != 0 {
				t.Fatalf("expected consumer raft groups to not be returned by %s but got %v", url, info)
			}
		}
	})
	t.Run("direct-consumers", func(t *testing.T) {
		// It could take time for the sourcing to set up.
		checkFor(t, 5*time.Second, 250*time.Millisecond, func() error {
			for _, url := range []string{monUrl1, monUrl2} {
				info := readJsInfo(url + "?acc=ACC&consumers=true&direct-consumers=true")
				if len(info.AccountDetails) != 1 {
					t.Fatalf("expected account ACC to be returned by %s but got %v", url, info)
				}
				for _, stream := range info.AccountDetails[0].Streams {
					if len(stream.DirectConsumer) == 0 {
						continue
					}
					if stream.Name != "my-stream-replicated" {
						t.Fatalf("expected direct consumers only on the mirror origin stream, %s reported %d on %q",
							url, len(stream.DirectConsumer), stream.Name)
					}
					if len(stream.DirectConsumer) != 1 {
						t.Fatalf("expected exactly one direct consumer on %q but %s returned %d",
							stream.Name, url, len(stream.DirectConsumer))
					}
					for _, dc := range stream.DirectConsumer {
						if slices.ContainsFunc(stream.Consumer, func(c *ConsumerInfo) bool {
							return c.Name == dc.Name
						}) {
							t.Fatalf("public consumer %q leaked into the direct consumer list of %q on %s",
								dc.Name, stream.Name, url)
						}
					}
					return nil
				}
			}
			return fmt.Errorf("expected direct consumer info to be present on one of the servers")
		})
	})
	t.Run("config", func(t *testing.T) {
		for _, url := range []string{monUrl1, monUrl2} {
			info := readJsInfo(url + "?acc=ACC&consumers=true&config=true")
			if len(info.AccountDetails) != 1 {
				t.Fatalf("expected account ACC to be returned by %s but got %v", url, info)
			}
			if info.AccountDetails[0].Streams[0].Config == nil {
				t.Fatal("Config expected to be present")
			}
			if info.AccountDetails[0].Streams[0].Consumer[0].Config == nil {
				t.Fatal("Config expected to be present")
			}
		}
	})
	t.Run("replication", func(t *testing.T) {
		// The replication lag may only be present on the leader
		replicationFound := false
		for _, url := range []string{monUrl1, monUrl2} {
			info := readJsInfo(url + "?acc=ACC&streams=true")
			if len(info.AccountDetails) != 1 {
				t.Fatalf("expected account ACC to be returned by %s but got %v", url, info)
			}
			streamFound := false
			for _, stream := range info.AccountDetails[0].Streams {
				if stream.Name == "my-stream-mirror" {
					streamFound = true
					if stream.Mirror != nil {
						replicationFound = true
					}
				}
			}
			if !streamFound {
				t.Fatalf("Did not locate my-stream-mirror stream in results")
			}
		}
		if !replicationFound {
			t.Fatal("ReplicationLag expected to be present for my-stream-mirror stream")
		}
	})
	t.Run("cluster-info", func(t *testing.T) {
		for _, url := range []string{monUrl1, monUrl2} {
			info := readJsInfo(url + "")
			if info.Meta.Peer != getHash(info.Meta.Leader) {
				t.Fatalf("Invalid Peer: %+v", info.Meta)
			}
			// Replicas are populated on every server, not just the meta leader.
			if len(info.Meta.Replicas) == 0 {
				t.Fatalf("Expected replicas to be populated: %+v", info.Meta)
			}
			for _, r := range info.Meta.Replicas {
				if r.Peer == _EMPTY_ {
					t.Fatalf("Replicas' Peer is empty: %+v", r)
				}
			}
		}
	})
	t.Run("meta-snapshot-stats", func(t *testing.T) {
		for _, url := range []string{monUrl1, monUrl2} {
			info := readJsInfo(url)
			require_True(t, info.Meta != nil)
			require_True(t, info.Meta.Snapshot != nil)

			snapshot := info.Meta.Snapshot

			// In case no snapshots have happened there would be some pending entries.
			if snapshot.LastTime.IsZero() {
				require_True(t, snapshot.PendingEntries >= 1)
				require_True(t, snapshot.PendingSize >= 1)
			}

			// Force meta snapshots on both servers to test snapshot timing.
			for _, srv := range srvs {
				if js := srv.getJetStream(); js != nil {
					if mg := js.getMetaGroup(); mg != nil {
						if snap, _, _, err := js.metaSnapshot(); err == nil {
							mg.InstallSnapshot(snap, false)
						}
					}
				}
			}
			// Wait for snapshot timing to be recorded
			time.Sleep(100 * time.Millisecond)

			// Get latest stats again.
			info = readJsInfo(url)
			require_True(t, info.Meta != nil)
			require_True(t, info.Meta.Snapshot != nil)

			snapshot = info.Meta.Snapshot

			require_True(t, !snapshot.LastTime.IsZero())
			// Assert that snapshot time is in UTC
			require_Equal(t, snapshot.LastTime.Location(), time.UTC)

			// Assert that duration is non-negative and reasonable
			require_True(t, snapshot.LastDuration >= 0)
			require_True(t, snapshot.LastDuration < 30*time.Second)

			// Assert that snapshot time is recent.
			require_True(t, time.Since(snapshot.LastTime) < 5*time.Minute)
		}
	})
	t.Run("account-non-existing", func(t *testing.T) {
		for _, url := range []string{monUrl1, monUrl2} {
			info := readJsInfo(url + "?acc=DOES_NOT_EXIST")
			if len(info.AccountDetails) != 0 {
				t.Fatalf("expected no account to be returned by %s but got %v", url, info)
			}
		}
	})
	t.Run("account-non-existing-with-stream-details", func(t *testing.T) {
		for _, url := range []string{monUrl1, monUrl2} {
			info := readJsInfo(url + "?acc=DOES_NOT_EXIST&streams=true")
			if len(info.AccountDetails) != 0 {
				t.Fatalf("expected no account to be returned by %s but got %v", url, info)
			}
		}
	})
	t.Run("unique-tag-exists", func(t *testing.T) {
		for _, url := range []string{monUrl1, monUrl2} {
			info := readJsInfo(url)
			if len(info.Config.UniqueTag) == 0 {
				t.Fatalf("expected unique_tag to be returned by %s but got %v", url, info)
			}
		}
	})
	t.Run("raftgroups", func(t *testing.T) {
		for _, url := range []string{monUrl1, monUrl2} {
			info := readJsInfo(url + "?acc=ACC&consumers=true&raft=true")
			if len(info.AccountDetails) != 1 {
				t.Fatalf("expected account ACC to be returned by %s but got %v", url, info)
			}

			// We will have two streams and order is not guaranteed. So grab the one we want.
			var si StreamDetail
			if info.AccountDetails[0].Streams[0].Name == "my-stream-replicated" {
				si = info.AccountDetails[0].Streams[0]
			} else {
				si = info.AccountDetails[0].Streams[1]
			}

			if len(si.Consumer) == 0 {
				t.Fatalf("expected consumers to be returned by %s but got %v", url, info)
			}
			if len(si.ConsumerRaftGroups) == 0 {
				t.Fatalf("expected consumer raft groups to be returned by %s but got %v", url, info)
			}
			if len(si.RaftGroup) == 0 {
				t.Fatal("expected stream raft group info to be included")
			}
			crgroup := si.ConsumerRaftGroups[0]
			if crgroup.Name != "my-consumer-replicated" && crgroup.Name != "my-consumer-mirror" {
				t.Fatalf("expected consumer name to be included in raft group info, got: %v", crgroup.Name)
			}
			if len(crgroup.RaftGroup) == 0 {
				t.Fatal("expected consumer raft group info to be included")
			}
			require_True(t, si.Cluster.SystemAcc)
			require_Equal(t, si.Cluster.TrafficAcc, "SYS")
		}
	})
	t.Run("js-api-level", func(t *testing.T) {
		for _, url := range []string{monUrl1, monUrl2} {
			info := readJsInfo(url)
			require_Equal(t, info.API.Level, JSApiLevel)
		}
	})
}

func TestMonitorJszOperatorMode(t *testing.T) {
	sysName := "SYS"
	accName := "APP"

	srvs, _, accKp := runMonitorServerWithOperator(t, sysName, accName)
	for _, s := range srvs {
		defer s.Shutdown()
	}
	s := srvs[0]

	accPub, _ := accKp.PublicKey()

	_, aCreds := createUser(t, accKp)

	nc, err := nats.Connect(s.ClientURL(), nats.UserCredentials(aCreds))
	require_NoError(t, err)
	defer nc.Close()

	// Create a stream so the APP account shows up in Jsz.
	js, err := nc.JetStream(nats.MaxWait(5 * time.Second))
	require_NoError(t, err)
	_, err = js.AddStream(&nats.StreamConfig{
		Name:     "my-stream",
		Subjects: []string{"foo", "bar"},
		MaxBytes: 1024,
	})
	require_NoError(t, err)

	for pollMode := 0; pollMode < 2; pollMode++ {
		a := pollJsz(t, s, pollMode, fmt.Sprintf("http://127.0.0.1:%d%s?accounts=1", s.MonitorAddr().Port, JszPath), &JSzOptions{Accounts: true})

		require_Equal(t, a.ID, s.ID())
		require_Equal(t, len(a.AccountDetails), 1)

		// Check accounts.
		d := a.AccountDetails[0]
		require_Equal(t, d.Id, accPub)
		require_Equal(t, d.Name, accName)
	}
}

func TestMonitorReloadTLSConfig(t *testing.T) {
	template := `
		listen: "127.0.0.1:-1"
		https: "127.0.0.1:-1"
		tls {
			cert_file: '%s'
			key_file: '%s'
			ca_file: '../test/configs/certs/ca.pem'

			# Set this to make sure that it does not impact secure monitoring
			# (which it did, see issue: https://github.com/nats-io/nats-server/issues/2980)
			verify_and_map: true
		}
	`
	conf := createConfFile(t, []byte(fmt.Sprintf(template,
		"../test/configs/certs/server-noip.pem",
		"../test/configs/certs/server-key-noip.pem")))

	s, _ := RunServerWithConfig(conf)
	defer s.Shutdown()

	addr := fmt.Sprintf("127.0.0.1:%d", s.MonitorAddr().Port)
	c, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatalf("Error creating ws connection: %v", err)
	}
	defer c.Close()

	tc := &TLSConfigOpts{CaFile: "../test/configs/certs/ca.pem"}
	tlsConfig, err := GenTLSConfig(tc)
	if err != nil {
		t.Fatalf("Error generating TLS config: %v", err)
	}
	tlsConfig.ServerName = "127.0.0.1"
	tlsConfig.RootCAs = tlsConfig.ClientCAs
	tlsConfig.ClientCAs = nil
	c = tls.Client(c, tlsConfig.Clone())
	if err := c.(*tls.Conn).Handshake(); err == nil || !strings.Contains(err.Error(), "SAN") {
		t.Fatalf("Unexpected error: %v", err)
	}
	c.Close()

	reloadUpdateConfig(t, s, conf, fmt.Sprintf(template,
		"../test/configs/certs/server-cert.pem",
		"../test/configs/certs/server-key.pem"))

	c, err = net.Dial("tcp", addr)
	if err != nil {
		t.Fatalf("Error creating ws connection: %v", err)
	}
	defer c.Close()

	c = tls.Client(c, tlsConfig.Clone())
	if err := c.(*tls.Conn).Handshake(); err != nil {
		t.Fatalf("Error on TLS handshake: %v", err)
	}

	// Need to read something to see if there is a problem with the certificate or not.
	var buf [64]byte
	c.SetReadDeadline(time.Now().Add(250 * time.Millisecond))
	_, err = c.Read(buf[:])
	if ne, ok := err.(net.Error); !ok || !ne.Timeout() {
		t.Fatalf("Error: %v", err)
	}
}

func TestMonitorMQTT(t *testing.T) {
	o := DefaultOptions()
	o.HTTPHost = "127.0.0.1"
	o.HTTPPort = -1
	o.ServerName = "mqtt_server"
	o.Users = []*User{{Username: "someuser"}}
	pinnedCerts := make(PinnedCertSet)
	pinnedCerts["7f83b1657ff1fc53b92dc18148a1d65dfc2d4b1fa3d677284addd200126d9069"] = struct{}{}
	o.MQTT = MQTTOpts{
		Host:           "127.0.0.1",
		Port:           -1,
		NoAuthUser:     "someuser",
		JsDomain:       "js",
		AuthTimeout:    2.0,
		TLSMap:         true,
		TLSTimeout:     3.0,
		TLSPinnedCerts: pinnedCerts,
		AckWait:        4 * time.Second,
		MaxAckPending:  256,
	}
	s := RunServer(o)
	defer s.Shutdown()

	expected := &MQTTOptsVarz{
		Host:           "127.0.0.1",
		Port:           o.MQTT.Port,
		NoAuthUser:     "someuser",
		JsDomain:       "js",
		AuthTimeout:    2.0,
		TLSMap:         true,
		TLSTimeout:     3.0,
		TLSPinnedCerts: []string{"7f83b1657ff1fc53b92dc18148a1d65dfc2d4b1fa3d677284addd200126d9069"},
		AckWait:        4 * time.Second,
		MaxAckPending:  256,
	}
	url := fmt.Sprintf("http://127.0.0.1:%d/varz", s.MonitorAddr().Port)
	for mode := 0; mode < 2; mode++ {
		v := pollVarz(t, s, mode, url, nil)
		vm := &v.MQTT
		if !reflect.DeepEqual(vm, expected) {
			t.Fatalf("Expected\n%+v\nGot:\n%+v", expected, vm)
		}
	}
}

func TestMonitorWebsocket(t *testing.T) {
	o := DefaultOptions()
	o.HTTPHost = "127.0.0.1"
	o.HTTPPort = -1
	kp, _ := nkeys.FromSeed(oSeed)
	pub, _ := kp.PublicKey()
	o.TrustedKeys = []string{pub}
	o.Users = []*User{{Username: "someuser"}}
	pinnedCerts := make(PinnedCertSet)
	pinnedCerts["7f83b1657ff1fc53b92dc18148a1d65dfc2d4b1fa3d677284addd200126d9069"] = struct{}{}
	o.Websocket = WebsocketOpts{
		Host:             "127.0.0.1",
		Port:             -1,
		Advertise:        "somehost:8080",
		NoAuthUser:       "someuser",
		JWTCookie:        "somecookiename",
		AuthTimeout:      2.0,
		NoTLS:            true,
		TLSMap:           true,
		TLSPinnedCerts:   pinnedCerts,
		SameOrigin:       true,
		AllowedOrigins:   []string{"https://origin1", "https://origin2"},
		Compression:      true,
		HandshakeTimeout: 4 * time.Second,
	}
	s := RunServer(o)
	defer s.Shutdown()

	expected := &WebsocketOptsVarz{
		Host:             "127.0.0.1",
		Port:             o.Websocket.Port,
		Advertise:        "somehost:8080",
		NoAuthUser:       "someuser",
		JWTCookie:        "somecookiename",
		AuthTimeout:      2.0,
		NoTLS:            true,
		TLSMap:           true,
		TLSPinnedCerts:   []string{"7f83b1657ff1fc53b92dc18148a1d65dfc2d4b1fa3d677284addd200126d9069"},
		SameOrigin:       true,
		AllowedOrigins:   []string{"https://origin1", "https://origin2"},
		Compression:      true,
		HandshakeTimeout: 4 * time.Second,
	}
	url := fmt.Sprintf("http://127.0.0.1:%d/varz", s.MonitorAddr().Port)
	for mode := 0; mode < 2; mode++ {
		v := pollVarz(t, s, mode, url, nil)
		vw := &v.Websocket
		if !reflect.DeepEqual(vw, expected) {
			t.Fatalf("Expected\n%+v\nGot:\n%+v", expected, vw)
		}
	}
}

func TestMonitorServerIDZRequest(t *testing.T) {
	conf := createConfFile(t, []byte(`
		listen: 127.0.0.1:-1
		server_name: TEST22
		# For access to system account.
		accounts { $SYS { users = [ { user: "admin", pass: "s3cr3t!" } ] } }
	`))
	defer removeFile(t, conf)

	s, _ := RunServerWithConfig(conf)
	defer s.Shutdown()

	nc, err := nats.Connect(s.ClientURL(), nats.UserInfo("admin", "s3cr3t!"))
	require_NoError(t, err)
	defer nc.Close()

	subject := fmt.Sprintf(serverPingReqSubj, "IDZ")
	resp, err := nc.Request(subject, nil, time.Second)
	require_NoError(t, err)

	var sid ServerID
	err = json.Unmarshal(resp.Data, &sid)
	require_NoError(t, err)

	require_True(t, sid.Name == "TEST22")
	require_True(t, strings.HasPrefix(sid.ID, "N"))
}

func TestMonitorProfilez(t *testing.T) {
	s := RunServer(DefaultOptions())
	defer s.Shutdown()

	// Then start profiling.
	s.StartProfiler()

	// Now check that all of the profiles that we expect are
	// returning instead of erroring.
	for _, try := range []*ProfilezOptions{
		{Name: "allocs", Debug: 0},
		{Name: "allocs", Debug: 1},
		{Name: "block", Debug: 0},
		{Name: "goroutine", Debug: 0},
		{Name: "goroutine", Debug: 1},
		{Name: "goroutine", Debug: 2},
		{Name: "heap", Debug: 0},
		{Name: "heap", Debug: 1},
		{Name: "mutex", Debug: 0},
		{Name: "threadcreate", Debug: 0},
	} {
		if ps := s.profilez(try); ps.Error != _EMPTY_ {
			t.Fatalf("Unexpected error on %v: %s", try, ps.Error)
		}
	}
}

func TestMonitorRoutezPoolSize(t *testing.T) {
	conf1 := createConfFile(t, []byte(`
		port: -1
		http: -1
		cluster {
			port: -1
			name: "local"
			pool_size: 5
		}
		no_sys_acc: true
	`))
	defer removeFile(t, conf1)
	s1, o1 := RunServerWithConfig(conf1)
	defer s1.Shutdown()

	conf23 := createConfFile(t, []byte(fmt.Sprintf(`
		port: -1
		http: -1
		cluster {
			port: -1
			name: "local"
			routes: ["nats://127.0.0.1:%d"]
			pool_size: 5
		}
		no_sys_acc: true
	`, o1.Cluster.Port)))
	defer removeFile(t, conf23)

	s2, _ := RunServerWithConfig(conf23)
	defer s2.Shutdown()
	s3, _ := RunServerWithConfig(conf23)
	defer s3.Shutdown()

	checkClusterFormed(t, s1, s2, s3)

	for i, s := range []*Server{s1, s2, s3} {
		url := fmt.Sprintf("http://127.0.0.1:%d/varz", s.MonitorAddr().Port)
		for mode := 0; mode < 2; mode++ {
			v := pollVarz(t, s, mode, url, nil)
			if v.Cluster.PoolSize != 5 {
				t.Fatalf("Expected Cluster.PoolSize==5, got %v", v.Cluster.PoolSize)
			}
			if v.Remotes != 2 {
				t.Fatalf("Expected Remotes==2, got %v", v.Remotes)
			}
			if v.Routes != 10 {
				t.Fatalf("Expected NumRoutes==10, got %v", v.Routes)
			}
		}

		url = fmt.Sprintf("http://127.0.0.1:%d/routez", s.MonitorAddr().Port)
		for mode := 0; mode < 2; mode++ {
			v := pollRoutez(t, s, mode, url, nil)
			if v.NumRoutes != 10 {
				t.Fatalf("Expected NumRoutes==10, got %v", v.NumRoutes)
			}
			if n := len(v.Routes); n != 10 {
				t.Fatalf("Expected len(Routes)==10, got %v", n)
			}
			remotes := make(map[string]int)
			for _, r := range v.Routes {
				remotes[r.RemoteID]++
			}
			if n := len(remotes); n != 2 {
				t.Fatalf("Expected routes for 2 different servers, got %v", n)
			}
			switch i {
			case 0:
				if n := remotes[s2.ID()]; n != 5 {
					t.Fatalf("Expected 5 routes from S1 to S2, got %v", n)
				}
				if n := remotes[s3.ID()]; n != 5 {
					t.Fatalf("Expected 5 routes from S1 to S3, got %v", n)
				}
			case 1:
				if n := remotes[s1.ID()]; n != 5 {
					t.Fatalf("Expected 5 routes from S2 to S1, got %v", n)
				}
				if n := remotes[s3.ID()]; n != 5 {
					t.Fatalf("Expected 5 routes from S2 to S3, got %v", n)
				}
			case 2:
				if n := remotes[s1.ID()]; n != 5 {
					t.Fatalf("Expected 5 routes from S3 to S1, got %v", n)
				}
				if n := remotes[s2.ID()]; n != 5 {
					t.Fatalf("Expected 5 routes from S3 to S2, got %v", n)
				}
			}
		}
	}
}

func TestMonitorRoutezPerAccount(t *testing.T) {
	conf1 := createConfFile(t, []byte(`
		port: -1
		http: -1
		accounts {
			A { users: [{user: "a", password: "pwd"}] }
		}
		cluster {
			port: -1
			name: "local"
			accounts: ["A"]
		}
	`))
	defer removeFile(t, conf1)
	s1, o1 := RunServerWithConfig(conf1)
	defer s1.Shutdown()

	conf23 := createConfFile(t, []byte(fmt.Sprintf(`
		port: -1
		http: -1
		accounts {
			A { users: [{user: "a", password: "pwd"}] }
		}
		cluster {
			port: -1
			name: "local"
			routes: ["nats://127.0.0.1:%d"]
			accounts: ["A"]
		}
	`, o1.Cluster.Port)))
	defer removeFile(t, conf23)

	s2, _ := RunServerWithConfig(conf23)
	defer s2.Shutdown()
	s3, _ := RunServerWithConfig(conf23)
	defer s3.Shutdown()

	checkClusterFormed(t, s1, s2, s3)

	for _, s := range []*Server{s1, s2, s3} {
		// Default pool size + account "A" + system account (added by default)
		enr := 2 * (DEFAULT_ROUTE_POOL_SIZE + 1 + 1)
		url := fmt.Sprintf("http://127.0.0.1:%d/varz", s.MonitorAddr().Port)
		for mode := 0; mode < 2; mode++ {
			v := pollVarz(t, s, mode, url, nil)
			if v.Remotes != 2 {
				t.Fatalf("Expected Remotes==2, got %v", v.Remotes)
			}
			if v.Routes != enr {
				t.Fatalf("Expected NumRoutes==%d, got %v", enr, v.Routes)
			}
		}

		url = fmt.Sprintf("http://127.0.0.1:%d/routez", s.MonitorAddr().Port)
		for mode := 0; mode < 2; mode++ {
			v := pollRoutez(t, s, mode, url, nil)
			if v.NumRoutes != enr {
				t.Fatalf("Expected NumRoutes==%d, got %v", enr, v.NumRoutes)
			}
			if n := len(v.Routes); n != enr {
				t.Fatalf("Expected len(Routes)==%d, got %v", enr, n)
			}
			remotes := make(map[string]int)
			for _, r := range v.Routes {
				var acc int
				if r.Account == "A" {
					acc = 1
				}
				remotes[r.RemoteID] += acc
			}
			if n := len(remotes); n != 2 {
				t.Fatalf("Expected routes for 2 different servers, got %v", n)
			}
			for remoteID, v := range remotes {
				if v != 1 {
					t.Fatalf("Expected one and only one connection for account A for remote %q, got %v", remoteID, v)
				}
			}
		}
	}
}

func TestMonitorConnzOperatorAccountNames(t *testing.T) {
	sysName := "SYS"
	accName := "APP"

	srvs, _, accKp := runMonitorServerWithOperator(t, sysName, accName)
	for _, s := range srvs {
		defer s.Shutdown()
	}
	s := srvs[0]

	accPub, _ := accKp.PublicKey()

	// Now create 2 users.
	_, creds := createUser(t, accKp)

	nc, err := nats.Connect(s.ClientURL(), nats.UserCredentials(creds))
	require_NoError(t, err)
	defer nc.Close()

	for pollMode := 0; pollMode < 2; pollMode++ {
		url := fmt.Sprintf("http://127.0.0.1:%d/connz?auth=1", s.MonitorAddr().Port)
		connz := pollConnz(t, s, pollMode, url, &ConnzOptions{Username: true})
		require_Equal(t, connz.NumConns, 2)
		idx := slices.IndexFunc(connz.Conns, func(c *ConnInfo) bool {
			return c.Kind == kindStringMap[CLIENT]
		})
		ci := connz.Conns[idx]
		require_Equal(t, ci.Account, accPub)
		require_Equal(t, ci.NameTag, accName)
	}
}

func TestMonitorConnzOperatorModeFilterByUser(t *testing.T) {
	sysName := "SYS"
	accName := "APP"

	srvs, _, accKp := runMonitorServerWithOperator(t, sysName, accName)
	for _, s := range srvs {
		defer s.Shutdown()
	}
	s := srvs[0]

	// Now create 2 users.
	aUser, aCreds := createUser(t, accKp)
	bUser, bCreds := createUser(t, accKp)

	var users []*nats.Conn

	// Create 2 for A
	for i := 0; i < 2; i++ {
		nc, err := nats.Connect(s.ClientURL(), nats.UserCredentials(aCreds))
		require_NoError(t, err)
		defer nc.Close()
		users = append(users, nc)
	}
	// Create 5 for B
	for i := 0; i < 5; i++ {
		nc, err := nats.Connect(s.ClientURL(), nats.UserCredentials(bCreds))
		require_NoError(t, err)
		defer nc.Close()
		users = append(users, nc)
	}

	// Test A
	connz := pollConnz(t, s, 1, _EMPTY_, &ConnzOptions{User: aUser, Username: true})
	require_True(t, connz.NumConns == 2)
	for _, ci := range connz.Conns {
		require_True(t, ci.AuthorizedUser == aUser)
	}
	// Test B
	connz = pollConnz(t, s, 1, _EMPTY_, &ConnzOptions{User: bUser, Username: true})
	require_True(t, connz.NumConns == 5)
	for _, ci := range connz.Conns {
		require_True(t, ci.AuthorizedUser == bUser)
	}

	// Make sure URL access is the same.
	url := fmt.Sprintf("http://127.0.0.1:%d/", s.MonitorAddr().Port)
	urlFull := url + fmt.Sprintf("connz?auth=true&user=%s", aUser)
	connz = pollConnz(t, s, 0, urlFull, nil)
	require_True(t, connz.NumConns == 2)
	for _, ci := range connz.Conns {
		require_True(t, ci.AuthorizedUser == aUser)
	}

	// Now test closed filtering as well.
	for _, nc := range users {
		nc.Close()
	}
	// Let them process and be moved to closed ring buffer in server.
	time.Sleep(100 * time.Millisecond)

	connz = pollConnz(t, s, 1, _EMPTY_, &ConnzOptions{User: aUser, Username: true, State: ConnClosed})
	require_True(t, connz.NumConns == 2)
	for _, ci := range connz.Conns {
		require_True(t, ci.AuthorizedUser == aUser)
	}
}

func TestMonitorConnzSortByRTT(t *testing.T) {
	s := runMonitorServer()
	defer s.Shutdown()

	for i := 0; i < 10; i++ {
		nc, err := nats.Connect(s.ClientURL())
		require_NoError(t, err)
		defer nc.Close()
	}

	connz := pollConnz(t, s, 1, _EMPTY_, &ConnzOptions{Sort: ByRTT})
	require_True(t, connz.NumConns == 10)

	var rtt int64
	for _, ci := range connz.Conns {
		if rtt == 0 {
			rtt = ci.rtt
		} else {
			if ci.rtt > rtt {
				t.Fatalf("RTT not in descending order: %v vs %v",
					time.Duration(rtt), time.Duration(ci.rtt))
			}
			rtt = ci.rtt
		}
	}

	// Make sure url works as well.
	url := fmt.Sprintf("http://127.0.0.1:%d/connz?sort=rtt", s.MonitorAddr().Port)
	connz = pollConnz(t, s, 0, url, nil)
	require_True(t, connz.NumConns == 10)

	rtt = 0
	for _, ci := range connz.Conns {
		crttd, err := time.ParseDuration(ci.RTT)
		require_NoError(t, err)
		crtt := int64(crttd)
		if rtt == 0 {
			rtt = crtt
		} else {
			if crtt > rtt {
				t.Fatalf("RTT not in descending order: %v vs %v",
					time.Duration(rtt), time.Duration(crtt))
			}
			rtt = ci.rtt
		}
	}
}

func TestMonitorConnzIncludesLeafnodes(t *testing.T) {
	content := `
		server_name: "hub"
		listen: "127.0.0.1:-1"
		http: "127.0.0.1:-1"
		operator = "../test/configs/nkeys/op.jwt"
		resolver = MEMORY
		ping_interval = 1
		leafnodes {
			listen: "127.0.0.1:-1"
		}
	`
	conf := createConfFile(t, []byte(content))
	sb, ob := RunServerWithConfig(conf)
	defer sb.Shutdown()

	createAcc := func(t *testing.T) (*Account, string) {
		t.Helper()
		acc, akp := createAccount(sb)
		kp, _ := nkeys.CreateUser()
		pub, _ := kp.PublicKey()
		nuc := jwt.NewUserClaims(pub)
		ujwt, err := nuc.Encode(akp)
		if err != nil {
			t.Fatalf("Error generating user JWT: %v", err)
		}
		seed, _ := kp.Seed()
		creds := genCredsFile(t, ujwt, seed)
		return acc, creds
	}
	acc, mycreds := createAcc(t)
	leafName := "my-leaf-node"

	content = `
		port: -1
		http: "127.0.0.1:-1"
		ping_interval = 1
		server_name: %s
		accounts {
			%s {
				users [
					{user: user1, password: pwd}
				]
			}
		}
		leafnodes {
			remotes = [
				{
					account: "%s"
					url: nats-leaf://127.0.0.1:%d
					credentials: '%s'
				}
			]
		}
		`
	config := fmt.Sprintf(content,
		leafName,
		acc.Name,
		acc.Name, ob.LeafNode.Port, mycreds)
	conf = createConfFile(t, []byte(config))
	sa, _ := RunServerWithConfig(conf)
	defer sa.Shutdown()

	checkFor(t, time.Second, 15*time.Millisecond, func() error {
		if n := sa.NumLeafNodes(); n != 1 {
			return fmt.Errorf("Expected 1 leaf connection, got %v", n)
		}
		return nil
	})

	for test, options := range map[string]*ConnzOptions{
		"WithoutAccount": {},
		"WithAccount":    {Account: acc.Name},
	} {
		t.Run(test, func(t *testing.T) {
			c := pollConnz(t, sb, 1, "http://127.0.0.1:%d/connz", options)
			require_Equal(t, c.NumConns, 1)
			require_Equal(t, c.Conns[0].Kind, kindStringMap[LEAF])
		})
	}
}

// https://github.com/nats-io/nats-server/issues/4144
func TestMonitorAccountszMappingOrderReporting(t *testing.T) {
	conf := createConfFile(t, []byte(`
	listen: 127.0.0.1:-1
	server_name: SR22
	accounts {
		CLOUD {
			exports [ { service: "downlink.>" } ]
		}
		CLOUD2 {
			exports [ { service: "downlink.>" } ]
		}
		APP {
			imports [
				{ service: { account: CLOUD, subject: "downlink.>"}, to: "event.>"}
				{ service: { account: CLOUD2, subject: "downlink.>"}, to: "event.>"}
			]
		}
	}`))

	s, _ := RunServerWithConfig(conf)
	defer s.Shutdown()

	az, err := s.Accountz(&AccountzOptions{"APP"})
	require_NoError(t, err)
	require_NotNil(t, az.Account)
	require_True(t, len(az.Account.Imports) > 0)

	var found bool
	m := map[string]struct{}{}
	for _, si := range az.Account.Imports {
		if si.Import.Subject == "downlink.>" {
			found = true
			require_True(t, si.Import.LocalSubject == "event.>")
			m[si.Import.Account] = struct{}{}
		}
	}
	require_True(t, found)
	if len(m) != 2 {
		t.Fatalf("Expected imports from CLOUD and CLOUD2, got %v", m)
	}
}

// expectHealthStatus makes a request to the URL and checks the HTTP status code,
// Content-Type header and health status string.
func expectHealthStatus(t *testing.T, url string, statusCode int, wantStatus string) {
	t.Helper()

	body := readBodyEx(t, url, statusCode, appJSONContent)
	checkHealthStatus(t, body, wantStatus)
}

// checkHealthStatus checks the health status from a JSON response.
func checkHealthStatus(t *testing.T, body []byte, wantStatus string) {
	t.Helper()

	h := &HealthStatus{}

	if err := json.Unmarshal(body, h); err != nil {
		t.Fatalf("error unmarshalling the body: %v", err)
	}

	if h.Status != wantStatus {
		t.Errorf("want health status %q, got %q", wantStatus, h.Status)
	}
}

// checkHealthzEndpoint makes requests to the /healthz endpoint and checks the health status.
func checkHealthzEndpoint(t *testing.T, address string, statusCode int, wantStatus string) {
	t.Helper()

	cases := map[string]string{
		"healthz":         fmt.Sprintf("http://%s/healthz", address),
		"js-enabled-only": fmt.Sprintf("http://%s/healthz?js-enabled-only=true", address),
		"js-server-only":  fmt.Sprintf("http://%s/healthz?js-server-only=true", address),
	}

	for name, url := range cases {
		t.Run(name, func(t *testing.T) {
			expectHealthStatus(t, url, statusCode, wantStatus)
		})
	}
}

func TestMonitorHealthzStatusOK(t *testing.T) {
	s := runMonitorServer()
	defer s.Shutdown()

	checkHealthzEndpoint(t, s.MonitorAddr().String(), http.StatusOK, "ok")
}

func TestMonitorHealthzStatusError(t *testing.T) {
	s := runMonitorServer()
	defer s.Shutdown()

	// Intentionally causing an error in readyForConnections().
	// Note: Private field access, taking advantage of having the tests in the same package.
	s.mu.Lock()
	sl := s.listener
	s.listener = nil
	s.mu.Unlock()

	checkHealthzEndpoint(t, s.MonitorAddr().String(), http.StatusInternalServerError, "error")

	// Restore for proper shutdown.
	s.mu.Lock()
	s.listener = sl
	s.mu.Unlock()
}

func TestMonitorHealthzStatusUnavailable(t *testing.T) {
	opts := DefaultMonitorOptions()
	opts.JetStream = true

	s := RunServer(opts)
	defer s.Shutdown()

	if !s.JetStreamEnabled() {
		t.Fatalf("want JetStream to be enabled first")
	}

	err := s.DisableJetStream()

	if err != nil {
		t.Fatalf("got an error disabling JetStream: %v", err)
	}

	for _, test := range []struct {
		name       string
		url        string
		statusCode int
		wantStatus string
	}{
		{
			"healthz",
			fmt.Sprintf("http://%s/healthz?", s.MonitorAddr().String()),
			http.StatusServiceUnavailable,
			"unavailable",
		},
		{
			"js-enabled-only",
			fmt.Sprintf("http://%s/healthz?js-enabled-only=true", s.MonitorAddr().String()),
			http.StatusServiceUnavailable,
			"unavailable",
		},
		{
			"js-server-only",
			fmt.Sprintf("http://%s/healthz?js-server-only=true", s.MonitorAddr().String()),
			http.StatusOK,
			"ok",
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Run("no-details", func(t *testing.T) {
				expectHealthStatus(t, test.url, test.statusCode, test.wantStatus)
			})
			t.Run("with-details", func(t *testing.T) {
				detailsUrl := fmt.Sprintf("%s&details=true", test.url)
				if test.wantStatus != "ok" {
					test.wantStatus = "error"
				}
				expectHealthStatus(t, detailsUrl, test.statusCode, test.wantStatus)
			})
		})
	}
}

func TestServerHealthz(t *testing.T) {
	t.Run("BasicHealth", func(t *testing.T) {
		s := runMonitorServer()
		defer s.Shutdown()

		// Test with nil options
		status := s.Healthz(nil)
		if status == nil {
			t.Fatal("Expected non-nil HealthStatus")
		}
		if status.Status != "ok" {
			t.Fatalf("Expected status 'ok', got %q", status.Status)
		}
		if status.StatusCode != http.StatusOK {
			t.Fatalf("Expected status code %d, got %d", http.StatusOK, status.StatusCode)
		}

		// Test with empty options
		status = s.Healthz(&HealthzOptions{})
		if status == nil {
			t.Fatal("Expected non-nil HealthStatus")
		}
		if status.Status != "ok" {
			t.Fatalf("Expected status 'ok', got %q", status.Status)
		}

		// Test with JSServerOnly option
		status = s.Healthz(&HealthzOptions{JSServerOnly: true})
		if status == nil {
			t.Fatal("Expected non-nil HealthStatus")
		}
		if status.Status != "ok" {
			t.Fatalf("Expected status 'ok', got %q", status.Status)
		}
	})

	t.Run("BadRequestMissingAccount", func(t *testing.T) {
		s := runMonitorServer()
		defer s.Shutdown()

		// Stream without account should return bad request
		status := s.Healthz(&HealthzOptions{Stream: "TEST"})
		if status.StatusCode != http.StatusBadRequest {
			t.Fatalf("Expected status code %d, got %d", http.StatusBadRequest, status.StatusCode)
		}
		if status.Status != "error" {
			t.Fatalf("Expected status 'error', got %q", status.Status)
		}
	})

	t.Run("BadRequestMissingStream", func(t *testing.T) {
		s := runMonitorServer()
		defer s.Shutdown()

		// Consumer without stream should return bad request
		status := s.Healthz(&HealthzOptions{Account: "ACC", Consumer: "TEST"})
		if status.StatusCode != http.StatusBadRequest {
			t.Fatalf("Expected status code %d, got %d", http.StatusBadRequest, status.StatusCode)
		}
		if status.Status != "error" {
			t.Fatalf("Expected status 'error', got %q", status.Status)
		}
	})

	t.Run("DetailsOption", func(t *testing.T) {
		s := runMonitorServer()
		defer s.Shutdown()

		// Test with Details option - should still return ok for healthy server
		status := s.Healthz(&HealthzOptions{Details: true})
		if status == nil {
			t.Fatal("Expected non-nil HealthStatus")
		}
		if status.Status != "ok" {
			t.Fatalf("Expected status 'ok', got %q", status.Status)
		}

		// Test bad request with details - should populate Errors slice
		status = s.Healthz(&HealthzOptions{Stream: "TEST", Details: true})
		if status.StatusCode != http.StatusBadRequest {
			t.Fatalf("Expected status code %d, got %d", http.StatusBadRequest, status.StatusCode)
		}
		if len(status.Errors) == 0 {
			t.Fatal("Expected Errors slice to be populated with Details=true")
		}
		if status.Errors[0].Type != HealthzErrorBadRequest {
			t.Fatalf("Expected error type %v, got %v", HealthzErrorBadRequest, status.Errors[0].Type)
		}
	})

	t.Run("ServerNotReady", func(t *testing.T) {
		s := runMonitorServer()
		defer s.Shutdown()

		// Simulate server not ready by removing listener
		s.mu.Lock()
		sl := s.listener
		s.listener = nil
		s.mu.Unlock()

		status := s.Healthz(nil)
		if status.StatusCode != http.StatusInternalServerError {
			t.Fatalf("Expected status code %d, got %d", http.StatusInternalServerError, status.StatusCode)
		}
		if status.Status != "error" {
			t.Fatalf("Expected status 'error', got %q", status.Status)
		}

		// Restore for proper shutdown
		s.mu.Lock()
		s.listener = sl
		s.mu.Unlock()
	})

	t.Run("JetStreamUnavailable", func(t *testing.T) {
		opts := DefaultMonitorOptions()
		opts.JetStream = true
		s := RunServer(opts)
		defer s.Shutdown()

		if !s.JetStreamEnabled() {
			t.Fatalf("want JetStream to be enabled first")
		}

		err := s.DisableJetStream()
		if err != nil {
			t.Fatalf("got an error disabling JetStream: %v", err)
		}

		// Should report unavailable when JS is disabled
		status := s.Healthz(nil)
		if status.StatusCode != http.StatusServiceUnavailable {
			t.Fatalf("Expected status code %d, got %d", http.StatusServiceUnavailable, status.StatusCode)
		}
		if status.Status != "unavailable" {
			t.Fatalf("Expected status 'unavailable', got %q", status.Status)
		}

		// JSServerOnly should still report ok
		status = s.Healthz(&HealthzOptions{JSServerOnly: true})
		if status.Status != "ok" {
			t.Fatalf("Expected status 'ok' with JSServerOnly, got %q", status.Status)
		}
	})
}

// When we converted ipq to use generics we still were using sync.Map. Currently you can not convert
// any or any to a generic parameterized type. So this stopped working and panics.
func TestMonitorIpqzWithGenerics(t *testing.T) {
	opts := DefaultMonitorOptions()
	opts.JetStream = true

	s := RunServer(opts)
	defer s.Shutdown()

	url := fmt.Sprintf("http://%s/ipqueuesz?all=1", s.MonitorAddr().String())
	body := readBody(t, url)
	require_True(t, len(body) > 0)

	queues := IpqueueszStatus{}
	require_NoError(t, json.Unmarshal(body, &queues))
	require_True(t, len(queues) >= 4)
	_, ok := queues["SendQ"]
	require_True(t, ok)
}

func TestMonitorVarzSyncInterval(t *testing.T) {
	resetPreviousHTTPConnections()
	opts := DefaultMonitorOptions()
	opts.JetStream = true
	opts.SyncInterval = 22 * time.Second
	opts.SyncAlways = true

	s := RunServer(opts)
	defer s.Shutdown()

	url := fmt.Sprintf("http://127.0.0.1:%d/varz", s.MonitorAddr().Port)

	jscfg := pollVarz(t, s, 0, url, nil).JetStream.Config
	require_True(t, jscfg.SyncInterval == opts.SyncInterval)
	require_True(t, jscfg.SyncAlways)
}

func TestMonitorVarzJSApiLevel(t *testing.T) {
	resetPreviousHTTPConnections()
	opts := DefaultMonitorOptions()
	opts.JetStream = true

	s := RunServer(opts)
	defer s.Shutdown()

	url := fmt.Sprintf("http://127.0.0.1:%d/varz", s.MonitorAddr().Port)

	varz := pollVarz(t, s, 0, url, nil)
	apiLevel := varz.JetStream.Stats.API.Level
	require_Equal(t, apiLevel, JSApiLevel)
}

func TestMonitorVarzMetadata(t *testing.T) {
	s := runMonitorServer()
	defer s.Shutdown()

	v, err := s.Varz(nil)
	require_NoError(t, err)

	expected := map[string]string{"key1": "value1", "key2": "value2"}
	if !reflect.DeepEqual(expected, v.Metadata) {
		t.Fatalf("expected: %v, got: %v", expected, v.Metadata)
	}
}

func TestMonitorVarzFeatureFlags(t *testing.T) {
	featureFlags["fix"] = false
	t.Cleanup(func() { delete(featureFlags, "fix") })

	expected := make(map[string]bool)
	for k, v := range featureFlags {
		expected[k] = v
	}
	expected["fix"] = true

	s := runMonitorServer()
	defer s.Shutdown()

	v, err := s.Varz(nil)
	require_NoError(t, err)

	if !reflect.DeepEqual(expected, v.FeatureFlags) {
		t.Fatalf("expected: %v, got: %v", expected, v.FeatureFlags)
	}
}

func TestMonitorVarzTLSCertEndDate(t *testing.T) {
	resetPreviousHTTPConnections()
	opts := DefaultMonitorOptions()
	tlsConfig, err := GenTLSConfig(
		&TLSConfigOpts{
			CertFile: "../test/configs/certs/server-cert.pem",
			KeyFile:  "../test/configs/certs/server-key.pem",
			CaFile:   "../test/configs/certs/ca.pem",
		})
	if err != nil {
		t.Fatalf("Error generating TLS config: %v", err)
	}

	opts.TLSConfig = tlsConfig
	opts.Cluster.TLSConfig = tlsConfig
	opts.Gateway.TLSConfig = tlsConfig
	opts.LeafNode.TLSConfig = tlsConfig
	opts.MQTT.TLSConfig = tlsConfig
	opts.Websocket.TLSConfig = tlsConfig

	s := RunServer(opts)
	defer s.Shutdown()

	url := fmt.Sprintf("http://127.0.0.1:%d/varz", s.MonitorAddr().Port)
	v := pollVarz(t, s, 0, url, nil)

	expected := time.Date(2032, 8, 24, 20, 23, 02, 0, time.UTC)

	check := func(t *testing.T, notAfter time.Time) {
		t.Helper()
		if notAfter != expected {
			t.Fatalf("Expected expiration date '%v', got '%v'", expected, notAfter)
		}
	}

	check(t, v.TLSCertNotAfter)
	check(t, v.Cluster.TLSCertNotAfter)
	check(t, v.Gateway.TLSCertNotAfter)
	check(t, v.LeafNode.TLSCertNotAfter)
	check(t, v.MQTT.TLSCertNotAfter)
	check(t, v.Websocket.TLSCertNotAfter)
}

func TestMetaClusterInfoSnapshotStats(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "R3S", 3)
	defer c.shutdown()

	// Create a stream to generate some meta activity.
	s := c.randomNonLeader()
	nc, js := jsClientConnect(t, s)
	defer nc.Close()
	_, err := js.AddStream(&nats.StreamConfig{
		Name:     "TEST",
		Subjects: []string{"foo"},
		Replicas: 3,
	})
	require_NoError(t, err)

	leader := c.leader()
	require_True(t, leader != nil)

	// Check Jsz() includes Snapshot.
	checkFor(t, 5*time.Second, 250*time.Millisecond, func() error {
		jsi, err := leader.Jsz(nil)
		if err != nil {
			return err
		}
		if jsi.Meta == nil {
			return errors.New("expected meta cluster info from Jsz")
		}
		if jsi.Meta.Snapshot == nil {
			return errors.New("expected snapshot stats in Jsz meta cluster info")
		}
		return nil
	})

	// Check Varz() includes Snapshot.
	checkFor(t, 5*time.Second, 250*time.Millisecond, func() error {
		v, err := leader.Varz(nil)
		if err != nil {
			return err
		}
		if v.JetStream.Meta == nil {
			return errors.New("expected meta cluster info from Varz")
		}
		if v.JetStream.Meta.Snapshot == nil {
			return errors.New("expected snapshot stats in Varz meta cluster info")
		}
		return nil
	})

	// Check STATSZ event includes Snapshot.
	snc, _ := jsClientConnect(t, c.randomServer(), nats.UserInfo("admin", "s3cr3t!"))
	defer snc.Close()

	ch := make(chan *nats.Msg, 1)
	_, err = snc.ChanSubscribe(fmt.Sprintf(serverStatsSubj, leader.ID()), ch)
	require_NoError(t, err)

	msg := require_ChanRead(t, ch, 5*time.Second)
	var m ServerStatsMsg
	require_NoError(t, json.Unmarshal(msg.Data, &m))
	require_True(t, m.Stats.JetStream != nil)
	require_True(t, m.Stats.JetStream.Meta != nil)
	require_True(t, m.Stats.JetStream.Meta.Snapshot != nil)
}

func TestMonitorConnzAccountRace(t *testing.T) {
	s := runMonitorServerWithAccounts()
	defer s.Shutdown()

	accA, err := s.LookupAccount("A")
	if err != nil {
		t.Fatalf("LookupAccount A: %v", err)
	}
	accB, err := s.LookupAccount("B")
	if err != nil {
		t.Fatalf("LookupAccount B: %v", err)
	}

	// Real client bound to account A.
	nc := createClientConnWithUserSubscribeAndPublish(t, s, "a", "a")
	defer nc.Close()

	// Grab the server-side *client.
	var c *client
	checkFor(t, 2*time.Second, 5*time.Millisecond, func() error {
		accA.mu.RLock()
		defer accA.mu.RUnlock()
		for cl := range accA.clients {
			c = cl
			return nil
		}
		return errors.New("could not find server-side client for account A")
	})

	var wg sync.WaitGroup
	stop := make(chan struct{})

	// Writer: write c.acc under c.mu, mimicking registerWithAccount /
	// swapAccountAfterReload binding/swapping an account.
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-stop:
				return
			default:
			}
			c.mu.Lock()
			if c.acc == accA {
				c.acc = accB
			} else {
				c.acc = accA
			}
			c.mu.Unlock()
		}
	}()

	// Reader: account-filter path reads client.acc / client.acc.Name under only s.mu.
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-stop:
				return
			default:
			}
			if _, err := s.Connz(&ConnzOptions{Account: "A"}); err != nil {
				t.Errorf("Connz error: %v", err)
				return
			}
		}
	}()

	time.Sleep(500 * time.Millisecond)
	close(stop)
	wg.Wait()

	// Restore so shutdown/cleanup is consistent.
	c.mu.Lock()
	c.acc = accA
	c.mu.Unlock()
}

func TestConnzClosedSubsDetailNoSharedMutation(t *testing.T) {
	s := runMonitorServer()
	defer s.Shutdown()

	// Inject a closed client with subscription detail directly into the ring buffer.
	cc := &closedClient{}
	cc.Cid = 1
	cc.subs = []SubDetail{{Subject: "foo.bar"}}
	cc.NumSubs = 1
	s.closed.append(cc)

	// Concurrently request closed connections with subscription detail.
	var wg sync.WaitGroup
	opts := &ConnzOptions{State: ConnClosed, SubscriptionsDetail: true}
	for range 4 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if _, err := s.Connz(opts); err != nil {
				t.Errorf("Error on Connz: %v", err)
			}
		}()
	}
	wg.Wait()

	// The shared cached object must not have been mutated.
	s.mu.Lock()
	leaked := cc.SubsDetail != nil
	s.mu.Unlock()
	require_False(t, leaked)

	// A plain closed-conn query must not carry SubsDetail.
	c, err := s.Connz(&ConnzOptions{State: ConnClosed})
	require_NoError(t, err)
	require_Len(t, len(c.Conns), 1)
	if c.Conns[0].SubsDetail != nil {
		t.Fatalf("Plain closed-conn query unexpectedly carried SubsDetail: %+v", c.Conns[0].SubsDetail)
	}
}

// Returns a listener that is never accepted from, so that a leafnode remote
// pointed at it can dial but never completes the connect process, and no other
// process can take its port while the test runs.
func monitorHealthzStalledRemoteURL(t *testing.T) *url.URL {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require_NoError(t, err)
	t.Cleanup(func() { ln.Close() })
	u, err := url.Parse(fmt.Sprintf("nats://%s", ln.Addr()))
	require_NoError(t, err)
	return u
}

func TestMonitorHealthzLeafRemotes(t *testing.T) {
	ho := DefaultOptions()
	ho.Port = -1
	ho.Cluster.Name = "hub"
	ho.LeafNode.Host = "127.0.0.1"
	ho.LeafNode.Port = -1
	hub := RunServer(ho)
	defer hub.Shutdown()

	u, err := url.Parse(fmt.Sprintf("nats://127.0.0.1:%d", ho.LeafNode.Port))
	require_NoError(t, err)

	lo := DefaultOptions()
	lo.Port = -1
	lo.Cluster.Name = "leaf"
	lo.HTTPHost = "127.0.0.1"
	lo.HTTPPort = -1
	lo.LeafNode.Remotes = []*RemoteLeafOpts{{URLs: []*url.URL{u}}}
	leaf := RunServer(lo)
	defer leaf.Shutdown()

	addr := leaf.MonitorAddr().String()
	get := func(query string) (int, HealthStatus) {
		t.Helper()
		resp, err := http.Get(fmt.Sprintf("http://%s/healthz%s", addr, query))
		require_NoError(t, err)
		defer resp.Body.Close()
		var hs HealthStatus
		require_NoError(t, json.NewDecoder(resp.Body).Decode(&hs))
		return resp.StatusCode, hs
	}
	checkStatus := func(query string, expected int) {
		t.Helper()
		checkFor(t, 10*time.Second, 50*time.Millisecond, func() error {
			if code, hs := get(query); code != expected {
				return fmt.Errorf("status %d, expected %d: %s", code, expected, hs.Error)
			}
			return nil
		})
	}

	// Remote connects.
	checkStatus("?leaf-remotes=true", http.StatusOK)
	code, hs := get("?leaf-remotes=true&details=true")
	require_Equal(t, code, http.StatusOK)
	require_Equal(t, len(hs.Errors), 0)

	// Drop the link and prevent it from coming back.
	leaf.closeAndDisableLeafnodes()
	checkStatus("?leaf-remotes=true", http.StatusServiceUnavailable)
	// Plain healthz is unaffected by the remote being down.
	code, _ = get("")
	require_Equal(t, code, http.StatusOK)
	code, hs = get("?leaf-remotes=true")
	require_Equal(t, code, http.StatusServiceUnavailable)
	require_True(t, strings.Contains(hs.Error, "is not connected"))
	code, hs = get("?leaf-remotes=true&details=true")
	require_Equal(t, code, http.StatusServiceUnavailable)
	require_Equal(t, len(hs.Errors), 1)
	require_Equal(t, hs.Errors[0].Type, HealthzErrorConn)

	// Allow it to reconnect.
	leaf.reEnableLeafnodes()
	checkStatus("?leaf-remotes=true", http.StatusOK)
}

func TestMonitorHealthzLeafRemotesMultipleAndDisabled(t *testing.T) {
	// None of these remotes can complete a connection.
	a, b, c := monitorHealthzStalledRemoteURL(t), monitorHealthzStalledRemoteURL(t), monitorHealthzStalledRemoteURL(t)
	conf := createConfFile(t, fmt.Appendf(nil, `
		listen: 127.0.0.1:-1
		http: 127.0.0.1:-1
		accounts {
			A { users = [ { user: "a", pass: "p" } ] }
			B { users = [ { user: "b", pass: "p" } ] }
			C { users = [ { user: "c", pass: "p" } ] }
		}
		leafnodes {
			remotes = [
				{ url: "%s", account: "A" }
				{ url: "%s", account: "B" }
				{ url: "%s", account: "C", disabled: true }
			]
		}
	`, a, b, c))
	s, _ := RunServerWithConfig(conf)
	defer s.Shutdown()

	hs := s.Healthz(&HealthzOptions{LeafRemotes: true, Details: true})
	require_Equal(t, hs.StatusCode, http.StatusServiceUnavailable)
	require_Equal(t, hs.Status, "error")
	// Both enabled remotes are reported, in a stable order, and the disabled one is not.
	require_Equal(t, len(hs.Errors), 2)
	require_True(t, strings.Contains(hs.Errors[0].Error, `account "A"`))
	require_True(t, strings.Contains(hs.Errors[1].Error, `account "B"`))
	for _, he := range hs.Errors {
		require_Equal(t, he.Type, HealthzErrorConn)
		require_False(t, strings.Contains(he.Error, `account "C"`))
	}

	// Without details, a single error string lists them all.
	hs = s.Healthz(&HealthzOptions{LeafRemotes: true})
	require_Equal(t, hs.StatusCode, http.StatusServiceUnavailable)
	require_True(t, strings.Contains(hs.Error, `account "A"`))
	require_True(t, strings.Contains(hs.Error, `account "B"`))
	require_False(t, strings.Contains(hs.Error, `account "C"`))

	// Same server without the option is healthy.
	hs = s.Healthz(nil)
	require_Equal(t, hs.StatusCode, http.StatusOK)
}

func TestMonitorHealthzLeafRemotesErrorDoesNotLeakSecrets(t *testing.T) {
	u := monitorHealthzStalledRemoteURL(t)
	creds := filepath.Join(t.TempDir(), "secret-creds-file.creds")
	conf := createConfFile(t, fmt.Appendf(nil, `
		listen: 127.0.0.1:-1
		leafnodes {
			remotes = [ { url: "nats://user:s3cr3tpass@%s", credentials: %q } ]
		}
	`, u.Host, creds))
	// The credentials file does not need to be valid for the remote to be configured.
	require_NoError(t, os.WriteFile(creds, []byte("x"), 0600))
	s, _ := RunServerWithConfig(conf)
	defer s.Shutdown()

	hs := s.Healthz(&HealthzOptions{LeafRemotes: true})
	require_Equal(t, hs.StatusCode, http.StatusServiceUnavailable)
	for _, secret := range []string{"s3cr3tpass", "secret-creds-file", "credentials"} {
		require_False(t, strings.Contains(hs.Error, secret))
	}
}

func TestMonitorHealthzLeafRemotesSystemAccountRequest(t *testing.T) {
	u := monitorHealthzStalledRemoteURL(t)
	conf := createConfFile(t, fmt.Appendf(nil, `
		listen: 127.0.0.1:-1
		accounts {
			A { users = [ { user: "a", pass: "p" } ] }
			$SYS { users = [ { user: "admin", pass: "s3cr3t!" } ] }
		}
		leafnodes {
			remotes = [ { url: "%s", account: "A" } ]
		}
	`, u))
	s, _ := RunServerWithConfig(conf)
	defer s.Shutdown()

	nc, err := nats.Connect(s.ClientURL(), nats.UserInfo("admin", "s3cr3t!"))
	require_NoError(t, err)
	defer nc.Close()

	request := func(payload string) HealthStatus {
		t.Helper()
		msg, err := nc.Request(fmt.Sprintf("$SYS.REQ.SERVER.%s.HEALTHZ", s.ID()), []byte(payload), 2*time.Second)
		require_NoError(t, err)
		var resp struct {
			Data HealthStatus `json:"data"`
		}
		require_NoError(t, json.Unmarshal(msg.Data, &resp))
		return resp.Data
	}

	hs := request(`{"leaf-remotes":true}`)
	require_Equal(t, hs.StatusCode, http.StatusServiceUnavailable)
	require_True(t, strings.Contains(hs.Error, "is not connected"))

	hs = request(`{}`)
	require_Equal(t, hs.StatusCode, http.StatusOK)
}
