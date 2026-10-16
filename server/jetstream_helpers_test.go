// Copyright 2020-2026 The NATS Authors
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

//lint:file-ignore U1000 Avoid detecting as unused code

package server

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"math/rand/v2"
	"net"
	"net/url"
	"os"
	"path"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"golang.org/x/time/rate"

	"github.com/nats-io/nats-server/v2/internal/antithesis"
)

// Support functions

func init() {
	// Speed up raft for tests.
	hbInterval = 50 * time.Millisecond
	minElectionTimeout = 1500 * time.Millisecond
	maxElectionTimeout = 3500 * time.Millisecond
	lostQuorumInterval = 5 * time.Second
	lostQuorumCheck = 4 * hbInterval

	// For statz and jetstream placement speedups as well.
	statszRateLimit = 0
}

// Used to setup clusters of clusters for tests.
type cluster struct {
	servers  []*Server
	opts     []*Options
	name     string
	t        testing.TB
	nproxies []*netProxy
}

// Used to setup superclusters for tests.
type supercluster struct {
	t        *testing.T
	clusters []*cluster
	nproxies []*netProxy
}

func (sc *supercluster) shutdown() {
	if sc == nil {
		return
	}
	for _, np := range sc.nproxies {
		np.stop()
	}
	for _, c := range sc.clusters {
		shutdownCluster(c)
	}
}

func (sc *supercluster) randomServer() *Server {
	return sc.randomCluster().randomServer()
}

func (sc *supercluster) serverByName(sname string) *Server {
	for _, c := range sc.clusters {
		if s := c.serverByName(sname); s != nil {
			return s
		}
	}
	return nil
}

func (sc *supercluster) waitOnStreamLeader(account, stream string) {
	sc.t.Helper()
	expires := time.Now().Add(30 * time.Second)
	for time.Now().Before(expires) {
		for _, c := range sc.clusters {
			leader := c.streamLeader(account, stream)
			if leader == nil {
				time.Sleep(100 * time.Millisecond)
				continue
			}
			// Also wait for the stream leader to finish any scales/moves.
			js := leader.getJetStream()
			js.mu.RLock()
			sa := js.streamAssignment(account, stream)
			wait := sa == nil || (sa.Group != nil && sa.Group.Desired != nil)
			js.mu.RUnlock()
			if wait {
				time.Sleep(100 * time.Millisecond)
				continue
			}
			time.Sleep(200 * time.Millisecond)
			return
		}
		time.Sleep(100 * time.Millisecond)
	}
	antithesis.AssertUnreachable(sc.t, "Timeout in supercluster.waitOnStreamLeader", map[string]any{
		"account": account,
		"stream":  stream,
	})
	sc.t.Fatalf("Expected a stream leader for %q %q, got none", account, stream)
}

var jsClusterAccountsTempl = `
	listen: 127.0.0.1:-1

	server_name: %s
	jetstream: {max_mem_store: 2GB, max_file_store: 2GB, store_dir: '%s'}

	leaf {
		listen: 127.0.0.1:-1
	}

	cluster {
		name: %s
		listen: 127.0.0.1:%d
		routes = [%s]
	}

	websocket {
		listen: 127.0.0.1:-1
		compression: true
		handshake_timeout: "5s"
		no_tls: true
	}

	no_auth_user: one

	accounts {
		ONE { users = [ { user: "one", pass: "p" } ]; jetstream: enabled }
		TWO { users = [ { user: "two", pass: "p" } ]; jetstream: enabled }
		NOJS { users = [ { user: "nojs", pass: "p" } ] }
		$SYS { users = [ { user: "admin", pass: "s3cr3t!" } ] }
	}
`

var jsClusterTempl = `
	listen: 127.0.0.1:-1
	server_name: %s
	jetstream: {max_mem_store: 2GB, max_file_store: 2GB, store_dir: '%s'}

	leaf {
		listen: 127.0.0.1:-1
	}

	cluster {
		name: %s
		listen: 127.0.0.1:%d
		routes = [%s]
	}

	# For access to system account.
	accounts { $SYS { users = [ { user: "admin", pass: "s3cr3t!" } ] } }
`

var jsClusterSyncAlwaysTempl = `
	listen: 127.0.0.1:-1
	server_name: %s
	jetstream: {max_mem_store: 2GB, max_file_store: 2GB, store_dir: '%s', sync_interval: always}

	leaf {
		listen: 127.0.0.1:-1
	}

	cluster {
		name: %s
		listen: 127.0.0.1:%d
		routes = [%s]
	}

	# For access to system account.
	accounts { $SYS { users = [ { user: "admin", pass: "s3cr3t!" } ] } }
`

var jsClusterEncryptedTempl = `
	listen: 127.0.0.1:-1
	server_name: %s
	jetstream: {max_mem_store: 2GB, max_file_store: 2GB, store_dir: '%s', key: "s3cr3t!"}

	leaf {
		listen: 127.0.0.1:-1
	}

	cluster {
		name: %s
		listen: 127.0.0.1:%d
		routes = [%s]
	}

	# For access to system account.
	accounts { $SYS { users = [ { user: "admin", pass: "s3cr3t!" } ] } }
`

var jsClusterMaxBytesTempl = `
	listen: 127.0.0.1:-1
	server_name: %s
	jetstream: {max_mem_store: 2GB, max_file_store: 2GB, store_dir: '%s'}

	leaf {
		listen: 127.0.0.1:-1
	}

	cluster {
		name: %s
		listen: 127.0.0.1:%d
		routes = [%s]
	}

	no_auth_user: u

	accounts {
		$U {
			users = [ { user: "u", pass: "p" } ]
			jetstream: {
				max_mem:   128MB
				max_file:  18GB
				max_bytes: true // Forces streams to indicate max_bytes.
			}
		}
		$SYS { users = [ { user: "admin", pass: "s3cr3t!" } ] }
	}
`

var jsClusterMaxBytesAccountLimitTempl = `
	listen: 127.0.0.1:-1
	server_name: %s
	jetstream: {max_mem_store: 256MB, max_file_store: 4GB, store_dir: '%s'}

	leaf {
		listen: 127.0.0.1:-1
	}

	cluster {
		name: %s
		listen: 127.0.0.1:%d
		routes = [%s]
	}

	no_auth_user: u

	accounts {
		$U {
			users = [ { user: "u", pass: "p" } ]
			jetstream: {
				max_mem:   128MB
				max_file:  3GB
				max_bytes: true // Forces streams to indicate max_bytes.
			}
		}
		$SYS { users = [ { user: "admin", pass: "s3cr3t!" } ] }
	}
`

var jsSuperClusterTempl = `
	%s
	gateway {
		name: %s
		listen: 127.0.0.1:%d
		gateways = [%s
		]
	}

	system_account: "$SYS"
`

var jsClusterLimitsTempl = `
	listen: 127.0.0.1:-1
	server_name: %s
	jetstream: {max_mem_store: 2MB, max_file_store: 8MB, store_dir: '%s'}

	cluster {
		name: %s
		listen: 127.0.0.1:%d
		routes = [%s]
	}

	no_auth_user: u

	accounts {
		ONE {
			users = [ { user: "u", pass: "s3cr3t!" } ]
			jetstream: enabled
		}
		$SYS { users = [ { user: "admin", pass: "s3cr3t!" } ] }
	}
`

var jsMixedModeGlobalAccountTempl = `
	listen: 127.0.0.1:-1
	server_name: %s
	jetstream: {max_mem_store: 2MB, max_file_store: 8MB, store_dir: '%s'}

	cluster {
		name: %s
		listen: 127.0.0.1:%d
		routes = [%s]
	}

	accounts {$SYS { users = [ { user: "admin", pass: "s3cr3t!" } ] } }
`

var jsGWTempl = `%s{name: %s, urls: [%s]}`

var jsClusterAccountLimitsTempl = `
	listen: 127.0.0.1:-1
	server_name: %s
	jetstream: {max_mem_store: 2GB, max_file_store: 2GB, store_dir: '%s'}

	cluster {
		name: %s
		listen: 127.0.0.1:%d
		routes = [%s]
	}

	no_auth_user: js

	accounts {
		$JS { users = [ { user: "js", pass: "p" } ]; jetstream: {max_store: 1MB, max_mem: 0} }
		$SYS { users = [ { user: "admin", pass: "s3cr3t!" } ] }
	}
`

func createJetStreamTaggedSuperCluster(t *testing.T) *supercluster {
	return createJetStreamTaggedSuperClusterWithGWProxy(t, nil)
}

func createJetStreamTaggedSuperClusterWithGWProxy(t *testing.T, gwm gwProxyMap) *supercluster {
	sc := createJetStreamSuperClusterWithTemplateAndModHook(t, jsClusterTempl, 3, 3, nil, gwm)
	sc.waitOnPeerCount(9)

	reset := func(s *Server) {
		s.mu.Lock()
		rch := s.sys.resetCh
		s.mu.Unlock()
		if rch != nil {
			rch <- struct{}{}
		}
		s.sendStatszUpdate()
	}

	// Make first cluster AWS, US country code.
	for i, s := range sc.clusterForName("C1").servers {
		s.optsMu.Lock()
		s.opts.Tags.Add("cloud:aws")
		s.opts.Tags.Add("country:us")
		s.opts.Tags.Add(fmt.Sprintf("node:%d", i+1))
		s.optsMu.Unlock()
		reset(s)
	}
	// Make second cluster GCP, UK country code.
	for i, s := range sc.clusterForName("C2").servers {
		s.optsMu.Lock()
		s.opts.Tags.Add("cloud:gcp")
		s.opts.Tags.Add("country:uk")
		s.opts.Tags.Add(fmt.Sprintf("node:%d", i+1))
		s.optsMu.Unlock()
		reset(s)
	}
	// Make third cluster AZ, JP country code.
	for i, s := range sc.clusterForName("C3").servers {
		s.optsMu.Lock()
		s.opts.Tags.Add("cloud:az")
		s.opts.Tags.Add("country:jp")
		s.opts.Tags.Add(fmt.Sprintf("node:%d", i+1))
		s.optsMu.Unlock()
		reset(s)
	}

	sc.waitOnLeader()
	ml := sc.leader()
	require_True(t, ml != nil)
	js := ml.getJetStream()
	require_True(t, js != nil)
	js.mu.RLock()
	defer js.mu.RUnlock()
	cc := js.cluster
	require_True(t, cc != nil)

	// Walk and make sure all tags are registered.
	expires := time.Now().Add(10 * time.Second)
	for time.Now().Before(expires) {
		allOK := true
		for _, p := range cc.meta.Peers() {
			si, ok := ml.nodeToInfo.Load(p.ID)
			require_True(t, ok)
			ni := si.(nodeInfo)
			if len(ni.tags) == 0 {
				allOK = false
				reset(sc.serverByName(ni.name))
			}
		}
		if allOK {
			break
		}
	}

	return sc
}

func createJetStreamSuperCluster(t *testing.T, numServersPer, numClusters int) *supercluster {
	return createJetStreamSuperClusterWithTemplate(t, jsClusterTempl, numServersPer, numClusters)
}

func createJetStreamSuperClusterWithTemplate(t *testing.T, tmpl string, numServersPer, numClusters int) *supercluster {
	return createJetStreamSuperClusterWithTemplateAndModHook(t, tmpl, numServersPer, numClusters, nil, nil)
}

// For doing proxyies in GWs.
type gwProxy struct {
	rtt  time.Duration
	up   int
	down int
}

// For use in normal clusters.
type clusterProxy = gwProxy

// Maps cluster names to proxy settings.
type gwProxyMap map[string]*gwProxy

func createJetStreamSuperClusterWithTemplateAndModHook(t *testing.T, tmpl string, numServersPer, numClusters int, modify modifyCb, gwm gwProxyMap) *supercluster {
	t.Helper()
	if numServersPer < 1 {
		t.Fatalf("Number of servers must be >= 1")
	}
	if numClusters <= 1 {
		t.Fatalf("Number of clusters must be > 1")
	}

	startClusterPorts := []int{20_022, 22_022, 24_022}
	startGatewayPorts := []int{20_122, 22_122, 24_122}
	startClusterPort := startClusterPorts[rand.IntN(len(startClusterPorts))]
	startGWPort := startGatewayPorts[rand.IntN(len(startGatewayPorts))]

	// Make the GWs form faster for the tests.
	SetGatewaysSolicitDelay(10 * time.Millisecond)
	defer ResetGatewaysSolicitDelay()

	cp, gp := startClusterPort, startGWPort
	var clusters []*cluster
	var nproxies []*netProxy
	var gws []string

	// Build GWs first, will be same for all servers.
	for i, port := 1, gp; i <= numClusters; i++ {
		cn := fmt.Sprintf("C%d", i)
		var gwp *gwProxy
		if len(gwm) > 0 {
			gwp = gwm[cn]
		}
		var urls []string
		for n := 0; n < numServersPer; n++ {
			routeURL := fmt.Sprintf("nats-route://127.0.0.1:%d", port)
			if gwp != nil {
				np := createNetProxy(gwp.rtt, gwp.up, gwp.down, routeURL, false)
				nproxies = append(nproxies, np)
				routeURL = np.routeURL()
			}
			urls = append(urls, routeURL)
			port++
		}
		gws = append(gws, fmt.Sprintf(jsGWTempl, "\n\t\t\t", cn, strings.Join(urls, ",")))
	}
	gwconf := strings.Join(gws, _EMPTY_)

	for i := 1; i <= numClusters; i++ {
		cn := fmt.Sprintf("C%d", i)
		// Go ahead and build configurations.
		c := &cluster{servers: make([]*Server, 0, numServersPer), opts: make([]*Options, 0, numServersPer), name: cn}

		// Build out the routes that will be shared with all configs.
		var routes []string
		for port := cp; port < cp+numServersPer; port++ {
			routes = append(routes, fmt.Sprintf("nats-route://127.0.0.1:%d", port))
		}
		routeConfig := strings.Join(routes, ",")

		for si := 0; si < numServersPer; si++ {
			storeDir := t.TempDir()
			sn := fmt.Sprintf("%s-S%d", cn, si+1)
			bconf := fmt.Sprintf(tmpl, sn, storeDir, cn, cp+si, routeConfig)
			conf := fmt.Sprintf(jsSuperClusterTempl, bconf, cn, gp, gwconf)
			gp++
			if modify != nil {
				conf = modify(sn, cn, storeDir, conf)
			}
			s, o := RunServerWithConfig(createConfFile(t, []byte(conf)))
			c.servers = append(c.servers, s)
			c.opts = append(c.opts, o)
		}
		checkClusterFormed(t, c.servers...)
		clusters = append(clusters, c)
		cp += numServersPer
		c.t = t
	}

	// Start any proxies.
	for _, np := range nproxies {
		np.start()
	}

	// Wait for the supercluster to be formed.
	egws := numClusters - 1
	for _, c := range clusters {
		for _, s := range c.servers {
			waitForOutboundGateways(t, s, egws, 10*time.Second)
		}
	}

	sc := &supercluster{t, clusters, nproxies}
	sc.campaignMetaImmediately()
	sc.waitOnLeader()
	sc.waitOnAllCurrent()

	// Wait for all the peer nodes to be registered.
	checkFor(t, 5*time.Second, 100*time.Millisecond, func() error {
		var peers []string
		if ml := sc.leader(); ml != nil {
			peers = ml.ActivePeers()
			if len(peers) == numClusters*numServersPer {
				return nil
			}
		}
		return fmt.Errorf("Not correct number of peers, expected %d, got %d", numClusters*numServersPer, len(peers))
	})

	if sc.leader() == nil {
		sc.t.Fatalf("Expected a cluster leader, got none")
	}

	return sc
}

func (sc *supercluster) createLeafNodes(clusterName string, numServers int) *cluster {
	sc.t.Helper()

	// Create our leafnode cluster template first.
	return sc.createLeafNodesWithDomain(clusterName, numServers, "")
}

func (sc *supercluster) createLeafNodesWithDomain(clusterName string, numServers int, domain string) *cluster {
	sc.t.Helper()

	// Create our leafnode cluster template first.
	return sc.randomCluster().createLeafNodes(clusterName, numServers, domain)
}

func (sc *supercluster) createSingleLeafNode(extend bool) *Server {
	sc.t.Helper()

	return sc.randomCluster().createLeafNode(extend)
}

func (sc *supercluster) leader() *Server {
	for _, c := range sc.clusters {
		if leader := c.leader(); leader != nil {
			return leader
		}
	}
	return nil
}

func (sc *supercluster) waitOnLeader() {
	sc.t.Helper()
	expires := time.Now().Add(30 * time.Second)
	for time.Now().Before(expires) {
		for _, c := range sc.clusters {
			if leader := c.leader(); leader != nil {
				time.Sleep(250 * time.Millisecond)
				return
			}
		}
		time.Sleep(25 * time.Millisecond)
	}
	antithesis.AssertUnreachable(sc.t, "Timeout in supercluster.waitOnLeader", nil)
	sc.t.Fatalf("Expected a cluster leader, got none")
}

func (sc *supercluster) waitOnAccount(account string) {
	sc.t.Helper()
	expires := time.Now().Add(40 * time.Second)
	for time.Now().Before(expires) {
		found := true
		for _, c := range sc.clusters {
			for _, s := range c.servers {
				acc, err := s.fetchAccount(account)
				found = found && err == nil && acc != nil
			}
		}
		if found {
			return
		}
		time.Sleep(100 * time.Millisecond)
		continue
	}

	sc.t.Fatalf("Expected account %q to exist but didn't", account)
}

func (sc *supercluster) campaignMetaImmediately() {
	var servers []*Server
	for _, c := range sc.clusters {
		servers = append(servers, c.servers...)
	}
	campaignMetaImmediately(servers)
}

func (sc *supercluster) waitOnAllCurrent() {
	sc.t.Helper()
	for _, c := range sc.clusters {
		c.waitOnAllCurrent()
	}
}

func (sc *supercluster) clusterForName(name string) *cluster {
	for _, c := range sc.clusters {
		if c.name == name {
			return c
		}
	}
	return nil
}

func (sc *supercluster) randomCluster() *cluster {
	clusters := append(sc.clusters[:0:0], sc.clusters...)
	rand.Shuffle(len(clusters), func(i, j int) { clusters[i], clusters[j] = clusters[j], clusters[i] })
	return clusters[0]
}

func (sc *supercluster) waitOnPeerCount(n int) {
	sc.t.Helper()
	sc.waitOnLeader()
	leader := sc.leader()
	expires := time.Now().Add(30 * time.Second)
	// Make sure we have all peers, and take into account the meta leader could still change.
	for time.Now().Before(expires) {
		if leader = sc.leader(); leader == nil {
			time.Sleep(50 * time.Millisecond)
			continue
		}
		if len(leader.JetStreamClusterPeers()) == n {
			return
		}
		time.Sleep(100 * time.Millisecond)
	}
	antithesis.AssertUnreachable(sc.t, "Timeout in supercluster.waitOnPeerCount", nil)
	sc.t.Fatalf("Expected a super cluster peer count of %d, got %d", n, len(leader.JetStreamClusterPeers()))
}

var jsClusterMirrorSourceImportsTempl = `
	listen: 127.0.0.1:-1
	server_name: %s
	jetstream: {max_mem_store: 2GB, max_file_store: 2GB, store_dir: '%s'}

	cluster {
		name: %s
		listen: 127.0.0.1:%d
		routes = [%s]
	}

	no_auth_user: dlc

	accounts {
		JS {
			jetstream: enabled
			users = [ { user: "rip", pass: "pass" } ]
			exports [
				{ service: "$JS.API.CONSUMER.>" } # To create internal consumers to mirror/source.
				{ stream: "RI.DELIVER.SYNC.>" }   # For the mirror/source consumers sending to IA via delivery subject.
				{ service: "$JS.FC.>" }
			]
		}
		IA {
			jetstream: enabled
			users = [ { user: "dlc", pass: "pass" } ]
			imports [
				{ service: { account: JS, subject: "$JS.API.CONSUMER.>"}, to: "RI.JS.API.CONSUMER.>" }
				{ stream: { account: JS, subject: "RI.DELIVER.SYNC.>"} }
				{ service: {account: JS, subject: "$JS.FC.>" }}
			]
		}
		$SYS { users = [ { user: "admin", pass: "s3cr3t!" } ] }
	}
`

var jsClusterImportsTempl = `
	listen: 127.0.0.1:-1
	server_name: %s
	jetstream: {max_mem_store: 2GB, max_file_store: 2GB, store_dir: '%s'}

	cluster {
		name: %s
		listen: 127.0.0.1:%d
		routes = [%s]
	}

	no_auth_user: dlc

	accounts {
		JS {
			jetstream: enabled
			users = [ { user: "rip", pass: "pass" } ]
			exports [
				{ service: "$JS.API.>", response: stream }
				{ service: "TEST" } # For publishing to the stream.
				{ service: "$JS.ACK.TEST.*.>" }
				{ service: "$JS.ACK.*.*.TEST.*.>" }
			]
		}
		IA {
			users = [ { user: "dlc", pass: "pass" } ]
			imports [
				{ service: { subject: "$JS.API.>", account: JS }}
				{ service: { subject: "TEST", account: JS }}
				{ service: { subject: "$JS.ACK.TEST.*.>", account: JS }}
				{ service: { subject: "$JS.ACK.*.*.TEST.*.>", account: JS }}
			]
		}
		$SYS { users = [ { user: "admin", pass: "s3cr3t!" } ] }
	}
`

func createMixedModeCluster(t testing.TB, tmpl string, clusterName, snPre string, numJsServers, numNonServers int, doJSConfig bool) *cluster {
	t.Helper()

	if clusterName == _EMPTY_ || numJsServers < 0 || numNonServers < 1 {
		t.Fatalf("Bad params")
	}

	numServers := numJsServers + numNonServers
	const startClusterPort = 23232

	// Build out the routes that will be shared with all configs.
	var routes []string
	for cp := startClusterPort; cp < startClusterPort+numServers; cp++ {
		routes = append(routes, fmt.Sprintf("nats-route://127.0.0.1:%d", cp))
	}
	routeConfig := strings.Join(routes, ",")

	// Go ahead and build configurations and start servers.
	c := &cluster{servers: make([]*Server, 0, numServers), opts: make([]*Options, 0, numServers), name: clusterName}

	for cp := startClusterPort; cp < startClusterPort+numServers; cp++ {
		storeDir := t.TempDir()

		sn := fmt.Sprintf("%sS-%d", snPre, cp-startClusterPort+1)
		conf := fmt.Sprintf(tmpl, sn, storeDir, clusterName, cp, routeConfig)

		// Disable JS here.
		if cp-startClusterPort >= numJsServers {
			// We can disable by commmenting it out, meaning no JS config, or can set the config up and just set disabled.
			// e.g. jetstream: {domain: "SPOKE", enabled: false}
			if doJSConfig {
				conf = strings.Replace(conf, "jetstream: {", "jetstream: { enabled: false, ", 1)
			} else {
				conf = strings.Replace(conf, "jetstream: ", "# jetstream: ", 1)
			}
		}

		s, o := RunServerWithConfig(createConfFile(t, []byte(conf)))
		c.servers = append(c.servers, s)
		c.opts = append(c.opts, o)
	}
	c.t = t

	// Wait til we are formed and have a leader.
	c.checkClusterFormed()
	if numJsServers > 0 {
		c.campaignMetaImmediately()
		c.waitOnPeerCount(numJsServers)
	}

	return c
}

// This will create a cluster that is explicitly configured for the routes, etc.
// and also has a defined clustername. All configs for routes and cluster name will be the same.
func createJetStreamClusterExplicit(t testing.TB, clusterName string, numServers int) *cluster {
	return createJetStreamClusterWithTemplate(t, jsClusterTempl, clusterName, numServers)
}

func createJetStreamClusterWithTemplate(t testing.TB, tmpl string, clusterName string, numServers int) *cluster {
	return createJetStreamClusterWithTemplateAndModHook(t, tmpl, clusterName, numServers, nil)
}

func createJetStreamClusterWithTemplateAndModHook(t testing.TB, tmpl string, clusterName string, numServers int, modify modifyCb) *cluster {
	startPorts := []int{7_022, 9_022, 11_022, 15_022}
	port := startPorts[rand.IntN(len(startPorts))]
	return createJetStreamClusterAndModHook(t, tmpl, clusterName, _EMPTY_, numServers, port, true, modify)
}

func createJetStreamCluster(t testing.TB, tmpl string, clusterName, snPre string, numServers int, portStart int, waitOnReady bool) *cluster {
	return createJetStreamClusterAndModHook(t, tmpl, clusterName, snPre, numServers, portStart, waitOnReady, nil)
}

type modifyCb func(serverName, clusterName, storeDir, conf string) string

func createJetStreamClusterAndModHook(t testing.TB, tmpl, cName, snPre string, numServers int, portStart int, waitOnReady bool, modify modifyCb) *cluster {
	return createJetStreamClusterEx(t, tmpl, cName, snPre, numServers, portStart, waitOnReady, modify, nil)
}

func createJetStreamClusterWithNetProxy(t testing.TB, cName string, numServers int, cnp *clusterProxy) *cluster {
	startPorts := []int{7_122, 9_122, 11_122, 15_122}
	port := startPorts[rand.IntN(len(startPorts))]
	return createJetStreamClusterEx(t, jsClusterTempl, cName, _EMPTY_, numServers, port, true, nil, cnp)
}

func createJetStreamClusterEx(t testing.TB, tmpl, cName, snPre string, numServers int, portStart int, wait bool, modify modifyCb, cnp *clusterProxy) *cluster {
	t.Helper()
	if cName == _EMPTY_ || numServers < 1 {
		t.Fatalf("Bad params")
	}

	// Flaky test prevention:
	// Binding a socket to IP stack port 0 will bind an ephemeral port from an OS-specific range.
	// If someone passes in to us a port spec which would cover that range, the test would be flaky.
	// Adjust these ports to be the most inclusive across the port runner OSes.
	// Linux: /proc/sys/net/ipv4/ip_local_port_range : 32768:60999
	// <https://dataplane.org/ephemeralports.html> is useful, and shows there's no safe available range without OS-specific tuning.
	// Our tests are usually run on Linux.  Folks who care about other OSes: if you can't tune your test-runner OS to match, please
	// propose a viable alternative.
	const prohibitedPortFirst = 32768
	const prohibitedPortLast = 60999
	if (portStart >= prohibitedPortFirst && portStart <= prohibitedPortLast) ||
		(portStart+numServers-1 >= prohibitedPortFirst && portStart+numServers-1 <= prohibitedPortLast) {
		t.Fatalf("test setup failure: may not specify a cluster port range which falls within %d:%d", prohibitedPortFirst, prohibitedPortLast)
	}

	// Build out the routes that will be shared with all configs.
	var routes []string
	var nproxies []*netProxy
	for cp := portStart; cp < portStart+numServers; cp++ {
		routeURL := fmt.Sprintf("nats-route://127.0.0.1:%d", cp)
		if cnp != nil {
			np := createNetProxy(cnp.rtt, cnp.up, cnp.down, routeURL, false)
			nproxies = append(nproxies, np)
			routeURL = np.routeURL()
		}
		routes = append(routes, routeURL)
	}
	routeConfig := strings.Join(routes, ",")

	// Go ahead and build configurations and start servers.
	c := &cluster{servers: make([]*Server, 0, numServers), opts: make([]*Options, 0, numServers), name: cName, nproxies: nproxies}

	// Start any proxies.
	for _, np := range nproxies {
		np.start()
	}

	for cp := portStart; cp < portStart+numServers; cp++ {
		storeDir := t.TempDir()
		sn := fmt.Sprintf("%sS-%d", snPre, cp-portStart+1)
		conf := fmt.Sprintf(tmpl, sn, storeDir, cName, cp, routeConfig)
		if modify != nil {
			conf = modify(sn, cName, storeDir, conf)
		}
		s, o := RunServerWithConfig(createConfFile(t, []byte(conf)))
		c.servers = append(c.servers, s)
		c.opts = append(c.opts, o)
	}
	c.t = t

	// Wait til we are formed and have a leader.
	c.checkClusterFormed()
	if wait {
		c.campaignMetaImmediately()
		c.waitOnClusterReady()
	}

	return c
}

func (c *cluster) campaignMetaImmediately() {
	campaignMetaImmediately(c.servers)
}

func campaignMetaImmediately(servers []*Server) {
	// Only campaign on a single meta group, but check all servers first
	// since one could already know of a leader. Pick the campaigning server
	// at random so tests don't always end up with the same meta leader.
	var nodes []RaftNode
	for _, s := range servers {
		js := s.getJetStream()
		if js == nil {
			continue
		}
		n := js.getMetaGroup()
		if n == nil {
			continue
		}
		if n.GroupLeader() != _EMPTY_ {
			return
		}
		nodes = append(nodes, n)
	}
	if len(nodes) > 0 {
		nodes[rand.IntN(len(nodes))].CampaignImmediately()
	}
}

func (c *cluster) addInNewServer() *Server {
	c.t.Helper()
	sn := fmt.Sprintf("S-%d", len(c.servers)+1)
	storeDir := c.t.TempDir()
	seedRoute := fmt.Sprintf("nats-route://127.0.0.1:%d", c.opts[0].Cluster.Port)
	conf := fmt.Sprintf(jsClusterTempl, sn, storeDir, c.name, -1, seedRoute)
	s, o := RunServerWithConfig(createConfFile(c.t, []byte(conf)))
	c.servers = append(c.servers, s)
	c.opts = append(c.opts, o)
	c.checkClusterFormed()
	return s
}

// This is tied to jsClusterAccountsTempl, so changes there to users needs to be reflected here.
func (c *cluster) createSingleLeafNodeNoSystemAccount() *Server {
	as := c.randomServer()
	lno := as.getOpts().LeafNode
	ln1 := fmt.Sprintf("nats://one:p@%s:%d", lno.Host, lno.Port)
	ln2 := fmt.Sprintf("nats://two:p@%s:%d", lno.Host, lno.Port)
	conf := fmt.Sprintf(jsClusterSingleLeafNodeTempl, c.t.TempDir(), ln1, ln2)
	s, o := RunServerWithConfig(createConfFile(c.t, []byte(conf)))
	c.servers = append(c.servers, s)
	c.opts = append(c.opts, o)

	checkLeafNodeConnectedCount(c.t, as, 2)

	return s
}

// This is tied to jsClusterAccountsTempl, so changes there to users needs to be reflected here.
func (c *cluster) createSingleLeafNodeNoSystemAccountAndEnablesJetStream() *Server {
	return c.createSingleLeafNodeNoSystemAccountAndEnablesJetStreamWithDomain(_EMPTY_, "nojs")
}

func (c *cluster) createSingleLeafNodeNoSystemAccountAndEnablesJetStreamWithDomain(domain, user string) *Server {
	tmpl := jsClusterSingleLeafNodeLikeNGSTempl
	if domain != _EMPTY_ {
		nsc := fmt.Sprintf("domain: %s, store_dir:", domain)
		tmpl = strings.Replace(jsClusterSingleLeafNodeLikeNGSTempl, "store_dir:", nsc, 1)
	}
	as := c.randomServer()
	lno := as.getOpts().LeafNode
	ln := fmt.Sprintf("nats://%s:p@%s:%d", user, lno.Host, lno.Port)
	conf := fmt.Sprintf(tmpl, c.t.TempDir(), ln)
	s, o := RunServerWithConfig(createConfFile(c.t, []byte(conf)))
	c.servers = append(c.servers, s)
	c.opts = append(c.opts, o)

	checkLeafNodeConnectedCount(c.t, as, 1)

	return s
}

var jsClusterSingleLeafNodeLikeNGSTempl = `
	listen: 127.0.0.1:-1
	server_name: LNJS
	jetstream: {max_mem_store: 2GB, max_file_store: 2GB, store_dir: '%s'}

	leaf { remotes [ { urls: [ %s ] } ] }
`

var jsClusterSingleLeafNodeTempl = `
	listen: 127.0.0.1:-1
	server_name: LNJS
	jetstream: {max_mem_store: 2GB, max_file_store: 2GB, store_dir: '%s'}

	leaf { remotes [
		{ urls: [ %s ], account: "JSY" }
		{ urls: [ %s ], account: "JSN" } ]
	}

	accounts {
		JSY { users = [ { user: "y", pass: "p" } ]; jetstream: true }
		JSN { users = [ { user: "n", pass: "p" } ] }
		$SYS { users = [ { user: "admin", pass: "s3cr3t!" } ] }
	}
`

var jsClusterTemplWithLeafNode = `
	listen: 127.0.0.1:-1
	server_name: %s
	jetstream: {max_mem_store: 2GB, max_file_store: 2GB, store_dir: '%s'}

	{{leaf}}

	cluster {
		name: %s
		listen: 127.0.0.1:%d
		routes = [%s]
	}

	# For access to system account.
	accounts { $SYS { users = [ { user: "admin", pass: "s3cr3t!" } ] } }
`

var jsClusterTemplWithLeafNodeNoJS = `
	listen: 127.0.0.1:-1
	server_name: %s

	# Need to keep below since it fills in the store dir by default so just comment out.
	# jetstream: {max_mem_store: 2GB, max_file_store: 2GB, store_dir: '%s'}

	{{leaf}}

	cluster {
		name: %s
		listen: 127.0.0.1:%d
		routes = [%s]
	}

	# For access to system account.
	accounts { $SYS { users = [ { user: "admin", pass: "s3cr3t!" } ] } }
`

var jsClusterTemplWithSingleLeafNode = `
	listen: 127.0.0.1:-1
	server_name: %s
	jetstream: {max_mem_store: 2GB, max_file_store: 2GB, store_dir: '%s'}

	{{leaf}}

	# For access to system account.
	accounts { $SYS { users = [ { user: "admin", pass: "s3cr3t!" } ] } }
`

var jsClusterTemplWithSingleFleetLeafNode = `
	listen: 127.0.0.1:-1
	server_name: %s
	cluster: { name: fleet }
	jetstream: {max_mem_store: 2GB, max_file_store: 2GB, store_dir: '%s'}

	{{leaf}}

	# For access to system account.
	accounts { $SYS { users = [ { user: "admin", pass: "s3cr3t!" } ] } }
`

var jsClusterTemplWithSingleLeafNodeNoJS = `
	listen: 127.0.0.1:-1
	server_name: %s

	# jetstream: {store_dir: '%s'}

	{{leaf}}

	# For access to system account.
	accounts { $SYS { users = [ { user: "admin", pass: "s3cr3t!" } ] } }
`

var jsLeafFrag = `
	leaf {
		remotes [
			{ urls: [ %s ] }
			{ urls: [ %s ], account: "$SYS" }
		]
	}
`
var jsLeafNoSysFrag = `
	leaf {
		remotes [
			{ urls: [ %s ] }
		]
	}
`

func (c *cluster) createLeafNodes(clusterName string, numServers int, domain string) *cluster {
	c.t.Helper()
	return c.createLeafNodesWithStartPortAndDomain(clusterName, numServers, 22111, domain)
}

func (c *cluster) createLeafNodesNoJS(clusterName string, numServers int) *cluster {
	c.t.Helper()
	return c.createLeafNodesWithTemplateAndStartPort(jsClusterTemplWithLeafNodeNoJS, clusterName, numServers, 21333)
}

func (c *cluster) createLeafNodesWithStartPortAndDomain(clusterName string, numServers int, portStart int, domain string) *cluster {
	c.t.Helper()
	if domain == _EMPTY_ {
		return c.createLeafNodesWithTemplateAndStartPort(jsClusterTemplWithLeafNode, clusterName, numServers, portStart)
	}
	tmpl := strings.Replace(jsClusterTemplWithLeafNode, "store_dir:", fmt.Sprintf(`domain: "%s", store_dir:`, domain), 1)
	return c.createLeafNodesWithTemplateAndStartPort(tmpl, clusterName, numServers, portStart)
}

func (c *cluster) createLeafNode(extend bool) *Server {
	c.t.Helper()
	if extend {
		return c.createLeafNodeWithTemplate("LNS",
			strings.ReplaceAll(jsClusterTemplWithSingleLeafNode, "store_dir:", " extension_hint: will_extend, store_dir:"))
	} else {
		return c.createLeafNodeWithTemplate("LNS", jsClusterTemplWithSingleLeafNode)
	}
}

func (c *cluster) createLeafNodeWithTemplate(name, template string) *Server {
	c.t.Helper()
	tmpl := c.createLeafSolicit(template)
	conf := fmt.Sprintf(tmpl, name, c.t.TempDir())
	s, o := RunServerWithConfig(createConfFile(c.t, []byte(conf)))
	c.servers = append(c.servers, s)
	c.opts = append(c.opts, o)
	return s
}

func (c *cluster) createLeafNodeWithTemplateNoSystem(name, template string) *Server {
	return c.createLeafNodeWithTemplateNoSystemWithProto(name, template, "nats")
}

func (c *cluster) createLeafNodeWithTemplateNoSystemWithProto(name, template, proto string) *Server {
	c.t.Helper()
	tmpl := c.createLeafSolicitNoSystemWithProto(template, proto)
	conf := fmt.Sprintf(tmpl, name, c.t.TempDir())
	s, o := RunServerWithConfig(createConfFile(c.t, []byte(conf)))
	c.servers = append(c.servers, s)
	c.opts = append(c.opts, o)
	return s
}

// Helper to generate the leaf solicit configs.
func (c *cluster) createLeafSolicit(tmpl string) string {
	return c.createLeafSolicitWithProto(tmpl, "nats")
}

func (c *cluster) createLeafSolicitWithProto(tmpl, proto string) string {
	c.t.Helper()

	// Create our leafnode cluster template first.
	var lns, lnss []string
	for _, s := range c.servers {
		if s.ClusterName() != c.name {
			continue
		}
		ln := s.getOpts().LeafNode
		lns = append(lns, fmt.Sprintf("%s://%s:%d", proto, ln.Host, ln.Port))
		lnss = append(lnss, fmt.Sprintf("%s://admin:s3cr3t!@%s:%d", proto, ln.Host, ln.Port))
	}
	lnc := strings.Join(lns, ", ")
	lnsc := strings.Join(lnss, ", ")
	lconf := fmt.Sprintf(jsLeafFrag, lnc, lnsc)
	return strings.Replace(tmpl, "{{leaf}}", lconf, 1)
}

func (c *cluster) createLeafSolicitNoSystemWithProto(tmpl, proto string) string {
	c.t.Helper()

	// Create our leafnode cluster template first.
	var lns []string
	for _, s := range c.servers {
		if s.ClusterName() != c.name {
			continue
		}
		switch proto {
		case "nats", "tls":
			ln := s.getOpts().LeafNode
			lns = append(lns, fmt.Sprintf("%s://%s:%d", proto, ln.Host, ln.Port))
		case "ws", "wss":
			ln := s.getOpts().Websocket
			lns = append(lns, fmt.Sprintf("%s://%s:%d", proto, ln.Host, ln.Port))
		}
	}
	lnc := strings.Join(lns, ", ")
	return strings.Replace(tmpl, "{{leaf}}", fmt.Sprintf(jsLeafNoSysFrag, lnc), 1)
}

func (c *cluster) createLeafNodesWithTemplateMixedMode(template, clusterName string, numJsServers, numNonServers int, doJSConfig bool) *cluster {
	c.t.Helper()

	// Create our leafnode cluster template first.
	tmpl := c.createLeafSolicit(template)
	pre := clusterName + "-"

	lc := createMixedModeCluster(c.t, tmpl, clusterName, pre, numJsServers, numNonServers, doJSConfig)
	for _, s := range lc.servers {
		checkLeafNodeConnectedCount(c.t, s, 2)
	}
	lc.waitOnClusterReadyWithNumPeers(numJsServers)

	return lc
}

func (c *cluster) createLeafNodesWithTemplateAndStartPort(template, clusterName string, numServers int, portStart int) *cluster {
	c.t.Helper()

	// Create our leafnode cluster template first.
	tmpl := c.createLeafSolicit(template)
	pre := clusterName + "-"
	lc := createJetStreamCluster(c.t, tmpl, clusterName, pre, numServers, portStart, false)
	for _, s := range lc.servers {
		checkLeafNodeConnectedCount(c.t, s, 2)
	}
	return lc
}

// Helper function to close and disable leafnodes.
func (s *Server) closeAndDisableLeafnodes() {
	var leafs []*client
	s.mu.Lock()
	for _, ln := range s.leafs {
		leafs = append(leafs, ln)
	}
	// Disable leafnodes for now.
	s.leafDisableConnect = true
	s.mu.Unlock()

	for _, ln := range leafs {
		ln.closeConnection(Revocation)
	}
}

// Helper function to re-enable leafnode connections.
func (s *Server) reEnableLeafnodes() {
	s.mu.Lock()
	// Re-enable leafnodes.
	s.leafDisableConnect = false
	s.mu.Unlock()
}

// Helper to set the remote migrate feature.
func (s *Server) setJetStreamMigrateOnRemoteLeaf() {
	s.mu.Lock()
	for cfg := range s.leafRemoteCfgs {
		cfg.JetStreamClusterMigrate = true
	}
	s.mu.Unlock()
}

// Helper to set the remote migrate feature.
func (s *Server) setJetStreamMigrateOnRemoteLeafWithDelay(delay time.Duration) {
	s.mu.Lock()
	for cfg := range s.leafRemoteCfgs {
		cfg.JetStreamClusterMigrate = true
		cfg.JetStreamClusterMigrateDelay = delay
	}
	s.mu.Unlock()
}

// Will add in the mapping for the account to each server.
func (c *cluster) addSubjectMapping(account, src, dest string) {
	c.t.Helper()

	for _, s := range c.servers {
		if s.ClusterName() != c.name {
			continue
		}
		acc, err := s.LookupAccount(account)
		if err != nil {
			c.t.Fatalf("Unexpected error on %v: %v", s, err)
		}
		if err := acc.AddMapping(src, dest); err != nil {
			c.t.Fatalf("Error adding mapping: %v", err)
		}
	}
	// Make sure interest propagates.
	time.Sleep(200 * time.Millisecond)
}

// Adjust limits for the given account.
func (c *cluster) updateLimits(account string, newLimits map[string]JetStreamAccountLimits) {
	c.t.Helper()
	for _, s := range c.servers {
		acc, err := s.LookupAccount(account)
		if err != nil {
			c.t.Fatalf("Unexpected error: %v", err)
		}
		if err := acc.UpdateJetStreamLimits(newLimits); err != nil {
			c.t.Fatalf("Unexpected error: %v", err)
		}
	}
}

// Hack for staticcheck
var skip = func(t *testing.T) {
	t.SkipNow()
}

func jsClientConnect(t testing.TB, s *Server, opts ...nats.Option) (*nats.Conn, nats.JetStreamContext) {
	t.Helper()
	nc, err := nats.Connect(s.ClientURL(), opts...)
	if err != nil {
		t.Fatalf("Failed to create client: %v", err)
	}
	js, err := nc.JetStream(nats.MaxWait(10 * time.Second))
	if err != nil {
		t.Fatalf("Unexpected error getting JetStream context: %v", err)
	}
	return nc, js
}

func jsClientConnectEx(t testing.TB, s *Server, jsOpts []nats.JSOpt, opts ...nats.Option) (*nats.Conn, nats.JetStreamContext) {
	t.Helper()
	nc, err := nats.Connect(s.ClientURL(), opts...)
	if err != nil {
		t.Fatalf("Failed to create client: %v", err)
	}
	jo := []nats.JSOpt{nats.MaxWait(10 * time.Second)}
	if len(jsOpts) > 0 {
		jo = append(jo, jsOpts...)
	}
	js, err := nc.JetStream(jo...)
	if err != nil {
		t.Fatalf("Unexpected error getting JetStream context: %v", err)
	}
	return nc, js
}

func jsClientConnectNewAPI(t testing.TB, s *Server, opts ...nats.Option) (*nats.Conn, jetstream.JetStream) {
	t.Helper()
	nc, err := nats.Connect(s.ClientURL(), opts...)
	if err != nil {
		t.Fatalf("Failed to create client: %v", err)
	}
	js, err := jetstream.New(nc, jetstream.WithDefaultTimeout(10*time.Second))
	if err != nil {
		t.Fatalf("Unexpected error getting JetStream context: %v", err)
	}
	return nc, js
}

func jsClientConnectURL(t testing.TB, url string, opts ...nats.Option) (*nats.Conn, nats.JetStreamContext) {
	t.Helper()

	nc, err := nats.Connect(url, opts...)
	if err != nil {
		t.Fatalf("Failed to create client: %v", err)
	}
	js, err := nc.JetStream(nats.MaxWait(10 * time.Second))
	if err != nil {
		t.Fatalf("Unexpected error getting JetStream context: %v", err)
	}
	return nc, js
}

// jsStreamCreate is for sending a stream create for fields that nats.go does not know about yet.
func jsStreamCreate(t testing.TB, nc *nats.Conn, cfg *StreamConfig) (*StreamConfig, error) {
	t.Helper()

	j, err := json.Marshal(cfg)
	require_NoError(t, err)

	msg, err := nc.Request(fmt.Sprintf(JSApiStreamCreateT, cfg.Name), j, time.Second*3)
	require_NoError(t, err)

	var resp JSApiStreamUpdateResponse
	require_NoError(t, json.Unmarshal(msg.Data, &resp))

	if resp.Error != nil {
		return nil, resp.Error
	}

	require_NotNil(t, resp.StreamInfo)
	return &resp.Config, nil
}

// jsStreamUpdate is for sending a stream create for fields that nats.go does not know about yet.
func jsStreamUpdate(t testing.TB, nc *nats.Conn, cfg *StreamConfig) (*StreamConfig, error) {
	t.Helper()

	j, err := json.Marshal(cfg)
	require_NoError(t, err)

	msg, err := nc.Request(fmt.Sprintf(JSApiStreamUpdateT, cfg.Name), j, time.Second*3)
	require_NoError(t, err)

	var resp JSApiStreamUpdateResponse
	require_NoError(t, json.Unmarshal(msg.Data, &resp))

	if resp.Error != nil {
		return nil, resp.Error
	}

	require_NotNil(t, resp.StreamInfo)
	return &resp.Config, nil
}

// jsConsumerCreate is for sending a consumer create for fields that nats.go does not know about yet.
func jsConsumerCreate(t testing.TB, nc *nats.Conn, stream string, cfg ConsumerConfig, pedantic bool) (*ConsumerConfig, error) {
	t.Helper()

	j, err := json.Marshal(CreateConsumerRequest{Stream: stream, Config: cfg, Pedantic: pedantic})
	require_NoError(t, err)

	msg, err := nc.Request(fmt.Sprintf(JSApiDurableCreateT, stream, cfg.Durable), j, time.Second*3)
	require_NoError(t, err)

	var resp JSApiConsumerCreateResponse
	require_NoError(t, json.Unmarshal(msg.Data, &resp))

	if resp.Error != nil {
		return nil, resp.Error
	}

	require_NotNil(t, resp.ConsumerInfo)
	return resp.Config, nil
}

func checkSubsPending(t *testing.T, sub *nats.Subscription, numExpected int) {
	t.Helper()
	checkFor(t, 10*time.Second, 20*time.Millisecond, func() error {
		if nmsgs, _, err := sub.Pending(); err != nil || nmsgs != numExpected {
			return fmt.Errorf("Did not receive correct number of messages: %d vs %d", nmsgs, numExpected)
		}
		return nil
	})
}

func fetchMsgs(t *testing.T, sub *nats.Subscription, numExpected int, totalWait time.Duration) []*nats.Msg {
	t.Helper()
	result := make([]*nats.Msg, 0, numExpected)
	for start, count, wait := time.Now(), numExpected, totalWait; len(result) != numExpected; {
		msgs, err := sub.Fetch(count, nats.MaxWait(wait))
		if err != nil {
			antithesis.AssertUnreachable(t, "Fetch error", map[string]any{
				"error": err.Error(),
			})
			t.Fatal(err)
		}
		result = append(result, msgs...)
		count -= len(msgs)
		if wait = totalWait - time.Since(start); wait < 0 {
			break
		}
	}
	if len(result) != numExpected {
		antithesis.AssertUnreachable(t, "Unexpected fetch messages count", map[string]any{
			"expected": numExpected,
			"actual":   len(result),
		})
		t.Fatalf("Unexpected msg count, got %d, want %d", len(result), numExpected)
	}
	return result
}

func (c *cluster) restartServer(rs *Server) *Server {
	c.t.Helper()
	index := -1
	var opts *Options
	for i, s := range c.servers {
		if s == rs {
			index = i
			break
		}
	}
	if index < 0 {
		c.t.Fatalf("Could not find server %v to restart", rs)
	}
	opts = c.opts[index]
	s, o := RunServerWithConfig(opts.ConfigFile)
	c.servers[index] = s
	c.opts[index] = o
	return s
}

func (c *cluster) checkClusterFormed() {
	c.t.Helper()
	checkClusterFormed(c.t, c.servers...)
}

func (c *cluster) waitOnPeerCount(n int) {
	c.t.Helper()
	c.waitOnLeader()
	leader := c.leader()
	expires := time.Now().Add(30 * time.Second)
	// Make sure we have all peers, and take into account the meta leader could still change.
	for time.Now().Before(expires) {
		if leader = c.leader(); leader == nil {
			time.Sleep(50 * time.Millisecond)
			continue
		}
		if len(leader.JetStreamClusterPeers()) == n {
			return
		}
		time.Sleep(100 * time.Millisecond)
	}
	antithesis.AssertUnreachable(c.t, "Timeout in cluster.waitOnPeerCount", map[string]any{
		"cluster": c.name,
	})
	c.t.Fatalf("Expected a cluster peer count of %d, got %d", n, len(leader.JetStreamClusterPeers()))
}

func (c *cluster) waitOnConsumerLeader(account, stream, consumer string) {
	c.t.Helper()
	expires := time.Now().Add(30 * time.Second)
	for time.Now().Before(expires) {
		leader := c.consumerLeader(account, stream, consumer)
		if leader == nil {
			time.Sleep(100 * time.Millisecond)
			continue
		}
		// Also wait for the consumer leader to finish any scales/moves.
		js := leader.getJetStream()
		js.mu.RLock()
		ca := js.consumerAssignment(account, stream, consumer)
		wait := ca == nil || (ca.Group != nil && ca.Group.Desired != nil)
		js.mu.RUnlock()
		if wait {
			time.Sleep(100 * time.Millisecond)
			continue
		}
		time.Sleep(200 * time.Millisecond)
		return
	}
	antithesis.AssertUnreachable(c.t, "Timeout in cluster.waitOnConsumerLeader", map[string]any{
		"cluster":  c.name,
		"account":  account,
		"stream":   stream,
		"consumer": consumer,
	})
	c.t.Fatalf("Expected a consumer leader for %q %q %q, got none", account, stream, consumer)
}

func (c *cluster) consumerLeader(account, stream, consumer string) *Server {
	c.t.Helper()
	for _, s := range c.servers {
		if s.JetStreamIsConsumerLeader(account, stream, consumer) {
			return s
		}
	}
	return nil
}

func (c *cluster) randomNonConsumerLeader(account, stream, consumer string) *Server {
	c.t.Helper()
	for _, s := range c.servers {
		if !s.JetStreamIsConsumerLeader(account, stream, consumer) {
			return s
		}
	}
	return nil
}

// stepDownStreamLeader steps the stream leader down to the given server if it's not equal.
func (c *cluster) stepDownStreamLeader(nc *nats.Conn, account, stream string, s *Server) {
	c.t.Helper()
	req, err := json.Marshal(JSApiLeaderStepdownRequest{Placement: &Placement{Preferred: s.Name()}})
	require_NoError(c.t, err)
	// The leader skips the preferred peer if it hasn't heard from it yet, retry until it's picked.
	checkFor(c.t, 10*time.Second, 100*time.Millisecond, func() error {
		c.waitOnStreamLeader(account, stream)
		if sl := c.streamLeader(account, stream); sl == s {
			return nil
		}
		_, err = nc.Request(fmt.Sprintf(JSApiStreamLeaderStepDownT, stream), req, 5*time.Second)
		require_NoError(c.t, err)
		c.waitOnStreamLeader(account, stream)
		if sl := c.streamLeader(account, stream); sl != s {
			return fmt.Errorf("expected stream leader %s, got %s", s, sl)
		}
		return nil
	})
}

// stepDownConsumerLeader steps the consumer leader down to the given server if it's not equal.
func (c *cluster) stepDownConsumerLeader(nc *nats.Conn, account, stream, consumer string, s *Server) {
	c.t.Helper()
	req, err := json.Marshal(JSApiLeaderStepdownRequest{Placement: &Placement{Preferred: s.Name()}})
	require_NoError(c.t, err)
	// The leader skips the preferred peer if it hasn't heard from it yet, retry until it's picked.
	checkFor(c.t, 10*time.Second, 100*time.Millisecond, func() error {
		c.waitOnConsumerLeader(account, stream, consumer)
		if cl := c.consumerLeader(account, stream, consumer); cl == s {
			return nil
		}
		_, err = nc.Request(fmt.Sprintf(JSApiConsumerLeaderStepDownT, stream, consumer), req, 5*time.Second)
		require_NoError(c.t, err)
		c.waitOnConsumerLeader(account, stream, consumer)
		if cl := c.consumerLeader(account, stream, consumer); cl != s {
			return fmt.Errorf("expected consumer leader %s, got %s", s, cl)
		}
		return nil
	})
}

func (c *cluster) waitOnStreamLeader(account, stream string) {
	c.t.Helper()
	expires := time.Now().Add(30 * time.Second)
	for time.Now().Before(expires) {
		leader := c.streamLeader(account, stream)
		if leader == nil {
			time.Sleep(100 * time.Millisecond)
			continue
		}
		// Also wait for the stream leader to finish any scales/moves.
		js := leader.getJetStream()
		js.mu.RLock()
		sa := js.streamAssignment(account, stream)
		wait := sa == nil || (sa.Group != nil && sa.Group.Desired != nil)
		js.mu.RUnlock()
		if wait {
			time.Sleep(100 * time.Millisecond)
			continue
		}
		time.Sleep(200 * time.Millisecond)
		return
	}
	antithesis.AssertUnreachable(c.t, "Timeout in cluster.waitOnStreamLeader", map[string]any{
		"cluster": c.name,
		"account": account,
		"stream":  stream,
	})
	c.t.Fatalf("Expected a stream leader for %q %q, got none", account, stream)
}

func (c *cluster) randomNonStreamLeader(account, stream string) *Server {
	c.t.Helper()
	for _, s := range c.servers {
		if s.JetStreamIsStreamAssigned(account, stream) && !s.JetStreamIsStreamLeader(account, stream) {
			return s
		}
	}
	return nil
}

func (c *cluster) streamLeader(account, stream string) *Server {
	c.t.Helper()
	for _, s := range c.servers {
		if s.JetStreamIsStreamLeader(account, stream) {
			return s
		}
	}
	return nil
}

func (c *cluster) waitOnStreamCurrent(s *Server, account, stream string) {
	c.t.Helper()
	expires := time.Now().Add(30 * time.Second)
	for time.Now().Before(expires) {
		if s.JetStreamIsStreamCurrent(account, stream) {
			time.Sleep(100 * time.Millisecond)
			return
		}
		time.Sleep(100 * time.Millisecond)
	}
	antithesis.AssertUnreachable(c.t, "Timeout in cluster.waitOnStreamCurrent", map[string]any{
		"cluster": c.name,
		"account": account,
		"stream":  stream,
	})
	c.t.Fatalf("Expected server %q to eventually be current for stream %q", s, stream)
}

func (c *cluster) waitOnServerHealthz(s *Server) {
	c.t.Helper()
	expires := time.Now().Add(30 * time.Second)
	for time.Now().Before(expires) {
		hs := s.healthz(nil)
		if hs.Status == "ok" && hs.Error == _EMPTY_ {
			return
		}
		time.Sleep(100 * time.Millisecond)
	}
	antithesis.AssertUnreachable(c.t, "Timeout in cluster.waitOnServerHealthz", map[string]any{
		"cluster": c.name,
		"server":  s.Name(),
	})
	c.t.Fatalf("Expected server %q to eventually return healthz 'ok', but got %q", s, s.healthz(nil).Error)
}

func (c *cluster) waitOnServerCurrent(s *Server) {
	c.t.Helper()
	expires := time.Now().Add(30 * time.Second)
	for time.Now().Before(expires) {
		time.Sleep(100 * time.Millisecond)
		if !s.JetStreamEnabled() || s.JetStreamIsCurrent() {
			return
		}
	}
	antithesis.AssertUnreachable(c.t, "Timeout in cluster.waitOnServerCurrent", map[string]any{
		"cluster": c.name,
		"server":  s.Name(),
	})
	c.t.Fatalf("Expected server %q to eventually be current", s)
}

func (c *cluster) waitOnAllCurrent() {
	c.t.Helper()
	// Callers use this to wait for a proposal they just made to be applied,
	// so give it a chance to be committed before checking for currency.
	time.Sleep(100 * time.Millisecond)
	expires := time.Now().Add(30 * time.Second)
	for time.Now().Before(expires) {
		current := true
		for _, s := range c.servers {
			if s.JetStreamEnabled() && !s.JetStreamIsCurrent() {
				current = false
				break
			}
		}
		if current {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	antithesis.AssertUnreachable(c.t, "Timeout in cluster.waitOnAllCurrent", map[string]any{
		"cluster": c.name,
	})
	c.t.Fatalf("Expected all servers in cluster %q to eventually be current", c.name)
}

func (c *cluster) serverByName(sname string) *Server {
	for _, s := range c.servers {
		if s.Name() == sname {
			return s
		}
	}
	return nil
}

func (c *cluster) randomNonLeader() *Server {
	// range should randomize.. but..
	for _, s := range c.servers {
		if s.Running() && !s.JetStreamIsLeader() {
			return s
		}
	}
	return nil
}

func (c *cluster) leader() *Server {
	for _, s := range c.servers {
		if s.JetStreamIsLeader() {
			// The upper layer is only signaled asynchronously, so also require the meta
			// group to still be leader. Otherwise, a leader that just stepped down could
			// still be returned until it processes the leader change.
			if js := s.getJetStream(); js != nil {
				if meta := js.getMetaGroup(); meta != nil && meta.Leader() {
					return s
				}
			}
		}
	}
	return nil
}

func (c *cluster) expectNoLeader() {
	c.t.Helper()
	expires := time.Now().Add(maxElectionTimeout)
	for time.Now().Before(expires) {
		if c.leader() == nil {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	antithesis.AssertUnreachable(c.t, "Timeout in cluster.expectNoLeader", map[string]any{
		"cluster": c.name,
	})
	c.t.Fatalf("Expected no leader but have one")
}

func (c *cluster) waitOnLeader() {
	c.t.Helper()
	expires := time.Now().Add(40 * time.Second)
	for time.Now().Before(expires) {
		if leader := c.leader(); leader != nil {
			time.Sleep(100 * time.Millisecond)
			return
		}
		time.Sleep(10 * time.Millisecond)
	}

	antithesis.AssertUnreachable(c.t, "Timeout in cluster.waitOnLeader", map[string]any{
		"cluster": c.name,
	})
	c.t.Fatalf("Expected a cluster leader, got none")
}

func (c *cluster) waitOnAccount(account string) {
	c.t.Helper()
	expires := time.Now().Add(40 * time.Second)
	for time.Now().Before(expires) {
		found := true
		for _, s := range c.servers {
			s.optsMu.RLock()
			wantJS := s.opts.JetStream
			s.optsMu.RUnlock()
			acc, err := s.fetchAccount(account)
			if found = found && err == nil && acc != nil; wantJS {
				found = found && acc.JetStreamEnabled()
			}
		}
		if found {
			return
		}
		time.Sleep(100 * time.Millisecond)
		continue
	}

	antithesis.AssertUnreachable(c.t, "Timeout in cluster.waitOnAccount", map[string]any{
		"account": account,
	})
	c.t.Fatalf("Expected account %q to exist but didn't", account)
}

// Helper function to check that a cluster is formed
func (c *cluster) waitOnClusterReady() {
	c.t.Helper()
	c.waitOnClusterReadyWithNumPeers(len(c.servers))
}

func (c *cluster) waitOnClusterReadyWithNumPeers(numPeersExpected int) {
	c.t.Helper()
	var leader *Server
	expires := time.Now().Add(40 * time.Second)
	// Make sure we have all peers, and take into account the meta leader could still change.
	for time.Now().Before(expires) {
		if leader = c.leader(); leader == nil {
			time.Sleep(50 * time.Millisecond)
			continue
		}
		if len(leader.JetStreamClusterPeers()) == numPeersExpected {
			current := true
			for _, s := range c.servers {
				if s.Running() && s.JetStreamEnabled() && !s.JetStreamIsCurrent() {
					current = false
					break
				}
			}
			if current {
				return
			}
		}
		time.Sleep(10 * time.Millisecond)
	}

	if leader == nil {
		c.shutdown()
		antithesis.AssertUnreachable(c.t, "Timeout in cluster.waitOnClusterReadyWithNumPeers (1)", map[string]any{
			"cluster": c.name,
		})
		c.t.Fatalf("Failed to elect a meta-leader")
	}

	peersSeen := len(leader.JetStreamClusterPeers())
	c.shutdown()

	if leader == nil {
		antithesis.AssertUnreachable(c.t, "Timeout in cluster.waitOnClusterReadyWithNumPeers (2)", map[string]any{
			"cluster": c.name,
		})
		c.t.Fatalf("Expected a cluster leader and fully formed cluster, no leader")
	} else {
		antithesis.AssertUnreachable(c.t, "Timeout in cluster.waitOnClusterReadyWithNumPeers (3)", map[string]any{
			"cluster":        c.name,
			"peers_expected": numPeersExpected,
			"peers_seen":     peersSeen,
		})
		c.t.Fatalf("Expected a fully formed cluster, only %d of %d peers seen", peersSeen, numPeersExpected)
	}
}

// Helper function to remove JetStream from a server.
func (c *cluster) removeJetStream(s *Server) {
	c.t.Helper()
	index := -1
	for i, cs := range c.servers {
		if cs == s {
			index = i
			break
		}
	}
	cf := c.opts[index].ConfigFile
	cb, _ := os.ReadFile(cf)
	var sb strings.Builder
	for _, l := range strings.Split(string(cb), "\n") {
		if !strings.HasPrefix(strings.TrimSpace(l), "jetstream") {
			sb.WriteString(l + "\n")
		}
	}
	if err := os.WriteFile(cf, []byte(sb.String()), 0644); err != nil {
		c.t.Fatalf("Error writing updated config file: %v", err)
	}
	if err := s.Reload(); err != nil {
		c.t.Fatalf("Error on server reload: %v", err)
	}
	time.Sleep(100 * time.Millisecond)
}

func (c *cluster) stopAll() {
	c.t.Helper()
	for _, s := range c.servers {
		s.Shutdown()
	}
	for _, s := range c.servers {
		s.WaitForShutdown()
	}
}

func (c *cluster) restartAll() {
	c.t.Helper()
	for i, s := range c.servers {
		if !s.Running() {
			opts := c.opts[i]
			s, o := RunServerWithConfig(opts.ConfigFile)
			c.servers[i] = s
			c.opts[i] = o
		}
	}
	c.checkClusterFormed()
	c.campaignMetaImmediately()
	c.waitOnClusterReady()
}

func (c *cluster) lameDuckRestartAll() {
	c.t.Helper()
	for _, s := range c.servers {
		s.lameDuckMode()
	}
	for i, s := range c.servers {
		s.WaitForShutdown()
		if !s.Running() {
			opts := c.opts[i]
			s, o := RunServerWithConfig(opts.ConfigFile)
			c.servers[i] = s
			c.opts[i] = o
		}
	}
	c.checkClusterFormed()
	c.campaignMetaImmediately()
	c.waitOnClusterReady()
}

func (c *cluster) restartAllSamePorts() {
	c.t.Helper()
	for i, s := range c.servers {
		if !s.Running() {
			opts := c.opts[i]
			s := RunServer(opts)
			c.servers[i] = s
		}
	}
	c.checkClusterFormed()
	c.campaignMetaImmediately()
	c.waitOnClusterReady()
}

func (c *cluster) totalSubs() (total int) {
	c.t.Helper()
	for _, s := range c.servers {
		total += int(s.NumSubscriptions())
	}
	return total
}

func (c *cluster) stableTotalSubs() (total int) {
	nsubs := -1
	checkFor(c.t, 2*time.Second, 250*time.Millisecond, func() error {
		subs := c.totalSubs()
		if subs == nsubs {
			return nil
		}
		nsubs = subs
		return fmt.Errorf("Still stabilizing")
	})
	return nsubs

}

func addStreamPedanticWithError(t *testing.T, nc *nats.Conn, cfg *StreamConfigRequest) (*StreamInfo, *ApiError) {
	t.Helper()
	req, err := json.Marshal(cfg)
	require_NoError(t, err)
	rmsg, err := nc.Request(fmt.Sprintf(JSApiStreamCreateT, cfg.Name), req, 5*time.Second)
	require_NoError(t, err)
	var resp JSApiStreamCreateResponse
	err = json.Unmarshal(rmsg.Data, &resp)
	require_NoError(t, err)
	if resp.Type != JSApiStreamCreateResponseType {
		t.Fatalf("Invalid response type %s expected %s", resp.Type, JSApiStreamCreateResponseType)
	}
	return resp.StreamInfo, resp.Error
}

func updateStreamPedanticWithError(t *testing.T, nc *nats.Conn, cfg *StreamConfigRequest) (*StreamInfo, *ApiError) {
	t.Helper()
	req, err := json.Marshal(cfg)
	require_NoError(t, err)
	rmsg, err := nc.Request(fmt.Sprintf(JSApiStreamUpdateT, cfg.Name), req, time.Second)
	require_NoError(t, err)
	var resp JSApiStreamCreateResponse
	err = json.Unmarshal(rmsg.Data, &resp)
	require_NoError(t, err)
	if resp.Type != JSApiStreamUpdateResponseType {
		t.Fatalf("Invalid response type %s expected %s", resp.Type, JSApiStreamUpdateResponseType)
	}
	return resp.StreamInfo, resp.Error
}

func addStream(t *testing.T, nc *nats.Conn, cfg *StreamConfig) *StreamInfo {
	t.Helper()
	si, err := addStreamWithError(t, nc, cfg)
	if err != nil {
		t.Fatalf("Unexpected error: %+v", err)
	}
	return si
}

func addStreamWithError(t *testing.T, nc *nats.Conn, cfg *StreamConfig) (*StreamInfo, *ApiError) {
	t.Helper()
	req, err := json.Marshal(cfg)
	require_NoError(t, err)
	rmsg, err := nc.Request(fmt.Sprintf(JSApiStreamCreateT, cfg.Name), req, 5*time.Second)
	require_NoError(t, err)
	var resp JSApiStreamCreateResponse
	err = json.Unmarshal(rmsg.Data, &resp)
	require_NoError(t, err)
	if resp.Type != JSApiStreamCreateResponseType {
		t.Fatalf("Invalid response type %s expected %s", resp.Type, JSApiStreamCreateResponseType)
	}
	return resp.StreamInfo, resp.Error
}

func updateStream(t *testing.T, nc *nats.Conn, cfg *StreamConfig) *StreamInfo {
	t.Helper()
	req, err := json.Marshal(cfg)
	require_NoError(t, err)
	rmsg, err := nc.Request(fmt.Sprintf(JSApiStreamUpdateT, cfg.Name), req, time.Second)
	require_NoError(t, err)
	var resp JSApiStreamCreateResponse
	err = json.Unmarshal(rmsg.Data, &resp)
	require_NoError(t, err)
	if resp.Type != JSApiStreamUpdateResponseType {
		t.Fatalf("Invalid response type %s expected %s", resp.Type, JSApiStreamUpdateResponseType)
	}
	if resp.Error != nil {
		t.Fatalf("Unexpected error: %+v", resp.Error)
	}
	return resp.StreamInfo
}

// setInActiveDeleteThreshold sets the delete threshold for how long to wait
// before deleting an inactive consumer.
func (o *consumer) setInActiveDeleteThreshold(dthresh time.Duration) error {
	o.mu.Lock()
	defer o.mu.Unlock()

	deleteWasRunning := o.dtmr != nil
	stopAndClearTimer(&o.dtmr)
	// Do not add jitter if set via here.
	o.dthresh = dthresh
	if deleteWasRunning {
		o.dtmr = time.AfterFunc(o.dthresh, func() { o.deleteNotActive() })
	}
	return nil
}

// Net Proxy - For introducing RTT and BW constraints.
type netProxy struct {
	sync.RWMutex
	listener net.Listener
	conns    []net.Conn
	rtt      time.Duration
	up       int
	down     int
	url      string
	surl     string
	port     int
}

func newNetProxy(rtt time.Duration, upRate, downRate int, serverURL string) *netProxy {
	return createNetProxy(rtt, upRate, downRate, serverURL, true)
}

func createNetProxy(rtt time.Duration, upRate, downRate int, serverURL string, start bool) *netProxy {
	hp := net.JoinHostPort("127.0.0.1", "0")
	l, e := net.Listen("tcp", hp)
	if e != nil {
		panic(fmt.Sprintf("Error listening on port: %s, %q", hp, e))
	}
	u, err := url.Parse(serverURL)
	if err != nil {
		panic(fmt.Sprintf("Could not parse server URL: %v", err))
	}

	var clientURL string
	port := l.Addr().(*net.TCPAddr).Port
	if u.User != nil {
		clientURL = fmt.Sprintf("nats://%v@127.0.0.1:%d", u.User, port)
	} else {
		clientURL = fmt.Sprintf("nats://127.0.0.1:%d", port)
	}

	proxy := &netProxy{
		listener: l,
		rtt:      rtt,
		up:       upRate,
		down:     downRate,
		url:      clientURL,
		surl:     serverURL,
		port:     port,
	}
	if start {
		proxy.start()
	}
	return proxy
}

func (np *netProxy) start() {
	u, err := url.Parse(np.surl)
	if err != nil {
		panic(fmt.Sprintf("Could not parse server URL: %v", err))
	}
	host := u.Host

	// Check if this is restart.
	// We nil out listener on stop()
	if np.listener == nil && np.port != 0 {
		hp := net.JoinHostPort("127.0.0.1", fmt.Sprintf("%d", np.port))
		np.listener, err = net.Listen("tcp", hp)
		if err != nil {
			panic(fmt.Sprintf("Error listening on port: %s, %q", hp, err))
		}
	}

	go func() {
		for {
			client, err := np.listener.Accept()
			if err != nil {
				return
			}
			server, err := net.DialTimeout("tcp", host, time.Second)
			if err != nil {
				continue
			}
			np.conns = append(np.conns, client, server)
			go np.loop(np.up, client, server)
			go np.loop(np.down, server, client)
		}
	}()
}

func (np *netProxy) clientURL() string {
	return np.url
}

func (np *netProxy) routeURL() string {
	return strings.Replace(np.url, "nats", "nats-route", 1)
}

func (np *netProxy) leafURL() string {
	return strings.Replace(np.url, "nats", "nats-leaf", 1)
}

func (np *netProxy) loop(tbw int, r, w net.Conn) {
	const rbl = 8192
	var buf [rbl]byte
	ctx := context.Background()

	rl := rate.NewLimiter(rate.Limit(tbw), rbl)

	for {
		n, err := r.Read(buf[:])
		if err != nil {
			w.Close()
			return
		}
		// RTT delays
		np.RLock()
		delay := np.rtt / 2
		np.RUnlock()
		if delay > 0 {
			time.Sleep(delay)
		}
		if err := rl.WaitN(ctx, n); err != nil {
			return
		}
		if _, err = w.Write(buf[:n]); err != nil {
			r.Close()
			return
		}
	}
}

func (np *netProxy) updateRTT(rtt time.Duration) {
	np.Lock()
	np.rtt = rtt
	np.Unlock()
}

func (np *netProxy) stop() {
	if np.listener != nil {
		np.listener.Close()
		np.listener = nil
		for _, c := range np.conns {
			c.Close()
		}
	}
}

// Bitset, aka bitvector, allows tracking of large number of bits efficiently
type bitset struct {
	// Bit map storage
	bitmap []uint8
	// Number of bits currently set to 1
	currentCount uint64
	// Number of bits stored
	size uint64
}

func NewBitset(size uint64) *bitset {
	byteSize := (size + 7) / 8 //Round up to the nearest byte

	return &bitset{
		bitmap:       make([]uint8, int(byteSize)),
		size:         size,
		currentCount: 0,
	}
}

func (b *bitset) get(index uint64) bool {
	if index >= b.size {
		panic(fmt.Sprintf("Index %d out of bounds, size %d", index, b.size))
	}
	byteIndex := index / 8
	bitIndex := uint(index % 8)
	bit := (b.bitmap[byteIndex] & (uint8(1) << bitIndex))
	return bit != 0
}

func (b *bitset) set(index uint64, value bool) {
	if index >= b.size {
		panic(fmt.Sprintf("Index %d out of bounds, size %d", index, b.size))
	}
	byteIndex := index / 8
	bitIndex := uint(index % 8)
	byteMask := uint8(1) << bitIndex
	isSet := (b.bitmap[byteIndex] & (uint8(1) << bitIndex)) != 0
	if value {
		b.bitmap[byteIndex] |= byteMask
		if !isSet {
			b.currentCount += 1
		}
	} else {
		b.bitmap[byteIndex] &= ^byteMask
		if isSet {
			b.currentCount -= 1
		}
	}
}

func (b *bitset) count() uint64 {
	return b.currentCount
}

func (b *bitset) String() string {
	const block = 8 // 8 bytes, 64 bits per line
	sb := strings.Builder{}

	sb.WriteString(fmt.Sprintf("Bits set: %d/%d\n", b.currentCount, b.size))
	for i := 0; i < len(b.bitmap); i++ {
		if i%block == 0 {
			if i > 0 {
				sb.WriteString("\n")
			}
			sb.WriteString(fmt.Sprintf("[%4d] ", i*8))
		}
		for j := uint8(0); j < 8; j++ {
			if b.bitmap[i]&(1<<j) > 0 {
				sb.WriteString("1")
			} else {
				sb.WriteString("0")
			}
		}
	}
	sb.WriteString("\n")
	return sb.String()
}

func copyDir(t *testing.T, dst, src string) error {
	t.Helper()
	srcFS := os.DirFS(src)
	return fs.WalkDir(srcFS, ".", func(p string, d os.DirEntry, err error) error {
		if err != nil {
			// The entry is gone, don't include it in the copy.
			if errors.Is(err, fs.ErrNotExist) {
				return nil
			}
			return err
		}
		// Skip stream purge directories.
		if d.IsDir() && (d.Name() == purgeDir || d.Name() == newMsgDir) {
			return fs.SkipDir
		}
		// Atomic writes use .tmp files that are renamed into place when complete.
		if strings.HasSuffix(d.Name(), blkTmpSuffix) {
			if d.IsDir() {
				return fs.SkipDir
			}
			return nil
		}
		newPath := path.Join(dst, p)
		if d.IsDir() {
			return os.MkdirAll(newPath, defaultDirPerms)
		}
		r, err := srcFS.Open(p)
		if err != nil {
			// The entry is gone, don't include it in the copy.
			if errors.Is(err, fs.ErrNotExist) {
				return nil
			}
			return err
		}
		defer r.Close()

		w, err := os.OpenFile(newPath, os.O_CREATE|os.O_WRONLY, defaultFilePerms)
		if err != nil {
			return err
		}
		defer w.Close()
		_, err = io.Copy(w, r)
		return err
	})
}

func getStreamDetails(t *testing.T, c *cluster, accountName, streamName string) *StreamDetail {
	t.Helper()
	srv := c.streamLeader(accountName, streamName)
	if srv == nil {
		return nil
	}
	jsz, err := srv.Jsz(&JSzOptions{Accounts: true, Streams: true, Consumer: true})
	require_NoError(t, err)
	for _, acc := range jsz.AccountDetails {
		if acc.Name == accountName {
			for _, stream := range acc.Streams {
				if stream.Name == streamName {
					return &stream
				}
			}
		}
	}
	t.Error("Could not find account details")
	return nil
}

func checkState(t *testing.T, c *cluster, accountName, streamName string) error {
	t.Helper()
	_, err := checkStateAndErr(t, c, accountName, streamName)
	return err
}

func checkStateAndErr(t *testing.T, c *cluster, accountName, streamName string) (StreamState, error) {
	t.Helper()

	leaderSrv := c.streamLeader(accountName, streamName)
	if leaderSrv == nil {
		return StreamState{}, fmt.Errorf("no leader server found for stream %q", streamName)
	}
	streamLeader := getStreamDetails(t, c, accountName, streamName)
	if streamLeader == nil {
		return StreamState{}, fmt.Errorf("no leader found for stream %q", streamName)
	}

	acc, err := leaderSrv.LookupAccount(accountName)
	require_NoError(t, err)
	mset, err := acc.lookupStream(streamName)
	require_NoError(t, err)

	cfgReplicas := mset.config().Replicas
	foundReplicas := 1 // We already know the leader.
	var errs []error
	for _, srv := range c.servers {
		if srv == leaderSrv {
			// Skip self
			continue
		}
		if srv.isShuttingDown() {
			// Skip if shutdown.
			for _, replica := range streamLeader.Cluster.Replicas {
				if replica.Name == srv.Name() {
					foundReplicas++
					break
				}
			}
			continue
		}
		acc, err = srv.LookupAccount(accountName)
		if err != nil {
			continue
		}
		mset, err = acc.lookupStream(streamName)
		if err != nil {
			continue
		}

		foundReplicas++
		state := mset.state()

		if state.Msgs != streamLeader.State.Msgs {
			err := fmt.Errorf("[%s] Leader %v has %d messages, Follower %v has %d messages",
				streamName, leaderSrv, streamLeader.State.Msgs,
				srv, state.Msgs,
			)
			errs = append(errs, err)
		}
		if state.FirstSeq != streamLeader.State.FirstSeq {
			err := fmt.Errorf("[%s] Leader %v FirstSeq is %d, Follower %v is at %d",
				streamName, leaderSrv, streamLeader.State.FirstSeq,
				srv, state.FirstSeq,
			)
			errs = append(errs, err)
		}
		if state.LastSeq != streamLeader.State.LastSeq {
			err := fmt.Errorf("[%s] Leader %v LastSeq is %d, Follower %v is at %d",
				streamName, leaderSrv, streamLeader.State.LastSeq,
				srv, state.LastSeq,
			)
			errs = append(errs, err)
		}
		if state.NumDeleted != streamLeader.State.NumDeleted {
			err := fmt.Errorf("[%s] Leader %v NumDeleted is %d, Follower %v is at %d\nSTATE_A: %+v\nSTATE_B: %+v\n",
				streamName, leaderSrv, streamLeader.State.NumDeleted,
				srv, state.NumDeleted, streamLeader.State, state,
			)
			errs = append(errs, err)
		}
	}
	if cfgReplicas != foundReplicas {
		err := fmt.Errorf("[%s] Expected %d Replicas, got %d\n", streamName, cfgReplicas, foundReplicas)
		errs = append(errs, err)
	}
	if len(errs) > 0 {
		return StreamState{}, errors.Join(errs...)
	}
	return streamLeader.State, nil
}

// Helpers for the meta leader peer reconciliation tests.
func metaStreamPeers(s *Server, account, stream string) []string {
	js := s.getJetStream()
	js.mu.RLock()
	defer js.mu.RUnlock()
	sa := js.streamAssignmentOrInflight(account, stream)
	if sa == nil {
		return nil
	}
	peers := copyStrings(sa.Group.Peers)
	slices.Sort(peers)
	return peers
}

func metaConsumerPeers(s *Server, account, stream, consumer string) []string {
	js := s.getJetStream()
	js.mu.RLock()
	defer js.mu.RUnlock()
	ca := js.consumerAssignmentOrInflight(account, stream, consumer)
	if ca == nil {
		return nil
	}
	peers := copyStrings(ca.Group.Peers)
	slices.Sort(peers)
	return peers
}

func performStreamBackup(t *testing.T, nc *nats.Conn, streamName string) (StreamConfig, StreamState, []byte) {
	t.Helper()
	endpoint := fmt.Sprintf(JSApiStreamSnapshotT, streamName)
	inbox := nc.NewRespInbox()

	sub, err := nc.SubscribeSync(inbox)
	require_NoError(t, err)

	resp, err := nc.RequestMsg(&nats.Msg{
		Subject: endpoint,
		Data:    fmt.Appendf(nil, `{"deliver_subject": %q}`, inbox),
	}, 5*time.Second)
	require_NoError(t, err)

	var body struct {
		StreamState  `json:"state"`
		StreamConfig `json:"config"`
	}
	require_NoError(t, json.Unmarshal(resp.Data, &body))

	buf := bytes.NewBuffer(nil)
	for {
		msg, err := sub.NextMsg(5 * time.Second)
		require_NoError(t, err)
		if msg.Reply != _EMPTY_ {
			require_NoError(t, msg.Respond(nil))
		}
		if len(msg.Data) == 0 {
			break
		}
		_, err = buf.Write(msg.Data)
		require_NoError(t, err)
	}
	return body.StreamConfig, body.StreamState, buf.Bytes()
}

func performStreamRestore(t *testing.T, nc *nats.Conn, sc StreamConfig, ss StreamState, archive []byte) bool {
	t.Helper()
	endpoint := fmt.Sprintf(JSApiStreamRestoreT, sc.Name)

	req, err := json.Marshal(struct {
		StreamState  `json:"state"`
		StreamConfig `json:"config"`
	}{
		StreamState:  ss,
		StreamConfig: sc,
	})
	require_NoError(t, err)

	resp, err := nc.RequestMsg(&nats.Msg{
		Subject: endpoint,
		Data:    req,
	}, 5*time.Second)
	require_NoError(t, err)

	var res struct {
		DeliverSubject string `json:"deliver_subject"`
	}
	require_NoError(t, json.Unmarshal(resp.Data, &res))
	if !IsValidLiteralSubject(res.DeliverSubject) {
		return false
	}

	reader := bytes.NewReader(archive)
	buf := make([]byte, 128*1024)
	for {
		n, err := reader.Read(buf)
		if err == io.EOF {
			break
		}
		require_NoError(t, err)

		_, err = nc.Request(res.DeliverSubject, buf[:n], time.Second)
		require_NoError(t, err)
	}

	_, err = nc.Request(res.DeliverSubject, nil, time.Second)
	require_NoError(t, err)
	return true
}

// publishAsync publishes a message asynchronously, retrying if we're stalled
// on async publishes.
func publishAsync(t testing.TB, js nats.JetStreamContext, subj string, msg []byte) nats.PubAckFuture {
	t.Helper()
	for {
		pubAck, err := js.PublishAsync(subj, msg)
		if err == nil {
			return pubAck
		}
		require_Error(t, err, nats.ErrTooManyStalledMsgs)
	}
}

// requireFileStoreStopped checks that the named filestore has already logged
// to l that it stopped.
func requireFileStoreStopped(t *testing.T, l *captureDebugLogger, name string) {
	t.Helper()
	stopped := fmt.Sprintf("Filestore [%s] Stopped", name)
	for {
		select {
		case msg := <-l.dbgCh:
			if msg == stopped {
				return
			}
		default:
			t.Fatalf("Filestore %q not stopped", name)
		}
	}
}
