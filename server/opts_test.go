// Copyright 2012-2026 The NATS Authors
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
	"crypto/tls"
	"encoding/json"
	"flag"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"reflect"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nats-io/jwt/v2"
	"github.com/nats-io/nats-server/v2/conf"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nkeys"
)

func checkOptionsEqual(t *testing.T, golden, opts *Options) {
	t.Helper()
	// Clone them so we can remove private fields that we don't
	// want to be compared.
	goldenClone := golden.Clone()
	goldenClone.inConfig, goldenClone.inCmdLine = nil, nil
	optsClone := opts.Clone()
	optsClone.inConfig, optsClone.inCmdLine = nil, nil
	if !reflect.DeepEqual(goldenClone, optsClone) {
		t.Fatalf("Options are incorrect.\nexpected: %+v\ngot: %+v", goldenClone, optsClone)
	}
}

func TestDefaultOptions(t *testing.T) {
	golden := &Options{
		Host:                DEFAULT_HOST,
		Port:                DEFAULT_PORT,
		MaxConn:             DEFAULT_MAX_CONNECTIONS,
		HTTPHost:            DEFAULT_HOST,
		PingInterval:        DEFAULT_PING_INTERVAL,
		MaxPingsOut:         DEFAULT_PING_MAX_OUT,
		TLSTimeout:          float64(TLS_TIMEOUT) / float64(time.Second),
		AuthTimeout:         float64(AUTH_TIMEOUT) / float64(time.Second),
		MaxControlLine:      MAX_CONTROL_LINE_SIZE,
		MaxPayload:          MAX_PAYLOAD_SIZE,
		MaxPending:          MAX_PENDING_SIZE,
		WriteDeadline:       DEFAULT_FLUSH_DEADLINE,
		MaxClosedClients:    DEFAULT_MAX_CLOSED_CLIENTS,
		LameDuckDuration:    DEFAULT_LAME_DUCK_DURATION,
		LameDuckGracePeriod: DEFAULT_LAME_DUCK_GRACE_PERIOD,
		LeafNode: LeafNodeOpts{
			ReconnectInterval: DEFAULT_LEAF_NODE_RECONNECT,
		},
		ConnectErrorReports:        DEFAULT_CONNECT_ERROR_REPORTS,
		ReconnectErrorReports:      DEFAULT_RECONNECT_ERROR_REPORTS,
		MaxTracedMsgLen:            0,
		JetStreamMaxMemory:         -1,
		JetStreamMaxStore:          -1,
		SyncInterval:               2 * time.Minute,
		JetStreamRequestQueueLimit: JSDefaultRequestQueueLimit,
		JetStreamInfoQueueLimit:    JSDefaultRequestQueueLimit,
		JetStreamConcurrentIOs:     defaultConcurrentIOs,
		JetStreamLimits: JSLimitOpts{
			DefaultMaxConsumers: JSDefaultMaxConsumersPerStream,
		},
	}

	opts := &Options{}
	setBaselineOptions(opts)

	checkOptionsEqual(t, golden, opts)
}

func TestOptions_RandomPort(t *testing.T) {
	opts := &Options{Port: RANDOM_PORT}
	setBaselineOptions(opts)

	if opts.Port != 0 {
		t.Fatalf("Process of options should have resolved random port to "+
			"zero.\nexpected: %d\ngot: %d", 0, opts.Port)
	}
}

func TestConfigFile(t *testing.T) {
	golden := &Options{
		ConfigFile:            "./configs/test.conf",
		ServerName:            "testing_server",
		Host:                  "127.0.0.1",
		Port:                  4242,
		Username:              "derek",
		Password:              "porkchop",
		AuthTimeout:           1.0,
		Debug:                 false,
		Trace:                 true,
		Logtime:               false,
		HTTPPort:              8222,
		HTTPBasePath:          "/nats",
		PidFile:               "/tmp/nats-server/nats-server.pid",
		ProfPort:              6543,
		Syslog:                true,
		RemoteSyslog:          "udp://foo.com:33",
		MaxControlLine:        2048,
		MaxPayload:            65536,
		MaxConn:               100,
		MaxSubs:               1000,
		MaxPending:            10000000,
		PingInterval:          60 * time.Second,
		MaxPingsOut:           3,
		WriteDeadline:         3 * time.Second,
		LameDuckDuration:      4 * time.Minute,
		ConnectErrorReports:   86400,
		ReconnectErrorReports: 5,
		Metadata:              map[string]string{"key1": "value1", "key2": "value2"},
		FeatureFlags:          map[string]bool{"feature": false, "fix": true, "revert_fix": true},
		configDigest:          "sha256:f10eacddb9ce83a6bdc79b42c851b9628d49cf8b3c6ba95b1ecf090307504948",
		authBlockDefined:      true,
	}

	opts, err := ProcessConfigFile("./configs/test.conf")
	if err != nil {
		t.Fatalf("Received an error reading config file: %v", err)
	}

	checkOptionsEqual(t, golden, opts)
}

func TestTLSConfigFile(t *testing.T) {
	golden := &Options{
		ConfigFile:       "./configs/tls.conf",
		Host:             "127.0.0.1",
		Port:             4443,
		Username:         "derek",
		Password:         "foo",
		AuthTimeout:      1.0,
		TLSTimeout:       2.0,
		authBlockDefined: true,
		configDigest:     "sha256:cec986d37d7c09c86916d1ba4990cea1b8dadd49c86f9f782b455b47d07c2ac8",
	}
	opts, err := ProcessConfigFile("./configs/tls.conf")
	if err != nil {
		t.Fatalf("Received an error reading config file: %v", err)
	}
	tlsConfig := opts.TLSConfig
	if tlsConfig == nil {
		t.Fatal("Expected opts.TLSConfig to be non-nil")
	}
	opts.TLSConfig = nil
	opts.tlsConfigOpts = nil
	checkOptionsEqual(t, golden, opts)

	// Now check TLSConfig a bit more closely
	// CipherSuites
	ciphers := defaultCipherSuites()
	if !reflect.DeepEqual(tlsConfig.CipherSuites, ciphers) {
		t.Fatalf("Got incorrect cipher suite list: [%+v]", tlsConfig.CipherSuites)
	}
	if tlsConfig.MinVersion != tls.VersionTLS12 {
		t.Fatalf("Expected MinVersion of 1.2 [%v], got [%v]", tls.VersionTLS12, tlsConfig.MinVersion)
	}
	// Verify hostname is correct in certificate
	if len(tlsConfig.Certificates) != 1 {
		t.Fatal("Expected 1 certificate")
	}
	cert := tlsConfig.Certificates[0].Leaf
	if err := cert.VerifyHostname("127.0.0.1"); err != nil {
		t.Fatalf("Could not verify hostname in certificate: %v", err)
	}

	// First make sure that we can't add insecure cipher suites accidentally.
	_, err = ProcessConfigFile("./configs/tls_insecure_ciphers.conf")
	if err == nil || !strings.Contains(err.Error(), "insecure") {
		t.Fatalf("Expected to receive insecure cipher error reading configuration file but didn't")
	}
	_, err = ProcessConfigFile("./configs/tls_insecure_ciphers_allowed.conf")
	if err != nil {
		t.Fatalf("Received an error reading config file: %v", err)
	}

	// Now test adding cipher suites.
	opts, err = ProcessConfigFile("./configs/tls_ciphers.conf")
	if err != nil {
		t.Fatalf("Received an error reading config file: %v", err)
	}
	tlsConfig = opts.TLSConfig
	if tlsConfig == nil {
		t.Fatal("Expected opts.TLSConfig to be non-nil")
	}

	// CipherSuites listed in the config - test all of them.
	ciphers = []uint16{
		tls.TLS_ECDHE_ECDSA_WITH_AES_128_CBC_SHA,
		tls.TLS_ECDHE_ECDSA_WITH_AES_256_CBC_SHA,
		tls.TLS_ECDHE_RSA_WITH_AES_128_CBC_SHA,
		tls.TLS_ECDHE_RSA_WITH_AES_256_CBC_SHA,
		tls.TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256,
		tls.TLS_ECDHE_ECDSA_WITH_AES_128_GCM_SHA256,
	}

	if !reflect.DeepEqual(tlsConfig.CipherSuites, ciphers) {
		t.Fatalf("Got incorrect cipher suite list: [%+v]", tlsConfig.CipherSuites)
	}

	// Test an unrecognized/bad cipher
	if _, err := ProcessConfigFile("./configs/tls_bad_cipher.conf"); err == nil {
		t.Fatal("Did not receive an error from a unrecognized cipher")
	}

	// Test an empty cipher entry in a config file.
	if _, err := ProcessConfigFile("./configs/tls_empty_cipher.conf"); err == nil {
		t.Fatal("Did not receive an error from empty cipher_suites")
	}

	// Test a curve preference from the config.
	curves := []tls.CurveID{
		tls.CurveP256,
	}

	// test on a file that  will load the curve preference defaults
	opts, err = ProcessConfigFile("./configs/tls_ciphers.conf")
	if err != nil {
		t.Fatalf("Received an error reading config file: %v", err)
	}

	if !reflect.DeepEqual(opts.TLSConfig.CurvePreferences, defaultCurvePreferences()) {
		t.Fatalf("Got incorrect curve preference list: [%+v]", tlsConfig.CurvePreferences)
	}

	// Test specifying a single curve preference
	opts, err = ProcessConfigFile("./configs/tls_curve_prefs.conf")
	if err != nil {
		t.Fatal("Did not receive an error from a unrecognized cipher.")
	}

	if !reflect.DeepEqual(opts.TLSConfig.CurvePreferences, curves) {
		t.Fatalf("Got incorrect cipher suite list: [%+v]", tlsConfig.CurvePreferences)
	}

	// Test an unrecognized/bad curve preference
	if _, err := ProcessConfigFile("./configs/tls_bad_curve_prefs.conf"); err == nil {
		t.Fatal("Did not receive an error from a unrecognized curve preference")
	}
	// Test an empty curve preference
	if _, err := ProcessConfigFile("./configs/tls_empty_curve_prefs.conf"); err == nil {
		t.Fatal("Did not receive an error from empty curve preferences")
	}
}

func TestMergeOverrides(t *testing.T) {
	golden := &Options{
		ConfigFile:     "./configs/test.conf",
		ServerName:     "testing_server",
		Host:           "127.0.0.1",
		Port:           2222,
		Username:       "derek",
		Password:       "porkchop",
		AuthTimeout:    1.0,
		Debug:          true,
		Trace:          true,
		Logtime:        false,
		HTTPPort:       DEFAULT_HTTP_PORT,
		HTTPBasePath:   DEFAULT_HTTP_BASE_PATH,
		PidFile:        "/tmp/nats-server/nats-server.pid",
		ProfPort:       6789,
		Syslog:         true,
		RemoteSyslog:   "udp://foo.com:33",
		MaxControlLine: 2048,
		MaxPayload:     65536,
		MaxConn:        100,
		MaxSubs:        1000,
		MaxPending:     10000000,
		PingInterval:   60 * time.Second,
		MaxPingsOut:    3,
		Cluster: ClusterOpts{
			NoAdvertise:    true,
			ConnectRetries: 2,
		},
		WriteDeadline:         3 * time.Second,
		LameDuckDuration:      4 * time.Minute,
		ConnectErrorReports:   86400,
		ReconnectErrorReports: 5,
		JetStream:             true,
		StoreDir:              "/store/dir",
		authBlockDefined:      true,
		Metadata:              map[string]string{"key1": "value1", "key2": "value2"},
		FeatureFlags:          map[string]bool{"feature": false, "fix": true, "revert_fix": true},
		configDigest:          "sha256:f10eacddb9ce83a6bdc79b42c851b9628d49cf8b3c6ba95b1ecf090307504948",
	}
	fopts, err := ProcessConfigFile("./configs/test.conf")
	if err != nil {
		t.Fatalf("Received an error reading config file: %v", err)
	}

	// Overrides via flags
	opts := &Options{
		Port:         2222,
		Password:     "porkchop",
		Debug:        true,
		HTTPPort:     DEFAULT_HTTP_PORT,
		HTTPBasePath: DEFAULT_HTTP_BASE_PATH,
		ProfPort:     6789,
		Cluster: ClusterOpts{
			NoAdvertise:    true,
			ConnectRetries: 2,
		},
		JetStream: true,
		StoreDir:  "/store/dir",
	}
	merged := MergeOptions(fopts, opts)

	checkOptionsEqual(t, golden, merged)
}

func TestRouteFlagOverride(t *testing.T) {
	routeFlag := "nats-route://ruser:top_secret@127.0.0.1:8246"
	rurl, _ := url.Parse(routeFlag)

	golden := &Options{
		ConfigFile: "./configs/srv_a.conf",
		Host:       "127.0.0.1",
		Port:       7222,
		Cluster: ClusterOpts{
			Name:        "abc",
			Host:        "127.0.0.1",
			Port:        7244,
			Username:    "ruser",
			Password:    "top_secret",
			AuthTimeout: 0.5,
		},
		Routes:       []*url.URL{rurl},
		RoutesStr:    routeFlag,
		configDigest: "sha256:fe3c13f82637723989a9bbd0ad6d064b95d48971666af440d4196d9c0d3af979",
	}

	fopts, err := ProcessConfigFile("./configs/srv_a.conf")
	if err != nil {
		t.Fatalf("Received an error reading config file: %v", err)
	}

	// Overrides via flags
	opts := &Options{
		RoutesStr: routeFlag,
	}
	merged := MergeOptions(fopts, opts)

	checkOptionsEqual(t, golden, merged)
}

func TestClusterFlagsOverride(t *testing.T) {
	routeFlag := "nats-route://ruser:top_secret@127.0.0.1:7246"
	rurl, _ := url.Parse(routeFlag)

	// In this test, we override the cluster listen string. Note that in
	// the golden options, the cluster other infos correspond to what
	// is recovered from the configuration file, this explains the
	// discrepency between ClusterListenStr and the rest.
	// The server would then process the ClusterListenStr override and
	// correctly override ClusterHost/ClustherPort/etc..
	golden := &Options{
		ConfigFile: "./configs/srv_a.conf",
		Host:       "127.0.0.1",
		Port:       7222,
		Cluster: ClusterOpts{
			Name:        "abc",
			Host:        "127.0.0.1",
			Port:        7244,
			ListenStr:   "nats://127.0.0.1:8224",
			Username:    "ruser",
			Password:    "top_secret",
			AuthTimeout: 0.5,
		},
		Routes:       []*url.URL{rurl},
		configDigest: "sha256:fe3c13f82637723989a9bbd0ad6d064b95d48971666af440d4196d9c0d3af979",
	}

	fopts, err := ProcessConfigFile("./configs/srv_a.conf")
	if err != nil {
		t.Fatalf("Received an error reading config file: %v", err)
	}

	// Overrides via flags
	opts := &Options{
		Cluster: ClusterOpts{
			ListenStr: "nats://127.0.0.1:8224",
		},
	}
	merged := MergeOptions(fopts, opts)

	checkOptionsEqual(t, golden, merged)
}

func TestRouteFlagOverrideWithMultiple(t *testing.T) {
	routeFlag := "nats-route://ruser:top_secret@127.0.0.1:8246, nats-route://ruser:top_secret@127.0.0.1:8266"
	rurls := RoutesFromStr(routeFlag)

	golden := &Options{
		ConfigFile: "./configs/srv_a.conf",
		Host:       "127.0.0.1",
		Port:       7222,
		Cluster: ClusterOpts{
			Host:        "127.0.0.1",
			Name:        "abc",
			Port:        7244,
			Username:    "ruser",
			Password:    "top_secret",
			AuthTimeout: 0.5,
		},
		Routes:       rurls,
		RoutesStr:    routeFlag,
		configDigest: "sha256:fe3c13f82637723989a9bbd0ad6d064b95d48971666af440d4196d9c0d3af979",
	}

	fopts, err := ProcessConfigFile("./configs/srv_a.conf")
	if err != nil {
		t.Fatalf("Received an error reading config file: %v", err)
	}

	// Overrides via flags
	opts := &Options{
		RoutesStr: routeFlag,
	}
	merged := MergeOptions(fopts, opts)

	checkOptionsEqual(t, golden, merged)
}

func TestDynamicPortOnListen(t *testing.T) {
	opts, err := ProcessConfigFile("./configs/listen-1.conf")
	if err != nil {
		t.Fatalf("Received an error reading config file: %v", err)
	}
	if opts.Port != -1 {
		t.Fatalf("Received incorrect port %v, expected -1", opts.Port)
	}
	if opts.HTTPPort != -1 {
		t.Fatalf("Received incorrect monitoring port %v, expected -1", opts.HTTPPort)
	}
	if opts.HTTPSPort != -1 {
		t.Fatalf("Received incorrect secure monitoring port %v, expected -1", opts.HTTPSPort)
	}
}

func TestListenConfig(t *testing.T) {
	opts, err := ProcessConfigFile("./configs/listen.conf")
	if err != nil {
		t.Fatalf("Received an error reading config file: %v", err)
	}
	setBaselineOptions(opts)

	// Normal clients
	host := "10.0.1.22"
	port := 4422
	monHost := "127.0.0.1"
	if opts.Host != host {
		t.Fatalf("Received incorrect host %q, expected %q", opts.Host, host)
	}
	if opts.HTTPHost != monHost {
		t.Fatalf("Received incorrect host %q, expected %q", opts.HTTPHost, monHost)
	}
	if opts.Port != port {
		t.Fatalf("Received incorrect port %v, expected %v", opts.Port, port)
	}

	// Clustering
	clusterHost := "127.0.0.1"
	clusterPort := 4244

	if opts.Cluster.Host != clusterHost {
		t.Fatalf("Received incorrect cluster host %q, expected %q", opts.Cluster.Host, clusterHost)
	}
	if opts.Cluster.Port != clusterPort {
		t.Fatalf("Received incorrect cluster port %v, expected %v", opts.Cluster.Port, clusterPort)
	}

	// HTTP
	httpHost := "127.0.0.1"
	httpPort := 8422

	if opts.HTTPHost != httpHost {
		t.Fatalf("Received incorrect http host %q, expected %q", opts.HTTPHost, httpHost)
	}
	if opts.HTTPPort != httpPort {
		t.Fatalf("Received incorrect http port %v, expected %v", opts.HTTPPort, httpPort)
	}

	// HTTPS
	httpsPort := 9443
	if opts.HTTPSPort != httpsPort {
		t.Fatalf("Received incorrect https port %v, expected %v", opts.HTTPSPort, httpsPort)
	}
}

func TestListenPortOnlyConfig(t *testing.T) {
	opts, err := ProcessConfigFile("./configs/listen_port.conf")
	if err != nil {
		t.Fatalf("Received an error reading config file: %v", err)
	}
	setBaselineOptions(opts)

	port := 8922

	if opts.Host != DEFAULT_HOST {
		t.Fatalf("Received incorrect host %q, expected %q", opts.Host, DEFAULT_HOST)
	}
	if opts.HTTPHost != DEFAULT_HOST {
		t.Fatalf("Received incorrect host %q, expected %q", opts.Host, DEFAULT_HOST)
	}
	if opts.Port != port {
		t.Fatalf("Received incorrect port %v, expected %v", opts.Port, port)
	}
}

func TestListenPortWithColonConfig(t *testing.T) {
	opts, err := ProcessConfigFile("./configs/listen_port_with_colon.conf")
	if err != nil {
		t.Fatalf("Received an error reading config file: %v", err)
	}
	setBaselineOptions(opts)

	port := 8922

	if opts.Host != DEFAULT_HOST {
		t.Fatalf("Received incorrect host %q, expected %q", opts.Host, DEFAULT_HOST)
	}
	if opts.HTTPHost != DEFAULT_HOST {
		t.Fatalf("Received incorrect host %q, expected %q", opts.Host, DEFAULT_HOST)
	}
	if opts.Port != port {
		t.Fatalf("Received incorrect port %v, expected %v", opts.Port, port)
	}
}

func TestListenMonitoringDefault(t *testing.T) {
	opts := &Options{
		Host: "10.0.1.22",
	}
	setBaselineOptions(opts)

	host := "10.0.1.22"
	if opts.Host != host {
		t.Fatalf("Received incorrect host %q, expected %q", opts.Host, host)
	}
	if opts.HTTPHost != host {
		t.Fatalf("Received incorrect host %q, expected %q", opts.Host, host)
	}
	if opts.Port != DEFAULT_PORT {
		t.Fatalf("Received incorrect port %v, expected %v", opts.Port, DEFAULT_PORT)
	}
}

func TestMultipleUsersConfig(t *testing.T) {
	opts, err := ProcessConfigFile("./configs/multiple_users.conf")
	if err != nil {
		t.Fatalf("Received an error reading config file: %v", err)
	}
	setBaselineOptions(opts)
}

// Test highly depends on contents of the config file listed below. Any changes to that file
// may very well break this test.
func TestAuthorizationConfig(t *testing.T) {
	opts, err := ProcessConfigFile("./configs/authorization.conf")
	if err != nil {
		t.Fatalf("Received an error reading config file: %v", err)
	}
	setBaselineOptions(opts)
	lu := len(opts.Users)
	if lu != 5 {
		t.Fatalf("Expected 5 users, got %d", lu)
	}
	// Build a map
	mu := make(map[string]*User)
	for _, u := range opts.Users {
		mu[u.Username] = u
	}

	// Alice
	alice, ok := mu["alice"]
	if !ok {
		t.Fatalf("Expected to see user Alice")
	}
	// Check for permissions details
	if alice.Permissions == nil {
		t.Fatalf("Expected Alice's permissions to be non-nil")
	}
	if alice.Permissions.Publish == nil {
		t.Fatalf("Expected Alice's publish permissions to be non-nil")
	}
	if len(alice.Permissions.Publish.Allow) != 1 {
		t.Fatalf("Expected Alice's publish permissions to have 1 element, got %d",
			len(alice.Permissions.Publish.Allow))
	}
	pubPerm := alice.Permissions.Publish.Allow[0]
	if pubPerm != "*" {
		t.Fatalf("Expected Alice's publish permissions to be '*', got %q", pubPerm)
	}
	if alice.Permissions.Subscribe == nil {
		t.Fatalf("Expected Alice's subscribe permissions to be non-nil")
	}
	if len(alice.Permissions.Subscribe.Allow) != 1 {
		t.Fatalf("Expected Alice's subscribe permissions to have 1 element, got %d",
			len(alice.Permissions.Subscribe.Allow))
	}
	subPerm := alice.Permissions.Subscribe.Allow[0]
	if subPerm != ">" {
		t.Fatalf("Expected Alice's subscribe permissions to be '>', got %q", subPerm)
	}

	// Bob
	bob, ok := mu["bob"]
	if !ok {
		t.Fatalf("Expected to see user Bob")
	}
	if bob.Permissions == nil {
		t.Fatalf("Expected Bob's permissions to be non-nil")
	}

	// Susan
	susan, ok := mu["susan"]
	if !ok {
		t.Fatalf("Expected to see user Susan")
	}
	if susan.Permissions == nil {
		t.Fatalf("Expected Susan's permissions to be non-nil")
	}
	// Check susan closely since she inherited the default permissions.
	if susan.Permissions == nil {
		t.Fatalf("Expected Susan's permissions to be non-nil")
	}
	if susan.Permissions.Publish != nil {
		t.Fatalf("Expected Susan's publish permissions to be nil")
	}
	if susan.Permissions.Subscribe == nil {
		t.Fatalf("Expected Susan's subscribe permissions to be non-nil")
	}
	if len(susan.Permissions.Subscribe.Allow) != 1 {
		t.Fatalf("Expected Susan's subscribe permissions to have 1 element, got %d",
			len(susan.Permissions.Subscribe.Allow))
	}
	subPerm = susan.Permissions.Subscribe.Allow[0]
	if subPerm != "PUBLIC.>" {
		t.Fatalf("Expected Susan's subscribe permissions to be 'PUBLIC.>', got %q", subPerm)
	}

	// Service A
	svca, ok := mu["svca"]
	if !ok {
		t.Fatalf("Expected to see user Service A")
	}
	if svca.Permissions == nil {
		t.Fatalf("Expected Service A's permissions to be non-nil")
	}
	if svca.Permissions.Subscribe == nil {
		t.Fatalf("Expected Service A's subscribe permissions to be non-nil")
	}
	if len(svca.Permissions.Subscribe.Allow) != 1 {
		t.Fatalf("Expected Service A's subscribe permissions to have 1 element, got %d",
			len(svca.Permissions.Subscribe.Allow))
	}
	subPerm = svca.Permissions.Subscribe.Allow[0]
	if subPerm != "my.service.req" {
		t.Fatalf("Expected Service A's subscribe permissions to be 'my.service.req', got %q", subPerm)
	}
	// We want allow_responses to essentially set deny all, or allow none in this case.
	if svca.Permissions.Publish == nil {
		t.Fatalf("Expected Service A's publish permissions to be non-nil")
	}
	if len(svca.Permissions.Publish.Allow) != 0 {
		t.Fatalf("Expected Service A's publish permissions to have no elements, got %d",
			len(svca.Permissions.Publish.Allow))
	}
	// We should have a ResponsePermission present with default values.
	if svca.Permissions.Response == nil {
		t.Fatalf("Expected Service A's response permissions to be non-nil")
	}
	if svca.Permissions.Response.MaxMsgs != DEFAULT_ALLOW_RESPONSE_MAX_MSGS {
		t.Fatalf("Expected Service A's response permissions of max msgs to be %d, got %d",
			DEFAULT_ALLOW_RESPONSE_MAX_MSGS, svca.Permissions.Response.MaxMsgs,
		)
	}
	if svca.Permissions.Response.Expires != DEFAULT_ALLOW_RESPONSE_EXPIRATION {
		t.Fatalf("Expected Service A's response permissions of expiration to be %v, got %v",
			DEFAULT_ALLOW_RESPONSE_EXPIRATION, svca.Permissions.Response.Expires,
		)
	}

	// Service B
	svcb, ok := mu["svcb"]
	if !ok {
		t.Fatalf("Expected to see user Service B")
	}
	if svcb.Permissions == nil {
		t.Fatalf("Expected Service B's permissions to be non-nil")
	}
	if svcb.Permissions.Subscribe == nil {
		t.Fatalf("Expected Service B's subscribe permissions to be non-nil")
	}
	if len(svcb.Permissions.Subscribe.Allow) != 1 {
		t.Fatalf("Expected Service B's subscribe permissions to have 1 element, got %d",
			len(svcb.Permissions.Subscribe.Allow))
	}
	subPerm = svcb.Permissions.Subscribe.Allow[0]
	if subPerm != "my.service.req" {
		t.Fatalf("Expected Service B's subscribe permissions to be 'my.service.req', got %q", subPerm)
	}
	// We want allow_responses to essentially set deny all, or allow none in this case.
	if svcb.Permissions.Publish == nil {
		t.Fatalf("Expected Service B's publish permissions to be non-nil")
	}
	if len(svcb.Permissions.Publish.Allow) != 0 {
		t.Fatalf("Expected Service B's publish permissions to have no elements, got %d",
			len(svcb.Permissions.Publish.Allow))
	}
	// We should have a ResponsePermission present with default values.
	if svcb.Permissions.Response == nil {
		t.Fatalf("Expected Service B's response permissions to be non-nil")
	}
	if svcb.Permissions.Response.MaxMsgs != 10 {
		t.Fatalf("Expected Service B's response permissions of max msgs to be %d, got %d",
			10, svcb.Permissions.Response.MaxMsgs,
		)
	}
	if svcb.Permissions.Response.Expires != time.Minute {
		t.Fatalf("Expected Service B's response permissions of expiration to be %v, got %v",
			time.Minute, svcb.Permissions.Response.Expires,
		)
	}
}

// Test highly depends on contents of the config file listed below. Any changes to that file
// may very well break this test.
func TestNewStyleAuthorizationConfig(t *testing.T) {
	opts, err := ProcessConfigFile("./configs/new_style_authorization.conf")
	if err != nil {
		t.Fatalf("Received an error reading config file: %v", err)
	}
	setBaselineOptions(opts)

	lu := len(opts.Users)
	if lu != 2 {
		t.Fatalf("Expected 2 users, got %d", lu)
	}
	// Build a map
	mu := make(map[string]*User)
	for _, u := range opts.Users {
		mu[u.Username] = u
	}
	// Alice
	alice, ok := mu["alice"]
	if !ok {
		t.Fatalf("Expected to see user Alice")
	}
	if alice.Permissions == nil {
		t.Fatalf("Expected Alice's permissions to be non-nil")
	}

	if alice.Permissions.Publish == nil {
		t.Fatalf("Expected Alice's publish permissions to be non-nil")
	}
	if len(alice.Permissions.Publish.Allow) != 3 {
		t.Fatalf("Expected Alice's allowed publish permissions to have 3 elements, got %d",
			len(alice.Permissions.Publish.Allow))
	}
	pubPerm := alice.Permissions.Publish.Allow[0]
	if pubPerm != "foo" {
		t.Fatalf("Expected Alice's first allowed publish permission to be 'foo', got %q", pubPerm)
	}
	pubPerm = alice.Permissions.Publish.Allow[1]
	if pubPerm != "bar" {
		t.Fatalf("Expected Alice's second allowed publish permission to be 'bar', got %q", pubPerm)
	}
	pubPerm = alice.Permissions.Publish.Allow[2]
	if pubPerm != "baz" {
		t.Fatalf("Expected Alice's third allowed publish permission to be 'baz', got %q", pubPerm)
	}
	if len(alice.Permissions.Publish.Deny) != 0 {
		t.Fatalf("Expected Alice's denied publish permissions to have 0 elements, got %d",
			len(alice.Permissions.Publish.Deny))
	}

	if alice.Permissions.Subscribe == nil {
		t.Fatalf("Expected Alice's subscribe permissions to be non-nil")
	}
	if len(alice.Permissions.Subscribe.Allow) != 0 {
		t.Fatalf("Expected Alice's allowed subscribe permissions to have 0 elements, got %d",
			len(alice.Permissions.Subscribe.Allow))
	}
	if len(alice.Permissions.Subscribe.Deny) != 1 {
		t.Fatalf("Expected Alice's denied subscribe permissions to have 1 element, got %d",
			len(alice.Permissions.Subscribe.Deny))
	}
	subPerm := alice.Permissions.Subscribe.Deny[0]
	if subPerm != "$SYS.>" {
		t.Fatalf("Expected Alice's only denied subscribe permission to be '$SYS.>', got %q", subPerm)
	}

	// Bob
	bob, ok := mu["bob"]
	if !ok {
		t.Fatalf("Expected to see user Bob")
	}
	if bob.Permissions == nil {
		t.Fatalf("Expected Bob's permissions to be non-nil")
	}

	if bob.Permissions.Publish == nil {
		t.Fatalf("Expected Bobs's publish permissions to be non-nil")
	}
	if len(bob.Permissions.Publish.Allow) != 1 {
		t.Fatalf("Expected Bob's allowed publish permissions to have 1 element, got %d",
			len(bob.Permissions.Publish.Allow))
	}
	pubPerm = bob.Permissions.Publish.Allow[0]
	if pubPerm != "$SYS.>" {
		t.Fatalf("Expected Bob's first allowed publish permission to be '$SYS.>', got %q", pubPerm)
	}
	if len(bob.Permissions.Publish.Deny) != 0 {
		t.Fatalf("Expected Bob's denied publish permissions to have 0 elements, got %d",
			len(bob.Permissions.Publish.Deny))
	}

	if bob.Permissions.Subscribe == nil {
		t.Fatalf("Expected Bob's subscribe permissions to be non-nil")
	}
	if len(bob.Permissions.Subscribe.Allow) != 0 {
		t.Fatalf("Expected Bob's allowed subscribe permissions to have 0 elements, got %d",
			len(bob.Permissions.Subscribe.Allow))
	}
	if len(bob.Permissions.Subscribe.Deny) != 3 {
		t.Fatalf("Expected Bobs's denied subscribe permissions to have 3 elements, got %d",
			len(bob.Permissions.Subscribe.Deny))
	}
	subPerm = bob.Permissions.Subscribe.Deny[0]
	if subPerm != "foo" {
		t.Fatalf("Expected Bobs's first denied subscribe permission to be 'foo', got %q", subPerm)
	}
	subPerm = bob.Permissions.Subscribe.Deny[1]
	if subPerm != "bar" {
		t.Fatalf("Expected Bobs's second denied subscribe permission to be 'bar', got %q", subPerm)
	}
	subPerm = bob.Permissions.Subscribe.Deny[2]
	if subPerm != "baz" {
		t.Fatalf("Expected Bobs's third denied subscribe permission to be 'baz', got %q", subPerm)
	}
}

// Test new nkey users
func TestNkeyUsersConfig(t *testing.T) {
	confFileName := createConfFile(t, []byte(`
    authorization {
      users = [
        {nkey: "UDKTV7HZVYJFJN64LLMYQBUR6MTNNYCDC3LAZH4VHURW3GZLL3FULBXV"}
        {nkey: "UA3C5TBZYK5GJQJRWPMU6NFY5JNAEVQB2V2TUZFZDHFJFUYVKTTUOFKZ"}
      ]
    }`))
	opts, err := ProcessConfigFile(confFileName)
	if err != nil {
		t.Fatalf("Received an error reading config file: %v", err)
	}
	lu := len(opts.Nkeys)
	if lu != 2 {
		t.Fatalf("Expected 2 nkey users, got %d", lu)
	}
}

// Test pinned certificates
func TestTlsPinnedCertificates(t *testing.T) {
	confFileName := createConfFile(t, []byte(`
	tls {
		cert_file: "./configs/certs/server.pem"
		key_file: "./configs/certs/key.pem"
		# Require a client certificate and map user id from certificate
		verify: true
		pinned_certs: ["7f83b1657ff1fc53b92dc18148a1d65dfc2d4b1fa3d677284addd200126d9069",
			"a8f407340dcc719864214b85ed96f98d16cbffa8f509d9fa4ca237b7bb3f9c32"]
	}
	cluster {
		port -1
		name cluster-hub
		tls {
			cert_file: "./configs/certs/server.pem"
			key_file: "./configs/certs/key.pem"
			# Require a client certificate and map user id from certificate
			verify: true
			pinned_certs: ["7f83b1657ff1fc53b92dc18148a1d65dfc2d4b1fa3d677284addd200126d9069",
				"a8f407340dcc719864214b85ed96f98d16cbffa8f509d9fa4ca237b7bb3f9c32"]
		}
	}
	leafnodes {
		port -1
		tls {
			cert_file: "./configs/certs/server.pem"
			key_file: "./configs/certs/key.pem"
			# Require a client certificate and map user id from certificate
			verify: true
			pinned_certs: ["7f83b1657ff1fc53b92dc18148a1d65dfc2d4b1fa3d677284addd200126d9069",
				"a8f407340dcc719864214b85ed96f98d16cbffa8f509d9fa4ca237b7bb3f9c32"]
		}
	}
	gateway {
		name: "A"
		port -1
		tls {
			cert_file: "./configs/certs/server.pem"
			key_file: "./configs/certs/key.pem"
			# Require a client certificate and map user id from certificate
			verify: true
			pinned_certs: ["7f83b1657ff1fc53b92dc18148a1d65dfc2d4b1fa3d677284addd200126d9069",
				"a8f407340dcc719864214b85ed96f98d16cbffa8f509d9fa4ca237b7bb3f9c32"]
		}
	}
	websocket {
		port -1
		tls {
			cert_file: "./configs/certs/server.pem"
			key_file: "./configs/certs/key.pem"
			# Require a client certificate and map user id from certificate
			verify: true
			pinned_certs: ["7f83b1657ff1fc53b92dc18148a1d65dfc2d4b1fa3d677284addd200126d9069",
				"a8f407340dcc719864214b85ed96f98d16cbffa8f509d9fa4ca237b7bb3f9c32"]
		}
	}
	mqtt {
		port -1
		tls {
			cert_file: "./configs/certs/server.pem"
			key_file: "./configs/certs/key.pem"
			# Require a client certificate and map user id from certificate
			verify: true
			pinned_certs: ["7f83b1657ff1fc53b92dc18148a1d65dfc2d4b1fa3d677284addd200126d9069",
				"a8f407340dcc719864214b85ed96f98d16cbffa8f509d9fa4ca237b7bb3f9c32"]
		}
	}`))
	opts, err := ProcessConfigFile(confFileName)
	if err != nil {
		t.Fatalf("Received an error reading config file: %v", err)
	}
	check := func(set PinnedCertSet) {
		t.Helper()
		if l := len(set); l != 2 {
			t.Fatalf("Expected 2 pinned certificates, got got %d", l)
		}
	}

	check(opts.TLSPinnedCerts)
	check(opts.LeafNode.TLSPinnedCerts)
	check(opts.Cluster.TLSPinnedCerts)
	check(opts.MQTT.TLSPinnedCerts)
	check(opts.Gateway.TLSPinnedCerts)
	check(opts.Websocket.TLSPinnedCerts)
}

func TestTlsVerifyClientCertsIfGiven(t *testing.T) {
	confFileName := createConfFile(t, []byte(`
	tls {
		cert_file: "./configs/certs/server.pem"
		key_file: "./configs/certs/key.pem"
		verify: optional
	}
	`))
	opts, err := ProcessConfigFile(confFileName)
	require_NoError(t, err)
	require_NotNil(t, opts.TLSConfig)
	require_Equal(t, opts.TLSConfig.ClientAuth, tls.VerifyClientCertIfGiven)
}

func TestNkeyUsersDefaultPermissionsConfig(t *testing.T) {
	confFileName := createConfFile(t, []byte(`
	authorization {
		default_permissions = {
			publish = "foo"
		}
		users = [
			{ user: "user", password: "pwd"}
			{ user: "other", password: "pwd",
				permissions = {
					subscribe = "bar"
				}
			}
			{ nkey: "UDKTV7HZVYJFJN64LLMYQBUR6MTNNYCDC3LAZH4VHURW3GZLL3FULBXV" }
			{ nkey: "UA3C5TBZYK5GJQJRWPMU6NFY5JNAEVQB2V2TUZFZDHFJFUYVKTTUOFKZ",
				permissions = {
					subscribe = "bar"
				}
			}
		]
	}
	accounts {
		A {
			default_permissions = {
				publish = "foo"
			}
			users = [
				{ user: "accuser", password: "pwd"}
				{ user: "accother", password: "pwd",
					permissions = {
						subscribe = "bar"
					}
				}
				{ nkey: "UC4YEYJHYKTU4LHROX7UEKEIO5RP5OUWDYXELHWXZOQHZYXHUD44LCRS" }
				{ nkey: "UDLSDF4UY3YW7JJQCYE6T2D4KFDCH6RGF3R65KHK247G3POJPI27VMQ3",
					permissions = {
						subscribe = "bar"
					}
				}
			]
		}
	}
	`))
	checkPerms := func(permsDef *Permissions, permsNonDef *Permissions) {
		if permsDef.Publish.Allow[0] != "foo" {
			t.Fatal("Publish allow foo missing")
		} else if permsDef.Subscribe != nil {
			t.Fatal("Has unexpected Subscribe permission")
		} else if permsNonDef.Subscribe.Allow[0] != "bar" {
			t.Fatal("Subscribe allow bar missing")
		} else if permsNonDef.Publish != nil {
			t.Fatal("Has unexpected Publish permission")
		}
	}
	opts, err := ProcessConfigFile(confFileName)
	if err != nil {
		t.Fatalf("Received an error reading config file: %v", err)
	}

	findUsers := func(u1, u2 string) (found []*User) {
		find := []string{u1, u2}
		for _, f := range find {
			for _, u := range opts.Users {
				if u.Username == f {
					found = append(found, u)
					break
				}
			}
		}
		return
	}

	findNkeyUsers := func(nk1, nk2 string) (found []*NkeyUser) {
		find := []string{nk1, nk2}
		for _, f := range find {
			for _, u := range opts.Nkeys {
				if strings.HasPrefix(u.Nkey, f) {
					found = append(found, u)
					break
				}
			}
		}
		return
	}

	if lu := len(opts.Users); lu != 4 {
		t.Fatalf("Expected 4 nkey users, got %d", lu)
	}
	foundU := findUsers("user", "other")
	checkPerms(foundU[0].Permissions, foundU[1].Permissions)
	foundU = findUsers("accuser", "accother")
	checkPerms(foundU[0].Permissions, foundU[1].Permissions)

	if lu := len(opts.Nkeys); lu != 4 {
		t.Fatalf("Expected 4 nkey users, got %d", lu)
	}
	foundNk := findNkeyUsers("UDK", "UA3")
	checkPerms(foundNk[0].Permissions, foundNk[1].Permissions)
	foundNk = findNkeyUsers("UC4", "UDL")
	checkPerms(foundNk[0].Permissions, foundNk[1].Permissions)
}

func TestNkeyUsersWithPermsConfig(t *testing.T) {
	confFileName := createConfFile(t, []byte(`
    authorization {
      users = [
        {nkey: "UDKTV7HZVYJFJN64LLMYQBUR6MTNNYCDC3LAZH4VHURW3GZLL3FULBXV",
         permissions = {
           publish = "$SYS.>"
           subscribe = { deny = ["foo", "bar", "baz"] }
         }
        }
      ]
    }`))
	opts, err := ProcessConfigFile(confFileName)
	if err != nil {
		t.Fatalf("Received an error reading config file: %v", err)
	}
	lu := len(opts.Nkeys)
	if lu != 1 {
		t.Fatalf("Expected 1 nkey user, got %d", lu)
	}
	nk := opts.Nkeys[0]
	if nk.Permissions == nil {
		t.Fatal("Expected to have permissions")
	}
	if nk.Permissions.Publish == nil {
		t.Fatal("Expected to have publish permissions")
	}
	if nk.Permissions.Publish.Allow[0] != "$SYS.>" {
		t.Fatalf("Expected publish to allow \"$SYS.>\", but got %v", nk.Permissions.Publish.Allow[0])
	}
	if nk.Permissions.Subscribe == nil {
		t.Fatal("Expected to have subscribe permissions")
	}
	if nk.Permissions.Subscribe.Allow != nil {
		t.Fatal("Expected to have no subscribe allow permissions")
	}
	deny := nk.Permissions.Subscribe.Deny
	if deny == nil || len(deny) != 3 ||
		deny[0] != "foo" || deny[1] != "bar" || deny[2] != "baz" {
		t.Fatalf("Expected to have subscribe deny permissions, got %v", deny)
	}
}

func TestBadNkeyConfig(t *testing.T) {
	confFileName := "nkeys_bad.conf"
	content := `
    authorization {
      users = [ {nkey: "Ufoo"}]
    }`
	if err := os.WriteFile(confFileName, []byte(content), 0666); err != nil {
		t.Fatalf("Error writing config file: %v", err)
	}
	defer removeFile(t, confFileName)
	if _, err := ProcessConfigFile(confFileName); err == nil {
		t.Fatalf("Expected an error from nkey entry with password")
	}
}

func TestNkeyWithPassConfig(t *testing.T) {
	confFileName := "nkeys_pass.conf"
	content := `
    authorization {
      users = [
        {nkey: "UDKTV7HZVYJFJN64LLMYQBUR6MTNNYCDC3LAZH4VHURW3GZLL3FULBXV", pass: "foo"}
      ]
    }`
	if err := os.WriteFile(confFileName, []byte(content), 0666); err != nil {
		t.Fatalf("Error writing config file: %v", err)
	}
	defer removeFile(t, confFileName)
	if _, err := ProcessConfigFile(confFileName); err == nil {
		t.Fatalf("Expected an error from bad nkey entry")
	}
}

func TestTokenWithUserPass(t *testing.T) {
	confFileName := "test.conf"
	content := `
	authorization={
		user: user
		pass: password
		token: $2a$11$whatever
	}`
	if err := os.WriteFile(confFileName, []byte(content), 0666); err != nil {
		t.Fatalf("Error writing config file: %v", err)
	}
	defer removeFile(t, confFileName)
	_, err := ProcessConfigFile(confFileName)
	if err == nil {
		t.Fatal("Expected error, got none")
	}
	if !strings.Contains(err.Error(), "token") {
		t.Fatalf("Expected error related to token, got %v", err)
	}
}

func TestTokenWithUsers(t *testing.T) {
	confFileName := "test.conf"
	content := `
	authorization={
		token: $2a$11$whatever
		users: [
			{user: test, password: test}
		]
	}`
	if err := os.WriteFile(confFileName, []byte(content), 0666); err != nil {
		t.Fatalf("Error writing config file: %v", err)
	}
	defer removeFile(t, confFileName)
	_, err := ProcessConfigFile(confFileName)
	if err == nil {
		t.Fatal("Expected error, got none")
	}
	if !strings.Contains(err.Error(), "token") {
		t.Fatalf("Expected error related to token, got %v", err)
	}
}

func TestParseWriteDeadline(t *testing.T) {
	confFile := createConfFile(t, []byte("write_deadline: \"1x\""))
	_, err := ProcessConfigFile(confFile)
	if err == nil {
		t.Fatal("Expected error, got none")
	}
	if !strings.Contains(err.Error(), "parsing") {
		t.Fatalf("Expected error related to parsing, got %v", err)
	}
	confFile = createConfFile(t, []byte("write_deadline: \"1s\""))
	opts, err := ProcessConfigFile(confFile)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if opts.WriteDeadline != time.Second {
		t.Fatalf("Expected write_deadline to be 1s, got %v", opts.WriteDeadline)
	}
	oldStdout := os.Stdout
	_, w, _ := os.Pipe()
	defer func() {
		w.Close()
		os.Stdout = oldStdout
	}()
	os.Stdout = w
	confFile = createConfFile(t, []byte("write_deadline: 2"))
	opts, err = ProcessConfigFile(confFile)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if opts.WriteDeadline != 2*time.Second {
		t.Fatalf("Expected write_deadline to be 2s, got %v", opts.WriteDeadline)
	}
}

func TestOptionsClone(t *testing.T) {
	opts := &Options{
		ConfigFile:     "./configs/test.conf",
		Host:           "127.0.0.1",
		Port:           2222,
		Username:       "derek",
		Password:       "porkchop",
		AuthTimeout:    1.0,
		Debug:          true,
		Trace:          true,
		Logtime:        false,
		HTTPPort:       DEFAULT_HTTP_PORT,
		HTTPBasePath:   DEFAULT_HTTP_BASE_PATH,
		PidFile:        "/tmp/nats-server/nats-server.pid",
		ProfPort:       6789,
		Syslog:         true,
		RemoteSyslog:   "udp://foo.com:33",
		MaxControlLine: 2048,
		MaxPayload:     65536,
		MaxConn:        100,
		PingInterval:   60 * time.Second,
		MaxPingsOut:    3,
		Cluster: ClusterOpts{
			NoAdvertise:    true,
			ConnectRetries: 2,
			WriteDeadline:  3 * time.Second,
		},
		Gateway: GatewayOpts{
			Name: "A",
			Gateways: []*RemoteGatewayOpts{
				{Name: "B", URLs: []*url.URL{{Scheme: "nats", Host: "host:5222"}}},
				{Name: "C"},
			},
		},
		WriteDeadline: 3 * time.Second,
		Routes:        []*url.URL{{}},
		Users:         []*User{{Username: "foo", Password: "bar"}},
	}

	clone := opts.Clone()

	if !reflect.DeepEqual(opts, clone) {
		t.Fatalf("Cloned Options are incorrect.\nexpected: %+v\ngot: %+v",
			clone, opts)
	}

	clone.Users[0].Password = "baz"
	if reflect.DeepEqual(opts, clone) {
		t.Fatal("Expected Options to be different")
	}

	opts.Gateway.Gateways[0].URLs[0] = nil
	if reflect.DeepEqual(opts.Gateway.Gateways[0], clone.Gateway.Gateways[0]) {
		t.Fatal("Expected Options to be different")
	}
	if clone.Gateway.Gateways[0].URLs[0].Host != "host:5222" {
		t.Fatalf("Unexpected URL: %v", clone.Gateway.Gateways[0].URLs[0])
	}
}

func TestOptionsCloneNilLists(t *testing.T) {
	opts := &Options{}

	clone := opts.Clone()

	if clone.Routes != nil {
		t.Fatalf("Expected Routes to be nil, got: %v", clone.Routes)
	}
	if clone.Users != nil {
		t.Fatalf("Expected Users to be nil, got: %v", clone.Users)
	}
}

func TestOptionsCloneNil(t *testing.T) {
	opts := (*Options)(nil)
	clone := opts.Clone()
	if clone != nil {
		t.Fatalf("Expected nil, got: %+v", clone)
	}
}

func TestEmptyConfig(t *testing.T) {
	opts, err := ProcessConfigFile("")

	if err != nil {
		t.Fatalf("Expected no error from empty config, got: %+v", err)
	}

	if opts.ConfigFile != "" {
		t.Fatalf("Expected empty config, got: %+v", opts)
	}
}

func TestMalformedListenAddress(t *testing.T) {
	opts, err := ProcessConfigFile("./configs/malformed_listen_address.conf")
	if err == nil {
		t.Fatalf("Expected an error reading config file: got %+v", opts)
	}
}

func TestMalformedClusterAddress(t *testing.T) {
	opts, err := ProcessConfigFile("./configs/malformed_cluster_address.conf")
	if err == nil {
		t.Fatalf("Expected an error reading config file: got %+v", opts)
	}
}

func TestPanic(t *testing.T) {
	conf := createConfFile(t, []byte(`port: "this_string_trips_a_panic"`))
	opts, err := ProcessConfigFile(conf)
	if err == nil {
		t.Fatalf("Expected an error reading config file: got %+v", opts)
	} else {
		if !strings.Contains(err.Error(), ":1:0: interface conversion:") {
			t.Fatalf("This was supposed to trip a panic on interface conversion right at the beginning")
		}
	}
}

func TestMaxClosedClients(t *testing.T) {
	conf := createConfFile(t, []byte(`max_closed_clients: 5`))
	opts, err := ProcessConfigFile(conf)
	require_NoError(t, err)
	require_Equal(t, opts.MaxClosedClients, 5)
}

func TestPingIntervalOld(t *testing.T) {
	conf := createConfFile(t, []byte(`ping_interval: 5`))
	opts := &Options{}
	err := opts.ProcessConfigFile(conf)
	if err == nil {
		t.Fatalf("expected an error")
	}
	errTyped, ok := err.(*processConfigErr)
	if !ok {
		t.Fatalf("expected an error of type processConfigErr")
	}
	if len(errTyped.warnings) != 1 {
		t.Fatalf("expected processConfigErr to have one warning")
	}
	if len(errTyped.errors) != 0 {
		t.Fatalf("expected processConfigErr to have no error")
	}
	if opts.PingInterval != 5*time.Second {
		t.Fatalf("expected ping interval to be 5 seconds")
	}
}

func TestPingIntervalNew(t *testing.T) {
	conf := createConfFile(t, []byte(`ping_interval: "5m"`))
	opts := &Options{}
	if err := opts.ProcessConfigFile(conf); err != nil {
		t.Fatalf("expected no error")
	}
	if opts.PingInterval != 5*time.Minute {
		t.Fatalf("expected ping interval to be 5 minutes")
	}
}

func TestOptionsProcessConfigFile(t *testing.T) {
	// Create options with default values of Debug and Trace
	// that are the opposite of what is in the config file.
	// Set another option that is not present in the config file.
	logFileName := "test.log"
	opts := &Options{
		Debug:   true,
		Trace:   false,
		LogFile: logFileName,
	}
	configFileName := "./configs/test.conf"
	if err := opts.ProcessConfigFile(configFileName); err != nil {
		t.Fatalf("Error processing config file: %v", err)
	}
	// Verify that values are as expected
	if opts.ConfigFile != configFileName {
		t.Fatalf("Expected ConfigFile to be set to %q, got %v", configFileName, opts.ConfigFile)
	}
	if opts.Debug {
		t.Fatal("Debug option should have been set to false from config file")
	}
	if !opts.Trace {
		t.Fatal("Trace option should have been set to true from config file")
	}
	if opts.LogFile != logFileName {
		t.Fatalf("Expected LogFile to be %q, got %q", logFileName, opts.LogFile)
	}
}

func TestConfigureOptions(t *testing.T) {
	// Options.Configure() will snapshot the flags. This is used by the reload code.
	// We need to set it back to nil otherwise it will impact reload tests.
	defer func() { FlagSnapshot = nil }()

	ch := make(chan bool, 1)
	checkPrintInvoked := func() {
		ch <- true
	}
	usage := func() { panic("should not get there") }
	var fs *flag.FlagSet
	type testPrint struct {
		args                   []string
		version, help, tlsHelp func()
	}
	testFuncs := []testPrint{
		{[]string{"-v"}, checkPrintInvoked, usage, PrintTLSHelpAndDie},
		{[]string{"version"}, checkPrintInvoked, usage, PrintTLSHelpAndDie},
		{[]string{"-h"}, PrintServerAndExit, checkPrintInvoked, PrintTLSHelpAndDie},
		{[]string{"help"}, PrintServerAndExit, checkPrintInvoked, PrintTLSHelpAndDie},
		{[]string{"-help_tls"}, PrintServerAndExit, usage, checkPrintInvoked},
	}
	for _, tf := range testFuncs {
		fs = flag.NewFlagSet("test", flag.ContinueOnError)
		opts, err := ConfigureOptions(fs, tf.args, tf.version, tf.help, tf.tlsHelp)
		if err != nil {
			t.Fatalf("Error on configure: %v", err)
		}
		if opts != nil {
			t.Fatalf("Expected options to be nil, got %v", opts)
		}
		select {
		case <-ch:
		case <-time.After(time.Second):
			t.Fatalf("Should have invoked print function for args=%v", tf.args)
		}
	}

	// Helper function that expect parsing with given args to not produce an error.
	mustNotFail := func(args []string) *Options {
		fs := flag.NewFlagSet("test", flag.ContinueOnError)
		opts, err := ConfigureOptions(fs, args, PrintServerAndExit, fs.Usage, PrintTLSHelpAndDie)
		if err != nil {
			stackFatalf(t, "Error on configure: %v", err)
		}
		return opts
	}

	// Helper function that expect configuration to fail.
	expectToFail := func(args []string, errContent ...string) {
		fs := flag.NewFlagSet("test", flag.ContinueOnError)
		// Silence the flagSet so that on failure nothing is printed.
		// (flagSet would print error message about unknown flags, etc..)
		silenceOuput := &bytes.Buffer{}
		fs.SetOutput(silenceOuput)
		opts, err := ConfigureOptions(fs, args, PrintServerAndExit, fs.Usage, PrintTLSHelpAndDie)
		if opts != nil || err == nil {
			stackFatalf(t, "Expected no option and an error, got opts=%v and err=%v", opts, err)
		}
		for _, testErr := range errContent {
			if strings.Contains(err.Error(), testErr) {
				// We got the error we wanted.
				return
			}
		}
		stackFatalf(t, "Expected errors containing any of those %v, got %v", errContent, err)
	}

	// Basic test with port number
	opts := mustNotFail([]string{"-p", "1234"})
	if opts.Port != 1234 {
		t.Fatalf("Expected port to be 1234, got %v", opts.Port)
	}

	// Should fail because of unknown parameter
	expectToFail([]string{"foo"}, "command")

	// Should fail because unknown flag
	expectToFail([]string{"-xxx", "foo"}, "flag")

	// Should fail because of config file missing
	expectToFail([]string{"-c", "xxx.cfg"}, "file")

	// Should fail because of too many args for signal command
	expectToFail([]string{"-sl", "quit=pid=foo"}, "signal")

	// Should fail because of invalid pid
	// On windows, if not running with admin privileges, you would get access denied.
	expectToFail([]string{"-sl", "quit=pid"}, "pid", "denied")

	// The config file set Trace to true.
	opts = mustNotFail([]string{"-c", "./configs/test.conf"})
	if !opts.Trace {
		t.Fatal("Trace should have been set to true")
	}

	// The config file set Trace to true, but was overridden by param -V=false
	opts = mustNotFail([]string{"-c", "./configs/test.conf", "-V=false"})
	if opts.Trace {
		t.Fatal("Trace should have been set to false")
	}

	// The config file set Trace to true, but was overridden by param -DV=false
	opts = mustNotFail([]string{"-c", "./configs/test.conf", "-DV=false"})
	if opts.Debug || opts.Trace {
		t.Fatal("Debug and Trace should have been set to false")
	}

	// The config file set Trace to true, but was overridden by param -DV
	opts = mustNotFail([]string{"-c", "./configs/test.conf", "-DV"})
	if !opts.Debug || !opts.Trace {
		t.Fatal("Debug and Trace should have been set to true")
	}

	// This should fail since -cluster is missing
	expectedURL, _ := url.Parse("nats://127.0.0.1:6223")
	expectToFail([]string{"-routes", expectedURL.String()}, "solicited routes")

	// Ensure that we can set cluster and routes from command line
	opts = mustNotFail([]string{"-cluster", "nats://127.0.0.1:6222", "-routes", expectedURL.String()})
	if opts.Cluster.ListenStr != "nats://127.0.0.1:6222" {
		t.Fatalf("Unexpected Cluster.ListenStr=%q", opts.Cluster.ListenStr)
	}
	if opts.RoutesStr != "nats://127.0.0.1:6223" || len(opts.Routes) != 1 || opts.Routes[0].String() != expectedURL.String() {
		t.Fatalf("Unexpected RoutesStr: %q and Routes: %v", opts.RoutesStr, opts.Routes)
	}

	// Use a config with cluster configuration and explicit route defined.
	// Override with empty routes string.
	opts = mustNotFail([]string{"-c", "./configs/srv_a.conf", "-routes", ""})
	if opts.RoutesStr != "" || len(opts.Routes) != 0 {
		t.Fatalf("Unexpected RoutesStr: %q and Routes: %v", opts.RoutesStr, opts.Routes)
	}

	// Use a config with cluster configuration and override cluster listen string
	expectedURL, _ = url.Parse("nats-route://ruser:top_secret@127.0.0.1:7246")
	opts = mustNotFail([]string{"-c", "./configs/srv_a.conf", "-cluster", "nats://ivan:pwd@127.0.0.1:6222"})
	if opts.Cluster.Username != "ivan" || opts.Cluster.Password != "pwd" || opts.Cluster.Port != 6222 ||
		len(opts.Routes) != 1 || opts.Routes[0].String() != expectedURL.String() {
		t.Fatalf("Unexpected Cluster and/or Routes: %#v - %v", opts.Cluster, opts.Routes)
	}

	// Disable clustering from command line
	opts = mustNotFail([]string{"-c", "./configs/srv_a.conf", "-cluster", ""})
	if opts.Cluster.Port != 0 {
		t.Fatalf("Unexpected Cluster: %v", opts.Cluster)
	}

	// Various erros due to malformed cluster listen string.
	// (adding -routes to have more than 1 set flag to check
	// that Visit() stops when an error is found).
	expectToFail([]string{"-cluster", ":", "-routes", ""}, "protocol")
	expectToFail([]string{"-cluster", "nats://127.0.0.1", "-routes", ""}, "port")
	expectToFail([]string{"-cluster", "nats://127.0.0.1:xxx", "-routes", ""}, "invalid port")
	expectToFail([]string{"-cluster", "nats://ivan:127.0.0.1:6222", "-routes", ""}, "colons")
	expectToFail([]string{"-cluster", "nats://ivan@127.0.0.1:6222", "-routes", ""}, "password")

	// Override config file's TLS configuration from command line, and completely disable TLS
	opts = mustNotFail([]string{"-c", "./configs/tls.conf", "-tls=false"})
	if opts.TLSConfig != nil || opts.TLS {
		t.Fatal("Expected TLS to be disabled")
	}
	// Override config file's TLS configuration from command line, and force TLS verification.
	// However, since TLS config has to be regenerated, user need to provide -tlscert and -tlskey too.
	// So this should fail.
	expectToFail([]string{"-c", "./configs/tls.conf", "-tlsverify"}, "valid")

	// Now same than above, but with all valid params.
	opts = mustNotFail([]string{"-c", "./configs/tls.conf", "-tlsverify", "-tlscert", "./configs/certs/server.pem", "-tlskey", "./configs/certs/key.pem"})
	if opts.TLSConfig == nil || !opts.TLSVerify {
		t.Fatal("Expected TLS to be configured and force verification")
	}

	// Configure TLS, but some TLS params missing
	expectToFail([]string{"-tls"}, "valid")
	expectToFail([]string{"-tls", "-tlscert", "./configs/certs/server.pem"}, "valid")
	// One of the file does not exist
	expectToFail([]string{"-tls", "-tlscert", "./configs/certs/server.pem", "-tlskey", "./configs/certs/notfound.pem"}, "file")

	// Configure TLS and check that this results in a TLSConfig option.
	opts = mustNotFail([]string{"-tls", "-tlscert", "./configs/certs/server.pem", "-tlskey", "./configs/certs/key.pem"})
	if opts.TLSConfig == nil || !opts.TLS {
		t.Fatal("Expected TLSConfig to be set")
	}
	// Check that we use default TLS ciphers
	if !reflect.DeepEqual(opts.TLSConfig.CipherSuites, defaultCipherSuites()) {
		t.Fatalf("Default ciphers not set, expected %v, got %v", defaultCipherSuites(), opts.TLSConfig.CipherSuites)
	}
}

func TestClusterPermissionsConfig(t *testing.T) {
	template := `
		cluster {
			port: 1234
			%s
			authorization {
				user: ivan
				password: pwd
				permissions {
					import {
						allow: "foo"
					}
					export {
						allow: "bar"
					}
				}
			}
		}
	`
	conf := createConfFile(t, []byte(fmt.Sprintf(template, "")))
	opts, err := ProcessConfigFile(conf)
	if err != nil {
		if cerr, ok := err.(*processConfigErr); ok && len(cerr.Errors()) > 0 {
			t.Fatalf("Error processing config file: %v", err)
		}
	}
	if opts.Cluster.Permissions == nil {
		t.Fatal("Expected cluster permissions to be set")
	}
	if opts.Cluster.Permissions.Import == nil {
		t.Fatal("Expected cluster import permissions to be set")
	}
	if len(opts.Cluster.Permissions.Import.Allow) != 1 || opts.Cluster.Permissions.Import.Allow[0] != "foo" {
		t.Fatalf("Expected cluster import permissions to have %q, got %v", "foo", opts.Cluster.Permissions.Import.Allow)
	}
	if opts.Cluster.Permissions.Export == nil {
		t.Fatal("Expected cluster export permissions to be set")
	}
	if len(opts.Cluster.Permissions.Export.Allow) != 1 || opts.Cluster.Permissions.Export.Allow[0] != "bar" {
		t.Fatalf("Expected cluster export permissions to have %q, got %v", "bar", opts.Cluster.Permissions.Export.Allow)
	}

	// Now add permissions in top level cluster and check
	// that this is the one that is being used.
	conf = createConfFile(t, []byte(fmt.Sprintf(template, `
		permissions {
			import {
				allow: "baz"
			}
			export {
				allow: "bat"
			}
		}
	`)))
	opts, err = ProcessConfigFile(conf)
	if err != nil {
		t.Fatalf("Error processing config file: %v", err)
	}
	if opts.Cluster.Permissions == nil {
		t.Fatal("Expected cluster permissions to be set")
	}
	if opts.Cluster.Permissions.Import == nil {
		t.Fatal("Expected cluster import permissions to be set")
	}
	if len(opts.Cluster.Permissions.Import.Allow) != 1 || opts.Cluster.Permissions.Import.Allow[0] != "baz" {
		t.Fatalf("Expected cluster import permissions to have %q, got %v", "baz", opts.Cluster.Permissions.Import.Allow)
	}
	if opts.Cluster.Permissions.Export == nil {
		t.Fatal("Expected cluster export permissions to be set")
	}
	if len(opts.Cluster.Permissions.Export.Allow) != 1 || opts.Cluster.Permissions.Export.Allow[0] != "bat" {
		t.Fatalf("Expected cluster export permissions to have %q, got %v", "bat", opts.Cluster.Permissions.Export.Allow)
	}

	// Tests with invalid permissions
	invalidPerms := []string{
		`permissions: foo`,
		`permissions {
			unknown_field: "foo"
		}`,
		`permissions {
			import: [1, 2, 3]
		}`,
		`permissions {
			import {
				unknown_field: "foo"
			}
		}`,
		`permissions {
			import {
				allow {
					x: y
				}
			}
		}`,
		`permissions {
			import {
				deny {
					x: y
				}
			}
		}`,
		`permissions {
			export: [1, 2, 3]
		}`,
		`permissions {
			export {
				unknown_field: "foo"
			}
		}`,
		`permissions {
			export {
				allow {
					x: y
				}
			}
		}`,
		`permissions {
			export {
				deny {
					x: y
				}
			}
		}`,
	}
	for _, perms := range invalidPerms {
		conf = createConfFile(t, []byte(fmt.Sprintf(`
			cluster {
				port: 1234
				%s
			}
		`, perms)))
		_, err := ProcessConfigFile(conf)
		if err == nil {
			t.Fatalf("Expected failure for permissions %s", perms)
		}
	}

	for _, perms := range invalidPerms {
		conf = createConfFile(t, []byte(fmt.Sprintf(`
			cluster {
				port: 1234
				authorization {
					user: ivan
					password: pwd
					%s
				}
			}
		`, perms)))
		_, err := ProcessConfigFile(conf)
		if err == nil {
			t.Fatalf("Expected failure for permissions %s", perms)
		}
	}
}

func TestParseServiceLatency(t *testing.T) {
	cases := []struct {
		name    string
		conf    string
		want    *serviceLatency
		wantErr bool
	}{
		{
			name: "block with percent sample default value",
			conf: `system_account = nats.io
			accounts {
				nats.io {
					exports [{
						service: nats.add
						latency: {
							sampling: 100%
							subject: latency.tracking.add
						}
					}]
				}
			}`,
			want: &serviceLatency{
				subject:  "latency.tracking.add",
				sampling: 100,
			},
		},
		{
			name: "block with percent sample nondefault value",
			conf: `system_account = nats.io
			accounts {
				nats.io {
					exports [{
						service: nats.add
						latency: {
							sampling: 33%
							subject: latency.tracking.add
						}
					}]
				}
			}`,
			want: &serviceLatency{
				subject:  "latency.tracking.add",
				sampling: 33,
			},
		},
		{
			name: "block with number sample nondefault value",
			conf: `system_account = nats.io
			accounts {
				nats.io {
					exports [{
						service: nats.add
						latency: {
							sampling: 87
							subject: latency.tracking.add
						}
					}]
				}
			}`,
			want: &serviceLatency{
				subject:  "latency.tracking.add",
				sampling: 87,
			},
		},
		{
			name: "field with subject",
			conf: `system_account = nats.io
			accounts {
				nats.io {
					exports [{
						service: nats.add
						latency: latency.tracking.add
					}]
				}
			}`,
			want: &serviceLatency{
				subject:  "latency.tracking.add",
				sampling: 100,
			},
		},
		{
			name: "block with missing subject",
			conf: `system_account = nats.io
			accounts {
				nats.io {
					exports [{
						service: nats.add
						latency: {
							sampling: 87
						}
					}]
				}
			}`,
			wantErr: true,
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			f := createConfFile(t, []byte(c.conf))
			opts, err := ProcessConfigFile(f)
			switch {
			case c.wantErr && err == nil:
				t.Fatalf("Expected ProcessConfigFile to fail, but didn't")
			case c.wantErr && err != nil:
				// We wanted an error and got one, test passed.
				return
			case !c.wantErr && err == nil:
				// We didn't want an error and didn't get one, keep going.
				break
			case !c.wantErr && err != nil:
				t.Fatalf("Failed to process config: %v", err)
			}

			if len(opts.Accounts) != 1 {
				t.Fatalf("Expected accounts to have len %d, got %d", 1, len(opts.Accounts))
			}
			if len(opts.Accounts[0].exports.services) != 1 {
				t.Fatalf("Expected export services to have len %d, got %d", 1, len(opts.Accounts[0].exports.services))
			}
			s, ok := opts.Accounts[0].exports.services["nats.add"]
			if !ok {
				t.Fatalf("Expected export service nats.add, missing")
			}

			if !reflect.DeepEqual(s.latency, c.want) {
				t.Fatalf("Expected latency to be %#v, got %#v", c.want, s.latency)
			}
		})
	}
}

func TestParseExport(t *testing.T) {
	conf := `
		port: -1
		system_account: sys
		accounts {
			sys {
				exports [{
					stream "$SYS.SERVER.ACCOUNT.*.CONNS"
					account_token_position 4
				}]
			}
			accE {
				exports [{
					service foo.*
					account_token_position 2
				}]
				users [{
					user ue
					password pwd
				}],
			}
			accI1 {
				imports [{
					service {
						account accE
						subject foo.accI1
					}
					to foo
				},{
					stream {
						account sys
						subject "$SYS.SERVER.ACCOUNT.accI1.CONNS"
					}
				}],
				users [{
					user u1
					password pwd
				}],
			}
			accI2 {
				imports [{
					service {
						account accE
						subject foo.accI2
					}
					to foo
				},{
					stream {
						account sys
						subject "$SYS.SERVER.ACCOUNT.accI2.CONNS"
					}
				}],
				users [{
					user u2
					password pwd
				}],
			}
		}`
	f := createConfFile(t, []byte(conf))
	s, o := RunServerWithConfig(f)
	if s == nil {
		t.Fatal("Failed startup")
	}
	defer s.Shutdown()
	connect := func(user string) *nats.Conn {
		nc, err := nats.Connect(fmt.Sprintf("nats://%s:pwd@%s:%d", user, o.Host, o.Port))
		require_NoError(t, err)
		return nc
	}
	nc1 := connect("u1")
	defer nc1.Close()
	nc2 := connect("u2")
	defer nc2.Close()

	// Due to the fact that above CONNECT events are generated and sent from
	// a system go routine, it is possible that by the time we create the
	// subscriptions below, the interest would exist and messages be sent,
	// which was causing issues since wg.Done() was called too many times.
	// Add a little delay to minimize risk, but also use counter to decide
	// when to call wg.Done() to avoid panic due to negative number.
	time.Sleep(100 * time.Millisecond)

	wg := sync.WaitGroup{}
	wg.Add(1)
	count := int32(0)
	// We expect a total of 6 messages
	expected := int32(6)
	subscribe := func(nc *nats.Conn, subj string) {
		t.Helper()
		_, err := nc.Subscribe(subj, func(msg *nats.Msg) {
			if msg.Reply != _EMPTY_ {
				msg.Respond(msg.Data)
			}
			if atomic.AddInt32(&count, 1) == expected {
				wg.Done()
			}
		})
		require_NoError(t, err)
		nc.Flush()
	}
	//Subscribe to CONNS events
	subscribe(nc1, "$SYS.SERVER.ACCOUNT.accI1.CONNS")
	subscribe(nc2, "$SYS.SERVER.ACCOUNT.accI2.CONNS")
	// Trigger 2 CONNS event
	nc3 := connect("u1")
	nc3.Close()
	nc4 := connect("u2")
	nc4.Close()
	// test service
	ncE := connect("ue")
	defer ncE.Close()
	subscribe(ncE, "foo.*")
	request := func(nc *nats.Conn, msg string) {
		if m, err := nc.Request("foo", []byte(msg), time.Second); err != nil {
			t.Fatal("Failed request ", msg, err)
		} else if m == nil {
			t.Fatal("No response msg")
		} else if string(m.Data) != msg {
			t.Fatal("Wrong response msg", string(m.Data))
		}
	}
	request(nc1, "1")
	request(nc2, "1")
	wg.Wait()
}

func TestAccountUsersLoadedProperly(t *testing.T) {
	conf := createConfFile(t, []byte(`
	listen: "127.0.0.1:-1"
	authorization {
		users [
			{user: ivan, password: bar}
			{nkey : UC6NLCN7AS34YOJVCYD4PJ3QB7QGLYG5B5IMBT25VW5K4TNUJODM7BOX}
		]
	}
	accounts {
		synadia {
			users [
				{user: derek, password: foo}
				{nkey : UBAAQWTW6CG2G6ANGNKB5U2B7HRWHSGMZEZX3AQSAJOQDAUGJD46LD2E}
			]
		}
	}
	`))
	check := func(t *testing.T) {
		t.Helper()
		s, _ := RunServerWithConfig(conf)
		defer s.Shutdown()
		opts := s.getOpts()
		if n := len(opts.Users); n != 2 {
			t.Fatalf("Should have 2 users, got %v", n)
		}
		if n := len(opts.Nkeys); n != 2 {
			t.Fatalf("Should have 2 nkeys, got %v", n)
		}
	}
	// Repeat test since issue was with ordering of processing
	// of authorization vs accounts that depends on range of a map (after actual parsing)
	for i := 0; i < 20; i++ {
		check(t)
	}
}

func TestParsingGateways(t *testing.T) {
	content := `
	gateway {
		name: "A"
		listen: "127.0.0.1:4444"
		host: "127.0.0.1"
		port: 4444
		reject_unknown_cluster: true
		authorization {
			user: "ivan"
			password: "pwd"
			timeout: 2.0
		}
		tls {
			cert_file: "./configs/certs/server.pem"
			key_file: "./configs/certs/key.pem"
			timeout: 3.0
		}
		advertise: "me:1"
		connect_retries: 10
		connect_backoff: true
		gateways: [
			{
				name: "B"
				urls: ["nats://user1:pwd1@host2:5222", "nats://user1:pwd1@host3:6222"]
			}
			{
				name: "C"
				url: "nats://host4:7222"
			}
		]
	}
	`
	file := "server_config_gateways.conf"
	if err := os.WriteFile(file, []byte(content), 0600); err != nil {
		t.Fatalf("Error writing config file: %v", err)
	}
	defer removeFile(t, file)
	opts, err := ProcessConfigFile(file)
	if err != nil {
		t.Fatalf("Error processing file: %v", err)
	}

	expected := &GatewayOpts{
		Name:           "A",
		Host:           "127.0.0.1",
		Port:           4444,
		Username:       "ivan",
		Password:       "pwd",
		AuthTimeout:    2.0,
		Advertise:      "me:1",
		ConnectRetries: 10,
		ConnectBackoff: true,
		TLSTimeout:     3.0,
		RejectUnknown:  true,
	}
	u1, _ := url.Parse("nats://user1:pwd1@host2:5222")
	u2, _ := url.Parse("nats://user1:pwd1@host3:6222")
	urls := []*url.URL{u1, u2}
	gw := &RemoteGatewayOpts{
		Name: "B",
		URLs: urls,
	}
	expected.Gateways = append(expected.Gateways, gw)

	u1, _ = url.Parse("nats://host4:7222")
	urls = []*url.URL{u1}
	gw = &RemoteGatewayOpts{
		Name: "C",
		URLs: urls,
	}
	expected.Gateways = append(expected.Gateways, gw)

	// Just make sure that TLSConfig is set.. we have aother test
	// to check proper generating TLSConfig from config file...
	if opts.Gateway.TLSConfig == nil {
		t.Fatalf("Expected TLSConfig, got none")
	}
	opts.Gateway.TLSConfig = nil
	opts.Gateway.tlsConfigOpts = nil
	if !reflect.DeepEqual(&opts.Gateway, expected) {
		t.Fatalf("Expected %v, got %v", expected, opts.Gateway)
	}
}

func TestParsingGatewaysErrors(t *testing.T) {
	for _, test := range []struct {
		name        string
		content     string
		expectedErr string
	}{
		{
			"bad_type",
			`gateway: "bad_type"`,
			"Expected gateway to be a map",
		},
		{
			"bad_listen",
			`gateway {
				name: "A"
				port: -1
				listen: "bad::address"
			}`,
			"parse address",
		},
		{
			"bad_auth",
			`gateway {
				name: "A"
				port: -1
				authorization {
					users {
					}
				}
			}`,
			"be an array",
		},
		{
			"unknown_field",
			`gateway {
				name: "A"
				port: -1
				reject_unknown_cluster: true
				unknown_field: 1
			}`,
			"unknown field",
		},
		{
			"users_not_supported",
			`gateway {
				name: "A"
				port: -1
				authorization {
					users [
						{user: alice, password: foo}
						{user: bob,   password: bar}
					]
				}
			}`,
			"does not allow multiple users",
		},
		{
			"tls_error",
			`gateway {
				name: "A"
				port: -1
				tls {
					cert_file: 123
				}
			}`,
			"to be filename",
		},
		{
			"tls_gen_error_cert_file_not_found",
			`gateway {
				name: "A"
				port: -1
				tls {
					cert_file: "./configs/certs/missing.pem"
					key_file: "./configs/certs/server-key.pem"
				}
			}`,
			"certificate/key pair",
		},
		{
			"tls_gen_error_key_file_not_found",
			`gateway {
				name: "A"
				port: -1
				tls {
					cert_file: "./configs/certs/server.pem"
					key_file: "./configs/certs/missing.pem"
				}
			}`,
			"certificate/key pair",
		},
		{
			"tls_gen_error_key_file_missing",
			`gateway {
				name: "A"
				port: -1
				tls {
					cert_file: "./configs/certs/server.pem"
				}
			}`,
			`missing 'key_file' in TLS configuration`,
		},
		{
			"tls_gen_error_cert_file_missing",
			`gateway {
				name: "A"
				port: -1
				tls {
					key_file: "./configs/certs/server-key.pem"
				}
			}`,
			`missing 'cert_file' in TLS configuration`,
		},
		{
			"tls_gen_error_key_file_not_found",
			`gateway {
				name: "A"
				port: -1
				tls {
					cert_file: "./configs/certs/server.pem"
					key_file: "./configs/certs/missing.pem"
				}
			}`,
			"certificate/key pair",
		},
		{
			"gateways_needs_to_be_an_array",
			`gateway {
				name: "A"
				gateways {
					name: "B"
				}
			}`,
			"Expected gateways field to be an array",
		},
		{
			"gateways_entry_needs_to_be_a_map",
			`gateway {
				name: "A"
				gateways [
					"g1", "g2"
				]
			}`,
			"Expected gateway entry to be a map",
		},
		{
			"bad_url",
			`gateway {
				name: "A"
				gateways [
					{
						name: "B"
						url: "nats://wrong url"
					}
				]
			}`,
			"error parsing gateway url",
		},
		{
			"bad_urls",
			`gateway {
				name: "A"
				gateways [
					{
						name: "B"
						urls: ["nats://wrong url", "nats://host:5222"]
					}
				]
			}`,
			"error parsing gateway url",
		},
		{
			"gateway_tls_error",
			`gateway {
				name: "A"
				port: -1
				gateways [
					{
						name: "B"
						tls {
							cert_file: 123
						}
					}
				]
			}`,
			"to be filename",
		},
		{
			"gateway_unknown_field",
			`gateway {
				name: "A"
				port: -1
				gateways [
					{
						name: "B"
						unknown_field: 1
					}
				]
			}`,
			"unknown field",
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			file := fmt.Sprintf("server_config_gateways_%s.conf", test.name)
			if err := os.WriteFile(file, []byte(test.content), 0600); err != nil {
				t.Fatalf("Error writing config file: %v", err)
			}
			defer removeFile(t, file)
			_, err := ProcessConfigFile(file)
			if err == nil {
				t.Fatalf("Expected to fail, did not. Content:\n%s", test.content)
			} else if !strings.Contains(err.Error(), test.expectedErr) {
				t.Fatalf("Expected error containing %q, got %q, for content:\n%s", test.expectedErr, err, test.content)
			}
		})
	}
}

func TestParsingLeafNodesListener(t *testing.T) {
	content := `
	leafnodes {
		listen: "127.0.0.1:3333"
		host: "127.0.0.1"
		port: 3333
		advertise: "me:22"
		authorization {
			user: "derek"
			password: "s3cr3t!"
			timeout: 2.2
		}
		tls {
			cert_file: "./configs/certs/server.pem"
			key_file: "./configs/certs/key.pem"
			timeout: 3.3
		}
	}
	`
	conf := createConfFile(t, []byte(content))
	opts, err := ProcessConfigFile(conf)
	if err != nil {
		t.Fatalf("Error processing file: %v", err)
	}

	expected := &LeafNodeOpts{
		Host:        "127.0.0.1",
		Port:        3333,
		Username:    "derek",
		Password:    "s3cr3t!",
		AuthTimeout: 2.2,
		Advertise:   "me:22",
		TLSTimeout:  3.3,
	}
	if opts.LeafNode.TLSConfig == nil {
		t.Fatalf("Expected TLSConfig, got none")
	}
	if opts.LeafNode.tlsConfigOpts == nil {
		t.Fatalf("Expected TLSConfig snapshot, got none")
	}
	opts.LeafNode.TLSConfig = nil
	opts.LeafNode.tlsConfigOpts = nil
	if !reflect.DeepEqual(&opts.LeafNode, expected) {
		t.Fatalf("Expected %v, got %v", expected, opts.LeafNode)
	}
}

func TestParsingLeafNodeRemotes(t *testing.T) {
	t.Run("parse config file with relative path", func(t *testing.T) {
		content := `
		leafnodes {
			remotes = [
				{
					url: nats-leaf://127.0.0.1:2222
					account: foobar // Local Account to bind to..
					credentials: "./my.creds"
				}
			]
		}
		`
		conf := createConfFile(t, []byte(content))
		opts, err := ProcessConfigFile(conf)
		if err != nil {
			t.Fatalf("Error processing file: %v", err)
		}
		if len(opts.LeafNode.Remotes) != 1 {
			t.Fatalf("Expected 1 remote, got %d", len(opts.LeafNode.Remotes))
		}
		expected := &RemoteLeafOpts{
			LocalAccount: "foobar",
			Credentials:  "./my.creds",
		}
		u, _ := url.Parse("nats-leaf://127.0.0.1:2222")
		expected.URLs = append(expected.URLs, u)
		if !reflect.DeepEqual(opts.LeafNode.Remotes[0], expected) {
			t.Fatalf("Expected %v, got %v", expected, opts.LeafNode.Remotes[0])
		}
	})

	t.Run("parse config file with tilde path", func(t *testing.T) {
		if runtime.GOOS == "windows" {
			t.SkipNow()
		}

		origHome := os.Getenv("HOME")
		defer os.Setenv("HOME", origHome)
		os.Setenv("HOME", "/home/foo")

		content := `
		leafnodes {
			remotes = [
				{
					url: nats-leaf://127.0.0.1:2222
					account: foobar // Local Account to bind to..
					credentials: "~/my.creds"
				}
			]
		}
		`
		conf := createConfFile(t, []byte(content))
		opts, err := ProcessConfigFile(conf)
		if err != nil {
			t.Fatalf("Error processing file: %v", err)
		}
		expected := &RemoteLeafOpts{
			LocalAccount: "foobar",
			Credentials:  "/home/foo/my.creds",
		}
		u, _ := url.Parse("nats-leaf://127.0.0.1:2222")
		expected.URLs = append(expected.URLs, u)
		if !reflect.DeepEqual(opts.LeafNode.Remotes[0], expected) {
			t.Fatalf("Expected %v, got %v", expected, opts.LeafNode.Remotes[0])
		}
	})

	t.Run("url ordering", func(t *testing.T) {
		// 16! possible permutations.
		orderedURLs := make([]string, 0, 16)
		for i := 0; i < cap(orderedURLs); i++ {
			orderedURLs = append(orderedURLs, fmt.Sprintf("nats-leaf://host%d:7422", i))
		}
		confURLs, err := json.Marshal(orderedURLs)
		if err != nil {
			t.Fatal(err)
		}

		content := `
		port: -1
		accounts: {
			A { users [ {user: a, password: a} ]}
			B { users [ {user: b, password: b} ]}
		}
		leafnodes {
			remotes = [
				{
					dont_randomize: true
					urls: %[1]s
					account: "A"
				}
				{
					urls: %[1]s
					account: "B"
				}
			]
		}
		`
		conf := createConfFile(t, []byte(fmt.Sprintf(content, confURLs)))

		s, _ := RunServerWithConfig(conf)
		defer s.Shutdown()

		var r1, r2 *leafNodeCfg
		s.mu.RLock()
		for r := range s.leafRemoteCfgs {
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
			if got, want := gotOrdered[i].String(), orderedURLs[i]; got != want {
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
			if gotRandom[i].String() != orderedURLs[i] {
				random = true
				break
			}
		}
		if !random {
			t.Fatal("Expected urls to be random")
		}
	})

	t.Run("parse config file js_cluster_migrate", func(t *testing.T) {
		content := `
		leafnodes {
			remotes = [
				{
					url: nats-leaf://127.0.0.1:2222
					account: foo // Local Account to bind to..
					credentials: "./my.creds"
					js_cluster_migrate: true
				},
				{
					url: nats-leaf://127.0.0.1:2222
					account: bar // Local Account to bind to..
					credentials: "./my.creds"
					js_cluster_migrate: {
						leader_migrate_delay: 30s
					}
				},
				{
					url: nats-leaf://127.0.0.1:2222
					account: baz // Local Account to bind to..
					credentials: "./my.creds"
					js_cluster_migrate: false
				}
			]
		}
		`
		conf := createConfFile(t, []byte(content))
		opts, err := ProcessConfigFile(conf)
		if err != nil {
			t.Fatalf("Error processing file: %v", err)
		}
		if len(opts.LeafNode.Remotes) != 3 {
			t.Fatalf("Expected 2 remote, got %d", len(opts.LeafNode.Remotes))
		}
		u, _ := url.Parse("nats-leaf://127.0.0.1:2222")
		expected := []*RemoteLeafOpts{
			{
				URLs:                    []*url.URL{u},
				LocalAccount:            "foo",
				Credentials:             "./my.creds",
				JetStreamClusterMigrate: true,
			},
			{
				URLs:                         []*url.URL{u},
				LocalAccount:                 "bar",
				Credentials:                  "./my.creds",
				JetStreamClusterMigrate:      true,
				JetStreamClusterMigrateDelay: 30 * time.Second,
			},
			{
				URLs:                    []*url.URL{u},
				LocalAccount:            "baz",
				Credentials:             "./my.creds",
				JetStreamClusterMigrate: false,
			},
		}
		if !reflect.DeepEqual(opts.LeafNode.Remotes, expected) {
			t.Fatalf("Expected %v, got %v", expected, opts.LeafNode.Remotes)
		}
	})

}

func TestLargeMaxControlLine(t *testing.T) {
	confFileName := createConfFile(t, []byte(`
		max_control_line = 3000000000
	`))
	if _, err := ProcessConfigFile(confFileName); err == nil {
		t.Fatalf("Expected an error from too large of a max_control_line entry")
	}
}

func TestLargeMaxPayload(t *testing.T) {
	confFileName := createConfFile(t, []byte(`
		max_payload = 3000000000
	`))
	if _, err := ProcessConfigFile(confFileName); err == nil {
		t.Fatalf("Expected an error from too large of a max_payload entry")
	}

	confFileName = createConfFile(t, []byte(`
		max_payload = 100000
		max_pending = 50000
	`))
	o := LoadConfig(confFileName)
	s, err := NewServer(o)
	if err == nil || !strings.Contains(err.Error(), "cannot be higher") {
		if s != nil {
			s.Shutdown()
		}
		t.Fatalf("Unexpected error: %v", err)
	}
}

func TestHandleUnknownTopLevelConfigurationField(t *testing.T) {
	conf := createConfFile(t, []byte(`
		port: 1234
		streaming {
			id: "me"
		}
	`))

	// Verify that we get an error because of unknown "streaming" field.
	opts := &Options{}
	if err := opts.ProcessConfigFile(conf); err == nil || !strings.Contains(err.Error(), "streaming") {
		t.Fatal("Expected error, got none")
	}

	// Verify that if that is set, we get no error
	NoErrOnUnknownFields(true)
	defer NoErrOnUnknownFields(false)

	if err := opts.ProcessConfigFile(conf); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if opts.Port != 1234 {
		t.Fatalf("Port was not parsed correctly: %v", opts.Port)
	}

	// Verify that ignore works only on top level fields.
	changeCurrentConfigContentWithNewContent(t, conf, []byte(`
		port: 1234
		cluster {
			non_top_level_unknown_field: 123
		}
		streaming {
			id: "me"
		}
	`))
	if err := opts.ProcessConfigFile(conf); err == nil || !strings.Contains(err.Error(), "non_top_level") {
		t.Fatal("Expected error, got none")
	}
}

func TestSublistNoCacheConfig(t *testing.T) {
	confFileName := createConfFile(t, []byte(`
      disable_sublist_cache: true
    `))
	opts, err := ProcessConfigFile(confFileName)
	if err != nil {
		t.Fatalf("Received an error reading config file: %v", err)
	}
	if !opts.NoSublistCache {
		t.Fatalf("Expected sublist cache to be disabled")
	}
}

func TestSublistNoCacheConfigOnAccounts(t *testing.T) {
	confFileName := createConfFile(t, []byte(`
	  listen: "127.0.0.1:-1"
      disable_sublist_cache: true

	  accounts {
		synadia {
			users [ {nkey : UBAAQWTW6CG2G6ANGNKB5U2B7HRWHSGMZEZX3AQSAJOQDAUGJD46LD2E} ]
		}
		nats.io {
			users [ {nkey : UC6NLCN7AS34YOJVCYD4PJ3QB7QGLYG5B5IMBT25VW5K4TNUJODM7BOX} ]
		}
	  }
	  no_sys_acc = true
    `))

	s, _ := RunServerWithConfig(confFileName)
	defer s.Shutdown()

	// Check that all account sublists do not have caching enabled.
	ta := s.numReservedAccounts() + 2
	if la := s.numAccounts(); la != ta {
		t.Fatalf("Expected to have a server with %d active accounts, got %v", ta, la)
	}

	s.accounts.Range(func(k, v any) bool {
		acc := v.(*Account)
		if acc == nil {
			t.Fatalf("Expected non-nil sublist for account")
		}
		if acc.sl.CacheEnabled() {
			t.Fatalf("Expected the account sublist to not have caching enabled")
		}
		return true
	})
}

func TestParsingResponsePermissions(t *testing.T) {
	template := `
		listen: "127.0.0.1:-1"
		authorization {
			users [
				{
					user: ivan
					password: pwd
					permissions {
						allow_responses {
							%s
							%s
						}
					}
				}
			]
		}
	`

	check := func(t *testing.T, conf string, expectedError string, expectedMaxMsgs int, expectedTTL time.Duration) {
		t.Helper()
		opts, err := ProcessConfigFile(conf)
		if expectedError != "" {
			if err == nil || !strings.Contains(err.Error(), expectedError) {
				t.Fatalf("Expected error about %q, got %q", expectedError, err)
			}
			// OK!
			return
		}
		if err != nil {
			t.Fatalf("Error on process: %v", err)
		}
		u := opts.Users[0]
		p := u.Permissions.Response
		if p == nil {
			t.Fatalf("Expected response permissions to be set, it was not")
		}
		if n := p.MaxMsgs; n != expectedMaxMsgs {
			t.Fatalf("Expected response max msgs to be %v, got %v", expectedMaxMsgs, n)
		}
		if ttl := p.Expires; ttl != expectedTTL {
			t.Fatalf("Expected response ttl to be %v, got %v", expectedTTL, ttl)
		}
	}

	// Check defaults
	conf := createConfFile(t, []byte(fmt.Sprintf(template, "", "")))
	check(t, conf, "", DEFAULT_ALLOW_RESPONSE_MAX_MSGS, DEFAULT_ALLOW_RESPONSE_EXPIRATION)

	conf = createConfFile(t, []byte(fmt.Sprintf(template, "max: 10", "")))
	check(t, conf, "", 10, DEFAULT_ALLOW_RESPONSE_EXPIRATION)

	conf = createConfFile(t, []byte(fmt.Sprintf(template, "", "ttl: 5s")))
	check(t, conf, "", DEFAULT_ALLOW_RESPONSE_MAX_MSGS, 5*time.Second)

	conf = createConfFile(t, []byte(fmt.Sprintf(template, "max: 0", "")))
	check(t, conf, "", DEFAULT_ALLOW_RESPONSE_MAX_MSGS, DEFAULT_ALLOW_RESPONSE_EXPIRATION)

	conf = createConfFile(t, []byte(fmt.Sprintf(template, "", `ttl: "0s"`)))
	check(t, conf, "", DEFAULT_ALLOW_RESPONSE_MAX_MSGS, DEFAULT_ALLOW_RESPONSE_EXPIRATION)

	// Check normal values
	conf = createConfFile(t, []byte(fmt.Sprintf(template, "max: 10", `ttl: "5s"`)))
	check(t, conf, "", 10, 5*time.Second)

	// Check negative values ok
	conf = createConfFile(t, []byte(fmt.Sprintf(template, "max: -1", `ttl: "5s"`)))
	check(t, conf, "", -1, 5*time.Second)

	conf = createConfFile(t, []byte(fmt.Sprintf(template, "max: 10", `ttl: "-1s"`)))
	check(t, conf, "", 10, -1*time.Second)

	conf = createConfFile(t, []byte(fmt.Sprintf(template, "max: -1", `ttl: "-1s"`)))
	check(t, conf, "", -1, -1*time.Second)

	// Check parsing errors
	conf = createConfFile(t, []byte(fmt.Sprintf(template, "unknown_field: 123", "")))
	check(t, conf, "Unknown field", 0, 0)

	conf = createConfFile(t, []byte(fmt.Sprintf(template, "max: 10", "ttl: 123")))
	check(t, conf, "not a duration string", 0, 0)

	conf = createConfFile(t, []byte(fmt.Sprintf(template, "max: 10", "ttl: xyz")))
	check(t, conf, "error parsing expires", 0, 0)
}

func TestExpandPath(t *testing.T) {
	if runtime.GOOS == "windows" {
		origUserProfile := os.Getenv("USERPROFILE")
		origHomeDrive, origHomePath := os.Getenv("HOMEDRIVE"), os.Getenv("HOMEPATH")
		defer func() {
			os.Setenv("USERPROFILE", origUserProfile)
			os.Setenv("HOMEDRIVE", origHomeDrive)
			os.Setenv("HOMEPATH", origHomePath)
		}()

		cases := []struct {
			path        string
			userProfile string
			homeDrive   string
			homePath    string

			wantPath string
			wantErr  bool
		}{
			// Missing HOMEDRIVE and HOMEPATH.
			{path: "/Foo/Bar", userProfile: `C:\Foo\Bar`, wantPath: "/Foo/Bar"},
			{path: "Foo/Bar", userProfile: `C:\Foo\Bar`, wantPath: "Foo/Bar"},
			{path: "~/Fizz", userProfile: `C:\Foo\Bar`, wantPath: `C:\Foo\Bar\Fizz`},
			{path: `${HOMEDRIVE}${HOMEPATH}\Fizz`, homeDrive: `C:`, homePath: `\Foo\Bar`, wantPath: `C:\Foo\Bar\Fizz`},

			// Missing USERPROFILE.
			{path: "~/Fizz", homeDrive: "X:", homePath: `\Foo\Bar`, wantPath: `X:\Foo\Bar\Fizz`},

			// Set all environment variables. HOMEDRIVE and HOMEPATH take
			// precedence.
			{path: "~/Fizz", userProfile: `C:\Foo\Bar`,
				homeDrive: "X:", homePath: `\Foo\Bar`, wantPath: `X:\Foo\Bar\Fizz`},

			// Missing all environment variables.
			{path: "~/Fizz", wantErr: true},
		}
		for i, c := range cases {
			t.Run(fmt.Sprintf("windows case %d", i), func(t *testing.T) {
				os.Setenv("USERPROFILE", c.userProfile)
				os.Setenv("HOMEDRIVE", c.homeDrive)
				os.Setenv("HOMEPATH", c.homePath)

				gotPath, err := expandPath(c.path)
				if !c.wantErr && err != nil {
					t.Fatalf("unexpected error: got=%v; want=%v", err, nil)
				} else if c.wantErr && err == nil {
					t.Fatalf("unexpected success: got=%v; want=%v", nil, "err")
				}

				if gotPath != c.wantPath {
					t.Fatalf("unexpected path: got=%v; want=%v", gotPath, c.wantPath)
				}
			})
		}

		return
	}

	// Unix tests

	origHome := os.Getenv("HOME")
	defer os.Setenv("HOME", origHome)

	cases := []struct {
		path     string
		home     string
		wantPath string
		wantErr  bool
	}{
		{path: "/foo/bar", home: "/fizz/buzz", wantPath: "/foo/bar"},
		{path: "foo/bar", home: "/fizz/buzz", wantPath: "foo/bar"},
		{path: "~/fizz", home: "/foo/bar", wantPath: "/foo/bar/fizz"},
		{path: "$HOME/fizz", home: "/foo/bar", wantPath: "/foo/bar/fizz"},

		// missing HOME env var
		{path: "~/fizz", wantErr: true},
	}
	for i, c := range cases {
		t.Run(fmt.Sprintf("unix case %d", i), func(t *testing.T) {
			os.Setenv("HOME", c.home)

			gotPath, err := expandPath(c.path)
			if !c.wantErr && err != nil {
				t.Fatalf("unexpected error: got=%v; want=%v", err, nil)
			} else if c.wantErr && err == nil {
				t.Fatalf("unexpected success: got=%v; want=%v", nil, "err")
			}

			if gotPath != c.wantPath {
				t.Fatalf("unexpected path: got=%v; want=%v", gotPath, c.wantPath)
			}
		})
	}
}

func TestNoAuthUserCode(t *testing.T) {
	confFileName := createConfFile(t, []byte(`
		listen: "127.0.0.1:-1"
		no_auth_user: $NO_AUTH_USER

		accounts {
			synadia {
				users [
					{user: "a", password: "a"},
					{nkey : UBAAQWTW6CG2G6ANGNKB5U2B7HRWHSGMZEZX3AQSAJOQDAUGJD46LD2E},
				]
			}
			acc {
				users [
					{user: "c", password: "c"}
				]
			}
		}
		# config for $G
		authorization {
			users [
				{user: "b", password: "b"}
			]
		}
	`))
	defer os.Unsetenv("NO_AUTH_USER")

	for _, user := range []string{"a", "b", "b"} {
		t.Run(user, func(t *testing.T) {
			os.Setenv("NO_AUTH_USER", user)
			opts, err := ProcessConfigFile(confFileName)
			if err != nil {
				t.Fatalf("Received unexpected error %s", err)
			} else {
				opts.NoLog = true
				srv := RunServer(opts)
				nc, err := nats.Connect(fmt.Sprintf("nats://127.0.0.1:%d", opts.Port))
				if err != nil {
					t.Fatalf("couldn't connect %s", err)
				}
				nc.Close()
				srv.Shutdown()
			}
		})
	}

	for _, badUser := range []string{"notthere", "UBAAQWTW6CG2G6ANGNKB5U2B7HRWHSGMZEZX3AQSAJOQDAUGJD46LD2F"} {
		t.Run(badUser, func(t *testing.T) {
			os.Setenv("NO_AUTH_USER", badUser)
			opts, err := ProcessConfigFile(confFileName)
			if err != nil {
				t.Fatalf("Received unexpected error %s", err)
			}
			s, err := NewServer(opts)
			if err != nil {
				if !strings.HasPrefix(err.Error(), "no_auth_user") {
					t.Fatalf("Received unexpected error %s", err)
				}
				return // error looks as expected
			}
			s.Shutdown()
			t.Fatalf("Received no error, where no_auth_user error was expected")
		})
	}

}

const operatorJwtWithSysAccAndUrlResolver = `
	listen: "127.0.0.1:-1"
	operator: eyJ0eXAiOiJqd3QiLCJhbGciOiJlZDI1NTE5In0.eyJqdGkiOiJJVEdJNjNCUUszM1VNN1pBSzZWT1RXNUZEU01ESlNQU1pRQ0RMNUlLUzZQTVhBU0ROQ01RIiwiaWF0IjoxNTg5ODM5MjA1LCJpc3MiOiJPQ1k2REUyRVRTTjNVT0RGVFlFWEJaTFFMSTdYNEdTWFI1NE5aQzRCQkxJNlFDVFpVVDY1T0lWTiIsIm5hbWUiOiJPUCIsInN1YiI6Ik9DWTZERTJFVFNOM1VPREZUWUVYQlpMUUxJN1g0R1NYUjU0TlpDNEJCTEk2UUNUWlVUNjVPSVZOIiwidHlwZSI6Im9wZXJhdG9yIiwibmF0cyI6eyJhY2NvdW50X3NlcnZlcl91cmwiOiJodHRwOi8vbG9jYWxob3N0OjgwMDAvand0L3YxIiwib3BlcmF0b3Jfc2VydmljZV91cmxzIjpbIm5hdHM6Ly9sb2NhbGhvc3Q6NDIyMiJdLCJzeXN0ZW1fYWNjb3VudCI6IkFEWjU0N0IyNFdIUExXT0s3VE1MTkJTQTdGUUZYUjZVTTJOWjRISE5JQjdSREZWWlFGT1o0R1FRIn19.3u710KqMLwgXwsMvhxfEp9xzK84XyAZ-4dd6QY0T6hGj8Bw9mS-HcQ7HbvDDNU01S61tNFfpma_JR6LtB3ixBg
`

func TestReadOperatorJWT(t *testing.T) {
	confFileName := createConfFile(t, []byte(operatorJwtWithSysAccAndUrlResolver))
	opts, err := ProcessConfigFile(confFileName)
	if err != nil {
		t.Fatalf("Received unexpected error %s", err)
	}
	if opts.SystemAccount != "ADZ547B24WHPLWOK7TMLNBSA7FQFXR6UM2NZ4HHNIB7RDFVZQFOZ4GQQ" {
		t.Fatalf("Expected different SystemAccount: %s", opts.SystemAccount)
	}
	if r, ok := opts.AccountResolver.(*URLAccResolver); !ok {
		t.Fatalf("Expected different SystemAccount: %s", opts.SystemAccount)
	} else if r.url != "http://localhost:8000/jwt/v1/accounts/" {
		t.Fatalf("Expected different SystemAccount: %s", r.url)
	}
}

const operatorJwtList = `
	listen: "127.0.0.1:-1"
    system_account = SYSACC
	operator: [
        eyJ0eXAiOiJqd3QiLCJhbGciOiJlZDI1NTE5In0.eyJqdGkiOiJJVEdJNjNCUUszM1VNN1pBSzZWT1RXNUZEU01ESlNQU1pRQ0RMNUlLUzZQTVhBU0ROQ01RIiwiaWF0IjoxNTg5ODM5MjA1LCJpc3MiOiJPQ1k2REUyRVRTTjNVT0RGVFlFWEJaTFFMSTdYNEdTWFI1NE5aQzRCQkxJNlFDVFpVVDY1T0lWTiIsIm5hbWUiOiJPUCIsInN1YiI6Ik9DWTZERTJFVFNOM1VPREZUWUVYQlpMUUxJN1g0R1NYUjU0TlpDNEJCTEk2UUNUWlVUNjVPSVZOIiwidHlwZSI6Im9wZXJhdG9yIiwibmF0cyI6eyJhY2NvdW50X3NlcnZlcl91cmwiOiJodHRwOi8vbG9jYWxob3N0OjgwMDAvand0L3YxIiwib3BlcmF0b3Jfc2VydmljZV91cmxzIjpbIm5hdHM6Ly9sb2NhbGhvc3Q6NDIyMiJdLCJzeXN0ZW1fYWNjb3VudCI6IkFEWjU0N0IyNFdIUExXT0s3VE1MTkJTQTdGUUZYUjZVTTJOWjRISE5JQjdSREZWWlFGT1o0R1FRIn19.3u710KqMLwgXwsMvhxfEp9xzK84XyAZ-4dd6QY0T6hGj8Bw9mS-HcQ7HbvDDNU01S61tNFfpma_JR6LtB3ixBg,
        eyJ0eXAiOiJKV1QiLCJhbGciOiJlZDI1NTE5LW5rZXkifQ.eyJqdGkiOiIzTVJCS1BRTU1IUjdOQVFQU080NUlWTlkyMzVMRlQyTEs0WkZFVU1KWU9EWUJXU0RXWlRBIiwiaWF0IjoxNzI2NTYwMjAwLCJpc3MiOiJPQkxPR1VCSVVQSkhGVE00RjRaTE9CR1BMSlBJRjRTR0JDWUVERUtFUVNNWVVaTVFTMkRGTUUyWCIsIm5hbWUiOiJvcDIiLCJzdWIiOiJPQkxPR1VCSVVQSkhGVE00RjRaTE9CR1BMSlBJRjRTR0JDWUVERUtFUVNNWVVaTVFTMkRGTUUyWCIsIm5hdHMiOnsic2lnbmluZ19rZXlzIjpbIk9ES0xMSTZWWldWNk03V1RaV0I3MjVITE9MVFFRVERLNE5RR1ZFR0Q0Q083SjJMMlVJWk81U0dXIl0sInN5c3RlbV9hY2NvdW50IjoiQUNRVFdWR1NHSFlWWTNSNkQyV01PM1Y2TFYyTUdLNUI3RzQ3RTQzQkhKQjZGUVZZN0VITlRNTUciLCJ0eXBlIjoib3BlcmF0b3IiLCJ2ZXJzaW9uIjoyfX0.8kUmC6CwGLTJSs1zj_blsMpP5b6n2jZhZFNvMPXvJlRyyR5ZbCsxJ442BimaxaiosS8T-IFcZAIphtiOcqhRCg
    ]
`

func TestReadMultipleOperatorJWT(t *testing.T) {
	confFileName := createConfFile(t, []byte(operatorJwtList))
	opts, err := ProcessConfigFile(confFileName)
	if err != nil {
		t.Fatalf("Received unexpected error %s", err)
	}

	require_Equal(t, len(opts.TrustedOperators), 2)
	require_Equal(t, opts.TrustedOperators[0].Name, "OP")
	require_Equal(t, opts.TrustedOperators[1].Name, "op2")
	require_Equal(t, opts.TrustedOperators[0].SystemAccount, "ADZ547B24WHPLWOK7TMLNBSA7FQFXR6UM2NZ4HHNIB7RDFVZQFOZ4GQQ")
	require_Equal(t, opts.TrustedOperators[1].SystemAccount, "ACQTWVGSGHYVY3R6D2WMO3V6LV2MGK5B7G47E43BHJB6FQVY7EHNTMMG")
	// check if system account precedence is correct
	require_Equal(t, opts.SystemAccount, "SYSACC")

}

// using memory resolver so this test does not have to start the memory resolver
const operatorJwtWithSysAccAndMemResolver = `
	listen: "127.0.0.1:-1"
	// Operator "TESTOP"
	operator: eyJ0eXAiOiJqd3QiLCJhbGciOiJlZDI1NTE5In0.eyJqdGkiOiJLRTZRU0tWTU1VWFFKNFZCTDNSNDdGRFlIWElaTDRZSE1INjVIT0k1UjZCNUpPUkxVQlZBIiwiaWF0IjoxNTg5OTE2MzgyLCJpc3MiOiJPQVRUVkJYTElVTVRRT1FXVUEySU0zRkdUQlFRSEFHUEZaQTVET05NTlFSUlRQUjYzTERBTDM1WiIsIm5hbWUiOiJURVNUT1AiLCJzdWIiOiJPQVRUVkJYTElVTVRRT1FXVUEySU0zRkdUQlFRSEFHUEZaQTVET05NTlFSUlRQUjYzTERBTDM1WiIsInR5cGUiOiJvcGVyYXRvciIsIm5hdHMiOnsic3lzdGVtX2FjY291bnQiOiJBRFNQT1lNSFhKTjZKVllRQ0xSWjVYUTVJVU42QTNTMzNYQTROVjRWSDc0NDIzVTdVN1lSNFlWVyJ9fQ.HiyUtlk8kectKHeQHtuqFcjFt0RbYZE_WAqPCcoWlV2IFVdXuOTzShYEMgDmtgvsFG_zxNQOj08Gr6a06ovwBA
	resolver: MEMORY
	resolver_preload: {
  		// Account "TESTSYS"
  		ADSPOYMHXJN6JVYQCLRZ5XQ5IUN6A3S33XA4NV4VH74423U7U7YR4YVW: eyJ0eXAiOiJqd3QiLCJhbGciOiJlZDI1NTE5In0.eyJqdGkiOiI2WEtYUFZNTjdEVFlBSUE0R1JDWUxXUElSM1ZEM1Q2UVk2RFg3NURHTVFVWkdVWTJSRFNRIiwiaWF0IjoxNTg5OTE2MzIzLCJpc3MiOiJPQVRUVkJYTElVTVRRT1FXVUEySU0zRkdUQlFRSEFHUEZaQTVET05NTlFSUlRQUjYzTERBTDM1WiIsIm5hbWUiOiJURVNUU1lTIiwic3ViIjoiQURTUE9ZTUhYSk42SlZZUUNMUlo1WFE1SVVONkEzUzMzWEE0TlY0Vkg3NDQyM1U3VTdZUjRZVlciLCJ0eXBlIjoiYWNjb3VudCIsIm5hdHMiOnsibGltaXRzIjp7InN1YnMiOi0xLCJjb25uIjotMSwibGVhZiI6LTEsImltcG9ydHMiOi0xLCJleHBvcnRzIjotMSwiZGF0YSI6LTEsInBheWxvYWQiOi0xLCJ3aWxkY2FyZHMiOnRydWV9fX0.vhtWanIrOncdNfg-yO-7L61ccc-yRacvVtEsaIgWBEmW4czlEPhsiF1MkUKG91rtgcbwUf73ZIFEfja5MgFBAQ
	}
`

func TestReadOperatorJWTSystemAccountMatch(t *testing.T) {
	confFileName := createConfFile(t, []byte(operatorJwtWithSysAccAndMemResolver+`
		system_account: ADSPOYMHXJN6JVYQCLRZ5XQ5IUN6A3S33XA4NV4VH74423U7U7YR4YVW
	`))
	opts, err := ProcessConfigFile(confFileName)
	if err != nil {
		t.Fatalf("Received unexpected error %s", err)
	}
	s, err := NewServer(opts)
	if err != nil {
		t.Fatalf("Received unexpected error %s", err)
	}
	s.Shutdown()
}

func TestReadOperatorJWTSystemAccountMismatch(t *testing.T) {
	confFileName := createConfFile(t, []byte(operatorJwtWithSysAccAndMemResolver+`
		system_account: ADXJJCDCSRSMCOV25FXQW7R4QOG7R763TVEXBNWJHLBMBGWOJYG5XZBG
	`))
	opts, err := ProcessConfigFile(confFileName)
	if err != nil {
		t.Fatalf("Received unexpected error %s", err)
	}
	s, err := NewServer(opts)
	if err == nil {
		s.Shutdown()
		t.Fatalf("Received no error")
	} else if !strings.Contains(err.Error(), "system_account in config and operator JWT must be identical") {
		t.Fatalf("Received unexpected error %s", err)
	}
}

func TestReadOperatorAssertVersion(t *testing.T) {
	kp, _ := nkeys.CreateOperator()
	pk, _ := kp.PublicKey()
	op := jwt.NewOperatorClaims(pk)
	op.AssertServerVersion = "1.2.3"
	jwt, err := op.Encode(kp)
	if err != nil {
		t.Fatalf("Received unexpected error %s", err)
	}
	confFileName := createConfFile(t, []byte(fmt.Sprintf(`
		operator: %s
		resolver: MEM
	`, jwt)))
	opts, err := ProcessConfigFile(confFileName)
	if err != nil {
		t.Fatalf("Received unexpected error %s", err)
	}
	s, err := NewServer(opts)
	if err != nil {
		t.Fatalf("Received unexpected error %s", err)
	}
	s.Shutdown()
}

func TestReadOperatorAssertVersionFail(t *testing.T) {
	kp, _ := nkeys.CreateOperator()
	pk, _ := kp.PublicKey()
	op := jwt.NewOperatorClaims(pk)
	op.AssertServerVersion = "10.20.30"
	jwt, err := op.Encode(kp)
	if err != nil {
		t.Fatalf("Received unexpected error %s", err)
	}
	confFileName := createConfFile(t, []byte(fmt.Sprintf(`
		operator: %s
		resolver: MEM
	`, jwt)))
	opts, err := ProcessConfigFile(confFileName)
	if err != nil {
		t.Fatalf("Received unexpected error %s", err)
	}
	s, err := NewServer(opts)
	if err == nil {
		s.Shutdown()
		t.Fatalf("Received no error")
	} else if !strings.Contains(err.Error(), "expected major version 10 > server major version") {
		t.Fatal("expected different error got: ", err)
	}
}

func TestClusterNameAndGatewayNameConflict(t *testing.T) {
	conf := createConfFile(t, []byte(`
		listen: 127.0.0.1:-1
		cluster {
			name: A
			listen: 127.0.0.1:-1
		}
		gateway {
			name: B
			listen: 127.0.0.1:-1
		}
	`))

	opts, err := ProcessConfigFile(conf)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	if err := validateOptions(opts); err != ErrClusterNameConfigConflict {
		t.Fatalf("Expected ErrClusterNameConfigConflict got %v", err)
	}
}

func TestDefaultAuthTimeout(t *testing.T) {
	opts := DefaultOptions()
	opts.AuthTimeout = 0
	s := RunServer(opts)
	defer s.Shutdown()

	sopts := s.getOpts()
	if at := time.Duration(sopts.AuthTimeout * float64(time.Second)); at != AUTH_TIMEOUT {
		t.Fatalf("Expected auth timeout to be %v, got %v", AUTH_TIMEOUT, at)
	}
	s.Shutdown()

	opts = DefaultOptions()
	tc := &TLSConfigOpts{
		CertFile: "../test/configs/certs/server-cert.pem",
		KeyFile:  "../test/configs/certs/server-key.pem",
		CaFile:   "../test/configs/certs/ca.pem",
		Timeout:  4.0,
	}
	tlsConfig, err := GenTLSConfig(tc)
	if err != nil {
		t.Fatalf("Error generating tls config: %v", err)
	}
	opts.TLSConfig = tlsConfig
	opts.TLSTimeout = tc.Timeout
	s = RunServer(opts)
	defer s.Shutdown()

	sopts = s.getOpts()
	if sopts.AuthTimeout != 5 {
		t.Fatalf("Expected auth timeout to be %v, got %v", 5, sopts.AuthTimeout)
	}
}

func TestQueuePermissions(t *testing.T) {
	cfgFmt := `
		listen: 127.0.0.1:-1
		no_auth_user: u
		authorization {
			users [{
				user: u, password: pwd, permissions: { sub: { %s: ["foo.> *.dev"] } }
			}]
		}`
	errChan := make(chan error, 1)
	defer close(errChan)
	for _, test := range []struct {
		permType    string
		queue       string
		errExpected bool
	}{
		{"allow", "queue.dev", false},
		{"allow", "", true},
		{"allow", "bad", true},
		{"deny", "", false},
		{"deny", "queue.dev", true},
	} {
		t.Run(test.permType+test.queue, func(t *testing.T) {
			confFileName := createConfFile(t, []byte(fmt.Sprintf(cfgFmt, test.permType)))
			opts, err := ProcessConfigFile(confFileName)
			if err != nil {
				t.Fatalf("Received unexpected error %s", err)
			}
			opts.NoLog, opts.NoSigs = true, true
			s := RunServer(opts)
			defer s.Shutdown()
			nc, err := nats.Connect(fmt.Sprintf("nats://127.0.0.1:%d", opts.Port),
				nats.ErrorHandler(func(conn *nats.Conn, s *nats.Subscription, err error) {
					errChan <- err
				}))
			if err != nil {
				t.Fatalf("No error expected: %v", err)
			}
			defer nc.Close()
			if test.queue == "" {
				if _, err := nc.Subscribe("foo.bar", func(msg *nats.Msg) {}); err != nil {
					t.Fatalf("no error expected: %v", err)
				}
			} else {
				if _, err := nc.QueueSubscribe("foo.bar", test.queue, func(msg *nats.Msg) {}); err != nil {
					t.Fatalf("no error expected: %v", err)
				}
			}
			nc.Flush()
			select {
			case err := <-errChan:
				if !test.errExpected {
					t.Fatalf("Expected no error, got %v", err)
				}
				if !strings.Contains(err.Error(), `Permissions Violation for Subscription to "foo.bar"`) {
					t.Fatalf("error %v", err)
				}
			case <-time.After(150 * time.Millisecond):
				if test.errExpected {
					t.Fatal("Expected an error")
				}
			}
		})

	}
}

func TestQueueQualifierOnlyAllowedForSubscribePermissions(t *testing.T) {
	for _, tc := range []struct {
		name      string
		config    string
		shouldErr bool
	}{
		{
			name: "subscribe deny",
			config: `
				authorization {
					users: [{
						user: u
						password: p
						permissions: { subscribe: { deny: ["admin.secret workers"] } }
					}]
				}
			`,
		},
		{
			name: "publish deny",
			config: `
				authorization {
					users: [{
						user: u
						password: p
						permissions: { publish: { deny: ["admin.secret workers"] } }
					}]
				}
			`,
			shouldErr: true,
		},
		{
			name: "leaf deny imports",
			config: `
				leafnodes {
					remotes: [{
						url: "nats://127.0.0.1:7422"
						deny_imports: ["admin.secret workers"]
					}]
				}
			`,
			shouldErr: true,
		},
		{
			name: "leaf deny exports",
			config: `
				leafnodes {
					remotes: [{
						url: "nats://127.0.0.1:7422"
						deny_exports: ["admin.secret workers"]
					}]
				}
			`,
			shouldErr: true,
		},
		{
			name: "cluster export deny",
			config: `
				cluster {
					name: "C"
					permissions {
						export { deny: ["admin.secret workers"] }
					}
				}
			`,
			shouldErr: true,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			conf := createConfFile(t, []byte(tc.config))
			_, err := ProcessConfigFile(conf)
			if tc.shouldErr && err == nil {
				t.Fatal("Expected queue-qualified permission to be rejected")
			}
			if !tc.shouldErr && err != nil {
				t.Fatalf("Expected queue-qualified subscribe permission to be accepted, got %v", err)
			}
		})
	}

	opts := DefaultOptions()
	opts.Cluster.Permissions = &RoutePermissions{
		Export: &SubjectPermission{Deny: []string{"admin.secret workers"}},
	}
	if _, err := NewServer(opts); err == nil {
		t.Fatal("Expected programmatic queue-qualified cluster export permission to be rejected")
	}
}

func TestResolverPinnedAccountsFail(t *testing.T) {
	cfgFmt := `
		operator: %s
		resolver: URL(foo.bar)
		resolver_pinned_accounts: [%s]
	`

	conf := createConfFile(t, []byte(fmt.Sprintf(cfgFmt, ojwt, "f")))
	srv, err := NewServer(LoadConfig(conf))
	defer srv.Shutdown()
	require_Error(t, err)
	require_Contains(t, err.Error(), " is not a valid public account nkey")

	conf = createConfFile(t, []byte(fmt.Sprintf(cfgFmt, ojwt, "1, x")))
	_, err = ProcessConfigFile(conf)
	require_Error(t, err)
	require_Contains(t, "parsing resolver_pinned_accounts: unsupported type")
}

func TestMaxSubTokens(t *testing.T) {
	conf := createConfFile(t, []byte(`
		listen: 127.0.0.1:-1
		max_sub_tokens: 4
	`))

	s, _ := RunServerWithConfig(conf)
	defer s.Shutdown()

	nc, err := nats.Connect(s.ClientURL())
	require_NoError(t, err)
	defer nc.Close()

	errs := make(chan error, 1)

	nc.SetErrorHandler(func(_ *nats.Conn, _ *nats.Subscription, err error) {
		errs <- err
	})

	bad := "a.b.c.d.e"
	_, err = nc.SubscribeSync(bad)
	require_NoError(t, err)

	select {
	case e := <-errs:
		if !strings.Contains(e.Error(), "too many tokens") {
			t.Fatalf("Got wrong error: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Did not get the permissions error")
	}
}

func TestGetStorageSize(t *testing.T) {
	tt := []struct {
		input string
		want  int64
		err   bool
	}{
		{"1K", 1024, false},
		{"1M", 1048576, false},
		{"1G", 1073741824, false},
		{"1T", 1099511627776, false},
		{"1L", 0, true},
		{"TT", 0, true},
		{"", 0, false},
	}

	for _, v := range tt {
		var testErr bool
		got, err := getStorageSize(v.input)
		if err != nil {
			testErr = true
		}

		if got != v.want || v.err != testErr {
			t.Errorf("Got: %v, want %v with error: %v", got, v.want, testErr)
		}
	}

}

func TestAuthorizationAndAccountsMisconfigurations(t *testing.T) {
	// There is a test called TestConfigCheck but we can't use it
	// because the place where the error will be reported will depend
	// if the error is found while parsing authorization{} or
	// accounts{}, but we can't control the internal parsing of those
	// (due to lexer giving back a map and iteration over map is random)
	// regardless of the ordering in the configuration file.
	// The test is also repeated
	for _, test := range []struct {
		name   string
		config string
		err    string
	}{
		{
			"duplicate users",
			`
			authorization = {users = [ {user: "user1", pass: "pwd"} ] }
			accounts { ACC { users = [ {user: "user1"} ] } }
			`,
			fmt.Sprintf("Duplicate user %q detected", "user1"),
		},
		{
			"duplicate nkey",
			`
			authorization = {users = [ {nkey: UC6NLCN7AS34YOJVCYD4PJ3QB7QGLYG5B5IMBT25VW5K4TNUJODM7BOX} ] }
			accounts { ACC { users = [ {nkey: UC6NLCN7AS34YOJVCYD4PJ3QB7QGLYG5B5IMBT25VW5K4TNUJODM7BOX} ] } }
			`,
			fmt.Sprintf("Duplicate nkey %q detected", "UC6NLCN7AS34YOJVCYD4PJ3QB7QGLYG5B5IMBT25VW5K4TNUJODM7BOX"),
		},
		{
			"auth single user and password and accounts users",
			`
			authorization = {user: "user1", password: "pwd"}
			accounts = { ACC { users = [ {user: "user2", pass: "pwd"} ] } }
			`,
			"Can not have a single user/pass",
		},
		{
			"auth single user and password and accounts nkeys",
			`
			authorization = {user: "user1", password: "pwd"}
			accounts = { ACC { users = [ {nkey: UC6NLCN7AS34YOJVCYD4PJ3QB7QGLYG5B5IMBT25VW5K4TNUJODM7BOX} ] } }
			`,
			"Can not have a single user/pass",
		},
		{
			"auth token and accounts users",
			`
			authorization = {token: "my_token"}
			accounts = { ACC { users = [ {user: "user2", pass: "pwd"} ] } }
			`,
			"Can not have a token",
		},
		{
			"auth token and accounts nkeys",
			`
			authorization = {token: "my_token"}
			accounts = { ACC { users = [ {nkey: UC6NLCN7AS34YOJVCYD4PJ3QB7QGLYG5B5IMBT25VW5K4TNUJODM7BOX} ] } }
			`,
			"Can not have a token",
		},
		{
			"auth callout allowed accounts",
			`
			accounts {
				AUTH { users = [ {user: "auth", password: "auth"} ] }
				FOO {}
			}
			authorization {
				auth_callout {
					issuer: "ABJHLOVMPA4CI6R5KLNGOB4GSLNIY7IOUPAJC4YFNDLQVIOBYQGUWVLA"
					account: AUTH
					auth_users: [ auth ]
					allowed_accounts: [ BAR ]
				}
			}
			`,
			"auth_callout allowed account \"BAR\" not found in configured accounts",
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			conf := createConfFile(t, []byte(test.config))
			if _, err := ProcessConfigFile(conf); err == nil || !strings.Contains(err.Error(), test.err) {
				t.Fatalf("Expected error %q, got %q", test.err, err.Error())
			}
		})
	}
}

// TestProcessConfigString duplicates the previous test, but uses the (*Options).ProcessConfigString
// instead of the ProcessConfigFile function.
func TestProcessConfigString(t *testing.T) {
	for _, test := range []struct {
		name   string
		config string
		err    string
	}{
		{
			"duplicate users",
			`
			authorization = {users = [ {user: "user1", pass: "pwd"} ] }
			accounts { ACC { users = [ {user: "user1"} ] } }
			`,
			fmt.Sprintf("Duplicate user %q detected", "user1"),
		},
		{
			"duplicate nkey",
			`
			authorization = {users = [ {nkey: UC6NLCN7AS34YOJVCYD4PJ3QB7QGLYG5B5IMBT25VW5K4TNUJODM7BOX} ] }
			accounts { ACC { users = [ {nkey: UC6NLCN7AS34YOJVCYD4PJ3QB7QGLYG5B5IMBT25VW5K4TNUJODM7BOX} ] } }
			`,
			fmt.Sprintf("Duplicate nkey %q detected", "UC6NLCN7AS34YOJVCYD4PJ3QB7QGLYG5B5IMBT25VW5K4TNUJODM7BOX"),
		},
		{
			"auth single user and password and accounts users",
			`
			authorization = {user: "user1", password: "pwd"}
			accounts = { ACC { users = [ {user: "user2", pass: "pwd"} ] } }
			`,
			"Can not have a single user/pass",
		},
		{
			"auth single user and password and accounts nkeys",
			`
			authorization = {user: "user1", password: "pwd"}
			accounts = { ACC { users = [ {nkey: UC6NLCN7AS34YOJVCYD4PJ3QB7QGLYG5B5IMBT25VW5K4TNUJODM7BOX} ] } }
			`,
			"Can not have a single user/pass",
		},
		{
			"auth token and accounts users",
			`
			authorization = {token: "my_token"}
			accounts = { ACC { users = [ {user: "user2", pass: "pwd"} ] } }
			`,
			"Can not have a token",
		},
		{
			"auth token and accounts nkeys",
			`
			authorization = {token: "my_token"}
			accounts = { ACC { users = [ {nkey: UC6NLCN7AS34YOJVCYD4PJ3QB7QGLYG5B5IMBT25VW5K4TNUJODM7BOX} ] } }
			`,
			"Can not have a token",
		},
		{
			"auth callout allowed accounts",
			`
			accounts {
				AUTH { users = [ {user: "auth", password: "auth"} ] }
				FOO {}
			}
			authorization {
				auth_callout {
					issuer: "ABJHLOVMPA4CI6R5KLNGOB4GSLNIY7IOUPAJC4YFNDLQVIOBYQGUWVLA"
					account: AUTH
					auth_users: [ auth ]
					allowed_accounts: [ BAR ]
				}
			}
			`,
			"auth_callout allowed account \"BAR\" not found in configured accounts",
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			opts := &Options{}
			if err := opts.ProcessConfigString(test.config); err == nil || !strings.Contains(err.Error(), test.err) {
				t.Fatalf("Expected error %q, got %q", test.err, err.Error())
			}
		})
	}
}

func TestDefaultSentinel(t *testing.T) {
	d := `
		default_sentinel: "hello"
	`
	conf := createConfFile(t, []byte(d))
	opts := LoadConfig(conf)
	require_Equal(t, "hello", opts.DefaultSentinel)

	// if we validate, it will fail, we need an operator
	// tests verifying run of configured, elsewhere
	err := validateOptions(opts)
	require_Error(t, err)
	require_Equal(t, "default sentinel requires operators and accounts", err.Error())
}

func TestAuthorizationTimeoutConfigParsing(t *testing.T) {
	type testCase struct {
		name                string
		config              string
		expectParsed        float64
		expectRunning       float64
		expectErrorContains string
	}

	for _, tc := range []testCase{{
		name:          "defaults",
		config:        "authorization {}",
		expectParsed:  0,
		expectRunning: 2,
	}, {
		name: "explicit zero",
		config: `
			authorization {
				timeout: 0
			}`,
		expectParsed:  0,
		expectRunning: 2,
	}, {
		name: "explicit one",
		config: `
			authorization {
				timeout: 1
			}`,
		expectParsed:  1,
		expectRunning: 1,
	}, {
		name: "garbage",
		config: `
			authorization {
				timeout: random_garbage
			}`,
		expectErrorContains: `invalid duration "random_garbage"`,
	}, {
		name: "human readable",
		config: `
			authorization {
				timeout: 10s
			}`,
		expectParsed:  10,
		expectRunning: 10,
	}, {
		name: "bare values could be parsed as integers",
		config: `
			authorization {
				timeout: 1m
			}`,
		expectParsed:  1000000,
		expectRunning: 1000000,
	}, {
		name: "but quoted values will be parsed as durations",
		config: `
			authorization {
				timeout: "1m"
			}`,
		expectParsed:  60,
		expectRunning: 60,
	}, {
		name: "human readable minutes quoted",
		config: `
			authorization {
				timeout: "10m5s30ms"
			}`,
		expectParsed:  605.03,
		expectRunning: 605.03,
	}, {
		name: "floats work",
		config: `
			authorization {
				timeout: 0.091
			}`,
		expectParsed:  .091,
		expectRunning: .091,
	}, {
		name: "but no leading digit fails",
		config: `
			authorization {
				timeout: .091
			}`,
		expectErrorContains: "Floats must start with a digit",
	}} {
		t.Run(tc.name, func(t *testing.T) {
			opts, err := parseConfigTolerantly(t, tc.config)
			if tc.expectErrorContains != "" {
				if !strings.Contains(err.Error(), tc.expectErrorContains) {
					t.Errorf("Expected error like %q, got %v", tc.expectErrorContains, err)
				}
				return
			} else {
				if err != nil {
					t.Errorf("Error processing config: %v", err)
				}
			}

			if opts.AuthTimeout != tc.expectParsed {
				t.Errorf("Expected Parsed AuthTimeout to be %f, got %f", tc.expectParsed, opts.AuthTimeout)
			}

			s := RunServer(opts)
			defer s.Shutdown()

			sopts := s.getOpts()
			if sopts.AuthTimeout != tc.expectRunning {
				t.Errorf("Expected Running AuthTimeout to be %f, got %f", tc.expectRunning, sopts.AuthTimeout)
			}
		})
	}
}

func TestLeafnodeAuthorizationTimeoutConfigParsing(t *testing.T) {
	type testCase struct {
		name                string
		config              string
		expect              float64
		expectErrorContains string
	}

	for _, tc := range []testCase{{
		name:   "defaults",
		config: "leafnodes { authorization {} }",
		expect: 0,
	}, {
		name: "explicit zero",
		config: `
			leafnodes {
				authorization {
					timeout: 0
				}
			}`,
		expect: 0,
	}, {
		name: "explicit one",
		config: `
			leafnodes {
				authorization {
					timeout: 1
				}
			}`,
		expect: 1,
	}, {
		name: "garbage",
		config: `
			leafnodes {
				authorization {
					timeout: random_garbage
				}
			}`,
		expectErrorContains: `invalid duration "random_garbage"`,
	}, {
		name: "human readable",
		config: `
			leafnodes {
				authorization {
					timeout: 10s
				}
			}`,
		expect: 10,
	}, {
		name: "bare values could be parsed as integers",
		config: `
			leafnodes {
				authorization {
					timeout: 1m
				}
			}`,
		expect: 1000000,
	}, {
		name: "but quoted values will be parsed as durations",
		config: `
			leafnodes {
				authorization {
					timeout: "1m"
				}
			}`,
		expect: 60,
	}, {
		name: "human readable minutes quoted",
		config: `
			leafnodes {
				authorization {
					timeout: "10m5s30ms"
				}
			}`,
		expect: 605.03,
	}, {
		name: "floats work",
		config: `
			leafnodes {
				authorization {
					timeout: 0.091
				}
			}`,
		expect: .091,
	}, {
		name: "but no leading digit fails",
		config: `
			leafnodes {
				authorization {
					timeout: .091
				}
			}`,
		expectErrorContains: "Floats must start with a digit",
	}} {
		t.Run(tc.name, func(t *testing.T) {
			opts, err := parseConfigTolerantly(t, tc.config)
			if tc.expectErrorContains != "" {
				if !strings.Contains(err.Error(), tc.expectErrorContains) {
					t.Errorf("Expected error like %q, got %v", tc.expectErrorContains, err)
				}
				return
			} else {
				if err != nil {
					t.Errorf("Error processing config: %v", err)
				}
			}

			if opts.LeafNode.AuthTimeout != tc.expect {
				t.Errorf("Expected Parsed LeafNode AuthTimeout to be %f, got %f", tc.expect, opts.LeafNode.AuthTimeout)
			}
		})
	}
}

func parseConfigTolerantly(t *testing.T, data string) (*Options, error) {
	t.Helper()

	m, err := conf.ParseWithChecks(data)
	if err != nil {
		return nil, err
	}

	o := new(Options)
	if err = o.processConfigFile(_EMPTY_, m); err != nil {
		switch v := err.(type) {
		case *processConfigErr:
			if len(v.errors) > 0 {
				return o, err
			}
			for _, w := range v.warnings {
				t.Logf("WARNING: %v", w)
			}
			return o, nil
		default:
			t.Logf("Unexpected error type %T", v)
			return o, err
		}
	}

	return o, nil
}

func TestOptionsProxyTrustedKeys(t *testing.T) {
	o := DefaultOptions()
	o.Proxies = &ProxiesConfig{
		Trusted: []*ProxyConfig{
			{Key: "UCARKS2E3KVB7YORL2DG34XLT7PUCOL2SVM7YXV6ETHLW6Z46UUJ2VZ3"},
			{Key: "bad1"},
			{Key: "UD6AYQSOIN2IN5OGC6VQZCR4H3UFMIOXSW6NNS6N53CLJA4PB56CEJJI"},
			{Key: "bad2"},
		},
	}
	err := validateOptions(o)
	require_Error(t, err)
	require_Equal(t, "proxy trusted key \"bad1\" is invalid", err.Error())

	o.Proxies = &ProxiesConfig{
		Trusted: []*ProxyConfig{
			{Key: "UCARKS2E3KVB7YORL2DG34XLT7PUCOL2SVM7YXV6ETHLW6Z46UUJ2VZ3"},
			{Key: "UD6AYQSOIN2IN5OGC6VQZCR4H3UFMIOXSW6NNS6N53CLJA4PB56CEJJI"},
		},
	}
	s := RunServer(o)
	defer s.Shutdown()

	s.mu.RLock()
	defer s.mu.RUnlock()
	opts := s.getOpts()
	for i, kp := range s.proxiesKeyPairs {
		pub, err := kp.PublicKey()
		require_NoError(t, err)
		require_Equal(t, opts.Proxies.Trusted[i].Key, pub)
	}
}

func TestOptionsProxyRequired(t *testing.T) {
	conf := createConfFile(t, []byte(`
		port: -1
		authorization {
			user: user
			password: pwd
			proxy_required: true
		}
	`))
	o, err := ProcessConfigFile(conf)
	require_NoError(t, err)
	require_Equal(t, o.Username, "user")
	require_Equal(t, o.Password, "pwd")
	require_True(t, o.ProxyRequired)

	conf = createConfFile(t, []byte(`
		port: -1
		authorization {
			users: [
				{user: user1, password: pwd1}
				{user: user2, password: pwd2, proxy_required: true}
				{user: user3, password: pwd3, proxy_required: false}
				{nkey: "UCARKS2E3KVB7YORL2DG34XLT7PUCOL2SVM7YXV6ETHLW6Z46UUJ2VZ3", proxy_required: true}
				{nkey: "UD6AYQSOIN2IN5OGC6VQZCR4H3UFMIOXSW6NNS6N53CLJA4PB56CEJJI", proxy_required: false}
			]
		}
	`))
	o, err = ProcessConfigFile(conf)
	require_NoError(t, err)

	checkUsersAndNkeys := func(users []*User, hasNkeys bool, nkeys []*NkeyUser) {
		t.Helper()
		var found bool

		require_Len(t, len(users), 3)
		for _, u := range users {
			switch u.Username {
			case "user1", "user3":
				require_False(t, u.ProxyRequired)
			case "user2":
				require_True(t, u.ProxyRequired)
				found = true
			}
		}
		require_True(t, found)

		if !hasNkeys {
			return
		}

		found = false
		require_Len(t, len(nkeys), 2)
		for _, u := range nkeys {
			switch u.Nkey {
			case "UCARKS2E3KVB7YORL2DG34XLT7PUCOL2SVM7YXV6ETHLW6Z46UUJ2VZ3":
				require_True(t, u.ProxyRequired)
				found = true
			case "UD6AYQSOIN2IN5OGC6VQZCR4H3UFMIOXSW6NNS6N53CLJA4PB56CEJJI":
				require_False(t, u.ProxyRequired)
			}
		}
		require_True(t, found)
	}
	checkUsersAndNkeys(o.Users, true, o.Nkeys)

	conf = createConfFile(t, []byte(`
		port: -1
		accounts {
			A {
				users: [
					{user: user1, password: pwd1}
					{user: user2, password: pwd2, proxy_required: true}
					{user: user3, password: pwd3, proxy_required: false}
					{nkey: "UCARKS2E3KVB7YORL2DG34XLT7PUCOL2SVM7YXV6ETHLW6Z46UUJ2VZ3", proxy_required: true}
					{nkey: "UD6AYQSOIN2IN5OGC6VQZCR4H3UFMIOXSW6NNS6N53CLJA4PB56CEJJI", proxy_required: false}
				]
			}
		}
	`))
	o, err = ProcessConfigFile(conf)
	require_NoError(t, err)
	require_Len(t, len(o.Accounts), 1)
	require_Equal(t, o.Accounts[0].Name, "A")
	checkUsersAndNkeys(o.Users, true, o.Nkeys)

	conf = createConfFile(t, []byte(`
		port: -1
		leafnodes {
			port: -1
			authorization {
				user: user
				password: pwd
				proxy_required: true
			}
		}
	`))
	o, err = ProcessConfigFile(conf)
	require_NoError(t, err)
	require_Equal(t, o.LeafNode.Username, "user")
	require_Equal(t, o.LeafNode.Password, "pwd")
	require_True(t, o.LeafNode.ProxyRequired)

	conf = createConfFile(t, []byte(`
		port: -1
		leafnodes {
			port: -1
			authorization {
				nkey: "UCARKS2E3KVB7YORL2DG34XLT7PUCOL2SVM7YXV6ETHLW6Z46UUJ2VZ3"
				proxy_required: true
			}
		}
	`))
	o, err = ProcessConfigFile(conf)
	require_NoError(t, err)
	require_Equal(t, o.LeafNode.Nkey, "UCARKS2E3KVB7YORL2DG34XLT7PUCOL2SVM7YXV6ETHLW6Z46UUJ2VZ3")
	require_True(t, o.LeafNode.ProxyRequired)

	conf = createConfFile(t, []byte(`
		port: -1
		leafnodes: {
			port: -1
			authorization {
				users: [
					{user: user1, password: pwd1}
					{user: user2, password: pwd2, proxy_required: true}
					{user: user3, password: pwd3, proxy_required: false}
				]
			}
		}
	`))
	o, err = ProcessConfigFile(conf)
	require_NoError(t, err)
	checkUsersAndNkeys(o.LeafNode.Users, false, nil)
}

// TestNewServerFromConfigFunctionality tests the NewServerFromConfig() function
// to ensure it properly processes config files and creates servers correctly.
func TestNewServerFromConfigFunctionality(t *testing.T) {
	// Test 1: Error handling - invalid configuration
	confFileName := createConfFile(t, []byte(`
		max_payload = 3000000000
	`))

	opts1 := &Options{
		ConfigFile: confFileName,
	}

	// Should fail due to oversized max_payload (same as TestLargeMaxPayload)
	if _, err := NewServerFromConfig(opts1); err == nil {
		t.Fatalf("Expected an error from too large of a max_payload entry")
	}

	// Test 2: Config validation error - max_pending > max_payload
	confFileName = createConfFile(t, []byte(`
		max_payload = 100000
		max_pending = 50000
	`))

	opts2 := &Options{
		ConfigFile: confFileName,
	}

	// This should trigger validation error (same as TestLargeMaxPayload)
	server, err := NewServerFromConfig(opts2)
	if err == nil || !strings.Contains(err.Error(), "cannot be higher") {
		if server != nil {
			server.Shutdown()
		}
		t.Fatalf("Expected validation error, got: %v", err)
	}
}

// TestNewServerFromConfigVsLoadConfig tests that NewServerFromConfig produces
// equivalent results to the traditional LoadConfig approach.
func TestNewServerFromConfigVsLoadConfig(t *testing.T) {
	confFileName := createConfFile(t, []byte(`
		port = 4224
		max_payload = 4194304
		max_connections = 200
		ping_interval = "30s"
	`))

	// Method 1: Using LoadConfig (traditional approach)
	opts1 := LoadConfig(confFileName)

	// Method 2: Using NewServerFromConfig (new approach for embedded servers)
	opts2 := &Options{ConfigFile: confFileName}

	// Test 1: Both should be able to create servers successfully
	server1, err := NewServer(opts1)
	if err != nil {
		t.Fatalf("Failed to create server with LoadConfig options: %v", err)
	}
	server1.Shutdown()

	server2, err := NewServerFromConfig(opts2)
	if err != nil {
		t.Fatalf("Failed to create server with NewServerFromConfig: %v", err)
	}
	server2.Shutdown()

	// Test 2: Both methods should produce equivalent results - normalize test environment fields
	// LoadConfig sets these fields for testing, so we need to match them for fair comparison
	opts2.NoSigs, opts2.NoLog = true, opts2.LogFile == _EMPTY_

	checkOptionsEqual(t, opts1, opts2)
}

func TestWriteDeadlineConfigParsing(t *testing.T) {
	type testCase struct {
		name   string
		config string
		expect func(t *testing.T, opts *Options)
	}

	for _, tc := range []testCase{
		{
			name: "LeafNode",
			config: `
				leafnodes {
					write_deadline: 5s
				}
			`,
			expect: func(t *testing.T, opts *Options) {
				require_Equal(t, opts.LeafNode.WriteDeadline, 5*time.Second)
			},
		},
		{
			name: "Gateway",
			config: `
				gateway {
					write_deadline: 6s
				}
			`,
			expect: func(t *testing.T, opts *Options) {
				require_Equal(t, opts.Gateway.WriteDeadline, 6*time.Second)
			},
		},
		{
			name: "Cluster",
			config: `
				cluster {
					write_deadline: 7s
				}
			`,
			expect: func(t *testing.T, opts *Options) {
				require_Equal(t, opts.Cluster.WriteDeadline, 7*time.Second)
			},
		},
		{
			name: "Global",
			config: `
				write_deadline: 8s
			`,
			expect: func(t *testing.T, opts *Options) {
				require_Equal(t, opts.WriteDeadline, 8*time.Second)
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			opts, err := parseConfigTolerantly(t, tc.config)
			require_NoError(t, err)
			tc.expect(t, opts)
		})
	}
}

func TestWriteTimeoutConfigParsing(t *testing.T) {
	type testCase struct {
		name   string
		config string
		expect func(t *testing.T, opts *Options)
	}

	for str, pol := range map[string]WriteTimeoutPolicy{
		"default": WriteTimeoutPolicyDefault,
		"retry":   WriteTimeoutPolicyRetry,
		"close":   WriteTimeoutPolicyClose,
	} {
		for _, tc := range []testCase{
			{
				name: "LeafNode",
				config: fmt.Sprintf(`
					leafnodes {
						write_timeout: %s
					}
				`, str),
				expect: func(t *testing.T, opts *Options) {
					require_Equal(t, opts.LeafNode.WriteTimeout, pol)
				},
			},
			{
				name: "Gateway",
				config: fmt.Sprintf(`
					gateway {
						write_timeout: %s
					}
				`, str),
				expect: func(t *testing.T, opts *Options) {
					require_Equal(t, opts.Gateway.WriteTimeout, pol)
				},
			},
			{
				name: "Cluster",
				config: fmt.Sprintf(`
					cluster {
						write_timeout: %s
					}
				`, str),
				expect: func(t *testing.T, opts *Options) {
					require_Equal(t, opts.Cluster.WriteTimeout, pol)
				},
			},
			{
				name: "Global",
				config: fmt.Sprintf(`
					write_timeout: %s
				`, str),
				expect: func(t *testing.T, opts *Options) {
					require_Equal(t, opts.WriteTimeout, pol)
				},
			},
		} {
			t.Run(fmt.Sprintf("%s/%s", tc.name, str), func(t *testing.T) {
				opts, err := parseConfigTolerantly(t, tc.config)
				require_NoError(t, err)
				tc.expect(t, opts)
			})
		}
	}
}

func TestWebsocketPingIntervalConfig(t *testing.T) {
	// Test with string format (duration string)
	confFile := createConfFile(t, []byte(`
		websocket {
			port: 8080
			ping_interval: "30s"
		}
	`))
	opts, err := ProcessConfigFile(confFile)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if opts.Websocket.PingInterval != 30*time.Second {
		t.Fatalf("Expected websocket ping_interval to be 30s, got %v", opts.Websocket.PingInterval)
	}

	// Test with integer format (seconds)
	confFile = createConfFile(t, []byte(`
		websocket {
			port: 8080
			ping_interval: 45
		}
	`))
	opts, err = ProcessConfigFile(confFile)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if opts.Websocket.PingInterval != 45*time.Second {
		t.Fatalf("Expected websocket ping_interval to be 45s, got %v", opts.Websocket.PingInterval)
	}

	// Test with different duration format
	confFile = createConfFile(t, []byte(`
		websocket {
			port: 8080
			ping_interval: "2m"
		}
	`))
	opts, err = ProcessConfigFile(confFile)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if opts.Websocket.PingInterval != 2*time.Minute {
		t.Fatalf("Expected websocket ping_interval to be 2m, got %v", opts.Websocket.PingInterval)
	}

	// Test without ping_interval (should be zero/unset)
	confFile = createConfFile(t, []byte(`
		websocket {
			port: 8080
		}
	`))
	opts, err = ProcessConfigFile(confFile)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if opts.Websocket.PingInterval != 0 {
		t.Fatalf("Expected websocket ping_interval to be 0 (unset), got %v", opts.Websocket.PingInterval)
	}
}

// Test variables that reference other variables
func TestVarReferencesVar(t *testing.T) {
	confFileName := createConfFile(t, []byte(`
		A: 7890
		B: $A
		C: $B
		port: $C
	`))
	opts, err := ProcessConfigFile(confFileName)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if opts.Port != 7890 {
		t.Fatalf("Expected port 7890, found %d", opts.Port)
	}
}

// A variables that reference an environment variable
func TestVarReferencesEnvVar(t *testing.T) {
	confFileName := createConfFile(t, []byte(`
		A: $_TEST_ENV_NATS_PORT_
		B: $A
		C: $B
		port: $C
	`))

	defer os.Unsetenv("_TEST_ENV_NATS_PORT_")
	os.Setenv("_TEST_ENV_NATS_PORT_", "7890")
	opts, err := ProcessConfigFile(confFileName)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if opts.Port != 7890 {
		t.Fatalf("Expected port 7890, found %d", opts.Port)
	}
}

// Test a variable that references itself
func TestVarReferencesSelf(t *testing.T) {
	confFileName := createConfFile(t, []byte(`A: $A`))
	_, err := ProcessConfigFile(confFileName)
	if err == nil {
		t.Fatalf("Expected var not found error")
	}
	require_Contains(t, err.Error(),
		"variable reference for 'A' on line 1 can not be found")
}

// An environment variable can't reference a variable
func TestEnvVarReferencesVar(t *testing.T) {
	confFileName := createConfFile(t, []byte(`
		P: 8080
		port: $_TEST_ENV_NATS_PORT_
	`))

	defer os.Unsetenv("_TEST_ENV_NATS_PORT_")
	os.Setenv("_TEST_ENV_NATS_PORT_", "$P")

	_, err := ProcessConfigFile(confFileName)
	if err == nil {
		t.Fatalf("Expected var not found error")
	}
	require_Contains(t, err.Error(),
		"variable reference for 'P' on line 1 can not be found")
}

// Environment variables can reference other environment variables
func TestEnvVarReferencesEnvVar(t *testing.T) {
	confFileName := createConfFile(t, []byte(`
		port: $_TEST_ENV_A_
	`))

	defer os.Unsetenv("_TEST_ENV_A_")
	defer os.Unsetenv("_TEST_ENV_B_")
	defer os.Unsetenv("_TEST_ENV_C_")

	os.Setenv("_TEST_ENV_A_", "$_TEST_ENV_B_")
	os.Setenv("_TEST_ENV_B_", "$_TEST_ENV_C_")
	os.Setenv("_TEST_ENV_C_", "7890")

	opts, err := ProcessConfigFile(confFileName)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if opts.Port != 7890 {
		t.Fatalf("Expected port 7890, found %d", opts.Port)
	}
}

// Test an environment variable that references itself
func TestEnvVarReferencesSelf(t *testing.T) {
	confFileName := createConfFile(t, []byte(`
		TEST: $_TEST_ENV_
	`))

	defer os.Unsetenv("_TEST_ENV_")

	os.Setenv("_TEST_ENV_", "$_TEST_ENV_")

	_, err := ProcessConfigFile(confFileName)
	if err == nil {
		t.Fatalf("Expected an error")
	}
	require_Contains(t, err.Error(), "variable reference cycle")
}

// Test an environment variable that references itself through a cycle
func TestEnvVarReferencesSelfCycle(t *testing.T) {
	confFileName := createConfFile(t, []byte(`
		TEST: $_TEST_ENV_A_
	`))

	defer os.Unsetenv("_TEST_ENV_A_")
	defer os.Unsetenv("_TEST_ENV_B_")
	defer os.Unsetenv("_TEST_ENV_C_")

	os.Setenv("_TEST_ENV_A_", "$_TEST_ENV_B_")
	os.Setenv("_TEST_ENV_B_", "$_TEST_ENV_C_")
	os.Setenv("_TEST_ENV_C_", "$_TEST_ENV_A_")

	_, err := ProcessConfigFile(confFileName)
	if err == nil {
		t.Fatalf("Expected an error")
	}
	require_Contains(t, err.Error(), "variable reference cycle")
}

// Test can't include from environment variable
func TestEnvVarInclude(t *testing.T) {
	confFileName := createConfFile(t, []byte(`
		TEST: $_TEST_ENV_A_
	`))

	defer os.Unsetenv("_TEST_ENV_A_")

	os.Setenv("_TEST_ENV_A_", "include x")

	_, err := ProcessConfigFile(confFileName)
	if err == nil {
		t.Fatal("Expected an error")
	}
	require_Contains(t, err.Error(), "Expected a top-level value to end with a new line, comment or EOF")
}

func TestEnvVarFromIncludedFile(t *testing.T) {
	includeFileName := createConfFile(t, []byte(`
		TEST_PORT: $_TEST_ENV_PORT_A_
	`))

	confFileContent := fmt.Sprintf(`
		include "./%s"
		port: $TEST_PORT
	`, filepath.Base(includeFileName))

	dir := filepath.Dir(includeFileName)
	conf, err := os.CreateTemp(dir, "conf-")
	require_NoError(t, err)
	if err := os.WriteFile(conf.Name(), []byte(confFileContent), 0666); err != nil {
		t.Fatalf("Error writing conf file: %v", err)
	}

	defer os.Unsetenv("_TEST_ENV_PORT_A_")
	defer os.Unsetenv("_TEST_ENV_PORT_B_")

	os.Setenv("_TEST_ENV_PORT_A_", "$_TEST_ENV_PORT_B_")
	os.Setenv("_TEST_ENV_PORT_B_", "7890")

	opts, err := ProcessConfigFile(conf.Name())
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if opts.Port != 7890 {
		t.Fatalf("Expected port 7890, found %d", opts.Port)
	}
}

func TestRedactArgs(t *testing.T) {
	var tests []struct {
		name     string
		input    []string
		expected []string
	}

	for _, dash := range []string{"-", "--"} {
		for _, tag := range []string{"user", "pass", "auth"} {
			for _, value := range []string{"hello", "hello world"} {
				flag := dash + tag
				tests = append(tests, struct {
					name     string
					input    []string
					expected []string
				}{
					name:     fmt.Sprintf("%s%s-space-%q", dash, tag, value),
					input:    []string{flag, value},
					expected: []string{flag, "[REDACTED]"},
				})
				tests = append(tests, struct {
					name     string
					input    []string
					expected []string
				}{
					name:     fmt.Sprintf("%s%s-equals-%q", dash, tag, value),
					input:    []string{flag + "=" + value},
					expected: []string{flag + "=[REDACTED]"},
				})
			}
		}
	}

	for _, dash := range []string{"-", "--"} {
		for _, test := range []struct {
			name     string
			tag      string
			input    string
			expected string
		}{
			{
				name:     "routes-single",
				tag:      "routes",
				input:    "nats://ruser:s3cret@127.0.0.1:6222",
				expected: "nats://[REDACTED]@127.0.0.1:6222",
			},
			{
				name:     "routes-no-creds",
				tag:      "routes",
				input:    "nats://127.0.0.1:6222",
				expected: "nats://127.0.0.1:6222",
			},
			{
				name:     "routes-multi",
				tag:      "routes",
				input:    "nats://ruser:s3cret@127.0.0.1:6222, nats://ruser2:s3cret2@127.0.0.1:6223",
				expected: "nats://[REDACTED]@127.0.0.1:6222,nats://[REDACTED]@127.0.0.1:6223",
			},
			{
				name:     "cluster",
				tag:      "cluster",
				input:    "nats://cuser:s3cret@127.0.0.1:6224",
				expected: "nats://[REDACTED]@127.0.0.1:6224",
			},
			{
				name:     "cluster-no-creds",
				tag:      "cluster",
				input:    "nats://127.0.0.1:6224",
				expected: "nats://127.0.0.1:6224",
			},
			{
				name:     "cluster-comma-user",
				tag:      "cluster",
				input:    "nats://cuser,extra:s3cret@127.0.0.1:6224",
				expected: "nats://[REDACTED]@127.0.0.1:6224",
			},
			{
				name:     "cluster-random-port",
				tag:      "cluster",
				input:    "nats://cuser:s3cret@127.0.0.1:-1",
				expected: "nats://[REDACTED]@127.0.0.1:-1",
			},
			{
				name:     "cluster-listen",
				tag:      "cluster_listen",
				input:    "nats://luser:s3cret@127.0.0.1:6225",
				expected: "nats://[REDACTED]@127.0.0.1:6225",
			},
			{
				name:     "cluster-listen-comma-user",
				tag:      "cluster_listen",
				input:    "nats://luser,extra:s3cret@127.0.0.1:6225",
				expected: "nats://[REDACTED]@127.0.0.1:6225",
			},
			{
				name:     "cluster-listen-random-port",
				tag:      "cluster_listen",
				input:    "nats://luser:s3cret@127.0.0.1:-1",
				expected: "nats://[REDACTED]@127.0.0.1:-1",
			},
		} {
			tests = append(tests, struct {
				name     string
				input    []string
				expected []string
			}{
				name:     fmt.Sprintf("%s%s-space", dash, test.name),
				input:    []string{dash + test.tag, test.input},
				expected: []string{dash + test.tag, test.expected},
			})
			tests = append(tests, struct {
				name     string
				input    []string
				expected []string
			}{
				name:     fmt.Sprintf("%s%s-equals", dash, test.name),
				input:    []string{dash + test.tag + "=" + test.input},
				expected: []string{dash + test.tag + "=" + test.expected},
			})
		}
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			input := append([]string(nil), test.input...)
			RedactArgs(input)
			if !reflect.DeepEqual(input, test.expected) {
				t.Fatalf("A %v\nB %v", test.expected, input)
			}
		})
	}
}

func TestOptionsCompressionEqual(t *testing.T) {
	for _, test := range []struct {
		name    string
		genOpts func() (*CompressionOpts, *CompressionOpts)
		equal   bool
	}{
		{"same pointer", func() (*CompressionOpts, *CompressionOpts) {
			c := &CompressionOpts{}
			return c, c
		}, true},
		{"first nil", func() (*CompressionOpts, *CompressionOpts) {
			return &CompressionOpts{}, nil
		}, false},
		{"second nil", func() (*CompressionOpts, *CompressionOpts) {
			return nil, &CompressionOpts{}
		}, false},
		{"both nil", func() (*CompressionOpts, *CompressionOpts) {
			return nil, nil
		}, true},
		{"different mode", func() (*CompressionOpts, *CompressionOpts) {
			return &CompressionOpts{Mode: CompressionS2Fast},
				&CompressionOpts{Mode: CompressionS2Best}
		}, false},
		{"same mode", func() (*CompressionOpts, *CompressionOpts) {
			return &CompressionOpts{Mode: CompressionS2Best},
				&CompressionOpts{Mode: CompressionS2Best}
		}, true},
		{"s2 auto c1 default rtt thresholds", func() (*CompressionOpts, *CompressionOpts) {
			return &CompressionOpts{
				Mode:          CompressionS2Auto,
				RTTThresholds: defaultCompressionS2AutoRTTThresholds,
			}, &CompressionOpts{
				Mode: CompressionS2Auto,
			}
		}, true},
		{"s2 auto c2 default rtt thresholds", func() (*CompressionOpts, *CompressionOpts) {
			return &CompressionOpts{
				Mode: CompressionS2Auto,
			}, &CompressionOpts{
				Mode:          CompressionS2Auto,
				RTTThresholds: defaultCompressionS2AutoRTTThresholds,
			}
		}, true},
		{"s2 auto same rtt thresholds", func() (*CompressionOpts, *CompressionOpts) {
			return &CompressionOpts{
				Mode:          CompressionS2Auto,
				RTTThresholds: []time.Duration{5 * time.Millisecond, 10 * time.Millisecond},
			}, &CompressionOpts{
				Mode:          CompressionS2Auto,
				RTTThresholds: []time.Duration{5 * time.Millisecond, 10 * time.Millisecond},
			}
		}, true},
		{"s2 auto different rtt thresholds", func() (*CompressionOpts, *CompressionOpts) {
			return &CompressionOpts{
				Mode:          CompressionS2Auto,
				RTTThresholds: []time.Duration{5 * time.Millisecond, 10 * time.Millisecond},
			}, &CompressionOpts{
				Mode:          CompressionS2Auto,
				RTTThresholds: []time.Duration{15 * time.Millisecond, 30 * time.Millisecond},
			}
		}, false},
		{"s2 auto different rtt thresholds c1 not set", func() (*CompressionOpts, *CompressionOpts) {
			return &CompressionOpts{
				Mode: CompressionS2Auto,
			}, &CompressionOpts{
				Mode:          CompressionS2Auto,
				RTTThresholds: []time.Duration{15 * time.Millisecond, 30 * time.Millisecond},
			}
		}, false},
		{"s2 auto different rtt thresholds c2 not set", func() (*CompressionOpts, *CompressionOpts) {
			return &CompressionOpts{
				Mode:          CompressionS2Auto,
				RTTThresholds: []time.Duration{15 * time.Millisecond, 30 * time.Millisecond},
			}, &CompressionOpts{
				Mode: CompressionS2Auto,
			}
		}, false},
		{"s2 auto both rtt thresholds empty", func() (*CompressionOpts, *CompressionOpts) {
			return &CompressionOpts{
				Mode:          CompressionS2Auto,
				RTTThresholds: []time.Duration{},
			}, &CompressionOpts{
				Mode:          CompressionS2Auto,
				RTTThresholds: []time.Duration{},
			}
		}, true},
		{"s2 auto both rtt thresholds nil", func() (*CompressionOpts, *CompressionOpts) {
			return &CompressionOpts{
				Mode: CompressionS2Auto,
			}, &CompressionOpts{
				Mode: CompressionS2Auto,
			}
		}, true},
	} {
		t.Run(test.name, func(t *testing.T) {
			c1, c2 := test.genOpts()
			res := c1.equals(c2)
			require_Equal(t, test.equal, res)
		})
	}
}

func TestOptionsRemoteLeafNodeName(t *testing.T) {
	u1, err := url.Parse("nats://user1:secretpwd@127.0.0.1:7222")
	require_NoError(t, err)
	u2, err := url.Parse("nats://user2:secretpwd@127.0.0.1:7222")
	require_NoError(t, err)
	// With unredacted versions of the URLs
	urls := []*url.URL{u1, u2}
	safeURLs := redactURLList(urls)
	// Some Nkey
	nkey := "SUACJN3OSKWWPQXME4JUNFJ3PARXPO657GGNWNU7PK7G3AUQQYHLW26XH4"
	for _, test := range []struct {
		name       string
		input      *RemoteLeafOpts
		output     string
		safeOutput string
	}{
		{
			"url only", &RemoteLeafOpts{
				URLs: []*url.URL{u1, u2},
			},
			fmt.Sprintf("urls=%q, account=%q", urls, globalAccountName),
			fmt.Sprintf("urls=%q, account=%q", safeURLs, globalAccountName),
		},
		{
			"url with account", &RemoteLeafOpts{
				URLs:         []*url.URL{u1, u2},
				LocalAccount: "A",
			},
			fmt.Sprintf("urls=%q, account=%q", urls, "A"),
			fmt.Sprintf("urls=%q, account=%q", safeURLs, "A"),
		},
		{
			"url with credentials", &RemoteLeafOpts{
				URLs:        []*url.URL{u1, u2},
				Credentials: "credsfile",
			},
			fmt.Sprintf("urls=%q, account=%q, credentials=%q", urls, globalAccountName, "credsfile"),
			fmt.Sprintf("urls=%q, account=%q, credentials=%q", safeURLs, globalAccountName, "credsfile"),
		},
		{
			"url with nkey", &RemoteLeafOpts{
				URLs: []*url.URL{u1, u2},
				Nkey: nkey,
			},
			fmt.Sprintf("urls=%q, account=%q, nkey=%q", urls, globalAccountName, nkey),
			fmt.Sprintf("urls=%q, account=%q, nkey=%q", safeURLs, globalAccountName, "[REDACTED]"),
		},
		{
			"url with account and credentials", &RemoteLeafOpts{
				URLs:         []*url.URL{u1, u2},
				LocalAccount: "A",
				Credentials:  "credsfile",
			},
			fmt.Sprintf("urls=%q, account=%q, credentials=%q", urls, "A", "credsfile"),
			fmt.Sprintf("urls=%q, account=%q, credentials=%q", safeURLs, "A", "credsfile"),
		},
		{
			"url with account and nkey", &RemoteLeafOpts{
				URLs:         []*url.URL{u1, u2},
				LocalAccount: "A",
				Nkey:         nkey,
			},
			fmt.Sprintf("urls=%q, account=%q, nkey=%q", urls, "A", nkey),
			fmt.Sprintf("urls=%q, account=%q, nkey=%q", safeURLs, "A", "[REDACTED]"),
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			name := test.input.name()
			require_Equal(t, name, test.output)
			name = test.input.safeName()
			require_Equal(t, name, test.safeOutput)
		})
	}

	// Because we use `%q` when building the name, those two will have different
	// names:
	// r1=urls=["nats://user1:secretpwd@127.0.0.1:7222"], account="A", credentials="creds"
	// r2=urls=["nats://user1:secretpwd@127.0.0.1:7222"], account="A\", credentials=\"creds"
	r1 := &RemoteLeafOpts{URLs: []*url.URL{u1}, LocalAccount: "A", Credentials: "creds"}
	r2 := &RemoteLeafOpts{URLs: []*url.URL{u1}, LocalAccount: `A", credentials="creds`}
	require_False(t, r1.name() == r2.name())
}
