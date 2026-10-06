// Copyright 2017-2026 The NATS Authors
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
	"encoding/base64"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"maps"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"reflect"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/nats-io/jwt/v2"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nkeys"
)

func newServerWithConfig(t *testing.T, configFile string) (*Server, *Options, string) {
	t.Helper()
	content, err := os.ReadFile(configFile)
	if err != nil {
		t.Fatalf("Error loading file: %v", err)
	}
	return newServerWithContent(t, content)
}

func newServerWithContent(t *testing.T, content []byte) (*Server, *Options, string) {
	t.Helper()
	opts, tmpFile := newOptionsFromContent(t, content)
	return New(opts), opts, tmpFile
}

func newOptionsFromContent(t *testing.T, content []byte) (*Options, string) {
	t.Helper()
	tmpFile := createConfFile(t, content)
	opts, err := ProcessConfigFile(tmpFile)
	if err != nil {
		t.Fatalf("Error processing config file: %v", err)
	}
	opts.NoSigs = true
	return opts, tmpFile
}

func createConfFile(t testing.TB, content []byte) string {
	t.Helper()
	conf := createTempFile(t, _EMPTY_)
	fName := conf.Name()
	conf.Close()
	if err := os.WriteFile(fName, content, 0666); err != nil {
		t.Fatalf("Error writing conf file: %v", err)
	}
	return fName
}

func runReloadServerWithConfig(t *testing.T, configFile string) (*Server, *Options, string) {
	t.Helper()
	content, err := os.ReadFile(configFile)
	if err != nil {
		t.Fatalf("Error loading file: %v", err)
	}
	return runReloadServerWithContent(t, content)
}

func runReloadServerWithContent(t *testing.T, content []byte) (*Server, *Options, string) {
	t.Helper()
	opts, tmpFile := newOptionsFromContent(t, content)
	opts.NoLog = true
	opts.NoSigs = true
	s := RunServer(opts)
	return s, opts, tmpFile
}

func changeCurrentConfigContent(t *testing.T, curConfig, newConfig string) {
	t.Helper()
	content, err := os.ReadFile(newConfig)
	if err != nil {
		t.Fatalf("Error loading file: %v", err)
	}
	changeCurrentConfigContentWithNewContent(t, curConfig, content)
}

func changeCurrentConfigContentWithNewContent(t *testing.T, curConfig string, content []byte) {
	t.Helper()
	if err := os.WriteFile(curConfig, content, 0666); err != nil {
		t.Fatalf("Error writing config: %v", err)
	}
}

// Ensure Reload returns an error when attempting to reload a server that did
// not start with a config file.
func TestConfigReloadNoConfigFile(t *testing.T) {
	server := New(&Options{NoSigs: true})
	loaded := server.ConfigTime()
	if server.Reload() == nil {
		t.Fatal("Expected Reload to return an error")
	}
	if reloaded := server.ConfigTime(); reloaded != loaded {
		t.Fatalf("ConfigTime is incorrect.\nexpected: %s\ngot: %s", loaded, reloaded)
	}
}

// Ensure Reload returns an error when attempting to change an option which
// does not support reloading.
func TestConfigReloadUnsupported(t *testing.T) {
	server, _, config := newServerWithConfig(t, "./configs/reload/test.conf")
	defer server.Shutdown()

	loaded := server.ConfigTime()

	golden := &Options{
		ConfigFile:     config,
		Host:           "0.0.0.0",
		Port:           2233,
		AuthTimeout:    float64(AUTH_TIMEOUT / time.Second),
		Debug:          false,
		Trace:          false,
		Logtime:        false,
		MaxControlLine: 4096,
		MaxPayload:     1048576,
		MaxConn:        65536,
		PingInterval:   2 * time.Minute,
		MaxPingsOut:    2,
		WriteDeadline:  10 * time.Second,
		Cluster: ClusterOpts{
			Name: "abc",
			Host: "127.0.0.1",
			Port: -1,
		},
		NoSigs:       true,
		configDigest: "sha256:3c5c4141f56274bcfa801f2d7326ec64d5eabddd9e6d4f9a06ed167315a57f55",
	}
	setBaselineOptions(golden)

	checkOptionsEqual(t, golden, server.getOpts())

	// Change config file to bad config.
	changeCurrentConfigContent(t, config, "./configs/reload/reload_unsupported.conf")

	// This should fail because `cluster` host cannot be changed.
	if err := server.Reload(); err == nil {
		t.Fatal("Expected Reload to return an error")
	}

	// Ensure config didn't change.
	checkOptionsEqual(t, golden, server.getOpts())

	if reloaded := server.ConfigTime(); reloaded != loaded {
		t.Fatalf("ConfigTime is incorrect.\nexpected: %s\ngot: %s", loaded, reloaded)
	}
}

// This checks that if we change an option that does not support hot-swapping
// we get an error. Using `listen` for now (test may need to be updated if
// server is changed to support change of listen spec).
func TestConfigReloadUnsupportedHotSwapping(t *testing.T) {
	server, _, config := newServerWithContent(t, []byte("listen: 127.0.0.1:-1"))
	defer server.Shutdown()

	loaded := server.ConfigTime()

	time.Sleep(time.Millisecond)

	// Change config file with unsupported option hot-swap
	changeCurrentConfigContentWithNewContent(t, config, []byte("listen: 127.0.0.1:9999"))

	// This should fail because `listen` host cannot be changed.
	if err := server.Reload(); err == nil || !strings.Contains(err.Error(), "not supported") {
		t.Fatalf("Expected Reload to return a not supported error, got %v", err)
	}

	if reloaded := server.ConfigTime(); reloaded != loaded {
		t.Fatalf("ConfigTime is incorrect.\nexpected: %s\ngot: %s", loaded, reloaded)
	}
}

// Ensure Reload returns an error when reloading from a bad config file.
func TestConfigReloadInvalidConfig(t *testing.T) {
	server, _, config := newServerWithConfig(t, "./configs/reload/test.conf")
	defer server.Shutdown()

	loaded := server.ConfigTime()

	golden := &Options{
		ConfigFile:     config,
		Host:           "0.0.0.0",
		Port:           2233,
		AuthTimeout:    float64(AUTH_TIMEOUT / time.Second),
		Debug:          false,
		Trace:          false,
		Logtime:        false,
		MaxControlLine: 4096,
		MaxPayload:     1048576,
		MaxConn:        65536,
		PingInterval:   2 * time.Minute,
		MaxPingsOut:    2,
		WriteDeadline:  10 * time.Second,
		Cluster: ClusterOpts{
			Name: "abc",
			Host: "127.0.0.1",
			Port: -1,
		},
		NoSigs:       true,
		configDigest: "sha256:3c5c4141f56274bcfa801f2d7326ec64d5eabddd9e6d4f9a06ed167315a57f55",
	}
	setBaselineOptions(golden)

	checkOptionsEqual(t, golden, server.getOpts())

	// Change config file to bad config.
	changeCurrentConfigContent(t, config, "./configs/reload/invalid.conf")

	// This should fail because the new config should not parse.
	if err := server.Reload(); err == nil {
		t.Fatal("Expected Reload to return an error")
	}

	// Ensure config didn't change.
	checkOptionsEqual(t, golden, server.getOpts())

	if reloaded := server.ConfigTime(); reloaded != loaded {
		t.Fatalf("ConfigTime is incorrect.\nexpected: %s\ngot: %s", loaded, reloaded)
	}
}

// Ensure Reload returns nil and the config is changed on success.
func TestConfigReload(t *testing.T) {
	server, opts, config := runReloadServerWithConfig(t, "./configs/reload/test.conf")
	defer removeFile(t, "nats-server.pid")
	defer removeFile(t, "nats-server.log")
	defer server.Shutdown()

	var content []byte
	if runtime.GOOS != "windows" {
		content = []byte(`
			remote_syslog: "udp://127.0.0.1:514" # change on reload
			syslog:        true # enable on reload
		`)
	}
	platformConf := filepath.Join(filepath.Dir(config), "platform.conf")
	if err := os.WriteFile(platformConf, content, 0666); err != nil {
		t.Fatalf("Unable to write config file: %v", err)
	}

	loaded := server.ConfigTime()

	golden := &Options{
		ConfigFile:     config,
		Host:           "0.0.0.0",
		Port:           2233,
		AuthTimeout:    float64(AUTH_TIMEOUT / time.Second),
		Debug:          false,
		Trace:          false,
		NoLog:          true,
		Logtime:        false,
		MaxControlLine: 4096,
		MaxPayload:     1048576,
		MaxConn:        65536,
		PingInterval:   2 * time.Minute,
		MaxPingsOut:    2,
		WriteDeadline:  10 * time.Second,
		Cluster: ClusterOpts{
			Name: "abc",
			Host: "127.0.0.1",
			Port: server.ClusterAddr().Port,
		},
		NoSigs:       true,
		configDigest: "sha256:3c5c4141f56274bcfa801f2d7326ec64d5eabddd9e6d4f9a06ed167315a57f55",
	}
	setBaselineOptions(golden)

	checkOptionsEqual(t, golden, opts)

	// Change config file to new config.
	changeCurrentConfigContent(t, config, "./configs/reload/reload.conf")

	if err := server.Reload(); err != nil {
		t.Fatalf("Error reloading config: %v", err)
	}

	// Ensure config changed.
	updated := server.getOpts()
	if !updated.Trace {
		t.Fatal("Expected Trace to be true")
	}
	if !updated.Debug {
		t.Fatal("Expected Debug to be true")
	}
	if !updated.Logtime {
		t.Fatal("Expected Logtime to be true")
	}
	if !updated.LogtimeUTC {
		t.Fatal("Expected LogtimeUTC to be true")
	}
	if runtime.GOOS != "windows" {
		if !updated.Syslog {
			t.Fatal("Expected Syslog to be true")
		}
		if updated.RemoteSyslog != "udp://127.0.0.1:514" {
			t.Fatalf("RemoteSyslog is incorrect.\nexpected: udp://127.0.0.1:514\ngot: %s", updated.RemoteSyslog)
		}
	}
	if updated.LogFile != "nats-server.log" {
		t.Fatalf("LogFile is incorrect.\nexpected: nats-server.log\ngot: %s", updated.LogFile)
	}
	if updated.TLSConfig == nil {
		t.Fatal("Expected TLSConfig to be non-nil")
	}
	if !server.info.TLSRequired {
		t.Fatal("Expected TLSRequired to be true")
	}
	if !server.info.TLSVerify {
		t.Fatal("Expected TLSVerify to be true")
	}
	if updated.Username != "tyler" {
		t.Fatalf("Username is incorrect.\nexpected: tyler\ngot: %s", updated.Username)
	}
	if updated.Password != "T0pS3cr3t" {
		t.Fatalf("Password is incorrect.\nexpected: T0pS3cr3t\ngot: %s", updated.Password)
	}
	if updated.AuthTimeout != 2 {
		t.Fatalf("AuthTimeout is incorrect.\nexpected: 2\ngot: %f", updated.AuthTimeout)
	}
	if !server.info.AuthRequired {
		t.Fatal("Expected AuthRequired to be true")
	}
	if !updated.Cluster.NoAdvertise {
		t.Fatal("Expected NoAdvertise to be true")
	}
	if updated.Cluster.PingInterval != 20*time.Second {
		t.Fatalf("Cluster PingInterval is incorrect.\nexpected: 20s\ngot: %v", updated.Cluster.PingInterval)
	}
	if updated.Cluster.MaxPingsOut != 8 {
		t.Fatalf("Cluster MaxPingsOut is incorrect.\nexpected: 6\ngot: %v", updated.Cluster.MaxPingsOut)
	}
	if updated.PidFile != "nats-server.pid" {
		t.Fatalf("PidFile is incorrect.\nexpected: nats-server.pid\ngot: %s", updated.PidFile)
	}
	if updated.MaxControlLine != 512 {
		t.Fatalf("MaxControlLine is incorrect.\nexpected: 512\ngot: %d", updated.MaxControlLine)
	}
	if updated.PingInterval != 5*time.Second {
		t.Fatalf("PingInterval is incorrect.\nexpected 5s\ngot: %s", updated.PingInterval)
	}
	if updated.MaxPingsOut != 1 {
		t.Fatalf("MaxPingsOut is incorrect.\nexpected 1\ngot: %d", updated.MaxPingsOut)
	}
	if updated.WriteDeadline != 3*time.Second {
		t.Fatalf("WriteDeadline is incorrect.\nexpected 3s\ngot: %s", updated.WriteDeadline)
	}
	if updated.MaxPayload != 1024 {
		t.Fatalf("MaxPayload is incorrect.\nexpected 1024\ngot: %d", updated.MaxPayload)
	}
	expectedMetadata := map[string]string{"key1": "value1", "key2": "value2"}
	if !reflect.DeepEqual(expectedMetadata, updated.Metadata) {
		t.Fatalf("Metadata is incorrect.\nexpected: %v\ngot: %v", expectedMetadata, updated.Metadata)
	}

	if reloaded := server.ConfigTime(); !reloaded.After(loaded) {
		t.Fatalf("ConfigTime is incorrect.\nexpected greater than: %s\ngot: %s", loaded, reloaded)
	}
}

// Ensure Reload supports TLS config changes. Test this by starting a server
// with TLS enabled, connect to it to verify, reload config using a different
// key pair and client verification enabled, ensure reconnect fails, then
// ensure reconnect succeeds when the client provides a cert.
func TestConfigReloadRotateTLS(t *testing.T) {
	server, opts, config := runReloadServerWithConfig(t, "./configs/reload/tls_test.conf")
	defer server.Shutdown()

	// Ensure we can connect as a sanity check.
	addr := fmt.Sprintf("nats://%s:%d", opts.Host, server.Addr().(*net.TCPAddr).Port)

	nc, err := nats.Connect(addr, nats.Secure(&tls.Config{InsecureSkipVerify: true}))
	if err != nil {
		t.Fatalf("Error creating client: %v", err)
	}
	defer nc.Close()
	sub, err := nc.SubscribeSync("foo")
	if err != nil {
		t.Fatalf("Error subscribing: %v", err)
	}
	defer sub.Unsubscribe()

	// Rotate cert and enable client verification.
	changeCurrentConfigContent(t, config, "./configs/reload/tls_verify_test.conf")
	if err := server.Reload(); err != nil {
		t.Fatalf("Error reloading config: %v", err)
	}

	// Ensure connecting fails.
	if _, err := nats.Connect(addr, nats.Secure(&tls.Config{InsecureSkipVerify: true})); err == nil {
		t.Fatal("Expected connect to fail")
	}

	// Ensure connecting succeeds when client presents cert.
	cert := nats.ClientCert("./configs/certs/cert.new.pem", "./configs/certs/key.new.pem")
	conn, err := nats.Connect(addr, cert, nats.RootCAs("./configs/certs/cert.new.pem"))
	if err != nil {
		t.Fatalf("Error creating client: %v", err)
	}
	conn.Close()

	// Ensure the original connection can still publish/receive.
	if err := nc.Publish("foo", []byte("hello")); err != nil {
		t.Fatalf("Error publishing: %v", err)
	}
	nc.Flush()
	msg, err := sub.NextMsg(2 * time.Second)
	if err != nil {
		t.Fatalf("Error receiving msg: %v", err)
	}
	if string(msg.Data) != "hello" {
		t.Fatalf("Msg is incorrect.\nexpected: %+v\ngot: %+v", []byte("hello"), msg.Data)
	}
}

func TestConfigReloadTLSPinnedCertsDisconnectsClient(t *testing.T) {
	const config = `
		host: localhost
		port: -1
		tls {
			ca_file: "../test/configs/certs/ca.pem"
			cert_file: "../test/configs/certs/server-cert.pem"
			key_file: "../test/configs/certs/server-key.pem"
			verify: true
			pinned_certs: ["%s"]
		}
	`
	const (
		clientCertPin = "bf6f821f09fde09451411ba3b42c0f74727d61a974c69fd3cf5257f39c75f0e9"
		otherCertPin  = "aaaaaaaa09fde09451411ba3b42c0f74727d61a974c69fd3cf5257f39c75f0e9"
	)

	conf := createConfFile(t, fmt.Appendf(nil, config, clientCertPin))
	srv, _ := RunServerWithConfig(conf)
	defer srv.Shutdown()

	nc, err := nats.Connect(srv.ClientURL(),
		nats.RootCAs("../test/configs/certs/ca.pem"),
		nats.ClientCert("../test/configs/certs/client-cert.pem", "../test/configs/certs/client-key.pem"),
		nats.NoReconnect(),
	)
	require_NoError(t, err)
	defer nc.Close()

	require_NoError(t, os.WriteFile(conf, fmt.Appendf(nil, config, otherCertPin), 0660))
	require_NoError(t, srv.Reload())

	checkFor(t, 2*time.Second, 10*time.Millisecond, func() error {
		if !nc.IsClosed() {
			return fmt.Errorf("client with removed certificate pin is still connected")
		}
		return nil
	})
}

// Ensure Reload supports enabling TLS. Test this by starting a server without
// TLS enabled, connect to it to verify, reload config with TLS enabled, ensure
// reconnect fails, then ensure reconnect succeeds when using secure.
func TestConfigReloadEnableTLS(t *testing.T) {
	server, opts, config := runReloadServerWithConfig(t, "./configs/reload/basic.conf")
	defer server.Shutdown()

	// Ensure we can connect as a sanity check.
	addr := fmt.Sprintf("nats://%s:%d", opts.Host, server.Addr().(*net.TCPAddr).Port)
	nc, err := nats.Connect(addr)
	if err != nil {
		t.Fatalf("Error creating client: %v", err)
	}
	nc.Close()

	// Enable TLS.
	changeCurrentConfigContent(t, config, "./configs/reload/tls_test.conf")
	if err := server.Reload(); err != nil {
		t.Fatalf("Error reloading config: %v", err)
	}

	// Ensure connecting is OK (we need to skip server cert verification since
	// the library is not doing that by default now).
	nc, err = nats.Connect(addr, nats.Secure(&tls.Config{InsecureSkipVerify: true}))
	if err != nil {
		t.Fatalf("Error creating client: %v", err)
	}
	nc.Close()
}

// Ensure Reload supports disabling TLS. Test this by starting a server with
// TLS enabled, connect to it to verify, reload config with TLS disabled,
// ensure reconnect fails, then ensure reconnect succeeds when connecting
// without secure.
func TestConfigReloadDisableTLS(t *testing.T) {
	server, opts, config := runReloadServerWithConfig(t, "./configs/reload/tls_test.conf")
	defer server.Shutdown()

	// Ensure we can connect as a sanity check.
	addr := fmt.Sprintf("nats://%s:%d", opts.Host, server.Addr().(*net.TCPAddr).Port)
	nc, err := nats.Connect(addr, nats.Secure(&tls.Config{InsecureSkipVerify: true}))
	if err != nil {
		t.Fatalf("Error creating client: %v", err)
	}
	nc.Close()

	// Disable TLS.
	changeCurrentConfigContent(t, config, "./configs/reload/basic.conf")
	if err := server.Reload(); err != nil {
		t.Fatalf("Error reloading config: %v", err)
	}

	// Ensure connecting fails.
	if _, err := nats.Connect(addr, nats.Secure(&tls.Config{InsecureSkipVerify: true})); err == nil {
		t.Fatal("Expected connect to fail")
	}

	// Ensure connecting succeeds when not using secure.
	nc, err = nats.Connect(addr)
	if err != nil {
		t.Fatalf("Error creating client: %v", err)
	}
	nc.Close()
}

func TestConfigReloadRotateTLSMultiCert(t *testing.T) {
	server, opts, config := runReloadServerWithConfig(t, "./configs/reload/tls_multi_cert_1.conf")
	defer server.Shutdown()

	// Ensure we can connect as a sanity check.
	addr := fmt.Sprintf("nats://%s:%d", opts.Host, server.Addr().(*net.TCPAddr).Port)

	rawCerts := make(chan []byte, 3)
	nc, err := nats.Connect(addr, nats.Secure(&tls.Config{
		VerifyConnection: func(s tls.ConnectionState) error {
			rawCerts <- s.PeerCertificates[0].Raw
			return nil
		},
		InsecureSkipVerify: true,
	}))
	if err != nil {
		t.Fatalf("Error creating client: %v", err)
	}
	defer nc.Close()
	sub, err := nc.SubscribeSync("foo")
	if err != nil {
		t.Fatalf("Error subscribing: %v", err)
	}
	defer sub.Unsubscribe()

	// Rotate cert and enable client verification.
	changeCurrentConfigContent(t, config, "./configs/reload/tls_multi_cert_2.conf")
	if err := server.Reload(); err != nil {
		t.Fatalf("Error reloading config: %v", err)
	}

	// Ensure connecting fails.
	if _, err := nats.Connect(addr, nats.Secure(&tls.Config{InsecureSkipVerify: true})); err == nil {
		t.Fatal("Expected connect to fail")
	}

	// Ensure connecting succeeds when client presents cert.
	cert := nats.ClientCert("../test/configs/certs/client-cert.pem", "../test/configs/certs/client-key.pem")
	conn, err := nats.Connect(addr, cert, nats.RootCAs("../test/configs/certs/ca.pem"), nats.Secure(&tls.Config{
		VerifyConnection: func(s tls.ConnectionState) error {
			rawCerts <- s.PeerCertificates[0].Raw
			return nil
		},
	}))
	if err != nil {
		t.Fatalf("Error creating client: %v", err)
	}
	conn.Close()

	// Ensure the original connection can still publish/receive.
	if err := nc.Publish("foo", []byte("hello")); err != nil {
		t.Fatalf("Error publishing: %v", err)
	}
	nc.Flush()
	msg, err := sub.NextMsg(2 * time.Second)
	if err != nil {
		t.Fatalf("Error receiving msg: %v", err)
	}
	if string(msg.Data) != "hello" {
		t.Fatalf("Msg is incorrect.\nexpected: %+v\ngot: %+v", []byte("hello"), msg.Data)
	}

	// Rotate cert and disable client verification.
	changeCurrentConfigContent(t, config, "./configs/reload/tls_multi_cert_3.conf")
	if err := server.Reload(); err != nil {
		t.Fatalf("Error reloading config: %v", err)
	}

	nc, err = nats.Connect(addr, nats.Secure(&tls.Config{
		VerifyConnection: func(s tls.ConnectionState) error {
			rawCerts <- s.PeerCertificates[0].Raw
			return nil
		},
		InsecureSkipVerify: true,
	}))
	if err != nil {
		t.Fatalf("Error creating client: %v", err)
	}
	defer nc.Close()
	sub, err = nc.SubscribeSync("foo")
	if err != nil {
		t.Fatalf("Error subscribing: %v", err)
	}
	defer sub.Unsubscribe()

	certA := <-rawCerts
	certB := <-rawCerts
	certC := <-rawCerts
	if !bytes.Equal(certA, certB) {
		t.Error("Expected the same cert")
	}
	if bytes.Equal(certB, certC) {
		t.Error("Expected a different cert")
	}
}

func TestConfigReloadTrustedOperatorAndTrustedKeysUnsupported(t *testing.T) {
	kp, _ := nkeys.CreateAccount()
	aPub, _ := kp.PublicKey()
	claim := jwt.NewAccountClaims(aPub)
	aJwt, err := claim.Encode(oKp)
	require_NoError(t, err)
	tmpl := `
		listen: 127.0.0.1:-1
		%s
		resolver: MEM
		resolver_preload: {
			%s: %s
		}
	`
	conf := createConfFile(t, fmt.Appendf(nil, tmpl, fmt.Sprintf("operator: %s", ojwt), aPub, aJwt))
	s, o := RunServerWithConfig(conf)
	defer s.Shutdown()

	// The server fills the TrustedKeys with the operators' signing keys.
	require_True(t, len(o.TrustedKeys) > 0)

	keysConf := strings.Builder{}
	keysConf.WriteByte('[')
	for i, k := range o.TrustedKeys {
		if i > 0 {
			keysConf.WriteString(", ")
		}
		keysConf.WriteByte('"')
		keysConf.WriteString(k)
		keysConf.WriteByte('"')
	}
	keysConf.WriteByte(']')

	err = os.WriteFile(conf, fmt.Appendf(nil, tmpl, _EMPTY_, aPub, aJwt), 0666)
	require_NoError(t, err)
	err = s.Reload()
	require_Error(t, err)
	require_Contains(t, err.Error(), "not supported for TrustedOperator")

	// We used to wipe the TrustedKeys, we no longer do that.
	require_True(t, len(o.TrustedKeys) > 0)

	s.Shutdown()
	conf = createConfFile(t,
		fmt.Appendf(nil, tmpl, fmt.Sprintf("trusted_keys: %s", keysConf.String()), aPub, aJwt))
	s, o = RunServerWithConfig(conf)
	defer s.Shutdown()

	err = os.WriteFile(conf, fmt.Appendf(nil, tmpl, _EMPTY_, aPub, aJwt), 0666)
	require_NoError(t, err)
	err = s.Reload()
	require_Error(t, err)
	require_Contains(t, err.Error(), "not supported for TrustedKeys")

	require_True(t, len(o.TrustedKeys) > 0)
}

func TestConfigReloadDefaultSentinel(t *testing.T) {
	var err error
	preload := make(map[string]string)

	_, sysPub, sysAC := NewJwtAccountClaim("SYS")
	preload[sysPub], err = sysAC.Encode(oKp)
	require_NoError(t, err)

	aKP, aPub, aAC := NewJwtAccountClaim("A")
	preload[aPub], err = aAC.Encode(oKp)
	require_NoError(t, err)

	preloadConfig, err := json.MarshalIndent(preload, "", " ")
	require_NoError(t, err)

	uKP, err := nkeys.CreateUser()
	require_NoError(t, err)
	uPub, err := uKP.PublicKey()
	require_NoError(t, err)
	uc := jwt.NewUserClaims(uPub)
	uc.BearerToken = true
	uc.Name = "sentinel"
	sentinelToken, err := uc.Encode(aKP)
	require_NoError(t, err)
	content := func() []byte {
		return []byte(fmt.Sprintf(`
            listen: 127.0.0.1:4747
            operator: %s
            system_account: %s
            resolver: MEM
            resolver_preload: %s
			default_sentinel: %s
`, ojwt, sysPub, preloadConfig, sentinelToken))
	}

	server, opts, config := runReloadServerWithContent(t, content())
	defer server.Shutdown()
	require_Equal(t, opts.DefaultSentinel, sentinelToken)

	uc.Name = "sentinel-updated"
	sentinelToken, err = uc.Encode(aKP)
	require_NoError(t, err)
	changeCurrentConfigContentWithNewContent(t, config, content())
	if err := server.Reload(); err != nil {
		t.Fatalf("Error reloading config: %v", err)
	}
	opts = server.getOpts()
	require_Equal(t, opts.DefaultSentinel, sentinelToken)
}

// Ensure Reload supports single user authentication config changes. Test this
// by starting a server with authentication enabled, connect to it to verify,
// reload config using a different username/password, ensure reconnect fails,
// then ensure reconnect succeeds when using the correct credentials.
func TestConfigReloadRotateUserAuthentication(t *testing.T) {
	server, opts, config := runReloadServerWithConfig(t, "./configs/reload/single_user_authentication_1.conf")
	defer server.Shutdown()

	// Ensure we can connect as a sanity check.
	addr := fmt.Sprintf("nats://%s:%d", opts.Host, opts.Port)
	nc, err := nats.Connect(addr, nats.UserInfo("tyler", "T0pS3cr3t"))
	if err != nil {
		t.Fatalf("Error creating client: %v", err)
	}
	defer nc.Close()
	disconnected := make(chan struct{}, 1)
	asyncErr := make(chan error, 1)
	nc.SetErrorHandler(func(nc *nats.Conn, sub *nats.Subscription, err error) {
		asyncErr <- err
	})
	nc.SetDisconnectHandler(func(*nats.Conn) {
		disconnected <- struct{}{}
	})

	// Change user credentials.
	changeCurrentConfigContent(t, config, "./configs/reload/single_user_authentication_2.conf")
	if err := server.Reload(); err != nil {
		t.Fatalf("Error reloading config: %v", err)
	}

	// Ensure connecting fails.
	if _, err := nats.Connect(addr, nats.UserInfo("tyler", "T0pS3cr3t")); err == nil {
		t.Fatal("Expected connect to fail")
	}

	// Ensure connecting succeeds when using new credentials.
	conn, err := nats.Connect(addr, nats.UserInfo("derek", "passw0rd"))
	if err != nil {
		t.Fatalf("Error creating client: %v", err)
	}
	conn.Close()

	// Ensure the previous connection received an authorization error.
	// Note that it is possible that client gets EOF and not able to
	// process async error, so don't fail if we don't get it.
	select {
	case err := <-asyncErr:
		if err != nats.ErrAuthorization {
			t.Fatalf("Expected ErrAuthorization, got %v", err)
		}
	case <-time.After(time.Second):
		// Give it up to 1 sec.
	}

	// Ensure the previous connection was disconnected.
	select {
	case <-disconnected:
	case <-time.After(2 * time.Second):
		t.Fatal("Expected connection to be disconnected")
	}
}

// Ensure Reload supports enabling single user authentication. Test this by
// starting a server with authentication disabled, connect to it to verify,
// reload config using with a username/password, ensure reconnect fails, then
// ensure reconnect succeeds when using the correct credentials.
func TestConfigReloadEnableUserAuthentication(t *testing.T) {
	server, opts, config := runReloadServerWithConfig(t, "./configs/reload/basic.conf")
	defer server.Shutdown()

	// Ensure we can connect as a sanity check.
	addr := fmt.Sprintf("nats://%s:%d", opts.Host, opts.Port)
	nc, err := nats.Connect(addr)
	if err != nil {
		t.Fatalf("Error creating client: %v", err)
	}
	defer nc.Close()
	disconnected := make(chan struct{}, 1)
	asyncErr := make(chan error, 1)
	nc.SetErrorHandler(func(nc *nats.Conn, sub *nats.Subscription, err error) {
		asyncErr <- err
	})
	nc.SetDisconnectHandler(func(*nats.Conn) {
		disconnected <- struct{}{}
	})

	// Enable authentication.
	changeCurrentConfigContent(t, config, "./configs/reload/single_user_authentication_1.conf")
	if err := server.Reload(); err != nil {
		t.Fatalf("Error reloading config: %v", err)
	}

	// Ensure connecting fails.
	if _, err := nats.Connect(addr); err == nil {
		t.Fatal("Expected connect to fail")
	}

	// Ensure connecting succeeds when using new credentials.
	conn, err := nats.Connect(addr, nats.UserInfo("tyler", "T0pS3cr3t"))
	if err != nil {
		t.Fatalf("Error creating client: %v", err)
	}
	conn.Close()

	// Ensure the previous connection received an authorization error.
	// Note that it is possible that client gets EOF and not able to
	// process async error, so don't fail if we don't get it.
	select {
	case err := <-asyncErr:
		if err != nats.ErrAuthorization {
			t.Fatalf("Expected ErrAuthorization, got %v", err)
		}
	case <-time.After(time.Second):
	}

	// Ensure the previous connection was disconnected.
	select {
	case <-disconnected:
	case <-time.After(2 * time.Second):
		t.Fatal("Expected connection to be disconnected")
	}
}

// Ensure Reload supports disabling single user authentication. Test this by
// starting a server with authentication enabled, connect to it to verify,
// reload config using with authentication disabled, then ensure connecting
// with no credentials succeeds.
func TestConfigReloadDisableUserAuthentication(t *testing.T) {
	server, opts, config := runReloadServerWithConfig(t, "./configs/reload/single_user_authentication_1.conf")
	defer server.Shutdown()

	// Ensure we can connect as a sanity check.
	addr := fmt.Sprintf("nats://%s:%d", opts.Host, opts.Port)
	nc, err := nats.Connect(addr, nats.UserInfo("tyler", "T0pS3cr3t"))
	if err != nil {
		t.Fatalf("Error creating client: %v", err)
	}
	defer nc.Close()
	nc.SetErrorHandler(func(nc *nats.Conn, sub *nats.Subscription, err error) {
		t.Fatalf("Client received an unexpected error: %v", err)
	})

	// Disable authentication.
	changeCurrentConfigContent(t, config, "./configs/reload/basic.conf")
	if err := server.Reload(); err != nil {
		t.Fatalf("Error reloading config: %v", err)
	}

	// Ensure connecting succeeds with no credentials.
	conn, err := nats.Connect(addr)
	if err != nil {
		t.Fatalf("Error creating client: %v", err)
	}
	conn.Close()
}

// Ensure Reload supports token authentication config changes. Test this by
// starting a server with token authentication enabled, connect to it to
// verify, reload config using a different token, ensure reconnect fails, then
// ensure reconnect succeeds when using the correct token.
func TestConfigReloadRotateTokenAuthentication(t *testing.T) {
	server, opts, config := runReloadServerWithConfig(t, "./configs/reload/token_authentication_1.conf")
	defer server.Shutdown()

	disconnected := make(chan struct{})
	asyncErr := make(chan error)
	eh := func(nc *nats.Conn, sub *nats.Subscription, err error) { asyncErr <- err }
	dh := func(*nats.Conn) { disconnected <- struct{}{} }

	// Ensure we can connect as a sanity check.
	addr := fmt.Sprintf("nats://%s:%d", opts.Host, opts.Port)
	nc, err := nats.Connect(addr, nats.Token("T0pS3cr3t"), nats.ErrorHandler(eh), nats.DisconnectHandler(dh))
	if err != nil {
		t.Fatalf("Error creating client: %v", err)
	}
	defer nc.Close()

	// Change authentication token.
	changeCurrentConfigContent(t, config, "./configs/reload/token_authentication_2.conf")
	if err := server.Reload(); err != nil {
		t.Fatalf("Error reloading config: %v", err)
	}

	// Ensure connecting fails.
	if _, err := nats.Connect(addr, nats.Token("T0pS3cr3t")); err == nil {
		t.Fatal("Expected connect to fail")
	}

	// Ensure connecting succeeds when using new credentials.
	conn, err := nats.Connect(addr, nats.Token("passw0rd"))
	if err != nil {
		t.Fatalf("Error creating client: %v", err)
	}
	conn.Close()

	// Ensure the previous connection received an authorization error.
	select {
	case err := <-asyncErr:
		if err != nats.ErrAuthorization {
			t.Fatalf("Expected ErrAuthorization, got %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Expected authorization error")
	}

	// Ensure the previous connection was disconnected.
	select {
	case <-disconnected:
	case <-time.After(2 * time.Second):
		t.Fatal("Expected connection to be disconnected")
	}
}

// Ensure Reload supports enabling token authentication. Test this by starting
// a server with authentication disabled, connect to it to verify, reload
// config using with a token, ensure reconnect fails, then ensure reconnect
// succeeds when using the correct token.
func TestConfigReloadEnableTokenAuthentication(t *testing.T) {
	server, opts, config := runReloadServerWithConfig(t, "./configs/reload/basic.conf")
	defer server.Shutdown()

	// Ensure we can connect as a sanity check.
	addr := fmt.Sprintf("nats://%s:%d", opts.Host, opts.Port)
	nc, err := nats.Connect(addr)
	if err != nil {
		t.Fatalf("Error creating client: %v", err)
	}
	defer nc.Close()
	disconnected := make(chan struct{}, 1)
	asyncErr := make(chan error, 1)
	nc.SetErrorHandler(func(nc *nats.Conn, sub *nats.Subscription, err error) {
		asyncErr <- err
	})
	nc.SetDisconnectHandler(func(*nats.Conn) {
		disconnected <- struct{}{}
	})

	// Enable authentication.
	changeCurrentConfigContent(t, config, "./configs/reload/token_authentication_1.conf")
	if err := server.Reload(); err != nil {
		t.Fatalf("Error reloading config: %v", err)
	}

	// Ensure connecting fails.
	if _, err := nats.Connect(addr); err == nil {
		t.Fatal("Expected connect to fail")
	}

	// Ensure connecting succeeds when using new credentials.
	conn, err := nats.Connect(addr, nats.Token("T0pS3cr3t"))
	if err != nil {
		t.Fatalf("Error creating client: %v", err)
	}
	conn.Close()

	// Ensure the previous connection received an authorization error.
	// Note that it is possible that client gets EOF and not able to
	// process async error, so don't fail if we don't get it.
	select {
	case err := <-asyncErr:
		if err != nats.ErrAuthorization {
			t.Fatalf("Expected ErrAuthorization, got %v", err)
		}
	case <-time.After(time.Second):
	}

	// Ensure the previous connection was disconnected.
	select {
	case <-disconnected:
	case <-time.After(2 * time.Second):
		t.Fatal("Expected connection to be disconnected")
	}
}

// Ensure Reload supports disabling single token authentication. Test this by
// starting a server with authentication enabled, connect to it to verify,
// reload config using with authentication disabled, then ensure connecting
// with no token succeeds.
func TestConfigReloadDisableTokenAuthentication(t *testing.T) {
	server, opts, config := runReloadServerWithConfig(t, "./configs/reload/token_authentication_1.conf")
	defer server.Shutdown()

	// Ensure we can connect as a sanity check.
	addr := fmt.Sprintf("nats://%s:%d", opts.Host, opts.Port)
	nc, err := nats.Connect(addr, nats.Token("T0pS3cr3t"))
	if err != nil {
		t.Fatalf("Error creating client: %v", err)
	}
	defer nc.Close()
	nc.SetErrorHandler(func(nc *nats.Conn, sub *nats.Subscription, err error) {
		t.Fatalf("Client received an unexpected error: %v", err)
	})

	// Disable authentication.
	changeCurrentConfigContent(t, config, "./configs/reload/basic.conf")
	if err := server.Reload(); err != nil {
		t.Fatalf("Error reloading config: %v", err)
	}

	// Ensure connecting succeeds with no credentials.
	conn, err := nats.Connect(addr)
	if err != nil {
		t.Fatalf("Error creating client: %v", err)
	}
	conn.Close()
}

// Ensure Reload supports users authentication config changes. Test this by
// starting a server with users authentication enabled, connect to it to
// verify, reload config using a different user, ensure reconnect fails, then
// ensure reconnect succeeds when using the correct credentials.
func TestConfigReloadRotateUsersAuthentication(t *testing.T) {
	server, opts, config := runReloadServerWithConfig(t, "./configs/reload/multiple_users_1.conf")
	defer server.Shutdown()

	// Ensure we can connect as a sanity check.
	addr := fmt.Sprintf("nats://%s:%d", opts.Host, opts.Port)
	nc, err := nats.Connect(addr, nats.UserInfo("alice", "foo"))
	if err != nil {
		t.Fatalf("Error creating client: %v", err)
	}
	defer nc.Close()
	disconnected := make(chan struct{}, 1)
	asyncErr := make(chan error, 1)
	nc.SetErrorHandler(func(nc *nats.Conn, sub *nats.Subscription, err error) {
		asyncErr <- err
	})
	nc.SetDisconnectHandler(func(*nats.Conn) {
		disconnected <- struct{}{}
	})

	// These credentials won't change.
	nc2, err := nats.Connect(addr, nats.UserInfo("bob", "bar"))
	if err != nil {
		t.Fatalf("Error creating client: %v", err)
	}
	defer nc2.Close()
	sub, err := nc2.SubscribeSync("foo")
	if err != nil {
		t.Fatalf("Error subscribing: %v", err)
	}
	defer sub.Unsubscribe()

	// Change users credentials.
	changeCurrentConfigContent(t, config, "./configs/reload/multiple_users_2.conf")
	if err := server.Reload(); err != nil {
		t.Fatalf("Error reloading config: %v", err)
	}

	// Ensure connecting fails.
	if _, err := nats.Connect(addr, nats.UserInfo("alice", "foo")); err == nil {
		t.Fatal("Expected connect to fail")
	}

	// Ensure connecting succeeds when using new credentials.
	conn, err := nats.Connect(addr, nats.UserInfo("alice", "baz"))
	if err != nil {
		t.Fatalf("Error creating client: %v", err)
	}
	conn.Close()

	// Ensure the previous connection received an authorization error.
	// Note that it is possible that client gets EOF and not able to
	// process async error, so don't fail if we don't get it.
	select {
	case err := <-asyncErr:
		if err != nats.ErrAuthorization {
			t.Fatalf("Expected ErrAuthorization, got %v", err)
		}
	case <-time.After(time.Second):
	}

	// Ensure the previous connection was disconnected.
	select {
	case <-disconnected:
	case <-time.After(2 * time.Second):
		t.Fatal("Expected connection to be disconnected")
	}

	// Ensure the connection using unchanged credentials can still
	// publish/receive.
	if err := nc2.Publish("foo", []byte("hello")); err != nil {
		t.Fatalf("Error publishing: %v", err)
	}
	nc2.Flush()
	msg, err := sub.NextMsg(2 * time.Second)
	if err != nil {
		t.Fatalf("Error receiving msg: %v", err)
	}
	if string(msg.Data) != "hello" {
		t.Fatalf("Msg is incorrect.\nexpected: %+v\ngot: %+v", []byte("hello"), msg.Data)
	}
}

// Ensure Reload supports enabling users authentication. Test this by starting
// a server with authentication disabled, connect to it to verify, reload
// config using with users, ensure reconnect fails, then ensure reconnect
// succeeds when using the correct credentials.
func TestConfigReloadEnableUsersAuthentication(t *testing.T) {
	server, opts, config := runReloadServerWithConfig(t, "./configs/reload/basic.conf")
	defer server.Shutdown()

	// Ensure we can connect as a sanity check.
	addr := fmt.Sprintf("nats://%s:%d", opts.Host, opts.Port)
	nc, err := nats.Connect(addr)
	if err != nil {
		t.Fatalf("Error creating client: %v", err)
	}
	defer nc.Close()
	disconnected := make(chan struct{}, 1)
	asyncErr := make(chan error, 1)
	nc.SetErrorHandler(func(nc *nats.Conn, sub *nats.Subscription, err error) {
		asyncErr <- err
	})
	nc.SetDisconnectHandler(func(*nats.Conn) {
		disconnected <- struct{}{}
	})

	// Enable authentication.
	changeCurrentConfigContent(t, config, "./configs/reload/multiple_users_1.conf")
	if err := server.Reload(); err != nil {
		t.Fatalf("Error reloading config: %v", err)
	}

	// Ensure connecting fails.
	if _, err := nats.Connect(addr); err == nil {
		t.Fatal("Expected connect to fail")
	}

	// Ensure connecting succeeds when using new credentials.
	conn, err := nats.Connect(addr, nats.UserInfo("alice", "foo"))
	if err != nil {
		t.Fatalf("Error creating client: %v", err)
	}
	conn.Close()

	// Ensure the previous connection received an authorization error.
	// Note that it is possible that client gets EOF and not able to
	// process async error, so don't fail if we don't get it.
	select {
	case err := <-asyncErr:
		if err != nats.ErrAuthorization {
			t.Fatalf("Expected ErrAuthorization, got %v", err)
		}
	case <-time.After(time.Second):
	}

	// Ensure the previous connection was disconnected.
	select {
	case <-disconnected:
	case <-time.After(5 * time.Second):
		t.Fatal("Expected connection to be disconnected")
	}
}

// Ensure Reload supports disabling users authentication. Test this by starting
// a server with authentication enabled, connect to it to verify,
// reload config using with authentication disabled, then ensure connecting
// with no credentials succeeds.
func TestConfigReloadDisableUsersAuthentication(t *testing.T) {
	server, opts, config := runReloadServerWithConfig(t, "./configs/reload/multiple_users_1.conf")
	defer server.Shutdown()

	// Ensure we can connect as a sanity check.
	addr := fmt.Sprintf("nats://%s:%d", opts.Host, opts.Port)
	nc, err := nats.Connect(addr, nats.UserInfo("alice", "foo"))
	if err != nil {
		t.Fatalf("Error creating client: %v", err)
	}
	defer nc.Close()
	nc.SetErrorHandler(func(nc *nats.Conn, sub *nats.Subscription, err error) {
		t.Fatalf("Client received an unexpected error: %v", err)
	})

	// Disable authentication.
	changeCurrentConfigContent(t, config, "./configs/reload/basic.conf")
	if err := server.Reload(); err != nil {
		t.Fatalf("Error reloading config: %v", err)
	}

	// Ensure connecting succeeds with no credentials.
	conn, err := nats.Connect(addr)
	if err != nil {
		t.Fatalf("Error creating client: %v", err)
	}
	conn.Close()
}

// Ensure Reload supports changing permissions. Test this by starting a server
// with a user configured with certain permissions, test publish and subscribe,
// reload config with new permissions, ensure the previous subscription was
// closed and publishes fail, then ensure the new permissions succeed.
func TestConfigReloadChangePermissions(t *testing.T) {
	server, opts, config := runReloadServerWithConfig(t, "./configs/reload/authorization_1.conf")
	defer server.Shutdown()

	addr := fmt.Sprintf("nats://%s:%d", opts.Host, opts.Port)
	nc, err := nats.Connect(addr, nats.UserInfo("bob", "bar"))
	if err != nil {
		t.Fatalf("Error creating client: %v", err)
	}
	defer nc.Close()
	asyncErr := make(chan error, 1)
	nc.SetErrorHandler(func(nc *nats.Conn, sub *nats.Subscription, err error) {
		asyncErr <- err
	})
	// Ensure we can publish and receive messages as a sanity check.
	sub, err := nc.SubscribeSync("_INBOX.>")
	if err != nil {
		t.Fatalf("Error subscribing: %v", err)
	}
	nc.Flush()

	conn, err := nats.Connect(addr, nats.UserInfo("alice", "foo"))
	if err != nil {
		t.Fatalf("Error creating client: %v", err)
	}
	defer conn.Close()

	sub2, err := conn.SubscribeSync("req.foo")
	if err != nil {
		t.Fatalf("Error subscribing: %v", err)
	}
	if err := conn.Publish("_INBOX.foo", []byte("hello")); err != nil {
		t.Fatalf("Error publishing message: %v", err)
	}
	conn.Flush()

	msg, err := sub.NextMsg(2 * time.Second)
	if err != nil {
		t.Fatalf("Error receiving msg: %v", err)
	}
	if string(msg.Data) != "hello" {
		t.Fatalf("Msg is incorrect.\nexpected: %+v\ngot: %+v", []byte("hello"), msg.Data)
	}

	if err := nc.Publish("req.foo", []byte("world")); err != nil {
		t.Fatalf("Error publishing message: %v", err)
	}
	nc.Flush()

	msg, err = sub2.NextMsg(2 * time.Second)
	if err != nil {
		t.Fatalf("Error receiving msg: %v", err)
	}
	if string(msg.Data) != "world" {
		t.Fatalf("Msg is incorrect.\nexpected: %+v\ngot: %+v", []byte("world"), msg.Data)
	}

	// Susan will subscribe to two subjects, both will succeed but a send to foo.bar should not succeed
	// however PUBLIC.foo should.
	sconn, err := nats.Connect(addr, nats.UserInfo("susan", "baz"))
	if err != nil {
		t.Fatalf("Error creating client: %v", err)
	}
	defer sconn.Close()

	asyncErr2 := make(chan error, 1)
	sconn.SetErrorHandler(func(nc *nats.Conn, sub *nats.Subscription, err error) {
		asyncErr2 <- err
	})

	fooSub, err := sconn.SubscribeSync("foo.*")
	if err != nil {
		t.Fatalf("Error subscribing: %v", err)
	}
	sconn.Flush()

	// Publishing from bob on foo.bar should not come through.
	if err := conn.Publish("foo.bar", []byte("hello")); err != nil {
		t.Fatalf("Error publishing message: %v", err)
	}
	conn.Flush()

	_, err = fooSub.NextMsg(100 * time.Millisecond)
	if err != nats.ErrTimeout {
		t.Fatalf("Received a message we shouldn't have")
	}

	pubSub, err := sconn.SubscribeSync("PUBLIC.*")
	if err != nil {
		t.Fatalf("Error subscribing: %v", err)
	}
	sconn.Flush()

	select {
	case err := <-asyncErr2:
		t.Fatalf("Received unexpected error for susan: %v", err)
	default:
	}

	// This should work ok with original config.
	if err := conn.Publish("PUBLIC.foo", []byte("hello monkey")); err != nil {
		t.Fatalf("Error publishing message: %v", err)
	}
	conn.Flush()

	msg, err = pubSub.NextMsg(2 * time.Second)
	if err != nil {
		t.Fatalf("Error receiving msg: %v", err)
	}
	if string(msg.Data) != "hello monkey" {
		t.Fatalf("Msg is incorrect.\nexpected: %q\ngot: %q", "hello monkey", msg.Data)
	}

	///////////////////////////////////////////
	// Change permissions.
	///////////////////////////////////////////

	changeCurrentConfigContent(t, config, "./configs/reload/authorization_2.conf")
	if err := server.Reload(); err != nil {
		t.Fatalf("Error reloading config: %v", err)
	}

	// Ensure we receive an error for the subscription that is no longer authorized.
	// In this test, since connection is not closed by the server,
	// the client must receive an -ERR
	select {
	case err := <-asyncErr:
		if !strings.Contains(strings.ToLower(err.Error()), "permissions violation for subscription to \"_inbox.>\"") {
			t.Fatalf("Expected permissions violation error, got %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Expected permissions violation error")
	}

	// Ensure we receive an error when publishing to req.foo and we no longer
	// receive messages on _INBOX.>.
	if err := nc.Publish("req.foo", []byte("hola")); err != nil {
		t.Fatalf("Error publishing message: %v", err)
	}
	nc.Flush()
	if err := conn.Publish("_INBOX.foo", []byte("mundo")); err != nil {
		t.Fatalf("Error publishing message: %v", err)
	}
	conn.Flush()

	select {
	case err := <-asyncErr:
		if !strings.Contains(strings.ToLower(err.Error()), "permissions violation for publish to \"req.foo\"") {
			t.Fatalf("Expected permissions violation error, got %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Expected permissions violation error")
	}

	queued, _, err := sub2.Pending()
	if err != nil {
		t.Fatalf("Failed to get pending messaged: %v", err)
	}
	if queued != 0 {
		t.Fatalf("Pending is incorrect.\nexpected: 0\ngot: %d", queued)
	}

	queued, _, err = sub.Pending()
	if err != nil {
		t.Fatalf("Failed to get pending messaged: %v", err)
	}
	if queued != 0 {
		t.Fatalf("Pending is incorrect.\nexpected: 0\ngot: %d", queued)
	}

	// Ensure we can publish to _INBOX.foo.bar and subscribe to _INBOX.foo.>.
	sub, err = nc.SubscribeSync("_INBOX.foo.>")
	if err != nil {
		t.Fatalf("Error subscribing: %v", err)
	}
	nc.Flush()
	if err := nc.Publish("_INBOX.foo.bar", []byte("testing")); err != nil {
		t.Fatalf("Error publishing message: %v", err)
	}
	nc.Flush()
	msg, err = sub.NextMsg(2 * time.Second)
	if err != nil {
		t.Fatalf("Error receiving msg: %v", err)
	}
	if string(msg.Data) != "testing" {
		t.Fatalf("Msg is incorrect.\nexpected: %+v\ngot: %+v", []byte("testing"), msg.Data)
	}

	select {
	case err := <-asyncErr:
		t.Fatalf("Received unexpected error: %v", err)
	default:
	}

	// Now check susan again.
	//
	// This worked ok with original config but should not deliver a message now.
	if err := conn.Publish("PUBLIC.foo", []byte("hello monkey")); err != nil {
		t.Fatalf("Error publishing message: %v", err)
	}
	conn.Flush()

	_, err = pubSub.NextMsg(100 * time.Millisecond)
	if err != nats.ErrTimeout {
		t.Fatalf("Received a message we shouldn't have")
	}

	// Now check foo.bar, which did not work before but should work now..
	if err := conn.Publish("foo.bar", []byte("hello?")); err != nil {
		t.Fatalf("Error publishing message: %v", err)
	}
	conn.Flush()

	msg, err = fooSub.NextMsg(2 * time.Second)
	if err != nil {
		t.Fatalf("Error receiving msg: %v", err)
	}
	if string(msg.Data) != "hello?" {
		t.Fatalf("Msg is incorrect.\nexpected: %q\ngot: %q", "hello?", msg.Data)
	}

	// Once last check for no errors.
	sconn.Flush()

	select {
	case err := <-asyncErr2:
		t.Fatalf("Received unexpected error for susan: %v", err)
	default:
	}
}

func TestConfigReloadRevokesQueueSubscriptionWithQueueScopedDeny(t *testing.T) {
	config := func(deny string) []byte {
		return fmt.Appendf(nil, `
			listen: 127.0.0.1:-1
			authorization {
				users: [
					{
						user: attacker
						password: pass
						permissions: {
							subscribe: {
								allow: [">"]
								%s
							}
						}
					}
				]
			}
		`, deny)
	}

	s, opts, configFile := runReloadServerWithContent(t, config(""))
	defer s.Shutdown()

	asyncErr := make(chan error, 1)
	nc := natsConnect(t, fmt.Sprintf("nats://127.0.0.1:%d", opts.Port),
		nats.UserInfo("attacker", "pass"),
		nats.ErrorHandler(func(_ *nats.Conn, _ *nats.Subscription, err error) {
			asyncErr <- err
		}))
	defer nc.Close()

	sub := natsQueueSubSync(t, nc, "admin.secret", "workers")
	natsFlush(t, nc)

	changeCurrentConfigContentWithNewContent(t, configFile, config(`deny: ["admin.secret workers"]`))
	require_NoError(t, s.Reload())

	select {
	case err := <-asyncErr:
		if !strings.Contains(strings.ToLower(err.Error()), `permissions violation for subscription to "admin.secret" using queue "workers"`) {
			t.Fatalf("Expected queue subscription permission violation, got %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Expected queue subscription to be revoked")
	}
	natsPub(t, nc, "admin.secret", []byte("blocked"))
	natsFlush(t, nc)
	if _, err := sub.NextMsg(100 * time.Millisecond); err != nats.ErrTimeout {
		t.Fatalf("Expected revoked queue subscription not to receive messages, got %v", err)
	}
}

// Ensure Reload returns an error when attempting to change cluster address
// host.
func TestConfigReloadClusterHostUnsupported(t *testing.T) {
	server, _, config := runReloadServerWithConfig(t, "./configs/reload/srv_a_1.conf")
	defer server.Shutdown()

	// Attempt to change cluster listen host.
	changeCurrentConfigContent(t, config, "./configs/reload/srv_c_1.conf")

	// This should fail because cluster address cannot be changed.
	if err := server.Reload(); err == nil {
		t.Fatal("Expected Reload to return an error")
	}
}

// Ensure Reload returns an error when attempting to change cluster address
// port.
func TestConfigReloadClusterPortUnsupported(t *testing.T) {
	server, _, config := runReloadServerWithConfig(t, "./configs/reload/srv_a_1.conf")
	defer server.Shutdown()

	// Attempt to change cluster listen port.
	changeCurrentConfigContent(t, config, "./configs/reload/srv_b_1.conf")

	// This should fail because cluster address cannot be changed.
	if err := server.Reload(); err == nil {
		t.Fatal("Expected Reload to return an error")
	}
}

// Ensure Reload supports enabling route authorization. Test this by starting
// two servers in a cluster without authorization, ensuring messages flow
// between them, then reloading with authorization and ensuring messages no
// longer flow until reloading with the correct credentials.
func TestConfigReloadEnableClusterAuthorization(t *testing.T) {
	srvb, srvbOpts, srvbConfig := runReloadServerWithConfig(t, "./configs/reload/srv_b_1.conf")
	defer srvb.Shutdown()

	srva, srvaOpts, srvaConfig := runReloadServerWithConfig(t, "./configs/reload/srv_a_1.conf")
	defer srva.Shutdown()

	checkClusterFormed(t, srva, srvb)

	srvaAddr := fmt.Sprintf("nats://%s:%d", srvaOpts.Host, srvaOpts.Port)
	srvaConn, err := nats.Connect(srvaAddr)
	if err != nil {
		t.Fatalf("Error creating client: %v", err)
	}
	defer srvaConn.Close()
	sub, err := srvaConn.SubscribeSync("foo")
	if err != nil {
		t.Fatalf("Error subscribing: %v", err)
	}
	defer sub.Unsubscribe()
	if err := srvaConn.Flush(); err != nil {
		t.Fatalf("Error flushing: %v", err)
	}

	srvbAddr := fmt.Sprintf("nats://%s:%d", srvbOpts.Host, srvbOpts.Port)
	srvbConn, err := nats.Connect(srvbAddr)
	if err != nil {
		t.Fatalf("Error creating client: %v", err)
	}
	defer srvbConn.Close()

	if numRoutes := srvb.NumRoutes(); numRoutes != DEFAULT_ROUTE_POOL_SIZE {
		t.Fatalf("Expected %d route, got %d", DEFAULT_ROUTE_POOL_SIZE, numRoutes)
	}

	// Ensure messages flow through the cluster as a sanity check.
	if err := srvbConn.Publish("foo", []byte("hello")); err != nil {
		t.Fatalf("Error publishing: %v", err)
	}
	srvbConn.Flush()
	msg, err := sub.NextMsg(2 * time.Second)
	if err != nil {
		t.Fatalf("Error receiving message: %v", err)
	}
	if string(msg.Data) != "hello" {
		t.Fatalf("Msg is incorrect.\nexpected: %+v\ngot: %+v", []byte("hello"), msg.Data)
	}

	// Enable route authorization.
	changeCurrentConfigContent(t, srvbConfig, "./configs/reload/srv_b_2.conf")
	if err := srvb.Reload(); err != nil {
		t.Fatalf("Error reloading config: %v", err)
	}

	checkNumRoutes(t, srvb, 0)

	// Ensure messages no longer flow through the cluster.
	for i := 0; i < 5; i++ {
		if err := srvbConn.Publish("foo", []byte("world")); err != nil {
			t.Fatalf("Error publishing: %v", err)
		}
		srvbConn.Flush()
	}
	if _, err := sub.NextMsg(50 * time.Millisecond); err != nats.ErrTimeout {
		t.Fatalf("Expected ErrTimeout, got %v", err)
	}

	// Reload Server A with correct route credentials.
	changeCurrentConfigContent(t, srvaConfig, "./configs/reload/srv_a_2.conf")
	if err := srva.Reload(); err != nil {
		t.Fatalf("Error reloading config: %v", err)
	}
	checkClusterFormed(t, srva, srvb)

	if numRoutes := srvb.NumRoutes(); numRoutes != DEFAULT_ROUTE_POOL_SIZE {
		t.Fatalf("Expected %d route, got %d", DEFAULT_ROUTE_POOL_SIZE, numRoutes)
	}

	// Ensure messages flow through the cluster now.
	if err := srvbConn.Publish("foo", []byte("hola")); err != nil {
		t.Fatalf("Error publishing: %v", err)
	}
	srvbConn.Flush()
	msg, err = sub.NextMsg(2 * time.Second)
	if err != nil {
		t.Fatalf("Error receiving message: %v", err)
	}
	if string(msg.Data) != "hola" {
		t.Fatalf("Msg is incorrect.\nexpected: %+v\ngot: %+v", []byte("hola"), msg.Data)
	}
}

// Ensure Reload supports disabling route authorization. Test this by starting
// two servers in a cluster with authorization, ensuring messages flow
// between them, then reloading without authorization and ensuring messages
// still flow.
func TestConfigReloadDisableClusterAuthorization(t *testing.T) {
	srvb, srvbOpts, srvbConfig := runReloadServerWithConfig(t, "./configs/reload/srv_b_2.conf")
	defer srvb.Shutdown()

	srva, srvaOpts, _ := runReloadServerWithConfig(t, "./configs/reload/srv_a_2.conf")
	defer srva.Shutdown()

	checkClusterFormed(t, srva, srvb)

	srvaAddr := fmt.Sprintf("nats://%s:%d", srvaOpts.Host, srvaOpts.Port)
	srvaConn, err := nats.Connect(srvaAddr)
	if err != nil {
		t.Fatalf("Error creating client: %v", err)
	}
	defer srvaConn.Close()

	sub, err := srvaConn.SubscribeSync("foo")
	if err != nil {
		t.Fatalf("Error subscribing: %v", err)
	}
	defer sub.Unsubscribe()
	if err := srvaConn.Flush(); err != nil {
		t.Fatalf("Error flushing: %v", err)
	}

	srvbAddr := fmt.Sprintf("nats://%s:%d", srvbOpts.Host, srvbOpts.Port)
	srvbConn, err := nats.Connect(srvbAddr)
	if err != nil {
		t.Fatalf("Error creating client: %v", err)
	}
	defer srvbConn.Close()

	if numRoutes := srvb.NumRoutes(); numRoutes != DEFAULT_ROUTE_POOL_SIZE {
		t.Fatalf("Expected %d route, got %d", DEFAULT_ROUTE_POOL_SIZE, numRoutes)
	}

	// Ensure messages flow through the cluster as a sanity check.
	if err := srvbConn.Publish("foo", []byte("hello")); err != nil {
		t.Fatalf("Error publishing: %v", err)
	}
	srvbConn.Flush()
	msg, err := sub.NextMsg(2 * time.Second)
	if err != nil {
		t.Fatalf("Error receiving message: %v", err)
	}
	if string(msg.Data) != "hello" {
		t.Fatalf("Msg is incorrect.\nexpected: %+v\ngot: %+v", []byte("hello"), msg.Data)
	}

	// Disable route authorization.
	changeCurrentConfigContent(t, srvbConfig, "./configs/reload/srv_b_1.conf")
	if err := srvb.Reload(); err != nil {
		t.Fatalf("Error reloading config: %v", err)
	}

	checkClusterFormed(t, srva, srvb)

	if numRoutes := srvb.NumRoutes(); numRoutes != DEFAULT_ROUTE_POOL_SIZE {
		t.Fatalf("Expected %d route, got %d", DEFAULT_ROUTE_POOL_SIZE, numRoutes)
	}

	// Ensure messages still flow through the cluster.
	if err := srvbConn.Publish("foo", []byte("hola")); err != nil {
		t.Fatalf("Error publishing: %v", err)
	}
	srvbConn.Flush()
	msg, err = sub.NextMsg(2 * time.Second)
	if err != nil {
		t.Fatalf("Error receiving message: %v", err)
	}
	if string(msg.Data) != "hola" {
		t.Fatalf("Msg is incorrect.\nexpected: %+v\ngot: %+v", []byte("hola"), msg.Data)
	}
}

// Ensure Reload supports changing cluster routes. Test this by starting
// two servers in a cluster, ensuring messages flow between them, then
// reloading with a different route and ensuring messages flow through the new
// cluster.
func TestConfigReloadClusterRoutes(t *testing.T) {
	srvb, srvbOpts, _ := runReloadServerWithConfig(t, "./configs/reload/srv_b_1.conf")
	defer srvb.Shutdown()

	srva, srvaOpts, srvaConfig := runReloadServerWithConfig(t, "./configs/reload/srv_a_1.conf")
	defer srva.Shutdown()

	checkClusterFormed(t, srva, srvb)

	srvcOpts, err := ProcessConfigFile("./configs/reload/srv_c_1.conf")
	if err != nil {
		t.Fatalf("Error processing config file: %v", err)
	}
	srvcOpts.NoLog = true
	srvcOpts.NoSigs = true

	srvc := RunServer(srvcOpts)
	defer srvc.Shutdown()

	srvaAddr := fmt.Sprintf("nats://%s:%d", srvaOpts.Host, srvaOpts.Port)
	srvaConn, err := nats.Connect(srvaAddr)
	if err != nil {
		t.Fatalf("Error creating client: %v", err)
	}
	defer srvaConn.Close()

	sub, err := srvaConn.SubscribeSync("foo")
	if err != nil {
		t.Fatalf("Error subscribing: %v", err)
	}
	defer sub.Unsubscribe()
	if err := srvaConn.Flush(); err != nil {
		t.Fatalf("Error flushing: %v", err)
	}

	srvbAddr := fmt.Sprintf("nats://%s:%d", srvbOpts.Host, srvbOpts.Port)
	srvbConn, err := nats.Connect(srvbAddr)
	if err != nil {
		t.Fatalf("Error creating client: %v", err)
	}
	defer srvbConn.Close()

	if numRoutes := srvb.NumRoutes(); numRoutes != DEFAULT_ROUTE_POOL_SIZE {
		t.Fatalf("Expected %d route, got %d", DEFAULT_ROUTE_POOL_SIZE, numRoutes)
	}

	// Ensure consumer on srvA is propagated to srvB
	checkExpectedSubs(t, 1, srvb)

	// Ensure messages flow through the cluster as a sanity check.
	if err := srvbConn.Publish("foo", []byte("hello")); err != nil {
		t.Fatalf("Error publishing: %v", err)
	}
	srvbConn.Flush()
	msg, err := sub.NextMsg(2 * time.Second)
	if err != nil {
		t.Fatalf("Error receiving message: %v", err)
	}
	if string(msg.Data) != "hello" {
		t.Fatalf("Msg is incorrect.\nexpected: %+v\ngot: %+v", []byte("hello"), msg.Data)
	}

	// Reload cluster routes.
	changeCurrentConfigContent(t, srvaConfig, "./configs/reload/srv_a_3.conf")
	if err := srva.Reload(); err != nil {
		t.Fatalf("Error reloading config: %v", err)
	}

	// Kill old route server.
	srvbConn.Close()
	srvb.Shutdown()

	checkClusterFormed(t, srva, srvc)

	srvcAddr := fmt.Sprintf("nats://%s:%d", srvcOpts.Host, srvcOpts.Port)
	srvcConn, err := nats.Connect(srvcAddr)
	if err != nil {
		t.Fatalf("Error creating client: %v", err)
	}
	defer srvcConn.Close()

	// Ensure messages flow through the new cluster.
	for i := 0; i < 5; i++ {
		if err := srvcConn.Publish("foo", []byte("hola")); err != nil {
			t.Fatalf("Error publishing: %v", err)
		}
		srvcConn.Flush()
	}
	msg, err = sub.NextMsg(2 * time.Second)
	if err != nil {
		t.Fatalf("Error receiving message: %v", err)
	}
	if string(msg.Data) != "hola" {
		t.Fatalf("Msg is incorrect.\nexpected: %+v\ngot: %+v", []byte("hola"), msg.Data)
	}
}

// Ensure Reload supports removing a solicited route. In this case from A->B
// Test this by starting two servers in a cluster, ensuring messages flow between them.
// Then stop server B, and have server A continue to try to connect. Reload A with a config
// that removes the route and make sure it does not connect to server B when its restarted.
func TestConfigReloadClusterRemoveSolicitedRoutes(t *testing.T) {
	srvb, srvbOpts := RunServerWithConfig("./configs/reload/srv_b_1.conf")
	defer srvb.Shutdown()

	srva, srvaOpts, srvaConfig := runReloadServerWithConfig(t, "./configs/reload/srv_a_1.conf")
	defer srva.Shutdown()

	checkClusterFormed(t, srva, srvb)

	srvaAddr := fmt.Sprintf("nats://%s:%d", srvaOpts.Host, srvaOpts.Port)
	srvaConn, err := nats.Connect(srvaAddr)
	if err != nil {
		t.Fatalf("Error creating client: %v", err)
	}
	defer srvaConn.Close()
	sub, err := srvaConn.SubscribeSync("foo")
	if err != nil {
		t.Fatalf("Error subscribing: %v", err)
	}
	defer sub.Unsubscribe()
	if err := srvaConn.Flush(); err != nil {
		t.Fatalf("Error flushing: %v", err)
	}
	checkExpectedSubs(t, 1, srvb)

	srvbAddr := fmt.Sprintf("nats://%s:%d", srvbOpts.Host, srvbOpts.Port)
	srvbConn, err := nats.Connect(srvbAddr)
	if err != nil {
		t.Fatalf("Error creating client: %v", err)
	}
	defer srvbConn.Close()

	if err := srvbConn.Publish("foo", []byte("hello")); err != nil {
		t.Fatalf("Error publishing: %v", err)
	}
	srvbConn.Flush()
	msg, err := sub.NextMsg(5 * time.Second)
	if err != nil {
		t.Fatalf("Error receiving message: %v", err)
	}
	if string(msg.Data) != "hello" {
		t.Fatalf("Msg is incorrect.\nexpected: %+v\ngot: %+v", []byte("hello"), msg.Data)
	}

	// Now stop server B.
	srvb.Shutdown()

	// Wait til route is dropped.
	checkNumRoutes(t, srva, 0)

	// Now change config for server A to not solicit a route to server B.
	changeCurrentConfigContent(t, srvaConfig, "./configs/reload/srv_a_4.conf")
	if err := srva.Reload(); err != nil {
		t.Fatalf("Error reloading config: %v", err)
	}

	// Restart server B.
	srvb, _ = RunServerWithConfig("./configs/reload/srv_b_1.conf")
	defer srvb.Shutdown()

	// We should not have a cluster formed here.
	numRoutes := 0
	deadline := time.Now().Add(routeReconnectDelay + 500*time.Millisecond)
	for time.Now().Before(deadline) {
		if numRoutes = srva.NumRoutes(); numRoutes != 0 {
			break
		} else {
			time.Sleep(100 * time.Millisecond)
		}
	}
	if numRoutes != 0 {
		t.Fatalf("Expected 0 routes for server A, got %d", numRoutes)
	}
}

func reloadUpdateConfig(t *testing.T, s *Server, conf, content string) {
	t.Helper()
	if err := os.WriteFile(conf, []byte(content), 0666); err != nil {
		t.Fatalf("Error creating config file: %v", err)
	}
	if err := s.Reload(); err != nil {
		t.Fatalf("Error on reload: %v", err)
	}
}

func TestConfigReloadClusterAdvertise(t *testing.T) {
	s, _, conf := runReloadServerWithContent(t, []byte(`
		listen: "0.0.0.0:-1"
		cluster: {
			listen: "0.0.0.0:-1"
		}
	`))
	defer s.Shutdown()

	orgClusterPort := s.ClusterAddr().Port

	verify := func(expectedHost string, expectedPort int, expectedIP string) {
		s.mu.Lock()
		routeInfo := s.routeInfo
		rij := generateInfoJSON(&routeInfo)
		s.mu.Unlock()
		if routeInfo.Host != expectedHost || routeInfo.Port != expectedPort || routeInfo.IP != expectedIP {
			t.Fatalf("Expected host/port/IP to be %s:%v, %q, got %s:%d, %q",
				expectedHost, expectedPort, expectedIP, routeInfo.Host, routeInfo.Port, routeInfo.IP)
		}
		routeInfoJSON := Info{}
		err := json.Unmarshal(rij[5:], &routeInfoJSON) // Skip "INFO "
		if err != nil {
			t.Fatalf("Error on Unmarshal: %v", err)
		}
		// Check that server routeInfoJSON was updated too
		if !reflect.DeepEqual(routeInfo, routeInfoJSON) {
			t.Fatalf("Expected routeInfoJSON to be %+v, got %+v", routeInfo, routeInfoJSON)
		}
	}

	// Update config with cluster_advertise
	reloadUpdateConfig(t, s, conf, `
	listen: "0.0.0.0:-1"
	cluster: {
		listen: "0.0.0.0:-1"
		cluster_advertise: "me:1"
	}
	`)
	verify("me", 1, "nats-route://me:1/")

	// Update config with cluster_advertise (no port specified)
	reloadUpdateConfig(t, s, conf, `
	listen: "0.0.0.0:-1"
	cluster: {
		listen: "0.0.0.0:-1"
		cluster_advertise: "me"
	}
	`)
	verify("me", orgClusterPort, fmt.Sprintf("nats-route://me:%d/", orgClusterPort))

	// Update config with cluster_advertise (-1 port specified)
	reloadUpdateConfig(t, s, conf, `
	listen: "0.0.0.0:-1"
	cluster: {
		listen: "0.0.0.0:-1"
		cluster_advertise: "me:-1"
	}
	`)
	verify("me", orgClusterPort, fmt.Sprintf("nats-route://me:%d/", orgClusterPort))

	// Update to remove cluster_advertise
	reloadUpdateConfig(t, s, conf, `
	listen: "0.0.0.0:-1"
	cluster: {
		listen: "0.0.0.0:-1"
	}
	`)
	verify("0.0.0.0", orgClusterPort, "")
}

func TestConfigReloadClusterNoAdvertise(t *testing.T) {
	s, _, conf := runReloadServerWithContent(t, []byte(`
		listen: "0.0.0.0:-1"
		client_advertise: "me:1"
		cluster: {
			listen: "0.0.0.0:-1"
		}
	`))
	defer s.Shutdown()

	s.mu.Lock()
	ccurls := s.routeInfo.ClientConnectURLs
	s.mu.Unlock()
	if len(ccurls) != 1 && ccurls[0] != "me:1" {
		t.Fatalf("Unexpected routeInfo.ClientConnectURLS: %v", ccurls)
	}

	// Update config with no_advertise
	reloadUpdateConfig(t, s, conf, `
	listen: "0.0.0.0:-1"
	client_advertise: "me:1"
	cluster: {
		listen: "0.0.0.0:-1"
		no_advertise: true
	}
	`)

	s.mu.Lock()
	ccurls = s.routeInfo.ClientConnectURLs
	s.mu.Unlock()
	if len(ccurls) != 0 {
		t.Fatalf("Unexpected routeInfo.ClientConnectURLS: %v", ccurls)
	}

	// Update config with cluster_advertise (no port specified)
	reloadUpdateConfig(t, s, conf, `
	listen: "0.0.0.0:-1"
	client_advertise: "me:1"
	cluster: {
		listen: "0.0.0.0:-1"
	}
	`)
	s.mu.Lock()
	ccurls = s.routeInfo.ClientConnectURLs
	s.mu.Unlock()
	if len(ccurls) != 1 && ccurls[0] != "me:1" {
		t.Fatalf("Unexpected routeInfo.ClientConnectURLS: %v", ccurls)
	}
}

func TestConfigReloadClusterName(t *testing.T) {
	s, _, conf := runReloadServerWithContent(t, []byte(`
		listen: "0.0.0.0:-1"
		cluster: {
			name: "abc"
			listen: "0.0.0.0:-1"
		}
	`))
	defer s.Shutdown()

	// Update config with a new cluster name.
	reloadUpdateConfig(t, s, conf, `
	listen: "0.0.0.0:-1"
	cluster: {
		name: "xyz"
		listen: "0.0.0.0:-1"
	}
	`)

	if s.ClusterName() != "xyz" {
		t.Fatalf("Expected update clustername of \"xyz\", got %q", s.ClusterName())
	}
}

func TestConfigReloadMaxSubsUnsupported(t *testing.T) {
	s, _, conf := runReloadServerWithContent(t, []byte(`
		port: -1
		max_subs: 1
		`))
	defer s.Shutdown()

	if err := os.WriteFile(conf, []byte(`max_subs: 10`), 0666); err != nil {
		t.Fatalf("Error writing config file: %v", err)
	}
	if err := s.Reload(); err == nil {
		t.Fatal("Expected Reload to return an error")
	}
}

func TestConfigReloadClientAdvertise(t *testing.T) {
	s, _, conf := runReloadServerWithContent(t, []byte(`listen: "0.0.0.0:-1"`))
	defer s.Shutdown()

	orgPort := s.Addr().(*net.TCPAddr).Port

	verify := func(expectedHost string, expectedPort int) {
		s.mu.Lock()
		info := s.info
		s.mu.Unlock()
		if info.Host != expectedHost || info.Port != expectedPort {
			stackFatalf(t, "Expected host/port to be %s:%d, got %s:%d",
				expectedHost, expectedPort, info.Host, info.Port)
		}
	}

	// Update config with ClientAdvertise (port specified)
	reloadUpdateConfig(t, s, conf, `
	listen: "0.0.0.0:-1"
	client_advertise: "me:1"
	`)
	verify("me", 1)

	// Update config with ClientAdvertise (no port specified)
	reloadUpdateConfig(t, s, conf, `
	listen: "0.0.0.0:-1"
	client_advertise: "me"
	`)
	verify("me", orgPort)

	// Update config with ClientAdvertise (-1 port specified)
	reloadUpdateConfig(t, s, conf, `
	listen: "0.0.0.0:-1"
	client_advertise: "me:-1"
	`)
	verify("me", orgPort)

	// Now remove ClientAdvertise to check that original values
	// are restored.
	reloadUpdateConfig(t, s, conf, `listen: "0.0.0.0:-1"`)
	verify("0.0.0.0", orgPort)
}

// Ensure Reload supports changing the max connections. Test this by starting a
// server with no max connections, connecting two clients, reloading with a
// max connections of one, and ensuring one client is disconnected.
func TestConfigReloadMaxConnections(t *testing.T) {
	server, opts, config := runReloadServerWithConfig(t, "./configs/reload/basic.conf")
	defer server.Shutdown()

	// Make two connections.
	addr := fmt.Sprintf("nats://%s:%d", opts.Host, server.Addr().(*net.TCPAddr).Port)
	nc1, err := nats.Connect(addr)
	if err != nil {
		t.Fatalf("Error creating client: %v", err)
	}
	defer nc1.Close()
	closed := make(chan struct{}, 1)
	nc1.SetDisconnectHandler(func(*nats.Conn) {
		closed <- struct{}{}
	})
	nc2, err := nats.Connect(addr)
	if err != nil {
		t.Fatalf("Error creating client: %v", err)
	}
	defer nc2.Close()
	nc2.SetDisconnectHandler(func(*nats.Conn) {
		closed <- struct{}{}
	})

	if numClients := server.NumClients(); numClients != 2 {
		t.Fatalf("Expected 2 clients, got %d", numClients)
	}

	// Set max connections to one.
	changeCurrentConfigContent(t, config, "./configs/reload/max_connections.conf")
	if err := server.Reload(); err != nil {
		t.Fatalf("Error reloading config: %v", err)
	}

	// Ensure one connection was closed.
	select {
	case <-closed:
	case <-time.After(5 * time.Second):
		t.Fatal("Expected to be disconnected")
	}

	checkClientsCount(t, server, 1)

	// Ensure new connections fail.
	_, err = nats.Connect(addr)
	if err == nil {
		t.Fatal("Expected error on connect")
	}
}

// Ensure Reload supports refusing all connections. Test this by starting a
// server with no max connections, connecting two clients, reloading with a
// max connections of one, and ensuring one client is disconnected.
func TestConfigReloadMaxConnectionsPreventAll(t *testing.T) {
	server, opts, config := runReloadServerWithConfig(t, "./configs/reload/basic.conf")
	defer server.Shutdown()

	// Make two connections.
	addr := fmt.Sprintf("nats://%s:%d", opts.Host, server.Addr().(*net.TCPAddr).Port)
	nc1, err := nats.Connect(addr)
	if err != nil {
		t.Fatalf("Error creating client: %v", err)
	}
	defer nc1.Close()
	closed := make(chan struct{}, 1)
	nc1.SetDisconnectHandler(func(*nats.Conn) {
		closed <- struct{}{}
	})
	nc2, err := nats.Connect(addr)
	if err != nil {
		t.Fatalf("Error creating client: %v", err)
	}
	defer nc2.Close()
	nc2.SetDisconnectHandler(func(*nats.Conn) {
		closed <- struct{}{}
	})

	if numClients := server.NumClients(); numClients != 2 {
		t.Fatalf("Expected 2 clients, got %d", numClients)
	}

	// Set max connections to one.
	changeCurrentConfigContent(t, config, "./configs/reload/max_connections_refuse_all.conf")
	if err := server.Reload(); err != nil {
		t.Fatalf("Error reloading config: %v", err)
	}

	// Ensure one connection was closed.
	select {
	case <-closed:
	case <-time.After(5 * time.Second):
		t.Fatal("Expected to be disconnected")
	}

	checkClientsCount(t, server, 0)

	// Ensure new connections fail.
	_, err = nats.Connect(addr)
	if err == nil {
		t.Fatal("Expected error on connect")
	}
}

// Ensure reload supports changing the max payload size. Test this by starting
// a server with the default size limit, ensuring publishes work, reloading
// with a restrictive limit, and ensuring publishing an oversized message fails
// and disconnects the client.
func TestConfigReloadMaxPayload(t *testing.T) {
	server, opts, config := runReloadServerWithConfig(t, "./configs/reload/basic.conf")
	defer server.Shutdown()

	addr := fmt.Sprintf("nats://%s:%d", opts.Host, server.Addr().(*net.TCPAddr).Port)
	nc, err := nats.Connect(addr)
	if err != nil {
		t.Fatalf("Error creating client: %v", err)
	}
	defer nc.Close()
	closed := make(chan struct{})
	nc.SetDisconnectHandler(func(*nats.Conn) {
		closed <- struct{}{}
	})

	conn, err := nats.Connect(addr)
	if err != nil {
		t.Fatalf("Error creating client: %v", err)
	}
	defer conn.Close()
	sub, err := conn.SubscribeSync("foo")
	if err != nil {
		t.Fatalf("Error subscribing: %v", err)
	}
	conn.Flush()

	// Ensure we can publish as a sanity check.
	if err := nc.Publish("foo", []byte("hello")); err != nil {
		t.Fatalf("Error publishing: %v", err)
	}
	nc.Flush()
	_, err = sub.NextMsg(2 * time.Second)
	if err != nil {
		t.Fatalf("Error receiving message: %v", err)
	}

	// Set max payload to one.
	changeCurrentConfigContent(t, config, "./configs/reload/max_payload.conf")
	if err := server.Reload(); err != nil {
		t.Fatalf("Error reloading config: %v", err)
	}

	// Ensure oversized messages don't get delivered and the client is
	// disconnected.
	if err := nc.Publish("foo", []byte("hello")); err != nil {
		t.Fatalf("Error publishing: %v", err)
	}
	nc.Flush()
	_, err = sub.NextMsg(20 * time.Millisecond)
	if err != nats.ErrTimeout {
		t.Fatalf("Expected ErrTimeout, got: %v", err)
	}

	select {
	case <-closed:
	case <-time.After(5 * time.Second):
		t.Fatal("Expected to be disconnected")
	}
}

// Ensure reload supports rotating out files. Test this by starting
// a server with log and pid files, reloading new ones, then check that
// we can rename and delete the old log/pid files.
func TestConfigReloadRotateFiles(t *testing.T) {
	server, _, config := runReloadServerWithConfig(t, "./configs/reload/file_rotate.conf")
	defer func() {
		removeFile(t, "log1.txt")
		removeFile(t, "nats-server1.pid")
	}()
	defer server.Shutdown()

	// Configure the logger to enable actual logging
	opts := server.getOpts()
	opts.NoLog = false
	server.ConfigureLogger()

	// Load a config that renames the files.
	changeCurrentConfigContent(t, config, "./configs/reload/file_rotate1.conf")
	if err := server.Reload(); err != nil {
		t.Fatalf("Error reloading config: %v", err)
	}

	// Make sure the new files exist.
	if _, err := os.Stat("log1.txt"); os.IsNotExist(err) {
		t.Fatalf("Error reloading config, no new file: %v", err)
	}
	if _, err := os.Stat("nats-server1.pid"); os.IsNotExist(err) {
		t.Fatalf("Error reloading config, no new file: %v", err)
	}

	// Check that old file can be renamed.
	if err := os.Rename("log.txt", "log_old.txt"); err != nil {
		t.Fatalf("Error reloading config, cannot rename file: %v", err)
	}
	if err := os.Rename("nats-server.pid", "nats-server_old.pid"); err != nil {
		t.Fatalf("Error reloading config, cannot rename file: %v", err)
	}

	// Check that the old files can be removed after rename.
	removeFile(t, "log_old.txt")
	removeFile(t, "nats-server_old.pid")
}

func TestConfigReloadClusterWorks(t *testing.T) {
	confBTemplate := `
		listen: -1
		cluster: {
			listen: 127.0.0.1:7244
			authorization {
				user: ruser
				password: pwd
				timeout: %d
			}
			routes = [
				nats-route://ruser:pwd@127.0.0.1:7246
			]
		}`
	confB := createConfFile(t, []byte(fmt.Sprintf(confBTemplate, 3)))

	confATemplate := `
		listen: -1
		cluster: {
			listen: 127.0.0.1:7246
			authorization {
				user: ruser
				password: pwd
				timeout: %d
			}
			routes = [
				nats-route://ruser:pwd@127.0.0.1:7244
			]
		}`
	confA := createConfFile(t, []byte(fmt.Sprintf(confATemplate, 3)))

	srvb, _ := RunServerWithConfig(confB)
	defer srvb.Shutdown()

	srva, _ := RunServerWithConfig(confA)
	defer srva.Shutdown()

	// Wait for the cluster to form and capture the connection IDs of each route
	checkClusterFormed(t, srva, srvb)

	getCID := func(s *Server) uint64 {
		s.mu.Lock()
		defer s.mu.Unlock()
		if r := getFirstRoute(s); r != nil {
			return r.cid
		}
		return 0
	}
	acid := getCID(srva)
	bcid := getCID(srvb)

	// Update auth timeout to force a check of the connected route auth
	reloadUpdateConfig(t, srvb, confB, fmt.Sprintf(confBTemplate, 5))
	reloadUpdateConfig(t, srva, confA, fmt.Sprintf(confATemplate, 5))

	// Wait a little bit to ensure that there is no issue with connection
	// breaking at this point (this was an issue before).
	time.Sleep(100 * time.Millisecond)

	// Cluster should still exist
	checkClusterFormed(t, srva, srvb)

	// Check that routes were not re-created
	newacid := getCID(srva)
	newbcid := getCID(srvb)

	if newacid != acid {
		t.Fatalf("Expected server A route ID to be %v, got %v", acid, newacid)
	}
	if newbcid != bcid {
		t.Fatalf("Expected server B route ID to be %v, got %v", bcid, newbcid)
	}
}

func TestConfigReloadClusterPerms(t *testing.T) {
	confATemplate := `
		port: -1
		cluster {
			listen: 127.0.0.1:-1
			permissions {
				import {
					allow: %s
				}
				export {
					allow: %s
				}
			}
		}
		no_sys_acc: true
	`
	confA := createConfFile(t, []byte(fmt.Sprintf(confATemplate, `"foo"`, `"foo"`)))
	srva, _ := RunServerWithConfig(confA)
	defer srva.Shutdown()

	confBTemplate := `
		port: -1
		cluster {
			listen: 127.0.0.1:-1
			permissions {
				import {
					allow: %s
				}
				export {
					allow: %s
				}
			}
			routes = [
				"nats://127.0.0.1:%d"
			]
		}
		no_sys_acc: true
	`
	confB := createConfFile(t, []byte(fmt.Sprintf(confBTemplate, `"foo"`, `"foo"`, srva.ClusterAddr().Port)))
	srvb, _ := RunServerWithConfig(confB)
	defer srvb.Shutdown()

	checkClusterFormed(t, srva, srvb)

	// Create a connection on A
	nca, err := nats.Connect(fmt.Sprintf("nats://127.0.0.1:%d", srva.Addr().(*net.TCPAddr).Port))
	if err != nil {
		t.Fatalf("Error on connect: %v", err)
	}
	defer nca.Close()
	// Create a subscription on "foo" and "bar", only "foo" will be also on server B.
	subFooOnA, err := nca.SubscribeSync("foo")
	if err != nil {
		t.Fatalf("Error on subscribe: %v", err)
	}
	subBarOnA, err := nca.SubscribeSync("bar")
	if err != nil {
		t.Fatalf("Error on subscribe: %v", err)
	}

	// Connect on B and do the same
	ncb, err := nats.Connect(fmt.Sprintf("nats://127.0.0.1:%d", srvb.Addr().(*net.TCPAddr).Port))
	if err != nil {
		t.Fatalf("Error on connect: %v", err)
	}
	defer ncb.Close()
	// Create a subscription on "foo" and "bar", only "foo" will be also on server B.
	subFooOnB, err := ncb.SubscribeSync("foo")
	if err != nil {
		t.Fatalf("Error on subscribe: %v", err)
	}
	subBarOnB, err := ncb.SubscribeSync("bar")
	if err != nil {
		t.Fatalf("Error on subscribe: %v", err)
	}

	// Check subscriptions on each server. There should be 3 on each server,
	// foo and bar locally and foo from remote server.
	checkExpectedSubs(t, 3, srva, srvb)

	sendMsg := func(t *testing.T, subj string, nc *nats.Conn) {
		t.Helper()
		if err := nc.Publish(subj, []byte("msg")); err != nil {
			t.Fatalf("Error on publish: %v", err)
		}
	}

	checkSub := func(t *testing.T, sub *nats.Subscription, shouldReceive bool) {
		t.Helper()
		_, err := sub.NextMsg(100 * time.Millisecond)
		if shouldReceive && err != nil {
			t.Fatalf("Expected message on %q, got %v", sub.Subject, err)
		} else if !shouldReceive && err == nil {
			t.Fatalf("Expected no message on %q, got one", sub.Subject)
		}
	}

	// Produce from A and check received on both sides
	sendMsg(t, "foo", nca)
	checkSub(t, subFooOnA, true)
	checkSub(t, subFooOnB, true)
	// Now from B:
	sendMsg(t, "foo", ncb)
	checkSub(t, subFooOnA, true)
	checkSub(t, subFooOnB, true)

	// Publish on bar from A and make sure only local sub receives
	sendMsg(t, "bar", nca)
	checkSub(t, subBarOnA, true)
	checkSub(t, subBarOnB, false)

	// Publish on bar from B and make sure only local sub receives
	sendMsg(t, "bar", ncb)
	checkSub(t, subBarOnA, false)
	checkSub(t, subBarOnB, true)

	// We will now both import/export foo and bar. Start with reloading A.
	reloadUpdateConfig(t, srva, confA, fmt.Sprintf(confATemplate, `["foo", "bar"]`, `["foo", "bar"]`))

	// Since B has not been updated yet, the state should remain the same,
	// that is 3 subs on each server.
	checkExpectedSubs(t, 3, srva, srvb)

	// Now update and reload B. Add "baz" for another test down below
	reloadUpdateConfig(t, srvb, confB, fmt.Sprintf(confBTemplate, `["foo", "bar", "baz"]`, `["foo", "bar", "baz"]`, srva.ClusterAddr().Port))

	// Now 4 on each server
	checkExpectedSubs(t, 4, srva, srvb)

	// Make sure that we can receive all messages
	sendMsg(t, "foo", nca)
	checkSub(t, subFooOnA, true)
	checkSub(t, subFooOnB, true)
	sendMsg(t, "foo", ncb)
	checkSub(t, subFooOnA, true)
	checkSub(t, subFooOnB, true)

	sendMsg(t, "bar", nca)
	checkSub(t, subBarOnA, true)
	checkSub(t, subBarOnB, true)
	sendMsg(t, "bar", ncb)
	checkSub(t, subBarOnA, true)
	checkSub(t, subBarOnB, true)

	// Create subscription on baz on server B.
	subBazOnB, err := ncb.SubscribeSync("baz")
	if err != nil {
		t.Fatalf("Error on subscribe: %v", err)
	}
	// Check subscriptions count
	checkExpectedSubs(t, 5, srvb)
	checkExpectedSubs(t, 4, srva)

	sendMsg(t, "baz", nca)
	checkSub(t, subBazOnB, false)
	sendMsg(t, "baz", ncb)
	checkSub(t, subBazOnB, true)

	// Test UNSUB by denying something that was previously imported
	reloadUpdateConfig(t, srva, confA, fmt.Sprintf(confATemplate, `"foo"`, `["foo", "bar"]`))
	// Since A no longer imports "bar", we should have one less subscription
	// on B (B will have received an UNSUB for bar)
	checkExpectedSubs(t, 4, srvb)
	// A, however, should still have same number of subs.
	checkExpectedSubs(t, 4, srva)

	// Remove all permissions from A.
	reloadUpdateConfig(t, srva, confA, `
		port: -1
		cluster {
			listen: 127.0.0.1:-1
		}
		no_sys_acc: true
	`)
	// Server A should now have baz sub
	checkExpectedSubs(t, 5, srvb)
	checkExpectedSubs(t, 5, srva)

	sendMsg(t, "baz", nca)
	checkSub(t, subBazOnB, true)
	sendMsg(t, "baz", ncb)
	checkSub(t, subBazOnB, true)

	// Finally, remove permissions from B
	reloadUpdateConfig(t, srvb, confB, fmt.Sprintf(`
		port: -1
		cluster {
			listen: 127.0.0.1:-1
			routes = [
				"nats://127.0.0.1:%d"
			]
		}
		no_sys_acc: true
	`, srva.ClusterAddr().Port))
	// Check expected subscriptions count.
	checkExpectedSubs(t, 5, srvb)
	checkExpectedSubs(t, 5, srva)
}

func TestConfigReloadClusterPermsImport(t *testing.T) {
	confATemplate := `
		port: -1
		cluster {
			listen: 127.0.0.1:-1
			permissions {
				import: {
					allow: %s
				}
			}
		}
		no_sys_acc: true
	`
	confA := createConfFile(t, []byte(fmt.Sprintf(confATemplate, `["foo", "bar"]`)))
	srva, _ := RunServerWithConfig(confA)
	defer srva.Shutdown()

	confBTemplate := `
		port: -1
		cluster {
			listen: 127.0.0.1:-1
			routes = [
				"nats://127.0.0.1:%d"
			]
		}
		no_sys_acc: true
	`
	confB := createConfFile(t, []byte(fmt.Sprintf(confBTemplate, srva.ClusterAddr().Port)))
	srvb, _ := RunServerWithConfig(confB)
	defer srvb.Shutdown()

	checkClusterFormed(t, srva, srvb)

	// Create a connection on A
	nca, err := nats.Connect(fmt.Sprintf("nats://127.0.0.1:%d", srva.Addr().(*net.TCPAddr).Port))
	if err != nil {
		t.Fatalf("Error on connect: %v", err)
	}
	defer nca.Close()
	// Create a subscription on "foo" and "bar"
	if _, err := nca.SubscribeSync("foo"); err != nil {
		t.Fatalf("Error on subscribe: %v", err)
	}
	if _, err := nca.SubscribeSync("bar"); err != nil {
		t.Fatalf("Error on subscribe: %v", err)
	}

	checkExpectedSubs(t, 2, srva, srvb)

	// Drop foo
	reloadUpdateConfig(t, srva, confA, fmt.Sprintf(confATemplate, `"bar"`))
	checkExpectedSubs(t, 2, srva)
	checkExpectedSubs(t, 1, srvb)

	// Add it back
	reloadUpdateConfig(t, srva, confA, fmt.Sprintf(confATemplate, `["foo", "bar"]`))
	checkExpectedSubs(t, 2, srva, srvb)

	// Empty Import means implicit allow
	reloadUpdateConfig(t, srva, confA, `
		port: -1
		cluster {
			listen: 127.0.0.1:-1
			permissions {
				export: ">"
			}
		}
		no_sys_acc: true
	`)
	checkExpectedSubs(t, 2, srva, srvb)

	confATemplate = `
		port: -1
		cluster {
			listen: 127.0.0.1:-1
			permissions {
				import: {
					allow: ["foo", "bar"]
					deny: %s
				}
			}
		}
		no_sys_acc: true
	`
	// Now deny all:
	reloadUpdateConfig(t, srva, confA, fmt.Sprintf(confATemplate, `["foo", "bar"]`))
	checkExpectedSubs(t, 2, srva)
	checkExpectedSubs(t, 0, srvb)

	// Drop foo from the deny list
	reloadUpdateConfig(t, srva, confA, fmt.Sprintf(confATemplate, `"bar"`))
	checkExpectedSubs(t, 2, srva)
	checkExpectedSubs(t, 1, srvb)
}

func TestConfigReloadClusterPermsExport(t *testing.T) {
	confATemplate := `
		port: -1
		cluster {
			listen: 127.0.0.1:-1
			permissions {
				export: {
					allow: %s
				}
			}
		}
		no_sys_acc: true
	`
	confA := createConfFile(t, []byte(fmt.Sprintf(confATemplate, `["foo", "bar"]`)))
	srva, _ := RunServerWithConfig(confA)
	defer srva.Shutdown()

	confBTemplate := `
		port: -1
		cluster {
			listen: 127.0.0.1:-1
			routes = [
				"nats://127.0.0.1:%d"
			]
		}
		no_sys_acc: true
	`
	confB := createConfFile(t, []byte(fmt.Sprintf(confBTemplate, srva.ClusterAddr().Port)))
	srvb, _ := RunServerWithConfig(confB)
	defer srvb.Shutdown()

	checkClusterFormed(t, srva, srvb)

	// Create a connection on B
	ncb, err := nats.Connect(fmt.Sprintf("nats://127.0.0.1:%d", srvb.Addr().(*net.TCPAddr).Port))
	if err != nil {
		t.Fatalf("Error on connect: %v", err)
	}
	defer ncb.Close()
	// Create a subscription on "foo" and "bar"
	if _, err := ncb.SubscribeSync("foo"); err != nil {
		t.Fatalf("Error on subscribe: %v", err)
	}
	if _, err := ncb.SubscribeSync("bar"); err != nil {
		t.Fatalf("Error on subscribe: %v", err)
	}

	checkExpectedSubs(t, 2, srva, srvb)

	// Drop foo
	reloadUpdateConfig(t, srva, confA, fmt.Sprintf(confATemplate, `"bar"`))
	checkExpectedSubs(t, 2, srvb)
	checkExpectedSubs(t, 1, srva)

	// Add it back
	reloadUpdateConfig(t, srva, confA, fmt.Sprintf(confATemplate, `["foo", "bar"]`))
	checkExpectedSubs(t, 2, srva, srvb)

	// Empty Export means implicit allow
	reloadUpdateConfig(t, srva, confA, `
		port: -1
		cluster {
			listen: 127.0.0.1:-1
			permissions {
				import: ">"
			}
		}
		no_sys_acc: true
	`)
	checkExpectedSubs(t, 2, srva, srvb)

	confATemplate = `
		port: -1
		cluster {
			listen: 127.0.0.1:-1
			permissions {
				export: {
					allow: ["foo", "bar"]
					deny: %s
				}
			}
		}
		no_sys_acc: true
	`
	// Now deny all:
	reloadUpdateConfig(t, srva, confA, fmt.Sprintf(confATemplate, `["foo", "bar"]`))
	checkExpectedSubs(t, 0, srva)
	checkExpectedSubs(t, 2, srvb)

	// Drop foo from the deny list
	reloadUpdateConfig(t, srva, confA, fmt.Sprintf(confATemplate, `"bar"`))
	checkExpectedSubs(t, 1, srva)
	checkExpectedSubs(t, 2, srvb)
}

func TestConfigReloadClusterPermsOldServer(t *testing.T) {
	confATemplate := `
		port: -1
		cluster {
			listen: 127.0.0.1:-1
			permissions {
				export: {
					allow: %s
				}
			}
		}
	`
	confA := createConfFile(t, []byte(fmt.Sprintf(confATemplate, `["foo", "bar"]`)))
	srva, _ := RunServerWithConfig(confA)
	defer srva.Shutdown()

	optsB := DefaultOptions()
	optsB.Routes = RoutesFromStr(fmt.Sprintf("nats://127.0.0.1:%d", srva.ClusterAddr().Port))
	// Make server B behave like an old server
	optsB.overrideProto = setServerProtoForTest(RouteProtoZero)
	srvb := RunServer(optsB)
	defer srvb.Shutdown()

	checkClusterFormed(t, srva, srvb)

	// Get the route's connection ID
	getRouteRID := func() uint64 {
		rid := uint64(0)
		srvb.mu.Lock()
		if r := getFirstRoute(srvb); r != nil {
			r.mu.Lock()
			rid = r.cid
			r.mu.Unlock()
		}
		srvb.mu.Unlock()
		return rid
	}
	orgRID := getRouteRID()

	// Cause a config reload on A
	reloadUpdateConfig(t, srva, confA, fmt.Sprintf(confATemplate, `"bar"`))

	// Check that new route gets created
	check := func(t *testing.T) {
		t.Helper()
		checkFor(t, 3*time.Second, 15*time.Millisecond, func() error {
			if rid := getRouteRID(); rid == orgRID {
				return fmt.Errorf("Route does not seem to have been recreated")
			}
			return nil
		})
	}
	check(t)

	// Save the current value
	orgRID = getRouteRID()

	// Add another server that supports INFO updates

	optsC := DefaultOptions()
	optsC.Routes = RoutesFromStr(fmt.Sprintf("nats://127.0.0.1:%d", srva.ClusterAddr().Port))
	srvc := RunServer(optsC)
	defer srvc.Shutdown()

	checkClusterFormed(t, srva, srvb, srvc)

	// Cause a config reload on A
	reloadUpdateConfig(t, srva, confA, fmt.Sprintf(confATemplate, `"foo"`))
	// Check that new route gets created
	check(t)
}

func TestConfigReloadAccountUsers(t *testing.T) {
	conf := createConfFile(t, []byte(`
	listen: "127.0.0.1:-1"
	accounts {
		synadia {
			users = [
				{user: derek, password: derek}
				{user: foo, password: foo}
			]
		}
		nats.io {
			users = [
				{user: ivan, password: ivan}
				{user: bar, password: bar}
			]
		}
		acc_deleted_after_reload {
			users = [
				{user: gone, password: soon}
				{user: baz, password: baz}
				{user: bat, password: bat}
			]
		}
	}
	`))
	s, opts := RunServerWithConfig(conf)
	defer s.Shutdown()

	// Connect as exisiting users, should work.
	nc, err := nats.Connect(fmt.Sprintf("nats://derek:derek@%s:%d", opts.Host, opts.Port))
	if err != nil {
		t.Fatalf("Error on connect: %v", err)
	}
	defer nc.Close()
	ch := make(chan bool, 2)
	cb := func(_ *nats.Conn) {
		ch <- true
	}
	nc2, err := nats.Connect(
		fmt.Sprintf("nats://ivan:ivan@%s:%d", opts.Host, opts.Port),
		nats.NoReconnect(),
		nats.ClosedHandler(cb))
	if err != nil {
		t.Fatalf("Error on connect: %v", err)
	}
	defer nc2.Close()
	nc3, err := nats.Connect(
		fmt.Sprintf("nats://gone:soon@%s:%d", opts.Host, opts.Port),
		nats.NoReconnect(),
		nats.ClosedHandler(cb))
	if err != nil {
		t.Fatalf("Error on connect: %v", err)
	}
	defer nc3.Close()
	// These users will be moved from an account to another (to a specific or to global account)
	// We will create subscriptions to ensure that they are moved to proper sublists too.
	rch := make(chan bool, 4)
	rcb := func(_ *nats.Conn) {
		rch <- true
	}
	nc4, err := nats.Connect(fmt.Sprintf("nats://foo:foo@%s:%d", opts.Host, opts.Port),
		nats.ReconnectWait(50*time.Millisecond), nats.ReconnectHandler(rcb))
	if err != nil {
		t.Fatalf("Error on connect: %v", err)
	}
	defer nc4.Close()
	if _, err := nc4.SubscribeSync("foo"); err != nil {
		t.Fatalf("Error on subscribe: %v", err)
	}
	nc5, err := nats.Connect(fmt.Sprintf("nats://bar:bar@%s:%d", opts.Host, opts.Port),
		nats.ReconnectWait(50*time.Millisecond), nats.ReconnectHandler(rcb))
	if err != nil {
		t.Fatalf("Error on connect: %v", err)
	}
	defer nc5.Close()
	if _, err := nc5.SubscribeSync("bar"); err != nil {
		t.Fatalf("Error on subscribe: %v", err)
	}
	nc6, err := nats.Connect(fmt.Sprintf("nats://baz:baz@%s:%d", opts.Host, opts.Port),
		nats.ReconnectWait(50*time.Millisecond), nats.ReconnectHandler(rcb))
	if err != nil {
		t.Fatalf("Error on connect: %v", err)
	}
	defer nc6.Close()
	if _, err := nc6.SubscribeSync("baz"); err != nil {
		t.Fatalf("Error on subscribe: %v", err)
	}
	nc7, err := nats.Connect(fmt.Sprintf("nats://bat:bat@%s:%d", opts.Host, opts.Port),
		nats.ReconnectWait(50*time.Millisecond), nats.ReconnectHandler(rcb))
	if err != nil {
		t.Fatalf("Error on connect: %v", err)
	}
	defer nc7.Close()
	if _, err := nc7.SubscribeSync("bat"); err != nil {
		t.Fatalf("Error on subscribe: %v", err)
	}

	// confirm subscriptions before and after reload.
	var expectedSubs uint32 = 5
	sAcc, err := s.LookupAccount("synadia")
	require_NoError(t, err)
	sAcc.mu.RLock()
	n := sAcc.sl.Count()
	sAcc.mu.RUnlock()
	if n != expectedSubs {
		t.Errorf("Synadia account should have %d sub, got %v", expectedSubs, n)
	}
	nAcc, err := s.LookupAccount("nats.io")
	require_NoError(t, err)
	nAcc.mu.RLock()
	n = nAcc.sl.Count()
	nAcc.mu.RUnlock()
	if n != expectedSubs {
		t.Errorf("Nats.io account should have %d sub, got %v", expectedSubs, n)
	}

	// Remove user from account and whole account
	reloadUpdateConfig(t, s, conf, `
	listen: "127.0.0.1:-1"
	authorization {
		users = [
			{user: foo, password: foo}
			{user: baz, password: baz}
		]
	}
	accounts {
		synadia {
			users = [
				{user: derek, password: derek}
				{user: bar, password: bar}
			]
		}
		nats.io {
			users = [
				{user: bat, password: bat}
			]
		}
	}
	`)
	// nc2 and nc3 should be closed
	if err := wait(ch); err != nil {
		t.Fatal("Did not get the closed callback")
	}
	if err := wait(ch); err != nil {
		t.Fatal("Did not get the closed callback")
	}
	// And first connection should still be connected
	if !nc.IsConnected() {
		t.Fatal("First connection should still be connected")
	}

	// Old account should be gone
	if _, err := s.LookupAccount("acc_deleted_after_reload"); err == nil {
		t.Fatal("old account should be gone")
	}

	// Check subscriptions. Since most of the users have been
	// moving accounts, make sure we account for the reconnect
	for i := 0; i < 4; i++ {
		if err := wait(rch); err != nil {
			t.Fatal("Did not get the reconnect cb")
		}
	}
	// Still need to do the tests in a checkFor() because clients
	// being reconnected does not mean that resent of subscriptions
	// has already been processed.
	checkFor(t, 2*time.Second, 100*time.Millisecond, func() error {
		gAcc, err := s.LookupAccount(globalAccountName)
		require_NoError(t, err)
		gAcc.mu.RLock()
		n := gAcc.sl.Count()
		fooMatch := gAcc.sl.Match("foo")
		bazMatch := gAcc.sl.Match("baz")
		gAcc.mu.RUnlock()
		// The number of subscriptions should be 4 ($SYS.REQ.USER.INFO,
		// $SYS.REQ.ACCOUNT.PING.CONNZ, $SYS.REQ.ACCOUNT.PING.STATZ,
		// $SYS.REQ.SERVER.PING.CONNZ) + 2 (foo and baz)
		if n != 6 {
			return fmt.Errorf("Global account should have 6 subs, got %v", n)
		}
		if len(fooMatch.psubs) != 1 {
			return fmt.Errorf("Global account should have foo sub")
		}
		if len(bazMatch.psubs) != 1 {
			return fmt.Errorf("Global account should have baz sub")
		}

		sAcc, err := s.LookupAccount("synadia")
		require_NoError(t, err)
		sAcc.mu.RLock()
		n = sAcc.sl.Count()
		barMatch := sAcc.sl.Match("bar")

		sAcc.mu.RUnlock()
		if n != expectedSubs {
			return fmt.Errorf("Synadia account should have %d sub, got %v", expectedSubs, n)
		}
		if len(barMatch.psubs) != 1 {
			return fmt.Errorf("Synadia account should have bar sub")
		}

		nAcc, err := s.LookupAccount("nats.io")
		require_NoError(t, err)
		nAcc.mu.RLock()
		n = nAcc.sl.Count()
		batMatch := nAcc.sl.Match("bat")
		nAcc.mu.RUnlock()
		if n != expectedSubs {
			return fmt.Errorf("Nats.io account should have %d sub, got %v", expectedSubs, n)
		}
		if len(batMatch.psubs) != 1 {
			return fmt.Errorf("Synadia account should have bar sub")
		}
		return nil
	})
}

func TestConfigReloadAccountWithNoChanges(t *testing.T) {
	conf := createConfFile(t, []byte(`
	listen: "127.0.0.1:-1"
        system_account: sys
	accounts {
		A {
			users = [{ user: a }]
		}
		B {
			users = [{ user: b }]
		}
		C {
			users = [{ user: c }]
		}
		sys {
			users = [{ user: sys }]
		}
	}
	`))
	s, opts := RunServerWithConfig(conf)
	defer s.Shutdown()

	ncA, err := nats.Connect(fmt.Sprintf("nats://a:@%s:%d", opts.Host, opts.Port))
	if err != nil {
		t.Fatalf("Error on connect: %v", err)
	}
	defer ncA.Close()

	// Confirm default service imports are ok.
	checkSubs := func(t *testing.T) {
		resp, err := ncA.Request("$SYS.REQ.ACCOUNT.PING.CONNZ", nil, time.Second)
		if err != nil {
			t.Error(err)
		}
		if resp == nil || !strings.Contains(string(resp.Data), `"num_connections":1`) {
			t.Fatal("unexpected data in connz response")
		}
		resp, err = ncA.Request("$SYS.REQ.SERVER.PING.CONNZ", nil, time.Second)
		if err != nil {
			t.Error(err)
		}
		if resp == nil || !strings.Contains(string(resp.Data), `"num_connections":1`) {
			t.Fatal("unexpected data in connz response")
		}
		resp, err = ncA.Request("$SYS.REQ.ACCOUNT.PING.STATZ", nil, time.Second)
		if err != nil {
			t.Error(err)
		}
		if resp == nil || !strings.Contains(string(resp.Data), `"conns":1`) {
			t.Fatal("unexpected data in connz response")
		}
	}
	checkSubs(t)
	before := s.NumSubscriptions()
	s.Reload()
	after := s.NumSubscriptions()
	if before != after {
		t.Errorf("Number of subscriptions changed after reload: %d -> %d", before, after)
	}

	// Confirm this still works after a reload...
	checkSubs(t)
	before = s.NumSubscriptions()
	s.Reload()
	after = s.NumSubscriptions()
	if before != after {
		t.Errorf("Number of subscriptions changed after reload: %d -> %d", before, after)
	}

	// Do another extra reload just in case.
	checkSubs(t)
	before = s.NumSubscriptions()
	s.Reload()
	after = s.NumSubscriptions()
	if before != after {
		t.Errorf("Number of subscriptions changed after reload: %d -> %d", before, after)
	}
}

func TestConfigReloadAccountNKeyUsers(t *testing.T) {
	conf := createConfFile(t, []byte(`
	listen: "127.0.0.1:-1"
	accounts {
		synadia {
			users = [
				# Derek
				{nkey : UCNGL4W5QX66CFX6A6DCBVDH5VOHMI7B2UZZU7TXAUQQSI2JPHULCKBR}
			]
		}
		nats.io {
			users = [
				# Ivan
				{nkey : UDPGQVFIWZ7Q5UH4I5E6DBCZULQS6VTVBG6CYBD7JV3G3N2GMQOMNAUH}
			]
		}
	}
	`))
	s, _ := RunServerWithConfig(conf)
	defer s.Shutdown()

	synadia, _ := s.LookupAccount("synadia")
	nats, _ := s.LookupAccount("nats.io")

	seed1 := []byte("SUAPM67TC4RHQLKBX55NIQXSMATZDOZK6FNEOSS36CAYA7F7TY66LP4BOM")
	seed2 := []byte("SUAIS5JPX4X4GJ7EIIJEQ56DH2GWPYJRPWN5XJEDENJOZHCBLI7SEPUQDE")

	kp, _ := nkeys.FromSeed(seed1)
	pubKey, _ := kp.PublicKey()

	c, cr, l := newClientForServer(s)
	defer c.close()
	// Check for Nonce
	var info nonceInfo
	if err := json.Unmarshal([]byte(l[5:]), &info); err != nil {
		t.Fatalf("Could not parse INFO json: %v\n", err)
	}
	if info.Nonce == "" {
		t.Fatalf("Expected a non-empty nonce with nkeys defined")
	}
	sigraw, err := kp.Sign([]byte(info.Nonce))
	if err != nil {
		t.Fatalf("Failed signing nonce: %v", err)
	}
	sig := base64.RawURLEncoding.EncodeToString(sigraw)

	// PING needed to flush the +OK to us.
	cs := fmt.Sprintf("CONNECT {\"nkey\":%q,\"sig\":\"%s\",\"verbose\":true,\"pedantic\":true}\r\nPING\r\n", pubKey, sig)
	c.parseAsync(cs)
	l, _ = cr.ReadString('\n')
	if !strings.HasPrefix(l, "+OK") {
		t.Fatalf("Expected an OK, got: %v", l)
	}
	if c.acc != synadia {
		t.Fatalf("Expected the nkey client's account to match 'synadia', got %v", c.acc)
	}

	// Now nats account nkey user.
	kp, _ = nkeys.FromSeed(seed2)
	pubKey, _ = kp.PublicKey()

	c, cr, l = newClientForServer(s)
	defer c.close()
	// Check for Nonce
	err = json.Unmarshal([]byte(l[5:]), &info)
	if err != nil {
		t.Fatalf("Could not parse INFO json: %v\n", err)
	}
	if info.Nonce == "" {
		t.Fatalf("Expected a non-empty nonce with nkeys defined")
	}
	sigraw, err = kp.Sign([]byte(info.Nonce))
	if err != nil {
		t.Fatalf("Failed signing nonce: %v", err)
	}
	sig = base64.RawURLEncoding.EncodeToString(sigraw)

	// PING needed to flush the +OK to us.
	cs = fmt.Sprintf("CONNECT {\"nkey\":%q,\"sig\":\"%s\",\"verbose\":true,\"pedantic\":true}\r\nPING\r\n", pubKey, sig)
	c.parseAsync(cs)
	l, _ = cr.ReadString('\n')
	if !strings.HasPrefix(l, "+OK") {
		t.Fatalf("Expected an OK, got: %v", l)
	}
	if c.acc != nats {
		t.Fatalf("Expected the nkey client's account to match 'nats', got %v", c.acc)
	}

	// Remove user from account and whole account
	reloadUpdateConfig(t, s, conf, `
	listen: "127.0.0.1:-1"
	authorization {
		users = [
			# Ivan
			{nkey : UDPGQVFIWZ7Q5UH4I5E6DBCZULQS6VTVBG6CYBD7JV3G3N2GMQOMNAUH}
		]
	}
	accounts {
		nats.io {
			users = [
				# Derek
				{nkey : UCNGL4W5QX66CFX6A6DCBVDH5VOHMI7B2UZZU7TXAUQQSI2JPHULCKBR}
			]
		}
	}
	`)

	s.mu.Lock()
	nkeys := s.nkeys
	globalAcc := s.gacc
	s.mu.Unlock()

	if n := len(nkeys); n != 2 {
		t.Fatalf("NKeys map should have 2 users, got %v", n)
	}
	derek := nkeys["UCNGL4W5QX66CFX6A6DCBVDH5VOHMI7B2UZZU7TXAUQQSI2JPHULCKBR"]
	if derek == nil {
		t.Fatal("NKey for user Derek not found")
	}
	if derek.Account == nil || derek.Account.Name != "nats.io" {
		t.Fatalf("Invalid account for user Derek: %#v", derek.Account)
	}
	ivan := nkeys["UDPGQVFIWZ7Q5UH4I5E6DBCZULQS6VTVBG6CYBD7JV3G3N2GMQOMNAUH"]
	if ivan == nil {
		t.Fatal("NKey for user Ivan not found")
	}
	if ivan.Account != globalAcc {
		t.Fatalf("Invalid account for user Ivan: %#v", ivan.Account)
	}
	if _, err := s.LookupAccount("synadia"); err == nil {
		t.Fatal("Account Synadia should have been removed")
	}
}

func TestConfigReloadAccountStreamsImportExport(t *testing.T) {
	template := `
	listen: "127.0.0.1:-1"
	accounts {
		synadia {
			users [{user: derek, password: foo}]
			exports = [
				{stream: "private.>", accounts: [nats.io]}
				{stream: %s}
			]
		}
		nats.io {
			users [
				{user: ivan, password: bar, permissions: {subscribe: {deny: %s}}}
			]
			imports = [
				{stream: {account: "synadia", subject: %s}}
				{stream: {account: "synadia", subject: "private.natsio.*"}, prefix: %s}
			]
		}
	}
	no_sys_acc: true
	`
	// synadia account exports "private.>" to nats.io
	// synadia account exports "foo.*"
	// user ivan denies subscription on "xxx"
	// nats.io account imports "foo.*" from synadia
	// nats.io account imports "private.natsio.*" from synadia with prefix "ivan"
	conf := createConfFile(t, []byte(fmt.Sprintf(template, `"foo.*"`, `"xxx"`, `"foo.*"`, `"ivan"`)))
	s, opts := RunServerWithConfig(conf)
	defer s.Shutdown()

	derek, err := nats.Connect(fmt.Sprintf("nats://derek:foo@%s:%d", opts.Host, opts.Port))
	if err != nil {
		t.Fatalf("Error on connect: %v", err)
	}
	defer derek.Close()
	checkClientsCount(t, s, 1)

	ch := make(chan bool, 1)
	ivan, err := nats.Connect(fmt.Sprintf("nats://ivan:bar@%s:%d", opts.Host, opts.Port),
		nats.ErrorHandler(func(_ *nats.Conn, _ *nats.Subscription, err error) {
			if strings.Contains(strings.ToLower(err.Error()), "permissions violation") {
				ch <- true
			}
		}))
	if err != nil {
		t.Fatalf("Error on connect: %v", err)
	}
	defer ivan.Close()
	checkClientsCount(t, s, 2)

	subscribe := func(t *testing.T, nc *nats.Conn, subj string) *nats.Subscription {
		t.Helper()
		s, err := nc.SubscribeSync(subj)
		if err != nil {
			t.Fatalf("Error on subscribe: %v", err)
		}
		return s
	}

	subFooBar := subscribe(t, ivan, "foo.bar")
	subFooBaz := subscribe(t, ivan, "foo.baz")
	subFooBat := subscribe(t, ivan, "foo.bat")
	subPriv := subscribe(t, ivan, "ivan.private.natsio.*")
	ivan.Flush()

	publish := func(t *testing.T, nc *nats.Conn, subj string) {
		t.Helper()
		if err := nc.Publish(subj, []byte("hello")); err != nil {
			t.Fatalf("Error on publish: %v", err)
		}
	}

	nextMsg := func(t *testing.T, sub *nats.Subscription, expected bool) {
		t.Helper()
		dur := 100 * time.Millisecond
		if expected {
			dur = time.Second
		}
		_, err := sub.NextMsg(dur)
		if expected && err != nil {
			t.Fatalf("Expected a message on %s, got %v", sub.Subject, err)
		} else if !expected && err != nats.ErrTimeout {
			t.Fatalf("Expected a timeout on %s, got %v", sub.Subject, err)
		}
	}

	// Checks the derek's user sublist for presence of given subject
	// interest. Boolean says if interest is expected or not.
	checkSublist := func(t *testing.T, subject string, shouldBeThere bool) {
		t.Helper()
		dcli := s.getClient(1)
		dcli.mu.Lock()
		r := dcli.acc.sl.Match(subject)
		dcli.mu.Unlock()
		if shouldBeThere && len(r.psubs) != 1 {
			t.Fatalf("%s should have 1 match in derek's sublist, got %v", subject, len(r.psubs))
		} else if !shouldBeThere && len(r.psubs) > 0 {
			t.Fatalf("%s should not be in derek's sublist", subject)
		}
	}

	// Publish on all subjects and the subs should receive and
	// subjects should be in sublist
	publish(t, derek, "foo.bar")
	nextMsg(t, subFooBar, true)
	checkSublist(t, "foo.bar", true)

	publish(t, derek, "foo.baz")
	nextMsg(t, subFooBaz, true)
	checkSublist(t, "foo.baz", true)

	publish(t, derek, "foo.bat")
	nextMsg(t, subFooBat, true)
	checkSublist(t, "foo.bat", true)

	publish(t, derek, "private.natsio.foo")
	nextMsg(t, subPriv, true)
	checkSublist(t, "private.natsio.foo", true)

	// Also make sure that intra-account subscription works OK
	ivanSub := subscribe(t, ivan, "ivan.sub")
	publish(t, ivan, "ivan.sub")
	nextMsg(t, ivanSub, true)
	derekSub := subscribe(t, derek, "derek.sub")
	publish(t, derek, "derek.sub")
	nextMsg(t, derekSub, true)

	// synadia account exports "private.>" to nats.io
	// synadia account exports "foo.*"
	// user ivan denies subscription on "foo.bat"
	// nats.io account imports "foo.baz" from synadia
	// nats.io account imports "private.natsio.*" from synadia with prefix "yyyy"
	reloadUpdateConfig(t, s, conf, fmt.Sprintf(template, `"foo.*"`, `"foo.bat"`, `"foo.baz"`, `"yyyy"`))

	// Sub on foo.bar should now fail to receive
	publish(t, derek, "foo.bar")
	nextMsg(t, subFooBar, false)
	checkSublist(t, "foo.bar", false)
	// But foo.baz should be received
	publish(t, derek, "foo.baz")
	nextMsg(t, subFooBaz, true)
	checkSublist(t, "foo.baz", true)
	// Due to permissions, foo.bat should not
	publish(t, derek, "foo.bat")
	nextMsg(t, subFooBat, false)
	checkSublist(t, "foo.bat", false)
	// Prefix changed, so should not be received
	publish(t, derek, "private.natsio.foo")
	nextMsg(t, subPriv, false)
	checkSublist(t, "private.natsio.foo", false)

	// Wait for client notification of permissions error
	if err := wait(ch); err != nil {
		t.Fatal("Did not the permissions error")
	}

	publish(t, ivan, "ivan.sub")
	nextMsg(t, ivanSub, true)
	publish(t, derek, "derek.sub")
	nextMsg(t, derekSub, true)

	// Change export so that foo.* is no longer exported
	// synadia account exports "private.>" to nats.io
	// synadia account exports "xxx"
	// user ivan denies subscription on "foo.bat"
	// nats.io account imports "xxx" from synadia
	// nats.io account imports "private.natsio.*" from synadia with prefix "ivan"
	reloadUpdateConfig(t, s, conf, fmt.Sprintf(template, `"xxx"`, `"foo.bat"`, `"xxx"`, `"ivan"`))

	publish(t, derek, "foo.bar")
	nextMsg(t, subFooBar, false)
	checkSublist(t, "foo.bar", false)

	publish(t, derek, "foo.baz")
	nextMsg(t, subFooBaz, false)
	checkSublist(t, "foo.baz", false)

	publish(t, derek, "foo.bat")
	nextMsg(t, subFooBat, false)
	checkSublist(t, "foo.bat", false)

	// Prefix changed back, so should receive
	publish(t, derek, "private.natsio.foo")
	nextMsg(t, subPriv, true)
	checkSublist(t, "private.natsio.foo", true)

	publish(t, ivan, "ivan.sub")
	nextMsg(t, ivanSub, true)
	publish(t, derek, "derek.sub")
	nextMsg(t, derekSub, true)
}

func TestConfigReloadAccountServicesImportExport(t *testing.T) {
	conf := createConfFile(t, []byte(`
	listen: "127.0.0.1:-1"
	accounts {
		synadia {
			users [{user: derek, password: foo}]
			exports = [
				{service: "pub.request"}
				{service: "pub.special.request", accounts: [nats.io]}
			]
		}
		nats.io {
			users [{user: ivan, password: bar}]
			imports = [
				{service: {account: "synadia", subject: "pub.special.request"}, to: "foo"}
				{service: {account: "synadia", subject: "pub.request"}, to: "bar"}
			]
		}
	}
	cluster {
		name: "abc"
		port: -1
	}
	`))
	s, opts := RunServerWithConfig(conf)
	defer s.Shutdown()

	opts2 := DefaultOptions()
	opts2.Cluster.Name = "abc"
	opts2.Routes = RoutesFromStr(fmt.Sprintf("nats://127.0.0.1:%d", opts.Cluster.Port))
	s2 := RunServer(opts2)
	defer s2.Shutdown()

	checkClusterFormed(t, s, s2)

	derek, err := nats.Connect(fmt.Sprintf("nats://derek:foo@%s:%d", opts.Host, opts.Port))
	if err != nil {
		t.Fatalf("Error on connect: %v", err)
	}
	defer derek.Close()
	checkClientsCount(t, s, 1)

	ivan, err := nats.Connect(fmt.Sprintf("nats://ivan:bar@%s:%d", opts.Host, opts.Port))
	if err != nil {
		t.Fatalf("Error on connect: %v", err)
	}
	defer ivan.Close()
	checkClientsCount(t, s, 2)

	if _, err := derek.Subscribe("pub.special.request", func(m *nats.Msg) {
		derek.Publish(m.Reply, []byte("reply1"))
	}); err != nil {
		t.Fatalf("Error on subscribe: %v", err)
	}
	if _, err := derek.Subscribe("pub.request", func(m *nats.Msg) {
		derek.Publish(m.Reply, []byte("reply2"))
	}); err != nil {
		t.Fatalf("Error on subscribe: %v", err)
	}
	if _, err := derek.Subscribe("pub.special.request.new", func(m *nats.Msg) {
		derek.Publish(m.Reply, []byte("reply3"))
	}); err != nil {
		t.Fatalf("Error on subscribe: %v", err)
	}
	// Also create one that will be used for intra-account communication
	if _, err := derek.Subscribe("derek.sub", func(m *nats.Msg) {
		derek.Publish(m.Reply, []byte("private"))
	}); err != nil {
		t.Fatalf("Error on subscribe: %v", err)
	}
	derek.Flush()

	// Create an intra-account sub for ivan too
	if _, err := ivan.Subscribe("ivan.sub", func(m *nats.Msg) {
		ivan.Publish(m.Reply, []byte("private"))
	}); err != nil {
		t.Fatalf("Error on subscribe: %v", err)
	}
	// This subscription is just to make sure that we can update
	// route map without locking issues during reload.
	natsSubSync(t, ivan, "bar")

	req := func(t *testing.T, nc *nats.Conn, subj string, reply string) {
		t.Helper()
		var timeout time.Duration
		if reply != "" {
			timeout = time.Second
		} else {
			timeout = 100 * time.Millisecond
		}
		msg, err := nc.Request(subj, []byte("request"), timeout)
		if reply != "" {
			if err != nil {
				t.Fatalf("Expected reply %s on subject %s, got %v", reply, subj, err)
			}
			if string(msg.Data) != reply {
				t.Fatalf("Expected reply %s on subject %s, got %s", reply, subj, msg.Data)
			}
		} else if err != nats.ErrTimeout && err != nats.ErrNoResponders {
			t.Fatalf("Expected timeout on subject %s, got %v", subj, err)
		}
	}

	req(t, ivan, "foo", "reply1")
	req(t, ivan, "bar", "reply2")
	// This not exported/imported, so should timeout
	req(t, ivan, "baz", "")

	// Check intra-account communication
	req(t, ivan, "ivan.sub", "private")
	req(t, derek, "derek.sub", "private")

	reloadUpdateConfig(t, s, conf, `
	listen: "127.0.0.1:-1"
	accounts {
		synadia {
			users [{user: derek, password: foo}]
			exports = [
				{service: "pub.request"}
				{service: "pub.special.request", accounts: [nats.io]}
				{service: "pub.special.request.new", accounts: [nats.io]}
			]
		}
		nats.io {
			users [{user: ivan, password: bar}]
			imports = [
				{service: {account: "synadia", subject: "pub.special.request"}, to: "foo"}
				{service: {account: "synadia", subject: "pub.special.request.new"}, to: "baz"}
			]
		}
	}
	cluster {
		name: "abc"
		port: -1
	}
	`)
	// This still should work
	req(t, ivan, "foo", "reply1")
	// This should not
	req(t, ivan, "bar", "")
	// This now should work
	req(t, ivan, "baz", "reply3")

	// Check intra-account communication
	req(t, ivan, "ivan.sub", "private")
	req(t, derek, "derek.sub", "private")
}

// As of now, config reload does not support changes for gateways.
// However, ensure that if a gateway is defined, one can still
// do reload as long as we don't change the gateway spec.
func TestConfigReloadNotPreventedByGateways(t *testing.T) {
	confTemplate := `
		listen: "127.0.0.1:-1"
		%s
		gateway {
			name: "A"
			listen: "127.0.0.1:-1"
			tls {
				cert_file: "configs/certs/server.pem"
				key_file: "configs/certs/key.pem"
				timeout: %s
			}
			gateways [
				{
					name: "B"
					url: "nats://localhost:8888"
				}
			]
		}
		no_sys_acc: true
	`
	conf := createConfFile(t, []byte(fmt.Sprintf(confTemplate, "", "5")))
	s, _ := RunServerWithConfig(conf)
	defer s.Shutdown()

	// Cause reload with adding a param that is supported
	reloadUpdateConfig(t, s, conf, fmt.Sprintf(confTemplate, "max_payload: 100000", "5"))

	// Now update gateway, should fail to reload.
	changeCurrentConfigContentWithNewContent(t, conf, []byte(fmt.Sprintf(confTemplate, "max_payload: 100000", "3")))
	if err := s.Reload(); err == nil || !strings.Contains(err.Error(), "not supported for Gateway") {
		t.Fatalf("Expected Reload to return a not supported error, got %v", err)
	}
}

func TestConfigReloadBoolFlags(t *testing.T) {
	defer func() { FlagSnapshot = nil }()

	logfile := filepath.Join(t.TempDir(), "logtime.log")
	template := `
		listen: "127.0.0.1:-1"
		logfile: "%s"
		%s
	`

	var opts *Options
	var err error

	for _, test := range []struct {
		name     string
		content  string
		cmdLine  []string
		expected bool
		val      func() bool
	}{
		// Logtime
		{
			"logtime_not_in_config_no_override",
			"",
			nil,
			true,
			func() bool { return opts.Logtime },
		},
		{
			"logtime_not_in_config_override_short_true",
			"",
			[]string{"-T"},
			true,
			func() bool { return opts.Logtime },
		},
		{
			"logtime_not_in_config_override_true",
			"",
			[]string{"-logtime"},
			true,
			func() bool { return opts.Logtime },
		},
		{
			"logtime_false_in_config_no_override",
			"logtime: false",
			nil,
			false,
			func() bool { return opts.Logtime },
		},
		{
			"logtime_false_in_config_override_short_true",
			"logtime: false",
			[]string{"-T"},
			true,
			func() bool { return opts.Logtime },
		},
		{
			"logtime_false_in_config_override_true",
			"logtime: false",
			[]string{"-logtime"},
			true,
			func() bool { return opts.Logtime },
		},
		{
			"logtime_true_in_config_no_override",
			"logtime: true",
			nil,
			true,
			func() bool { return opts.Logtime },
		},
		{
			"logtime_true_in_config_override_short_false",
			"logtime: true",
			[]string{"-T=false"},
			false,
			func() bool { return opts.Logtime },
		},
		{
			"logtime_true_in_config_override_false",
			"logtime: true",
			[]string{"-logtime=false"},
			false,
			func() bool { return opts.Logtime },
		},
		// Debug
		{
			"debug_not_in_config_no_override",
			"",
			nil,
			false,
			func() bool { return opts.Debug },
		},
		{
			"debug_not_in_config_override_short_true",
			"",
			[]string{"-D"},
			true,
			func() bool { return opts.Debug },
		},
		{
			"debug_not_in_config_override_true",
			"",
			[]string{"-debug"},
			true,
			func() bool { return opts.Debug },
		},
		{
			"debug_false_in_config_no_override",
			"debug: false",
			nil,
			false,
			func() bool { return opts.Debug },
		},
		{
			"debug_false_in_config_override_short_true",
			"debug: false",
			[]string{"-D"},
			true,
			func() bool { return opts.Debug },
		},
		{
			"debug_false_in_config_override_true",
			"debug: false",
			[]string{"-debug"},
			true,
			func() bool { return opts.Debug },
		},
		{
			"debug_true_in_config_no_override",
			"debug: true",
			nil,
			true,
			func() bool { return opts.Debug },
		},
		{
			"debug_true_in_config_override_short_false",
			"debug: true",
			[]string{"-D=false"},
			false,
			func() bool { return opts.Debug },
		},
		{
			"debug_true_in_config_override_false",
			"debug: true",
			[]string{"-debug=false"},
			false,
			func() bool { return opts.Debug },
		},
		// Trace
		{
			"trace_not_in_config_no_override",
			"",
			nil,
			false,
			func() bool { return opts.Trace },
		},
		{
			"trace_not_in_config_override_short_true",
			"",
			[]string{"-V"},
			true,
			func() bool { return opts.Trace },
		},
		{
			"trace_not_in_config_override_true",
			"",
			[]string{"-trace"},
			true,
			func() bool { return opts.Trace },
		},
		{
			"trace_false_in_config_no_override",
			"trace: false",
			nil,
			false,
			func() bool { return opts.Trace },
		},
		{
			"trace_false_in_config_override_short_true",
			"trace: false",
			[]string{"-V"},
			true,
			func() bool { return opts.Trace },
		},
		{
			"trace_false_in_config_override_true",
			"trace: false",
			[]string{"-trace"},
			true,
			func() bool { return opts.Trace },
		},
		{
			"trace_true_in_config_no_override",
			"trace: true",
			nil,
			true,
			func() bool { return opts.Trace },
		},
		{
			"trace_true_in_config_override_short_false",
			"trace: true",
			[]string{"-V=false"},
			false,
			func() bool { return opts.Trace },
		},
		{
			"trace_true_in_config_override_false",
			"trace: true",
			[]string{"-trace=false"},
			false,
			func() bool { return opts.Trace },
		},
		// Syslog
		{
			"syslog_not_in_config_no_override",
			"",
			nil,
			false,
			func() bool { return opts.Syslog },
		},
		{
			"syslog_not_in_config_override_short_true",
			"",
			[]string{"-s"},
			true,
			func() bool { return opts.Syslog },
		},
		{
			"syslog_not_in_config_override_true",
			"",
			[]string{"-syslog"},
			true,
			func() bool { return opts.Syslog },
		},
		{
			"syslog_false_in_config_no_override",
			"syslog: false",
			nil,
			false,
			func() bool { return opts.Syslog },
		},
		{
			"syslog_false_in_config_override_short_true",
			"syslog: false",
			[]string{"-s"},
			true,
			func() bool { return opts.Syslog },
		},
		{
			"syslog_false_in_config_override_true",
			"syslog: false",
			[]string{"-syslog"},
			true,
			func() bool { return opts.Syslog },
		},
		{
			"syslog_true_in_config_no_override",
			"syslog: true",
			nil,
			true,
			func() bool { return opts.Syslog },
		},
		{
			"syslog_true_in_config_override_short_false",
			"syslog: true",
			[]string{"-s=false"},
			false,
			func() bool { return opts.Syslog },
		},
		{
			"syslog_true_in_config_override_false",
			"syslog: true",
			[]string{"-syslog=false"},
			false,
			func() bool { return opts.Syslog },
		},
		// Cluster.NoAdvertise
		{
			"cluster_no_advertise_not_in_config_no_override",
			`cluster {
				port: -1
			}`,
			nil,
			false,
			func() bool { return opts.Cluster.NoAdvertise },
		},
		{
			"cluster_no_advertise_not_in_config_override_true",
			`cluster {
				port: -1
			}`,
			[]string{"-no_advertise"},
			true,
			func() bool { return opts.Cluster.NoAdvertise },
		},
		{
			"cluster_no_advertise_false_in_config_no_override",
			`cluster {
				port: -1
				no_advertise: false
			}`,
			nil,
			false,
			func() bool { return opts.Cluster.NoAdvertise },
		},
		{
			"cluster_no_advertise_false_in_config_override_true",
			`cluster {
				port: -1
				no_advertise: false
			}`,
			[]string{"-no_advertise"},
			true,
			func() bool { return opts.Cluster.NoAdvertise },
		},
		{
			"cluster_no_advertise_true_in_config_no_override",
			`cluster {
				port: -1
				no_advertise: true
			}`,
			nil,
			true,
			func() bool { return opts.Cluster.NoAdvertise },
		},
		{
			"cluster_no_advertise_true_in_config_override_false",
			`cluster {
				port: -1
				no_advertise: true
			}`,
			[]string{"-no_advertise=false"},
			false,
			func() bool { return opts.Syslog },
		},
		// -DV override
		{
			"debug_trace_not_in_config_dv_override_true",
			"",
			[]string{"-DV"},
			true,
			func() bool { return opts.Debug && opts.Trace },
		},
		{
			"debug_trace_false_in_config_dv_override_true",
			`debug: false
		     trace: false
			`,
			[]string{"-DV"},
			true,
			func() bool { return opts.Debug && opts.Trace },
		},
		{
			"debug_trace_true_in_config_dv_override_false",
			`debug: true
		     trace: true
			`,
			[]string{"-DV=false"},
			false,
			func() bool { return opts.Debug && opts.Trace },
		},
		{
			"trace_verbose_true_in_config_override_true",
			`trace_verbose: true
			`,
			nil,
			true,
			func() bool { return opts.Trace && opts.TraceVerbose },
		},
		{
			"trace_verbose_true_in_config_override_false",
			`trace_verbose: true
			`,
			[]string{"--VV=false"},
			true,
			func() bool { return !opts.TraceVerbose },
		},
		{
			"trace_verbose_true_in_config_override_false",
			`trace_verbose: false
			`,
			[]string{"--VV=true"},
			true,
			func() bool { return opts.TraceVerbose },
		},
		// --js override
		{
			"jetstream_not_in_config_no_override",
			"",
			nil,
			false,
			func() bool { return opts.JetStream },
		},
		{
			"jetstream_not_in_config_override_true",
			"",
			[]string{"--js"},
			true,
			func() bool { return opts.JetStream },
		},
		{
			"jetstream_false_in_config_no_override",
			"jetstream: false",
			nil,
			false,
			func() bool { return opts.JetStream },
		},
		{
			"jetstream_false_in_config_override_true",
			"jetstream: false",
			[]string{"--js"},
			true,
			func() bool { return opts.JetStream },
		},
		{
			"jetstream_true_in_config_no_override",
			"jetstream: true",
			nil,
			true,
			func() bool { return opts.JetStream },
		},
		{
			"jetstream_true_in_config_override_false",
			"jetstream: true",
			[]string{"--js=false"},
			false,
			func() bool { return opts.JetStream },
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			content := fmt.Sprintf(template, logfile, test.content)
			conf := createConfFile(t, []byte(content))

			fs := flag.NewFlagSet("test", flag.ContinueOnError)
			var args []string
			args = append(args, "-c", conf)
			if test.cmdLine != nil {
				args = append(args, test.cmdLine...)
			}
			opts, err = ConfigureOptions(fs, args, nil, nil, nil)
			if err != nil {
				t.Fatalf("Error processing config: %v", err)
			}
			opts.NoSigs = true
			s := RunServer(opts)
			defer s.Shutdown()

			if test.val() != test.expected {
				t.Fatalf("Expected to be set to %v, got %v", test.expected, test.val())
			}
			// Do a config reload with a modified config file so that s.Reload()
			// actually does something (otherwise it would not because config
			// digest would not have changed). We could alternatively change
			// s.opts.configDigest to the empty string.
			reloadUpdateConfig(t, s, conf, content+`
				max_connections: 1000
			`)
			// Have `opts` now point to the new options after the Reload()
			opts = s.getOpts()
			if test.val() != test.expected {
				t.Fatalf("Expected to be set to %v, got %v", test.expected, test.val())
			}
		})
	}
}

func TestConfigReloadMaxControlLineWithClients(t *testing.T) {
	server, opts, config := runReloadServerWithConfig(t, "./configs/reload/basic.conf")
	defer server.Shutdown()

	// Ensure we can connect as a sanity check.
	addr := fmt.Sprintf("nats://%s:%d", opts.Host, server.Addr().(*net.TCPAddr).Port)
	nc, err := nats.Connect(addr)
	if err != nil {
		t.Fatalf("Error creating client: %v", err)
	}
	defer nc.Close()

	// Now grab server's internal client that matches.
	cid, _ := nc.GetClientID()
	c := server.getClient(cid)
	if c == nil {
		t.Fatalf("Could not look up internal client")
	}

	// Check that we have the correct mcl snapshotted into the connected client.
	getMcl := func(c *client) int32 {
		c.mu.Lock()
		defer c.mu.Unlock()
		return c.mcl
	}
	if mcl := getMcl(c); mcl != opts.MaxControlLine {
		t.Fatalf("Expected snapshot in client for mcl to be same as opts.MaxControlLine, got %d vs %d",
			mcl, opts.MaxControlLine)
	}

	changeCurrentConfigContentWithNewContent(t, config, []byte("listen: 127.0.0.1:-1; max_control_line: 222"))
	if err := server.Reload(); err != nil {
		t.Fatalf("Expected Reload to succeed, got %v", err)
	}

	// Refresh properly.
	opts = server.getOpts()

	if mcl := getMcl(c); mcl != opts.MaxControlLine {
		t.Fatalf("Expected snapshot in client for mcl to be same as new opts.MaxControlLine, got %d vs %d",
			mcl, opts.MaxControlLine)
	}
}

type testCustomAuth struct{}

func (ca *testCustomAuth) Check(c ClientAuthentication) bool { return true }

func TestConfigReloadIgnoreCustomAuth(t *testing.T) {
	conf := createConfFile(t, []byte(`
		port: -1
	`))
	opts := LoadConfig(conf)

	ca := &testCustomAuth{}
	opts.CustomClientAuthentication = ca
	opts.CustomRouterAuthentication = ca

	s := RunServer(opts)
	defer s.Shutdown()

	if err := s.Reload(); err != nil {
		t.Fatalf("Error during reload: %v", err)
	}

	if s.getOpts().CustomClientAuthentication != ca || s.getOpts().CustomRouterAuthentication != ca {
		t.Fatalf("Custom auth missing")
	}
}

func TestConfigReloadLeafNodeRandomPort(t *testing.T) {
	conf := createConfFile(t, []byte(`
		port: -1
		leafnodes {
			port: -1
		}
	`))
	s, _ := RunServerWithConfig(conf)
	defer s.Shutdown()

	s.mu.Lock()
	lnPortBefore := s.leafNodeListener.Addr().(*net.TCPAddr).Port
	s.mu.Unlock()

	if err := s.Reload(); err != nil {
		t.Fatalf("Error during reload: %v", err)
	}

	s.mu.Lock()
	lnPortAfter := s.leafNodeListener.Addr().(*net.TCPAddr).Port
	s.mu.Unlock()

	if lnPortBefore != lnPortAfter {
		t.Fatalf("Expected leafnodes listen port to be same, was %v is now %v", lnPortBefore, lnPortAfter)
	}
}

func TestConfigReloadLeafNodeWithTLS(t *testing.T) {
	template := `
		port: -1
		%s
		leaf {
			listen: "127.0.0.1:-1"
			tls: {
				ca_file: "../test/configs/certs/tlsauth/ca.pem"
				cert_file: "../test/configs/certs/tlsauth/server.pem"
				key_file: "../test/configs/certs/tlsauth/server-key.pem"
				timeout: 3
			}
		}
	`
	conf1 := createConfFile(t, []byte(fmt.Sprintf(template, "")))
	s1, o1 := RunServerWithConfig(conf1)
	defer s1.Shutdown()

	u, err := url.Parse(fmt.Sprintf("nats://localhost:%d", o1.LeafNode.Port))
	if err != nil {
		t.Fatalf("Error creating url: %v", err)
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

	checkFor(t, 3*time.Second, 15*time.Millisecond, func() error {
		if n := s1.NumLeafNodes(); n != 1 {
			return fmt.Errorf("Expected 1 leaf node, got %v", n)
		}
		return nil
	})

	changeCurrentConfigContentWithNewContent(t, conf1, []byte(fmt.Sprintf(template, "debug: false")))

	if err := s1.Reload(); err != nil {
		t.Fatalf("Error during reload: %v", err)
	}
}

func TestConfigReloadLeafNodeWithRemotesNoChanges(t *testing.T) {
	template := `
        port: -1
        cluster {
            port: -1
            name: "%s"
        }
        leaf {
            remotes [
                {
                    urls: [
                        "nats://127.0.0.1:1234",
                        "nats://127.0.0.1:1235",
                        "nats://127.0.0.1:1236",
                        "nats://127.0.0.1:1237",
                        "nats://127.0.0.1:1238",
                        "nats://127.0.0.1:1239",
                    ]
                }
            ]
        }
	`
	config := fmt.Sprintf(template, "A")
	conf := createConfFile(t, []byte(config))
	o, err := ProcessConfigFile(conf)
	if err != nil {
		t.Fatalf("Error processing config file: %v", err)
	}
	o.NoLog, o.NoSigs = true, false
	s := RunServer(o)
	defer s.Shutdown()

	config = fmt.Sprintf(template, "B")
	changeCurrentConfigContentWithNewContent(t, conf, []byte(config))

	if err := s.Reload(); err != nil {
		t.Fatalf("Error during reload: %v", err)
	}
}

func TestConfigReloadAndVarz(t *testing.T) {
	template := `
		port: -1
		%s
	`
	conf := createConfFile(t, []byte(fmt.Sprintf(template, "")))
	s, _ := RunServerWithConfig(conf)
	defer s.Shutdown()

	s.mu.Lock()
	initConfigTime := s.configTime
	s.mu.Unlock()

	v, _ := s.Varz(nil)
	if !v.ConfigLoadTime.Equal(initConfigTime) {
		t.Fatalf("ConfigLoadTime should be %v, got %v", initConfigTime, v.ConfigLoadTime)
	}
	if v.MaxConn != DEFAULT_MAX_CONNECTIONS {
		t.Fatalf("MaxConn should be %v, got %v", DEFAULT_MAX_CONNECTIONS, v.MaxConn)
	}
	got := v.ConfigDigest
	expected := "sha256:7ea4cc5c6864139d814ce940a40ee6546b7ac5285eec3c390a4ce11e34dc1102"
	if got != expected {
		t.Fatalf("got: %v, expected: %v", got, expected)
	}

	changeCurrentConfigContentWithNewContent(t, conf, []byte(fmt.Sprintf(template, "max_connections: 10")))

	// Make sure we wait a bit so config load time has a chance to change.
	time.Sleep(15 * time.Millisecond)

	if err := s.Reload(); err != nil {
		t.Fatalf("Error during reload: %v", err)
	}

	v, _ = s.Varz(nil)
	if v.ConfigLoadTime.Equal(initConfigTime) {
		t.Fatalf("ConfigLoadTime should be different from %v", initConfigTime)
	}
	if v.MaxConn != 10 {
		t.Fatalf("MaxConn should be 10, got %v", v.MaxConn)
	}
	got = v.ConfigDigest
	expected = "sha256:508a26309068f62c3022aa8951e712ed8ff5dd6f2360f727ad8242a2a233176e"
	if got != expected {
		t.Fatalf("got: %v, expected: %v", got, expected)
	}
}

func TestConfigReloadConnectErrReports(t *testing.T) {
	template := `
		port: -1
		%s
		%s
	`
	conf := createConfFile(t, []byte(fmt.Sprintf(template, "", "")))
	s, _ := RunServerWithConfig(conf)
	defer s.Shutdown()

	opts := s.getOpts()
	if cer := opts.ConnectErrorReports; cer != DEFAULT_CONNECT_ERROR_REPORTS {
		t.Fatalf("Expected ConnectErrorReports to be %v, got %v", DEFAULT_CONNECT_ERROR_REPORTS, cer)
	}
	if rer := opts.ReconnectErrorReports; rer != DEFAULT_RECONNECT_ERROR_REPORTS {
		t.Fatalf("Expected ReconnectErrorReports to be %v, got %v", DEFAULT_RECONNECT_ERROR_REPORTS, rer)
	}

	changeCurrentConfigContentWithNewContent(t, conf,
		[]byte(fmt.Sprintf(template, "connect_error_reports: 2", "reconnect_error_reports: 3")))

	if err := s.Reload(); err != nil {
		t.Fatalf("Error during reload: %v", err)
	}

	opts = s.getOpts()
	if cer := opts.ConnectErrorReports; cer != 2 {
		t.Fatalf("Expected ConnectErrorReports to be %v, got %v", 2, cer)
	}
	if rer := opts.ReconnectErrorReports; rer != 3 {
		t.Fatalf("Expected ReconnectErrorReports to be %v, got %v", 3, rer)
	}
}

func TestConfigReloadAuthDoesNotBreakRouteInterest(t *testing.T) {
	s, opts := RunServerWithConfig("./configs/seed_tls.conf")
	defer s.Shutdown()

	// Create client and sub interest on seed server.
	urlSeed := fmt.Sprintf("nats://%s:%d/", opts.Host, opts.Port)
	nc, err := nats.Connect(urlSeed)
	if err != nil {
		t.Fatalf("Error creating client: %v\n", err)
	}
	defer nc.Close()

	ch := make(chan bool)
	nc.Subscribe("foo", func(m *nats.Msg) { ch <- true })
	nc.Flush()

	// Use this to check for message.
	checkForMsg := func() {
		t.Helper()
		select {
		case <-ch:
		case <-time.After(2 * time.Second):
			t.Fatal("Timeout waiting for message across route")
		}
	}

	// Create second server and form cluster. We will send from here.
	urlRoute := fmt.Sprintf("nats://%s:%d", opts.Cluster.Host, opts.Cluster.Port)
	optsA := nextServerOpts(opts)
	optsA.Routes = RoutesFromStr(urlRoute)

	sa := RunServer(optsA)
	defer sa.Shutdown()

	checkClusterFormed(t, s, sa)
	checkSubInterest(t, sa, globalAccountName, "foo", time.Second)

	// Create second client and send message from this one. Interest should be here.
	urlA := fmt.Sprintf("nats://%s:%d/", optsA.Host, optsA.Port)
	nc2, err := nats.Connect(urlA)
	if err != nil {
		t.Fatalf("Error creating client: %v\n", err)
	}
	defer nc2.Close()

	// Check that we can send messages.
	nc2.Publish("foo", nil)
	checkForMsg()

	// Now shutdown nc2 and srvA.
	nc2.Close()
	sa.Shutdown()

	// Now force reload on seed server of auth.
	s.reloadAuthorization(false)

	// Restart both server A and client 2.
	sa = RunServer(optsA)
	defer sa.Shutdown()

	checkClusterFormed(t, s, sa)
	checkSubInterest(t, sa, globalAccountName, "foo", time.Second)

	nc2, err = nats.Connect(urlA)
	if err != nil {
		t.Fatalf("Error creating client: %v\n", err)
	}
	defer nc2.Close()

	// Check that we can still send messages.
	nc2.Publish("foo", nil)
	checkForMsg()
}

func TestConfigReloadAccountResolverTLSConfig(t *testing.T) {
	kp, _ := nkeys.FromSeed(oSeed)
	akp, _ := nkeys.CreateAccount()
	apub, _ := akp.PublicKey()
	nac := jwt.NewAccountClaims(apub)
	ajwt, err := nac.Encode(kp)
	if err != nil {
		t.Fatalf("Error generating account JWT: %v", err)
	}
	pub, _ := kp.PublicKey()

	tc := &TLSConfigOpts{
		CertFile: "../test/configs/certs/server-cert.pem",
		KeyFile:  "../test/configs/certs/server-key.pem",
		CaFile:   "../test/configs/certs/ca.pem",
	}
	tlsConfig, err := GenTLSConfig(tc)
	if err != nil {
		t.Fatalf("Error generating tls config: %v", err)
	}
	ts := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte(ajwt))
	}))
	ts.TLS = tlsConfig
	ts.StartTLS()
	defer ts.Close()
	// Set a dummy logger to prevent tls bad certificate output to stderr.
	ts.Config.ErrorLog = log.New(&bytes.Buffer{}, "", 0)

	confTemplate := `
				listen: -1
				trusted_keys: %s
				resolver: URL("%s/ngs/v1/accounts/jwt/")
				%s
	`
	conf := createConfFile(t, []byte(fmt.Sprintf(confTemplate, pub, ts.URL, `
		resolver_tls {
			cert_file: "../test/configs/certs/client-cert.pem"
			key_file: "../test/configs/certs/client-key.pem"
			ca_file: "../test/configs/certs/ca.pem"
		}
	`)))

	s, _ := RunServerWithConfig(conf)
	defer s.Shutdown()

	changeCurrentConfigContentWithNewContent(t, conf, []byte(fmt.Sprintf(confTemplate, pub, ts.URL, "")))
	if err := s.Reload(); err != nil {
		t.Fatalf("Error during reload: %v", err)
	}

	if _, err := s.LookupAccount(apub); err == nil {
		t.Fatal("Expected error during lookup, did not get one")
	}

	changeCurrentConfigContentWithNewContent(t, conf, []byte(fmt.Sprintf(confTemplate, pub, ts.URL, `
		resolver_tls {
			insecure: true
		}
	`)))
	if err := s.Reload(); err != nil {
		t.Fatalf("Error during reload: %v", err)
	}

	acc, err := s.LookupAccount(apub)
	if err != nil {
		t.Fatalf("Error during lookup: %v", err)
	}
	if acc == nil {
		t.Fatalf("Expected to receive an account")
	}
	if acc.Name != apub {
		t.Fatalf("Account name did not match claim key")
	}
}

func TestConfigReloadLogging(t *testing.T) {
	// This test basically starts a server and causes it's configuration to be reloaded 3 times.
	// Each time, a new log file is created and trace levels are turned, off - on - off.

	// At the end of the test, all 3 log files are inspected for certain traces.
	countMatches := func(log []byte, stmts ...string) int {
		matchCnt := 0
		for _, stmt := range stmts {
			if strings.Contains(string(log), stmt) {
				matchCnt++
			}
		}
		return matchCnt
	}

	traces := []string{"[TRC]", "[DBG]", "SYSTEM", "MSG_PAYLOAD", "$SYS.SERVER.ACCOUNT"}

	didTrace := func(log []byte) bool {
		return countMatches(log, "[INF] Reloaded server configuration") == 1
	}

	tracingAbsent := func(log []byte) bool {
		return countMatches(log, traces...) == 0 && didTrace(log)
	}

	tracingPresent := func(log []byte) bool {
		return len(traces) == countMatches(log, traces...) && didTrace(log)
	}

	check := func(filename string, valid func([]byte) bool) {
		t.Helper()
		log, err := os.ReadFile(filename)
		if err != nil {
			t.Fatalf("Error reading log file %s: %v\n", filename, err)
		}
		if !valid(log) {
			t.Fatalf("%s is not valid: %s", filename, log)
		}
		//t.Logf("%s contains: %s\n", filename, log)
	}

	// common configuration setting up system accounts. trace_verbose needs this to cause traces
	commonCfg := `
		port: -1
		system_account: sys
		accounts {
		  sys { users = [ {user: sys, pass: "" } ] }
		  nats.io: { users = [ { user : bar, pass: "pwd" } ] }
		}
	`

	conf := createConfFile(t, []byte(commonCfg))

	defer removeFile(t, "off-pre.log")
	defer removeFile(t, "on.log")
	defer removeFile(t, "off-post.log")

	s, opts := RunServerWithConfig(conf)
	defer s.Shutdown()

	reload := func(change string) {
		t.Helper()
		changeCurrentConfigContentWithNewContent(t, conf, []byte(commonCfg+`
			`+change+`
		`))

		if err := s.Reload(); err != nil {
			t.Fatalf("Error during reload: %v", err)
		}
	}

	traffic := func(cnt int) {
		t.Helper()
		// Create client and sub interest on server and create traffic
		urlSeed := fmt.Sprintf("nats://bar:pwd@%s:%d/", opts.Host, opts.Port)
		nc, err := nats.Connect(urlSeed)
		if err != nil {
			t.Fatalf("Error creating client: %v\n", err)
		}
		defer nc.Close()

		msgs := make(chan *nats.Msg, 1)
		defer close(msgs)

		sub, err := nc.ChanSubscribe("foo", msgs)
		if err != nil {
			t.Fatalf("Error creating subscriber: %v\n", err)
		}

		nc.Flush()

		for i := 0; i < cnt; i++ {
			if err := nc.Publish("foo", []byte("bar")); err == nil {
				<-msgs
			}
		}

		sub.Unsubscribe()
		nc.Close()
	}

	reload("log_file: off-pre.log")

	traffic(10) // generate NO trace/debug entries in off-pre.log

	reload(`
		log_file: on.log
		debug: true
		trace_verbose: true
	`)

	traffic(10) // generate trace/debug entries in on.log

	reload(`
		log_file: off-post.log
		debug: false
		trace_verbose: false
	`)

	traffic(10) // generate trace/debug entries in off-post.log

	// check resulting log files for expected content
	check("off-pre.log", tracingAbsent)
	check("on.log", tracingPresent)
	check("off-post.log", tracingAbsent)
}

func TestConfigReloadValidate(t *testing.T) {
	confFileName := createConfFile(t, []byte(`
		listen: "127.0.0.1:-1"
		no_auth_user: a
		authorization {
			users [
				{user: "a", password: "a"},
				{user: "b", password: "b"}
			]
		}
	`))
	srv, _ := RunServerWithConfig(confFileName)
	defer srv.Shutdown()
	// Induce error by removing the user no_auth_user points to
	changeCurrentConfigContentWithNewContent(t, confFileName, []byte(`
		listen: "127.0.0.1:-1"
		no_auth_user: a
		authorization {
			users [
				{user: "b", password: "b"}
			]
		}
	`))
	if err := srv.Reload(); err == nil {
		t.Fatal("Expected error on reload, got none")
	} else if !strings.HasPrefix(err.Error(), "no_auth_user:") {
		t.Logf("Expected no_auth_user error, got different one %s", err)
	}
}

func TestConfigReloadAccounts(t *testing.T) {
	conf := createConfFile(t, []byte(`
	listen: "127.0.0.1:-1"
	system_account: SYS
	accounts {
		SYS {
			users = [
				{user: sys, password: pwd}
			]
		}
		ACC {
			users = [
				{user: usr, password: pwd}
			]
		}
		acc_deleted_after_reload_will_trigger_reload_of_all_accounts {
			users = [
				{user: notused, password: soon}
			]
		}
	}
	`))
	s, o := RunServerWithConfig(conf)
	defer s.Shutdown()

	urlSys := fmt.Sprintf("nats://sys:pwd@%s:%d", o.Host, o.Port)
	urlUsr := fmt.Sprintf("nats://usr:pwd@%s:%d", o.Host, o.Port)
	oldAcci, ok := s.accounts.Load("SYS")
	if !ok {
		t.Fatal("No SYS account")
	}
	oldAcc := oldAcci.(*Account)

	testSrvState := func(oldAcc *Account) {
		t.Helper()
		sysAcc := s.SystemAccount()
		s.mu.Lock()
		defer s.mu.Unlock()
		if s.sys == nil || sysAcc == nil {
			t.Fatal("Expected sys.account to be non-nil")
		}
		if sysAcc.Name != "SYS" {
			t.Fatal("Found wrong sys.account")
		}
		if s.opts.SystemAccount != "SYS" {
			t.Fatal("Found wrong sys.account")
		}
		ai, ok := s.accounts.Load(s.opts.SystemAccount)
		if !ok {
			t.Fatalf("System account %q not found in s.accounts map", s.opts.SystemAccount)
		}
		acc := ai.(*Account)
		if acc != oldAcc {
			t.Fatalf("System account pointer was changed during reload, was %p now %p", oldAcc, acc)
		}
		if s.sys.client == nil {
			t.Fatal("Expected sys.client to be non-nil")
		}
		s.sys.client.mu.Lock()
		defer s.sys.client.mu.Unlock()
		if s.sys.client.acc.Name != "SYS" {
			t.Fatal("Found wrong sys.account")
		}
		if s.sys.client.echo {
			t.Fatal("Internal clients should always have echo false")
		}
		s.sys.account.mu.Lock()
		if _, ok := s.sys.account.clients[s.sys.client]; !ok {
			s.sys.account.mu.Unlock()
			t.Fatal("internal client not present")
		}
		s.sys.account.mu.Unlock()
	}

	// Below tests use connection names so that they can be checked for.
	// The test subscribes to ACC only. This avoids receiving own messages.
	subscribe := func(name string) (*nats.Conn, *nats.Subscription, *nats.Subscription) {
		t.Helper()
		c, err := nats.Connect(urlSys, nats.Name(name))
		if err != nil {
			t.Fatalf("Error on connect: %v", err)
		}
		subCon, err := c.SubscribeSync("$SYS.ACCOUNT.ACC.CONNECT")
		if err != nil {
			t.Fatalf("Error on subscribe CONNECT: %v", err)
		}
		subDis, err := c.SubscribeSync("$SYS.ACCOUNT.ACC.DISCONNECT")
		if err != nil {
			t.Fatalf("Error on subscribe DISCONNECT: %v", err)
		}
		c.Flush()
		return c, subCon, subDis
	}
	recv := func(name string, sub *nats.Subscription) {
		t.Helper()
		if msg, err := sub.NextMsg(1 * time.Second); err != nil {
			t.Fatalf("%s Error on next: %v", name, err)
		} else {
			cMsg := ConnectEventMsg{}
			json.Unmarshal(msg.Data, &cMsg)
			if cMsg.Client.Name != name {
				t.Fatalf("%s wrong message: %s", name, string(msg.Data))
			}
		}
	}
	triggerSysEvent := func(name string, subs []*nats.Subscription) {
		t.Helper()
		ncs1, err := nats.Connect(urlUsr, nats.Name(name))
		if err != nil {
			t.Fatalf("Error on connect: %v", err)
		}
		ncs1.Close()
		for _, sub := range subs {
			recv(name, sub)
			// Make sure they are empty.
			if pending, _, _ := sub.Pending(); pending != 0 {
				t.Fatalf("Expected no pending, got %d for %+v", pending, sub)
			}
		}
	}

	testSrvState(oldAcc)
	c1, s1C, s1D := subscribe("SYS1")
	defer c1.Close()
	defer s1C.Unsubscribe()
	defer s1D.Unsubscribe()
	triggerSysEvent("BEFORE1", []*nats.Subscription{s1C, s1D})
	triggerSysEvent("BEFORE2", []*nats.Subscription{s1C, s1D})

	// Remove account to trigger account reload
	reloadUpdateConfig(t, s, conf, `
	listen: "127.0.0.1:-1"
	system_account: SYS
	accounts {
		SYS {
			users = [
				{user: sys, password: pwd}
			]
		}
		ACC {
			users = [
				{user: usr, password: pwd}
			]
		}
	}
	`)

	testSrvState(oldAcc)
	c2, s2C, s2D := subscribe("SYS2")
	defer c2.Close()
	defer s2C.Unsubscribe()
	defer s2D.Unsubscribe()
	// test new and existing subscriptions
	triggerSysEvent("AFTER1", []*nats.Subscription{s1C, s1D, s2C, s2D})
	triggerSysEvent("AFTER2", []*nats.Subscription{s1C, s1D, s2C, s2D})
}

func TestConfigReloadDefaultSystemAccount(t *testing.T) {
	conf := createConfFile(t, []byte(`
	listen: "127.0.0.1:-1"
	accounts {
		ACC {
			users = [
				{user: usr, password: pwd}
			]
		}
	}
	`))
	s, _ := RunServerWithConfig(conf)
	defer s.Shutdown()

	sysAcc := s.SystemAccount()
	if sysAcc == nil {
		t.Fatalf("Expected system account to be present")
	}
	numSubs := sysAcc.TotalSubs()

	sname := sysAcc.GetName()
	testInAccounts := func() {
		t.Helper()
		var found bool
		s.accounts.Range(func(k, v any) bool {
			acc := v.(*Account)
			if acc.GetName() == sname {
				found = true
				return false
			}
			return true
		})
		if !found {
			t.Fatalf("System account not found in accounts list")
		}
	}
	testInAccounts()

	if err := s.Reload(); err != nil {
		t.Fatalf("Unexpected error reloading: %v", err)
	}

	sysAcc = s.SystemAccount()
	if sysAcc == nil {
		t.Fatalf("Expected system account to still be present")
	}
	if sysAcc.TotalSubs() != numSubs {
		t.Fatalf("Expected %d subs, got %d", numSubs, sysAcc.TotalSubs())
	}
	testInAccounts()
}

func TestConfigReloadAccountMappings(t *testing.T) {
	conf := createConfFile(t, []byte(`
	listen: "127.0.0.1:-1"
	accounts {
		ACC {
			users = [{user: usr, password: pwd}]
			mappings = { foo: bar }
		}
	}
	`))
	s, opts := RunServerWithConfig(conf)
	defer s.Shutdown()

	reloadUpdateConfig(t, s, conf, `
	listen: "127.0.0.1:-1"
	accounts {
		ACC {
			users = [{user: usr, password: pwd}]
			mappings = { foo: baz }
		}
	}
	`)

	nc := natsConnect(t, fmt.Sprintf("nats://usr:pwd@%s:%d", opts.Host, opts.Port))
	defer nc.Close()

	fsub, _ := nc.SubscribeSync("foo")
	sub, _ := nc.SubscribeSync("baz")
	nc.Publish("foo", nil)
	nc.Flush()

	checkPending := func(sub *nats.Subscription, expected int) {
		t.Helper()
		if n, _, _ := sub.Pending(); n != expected {
			t.Fatalf("Expected %d msgs for %q, but got %d", expected, sub.Subject, n)
		}
	}
	checkPending(fsub, 0)
	checkPending(sub, 1)

	// Drain it off
	if _, err := sub.NextMsg(2 * time.Second); err != nil {
		t.Fatalf("Error receiving msg: %v", err)
	}

	reloadUpdateConfig(t, s, conf, `
	listen: "127.0.0.1:-1"
	accounts {
		ACC {
			users = [{user: usr, password: pwd}]
		}
	}
	`)

	nc.Publish("foo", nil)
	nc.Flush()

	checkPending(fsub, 1)
	checkPending(sub, 0)
}

func TestConfigReloadWithSysAccountOnly(t *testing.T) {
	conf := createConfFile(t, []byte(`
		listen: "127.0.0.1:-1"
		accounts {
			$SYS {
				users = [{user: "system",pass: "password"}, {user: "system2",pass: "password2"}]
			}
		}
	`))
	defer os.Remove(conf)
	s, _ := RunServerWithConfig(conf)
	defer s.Shutdown()

	dch := make(chan struct{}, 1)
	nc := natsConnect(t,
		s.ClientURL(),
		nats.DisconnectErrHandler(func(_ *nats.Conn, _ error) {
			dch <- struct{}{}
		}),
		nats.NoCallbacksAfterClientClose())
	defer nc.Close()

	// Just reload...
	if err := s.Reload(); err != nil {
		t.Fatalf("Error on reload: %v", err)
	}

	// Make sure we did not get disconnected
	select {
	case <-dch:
		t.Fatal("Got disconnected!")
	case <-time.After(500 * time.Millisecond):
		// ok
	}
}

func TestConfigReloadRouteImportPermissionsWithAccounts(t *testing.T) {
	for _, test := range []struct {
		name     string
		poolSize string
		accounts string
	}{
		{"regular", "pool_size: -1", _EMPTY_},
		{"pooling", "pool_size: 5", _EMPTY_},
		{"per-account", _EMPTY_, "accounts: [\"A\"]"},
		{"pool and per-account", "pool_size: 3", "accounts: [\"A\"]"},
	} {
		t.Run("import "+test.name, func(t *testing.T) {
			confATemplate := `
				server_name: "A"
				port: -1
				accounts {
					A { users: [{user: "user1", password: "pwd"}] }
					B { users: [{user: "user2", password: "pwd"}] }
					C { users: [{user: "user3", password: "pwd"}] }
					D { users: [{user: "user4", password: "pwd"}] }
				}
				cluster {
					name: "local"
					listen: 127.0.0.1:-1
					permissions {
						import {
							allow: %s
						}
						export {
							allow: ">"
						}
					}
					%s
					%s
				}
			`
			confA := createConfFile(t, []byte(fmt.Sprintf(confATemplate, `"foo"`, test.poolSize, test.accounts)))
			srva, optsA := RunServerWithConfig(confA)
			defer srva.Shutdown()

			confBTemplate := `
				server_name: "B"
				port: -1
				accounts {
					A { users: [{user: "user1", password: "pwd"}] }
					B { users: [{user: "user2", password: "pwd"}] }
					C { users: [{user: "user3", password: "pwd"}] }
					D { users: [{user: "user4", password: "pwd"}] }
				}
				cluster {
					listen: 127.0.0.1:-1
					name: "local"
					permissions {
						import {
							allow: %s
						}
						export {
							allow: ">"
						}
					}
					routes = [
						"nats://127.0.0.1:%d"
					]
					%s
					%s
				}
			`
			confB := createConfFile(t, []byte(fmt.Sprintf(confBTemplate, `"foo"`, optsA.Cluster.Port, test.poolSize, test.accounts)))
			srvb, _ := RunServerWithConfig(confB)
			defer srvb.Shutdown()

			checkClusterFormed(t, srva, srvb)

			ncA := natsConnect(t, srva.ClientURL(), nats.UserInfo("user1", "pwd"))
			defer ncA.Close()

			sub1Foo := natsSubSync(t, ncA, "foo")
			sub2Foo := natsSubSync(t, ncA, "foo")

			sub1Bar := natsSubSync(t, ncA, "bar")
			sub2Bar := natsSubSync(t, ncA, "bar")

			natsFlush(t, ncA)

			checkSubInterest(t, srvb, "A", "foo", 2*time.Second)
			checkSubNoInterest(t, srvb, "A", "bar", 2*time.Second)

			ncB := natsConnect(t, srvb.ClientURL(), nats.UserInfo("user1", "pwd"))
			defer ncB.Close()

			check := func(sub *nats.Subscription, expected bool) {
				t.Helper()
				if expected {
					natsNexMsg(t, sub, time.Second)
				} else {
					if msg, err := sub.NextMsg(50 * time.Millisecond); err == nil {
						t.Fatalf("Should not have gotten the message, got %s/%s", msg.Subject, msg.Data)
					}
				}
			}

			// Should receive on "foo"
			natsPub(t, ncB, "foo", []byte("foo1"))
			check(sub1Foo, true)
			check(sub2Foo, true)

			// But not on "bar"
			natsPub(t, ncB, "bar", []byte("bar1"))
			check(sub1Bar, false)
			check(sub2Bar, false)

			reloadUpdateConfig(t, srva, confA, fmt.Sprintf(confATemplate, `"bar"`, test.poolSize, test.accounts))
			reloadUpdateConfig(t, srvb, confB, fmt.Sprintf(confBTemplate, `"bar"`, optsA.Cluster.Port, test.poolSize, test.accounts))

			checkClusterFormed(t, srva, srvb)

			checkSubNoInterest(t, srvb, "A", "foo", 2*time.Second)
			checkSubInterest(t, srvb, "A", "bar", 2*time.Second)

			// Should not receive on foo
			natsPub(t, ncB, "foo", []byte("foo2"))
			check(sub1Foo, false)
			check(sub2Foo, false)

			// Should be able to receive on bar
			natsPub(t, ncB, "bar", []byte("bar2"))
			check(sub1Bar, true)
			check(sub2Bar, true)

			// Restore "foo"
			reloadUpdateConfig(t, srva, confA, fmt.Sprintf(confATemplate, `"foo"`, test.poolSize, test.accounts))
			reloadUpdateConfig(t, srvb, confB, fmt.Sprintf(confBTemplate, `"foo"`, optsA.Cluster.Port, test.poolSize, test.accounts))

			checkClusterFormed(t, srva, srvb)

			checkSubInterest(t, srvb, "A", "foo", 2*time.Second)
			checkSubNoInterest(t, srvb, "A", "bar", 2*time.Second)

			// Should receive on "foo"
			natsPub(t, ncB, "foo", []byte("foo3"))
			check(sub1Foo, true)
			check(sub2Foo, true)
			// But make sure there are no more than what we expect
			check(sub1Foo, false)
			check(sub2Foo, false)

			// And now "bar" should fail
			natsPub(t, ncB, "bar", []byte("bar3"))
			check(sub1Bar, false)
			check(sub2Bar, false)
		})
	}
	// Check export now
	for _, test := range []struct {
		name     string
		poolSize string
		accounts string
	}{
		{"regular", "pool_size: -1", _EMPTY_},
		{"pooling", "pool_size: 5", _EMPTY_},
		{"per-account", _EMPTY_, "accounts: [\"A\"]"},
		{"pool and per-account", "pool_size: 3", "accounts: [\"A\"]"},
	} {
		t.Run("export "+test.name, func(t *testing.T) {
			confATemplate := `
				server_name: "A"
				port: -1
				accounts {
					A { users: [{user: "user1", password: "pwd"}] }
					B { users: [{user: "user2", password: "pwd"}] }
					C { users: [{user: "user3", password: "pwd"}] }
					D { users: [{user: "user4", password: "pwd"}] }
				}
				cluster {
					name: "local"
					listen: 127.0.0.1:-1
					permissions {
						import {
							allow: ">"
						}
						export {
							allow: %s
						}
					}
					%s
					%s
				}
			`
			confA := createConfFile(t, []byte(fmt.Sprintf(confATemplate, `"foo"`, test.poolSize, test.accounts)))
			srva, optsA := RunServerWithConfig(confA)
			defer srva.Shutdown()

			confBTemplate := `
				server_name: "B"
				port: -1
				accounts {
					A { users: [{user: "user1", password: "pwd"}] }
					B { users: [{user: "user2", password: "pwd"}] }
					C { users: [{user: "user3", password: "pwd"}] }
					D { users: [{user: "user4", password: "pwd"}] }
				}
				cluster {
					listen: 127.0.0.1:-1
					name: "local"
					permissions {
						import {
							allow: ">"
						}
						export {
							allow: %s
						}
					}
					routes = [
						"nats://127.0.0.1:%d"
					]
					%s
					%s
				}
			`
			confB := createConfFile(t, []byte(fmt.Sprintf(confBTemplate, `"foo"`, optsA.Cluster.Port, test.poolSize, test.accounts)))
			srvb, _ := RunServerWithConfig(confB)
			defer srvb.Shutdown()

			checkClusterFormed(t, srva, srvb)

			ncA := natsConnect(t, srva.ClientURL(), nats.UserInfo("user1", "pwd"))
			defer ncA.Close()

			sub1Foo := natsSubSync(t, ncA, "foo")
			sub2Foo := natsSubSync(t, ncA, "foo")

			sub1Bar := natsSubSync(t, ncA, "bar")
			sub2Bar := natsSubSync(t, ncA, "bar")

			natsFlush(t, ncA)

			checkSubInterest(t, srvb, "A", "foo", 2*time.Second)
			checkSubNoInterest(t, srvb, "A", "bar", 2*time.Second)

			ncB := natsConnect(t, srvb.ClientURL(), nats.UserInfo("user1", "pwd"))
			defer ncB.Close()

			check := func(sub *nats.Subscription, expected bool) {
				t.Helper()
				if expected {
					natsNexMsg(t, sub, time.Second)
				} else {
					if msg, err := sub.NextMsg(50 * time.Millisecond); err == nil {
						t.Fatalf("Should not have gotten the message, got %s/%s", msg.Subject, msg.Data)
					}
				}
			}

			// Should receive on "foo"
			natsPub(t, ncB, "foo", []byte("foo1"))
			check(sub1Foo, true)
			check(sub2Foo, true)

			// But not on "bar"
			natsPub(t, ncB, "bar", []byte("bar1"))
			check(sub1Bar, false)
			check(sub2Bar, false)

			reloadUpdateConfig(t, srva, confA, fmt.Sprintf(confATemplate, `["foo", "bar"]`, test.poolSize, test.accounts))
			reloadUpdateConfig(t, srvb, confB, fmt.Sprintf(confBTemplate, `["foo", "bar"]`, optsA.Cluster.Port, test.poolSize, test.accounts))

			checkClusterFormed(t, srva, srvb)

			checkSubInterest(t, srvb, "A", "foo", 2*time.Second)
			checkSubInterest(t, srvb, "A", "bar", 2*time.Second)

			// Should receive on foo and bar
			natsPub(t, ncB, "foo", []byte("foo2"))
			check(sub1Foo, true)
			check(sub2Foo, true)

			natsPub(t, ncB, "bar", []byte("bar2"))
			check(sub1Bar, true)
			check(sub2Bar, true)

			// Remove "bar"
			reloadUpdateConfig(t, srva, confA, fmt.Sprintf(confATemplate, `"foo"`, test.poolSize, test.accounts))
			reloadUpdateConfig(t, srvb, confB, fmt.Sprintf(confBTemplate, `"foo"`, optsA.Cluster.Port, test.poolSize, test.accounts))

			checkClusterFormed(t, srva, srvb)

			checkSubInterest(t, srvb, "A", "foo", 2*time.Second)
			checkSubNoInterest(t, srvb, "A", "bar", 2*time.Second)

			// Should receive on "foo"
			natsPub(t, ncB, "foo", []byte("foo3"))
			check(sub1Foo, true)
			check(sub2Foo, true)
			// But make sure there are no more than what we expect
			check(sub1Foo, false)
			check(sub2Foo, false)

			// And now "bar" should fail
			natsPub(t, ncB, "bar", []byte("bar3"))
			check(sub1Bar, false)
			check(sub2Bar, false)
		})
	}
}

func TestConfigReloadRoutePoolAndPerAccount(t *testing.T) {
	confATemplate := `
		port: -1
		server_name: "A"
		accounts {
				A { users: [{user: "user1", password: "pwd"}] }
				B { users: [{user: "user2", password: "pwd"}] }
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
	confA := createConfFile(t, []byte(fmt.Sprintf(confATemplate, "pool_size: 3", "accounts: [\"A\"]")))
	srva, optsA := RunServerWithConfig(confA)
	defer srva.Shutdown()

	confBCTemplate := `
		port: -1
		server_name: "%s"
		accounts {
				A { users: [{user: "user1", password: "pwd"}] }
				B { users: [{user: "user2", password: "pwd"}] }
				C { users: [{user: "user3", password: "pwd"}] }
				D { users: [{user: "user4", password: "pwd"}] }
		}
		cluster {
				listen: 127.0.0.1:-1
				name: "local"
				routes = [
						"nats://127.0.0.1:%d"
				]
				%s
				%s
		}
	`
	confB := createConfFile(t, []byte(fmt.Sprintf(confBCTemplate, "B", optsA.Cluster.Port, "pool_size: 3", "accounts: [\"A\"]")))
	srvb, _ := RunServerWithConfig(confB)
	defer srvb.Shutdown()
	confC := createConfFile(t, []byte(fmt.Sprintf(confBCTemplate, "C", optsA.Cluster.Port, "pool_size: 3", "accounts: [\"A\"]")))
	srvc, _ := RunServerWithConfig(confC)
	defer srvc.Shutdown()

	checkClusterFormed(t, srva, srvb, srvc)

	// We will also create subscriptions for accounts A, B and C on all sides
	// just to make sure that interest is properly propagated after a reload.
	// The conns slices will contain connections for accounts A on srva, srvb,
	// srvc, then B on srva, srvb, etc.. and the subs slices will contain
	// subscriptions for account A on foo on srva, bar on srvb, baz on srvc,
	// then for account B on foo on srva, etc...
	var conns []*nats.Conn
	var subs []*nats.Subscription
	for _, user := range []string{"user1", "user2", "user3"} {
		nc := natsConnect(t, srva.ClientURL(), nats.UserInfo(user, "pwd"))
		defer nc.Close()
		conns = append(conns, nc)
		sub := natsSubSync(t, nc, "foo")
		subs = append(subs, sub)
		nc = natsConnect(t, srvb.ClientURL(), nats.UserInfo(user, "pwd"))
		defer nc.Close()
		conns = append(conns, nc)
		sub = natsSubSync(t, nc, "bar")
		subs = append(subs, sub)
		nc = natsConnect(t, srvc.ClientURL(), nats.UserInfo(user, "pwd"))
		defer nc.Close()
		conns = append(conns, nc)
		sub = natsSubSync(t, nc, "baz")
		subs = append(subs, sub)
	}

	checkCluster := func() {
		t.Helper()
		checkClusterFormed(t, srva, srvb, srvc)

		for _, acc := range []string{"A", "B", "C"} {
			// On server A, there should be interest for bar/baz
			checkSubInterest(t, srva, acc, "bar", 2*time.Second)
			checkSubInterest(t, srva, acc, "baz", 2*time.Second)
			// On serer B, there should be interest on foo/baz
			checkSubInterest(t, srvb, acc, "foo", 2*time.Second)
			checkSubInterest(t, srvb, acc, "baz", 2*time.Second)
			// And on server C, interest on foo/bar
			checkSubInterest(t, srvc, acc, "foo", 2*time.Second)
			checkSubInterest(t, srvc, acc, "bar", 2*time.Second)
		}
	}
	checkCluster()

	getAccRouteID := func(acc string) uint64 {
		s := srva
		var id uint64
		srvbId := srvb.ID()
		s.mu.RLock()
		if remotes, ok := s.accRoutes[acc]; ok {
			// For this test, we will take a single remote, say srvb
			if r := remotes[srvbId]; r != nil {
				r.mu.Lock()
				if string(r.route.accName) == acc {
					id = r.cid
				}
				r.mu.Unlock()
			}
		}
		s.mu.RUnlock()
		return id
	}
	// Capture the route for account "A"
	raid := getAccRouteID("A")
	if raid == 0 {
		t.Fatal("Did not find route for account A")
	}

	getRouteIDForAcc := func(acc string) uint64 {
		s := srva
		a, _ := s.LookupAccount(acc)
		if a == nil {
			return 0
		}
		a.mu.RLock()
		pidx := a.routePoolIdx
		a.mu.RUnlock()
		var id uint64
		s.mu.RLock()
		// For this test, we will take a single remote, say srvb
		srvbId := srvb.ID()
		if conns, ok := s.routes[srvbId]; ok {
			if r := conns[pidx]; r != nil {
				r.mu.Lock()
				id = r.cid
				r.mu.Unlock()
			}
		}
		s.mu.RUnlock()
		return id
	}
	rbid := getRouteIDForAcc("B")
	if rbid == 0 {
		t.Fatal("Did not find route for account B")
	}
	rcid := getRouteIDForAcc("C")
	if rcid == 0 {
		t.Fatal("Did not find route for account C")
	}
	rdid := getRouteIDForAcc("D")
	if rdid == 0 {
		t.Fatal("Did not find route for account D")
	}

	sendAndRecv := func(msg string) {
		t.Helper()
		for accIdx := 0; accIdx < 9; accIdx += 3 {
			natsPub(t, conns[accIdx], "bar", []byte(msg))
			m := natsNexMsg(t, subs[accIdx+1], time.Second)
			checkMsg := func(m *nats.Msg, subj string) {
				t.Helper()
				if string(m.Data) != msg {
					t.Fatalf("For accIdx=%v, subject %q, expected message %q, got %q", accIdx, subj, msg, m.Data)
				}
			}
			checkMsg(m, "bar")
			natsPub(t, conns[accIdx+1], "baz", []byte(msg))
			m = natsNexMsg(t, subs[accIdx+2], time.Second)
			checkMsg(m, "baz")
			natsPub(t, conns[accIdx+2], "foo", []byte(msg))
			m = natsNexMsg(t, subs[accIdx], time.Second)
			checkMsg(m, "foo")
		}
	}
	sendAndRecv("0")

	// Now add accounts "B" and "D" and do a config reload.
	reloadUpdateConfig(t, srva, confA, fmt.Sprintf(confATemplate, "pool_size: 3", "accounts: [\"A\",\"B\",\"D\"]"))

	// reloadClusterPoolAndAccounts must not let srva's Reload() return until the
	// remotes have confirmed they processed the per-account route additions.
	for _, s := range []*Server{srvb, srvc} {
		for _, acc := range []string{"B", "D"} {
			s.mu.RLock()
			_, hasRoute := s.accRoutes[acc]
			a, found := s.accounts.Load(acc)
			s.mu.RUnlock()
			if !hasRoute {
				t.Fatalf("%s.Reload() returned before remote %s set up the per-account route for %q; the route confirmation wait was skipped", srva, s, acc)
			}
			if !found {
				t.Fatalf("Remote %s does not know account %q", s, acc)
			}
			acc := a.(*Account)
			acc.mu.RLock()
			rpi := acc.routePoolIdx
			acc.mu.RUnlock()
			if rpi != accTransitioningToDedicatedRoute {
				t.Fatalf("%s.Reload() returned before remote %s marked account %q transitioning (routePoolIdx=%d); the route confirmation wait was skipped", srva, s, acc.Name, rpi)
			}
		}
	}

	// Even before reloading srvb and srvc, we should already have per-account
	// routes for accounts B and D being established. The accounts routePoolIdx
	// should be marked as transitioning.
	checkAccPoolIdx := func(s *Server, acc string, expected int) {
		t.Helper()
		checkFor(t, 2*time.Second, 50*time.Millisecond, func() error {
			s.mu.RLock()
			defer s.mu.RUnlock()
			if a, ok := s.accounts.Load(acc); ok {
				acc := a.(*Account)
				acc.mu.RLock()
				rpi := acc.routePoolIdx
				acc.mu.RUnlock()
				if rpi != expected {
					return fmt.Errorf("Server %q - Account %q routePoolIdx should be %v, but is %v", s, acc, expected, rpi)
				}
				return nil
			}
			return fmt.Errorf("Server %q - Account %q not found", s, acc)
		})
	}
	checkRoutePerAccAlreadyEstablished := func(s *Server, acc string) {
		t.Helper()
		checkFor(t, 2*time.Second, 50*time.Millisecond, func() error {
			s.mu.RLock()
			defer s.mu.RUnlock()
			if _, ok := s.accRoutes[acc]; !ok {
				return fmt.Errorf("Route for account %q still not established", acc)
			}
			return nil
		})
		checkAccPoolIdx(s, acc, accTransitioningToDedicatedRoute)
	}
	// Check srvb and srvc for both accounts.
	for _, s := range []*Server{srvb, srvc} {
		for _, acc := range []string{"B", "D"} {
			checkRoutePerAccAlreadyEstablished(s, acc)
		}
	}
	// On srva, the accounts should already have their routePoolIdx set to
	// the accDedicatedRoute value.
	for _, acc := range []string{"B", "D"} {
		checkAccPoolIdx(srva, acc, accDedicatedRoute)
	}
	// Now reload the other servers
	reloadUpdateConfig(t, srvb, confB, fmt.Sprintf(confBCTemplate, "B", optsA.Cluster.Port, "pool_size: 3", "accounts: [\"A\",\"B\",\"D\"]"))
	reloadUpdateConfig(t, srvc, confC, fmt.Sprintf(confBCTemplate, "C", optsA.Cluster.Port, "pool_size: 3", "accounts: [\"A\",\"B\",\"D\"]"))

	checkCluster()
	// Now check that the accounts B and D are no longer transitioning
	for _, s := range []*Server{srva, srvb, srvc} {
		for _, acc := range []string{"B", "D"} {
			checkAccPoolIdx(s, acc, accDedicatedRoute)
		}
	}

	checkRouteForADidNotChange := func() {
		t.Helper()
		if id := getAccRouteID("A"); id != raid {
			t.Fatalf("Route id for account 'A' was %d, is now %d", raid, id)
		}
	}
	// Verify that the route for account "A" did not change.
	checkRouteForADidNotChange()

	// Verify that account "B" has now its own route
	if id := getAccRouteID("B"); id == 0 {
		t.Fatal("Did not find route for account B")
	}
	// Same for "D".
	if id := getAccRouteID("D"); id == 0 {
		t.Fatal("Did not find route for account D")
	}

	checkRouteStillPresent := func(id uint64) {
		t.Helper()
		srva.mu.RLock()
		defer srva.mu.RUnlock()
		srvbId := srvb.ID()
		for _, r := range srva.routes[srvbId] {
			if r != nil {
				r.mu.Lock()
				found := r.cid == id
				r.mu.Unlock()
				if found {
					return
				}
			}
		}
		t.Fatalf("Route id %v has been disconnected", id)
	}
	// Verify that routes that were dealing with "B", and "D" were not disconnected.
	// Of course, since "C" was not involved, that route should still be present too.
	checkRouteStillPresent(rbid)
	checkRouteStillPresent(rcid)
	checkRouteStillPresent(rdid)

	sendAndRecv("1")

	// Now remove "B" and "D" and verify that route for "A" did not change.
	reloadUpdateConfig(t, srva, confA, fmt.Sprintf(confATemplate, "pool_size: 3", "accounts: [\"A\"]"))
	reloadUpdateConfig(t, srvb, confB, fmt.Sprintf(confBCTemplate, "B", optsA.Cluster.Port, "pool_size: 3", "accounts: [\"A\"]"))
	reloadUpdateConfig(t, srvc, confC, fmt.Sprintf(confBCTemplate, "C", optsA.Cluster.Port, "pool_size: 3", "accounts: [\"A\"]"))

	checkCluster()

	// Verify that the route for account "A" did not change.
	checkRouteForADidNotChange()

	// Verify that there is no dedicated route for account "B"
	if id := getAccRouteID("B"); id != 0 {
		t.Fatal("Should not have found a route for account B")
	}
	// It should instead be in one of the pooled route, and same
	// than it was before.
	if id := getRouteIDForAcc("B"); id != rbid {
		t.Fatalf("Account B's route was %d, it is now %d", rbid, id)
	}
	// Same for "D"
	if id := getAccRouteID("D"); id != 0 {
		t.Fatal("Should not have found a route for account D")
	}
	if id := getRouteIDForAcc("D"); id != rdid {
		t.Fatalf("Account D's route was %d, it is now %d", rdid, id)
	}

	sendAndRecv("2")

	// Finally, change pool size and make sure that routes handling B, C and D
	// were disconnected/reconnected, and that A did not change.
	reloadUpdateConfig(t, srva, confA, fmt.Sprintf(confATemplate, "pool_size: 5", "accounts: [\"A\"]"))
	reloadUpdateConfig(t, srvb, confB, fmt.Sprintf(confBCTemplate, "B", optsA.Cluster.Port, "pool_size: 5", "accounts: [\"A\"]"))
	reloadUpdateConfig(t, srvc, confC, fmt.Sprintf(confBCTemplate, "C", optsA.Cluster.Port, "pool_size: 5", "accounts: [\"A\"]"))

	checkCluster()

	checkRouteForADidNotChange()

	checkRouteDisconnected := func(acc string, oldID uint64) {
		t.Helper()
		if id := getRouteIDForAcc(acc); id == oldID {
			t.Fatalf("Route that was handling account %q did not change", acc)
		}
	}
	checkRouteDisconnected("B", rbid)
	checkRouteDisconnected("C", rcid)
	checkRouteDisconnected("D", rdid)

	sendAndRecv("3")

	// Now check that there were no duplicates and that all subs have 0 pending messages.
	for i, sub := range subs {
		if n, _, _ := sub.Pending(); n != 0 {
			t.Fatalf("Expected 0 pending messages, got %v for accIdx=%d sub=%q", n, i, sub.Subject)
		}
	}
}

func TestConfigReloadRoutePoolAndPerAccountNoPanicIfFirstAdded(t *testing.T) {
	tmpl := `
		port: -1
		server_name: "%s"
		accounts {
			A { users: [{user: "user1", password: "pwd"}] }
			B { users: [{user: "user2", password: "pwd"}] }
		}
		cluster {
				name: "local"
				listen: 127.0.0.1:-1
				pool_size: 2
				%s
				%s
		}
		no_sys_acc: true
	`
	conf1 := createConfFile(t, fmt.Appendf(nil, tmpl, "A", _EMPTY_, _EMPTY_))
	s1, o1 := RunServerWithConfig(conf1)
	defer s1.Shutdown()

	route := fmt.Sprintf("routes: [\"nats://127.0.0.1:%d\"]", o1.Cluster.Port)
	conf2 := createConfFile(t, fmt.Appendf(nil, tmpl, "B", _EMPTY_, route))
	s2, _ := RunServerWithConfig(conf2)
	defer s2.Shutdown()

	checkClusterFormed(t, s1, s2)

	reloadUpdateConfig(t, s1, conf1, fmt.Sprintf(tmpl, "A", "accounts:[\"A\"]", _EMPTY_))
	reloadUpdateConfig(t, s2, conf2, fmt.Sprintf(tmpl, "B", "accounts:[\"A\"]", route))

	time.Sleep(50 * time.Millisecond)
	checkClusterFormed(t, s1, s2)
}

func TestConfigReloadRoutePoolCannotBeDisabledIfAccountsPresent(t *testing.T) {
	tmpl := `
		port: -1
		server_name: "%s"
		accounts {
			A { users: [{user: "user1", password: "pwd"}] }
			B { users: [{user: "user2", password: "pwd"}] }
		}
		cluster {
				name: "local"
				listen: 127.0.0.1:-1
				%s
				%s
				%s
		}
	`
	conf1 := createConfFile(t, []byte(fmt.Sprintf(tmpl, "A", "accounts: [\"A\"]", _EMPTY_, _EMPTY_)))
	s1, o1 := RunServerWithConfig(conf1)
	defer s1.Shutdown()

	conf2 := createConfFile(t, []byte(fmt.Sprintf(tmpl, "B", "accounts: [\"A\"]", _EMPTY_,
		fmt.Sprintf("routes: [\"nats://127.0.0.1:%d\"]", o1.Cluster.Port))))
	s2, _ := RunServerWithConfig(conf2)
	defer s2.Shutdown()

	checkClusterFormed(t, s1, s2)

	err := os.WriteFile(conf1, []byte(fmt.Sprintf(tmpl, "A", "accounts: [\"A\"]", "pool_size: -1", _EMPTY_)), 0666)
	require_NoError(t, err)
	if err := s1.Reload(); err == nil || !strings.Contains(err.Error(), "accounts") {
		t.Fatalf("Expected error regarding presence of accounts, got %v", err)
	}

	// Now remove the accounts too and reload, this should work
	reloadUpdateConfig(t, s1, conf1, fmt.Sprintf(tmpl, "A", _EMPTY_, "pool_size: -1", _EMPTY_))
	reloadUpdateConfig(t, s2, conf2, fmt.Sprintf(tmpl, "B", _EMPTY_, "pool_size: -1", fmt.Sprintf("routes: [\"nats://127.0.0.1:%d\"]", o1.Cluster.Port)))
	checkClusterFormed(t, s1, s2)

	ncs2 := natsConnect(t, s2.ClientURL(), nats.UserInfo("user1", "pwd"))
	defer ncs2.Close()
	sub := natsSubSync(t, ncs2, "foo")
	checkSubInterest(t, s1, "A", "foo", time.Second)

	ncs1 := natsConnect(t, s1.ClientURL(), nats.UserInfo("user1", "pwd"))
	defer ncs1.Close()
	natsPub(t, ncs1, "foo", []byte("hello"))
	natsNexMsg(t, sub, time.Second)

	// Wait a bit and make sure there are no duplicates
	time.Sleep(50 * time.Millisecond)
	if n, _, _ := sub.Pending(); n != 0 {
		t.Fatalf("Expected no pending messages, got %v", n)
	}

	// Finally, verify that the system account is no longer bound to
	// a dedicated route. For that matter, s.accRoutes should be nil.
	for _, s := range []*Server{s1, s2} {
		sys := s.SystemAccount()
		if sys == nil {
			t.Fatal("No system account found")
		}
		sys.mu.RLock()
		rpi := sys.routePoolIdx
		sys.mu.RUnlock()
		if rpi != 0 {
			t.Fatalf("Server %q - expected account's routePoolIdx to be 0, got %v", s, rpi)
		}
		s.mu.RLock()
		arNil := s.accRoutes == nil
		s.mu.RUnlock()
		if !arNil {
			t.Fatalf("Server %q - accRoutes expected to be nil, it was not", s)
		}
	}
}

func TestConfigReloadRoutePoolAndPerAccountWithOlderServer(t *testing.T) {
	confATemplate := `
		port: -1
		server_name: "A"
		accounts {
				A { users: [{user: "user1", password: "pwd"}] }
		}
		cluster {
				name: "local"
				listen: 127.0.0.1:-1
				%s
				%s
		}
	`
	confA := createConfFile(t, []byte(fmt.Sprintf(confATemplate, "pool_size: 3", _EMPTY_)))
	srva, optsA := RunServerWithConfig(confA)
	defer srva.Shutdown()

	confBCTemplate := `
		port: -1
		server_name: "%s"
		accounts {
				A { users: [{user: "user1", password: "pwd"}] }
		}
		cluster {
				listen: 127.0.0.1:-1
				name: "local"
				routes = [
						"nats://127.0.0.1:%d"
				]
				%s
				%s
		}
	`
	confB := createConfFile(t, []byte(fmt.Sprintf(confBCTemplate, "B", optsA.Cluster.Port, "pool_size: 3", _EMPTY_)))
	srvb, _ := RunServerWithConfig(confB)
	defer srvb.Shutdown()
	confC := createConfFile(t, []byte(fmt.Sprintf(confBCTemplate, "C", optsA.Cluster.Port, "pool_size: -1", _EMPTY_)))
	srvc, _ := RunServerWithConfig(confC)
	defer srvc.Shutdown()

	checkClusterFormed(t, srva, srvb, srvc)

	// Create a connection and sub on B and C
	ncB := natsConnect(t, srvb.ClientURL(), nats.UserInfo("user1", "pwd"))
	defer ncB.Close()
	subB := natsSubSync(t, ncB, "foo")

	ncC := natsConnect(t, srvc.ClientURL(), nats.UserInfo("user1", "pwd"))
	defer ncC.Close()
	subC := natsSubSync(t, ncC, "bar")

	// Check that on server B, there is interest on "bar" for account A
	// (coming from server C), and on server C, there is interest on "foo"
	// for account A (coming from server B).
	checkCluster := func() {
		t.Helper()
		checkClusterFormed(t, srva, srvb, srvc)
		checkSubInterest(t, srvb, "A", "bar", 2*time.Second)
		checkSubInterest(t, srvc, "A", "foo", 2*time.Second)
	}
	checkCluster()

	sendAndRecv := func(msg string) {
		t.Helper()
		natsPub(t, ncB, "bar", []byte(msg))
		if m := natsNexMsg(t, subC, time.Second); string(m.Data) != msg {
			t.Fatalf("Expected message %q on %q, got %q", msg, "bar", m.Data)
		}
		natsPub(t, ncC, "foo", []byte(msg))
		if m := natsNexMsg(t, subB, time.Second); string(m.Data) != msg {
			t.Fatalf("Expected message %q on %q, got %q", msg, "foo", m.Data)
		}
	}
	sendAndRecv("0")

	// Now add account "A" and do a config reload. We do this only on
	// server srva and srb since server C really does not change.
	reloadUpdateConfig(t, srva, confA, fmt.Sprintf(confATemplate, "pool_size: 3", "accounts: [\"A\"]"))
	reloadUpdateConfig(t, srvb, confB, fmt.Sprintf(confBCTemplate, "B", optsA.Cluster.Port, "pool_size: 3", "accounts: [\"A\"]"))
	checkCluster()
	sendAndRecv("1")

	// Remove "A" from the accounts list
	reloadUpdateConfig(t, srva, confA, fmt.Sprintf(confATemplate, "pool_size: 3", _EMPTY_))
	reloadUpdateConfig(t, srvb, confB, fmt.Sprintf(confBCTemplate, "B", optsA.Cluster.Port, "pool_size: 3", _EMPTY_))
	checkCluster()
	sendAndRecv("2")

	// Change the pool size
	reloadUpdateConfig(t, srva, confA, fmt.Sprintf(confATemplate, "pool_size: 5", _EMPTY_))
	reloadUpdateConfig(t, srvb, confB, fmt.Sprintf(confBCTemplate, "B", optsA.Cluster.Port, "pool_size: 5", _EMPTY_))
	checkCluster()
	sendAndRecv("3")

	// Add account "A" and change the pool size
	reloadUpdateConfig(t, srva, confA, fmt.Sprintf(confATemplate, "pool_size: 4", "accounts: [\"A\"]"))
	reloadUpdateConfig(t, srvb, confB, fmt.Sprintf(confBCTemplate, "B", optsA.Cluster.Port, "pool_size: 4", "accounts: [\"A\"]"))
	checkCluster()
	sendAndRecv("4")

	// Remove account "A" and change the pool size
	reloadUpdateConfig(t, srva, confA, fmt.Sprintf(confATemplate, "pool_size: 3", _EMPTY_))
	reloadUpdateConfig(t, srvb, confB, fmt.Sprintf(confBCTemplate, "B", optsA.Cluster.Port, "pool_size: 3", _EMPTY_))
	checkCluster()
	sendAndRecv("5")
}

func TestConfigReloadRoutePoolAndPerAccountNoDuplicateSub(t *testing.T) {
	confATemplate := `
		port: -1
		server_name: "A"
		accounts {
				A { users: [{user: "user1", password: "pwd"}] }
		}
		cluster {
				name: "local"
				listen: 127.0.0.1:-1
				pool_size: 3
				%s
		}
	`
	confA := createConfFile(t, []byte(fmt.Sprintf(confATemplate, _EMPTY_)))
	srva, optsA := RunServerWithConfig(confA)
	defer srva.Shutdown()

	confBCTemplate := `
		port: -1
		server_name: "%s"
		accounts {
				A { users: [{user: "user1", password: "pwd"}] }
		}
		cluster {
				listen: 127.0.0.1:-1
				name: "local"
				routes = [
						"nats://127.0.0.1:%d"
				]
				pool_size: 3
				%s
		}
	`
	confB := createConfFile(t, []byte(fmt.Sprintf(confBCTemplate, "B", optsA.Cluster.Port, _EMPTY_)))
	srvb, _ := RunServerWithConfig(confB)
	defer srvb.Shutdown()
	confC := createConfFile(t, []byte(fmt.Sprintf(confBCTemplate, "C", optsA.Cluster.Port, _EMPTY_)))
	srvc, _ := RunServerWithConfig(confC)
	defer srvc.Shutdown()

	checkClusterFormed(t, srva, srvb, srvc)

	ncC := natsConnect(t, srvc.ClientURL(), nats.UserInfo("user1", "pwd"))
	defer ncC.Close()

	ch := make(chan struct{})
	wg := sync.WaitGroup{}
	wg.Add(1)
	var subs []*nats.Subscription
	go func() {
		defer wg.Done()
		// Limit the number of subscriptions. From experimentation, the issue would
		// arise around subscriptions ~700.
		for i := 0; i < 1000; i++ {
			if sub, err := ncC.SubscribeSync(fmt.Sprintf("foo.%d", i)); err == nil {
				subs = append(subs, sub)
			}
			select {
			case <-ch:
				return
			default:
				if i%100 == 0 {
					time.Sleep(5 * time.Millisecond)
				}
			}
		}
	}()

	// Wait a tiny bit before doing the configuration reload.
	time.Sleep(100 * time.Millisecond)

	reloadUpdateConfig(t, srva, confA, fmt.Sprintf(confATemplate, "accounts: [\"A\"]"))
	reloadUpdateConfig(t, srvb, confB, fmt.Sprintf(confBCTemplate, "B", optsA.Cluster.Port, "accounts: [\"A\"]"))
	reloadUpdateConfig(t, srvc, confC, fmt.Sprintf(confBCTemplate, "C", optsA.Cluster.Port, "accounts: [\"A\"]"))

	checkClusterFormed(t, srva, srvb, srvc)

	close(ch)
	wg.Wait()

	for _, sub := range subs {
		checkSubInterest(t, srvb, "A", sub.Subject, 500*time.Millisecond)
	}

	ncB := natsConnect(t, srvb.ClientURL(), nats.UserInfo("user1", "pwd"))
	defer ncB.Close()

	for _, sub := range subs {
		natsPub(t, ncB, sub.Subject, []byte("hello"))
	}

	// Now make sure that there is only 1 pending message for each sub.
	// Wait a bit to give a chance to duplicate messages to arrive if
	// there was a bug that would lead to a sub on each route (the pooled
	// and the per-account)
	time.Sleep(250 * time.Millisecond)
	for _, sub := range subs {
		if n, _, _ := sub.Pending(); n != 1 {
			t.Fatalf("Expected only 1 message for subscription on %q, got %v", sub.Subject, n)
		}
	}
}

func TestConfigReloadGlobalAccountWithMappingAndJetStream(t *testing.T) {
	tmpl := `
		listen: 127.0.0.1:-1
		server_name: %s
		jetstream: {max_mem_store: 256MB, max_file_store: 2GB, store_dir: '%s'}

		mappings {
			subj.orig: subj.mapped.before.reload
		}

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
	c := createJetStreamClusterWithTemplate(t, tmpl, "R3S", 3)
	defer c.shutdown()

	nc, js := jsClientConnect(t, c.randomServer())
	defer nc.Close()

	// Verify that mapping works
	checkMapping := func(expectedSubj string) {
		t.Helper()
		sub := natsSubSync(t, nc, "subj.>")
		defer sub.Unsubscribe()
		natsPub(t, nc, "subj.orig", nil)
		msg := natsNexMsg(t, sub, time.Second)
		if msg.Subject != expectedSubj {
			t.Fatalf("Expected subject to have been mapped to %q, got %q", expectedSubj, msg.Subject)
		}
	}
	checkMapping("subj.mapped.before.reload")

	// Create a stream and check that we can get the INFO
	_, err := js.AddStream(&nats.StreamConfig{
		Name:      "TEST",
		Replicas:  3,
		Subjects:  []string{"foo"},
		Retention: nats.InterestPolicy,
	})
	require_NoError(t, err)
	c.waitOnStreamLeader(globalAccountName, "TEST")

	_, err = js.StreamInfo("TEST")
	require_NoError(t, err)

	// Change mapping on all servers and issue reload
	for i, s := range c.servers {
		opts := c.opts[i]
		content, err := os.ReadFile(opts.ConfigFile)
		require_NoError(t, err)
		reloadUpdateConfig(t, s, opts.ConfigFile, strings.Replace(string(content), "subj.mapped.before.reload", "subj.mapped.after.reload", 1))
	}
	// Make sure the cluster is still formed
	checkClusterFormed(t, c.servers...)
	// Now repeat the test for the subject mapping and stream info
	checkMapping("subj.mapped.after.reload")
	_, err = js.StreamInfo("TEST")
	require_NoError(t, err)
}

func TestConfigReloadRouteCompression(t *testing.T) {
	org := testDefaultClusterCompression
	testDefaultClusterCompression = _EMPTY_
	defer func() { testDefaultClusterCompression = org }()

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
	conf2 := createConfFile(t, []byte(fmt.Sprintf(tmpl, "B", routes, _EMPTY_)))
	s2, _ := RunServerWithConfig(conf2)
	defer s2.Shutdown()

	// Run a 3rd server but make it as if it was an old server. We want to
	// make sure that reload of s1 and s2 will not affect routes from s3 to
	// s1/s2 because these do not support compression.
	conf3 := createConfFile(t, []byte(fmt.Sprintf(tmpl, "C", routes, "compression: \"not supported\"")))
	s3, _ := RunServerWithConfig(conf3)
	defer s3.Shutdown()

	checkClusterFormed(t, s1, s2, s3)

	// Collect routes' cid from servers so we can check if routes are
	// recreated when they should and are not when they should not.
	collect := func(s *Server) map[uint64]struct{} {
		m := make(map[uint64]struct{})
		s.mu.RLock()
		defer s.mu.RUnlock()
		s.forEachRoute(func(r *client) {
			r.mu.Lock()
			m[r.cid] = struct{}{}
			r.mu.Unlock()
		})
		return m
	}
	s1RouteIDs := collect(s1)
	s2RouteIDs := collect(s2)
	s3ID := s3.ID()

	servers := []*Server{s1, s2}
	checkCompMode := func(s1Expected, s2Expected string, shouldBeNew bool) {
		t.Helper()
		// We wait a bit to make sure that we have routes closed before
		// checking that the cluster has (re)formed.
		time.Sleep(100 * time.Millisecond)
		// First, make sure that the cluster is formed
		checkClusterFormed(t, s1, s2, s3)
		// Then check that all routes are with the expected mode. We need to
		// possibly wait a bit since there is negotiation going on.
		checkFor(t, 2*time.Second, 50*time.Millisecond, func() error {
			for _, s := range servers {
				var err error
				s.mu.RLock()
				s.forEachRoute(func(r *client) {
					if err != nil {
						return
					}
					r.mu.Lock()
					var exp string
					var m map[uint64]struct{}
					if r.route.remoteID == s3ID {
						exp = CompressionNotSupported
					} else if s == s1 {
						exp = s1Expected
					} else {
						exp = s2Expected
					}
					if s == s1 {
						m = s1RouteIDs
					} else {
						m = s2RouteIDs
					}
					_, present := m[r.cid]
					cm := r.route.compression
					r.mu.Unlock()
					if cm != exp {
						err = fmt.Errorf("Expected route %v for server %s to have compression mode %q, got %q", r, s, exp, cm)
					}
					sbn := shouldBeNew
					if exp == CompressionNotSupported {
						// Override for routes to s3
						sbn = false
					}
					if sbn && present {
						err = fmt.Errorf("Expected route %v for server %s to be a new route, but it was already present", r, s)
					} else if !sbn && !present {
						err = fmt.Errorf("Expected route %v for server %s to not be new", r, s)
					}
				})
				s.mu.RUnlock()
				if err != nil {
					return err
				}
			}
			s1RouteIDs = collect(s1)
			s2RouteIDs = collect(s2)
			return nil
		})
	}
	// Since both started without any compression setting, we default to
	// "accept" which means that a server can accept/switch to compression
	// but not initiate compression, so they should both be "off"
	checkCompMode(CompressionOff, CompressionOff, false)

	// Now reload s1 with "on" (s2_fast), since s2 is *configured* with "accept",
	// they should both be CompressionS2Fast, even before we reload s2.
	reloadUpdateConfig(t, s1, conf1, fmt.Sprintf(tmpl, "A", _EMPTY_, "compression: on"))
	checkCompMode(CompressionS2Fast, CompressionS2Fast, true)
	// Now reload s2
	reloadUpdateConfig(t, s2, conf2, fmt.Sprintf(tmpl, "B", routes, "compression: on"))
	checkCompMode(CompressionS2Fast, CompressionS2Fast, false)

	// Move on with "better"
	reloadUpdateConfig(t, s1, conf1, fmt.Sprintf(tmpl, "A", _EMPTY_, "compression: s2_better"))
	// s1 should be at "better", but s2 still at "fast"
	checkCompMode(CompressionS2Better, CompressionS2Fast, false)
	// Now reload s2
	reloadUpdateConfig(t, s2, conf2, fmt.Sprintf(tmpl, "B", routes, "compression: s2_better"))
	checkCompMode(CompressionS2Better, CompressionS2Better, false)

	// Move to "best"
	reloadUpdateConfig(t, s1, conf1, fmt.Sprintf(tmpl, "A", _EMPTY_, "compression: s2_best"))
	checkCompMode(CompressionS2Best, CompressionS2Better, false)
	// Now reload s2
	reloadUpdateConfig(t, s2, conf2, fmt.Sprintf(tmpl, "B", routes, "compression: s2_best"))
	checkCompMode(CompressionS2Best, CompressionS2Best, false)

	// Now turn off
	reloadUpdateConfig(t, s1, conf1, fmt.Sprintf(tmpl, "A", _EMPTY_, "compression: off"))
	checkCompMode(CompressionOff, CompressionOff, true)
	// Now reload s2
	reloadUpdateConfig(t, s2, conf2, fmt.Sprintf(tmpl, "B", routes, "compression: off"))
	checkCompMode(CompressionOff, CompressionOff, false)

	// When "off" (and not "accept"), enabling 1 is not enough, the reload
	// has to be done on both to take effect.
	reloadUpdateConfig(t, s1, conf1, fmt.Sprintf(tmpl, "A", _EMPTY_, "compression: s2_better"))
	checkCompMode(CompressionOff, CompressionOff, true)
	// Now reload s2
	reloadUpdateConfig(t, s2, conf2, fmt.Sprintf(tmpl, "B", routes, "compression: s2_better"))
	checkCompMode(CompressionS2Better, CompressionS2Better, true)

	// Try now to have different ones
	reloadUpdateConfig(t, s1, conf1, fmt.Sprintf(tmpl, "A", _EMPTY_, "compression: s2_best"))
	// S1 should be "best" but S2 should have stayed at "better"
	checkCompMode(CompressionS2Best, CompressionS2Better, false)

	// If we remove the compression setting, it defaults to "accept", which
	// in that case we want to have a negotiation and use the remote's compression
	// level. So connections should be re-created.
	reloadUpdateConfig(t, s1, conf1, fmt.Sprintf(tmpl, "A", _EMPTY_, _EMPTY_))
	checkCompMode(CompressionS2Better, CompressionS2Better, true)

	// To avoid flapping, add a little sleep here to make sure we have things
	// settled before reloading s2.
	time.Sleep(100 * time.Millisecond)
	// And if we do the same with s2, then we will end-up with no compression.
	reloadUpdateConfig(t, s2, conf2, fmt.Sprintf(tmpl, "B", routes, _EMPTY_))
	checkCompMode(CompressionOff, CompressionOff, true)
}

func TestConfigReloadRouteCompressionS2Auto(t *testing.T) {
	// This test checks s2_auto specific behavior. It makes sure that we update
	// only if the rtt_thresholds and current RTT value warrants a change and
	// also that we actually save in c.route.compression the actual compression
	// level (not s2_auto).
	tmpl1 := `
		port: -1
		server_name: "A"
		cluster {
			port: -1
			name: "local"
			pool_size: -1
			compression: {mode: s2_auto, rtt_thresholds: [%s]}
		}
	`
	conf1 := createConfFile(t, []byte(fmt.Sprintf(tmpl1, "50ms, 100ms, 150ms")))
	s1, o1 := RunServerWithConfig(conf1)
	defer s1.Shutdown()

	conf2 := createConfFile(t, []byte(fmt.Sprintf(`
		port: -1
		server_name: "B"
		cluster {
			port: -1
			name: "local"
			pool_size: -1
			compression: s2_fast
			routes: ["nats://127.0.0.1:%d"]
		}
	`, o1.Cluster.Port)))
	s2, _ := RunServerWithConfig(conf2)
	defer s2.Shutdown()

	checkClusterFormed(t, s1, s2)

	getCompInfo := func() (string, io.Writer) {
		var cm string
		var cw io.Writer
		s1.mu.RLock()
		// There should be only 1 route...
		s1.forEachRemote(func(r *client) {
			r.mu.Lock()
			cm = r.route.compression
			cw = r.out.cw
			r.mu.Unlock()
		})
		s1.mu.RUnlock()
		return cm, cw
	}
	// Capture the s2 writer from s1 to s2
	cm, cw := getCompInfo()

	// We do a reload but really the mode is still s2_auto (even if the current
	// compression level may be "uncompressed", "better", etc.. so we don't
	// expect the writer to have changed.
	reloadUpdateConfig(t, s1, conf1, fmt.Sprintf(tmpl1, "100ms, 200ms, 300ms"))
	if ncm, ncw := getCompInfo(); ncm != cm || ncw != cw {
		t.Fatalf("Expected compression info to have stayed the same, was %q - %p, got %q - %p", cm, cw, ncm, ncw)
	}
}

func TestConfigReloadLeafNodeCompression(t *testing.T) {
	org := testDefaultLeafNodeCompression
	testDefaultLeafNodeCompression = _EMPTY_
	defer func() { testDefaultLeafNodeCompression = org }()

	tmpl1 := `
		port: -1
		server_name: "A"
		leafnodes {
			port: -1
			%s
		}
	`
	conf1 := createConfFile(t, []byte(fmt.Sprintf(tmpl1, "compression: accept")))
	s1, o1 := RunServerWithConfig(conf1)
	defer s1.Shutdown()

	port := o1.LeafNode.Port

	tmpl2 := `
		port: -1
		server_name: "%s"
		leafnodes {
			remotes [
				{
					url: "nats://127.0.0.1:%d"
					%s
				}
			]
		}
	`
	conf2 := createConfFile(t, []byte(fmt.Sprintf(tmpl2, "B", port, "compression: accept")))
	s2, _ := RunServerWithConfig(conf2)
	defer s2.Shutdown()

	// Run a 3rd server but make it as if it was an old server. We want to
	// make sure that reload of s1 and s2 will not affect leafnodes from s3 to
	// s1/s2 because these do not support compression.
	conf3 := createConfFile(t, []byte(fmt.Sprintf(tmpl2, "C", port, "compression: \"not supported\"")))
	s3, _ := RunServerWithConfig(conf3)
	defer s3.Shutdown()

	checkLeafNodeConnected(t, s2)
	checkLeafNodeConnected(t, s3)
	checkLeafNodeConnectedCount(t, s1, 2)

	// Collect leafnodes' cid from servers so we can check if connections are
	// recreated when they should and are not when they should not.
	collect := func(s *Server) map[uint64]struct{} {
		m := make(map[uint64]struct{})
		s.mu.RLock()
		defer s.mu.RUnlock()
		for _, l := range s.leafs {
			l.mu.Lock()
			m[l.cid] = struct{}{}
			l.mu.Unlock()
		}
		return m
	}
	s1LeafNodeIDs := collect(s1)
	s2LeafNodeIDs := collect(s2)

	servers := []*Server{s1, s2}
	checkCompMode := func(s1Expected, s2Expected string, shouldBeNew bool) {
		t.Helper()
		// We wait a bit to make sure that we have leaf connections closed
		// before checking that they are properly reconnected.
		time.Sleep(100 * time.Millisecond)
		checkLeafNodeConnected(t, s2)
		checkLeafNodeConnected(t, s3)
		checkLeafNodeConnectedCount(t, s1, 2)
		// Check that all leafnodes are with the expected mode. We need to
		// possibly wait a bit since there is negotiation going on.
		checkFor(t, 2*time.Second, 50*time.Millisecond, func() error {
			for _, s := range servers {
				var err error
				s.mu.RLock()
				for _, l := range s.leafs {
					l.mu.Lock()
					var exp string
					var m map[uint64]struct{}
					if l.leaf.remoteServer == "C" {
						exp = CompressionNotSupported
					} else if s == s1 {
						exp = s1Expected
					} else {
						exp = s2Expected
					}
					if s == s1 {
						m = s1LeafNodeIDs
					} else {
						m = s2LeafNodeIDs
					}
					_, present := m[l.cid]
					cm := l.leaf.compression
					l.mu.Unlock()
					if cm != exp {
						err = fmt.Errorf("Expected leaf %v for server %s to have compression mode %q, got %q", l, s, exp, cm)
					}
					sbn := shouldBeNew
					if exp == CompressionNotSupported {
						// Override for routes to s3
						sbn = false
					}
					if sbn && present {
						err = fmt.Errorf("Expected leaf %v for server %s to be a new leaf, but it was already present", l, s)
					} else if !sbn && !present {
						err = fmt.Errorf("Expected leaf %v for server %s to not be new", l, s)
					}
					if err != nil {
						break
					}
				}
				s.mu.RUnlock()
				if err != nil {
					return err
				}
			}
			s1LeafNodeIDs = collect(s1)
			s2LeafNodeIDs = collect(s2)
			return nil
		})
	}
	// Since both started with compression "accept", they should both be set to "off"
	checkCompMode(CompressionOff, CompressionOff, false)

	// Now reload s1 with "on" (s2_auto), since s2 is *configured* with "accept",
	// s1 should be "uncompressed" (due to low RTT), and s2 is in that case set
	// to s2_fast.
	reloadUpdateConfig(t, s1, conf1, fmt.Sprintf(tmpl1, "compression: on"))
	checkCompMode(CompressionS2Uncompressed, CompressionS2Fast, true)
	// Now reload s2
	reloadUpdateConfig(t, s2, conf2, fmt.Sprintf(tmpl2, "B", port, "compression: on"))
	checkCompMode(CompressionS2Uncompressed, CompressionS2Uncompressed, false)

	// Move on with "better"
	reloadUpdateConfig(t, s1, conf1, fmt.Sprintf(tmpl1, "compression: s2_better"))
	// s1 should be at "better", but s2 still at "uncompressed"
	checkCompMode(CompressionS2Better, CompressionS2Uncompressed, false)
	// Now reload s2
	reloadUpdateConfig(t, s2, conf2, fmt.Sprintf(tmpl2, "B", port, "compression: s2_better"))
	checkCompMode(CompressionS2Better, CompressionS2Better, false)

	// Move to "best"
	reloadUpdateConfig(t, s1, conf1, fmt.Sprintf(tmpl1, "compression: s2_best"))
	checkCompMode(CompressionS2Best, CompressionS2Better, false)
	// Now reload s2
	reloadUpdateConfig(t, s2, conf2, fmt.Sprintf(tmpl2, "B", port, "compression: s2_best"))
	checkCompMode(CompressionS2Best, CompressionS2Best, false)

	// Now turn off
	reloadUpdateConfig(t, s1, conf1, fmt.Sprintf(tmpl1, "compression: off"))
	checkCompMode(CompressionOff, CompressionOff, true)
	// Now reload s2
	reloadUpdateConfig(t, s2, conf2, fmt.Sprintf(tmpl2, "B", port, "compression: off"))
	checkCompMode(CompressionOff, CompressionOff, false)

	// When "off" (and not "accept"), enabling 1 is not enough, the reload
	// has to be done on both to take effect.
	reloadUpdateConfig(t, s1, conf1, fmt.Sprintf(tmpl1, "compression: s2_better"))
	checkCompMode(CompressionOff, CompressionOff, true)
	// Now reload s2
	reloadUpdateConfig(t, s2, conf2, fmt.Sprintf(tmpl2, "B", port, "compression: s2_better"))
	checkCompMode(CompressionS2Better, CompressionS2Better, true)

	// Try now to have different ones
	reloadUpdateConfig(t, s1, conf1, fmt.Sprintf(tmpl1, "compression: s2_best"))
	// S1 should be "best" but S2 should have stayed at "better"
	checkCompMode(CompressionS2Best, CompressionS2Better, false)

	// Change the setting to "accept", which in that case we want to have a
	// negotiation and use the remote's compression level. So connections
	// should be re-created.
	reloadUpdateConfig(t, s1, conf1, fmt.Sprintf(tmpl1, "compression: accept"))
	checkCompMode(CompressionS2Better, CompressionS2Better, true)

	// To avoid flapping, add a little sleep here to make sure we have things
	// settled before reloading s2.
	time.Sleep(100 * time.Millisecond)
	// And if we do the same with s2, then we will end-up with no compression.
	reloadUpdateConfig(t, s2, conf2, fmt.Sprintf(tmpl2, "B", port, "compression: accept"))
	checkCompMode(CompressionOff, CompressionOff, true)

	// Now remove completely and we should default to s2_auto, which means that
	// s1 should be at "uncompressed" and s2 to "fast".
	reloadUpdateConfig(t, s1, conf1, fmt.Sprintf(tmpl1, _EMPTY_))
	checkCompMode(CompressionS2Uncompressed, CompressionS2Fast, true)

	// Now with s2, both will be "uncompressed"
	reloadUpdateConfig(t, s2, conf2, fmt.Sprintf(tmpl2, "B", port, _EMPTY_))
	checkCompMode(CompressionS2Uncompressed, CompressionS2Uncompressed, false)
}

func TestConfigReloadLeafNodeCompressionS2Auto(t *testing.T) {
	// This test checks s2_auto specific behavior. It makes sure that we update
	// only if the rtt_thresholds and current RTT value warrants a change and
	// also that we actually save in c.leaf.compression the actual compression
	// level (not s2_auto).
	tmpl1 := `
		port: -1
		server_name: "A"
		leafnodes {
			port: -1
			compression: {mode: s2_auto, rtt_thresholds: [%s]}
		}
	`
	conf1 := createConfFile(t, []byte(fmt.Sprintf(tmpl1, "50ms, 100ms, 150ms")))
	s1, o1 := RunServerWithConfig(conf1)
	defer s1.Shutdown()

	conf2 := createConfFile(t, []byte(fmt.Sprintf(`
		port: -1
		server_name: "B"
		leafnodes {
			remotes [{ url: "nats://127.0.0.1:%d", compression: s2_fast}]
		}
	`, o1.LeafNode.Port)))
	s2, _ := RunServerWithConfig(conf2)
	defer s2.Shutdown()

	checkLeafNodeConnected(t, s2)

	getCompInfo := func() (string, io.Writer) {
		var cm string
		var cw io.Writer
		s1.mu.RLock()
		// There should be only 1 leaf...
		for _, l := range s1.leafs {
			l.mu.Lock()
			cm = l.leaf.compression
			cw = l.out.cw
			l.mu.Unlock()
		}
		s1.mu.RUnlock()
		return cm, cw
	}
	// Capture the s2 writer from s1 to s2
	cm, cw := getCompInfo()

	// We do a reload but really the mode is still s2_auto (even if the current
	// compression level may be "uncompressed", "better", etc.. so we don't
	// expect the writer to have changed.
	reloadUpdateConfig(t, s1, conf1, fmt.Sprintf(tmpl1, "100ms, 200ms, 300ms"))
	if ncm, ncw := getCompInfo(); ncm != cm || ncw != cw {
		t.Fatalf("Expected compression info to have stayed the same, was %q - %p, got %q - %p", cm, cw, ncm, ncw)
	}
}

func TestConfigReloadGetRemoteLeafOpts(t *testing.T) {
	u1, err := url.Parse("nats://127.0.0.1:1234")
	require_NoError(t, err)
	u2, err := url.Parse("nats://127.0.0.1:1235")
	require_NoError(t, err)
	u3, err := url.Parse("nats://127.0.0.1:1236")
	require_NoError(t, err)

	rlo1 := &RemoteLeafOpts{URLs: []*url.URL{u1}}
	rlo2 := &RemoteLeafOpts{URLs: []*url.URL{u2}}
	rlo3 := &RemoteLeafOpts{URLs: []*url.URL{u3}}

	for _, test := range []struct {
		name    string
		search  *RemoteLeafOpts
		listIn  []*RemoteLeafOpts
		result  *RemoteLeafOpts
		listOut []*RemoteLeafOpts
	}{
		{"list empty", rlo1, nil, nil, nil},
		{"list of one and found", rlo1, []*RemoteLeafOpts{rlo1}, rlo1, nil},
		{"list of one and not found", rlo2, []*RemoteLeafOpts{rlo1}, nil, []*RemoteLeafOpts{rlo1}},
		{"list of two and found at pos 1", rlo1, []*RemoteLeafOpts{rlo1, rlo2}, rlo1, []*RemoteLeafOpts{rlo2}},
		{"list of two and found at pos 2", rlo2, []*RemoteLeafOpts{rlo1, rlo2}, rlo2, []*RemoteLeafOpts{rlo1}},
		{"list of two and not found", rlo3, []*RemoteLeafOpts{rlo1, rlo2}, nil, []*RemoteLeafOpts{rlo1, rlo2}},
		// When finding an element, we take the last from the list and move it in place of the found one and reduce the list size.
		{"list of three and found at pos 1", rlo1, []*RemoteLeafOpts{rlo1, rlo2, rlo3}, rlo1, []*RemoteLeafOpts{rlo3, rlo2}},
		{"list of three and found at pos 2", rlo2, []*RemoteLeafOpts{rlo1, rlo2, rlo3}, rlo2, []*RemoteLeafOpts{rlo1, rlo3}},
		{"list of three and found at pos 3", rlo3, []*RemoteLeafOpts{rlo1, rlo2, rlo3}, rlo3, []*RemoteLeafOpts{rlo1, rlo2}},
	} {
		t.Run(test.name, func(t *testing.T) {
			rlo, list := getRemoteLeafOpts(test.search.name(), test.listIn)
			require_True(t, reflect.DeepEqual(list, test.listOut))
			require_Equal(t, rlo, test.result)
		})
	}
}

func TestConfigReloadGetLeafNodeOptionsChanges(t *testing.T) {
	u, err := url.Parse("nats://127.0.0.1:7222")
	require_NoError(t, err)
	remote := &RemoteLeafOpts{
		URLs:        []*url.URL{u},
		Compression: CompressionOpts{Mode: CompressionS2Fast},
	}
	cfg := &leafNodeCfg{RemoteLeafOpts: remote}
	s := &Server{}
	s.leafRemoteCfgs = make(map[*leafNodeCfg]struct{})
	s.leafRemoteCfgs[cfg] = struct{}{}

	changedRemotes := make(map[*leafNodeCfg]*remoteLeafOption)
	changedRemotes[cfg] = &remoteLeafOption{opts: remote}

	changedRemotesTLSFirst := make(map[*leafNodeCfg]*remoteLeafOption)
	tlsFirstRemote := *remote
	tlsFirstRemote.TLSHandshakeFirst = true
	changedRemotesTLSFirst[cfg] = &remoteLeafOption{tlsFirstChanged: true, opts: &tlsFirstRemote}

	changedRemotesCompression := make(map[*leafNodeCfg]*remoteLeafOption)
	compressionRemote := *remote
	compressionRemote.Compression = CompressionOpts{Mode: CompressionS2Best}
	changedRemotesCompression[cfg] = &remoteLeafOption{compressionChanged: true, opts: &compressionRemote}

	changedRemotesDisabled := make(map[*leafNodeCfg]*remoteLeafOption)
	disabledRemote := *remote
	disabledRemote.Disabled = true
	changedRemotesDisabled[cfg] = &remoteLeafOption{disabledChanged: true, opts: &disabledRemote}

	u2, err := url.Parse("nats://127.0.0.1:7223")
	require_NoError(t, err)
	addedRemote := &RemoteLeafOpts{
		URLs:         []*url.URL{u2},
		Compression:  CompressionOpts{Mode: CompressionS2Fast},
		LocalAccount: "A",
	}

	addedRemoteDueToDifferentAccount := &RemoteLeafOpts{
		URLs:         []*url.URL{u},
		Compression:  CompressionOpts{Mode: CompressionS2Fast},
		LocalAccount: "A",
	}
	addedRemoteDueToDifferentCreds := &RemoteLeafOpts{
		URLs:        []*url.URL{u},
		Compression: CompressionOpts{Mode: CompressionS2Fast},
		Credentials: "credsfile",
	}
	addedRemoteDueToDifferentNkey := &RemoteLeafOpts{
		URLs:        []*url.URL{u},
		Compression: CompressionOpts{Mode: CompressionS2Fast},
		Credentials: "SUACJN3OSKWWPQXME4JUNFJ3PARXPO657GGNWNU7PK7G3AUQQYHLW26XH4",
	}

	acc1 := &Account{Name: "A1"}
	acc2 := &Account{Name: "A2"}

	for _, test := range []struct {
		name   string
		genCfg func() (*Server, *LeafNodeOpts, *LeafNodeOpts)
		lo     *leafNodeOption
		errTxt string
	}{
		{
			"no change no remote",
			func() (*Server, *LeafNodeOpts, *LeafNodeOpts) {
				ts := &Server{}
				old := &LeafNodeOpts{
					Users:             []*User{{Username: "a", Password: "pwd"}},
					Compression:       CompressionOpts{Mode: CompressionS2Fast},
					TLSHandshakeFirst: true,
				}
				new := &LeafNodeOpts{
					Users:             []*User{{Username: "a", Password: "pwd"}},
					Compression:       CompressionOpts{Mode: CompressionS2Fast},
					TLSHandshakeFirst: true,
				}
				return ts, old, new
			},
			nil,
			_EMPTY_,
		},
		{
			"no change with remote",
			func() (*Server, *LeafNodeOpts, *LeafNodeOpts) {
				ts := &Server{}
				ts.leafRemoteCfgs = maps.Clone(s.leafRemoteCfgs)
				old := &LeafNodeOpts{
					Users:             []*User{{Username: "a", Password: "pwd"}},
					Compression:       CompressionOpts{Mode: CompressionS2Fast},
					TLSHandshakeFirst: true,
					Remotes:           []*RemoteLeafOpts{remote},
				}
				r := *remote
				new := &LeafNodeOpts{
					Users:             []*User{{Username: "a", Password: "pwd"}},
					Compression:       CompressionOpts{Mode: CompressionS2Fast},
					TLSHandshakeFirst: true,
					Remotes:           []*RemoteLeafOpts{&r},
				}
				return ts, old, new
			},
			&leafNodeOption{changed: changedRemotes},
			_EMPTY_,
		},
		{
			"users order changed but equivalent",
			func() (*Server, *LeafNodeOpts, *LeafNodeOpts) {
				ts := &Server{}
				old := &LeafNodeOpts{
					Users: []*User{
						{Username: "a", Password: "pwd"},
						{Username: "b", Password: "pwd"},
					},
					Compression:       CompressionOpts{Mode: CompressionS2Fast},
					TLSHandshakeFirst: true,
				}
				new := &LeafNodeOpts{
					Users: []*User{
						{Username: "b", Password: "pwd"},
						{Username: "a", Password: "pwd"},
					},
					Compression:       CompressionOpts{Mode: CompressionS2Fast},
					TLSHandshakeFirst: true,
				}
				return ts, old, new
			},
			nil,
			_EMPTY_,
		},
		{
			"users different length",
			func() (*Server, *LeafNodeOpts, *LeafNodeOpts) {
				ts := &Server{}
				old := &LeafNodeOpts{
					Users:             []*User{{Username: "a", Password: "pwd"}},
					Compression:       CompressionOpts{Mode: CompressionS2Fast},
					TLSHandshakeFirst: true,
				}
				new := &LeafNodeOpts{
					Users: []*User{
						{Username: "a", Password: "pwd"},
						{Username: "b", Password: "pwd"},
					},
					Compression:       CompressionOpts{Mode: CompressionS2Fast},
					TLSHandshakeFirst: true,
				}
				return ts, old, new
			},
			nil,
			"field \"Users\": old=",
		},
		{
			"users different pwd",
			func() (*Server, *LeafNodeOpts, *LeafNodeOpts) {
				ts := &Server{}
				old := &LeafNodeOpts{
					Users:             []*User{{Username: "a", Password: "pwd"}},
					Compression:       CompressionOpts{Mode: CompressionS2Fast},
					TLSHandshakeFirst: true,
				}
				new := &LeafNodeOpts{
					Users:             []*User{{Username: "a", Password: "changedpwd"}},
					Compression:       CompressionOpts{Mode: CompressionS2Fast},
					TLSHandshakeFirst: true,
				}
				return ts, old, new
			},
			nil,
			"field \"Users\": old=",
		},
		{
			"users different account",
			func() (*Server, *LeafNodeOpts, *LeafNodeOpts) {
				ts := &Server{}
				old := &LeafNodeOpts{
					Users:             []*User{{Username: "a", Password: "pwd", Account: acc1}},
					Compression:       CompressionOpts{Mode: CompressionS2Fast},
					TLSHandshakeFirst: true,
				}
				new := &LeafNodeOpts{
					Users:             []*User{{Username: "a", Password: "pwd", Account: acc2}},
					Compression:       CompressionOpts{Mode: CompressionS2Fast},
					TLSHandshakeFirst: true,
				}
				return ts, old, new
			},
			nil,
			"field \"Users\": old=",
		},
		{
			"user not found",
			func() (*Server, *LeafNodeOpts, *LeafNodeOpts) {
				ts := &Server{}
				old := &LeafNodeOpts{
					Users:             []*User{{Username: "a", Password: "pwd"}},
					Compression:       CompressionOpts{Mode: CompressionS2Fast},
					TLSHandshakeFirst: true,
				}
				new := &LeafNodeOpts{
					Users:             []*User{{Username: "b", Password: "pwd"}},
					Compression:       CompressionOpts{Mode: CompressionS2Fast},
					TLSHandshakeFirst: true,
				}
				return ts, old, new
			},
			nil,
			"field \"Users\": old=",
		},
		{
			"unsupported change",
			func() (*Server, *LeafNodeOpts, *LeafNodeOpts) {
				ts := &Server{}
				old := &LeafNodeOpts{
					Users:             []*User{{Username: "a", Password: "pwd"}},
					Compression:       CompressionOpts{Mode: CompressionS2Fast},
					TLSHandshakeFirst: true,
					Port:              1234,
				}
				new := &LeafNodeOpts{
					Users:             []*User{{Username: "a", Password: "pwd"}},
					Compression:       CompressionOpts{Mode: CompressionS2Fast},
					TLSHandshakeFirst: true,
					// Pick some field that is not Compression or TLSHandshakeFirst
					Port: 5678,
				}
				return ts, old, new
			},
			nil,
			"field \"Port\": old=1234, new=5678",
		},
		{
			"tlsfirst changed",
			func() (*Server, *LeafNodeOpts, *LeafNodeOpts) {
				ts := &Server{}
				old := &LeafNodeOpts{
					Users:             []*User{{Username: "a", Password: "pwd"}},
					Compression:       CompressionOpts{Mode: CompressionS2Fast},
					TLSHandshakeFirst: true,
				}
				new := &LeafNodeOpts{
					Users:             []*User{{Username: "a", Password: "pwd"}},
					Compression:       CompressionOpts{Mode: CompressionS2Fast},
					TLSHandshakeFirst: false,
				}
				return ts, old, new
			},
			&leafNodeOption{tlsFirstChanged: true},
			_EMPTY_,
		},
		{
			"compression changed",
			func() (*Server, *LeafNodeOpts, *LeafNodeOpts) {
				ts := &Server{}
				old := &LeafNodeOpts{
					Users:             []*User{{Username: "a", Password: "pwd"}},
					Compression:       CompressionOpts{Mode: CompressionS2Fast},
					TLSHandshakeFirst: true,
				}
				new := &LeafNodeOpts{
					Users:             []*User{{Username: "a", Password: "pwd"}},
					Compression:       CompressionOpts{Mode: CompressionS2Best},
					TLSHandshakeFirst: true,
				}
				return ts, old, new
			},
			&leafNodeOption{compressionChanged: true},
			_EMPTY_,
		},
		{
			"tlsfirst and compression changed",
			func() (*Server, *LeafNodeOpts, *LeafNodeOpts) {
				ts := &Server{}
				old := &LeafNodeOpts{
					Users:             []*User{{Username: "a", Password: "pwd"}},
					Compression:       CompressionOpts{Mode: CompressionS2Fast},
					TLSHandshakeFirst: true,
				}
				new := &LeafNodeOpts{
					Users:             []*User{{Username: "a", Password: "pwd"}},
					Compression:       CompressionOpts{Mode: CompressionS2Best},
					TLSHandshakeFirst: false,
				}
				return ts, old, new
			},
			&leafNodeOption{tlsFirstChanged: true, compressionChanged: true},
			_EMPTY_,
		},
		{
			"unsupported remote change",
			func() (*Server, *LeafNodeOpts, *LeafNodeOpts) {
				ts := &Server{}
				ts.leafRemoteCfgs = maps.Clone(s.leafRemoteCfgs)
				old := &LeafNodeOpts{
					Users:             []*User{{Username: "a", Password: "pwd"}},
					Compression:       CompressionOpts{Mode: CompressionS2Fast},
					TLSHandshakeFirst: true,
					Remotes:           []*RemoteLeafOpts{remote},
				}
				r := *remote
				// Set something that cannot be changed.
				r.Hub = true
				new := &LeafNodeOpts{
					Users:             []*User{{Username: "a", Password: "pwd"}},
					Compression:       CompressionOpts{Mode: CompressionS2Best},
					TLSHandshakeFirst: false,
					Remotes:           []*RemoteLeafOpts{&r},
				}
				return ts, old, new
			},
			nil,
			fmt.Sprintf("remote %s: field %q: old=false, new=true", remote.safeName(), "Hub"),
		},
		{
			"remote tlsfirst changed",
			func() (*Server, *LeafNodeOpts, *LeafNodeOpts) {
				ts := &Server{}
				ts.leafRemoteCfgs = maps.Clone(s.leafRemoteCfgs)
				old := &LeafNodeOpts{
					Users:       []*User{{Username: "a", Password: "pwd"}},
					Compression: CompressionOpts{Mode: CompressionS2Fast},
					Remotes:     []*RemoteLeafOpts{remote},
				}
				new := &LeafNodeOpts{
					Users: []*User{{Username: "a", Password: "pwd"}},
					// At the same time, change compression for the LeafNodeOpts block.
					Compression: CompressionOpts{Mode: CompressionS2Best},
					Remotes:     []*RemoteLeafOpts{&tlsFirstRemote},
				}
				return ts, old, new
			},
			&leafNodeOption{
				compressionChanged: true,
				changed:            changedRemotesTLSFirst,
			},
			_EMPTY_,
		},
		{
			"remote compression changed",
			func() (*Server, *LeafNodeOpts, *LeafNodeOpts) {
				ts := &Server{}
				ts.leafRemoteCfgs = maps.Clone(s.leafRemoteCfgs)
				old := &LeafNodeOpts{
					Users:       []*User{{Username: "a", Password: "pwd"}},
					Compression: CompressionOpts{Mode: CompressionS2Fast},
					Remotes:     []*RemoteLeafOpts{remote},
				}
				new := &LeafNodeOpts{
					Users:       []*User{{Username: "a", Password: "pwd"}},
					Compression: CompressionOpts{Mode: CompressionS2Fast},
					// At the same time, change tls first for the LeafNodeOpts block.
					TLSHandshakeFirst: true,
					Remotes:           []*RemoteLeafOpts{&compressionRemote},
				}
				return ts, old, new
			},
			&leafNodeOption{
				tlsFirstChanged: true,
				changed:         changedRemotesCompression,
			},
			_EMPTY_,
		},
		{
			"remote disabled changed",
			func() (*Server, *LeafNodeOpts, *LeafNodeOpts) {
				ts := &Server{}
				ts.leafRemoteCfgs = maps.Clone(s.leafRemoteCfgs)
				old := &LeafNodeOpts{
					Users:       []*User{{Username: "a", Password: "pwd"}},
					Compression: CompressionOpts{Mode: CompressionS2Fast},
					Remotes:     []*RemoteLeafOpts{remote},
				}
				new := &LeafNodeOpts{
					Users:       []*User{{Username: "a", Password: "pwd"}},
					Compression: CompressionOpts{Mode: CompressionS2Fast},
					// At the same time, change tls first for the LeafNodeOpts block.
					TLSHandshakeFirst: true,
					Remotes:           []*RemoteLeafOpts{&disabledRemote},
				}
				return ts, old, new
			},
			&leafNodeOption{
				tlsFirstChanged: true,
				changed:         changedRemotesDisabled,
			},
			_EMPTY_,
		},
		{
			"remote removed",
			func() (*Server, *LeafNodeOpts, *LeafNodeOpts) {
				ts := &Server{}
				ts.leafRemoteCfgs = maps.Clone(s.leafRemoteCfgs)
				old := &LeafNodeOpts{
					Users:       []*User{{Username: "a", Password: "pwd"}},
					Compression: CompressionOpts{Mode: CompressionS2Fast},
					Remotes:     []*RemoteLeafOpts{remote},
				}
				new := &LeafNodeOpts{
					Users:       []*User{{Username: "a", Password: "pwd"}},
					Compression: CompressionOpts{Mode: CompressionS2Fast},
					// At the same time, change tls first for the LeafNodeOpts block.
					TLSHandshakeFirst: true,
				}
				return ts, old, new
			},
			&leafNodeOption{tlsFirstChanged: true},
			_EMPTY_,
		},
		{
			"remote added",
			func() (*Server, *LeafNodeOpts, *LeafNodeOpts) {
				ts := &Server{}
				ts.leafRemoteCfgs = maps.Clone(s.leafRemoteCfgs)
				old := &LeafNodeOpts{
					Users:       []*User{{Username: "a", Password: "pwd"}},
					Compression: CompressionOpts{Mode: CompressionS2Fast},
					Remotes:     []*RemoteLeafOpts{remote},
				}
				new := &LeafNodeOpts{
					Users:       []*User{{Username: "a", Password: "pwd"}},
					Compression: CompressionOpts{Mode: CompressionS2Fast},
					// At the same time, change tls first for the LeafNodeOpts block.
					TLSHandshakeFirst: true,
					Remotes:           []*RemoteLeafOpts{remote, addedRemote},
				}
				return ts, old, new
			},
			&leafNodeOption{
				tlsFirstChanged: true,
				added:           []*RemoteLeafOpts{addedRemote},
				changed:         changedRemotes,
			},
			_EMPTY_,
		},
		{
			"remote added due to different account",
			func() (*Server, *LeafNodeOpts, *LeafNodeOpts) {
				ts := &Server{}
				ts.leafRemoteCfgs = maps.Clone(s.leafRemoteCfgs)
				old := &LeafNodeOpts{
					Users:       []*User{{Username: "a", Password: "pwd"}},
					Compression: CompressionOpts{Mode: CompressionS2Fast},
					Remotes:     []*RemoteLeafOpts{remote},
				}
				new := &LeafNodeOpts{
					Users:       []*User{{Username: "a", Password: "pwd"}},
					Compression: CompressionOpts{Mode: CompressionS2Fast},
					// At the same time, change tls first for the LeafNodeOpts block.
					TLSHandshakeFirst: true,
					Remotes:           []*RemoteLeafOpts{addedRemoteDueToDifferentAccount},
				}
				return ts, old, new
			},
			&leafNodeOption{
				tlsFirstChanged: true,
				added:           []*RemoteLeafOpts{addedRemoteDueToDifferentAccount},
			},
			_EMPTY_,
		},
		{
			"remote added due to different creds",
			func() (*Server, *LeafNodeOpts, *LeafNodeOpts) {
				ts := &Server{}
				ts.leafRemoteCfgs = maps.Clone(s.leafRemoteCfgs)
				old := &LeafNodeOpts{
					Users:       []*User{{Username: "a", Password: "pwd"}},
					Compression: CompressionOpts{Mode: CompressionS2Fast},
					Remotes:     []*RemoteLeafOpts{remote},
				}
				new := &LeafNodeOpts{
					Users:       []*User{{Username: "a", Password: "pwd"}},
					Compression: CompressionOpts{Mode: CompressionS2Fast},
					// At the same time, change tls first for the LeafNodeOpts block.
					TLSHandshakeFirst: true,
					Remotes:           []*RemoteLeafOpts{addedRemoteDueToDifferentCreds},
				}
				return ts, old, new
			},
			&leafNodeOption{
				tlsFirstChanged: true,
				added:           []*RemoteLeafOpts{addedRemoteDueToDifferentCreds},
			},
			_EMPTY_,
		},
		{
			"remote added due to different nkey",
			func() (*Server, *LeafNodeOpts, *LeafNodeOpts) {
				ts := &Server{}
				ts.leafRemoteCfgs = maps.Clone(s.leafRemoteCfgs)
				old := &LeafNodeOpts{
					Users:       []*User{{Username: "a", Password: "pwd"}},
					Compression: CompressionOpts{Mode: CompressionS2Fast},
					Remotes:     []*RemoteLeafOpts{remote},
				}
				new := &LeafNodeOpts{
					Users:       []*User{{Username: "a", Password: "pwd"}},
					Compression: CompressionOpts{Mode: CompressionS2Fast},
					// At the same time, change tls first for the LeafNodeOpts block.
					TLSHandshakeFirst: true,
					Remotes:           []*RemoteLeafOpts{addedRemoteDueToDifferentNkey},
				}
				return ts, old, new
			},
			&leafNodeOption{
				tlsFirstChanged: true,
				added:           []*RemoteLeafOpts{addedRemoteDueToDifferentNkey},
			},
			_EMPTY_,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			s, old, new := test.genCfg()
			lo, err := getLeafNodeOptionsChanges(s, old, new)
			if test.errTxt != _EMPTY_ {
				if err == nil || !strings.Contains(err.Error(), test.errTxt) {
					t.Fatalf("Expected error containing %q, got %q", test.errTxt, err)
				}
				return
			}
			if !reflect.DeepEqual(lo, test.lo) {
				t.Fatalf("Expected result to be %+v, got %+v", test.lo, lo)
			}
		})
	}
}

func TestConfigReloadLeafNodeUnsupportedChangesFail(t *testing.T) {
	conf1 := createConfFile(t, []byte(`
		port: -1
		server_name: "A"
		leafnodes {
			port: -1
		}
	`))
	s1, o1 := RunServerWithConfig(conf1)
	defer s1.Shutdown()

	tmpl2 := `
		port: -1
		server_name: "B"
		leafnodes {
			port: %d
			remotes [
				{ url: "nats://127.0.0.1:%d", deny_imports: ["%s"], deny_exports: ["%s"]%s }
				%s
			]
		}
	`
	conf2 := createConfFile(t,
		fmt.Appendf(nil, tmpl2, -1, o1.LeafNode.Port, "foo", "bar", _EMPTY_, _EMPTY_))
	s2, o2 := RunServerWithConfig(conf2)
	defer s2.Shutdown()

	checkLeafNodeConnectedCount(t, s2, 1)

	remote := o2.LeafNode.Remotes[0]
	require_NotNil(t, remote)
	remoteName := remote.safeName()
	require_NotEqual(t, remoteName, _EMPTY_)

	checkConfigReload := func(content []byte, errTxt string) {
		t.Helper()
		err := os.WriteFile(conf2, content, 0666)
		require_NoError(t, err)
		err = s2.Reload()
		require_Error(t, err)
		require_Contains(t, err.Error(), errTxt)
	}

	// Make a change that is not supported in the main block
	checkConfigReload(fmt.Appendf(nil, tmpl2, 1234, o1.LeafNode.Port, "foo", "bar", _EMPTY_, _EMPTY_),
		"not supported for LeafNode: field \"Port\": old=")

	// Now make one in the remote block
	checkConfigReload(fmt.Appendf(nil, tmpl2, -1, o1.LeafNode.Port, "foo", "bar", ", hub: true", _EMPTY_),
		fmt.Sprintf("not supported for LeafNode: remote %s: field %q: old=false, new=true",
			remoteName, "Hub"))

	// Check we now correctly fail a change in deny_imports
	checkConfigReload(fmt.Appendf(nil, tmpl2, -1, o1.LeafNode.Port, "xxx", "bar", _EMPTY_, _EMPTY_),
		fmt.Sprintf("not supported for LeafNode: remote %s: field %q: old=[foo], new=[xxx]",
			remoteName, "DenyImports"))

	// Check we now correctly fail a change in deny_exports
	checkConfigReload(fmt.Appendf(nil, tmpl2, -1, o1.LeafNode.Port, "foo", "xxx", _EMPTY_, _EMPTY_),
		fmt.Sprintf("not supported for LeafNode: remote %s: field %q: old=[bar], new=[xxx]",
			remoteName, "DenyExports"))

	// Try with a duplicate, it should fail too.
	duplicate := fmt.Sprintf(`{ url: "nats://127.0.0.1:%d" }`, o1.LeafNode.Port)
	checkConfigReload(fmt.Appendf(nil, tmpl2, -1, o1.LeafNode.Port, "foo", "bar", _EMPTY_, duplicate),
		fmt.Sprintf("duplicate remote %s", remoteName))

	// Now repeat with s.ReloadOptions()
	checkOptionsReload := func(o *Options, errTxt string) {
		t.Helper()
		err := s2.ReloadOptions(o)
		require_Error(t, err)
		require_Contains(t, err.Error(), errTxt)
	}
	err := os.WriteFile(conf2, fmt.Appendf(nil, tmpl2, -1, o1.LeafNode.Port, "foo", "bar", _EMPTY_, _EMPTY_), 0666)
	require_NoError(t, err)
	o := LoadConfig(conf2)
	o.LeafNode.Port = 1234
	checkOptionsReload(o, "not supported for LeafNode: field \"Port\"")

	o = LoadConfig(conf2)
	o.LeafNode.Remotes[0].Hub = true
	checkOptionsReload(o,
		fmt.Sprintf("not supported for LeafNode: remote %s: field %q: old=false, new=true",
			remoteName, "Hub"))

	o = LoadConfig(conf2)
	duplicateRemote := *o.LeafNode.Remotes[0]
	o.LeafNode.Remotes = append(o.LeafNode.Remotes, &duplicateRemote)
	checkOptionsReload(o, fmt.Sprintf("duplicate remote %s", remoteName))

	o = LoadConfig(conf2)
	o.LeafNode.Remotes[0].DenyImports = []string{"xxx"}
	checkOptionsReload(o, fmt.Sprintf("not supported for LeafNode: remote %s: field %q: old=[foo], new=[xxx]",
		remoteName, "DenyImports"))

	o = LoadConfig(conf2)
	o.LeafNode.Remotes[0].DenyExports = []string{"xxx"}
	checkOptionsReload(o, fmt.Sprintf("not supported for LeafNode: remote %s: field %q: old=[bar], new=[xxx]",
		remoteName, "DenyExports"))
}

func TestConfigReloadLeafNodeRemotesReorderOk(t *testing.T) {
	conf1 := createConfFile(t, []byte(`
		port: -1
		server_name: "A"
		accounts {
			A: {users:[{user: "A", password: "pwd"}]}
			B: {users:[{user: "B", password: "pwd"}]}
		}
		leafnodes {
			port: -1
		}
	`))
	s1, o1 := RunServerWithConfig(conf1)
	defer s1.Shutdown()

	tmpl2 := `
		port: -1
		server_name: "B"
		leafnodes {
			remotes [
				{ url: "nats://%s:pwd@127.0.0.1:%d"%s }
				{ url: "nats://%s:pwd@127.0.0.1:%d" }
			]
		}
	`
	conf2 := createConfFile(t, []byte(fmt.Sprintf(tmpl2,
		"A", o1.LeafNode.Port, _EMPTY_, "B", o1.LeafNode.Port)))
	s2, _ := RunServerWithConfig(conf2)
	defer s2.Shutdown()

	checkLeafNodeConnectedCount(t, s2, 2)

	// Flip the order of the remotes, and disable "B". We should have only
	// one leafnode connected on s1, and it should be for account "A".
	reloadUpdateConfig(t, s2, conf2, fmt.Sprintf(tmpl2,
		"B", o1.LeafNode.Port, ", disabled: true", "A", o1.LeafNode.Port))

	checkLeafNodeConnectedCount(t, s1, 1)
	checkLeafNodeConnectedCount(t, s2, 1)
	var leaf *client
	s2.mu.RLock()
	for _, l := range s2.leafs {
		leaf = l
		break
	}
	s2.mu.RUnlock()
	require_NotNil(t, leaf)
	leaf.mu.Lock()
	acc := leaf.leaf.remoteAccName
	leaf.mu.Unlock()
	require_Equal(t, "A", acc)
}

func TestConfigReloadAddRemoveRemoteLeafNodes(t *testing.T) {
	conf1 := createConfFile(t, []byte(`
		port: -1
		server_name: "A"
		leafnodes {
			port: -1
		}
	`))
	s1, o1 := RunServerWithConfig(conf1)
	defer s1.Shutdown()

	tmpl2 := `
		port: -1
		server_name: "B"
		accounts {
			A: {users:[{user: "A", password: "pwd"}]}
			B: {users:[{user: "B", password: "pwd"}]}
			C: {users:[{user: "C", password: "pwd"}]}
		}
		leafnodes {
			remotes [
				%s
				%s
				%s
			]
		}
	`
	remoteTmpl := fmt.Sprintf(`{ url: "nats://127.0.0.1:%d"`, o1.LeafNode.Port) + ", account=%q}"
	accA := fmt.Sprintf(remoteTmpl, "A")
	accB := fmt.Sprintf(remoteTmpl, "B")
	accC := fmt.Sprintf(remoteTmpl, "C")
	conf2 := createConfFile(t, fmt.Appendf(nil, tmpl2, accA, _EMPTY_, _EMPTY_))
	s2, _ := RunServerWithConfig(conf2)
	defer s2.Shutdown()

	checkLeafs := func(accs []string) {
		t.Helper()
		s2.mu.RLock()
		defer s2.mu.RUnlock()
		require_Len(t, len(accs), len(s2.leafRemoteCfgs))
		for _, acc := range accs {
			var ok bool
			for cfg := range s2.leafRemoteCfgs {
				cfg.RLock()
				ok = cfg.LocalAccount == acc
				cfg.RUnlock()
				if ok {
					break
				}
			}
			if !ok {
				t.Fatalf("Did not find account %q in the remote configs", acc)
			}
		}
	}

	checkLeafNodeConnectedCount(t, s2, 1)
	checkLeafs([]string{"A"})

	// Add a remote with local account "B" and "C"
	reloadUpdateConfig(t, s2, conf2, fmt.Sprintf(tmpl2, accA, accB, accC))

	checkLeafNodeConnectedCount(t, s2, 3)
	checkLeafs([]string{"A", "B", "C"})

	// Remove remote with local account "A" and "C"
	reloadUpdateConfig(t, s2, conf2, fmt.Sprintf(tmpl2, _EMPTY_, accB, _EMPTY_))

	checkLeafNodeConnectedCount(t, s2, 1)
	checkLeafs([]string{"B"})

	// Remove all now
	reloadUpdateConfig(t, s2, conf2, fmt.Sprintf(tmpl2, _EMPTY_, _EMPTY_, _EMPTY_))
	checkLeafNodeConnectedCount(t, s2, 0)
	checkLeafs(nil)
}

func TestConfigReloadAddRemoveRemoteLeafNodesVarz(t *testing.T) {
	conf1 := createConfFile(t, []byte(`
		port: -1
		server_name: "A"
		leafnodes {
			port: -1
		}
	`))
	s1, o1 := RunServerWithConfig(conf1)
	defer s1.Shutdown()

	tmpl2 := `
		port: -1
		http: -1
		server_name: "B"
		accounts {
			A: {users:[{user: "A", password: "pwd"}]}
			B: {users:[{user: "B", password: "pwd"}]}
		}
		leafnodes {
			remotes [
				%s
				%s
			]
		}
	`
	remoteTmpl := fmt.Sprintf(`{ url: "nats://127.0.0.1:%d"`, o1.LeafNode.Port) + ", account=%q}"
	accA := fmt.Sprintf(remoteTmpl, "A")
	accB := fmt.Sprintf(remoteTmpl, "B")
	conf2 := createConfFile(t, fmt.Appendf(nil, tmpl2, accA, _EMPTY_))
	s2, _ := RunServerWithConfig(conf2)
	defer s2.Shutdown()

	checkLeafNodeConnectedCount(t, s2, 1)

	checkRemotes := func(accs ...string) {
		t.Helper()
		// Check both the HTTP endpoint (mode 0), which caches the Varz
		// object, and the programmatic API (mode 1).
		url := fmt.Sprintf("http://127.0.0.1:%d/varz", s2.MonitorAddr().Port)
		for mode := 0; mode < 2; mode++ {
			v := pollVarz(t, s2, mode, url, nil)
			require_Len(t, len(v.LeafNode.Remotes), len(accs))
			for _, acc := range accs {
				var ok bool
				for _, r := range v.LeafNode.Remotes {
					if r.LocalAccount == acc {
						ok = true
						break
					}
				}
				if !ok {
					t.Fatalf("Mode %d: did not find account %q in varz remotes: %+v", mode, acc, v.LeafNode.Remotes)
				}
			}
		}
	}
	// This will also populate the cached s.varz used by the HTTP endpoint.
	checkRemotes("A")

	// Add a remote with local account "B".
	reloadUpdateConfig(t, s2, conf2, fmt.Sprintf(tmpl2, accA, accB))
	checkLeafNodeConnectedCount(t, s2, 2)
	checkRemotes("A", "B")

	// Remove the remote with local account "A".
	reloadUpdateConfig(t, s2, conf2, fmt.Sprintf(tmpl2, _EMPTY_, accB))
	checkLeafNodeConnectedCount(t, s2, 1)
	checkRemotes("B")
}

func TestConfigReloadRemoteLeafNodeNkeyChange(t *testing.T) {
	conf1 := createConfFile(t, []byte(`
		listen: "127.0.0.1:-1"
		server_name: "A"
		leaf {
			listen: 127.0.0.1:-1
			authorization: { nkey: UCSTG5CRF5GEJERAFKUUYRODGABTBVWY2NPE4GGKRQVQOH74PIAKTVKO }
		}
	`))
	s1, o1 := RunServerWithConfig(conf1)
	defer s1.Shutdown()

	tmpl2 := `
		listen: "127.0.0.1:-1"
		server_name: "B"
		leaf {
			reconnect_interval: "50ms"
			remotes: [
				{
					url:  "nats-leaf://127.0.0.1:%d"
					nkey: %s
				}
			]
		}
	`
	conf2 := createConfFile(t, fmt.Appendf(nil, tmpl2, o1.LeafNode.Port,
		"SUAPM67TC4RHQLKBX55NIQXSMATZDOZK6FNEOSS36CAYA7F7TY66LP4BOM"))
	s2, _ := RunServerWithConfig(conf2)
	defer s2.Shutdown()

	// Should not be able to connect...
	time.Sleep(70 * time.Millisecond)
	checkLeafNodeConnectedCount(t, s2, 0)

	reloadUpdateConfig(t, s2, conf2, fmt.Sprintf(tmpl2, o1.LeafNode.Port,
		"SUACJN3OSKWWPQXME4JUNFJ3PARXPO657GGNWNU7PK7G3AUQQYHLW26XH4"))

	checkLeafNodeConnectedCount(t, s2, 1)
}

func TestConfigReloadNoPanicOnShutdown(t *testing.T) {
	tmpl := `
		port: -1
		jetstream: true
		accounts {
			A {
				users: [{user: A, password: pwd}]
				%s
			}
			B {
				users: [{user: B, password: pwd}]
			}
		}
	`
	for i := 0; i < 50; i++ {
		conf := createConfFile(t, []byte(fmt.Sprintf(tmpl, _EMPTY_)))
		s, _ := RunServerWithConfig(conf)
		// Don't use a defer s.Shutdown() here since it would prevent the panic
		// to be reported (but the test would still fail because of a runtime timeout).

		err := os.WriteFile(conf, []byte(fmt.Sprintf(tmpl, "jetstream: true")), 0666)
		require_NoError(t, err)

		wg := sync.WaitGroup{}
		wg.Add(1)
		go func() {
			defer wg.Done()
			time.Sleep(10 * time.Millisecond)
			s.Shutdown()
		}()

		time.Sleep(8 * time.Millisecond)
		err = s.Reload()
		require_NoError(t, err)
		wg.Wait()
	}
}

func TestJetStreamReloadPreservesMaxConcurrentIOOnDisable(t *testing.T) {
	storeDir := t.TempDir()
	initialLimit := 128
	disabledConfig := `listen: 127.0.0.1:-1`
	jsConfig := fmt.Sprintf(`
		listen: 127.0.0.1:-1
		jetstream: {
			max_mem_store: 2MB
			max_file_store: 8MB
			store_dir: '%s'
			max_concurrent_io: %d
		}
	`, storeDir, initialLimit)
	conf := createConfFile(t, []byte(jsConfig))

	s, _ := RunServerWithConfig(conf)
	defer s.Shutdown()

	require_True(t, s.ReadyForConnections(5*time.Second))
	require_Equal(t, s.dios.cap(), initialLimit)
	require_Equal(t, s.getOpts().JetStreamConcurrentIOs, initialLimit)

	// disabling JS is OK
	reloadUpdateConfig(t, s, conf, disabledConfig)
	require_Equal(t, s.dios.cap(), initialLimit)
	require_Equal(t, s.getOpts().JetStreamConcurrentIOs, initialLimit)

	// disabling JS repeatedly is OK
	reloadUpdateConfig(t, s, conf, disabledConfig)
	require_Equal(t, s.dios.cap(), initialLimit)
	require_Equal(t, s.getOpts().JetStreamConcurrentIOs, initialLimit)

	// renabling with the same values is OK
	reloadUpdateConfig(t, s, conf, jsConfig)
	require_Equal(t, s.dios.cap(), initialLimit)
	require_Equal(t, s.getOpts().JetStreamConcurrentIOs, initialLimit)

	// changing value not OK
	jsConfig = strings.Replace(jsConfig,
		fmt.Sprintf("max_concurrent_io: %d", initialLimit),
		"max_concurrent_io: 256", 1)
	require_NoError(t, os.WriteFile(conf, []byte(jsConfig), 0666))
	err := s.Reload()
	require_Error(t, err)
	require_Contains(t, err.Error(), "JetStreamConcurrentIOs")
	require_Equal(t, s.dios.cap(), initialLimit)
	require_Equal(t, s.getOpts().JetStreamConcurrentIOs, initialLimit)
}

func TestJetStreamReloadMaxMemAndStore(t *testing.T) {
	tdir := t.TempDir()
	template := `
		listen: 127.0.0.1:-1
		http: 127.0.0.1:-1
		jetstream {
			max_mem_store: %s
			max_file_store: %s
			store_dir = %q
		}
	`
	conf := createConfFile(t, []byte(fmt.Sprintf(template, "128MB", "128MB", tdir)))

	s, _ := RunServerWithConfig(conf)
	defer s.Shutdown()

	varzURL := fmt.Sprintf("http://127.0.0.1:%d/varz", s.MonitorAddr().Port)

	// Verify initial config.
	cfg := s.JetStreamConfig()
	require_Equal(t, cfg.MaxMemory, 128*1024*1024)
	require_Equal(t, cfg.MaxStore, 128*1024*1024)

	cfg = pollVarz(t, s, 0, varzURL, nil).JetStream.Config
	require_Equal(t, cfg.MaxMemory, 128*1024*1024)
	require_Equal(t, cfg.MaxStore, 128*1024*1024)

	nc, js := jsClientConnect(t, s)
	defer nc.Close()

	scfg := &nats.StreamConfig{MaxBytes: 128 * 1024 * 1024}
	storageTypes := []nats.StorageType{nats.FileStorage, nats.MemoryStorage}
	for _, st := range storageTypes {
		scfg.Name = fmt.Sprintf("TEST-%s", st)
		scfg.Storage = st
		_, err := js.AddStream(scfg)
		require_NoError(t, err)
	}

	for _, st := range storageTypes {
		scfg.Name = fmt.Sprintf("TEST2-%s", st)
		scfg.Storage = st
		_, err := js.AddStream(scfg)
		if st == nats.FileStorage {
			require_Error(t, err, NewJSStorageResourcesExceededError())
		} else {
			require_Error(t, err, NewJSMemoryResourcesExceededError())
		}
	}

	// Reload with increased limits.
	err := os.WriteFile(conf, []byte(fmt.Sprintf(template, "512MB", "512MB", tdir)), 0666)
	require_NoError(t, err)
	err = s.Reload()
	require_NoError(t, err)
	cfg = s.JetStreamConfig()
	require_Equal(t, cfg.MaxMemory, 512*1024*1024)
	require_Equal(t, cfg.MaxStore, 512*1024*1024)

	cfg = pollVarz(t, s, 0, varzURL, nil).JetStream.Config
	require_Equal(t, cfg.MaxMemory, 512*1024*1024)
	require_Equal(t, cfg.MaxStore, 512*1024*1024)

	// We should now be able to create the stream.
	for _, st := range storageTypes {
		scfg.Name = fmt.Sprintf("TEST2-%s", st)
		scfg.Storage = st
		_, err := js.AddStream(scfg)
		require_NoError(t, err)
	}

	// Decreasing the limits should fail.
	err = os.WriteFile(conf, []byte(fmt.Sprintf(template, "511MB", "511MB", tdir)), 0666)
	require_NoError(t, err)
	err = s.Reload()
	require_Error(t, err, errors.New("config reload not supported for decreasing jetstream max memory and store"))

	// Config should remain the same.
	cfg = s.JetStreamConfig()
	require_Equal(t, cfg.MaxMemory, 512*1024*1024)
	require_Equal(t, cfg.MaxStore, 512*1024*1024)

	cfg = pollVarz(t, s, 0, varzURL, nil).JetStream.Config
	require_Equal(t, cfg.MaxMemory, 512*1024*1024)
	require_Equal(t, cfg.MaxStore, 512*1024*1024)

}

func TestConfigReloadJetStreamLimits(t *testing.T) {
	tmpl := `
		listen: 127.0.0.1:-1
		jetstream: {
			store_dir: %q
			limits: {
				default_max_consumers: %d
			}
		}
	`
	storeDir := t.TempDir()
	conf := createConfFile(t, []byte(fmt.Sprintf(tmpl, storeDir, 1)))
	s, _ := RunServerWithConfig(conf)
	defer s.Shutdown()

	nc, js := jsClientConnect(t, s)
	defer nc.Close()

	_, err := js.AddStream(&nats.StreamConfig{Name: "S", Subjects: []string{"foo"}})
	require_NoError(t, err)
	_, err = js.AddConsumer("S", &nats.ConsumerConfig{Durable: "C1", AckPolicy: nats.AckExplicitPolicy})
	require_NoError(t, err)

	// The limit of one consumer is reached.
	_, err = js.AddConsumer("S", &nats.ConsumerConfig{Durable: "C2", AckPolicy: nats.AckExplicitPolicy})
	require_Error(t, err)
	require_Contains(t, err.Error(), "maximum consumers limit reached")

	// Raising the limit must be picked up by a reload.
	reloadUpdateConfig(t, s, conf, fmt.Sprintf(tmpl, storeDir, 2))
	require_Equal(t, s.getOpts().JetStreamLimits.DefaultMaxConsumers, 2)

	_, err = js.AddConsumer("S", &nats.ConsumerConfig{Durable: "C2", AckPolicy: nats.AckExplicitPolicy})
	require_NoError(t, err)

	// As must lowering it again.
	reloadUpdateConfig(t, s, conf, fmt.Sprintf(tmpl, storeDir, 1))
	require_Equal(t, s.getOpts().JetStreamLimits.DefaultMaxConsumers, 1)

	_, err = js.AddConsumer("S", &nats.ConsumerConfig{Durable: "C3", AckPolicy: nats.AckExplicitPolicy})
	require_Error(t, err)
	require_Contains(t, err.Error(), "maximum consumers limit reached")

	// Unset means the server default applies, -1 means unlimited.
	reloadUpdateConfig(t, s, conf, fmt.Sprintf(tmpl, storeDir, -1))
	require_Equal(t, s.getOpts().JetStreamLimits.DefaultMaxConsumers, -1)

	_, err = js.AddConsumer("S", &nats.ConsumerConfig{Durable: "C3", AckPolicy: nats.AckExplicitPolicy})
	require_NoError(t, err)
}

// https://github.com/nats-io/nats-server/issues/8606
func TestConfigReloadDoesNotDisconnectJWTClientSendingNkey(t *testing.T) {
	var err error
	preload := make(map[string]string)

	_, sysPub, sysAC := NewJwtAccountClaim("SYS")
	preload[sysPub], err = sysAC.Encode(oKp)
	require_NoError(t, err)

	aKP, aPub, aAC := NewJwtAccountClaim("A")
	preload[aPub], err = aAC.Encode(oKp)
	require_NoError(t, err)

	preloadConfig, err := json.MarshalIndent(preload, "", " ")
	require_NoError(t, err)

	uKP, err := nkeys.CreateUser()
	require_NoError(t, err)
	uPub, err := uKP.PublicKey()
	require_NoError(t, err)
	uJwt, err := jwt.NewUserClaims(uPub).Encode(aKP)
	require_NoError(t, err)

	content := fmt.Sprintf(`
		listen: 127.0.0.1:-1
		operator: %s
		system_account: %s
		resolver: MEM
		resolver_preload: %s
	`, ojwt, sysPub, preloadConfig)

	s, _, _ := runReloadServerWithContent(t, []byte(content))
	defer s.Shutdown()

	acc, err := s.LookupAccount(aPub)
	require_NoError(t, err)

	c, cr, l := newClientForServer(s)
	defer c.close()

	var info nonceInfo
	require_NoError(t, json.Unmarshal([]byte(l[5:]), &info))
	require_True(t, info.Nonce != _EMPTY_)

	sigraw, err := uKP.Sign([]byte(info.Nonce))
	require_NoError(t, err)
	sig := base64.RawURLEncoding.EncodeToString(sigraw)

	// Send the nkey along with the JWT, like nats.js' jwtAuthenticator does.
	// The Go client does not allow sending both.
	c.parseAsync(fmt.Sprintf("CONNECT {\"jwt\":%q,\"nkey\":%q,\"sig\":%q,\"verbose\":true,\"pedantic\":true}\r\nPING\r\n", uJwt, uPub, sig))
	l, err = cr.ReadString('\n')
	require_NoError(t, err)
	require_True(t, strings.HasPrefix(l, "+OK"))
	l, err = cr.ReadString('\n')
	require_NoError(t, err)
	require_True(t, strings.HasPrefix(l, "PONG"))
	checkClientsCount(t, s, 1)
	require_Equal(t, c.acc, acc)

	// Reloading the same configuration must be a no-op for this client.
	require_NoError(t, s.Reload())

	// The client must not have been disconnected by the reload, nor have been
	// considered as having moved to a different account.
	checkClientsCount(t, s, 1)
	require_Equal(t, c.acc, acc)
	c.parseAsync("PING\r\n")
	l, err = cr.ReadString('\n')
	require_NoError(t, err)
	require_True(t, strings.HasPrefix(l, "PONG"))
}

func TestConfigReloadKeepsJetStreamAPIImportLinkedToSystemExport(t *testing.T) {
	tmpl := jsClusterTempl + `
		authorization {
			users = [
				{user: app, password: pwd}
			]
		}
	`
	c := createJetStreamClusterWithTemplate(t, tmpl, "R3S", 3)
	defer c.shutdown()

	s := c.randomServer()
	nc, js := jsClientConnect(t, s, nats.UserInfo("app", "pwd"))
	defer nc.Close()

	_, err := js.AddStream(&nats.StreamConfig{Name: "TEST", Subjects: []string{"foo"}, Replicas: 3})
	require_NoError(t, err)
	sub, err := js.PullSubscribe("foo", "dur")
	require_NoError(t, err)

	fetch := func() {
		t.Helper()
		for i := 0; i < 50; i++ {
			_, err := js.Publish("foo", []byte("msg"))
			require_NoError(t, err)
			msgs, err := sub.Fetch(1, nats.MaxWait(time.Second))
			require_NoError(t, err)
			for _, m := range msgs {
				require_NoError(t, m.AckSync())
			}
		}
	}
	numResponses := func() int {
		sacc := s.SystemAccount()
		sacc.mu.RLock()
		defer sacc.mu.RUnlock()
		return len(sacc.exports.responses)
	}
	checkImportLinked := func() {
		t.Helper()
		gacc := s.GlobalAccount()
		gacc.mu.RLock()
		defer gacc.mu.RUnlock()
		sis := gacc.imports.services[jsAllAPI]
		require_Len(t, len(sis), 1)
		if se := sis[0].se; se == nil || se.acc != s.SystemAccount() {
			t.Fatalf("Expected %q import to be linked to the system account export, got %+v", jsAllAPI, se)
		}
	}

	fetch()
	checkImportLinked()
	before := numResponses()

	// Any authorization change reconfigures the accounts on reload.
	for _, srv := range c.servers {
		cf := srv.getOpts().ConfigFile
		buf, err := os.ReadFile(cf)
		require_NoError(t, err)
		nbuf := bytes.Replace(buf, []byte("{user: app, password: pwd}"), []byte("{user: app, password: pwd}\n\t\t\t\t{user: other, password: pwd}"), 1)
		require_NoError(t, os.WriteFile(cf, nbuf, defaultFilePerms))
		require_NoError(t, srv.Reload())
	}

	checkImportLinked()
	// Pull requests must still bypass the service import, otherwise each one
	// leaves behind a response service import that never expires.
	fetch()
	require_Equal(t, numResponses(), before)
}

func TestConfigReloadNoRaceWithServiceImports(t *testing.T) {
	kp, err := nkeys.CreateUser()
	require_NoError(t, err)
	pub, _ := kp.PublicKey()

	storeDir := t.TempDir()
	mkOpts := func() *Options {
		return &Options{
			Host:      "127.0.0.1",
			Port:      -1,
			JetStream: true,
			StoreDir:  storeDir,
			NoSigs:    true,
			NoLog:     true,
			Nkeys:     []*NkeyUser{{Nkey: pub}},
		}
	}
	srv := RunServer(mkOpts())
	defer srv.Shutdown()

	sign := func(nonce []byte) ([]byte, error) { return kp.Sign(nonce) }
	nc, js := jsClientConnect(t, srv, nats.Nkey(pub, sign), nats.Timeout(5*time.Second))
	defer nc.Close()

	_, err = js.AddStream(&nats.StreamConfig{Name: "S", Subjects: []string{"js.>"}})
	require_NoError(t, err)
	pull, err := js.PullSubscribe("js.>", "dur")
	require_NoError(t, err)

	stop := make(chan struct{})
	var wg sync.WaitGroup
	for i := 0; i < 4; i++ { // concurrent JetStream publishers ($JS.API service imports)
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
				}
				_, _ = js.Publish("js.x", []byte("x"), nats.AckWait(500*time.Millisecond))
			}
		}()
	}
	wg.Add(1)
	go func() { // consumer fetch loop: delivery is the read side
		defer wg.Done()
		for {
			select {
			case <-stop:
				return
			default:
			}
			msgs, _ := pull.Fetch(20, nats.MaxWait(200*time.Millisecond))
			for _, m := range msgs {
				_ = m.Ack()
			}
		}
	}()
	wg.Add(1)
	errCh := make(chan error, 1)
	go func() { // ReloadOptions loop: the write side
		defer wg.Done()
		tk := time.NewTicker(5 * time.Millisecond)
		defer tk.Stop()
		for {
			select {
			case <-stop:
				return
			case <-tk.C:
				if err := srv.ReloadOptions(mkOpts()); err != nil {
					errCh <- fmt.Errorf("reload error: %v", err)
					return
				}
			}
		}
	}()

	time.Sleep(3 * time.Second)
	close(stop)
	wg.Wait()
	select {
	case err := <-errCh:
		t.Fatal(err)
	default:
	}
}

func TestCalloutInputsChanged(t *testing.T) {
	newOptions := func() *Options {
		return &Options{
			AuthCallout: &AuthCallout{AuthUsers: []string{"auth", "key"}},
			Users: []*User{
				{Username: "auth", Password: "pwd", Account: NewAccount("A")},
				{Username: "other", Password: "pwd", Account: NewAccount("A")},
			},
			Nkeys: []*NkeyUser{
				{Nkey: "key", Account: NewAccount("A")},
				{Nkey: "otherkey", Account: NewAccount("A")},
			},
		}
	}
	for _, test := range []struct {
		name string
		edit func(*Options)
		want bool
	}{
		{"unchanged", func(*Options) {}, false},
		{"tags", func(o *Options) { o.Tags = append(o.Tags, "updated") }, true},
		{"permissions", func(o *Options) {
			o.Users[0].Permissions = &Permissions{Publish: &SubjectPermission{Allow: []string{"foo"}}}
		}, false},
		{"nkey permissions", func(o *Options) { o.Nkeys[0].Permissions = &Permissions{} }, false},
		{"password", func(o *Options) { o.Users[0].Password = "changed" }, true},
		{"other password", func(o *Options) { o.Users[1].Password = "changed" }, true},
		{"account", func(o *Options) { o.Users[0].Account = NewAccount("B") }, true},
		{"account limits", func(o *Options) { o.Users[0].Account.mconns = 10 }, false},
		{"connection types", func(o *Options) { o.Users[0].AllowedConnectionTypes = map[string]struct{}{"STANDARD": {}} }, true},
		{"empty connection types", func(o *Options) { o.Users[0].AllowedConnectionTypes = map[string]struct{}{} }, false},
		{"proxy", func(o *Options) { o.Users[0].ProxyRequired = true }, true},
		{"delegated user deadline", func(o *Options) { o.Users[1].ConnectionDeadline = time.Now().Add(time.Hour) }, true},
		{"delegated user proxy", func(o *Options) { o.Users[1].ProxyRequired = true }, true},
		{"delegated nkey proxy", func(o *Options) { o.Nkeys[1].ProxyRequired = true }, true},
		{"global proxy", func(o *Options) { o.ProxyRequired = true }, true},
		{"removed user", func(o *Options) { o.Users = o.Users[1:] }, true},
		{"removed unrelated user", func(o *Options) { o.Users = o.Users[:1] }, true},
		{"added unrelated nkey", func(o *Options) { o.Nkeys = append(o.Nkeys, &NkeyUser{Nkey: "unrelated"}) }, true},
		{"added unrelated user", func(o *Options) { o.Users = append(o.Users, &User{Username: "new"}) }, true},
		{"removed nkey", func(o *Options) { o.Nkeys = nil }, true},
		{"nkey account", func(o *Options) { o.Nkeys[0].Account = NewAccount("B") }, true},
		{"identity kind", func(o *Options) {
			o.Nkeys = nil
			o.Users = append(o.Users, &User{Username: "key", Account: NewAccount("A")})
		}, true},
		{"user order", func(o *Options) { o.Users[0], o.Users[1] = o.Users[1], o.Users[0] }, false},
		{"single auth user", func(o *Options) { o.Username = "auth" }, true},
		{"single unrelated user", func(o *Options) { o.Username = "other" }, true},
		{"single user password", func(o *Options) { o.Password = "changed" }, true},
		{"token", func(o *Options) { o.Authorization = "changed" }, true},
		{"TLS mapping", func(o *Options) { o.TLSMap = true }, true},
	} {
		for _, allowed := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/allowed_accounts=%v", test.name, allowed), func(t *testing.T) {
				old, updated := newOptions(), newOptions()
				if allowed {
					old.AuthCallout.AllowedAccounts = []string{"A"}
					updated.AuthCallout.AllowedAccounts = []string{"A"}
				}
				test.edit(updated)
				// Match the ordering established by diffOptions.
				for _, o := range []*Options{old, updated} {
					require_NoError(t, imposeOrder(o.Users))
					require_NoError(t, imposeOrder(o.Nkeys))
				}
				require_Equal(t, calloutInputsChanged(old, updated), test.want)
				updated.AuthCallout = nil
				require_False(t, calloutInputsChanged(old, updated))
			})
		}
	}
}

// Readloading the config must not race against startup.
// A race could cause JetStream to appear un-enabled, and the reload would attempt to start it again.
// That would make Start() fail ("jetstream already enabled"), or register every JetStream API handler twice.
func TestConfigReloadDuringStartupWithJetStream(t *testing.T) {
	for i := 0; i < 50; i++ { // Kinda arbitrary count, based on TestConfigReloadNoPanicOnShutdown
		func() {
			opts, _ := newOptionsFromContent(t, []byte(fmt.Sprintf(`
				listen: 127.0.0.1:-1
				jetstream { store_dir: %q }
			`, t.TempDir())))
			opts.NoLog = true
			s, err := NewServer(opts)
			require_NoError(t, err)
			defer s.Shutdown()
			l := &captureFatalLogger{fatalCh: make(chan string, 1)}
			s.SetLogger(l, false, false)

			go s.Start()
			// Reload as soon as Start() is under way, before it has enabled JetStream.
			for !s.isRunning() {
				runtime.Gosched()
			}
			require_NoError(t, s.Reload())

			if !s.ReadyForConnections(5 * time.Second) {
				select {
				case msg := <-l.fatalCh:
					t.Fatalf("Iteration %d: server failed to start after a reload during startup: %s", i, msg)
				default:
					t.Fatalf("Iteration %d: server did not finish starting after a reload during startup", i)
				}
			}
			nc := natsConnect(t, s.ClientURL())
			defer nc.Close()
			resp, err := nc.Request(JSApiAccountInfo, nil, 2*time.Second)
			require_NoError(t, err)
			var info JSApiAccountInfoResponse
			require_NoError(t, json.Unmarshal(resp.Data, &info))
			if info.Error != nil {
				t.Fatalf("Iteration %d: JetStream API request failed after a reload during startup: %+v", i, info.Error)
			}
		}()
	}
}
