// Copyright 2017-2020 The NATS Authors
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
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"reflect"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/nats-io/jwt/v2"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nkeys"
)

func newServerWithConfig(t *testing.T, configFile string) (*Server, *Options, string) {
	t.Helper()
	content, err := ioutil.ReadFile(configFile)
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

func createConfFile(t *testing.T, content []byte) string {
	t.Helper()
	conf, err := ioutil.TempFile("", "")
	if err != nil {
		t.Fatalf("Error creating conf file: %v", err)
	}
	fName := conf.Name()
	conf.Close()
	if err := ioutil.WriteFile(fName, content, 0666); err != nil {
		os.Remove(fName)
		t.Fatalf("Error writing conf file: %v", err)
	}
	return fName
}

func runReloadServerWithConfig(t *testing.T, configFile string) (*Server, *Options, string) {
	t.Helper()
	content, err := ioutil.ReadFile(configFile)
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
	content, err := ioutil.ReadFile(newConfig)
	if err != nil {
		t.Fatalf("Error loading file: %v", err)
	}
	changeCurrentConfigContentWithNewContent(t, curConfig, content)
}

func changeCurrentConfigContentWithNewContent(t *testing.T, curConfig string, content []byte) {
	t.Helper()
	if err := ioutil.WriteFile(curConfig, content, 0666); err != nil {
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
	defer os.Remove(config)
	defer server.Shutdown()

	loaded := server.ConfigTime()

	golden := &Options{
		ConfigFile:     config,
		Host:           "0.0.0.0",
		Port:           2233,
		AuthTimeout:    1.0,
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
		NoSigs: true,
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
	defer os.Remove(config)
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
	defer os.Remove(config)
	defer server.Shutdown()

	loaded := server.ConfigTime()

	golden := &Options{
		ConfigFile:     config,
		Host:           "0.0.0.0",
		Port:           2233,
		AuthTimeout:    1.0,
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
		NoSigs: true,
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
	defer os.Remove(config)
	defer os.Remove("nats-server.pid")
	defer os.Remove("nats-server.log")
	defer server.Shutdown()

	dir := filepath.Dir(config)
	var content []byte
	if runtime.GOOS != "windows" {
		content = []byte(`
			remote_syslog: "udp://127.0.0.1:514" # change on reload
			syslog:        true # enable on reload
		`)
	}
	platformConf := filepath.Join(dir, "platform.conf")
	defer os.Remove(platformConf)
	if err := ioutil.WriteFile(platformConf, content, 0666); err != nil {
		t.Fatalf("Unable to write config file: %v", err)
	}

	loaded := server.ConfigTime()

	golden := &Options{
		ConfigFile:     config,
		Host:           "0.0.0.0",
		Port:           2233,
		AuthTimeout:    1.0,
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
		NoSigs: true,
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
	defer os.Remove(config)
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

// Ensure Reload supports enabling TLS. Test this by starting a server without
// TLS enabled, connect to it to verify, reload config with TLS enabled, ensure
// reconnect fails, then ensure reconnect succeeds when using secure.
func TestConfigReloadEnableTLS(t *testing.T) {
	server, opts, config := runReloadServerWithConfig(t, "./configs/reload/basic.conf")
	defer os.Remove(config)
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
	defer os.Remove(config)
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

// Ensure Reload supports single user authentication config changes. Test this
// by starting a server with authentication enabled, connect to it to verify,
// reload config using a different username/password, ensure reconnect fails,
// then ensure reconnect succeeds when using the correct credentials.
func TestConfigReloadRotateUserAuthentication(t *testing.T) {
	server, opts, config := runReloadServerWithConfig(t, "./configs/reload/single_user_authentication_1.conf")
	defer os.Remove(config)
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
	defer os.Remove(config)
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
	defer os.Remove(config)
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
	defer os.Remove(config)
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
	defer os.Remove(config)
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
	defer os.Remove(config)
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
	defer os.Remove(config)
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
	defer os.Remove(config)
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
	defer os.Remove(config)
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
	defer os.Remove(config)
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

// Ensure Reload returns an error when attempting to change cluster address
// host.
func TestConfigReloadClusterHostUnsupported(t *testing.T) {
	server, _, config := runReloadServerWithConfig(t, "./configs/reload/srv_a_1.conf")
	defer os.Remove(config)
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
	defer os.Remove(config)
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
	defer os.Remove(srvbConfig)
	defer srvb.Shutdown()

	srva, srvaOpts, srvaConfig := runReloadServerWithConfig(t, "./configs/reload/srv_a_1.conf")
	defer os.Remove(srvaConfig)
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

	if numRoutes := srvb.NumRoutes(); numRoutes != 1 {
		t.Fatalf("Expected 1 route, got %d", numRoutes)
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
	defer os.Remove(srvaConfig)
	if err := srva.Reload(); err != nil {
		t.Fatalf("Error reloading config: %v", err)
	}
	checkClusterFormed(t, srva, srvb)

	if numRoutes := srvb.NumRoutes(); numRoutes != 1 {
		t.Fatalf("Expected 1 route, got %d", numRoutes)
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
	defer os.Remove(srvbConfig)
	defer srvb.Shutdown()

	srva, srvaOpts, srvaConfig := runReloadServerWithConfig(t, "./configs/reload/srv_a_2.conf")
	defer os.Remove(srvaConfig)
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

	if numRoutes := srvb.NumRoutes(); numRoutes != 1 {
		t.Fatalf("Expected 1 route, got %d", numRoutes)
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

	if numRoutes := srvb.NumRoutes(); numRoutes != 1 {
		t.Fatalf("Expected 1 route, got %d", numRoutes)
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
	srvb, srvbOpts, srvbConfig := runReloadServerWithConfig(t, "./configs/reload/srv_b_1.conf")
	defer os.Remove(srvbConfig)
	defer srvb.Shutdown()

	srva, srvaOpts, srvaConfig := runReloadServerWithConfig(t, "./configs/reload/srv_a_1.conf")
	defer os.Remove(srvaConfig)
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

	if numRoutes := srvb.NumRoutes(); numRoutes != 1 {
		t.Fatalf("Expected 1 route, got %d", numRoutes)
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
	defer os.Remove(srvaConfig)
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
	defer os.Remove(srvaConfig)
	if err := srva.Reload(); err != nil {
		t.Fatalf("Error reloading config: %v", err)
	}

	// Restart server B.
	srvb, _ = RunServerWithConfig("./configs/reload/srv_b_1.conf")
	defer srvb.Shutdown()

	// We should not have a cluster formed here.
	numRoutes := 0
	deadline := time.Now().Add(2 * DEFAULT_ROUTE_RECONNECT)
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
	if err := ioutil.WriteFile(conf, []byte(content), 0666); err != nil {
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
	defer os.Remove(conf)
	defer s.Shutdown()

	orgClusterPort := s.ClusterAddr().Port

	verify := func(t *testing.T, expectedHost string, expectedPort int, expectedIP string) {
		t.Helper()
		s.mu.Lock()
		routeInfo := s.routeInfo
		routeInfoJSON := Info{}
		err := json.Unmarshal(s.routeInfoJSON[5:], &routeInfoJSON) // Skip "INFO "
		s.mu.Unlock()
		if err != nil {
			t.Fatalf("Error on Unmarshal: %v", err)
		}
		if routeInfo.Host != expectedHost || routeInfo.Port != expectedPort || routeInfo.IP != expectedIP {
			t.Fatalf("Expected host/port/IP to be %s:%v, %q, got %s:%d, %q",
				expectedHost, expectedPort, expectedIP, routeInfo.Host, routeInfo.Port, routeInfo.IP)
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
	verify(t, "me", 1, "nats-route://me:1/")

	// Update config with cluster_advertise (no port specified)
	reloadUpdateConfig(t, s, conf, `
	listen: "0.0.0.0:-1"
	cluster: {
		listen: "0.0.0.0:-1"
		cluster_advertise: "me"
	}
	`)
	verify(t, "me", orgClusterPort, fmt.Sprintf("nats-route://me:%d/", orgClusterPort))

	// Update config with cluster_advertise (-1 port specified)
	reloadUpdateConfig(t, s, conf, `
	listen: "0.0.0.0:-1"
	cluster: {
		listen: "0.0.0.0:-1"
		cluster_advertise: "me:-1"
	}
	`)
	verify(t, "me", orgClusterPort, fmt.Sprintf("nats-route://me:%d/", orgClusterPort))

	// Update to remove cluster_advertise
	reloadUpdateConfig(t, s, conf, `
	listen: "0.0.0.0:-1"
	cluster: {
		listen: "0.0.0.0:-1"
	}
	`)
	verify(t, "0.0.0.0", orgClusterPort, "")
}

func TestConfigReloadClusterNoAdvertise(t *testing.T) {
	s, _, conf := runReloadServerWithContent(t, []byte(`
		listen: "0.0.0.0:-1"
		client_advertise: "me:1"
		cluster: {
			listen: "0.0.0.0:-1"
		}
	`))
	defer os.Remove(conf)
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
	defer os.Remove(conf)
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

func TestConfigClusterMembershipReload(t *testing.T) {
	//
	// [A] starts in dynamic mode.
	//
	s1, _, conf1 := runReloadServerWithContent(t, []byte(`
	listen: "0.0.0.0:-1"
        server_name: "A"

	cluster: {
          listen: "0.0.0.0:-1"
	}
	`))
	defer os.Remove(conf1)
	defer s1.Shutdown()

	got := s1.ClusterName()
	if got == "" {
		t.Fatalf("Expected update clustername to be set dynamically")
	}

	//
	// [A] joins AB cluster with explicit name.
	//
	reloadUpdateConfig(t, s1, conf1, `
        listen: "0.0.0.0:-1"
        server_name: "A"

        cluster: {
          name: "AB"
          listen: "0.0.0.0:-1"
        }
        `)
	if s1.ClusterName() != "AB" {
		t.Fatalf("Expected update clustername of \"AB\", got %q", s1.ClusterName())
	}

	//
	// [B] joins AB cluster with explicit name.
	//
	template := fmt.Sprintf(`
	listen: "0.0.0.0:-1"
        server_name: "B"

	cluster: {
          name: "AB"
          listen: "0.0.0.0:-1"
          routes: [ nats://localhost:%d ] # Route to A
	}
	`, s1.ClusterAddr().Port)

	s2, _, conf2 := runReloadServerWithContent(t, []byte(template))
	defer os.Remove(conf2)
	defer s2.Shutdown()
	checkClusterFormed(t, s1, s2)

	//
	// [A] client sends request to [B] client and should be able to respond.
	//
	nc1, err := nats.Connect(fmt.Sprintf("nats://%s:%d", "127.0.0.1", s1.Addr().(*net.TCPAddr).Port))
	if err != nil {
		t.Fatalf("Error creating client: %v", err)
	}
	defer nc1.Close()

	nc2, err := nats.Connect(fmt.Sprintf("nats://%s:%d", "127.0.0.1", s2.Addr().(*net.TCPAddr).Port))
	if err != nil {
		t.Fatalf("Error creating client: %v", err)
	}
	defer nc2.Close()
	nc1.Subscribe("test", func(m *nats.Msg) {
		m.Respond([]byte("pong"))
	})
	nc1.Flush()
	_, err = nc2.Request("test", []byte("ping"), 2*time.Second)
	if err != nil {
		t.Fatalf("Error making request to cluster, got: %s", err)
	}

	//
	// [B] leaves the cluster AB and stops soliciting from [A],
	// [A] is still is part of AB cluster with explicit name at this point.
	//
	template = fmt.Sprintf(`
	listen: "0.0.0.0:-1"
        server_name: "B"

	cluster: {
          # name: "AB"
          listen: "0.0.0.0:-1"
          # routes: [ nats://localhost:%d ] # Route to A is gone
	}
	`, s1.ClusterAddr().Port)
	reloadUpdateConfig(t, s2, conf2, template)
	checkFor(t, 10*time.Second, 100*time.Millisecond, func() error {
		if numRoutes := s1.NumRoutes(); numRoutes > 0 {
			return fmt.Errorf("Expected %d routes for server %q, got %d", 0, s1.ID(), numRoutes)
		}
		got = s2.ClusterName()
		if got == "AB" || got == "" {
			return fmt.Errorf("Expected update cluster name to be new, got %q", got)
		}
		got = s1.ClusterName()
		if got != "AB" {
			return fmt.Errorf("Expected update cluster name to be AB, got %q", got)
		}

		// [B] leaving AB cluster and stop soliciting should dissolve the cluster.
		if numRoutes := s1.NumRoutes(); numRoutes != 0 {
			return fmt.Errorf("Expected no routes for server %q, got %d", s1.ID(), numRoutes)
		}
		if numRoutes := s2.NumRoutes(); numRoutes != 0 {
			return fmt.Errorf("Expected no routes for server %q, got %d", s2.ID(), numRoutes)
		}
		return nil
	})

	//
	// [A] leaves cluster AB and goes back to dynamic.
	//
	reloadUpdateConfig(t, s1, conf1, `
        listen: "0.0.0.0:-1"
        server_name: "A"

        cluster: {
          listen: "0.0.0.0:-1"
        }
        `)
	checkFor(t, 10*time.Second, 100*time.Millisecond, func() error {
		got = s1.ClusterName()
		if got == "AB" || got == "" {
			return fmt.Errorf("Expected update cluster name to be new, got %q", got)
		}

		return nil
	})

	//
	// [B] client request fails since not part of cluster.
	//
	_, err = nc2.Request("test", []byte("failed ping"), 2*time.Second)
	if err == nil {
		t.Fatalf("Expected error making a request to cluster.")
	}

	//
	// [C] solicits from [A] and both form a dynamic cluster.
	//
	template = fmt.Sprintf(`
	listen: "0.0.0.0:-1"
        server_name: "C"

	cluster: {
          listen: "0.0.0.0:-1"
          routes: [ nats://localhost:%d ] # Route to A
	}
	`, s1.ClusterAddr().Port)

	s3, _, conf3 := runReloadServerWithContent(t, []byte(template))
	defer os.Remove(conf3)
	defer s3.Shutdown()
	checkClusterFormed(t, s1, s3)
	checkFor(t, 10*time.Second, 100*time.Millisecond, func() error {
		if numRoutes := s1.NumRoutes(); numRoutes < 1 {
			return fmt.Errorf("Expected %d routes for server %q, got %d", 0, s1.ID(), numRoutes)
		}

		if s1.ClusterName() != s3.ClusterName() {
			return fmt.Errorf("Expected cluster names to be the same: %s != %s", s1.ClusterName(), s3.ClusterName())
		}

		if s2.ClusterName() == s3.ClusterName() {
			return fmt.Errorf("Expected cluster names to not be the same: %s == %s", s1.ClusterName(), s3.ClusterName())
		}
		return nil
	})

	//
	// [C] client makes request to service from [A] which should respond OK.
	//
	nc3, err := nats.Connect(fmt.Sprintf("nats://%s:%d", "127.0.0.1", s3.Addr().(*net.TCPAddr).Port))
	if err != nil {
		t.Fatalf("Error creating client: %v", err)
	}
	defer nc3.Close()

	_, err = nc3.Request("test", []byte("ping"), 2*time.Second)
	if err != nil {
		t.Fatalf("Error making request to cluster, got: %s", err)
	}
}

func TestConfigSolicitClusterMembershipReload(t *testing.T) {
	//
	// [A] starts in dynamic mode.
	//
	sA, _, confA := runReloadServerWithContent(t, []byte(`
	listen: "0.0.0.0:-1"
        server_name: "A"

	cluster: {
          listen: "0.0.0.0:-1"
	}
	`))
	defer os.Remove(confA)
	defer sA.Shutdown()

	if sA.ClusterName() == "" {
		t.Fatalf("Expected update cluster name to be set dynamically")
	}

	//
	// [B] starts in dynamic mode.
	//
	sB, _, confB := runReloadServerWithContent(t, []byte(`
	listen: "0.0.0.0:-1"
        server_name: "B"

	cluster: {
          listen: "0.0.0.0:-1"
	}
	`))
	defer os.Remove(confB)
	defer sB.Shutdown()

	if sB.ClusterName() == "" {
		t.Fatalf("Expected update cluster name to be set dynamically")
	}

	//
	// [C] starts with ABC cluster name and the 2 routes.
	//
	clusterConf := fmt.Sprintf(`
	cluster {
          name: "ABC"
          listen: "0.0.0.0:-1"
          routes: [ 
            nats://localhost:%d
            nats://localhost:%d
          ]
	}
	`, sA.ClusterAddr().Port, sB.ClusterAddr().Port)

	sC, _, confC := runReloadServerWithContent(t, []byte(`
	listen: "0.0.0.0:-1"
        server_name: "C"
	`+clusterConf))
	defer os.Remove(confC)
	defer sC.Shutdown()

	if sC.ClusterName() != "ABC" {
		t.Fatalf("Expected update clustername to be set dynamically")
	}

	checkClusterFormed(t, sA, sB, sC)
}

func TestConfigLeafnodeClusterMembershipReload(t *testing.T) {
	//
	// [GROUND] starts in dynamic mode.
	//
	s0, _, conf0 := runReloadServerWithContent(t, []byte(`
	listen: "0.0.0.0:-1"
        server_name: "GROUND"

	leafnodes: {
          listen: "0.0.0.0:-1"
	}
	`))
	defer os.Remove(conf0)
	defer s0.Shutdown()

	leafConf := fmt.Sprintf(`
	leafnodes: {
          remotes [{ url: "nats://localhost:%d" }]
	}
	`, s0.LeafnodeAddr().Port)

	//
	// [A] connects to [GROUND] via leafnode port
	//
	sA, _, confA := runReloadServerWithContent(t, []byte(`
	listen: "0.0.0.0:-1"
        server_name: "A"

	cluster: {
          listen: "0.0.0.0:-1"
	}
	`+leafConf))
	defer os.Remove(confA)
	defer sA.Shutdown()

	//
	// [B] connects to [GROUND] via leafnode port
	//
	sB, _, confB := runReloadServerWithContent(t, []byte(`
	listen: "0.0.0.0:-1"
        server_name: "B"

	cluster: {
          listen: "0.0.0.0:-1"
	}
	`+leafConf))
	defer os.Remove(confB)
	defer sB.Shutdown()

	//
	// [A] client can make requests to service at [B]
	// with the request traveling through [GROUND] node.
	//
	ncA, err := nats.Connect(fmt.Sprintf("nats://localhost:%d", sA.Addr().(*net.TCPAddr).Port))
	if err != nil {
		t.Fatalf("Error creating client: %v", err)
	}
	defer ncA.Close()

	ncB, err := nats.Connect(fmt.Sprintf("nats://localhost:%d", sB.Addr().(*net.TCPAddr).Port))
	if err != nil {
		t.Fatalf("Error creating client: %v", err)
	}
	defer ncB.Close()

	ncB.Subscribe("help", func(msg *nats.Msg) {
		msg.Respond([]byte("OK"))
	})
	ncB.Flush()
	time.Sleep(500 * time.Millisecond)

	makeRequest := func(payload []byte) error {
		_, err := ncA.Request("help", payload, 5*time.Second)
		if err != nil {
			return err
		}
		return nil
	}

	//
	// Make roundtrip via leafnode connections.
	//
	// [A] <-- lid --> [GROUND] <-- lid --> [B]
	//
	//
	err = makeRequest([]byte("VIA LEAFNODE"))
	if err != nil {
		t.Fatal(err)
	}

	//
	// [GROUND] should have 2 leaf connections and
	// a single message flowing in/out.
	//
	leafz, err := s0.Leafz(nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(leafz.Leafs) != 2 {
		t.Errorf("Expected 2 leafs but got %d", len(leafz.Leafs))
	}
	for _, leaf := range leafz.Leafs {
		got := int(leaf.InMsgs)
		expected := 1
		if got != expected {
			t.Errorf("Expected: %d, got: %d", expected, got)
		}

		got = int(leaf.OutMsgs)
		expected = 1
		if got != expected {
			t.Errorf("Expected: %d, got: %d", expected, got)
		}

		got = int(leaf.NumSubs)
		expected = 2
		if got != expected {
			t.Errorf("Expected: %d, got: %d", expected, got)
		}
	}

	//
	// [B] solicit from [A] which will make [A] use the explicit
	// cluster name AB, since [A] is in dynamic cluster name mode,
	// forming a cluster together.
	//
	clusterConf := fmt.Sprintf(`
	cluster {
          name: "AB"
          listen: "0.0.0.0:-1"
          routes: [ nats://localhost:%d ]
	}
	`, sA.ClusterAddr().Port)

	reloadUpdateConfig(t, sB, confB, `
	listen: "0.0.0.0:-1"
        server_name: "B"
        `+clusterConf+leafConf)

	checkFor(t, 10*time.Second, 100*time.Millisecond, func() error {
		if sA.ClusterName() != "AB" || sB.ClusterName() != "AB" {
			return fmt.Errorf("Expected clustername of \"AB\", got %q and %q", sA.ClusterName(), sB.ClusterName())
		}
		return nil
	})
	checkClusterFormed(t, sA, sB)

	//
	// The cluster renaming should have caused a reconnection
	// so stats will be reset.
	//
	checkNoChangeInLeafStats := func(t *testing.T) {
		t.Helper()
		leafz, err = s0.Leafz(nil)
		if err != nil {
			t.Fatal(err)
		}
		for _, leaf := range leafz.Leafs {
			got := int(leaf.InMsgs)
			expected := 0
			if got != expected {
				t.Errorf("Expected: %d InMsgs, got: %d", expected, got)
			}

			got = int(leaf.OutMsgs)
			expected = 0
			if got != expected {
				t.Errorf("Expected: %d OutMsgs, got: %d", expected, got)
			}

			got = int(leaf.NumSubs)
			expected = 2
			if got != expected {
				t.Errorf("Expected: %d NumSubs, got: %d", expected, got)
			}
		}
	}
	checkNoChangeInLeafStats(t)

	connzB, err := sB.Connz(nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(connzB.Conns) != 1 || connzB.Conns[0].InMsgs != 1 || connzB.Conns[0].OutMsgs != 1 {
		t.Fatal("Expected connection to node B to receive messages.")
	}

	//
	// [A] to [B] roundtrip should be now via the cluster routes,
	// not the leafnode connections through the [GROUND] node.
	//
	err = makeRequest([]byte("AFTER CLUSTER FORMED"))
	if err != nil {
		t.Fatal(err)
	}
	time.Sleep(500 * time.Millisecond)
	connzB, err = sB.Connz(nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(connzB.Conns) != 1 || connzB.Conns[0].InMsgs != 2 || connzB.Conns[0].OutMsgs != 2 {
		t.Fatal("Expected connection to node B to receive messages.")
	}

	// [B] stops soliciting routes from [A] and goes back to dynamic.
	// This will cause another leafnode reconnection.
	reloadUpdateConfig(t, sB, confB, `
	listen: "0.0.0.0:-1"
        server_name: "B"

	cluster: {
          listen: "0.0.0.0:-1"
	}
        `+leafConf)

	// Confirm that there are no routes to both servers and
	// that the cluster name has changed.
	checkFor(t, 10*time.Second, 100*time.Millisecond, func() error {
		if numRoutes := sA.NumRoutes(); numRoutes != 0 {
			return fmt.Errorf("Expected no routes for server, got %d", numRoutes)
		}
		if numRoutes := sB.NumRoutes(); numRoutes != 0 {
			return fmt.Errorf("Expected no routes for server, got %d", numRoutes)
		}

		nameB := sB.ClusterName()
		if nameB == "AB" || nameB == "" {
			return fmt.Errorf("Expected clustername to change, got %q", nameB)
		}

		// Wait for leafnode connections to reconnect.
		leafz, _ = s0.Leafz(nil)
		if len(leafz.Leafs) < 2 {
			return fmt.Errorf("Expected 2 leafnode connections, got: %d", len(leafz.Leafs))
		}

		//
		// [A] cluster name should not be AB anymore after [B] went back to dynamic mode.
		//
		if sA.ClusterName() == "AB" {
			return fmt.Errorf("Expected cluster name to not be AB!")
		}

		return nil
	})

	// New request should have been through the leafnode again,
	// all using a dynamic cluster name on each side.
	//
	// [A] <-- lid --> [GROUND] <-- lid --> [B]
	//
	err = makeRequest([]byte("REQUEST VIA LEAFNODE AGAIN"))
	if err != nil {
		t.Fatalf("Expected response via leafnode, got: %s", err)
	}
}

func TestConfigReloadMaxSubsUnsupported(t *testing.T) {
	s, _, conf := runReloadServerWithContent(t, []byte(`max_subs: 1`))
	defer os.Remove(conf)
	defer s.Shutdown()

	if err := ioutil.WriteFile(conf, []byte(`max_subs: 10`), 0666); err != nil {
		t.Fatalf("Error writing config file: %v", err)
	}
	if err := s.Reload(); err == nil {
		t.Fatal("Expected Reload to return an error")
	}
}

func TestConfigReloadClientAdvertise(t *testing.T) {
	s, _, conf := runReloadServerWithContent(t, []byte(`listen: "0.0.0.0:-1"`))
	defer os.Remove(conf)
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
	defer os.Remove(config)
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

// Ensure reload supports changing the max payload size. Test this by starting
// a server with the default size limit, ensuring publishes work, reloading
// with a restrictive limit, and ensuring publishing an oversized message fails
// and disconnects the client.
func TestConfigReloadMaxPayload(t *testing.T) {
	server, opts, config := runReloadServerWithConfig(t, "./configs/reload/basic.conf")
	defer os.Remove(config)
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
		os.Remove(config)
		os.Remove("log.txt")
		os.Remove("nats-server.pid")
		os.Remove("log1.txt")
		os.Remove("nats-server1.pid")
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
	if err := os.Remove("log_old.txt"); err != nil {
		t.Fatalf("Error reloading config, cannot delete file: %v", err)
	}
	if err := os.Remove("nats-server_old.pid"); err != nil {
		t.Fatalf("Error reloading config, cannot delete file: %v", err)
	}
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
	defer os.Remove(confB)

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
	defer os.Remove(confA)

	srvb, _ := RunServerWithConfig(confB)
	defer srvb.Shutdown()

	srva, _ := RunServerWithConfig(confA)
	defer srva.Shutdown()

	// Wait for the cluster to form and capture the connection IDs of each route
	checkClusterFormed(t, srva, srvb)

	getCID := func(s *Server) uint64 {
		s.mu.Lock()
		defer s.mu.Unlock()
		for _, r := range s.routes {
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
	defer os.Remove(confA)
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
	defer os.Remove(confB)
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
	defer os.Remove(confA)
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
	defer os.Remove(confB)
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
	defer os.Remove(confA)
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
	defer os.Remove(confB)
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
	defer os.Remove(confA)
	srva, _ := RunServerWithConfig(confA)
	defer srva.Shutdown()

	optsB := DefaultOptions()
	optsB.Routes = RoutesFromStr(fmt.Sprintf("nats://127.0.0.1:%d", srva.ClusterAddr().Port))
	// Make server B behave like an old server
	optsB.routeProto = setRouteProtoForTest(RouteProtoZero)
	srvb := RunServer(optsB)
	defer srvb.Shutdown()

	checkClusterFormed(t, srva, srvb)

	// Get the route's connection ID
	getRouteRID := func() uint64 {
		rid := uint64(0)
		srvb.mu.Lock()
		for _, r := range srvb.routes {
			r.mu.Lock()
			rid = r.cid
			r.mu.Unlock()
			break
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
	defer os.Remove(conf)
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
		gAcc, _ := s.LookupAccount(globalAccountName)
		gAcc.mu.RLock()
		n := gAcc.sl.Count()
		fooMatch := gAcc.sl.Match("foo")
		bazMatch := gAcc.sl.Match("baz")
		gAcc.mu.RUnlock()
		if n != 2 {
			return fmt.Errorf("Global account should have 2 subs, got %v", n)
		}
		if len(fooMatch.psubs) != 1 {
			return fmt.Errorf("Global account should have foo sub")
		}
		if len(bazMatch.psubs) != 1 {
			return fmt.Errorf("Global account should have baz sub")
		}

		sAcc, _ := s.LookupAccount("synadia")
		sAcc.mu.RLock()
		n = sAcc.sl.Count()
		barMatch := sAcc.sl.Match("bar")
		sAcc.mu.RUnlock()
		if n != 1 {
			return fmt.Errorf("Synadia account should have 1 sub, got %v", n)
		}
		if len(barMatch.psubs) != 1 {
			return fmt.Errorf("Synadia account should have bar sub")
		}

		nAcc, _ := s.LookupAccount("nats.io")
		nAcc.mu.RLock()
		n = nAcc.sl.Count()
		batMatch := nAcc.sl.Match("bat")
		nAcc.mu.RUnlock()
		if n != 1 {
			return fmt.Errorf("Nats.io account should have 1 sub, got %v", n)
		}
		if len(batMatch.psubs) != 1 {
			return fmt.Errorf("Synadia account should have bar sub")
		}
		return nil
	})
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
	defer os.Remove(conf)
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
	defer os.Remove(conf)
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
	no_sys_acc: true
	cluster {
		name: "abc"
		port: -1
	}
	`))
	defer os.Remove(conf)
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
		} else if err != nats.ErrTimeout {
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
	no_sys_acc: true
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
	defer os.Remove(conf)
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

	logfile := "logtime.log"
	defer os.Remove(logfile)
	template := `
		listen: "127.0.0.1:-1"
		logfile: "logtime.log"
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
	} {
		t.Run(test.name, func(t *testing.T) {
			conf := createConfFile(t, []byte(fmt.Sprintf(template, test.content)))
			defer os.Remove(conf)

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
			if err := s.Reload(); err != nil {
				t.Fatalf("Error on reload: %v", err)
			}
			if test.val() != test.expected {
				t.Fatalf("Expected to be set to %v, got %v", test.expected, test.val())
			}
		})
	}
}

func TestConfigReloadMaxControlLineWithClients(t *testing.T) {
	server, opts, config := runReloadServerWithConfig(t, "./configs/reload/basic.conf")
	defer os.Remove(config)
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
	defer os.Remove(conf)
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
	defer os.Remove(conf)
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
	defer os.Remove(conf1)
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
	defer os.Remove(conf2)
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
	  %s
	  leaf {
	    listen: "127.0.0.1:-1"
	  }
	`
	conf1 := createConfFile(t, []byte(fmt.Sprintf(template, "")))
	defer os.Remove(conf1)
	s1, o1 := RunServerWithConfig(conf1)
	defer s1.Shutdown()

	u, err := url.Parse(fmt.Sprintf("nats://127.0.0.1:%d", o1.LeafNode.Port))
	if err != nil {
		t.Fatalf("Error creating url: %v", err)
	}

	template2 := `
          port: -1
          cluster {
            port: -1
            name: "%s"
          }
          leaf {
            remotes [
              {
                url: "%s"
              }
            ]
          }
	`
	config := fmt.Sprintf(template2, "A", u.String())
	conf2 := createConfFile(t, []byte(config))
	defer os.Remove(conf2)
	o2, err := ProcessConfigFile(conf2)
	if err != nil {
		t.Fatalf("Error processing config file: %v", err)
	}
	o2.NoLog, o2.NoSigs = true, false
	s2 := RunServer(o2)
	defer s2.Shutdown()

	checkFor(t, 3*time.Second, 15*time.Millisecond, func() error {
		if n := s1.NumLeafNodes(); n != 1 {
			return fmt.Errorf("Expected 1 leaf node, got %v", n)
		}
		return nil
	})

	config = fmt.Sprintf(template2, "B", u.String())
	changeCurrentConfigContentWithNewContent(t, conf2, []byte(config))

	if err := s2.Reload(); err != nil {
		t.Fatalf("Error during reload: %v", err)
	}
}

func TestConfigReloadAndVarz(t *testing.T) {
	template := `
		port: -1
		%s
	`
	conf := createConfFile(t, []byte(fmt.Sprintf(template, "")))
	defer os.Remove(conf)
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
}

func TestConfigReloadConnectErrReports(t *testing.T) {
	template := `
		port: -1
		%s
		%s
	`
	conf := createConfFile(t, []byte(fmt.Sprintf(template, "", "")))
	defer os.Remove(conf)
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

func TestAuthReloadDoesNotBreakRouteInterest(t *testing.T) {
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
	s.reloadAuthorization()

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

	ts := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte(ajwt))
	}))
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
			insecure: true
		}
	`)))
	defer os.Remove(conf)

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

func TestLoggingReload(t *testing.T) {
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
		log, err := ioutil.ReadFile(filename)
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
	defer os.Remove(conf)

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

	defer os.Remove("off-pre.log")
	reload("log_file: off-pre.log")

	traffic(10) // generate NO trace/debug entries in off-pre.log

	defer os.Remove("on.log")
	reload(`
		log_file: on.log
		debug: true
		trace_verbose: true
	`)

	traffic(10) // generate trace/debug entries in on.log

	defer os.Remove("off-post.log")
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

func TestReloadValidate(t *testing.T) {
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
	defer os.Remove(confFileName)
	srv, _ := RunServerWithConfig(confFileName)
	if srv == nil {
		t.Fatal("Server did not start")
	}
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
	} else if strings.HasPrefix(err.Error(), " no_auth_user:") {
		t.Logf("Expected no_auth_user error, got different one %s", err)
	}
	srv.Shutdown()
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
	defer os.Remove(conf)
	s, o := RunServerWithConfig(conf)
	defer s.Shutdown()

	urlSys := fmt.Sprintf("nats://sys:pwd@%s:%d", o.Host, o.Port)
	urlUsr := fmt.Sprintf("nats://usr:pwd@%s:%d", o.Host, o.Port)
	oldAcc, ok := s.accounts.Load("SYS")
	if !ok {
		t.Fatal("No SYS account")
	}

	testSrvState := func(oldAcc interface{}) {
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
		// This will fail prior to system account reload
		if acc, ok := s.accounts.Load(s.opts.SystemAccount); !ok {
			t.Fatal("Found different sys.account pointer")
		} else if acc == oldAcc {
			t.Fatal("System account is unaltered")
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

	testSrvState(nil)
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
	defer os.Remove(conf)
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
		s.accounts.Range(func(k, v interface{}) bool {
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
