// Copyright 2025-2026 The NATS Authors
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
	"crypto/tls"
	"errors"
	"fmt"
	"io"
	"net"
	"net/url"
	"os"
	"strings"
	"testing"
	"time"
)

// Basic HTTP proxy for testing
type testHTTPProxy struct {
	listener    net.Listener
	port        int
	username    string
	password    string
	started     bool
	closeDelay  time.Duration // Delay before closing connections for robustness
	connections []net.Conn    // Track connections for cleanup
	scheme      string
	readDelay   time.Duration // Delay before reading, which also delays a TLS handshake
}

// createTestHTTPSProxy creates a proxy that requires TLS using the given server config.
func createTestHTTPSProxy(username, password string, tlsConfig *tls.Config) *testHTTPProxy {
	p := createTestHTTPProxy(username, password)
	p.listener = tls.NewListener(p.listener, tlsConfig)
	p.scheme = "https"
	return p
}

// testProxyServerTLSConfig returns a TLS config for a proxy listening on 127.0.0.1.
func testProxyServerTLSConfig(t *testing.T, requireClientCert bool) *tls.Config {
	t.Helper()
	tc := &TLSConfigOpts{
		CertFile: "../test/configs/certs/server-cert.pem",
		KeyFile:  "../test/configs/certs/server-key.pem",
	}
	if requireClientCert {
		tc.CaFile = "../test/configs/certs/ca.pem"
		tc.Verify = true
	}
	cfg, err := GenTLSConfig(tc)
	require_NoError(t, err)
	return cfg
}

func createTestHTTPProxy(username, password string) *testHTTPProxy {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		panic(err)
	}
	port := l.Addr().(*net.TCPAddr).Port

	proxy := &testHTTPProxy{
		listener:    l,
		port:        port,
		username:    username,
		password:    password,
		closeDelay:  100 * time.Millisecond, // Default delay for test robustness
		connections: make([]net.Conn, 0),
		scheme:      "http",
	}

	return proxy
}

func (p *testHTTPProxy) setCloseDelay(delay time.Duration) {
	p.closeDelay = delay
}

func (p *testHTTPProxy) start() {
	if p.started {
		return
	}
	p.started = true

	go func() {
		for {
			conn, err := p.listener.Accept()
			if err != nil {
				return
			}
			p.connections = append(p.connections, conn)
			go p.handleConnection(conn)
		}
	}()
}

func (p *testHTTPProxy) handleConnection(conn net.Conn) {
	defer func() {
		if p.closeDelay > 0 {
			time.Sleep(p.closeDelay)
		}
		conn.Close()
	}()

	if p.readDelay > 0 {
		time.Sleep(p.readDelay)
	}

	// Set read timeout to prevent hanging on malformed requests
	conn.SetReadDeadline(time.Now().Add(10 * time.Second))

	// Read the CONNECT request
	buffer := make([]byte, 4096)
	n, err := conn.Read(buffer)
	if err != nil {
		return
	}

	request := string(buffer[:n])
	lines := strings.Split(request, "\r\n")

	if len(lines) == 0 || !strings.HasPrefix(lines[0], "CONNECT ") {
		conn.Write([]byte("HTTP/1.1 400 Bad Request\r\n\r\n"))
		return
	}

	// Check authentication if required
	if p.username != _EMPTY_ || p.password != _EMPTY_ {
		authFound := false
		for _, line := range lines {
			if strings.HasPrefix(line, "Proxy-Authorization: Basic ") {
				authFound = true
				break
			}
		}
		if !authFound {
			conn.Write([]byte("HTTP/1.1 407 Proxy Authentication Required\r\n\r\n"))
			return
		}
	}

	// Extract target host from CONNECT line
	parts := strings.Fields(lines[0])
	if len(parts) < 3 {
		conn.Write([]byte("HTTP/1.1 400 Bad Request\r\n\r\n"))
		return
	}

	targetHost := parts[1]

	// Connect to target with timeout
	target, err := net.DialTimeout("tcp", targetHost, 5*time.Second)
	if err != nil {
		conn.Write([]byte("HTTP/1.1 502 Bad Gateway\r\n\r\n"))
		return
	}
	defer target.Close()

	// Send success response
	conn.Write([]byte("HTTP/1.1 200 Connection established\r\n\r\n"))

	// Clear read deadline for ongoing connection
	conn.SetReadDeadline(time.Time{})

	// Relay data between client and target with proper error handling
	done := make(chan bool, 2)

	// Client to target
	go func() {
		defer func() {
			done <- true
			target.Close()
		}()
		buffer := make([]byte, 32*1024)
		for {
			conn.SetReadDeadline(time.Now().Add(30 * time.Second))
			n, err := conn.Read(buffer)
			if err != nil {
				return
			}
			target.SetWriteDeadline(time.Now().Add(10 * time.Second))
			_, err = target.Write(buffer[:n])
			if err != nil {
				return
			}
		}
	}()

	// Target to client
	go func() {
		defer func() {
			done <- true
			conn.Close()
		}()
		buffer := make([]byte, 32*1024)
		for {
			target.SetReadDeadline(time.Now().Add(30 * time.Second))
			n, err := target.Read(buffer)
			if err != nil {
				return
			}
			conn.SetWriteDeadline(time.Now().Add(10 * time.Second))
			_, err = conn.Write(buffer[:n])
			if err != nil {
				return
			}
		}
	}()

	// Wait for either direction to finish
	<-done
}

func (p *testHTTPProxy) stop() {
	if p.listener != nil {
		p.listener.Close()
	}
	// Close all tracked connections with delay for robustness
	for _, conn := range p.connections {
		go func(c net.Conn) {
			if p.closeDelay > 0 {
				time.Sleep(p.closeDelay)
			}
			c.Close()
		}(conn)
	}
}

func (p *testHTTPProxy) url() string {
	return fmt.Sprintf("%s://127.0.0.1:%d", p.scheme, p.port)
}

func TestLeafNodeHttpProxyConfigParsing(t *testing.T) {
	// Test valid proxy configuration
	conf := `
		leafnodes {
			remotes = [
				{
					url: "ws://127.0.0.1:7422"
					proxy {
						url: "http://proxy.example.com:8080"
						username: "user"
						password: "pass"
						timeout: "10s"
					}
				}
			]
		}
	`

	configFile := createConfFile(t, []byte(conf))

	opts, err := ProcessConfigFile(configFile)
	if err != nil {
		t.Fatalf("Error parsing config: %v", err)
	}

	if len(opts.LeafNode.Remotes) != 1 {
		t.Fatalf("Expected 1 remote, got %d", len(opts.LeafNode.Remotes))
	}

	remote := opts.LeafNode.Remotes[0]
	if remote.Proxy.URL != "http://proxy.example.com:8080" {
		t.Errorf("Expected proxy URL 'http://proxy.example.com:8080', got '%s'", remote.Proxy.URL)
	}
	if remote.Proxy.Username != "user" {
		t.Errorf("Expected proxy username 'user', got '%s'", remote.Proxy.Username)
	}
	if remote.Proxy.Password != "pass" {
		t.Errorf("Expected proxy password 'pass', got '%s'", remote.Proxy.Password)
	}
	if remote.Proxy.Timeout != 10*time.Second {
		t.Errorf("Expected proxy timeout 10s, got %v", remote.Proxy.Timeout)
	}
}

func TestLeafNodeHttpProxyNoSchemeWarnings(t *testing.T) {
	// The proxy is used for all remote URL schemes, so none of them should warn.
	for _, urls := range [][]string{
		{"nats://127.0.0.1:7422"},
		{"nats://127.0.0.1:7422", "ws://127.0.0.1:8080"},
		{"ws://127.0.0.1:7422"},
	} {
		t.Run(strings.Join(urls, ","), func(t *testing.T) {
			remote := &RemoteLeafOpts{}
			for _, u := range urls {
				pu, err := url.Parse(u)
				require_NoError(t, err)
				remote.URLs = append(remote.URLs, pu)
			}
			remote.Proxy.URL = "http://proxy.example.com:8080"
			warnings, err := validateLeafNodeProxyOptions(remote)
			require_NoError(t, err)
			require_Len(t, len(warnings), 0)
		})
	}
}

func TestLeafNodeHttpProxyConnection(t *testing.T) {
	// Create a hub server with WebSocket support using config file
	hubConfig := createConfFile(t, []byte(`
		listen: "127.0.0.1:-1"
		websocket {
			listen: "127.0.0.1:-1"
			no_tls: true
		}
		leafnodes {
			listen: "127.0.0.1:-1"
		}
	`))

	hub, hubOpts := RunServerWithConfig(hubConfig)
	defer hub.Shutdown()

	// Create HTTP proxy
	proxy := createTestHTTPProxy(_EMPTY_, _EMPTY_)
	proxy.start()
	defer proxy.stop()

	// Create spoke server with proxy configuration via config file
	configContent := fmt.Sprintf(`
		listen: "127.0.0.1:-1"
		leafnodes {
			reconnect_interval: "50ms"
			remotes = [
				{
					url: "ws://127.0.0.1:%d"
					proxy {
						url: "%s"
						timeout: 5s
					}
				}
			]
		}
	`, hubOpts.Websocket.Port, proxy.url())

	configFile := createConfFile(t, []byte(configContent))

	spoke, _ := RunServerWithConfig(configFile)
	defer spoke.Shutdown()

	// Verify leafnode connections are established
	checkLeafNodeConnected(t, spoke)
	checkLeafNodeConnected(t, hub)
}

func TestLeafNodeHttpProxyConnectionToTCP(t *testing.T) {
	// Create a hub server using config file
	hubConfig := createConfFile(t, []byte(`
		listen: "127.0.0.1:-1"
		leafnodes {
			listen: "127.0.0.1:-1"
		}
	`))

	hub, hubOpts := RunServerWithConfig(hubConfig)
	defer hub.Shutdown()

	// Create HTTP proxy
	proxy := createTestHTTPProxy(_EMPTY_, _EMPTY_)
	proxy.start()
	defer proxy.stop()

	// Create spoke server with proxy configuration via config file
	configContent := fmt.Sprintf(`
		listen: "127.0.0.1:-1"
		leafnodes {
			reconnect_interval: "50ms"
			remotes = [
				{
					url: "nats://127.0.0.1:%d"
					proxy {
						url: "%s"
						timeout: 5s
					}
				}
			]
		}
	`, hubOpts.LeafNode.Port, proxy.url())

	configFile := createConfFile(t, []byte(configContent))

	spoke, _ := RunServerWithConfig(configFile)
	defer spoke.Shutdown()

	// Verify leafnode connections are established
	checkLeafNodeConnected(t, spoke)
	checkLeafNodeConnected(t, hub)
}

func TestLeafNodeHttpProxyWithAuthentication(t *testing.T) {
	// Create a hub server with WebSocket support using config file
	hubConfig := createConfFile(t, []byte(`
		listen: "127.0.0.1:-1"
		websocket {
			listen: "127.0.0.1:-1"
			no_tls: true
		}
		leafnodes {
			listen: "127.0.0.1:-1"
		}
	`))

	hub, hubOpts := RunServerWithConfig(hubConfig)
	defer hub.Shutdown()

	// Create HTTP proxy with authentication
	proxy := createTestHTTPProxy("testuser", "testpass")
	proxy.start()
	defer proxy.stop()

	// Create spoke server with proxy configuration including auth via config file
	configContent := fmt.Sprintf(`
		listen: "127.0.0.1:-1"
		leafnodes {
			reconnect_interval: "50ms"
			remotes = [
				{
					url: "ws://127.0.0.1:%d"
					proxy {
						url: "%s"
						username: "testuser"
						password: "testpass"
						timeout: 5s
					}
				}
			]
		}
	`, hubOpts.Websocket.Port, proxy.url())

	configFile := createConfFile(t, []byte(configContent))

	spoke, _ := RunServerWithConfig(configFile)
	defer spoke.Shutdown()

	// Verify leafnode connections are established
	checkLeafNodeConnected(t, spoke)
	checkLeafNodeConnected(t, hub)
}

func TestLeafNodeHttpProxyTLSMismatchDetection(t *testing.T) {
	// This test simulates the TLS mismatch scenario described in the feedback:
	// - Leafnode configured with proxy but no TLS
	// - Hub requires TLS
	// - Connection should fail with appropriate error message

	// Create hub server with TLS required using config file
	hubConfig := createConfFile(t, []byte(`
		listen: "127.0.0.1:-1"
		leafnodes {
			listen: "127.0.0.1:-1"
			tls: {
				cert_file: "../test/configs/certs/server-cert.pem"
				key_file: "../test/configs/certs/server-key.pem"
			}
		}
	`))

	hub, hubOpts := RunServerWithConfig(hubConfig)
	defer hub.Shutdown()

	// Create HTTP proxy
	proxy := createTestHTTPProxy(_EMPTY_, _EMPTY_)
	proxy.start()
	defer proxy.stop()

	// Create spoke server with proxy but no TLS configuration (intentional mismatch)
	spokeConfigContent := fmt.Sprintf(`
		listen: "127.0.0.1:-1"
		leafnodes {
			reconnect_interval: "50ms"
			remotes = [
				{
					url: "ws://127.0.0.1:%d"
					proxy {
						url: "%s"
						timeout: 5s
					}
					# Intentionally no TLS configuration to create mismatch
				}
			]
		}
	`, hubOpts.LeafNode.Port, proxy.url())

	spokeConfig := createConfFile(t, []byte(spokeConfigContent))
	spoke, _ := RunServerWithConfig(spokeConfig)
	defer spoke.Shutdown()

	// Wait and verify that connection was NOT established due to TLS mismatch
	// First attempt happens during RunServerWithConfig(), then retries every 50ms
	time.Sleep(250 * time.Millisecond)

	if spoke.NumLeafNodes() != 0 {
		t.Errorf("Expected 0 leafnode connections due to TLS mismatch, got %d", spoke.NumLeafNodes())
	}
}

func TestLeafNodeHttpProxyTunnelBasic(t *testing.T) {
	// Create HTTP proxy with longer delay for robustness
	proxy := createTestHTTPProxy(_EMPTY_, _EMPTY_)
	proxy.setCloseDelay(200 * time.Millisecond)
	proxy.start()
	defer proxy.stop()

	// Create a simple TCP server to connect to through proxy
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("Failed to create test server: %v", err)
	}
	defer listener.Close()

	targetPort := listener.Addr().(*net.TCPAddr).Port
	targetHost := fmt.Sprintf("127.0.0.1:%d", targetPort)

	errCh := make(chan error, 1)

	// Accept one connection with proper timeout handling
	go func() {
		conn, err := listener.Accept()
		if err != nil {
			errCh <- fmt.Errorf("unable to accept: %v", err)
			return
		}
		defer conn.Close()

		// Set read deadline to prevent hanging forever
		conn.SetReadDeadline(time.Now().Add(5 * time.Second))

		// Read the incoming data first
		buffer := make([]byte, 1024)
		n, err := conn.Read(buffer)
		if err != nil {
			errCh <- fmt.Errorf("server failed to read: %v", err)
			return
		}

		receivedMsg := string(buffer[:n])
		if receivedMsg != "Hello" {
			errCh <- fmt.Errorf("server expected 'Hello', got '%s'", receivedMsg)
			return
		}

		// Send response
		conn.SetWriteDeadline(time.Now().Add(5 * time.Second))
		_, err = conn.Write([]byte("Hello from target server"))
		if err != nil {
			errCh <- fmt.Errorf("server failed to write: %v", err)
			return
		}

		// Wait a bit to ensure the client has time to read before closing
		time.Sleep(50 * time.Millisecond)
		errCh <- nil
	}()

	// Test establishing proxy tunnel with timeout
	conn, err := establishHTTPProxyTunnel(proxy.url(), targetHost, 10*time.Second, _EMPTY_, _EMPTY_, nil, 0)
	if err != nil {
		t.Fatalf("Failed to establish proxy tunnel: %v", err)
	}
	defer conn.Close()

	// Test that we can communicate through the tunnel with deadlines
	conn.SetWriteDeadline(time.Now().Add(5 * time.Second))
	_, err = conn.Write([]byte("Hello"))
	if err != nil {
		t.Fatalf("Failed to write to proxy tunnel: %v", err)
	}

	conn.SetReadDeadline(time.Now().Add(5 * time.Second))
	buffer := make([]byte, 1024)
	n, err := conn.Read(buffer)
	if err != nil {
		t.Fatalf("Failed to read from proxy tunnel: %v", err)
	}

	response := string(buffer[:n])
	if response != "Hello from target server" {
		t.Errorf("Unexpected response: '%s', expected 'Hello from target server'", response)
	}

	select {
	case err := <-errCh:
		if err != nil {
			t.Fatalf("%v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("Server goroutine didn't complete in time, but test data was exchanged successfully")
	}
}

func TestLeafNodeHttpProxyTunnelWithAuth(t *testing.T) {
	// Create HTTP proxy with authentication and delay for robustness
	proxy := createTestHTTPProxy("testuser", "testpass")
	proxy.setCloseDelay(200 * time.Millisecond)
	proxy.start()
	defer proxy.stop()

	// Create a simple TCP server to connect to through proxy
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("Failed to create test server: %v", err)
	}
	defer listener.Close()

	targetPort := listener.Addr().(*net.TCPAddr).Port
	targetHost := fmt.Sprintf("127.0.0.1:%d", targetPort)

	errCh := make(chan error, 1)

	// Accept one connection with proper timeout handling
	go func() {
		conn, err := listener.Accept()
		if err != nil {
			errCh <- fmt.Errorf("unable to accept: %v", err)
			return
		}
		defer conn.Close()

		// Set read deadline to prevent hanging
		conn.SetReadDeadline(time.Now().Add(5 * time.Second))

		// Read the incoming data first
		buffer := make([]byte, 1024)
		_, err = conn.Read(buffer)
		if err != nil {
			errCh <- fmt.Errorf("unable to read: %v", err)
			return
		}

		// Send response
		conn.SetWriteDeadline(time.Now().Add(5 * time.Second))
		if _, err := conn.Write([]byte("Hello from authenticated server")); err != nil {
			errCh <- fmt.Errorf("unable to write: %v", err)
			return
		}

		errCh <- nil
	}()

	// Test establishing proxy tunnel with authentication and timeout
	conn, err := establishHTTPProxyTunnel(proxy.url(), targetHost, 10*time.Second, "testuser", "testpass", nil, 0)
	if err != nil {
		t.Fatalf("Failed to establish proxy tunnel with auth: %v", err)
	}
	defer conn.Close()

	// Test that we can communicate through the tunnel with deadlines
	conn.SetWriteDeadline(time.Now().Add(5 * time.Second))
	_, err = conn.Write([]byte("Hello"))
	if err != nil {
		t.Fatalf("Failed to write to proxy tunnel: %v", err)
	}

	conn.SetReadDeadline(time.Now().Add(5 * time.Second))
	buffer := make([]byte, 1024)
	n, err := conn.Read(buffer)
	if err != nil {
		t.Fatalf("Failed to read from proxy tunnel: %v", err)
	}

	response := string(buffer[:n])
	if response != "Hello from authenticated server" {
		t.Errorf("Unexpected response: %s", response)
	}

	select {
	case err := <-errCh:
		if err != nil {
			t.Fatalf("%v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Server goroutine didn't complete in time, but test data was exchanged successfully")
	}
}

func TestLeafNodeHttpProxyTunnelKeepsBufferedBytes(t *testing.T) {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	require_NoError(t, err)
	defer l.Close()

	// The proxy sends the CONNECT response and the target's first bytes in one write.
	go func() {
		conn, err := l.Accept()
		if err != nil {
			return
		}
		defer conn.Close()
		buf := make([]byte, 4096)
		if _, err := conn.Read(buf); err != nil {
			return
		}
		conn.Write([]byte("HTTP/1.1 200 Connection established\r\n\r\nINFO {}\r\n"))
		time.Sleep(time.Second)
	}()

	conn, err := establishHTTPProxyTunnel("http://"+l.Addr().String(), "127.0.0.1:7422", 5*time.Second, _EMPTY_, _EMPTY_, nil, 0)
	require_NoError(t, err)
	defer conn.Close()

	conn.SetReadDeadline(time.Now().Add(500 * time.Millisecond))
	buf := make([]byte, len("INFO {}\r\n"))
	_, err = io.ReadFull(conn, buf)
	require_NoError(t, err)
	require_Equal(t, string(buf), "INFO {}\r\n")
}

func TestLeafNodeHttpProxyTunnelFailsWithoutAuth(t *testing.T) {
	// Create HTTP proxy with authentication required and delay for robustness
	proxy := createTestHTTPProxy("testuser", "testpass")
	proxy.setCloseDelay(200 * time.Millisecond)
	proxy.start()
	defer proxy.stop()

	// Try to establish tunnel without providing credentials (should fail quickly)
	_, err := establishHTTPProxyTunnel(proxy.url(), "127.0.0.1:80", 10*time.Second, _EMPTY_, _EMPTY_, nil, 0)
	if err == nil {
		t.Fatal("Expected error when connecting without authentication")
	}

	if !strings.Contains(err.Error(), "proxy CONNECT failed") {
		t.Errorf("Expected proxy authentication error, got: %v", err)
	}

	// Verify the error contains the expected HTTP response code
	if !strings.Contains(err.Error(), "407") {
		t.Errorf("Expected HTTP 407 error in response, got: %v", err)
	}
}

// TestLeafNodeProxyValidationProgrammatic tests proxy validation when configuring server programmatically
func TestLeafNodeHttpProxyValidationProgrammatic(t *testing.T) {
	tests := []struct {
		// name is the name of the test.
		name string

		// setupOptions creates the Options configuration for the test.
		setupOptions func() *Options

		// err is the expected error. nil means no error expected.
		err error
	}{
		{
			name: "invalid proxy scheme",
			setupOptions: func() *Options {
				opts := &Options{}
				opts.LeafNode.Remotes = []*RemoteLeafOpts{
					{
						URLs: []*url.URL{{Scheme: wsSchemePrefix, Host: "127.0.0.1:7422"}},
					},
				}
				opts.LeafNode.Remotes[0].Proxy.URL = "ftp://proxy.example.com:21"
				return opts
			},
			err: errors.New("proxy URL scheme must be http or https"),
		},
		{
			name: "empty proxy URL - no validation performed",
			setupOptions: func() *Options {
				opts := &Options{}
				opts.LeafNode.Remotes = []*RemoteLeafOpts{
					{
						URLs: []*url.URL{{Scheme: wsSchemePrefix, Host: "127.0.0.1:7422"}},
					},
				}
				opts.LeafNode.Remotes[0].Proxy.URL = _EMPTY_
				return opts
			},
			err: nil, // No error expected for empty URL
		},
		{
			name: "proxy options without proxy URL",
			setupOptions: func() *Options {
				opts := &Options{}
				opts.LeafNode.Remotes = []*RemoteLeafOpts{
					{
						URLs: []*url.URL{{Scheme: wsSchemePrefix, Host: "127.0.0.1:7422"}},
					},
				}
				opts.LeafNode.Remotes[0].Proxy.Timeout = 5 * time.Second
				return opts
			},
			err: errors.New("proxy URL must be specified when other proxy options are set"),
		},
		{
			name: "proxy tls config without proxy URL",
			setupOptions: func() *Options {
				opts := &Options{}
				opts.LeafNode.Remotes = []*RemoteLeafOpts{
					{
						URLs: []*url.URL{{Scheme: wsSchemePrefix, Host: "127.0.0.1:7422"}},
					},
				}
				opts.LeafNode.Remotes[0].Proxy.TLSConfig = &tls.Config{}
				return opts
			},
			err: errors.New("proxy URL must be specified when other proxy options are set"),
		},
		{
			name: "invalid proxy URL parse failure",
			setupOptions: func() *Options {
				opts := &Options{}
				opts.LeafNode.Remotes = []*RemoteLeafOpts{
					{
						URLs: []*url.URL{{Scheme: wsSchemePrefix, Host: "127.0.0.1:7422"}},
					},
				}
				opts.LeafNode.Remotes[0].Proxy.URL = "ht!tp://invalid-url-with-bad-characters"
				return opts
			},
			err: errors.New("invalid proxy URL"),
		},
		{
			name: "missing proxy host",
			setupOptions: func() *Options {
				opts := &Options{}
				opts.LeafNode.Remotes = []*RemoteLeafOpts{
					{
						URLs: []*url.URL{{Scheme: wsSchemePrefix, Host: "127.0.0.1:7422"}},
					},
				}
				opts.LeafNode.Remotes[0].Proxy.URL = "http://"
				return opts
			},
			err: errors.New("proxy URL must specify a host"),
		},
		{
			name: "username without password",
			setupOptions: func() *Options {
				opts := &Options{}
				opts.LeafNode.Remotes = []*RemoteLeafOpts{
					{
						URLs: []*url.URL{{Scheme: wsSchemePrefix, Host: "127.0.0.1:7422"}},
					},
				}
				opts.LeafNode.Remotes[0].Proxy.URL = "http://proxy.example.com:8080"
				opts.LeafNode.Remotes[0].Proxy.Username = "user"
				return opts
			},
			err: errors.New("proxy username and password must both be specified"),
		},
		{
			name: "password without username",
			setupOptions: func() *Options {
				opts := &Options{}
				opts.LeafNode.Remotes = []*RemoteLeafOpts{
					{
						URLs: []*url.URL{{Scheme: wsSchemePrefix, Host: "127.0.0.1:7422"}},
					},
				}
				opts.LeafNode.Remotes[0].Proxy.URL = "http://proxy.example.com:8080"
				opts.LeafNode.Remotes[0].Proxy.Password = "pass"
				return opts
			},
			err: errors.New("proxy username and password must both be specified"),
		},
		{
			name: "negative timeout value",
			setupOptions: func() *Options {
				opts := &Options{}
				opts.LeafNode.Remotes = []*RemoteLeafOpts{
					{
						URLs: []*url.URL{{Scheme: wsSchemePrefix, Host: "127.0.0.1:7422"}},
					},
				}
				opts.LeafNode.Remotes[0].Proxy.URL = "http://proxy.example.com:8080"
				opts.LeafNode.Remotes[0].Proxy.Timeout = -5 * time.Second
				return opts
			},
			err: errors.New("proxy timeout must be >= 0"),
		},
		{
			name: "tls config with http proxy URL",
			setupOptions: func() *Options {
				opts := &Options{}
				opts.LeafNode.Remotes = []*RemoteLeafOpts{
					{
						URLs: []*url.URL{{Scheme: wsSchemePrefix, Host: "127.0.0.1:7422"}},
					},
				}
				opts.LeafNode.Remotes[0].Proxy.URL = "http://proxy.example.com:8080"
				opts.LeafNode.Remotes[0].Proxy.TLSConfig = &tls.Config{}
				return opts
			},
			err: errors.New("proxy TLS configuration requires an https proxy URL"),
		},
		{
			name: "negative tls timeout value",
			setupOptions: func() *Options {
				opts := &Options{}
				opts.LeafNode.Remotes = []*RemoteLeafOpts{
					{
						URLs: []*url.URL{{Scheme: wsSchemePrefix, Host: "127.0.0.1:7422"}},
					},
				}
				opts.LeafNode.Remotes[0].Proxy.URL = "https://proxy.example.com:8080"
				opts.LeafNode.Remotes[0].Proxy.TLSTimeout = -1
				return opts
			},
			err: errors.New("proxy TLS timeout must be >= 0"),
		},
		{
			name: "tls config with https proxy URL - valid",
			setupOptions: func() *Options {
				opts := &Options{}
				opts.LeafNode.Remotes = []*RemoteLeafOpts{
					{
						URLs: []*url.URL{{Scheme: wsSchemePrefix, Host: "127.0.0.1:7422"}},
					},
				}
				opts.LeafNode.Remotes[0].Proxy.URL = "https://proxy.example.com:8080"
				opts.LeafNode.Remotes[0].Proxy.TLSConfig = &tls.Config{}
				return opts
			},
			err: nil,
		},
		{
			name: "zero timeout value - valid",
			setupOptions: func() *Options {
				opts := &Options{}
				opts.LeafNode.Remotes = []*RemoteLeafOpts{
					{
						URLs: []*url.URL{{Scheme: wsSchemePrefix, Host: "127.0.0.1:7422"}},
					},
				}
				opts.LeafNode.Remotes[0].Proxy.URL = "http://proxy.example.com:8080"
				opts.LeafNode.Remotes[0].Proxy.Timeout = 0
				return opts
			},
			err: nil, // No error expected for zero timeout
		},
		{
			name: "valid proxy configuration with authentication",
			setupOptions: func() *Options {
				opts := &Options{}
				opts.LeafNode.Remotes = []*RemoteLeafOpts{
					{
						URLs: []*url.URL{{Scheme: wsSchemePrefix, Host: "127.0.0.1:7422"}},
					},
				}
				opts.LeafNode.Remotes[0].Proxy.URL = "http://proxy.example.com:8080"
				opts.LeafNode.Remotes[0].Proxy.Username = "user"
				opts.LeafNode.Remotes[0].Proxy.Password = "pass"
				opts.LeafNode.Remotes[0].Proxy.Timeout = 10 * time.Second
				return opts
			},
			err: nil, // No error expected
		},
		{
			name: "valid proxy configuration without authentication",
			setupOptions: func() *Options {
				opts := &Options{}
				opts.LeafNode.Remotes = []*RemoteLeafOpts{
					{
						URLs: []*url.URL{{Scheme: wsSchemePrefix, Host: "127.0.0.1:7422"}},
					},
				}
				opts.LeafNode.Remotes[0].Proxy.URL = "https://proxy.example.com:3128"
				opts.LeafNode.Remotes[0].Proxy.Timeout = 30 * time.Second
				return opts
			},
			err: nil, // No error expected
		},
	}

	checkErr := func(t *testing.T, err, expectedErr error) {
		t.Helper()
		switch {
		case err == nil && expectedErr == nil:
			// OK
		case err != nil && expectedErr == nil:
			t.Errorf("Unexpected error after validating options: %s", err)
		case err == nil && expectedErr != nil:
			t.Errorf("Expected %q error after validating invalid options but got nothing", expectedErr)
		case err != nil && expectedErr != nil:
			if !strings.Contains(err.Error(), expectedErr.Error()) {
				t.Errorf("Expected error containing %q, got: %q", expectedErr.Error(), err.Error())
			}
		}
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			opts := test.setupOptions()
			err := validateLeafNode(opts)
			checkErr(t, err, test.err)
		})
	}
}

func TestLeafNodeHttpProxyTLSConfigParsing(t *testing.T) {
	for _, test := range []struct {
		name    string
		proxy   string
		err     string
		hasTLS  bool
		timeout float64
	}{
		{
			name:  "https without tls block",
			proxy: `url: "https://proxy.example.com:3128"`,
		},
		{
			name: "https with tls block",
			proxy: `url: "https://proxy.example.com:3128"
				tls { ca_file: "../test/configs/certs/ca.pem", timeout: 3 }`,
			hasTLS:  true,
			timeout: 3,
		},
		{
			name: "http with tls block",
			proxy: `url: "http://proxy.example.com:3128"
				tls { ca_file: "../test/configs/certs/ca.pem" }`,
			err: "proxy TLS configuration requires an https proxy URL",
		},
		{
			name: "unknown tls field",
			proxy: `url: "https://proxy.example.com:3128"
				tls { foo: bar }`,
			err: `"foo" is not supported for proxy TLS`,
		},
		{
			name:  "tls block without proxy url",
			proxy: `tls { ca_file: "../test/configs/certs/ca.pem" }`,
			err:   "proxy URL must be specified when other proxy options are set",
		},
		{
			name:  "credentials without proxy url",
			proxy: `username: "user", password: "pass"`,
			err:   "proxy URL must be specified when other proxy options are set",
		},
		{
			name: "https with client tls options",
			proxy: `url: "https://proxy.example.com:3128"
				tls {
					cert_file: "../test/configs/certs/client-cert.pem"
					key_file: "../test/configs/certs/client-key.pem"
					ca_file: "../test/configs/certs/ca.pem"
					insecure: false
					cipher_suites: ["TLS_ECDHE_ECDSA_WITH_AES_128_GCM_SHA256"]
					curve_preferences: ["CurveP256"]
					min_version: "1.2"
					timeout: 3
				}`,
			hasTLS:  true,
			timeout: 3,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			conf := createConfFile(t, fmt.Appendf(nil, `
				leafnodes {
					remotes = [
						{
							url: "ws://127.0.0.1:7422"
							proxy {
								%s
							}
						}
					]
				}
			`, test.proxy))
			opts, err := ProcessConfigFile(conf)
			if test.err != _EMPTY_ {
				require_Error(t, err)
				require_Contains(t, err.Error(), test.err)
				return
			}
			require_NoError(t, err)
			remote := opts.LeafNode.Remotes[0]
			require_Equal(t, remote.Proxy.TLSConfig != nil, test.hasTLS)
			if test.hasTLS {
				require_NotNil(t, remote.Proxy.TLSConfig.RootCAs)
			}
			require_Equal(t, remote.Proxy.TLSTimeout, test.timeout)
		})
	}
}

func TestLeafNodeHttpProxyTLSUnsupportedKeys(t *testing.T) {
	for _, setting := range []string{
		`ocsp_peer: true`,
		`pinned_certs: ["a8b1c3d5e7f9a1b3c5d7e9f1a3b5c7d9e1f3a5b7c9d1e3f5a7b9c1d3e5f7a9b1"]`,
		`verify: false`,
		`verify_and_map: true`,
		`connection_rate_limit: 10`,
		`handshake_first: true`,
		`first: true`,
		`immediate: true`,
		`verify_cert_and_check_known_urls: true`,
		`OCSP_Peer: true`,
	} {
		key := strings.SplitN(setting, ":", 2)[0]
		t.Run(key, func(t *testing.T) {
			conf := createConfFile(t, fmt.Appendf(nil, `
				leafnodes {
					remotes = [
						{
							url: "ws://127.0.0.1:7422"
							proxy {
								url: "https://proxy.example.com:3128"
								tls {
									ca_file: "../test/configs/certs/ca.pem"
									%s
								}
							}
						}
					]
				}
			`, setting))
			_, err := ProcessConfigFile(conf)
			require_Error(t, err)
			require_Contains(t, err.Error(), fmt.Sprintf("%q is not supported for proxy TLS", key))
		})
	}
}

func TestLeafNodeHttpProxyDialAddress(t *testing.T) {
	for _, test := range []struct {
		url      string
		expected string
	}{
		{"http://proxy.example.com", "proxy.example.com:80"},
		{"https://proxy.example.com", "proxy.example.com:443"},
		{"http://proxy.example.com:3128", "proxy.example.com:3128"},
		{"https://proxy.example.com:3128", "proxy.example.com:3128"},
		{"https://[::1]", "[::1]:443"},
	} {
		u, err := url.Parse(test.url)
		require_NoError(t, err)
		require_Equal(t, proxyDialAddress(u), test.expected)
	}
}

// startTestEchoServer starts a TCP server that echoes back what it reads.
func startTestEchoServer(t *testing.T) string {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	require_NoError(t, err)
	t.Cleanup(func() { l.Close() })
	go func() {
		for {
			conn, err := l.Accept()
			if err != nil {
				return
			}
			go func() {
				defer conn.Close()
				buf := make([]byte, 1024)
				for {
					n, err := conn.Read(buf)
					if err != nil {
						return
					}
					if _, err = conn.Write(buf[:n]); err != nil {
						return
					}
				}
			}()
		}
	}()
	return l.Addr().String()
}

func TestLeafNodeHttpsProxyTunnel(t *testing.T) {
	target := startTestEchoServer(t)

	clientTLSConfig := func(t *testing.T, tc *TLSConfigOpts) *tls.Config {
		t.Helper()
		cfg, err := GenTLSConfig(tc)
		require_NoError(t, err)
		cfg.RootCAs = cfg.ClientCAs
		return cfg
	}

	checkEcho := func(t *testing.T, conn net.Conn) {
		t.Helper()
		conn.SetDeadline(time.Now().Add(5 * time.Second))
		_, err := conn.Write([]byte("ping"))
		require_NoError(t, err)
		buf := make([]byte, 4)
		_, err = io.ReadFull(conn, buf)
		require_NoError(t, err)
		require_Equal(t, string(buf), "ping")
	}

	t.Run("trusted CA", func(t *testing.T) {
		proxy := createTestHTTPSProxy(_EMPTY_, _EMPTY_, testProxyServerTLSConfig(t, false))
		proxy.start()
		defer proxy.stop()

		cfg := clientTLSConfig(t, &TLSConfigOpts{CaFile: "../test/configs/certs/ca.pem"})
		conn, err := establishHTTPProxyTunnel(proxy.url(), target, 5*time.Second, _EMPTY_, _EMPTY_, cfg, 0)
		require_NoError(t, err)
		defer conn.Close()
		// The proxy's TLS must not be mistaken for TLS to the remote.
		_, ok := conn.(*tls.Conn)
		require_False(t, ok)
		pc, ok := conn.(*proxyTunnelConn)
		require_True(t, ok)
		_, ok = pc.Conn.(*tls.Conn)
		require_True(t, ok)
		checkEcho(t, conn)
	})

	t.Run("untrusted CA", func(t *testing.T) {
		proxy := createTestHTTPSProxy(_EMPTY_, _EMPTY_, testProxyServerTLSConfig(t, false))
		proxy.start()
		defer proxy.stop()

		// No TLS config uses the system roots, which don't trust the test CA.
		_, err := establishHTTPProxyTunnel(proxy.url(), target, 5*time.Second, _EMPTY_, _EMPTY_, nil, 0)
		require_Error(t, err)
		require_Contains(t, err.Error(), "proxy TLS handshake failed")
	})

	t.Run("client certificate", func(t *testing.T) {
		proxy := createTestHTTPSProxy("user", "pass", testProxyServerTLSConfig(t, true))
		proxy.start()
		defer proxy.stop()

		// Without a client certificate the proxy rejects the connection.
		cfg := clientTLSConfig(t, &TLSConfigOpts{CaFile: "../test/configs/certs/ca.pem"})
		_, err := establishHTTPProxyTunnel(proxy.url(), target, 5*time.Second, "user", "pass", cfg, 0)
		require_Error(t, err)

		cfg = clientTLSConfig(t, &TLSConfigOpts{
			CaFile:   "../test/configs/certs/ca.pem",
			CertFile: "../test/configs/certs/client-cert.pem",
			KeyFile:  "../test/configs/certs/client-key.pem",
		})
		conn, err := establishHTTPProxyTunnel(proxy.url(), target, 5*time.Second, "user", "pass", cfg, 0)
		require_NoError(t, err)
		defer conn.Close()
		checkEcho(t, conn)
	})

	// The proxy delays its side of the TLS handshake.
	for _, test := range []struct {
		name         string
		proxyTimeout time.Duration
		tlsTimeout   time.Duration
		ok           bool
	}{
		{"default tls timeout", 100 * time.Millisecond, 0, true},
		{"longer tls timeout", 100 * time.Millisecond, time.Second, true},
		{"shorter tls timeout", time.Second, 50 * time.Millisecond, false},
	} {
		t.Run(test.name, func(t *testing.T) {
			proxy := createTestHTTPSProxy(_EMPTY_, _EMPTY_, testProxyServerTLSConfig(t, false))
			proxy.readDelay = 300 * time.Millisecond
			proxy.start()
			defer proxy.stop()

			cfg := clientTLSConfig(t, &TLSConfigOpts{CaFile: "../test/configs/certs/ca.pem"})
			conn, err := establishHTTPProxyTunnel(proxy.url(), target, test.proxyTimeout, _EMPTY_, _EMPTY_, cfg, test.tlsTimeout)
			if !test.ok {
				require_Error(t, err)
				require_Contains(t, err.Error(), "proxy TLS handshake failed")
				return
			}
			require_NoError(t, err)
			defer conn.Close()
			checkEcho(t, conn)
		})
	}
}

func TestLeafNodeHttpsProxyTunnelCloseDoesNotBlock(t *testing.T) {
	// Writes on a pipe block until the peer reads, like a stalled proxy.
	client, server := net.Pipe()
	defer server.Close()

	serverTLS := tls.Server(server, testProxyServerTLSConfig(t, false))
	errCh := make(chan error, 1)
	go func() { errCh <- serverTLS.Handshake() }()

	cfg, err := GenTLSConfig(&TLSConfigOpts{CaFile: "../test/configs/certs/ca.pem"})
	require_NoError(t, err)
	cfg.RootCAs = cfg.ClientCAs
	cfg.ServerName = "localhost"
	clientTLS := tls.Client(client, cfg)
	require_NoError(t, clientTLS.Handshake())
	require_NoError(t, <-errCh)

	// The server stops reading, so close_notify can't be written.
	pc := &proxyTunnelConn{Conn: clientTLS}
	start := time.Now()
	require_NoError(t, pc.Close())
	if elapsed := time.Since(start); elapsed > time.Second {
		t.Fatalf("Close blocked for %v", elapsed)
	}
}

func TestLeafNodeHttpsProxyConnection(t *testing.T) {
	hubConf := createConfFile(t, []byte(`
		listen: "127.0.0.1:-1"
		websocket {
			listen: "127.0.0.1:-1"
			tls {
				cert_file: "../test/configs/certs/server-cert.pem"
				key_file: "../test/configs/certs/server-key.pem"
			}
		}
		leafnodes {
			listen: "127.0.0.1:-1"
		}
	`))
	hub, hubOpts := RunServerWithConfig(hubConf)
	defer hub.Shutdown()

	// The proxy requires a client certificate.
	proxy := createTestHTTPSProxy(_EMPTY_, _EMPTY_, testProxyServerTLSConfig(t, true))
	proxy.start()
	defer proxy.stop()

	// TLS to the hub is tunneled inside TLS to the proxy.
	spokeConf := createConfFile(t, fmt.Appendf(nil, `
		listen: "127.0.0.1:-1"
		leafnodes {
			reconnect_interval: "50ms"
			remotes = [
				{
					url: "wss://127.0.0.1:%d"
					tls { ca_file: "../test/configs/certs/ca.pem" }
					proxy {
						url: "%s"
						timeout: 5s
						tls {
							ca_file: "../test/configs/certs/ca.pem"
							cert_file: "../test/configs/certs/client-cert.pem"
							key_file: "../test/configs/certs/client-key.pem"
						}
					}
				}
			]
		}
	`, hubOpts.Websocket.Port, proxy.url()))
	spoke, _ := RunServerWithConfig(spokeConf)
	defer spoke.Shutdown()

	checkLeafNodeConnected(t, spoke)
	checkLeafNodeConnected(t, hub)
}

func TestLeafNodeHttpsProxyTLSReload(t *testing.T) {
	hubConf := createConfFile(t, []byte(`
		listen: "127.0.0.1:-1"
		websocket {
			listen: "127.0.0.1:-1"
			no_tls: true
		}
		leafnodes {
			listen: "127.0.0.1:-1"
		}
	`))
	hub, hubOpts := RunServerWithConfig(hubConf)
	defer hub.Shutdown()

	proxy := createTestHTTPSProxy(_EMPTY_, _EMPTY_, testProxyServerTLSConfig(t, false))
	proxy.start()
	defer proxy.stop()

	tmpl := `
		listen: "127.0.0.1:-1"
		leafnodes {
			reconnect_interval: "50ms"
			remotes = [
				{
					url: "ws://127.0.0.1:%d"
					proxy {
						url: "%s"
						timeout: 5s
						%s
					}
				}
			]
		}
	`
	// Without a tls block the system roots are used, which don't trust the test CA.
	spokeConf := createConfFile(t, fmt.Appendf(nil, tmpl, hubOpts.Websocket.Port, proxy.url(), _EMPTY_))
	spoke, _ := RunServerWithConfig(spokeConf)
	defer spoke.Shutdown()

	time.Sleep(250 * time.Millisecond)
	checkLeafNodeConnectedCount(t, spoke, 0)

	reloadUpdateConfig(t, spoke, spokeConf, fmt.Sprintf(tmpl, hubOpts.Websocket.Port, proxy.url(),
		`tls { ca_file: "../test/configs/certs/ca.pem" }`))
	checkLeafNodeConnected(t, spoke)
	checkLeafNodeConnected(t, hub)

	// Changing anything else in the proxy is still not supported.
	content := strings.Replace(fmt.Sprintf(tmpl, hubOpts.Websocket.Port, proxy.url(),
		`tls { ca_file: "../test/configs/certs/ca.pem" }`), "timeout: 5s", "timeout: 6s", 1)
	require_NoError(t, os.WriteFile(spokeConf, []byte(content), 0666))
	err := spoke.Reload()
	require_Error(t, err)
	require_Contains(t, err.Error(), "only the proxy TLS configuration can be changed")
}
