// Copyright 2023-2025 The NATS Authors
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

package test

import (
	"context"
	"crypto"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	. "github.com/nats-io/nats-server/v2/internal/ocsp"
	"github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
	"golang.org/x/crypto/ocsp"
)

func NewOCSPResponderRootCA(t *testing.T) *http.Server {
	t.Helper()
	respCertPEM := "configs/certs/ocsp_peer/mini-ca/caocsp/caocsp_cert.pem"
	respKeyPEM := "configs/certs/ocsp_peer/mini-ca/caocsp/private/caocsp_keypair.pem"
	issuerCertPEM := "configs/certs/ocsp_peer/mini-ca/root/root_cert.pem"
	return NewOCSPResponderDesignatedCustomAddress(t, issuerCertPEM, respCertPEM, respKeyPEM, "127.0.0.1:8888")
}

func NewOCSPResponderIntermediateCA1(t *testing.T) *http.Server {
	t.Helper()
	respCertPEM := "configs/certs/ocsp_peer/mini-ca/ocsp1/ocsp1_bundle.pem"
	respKeyPEM := "configs/certs/ocsp_peer/mini-ca/ocsp1/private/ocsp1_keypair.pem"
	issuerCertPEM := "configs/certs/ocsp_peer/mini-ca/intermediate1/intermediate1_cert.pem"
	return NewOCSPResponderDesignatedCustomAddress(t, issuerCertPEM, respCertPEM, respKeyPEM, "127.0.0.1:18888")
}

func NewOCSPResponderIntermediateCA1Undelegated(t *testing.T) *http.Server {
	t.Helper()
	issuerCertPEM := "configs/certs/ocsp_peer/mini-ca/intermediate1/intermediate1_cert.pem"
	issuerCertKey := "configs/certs/ocsp_peer/mini-ca/intermediate1/private/intermediate1_keypair.pem"
	return NewOCSPResponderCustomAddress(t, issuerCertPEM, issuerCertKey, "127.0.0.1:18888")
}

func NewOCSPResponderBadDelegateIntermediateCA1(t *testing.T) *http.Server {
	t.Helper()
	// UserA2 is a cert issued by intermediate1, but intermediate1 did not add OCSP signing extension
	respCertPEM := "configs/certs/ocsp_peer/mini-ca/client1/UserA2_bundle.pem"
	respKeyPEM := "configs/certs/ocsp_peer/mini-ca/client1/private/UserA2_keypair.pem"
	issuerCertPEM := "configs/certs/ocsp_peer/mini-ca/intermediate1/intermediate1_cert.pem"
	return NewOCSPResponderDesignatedCustomAddress(t, issuerCertPEM, respCertPEM, respKeyPEM, "127.0.0.1:18888")
}

func NewOCSPResponderIntermediateCA2(t *testing.T) *http.Server {
	t.Helper()
	respCertPEM := "configs/certs/ocsp_peer/mini-ca/ocsp2/ocsp2_bundle.pem"
	respKeyPEM := "configs/certs/ocsp_peer/mini-ca/ocsp2/private/ocsp2_keypair.pem"
	issuerCertPEM := "configs/certs/ocsp_peer/mini-ca/intermediate2/intermediate2_cert.pem"
	return NewOCSPResponderDesignatedCustomAddress(t, issuerCertPEM, respCertPEM, respKeyPEM, "127.0.0.1:28888")
}

// TestOCSPPeerGoodClients is test of two NATS client (AIA enabled at leaf and cert) under good path (different intermediates)
// and default ocsp_cache implementation and oscp_cache=false configuration
func TestOCSPPeerGoodClients(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	rootCAResponder := NewOCSPResponderRootCA(t)
	rootCAResponderURL := fmt.Sprintf("http://%s", rootCAResponder.Addr)
	defer rootCAResponder.Shutdown(ctx)
	SetOCSPStatus(t, rootCAResponderURL, "configs/certs/ocsp_peer/mini-ca/intermediate1/intermediate1_cert.pem", ocsp.Good)
	SetOCSPStatus(t, rootCAResponderURL, "configs/certs/ocsp_peer/mini-ca/intermediate2/intermediate2_cert.pem", ocsp.Good)

	intermediateCA1Responder := NewOCSPResponderIntermediateCA1(t)
	intermediateCA1ResponderURL := fmt.Sprintf("http://%s", intermediateCA1Responder.Addr)
	defer intermediateCA1Responder.Shutdown(ctx)
	SetOCSPStatus(t, intermediateCA1ResponderURL, "configs/certs/ocsp_peer/mini-ca/client1/UserA1_cert.pem", ocsp.Good)

	intermediateCA2Responder := NewOCSPResponderIntermediateCA2(t)
	intermediateCA2ResponderURL := fmt.Sprintf("http://%s", intermediateCA2Responder.Addr)
	defer intermediateCA2Responder.Shutdown(ctx)
	SetOCSPStatus(t, intermediateCA2ResponderURL, "configs/certs/ocsp_peer/mini-ca/client2/UserB1_cert.pem", ocsp.Good)

	for _, test := range []struct {
		name      string
		config    string
		opts      []nats.Option
		err       error
		rerr      error
		configure func()
	}{
		{
			"Default cache: mTLS OCSP peer check on inbound client connection, client of intermediate CA 1",
			`
				port: -1
				# default ocsp_cache since omitted
				tls: {
					cert_file: "configs/certs/ocsp_peer/mini-ca/server1/TestServer1_bundle.pem"
					key_file: "configs/certs/ocsp_peer/mini-ca/server1/private/TestServer1_keypair.pem"
					ca_file: "configs/certs/ocsp_peer/mini-ca/root/root_cert.pem"
					timeout: 5
					verify: true
					# Long form configuration, non-default ca_timeout
					ocsp_peer: {
						verify: true
						ca_timeout: 5
						allowed_clockskew: 30
					}
				}
			`,
			[]nats.Option{
				nats.ClientCert("./configs/certs/ocsp_peer/mini-ca/client1/UserA1_bundle.pem", "./configs/certs/ocsp_peer/mini-ca/client1/private/UserA1_keypair.pem"),
				nats.RootCAs("./configs/certs/ocsp_peer/mini-ca/root/root_cert.pem"),
				nats.ErrorHandler(noOpErrHandler),
			},
			nil,
			nil,
			func() {},
		},
		{
			"Default cache: mTLS OCSP peer check on inbound client connection, client of intermediate CA 2",
			`
				port: -1
				# default ocsp_cache since omitted
				tls: {
					cert_file: "configs/certs/ocsp_peer/mini-ca/server1/TestServer1_bundle.pem"
					key_file: "configs/certs/ocsp_peer/mini-ca/server1/private/TestServer1_keypair.pem"
					ca_file: "configs/certs/ocsp_peer/mini-ca/root/root_cert.pem"
					timeout: 5
					verify: true
					# Short form configuration
					ocsp_peer: true
				}
			`,
			[]nats.Option{
				nats.ClientCert("./configs/certs/ocsp_peer/mini-ca/client2/UserB1_bundle.pem", "./configs/certs/ocsp_peer/mini-ca/client2/private/UserB1_keypair.pem"),
				nats.RootCAs("./configs/certs/ocsp_peer/mini-ca/root/root_cert.pem"),
				nats.ErrorHandler(noOpErrHandler),
			},
			nil,
			nil,
			func() {},
		},
		{
			"Explicit true cache: mTLS OCSP peer check on inbound client connection, client of intermediate CA 1",
			`
				port: -1
				# Short form configuration
				ocsp_cache: true
				tls: {
					cert_file: "configs/certs/ocsp_peer/mini-ca/server1/TestServer1_bundle.pem"
					key_file: "configs/certs/ocsp_peer/mini-ca/server1/private/TestServer1_keypair.pem"
					ca_file: "configs/certs/ocsp_peer/mini-ca/root/root_cert.pem"
					timeout: 5
					verify: true
					# Long form configuration
					ocsp_peer: {
						verify: true
						ca_timeout: 5
						allowed_clockskew: 30
					}
				}
			`,
			[]nats.Option{
				nats.ClientCert("./configs/certs/ocsp_peer/mini-ca/client1/UserA1_bundle.pem", "./configs/certs/ocsp_peer/mini-ca/client1/private/UserA1_keypair.pem"),
				nats.RootCAs("./configs/certs/ocsp_peer/mini-ca/root/root_cert.pem"),
				nats.ErrorHandler(noOpErrHandler),
			},
			nil,
			nil,
			func() {},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			deleteLocalStore(t, "")
			test.configure()
			content := test.config
			conf := createConfFile(t, []byte(content))
			s, opts := RunServerWithConfig(conf)
			defer s.Shutdown()
			nc, err := nats.Connect(fmt.Sprintf("tls://localhost:%d", opts.Port), test.opts...)
			if test.err == nil && err != nil {
				t.Errorf("Expected to connect, got %v", err)
			} else if test.err != nil && err == nil {
				t.Errorf("Expected error on connect")
			} else if test.err != nil && err != nil {
				// Error on connect was expected
				if test.err.Error() != err.Error() {
					t.Errorf("Expected error %s, got: %s", test.err, err)
				}
				return
			}
			defer nc.Close()
			nc.Subscribe("ping", func(m *nats.Msg) {
				m.Respond([]byte("pong"))
			})
			nc.Flush()
			_, err = nc.Request("ping", []byte("ping"), 250*time.Millisecond)
			if test.rerr != nil && err == nil {
				t.Errorf("Expected error getting response")
			} else if test.rerr == nil && err != nil {
				t.Errorf("Expected response")
			}
		})
	}
}

// TestOCSPPeerUnknownClient is test of NATS client that is OCSP status Unknown from its OCSP Responder
func TestOCSPPeerUnknownClient(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	rootCAResponder := NewOCSPResponderRootCA(t)
	rootCAResponderURL := fmt.Sprintf("http://%s", rootCAResponder.Addr)
	defer rootCAResponder.Shutdown(ctx)
	SetOCSPStatus(t, rootCAResponderURL, "configs/certs/ocsp_peer/mini-ca/intermediate1/intermediate1_cert.pem", ocsp.Good)

	intermediateCA1Responder := NewOCSPResponderIntermediateCA1(t)
	defer intermediateCA1Responder.Shutdown(ctx)

	for _, test := range []struct {
		name      string
		config    string
		opts      []nats.Option
		err       error
		rerr      error
		configure func()
	}{
		{
			"Default cache, mTLS OCSP peer check on inbound client connection, client unknown to intermediate CA 1",
			`
				port: -1
				# Cache configuration is default
				tls: {
					cert_file: "configs/certs/ocsp_peer/mini-ca/server1/TestServer1_bundle.pem"
					key_file: "configs/certs/ocsp_peer/mini-ca/server1/private/TestServer1_keypair.pem"
					ca_file: "configs/certs/ocsp_peer/mini-ca/root/root_cert.pem"
					timeout: 5
					verify: true
					# Short form configuration
					ocsp_peer: true
				}
			`,
			[]nats.Option{
				nats.ClientCert("./configs/certs/ocsp_peer/mini-ca/client1/UserA1_bundle.pem", "./configs/certs/ocsp_peer/mini-ca/client1/private/UserA1_keypair.pem"),
				nats.RootCAs("./configs/certs/ocsp_peer/mini-ca/root/root_cert.pem"),
				nats.ErrorHandler(noOpErrHandler),
			},
			errors.New("remote error: tls: bad certificate"),
			errors.New("expect error"),
			func() {},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			deleteLocalStore(t, "")
			test.configure()
			content := test.config
			conf := createConfFile(t, []byte(content))
			s, opts := RunServerWithConfig(conf)
			defer s.Shutdown()
			nc, err := nats.Connect(fmt.Sprintf("tls://localhost:%d", opts.Port), test.opts...)
			if test.err == nil && err != nil {
				t.Errorf("Expected to connect, got %v", err)
			} else if test.err != nil && err == nil {
				t.Errorf("Expected error on connect")
			} else if test.err != nil && err != nil {
				// Error on connect was expected
				if test.err.Error() != err.Error() {
					t.Errorf("Expected error %s, got: %s", test.err, err)
				}
				return
			}
			defer nc.Close()

			t.Errorf("Expected connection error, fell through")
		})
	}
}

// TestOCSPPeerRevokedClient is test of NATS client that is OCSP status Revoked from its OCSP Responder
func TestOCSPPeerRevokedClient(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	rootCAResponder := NewOCSPResponderRootCA(t)
	rootCAResponderURL := fmt.Sprintf("http://%s", rootCAResponder.Addr)
	defer rootCAResponder.Shutdown(ctx)
	SetOCSPStatus(t, rootCAResponderURL, "configs/certs/ocsp_peer/mini-ca/intermediate1/intermediate1_cert.pem", ocsp.Good)

	intermediateCA1Responder := NewOCSPResponderIntermediateCA1(t)
	intermediateCA1ResponderURL := fmt.Sprintf("http://%s", intermediateCA1Responder.Addr)
	defer intermediateCA1Responder.Shutdown(ctx)
	SetOCSPStatus(t, intermediateCA1ResponderURL, "configs/certs/ocsp_peer/mini-ca/client1/UserA1_cert.pem", ocsp.Revoked)

	for _, test := range []struct {
		name      string
		config    string
		opts      []nats.Option
		err       error
		rerr      error
		configure func()
	}{
		{
			"mTLS OCSP peer check on inbound client connection, client revoked by intermediate CA 1",
			`
				port: -1
				# Cache configuration is default
				tls: {
					cert_file: "configs/certs/ocsp_peer/mini-ca/server1/TestServer1_bundle.pem"
					key_file: "configs/certs/ocsp_peer/mini-ca/server1/private/TestServer1_keypair.pem"
					ca_file: "configs/certs/ocsp_peer/mini-ca/root/root_cert.pem"
					timeout: 5
					verify: true
					# Turn on CA OCSP check so this revoked client should NOT be able to connect
					ocsp_peer: true
				}
			`,
			[]nats.Option{
				nats.ClientCert("./configs/certs/ocsp_peer/mini-ca/client1/UserA1_bundle.pem", "./configs/certs/ocsp_peer/mini-ca/client1/private/UserA1_keypair.pem"),
				nats.RootCAs("./configs/certs/ocsp_peer/mini-ca/root/root_cert.pem"),
				nats.ErrorHandler(noOpErrHandler),
			},
			errors.New("remote error: tls: bad certificate"),
			errors.New("expect error"),
			func() {},
		},
		{
			"Explicit disable, mTLS OCSP peer check on inbound client connection, client revoked by intermediate CA 1 but no OCSP check",
			`
				port: -1
				# Cache configuration is default
				tls: {
					cert_file: "configs/certs/ocsp_peer/mini-ca/server1/TestServer1_bundle.pem"
					key_file: "configs/certs/ocsp_peer/mini-ca/server1/private/TestServer1_keypair.pem"
					ca_file: "configs/certs/ocsp_peer/mini-ca/root/root_cert.pem"
					timeout: 5
					verify: true
					# Explicit disable of OCSP peer check
					ocsp_peer: false
				}
			`,
			[]nats.Option{
				nats.ClientCert("./configs/certs/ocsp_peer/mini-ca/client1/UserA1_bundle.pem", "./configs/certs/ocsp_peer/mini-ca/client1/private/UserA1_keypair.pem"),
				nats.RootCAs("./configs/certs/ocsp_peer/mini-ca/root/root_cert.pem"),
				nats.ErrorHandler(noOpErrHandler),
			},
			nil,
			nil,
			func() {},
		},
		{
			"Implicit disable, mTLS OCSP peer check on inbound client connection, client revoked by intermediate CA 1 but no OCSP check",
			`
				port: -1
				# Cache configuration is default
				tls: {
					cert_file: "configs/certs/ocsp_peer/mini-ca/server1/TestServer1_bundle.pem"
					key_file: "configs/certs/ocsp_peer/mini-ca/server1/private/TestServer1_keypair.pem"
					ca_file: "configs/certs/ocsp_peer/mini-ca/root/root_cert.pem"
					timeout: 5
					verify: true
					# Implicit disable of OCSP peer check (i.e. not configured)
					# ocsp_peer: false
				}
			`,
			[]nats.Option{
				nats.ClientCert("./configs/certs/ocsp_peer/mini-ca/client1/UserA1_bundle.pem", "./configs/certs/ocsp_peer/mini-ca/client1/private/UserA1_keypair.pem"),
				nats.RootCAs("./configs/certs/ocsp_peer/mini-ca/root/root_cert.pem"),
				nats.ErrorHandler(noOpErrHandler),
			},
			nil,
			nil,
			func() {},
		},
		{
			"Explicit disable (long form), mTLS OCSP peer check on inbound client connection, client revoked by intermediate CA 1 but no OCSP check",
			`
				port: -1
				# Cache configuration is default
				tls: {
					cert_file: "configs/certs/ocsp_peer/mini-ca/server1/TestServer1_bundle.pem"
					key_file: "configs/certs/ocsp_peer/mini-ca/server1/private/TestServer1_keypair.pem"
					ca_file: "configs/certs/ocsp_peer/mini-ca/root/root_cert.pem"
					timeout: 5
					verify: true
					# Explicit disable of OCSP peer check, long form
					ocsp_peer: { verify: false }
				}
			`,
			[]nats.Option{
				nats.ClientCert("./configs/certs/ocsp_peer/mini-ca/client1/UserA1_bundle.pem", "./configs/certs/ocsp_peer/mini-ca/client1/private/UserA1_keypair.pem"),
				nats.RootCAs("./configs/certs/ocsp_peer/mini-ca/root/root_cert.pem"),
				nats.ErrorHandler(noOpErrHandler),
			},
			nil,
			nil,
			func() {},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			deleteLocalStore(t, "")
			test.configure()
			content := test.config
			conf := createConfFile(t, []byte(content))
			s, opts := RunServerWithConfig(conf)
			defer s.Shutdown()
			nc, err := nats.Connect(fmt.Sprintf("tls://localhost:%d", opts.Port), test.opts...)
			if test.err == nil && err != nil {
				t.Errorf("Expected to connect, got %v", err)
			} else if test.err != nil && err == nil {
				t.Errorf("Expected error on connect")
			} else if test.err != nil && err != nil {
				// Error on connect was expected
				if test.err.Error() != err.Error() {
					t.Errorf("Expected error %s, got: %s", test.err, err)
				}
				return
			}
			defer nc.Close()
		})
	}
}

// TestOCSPPeerUnknownAndRevokedIntermediate test of NATS client that is OCSP good but either its intermediate is unknown or revoked
func TestOCSPPeerUnknownAndRevokedIntermediate(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	rootCAResponder := NewOCSPResponderRootCA(t)
	rootCAResponderURL := fmt.Sprintf("http://%s", rootCAResponder.Addr)
	defer rootCAResponder.Shutdown(ctx)
	SetOCSPStatus(t, rootCAResponderURL, "configs/certs/ocsp_peer/mini-ca/intermediate1/intermediate1_cert.pem", ocsp.Revoked)
	// No test OCSP status set on intermediate2, so unknown

	intermediateCA1Responder := NewOCSPResponderIntermediateCA1(t)
	intermediateCA1ResponderURL := fmt.Sprintf("http://%s", intermediateCA1Responder.Addr)
	defer intermediateCA1Responder.Shutdown(ctx)
	SetOCSPStatus(t, intermediateCA1ResponderURL, "configs/certs/ocsp_peer/mini-ca/client1/UserA1_cert.pem", ocsp.Good)

	intermediateCA2Responder := NewOCSPResponderIntermediateCA2(t)
	intermediateCA2ResponderURL := fmt.Sprintf("http://%s", intermediateCA2Responder.Addr)
	defer intermediateCA2Responder.Shutdown(ctx)
	SetOCSPStatus(t, intermediateCA2ResponderURL, "configs/certs/ocsp_peer/mini-ca/client2/UserB1_cert.pem", ocsp.Good)

	for _, test := range []struct {
		name      string
		config    string
		opts      []nats.Option
		err       error
		rerr      error
		configure func()
	}{
		{
			"mTLS OCSP peer check on inbound client connection, client's intermediate is revoked",
			`
				port: -1
				# Cache configuration is default
				tls: {
					cert_file: "configs/certs/ocsp_peer/mini-ca/server1/TestServer1_bundle.pem"
					key_file: "configs/certs/ocsp_peer/mini-ca/server1/private/TestServer1_keypair.pem"
					ca_file: "configs/certs/ocsp_peer/mini-ca/root/root_cert.pem"
					timeout: 5
					verify: true
					# Short form configuration
					ocsp_peer: true
				}
			`,
			[]nats.Option{
				nats.ClientCert("./configs/certs/ocsp_peer/mini-ca/client1/UserA1_bundle.pem", "./configs/certs/ocsp_peer/mini-ca/client1/private/UserA1_keypair.pem"),
				nats.RootCAs("./configs/certs/ocsp_peer/mini-ca/root/root_cert.pem"),
				nats.ErrorHandler(noOpErrHandler),
			},
			errors.New("remote error: tls: bad certificate"),
			errors.New("expect error"),
			func() {},
		},
		{
			"mTLS OCSP peer check on inbound client connection, client's intermediate is unknown'",
			`
				port: -1
				# Cache configuration is default
				tls: {
					cert_file: "configs/certs/ocsp_peer/mini-ca/server1/TestServer1_bundle.pem"
					key_file: "configs/certs/ocsp_peer/mini-ca/server1/private/TestServer1_keypair.pem"
					ca_file: "configs/certs/ocsp_peer/mini-ca/root/root_cert.pem"
					timeout: 5
					verify: true
					# Short form configuration
					ocsp_peer: true
				}
			`,
			[]nats.Option{
				nats.ClientCert("./configs/certs/ocsp_peer/mini-ca/client2/UserB1_bundle.pem", "./configs/certs/ocsp_peer/mini-ca/client2/private/UserB1_keypair.pem"),
				nats.RootCAs("./configs/certs/ocsp_peer/mini-ca/root/root_cert.pem"),
				nats.ErrorHandler(noOpErrHandler),
			},
			errors.New("remote error: tls: bad certificate"),
			errors.New("expect error"),
			func() {},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			deleteLocalStore(t, "")
			test.configure()
			content := test.config
			conf := createConfFile(t, []byte(content))
			s, opts := RunServerWithConfig(conf)
			defer s.Shutdown()

			nc, err := nats.Connect(fmt.Sprintf("tls://localhost:%d", opts.Port), test.opts...)
			if test.err == nil && err != nil {
				t.Errorf("Expected to connect, got %v", err)
			} else if test.err != nil && err == nil {
				t.Errorf("Expected error on connect")
			} else if test.err != nil && err != nil {
				// Error on connect was expected
				if test.err.Error() != err.Error() {
					t.Errorf("Expected error %s, got: %s", test.err, err)
				}
				return
			}
			defer nc.Close()

			t.Errorf("Expected connection error, fell through")
		})
	}
}

// TestOCSPPeerLeafGood tests Leaf Spoke peer checking Leaf Hub, Leaf Hub peer checking Leaf Spoke, and both peer checking
func TestOCSPPeerLeafGood(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	rootCAResponder := NewOCSPResponderRootCA(t)
	rootCAResponderURL := fmt.Sprintf("http://%s", rootCAResponder.Addr)
	defer rootCAResponder.Shutdown(ctx)
	SetOCSPStatus(t, rootCAResponderURL, "configs/certs/ocsp_peer/mini-ca/intermediate1/intermediate1_cert.pem", ocsp.Good)

	intermediateCA1Responder := NewOCSPResponderIntermediateCA1(t)
	intermediateCA1ResponderURL := fmt.Sprintf("http://%s", intermediateCA1Responder.Addr)
	defer intermediateCA1Responder.Shutdown(ctx)
	SetOCSPStatus(t, intermediateCA1ResponderURL, "configs/certs/ocsp_peer/mini-ca/server1/TestServer1_cert.pem", ocsp.Good)
	SetOCSPStatus(t, intermediateCA1ResponderURL, "configs/certs/ocsp_peer/mini-ca/server1/TestServer2_cert.pem", ocsp.Good)

	for _, test := range []struct {
		name        string
		hubconfig   string
		spokeconfig string
		expected    int
	}{
		{
			"OCSP peer check on Leaf Hub by Leaf Spoke (TLS client OCSP verification of TLS server)",
			`
				port: -1
				# Cache configuration is default
				leaf: {
					listen: 127.0.0.1:7444
					tls: {
						cert_file: "configs/certs/ocsp_peer/mini-ca/server1/TestServer1_bundle.pem"
						key_file: "configs/certs/ocsp_peer/mini-ca/server1/private/TestServer1_keypair.pem"
						ca_file: "configs/certs/ocsp_peer/mini-ca/root/root_cert.pem"
						timeout: 5
					}
				}
			`,
			`
				port: -1
				leaf: {
					remotes: [
						{
							url: "nats://127.0.0.1:7444",
							tls: {
								ca_file: "configs/certs/ocsp_peer/mini-ca/root/root_cert.pem"
								timeout: 5
								# Short form configuration
								ocsp_peer: true
							}
						}
					]
				}
			`,
			1,
		},
		{
			"OCSP peer check on Leaf Spoke by Leaf Hub (TLS server OCSP verification of TLS client)",
			`
				port: -1
				# Cache configuration is default
				leaf: {
					listen: 127.0.0.1:7444
					tls: {
						cert_file: "configs/certs/ocsp_peer/mini-ca/server1/TestServer1_bundle.pem"
						key_file: "configs/certs/ocsp_peer/mini-ca/server1/private/TestServer1_keypair.pem"
						ca_file: "configs/certs/ocsp_peer/mini-ca/root/root_cert.pem"
						timeout: 5
						verify: true
						# Short form configuration
						ocsp_peer: true
					}
				}
			`,
			`
				port: -1
				leaf: {
					remotes: [
						{
							url: "nats://127.0.0.1:7444",
							tls: {
								cert_file: "configs/certs/ocsp_peer/mini-ca/server1/TestServer2_bundle.pem"
								key_file: "configs/certs/ocsp_peer/mini-ca/server1/private/TestServer2_keypair.pem"
								ca_file: "configs/certs/ocsp_peer/mini-ca/root/root_cert.pem"
								timeout: 5
							}
						}
					]
				}
			`,
			1,
		},
		{
			"OCSP peer check bi-directionally",
			`
				port: -1
				# Cache configuration is default
				leaf: {
					listen: 127.0.0.1:7444
					tls: {
						cert_file: "configs/certs/ocsp_peer/mini-ca/server1/TestServer1_bundle.pem"
						key_file: "configs/certs/ocsp_peer/mini-ca/server1/private/TestServer1_keypair.pem"
						ca_file: "configs/certs/ocsp_peer/mini-ca/root/root_cert.pem"
						timeout: 5
						verify: true
						# Short form configuration
						ocsp_peer: true
					}
				}
			`,
			`
				port: -1
				leaf: {
					remotes: [
						{
							url: "nats://127.0.0.1:7444",
							tls: {
								cert_file: "configs/certs/ocsp_peer/mini-ca/server1/TestServer2_bundle.pem"
								key_file: "configs/certs/ocsp_peer/mini-ca/server1/private/TestServer2_keypair.pem"
								ca_file: "configs/certs/ocsp_peer/mini-ca/root/root_cert.pem"
								timeout: 5
								# Short form configuration
								ocsp_peer: true
							}
						}
					]
				}
			`,
			1,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			deleteLocalStore(t, "")
			hubcontent := test.hubconfig
			hubconf := createConfFile(t, []byte(hubcontent))
			hub, _ := RunServerWithConfig(hubconf)
			defer hub.Shutdown()

			spokecontent := test.spokeconfig
			spokeconf := createConfFile(t, []byte(spokecontent))
			spoke, _ := RunServerWithConfig(spokeconf)
			defer spoke.Shutdown()

			checkLeafNodeConnectedCount(t, hub, test.expected)
		})
	}
}

// TestOCSPPeerLeafReject tests rejected Leaf Hub, rejected Leaf Spoke, and both rejecting each other
func TestOCSPPeerLeafReject(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	rootCAResponder := NewOCSPResponderRootCA(t)
	rootCAResponderURL := fmt.Sprintf("http://%s", rootCAResponder.Addr)
	defer rootCAResponder.Shutdown(ctx)
	SetOCSPStatus(t, rootCAResponderURL, "configs/certs/ocsp_peer/mini-ca/intermediate1/intermediate1_cert.pem", ocsp.Good)

	intermediateCA1Responder := NewOCSPResponderIntermediateCA1(t)
	intermediateCA1ResponderURL := fmt.Sprintf("http://%s", intermediateCA1Responder.Addr)
	defer intermediateCA1Responder.Shutdown(ctx)
	SetOCSPStatus(t, intermediateCA1ResponderURL, "configs/certs/ocsp_peer/mini-ca/server1/TestServer1_cert.pem", ocsp.Revoked)
	SetOCSPStatus(t, intermediateCA1ResponderURL, "configs/certs/ocsp_peer/mini-ca/server1/TestServer2_cert.pem", ocsp.Revoked)

	for _, test := range []struct {
		name        string
		hubconfig   string
		spokeconfig string
		expected    int
	}{
		{
			"OCSP peer check on Leaf Hub by Leaf Spoke (TLS client OCSP verification of TLS server)",
			`
				port: -1
				# Cache configuration is default
				leaf: {
					listen: 127.0.0.1:7444
					tls: {
						cert_file: "configs/certs/ocsp_peer/mini-ca/server1/TestServer1_bundle.pem"
						key_file: "configs/certs/ocsp_peer/mini-ca/server1/private/TestServer1_keypair.pem"
						ca_file: "configs/certs/ocsp_peer/mini-ca/root/root_cert.pem"
						timeout: 5
					}
				}
			`,
			`
				port: -1
				leaf: {
					remotes: [
						{
							url: "nats://127.0.0.1:7444",
							tls: {
								ca_file: "configs/certs/ocsp_peer/mini-ca/root/root_cert.pem"
								timeout: 5
								# Short form configuration
								ocsp_peer: true
							}
						}
					]
				}
			`,
			0,
		},
		{
			"OCSP peer check on Leaf Spoke by Leaf Hub (TLS server OCSP verification of TLS client)",
			`
				port: -1
				leaf: {
					listen: 127.0.0.1:7444
					tls: {
						cert_file: "configs/certs/ocsp_peer/mini-ca/server1/TestServer1_bundle.pem"
						key_file: "configs/certs/ocsp_peer/mini-ca/server1/private/TestServer1_keypair.pem"
						ca_file: "configs/certs/ocsp_peer/mini-ca/root/root_cert.pem"
						timeout: 5
						verify: true
						# Short form configuration
						ocsp_peer: true
					}
				}
			`,
			`
				port: -1
				leaf: {
					remotes: [
						{
							url: "nats://127.0.0.1:7444",
							tls: {
								cert_file: "configs/certs/ocsp_peer/mini-ca/server1/TestServer2_bundle.pem"
								key_file: "configs/certs/ocsp_peer/mini-ca/server1/private/TestServer2_keypair.pem"
								ca_file: "configs/certs/ocsp_peer/mini-ca/root/root_cert.pem"
								timeout: 5
							}
						}
					]
				}
			`,
			0,
		},
		{
			"OCSP peer check bi-directionally",
			`
				port: -1
				leaf: {
					listen: 127.0.0.1:7444
					tls: {
						cert_file: "configs/certs/ocsp_peer/mini-ca/server1/TestServer1_bundle.pem"
						key_file: "configs/certs/ocsp_peer/mini-ca/server1/private/TestServer1_keypair.pem"
						ca_file: "configs/certs/ocsp_peer/mini-ca/root/root_cert.pem"
						timeout: 5
						verify: true
						# Short form configuration
						ocsp_peer: true
					}
				}
			`,
			`
				port: -1
				leaf: {
					remotes: [
						{
							url: "nats://127.0.0.1:7444",
							tls: {
								cert_file: "configs/certs/ocsp_peer/mini-ca/server1/TestServer2_bundle.pem"
								key_file: "configs/certs/ocsp_peer/mini-ca/server1/private/TestServer2_keypair.pem"
								ca_file: "configs/certs/ocsp_peer/mini-ca/root/root_cert.pem"
								timeout: 5
								# Short form configuration
								ocsp_peer: true
							}
						}
					]
				}
			`,
			0,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			deleteLocalStore(t, "")
			hubcontent := test.hubconfig
			hubconf := createConfFile(t, []byte(hubcontent))
			hub, _ := RunServerWithConfig(hubconf)
			defer hub.Shutdown()
			spokecontent := test.spokeconfig
			spokeconf := createConfFile(t, []byte(spokecontent))
			spoke, _ := RunServerWithConfig(spokeconf)
			defer spoke.Shutdown()
			// Need to inject some time for leaf connection attempts to complete, could refine this to better
			// negative test
			time.Sleep(2000 * time.Millisecond)
			checkLeafNodeConnectedCount(t, hub, test.expected)
		})
	}
}

func checkLeafNodeConnectedCount(t testing.TB, s *server.Server, lnCons int) {
	t.Helper()
	checkFor(t, 5*time.Second, 15*time.Millisecond, func() error {
		if nln := s.NumLeafNodes(); nln != lnCons {
			return fmt.Errorf("expected %d connected leafnode(s) for server %q, got %d",
				lnCons, s.ID(), nln)
		}
		return nil
	})
}

// TestOCSPPeerGoodClientsNoneCache is test of two NATS client (AIA enabled at leaf and cert) under good path (different intermediates)
// and ocsp cache type of none (no-op)
func TestOCSPPeerGoodClientsNoneCache(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	rootCAResponder := NewOCSPResponderRootCA(t)
	rootCAResponderURL := fmt.Sprintf("http://%s", rootCAResponder.Addr)
	defer rootCAResponder.Shutdown(ctx)
	SetOCSPStatus(t, rootCAResponderURL, "configs/certs/ocsp_peer/mini-ca/intermediate1/intermediate1_cert.pem", ocsp.Good)
	SetOCSPStatus(t, rootCAResponderURL, "configs/certs/ocsp_peer/mini-ca/intermediate2/intermediate2_cert.pem", ocsp.Good)

	intermediateCA1Responder := NewOCSPResponderIntermediateCA1(t)
	intermediateCA1ResponderURL := fmt.Sprintf("http://%s", intermediateCA1Responder.Addr)
	defer intermediateCA1Responder.Shutdown(ctx)
	SetOCSPStatus(t, intermediateCA1ResponderURL, "configs/certs/ocsp_peer/mini-ca/client1/UserA1_cert.pem", ocsp.Good)

	intermediateCA2Responder := NewOCSPResponderIntermediateCA2(t)
	intermediateCA2ResponderURL := fmt.Sprintf("http://%s", intermediateCA2Responder.Addr)
	defer intermediateCA2Responder.Shutdown(ctx)
	SetOCSPStatus(t, intermediateCA2ResponderURL, "configs/certs/ocsp_peer/mini-ca/client2/UserB1_cert.pem", ocsp.Good)

	deleteLocalStore(t, "")

	for _, test := range []struct {
		name      string
		config    string
		opts      []nats.Option
		err       error
		rerr      error
		configure func()
	}{
		{
			"None cache explicit long form: mTLS OCSP peer check on inbound client connection, client of intermediate CA 1",
			`
				port: -1
				tls: {
					cert_file: "configs/certs/ocsp_peer/mini-ca/server1/TestServer1_bundle.pem"
					key_file: "configs/certs/ocsp_peer/mini-ca/server1/private/TestServer1_keypair.pem"
					ca_file: "configs/certs/ocsp_peer/mini-ca/root/root_cert.pem"
					timeout: 5
					verify: true
					# Long form configuration
					ocsp_peer: {
						verify: true
						ca_timeout: 5
						allowed_clockskew: 30
					}
				}
				# Long form configuration
				ocsp_cache: {
					type: none
				}
			`,
			[]nats.Option{
				nats.ClientCert("./configs/certs/ocsp_peer/mini-ca/client1/UserA1_bundle.pem", "./configs/certs/ocsp_peer/mini-ca/client1/private/UserA1_keypair.pem"),
				nats.RootCAs("./configs/certs/ocsp_peer/mini-ca/root/root_cert.pem"),
				nats.ErrorHandler(noOpErrHandler),
			},
			nil,
			nil,
			func() {},
		},
		{
			"None cache explicit short form: mTLS OCSP peer check on inbound client connection, client of intermediate CA 1",
			`
				port: -1
				tls: {
					cert_file: "configs/certs/ocsp_peer/mini-ca/server1/TestServer1_bundle.pem"
					key_file: "configs/certs/ocsp_peer/mini-ca/server1/private/TestServer1_keypair.pem"
					ca_file: "configs/certs/ocsp_peer/mini-ca/root/root_cert.pem"
					timeout: 5
					verify: true
					# Long form configuration
					ocsp_peer: {
						verify: true
						ca_timeout: 5
						allowed_clockskew: 30
					}
				}
				# Short form configuration
				ocsp_cache: false
			`,
			[]nats.Option{
				nats.ClientCert("./configs/certs/ocsp_peer/mini-ca/client1/UserA1_bundle.pem", "./configs/certs/ocsp_peer/mini-ca/client1/private/UserA1_keypair.pem"),
				nats.RootCAs("./configs/certs/ocsp_peer/mini-ca/root/root_cert.pem"),
				nats.ErrorHandler(noOpErrHandler),
			},
			nil,
			nil,
			func() {},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			test.configure()
			content := test.config
			conf := createConfFile(t, []byte(content))
			s, opts := RunServerWithConfig(conf)
			defer s.Shutdown()
			nc, err := nats.Connect(fmt.Sprintf("tls://localhost:%d", opts.Port), test.opts...)
			if test.err == nil && err != nil {
				t.Errorf("Expected to connect, got %v", err)
			} else if test.err != nil && err == nil {
				t.Errorf("Expected error on connect")
			} else if test.err != nil && err != nil {
				// Error on connect was expected
				if test.err.Error() != err.Error() {
					t.Errorf("Expected error %s, got: %s", test.err, err)
				}
				return
			}
			defer nc.Close()
			nc.Subscribe("ping", func(m *nats.Msg) {
				m.Respond([]byte("pong"))
			})
			nc.Flush()
			_, err = nc.Request("ping", []byte("ping"), 250*time.Millisecond)
			if test.rerr != nil && err == nil {
				t.Errorf("Expected error getting response")
			} else if test.rerr == nil && err != nil {
				t.Errorf("Expected response")
			}
		})
	}
}

// TestOCSPPeerGoodClientsLocalCache is test of two NATS client (AIA enabled at leaf and cert) under good path (different intermediates)
// and leveraging the local ocsp cache type
func TestOCSPPeerGoodClientsLocalCache(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	rootCAResponder := NewOCSPResponderRootCA(t)
	rootCAResponderURL := fmt.Sprintf("http://%s", rootCAResponder.Addr)
	defer rootCAResponder.Shutdown(ctx)
	SetOCSPStatus(t, rootCAResponderURL, "configs/certs/ocsp_peer/mini-ca/intermediate1/intermediate1_cert.pem", ocsp.Good)
	SetOCSPStatus(t, rootCAResponderURL, "configs/certs/ocsp_peer/mini-ca/intermediate2/intermediate2_cert.pem", ocsp.Good)

	intermediateCA1Responder := NewOCSPResponderIntermediateCA1(t)
	intermediateCA1ResponderURL := fmt.Sprintf("http://%s", intermediateCA1Responder.Addr)
	defer intermediateCA1Responder.Shutdown(ctx)
	SetOCSPStatus(t, intermediateCA1ResponderURL, "configs/certs/ocsp_peer/mini-ca/client1/UserA1_cert.pem", ocsp.Good)

	intermediateCA2Responder := NewOCSPResponderIntermediateCA2(t)
	intermediateCA2ResponderURL := fmt.Sprintf("http://%s", intermediateCA2Responder.Addr)
	defer intermediateCA2Responder.Shutdown(ctx)
	SetOCSPStatus(t, intermediateCA2ResponderURL, "configs/certs/ocsp_peer/mini-ca/client2/UserB1_cert.pem", ocsp.Good)

	for _, test := range []struct {
		name      string
		config    string
		opts      []nats.Option
		err       error
		rerr      error
		configure func()
	}{
		{
			"Default cache, short form: mTLS OCSP peer check on inbound client connection, UserA1 client of intermediate CA 1",
			`
				port: -1
				http_port: 8222
				tls: {
					cert_file: "configs/certs/ocsp_peer/mini-ca/server1/TestServer1_bundle.pem"
					key_file: "configs/certs/ocsp_peer/mini-ca/server1/private/TestServer1_keypair.pem"
					ca_file: "configs/certs/ocsp_peer/mini-ca/root/root_cert.pem"
					timeout: 5
					verify: true
					# Long form configuration
					ocsp_peer: {
						verify: true
						ca_timeout: 5
						allowed_clockskew: 30
					}
				}
				# Short form configuration, local as default
				ocsp_cache: true
			`,
			[]nats.Option{
				nats.ClientCert("./configs/certs/ocsp_peer/mini-ca/client1/UserA1_bundle.pem", "./configs/certs/ocsp_peer/mini-ca/client1/private/UserA1_keypair.pem"),
				nats.RootCAs("./configs/certs/ocsp_peer/mini-ca/root/root_cert.pem"),
				nats.ErrorHandler(noOpErrHandler),
			},
			nil,
			nil,
			func() {},
		},
		{
			"Local cache long form: mTLS OCSP peer check on inbound client connection, UserB1 client of intermediate CA 2",
			`
				port: -1
				http_port: 8222

				tls: {
					cert_file: "configs/certs/ocsp_peer/mini-ca/server1/TestServer1_bundle.pem"
					key_file: "configs/certs/ocsp_peer/mini-ca/server1/private/TestServer1_keypair.pem"
					ca_file: "configs/certs/ocsp_peer/mini-ca/root/root_cert.pem"
					timeout: 5
					verify: true
					# Short form configuration
					ocsp_peer: true
				}
				# Long form configuration
				ocsp_cache: {
					type: local
				}
			`,
			[]nats.Option{
				nats.ClientCert("./configs/certs/ocsp_peer/mini-ca/client2/UserB1_bundle.pem", "./configs/certs/ocsp_peer/mini-ca/client2/private/UserB1_keypair.pem"),
				nats.RootCAs("./configs/certs/ocsp_peer/mini-ca/root/root_cert.pem"),
				nats.ErrorHandler(noOpErrHandler),
			},
			nil,
			nil,
			func() {},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			// Cleanup any previous test that saved a local cache
			deleteLocalStore(t, "")
			test.configure()
			content := test.config
			conf := createConfFile(t, []byte(content))
			s, opts := RunServerWithConfig(conf)
			defer s.Shutdown()
			nc, err := nats.Connect(fmt.Sprintf("tls://localhost:%d", opts.Port), test.opts...)
			if test.err == nil && err != nil {
				t.Errorf("Expected to connect, got %v", err)
			} else if test.err != nil && err == nil {
				t.Errorf("Expected error on connect")
			} else if test.err != nil && err != nil {
				// Error on connect was expected
				if test.err.Error() != err.Error() {
					t.Errorf("Expected error %s, got: %s", test.err, err)
				}
				return
			}
			nc.Close()

			v := monitorGetVarzHelper(t, 8222)
			if v.OCSPResponseCache == nil {
				t.Fatalf("Expected OCSP statistics to be in varz")
			}
			if v.OCSPResponseCache.Misses != 2 || v.OCSPResponseCache.Responses != 2 {
				t.Errorf("Expected cache misses and cache items to be 2, got %d and %d", v.OCSPResponseCache.Misses, v.OCSPResponseCache.Responses)
			}

			// Should get a cache hit now
			nc, err = nats.Connect(fmt.Sprintf("tls://localhost:%d", opts.Port), test.opts...)
			if test.err == nil && err != nil {
				t.Errorf("Expected to connect, got %v", err)
			} else if test.err != nil && err == nil {
				t.Errorf("Expected error on connect")
			} else if test.err != nil && err != nil {
				// Error on connect was expected
				if test.err.Error() != err.Error() {
					t.Errorf("Expected error %s, got: %s", test.err, err)
				}
				return
			}
			defer nc.Close()
			nc.Subscribe("ping", func(m *nats.Msg) {
				m.Respond([]byte("pong"))
			})
			nc.Flush()
			_, err = nc.Request("ping", []byte("ping"), 250*time.Millisecond)
			if test.rerr != nil && err == nil {
				t.Errorf("Expected error getting response")
			} else if test.rerr == nil && err != nil {
				t.Errorf("Expected response")
			}

			v = monitorGetVarzHelper(t, 8222)
			if v.OCSPResponseCache == nil {
				t.Fatalf("Expected OCSP statistics to be in varz")
			}
			if v.OCSPResponseCache.Misses != 2 || v.OCSPResponseCache.Hits != 2 || v.OCSPResponseCache.Responses != 2 {
				t.Errorf("Expected cache misses, hits and cache items to be 2, got %d and %d and %d", v.OCSPResponseCache.Misses, v.OCSPResponseCache.Hits, v.OCSPResponseCache.Responses)
			}
		})
	}
}

func TestOCSPPeerMonitor(t *testing.T) {
	for _, test := range []struct {
		name               string
		config             string
		NATSClient         bool
		WSClient           bool
		MQTTClient         bool
		LeafClient         bool
		LeafRemotes        bool
		NumTrueLeafRemotes int
	}{
		{
			"Monitor peer config setting on NATS client",
			`
				port: -1
				http_port: 8222
				# Default cache configuration
				tls: {
					cert_file: "configs/certs/ocsp_peer/mini-ca/server1/TestServer1_bundle.pem"
					key_file: "configs/certs/ocsp_peer/mini-ca/server1/private/TestServer1_keypair.pem"
					ca_file: "configs/certs/ocsp_peer/mini-ca/root/root_cert.pem"
					timeout: 5
					verify: true
					# Long form configuration
					ocsp_peer: {
						verify: true
					}
				}
			`,
			true,
			false,
			false,
			false,
			false,
			0,
		},
		{
			"Monitor peer config setting on Websockets client",
			`
				port: -1
				http_port: 8222
				# Default cache configuration
				websocket: {
					port: 8443
					tls: {
						cert_file: "configs/certs/ocsp_peer/mini-ca/server1/TestServer1_bundle.pem"
						key_file: "configs/certs/ocsp_peer/mini-ca/server1/private/TestServer1_keypair.pem"
						ca_file: "configs/certs/ocsp_peer/mini-ca/root/root_cert.pem"
						timeout: 5
						verify: true
						# Long form configuration
						ocsp_peer: {
							verify: true
						}
					}
				}
			`,
			false,
			true,
			false,
			false,
			false,
			0,
		},
		{
			"Monitor peer config setting on MQTT client",
			`
				port: -1
				http_port: 8222
				# Default cache configuration
				# Required for MQTT
				server_name: "my_mqtt_server"
				jetstream: {
					enabled: true
				}
				mqtt: {
					port: 1883
					tls: {
						cert_file: "configs/certs/ocsp_peer/mini-ca/server1/TestServer1_bundle.pem"
						key_file: "configs/certs/ocsp_peer/mini-ca/server1/private/TestServer1_keypair.pem"
						ca_file: "configs/certs/ocsp_peer/mini-ca/root/root_cert.pem"
						timeout: 5
						verify: true
						# Long form configuration
						ocsp_peer: {
							verify: true
						}
					}
				}
			`,
			false,
			false,
			true,
			false,
			false,
			0,
		},
		{
			"Monitor peer config setting on Leaf client",
			`
				port: -1
				http_port: 8222
				# Default cache configuration
				leaf: {
					port: 7422
					tls: {
						cert_file: "configs/certs/ocsp_peer/mini-ca/server1/TestServer1_bundle.pem"
						key_file: "configs/certs/ocsp_peer/mini-ca/server1/private/TestServer1_keypair.pem"
						ca_file: "configs/certs/ocsp_peer/mini-ca/root/root_cert.pem"
						timeout: 5
						verify: true
						# Long form configuration
						ocsp_peer: {
							verify: true
						}
					}
				}
			`,
			false,
			false,
			false,
			true,
			false,
			0,
		},
		{
			"Monitor peer config on some Leaf Remotes as well as Leaf client",
			`
				port: -1
				http_port: 8222
				# Default cache configuration
				leaf: {
					port: 7422
					tls: {
						cert_file: "configs/certs/ocsp_peer/mini-ca/server1/TestServer1_bundle.pem"
						key_file: "configs/certs/ocsp_peer/mini-ca/server1/private/TestServer1_keypair.pem"
						ca_file: "configs/certs/ocsp_peer/mini-ca/root/root_cert.pem"
						timeout: 5
						verify: true
						# Long form configuration
						ocsp_peer: {
							verify: true
						}
					}
					remotes: [
						{
							url: "nats-leaf://bogus:7422"
							tls: {
								cert_file: "configs/certs/ocsp_peer/mini-ca/server1/TestServer1_bundle.pem"
								key_file: "configs/certs/ocsp_peer/mini-ca/server1/private/TestServer1_keypair.pem"
								ca_file: "configs/certs/ocsp_peer/mini-ca/root/root_cert.pem"
								timeout: 5
								# Long form configuration
								ocsp_peer: {
									verify: true
								}
							}
						},
						{
							url: "nats-leaf://anotherbogus:7422"
							tls: {
								cert_file: "configs/certs/ocsp_peer/mini-ca/server1/TestServer1_bundle.pem"
								key_file: "configs/certs/ocsp_peer/mini-ca/server1/private/TestServer1_keypair.pem"
								ca_file: "configs/certs/ocsp_peer/mini-ca/root/root_cert.pem"
								timeout: 5
								# Short form configuration
								ocsp_peer: true
							}
						},
						{
							url: "nats-leaf://yetanotherbogus:7422"
							tls: {
								cert_file: "configs/certs/ocsp_peer/mini-ca/server1/TestServer1_bundle.pem"
								key_file: "configs/certs/ocsp_peer/mini-ca/server1/private/TestServer1_keypair.pem"
								ca_file: "configs/certs/ocsp_peer/mini-ca/root/root_cert.pem"
								timeout: 5
								# Peer not configured (default false)
							}
						}
					]
				}
			`,
			false,
			false,
			false,
			true,
			true,
			2,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			content := test.config
			conf := createConfFile(t, []byte(content))
			s, _ := RunServerWithConfig(conf)
			defer s.Shutdown()
			v := monitorGetVarzHelper(t, 8222)
			if test.NATSClient {
				if !v.TLSOCSPPeerVerify {
					t.Fatalf("Expected NATS Client TLSOCSPPeerVerify to be true, got false")
				}
			}
			if test.WSClient {
				if !v.Websocket.TLSOCSPPeerVerify {
					t.Fatalf("Expected WS Client TLSOCSPPeerVerify to be true, got false")
				}
			}
			if test.MQTTClient {
				if !v.MQTT.TLSOCSPPeerVerify {
					t.Fatalf("Expected MQTT Client TLSOCSPPeerVerify to be true, got false")
				}
			}
			if test.LeafClient {
				if !v.LeafNode.TLSOCSPPeerVerify {
					t.Fatalf("Expected Leaf Client TLSOCSPPeerVerify to be true, got false")
				}
			}
			if test.LeafRemotes {
				cnt := 0
				for _, r := range v.LeafNode.Remotes {
					if r.TLSOCSPPeerVerify {
						cnt++
					}
				}
				if cnt != test.NumTrueLeafRemotes {
					t.Fatalf("Expected %d Leaf Remotes with TLSOCSPPeerVerify true, got %d", test.NumTrueLeafRemotes, cnt)
				}
			}
		})
	}
}

func TestOCSPResponseCacheMonitor(t *testing.T) {
	for _, test := range []struct {
		name   string
		config string
		expect string
	}{
		{
			"Monitor local cache enabled, explicit cache true",
			`
				port: -1
				http_port: 8222
				tls: {
					cert_file: "configs/certs/ocsp_peer/mini-ca/server1/TestServer1_bundle.pem"
					key_file: "configs/certs/ocsp_peer/mini-ca/server1/private/TestServer1_keypair.pem"
					ca_file: "configs/certs/ocsp_peer/mini-ca/root/root_cert.pem"
					timeout: 5
					verify: true
					# Long form configuration
					ocsp_peer: {
						verify: true
					}
				}
				# Short form configuration
				ocsp_cache: true
			`,
			"local",
		},
		{
			"Monitor local cache enabled, explicit cache type local",
			`
				port: -1
				http_port: 8222
				tls: {
					cert_file: "configs/certs/ocsp_peer/mini-ca/server1/TestServer1_bundle.pem"
					key_file: "configs/certs/ocsp_peer/mini-ca/server1/private/TestServer1_keypair.pem"
					ca_file: "configs/certs/ocsp_peer/mini-ca/root/root_cert.pem"
					timeout: 5
					verify: true
					# Long form configuration
					ocsp_peer: {
						verify: true
					}
				}
				# Long form configuration
				ocsp_cache: {
					type: local
				}
			`,
			"local",
		},
		{
			"Monitor local cache enabled, implicit default",
			`
				port: -1
				http_port: 8222
				tls: {
					cert_file: "configs/certs/ocsp_peer/mini-ca/server1/TestServer1_bundle.pem"
					key_file: "configs/certs/ocsp_peer/mini-ca/server1/private/TestServer1_keypair.pem"
					ca_file: "configs/certs/ocsp_peer/mini-ca/root/root_cert.pem"
					timeout: 5
					verify: true
					# Long form configuration
					ocsp_peer: {
						verify: true
					}
				}
				# Short form configuration
				# ocsp_cache: true
			`,
			"local",
		},
		{
			"Monitor none cache enabled, explicit cache false (short)",
			`
				port: -1
				http_port: 8222
				tls: {
					cert_file: "configs/certs/ocsp_peer/mini-ca/server1/TestServer1_bundle.pem"
					key_file: "configs/certs/ocsp_peer/mini-ca/server1/private/TestServer1_keypair.pem"
					ca_file: "configs/certs/ocsp_peer/mini-ca/root/root_cert.pem"
					timeout: 5
					verify: true
					# Long form configuration
					ocsp_peer: {
						verify: true
					}
				}
				# Short form configuration
				ocsp_cache: false
			`,
			"",
		},
		{
			"Monitor none cache enabled, explicit cache false (long)",
			`
				port: -1
				http_port: 8222
				tls: {
					cert_file: "configs/certs/ocsp_peer/mini-ca/server1/TestServer1_bundle.pem"
					key_file: "configs/certs/ocsp_peer/mini-ca/server1/private/TestServer1_keypair.pem"
					ca_file: "configs/certs/ocsp_peer/mini-ca/root/root_cert.pem"
					timeout: 5
					verify: true
					# Long form configuration
					ocsp_peer: {
						verify: true
					}
				}
				# Long form configuration
				ocsp_cache: {
					type: none
				}
			`,
			"",
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			deleteLocalStore(t, "")
			content := test.config
			conf := createConfFile(t, []byte(content))
			s, _ := RunServerWithConfig(conf)
			defer s.Shutdown()
			v := monitorGetVarzHelper(t, 8222)
			var ct string
			if v.OCSPResponseCache != nil {
				ct = v.OCSPResponseCache.Type
			}
			if ct != test.expect {
				t.Fatalf("Expected OCSP Response Cache to be %s, got %s", test.expect, ct)
			}
		})
	}
}

func TestOCSPResponseCacheChangeAndReload(t *testing.T) {
	deleteLocalStore(t, "")

	// Start with ocsp cache set to none
	content := `
		port: -1
		http_port: 8222
		tls {
			cert_file: "configs/certs/ocsp_peer/mini-ca/server1/TestServer1_bundle.pem"
			key_file: "configs/certs/ocsp_peer/mini-ca/server1/private/TestServer1_keypair.pem"
			ca_file: "configs/certs/ocsp_peer/mini-ca/root/root_cert.pem"
			timeout: 5
			verify: true
			# Short form configuration
			ocsp_peer: true
		}
		# Long form configuration
		ocsp_cache: {
			type: none
		}
	`
	conf := createConfFile(t, []byte(content))
	s, _ := RunServerWithConfig(conf)
	defer s.Shutdown()
	v := monitorGetVarzHelper(t, 8222)
	var ct string
	if v.OCSPResponseCache != nil {
		ct = v.OCSPResponseCache.Type
	}
	if ct != "" {
		t.Fatalf("Expected OCSP Response Cache to have empty type in varz indicating none")
	}

	// Change to local cache
	content = `
		port: -1
		http_port: 8222
		tls {
			cert_file: "configs/certs/ocsp_peer/mini-ca/server1/TestServer1_bundle.pem"
			key_file: "configs/certs/ocsp_peer/mini-ca/server1/private/TestServer1_keypair.pem"
			ca_file: "configs/certs/ocsp_peer/mini-ca/root/root_cert.pem"
			timeout: 5
			verify: true
			# Short form configuration
			ocsp_peer: true
		}
		# Long form configuration
		ocsp_cache: {
			type: local
		}
	`
	if err := os.WriteFile(conf, []byte(content), 0666); err != nil {
		t.Fatalf("Error writing config: %v", err)
	}
	if err := s.Reload(); err != nil {
		t.Fatal(err)
	}
	time.Sleep(2 * time.Second)
	v = monitorGetVarzHelper(t, 8222)
	ct = ""
	if v.OCSPResponseCache != nil {
		ct = v.OCSPResponseCache.Type
	}
	if ct != "local" {
		t.Fatalf("Expected OCSP Response Cache type to be local, got %q", ct)
	}
}

func deleteLocalStore(t *testing.T, dir string) {
	t.Helper()
	if dir == "" {
		// default
		dir = "_rc_"
	}
	if err := os.RemoveAll(dir); err != nil {
		t.Fatalf("Error cleaning up local store: %v", err)
	}
}

func monitorGetVarzHelper(t *testing.T, httpPort int) *server.Varz {
	t.Helper()
	url := fmt.Sprintf("http://127.0.0.1:%d/", httpPort)
	resp, err := http.Get(url + "varz")
	if err != nil {
		t.Fatalf("Expected no error: Got %v\n", err)
	}
	if resp.StatusCode != 200 {
		t.Fatalf("Expected a 200 response, got %d\n", resp.StatusCode)
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("Got an error reading the body: %v\n", err)
	}
	v := server.Varz{}
	if err := json.Unmarshal(body, &v); err != nil {
		t.Fatalf("Got an error unmarshalling the body: %v\n", err)
	}
	return &v
}

func writeCacheFile(dir string, content []byte) error {
	if dir == "" {
		dir = "_rc_"
	}
	err := os.MkdirAll(filepath.Join(dir), os.ModePerm)
	if err != nil {
		return err
	}
	return os.WriteFile(filepath.Join(dir, "cache.json"), content, os.ModePerm)
}

// TestOCSPPeerPreserveRevokedCacheItem is test of the preserve_revoked cache policy
func TestOCSPPeerPreserveRevokedCacheItem(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	rootCAResponder := NewOCSPResponderRootCA(t)
	rootCAResponderURL := fmt.Sprintf("http://%s", rootCAResponder.Addr)
	defer rootCAResponder.Shutdown(ctx)
	SetOCSPStatus(t, rootCAResponderURL, "configs/certs/ocsp_peer/mini-ca/intermediate1/intermediate1_cert.pem", ocsp.Good)

	for _, test := range []struct {
		name      string
		config    string
		opts      []nats.Option
		responses int64
		revokes   int64
		goods     int64
		unknowns  int64
		err       error
		rerr      error
		clean     bool
	}{
		{
			"Test expired revoked cert not actually deleted",
			`
				port: -1
				http_port: 8222
				tls: {
					cert_file: "configs/certs/ocsp_peer/mini-ca/server1/TestServer1_bundle.pem"
					key_file: "configs/certs/ocsp_peer/mini-ca/server1/private/TestServer1_keypair.pem"
					ca_file: "configs/certs/ocsp_peer/mini-ca/root/root_cert.pem"
					timeout: 5
					verify: true
					# Turn on CA OCSP check so this revoked client should NOT be able to connect
					ocsp_peer: {
						verify: true
						ca_timeout: 0.5
					}
				}
				# preserve revoked true
				ocsp_cache: {
					type: local
					preserve_revoked: true
				}
			`,
			[]nats.Option{
				nats.ClientCert("./configs/certs/ocsp_peer/mini-ca/client1/UserA1_bundle.pem", "./configs/certs/ocsp_peer/mini-ca/client1/private/UserA1_keypair.pem"),
				nats.RootCAs("./configs/certs/ocsp_peer/mini-ca/root/root_cert.pem"),
				nats.ErrorHandler(noOpErrHandler),
			},
			1,
			1,
			0,
			0,
			errors.New("remote error: tls: bad certificate"),
			errors.New("expect error"),
			true,
		},
		{
			"Test expired revoked cert replaced by current good cert",
			`
				port: -1
				http_port: 8222
				tls: {
					cert_file: "configs/certs/ocsp_peer/mini-ca/server1/TestServer1_bundle.pem"
					key_file: "configs/certs/ocsp_peer/mini-ca/server1/private/TestServer1_keypair.pem"
					ca_file: "configs/certs/ocsp_peer/mini-ca/root/root_cert.pem"
					timeout: 5
					verify: true
					# Turn on CA OCSP check so this revoked client should NOT be able to connect
					ocsp_peer: true
				}
				# preserve revoked true
				ocsp_cache: {
					type: local
					preserve_revoked: true
				}
			`,
			[]nats.Option{
				nats.ClientCert("./configs/certs/ocsp_peer/mini-ca/client1/UserA1_bundle.pem", "./configs/certs/ocsp_peer/mini-ca/client1/private/UserA1_keypair.pem"),
				nats.RootCAs("./configs/certs/ocsp_peer/mini-ca/root/root_cert.pem"),
				nats.ErrorHandler(noOpErrHandler),
			},
			2,
			0,
			2,
			0,
			nil,
			nil,
			false,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			var intermediateCA1Responder *http.Server
			// clean slate starting the test and start the leaf CA responder for first run
			if test.clean {
				deleteLocalStore(t, "")
				// establish the revoked item (expired) in cache
				c := []byte(`
				{
				 "5xL/SuHl6JN0OmxrNMpzVMTA73JVYcRfGX8+HvJinEI=": {
				  "subject": "CN=UserA1,O=Testnats,L=Tacoma,ST=WA,C=US",
				  "cached_at": "2023-05-29T17:56:45Z",
				  "resp_status": "revoked",
				  "resp_expires": "2023-05-29T17:56:49Z",
				  "resp": "/wYAAFMyc1R3TwBSBQBao1Qr1QzUMIIGUQoBAKCCBkowggZGBgkrBgEFBQcwAQEEggY3MIIGMzCB46FZMFcxCzAJBgNVBAYTAlVTEQ0gCAwCV0ExDzANAQ0wBwwGVGFjb21hMREwDwEROAoMCFRlc3RuYXRzMRcwFQET8HQDDA5PQ1NQIFJlc3BvbmRlchgPMjAyMzA1MjkxNzU2MDBaMHUwczBNMAkGBSsOAwIaBQAEFKgwn5fplwQy+DsulBg5SRpx0iaYBBS1kW5PZLcWhHb5tL6ZzmCVmBqOnQIUXKGv1Xy7Fu/Cx+ZT/JQa7SS7tBc2ZAAQNDVaoBE2dwD0QQE0OVowDQYJKoZIhvcNAQELBQADggEBAGAax/vkv3SBFNbxp2utc/N6Rje4E0ceC972sWgqYjzYrH0oc/acg+OAXaxUjwqoQWaT+dHaI4D5qoTkMx7XlWATjI2L72IUTf6Luo92jPzyDFwb10CdeFHtRtEYD54Qbi/nD4oxQ8cSoLKC3wft2l3E/mK/1I4Mxwq15CioK4MhfzTISoeGZbjDXPKgloJOG3rn9v64vFGV6dosbLgaXEs+MPcCsPQYkwhOOyazuewRmIDOBp5QSsKPhqsT8Rs20t8LGTMkvjZniFWJs90l9QL9F1m3obq5nyuxrGt+7Rf5zoj4T+0XCOGtE+b7cRCLg43tFuTbaAQG8Z+qkPzpza+gggQ1MIIEMTCCBC0wggMVoAMCAQICFCnhUo39pSqH6x3kHUds4YpYaXOrOj8BBDBaUSLaLwIIGjAYSS+oEUludGVybWVkaWF0ZSBDQSAxMB4XDTIzMDUwMTE5MjgzOVoXDTMzMDQyOA0PUasVAEkMMIIBIi4nAgABBQD0QAEPADCCAQoCggEBAKMMyuuA66EOHnGb07P5Zc5wwiEGPDHBBn6lqErhIaN0VJ9XzlDWwyk8Q7CdPlSU7o36DXFs316eATB5bLuXXa+7WwV3cp9V5mZF9OLCz3sOWNYUanYprOMwKA3uvcqqrh8e70Dzw6sX8tfsDeH7aJoJg5kRWEKU+A3Umm+fO+hW8Km3GBqRQXxD49uxAfGtCznXZZjmFbAXqVZu+4R6wMxndfz2dYQxeMVtUY/QGdMWT4fvWzO5et3+X6hq/URUAPOkplv9O2U4T4JPucS9yZpW/FTxWC/L7vQI/bfsrSgIZpv4eJgy27FW3Q4xusbjVvUCL/t2KLvEi/Nr2qodOCECAwEAAaOB7TCB6jAdBgNVHQ4EFgQUy15QYHqrL6k7HiSrAkKN7IFgSBMwHwYDVR0jBBgwFoBSyQNQMAwGA1UdEwEB/wQCMAAwDgYDVR0PAQ4YBAMCB4AwFgEeACUBEBAMMAoGCIm0sAMJMD0GA1UdHwQ2MDQwMqAwoC6GLGh0dHA6Ly8xMjcuMC4wLjE6MTg4ODgvaV1WKDFfY3JsLmRlcjAzEUscAQEEJzAlMCMRWwwwAYYXWkoALhICkTnw/0hlzm2RRjA3tvJ2wELj9e7pMg5GtdWdrLDyI/U1qBxhZoHADbyku7W+R1iL8dFfc4PSmdo+owsygZakvahXjv49xJNX7wV3YMmIHC4lfurIlY2mSnPlu2zEOwEDkI0S9WkTxXmHrkXLSciQJDkwzye6MR5fW+APk4JmKDPc46Go/K1A0EgxY/ugahMYsYtZu++W+IOYbEoYNxoCrcJCHX4c3Ep3t/Wulz4X6DWWhaDkMMUDC2JVE8E/3xUbw0X3adZe9Xf8T+goOz7wLCAigXKj1hvRUmOGISIGelv0KsfluZesG1a1TGLp+W9JX0M9nOaFOvjJTDP96aqIjs8oXGk="
				 }
				}`)
				err := writeCacheFile("", c)
				if err != nil {
					t.Fatal(err)
				}
			} else {
				intermediateCA1Responder = NewOCSPResponderIntermediateCA1(t)
				intermediateCA1ResponderURL := fmt.Sprintf("http://%s", intermediateCA1Responder.Addr)
				SetOCSPStatus(t, intermediateCA1ResponderURL, "configs/certs/ocsp_peer/mini-ca/client1/UserA1_cert.pem", ocsp.Good)
				defer intermediateCA1Responder.Shutdown(ctx)
			}
			content := test.config
			conf := createConfFile(t, []byte(content))
			s, opts := RunServerWithConfig(conf)
			defer s.Shutdown()
			nc, err := nats.Connect(fmt.Sprintf("tls://localhost:%d", opts.Port), test.opts...)
			if test.err == nil && err != nil {
				t.Errorf("Expected to connect, got %v", err)
			} else if test.err != nil && err == nil {
				t.Errorf("Expected error on connect")
			} else if test.err != nil && err != nil {
				// Error on connect was expected
				if test.err.Error() != err.Error() {
					t.Errorf("Expected error %s, got: %s", test.err, err)
				}
				return
			}
			defer nc.Close()
			v := monitorGetVarzHelper(t, 8222)
			if v.OCSPResponseCache == nil {
				t.Fatalf("Expected OCSP statistics to be in varz")
			}
			responses := v.OCSPResponseCache.Responses
			revokes := v.OCSPResponseCache.Revokes
			goods := v.OCSPResponseCache.Goods
			unknowns := v.OCSPResponseCache.Unknowns
			if !(responses == test.responses && revokes == test.revokes && goods == test.goods && unknowns == test.unknowns) {
				t.Fatalf("Expected %d response, %d revoked, %d good, %d unknown; got [%d] and [%d] and [%d] and [%d]", test.responses, test.revokes, test.goods, test.unknowns, responses, revokes, goods, unknowns)
			}
		})
	}
}

// TestOCSPStapleFeatureInterop is a test of a NATS client (AIA enabled at leaf and cert) connecting to a NATS Server
// in which both ocsp_peer is enabled on NATS client connections (verify client) and the ocsp staple is enabled such
// that the NATS Server will staple its own OCSP response and make available to the NATS client during handshake.
func TestOCSPStapleFeatureInterop(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	rootCAResponder := NewOCSPResponderRootCA(t)
	rootCAResponderURL := fmt.Sprintf("http://%s", rootCAResponder.Addr)
	defer rootCAResponder.Shutdown(ctx)
	SetOCSPStatus(t, rootCAResponderURL, "configs/certs/ocsp_peer/mini-ca/intermediate1/intermediate1_cert.pem", ocsp.Good)

	intermediateCA1Responder := NewOCSPResponderIntermediateCA1(t)
	intermediateCA1ResponderURL := fmt.Sprintf("http://%s", intermediateCA1Responder.Addr)
	defer intermediateCA1Responder.Shutdown(ctx)
	SetOCSPStatus(t, intermediateCA1ResponderURL, "configs/certs/ocsp_peer/mini-ca/server1/TestServer1_cert.pem", ocsp.Good)

	for _, test := range []struct {
		name      string
		config    string
		opts      []nats.Option
		err       error
		rerr      error
		configure func()
	}{
		{
			"Interop: Both Good: mTLS OCSP peer check on inbound client connection and server's OCSP staple validated at client",
			`
				port: -1
				ocsp_cache: true
				ocsp: {
					mode: always
				}
				tls: {
					cert_file: "configs/certs/ocsp_peer/mini-ca/server1/TestServer1_bundle.pem"
					key_file: "configs/certs/ocsp_peer/mini-ca/server1/private/TestServer1_keypair.pem"
					ca_file: "configs/certs/ocsp_peer/mini-ca/root/root_cert.pem"
					timeout: 5
					verify: true
					# Long form configuration, non-default ca_timeout
					ocsp_peer: {
						verify: true
						ca_timeout: 5
						allowed_clockskew: 30
					}
				}
			`,
			[]nats.Option{
				nats.Secure(&tls.Config{
					VerifyConnection: func(s tls.ConnectionState) error {
						if s.OCSPResponse == nil {
							return fmt.Errorf("expected OCSP staple to be present")
						}
						resp, err := ocsp.ParseResponse(s.OCSPResponse, s.VerifiedChains[0][1])
						if err != nil || resp.Status != ocsp.Good {
							return fmt.Errorf("expected a valid GOOD stapled response")
						}
						return nil
					},
				}),
				nats.ClientCert("./configs/certs/ocsp_peer/mini-ca/client1/UserA1_bundle.pem", "./configs/certs/ocsp_peer/mini-ca/client1/private/UserA1_keypair.pem"),
				nats.RootCAs("./configs/certs/ocsp_peer/mini-ca/root/root_cert.pem"),
				nats.ErrorHandler(noOpErrHandler),
			},
			nil,
			nil,
			func() {
				SetOCSPStatus(t, intermediateCA1ResponderURL, "configs/certs/ocsp_peer/mini-ca/client1/UserA1_cert.pem", ocsp.Good)
			},
		},
		{
			"Interop: Bad Client: mTLS OCSP peer check on inbound client connection and server's OCSP staple validated at client",
			`
				port: -1
				ocsp_cache: true
				ocsp: {
					mode: always
				}
				tls: {
					cert_file: "configs/certs/ocsp_peer/mini-ca/server1/TestServer1_bundle.pem"
					key_file: "configs/certs/ocsp_peer/mini-ca/server1/private/TestServer1_keypair.pem"
					ca_file: "configs/certs/ocsp_peer/mini-ca/root/root_cert.pem"
					timeout: 5
					verify: true
					# Long form configuration, non-default ca_timeout
					ocsp_peer: {
						verify: true
						ca_timeout: 5
						allowed_clockskew: 30
					}
				}
			`,
			[]nats.Option{
				nats.Secure(&tls.Config{
					VerifyConnection: func(s tls.ConnectionState) error {
						if s.OCSPResponse == nil {
							return fmt.Errorf("expected OCSP staple to be present")
						}
						resp, err := ocsp.ParseResponse(s.OCSPResponse, s.VerifiedChains[0][1])
						if err != nil || resp.Status != ocsp.Good {
							return fmt.Errorf("expected a valid GOOD stapled response")
						}
						return nil
					},
				}),
				nats.ClientCert("./configs/certs/ocsp_peer/mini-ca/client1/UserA1_bundle.pem", "./configs/certs/ocsp_peer/mini-ca/client1/private/UserA1_keypair.pem"),
				nats.RootCAs("./configs/certs/ocsp_peer/mini-ca/root/root_cert.pem"),
				nats.ErrorHandler(noOpErrHandler),
			},
			fmt.Errorf("remote error: tls: bad certificate"),
			nil,
			func() {
				SetOCSPStatus(t, intermediateCA1ResponderURL, "configs/certs/ocsp_peer/mini-ca/client1/UserA1_cert.pem", ocsp.Revoked)
			},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			deleteLocalStore(t, "")
			test.configure()
			content := test.config
			conf := createConfFile(t, []byte(content))

			s, opts := RunServerWithConfig(conf)
			defer s.Shutdown()
			nc, err := nats.Connect(fmt.Sprintf("tls://localhost:%d", opts.Port), test.opts...)
			if test.err == nil && err != nil {
				t.Errorf("Expected to connect, got %v", err)
			} else if test.err != nil && err == nil {
				t.Errorf("Expected error on connect")
			} else if test.err != nil && err != nil {
				// Error on connect was expected
				if test.err.Error() != err.Error() {
					t.Errorf("Expected error %s, got: %s", test.err, err)
				}
				return
			}
			defer nc.Close()
			nc.Subscribe("ping", func(m *nats.Msg) {
				m.Respond([]byte("pong"))
			})
			nc.Flush()
			_, err = nc.Request("ping", []byte("ping"), 250*time.Millisecond)
			if test.rerr != nil && err == nil {
				t.Errorf("Expected error getting response")
			} else if test.rerr == nil && err != nil {
				t.Errorf("Expected response")
			}
		})
	}
}

// TestOCSPPeerWarnOnlyOption is test of NATS client that is OCSP Revoked status but allowed to pass with warn_only option
func TestOCSPPeerWarnOnlyOption(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	rootCAResponder := NewOCSPResponderRootCA(t)
	rootCAResponderURL := fmt.Sprintf("http://%s", rootCAResponder.Addr)
	defer rootCAResponder.Shutdown(ctx)
	SetOCSPStatus(t, rootCAResponderURL, "configs/certs/ocsp_peer/mini-ca/intermediate1/intermediate1_cert.pem", ocsp.Good)

	intermediateCA1Responder := NewOCSPResponderIntermediateCA1(t)
	intermediateCA1ResponderURL := fmt.Sprintf("http://%s", intermediateCA1Responder.Addr)
	defer intermediateCA1Responder.Shutdown(ctx)
	SetOCSPStatus(t, intermediateCA1ResponderURL, "configs/certs/ocsp_peer/mini-ca/client1/UserA1_cert.pem", ocsp.Revoked)

	for _, test := range []struct {
		name      string
		config    string
		opts      []nats.Option
		err       error
		rerr      error
		configure func()
	}{
		{
			"Revoked NATS client with warn_only explicitly set to false",
			`
				port: -1
				# Cache configuration is default
				tls: {
					cert_file: "configs/certs/ocsp_peer/mini-ca/server1/TestServer1_bundle.pem"
					key_file: "configs/certs/ocsp_peer/mini-ca/server1/private/TestServer1_keypair.pem"
					ca_file: "configs/certs/ocsp_peer/mini-ca/root/root_cert.pem"
					timeout: 5
					verify: true
					# Enable OCSP peer but with warn_only option set to false
					ocsp_peer: {
						verify: true
						warn_only: false
					}
				}
			`,
			[]nats.Option{
				nats.ClientCert("./configs/certs/ocsp_peer/mini-ca/client1/UserA1_bundle.pem", "./configs/certs/ocsp_peer/mini-ca/client1/private/UserA1_keypair.pem"),
				nats.RootCAs("./configs/certs/ocsp_peer/mini-ca/root/root_cert.pem"),
				nats.ErrorHandler(noOpErrHandler),
			},
			errors.New("remote error: tls: bad certificate"),
			errors.New("expect error"),
			func() {},
		},
		{
			"Revoked NATS client with warn_only explicitly set to true",
			`
				port: -1
				# Cache configuration is default
				tls: {
					cert_file: "configs/certs/ocsp_peer/mini-ca/server1/TestServer1_bundle.pem"
					key_file: "configs/certs/ocsp_peer/mini-ca/server1/private/TestServer1_keypair.pem"
					ca_file: "configs/certs/ocsp_peer/mini-ca/root/root_cert.pem"
					timeout: 5
					verify: true
					# Enable OCSP peer but with warn_only option set to true
					ocsp_peer: {
						verify: true
						warn_only: true
					}
				}
			`,
			[]nats.Option{
				nats.ClientCert("./configs/certs/ocsp_peer/mini-ca/client1/UserA1_bundle.pem", "./configs/certs/ocsp_peer/mini-ca/client1/private/UserA1_keypair.pem"),
				nats.RootCAs("./configs/certs/ocsp_peer/mini-ca/root/root_cert.pem"),
				nats.ErrorHandler(noOpErrHandler),
			},
			nil,
			nil,
			func() {},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			deleteLocalStore(t, "")
			test.configure()
			content := test.config
			conf := createConfFile(t, []byte(content))
			s, opts := RunServerWithConfig(conf)
			defer s.Shutdown()
			nc, err := nats.Connect(fmt.Sprintf("tls://localhost:%d", opts.Port), test.opts...)
			if test.err == nil && err != nil {
				t.Errorf("Expected to connect, got %v", err)
			} else if test.err != nil && err == nil {
				t.Errorf("Expected error on connect")
			} else if test.err != nil && err != nil {
				// Error on connect was expected
				if test.err.Error() != err.Error() {
					t.Errorf("Expected error %s, got: %s", test.err, err)
				}
				return
			}
			defer nc.Close()
		})
	}
}

// TestOCSPPeerUnknownIsGoodOption is test of NATS client that is OCSP status Unknown from its OCSP Responder but we treat
// status Unknown as "Good"
func TestOCSPPeerUnknownIsGoodOption(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	rootCAResponder := NewOCSPResponderRootCA(t)
	rootCAResponderURL := fmt.Sprintf("http://%s", rootCAResponder.Addr)
	defer rootCAResponder.Shutdown(ctx)
	SetOCSPStatus(t, rootCAResponderURL, "configs/certs/ocsp_peer/mini-ca/intermediate1/intermediate1_cert.pem", ocsp.Good)

	intermediateCA1Responder := NewOCSPResponderIntermediateCA1(t)
	defer intermediateCA1Responder.Shutdown(ctx)

	for _, test := range []struct {
		name      string
		config    string
		opts      []nats.Option
		err       error
		rerr      error
		configure func()
	}{
		{
			"Unknown NATS client with no unknown_is_good option set (default false)",
			`
				port: -1
				# Cache configuration is default
				tls: {
					cert_file: "configs/certs/ocsp_peer/mini-ca/server1/TestServer1_bundle.pem"
					key_file: "configs/certs/ocsp_peer/mini-ca/server1/private/TestServer1_keypair.pem"
					ca_file: "configs/certs/ocsp_peer/mini-ca/root/root_cert.pem"
					timeout: 5
					verify: true
					# Short form configuration
					ocsp_peer: true
				}
			`,
			[]nats.Option{
				nats.ClientCert("./configs/certs/ocsp_peer/mini-ca/client1/UserA1_bundle.pem", "./configs/certs/ocsp_peer/mini-ca/client1/private/UserA1_keypair.pem"),
				nats.RootCAs("./configs/certs/ocsp_peer/mini-ca/root/root_cert.pem"),
				nats.ErrorHandler(noOpErrHandler),
			},
			errors.New("remote error: tls: bad certificate"),
			errors.New("expect error"),
			func() {},
		},
		{
			"Unknown NATS client with unknown_is_good set to true",
			`
				port: -1
				# Cache configuration is default
				tls: {
					cert_file: "configs/certs/ocsp_peer/mini-ca/server1/TestServer1_bundle.pem"
					key_file: "configs/certs/ocsp_peer/mini-ca/server1/private/TestServer1_keypair.pem"
					ca_file: "configs/certs/ocsp_peer/mini-ca/root/root_cert.pem"
					timeout: 5
					verify: true
					# Long form configuration
					ocsp_peer: {
						verify: true
						unknown_is_good: true
					}
				}
			`,
			[]nats.Option{
				nats.ClientCert("./configs/certs/ocsp_peer/mini-ca/client1/UserA1_bundle.pem", "./configs/certs/ocsp_peer/mini-ca/client1/private/UserA1_keypair.pem"),
				nats.RootCAs("./configs/certs/ocsp_peer/mini-ca/root/root_cert.pem"),
				nats.ErrorHandler(noOpErrHandler),
			},
			nil,
			nil,
			func() {},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			deleteLocalStore(t, "")
			test.configure()
			content := test.config
			conf := createConfFile(t, []byte(content))
			s, opts := RunServerWithConfig(conf)
			defer s.Shutdown()
			nc, err := nats.Connect(fmt.Sprintf("tls://localhost:%d", opts.Port), test.opts...)
			if test.err == nil && err != nil {
				t.Errorf("Expected to connect, got %v", err)
			} else if test.err != nil && err == nil {
				t.Errorf("Expected error on connect")
			} else if test.err != nil && err != nil {
				// Error on connect was expected
				if test.err.Error() != err.Error() {
					t.Errorf("Expected error %s, got: %s", test.err, err)
				}
				return
			}
			defer nc.Close()
		})
	}
}

// TestOCSPPeerAllowWhenCAUnreachableOption is test of the allow_when_ca_unreachable peer option
func TestOCSPPeerAllowWhenCAUnreachableOption(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	rootCAResponder := NewOCSPResponderRootCA(t)
	rootCAResponderURL := fmt.Sprintf("http://%s", rootCAResponder.Addr)
	defer rootCAResponder.Shutdown(ctx)
	SetOCSPStatus(t, rootCAResponderURL, "configs/certs/ocsp_peer/mini-ca/intermediate1/intermediate1_cert.pem", ocsp.Good)

	for _, test := range []struct {
		name           string
		config         string
		opts           []nats.Option
		cachedResponse string
		err            error
		rerr           error
	}{
		{
			"Expired Revoked response in cache for UserA1 -- should be rejected connection (expired revoke honored)",
			`
				port: -1
				http_port: 8222
				tls: {
					cert_file: "configs/certs/ocsp_peer/mini-ca/server1/TestServer1_bundle.pem"
					key_file: "configs/certs/ocsp_peer/mini-ca/server1/private/TestServer1_keypair.pem"
					ca_file: "configs/certs/ocsp_peer/mini-ca/root/root_cert.pem"
					timeout: 5
					verify: true
					# Turn on CA OCSP check but allow when CA is unreachable
					ocsp_peer: {
						verify: true
						ca_timeout: 0.5
						allow_when_ca_unreachable: true
					}
				}
				# preserve revoked true
				ocsp_cache: {
					type: local
					preserve_revoked: true
				}
			`,
			[]nats.Option{
				nats.ClientCert("./configs/certs/ocsp_peer/mini-ca/client1/UserA1_bundle.pem", "./configs/certs/ocsp_peer/mini-ca/client1/private/UserA1_keypair.pem"),
				nats.RootCAs("./configs/certs/ocsp_peer/mini-ca/root/root_cert.pem"),
				nats.ErrorHandler(noOpErrHandler),
			},
			`
				{
					"5xL/SuHl6JN0OmxrNMpzVMTA73JVYcRfGX8+HvJinEI=": {
						"subject": "CN=UserA1,O=Testnats,L=Tacoma,ST=WA,C=US",
						"cached_at": "2023-05-29T17:56:45Z",
						"resp_status": "revoked",
						"resp_expires": "2023-05-29T17:56:49Z",
						"resp": "/wYAAFMyc1R3TwBSBQBao1Qr1QzUMIIGUQoBAKCCBkowggZGBgkrBgEFBQcwAQEEggY3MIIGMzCB46FZMFcxCzAJBgNVBAYTAlVTEQ0gCAwCV0ExDzANAQ0wBwwGVGFjb21hMREwDwEROAoMCFRlc3RuYXRzMRcwFQET8HQDDA5PQ1NQIFJlc3BvbmRlchgPMjAyMzA1MjkxNzU2MDBaMHUwczBNMAkGBSsOAwIaBQAEFKgwn5fplwQy+DsulBg5SRpx0iaYBBS1kW5PZLcWhHb5tL6ZzmCVmBqOnQIUXKGv1Xy7Fu/Cx+ZT/JQa7SS7tBc2ZAAQNDVaoBE2dwD0QQE0OVowDQYJKoZIhvcNAQELBQADggEBAGAax/vkv3SBFNbxp2utc/N6Rje4E0ceC972sWgqYjzYrH0oc/acg+OAXaxUjwqoQWaT+dHaI4D5qoTkMx7XlWATjI2L72IUTf6Luo92jPzyDFwb10CdeFHtRtEYD54Qbi/nD4oxQ8cSoLKC3wft2l3E/mK/1I4Mxwq15CioK4MhfzTISoeGZbjDXPKgloJOG3rn9v64vFGV6dosbLgaXEs+MPcCsPQYkwhOOyazuewRmIDOBp5QSsKPhqsT8Rs20t8LGTMkvjZniFWJs90l9QL9F1m3obq5nyuxrGt+7Rf5zoj4T+0XCOGtE+b7cRCLg43tFuTbaAQG8Z+qkPzpza+gggQ1MIIEMTCCBC0wggMVoAMCAQICFCnhUo39pSqH6x3kHUds4YpYaXOrOj8BBDBaUSLaLwIIGjAYSS+oEUludGVybWVkaWF0ZSBDQSAxMB4XDTIzMDUwMTE5MjgzOVoXDTMzMDQyOA0PUasVAEkMMIIBIi4nAgABBQD0QAEPADCCAQoCggEBAKMMyuuA66EOHnGb07P5Zc5wwiEGPDHBBn6lqErhIaN0VJ9XzlDWwyk8Q7CdPlSU7o36DXFs316eATB5bLuXXa+7WwV3cp9V5mZF9OLCz3sOWNYUanYprOMwKA3uvcqqrh8e70Dzw6sX8tfsDeH7aJoJg5kRWEKU+A3Umm+fO+hW8Km3GBqRQXxD49uxAfGtCznXZZjmFbAXqVZu+4R6wMxndfz2dYQxeMVtUY/QGdMWT4fvWzO5et3+X6hq/URUAPOkplv9O2U4T4JPucS9yZpW/FTxWC/L7vQI/bfsrSgIZpv4eJgy27FW3Q4xusbjVvUCL/t2KLvEi/Nr2qodOCECAwEAAaOB7TCB6jAdBgNVHQ4EFgQUy15QYHqrL6k7HiSrAkKN7IFgSBMwHwYDVR0jBBgwFoBSyQNQMAwGA1UdEwEB/wQCMAAwDgYDVR0PAQ4YBAMCB4AwFgEeACUBEBAMMAoGCIm0sAMJMD0GA1UdHwQ2MDQwMqAwoC6GLGh0dHA6Ly8xMjcuMC4wLjE6MTg4ODgvaV1WKDFfY3JsLmRlcjAzEUscAQEEJzAlMCMRWwwwAYYXWkoALhICkTnw/0hlzm2RRjA3tvJ2wELj9e7pMg5GtdWdrLDyI/U1qBxhZoHADbyku7W+R1iL8dFfc4PSmdo+owsygZakvahXjv49xJNX7wV3YMmIHC4lfurIlY2mSnPlu2zEOwEDkI0S9WkTxXmHrkXLSciQJDkwzye6MR5fW+APk4JmKDPc46Go/K1A0EgxY/ugahMYsYtZu++W+IOYbEoYNxoCrcJCHX4c3Ep3t/Wulz4X6DWWhaDkMMUDC2JVE8E/3xUbw0X3adZe9Xf8T+goOz7wLCAigXKj1hvRUmOGISIGelv0KsfluZesG1a1TGLp+W9JX0M9nOaFOvjJTDP96aqIjs8oXGk="
					}
				}`,
			errors.New("remote error: tls: bad certificate"),
			errors.New("expect error"),
		},
		{
			"Expired Good response in cache for UserA1 -- should be allowed connection (cached item irrelevant)",
			`
				port: -1
				http_port: 8222
				tls: {
					cert_file: "configs/certs/ocsp_peer/mini-ca/server1/TestServer1_bundle.pem"
					key_file: "configs/certs/ocsp_peer/mini-ca/server1/private/TestServer1_keypair.pem"
					ca_file: "configs/certs/ocsp_peer/mini-ca/root/root_cert.pem"
					timeout: 5
					verify: true
					# Turn on CA OCSP check but allow when CA is unreachable
					ocsp_peer: {
						verify: true
						ca_timeout: 0.5
						allow_when_ca_unreachable: true
					}
				}
				# preserve revoked true
				ocsp_cache: {
					type: local
					preserve_revoked: true
				}
			`,
			[]nats.Option{
				nats.ClientCert("./configs/certs/ocsp_peer/mini-ca/client1/UserA1_bundle.pem", "./configs/certs/ocsp_peer/mini-ca/client1/private/UserA1_keypair.pem"),
				nats.RootCAs("./configs/certs/ocsp_peer/mini-ca/root/root_cert.pem"),
				nats.ErrorHandler(noOpErrHandler),
			},
			`
			{
				"5xL/SuHl6JN0OmxrNMpzVMTA73JVYcRfGX8+HvJinEI=": {
					"subject": "CN=UserA1,O=Testnats,L=Tacoma,ST=WA,C=US",
					"cached_at": "2023-06-05T16:33:52Z",
					"resp_status": "good",
					"resp_expires": "2023-06-05T16:33:55Z",
					"resp": "/wYAAFMyc1R3TwBYBQBgpzMn1wzUMIIGUwoBAKCCBkwwggZIBgkrBgEFBQcwAQEEggY5MIIGNTCB5aFZMFcxCzAJBgNVBAYTAlVTEQ0gCAwCV0ExDzANAQ0wBwwGVGFjb21hMREwDwEROAoMCFRlc3RuYXRzMRcwFQET8HYDDA5PQ1NQIFJlc3BvbmRlchgPMjAyMzA2MDUxNjMzMDBaMHcwdTBNMAkGBSsOAwIaBQAEFKgwn5fplwQy+DsulBg5SRpx0iaYBBS1kW5PZLcWhHb5tL6ZzmCVmBqOnQIUXKGv1Xy7Fu/Cx+ZT/JQa7SS7tBeAADZmABA1MVqgEToTAPRAATVaMA0GCSqGSIb3DQEBCwUAA4IBAQB9csJxA2VcpQYmC5lzI0dJJnEC1zcaP1oFbBm5VsZAbWh4mIGTqyEjMkucBqANTypnhWFRYcZE5nJE8tN8Aen0EBYkhk1wfEIhincaF3zs6x5RzigxQOqTPAanO55keKH5exYnfBcyCwAQiBbQaTXLJJdjerfqiRjqgsnLW8yl2IC8+kxTCfR4GHTK34mvqVWYYhP/xbaxTLJTp35t1vf1o78ct9R+z8W5sCSO4TsMNnExZlu4Ejeon/KivMR22j7nelTEaDCuaOl03WxKh9yhw2ix8V7lvR74wh5f4fjLZria6Y0+lUjwvvrZyBRwA62W/ihe3778q8KhLECFPQSaoIIENTCCBDEwggQtMIIDFaADAgECAhQp4VKN/aUqh+sd5B1HbOGKWGlzqzo/AQQwWlEk2jECCBowGEkxqBFJbnRlcm1lZGlhdGUgQ0EgMTAeFw0yMzA1MDExOTI4MzlaFw0zMzA0MjgND1GtFQBJDDCCASIuJwIAAQUA9EABDwAwggEKAoIBAQCjDMrrgOuhDh5xm9Oz+WXOcMIhBjwxwQZ+pahK4SGjdFSfV85Q1sMpPEOwnT5UlO6N+g1xbN9engEweWy7l12vu1sFd3KfVeZmRfTiws97DljWFGp2KazjMCgN7r3Kqq4fHu9A88OrF/LX7A3h+2iaCYOZEVhClPgN1JpvnzvoVvCptxgakUF8Q+PbsQHxrQs512WY5hWwF6lWbvuEesDMZ3X89nWEMXjFbVGP0BnTFk+H71szuXrd/l+oav1EVADzpKZb/TtlOE+CT7nEvcmaVvxU8Vgvy+70CP237K0oCGab+HiYMtuxVt0OMbrG41b1Ai/7dii7xIvza9qqHTghAgMBAAGjge0wgeowHQYDVR0OBBYEFMteUGB6qy+pOx4kqwJCjeyBYEgTMB8GA1UdIwQYMBaAUssDUDAMBgNVHRMBAf8EAjAAMA4GA1UdDwEOGAQDAgeAMBYBHgAlARAQDDAKBgiJtrADCTA9BgNVHR8ENjA0MDKgMKAuhixodHRwOi8vMTI3LjAuMC4xOjE4ODg4L2ldVigxX2NybC5kZXIwMxFLHAEBBCcwJTAjEVsMMAGGF1pKAC4SAgALBQD0AQEBAEhlzm2RRjA3tvJ2wELj9e7pMg5GtdWdrLDyI/U1qBxhZoHADbyku7W+R1iL8dFfc4PSmdo+owsygZakvahXjv49xJNX7wV3YMmIHC4lfurIlY2mSnPlu2zEOwEDkI0S9WkTxXmHrkXLSciQJDkwzye6MR5fW+APk4JmKDPc46Go/K1A0EgxY/ugahMYsYtZu++W+IOYbEoYNxoCrcJCHX4c3Ep3t/Wulz4X6DWWhaDkMMUDC2JVE8E/3xUbw0X3adZe9Xf8T+goOz7wLCAigXKj1hvRUmOGISIGelv0KsfluZesG1a1TGLp+W9JX0M9nOaFOvjJTDP96aqIjs8oXGk="
				}
			}`,
			nil,
			nil,
		},
		{
			"Expired Unknown response in cache for UserA1 -- should be allowed connection (cached item irrelevant)",
			`
				port: -1
				http_port: 8222
				tls: {
					cert_file: "configs/certs/ocsp_peer/mini-ca/server1/TestServer1_bundle.pem"
					key_file: "configs/certs/ocsp_peer/mini-ca/server1/private/TestServer1_keypair.pem"
					ca_file: "configs/certs/ocsp_peer/mini-ca/root/root_cert.pem"
					timeout: 5
					verify: true
					# Turn on CA OCSP check but allow when CA is unreachable
					ocsp_peer: {
						verify: true
						ca_timeout: 0.5
						allow_when_ca_unreachable: true
					}
				}
				# preserve revoked true
				ocsp_cache: {
					type: local
					preserve_revoked: true
				}
			`,
			[]nats.Option{
				nats.ClientCert("./configs/certs/ocsp_peer/mini-ca/client1/UserA1_bundle.pem", "./configs/certs/ocsp_peer/mini-ca/client1/private/UserA1_keypair.pem"),
				nats.RootCAs("./configs/certs/ocsp_peer/mini-ca/root/root_cert.pem"),
				nats.ErrorHandler(noOpErrHandler),
			},
			`
			{
				"5xL/SuHl6JN0OmxrNMpzVMTA73JVYcRfGX8+HvJinEI=": {
					"subject": "CN=UserA1,O=Testnats,L=Tacoma,ST=WA,C=US",
					"cached_at": "2023-06-05T16:45:01Z",
					"resp_status": "unknown",
					"resp_expires": "2023-06-05T16:45:05Z",
					"resp": "/wYAAFMyc1R3TwBSBQBH1aW01wzUMIIGUwoBAKCCBkwwggZIBgkrBgEFBQcwAQEEggY5MIIGNTCB5aFZMFcxCzAJBgNVBAYTAlVTEQ0gCAwCV0ExDzANAQ0wBwwGVGFjb21hMREwDwEROAoMCFRlc3RuYXRzMRcwFQET8HYDDA5PQ1NQIFJlc3BvbmRlchgPMjAyMzA2MDUxNjQ1MDBaMHcwdTBNMAkGBSsOAwIaBQAEFKgwn5fplwQy+DsulBg5SRpx0iaYBBS1kW5PZLcWhHb5tL6ZzmCVmBqOnQIUXKGv1Xy7Fu/Cx+ZT/JQa7SS7tBeCADpmAAwxWqAROhMA9EABNVowDQYJKoZIhvcNAQELBQADggEBAAzwED88ngiFj3hLzIejZ8DE/ppticX0vgAjUM8oXjDjwNXpSQ5xdXHsDk4RAYAVpNyiAfL2fapwz91g3JuZot6npp8smtZC5D0YMKK8iMjrx2aMqVyv+ai/33WG8PRWpBNzSTYaLhlBFjhUrx8HDu97ozNbmfgDWzRS1LqkJRa5YXvkyppqYTFSX73DV9R9tOVOwZ0x5WEKst9IJ+88mXsOBGyuye2Gh9RK6KsLgaOwiD9FBf18WRKIixeVM1Y/xHc/iwFPi8k3Z6hZ6gHX8NboQ/djCyzYVSWsUedTo/62uuagHPRWoYci4HQl4bSFXfcEO/EkWGnqkWBHfYZ4soigggQ1MIIEMTCCBC0wggMVoAMCAQICFCnhUo39pSqH6x3kHUds4YpYaXOrOj8BBDBaUSTaMQIIGjAYSTGoEUludGVybWVkaWF0ZSBDQSAxMB4XDTIzMDUwMTE5MjgzOVoXDTMzMDQyOA0PUa0VAEkMMIIBIi4nAgABBQD0QAEPADCCAQoCggEBAKMMyuuA66EOHnGb07P5Zc5wwiEGPDHBBn6lqErhIaN0VJ9XzlDWwyk8Q7CdPlSU7o36DXFs316eATB5bLuXXa+7WwV3cp9V5mZF9OLCz3sOWNYUanYprOMwKA3uvcqqrh8e70Dzw6sX8tfsDeH7aJoJg5kRWEKU+A3Umm+fO+hW8Km3GBqRQXxD49uxAfGtCznXZZjmFbAXqVZu+4R6wMxndfz2dYQxeMVtUY/QGdMWT4fvWzO5et3+X6hq/URUAPOkplv9O2U4T4JPucS9yZpW/FTxWC/L7vQI/bfsrSgIZpv4eJgy27FW3Q4xusbjVvUCL/t2KLvEi/Nr2qodOCECAwEAAaOB7TCB6jAdBgNVHQ4EFgQUy15QYHqrL6k7HiSrAkKN7IFgSBMwHwYDVR0jBBgwFoBSywNQMAwGA1UdEwEB/wQCMAAwDgYDVR0PAQ4YBAMCB4AwFgEeACUBEBAMMAoGCIm2sAMJMD0GA1UdHwQ2MDQwMqAwoC6GLGh0dHA6Ly8xMjcuMC4wLjE6MTg4ODgvaV1WKDFfY3JsLmRlcjAzEUscAQEEJzAlMCMRWwwwAYYXWkoALhICkTnw/0hlzm2RRjA3tvJ2wELj9e7pMg5GtdWdrLDyI/U1qBxhZoHADbyku7W+R1iL8dFfc4PSmdo+owsygZakvahXjv49xJNX7wV3YMmIHC4lfurIlY2mSnPlu2zEOwEDkI0S9WkTxXmHrkXLSciQJDkwzye6MR5fW+APk4JmKDPc46Go/K1A0EgxY/ugahMYsYtZu++W+IOYbEoYNxoCrcJCHX4c3Ep3t/Wulz4X6DWWhaDkMMUDC2JVE8E/3xUbw0X3adZe9Xf8T+goOz7wLCAigXKj1hvRUmOGISIGelv0KsfluZesG1a1TGLp+W9JX0M9nOaFOvjJTDP96aqIjs8oXGk="
				}
			}`,
			nil,
			nil,
		},
		{
			"No response in cache for UserA1 -- should be allowed connection",
			`
				port: -1
				http_port: 8222
				tls: {
					cert_file: "configs/certs/ocsp_peer/mini-ca/server1/TestServer1_bundle.pem"
					key_file: "configs/certs/ocsp_peer/mini-ca/server1/private/TestServer1_keypair.pem"
					ca_file: "configs/certs/ocsp_peer/mini-ca/root/root_cert.pem"
					timeout: 5
					verify: true
					# Turn on CA OCSP check but allow when CA is unreachable
					ocsp_peer: {
						verify: true
						ca_timeout: 0.5
						allow_when_ca_unreachable: true
					}
				}
				# preserve revoked true
				ocsp_cache: {
					type: local
					preserve_revoked: true
				}
			`,
			[]nats.Option{
				nats.ClientCert("./configs/certs/ocsp_peer/mini-ca/client1/UserA1_bundle.pem", "./configs/certs/ocsp_peer/mini-ca/client1/private/UserA1_keypair.pem"),
				nats.RootCAs("./configs/certs/ocsp_peer/mini-ca/root/root_cert.pem"),
				nats.ErrorHandler(noOpErrHandler),
			},
			"",
			nil,
			nil,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			deleteLocalStore(t, "")
			c := []byte(test.cachedResponse)
			err := writeCacheFile("", c)
			if err != nil {
				t.Fatal(err)
			}
			content := test.config
			conf := createConfFile(t, []byte(content))
			s, opts := RunServerWithConfig(conf)
			defer s.Shutdown()
			nc, err := nats.Connect(fmt.Sprintf("tls://localhost:%d", opts.Port), test.opts...)
			if test.err == nil && err != nil {
				t.Errorf("Expected to connect, got %v", err)
			} else if test.err != nil && err == nil {
				t.Errorf("Expected error on connect")
			} else if test.err != nil && err != nil {
				// Error on connect was expected
				if test.err.Error() != err.Error() {
					t.Errorf("Expected error %s, got: %s", test.err, err)
				}
				return
			}
			defer nc.Close()
		})
	}
}

// TestOCSPResponseCacheLocalStoreOpt is test of default and non-default local_store option
func TestOCSPResponseCacheLocalStoreOpt(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	rootCAResponder := NewOCSPResponderRootCA(t)
	rootCAResponderURL := fmt.Sprintf("http://%s", rootCAResponder.Addr)
	defer rootCAResponder.Shutdown(ctx)
	SetOCSPStatus(t, rootCAResponderURL, "configs/certs/ocsp_peer/mini-ca/intermediate1/intermediate1_cert.pem", ocsp.Good)

	for _, test := range []struct {
		name           string
		config         string
		opts           []nats.Option
		cachedResponse string
		err            error
		rerr           error
		storeLocation  string
	}{
		{
			"Test load from non-default local store _custom_; connect will reject only if cache file found and loaded",
			`
				port: -1
				http_port: 8222
				tls: {
					cert_file: "configs/certs/ocsp_peer/mini-ca/server1/TestServer1_bundle.pem"
					key_file: "configs/certs/ocsp_peer/mini-ca/server1/private/TestServer1_keypair.pem"
					ca_file: "configs/certs/ocsp_peer/mini-ca/root/root_cert.pem"
					timeout: 5
					verify: true
					# Turn on CA OCSP check but allow when CA is unreachable
					ocsp_peer: {
						verify: true
						ca_timeout: 0.5
						allow_when_ca_unreachable: true
					}
				}
				# preserve revoked true
				ocsp_cache: {
					type: local
					local_store: "_custom_"
					preserve_revoked: true
				}
			`,
			[]nats.Option{
				nats.ClientCert("./configs/certs/ocsp_peer/mini-ca/client1/UserA1_bundle.pem", "./configs/certs/ocsp_peer/mini-ca/client1/private/UserA1_keypair.pem"),
				nats.RootCAs("./configs/certs/ocsp_peer/mini-ca/root/root_cert.pem"),
				nats.ErrorHandler(noOpErrHandler),
			},
			`
				{
					"5xL/SuHl6JN0OmxrNMpzVMTA73JVYcRfGX8+HvJinEI=": {
						"subject": "CN=UserA1,O=Testnats,L=Tacoma,ST=WA,C=US",
						"cached_at": "2023-05-29T17:56:45Z",
						"resp_status": "revoked",
						"resp_expires": "2023-05-29T17:56:49Z",
						"resp": "/wYAAFMyc1R3TwBSBQBao1Qr1QzUMIIGUQoBAKCCBkowggZGBgkrBgEFBQcwAQEEggY3MIIGMzCB46FZMFcxCzAJBgNVBAYTAlVTEQ0gCAwCV0ExDzANAQ0wBwwGVGFjb21hMREwDwEROAoMCFRlc3RuYXRzMRcwFQET8HQDDA5PQ1NQIFJlc3BvbmRlchgPMjAyMzA1MjkxNzU2MDBaMHUwczBNMAkGBSsOAwIaBQAEFKgwn5fplwQy+DsulBg5SRpx0iaYBBS1kW5PZLcWhHb5tL6ZzmCVmBqOnQIUXKGv1Xy7Fu/Cx+ZT/JQa7SS7tBc2ZAAQNDVaoBE2dwD0QQE0OVowDQYJKoZIhvcNAQELBQADggEBAGAax/vkv3SBFNbxp2utc/N6Rje4E0ceC972sWgqYjzYrH0oc/acg+OAXaxUjwqoQWaT+dHaI4D5qoTkMx7XlWATjI2L72IUTf6Luo92jPzyDFwb10CdeFHtRtEYD54Qbi/nD4oxQ8cSoLKC3wft2l3E/mK/1I4Mxwq15CioK4MhfzTISoeGZbjDXPKgloJOG3rn9v64vFGV6dosbLgaXEs+MPcCsPQYkwhOOyazuewRmIDOBp5QSsKPhqsT8Rs20t8LGTMkvjZniFWJs90l9QL9F1m3obq5nyuxrGt+7Rf5zoj4T+0XCOGtE+b7cRCLg43tFuTbaAQG8Z+qkPzpza+gggQ1MIIEMTCCBC0wggMVoAMCAQICFCnhUo39pSqH6x3kHUds4YpYaXOrOj8BBDBaUSLaLwIIGjAYSS+oEUludGVybWVkaWF0ZSBDQSAxMB4XDTIzMDUwMTE5MjgzOVoXDTMzMDQyOA0PUasVAEkMMIIBIi4nAgABBQD0QAEPADCCAQoCggEBAKMMyuuA66EOHnGb07P5Zc5wwiEGPDHBBn6lqErhIaN0VJ9XzlDWwyk8Q7CdPlSU7o36DXFs316eATB5bLuXXa+7WwV3cp9V5mZF9OLCz3sOWNYUanYprOMwKA3uvcqqrh8e70Dzw6sX8tfsDeH7aJoJg5kRWEKU+A3Umm+fO+hW8Km3GBqRQXxD49uxAfGtCznXZZjmFbAXqVZu+4R6wMxndfz2dYQxeMVtUY/QGdMWT4fvWzO5et3+X6hq/URUAPOkplv9O2U4T4JPucS9yZpW/FTxWC/L7vQI/bfsrSgIZpv4eJgy27FW3Q4xusbjVvUCL/t2KLvEi/Nr2qodOCECAwEAAaOB7TCB6jAdBgNVHQ4EFgQUy15QYHqrL6k7HiSrAkKN7IFgSBMwHwYDVR0jBBgwFoBSyQNQMAwGA1UdEwEB/wQCMAAwDgYDVR0PAQ4YBAMCB4AwFgEeACUBEBAMMAoGCIm0sAMJMD0GA1UdHwQ2MDQwMqAwoC6GLGh0dHA6Ly8xMjcuMC4wLjE6MTg4ODgvaV1WKDFfY3JsLmRlcjAzEUscAQEEJzAlMCMRWwwwAYYXWkoALhICkTnw/0hlzm2RRjA3tvJ2wELj9e7pMg5GtdWdrLDyI/U1qBxhZoHADbyku7W+R1iL8dFfc4PSmdo+owsygZakvahXjv49xJNX7wV3YMmIHC4lfurIlY2mSnPlu2zEOwEDkI0S9WkTxXmHrkXLSciQJDkwzye6MR5fW+APk4JmKDPc46Go/K1A0EgxY/ugahMYsYtZu++W+IOYbEoYNxoCrcJCHX4c3Ep3t/Wulz4X6DWWhaDkMMUDC2JVE8E/3xUbw0X3adZe9Xf8T+goOz7wLCAigXKj1hvRUmOGISIGelv0KsfluZesG1a1TGLp+W9JX0M9nOaFOvjJTDP96aqIjs8oXGk="
					}
				}`,
			errors.New("remote error: tls: bad certificate"),
			errors.New("expect error"),
			"_custom_",
		},
		{
			"Test load from default local store when \"\" set; connect will reject only if cache file found and loaded",
			`
				port: -1
				http_port: 8222
				tls: {
					cert_file: "configs/certs/ocsp_peer/mini-ca/server1/TestServer1_bundle.pem"
					key_file: "configs/certs/ocsp_peer/mini-ca/server1/private/TestServer1_keypair.pem"
					ca_file: "configs/certs/ocsp_peer/mini-ca/root/root_cert.pem"
					timeout: 5
					verify: true
					# Turn on CA OCSP check but allow when CA is unreachable
					ocsp_peer: {
						verify: true
						ca_timeout: 0.5
						allow_when_ca_unreachable: true
					}
				}
				# preserve revoked true
				ocsp_cache: {
					type: local
					local_store: ""
					preserve_revoked: true
				}
			`,
			[]nats.Option{
				nats.ClientCert("./configs/certs/ocsp_peer/mini-ca/client1/UserA1_bundle.pem", "./configs/certs/ocsp_peer/mini-ca/client1/private/UserA1_keypair.pem"),
				nats.RootCAs("./configs/certs/ocsp_peer/mini-ca/root/root_cert.pem"),
				nats.ErrorHandler(noOpErrHandler),
			},
			`
				{
					"5xL/SuHl6JN0OmxrNMpzVMTA73JVYcRfGX8+HvJinEI=": {
						"subject": "CN=UserA1,O=Testnats,L=Tacoma,ST=WA,C=US",
						"cached_at": "2023-05-29T17:56:45Z",
						"resp_status": "revoked",
						"resp_expires": "2023-05-29T17:56:49Z",
						"resp": "/wYAAFMyc1R3TwBSBQBao1Qr1QzUMIIGUQoBAKCCBkowggZGBgkrBgEFBQcwAQEEggY3MIIGMzCB46FZMFcxCzAJBgNVBAYTAlVTEQ0gCAwCV0ExDzANAQ0wBwwGVGFjb21hMREwDwEROAoMCFRlc3RuYXRzMRcwFQET8HQDDA5PQ1NQIFJlc3BvbmRlchgPMjAyMzA1MjkxNzU2MDBaMHUwczBNMAkGBSsOAwIaBQAEFKgwn5fplwQy+DsulBg5SRpx0iaYBBS1kW5PZLcWhHb5tL6ZzmCVmBqOnQIUXKGv1Xy7Fu/Cx+ZT/JQa7SS7tBc2ZAAQNDVaoBE2dwD0QQE0OVowDQYJKoZIhvcNAQELBQADggEBAGAax/vkv3SBFNbxp2utc/N6Rje4E0ceC972sWgqYjzYrH0oc/acg+OAXaxUjwqoQWaT+dHaI4D5qoTkMx7XlWATjI2L72IUTf6Luo92jPzyDFwb10CdeFHtRtEYD54Qbi/nD4oxQ8cSoLKC3wft2l3E/mK/1I4Mxwq15CioK4MhfzTISoeGZbjDXPKgloJOG3rn9v64vFGV6dosbLgaXEs+MPcCsPQYkwhOOyazuewRmIDOBp5QSsKPhqsT8Rs20t8LGTMkvjZniFWJs90l9QL9F1m3obq5nyuxrGt+7Rf5zoj4T+0XCOGtE+b7cRCLg43tFuTbaAQG8Z+qkPzpza+gggQ1MIIEMTCCBC0wggMVoAMCAQICFCnhUo39pSqH6x3kHUds4YpYaXOrOj8BBDBaUSLaLwIIGjAYSS+oEUludGVybWVkaWF0ZSBDQSAxMB4XDTIzMDUwMTE5MjgzOVoXDTMzMDQyOA0PUasVAEkMMIIBIi4nAgABBQD0QAEPADCCAQoCggEBAKMMyuuA66EOHnGb07P5Zc5wwiEGPDHBBn6lqErhIaN0VJ9XzlDWwyk8Q7CdPlSU7o36DXFs316eATB5bLuXXa+7WwV3cp9V5mZF9OLCz3sOWNYUanYprOMwKA3uvcqqrh8e70Dzw6sX8tfsDeH7aJoJg5kRWEKU+A3Umm+fO+hW8Km3GBqRQXxD49uxAfGtCznXZZjmFbAXqVZu+4R6wMxndfz2dYQxeMVtUY/QGdMWT4fvWzO5et3+X6hq/URUAPOkplv9O2U4T4JPucS9yZpW/FTxWC/L7vQI/bfsrSgIZpv4eJgy27FW3Q4xusbjVvUCL/t2KLvEi/Nr2qodOCECAwEAAaOB7TCB6jAdBgNVHQ4EFgQUy15QYHqrL6k7HiSrAkKN7IFgSBMwHwYDVR0jBBgwFoBSyQNQMAwGA1UdEwEB/wQCMAAwDgYDVR0PAQ4YBAMCB4AwFgEeACUBEBAMMAoGCIm0sAMJMD0GA1UdHwQ2MDQwMqAwoC6GLGh0dHA6Ly8xMjcuMC4wLjE6MTg4ODgvaV1WKDFfY3JsLmRlcjAzEUscAQEEJzAlMCMRWwwwAYYXWkoALhICkTnw/0hlzm2RRjA3tvJ2wELj9e7pMg5GtdWdrLDyI/U1qBxhZoHADbyku7W+R1iL8dFfc4PSmdo+owsygZakvahXjv49xJNX7wV3YMmIHC4lfurIlY2mSnPlu2zEOwEDkI0S9WkTxXmHrkXLSciQJDkwzye6MR5fW+APk4JmKDPc46Go/K1A0EgxY/ugahMYsYtZu++W+IOYbEoYNxoCrcJCHX4c3Ep3t/Wulz4X6DWWhaDkMMUDC2JVE8E/3xUbw0X3adZe9Xf8T+goOz7wLCAigXKj1hvRUmOGISIGelv0KsfluZesG1a1TGLp+W9JX0M9nOaFOvjJTDP96aqIjs8oXGk="
					}
				}`,
			errors.New("remote error: tls: bad certificate"),
			errors.New("expect error"),
			"_rc_",
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			deleteLocalStore(t, test.storeLocation)
			c := []byte(test.cachedResponse)
			err := writeCacheFile(test.storeLocation, c)
			if err != nil {
				t.Fatal(err)
			}
			content := test.config
			conf := createConfFile(t, []byte(content))
			s, opts := RunServerWithConfig(conf)
			defer s.Shutdown()
			nc, err := nats.Connect(fmt.Sprintf("tls://localhost:%d", opts.Port), test.opts...)
			if test.err == nil && err != nil {
				t.Errorf("Expected to connect, got %v", err)
			} else if test.err != nil && err == nil {
				t.Errorf("Expected error on connect")
			} else if test.err != nil && err != nil {
				// Error on connect was expected
				if test.err.Error() != err.Error() {
					t.Errorf("Expected error %s, got: %s", test.err, err)
				}
				return
			}
			defer nc.Close()
		})
	}
}

// TestOCSPPeerIncrementalSaveLocalCache is test of timer-based response cache save as new entries added
func TestOCSPPeerIncrementalSaveLocalCache(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	rootCAResponder := NewOCSPResponderRootCA(t)
	rootCAResponderURL := fmt.Sprintf("http://%s", rootCAResponder.Addr)
	defer rootCAResponder.Shutdown(ctx)
	SetOCSPStatus(t, rootCAResponderURL, "configs/certs/ocsp_peer/mini-ca/intermediate1/intermediate1_cert.pem", ocsp.Good)
	SetOCSPStatus(t, rootCAResponderURL, "configs/certs/ocsp_peer/mini-ca/intermediate2/intermediate2_cert.pem", ocsp.Good)

	intermediateCA1Responder := NewOCSPResponderIntermediateCA1(t)
	intermediateCA1ResponderURL := fmt.Sprintf("http://%s", intermediateCA1Responder.Addr)
	defer intermediateCA1Responder.Shutdown(ctx)
	SetOCSPStatus(t, intermediateCA1ResponderURL, "configs/certs/ocsp_peer/mini-ca/client1/UserA1_cert.pem", ocsp.Good)

	intermediateCA2Responder := NewOCSPResponderIntermediateCA2(t)
	intermediateCA2ResponderURL := fmt.Sprintf("http://%s", intermediateCA2Responder.Addr)
	defer intermediateCA2Responder.Shutdown(ctx)
	SetOCSPStatus(t, intermediateCA2ResponderURL, "configs/certs/ocsp_peer/mini-ca/client2/UserB1_cert.pem", ocsp.Good)

	var fi os.FileInfo
	var err error

	for _, test := range []struct {
		name      string
		config    string
		opts      [][]nats.Option
		err       error
		rerr      error
		configure func()
	}{
		{
			"Default cache, short form: mTLS OCSP peer check on inbound client connection, UserA1 client of intermediate CA 1",
			`
				port: -1
				http_port: 8222
				tls: {
					cert_file: "configs/certs/ocsp_peer/mini-ca/server1/TestServer1_bundle.pem"
					key_file: "configs/certs/ocsp_peer/mini-ca/server1/private/TestServer1_keypair.pem"
					ca_file: "configs/certs/ocsp_peer/mini-ca/root/root_cert.pem"
					timeout: 5
					verify: true
					# Long form configuration
					ocsp_peer: {
						verify: true
						ca_timeout: 5
						allowed_clockskew: 30
					}
				}
				# Local cache with custom save_interval for testability
				ocsp_cache: {
					type: local
					# Save if dirty ever 1 second
					save_interval: 1
				}
			`,
			[][]nats.Option{
				{
					nats.ClientCert("./configs/certs/ocsp_peer/mini-ca/client1/UserA1_bundle.pem", "./configs/certs/ocsp_peer/mini-ca/client1/private/UserA1_keypair.pem"),
					nats.RootCAs("./configs/certs/ocsp_peer/mini-ca/root/root_cert.pem"),
					nats.ErrorHandler(noOpErrHandler),
				},
				{
					nats.ClientCert("./configs/certs/ocsp_peer/mini-ca/client2/UserB1_bundle.pem", "./configs/certs/ocsp_peer/mini-ca/client2/private/UserB1_keypair.pem"),
					nats.RootCAs("./configs/certs/ocsp_peer/mini-ca/root/root_cert.pem"),
					nats.ErrorHandler(noOpErrHandler),
				},
			},
			nil,
			nil,
			func() {},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			// Cleanup any previous test that saved a local cache
			deleteLocalStore(t, "")
			fi, err = statCacheFile("")
			if err != nil && fi != nil && fi.Size() != 0 {
				t.Fatalf("Expected no local cache file, got a FileInfo with size %d", fi.Size())
			}
			test.configure()
			content := test.config
			conf := createConfFile(t, []byte(content))
			s, opts := RunServerWithConfig(conf)
			defer s.Shutdown()

			// Connect with UserA1 client and get a CA Response
			nc, err := nats.Connect(fmt.Sprintf("tls://localhost:%d", opts.Port), test.opts[0]...)
			if test.err == nil && err != nil {
				t.Errorf("Expected to connect, got %v", err)
			} else if test.err != nil && err == nil {
				t.Errorf("Expected error on connect")
			} else if test.err != nil && err != nil {
				// Error on connect was expected
				if test.err.Error() != err.Error() {
					t.Errorf("Expected error %s, got: %s", test.err, err)
				}
				return
			}
			nc.Close()
			time.Sleep(2 * time.Second)
			fi, err = statCacheFile("")
			if err == nil && fi != nil && fi.Size() > 0 {
				// good
			} else {
				if err != nil {
					t.Fatalf("Expected an extant local cache file, got error: %v", err)
				}
				if fi != nil {
					t.Fatalf("Expected non-zero size local cache file, got a FileInfo with size %d", fi.Size())
				}
			}
			firstFi := fi
			// Connect with UserB1 client and get another CA Response
			nc, err = nats.Connect(fmt.Sprintf("tls://localhost:%d", opts.Port), test.opts[1]...)
			if test.err == nil && err != nil {
				t.Errorf("Expected to connect, got %v", err)
			} else if test.err != nil && err == nil {
				t.Errorf("Expected error on connect")
			} else if test.err != nil && err != nil {
				// Error on connect was expected
				if test.err.Error() != err.Error() {
					t.Errorf("Expected error %s, got: %s", test.err, err)
				}
				return
			}
			nc.Close()
			time.Sleep(2 * time.Second)
			fi, err = statCacheFile("")
			if err == nil && fi != nil && fi.Size() > firstFi.Size() {
				// good
			} else {
				if err != nil {
					t.Fatalf("Expected an extant local cache file, got error: %v", err)
				}
				if fi != nil {
					t.Fatalf("Expected non-zero size local cache file with more bytes, got a FileInfo with size %d", fi.Size())
				}
			}
		})
	}
}

func statCacheFile(dir string) (os.FileInfo, error) {
	if dir == "" {
		dir = "_rc_"
	}
	return os.Stat(filepath.Join(dir, "cache.json"))
}

// TestOCSPPeerUndelegatedCAResponseSigner
func TestOCSPPeerUndelegatedCAResponseSigner(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	rootCAResponder := NewOCSPResponderRootCA(t)
	rootCAResponderURL := fmt.Sprintf("http://%s", rootCAResponder.Addr)
	defer rootCAResponder.Shutdown(ctx)
	SetOCSPStatus(t, rootCAResponderURL, "configs/certs/ocsp_peer/mini-ca/intermediate1/intermediate1_cert.pem", ocsp.Good)

	intermediateCA1Responder := NewOCSPResponderIntermediateCA1Undelegated(t)
	intermediateCA1ResponderURL := fmt.Sprintf("http://%s", intermediateCA1Responder.Addr)
	defer intermediateCA1Responder.Shutdown(ctx)
	SetOCSPStatus(t, intermediateCA1ResponderURL, "configs/certs/ocsp_peer/mini-ca/client1/UserA1_cert.pem", ocsp.Good)

	for _, test := range []struct {
		name      string
		config    string
		opts      []nats.Option
		err       error
		rerr      error
		configure func()
	}{
		{
			"mTLS OCSP peer check on inbound client connection, responder is CA (undelegated)",
			`
				port: -1
				# Cache configuration is default
				tls: {
					cert_file: "configs/certs/ocsp_peer/mini-ca/server1/TestServer1_bundle.pem"
					key_file: "configs/certs/ocsp_peer/mini-ca/server1/private/TestServer1_keypair.pem"
					ca_file: "configs/certs/ocsp_peer/mini-ca/root/root_cert.pem"
					timeout: 5
					verify: true
					# Turn on CA OCSP check so unvalidated clients can't connect
					ocsp_peer: true
				}
			`,
			[]nats.Option{
				nats.ClientCert("./configs/certs/ocsp_peer/mini-ca/client1/UserA1_bundle.pem", "./configs/certs/ocsp_peer/mini-ca/client1/private/UserA1_keypair.pem"),
				nats.RootCAs("./configs/certs/ocsp_peer/mini-ca/root/root_cert.pem"),
				nats.ErrorHandler(noOpErrHandler),
			},
			nil,
			nil,
			func() {},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			deleteLocalStore(t, "")
			test.configure()
			content := test.config
			conf := createConfFile(t, []byte(content))
			s, opts := RunServerWithConfig(conf)
			defer s.Shutdown()
			nc, err := nats.Connect(fmt.Sprintf("tls://localhost:%d", opts.Port), test.opts...)
			if test.err == nil && err != nil {
				t.Errorf("Expected to connect, got %v", err)
			} else if test.err != nil && err == nil {
				t.Errorf("Expected error on connect")
			} else if test.err != nil && err != nil {
				// Error on connect was expected
				if test.err.Error() != err.Error() {
					t.Errorf("Expected error %s, got: %s", test.err, err)
				}
				return
			}
			defer nc.Close()
		})
	}
}

// TestOCSPPeerDelegatedCAResponseSigner
func TestOCSPPeerDelegatedCAResponseSigner(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	rootCAResponder := NewOCSPResponderRootCA(t)
	rootCAResponderURL := fmt.Sprintf("http://%s", rootCAResponder.Addr)
	defer rootCAResponder.Shutdown(ctx)
	SetOCSPStatus(t, rootCAResponderURL, "configs/certs/ocsp_peer/mini-ca/intermediate1/intermediate1_cert.pem", ocsp.Good)

	intermediateCA1Responder := NewOCSPResponderIntermediateCA1(t)
	intermediateCA1ResponderURL := fmt.Sprintf("http://%s", intermediateCA1Responder.Addr)
	defer intermediateCA1Responder.Shutdown(ctx)
	SetOCSPStatus(t, intermediateCA1ResponderURL, "configs/certs/ocsp_peer/mini-ca/client1/UserA1_cert.pem", ocsp.Good)

	for _, test := range []struct {
		name      string
		config    string
		opts      []nats.Option
		err       error
		rerr      error
		configure func()
	}{
		{
			"mTLS OCSP peer check on inbound client connection, responder is CA (undelegated)",
			`
				port: -1
				# Cache configuration is default
				tls: {
					cert_file: "configs/certs/ocsp_peer/mini-ca/server1/TestServer1_bundle.pem"
					key_file: "configs/certs/ocsp_peer/mini-ca/server1/private/TestServer1_keypair.pem"
					ca_file: "configs/certs/ocsp_peer/mini-ca/root/root_cert.pem"
					timeout: 5
					verify: true
					# Turn on CA OCSP check so unvalidated clients can't connect
					ocsp_peer: true
				}
			`,
			[]nats.Option{
				nats.ClientCert("./configs/certs/ocsp_peer/mini-ca/client1/UserA1_bundle.pem", "./configs/certs/ocsp_peer/mini-ca/client1/private/UserA1_keypair.pem"),
				nats.RootCAs("./configs/certs/ocsp_peer/mini-ca/root/root_cert.pem"),
				nats.ErrorHandler(noOpErrHandler),
			},
			nil,
			nil,
			func() {},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			deleteLocalStore(t, "")
			test.configure()
			content := test.config
			conf := createConfFile(t, []byte(content))
			s, opts := RunServerWithConfig(conf)
			defer s.Shutdown()
			nc, err := nats.Connect(fmt.Sprintf("tls://localhost:%d", opts.Port), test.opts...)
			if test.err == nil && err != nil {
				t.Errorf("Expected to connect, got %v", err)
			} else if test.err != nil && err == nil {
				t.Errorf("Expected error on connect")
			} else if test.err != nil && err != nil {
				// Error on connect was expected
				if test.err.Error() != err.Error() {
					t.Errorf("Expected error %s, got: %s", test.err, err)
				}
				return
			}
			defer nc.Close()
		})
	}
}

// TestOCSPPeerBadDelegatedCAResponseSigner
func TestOCSPPeerBadDelegatedCAResponseSigner(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	rootCAResponder := NewOCSPResponderRootCA(t)
	rootCAResponderURL := fmt.Sprintf("http://%s", rootCAResponder.Addr)
	defer rootCAResponder.Shutdown(ctx)
	SetOCSPStatus(t, rootCAResponderURL, "configs/certs/ocsp_peer/mini-ca/intermediate1/intermediate1_cert.pem", ocsp.Good)

	intermediateCA1Responder := NewOCSPResponderBadDelegateIntermediateCA1(t)
	intermediateCA1ResponderURL := fmt.Sprintf("http://%s", intermediateCA1Responder.Addr)
	defer intermediateCA1Responder.Shutdown(ctx)
	SetOCSPStatus(t, intermediateCA1ResponderURL, "configs/certs/ocsp_peer/mini-ca/client1/UserA1_cert.pem", ocsp.Good)

	for _, test := range []struct {
		name      string
		config    string
		opts      []nats.Option
		err       error
		rerr      error
		configure func()
	}{
		{
			"mTLS OCSP peer check on inbound client connection, responder is not a legal delegate",
			`
				port: -1
				# Cache configuration is default
				tls: {
					cert_file: "configs/certs/ocsp_peer/mini-ca/server1/TestServer1_bundle.pem"
					key_file: "configs/certs/ocsp_peer/mini-ca/server1/private/TestServer1_keypair.pem"
					ca_file: "configs/certs/ocsp_peer/mini-ca/root/root_cert.pem"
					timeout: 5
					verify: true
					# Turn on CA OCSP check so unvalidated clients can't connect
					ocsp_peer: true
				}
			`,
			[]nats.Option{
				nats.ClientCert("./configs/certs/ocsp_peer/mini-ca/client1/UserA1_bundle.pem", "./configs/certs/ocsp_peer/mini-ca/client1/private/UserA1_keypair.pem"),
				nats.RootCAs("./configs/certs/ocsp_peer/mini-ca/root/root_cert.pem"),
				nats.ErrorHandler(noOpErrHandler),
			},
			errors.New("remote error: tls: bad certificate"),
			errors.New("expect error"),
			func() {},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			deleteLocalStore(t, "")
			test.configure()
			content := test.config
			conf := createConfFile(t, []byte(content))
			s, opts := RunServerWithConfig(conf)
			defer s.Shutdown()
			nc, err := nats.Connect(fmt.Sprintf("tls://localhost:%d", opts.Port), test.opts...)
			if test.err == nil && err != nil {
				t.Errorf("Expected to connect, got %v", err)
			} else if test.err != nil && err == nil {
				t.Errorf("Expected error on connect")
			} else if test.err != nil && err != nil {
				// Error on connect was expected
				if test.err.Error() != err.Error() {
					t.Errorf("Expected error %s, got: %s", test.err, err)
				}
				return
			}
			defer nc.Close()
		})
	}
}

// TestOCSPPeerNextUpdateUnset is test of scenario when responder does not set NextUpdate and cache TTL option is used
func TestOCSPPeerNextUpdateUnset(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	rootCAResponder := NewOCSPResponderRootCA(t)
	rootCAResponderURL := fmt.Sprintf("http://%s", rootCAResponder.Addr)
	defer rootCAResponder.Shutdown(ctx)
	SetOCSPStatus(t, rootCAResponderURL, "configs/certs/ocsp_peer/mini-ca/intermediate1/intermediate1_cert.pem", ocsp.Good)

	respCertPEM := "configs/certs/ocsp_peer/mini-ca/ocsp1/ocsp1_bundle.pem"
	respKeyPEM := "configs/certs/ocsp_peer/mini-ca/ocsp1/private/ocsp1_keypair.pem"
	issuerCertPEM := "configs/certs/ocsp_peer/mini-ca/intermediate1/intermediate1_cert.pem"
	intermediateCA1Responder := NewOCSPResponderBase(t, issuerCertPEM, respCertPEM, respKeyPEM, true, "127.0.0.1:18888", 0, "")
	intermediateCA1ResponderURL := fmt.Sprintf("http://%s", intermediateCA1Responder.Addr)
	defer intermediateCA1Responder.Shutdown(ctx)
	SetOCSPStatus(t, intermediateCA1ResponderURL, "configs/certs/ocsp_peer/mini-ca/client1/UserA1_cert.pem", ocsp.Good)

	for _, test := range []struct {
		name           string
		config         string
		opts           []nats.Option
		err            error
		rerr           error
		expectedMisses int64
		configure      func()
	}{
		{
			"TTL set to 4 seconds with second client connection leveraging cache from first client connect",
			`
				port: -1
				http_port: 8222
				tls: {
					cert_file: "configs/certs/ocsp_peer/mini-ca/server1/TestServer1_bundle.pem"
					key_file: "configs/certs/ocsp_peer/mini-ca/server1/private/TestServer1_keypair.pem"
					ca_file: "configs/certs/ocsp_peer/mini-ca/root/root_cert.pem"
					timeout: 5
					verify: true
					# Long form configuration
					ocsp_peer: {
						verify: true
						ca_timeout: 5
						allowed_clockskew: 0
						cache_ttl_when_next_update_unset: 4
					}
				}
				# Short form configuration, local as default
				ocsp_cache: true
			`,
			[]nats.Option{
				nats.ClientCert("./configs/certs/ocsp_peer/mini-ca/client1/UserA1_bundle.pem", "./configs/certs/ocsp_peer/mini-ca/client1/private/UserA1_keypair.pem"),
				nats.RootCAs("./configs/certs/ocsp_peer/mini-ca/root/root_cert.pem"),
				nats.ErrorHandler(noOpErrHandler),
			},
			nil,
			nil,
			2,
			func() {},
		},
		{
			"TTL set to 1 seconds with second client connection not leveraging cache items from first client connect",
			`
				port: -1
				http_port: 8222
				tls: {
					cert_file: "configs/certs/ocsp_peer/mini-ca/server1/TestServer1_bundle.pem"
					key_file: "configs/certs/ocsp_peer/mini-ca/server1/private/TestServer1_keypair.pem"
					ca_file: "configs/certs/ocsp_peer/mini-ca/root/root_cert.pem"
					timeout: 5
					verify: true
					# Long form configuration
					ocsp_peer: {
						verify: true
						ca_timeout: 5
						allowed_clockskew: 0
						cache_ttl_when_next_update_unset: 1
					}
				}
				# Short form configuration, local as default
				ocsp_cache: true
			`,
			[]nats.Option{
				nats.ClientCert("./configs/certs/ocsp_peer/mini-ca/client1/UserA1_bundle.pem", "./configs/certs/ocsp_peer/mini-ca/client1/private/UserA1_keypair.pem"),
				nats.RootCAs("./configs/certs/ocsp_peer/mini-ca/root/root_cert.pem"),
				nats.ErrorHandler(noOpErrHandler),
			},
			nil,
			nil,
			3,
			func() {},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			// Cleanup any previous test that saved a local cache
			deleteLocalStore(t, "")
			test.configure()
			content := test.config
			conf := createConfFile(t, []byte(content))
			s, opts := RunServerWithConfig(conf)
			defer s.Shutdown()
			nc, err := nats.Connect(fmt.Sprintf("tls://localhost:%d", opts.Port), test.opts...)
			if test.err == nil && err != nil {
				t.Errorf("Expected to connect, got %v", err)
			} else if test.err != nil && err == nil {
				t.Errorf("Expected error on connect")
			} else if test.err != nil && err != nil {
				// Error on connect was expected
				if test.err.Error() != err.Error() {
					t.Errorf("Expected error %s, got: %s", test.err, err)
				}
				return
			}
			nc.Close()

			// Wait interval shorter than first test, and longer than second test
			time.Sleep(2 * time.Second)

			nc, err = nats.Connect(fmt.Sprintf("tls://localhost:%d", opts.Port), test.opts...)
			if test.err == nil && err != nil {
				t.Errorf("Expected to connect, got %v", err)
			} else if test.err != nil && err == nil {
				t.Errorf("Expected error on connect")
			} else if test.err != nil && err != nil {
				// Error on connect was expected
				if test.err.Error() != err.Error() {
					t.Errorf("Expected error %s, got: %s", test.err, err)
				}
				return
			}
			defer nc.Close()

			v := monitorGetVarzHelper(t, 8222)
			if v.OCSPResponseCache == nil {
				t.Fatalf("Expected OCSP statistics to be in varz")
			}
			if v.OCSPResponseCache.Misses != test.expectedMisses || v.OCSPResponseCache.Responses != 2 {
				t.Errorf("Expected cache misses to be %d and cache items to be 2, got %d and %d", test.expectedMisses, v.OCSPResponseCache.Misses, v.OCSPResponseCache.Responses)
			}
		})
	}
}

func TestOCSPMonitoringPort(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	rootCAResponder := NewOCSPResponderRootCA(t)
	rootCAResponderURL := fmt.Sprintf("http://%s", rootCAResponder.Addr)
	defer rootCAResponder.Shutdown(ctx)
	SetOCSPStatus(t, rootCAResponderURL, "configs/certs/ocsp_peer/mini-ca/intermediate1/intermediate1_cert.pem", ocsp.Good)

	respCertPEM := "configs/certs/ocsp_peer/mini-ca/ocsp1/ocsp1_bundle.pem"
	respKeyPEM := "configs/certs/ocsp_peer/mini-ca/ocsp1/private/ocsp1_keypair.pem"
	issuerCertPEM := "configs/certs/ocsp_peer/mini-ca/intermediate1/intermediate1_cert.pem"
	intermediateCA1Responder := NewOCSPResponderBase(t, issuerCertPEM, respCertPEM, respKeyPEM, true, "127.0.0.1:18888", 0, "")
	intermediateCA1ResponderURL := fmt.Sprintf("http://%s", intermediateCA1Responder.Addr)
	defer intermediateCA1Responder.Shutdown(ctx)
	SetOCSPStatus(t, intermediateCA1ResponderURL, "configs/certs/ocsp_peer/mini-ca/client1/UserA1_cert.pem", ocsp.Good)
	SetOCSPStatus(t, intermediateCA1ResponderURL, "configs/certs/ocsp_peer/mini-ca/server1/TestServer1_bundle.pem", ocsp.Good)

	for _, test := range []struct {
		name   string
		config string
		opts   []nats.Option
		err    error
		rerr   error
	}{
		{
			"https with ocsp_peer",
			`
				net: 127.0.0.1
				port: -1
				https: -1
				# Short form configuration
				ocsp_cache: true
                                store_dir = %s
				tls: {
					cert_file: "configs/certs/ocsp_peer/mini-ca/server1/TestServer1_bundle.pem"
					key_file: "configs/certs/ocsp_peer/mini-ca/server1/private/TestServer1_keypair.pem"
					ca_file: "configs/certs/ocsp_peer/mini-ca/root/root_cert.pem"
					timeout: 5
					verify: true
					# Long form configuration
					ocsp_peer: {
						verify: true
						ca_timeout: 5
						allowed_clockskew: 30
					}
				}
			`,
			[]nats.Option{
				nats.ClientCert("./configs/certs/ocsp_peer/mini-ca/client1/UserA1_bundle.pem", "./configs/certs/ocsp_peer/mini-ca/client1/private/UserA1_keypair.pem"),
				nats.RootCAs("./configs/certs/ocsp_peer/mini-ca/root/root_cert.pem"),
				nats.ErrorHandler(noOpErrHandler),
			},
			nil,
			nil,
		},
		{
			"https with just ocsp",
			`
				net: 127.0.0.1
				port: -1
				https: -1
				ocsp {
                                  mode = always
                                  url = http://127.0.0.1:18888
                                }
                                store_dir = %s

				tls: {
					cert_file: "configs/certs/ocsp_peer/mini-ca/server1/TestServer1_bundle.pem"
					key_file: "configs/certs/ocsp_peer/mini-ca/server1/private/TestServer1_keypair.pem"
					ca_file: "configs/certs/ocsp_peer/mini-ca/root/root_cert.pem"
					timeout: 5
					verify: true
				}
			`,
			[]nats.Option{
				nats.ClientCert("./configs/certs/ocsp_peer/mini-ca/client1/UserA1_bundle.pem", "./configs/certs/ocsp_peer/mini-ca/client1/private/UserA1_keypair.pem"),
				nats.RootCAs("./configs/certs/ocsp_peer/mini-ca/root/root_cert.pem"),
				nats.ErrorHandler(noOpErrHandler),
			},
			nil,
			nil,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			deleteLocalStore(t, "")
			content := test.config
			conf := createConfFile(t, []byte(fmt.Sprintf(content, t.TempDir())))
			s, opts := RunServerWithConfig(conf)
			defer s.Shutdown()
			nc, err := nats.Connect(fmt.Sprintf("tls://localhost:%d", opts.Port), test.opts...)
			if test.err == nil && err != nil {
				t.Errorf("Expected to connect, got %v", err)
			} else if test.err != nil && err == nil {
				t.Errorf("Expected error on connect")
			} else if test.err != nil && err != nil {
				// Error on connect was expected
				if test.err.Error() != err.Error() {
					t.Errorf("Expected error %s, got: %s", test.err, err)
				}
				return
			}
			defer nc.Close()
			nc.Subscribe("ping", func(m *nats.Msg) {
				m.Respond([]byte("pong"))
			})
			nc.Flush()
			_, err = nc.Request("ping", []byte("ping"), 250*time.Millisecond)
			if test.rerr != nil && err == nil {
				t.Errorf("Expected error getting response")
			} else if test.rerr == nil && err != nil {
				t.Errorf("Expected response")
			}

			// Make request to the HTTPS port using the client cert.
			tlsConfig := &tls.Config{}
			clientCertFile := "./configs/certs/ocsp_peer/mini-ca/client1/UserA1_bundle.pem"
			clientKeyFile := "./configs/certs/ocsp_peer/mini-ca/client1/private/UserA1_keypair.pem"
			cert, err := tls.LoadX509KeyPair(clientCertFile, clientKeyFile)
			if err != nil {
				t.Fatal(err)
			}
			tlsConfig.Certificates = []tls.Certificate{cert}
			caCertFile := "./configs/certs/ocsp_peer/mini-ca/root/root_cert.pem"
			caCert, err := os.ReadFile(caCertFile)
			if err != nil {
				t.Fatal(err)
			}
			caCertPool := x509.NewCertPool()
			caCertPool.AppendCertsFromPEM(caCert)
			tlsConfig.RootCAs = caCertPool

			hc := &http.Client{
				Transport: &http.Transport{
					TLSClientConfig: tlsConfig,
				},
			}
			resp, err := hc.Get("https://" + s.MonitorAddr().String())
			if err != nil {
				t.Fatal(err)
			}
			defer resp.Body.Close()
			if resp.StatusCode != 200 {
				t.Errorf("Unexpected status: %v", resp.Status)
			}
		})
	}
}

// TestOCSPPeerResponseSerialMismatch is test of a CA signed OCSP response for a different certificate than the one presented
func TestOCSPPeerResponseSerialMismatch(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	rootCAResponder := NewOCSPResponderRootCA(t)
	rootCAResponderURL := fmt.Sprintf("http://%s", rootCAResponder.Addr)
	defer rootCAResponder.Shutdown(ctx)
	SetOCSPStatus(t, rootCAResponderURL, "configs/certs/ocsp_peer/mini-ca/intermediate1/intermediate1_cert.pem", ocsp.Good)

	// Intermediate CA responder always answers good for UserA2, regardless of the requested certificate
	ca, err := tls.LoadX509KeyPair("configs/certs/ocsp_peer/mini-ca/intermediate1/intermediate1_cert.pem", "configs/certs/ocsp_peer/mini-ca/intermediate1/private/intermediate1_keypair.pem")
	if err != nil {
		t.Fatal(err)
	}
	other, err := tls.LoadX509KeyPair("configs/certs/ocsp_peer/mini-ca/client1/UserA2_bundle.pem", "configs/certs/ocsp_peer/mini-ca/client1/private/UserA2_keypair.pem")
	if err != nil {
		t.Fatal(err)
	}
	resp, err := ocsp.CreateResponse(ca.Leaf, ca.Leaf, ocsp.Response{
		Status:       ocsp.Good,
		SerialNumber: other.Leaf.SerialNumber,
		ThisUpdate:   time.Now(),
		NextUpdate:   time.Now().Add(time.Hour),
	}, ca.PrivateKey.(crypto.Signer))
	if err != nil {
		t.Fatal(err)
	}
	// Listen synchronously, the server must not silently fail to reach the responder.
	ln, err := net.Listen("tcp", "127.0.0.1:18888")
	if err != nil {
		t.Fatal(err)
	}
	var requests atomic.Int32
	intermediateCA1Responder := &http.Server{Handler: http.HandlerFunc(func(rw http.ResponseWriter, _ *http.Request) {
		requests.Add(1)
		rw.Header().Set("Content-Type", "application/ocsp-response")
		rw.Write(resp)
	})}
	go intermediateCA1Responder.Serve(ln)
	defer intermediateCA1Responder.Shutdown(ctx)

	deleteLocalStore(t, "")
	conf := createConfFile(t, []byte(`
		port: -1
		tls: {
			cert_file: "configs/certs/ocsp_peer/mini-ca/server1/TestServer1_bundle.pem"
			key_file: "configs/certs/ocsp_peer/mini-ca/server1/private/TestServer1_keypair.pem"
			ca_file: "configs/certs/ocsp_peer/mini-ca/root/root_cert.pem"
			timeout: 5
			verify: true
			# Turn on CA OCSP check so unvalidated clients can't connect
			ocsp_peer: true
		}
	`))
	s, opts := RunServerWithConfig(conf)
	defer s.Shutdown()

	// UserA1 must be rejected, the only response available is for UserA2
	nc, err := nats.Connect(fmt.Sprintf("tls://localhost:%d", opts.Port),
		nats.ClientCert("./configs/certs/ocsp_peer/mini-ca/client1/UserA1_bundle.pem", "./configs/certs/ocsp_peer/mini-ca/client1/private/UserA1_keypair.pem"),
		nats.RootCAs("./configs/certs/ocsp_peer/mini-ca/root/root_cert.pem"),
		nats.ErrorHandler(noOpErrHandler),
	)
	if err == nil {
		nc.Close()
		t.Fatalf("Expected error on connect")
	}
	if err.Error() != "remote error: tls: bad certificate" {
		t.Fatalf("Expected error remote error: tls: bad certificate, got: %s", err)
	}
	if requests.Load() == 0 {
		t.Fatal("Expected the intermediate CA responder to be queried")
	}
}
