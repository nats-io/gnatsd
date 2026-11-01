// Copyright 2021-2026 The NATS Authors
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
	"bytes"
	"context"
	"crypto/tls"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	. "github.com/nats-io/nats-server/v2/internal/ocsp"
	"github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
	"golang.org/x/crypto/ocsp"
)

func TestOCSPAlwaysMustStapleAndShutdown(t *testing.T) {
	// Certs that have must staple will auto shutdown the server.
	const (
		caCert     = "configs/certs/ocsp/ca-cert.pem"
		caKey      = "configs/certs/ocsp/ca-key.pem"
		serverCert = "configs/certs/ocsp/server-cert.pem"
		serverKey  = "configs/certs/ocsp/server-key.pem"
	)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ocspr := NewOCSPResponder(t, caCert, caKey)
	defer ocspr.Shutdown(ctx)
	addr := fmt.Sprintf("http://%s", ocspr.Addr)
	SetOCSPStatus(t, addr, serverCert, ocsp.Good)

	opts := server.Options{}
	opts.Host = "127.0.0.1"
	opts.NoLog = true
	opts.NoSigs = true
	opts.MaxControlLine = 4096
	opts.Port = -1
	opts.TLSCert = serverCert
	opts.TLSKey = serverKey
	opts.TLSCaCert = caCert
	opts.TLSTimeout = 5
	tcOpts := &server.TLSConfigOpts{
		CertFile: opts.TLSCert,
		KeyFile:  opts.TLSKey,
		CaFile:   opts.TLSCaCert,
		Timeout:  opts.TLSTimeout,
	}

	tlsConf, err := server.GenTLSConfig(tcOpts)
	if err != nil {
		t.Fatal(err)
	}
	opts.TLSConfig = tlsConf

	opts.OCSPConfig = &server.OCSPConfig{
		Mode:         server.OCSPModeAlways,
		OverrideURLs: []string{addr},
	}
	srv := RunServer(&opts)
	defer srv.Shutdown()

	nc, err := nats.Connect(fmt.Sprintf("tls://localhost:%d", opts.Port),
		nats.Secure(&tls.Config{
			VerifyConnection: func(s tls.ConnectionState) error {
				resp, err := GetOCSPStatus(s)
				if err != nil {
					return err
				}
				if resp.Status != ocsp.Good {
					return fmt.Errorf("invalid staple")
				}
				return nil
			},
		}),
		nats.RootCAs(caCert),
		nats.ErrorHandler(noOpErrHandler),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer nc.Close()
	sub, err := nc.SubscribeSync("foo")
	if err != nil {
		t.Fatal(err)
	}
	nc.Publish("foo", []byte("hello world"))
	nc.Flush()

	_, err = sub.NextMsg(1 * time.Second)
	if err != nil {
		t.Fatal(err)
	}
	nc.Close()

	// The server will shutdown because the server becomes revoked
	// and the policy is to always must-staple.  The OCSP Responder
	// instructs the NATS Server to fetch OCSP Staples every 2 seconds.
	time.Sleep(2 * time.Second)
	SetOCSPStatus(t, addr, serverCert, ocsp.Revoked)
	time.Sleep(2 * time.Second)

	// Should be connection refused since server will abort now.
	_, err = nats.Connect(fmt.Sprintf("tls://localhost:%d", opts.Port),
		nats.RootCAs(caCert),
		nats.ErrorHandler(noOpErrHandler),
	)
	if err != nats.ErrNoServers {
		t.Errorf("Expected connection refused")
	}
	// Verify that the server finishes shutdown
	srv.WaitForShutdown()
}

func TestOCSPMustStapleShutdown(t *testing.T) {
	const (
		caCert     = "configs/certs/ocsp/ca-cert.pem"
		caKey      = "configs/certs/ocsp/ca-key.pem"
		serverCert = "configs/certs/ocsp/server-status-request-cert.pem"
		serverKey  = "configs/certs/ocsp/server-status-request-key.pem"
	)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ocspr := NewOCSPResponder(t, caCert, caKey)
	defer ocspr.Shutdown(ctx)
	addr := fmt.Sprintf("http://%s", ocspr.Addr)
	SetOCSPStatus(t, addr, serverCert, ocsp.Good)

	opts := server.Options{}
	opts.Host = "127.0.0.1"
	opts.NoLog = true
	opts.NoSigs = true
	opts.MaxControlLine = 4096
	opts.Port = -1
	opts.TLSCert = serverCert
	opts.TLSKey = serverKey
	opts.TLSCaCert = caCert
	opts.TLSTimeout = 5
	tlsConfigOpts := &server.TLSConfigOpts{
		CertFile: opts.TLSCert,
		KeyFile:  opts.TLSKey,
		CaFile:   opts.TLSCaCert,
		Timeout:  opts.TLSTimeout,
	}

	tlsConf, err := server.GenTLSConfig(tlsConfigOpts)
	if err != nil {
		t.Fatal(err)
	}
	opts.TLSConfig = tlsConf

	opts.OCSPConfig = &server.OCSPConfig{
		Mode:         server.OCSPModeMust,
		OverrideURLs: []string{addr},
	}

	srv := RunServer(&opts)
	defer srv.Shutdown()

	nc, err := nats.Connect(fmt.Sprintf("tls://localhost:%d", opts.Port),
		nats.Secure(&tls.Config{
			VerifyConnection: func(s tls.ConnectionState) error {
				resp, err := GetOCSPStatus(s)
				if err != nil {
					return err
				}
				if resp.Status != ocsp.Good {
					return fmt.Errorf("invalid staple")
				}
				return nil
			},
		}),
		nats.RootCAs(caCert),
		nats.ErrorHandler(noOpErrHandler),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer nc.Close()
	sub, err := nc.SubscribeSync("foo")
	if err != nil {
		t.Fatal(err)
	}
	nc.Publish("foo", []byte("hello world"))
	nc.Flush()

	_, err = sub.NextMsg(1 * time.Second)
	if err != nil {
		t.Fatal(err)
	}
	nc.Close()

	// The server will shutdown because the server becomes revoked
	// and the policy is to always must-staple.  The OCSP Responder
	// instructs the NATS Server to fetch OCSP Staples every 2 seconds.
	time.Sleep(2 * time.Second)
	SetOCSPStatus(t, addr, serverCert, ocsp.Revoked)
	time.Sleep(2 * time.Second)

	// Should be connection refused since server will abort now.
	_, err = nats.Connect(fmt.Sprintf("tls://localhost:%d", opts.Port),
		nats.RootCAs(caCert),
		nats.ErrorHandler(noOpErrHandler),
	)
	if err != nats.ErrNoServers {
		t.Errorf("Expected connection refused")
	}
}

func TestOCSPMustStapleAutoDoesNotShutdown(t *testing.T) {
	const (
		caCert     = "configs/certs/ocsp/ca-cert.pem"
		caKey      = "configs/certs/ocsp/ca-key.pem"
		serverCert = "configs/certs/ocsp/server-status-request-url-01-cert.pem"
	)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ocspr := NewOCSPResponder(t, caCert, caKey)
	defer ocspr.Shutdown(ctx)
	addr := fmt.Sprintf("http://%s", ocspr.Addr)
	SetOCSPStatus(t, addr, serverCert, ocsp.Good)

	content := `
		port: -1

		tls {
			cert_file: "configs/certs/ocsp/server-status-request-url-01-cert.pem"
			key_file: "configs/certs/ocsp/server-status-request-url-01-key.pem"
			ca_file: "configs/certs/ocsp/ca-cert.pem"
			timeout: 5
		}
	`
	conf := createConfFile(t, []byte(content))
	s, opts := RunServerWithConfig(conf)
	defer s.Shutdown()

	nc, err := nats.Connect(fmt.Sprintf("tls://localhost:%d", opts.Port),
		nats.Secure(&tls.Config{
			VerifyConnection: func(s tls.ConnectionState) error {
				resp, err := GetOCSPStatus(s)
				if err != nil {
					return err
				}
				if resp.Status != ocsp.Good {
					t.Errorf("Expected valid OCSP staple status")
				}
				return nil
			},
		}),
		nats.RootCAs(caCert),
		nats.ErrorHandler(noOpErrHandler),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer nc.Close()
	sub, err := nc.SubscribeSync("foo")
	if err != nil {
		t.Fatal(err)
	}
	nc.Publish("foo", []byte("hello world"))
	nc.Flush()

	_, err = sub.NextMsg(1 * time.Second)
	if err != nil {
		t.Fatal(err)
	}
	nc.Close()

	// The server will shutdown because the server becomes revoked
	// and the policy is to always must-staple.  The OCSP Responder
	// instructs the NATS Server to fetch OCSP Staples every 2 seconds.
	time.Sleep(2 * time.Second)
	SetOCSPStatus(t, addr, serverCert, ocsp.Revoked)
	time.Sleep(2 * time.Second)

	// Should not be connection refused, the client will continue running and
	// be served the stale OCSP staple instead.
	nc, err = nats.Connect(fmt.Sprintf("tls://localhost:%d", opts.Port),
		nats.Secure(&tls.Config{
			VerifyConnection: func(s tls.ConnectionState) error {
				resp, err := GetOCSPStatus(s)
				if err != nil {
					return err
				}
				if resp.Status != ocsp.Revoked {
					t.Errorf("Expected revoked status")
				}
				return nil
			},
		}),
		nats.RootCAs(caCert),
		nats.ErrorHandler(noOpErrHandler),
	)
	if err != nil {
		t.Fatal(err)
	}
	nc.Close()
}

func TestOCSPAutoWithoutMustStapleDoesNotShutdownOnRevoke(t *testing.T) {
	const (
		caCert     = "configs/certs/ocsp/ca-cert.pem"
		caKey      = "configs/certs/ocsp/ca-key.pem"
		serverCert = "configs/certs/ocsp/server-cert.pem"
		serverKey  = "configs/certs/ocsp/server-key.pem"
	)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ocspr := NewOCSPResponder(t, caCert, caKey)
	defer ocspr.Shutdown(ctx)
	addr := fmt.Sprintf("http://%s", ocspr.Addr)
	SetOCSPStatus(t, addr, serverCert, ocsp.Good)

	opts := server.Options{}
	opts.Host = "127.0.0.1"
	opts.NoLog = true
	opts.NoSigs = true
	opts.MaxControlLine = 4096
	opts.Port = -1
	opts.TLSCert = serverCert
	opts.TLSKey = serverKey
	opts.TLSCaCert = caCert
	opts.TLSTimeout = 5
	tlsConfigOpts := &server.TLSConfigOpts{
		CertFile: opts.TLSCert,
		KeyFile:  opts.TLSKey,
		CaFile:   opts.TLSCaCert,
		Timeout:  opts.TLSTimeout,
	}
	tlsConf, err := server.GenTLSConfig(tlsConfigOpts)
	if err != nil {
		t.Fatal(err)
	}
	opts.TLSConfig = tlsConf

	opts.OCSPConfig = &server.OCSPConfig{
		Mode:         server.OCSPModeAuto,
		OverrideURLs: []string{addr},
	}

	srv := RunServer(&opts)
	defer srv.Shutdown()

	nc, err := nats.Connect(fmt.Sprintf("tls://localhost:%d", opts.Port),
		nats.Secure(&tls.Config{
			VerifyConnection: func(s tls.ConnectionState) error {
				if s.OCSPResponse != nil {
					return fmt.Errorf("Unexpected OCSP staple for certificate")
				}
				return nil
			},
		}),

		nats.RootCAs(caCert),
		nats.ErrorHandler(noOpErrHandler),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer nc.Close()
	sub, err := nc.SubscribeSync("foo")
	if err != nil {
		t.Fatal(err)
	}
	nc.Publish("foo", []byte("hello world"))
	nc.Flush()

	_, err = sub.NextMsg(1 * time.Second)
	if err != nil {
		t.Fatal(err)
	}
	nc.Close()

	// Revoke the client certificate, nothing will happens since does
	// not have MustStaple.
	time.Sleep(2 * time.Second)
	SetOCSPStatus(t, addr, serverCert, ocsp.Revoked)
	time.Sleep(2 * time.Second)

	// Should not be connection refused since server will continue running.
	nc, err = nats.Connect(fmt.Sprintf("tls://localhost:%d", opts.Port),
		nats.RootCAs(caCert),
		nats.ErrorHandler(noOpErrHandler),
	)
	if err != nil {
		t.Errorf("Unexpected error: %s", err)
	}
	nc.Close()
}

func TestOCSPClient(t *testing.T) {
	const (
		caCert     = "configs/certs/ocsp/ca-cert.pem"
		caKey      = "configs/certs/ocsp/ca-key.pem"
		serverCert = "configs/certs/ocsp/server-cert.pem"
		serverKey  = "configs/certs/ocsp/server-key.pem"
	)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ocspr := NewOCSPResponder(t, caCert, caKey)
	ocspURL := fmt.Sprintf("http://%s", ocspr.Addr)
	defer ocspr.Shutdown(ctx)

	for _, test := range []struct {
		name      string
		config    string
		opts      []nats.Option
		err       error
		rerr      error
		configure func()
	}{
		{
			"OCSP Stapling makes server fail to boot if status is unknown",
			`
				port: -1

				# Enable OCSP stapling with policy to honor must staple if present.
				ocsp: true

				tls {
					cert_file: "configs/certs/ocsp/server-cert.pem"
					key_file: "configs/certs/ocsp/server-key.pem"
					ca_file: "configs/certs/ocsp/ca-cert.pem"
					timeout: 5
				}
			`,
			[]nats.Option{
				nats.ClientCert("./configs/certs/ocsp/client-cert.pem", "./configs/certs/ocsp/client-key.pem"),
				nats.RootCAs(caCert),
				nats.ErrorHandler(noOpErrHandler),
			},
			nil,
			nil,
			func() {},
		},
		{
			"OCSP Stapling ignored by default if server without must staple status",
			`
				port: -1

				ocsp: true

				tls {
					cert_file: "configs/certs/ocsp/server-cert.pem"
					key_file: "configs/certs/ocsp/server-key.pem"
					ca_file: "configs/certs/ocsp/ca-cert.pem"
					timeout: 5
				}
			`,
			[]nats.Option{
				nats.ClientCert("./configs/certs/ocsp/client-cert.pem", "./configs/certs/ocsp/client-key.pem"),
				nats.RootCAs(caCert),
				nats.ErrorHandler(noOpErrHandler),
			},
			nil,
			nil,
			func() { SetOCSPStatus(t, ocspURL, serverCert, ocsp.Good) },
		},
		{
			"OCSP Stapling honored by default if server has must staple status",
			`
				port: -1

				tls {
					cert_file: "configs/certs/ocsp/server-status-request-url-01-cert.pem"
					key_file: "configs/certs/ocsp/server-status-request-url-01-key.pem"
					ca_file: "configs/certs/ocsp/ca-cert.pem"
					timeout: 5
				}
			`,
			[]nats.Option{
				nats.ClientCert("./configs/certs/ocsp/client-cert.pem", "./configs/certs/ocsp/client-key.pem"),
				nats.RootCAs(caCert),
				nats.ErrorHandler(noOpErrHandler),
			},
			nil,
			nil,
			func() {
				SetOCSPStatus(t, ocspURL, "configs/certs/ocsp/server-status-request-url-01-cert.pem", ocsp.Good)
			},
		},
		{
			"OCSP Stapling can be disabled even if server has must staple status",
			`
				port: -1

				ocsp: false

				tls {
					cert_file: "configs/certs/ocsp/server-status-request-url-01-cert.pem"
					key_file: "configs/certs/ocsp/server-status-request-url-01-key.pem"
					ca_file: "configs/certs/ocsp/ca-cert.pem"
					timeout: 5
				}
			`,
			[]nats.Option{
				nats.ClientCert("./configs/certs/ocsp/client-cert.pem", "./configs/certs/ocsp/client-key.pem"),
				nats.RootCAs(caCert),
				nats.ErrorHandler(noOpErrHandler),
			},
			nil,
			nil,
			func() {
				SetOCSPStatus(t, ocspURL, "configs/certs/ocsp/server-status-request-url-01-cert.pem", ocsp.Revoked)
			},
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

func TestOCSPReloadRotateTLSCertWithNoURL(t *testing.T) {
	const (
		caCert            = "configs/certs/ocsp/ca-cert.pem"
		caKey             = "configs/certs/ocsp/ca-key.pem"
		serverCert        = "configs/certs/ocsp/server-status-request-url-01-cert.pem"
		updatedServerCert = "configs/certs/ocsp/server-status-request-cert.pem"
	)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ocspr := NewOCSPResponder(t, caCert, caKey)
	defer ocspr.Shutdown(ctx)
	addr := fmt.Sprintf("http://%s", ocspr.Addr)
	SetOCSPStatus(t, addr, serverCert, ocsp.Good)

	content := `
		port: -1

		tls {
			cert_file: "configs/certs/ocsp/server-status-request-url-01-cert.pem"
			key_file: "configs/certs/ocsp/server-status-request-url-01-key.pem"
			ca_file: "configs/certs/ocsp/ca-cert.pem"
			timeout: 5
		}
	`
	conf := createConfFile(t, []byte(content))
	s, opts := RunServerWithConfig(conf)
	defer s.Shutdown()

	nc, err := nats.Connect(fmt.Sprintf("tls://localhost:%d", opts.Port),
		nats.Secure(&tls.Config{
			VerifyConnection: func(s tls.ConnectionState) error {
				resp, err := GetOCSPStatus(s)
				if err != nil {
					return err
				}
				if resp.Status != ocsp.Good {
					t.Errorf("Expected valid OCSP staple status")
				}
				return nil
			},
		}),
		nats.RootCAs(caCert),
		nats.ErrorHandler(noOpErrHandler),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer nc.Close()
	sub, err := nc.SubscribeSync("foo")
	if err != nil {
		t.Fatal(err)
	}
	nc.Publish("foo", []byte("hello world"))
	nc.Flush()

	_, err = sub.NextMsg(1 * time.Second)
	if err != nil {
		t.Fatal(err)
	}
	nc.Close()

	// Change the contents with another that will fail to get a staple
	// since it does not have an URL.
	content = `
		port: -1

		tls {
			cert_file: "configs/certs/ocsp/server-status-request-cert.pem"
			key_file: "configs/certs/ocsp/server-status-request-key.pem"
			ca_file: "configs/certs/ocsp/ca-cert.pem"
			timeout: 5
		}
	`
	if err := os.WriteFile(conf, []byte(content), 0666); err != nil {
		t.Fatalf("Error writing config: %v", err)
	}
	// Reload show warning because of cert missing OCSP Url so cannot be used
	// with OCSP stapling.
	if err := s.Reload(); err != nil {
		t.Fatal(err)
	}
	expectedErr := fmt.Errorf("missing OCSP response")
	// The server will not shutdown because the reload will fail.
	_, err = nats.Connect(fmt.Sprintf("tls://localhost:%d", opts.Port),
		nats.Secure(&tls.Config{
			VerifyConnection: func(s tls.ConnectionState) error {
				// The new certificate does not have OCSP Staples since
				// it could not fetch one from a OCSP server.
				if s.OCSPResponse == nil {
					return expectedErr
				}
				return nil
			},
		}),
		nats.RootCAs(caCert),
		nats.ErrorHandler(noOpErrHandler),
	)
	if err == nil || !strings.Contains(err.Error(), expectedErr.Error()) {
		t.Fatalf("Unexpected error: %s, got %s", expectedErr, err)
	}
}

func TestOCSPReloadRotateTLSCertDisableMustStaple(t *testing.T) {
	const (
		caCert            = "configs/certs/ocsp/ca-cert.pem"
		caKey             = "configs/certs/ocsp/ca-key.pem"
		serverCert        = "configs/certs/ocsp/server-status-request-url-01-cert.pem"
		updatedServerCert = "configs/certs/ocsp/server-status-request-cert.pem"
	)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ocspr := NewOCSPResponder(t, caCert, caKey)
	defer ocspr.Shutdown(ctx)
	addr := fmt.Sprintf("http://%s", ocspr.Addr)
	SetOCSPStatus(t, addr, serverCert, ocsp.Good)

	storeDir := t.TempDir()

	originalContent := `
		port: -1

		store_dir: '%s'

		tls {
			cert_file: "configs/certs/ocsp/server-status-request-url-01-cert.pem"
			key_file: "configs/certs/ocsp/server-status-request-url-01-key.pem"
			ca_file: "configs/certs/ocsp/ca-cert.pem"
			timeout: 5
		}
	`

	content := fmt.Sprintf(originalContent, storeDir)
	conf := createConfFile(t, []byte(content))
	s, opts := RunServerWithConfig(conf)
	defer s.Shutdown()

	var staple []byte
	nc, err := nats.Connect(fmt.Sprintf("tls://localhost:%d", opts.Port),
		nats.Secure(&tls.Config{
			VerifyConnection: func(s tls.ConnectionState) error {
				staple = s.OCSPResponse
				resp, err := GetOCSPStatus(s)
				if err != nil {
					return err
				}
				if resp.Status != ocsp.Good {
					t.Errorf("Expected valid OCSP staple status")
				}
				return nil
			},
		}),
		nats.RootCAs(caCert),
		nats.ErrorHandler(noOpErrHandler),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer nc.Close()
	sub, err := nc.SubscribeSync("foo")
	if err != nil {
		t.Fatal(err)
	}
	nc.Publish("foo", []byte("hello world"))
	nc.Flush()

	_, err = sub.NextMsg(1 * time.Second)
	if err != nil {
		t.Fatal(err)
	}
	nc.Close()

	files := []string{}
	err = filepath.Walk(storeDir+"/ocsp/", func(path string, info os.FileInfo, err error) error {
		if info.IsDir() {
			return nil
		}
		files = append(files, path)
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	found := false
	for _, file := range files {
		data, err := os.ReadFile(file)
		if err != nil {
			t.Error(err)
		}
		if bytes.Equal(staple, data) {
			found = true
		}
	}
	if !found {
		t.Error("Could not find OCSP Staple")
	}

	// Change the contents with another that has OCSP Stapling disabled.
	updatedContent := `
		port: -1

		store_dir: '%s'

		tls {
			cert_file: "configs/certs/ocsp/server-cert.pem"
			key_file: "configs/certs/ocsp/server-key.pem"
			ca_file: "configs/certs/ocsp/ca-cert.pem"
			timeout: 5
		}
	`
	content = fmt.Sprintf(updatedContent, storeDir)
	if err := os.WriteFile(conf, []byte(content), 0666); err != nil {
		t.Fatalf("Error writing config: %v", err)
	}
	if err := s.Reload(); err != nil {
		t.Fatal(err)
	}

	// The new certificate does not have must staple so they will be missing.
	time.Sleep(4 * time.Second)

	nc, err = nats.Connect(fmt.Sprintf("tls://localhost:%d", opts.Port),
		nats.Secure(&tls.Config{
			VerifyConnection: func(s tls.ConnectionState) error {
				if s.OCSPResponse != nil {
					return fmt.Errorf("unexpected OCSP Staple!")
				}
				return nil
			},
		}),
		nats.RootCAs(caCert),
		nats.ErrorHandler(noOpErrHandler),
	)
	if err != nil {
		t.Fatal(err)
	}
	nc.Close()

	// Re-enable OCSP Stapling
	content = fmt.Sprintf(originalContent, storeDir)
	if err := os.WriteFile(conf, []byte(content), 0666); err != nil {
		t.Fatalf("Error writing config: %v", err)
	}
	if err := s.Reload(); err != nil {
		t.Fatal(err)
	}

	var newStaple []byte
	nc, err = nats.Connect(fmt.Sprintf("tls://localhost:%d", opts.Port),
		nats.Secure(&tls.Config{
			VerifyConnection: func(s tls.ConnectionState) error {
				newStaple = s.OCSPResponse
				resp, err := GetOCSPStatus(s)
				if err != nil {
					return err
				}
				if resp.Status != ocsp.Good {
					t.Errorf("Expected valid OCSP staple status")
				}
				return nil
			},
		}),
		nats.RootCAs(caCert),
		nats.ErrorHandler(noOpErrHandler),
	)
	if err != nil {
		t.Fatal(err)
	}
	nc.Close()

	// Confirm that it got a new staple.
	files = []string{}
	err = filepath.Walk(storeDir+"/ocsp/", func(path string, info os.FileInfo, err error) error {
		if info.IsDir() {
			return nil
		}
		files = append(files, path)
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	found = false
	for _, file := range files {
		data, err := os.ReadFile(file)
		if err != nil {
			t.Error(err)
		}
		if bytes.Equal(newStaple, data) {
			found = true
		}
	}
	if !found {
		t.Error("Could not find OCSP Staple")
	}
	if bytes.Equal(staple, newStaple) {
		t.Error("Expected new OCSP Staple")
	}
}

func TestOCSPReloadRotateTLSCertEnableMustStaple(t *testing.T) {
	const (
		caCert            = "configs/certs/ocsp/ca-cert.pem"
		caKey             = "configs/certs/ocsp/ca-key.pem"
		serverCert        = "configs/certs/ocsp/server-cert.pem"
		updatedServerCert = "configs/certs/ocsp/server-status-request-url-01-cert.pem"
	)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ocspr := NewOCSPResponder(t, caCert, caKey)
	defer ocspr.Shutdown(ctx)
	addr := fmt.Sprintf("http://%s", ocspr.Addr)
	SetOCSPStatus(t, addr, serverCert, ocsp.Good)
	SetOCSPStatus(t, addr, updatedServerCert, ocsp.Good)

	// Start without OCSP Stapling MustStaple
	content := `
		port: -1

		tls {
			cert_file: "configs/certs/ocsp/server-cert.pem"
			key_file: "configs/certs/ocsp/server-key.pem"
			ca_file: "configs/certs/ocsp/ca-cert.pem"
			timeout: 5
		}
	`
	conf := createConfFile(t, []byte(content))
	s, opts := RunServerWithConfig(conf)
	defer s.Shutdown()

	nc, err := nats.Connect(fmt.Sprintf("tls://localhost:%d", opts.Port),
		nats.Secure(&tls.Config{
			VerifyConnection: func(s tls.ConnectionState) error {
				if s.OCSPResponse != nil {
					return fmt.Errorf("unexpected OCSP Staple!")
				}
				return nil
			},
		}),
		nats.RootCAs(caCert),
		nats.ErrorHandler(noOpErrHandler),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer nc.Close()
	sub, err := nc.SubscribeSync("foo")
	if err != nil {
		t.Fatal(err)
	}
	nc.Publish("foo", []byte("hello world"))
	nc.Flush()

	_, err = sub.NextMsg(1 * time.Second)
	if err != nil {
		t.Fatal(err)
	}
	nc.Close()

	// Change the contents with another that has OCSP Stapling enabled.
	content = `
		port: -1

		tls {
			cert_file: "configs/certs/ocsp/server-status-request-url-01-cert.pem"
			key_file: "configs/certs/ocsp/server-status-request-url-01-key.pem"
			ca_file: "configs/certs/ocsp/ca-cert.pem"
			timeout: 5
		}
	`
	if err := os.WriteFile(conf, []byte(content), 0666); err != nil {
		t.Fatalf("Error writing config: %v", err)
	}
	if err := s.Reload(); err != nil {
		t.Fatal(err)
	}

	// The new certificate does not have must staple so they will be missing.
	time.Sleep(2 * time.Second)

	nc, err = nats.Connect(fmt.Sprintf("tls://localhost:%d", opts.Port),
		nats.Secure(&tls.Config{
			VerifyConnection: func(s tls.ConnectionState) error {
				resp, err := GetOCSPStatus(s)
				if err != nil {
					return err
				}
				if resp.Status != ocsp.Good {
					t.Errorf("Expected valid OCSP staple status")
				}
				return nil
			},
		}),
		nats.RootCAs(caCert),
		nats.ErrorHandler(noOpErrHandler),
	)
	if err != nil {
		t.Fatal(err)
	}
	nc.Close()
}

func TestOCSPCluster(t *testing.T) {
	const (
		caCert = "configs/certs/ocsp/ca-cert.pem"
		caKey  = "configs/certs/ocsp/ca-key.pem"
	)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ocspr := NewOCSPResponder(t, caCert, caKey)
	defer ocspr.Shutdown(ctx)
	addr := fmt.Sprintf("http://%s", ocspr.Addr)
	SetOCSPStatus(t, addr, "configs/certs/ocsp/server-status-request-url-01-cert.pem", ocsp.Good)
	SetOCSPStatus(t, addr, "configs/certs/ocsp/server-status-request-url-02-cert.pem", ocsp.Good)
	SetOCSPStatus(t, addr, "configs/certs/ocsp/server-status-request-url-03-cert.pem", ocsp.Good)
	SetOCSPStatus(t, addr, "configs/certs/ocsp/server-status-request-url-04-cert.pem", ocsp.Good)
	SetOCSPStatus(t, addr, "configs/certs/ocsp/server-status-request-url-05-cert.pem", ocsp.Good)
	SetOCSPStatus(t, addr, "configs/certs/ocsp/server-status-request-url-06-cert.pem", ocsp.Good)
	SetOCSPStatus(t, addr, "configs/certs/ocsp/server-status-request-url-07-cert.pem", ocsp.Good)
	SetOCSPStatus(t, addr, "configs/certs/ocsp/server-status-request-url-08-cert.pem", ocsp.Good)

	// Store Dirs
	storeDirA := t.TempDir()
	storeDirB := t.TempDir()
	storeDirC := t.TempDir()

	// Seed server configuration
	srvConfA := `
		host: "127.0.0.1"
		port: -1

		server_name: "AAA"

		tls {
			cert_file: "configs/certs/ocsp/server-status-request-url-01-cert.pem"
			key_file: "configs/certs/ocsp/server-status-request-url-01-key.pem"
			ca_file: "configs/certs/ocsp/ca-cert.pem"
			timeout: 5
		}
		store_dir: '%s'
		cluster {
			name: AB
			host: "127.0.0.1"
			advertise: 127.0.0.1
			port: -1
			pool_size: -1

			tls {
				cert_file: "configs/certs/ocsp/server-status-request-url-02-cert.pem"
				key_file: "configs/certs/ocsp/server-status-request-url-02-key.pem"
				ca_file: "configs/certs/ocsp/ca-cert.pem"
				timeout: 5
			}
		}
	`
	srvConfA = fmt.Sprintf(srvConfA, storeDirA)
	sconfA := createConfFile(t, []byte(srvConfA))
	srvA, optsA := RunServerWithConfig(sconfA)
	defer srvA.Shutdown()

	// The rest
	srvConfB := `
		host: "127.0.0.1"
		port: -1

		server_name: "BBB"

		tls {
			cert_file: "configs/certs/ocsp/server-status-request-url-03-cert.pem"
			key_file: "configs/certs/ocsp/server-status-request-url-03-key.pem"
			ca_file: "configs/certs/ocsp/ca-cert.pem"
			timeout: 5
		}
		store_dir: '%s'
		cluster {
			name: AB
			host: "127.0.0.1"
			advertise: 127.0.0.1
			port: -1
			pool_size: -1

			routes: [ nats://127.0.0.1:%d ]
			connect_retries: 30

			tls {
				cert_file: "configs/certs/ocsp/server-status-request-url-04-cert.pem"
				key_file: "configs/certs/ocsp/server-status-request-url-04-key.pem"
				ca_file: "configs/certs/ocsp/ca-cert.pem"
				timeout: 5
			}
		}
	`
	srvConfB = fmt.Sprintf(srvConfB, storeDirB, optsA.Cluster.Port)
	conf := createConfFile(t, []byte(srvConfB))
	srvB, optsB := RunServerWithConfig(conf)
	defer srvB.Shutdown()

	// Client connects to server A.
	cA, err := nats.Connect(fmt.Sprintf("tls://localhost:%d", optsA.Port),
		nats.Secure(&tls.Config{
			VerifyConnection: func(s tls.ConnectionState) error {
				if s.OCSPResponse == nil {
					return fmt.Errorf("missing OCSP Staple from server")
				}
				return nil
			},
		}),
		nats.RootCAs(caCert),
		nats.ErrorHandler(noOpErrHandler),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer cA.Close()
	checkClusterFormed(t, srvA, srvB)

	// Revoke the seed server cluster certificate, following servers will not be able to verify connection.
	SetOCSPStatus(t, addr, "configs/certs/ocsp/server-status-request-url-02-cert.pem", ocsp.Revoked)

	// Original set of servers still can communicate to each other, even though the cert has been revoked.
	// NOTE: Should we unplug from the cluster in case our server is revoke and OCSP policy is always or must?
	checkClusterFormed(t, srvA, srvB)

	// Wait for seed server to notice that its certificate has been revoked,
	// so that new routes can't connect to it.
	time.Sleep(6 * time.Second)

	// Start another server against the seed server that has an invalid OCSP Staple
	srvConfC := `
		host: "127.0.0.1"
		port: -1

		server_name: "CCC"

		tls {
			cert_file: "configs/certs/ocsp/server-status-request-url-05-cert.pem"
			key_file: "configs/certs/ocsp/server-status-request-url-05-key.pem"
			ca_file: "configs/certs/ocsp/ca-cert.pem"
			timeout: 5
		}
		store_dir: '%s'
		cluster {
			name: AB
			host: "127.0.0.1"
			advertise: 127.0.0.1
			port: -1
			pool_size: -1

			routes: [ nats://127.0.0.1:%d ]
			connect_retries: 30

			tls {
				cert_file: "configs/certs/ocsp/server-status-request-url-06-cert.pem"
				key_file: "configs/certs/ocsp/server-status-request-url-06-key.pem"
				ca_file: "configs/certs/ocsp/ca-cert.pem"
				timeout: 5
			}
		}
	`
	srvConfC = fmt.Sprintf(srvConfC, storeDirC, optsA.Cluster.Port)
	conf = createConfFile(t, []byte(srvConfC))
	srvC, optsC := RunServerWithConfig(conf)
	defer srvC.Shutdown()

	cB, err := nats.Connect(fmt.Sprintf("tls://localhost:%d", optsB.Port),
		nats.Secure(&tls.Config{
			VerifyConnection: func(s tls.ConnectionState) error {
				if s.OCSPResponse == nil {
					return fmt.Errorf("missing OCSP Staple from server")
				}
				return nil
			},
		}),
		nats.RootCAs(caCert),
		nats.ErrorHandler(noOpErrHandler),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer cB.Close()
	cC, err := nats.Connect(fmt.Sprintf("tls://localhost:%d", optsC.Port),
		nats.Secure(&tls.Config{
			VerifyConnection: func(s tls.ConnectionState) error {
				if s.OCSPResponse == nil {
					return fmt.Errorf("missing OCSP Staple from server")
				}
				return nil
			},
		}),
		nats.RootCAs(caCert),
		nats.ErrorHandler(noOpErrHandler),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer cC.Close()

	// There should be no connectivity between the clients due to the revoked staple.
	_, err = cA.Subscribe("foo", func(m *nats.Msg) {
		m.Respond(nil)
	})
	if err != nil {
		t.Errorf("%v", err)
	}
	cA.Flush()
	_, err = cB.Subscribe("bar", func(m *nats.Msg) {
		m.Respond(nil)
	})
	if err != nil {
		t.Fatal(err)
	}
	cB.Flush()
	resp, err := cC.Request("foo", nil, 2*time.Second)
	if err == nil {
		t.Errorf("Unexpected success, response: %+v", resp)
	}
	resp, err = cC.Request("bar", nil, 2*time.Second)
	if err == nil {
		t.Errorf("Unexpected success, response: %+v", resp)
	}

	// Switch the certs from the seed server to new ones that are not revoked,
	// this should restart OCSP Stapling for the cluster routes.
	srvConfA = `
		host: "127.0.0.1"
		port: -1

		server_name: "AAA"

		tls {
			cert_file: "configs/certs/ocsp/server-status-request-url-07-cert.pem"
			key_file: "configs/certs/ocsp/server-status-request-url-07-key.pem"
			ca_file: "configs/certs/ocsp/ca-cert.pem"
			timeout: 5
		}
		store_dir: '%s'
		cluster {
			port: -1
			pool_size: -1
			compression: "disabled"
			name: AB
			host: "127.0.0.1"
			advertise: 127.0.0.1
			connect_retries: 30

			tls {
				cert_file: "configs/certs/ocsp/server-status-request-url-08-cert.pem"
				key_file: "configs/certs/ocsp/server-status-request-url-08-key.pem"
				ca_file: "configs/certs/ocsp/ca-cert.pem"
				timeout: 5
			}
		}
	`
	srvConfA = fmt.Sprintf(srvConfA, storeDirA)
	if err := os.WriteFile(sconfA, []byte(srvConfA), 0666); err != nil {
		t.Fatalf("Error writing config: %v", err)
	}
	if err := srvA.Reload(); err != nil {
		t.Fatal(err)
	}
	// Wait to get a new OCSP Staple.
	time.Sleep(10 * time.Second)
	checkClusterFormed(t, srvA, srvB, srvC)

	// Now clients connect to C can communicate with B and A.
	_, err = cC.Request("foo", nil, 2*time.Second)
	if err != nil {
		t.Errorf("%v", err)
	}
	_, err = cC.Request("bar", nil, 2*time.Second)
	if err != nil {
		t.Errorf("%v", err)
	}
}

func TestOCSPLeaf(t *testing.T) {
	const (
		caCert = "configs/certs/ocsp/ca-cert.pem"
		caKey  = "configs/certs/ocsp/ca-key.pem"
	)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ocspr := NewOCSPResponder(t, caCert, caKey)
	defer ocspr.Shutdown(ctx)
	addr := fmt.Sprintf("http://%s", ocspr.Addr)
	SetOCSPStatus(t, addr, "configs/certs/ocsp/server-status-request-url-01-cert.pem", ocsp.Good)
	SetOCSPStatus(t, addr, "configs/certs/ocsp/server-status-request-url-02-cert.pem", ocsp.Good)
	SetOCSPStatus(t, addr, "configs/certs/ocsp/server-status-request-url-03-cert.pem", ocsp.Good)
	SetOCSPStatus(t, addr, "configs/certs/ocsp/server-status-request-url-04-cert.pem", ocsp.Good)
	SetOCSPStatus(t, addr, "configs/certs/ocsp/server-status-request-url-05-cert.pem", ocsp.Good)
	SetOCSPStatus(t, addr, "configs/certs/ocsp/server-status-request-url-06-cert.pem", ocsp.Good)
	SetOCSPStatus(t, addr, "configs/certs/ocsp/server-status-request-url-07-cert.pem", ocsp.Good)
	SetOCSPStatus(t, addr, "configs/certs/ocsp/server-status-request-url-08-cert.pem", ocsp.Good)
	SetOCSPStatus(t, addr, "configs/certs/ocsp/client-cert.pem", ocsp.Good)

	// Store Dirs
	storeDirA := t.TempDir()
	storeDirB := t.TempDir()
	storeDirC := t.TempDir()

	// LeafNode server configuration
	srvConfA := `
		host: "127.0.0.1"
		port: -1

		server_name: "AAA"

		tls {
			cert_file: "configs/certs/ocsp/server-status-request-url-01-cert.pem"
			key_file: "configs/certs/ocsp/server-status-request-url-01-key.pem"
			ca_file: "configs/certs/ocsp/ca-cert.pem"
			timeout: 5
		}
		store_dir: '%s'

		leafnodes {
			host: "127.0.0.1"
			port: -1
			advertise: "127.0.0.1"

			tls {
				cert_file: "configs/certs/ocsp/server-status-request-url-02-cert.pem"
				key_file: "configs/certs/ocsp/server-status-request-url-02-key.pem"
				ca_file: "configs/certs/ocsp/ca-cert.pem"
				timeout: 5
				# Leaf connection must present certs.
				verify: true
			}
		}
	`
	srvConfA = fmt.Sprintf(srvConfA, storeDirA)
	sconfA := createConfFile(t, []byte(srvConfA))
	srvA, optsA := RunServerWithConfig(sconfA)
	defer srvA.Shutdown()

	// LeafNode that has the original as a remote and running
	// without OCSP Stapling for the leaf remote.
	srvConfB := `
		host: "127.0.0.1"
		port: -1

		server_name: "BBB"

		tls {
			cert_file: "configs/certs/ocsp/server-status-request-url-03-cert.pem"
			key_file: "configs/certs/ocsp/server-status-request-url-03-key.pem"
			ca_file: "configs/certs/ocsp/ca-cert.pem"
			timeout: 5
		}
		store_dir: '%s'

		leafnodes {
			remotes: [ {
				url: "tls://127.0.0.1:%d"
				tls {
					# Cert without OCSP Stapling enabled is able to connect.
					cert_file: "configs/certs/ocsp/client-cert.pem"
					key_file: "configs/certs/ocsp/client-key.pem"
					ca_file: "configs/certs/ocsp/ca-cert.pem"
					timeout: 5
				}
			} ]
		}
	`
	srvConfB = fmt.Sprintf(srvConfB, storeDirB, optsA.LeafNode.Port)
	conf := createConfFile(t, []byte(srvConfB))
	srvB, optsB := RunServerWithConfig(conf)
	defer srvB.Shutdown()

	// Client connects to server A.
	cA, err := nats.Connect(fmt.Sprintf("tls://127.0.0.1:%d", optsA.Port),
		nats.Secure(&tls.Config{
			VerifyConnection: func(s tls.ConnectionState) error {
				if s.OCSPResponse == nil {
					return fmt.Errorf("missing OCSP Staple from server")
				}
				return nil
			},
		}),
		nats.RootCAs(caCert),
		nats.ErrorHandler(noOpErrHandler),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer cA.Close()
	checkLeafNodeConnected(t, srvA)

	// Revoke the seed server cluster certificate, following servers will not be able to verify connection.
	SetOCSPStatus(t, addr, "configs/certs/ocsp/server-status-request-url-02-cert.pem", ocsp.Revoked)

	// Original set of servers still can communicate to each other via leafnode, even though the staple
	// for the leaf server has been revoked.
	checkLeafNodeConnected(t, srvA)

	// Wait for seed server to notice that its certificate has been revoked.
	time.Sleep(6 * time.Second)

	// Start another server against the seed server that has an revoked OCSP Staple.
	srvConfC := `
		host: "127.0.0.1"
		port: -1

		server_name: "CCC"

		tls {
			cert_file: "configs/certs/ocsp/server-status-request-url-05-cert.pem"
			key_file: "configs/certs/ocsp/server-status-request-url-05-key.pem"
			ca_file: "configs/certs/ocsp/ca-cert.pem"
			timeout: 5
		}
		store_dir: '%s'
		leafnodes {
			remotes: [ {
				url: "tls://127.0.0.1:%d"
				tls {
					cert_file: "configs/certs/ocsp/server-status-request-url-06-cert.pem"
					key_file: "configs/certs/ocsp/server-status-request-url-06-key.pem"
					ca_file: "configs/certs/ocsp/ca-cert.pem"
					timeout: 5
				}
			} ]
		}
	`
	srvConfC = fmt.Sprintf(srvConfC, storeDirC, optsA.LeafNode.Port)
	conf = createConfFile(t, []byte(srvConfC))
	srvC, optsC := RunServerWithConfig(conf)
	defer srvC.Shutdown()

	cB, err := nats.Connect(fmt.Sprintf("tls://127.0.0.1:%d", optsB.Port),
		nats.Secure(&tls.Config{
			VerifyConnection: func(s tls.ConnectionState) error {
				if s.OCSPResponse == nil {
					return fmt.Errorf("missing OCSP Staple from server")
				}
				return nil
			},
		}),
		nats.RootCAs(caCert),
		nats.ErrorHandler(noOpErrHandler),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer cB.Close()
	cC, err := nats.Connect(fmt.Sprintf("tls://127.0.0.1:%d", optsC.Port),
		nats.Secure(&tls.Config{
			VerifyConnection: func(s tls.ConnectionState) error {
				if s.OCSPResponse == nil {
					return fmt.Errorf("missing OCSP Staple from server")
				}
				return nil
			},
		}),
		nats.RootCAs(caCert),
		nats.ErrorHandler(noOpErrHandler),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer cC.Close()

	// There should be connectivity between the clients even if there is a revoked staple
	// from a leafnode connection.
	_, err = cA.Subscribe("foo", func(m *nats.Msg) {
		m.Respond(nil)
	})
	if err != nil {
		t.Errorf("%v", err)
	}
	cA.Flush()
	_, err = cB.Subscribe("bar", func(m *nats.Msg) {
		m.Respond(nil)
	})
	if err != nil {
		t.Fatal(err)
	}
	cB.Flush()

	// Ensure the subscriptions are known by the server we're connected to.
	time.Sleep(100 * time.Millisecond)

	_, err = cC.Request("foo", nil, 2*time.Second)
	if err != nil {
		t.Errorf("Expected success, got: %+v", err)
	}
	_, err = cC.Request("bar", nil, 2*time.Second)
	if err != nil {
		t.Errorf("Expected success, got: %+v", err)
	}

	// Switch the certs from the leafnode server to new ones that are not revoked,
	// this should restart OCSP Stapling for the leafnode server.
	srvConfA = `
		host: "127.0.0.1"
		port: -1

		server_name: "AAA"

		tls {
			cert_file: "configs/certs/ocsp/server-status-request-url-07-cert.pem"
			key_file: "configs/certs/ocsp/server-status-request-url-07-key.pem"
			ca_file: "configs/certs/ocsp/ca-cert.pem"
			timeout: 5
		}
		store_dir: '%s'
		leafnodes {
			host: "127.0.0.1"
			port: -1
			advertise: "127.0.0.1"

			tls {
				cert_file: "configs/certs/ocsp/server-status-request-url-08-cert.pem"
				key_file: "configs/certs/ocsp/server-status-request-url-08-key.pem"
				ca_file: "configs/certs/ocsp/ca-cert.pem"
				timeout: 5
				verify: true
			}
		}
	`
	srvConfA = fmt.Sprintf(srvConfA, storeDirA)
	if err := os.WriteFile(sconfA, []byte(srvConfA), 0666); err != nil {
		t.Fatalf("Error writing config: %v", err)
	}
	if err := srvA.Reload(); err != nil {
		t.Fatal(err)
	}
	time.Sleep(4 * time.Second)

	// A <-> A
	_, err = cA.Request("foo", nil, 2*time.Second)
	if err != nil {
		t.Errorf("%v", err)
	}

	// B <-> A
	_, err = cB.Request("foo", nil, 2*time.Second)
	if err != nil {
		t.Errorf("%v", err)
	}

	// C <-> A
	_, err = cC.Request("foo", nil, 2*time.Second)
	if err != nil {
		t.Errorf("%v", err)
	}
	// C <-> B via leafnode A
	_, err = cC.Request("bar", nil, 2*time.Second)
	if err != nil {
		t.Errorf("%v", err)
	}
}

func TestOCSPLeafNoVerify(t *testing.T) {
	const (
		caCert = "configs/certs/ocsp/ca-cert.pem"
		caKey  = "configs/certs/ocsp/ca-key.pem"
	)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ocspr := NewOCSPResponder(t, caCert, caKey)
	defer ocspr.Shutdown(ctx)
	addr := fmt.Sprintf("http://%s", ocspr.Addr)
	SetOCSPStatus(t, addr, "configs/certs/ocsp/server-status-request-url-01-cert.pem", ocsp.Good)
	SetOCSPStatus(t, addr, "configs/certs/ocsp/server-status-request-url-02-cert.pem", ocsp.Good)
	SetOCSPStatus(t, addr, "configs/certs/ocsp/server-status-request-url-03-cert.pem", ocsp.Good)
	SetOCSPStatus(t, addr, "configs/certs/ocsp/server-status-request-url-04-cert.pem", ocsp.Good)
	SetOCSPStatus(t, addr, "configs/certs/ocsp/server-status-request-url-05-cert.pem", ocsp.Good)
	SetOCSPStatus(t, addr, "configs/certs/ocsp/server-status-request-url-06-cert.pem", ocsp.Good)
	SetOCSPStatus(t, addr, "configs/certs/ocsp/server-status-request-url-07-cert.pem", ocsp.Good)
	SetOCSPStatus(t, addr, "configs/certs/ocsp/server-status-request-url-08-cert.pem", ocsp.Good)
	SetOCSPStatus(t, addr, "configs/certs/ocsp/client-cert.pem", ocsp.Good)

	// Store Dirs
	storeDirA := t.TempDir()
	storeDirB := t.TempDir()
	storeDirC := t.TempDir()

	// LeafNode server configuration
	srvConfA := `
		host: "127.0.0.1"
		port: -1

		server_name: "AAA"

		tls {
			cert_file: "configs/certs/ocsp/server-status-request-url-01-cert.pem"
			key_file: "configs/certs/ocsp/server-status-request-url-01-key.pem"
			ca_file: "configs/certs/ocsp/ca-cert.pem"
			timeout: 5
		}
		store_dir: '%s'

		leafnodes {
			host: "127.0.0.1"
			port: -1
			advertise: "127.0.0.1"
			# for this test, explicitly disable compression because we do it
			# in RunServer but here we do a config reload...
			compression: off

			tls {
				cert_file: "configs/certs/ocsp/server-status-request-url-02-cert.pem"
				key_file: "configs/certs/ocsp/server-status-request-url-02-key.pem"
				ca_file: "configs/certs/ocsp/ca-cert.pem"
				timeout: 5
				# Leaf server does not require certs for clients.
				verify: false
			}
		}
	`
	srvConfA = fmt.Sprintf(srvConfA, storeDirA)
	sconfA := createConfFile(t, []byte(srvConfA))
	srvA, optsA := RunServerWithConfig(sconfA)
	defer srvA.Shutdown()

	// LeafNode remote that will connect to A and will not present certs.
	srvConfB := `
		host: "127.0.0.1"
		port: -1

		server_name: "BBB"

		tls {
			cert_file: "configs/certs/ocsp/server-status-request-url-03-cert.pem"
			key_file: "configs/certs/ocsp/server-status-request-url-03-key.pem"
			ca_file: "configs/certs/ocsp/ca-cert.pem"
			timeout: 5
		}
		store_dir: '%s'

		leafnodes {
			remotes: [ {
				url: "tls://127.0.0.1:%d"
				tls {
					ca_file: "configs/certs/ocsp/ca-cert.pem"
					timeout: 5
				}
			} ]
		}
	`
	srvConfB = fmt.Sprintf(srvConfB, storeDirB, optsA.LeafNode.Port)
	conf := createConfFile(t, []byte(srvConfB))
	srvB, optsB := RunServerWithConfig(conf)
	defer srvB.Shutdown()

	// Client connects to server A.
	cA, err := nats.Connect(fmt.Sprintf("tls://127.0.0.1:%d", optsA.Port),
		nats.Secure(&tls.Config{
			VerifyConnection: func(s tls.ConnectionState) error {
				if s.OCSPResponse == nil {
					return fmt.Errorf("missing OCSP Staple from server")
				}
				return nil
			},
		}),
		nats.RootCAs(caCert),
		nats.ErrorHandler(noOpErrHandler),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer cA.Close()
	checkLeafNodeConnected(t, srvA)

	// Revoke the seed server cluster certificate, following servers will not be able to verify connection.
	SetOCSPStatus(t, addr, "configs/certs/ocsp/server-status-request-url-02-cert.pem", ocsp.Revoked)

	// Original set of servers still can communicate to each other, even though the cert has been revoked.
	checkLeafNodeConnected(t, srvA)

	// Wait for seed server to notice that its certificate has been revoked.
	time.Sleep(6 * time.Second)

	// Start another server against the seed server that has an revoked OCSP Staple.
	srvConfC := `
		host: "127.0.0.1"
		port: -1

		server_name: "CCC"

		tls {
			cert_file: "configs/certs/ocsp/server-status-request-url-05-cert.pem"
			key_file: "configs/certs/ocsp/server-status-request-url-05-key.pem"
			ca_file: "configs/certs/ocsp/ca-cert.pem"
			timeout: 5
		}
		store_dir: '%s'
		leafnodes {
			remotes: [ {
				url: "tls://127.0.0.1:%d"
				tls {
					cert_file: "configs/certs/ocsp/server-status-request-url-06-cert.pem"
					key_file: "configs/certs/ocsp/server-status-request-url-06-key.pem"
					ca_file: "configs/certs/ocsp/ca-cert.pem"
					timeout: 5
					timeout: 5
				}
			} ]
		}
	`
	srvConfC = fmt.Sprintf(srvConfC, storeDirC, optsA.LeafNode.Port)
	conf = createConfFile(t, []byte(srvConfC))
	srvC, optsC := RunServerWithConfig(conf)
	defer srvC.Shutdown()

	cB, err := nats.Connect(fmt.Sprintf("tls://127.0.0.1:%d", optsB.Port),
		nats.Secure(&tls.Config{
			VerifyConnection: func(s tls.ConnectionState) error {
				if s.OCSPResponse == nil {
					return fmt.Errorf("missing OCSP Staple from server")
				}
				return nil
			},
		}),
		nats.RootCAs(caCert),
		nats.ErrorHandler(noOpErrHandler),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer cB.Close()
	cC, err := nats.Connect(fmt.Sprintf("tls://127.0.0.1:%d", optsC.Port),
		nats.Secure(&tls.Config{
			VerifyConnection: func(s tls.ConnectionState) error {
				if s.OCSPResponse == nil {
					return fmt.Errorf("missing OCSP Staple from server")
				}
				return nil
			},
		}),
		nats.RootCAs(caCert),
		nats.ErrorHandler(noOpErrHandler),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer cC.Close()

	// There should be connectivity between the clients even if there is a revoked staple
	// from a leafnode connection.
	_, err = cA.Subscribe("foo", func(m *nats.Msg) {
		m.Respond(nil)
	})
	if err != nil {
		t.Errorf("%v", err)
	}
	cA.Flush()
	_, err = cB.Subscribe("bar", func(m *nats.Msg) {
		m.Respond(nil)
	})
	if err != nil {
		t.Fatal(err)
	}
	cB.Flush()

	// Ensure the subscriptions are known by the server we're connected to.
	time.Sleep(100 * time.Millisecond)

	_, err = cC.Request("foo", nil, 2*time.Second)
	if err != nil {
		t.Errorf("Expected success, got: %+v", err)
	}
	_, err = cC.Request("bar", nil, 2*time.Second)
	if err != nil {
		t.Errorf("Expected success, got: %+v", err)
	}

	// Switch the certs from the leafnode server to new ones that are not revoked,
	// this should restart OCSP Stapling for the leafnode server.
	srvConfA = `
		host: "127.0.0.1"
		port: -1

		server_name: "AAA"

		tls {
			cert_file: "configs/certs/ocsp/server-status-request-url-07-cert.pem"
			key_file: "configs/certs/ocsp/server-status-request-url-07-key.pem"
			ca_file: "configs/certs/ocsp/ca-cert.pem"
			timeout: 5
		}
		store_dir: '%s'
		leafnodes {
			host: "127.0.0.1"
			port: -1
			advertise: "127.0.0.1"
			compression: off

			tls {
				cert_file: "configs/certs/ocsp/server-status-request-url-08-cert.pem"
				key_file: "configs/certs/ocsp/server-status-request-url-08-key.pem"
				ca_file: "configs/certs/ocsp/ca-cert.pem"
				timeout: 5
				verify: true
			}
		}
	`
	srvConfA = fmt.Sprintf(srvConfA, storeDirA)
	if err := os.WriteFile(sconfA, []byte(srvConfA), 0666); err != nil {
		t.Fatalf("Error writing config: %v", err)
	}
	if err := srvA.Reload(); err != nil {
		t.Fatal(err)
	}
	time.Sleep(4 * time.Second)

	// A <-> A
	_, err = cA.Request("foo", nil, 2*time.Second)
	if err != nil {
		t.Errorf("%v", err)
	}

	// B <-> A
	_, err = cB.Request("foo", nil, 2*time.Second)
	if err != nil {
		t.Errorf("%v", err)
	}

	// C <-> A
	_, err = cC.Request("foo", nil, 2*time.Second)
	if err != nil {
		t.Errorf("%v", err)
	}
	// C <-> B via leafnode A
	_, err = cC.Request("bar", nil, 2*time.Second)
	if err != nil {
		t.Errorf("%v", err)
	}
}

func TestOCSPLeafVerifyLeafRemote(t *testing.T) {
	const (
		caCert = "configs/certs/ocsp/ca-cert.pem"
		caKey  = "configs/certs/ocsp/ca-key.pem"
	)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ocspr := NewOCSPResponder(t, caCert, caKey)
	defer ocspr.Shutdown(ctx)
	addr := fmt.Sprintf("http://%s", ocspr.Addr)
	SetOCSPStatus(t, addr, "configs/certs/ocsp/server-status-request-url-01-cert.pem", ocsp.Good)
	SetOCSPStatus(t, addr, "configs/certs/ocsp/server-status-request-url-02-cert.pem", ocsp.Good)
	SetOCSPStatus(t, addr, "configs/certs/ocsp/server-status-request-url-03-cert.pem", ocsp.Good)
	SetOCSPStatus(t, addr, "configs/certs/ocsp/server-status-request-url-04-cert.pem", ocsp.Good)
	SetOCSPStatus(t, addr, "configs/certs/ocsp/server-status-request-url-05-cert.pem", ocsp.Good)
	SetOCSPStatus(t, addr, "configs/certs/ocsp/server-status-request-url-06-cert.pem", ocsp.Good)
	SetOCSPStatus(t, addr, "configs/certs/ocsp/server-status-request-url-07-cert.pem", ocsp.Good)
	SetOCSPStatus(t, addr, "configs/certs/ocsp/server-status-request-url-08-cert.pem", ocsp.Good)
	SetOCSPStatus(t, addr, "configs/certs/ocsp/client-cert.pem", ocsp.Good)

	// Store Dirs
	storeDirA := t.TempDir()
	storeDirB := t.TempDir()

	// LeafNode server configuration
	srvConfA := `
		host: "127.0.0.1"
		port: -1

		server_name: "AAA"

		tls {
			cert_file: "configs/certs/ocsp/server-status-request-url-01-cert.pem"
			key_file: "configs/certs/ocsp/server-status-request-url-01-key.pem"
			ca_file: "configs/certs/ocsp/ca-cert.pem"
			timeout: 5
		}
		store_dir: '%s'

		leafnodes {
			host: "127.0.0.1"
			port: -1
			advertise: "127.0.0.1"

			tls {
				cert_file: "configs/certs/ocsp/server-status-request-url-02-cert.pem"
				key_file: "configs/certs/ocsp/server-status-request-url-02-key.pem"
				ca_file: "configs/certs/ocsp/ca-cert.pem"
				timeout: 5
				verify: true
			}
		}
	`
	srvConfA = fmt.Sprintf(srvConfA, storeDirA)
	sconfA := createConfFile(t, []byte(srvConfA))
	srvA, optsA := RunServerWithConfig(sconfA)
	defer srvA.Shutdown()

	// LeafNode remote that will connect to A and will not present certs.
	srvConfB := `
		host: "127.0.0.1"
		port: -1

		server_name: "BBB"

		tls {
			cert_file: "configs/certs/ocsp/server-status-request-url-03-cert.pem"
			key_file: "configs/certs/ocsp/server-status-request-url-03-key.pem"
			ca_file: "configs/certs/ocsp/ca-cert.pem"
			timeout: 5
		}
		store_dir: '%s'

		leafnodes {
			remotes: [ {
				url: "tls://127.0.0.1:%d"
				tls {
					ca_file: "configs/certs/ocsp/ca-cert.pem"
					timeout: 5
				}
			} ]
		}
	`
	srvConfB = fmt.Sprintf(srvConfB, storeDirB, optsA.LeafNode.Port)
	conf := createConfFile(t, []byte(srvConfB))
	srvB, _ := RunServerWithConfig(conf)
	defer srvB.Shutdown()

	// Client connects to server A.
	cA, err := nats.Connect(fmt.Sprintf("tls://127.0.0.1:%d", optsA.Port),
		nats.Secure(&tls.Config{
			VerifyConnection: func(s tls.ConnectionState) error {
				if s.OCSPResponse == nil {
					return fmt.Errorf("missing OCSP Staple from server")
				}
				return nil
			},
		}),
		nats.RootCAs(caCert),
		nats.ErrorHandler(noOpErrHandler),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer cA.Close()

	// Should not have been able to connect.
	checkLeafNodeConnections(t, srvA, 0)
}

func TestOCSPLeafVerifyAndMapLeafRemote(t *testing.T) {
	const (
		caCert = "configs/certs/ocsp/ca-cert.pem"
		caKey  = "configs/certs/ocsp/ca-key.pem"
	)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ocspr := NewOCSPResponder(t, caCert, caKey)
	defer ocspr.Shutdown(ctx)
	addr := fmt.Sprintf("http://%s", ocspr.Addr)
	SetOCSPStatus(t, addr, "configs/certs/ocsp/server-status-request-url-01-cert.pem", ocsp.Good)
	SetOCSPStatus(t, addr, "configs/certs/ocsp/server-status-request-url-02-cert.pem", ocsp.Good)
	SetOCSPStatus(t, addr, "configs/certs/ocsp/server-status-request-url-03-cert.pem", ocsp.Good)
	SetOCSPStatus(t, addr, "configs/certs/ocsp/server-status-request-url-04-cert.pem", ocsp.Good)
	SetOCSPStatus(t, addr, "configs/certs/ocsp/server-status-request-url-05-cert.pem", ocsp.Good)
	SetOCSPStatus(t, addr, "configs/certs/ocsp/server-status-request-url-06-cert.pem", ocsp.Good)
	SetOCSPStatus(t, addr, "configs/certs/ocsp/server-status-request-url-07-cert.pem", ocsp.Good)
	SetOCSPStatus(t, addr, "configs/certs/ocsp/server-status-request-url-08-cert.pem", ocsp.Good)
	SetOCSPStatus(t, addr, "configs/certs/ocsp/client-cert.pem", ocsp.Good)

	// Store Dirs
	storeDirA := t.TempDir()
	storeDirB := t.TempDir()

	// LeafNode server configuration
	srvConfA := `
		host: "127.0.0.1"
		port: -1

		server_name: "AAA"

		tls {
			cert_file: "configs/certs/ocsp/server-status-request-url-01-cert.pem"
			key_file: "configs/certs/ocsp/server-status-request-url-01-key.pem"
			ca_file: "configs/certs/ocsp/ca-cert.pem"
			timeout: 5
			verify_and_map: true
		}
		store_dir: '%s'

		leafnodes {
			host: "127.0.0.1"
			port: -1
			advertise: "127.0.0.1"

			tls {
				cert_file: "configs/certs/ocsp/server-status-request-url-02-cert.pem"
				key_file: "configs/certs/ocsp/server-status-request-url-02-key.pem"
				ca_file: "configs/certs/ocsp/ca-cert.pem"
				timeout: 5
				verify_and_map: true
			}
		}

		accounts: {
			leaf: {
			  users: [ {user: "C=US, ST=CA, L=San Francisco, O=Synadia, OU=nats.io, CN=localhost server-status-request-url-04"} ]
			}
			client: {
			  users: [ {user: "C=US, ST=CA, L=San Francisco, O=Synadia, OU=nats.io, CN=localhost client"} ]
			}
		}

	`
	srvConfA = fmt.Sprintf(srvConfA, storeDirA)
	sconfA := createConfFile(t, []byte(srvConfA))
	srvA, optsA := RunServerWithConfig(sconfA)
	defer srvA.Shutdown()

	// LeafNode remote that will connect to A and will not present certs.
	srvConfB := `
		host: "127.0.0.1"
		port: -1

		server_name: "BBB"

		tls {
			cert_file: "configs/certs/ocsp/server-status-request-url-03-cert.pem"
			key_file: "configs/certs/ocsp/server-status-request-url-03-key.pem"
			ca_file: "configs/certs/ocsp/ca-cert.pem"
			timeout: 5
		}
		store_dir: '%s'

		leafnodes {
			remotes: [ {
				url: "tls://127.0.0.1:%d"
				tls {
					cert_file: "configs/certs/ocsp/server-status-request-url-04-cert.pem"
					key_file: "configs/certs/ocsp/server-status-request-url-04-key.pem"
					ca_file: "configs/certs/ocsp/ca-cert.pem"
					timeout: 5
				}
			} ]
		}
	`
	srvConfB = fmt.Sprintf(srvConfB, storeDirB, optsA.LeafNode.Port)
	conf := createConfFile(t, []byte(srvConfB))
	srvB, _ := RunServerWithConfig(conf)
	defer srvB.Shutdown()

	// Client connects to server A.
	cA, err := nats.Connect(fmt.Sprintf("tls://127.0.0.1:%d", optsA.Port),
		nats.Secure(&tls.Config{
			VerifyConnection: func(s tls.ConnectionState) error {
				if s.OCSPResponse == nil {
					return fmt.Errorf("missing OCSP Staple from server")
				}
				return nil
			},
		}),
		nats.ClientCert("./configs/certs/ocsp/client-cert.pem", "./configs/certs/ocsp/client-key.pem"),
		nats.RootCAs(caCert),
		nats.ErrorHandler(noOpErrHandler),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer cA.Close()
	checkLeafNodeConnections(t, srvA, 1)
}

func TestOCSPGateway(t *testing.T) {
	const (
		caCert = "configs/certs/ocsp/ca-cert.pem"
		caKey  = "configs/certs/ocsp/ca-key.pem"
	)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ocspr := NewOCSPResponder(t, caCert, caKey)
	defer ocspr.Shutdown(ctx)
	addr := fmt.Sprintf("http://%s", ocspr.Addr)
	SetOCSPStatus(t, addr, "configs/certs/ocsp/server-status-request-url-01-cert.pem", ocsp.Good)
	SetOCSPStatus(t, addr, "configs/certs/ocsp/server-status-request-url-02-cert.pem", ocsp.Good)
	SetOCSPStatus(t, addr, "configs/certs/ocsp/server-status-request-url-03-cert.pem", ocsp.Good)
	SetOCSPStatus(t, addr, "configs/certs/ocsp/server-status-request-url-04-cert.pem", ocsp.Good)
	SetOCSPStatus(t, addr, "configs/certs/ocsp/server-status-request-url-05-cert.pem", ocsp.Good)
	SetOCSPStatus(t, addr, "configs/certs/ocsp/server-status-request-url-06-cert.pem", ocsp.Good)
	SetOCSPStatus(t, addr, "configs/certs/ocsp/server-status-request-url-07-cert.pem", ocsp.Good)
	SetOCSPStatus(t, addr, "configs/certs/ocsp/server-status-request-url-08-cert.pem", ocsp.Good)

	// Store Dirs
	storeDirA := t.TempDir()
	storeDirB := t.TempDir()
	storeDirC := t.TempDir()

	// Gateway server configuration
	srvConfA := `
		host: "127.0.0.1"
		port: -1

		server_name: "AAA"

		tls {
			cert_file: "configs/certs/ocsp/server-status-request-url-01-cert.pem"
			key_file: "configs/certs/ocsp/server-status-request-url-01-key.pem"
			ca_file: "configs/certs/ocsp/ca-cert.pem"
			timeout: 5
		}
		store_dir: '%s'
		gateway {
			name: A
			host: "127.0.0.1"
			port: -1
			advertise: "127.0.0.1"

			tls {
				cert_file: "configs/certs/ocsp/server-status-request-url-02-cert.pem"
				key_file: "configs/certs/ocsp/server-status-request-url-02-key.pem"
				ca_file: "configs/certs/ocsp/ca-cert.pem"
				timeout: 5
			}
		}
	`
	srvConfA = fmt.Sprintf(srvConfA, storeDirA)
	sconfA := createConfFile(t, []byte(srvConfA))
	srvA, optsA := RunServerWithConfig(sconfA)
	defer srvA.Shutdown()

	// LeafNode that has the original as a remote.
	srvConfB := `
		host: "127.0.0.1"
		port: -1

		server_name: "BBB"

		tls {
			cert_file: "configs/certs/ocsp/server-status-request-url-03-cert.pem"
			key_file: "configs/certs/ocsp/server-status-request-url-03-key.pem"
			ca_file: "configs/certs/ocsp/ca-cert.pem"
			timeout: 5
		}
		store_dir: '%s'
		gateway {
			name: B
			host: "127.0.0.1"
			advertise: "127.0.0.1"
			port: -1
			gateways: [{
				name: "A"
				url: "nats://127.0.0.1:%d"
				tls {
					cert_file: "configs/certs/ocsp/server-status-request-url-04-cert.pem"
					key_file: "configs/certs/ocsp/server-status-request-url-04-key.pem"
					ca_file: "configs/certs/ocsp/ca-cert.pem"
					timeout: 5
				}
			}]
			tls {
				cert_file: "configs/certs/ocsp/server-status-request-url-04-cert.pem"
				key_file: "configs/certs/ocsp/server-status-request-url-04-key.pem"
				ca_file: "configs/certs/ocsp/ca-cert.pem"
				timeout: 5
			}
		}
	`
	srvConfB = fmt.Sprintf(srvConfB, storeDirB, optsA.Gateway.Port)
	conf := createConfFile(t, []byte(srvConfB))
	srvB, optsB := RunServerWithConfig(conf)
	defer srvB.Shutdown()

	// Client connects to server A.
	cA, err := nats.Connect(fmt.Sprintf("tls://127.0.0.1:%d", optsA.Port),
		nats.Secure(&tls.Config{
			VerifyConnection: func(s tls.ConnectionState) error {
				if s.OCSPResponse == nil {
					return fmt.Errorf("missing OCSP Staple from server")
				}
				return nil
			},
		}),
		nats.RootCAs(caCert),
		nats.ErrorHandler(noOpErrHandler),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer cA.Close()
	waitForOutboundGateways(t, srvB, 1, 5*time.Second)

	// Revoke the seed server cluster certificate, following servers will not be able to verify connection.
	SetOCSPStatus(t, addr, "configs/certs/ocsp/server-status-request-url-02-cert.pem", ocsp.Revoked)

	// Original set of servers still can communicate to each other, even though the cert has been revoked.
	waitForOutboundGateways(t, srvA, 1, 5*time.Second)
	waitForOutboundGateways(t, srvB, 1, 5*time.Second)

	// Wait for gateway A to notice that its certificate has been revoked,
	// so that new gateways can't connect to it.
	time.Sleep(6 * time.Second)

	// Start another server against the seed server that has an invalid OCSP Staple
	srvConfC := `
		host: "127.0.0.1"
		port: -1

		server_name: "CCC"

		tls {
			cert_file: "configs/certs/ocsp/server-status-request-url-05-cert.pem"
			key_file: "configs/certs/ocsp/server-status-request-url-05-key.pem"
			ca_file: "configs/certs/ocsp/ca-cert.pem"
			timeout: 5
		}
		store_dir: '%s'
		gateway {
			name: C
			host: "127.0.0.1"
			advertise: "127.0.0.1"
			port: -1
			gateways: [{name: "A", url: "nats://127.0.0.1:%d" }]
			tls {
				cert_file: "configs/certs/ocsp/server-status-request-url-06-cert.pem"
				key_file: "configs/certs/ocsp/server-status-request-url-06-key.pem"
				ca_file: "configs/certs/ocsp/ca-cert.pem"
				timeout: 5
			}
		}
	`
	srvConfC = fmt.Sprintf(srvConfC, storeDirC, optsA.Gateway.Port)
	conf = createConfFile(t, []byte(srvConfC))
	srvC, optsC := RunServerWithConfig(conf)
	defer srvC.Shutdown()

	cB, err := nats.Connect(fmt.Sprintf("tls://127.0.0.1:%d", optsB.Port),
		nats.Secure(&tls.Config{
			VerifyConnection: func(s tls.ConnectionState) error {
				if s.OCSPResponse == nil {
					return fmt.Errorf("missing OCSP Staple from server")
				}
				return nil
			},
		}),
		nats.RootCAs(caCert),
		nats.ErrorHandler(noOpErrHandler),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer cB.Close()
	cC, err := nats.Connect(fmt.Sprintf("tls://127.0.0.1:%d", optsC.Port),
		nats.Secure(&tls.Config{
			VerifyConnection: func(s tls.ConnectionState) error {
				if s.OCSPResponse == nil {
					return fmt.Errorf("missing OCSP Staple from server")
				}
				return nil
			},
		}),
		nats.RootCAs(caCert),
		nats.ErrorHandler(noOpErrHandler),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer cC.Close()

	// There should be no connectivity between the clients due to the revoked staple.
	_, err = cA.Subscribe("foo", func(m *nats.Msg) {
		m.Respond(nil)
	})
	if err != nil {
		t.Errorf("%v", err)
	}
	cA.Flush()
	_, err = cB.Subscribe("bar", func(m *nats.Msg) {
		m.Respond(nil)
	})
	if err != nil {
		t.Fatal(err)
	}
	cB.Flush()

	// Gateway C was not able to mesh with Gateway A because of the revoked OCSP staple
	// so these requests to A and B should fail.
	resp, err := cC.Request("foo", nil, 2*time.Second)
	if err == nil {
		t.Errorf("Unexpected success, response: %+v", resp)
	}
	// Make request to B
	resp, err = cC.Request("bar", nil, 2*time.Second)
	if err == nil {
		t.Errorf("Unexpected success, response: %+v", resp)
	}

	// Switch the certs from the seed server to new ones that are not revoked,
	// this should restart OCSP Stapling for the cluster routes.
	srvConfA = `
		host: "127.0.0.1"
		port: -1

		server_name: "AAA"

		tls {
			cert_file: "configs/certs/ocsp/server-status-request-url-07-cert.pem"
			key_file: "configs/certs/ocsp/server-status-request-url-07-key.pem"
			ca_file: "configs/certs/ocsp/ca-cert.pem"
			timeout: 5
		}
		store_dir: '%s'
		gateway {
			name: A
			host: "127.0.0.1"
			port: -1
			advertise: "127.0.0.1"

			tls {
				cert_file: "configs/certs/ocsp/server-status-request-url-08-cert.pem"
				key_file: "configs/certs/ocsp/server-status-request-url-08-key.pem"
				ca_file: "configs/certs/ocsp/ca-cert.pem"
				timeout: 5
			}
		}
	`

	srvConfA = fmt.Sprintf(srvConfA, storeDirA)
	if err := os.WriteFile(sconfA, []byte(srvConfA), 0666); err != nil {
		t.Fatalf("Error writing config: %v", err)
	}
	if err := srvA.Reload(); err != nil {
		t.Fatal(err)
	}
	time.Sleep(4 * time.Second)
	waitForOutboundGateways(t, srvA, 2, 5*time.Second)
	waitForOutboundGateways(t, srvB, 2, 5*time.Second)
	waitForOutboundGateways(t, srvC, 2, 5*time.Second)

	// Now clients connect to C can communicate with B and A.
	_, err = cC.Request("foo", nil, 2*time.Second)
	if err != nil {
		t.Errorf("%v", err)
	}
	_, err = cC.Request("bar", nil, 2*time.Second)
	if err != nil {
		t.Errorf("%v", err)
	}
}

func TestOCSPGatewayIntermediate(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	intermediateCA1Responder := NewOCSPResponderIntermediateCA1(t)
	intermediateCA1ResponderURL := fmt.Sprintf("http://%s", intermediateCA1Responder.Addr)
	defer intermediateCA1Responder.Shutdown(ctx)
	SetOCSPStatus(t, intermediateCA1ResponderURL, "configs/certs/ocsp_peer/mini-ca/server1/TestServer1_cert.pem", ocsp.Good)
	SetOCSPStatus(t, intermediateCA1ResponderURL, "configs/certs/ocsp_peer/mini-ca/server1/TestServer2_cert.pem", ocsp.Good)

	// Gateway server configuration
	srvConfA := `
		host: "127.0.0.1"
		port: -1

		server_name: "AAA"

		ocsp: {
			mode: always
			url: %s
		}

		gateway {
			name: A
			host: "127.0.0.1"
			port: -1
			advertise: "127.0.0.1"

			tls {
				cert_file: "configs/certs/ocsp_peer/mini-ca/server1/TestServer1_bundle.pem"
				key_file: "configs/certs/ocsp_peer/mini-ca/server1/private/TestServer1_keypair.pem"
				ca_file: "configs/certs/ocsp_peer/mini-ca/root/root_cert.pem"
				timeout: 5
			}
		}
	`
	srvConfA = fmt.Sprintf(srvConfA, intermediateCA1ResponderURL)
	sconfA := createConfFile(t, []byte(srvConfA))
	srvA, optsA := RunServerWithConfig(sconfA)
	defer srvA.Shutdown()

	srvConfB := `
		host: "127.0.0.1"
		port: -1

		server_name: "BBB"

		ocsp: {
			mode: always
			url: %s
		}

		gateway {
			name: B
			host: "127.0.0.1"
			advertise: "127.0.0.1"
			port: -1
			gateways: [{
				name: "A"
				url: "nats://127.0.0.1:%d"
			}]
			tls {
				cert_file: "configs/certs/ocsp_peer/mini-ca/server1/TestServer2_bundle.pem"
				key_file: "configs/certs/ocsp_peer/mini-ca/server1/private/TestServer2_keypair.pem"
				ca_file: "configs/certs/ocsp_peer/mini-ca/root/root_cert.pem"
				timeout: 5
			}
		}
	`
	srvConfB = fmt.Sprintf(srvConfB, intermediateCA1ResponderURL, optsA.Gateway.Port)
	conf := createConfFile(t, []byte(srvConfB))
	srvB, optsB := RunServerWithConfig(conf)
	defer srvB.Shutdown()

	// Client connects to server A.
	cA, err := nats.Connect(fmt.Sprintf("nats://127.0.0.1:%d", optsA.Port),
		nats.ErrorHandler(noOpErrHandler),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer cA.Close()
	waitForOutboundGateways(t, srvB, 1, 5*time.Second)

	cB, err := nats.Connect(fmt.Sprintf("nats://127.0.0.1:%d", optsB.Port),
		nats.ErrorHandler(noOpErrHandler),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer cB.Close()
}

func TestOCSPGatewayReload(t *testing.T) {
	const (
		caCert = "configs/certs/ocsp/ca-cert.pem"
		caKey  = "configs/certs/ocsp/ca-key.pem"
	)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ocspr := NewOCSPResponder(t, caCert, caKey)
	defer ocspr.Shutdown(ctx)
	addr := fmt.Sprintf("http://%s", ocspr.Addr)

	// Node A
	SetOCSPStatus(t, addr, "configs/certs/ocsp/server-status-request-url-01-cert.pem", ocsp.Good)
	SetOCSPStatus(t, addr, "configs/certs/ocsp/server-status-request-url-02-cert.pem", ocsp.Good)

	// Node B
	SetOCSPStatus(t, addr, "configs/certs/ocsp/server-status-request-url-03-cert.pem", ocsp.Good)
	SetOCSPStatus(t, addr, "configs/certs/ocsp/server-status-request-url-04-cert.pem", ocsp.Good)

	// Node C
	SetOCSPStatus(t, addr, "configs/certs/ocsp/server-status-request-url-05-cert.pem", ocsp.Good)
	SetOCSPStatus(t, addr, "configs/certs/ocsp/server-status-request-url-06-cert.pem", ocsp.Good)

	// Node A rotated certs
	SetOCSPStatus(t, addr, "configs/certs/ocsp/server-status-request-url-07-cert.pem", ocsp.Good)
	SetOCSPStatus(t, addr, "configs/certs/ocsp/server-status-request-url-08-cert.pem", ocsp.Good)

	// Store Dirs
	storeDirA := t.TempDir()
	storeDirB := t.TempDir()
	storeDirC := t.TempDir()

	// Gateway server configuration
	srvConfA := `
		host: "127.0.0.1"
		port: -1

		server_name: "AAA"

		ocsp { mode = always }

		store_dir: '%s'
		gateway {
			name: A
			host: "127.0.0.1"
			port: -1
			advertise: "127.0.0.1"

			tls {
				cert_file: "configs/certs/ocsp/server-status-request-url-02-cert.pem"
				key_file: "configs/certs/ocsp/server-status-request-url-02-key.pem"
				ca_file: "configs/certs/ocsp/ca-cert.pem"
				timeout: 5
			}
		}
	`
	srvConfA = fmt.Sprintf(srvConfA, storeDirA)
	sconfA := createConfFile(t, []byte(srvConfA))
	srvA, optsA := RunServerWithConfig(sconfA)
	defer srvA.Shutdown()

	// Gateway B connects to Gateway A.
	srvConfB := `
		host: "127.0.0.1"
		port: -1

		server_name: "BBB"

		ocsp { mode = always }

		store_dir: '%s'
		gateway {
			name: B
			host: "127.0.0.1"
			advertise: "127.0.0.1"
			port: -1
			gateways: [{
				name: "A"
				url: "nats://127.0.0.1:%d"
			}]
			tls {
				cert_file: "configs/certs/ocsp/server-status-request-url-04-cert.pem"
				key_file: "configs/certs/ocsp/server-status-request-url-04-key.pem"
				ca_file: "configs/certs/ocsp/ca-cert.pem"
				timeout: 5
			}
		}
	`
	srvConfB = fmt.Sprintf(srvConfB, storeDirB, optsA.Gateway.Port)
	conf := createConfFile(t, []byte(srvConfB))
	srvB, optsB := RunServerWithConfig(conf)
	defer srvB.Shutdown()

	// Client connects to server A.
	cA, err := nats.Connect(fmt.Sprintf("nats://127.0.0.1:%d", optsA.Port),
		nats.ErrorHandler(noOpErrHandler),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer cA.Close()

	// Wait for connectivity between A and B.
	waitForOutboundGateways(t, srvB, 1, 5*time.Second)

	// Gateway C also connects to Gateway A.
	srvConfC := `
		host: "127.0.0.1"
		port: -1

		server_name: "CCC"

		ocsp { mode = always }

		store_dir: '%s'
		gateway {
			name: C
			host: "127.0.0.1"
			advertise: "127.0.0.1"
			port: -1
			gateways: [{name: "A", url: "nats://127.0.0.1:%d" }]
			tls {
				cert_file: "configs/certs/ocsp/server-status-request-url-06-cert.pem"
				key_file: "configs/certs/ocsp/server-status-request-url-06-key.pem"
				ca_file: "configs/certs/ocsp/ca-cert.pem"
				timeout: 5
			}
		}
	`
	srvConfC = fmt.Sprintf(srvConfC, storeDirC, optsA.Gateway.Port)
	conf = createConfFile(t, []byte(srvConfC))
	srvC, optsC := RunServerWithConfig(conf)
	defer srvC.Shutdown()

	////////////////////////////////////////////////////////////////////////////
	//                                                                        //
	//  A and B are connected at this point and C is starting with certs that //
	//  will be rotated, in v2.10.8 on reload now all OCSP monitors are also  //
	//  always restarted.                                                     //
	//                                                                        //
	////////////////////////////////////////////////////////////////////////////
	cB, err := nats.Connect(fmt.Sprintf("nats://127.0.0.1:%d", optsB.Port),
		nats.ErrorHandler(noOpErrHandler),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer cB.Close()
	cC, err := nats.Connect(fmt.Sprintf("nats://127.0.0.1:%d", optsC.Port),
		nats.ErrorHandler(noOpErrHandler),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer cC.Close()

	_, err = cA.Subscribe("foo", func(m *nats.Msg) {
		m.Respond(nil)
	})
	if err != nil {
		t.Errorf("%v", err)
	}
	cA.Flush()
	_, err = cB.Subscribe("bar", func(m *nats.Msg) {
		m.Respond(nil)
	})
	if err != nil {
		t.Fatal(err)
	}
	cB.Flush()

	/////////////////////////////////////////////////////////////////////////////////
	//                                                                             //
	//  Switch all the certs from server A, all OCSP monitors should be restarted  //
	//  so it should have new staples.                                             //
	//                                                                             //
	/////////////////////////////////////////////////////////////////////////////////
	srvConfA = `
		host: "127.0.0.1"
		port: -1

		server_name: "AAA"

		ocsp { mode = always }

		store_dir: '%s'
		gateway {
			name: A
			host: "127.0.0.1"
			port: -1
			advertise: "127.0.0.1"

			tls {
				cert_file: "configs/certs/ocsp/server-status-request-url-08-cert.pem"
				key_file: "configs/certs/ocsp/server-status-request-url-08-key.pem"
				ca_file: "configs/certs/ocsp/ca-cert.pem"
				timeout: 5
			}
		}
	`

	srvConfA = fmt.Sprintf(srvConfA, storeDirA)
	if err := os.WriteFile(sconfA, []byte(srvConfA), 0666); err != nil {
		t.Fatalf("Error writing config: %v", err)
	}
	if err := srvA.Reload(); err != nil {
		t.Fatal(err)
	}
	waitForOutboundGateways(t, srvA, 2, 5*time.Second)
	waitForOutboundGateways(t, srvB, 2, 5*time.Second)
	waitForOutboundGateways(t, srvC, 2, 5*time.Second)

	// Now clients connect to C can communicate with B and A.
	_, err = cC.Request("foo", nil, 2*time.Second)
	if err != nil {
		t.Errorf("%v", err)
	}
	_, err = cC.Request("bar", nil, 2*time.Second)
	if err != nil {
		t.Errorf("%v", err)
	}
}

func TestOCSPCustomConfig(t *testing.T) {
	const (
		caCert     = "configs/certs/ocsp/ca-cert.pem"
		caKey      = "configs/certs/ocsp/ca-key.pem"
		serverCert = "configs/certs/ocsp/server-cert.pem"
		serverKey  = "configs/certs/ocsp/server-key.pem"
	)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ocspr := NewOCSPResponder(t, caCert, caKey)
	ocspURL := fmt.Sprintf("http://%s", ocspr.Addr)
	defer ocspr.Shutdown(ctx)

	var (
		errExpectedNoStaple = fmt.Errorf("expected no staple")
		errMissingStaple    = fmt.Errorf("missing OCSP Staple from server")
	)

	for _, test := range []struct {
		name      string
		config    string
		opts      []nats.Option
		err       error
		rerr      error
		configure func()
	}{
		{
			"OCSP Stapling in auto mode makes server fail to boot if status is revoked",
			`
				port: -1

				ocsp {
					mode: auto
				}

				tls {
					cert_file: "configs/certs/ocsp/server-cert.pem"
					key_file: "configs/certs/ocsp/server-key.pem"
					ca_file: "configs/certs/ocsp/ca-cert.pem"
					timeout: 5
				}
			`,
			[]nats.Option{
				nats.Secure(&tls.Config{
					VerifyConnection: func(s tls.ConnectionState) error {
						if s.OCSPResponse != nil {
							return errExpectedNoStaple
						}
						return nil
					},
				}),
				nats.ClientCert("./configs/certs/ocsp/client-cert.pem", "./configs/certs/ocsp/client-key.pem"),
				nats.RootCAs(caCert),
				nats.ErrorHandler(noOpErrHandler),
			},
			nil,
			nil,
			func() { SetOCSPStatus(t, ocspURL, serverCert, ocsp.Revoked) },
		},
		{
			"OCSP Stapling must staple ignored if disabled with ocsp: false",
			`
				port: -1

				ocsp: false

				tls {
					cert_file: "configs/certs/ocsp/server-status-request-url-01-cert.pem"
					key_file: "configs/certs/ocsp/server-status-request-url-01-key.pem"
					ca_file: "configs/certs/ocsp/ca-cert.pem"
					timeout: 5
				}
			`,
			[]nats.Option{
				nats.Secure(&tls.Config{
					VerifyConnection: func(s tls.ConnectionState) error {
						if s.OCSPResponse != nil {
							return errExpectedNoStaple
						}
						return nil
					},
				}),
				nats.ClientCert("./configs/certs/ocsp/client-cert.pem", "./configs/certs/ocsp/client-key.pem"),
				nats.RootCAs(caCert),
				nats.ErrorHandler(noOpErrHandler),
			},
			nil,
			nil,
			func() {
				SetOCSPStatus(t, ocspURL, "configs/certs/ocsp/server-status-request-url-01-cert.pem", ocsp.Good)
			},
		},
		{
			"OCSP Stapling must staple ignored if disabled with ocsp mode never",
			`
				port: -1

				ocsp: { mode: never }

				tls {
					cert_file: "configs/certs/ocsp/server-status-request-url-01-cert.pem"
					key_file: "configs/certs/ocsp/server-status-request-url-01-key.pem"
					ca_file: "configs/certs/ocsp/ca-cert.pem"
					timeout: 5
				}
			`,
			[]nats.Option{
				nats.Secure(&tls.Config{
					VerifyConnection: func(s tls.ConnectionState) error {
						if s.OCSPResponse != nil {
							return errExpectedNoStaple
						}
						return nil
					},
				}),
				nats.ClientCert("./configs/certs/ocsp/client-cert.pem", "./configs/certs/ocsp/client-key.pem"),
				nats.RootCAs(caCert),
				nats.ErrorHandler(noOpErrHandler),
			},
			nil,
			nil,
			func() {
				SetOCSPStatus(t, ocspURL, "configs/certs/ocsp/server-status-request-url-01-cert.pem", ocsp.Good)
			},
		},
		{
			"OCSP Stapling in always mode fetches a staple even if cert does not have one",
			`
				port: -1

				ocsp {
					mode: always
					url: "http://127.0.0.1:8888"
				}

				tls {
					cert_file: "configs/certs/ocsp/server-cert.pem"
					key_file: "configs/certs/ocsp/server-key.pem"
					ca_file: "configs/certs/ocsp/ca-cert.pem"
					timeout: 5
				}
			`,
			[]nats.Option{
				nats.Secure(&tls.Config{
					VerifyConnection: func(s tls.ConnectionState) error {
						if s.OCSPResponse == nil {
							return errMissingStaple
						}
						return nil
					},
				}),
				nats.ClientCert("./configs/certs/ocsp/client-cert.pem", "./configs/certs/ocsp/client-key.pem"),
				nats.RootCAs(caCert),
				nats.ErrorHandler(noOpErrHandler),
			},
			nil,
			nil,
			func() { SetOCSPStatus(t, ocspURL, serverCert, ocsp.Good) },
		},
		{
			"OCSP Stapling in must staple mode does not fetch staple if there is no must staple flag",
			`
				port: -1

				ocsp {
					mode: must
					url: "http://127.0.0.1:8888"
				}

				tls {
					cert_file: "configs/certs/ocsp/server-cert.pem"
					key_file: "configs/certs/ocsp/server-key.pem"
					ca_file: "configs/certs/ocsp/ca-cert.pem"
					timeout: 5
				}
			`,
			[]nats.Option{
				nats.Secure(&tls.Config{
					VerifyConnection: func(s tls.ConnectionState) error {
						if s.OCSPResponse != nil {
							return errExpectedNoStaple
						}
						return nil
					},
				}),
				nats.ClientCert("./configs/certs/ocsp/client-cert.pem", "./configs/certs/ocsp/client-key.pem"),
				nats.RootCAs(caCert),
				nats.ErrorHandler(noOpErrHandler),
			},
			nil,
			nil,
			func() { SetOCSPStatus(t, ocspURL, serverCert, ocsp.Good) },
		},
		{
			"OCSP Stapling in must staple mode fetches staple if there is a must staple flag",
			`
				port: -1

				ocsp {
					mode: must
					url: "http://127.0.0.1:8888"
				}

				tls {
					cert_file: "configs/certs/ocsp/server-status-request-url-01-cert.pem"
					key_file: "configs/certs/ocsp/server-status-request-url-01-key.pem"
					ca_file: "configs/certs/ocsp/ca-cert.pem"
					timeout: 5
				}
			`,
			[]nats.Option{
				nats.Secure(&tls.Config{
					VerifyConnection: func(s tls.ConnectionState) error {
						if s.OCSPResponse == nil {
							return errMissingStaple
						}
						return nil
					},
				}),
				nats.ClientCert("./configs/certs/ocsp/client-cert.pem", "./configs/certs/ocsp/client-key.pem"),
				nats.RootCAs(caCert),
				nats.ErrorHandler(noOpErrHandler),
			},
			nil,
			nil,
			func() {
				SetOCSPStatus(t, ocspURL, "configs/certs/ocsp/server-status-request-url-01-cert.pem", ocsp.Good)
			},
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

func TestOCSPCustomConfigReloadDisable(t *testing.T) {
	const (
		caCert            = "configs/certs/ocsp/ca-cert.pem"
		caKey             = "configs/certs/ocsp/ca-key.pem"
		serverCert        = "configs/certs/ocsp/server-cert.pem"
		updatedServerCert = "configs/certs/ocsp/server-status-request-url-01-cert.pem"
	)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ocspr := NewOCSPResponder(t, caCert, caKey)
	defer ocspr.Shutdown(ctx)
	addr := fmt.Sprintf("http://%s", ocspr.Addr)
	SetOCSPStatus(t, addr, serverCert, ocsp.Good)
	SetOCSPStatus(t, addr, updatedServerCert, ocsp.Good)

	// Start with server without OCSP Stapling MustStaple
	content := `
		port: -1

		ocsp: { mode: always, url: "http://127.0.0.1:8888" }

		tls {
			cert_file: "configs/certs/ocsp/server-cert.pem"
			key_file: "configs/certs/ocsp/server-key.pem"
			ca_file: "configs/certs/ocsp/ca-cert.pem"
			timeout: 5
		}
	`
	conf := createConfFile(t, []byte(content))
	s, opts := RunServerWithConfig(conf)
	defer s.Shutdown()

	nc, err := nats.Connect(fmt.Sprintf("tls://localhost:%d", opts.Port),
		nats.Secure(&tls.Config{
			VerifyConnection: func(s tls.ConnectionState) error {
				if s.OCSPResponse == nil {
					return fmt.Errorf("missing OCSP Staple!")
				}
				return nil
			},
		}),
		nats.RootCAs(caCert),
		nats.ErrorHandler(noOpErrHandler),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer nc.Close()
	sub, err := nc.SubscribeSync("foo")
	if err != nil {
		t.Fatal(err)
	}
	nc.Publish("foo", []byte("hello world"))
	nc.Flush()

	_, err = sub.NextMsg(1 * time.Second)
	if err != nil {
		t.Fatal(err)
	}
	nc.Close()

	// Change and disable OCSP Stapling.
	content = `
		port: -1

		ocsp: { mode: never }

		tls {
			cert_file: "configs/certs/ocsp/server-status-request-url-01-cert.pem"
			key_file: "configs/certs/ocsp/server-status-request-url-01-key.pem"
			ca_file: "configs/certs/ocsp/ca-cert.pem"
			timeout: 5
		}
	`
	if err := os.WriteFile(conf, []byte(content), 0666); err != nil {
		t.Fatalf("Error writing config: %v", err)
	}
	if err := s.Reload(); err != nil {
		t.Fatal(err)
	}

	// The new certificate has must staple but OCSP Stapling is disabled.
	time.Sleep(2 * time.Second)

	nc, err = nats.Connect(fmt.Sprintf("tls://localhost:%d", opts.Port),
		nats.Secure(&tls.Config{
			VerifyConnection: func(s tls.ConnectionState) error {
				if s.OCSPResponse != nil {
					return fmt.Errorf("unexpected OCSP Staple!")
				}
				return nil
			},
		}),
		nats.RootCAs(caCert),
		nats.ErrorHandler(noOpErrHandler),
	)
	if err != nil {
		t.Fatal(err)
	}
	nc.Close()
}

func TestOCSPCustomConfigReloadEnable(t *testing.T) {
	const (
		caCert            = "configs/certs/ocsp/ca-cert.pem"
		caKey             = "configs/certs/ocsp/ca-key.pem"
		serverCert        = "configs/certs/ocsp/server-cert.pem"
		updatedServerCert = "configs/certs/ocsp/server-status-request-url-01-cert.pem"
	)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ocspr := NewOCSPResponder(t, caCert, caKey)
	defer ocspr.Shutdown(ctx)
	addr := fmt.Sprintf("http://%s", ocspr.Addr)
	SetOCSPStatus(t, addr, serverCert, ocsp.Good)
	SetOCSPStatus(t, addr, updatedServerCert, ocsp.Good)

	// Start with server without OCSP Stapling MustStaple
	content := `
		port: -1

		ocsp: { mode: never, url: "http://127.0.0.1:8888" }

		tls {
			cert_file: "configs/certs/ocsp/server-status-request-url-01-cert.pem"
			key_file: "configs/certs/ocsp/server-status-request-url-01-key.pem"
			ca_file: "configs/certs/ocsp/ca-cert.pem"
			timeout: 5
		}
	`
	conf := createConfFile(t, []byte(content))
	s, opts := RunServerWithConfig(conf)
	defer s.Shutdown()

	nc, err := nats.Connect(fmt.Sprintf("tls://localhost:%d", opts.Port),
		nats.Secure(&tls.Config{
			VerifyConnection: func(s tls.ConnectionState) error {
				if s.OCSPResponse != nil {
					return fmt.Errorf("unexpected OCSP Staple!")
				}
				return nil
			},
		}),
		nats.RootCAs(caCert),
		nats.ErrorHandler(noOpErrHandler),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer nc.Close()
	sub, err := nc.SubscribeSync("foo")
	if err != nil {
		t.Fatal(err)
	}
	nc.Publish("foo", []byte("hello world"))
	nc.Flush()

	_, err = sub.NextMsg(1 * time.Second)
	if err != nil {
		t.Fatal(err)
	}
	nc.Close()

	// Change and disable OCSP Stapling.
	content = `
		port: -1

		ocsp: { mode: always, url: "http://127.0.0.1:8888" }

		tls {
			cert_file: "configs/certs/ocsp/server-status-request-url-01-cert.pem"
			key_file: "configs/certs/ocsp/server-status-request-url-01-key.pem"
			ca_file: "configs/certs/ocsp/ca-cert.pem"
			timeout: 5
		}
	`
	if err := os.WriteFile(conf, []byte(content), 0666); err != nil {
		t.Fatalf("Error writing config: %v", err)
	}
	if err := s.Reload(); err != nil {
		t.Fatal(err)
	}
	time.Sleep(2 * time.Second)

	nc, err = nats.Connect(fmt.Sprintf("tls://localhost:%d", opts.Port),
		nats.Secure(&tls.Config{
			VerifyConnection: func(s tls.ConnectionState) error {
				if s.OCSPResponse == nil {
					return fmt.Errorf("missing OCSP Staple!")
				}
				return nil
			},
		}),
		nats.RootCAs(caCert),
		nats.ErrorHandler(noOpErrHandler),
	)
	if err != nil {
		t.Fatal(err)
	}
	nc.Close()
}

func TestOCSPTLSConfigNoLeafSet(t *testing.T) {
	o := DefaultTestOptions
	o.HTTPHost = "127.0.0.1"
	o.HTTPSPort = -1
	o.TLSConfig = &tls.Config{ServerName: "localhost"}
	cert, err := tls.LoadX509KeyPair("configs/certs/server-cert.pem", "configs/certs/server-key.pem")
	if err != nil {
		t.Fatalf("Got error reading certificates: %s", err)
	}
	o.TLSConfig.Certificates = []tls.Certificate{cert}
	s := RunServer(&o)
	s.Shutdown()
}

func TestOCSPSuperCluster(t *testing.T) {
	const (
		caCert = "configs/certs/ocsp/ca-cert.pem"
		caKey  = "configs/certs/ocsp/ca-key.pem"
	)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ocspr := NewOCSPResponder(t, caCert, caKey)
	defer ocspr.Shutdown(ctx)
	addr := fmt.Sprintf("http://%s", ocspr.Addr)
	SetOCSPStatus(t, addr, "configs/certs/ocsp/server-status-request-url-01-cert.pem", ocsp.Good)
	SetOCSPStatus(t, addr, "configs/certs/ocsp/server-status-request-url-02-cert.pem", ocsp.Good)
	SetOCSPStatus(t, addr, "configs/certs/ocsp/server-status-request-url-03-cert.pem", ocsp.Good)
	SetOCSPStatus(t, addr, "configs/certs/ocsp/server-status-request-url-04-cert.pem", ocsp.Good)
	SetOCSPStatus(t, addr, "configs/certs/ocsp/server-status-request-url-05-cert.pem", ocsp.Good)
	SetOCSPStatus(t, addr, "configs/certs/ocsp/server-status-request-url-06-cert.pem", ocsp.Good)
	SetOCSPStatus(t, addr, "configs/certs/ocsp/server-status-request-url-07-cert.pem", ocsp.Good)
	SetOCSPStatus(t, addr, "configs/certs/ocsp/server-status-request-url-08-cert.pem", ocsp.Good)
	SetOCSPStatus(t, addr, "configs/certs/ocsp/server-cert.pem", ocsp.Good)

	// Store Dirs
	storeDirA := t.TempDir()
	storeDirB := t.TempDir()
	storeDirC := t.TempDir()
	storeDirD := t.TempDir()

	// Gateway server configuration
	srvConfA := `
		host: "127.0.0.1"
		port: -1

		server_name: "A"

		ocsp { mode: "always" }

		tls {
			cert_file: "configs/certs/ocsp/server-status-request-url-01-cert.pem"
			key_file: "configs/certs/ocsp/server-status-request-url-01-key.pem"
			ca_file: "configs/certs/ocsp/ca-cert.pem"
			timeout: 5
		}
		store_dir: '%s'

		cluster {
			name: A
			host: "127.0.0.1"
			advertise: 127.0.0.1
			port: -1

			tls {
				cert_file: "configs/certs/ocsp/server-status-request-url-02-cert.pem"
				key_file: "configs/certs/ocsp/server-status-request-url-02-key.pem"
				ca_file: "configs/certs/ocsp/ca-cert.pem"
				timeout: 5
			}
		}

		gateway {
			name: A
			host: "127.0.0.1"
			port: -1
			advertise: "127.0.0.1"

			tls {
				cert_file: "configs/certs/ocsp/server-status-request-url-03-cert.pem"
				key_file: "configs/certs/ocsp/server-status-request-url-03-key.pem"
				ca_file: "configs/certs/ocsp/ca-cert.pem"
				timeout: 5
				verify: true
			}
		}
	`
	srvConfA = fmt.Sprintf(srvConfA, storeDirA)
	sconfA := createConfFile(t, []byte(srvConfA))
	srvA, optsA := RunServerWithConfig(sconfA)
	defer srvA.Shutdown()

	// Server that has the original as a cluster.
	srvConfB := `
		host: "127.0.0.1"
		port: -1

		server_name: "B"

		ocsp { mode: "always" }

		tls {
			cert_file: "configs/certs/ocsp/server-status-request-url-01-cert.pem"
			key_file: "configs/certs/ocsp/server-status-request-url-01-key.pem"
			ca_file: "configs/certs/ocsp/ca-cert.pem"
			timeout: 5
		}
		store_dir: '%s'

		cluster {
			name: A
			host: "127.0.0.1"
			advertise: 127.0.0.1
			port: -1

			routes: [ nats://127.0.0.1:%d ]

			tls {
				cert_file: "configs/certs/ocsp/server-status-request-url-02-cert.pem"
				key_file: "configs/certs/ocsp/server-status-request-url-02-key.pem"
				ca_file: "configs/certs/ocsp/ca-cert.pem"
				timeout: 5
			}
		}

		gateway {
			name: A
			host: "127.0.0.1"
			advertise: "127.0.0.1"
			port: -1

			tls {
				cert_file: "configs/certs/ocsp/server-status-request-url-03-cert.pem"
				key_file: "configs/certs/ocsp/server-status-request-url-03-key.pem"
				ca_file: "configs/certs/ocsp/ca-cert.pem"
				timeout: 5
				verify: true
			}
		}
	`
	srvConfB = fmt.Sprintf(srvConfB, storeDirB, optsA.Cluster.Port)
	conf := createConfFile(t, []byte(srvConfB))
	srvB, optsB := RunServerWithConfig(conf)
	defer srvB.Shutdown()

	// Client connects to server A.
	cA, err := nats.Connect(fmt.Sprintf("tls://127.0.0.1:%d", optsA.Port),
		nats.Secure(&tls.Config{
			VerifyConnection: func(s tls.ConnectionState) error {
				if s.OCSPResponse == nil {
					return fmt.Errorf("missing OCSP Staple from server")
				}
				return nil
			},
		}),
		nats.RootCAs(caCert),
		nats.ErrorHandler(noOpErrHandler),
	)
	if err != nil {
		t.Fatal(err)

	}
	defer cA.Close()

	// Start another server that will make connect as a gateway to cluster A.
	srvConfC := `
		host: "127.0.0.1"
		port: -1

		server_name: "C"

		ocsp { mode: "always" }

		tls {
			cert_file: "configs/certs/ocsp/server-status-request-url-05-cert.pem"
			key_file: "configs/certs/ocsp/server-status-request-url-05-key.pem"
			ca_file: "configs/certs/ocsp/ca-cert.pem"
			timeout: 5
		}
		store_dir: '%s'
		gateway {
			name: C
			host: "127.0.0.1"
			advertise: "127.0.0.1"
			port: -1
			gateways: [{
				name: "A",
				urls: ["nats://127.0.0.1:%d"]
				tls {
					cert_file: "configs/certs/ocsp/server-status-request-url-06-cert.pem"
					key_file: "configs/certs/ocsp/server-status-request-url-06-key.pem"
					ca_file: "configs/certs/ocsp/ca-cert.pem"
					timeout: 5
				}
			}]
			tls {
				cert_file: "configs/certs/ocsp/server-status-request-url-06-cert.pem"
				key_file: "configs/certs/ocsp/server-status-request-url-06-key.pem"
				ca_file: "configs/certs/ocsp/ca-cert.pem"
				timeout: 5
				verify: true
			}
		}
	`
	srvConfC = fmt.Sprintf(srvConfC, storeDirC, optsA.Gateway.Port)
	conf = createConfFile(t, []byte(srvConfC))
	srvC, optsC := RunServerWithConfig(conf)
	defer srvC.Shutdown()

	// Check that server is connected to any server from the other cluster.
	checkClusterFormed(t, srvA, srvB)
	waitForOutboundGateways(t, srvC, 1, 5*time.Second)

	// Start one more server that will become another gateway.
	srvConfD := `
		host: "127.0.0.1"
		port: -1

		server_name: "D"

		ocsp { mode: "auto", url: "%s" }

		tls {
			cert_file: "configs/certs/ocsp/server-status-request-url-07-cert.pem"
			key_file: "configs/certs/ocsp/server-status-request-url-07-key.pem"
			ca_file: "configs/certs/ocsp/ca-cert.pem"
			timeout: 5
		}
		store_dir: '%s'
		gateway {
			name: D
			host: "127.0.0.1"
			advertise: "127.0.0.1"
			port: -1
			gateways: [{
				name: "A",
				urls: ["nats://127.0.0.1:%d"]
				tls {
					cert_file: "configs/certs/ocsp/server-status-request-url-08-cert.pem"
					key_file: "configs/certs/ocsp/server-status-request-url-08-key.pem"
					ca_file: "configs/certs/ocsp/ca-cert.pem"
					timeout: 5
				}},
				{
				name: "C",
				urls: ["nats://127.0.0.1:%d"]

				####################################################################
				## TEST NOTE: This cert does not have an OCSP Staple intentionally##
				####################################################################
				tls {
					ca_file: "configs/certs/ocsp/ca-cert.pem"
					cert_file: "configs/certs/ocsp/server-cert.pem"
					key_file: "configs/certs/ocsp/server-key.pem"
					timeout: 5
				}}
			]
			tls {
				cert_file: "configs/certs/ocsp/server-status-request-url-08-cert.pem"
				key_file: "configs/certs/ocsp/server-status-request-url-08-key.pem"
				ca_file: "configs/certs/ocsp/ca-cert.pem"
				timeout: 5
				verify: true
			}
		}
	`
	srvConfD = fmt.Sprintf(srvConfD, addr, storeDirD, optsA.Gateway.Port, optsC.Gateway.Port)
	conf = createConfFile(t, []byte(srvConfD))
	srvD, _ := RunServerWithConfig(conf)
	defer srvD.Shutdown()

	// There should be a single gateway here because one of the gateway connections does not have a OCSP staple.
	waitForOutboundGateways(t, srvD, 1, 10*time.Second)

	// Connect to cluster A using server B.
	cB, err := nats.Connect(fmt.Sprintf("tls://127.0.0.1:%d", optsB.Port),
		nats.Secure(&tls.Config{
			VerifyConnection: func(s tls.ConnectionState) error {
				if s.OCSPResponse == nil {
					return fmt.Errorf("missing OCSP Staple from server")
				}
				return nil
			},
		}),
		nats.RootCAs(caCert),
		nats.ErrorHandler(noOpErrHandler),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer cB.Close()

	// Connects to cluster C using server C.
	cC, err := nats.Connect(fmt.Sprintf("tls://127.0.0.1:%d", optsC.Port),
		nats.Secure(&tls.Config{
			VerifyConnection: func(s tls.ConnectionState) error {
				if s.OCSPResponse == nil {
					return fmt.Errorf("missing OCSP Staple from server")
				}
				return nil
			},
		}),
		nats.RootCAs(caCert),
		nats.ErrorHandler(noOpErrHandler),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer cC.Close()

	_, err = cA.Subscribe("foo", func(m *nats.Msg) {
		m.Respond([]byte("From Server A"))
	})
	if err != nil {
		t.Errorf("%v", err)
	}
	cA.Flush()

	_, err = cB.Subscribe("bar", func(m *nats.Msg) {
		m.Respond([]byte("From Server B"))
	})
	if err != nil {
		t.Fatal(err)
	}
	cB.Flush()

	// Confirm that a message from server C can flow back to server A via gateway..
	var (
		resp *nats.Msg
		lerr error
	)
	for i := 0; i < 10; i++ {
		resp, lerr = cC.Request("foo", nil, 500*time.Millisecond)
		if lerr != nil {
			continue
		}
		got := string(resp.Data)
		expected := "From Server A"
		if got != expected {
			t.Fatalf("Expected %v, got: %v", expected, got)
		}

		// Make request to B
		resp, lerr = cC.Request("bar", nil, 500*time.Millisecond)
		if lerr != nil {
			continue
		}
		got = string(resp.Data)
		expected = "From Server B"
		if got != expected {
			t.Errorf("Expected %v, got: %v", expected, got)
		}
		lerr = nil
		break
	}
	if lerr != nil {
		t.Errorf("Unexpected error: %v", lerr)
	}
	if n := srvD.NumOutboundGateways(); n > 1 {
		t.Errorf("Expected single gateway, got: %v", n)
	}
}

func TestOCSPLocalIssuerDetermination(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	intermediateCA1Responder := NewOCSPResponderIntermediateCA1(t)
	intermediateCA1ResponderURL := fmt.Sprintf("http://%s", intermediateCA1Responder.Addr)
	defer intermediateCA1Responder.Shutdown(ctx)

	// Test constants
	ocspURL := intermediateCA1ResponderURL
	clientTrustBundle := "configs/certs/ocsp_peer/mini-ca/misc/trust_config1_bundle.pem"
	serverCert := "configs/certs/ocsp_peer/mini-ca/server1/TestServer1_cert.pem"

	var (
		errMissingStaple = fmt.Errorf("missing OCSP Staple from server")
	)

	for _, test := range []struct {
		name        string
		config      string
		opts        []nats.Option
		err         error
		rerr        error
		serverStart bool
		configure   func()
	}{
		{
			"Correct issuer configured in cert bundle",
			`
				port: -1

				ocsp {
					mode: always
				}

				tls {
					cert_file: "configs/certs/ocsp_peer/mini-ca/server1/TestServer1_bundle.pem"
					key_file: "configs/certs/ocsp_peer/mini-ca/server1/private/TestServer1_keypair.pem"
					ca_file: "configs/certs/ocsp_peer/mini-ca/root/root_cert.pem"
					timeout: 5
				}
			`,
			[]nats.Option{
				nats.Secure(&tls.Config{
					VerifyConnection: func(s tls.ConnectionState) error {
						if s.OCSPResponse == nil {
							return errMissingStaple
						}
						return nil
					},
				}),
				nats.ClientCert("./configs/certs/ocsp/client-cert.pem", "./configs/certs/ocsp/client-key.pem"),
				nats.RootCAs(clientTrustBundle),
				nats.ErrorHandler(noOpErrHandler),
			},
			nil,
			nil,
			true,
			func() {
				SetOCSPStatus(t, ocspURL, serverCert, ocsp.Good)
			},
		},
		{
			"Wrong issuer configured in cert bundle, server no start",
			`
				port: -1

				ocsp {
					mode: always
				}

				tls {
					cert_file: "configs/certs/ocsp_peer/mini-ca/misc/misconfig_TestServer1_bundle.pem"
					key_file: "configs/certs/ocsp_peer/mini-ca/server1/private/TestServer1_keypair.pem"
					ca_file: "configs/certs/ocsp_peer/mini-ca/root/root_cert.pem"
					timeout: 5
				}
			`,
			[]nats.Option{
				nats.Secure(&tls.Config{
					VerifyConnection: func(s tls.ConnectionState) error {
						if s.OCSPResponse == nil {
							return errMissingStaple
						}
						return nil
					},
				}),
				nats.ClientCert("./configs/certs/ocsp/client-cert.pem", "./configs/certs/ocsp/client-key.pem"),
				nats.RootCAs(clientTrustBundle),
				nats.ErrorHandler(noOpErrHandler),
			},
			nil,
			nil,
			false,
			func() {
				SetOCSPStatus(t, ocspURL, serverCert, ocsp.Good)
			},
		},
		{
			"Issuer configured in CA bundle only, configuration 1",
			`
				port: -1

				ocsp {
					mode: always
				}

				tls {
					cert_file: "configs/certs/ocsp_peer/mini-ca/server1/TestServer1_cert.pem"
					key_file: "configs/certs/ocsp_peer/mini-ca/server1/private/TestServer1_keypair.pem"
					ca_file: "configs/certs/ocsp_peer/mini-ca/misc/trust_config1_bundle.pem"
					timeout: 5
				}
			`,
			[]nats.Option{
				nats.Secure(&tls.Config{
					VerifyConnection: func(s tls.ConnectionState) error {
						if s.OCSPResponse == nil {
							return errMissingStaple
						}
						return nil
					},
				}),
				nats.ClientCert("./configs/certs/ocsp/client-cert.pem", "./configs/certs/ocsp/client-key.pem"),
				nats.RootCAs(clientTrustBundle),
				nats.ErrorHandler(noOpErrHandler),
			},
			nil,
			nil,
			true,
			func() {
				SetOCSPStatus(t, ocspURL, serverCert, ocsp.Good)
			},
		},
		{
			"Issuer configured in CA bundle only, configuration 2",
			`
				port: -1

				ocsp {
					mode: always
				}

				tls {
					cert_file: "configs/certs/ocsp_peer/mini-ca/server1/TestServer1_cert.pem"
					key_file: "configs/certs/ocsp_peer/mini-ca/server1/private/TestServer1_keypair.pem"
					ca_file: "configs/certs/ocsp_peer/mini-ca/misc/trust_config2_bundle.pem"
					timeout: 5
				}
			`,
			[]nats.Option{
				nats.Secure(&tls.Config{
					VerifyConnection: func(s tls.ConnectionState) error {
						if s.OCSPResponse == nil {
							return errMissingStaple
						}
						return nil
					},
				}),
				nats.ClientCert("./configs/certs/ocsp/client-cert.pem", "./configs/certs/ocsp/client-key.pem"),
				nats.RootCAs(clientTrustBundle),
				nats.ErrorHandler(noOpErrHandler),
			},
			nil,
			nil,
			true,
			func() {
				SetOCSPStatus(t, ocspURL, serverCert, ocsp.Good)
			},
		},
		{
			"Issuer configured in CA bundle only, configuration 3",
			`
				port: -1

				ocsp {
					mode: always
				}

				tls {
					cert_file: "configs/certs/ocsp_peer/mini-ca/server1/TestServer1_cert.pem"
					key_file: "configs/certs/ocsp_peer/mini-ca/server1/private/TestServer1_keypair.pem"
					ca_file: "configs/certs/ocsp_peer/mini-ca/misc/trust_config3_bundle.pem"
					timeout: 5
				}
			`,
			[]nats.Option{
				nats.Secure(&tls.Config{
					VerifyConnection: func(s tls.ConnectionState) error {
						if s.OCSPResponse == nil {
							return errMissingStaple
						}
						return nil
					},
				}),
				nats.ClientCert("./configs/certs/ocsp/client-cert.pem", "./configs/certs/ocsp/client-key.pem"),
				nats.RootCAs(clientTrustBundle),
				nats.ErrorHandler(noOpErrHandler),
			},
			nil,
			nil,
			true,
			func() {
				SetOCSPStatus(t, ocspURL, serverCert, ocsp.Good)
			},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			defer func() {
				r := recover()
				if r != nil && test.serverStart {
					t.Fatalf("Expected server start, unexpected panic: %v", r)
				}
				if r == nil && !test.serverStart {
					t.Fatalf("Expected server to not start and panic thrown")
				}
			}()
			test.configure()
			content := test.config
			conf := createConfFile(t, []byte(content))
			s, opts := RunServerWithConfig(conf)
			// server may not start for some tests
			if s != nil {
				defer s.Shutdown()
			}

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

func TestMixedCAOCSPSuperCluster(t *testing.T) {
	const (
		caCert = "configs/certs/ocsp_peer/mini-ca/root/root_cert.pem"
		caKey  = "configs/certs/ocsp/ca-key.pem"
	)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	intermediateCA1Responder := NewOCSPResponderIntermediateCA1(t)
	intermediateCA1ResponderURL := fmt.Sprintf("http://%s", intermediateCA1Responder.Addr)
	defer intermediateCA1Responder.Shutdown(ctx)
	SetOCSPStatus(t, intermediateCA1ResponderURL, "configs/certs/ocsp_peer/mini-ca/server1/TestServer1_cert.pem", ocsp.Good)

	intermediateCA2Responder := NewOCSPResponderIntermediateCA2(t)
	intermediateCA2ResponderURL := fmt.Sprintf("http://%s", intermediateCA2Responder.Addr)
	defer intermediateCA2Responder.Shutdown(ctx)
	SetOCSPStatus(t, intermediateCA2ResponderURL, "configs/certs/ocsp_peer/mini-ca/server2/TestServer3_cert.pem", ocsp.Good)

	// Store Dirs
	storeDirA := t.TempDir()
	storeDirB := t.TempDir()
	storeDirC := t.TempDir()

	// Gateway server configuration
	srvConfA := `
		host: "127.0.0.1"
		port: -1

		server_name: "A"

		ocsp { mode: "always" }

		tls {
			cert_file: "configs/certs/ocsp_peer/mini-ca/server1/TestServer1_bundle.pem"
			key_file: "configs/certs/ocsp_peer/mini-ca/server1/private/TestServer1_keypair.pem"
			ca_file: "configs/certs/ocsp_peer/mini-ca/root/root_cert.pem"
			timeout: 5
		}
		store_dir: '%s'

		cluster {
			name: A
			host: "127.0.0.1"
			advertise: 127.0.0.1
			port: -1

			tls {
				cert_file: "configs/certs/ocsp_peer/mini-ca/server1/TestServer1_bundle.pem"
				key_file: "configs/certs/ocsp_peer/mini-ca/server1/private/TestServer1_keypair.pem"
				ca_file: "configs/certs/ocsp_peer/mini-ca/root/root_cert.pem"
				timeout: 5
			}
		}

		gateway {
			name: A
			host: "127.0.0.1"
			port: -1
			advertise: "127.0.0.1"

			tls {
				cert_file: "configs/certs/ocsp_peer/mini-ca/server1/TestServer1_bundle.pem"
				key_file: "configs/certs/ocsp_peer/mini-ca/server1/private/TestServer1_keypair.pem"
				ca_file: "configs/certs/ocsp_peer/mini-ca/root/root_cert.pem"
				timeout: 5
				verify: true
			}
		}
	`
	srvConfA = fmt.Sprintf(srvConfA, storeDirA)
	sconfA := createConfFile(t, []byte(srvConfA))
	srvA, optsA := RunServerWithConfig(sconfA)
	defer srvA.Shutdown()

	// Server that has the original as a cluster.
	srvConfB := `
		host: "127.0.0.1"
		port: -1

		server_name: "B"

		ocsp { mode: "always" }

		tls {
			cert_file: "configs/certs/ocsp_peer/mini-ca/server1/TestServer1_bundle.pem"
			key_file: "configs/certs/ocsp_peer/mini-ca/server1/private/TestServer1_keypair.pem"
			ca_file: "configs/certs/ocsp_peer/mini-ca/root/root_cert.pem"
			timeout: 5
		}
		store_dir: '%s'

		cluster {
			name: A
			host: "127.0.0.1"
			advertise: 127.0.0.1
			port: -1

			routes: [ nats://127.0.0.1:%d ]

			tls {
				cert_file: "configs/certs/ocsp_peer/mini-ca/server1/TestServer1_bundle.pem"
				key_file: "configs/certs/ocsp_peer/mini-ca/server1/private/TestServer1_keypair.pem"
				ca_file: "configs/certs/ocsp_peer/mini-ca/root/root_cert.pem"
				timeout: 5
			}
		}

		gateway {
			name: A
			host: "127.0.0.1"
			advertise: "127.0.0.1"
			port: -1

			tls {
				cert_file: "configs/certs/ocsp_peer/mini-ca/server1/TestServer1_bundle.pem"
				key_file: "configs/certs/ocsp_peer/mini-ca/server1/private/TestServer1_keypair.pem"
				ca_file: "configs/certs/ocsp_peer/mini-ca/root/root_cert.pem"
				timeout: 5
				verify: true
			}
		}
	`
	srvConfB = fmt.Sprintf(srvConfB, storeDirB, optsA.Cluster.Port)
	conf := createConfFile(t, []byte(srvConfB))
	srvB, optsB := RunServerWithConfig(conf)
	defer srvB.Shutdown()

	// Client connects to server A.
	cA, err := nats.Connect(fmt.Sprintf("tls://127.0.0.1:%d", optsA.Port),
		nats.Secure(&tls.Config{
			VerifyConnection: func(s tls.ConnectionState) error {
				if s.OCSPResponse == nil {
					return fmt.Errorf("missing OCSP Staple from server")
				}
				return nil
			},
		}),
		nats.RootCAs(caCert),
		nats.ErrorHandler(noOpErrHandler),
	)
	if err != nil {
		t.Fatal(err)

	}
	defer cA.Close()

	// Start another server that will make connect as a gateway to cluster A but with different CA issuer.
	srvConfC := `
		host: "127.0.0.1"
		port: -1

		server_name: "C"

		ocsp { mode: "always" }

		tls {
			cert_file: "configs/certs/ocsp_peer/mini-ca/server2/TestServer3_bundle.pem"
			key_file: "configs/certs/ocsp_peer/mini-ca/server2/private/TestServer3_keypair.pem"
			ca_file: "configs/certs/ocsp_peer/mini-ca/root/root_cert.pem"
			timeout: 5
		}
		store_dir: '%s'
		gateway {
			name: C
			host: "127.0.0.1"
			advertise: "127.0.0.1"
			port: -1
			gateways: [{
				name: "A",
				urls: ["nats://127.0.0.1:%d"]
				tls {
					cert_file: "configs/certs/ocsp_peer/mini-ca/server2/TestServer3_bundle.pem"
					key_file: "configs/certs/ocsp_peer/mini-ca/server2/private/TestServer3_keypair.pem"
					ca_file: "configs/certs/ocsp_peer/mini-ca/root/root_cert.pem"
					timeout: 5
				}
			}]
			tls {
				cert_file: "configs/certs/ocsp_peer/mini-ca/server2/TestServer3_bundle.pem"
				key_file: "configs/certs/ocsp_peer/mini-ca/server2/private/TestServer3_keypair.pem"
				ca_file: "configs/certs/ocsp_peer/mini-ca/root/root_cert.pem"
				timeout: 5
				verify: true
			}
		}
	`
	srvConfC = fmt.Sprintf(srvConfC, storeDirC, optsA.Gateway.Port)
	conf = createConfFile(t, []byte(srvConfC))
	srvC, optsC := RunServerWithConfig(conf)
	defer srvC.Shutdown()

	// Check that server is connected to any server from the other cluster.
	checkClusterFormed(t, srvA, srvB)
	waitForOutboundGateways(t, srvC, 1, 5*time.Second)

	// Connect to cluster A using server B.
	cB, err := nats.Connect(fmt.Sprintf("tls://127.0.0.1:%d", optsB.Port),
		nats.Secure(&tls.Config{
			VerifyConnection: func(s tls.ConnectionState) error {
				if s.OCSPResponse == nil {
					return fmt.Errorf("missing OCSP Staple from server")
				}
				return nil
			},
		}),
		nats.RootCAs(caCert),
		nats.ErrorHandler(noOpErrHandler),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer cB.Close()

	// Connects to cluster C using server C.
	cC, err := nats.Connect(fmt.Sprintf("tls://127.0.0.1:%d", optsC.Port),
		nats.Secure(&tls.Config{
			VerifyConnection: func(s tls.ConnectionState) error {
				if s.OCSPResponse == nil {
					return fmt.Errorf("missing OCSP Staple from server")
				}
				return nil
			},
		}),
		nats.RootCAs(caCert),
		nats.ErrorHandler(noOpErrHandler),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer cC.Close()

	_, err = cA.Subscribe("foo", func(m *nats.Msg) {
		m.Respond([]byte("From Server A"))
	})
	if err != nil {
		t.Errorf("%v", err)
	}
	cA.Flush()

	_, err = cB.Subscribe("bar", func(m *nats.Msg) {
		m.Respond([]byte("From Server B"))
	})
	if err != nil {
		t.Fatal(err)
	}
	cB.Flush()

	// Confirm that a message from server C can flow back to server A via gateway..
	var (
		resp *nats.Msg
		lerr error
	)
	for i := 0; i < 10; i++ {
		resp, lerr = cC.Request("foo", nil, 500*time.Millisecond)
		if lerr != nil {
			continue
		}
		got := string(resp.Data)
		expected := "From Server A"
		if got != expected {
			t.Fatalf("Expected %v, got: %v", expected, got)
		}

		// Make request to B
		resp, lerr = cC.Request("bar", nil, 500*time.Millisecond)
		if lerr != nil {
			continue
		}
		got = string(resp.Data)
		expected = "From Server B"
		if got != expected {
			t.Errorf("Expected %v, got: %v", expected, got)
		}
		lerr = nil
		break
	}
	if lerr != nil {
		t.Errorf("Unexpected error: %v", lerr)
	}
}

func TestOCSPResponderHTTPMethods(t *testing.T) {
	t.Run("prefer get", func(t *testing.T) {
		testOCSPResponderHTTPMethods(t, "GET")
	})
	t.Run("prefer post", func(t *testing.T) {
		testOCSPResponderHTTPMethods(t, "POST")
	})
	t.Run("all methods failing", func(t *testing.T) {
		testOCSPResponderFailing(t, "TEST")
	})
}

func testOCSPResponderHTTPMethods(t *testing.T, method string) {
	const (
		caCert     = "configs/certs/ocsp/ca-cert.pem"
		caKey      = "configs/certs/ocsp/ca-key.pem"
		serverCert = "configs/certs/ocsp/server-cert.pem"
		serverKey  = "configs/certs/ocsp/server-key.pem"
	)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ocspr := NewOCSPResponderPreferringHTTPMethod(t, caCert, caKey, method)
	defer ocspr.Shutdown(ctx)
	addr := fmt.Sprintf("http://%s", ocspr.Addr)
	SetOCSPStatus(t, addr, serverCert, ocsp.Good)

	// Add another responder that fails.
	badaddr := "http://127.0.0.1:8889"
	badocsp := NewOCSPResponderCustomAddress(t, caCert, caKey, badaddr)
	defer badocsp.Shutdown(ctx)

	opts := server.Options{}
	opts.Host = "127.0.0.1"
	opts.NoLog = true
	opts.NoSigs = true
	opts.MaxControlLine = 4096
	opts.Port = -1
	opts.TLSCert = serverCert
	opts.TLSKey = serverKey
	opts.TLSCaCert = caCert
	opts.TLSTimeout = 5
	tcOpts := &server.TLSConfigOpts{
		CertFile: opts.TLSCert,
		KeyFile:  opts.TLSKey,
		CaFile:   opts.TLSCaCert,
		Timeout:  opts.TLSTimeout,
	}

	tlsConf, err := server.GenTLSConfig(tcOpts)
	if err != nil {
		t.Fatal(err)
	}
	opts.TLSConfig = tlsConf

	opts.OCSPConfig = &server.OCSPConfig{
		Mode:         server.OCSPModeAlways,
		OverrideURLs: []string{badaddr, addr},
	}
	srv := RunServer(&opts)
	defer srv.Shutdown()

	nc, err := nats.Connect(fmt.Sprintf("tls://localhost:%d", opts.Port),
		nats.Secure(&tls.Config{
			VerifyConnection: func(s tls.ConnectionState) error {
				resp, err := GetOCSPStatus(s)
				if err != nil {
					return err
				}
				if resp.Status != ocsp.Good {
					return fmt.Errorf("invalid staple")
				}
				return nil
			},
		}),
		nats.RootCAs(caCert),
		nats.ErrorHandler(noOpErrHandler),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer nc.Close()
	sub, err := nc.SubscribeSync("foo")
	if err != nil {
		t.Fatal(err)
	}
	nc.Publish("foo", []byte("hello world"))
	nc.Flush()

	_, err = sub.NextMsg(1 * time.Second)
	if err != nil {
		t.Fatal(err)
	}
	nc.Close()
}

func testOCSPResponderFailing(t *testing.T, method string) {
	const (
		caCert     = "configs/certs/ocsp/ca-cert.pem"
		caKey      = "configs/certs/ocsp/ca-key.pem"
		serverCert = "configs/certs/ocsp/server-cert.pem"
		serverKey  = "configs/certs/ocsp/server-key.pem"
	)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ocspr := NewOCSPResponderPreferringHTTPMethod(t, caCert, caKey, method)
	defer ocspr.Shutdown(ctx)
	addr := fmt.Sprintf("http://%s", ocspr.Addr)
	SetOCSPStatus(t, addr, serverCert, ocsp.Good)

	opts := server.Options{}
	opts.Host = "127.0.0.1"
	opts.NoLog = true
	opts.NoSigs = true
	opts.MaxControlLine = 4096
	opts.Port = -1
	opts.TLSCert = serverCert
	opts.TLSKey = serverKey
	opts.TLSCaCert = caCert
	opts.TLSTimeout = 5
	tcOpts := &server.TLSConfigOpts{
		CertFile: opts.TLSCert,
		KeyFile:  opts.TLSKey,
		CaFile:   opts.TLSCaCert,
		Timeout:  opts.TLSTimeout,
	}

	tlsConf, err := server.GenTLSConfig(tcOpts)
	if err != nil {
		t.Fatal(err)
	}
	opts.TLSConfig = tlsConf

	opts.OCSPConfig = &server.OCSPConfig{
		Mode:         server.OCSPModeAlways,
		OverrideURLs: []string{addr},
	}
	expected := "bad OCSP status update for certificate at 'configs/certs/ocsp/server-cert.pem': "
	expected += "exhausted ocsp servers: non-ok http status on POST request (reqlen=68): 400\nnon-ok http status on GET request (reqlen=92): 400"
	_, err = server.NewServer(&opts)
	if err == nil {
		t.Error("Unexpected success setting up server")
	} else if err.Error() != expected {
		t.Errorf("Expected %q, got: %q", expected, err.Error())
	}
}

func TestOCSPAlwaysUnauthorizedSignerDoesNotShutdown(t *testing.T) {
	const (
		caCert     = "configs/certs/ocsp/ca-cert.pem"
		caKey      = "configs/certs/ocsp/ca-key.pem"
		serverCert = "configs/certs/ocsp/server-cert.pem"
		serverKey  = "configs/certs/ocsp/server-key.pem"
		// Issued by the CA, but not authorized by it to act as OCSP signer.
		badSignerCert = "configs/certs/ocsp/client-cert.pem"
		badSignerKey  = "configs/certs/ocsp/client-key.pem"
	)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ocspr := NewOCSPResponder(t, caCert, caKey)
	defer ocspr.Shutdown(ctx)
	addr := fmt.Sprintf("http://%s", ocspr.Addr)
	SetOCSPStatus(t, addr, serverCert, ocsp.Good)

	badOcspr := NewOCSPResponderDesignatedCustomAddress(t, caCert, badSignerCert, badSignerKey, "127.0.0.1:8887")
	defer badOcspr.Shutdown(ctx)
	badAddr := fmt.Sprintf("http://%s", badOcspr.Addr)
	SetOCSPStatus(t, badAddr, serverCert, ocsp.Revoked)

	opts := server.Options{}
	opts.Host = "127.0.0.1"
	opts.NoLog = true
	opts.NoSigs = true
	opts.MaxControlLine = 4096
	opts.Port = -1
	opts.TLSCert = serverCert
	opts.TLSKey = serverKey
	opts.TLSCaCert = caCert
	opts.TLSTimeout = 5
	tcOpts := &server.TLSConfigOpts{
		CertFile: opts.TLSCert,
		KeyFile:  opts.TLSKey,
		CaFile:   opts.TLSCaCert,
		Timeout:  opts.TLSTimeout,
	}

	tlsConf, err := server.GenTLSConfig(tcOpts)
	if err != nil {
		t.Fatal(err)
	}
	opts.TLSConfig = tlsConf

	// The revoked status signed by an unauthorized signer must be ignored, falling back to the CA's responder.
	opts.OCSPConfig = &server.OCSPConfig{
		Mode:         server.OCSPModeAlways,
		OverrideURLs: []string{badAddr, addr},
	}
	srv := RunServer(&opts)
	defer srv.Shutdown()
	l := &captureErrLogger{ch: make(chan string, 10)}
	srv.SetLogger(l, false, false)

	// The CA's responder becomes unavailable, so the next fetch of OCSP Staples only gets
	// the revoked status signed by an unauthorized signer, which must still be ignored.
	// The OCSP Responder instructs the NATS Server to fetch OCSP Staples every 2 seconds.
	ocspr.Shutdown(ctx)
	timeout := time.After(10 * time.Second)
	for refreshed := false; !refreshed; {
		select {
		case msg := <-l.ch:
			refreshed = strings.Contains(msg, "Bad OCSP status update")
		case <-timeout:
			t.Fatal("Expected OCSP status update from the unauthorized signer to be rejected")
		}
	}

	nc, err := nats.Connect(fmt.Sprintf("tls://localhost:%d", opts.Port),
		nats.RootCAs(caCert),
		nats.ErrorHandler(noOpErrHandler),
	)
	if err != nil {
		t.Fatalf("Expected server to not shutdown, got: %v", err)
	}
	nc.Close()
}
