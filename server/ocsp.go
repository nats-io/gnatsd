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

package server

import (
	"bytes"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"encoding/asn1"
	"encoding/base64"
	"encoding/pem"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"golang.org/x/crypto/ocsp"

	"github.com/nats-io/nats-server/v2/server/certidp"
	"github.com/nats-io/nats-server/v2/server/certstore"
)

const (
	defaultOCSPStoreDir      = "ocsp"
	defaultOCSPCheckInterval = 24 * time.Hour
	minOCSPCheckInterval     = 2 * time.Minute
)

type OCSPMode uint8

const (
	// OCSPModeAuto staples a status, only if "status_request" is set in cert.
	OCSPModeAuto OCSPMode = iota

	// OCSPModeAlways enforces OCSP stapling for certs and shuts down the server in
	// case a server is revoked or cannot get OCSP staples.
	OCSPModeAlways

	// OCSPModeNever disables OCSP stapling even if cert has Must-Staple flag.
	OCSPModeNever

	// OCSPModeMust honors the Must-Staple flag from a certificate but also causing shutdown
	// in case the certificate has been revoked.
	OCSPModeMust
)

// OCSPMonitor monitors the state of a staple per certificate.
type OCSPMonitor struct {
	kind     string
	mu       sync.Mutex
	raw      []byte
	srv      *Server
	certFile string
	resp     *ocsp.Response
	hc       *http.Client
	stopCh   chan struct{}
	Leaf     *x509.Certificate
	Issuer   *x509.Certificate

	shutdownOnRevoke bool
}

func (oc *OCSPMonitor) getNextRun() time.Duration {
	oc.mu.Lock()
	nextUpdate := oc.resp.NextUpdate
	oc.mu.Unlock()

	now := time.Now()
	if nextUpdate.IsZero() {
		// If response is missing NextUpdate, we check the day after.
		// Technically, if NextUpdate is missing, we can try whenever.
		// https://tools.ietf.org/html/rfc6960#section-4.2.2.1
		return defaultOCSPCheckInterval
	}
	dur := nextUpdate.Sub(now) / 2

	// If negative, then wait a couple of minutes before getting another staple.
	if dur < 0 {
		return minOCSPCheckInterval
	}

	return dur
}

func (oc *OCSPMonitor) getStatus() ([]byte, *ocsp.Response, error) {
	raw, resp := oc.getCacheStatus()
	if len(raw) > 0 && resp != nil {
		// Check if the OCSP is still valid.
		if err := validOCSPResponse(resp); err == nil {
			return raw, resp, nil
		}
	}
	var err error
	raw, resp, err = oc.getLocalStatus()
	if err == nil {
		return raw, resp, nil
	}

	return oc.getRemoteStatus()
}

func (oc *OCSPMonitor) getCacheStatus() ([]byte, *ocsp.Response) {
	oc.mu.Lock()
	defer oc.mu.Unlock()
	return oc.raw, oc.resp
}

func (oc *OCSPMonitor) getLocalStatus() ([]byte, *ocsp.Response, error) {
	opts := oc.srv.getOpts()
	storeDir := opts.StoreDir
	if storeDir == _EMPTY_ {
		return nil, nil, fmt.Errorf("store_dir not set")
	}

	// This key must be based upon the current full certificate, not the public key,
	// so MUST be on the full raw certificate and not an SPKI or other reduced form.
	key := fmt.Sprintf("%x", sha256.Sum256(oc.Leaf.Raw))

	oc.mu.Lock()
	raw, err := os.ReadFile(filepath.Join(storeDir, defaultOCSPStoreDir, key))
	oc.mu.Unlock()
	if err != nil {
		return nil, nil, err
	}

	resp, err := oc.parseStatus(raw)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to get local status: %w", err)
	}

	// Cache the response.
	oc.mu.Lock()
	oc.raw = raw
	oc.resp = resp
	oc.mu.Unlock()

	return raw, resp, nil
}

// parseStatus parses the OCSP response for the monitored certificate, checks if OCSP Response
// delegation present and if so is valid, and checks the response's time validity.
func (oc *OCSPMonitor) parseStatus(raw []byte) (*ocsp.Response, error) {
	resp, err := ocsp.ParseResponseForCert(raw, oc.Leaf, oc.Issuer)
	if err != nil {
		return nil, err
	}
	// Check if OCSP Response delegation present and if so is valid
	if !certidp.ValidDelegationCheck(oc.Issuer, resp) {
		return nil, errors.New("OCSP response signer missing authorization by CA to act as OCSP signer")
	}
	if err := validOCSPResponse(resp); err != nil {
		return nil, err
	}
	return resp, nil
}

func (oc *OCSPMonitor) getRemoteStatus() ([]byte, *ocsp.Response, error) {
	opts := oc.srv.getOpts()
	var overrideURLs []string
	if config := opts.OCSPConfig; config != nil {
		overrideURLs = config.OverrideURLs
	}
	getRequestBytes := func(u string, reqDER []byte, hc *http.Client) ([]byte, error) {
		reqEnc := base64.StdEncoding.EncodeToString(reqDER)
		u = fmt.Sprintf("%s/%s", u, reqEnc)
		start := time.Now()
		resp, err := hc.Get(u)
		if err != nil {
			return nil, err
		}
		defer resp.Body.Close()

		oc.srv.Debugf("Received OCSP response (method=GET, status=%v, url=%s, duration=%.3fs)",
			resp.StatusCode, u, time.Since(start).Seconds())
		if resp.StatusCode > 299 {
			return nil, fmt.Errorf("non-ok http status on GET request (reqlen=%d): %d", len(reqEnc), resp.StatusCode)
		}
		return io.ReadAll(resp.Body)
	}
	postRequestBytes := func(u string, body []byte, hc *http.Client) ([]byte, error) {
		hreq, err := http.NewRequest("POST", u, bytes.NewReader(body))
		if err != nil {
			return nil, err
		}
		hreq.Header.Add("Content-Type", "application/ocsp-request")
		hreq.Header.Add("Accept", "application/ocsp-response")

		start := time.Now()
		resp, err := hc.Do(hreq)
		if err != nil {
			return nil, err
		}
		defer resp.Body.Close()

		oc.srv.Debugf("Received OCSP response (method=POST, status=%v, url=%s, duration=%.3fs)",
			resp.StatusCode, u, time.Since(start).Seconds())
		if resp.StatusCode > 299 {
			return nil, fmt.Errorf("non-ok http status on POST request (reqlen=%d): %d", len(body), resp.StatusCode)
		}
		return io.ReadAll(resp.Body)
	}

	// Request documentation:
	// https://tools.ietf.org/html/rfc6960#appendix-A.1

	reqDER, err := ocsp.CreateRequest(oc.Leaf, oc.Issuer, nil)
	if err != nil {
		return nil, nil, err
	}

	responders := oc.Leaf.OCSPServer
	if len(overrideURLs) > 0 {
		responders = overrideURLs
	}
	if len(responders) == 0 {
		return nil, nil, fmt.Errorf("no available ocsp servers")
	}

	oc.mu.Lock()
	hc := oc.hc
	oc.mu.Unlock()

	var (
		raw  []byte
		resp *ocsp.Response
	)
	for _, u := range responders {
		var postErr, getErr error
		u = strings.TrimSuffix(u, "/")
		// Prefer to make POST requests first.
		raw, postErr = postRequestBytes(u, reqDER, hc)
		if postErr != nil {
			// Fallback to use a GET request.
			raw, getErr = getRequestBytes(u, reqDER, hc)
			if getErr != nil {
				err = errors.Join(postErr, getErr)
				continue
			}
		}
		// An invalid response must not prevent trying the next responder.
		if resp, err = oc.parseStatus(raw); err != nil {
			err = fmt.Errorf("failed to get remote status from %s: %w", u, err)
			continue
		}
		break
	}
	if err != nil {
		return nil, nil, fmt.Errorf("exhausted ocsp servers: %w", err)
	}

	if storeDir := opts.StoreDir; storeDir != _EMPTY_ {
		key := fmt.Sprintf("%x", sha256.Sum256(oc.Leaf.Raw))
		if err := oc.writeOCSPStatus(storeDir, key, raw); err != nil {
			return nil, nil, fmt.Errorf("failed to write ocsp status: %w", err)
		}
	}

	oc.mu.Lock()
	oc.raw = raw
	oc.resp = resp
	oc.mu.Unlock()

	return raw, resp, nil
}

func (oc *OCSPMonitor) run() {
	s := oc.srv
	s.mu.Lock()
	quitCh := s.quitCh
	s.mu.Unlock()

	var doShutdown bool
	defer func() {
		// Need to decrement before shuting down, otherwise shutdown
		// would be stuck waiting on grWG to go down to 0.
		s.grWG.Done()
		if doShutdown {
			s.Shutdown()
		}
	}()

	oc.mu.Lock()
	shutdownOnRevoke := oc.shutdownOnRevoke
	certFile := oc.certFile
	stopCh := oc.stopCh
	kind := oc.kind
	oc.mu.Unlock()

	var nextRun time.Duration
	_, resp, err := oc.getStatus()
	if err == nil && resp.Status == ocsp.Good {
		nextRun = oc.getNextRun()
		t := resp.NextUpdate.Format(time.RFC3339Nano)
		s.Noticef(
			"Found OCSP status for %s certificate at '%s': good, next update %s, checking again in %s",
			kind, certFile, t, nextRun,
		)
	} else if err == nil && shutdownOnRevoke {
		// If resp.Status is ocsp.Revoked, ocsp.Unknown, or any other value.
		s.Errorf("Found OCSP status for %s certificate at '%s': %s", kind, certFile, ocspStatusString(resp.Status))
		doShutdown = true
		return
	}

	nextRunTimer := time.NewTimer(nextRun)
	defer stopAndClearTimer(&nextRunTimer)

	for {
		// On reload, if the certificate changes then need to stop this monitor.
		select {
		case <-nextRunTimer.C:
		case <-stopCh:
			// In case of reload and have to restart the OCSP stapling monitoring.
			return
		case <-quitCh:
			// Server quit channel.
			return
		}
		_, resp, err := oc.getRemoteStatus()
		if err != nil {
			nextRun = oc.getNextRun()
			s.Errorf("Bad OCSP status update for certificate '%s': %s, trying again in %v", certFile, err, nextRun)
			nextRunTimer.Reset(nextRun)
			continue
		}

		switch n := resp.Status; n {
		case ocsp.Good:
			nextRun = oc.getNextRun()
			t := resp.NextUpdate.Format(time.RFC3339Nano)
			s.Noticef(
				"Received OCSP status for %s certificate '%s': good, next update %s, checking again in %s",
				kind, certFile, t, nextRun,
			)
			nextRunTimer.Reset(nextRun)
			continue
		default:
			s.Errorf("Received OCSP status for %s certificate '%s': %s", kind, certFile, ocspStatusString(n))
			if shutdownOnRevoke {
				doShutdown = true
			}
			return
		}
	}
}

func (oc *OCSPMonitor) stop() {
	oc.mu.Lock()
	stopCh := oc.stopCh
	oc.mu.Unlock()
	stopCh <- struct{}{}
}

// NewOCSPMonitor takes a TLS configuration then wraps it with the callbacks set for OCSP verification
// along with a monitor that will periodically fetch OCSP staples.
func (srv *Server) NewOCSPMonitor(config *tlsConfigKind) (*tls.Config, *OCSPMonitor, error) {
	kind := config.kind
	tc := config.tlsConfig
	tcOpts := config.tlsOpts
	opts := srv.getOpts()
	oc := opts.OCSPConfig

	// We need to track the CA certificate in case the CA is not present
	// in the chain to be able to verify the signature of the OCSP staple.
	var (
		certFile string
		caFile   string
	)
	if kind == kindStringMap[CLIENT] {
		tcOpts = opts.tlsConfigOpts
		if opts.TLSCert != _EMPTY_ {
			certFile = opts.TLSCert
		}
		if opts.TLSCaCert != _EMPTY_ {
			caFile = opts.TLSCaCert
		}
	}
	if tcOpts != nil {
		certFile = tcOpts.CertFile
		caFile = tcOpts.CaFile
	}

	// NOTE: Currently OCSP Stapling is enabled only for the first certificate found.
	var mon *OCSPMonitor
	for _, currentCert := range tc.Certificates {
		// Create local copy since this will be used in the GetCertificate callback.
		cert := currentCert

		// This is normally non-nil, but can still be nil here when in tests
		// or in some embedded scenarios.
		if cert.Leaf == nil {
			if len(cert.Certificate) <= 0 {
				return nil, nil, fmt.Errorf("no certificate found")
			}
			var err error
			cert.Leaf, err = x509.ParseCertificate(cert.Certificate[0])
			if err != nil {
				return nil, nil, fmt.Errorf("error parsing certificate: %v", err)
			}
		}
		var shutdownOnRevoke bool
		mustStaple := hasOCSPStatusRequest(cert.Leaf)
		if oc != nil {
			switch {
			case oc.Mode == OCSPModeNever:
				if mustStaple {
					srv.Warnf("Certificate at '%s' has MustStaple but OCSP is disabled", certFile)
				}
				return tc, nil, nil
			case oc.Mode == OCSPModeAlways:
				// Start the monitor for this cert even if it does not have
				// the MustStaple flag and shutdown the server in case the
				// staple ever gets revoked.
				mustStaple = true
				shutdownOnRevoke = true
			case oc.Mode == OCSPModeMust && mustStaple:
				shutdownOnRevoke = true
			case oc.Mode == OCSPModeAuto && !mustStaple:
				// "status_request" MustStaple flag not set in certificate. No need to do anything.
				return tc, nil, nil
			}
		}
		if !mustStaple {
			// No explicit OCSP config and cert does not have MustStaple flag either.
			return tc, nil, nil
		}

		if err := srv.setupOCSPStapleStoreDir(); err != nil {
			return nil, nil, err
		}

		// TODO: Add OCSP 'responder_cert' option in case CA cert not available.
		issuer, err := getOCSPIssuer(caFile, cert.Certificate)
		if err != nil {
			return nil, nil, err
		}

		mon = &OCSPMonitor{
			kind:             kind,
			srv:              srv,
			hc:               &http.Client{Timeout: 30 * time.Second},
			shutdownOnRevoke: shutdownOnRevoke,
			certFile:         certFile,
			stopCh:           make(chan struct{}, 1),
			Leaf:             cert.Leaf,
			Issuer:           issuer,
		}

		// Get the certificate status from the memory, then remote OCSP responder.
		if _, resp, err := mon.getStatus(); err != nil {
			return nil, nil, fmt.Errorf("bad OCSP status update for certificate at '%s': %s", certFile, err)
		} else if resp != nil && resp.Status != ocsp.Good && shutdownOnRevoke {
			return nil, nil, fmt.Errorf("found existing OCSP status for certificate at '%s': %s", certFile, ocspStatusString(resp.Status))
		}

		// Callbacks below will be in charge of returning the certificate instead,
		// so this has to be nil.
		tc.Certificates = nil

		// GetCertificate returns a certificate that's presented to a client.
		tc.GetCertificate = func(info *tls.ClientHelloInfo) (*tls.Certificate, error) {
			ccert := cert
			raw, _, err := mon.getStatus()
			if err != nil {
				return nil, err
			}
			return &tls.Certificate{
				OCSPStaple:                   raw,
				Certificate:                  ccert.Certificate,
				PrivateKey:                   ccert.PrivateKey,
				SupportedSignatureAlgorithms: ccert.SupportedSignatureAlgorithms,
				SignedCertificateTimestamps:  ccert.SignedCertificateTimestamps,
				Leaf:                         ccert.Leaf,
			}, nil
		}

		// Check whether need to verify staples from a peer router or gateway connection.
		switch kind {
		case kindStringMap[ROUTER], kindStringMap[GATEWAY]:
			tc.VerifyConnection = func(s tls.ConnectionState) error {
				oresp := s.OCSPResponse
				if oresp == nil {
					return fmt.Errorf("%s peer missing OCSP Staple", kind)
				}

				// Peer connections will verify the response of the staple.
				if len(s.VerifiedChains) == 0 {
					return fmt.Errorf("%s peer missing TLS verified chains", kind)
				}

				chain := s.VerifiedChains[0]
				peerLeaf := chain[0]
				peerIssuer := certidp.GetLeafIssuerCert(chain, 0)
				if peerIssuer == nil {
					return fmt.Errorf("failed to get issuer certificate for %s peer", kind)
				}

				// Response signature of issuer or issuer delegate is checked in the library parse
				resp, err := ocsp.ParseResponseForCert(oresp, peerLeaf, peerIssuer)
				if err != nil {
					return fmt.Errorf("failed to parse OCSP response from %s peer: %w", kind, err)
				}

				// If signer was issuer delegate double-check issuer delegate authorization
				if !certidp.ValidDelegationCheck(peerIssuer, resp) {
					return fmt.Errorf("OCSP staple's signer missing authorization by CA to act as OCSP signer")
				}

				// Check that the OCSP response is effective, take defaults for clockskew and default validity
				peerOpts := certidp.OCSPPeerConfig{ClockSkew: -1, TTLUnsetNextUpdate: -1}
				sLog := certidp.Log{Debugf: srv.Debugf}
				if !certidp.OCSPResponseCurrent(resp, &peerOpts, &sLog) {
					return fmt.Errorf("OCSP staple from %s peer not current", kind)
				}

				if resp.Status != ocsp.Good {
					return fmt.Errorf("bad status for OCSP Staple from %s peer: %s", kind, ocspStatusString(resp.Status))
				}

				return nil
			}

			// When server makes a peer connection, need to also present an OCSP Staple.
			tc.GetClientCertificate = func(info *tls.CertificateRequestInfo) (*tls.Certificate, error) {
				ccert := cert
				raw, _, err := mon.getStatus()
				if err != nil {
					return nil, err
				}
				// NOTE: crypto/tls.sendClientCertificate internally also calls getClientCertificate
				// so if for some reason these callbacks are triggered concurrently during a reconnect
				// there can be a race. To avoid that, the OCSP monitor lock is used to serialize access
				// to the staple which could also change inflight during an update.
				mon.mu.Lock()
				ccert.OCSPStaple = raw
				mon.mu.Unlock()

				return &ccert, nil
			}
		default:
			// GetClientCertificate returns a certificate that's presented to a server.
			tc.GetClientCertificate = func(info *tls.CertificateRequestInfo) (*tls.Certificate, error) {
				return &cert, nil
			}
		}
	}
	return tc, mon, nil
}

func (s *Server) setupOCSPStapleStoreDir() error {
	opts := s.getOpts()
	storeDir := opts.StoreDir
	if storeDir == _EMPTY_ {
		return nil
	}
	storeDir = filepath.Join(storeDir, defaultOCSPStoreDir)
	if stat, err := os.Stat(storeDir); os.IsNotExist(err) {
		if err := os.MkdirAll(storeDir, defaultDirPerms); err != nil {
			return fmt.Errorf("could not create OCSP storage directory - %v", err)
		}
	} else if stat == nil || !stat.IsDir() {
		return fmt.Errorf("OCSP storage directory is not a directory")
	}
	return nil
}

type tlsConfigKind struct {
	tlsConfig   *tls.Config
	tlsOpts     *TLSConfigOpts
	kind        string
	isLeafSpoke bool
	apply       func(*tls.Config)
}

func (s *Server) configureOCSP() []*tlsConfigKind {
	sopts := s.getOpts()

	configs := make([]*tlsConfigKind, 0)

	if config := sopts.TLSConfig; config != nil {
		opts := sopts.tlsConfigOpts
		o := &tlsConfigKind{
			kind:      kindStringMap[CLIENT],
			tlsConfig: config,
			tlsOpts:   opts,
			apply:     func(tc *tls.Config) { sopts.TLSConfig = tc },
		}
		configs = append(configs, o)
	}
	if config := sopts.Websocket.TLSConfig; config != nil {
		opts := sopts.Websocket.tlsConfigOpts
		o := &tlsConfigKind{
			kind:      kindStringMap[CLIENT],
			tlsConfig: config,
			tlsOpts:   opts,
			apply:     func(tc *tls.Config) { sopts.Websocket.TLSConfig = tc },
		}
		configs = append(configs, o)
	}
	if config := sopts.MQTT.TLSConfig; config != nil {
		opts := sopts.MQTT.tlsConfigOpts
		o := &tlsConfigKind{
			kind:      kindStringMap[CLIENT],
			tlsConfig: config,
			tlsOpts:   opts,
			apply:     func(tc *tls.Config) { sopts.MQTT.TLSConfig = tc },
		}
		configs = append(configs, o)
	}
	if config := sopts.Cluster.TLSConfig; config != nil {
		opts := sopts.Cluster.tlsConfigOpts
		o := &tlsConfigKind{
			kind:      kindStringMap[ROUTER],
			tlsConfig: config,
			tlsOpts:   opts,
			apply:     func(tc *tls.Config) { sopts.Cluster.TLSConfig = tc },
		}
		configs = append(configs, o)
	}
	if config := sopts.LeafNode.TLSConfig; config != nil {
		opts := sopts.LeafNode.tlsConfigOpts
		o := &tlsConfigKind{
			kind:      kindStringMap[LEAF],
			tlsConfig: config,
			tlsOpts:   opts,
			apply:     func(tc *tls.Config) { sopts.LeafNode.TLSConfig = tc },
		}
		configs = append(configs, o)
	}
	for _, remote := range sopts.LeafNode.Remotes {
		if config := remote.TLSConfig; config != nil {
			// Use a copy of the remote here since will be used
			// in the apply func callback below.
			r, opts := remote, remote.tlsConfigOpts
			o := &tlsConfigKind{
				kind:        kindStringMap[LEAF],
				tlsConfig:   config,
				tlsOpts:     opts,
				isLeafSpoke: true,
				apply:       func(tc *tls.Config) { r.TLSConfig = tc },
			}
			configs = append(configs, o)
		}
	}
	if config := sopts.Gateway.TLSConfig; config != nil {
		opts := sopts.Gateway.tlsConfigOpts
		o := &tlsConfigKind{
			kind:      kindStringMap[GATEWAY],
			tlsConfig: config,
			tlsOpts:   opts,
			apply:     func(tc *tls.Config) { sopts.Gateway.TLSConfig = tc },
		}
		configs = append(configs, o)
	}
	for _, remote := range sopts.Gateway.Gateways {
		if config := remote.TLSConfig; config != nil {
			gw, opts := remote, remote.tlsConfigOpts
			o := &tlsConfigKind{
				kind:      kindStringMap[GATEWAY],
				tlsConfig: config,
				tlsOpts:   opts,
				apply:     func(tc *tls.Config) { gw.TLSConfig = tc },
			}
			configs = append(configs, o)
		}
	}
	return configs
}

func (s *Server) enableOCSP() error {
	configs := s.configureOCSP()

	for _, config := range configs {

		// We do not staple Leaf Hub and Leaf Spokes, use ocsp_peer
		if config.kind != kindStringMap[LEAF] {
			// OCSP Stapling feature, will also enable tls server peer check for gateway and route peers
			tc, mon, err := s.NewOCSPMonitor(config)
			if err != nil {
				return err
			}
			// Check if an OCSP stapling monitor is required for this certificate.
			if mon != nil {
				s.ocsps = append(s.ocsps, mon)

				// Override the TLS config with one that follows OCSP stapling
				config.apply(tc)
			}
		}

		// OCSP peer check (client mTLS, leaf mTLS, leaf remote TLS)
		if config.kind == kindStringMap[CLIENT] || config.kind == kindStringMap[LEAF] {
			tc, plugged, err := s.plugTLSOCSPPeer(config)
			if err != nil {
				return err
			}
			if plugged && tc != nil {
				s.ocspPeerVerify = true
				config.apply(tc)
			}
		}
	}

	return nil
}

func (s *Server) startOCSPMonitoring() {
	s.mu.Lock()
	ocsps := s.ocsps
	s.mu.Unlock()
	if ocsps == nil {
		return
	}
	for _, mon := range ocsps {
		m := mon
		m.mu.Lock()
		kind := m.kind
		m.mu.Unlock()
		s.Noticef("OCSP Stapling enabled for %s connections", kind)
		s.startGoRoutine(func() { m.run() })
	}
}

func (s *Server) reloadOCSP() error {
	if err := s.setupOCSPStapleStoreDir(); err != nil {
		return err
	}

	s.mu.Lock()
	ocsps := s.ocsps
	s.mu.Unlock()

	// Stop all OCSP Stapling monitors in case there were any running.
	for _, oc := range ocsps {
		oc.stop()
	}

	configs := s.configureOCSP()

	// Restart the monitors under the new configuration.
	ocspm := make([]*OCSPMonitor, 0)

	// Reset server's ocspPeerVerify flag to re-detect at least one plugged OCSP peer
	s.mu.Lock()
	s.ocspPeerVerify = false
	s.mu.Unlock()
	s.stopOCSPResponseCache()

	for _, config := range configs {
		// We do not staple Leaf Hub and Leaf Spokes, use ocsp_peer
		if config.kind != kindStringMap[LEAF] {
			tc, mon, err := s.NewOCSPMonitor(config)
			if err != nil {
				return err
			}
			// Check if an OCSP stapling monitor is required for this certificate.
			if mon != nil {
				ocspm = append(ocspm, mon)

				// Apply latest TLS configuration after OCSP monitors have started.
				defer config.apply(tc)
			}
		}

		// OCSP peer check (client mTLS, leaf mTLS, leaf remote TLS)
		if config.kind == kindStringMap[CLIENT] || config.kind == kindStringMap[LEAF] {
			tc, plugged, err := s.plugTLSOCSPPeer(config)
			if err != nil {
				return err
			}
			if plugged && tc != nil {
				s.ocspPeerVerify = true
				defer config.apply(tc)
			}
		}
	}

	// Replace stopped monitors with the new ones.
	s.mu.Lock()
	s.ocsps = ocspm
	s.mu.Unlock()

	// Dispatch all goroutines once again.
	s.startOCSPMonitoring()

	// Init and restart OCSP responder cache
	s.stopOCSPResponseCache()
	s.initOCSPResponseCache()
	s.startOCSPResponseCache()

	return nil
}

func hasOCSPStatusRequest(cert *x509.Certificate) bool {
	// OID for id-pe-tlsfeature defined in RFC here:
	// https://datatracker.ietf.org/doc/html/rfc7633
	tlsFeatures := asn1.ObjectIdentifier{1, 3, 6, 1, 5, 5, 7, 1, 24}
	const statusRequestExt = 5

	// Example values:
	// * [48 3 2 1 5] - seen when creating own certs locally
	// * [30 3 2 1 5] - seen in the wild
	// Documentation:
	// https://tools.ietf.org/html/rfc6066

	for _, ext := range cert.Extensions {
		if !ext.Id.Equal(tlsFeatures) {
			continue
		}

		var val []int
		rest, err := asn1.Unmarshal(ext.Value, &val)
		if err != nil || len(rest) > 0 {
			return false
		}

		for _, n := range val {
			if n == statusRequestExt {
				return true
			}
		}
		break
	}

	return false
}

// writeOCSPStatus writes an OCSP status to a temporary file then moves it to a
// new path, in an attempt to avoid corrupting existing data.
func (oc *OCSPMonitor) writeOCSPStatus(storeDir, file string, data []byte) error {
	storeDir = filepath.Join(storeDir, defaultOCSPStoreDir)
	tmp, err := os.CreateTemp(storeDir, "tmp-cert-status")
	if err != nil {
		return err
	}

	if _, err := tmp.Write(data); err != nil {
		tmp.Close()
		os.Remove(tmp.Name())
		return err
	}
	if err := tmp.Close(); err != nil {
		return err
	}

	oc.mu.Lock()
	err = os.Rename(tmp.Name(), filepath.Join(storeDir, file))
	oc.mu.Unlock()
	if err != nil {
		os.Remove(tmp.Name())
		return err
	}

	return nil
}

func parseCertPEM(name string) ([]*x509.Certificate, error) {
	data, err := os.ReadFile(name)
	if err != nil {
		return nil, err
	}

	var pemBytes []byte

	var block *pem.Block
	for len(data) != 0 {
		block, data = pem.Decode(data)
		if block == nil {
			break
		}
		if block.Type != "CERTIFICATE" {
			return nil, fmt.Errorf("unexpected PEM certificate type: %s", block.Type)
		}

		pemBytes = append(pemBytes, block.Bytes...)
	}

	return x509.ParseCertificates(pemBytes)
}

// getOCSPIssuerLocally determines a leaf's issuer from locally configured certificates
func getOCSPIssuerLocally(trustedCAs []*x509.Certificate, certBundle []*x509.Certificate) (*x509.Certificate, error) {
	var vOpts x509.VerifyOptions
	var leaf *x509.Certificate
	trustedCAPool := x509.NewCertPool()

	// Require Leaf as first cert in bundle
	if len(certBundle) > 0 {
		leaf = certBundle[0]
	} else {
		return nil, fmt.Errorf("invalid ocsp ca configuration")
	}

	// Allow Issuer to be configured as second cert in bundle
	if len(certBundle) > 1 {
		// The operator may have misconfigured the cert bundle
		issuerCandidate := certBundle[1]
		err := issuerCandidate.CheckSignature(leaf.SignatureAlgorithm, leaf.RawTBSCertificate, leaf.Signature)
		if err != nil {
			return nil, fmt.Errorf("invalid issuer configuration: %w", err)
		} else {
			return issuerCandidate, nil
		}
	}

	// Operator did not provide the Leaf Issuer in cert bundle second position
	// so we will attempt to create at least one ordered verified chain from the
	// trusted CA pool.

	// Specify CA trust store to validator; if unset, system trust store used
	if len(trustedCAs) > 0 {
		for _, ca := range trustedCAs {
			trustedCAPool.AddCert(ca)
		}
		vOpts.Roots = trustedCAPool
	}

	return certstore.GetLeafIssuer(leaf, vOpts), nil
}

// getOCSPIssuer determines an issuer certificate from the cert (bundle) or the file-based CA trust store
func getOCSPIssuer(caFile string, chain [][]byte) (*x509.Certificate, error) {
	var issuer *x509.Certificate
	var trustedCAs []*x509.Certificate
	var certBundle []*x509.Certificate
	var err error

	// FIXME(tgb): extend if pluggable CA store provider added to NATS (i.e. other than PEM file)

	// Non-system default CA trust store passed
	if caFile != _EMPTY_ {
		trustedCAs, err = parseCertPEM(caFile)
		if err != nil {
			return nil, fmt.Errorf("failed to parse ca_file: %v", err)
		}
	}

	// Specify bundled intermediate CA store
	for _, certBytes := range chain {
		cert, err := x509.ParseCertificate(certBytes)
		if err != nil {
			return nil, fmt.Errorf("failed to parse cert: %v", err)
		}
		certBundle = append(certBundle, cert)
	}

	issuer, err = getOCSPIssuerLocally(trustedCAs, certBundle)
	if err != nil || issuer == nil {
		return nil, fmt.Errorf("no issuers found")
	}

	if !issuer.IsCA {
		return nil, fmt.Errorf("%s invalid ca basic constraints: is not ca", issuer.Subject)
	}
	return issuer, nil
}

func ocspStatusString(n int) string {
	switch n {
	case ocsp.Good:
		return "good"
	case ocsp.Revoked:
		return "revoked"
	default:
		return "unknown"
	}
}

func validOCSPResponse(r *ocsp.Response) error {
	// Time validation not handled by ParseResponse.
	// https://tools.ietf.org/html/rfc6960#section-4.2.2.1
	if !r.NextUpdate.IsZero() && r.NextUpdate.Before(time.Now()) {
		t := r.NextUpdate.Format(time.RFC3339Nano)
		return fmt.Errorf("invalid ocsp NextUpdate, is past time: %s", t)
	}
	if r.ThisUpdate.After(time.Now()) {
		t := r.ThisUpdate.Format(time.RFC3339Nano)
		return fmt.Errorf("invalid ocsp ThisUpdate, is future time: %s", t)
	}

	return nil
}
