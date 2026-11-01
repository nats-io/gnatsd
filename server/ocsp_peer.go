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

package server

import (
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"strings"
	"time"

	"golang.org/x/crypto/ocsp"

	"github.com/nats-io/nats-server/v2/server/certidp"
)

func parseOCSPPeer(v any) (pcfg *certidp.OCSPPeerConfig, retError error) {
	var lt token
	defer convertPanicToError(&lt, &retError)
	tk, v := unwrapValue(v, &lt)
	cm, ok := v.(map[string]any)
	if !ok {
		return nil, &configErr{tk, fmt.Sprintf(certidp.ErrIllegalPeerOptsConfig, v)}
	}
	pcfg = certidp.NewOCSPPeerConfig()
	retError = nil
	for mk, mv := range cm {
		tk, mv = unwrapValue(mv, &lt)
		switch strings.ToLower(mk) {
		case "verify":
			verify, ok := mv.(bool)
			if !ok {
				return nil, &configErr{tk, fmt.Sprintf(certidp.ErrParsingPeerOptFieldGeneric, mk)}
			}
			pcfg.Verify = verify
		case "allowed_clockskew":
			at := float64(0)
			switch mv := mv.(type) {
			case int64:
				at = float64(mv)
			case float64:
				at = mv
			case string:
				d, err := time.ParseDuration(mv)
				if err != nil {
					return nil, &configErr{tk, fmt.Sprintf(certidp.ErrParsingPeerOptFieldTypeConversion, "unexpected type")}
				}
				at = d.Seconds()
			default:
				return nil, &configErr{tk, fmt.Sprintf(certidp.ErrParsingPeerOptFieldTypeConversion, "unexpected type")}
			}
			if at >= 0 {
				pcfg.ClockSkew = at
			}
		case "ca_timeout":
			at := float64(0)
			switch mv := mv.(type) {
			case int64:
				at = float64(mv)
			case float64:
				at = mv
			case string:
				d, err := time.ParseDuration(mv)
				if err != nil {
					return nil, &configErr{tk, fmt.Sprintf(certidp.ErrParsingPeerOptFieldTypeConversion, err)}
				}
				at = d.Seconds()
			default:
				return nil, &configErr{tk, fmt.Sprintf(certidp.ErrParsingPeerOptFieldTypeConversion, "unexpected type")}
			}
			if at >= 0 {
				pcfg.Timeout = at
			}
		case "cache_ttl_when_next_update_unset":
			at := float64(0)
			switch mv := mv.(type) {
			case int64:
				at = float64(mv)
			case float64:
				at = mv
			case string:
				d, err := time.ParseDuration(mv)
				if err != nil {
					return nil, &configErr{tk, fmt.Sprintf(certidp.ErrParsingPeerOptFieldTypeConversion, err)}
				}
				at = d.Seconds()
			default:
				return nil, &configErr{tk, fmt.Sprintf(certidp.ErrParsingPeerOptFieldTypeConversion, "unexpected type")}
			}
			if at >= 0 {
				pcfg.TTLUnsetNextUpdate = at
			}
		case "warn_only":
			warnOnly, ok := mv.(bool)
			if !ok {
				return nil, &configErr{tk, fmt.Sprintf(certidp.ErrParsingPeerOptFieldGeneric, mk)}
			}
			pcfg.WarnOnly = warnOnly
		case "unknown_is_good":
			unknownIsGood, ok := mv.(bool)
			if !ok {
				return nil, &configErr{tk, fmt.Sprintf(certidp.ErrParsingPeerOptFieldGeneric, mk)}
			}
			pcfg.UnknownIsGood = unknownIsGood
		case "allow_when_ca_unreachable":
			allowWhenCAUnreachable, ok := mv.(bool)
			if !ok {
				return nil, &configErr{tk, fmt.Sprintf(certidp.ErrParsingPeerOptFieldGeneric, mk)}
			}
			pcfg.AllowWhenCAUnreachable = allowWhenCAUnreachable
		default:
			return nil, &configErr{tk, fmt.Sprintf(certidp.ErrParsingPeerOptFieldGeneric, mk)}
		}
	}
	return pcfg, nil
}

func peerFromVerifiedChains(chains [][]*x509.Certificate) *x509.Certificate {
	if len(chains) == 0 || len(chains[0]) == 0 {
		return nil
	}
	return chains[0][0]
}

// plugTLSOCSPPeer will plug the TLS handshake lifecycle for client mTLS connections and Leaf connections
func (s *Server) plugTLSOCSPPeer(config *tlsConfigKind) (*tls.Config, bool, error) {
	if config == nil || config.tlsConfig == nil {
		return nil, false, errors.New(certidp.ErrUnableToPlugTLSEmptyConfig)
	}
	kind := config.kind
	isSpoke := config.isLeafSpoke
	tcOpts := config.tlsOpts
	if tcOpts == nil || tcOpts.OCSPPeerConfig == nil || !tcOpts.OCSPPeerConfig.Verify {
		return nil, false, nil
	}
	s.Debugf(certidp.DbgPlugTLSForKind, config.kind)
	// peer is a tls client
	if kind == kindStringMap[CLIENT] || (kind == kindStringMap[LEAF] && !isSpoke) {
		if !tcOpts.Verify {
			return nil, false, errors.New(certidp.ErrMTLSRequired)
		}
		return s.plugClientTLSOCSPPeer(config)
	}
	// peer is a tls server
	if kind == kindStringMap[LEAF] && isSpoke {
		return s.plugServerTLSOCSPPeer(config)
	}
	return nil, false, nil
}

func (s *Server) plugClientTLSOCSPPeer(config *tlsConfigKind) (*tls.Config, bool, error) {
	if config == nil || config.tlsConfig == nil || config.tlsOpts == nil {
		return nil, false, errors.New(certidp.ErrUnableToPlugTLSClient)
	}
	tc := config.tlsConfig
	tcOpts := config.tlsOpts
	kind := config.kind
	if tcOpts.OCSPPeerConfig == nil || !tcOpts.OCSPPeerConfig.Verify {
		return tc, false, nil
	}
	tc.VerifyConnection = func(cs tls.ConnectionState) error {
		if !s.tlsClientOCSPValid(cs.VerifiedChains, tcOpts.OCSPPeerConfig) {
			s.sendOCSPPeerRejectEvent(kind, peerFromVerifiedChains(cs.VerifiedChains), certidp.MsgTLSClientRejectConnection)
			return errors.New(certidp.MsgTLSClientRejectConnection)
		}
		return nil
	}
	return tc, true, nil
}

func (s *Server) plugServerTLSOCSPPeer(config *tlsConfigKind) (*tls.Config, bool, error) {
	if config == nil || config.tlsConfig == nil || config.tlsOpts == nil {
		return nil, false, errors.New(certidp.ErrUnableToPlugTLSServer)
	}
	tc := config.tlsConfig
	tcOpts := config.tlsOpts
	kind := config.kind
	if tcOpts.OCSPPeerConfig == nil || !tcOpts.OCSPPeerConfig.Verify {
		return tc, false, nil
	}
	tc.VerifyConnection = func(cs tls.ConnectionState) error {
		if !s.tlsServerOCSPValid(cs.VerifiedChains, tcOpts.OCSPPeerConfig) {
			s.sendOCSPPeerRejectEvent(kind, peerFromVerifiedChains(cs.VerifiedChains), certidp.MsgTLSServerRejectConnection)
			return errors.New(certidp.MsgTLSServerRejectConnection)
		}
		return nil
	}
	return tc, true, nil
}

// tlsServerOCSPValid evaluates verified chains (post successful TLS handshake) against OCSP
// eligibility. A verified chain is considered OCSP Valid if either none of the links are
// OCSP eligible, or current "good" responses from the CA can be obtained for each eligible link.
// Upon first OCSP Valid chain found, the Server is deemed OCSP Valid. If none of the chains are
// OCSP Valid, the Server is deemed OCSP Invalid. A verified self-signed certificate (chain length 1)
// is also considered OCSP Valid.
func (s *Server) tlsServerOCSPValid(chains [][]*x509.Certificate, opts *certidp.OCSPPeerConfig) bool {
	s.Debugf(certidp.DbgNumServerChains, len(chains))
	return s.peerOCSPValid(chains, opts)
}

// tlsClientOCSPValid evaluates verified chains (post successful TLS handshake) against OCSP
// eligibility. A verified chain is considered OCSP Valid if either none of the links are
// OCSP eligible, or current "good" responses from the CA can be obtained for each eligible link.
// Upon first OCSP Valid chain found, the Client is deemed OCSP Valid. If none of the chains are
// OCSP Valid, the Client is deemed OCSP Invalid. A verified self-signed certificate (chain length 1)
// is also considered OCSP Valid.
func (s *Server) tlsClientOCSPValid(chains [][]*x509.Certificate, opts *certidp.OCSPPeerConfig) bool {
	s.Debugf(certidp.DbgNumClientChains, len(chains))
	return s.peerOCSPValid(chains, opts)
}

func (s *Server) peerOCSPValid(chains [][]*x509.Certificate, opts *certidp.OCSPPeerConfig) bool {
	peer := peerFromVerifiedChains(chains)
	if peer == nil {
		s.Errorf(certidp.ErrPeerEmptyAutoReject)
		return false
	}
	for ci, chain := range chains {
		s.Debugf(certidp.DbgLinksInChain, ci, len(chain))
		// Self-signed certificate is Client OCSP Valid (no CA)
		if len(chain) == 1 {
			s.Debugf(certidp.DbgSelfSignedValid, ci)
			return true
		}
		// Check if any of the links in the chain are OCSP eligible
		chainEligible := false
		var eligibleLinks []*certidp.ChainLink
		// Iterate over links skipping the root cert which is not OCSP eligible (self == issuer)
		for linkPos := 0; linkPos < len(chain)-1; linkPos++ {
			cert := chain[linkPos]
			link := &certidp.ChainLink{
				Leaf: cert,
			}
			if certidp.CertOCSPEligible(link) {
				chainEligible = true
				issuerCert := certidp.GetLeafIssuerCert(chain, linkPos)
				if issuerCert == nil {
					// unexpected chain condition, reject Client as OCSP Invalid
					return false
				}
				link.Issuer = issuerCert
				eligibleLinks = append(eligibleLinks, link)
			}
		}
		// A trust-store verified chain that is not OCSP eligible is always OCSP Valid
		if !chainEligible {
			s.Debugf(certidp.DbgValidNonOCSPChain, ci)
			return true
		}
		s.Debugf(certidp.DbgChainIsOCSPEligible, ci, len(eligibleLinks))
		// Chain has at least one OCSP eligible link, so check each eligible link;
		// any link with a !good OCSP response chain OCSP Invalid
		chainValid := true
		for _, link := range eligibleLinks {
			// if option selected, good could reflect either ocsp.Good or ocsp.Unknown
			if badReason, good := s.certOCSPGood(link, opts); !good {
				s.Debugf(badReason)
				s.sendOCSPPeerChainlinkInvalidEvent(peer, link.Leaf, badReason)
				chainValid = false
				break
			}
		}
		if chainValid {
			s.Debugf(certidp.DbgChainIsOCSPValid, ci)
			return true
		}
	}
	// If we are here, all chains had OCSP eligible links, but none of the chains achieved OCSP valid
	s.Debugf(certidp.DbgNoOCSPValidChains)
	return false
}

func (s *Server) certOCSPGood(link *certidp.ChainLink, opts *certidp.OCSPPeerConfig) (string, bool) {
	if link == nil || link.Leaf == nil || link.Issuer == nil || link.OCSPWebEndpoints == nil || len(*link.OCSPWebEndpoints) < 1 {
		return "Empty chainlink found", false
	}
	var err error
	sLogs := &certidp.Log{
		Debugf:  s.Debugf,
		Noticef: s.Noticef,
		Warnf:   s.Warnf,
		Errorf:  s.Errorf,
		Tracef:  s.Tracef,
	}
	fingerprint := certidp.GenerateFingerprint(link.Leaf)
	// Used for debug/operator only, not match
	subj := certidp.GetSubjectDNForm(link.Leaf)
	var rawResp []byte
	var ocspr *ocsp.Response
	var useCachedResp bool
	var rc = s.ocsprc
	var cachedRevocation bool
	// Check our cache before calling out to the CA OCSP responder
	s.Debugf(certidp.DbgCheckingCacheForCert, subj, fingerprint)
	if rawResp = rc.Get(fingerprint, sLogs); len(rawResp) > 0 {
		// Signature and serial number validation of CA's OCSP response occurs in ParseResponseForCert
		ocspr, err = ocsp.ParseResponseForCert(rawResp, link.Leaf, link.Issuer)
		if err == nil && ocspr != nil {
			// Check if OCSP Response delegation present and if so is valid
			if !certidp.ValidDelegationCheck(link.Issuer, ocspr) {
				// Invalid delegation was already in cache, purge it and don't use it
				s.Debugf(certidp.MsgCachedOCSPResponseInvalid, subj, fingerprint)
				rc.Delete(fingerprint, true, sLogs)
				goto AFTERCACHE
			}
			if certidp.OCSPResponseCurrent(ocspr, opts, sLogs) {
				s.Debugf(certidp.DbgCurrentResponseCached, certidp.GetStatusAssertionStr(ocspr.Status))
				useCachedResp = true
			} else {
				// Cached response is not current, delete it and tidy runtime stats to reflect a miss;
				// if preserve_revoked is enabled, the cache will not delete the cached response
				s.Debugf(certidp.DbgExpiredResponseCached, certidp.GetStatusAssertionStr(ocspr.Status))
				rc.Delete(fingerprint, true, sLogs)
			}
			// Regardless of currency, record a cached revocation found in case AllowWhenCAUnreachable is set
			if ocspr.Status == ocsp.Revoked {
				cachedRevocation = true
			}
		} else {
			// Bogus cached assertion, purge it and don't use it
			s.Debugf(certidp.MsgCachedOCSPResponseInvalid, subj, fingerprint)
			rc.Delete(fingerprint, true, sLogs)
			goto AFTERCACHE
		}
	}
AFTERCACHE:
	if !useCachedResp {
		// CA OCSP responder callout needed
		rawResp, err = certidp.FetchOCSPResponse(link, opts, sLogs)
		if err != nil || rawResp == nil || len(rawResp) == 0 {
			s.Warnf(certidp.ErrCAResponderCalloutFail, subj, err)
			if opts.WarnOnly {
				s.Warnf(certidp.MsgAllowWarnOnlyOccurred, subj)
				return _EMPTY_, true
			}
			if opts.AllowWhenCAUnreachable && !cachedRevocation {
				// Link has no cached history of revocation, so allow it to pass
				s.Warnf(certidp.MsgAllowWhenCAUnreachableOccurred, subj)
				return _EMPTY_, true
			} else if opts.AllowWhenCAUnreachable {
				// Link has cached but expired revocation so reject when CA is unreachable
				s.Warnf(certidp.MsgAllowWhenCAUnreachableOccurredCachedRevoke, subj)
			}
			return certidp.MsgFailedOCSPResponseFetch, false
		}
		// Signature and serial number validation of CA's OCSP response occurs in ParseResponseForCert
		ocspr, err = ocsp.ParseResponseForCert(rawResp, link.Leaf, link.Issuer)
		if err == nil && ocspr != nil {
			// Check if OCSP Response delegation present and if so is valid
			if !certidp.ValidDelegationCheck(link.Issuer, ocspr) {
				s.Warnf(certidp.MsgOCSPResponseDelegationInvalid, subj)
				if opts.WarnOnly {
					// Can't use bogus assertion, but warn-only set so allow link to pass
					s.Warnf(certidp.MsgAllowWarnOnlyOccurred, subj)
					return _EMPTY_, true
				}
				return fmt.Sprintf(certidp.MsgOCSPResponseDelegationInvalid, subj), false
			}
			if !certidp.OCSPResponseCurrent(ocspr, opts, sLogs) {
				s.Warnf(certidp.ErrNewCAResponseNotCurrent, subj)
				if opts.WarnOnly {
					// Can't use non-effective assertion, but warn-only set so allow link to pass
					s.Warnf(certidp.MsgAllowWarnOnlyOccurred, subj)
					return _EMPTY_, true
				}
				return certidp.MsgOCSPResponseNotEffective, false
			}
		} else {
			s.Errorf(certidp.ErrCAResponseParseFailed, subj, err)
			if opts.WarnOnly {
				// Can't use bogus assertion, but warn-only set so allow link to pass
				s.Warnf(certidp.MsgAllowWarnOnlyOccurred, subj)
				return _EMPTY_, true
			}
			return certidp.MsgFailedOCSPResponseParse, false
		}
		// cache the valid fetched CA OCSP Response
		rc.Put(fingerprint, ocspr, subj, sLogs)
	}

	// Whether through valid cache response available or newly fetched valid response, now check the status
	if ocspr.Status == ocsp.Revoked || (ocspr.Status == ocsp.Unknown && !opts.UnknownIsGood) {
		s.Warnf(certidp.ErrOCSPInvalidPeerLink, subj, certidp.GetStatusAssertionStr(ocspr.Status))
		if opts.WarnOnly {
			s.Warnf(certidp.MsgAllowWarnOnlyOccurred, subj)
			return _EMPTY_, true
		}
		return fmt.Sprintf(certidp.MsgOCSPResponseInvalidStatus, certidp.GetStatusAssertionStr(ocspr.Status)), false
	}
	s.Debugf(certidp.DbgOCSPValidPeerLink, subj)
	return _EMPTY_, true
}
