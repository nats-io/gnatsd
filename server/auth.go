// Copyright 2012-2019 The NATS Authors
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
	"crypto/x509/pkix"
	"encoding/asn1"
	"encoding/base64"
	"fmt"
	"net"
	"regexp"
	"strings"
	"time"

	"github.com/nats-io/jwt/v2"
	"github.com/nats-io/nkeys"
	"golang.org/x/crypto/bcrypt"
)

// Authentication is an interface for implementing authentication
type Authentication interface {
	// Check if a client is authorized to connect
	Check(c ClientAuthentication) bool
}

// ClientAuthentication is an interface for client authentication
type ClientAuthentication interface {
	// Get options associated with a client
	GetOpts() *clientOpts
	// If TLS is enabled, TLS ConnectionState, nil otherwise
	GetTLSConnectionState() *tls.ConnectionState
	// Optionally map a user after auth.
	RegisterUser(*User)
	// RemoteAddress expose the connection information of the client
	RemoteAddress() net.Addr
}

// NkeyUser is for multiple nkey based users
type NkeyUser struct {
	Nkey        string       `json:"user"`
	Permissions *Permissions `json:"permissions,omitempty"`
	Account     *Account     `json:"account,omitempty"`
	SigningKey  string       `json:"signing_key,omitempty"`
}

// User is for multiple accounts/users.
type User struct {
	Username    string       `json:"user"`
	Password    string       `json:"password"`
	Permissions *Permissions `json:"permissions,omitempty"`
	Account     *Account     `json:"account,omitempty"`
}

// clone performs a deep copy of the User struct, returning a new clone with
// all values copied.
func (u *User) clone() *User {
	if u == nil {
		return nil
	}
	clone := &User{}
	*clone = *u
	clone.Permissions = u.Permissions.clone()
	return clone
}

// clone performs a deep copy of the NkeyUser struct, returning a new clone with
// all values copied.
func (n *NkeyUser) clone() *NkeyUser {
	if n == nil {
		return nil
	}
	clone := &NkeyUser{}
	*clone = *n
	clone.Permissions = n.Permissions.clone()
	return clone
}

// SubjectPermission is an individual allow and deny struct for publish
// and subscribe authorizations.
type SubjectPermission struct {
	Allow []string `json:"allow,omitempty"`
	Deny  []string `json:"deny,omitempty"`
}

// ResponsePermission can be used to allow responses to any reply subject
// that is received on a valid subscription.
type ResponsePermission struct {
	MaxMsgs int           `json:"max"`
	Expires time.Duration `json:"ttl"`
}

// Permissions are the allowed subjects on a per
// publish or subscribe basis.
type Permissions struct {
	Publish   *SubjectPermission  `json:"publish"`
	Subscribe *SubjectPermission  `json:"subscribe"`
	Response  *ResponsePermission `json:"responses,omitempty"`
}

// RoutePermissions are similar to user permissions
// but describe what a server can import/export from and to
// another server.
type RoutePermissions struct {
	Import *SubjectPermission `json:"import"`
	Export *SubjectPermission `json:"export"`
}

// clone will clone an individual subject permission.
func (p *SubjectPermission) clone() *SubjectPermission {
	if p == nil {
		return nil
	}
	clone := &SubjectPermission{}
	if p.Allow != nil {
		clone.Allow = make([]string, len(p.Allow))
		copy(clone.Allow, p.Allow)
	}
	if p.Deny != nil {
		clone.Deny = make([]string, len(p.Deny))
		copy(clone.Deny, p.Deny)
	}
	return clone
}

// clone performs a deep copy of the Permissions struct, returning a new clone
// with all values copied.
func (p *Permissions) clone() *Permissions {
	if p == nil {
		return nil
	}
	clone := &Permissions{}
	if p.Publish != nil {
		clone.Publish = p.Publish.clone()
	}
	if p.Subscribe != nil {
		clone.Subscribe = p.Subscribe.clone()
	}
	if p.Response != nil {
		clone.Response = &ResponsePermission{
			MaxMsgs: p.Response.MaxMsgs,
			Expires: p.Response.Expires,
		}
	}
	return clone
}

// checkAuthforWarnings will look for insecure settings and log concerns.
// Lock is assumed held.
func (s *Server) checkAuthforWarnings() {
	warn := false
	if s.opts.Password != "" && !isBcrypt(s.opts.Password) {
		warn = true
	}
	for _, u := range s.users {
		// Skip warn if using TLS certs based auth
		// unless a password has been left in the config.
		if u.Password == "" && s.opts.TLSMap {
			continue
		}

		if !isBcrypt(u.Password) {
			warn = true
			break
		}
	}
	if warn {
		// Warning about using plaintext passwords.
		s.Warnf("Plaintext passwords detected, use nkeys or bcrypt")
	}
}

// If Users or Nkeys options have definitions without an account defined,
// assign them to the default global account.
// Lock should be held.
func (s *Server) assignGlobalAccountToOrphanUsers(nkeys map[string]*NkeyUser, users map[string]*User) {
	for _, u := range users {
		if u.Account == nil {
			u.Account = s.gacc
		}
	}
	for _, u := range nkeys {
		if u.Account == nil {
			u.Account = s.gacc
		}
	}
}

// If the given permissions has a ResponsePermission
// set, ensure that defaults are set (if values are 0)
// and that a Publish permission is set, and Allow
// is disabled if not explicitly set.
func validateResponsePermissions(p *Permissions) {
	if p == nil || p.Response == nil {
		return
	}
	if p.Publish == nil {
		p.Publish = &SubjectPermission{}
	}
	if p.Publish.Allow == nil {
		// We turn off the blanket allow statement.
		p.Publish.Allow = []string{}
	}
	// If there is a response permission, ensure
	// that if value is 0, we set the default value.
	if p.Response.MaxMsgs == 0 {
		p.Response.MaxMsgs = DEFAULT_ALLOW_RESPONSE_MAX_MSGS
	}
	if p.Response.Expires == 0 {
		p.Response.Expires = DEFAULT_ALLOW_RESPONSE_EXPIRATION
	}
}

// configureAuthorization will do any setup needed for authorization.
// Lock is assumed held.
func (s *Server) configureAuthorization() {
	opts := s.getOpts()
	if opts == nil {
		return
	}

	// Check for multiple users first
	// This just checks and sets up the user map if we have multiple users.
	if opts.CustomClientAuthentication != nil {
		s.info.AuthRequired = true
	} else if len(s.trustedKeys) > 0 {
		s.info.AuthRequired = true
	} else if opts.Nkeys != nil || opts.Users != nil {
		s.nkeys, s.users = s.buildNkeysAndUsersFromOptions(opts.Nkeys, opts.Users)
		s.info.AuthRequired = true
	} else if opts.Username != "" || opts.Authorization != "" {
		s.info.AuthRequired = true
	} else {
		s.users = nil
		s.nkeys = nil
		s.info.AuthRequired = false
	}

	// Do similar for websocket config
	s.wsConfigAuth(&opts.Websocket)
}

// Takes the given slices of NkeyUser and User options and build
// corresponding maps used by the server. The users are cloned
// so that server does not reference options.
// The global account is assigned to users that don't have an
// existing account.
// Server lock is held on entry.
func (s *Server) buildNkeysAndUsersFromOptions(nko []*NkeyUser, uo []*User) (map[string]*NkeyUser, map[string]*User) {
	var nkeys map[string]*NkeyUser
	var users map[string]*User

	if len(nko) > 0 {
		nkeys = make(map[string]*NkeyUser, len(nko))
		for _, u := range nko {
			copy := u.clone()
			if u.Account != nil {
				if v, ok := s.accounts.Load(u.Account.Name); ok {
					copy.Account = v.(*Account)
				}
			}
			if copy.Permissions != nil {
				validateResponsePermissions(copy.Permissions)
			}
			nkeys[u.Nkey] = copy
		}
	}
	if len(uo) > 0 {
		users = make(map[string]*User, len(uo))
		for _, u := range uo {
			copy := u.clone()
			if u.Account != nil {
				if v, ok := s.accounts.Load(u.Account.Name); ok {
					copy.Account = v.(*Account)
				}
			}
			if copy.Permissions != nil {
				validateResponsePermissions(copy.Permissions)
			}
			users[u.Username] = copy
		}
	}
	s.assignGlobalAccountToOrphanUsers(nkeys, users)
	return nkeys, users
}

// checkAuthentication will check based on client type and
// return boolean indicating if client is authorized.
func (s *Server) checkAuthentication(c *client) bool {
	switch c.kind {
	case CLIENT:
		return s.isClientAuthorized(c)
	case ROUTER:
		return s.isRouterAuthorized(c)
	case GATEWAY:
		return s.isGatewayAuthorized(c)
	case LEAF:
		return s.isLeafNodeAuthorized(c)
	default:
		return false
	}
}

// isClientAuthorized will check the client against the proper authorization method and data.
// This could be nkey, token, or username/password based.
func (s *Server) isClientAuthorized(c *client) bool {
	opts := s.getOpts()

	// Check custom auth first, then jwts, then nkeys, then
	// multiple users with TLS map if enabled, then token,
	// then single user/pass.
	if opts.CustomClientAuthentication != nil {
		return opts.CustomClientAuthentication.Check(c)
	}

	return s.processClientOrLeafAuthentication(c, opts)
}

type authOpts struct {
	username   string
	password   string
	token      string
	noAuthUser string
	tlsMap     bool
	users      map[string]*User
	nkeys      map[string]*NkeyUser
}

func (s *Server) getAuthOpts(c *client, o *Options, auth *authOpts) bool {
	// c.ws is immutable, but may need lock if we get race reports.
	wsClient := c.ws != nil

	authRequired := s.info.AuthRequired
	// For websocket clients, if there is no top-level auth, then we
	// check for websocket specifically.
	if !authRequired && wsClient {
		authRequired = s.websocket.authRequired
	}
	if !authRequired {
		return false
	}
	auth.noAuthUser = o.NoAuthUser
	auth.tlsMap = o.TLSMap
	if wsClient {
		wo := &o.Websocket
		// If those are specified, override, regardless if there is
		// auth configuration (like user/pwd, etc..) in websocket section.
		if wo.NoAuthUser != _EMPTY_ {
			auth.noAuthUser = wo.NoAuthUser
		}
		if wo.TLSMap {
			auth.tlsMap = true
		}
		// Now check for websocket auth specific override
		if s.websocket.authRequired {
			auth.username = wo.Username
			auth.password = wo.Password
			auth.token = wo.Token
			auth.users = s.websocket.users
			auth.nkeys = s.websocket.nkeys
			return true
		}
		// else fallback to regular auth config
	}
	auth.username = o.Username
	auth.password = o.Password
	auth.token = o.Authorization
	auth.users = s.users
	auth.nkeys = s.nkeys
	return true
}

func (s *Server) processClientOrLeafAuthentication(c *client, opts *Options) bool {
	var (
		nkey *NkeyUser
		juc  *jwt.UserClaims
		acc  *Account
		user *User
		ok   bool
		err  error
		auth authOpts
	)

	s.mu.Lock()
	authRequired := s.getAuthOpts(c, opts, &auth)
	if !authRequired {
		// TODO(dlc) - If they send us credentials should we fail?
		s.mu.Unlock()
		return true
	}

	// Check if we have trustedKeys defined in the server. If so we require a user jwt.
	if s.trustedKeys != nil {
		if c.opts.JWT == "" && (c.opts.Nkey == "" || s.opts.SystemAccount == "") {
			s.mu.Unlock()
			c.Debugf("Authentication requires a user JWT")
			return false
		}
		if c.opts.JWT != "" {
			// So we have a valid user jwt here.
			juc, err = jwt.DecodeUserClaims(c.opts.JWT)
			if err != nil {
				s.mu.Unlock()
				c.Debugf("User JWT not valid: %v", err)
				return false
			}
			vr := jwt.CreateValidationResults()
			juc.Validate(vr)
			if vr.IsBlocking(true) {
				s.mu.Unlock()
				c.Debugf("User JWT no longer valid: %+v", vr)
				return false
			}
		}
	}

	// Check if we have nkeys or users for client.
	hasNkeys := auth.nkeys != nil
	hasUsers := auth.users != nil
	if hasNkeys && c.opts.Nkey != "" {
		nkey, ok = auth.nkeys[c.opts.Nkey]
		if !ok && s.opts.SystemAccount == "" {
			s.mu.Unlock()
			return false
		}
	} else if hasUsers {
		// Check if we are tls verify and are mapping users from the client_certificate
		if auth.tlsMap {
			var euser string
			authorized := checkClientTLSCertSubject(c, func(u string) bool {
				var ok bool
				user, ok = auth.users[u]
				if !ok {
					c.Debugf("User in cert [%q], not found", u)
					return false
				}
				euser = u
				return true
			})
			if !authorized {
				s.mu.Unlock()
				return false
			}
			if c.opts.Username != "" {
				s.Warnf("User found in connect proto, but user required from cert - %v", c)
			}
			// Already checked that the client didn't send a user in connect
			// but we set it here to be able to identify it in the logs.
			c.opts.Username = euser
		} else {
			if c.kind == CLIENT && c.opts.Username == "" && auth.noAuthUser != "" {
				if u, exists := auth.users[auth.noAuthUser]; exists {
					c.opts.Username = u.Username
					c.opts.Password = u.Password
				}
			}
			if c.opts.Username != "" {
				user, ok = auth.users[c.opts.Username]
				if !ok {
					s.mu.Unlock()
					return false
				}
			}
		}
	}
	s.mu.Unlock()

	// If we have a jwt and a userClaim, make sure we have the Account, etc associated.
	// We need to look up the account. This will use an account resolver if one is present.
	if juc != nil {
		issuer := juc.Issuer
		if juc.IssuerAccount != "" {
			issuer = juc.IssuerAccount
		}
		if acc, err = s.LookupAccount(issuer); acc == nil {
			c.Debugf("Account JWT lookup error: %v", err)
			return false
		}
		if !s.isTrustedIssuer(acc.Issuer) {
			c.Debugf("Account JWT not signed by trusted operator")
			return false
		}
		if juc.IssuerAccount != "" && !acc.hasIssuer(juc.Issuer) {
			c.Debugf("User JWT issuer is not known")
			return false
		}
		if acc.IsExpired() {
			c.Debugf("Account JWT has expired")
			return false
		}
		// skip validation of nonce when presented with a bearer token
		// FIXME: if BearerToken is only for WSS, need check for server with that port enabled
		if !juc.BearerToken {
			// Verify the signature against the nonce.
			if c.opts.Sig == "" {
				c.Debugf("Signature missing")
				return false
			}
			sig, err := base64.RawURLEncoding.DecodeString(c.opts.Sig)
			if err != nil {
				// Allow fallback to normal base64.
				sig, err = base64.StdEncoding.DecodeString(c.opts.Sig)
				if err != nil {
					c.Debugf("Signature not valid base64")
					return false
				}
			}
			pub, err := nkeys.FromPublicKey(juc.Subject)
			if err != nil {
				c.Debugf("User nkey not valid: %v", err)
				return false
			}
			if err := pub.Verify(c.nonce, sig); err != nil {
				c.Debugf("Signature not verified")
				return false
			}
		}
		if acc.checkUserRevoked(juc.Subject) {
			c.Debugf("User authentication revoked")
			return false
		}

		nkey = buildInternalNkeyUser(juc, acc)
		if err := c.RegisterNkeyUser(nkey); err != nil {
			return false
		}
		// Hold onto the user's public key.
		c.pubKey = juc.Subject

		// Generate an event if we have a system account.
		s.accountConnectEvent(c)

		// Check if we need to set an auth timer if the user jwt expires.
		c.checkExpiration(juc.Claims())
		return true
	}

	if nkey == nil && c.kind == CLIENT && c.opts.Nkey != "" && s.opts.SystemAccount != "" {
		var nkeyAcc *Account
		if acc, ok := s.accounts.Load(c.opts.Nkey); ok {
			nkeyAcc = acc.(*Account)
			c.Debugf("Found Generated Account: %s", nkeyAcc.Name)
		} else {
			if sAcc, err := s.lookupAccount(s.opts.SystemAccount); err != nil {
				c.Debugf("Account JWT lookup error: %v", err)
				return false
			} else if jwtClaim, _, err := s.verifyAccountClaims(sAcc.claimJWT); err != nil {
				c.Debugf("Account JWT verification failed: %v", err)
				return false
			} else {
				jwtClaim.Name = c.opts.Nkey
				jwtClaim.Subject = c.opts.Nkey
				claimJwt := jwtClaim.String()
				nkeyAcc = s.buildInternalAccount(jwtClaim)
				nkeyAcc.claimJWT = claimJwt
				if acc := s.registerAccount(nkeyAcc); acc != nil {
					err = s.updateAccountWithClaimJWT(acc, claimJwt)
					if err != nil && err != ErrAccountResolverSameClaims {
						c.Debugf("Account JWT claim update failed: %v", err)
						return false
					}
					nkeyAcc = acc
				}
				c.Debugf("Generated Account: %s", nkeyAcc.Name)
			}
		}
		if nkeyAcc != nil {
			nkey = &NkeyUser{Nkey: c.opts.Nkey, Account: nkeyAcc,
				Permissions: nkeyAcc.defaultPerms.clone()}
		}
	}

	if nkey != nil {
		if c.opts.Sig == "" {
			c.Debugf("Signature missing")
			return false
		}
		sig, err := base64.RawURLEncoding.DecodeString(c.opts.Sig)
		if err != nil {
			// Allow fallback to normal base64.
			sig, err = base64.StdEncoding.DecodeString(c.opts.Sig)
			if err != nil {
				c.Debugf("Signature not valid base64")
				return false
			}
		}
		pub, err := nkeys.FromPublicKey(c.opts.Nkey)
		if err != nil {
			c.Debugf("User nkey not valid: %v", err)
			return false
		}
		if err := pub.Verify(c.nonce, sig); err != nil {
			c.Debugf("Signature not verified")
			return false
		}
		if err := c.RegisterNkeyUser(nkey); err != nil {
			return false
		}
		return true
	}

	if user != nil {
		ok = comparePasswords(user.Password, c.opts.Password)
		// If we are authorized, register the user which will properly setup any permissions
		// for pub/sub authorizations.
		if ok {
			c.RegisterUser(user)
			// Generate an event if we have a system account and this is not the $G account.
			s.accountConnectEvent(c)
		}
		return ok
	}

	if c.kind == CLIENT {
		if auth.token != "" {
			return comparePasswords(auth.token, c.opts.Token)
		} else if auth.username != "" {
			if auth.username != c.opts.Username {
				return false
			}
			return comparePasswords(auth.password, c.opts.Password)
		}
	} else if c.kind == LEAF {
		// There is no required username/password to connect and
		// there was no u/p in the CONNECT or none that matches the
		// know users. Register the leaf connection with global account
		// or the one specified in config (if provided).
		return s.registerLeafWithAccount(c, opts.LeafNode.Account)
	}

	return false
}

func getTLSAuthDCs(rdns *pkix.RDNSequence) string {
	dcOID := asn1.ObjectIdentifier{0, 9, 2342, 19200300, 100, 1, 25}
	dcs := []string{}
	for _, rdn := range *rdns {
		if len(rdn) == 0 {
			continue
		}
		for _, atv := range rdn {
			value, ok := atv.Value.(string)
			if !ok {
				continue
			}
			if atv.Type.Equal(dcOID) {
				dcs = append(dcs, "DC="+value)
			}
		}
	}
	return strings.Join(dcs, ",")
}

func checkClientTLSCertSubject(c *client, fn func(string) bool) bool {
	tlsState := c.GetTLSConnectionState()
	if tlsState == nil {
		c.Debugf("User required in cert, no TLS connection state")
		return false
	}
	if len(tlsState.PeerCertificates) == 0 {
		c.Debugf("User required in cert, no peer certificates found")
		return false
	}
	cert := tlsState.PeerCertificates[0]
	if len(tlsState.PeerCertificates) > 1 {
		c.Debugf("Multiple peer certificates found, selecting first")
	}

	hasSANs := len(cert.DNSNames) > 0
	hasEmailAddresses := len(cert.EmailAddresses) > 0
	hasSubject := len(cert.Subject.String()) > 0
	hasURIs := len(cert.URIs) > 0
	if !hasEmailAddresses && !hasSubject && !hasURIs {
		c.Debugf("User required in cert, none found")
		return false
	}

	switch {
	case hasEmailAddresses:
		for _, u := range cert.EmailAddresses {
			if fn(u) {
				c.Debugf("Using email found in cert for auth [%q]", u)
				return true
			}
		}
		fallthrough
	case hasSANs:
		for _, u := range cert.DNSNames {
			if fn(u) {
				c.Debugf("Using SAN found in cert for auth [%q]", u)
				return true
			}
		}
	case hasURIs:
		for _, u := range cert.URIs {
			if fn(u.String()) {
				c.Debugf("Using URI found in cert for auth [%q]", u)
				return true
			}
		}
	}

	// Try to get the full RDN Sequence that includes the domain components.
	var rdns pkix.RDNSequence
	if _, err := asn1.Unmarshal(cert.RawSubject, &rdns); err == nil {
		// If found domain components then include roughly following
		// the order from https://tools.ietf.org/html/rfc2253
		rdn := cert.Subject.ToRDNSequence().String()
		dcs := getTLSAuthDCs(&rdns)
		if len(dcs) > 0 {
			u := strings.Join([]string{rdn, dcs}, ",")
			if fn(u) {
				c.Debugf("Using RDNSequence for auth [%q]", u)
				return true
			}
		}
	}

	// Use the subject of the certificate.
	u := cert.Subject.String()
	c.Debugf("Using certificate subject for auth [%q]", u)
	return fn(u)
}

// checkRouterAuth checks optional router authorization which can be nil or username/password.
func (s *Server) isRouterAuthorized(c *client) bool {
	// Snapshot server options.
	opts := s.getOpts()

	// Check custom auth first, then TLS map if enabled
	// then single user/pass.
	if s.opts.CustomRouterAuthentication != nil {
		return s.opts.CustomRouterAuthentication.Check(c)
	}

	if opts.Cluster.Username == "" {
		return true
	}

	if opts.Cluster.TLSMap {
		return checkClientTLSCertSubject(c, func(user string) bool {
			return opts.Cluster.Username == user
		})
	}

	if opts.Cluster.Username != c.opts.Username {
		return false
	}
	if !comparePasswords(opts.Cluster.Password, c.opts.Password) {
		return false
	}
	return true
}

// isGatewayAuthorized checks optional gateway authorization which can be nil or username/password.
func (s *Server) isGatewayAuthorized(c *client) bool {
	// Snapshot server options.
	opts := s.getOpts()
	if opts.Gateway.Username == "" {
		return true
	}

	// Check whether TLS map is enabled, otherwise use single user/pass.
	if opts.Gateway.TLSMap {
		return checkClientTLSCertSubject(c, func(user string) bool {
			return opts.Gateway.Username == user
		})
	}

	if opts.Gateway.Username != c.opts.Username {
		return false
	}
	return comparePasswords(opts.Gateway.Password, c.opts.Password)
}

func (s *Server) registerLeafWithAccount(c *client, account string) bool {
	var err error
	acc := s.globalAccount()
	if account != _EMPTY_ {
		acc, err = s.lookupAccount(account)
		if err != nil {
			s.Errorf("authentication of user %q failed, unable to lookup account %q: %v",
				c.opts.Username, account, err)
			return false
		}
	}
	if err = c.registerWithAccount(acc); err != nil {
		return false
	}
	return true
}

// isLeafNodeAuthorized will check for auth for an inbound leaf node connection.
func (s *Server) isLeafNodeAuthorized(c *client) bool {
	opts := s.getOpts()

	isAuthorized := func(username, password, account string) bool {
		if username != c.opts.Username {
			return false
		}
		if !comparePasswords(password, c.opts.Password) {
			return false
		}
		return s.registerLeafWithAccount(c, account)
	}

	// If leafnodes config has an authorization{} stanza, this takes precedence.
	// The user in CONNECT mutch match. We will bind to the account associated
	// with that user (from the leafnode's authorization{} config).
	if opts.LeafNode.Username != _EMPTY_ {
		return isAuthorized(opts.LeafNode.Username, opts.LeafNode.Password, opts.LeafNode.Account)
	} else if len(opts.LeafNode.Users) > 0 {
		// This is expected to be a very small array.
		for _, u := range opts.LeafNode.Users {
			if u.Username == c.opts.Username {
				var accName string
				if u.Account != nil {
					accName = u.Account.Name
				}
				return isAuthorized(u.Username, u.Password, accName)
			}
		}
		return false
	}

	// We are here if we accept leafnode connections without any credential.

	// Still, if the CONNECT has some user info, we will bind to the
	// user's account or to the specified default account (if provided)
	// or to the global account.
	return s.processClientOrLeafAuthentication(c, opts)
}

// Support for bcrypt stored passwords and tokens.
var validBcryptPrefix = regexp.MustCompile(`^\$2[a,b,x,y]{1}\$\d{2}\$.*`)

// isBcrypt checks whether the given password or token is bcrypted.
func isBcrypt(password string) bool {
	if strings.HasPrefix(password, "$") {
		return validBcryptPrefix.MatchString(password)
	}

	return false
}

func comparePasswords(serverPassword, clientPassword string) bool {
	// Check to see if the server password is a bcrypt hash
	if isBcrypt(serverPassword) {
		if err := bcrypt.CompareHashAndPassword([]byte(serverPassword), []byte(clientPassword)); err != nil {
			return false
		}
	} else if serverPassword != clientPassword {
		return false
	}
	return true
}

func validateAuth(o *Options) error {
	if o.NoAuthUser == "" {
		return nil
	}
	if len(o.TrustedOperators) > 0 {
		return fmt.Errorf("no_auth_user not compatible with Trusted Operator")
	}
	if o.Users == nil {
		return fmt.Errorf(`no_auth_user: "%s" present, but users are not defined`, o.NoAuthUser)
	}
	for _, u := range o.Users {
		if u.Username == o.NoAuthUser {
			return nil
		}
	}
	return fmt.Errorf(
		`no_auth_user: "%s" not present as user in authorization block or account configuration`,
		o.NoAuthUser)
}
