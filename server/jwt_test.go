// Copyright 2018-2020 The NATS Authors
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
	"bufio"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nats-io/jwt/v2"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nkeys"
)

var (
	// This matches ./configs/nkeys_jwts/test.seed
	oSeed = []byte("SOAFYNORQLQFJYBYNUGC5D7SH2MXMUX5BFEWWGHN3EK4VGG5TPT5DZP7QU")
	// This matches ./configs/nkeys/op.jwt
	ojwt = "eyJ0eXAiOiJqd3QiLCJhbGciOiJlZDI1NTE5In0.eyJhdWQiOiJURVNUUyIsImV4cCI6MTg1OTEyMTI3NSwianRpIjoiWE5MWjZYWVBIVE1ESlFSTlFPSFVPSlFHV0NVN01JNVc1SlhDWk5YQllVS0VRVzY3STI1USIsImlhdCI6MTU0Mzc2MTI3NSwiaXNzIjoiT0NBVDMzTVRWVTJWVU9JTUdOR1VOWEo2NkFIMlJMU0RBRjNNVUJDWUFZNVFNSUw2NU5RTTZYUUciLCJuYW1lIjoiU3luYWRpYSBDb21tdW5pY2F0aW9ucyBJbmMuIiwibmJmIjoxNTQzNzYxMjc1LCJzdWIiOiJPQ0FUMzNNVFZVMlZVT0lNR05HVU5YSjY2QUgyUkxTREFGM01VQkNZQVk1UU1JTDY1TlFNNlhRRyIsInR5cGUiOiJvcGVyYXRvciIsIm5hdHMiOnsic2lnbmluZ19rZXlzIjpbIk9EU0tSN01ZRlFaNU1NQUo2RlBNRUVUQ1RFM1JJSE9GTFRZUEpSTUFWVk40T0xWMllZQU1IQ0FDIiwiT0RTS0FDU1JCV1A1MzdEWkRSVko2NTdKT0lHT1BPUTZLRzdUNEhONk9LNEY2SUVDR1hEQUhOUDIiLCJPRFNLSTM2TFpCNDRPWTVJVkNSNlA1MkZaSlpZTVlXWlZXTlVEVExFWjVUSzJQTjNPRU1SVEFCUiJdfX0.hyfz6E39BMUh0GLzovFfk3wT4OfualftjdJ_eYkLfPvu5tZubYQ_Pn9oFYGCV_6yKy3KMGhWGUCyCdHaPhalBw"
	oKp  nkeys.KeyPair
)

func init() {
	var err error
	oKp, err = nkeys.FromSeed(oSeed)
	if err != nil {
		panic(fmt.Sprintf("Parsing oSeed failed with: %v", err))
	}
}

func chanRecv(t *testing.T, recvChan <-chan struct{}, limit time.Duration) {
	t.Helper()
	select {
	case <-recvChan:
	case <-time.After(limit):
		t.Fatal("Should have received from channel")
	}
}

func opTrustBasicSetup() *Server {
	kp, _ := nkeys.FromSeed(oSeed)
	pub, _ := kp.PublicKey()
	opts := defaultServerOptions
	opts.TrustedKeys = []string{pub}
	s, c, _, _ := rawSetup(opts)
	c.close()
	return s
}

func buildMemAccResolver(s *Server) {
	mr := &MemAccResolver{}
	s.SetAccountResolver(mr)
}

func addAccountToMemResolver(s *Server, pub, jwtclaim string) {
	s.AccountResolver().Store(pub, jwtclaim)
}

func createClient(t *testing.T, s *Server, akp nkeys.KeyPair) (*testAsyncClient, *bufio.Reader, string) {
	return createClientWithIssuer(t, s, akp, "")
}

func createClientWithIssuer(t *testing.T, s *Server, akp nkeys.KeyPair, optIssuerAccount string) (*testAsyncClient, *bufio.Reader, string) {
	t.Helper()
	nkp, _ := nkeys.CreateUser()
	pub, _ := nkp.PublicKey()
	nuc := jwt.NewUserClaims(pub)
	if optIssuerAccount != "" {
		nuc.IssuerAccount = optIssuerAccount
	}
	ujwt, err := nuc.Encode(akp)
	if err != nil {
		t.Fatalf("Error generating user JWT: %v", err)
	}
	c, cr, l := newClientForServer(s)

	// Sign Nonce
	var info nonceInfo
	json.Unmarshal([]byte(l[5:]), &info)
	sigraw, _ := nkp.Sign([]byte(info.Nonce))
	sig := base64.RawURLEncoding.EncodeToString(sigraw)

	cs := fmt.Sprintf("CONNECT {\"jwt\":%q,\"sig\":\"%s\"}\r\nPING\r\n", ujwt, sig)
	return c, cr, cs
}

func setupJWTTestWithClaims(t *testing.T, nac *jwt.AccountClaims, nuc *jwt.UserClaims, expected string) (*Server, nkeys.KeyPair, *testAsyncClient, *bufio.Reader) {
	t.Helper()

	akp, _ := nkeys.CreateAccount()
	apub, _ := akp.PublicKey()
	if nac == nil {
		nac = jwt.NewAccountClaims(apub)
	} else {
		nac.Subject = apub
	}
	ajwt, err := nac.Encode(oKp)
	if err != nil {
		t.Fatalf("Error generating account JWT: %v", err)
	}

	nkp, _ := nkeys.CreateUser()
	pub, _ := nkp.PublicKey()
	if nuc == nil {
		nuc = jwt.NewUserClaims(pub)
	} else {
		nuc.Subject = pub
	}
	jwt, err := nuc.Encode(akp)
	if err != nil {
		t.Fatalf("Error generating user JWT: %v", err)
	}

	s := opTrustBasicSetup()
	buildMemAccResolver(s)
	addAccountToMemResolver(s, apub, ajwt)

	c, cr, l := newClientForServer(s)

	// Sign Nonce
	var info nonceInfo
	json.Unmarshal([]byte(l[5:]), &info)
	sigraw, _ := nkp.Sign([]byte(info.Nonce))
	sig := base64.RawURLEncoding.EncodeToString(sigraw)

	// PING needed to flush the +OK/-ERR to us.
	cs := fmt.Sprintf("CONNECT {\"jwt\":%q,\"sig\":\"%s\",\"verbose\":true,\"pedantic\":true}\r\nPING\r\n", jwt, sig)
	wg := sync.WaitGroup{}
	wg.Add(1)
	go func() {
		c.parse([]byte(cs))
		wg.Done()
	}()
	l, _ = cr.ReadString('\n')
	if !strings.HasPrefix(l, expected) {
		t.Fatalf("Expected %q, got %q", expected, l)
	}
	wg.Wait()

	return s, akp, c, cr
}

func setupJWTTestWitAccountClaims(t *testing.T, nac *jwt.AccountClaims, expected string) (*Server, nkeys.KeyPair, *testAsyncClient, *bufio.Reader) {
	t.Helper()
	return setupJWTTestWithClaims(t, nac, nil, expected)
}

// This is used in test to create account claims and pass it
// to setupJWTTestWitAccountClaims.
func newJWTTestAccountClaims() *jwt.AccountClaims {
	// We call NewAccountClaims() because it sets some defaults.
	// However, this call needs a subject, but the real subject will
	// be set in setupJWTTestWitAccountClaims(). Use some temporary one
	// here.
	return jwt.NewAccountClaims("temp")
}

func setupJWTTestWithUserClaims(t *testing.T, nuc *jwt.UserClaims, expected string) (*Server, *testAsyncClient, *bufio.Reader) {
	t.Helper()
	s, _, c, cr := setupJWTTestWithClaims(t, nil, nuc, expected)
	return s, c, cr
}

// This is used in test to create user claims and pass it
// to setupJWTTestWithUserClaims.
func newJWTTestUserClaims() *jwt.UserClaims {
	// As of now, tests could simply do &jwt.UserClaims{}, but in
	// case some defaults are later added, we call NewUserClaims().
	// However, this call needs a subject, but the real subject will
	// be set in setupJWTTestWithUserClaims(). Use some temporary one
	// here.
	return jwt.NewUserClaims("temp")
}

func TestJWTUser(t *testing.T) {
	s := opTrustBasicSetup()
	defer s.Shutdown()

	// Check to make sure we would have an authTimer
	if !s.info.AuthRequired {
		t.Fatalf("Expect the server to require auth")
	}

	c, cr, _ := newClientForServer(s)
	defer c.close()

	// Don't send jwt field, should fail.
	c.parseAsync("CONNECT {\"verbose\":true,\"pedantic\":true}\r\nPING\r\n")
	l, _ := cr.ReadString('\n')
	if !strings.HasPrefix(l, "-ERR ") {
		t.Fatalf("Expected an error")
	}

	okp, _ := nkeys.FromSeed(oSeed)

	// Create an account that will be expired.
	akp, _ := nkeys.CreateAccount()
	apub, _ := akp.PublicKey()
	nac := jwt.NewAccountClaims(apub)
	ajwt, err := nac.Encode(okp)
	if err != nil {
		t.Fatalf("Error generating account JWT: %v", err)
	}

	c, cr, cs := createClient(t, s, akp)
	defer c.close()

	// PING needed to flush the +OK/-ERR to us.
	// This should fail too since no account resolver is defined.
	c.parseAsync(cs)
	l, _ = cr.ReadString('\n')
	if !strings.HasPrefix(l, "-ERR ") {
		t.Fatalf("Expected an error")
	}

	// Ok now let's walk through and make sure all is good.
	// We will set the account resolver by hand to a memory resolver.
	buildMemAccResolver(s)
	addAccountToMemResolver(s, apub, ajwt)

	c, cr, cs = createClient(t, s, akp)
	defer c.close()

	c.parseAsync(cs)
	l, _ = cr.ReadString('\n')
	if !strings.HasPrefix(l, "PONG") {
		t.Fatalf("Expected a PONG, got %q", l)
	}
}

func TestJWTUserBadTrusted(t *testing.T) {
	s := opTrustBasicSetup()
	defer s.Shutdown()

	// Check to make sure we would have an authTimer
	if !s.info.AuthRequired {
		t.Fatalf("Expect the server to require auth")
	}
	// Now place bad trusted key
	s.mu.Lock()
	s.trustedKeys = []string{"bad"}
	s.mu.Unlock()

	buildMemAccResolver(s)

	okp, _ := nkeys.FromSeed(oSeed)

	// Create an account that will be expired.
	akp, _ := nkeys.CreateAccount()
	apub, _ := akp.PublicKey()
	nac := jwt.NewAccountClaims(apub)
	ajwt, err := nac.Encode(okp)
	if err != nil {
		t.Fatalf("Error generating account JWT: %v", err)
	}
	addAccountToMemResolver(s, apub, ajwt)

	c, cr, cs := createClient(t, s, akp)
	defer c.close()
	c.parseAsync(cs)
	l, _ := cr.ReadString('\n')
	if !strings.HasPrefix(l, "-ERR ") {
		t.Fatalf("Expected an error")
	}
}

// Test that if a user tries to connect with an expired user JWT we do the right thing.
func TestJWTUserExpired(t *testing.T) {
	nuc := newJWTTestUserClaims()
	nuc.IssuedAt = time.Now().Add(-10 * time.Second).Unix()
	nuc.Expires = time.Now().Add(-2 * time.Second).Unix()
	s, c, _ := setupJWTTestWithUserClaims(t, nuc, "-ERR ")
	c.close()
	s.Shutdown()
}

func TestJWTUserExpiresAfterConnect(t *testing.T) {
	nuc := newJWTTestUserClaims()
	nuc.IssuedAt = time.Now().Unix()
	nuc.Expires = time.Now().Add(time.Second).Unix()
	s, c, cr := setupJWTTestWithUserClaims(t, nuc, "+OK")
	defer s.Shutdown()
	defer c.close()
	l, err := cr.ReadString('\n')
	if err != nil {
		t.Fatalf("Received %v", err)
	}
	if !strings.HasPrefix(l, "PONG") {
		t.Fatalf("Expected a PONG")
	}

	// Now we should expire after 1 second or so.
	time.Sleep(1250 * time.Millisecond)

	l, err = cr.ReadString('\n')
	if err != nil {
		t.Fatalf("Received %v", err)
	}
	if !strings.HasPrefix(l, "-ERR ") {
		t.Fatalf("Expected an error")
	}
	if !strings.Contains(l, "Expired") {
		t.Fatalf("Expected 'Expired' to be in the error")
	}
}

func TestJWTUserPermissionClaims(t *testing.T) {
	nuc := newJWTTestUserClaims()
	nuc.Permissions.Pub.Allow.Add("foo")
	nuc.Permissions.Pub.Allow.Add("bar")
	nuc.Permissions.Pub.Deny.Add("baz")
	nuc.Permissions.Sub.Allow.Add("foo")
	nuc.Permissions.Sub.Allow.Add("bar")
	nuc.Permissions.Sub.Deny.Add("baz")

	s, c, _ := setupJWTTestWithUserClaims(t, nuc, "+OK")
	defer s.Shutdown()
	defer c.close()

	// Now check client to make sure permissions transferred.
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.perms == nil {
		t.Fatalf("Expected client permissions to be set")
	}

	if lpa := c.perms.pub.allow.Count(); lpa != 2 {
		t.Fatalf("Expected 2 publish allow subjects, got %d", lpa)
	}
	if lpd := c.perms.pub.deny.Count(); lpd != 1 {
		t.Fatalf("Expected 1 publish deny subjects, got %d", lpd)
	}
	if lsa := c.perms.sub.allow.Count(); lsa != 2 {
		t.Fatalf("Expected 2 subscribe allow subjects, got %d", lsa)
	}
	if lsd := c.perms.sub.deny.Count(); lsd != 1 {
		t.Fatalf("Expected 1 subscribe deny subjects, got %d", lsd)
	}
}

func TestJWTUserResponsePermissionClaims(t *testing.T) {
	nuc := newJWTTestUserClaims()
	nuc.Permissions.Resp = &jwt.ResponsePermission{
		MaxMsgs: 22,
		Expires: 100 * time.Millisecond,
	}
	s, c, _ := setupJWTTestWithUserClaims(t, nuc, "+OK")
	defer s.Shutdown()
	defer c.close()

	// Now check client to make sure permissions transferred.
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.perms == nil {
		t.Fatalf("Expected client permissions to be set")
	}
	if c.perms.pub.allow == nil {
		t.Fatalf("Expected client perms for pub allow to be non-nil")
	}
	if lpa := c.perms.pub.allow.Count(); lpa != 0 {
		t.Fatalf("Expected 0 publish allow subjects, got %d", lpa)
	}
	if c.perms.resp == nil {
		t.Fatalf("Expected client perms for response permissions to be non-nil")
	}
	if c.perms.resp.MaxMsgs != nuc.Permissions.Resp.MaxMsgs {
		t.Fatalf("Expected client perms for response permissions MaxMsgs to be same as jwt: %d vs %d",
			c.perms.resp.MaxMsgs, nuc.Permissions.Resp.MaxMsgs)
	}
	if c.perms.resp.Expires != nuc.Permissions.Resp.Expires {
		t.Fatalf("Expected client perms for response permissions Expires to be same as jwt: %v vs %v",
			c.perms.resp.Expires, nuc.Permissions.Resp.Expires)
	}
}

func TestJWTUserResponsePermissionClaimsDefaultValues(t *testing.T) {
	nuc := newJWTTestUserClaims()
	nuc.Permissions.Resp = &jwt.ResponsePermission{}
	s, c, _ := setupJWTTestWithUserClaims(t, nuc, "+OK")
	defer s.Shutdown()
	defer c.close()

	// Now check client to make sure permissions transferred
	// and defaults are set.
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.perms == nil {
		t.Fatalf("Expected client permissions to be set")
	}
	if c.perms.pub.allow == nil {
		t.Fatalf("Expected client perms for pub allow to be non-nil")
	}
	if lpa := c.perms.pub.allow.Count(); lpa != 0 {
		t.Fatalf("Expected 0 publish allow subjects, got %d", lpa)
	}
	if c.perms.resp == nil {
		t.Fatalf("Expected client perms for response permissions to be non-nil")
	}
	if c.perms.resp.MaxMsgs != DEFAULT_ALLOW_RESPONSE_MAX_MSGS {
		t.Fatalf("Expected client perms for response permissions MaxMsgs to be default %v, got %v",
			DEFAULT_ALLOW_RESPONSE_MAX_MSGS, c.perms.resp.MaxMsgs)
	}
	if c.perms.resp.Expires != DEFAULT_ALLOW_RESPONSE_EXPIRATION {
		t.Fatalf("Expected client perms for response permissions Expires to be default %v, got %v",
			DEFAULT_ALLOW_RESPONSE_EXPIRATION, c.perms.resp.Expires)
	}
}

func TestJWTUserResponsePermissionClaimsNegativeValues(t *testing.T) {
	nuc := newJWTTestUserClaims()
	nuc.Permissions.Resp = &jwt.ResponsePermission{
		MaxMsgs: -1,
		Expires: -1 * time.Second,
	}
	s, c, _ := setupJWTTestWithUserClaims(t, nuc, "+OK")
	defer s.Shutdown()
	defer c.close()

	// Now check client to make sure permissions transferred
	// and negative values are transferred.
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.perms == nil {
		t.Fatalf("Expected client permissions to be set")
	}
	if c.perms.pub.allow == nil {
		t.Fatalf("Expected client perms for pub allow to be non-nil")
	}
	if lpa := c.perms.pub.allow.Count(); lpa != 0 {
		t.Fatalf("Expected 0 publish allow subjects, got %d", lpa)
	}
	if c.perms.resp == nil {
		t.Fatalf("Expected client perms for response permissions to be non-nil")
	}
	if c.perms.resp.MaxMsgs != -1 {
		t.Fatalf("Expected client perms for response permissions MaxMsgs to be %v, got %v",
			-1, c.perms.resp.MaxMsgs)
	}
	if c.perms.resp.Expires != -1*time.Second {
		t.Fatalf("Expected client perms for response permissions Expires to be %v, got %v",
			-1*time.Second, c.perms.resp.Expires)
	}
}

func TestJWTAccountExpired(t *testing.T) {
	nac := newJWTTestAccountClaims()
	nac.IssuedAt = time.Now().Add(-10 * time.Second).Unix()
	nac.Expires = time.Now().Add(-2 * time.Second).Unix()
	s, _, c, _ := setupJWTTestWitAccountClaims(t, nac, "-ERR ")
	defer s.Shutdown()
	defer c.close()
}

func TestJWTAccountExpiresAfterConnect(t *testing.T) {
	nac := newJWTTestAccountClaims()
	now := time.Now()
	nac.IssuedAt = now.Add(-10 * time.Second).Unix()
	nac.Expires = now.Round(time.Second).Add(time.Second).Unix()
	s, akp, c, cr := setupJWTTestWitAccountClaims(t, nac, "+OK")
	defer s.Shutdown()
	defer c.close()

	apub, _ := akp.PublicKey()
	acc, err := s.LookupAccount(apub)
	if acc == nil || err != nil {
		t.Fatalf("Expected to retrieve the account")
	}

	if l, _ := cr.ReadString('\n'); !strings.HasPrefix(l, "PONG") {
		t.Fatalf("Expected PONG, got %q", l)
	}

	// Wait for the account to be expired.
	checkFor(t, 3*time.Second, 100*time.Millisecond, func() error {
		if acc.IsExpired() {
			return nil
		}
		return fmt.Errorf("Account not expired yet")
	})

	l, _ := cr.ReadString('\n')
	if !strings.HasPrefix(l, "-ERR ") {
		t.Fatalf("Expected an error, got %q", l)
	}
	if !strings.Contains(l, "Expired") {
		t.Fatalf("Expected 'Expired' to be in the error")
	}

	// Now make sure that accounts that have expired return an error.
	c, cr, cs := createClient(t, s, akp)
	defer c.close()
	c.parseAsync(cs)
	l, _ = cr.ReadString('\n')
	if !strings.HasPrefix(l, "-ERR ") {
		t.Fatalf("Expected an error")
	}
}

func TestJWTAccountRenew(t *testing.T) {
	nac := newJWTTestAccountClaims()
	// Create an account that has expired.
	nac.IssuedAt = time.Now().Add(-10 * time.Second).Unix()
	nac.Expires = time.Now().Add(-2 * time.Second).Unix()
	// Expect an error
	s, akp, c, _ := setupJWTTestWitAccountClaims(t, nac, "-ERR ")
	defer s.Shutdown()
	defer c.close()

	okp, _ := nkeys.FromSeed(oSeed)
	apub, _ := akp.PublicKey()

	// Now update with new expiration
	nac.IssuedAt = time.Now().Unix()
	nac.Expires = time.Now().Add(5 * time.Second).Unix()
	ajwt, err := nac.Encode(okp)
	if err != nil {
		t.Fatalf("Error generating account JWT: %v", err)
	}

	// Update the account
	addAccountToMemResolver(s, apub, ajwt)
	acc, _ := s.LookupAccount(apub)
	if acc == nil {
		t.Fatalf("Expected to retrieve the account")
	}
	s.UpdateAccountClaims(acc, nac)

	// Now make sure we can connect.
	c, cr, cs := createClient(t, s, akp)
	defer c.close()
	c.parseAsync(cs)
	if l, _ := cr.ReadString('\n'); !strings.HasPrefix(l, "PONG") {
		t.Fatalf("Expected a PONG, got: %q", l)
	}
}

func TestJWTAccountRenewFromResolver(t *testing.T) {
	s := opTrustBasicSetup()
	defer s.Shutdown()
	buildMemAccResolver(s)

	okp, _ := nkeys.FromSeed(oSeed)

	akp, _ := nkeys.CreateAccount()
	apub, _ := akp.PublicKey()
	nac := jwt.NewAccountClaims(apub)
	nac.IssuedAt = time.Now().Add(-10 * time.Second).Unix()
	nac.Expires = time.Now().Add(time.Second).Unix()
	ajwt, err := nac.Encode(okp)
	if err != nil {
		t.Fatalf("Error generating account JWT: %v", err)
	}

	addAccountToMemResolver(s, apub, ajwt)
	// Force it to be loaded by the server and start the expiration timer.
	acc, _ := s.LookupAccount(apub)
	if acc == nil {
		t.Fatalf("Could not retrieve account for %q", apub)
	}

	// Create a new user
	c, cr, cs := createClient(t, s, akp)
	defer c.close()
	// Wait for expiration.
	time.Sleep(1250 * time.Millisecond)

	c.parseAsync(cs)
	l, _ := cr.ReadString('\n')
	if !strings.HasPrefix(l, "-ERR ") {
		t.Fatalf("Expected an error")
	}

	// Now update with new expiration
	nac.IssuedAt = time.Now().Unix()
	nac.Expires = time.Now().Add(5 * time.Second).Unix()
	ajwt, err = nac.Encode(okp)
	if err != nil {
		t.Fatalf("Error generating account JWT: %v", err)
	}

	// Update the account
	addAccountToMemResolver(s, apub, ajwt)
	// Make sure the too quick update suppression does not bite us.
	acc.mu.Lock()
	acc.updated = time.Now().Add(-1 * time.Hour)
	acc.mu.Unlock()

	// Do not update the account directly. The resolver should
	// happen automatically.

	// Now make sure we can connect.
	c, cr, cs = createClient(t, s, akp)
	defer c.close()
	c.parseAsync(cs)
	l, _ = cr.ReadString('\n')
	if !strings.HasPrefix(l, "PONG") {
		t.Fatalf("Expected a PONG, got: %q", l)
	}
}

func TestJWTAccountBasicImportExport(t *testing.T) {
	s := opTrustBasicSetup()
	defer s.Shutdown()
	buildMemAccResolver(s)

	okp, _ := nkeys.FromSeed(oSeed)

	// Create accounts and imports/exports.
	fooKP, _ := nkeys.CreateAccount()
	fooPub, _ := fooKP.PublicKey()
	fooAC := jwt.NewAccountClaims(fooPub)

	// Now create Exports.
	streamExport := &jwt.Export{Subject: "foo", Type: jwt.Stream}
	streamExport2 := &jwt.Export{Subject: "private", Type: jwt.Stream, TokenReq: true}
	serviceExport := &jwt.Export{Subject: "req.echo", Type: jwt.Service, TokenReq: true}
	serviceExport2 := &jwt.Export{Subject: "req.add", Type: jwt.Service, TokenReq: true}

	fooAC.Exports.Add(streamExport, streamExport2, serviceExport, serviceExport2)
	fooJWT, err := fooAC.Encode(okp)
	if err != nil {
		t.Fatalf("Error generating account JWT: %v", err)
	}

	addAccountToMemResolver(s, fooPub, fooJWT)

	acc, _ := s.LookupAccount(fooPub)
	if acc == nil {
		t.Fatalf("Expected to retrieve the account")
	}

	// Check to make sure exports transferred over.
	if les := len(acc.exports.streams); les != 2 {
		t.Fatalf("Expected exports streams len of 2, got %d", les)
	}
	if les := len(acc.exports.services); les != 2 {
		t.Fatalf("Expected exports services len of 2, got %d", les)
	}
	_, ok := acc.exports.streams["foo"]
	if !ok {
		t.Fatalf("Expected to map a stream export")
	}
	se, ok := acc.exports.services["req.echo"]
	if !ok || se == nil {
		t.Fatalf("Expected to map a service export")
	}
	if !se.tokenReq {
		t.Fatalf("Expected the service export to require tokens")
	}

	barKP, _ := nkeys.CreateAccount()
	barPub, _ := barKP.PublicKey()
	barAC := jwt.NewAccountClaims(barPub)

	streamImport := &jwt.Import{Account: fooPub, Subject: "foo", To: "import.foo", Type: jwt.Stream}
	serviceImport := &jwt.Import{Account: fooPub, Subject: "req.echo", Type: jwt.Service}
	barAC.Imports.Add(streamImport, serviceImport)
	barJWT, err := barAC.Encode(okp)
	if err != nil {
		t.Fatalf("Error generating account JWT: %v", err)
	}
	addAccountToMemResolver(s, barPub, barJWT)

	acc, _ = s.LookupAccount(barPub)
	if acc == nil {
		t.Fatalf("Expected to retrieve the account")
	}
	if les := len(acc.imports.streams); les != 1 {
		t.Fatalf("Expected imports streams len of 1, got %d", les)
	}
	// Our service import should have failed without a token.
	if les := len(acc.imports.services); les != 0 {
		t.Fatalf("Expected imports services len of 0, got %d", les)
	}

	// Now add in a bad activation token.
	barAC = jwt.NewAccountClaims(barPub)
	serviceImport = &jwt.Import{Account: fooPub, Subject: "req.echo", Token: "not a token", Type: jwt.Service}
	barAC.Imports.Add(serviceImport)
	barJWT, err = barAC.Encode(okp)
	if err != nil {
		t.Fatalf("Error generating account JWT: %v", err)
	}
	addAccountToMemResolver(s, barPub, barJWT)

	s.UpdateAccountClaims(acc, barAC)

	// Our service import should have failed with a bad token.
	if les := len(acc.imports.services); les != 0 {
		t.Fatalf("Expected imports services len of 0, got %d", les)
	}

	// Now make a correct one.
	barAC = jwt.NewAccountClaims(barPub)
	serviceImport = &jwt.Import{Account: fooPub, Subject: "req.echo", Type: jwt.Service}

	activation := jwt.NewActivationClaims(barPub)
	activation.ImportSubject = "req.echo"
	activation.ImportType = jwt.Service
	actJWT, err := activation.Encode(fooKP)
	if err != nil {
		t.Fatalf("Error generating activation token: %v", err)
	}
	serviceImport.Token = actJWT
	barAC.Imports.Add(serviceImport)
	barJWT, err = barAC.Encode(okp)
	if err != nil {
		t.Fatalf("Error generating account JWT: %v", err)
	}

	vr := jwt.ValidationResults{}
	barAC.Validate(&vr)
	if vr.IsBlocking(true) {
		t.Fatalf("Error generating account JWT: %v", vr)
	}

	addAccountToMemResolver(s, barPub, barJWT)
	s.UpdateAccountClaims(acc, barAC)
	// Our service import should have succeeded.
	if les := len(acc.imports.services); les != 1 {
		t.Fatalf("Expected imports services len of 1, got %d", les)
	}

	// Now streams
	barAC = jwt.NewAccountClaims(barPub)
	streamImport = &jwt.Import{Account: fooPub, Subject: "private", To: "import.private", Type: jwt.Stream}

	barAC.Imports.Add(streamImport)
	barJWT, err = barAC.Encode(okp)
	if err != nil {
		t.Fatalf("Error generating account JWT: %v", err)
	}
	addAccountToMemResolver(s, barPub, barJWT)
	s.UpdateAccountClaims(acc, barAC)
	// Our stream import should have not succeeded.
	if les := len(acc.imports.streams); les != 0 {
		t.Fatalf("Expected imports services len of 0, got %d", les)
	}

	// Now add in activation.
	barAC = jwt.NewAccountClaims(barPub)
	streamImport = &jwt.Import{Account: fooPub, Subject: "private", To: "import.private", Type: jwt.Stream}

	activation = jwt.NewActivationClaims(barPub)
	activation.ImportSubject = "private"
	activation.ImportType = jwt.Stream
	actJWT, err = activation.Encode(fooKP)
	if err != nil {
		t.Fatalf("Error generating activation token: %v", err)
	}
	streamImport.Token = actJWT
	barAC.Imports.Add(streamImport)
	barJWT, err = barAC.Encode(okp)
	if err != nil {
		t.Fatalf("Error generating account JWT: %v", err)
	}
	addAccountToMemResolver(s, barPub, barJWT)
	s.UpdateAccountClaims(acc, barAC)
	// Our stream import should have not succeeded.
	if les := len(acc.imports.streams); les != 1 {
		t.Fatalf("Expected imports services len of 1, got %d", les)
	}
}

func TestJWTAccountExportWithResponseType(t *testing.T) {
	s := opTrustBasicSetup()
	defer s.Shutdown()
	buildMemAccResolver(s)

	okp, _ := nkeys.FromSeed(oSeed)

	// Create accounts and imports/exports.
	fooKP, _ := nkeys.CreateAccount()
	fooPub, _ := fooKP.PublicKey()
	fooAC := jwt.NewAccountClaims(fooPub)

	// Now create Exports.
	serviceStreamExport := &jwt.Export{Subject: "test.stream", Type: jwt.Service, ResponseType: jwt.ResponseTypeStream, TokenReq: false}
	serviceChunkExport := &jwt.Export{Subject: "test.chunk", Type: jwt.Service, ResponseType: jwt.ResponseTypeChunked, TokenReq: false}
	serviceSingletonExport := &jwt.Export{Subject: "test.single", Type: jwt.Service, ResponseType: jwt.ResponseTypeSingleton, TokenReq: true}
	serviceDefExport := &jwt.Export{Subject: "test.def", Type: jwt.Service, TokenReq: true}
	serviceOldExport := &jwt.Export{Subject: "test.old", Type: jwt.Service, TokenReq: false}

	fooAC.Exports.Add(serviceStreamExport, serviceSingletonExport, serviceChunkExport, serviceDefExport, serviceOldExport)
	fooJWT, err := fooAC.Encode(okp)
	if err != nil {
		t.Fatalf("Error generating account JWT: %v", err)
	}

	addAccountToMemResolver(s, fooPub, fooJWT)

	fooAcc, _ := s.LookupAccount(fooPub)
	if fooAcc == nil {
		t.Fatalf("Expected to retrieve the account")
	}

	services := fooAcc.exports.services

	if len(services) != 5 {
		t.Fatalf("Expected 4 services")
	}

	se, ok := services["test.stream"]
	if !ok || se == nil {
		t.Fatalf("Expected to map a service export")
	}
	if se.tokenReq {
		t.Fatalf("Expected the service export to not require tokens")
	}
	if se.respType != Streamed {
		t.Fatalf("Expected the service export to respond with a stream")
	}

	se, ok = services["test.chunk"]
	if !ok || se == nil {
		t.Fatalf("Expected to map a service export")
	}
	if se.tokenReq {
		t.Fatalf("Expected the service export to not require tokens")
	}
	if se.respType != Chunked {
		t.Fatalf("Expected the service export to respond with a stream")
	}

	se, ok = services["test.def"]
	if !ok || se == nil {
		t.Fatalf("Expected to map a service export")
	}
	if !se.tokenReq {
		t.Fatalf("Expected the service export to not require tokens")
	}
	if se.respType != Singleton {
		t.Fatalf("Expected the service export to respond with a stream")
	}

	se, ok = services["test.single"]
	if !ok || se == nil {
		t.Fatalf("Expected to map a service export")
	}
	if !se.tokenReq {
		t.Fatalf("Expected the service export to not require tokens")
	}
	if se.respType != Singleton {
		t.Fatalf("Expected the service export to respond with a stream")
	}

	se, ok = services["test.old"]
	if !ok || se == nil || len(se.approved) > 0 {
		t.Fatalf("Service with a singleton response and no tokens should not be nil and have no approvals")
	}
}

func expectPong(t *testing.T, cr *bufio.Reader) {
	t.Helper()
	l, _ := cr.ReadString('\n')
	if !strings.HasPrefix(l, "PONG") {
		t.Fatalf("Expected a PONG, got %q", l)
	}
}

func expectMsg(t *testing.T, cr *bufio.Reader, sub, payload string) {
	t.Helper()
	l, _ := cr.ReadString('\n')
	expected := "MSG " + sub
	if !strings.HasPrefix(l, expected) {
		t.Fatalf("Expected %q, got %q", expected, l)
	}
	l, _ = cr.ReadString('\n')
	if l != payload+"\r\n" {
		t.Fatalf("Expected %q, got %q", payload, l)
	}
	expectPong(t, cr)
}

func TestJWTAccountImportExportUpdates(t *testing.T) {
	s := opTrustBasicSetup()
	defer s.Shutdown()
	buildMemAccResolver(s)

	okp, _ := nkeys.FromSeed(oSeed)

	// Create accounts and imports/exports.
	fooKP, _ := nkeys.CreateAccount()
	fooPub, _ := fooKP.PublicKey()
	fooAC := jwt.NewAccountClaims(fooPub)
	streamExport := &jwt.Export{Subject: "foo", Type: jwt.Stream}

	fooAC.Exports.Add(streamExport)
	fooJWT, err := fooAC.Encode(okp)
	if err != nil {
		t.Fatalf("Error generating account JWT: %v", err)
	}
	addAccountToMemResolver(s, fooPub, fooJWT)

	barKP, _ := nkeys.CreateAccount()
	barPub, _ := barKP.PublicKey()
	barAC := jwt.NewAccountClaims(barPub)
	streamImport := &jwt.Import{Account: fooPub, Subject: "foo", To: "import", Type: jwt.Stream}

	barAC.Imports.Add(streamImport)
	barJWT, err := barAC.Encode(okp)
	if err != nil {
		t.Fatalf("Error generating account JWT: %v", err)
	}
	addAccountToMemResolver(s, barPub, barJWT)

	// Create a client.
	c, cr, cs := createClient(t, s, barKP)
	defer c.close()

	c.parseAsync(cs)
	expectPong(t, cr)

	c.parseAsync("SUB import.foo 1\r\nPING\r\n")
	expectPong(t, cr)

	checkShadow := func(expected int) {
		t.Helper()
		c.mu.Lock()
		defer c.mu.Unlock()
		sub := c.subs["1"]
		if ls := len(sub.shadow); ls != expected {
			t.Fatalf("Expected shadows to be %d, got %d", expected, ls)
		}
	}

	// We created a SUB on foo which should create a shadow subscription.
	checkShadow(1)

	// Now update bar and remove the import which should make the shadow go away.
	barAC = jwt.NewAccountClaims(barPub)
	barJWT, _ = barAC.Encode(okp)
	addAccountToMemResolver(s, barPub, barJWT)
	acc, _ := s.LookupAccount(barPub)
	s.UpdateAccountClaims(acc, barAC)

	checkShadow(0)

	// Now add it back and make sure the shadow comes back.
	streamImport = &jwt.Import{Account: string(fooPub), Subject: "foo", To: "import", Type: jwt.Stream}
	barAC.Imports.Add(streamImport)
	barJWT, _ = barAC.Encode(okp)
	addAccountToMemResolver(s, barPub, barJWT)
	s.UpdateAccountClaims(acc, barAC)

	checkShadow(1)

	// Now change export and make sure it goes away as well. So no exports anymore.
	fooAC = jwt.NewAccountClaims(fooPub)
	fooJWT, _ = fooAC.Encode(okp)
	addAccountToMemResolver(s, fooPub, fooJWT)
	acc, _ = s.LookupAccount(fooPub)
	s.UpdateAccountClaims(acc, fooAC)
	checkShadow(0)

	// Now add it in but with permission required.
	streamExport = &jwt.Export{Subject: "foo", Type: jwt.Stream, TokenReq: true}
	fooAC.Exports.Add(streamExport)
	fooJWT, _ = fooAC.Encode(okp)
	addAccountToMemResolver(s, fooPub, fooJWT)
	s.UpdateAccountClaims(acc, fooAC)

	checkShadow(0)

	// Now put it back as normal.
	fooAC = jwt.NewAccountClaims(fooPub)
	streamExport = &jwt.Export{Subject: "foo", Type: jwt.Stream}
	fooAC.Exports.Add(streamExport)
	fooJWT, _ = fooAC.Encode(okp)
	addAccountToMemResolver(s, fooPub, fooJWT)
	s.UpdateAccountClaims(acc, fooAC)

	checkShadow(1)
}

func TestJWTAccountImportActivationExpires(t *testing.T) {
	s := opTrustBasicSetup()
	defer s.Shutdown()
	buildMemAccResolver(s)

	okp, _ := nkeys.FromSeed(oSeed)

	// Create accounts and imports/exports.
	fooKP, _ := nkeys.CreateAccount()
	fooPub, _ := fooKP.PublicKey()
	fooAC := jwt.NewAccountClaims(fooPub)
	streamExport := &jwt.Export{Subject: "foo", Type: jwt.Stream, TokenReq: true}
	fooAC.Exports.Add(streamExport)

	fooJWT, err := fooAC.Encode(okp)
	if err != nil {
		t.Fatalf("Error generating account JWT: %v", err)
	}

	addAccountToMemResolver(s, fooPub, fooJWT)
	acc, _ := s.LookupAccount(fooPub)
	if acc == nil {
		t.Fatalf("Expected to retrieve the account")
	}

	barKP, _ := nkeys.CreateAccount()
	barPub, _ := barKP.PublicKey()
	barAC := jwt.NewAccountClaims(barPub)
	streamImport := &jwt.Import{Account: fooPub, Subject: "foo", To: "import.", Type: jwt.Stream}

	activation := jwt.NewActivationClaims(barPub)
	activation.ImportSubject = "foo"
	activation.ImportType = jwt.Stream
	now := time.Now()
	activation.IssuedAt = now.Add(-10 * time.Second).Unix()
	// These are second resolution. So round up before adding a second.
	activation.Expires = now.Round(time.Second).Add(time.Second).Unix()
	actJWT, err := activation.Encode(fooKP)
	if err != nil {
		t.Fatalf("Error generating activation token: %v", err)
	}
	streamImport.Token = actJWT
	barAC.Imports.Add(streamImport)
	barJWT, err := barAC.Encode(okp)
	if err != nil {
		t.Fatalf("Error generating account JWT: %v", err)
	}
	addAccountToMemResolver(s, barPub, barJWT)
	if acc, _ := s.LookupAccount(barPub); acc == nil {
		t.Fatalf("Expected to retrieve the account")
	}

	// Create a client.
	c, cr, cs := createClient(t, s, barKP)
	defer c.close()

	c.parseAsync(cs)
	expectPong(t, cr)

	c.parseAsync("SUB import.foo 1\r\nPING\r\n")
	expectPong(t, cr)

	checkShadow := func(t *testing.T, expected int) {
		t.Helper()
		checkFor(t, 3*time.Second, 15*time.Millisecond, func() error {
			c.mu.Lock()
			defer c.mu.Unlock()
			sub := c.subs["1"]
			if ls := len(sub.shadow); ls != expected {
				return fmt.Errorf("Expected shadows to be %d, got %d", expected, ls)
			}
			return nil
		})
	}

	// We created a SUB on foo which should create a shadow subscription.
	checkShadow(t, 1)

	time.Sleep(1250 * time.Millisecond)

	// Should have expired and been removed.
	checkShadow(t, 0)
}

func TestJWTAccountLimitsSubs(t *testing.T) {
	fooAC := newJWTTestAccountClaims()
	fooAC.Limits.Subs = 10
	s, fooKP, c, _ := setupJWTTestWitAccountClaims(t, fooAC, "+OK")
	defer s.Shutdown()
	defer c.close()

	okp, _ := nkeys.FromSeed(oSeed)
	fooPub, _ := fooKP.PublicKey()

	// Create a client.
	c, cr, cs := createClient(t, s, fooKP)
	defer c.close()

	c.parseAsync(cs)
	expectPong(t, cr)

	// Check to make sure we have the limit set.
	// Account first
	fooAcc, _ := s.LookupAccount(fooPub)
	fooAcc.mu.RLock()
	if fooAcc.msubs != 10 {
		fooAcc.mu.RUnlock()
		t.Fatalf("Expected account to have msubs of 10, got %d", fooAcc.msubs)
	}
	fooAcc.mu.RUnlock()
	// Now test that the client has limits too.
	c.mu.Lock()
	if c.msubs != 10 {
		c.mu.Unlock()
		t.Fatalf("Expected client msubs to be 10, got %d", c.msubs)
	}
	c.mu.Unlock()

	// Now make sure its enforced.
	/// These should all work ok.
	for i := 0; i < 10; i++ {
		c.parseAsync(fmt.Sprintf("SUB foo %d\r\nPING\r\n", i))
		expectPong(t, cr)
	}

	// This one should fail.
	c.parseAsync("SUB foo 22\r\n")
	l, _ := cr.ReadString('\n')
	if !strings.HasPrefix(l, "-ERR") {
		t.Fatalf("Expected an ERR, got: %v", l)
	}
	if !strings.Contains(l, "maximum subscriptions exceeded") {
		t.Fatalf("Expected an ERR for max subscriptions exceeded, got: %v", l)
	}

	// Now update the claims and expect if max is lower to be disconnected.
	fooAC.Limits.Subs = 5
	fooJWT, err := fooAC.Encode(okp)
	if err != nil {
		t.Fatalf("Error generating account JWT: %v", err)
	}
	addAccountToMemResolver(s, fooPub, fooJWT)
	s.UpdateAccountClaims(fooAcc, fooAC)
	l, _ = cr.ReadString('\n')
	if !strings.HasPrefix(l, "-ERR") {
		t.Fatalf("Expected an ERR, got: %v", l)
	}
	if !strings.Contains(l, "maximum subscriptions exceeded") {
		t.Fatalf("Expected an ERR for max subscriptions exceeded, got: %v", l)
	}
}

func TestJWTAccountLimitsSubsButServerOverrides(t *testing.T) {
	s := opTrustBasicSetup()
	defer s.Shutdown()
	buildMemAccResolver(s)

	// override with server setting of 2.
	opts := s.getOpts()
	opts.MaxSubs = 2

	okp, _ := nkeys.FromSeed(oSeed)

	// Create accounts and imports/exports.
	fooKP, _ := nkeys.CreateAccount()
	fooPub, _ := fooKP.PublicKey()
	fooAC := jwt.NewAccountClaims(fooPub)
	fooAC.Limits.Subs = 10
	fooJWT, err := fooAC.Encode(okp)
	if err != nil {
		t.Fatalf("Error generating account JWT: %v", err)
	}
	addAccountToMemResolver(s, fooPub, fooJWT)
	fooAcc, _ := s.LookupAccount(fooPub)
	fooAcc.mu.RLock()
	if fooAcc.msubs != 10 {
		fooAcc.mu.RUnlock()
		t.Fatalf("Expected account to have msubs of 10, got %d", fooAcc.msubs)
	}
	fooAcc.mu.RUnlock()

	// Create a client.
	c, cr, cs := createClient(t, s, fooKP)
	defer c.close()

	c.parseAsync(cs)
	expectPong(t, cr)

	c.parseAsync("SUB foo 1\r\nSUB bar 2\r\nSUB baz 3\r\nPING\r\n")
	l, _ := cr.ReadString('\n')

	if !strings.HasPrefix(l, "-ERR ") {
		t.Fatalf("Expected an error")
	}
	if !strings.Contains(l, "maximum subscriptions exceeded") {
		t.Fatalf("Expected an ERR for max subscriptions exceeded, got: %v", l)
	}
	// Read last PONG so does not hold up test.
	cr.ReadString('\n')
}

func TestJWTAccountLimitsMaxPayload(t *testing.T) {
	fooAC := newJWTTestAccountClaims()
	fooAC.Limits.Payload = 8
	s, fooKP, c, _ := setupJWTTestWitAccountClaims(t, fooAC, "+OK")
	defer s.Shutdown()
	defer c.close()

	fooPub, _ := fooKP.PublicKey()

	// Create a client.
	c, cr, cs := createClient(t, s, fooKP)
	defer c.close()

	c.parseAsync(cs)
	expectPong(t, cr)

	// Check to make sure we have the limit set.
	// Account first
	fooAcc, _ := s.LookupAccount(fooPub)
	fooAcc.mu.RLock()
	if fooAcc.mpay != 8 {
		fooAcc.mu.RUnlock()
		t.Fatalf("Expected account to have mpay of 8, got %d", fooAcc.mpay)
	}
	fooAcc.mu.RUnlock()
	// Now test that the client has limits too.
	c.mu.Lock()
	if c.mpay != 8 {
		c.mu.Unlock()
		t.Fatalf("Expected client to have mpay of 10, got %d", c.mpay)
	}
	c.mu.Unlock()

	c.parseAsync("PUB foo 4\r\nXXXX\r\nPING\r\n")
	expectPong(t, cr)

	c.parseAsync("PUB foo 10\r\nXXXXXXXXXX\r\nPING\r\n")
	l, _ := cr.ReadString('\n')
	if !strings.HasPrefix(l, "-ERR ") {
		t.Fatalf("Expected an error")
	}
	if !strings.Contains(l, "Maximum Payload") {
		t.Fatalf("Expected an ERR for max payload violation, got: %v", l)
	}
}

func TestJWTAccountLimitsMaxPayloadButServerOverrides(t *testing.T) {
	s := opTrustBasicSetup()
	defer s.Shutdown()
	buildMemAccResolver(s)

	// override with server setting of 4.
	opts := s.getOpts()
	opts.MaxPayload = 4

	okp, _ := nkeys.FromSeed(oSeed)

	// Create accounts and imports/exports.
	fooKP, _ := nkeys.CreateAccount()
	fooPub, _ := fooKP.PublicKey()
	fooAC := jwt.NewAccountClaims(fooPub)
	fooAC.Limits.Payload = 8
	fooJWT, err := fooAC.Encode(okp)
	if err != nil {
		t.Fatalf("Error generating account JWT: %v", err)
	}
	addAccountToMemResolver(s, fooPub, fooJWT)

	// Create a client.
	c, cr, cs := createClient(t, s, fooKP)
	defer c.close()

	c.parseAsync(cs)
	expectPong(t, cr)

	c.parseAsync("PUB foo 6\r\nXXXXXX\r\nPING\r\n")
	l, _ := cr.ReadString('\n')
	if !strings.HasPrefix(l, "-ERR ") {
		t.Fatalf("Expected an error")
	}
	if !strings.Contains(l, "Maximum Payload") {
		t.Fatalf("Expected an ERR for max payload violation, got: %v", l)
	}
}

func TestJWTAccountLimitsMaxConns(t *testing.T) {
	fooAC := newJWTTestAccountClaims()
	fooAC.Limits.Conn = 8
	s, fooKP, c, _ := setupJWTTestWitAccountClaims(t, fooAC, "+OK")
	defer s.Shutdown()
	defer c.close()

	newClient := func(expPre string) *testAsyncClient {
		t.Helper()
		// Create a client.
		c, cr, cs := createClient(t, s, fooKP)
		c.parseAsync(cs)
		l, _ := cr.ReadString('\n')
		if !strings.HasPrefix(l, expPre) {
			t.Fatalf("Expected a response starting with %q, got %q", expPre, l)
		}
		return c
	}

	// A connection is created in setupJWTTestWitAccountClaims(), so limit
	// to 7 here (8 total).
	for i := 0; i < 7; i++ {
		c := newClient("PONG")
		defer c.close()
	}
	// Now this one should fail.
	c = newClient("-ERR ")
	c.close()
}

// This will test that we can switch from a public export to a private
// one and back with export claims to make sure the claim update mechanism
// is working properly.
func TestJWTAccountServiceImportAuthSwitch(t *testing.T) {
	s := opTrustBasicSetup()
	defer s.Shutdown()
	buildMemAccResolver(s)

	okp, _ := nkeys.FromSeed(oSeed)

	// Create accounts and imports/exports.
	fooKP, _ := nkeys.CreateAccount()
	fooPub, _ := fooKP.PublicKey()
	fooAC := jwt.NewAccountClaims(fooPub)
	serviceExport := &jwt.Export{Subject: "ngs.usage.*", Type: jwt.Service}
	fooAC.Exports.Add(serviceExport)
	fooJWT, err := fooAC.Encode(okp)
	if err != nil {
		t.Fatalf("Error generating account JWT: %v", err)
	}
	addAccountToMemResolver(s, fooPub, fooJWT)

	barKP, _ := nkeys.CreateAccount()
	barPub, _ := barKP.PublicKey()
	barAC := jwt.NewAccountClaims(barPub)
	serviceImport := &jwt.Import{Account: fooPub, Subject: "ngs.usage", To: "ngs.usage.DEREK", Type: jwt.Service}
	barAC.Imports.Add(serviceImport)
	barJWT, err := barAC.Encode(okp)
	if err != nil {
		t.Fatalf("Error generating account JWT: %v", err)
	}
	addAccountToMemResolver(s, barPub, barJWT)

	// Create a client that will send the request
	ca, cra, csa := createClient(t, s, barKP)
	defer ca.close()
	ca.parseAsync(csa)
	expectPong(t, cra)

	// Create the client that will respond to the requests.
	cb, crb, csb := createClient(t, s, fooKP)
	defer cb.close()
	cb.parseAsync(csb)
	expectPong(t, crb)

	// Create Subscriber.
	cb.parseAsync("SUB ngs.usage.* 1\r\nPING\r\n")
	expectPong(t, crb)

	// Send Request
	ca.parseAsync("PUB ngs.usage 2\r\nhi\r\nPING\r\n")
	expectPong(t, cra)

	// We should receive the request mapped into our account. PING needed to flush.
	cb.parseAsync("PING\r\n")
	expectMsg(t, crb, "ngs.usage.DEREK", "hi")

	// Now update to make the export private.
	fooACPrivate := jwt.NewAccountClaims(fooPub)
	serviceExport = &jwt.Export{Subject: "ngs.usage.*", Type: jwt.Service, TokenReq: true}
	fooACPrivate.Exports.Add(serviceExport)
	fooJWTPrivate, err := fooACPrivate.Encode(okp)
	if err != nil {
		t.Fatalf("Error generating account JWT: %v", err)
	}
	addAccountToMemResolver(s, fooPub, fooJWTPrivate)
	acc, _ := s.LookupAccount(fooPub)
	s.UpdateAccountClaims(acc, fooACPrivate)

	// Send Another Request
	ca.parseAsync("PUB ngs.usage 2\r\nhi\r\nPING\r\n")
	expectPong(t, cra)

	// We should not receive the request this time.
	cb.parseAsync("PING\r\n")
	expectPong(t, crb)

	// Now put it back again to public and make sure it works again.
	addAccountToMemResolver(s, fooPub, fooJWT)
	s.UpdateAccountClaims(acc, fooAC)

	// Send Request
	ca.parseAsync("PUB ngs.usage 2\r\nhi\r\nPING\r\n")
	expectPong(t, cra)

	// We should receive the request mapped into our account. PING needed to flush.
	cb.parseAsync("PING\r\n")
	expectMsg(t, crb, "ngs.usage.DEREK", "hi")
}

func TestJWTAccountServiceImportExpires(t *testing.T) {
	s := opTrustBasicSetup()
	defer s.Shutdown()
	buildMemAccResolver(s)

	okp, _ := nkeys.FromSeed(oSeed)

	// Create accounts and imports/exports.
	fooKP, _ := nkeys.CreateAccount()
	fooPub, _ := fooKP.PublicKey()
	fooAC := jwt.NewAccountClaims(fooPub)
	serviceExport := &jwt.Export{Subject: "foo", Type: jwt.Service}

	fooAC.Exports.Add(serviceExport)
	fooJWT, err := fooAC.Encode(okp)
	if err != nil {
		t.Fatalf("Error generating account JWT: %v", err)
	}
	addAccountToMemResolver(s, fooPub, fooJWT)

	barKP, _ := nkeys.CreateAccount()
	barPub, _ := barKP.PublicKey()
	barAC := jwt.NewAccountClaims(barPub)
	serviceImport := &jwt.Import{Account: fooPub, Subject: "foo", Type: jwt.Service}

	barAC.Imports.Add(serviceImport)
	barJWT, err := barAC.Encode(okp)
	if err != nil {
		t.Fatalf("Error generating account JWT: %v", err)
	}
	addAccountToMemResolver(s, barPub, barJWT)

	// Create a client that will send the request
	ca, cra, csa := createClient(t, s, barKP)
	defer ca.close()
	ca.parseAsync(csa)
	expectPong(t, cra)

	// Create the client that will respond to the requests.
	cb, crb, csb := createClient(t, s, fooKP)
	defer cb.close()
	cb.parseAsync(csb)
	expectPong(t, crb)

	// Create Subscriber.
	cb.parseAsync("SUB foo 1\r\nPING\r\n")
	expectPong(t, crb)

	// Send Request
	ca.parseAsync("PUB foo 2\r\nhi\r\nPING\r\n")
	expectPong(t, cra)

	// We should receive the request. PING needed to flush.
	cb.parseAsync("PING\r\n")
	expectMsg(t, crb, "foo", "hi")

	// Now update the exported service to require auth.
	fooAC = jwt.NewAccountClaims(fooPub)
	serviceExport = &jwt.Export{Subject: "foo", Type: jwt.Service, TokenReq: true}

	fooAC.Exports.Add(serviceExport)
	fooJWT, err = fooAC.Encode(okp)
	if err != nil {
		t.Fatalf("Error generating account JWT: %v", err)
	}
	addAccountToMemResolver(s, fooPub, fooJWT)
	acc, _ := s.LookupAccount(fooPub)
	s.UpdateAccountClaims(acc, fooAC)

	// Send Another Request
	ca.parseAsync("PUB foo 2\r\nhi\r\nPING\r\n")
	expectPong(t, cra)

	// We should not receive the request this time.
	cb.parseAsync("PING\r\n")
	expectPong(t, crb)

	// Now get an activation token such that it will work, but will expire.
	barAC = jwt.NewAccountClaims(barPub)
	serviceImport = &jwt.Import{Account: fooPub, Subject: "foo", Type: jwt.Service}

	now := time.Now()
	activation := jwt.NewActivationClaims(barPub)
	activation.ImportSubject = "foo"
	activation.ImportType = jwt.Service
	activation.IssuedAt = now.Add(-10 * time.Second).Unix()
	activation.Expires = now.Add(time.Second).Round(time.Second).Unix()
	actJWT, err := activation.Encode(fooKP)
	if err != nil {
		t.Fatalf("Error generating activation token: %v", err)
	}
	serviceImport.Token = actJWT

	barAC.Imports.Add(serviceImport)
	barJWT, err = barAC.Encode(okp)
	if err != nil {
		t.Fatalf("Error generating account JWT: %v", err)
	}
	addAccountToMemResolver(s, barPub, barJWT)
	acc, _ = s.LookupAccount(barPub)
	s.UpdateAccountClaims(acc, barAC)

	// Now it should work again.
	// Send Another Request
	ca.parseAsync("PUB foo 3\r\nhi2\r\nPING\r\n")
	expectPong(t, cra)

	// We should receive the request. PING needed to flush.
	cb.parseAsync("PING\r\n")
	expectMsg(t, crb, "foo", "hi2")

	// Now wait for it to expire, then retry.
	waitTime := time.Duration(activation.Expires-time.Now().Unix()) * time.Second
	time.Sleep(waitTime + 250*time.Millisecond)

	// Send Another Request
	ca.parseAsync("PUB foo 3\r\nhi3\r\nPING\r\n")
	expectPong(t, cra)

	// We should NOT receive the request. PING needed to flush.
	cb.parseAsync("PING\r\n")
	expectPong(t, crb)
}

func TestAccountURLResolver(t *testing.T) {
	for _, test := range []struct {
		name   string
		useTLS bool
	}{
		{"plain", false},
		{"tls", true},
	} {
		t.Run(test.name, func(t *testing.T) {
			kp, _ := nkeys.FromSeed(oSeed)
			akp, _ := nkeys.CreateAccount()
			apub, _ := akp.PublicKey()
			nac := jwt.NewAccountClaims(apub)
			ajwt, err := nac.Encode(kp)
			if err != nil {
				t.Fatalf("Error generating account JWT: %v", err)
			}

			hf := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Write([]byte(ajwt))
			})
			var ts *httptest.Server
			if test.useTLS {
				ts = httptest.NewTLSServer(hf)
			} else {
				ts = httptest.NewServer(hf)
			}
			defer ts.Close()

			confTemplate := `
				operator: %s
				listen: -1
				resolver: URL("%s/ngs/v1/accounts/jwt/")
				resolver_tls {
					insecure: true
				}
			`
			conf := createConfFile(t, []byte(fmt.Sprintf(confTemplate, ojwt, ts.URL)))
			defer os.Remove(conf)

			s, opts := RunServerWithConfig(conf)
			pub, _ := kp.PublicKey()
			opts.TrustedKeys = []string{pub}
			defer s.Shutdown()

			acc, _ := s.LookupAccount(apub)
			if acc == nil {
				t.Fatalf("Expected to receive an account")
			}
			if acc.Name != apub {
				t.Fatalf("Account name did not match claim key")
			}
		})
	}
}

func TestAccountURLResolverTimeout(t *testing.T) {
	kp, _ := nkeys.FromSeed(oSeed)
	akp, _ := nkeys.CreateAccount()
	apub, _ := akp.PublicKey()
	nac := jwt.NewAccountClaims(apub)
	ajwt, err := nac.Encode(kp)
	if err != nil {
		t.Fatalf("Error generating account JWT: %v", err)
	}

	basePath := "/ngs/v1/accounts/jwt/"

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == basePath {
			w.Write([]byte("ok"))
			return
		}
		// Purposely be slow on account lookup.
		time.Sleep(200 * time.Millisecond)
		w.Write([]byte(ajwt))
	}))
	defer ts.Close()

	confTemplate := `
		listen: -1
		resolver: URL("%s%s")
    `
	conf := createConfFile(t, []byte(fmt.Sprintf(confTemplate, ts.URL, basePath)))
	defer os.Remove(conf)

	s, opts := RunServerWithConfig(conf)
	pub, _ := kp.PublicKey()
	opts.TrustedKeys = []string{pub}
	defer s.Shutdown()

	// Lower default timeout to speed-up test
	s.AccountResolver().(*URLAccResolver).c.Timeout = 50 * time.Millisecond

	acc, _ := s.LookupAccount(apub)
	if acc != nil {
		t.Fatalf("Expected to not receive an account due to timeout")
	}
}

func TestAccountURLResolverNoFetchOnReload(t *testing.T) {
	kp, _ := nkeys.FromSeed(oSeed)
	akp, _ := nkeys.CreateAccount()
	apub, _ := akp.PublicKey()
	nac := jwt.NewAccountClaims(apub)
	ajwt, err := nac.Encode(kp)
	if err != nil {
		t.Fatalf("Error generating account JWT: %v", err)
	}

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte(ajwt))
	}))
	defer ts.Close()

	confTemplate := `
		operator: %s
		listen: -1
		resolver: URL("%s/ngs/v1/accounts/jwt/")
    `
	conf := createConfFile(t, []byte(fmt.Sprintf(confTemplate, ojwt, ts.URL)))
	defer os.Remove(conf)

	s, _ := RunServerWithConfig(conf)
	defer s.Shutdown()

	acc, _ := s.LookupAccount(apub)
	if acc == nil {
		t.Fatalf("Expected to receive an account")
	}

	// Reload would produce a DATA race during the DeepEqual check for the account resolver,
	// so close the current one and we will create a new one that keeps track of fetch calls.
	ts.Close()

	fetch := int32(0)
	ts = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&fetch, 1)
		w.Write([]byte(ajwt))
	}))
	defer ts.Close()

	changeCurrentConfigContentWithNewContent(t, conf, []byte(fmt.Sprintf(confTemplate, ojwt, ts.URL)))

	if err := s.Reload(); err != nil {
		t.Fatalf("Error on reload: %v", err)
	}
	if atomic.LoadInt32(&fetch) != 0 {
		t.Fatalf("Fetch invoked during reload")
	}

	// Now stop the resolver and make sure that on startup, we report URL resolver failure
	s.Shutdown()
	s = nil
	ts.Close()

	opts := LoadConfig(conf)
	if s, err := NewServer(opts); err == nil || !strings.Contains(err.Error(), "could not fetch") {
		if s != nil {
			s.Shutdown()
		}
		t.Fatalf("Expected error regarding account resolver, got %v", err)
	}
}

func TestAccountURLResolverFetchFailureInServer1(t *testing.T) {
	const subj = "test"
	const crossAccSubj = "test"
	// Create Exporting Account
	expkp, _ := nkeys.CreateAccount()
	exppub, _ := expkp.PublicKey()
	expac := jwt.NewAccountClaims(exppub)
	expac.Exports.Add(&jwt.Export{
		Subject: crossAccSubj,
		Type:    jwt.Stream,
	})
	expjwt, err := expac.Encode(oKp)
	if err != nil {
		t.Fatalf("Error generating account JWT: %v", err)
	}
	// Create importing Account
	impkp, _ := nkeys.CreateAccount()
	imppub, _ := impkp.PublicKey()
	impac := jwt.NewAccountClaims(imppub)
	impac.Imports.Add(&jwt.Import{
		Account: exppub,
		Subject: crossAccSubj,
		Type:    jwt.Stream,
	})
	impjwt, err := impac.Encode(oKp)
	if err != nil {
		t.Fatalf("Error generating account JWT: %v", err)
	}
	// Simulate an account server that drops the first request to exppub
	chanImpA := make(chan struct{}, 10)
	defer close(chanImpA)
	chanExpS := make(chan struct{}, 10)
	defer close(chanExpS)
	chanExpF := make(chan struct{}, 1)
	defer close(chanExpF)
	failureCnt := int32(0)
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/A/" {
			// Server startup
			w.Write(nil)
			chanImpA <- struct{}{}
		} else if r.URL.Path == "/A/"+imppub {
			w.Write([]byte(impjwt))
			chanImpA <- struct{}{}
		} else if r.URL.Path == "/A/"+exppub {
			if atomic.AddInt32(&failureCnt, 1) <= 1 {
				// skip the write to simulate the failure
				chanExpF <- struct{}{}
			} else {
				w.Write([]byte(expjwt))
				chanExpS <- struct{}{}
			}
		} else {
			t.Fatal("not expected")
		}
	}))
	defer ts.Close()
	// Create server
	confA := createConfFile(t, []byte(fmt.Sprintf(`
		listen: -1
		operator: %s
		resolver: URL("%s/A/")
    `, ojwt, ts.URL)))
	defer os.Remove(confA)
	sA := RunServer(LoadConfig(confA))
	defer sA.Shutdown()
	// server observed one fetch on startup
	chanRecv(t, chanImpA, 10*time.Second)
	// Create first client
	ncA := natsConnect(t, sA.ClientURL(), createUserCreds(t, nil, impkp))
	defer ncA.Close()
	// create a test subscription
	subA, err := ncA.SubscribeSync(subj)
	if err != nil {
		t.Fatalf("Expected no error during subscribe: %v", err)
	}
	defer subA.Unsubscribe()
	// Connect of client triggered a fetch of both accounts
	// the fetch for the imported account will fail
	chanRecv(t, chanImpA, 10*time.Second)
	chanRecv(t, chanExpF, 10*time.Second)
	// create second client for user exporting
	ncB := natsConnect(t, sA.ClientURL(), createUserCreds(t, nil, expkp))
	defer ncB.Close()
	chanRecv(t, chanExpS, 10*time.Second)
	// Connect of client triggered another fetch, this time passing
	checkSubInterest(t, sA, imppub, subj, 10*time.Second)
	checkSubInterest(t, sA, exppub, crossAccSubj, 10*time.Second) // Will fail as a result of this issue
}

func TestAccountURLResolverFetchFailurePushReorder(t *testing.T) {
	const subj = "test"
	const crossAccSubj = "test"
	// Create System Account
	syskp, _ := nkeys.CreateAccount()
	syspub, _ := syskp.PublicKey()
	sysAc := jwt.NewAccountClaims(syspub)
	sysjwt, err := sysAc.Encode(oKp)
	if err != nil {
		t.Fatalf("Error generating account JWT: %v", err)
	}
	// Create Exporting Account
	expkp, _ := nkeys.CreateAccount()
	exppub, _ := expkp.PublicKey()
	expac := jwt.NewAccountClaims(exppub)
	expjwt1, err := expac.Encode(oKp)
	if err != nil {
		t.Fatalf("Error generating account JWT: %v", err)
	}
	expac.Exports.Add(&jwt.Export{
		Subject: crossAccSubj,
		Type:    jwt.Stream,
	})
	expjwt2, err := expac.Encode(oKp)
	if err != nil {
		t.Fatalf("Error generating account JWT: %v", err)
	}
	// Create importing Account
	impkp, _ := nkeys.CreateAccount()
	imppub, _ := impkp.PublicKey()
	impac := jwt.NewAccountClaims(imppub)
	impac.Imports.Add(&jwt.Import{
		Account: exppub,
		Subject: crossAccSubj,
		Type:    jwt.Stream,
	})
	impjwt, err := impac.Encode(oKp)
	if err != nil {
		t.Fatalf("Error generating account JWT: %v", err)
	}
	// Simulate an account server that does not serve the updated jwt for exppub
	chanImpA := make(chan struct{}, 10)
	defer close(chanImpA)
	chanExpS := make(chan struct{}, 10)
	defer close(chanExpS)
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/A/" {
			// Server startup
			w.Write(nil)
			chanImpA <- struct{}{}
		} else if r.URL.Path == "/A/"+imppub {
			w.Write([]byte(impjwt))
			chanImpA <- struct{}{}
		} else if r.URL.Path == "/A/"+exppub {
			// respond with jwt that does not have the export
			// this simulates an ordering issue
			w.Write([]byte(expjwt1))
			chanExpS <- struct{}{}
		} else if r.URL.Path == "/A/"+syspub {
			w.Write([]byte(sysjwt))
		} else {
			t.Fatal("not expected")
		}
	}))
	defer ts.Close()
	confA := createConfFile(t, []byte(fmt.Sprintf(`
		listen: -1
		operator: %s
		resolver: URL("%s/A/")
		system_account: %s
    `, ojwt, ts.URL, syspub)))
	defer os.Remove(confA)
	sA := RunServer(LoadConfig(confA))
	defer sA.Shutdown()
	// server observed one fetch on startup
	chanRecv(t, chanImpA, 10*time.Second)
	// Create first client
	ncA := natsConnect(t, sA.ClientURL(), createUserCreds(t, nil, impkp))
	defer ncA.Close()
	// create a test subscription
	subA, err := ncA.SubscribeSync(subj)
	if err != nil {
		t.Fatalf("Expected no error during subscribe: %v", err)
	}
	defer subA.Unsubscribe()
	// Connect of client triggered a fetch of both accounts
	// the fetch for the imported account will fail
	chanRecv(t, chanImpA, 10*time.Second)
	chanRecv(t, chanExpS, 10*time.Second)
	// create second client for user exporting
	ncB := natsConnect(t, sA.ClientURL(), createUserCreds(t, nil, expkp))
	defer ncB.Close()
	// update expjwt2, this will correct the import issue
	sysc := natsConnect(t, sA.ClientURL(), createUserCreds(t, nil, syskp))
	defer sysc.Close()
	natsPub(t, sysc, fmt.Sprintf(accUpdateEventSubjNew, exppub), []byte(expjwt2))
	sysc.Flush()
	// updating expjwt should cause this to pass
	checkSubInterest(t, sA, imppub, subj, 10*time.Second)
	checkSubInterest(t, sA, exppub, crossAccSubj, 10*time.Second) // Will fail as a result of this issue
}

type captureDebugLogger struct {
	DummyLogger
	dbgCh chan string
}

func (l *captureDebugLogger) Debugf(format string, v ...interface{}) {
	select {
	case l.dbgCh <- fmt.Sprintf(format, v...):
	default:
	}
}

func TestAccountURLResolverPermanentFetchFailure(t *testing.T) {
	const crossAccSubj = "test"
	expkp, _ := nkeys.CreateAccount()
	exppub, _ := expkp.PublicKey()
	impkp, _ := nkeys.CreateAccount()
	imppub, _ := impkp.PublicKey()
	// Create System Account
	syskp, _ := nkeys.CreateAccount()
	syspub, _ := syskp.PublicKey()
	sysAc := jwt.NewAccountClaims(syspub)
	sysjwt, err := sysAc.Encode(oKp)
	if err != nil {
		t.Fatalf("Error generating account JWT: %v", err)
	}
	// Create 2 Accounts. Each importing from the other, but NO matching export
	expac := jwt.NewAccountClaims(exppub)
	expac.Imports.Add(&jwt.Import{
		Account: imppub,
		Subject: crossAccSubj,
		Type:    jwt.Stream,
	})
	expjwt, err := expac.Encode(oKp)
	if err != nil {
		t.Fatalf("Error generating account JWT: %v", err)
	}
	// Create importing Account
	impac := jwt.NewAccountClaims(imppub)
	impac.Imports.Add(&jwt.Import{
		Account: exppub,
		Subject: crossAccSubj,
		Type:    jwt.Stream,
	})
	impjwt, err := impac.Encode(oKp)
	if err != nil {
		t.Fatalf("Error generating account JWT: %v", err)
	}
	// Simulate an account server that does not serve the updated jwt for exppub
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/A/" {
			// Server startup
			w.Write(nil)
		} else if r.URL.Path == "/A/"+imppub {
			w.Write([]byte(impjwt))
		} else if r.URL.Path == "/A/"+exppub {
			w.Write([]byte(expjwt))
		} else if r.URL.Path == "/A/"+syspub {
			w.Write([]byte(sysjwt))
		} else {
			t.Fatal("not expected")
		}
	}))
	defer ts.Close()
	confA := createConfFile(t, []byte(fmt.Sprintf(`
		listen: -1
		operator: %s
		resolver: URL("%s/A/")
		system_account: %s
    `, ojwt, ts.URL, syspub)))
	defer os.Remove(confA)
	o := LoadConfig(confA)
	sA := RunServer(o)
	defer sA.Shutdown()
	l := &captureDebugLogger{dbgCh: make(chan string, 100)} // has enough space to not block
	sA.SetLogger(l, true, false)
	// Create clients
	ncA := natsConnect(t, sA.ClientURL(), createUserCreds(t, nil, impkp))
	defer ncA.Close()
	ncB := natsConnect(t, sA.ClientURL(), createUserCreds(t, nil, expkp))
	defer ncB.Close()
	sysc := natsConnect(t, sA.ClientURL(), createUserCreds(t, nil, syskp))
	defer sysc.Close()
	// push accounts
	natsPub(t, sysc, fmt.Sprintf(accUpdateEventSubjNew, imppub), []byte(impjwt))
	natsPub(t, sysc, fmt.Sprintf(accUpdateEventSubjOld, exppub), []byte(expjwt))
	sysc.Flush()
	importErrCnt := 0
	tmr := time.NewTimer(500 * time.Millisecond)
	defer tmr.Stop()
	for {
		select {
		case line := <-l.dbgCh:
			if strings.HasPrefix(line, "Error adding stream import to account") {
				importErrCnt++
			}
		case <-tmr.C:
			// connecting and updating, each cause 3 traces (2 + 1 on iteration)
			if importErrCnt != 6 {
				t.Fatalf("Expected 6 debug traces, got %d", importErrCnt)
			}
			return
		}
	}
}

func TestAccountURLResolverFetchFailureInCluster(t *testing.T) {
	assertChanLen := func(x int, chans ...chan struct{}) {
		t.Helper()
		for _, c := range chans {
			if len(c) != x {
				t.Fatalf("length of channel is not %d", x)
			}
		}
	}
	const subj = ">"
	const crossAccSubj = "test"
	// Create Exporting Account
	expkp, _ := nkeys.CreateAccount()
	exppub, _ := expkp.PublicKey()
	expac := jwt.NewAccountClaims(exppub)
	expac.Exports.Add(&jwt.Export{
		Subject: crossAccSubj,
		Type:    jwt.Stream,
	})
	expjwt, err := expac.Encode(oKp)
	if err != nil {
		t.Fatalf("Error generating account JWT: %v", err)
	}
	// Create importing Account
	impkp, _ := nkeys.CreateAccount()
	imppub, _ := impkp.PublicKey()
	impac := jwt.NewAccountClaims(imppub)
	impac.Imports.Add(&jwt.Import{
		Account: exppub,
		Subject: crossAccSubj,
		Type:    jwt.Stream,
	})
	impac.Exports.Add(&jwt.Export{
		Subject: "srvc",
		Type:    jwt.Service,
	})
	impjwt, err := impac.Encode(oKp)
	if err != nil {
		t.Fatalf("Error generating account JWT: %v", err)
	}
	// Create User
	nkp, _ := nkeys.CreateUser()
	uSeed, _ := nkp.Seed()
	upub, _ := nkp.PublicKey()
	nuc := newJWTTestUserClaims()
	nuc.Subject = upub
	uJwt, err := nuc.Encode(impkp)
	if err != nil {
		t.Fatalf("Error generating user JWT: %v", err)
	}
	creds := genCredsFile(t, uJwt, uSeed)
	defer os.Remove(creds)
	// Simulate an account server that drops the first request to /B/acc
	chanImpA := make(chan struct{}, 4)
	defer close(chanImpA)
	chanImpB := make(chan struct{}, 4)
	defer close(chanImpB)
	chanExpA := make(chan struct{}, 4)
	defer close(chanExpA)
	chanExpB := make(chan struct{}, 4)
	defer close(chanExpB)
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/A/" {
			// Server A startup
			w.Write(nil)
			chanImpA <- struct{}{}
		} else if r.URL.Path == "/B/" {
			// Server B startup
			w.Write(nil)
			chanImpB <- struct{}{}
		} else if r.URL.Path == "/A/"+imppub {
			// First Client connecting to Server A
			w.Write([]byte(impjwt))
			chanImpA <- struct{}{}
		} else if r.URL.Path == "/B/"+imppub {
			// Second Client connecting to Server B
			w.Write([]byte(impjwt))
			chanImpB <- struct{}{}
		} else if r.URL.Path == "/A/"+exppub {
			// First Client connecting to Server A
			w.Write([]byte(expjwt))
			chanExpA <- struct{}{}
		} else if r.URL.Path == "/B/"+exppub {
			// Second Client connecting to Server B
			w.Write([]byte(expjwt))
			chanExpB <- struct{}{}
		} else {
			t.Fatal("not expected")
		}
	}))
	defer ts.Close()
	// Create seed server A
	confA := createConfFile(t, []byte(fmt.Sprintf(`
		listen: -1
		operator: %s
		resolver: URL("%s/A/")
		cluster {
			name: clust
			no_advertise: true
			listen: -1
		}
    `, ojwt, ts.URL)))
	defer os.Remove(confA)
	sA := RunServer(LoadConfig(confA))
	defer sA.Shutdown()
	// Create Server B (using no_advertise to prevent failover)
	confB := createConfFile(t, []byte(fmt.Sprintf(`
		listen: -1
		operator: %s
		resolver: URL("%s/B/")
		cluster {
			name: clust
			no_advertise: true
			listen: -1
			routes [
				nats-route://localhost:%d
			]
		}
    `, ojwt, ts.URL, sA.opts.Cluster.Port)))
	defer os.Remove(confB)
	sB := RunServer(LoadConfig(confB))
	defer sB.Shutdown()
	// startup cluster
	checkClusterFormed(t, sA, sB)
	// Both server observed one fetch on startup
	chanRecv(t, chanImpA, 10*time.Second)
	chanRecv(t, chanImpB, 10*time.Second)
	assertChanLen(0, chanImpA, chanImpB, chanExpA, chanExpB)
	// Create first client, directly connects to A
	urlA := fmt.Sprintf("nats://%s:%d", sA.opts.Host, sA.opts.Port)
	ncA, err := nats.Connect(urlA, nats.UserCredentials(creds),
		nats.DisconnectErrHandler(func(_ *nats.Conn, err error) {
			if err != nil {
				t.Fatal("error not expected in this test", err)
			}
		}),
		nats.ErrorHandler(func(_ *nats.Conn, _ *nats.Subscription, err error) {
			t.Fatal("error not expected in this test", err)
		}),
	)
	if err != nil {
		t.Fatalf("Expected to connect, got %v %s", err, urlA)
	}
	defer ncA.Close()
	// create a test subscription
	subA, err := ncA.SubscribeSync(subj)
	if err != nil {
		t.Fatalf("Expected no error during subscribe: %v", err)
	}
	defer subA.Unsubscribe()
	// Connect of client triggered a fetch by Server A
	chanRecv(t, chanImpA, 10*time.Second)
	chanRecv(t, chanExpA, 10*time.Second)
	assertChanLen(0, chanImpA, chanImpB, chanExpA, chanExpB)
	//time.Sleep(10 * time.Second)
	// create second client, directly connect to B
	urlB := fmt.Sprintf("nats://%s:%d", sB.opts.Host, sB.opts.Port)
	ncB, err := nats.Connect(urlB, nats.UserCredentials(creds), nats.NoReconnect())
	if err != nil {
		t.Fatalf("Expected to connect, got %v %s", err, urlB)
	}
	defer ncB.Close()
	// Connect of client triggered a fetch by Server B
	chanRecv(t, chanImpB, 10*time.Second)
	chanRecv(t, chanExpB, 10*time.Second)
	assertChanLen(0, chanImpA, chanImpB, chanExpA, chanExpB)
	checkClusterFormed(t, sA, sB)
	// the route subscription was lost due to the failed fetch
	// Now we test if some recover mechanism is in play
	checkSubInterest(t, sB, imppub, subj, 10*time.Second)         // Will fail as a result of this issue
	checkSubInterest(t, sB, exppub, crossAccSubj, 10*time.Second) // Will fail as a result of this issue
	if err := ncB.Publish(subj, []byte("msg")); err != nil {
		t.Fatalf("Expected to publish %v", err)
	}
	// expect the message from B to flow to A
	if m, err := subA.NextMsg(10 * time.Second); err != nil {
		t.Fatalf("Expected to receive a message %v", err)
	} else if string(m.Data) != "msg" {
		t.Fatalf("Expected to receive 'msg', got: %s", string(m.Data))
	}
	assertChanLen(0, chanImpA, chanImpB, chanExpA, chanExpB)
}

func TestAccountURLResolverReturnDifferentOperator(t *testing.T) {
	// Create a valid chain of op/acc/usr using a different operator
	// This is so we can test if the server rejects this chain.
	// Create Operator
	op, _ := nkeys.CreateOperator()
	// Create Account, this account is the one returned by the resolver
	akp, _ := nkeys.CreateAccount()
	apub, _ := akp.PublicKey()
	nac := jwt.NewAccountClaims(apub)
	ajwt, err := nac.Encode(op)
	if err != nil {
		t.Fatalf("Error generating account JWT: %v", err)
	}
	// Create User
	nkp, _ := nkeys.CreateUser()
	uSeed, _ := nkp.Seed()
	upub, _ := nkp.PublicKey()
	nuc := newJWTTestUserClaims()
	nuc.Subject = upub
	uJwt, err := nuc.Encode(akp)
	if err != nil {
		t.Fatalf("Error generating user JWT: %v", err)
	}
	creds := genCredsFile(t, uJwt, uSeed)
	defer os.Remove(creds)
	// Simulate an account server that was hijacked/mis configured
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte(ajwt))
	}))
	defer ts.Close()
	// Create Server
	confA := createConfFile(t, []byte(fmt.Sprintf(`
		listen: -1
		operator: %s
		resolver: URL("%s/A/")
    `, ojwt, ts.URL)))
	defer os.Remove(confA)
	sA, _ := RunServerWithConfig(confA)
	defer sA.Shutdown()
	// Create first client, directly connects to A
	urlA := fmt.Sprintf("nats://%s:%d", sA.opts.Host, sA.opts.Port)
	if _, err := nats.Connect(urlA, nats.UserCredentials(creds),
		nats.DisconnectErrHandler(func(_ *nats.Conn, err error) {
			if err != nil {
				t.Fatal("error not expected in this test", err)
			}
		}),
		nats.ErrorHandler(func(_ *nats.Conn, _ *nats.Subscription, err error) {
			t.Fatal("error not expected in this test", err)
		}),
	); err == nil {
		t.Fatal("Expected connect to fail")
	}
	// Test if the server has the account in memory. (shouldn't)
	if v, ok := sA.accounts.Load(apub); ok {
		t.Fatalf("Expected account to NOT be in memory: %v", v.(*Account))
	}
}

func TestJWTUserSigningKey(t *testing.T) {
	s := opTrustBasicSetup()
	defer s.Shutdown()

	// Check to make sure we would have an authTimer
	if !s.info.AuthRequired {
		t.Fatalf("Expect the server to require auth")
	}

	c, cr, _ := newClientForServer(s)
	defer c.close()
	// Don't send jwt field, should fail.
	c.parseAsync("CONNECT {\"verbose\":true,\"pedantic\":true}\r\nPING\r\n")
	l, _ := cr.ReadString('\n')
	if !strings.HasPrefix(l, "-ERR ") {
		t.Fatalf("Expected an error")
	}

	okp, _ := nkeys.FromSeed(oSeed)

	// Create an account
	akp, _ := nkeys.CreateAccount()
	apub, _ := akp.PublicKey()

	// Create a signing key for the account
	askp, _ := nkeys.CreateAccount()
	aspub, _ := askp.PublicKey()

	nac := jwt.NewAccountClaims(apub)
	ajwt, err := nac.Encode(okp)
	if err != nil {
		t.Fatalf("Error generating account JWT: %v", err)
	}

	// Create a client with the account signing key
	c, cr, cs := createClientWithIssuer(t, s, askp, apub)
	defer c.close()

	// PING needed to flush the +OK/-ERR to us.
	// This should fail too since no account resolver is defined.
	c.parseAsync(cs)
	l, _ = cr.ReadString('\n')
	if !strings.HasPrefix(l, "-ERR ") {
		t.Fatalf("Expected an error")
	}

	// Ok now let's walk through and make sure all is good.
	// We will set the account resolver by hand to a memory resolver.
	buildMemAccResolver(s)
	addAccountToMemResolver(s, apub, ajwt)

	// Create a client with a signing key
	c, cr, cs = createClientWithIssuer(t, s, askp, apub)
	defer c.close()
	// should fail because the signing key is not known
	c.parseAsync(cs)
	l, _ = cr.ReadString('\n')
	if !strings.HasPrefix(l, "-ERR ") {
		t.Fatalf("Expected an error: %v", l)
	}

	// add a signing key
	nac.SigningKeys.Add(aspub)
	// update the memory resolver
	acc, _ := s.LookupAccount(apub)
	s.UpdateAccountClaims(acc, nac)

	// Create a client with a signing key
	c, cr, cs = createClientWithIssuer(t, s, askp, apub)
	defer c.close()

	// expect this to work
	c.parseAsync(cs)
	l, _ = cr.ReadString('\n')
	if !strings.HasPrefix(l, "PONG") {
		t.Fatalf("Expected a PONG, got %q", l)
	}

	isClosed := func() bool {
		c.mu.Lock()
		defer c.mu.Unlock()
		return c.isClosed()
	}

	if isClosed() {
		t.Fatal("expected client to be alive")
	}
	// remove the signing key should bounce client
	nac.SigningKeys = nil
	acc, _ = s.LookupAccount(apub)
	s.UpdateAccountClaims(acc, nac)

	if !isClosed() {
		t.Fatal("expected client to be gone")
	}
}

func TestJWTAccountImportSignerRemoved(t *testing.T) {
	s := opTrustBasicSetup()
	defer s.Shutdown()
	buildMemAccResolver(s)

	okp, _ := nkeys.FromSeed(oSeed)

	// Exporter keys
	srvKP, _ := nkeys.CreateAccount()
	srvPK, _ := srvKP.PublicKey()
	srvSignerKP, _ := nkeys.CreateAccount()
	srvSignerPK, _ := srvSignerKP.PublicKey()

	// Importer keys
	clientKP, _ := nkeys.CreateAccount()
	clientPK, _ := clientKP.PublicKey()

	createSrvJwt := func(signingKeys ...string) (string, *jwt.AccountClaims) {
		ac := jwt.NewAccountClaims(srvPK)
		ac.SigningKeys.Add(signingKeys...)
		ac.Exports.Add(&jwt.Export{Subject: "foo", Type: jwt.Service, TokenReq: true})
		ac.Exports.Add(&jwt.Export{Subject: "bar", Type: jwt.Stream, TokenReq: true})
		token, err := ac.Encode(okp)
		if err != nil {
			t.Fatalf("Error generating exporter JWT: %v", err)
		}
		return token, ac
	}

	createImportToken := func(sub string, kind jwt.ExportType) string {
		actC := jwt.NewActivationClaims(clientPK)
		actC.IssuerAccount = srvPK
		actC.ImportType = kind
		actC.ImportSubject = jwt.Subject(sub)
		token, err := actC.Encode(srvSignerKP)
		if err != nil {
			t.Fatal(err)
		}
		return token
	}

	createClientJwt := func() string {
		ac := jwt.NewAccountClaims(clientPK)
		ac.Imports.Add(&jwt.Import{Account: srvPK, Subject: "foo", Type: jwt.Service, Token: createImportToken("foo", jwt.Service)})
		ac.Imports.Add(&jwt.Import{Account: srvPK, Subject: "bar", Type: jwt.Stream, Token: createImportToken("bar", jwt.Stream)})
		token, err := ac.Encode(okp)
		if err != nil {
			t.Fatalf("Error generating importer JWT: %v", err)
		}
		return token
	}

	srvJWT, _ := createSrvJwt(srvSignerPK)
	addAccountToMemResolver(s, srvPK, srvJWT)

	clientJWT := createClientJwt()
	addAccountToMemResolver(s, clientPK, clientJWT)

	// Create a client that will send the request
	client, clientReader, clientCS := createClient(t, s, clientKP)
	defer client.close()
	client.parseAsync(clientCS)
	expectPong(t, clientReader)

	checkShadow := func(expected int) {
		t.Helper()
		client.mu.Lock()
		defer client.mu.Unlock()
		sub := client.subs["1"]
		count := 0
		if sub != nil {
			count = len(sub.shadow)
		}
		if count != expected {
			t.Fatalf("Expected shadows to be %d, got %d", expected, count)
		}
	}

	checkShadow(0)
	// Create the client that will respond to the requests.
	srv, srvReader, srvCS := createClient(t, s, srvKP)
	defer srv.close()
	srv.parseAsync(srvCS)
	expectPong(t, srvReader)

	// Create Subscriber.
	srv.parseAsync("SUB foo 1\r\nPING\r\n")
	expectPong(t, srvReader)

	// Send Request
	client.parseAsync("PUB foo 2\r\nhi\r\nPING\r\n")
	expectPong(t, clientReader)

	// We should receive the request. PING needed to flush.
	srv.parseAsync("PING\r\n")
	expectMsg(t, srvReader, "foo", "hi")

	client.parseAsync("SUB bar 1\r\nPING\r\n")
	expectPong(t, clientReader)
	checkShadow(1)

	srv.parseAsync("PUB bar 2\r\nhi\r\nPING\r\n")
	expectPong(t, srvReader)

	// We should receive from stream. PING needed to flush.
	client.parseAsync("PING\r\n")
	expectMsg(t, clientReader, "bar", "hi")

	// Now update the exported service no signer
	srvJWT, srvAC := createSrvJwt()
	addAccountToMemResolver(s, srvPK, srvJWT)
	acc, _ := s.LookupAccount(srvPK)
	s.UpdateAccountClaims(acc, srvAC)

	// Send Another Request
	client.parseAsync("PUB foo 2\r\nhi\r\nPING\r\n")
	expectPong(t, clientReader)

	// We should not receive the request this time.
	srv.parseAsync("PING\r\n")
	expectPong(t, srvReader)

	// Publish on the stream
	srv.parseAsync("PUB bar 2\r\nhi\r\nPING\r\n")
	expectPong(t, srvReader)

	// We should not receive from the stream this time
	client.parseAsync("PING\r\n")
	expectPong(t, clientReader)
	checkShadow(0)
}

func TestJWTAccountImportSignerDeadlock(t *testing.T) {
	s := opTrustBasicSetup()
	defer s.Shutdown()
	buildMemAccResolver(s)

	okp, _ := nkeys.FromSeed(oSeed)

	// Exporter keys
	srvKP, _ := nkeys.CreateAccount()
	srvPK, _ := srvKP.PublicKey()
	srvSignerKP, _ := nkeys.CreateAccount()
	srvSignerPK, _ := srvSignerKP.PublicKey()

	// Importer keys
	clientKP, _ := nkeys.CreateAccount()
	clientPK, _ := clientKP.PublicKey()

	createSrvJwt := func(signingKeys ...string) (string, *jwt.AccountClaims) {
		ac := jwt.NewAccountClaims(srvPK)
		ac.SigningKeys.Add(signingKeys...)
		ac.Exports.Add(&jwt.Export{Subject: "foo", Type: jwt.Service, TokenReq: true})
		ac.Exports.Add(&jwt.Export{Subject: "bar", Type: jwt.Stream, TokenReq: true})
		token, err := ac.Encode(okp)
		if err != nil {
			t.Fatalf("Error generating exporter JWT: %v", err)
		}
		return token, ac
	}

	createImportToken := func(sub string, kind jwt.ExportType) string {
		actC := jwt.NewActivationClaims(clientPK)
		actC.IssuerAccount = srvPK
		actC.ImportType = kind
		actC.ImportSubject = jwt.Subject(sub)
		token, err := actC.Encode(srvSignerKP)
		if err != nil {
			t.Fatal(err)
		}
		return token
	}

	createClientJwt := func() string {
		ac := jwt.NewAccountClaims(clientPK)
		ac.Imports.Add(&jwt.Import{Account: srvPK, Subject: "foo", Type: jwt.Service, Token: createImportToken("foo", jwt.Service)})
		ac.Imports.Add(&jwt.Import{Account: srvPK, Subject: "bar", Type: jwt.Stream, Token: createImportToken("bar", jwt.Stream)})
		token, err := ac.Encode(okp)
		if err != nil {
			t.Fatalf("Error generating importer JWT: %v", err)
		}
		return token
	}

	srvJWT, _ := createSrvJwt(srvSignerPK)
	addAccountToMemResolver(s, srvPK, srvJWT)

	clientJWT := createClientJwt()
	addAccountToMemResolver(s, clientPK, clientJWT)

	acc, _ := s.LookupAccount(srvPK)
	// Have a go routine that constantly gets/releases the acc's write lock.
	// There was a bug that could cause AddServiceImportWithClaim to deadlock.
	ch := make(chan bool, 1)
	wg := sync.WaitGroup{}
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-ch:
				return
			default:
				acc.mu.Lock()
				acc.mu.Unlock()
				time.Sleep(time.Millisecond)
			}
		}
	}()

	// Create a client that will send the request
	client, clientReader, clientCS := createClient(t, s, clientKP)
	defer client.close()
	client.parseAsync(clientCS)
	expectPong(t, clientReader)

	close(ch)
	wg.Wait()
}

func TestJWTAccountImportWrongIssuerAccount(t *testing.T) {
	s := opTrustBasicSetup()
	defer s.Shutdown()
	buildMemAccResolver(s)

	l := &captureErrorLogger{errCh: make(chan string, 2)}
	s.SetLogger(l, false, false)

	okp, _ := nkeys.FromSeed(oSeed)

	// Exporter keys
	srvKP, _ := nkeys.CreateAccount()
	srvPK, _ := srvKP.PublicKey()
	srvSignerKP, _ := nkeys.CreateAccount()
	srvSignerPK, _ := srvSignerKP.PublicKey()

	// Importer keys
	clientKP, _ := nkeys.CreateAccount()
	clientPK, _ := clientKP.PublicKey()

	createSrvJwt := func(signingKeys ...string) (string, *jwt.AccountClaims) {
		ac := jwt.NewAccountClaims(srvPK)
		ac.SigningKeys.Add(signingKeys...)
		ac.Exports.Add(&jwt.Export{Subject: "foo", Type: jwt.Service, TokenReq: true})
		ac.Exports.Add(&jwt.Export{Subject: "bar", Type: jwt.Stream, TokenReq: true})
		token, err := ac.Encode(okp)
		if err != nil {
			t.Fatalf("Error generating exporter JWT: %v", err)
		}
		return token, ac
	}

	createImportToken := func(sub string, kind jwt.ExportType) string {
		actC := jwt.NewActivationClaims(clientPK)
		// Reference ourselves, which is wrong.
		actC.IssuerAccount = clientPK
		actC.ImportType = kind
		actC.ImportSubject = jwt.Subject(sub)
		token, err := actC.Encode(srvSignerKP)
		if err != nil {
			t.Fatal(err)
		}
		return token
	}

	createClientJwt := func() string {
		ac := jwt.NewAccountClaims(clientPK)
		ac.Imports.Add(&jwt.Import{Account: srvPK, Subject: "foo", Type: jwt.Service, Token: createImportToken("foo", jwt.Service)})
		ac.Imports.Add(&jwt.Import{Account: srvPK, Subject: "bar", Type: jwt.Stream, Token: createImportToken("bar", jwt.Stream)})
		token, err := ac.Encode(okp)
		if err != nil {
			t.Fatalf("Error generating importer JWT: %v", err)
		}
		return token
	}

	srvJWT, _ := createSrvJwt(srvSignerPK)
	addAccountToMemResolver(s, srvPK, srvJWT)

	clientJWT := createClientJwt()
	addAccountToMemResolver(s, clientPK, clientJWT)

	// Create a client that will send the request
	client, clientReader, clientCS := createClient(t, s, clientKP)
	defer client.close()
	client.parseAsync(clientCS)
	if l, _, err := clientReader.ReadLine(); err != nil {
		t.Fatalf("Expected no Error, got: %v", err)
	} else if !strings.Contains(string(l), "-ERR 'Authorization Violation'") {
		t.Fatalf("Expected Error, got: %v", l)
	}
}

func TestJWTUserRevokedOnAccountUpdate(t *testing.T) {
	nac := newJWTTestAccountClaims()
	s, akp, c, cr := setupJWTTestWitAccountClaims(t, nac, "+OK")
	defer s.Shutdown()
	defer c.close()

	expectPong(t, cr)

	okp, _ := nkeys.FromSeed(oSeed)
	apub, _ := akp.PublicKey()

	c.mu.Lock()
	pub := c.user.Nkey
	c.mu.Unlock()

	// Now revoke the user.
	nac.Revoke(pub)

	ajwt, err := nac.Encode(okp)
	if err != nil {
		t.Fatalf("Error generating account JWT: %v", err)
	}

	// Update the account on the server.
	addAccountToMemResolver(s, apub, ajwt)
	acc, err := s.LookupAccount(apub)
	if err != nil {
		t.Fatalf("Error looking up the account: %v", err)
	}

	// This is simulating a system update for the account claims.
	go s.updateAccountWithClaimJWT(acc, ajwt)

	l, _ := cr.ReadString('\n')
	if !strings.HasPrefix(l, "-ERR ") {
		t.Fatalf("Expected an error")
	}
	if !strings.Contains(l, "Revoked") {
		t.Fatalf("Expected 'Revoked' to be in the error")
	}
}

func TestJWTUserRevoked(t *testing.T) {
	okp, _ := nkeys.FromSeed(oSeed)

	// Create a new user that we will make sure has been revoked.
	nkp, _ := nkeys.CreateUser()
	pub, _ := nkp.PublicKey()
	nuc := jwt.NewUserClaims(pub)

	akp, _ := nkeys.CreateAccount()
	apub, _ := akp.PublicKey()
	nac := jwt.NewAccountClaims(apub)
	// Revoke the user right away.
	nac.Revoke(pub)
	ajwt, err := nac.Encode(okp)
	if err != nil {
		t.Fatalf("Error generating account JWT: %v", err)
	}

	// Sign for the user.
	jwt, err := nuc.Encode(akp)
	if err != nil {
		t.Fatalf("Error generating user JWT: %v", err)
	}

	s := opTrustBasicSetup()
	defer s.Shutdown()
	buildMemAccResolver(s)
	addAccountToMemResolver(s, apub, ajwt)

	c, cr, l := newClientForServer(s)
	defer c.close()

	// Sign Nonce
	var info nonceInfo
	json.Unmarshal([]byte(l[5:]), &info)
	sigraw, _ := nkp.Sign([]byte(info.Nonce))
	sig := base64.RawURLEncoding.EncodeToString(sigraw)

	// PING needed to flush the +OK/-ERR to us.
	cs := fmt.Sprintf("CONNECT {\"jwt\":%q,\"sig\":\"%s\"}\r\nPING\r\n", jwt, sig)

	c.parseAsync(cs)

	l, _ = cr.ReadString('\n')
	if !strings.HasPrefix(l, "-ERR ") {
		t.Fatalf("Expected an error")
	}
	if !strings.Contains(l, "Authorization") {
		t.Fatalf("Expected 'Revoked' to be in the error")
	}
}

// Test that an account update that revokes an import authorization cancels the import.
func TestJWTImportTokenRevokedAfter(t *testing.T) {
	s := opTrustBasicSetup()
	defer s.Shutdown()
	buildMemAccResolver(s)

	okp, _ := nkeys.FromSeed(oSeed)

	// Create accounts and imports/exports.
	fooKP, _ := nkeys.CreateAccount()
	fooPub, _ := fooKP.PublicKey()
	fooAC := jwt.NewAccountClaims(fooPub)

	// Now create Exports.
	export := &jwt.Export{Subject: "foo.private", Type: jwt.Stream, TokenReq: true}

	fooAC.Exports.Add(export)
	fooJWT, err := fooAC.Encode(okp)
	if err != nil {
		t.Fatalf("Error generating account JWT: %v", err)
	}

	addAccountToMemResolver(s, fooPub, fooJWT)

	barKP, _ := nkeys.CreateAccount()
	barPub, _ := barKP.PublicKey()
	barAC := jwt.NewAccountClaims(barPub)
	simport := &jwt.Import{Account: fooPub, Subject: "foo.private", Type: jwt.Stream}

	activation := jwt.NewActivationClaims(barPub)
	activation.ImportSubject = "foo.private"
	activation.ImportType = jwt.Stream
	actJWT, err := activation.Encode(fooKP)
	if err != nil {
		t.Fatalf("Error generating activation token: %v", err)
	}

	simport.Token = actJWT
	barAC.Imports.Add(simport)
	barJWT, err := barAC.Encode(okp)
	if err != nil {
		t.Fatalf("Error generating account JWT: %v", err)
	}
	addAccountToMemResolver(s, barPub, barJWT)

	// Now revoke the export.
	decoded, _ := jwt.DecodeActivationClaims(actJWT)
	export.Revoke(decoded.Subject)

	fooJWT, err = fooAC.Encode(okp)
	if err != nil {
		t.Fatalf("Error generating account JWT: %v", err)
	}

	addAccountToMemResolver(s, fooPub, fooJWT)

	fooAcc, _ := s.LookupAccount(fooPub)
	if fooAcc == nil {
		t.Fatalf("Expected to retrieve the account")
	}

	// Now lookup bar account and make sure it was revoked.
	acc, _ := s.LookupAccount(barPub)
	if acc == nil {
		t.Fatalf("Expected to retrieve the account")
	}
	if les := len(acc.imports.streams); les != 0 {
		t.Fatalf("Expected imports streams len of 0, got %d", les)
	}
}

// Test that an account update that revokes an import authorization cancels the import.
func TestJWTImportTokenRevokedBefore(t *testing.T) {
	s := opTrustBasicSetup()
	defer s.Shutdown()
	buildMemAccResolver(s)

	okp, _ := nkeys.FromSeed(oSeed)

	// Create accounts and imports/exports.
	fooKP, _ := nkeys.CreateAccount()
	fooPub, _ := fooKP.PublicKey()
	fooAC := jwt.NewAccountClaims(fooPub)

	// Now create Exports.
	export := &jwt.Export{Subject: "foo.private", Type: jwt.Stream, TokenReq: true}

	fooAC.Exports.Add(export)

	// Import account
	barKP, _ := nkeys.CreateAccount()
	barPub, _ := barKP.PublicKey()
	barAC := jwt.NewAccountClaims(barPub)
	simport := &jwt.Import{Account: fooPub, Subject: "foo.private", Type: jwt.Stream}

	activation := jwt.NewActivationClaims(barPub)
	activation.ImportSubject = "foo.private"
	activation.ImportType = jwt.Stream
	actJWT, err := activation.Encode(fooKP)
	if err != nil {
		t.Fatalf("Error generating activation token: %v", err)
	}

	simport.Token = actJWT
	barAC.Imports.Add(simport)

	// Now revoke the export.
	decoded, _ := jwt.DecodeActivationClaims(actJWT)
	export.Revoke(decoded.Subject)

	fooJWT, err := fooAC.Encode(okp)
	if err != nil {
		t.Fatalf("Error generating account JWT: %v", err)
	}

	addAccountToMemResolver(s, fooPub, fooJWT)

	barJWT, err := barAC.Encode(okp)
	if err != nil {
		t.Fatalf("Error generating account JWT: %v", err)
	}
	addAccountToMemResolver(s, barPub, barJWT)

	fooAcc, _ := s.LookupAccount(fooPub)
	if fooAcc == nil {
		t.Fatalf("Expected to retrieve the account")
	}

	// Now lookup bar account and make sure it was revoked.
	acc, _ := s.LookupAccount(barPub)
	if acc == nil {
		t.Fatalf("Expected to retrieve the account")
	}
	if les := len(acc.imports.streams); les != 0 {
		t.Fatalf("Expected imports streams len of 0, got %d", les)
	}
}

func TestJWTCircularAccountServiceImport(t *testing.T) {
	s := opTrustBasicSetup()
	defer s.Shutdown()
	buildMemAccResolver(s)

	okp, _ := nkeys.FromSeed(oSeed)

	// Create accounts
	fooKP, _ := nkeys.CreateAccount()
	fooPub, _ := fooKP.PublicKey()
	fooAC := jwt.NewAccountClaims(fooPub)

	barKP, _ := nkeys.CreateAccount()
	barPub, _ := barKP.PublicKey()
	barAC := jwt.NewAccountClaims(barPub)

	// Create service export/import for account foo
	serviceExport := &jwt.Export{Subject: "foo", Type: jwt.Service, TokenReq: true}
	serviceImport := &jwt.Import{Account: barPub, Subject: "bar", Type: jwt.Service}

	fooAC.Exports.Add(serviceExport)
	fooAC.Imports.Add(serviceImport)
	fooJWT, err := fooAC.Encode(okp)
	if err != nil {
		t.Fatalf("Error generating account JWT: %v", err)
	}

	addAccountToMemResolver(s, fooPub, fooJWT)

	// Create service export/import for account bar
	serviceExport = &jwt.Export{Subject: "bar", Type: jwt.Service, TokenReq: true}
	serviceImport = &jwt.Import{Account: fooPub, Subject: "foo", Type: jwt.Service}

	barAC.Exports.Add(serviceExport)
	barAC.Imports.Add(serviceImport)
	barJWT, err := barAC.Encode(okp)
	if err != nil {
		t.Fatalf("Error generating account JWT: %v", err)
	}

	addAccountToMemResolver(s, barPub, barJWT)

	c, cr, cs := createClient(t, s, fooKP)
	defer c.close()

	c.parseAsync(cs)
	expectPong(t, cr)

	c.parseAsync("SUB foo 1\r\nPING\r\n")
	expectPong(t, cr)
}

// This test ensures that connected clients are properly evicted
// (no deadlock) if the max conns of an account has been lowered
// and the account is being updated (following expiration during
// a lookup).
func TestJWTAccountLimitsMaxConnsAfterExpired(t *testing.T) {
	s := opTrustBasicSetup()
	defer s.Shutdown()
	buildMemAccResolver(s)

	okp, _ := nkeys.FromSeed(oSeed)

	// Create accounts and imports/exports.
	fooKP, _ := nkeys.CreateAccount()
	fooPub, _ := fooKP.PublicKey()
	fooAC := jwt.NewAccountClaims(fooPub)
	fooAC.Limits.Conn = 10
	fooJWT, err := fooAC.Encode(okp)
	if err != nil {
		t.Fatalf("Error generating account JWT: %v", err)
	}
	addAccountToMemResolver(s, fooPub, fooJWT)

	newClient := func(expPre string) *testAsyncClient {
		t.Helper()
		// Create a client.
		c, cr, cs := createClient(t, s, fooKP)
		c.parseAsync(cs)
		l, _ := cr.ReadString('\n')
		if !strings.HasPrefix(l, expPre) {
			t.Fatalf("Expected a response starting with %q, got %q", expPre, l)
		}
		go func() {
			for {
				if _, _, err := cr.ReadLine(); err != nil {
					return
				}
			}
		}()
		return c
	}

	for i := 0; i < 4; i++ {
		c := newClient("PONG")
		defer c.close()
	}

	// We will simulate that the account has expired. When
	// a new client will connect, the server will do a lookup
	// and find the account expired, which then will cause
	// a fetch and a rebuild of the account. Since max conns
	// is now lower, some clients should have been removed.
	acc, _ := s.LookupAccount(fooPub)
	acc.mu.Lock()
	acc.expired = true
	acc.updated = time.Now().Add(-2 * time.Second) // work around updating to quickly
	acc.mu.Unlock()

	// Now update with new expiration and max connections lowered to 2
	fooAC.Limits.Conn = 2
	fooJWT, err = fooAC.Encode(okp)
	if err != nil {
		t.Fatalf("Error generating account JWT: %v", err)
	}
	addAccountToMemResolver(s, fooPub, fooJWT)

	// Cause the lookup that will detect that account was expired
	// and rebuild it, and kick clients out.
	c := newClient("-ERR ")
	defer c.close()

	acc, _ = s.LookupAccount(fooPub)
	checkFor(t, 2*time.Second, 15*time.Millisecond, func() error {
		acc.mu.RLock()
		numClients := len(acc.clients)
		acc.mu.RUnlock()
		if numClients != 2 {
			return fmt.Errorf("Should have 2 clients, got %v", numClients)
		}
		return nil
	})
}

func TestBearerToken(t *testing.T) {
	okp, _ := nkeys.FromSeed(oSeed)
	akp, _ := nkeys.CreateAccount()
	apub, _ := akp.PublicKey()
	nac := jwt.NewAccountClaims(apub)
	ajwt, err := nac.Encode(okp)
	if err != nil {
		t.Fatalf("Error generating account JWT: %v", err)
	}

	nkp, _ := nkeys.CreateUser()
	pub, _ := nkp.PublicKey()
	nuc := newJWTTestUserClaims()
	nuc.Subject = pub
	// Set bearer token.
	nuc.BearerToken = true
	jwt, err := nuc.Encode(akp)
	if err != nil {
		t.Fatalf("Error generating user JWT: %v", err)
	}

	s := opTrustBasicSetup()
	defer s.Shutdown()
	buildMemAccResolver(s)
	addAccountToMemResolver(s, apub, ajwt)

	c, cr, _ := newClientForServer(s)
	defer c.close()

	// Skip nonce signature...

	// PING needed to flush the +OK/-ERR to us.
	cs := fmt.Sprintf("CONNECT {\"jwt\":%q,\"verbose\":true,\"pedantic\":true}\r\nPING\r\n", jwt)
	wg := sync.WaitGroup{}
	wg.Add(1)
	go func() {
		c.parse([]byte(cs))
		wg.Done()
	}()
	l, _ := cr.ReadString('\n')
	if !strings.HasPrefix(l, "+OK") {
		t.Fatalf("Expected +OK, got %s", l)
	}
	wg.Wait()
}

func TestBearerWithIssuerSameAsAccountToken(t *testing.T) {
	okp, _ := nkeys.FromSeed(oSeed)
	akp, _ := nkeys.CreateAccount()
	apub, _ := akp.PublicKey()
	nac := jwt.NewAccountClaims(apub)
	ajwt, err := nac.Encode(okp)
	if err != nil {
		t.Fatalf("Error generating account JWT: %v", err)
	}

	nkp, _ := nkeys.CreateUser()
	pub, _ := nkp.PublicKey()
	nuc := newJWTTestUserClaims()
	// we are setting the issuer account here to trigger verification
	// of the issuer - the account has no signing keys, but the issuer
	// account is set to the public key of the account which should be OK.
	nuc.IssuerAccount = apub
	nuc.Subject = pub
	// Set bearer token.
	nuc.BearerToken = true
	jwt, err := nuc.Encode(akp)
	if err != nil {
		t.Fatalf("Error generating user JWT: %v", err)
	}

	s := opTrustBasicSetup()
	defer s.Shutdown()
	buildMemAccResolver(s)
	addAccountToMemResolver(s, apub, ajwt)

	c, cr, _ := newClientForServer(s)
	defer c.close()

	// Skip nonce signature...

	// PING needed to flush the +OK/-ERR to us.
	cs := fmt.Sprintf("CONNECT {\"jwt\":%q,\"verbose\":true,\"pedantic\":true}\r\nPING\r\n", jwt)
	wg := sync.WaitGroup{}
	wg.Add(1)
	go func() {
		c.parse([]byte(cs))
		wg.Done()
	}()
	l, _ := cr.ReadString('\n')
	if !strings.HasPrefix(l, "+OK") {
		t.Fatalf("Expected +OK, got %s", l)
	}
	wg.Wait()
}

func TestBearerWithBadIssuerToken(t *testing.T) {
	okp, _ := nkeys.FromSeed(oSeed)
	akp, _ := nkeys.CreateAccount()
	apub, _ := akp.PublicKey()
	nac := jwt.NewAccountClaims(apub)
	ajwt, err := nac.Encode(okp)
	if err != nil {
		t.Fatalf("Error generating account JWT: %v", err)
	}

	nkp, _ := nkeys.CreateUser()
	pub, _ := nkp.PublicKey()
	nuc := newJWTTestUserClaims()
	bakp, _ := nkeys.CreateAccount()
	bapub, _ := bakp.PublicKey()
	nuc.IssuerAccount = bapub
	nuc.Subject = pub
	// Set bearer token.
	nuc.BearerToken = true
	jwt, err := nuc.Encode(akp)
	if err != nil {
		t.Fatalf("Error generating user JWT: %v", err)
	}

	s := opTrustBasicSetup()
	defer s.Shutdown()
	buildMemAccResolver(s)
	addAccountToMemResolver(s, apub, ajwt)

	c, cr, _ := newClientForServer(s)
	defer c.close()

	// Skip nonce signature...

	// PING needed to flush the +OK/-ERR to us.
	cs := fmt.Sprintf("CONNECT {\"jwt\":%q,\"verbose\":true,\"pedantic\":true}\r\nPING\r\n", jwt)
	wg := sync.WaitGroup{}
	wg.Add(1)
	go func() {
		c.parse([]byte(cs))
		wg.Done()
	}()
	l, _ := cr.ReadString('\n')
	if !strings.HasPrefix(l, "-ERR") {
		t.Fatalf("Expected -ERR, got %s", l)
	}
	wg.Wait()
}

func TestExpiredUserCredentialsRenewal(t *testing.T) {
	createTmpFile := func(t *testing.T, content []byte) string {
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
	waitTime := func(ch chan bool, timeout time.Duration) error {
		select {
		case <-ch:
			return nil
		case <-time.After(timeout):
		}
		return errors.New("timeout")
	}

	okp, _ := nkeys.FromSeed(oSeed)
	akp, err := nkeys.CreateAccount()
	if err != nil {
		t.Fatalf("Error generating account")
	}
	aPub, _ := akp.PublicKey()
	nac := jwt.NewAccountClaims(aPub)
	aJwt, err := nac.Encode(okp)
	if err != nil {
		t.Fatalf("Error generating account JWT: %v", err)
	}

	kp, _ := nkeys.FromSeed(oSeed)
	oPub, _ := kp.PublicKey()
	opts := defaultServerOptions
	opts.TrustedKeys = []string{oPub}
	s := RunServer(&opts)
	if s == nil {
		t.Fatal("Server did not start")
	}
	defer s.Shutdown()
	buildMemAccResolver(s)
	addAccountToMemResolver(s, aPub, aJwt)

	nkp, _ := nkeys.CreateUser()
	pub, _ := nkp.PublicKey()
	uSeed, _ := nkp.Seed()
	nuc := newJWTTestUserClaims()
	nuc.Subject = pub
	nuc.Expires = time.Now().Add(time.Second).Unix()
	uJwt, err := nuc.Encode(akp)
	if err != nil {
		t.Fatalf("Error generating user JWT: %v", err)
	}

	creds, err := jwt.FormatUserConfig(uJwt, uSeed)
	if err != nil {
		t.Fatalf("Error encoding credentials: %v", err)
	}
	chainedFile := createTmpFile(t, creds)
	defer os.Remove(chainedFile)

	rch := make(chan bool)

	url := fmt.Sprintf("nats://%s:%d", s.opts.Host, s.opts.Port)
	nc, err := nats.Connect(url,
		nats.UserCredentials(chainedFile),
		nats.ReconnectWait(25*time.Millisecond),
		nats.ReconnectJitter(0, 0),
		nats.MaxReconnects(2),
		nats.ErrorHandler(noOpErrHandler),
		nats.ReconnectHandler(func(nc *nats.Conn) {
			rch <- true
		}),
	)
	if err != nil {
		t.Fatalf("Expected to connect, got %v %s", err, url)
	}
	defer nc.Close()

	// Place new credentials underneath.
	nuc.Expires = time.Now().Add(30 * time.Second).Unix()
	uJwt, err = nuc.Encode(akp)
	if err != nil {
		t.Fatalf("Error encoding user jwt: %v", err)
	}
	creds, err = jwt.FormatUserConfig(uJwt, uSeed)
	if err != nil {
		t.Fatalf("Error encoding credentials: %v", err)
	}
	if err := ioutil.WriteFile(chainedFile, creds, 0666); err != nil {
		t.Fatalf("Error writing conf file: %v", err)
	}

	// Make sure we get disconnected and reconnected first.
	if err := waitTime(rch, 2*time.Second); err != nil {
		t.Fatal("Should have reconnected.")
	}

	// We should not have been closed.
	if nc.IsClosed() {
		t.Fatal("Got disconnected when we should have reconnected.")
	}

	// Check that we clear the lastErr that can cause the disconnect.
	// Our reconnect CB will happen before the clear. So check after a bit.
	time.Sleep(50 * time.Millisecond)
	if nc.LastError() != nil {
		t.Fatalf("Expected lastErr to be cleared, got %q", nc.LastError())
	}
}

func updateJwt(t *testing.T, url string, creds string, jwt string, respCnt int) int {
	t.Helper()
	require_NextMsg := func(sub *nats.Subscription) bool {
		t.Helper()
		msg := natsNexMsg(t, sub, time.Second)
		content := make(map[string]interface{})
		json.Unmarshal(msg.Data, &content)
		if _, ok := content["data"]; ok {
			return true
		}
		return false
	}
	c := natsConnect(t, url, nats.UserCredentials(creds),
		nats.DisconnectErrHandler(func(_ *nats.Conn, err error) {
			if err != nil {
				t.Fatal("error not expected in this test", err)
			}
		}),
		nats.ErrorHandler(func(_ *nats.Conn, _ *nats.Subscription, err error) {
			t.Fatal("error not expected in this test", err)
		}),
	)
	defer c.Close()
	resp := c.NewRespInbox()
	sub := natsSubSync(t, c, resp)
	err := sub.AutoUnsubscribe(respCnt)
	require_NoError(t, err)
	require_NoError(t, c.PublishRequest(accClaimsReqSubj, resp, []byte(jwt)))
	passCnt := 0
	for i := 0; i < respCnt; i++ {
		if require_NextMsg(sub) {
			passCnt++
		}
	}
	return passCnt
}

func require_JWTAbsent(t *testing.T, dir string, pub string) {
	t.Helper()
	_, err := os.Stat(filepath.Join(dir, pub+".jwt"))
	require_Error(t, err)
	require_True(t, os.IsNotExist(err))
}

func require_JWTPresent(t *testing.T, dir string, pub string) {
	t.Helper()
	_, err := os.Stat(filepath.Join(dir, pub+".jwt"))
	require_NoError(t, err)
}

func require_JWTEqual(t *testing.T, dir string, pub string, jwt string) {
	t.Helper()
	content, err := ioutil.ReadFile(filepath.Join(dir, pub+".jwt"))
	require_NoError(t, err)
	require_Equal(t, string(content), jwt)
}

func createDir(t *testing.T, prefix string) string {
	t.Helper()
	dir, err := ioutil.TempDir("", prefix)
	require_NoError(t, err)
	return dir
}

func writeJWT(t *testing.T, dir string, pub string, jwt string) {
	t.Helper()
	err := ioutil.WriteFile(filepath.Join(dir, pub+".jwt"), []byte(jwt), 0644)
	require_NoError(t, err)
}

func TestAccountNATSResolverFetch(t *testing.T) {
	origEventsHBInterval := eventsHBInterval
	eventsHBInterval = 50 * time.Millisecond // speed up eventing
	defer func() { eventsHBInterval = origEventsHBInterval }()
	require_NoLocalOrRemoteConnections := func(account string, srvs ...*Server) {
		t.Helper()
		for _, srv := range srvs {
			if acc, ok := srv.accounts.Load(account); ok {
				checkAccClientsCount(t, acc.(*Account), 0)
			}
		}
	}
	// After each connection check, require_XConnection and connect assures that
	// listed server have no connections for the account used
	require_1Connection := func(url, creds, acc string, srvs ...*Server) {
		t.Helper()
		func() {
			t.Helper()
			c := natsConnect(t, url, nats.UserCredentials(creds))
			defer c.Close()
			if _, err := nats.Connect(url, nats.UserCredentials(creds)); err == nil {
				t.Fatal("Second connection was supposed to fail due to limits")
			} else if !strings.Contains(err.Error(), ErrTooManyAccountConnections.Error()) {
				t.Fatal("Second connection was supposed to fail with too many conns")
			}
		}()
		require_NoLocalOrRemoteConnections(acc, srvs...)
	}
	require_2Connection := func(url, creds, acc string, srvs ...*Server) {
		t.Helper()
		func() {
			t.Helper()
			c1 := natsConnect(t, url, nats.UserCredentials(creds))
			defer c1.Close()
			c2 := natsConnect(t, url, nats.UserCredentials(creds))
			defer c2.Close()
			if _, err := nats.Connect(url, nats.UserCredentials(creds)); err == nil {
				t.Fatal("Third connection was supposed to fail due to limits")
			} else if !strings.Contains(err.Error(), ErrTooManyAccountConnections.Error()) {
				t.Fatal("Third connection was supposed to fail with too many conns")
			}
		}()
		require_NoLocalOrRemoteConnections(acc, srvs...)
	}
	connect := func(url string, credsfile string, acc string, srvs ...*Server) {
		t.Helper()
		nc := natsConnect(t, url, nats.UserCredentials(credsfile))
		nc.Close()
		require_NoLocalOrRemoteConnections(acc, srvs...)
	}
	createAccountAndUser := func(limit bool, done chan struct{}, pubKey, jwt1, jwt2, creds *string) {
		t.Helper()
		kp, _ := nkeys.CreateAccount()
		*pubKey, _ = kp.PublicKey()
		claim := jwt.NewAccountClaims(*pubKey)
		if limit {
			claim.Limits.Conn = 1
		}
		var err error
		*jwt1, err = claim.Encode(oKp)
		require_NoError(t, err)
		// need to assure that create time differs (resolution is sec)
		time.Sleep(time.Millisecond * 1100)
		// create updated claim allowing more connections
		if limit {
			claim.Limits.Conn = 2
		}
		*jwt2, err = claim.Encode(oKp)
		require_NoError(t, err)
		ukp, _ := nkeys.CreateUser()
		seed, _ := ukp.Seed()
		upub, _ := ukp.PublicKey()
		uclaim := newJWTTestUserClaims()
		uclaim.Subject = upub
		ujwt, err := uclaim.Encode(kp)
		require_NoError(t, err)
		*creds = genCredsFile(t, ujwt, seed)
		done <- struct{}{}
	}
	// Create Accounts and corresponding user creds. Do so concurrently to speed up the test
	doneChan := make(chan struct{}, 5)
	defer close(doneChan)
	var syspub, sysjwt, dummy1, sysCreds string
	go createAccountAndUser(false, doneChan, &syspub, &sysjwt, &dummy1, &sysCreds)
	var apub, ajwt1, ajwt2, aCreds string
	go createAccountAndUser(true, doneChan, &apub, &ajwt1, &ajwt2, &aCreds)
	var bpub, bjwt1, bjwt2, bCreds string
	go createAccountAndUser(true, doneChan, &bpub, &bjwt1, &bjwt2, &bCreds)
	var cpub, cjwt1, cjwt2, cCreds string
	go createAccountAndUser(true, doneChan, &cpub, &cjwt1, &cjwt2, &cCreds)
	var dpub, djwt1, dummy2, dCreds string // extra user used later in the test in order to test limits
	go createAccountAndUser(true, doneChan, &dpub, &djwt1, &dummy2, &dCreds)
	for i := 0; i < cap(doneChan); i++ {
		<-doneChan
	}
	defer os.Remove(sysCreds)
	defer os.Remove(aCreds)
	defer os.Remove(bCreds)
	defer os.Remove(cCreds)
	defer os.Remove(dCreds)
	// Create one directory for each server
	dirA := createDir(t, "srv-a")
	defer os.RemoveAll(dirA)
	dirB := createDir(t, "srv-b")
	defer os.RemoveAll(dirB)
	dirC := createDir(t, "srv-c")
	defer os.RemoveAll(dirC)
	// simulate a restart of the server by storing files in them
	// Server A/B will completely sync, so after startup each server
	// will contain the union off all stored/configured jwt
	// Server C will send out lookup requests for jwt it does not store itself
	writeJWT(t, dirA, apub, ajwt1)
	writeJWT(t, dirB, bpub, bjwt1)
	writeJWT(t, dirC, cpub, cjwt1)
	// Create seed server A (using no_advertise to prevent fail over)
	confA := createConfFile(t, []byte(fmt.Sprintf(`
		listen: -1
		server_name: srv-A
		operator: %s
		system_account: %s
		resolver: {
			type: full
			dir: %s
			interval: "200ms"
			limit: 4
		}
		resolver_preload: {
			%s: %s
		}
		cluster {
			name: clust
			listen: -1
			no_advertise: true
		}
    `, ojwt, syspub, dirA, cpub, cjwt1)))
	defer os.Remove(confA)
	sA, _ := RunServerWithConfig(confA)
	defer sA.Shutdown()
	// during startup resolver_preload causes the directory to contain data
	require_JWTPresent(t, dirA, cpub)
	// Create Server B (using no_advertise to prevent fail over)
	confB := createConfFile(t, []byte(fmt.Sprintf(`
		listen: -1
		server_name: srv-B
		operator: %s
		system_account: %s
		resolver: {
			type: full
			dir: %s
			interval: "200ms"
			limit: 4
		}
		cluster {
			name: clust
			listen: -1
			no_advertise: true
			routes [
				nats-route://localhost:%d
			]
		}
    `, ojwt, syspub, dirB, sA.opts.Cluster.Port)))
	defer os.Remove(confB)
	sB, _ := RunServerWithConfig(confB)
	defer sB.Shutdown()
	// Create Server C (using no_advertise to prevent fail over)
	fmtC := `
		listen: -1
		server_name: srv-C
		operator: %s
		system_account: %s
		resolver: {
			type: cache
			dir: %s
			ttl: "%dms"
			limit: 4
		}
		cluster {
			name: clust
			listen: -1
			no_advertise: true
			routes [
				nats-route://localhost:%d
			]
		}
    `
	confClongTTL := createConfFile(t, []byte(fmt.Sprintf(fmtC, ojwt, syspub, dirC, 10000, sA.opts.Cluster.Port)))
	defer os.Remove(confClongTTL)
	confCshortTTL := createConfFile(t, []byte(fmt.Sprintf(fmtC, ojwt, syspub, dirC, 1000, sA.opts.Cluster.Port)))
	defer os.Remove(confCshortTTL)
	sC, _ := RunServerWithConfig(confClongTTL) // use long ttl to assure it is not kicking
	defer sC.Shutdown()
	// startup cluster
	checkClusterFormed(t, sA, sB, sC)
	time.Sleep(500 * time.Millisecond) // wait for the protocol to converge
	// Check all accounts
	require_JWTPresent(t, dirA, apub) // was already present on startup
	require_JWTPresent(t, dirB, apub) // was copied from server A
	require_JWTAbsent(t, dirC, apub)
	require_JWTPresent(t, dirA, bpub) // was copied from server B
	require_JWTPresent(t, dirB, bpub) // was already present on startup
	require_JWTAbsent(t, dirC, bpub)
	require_JWTPresent(t, dirA, cpub) // was present in preload
	require_JWTPresent(t, dirB, cpub) // was copied from server A
	require_JWTPresent(t, dirC, cpub) // was already present on startup
	// This is to test that connecting to it still works
	require_JWTAbsent(t, dirA, syspub)
	require_JWTAbsent(t, dirB, syspub)
	require_JWTAbsent(t, dirC, syspub)
	// system account client can connect to every server
	connect(sA.ClientURL(), sysCreds, "")
	connect(sB.ClientURL(), sysCreds, "")
	connect(sC.ClientURL(), sysCreds, "")
	checkClusterFormed(t, sA, sB, sC)
	// upload system account and require a response from each server
	passCnt := updateJwt(t, sA.ClientURL(), sysCreds, sysjwt, 3)
	require_True(t, passCnt == 3)
	require_JWTPresent(t, dirA, syspub) // was just received
	require_JWTPresent(t, dirB, syspub) // was just received
	require_JWTPresent(t, dirC, syspub) // was just received
	// Only files missing are in C, which is only caching
	connect(sC.ClientURL(), aCreds, apub, sA, sB, sC)
	connect(sC.ClientURL(), bCreds, bpub, sA, sB, sC)
	require_JWTPresent(t, dirC, apub) // was looked up form A or B
	require_JWTPresent(t, dirC, bpub) // was looked up from A or B
	// Check limits and update jwt B connecting to server A
	for port, v := range map[string]struct{ pub, jwt, creds string }{
		sB.ClientURL(): {bpub, bjwt2, bCreds},
		sC.ClientURL(): {cpub, cjwt2, cCreds},
	} {
		require_1Connection(sA.ClientURL(), v.creds, v.pub, sA, sB, sC)
		require_1Connection(sB.ClientURL(), v.creds, v.pub, sA, sB, sC)
		require_1Connection(sC.ClientURL(), v.creds, v.pub, sA, sB, sC)
		passCnt := updateJwt(t, port, sysCreds, v.jwt, 3)
		require_True(t, passCnt == 3)
		require_2Connection(sA.ClientURL(), v.creds, v.pub, sA, sB, sC)
		require_2Connection(sB.ClientURL(), v.creds, v.pub, sA, sB, sC)
		require_2Connection(sC.ClientURL(), v.creds, v.pub, sA, sB, sC)
		require_JWTEqual(t, dirA, v.pub, v.jwt)
		require_JWTEqual(t, dirB, v.pub, v.jwt)
		require_JWTEqual(t, dirC, v.pub, v.jwt)
	}
	// Simulates A having missed an update
	// shutting B down as it has it will directly connect to A and connect right away
	sB.Shutdown()
	writeJWT(t, dirB, apub, ajwt2) // this will be copied to server A
	sB, _ = RunServerWithConfig(confB)
	defer sB.Shutdown()
	checkClusterFormed(t, sA, sB, sC)
	time.Sleep(500 * time.Millisecond) // wait for the protocol to converge
	// Restart server C. this is a workaround to force C to do a lookup in the absence of account cleanup
	sC.Shutdown()
	sC, _ = RunServerWithConfig(confClongTTL) //TODO remove this once we clean up accounts
	defer sC.Shutdown()
	require_JWTEqual(t, dirA, apub, ajwt2) // was copied from server B
	require_JWTEqual(t, dirB, apub, ajwt2) // was restarted with this
	require_JWTEqual(t, dirC, apub, ajwt1) // still contains old cached value
	require_2Connection(sA.ClientURL(), aCreds, apub, sA, sB, sC)
	require_2Connection(sB.ClientURL(), aCreds, apub, sA, sB, sC)
	require_1Connection(sC.ClientURL(), aCreds, apub, sA, sB, sC)
	// Restart server C. this is a workaround to force C to do a lookup in the absence of account cleanup
	sC.Shutdown()
	sC, _ = RunServerWithConfig(confCshortTTL) //TODO remove this once we clean up accounts
	defer sC.Shutdown()
	require_JWTEqual(t, dirC, apub, ajwt1) // still contains old cached value
	checkClusterFormed(t, sA, sB, sC)
	// Force next connect to do a lookup exceeds ttl
	fname := filepath.Join(dirC, apub+".jwt")
	checkFor(t, 2*time.Second, 100*time.Millisecond, func() error {
		_, err := os.Stat(fname)
		if os.IsNotExist(err) {
			return nil
		}
		return fmt.Errorf("File not removed in time")
	})
	connect(sC.ClientURL(), aCreds, apub, sA, sB, sC) // When lookup happens
	require_JWTEqual(t, dirC, apub, ajwt2)            // was looked up form A or B
	require_2Connection(sC.ClientURL(), aCreds, apub, sA, sB, sC)
	// Test exceeding limit. For the exclusive directory resolver, limit is a stop gap measure.
	// It is not expected to be hit. When hit the administrator is supposed to take action.
	passCnt = updateJwt(t, sA.ClientURL(), sysCreds, djwt1, 3)
	require_True(t, passCnt == 1) // Only Server C updated
	for _, srv := range []*Server{sA, sB, sC} {
		if a, ok := srv.accounts.Load(syspub); ok {
			acc := a.(*Account)
			checkFor(t, time.Second, 20*time.Millisecond, func() error {
				acc.mu.Lock()
				defer acc.mu.Unlock()
				if acc.ctmr != nil {
					return fmt.Errorf("Timer still exists")
				}
				return nil
			})
		}
	}
}

func TestAccountNATSResolverCrossClusterFetch(t *testing.T) {
	connect := func(url string, credsfile string) {
		t.Helper()
		nc := natsConnect(t, url, nats.UserCredentials(credsfile))
		nc.Close()
	}
	createAccountAndUser := func(done chan struct{}, pubKey, jwt1, jwt2, creds *string) {
		t.Helper()
		kp, _ := nkeys.CreateAccount()
		*pubKey, _ = kp.PublicKey()
		claim := jwt.NewAccountClaims(*pubKey)
		var err error
		*jwt1, err = claim.Encode(oKp)
		require_NoError(t, err)
		// need to assure that create time differs (resolution is sec)
		time.Sleep(time.Millisecond * 1100)
		// create updated claim
		claim.Tags.Add("tag")
		*jwt2, err = claim.Encode(oKp)
		require_NoError(t, err)
		ukp, _ := nkeys.CreateUser()
		seed, _ := ukp.Seed()
		upub, _ := ukp.PublicKey()
		uclaim := newJWTTestUserClaims()
		uclaim.Subject = upub
		ujwt, err := uclaim.Encode(kp)
		require_NoError(t, err)
		*creds = genCredsFile(t, ujwt, seed)
		done <- struct{}{}
	}
	// Create Accounts and corresponding user creds. Do so concurrently to speed up the test
	doneChan := make(chan struct{}, 3)
	defer close(doneChan)
	var syspub, sysjwt, dummy1, sysCreds string
	go createAccountAndUser(doneChan, &syspub, &sysjwt, &dummy1, &sysCreds)
	var apub, ajwt1, ajwt2, aCreds string
	go createAccountAndUser(doneChan, &apub, &ajwt1, &ajwt2, &aCreds)
	var bpub, bjwt1, bjwt2, bCreds string
	go createAccountAndUser(doneChan, &bpub, &bjwt1, &bjwt2, &bCreds)
	for i := 0; i < cap(doneChan); i++ {
		<-doneChan
	}
	defer os.Remove(sysCreds)
	defer os.Remove(aCreds)
	defer os.Remove(bCreds)
	// Create one directory for each server
	dirAA := createDir(t, "srv-a-a")
	defer os.RemoveAll(dirAA)
	dirAB := createDir(t, "srv-a-b")
	defer os.RemoveAll(dirAB)
	dirBA := createDir(t, "srv-b-a")
	defer os.RemoveAll(dirBA)
	dirBB := createDir(t, "srv-b-b")
	defer os.RemoveAll(dirBB)
	// simulate a restart of the server by storing files in them
	// Server AA & AB will completely sync
	// Server BA & BB will completely sync
	// Be aware that no syncing will occur between cluster
	writeJWT(t, dirAA, apub, ajwt1)
	writeJWT(t, dirBA, bpub, bjwt1)
	// Create seed server A (using no_advertise to prevent fail over)
	confAA := createConfFile(t, []byte(fmt.Sprintf(`
		listen: -1
		server_name: srv-A-A
		operator: %s
		system_account: %s
		resolver: {
			type: full
			dir: %s
			interval: "200ms"
		}
		gateway: {
			name: "clust-A"
			listen: -1
		}
		cluster {
			name: clust-A
			listen: -1
			no_advertise: true
		}
    `, ojwt, syspub, dirAA)))
	defer os.Remove(confAA)
	sAA, _ := RunServerWithConfig(confAA)
	defer sAA.Shutdown()
	// Create Server B (using no_advertise to prevent fail over)
	confAB := createConfFile(t, []byte(fmt.Sprintf(`
		listen: -1
		server_name: srv-A-B
		operator: %s
		system_account: %s
		resolver: {
			type: full
			dir: %s
			interval: "200ms"
		}
		gateway: {
			name: "clust-A"
			listen: -1
		}
		cluster {
			name: clust-A
			listen: -1
			no_advertise: true
			routes [
				nats-route://localhost:%d
			]
		}
    `, ojwt, syspub, dirAB, sAA.opts.Cluster.Port)))
	defer os.Remove(confAB)
	sAB, _ := RunServerWithConfig(confAB)
	defer sAB.Shutdown()
	// Create Server C (using no_advertise to prevent fail over)
	confBA := createConfFile(t, []byte(fmt.Sprintf(`
		listen: -1
		server_name: srv-B-A
		operator: %s
		system_account: %s
		resolver: {
			type: full
			dir: %s
			interval: "200ms"
		}
		gateway: {
			name: "clust-B"
			listen: -1
			gateways: [
				{name: "clust-A", url: "nats://localhost:%d"},
			]
		}
		cluster {
			name: clust-B
			listen: -1
			no_advertise: true
		}
    `, ojwt, syspub, dirBA, sAA.opts.Gateway.Port)))
	defer os.Remove(confBA)
	sBA, _ := RunServerWithConfig(confBA)
	defer sBA.Shutdown()
	// Create Sever BA  (using no_advertise to prevent fail over)
	confBB := createConfFile(t, []byte(fmt.Sprintf(`
		listen: -1
		server_name: srv-B-B
		operator: %s
		system_account: %s
		resolver: {
			type: full
			dir: %s
			interval: "200ms"
		}
		cluster {
			name: clust-B
			listen: -1
			no_advertise: true
			routes [
				nats-route://localhost:%d
			]
		}
		gateway: {
			name: "clust-B"
			listen: -1
			gateways: [
				{name: "clust-A", url: "nats://localhost:%d"},
			]
		}
    `, ojwt, syspub, dirBB, sBA.opts.Cluster.Port, sAA.opts.Cluster.Port)))
	defer os.Remove(confBB)
	sBB, _ := RunServerWithConfig(confBB)
	defer sBB.Shutdown()
	// Assert topology
	checkClusterFormed(t, sAA, sAB)
	checkClusterFormed(t, sBA, sBB)
	waitForOutboundGateways(t, sAA, 1, 5*time.Second)
	waitForOutboundGateways(t, sAB, 1, 5*time.Second)
	waitForOutboundGateways(t, sBA, 1, 5*time.Second)
	waitForOutboundGateways(t, sBB, 1, 5*time.Second)
	time.Sleep(500 * time.Millisecond)                 // wait for the protocol to converge
	updateJwt(t, sAA.ClientURL(), sysCreds, sysjwt, 4) // update system account jwt on all server
	require_JWTEqual(t, dirAA, syspub, sysjwt)         // assure this update made it to every server
	require_JWTEqual(t, dirAB, syspub, sysjwt)         // assure this update made it to every server
	require_JWTEqual(t, dirBA, syspub, sysjwt)         // assure this update made it to every server
	require_JWTEqual(t, dirBB, syspub, sysjwt)         // assure this update made it to every server
	require_JWTAbsent(t, dirAA, bpub)                  // assure that jwt are not synced across cluster
	require_JWTAbsent(t, dirAB, bpub)                  // assure that jwt are not synced across cluster
	require_JWTAbsent(t, dirBA, apub)                  // assure that jwt are not synced across cluster
	require_JWTAbsent(t, dirBB, apub)                  // assure that jwt are not synced across cluster
	connect(sAA.ClientURL(), aCreds)                   // connect to cluster where jwt was initially stored
	connect(sAB.ClientURL(), aCreds)                   // connect to cluster where jwt was initially stored
	connect(sBA.ClientURL(), bCreds)                   // connect to cluster where jwt was initially stored
	connect(sBB.ClientURL(), bCreds)                   // connect to cluster where jwt was initially stored
	time.Sleep(500 * time.Millisecond)                 // wait for the protocol to (NOT) converge
	require_JWTAbsent(t, dirAA, bpub)                  // assure that jwt are still not synced across cluster
	require_JWTAbsent(t, dirAB, bpub)                  // assure that jwt are still not synced across cluster
	require_JWTAbsent(t, dirBA, apub)                  // assure that jwt are still not synced across cluster
	require_JWTAbsent(t, dirBB, apub)                  // assure that jwt are still not synced across cluster
	// We have verified that account B does not exist in cluster A, neither does account A in cluster B
	// Despite that clients from account B can connect to server A, same for account A in cluster B
	connect(sAA.ClientURL(), bCreds)                  // connect to cluster where jwt was not initially stored
	connect(sAB.ClientURL(), bCreds)                  // connect to cluster where jwt was not initially stored
	connect(sBA.ClientURL(), aCreds)                  // connect to cluster where jwt was not initially stored
	connect(sBB.ClientURL(), aCreds)                  // connect to cluster where jwt was not initially stored
	require_JWTEqual(t, dirAA, bpub, bjwt1)           // assure that now jwt used in connect is stored
	require_JWTEqual(t, dirAB, bpub, bjwt1)           // assure that now jwt used in connect is stored
	require_JWTEqual(t, dirBA, apub, ajwt1)           // assure that now jwt used in connect is stored
	require_JWTEqual(t, dirBB, apub, ajwt1)           // assure that now jwt used in connect is stored
	updateJwt(t, sAA.ClientURL(), sysCreds, bjwt2, 4) // update bjwt, expect updates from everywhere
	updateJwt(t, sBA.ClientURL(), sysCreds, ajwt2, 4) // update ajwt, expect updates from everywhere
	require_JWTEqual(t, dirAA, bpub, bjwt2)           // assure that jwt got updated accordingly
	require_JWTEqual(t, dirAB, bpub, bjwt2)           // assure that jwt got updated accordingly
	require_JWTEqual(t, dirBA, apub, ajwt2)           // assure that jwt got updated accordingly
	require_JWTEqual(t, dirBB, apub, ajwt2)           // assure that jwt got updated accordingly
}

func newTimeRange(start time.Time, dur time.Duration) jwt.TimeRange {
	return jwt.TimeRange{Start: start.Format("15:04:05"), End: start.Add(dur).Format("15:04:05")}
}

func createUserWithLimit(t *testing.T, accKp nkeys.KeyPair, expiration time.Time, limits func(*jwt.Limits)) string {
	t.Helper()
	ukp, _ := nkeys.CreateUser()
	seed, _ := ukp.Seed()
	upub, _ := ukp.PublicKey()
	uclaim := newJWTTestUserClaims()
	uclaim.Subject = upub
	if limits != nil {
		limits(&uclaim.Limits)
	}
	if !expiration.IsZero() {
		uclaim.Expires = expiration.Unix()
	}
	vr := jwt.ValidationResults{}
	uclaim.Validate(&vr)
	require_Len(t, len(vr.Errors()), 0)
	ujwt, err := uclaim.Encode(accKp)
	require_NoError(t, err)
	return genCredsFile(t, ujwt, seed)
}

func TestJWTUserLimits(t *testing.T) {
	// helper for time
	inAnHour := time.Now().Add(time.Hour)
	inTwoHours := time.Now().Add(2 * time.Hour)
	doNotExpire := time.Now().AddDate(1, 0, 0)
	// create account
	kp, _ := nkeys.CreateAccount()
	aPub, _ := kp.PublicKey()
	claim := jwt.NewAccountClaims(aPub)
	aJwt, err := claim.Encode(oKp)
	require_NoError(t, err)
	conf := createConfFile(t, []byte(fmt.Sprintf(`
		listen: -1
		operator: %s
		resolver: MEM
		resolver_preload: {
			%s: %s
		}
    `, ojwt, aPub, aJwt)))
	defer os.Remove(conf)
	sA, _ := RunServerWithConfig(conf)
	defer sA.Shutdown()
	for _, v := range []struct {
		pass bool
		f    func(*jwt.Limits)
	}{
		{true, nil},
		{false, func(j *jwt.Limits) { j.Src.Set("8.8.8.8/8") }},
		{true, func(j *jwt.Limits) { j.Src.Set("8.8.8.8/0") }},
		{true, func(j *jwt.Limits) { j.Src.Set("127.0.0.1/8") }},
		{true, func(j *jwt.Limits) { j.Src.Set("8.8.8.8/8,127.0.0.1/8") }},
		{false, func(j *jwt.Limits) { j.Src.Set("8.8.8.8/8,9.9.9.9/8") }},
		{true, func(j *jwt.Limits) { j.Times = append(j.Times, newTimeRange(time.Now(), time.Hour)) }},
		{false, func(j *jwt.Limits) { j.Times = append(j.Times, newTimeRange(time.Now().Add(time.Hour), time.Hour)) }},
		{true, func(j *jwt.Limits) {
			j.Times = append(j.Times, newTimeRange(inAnHour, time.Hour), newTimeRange(time.Now(), time.Hour))
		}}, // last one is within range
		{false, func(j *jwt.Limits) {
			j.Times = append(j.Times, newTimeRange(inAnHour, time.Hour), newTimeRange(inTwoHours, time.Hour))
		}}, // out of range
		{false, func(j *jwt.Limits) {
			j.Times = append(j.Times, newTimeRange(inAnHour, 3*time.Hour), newTimeRange(inTwoHours, 2*time.Hour))
		}}, // overlapping [a[]b] out of range*/
		{false, func(j *jwt.Limits) {
			j.Times = append(j.Times, newTimeRange(inAnHour, 3*time.Hour), newTimeRange(inTwoHours, time.Hour))
		}}, // overlapping [a[b]] out of range
		// next day tests where end < begin
		{true, func(j *jwt.Limits) { j.Times = append(j.Times, newTimeRange(time.Now(), 25*time.Hour)) }},
		{true, func(j *jwt.Limits) { j.Times = append(j.Times, newTimeRange(time.Now(), -time.Hour)) }},
	} {
		t.Run("", func(t *testing.T) {
			creds := createUserWithLimit(t, kp, doNotExpire, v.f)
			defer os.Remove(creds)
			if c, err := nats.Connect(sA.ClientURL(), nats.UserCredentials(creds)); err == nil {
				c.Close()
				if !v.pass {
					t.Fatalf("Expected failure got none")
				}
			} else if v.pass {
				t.Fatalf("Expected success got %v", err)
			} else if !strings.Contains(err.Error(), "Authorization Violation") {
				t.Fatalf("Expected error other than %v", err)
			}
		})
	}
}

func TestJWTTimeExpiration(t *testing.T) {
	validFor := 1500 * time.Millisecond
	validRange := 500 * time.Millisecond
	doNotExpire := time.Now().AddDate(1, 0, 0)
	// create account
	kp, _ := nkeys.CreateAccount()
	aPub, _ := kp.PublicKey()
	claim := jwt.NewAccountClaims(aPub)
	aJwt, err := claim.Encode(oKp)
	require_NoError(t, err)
	conf := createConfFile(t, []byte(fmt.Sprintf(`
		listen: -1
		operator: %s
		resolver: MEM
		resolver_preload: {
			%s: %s
		}
    `, ojwt, aPub, aJwt)))
	defer os.Remove(conf)
	sA, _ := RunServerWithConfig(conf)
	defer sA.Shutdown()
	for _, l := range []string{"", "Europe/Berlin", "America/New_York"} {
		t.Run("simple expiration "+l, func(t *testing.T) {
			start := time.Now()
			creds := createUserWithLimit(t, kp, doNotExpire, func(j *jwt.Limits) {
				if l == "" {
					j.Times = []jwt.TimeRange{newTimeRange(start, validFor)}
				} else {
					loc, err := time.LoadLocation(l)
					require_NoError(t, err)
					j.Times = []jwt.TimeRange{newTimeRange(start.In(loc), validFor)}
					j.Locale = l
				}
			})
			defer os.Remove(creds)
			disconnectChan := make(chan struct{})
			defer close(disconnectChan)
			errChan := make(chan struct{})
			defer close(errChan)
			c := natsConnect(t, sA.ClientURL(),
				nats.UserCredentials(creds),
				nats.DisconnectErrHandler(func(conn *nats.Conn, err error) {
					if err != io.EOF {
						return
					}
					disconnectChan <- struct{}{}
				}),
				nats.ErrorHandler(func(conn *nats.Conn, s *nats.Subscription, err error) {
					if err != nats.ErrAuthExpired {
						return
					}
					now := time.Now()
					stop := start.Add(validFor)
					// assure event happens within a second of stop
					if stop.Add(-validRange).Before(stop) && now.Before(stop.Add(validRange)) {
						errChan <- struct{}{}
					}
				}))
			chanRecv(t, errChan, 10*time.Second)
			chanRecv(t, disconnectChan, 10*time.Second)
			require_True(t, c.IsReconnecting())
			require_False(t, c.IsConnected())
			c.Close()
		})
	}
	t.Run("double expiration", func(t *testing.T) {
		start1 := time.Now()
		start2 := start1.Add(2 * validFor)
		creds := createUserWithLimit(t, kp, doNotExpire, func(j *jwt.Limits) {
			j.Times = []jwt.TimeRange{newTimeRange(start1, validFor), newTimeRange(start2, validFor)}
		})
		defer os.Remove(creds)
		errChan := make(chan struct{})
		defer close(errChan)
		reConnectChan := make(chan struct{})
		defer close(reConnectChan)
		c := natsConnect(t, sA.ClientURL(),
			nats.UserCredentials(creds),
			nats.ReconnectHandler(func(conn *nats.Conn) {
				reConnectChan <- struct{}{}
			}),
			nats.ErrorHandler(func(conn *nats.Conn, s *nats.Subscription, err error) {
				if err != nats.ErrAuthExpired {
					return
				}
				now := time.Now()
				stop := start1.Add(validFor)
				// assure event happens within a second of stop
				if stop.Add(-validRange).Before(stop) && now.Before(stop.Add(validRange)) {
					errChan <- struct{}{}
					return
				}
				stop = start2.Add(validFor)
				// assure event happens within a second of stop
				if stop.Add(-validRange).Before(stop) && now.Before(stop.Add(validRange)) {
					errChan <- struct{}{}
				}
			}))
		chanRecv(t, errChan, 10*time.Second)
		chanRecv(t, reConnectChan, 10*time.Second)
		require_False(t, c.IsReconnecting())
		require_True(t, c.IsConnected())
		chanRecv(t, errChan, 10*time.Second)
		c.Close()
	})
	t.Run("lower jwt expiration overwrites time", func(t *testing.T) {
		start := time.Now()
		creds := createUserWithLimit(t, kp, start.Add(validFor), func(j *jwt.Limits) { j.Times = []jwt.TimeRange{newTimeRange(start, 2*validFor)} })
		defer os.Remove(creds)
		disconnectChan := make(chan struct{})
		defer close(disconnectChan)
		errChan := make(chan struct{})
		defer close(errChan)
		c := natsConnect(t, sA.ClientURL(),
			nats.UserCredentials(creds),
			nats.DisconnectErrHandler(func(conn *nats.Conn, err error) {
				if err != io.EOF {
					return
				}
				disconnectChan <- struct{}{}
			}),
			nats.ErrorHandler(func(conn *nats.Conn, s *nats.Subscription, err error) {
				if err != nats.ErrAuthExpired {
					return
				}
				now := time.Now()
				stop := start.Add(validFor)
				// assure event happens within a second of stop
				if stop.Add(-validRange).Before(stop) && now.Before(stop.Add(validRange)) {
					errChan <- struct{}{}
				}
			}))
		chanRecv(t, errChan, 10*time.Second)
		chanRecv(t, disconnectChan, 10*time.Second)
		require_True(t, c.IsReconnecting())
		require_False(t, c.IsConnected())
		c.Close()
	})
}

func TestJWTLimits(t *testing.T) {
	doNotExpire := time.Now().AddDate(1, 0, 0)
	// create account
	kp, _ := nkeys.CreateAccount()
	aPub, _ := kp.PublicKey()
	claim := jwt.NewAccountClaims(aPub)
	aJwt, err := claim.Encode(oKp)
	require_NoError(t, err)
	conf := createConfFile(t, []byte(fmt.Sprintf(`
		listen: -1
		operator: %s
		resolver: MEM
		resolver_preload: {
			%s: %s
		}
    `, ojwt, aPub, aJwt)))
	defer os.Remove(conf)
	sA, _ := RunServerWithConfig(conf)
	defer sA.Shutdown()
	errChan := make(chan struct{})
	defer close(errChan)
	t.Run("subs", func(t *testing.T) {
		creds := createUserWithLimit(t, kp, doNotExpire, func(j *jwt.Limits) { j.Subs = 1 })
		defer os.Remove(creds)
		c := natsConnect(t, sA.ClientURL(), nats.UserCredentials(creds),
			nats.DisconnectErrHandler(func(conn *nats.Conn, err error) {
				if e := conn.LastError(); e != nil && strings.Contains(e.Error(), "maximum subscriptions exceeded") {
					errChan <- struct{}{}
				}
			}),
		)
		defer c.Close()
		if _, err := c.Subscribe("foo", func(msg *nats.Msg) {}); err != nil {
			t.Fatalf("couldn't subscribe: %v", err)
		}
		if _, err = c.Subscribe("bar", func(msg *nats.Msg) {}); err != nil {
			t.Fatalf("expected error got: %v", err)
		}
		chanRecv(t, errChan, time.Second)
	})
	t.Run("payload", func(t *testing.T) {
		creds := createUserWithLimit(t, kp, doNotExpire, func(j *jwt.Limits) { j.Payload = 5 })
		defer os.Remove(creds)
		c := natsConnect(t, sA.ClientURL(), nats.UserCredentials(creds))
		defer c.Close()
		if err := c.Flush(); err != nil {
			t.Fatalf("flush failed %v", err)
		}
		if err := c.Publish("foo", []byte("world")); err != nil {
			t.Fatalf("couldn't publish: %v", err)
		}
		if err := c.Publish("foo", []byte("worldX")); err != nats.ErrMaxPayload {
			t.Fatalf("couldn't publish: %v", err)
		}
	})
}

func TestJWTNoOperatorMode(t *testing.T) {
	for _, login := range []bool{true, false} {
		t.Run("", func(t *testing.T) {
			opts := DefaultOptions()
			if login {
				opts.Users = append(opts.Users, &User{Username: "u", Password: "pwd"})
			}
			sA := RunServer(opts)
			defer sA.Shutdown()
			kp, _ := nkeys.CreateAccount()
			creds := createUserWithLimit(t, kp, time.Now().Add(time.Hour), nil)
			defer os.Remove(creds)
			url := sA.ClientURL()
			if login {
				url = fmt.Sprintf("nats://u:pwd@%s:%d", sA.opts.Host, sA.opts.Port)
			}
			c := natsConnect(t, url, nats.UserCredentials(creds))
			defer c.Close()
			sA.mu.Lock()
			defer sA.mu.Unlock()
			if len(sA.clients) != 1 {
				t.Fatalf("Expected exactly one client")
			}
			for _, v := range sA.clients {
				if v.opts.JWT != "" {
					t.Fatalf("Expected no jwt %v", v.opts.JWT)
				}
			}
		})
	}
}

func TestJWTJetStreamLimits(t *testing.T) {
	updateJwt := func(url string, creds string, pubKey string, jwt string) {
		t.Helper()
		c := natsConnect(t, url, nats.UserCredentials(creds))
		defer c.Close()
		if msg, err := c.Request(fmt.Sprintf(accUpdateEventSubjNew, pubKey), []byte(jwt), time.Second); err != nil {
			t.Fatal("error not expected in this test", err)
		} else {
			content := make(map[string]interface{})
			if err := json.Unmarshal(msg.Data, &content); err != nil {
				t.Fatalf("%v", err)
			} else if _, ok := content["data"]; !ok {
				t.Fatalf("did not get an ok response got: %v", content)
			}
		}
	}
	require_IdenticalLimits := func(infoLim JetStreamAccountLimits, lim jwt.JetStreamLimits) {
		t.Helper()
		if int64(infoLim.MaxConsumers) != lim.Consumer || int64(infoLim.MaxStreams) != lim.Streams ||
			infoLim.MaxMemory != lim.MemoryStorage || infoLim.MaxStore != lim.DiskStorage {
			t.Fatalf("limits do not match %v != %v", infoLim, lim)
		}
	}
	expect_JSDisabledForAccount := func(c *nats.Conn) {
		t.Helper()
		if _, err := c.Request("$JS.API.INFO", nil, time.Second); err != nats.ErrTimeout && err != nats.ErrNoResponders {
			t.Fatalf("Unexpected error: %v", err)
		}
	}
	expect_InfoError := func(c *nats.Conn) {
		t.Helper()
		var info JSApiAccountInfoResponse
		if resp, err := c.Request("$JS.API.INFO", nil, time.Second); err != nil {
			t.Fatalf("Unexpected error: %v", err)
		} else if err = json.Unmarshal(resp.Data, &info); err != nil {
			t.Fatalf("response1 %v got error %v", string(resp.Data), err)
		} else if info.Error == nil {
			t.Fatalf("expected error")
		}
	}
	validate_limits := func(c *nats.Conn, expectedLimits jwt.JetStreamLimits) {
		t.Helper()
		var info JSApiAccountInfoResponse
		if resp, err := c.Request("$JS.API.INFO", nil, time.Second); err != nil {
			t.Fatalf("Unexpected error: %v", err)
		} else if err = json.Unmarshal(resp.Data, &info); err != nil {
			t.Fatalf("response1 %v got error %v", string(resp.Data), err)
		} else {
			require_IdenticalLimits(info.Limits, expectedLimits)
		}
	}
	// create system account
	sysKp, _ := nkeys.CreateAccount()
	sysPub, _ := sysKp.PublicKey()
	claim := jwt.NewAccountClaims(sysPub)
	sysJwt, err := claim.Encode(oKp)
	require_NoError(t, err)
	sysUKp, _ := nkeys.CreateUser()
	sysUSeed, _ := sysUKp.Seed()
	uclaim := newJWTTestUserClaims()
	uclaim.Subject, _ = sysUKp.PublicKey()
	sysUserJwt, err := uclaim.Encode(sysKp)
	require_NoError(t, err)
	sysKp.Seed()
	sysCreds := genCredsFile(t, sysUserJwt, sysUSeed)
	// limits to apply and check
	limits1 := jwt.JetStreamLimits{MemoryStorage: 1024 * 1024, DiskStorage: 2048 * 1024, Streams: 1, Consumer: 2}
	// has valid limits that would fail when incorrectly applied twice
	limits2 := jwt.JetStreamLimits{MemoryStorage: 4096 * 1024, DiskStorage: 8192 * 1024, Streams: 3, Consumer: 4}
	// limits exceeding actual configured value of DiskStorage
	limitsExceeded := jwt.JetStreamLimits{MemoryStorage: 8192 * 1024, DiskStorage: 16384 * 1024, Streams: 5, Consumer: 6}
	// create account using jetstream with both limits
	akp, _ := nkeys.CreateAccount()
	aPub, _ := akp.PublicKey()
	claim = jwt.NewAccountClaims(aPub)
	claim.Limits.JetStreamLimits = limits1
	aJwt1, err := claim.Encode(oKp)
	require_NoError(t, err)
	claim.Limits.JetStreamLimits = limits2
	aJwt2, err := claim.Encode(oKp)
	require_NoError(t, err)
	claim.Limits.JetStreamLimits = limitsExceeded
	aJwtLimitsExceeded, err := claim.Encode(oKp)
	require_NoError(t, err)
	claim.Limits.JetStreamLimits = jwt.JetStreamLimits{} // disabled
	aJwt4, err := claim.Encode(oKp)
	require_NoError(t, err)
	// account user
	uKp, _ := nkeys.CreateUser()
	uSeed, _ := uKp.Seed()
	uclaim = newJWTTestUserClaims()
	uclaim.Subject, _ = uKp.PublicKey()
	userJwt, err := uclaim.Encode(akp)
	require_NoError(t, err)
	userCreds := genCredsFile(t, userJwt, uSeed)
	dir, err := ioutil.TempDir("", "srv")
	require_NoError(t, err)
	defer os.RemoveAll(dir)
	conf := createConfFile(t, []byte(fmt.Sprintf(`
		listen: -1
		jetstream: {max_mem_store: 10Mb, max_file_store: 10Mb}
		operator: %s
		resolver: {
			type: full
			dir: %s
		}
		system_account: %s
    `, ojwt, dir, sysPub)))
	defer os.Remove(conf)
	s, opts := RunServerWithConfig(conf)
	defer s.Shutdown()
	port := opts.Port
	updateJwt(s.ClientURL(), sysCreds, sysPub, sysJwt)
	sys := natsConnect(t, s.ClientURL(), nats.UserCredentials(sysCreds))
	expect_InfoError(sys)
	sys.Close()
	updateJwt(s.ClientURL(), sysCreds, aPub, aJwt1)
	c := natsConnect(t, s.ClientURL(), nats.UserCredentials(userCreds), nats.ReconnectWait(200*time.Millisecond))
	defer c.Close()
	validate_limits(c, limits1)
	// keep using the same connection
	updateJwt(s.ClientURL(), sysCreds, aPub, aJwt2)
	validate_limits(c, limits2)
	// keep using the same connection but do NOT CHANGE anything.
	// This tests if the jwt is applied a second time (would fail)
	updateJwt(s.ClientURL(), sysCreds, aPub, aJwt2)
	validate_limits(c, limits2)
	// keep using the same connection. This update EXCEEDS LIMITS
	updateJwt(s.ClientURL(), sysCreds, aPub, aJwtLimitsExceeded)
	validate_limits(c, limits2)
	// disable test after failure
	updateJwt(s.ClientURL(), sysCreds, aPub, aJwt4)
	expect_InfoError(c)
	// re enable, again testing with a value that can't be applied twice
	updateJwt(s.ClientURL(), sysCreds, aPub, aJwt2)
	validate_limits(c, limits2)
	// disable test no prior failure
	updateJwt(s.ClientURL(), sysCreds, aPub, aJwt4)
	expect_InfoError(c)
	// Wrong limits form start
	updateJwt(s.ClientURL(), sysCreds, aPub, aJwtLimitsExceeded)
	expect_JSDisabledForAccount(c)
	// enable js but exceed limits. Followed by fix via restart
	updateJwt(s.ClientURL(), sysCreds, aPub, aJwt2)
	validate_limits(c, limits2)
	updateJwt(s.ClientURL(), sysCreds, aPub, aJwtLimitsExceeded)
	validate_limits(c, limits2)
	s.Shutdown()
	conf = createConfFile(t, []byte(fmt.Sprintf(`
		listen: %d
		jetstream: {max_mem_store: 20Mb, max_file_store: 20Mb}
		operator: %s
		resolver: {
			type: full
			dir: %s
		}
		system_account: %s
    `, port, ojwt, dir, sysPub)))
	defer os.Remove(conf)
	s, _ = RunServerWithConfig(conf)
	defer s.Shutdown()
	c.Flush() // force client to discover the disconnect
	checkClientsCount(t, s, 1)
	validate_limits(c, limitsExceeded)
	s.Shutdown()
	// disable jetstream test
	conf = createConfFile(t, []byte(fmt.Sprintf(`
		listen: %d
		operator: %s
		resolver: {
			type: full
			dir: %s
		}
		system_account: %s
    `, port, ojwt, dir, sysPub)))
	defer os.Remove(conf)
	s, _ = RunServerWithConfig(conf)
	defer s.Shutdown()
	c.Flush() // force client to discover the disconnect
	checkClientsCount(t, s, 1)
	expect_JSDisabledForAccount(c)
	// test that it stays disabled
	updateJwt(s.ClientURL(), sysCreds, aPub, aJwt2)
	expect_JSDisabledForAccount(c)
	c.Close()
}

func TestJWTUserRevocation(t *testing.T) {
	createAccountAndUser := func(done chan struct{}, pubKey, jwt1, jwt2, creds1, creds2 *string) {
		t.Helper()
		kp, _ := nkeys.CreateAccount()
		*pubKey, _ = kp.PublicKey()
		claim := jwt.NewAccountClaims(*pubKey)
		var err error
		*jwt1, err = claim.Encode(oKp)
		require_NoError(t, err)

		ukp, _ := nkeys.CreateUser()
		seed, _ := ukp.Seed()
		upub, _ := ukp.PublicKey()
		uclaim := newJWTTestUserClaims()
		uclaim.Subject = upub

		ujwt1, err := uclaim.Encode(kp)
		require_NoError(t, err)
		*creds1 = genCredsFile(t, ujwt1, seed)

		// create updated claim need to assure that issue time differs
		claim.Revoke(upub) // revokes all jwt from now on
		time.Sleep(time.Millisecond * 1100)
		*jwt2, err = claim.Encode(oKp)
		require_NoError(t, err)

		ujwt2, err := uclaim.Encode(kp)
		require_NoError(t, err)
		*creds2 = genCredsFile(t, ujwt2, seed)

		done <- struct{}{}
	}
	// Create Accounts and corresponding revoked and non revoked user creds. Do so concurrently to speed up the test
	doneChan := make(chan struct{}, 2)
	defer close(doneChan)
	var syspub, sysjwt, dummy1, sysCreds, dummyCreds string
	go createAccountAndUser(doneChan, &syspub, &sysjwt, &dummy1, &sysCreds, &dummyCreds)
	var apub, ajwt1, ajwt2, aCreds1, aCreds2 string
	go createAccountAndUser(doneChan, &apub, &ajwt1, &ajwt2, &aCreds1, &aCreds2)
	for i := 0; i < cap(doneChan); i++ {
		<-doneChan
	}
	defer os.Remove(sysCreds)
	defer os.Remove(dummyCreds)
	defer os.Remove(aCreds1)
	defer os.Remove(aCreds2)
	dirSrv := createDir(t, "srv")
	defer os.RemoveAll(dirSrv)
	conf := createConfFile(t, []byte(fmt.Sprintf(`
		listen: -1
		operator: %s
		system_account: %s
		resolver: {
			type: full
			dir: %s
		}
    `, ojwt, syspub, dirSrv)))
	defer os.Remove(conf)
	srv, _ := RunServerWithConfig(conf)
	defer srv.Shutdown()
	updateJwt(t, srv.ClientURL(), sysCreds, sysjwt, 1) // update system account jwt
	updateJwt(t, srv.ClientURL(), sysCreds, ajwt1, 1)  // set account jwt without revocation
	ncSys := natsConnect(t, srv.ClientURL(), nats.UserCredentials(sysCreds), nats.Name("conn name"))
	defer ncSys.Close()
	ncChan := make(chan *nats.Msg, 10)
	defer close(ncChan)
	ncSys.ChanSubscribe(fmt.Sprintf(disconnectEventSubj, apub), ncChan) // observe disconnect message
	// use credentials that will be revoked ans assure that the connection will be disconnected
	nc := natsConnect(t, srv.ClientURL(), nats.UserCredentials(aCreds1),
		nats.DisconnectErrHandler(func(conn *nats.Conn, err error) {
			if lErr := conn.LastError(); lErr != nil && strings.Contains(lErr.Error(), "Authentication Revoked") {
				doneChan <- struct{}{}
			}
		}))
	defer nc.Close()
	// update account jwt to contain revocation
	if updateJwt(t, srv.ClientURL(), sysCreds, ajwt2, 1) != 1 {
		t.Fatalf("Expected jwt update to pass")
	}
	// assure that nc got disconnected due to the revocation
	select {
	case <-doneChan:
	case <-time.After(time.Second):
		t.Fatalf("Expected connection to have failed")
	}
	m := <-ncChan
	require_Len(t, strings.Count(string(m.Data), apub), 2)
	require_True(t, strings.Contains(string(m.Data), `"jwt": "eyJ0`))
	// try again with old credentials. Expected to fail
	if nc1, err := nats.Connect(srv.ClientURL(), nats.UserCredentials(aCreds1)); err == nil {
		nc1.Close()
		t.Fatalf("Expected revoked credentials to fail")
	}
	// Assure new creds pass
	nc2 := natsConnect(t, srv.ClientURL(), nats.UserCredentials(aCreds2))
	defer nc2.Close()
}

func TestJWTAccountOps(t *testing.T) {
	createAccountAndUser := func(pubKey, jwt1, creds1 *string) {
		t.Helper()
		kp, _ := nkeys.CreateAccount()
		*pubKey, _ = kp.PublicKey()
		claim := jwt.NewAccountClaims(*pubKey)
		var err error
		*jwt1, err = claim.Encode(oKp)
		require_NoError(t, err)

		ukp, _ := nkeys.CreateUser()
		seed, _ := ukp.Seed()
		upub, _ := ukp.PublicKey()
		uclaim := newJWTTestUserClaims()
		uclaim.Subject = upub

		ujwt1, err := uclaim.Encode(kp)
		require_NoError(t, err)
		*creds1 = genCredsFile(t, ujwt1, seed)
	}
	generateRequest := func(accs []string) []byte {
		t.Helper()
		opk, _ := oKp.PublicKey()
		c := jwt.NewGenericClaims(opk)
		c.Data["accounts"] = accs
		cJwt, err := c.Encode(oKp)
		if err != nil {
			t.Fatalf("Expected no error %v", err)
		}
		return []byte(cJwt)
	}
	for _, cfg := range []string{
		`type: full
 		allow_delete: true`,
		`type: cache`,
	} {
		t.Run("", func(t *testing.T) {
			var syspub, sysjwt, sysCreds string
			createAccountAndUser(&syspub, &sysjwt, &sysCreds)
			defer os.Remove(sysCreds)
			var apub, ajwt1, aCreds1 string
			createAccountAndUser(&apub, &ajwt1, &aCreds1)
			defer os.Remove(aCreds1)
			dirSrv := createDir(t, "srv")
			defer os.RemoveAll(dirSrv)
			conf := createConfFile(t, []byte(fmt.Sprintf(`
		listen: -1
		operator: %s
		system_account: %s
		resolver: {
			%s
			dir: %s
		}
    `, ojwt, syspub, cfg, dirSrv)))
			defer os.Remove(conf)
			srv, _ := RunServerWithConfig(conf)
			defer srv.Shutdown()
			updateJwt(t, srv.ClientURL(), sysCreds, sysjwt, 1) // update system account jwt
			// push jwt (for full resolver)
			updateJwt(t, srv.ClientURL(), sysCreds, ajwt1, 1) // set jwt
			nc := natsConnect(t, srv.ClientURL(), nats.UserCredentials(sysCreds))
			defer nc.Close()
			// simulate nas resolver in case of a lookup request (cache)
			nc.Subscribe(fmt.Sprintf(accLookupReqSubj, apub), func(msg *nats.Msg) {
				msg.Respond([]byte(ajwt1))
			})
			// connect so there is a reason to cache the request
			ncA := natsConnect(t, srv.ClientURL(), nats.UserCredentials(aCreds1))
			ncA.Close()
			resp, err := nc.Request(accListReqSubj, nil, time.Second)
			require_NoError(t, err)
			require_True(t, strings.Contains(string(resp.Data), apub))
			require_True(t, strings.Contains(string(resp.Data), syspub))
			// delete nothing
			resp, err = nc.Request(accDeleteReqSubj, generateRequest([]string{}), time.Second)
			require_NoError(t, err)
			require_True(t, strings.Contains(string(resp.Data), `"message": "deleted 0 accounts"`))
			// issue delete, twice to also delete a non existing account
			for i := 0; i < 2; i++ {
				resp, err = nc.Request(accDeleteReqSubj, generateRequest([]string{apub}), time.Second)
				require_NoError(t, err)
				require_True(t, strings.Contains(string(resp.Data), `"message": "deleted 1 accounts"`))
				resp, err = nc.Request(accListReqSubj, nil, time.Second)
				require_False(t, strings.Contains(string(resp.Data), apub))
				require_True(t, strings.Contains(string(resp.Data), syspub))
				require_NoError(t, err)
			}
		})
	}
}

func createKey(t *testing.T) (nkeys.KeyPair, string) {
	t.Helper()
	kp, _ := nkeys.CreateAccount()
	syspub, _ := kp.PublicKey()
	return kp, syspub
}

func encodeClaim(t *testing.T, claim *jwt.AccountClaims, pub string) string {
	t.Helper()
	theJWT, err := claim.Encode(oKp)
	require_NoError(t, err)
	return theJWT
}

// returns user creds
func newUserEx(t *testing.T, accKp nkeys.KeyPair, scoped bool, issuerAccount string) string {
	ukp, _ := nkeys.CreateUser()
	seed, _ := ukp.Seed()
	upub, _ := ukp.PublicKey()
	uclaim := newJWTTestUserClaims()
	uclaim.Subject = upub
	uclaim.SetScoped(scoped)
	if issuerAccount != _EMPTY_ {
		uclaim.IssuerAccount = issuerAccount
	}
	ujwt, err := uclaim.Encode(accKp)
	require_NoError(t, err)
	return genCredsFile(t, ujwt, seed)
}

// returns user creds
func newUser(t *testing.T, accKp nkeys.KeyPair) string {
	return newUserEx(t, accKp, false, "")
}

func TestJWTHeader(t *testing.T) {
	sysKp, syspub := createKey(t)
	sysJwt := encodeClaim(t, jwt.NewAccountClaims(syspub), syspub)
	sysCreds := newUser(t, sysKp)
	defer os.Remove(sysCreds)

	test := func(share bool) {
		aExpKp, aExpPub := createKey(t)
		aExpClaim := jwt.NewAccountClaims(aExpPub)
		aExpClaim.Exports.Add(&jwt.Export{
			Name:     "test",
			Subject:  "srvc",
			Type:     jwt.Service,
			TokenReq: false,
			Latency: &jwt.ServiceLatency{
				Sampling: jwt.Headers,
				Results:  "res",
			},
		})
		aExpJwt := encodeClaim(t, aExpClaim, aExpPub)
		aExpCreds := newUser(t, aExpKp)
		defer os.Remove(aExpCreds)

		aImpKp, aImpPub := createKey(t)
		aImpClaim := jwt.NewAccountClaims(aImpPub)
		aImpClaim.Imports.Add(&jwt.Import{
			Name:    "test",
			Subject: "srvc",
			Account: aExpPub,
			Type:    jwt.Service,
			Share:   share,
		})
		aImpJwt := encodeClaim(t, aImpClaim, aImpPub)
		aImpCreds := newUser(t, aImpKp)
		defer os.Remove(aImpCreds)

		dirSrv := createDir(t, "srv")
		defer os.RemoveAll(dirSrv)
		conf := createConfFile(t, []byte(fmt.Sprintf(`
		listen: -1
		operator: %s
		system_account: %s
		resolver: {
			type: full
			dir: %s
		}
    `, ojwt, syspub, dirSrv)))
		defer os.Remove(conf)
		srv, _ := RunServerWithConfig(conf)
		defer srv.Shutdown()
		updateJwt(t, srv.ClientURL(), sysCreds, sysJwt, 1) // update system account jwt
		updateJwt(t, srv.ClientURL(), sysCreds, aExpJwt, 1)
		updateJwt(t, srv.ClientURL(), sysCreds, aImpJwt, 1)

		expNc := natsConnect(t, srv.ClientURL(), nats.UserCredentials(aExpCreds))
		defer expNc.Close()
		resChan := make(chan *nats.Msg, 1)
		expNc.ChanSubscribe("res", resChan)
		sub, err := expNc.Subscribe("srvc", func(msg *nats.Msg) {
			msg.Respond(nil)
		})
		require_NoError(t, err)
		defer sub.Unsubscribe()

		impNc := natsConnect(t, srv.ClientURL(), nats.UserCredentials(aImpCreds))
		defer impNc.Close()
		// send request w/o header
		_, err = impNc.Request("srvc", []byte("msg1"), time.Second)
		require_NoError(t, err)
		require_True(t, len(resChan) == 0)

		_, err = impNc.RequestMsg(&nats.Msg{
			Subject: "srvc", Data: []byte("msg2"), Header: http.Header{
				"X-B3-Sampled": []string{"1"},
				"Share":        []string{"Me"}}}, time.Second)
		require_NoError(t, err)
		select {
		case <-time.After(time.Second):
			t.Fatalf("should have received a response")
		case m := <-resChan:
			obj := map[string]interface{}{}
			err = json.Unmarshal(m.Data, &obj)
			require_NoError(t, err)
			// test if shared is honored
			reqInfo := obj["requestor"].(map[string]interface{})
			// fields always set
			require_True(t, reqInfo["acc"] != nil)
			require_True(t, reqInfo["rtt"] != nil)

			// fields only set when shared
			_, ok1 := reqInfo["lang"]
			_, ok2 := reqInfo["ver"]
			_, ok3 := reqInfo["host"]
			_, ok4 := reqInfo["start"]
			if !share {
				ok1 = !ok1
				ok2 = !ok2
				ok3 = !ok3
				ok4 = !ok4
			}
			require_True(t, ok1)
			require_True(t, ok2)
			require_True(t, ok3)
			require_True(t, ok4)

		}
		require_True(t, len(resChan) == 0)
	}
	test(true)
	test(false)
}

func TestJWTAccountImportsWithWildcardSupport(t *testing.T) {
	test := func(aExpPub, aExpJwt, aExpCreds, aImpPub, aImpJwt, aImpCreds string, jsEnabled bool, exSubExpect, exPub, imReq, imSubExpect string) {
		t.Helper()

		var jsSetting string
		if jsEnabled {
			jsSetting = "jetstream: {max_mem_store: 10Mb, max_file_store: 10Mb}"
		}

		_, aSysPub := createKey(t)
		aSysClaim := jwt.NewAccountClaims(aSysPub)
		aSysJwt := encodeClaim(t, aSysClaim, aSysPub)
		cf := createConfFile(t, []byte(fmt.Sprintf(`
		port: -1
		operator = %s
		resolver = MEMORY
		resolver_preload = {
			%s : "%s"
			%s : "%s"
			%s : "%s"
		}
		system_account: %s
		%s
		`, ojwt, aExpPub, aExpJwt, aImpPub, aImpJwt, aSysPub, aSysJwt, aSysPub, jsSetting)))
		defer os.Remove(cf)

		s, opts := RunServerWithConfig(cf)
		defer s.Shutdown()

		ncExp := natsConnect(t, fmt.Sprintf("nats://%s:%d", opts.Host, opts.Port), nats.UserCredentials(aExpCreds))
		defer ncExp.Close()

		ncImp := natsConnect(t, fmt.Sprintf("nats://%s:%d", opts.Host, opts.Port), nats.UserCredentials(aImpCreds))
		defer ncImp.Close()

		// Create subscriber for the service endpoint in foo.
		_, err := ncExp.Subscribe(exSubExpect, func(m *nats.Msg) {
			m.Respond([]byte("yes!"))
		})
		require_NoError(t, err)
		ncExp.Flush()

		// Now test service import.
		if resp, err := ncImp.Request(imReq, []byte("yes?"), time.Second); err != nil {
			t.Fatalf("Expected a response to request %s got: %v", imReq, err)
		} else if string(resp.Data) != "yes!" {
			t.Fatalf("Expected a response of %q, got %q", "yes!", resp.Data)
		}
		subBar, err := ncImp.SubscribeSync(imSubExpect)
		require_NoError(t, err)
		ncImp.Flush()

		ncExp.Publish(exPub, []byte("event!"))

		if m, err := subBar.NextMsg(time.Second); err != nil {
			t.Fatalf("Expected a stream message got %v", err)
		} else if string(m.Data) != "event!" {
			t.Fatalf("Expected a response of %q, got %q", "event!", m.Data)
		}
	}
	createExporter := func() (string, string, string) {
		t.Helper()
		aExpKp, aExpPub := createKey(t)
		aExpClaim := jwt.NewAccountClaims(aExpPub)
		aExpClaim.Name = "Export"
		aExpClaim.Exports.Add(&jwt.Export{
			Subject: "$request.*.$in.*.>",
			Type:    jwt.Service,
		}, &jwt.Export{
			Subject: "$events.*.$in.*.>",
			Type:    jwt.Stream,
		})
		aExpJwt := encodeClaim(t, aExpClaim, aExpPub)
		aExpCreds := newUser(t, aExpKp)
		return aExpPub, aExpJwt, aExpCreds
	}
	t.Run("To", func(t *testing.T) {
		aExpPub, aExpJwt, aExpCreds := createExporter()
		defer os.Remove(aExpCreds)
		aImpKp, aImpPub := createKey(t)
		aImpClaim := jwt.NewAccountClaims(aImpPub)
		aImpClaim.Name = "Import"
		aImpClaim.Imports.Add(&jwt.Import{
			Subject: "my.request.*.*.>",
			Type:    jwt.Service,
			To:      "$request.*.$in.*.>", // services have local and remote switched between Subject and To
			Account: aExpPub,
		}, &jwt.Import{
			Subject: "$events.*.$in.*.>",
			Type:    jwt.Stream,
			To:      "prefix",
			Account: aExpPub,
		})
		aImpJwt := encodeClaim(t, aImpClaim, aImpPub)
		aImpCreds := newUser(t, aImpKp)
		defer os.Remove(aImpCreds)
		test(aExpPub, aExpJwt, aExpCreds, aImpPub, aImpJwt, aImpCreds, false,
			"$request.1.$in.2.bar", "$events.1.$in.2.bar",
			"my.request.1.2.bar", "prefix.$events.1.$in.2.bar")
	})
	t.Run("LocalSubject-No-Reorder", func(t *testing.T) {
		aExpPub, aExpJwt, aExpCreds := createExporter()
		defer os.Remove(aExpCreds)
		aImpKp, aImpPub := createKey(t)
		aImpClaim := jwt.NewAccountClaims(aImpPub)
		aImpClaim.Name = "Import"
		aImpClaim.Imports.Add(&jwt.Import{
			Subject:      "$request.*.$in.*.>",
			Type:         jwt.Service,
			LocalSubject: "my.request.*.*.>",
			Account:      aExpPub,
		}, &jwt.Import{
			Subject:      "$events.*.$in.*.>",
			Type:         jwt.Stream,
			LocalSubject: "my.events.*.*.>",
			Account:      aExpPub,
		})
		aImpJwt := encodeClaim(t, aImpClaim, aImpPub)
		aImpCreds := newUser(t, aImpKp)
		defer os.Remove(aImpCreds)
		test(aExpPub, aExpJwt, aExpCreds, aImpPub, aImpJwt, aImpCreds, false,
			"$request.1.$in.2.bar", "$events.1.$in.2.bar",
			"my.request.1.2.bar", "my.events.1.2.bar")
	})
	t.Run("LocalSubject-Reorder", func(t *testing.T) {
		for _, jsEnabled := range []bool{false, true} {
			t.Run(fmt.Sprintf("%t", jsEnabled), func(t *testing.T) {
				aExpPub, aExpJwt, aExpCreds := createExporter()
				defer os.Remove(aExpCreds)
				aImpKp, aImpPub := createKey(t)
				aImpClaim := jwt.NewAccountClaims(aImpPub)
				aImpClaim.Name = "Import"
				aImpClaim.Imports.Add(&jwt.Import{
					Subject:      "$request.*.$in.*.>",
					Type:         jwt.Service,
					LocalSubject: "my.request.$2.$1.>",
					Account:      aExpPub,
				}, &jwt.Import{
					Subject:      "$events.*.$in.*.>",
					Type:         jwt.Stream,
					LocalSubject: "my.events.$2.$1.>",
					Account:      aExpPub,
				})
				aImpJwt := encodeClaim(t, aImpClaim, aImpPub)
				aImpCreds := newUser(t, aImpKp)
				defer os.Remove(aImpCreds)
				test(aExpPub, aExpJwt, aExpCreds, aImpPub, aImpJwt, aImpCreds, jsEnabled,
					"$request.2.$in.1.bar", "$events.1.$in.2.bar",
					"my.request.1.2.bar", "my.events.2.1.bar")
			})
		}
	})
}

func TestJWTResponseThreshold(t *testing.T) {
	respThresh := 20 * time.Millisecond
	aExpKp, aExpPub := createKey(t)
	aExpClaim := jwt.NewAccountClaims(aExpPub)
	aExpClaim.Name = "Export"
	aExpClaim.Exports.Add(&jwt.Export{
		Subject:           "srvc",
		Type:              jwt.Service,
		ResponseThreshold: respThresh,
	})
	aExpJwt := encodeClaim(t, aExpClaim, aExpPub)
	aExpCreds := newUser(t, aExpKp)

	defer os.Remove(aExpCreds)
	aImpKp, aImpPub := createKey(t)
	aImpClaim := jwt.NewAccountClaims(aImpPub)
	aImpClaim.Name = "Import"
	aImpClaim.Imports.Add(&jwt.Import{
		Subject: "srvc",
		Type:    jwt.Service,
		Account: aExpPub,
	})
	aImpJwt := encodeClaim(t, aImpClaim, aImpPub)
	aImpCreds := newUser(t, aImpKp)
	defer os.Remove(aImpCreds)

	cf := createConfFile(t, []byte(fmt.Sprintf(`
		port: -1
		operator = %s
		resolver = MEMORY
		resolver_preload = {
			%s : "%s"
			%s : "%s"
		}
		`, ojwt, aExpPub, aExpJwt, aImpPub, aImpJwt)))
	defer os.Remove(cf)

	s, opts := RunServerWithConfig(cf)
	defer s.Shutdown()

	ncExp := natsConnect(t, fmt.Sprintf("nats://%s:%d", opts.Host, opts.Port), nats.UserCredentials(aExpCreds))
	defer ncExp.Close()

	ncImp := natsConnect(t, fmt.Sprintf("nats://%s:%d", opts.Host, opts.Port), nats.UserCredentials(aImpCreds))
	defer ncImp.Close()

	delayChan := make(chan time.Duration, 1)

	// Create subscriber for the service endpoint in foo.
	_, err := ncExp.Subscribe("srvc", func(m *nats.Msg) {
		time.Sleep(<-delayChan)
		m.Respond([]byte("yes!"))
	})
	require_NoError(t, err)
	ncExp.Flush()

	t.Run("No-Timeout", func(t *testing.T) {
		delayChan <- respThresh / 2
		if resp, err := ncImp.Request("srvc", []byte("yes?"), 4*respThresh); err != nil {
			t.Fatalf("Expected a response to request srvc got: %v", err)
		} else if string(resp.Data) != "yes!" {
			t.Fatalf("Expected a response of %q, got %q", "yes!", resp.Data)
		}
	})
	t.Run("Timeout", func(t *testing.T) {
		delayChan <- 2 * respThresh
		if _, err := ncImp.Request("srvc", []byte("yes?"), 4*respThresh); err == nil || err != nats.ErrTimeout {
			t.Fatalf("Expected a timeout")
		}
	})
}

func TestJWTQueuePermissions(t *testing.T) {
	aExpKp, aExpPub := createKey(t)
	aExpClaim := jwt.NewAccountClaims(aExpPub)
	aExpJwt := encodeClaim(t, aExpClaim, aExpPub)
	newUser := func(t *testing.T, permType string) string {
		ukp, _ := nkeys.CreateUser()
		seed, _ := ukp.Seed()
		upub, _ := ukp.PublicKey()
		uclaim := newJWTTestUserClaims()
		uclaim.Subject = upub
		switch permType {
		case "allow":
			uclaim.Permissions.Sub.Allow.Add("foo.> *.dev")
		case "deny":
			uclaim.Permissions.Sub.Deny.Add("foo.> *.dev")
		}
		ujwt, err := uclaim.Encode(aExpKp)
		require_NoError(t, err)
		return genCredsFile(t, ujwt, seed)
	}
	confFileName := createConfFile(t, []byte(fmt.Sprintf(`
		port: -1
		operator = %s
		resolver = MEMORY
		resolver_preload = {
			%s : %s
		}`, ojwt, aExpPub, aExpJwt)))
	defer os.Remove(confFileName)
	opts, err := ProcessConfigFile(confFileName)
	if err != nil {
		t.Fatalf("Received unexpected error %s", err)
	}
	opts.NoLog, opts.NoSigs = true, true
	errChan := make(chan error, 1)
	defer close(errChan)
	s := RunServer(opts)
	defer s.Shutdown()

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
			usrCreds := newUser(t, test.permType)
			defer os.Remove(usrCreds)
			nc, err := nats.Connect(fmt.Sprintf("nats://127.0.0.1:%d", opts.Port),
				nats.ErrorHandler(func(conn *nats.Conn, s *nats.Subscription, err error) {
					errChan <- err
				}),
				nats.UserCredentials(usrCreds))
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

func TestJWScopedSigningKeys(t *testing.T) {
	sysKp, syspub := createKey(t)
	sysJwt := encodeClaim(t, jwt.NewAccountClaims(syspub), syspub)
	sysCreds := newUser(t, sysKp)
	defer os.Remove(sysCreds)

	_, aExpPub := createKey(t)
	accClaim := jwt.NewAccountClaims(aExpPub)
	accClaim.Name = "acc"

	aSignNonScopedKp, aSignNonScopedPub := createKey(t)
	accClaim.SigningKeys.Add(aSignNonScopedPub)

	aSignScopedKp, aSignScopedPub := createKey(t)
	signer := jwt.NewUserScope()
	signer.Key = aSignScopedPub
	signer.Template.Pub.Deny.Add("denied")
	signer.Template.Payload = 5
	accClaim.SigningKeys.AddScopedSigner(signer)
	accJwt := encodeClaim(t, accClaim, aExpPub)

	aNonScopedCreds := newUserEx(t, aSignNonScopedKp, false, aExpPub)
	defer os.Remove(aNonScopedCreds)
	aBadScopedCreds := newUserEx(t, aSignScopedKp, false, aExpPub)
	defer os.Remove(aBadScopedCreds)
	aScopedCreds := newUserEx(t, aSignScopedKp, true, aExpPub)
	defer os.Remove(aScopedCreds)

	dirSrv := createDir(t, "srv")
	defer os.RemoveAll(dirSrv)
	cf := createConfFile(t, []byte(fmt.Sprintf(`
		listen: -1
		operator: %s
		system_account: %s
		resolver: {
			type: full
			dir: %s
		}
    `, ojwt, syspub, dirSrv)))
	defer os.Remove(cf)
	s, opts := RunServerWithConfig(cf)
	defer s.Shutdown()

	url := fmt.Sprintf("nats://%s:%d", opts.Host, opts.Port)
	errChan := make(chan error, 1)
	defer close(errChan)
	awaitError := func(expected bool) {
		t.Helper()
		select {
		case err := <-errChan:
			if !expected {
				t.Fatalf("Expected no error, got %v", err)
			}
		case <-time.After(150 * time.Millisecond):
			if expected {
				t.Fatal("Expected an error")
			}
		}
	}
	errHdlr := nats.ErrorHandler(func(conn *nats.Conn, s *nats.Subscription, err error) {
		errChan <- err
	})
	if updateJwt(t, url, sysCreds, sysJwt, 1) != 1 {
		t.Error("Expected update to pass")
	} else if updateJwt(t, url, sysCreds, accJwt, 1) != 1 {
		t.Error("Expected update to pass")
	}
	t.Run("bad-scoped-signing-key", func(t *testing.T) {
		_, err := nats.Connect(url, nats.UserCredentials(aBadScopedCreds))
		require_Error(t, err)
	})
	t.Run("regular-signing-key", func(t *testing.T) {
		nc := natsConnect(t, url, nats.UserCredentials(aNonScopedCreds), errHdlr)
		defer nc.Close()
		nc.Flush()
		err := nc.Publish("denied", nil)
		require_NoError(t, err)
		nc.Flush()
		awaitError(false)
	})
	t.Run("scoped-signing-key-client-side", func(t *testing.T) {
		nc := natsConnect(t, url, nats.UserCredentials(aScopedCreds), errHdlr)
		defer nc.Close()
		nc.Flush()
		err := nc.Publish("too-long", []byte("way.too.long.for.payload.limit"))
		require_Error(t, err)
		require_True(t, strings.Contains(err.Error(), ErrMaxPayload.Error()))
	})
	t.Run("scoped-signing-key-server-side", func(t *testing.T) {
		nc := natsConnect(t, url, nats.UserCredentials(aScopedCreds), errHdlr)
		defer nc.Close()
		nc.Flush()
		err := nc.Publish("denied", nil)
		require_NoError(t, err)
		nc.Flush()
		awaitError(true)
	})
	t.Run("scoped-signing-key-reload", func(t *testing.T) {
		reconChan := make(chan struct{}, 1)
		defer close(reconChan)
		msgChan := make(chan *nats.Msg, 2)
		defer close(msgChan)
		nc := natsConnect(t, url, nats.UserCredentials(aScopedCreds), errHdlr,
			nats.DisconnectErrHandler(func(conn *nats.Conn, err error) {
				if err != nil {
					errChan <- err
				}
			}),
			nats.ReconnectHandler(func(conn *nats.Conn) {
				reconChan <- struct{}{}
			}),
		)
		defer nc.Close()
		_, err := nc.ChanSubscribe("denied", msgChan)
		require_NoError(t, err)
		nc.Flush()
		err = nc.Publish("denied", nil)
		require_NoError(t, err)
		awaitError(true)
		require_Len(t, len(msgChan), 0)
		// Alter scoped permissions and update
		signer.Template.Payload = -1
		signer.Template.Pub.Deny.Remove("denied")
		accClaim.SigningKeys.AddScopedSigner(signer)
		accUpdatedJwt := encodeClaim(t, accClaim, aExpPub)
		if updateJwt(t, url, sysCreds, accUpdatedJwt, 1) != 1 {
			t.Error("Expected update to pass")
		}
		// disconnect triggered by update
		awaitError(true)
		<-reconChan
		nc.Flush()
		err = nc.Publish("denied", []byte("way.too.long.for.old.payload.limit"))
		require_NoError(t, err)
		awaitError(false)
		msg := <-msgChan
		require_Equal(t, string(msg.Data), "way.too.long.for.old.payload.limit")
		require_Len(t, len(msgChan), 0)
	})
	require_Len(t, len(errChan), 0)
}

func TestJWTStrictSigningKeys(t *testing.T) {
	newAccount := func(opKp nkeys.KeyPair) (nkeys.KeyPair, nkeys.KeyPair, string, *jwt.AccountClaims, string) {
		accId, err := nkeys.CreateAccount()
		require_NoError(t, err)
		accIdPub, err := accId.PublicKey()
		require_NoError(t, err)

		accSig, err := nkeys.CreateAccount()
		require_NoError(t, err)
		accSigPub, err := accSig.PublicKey()
		require_NoError(t, err)

		aClaim := jwt.NewAccountClaims(accIdPub)
		aClaim.SigningKeys.Add(accSigPub)
		theJwt, err := aClaim.Encode(opKp)
		require_NoError(t, err)
		return accId, accSig, accIdPub, aClaim, theJwt
	}

	opId, err := nkeys.CreateOperator()
	require_NoError(t, err)
	opIdPub, err := opId.PublicKey()
	require_NoError(t, err)

	opSig, err := nkeys.CreateOperator()
	require_NoError(t, err)
	opSigPub, err := opSig.PublicKey()
	require_NoError(t, err)

	aBadBadKp, aBadGoodKp, aBadPub, _, aBadJwt := newAccount(opId)
	aGoodBadKp, aGoodGoodKp, aGoodPub, _, aGoodJwt := newAccount(opSig)
	_, aSysKp, aSysPub, _, aSysJwt := newAccount(opSig)

	oClaim := jwt.NewOperatorClaims(opIdPub)
	oClaim.StrictSigningKeyUsage = true
	oClaim.SigningKeys.Add(opSigPub)
	oClaim.SystemAccount = aSysPub
	oJwt, err := oClaim.Encode(opId)
	require_NoError(t, err)

	uBadBadCreds := newUserEx(t, aBadBadKp, false, aBadPub)
	defer os.Remove(uBadBadCreds)
	uBadGoodCreds := newUserEx(t, aBadGoodKp, false, aBadPub)
	defer os.Remove(uBadGoodCreds)
	uGoodBadCreds := newUserEx(t, aGoodBadKp, false, aGoodPub)
	defer os.Remove(uGoodBadCreds)
	uGoodGoodCreds := newUserEx(t, aGoodGoodKp, false, aGoodPub)
	defer os.Remove(uGoodGoodCreds)
	uSysCreds := newUserEx(t, aSysKp, false, aSysPub)
	defer os.Remove(uSysCreds)

	connectTest := func(url string) {
		for _, test := range []struct {
			creds string
			fail  bool
		}{
			{uBadBadCreds, true},
			{uBadGoodCreds, true},
			{uGoodBadCreds, true},
			{uGoodGoodCreds, false},
		} {
			nc, err := nats.Connect(url, nats.UserCredentials(test.creds))
			nc.Close()
			if test.fail {
				require_Error(t, err)
			} else {
				require_NoError(t, err)
			}
		}
	}

	t.Run("resolver", func(t *testing.T) {
		dirSrv := createDir(t, "srv")
		defer os.RemoveAll(dirSrv)
		cf := createConfFile(t, []byte(fmt.Sprintf(`
		port: -1
		operator = %s
		resolver: {
			type: full
			dir: %s
		}
		resolver_preload = {
			%s : "%s"
		}
		`, oJwt, dirSrv, aSysPub, aSysJwt)))
		defer os.Remove(cf)
		s, _ := RunServerWithConfig(cf)
		defer s.Shutdown()
		url := s.ClientURL()
		if updateJwt(t, url, uSysCreds, aBadJwt, 1) != 0 {
			t.Fatal("Expected negative response")
		}
		if updateJwt(t, url, uSysCreds, aGoodJwt, 1) != 1 {
			t.Fatal("Expected positive response")
		}
		connectTest(url)
	})

	t.Run("mem-resolver", func(t *testing.T) {
		dirSrv := createDir(t, "srv")
		defer os.RemoveAll(dirSrv)
		cf := createConfFile(t, []byte(fmt.Sprintf(`
		port: -1
		operator = %s
		resolver: MEMORY
		resolver_preload = {
			%s : "%s"
			%s : "%s"
			%s : "%s"
		}
		`, oJwt, aSysPub, aSysJwt, aBadPub, aBadJwt, aGoodPub, aGoodJwt)))
		defer os.Remove(cf)
		s, _ := RunServerWithConfig(cf)
		defer s.Shutdown()
		connectTest(s.ClientURL())
	})
}

func TestJWTAccountProtectedImport(t *testing.T) {
	srvFmt := `
		port: -1
		operator = %s
		resolver: MEMORY
		resolver_preload = {
			%s : "%s"
			%s : "%s"
		} `
	setupAccounts := func(pass bool) (nkeys.KeyPair, string, string, string, nkeys.KeyPair, string, string, string, string) {
		// Create accounts and imports/exports.
		exportKP, _ := nkeys.CreateAccount()
		exportPub, _ := exportKP.PublicKey()
		exportAC := jwt.NewAccountClaims(exportPub)
		exportAC.Exports.Add(&jwt.Export{Subject: "service.*", Type: jwt.Service, AccountTokenPosition: 2})
		exportAC.Exports.Add(&jwt.Export{Subject: "stream.*", Type: jwt.Stream, AccountTokenPosition: 2})
		exportJWT, err := exportAC.Encode(oKp)
		require_NoError(t, err)
		// create alternative exporter jwt without account token pos set
		exportAC.Exports = jwt.Exports{}
		exportAC.Exports.Add(&jwt.Export{Subject: "service.*", Type: jwt.Service})
		exportAC.Exports.Add(&jwt.Export{Subject: "stream.*", Type: jwt.Stream})
		exportJWTNoPos, err := exportAC.Encode(oKp)
		require_NoError(t, err)

		importKP, _ := nkeys.CreateAccount()
		importPub, _ := importKP.PublicKey()
		importAc := jwt.NewAccountClaims(importPub)
		srvcSub, strmSub := "service.foo", "stream.foo"
		if pass {
			srvcSub = fmt.Sprintf("service.%s", importPub)
			strmSub = fmt.Sprintf("stream.%s", importPub)
		}
		importAc.Imports.Add(&jwt.Import{Account: exportPub, Subject: jwt.Subject(srvcSub), Type: jwt.Service})
		importAc.Imports.Add(&jwt.Import{Account: exportPub, Subject: jwt.Subject(strmSub), Type: jwt.Stream})
		importJWT, err := importAc.Encode(oKp)
		require_NoError(t, err)

		return exportKP, exportPub, exportJWT, exportJWTNoPos, importKP, importPub, importJWT, srvcSub, strmSub
	}
	t.Run("pass", func(t *testing.T) {
		exportKp, exportPub, exportJWT, _, importKp, importPub, importJWT, srvcSub, strmSub := setupAccounts(true)
		cf := createConfFile(t, []byte(fmt.Sprintf(srvFmt, ojwt, exportPub, exportJWT, importPub, importJWT)))
		defer os.Remove(cf)
		s, _ := RunServerWithConfig(cf)
		defer s.Shutdown()
		ncExp := natsConnect(t, s.ClientURL(), createUserCreds(t, s, exportKp))
		ncImp := natsConnect(t, s.ClientURL(), createUserCreds(t, s, importKp))
		t.Run("service", func(t *testing.T) {
			sub, err := ncExp.Subscribe("service.*", func(msg *nats.Msg) {
				msg.Respond([]byte("world"))
			})
			defer sub.Unsubscribe()
			require_NoError(t, err)
			ncExp.Flush()
			msg, err := ncImp.Request(srvcSub, []byte("hello"), time.Second)
			require_NoError(t, err)
			require_Equal(t, string(msg.Data), "world")
		})
		t.Run("stream", func(t *testing.T) {
			msgChan := make(chan *nats.Msg, 4)
			defer close(msgChan)
			sub, err := ncImp.ChanSubscribe(strmSub, msgChan)
			defer sub.Unsubscribe()
			require_NoError(t, err)
			ncImp.Flush()
			err = ncExp.Publish("stream.foo", []byte("hello"))
			require_NoError(t, err)
			err = ncExp.Publish(strmSub, []byte("hello"))
			require_NoError(t, err)
			msg := <-msgChan
			require_Equal(t, string(msg.Data), "hello")
			require_True(t, len(msgChan) == 0)
		})
	})
	t.Run("fail", func(t *testing.T) {
		exportKp, exportPub, exportJWT, _, importKp, importPub, importJWT, srvcSub, strmSub := setupAccounts(false)
		cf := createConfFile(t, []byte(fmt.Sprintf(srvFmt, ojwt, exportPub, exportJWT, importPub, importJWT)))
		defer os.Remove(cf)
		s, _ := RunServerWithConfig(cf)
		defer s.Shutdown()
		ncExp := natsConnect(t, s.ClientURL(), createUserCreds(t, s, exportKp))
		ncImp := natsConnect(t, s.ClientURL(), createUserCreds(t, s, importKp))
		t.Run("service", func(t *testing.T) {
			sub, err := ncExp.Subscribe("service.*", func(msg *nats.Msg) {
				msg.Respond([]byte("world"))
			})
			defer sub.Unsubscribe()
			require_NoError(t, err)
			ncExp.Flush()
			_, err = ncImp.Request(srvcSub, []byte("hello"), time.Second)
			require_Error(t, err)
			require_Contains(t, err.Error(), "no responders available for request")
		})
		t.Run("stream", func(t *testing.T) {
			msgChan := make(chan *nats.Msg, 4)
			defer close(msgChan)
			_, err := ncImp.ChanSubscribe(strmSub, msgChan)
			require_NoError(t, err)
			ncImp.Flush()
			err = ncExp.Publish("stream.foo", []byte("hello"))
			require_NoError(t, err)
			err = ncExp.Publish(strmSub, []byte("hello"))
			require_NoError(t, err)
			select {
			case <-msgChan:
				t.Fatal("did not expect a message")
			case <-time.After(250 * time.Millisecond):
			}
			require_True(t, len(msgChan) == 0)
		})
	})
	t.Run("reload-off-2-on", func(t *testing.T) {
		exportKp, exportPub, exportJWTOn, exportJWTOff, importKp, _, importJWT, srvcSub, strmSub := setupAccounts(false)
		dirSrv := createDir(t, "srv")
		defer os.RemoveAll(dirSrv)
		// set up system account. Relying bootstrapping system account to not create JWT
		sysAcc, err := nkeys.CreateAccount()
		require_NoError(t, err)
		sysPub, err := sysAcc.PublicKey()
		require_NoError(t, err)
		sysUsrCreds := newUserEx(t, sysAcc, false, sysPub)
		defer os.Remove(sysUsrCreds)
		cf := createConfFile(t, []byte(fmt.Sprintf(`		
		port: -1
		operator = %s
		system_account = %s
		resolver: {
			type: full
			dir: %s
		}`, ojwt, sysPub, dirSrv)))
		defer os.Remove(cf)
		s, _ := RunServerWithConfig(cf)
		defer s.Shutdown()
		updateJwt(t, s.ClientURL(), sysUsrCreds, importJWT, 1)
		updateJwt(t, s.ClientURL(), sysUsrCreds, exportJWTOff, 1)
		ncExp := natsConnect(t, s.ClientURL(), createUserCreds(t, s, exportKp))
		ncImp := natsConnect(t, s.ClientURL(), createUserCreds(t, s, importKp))
		msgChan := make(chan *nats.Msg, 4)
		defer close(msgChan)
		// ensure service passes
		subSrvc, err := ncExp.Subscribe("service.*", func(msg *nats.Msg) {
			msg.Respond([]byte("world"))
		})
		defer subSrvc.Unsubscribe()
		require_NoError(t, err)
		ncExp.Flush()
		respMst, err := ncImp.Request(srvcSub, []byte("hello"), time.Second)
		require_NoError(t, err)
		require_Equal(t, string(respMst.Data), "world")
		// ensure stream passes
		subStrm, err := ncImp.ChanSubscribe(strmSub, msgChan)
		defer subStrm.Unsubscribe()
		require_NoError(t, err)
		ncImp.Flush()
		err = ncExp.Publish(strmSub, []byte("hello"))
		require_NoError(t, err)
		msg := <-msgChan
		require_Equal(t, string(msg.Data), "hello")
		require_True(t, len(msgChan) == 0)

		updateJwt(t, s.ClientURL(), sysUsrCreds, exportJWTOn, 1)

		// ensure service fails
		_, err = ncImp.Request(srvcSub, []byte("hello"), time.Second)
		require_Error(t, err)
		require_Contains(t, err.Error(), "timeout")
		s.AccountResolver().Store(exportPub, exportJWTOn)
		// ensure stream fails
		err = ncExp.Publish(strmSub, []byte("hello"))
		require_NoError(t, err)
		select {
		case <-msgChan:
			t.Fatal("did not expect a message")
		case <-time.After(250 * time.Millisecond):
		}
		require_True(t, len(msgChan) == 0)
	})
}

func TestJWTAccountCleanup(t *testing.T) {
	// This test connects with different account over and over again and make sure memory does not grow
	jwtChan := make(chan string, 1)
	defer close(jwtChan)
	// usa http based resolver so we don't have to worry about account accumulation in resolver
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/A/" {
			w.Write(nil)
			return
		}
		theJwt := <-jwtChan
		if theJwt == "" {
			return
		}
		w.Write([]byte(theJwt))
	}))
	defer ts.Close()
	// To inspect inuse objects include: 	_ "net/http/pprof"
	// Here add: go func() { log.Println(http.ListenAndServe("localhost:6060", nil)) }()
	// Query with: go tool pprof -sample_index=inuse_objects -web http://localhost:6060/debug/pprof/heap

	// long text to use in jwt / subscriptions such that account/client/subscription leaks are more apparent
	longText := `01234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901` +
		`0123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789` +
		`0123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789` +
		`0123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789` +
		`0123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789` +
		`0123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789` +
		`012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678923456789`
	dirSrv := createDir(t, "srv")
	defer os.RemoveAll(dirSrv)
	conf := createConfFile(t, []byte(fmt.Sprintf(`
		listen: -1
		http: 50505
		operator: %s
		resolver: URL("%s/A/")
    `, ojwt, ts.URL)))
	defer os.Remove(conf)
	opts := LoadConfig(conf)
	opts.MaxClosedClients = 1  // reduce so we do not observe growth from that
	opts.MaxUnusedAccounts = 4 // reduce to not observe growth of unused but still buffered accounts
	srv := RunServer(opts)
	defer srv.Shutdown()

	lenClientsAndAccounts := func() int {
		accCnt := 0
		srv.accounts.Range(func(key, value interface{}) bool {
			accCnt++
			return true
		})
		srv.mu.Lock()
		clientCnt := len(srv.clients) + len(srv.clients) + len(srv.nkeys) +
			len(srv.grTmpClients) + len(srv.leafs) + len(srv.remotes) + len(srv.routes)

		srv.mu.Unlock()
		return accCnt + clientCnt
	}

	// create export account
	aExpKp, aExpPub := createKey(t)
	aExpClaim := jwt.NewAccountClaims(aExpPub)
	aExpClaim.Exports.Add(&jwt.Export{
		Name:    "bar.stream",
		Subject: "bar.stream",
		Type:    jwt.Stream,
	}, &jwt.Export{
		Name:    "bar.service",
		Subject: "bar.service",
		Type:    jwt.Service,
	})
	aExpJwt := encodeClaim(t, aExpClaim, aExpPub)
	jwtChan <- aExpJwt
	nc := natsConnect(t, srv.ClientURL(), createUserCreds(t, srv, aExpKp))
	nc.Close()

	testIteration := func(i int) {
		aKp, aPub := createKey(t)
		aClaim := jwt.NewAccountClaims(aPub)
		aClaim.Description = longText[:200]
		aClaim.Imports.Add(&jwt.Import{
			Name:         "bar.stream",
			Subject:      "bar.stream",
			Account:      aExpPub,
			LocalSubject: "stream",
			Type:         jwt.Stream,
		}, &jwt.Import{
			Name:         "bar.service",
			Subject:      "bar.service",
			Account:      aExpPub,
			LocalSubject: "service",
			Type:         jwt.Service,
		})
		aJwt := encodeClaim(t, aClaim, aPub)
		// make jwt available to http server
		jwtChan <- aJwt
		// test initial connect as well as a re connect
		for j := 0; j < 2; j++ {
			kp, _ := nkeys.CreateUser()
			pub, _ := kp.PublicKey()
			seed, _ := kp.Seed()
			nuc := jwt.NewUserClaims(pub)
			nuc.Name = longText
			ujwt, err := nuc.Encode(aKp)
			require_NoError(t, err)
			creds := genCredsFile(t, ujwt, seed)
			defer os.Remove(creds)
			require_NoError(t, err)
			nc, err := nats.Connect(srv.ClientURL(), nats.UserCredentials(creds))
			if err != nil {
				t.Fatalf("connect issue in iter %d err: %v", i, err)
			}
			_, err = nc.Subscribe(longText, func(msg *nats.Msg) {})
			require_NoError(t, err)
			_, err = nc.QueueSubscribe("stream", longText, func(msg *nats.Msg) {})
			require_NoError(t, err)
			err = nc.Flush()
			require_NoError(t, err)
			nc.Close()
		}
	}
	warmUpLimit := 50
	for i := 0; i < warmUpLimit; i++ {
		testIteration(i)
	}
	// post warm up measurements to compare against
	cnt := lenClientsAndAccounts()
	var memStart runtime.MemStats
	runtime.ReadMemStats(&memStart)
	for i := warmUpLimit; ; i += warmUpLimit {
		for j := 0; j < warmUpLimit; j++ {
			testIteration(i + j)
		}
		// check server
		currCnt := lenClientsAndAccounts()
		if currCnt > cnt+4 {
			t.Fatalf("too many accounts %d %d", lenClientsAndAccounts(), cnt)
		}
		// check monitoring endpoint
		aInfo := &Accountz{}
		body := readBody(t, fmt.Sprintf("http://localhost:%d%s", opts.HTTPPort, AccountzPath))
		err := json.Unmarshal(body, aInfo)
		require_NoError(t, err)
		require_True(t, len(aInfo.Accounts) > opts.MaxUnusedAccounts)
		require_True(t, len(aInfo.Accounts) < 2*opts.MaxUnusedAccounts)
		require_Contains(t, fmt.Sprintf("%v", aInfo.Accounts), "$SYS", "$G", aExpPub)
		// check memory usage
		runtime.GC()
		var memNow runtime.MemStats
		runtime.ReadMemStats(&memNow)
		if memNow.Alloc > memStart.Alloc+memStart.Alloc>>1 {
			t.Fatalf("memory usage %d exceeds %d (more than 1.5 times)", memNow.Alloc, memStart.Alloc)
		}
		// exit after sufficient (4 times) churn
		if memNow.TotalAlloc > memStart.TotalAlloc<<2 {
			break
		}
	}
}
