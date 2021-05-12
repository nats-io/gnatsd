package server

import (
	"bufio"
	"crypto/rsa"
	"fmt"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/dgrijalva/jwt-go"
)

const validPrivateKey = `-----BEGIN RSA PRIVATE KEY-----
MIIEowIBAAKCAQEAqU/GXp8MqmugQyRk5FUFBvlJt1/h7L3Crzlzejz/OxriZdq/
lBNQW9S1kzGc7qjXprZ1Kg3zP6irr6wmvP0WYBGltWs2cWUAmxh0PSxuKdT/OyL9
w+rjKLh4yo3ex6DX3Ij0iP01Ej2POe5WrPDS8j6LT0s4HZ1FprL5h7RUQWV3cO4p
F+1kl6HlBpNzEQzocW9ig4DNdSeUENARHWoCixE1gFYo9RXm7acqgqCk3ihdJRIb
O4e/m1aZq2mvAFK+yHTIWBL0p5PF0Fe8zcWdNeEATYB+eRdNJ3jjS8447YrcbQcB
QmhFjk8hbCnc3Rv3HvAapk8xDFhImdVF1ffDFwIDAQABAoIBAGZIs2ZmX5h0/JST
YAAw/KCB6W7Glg4XdY21/3VRdD+Ytj0iMaqbIGjZz/fkeRIVHnKwt4d4dgN3OoEe
VyjFHMdc4eb/phxLEFqiI1bxiHvtGWP4d6XsON9Y0mBL5NJk8QNiGZjIn08tsWEm
A2bm9gkyj6aPoo8BfBqA9Q5uepgmYIPT2NtEXvTbd2dedAEJDJspHKHqBfcuNBVo
VhUixVSgehWGGP4GX+FvAEHbawDrwULkMvgblH+X8nBtzikp29LNpOZSRRbqF/Da
0AkluFvuDUUIzitjZs5koSEAteaulkZO08BMxtovQjh/ZPtVZKZ27POCNOgRsbm/
lVIXRMECgYEA2TQQ2Xy6eO5XfbiT4ZD1Z1xe9B6Ti7J2fC0ZNNSXs4DzdYVcHNIu
ZqfK6fGqmByvSnFut7n5Po0z2FdXc7xcKFJdBZdFP3GLXbN9vpRPIk9b6n+0df47
1uTYwVocmAGXez++y73j5XzHQQW4WmmC5SlKjQUWCGkuzISVjRDtlZ0CgYEAx43K
PrJxSijjE2+VWYjNFVuv6KilnWoA8I2cZ7TtPi4h//r5vyOUst0egR3lJ7rBof74
VttQPvqAk3GN697IrE/bSwefwG2lM1Ta0KB3jn6b/iT4ckmaOB+v6aDHq/GPW6l/
sxD0RIEelRYZlsNLepRgKhcQckhjnWzQuGWSl0MCgYBYJQ0BdeCm2vKejp1U2OL+
Qzo1j4MJGi+DTToBepTlv9sNQkWTXKh/+HAcaHp2qI1qhIYOAWbov5zemvNegH5V
zrb5Yd40VPvd1s2c3csPfW0ryQ+PItFd8BkWvl8EQQEcf04KmNE3fF/QP2YFKvR3
0z3x5LKAT08yqEuYp9oC8QKBgQCfc9XqGU3bEya3Lg8ptt0gtt2ty6xiRwSvMoiK
eZCkgdpbH6EWMQktjvBD/a5Q+7KjjgfD54SMfj/lEPR1R9QTk8/HeTUWXsaFaMVb
tQ0zSEm/Xq1DLTrUo8U9qmJCK0gA10SZwe9dGctlF36k8DJMpWjd2QYkO2GVthBl
d4wV3wKBgC7S4q0wmcrQIjyDIFmISQNdOAJhR0pJXG8mK2jECbEXxbKkAJnLj73D
J+1OVBlx4HXx54PiEkV3M3iTinf5tBSi8nA2D3s829F65XKFli1RC4rJv+2ygH8P
nXX9rQKhK/v6/jeelKquH8zy894hLZe7feSsWV9GMgb5l9p+UzWB
-----END RSA PRIVATE KEY-----`

const publicKey = `-----BEGIN PUBLIC KEY-----
MIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8AMIIBCgKCAQEAqU/GXp8MqmugQyRk5FUF
BvlJt1/h7L3Crzlzejz/OxriZdq/lBNQW9S1kzGc7qjXprZ1Kg3zP6irr6wmvP0W
YBGltWs2cWUAmxh0PSxuKdT/OyL9w+rjKLh4yo3ex6DX3Ij0iP01Ej2POe5WrPDS
8j6LT0s4HZ1FprL5h7RUQWV3cO4pF+1kl6HlBpNzEQzocW9ig4DNdSeUENARHWoC
ixE1gFYo9RXm7acqgqCk3ihdJRIbO4e/m1aZq2mvAFK+yHTIWBL0p5PF0Fe8zcWd
NeEATYB+eRdNJ3jjS8447YrcbQcBQmhFjk8hbCnc3Rv3HvAapk8xDFhImdVF1ffD
FwIDAQAB
-----END PUBLIC KEY-----`

func cleanupBearerTest(t *testing.T, s *Server) {
	t.Helper()
	s.Shutdown()
	os.Unsetenv("JWT_SIGNER_PUBLIC_KEY")
}

func encodeBearerAuthJWT(issuedAt, expiresAt int64, payload map[string]interface{}, privateKey *rsa.PrivateKey) (string, error) {
	claims := map[string]interface{}{
		"iat": issuedAt,
		"exp": expiresAt,
	}

	for name, value := range payload {
		claims[name] = value
	}

	jwtToken := jwt.NewWithClaims(jwt.SigningMethodRS256, jwt.MapClaims(claims))
	token, err := jwtToken.SignedString(privateKey)
	if err != nil {
		panic(fmt.Sprintf("failed to sign JWT token; %s", err.Error()))
	}
	return token, nil
}

func encodeBearerAuthJWTWithExpirationAfter(expiresAfter time.Duration, permissions map[string]interface{}) (string, error) {
	issuedAt := time.Now()
	payload := map[string]interface{}{
		"permissions": permissions,
	}
	privateKey, err := jwt.ParseRSAPrivateKeyFromPEM([]byte(validPrivateKey))
	if err != nil {
		panic(fmt.Sprintf("failed to parse JWT private key; %s", err.Error()))
	}
	return encodeBearerAuthJWT(issuedAt.Unix(), issuedAt.Add(expiresAfter).Unix(), payload, privateKey)
}

func encodeExpiredBearerAuthJWT(permissions map[string]interface{}) (string, error) {
	return encodeBearerAuthJWTWithExpirationAfter(time.Minute*-5, permissions)
}

func encodeValidBearerAuthJWT(permissions map[string]interface{}) (string, error) {
	issuedAt := time.Now()
	expiresAt := issuedAt.Add(time.Minute * 10)
	payload := map[string]interface{}{}
	if permissions != nil {
		payload["permissions"] = permissions
	}
	privateKey, err := jwt.ParseRSAPrivateKeyFromPEM([]byte(validPrivateKey))
	if err != nil {
		panic(fmt.Sprintf("failed to parse JWT private key; %s", err.Error()))
	}
	return encodeBearerAuthJWT(issuedAt.Unix(), expiresAt.Unix(), payload, privateKey)
}

func setupBearerAuthTest(t *testing.T, jwt, expected string) (*Server, *testAsyncClient, *bufio.Reader) {
	t.Helper()

	os.Setenv("JWT_SIGNER_PUBLIC_KEY", publicKey)

	opts := defaultServerOptions
	s, c, _, _ := rawSetup(opts)
	c.close()

	c, cr, _ := newClientForServer(s)

	// PING needed to flush the +OK/-ERR to us.
	cs := fmt.Sprintf("CONNECT {\"jwt\":%q,\"verbose\":true,\"pedantic\":true}\r\nPING\r\n", jwt)
	wg := sync.WaitGroup{}
	wg.Add(1)
	go func() {
		c.parse([]byte(cs))
		wg.Done()
	}()

	l, _ := cr.ReadString('\n')
	if !strings.HasPrefix(l, expected) {
		t.Fatalf("Expected %q, got %q", expected, l)
	}
	wg.Wait()

	return s, c, cr
}

func TestBearerAuthJWTExpired(t *testing.T) {
	bearerAuthJWT, _ := encodeExpiredBearerAuthJWT(map[string]interface{}{
		"publish": "foo.bar",
	})
	s, _, _ := setupBearerAuthTest(t, bearerAuthJWT, "-ERR 'Authorization Violation'")
	defer cleanupBearerTest(t, s)
}

func TestBearerAuthJWTExpiredAfterConnect(t *testing.T) {
	bearerAuthJWT, _ := encodeBearerAuthJWTWithExpirationAfter(time.Second*2, map[string]interface{}{
		"publish": "foo.bar",
	})
	s, _, cr := setupBearerAuthTest(t, bearerAuthJWT, "+OK")
	defer cleanupBearerTest(t, s)

	l, _ := cr.ReadString('\n')
	if !strings.HasPrefix(l, "PONG") {
		t.Fatalf("Expected a PONG")
	}

	// Now we should expire after about 2 seconds or so.
	time.Sleep(2250 * time.Millisecond)

	l, _ = cr.ReadString('\n')
	if !strings.HasPrefix(l, "-ERR ") {
		t.Fatalf("Expected an error")
	}
	if !strings.Contains(l, "Expired") {
		t.Fatalf("Expected 'Expired' to be in the error")
	}
}

func TestBearerAuthJWTWithoutPermissions(t *testing.T) {
	bearerAuthJWT, _ := encodeValidBearerAuthJWT(nil)
	s, _, _ := setupBearerAuthTest(t, bearerAuthJWT, "-ERR 'Authorization Violation'")
	defer cleanupBearerTest(t, s)
}

func TestBearerAuthJWTWithPubSubAllowDenyPermissions(t *testing.T) {
	bearerAuthJWT, _ := encodeValidBearerAuthJWT(map[string]interface{}{
		"publish": map[string]interface{}{
			"allow": []string{"foo", "bar"},
			"deny":  []string{"baz"},
		},
		"subscribe": map[string]interface{}{
			"allow": []string{"foo", "bar"},
			"deny":  []string{"baz"},
		},
	})
	s, c, _ := setupBearerAuthTest(t, bearerAuthJWT, "+OK")
	defer cleanupBearerTest(t, s)

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

func TestBearerAuthJWTWithResponsePermissions(t *testing.T) {
	bearerAuthJWT, _ := encodeValidBearerAuthJWT(map[string]interface{}{
		"responses": map[string]interface{}{
			"max": 22,
			"ttl": 100 * time.Millisecond,
		},
	})
	s, c, _ := setupBearerAuthTest(t, bearerAuthJWT, "+OK")
	defer cleanupBearerTest(t, s)

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
	if c.perms.resp.MaxMsgs != 22 {
		t.Fatalf("Expected client perms for response permissions MaxMsgs to be same as jwt: %d vs %d",
			c.perms.resp.MaxMsgs, 22)
	}
	if c.perms.resp.Expires != 100*time.Millisecond {
		t.Fatalf("Expected client perms for response permissions Expires to be same as jwt: %v vs %v",
			c.perms.resp.Expires, 100*time.Millisecond)
	}
}

func TestBearerAuthJWTWithDefaultResponsePermissions(t *testing.T) {
	bearerAuthJWT, _ := encodeValidBearerAuthJWT(map[string]interface{}{})
	s, c, _ := setupBearerAuthTest(t, bearerAuthJWT, "+OK")
	defer cleanupBearerTest(t, s)

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
	if c.perms.resp.MaxMsgs != DEFAULT_ALLOW_RESPONSE_MAX_MSGS {
		t.Fatalf("Expected client perms for response permissions MaxMsgs to be default %v, got %v",
			DEFAULT_ALLOW_RESPONSE_MAX_MSGS, c.perms.resp.MaxMsgs)
	}
	if c.perms.resp.Expires != DEFAULT_ALLOW_RESPONSE_EXPIRATION {
		t.Fatalf("Expected client perms for response permissions Expires to be default %v, got %v",
			DEFAULT_ALLOW_RESPONSE_EXPIRATION, c.perms.resp.Expires)
	}
}

func TestBearerAuthJWTWithNegativeValueResponsePermissions(t *testing.T) {
	bearerAuthJWT, _ := encodeValidBearerAuthJWT(map[string]interface{}{
		"responses": map[string]interface{}{
			"max": -1,
			"ttl": -1 * time.Second,
		},
	})
	s, c, _ := setupBearerAuthTest(t, bearerAuthJWT, "+OK")
	defer cleanupBearerTest(t, s)

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
	if c.perms.resp.MaxMsgs != -1 {
		t.Fatalf("Expected client perms for response permissions MaxMsgs to be %v, got %v",
			-1, c.perms.resp.MaxMsgs)
	}
	if c.perms.resp.Expires != -1*time.Second {
		t.Fatalf("Expected client perms for response permissions Expires to be %v, got %v",
			-1*time.Second, c.perms.resp.Expires)
	}
}
