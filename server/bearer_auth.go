package server

import (
	"crypto/rsa"
	"encoding/json"
	"fmt"
	"golang.org/x/crypto/ssh"
	"os"
	"strings"
	"time"

	"github.com/dgrijalva/jwt-go"
	natsjwt "github.com/nats-io/jwt/v2"
)

type BearerAuth struct {
	server *Server
	jwks   map[string]*rsa.PublicKey
}

func bearerAuthFactory(s *Server) (*BearerAuth, error) {
	auth := &BearerAuth{
		server: s,
		jwks:   map[string]*rsa.PublicKey{},
	}
	err := auth.readPublicKey()
	if err != nil {
		return nil, fmt.Errorf("failed to read JWT_SIGNER_PUBLIC_KEY from environment")
	}
	return auth, nil
}

func (bearer *BearerAuth) readPublicKey() error {
	jwtPublicKeyPEM := strings.Replace(os.Getenv("JWT_SIGNER_PUBLIC_KEY"), `\n`, "\n", -1)
	publicKey, err := jwt.ParseRSAPublicKeyFromPEM([]byte(jwtPublicKeyPEM))
	if err != nil {
		return err
	}

	sshPublicKey, err := ssh.NewPublicKey(publicKey)
	if err != nil {
		bearer.server.Warnf("failed to resolve JWT public key fingerprint; %s", err.Error())
	}

	fingerprint := ssh.FingerprintLegacyMD5(sshPublicKey)
	bearer.jwks[fingerprint] = publicKey

	return nil
}

func (bearer *BearerAuth) Check(c ClientAuthentication) bool {
	bearerToken := c.GetOpts().JWT
	jwtToken, err := jwt.Parse(bearerToken, func(_jwtToken *jwt.Token) (interface{}, error) {
		if _, ok := _jwtToken.Method.(*jwt.SigningMethodRSA); !ok { // FIXME-- also support ed25519 spec
			return nil, fmt.Errorf("failed to parse bearer authorization; unexpected signing alg: %s", _jwtToken.Method.Alg())
		}

		var publicKey *rsa.PublicKey

		var kid *string
		if kidhdr, ok := _jwtToken.Header["kid"].(string); ok {
			kid = &kidhdr
		}

		if kid != nil {
			publicKey = bearer.jwks[*kid]
		}

		if publicKey == nil {
			return nil, fmt.Errorf("failed to resolve verifier for kid: %s", *kid)
		}

		return publicKey, nil
	})

	if err != nil {
		bearer.server.Tracef(fmt.Sprintf("failed to parse bearer authorization; %s", err.Error()))
		return false
	}

	bearer.server.Debugf(fmt.Sprintf("parsed bearer authorization: %s\n; client authentication: %s", jwtToken.Claims, c))
	claims, claimsOk := jwtToken.Claims.(jwt.MapClaims)
	if !claimsOk {
		bearer.server.Warnf(fmt.Sprintf("no claims present in verified JWT; %s", err.Error()))
		return false
	}

	permissions := &Permissions{}
	if permissionsClaim, permissionsClaimOk := claims["permissions"].(map[string]interface{}); permissionsClaimOk {
		if _, pubOk := permissionsClaim["publish"]; !pubOk {
			permissionsClaim["publish"] = map[string]interface{}{
				"allow": []string{},
				"deny":  []string{},
			}
		}
		if _, subOk := permissionsClaim["subscribe"]; !subOk {
			permissionsClaim["subscribe"] = map[string]interface{}{
				"allow": []string{},
				"deny":  []string{},
			}
		}
		if _, respOk := permissionsClaim["responses"]; !respOk {
			permissionsClaim["responses"] = map[string]interface{}{
				"max": DEFAULT_ALLOW_RESPONSE_MAX_MSGS,
				"ttl": DEFAULT_ALLOW_RESPONSE_EXPIRATION,
			}
		}
		permissionsRaw, _ := json.Marshal(permissionsClaim)
		json.Unmarshal(permissionsRaw, &permissions) // HACK
	} else {
		bearer.server.Warnf(fmt.Sprintf("no permissions claim present in verified JWT; %s", bearerToken))
		return false
	}

	bearer.server.Tracef("registering ephemeral user with permissions: %s", permissions)
	c.RegisterUser(&User{
		Permissions: permissions,
	})

	if cl, clOk := c.(*client); clOk {
		var exp int64
		switch expClaim := claims["exp"].(type) {
		case float64:
			exp = int64(expClaim)
		case json.Number:
			exp, _ = expClaim.Int64()
		default:
			bearer.server.Tracef("failed to parse bearer authorization expiration")
			return false
		}

		now := time.Now().Unix()
		if now >= exp {
			return false
		}

		bearer.server.Tracef("enforcing authorized expiration: %v", exp)
		cl.setExpiration(&natsjwt.ClaimsData{
			Expires: exp,
		}, 0)
	}
	return true
}
