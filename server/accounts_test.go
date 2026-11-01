// Copyright 2018-2026 The NATS Authors
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
	"fmt"
	"io"
	"net"
	"net/http"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nats-io/jwt/v2"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nkeys"
)

func simpleAccountServer(t *testing.T) (*Server, *Account, *Account) {
	opts := defaultServerOptions
	s := New(&opts)

	// Now create two accounts.
	f, err := s.RegisterAccount("$foo")
	if err != nil {
		t.Fatalf("Error creating account 'foo': %v", err)
	}
	b, err := s.RegisterAccount("$bar")
	if err != nil {
		t.Fatalf("Error creating account 'bar': %v", err)
	}
	return s, f, b
}

func TestRegisterDuplicateAccounts(t *testing.T) {
	s, _, _ := simpleAccountServer(t)
	if _, err := s.RegisterAccount("$foo"); err == nil {
		t.Fatal("Expected an error registering 'foo' twice")
	}
}

func TestAccountIsolation(t *testing.T) {
	s, fooAcc, barAcc := simpleAccountServer(t)
	cfoo, crFoo, _ := newClientForServer(s)
	defer cfoo.close()
	if err := cfoo.registerWithAccount(fooAcc); err != nil {
		t.Fatalf("Error register client with 'foo' account: %v", err)
	}
	cbar, crBar, _ := newClientForServer(s)
	defer cbar.close()
	if err := cbar.registerWithAccount(barAcc); err != nil {
		t.Fatalf("Error register client with 'bar' account: %v", err)
	}

	// Make sure they are different accounts/sl.
	if cfoo.acc == cbar.acc {
		t.Fatalf("Error, accounts the same for both clients")
	}

	// Now do quick test that makes sure messages do not cross over.
	// setup bar as a foo subscriber.
	cbar.parseAsync("SUB foo 1\r\nPING\r\nPING\r\n")
	l, err := crBar.ReadString('\n')
	if err != nil {
		t.Fatalf("Error for client 'bar' from server: %v", err)
	}
	if !strings.HasPrefix(l, "PONG\r\n") {
		t.Fatalf("PONG response incorrect: %q", l)
	}

	cfoo.parseAsync("SUB foo 1\r\nPUB foo 5\r\nhello\r\nPING\r\n")
	l, err = crFoo.ReadString('\n')
	if err != nil {
		t.Fatalf("Error for client 'foo' from server: %v", err)
	}

	matches := msgPat.FindAllStringSubmatch(l, -1)[0]
	if matches[SUB_INDEX] != "foo" {
		t.Fatalf("Did not get correct subject: '%s'\n", matches[SUB_INDEX])
	}
	if matches[SID_INDEX] != "1" {
		t.Fatalf("Did not get correct sid: '%s'", matches[SID_INDEX])
	}
	checkPayload(crFoo, []byte("hello\r\n"), t)

	// Now make sure nothing shows up on bar.
	l, err = crBar.ReadString('\n')
	if err != nil {
		t.Fatalf("Error for client 'bar' from server: %v", err)
	}
	if !strings.HasPrefix(l, "PONG\r\n") {
		t.Fatalf("PONG response incorrect: %q", l)
	}
}

func TestAccountIsolationExportImport(t *testing.T) {
	checkIsolation := func(t *testing.T, pubSubj string, ncExp, ncImp *nats.Conn) {
		// We keep track of 2 subjects.
		// One subject (pubSubj) is based off the stream import.
		// The other subject "fizz" is not imported and should be isolated.

		gotSubjs := map[string]int{
			pubSubj: 0,
			"fizz":  0,
		}
		count := int32(0)
		ch := make(chan struct{}, 1)
		if _, err := ncImp.Subscribe(">", func(m *nats.Msg) {
			gotSubjs[m.Subject] += 1
			if n := atomic.AddInt32(&count, 1); n == 3 {
				ch <- struct{}{}
			}
		}); err != nil {
			t.Fatalf("Error on subscribe: %v", err)
		}
		// Since both prod and cons use same server, flushing here will ensure
		// that the interest is registered and known at the time we publish.
		ncImp.Flush()

		if err := ncExp.Publish(pubSubj, []byte(fmt.Sprintf("ncExp pub %s", pubSubj))); err != nil {
			t.Fatal(err)
		}
		if err := ncImp.Publish(pubSubj, []byte(fmt.Sprintf("ncImp pub %s", pubSubj))); err != nil {
			t.Fatal(err)
		}

		if err := ncExp.Publish("fizz", []byte("ncExp pub fizz")); err != nil {
			t.Fatal(err)
		}
		if err := ncImp.Publish("fizz", []byte("ncImp pub fizz")); err != nil {
			t.Fatal(err)
		}

		wantSubjs := map[string]int{
			// Subscriber ncImp should receive publishes from ncExp and ncImp.
			pubSubj: 2,
			// Subscriber ncImp should only receive the publish from ncImp.
			"fizz": 1,
		}

		// Wait for at least the 3 expected messages
		select {
		case <-ch:
		case <-time.After(time.Second):
			t.Fatalf("Expected 3 messages, got %v", atomic.LoadInt32(&count))
		}
		// But now wait a bit to see if subscription receives more than expected.
		time.Sleep(50 * time.Millisecond)

		if got, want := len(gotSubjs), len(wantSubjs); got != want {
			t.Fatalf("unexpected subjs len, got=%d; want=%d", got, want)
		}

		for key, gotCnt := range gotSubjs {
			if wantCnt := wantSubjs[key]; gotCnt != wantCnt {
				t.Errorf("unexpected receive count for subject %q, got=%d, want=%d", key, gotCnt, wantCnt)
			}
		}
	}

	cases := []struct {
		name     string
		exp, imp string
		pubSubj  string
	}{
		{
			name: "export literal, import literal",
			exp:  "foo", imp: "foo",
			pubSubj: "foo",
		},
		{
			name: "export full wildcard, import literal",
			exp:  "foo.>", imp: "foo.bar",
			pubSubj: "foo.bar",
		},
		{
			name: "export full wildcard, import sublevel full wildcard",
			exp:  "foo.>", imp: "foo.bar.>",
			pubSubj: "foo.bar.whizz",
		},
		{
			name: "export full wildcard, import full wildcard",
			exp:  "foo.>", imp: "foo.>",
			pubSubj: "foo.bar",
		},
		{
			name: "export partial wildcard, import partial wildcard",
			exp:  "foo.*", imp: "foo.*",
			pubSubj: "foo.bar",
		},
		{
			name: "export mid partial wildcard, import mid partial wildcard",
			exp:  "foo.*.bar", imp: "foo.*.bar",
			pubSubj: "foo.whizz.bar",
		},
	}
	for _, c := range cases {
		t.Run(fmt.Sprintf("%s jwt", c.name), func(t *testing.T) {
			// Setup NATS server.
			s := opTrustBasicSetup()
			defer s.Shutdown()
			s.Start()
			if err := s.readyForConnections(5 * time.Second); err != nil {
				t.Fatal(err)
			}
			buildMemAccResolver(s)

			// Setup exporter account.
			accExpPair, accExpPub := createKey(t)
			accExpClaims := jwt.NewAccountClaims(accExpPub)
			if c.exp != "" {
				accExpClaims.Limits.WildcardExports = true
				accExpClaims.Exports.Add(&jwt.Export{
					Name:    fmt.Sprintf("%s-stream-export", c.exp),
					Subject: jwt.Subject(c.exp),
					Type:    jwt.Stream,
				})
			}
			accExpJWT, err := accExpClaims.Encode(oKp)
			require_NoError(t, err)
			addAccountToMemResolver(s, accExpPub, accExpJWT)

			// Setup importer account.
			accImpPair, accImpPub := createKey(t)
			accImpClaims := jwt.NewAccountClaims(accImpPub)
			if c.imp != "" {
				accImpClaims.Imports.Add(&jwt.Import{
					Name:    fmt.Sprintf("%s-stream-import", c.imp),
					Subject: jwt.Subject(c.imp),
					Account: accExpPub,
					Type:    jwt.Stream,
				})
			}
			accImpJWT, err := accImpClaims.Encode(oKp)
			require_NoError(t, err)
			addAccountToMemResolver(s, accImpPub, accImpJWT)

			// Connect with different accounts.
			ncExp := natsConnect(t, s.ClientURL(), createUserCreds(t, nil, accExpPair),
				nats.Name(fmt.Sprintf("nc-exporter-%s", c.exp)))
			defer ncExp.Close()
			ncImp := natsConnect(t, s.ClientURL(), createUserCreds(t, nil, accImpPair),
				nats.Name(fmt.Sprintf("nc-importer-%s", c.imp)))
			defer ncImp.Close()

			checkIsolation(t, c.pubSubj, ncExp, ncImp)
			if t.Failed() {
				t.Logf("exported=%q; imported=%q", c.exp, c.imp)
			}
		})

		t.Run(fmt.Sprintf("%s conf", c.name), func(t *testing.T) {
			// Setup NATS server.
			cf := createConfFile(t, []byte(fmt.Sprintf(`
				port: -1

				accounts: {
					accExp: {
						users: [{user: accExp, password: accExp}]
						exports: [{stream: %q}]
					}
					accImp: {
						users: [{user: accImp, password: accImp}]
						imports: [{stream: {account: accExp, subject: %q}}]
					}
				}
			`,
				c.exp, c.imp,
			)))
			s, _ := RunServerWithConfig(cf)
			defer s.Shutdown()

			// Connect with different accounts.
			ncExp := natsConnect(t, s.ClientURL(), nats.UserInfo("accExp", "accExp"),
				nats.Name(fmt.Sprintf("nc-exporter-%s", c.exp)))
			defer ncExp.Close()
			ncImp := natsConnect(t, s.ClientURL(), nats.UserInfo("accImp", "accImp"),
				nats.Name(fmt.Sprintf("nc-importer-%s", c.imp)))
			defer ncImp.Close()

			checkIsolation(t, c.pubSubj, ncExp, ncImp)
			if t.Failed() {
				t.Logf("exported=%q; imported=%q", c.exp, c.imp)
			}
		})
	}
}

func TestMultiAccountsIsolation(t *testing.T) {
	conf := createConfFile(t, []byte(`
	listen: 127.0.0.1:-1
	accounts: {
		PUBLIC: {
			users:[{user: public, password: public}]
			exports: [
			  { stream: orders.client.stream.> }
			  { stream: orders.client2.stream.> }
			]
		}
		CLIENT: {
			users:[{user: client, password: client}]
			imports: [
			  { stream: { account: PUBLIC, subject: orders.client.stream.> }}
			]
		}
		CLIENT2: {
			users:[{user: client2, password: client2}]
			imports: [
			  { stream: { account: PUBLIC, subject: orders.client2.stream.> }}
			]
		}
	}`))

	s, _ := RunServerWithConfig(conf)
	if config := s.JetStreamConfig(); config != nil {
		defer removeDir(t, config.StoreDir)
	}
	defer s.Shutdown()

	// Create a connection for CLIENT and subscribe on orders.>
	clientnc := natsConnect(t, s.ClientURL(), nats.UserInfo("client", "client"))
	defer clientnc.Close()

	clientsub := natsSubSync(t, clientnc, "orders.>")
	natsFlush(t, clientnc)

	// Now same for CLIENT2.
	client2nc := natsConnect(t, s.ClientURL(), nats.UserInfo("client2", "client2"))
	defer client2nc.Close()

	client2sub := natsSubSync(t, client2nc, "orders.>")
	natsFlush(t, client2nc)

	// Now create a connection for PUBLIC
	publicnc := natsConnect(t, s.ClientURL(), nats.UserInfo("public", "public"))
	defer publicnc.Close()
	// Publish on 'orders.client.stream.entry', so only CLIENT should receive it.
	natsPub(t, publicnc, "orders.client.stream.entry", []byte("test1"))

	// Verify that clientsub gets it.
	msg := natsNexMsg(t, clientsub, time.Second)
	require_Equal(t, string(msg.Data), "test1")

	// And also verify that client2sub does NOT get it.
	_, err := client2sub.NextMsg(100 * time.Microsecond)
	require_Error(t, err, nats.ErrTimeout)

	clientsub.Unsubscribe()
	natsFlush(t, clientnc)
	client2sub.Unsubscribe()
	natsFlush(t, client2nc)

	// Now have both accounts subscribe to "orders.*.stream.entry"
	clientsub = natsSubSync(t, clientnc, "orders.*.stream.entry")
	natsFlush(t, clientnc)

	client2sub = natsSubSync(t, client2nc, "orders.*.stream.entry")
	natsFlush(t, client2nc)

	// Using the PUBLIC account, publish on the "CLIENT" subject
	natsPub(t, publicnc, "orders.client.stream.entry", []byte("test2"))
	natsFlush(t, publicnc)

	msg = natsNexMsg(t, clientsub, time.Second)
	require_Equal(t, string(msg.Data), "test2")

	_, err = client2sub.NextMsg(100 * time.Microsecond)
	require_Error(t, err, nats.ErrTimeout)
}

func TestAccountFromOptions(t *testing.T) {
	opts := defaultServerOptions
	opts.Accounts = []*Account{NewAccount("foo"), NewAccount("bar")}
	s := New(&opts)
	defer s.Shutdown()

	ta := s.numReservedAccounts() + 2
	if la := s.numAccounts(); la != ta {
		t.Fatalf("Expected to have a server with %d active accounts, got %v", ta, la)
	}
	// Check that sl is filled in.
	fooAcc, _ := s.LookupAccount("foo")
	barAcc, _ := s.LookupAccount("bar")
	if fooAcc == nil || barAcc == nil {
		t.Fatalf("Error retrieving accounts for 'foo' and 'bar'")
	}
	if fooAcc.sl == nil || barAcc.sl == nil {
		t.Fatal("Expected Sublists to be filled in on Opts.Accounts")
	}
}

// Clients used to be able to ask that the account be forced to be new.
// This was for dynamic sandboxes for demo environments but was never really used.
// Make sure it always errors if set.
func TestNewAccountAndRequireNewAlwaysError(t *testing.T) {
	conf := createConfFile(t, []byte(`
		listen: 127.0.0.1:-1
		accounts: {
			A: { users: [ {user: ua, password: pa} ] },
			B: { users: [ {user: ub, password: pb} ] },
		}
	`))

	s, _ := RunServerWithConfig(conf)
	defer s.Shutdown()

	// Success case
	c, _, _ := newClientForServer(s)
	connectOp := "CONNECT {\"user\":\"ua\", \"pass\":\"pa\"}\r\n"
	err := c.parse([]byte(connectOp))
	require_NoError(t, err)
	c.close()

	// Simple cases, any setting of account or new_account always errors.
	// Even with proper auth.
	c, cr, _ := newClientForServer(s)
	connectOp = "CONNECT {\"user\":\"ua\", \"pass\":\"pa\", \"account\":\"ANY\"}\r\n"
	c.parseAsync(connectOp)
	l, _ := cr.ReadString('\n')
	if !strings.HasPrefix(l, "-ERR 'Authorization Violation'") {
		t.Fatalf("Expected an error, got %q", l)
	}
	c.close()

	// new_account with proper credentials.
	c, cr, _ = newClientForServer(s)
	connectOp = "CONNECT {\"user\":\"ua\", \"pass\":\"pa\", \"new_account\":true}\r\n"
	c.parseAsync(connectOp)
	l, _ = cr.ReadString('\n')
	if !strings.HasPrefix(l, "-ERR 'Authorization Violation'") {
		t.Fatalf("Expected an error, got %q", l)
	}
	c.close()

	// switch acccounts with proper credentials.
	c, cr, _ = newClientForServer(s)
	connectOp = "CONNECT {\"user\":\"ua\", \"pass\":\"pa\", \"account\":\"B\"}\r\n"
	c.parseAsync(connectOp)
	l, _ = cr.ReadString('\n')
	if !strings.HasPrefix(l, "-ERR 'Authorization Violation'") {
		t.Fatalf("Expected an error, got %q", l)
	}
	c.close()

	// Even if correct account designation, still make sure we error.
	c, cr, _ = newClientForServer(s)
	connectOp = "CONNECT {\"user\":\"ua\", \"pass\":\"pa\", \"account\":\"A\"}\r\n"
	c.parseAsync(connectOp)
	l, _ = cr.ReadString('\n')
	if !strings.HasPrefix(l, "-ERR 'Authorization Violation'") {
		t.Fatalf("Expected an error, got %q", l)
	}
	c.close()
}

func accountNameExists(name string, accounts []*Account) bool {
	for _, acc := range accounts {
		if strings.Compare(acc.Name, name) == 0 {
			return true
		}
	}
	return false
}

func TestAccountSimpleConfig(t *testing.T) {
	cfg1 := `
		accounts = [foo, bar]
	`

	confFileName := createConfFile(t, []byte(cfg1))
	opts, err := ProcessConfigFile(confFileName)
	if err != nil {
		t.Fatalf("Received an error processing config file: %v", err)
	}
	if la := len(opts.Accounts); la != 2 {
		t.Fatalf("Expected to see 2 accounts in opts, got %d", la)
	}
	if !accountNameExists("foo", opts.Accounts) {
		t.Fatal("Expected a 'foo' account")
	}
	if !accountNameExists("bar", opts.Accounts) {
		t.Fatal("Expected a 'bar' account")
	}

	cfg2 := `
		accounts = [foo, foo]
	`

	// Make sure double entries is an error.
	confFileName = createConfFile(t, []byte(cfg2))
	_, err = ProcessConfigFile(confFileName)
	if err == nil {
		t.Fatalf("Expected an error with double account entries")
	}
}

func TestAccountParseConfig(t *testing.T) {
	traceDest := "my.trace.dest"
	traceSampling := 50
	confFileName := createConfFile(t, []byte(fmt.Sprintf(`
    accounts {
      synadia {
        users = [
          {user: alice, password: foo}
          {user: bob, password: bar}
        ]
      }
      nats.io {
		msg_trace: {dest: %q, sampling: %d%%}
        users = [
          {user: derek, password: foo}
          {user: ivan, password: bar}
        ]
      }
    }
    `, traceDest, traceSampling)))
	opts, err := ProcessConfigFile(confFileName)
	if err != nil {
		t.Fatalf("Received an error processing config file: %v", err)
	}

	if la := len(opts.Accounts); la != 2 {
		t.Fatalf("Expected to see 2 accounts in opts, got %d", la)
	}

	if lu := len(opts.Users); lu != 4 {
		t.Fatalf("Expected 4 total Users, got %d", lu)
	}

	var natsAcc *Account
	for _, acc := range opts.Accounts {
		if acc.Name == "nats.io" {
			natsAcc = acc
			break
		}
	}
	if natsAcc == nil {
		t.Fatalf("Error retrieving account for 'nats.io'")
	}
	td, tds := natsAcc.getTraceDestAndSampling()
	require_Equal[string](t, td, traceDest)
	require_Equal[int](t, tds, traceSampling)

	for _, u := range opts.Users {
		if u.Username == "derek" {
			if u.Account != natsAcc {
				t.Fatalf("Expected to see the 'nats.io' account, but received %+v", u.Account)
			}
		}
	}
}

func TestAccountParseConfigDuplicateUsers(t *testing.T) {
	confFileName := createConfFile(t, []byte(`
    accounts {
      synadia {
        users = [
          {user: alice, password: foo}
          {user: bob, password: bar}
        ]
      }
      nats.io {
        users = [
          {user: alice, password: bar}
        ]
      }
    }
    `))
	_, err := ProcessConfigFile(confFileName)
	if err == nil {
		t.Fatalf("Expected an error with double user entries")
	}
}

func TestAccountParseConfigImportsExports(t *testing.T) {
	opts, err := ProcessConfigFile("./configs/accounts.conf")
	if err != nil {
		t.Fatal("parsing failed: ", err)
	}
	if la := len(opts.Accounts); la != 3 {
		t.Fatalf("Expected to see 3 accounts in opts, got %d", la)
	}
	if lu := len(opts.Nkeys); lu != 4 {
		t.Fatalf("Expected 4 total Nkey users, got %d", lu)
	}
	if lu := len(opts.Users); lu != 0 {
		t.Fatalf("Expected no Users, got %d", lu)
	}
	var natsAcc, synAcc *Account
	for _, acc := range opts.Accounts {
		if acc.Name == "nats.io" {
			natsAcc = acc
		} else if acc.Name == "synadia" {
			synAcc = acc
		}
	}
	if natsAcc == nil {
		t.Fatalf("Error retrieving account for 'nats.io'")
	}
	if natsAcc.Nkey != "AB5UKNPVHDWBP5WODG742274I3OGY5FM3CBIFCYI4OFEH7Y23GNZPXFE" {
		t.Fatalf("Expected nats account to have an nkey, got %q\n", natsAcc.Nkey)
	}
	// Check user assigned to the correct account.
	for _, nk := range opts.Nkeys {
		if nk.Nkey == "UBRYMDSRTC6AVJL6USKKS3FIOE466GMEU67PZDGOWYSYHWA7GSKO42VW" {
			if nk.Account != natsAcc {
				t.Fatalf("Expected user to be associated with natsAcc, got %q\n", nk.Account.Name)
			}
			break
		}
	}

	// Now check for the imports and exports of streams and services.
	if lis := len(natsAcc.imports.streams); lis != 2 {
		t.Fatalf("Expected 2 imported streams, got %d\n", lis)
	}
	for _, si := range natsAcc.imports.streams {
		if si.from == "public.synadia" {
			require_True(t, si.atrc)
		} else {
			require_False(t, si.atrc)
		}
	}
	if lis := len(natsAcc.imports.services); lis != 1 {
		t.Fatalf("Expected 1 imported service, got %d\n", lis)
	}
	if les := len(natsAcc.exports.services); les != 4 {
		t.Fatalf("Expected 4 exported services, got %d\n", les)
	}
	if les := len(natsAcc.exports.streams); les != 0 {
		t.Fatalf("Expected no exported streams, got %d\n", les)
	}

	ea := natsAcc.exports.services["nats.time"]
	if ea == nil {
		t.Fatalf("Expected to get a non-nil exportAuth for service")
	}
	if ea.respType != Streamed {
		t.Fatalf("Expected to get a Streamed response type, got %q", ea.respType)
	}
	require_True(t, ea.atrc)
	ea = natsAcc.exports.services["nats.photo"]
	if ea == nil {
		t.Fatalf("Expected to get a non-nil exportAuth for service")
	}
	if ea.respType != Chunked {
		t.Fatalf("Expected to get a Chunked response type, got %q", ea.respType)
	}
	require_False(t, ea.atrc)
	ea = natsAcc.exports.services["nats.add"]
	if ea == nil {
		t.Fatalf("Expected to get a non-nil exportAuth for service")
	}
	if ea.respType != Singleton {
		t.Fatalf("Expected to get a Singleton response type, got %q", ea.respType)
	}
	require_True(t, ea.atrc)

	if synAcc == nil {
		t.Fatalf("Error retrieving account for 'synadia'")
	}

	if lis := len(synAcc.imports.streams); lis != 0 {
		t.Fatalf("Expected no imported streams, got %d\n", lis)
	}
	if lis := len(synAcc.imports.services); lis != 1 {
		t.Fatalf("Expected 1 imported service, got %d\n", lis)
	}
	if les := len(synAcc.exports.services); les != 2 {
		t.Fatalf("Expected 2 exported service, got %d\n", les)
	}
	if les := len(synAcc.exports.streams); les != 2 {
		t.Fatalf("Expected 2 exported streams, got %d\n", les)
	}
}

func TestImportExportConfigFailures(t *testing.T) {
	// Import from unknow account
	cf := createConfFile(t, []byte(`
    accounts {
      nats.io {
        imports = [{stream: {account: "synadia", subject:"foo"}}]
      }
    }
    `))
	if _, err := ProcessConfigFile(cf); err == nil {
		t.Fatalf("Expected an error with import from unknown account")
	}
	// Import a service with no account.
	cf = createConfFile(t, []byte(`
    accounts {
      nats.io {
        imports = [{service: subject:"foo.*"}]
      }
    }
    `))
	if _, err := ProcessConfigFile(cf); err == nil {
		t.Fatalf("Expected an error with import of a service with no account")
	}
	// Import a service with a wildcard subject.
	cf = createConfFile(t, []byte(`
    accounts {
      nats.io {
        imports = [{service: {account: "nats.io", subject:"foo.*"}]
      }
    }
    `))
	if _, err := ProcessConfigFile(cf); err == nil {
		t.Fatalf("Expected an error with import of a service with wildcard subject")
	}
	// Export with unknown keyword.
	cf = createConfFile(t, []byte(`
    accounts {
      nats.io {
        exports = [{service: "foo.*", wat:true}]
      }
    }
    `))
	if _, err := ProcessConfigFile(cf); err == nil {
		t.Fatalf("Expected an error with export with unknown keyword")
	}
	// Import with unknown keyword.
	cf = createConfFile(t, []byte(`
    accounts {
      nats.io {
        imports = [{stream: {account: nats.io, subject: "foo.*"}, wat:true}]
      }
    }
    `))
	if _, err := ProcessConfigFile(cf); err == nil {
		t.Fatalf("Expected an error with import with unknown keyword")
	}
	// Export with an account.
	cf = createConfFile(t, []byte(`
    accounts {
      nats.io {
        exports = [{service: {account: nats.io, subject:"foo.*"}}]
      }
    }
    `))
	if _, err := ProcessConfigFile(cf); err == nil {
		t.Fatalf("Expected an error with export with account")
	}
}

func TestImportAuthorized(t *testing.T) {
	_, foo, bar := simpleAccountServer(t)

	checkBool(foo.checkStreamImportAuthorized(bar, "foo", nil), false, t)
	checkBool(foo.checkStreamImportAuthorized(bar, "*", nil), false, t)
	checkBool(foo.checkStreamImportAuthorized(bar, ">", nil), false, t)
	checkBool(foo.checkStreamImportAuthorized(bar, "foo.*", nil), false, t)
	checkBool(foo.checkStreamImportAuthorized(bar, "foo.>", nil), false, t)

	foo.AddStreamExport("foo", IsPublicExport)
	checkBool(foo.checkStreamImportAuthorized(bar, "foo", nil), true, t)
	checkBool(foo.checkStreamImportAuthorized(bar, "bar", nil), false, t)
	checkBool(foo.checkStreamImportAuthorized(bar, "*", nil), false, t)

	foo.AddStreamExport("*", []*Account{bar})
	checkBool(foo.checkStreamImportAuthorized(bar, "foo", nil), true, t)
	checkBool(foo.checkStreamImportAuthorized(bar, "bar", nil), true, t)
	checkBool(foo.checkStreamImportAuthorized(bar, "baz", nil), true, t)
	checkBool(foo.checkStreamImportAuthorized(bar, "foo.bar", nil), false, t)
	checkBool(foo.checkStreamImportAuthorized(bar, ">", nil), false, t)
	checkBool(foo.checkStreamImportAuthorized(bar, "*", nil), true, t)
	checkBool(foo.checkStreamImportAuthorized(bar, "foo.*", nil), false, t)
	checkBool(foo.checkStreamImportAuthorized(bar, "*.*", nil), false, t)
	checkBool(foo.checkStreamImportAuthorized(bar, "*.>", nil), false, t)

	// Reset and test '>' public export
	_, foo, bar = simpleAccountServer(t)
	foo.AddStreamExport(">", nil)
	// Everything should work.
	checkBool(foo.checkStreamImportAuthorized(bar, "foo", nil), true, t)
	checkBool(foo.checkStreamImportAuthorized(bar, "bar", nil), true, t)
	checkBool(foo.checkStreamImportAuthorized(bar, "baz", nil), true, t)
	checkBool(foo.checkStreamImportAuthorized(bar, "foo.bar", nil), true, t)
	checkBool(foo.checkStreamImportAuthorized(bar, ">", nil), true, t)
	checkBool(foo.checkStreamImportAuthorized(bar, "*", nil), true, t)
	checkBool(foo.checkStreamImportAuthorized(bar, "foo.*", nil), true, t)
	checkBool(foo.checkStreamImportAuthorized(bar, "*.*", nil), true, t)
	checkBool(foo.checkStreamImportAuthorized(bar, "*.>", nil), true, t)

	_, foo, bar = simpleAccountServer(t)
	foo.addStreamExportWithAccountPos("foo.*", []*Account{}, 2)
	foo.addStreamExportWithAccountPos("bar.*.foo", []*Account{}, 2)
	if err := foo.addStreamExportWithAccountPos("baz.*.>", []*Account{}, 3); err == nil {
		t.Fatal("expected error")
	}
	checkBool(foo.checkStreamImportAuthorized(bar, fmt.Sprintf("foo.%s", bar.Name), nil), true, t)
	checkBool(foo.checkStreamImportAuthorized(bar, fmt.Sprintf("bar.%s.foo", bar.Name), nil), true, t)
	checkBool(foo.checkStreamImportAuthorized(bar, fmt.Sprintf("baz.foo.%s", bar.Name), nil), false, t)
	checkBool(foo.checkStreamImportAuthorized(bar, "foo.X", nil), false, t)
	checkBool(foo.checkStreamImportAuthorized(bar, "bar.X.foo", nil), false, t)
	checkBool(foo.checkStreamImportAuthorized(bar, "baz.foo.X", nil), false, t)

	foo.addServiceExportWithAccountPos("a.*", []*Account{}, 2)
	foo.addServiceExportWithAccountPos("b.*.a", []*Account{}, 2)
	if err := foo.addServiceExportWithAccountPos("c.*.>", []*Account{}, 3); err == nil {
		t.Fatal("expected error")
	}
	checkBool(foo.checkServiceImportAuthorized(bar, fmt.Sprintf("a.%s", bar.Name), nil), true, t)
	checkBool(foo.checkServiceImportAuthorized(bar, fmt.Sprintf("b.%s.a", bar.Name), nil), true, t)
	checkBool(foo.checkServiceImportAuthorized(bar, fmt.Sprintf("c.a.%s", bar.Name), nil), false, t)
	checkBool(foo.checkServiceImportAuthorized(bar, "a.X", nil), false, t)
	checkBool(foo.checkServiceImportAuthorized(bar, "b.X.a", nil), false, t)
	checkBool(foo.checkServiceImportAuthorized(bar, "c.a.X", nil), false, t)

	// Reset and test pwc and fwc
	s, foo, bar := simpleAccountServer(t)
	foo.AddStreamExport("foo.*.baz.>", []*Account{bar})
	checkBool(foo.checkStreamImportAuthorized(bar, "foo.bar.baz.1", nil), true, t)
	checkBool(foo.checkStreamImportAuthorized(bar, "foo.bar.baz.*", nil), true, t)
	checkBool(foo.checkStreamImportAuthorized(bar, "foo.*.baz.1.1", nil), true, t)
	checkBool(foo.checkStreamImportAuthorized(bar, "foo.22.baz.22", nil), true, t)
	checkBool(foo.checkStreamImportAuthorized(bar, "foo.bar.baz", nil), false, t)
	checkBool(foo.checkStreamImportAuthorized(bar, "", nil), false, t)
	checkBool(foo.checkStreamImportAuthorized(bar, "foo.bar.*.*", nil), false, t)

	// Make sure we match the account as well

	fb, _ := s.RegisterAccount("foobar")
	bz, _ := s.RegisterAccount("baz")

	checkBool(foo.checkStreamImportAuthorized(fb, "foo.bar.baz.1", nil), false, t)
	checkBool(foo.checkStreamImportAuthorized(bz, "foo.bar.baz.1", nil), false, t)
}

func TestSimpleMapping(t *testing.T) {
	s, fooAcc, barAcc := simpleAccountServer(t)
	defer s.Shutdown()

	cfoo, _, _ := newClientForServer(s)
	defer cfoo.close()

	if err := cfoo.registerWithAccount(fooAcc); err != nil {
		t.Fatalf("Error registering client with 'foo' account: %v", err)
	}
	cbar, crBar, _ := newClientForServer(s)
	defer cbar.close()

	if err := cbar.registerWithAccount(barAcc); err != nil {
		t.Fatalf("Error registering client with 'bar' account: %v", err)
	}

	// Test first that trying to import with no matching export permission returns an error.
	if err := cbar.acc.AddStreamImport(fooAcc, "foo", "import"); err != ErrStreamImportAuthorization {
		t.Fatalf("Expected error of ErrAccountImportAuthorization but got %v", err)
	}

	// Now map the subject space between foo and bar.
	// Need to do export first.
	if err := cfoo.acc.AddStreamExport("foo", nil); err != nil { // Public with no accounts defined.
		t.Fatalf("Error adding account export to client foo: %v", err)
	}
	if err := cbar.acc.AddStreamImport(fooAcc, "foo", "import"); err != nil {
		t.Fatalf("Error adding account import to client bar: %v", err)
	}

	// Normal and Queue Subscription on bar client.
	if err := cbar.parse([]byte("SUB import.foo 1\r\nSUB import.foo bar 2\r\n")); err != nil {
		t.Fatalf("Error for client 'bar' from server: %v", err)
	}

	// Now publish our message.
	cfoo.parseAsync("PUB foo 5\r\nhello\r\n")

	checkMsg := func(l, sid string) {
		t.Helper()
		mraw := msgPat.FindAllStringSubmatch(l, -1)
		if len(mraw) == 0 {
			t.Fatalf("No message received")
		}
		matches := mraw[0]
		if matches[SUB_INDEX] != "import.foo" {
			t.Fatalf("Did not get correct subject: wanted %q, got %q", "import.foo", matches[SUB_INDEX])
		}
		if matches[SID_INDEX] != sid {
			t.Fatalf("Did not get correct sid: '%s'", matches[SID_INDEX])
		}
	}

	// Now check we got the message from normal subscription.
	l, err := crBar.ReadString('\n')
	if err != nil {
		t.Fatalf("Error reading from client 'bar': %v", err)
	}
	checkMsg(l, "1")
	checkPayload(crBar, []byte("hello\r\n"), t)

	l, err = crBar.ReadString('\n')
	if err != nil {
		t.Fatalf("Error reading from client 'bar': %v", err)
	}
	checkMsg(l, "2")
	checkPayload(crBar, []byte("hello\r\n"), t)

	// We should have 2 subscriptions in both. Normal and Queue Subscriber
	// for barAcc which are local, and 2 that are shadowed in fooAcc.
	// Now make sure that when we unsubscribe we clean up properly for both.
	if bslc := barAcc.sl.Count(); bslc != 2 {
		t.Fatalf("Expected 2 normal subscriptions on barAcc, got %d", bslc)
	}
	if fslc := fooAcc.sl.Count(); fslc != 2 {
		t.Fatalf("Expected 2 shadowed subscriptions on fooAcc, got %d", fslc)
	}

	// Now unsubscribe.
	if err := cbar.parse([]byte("UNSUB 1\r\nUNSUB 2\r\n")); err != nil {
		t.Fatalf("Error for client 'bar' from server: %v", err)
	}

	// We should have zero on both.
	if bslc := barAcc.sl.Count(); bslc != 0 {
		t.Fatalf("Expected no normal subscriptions on barAcc, got %d", bslc)
	}
	if fslc := fooAcc.sl.Count(); fslc != 0 {
		t.Fatalf("Expected no shadowed subscriptions on fooAcc, got %d", fslc)
	}
}

// https://github.com/nats-io/nats-server/issues/1159
func TestStreamImportLengthBug(t *testing.T) {
	s, fooAcc, barAcc := simpleAccountServer(t)
	defer s.Shutdown()

	cfoo, _, _ := newClientForServer(s)
	defer cfoo.close()

	if err := cfoo.registerWithAccount(fooAcc); err != nil {
		t.Fatalf("Error registering client with 'foo' account: %v", err)
	}
	cbar, _, _ := newClientForServer(s)
	defer cbar.close()

	if err := cbar.registerWithAccount(barAcc); err != nil {
		t.Fatalf("Error registering client with 'bar' account: %v", err)
	}

	if err := cfoo.acc.AddStreamExport("client.>", nil); err != nil {
		t.Fatalf("Error adding account export to client foo: %v", err)
	}
	if err := cbar.acc.AddStreamImport(fooAcc, "client.>", "events.>"); err == nil {
		t.Fatalf("Expected an error when using a stream import prefix with a wildcard")
	}

	if err := cbar.acc.AddStreamImport(fooAcc, "client.>", "events"); err != nil {
		t.Fatalf("Error adding account import to client bar: %v", err)
	}

	if err := cbar.parse([]byte("SUB events.> 1\r\n")); err != nil {
		t.Fatalf("Error for client 'bar' from server: %v", err)
	}

	// Also make sure that we will get an error from a config version.
	// JWT will be updated separately.
	cf := createConfFile(t, []byte(`
	accounts {
	  foo {
	    exports = [{stream: "client.>"}]
	  }
	  bar {
	    imports = [{stream: {account: "foo", subject:"client.>"}, prefix:"events.>"}]
	  }
	}
	`))
	if _, err := ProcessConfigFile(cf); err == nil {
		t.Fatalf("Expected an error with import with wildcard prefix")
	}
}

func TestStreamImportSubscribeDenyUsesMappedSubject(t *testing.T) {
	cf := createConfFile(t, []byte(`
		listen: 127.0.0.1:-1
		accounts {
			exporter {
				users: [{user: publisher, password: pass}]
				exports: [{stream: "foo.>"}]
			}
			importer {
				users: [{
					user: subscriber
					password: pass
					permissions: {
						subscribe: {
							allow: ["import.foo.>"]
							deny: ["import.foo.secret"]
						}
					}
				}]
				imports: [{
					stream: {account: exporter, subject: "foo.>"}
					prefix: "import"
				}]
			}
		}
	`))
	s, _ := RunServerWithConfig(cf)
	defer s.Shutdown()

	publisher := natsConnect(t, s.ClientURL(), nats.UserInfo("publisher", "pass"))
	defer publisher.Close()
	subscriber := natsConnect(t, s.ClientURL(), nats.UserInfo("subscriber", "pass"))
	defer subscriber.Close()

	plain := natsSubSync(t, subscriber, "import.foo.>")
	queue := natsQueueSubSync(t, subscriber, "import.foo.>", "workers")
	natsFlush(t, subscriber)

	natsPub(t, publisher, "foo.secret", []byte("blocked"))
	natsPub(t, publisher, "foo.public", []byte("allowed"))
	natsFlush(t, publisher)

	for _, sub := range []*nats.Subscription{plain, queue} {
		msg := natsNexMsg(t, sub, time.Second)
		require_Equal(t, msg.Subject, "import.foo.public")
		require_Equal(t, string(msg.Data), "allowed")
	}
}

func TestShadowSubsCleanupOnClientClose(t *testing.T) {
	s, fooAcc, barAcc := simpleAccountServer(t)
	defer s.Shutdown()

	// Now map the subject space between foo and bar.
	// Need to do export first.
	if err := fooAcc.AddStreamExport("foo", nil); err != nil { // Public with no accounts defined.
		t.Fatalf("Error adding account export to client foo: %v", err)
	}
	if err := barAcc.AddStreamImport(fooAcc, "foo", "import"); err != nil {
		t.Fatalf("Error adding account import to client bar: %v", err)
	}

	cbar, _, _ := newClientForServer(s)
	defer cbar.close()

	if err := cbar.registerWithAccount(barAcc); err != nil {
		t.Fatalf("Error registering client with 'bar' account: %v", err)
	}

	// Normal and Queue Subscription on bar client.
	if err := cbar.parse([]byte("SUB import.foo 1\r\nSUB import.foo bar 2\r\n")); err != nil {
		t.Fatalf("Error for client 'bar' from server: %v", err)
	}

	if fslc := fooAcc.sl.Count(); fslc != 2 {
		t.Fatalf("Expected 2 shadowed subscriptions on fooAcc, got %d", fslc)
	}

	// Now close cbar and make sure we remove shadows.
	cbar.closeConnection(ClientClosed)

	checkFor(t, time.Second, 10*time.Millisecond, func() error {
		if fslc := fooAcc.sl.Count(); fslc != 0 {
			return fmt.Errorf("Number of shadow subscriptions is %d", fslc)
		}
		return nil
	})
}

func TestNoPrefixWildcardMapping(t *testing.T) {
	s, fooAcc, barAcc := simpleAccountServer(t)
	defer s.Shutdown()

	cfoo, _, _ := newClientForServer(s)
	defer cfoo.close()

	if err := cfoo.registerWithAccount(fooAcc); err != nil {
		t.Fatalf("Error registering client with 'foo' account: %v", err)
	}
	cbar, crBar, _ := newClientForServer(s)
	defer cbar.close()

	if err := cbar.registerWithAccount(barAcc); err != nil {
		t.Fatalf("Error registering client with 'bar' account: %v", err)
	}

	if err := cfoo.acc.AddStreamExport(">", []*Account{barAcc}); err != nil {
		t.Fatalf("Error adding stream export to client foo: %v", err)
	}
	if err := cbar.acc.AddStreamImport(fooAcc, "*", ""); err != nil {
		t.Fatalf("Error adding stream import to client bar: %v", err)
	}

	// Normal Subscription on bar client for literal "foo".
	cbar.parseAsync("SUB foo 1\r\nPING\r\n")
	_, err := crBar.ReadString('\n') // Make sure subscriptions were processed.
	if err != nil {
		t.Fatalf("Error for client 'bar' from server: %v", err)
	}

	// Now publish our message.
	cfoo.parseAsync("PUB foo 5\r\nhello\r\n")

	// Now check we got the message from normal subscription.
	l, err := crBar.ReadString('\n')
	if err != nil {
		t.Fatalf("Error reading from client 'bar': %v", err)
	}
	mraw := msgPat.FindAllStringSubmatch(l, -1)
	if len(mraw) == 0 {
		t.Fatalf("No message received")
	}
	matches := mraw[0]
	if matches[SUB_INDEX] != "foo" {
		t.Fatalf("Did not get correct subject: '%s'", matches[SUB_INDEX])
	}
	if matches[SID_INDEX] != "1" {
		t.Fatalf("Did not get correct sid: '%s'", matches[SID_INDEX])
	}
	checkPayload(crBar, []byte("hello\r\n"), t)
}

func TestPrefixWildcardMapping(t *testing.T) {
	s, fooAcc, barAcc := simpleAccountServer(t)
	defer s.Shutdown()

	cfoo, _, _ := newClientForServer(s)
	defer cfoo.close()

	if err := cfoo.registerWithAccount(fooAcc); err != nil {
		t.Fatalf("Error registering client with 'foo' account: %v", err)
	}
	cbar, crBar, _ := newClientForServer(s)
	defer cbar.close()

	if err := cbar.registerWithAccount(barAcc); err != nil {
		t.Fatalf("Error registering client with 'bar' account: %v", err)
	}

	if err := cfoo.acc.AddStreamExport(">", []*Account{barAcc}); err != nil {
		t.Fatalf("Error adding stream export to client foo: %v", err)
	}
	// Checking that trailing '.' is accepted, tested that it is auto added above.
	if err := cbar.acc.AddStreamImport(fooAcc, "*", "pub.imports."); err != nil {
		t.Fatalf("Error adding stream import to client bar: %v", err)
	}

	// Normal Subscription on bar client for wildcard.
	cbar.parseAsync("SUB pub.imports.* 1\r\nPING\r\n")
	_, err := crBar.ReadString('\n') // Make sure subscriptions were processed.
	if err != nil {
		t.Fatalf("Error for client 'bar' from server: %v", err)
	}

	// Now publish our message.
	cfoo.parseAsync("PUB foo 5\r\nhello\r\n")

	// Now check we got the messages from wildcard subscription.
	l, err := crBar.ReadString('\n')
	if err != nil {
		t.Fatalf("Error reading from client 'bar': %v", err)
	}
	mraw := msgPat.FindAllStringSubmatch(l, -1)
	if len(mraw) == 0 {
		t.Fatalf("No message received")
	}
	matches := mraw[0]
	if matches[SUB_INDEX] != "pub.imports.foo" {
		t.Fatalf("Did not get correct subject: '%s'", matches[SUB_INDEX])
	}
	if matches[SID_INDEX] != "1" {
		t.Fatalf("Did not get correct sid: '%s'", matches[SID_INDEX])
	}
	checkPayload(crBar, []byte("hello\r\n"), t)
}

func TestPrefixWildcardMappingWithLiteralSub(t *testing.T) {
	s, fooAcc, barAcc := simpleAccountServer(t)
	defer s.Shutdown()

	cfoo, _, _ := newClientForServer(s)
	defer cfoo.close()

	if err := cfoo.registerWithAccount(fooAcc); err != nil {
		t.Fatalf("Error registering client with 'foo' account: %v", err)
	}
	cbar, crBar, _ := newClientForServer(s)
	defer cbar.close()

	if err := cbar.registerWithAccount(barAcc); err != nil {
		t.Fatalf("Error registering client with 'bar' account: %v", err)
	}

	if err := fooAcc.AddStreamExport(">", []*Account{barAcc}); err != nil {
		t.Fatalf("Error adding stream export to client foo: %v", err)
	}
	if err := barAcc.AddStreamImport(fooAcc, "*", "pub.imports."); err != nil {
		t.Fatalf("Error adding stream import to client bar: %v", err)
	}

	// Normal Subscription on bar client for wildcard.
	cbar.parseAsync("SUB pub.imports.foo 1\r\nPING\r\n")
	_, err := crBar.ReadString('\n') // Make sure subscriptions were processed.
	if err != nil {
		t.Fatalf("Error for client 'bar' from server: %v", err)
	}

	// Now publish our message.
	cfoo.parseAsync("PUB foo 5\r\nhello\r\n")

	// Now check we got the messages from wildcard subscription.
	l, err := crBar.ReadString('\n')
	if err != nil {
		t.Fatalf("Error reading from client 'bar': %v", err)
	}
	mraw := msgPat.FindAllStringSubmatch(l, -1)
	if len(mraw) == 0 {
		t.Fatalf("No message received")
	}
	matches := mraw[0]
	if matches[SUB_INDEX] != "pub.imports.foo" {
		t.Fatalf("Did not get correct subject: '%s'", matches[SUB_INDEX])
	}
	if matches[SID_INDEX] != "1" {
		t.Fatalf("Did not get correct sid: '%s'", matches[SID_INDEX])
	}
	checkPayload(crBar, []byte("hello\r\n"), t)
}

func TestMultipleImportsAndSingleWCSub(t *testing.T) {
	s, fooAcc, barAcc := simpleAccountServer(t)
	defer s.Shutdown()

	cfoo, _, _ := newClientForServer(s)
	defer cfoo.close()

	if err := cfoo.registerWithAccount(fooAcc); err != nil {
		t.Fatalf("Error registering client with 'foo' account: %v", err)
	}
	cbar, crBar, _ := newClientForServer(s)
	defer cbar.close()

	if err := cbar.registerWithAccount(barAcc); err != nil {
		t.Fatalf("Error registering client with 'bar' account: %v", err)
	}

	if err := fooAcc.AddStreamExport("foo", []*Account{barAcc}); err != nil {
		t.Fatalf("Error adding stream export to account foo: %v", err)
	}
	if err := fooAcc.AddStreamExport("bar", []*Account{barAcc}); err != nil {
		t.Fatalf("Error adding stream export to account foo: %v", err)
	}

	if err := barAcc.AddStreamImport(fooAcc, "foo", "pub."); err != nil {
		t.Fatalf("Error adding stream import to account bar: %v", err)
	}
	if err := barAcc.AddStreamImport(fooAcc, "bar", "pub."); err != nil {
		t.Fatalf("Error adding stream import to account bar: %v", err)
	}

	// Wildcard Subscription on bar client for both imports.
	cbar.parse([]byte("SUB pub.* 1\r\n"))

	// Now publish a message on 'foo' and 'bar'
	cfoo.parseAsync("PUB foo 5\r\nhello\r\nPUB bar 5\r\nworld\r\n")

	// Now check we got the messages from the wildcard subscription.
	l, err := crBar.ReadString('\n')
	if err != nil {
		t.Fatalf("Error reading from client 'bar': %v", err)
	}
	mraw := msgPat.FindAllStringSubmatch(l, -1)
	if len(mraw) == 0 {
		t.Fatalf("No message received")
	}
	matches := mraw[0]
	if matches[SUB_INDEX] != "pub.foo" {
		t.Fatalf("Did not get correct subject: '%s'", matches[SUB_INDEX])
	}
	if matches[SID_INDEX] != "1" {
		t.Fatalf("Did not get correct sid: '%s'", matches[SID_INDEX])
	}
	checkPayload(crBar, []byte("hello\r\n"), t)

	l, err = crBar.ReadString('\n')
	if err != nil {
		t.Fatalf("Error reading from client 'bar': %v", err)
	}
	mraw = msgPat.FindAllStringSubmatch(l, -1)
	if len(mraw) == 0 {
		t.Fatalf("No message received")
	}
	matches = mraw[0]
	if matches[SUB_INDEX] != "pub.bar" {
		t.Fatalf("Did not get correct subject: '%s'", matches[SUB_INDEX])
	}
	if matches[SID_INDEX] != "1" {
		t.Fatalf("Did not get correct sid: '%s'", matches[SID_INDEX])
	}
	checkPayload(crBar, []byte("world\r\n"), t)

	// Check subscription count.
	if fslc := fooAcc.sl.Count(); fslc != 2 {
		t.Fatalf("Expected 2 shadowed subscriptions on fooAcc, got %d", fslc)
	}
	if bslc := barAcc.sl.Count(); bslc != 1 {
		t.Fatalf("Expected 1 normal subscriptions on barAcc, got %d", bslc)
	}

	// Now unsubscribe.
	if err := cbar.parse([]byte("UNSUB 1\r\n")); err != nil {
		t.Fatalf("Error for client 'bar' from server: %v", err)
	}
	// We should have zero on both.
	if bslc := barAcc.sl.Count(); bslc != 0 {
		t.Fatalf("Expected no normal subscriptions on barAcc, got %d", bslc)
	}
	if fslc := fooAcc.sl.Count(); fslc != 0 {
		t.Fatalf("Expected no shadowed subscriptions on fooAcc, got %d", fslc)
	}
}

// Make sure the AddServiceExport function is additive if called multiple times.
func TestAddServiceExport(t *testing.T) {
	s, fooAcc, barAcc := simpleAccountServer(t)
	bazAcc, err := s.RegisterAccount("$baz")
	if err != nil {
		t.Fatalf("Error creating account 'baz': %v", err)
	}
	defer s.Shutdown()

	if err := fooAcc.AddServiceExport("test.request", nil); err != nil {
		t.Fatalf("Error adding account service export to client foo: %v", err)
	}
	tr := fooAcc.exports.services["test.request"]
	if len(tr.approved) != 0 {
		t.Fatalf("Expected no authorized accounts, got %d", len(tr.approved))
	}
	if err := fooAcc.AddServiceExport("test.request", []*Account{barAcc}); err != nil {
		t.Fatalf("Error adding account service export to client foo: %v", err)
	}
	tr = fooAcc.exports.services["test.request"]
	if tr == nil {
		t.Fatalf("Expected authorized accounts, got nil")
	}
	if ls := len(tr.approved); ls != 1 {
		t.Fatalf("Expected 1 authorized accounts, got %d", ls)
	}
	if err := fooAcc.AddServiceExport("test.request", []*Account{bazAcc}); err != nil {
		t.Fatalf("Error adding account service export to client foo: %v", err)
	}
	tr = fooAcc.exports.services["test.request"]
	if tr == nil {
		t.Fatalf("Expected authorized accounts, got nil")
	}
	if ls := len(tr.approved); ls != 2 {
		t.Fatalf("Expected 2 authorized accounts, got %d", ls)
	}
}

func TestServiceExportWithWildcards(t *testing.T) {
	for _, test := range []struct {
		name   string
		public bool
	}{
		{
			name:   "public",
			public: true,
		},
		{
			name:   "private",
			public: false,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			s, fooAcc, barAcc := simpleAccountServer(t)
			defer s.Shutdown()

			var accs []*Account
			if !test.public {
				accs = []*Account{barAcc}
			}
			// Add service export with a wildcard
			if err := fooAcc.AddServiceExport("ngs.update.*", accs); err != nil {
				t.Fatalf("Error adding account service export: %v", err)
			}
			// Import on bar account
			if err := barAcc.AddServiceImport(fooAcc, "ngs.update", "ngs.update.$bar"); err != nil {
				t.Fatalf("Error adding account service import: %v", err)
			}

			cfoo, crFoo, _ := newClientForServer(s)
			defer cfoo.close()

			if err := cfoo.registerWithAccount(fooAcc); err != nil {
				t.Fatalf("Error registering client with 'foo' account: %v", err)
			}
			cbar, crBar, _ := newClientForServer(s)
			defer cbar.close()

			if err := cbar.registerWithAccount(barAcc); err != nil {
				t.Fatalf("Error registering client with 'bar' account: %v", err)
			}

			// Now setup the responder under cfoo
			cfoo.parse([]byte("SUB ngs.update.* 1\r\n"))

			// Now send the request. Remember we expect the request on our local ngs.update.
			// We added the route with that "from" and will map it to "ngs.update.$bar"
			cbar.parseAsync("SUB reply 11\r\nPUB ngs.update reply 4\r\nhelp\r\n")

			// Now read the request from crFoo
			l, err := crFoo.ReadString('\n')
			if err != nil {
				t.Fatalf("Error reading from client 'bar': %v", err)
			}

			mraw := msgPat.FindAllStringSubmatch(l, -1)
			if len(mraw) == 0 {
				t.Fatalf("No message received")
			}
			matches := mraw[0]
			if matches[SUB_INDEX] != "ngs.update.$bar" {
				t.Fatalf("Did not get correct subject: '%s'", matches[SUB_INDEX])
			}
			if matches[SID_INDEX] != "1" {
				t.Fatalf("Did not get correct sid: '%s'", matches[SID_INDEX])
			}
			// Make sure this looks like _INBOX
			if !strings.HasPrefix(matches[REPLY_INDEX], "_R_.") {
				t.Fatalf("Expected an _R_.* like reply, got '%s'", matches[REPLY_INDEX])
			}
			checkPayload(crFoo, []byte("help\r\n"), t)

			replyOp := fmt.Sprintf("PUB %s 2\r\n22\r\n", matches[REPLY_INDEX])
			cfoo.parseAsync(replyOp)

			// Now read the response from crBar
			l, err = crBar.ReadString('\n')
			if err != nil {
				t.Fatalf("Error reading from client 'bar': %v", err)
			}
			mraw = msgPat.FindAllStringSubmatch(l, -1)
			if len(mraw) == 0 {
				t.Fatalf("No message received")
			}
			matches = mraw[0]
			if matches[SUB_INDEX] != "reply" {
				t.Fatalf("Did not get correct subject: '%s'", matches[SUB_INDEX])
			}
			if matches[SID_INDEX] != "11" {
				t.Fatalf("Did not get correct sid: '%s'", matches[SID_INDEX])
			}
			if matches[REPLY_INDEX] != "" {
				t.Fatalf("Did not get correct sid: '%s'", matches[SID_INDEX])
			}
			checkPayload(crBar, []byte("22\r\n"), t)

			if nr := barAcc.NumPendingAllResponses(); nr != 0 {
				t.Fatalf("Expected no responses on barAcc, got %d", nr)
			}
		})
	}
}

func TestAccountAddServiceImportRace(t *testing.T) {
	s, fooAcc, barAcc := simpleAccountServer(t)
	defer s.Shutdown()

	if err := fooAcc.AddServiceExport("foo.*", nil); err != nil {
		t.Fatalf("Error adding account service export to client foo: %v", err)
	}

	total := 100
	errCh := make(chan error, total)
	for i := 0; i < 100; i++ {
		go func(i int) {
			err := barAcc.AddServiceImport(fooAcc, fmt.Sprintf("foo.%d", i), "")
			errCh <- err // nil is a valid value.
		}(i)
	}

	for i := 0; i < 100; i++ {
		err := <-errCh
		if err != nil {
			t.Fatalf("Error adding account service import: %v", err)
		}
	}

	barAcc.mu.Lock()
	lens := len(barAcc.imports.services)
	c := barAcc.internalClient()
	barAcc.mu.Unlock()
	if lens != total {
		t.Fatalf("Expected %d imported services, got %d", total, lens)
	}
	c.mu.Lock()
	lens = len(c.subs)
	c.mu.Unlock()
	if lens != total {
		t.Fatalf("Expected %d subscriptions in internal client, got %d", total, lens)
	}
}

func TestServiceImportWithWildcards(t *testing.T) {
	s, fooAcc, barAcc := simpleAccountServer(t)
	defer s.Shutdown()

	if err := fooAcc.AddServiceExport("test.*", nil); err != nil {
		t.Fatalf("Error adding account service export to client foo: %v", err)
	}
	// We can not map wildcards atm, so if we supply a to mapping and a wildcard we should fail.
	if err := barAcc.AddServiceImport(fooAcc, "test.*", "foo"); err == nil {
		t.Fatalf("Expected error adding account service import with wildcard and mapping, got none")
	}
	if err := barAcc.AddServiceImport(fooAcc, "test.>", ""); err == nil {
		t.Fatalf("Expected error adding account service import with broader wildcard, got none")
	}
	// This should work.
	if err := barAcc.AddServiceImport(fooAcc, "test.*", ""); err != nil {
		t.Fatalf("Error adding account service import: %v", err)
	}
	// Make sure we can send and receive.
	cfoo, crFoo, _ := newClientForServer(s)
	defer cfoo.close()

	if err := cfoo.registerWithAccount(fooAcc); err != nil {
		t.Fatalf("Error registering client with 'foo' account: %v", err)
	}

	// Now setup the resonder under cfoo
	cfoo.parse([]byte("SUB test.* 1\r\n"))

	cbar, crBar, _ := newClientForServer(s)
	defer cbar.close()

	if err := cbar.registerWithAccount(barAcc); err != nil {
		t.Fatalf("Error registering client with 'bar' account: %v", err)
	}

	// Now send the request.
	go cbar.parse([]byte("SUB bar 11\r\nPUB test.22 bar 4\r\nhelp\r\n"))

	// Now read the request from crFoo
	l, err := crFoo.ReadString('\n')
	if err != nil {
		t.Fatalf("Error reading from client 'bar': %v", err)
	}

	mraw := msgPat.FindAllStringSubmatch(l, -1)
	if len(mraw) == 0 {
		t.Fatalf("No message received")
	}
	matches := mraw[0]
	if matches[SUB_INDEX] != "test.22" {
		t.Fatalf("Did not get correct subject: '%s'", matches[SUB_INDEX])
	}
	if matches[SID_INDEX] != "1" {
		t.Fatalf("Did not get correct sid: '%s'", matches[SID_INDEX])
	}
	// Make sure this looks like _INBOX
	if !strings.HasPrefix(matches[REPLY_INDEX], "_R_.") {
		t.Fatalf("Expected an _R_.* like reply, got '%s'", matches[REPLY_INDEX])
	}
	checkPayload(crFoo, []byte("help\r\n"), t)

	replyOp := fmt.Sprintf("PUB %s 2\r\n22\r\n", matches[REPLY_INDEX])
	go cfoo.parse([]byte(replyOp))

	// Now read the response from crBar
	l, err = crBar.ReadString('\n')
	if err != nil {
		t.Fatalf("Error reading from client 'bar': %v", err)
	}
	mraw = msgPat.FindAllStringSubmatch(l, -1)
	if len(mraw) == 0 {
		t.Fatalf("No message received")
	}
	matches = mraw[0]
	if matches[SUB_INDEX] != "bar" {
		t.Fatalf("Did not get correct subject: '%s'", matches[SUB_INDEX])
	}
	if matches[SID_INDEX] != "11" {
		t.Fatalf("Did not get correct sid: '%s'", matches[SID_INDEX])
	}
	if matches[REPLY_INDEX] != "" {
		t.Fatalf("Did not get correct sid: '%s'", matches[SID_INDEX])
	}
	checkPayload(crBar, []byte("22\r\n"), t)

	// Remove the service import with the wildcard and make sure hasWC is cleared.
	barAcc.removeServiceImport(fooAcc.Name, "test.*")

	barAcc.mu.Lock()
	defer barAcc.mu.Unlock()
	if len(barAcc.imports.services) != 0 {
		t.Fatalf("Expected no imported services, got %d", len(barAcc.imports.services))
	}
}

// Make sure the AddStreamExport function is additive if called multiple times.
func TestAddStreamExport(t *testing.T) {
	s, fooAcc, barAcc := simpleAccountServer(t)
	bazAcc, err := s.RegisterAccount("$baz")
	if err != nil {
		t.Fatalf("Error creating account 'baz': %v", err)
	}
	defer s.Shutdown()

	if err := fooAcc.AddStreamExport("test.request", nil); err != nil {
		t.Fatalf("Error adding account service export to client foo: %v", err)
	}
	tr := fooAcc.exports.streams["test.request"]
	if tr != nil {
		t.Fatalf("Expected no authorized accounts, got %d", len(tr.approved))
	}
	if err := fooAcc.AddStreamExport("test.request", []*Account{barAcc}); err != nil {
		t.Fatalf("Error adding account service export to client foo: %v", err)
	}
	tr = fooAcc.exports.streams["test.request"]
	if tr == nil {
		t.Fatalf("Expected authorized accounts, got nil")
	}
	if ls := len(tr.approved); ls != 1 {
		t.Fatalf("Expected 1 authorized accounts, got %d", ls)
	}
	if err := fooAcc.AddStreamExport("test.request", []*Account{bazAcc}); err != nil {
		t.Fatalf("Error adding account service export to client foo: %v", err)
	}
	tr = fooAcc.exports.streams["test.request"]
	if tr == nil {
		t.Fatalf("Expected authorized accounts, got nil")
	}
	if ls := len(tr.approved); ls != 2 {
		t.Fatalf("Expected 2 authorized accounts, got %d", ls)
	}
}

func TestCrossAccountRequestReply(t *testing.T) {
	s, fooAcc, barAcc := simpleAccountServer(t)
	defer s.Shutdown()

	cfoo, crFoo, _ := newClientForServer(s)
	defer cfoo.close()

	if err := cfoo.registerWithAccount(fooAcc); err != nil {
		t.Fatalf("Error registering client with 'foo' account: %v", err)
	}

	cbar, crBar, _ := newClientForServer(s)
	defer cbar.close()

	if err := cbar.registerWithAccount(barAcc); err != nil {
		t.Fatalf("Error registering client with 'bar' account: %v", err)
	}

	// Add in the service export for the requests. Make it public.
	if err := cfoo.acc.AddServiceExport("test.request", nil); err != nil {
		t.Fatalf("Error adding account service export to client foo: %v", err)
	}

	// Test addServiceImport to make sure it requires accounts.
	if err := cbar.acc.AddServiceImport(nil, "foo", "test.request"); err != ErrMissingAccount {
		t.Fatalf("Expected ErrMissingAccount but received %v.", err)
	}
	if err := cbar.acc.AddServiceImport(fooAcc, "foo", "test..request."); err != ErrInvalidSubject {
		t.Fatalf("Expected ErrInvalidSubject but received %v.", err)
	}

	// Now add in the route mapping for request to be routed to the foo account.
	if err := cbar.acc.AddServiceImport(fooAcc, "foo", "test.request"); err != nil {
		t.Fatalf("Error adding account service import to client bar: %v", err)
	}

	// Now setup the resonder under cfoo
	cfoo.parse([]byte("SUB test.request 1\r\n"))

	// Now send the request. Remember we expect the request on our local foo. We added the route
	// with that "from" and will map it to "test.request"
	cbar.parseAsync("SUB bar 11\r\nPUB foo bar 4\r\nhelp\r\n")

	// Now read the request from crFoo
	l, err := crFoo.ReadString('\n')
	if err != nil {
		t.Fatalf("Error reading from client 'bar': %v", err)
	}

	mraw := msgPat.FindAllStringSubmatch(l, -1)
	if len(mraw) == 0 {
		t.Fatalf("No message received")
	}
	matches := mraw[0]
	if matches[SUB_INDEX] != "test.request" {
		t.Fatalf("Did not get correct subject: '%s'", matches[SUB_INDEX])
	}
	if matches[SID_INDEX] != "1" {
		t.Fatalf("Did not get correct sid: '%s'", matches[SID_INDEX])
	}
	// Make sure this looks like _INBOX
	if !strings.HasPrefix(matches[REPLY_INDEX], "_R_.") {
		t.Fatalf("Expected an _R_.* like reply, got '%s'", matches[REPLY_INDEX])
	}
	checkPayload(crFoo, []byte("help\r\n"), t)

	replyOp := fmt.Sprintf("PUB %s 2\r\n22\r\n", matches[REPLY_INDEX])
	cfoo.parseAsync(replyOp)

	// Now read the response from crBar
	l, err = crBar.ReadString('\n')
	if err != nil {
		t.Fatalf("Error reading from client 'bar': %v", err)
	}
	mraw = msgPat.FindAllStringSubmatch(l, -1)
	if len(mraw) == 0 {
		t.Fatalf("No message received")
	}
	matches = mraw[0]
	if matches[SUB_INDEX] != "bar" {
		t.Fatalf("Did not get correct subject: '%s'", matches[SUB_INDEX])
	}
	if matches[SID_INDEX] != "11" {
		t.Fatalf("Did not get correct sid: '%s'", matches[SID_INDEX])
	}
	if matches[REPLY_INDEX] != "" {
		t.Fatalf("Did not get correct sid: '%s'", matches[SID_INDEX])
	}
	checkPayload(crBar, []byte("22\r\n"), t)

	if nr := barAcc.NumPendingAllResponses(); nr != 0 {
		t.Fatalf("Expected no responses on barAcc, got %d", nr)
	}
}

func TestAccountRequestReplyTrackLatency(t *testing.T) {
	s, fooAcc, barAcc := simpleAccountServer(t)
	defer s.Shutdown()

	// Run server in Go routine. We need this one running for internal sending of msgs.
	s.Start()
	// Wait for accept loop(s) to be started
	if err := s.readyForConnections(10 * time.Second); err != nil {
		t.Fatal(err)
	}

	cfoo, crFoo, _ := newClientForServer(s)
	defer cfoo.close()

	if err := cfoo.registerWithAccount(fooAcc); err != nil {
		t.Fatalf("Error registering client with 'foo' account: %v", err)
	}

	cbar, crBar, _ := newClientForServer(s)
	defer cbar.close()

	if err := cbar.registerWithAccount(barAcc); err != nil {
		t.Fatalf("Error registering client with 'bar' account: %v", err)
	}

	// Add in the service export for the requests. Make it public.
	if err := fooAcc.AddServiceExport("track.service", nil); err != nil {
		t.Fatalf("Error adding account service export to client foo: %v", err)
	}

	// Now let's add in tracking

	// First check we get an error if service does not exist.
	if err := fooAcc.TrackServiceExport("track.wrong", "results"); err != ErrMissingService {
		t.Fatalf("Expected error enabling tracking latency for wrong service")
	}
	// Check results should be a valid subject
	if err := fooAcc.TrackServiceExport("track.service", "results.*"); err != ErrBadPublishSubject {
		t.Fatalf("Expected error enabling tracking latency for bad results subject")
	}
	// Make sure we can not loop around on ourselves..
	if err := fooAcc.TrackServiceExport("track.service", "track.service"); err != ErrBadPublishSubject {
		t.Fatalf("Expected error enabling tracking latency for same subject")
	}
	// Check bad sampling
	if err := fooAcc.TrackServiceExportWithSampling("track.service", "results", -1); err != ErrBadSampling {
		t.Fatalf("Expected error enabling tracking latency for bad sampling")
	}
	if err := fooAcc.TrackServiceExportWithSampling("track.service", "results", 101); err != ErrBadSampling {
		t.Fatalf("Expected error enabling tracking latency for bad sampling")
	}

	// Now let's add in tracking for real. This will be 100%
	if err := fooAcc.TrackServiceExport("track.service", "results"); err != nil {
		t.Fatalf("Error enabling tracking latency: %v", err)
	}

	// Now add in the route mapping for request to be routed to the foo account.
	if err := barAcc.AddServiceImport(fooAcc, "req", "track.service"); err != nil {
		t.Fatalf("Error adding account service import to client bar: %v", err)
	}

	// Now setup the responder under cfoo and the listener for the results
	cfoo.parse([]byte("SUB track.service 1\r\nSUB results 2\r\n"))

	readFooMsg := func() ([]byte, string) {
		t.Helper()
		l, err := crFoo.ReadString('\n')
		if err != nil {
			t.Fatalf("Error reading from client 'foo': %v", err)
		}
		mraw := msgPat.FindAllStringSubmatch(l, -1)
		if len(mraw) == 0 {
			t.Fatalf("No message received")
		}
		msg := mraw[0]
		msgSize, _ := strconv.Atoi(msg[LEN_INDEX])
		return grabPayload(crFoo, msgSize), msg[REPLY_INDEX]
	}

	start := time.Now()

	// Now send the request. Remember we expect the request on our local foo. We added the route
	// with that "from" and will map it to "test.request"
	cbar.parseAsync("SUB resp 11\r\nPUB req resp 4\r\nhelp\r\n")

	// Now read the request from crFoo
	_, reply := readFooMsg()
	replyOp := fmt.Sprintf("PUB %s 2\r\n22\r\n", reply)

	serviceTime := 25 * time.Millisecond

	// We will wait a bit to check latency results
	go func() {
		time.Sleep(serviceTime)
		cfoo.parseAsync(replyOp)
	}()

	// Now read the response from crBar
	_, err := crBar.ReadString('\n')
	if err != nil {
		t.Fatalf("Error reading from client 'bar': %v", err)
	}

	// Now let's check that we got the sampling results
	rMsg, _ := readFooMsg()

	// Unmarshal and check it.
	var sl ServiceLatency
	err = json.Unmarshal(rMsg, &sl)
	if err != nil {
		t.Fatalf("Could not parse latency json: %v\n", err)
	}
	startDelta := sl.RequestStart.Sub(start)
	if startDelta > 5*time.Millisecond {
		t.Fatalf("Bad start delta %v", startDelta)
	}
	if sl.ServiceLatency < serviceTime {
		t.Fatalf("Bad service latency: %v", sl.ServiceLatency)
	}
	if sl.TotalLatency < sl.ServiceLatency {
		t.Fatalf("Bad total latency: %v", sl.ServiceLatency)
	}
}

// This will test for leaks in the remote latency tracking via client.rrTracking
func TestAccountTrackLatencyRemoteLeaks(t *testing.T) {
	optsA, err := ProcessConfigFile("./configs/seed.conf")
	require_NoError(t, err)
	optsA.NoSigs, optsA.NoLog = true, true
	optsA.ServerName = "A"
	srvA := RunServer(optsA)
	defer srvA.Shutdown()
	optsB := nextServerOpts(optsA)
	optsB.Routes = RoutesFromStr(fmt.Sprintf("nats://%s:%d", optsA.Cluster.Host, optsA.Cluster.Port))
	optsB.ServerName = "B"
	srvB := RunServer(optsB)
	defer srvB.Shutdown()

	checkClusterFormed(t, srvA, srvB)
	srvs := []*Server{srvA, srvB}

	// Now add in the accounts and setup tracking.
	for _, s := range srvs {
		s.SetSystemAccount(globalAccountName)
		fooAcc, _ := s.RegisterAccount("$foo")
		fooAcc.AddServiceExport("track.service", nil)
		fooAcc.TrackServiceExport("track.service", "results")
		barAcc, _ := s.RegisterAccount("$bar")
		if err := barAcc.AddServiceImport(fooAcc, "req", "track.service"); err != nil {
			t.Fatalf("Failed to import: %v", err)
		}
	}

	getClient := func(s *Server, name string) *client {
		t.Helper()
		s.mu.Lock()
		defer s.mu.Unlock()
		for _, c := range s.clients {
			c.mu.Lock()
			n := c.opts.Name
			c.mu.Unlock()
			if n == name {
				return c
			}
		}
		t.Fatalf("Did not find client %q on server %q", name, s.info.ID)
		return nil
	}

	// Test with a responder on second server, srvB. but they will not respond.
	cfooNC := natsConnect(t, srvB.ClientURL(), nats.Name("foo"))
	defer cfooNC.Close()
	cfoo := getClient(srvB, "foo")
	fooAcc, _ := srvB.LookupAccount("$foo")
	if err := cfoo.registerWithAccount(fooAcc); err != nil {
		t.Fatalf("Error registering client with 'foo' account: %v", err)
	}

	// Set new limits
	for _, srv := range srvs {
		fooAcc, _ := srv.LookupAccount("$foo")
		err := fooAcc.SetServiceExportResponseThreshold("track.service", 5*time.Millisecond)
		if err != nil {
			t.Fatalf("Error setting response threshold: %v", err)
		}
	}

	// Now setup the responder under cfoo and the listener for the results
	time.Sleep(50 * time.Millisecond)
	baseSubs := int(srvA.NumSubscriptions())
	fooSub := natsSubSync(t, cfooNC, "track.service")
	natsFlush(t, cfooNC)
	// Wait for it to propagate.
	checkExpectedSubs(t, baseSubs+1, srvA)

	cbarNC := natsConnect(t, srvA.ClientURL(), nats.Name("bar"))
	defer cbarNC.Close()
	cbar := getClient(srvA, "bar")

	barAcc, _ := srvA.LookupAccount("$bar")
	if err := cbar.registerWithAccount(barAcc); err != nil {
		t.Fatalf("Error registering client with 'bar' account: %v", err)
	}

	readFooMsg := func() {
		t.Helper()
		if _, err := fooSub.NextMsg(time.Second); err != nil {
			t.Fatalf("Did not receive foo msg: %v", err)
		}
	}

	// Send 2 requests
	natsSubSync(t, cbarNC, "resp")

	natsPubReq(t, cbarNC, "req", "resp", []byte("help"))
	natsPubReq(t, cbarNC, "req", "resp", []byte("help"))

	readFooMsg()
	readFooMsg()

	var rc *client
	// Pull out first client
	srvB.mu.Lock()
	for _, rc = range srvB.clients {
		if rc != nil {
			break
		}
	}
	srvB.mu.Unlock()

	tracking := func() int {
		rc.mu.Lock()
		var nt int
		if rc.rrTracking != nil {
			nt = len(rc.rrTracking.rmap)
		}
		rc.mu.Unlock()
		return nt
	}

	expectTracking := func(expected int) {
		t.Helper()
		checkFor(t, time.Second, 10*time.Millisecond, func() error {
			if numTracking := tracking(); numTracking != expected {
				return fmt.Errorf("Expected to have %d tracking replies, got %d", expected, numTracking)
			}
			return nil
		})
	}

	expectTracking(2)
	// Make sure these remote tracking replies honor the current respThresh for a service export.
	time.Sleep(10 * time.Millisecond)
	expectTracking(0)
	// Also make sure tracking is removed
	rc.mu.Lock()
	removed := rc.rrTracking == nil
	rc.mu.Unlock()
	if !removed {
		t.Fatalf("Expected the rrTracking to be removed")
	}

	// Now let's test that a lower response threshold is picked up.
	fSub := natsSubSync(t, cfooNC, "foo")
	natsFlush(t, cfooNC)

	// Wait for it to propagate.
	checkExpectedSubs(t, baseSubs+4, srvA)

	// queue up some first. We want to test changing when rrTracking exists.
	natsPubReq(t, cbarNC, "req", "resp", []byte("help"))
	readFooMsg()
	expectTracking(1)

	for _, s := range srvs {
		fooAcc, _ := s.LookupAccount("$foo")
		barAcc, _ := s.LookupAccount("$bar")
		fooAcc.AddServiceExport("foo", nil)
		fooAcc.TrackServiceExport("foo", "foo.results")
		fooAcc.SetServiceExportResponseThreshold("foo", time.Millisecond)
		barAcc.AddServiceImport(fooAcc, "foo", "foo")
	}

	natsSubSync(t, cbarNC, "reply")
	natsPubReq(t, cbarNC, "foo", "reply", []byte("help"))
	if _, err := fSub.NextMsg(time.Second); err != nil {
		t.Fatalf("Did not receive foo msg: %v", err)
	}
	expectTracking(2)

	rc.mu.Lock()
	lrt := rc.rrTracking.lrt
	rc.mu.Unlock()
	if lrt != time.Millisecond {
		t.Fatalf("Expected lrt of %v, got %v", time.Millisecond, lrt)
	}

	// Now make sure we clear on close.
	rc.closeConnection(ClientClosed)

	// Actual tear down will be not inline.
	checkFor(t, time.Second, 5*time.Millisecond, func() error {
		rc.mu.Lock()
		removed = rc.rrTracking == nil
		rc.mu.Unlock()
		if !removed {
			return fmt.Errorf("Expected the rrTracking to be removed after client close")
		}
		return nil
	})
}

func TestCrossAccountServiceResponseTypes(t *testing.T) {
	s, fooAcc, barAcc := simpleAccountServer(t)
	defer s.Shutdown()

	cfoo, crFoo, _ := newClientForServer(s)
	defer cfoo.close()

	if err := cfoo.registerWithAccount(fooAcc); err != nil {
		t.Fatalf("Error registering client with 'foo' account: %v", err)
	}
	cbar, crBar, _ := newClientForServer(s)
	defer cbar.close()

	if err := cbar.registerWithAccount(barAcc); err != nil {
		t.Fatalf("Error registering client with 'bar' account: %v", err)
	}

	// Add in the service export for the requests. Make it public.
	if err := fooAcc.AddServiceExportWithResponse("test.request", Streamed, nil); err != nil {
		t.Fatalf("Error adding account service export to client foo: %v", err)
	}
	// Now add in the route mapping for request to be routed to the foo account.
	if err := barAcc.AddServiceImport(fooAcc, "foo", "test.request"); err != nil {
		t.Fatalf("Error adding account service import to client bar: %v", err)
	}

	// Now setup the resonder under cfoo
	cfoo.parse([]byte("SUB test.request 1\r\n"))

	// Now send the request. Remember we expect the request on our local foo. We added the route
	// with that "from" and will map it to "test.request"
	cbar.parseAsync("SUB bar 11\r\nPUB foo bar 4\r\nhelp\r\n")

	// Now read the request from crFoo
	l, err := crFoo.ReadString('\n')
	if err != nil {
		t.Fatalf("Error reading from client 'bar': %v", err)
	}

	mraw := msgPat.FindAllStringSubmatch(l, -1)
	if len(mraw) == 0 {
		t.Fatalf("No message received")
	}
	matches := mraw[0]
	reply := matches[REPLY_INDEX]
	if !strings.HasPrefix(reply, "_R_.") {
		t.Fatalf("Expected an _R_.* like reply, got '%s'", reply)
	}
	crFoo.ReadString('\n')

	replyOp := fmt.Sprintf("PUB %s 2\r\n22\r\n", matches[REPLY_INDEX])
	var mReply []byte
	for i := 0; i < 10; i++ {
		mReply = append(mReply, replyOp...)
	}

	cfoo.parseAsync(string(mReply))

	var buf []byte
	for i := 0; i < 20; i++ {
		b, err := crBar.ReadBytes('\n')
		if err != nil {
			t.Fatalf("Error reading response: %v", err)
		}
		buf = append(buf[:], b...)
		if mraw = msgPat.FindAllStringSubmatch(string(buf), -1); len(mraw) == 10 {
			break
		}
	}
	if len(mraw) != 10 {
		t.Fatalf("Expected a response but got %d", len(mraw))
	}

	// Also make sure the response map gets cleaned up when interest goes away.
	cbar.closeConnection(ClientClosed)

	checkFor(t, time.Second, 10*time.Millisecond, func() error {
		if nr := barAcc.NumPendingAllResponses(); nr != 0 {
			return fmt.Errorf("Number of responses is %d", nr)
		}
		return nil
	})

	// Now test bogus reply subjects are handled and do not accumulate the response maps.
	cbar, _, _ = newClientForServer(s)
	defer cbar.close()

	if err := cbar.registerWithAccount(barAcc); err != nil {
		t.Fatalf("Error registering client with 'bar' account: %v", err)
	}

	// Do not create any interest in the reply subject 'bar'. Just send a request.
	cbar.parseAsync("PUB foo bar 4\r\nhelp\r\n")

	// Now read the request from crFoo
	l, err = crFoo.ReadString('\n')
	if err != nil {
		t.Fatalf("Error reading from client 'bar': %v", err)
	}
	mraw = msgPat.FindAllStringSubmatch(l, -1)
	if len(mraw) == 0 {
		t.Fatalf("No message received")
	}
	matches = mraw[0]
	reply = matches[REPLY_INDEX]
	if !strings.HasPrefix(reply, "_R_.") {
		t.Fatalf("Expected an _R_.* like reply, got '%s'", reply)
	}
	crFoo.ReadString('\n')

	replyOp = fmt.Sprintf("PUB %s 2\r\n22\r\n", matches[REPLY_INDEX])

	cfoo.parseAsync(replyOp)

	// Now wait for a bit, the reply should trip a no interest condition
	// which should clean this up.
	checkFor(t, time.Second, 10*time.Millisecond, func() error {
		if nr := fooAcc.NumPendingAllResponses(); nr != 0 {
			return fmt.Errorf("Number of responses is %d", nr)
		}
		return nil
	})

	// Also make sure the response map entry is gone as well.
	fooAcc.mu.RLock()
	lrm := len(fooAcc.exports.responses)
	fooAcc.mu.RUnlock()

	if lrm != 0 {
		t.Fatalf("Expected the responses to be cleared, got %d entries", lrm)
	}
}

func TestAccountMapsUsers(t *testing.T) {
	// Used for the nkey users to properly sign.
	seed1 := "SUAPM67TC4RHQLKBX55NIQXSMATZDOZK6FNEOSS36CAYA7F7TY66LP4BOM"
	seed2 := "SUAIS5JPX4X4GJ7EIIJEQ56DH2GWPYJRPWN5XJEDENJOZHCBLI7SEPUQDE"

	confFileName := createConfFile(t, []byte(`
    accounts {
      synadia {
        users = [
          {user: derek, password: foo},
          {nkey: UCNGL4W5QX66CFX6A6DCBVDH5VOHMI7B2UZZU7TXAUQQSI2JPHULCKBR}
        ]
      }
      nats {
        users = [
          {user: ivan, password: bar},
          {nkey: UDPGQVFIWZ7Q5UH4I5E6DBCZULQS6VTVBG6CYBD7JV3G3N2GMQOMNAUH}
        ]
      }
    }
    `))
	opts, err := ProcessConfigFile(confFileName)
	if err != nil {
		t.Fatalf("Unexpected error parsing config file: %v", err)
	}
	opts.NoSigs = true
	s := New(opts)
	defer s.Shutdown()
	synadia, _ := s.LookupAccount("synadia")
	nats, _ := s.LookupAccount("nats")

	if synadia == nil || nats == nil {
		t.Fatalf("Expected non nil accounts during lookup")
	}

	// Make sure a normal log in maps the accounts correctly.
	c, _, _ := newClientForServer(s)
	defer c.close()
	connectOp := []byte("CONNECT {\"user\":\"derek\",\"pass\":\"foo\"}\r\n")
	c.parse(connectOp)
	if c.acc != synadia {
		t.Fatalf("Expected the client's account to match 'synadia', got %v", c.acc)
	}

	c, _, _ = newClientForServer(s)
	defer c.close()
	connectOp = []byte("CONNECT {\"user\":\"ivan\",\"pass\":\"bar\"}\r\n")
	c.parse(connectOp)
	if c.acc != nats {
		t.Fatalf("Expected the client's account to match 'nats', got %v", c.acc)
	}

	// Now test nkeys as well.
	kp, _ := nkeys.FromSeed([]byte(seed1))
	pubKey, _ := kp.PublicKey()

	c, cr, l := newClientForServer(s)
	defer c.close()
	// Check for Nonce
	var info nonceInfo
	err = json.Unmarshal([]byte(l[5:]), &info)
	if err != nil {
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
	kp, _ = nkeys.FromSeed([]byte(seed2))
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
}

func TestAccountGlobalDefault(t *testing.T) {
	opts := defaultServerOptions
	s := New(&opts)

	if acc, _ := s.LookupAccount(globalAccountName); acc == nil {
		t.Fatalf("Expected a global default account on a new server, got none.")
	}
	// Make sure we can not create one with same name..
	if _, err := s.RegisterAccount(globalAccountName); err == nil {
		t.Fatalf("Expected error trying to create a new reserved account")
	}

	// Make sure we can not define one in a config file either.
	confFileName := createConfFile(t, []byte(`accounts { $G {} }`))

	if _, err := ProcessConfigFile(confFileName); err == nil {
		t.Fatalf("Expected an error parsing config file with reserved account")
	}
}

func TestAccountCheckStreamImportsEqual(t *testing.T) {
	// Create bare accounts for this test
	fooAcc := NewAccount("foo")
	if err := fooAcc.AddStreamExport(">", nil); err != nil {
		t.Fatalf("Error adding stream export: %v", err)
	}

	barAcc := NewAccount("bar")
	if err := barAcc.AddStreamImport(fooAcc, "foo", "myPrefix"); err != nil {
		t.Fatalf("Error adding stream import: %v", err)
	}
	bazAcc := NewAccount("baz")
	if err := bazAcc.AddStreamImport(fooAcc, "foo", "myPrefix"); err != nil {
		t.Fatalf("Error adding stream import: %v", err)
	}
	if !barAcc.checkStreamImportsEqual(bazAcc) {
		t.Fatal("Expected stream imports to be the same")
	}

	if err := bazAcc.AddStreamImport(fooAcc, "foo.>", ""); err != nil {
		t.Fatalf("Error adding stream import: %v", err)
	}
	if barAcc.checkStreamImportsEqual(bazAcc) {
		t.Fatal("Expected stream imports to be different")
	}
	if err := barAcc.AddStreamImport(fooAcc, "foo.>", ""); err != nil {
		t.Fatalf("Error adding stream import: %v", err)
	}
	if !barAcc.checkStreamImportsEqual(bazAcc) {
		t.Fatal("Expected stream imports to be the same")
	}

	// Create another account that is named "foo". We want to make sure
	// that the comparison still works (based on account name, not pointer)
	newFooAcc := NewAccount("foo")
	if err := newFooAcc.AddStreamExport(">", nil); err != nil {
		t.Fatalf("Error adding stream export: %v", err)
	}
	batAcc := NewAccount("bat")
	if err := batAcc.AddStreamImport(newFooAcc, "foo", "myPrefix"); err != nil {
		t.Fatalf("Error adding stream import: %v", err)
	}
	if err := batAcc.AddStreamImport(newFooAcc, "foo.>", ""); err != nil {
		t.Fatalf("Error adding stream import: %v", err)
	}
	if !batAcc.checkStreamImportsEqual(barAcc) {
		t.Fatal("Expected stream imports to be the same")
	}
	if !batAcc.checkStreamImportsEqual(bazAcc) {
		t.Fatal("Expected stream imports to be the same")
	}

	// Test with account with different "from"
	expAcc := NewAccount("new_acc")
	if err := expAcc.AddStreamExport(">", nil); err != nil {
		t.Fatalf("Error adding stream export: %v", err)
	}
	aAcc := NewAccount("a")
	if err := aAcc.AddStreamImport(expAcc, "bar", ""); err != nil {
		t.Fatalf("Error adding stream import: %v", err)
	}
	bAcc := NewAccount("b")
	if err := bAcc.AddStreamImport(expAcc, "baz", ""); err != nil {
		t.Fatalf("Error adding stream import: %v", err)
	}
	if aAcc.checkStreamImportsEqual(bAcc) {
		t.Fatal("Expected stream imports to be different")
	}

	// Test with account with different "prefix"
	aAcc = NewAccount("a")
	if err := aAcc.AddStreamImport(expAcc, "bar", "prefix"); err != nil {
		t.Fatalf("Error adding stream import: %v", err)
	}
	bAcc = NewAccount("b")
	if err := bAcc.AddStreamImport(expAcc, "bar", "diff_prefix"); err != nil {
		t.Fatalf("Error adding stream import: %v", err)
	}
	if aAcc.checkStreamImportsEqual(bAcc) {
		t.Fatal("Expected stream imports to be different")
	}

	// Test with account with different "name"
	expAcc = NewAccount("diff_name")
	if err := expAcc.AddStreamExport(">", nil); err != nil {
		t.Fatalf("Error adding stream export: %v", err)
	}
	bAcc = NewAccount("b")
	if err := bAcc.AddStreamImport(expAcc, "bar", "prefix"); err != nil {
		t.Fatalf("Error adding stream import: %v", err)
	}
	if aAcc.checkStreamImportsEqual(bAcc) {
		t.Fatal("Expected stream imports to be different")
	}
}

func TestAccountNoDeadlockOnQueueSubRouteMapUpdate(t *testing.T) {
	opts := DefaultOptions()
	s := RunServer(opts)
	defer s.Shutdown()

	nc, err := nats.Connect(fmt.Sprintf("nats://%s:%d", opts.Host, opts.Port))
	if err != nil {
		t.Fatalf("Error on connect: %v", err)
	}
	defer nc.Close()

	nc.QueueSubscribeSync("foo", "bar")

	var accs []*Account
	for i := 0; i < 10; i++ {
		acc, _ := s.RegisterAccount(fmt.Sprintf("acc%d", i))
		acc.mu.Lock()
		accs = append(accs, acc)
	}

	opts2 := DefaultOptions()
	opts2.Routes = RoutesFromStr(fmt.Sprintf("nats://%s:%d", opts.Cluster.Host, opts.Cluster.Port))
	s2 := RunServer(opts2)
	defer s2.Shutdown()

	wg := sync.WaitGroup{}
	wg.Add(1)
	go func() {
		time.Sleep(100 * time.Millisecond)
		for _, acc := range accs {
			acc.mu.Unlock()
		}
		wg.Done()
	}()

	nc.QueueSubscribeSync("foo", "bar")
	nc.Flush()

	wg.Wait()
}

func TestAccountDuplicateServiceImportSubject(t *testing.T) {
	opts := DefaultOptions()
	s := RunServer(opts)
	defer s.Shutdown()

	fooAcc, _ := s.RegisterAccount("foo")
	fooAcc.AddServiceExport("remote1", nil)
	fooAcc.AddServiceExport("remote2", nil)

	barAcc, _ := s.RegisterAccount("bar")
	if err := barAcc.AddServiceImport(fooAcc, "foo", "remote1"); err != nil {
		t.Fatalf("Error adding service import: %v", err)
	}
	if err := barAcc.AddServiceImport(fooAcc, "foo", "remote2"); err == nil || !strings.Contains(err.Error(), "duplicate") {
		t.Fatalf("Expected an error about duplicate service import subject, got %q", err)
	}

	// However, it is allowed to add more than one service import
	// on the same subject for different exporting accounts
	bazAcc, _ := s.RegisterAccount("baz")
	bazAcc.AddServiceExport("remote1", nil)
	if err := barAcc.AddServiceImport(bazAcc, "foo", "remote1"); err != nil {
		t.Fatalf("Error adding service import: %v", err)
	}
	batAcc, _ := s.RegisterAccount("bat")
	batAcc.AddServiceExport("remote1", nil)
	if err := barAcc.AddServiceImport(batAcc, "foo", "remote1"); err != nil {
		t.Fatalf("Error adding service import: %v", err)
	}

	// But again, can't add for one that has been already added, say bazAcc.
	if err := barAcc.AddServiceImport(bazAcc, "foo", "remote1"); err == nil || !strings.Contains(err.Error(), "duplicate") {
		t.Fatalf("Expected an error about duplicate service import subject, got %q", err)
	}
}

func TestAccountRemoveServiceImport(t *testing.T) {
	opts := DefaultOptions()
	s := RunServer(opts)
	defer s.Shutdown()

	fooAcc, _ := s.RegisterAccount("foo")
	fooAcc.AddServiceExport("remote1", nil)

	barAcc, _ := s.RegisterAccount("bar")
	err := barAcc.AddServiceImport(fooAcc, "foo", "remote1")
	require_NoError(t, err)

	bazAcc, _ := s.RegisterAccount("baz")
	bazAcc.AddServiceExport("remote1", nil)
	err = barAcc.AddServiceImport(bazAcc, "foo", "remote1")
	require_NoError(t, err)

	batAcc, _ := s.RegisterAccount("bat")
	batAcc.AddServiceExport("remote1", nil)
	err = barAcc.AddServiceImport(batAcc, "foo", "remote1")
	require_NoError(t, err)

	// Check that we can get the service import we expect
	checkGetSI := func(acc, subj string) bool {
		barAcc.mu.RLock()
		defer barAcc.mu.RUnlock()
		si := barAcc.getServiceImportForAccountLocked(acc, subj)
		if si == nil {
			return false
		}
		return si.acc.Name == acc
	}
	require_False(t, checkGetSI("nonsense", "foo"))
	require_False(t, checkGetSI("foo", "nonsense"))
	require_True(t, checkGetSI("foo", "foo"))
	require_True(t, checkGetSI("baz", "foo"))
	require_True(t, checkGetSI("bat", "foo"))

	// Remove service imports one by one and check expected map content.

	// Try first to remove one that does not exists, because of subject
	// or because of account.
	barAcc.removeServiceImport("nonsense", "foo")
	require_True(t, checkGetSI("foo", "foo"))
	require_True(t, checkGetSI("baz", "foo"))
	require_True(t, checkGetSI("bat", "foo"))
	barAcc.removeServiceImport("baz", "nonsense")
	require_True(t, checkGetSI("foo", "foo"))
	require_True(t, checkGetSI("baz", "foo"))
	require_True(t, checkGetSI("bat", "foo"))

	// Remove the 2nd account that was added
	barAcc.removeServiceImport("baz", "foo")
	require_False(t, checkGetSI("baz", "foo"))
	require_True(t, checkGetSI("foo", "foo"))
	require_True(t, checkGetSI("bat", "foo"))

	// Remove the last account that was added
	barAcc.removeServiceImport("bat", "foo")
	require_False(t, checkGetSI("baz", "foo"))
	require_False(t, checkGetSI("bat", "foo"))
	require_True(t, checkGetSI("foo", "foo"))

	// There is only one left, make sure that we still properly check
	// for appropriate subject and account name.
	barAcc.removeServiceImport("nonsense", "foo")
	require_False(t, checkGetSI("baz", "foo"))
	require_False(t, checkGetSI("bat", "foo"))
	require_True(t, checkGetSI("foo", "foo"))
	barAcc.removeServiceImport("foo", "nonsense")
	require_False(t, checkGetSI("baz", "foo"))
	require_False(t, checkGetSI("bat", "foo"))
	require_True(t, checkGetSI("foo", "foo"))

	// Remove the first account that was added
	barAcc.removeServiceImport("foo", "foo")
	require_False(t, checkGetSI("baz", "foo"))
	require_False(t, checkGetSI("bat", "foo"))
	require_False(t, checkGetSI("foo", "foo"))

	// Make sure that the service import map is cleared
	barAcc.mu.RLock()
	sis, ok := barAcc.imports.services["foo"]
	barAcc.mu.RUnlock()
	require_False(t, ok)
	require_Len(t, len(sis), 0)
}

func TestAccountMultipleServiceImportsWithSameSubjectFromDifferentAccounts(t *testing.T) {
	conf := createConfFile(t, []byte(`
		accounts: {
			SVC-E: {
				users: [ { user:svc-e, password:svc-e } ]
				exports: [ { accounts: [CLIENTS], service: "SvcReq.>", response_type: singleton } ]
			}

			SVC-W: {
				users: [ { user:svc-w, password:svc-w } ]
				exports: [ { accounts: [CLIENTS], service: "SvcReq.>", response_type: singleton } ]
			}

			CLIENTS: {
				users: [ { user:cl, password:cl } ]
				imports: [
					{ service: { account: SVC-E, subject: "SvcReq.>" } }
					{ service: { account: SVC-W, subject: "SvcReq.>" } }
				]
			}
		}
		listen: "127.0.0.1:-1"
	`))
	s, _ := RunServerWithConfig(conf)
	defer s.Shutdown()

	ncE := natsConnect(t, s.ClientURL(), nats.UserInfo("svc-e", "svc-e"))
	defer ncE.Close()
	natsSub(t, ncE, "SvcReq.>", func(m *nats.Msg) {
		m.Respond([]byte("From SVC-E"))
	})
	natsFlush(t, ncE)

	ncW := natsConnect(t, s.ClientURL(), nats.UserInfo("svc-w", "svc-w"))
	defer ncW.Close()
	natsSub(t, ncW, "SvcReq.>", func(m *nats.Msg) {
		m.Respond([]byte("From SVC-W"))
	})
	natsFlush(t, ncW)

	nc := natsConnect(t, s.ClientURL(), nats.UserInfo("cl", "cl"))
	defer nc.Close()
	sub := natsSubSync(t, nc, nats.NewInbox())
	natsPubReq(t, nc, "SvcReq.Test", sub.Subject, []byte("test"))
	msg1 := natsNexMsg(t, sub, time.Second)
	msg2 := natsNexMsg(t, sub, time.Second)
	m1 := string(msg1.Data)
	m2 := string(msg2.Data)
	if (m1 == "From SVC-E" && m2 == "From SVC-W") || (m1 == "From SVC-W" && m2 == "From SVC-E") {
		// Ok
		return
	}
	t.Fatalf("Unexpected responses: m1=%s m2=%s", m1, m2)
}

func TestMultipleStreamImportsWithSameSubjectDifferentPrefix(t *testing.T) {
	opts := DefaultOptions()
	s := RunServer(opts)
	defer s.Shutdown()

	fooAcc, _ := s.RegisterAccount("foo")
	fooAcc.AddStreamExport("test", nil)

	barAcc, _ := s.RegisterAccount("bar")
	barAcc.AddStreamExport("test", nil)

	importAcc, _ := s.RegisterAccount("import")

	if err := importAcc.AddStreamImport(fooAcc, "test", "foo"); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if err := importAcc.AddStreamImport(barAcc, "test", "bar"); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	// Now make sure we can see messages from both.
	cimport, crImport, _ := newClientForServer(s)
	defer cimport.close()
	if err := cimport.registerWithAccount(importAcc); err != nil {
		t.Fatalf("Error registering client with 'import' account: %v", err)
	}
	if err := cimport.parse([]byte("SUB *.test 1\r\n")); err != nil {
		t.Fatalf("Error for client 'import' from server: %v", err)
	}

	cfoo, _, _ := newClientForServer(s)
	defer cfoo.close()
	if err := cfoo.registerWithAccount(fooAcc); err != nil {
		t.Fatalf("Error registering client with 'foo' account: %v", err)
	}

	cbar, _, _ := newClientForServer(s)
	defer cbar.close()
	if err := cbar.registerWithAccount(barAcc); err != nil {
		t.Fatalf("Error registering client with 'bar' account: %v", err)
	}

	readMsg := func() {
		t.Helper()
		l, err := crImport.ReadString('\n')
		if err != nil {
			t.Fatalf("Error reading msg header from client 'import': %v", err)
		}
		mraw := msgPat.FindAllStringSubmatch(l, -1)
		if len(mraw) == 0 {
			t.Fatalf("No message received")
		}
		// Consume msg body too.
		if _, err = crImport.ReadString('\n'); err != nil {
			t.Fatalf("Error reading msg body from client 'import': %v", err)
		}
	}

	cbar.parseAsync("PUB test 9\r\nhello-bar\r\n")
	readMsg()

	cfoo.parseAsync("PUB test 9\r\nhello-foo\r\n")
	readMsg()
}

// This should work with prefixes that are different but we also want it to just work with same subject
// being imported from multiple accounts.
func TestMultipleStreamImportsWithSameSubject(t *testing.T) {
	opts := DefaultOptions()
	s := RunServer(opts)
	defer s.Shutdown()

	fooAcc, _ := s.RegisterAccount("foo")
	fooAcc.AddStreamExport("test", nil)

	barAcc, _ := s.RegisterAccount("bar")
	barAcc.AddStreamExport("test", nil)

	importAcc, _ := s.RegisterAccount("import")

	if err := importAcc.AddStreamImport(fooAcc, "test", ""); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	// Since we allow this now, make sure we do detect a duplicate import from same account etc.
	// That should be not allowed.
	if err := importAcc.AddStreamImport(fooAcc, "test", ""); err != ErrStreamImportDuplicate {
		t.Fatalf("Expected ErrStreamImportDuplicate but got %v", err)
	}

	if err := importAcc.AddStreamImport(barAcc, "test", ""); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	// Now make sure we can see messages from both.
	cimport, crImport, _ := newClientForServer(s)
	defer cimport.close()
	if err := cimport.registerWithAccount(importAcc); err != nil {
		t.Fatalf("Error registering client with 'import' account: %v", err)
	}
	if err := cimport.parse([]byte("SUB test 1\r\n")); err != nil {
		t.Fatalf("Error for client 'import' from server: %v", err)
	}

	cfoo, _, _ := newClientForServer(s)
	defer cfoo.close()
	if err := cfoo.registerWithAccount(fooAcc); err != nil {
		t.Fatalf("Error registering client with 'foo' account: %v", err)
	}

	cbar, _, _ := newClientForServer(s)
	defer cbar.close()
	if err := cbar.registerWithAccount(barAcc); err != nil {
		t.Fatalf("Error registering client with 'bar' account: %v", err)
	}

	readMsg := func() {
		t.Helper()
		l, err := crImport.ReadString('\n')
		if err != nil {
			t.Fatalf("Error reading msg header from client 'import': %v", err)
		}
		mraw := msgPat.FindAllStringSubmatch(l, -1)
		if len(mraw) == 0 {
			t.Fatalf("No message received")
		}
		// Consume msg body too.
		if _, err = crImport.ReadString('\n'); err != nil {
			t.Fatalf("Error reading msg body from client 'import': %v", err)
		}
	}

	cbar.parseAsync("PUB test 9\r\nhello-bar\r\n")
	readMsg()

	cfoo.parseAsync("PUB test 9\r\nhello-foo\r\n")
	readMsg()
}

func TestAccountBasicRouteMapping(t *testing.T) {
	opts := DefaultOptions()
	opts.Port = -1
	s := RunServer(opts)
	defer s.Shutdown()

	acc, _ := s.LookupAccount(DEFAULT_GLOBAL_ACCOUNT)
	acc.AddMapping("foo", "bar")

	nc := natsConnect(t, s.ClientURL())
	defer nc.Close()

	fsub, _ := nc.SubscribeSync("foo")
	bsub, _ := nc.SubscribeSync("bar")
	nc.Publish("foo", nil)
	nc.Flush()

	checkPending := func(sub *nats.Subscription, expected int) {
		t.Helper()
		if n, _, _ := sub.Pending(); n != expected {
			t.Fatalf("Expected %d msgs for %q, but got %d", expected, sub.Subject, n)
		}
	}

	checkPending(fsub, 0)
	checkPending(bsub, 1)

	acc.RemoveMapping("foo")

	nc.Publish("foo", nil)
	nc.Flush()

	checkPending(fsub, 1)
	checkPending(bsub, 1)
}

func TestAccountLeftRightMapping(t *testing.T) {
	// left and right are supported by the subject transform since 2.10.16,
	// so account mappings must accept them like the other functions.
	conf := createConfFile(t, []byte(`
		listen: 127.0.0.1:-1
		accounts {
			A {
				users = [{user: a, password: p}]
				mappings = {
					"foo.*": "bar.{{right(1,2)}}"
					"baz.*": "qux.{{left(1,3)}}"
				}
			}
		}
	`))

	s, _ := RunServerWithConfig(conf)
	defer s.Shutdown()

	nc := natsConnect(t, s.ClientURL(), nats.UserInfo("a", "p"))
	defer nc.Close()

	sub, err := nc.SubscribeSync("bar.>")
	require_NoError(t, err)
	sub2, err := nc.SubscribeSync("qux.>")
	require_NoError(t, err)
	require_NoError(t, nc.Flush())

	require_NoError(t, nc.Publish("foo.1234", nil))
	require_NoError(t, nc.Publish("baz.1234", nil))
	require_NoError(t, nc.Flush())

	msg, err := sub.NextMsg(time.Second)
	require_NoError(t, err)
	require_Equal(t, msg.Subject, "bar.34")

	msg, err = sub2.NextMsg(time.Second)
	require_NoError(t, err)
	require_Equal(t, msg.Subject, "qux.123")
}

func TestAccountWildcardRouteMapping(t *testing.T) {
	opts := DefaultOptions()
	opts.Port = -1
	s := RunServer(opts)
	defer s.Shutdown()

	acc, _ := s.LookupAccount(DEFAULT_GLOBAL_ACCOUNT)

	addMap := func(src, dest string) {
		t.Helper()
		if err := acc.AddMapping(src, dest); err != nil {
			t.Fatalf("Error adding mapping: %v", err)
		}
	}

	addMap("foo.*.*", "bar.$2.$1")
	addMap("bar.*.>", "baz.$1.>")

	nc := natsConnect(t, s.ClientURL())
	defer nc.Close()

	pub := func(subj string) {
		t.Helper()
		err := nc.Publish(subj, nil)
		if err == nil {
			err = nc.Flush()
		}
		if err != nil {
			t.Fatalf("Error publishing: %v", err)
		}
	}

	fsub, _ := nc.SubscribeSync("foo.>")
	bsub, _ := nc.SubscribeSync("bar.>")
	zsub, _ := nc.SubscribeSync("baz.>")

	checkPending := func(sub *nats.Subscription, expected int) {
		t.Helper()
		if n, _, _ := sub.Pending(); n != expected {
			t.Fatalf("Expected %d msgs for %q, but got %d", expected, sub.Subject, n)
		}
	}

	pub("foo.1.2")

	checkPending(fsub, 0)
	checkPending(bsub, 1)
	checkPending(zsub, 0)
}

func TestAccountRouteMappingChangesAfterClientStart(t *testing.T) {
	opts := DefaultOptions()
	opts.Port = -1
	s := RunServer(opts)
	defer s.Shutdown()

	// Create the client first then add in mapping.
	nc := natsConnect(t, s.ClientURL())
	defer nc.Close()

	nc.Flush()

	acc, _ := s.LookupAccount(DEFAULT_GLOBAL_ACCOUNT)
	acc.AddMapping("foo", "bar")

	fsub, _ := nc.SubscribeSync("foo")
	bsub, _ := nc.SubscribeSync("bar")
	nc.Publish("foo", nil)
	nc.Flush()

	checkPending := func(sub *nats.Subscription, expected int) {
		t.Helper()
		if n, _, _ := sub.Pending(); n != expected {
			t.Fatalf("Expected %d msgs for %q, but got %d", expected, sub.Subject, n)
		}
	}

	checkPending(fsub, 0)
	checkPending(bsub, 1)

	acc.RemoveMapping("foo")

	nc.Publish("foo", nil)
	nc.Flush()

	checkPending(fsub, 1)
	checkPending(bsub, 1)
}

func TestAccountSimpleWeightedRouteMapping(t *testing.T) {
	opts := DefaultOptions()
	opts.Port = -1
	s := RunServer(opts)
	defer s.Shutdown()

	acc, _ := s.LookupAccount(DEFAULT_GLOBAL_ACCOUNT)
	acc.AddWeightedMappings("foo", NewMapDest("bar", 50))

	nc := natsConnect(t, s.ClientURL())
	defer nc.Close()

	fsub, _ := nc.SubscribeSync("foo")
	bsub, _ := nc.SubscribeSync("bar")

	total := 500
	for i := 0; i < total; i++ {
		nc.Publish("foo", nil)
	}
	nc.Flush()

	fpending, _, _ := fsub.Pending()
	bpending, _, _ := bsub.Pending()

	h := total / 2
	tp := h / 5
	min, max := h-tp, h+tp
	if fpending < min || fpending > max {
		t.Fatalf("Expected about %d msgs, got %d and %d", h, fpending, bpending)
	}
}

func TestAccountMultiWeightedRouteMappings(t *testing.T) {
	opts := DefaultOptions()
	opts.Port = -1
	s := RunServer(opts)
	defer s.Shutdown()

	acc, _ := s.LookupAccount(DEFAULT_GLOBAL_ACCOUNT)

	// Check failures for bad weights.
	shouldErr := func(rds ...*MapDest) {
		t.Helper()
		if acc.AddWeightedMappings("foo", rds...) == nil {
			t.Fatalf("Expected an error, got none")
		}
	}
	shouldNotErr := func(rds ...*MapDest) {
		t.Helper()
		if err := acc.AddWeightedMappings("foo", rds...); err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
	}

	shouldErr(NewMapDest("bar", 150))
	shouldNotErr(NewMapDest("bar", 50))
	shouldNotErr(NewMapDest("bar", 50), NewMapDest("baz", 50))
	// Same dest duplicated should error.
	shouldErr(NewMapDest("bar", 50), NewMapDest("bar", 50))
	// total over 100
	shouldErr(NewMapDest("bar", 50), NewMapDest("baz", 60))

	acc.RemoveMapping("foo")

	// 20 for original, you can leave it off will be auto-added.
	shouldNotErr(NewMapDest("bar", 50), NewMapDest("baz", 30))

	nc := natsConnect(t, s.ClientURL())
	defer nc.Close()

	fsub, _ := nc.SubscribeSync("foo")
	bsub, _ := nc.SubscribeSync("bar")
	zsub, _ := nc.SubscribeSync("baz")

	// For checking later.
	rds := []struct {
		sub *nats.Subscription
		w   uint8
	}{
		{fsub, 20},
		{bsub, 50},
		{zsub, 30},
	}

	total := 5000
	for i := 0; i < total; i++ {
		nc.Publish("foo", nil)
	}
	nc.Flush()

	for _, rd := range rds {
		pending, _, _ := rd.sub.Pending()
		expected := total / int(100/rd.w)
		tp := expected / 5 // 20%
		min, max := expected-tp, expected+tp
		if pending < min || pending > max {
			t.Fatalf("Expected about %d msgs for %q, got %d", expected, rd.sub.Subject, pending)
		}
	}
}

func TestGlobalAccountRouteMappingsConfiguration(t *testing.T) {
	cf := createConfFile(t, []byte(`
	port: -1
	mappings = {
		foo: bar
		foo.*: [ { dest: bar.v1.$1, weight: 40% }, { destination: baz.v2.$1, weight: 20 } ]
		bar.*.*: RAB.$2.$1
    }
    `))

	s, _ := RunServerWithConfig(cf)
	defer s.Shutdown()

	nc := natsConnect(t, s.ClientURL())
	defer nc.Close()

	bsub, _ := nc.SubscribeSync("bar")
	fsub1, _ := nc.SubscribeSync("bar.v1.>")
	fsub2, _ := nc.SubscribeSync("baz.v2.>")
	zsub, _ := nc.SubscribeSync("RAB.>")
	f22sub, _ := nc.SubscribeSync("foo.*")

	checkPending := func(sub *nats.Subscription, expected int) {
		t.Helper()
		if n, _, _ := sub.Pending(); n != expected {
			t.Fatalf("Expected %d msgs for %q, but got %d", expected, sub.Subject, n)
		}
	}

	nc.Publish("foo", nil)
	nc.Publish("bar.11.22", nil)

	total := 500
	for i := 0; i < total; i++ {
		nc.Publish("foo.22", nil)
	}
	nc.Flush()

	checkPending(bsub, 1)
	checkPending(zsub, 1)

	fpending, _, _ := f22sub.Pending()
	fpending1, _, _ := fsub1.Pending()
	fpending2, _, _ := fsub2.Pending()

	if fpending1 < fpending2 || fpending < fpending2 {
		t.Fatalf("Loadbalancing seems off for the foo.* mappings: %d and %d and %d", fpending, fpending1, fpending2)
	}
}

func TestAccountRouteMappingsConfiguration(t *testing.T) {
	cf := createConfFile(t, []byte(`
	port: -1
	accounts {
		synadia {
			users = [{user: derek, password: foo}]
			mappings = {
				foo: bar
				foo.*: [ { dest: bar.v1.$1, weight: 40% }, { destination: baz.v2.$1, weight: 20 } ]
				bar.*.*: RAB.$2.$1
		    }
		}
	}
    `))

	s, _ := RunServerWithConfig(cf)
	defer s.Shutdown()

	// We test functionality above, so for this one just make sure we have mappings for the account.
	acc, _ := s.LookupAccount("synadia")
	if !acc.hasMappings() {
		t.Fatalf("Account %q does not have mappings", "synadia")
	}

	az, err := s.Accountz(&AccountzOptions{"synadia"})
	if err != nil {
		t.Fatalf("Error getting Accountz: %v", err)
	}
	if az.Account == nil {
		t.Fatalf("Expected an Account")
	}
	if len(az.Account.Mappings) != 3 {
		t.Fatalf("Expected %d mappings, saw %d", 3, len(az.Account.Mappings))
	}
}

func TestAccountRouteMappingsWithLossInjection(t *testing.T) {
	cf := createConfFile(t, []byte(`
	port: -1
	mappings = {
		foo: { dest: foo, weight: 80% }
		bar: { dest: bar, weight: 0% }
    }
    `))

	s, _ := RunServerWithConfig(cf)
	defer s.Shutdown()

	nc := natsConnect(t, s.ClientURL())
	defer nc.Close()

	sub, _ := nc.SubscribeSync("foo")

	total := 1000
	for i := 0; i < total; i++ {
		nc.Publish("foo", nil)
	}
	nc.Flush()

	if pending, _, _ := sub.Pending(); pending == total {
		t.Fatalf("Expected some loss and pending to not be same as sent")
	}

	sub, _ = nc.SubscribeSync("bar")
	for i := 0; i < total; i++ {
		nc.Publish("bar", nil)
	}
	nc.Flush()

	if pending, _, _ := sub.Pending(); pending != 0 {
		t.Fatalf("Expected all messages to be dropped and pending to be 0, got %d", pending)
	}
}

func TestAccountRouteMappingsWithOriginClusterFilter(t *testing.T) {
	cf := createConfFile(t, []byte(`
	port: -1
	mappings = {
		foo: { dest: bar, cluster: SYN, weight: 100% }
    }
    `))

	s, _ := RunServerWithConfig(cf)
	defer s.Shutdown()

	nc := natsConnect(t, s.ClientURL())
	defer nc.Close()

	sub, _ := nc.SubscribeSync("foo")

	total := 1000
	for i := 0; i < total; i++ {
		nc.Publish("foo", nil)
	}
	nc.Flush()

	if pending, _, _ := sub.Pending(); pending != total {
		t.Fatalf("Expected pending to be %d, got %d", total, pending)
	}

	s.setClusterName("SYN")
	sub, _ = nc.SubscribeSync("bar")
	for i := 0; i < total; i++ {
		nc.Publish("foo", nil)
	}
	nc.Flush()

	if pending, _, _ := sub.Pending(); pending != total {
		t.Fatalf("Expected pending to be %d, got %d", total, pending)
	}
}

func TestAccountServiceImportWithRouteMappings(t *testing.T) {
	cf := createConfFile(t, []byte(`
	port: -1
    accounts {
      foo {
        users = [{user: derek, password: foo}]
        exports = [{service: "request"}]
      }
      bar {
        users = [{user: ivan, password: bar}]
        imports = [{service: {account: "foo", subject:"request"}}]
      }
    }
    `))

	s, opts := RunServerWithConfig(cf)
	defer s.Shutdown()

	acc, _ := s.LookupAccount("foo")
	acc.AddMapping("request", "request.v2")

	// Create the service client first.
	ncFoo := natsConnect(t, fmt.Sprintf("nats://derek:foo@%s:%d", opts.Host, opts.Port))
	defer ncFoo.Close()

	fooSub := natsSubSync(t, ncFoo, "request.v2")
	ncFoo.Flush()

	// Requestor
	ncBar := natsConnect(t, fmt.Sprintf("nats://ivan:bar@%s:%d", opts.Host, opts.Port))
	defer ncBar.Close()

	ncBar.Publish("request", nil)
	ncBar.Flush()

	checkFor(t, time.Second, 10*time.Millisecond, func() error {
		if n, _, _ := fooSub.Pending(); n != 1 {
			return fmt.Errorf("Expected a request for %q, but got %d", fooSub.Subject, n)
		}
		return nil
	})
}

func TestAccountImportsWithWildcardSupport(t *testing.T) {
	cf := createConfFile(t, []byte(`
	port: -1
    accounts {
      foo {
        users = [{user: derek, password: foo}]
        exports = [
          { service: "request.*" }
          { stream: "events.>" }
          { stream: "info.*.*.>" }
        ]
      }
      bar {
        users = [{user: ivan, password: bar}]
        imports = [
          { service: {account: "foo", subject:"request.*"}, to:"my.request.*"}
          { stream:  {account: "foo", subject:"events.>"}, to:"foo.events.>"}
          { stream:  {account: "foo", subject:"info.*.*.>"}, to:"foo.info.$2.$1.>"}
        ]
      }
    }
    `))

	s, opts := RunServerWithConfig(cf)
	defer s.Shutdown()

	ncFoo := natsConnect(t, fmt.Sprintf("nats://derek:foo@%s:%d", opts.Host, opts.Port))
	defer ncFoo.Close()

	ncBar := natsConnect(t, fmt.Sprintf("nats://ivan:bar@%s:%d", opts.Host, opts.Port))
	defer ncBar.Close()

	// Create subscriber for the service endpoint in foo.
	_, err := ncFoo.QueueSubscribe("request.*", "t22", func(m *nats.Msg) {
		if m.Subject != "request.22" {
			t.Fatalf("Expected literal subject for request, got %q", m.Subject)
		}
		m.Respond([]byte("yes!"))
	})
	if err != nil {
		t.Fatalf("Error on subscribe: %v", err)
	}
	ncFoo.Flush()

	// Now test service import.
	resp, err := ncBar.Request("my.request.22", []byte("yes?"), time.Second)
	if err != nil {
		t.Fatalf("Expected a response")
	}
	if string(resp.Data) != "yes!" {
		t.Fatalf("Expected a response of %q, got %q", "yes!", resp.Data)
	}

	// Now test stream imports.
	esub, _ := ncBar.SubscribeSync("foo.events.*") // subset
	isub, _ := ncBar.SubscribeSync("foo.info.>")
	ncBar.Flush()

	// Now publish some stream events.
	ncFoo.Publish("events.22", nil)
	ncFoo.Publish("info.11.22.bar", nil)
	ncFoo.Flush()

	checkPending := func(sub *nats.Subscription, expected int) {
		t.Helper()
		checkFor(t, time.Second, 10*time.Millisecond, func() error {
			if n, _, _ := sub.Pending(); n != expected {
				return fmt.Errorf("Expected %d msgs for %q, but got %d", expected, sub.Subject, n)
			}
			return nil
		})
	}

	checkPending(esub, 1)
	checkPending(isub, 1)

	// Now check to make sure the subjects are correct etc.
	m, err := esub.NextMsg(time.Second)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if m.Subject != "foo.events.22" {
		t.Fatalf("Incorrect subject for stream import, expected %q, got %q", "foo.events.22", m.Subject)
	}

	m, err = isub.NextMsg(time.Second)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if m.Subject != "foo.info.22.11.bar" {
		t.Fatalf("Incorrect subject for stream import, expected %q, got %q", "foo.info.22.11.bar", m.Subject)
	}
}

// duplicates TestJWTAccountImportsWithWildcardSupport (jwt_test.go) in config
func TestAccountImportsWithWildcardSupportStreamAndService(t *testing.T) {
	cf := createConfFile(t, []byte(`
	port: -1
    accounts {
      foo {
        users = [{user: derek, password: foo}]
        exports = [
          { service: "$request.*.$in.*.>" }
          { stream: "$events.*.$in.*.>" }
        ]
      }
      bar {
        users = [{user: ivan, password: bar}]
        imports = [
          { service: {account: "foo", subject:"$request.*.$in.*.>"}, to:"my.request.$2.$1.>"}
          { stream:  {account: "foo", subject:"$events.*.$in.*.>"}, to:"my.events.$2.$1.>"}
        ]
      }
    }
    `))

	s, opts := RunServerWithConfig(cf)
	defer s.Shutdown()

	ncFoo := natsConnect(t, fmt.Sprintf("nats://derek:foo@%s:%d", opts.Host, opts.Port))
	defer ncFoo.Close()

	ncBar := natsConnect(t, fmt.Sprintf("nats://ivan:bar@%s:%d", opts.Host, opts.Port))
	defer ncBar.Close()

	// Create subscriber for the service endpoint in foo.
	_, err := ncFoo.Subscribe("$request.>", func(m *nats.Msg) {
		if m.Subject != "$request.2.$in.1.bar" {
			t.Fatalf("Expected literal subject for request, got %q", m.Subject)
		}
		m.Respond([]byte("yes!"))
	})
	if err != nil {
		t.Fatalf("Error on subscribe: %v", err)
	}
	ncFoo.Flush()

	// Now test service import.
	if resp, err := ncBar.Request("my.request.1.2.bar", []byte("yes?"), time.Second); err != nil {
		t.Fatalf("Expected a response")
	} else if string(resp.Data) != "yes!" {
		t.Fatalf("Expected a response of %q, got %q", "yes!", resp.Data)
	}
	subBar, err := ncBar.SubscribeSync("my.events.>")
	if err != nil {
		t.Fatalf("Expected a response")
	}
	ncBar.Flush()

	ncFoo.Publish("$events.1.$in.2.bar", nil)

	m, err := subBar.NextMsg(time.Second)
	if err != nil {
		t.Fatalf("Expected a response")
	}
	if m.Subject != "my.events.2.1.bar" {
		t.Fatalf("Expected literal subject for request, got %q", m.Subject)
	}
}

func BenchmarkNewRouteReply(b *testing.B) {
	opts := defaultServerOptions
	s := New(&opts)
	g := s.globalAccount()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		g.newServiceReply(false)
	}
}

func TestSamplingHeader(t *testing.T) {
	test := func(expectSampling bool, h http.Header) {
		t.Helper()
		b := strings.Builder{}
		b.WriteString("\r\n") // simulate status line
		h.Write(&b)
		b.WriteString("\r\n")
		hdrString := b.String()
		c := &client{parseState: parseState{msgBuf: []byte(hdrString), pa: pubArg{hdr: len(hdrString)}}}
		sample, hdr := shouldSample(&serviceLatency{0, "foo"}, c)
		if expectSampling {
			if !sample {
				t.Fatal("Expected to sample")
			} else if hdr == nil {
				t.Fatal("Expected a header")
			}
			for k, v := range h {
				if hdr.Get(k) != v[0] {
					t.Fatal("Expect header to match")
				}
			}
		} else {
			if sample {
				t.Fatal("Expected not to sample")
			} else if hdr != nil {
				t.Fatal("Expected no header")
			}
		}
	}

	test(false, http.Header{"Uber-Trace-Id": []string{"0:0:0:0"}})
	test(false, http.Header{"Uber-Trace-Id": []string{"0:0:0:00"}}) // one byte encoded as two hex digits
	test(true, http.Header{"Uber-Trace-Id": []string{"0:0:0:1"}})
	test(true, http.Header{"Uber-Trace-Id": []string{"0:0:0:01"}})
	test(true, http.Header{"Uber-Trace-Id": []string{"0:0:0:5"}}) // debug and sample
	test(true, http.Header{"Uber-Trace-Id": []string{"479fefe9525eddb:5adb976bfc1f95c1:479fefe9525eddb:1"}})
	test(true, http.Header{"Uber-Trace-Id": []string{"479fefe9525eddb:479fefe9525eddb:0:1"}})
	test(false, http.Header{"Uber-Trace-Id": []string{"479fefe9525eddb:5adb976bfc1f95c1:479fefe9525eddb:0"}})
	test(false, http.Header{"Uber-Trace-Id": []string{"479fefe9525eddb:479fefe9525eddb:0:0"}})

	test(true, http.Header{"X-B3-Sampled": []string{"1"}})
	test(false, http.Header{"X-B3-Sampled": []string{"0"}})
	test(true, http.Header{"X-B3-TraceId": []string{"80f198ee56343ba864fe8b2a57d3eff7"}}) // decision left to recipient
	test(false, http.Header{"X-B3-TraceId": []string{"80f198ee56343ba864fe8b2a57d3eff7"}, "X-B3-Sampled": []string{"0"}})
	test(true, http.Header{"X-B3-TraceId": []string{"80f198ee56343ba864fe8b2a57d3eff7"}, "X-B3-Sampled": []string{"1"}})

	test(false, http.Header{"B3": []string{"0"}}) // deny only
	test(false, http.Header{"B3": []string{"0-0-0-0"}})
	test(false, http.Header{"B3": []string{"0-0-0"}})
	test(true, http.Header{"B3": []string{"0-0-1-0"}})
	test(true, http.Header{"B3": []string{"0-0-1"}})
	test(true, http.Header{"B3": []string{"0-0-d"}}) // debug is not a deny
	test(true, http.Header{"B3": []string{"80f198ee56343ba864fe8b2a57d3eff7-e457b5a2e4d86bd1-1"}})
	test(true, http.Header{"B3": []string{"80f198ee56343ba864fe8b2a57d3eff7-e457b5a2e4d86bd1-1-05e3ac9a4f6e3b90"}})
	test(false, http.Header{"B3": []string{"80f198ee56343ba864fe8b2a57d3eff7-e457b5a2e4d86bd1-0-05e3ac9a4f6e3b90"}})

	test(true, http.Header{"traceparent": []string{"00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"}})
	test(true, http.Header{"traceparent": []string{"00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-27"}})
	test(false, http.Header{"traceparent": []string{"00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-00"}})
	test(false, http.Header{"traceparent": []string{"00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-22"}})
}

func TestAccountSystemPermsWithGlobalAccess(t *testing.T) {
	conf := createConfFile(t, []byte(`
		listen: 127.0.0.1:-1
		accounts {
			$SYS { users = [ { user: "admin", pass: "s3cr3t!" } ] }
		}
	`))

	s, _ := RunServerWithConfig(conf)
	defer s.Shutdown()

	// Make sure we can connect with no auth to global account as normal.
	nc, err := nats.Connect(s.ClientURL())
	if err != nil {
		t.Fatalf("Failed to create client: %v", err)
	}
	defer nc.Close()

	// Make sure we can connect to the system account with correct credentials.
	sc, err := nats.Connect(s.ClientURL(), nats.UserInfo("admin", "s3cr3t!"))
	if err != nil {
		t.Fatalf("Failed to create system client: %v", err)
	}
	defer sc.Close()
}

const importSubscriptionOverlapTemplate = `
listen: 127.0.0.1:-1
accounts: {
  ACCOUNT_X: {
	users: [
	  {user: publisher}
	]
	exports: [
	  {stream: %s}
	]
  },
  ACCOUNT_Y: {
	users: [
	  {user: subscriber}
	]
	imports: [
	  {stream: {account: ACCOUNT_X, subject: %s }, %s}
	]
  }
}`

func TestImportSubscriptionPartialOverlapWithPrefix(t *testing.T) {
	cf := createConfFile(t, []byte(fmt.Sprintf(importSubscriptionOverlapTemplate, ">", ">", "prefix: myprefix")))

	s, opts := RunServerWithConfig(cf)
	defer s.Shutdown()

	ncX := natsConnect(t, fmt.Sprintf("nats://%s:%s@127.0.0.1:%d", "publisher", "", opts.Port))
	defer ncX.Close()

	ncY := natsConnect(t, fmt.Sprintf("nats://%s:%s@127.0.0.1:%d", "subscriber", "", opts.Port))
	defer ncY.Close()

	for _, subj := range []string{">", "myprefix.*", "myprefix.>", "myprefix.test", "*.>", "*.*", "*.test"} {
		t.Run(subj, func(t *testing.T) {
			sub, err := ncY.SubscribeSync(subj)
			sub.AutoUnsubscribe(1)
			require_NoError(t, err)
			require_NoError(t, ncY.Flush())

			ncX.Publish("test", []byte("hello"))

			m, err := sub.NextMsg(time.Second)
			require_NoError(t, err)
			require_True(t, string(m.Data) == "hello")
		})
	}
}

func TestImportSubscriptionPartialOverlapWithTransform(t *testing.T) {
	cf := createConfFile(t, []byte(fmt.Sprintf(importSubscriptionOverlapTemplate, "*.*.>", "*.*.>", "to: myprefix.$2.$1.>")))

	s, opts := RunServerWithConfig(cf)
	defer s.Shutdown()

	ncX := natsConnect(t, fmt.Sprintf("nats://%s:%s@127.0.0.1:%d", "publisher", "", opts.Port))
	defer ncX.Close()

	ncY := natsConnect(t, fmt.Sprintf("nats://%s:%s@127.0.0.1:%d", "subscriber", "", opts.Port))
	defer ncY.Close()

	for _, subj := range []string{">", "*.*.*.>", "*.2.*.>", "*.*.1.>", "*.2.1.>", "*.*.*.*", "*.2.1.*", "*.*.*.test",
		"*.*.1.test", "*.2.*.test", "*.2.1.test", "myprefix.*.*.*", "myprefix.>", "myprefix.*.>", "myprefix.*.*.>",
		"myprefix.2.>", "myprefix.2.1.>", "myprefix.*.1.>", "myprefix.2.*.>", "myprefix.2.1.*", "myprefix.*.*.test",
		"myprefix.2.1.test"} {
		t.Run(subj, func(t *testing.T) {
			sub, err := ncY.SubscribeSync(subj)
			sub.AutoUnsubscribe(1)
			require_NoError(t, err)
			require_NoError(t, ncY.Flush())

			ncX.Publish("1.2.test", []byte("hello"))

			m, err := sub.NextMsg(time.Second)
			require_NoError(t, err)
			require_True(t, string(m.Data) == "hello")
			require_Equal(t, m.Subject, "myprefix.2.1.test")
		})
	}
}

func TestAccountLimitsServerConfig(t *testing.T) {
	cf := createConfFile(t, []byte(`
	port: -1
	max_connections: 10
	accounts {
		MAXC {
			users = [{user: derek, password: foo}]
			limits {
				max_connections: 5
				max_subs: 10
				max_payload: 32k
				max_leafnodes: 1
			}
		}
	}
    `))

	s, _ := RunServerWithConfig(cf)
	defer s.Shutdown()

	acc, err := s.lookupAccount("MAXC")
	require_NoError(t, err)

	if mc := acc.MaxActiveConnections(); mc != 5 {
		t.Fatalf("Did not set MaxActiveConnections properly, expected 5, got %d", mc)
	}
	if mlc := acc.MaxActiveLeafNodes(); mlc != 1 {
		t.Fatalf("Did not set MaxActiveLeafNodes properly, expected 1, got %d", mlc)
	}

	// Do quick test on connections, but if they are registered above should be good.
	for i := 0; i < 5; i++ {
		c, err := nats.Connect(s.ClientURL(), nats.UserInfo("derek", "foo"))
		require_NoError(t, err)
		defer c.Close()
	}

	// Should fail.
	_, err = nats.Connect(s.ClientURL(), nats.UserInfo("derek", "foo"))
	require_Error(t, err)
}

// Connections being closed should be the newer ones in case of JWT limits.
func TestAccountMaxConnectionsDisconnectsNewestFirst(t *testing.T) {
	cf := createConfFile(t, []byte(`
        port: -1
        server_name: A
        accounts {
                TEST {
                        users = [
                          {user: user1, password: foo}
                          {user: user2, password: foo}
                          {user: user3, password: foo}
                        ]
                        limits {
                                max_connections: 3
                        }
                }
        }
        `))
	s, _ := RunServerWithConfig(cf)
	defer s.Shutdown()

	var conns []*nats.Conn

	disconnects := make([]chan error, 0)
	for i := 1; i <= 3; i++ {
		disconnectCh := make(chan error)
		c, err := nats.Connect(
			s.ClientURL(),
			nats.UserInfo(fmt.Sprintf("user%d", i), "foo"),
			nats.DisconnectErrHandler(func(_ *nats.Conn, err error) {
				disconnectCh <- err
			}),
			nats.NoReconnect(),
		)
		require_NoError(t, err)
		defer c.Close()
		conns = append(conns, c)
		disconnects = append(disconnects, disconnectCh)
		// Small delay to ensure distinct start times.
		time.Sleep(10 * time.Millisecond)
	}
	acc, err := s.lookupAccount("TEST")
	require_NoError(t, err)
	require_Equal(t, acc.NumConnections(), 3)

	// Force account update to trigger connection limit enforcement.
	accClaims := jwt.NewAccountClaims(acc.Name)
	accClaims.Limits.Conn = 2
	s.UpdateAccountClaims(acc, accClaims)

	// Wait for disconnections from the most recent client.
	disconnectCh := disconnects[2]
	select {
	case <-disconnectCh:
	case <-time.After(2 * time.Second):
		t.Fatal("Expected newest connection to disconnect!")
	}

	// Check which connections are still active
	// The newest connection (last one created) should be disconnected first.
	activeConnections := 0
	var connected []int
	for i, conn := range conns {
		if !conn.IsClosed() {
			activeConnections++
			connected = append(connected, i)
		}
	}
	require_Equal(t, activeConnections, 2)
	require_Equal(t, len(connected), 2)

	// The first two connections should still be connected.
	require_Equal(t, connected[0], 0)
	require_Equal(t, connected[1], 1)

	// The newest connection should be closed.
	require_True(t, conns[2].IsClosed())
}

func TestAccountUpdateRemoteServerDisconnectsNewestFirst(t *testing.T) {
	cf := createConfFile(t, []byte(`
          port: -1
          accounts {
            TEST {
              users = [{user: dummy, password: foo}]
              limits {
                max_connections: 5
              }
            }
          }
        `))

	s, _ := RunServerWithConfig(cf)
	defer s.Shutdown()

	acc, err := s.lookupAccount("TEST")
	require_NoError(t, err)

	for i := 0; i < 3; i++ {
		c, err := nats.Connect(s.ClientURL(), nats.UserInfo("dummy", "foo"))
		require_NoError(t, err)
		defer c.Close()

		// Small delay to ensure distinct start times.
		time.Sleep(10 * time.Millisecond)
	}
	require_Equal(t, acc.NumConnections(), 3)

	// Simulate remote server reporting connections that would exceed the limit.
	// Remote has 4 + we have 3, meaning that 2 will have to be disconnected.
	remoteServerMsg := &AccountNumConns{
		Server: ServerInfo{
			ID:   "fake-server-1",
			Name: "fake-nats-1",
		},
		AccountStat: AccountStat{
			Account: "TEST",
			Conns:   4,
		},
	}
	toDisconnect := acc.updateRemoteServer(remoteServerMsg)

	// Should want to disconnect 2 clients (7 total - 5 max conns limit = 2 over).
	require_Equal(t, len(toDisconnect), 2)

	// Verify the returned clients are in reverse chronological order.
	var startTimes []time.Time
	for _, c := range toDisconnect {
		startTimes = append(startTimes, c.start)
	}
	if !startTimes[0].After(startTimes[1]) {
		t.Fatalf("Expected clients to be in reverse chronological order: %v is before %v", startTimes[0], startTimes[1])
	}

	// The clients to disconnect should be the newest ones we created.
	disconnected := make(map[uint64]bool)
	for _, c := range toDisconnect {
		disconnected[c.cid] = true
	}

	// Get all current clients and find the oldest ones.
	allClients := acc.getClients()
	byStartTime := make([]*client, len(allClients))
	copy(byStartTime, allClients)
	slices.SortFunc(byStartTime, func(i, j *client) int {
		return i.start.Compare(j.start)
	})

	// The last two clients in the sorted list should be the ones selected for disconnection.
	require_False(t, disconnected[byStartTime[0].cid])
	require_True(t, disconnected[byStartTime[1].cid])
	require_True(t, disconnected[byStartTime[2].cid])
}

func TestAccountMaxConnectionsDuringLameDuckMode(t *testing.T) {
	cf := createConfFile(t, []byte(`
        port: -1
        accounts {
                TEST {
                        users = [
                          {user: user, password: user}
                        ]
                        limits {
                                max_connections: 3
                        }
                }
        }
		cluster {
		  listen: 127.0.0.1:7248
		  name: "abc"
		}
		no_auth_user: user
		lame_duck_grace_period: 1s
        `))

	optsA, err := ProcessConfigFile(cf)
	require_NoError(t, err)
	optsA.NoSigs, optsA.NoLog = true, true
	optsA.ServerName = "A"
	srvA := RunServer(optsA)
	defer srvA.Shutdown()
	optsB := nextServerOpts(optsA)
	optsB.Routes = RoutesFromStr(fmt.Sprintf("nats://%s:%d", optsA.Cluster.Host, optsA.Cluster.Port))
	optsB.ServerName = "B"
	srvB := RunServer(optsB)
	defer srvB.Shutdown()

	checkClusterFormed(t, srvA, srvB)

	disconnects := make([]chan error, 0)
	for range 3 {
		disconnectCh := make(chan error)
		c, err := nats.Connect(
			srvA.ClientURL(),
			nats.UserInfo("user", "user"),
			nats.DisconnectErrHandler(func(c *nats.Conn, err error) {
				disconnectCh <- err
			}),
		)
		require_NoError(t, err)
		defer c.Close()
		disconnects = append(disconnects, disconnectCh)
		// Small delay to ensure distinct start times.
		time.Sleep(10 * time.Millisecond)
	}
	acc, err := srvA.lookupAccount("TEST")
	require_NoError(t, err)
	require_Equal(t, acc.NumConnections(), 3)

	go srvA.LameDuckShutdown()

	for _, disconnectCh := range disconnects {
		select {
		case err := <-disconnectCh:
			require_Equal(t, err.Error(), "EOF")
		case <-time.After(5 * time.Second):
			t.Fatal("Expected LDM reconnect")
		}
	}
}

func TestAccountMaxConnectionsRejectsPipelinedOps(t *testing.T) {
	cf := createConfFile(t, []byte(`
        port: -1
        no_auth_user: user
        accounts {
                TEST {
                        users = [ {user: user, password: pwd} ]
                        limits {
                                max_connections: 1
                        }
                }
        }
        `))
	s, _ := RunServerWithConfig(cf)
	defer s.Shutdown()

	// Observer takes the only slot.
	nc := natsConnect(t, s.ClientURL())
	defer nc.Close()
	sub := natsSubSync(t, nc, "foo")
	natsFlush(t, nc)

	// Raw client sends CONNECT with pipelined PUB in a single write.
	c, err := net.Dial("tcp", s.Addr().String())
	require_NoError(t, err)
	defer c.Close()
	require_NoError(t, c.SetDeadline(time.Now().Add(2*time.Second)))
	r := bufio.NewReader(c)
	info, err := r.ReadString('\n')
	require_NoError(t, err)
	require_True(t, strings.HasPrefix(info, "INFO"))
	_, err = c.Write([]byte("CONNECT {}\r\nPUB foo 2\r\nhi\r\n"))
	require_NoError(t, err)

	// Only the rejection error should come back before the server closes the connection.
	l, err := r.ReadString('\n')
	require_NoError(t, err)
	require_Contains(t, l, ErrTooManyAccountConnections.Error())
	_, err = r.ReadString('\n')
	require_Error(t, err, io.EOF)

	if m, err := sub.NextMsg(250 * time.Millisecond); err == nil {
		t.Fatalf("Pipelined PUB was delivered: %q", m.Data)
	}
}

func TestAccountUserSubPermsWithQueueGroups(t *testing.T) {
	cf := createConfFile(t, []byte(`
	listen: 127.0.0.1:-1

	authorization {
	users = [
		{ user: user, password: "pass",
			permissions: {
				publish: "foo.restricted"
				subscribe: { allow: "foo.>", deny: "foo.restricted" }
				allow_responses: { max: 1, ttl: 0s }
			}
		}
	]}
    `))

	s, _ := RunServerWithConfig(cf)
	defer s.Shutdown()

	nc, err := nats.Connect(s.ClientURL(), nats.UserInfo("user", "pass"))
	require_NoError(t, err)
	defer nc.Close()

	// qsub solo.
	qsub, err := nc.QueueSubscribeSync("foo.>", "qg")
	require_NoError(t, err)

	err = nc.Publish("foo.restricted", []byte("RESTRICTED"))
	require_NoError(t, err)
	nc.Flush()

	// Expect no msgs.
	checkSubsPending(t, qsub, 0)
}

func TestAccountImportCycle(t *testing.T) {
	tmpl := `
	port: -1
	accounts: {
		CP: {
			users: [
				{user: cp, password: cp},
			],
			exports: [
				{service: "q1.>", response_type: Singleton},
				{service: "q2.>", response_type: Singleton},
				%s
			],
		},
		A: {
			users: [
				{user: a, password: a},
			],
			imports: [
				{service: {account: CP, subject: "q1.>"}},
				{service: {account: CP, subject: "q2.>"}},
				%s
			]
		},
	}
	`
	cf := createConfFile(t, []byte(fmt.Sprintf(tmpl, _EMPTY_, _EMPTY_)))
	s, _ := RunServerWithConfig(cf)
	defer s.Shutdown()
	ncCp, err := nats.Connect(s.ClientURL(), nats.UserInfo("cp", "cp"))
	require_NoError(t, err)
	defer ncCp.Close()
	ncA, err := nats.Connect(s.ClientURL(), nats.UserInfo("a", "a"))
	require_NoError(t, err)
	defer ncA.Close()
	// setup responder
	natsSub(t, ncCp, "q1.>", func(m *nats.Msg) { m.Respond([]byte("reply")) })
	// setup requestor
	ib := "q2.inbox"
	subAResp, err := ncA.SubscribeSync(ib)
	ncA.Flush()
	require_NoError(t, err)
	req := func() {
		t.Helper()
		// send request
		err = ncA.PublishRequest("q1.a", ib, []byte("test"))
		ncA.Flush()
		require_NoError(t, err)
		mRep, err := subAResp.NextMsg(time.Second)
		require_NoError(t, err)
		require_Equal(t, string(mRep.Data), "reply")
	}
	req()

	// Update the config and do a config reload and make sure it all still work
	changeCurrentConfigContentWithNewContent(t, cf, []byte(
		fmt.Sprintf(tmpl, `{service: "q3.>", response_type: Singleton},`, `{service: {account: CP, subject: "q3.>"}},`)))
	err = s.Reload()
	require_NoError(t, err)
	req()
}

func TestAccountImportOwnExport(t *testing.T) {
	conf := createConfFile(t, []byte(`
		port: -1
		accounts: {
			A: {
			exports: [
				{ service: echo, accounts: [A], latency: { subject: "latency.echo" } }
			],
			imports: [
				{ service: { account: A, subject: echo } }
			]

			users: [
				{ user: user, pass: pass }
			]
			}
		}
	`))
	s, _ := RunServerWithConfig(conf)
	defer s.Shutdown()

	nc := natsConnect(t, s.ClientURL(), nats.UserInfo("user", "pass"))
	defer nc.Close()

	natsSub(t, nc, "echo", func(m *nats.Msg) { m.Respond(nil) })
	_, err := nc.Request("echo", []byte("request"), time.Second)
	require_NoError(t, err)
}

// Test for a bug that would cause duplicate deliveries in certain situations when
// service export/imports and leafnodes involved.
// https://github.com/nats-io/nats-server/issues/3191
func TestAccountImportDuplicateResponseDeliveryWithLeafnodes(t *testing.T) {
	conf := createConfFile(t, []byte(`
		port: -1
		accounts: {
			A: {
				users = [{user: A, password: P}]
				exports: [ { service: "foo", response_type: stream } ]
			}
			B: {
				users = [{user: B, password: P}]
				imports: [ { service: {account: "A", subject:"foo"} } ]
			}
		}
		leaf { listen: "127.0.0.1:17222" }
	`))
	s, _ := RunServerWithConfig(conf)
	defer s.Shutdown()

	// Requestors will connect to account B.
	nc, err := nats.Connect(s.ClientURL(), nats.UserInfo("B", "P"))
	require_NoError(t, err)
	defer nc.Close()

	// By sending a request (regardless of no responders), this will trigger a wildcard _R_ subscription since
	// we do not have a leafnode connected.
	nc.PublishRequest("foo", "reply", nil)
	nc.Flush()

	// Now connect the LN. This will be where the service responder lives.
	conf = createConfFile(t, []byte(`
		port: -1
		leaf {
			remotes [ { url: "nats://A:P@127.0.0.1:17222" } ]
		}
	`))
	ln, _ := RunServerWithConfig(conf)
	defer ln.Shutdown()
	checkLeafNodeConnected(t, s)

	// Now attach a responder to the LN.
	lnc, err := nats.Connect(ln.ClientURL())
	require_NoError(t, err)
	defer lnc.Close()

	lnc.Subscribe("foo", func(m *nats.Msg) {
		m.Respond([]byte("bar"))
	})
	lnc.Flush()
	checkSubInterest(t, s, "A", "foo", time.Second)

	// Make sure it works, but request only wants one, so need second test to show failure, but
	// want to make sure we are wired up correctly.
	_, err = nc.Request("foo", nil, time.Second)
	require_NoError(t, err)

	// Now setup inbox reply so we can check if we get multiple responses.
	reply := nats.NewInbox()
	sub, err := nc.SubscribeSync(reply)
	require_NoError(t, err)

	nc.PublishRequest("foo", reply, nil)

	// Do another to make sure we know the other request will have been processed too.
	_, err = nc.Request("foo", nil, time.Second)
	require_NoError(t, err)

	if n, _, _ := sub.Pending(); n > 1 {
		t.Fatalf("Expected only 1 response, got %d", n)
	}
}

func TestAccountReloadServiceImportPanic(t *testing.T) {
	conf := createConfFile(t, []byte(`
		listen: 127.0.0.1:-1
		accounts {
			A {
				users = [ { user: "a", pass: "p" } ]
				exports [ { service: "HELP" } ]
			}
			B {
				users = [ { user: "b", pass: "p" } ]
				imports [ { service: { account: A, subject: "HELP"} } ]
			}
			$SYS { users = [ { user: "admin", pass: "s3cr3t!" } ] }
		}
	`))

	s, _ := RunServerWithConfig(conf)
	defer s.Shutdown()

	// Now connect up the subscriber for HELP. No-op for this test.
	nc, _ := jsClientConnect(t, s, nats.UserInfo("a", "p"))
	defer nc.Close()

	_, err := nc.Subscribe("HELP", func(m *nats.Msg) { m.Respond([]byte("OK")) })
	require_NoError(t, err)
	// Make sure the subscription is processed before using a new connection to publish.
	natsFlush(t, nc)

	// Now create connection to account b where we will publish to HELP.
	nc, _ = jsClientConnect(t, s, nats.UserInfo("b", "p"))
	defer nc.Close()

	// We want to continually be publishing messages that will trigger the service import while calling reload.
	done := make(chan bool)
	var wg sync.WaitGroup
	wg.Add(1)

	var requests, responses atomic.Uint64
	reply := nats.NewInbox()
	_, err = nc.Subscribe(reply, func(m *nats.Msg) { responses.Add(1) })
	require_NoError(t, err)

	go func() {
		defer wg.Done()
		for {
			select {
			case <-done:
				return
			default:
				nc.PublishRequest("HELP", reply, []byte("HELP"))
				requests.Add(1)
			}
		}
	}()

	// Perform a bunch of reloads.
	for i := 0; i < 1000; i++ {
		require_NoError(t, s.Reload())
	}

	close(done)
	wg.Wait()

	totalRequests := requests.Load()
	checkFor(t, 20*time.Second, 250*time.Millisecond, func() error {
		resp := responses.Load()
		if resp == totalRequests {
			return nil
		}
		return fmt.Errorf("Have not received all responses, want %d got %d", totalRequests, resp)
	})
}

// https://github.com/nats-io/nats-server/issues/4674
func TestAccountServiceAndStreamExportDoubleDelivery(t *testing.T) {
	conf := createConfFile(t, []byte(`
		listen: 127.0.0.1:-1
		accounts: {
			tenant1: {
				jetstream: enabled
				users: [ { user: "one", password: "one" } ]
				exports: [
					{ stream: "DW.>" }
					{ service: "DW.>" }
				]
 			}
			global: {
				jetstream: enabled
				users: [ { user: "global", password: "global" } ]
				imports: [
					{ stream: { account: tenant1, subject: "DW.>" }, prefix: tenant1 }
					{ service: { account: tenant1, subject: "DW.>" }, to: "tenant1.DW.>" }
				]
			}
			$SYS { users = [ { user: "admin", pass: "s3cr3t!" } ] }
		}
	`))

	s, _ := RunServerWithConfig(conf)
	defer s.Shutdown()

	// Now connect up the subscriber for HELP. No-op for this test.
	nc, _ := jsClientConnect(t, s, nats.UserInfo("one", "one"))
	defer nc.Close()

	var msgs atomic.Int32
	_, err := nc.Subscribe(">", func(m *nats.Msg) {
		msgs.Add(1)
	})
	require_NoError(t, err)

	nc.Publish("DW.test.123", []byte("test"))
	time.Sleep(200 * time.Millisecond)
	require_Equal(t, msgs.Load(), 1)
}

func TestAccountServiceImportNoResponders(t *testing.T) {
	// Setup NATS server.
	cf := createConfFile(t, []byte(`
				port: -1
				accounts: {
					accExp: {
						users: [{user: accExp, password: accExp}]
						exports: [{service: "foo"}]
					}
					accImp: {
						users: [{user: accImp, password: accImp}]
						imports: [{service: {account: accExp, subject: "foo"}}]
					}
				}
			`))

	s, _ := RunServerWithConfig(cf)
	defer s.Shutdown()

	// Connect to the import account. We will not setup any responders, so a request should
	// error out with ErrNoResponders.
	nc := natsConnect(t, s.ClientURL(), nats.UserInfo("accImp", "accImp"))
	defer nc.Close()

	_, err := nc.Request("foo", []byte("request"), 250*time.Millisecond)
	require_Error(t, err, nats.ErrNoResponders)
}

// Test that service import replies are delivered when the requester's reply inbox
// is subscribed on a different cluster node.
func TestAccountServiceImportReplyDroppedAcrossClusterRoutes(t *testing.T) {
	tmpl := `
		server_name: %s
		listen: 127.0.0.1:-1
		cluster {
			name: test
			listen: 127.0.0.1:-1
			%s
		}
		accounts {
			A {
				users = [{ user: a, password: a }]
				imports = [{ service: { account: B, subject: svc } }]
			}
			B {
				users = [{ user: b, password: b }]
				exports = [{ service: svc, accounts: [A] }]
			}
		}
	`
	confA := createConfFile(t, []byte(fmt.Sprintf(tmpl, "node-A", "")))
	srvA, optsA := RunServerWithConfig(confA)
	defer srvA.Shutdown()

	confB := createConfFile(t, []byte(fmt.Sprintf(tmpl, "node-B",
		fmt.Sprintf(`routes = ["nats-route://127.0.0.1:%d"]`, optsA.Cluster.Port))))
	srvB, _ := RunServerWithConfig(confB)
	defer srvB.Shutdown()

	checkClusterFormed(t, srvA, srvB)

	// Responder on node-A, account B.
	ncB := natsConnect(t, srvA.ClientURL(), nats.UserInfo("b", "b"))
	defer ncB.Close()
	natsSub(t, ncB, "svc", func(m *nats.Msg) { m.Respond([]byte("pong")) })
	natsFlush(t, ncB)

	// Wait for B's "svc" sub to propagate to node-B so it can route the request.
	checkSubInterest(t, srvB, "B", "svc", time.Second)

	// Subscriber on node-A, account A: explicit reply inbox.
	// This subscription will be a ROUTE sub on node-B once it propagates.
	ncSub := natsConnect(t, srvA.ClientURL(), nats.UserInfo("a", "a"))
	defer ncSub.Close()
	inbox := "_INBOX.xroute.reply"
	replyCh := make(chan *nats.Msg, 1)
	_, err := ncSub.Subscribe(inbox, func(m *nats.Msg) { replyCh <- m })
	require_NoError(t, err)
	natsFlush(t, ncSub)

	// Wait for the inbox to appear as a ROUTE sub on node-B.
	checkSubInterest(t, srvB, "A", inbox, time.Second)

	// Publisher on node-B, account A: publish with explicit cross-node reply.
	// The response service import on node-B receives the reply via route
	// (c.kind==ROUTER) and must forward it back to the ROUTE sub for the inbox.
	ncPub := natsConnect(t, srvB.ClientURL(), nats.UserInfo("a", "a"))
	defer ncPub.Close()
	err = ncPub.PublishMsg(&nats.Msg{Subject: "svc", Reply: inbox, Data: []byte("ping")})
	require_NoError(t, err)

	select {
	case msg := <-replyCh:
		require_Equal(t, string(msg.Data), "pong")
	case <-time.After(3 * time.Second):
		t.Fatal("reply not received — service import reply was dropped across cluster routes")
	}

	// Reverse: subscribe on node-B, publish from node-A.
	ncSub2 := natsConnect(t, srvB.ClientURL(), nats.UserInfo("a", "a"))
	defer ncSub2.Close()
	inbox2 := "_INBOX.xroute.reply2"
	replyCh2 := make(chan *nats.Msg, 1)
	_, err = ncSub2.Subscribe(inbox2, func(m *nats.Msg) { replyCh2 <- m })
	require_NoError(t, err)
	natsFlush(t, ncSub2)
	checkSubInterest(t, srvA, "A", inbox2, time.Second)

	err = ncSub.PublishMsg(&nats.Msg{Subject: "svc", Reply: inbox2, Data: []byte("ping")})
	require_NoError(t, err)

	select {
	case msg := <-replyCh2:
		require_Equal(t, string(msg.Data), "pong")
	case <-time.After(3 * time.Second):
		t.Fatal("reverse reply not received — service import reply was dropped across cluster routes")
	}
}

func TestStreamActivationExpiredNoMatchDoesNotInvalidate(t *testing.T) {
	// Account performing the import.
	a := NewAccount("importer")
	// The account we will pass as the (non-matching) export account.
	exportAcc := NewAccount("exporter")
	// A different, still-valid stream import that does NOT match the
	// (exportAcc, subject) pair passed to streamActivationExpired.
	otherAcc := NewAccount("other")
	si := &streamImport{
		acc:  otherAcc,
		from: "other.subject",
		// claim is nil, so checkActivation would return false and the
		// buggy code would wrongly mark this import invalid.
		claim: nil,
	}
	a.imports.streams = []*streamImport{si}

	// No matching import.
	a.streamActivationExpired(exportAcc, "nomatch.subject")
	if si.invalid {
		t.Fatalf("unrelated stream import was wrongly invalidated on no match")
	}
}

func TestRemoveAllServiceImportSubsNoRaceUnderConcurrentReaders(t *testing.T) {
	for range 2000 {
		a := &Account{}
		a.imports.services = make(map[string][]*serviceImport)
		for i := 0; i < 8; i++ {
			si := &serviceImport{sid: []byte("sid")}
			a.imports.services[fmt.Sprintf("svc.%d", i)] = []*serviceImport{si}
		}

		var wg sync.WaitGroup
		wg.Add(2)

		// Reader: mimics updateAccountClaimsWithRefresh reading a.ic and
		// iterating si.sid under a read lock.
		go func() {
			defer wg.Done()
			a.mu.RLock()
			_ = a.ic
			for _, sis := range a.imports.services {
				for _, si := range sis {
					_ = si.sid
				}
			}
			a.mu.RUnlock()
		}()

		// Writer: nils si.sid and a.ic. a.ic is nil so the post-unlock
		// path returns early and does not touch a real client.
		go func() {
			defer wg.Done()
			a.removeAllServiceImportSubs()
		}()

		wg.Wait()
	}
}

func TestAccountDefaultPermsRaceDuringAuth(t *testing.T) {
	acc := NewAccount("foo")

	// User claims with no permissions so buildInternalNkeyUser hits the
	// acc.defaultPerms read path (p == nil).
	uc := jwt.NewUserClaims("U" + strings.Repeat("A", 51))

	var someClaims jwt.Permissions
	someClaims.Pub.Allow.Add("foo")

	var wg sync.WaitGroup
	wg.Add(2)

	// Reader: mimics authentication path.
	go func() {
		defer wg.Done()
		for range 1000 {
			_ = buildInternalNkeyUser(uc, nil, acc)
		}
	}()

	// Writer: mimics updateAccountClaimsWithRefresh refreshing defaultPerms.
	go func() {
		defer wg.Done()
		for range 1000 {
			acc.mu.Lock()
			acc.defaultPerms = buildPermissionsFromJwt(&someClaims)
			acc.mu.Unlock()
		}
	}()

	wg.Wait()
}

// Must be run with -race.
func TestAccountRemoveCbJSWriteRace(t *testing.T) {
	opts := DefaultTestOptions
	opts.Port = -1
	opts.JetStream = true
	opts.StoreDir = t.TempDir()
	s := RunServer(&opts)
	defer s.Shutdown()

	for i := range 50 {
		name := fmt.Sprintf("ACC_%d", i)
		acc, _ := s.LookupOrRegisterAccount(name)
		require_NoError(t, acc.EnableJetStream(nil, nil))
		// Confirm a.js is set, so removeCb will reach the `a.js = nil` write.
		if !acc.JetStreamEnabled() {
			t.Fatalf("expected JetStream enabled (a.js != nil) for %s", name)
		}

		var stop atomic.Bool
		done := make(chan struct{})
		// Reader: read a.js under a.mu.RLock, exactly like maxBytesLimits does.
		go func(a *Account) {
			defer close(done)
			for !stop.Load() {
				a.mu.RLock()
				_ = a.js
				a.mu.RUnlock()
			}
		}(acc)

		// Writer: removeCb sets `a.js = nil`, which must synchronize with the
		// locked read above.
		removeCb(s, name)

		stop.Store(true)
		<-done
	}
}

// Must be run with -race.
func TestAccountSendTrackingLatencyRcRace(t *testing.T) {
	a := NewAccount("A")
	rc := &client{kind: CLIENT}
	responder := &client{kind: SYSTEM}

	si := &serviceImport{
		acc:      a,
		rc:       rc,
		share:    false,
		ts:       time.Now().UnixNano(),
		tracking: true,
		latency:  &serviceLatency{subject: "latency.subj"},
	}

	const iters = 1000
	var wg sync.WaitGroup
	wg.Add(2)

	// Reader goroutine: exercises the unlocked re-read of si.rc.
	go func() {
		defer wg.Done()
		for range iters {
			a.sendTrackingLatency(si, responder)
			a.mu.Lock()
			si.m1 = nil
			a.mu.Unlock()
		}
	}()

	// Writer goroutine: mimics sendLatencyResult writing si.rc under a.mu.Lock.
	go func() {
		defer wg.Done()
		for range iters {
			a.mu.Lock()
			si.rc = rc
			a.mu.Unlock()
		}
	}()

	wg.Wait()
}

// sharedInboxRequests stands up a foo/bar service export/import pair and has
// bar send numReqs requests that all share one reply subject. The responder
// never answers, so every response is left outstanding under that single rrMap
// key. Returns both accounts and the shared reply subject.
func sharedInboxRequests(t *testing.T, numReqs int) (*Account, *Account, string) {
	t.Helper()

	s, fooAcc, barAcc := simpleAccountServer(t)
	t.Cleanup(s.Shutdown)

	if err := fooAcc.AddServiceExport("test", nil); err != nil {
		t.Fatalf("Error adding service export: %v", err)
	}
	if err := barAcc.AddServiceImport(fooAcc, "test", _EMPTY_); err != nil {
		t.Fatalf("Error adding service import: %v", err)
	}

	// Both connections are net.Pipe, so anything the server writes blocks until
	// it is read. Drain them, otherwise the write loops sit on the flush
	// deadline and shutdown takes ten seconds.
	cfoo, rfoo, _ := newClientForServer(s)
	t.Cleanup(cfoo.close)
	go io.Copy(io.Discard, rfoo)
	if err := cfoo.registerWithAccount(fooAcc); err != nil {
		t.Fatalf("Error registering client with 'foo' account: %v", err)
	}
	// Responder never answers, so the entries stay outstanding.
	cfoo.parse([]byte("SUB test 1\r\n"))

	cbar, rbar, _ := newClientForServer(s)
	t.Cleanup(cbar.close)
	go io.Copy(io.Discard, rbar)
	if err := cbar.registerWithAccount(barAcc); err != nil {
		t.Fatalf("Error registering client with 'bar' account: %v", err)
	}

	const inbox = "my.inbox"
	cbar.parse([]byte("SUB my.inbox 11\r\n"))
	for i := 0; i < numReqs; i++ {
		cbar.parse([]byte("PUB test my.inbox 4\r\nhelp\r\n"))
	}

	return fooAcc, barAcc, inbox
}

// A client is allowed to reuse a single reply subject across many concurrent
// service import requests, which piles every outstanding response entry under
// one rrMap key. Make sure those entries are tracked and removed correctly in
// both the small (unindexed) and large (indexed) cases.
func TestServiceImportReverseEntriesSharedReplySubject(t *testing.T) {
	for _, test := range []struct {
		name    string
		numReqs int
		indexed bool
	}{
		{"below index threshold", rrIndexThreshold - 1, false},
		{"above index threshold", rrIndexThreshold * 100, true},
	} {
		t.Run(test.name, func(t *testing.T) {
			fooAcc, barAcc, inbox := sharedInboxRequests(t, test.numReqs)

			barAcc.mu.RLock()
			re, ok := barAcc.imports.rrMap[inbox]
			numEntries, indexed := len(re.list), re.idx != nil
			barAcc.mu.RUnlock()

			if !ok {
				t.Fatalf("Expected a reverse entry for %q", inbox)
			}
			if numEntries != test.numReqs {
				t.Fatalf("Expected %d reverse entries, got %d", test.numReqs, numEntries)
			}
			if indexed != test.indexed {
				t.Fatalf("Expected indexed to be %v, got %v", test.indexed, indexed)
			}

			// Now expire them the way the response threshold timer does, and make
			// sure every entry is accounted for as we go.
			fooAcc.mu.RLock()
			var expired []*serviceImport
			for _, si := range fooAcc.exports.responses {
				expired = append(expired, si)
			}
			fooAcc.mu.RUnlock()

			if len(expired) != test.numReqs {
				t.Fatalf("Expected %d outstanding responses, got %d", test.numReqs, len(expired))
			}

			for i, si := range expired {
				fooAcc.removeRespServiceImport(si, rsiTimeout)
				barAcc.mu.RLock()
				re, ok := barAcc.imports.rrMap[inbox]
				remaining := len(re.list)
				idxLen := len(re.idx)
				barAcc.mu.RUnlock()

				if expect := test.numReqs - i - 1; remaining != expect {
					t.Fatalf("After %d removals expected %d entries, got %d", i+1, expect, remaining)
				} else if expect == 0 {
					if ok {
						t.Fatalf("Expected the reverse entry for %q to be removed once empty", inbox)
					}
				} else if re.idx != nil && idxLen != remaining {
					t.Fatalf("Index out of sync with list: %d vs %d", idxLen, remaining)
				}
			}

			fooAcc.mu.RLock()
			left := len(fooAcc.exports.responses)
			fooAcc.mu.RUnlock()
			if left != 0 {
				t.Fatalf("Expected no outstanding responses left, got %d", left)
			}
		})
	}
}

// The other removal path drops a whole reply subject at once instead of one
// entry at a time. That is what runs when interest in the reply subject goes
// away, so it is the path a disconnecting requestor takes.
func TestServiceImportReverseEntriesBulkRemoval(t *testing.T) {
	for _, test := range []struct {
		name    string
		numReqs int
	}{
		{"below index threshold", rrIndexThreshold - 1},
		{"above index threshold", rrIndexThreshold * 100},
	} {
		t.Run(test.name, func(t *testing.T) {
			fooAcc, barAcc, inbox := sharedInboxRequests(t, test.numReqs)

			fooAcc.mu.RLock()
			outstanding := len(fooAcc.exports.responses)
			fooAcc.mu.RUnlock()
			if outstanding != test.numReqs {
				t.Fatalf("Expected %d outstanding responses, got %d", test.numReqs, outstanding)
			}

			// A nil si means drop every entry under this reply subject and clean
			// up the responses they point at on the exporting account.
			barAcc.checkForReverseEntry(inbox, nil, false)

			barAcc.mu.RLock()
			_, ok := barAcc.imports.rrMap[inbox]
			remaining := len(barAcc.imports.rrMap)
			barAcc.mu.RUnlock()
			if ok {
				t.Fatalf("Expected the reverse entry for %q to be gone", inbox)
			}
			if remaining != 0 {
				t.Fatalf("Expected no reverse entries left, got %d", remaining)
			}

			fooAcc.mu.RLock()
			outstanding = len(fooAcc.exports.responses)
			fooAcc.mu.RUnlock()
			if outstanding != 0 {
				t.Fatalf("Expected every outstanding response to be cleaned up, got %d", outstanding)
			}
		})
	}
}

// Guards the cost of clearing outstanding responses. Each iteration clears the
// whole set, so ns/op is the cost of draining n entries, not of one removal.
// The shared case is the one that used to go quadratic; the unique case is the
// common one and should stay where it is.
func BenchmarkReverseRespMapRemoval(b *testing.B) {
	for _, bench := range []struct {
		name   string
		shared bool
	}{
		{"SharedReplySubject", true},
		{"UniqueReplySubjects", false},
	} {
		for _, n := range []int{1000, 10000, 40000} {
			b.Run(fmt.Sprintf("%s/%d", bench.name, n), func(b *testing.B) {
				replies, sis := make([]string, n), make([]*serviceImport, n)
				for i := 0; i < n; i++ {
					if bench.shared {
						replies[i] = "my.inbox"
					} else {
						replies[i] = fmt.Sprintf("my.inbox.%d", i)
					}
					sis[i] = &serviceImport{from: fmt.Sprintf("_R_.%d", i), to: replies[i]}
				}

				b.ReportAllocs()
				for i := 0; i < b.N; i++ {
					b.StopTimer()
					acc := NewAccount("bar")
					for j := 0; j < n; j++ {
						acc.addReverseRespMapEntry(acc, replies[j], sis[j].from)
					}
					b.StartTimer()

					for j := 0; j < n; j++ {
						acc.checkForReverseEntry(replies[j], sis[j], false)
					}

					if left := len(acc.imports.rrMap); left != 0 {
						b.Fatalf("Expected no reverse entries left, got %d", left)
					}
				}
			})
		}
	}
}

func TestAccountMappingsAccountFunctions(t *testing.T) {
	s, fooAcc, _ := simpleAccountServer(t)
	defer s.Shutdown()
	hash := getHash(fooAcc.Name)

	// In the destination.
	require_NoError(t, fooAcc.AddMapping("orders.*", "{{account-hash()}}.orders.{{wildcard(1)}}"))
	ndest, ok := fooAcc.selectMappedSubject("orders.new")
	require_True(t, ok)
	require_Equal(t, ndest, hash+".orders.new")

	// In the source.
	require_NoError(t, fooAcc.AddMapping("in.{{account-hash()}}.*", "out.{{wildcard(1)}}"))
	ndest, ok = fooAcc.selectMappedSubject("in." + hash + ".x")
	require_True(t, ok)
	require_Equal(t, ndest, "out.x")
	// Removing with the configured form works.
	require_True(t, fooAcc.RemoveMapping("in.{{account-hash()}}.*"))
	_, ok = fooAcc.selectMappedSubject("in." + hash + ".x")
	require_False(t, ok)

	// Functions that need an import or leafnode context are not available in mappings, and arguments are checked.
	for _, dest := range []string{
		"{{account-hash-import()}}", "{{account-hash-leaf-remote()}}",
		"{{account-hash(x)}}", "{{account-hash-import(x)}}", "{{account-hash(a,b)}}",
	} {
		require_Error(t, fooAcc.AddMapping("b", dest), ErrInvalidMappingDestination)
	}
	require_Error(t, fooAcc.AddMapping("{{account-hash-import()}}", "b"), ErrMappingFunctionNotAvailable)
}

func TestAccountImportExportAccountFunctions(t *testing.T) {
	conf := createConfFile(t, []byte(`
		listen: 127.0.0.1:-1
		accounts: {
			EXP: {
				users: [ {user: exp, password: pwd} ]
				exports [
					{ stream: "events.{{account-hash()}}.>" }
					{ service: "req.{{account-hash()}}.*" }
				]
			}
			IMP: {
				users: [ {user: imp, password: pwd} ]
				imports [
					{ stream: { subject: "events.{{account-hash-import()}}.>", account: EXP }, prefix: "from.{{account-hash()}}" }
					{ service: { subject: "req.{{account-hash-import()}}.*", account: EXP }, to: "svc.{{account-hash()}}.*" }
				]
			}
		}
	`))
	s, _ := RunServerWithConfig(conf)
	defer s.Shutdown()

	expHash, impHash := getHash("EXP"), getHash("IMP")
	exp, err := s.LookupAccount("EXP")
	require_NoError(t, err)
	exp.mu.RLock()
	_, hasStream := exp.exports.streams["events."+expHash+".>"]
	_, hasService := exp.exports.services["req."+expHash+".*"]
	exp.mu.RUnlock()
	require_True(t, hasStream)
	require_True(t, hasService)

	ncExp := natsConnect(t, s.ClientURL(), nats.UserInfo("exp", "pwd"))
	defer ncExp.Close()
	ncImp := natsConnect(t, s.ClientURL(), nats.UserInfo("imp", "pwd"))
	defer ncImp.Close()

	// Stream import with a prefix.
	sub := natsSubSync(t, ncImp, "from."+impHash+".events."+expHash+".>")
	natsFlush(t, ncImp)
	natsPub(t, ncExp, "events."+expHash+".a.b", []byte("hello"))
	m := natsNexMsg(t, sub, time.Second)
	require_Equal(t, m.Subject, "from."+impHash+".events."+expHash+".a.b")

	// Service import with a wildcard local subject.
	_, err = ncExp.Subscribe("req."+expHash+".*", func(m *nats.Msg) { m.Respond([]byte("ok")) })
	require_NoError(t, err)
	natsFlush(t, ncExp)
	resp, err := ncImp.Request("svc."+impHash+".hello", nil, time.Second)
	require_NoError(t, err)
	require_Equal(t, string(resp.Data), "ok")
}

func TestAccountImportExportAccountFunctionsConfigErrors(t *testing.T) {
	for _, test := range []struct {
		name     string
		conf     string
		expected string
	}{
		{"import function in export", `accounts: { A: { exports [ { stream: "x.{{account-hash-import()}}" } ] } }`, "not available"},
		{"leafnode function in import", `accounts: { A: { exports [ { stream: "x.>" } ] }, B: { imports [ { stream: { subject: "x.{{account-hash-leaf-remote()}}", account: A } } ] } }`, "not available"},
		{"bad arguments", `accounts: { A: { exports [ { service: "x.{{account-hash(a,b)}}" } ] } }`, "too many arguments"},
		{"mapping", `accounts: { A: { mappings: { "foo": "{{account-hash-import()}}" } } }`, "not available"},
	} {
		t.Run(test.name, func(t *testing.T) {
			conf := createConfFile(t, []byte("listen: 127.0.0.1:-1\n"+test.conf))
			_, err := ProcessConfigFile(conf)
			require_Error(t, err)
			require_Contains(t, err.Error(), test.expected)
		})
	}
}

func TestAccountUserPermissionsAccountFunctionsNotResolved(t *testing.T) {
	conf := createConfFile(t, []byte(`
		listen: 127.0.0.1:-1
		accounts: {
			A: {
				users: [ {user: a, password: pwd, permissions: {
					publish: { allow: ["ok.>", "{{account-hash()}}.>"], deny: "deny.{{account-hash()}}" }
					subscribe: { deny: ["deny.{{account-hash()}}.>", "deny.{{account-hash()}} q"] }
				} } ]
			}
		}
	`))
	s, _ := RunServerWithConfig(conf)
	defer s.Shutdown()
	hash := getHash("A")

	nc := natsConnect(t, s.ClientURL(), nats.UserInfo("a", "pwd"))
	defer nc.Close()

	var c *client
	checkFor(t, time.Second, 10*time.Millisecond, func() error {
		s.mu.RLock()
		defer s.mu.RUnlock()
		for _, cli := range s.clients {
			c = cli
			return nil
		}
		return fmt.Errorf("client not registered yet")
	})
	// Account scoped mapping functions are only resolved in leafnode remote deny lists,
	// in user permissions they are plain literal tokens.
	c.mu.Lock()
	defer c.mu.Unlock()
	require_False(t, c.perms.pub.allow.HasInterest(hash+".foo"))
	require_True(t, c.perms.pub.allow.HasInterest("{{account-hash()}}.foo"))
	require_False(t, c.perms.pub.deny.HasInterest("deny."+hash))
	require_True(t, c.perms.pub.deny.HasInterest("deny.{{account-hash()}}"))
	require_False(t, c.perms.sub.deny.HasInterest("deny."+hash+".x"))
	require_True(t, c.perms.sub.deny.HasInterest("deny.{{account-hash()}}.x"))
}
