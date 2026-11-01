// Copyright 2012-2024 The NATS Authors
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
	"encoding/json"
	"testing"
	"time"

	"github.com/nats-io/nats-server/v2/server"
)

const PROTO_TEST_PORT = 9922

func runProtoServer() *server.Server {
	opts := DefaultTestOptions
	opts.Port = PROTO_TEST_PORT
	opts.MaxControlLine = 256
	opts.NoSystemAccount = true
	return RunServer(&opts)
}

func TestProtoBasics(t *testing.T) {
	s := runProtoServer()
	defer s.Shutdown()

	c := createClientConn(t, "127.0.0.1", PROTO_TEST_PORT)
	defer c.Close()

	send, expect := setupConn(t, c)
	expectMsgs := expectMsgsCommand(t, expect)

	// Ping
	send("PING\r\n")
	expect(pongRe)

	// Single Msg
	send("SUB foo 1\r\nPUB foo 5\r\nhello\r\n")
	matches := expectMsgs(1)
	checkMsg(t, matches[0], "foo", "1", "", "5", "hello")

	// 2 Messages
	send("SUB * 2\r\nPUB foo 2\r\nok\r\n")
	matches = expectMsgs(2)
	// Could arrive in any order
	checkMsg(t, matches[0], "foo", "", "", "2", "ok")
	checkMsg(t, matches[1], "foo", "", "", "2", "ok")
}

func TestProtoErr(t *testing.T) {
	s := runProtoServer()
	defer s.Shutdown()

	c := createClientConn(t, "127.0.0.1", PROTO_TEST_PORT)
	defer c.Close()

	send, expect := setupConn(t, c)

	// Make sure we get an error on bad proto
	send("ZZZ")
	expect(errRe)
}

func TestUnsubMax(t *testing.T) {
	s := runProtoServer()
	defer s.Shutdown()

	c := createClientConn(t, "127.0.0.1", PROTO_TEST_PORT)
	defer c.Close()

	send, expect := setupConn(t, c)
	expectMsgs := expectMsgsCommand(t, expect)

	send("SUB foo 22\r\n")
	send("UNSUB 22 2\r\n")
	for i := 0; i < 100; i++ {
		send("PUB foo 2\r\nok\r\n")
	}

	time.Sleep(50 * time.Millisecond)

	matches := expectMsgs(2)
	checkMsg(t, matches[0], "foo", "22", "", "2", "ok")
	checkMsg(t, matches[1], "foo", "22", "", "2", "ok")
}

func TestQueueSub(t *testing.T) {
	s := runProtoServer()
	defer s.Shutdown()

	c := createClientConn(t, "127.0.0.1", PROTO_TEST_PORT)
	defer c.Close()

	send, expect := setupConn(t, c)
	expectMsgs := expectMsgsCommand(t, expect)

	sent := 100
	send("SUB foo qgroup1 22\r\n")
	send("SUB foo qgroup1 32\r\n")
	for i := 0; i < sent; i++ {
		send("PUB foo 2\r\nok\r\n")
	}
	// Wait for responses
	time.Sleep(250 * time.Millisecond)

	matches := expectMsgs(sent)
	sids := make(map[string]int)
	for _, m := range matches {
		sids[string(m[sidIndex])]++
	}
	if len(sids) != 2 {
		t.Fatalf("Expected only 2 sids, got %d\n", len(sids))
	}
	for k, c := range sids {
		// Queue member selection is random, allow for ~5 standard deviations.
		if c < 25 {
			t.Fatalf("Expected ~50 (+-25) msgs for sid:'%s', got %d\n", k, c)
		}
	}
}

func TestMultipleQueueSub(t *testing.T) {
	s := runProtoServer()
	defer s.Shutdown()

	c := createClientConn(t, "127.0.0.1", PROTO_TEST_PORT)
	defer c.Close()

	send, expect := setupConn(t, c)
	expectMsgs := expectMsgsCommand(t, expect)

	sent := 100
	send("SUB foo g1 1\r\n")
	send("SUB foo g1 2\r\n")
	send("SUB foo g2 3\r\n")
	send("SUB foo g2 4\r\n")

	for i := 0; i < sent; i++ {
		send("PUB foo 2\r\nok\r\n")
	}
	// Wait for responses
	time.Sleep(250 * time.Millisecond)

	matches := expectMsgs(sent * 2)
	sids := make(map[string]int)
	for _, m := range matches {
		sids[string(m[sidIndex])]++
	}
	if len(sids) != 4 {
		t.Fatalf("Expected 4 sids, got %d\n", len(sids))
	}
	for k, c := range sids {
		// Queue member selection is random, allow for ~5 standard deviations.
		if c < 25 {
			t.Fatalf("Expected ~50 (+-25) msgs for '%s', got %d\n", k, c)
		}
	}
}

func TestPubToArgState(t *testing.T) {
	s := runProtoServer()
	defer s.Shutdown()

	c := createClientConn(t, "127.0.0.1", PROTO_TEST_PORT)
	defer c.Close()

	send, expect := setupConn(t, c)

	send("PUBS foo 2\r\nok\r\n")
	expect(errRe)
}

func TestSubToArgState(t *testing.T) {
	s := runProtoServer()
	defer s.Shutdown()

	c := createClientConn(t, "127.0.0.1", PROTO_TEST_PORT)
	defer c.Close()

	send, expect := setupConn(t, c)

	send("SUBZZZ foo 1\r\n")
	expect(errRe)
}

// Issue #63
func TestProtoCrash(t *testing.T) {
	s := runProtoServer()
	defer s.Shutdown()

	c := createClientConn(t, "127.0.0.1", PROTO_TEST_PORT)
	defer c.Close()

	send, expect := sendCommand(t, c), expectCommand(t, c)

	checkInfoMsg(t, c)

	send("CONNECT {\"verbose\":true,\"tls_required\":false,\"user\":\"test\",\"pedantic\":true,\"pass\":\"password\"}")

	time.Sleep(100 * time.Millisecond)

	send("\r\n")
	expect(okRe)
}

// Issue #136
func TestDuplicateProtoSub(t *testing.T) {
	s := runProtoServer()
	defer s.Shutdown()

	c := createClientConn(t, "127.0.0.1", PROTO_TEST_PORT)
	defer c.Close()

	send, expect := setupConn(t, c)

	send("PING\r\n")
	expect(pongRe)

	send("SUB foo 1\r\n")

	send("SUB foo 1\r\n")

	ns := 0

	for i := 0; i < 5; i++ {
		ns = int(s.NumSubscriptions())
		if ns == 0 {
			time.Sleep(50 * time.Millisecond)
		} else {
			break
		}
	}

	if ns != 1 {
		t.Fatalf("Expected 1 subscription, got %d\n", ns)
	}
}

func TestIncompletePubArg(t *testing.T) {
	s := runProtoServer()
	defer s.Shutdown()

	c := createClientConn(t, "127.0.0.1", PROTO_TEST_PORT)
	defer c.Close()
	send, expect := setupConn(t, c)

	size := 10000
	goodBuf := ""
	for i := 0; i < size; i++ {
		goodBuf += "A"
	}
	goodBuf += "\r\n"

	badSize := 3371
	badBuf := ""
	for i := 0; i < badSize; i++ {
		badBuf += "B"
	}
	// Message is corrupted and since we are still reading from client,
	// next PUB accidentally becomes part of the payload of the
	// incomplete message thus breaking the protocol.
	badBuf2 := ""
	for i := 0; i < size; i++ {
		badBuf2 += "C"
	}
	badBuf2 += "\r\n"

	pub := "PUB example 10000\r\n"
	send(pub + goodBuf + pub + goodBuf + pub + badBuf + pub + badBuf2)
	expect(errRe)
}

func TestControlLineMaximums(t *testing.T) {
	s := runProtoServer()
	defer s.Shutdown()

	c := createClientConn(t, "127.0.0.1", PROTO_TEST_PORT)
	defer c.Close()

	send, expect := setupConn(t, c)

	pubTooLong := "PUB foo "
	for i := 0; i < 32; i++ {
		pubTooLong += "2222222222"
	}
	send(pubTooLong)
	expect(errRe)
}

func TestServerInfoWithClientAdvertise(t *testing.T) {
	opts := DefaultTestOptions
	opts.Port = PROTO_TEST_PORT
	opts.ClientAdvertise = "me:1"
	s := RunServer(&opts)
	defer s.Shutdown()

	c := createClientConn(t, opts.Host, PROTO_TEST_PORT)
	defer c.Close()

	buf := expectResult(t, c, infoRe)
	js := infoRe.FindAllSubmatch(buf, 1)[0][1]
	var sinfo server.Info
	err := json.Unmarshal(js, &sinfo)
	if err != nil {
		t.Fatalf("Could not unmarshal INFO json: %v\n", err)
	}
	if sinfo.Host != "me" || sinfo.Port != 1 {
		t.Fatalf("Expected INFO Host:Port to be me:1, got %s:%d", sinfo.Host, sinfo.Port)
	}
}
