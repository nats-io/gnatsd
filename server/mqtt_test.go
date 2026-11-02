// Copyright 2020-2026 The NATS Authors
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

//go:build !skip_mqtt_tests

package server

import (
	"bufio"
	"bytes"
	"crypto/tls"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math/rand/v2"
	"net"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nats-io/jwt/v2"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nkeys"
	"github.com/nats-io/nuid"
)

var testMQTTTimeout = 10 * time.Second

var jsClusterTemplWithLeafAndMQTT = `
	listen: 127.0.0.1:-1
	server_name: %s
	jetstream: {max_mem_store: 256MB, max_file_store: 2GB, store_dir: '%s'}

	{{leaf}}

	cluster {
		name: %s
		listen: 127.0.0.1:%d
		routes = [%s]
	}

	mqtt {
		listen: 127.0.0.1:-1
	}

	# For access to system account.
	accounts { $SYS { users = [ { user: "admin", pass: "s3cr3t!" } ] } }
`

type mqttWrapAsWs struct {
	net.Conn
	t   testing.TB
	br  *bufio.Reader
	tmp []byte
}

func (c *mqttWrapAsWs) Write(p []byte) (int, error) {
	proto := testWSCreateClientMsg(wsBinaryMessage, 1, true, false, p)
	return c.Conn.Write(proto)
}

func (c *mqttWrapAsWs) Read(p []byte) (int, error) {
	for {
		if n := len(c.tmp); n > 0 {
			if len(p) < n {
				n = len(p)
			}
			copy(p, c.tmp[:n])
			c.tmp = c.tmp[n:]
			return n, nil
		}
		c.tmp = testWSReadFrame(c.t, c.br)
	}
}

func testMQTTReadPacket(t testing.TB, r *mqttReader) (byte, int) {
	t.Helper()

	var b byte
	var pl int
	var err error

	rd := r.reader

	fill := func() []byte {
		t.Helper()
		var buf [512]byte

		n, err := rd.Read(buf[:])
		if err != nil {
			t.Fatalf("Error reading data: %v", err)
		}
		return copyBytes(buf[:n])
	}

	rd.SetReadDeadline(time.Now().Add(testMQTTTimeout))
	for {
		r.pstart = r.pos
		if !r.hasMore() {
			r.reset(fill())
			continue
		}
		b, err = r.readByte("packet type")
		if err != nil {
			t.Fatalf("Error reading packet: %v", err)
		}
		var complete bool
		pl, complete, err = r.readPacketLen(b&mqttPacketMask, MAX_PAYLOAD_SIZE)
		if err != nil {
			t.Fatalf("Error reading packet: %v", err)
		}
		if !complete {
			r.reset(fill())
			continue
		}
		break
	}
	rd.SetReadDeadline(time.Time{})
	return b, pl
}

func testMQTTReadPIPacket(expectedType byte, t testing.TB, r *mqttReader, expectedPI uint16) {
	t.Helper()
	b, _ := testMQTTReadPacket(t, r)
	if pt := b & mqttPacketMask; pt != expectedType {
		t.Fatalf("Expected packet %x, got %x", expectedType, pt)
	}
	rpi, err := r.readUint16("packet identifier")
	if err != nil || rpi != expectedPI {
		t.Fatalf("Expected PI %v got: %v, err=%v", expectedPI, rpi, err)
	}
}

func TestMQTTReader(t *testing.T) {
	r := &mqttReader{}
	r.reset([]byte{0, 2, 'a', 'b'})
	bs, err := r.readBytes("", false)
	if err != nil {
		t.Fatal(err)
	}
	sbs := string(bs)
	if sbs != "ab" {
		t.Fatalf(`expected "ab", got %q`, sbs)
	}

	r.reset([]byte{0, 2, 'a', 'b'})
	bs, err = r.readBytes("", true)
	if err != nil {
		t.Fatal(err)
	}
	bs[0], bs[1] = 'c', 'd'
	if bytes.Equal(bs, r.buf[2:]) {
		t.Fatal("readBytes should have returned a copy")
	}

	r.reset([]byte{'a', 'b'})
	if b, err := r.readByte(""); err != nil || b != 'a' {
		t.Fatalf("Error reading byte: b=%v err=%v", b, err)
	}
	if !r.hasMore() {
		t.Fatal("expected to have more, did not")
	}
	if b, err := r.readByte(""); err != nil || b != 'b' {
		t.Fatalf("Error reading byte: b=%v err=%v", b, err)
	}
	if r.hasMore() {
		t.Fatal("expected to not have more")
	}
	if _, err := r.readByte("test"); err == nil || !strings.Contains(err.Error(), "error reading test") {
		t.Fatalf("unexpected error: %v", err)
	}

	r.reset([]byte{0, 2, 'a', 'b'})
	if s, err := r.readString(""); err != nil || s != "ab" {
		t.Fatalf("Error reading string: s=%q err=%v", s, err)
	}

	r.reset([]byte{10})
	if _, err := r.readUint16("uint16"); err == nil || !strings.Contains(err.Error(), "error reading uint16") {
		t.Fatalf("unexpected error: %v", err)
	}

	r.reset([]byte{0x82, 0xff, 0x3})
	l, _, err := r.readVarInt()
	if err != nil {
		t.Fatal("error getting packet len")
	}
	if l != 0xff82 {
		t.Fatalf("expected length 0xff82 got 0x%x", l)
	}
	r.reset([]byte{0xff, 0xff, 0xff, 0xff, 0xff})
	if _, _, err := r.readVarInt(); err == nil || !strings.Contains(err.Error(), "malformed") {
		t.Fatalf("unexpected error: %v", err)
	}

	r.reset([]byte{0, 2, 'a', 'b', mqttPacketPub, 0x82, 0xff, 0x3})
	r.readString("")
	for i := 0; i < 2; i++ {
		r.pstart = r.pos
		b, err := r.readByte("")
		if err != nil {
			t.Fatalf("Error reading byte: %v", err)
		}
		if pt := b & mqttPacketMask; pt != mqttPacketPub {
			t.Fatalf("Unexpected byte: %v", b)
		}
		pl, complete, err := r.readPacketLen(mqttPacketPub, MAX_PAYLOAD_SIZE)
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		if complete {
			t.Fatal("Expected to be incomplete")
		}
		if pl != 0 {
			t.Fatalf("Expected pl to be 0, got %v", pl)
		}
		if i > 0 {
			break
		}
		if !bytes.Equal(r.pbuf, []byte{mqttPacketPub, 0x82, 0xff, 0x3}) {
			t.Fatalf("Invalid recorded partial: %v", r.pbuf)
		}
		r.reset([]byte{'a', 'b', 'c'})
		if !bytes.Equal(r.buf, []byte{mqttPacketPub, 0x82, 0xff, 0x3, 'a', 'b', 'c'}) {
			t.Fatalf("Invalid buffer: %v", r.buf)
		}
		if r.pbuf != nil {
			t.Fatalf("Partial buffer should have been reset, got %v", r.pbuf)
		}
		if r.pos != 0 {
			t.Fatalf("Pos should have been reset, got %v", r.pos)
		}
		if r.pstart != 0 {
			t.Fatalf("Pstart should have been reset, got %v", r.pstart)
		}
	}
	// On second pass, the pbuf should have been extended with 'abc'
	if !bytes.Equal(r.pbuf, []byte{mqttPacketPub, 0x82, 0xff, 0x3, 'a', 'b', 'c'}) {
		t.Fatalf("Invalid recorded partial: %v", r.pbuf)
	}
}

func TestMQTTWriter(t *testing.T) {
	w := newMQTTWriter(0)
	w.WriteUint16(1234)

	r := &mqttReader{}
	r.reset(w.Bytes())
	if v, err := r.readUint16(""); err != nil || v != 1234 {
		t.Fatalf("unexpected value: v=%v err=%v", v, err)
	}

	w.Reset()
	w.WriteString("test")
	r.reset(w.Bytes())
	if len(r.buf) != 6 {
		t.Fatalf("Expected 2 bytes size before string, got %v", r.buf)
	}

	w.Reset()
	w.WriteBytes([]byte("test"))
	r.reset(w.Bytes())
	if len(r.buf) != 6 {
		t.Fatalf("Expected 2 bytes size before bytes, got %v", r.buf)
	}

	ints := []int{
		0, 1, 127, 128, 16383, 16384, 2097151, 2097152, 268435455,
	}
	lens := []int{
		1, 1, 1, 2, 2, 3, 3, 4, 4,
	}

	tl := 0
	w.Reset()
	for i, v := range ints {
		w.WriteVarInt(v)
		tl += lens[i]
		if tl != w.Len() {
			t.Fatalf("expected len %d, got %d", tl, w.Len())
		}
	}

	r.reset(w.Bytes())
	for _, v := range ints {
		x, _, _ := r.readVarInt()
		if v != x {
			t.Fatalf("expected %d, got %d", v, x)
		}
	}
}

func TestMQTTIncompleteConnectMaxPayloadViolationDisconnects(t *testing.T) {
	const maxPayload = 1024
	const remainingLength = maxPayload + 1

	o := testMQTTDefaultOptions()
	o.MaxPayload = maxPayload
	s := testMQTTRunServer(t, o)
	defer testMQTTShutdownServer(s)

	c, err := net.Dial("tcp", net.JoinHostPort(o.MQTT.Host, fmt.Sprintf("%d", o.MQTT.Port)))
	require_NoError(t, err)
	defer c.Close()

	w := newMQTTWriter(0)
	w.WriteByte(mqttPacketConnect)
	w.WriteVarInt(remainingLength)
	w.Write(bytes.Repeat([]byte{'A'}, maxPayload))

	_, err = testMQTTWrite(c, w.Bytes())
	require_NoError(t, err)

	testMQTTExpectDisconnect(t, c)
}

func TestMQTTPacketLenMaxPayloadViolation(t *testing.T) {
	const maxPayload = 1024

	w := newMQTTWriter(0)
	w.WriteByte(mqttPacketConnect)
	w.WriteVarInt(maxPayload)
	w.Write(bytes.Repeat([]byte{'A'}, maxPayload))

	r := &mqttReader{}
	r.reset(w.Bytes())
	r.pstart = r.pos

	_, err := r.readByte("packet type")
	require_NoError(t, err)

	packetLen, complete, err := r.readPacketLen(mqttPacketConnect, maxPayload)
	require_Error(t, err, ErrMaxPayload)
	require_False(t, complete)
	require_Equal(t, packetLen, w.Len())
	require_Equal(t, len(r.pbuf), 0)
}

func TestMQTTPartialPacketNotCopiedOnEveryRead(t *testing.T) {
	const payloadLen = 64 * 1024
	w := newMQTTWriter(0)
	w.WriteByte(mqttPacketPub)
	w.WriteVarInt(2 + 3 + payloadLen)
	w.WriteString("foo")
	w.Write(bytes.Repeat([]byte{'A'}, payloadLen))
	packet := w.Bytes()

	r := &mqttReader{}
	var reallocs int
	var last *byte
	for i, b := range packet {
		r.reset([]byte{b})
		r.pstart = r.pos
		_, err := r.readByte("packet type")
		require_NoError(t, err)
		pl, complete, err := r.readPacketLen(mqttPacketPub, MAX_PAYLOAD_SIZE)
		require_NoError(t, err)
		if i == len(packet)-1 {
			require_True(t, complete)
			require_Equal(t, pl, 2+3+payloadLen)
			require_True(t, bytes.Equal(r.buf, packet))
		} else {
			require_False(t, complete)
			require_Equal(t, len(r.pbuf), i+1)
			if p := &r.pbuf[0]; p != last {
				reallocs++
				last = p
			}
		}
	}
	// Buffer growth should be amortized, not a new allocation and copy per read.
	require_True(t, reallocs < 100)
}

func testMQTTDefaultOptions() *Options {
	o := DefaultOptions()
	o.ServerName = nuid.Next()
	o.Cluster.Port = 0
	o.Gateway.Name = ""
	o.Gateway.Port = 0
	o.LeafNode.Port = 0
	o.Websocket.Port = 0
	o.MQTT.Host = "127.0.0.1"
	o.MQTT.Port = -1
	o.JetStream = true
	return o
}

func testMQTTRunServer(t testing.TB, o *Options) *Server {
	t.Helper()
	o.NoLog = false
	if o.StoreDir == _EMPTY_ {
		o.StoreDir = t.TempDir()
	}
	s, err := NewServer(o)
	if err != nil {
		t.Fatalf("Error creating server: %v", err)
	}
	l := &DummyLogger{}
	s.SetLogger(l, true, true)
	s.Start()
	if err := s.readyForConnections(3 * time.Second); err != nil {
		testMQTTShutdownServer(s)
		t.Fatal(err)
	}
	return s
}

func testMQTTShutdownRestartedServer(s **Server) {
	srv := *s
	testMQTTShutdownServer(srv)
	*s = nil
}

func testMQTTShutdownServer(s *Server) {
	if c := s.JetStreamConfig(); c != nil {
		dir := strings.TrimSuffix(c.StoreDir, JetStreamStoreDir)
		defer os.RemoveAll(dir)
	}
	s.Shutdown()
}

func testMQTTDefaultTLSOptions(t *testing.T, verify bool) *Options {
	t.Helper()
	o := testMQTTDefaultOptions()
	tc := &TLSConfigOpts{
		CertFile: "../test/configs/certs/server-cert.pem",
		KeyFile:  "../test/configs/certs/server-key.pem",
		CaFile:   "../test/configs/certs/ca.pem",
		Verify:   verify,
	}
	var err error
	o.MQTT.TLSConfig, err = GenTLSConfig(tc)
	o.MQTT.TLSTimeout = 2.0
	if err != nil {
		t.Fatalf("Error creating tls config: %v", err)
	}
	return o
}

func TestMQTTServerNameRequired(t *testing.T) {
	conf := createConfFile(t, []byte(`
		cluster {
			port: -1
		}
		mqtt {
			port: -1
		}
	`))
	o, err := ProcessConfigFile(conf)
	if err != nil {
		t.Fatalf("Error processing config file: %v", err)
	}
	if _, err := NewServer(o); err == nil || err.Error() != errMQTTServerNameMustBeSet.Error() {
		t.Fatalf("Expected error about requiring server name to be set, got %v", err)
	}

	conf = createConfFile(t, []byte(`
		mqtt {
			port: -1
		}
	`))
	o, err = ProcessConfigFile(conf)
	if err != nil {
		t.Fatalf("Error processing config file: %v", err)
	}
	if _, err := NewServer(o); err == nil || err.Error() != errMQTTStandaloneNeedsJetStream.Error() {
		t.Fatalf(`Expected errMQTTStandaloneNeedsJetStream ("next in line"), got %v`, err)
	}
}

func TestMQTTStandaloneRequiresJetStream(t *testing.T) {
	conf := createConfFile(t, []byte(`
		server_name: mqtt
		mqtt {
			port: -1
			tls {
				cert_file: "./configs/certs/server.pem"
				key_file: "./configs/certs/key.pem"
			}
		}
	`))
	o, err := ProcessConfigFile(conf)
	if err != nil {
		t.Fatalf("Error processing config file: %v", err)
	}
	if _, err := NewServer(o); err == nil || err.Error() != errMQTTStandaloneNeedsJetStream.Error() {
		t.Fatalf("Expected error about requiring JetStream in standalone mode, got %v", err)
	}
}

func TestMQTTConfig(t *testing.T) {
	conf := createConfFile(t, []byte(fmt.Sprintf(`
		jetstream {
			store_dir = %q
		}
		server_name: mqtt
		mqtt {
			port: -1
			tls {
				cert_file: "./configs/certs/server.pem"
				key_file: "./configs/certs/key.pem"
			}
		}
	`, t.TempDir())))
	s, o := RunServerWithConfig(conf)
	defer testMQTTShutdownServer(s)
	if o.MQTT.TLSConfig == nil {
		t.Fatal("expected TLS config to be set")
	}
}

func TestMQTTValidateOptions(t *testing.T) {
	nmqtto := DefaultOptions()
	mqtto := testMQTTDefaultOptions()
	for _, test := range []struct {
		name    string
		getOpts func() *Options
		err     error
	}{
		{"mqtt disabled", func() *Options { return nmqtto.Clone() }, nil},
		{"mqtt username not allowed if users specified", func() *Options {
			o := mqtto.Clone()
			o.Users = []*User{{Username: "abc", Password: "pwd"}}
			o.MQTT.Username = "b"
			o.MQTT.Password = "pwd"
			return o
		}, errMQTTUserMixWithUsersNKeys},
		{"mqtt token not allowed if users specified", func() *Options {
			o := mqtto.Clone()
			o.Nkeys = []*NkeyUser{{Nkey: "abc"}}
			o.MQTT.Token = "mytoken"
			return o
		}, errMQTTTokenMixWIthUsersNKeys},
		{"ack wait should be >=0", func() *Options {
			o := mqtto.Clone()
			o.MQTT.AckWait = -10 * time.Second
			return o
		}, errMQTTAckWaitMustBePositive},
		{"js api timeout should be >=0", func() *Options {
			o := mqtto.Clone()
			o.MQTT.JSAPITimeout = -10 * time.Second
			return o
		}, errMQTTJSAPITimeoutMustBePositive},
	} {
		t.Run(test.name, func(t *testing.T) {
			err := validateMQTTOptions(test.getOpts())
			if test.err == nil && err != nil {
				t.Fatalf("Unexpected error: %v", err)
			} else if test.err != nil && (err == nil || err.Error() != test.err.Error()) {
				t.Fatalf("Expected error to contain %q, got %v", test.err, err)
			}
		})
	}
}

func TestMQTTParseOptions(t *testing.T) {
	for _, test := range []struct {
		name     string
		content  string
		checkOpt func(*MQTTOpts) error
		err      string
	}{
		// Negative tests
		{"bad type", "mqtt: []", nil, "to be a map"},
		{"bad listen", "mqtt: { listen: [] }", nil, "port or host:port"},
		{"bad port", `mqtt: { port: "abc" }`, nil, "not int64"},
		{"bad host", `mqtt: { host: 123 }`, nil, "not string"},
		{"bad tls", `mqtt: { tls: 123 }`, nil, "not map[string]interface {}"},
		{"unknown field", `mqtt: { this_does_not_exist: 123 }`, nil, "unknown"},
		{"ack wait", `mqtt: {ack_wait: abc}`, nil, "invalid duration"},
		{"max ack pending", `mqtt: {max_ack_pending: abc}`, nil, "not int64"},
		{"max ack pending too high", `mqtt: {max_ack_pending: 12345678}`, nil, "invalid value"},
		{"js_api_timeout bad duration", `mqtt: {js_api_timeout: abc}`, nil, "invalid duration"},
		// Positive tests
		{"tls gen fails", `
			mqtt {
				tls {
					cert_file: "./configs/certs/server.pem"
				}
			}`, nil, "missing 'key_file'"},
		{"listen port only", `mqtt { listen: 1234 }`, func(o *MQTTOpts) error {
			if o.Port != 1234 {
				return fmt.Errorf("expected 1234, got %v", o.Port)
			}
			return nil
		}, ""},
		{"listen host and port", `mqtt { listen: "localhost:1234" }`, func(o *MQTTOpts) error {
			if o.Host != "localhost" || o.Port != 1234 {
				return fmt.Errorf("expected localhost:1234, got %v:%v", o.Host, o.Port)
			}
			return nil
		}, ""},
		{"host", `mqtt { host: "localhost" }`, func(o *MQTTOpts) error {
			if o.Host != "localhost" {
				return fmt.Errorf("expected localhost, got %v", o.Host)
			}
			return nil
		}, ""},
		{"port", `mqtt { port: 1234 }`, func(o *MQTTOpts) error {
			if o.Port != 1234 {
				return fmt.Errorf("expected 1234, got %v", o.Port)
			}
			return nil
		}, ""},
		{"tls config",
			`
			mqtt {
				tls {
					cert_file: "./configs/certs/server.pem"
					key_file: "./configs/certs/key.pem"
				}
			}
			`, func(o *MQTTOpts) error {
				if o.TLSConfig == nil {
					return fmt.Errorf("TLSConfig should have been set")
				}
				return nil
			}, ""},
		{"no auth user",
			`
			mqtt {
				no_auth_user: "noauthuser"
			}
			`, func(o *MQTTOpts) error {
				if o.NoAuthUser != "noauthuser" {
					return fmt.Errorf("Invalid NoAuthUser value: %q", o.NoAuthUser)
				}
				return nil
			}, ""},
		{"auth block",
			`
			mqtt {
				authorization {
					user: "mqttuser"
					password: "pwd"
					token: "token"
					timeout: 2.0
				}
			}
			`, func(o *MQTTOpts) error {
				if o.Username != "mqttuser" || o.Password != "pwd" || o.Token != "token" || o.AuthTimeout != 2.0 {
					return fmt.Errorf("Invalid auth block: %+v", o)
				}
				return nil
			}, ""},
		{"auth timeout as int",
			`
			mqtt {
				authorization {
					timeout: 2
				}
			}
			`, func(o *MQTTOpts) error {
				if o.AuthTimeout != 2.0 {
					return fmt.Errorf("Invalid auth timeout: %v", o.AuthTimeout)
				}
				return nil
			}, ""},
		{"ack wait",
			`
			mqtt {
				ack_wait: "10s"
			}
			`, func(o *MQTTOpts) error {
				if o.AckWait != 10*time.Second {
					return fmt.Errorf("Invalid ack wait: %v", o.AckWait)
				}
				return nil
			}, ""},
		{"max ack pending",
			`
			mqtt {
				max_ack_pending: 123
			}
			`, func(o *MQTTOpts) error {
				if o.MaxAckPending != 123 {
					return fmt.Errorf("Invalid max ack pending: %v", o.MaxAckPending)
				}
				return nil
			}, ""},
		{"reject_qos2_publish",
			`
			mqtt {
				reject_qos2_publish: true
			}
			`, func(o *MQTTOpts) error {
				if !o.rejectQoS2Pub {
					return fmt.Errorf("Invalid: expected rejectQoS2Pub to be set")
				}
				return nil
			}, ""},
		{"downgrade_qos2_subscribe",
			`
			mqtt {
				downgrade_qos2_subscribe: true
			}
			`, func(o *MQTTOpts) error {
				if !o.downgradeQoS2Sub {
					return fmt.Errorf("Invalid: expected downgradeQoS2Sub to be set")
				}
				return nil
			}, ""},
		{"js_api_timeout",
			`
			mqtt {
				js_api_timeout: "60s"
			}
			`, func(o *MQTTOpts) error {
				if o.JSAPITimeout != 60*time.Second {
					return fmt.Errorf("Invalid JS API timeout: %v", o.JSAPITimeout)
				}
				return nil
			}, ""},
	} {
		t.Run(test.name, func(t *testing.T) {
			conf := createConfFile(t, []byte(test.content))
			o, err := ProcessConfigFile(conf)
			if test.err != _EMPTY_ {
				if err == nil || !strings.Contains(err.Error(), test.err) {
					t.Fatalf("For content: %q, expected error about %q, got %v", test.content, test.err, err)
				}
				return
			} else if err != nil {
				t.Fatalf("Unexpected error for content %q: %v", test.content, err)
			}
			if err := test.checkOpt(&o.MQTT); err != nil {
				t.Fatalf("Incorrect option for content %q: %v", test.content, err.Error())
			}
		})
	}
}

func TestMQTTStart(t *testing.T) {
	o := testMQTTDefaultOptions()
	s := testMQTTRunServer(t, o)
	defer testMQTTShutdownServer(s)

	nc, err := net.Dial("tcp", net.JoinHostPort(o.MQTT.Host, fmt.Sprintf("%d", o.MQTT.Port)))
	if err != nil {
		t.Fatalf("Unable to create tcp connection to mqtt port: %v", err)
	}
	nc.Close()

	// Check failure to start due to port in use
	o2 := testMQTTDefaultOptions()
	o2.MQTT.Port = o.MQTT.Port
	s2, err := NewServer(o2)
	if err != nil {
		t.Fatalf("Error creating server: %v", err)
	}
	defer s2.Shutdown()
	l := &captureFatalLogger{fatalCh: make(chan string, 1)}
	s2.SetLogger(l, false, false)
	s2.Start()

	select {
	case e := <-l.fatalCh:
		if !strings.Contains(e, "Unable to listen for MQTT connections") {
			t.Fatalf("Unexpected error: %q", e)
		}
	case <-time.After(time.Second):
		t.Fatal("Should have gotten a fatal error")
	}
}

func TestMQTTTLS(t *testing.T) {
	o := testMQTTDefaultTLSOptions(t, false)
	s := testMQTTRunServer(t, o)
	defer testMQTTShutdownServer(s)

	c, _, err := testMQTTConnectRetryWithError(t, &mqttConnInfo{tls: true}, o.MQTT.Host, o.MQTT.Port, 0)
	if err != nil {
		t.Fatal(err)
	}
	c.Close()
	c = nil
	testMQTTShutdownServer(s)

	// Force client cert verification
	o = testMQTTDefaultTLSOptions(t, true)
	s = testMQTTRunServer(t, o)
	defer testMQTTShutdownServer(s)

	c, _, err = testMQTTConnectRetryWithError(t, &mqttConnInfo{tls: true}, o.MQTT.Host, o.MQTT.Port, 0)
	if err == nil || c != nil {
		if c != nil {
			c.Close()
		}
		t.Fatal("Handshake expected to fail since client did not provide cert")
	}

	// Add client cert.
	tc := &TLSConfigOpts{
		CertFile: "../test/configs/certs/client-cert.pem",
		KeyFile:  "../test/configs/certs/client-key.pem",
	}
	tlsc, err := GenTLSConfig(tc)
	if err != nil {
		t.Fatalf("Error generating tls config: %v", err)
	}
	tlsc.InsecureSkipVerify = true
	c, _, err = testMQTTConnectRetryWithError(t, &mqttConnInfo{
		tls:  true,
		tlsc: tlsc,
	}, o.MQTT.Host, o.MQTT.Port, 0)
	if err != nil {
		t.Fatal(err)
	}
	c.Close()
	c = nil
	testMQTTShutdownServer(s)

	// Lower TLS timeout so low that we should fail
	o.MQTT.TLSTimeout = 0.001
	s = testMQTTRunServer(t, o)
	defer testMQTTShutdownServer(s)

	nc, err := net.Dial("tcp", net.JoinHostPort(o.MQTT.Host, fmt.Sprintf("%d", o.MQTT.Port)))
	if err != nil {
		t.Fatalf("Unable to create tcp connection to mqtt port: %v", err)
	}
	defer nc.Close()
	time.Sleep(100 * time.Millisecond)
	tlsConn := tls.Client(nc, tlsc)
	tlsConn.SetDeadline(time.Now().Add(time.Second))
	if err := tlsConn.Handshake(); err == nil {
		t.Fatal("Expected failure, did not get one")
	}
}

type mqttConnInfo struct {
	clientID  string
	cleanSess bool
	keepAlive uint16
	will      *mqttWill
	user      string
	pass      string
	ws        bool
	tls       bool
	tlsc      *tls.Config
}

func testMQTTGetClient(t testing.TB, s *Server, clientID string) *client {
	t.Helper()
	var mc *client
	s.mu.Lock()
	for _, c := range s.clients {
		c.mu.Lock()
		if c.isMqtt() && c.mqtt.cid == clientID {
			mc = c
		}
		c.mu.Unlock()
		if mc != nil {
			break
		}
	}
	s.mu.Unlock()
	if mc == nil {
		t.Fatalf("Did not find client %q", clientID)
	}
	return mc
}

func testMQTTGetAccountSessionManager(t *testing.T, s *Server, cid string) *mqttAccountSessionManager {
	t.Helper()
	c := testMQTTGetClient(t, s, cid)
	require_NotNil(t, c)
	asm := c.mqtt.asm
	require_NotNil(t, asm)
	return asm
}

func testMQTTRead(c net.Conn) ([]byte, error) {
	var buf [512]byte
	// Make sure that test does not block
	c.SetReadDeadline(time.Now().Add(testMQTTTimeout))
	n, err := c.Read(buf[:])
	if err != nil {
		return nil, err
	}
	c.SetReadDeadline(time.Time{})
	return copyBytes(buf[:n]), nil
}

func testMQTTWrite(c net.Conn, buf []byte) (int, error) {
	c.SetWriteDeadline(time.Now().Add(testMQTTTimeout))
	n, err := c.Write(buf)
	c.SetWriteDeadline(time.Time{})
	return n, err
}

func testMQTTConnect(t testing.TB, ci *mqttConnInfo, host string, port int) (net.Conn, *mqttReader) {
	t.Helper()
	return testMQTTConnectRetry(t, ci, host, port, 0)
}

func testMQTTConnectRetry(t testing.TB, ci *mqttConnInfo, host string, port int, retryCount int) (net.Conn, *mqttReader) {
	t.Helper()
	c, r, err := testMQTTConnectRetryWithError(t, ci, host, port, retryCount)
	if err != nil {
		t.Fatal(err)
	}
	return c, r
}

func testMQTTConnectRetryWithError(t testing.TB, ci *mqttConnInfo, host string, port int, retryCount int) (net.Conn, *mqttReader, error) {
	retry := func(c net.Conn) bool {
		if c != nil {
			c.Close()
		}
		if retryCount == 0 {
			return false
		}
		time.Sleep(time.Second)
		retryCount--
		return true
	}

	addr := net.JoinHostPort(host, fmt.Sprintf("%d", port))
	var c net.Conn
	var err error
RETRY:
	if ci.ws {
		var br *bufio.Reader
		c, br, _, err = testNewWSClientWithError(t, testWSClientOptions{
			host:  host,
			port:  port,
			noTLS: !ci.tls,
			path:  mqttWSPath,
		})
		if err == nil {
			c = &mqttWrapAsWs{Conn: c, t: t, br: br}
		}
	} else {
		c, err = net.Dial("tcp", addr)
		if err == nil && ci.tls {
			tc := ci.tlsc
			if tc == nil {
				tc = &tls.Config{InsecureSkipVerify: true}
			}
			c = tls.Client(c, tc)
			c.SetDeadline(time.Now().Add(time.Second))
			err = c.(*tls.Conn).Handshake()
			c.SetDeadline(time.Time{})
		}
	}
	if err != nil {
		if retry(c) {
			goto RETRY
		}
		return nil, nil, fmt.Errorf("Error creating mqtt connection: %v", err)
	}

	proto := mqttCreateConnectProto(ci)
	if _, err := testMQTTWrite(c, proto); err != nil {
		if retry(c) {
			goto RETRY
		}
		return nil, nil, fmt.Errorf("Error writing connect: %v", err)
	}

	buf, err := testMQTTRead(c)
	if err != nil {
		if retry(c) {
			goto RETRY
		}
		return nil, nil, fmt.Errorf("Error reading: %v", err)
	}
	mr := &mqttReader{reader: c}
	mr.reset(buf)

	return c, mr, nil
}

func mqttCreateConnectProto(ci *mqttConnInfo) []byte {
	flags := byte(0)
	if ci.cleanSess {
		flags |= mqttConnFlagCleanSession
	}
	if ci.will != nil {
		flags |= mqttConnFlagWillFlag | (ci.will.qos << 3)
		if ci.will.retain {
			flags |= mqttConnFlagWillRetain
		}
	}
	if ci.user != _EMPTY_ {
		flags |= mqttConnFlagUsernameFlag
	}
	if ci.pass != _EMPTY_ {
		flags |= mqttConnFlagPasswordFlag
	}

	pkLen := 2 + len(mqttProtoName) +
		1 + // proto level
		1 + // flags
		2 + // keepAlive
		2 + len(ci.clientID)

	if ci.will != nil {
		pkLen += 2 + len(ci.will.topic)
		pkLen += 2 + len(ci.will.message)
	}
	if ci.user != _EMPTY_ {
		pkLen += 2 + len(ci.user)
	}
	if ci.pass != _EMPTY_ {
		pkLen += 2 + len(ci.pass)
	}

	w := newMQTTWriter(0)
	w.WriteByte(mqttPacketConnect)
	w.WriteVarInt(pkLen)
	w.WriteString(string(mqttProtoName))
	w.WriteByte(0x4)
	w.WriteByte(flags)
	w.WriteUint16(ci.keepAlive)
	w.WriteString(ci.clientID)
	if ci.will != nil {
		w.WriteBytes(ci.will.topic)
		w.WriteBytes(ci.will.message)
	}
	if ci.user != _EMPTY_ {
		w.WriteString(ci.user)
	}
	if ci.pass != _EMPTY_ {
		w.WriteBytes([]byte(ci.pass))
	}
	return w.Bytes()
}

func testMQTTCheckConnAck(t testing.TB, r *mqttReader, rc byte, sessionPresent bool) {
	t.Helper()
	b, pl := testMQTTReadPacket(t, r)
	pt := b & mqttPacketMask
	if pt != mqttPacketConnectAck {
		t.Fatalf("Expected ConnAck (%x), got %x", mqttPacketConnectAck, pt)
	}
	if pl != 2 {
		t.Fatalf("ConnAck packet length should be 2, got %v", pl)
	}
	caf, err := r.readByte("connack flags")
	if err != nil {
		t.Fatalf("Error reading packet length: %v", err)
	}
	if caf&0xfe != 0 {
		t.Fatalf("ConnAck flag bits 7-1 should all be 0, got %x", caf>>1)
	}
	if sp := caf == 1; sp != sessionPresent {
		t.Fatalf("Expected session present flag=%v got %v", sessionPresent, sp)
	}
	carc, err := r.readByte("connack return code")
	if err != nil {
		t.Fatalf("Error reading returned code: %v", err)
	}
	if carc != rc {
		t.Fatalf("Expected return code to be %v, got %v", rc, carc)
	}
}

func testMQTTCheckPubAck(t testing.TB, r *mqttReader, packetType byte) {
	t.Helper()
	b, pl := testMQTTReadPacket(t, r)
	pt := b & mqttPacketMask
	if pt != packetType {
		t.Fatalf("Expected %x, got %x", packetType, pt)
	}
	r.pos += pl
}

func TestMQTTRequiresJSEnabled(t *testing.T) {
	o := testMQTTDefaultOptions()
	acc := NewAccount("mqtt")
	o.Accounts = []*Account{acc}
	o.Users = []*User{{Username: "mqtt", Account: acc}}
	s := testMQTTRunServer(t, o)
	defer testMQTTShutdownServer(s)

	addr := net.JoinHostPort(o.MQTT.Host, fmt.Sprintf("%d", o.MQTT.Port))
	c, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatalf("Error creating mqtt connection: %v", err)
	}
	defer c.Close()

	proto := mqttCreateConnectProto(&mqttConnInfo{cleanSess: true, user: "mqtt"})
	if _, err := testMQTTWrite(c, proto); err != nil {
		t.Fatalf("Error writing connect: %v", err)
	}
	if _, err := testMQTTRead(c); err == nil {
		t.Fatal("Expected failure, did not get one")
	}
}

func testMQTTEnableJSForAccount(t *testing.T, s *Server, accName string) {
	t.Helper()
	acc, err := s.LookupAccount(accName)
	if err != nil {
		t.Fatalf("Error looking up account: %v", err)
	}
	limits := map[string]JetStreamAccountLimits{
		_EMPTY_: {
			MaxConsumers: -1,
			MaxStreams:   -1,
			MaxMemory:    1024 * 1024,
			MaxStore:     1024 * 1024,
		},
	}
	if err := acc.EnableJetStream(limits, nil); err != nil {
		t.Fatalf("Error enabling JS: %v", err)
	}
}

func TestMQTTTLSVerifyAndMap(t *testing.T) {
	accName := "MyAccount"
	acc := NewAccount(accName)
	certUserName := "CN=example.com,OU=NATS.io"
	users := []*User{{Username: certUserName, Account: acc}}

	for _, test := range []struct {
		name        string
		filtering   bool
		provideCert bool
	}{
		{"no filtering, client provides cert", false, true},
		{"no filtering, client does not provide cert", false, false},
		{"filtering, client provides cert", true, true},
		{"filtering, client does not provide cert", true, false},
	} {
		t.Run(test.name, func(t *testing.T) {
			o := testMQTTDefaultOptions()
			o.Host = "localhost"
			o.Accounts = []*Account{acc}
			o.Users = users
			if test.filtering {
				o.Users[0].AllowedConnectionTypes = testCreateAllowedConnectionTypes([]string{jwt.ConnectionTypeStandard, jwt.ConnectionTypeMqtt})
			}
			tc := &TLSConfigOpts{
				CertFile: "../test/configs/certs/tlsauth/server.pem",
				KeyFile:  "../test/configs/certs/tlsauth/server-key.pem",
				CaFile:   "../test/configs/certs/tlsauth/ca.pem",
				Verify:   true,
			}
			tlsc, err := GenTLSConfig(tc)
			if err != nil {
				t.Fatalf("Error creating tls config: %v", err)
			}
			o.MQTT.TLSConfig = tlsc
			o.MQTT.TLSTimeout = 2.0
			o.MQTT.TLSMap = true
			s := testMQTTRunServer(t, o)
			defer testMQTTShutdownServer(s)

			testMQTTEnableJSForAccount(t, s, accName)

			tlscc := &tls.Config{}
			if test.provideCert {
				tc := &TLSConfigOpts{
					CertFile: "../test/configs/certs/tlsauth/client.pem",
					KeyFile:  "../test/configs/certs/tlsauth/client-key.pem",
				}
				var err error
				tlscc, err = GenTLSConfig(tc)
				if err != nil {
					t.Fatalf("Error generating tls config: %v", err)
				}
			}
			tlscc.InsecureSkipVerify = true
			if test.provideCert {
				tlscc.MinVersion = tls.VersionTLS13
			}
			mc, r, err := testMQTTConnectRetryWithError(t, &mqttConnInfo{
				cleanSess: true,
				tls:       true,
				tlsc:      tlscc,
			}, o.MQTT.Host, o.MQTT.Port, 0)
			if !test.provideCert {
				if err == nil {
					t.Fatal("Expected error, did not get one")
				} else if !strings.Contains(err.Error(), "bad certificate") && !strings.Contains(err.Error(), "certificate required") {
					t.Fatalf("Unexpected error: %v", err)
				}
				return
			}
			if err != nil {
				t.Fatalf("Error reading: %v", err)
			}
			defer mc.Close()
			testMQTTCheckConnAck(t, r, mqttConnAckRCConnectionAccepted, false)

			var c *client
			s.mu.Lock()
			for _, sc := range s.clients {
				sc.mu.Lock()
				if sc.isMqtt() {
					c = sc
				}
				sc.mu.Unlock()
				if c != nil {
					break
				}
			}
			s.mu.Unlock()
			if c == nil {
				t.Fatal("Client not found")
			}

			var uname string
			var accname string
			c.mu.Lock()
			uname = c.opts.Username
			if c.acc != nil {
				accname = c.acc.GetName()
			}
			c.mu.Unlock()
			if uname != certUserName {
				t.Fatalf("Expected username %q, got %q", certUserName, uname)
			}
			if accname != accName {
				t.Fatalf("Expected account %q, got %v", accName, accname)
			}
		})
	}
}

func TestMQTTBasicAuth(t *testing.T) {
	for _, test := range []struct {
		name string
		opts func() *Options
		user string
		pass string
		rc   byte
	}{
		{
			"top level auth, no override, wrong u/p",
			func() *Options {
				o := testMQTTDefaultOptions()
				o.Username = "normal"
				o.Password = "client"
				return o
			},
			"mqtt", "client", mqttConnAckRCNotAuthorized,
		},
		{
			"top level auth, no override, correct u/p",
			func() *Options {
				o := testMQTTDefaultOptions()
				o.Username = "normal"
				o.Password = "client"
				return o
			},
			"normal", "client", mqttConnAckRCConnectionAccepted,
		},
		{
			"no top level auth, mqtt auth, wrong u/p",
			func() *Options {
				o := testMQTTDefaultOptions()
				o.MQTT.Username = "mqtt"
				o.MQTT.Password = "client"
				return o
			},
			"normal", "client", mqttConnAckRCNotAuthorized,
		},
		{
			"no top level auth, mqtt auth, correct u/p",
			func() *Options {
				o := testMQTTDefaultOptions()
				o.MQTT.Username = "mqtt"
				o.MQTT.Password = "client"
				return o
			},
			"mqtt", "client", mqttConnAckRCConnectionAccepted,
		},
		{
			"top level auth, mqtt override, wrong u/p",
			func() *Options {
				o := testMQTTDefaultOptions()
				o.Username = "normal"
				o.Password = "client"
				o.MQTT.Username = "mqtt"
				o.MQTT.Password = "client"
				return o
			},
			"normal", "client", mqttConnAckRCNotAuthorized,
		},
		{
			"top level auth, mqtt override, correct u/p",
			func() *Options {
				o := testMQTTDefaultOptions()
				o.Username = "normal"
				o.Password = "client"
				o.MQTT.Username = "mqtt"
				o.MQTT.Password = "client"
				return o
			},
			"mqtt", "client", mqttConnAckRCConnectionAccepted,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			o := test.opts()
			s := testMQTTRunServer(t, o)
			defer testMQTTShutdownServer(s)

			ci := &mqttConnInfo{
				cleanSess: true,
				user:      test.user,
				pass:      test.pass,
			}
			mc, r := testMQTTConnect(t, ci, o.MQTT.Host, o.MQTT.Port)
			defer mc.Close()
			testMQTTCheckConnAck(t, r, test.rc, false)
		})
	}
}

// Test MQTT over websocket authentication. MQTT uses its own override,
// or global authentication, regardless of whether it uses WebSocket.
// WebSocket authorization does not apply to MQTT clients.
func TestMQTTWebsocketAuth(t *testing.T) {
	for _, test := range []struct {
		name         string
		globalAuth   bool
		mqttOverride bool
		user         string
		pass         string
		rc           byte
	}{
		{"websocket only, no credentials", false, false, "", "", mqttConnAckRCConnectionAccepted},
		{"websocket only, wrong credentials", false, false, "ws", "wrong", mqttConnAckRCConnectionAccepted},
		{"websocket only, websocket credentials", false, false, "ws", "pwd", mqttConnAckRCConnectionAccepted},
		{"global auth, no credentials", true, false, "", "", mqttConnAckRCNotAuthorized},
		{"global auth, wrong password", true, false, "global", "wrong", mqttConnAckRCNotAuthorized},
		{"global auth, global credentials", true, false, "global", "pwd", mqttConnAckRCConnectionAccepted},
		{"global auth, websocket credentials", true, false, "ws", "pwd", mqttConnAckRCNotAuthorized},
		{"mqtt override, no credentials", false, true, "", "", mqttConnAckRCNotAuthorized},
		{"mqtt override, wrong password", false, true, "mqtt", "wrong", mqttConnAckRCNotAuthorized},
		{"mqtt override, mqtt credentials", false, true, "mqtt", "pwd", mqttConnAckRCConnectionAccepted},
		{"mqtt override, websocket credentials", false, true, "ws", "pwd", mqttConnAckRCNotAuthorized},
		{"global and mqtt auth, no credentials", true, true, "", "", mqttConnAckRCNotAuthorized},
		{"global and mqtt auth, wrong password", true, true, "mqtt", "wrong", mqttConnAckRCNotAuthorized},
		{"global and mqtt auth, global credentials", true, true, "global", "pwd", mqttConnAckRCNotAuthorized},
		{"global and mqtt auth, mqtt credentials", true, true, "mqtt", "pwd", mqttConnAckRCConnectionAccepted},
		{"global and mqtt auth, websocket credentials", true, true, "ws", "pwd", mqttConnAckRCNotAuthorized},
	} {
		t.Run(test.name, func(t *testing.T) {
			o := testMQTTDefaultOptions()
			o.Websocket = WebsocketOpts{
				Host: "127.0.0.1", Port: -1, NoTLS: true,
				Username: "ws", Password: "pwd",
			}
			if test.globalAuth {
				o.Username, o.Password = "global", "pwd"
			}
			if test.mqttOverride {
				o.MQTT.Username, o.MQTT.Password = "mqtt", "pwd"
			}
			s := testMQTTRunServer(t, o)
			defer testMQTTShutdownServer(s)

			for _, ws := range []bool{false, true} {
				t.Logf("MQTT websocket=%v", ws)
				host, port := o.MQTT.Host, o.MQTT.Port
				if ws {
					host, port = o.Websocket.Host, o.Websocket.Port
				}
				ci := &mqttConnInfo{cleanSess: true, ws: ws, user: test.user, pass: test.pass}
				c, r := testMQTTConnect(t, ci, host, port)
				testMQTTCheckConnAck(t, r, test.rc, false)
				c.Close()
			}
		})
	}
}

func TestMQTTAuthTimeout(t *testing.T) {
	for _, test := range []struct {
		name string
		at   float64
		mat  float64
		ok   bool
	}{
		{"use top-level auth timeout", 0.5, 0.0, true},
		{"use mqtt auth timeout", 0.5, 0.05, false},
	} {
		t.Run(test.name, func(t *testing.T) {
			o := testMQTTDefaultOptions()
			o.AuthTimeout = test.at
			o.MQTT.Username = "mqtt"
			o.MQTT.Password = "client"
			o.MQTT.AuthTimeout = test.mat
			s := testMQTTRunServer(t, o)
			defer testMQTTShutdownServer(s)

			mc, err := net.Dial("tcp", net.JoinHostPort(o.MQTT.Host, fmt.Sprintf("%d", o.MQTT.Port)))
			if err != nil {
				t.Fatalf("Error connecting: %v", err)
			}
			defer mc.Close()

			time.Sleep(100 * time.Millisecond)

			ci := &mqttConnInfo{
				cleanSess: true,
				user:      "mqtt",
				pass:      "client",
			}
			proto := mqttCreateConnectProto(ci)
			if _, err := testMQTTWrite(mc, proto); err != nil {
				if test.ok {
					t.Fatalf("Error sending connect: %v", err)
				}
				// else it is ok since we got disconnected due to auth timeout
				return
			}
			buf, err := testMQTTRead(mc)
			if err != nil {
				if test.ok {
					t.Fatalf("Error reading: %v", err)
				}
				// else it is ok since we got disconnected due to auth timeout
				return
			}
			r := &mqttReader{reader: mc}
			r.reset(buf)
			testMQTTCheckConnAck(t, r, mqttConnAckRCConnectionAccepted, false)

			time.Sleep(500 * time.Millisecond)
			testMQTTPublish(t, mc, r, 1, false, false, "foo", 1, []byte("msg"))
		})
	}
}

func TestMQTTTokenAuth(t *testing.T) {
	for _, test := range []struct {
		name  string
		opts  func() *Options
		token string
		rc    byte
	}{
		{
			"top level auth, no override, wrong token",
			func() *Options {
				o := testMQTTDefaultOptions()
				o.Authorization = "goodtoken"
				return o
			},
			"badtoken", mqttConnAckRCNotAuthorized,
		},
		{
			"top level auth, no override, correct token",
			func() *Options {
				o := testMQTTDefaultOptions()
				o.Authorization = "goodtoken"
				return o
			},
			"goodtoken", mqttConnAckRCConnectionAccepted,
		},
		{
			"no top level auth, mqtt auth, wrong token",
			func() *Options {
				o := testMQTTDefaultOptions()
				o.MQTT.Token = "goodtoken"
				return o
			},
			"badtoken", mqttConnAckRCNotAuthorized,
		},
		{
			"no top level auth, mqtt auth, correct token",
			func() *Options {
				o := testMQTTDefaultOptions()
				o.MQTT.Token = "goodtoken"
				return o
			},
			"goodtoken", mqttConnAckRCConnectionAccepted,
		},
		{
			"top level auth, mqtt override, wrong token",
			func() *Options {
				o := testMQTTDefaultOptions()
				o.Authorization = "clienttoken"
				o.MQTT.Token = "mqtttoken"
				return o
			},
			"clienttoken", mqttConnAckRCNotAuthorized,
		},
		{
			"top level auth, mqtt override, correct token",
			func() *Options {
				o := testMQTTDefaultOptions()
				o.Authorization = "clienttoken"
				o.MQTT.Token = "mqtttoken"
				return o
			},
			"mqtttoken", mqttConnAckRCConnectionAccepted,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			o := test.opts()
			s := testMQTTRunServer(t, o)
			defer testMQTTShutdownServer(s)

			ci := &mqttConnInfo{
				cleanSess: true,
				user:      "ignore_use_token",
				pass:      test.token,
			}
			mc, r := testMQTTConnect(t, ci, o.MQTT.Host, o.MQTT.Port)
			defer mc.Close()
			testMQTTCheckConnAck(t, r, test.rc, false)
		})
	}
}

func TestMQTTJWTWithAllowedConnectionTypes(t *testing.T) {
	o := testMQTTDefaultOptions()
	// Create System Account
	syskp, _ := nkeys.CreateAccount()
	syspub, _ := syskp.PublicKey()
	sysAc := jwt.NewAccountClaims(syspub)
	sysjwt, err := sysAc.Encode(oKp)
	if err != nil {
		t.Fatalf("Error generating account JWT: %v", err)
	}
	// Create memory resolver and store system account
	mr := &MemAccResolver{}
	mr.Store(syspub, sysjwt)
	if err != nil {
		t.Fatalf("Error saving system account JWT to memory resolver: %v", err)
	}
	// Add system account and memory resolver to server options
	o.SystemAccount = syspub
	o.AccountResolver = mr
	setupAddTrusted(o)

	s := testMQTTRunServer(t, o)
	defer testMQTTShutdownServer(s)

	for _, test := range []struct {
		name            string
		connectionTypes []string
		rc              byte
	}{
		{"not allowed", []string{jwt.ConnectionTypeStandard}, mqttConnAckRCNotAuthorized},
		{"allowed", []string{jwt.ConnectionTypeStandard, strings.ToLower(jwt.ConnectionTypeMqtt)}, mqttConnAckRCConnectionAccepted},
		{"allowed with unknown", []string{jwt.ConnectionTypeMqtt, "SomeNewType"}, mqttConnAckRCConnectionAccepted},
		{"not allowed with unknown", []string{"SomeNewType"}, mqttConnAckRCNotAuthorized},
	} {
		t.Run(test.name, func(t *testing.T) {

			nuc := newJWTTestUserClaims()
			nuc.AllowedConnectionTypes = test.connectionTypes
			nuc.BearerToken = true

			okp, _ := nkeys.FromSeed(oSeed)

			akp, _ := nkeys.CreateAccount()
			apub, _ := akp.PublicKey()
			nac := jwt.NewAccountClaims(apub)
			// Enable Jetstream on account with lax limitations
			nac.Limits.JetStreamLimits.Consumer = -1
			nac.Limits.JetStreamLimits.Streams = -1
			nac.Limits.JetStreamLimits.MemoryStorage = 1024 * 1024
			nac.Limits.JetStreamLimits.DiskStorage = 1024 * 1024
			ajwt, err := nac.Encode(okp)
			if err != nil {
				t.Fatalf("Error generating account JWT: %v", err)
			}

			nkp, _ := nkeys.CreateUser()
			pub, _ := nkp.PublicKey()
			nuc.Subject = pub
			jwt, err := nuc.Encode(akp)
			if err != nil {
				t.Fatalf("Error generating user JWT: %v", err)
			}

			addAccountToMemResolver(s, apub, ajwt)

			ci := &mqttConnInfo{
				cleanSess: true,
				user:      "ignore_use_token",
				pass:      jwt,
			}

			mc, r := testMQTTConnect(t, ci, o.MQTT.Host, o.MQTT.Port)
			defer mc.Close()
			testMQTTCheckConnAck(t, r, test.rc, false)
		})
	}
}

func TestMQTTUsersAuth(t *testing.T) {
	users := []*User{{Username: "user", Password: "pwd"}}
	for _, test := range []struct {
		name string
		opts func() *Options
		user string
		pass string
		rc   byte
	}{
		{
			"no filtering, wrong user",
			func() *Options {
				o := testMQTTDefaultOptions()
				o.Users = users
				return o
			},
			"wronguser", "pwd", mqttConnAckRCNotAuthorized,
		},
		{
			"no filtering, correct user",
			func() *Options {
				o := testMQTTDefaultOptions()
				o.Users = users
				return o
			},
			"user", "pwd", mqttConnAckRCConnectionAccepted,
		},
		{
			"filtering, user not allowed",
			func() *Options {
				o := testMQTTDefaultOptions()
				o.Users = users
				// Only allowed for regular clients
				o.Users[0].AllowedConnectionTypes = testCreateAllowedConnectionTypes([]string{jwt.ConnectionTypeStandard})
				return o
			},
			"user", "pwd", mqttConnAckRCNotAuthorized,
		},
		{
			"filtering, user allowed",
			func() *Options {
				o := testMQTTDefaultOptions()
				o.Users = users
				o.Users[0].AllowedConnectionTypes = testCreateAllowedConnectionTypes([]string{jwt.ConnectionTypeStandard, jwt.ConnectionTypeMqtt})
				return o
			},
			"user", "pwd", mqttConnAckRCConnectionAccepted,
		},
		{
			"filtering, wrong password",
			func() *Options {
				o := testMQTTDefaultOptions()
				o.Users = users
				o.Users[0].AllowedConnectionTypes = testCreateAllowedConnectionTypes([]string{jwt.ConnectionTypeStandard, jwt.ConnectionTypeMqtt})
				return o
			},
			"user", "badpassword", mqttConnAckRCNotAuthorized,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			o := test.opts()
			s := testMQTTRunServer(t, o)
			defer testMQTTShutdownServer(s)

			ci := &mqttConnInfo{
				cleanSess: true,
				user:      test.user,
				pass:      test.pass,
			}
			mc, r := testMQTTConnect(t, ci, o.MQTT.Host, o.MQTT.Port)
			defer mc.Close()
			testMQTTCheckConnAck(t, r, test.rc, false)
		})
	}
}

func TestMQTTNoAuthUserValidation(t *testing.T) {
	o := testMQTTDefaultOptions()
	o.Users = []*User{{Username: "user", Password: "pwd"}}
	// Should fail because it is not part of o.Users.
	o.MQTT.NoAuthUser = "notfound"
	if _, err := NewServer(o); err == nil || !strings.Contains(err.Error(), "not present as user") {
		t.Fatalf("Expected error saying not present as user, got %v", err)
	}

	// Set a valid no auth user for global options, but still should fail because
	// of o.MQTT.NoAuthUser
	o.NoAuthUser = "user"
	o.MQTT.NoAuthUser = "notfound"
	if _, err := NewServer(o); err == nil || !strings.Contains(err.Error(), "not present as user") {
		t.Fatalf("Expected error saying not present as user, got %v", err)
	}
}

func TestMQTTNoAuthUser(t *testing.T) {
	for _, test := range []struct {
		name         string
		override     bool
		useAuth      bool
		expectedUser string
		expectedAcc  string
	}{
		{"no override, no user provided", false, false, "noauth", "normal"},
		{"no override, user povided", false, true, "user", "normal"},
		{"override, no user provided", true, false, "mqttnoauth", "mqtt"},
		{"override, user provided", true, true, "mqttuser", "mqtt"},
	} {
		t.Run(test.name, func(t *testing.T) {
			o := testMQTTDefaultOptions()
			normalAcc := NewAccount("normal")
			mqttAcc := NewAccount("mqtt")
			o.Accounts = []*Account{normalAcc, mqttAcc}
			o.Users = []*User{
				{Username: "noauth", Password: "pwd", Account: normalAcc},
				{Username: "user", Password: "pwd", Account: normalAcc},
				{Username: "mqttnoauth", Password: "pwd", Account: mqttAcc},
				{Username: "mqttuser", Password: "pwd", Account: mqttAcc},
			}
			o.NoAuthUser = "noauth"
			if test.override {
				o.MQTT.NoAuthUser = "mqttnoauth"
			}
			s := testMQTTRunServer(t, o)
			defer testMQTTShutdownServer(s)

			testMQTTEnableJSForAccount(t, s, "normal")
			testMQTTEnableJSForAccount(t, s, "mqtt")

			ci := &mqttConnInfo{clientID: "mqtt", cleanSess: true}
			if test.useAuth {
				ci.user = test.expectedUser
				ci.pass = "pwd"
			}
			mc, r := testMQTTConnect(t, ci, o.MQTT.Host, o.MQTT.Port)
			defer mc.Close()
			testMQTTCheckConnAck(t, r, mqttConnAckRCConnectionAccepted, false)

			c := testMQTTGetClient(t, s, "mqtt")
			c.mu.Lock()
			uname := c.opts.Username
			aname := c.acc.GetName()
			c.mu.Unlock()
			if uname != test.expectedUser {
				t.Fatalf("Expected selected user to be %q, got %q", test.expectedUser, uname)
			}
			if aname != test.expectedAcc {
				t.Fatalf("Expected selected account to be %q, got %q", test.expectedAcc, aname)
			}
		})
	}
}

func TestMQTTConnectNotFirstPacket(t *testing.T) {
	o := testMQTTDefaultOptions()
	s := testMQTTRunServer(t, o)
	defer testMQTTShutdownServer(s)

	l := &captureErrorLogger{errCh: make(chan string, 10)}
	s.SetLogger(l, false, false)

	c, err := net.Dial("tcp", net.JoinHostPort(o.MQTT.Host, fmt.Sprintf("%d", o.MQTT.Port)))
	if err != nil {
		t.Fatalf("Error on dial: %v", err)
	}
	defer c.Close()

	testMQTTSendPublishPacket(t, c, 0, false, false, "foo", 0, []byte("hello"))
	testMQTTExpectDisconnect(t, c)

	select {
	case err := <-l.errCh:
		if !strings.Contains(err, "should be a CONNECT") {
			t.Fatalf("Expected error about first packet being a CONNECT, got %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("Did not log any error")
	}
}

func TestMQTTSecondConnect(t *testing.T) {
	o := testMQTTDefaultOptions()
	s := testMQTTRunServer(t, o)
	defer testMQTTShutdownServer(s)

	mc, r := testMQTTConnect(t, &mqttConnInfo{cleanSess: true}, o.MQTT.Host, o.MQTT.Port)
	defer mc.Close()
	testMQTTCheckConnAck(t, r, mqttConnAckRCConnectionAccepted, false)

	proto := mqttCreateConnectProto(&mqttConnInfo{cleanSess: true})
	if _, err := testMQTTWrite(mc, proto); err != nil {
		t.Fatalf("Error writing connect: %v", err)
	}
	testMQTTExpectDisconnect(t, mc)
}

func TestMQTTParseConnect(t *testing.T) {
	for _, test := range []struct {
		name    string
		proto   []byte
		err     string
		wantErr bool
	}{
		{"packet in buffer error", []byte{0}, io.ErrUnexpectedEOF.Error(), false},
		{"bad proto name", []byte{0, 4, 'B', 'A', 'D'}, "protocol name", false},
		{"invalid proto name", []byte{0, 3, 'B', 'A', 'D'}, "expected connect packet with protocol name", false},
		{"old proto not supported", []byte{0, 6, 'M', 'Q', 'I', 's', 'd', 'p'}, "older protocol", false},
		{"error on protocol level", []byte{0, 4, 'M', 'Q', 'T', 'T'}, "protocol level", false},
		{"unacceptable protocol version", []byte{0, 4, 'M', 'Q', 'T', 'T', 10}, "unacceptable protocol version", false},
		{"error on flags", []byte{0, 4, 'M', 'Q', 'T', 'T', mqttProtoLevel}, "flags", false},
		{"reserved flag", []byte{0, 4, 'M', 'Q', 'T', 'T', mqttProtoLevel, 1}, errMQTTConnFlagReserved.Error(), false},
		{"will qos without will flag", []byte{0, 4, 'M', 'Q', 'T', 'T', mqttProtoLevel, 1 << 3}, "if Will flag is set to 0, Will QoS must be 0 too", false},
		{"will retain without will flag", []byte{0, 4, 'M', 'Q', 'T', 'T', mqttProtoLevel, 1 << 5}, errMQTTWillAndRetainFlag.Error(), false},
		{"will qos", []byte{0, 4, 'M', 'Q', 'T', 'T', mqttProtoLevel, 3<<3 | 1<<2}, "if Will flag is set to 1, Will QoS can be 0, 1 or 2", false},
		{"no user but password", []byte{0, 4, 'M', 'Q', 'T', 'T', mqttProtoLevel, mqttConnFlagPasswordFlag}, errMQTTPasswordFlagAndNoUser.Error(), false},
		{"missing keep alive", []byte{0, 4, 'M', 'Q', 'T', 'T', mqttProtoLevel, 0}, "keep alive", false},
		{"missing client ID", []byte{0, 4, 'M', 'Q', 'T', 'T', mqttProtoLevel, 0, 0, 1}, "client ID", false},
		{"empty client ID", []byte{0, 4, 'M', 'Q', 'T', 'T', mqttProtoLevel, 0, 0, 1, 0, 0}, errMQTTCIDEmptyNeedsCleanFlag.Error(), false},
		{"invalid utf8 client ID", []byte{0, 4, 'M', 'Q', 'T', 'T', mqttProtoLevel, 0, 0, 1, 0, 1, 241}, "invalid utf8 for client ID", false},
		{"client ID with null byte", []byte{0, 4, 'M', 'Q', 'T', 'T', mqttProtoLevel, 0, 0, 1, 0, 3, 'a', 0, 'b'}, "", true},
		{"client ID with invalid character", []byte{0, 4, 'M', 'Q', 'T', 'T', mqttProtoLevel, 0, 0, 1, 0, 1, '.'}, "", true},
		{"missing will topic", []byte{0, 4, 'M', 'Q', 'T', 'T', mqttProtoLevel, mqttConnFlagWillFlag | mqttConnFlagCleanSession, 0, 0, 0, 0}, "Will topic", false},
		{"empty will topic", []byte{0, 4, 'M', 'Q', 'T', 'T', mqttProtoLevel, mqttConnFlagWillFlag | mqttConnFlagCleanSession, 0, 0, 0, 0, 0, 0}, errMQTTEmptyWillTopic.Error(), false},
		{"invalid utf8 will topic", []byte{0, 4, 'M', 'Q', 'T', 'T', mqttProtoLevel, mqttConnFlagWillFlag | mqttConnFlagCleanSession, 0, 0, 0, 0, 0, 1, 241}, "invalid utf8 for Will topic", false},
		{"invalid wildcard will topic", []byte{0, 4, 'M', 'Q', 'T', 'T', mqttProtoLevel, mqttConnFlagWillFlag | mqttConnFlagCleanSession, 0, 0, 0, 0, 0, 1, '#'}, "wildcards not allowed", false},
		{"error on will message", []byte{0, 4, 'M', 'Q', 'T', 'T', mqttProtoLevel, mqttConnFlagWillFlag | mqttConnFlagCleanSession, 0, 0, 0, 0, 0, 1, 'a', 0, 3}, "Will message", false},
		{"error on username", []byte{0, 4, 'M', 'Q', 'T', 'T', mqttProtoLevel, mqttConnFlagUsernameFlag | mqttConnFlagCleanSession, 0, 0, 0, 0}, "user name", false},
		{"empty username", []byte{0, 4, 'M', 'Q', 'T', 'T', mqttProtoLevel, mqttConnFlagUsernameFlag | mqttConnFlagCleanSession, 0, 0, 0, 0, 0, 0}, errMQTTEmptyUsername.Error(), false},
		{"invalid utf8 username", []byte{0, 4, 'M', 'Q', 'T', 'T', mqttProtoLevel, mqttConnFlagUsernameFlag | mqttConnFlagCleanSession, 0, 0, 0, 0, 0, 1, 241}, "invalid utf8 for user name", false},
		{"username with null byte", []byte{0, 4, 'M', 'Q', 'T', 'T', mqttProtoLevel, mqttConnFlagUsernameFlag | mqttConnFlagCleanSession, 0, 0, 0, 0, 0, 3, 'a', 0, 'b'}, "", true},
		{"error on password", []byte{0, 4, 'M', 'Q', 'T', 'T', mqttProtoLevel, mqttConnFlagUsernameFlag | mqttConnFlagPasswordFlag | mqttConnFlagCleanSession, 0, 0, 0, 0, 0, 1, 'a'}, "password", false},
	} {
		t.Run(test.name, func(t *testing.T) {
			r := &mqttReader{}
			r.reset(test.proto)
			mqtt := &mqtt{r: r}
			c := &client{mqtt: mqtt}
			_, _, err := c.mqttParseConnect(r, false)
			if test.wantErr {
				if err == nil {
					t.Fatal("Expected an error, got none")
				}
				return
			}
			if err == nil || !strings.Contains(err.Error(), test.err) {
				t.Fatalf("Expected error %q, got %v", test.err, err)
			}
		})
	}
}

func TestMQTTConnectFailsOnParse(t *testing.T) {
	o := testMQTTDefaultOptions()
	s := testMQTTRunServer(t, o)
	defer testMQTTShutdownServer(s)

	addr := net.JoinHostPort(o.MQTT.Host, fmt.Sprintf("%d", o.MQTT.Port))
	c, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatalf("Error creating mqtt connection: %v", err)
	}

	pkLen := 2 + len(mqttProtoName) +
		1 + // proto level
		1 + // flags
		2 + // keepAlive
		2 + len("mqtt")

	w := newMQTTWriter(0)
	w.WriteByte(mqttPacketConnect)
	w.WriteVarInt(pkLen)
	w.WriteString(string(mqttProtoName))
	w.WriteByte(0x7)
	w.WriteByte(mqttConnFlagCleanSession)
	w.WriteUint16(0)
	w.WriteString("mqtt")
	c.Write(w.Bytes())

	buf, err := testMQTTRead(c)
	if err != nil {
		t.Fatalf("Error reading: %v", err)
	}
	r := &mqttReader{reader: c}
	r.reset(buf)
	testMQTTCheckConnAck(t, r, mqttConnAckRCUnacceptableProtocolVersion, false)
}

func TestMQTTConnKeepAlive(t *testing.T) {
	o := testMQTTDefaultOptions()
	s := testMQTTRunServer(t, o)
	defer testMQTTShutdownServer(s)

	mc, r := testMQTTConnect(t, &mqttConnInfo{cleanSess: true, keepAlive: 1}, o.MQTT.Host, o.MQTT.Port)
	defer mc.Close()
	testMQTTCheckConnAck(t, r, mqttConnAckRCConnectionAccepted, false)

	testMQTTPublish(t, mc, r, 0, false, false, "foo", 0, []byte("msg"))

	time.Sleep(2 * time.Second)
	testMQTTExpectDisconnect(t, mc)
}

func TestMQTTMalformedFixedHeaderFlagsCauseDisconnect(t *testing.T) {
	o := testMQTTDefaultOptions()
	s := testMQTTRunServer(t, o)
	defer testMQTTShutdownServer(s)

	for _, test := range []struct {
		name   string
		packet func(t *testing.T) (net.Conn, []byte)
	}{
		{
			name: "connect",
			packet: func(t *testing.T) (net.Conn, []byte) {
				addr := net.JoinHostPort(o.MQTT.Host, fmt.Sprintf("%d", o.MQTT.Port))
				c, err := net.Dial("tcp", addr)
				if err != nil {
					t.Fatalf("Error creating mqtt connection: %v", err)
				}
				proto := mqttCreateConnectProto(&mqttConnInfo{cleanSess: true})
				proto[0] |= 0x1
				return c, proto
			},
		},
		{
			name: "pingreq",
			packet: func(t *testing.T) (net.Conn, []byte) {
				mc, r := testMQTTConnect(t, &mqttConnInfo{cleanSess: true}, o.MQTT.Host, o.MQTT.Port)
				testMQTTCheckConnAck(t, r, mqttConnAckRCConnectionAccepted, false)
				return mc, []byte{mqttPacketPing | 0x1, 0}
			},
		},
		{
			name: "puback",
			packet: func(t *testing.T) (net.Conn, []byte) {
				mc, r := testMQTTConnect(t, &mqttConnInfo{cleanSess: true}, o.MQTT.Host, o.MQTT.Port)
				testMQTTCheckConnAck(t, r, mqttConnAckRCConnectionAccepted, false)
				return mc, []byte{mqttPacketPubAck | 0x1, 2, 0, 1}
			},
		},
		{
			name: "pubrec",
			packet: func(t *testing.T) (net.Conn, []byte) {
				mc, r := testMQTTConnect(t, &mqttConnInfo{cleanSess: true}, o.MQTT.Host, o.MQTT.Port)
				testMQTTCheckConnAck(t, r, mqttConnAckRCConnectionAccepted, false)
				return mc, []byte{mqttPacketPubRec | 0x1, 2, 0, 1}
			},
		},
		{
			name: "pubrel",
			packet: func(t *testing.T) (net.Conn, []byte) {
				mc, r := testMQTTConnect(t, &mqttConnInfo{cleanSess: true}, o.MQTT.Host, o.MQTT.Port)
				testMQTTCheckConnAck(t, r, mqttConnAckRCConnectionAccepted, false)
				return mc, []byte{mqttPacketPubRel | 0x1, 2, 0, 1}
			},
		},
		{
			name: "pubcomp",
			packet: func(t *testing.T) (net.Conn, []byte) {
				mc, r := testMQTTConnect(t, &mqttConnInfo{cleanSess: true}, o.MQTT.Host, o.MQTT.Port)
				testMQTTCheckConnAck(t, r, mqttConnAckRCConnectionAccepted, false)
				return mc, []byte{mqttPacketPubComp | 0x1, 2, 0, 1}
			},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			c, packet := test.packet(t)
			defer c.Close()

			if _, err := testMQTTWrite(c, packet); err != nil {
				t.Fatalf("Error writing malformed %s packet: %v", test.name, err)
			}
			testMQTTExpectDisconnect(t, c)
		})
	}
}

func TestMQTTMalformedRemainingLengthCausesDisconnect(t *testing.T) {
	o := testMQTTDefaultOptions()
	s := testMQTTRunServer(t, o)
	defer testMQTTShutdownServer(s)

	for _, test := range []struct {
		name   string
		packet func(t *testing.T) (net.Conn, []byte)
	}{
		{
			name: "pingreq",
			packet: func(t *testing.T) (net.Conn, []byte) {
				mc, r := testMQTTConnect(t, &mqttConnInfo{cleanSess: true}, o.MQTT.Host, o.MQTT.Port)
				testMQTTCheckConnAck(t, r, mqttConnAckRCConnectionAccepted, false)
				return mc, []byte{mqttPacketPing, 1, 0}
			},
		},
		{
			name: "puback",
			packet: func(t *testing.T) (net.Conn, []byte) {
				mc, r := testMQTTConnect(t, &mqttConnInfo{cleanSess: true}, o.MQTT.Host, o.MQTT.Port)
				testMQTTCheckConnAck(t, r, mqttConnAckRCConnectionAccepted, false)
				return mc, []byte{mqttPacketPubAck, 3, 0, 1, 0}
			},
		},
		{
			name: "pubrec",
			packet: func(t *testing.T) (net.Conn, []byte) {
				mc, r := testMQTTConnect(t, &mqttConnInfo{cleanSess: true}, o.MQTT.Host, o.MQTT.Port)
				testMQTTCheckConnAck(t, r, mqttConnAckRCConnectionAccepted, false)
				return mc, []byte{mqttPacketPubRec, 3, 0, 1, 0}
			},
		},
		{
			name: "pubrel",
			packet: func(t *testing.T) (net.Conn, []byte) {
				mc, r := testMQTTConnect(t, &mqttConnInfo{cleanSess: true}, o.MQTT.Host, o.MQTT.Port)
				testMQTTCheckConnAck(t, r, mqttConnAckRCConnectionAccepted, false)
				return mc, []byte{mqttPacketPubRel | 0x2, 3, 0, 1, 0}
			},
		},
		{
			name: "pubcomp",
			packet: func(t *testing.T) (net.Conn, []byte) {
				mc, r := testMQTTConnect(t, &mqttConnInfo{cleanSess: true}, o.MQTT.Host, o.MQTT.Port)
				testMQTTCheckConnAck(t, r, mqttConnAckRCConnectionAccepted, false)
				return mc, []byte{mqttPacketPubComp, 3, 0, 1, 0}
			},
		},
		{
			name: "puback incomplete",
			packet: func(t *testing.T) (net.Conn, []byte) {
				mc, r := testMQTTConnect(t, &mqttConnInfo{cleanSess: true}, o.MQTT.Host, o.MQTT.Port)
				testMQTTCheckConnAck(t, r, mqttConnAckRCConnectionAccepted, false)
				// Declare the maximum remaining length but only send part of the packet.
				return mc, []byte{mqttPacketPubAck, 0xff, 0xff, 0xff, 0x7f, 0, 1}
			},
		},
		{
			name: "publish",
			packet: func(t *testing.T) (net.Conn, []byte) {
				mc, r := testMQTTConnect(t, &mqttConnInfo{cleanSess: true}, o.MQTT.Host, o.MQTT.Port)
				testMQTTCheckConnAck(t, r, mqttConnAckRCConnectionAccepted, false)

				// Declare a remaining length that only covers the topic length field,
				// then write a much larger topic.
				w := newMQTTWriter(0)
				w.WriteByte(mqttPacketPub)
				w.WriteVarInt(2)
				w.WriteBytes(bytes.Repeat([]byte("a"), 64))
				return mc, w.Bytes()
			},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			c, packet := test.packet(t)
			defer c.Close()

			if _, err := testMQTTWrite(c, packet); err != nil {
				t.Fatalf("Error writing malformed %s packet: %v", test.name, err)
			}
			testMQTTExpectDisconnect(t, c)
		})
	}
}

func TestMQTTDontSetPinger(t *testing.T) {
	o := testMQTTDefaultOptions()
	o.PingInterval = 15 * time.Millisecond
	s := testMQTTRunServer(t, o)
	defer testMQTTShutdownServer(s)

	mc, r := testMQTTConnect(t, &mqttConnInfo{clientID: "mqtt", cleanSess: true}, o.MQTT.Host, o.MQTT.Port)
	defer mc.Close()
	testMQTTCheckConnAck(t, r, mqttConnAckRCConnectionAccepted, false)

	c := testMQTTGetClient(t, s, "mqtt")
	c.mu.Lock()
	timerSet := c.ping.tmr != nil
	c.mu.Unlock()
	if timerSet {
		t.Fatalf("Ping timer should not be set for MQTT clients")
	}

	// Wait a bit and expect nothing (and connection should still be valid)
	testMQTTExpectNothing(t, r)
	testMQTTPublish(t, mc, r, 0, false, false, "foo", 0, []byte("msg"))
}

func TestMQTTTopicAndSubjectConversion(t *testing.T) {
	for _, test := range []struct {
		name        string
		mqttTopic   string
		natsSubject string
		err         string
	}{
		{"/", "/", "/./", ""},
		{"//", "//", "/././", ""},
		{"///", "///", "/./././", ""},
		{"////", "////", "/././././", ""},
		{"foo", "foo", "foo", ""},
		{"/foo", "/foo", "/.foo", ""},
		{"//foo", "//foo", "/./.foo", ""},
		{"///foo", "///foo", "/././.foo", ""},
		{"///foo/", "///foo/", "/././.foo./", ""},
		{"///foo//", "///foo//", "/././.foo././", ""},
		{"///foo///", "///foo///", "/././.foo./././", ""},
		{"//.foo.//", "//.foo.//", "/././/foo//././", ""},
		{"foo/bar", "foo/bar", "foo.bar", ""},
		{"/foo/bar", "/foo/bar", "/.foo.bar", ""},
		{"/foo/bar/", "/foo/bar/", "/.foo.bar./", ""},
		{"foo/bar/baz", "foo/bar/baz", "foo.bar.baz", ""},
		{"/foo/bar/baz", "/foo/bar/baz", "/.foo.bar.baz", ""},
		{"/foo/bar/baz/", "/foo/bar/baz/", "/.foo.bar.baz./", ""},
		{"bar", "bar/", "bar./", ""},
		{"bar//", "bar//", "bar././", ""},
		{"bar///", "bar///", "bar./././", ""},
		{"foo//bar", "foo//bar", "foo./.bar", ""},
		{"foo///bar", "foo///bar", "foo././.bar", ""},
		{"foo////bar", "foo////bar", "foo./././.bar", ""},
		{".", ".", "//", ""},
		{"..", "..", "////", ""},
		{"...", "...", "//////", ""},
		{"./", "./", "//./", ""},
		{".//.", ".//.", "//././/", ""},
		{"././.", "././.", "//.//.//", ""},
		{"././/.", "././/.", "//.//././/", ""},
		{".foo", ".foo", "//foo", ""},
		{"foo.", "foo.", "foo//", ""},
		{".foo.", ".foo.", "//foo//", ""},
		{"foo../bar/", "foo../bar/", "foo////.bar./", ""},
		{"foo../bar/.", "foo../bar/.", "foo////.bar.//", ""},
		{"/foo/", "/foo/", "/.foo./", ""},
		{"./foo/.", "./foo/.", "//.foo.//", ""},
		{"foo.bar/baz", "foo.bar/baz", "foo//bar.baz", ""},
		// These should produce errors
		{"foo/+", "foo/+", "", "wildcards not allowed in publish"},
		{"foo/#", "foo/#", "", "wildcards not allowed in publish"},
		{"foo bar", "foo bar", "", "not supported"},
		{"foo\x7fbar", "foo\x7fbar", "", "not supported"},
	} {
		t.Run(test.name, func(t *testing.T) {
			res, err := mqttTopicToNATSPubSubject([]byte(test.mqttTopic))
			if test.err != _EMPTY_ {
				if err == nil || !strings.Contains(err.Error(), test.err) {
					t.Fatalf("Expected error %q, got %v", test.err, err)
				}
				return
			}
			toNATS := string(res)
			if toNATS != test.natsSubject {
				t.Fatalf("Expected subject %q got %q", test.natsSubject, toNATS)
			}

			res = natsSubjectToMQTTTopic(res)
			backToMQTT := string(res)
			if backToMQTT != test.mqttTopic {
				t.Fatalf("Expected topic %q got %q (NATS conversion was %q)", test.mqttTopic, backToMQTT, toNATS)
			}
		})
	}
}

func TestMQTTFilterConversion(t *testing.T) {
	// Similar to TopicConversion test except that wildcards are OK here.
	// So testing only those.
	for _, test := range []struct {
		name        string
		mqttTopic   string
		natsSubject string
	}{
		{"single level wildcard", "+", "*"},
		{"single level wildcard", "/+", "/.*"},
		{"single level wildcard", "+/", "*./"},
		{"single level wildcard", "/+/", "/.*./"},
		{"single level wildcard", "foo/+", "foo.*"},
		{"single level wildcard", "foo/+/", "foo.*./"},
		{"single level wildcard", "foo/+/bar", "foo.*.bar"},
		{"single level wildcard", "foo/+/+", "foo.*.*"},
		{"single level wildcard", "foo/+/+/", "foo.*.*./"},
		{"single level wildcard", "foo/+/+/bar", "foo.*.*.bar"},
		{"single level wildcard", "foo//+", "foo./.*"},
		{"single level wildcard", "foo//+/", "foo./.*./"},
		{"single level wildcard", "foo//+//", "foo./.*././"},
		{"single level wildcard", "foo//+//bar", "foo./.*./.bar"},
		{"single level wildcard", "foo///+///bar", "foo././.*././.bar"},
		{"single level wildcard", "foo.bar///+///baz", "foo//bar././.*././.baz"},

		{"multi level wildcard", "#", ">"},
		{"multi level wildcard", "/#", "/.>"},
		{"multi level wildcard", "/foo/#", "/.foo.>"},
		{"multi level wildcard", "foo/#", "foo.>"},
		{"multi level wildcard", "foo//#", "foo./.>"},
		{"multi level wildcard", "foo///#", "foo././.>"},
		{"multi level wildcard", "foo/bar/#", "foo.bar.>"},
		{"multi level wildcard", "foo/bar.baz/#", "foo.bar//baz.>"},

		// NATS wildcards in MQTT filters are generally
		// not escaped, and get interpreted as wildcards
		// when converted to NATS subjects
		{"* literal", "foo*/bar", "foo*.bar"},
		{"> literal", "foo>/bar", "foo>.bar"},
		{"* wildcard middle", "foo/*/bar", "foo.*.bar"},
		{"* wildcard end", "foo/*/bar/*", "foo.*.bar.*"},
		{"> wildcard", "foo/>", "foo.>"},
		{"wildcard mix", "foo/*/bar/>", "foo.*.bar.>"},
	} {
		t.Run(test.name, func(t *testing.T) {
			res, err := mqttFilterToNATSSubject([]byte(test.mqttTopic))
			if err != nil {
				t.Fatalf("Error: %v", err)
			}
			if string(res) != test.natsSubject {
				t.Fatalf("Expected subject %q got %q", test.natsSubject, res)
			}
		})
	}
}

func TestMQTTParseSub(t *testing.T) {
	for _, test := range []struct {
		name    string
		proto   []byte
		b       byte
		pl      int
		err     string
		wantErr bool
	}{
		{"reserved flag", nil, 3, 0, "wrong subscribe reserved flags", false},
		{"ensure packet loaded", []byte{1, 2}, mqttSubscribeFlags, 10, io.ErrUnexpectedEOF.Error(), false},
		{"error reading packet id", []byte{1}, mqttSubscribeFlags, 1, "reading packet identifier", false},
		{"packet id cannot be zero", []byte{0, 0}, mqttSubscribeFlags, 2, errMQTTPacketIdentifierIsZero.Error(), false},
		{"missing filters", []byte{0, 1}, mqttSubscribeFlags, 2, "subscribe protocol must contain at least 1 topic filter", false},
		{"error reading topic", []byte{0, 1, 0, 2, 'a'}, mqttSubscribeFlags, 5, "topic filter", false},
		{"empty topic", []byte{0, 1, 0, 0}, mqttSubscribeFlags, 4, errMQTTTopicFilterCannotBeEmpty.Error(), false},
		{"invalid utf8 topic", []byte{0, 1, 0, 1, 241}, mqttSubscribeFlags, 5, "invalid utf8 for topic filter", false},
		{"topic with null byte", []byte{0, 1, 0, 3, 'a', 0, 'b', 0}, mqttSubscribeFlags, 8, "", true},
		{"missing qos", []byte{0, 1, 0, 1, 'a'}, mqttSubscribeFlags, 5, "QoS", false},
		{"invalid qos", []byte{0, 1, 0, 1, 'a', 3}, mqttSubscribeFlags, 6, "subscribe QoS value must be 0, 1 or 2", false},
	} {
		t.Run(test.name, func(t *testing.T) {
			r := &mqttReader{}
			r.reset(test.proto)
			mqtt := &mqtt{r: r}
			c := &client{mqtt: mqtt}
			_, _, err := c.mqttParseSubsOrUnsubs(r, test.b, test.pl, true)
			if test.wantErr {
				if err == nil {
					t.Fatal("Expected an error, got none")
				}
				return
			}
			if err == nil || !strings.Contains(err.Error(), test.err) {
				t.Fatalf("Expected error %q, got %v", test.err, err)
			}
		})
	}
}

func testMQTTSub(t testing.TB, pi uint16, c net.Conn, r *mqttReader, filters []*mqttFilter, expected []byte) {
	t.Helper()
	w := newMQTTWriter(0)
	pkLen := 2 // for pi
	for i := 0; i < len(filters); i++ {
		f := filters[i]
		pkLen += 2 + len(f.filter) + 1
	}
	w.WriteByte(mqttPacketSub | mqttSubscribeFlags)
	w.WriteVarInt(pkLen)
	w.WriteUint16(pi)
	for i := 0; i < len(filters); i++ {
		f := filters[i]
		w.WriteBytes([]byte(f.filter))
		w.WriteByte(f.qos)
	}
	if _, err := testMQTTWrite(c, w.Bytes()); err != nil {
		t.Fatalf("Error writing SUBSCRIBE protocol: %v", err)
	}
	b, pl := testMQTTReadPacket(t, r)
	if pt := b & mqttPacketMask; pt != mqttPacketSubAck {
		t.Fatalf("Expected SUBACK packet %x, got %x", mqttPacketSubAck, pt)
	}
	rpi, err := r.readUint16("packet identifier")
	if err != nil || rpi != pi {
		t.Fatalf("Error with packet identifier expected=%v got: %v err=%v", pi, rpi, err)
	}
	for i, rem := 0, pl-2; rem > 0; rem-- {
		qos, err := r.readByte("filter qos")
		if err != nil {
			t.Fatal(err)
		}
		if expected != nil && qos != expected[i] {
			t.Fatalf("For topic filter %q expected qos of %v, got %v",
				filters[i].filter, expected[i], qos)
		}
		i++
	}
}

func TestMQTTSubAck(t *testing.T) {
	o := testMQTTDefaultOptions()
	o.Users = []*User{
		{
			Username: "user",
			Password: "pass",
			Permissions: &Permissions{
				Subscribe: &SubjectPermission{Allow: []string{"foo", "bar", "baz", "foo.>"}},
			},
		},
	}
	s := testMQTTRunServer(t, o)
	defer testMQTTShutdownServer(s)

	mc, r := testMQTTConnect(t, &mqttConnInfo{cleanSess: true, user: "user", pass: "pass"}, o.MQTT.Host, o.MQTT.Port)
	defer mc.Close()
	testMQTTCheckConnAck(t, r, mqttConnAckRCConnectionAccepted, false)

	subs := []*mqttFilter{
		{filter: "foo", qos: 0},
		{filter: "bar", qos: 1},
		{filter: "baz", qos: 2},
		{filter: "foo/#/bar", qos: 0}, // Invalid sub, so we should receive a result of mqttSubAckFailure
	}
	expected := []byte{
		0,
		1,
		2,
		mqttSubAckFailure,
	}
	testMQTTSub(t, 1, mc, r, subs, expected)
}

func TestMQTTQoS2SubDowngrade(t *testing.T) {
	o := testMQTTDefaultOptions()
	o.MQTT.downgradeQoS2Sub = true
	s := testMQTTRunServer(t, o)
	defer testMQTTShutdownServer(s)

	mc, r := testMQTTConnect(t, &mqttConnInfo{cleanSess: true}, o.MQTT.Host, o.MQTT.Port)
	defer mc.Close()
	testMQTTCheckConnAck(t, r, mqttConnAckRCConnectionAccepted, false)

	subs := []*mqttFilter{
		{filter: "bar", qos: 1},
		{filter: "baz", qos: 2},
	}
	expected := []byte{
		1,
		1,
	}
	testMQTTSub(t, 1, mc, r, subs, expected)
}

func testMQTTFlush(t testing.TB, c net.Conn, bw *bufio.Writer, r *mqttReader) {
	t.Helper()
	w := newMQTTWriter(0)
	w.WriteByte(mqttPacketPing)
	w.WriteByte(0)
	if bw != nil {
		bw.Write(w.Bytes())
		bw.Flush()
	} else {
		c.Write(w.Bytes())
	}
	ab, l := testMQTTReadPacket(t, r)
	if pt := ab & mqttPacketMask; pt != mqttPacketPingResp {
		t.Fatalf("Expected ping response got %x", pt)
	}
	if l != 0 {
		t.Fatalf("Expected PINGRESP length to be 0, got %v", l)
	}
}

func testMQTTExpectNothing(t testing.TB, r *mqttReader) {
	t.Helper()
	// First, check that we don't have buffered data.
	if r.hasMore() {
		t.Fatalf("Expected nothing, got %v", r.buf[r.pos:])
	}
	// Then, try to read from the reader with some timeout.
	var buf [128]byte
	r.reader.SetReadDeadline(time.Now().Add(100 * time.Millisecond))
	if n, err := r.reader.Read(buf[:]); err == nil {
		t.Fatalf("Expected nothing, got %v", buf[:n])
	}
	r.reader.SetReadDeadline(time.Time{})
}

func testMQTTCheckPubMsg(t testing.TB, c net.Conn, r *mqttReader, topic string, expectedFlags byte, payload []byte) uint16 {
	t.Helper()
	pi := testMQTTCheckPubMsgNoAck(t, c, r, topic, expectedFlags, payload)
	if pi == 0 {
		return 0
	}
	qos := mqttGetQoS(expectedFlags)
	switch qos {
	case 1:
		testMQTTSendPIPacket(mqttPacketPubAck, t, c, pi)
	case 2:
		testMQTTSendPIPacket(mqttPacketPubRec, t, c, pi)
	}
	return pi
}

func testMQTTCheckPubMsgNoAck(t testing.TB, c net.Conn, r *mqttReader, topic string, expectedFlags byte, payload []byte) uint16 {
	t.Helper()
	pflags, pi := testMQTTGetPubMsg(t, c, r, topic, payload)
	if pflags != expectedFlags {
		t.Fatalf("Expected flags to be %x, got %x", expectedFlags, pflags)
	}
	return pi
}

func testMQTTGetPubMsg(t testing.TB, c net.Conn, r *mqttReader, topic string, payload []byte) (byte, uint16) {
	t.Helper()
	flags, pi, _, _ := testMQTTGetPubMsgEx(t, c, r, topic, payload)
	return flags, pi
}

func testMQTTGetPubMsgEx(t testing.TB, _ net.Conn, r *mqttReader, topic string, payload []byte) (byte, uint16, string, []byte) {
	t.Helper()
	b, pl := testMQTTReadPacket(t, r)
	if pt := b & mqttPacketMask; pt != mqttPacketPub {
		t.Fatalf("Expected PUBLISH packet %x, got %x", mqttPacketPub, pt)
	}
	return testMQTTGetPubMsgExEx(t, nil, r, b, pl, topic, payload)
}

func testMQTTGetPubMsgExEx(t testing.TB, _ net.Conn, r *mqttReader, b byte, pl int, topic string, payload []byte) (byte, uint16, string, []byte) {
	t.Helper()
	pflags := b & mqttPacketFlagMask
	qos := (pflags & mqttPubFlagQoS) >> 1
	start := r.pos
	ptopic, err := r.readString("topic name")
	if err != nil {
		t.Fatal(err)
	}
	if topic != _EMPTY_ && ptopic != topic {
		t.Fatalf("Expected topic %q, got %q", topic, ptopic)
	}
	var pi uint16
	if qos > 0 {
		pi, err = r.readUint16("packet identifier")
		if err != nil {
			t.Fatal(err)
		}
	}
	msgLen := pl - (r.pos - start)
	if r.pos+msgLen > len(r.buf) {
		t.Fatalf("computed message length goes beyond buffer: ml=%v pos=%v lenBuf=%v",
			msgLen, r.pos, len(r.buf))
	}
	ppayload := r.buf[r.pos : r.pos+msgLen]
	if payload != nil && !bytes.Equal(payload, ppayload) {
		t.Fatalf("Expected payload %q, got %q", payload, ppayload)
	}
	r.pos += msgLen
	return pflags, pi, ptopic, ppayload
}

func testMQTTReadPubPacket(t testing.TB, r *mqttReader) (flags byte, pi uint16, topic string, payload []byte) {
	t.Helper()
	b, pl := testMQTTReadPacket(t, r)
	if pt := b & mqttPacketMask; pt != mqttPacketPub {
		t.Fatalf("Expected PUBLISH packet %x, got %x", mqttPacketPub, pt)
	}
	flags = b & mqttPacketFlagMask
	start := r.pos
	topic, err := r.readString("topic name")
	if err != nil {
		t.Fatal(err)
	}
	qos := (flags & mqttPubFlagQoS) >> 1
	if qos > 0 {
		pi, err = r.readUint16("packet identifier")
		if err != nil {
			t.Fatal(err)
		}
	}
	msgLen := pl - (r.pos - start)
	if r.pos+msgLen > len(r.buf) {
		t.Fatalf("computed message length goes beyond buffer: ml=%v pos=%v lenBuf=%v",
			msgLen, r.pos, len(r.buf))
	}
	payload = r.buf[r.pos : r.pos+msgLen]
	r.pos += msgLen
	return flags, pi, topic, payload
}

func testMQTTSendPIPacket(packetType byte, t testing.TB, c net.Conn, pi uint16) {
	t.Helper()
	w := newMQTTWriter(0)
	w.WriteByte(packetType)
	w.WriteVarInt(2)
	w.WriteUint16(pi)
	if _, err := testMQTTWrite(c, w.Bytes()); err != nil {
		t.Fatalf("Error writing packet type %v: %v", packetType, err)
	}
}

func testMQTTWritePublishPacket(t testing.TB, w *mqttWriter, qos byte, dup, retain bool, topic string, pi uint16, payload []byte) {
	t.Helper()
	w.WritePublishHeader(pi, qos, dup, retain, []byte(topic), len(payload))
	if _, err := w.Write(payload); err != nil {
		t.Fatalf("Error writing PUBLISH proto: %v", err)
	}
}

func testMQTTSendPublishPacket(t testing.TB, c net.Conn, qos byte, dup, retain bool, topic string, pi uint16, payload []byte) {
	t.Helper()
	c.SetWriteDeadline(time.Now().Add(testMQTTTimeout))
	_, header := mqttMakePublishHeader(pi, qos, dup, retain, []byte(topic), len(payload))
	if _, err := c.Write(header); err != nil {
		t.Fatalf("Error writing PUBLISH header: %v", err)
	}
	if _, err := c.Write(payload); err != nil {
		t.Fatalf("Error writing PUBLISH payload: %v", err)
	}
	c.SetWriteDeadline(time.Time{})
}

func testMQTTPublish(t testing.TB, c net.Conn, r *mqttReader, qos byte, dup, retain bool, topic string, pi uint16, payload []byte) {
	t.Helper()
	testMQTTSendPublishPacket(t, c, qos, dup, retain, topic, pi, payload)
	switch qos {
	case 1:
		b, _ := testMQTTReadPacket(t, r)
		if pt := b & mqttPacketMask; pt != mqttPacketPubAck {
			t.Fatalf("Expected PUBACK packet %x, got %x", mqttPacketPubAck, pt)
		}
		rpi, err := r.readUint16("packet identifier")
		if err != nil || rpi != pi {
			t.Fatalf("Error with packet identifier expected=%v got: %v err=%v", pi, rpi, err)
		}

	case 2:
		b, _ := testMQTTReadPacket(t, r)
		if pt := b & mqttPacketMask; pt != mqttPacketPubRec {
			t.Fatalf("Expected PUBREC packet %x, got %x", mqttPacketPubRec, pt)
		}
		rpi, err := r.readUint16("packet identifier")
		if err != nil || rpi != pi {
			t.Fatalf("Error with packet identifier expected=%v got: %v err=%v", pi, rpi, err)
		}

		testMQTTSendPIPacket(mqttPacketPubRel|0x2, t, c, pi)

		b, _ = testMQTTReadPacket(t, r)
		if pt := b & mqttPacketMask; pt != mqttPacketPubComp {
			t.Fatalf("Expected PUBCOMP packet %x, got %x", mqttPacketPubComp, pt)
		}
		rpi, err = r.readUint16("packet identifier")
		if err != nil || rpi != pi {
			t.Fatalf("Error with packet identifier expected=%v got: %v err=%v", pi, rpi, err)
		}

		testMQTTFlush(t, c, nil, r)
	}
}

func TestMQTTParsePub(t *testing.T) {
	for _, test := range []struct {
		name    string
		flags   byte
		proto   []byte
		pl      int
		err     string
		wantErr bool
	}{
		{"qos not supported", (3 << 1), nil, 0, "QoS=3 is invalid in MQTT", false},
		{"packet in buffer error", 0, nil, 10, io.ErrUnexpectedEOF.Error(), false},
		{"error on topic", 0, []byte{0, 3, 'f', 'o'}, 4, "topic", false},
		{"empty topic", 0, []byte{0, 0}, 2, errMQTTTopicIsEmpty.Error(), false},
		{"wildcards topic", 0, []byte{0, 1, '#'}, 3, "wildcards not allowed", false},
		{"invalid utf8 topic", 0, []byte{0, 1, 241}, 3, "", true},
		{"topic with null byte", 0, []byte{0, 3, 'f', 0, 'o'}, 5, "", true},
		{"error on packet identifier", mqttPubQos1, []byte{0, 3, 'f', 'o', 'o'}, 5, "packet identifier", false},
		{"invalid packet identifier", mqttPubQos1, []byte{0, 3, 'f', 'o', 'o', 0, 0}, 7, errMQTTPacketIdentifierIsZero.Error(), false},
	} {
		t.Run(test.name, func(t *testing.T) {
			r := &mqttReader{}
			r.reset(test.proto)
			mqtt := &mqtt{r: r}
			c := &client{mqtt: mqtt}
			pp := &mqttPublish{flags: test.flags}
			err := c.mqttParsePub(r, test.pl, pp, false)
			if test.wantErr {
				if err == nil {
					t.Fatal("Expected an error, got none")
				}
				return
			}
			if err == nil || !strings.Contains(err.Error(), test.err) {
				t.Fatalf("Expected error %q, got %v", test.err, err)
			}
		})
	}
}

func TestMQTTNatsHeaderFlags(t *testing.T) {
	// Round-trip every MQTT flags nibble through the header byte.
	for flags := byte(0); flags <= mqttPubFlags; flags++ {
		enc := mqttNatsHeaderEncodeFlags(flags)
		if enc < '0' || (enc > '9' && enc < 'a') || enc > 'f' {
			t.Fatalf("flags 0x%x encoded to non-hex byte %q", flags, enc)
		}
		// Decode reads value[1]; value[0] is the (irrelevant here) QoS char.
		if got := mqttNatsHeaderDecodeFlags([]byte{'0', enc}); got != flags {
			t.Fatalf("flags 0x%x: round-trip got 0x%x", flags, got)
		}
	}

	// Bits above the nibble (e.g. a stray high bit) are not encoded.
	if enc := mqttNatsHeaderEncodeFlags(0xf0 | mqttPubFlagRetain); enc != '1' {
		t.Fatalf("high bits leaked into flags byte: %q", enc)
	}

	// Named examples, including the "25" from the header contract comment.
	for _, test := range []struct {
		flags byte
		enc   byte
	}{
		{0, '0'},
		{mqttPubFlagRetain, '1'},
		{mqttPubQoS2 | mqttPubFlagRetain, '5'},
		{mqttPubFlags, 'f'},
	} {
		if enc := mqttNatsHeaderEncodeFlags(test.flags); enc != test.enc {
			t.Fatalf("flags 0x%x encoded to %q, want %q", test.flags, enc, test.enc)
		}
	}

	// Cross-version contract: a value with no flags byte (older sender) reads
	// as no flags set, never a panic.
	for _, value := range [][]byte{nil, {}, {'2'}} {
		if got := mqttNatsHeaderDecodeFlags(value); got != 0 {
			t.Fatalf("value %q: want no flags, got 0x%x", value, got)
		}
	}
}

func TestMQTTParsePIMsg(t *testing.T) {
	for _, test := range []struct {
		name  string
		proto []byte
		err   string
	}{
		{"packet in buffer error", nil, io.ErrUnexpectedEOF.Error()},
		{"error reading packet identifier", []byte{0}, "packet identifier"},
		{"invalid packet identifier", []byte{0, 0}, errMQTTPacketIdentifierIsZero.Error()},
	} {
		t.Run(test.name, func(t *testing.T) {
			r := &mqttReader{}
			r.reset(test.proto)
			if _, err := mqttParsePIPacket(r); err == nil || !strings.Contains(err.Error(), test.err) {
				t.Fatalf("Expected error %q, got %v", test.err, err)
			}
		})
	}
}

func TestMQTTPublish(t *testing.T) {
	o := testMQTTDefaultOptions()
	s := testMQTTRunServer(t, o)
	defer testMQTTShutdownServer(s)

	nc := natsConnect(t, s.ClientURL())
	defer nc.Close()

	mcp, mpr := testMQTTConnect(t, &mqttConnInfo{cleanSess: true}, o.MQTT.Host, o.MQTT.Port)
	defer mcp.Close()
	testMQTTCheckConnAck(t, mpr, mqttConnAckRCConnectionAccepted, false)

	testMQTTPublish(t, mcp, mpr, 0, false, false, "foo", 0, []byte("msg"))
	testMQTTPublish(t, mcp, mpr, 1, false, false, "foo", 1, []byte("msg"))
	testMQTTPublish(t, mcp, mpr, 2, false, false, "foo", 2, []byte("msg"))
}

func TestMQTTQoS2PubReject(t *testing.T) {
	o := testMQTTDefaultOptions()
	o.MQTT.rejectQoS2Pub = true
	s := testMQTTRunServer(t, o)
	defer testMQTTShutdownServer(s)

	nc := natsConnect(t, s.ClientURL())
	defer nc.Close()

	mcp, mpr := testMQTTConnect(t, &mqttConnInfo{cleanSess: true}, o.MQTT.Host, o.MQTT.Port)
	defer mcp.Close()
	testMQTTCheckConnAck(t, mpr, mqttConnAckRCConnectionAccepted, false)

	testMQTTPublish(t, mcp, mpr, 1, false, false, "foo", 1, []byte("msg"))

	testMQTTSendPublishPacket(t, mcp, 2, false, false, "foo", 2, []byte("msg"))
	testMQTTExpectDisconnect(t, mcp)
}

func TestMQTTSub(t *testing.T) {
	o := testMQTTDefaultOptions()
	s := testMQTTRunServer(t, o)
	defer testMQTTShutdownServer(s)

	nc := natsConnect(t, s.ClientURL())
	defer nc.Close()

	mcp, mpr := testMQTTConnect(t, &mqttConnInfo{cleanSess: true}, o.MQTT.Host, o.MQTT.Port)
	defer mcp.Close()
	testMQTTCheckConnAck(t, mpr, mqttConnAckRCConnectionAccepted, false)

	for _, test := range []struct {
		name           string
		mqttSubTopic   string
		natsPubSubject string
		mqttPubTopic   string
		ok             bool
	}{
		{"1 level match", "foo", "foo", "foo", true},
		{"1 level no match", "foo", "bar", "bar", false},
		{"2 levels match", "foo/bar", "foo.bar", "foo/bar", true},
		{"2 levels no match", "foo/bar", "foo.baz", "foo/baz", false},
		{"3 levels match", "/foo/bar", "/.foo.bar", "/foo/bar", true},
		{"3 levels no match", "/foo/bar", "/.foo.baz", "/foo/baz", false},

		{"single level wc", "foo/+", "foo.bar.baz", "foo/bar/baz", false},
		{"single level wc", "foo/+", "foo.bar./", "foo/bar/", false},
		{"single level wc", "foo/+", "foo.bar", "foo/bar", true},
		{"single level wc", "foo/+", "foo./", "foo/", true},
		{"single level wc", "foo/+", "foo", "foo", false},
		{"single level wc", "foo/+", "/.foo", "/foo", false},

		{"multiple level wc", "foo/#", "foo.bar.baz./", "foo/bar/baz/", true},
		{"multiple level wc", "foo/#", "foo.bar.baz", "foo/bar/baz", true},
		{"multiple level wc", "foo/#", "foo.bar./", "foo/bar/", true},
		{"multiple level wc", "foo/#", "foo.bar", "foo/bar", true},
		{"multiple level wc", "foo/#", "foo./", "foo/", true},
		{"multiple level wc", "foo/#", "foo", "foo", true},
		{"multiple level wc", "foo/#", "/.foo", "/foo", false},
	} {
		t.Run(test.name, func(t *testing.T) {
			mc, r := testMQTTConnect(t, &mqttConnInfo{cleanSess: true}, o.MQTT.Host, o.MQTT.Port)
			defer mc.Close()
			testMQTTCheckConnAck(t, r, mqttConnAckRCConnectionAccepted, false)

			testMQTTSub(t, 1, mc, r, []*mqttFilter{{filter: test.mqttSubTopic, qos: 0}}, []byte{0})
			testMQTTFlush(t, mc, nil, r)

			natsPub(t, nc, test.natsPubSubject, []byte("msg"))
			if test.ok {
				testMQTTCheckPubMsg(t, mc, r, test.mqttPubTopic, 0, []byte("msg"))
			} else {
				testMQTTExpectNothing(t, r)
			}

			testMQTTPublish(t, mcp, mpr, 0, false, false, test.mqttPubTopic, 0, []byte("msg"))
			if test.ok {
				testMQTTCheckPubMsg(t, mc, r, test.mqttPubTopic, 0, []byte("msg"))
			} else {
				testMQTTExpectNothing(t, r)
			}
		})
	}
}

func TestMQTTSubQoS2(t *testing.T) {
	o := testMQTTDefaultOptions()
	s := testMQTTRunServer(t, o)
	defer testMQTTShutdownServer(s)

	nc := natsConnect(t, s.ClientURL())
	defer nc.Close()

	mcp, mpr := testMQTTConnect(t, &mqttConnInfo{cleanSess: true}, o.MQTT.Host, o.MQTT.Port)
	defer mcp.Close()
	testMQTTCheckConnAck(t, mpr, mqttConnAckRCConnectionAccepted, false)

	mc, r := testMQTTConnect(t, &mqttConnInfo{cleanSess: true}, o.MQTT.Host, o.MQTT.Port)
	defer mc.Close()
	testMQTTCheckConnAck(t, r, mqttConnAckRCConnectionAccepted, false)

	topic := "foo/bar/baz"
	mqttTopic0 := "foo/#"
	mqttTopic1 := "foo/bar/#"
	mqttTopic2 := topic
	testMQTTSub(t, 1, mc, r, []*mqttFilter{{filter: mqttTopic0, qos: 0}}, []byte{0})
	testMQTTSub(t, 1, mc, r, []*mqttFilter{{filter: mqttTopic1, qos: 1}}, []byte{1})
	testMQTTSub(t, 1, mc, r, []*mqttFilter{{filter: mqttTopic2, qos: 2}}, []byte{2})
	testMQTTFlush(t, mc, nil, r)

	for pubQoS, expectedCounts := range map[byte]map[byte]int{
		0: {0: 3},
		1: {0: 1, 1: 2},
		2: {0: 1, 1: 1, 2: 1},
	} {
		t.Run(fmt.Sprintf("pubQoS %v", pubQoS), func(t *testing.T) {
			pubPI := uint16(456)

			testMQTTPublish(t, mcp, mpr, pubQoS, false, false, topic, pubPI, []byte("msg"))

			qosCounts := map[byte]int{}
			delivered := map[uint16]byte{}

			// We have 3 subscriptions, each should receive the message, with the
			// QoS that maybe "trimmed" to that of the subscription.
			for i := 0; i < 3; i++ {
				flags, pi := testMQTTGetPubMsg(t, mc, r, topic, []byte("msg"))
				delivered[pi] = flags
				qosCounts[mqttGetQoS(flags)]++
			}

			for pi, flags := range delivered {
				switch mqttGetQoS(flags) {
				case 1:
					testMQTTSendPIPacket(mqttPacketPubAck, t, mc, pi)

				case 2:
					testMQTTSendPIPacket(mqttPacketPubRec, t, mc, pi)
					testMQTTReadPIPacket(mqttPacketPubRel, t, r, pi)
					testMQTTSendPIPacket(mqttPacketPubComp, t, mc, pi)
				}
			}

			if !reflect.DeepEqual(qosCounts, expectedCounts) {
				t.Fatalf("Expected QoS %#v, got %#v", expectedCounts, qosCounts)
			}
		})
	}
}

func TestMQTTSubQoS2Restart(t *testing.T) {
	o := testMQTTDefaultOptions()
	s := testMQTTRunServer(t, o)
	defer testMQTTShutdownServer(s)

	pubTopic := "foo/bar"

	publish := func(msg string) {
		t.Helper()
		mcp, mpr := testMQTTConnect(t, &mqttConnInfo{cleanSess: true}, o.MQTT.Host, o.MQTT.Port)
		defer mcp.Close()
		testMQTTCheckConnAck(t, mpr, mqttConnAckRCConnectionAccepted, false)
		testMQTTPublish(t, mcp, mpr, 2, false, false, pubTopic, 1, []byte(msg))
		testMQTTFlush(t, mcp, nil, mpr)
	}

	createSub := func(present bool) (net.Conn, *mqttReader) {
		t.Helper()
		c, r := testMQTTConnect(t, &mqttConnInfo{clientID: "sub", cleanSess: false}, o.MQTT.Host, o.MQTT.Port)
		testMQTTCheckConnAck(t, r, mqttConnAckRCConnectionAccepted, present)
		if !present {
			testMQTTSub(t, 1, c, r, []*mqttFilter{{filter: "foo/bar", qos: 2}}, []byte{2})
			testMQTTSub(t, 2, c, r, []*mqttFilter{{filter: "foo/+", qos: 2}}, []byte{2})
			testMQTTFlush(t, c, nil, r)
		}
		return c, r
	}

	c, r := createSub(false)
	defer c.Close()

	nc, js := jsClientConnect(t, s)
	defer nc.Close()
	checkSub := func(count int) {
		t.Helper()
		checkFor(t, time.Second, 15*time.Millisecond, func() error {
			for c := range js.Consumers(mqttOutStreamName) {
				acc := s.GlobalAccount()
				res := acc.sl.Match(c.Config.DeliverSubject)
				if n := len(res.psubs); n != count {
					return fmt.Errorf("Expected %v subscription on %q, got %v",
						count, c.Config.DeliverSubject, n)
				} else {
					// We got the JS consumer and verified that it has the
					// expected number of subscription, so we are done.
					return nil
				}
			}
			// No JS consumer was found.
			return fmt.Errorf("Did not find a JS consumer for %q", mqttOutStreamName)
		})
	}
	checkSub(1)

	publish("msg1")

	consume := func(msg string) {
		t.Helper()
		var pis []uint16
		for range 2 {
			b, pl := testMQTTReadPacket(t, r)
			if pt := b & mqttPacketMask; pt != mqttPacketPub {
				t.Fatalf("Expected PUBLISH packet %x, got %x", mqttPacketPub, pt)
			}
			flags, pi, _, _ := testMQTTGetPubMsgExEx(t, c, r, b, pl, pubTopic, []byte(msg))
			require_Equal(t, flags, byte(mqttPubQoS2))
			pis = append(pis, pi)
		}
		for _, pi := range pis {
			testMQTTSendPIPacket(mqttPacketPubRec, t, c, pi)
			testMQTTReadPIPacket(mqttPacketPubRel, t, r, pi)
			testMQTTSendPIPacket(mqttPacketPubComp, t, c, pi)
		}
		testMQTTExpectNothing(t, r)
	}

	consume("msg1")
	testMQTTDisconnect(t, c, nil)
	c.Close()
	checkSub(0)

	publish("msg2")

	c, r = createSub(true)
	defer c.Close()
	checkSub(1)
	consume("msg2")

	publish("msg3")
	consume("msg3")
}

func TestMQTTSubQoS1(t *testing.T) {
	o := testMQTTDefaultOptions()
	s := testMQTTRunServer(t, o)
	defer testMQTTShutdownServer(s)

	nc := natsConnect(t, s.ClientURL())
	defer nc.Close()

	mcp, mpr := testMQTTConnect(t, &mqttConnInfo{cleanSess: true}, o.MQTT.Host, o.MQTT.Port)
	defer mcp.Close()
	testMQTTCheckConnAck(t, mpr, mqttConnAckRCConnectionAccepted, false)

	mc, r := testMQTTConnect(t, &mqttConnInfo{cleanSess: true}, o.MQTT.Host, o.MQTT.Port)
	defer mc.Close()
	testMQTTCheckConnAck(t, r, mqttConnAckRCConnectionAccepted, false)

	mqttTopic := "foo/bar"

	// Subscribe with QoS 1
	testMQTTSub(t, 1, mc, r, []*mqttFilter{{filter: "foo/#", qos: 1}}, []byte{1})
	testMQTTSub(t, 1, mc, r, []*mqttFilter{{filter: mqttTopic, qos: 1}}, []byte{1})
	testMQTTFlush(t, mc, nil, r)

	// Publish from NATS, which means QoS 0
	natsPub(t, nc, "foo.bar", []byte("NATS"))
	// Will receive as QoS 0
	testMQTTCheckPubMsg(t, mc, r, mqttTopic, 0, []byte("NATS"))
	testMQTTCheckPubMsg(t, mc, r, mqttTopic, 0, []byte("NATS"))

	// Publish from MQTT with QoS 0
	testMQTTPublish(t, mcp, mpr, 0, false, false, mqttTopic, 0, []byte("msg"))
	// Will receive as QoS 0
	testMQTTCheckPubMsg(t, mc, r, mqttTopic, 0, []byte("msg"))
	testMQTTCheckPubMsg(t, mc, r, mqttTopic, 0, []byte("msg"))

	// Publish from MQTT with QoS 1
	testMQTTPublish(t, mcp, mpr, 1, false, false, mqttTopic, 1, []byte("msg"))
	pflags1, pi1 := testMQTTGetPubMsg(t, mc, r, mqttTopic, []byte("msg"))
	if pflags1 != 0x2 {
		t.Fatalf("Expected flags to be 0x2, got %v", pflags1)
	}
	pflags2, pi2 := testMQTTGetPubMsg(t, mc, r, mqttTopic, []byte("msg"))
	if pflags2 != 0x2 {
		t.Fatalf("Expected flags to be 0x2, got %v", pflags2)
	}
	if pi1 == pi2 {
		t.Fatalf("packet identifier for message 1: %v should be different from message 2", pi1)
	}
	testMQTTSendPIPacket(mqttPacketPubAck, t, mc, pi1)
	testMQTTSendPIPacket(mqttPacketPubAck, t, mc, pi2)
}

func getSubQoS(sub *subscription) int {
	if sub.mqtt != nil {
		return int(sub.mqtt.qos)
	}
	return -1
}

func TestMQTTSubDups(t *testing.T) {
	o := testMQTTDefaultOptions()
	s := testMQTTRunServer(t, o)
	defer testMQTTShutdownServer(s)

	mcp, mpr := testMQTTConnect(t, &mqttConnInfo{cleanSess: true}, o.MQTT.Host, o.MQTT.Port)
	defer mcp.Close()
	testMQTTCheckConnAck(t, mpr, mqttConnAckRCConnectionAccepted, false)

	mc, r := testMQTTConnect(t, &mqttConnInfo{clientID: "sub", user: "sub", cleanSess: true}, o.MQTT.Host, o.MQTT.Port)
	defer mc.Close()
	testMQTTCheckConnAck(t, r, mqttConnAckRCConnectionAccepted, false)

	// Test with single SUBSCRIBE protocol but multiple filters
	filters := []*mqttFilter{
		{filter: "foo", qos: 1},
		{filter: "foo", qos: 0},
	}
	testMQTTSub(t, 1, mc, r, filters, []byte{1, 0})
	testMQTTFlush(t, mc, nil, r)

	// And also with separate SUBSCRIBE protocols
	testMQTTSub(t, 1, mc, r, []*mqttFilter{{filter: "bar", qos: 0}}, []byte{0})
	// Ask for QoS 1 but server will downgrade to 1
	testMQTTSub(t, 1, mc, r, []*mqttFilter{{filter: "bar", qos: 1}}, []byte{1})
	testMQTTFlush(t, mc, nil, r)

	// Publish and test msg received only once
	testMQTTPublish(t, mcp, r, 0, false, false, "foo", 0, []byte("msg"))
	testMQTTCheckPubMsg(t, mc, r, "foo", 0, []byte("msg"))
	testMQTTExpectNothing(t, r)

	testMQTTPublish(t, mcp, r, 0, false, false, "bar", 0, []byte("msg"))
	testMQTTCheckPubMsg(t, mc, r, "bar", 0, []byte("msg"))
	testMQTTExpectNothing(t, r)

	// Check that the QoS for subscriptions have been updated to the latest received filter
	var err error
	subc := testMQTTGetClient(t, s, "sub")
	subc.mu.Lock()
	if subc.opts.Username != "sub" {
		err = fmt.Errorf("wrong user name")
	}
	if err == nil {
		if sub := subc.subs["foo"]; sub == nil || getSubQoS(sub) != 0 {
			err = fmt.Errorf("subscription foo QoS should be 0, got %v", getSubQoS(sub))
		}
	}
	if err == nil {
		if sub := subc.subs["bar"]; sub == nil || getSubQoS(sub) != 1 {
			err = fmt.Errorf("subscription bar QoS should be 1, got %v", getSubQoS(sub))
		}
	}
	subc.mu.Unlock()
	if err != nil {
		t.Fatal(err)
	}

	// Now subscribe on "foo/#" which means that a PUBLISH on "foo" will be received
	// by this subscription and also the one on "foo".
	testMQTTSub(t, 1, mc, r, []*mqttFilter{{filter: "foo/#", qos: 1}}, []byte{1})
	testMQTTFlush(t, mc, nil, r)

	// Publish and test msg received twice
	testMQTTPublish(t, mcp, r, 0, false, false, "foo", 0, []byte("msg"))
	testMQTTCheckPubMsg(t, mc, r, "foo", 0, []byte("msg"))
	testMQTTCheckPubMsg(t, mc, r, "foo", 0, []byte("msg"))

	checkWCSub := func(expectedQoS int) {
		t.Helper()

		subc.mu.Lock()
		defer subc.mu.Unlock()

		// When invoked with expectedQoS==1, we have the following subs:
		// foo (QoS-0), bar (QoS-1), foo.> (QoS-1)
		// which means (since QoS-1 have a JS consumer + sub for delivery
		// and foo.> causes a "foo fwc") that we should have the following
		// number of NATS subs: foo (1), bar (2), foo.> (2) and "foo fwc" (2),
		// so total=7.
		// When invoked with expectedQoS==0, it means that we have replaced
		// foo/# QoS-1 to QoS-0, so we should have 2 less NATS subs,
		// so total=5
		expected := 7
		if expectedQoS == 0 {
			expected = 5
		}
		if lenmap := len(subc.subs); lenmap != expected {
			t.Fatalf("Subs map should have %v entries, got %v", expected, lenmap)
		}
		if sub, ok := subc.subs["foo.>"]; !ok {
			t.Fatal("Expected sub foo.> to be present but was not")
		} else if getSubQoS(sub) != expectedQoS {
			t.Fatalf("Expected sub foo.> QoS to be %v, got %v", expectedQoS, getSubQoS(sub))
		}
		if sub, ok := subc.subs["foo fwc"]; !ok {
			t.Fatal("Expected sub foo fwc to be present but was not")
		} else if getSubQoS(sub) != expectedQoS {
			t.Fatalf("Expected sub foo fwc QoS to be %v, got %v", expectedQoS, getSubQoS(sub))
		}
		// Make sure existing sub on "foo" qos was not changed.
		if sub, ok := subc.subs["foo"]; !ok {
			t.Fatal("Expected sub foo to be present but was not")
		} else if getSubQoS(sub) != 0 {
			t.Fatalf("Expected sub foo QoS to be 0, got %v", getSubQoS(sub))
		}
	}
	checkWCSub(1)

	// Sub again on same subject with lower QoS
	testMQTTSub(t, 1, mc, r, []*mqttFilter{{filter: "foo/#", qos: 0}}, []byte{0})
	testMQTTFlush(t, mc, nil, r)

	// Publish and test msg received twice
	testMQTTPublish(t, mcp, r, 0, false, false, "foo", 0, []byte("msg"))
	testMQTTCheckPubMsg(t, mc, r, "foo", 0, []byte("msg"))
	testMQTTCheckPubMsg(t, mc, r, "foo", 0, []byte("msg"))
	checkWCSub(0)
}

func TestMQTTSubWithSpaces(t *testing.T) {
	o := testMQTTDefaultOptions()
	s := testMQTTRunServer(t, o)
	defer testMQTTShutdownServer(s)

	mcp, mpr := testMQTTConnect(t, &mqttConnInfo{cleanSess: true}, o.MQTT.Host, o.MQTT.Port)
	defer mcp.Close()
	testMQTTCheckConnAck(t, mpr, mqttConnAckRCConnectionAccepted, false)

	mc, r := testMQTTConnect(t, &mqttConnInfo{user: "sub", cleanSess: true}, o.MQTT.Host, o.MQTT.Port)
	defer mc.Close()
	testMQTTCheckConnAck(t, r, mqttConnAckRCConnectionAccepted, false)

	testMQTTSub(t, 1, mc, r, []*mqttFilter{{filter: "foo bar", qos: 0}}, []byte{mqttSubAckFailure})
}

// Filters containing characters that would corrupt the NATS wire protocol
// (\t, \n, \r, \f) must result in a per-filter SUBACK failure, not a
// connection drop. mqttFilterToNATSSubject rejects them; the SUBSCRIBE
// parser logs and continues so the failure surfaces as mqttSubAckFailure.
func TestMQTTSubWithControlChars(t *testing.T) {
	o := testMQTTDefaultOptions()
	s := testMQTTRunServer(t, o)
	defer testMQTTShutdownServer(s)

	for _, test := range []struct {
		name   string
		filter string
	}{
		{"tab", "foo\tbar"},
		{"line feed", "foo\nbar"},
		{"carriage return", "foo\rbar"},
		{"form feed", "foo\fbar"},
	} {
		t.Run(test.name, func(t *testing.T) {
			mc, r := testMQTTConnect(t, &mqttConnInfo{cleanSess: true}, o.MQTT.Host, o.MQTT.Port)
			defer mc.Close()
			testMQTTCheckConnAck(t, r, mqttConnAckRCConnectionAccepted, false)

			// Mix a bad filter with a good one to prove the good one still
			// succeeds (the connection is not torn down by the bad filter).
			testMQTTSub(t, 1, mc, r,
				[]*mqttFilter{{filter: test.filter, qos: 0}, {filter: "good/topic", qos: 0}},
				[]byte{mqttSubAckFailure, 0})
		})
	}
}

func TestMQTTSubCaseSensitive(t *testing.T) {
	o := testMQTTDefaultOptions()
	s := testMQTTRunServer(t, o)
	defer testMQTTShutdownServer(s)

	mcp, mpr := testMQTTConnect(t, &mqttConnInfo{cleanSess: true}, o.MQTT.Host, o.MQTT.Port)
	defer mcp.Close()
	testMQTTCheckConnAck(t, mpr, mqttConnAckRCConnectionAccepted, false)

	mc, r := testMQTTConnect(t, &mqttConnInfo{user: "sub", cleanSess: true}, o.MQTT.Host, o.MQTT.Port)
	defer mc.Close()
	testMQTTCheckConnAck(t, r, mqttConnAckRCConnectionAccepted, false)

	testMQTTSub(t, 1, mc, r, []*mqttFilter{{filter: "Foo/Bar", qos: 0}}, []byte{0})
	testMQTTFlush(t, mc, nil, r)

	testMQTTPublish(t, mcp, r, 0, false, false, "Foo/Bar", 0, []byte("msg"))
	testMQTTCheckPubMsg(t, mc, r, "Foo/Bar", 0, []byte("msg"))

	testMQTTPublish(t, mcp, r, 0, false, false, "foo/bar", 0, []byte("msg"))
	testMQTTExpectNothing(t, r)

	nc := natsConnect(t, s.ClientURL())
	defer nc.Close()

	natsPub(t, nc, "Foo.Bar", []byte("nats"))
	testMQTTCheckPubMsg(t, mc, r, "Foo/Bar", 0, []byte("nats"))

	natsPub(t, nc, "foo.bar", []byte("nats"))
	testMQTTExpectNothing(t, r)
}

func TestMQTTPubSubMatrix(t *testing.T) {
	for _, test := range []struct {
		name        string
		natsPub     bool
		mqttPub     bool
		mqttPubQoS  byte
		natsSub     bool
		mqttSubQoS0 bool
		mqttSubQoS1 bool
	}{
		{"NATS to MQTT sub QoS-0", true, false, 0, false, true, false},
		{"NATS to MQTT sub QoS-1", true, false, 0, false, false, true},
		{"NATS to MQTT sub QoS-0 and QoS-1", true, false, 0, false, true, true},

		{"MQTT QoS-0 to NATS sub", false, true, 0, true, false, false},
		{"MQTT QoS-0 to MQTT sub QoS-0", false, true, 0, false, true, false},
		{"MQTT QoS-0 to MQTT sub QoS-1", false, true, 0, false, false, true},
		{"MQTT QoS-0 to NATS sub and MQTT sub QoS-0", false, true, 0, true, true, false},
		{"MQTT QoS-0 to NATS sub and MQTT sub QoS-1", false, true, 0, true, false, true},
		{"MQTT QoS-0 to all subs", false, true, 0, true, true, true},

		{"MQTT QoS-1 to NATS sub", false, true, 1, true, false, false},
		{"MQTT QoS-1 to MQTT sub QoS-0", false, true, 1, false, true, false},
		{"MQTT QoS-1 to MQTT sub QoS-1", false, true, 1, false, false, true},
		{"MQTT QoS-1 to NATS sub and MQTT sub QoS-0", false, true, 1, true, true, false},
		{"MQTT QoS-1 to NATS sub and MQTT sub QoS-1", false, true, 1, true, false, true},
		{"MQTT QoS-1 to all subs", false, true, 1, true, true, true},
	} {
		t.Run(test.name, func(t *testing.T) {
			o := testMQTTDefaultOptions()
			s := testMQTTRunServer(t, o)
			defer testMQTTShutdownServer(s)

			nc := natsConnect(t, s.ClientURL())
			defer nc.Close()

			mc, r := testMQTTConnect(t, &mqttConnInfo{cleanSess: true}, o.MQTT.Host, o.MQTT.Port)
			defer mc.Close()
			testMQTTCheckConnAck(t, r, mqttConnAckRCConnectionAccepted, false)

			mc1, r1 := testMQTTConnect(t, &mqttConnInfo{cleanSess: true}, o.MQTT.Host, o.MQTT.Port)
			defer mc1.Close()
			testMQTTCheckConnAck(t, r1, mqttConnAckRCConnectionAccepted, false)

			mc2, r2 := testMQTTConnect(t, &mqttConnInfo{cleanSess: true}, o.MQTT.Host, o.MQTT.Port)
			defer mc2.Close()
			testMQTTCheckConnAck(t, r2, mqttConnAckRCConnectionAccepted, false)

			// First setup subscriptions based on test options.
			var ns *nats.Subscription
			if test.natsSub {
				ns = natsSubSync(t, nc, "foo")
			}
			if test.mqttSubQoS0 {
				testMQTTSub(t, 1, mc1, r1, []*mqttFilter{{filter: "foo", qos: 0}}, []byte{0})
				testMQTTFlush(t, mc1, nil, r1)
			}
			if test.mqttSubQoS1 {
				testMQTTSub(t, 1, mc2, r2, []*mqttFilter{{filter: "foo", qos: 1}}, []byte{1})
				testMQTTFlush(t, mc2, nil, r2)
			}

			// Just as a barrier
			natsFlush(t, nc)

			// Now publish
			if test.natsPub {
				natsPub(t, nc, "foo", []byte("msg"))
			} else {
				testMQTTPublish(t, mc, r, test.mqttPubQoS, false, false, "foo", 1, []byte("msg"))
			}

			// Check message received
			if test.natsSub {
				natsNexMsg(t, ns, time.Second)
				// Make sure no other is received
				if msg, err := ns.NextMsg(50 * time.Millisecond); err == nil {
					t.Fatalf("Should not have gotten a second message, got %v", msg)
				}
			}
			if test.mqttSubQoS0 {
				testMQTTCheckPubMsg(t, mc1, r1, "foo", 0, []byte("msg"))
				testMQTTExpectNothing(t, r1)
			}
			if test.mqttSubQoS1 {
				var expectedFlag byte
				if test.mqttPubQoS > 0 {
					expectedFlag = test.mqttPubQoS << 1
				}
				testMQTTCheckPubMsg(t, mc2, r2, "foo", expectedFlag, []byte("msg"))
				testMQTTExpectNothing(t, r2)
			}
		})
	}
}

func TestMQTTPreventSubWithMQTTSubPrefix(t *testing.T) {
	o := testMQTTDefaultOptions()
	s := testMQTTRunServer(t, o)
	defer testMQTTShutdownServer(s)

	mc, r := testMQTTConnect(t, &mqttConnInfo{cleanSess: true}, o.MQTT.Host, o.MQTT.Port)
	defer mc.Close()
	testMQTTCheckConnAck(t, r, mqttConnAckRCConnectionAccepted, false)
	testMQTTSub(t, 1, mc, r,
		[]*mqttFilter{{filter: strings.ReplaceAll(mqttSubPrefix, ".", "/") + "foo/bar", qos: 1}},
		[]byte{mqttSubAckFailure})
}

func TestMQTTSubWithNATSStream(t *testing.T) {
	o := testMQTTDefaultOptions()
	s := testMQTTRunServer(t, o)
	defer testMQTTShutdownServer(s)

	mc, r := testMQTTConnect(t, &mqttConnInfo{cleanSess: true}, o.MQTT.Host, o.MQTT.Port)
	defer mc.Close()
	testMQTTCheckConnAck(t, r, mqttConnAckRCConnectionAccepted, false)
	testMQTTSub(t, 1, mc, r, []*mqttFilter{{filter: "foo/bar", qos: 1}}, []byte{1})
	testMQTTFlush(t, mc, nil, r)

	mcp, rp := testMQTTConnect(t, &mqttConnInfo{cleanSess: true}, o.MQTT.Host, o.MQTT.Port)
	defer mcp.Close()
	testMQTTCheckConnAck(t, rp, mqttConnAckRCConnectionAccepted, false)
	testMQTTFlush(t, mcp, nil, rp)

	nc := natsConnect(t, s.ClientURL())
	defer nc.Close()

	sc := &StreamConfig{
		Name:      "test",
		Storage:   FileStorage,
		Retention: InterestPolicy,
		Subjects:  []string{"foo.>"},
	}
	mset, err := s.GlobalAccount().addStream(sc)
	if err != nil {
		t.Fatalf("Unable to create stream: %v", err)
	}

	sub := natsSubSync(t, nc, "bar")
	cc := &ConsumerConfig{
		Durable:        "dur",
		AckPolicy:      AckExplicit,
		DeliverSubject: "bar",
	}
	if _, err := mset.addConsumer(cc); err != nil {
		t.Fatalf("Unable to add consumer: %v", err)
	}

	// Now send message from NATS
	resp, err := nc.Request("foo.bar", []byte("nats"), time.Second)
	if err != nil {
		t.Fatalf("Error publishing: %v", err)
	}
	ar := &ApiResponse{}
	if err := json.Unmarshal(resp.Data, ar); err != nil || ar.Error != nil {
		t.Fatalf("Unexpected response: err=%v resp=%+v", err, ar.Error)
	}

	// Check that message is received by both
	checkRecv := func(content string, flags byte) {
		t.Helper()
		if msg := natsNexMsg(t, sub, time.Second); string(msg.Data) != content {
			t.Fatalf("Expected %q, got %q", content, msg.Data)
		}
		testMQTTCheckPubMsg(t, mc, r, "foo/bar", flags, []byte(content))
	}
	checkRecv("nats", 0)

	// Send from MQTT as a QoS0
	testMQTTPublish(t, mcp, rp, 0, false, false, "foo/bar", 0, []byte("qos0"))
	checkRecv("qos0", 0)

	// Send from MQTT as a QoS1
	testMQTTPublish(t, mcp, rp, 1, false, false, "foo/bar", 1, []byte("qos1"))
	checkRecv("qos1", mqttPubQos1)
}

func TestMQTTTrackPendingOverrun(t *testing.T) {
	sess := mqttSession{}

	sess.last_pi = 0xFFFF
	pi := sess.trackPublishRetained("foo", 1)
	if pi != 1 {
		t.Fatalf("Expected 1, got %v", pi)
	}

	p := &mqttPending{}
	for i := 1; i <= 0xFFFF; i++ {
		sess.pendingPublish[uint16(i)] = p
	}
	pi, _ = sess.trackPublish("test", "test", 1)
	if pi != 0 {
		t.Fatalf("Expected 0, got %v", pi)
	}

	delete(sess.pendingPublish, 1234)
	pi = sess.trackPublishRetained("foo", 1)
	if pi != 1234 {
		t.Fatalf("Expected 1234, got %v", pi)
	}
}

func TestMQTTSubRestart(t *testing.T) {
	o := testMQTTDefaultOptions()
	s := testMQTTRunServer(t, o)
	defer testMQTTShutdownServer(s)

	nc := natsConnect(t, s.ClientURL())
	defer nc.Close()

	mc, r := testMQTTConnect(t, &mqttConnInfo{clientID: "sub", cleanSess: false}, o.MQTT.Host, o.MQTT.Port)
	defer mc.Close()
	testMQTTCheckConnAck(t, r, mqttConnAckRCConnectionAccepted, false)

	// Start an MQTT subscription QoS=1 on "foo"
	testMQTTSub(t, 1, mc, r, []*mqttFilter{{filter: "foo", qos: 1}}, []byte{1})
	testMQTTFlush(t, mc, nil, r)

	// Now start a NATS subscription on ">" (anything that would match the JS consumer delivery subject)
	natsSubSync(t, nc, ">")
	natsFlush(t, nc)

	// Restart the MQTT client
	testMQTTDisconnect(t, mc, nil)
	mc.Close()

	mc, r = testMQTTConnect(t, &mqttConnInfo{clientID: "sub", cleanSess: false}, o.MQTT.Host, o.MQTT.Port)
	defer mc.Close()
	testMQTTCheckConnAck(t, r, mqttConnAckRCConnectionAccepted, true)

	// Restart an MQTT subscription QoS=1 on "foo"
	testMQTTSub(t, 1, mc, r, []*mqttFilter{{filter: "foo", qos: 1}}, []byte{1})
	testMQTTFlush(t, mc, nil, r)

	pc, pr := testMQTTConnect(t, &mqttConnInfo{clientID: "pub", cleanSess: true}, o.MQTT.Host, o.MQTT.Port)
	defer pc.Close()
	testMQTTCheckConnAck(t, pr, mqttConnAckRCConnectionAccepted, false)

	// Publish a message QoS1
	testMQTTPublish(t, pc, pr, 1, false, false, "foo", 1, []byte("msg1"))
	// Make sure we receive it
	testMQTTCheckPubMsg(t, mc, r, "foo", mqttPubQos1, []byte("msg1"))

	// Now "restart" the subscription but as a Qos0
	testMQTTSub(t, 1, mc, r, []*mqttFilter{{filter: "foo", qos: 0}}, []byte{0})
	testMQTTFlush(t, mc, nil, r)

	// Publish a message QoS
	testMQTTPublish(t, pc, pr, 1, false, false, "foo", 1, []byte("msg2"))
	// Make sure we receive but as a QoS0
	testMQTTCheckPubMsg(t, mc, r, "foo", 0, []byte("msg2"))
}

func testMQTTGetClusterTemplaceNoLeaf() string {
	return strings.Replace(jsClusterTemplWithLeafAndMQTT, "{{leaf}}", "", 1)
}

func TestMQTTSubPropagation(t *testing.T) {
	cl := createJetStreamClusterWithTemplate(t, testMQTTGetClusterTemplaceNoLeaf(), "MQTT", 2)
	defer cl.shutdown()

	o := cl.opts[0]
	s2 := cl.servers[1]
	nc := natsConnect(t, s2.ClientURL())
	defer nc.Close()

	mc, r := testMQTTConnect(t, &mqttConnInfo{cleanSess: true}, o.MQTT.Host, o.MQTT.Port)
	defer mc.Close()
	testMQTTCheckConnAck(t, r, mqttConnAckRCConnectionAccepted, false)

	testMQTTSub(t, 1, mc, r, []*mqttFilter{{filter: "foo/#", qos: 0}}, []byte{0})
	testMQTTFlush(t, mc, nil, r)

	// Because in MQTT foo/# means foo.> but also foo, check that this is propagated
	checkSubInterest(t, s2, globalAccountName, "foo", time.Second)

	// Publish on foo.bar, foo./ and foo and we should receive them
	natsPub(t, nc, "foo.bar", []byte("hello"))
	testMQTTCheckPubMsg(t, mc, r, "foo/bar", 0, []byte("hello"))

	natsPub(t, nc, "foo./", []byte("from"))
	testMQTTCheckPubMsg(t, mc, r, "foo/", 0, []byte("from"))

	natsPub(t, nc, "foo", []byte("NATS"))
	testMQTTCheckPubMsg(t, mc, r, "foo", 0, []byte("NATS"))
}

func TestMQTTCluster(t *testing.T) {
	cl := createJetStreamClusterWithTemplate(t, testMQTTGetClusterTemplaceNoLeaf(), "MQTT", 2)
	defer cl.shutdown()

	for _, topTest := range []struct {
		name    string
		restart bool
	}{
		{"first_start", true},
		{"restart", false},
	} {
		t.Run(topTest.name, func(t *testing.T) {
			for _, test := range []struct {
				name   string
				subQos byte
			}{
				{"qos_0", 0},
				{"qos_1", 1},
			} {
				t.Run(test.name, func(t *testing.T) {
					clientID := nuid.Next()

					o := cl.opts[0]
					mc, r := testMQTTConnectRetry(t, &mqttConnInfo{clientID: clientID, cleanSess: false}, o.MQTT.Host, o.MQTT.Port, 5)
					defer mc.Close()
					testMQTTCheckConnAck(t, r, mqttConnAckRCConnectionAccepted, false)

					testMQTTSub(t, 1, mc, r, []*mqttFilter{{filter: "foo/#", qos: test.subQos}}, []byte{test.subQos})
					testMQTTFlush(t, mc, nil, r)

					check := func(mc net.Conn, r *mqttReader, o *Options, s *Server) {
						t.Helper()

						nc := natsConnect(t, s.ClientURL())
						defer nc.Close()

						natsPub(t, nc, "foo.bar", []byte("fromNats"))
						testMQTTCheckPubMsg(t, mc, r, "foo/bar", 0, []byte("fromNats"))

						mpc, pr := testMQTTConnect(t, &mqttConnInfo{cleanSess: true}, o.MQTT.Host, o.MQTT.Port)
						defer mpc.Close()
						testMQTTCheckConnAck(t, pr, mqttConnAckRCConnectionAccepted, false)

						testMQTTPublish(t, mpc, pr, 0, false, false, "foo/baz", 0, []byte("mqtt_qos0"))
						testMQTTCheckPubMsg(t, mc, r, "foo/baz", 0, []byte("mqtt_qos0"))

						testMQTTPublish(t, mpc, pr, 1, false, false, "foo/bat", 1, []byte("mqtt_qos1"))
						expectedQoS := byte(0)
						if test.subQos == 1 {
							expectedQoS = mqttPubQos1
						}
						testMQTTCheckPubMsg(t, mc, r, "foo/bat", expectedQoS, []byte("mqtt_qos1"))
						testMQTTDisconnect(t, mpc, nil)
					}
					check(mc, r, cl.opts[0], cl.servers[0])
					check(mc, r, cl.opts[1], cl.servers[1])

					// Start the same subscription from the other server. It should disconnect
					// the one connected in the first server.
					o = cl.opts[1]

					mc2, r2 := testMQTTConnect(t, &mqttConnInfo{clientID: clientID, cleanSess: false}, o.MQTT.Host, o.MQTT.Port)
					defer mc2.Close()
					testMQTTCheckConnAck(t, r2, mqttConnAckRCConnectionAccepted, true)

					// Expect first connection to be closed.
					testMQTTExpectDisconnect(t, mc)

					// Now re-run the checks
					check(mc2, r2, cl.opts[0], cl.servers[0])
					check(mc2, r2, cl.opts[1], cl.servers[1])

					// Disconnect our sub and restart with clean session then disconnect again to clear the state.
					testMQTTDisconnect(t, mc2, nil)
					mc2.Close()
					mc2, r2 = testMQTTConnect(t, &mqttConnInfo{clientID: clientID, cleanSess: true}, o.MQTT.Host, o.MQTT.Port)
					defer mc2.Close()
					testMQTTCheckConnAck(t, r2, mqttConnAckRCConnectionAccepted, false)
					testMQTTFlush(t, mc2, nil, r2)
					testMQTTDisconnect(t, mc2, nil)

					// Remove the session from the flappers so we can restart the test
					// without failure and have to wait for 1sec before being able to reconnect.
					s := cl.servers[0]
					sm := &s.mqtt.sessmgr
					sm.mu.Lock()
					asm := sm.sessions[globalAccountName]
					sm.mu.Unlock()
					if asm != nil {
						asm.mu.Lock()
						delete(asm.flappers, clientID)
						asm.mu.Unlock()
					}
				})
			}
			if !t.Failed() && topTest.restart {
				cl.stopAll()
				cl.restartAll()

				streams := []string{mqttStreamName, mqttRetainedMsgsStreamName, mqttSessStreamName}
				for _, sn := range streams {
					cl.waitOnStreamLeader(globalAccountName, sn)
				}

				mset, err := cl.randomServer().GlobalAccount().lookupStream(mqttRetainedMsgsStreamName)
				if err != nil {
					t.Fatalf("Expected to find a stream for %q", mqttRetainedMsgsStreamName)
				}

				rmConsumerNames := []string{}
				mset.mu.RLock()
				for _, o := range mset.consumers {
					rmConsumerNames = append(rmConsumerNames, o.name)
				}
				mset.mu.RUnlock()

				for _, consumerName := range rmConsumerNames {
					cl.waitOnConsumerLeader(globalAccountName, mqttRetainedMsgsStreamName, consumerName)
				}
			}
		})
	}
}

func testMQTTConnectDisconnect(t *testing.T, o *Options, clientID string, clean bool, found bool) {
	t.Helper()
	mc, r := testMQTTConnect(t, &mqttConnInfo{clientID: clientID, cleanSess: clean}, o.MQTT.Host, o.MQTT.Port)
	testMQTTCheckConnAck(t, r, mqttConnAckRCConnectionAccepted, found)
	testMQTTDisconnectEx(t, mc, nil, false)
	mc.Close()
}

func TestMQTTClusterConnectDisconnectClean(t *testing.T) {
	// The purpose of this test was to illustrate and verify
	// https://github.com/nats-io/nats-server/pull/4734. Due to its timing
	// sensitivity it has never been 100% reliable, prone to flaking on an
	// occasional MQTT Connect failure (API timeout?). Skip for now, to reduce
	// the flaky noise.
	t.Skip()

	nServers := 3
	cl := createJetStreamClusterWithTemplate(t, testMQTTGetClusterTemplaceNoLeaf(), "MQTT", nServers)
	defer cl.shutdown()

	clientID := nuid.Next()

	// test runs a connect/disconnect against a random server in the cluster, as
	// specified.
	N := 100
	for n := 0; n < N; n++ {
		testMQTTConnectDisconnect(t, cl.opts[rand.IntN(nServers)], clientID, true, false)
	}
}

func TestMQTTClusterConnectDisconnectPersist(t *testing.T) {
	// The purpose of this test was to illustrate and verify
	// https://github.com/nats-io/nats-server/pull/4734. Due to its timing
	// sensitivity it has never been 100% reliable, prone to flaking on an
	// occasional MQTT Connect failure (API timeout?). Skip for now, to reduce
	// the flaky noise.
	t.Skip()

	nServers := 3
	cl := createJetStreamClusterWithTemplate(t, testMQTTGetClusterTemplaceNoLeaf(), "MQTT", nServers)
	defer cl.shutdown()

	clientID := nuid.Next()

	// test runs a connect/disconnect against a random server in the cluster, as
	// specified.
	N := 20
	for n := 0; n < N; n++ {
		// First clean sessions on all servers
		for i := 0; i < nServers; i++ {
			testMQTTConnectDisconnect(t, cl.opts[i], clientID, true, false)
		}

		testMQTTConnectDisconnect(t, cl.opts[0], clientID, false, false)
		testMQTTConnectDisconnect(t, cl.opts[1], clientID, false, true)
		testMQTTConnectDisconnect(t, cl.opts[2], clientID, false, true)
		testMQTTConnectDisconnect(t, cl.opts[0], clientID, false, true)
	}
}

func TestMQTTClusterRetainedMsg(t *testing.T) {
	cl := createJetStreamClusterWithTemplate(t, testMQTTGetClusterTemplaceNoLeaf(), "MQTT", 2)
	defer cl.shutdown()

	srv1Opts := cl.opts[0]
	srv2Opts := cl.opts[1]

	// Connect subscription on server 1.
	mc, rc := testMQTTConnectRetry(t, &mqttConnInfo{clientID: "sub", cleanSess: false}, srv1Opts.MQTT.Host, srv1Opts.MQTT.Port, 5)
	defer mc.Close()
	testMQTTCheckConnAck(t, rc, mqttConnAckRCConnectionAccepted, false)

	testMQTTSub(t, 1, mc, rc, []*mqttFilter{{filter: "foo/#", qos: 1}}, []byte{1})
	testMQTTFlush(t, mc, nil, rc)

	// Create a publisher from server 2.
	mp, rp := testMQTTConnect(t, &mqttConnInfo{cleanSess: true}, srv2Opts.MQTT.Host, srv2Opts.MQTT.Port)
	defer mp.Close()
	testMQTTCheckConnAck(t, rp, mqttConnAckRCConnectionAccepted, false)

	// Send retained message.
	testMQTTPublish(t, mp, rp, 1, false, true, "foo/bar", 1, []byte("retained"))
	// Check it is received.
	testMQTTCheckPubMsg(t, mc, rc, "foo/bar", mqttPubQos1, []byte("retained"))

	// Start a new subscription on server 1 and make sure we receive the retained message
	mc2, rc2 := testMQTTConnect(t, &mqttConnInfo{cleanSess: true}, srv1Opts.MQTT.Host, srv1Opts.MQTT.Port)
	defer mc2.Close()
	testMQTTCheckConnAck(t, rc2, mqttConnAckRCConnectionAccepted, false)
	testMQTTSub(t, 1, mc2, rc2, []*mqttFilter{{filter: "foo/#", qos: 1}}, []byte{1})
	testMQTTCheckPubMsg(t, mc2, rc2, "foo/bar", mqttPubQos1|mqttPubFlagRetain, []byte("retained"))
	testMQTTDisconnect(t, mc2, nil)
	mc2.Close()

	// Send an empty retained message which should remove it from storage, but still be delivered.
	testMQTTPublish(t, mp, rp, 1, false, true, "foo/bar", 1, []byte(""))
	testMQTTCheckPubMsg(t, mc, rc, "foo/bar", mqttPubQos1, []byte(""))

	// Now shutdown the consumer connection
	testMQTTDisconnect(t, mc, nil)
	mc.Close()

	// Reconnect to server where the retained message was published (server 2)
	mc, rc = testMQTTConnect(t, &mqttConnInfo{clientID: "sub", cleanSess: false}, srv2Opts.MQTT.Host, srv2Opts.MQTT.Port)
	defer mc.Close()
	testMQTTCheckConnAck(t, rc, mqttConnAckRCConnectionAccepted, true)
	testMQTTSub(t, 1, mc, rc, []*mqttFilter{{filter: "foo/#", qos: 1}}, []byte{1})
	// The retained message should not be delivered.
	testMQTTExpectNothing(t, rc)
	// Now disconnect and reconnect back to first server
	testMQTTDisconnect(t, mc, nil)
	mc.Close()

	// Now reconnect to the server 1, which is not where the messages were published, and check
	// that we don't receive the message.
	mc, rc = testMQTTConnect(t, &mqttConnInfo{clientID: "sub", cleanSess: false}, srv1Opts.MQTT.Host, srv1Opts.MQTT.Port)
	defer mc.Close()
	testMQTTCheckConnAck(t, rc, mqttConnAckRCConnectionAccepted, true)
	testMQTTSub(t, 1, mc, rc, []*mqttFilter{{filter: "foo/#", qos: 1}}, []byte{1})
	testMQTTExpectNothing(t, rc)
	testMQTTDisconnect(t, mc, nil)
	mc.Close()

	// Will now test network deletes

	// Create a subscription on server 1 and server 2
	mc, rc = testMQTTConnect(t, &mqttConnInfo{clientID: "sub_one", cleanSess: false}, srv1Opts.MQTT.Host, srv1Opts.MQTT.Port)
	defer mc.Close()
	testMQTTCheckConnAck(t, rc, mqttConnAckRCConnectionAccepted, false)
	testMQTTSub(t, 1, mc, rc, []*mqttFilter{{filter: "bar", qos: 1}}, []byte{1})
	testMQTTFlush(t, mc, nil, rc)

	mc2, rc2 = testMQTTConnect(t, &mqttConnInfo{clientID: "sub_two", cleanSess: false}, srv2Opts.MQTT.Host, srv2Opts.MQTT.Port)
	defer mc2.Close()
	testMQTTCheckConnAck(t, rc2, mqttConnAckRCConnectionAccepted, false)
	testMQTTSub(t, 1, mc2, rc2, []*mqttFilter{{filter: "bar", qos: 1}}, []byte{1})
	testMQTTFlush(t, mc2, nil, rc2)

	// Publish 1 retained message (producer is connected to server 2)
	testMQTTPublish(t, mp, rp, 1, false, true, "bar", 1, []byte("msg1"))

	// Make sure messages are received by both
	testMQTTCheckPubMsg(t, mc, rc, "bar", mqttPubQos1, []byte("msg1"))
	testMQTTCheckPubMsg(t, mc2, rc2, "bar", mqttPubQos1, []byte("msg1"))

	// Now send an empty retained message that should delete it. For the one on server 1,
	// this will be a network delete.
	testMQTTPublish(t, mp, rp, 1, false, true, "bar", 1, []byte(""))
	testMQTTCheckPubMsg(t, mc, rc, "bar", mqttPubQos1, []byte(""))
	testMQTTCheckPubMsg(t, mc2, rc2, "bar", mqttPubQos1, []byte(""))

	// Now send a new retained message
	testMQTTPublish(t, mp, rp, 1, false, true, "bar", 1, []byte("msg2"))

	// Again, verify that they all receive it.
	testMQTTCheckPubMsg(t, mc, rc, "bar", mqttPubQos1, []byte("msg2"))
	testMQTTCheckPubMsg(t, mc2, rc2, "bar", mqttPubQos1, []byte("msg2"))

	// But now, restart the consumer that was in the server that processed the
	// original network delete.
	testMQTTDisconnect(t, mc, nil)
	mc.Close()

	mc, rc = testMQTTConnect(t, &mqttConnInfo{clientID: "sub_one", cleanSess: false}, srv1Opts.MQTT.Host, srv1Opts.MQTT.Port)
	defer mc.Close()
	testMQTTCheckConnAck(t, rc, mqttConnAckRCConnectionAccepted, true)
	testMQTTSub(t, 1, mc, rc, []*mqttFilter{{filter: "bar", qos: 1}}, []byte{1})
	// Expect the message to be delivered as retained
	testMQTTCheckPubMsg(t, mc, rc, "bar", mqttPubQos1|mqttPubFlagRetain, []byte("msg2"))
}

func TestMQTTRetainedMsgMigration(t *testing.T) {
	o := testMQTTDefaultOptions()
	s := testMQTTRunServer(t, o)
	defer testMQTTShutdownServer(s)

	nc, js := jsClientConnect(t, s)
	defer nc.Close()

	// Create the retained messages stream to listen on the old subject first.
	// The server will correct this when the migration takes place.
	_, err := js.AddStream(&nats.StreamConfig{
		Name:      mqttRetainedMsgsStreamName,
		Subjects:  []string{`$MQTT.rmsgs`},
		Storage:   nats.FileStorage,
		Retention: nats.LimitsPolicy,
		Replicas:  1,
	})
	require_NoError(t, err)

	// Publish some retained messages on the old "$MQTT.rmsgs" subject.
	const N = 100
	for i := 0; i < N; i++ {
		msg := fmt.Sprintf(
			`{"origin":"b5IQZNtG","subject":"test%d","topic":"test%d","msg":"YmFy","flags":1}`, i, i,
		)
		_, err := js.Publish(`$MQTT.rmsgs`, []byte(msg))
		require_NoError(t, err)
	}

	// Check that the old subject looks right.
	si, err := js.StreamInfo(mqttRetainedMsgsStreamName, &nats.StreamInfoRequest{
		SubjectsFilter: `$MQTT.>`,
	})
	require_NoError(t, err)
	if si.State.NumSubjects != 1 {
		t.Fatalf("expected 1 subject, got %d", si.State.NumSubjects)
	}
	if n := si.State.Subjects[`$MQTT.rmsgs`]; n != N {
		t.Fatalf("expected to find %d messages on the original subject but found %d", N, n)
	}

	// Create an MQTT client, this will cause a migration to take place.
	mc, rc := testMQTTConnect(t, &mqttConnInfo{clientID: "sub", cleanSess: true}, o.MQTT.Host, o.MQTT.Port)
	defer mc.Close()
	testMQTTCheckConnAck(t, rc, mqttConnAckRCConnectionAccepted, false)

	as := testMQTTGetAccountSessionManager(t, s, "sub")
	checkFor(t, time.Second, 10*time.Millisecond, func() error {
		as.mu.RLock()
		defer as.mu.RUnlock()
		if n := as.retmsgs.Size(); n != N {
			return fmt.Errorf("Got only %v retained messages", n)
		}
		return nil
	})

	testMQTTSub(t, 1, mc, rc, []*mqttFilter{{filter: "+", qos: 0}}, []byte{0})
	topics := map[string]struct{}{}
	for i := 0; i < N; i++ {
		_, _, topic, _ := testMQTTGetPubMsgEx(t, mc, rc, _EMPTY_, []byte("bar"))
		topics[topic] = struct{}{}
	}
	if len(topics) != N {
		t.Fatalf("Unexpected topics: %v", topics)
	}

	// Now look at the stream, there should be N messages on the new
	// divided subjects and none on the old undivided subject.
	si, err = js.StreamInfo(mqttRetainedMsgsStreamName, &nats.StreamInfoRequest{
		SubjectsFilter: `$MQTT.>`,
	})
	require_NoError(t, err)
	if si.State.NumSubjects != N {
		t.Fatalf("expected %d subjects, got %d", N, si.State.NumSubjects)
	}
	if n := si.State.Subjects[`$MQTT.rmsgs`]; n > 0 {
		t.Fatalf("expected to find no messages on the original subject but found %d", n)
	}

	// Check that the message counts look right. There should be one
	// retained message per key.
	for i := 0; i < N; i++ {
		expected := fmt.Sprintf(`$MQTT.rmsgs.test%d`, i)
		n, ok := si.State.Subjects[expected]
		if !ok {
			t.Fatalf("expected to find %q but didn't", expected)
		}
		if n != 1 {
			t.Fatalf("expected %q to have 1 message but had %d", expected, n)
		}
	}
}

func TestMQTTRetainedNoMsgBodyCorruption(t *testing.T) {
	f := func() {
		o := testMQTTDefaultOptions()
		s := testMQTTRunServer(t, o)
		defer testMQTTShutdownServer(s)

		// Send a retained message.
		c, r := testMQTTConnect(t, &mqttConnInfo{clientID: "pub", cleanSess: true}, o.MQTT.Host, o.MQTT.Port)
		defer c.Close()
		testMQTTCheckConnAck(t, r, mqttConnAckRCConnectionAccepted, false)
		testMQTTPublish(t, c, r, 0, false, true, "foo/bar", 0, []byte("retained 1"))
		testMQTTFlush(t, c, nil, r)

		checkRetained := func(msg string) {
			t.Helper()
			c, r := testMQTTConnect(t, &mqttConnInfo{cleanSess: true}, o.MQTT.Host, o.MQTT.Port)
			defer c.Close()
			testMQTTCheckConnAck(t, r, mqttConnAckRCConnectionAccepted, false)
			testMQTTSub(t, 1, c, r, []*mqttFilter{{filter: "foo/#", qos: 0}}, []byte{0})
			testMQTTCheckPubMsg(t, c, r, "foo/bar", mqttPubFlagRetain, []byte(msg))
		}
		// Subscribe to make it load into the cache.
		checkRetained("retained 1")

		// Now send another one.
		testMQTTPublish(t, c, r, 0, false, true, "foo/bar", 0, []byte("retained 2"))
		testMQTTFlush(t, c, nil, r)

		// Check it is updated
		checkRetained("retained 2")

		// Now we will simulate an update coming from another server
		// if we were in cluster mode.
		nc := natsConnect(t, s.ClientURL())
		defer nc.Close()

		msg := nats.NewMsg("$MQTT.rmsgs.foo.bar")
		msg.Header.Set(mqttNatsRetainedMessageOrigin, "XXXXXXXX")
		msg.Header.Set(mqttNatsRetainedMessageTopic, "foo/bar")
		msg.Header.Set(mqttNatsRetainedMessageFlags, "1")
		msg.Data = []byte("retained 3")
		nc.PublishMsg(msg)
		natsFlush(t, nc)

		// Have a continuous flow of updates coming in
		wg := sync.WaitGroup{}
		wg.Add(1)
		ch := make(chan struct{})
		go func() {
			defer wg.Done()
			for {
				nc.PublishMsg(msg)
				select {
				case <-ch:
					return
				default:
				}
			}
		}()

		// Retrieve the account session manager using the "pub" client we have.
		as := testMQTTGetAccountSessionManager(t, s, "pub")
		as.mu.RLock()
		cache := as.rmsCache
		as.mu.RUnlock()

		// Wait to make sure at least the first update occurs
		checkFor(t, 5*time.Second, 10*time.Millisecond, func() error {
			v, ok := cache.Load("foo.bar")
			if !ok {
				return errors.New("not in the cache")
			}
			rm := v.(*mqttRetainedMsg)
			if !bytes.Equal(rm.Msg, []byte("retained 3")) {
				return fmt.Errorf("Retained message not updated, got %q", rm.Msg)
			}
			return nil
		})
		// Repeat starting a subscription to check the retained message and
		// make sure it is not corrupted. With the bug, the payload will at
		// the very least contain trailing "\r\n" and possibly be corrupted
		// (and the race detector would report a race).
		for range 50 {
			checkRetained("retained 3")
		}
		close(ch)
		wg.Wait()
	}
	for range 5 {
		f()
	}
}

func TestMQTTClusterReplicasCount(t *testing.T) {
	for _, test := range []struct {
		size     int
		replicas int
	}{
		{1, 1},
		{2, 2},
		{3, 3},
		{5, 3},
	} {
		t.Run(fmt.Sprintf("size %v", test.size), func(t *testing.T) {
			var s *Server
			var o *Options
			if test.size == 1 {
				o = testMQTTDefaultOptions()
				s = testMQTTRunServer(t, o)
				defer testMQTTShutdownServer(s)
			} else {
				cl := createJetStreamClusterWithTemplate(t, testMQTTGetClusterTemplaceNoLeaf(), "MQTT", test.size)
				defer cl.shutdown()
				o = cl.opts[0]
				s = cl.randomServer()
			}

			mc, rc := testMQTTConnectRetry(t, &mqttConnInfo{clientID: "sub", cleanSess: false}, o.MQTT.Host, o.MQTT.Port, 5)
			defer mc.Close()
			testMQTTCheckConnAck(t, rc, mqttConnAckRCConnectionAccepted, false)
			testMQTTSub(t, 1, mc, rc, []*mqttFilter{{filter: "foo/#", qos: 1}}, []byte{1})
			testMQTTFlush(t, mc, nil, rc)

			nc := natsConnect(t, s.ClientURL())
			defer nc.Close()

			// Check the replicas of all MQTT streams
			js, err := nc.JetStream()
			if err != nil {
				t.Fatalf("Error getting js: %v", err)
			}
			for _, sname := range []string{
				mqttStreamName,
				mqttRetainedMsgsStreamName,
				mqttSessStreamName,
			} {
				t.Run(sname, func(t *testing.T) {
					si, err := js.StreamInfo(sname)
					if err != nil {
						t.Fatalf("Error getting stream info: %v", err)
					}
					if si.Config.Replicas != test.replicas {
						t.Fatalf("Expected %v replicas, got %v", test.replicas, si.Config.Replicas)
					}
				})
			}
		})
	}
}

func TestMQTTClusterCanCreateSessionWithOnServerDown(t *testing.T) {
	cl := createJetStreamClusterWithTemplate(t, testMQTTGetClusterTemplaceNoLeaf(), "MQTT", 3)
	defer cl.shutdown()
	o := cl.opts[0]

	mc, rc := testMQTTConnectRetry(t, &mqttConnInfo{cleanSess: true}, o.MQTT.Host, o.MQTT.Port, 5)
	defer mc.Close()
	testMQTTCheckConnAck(t, rc, mqttConnAckRCConnectionAccepted, false)
	mc.Close()

	// Shutdown one of the server.
	sd := cl.servers[1].StoreDir()
	defer os.RemoveAll(strings.TrimSuffix(sd, JetStreamStoreDir))
	cl.servers[1].Shutdown()

	// Make sure there is a meta leader
	cl.waitOnPeerCount(2)
	cl.waitOnLeader()

	// Now try to create a new session. Since we use a single stream now for all sessions,
	// this should succeed.
	o = cl.opts[2]
	// We may still get failures because of some JS APIs may timeout while things
	// settle, so try again for a certain amount of times.
	mc, rc = testMQTTConnectRetry(t, &mqttConnInfo{cleanSess: true}, o.MQTT.Host, o.MQTT.Port, 5)
	defer mc.Close()
	testMQTTCheckConnAck(t, rc, mqttConnAckRCConnectionAccepted, false)
}

func TestMQTTClusterPlacement(t *testing.T) {
	sc := createJetStreamSuperCluster(t, 3, 2)
	defer sc.shutdown()

	c := sc.randomCluster()
	lnc := c.createLeafNodesWithTemplateAndStartPort(jsClusterTemplWithLeafAndMQTT, "SPOKE", 3, 22111)
	defer lnc.shutdown()

	sc.waitOnPeerCount(9)
	sc.waitOnLeader()

	for i := 0; i < 10; i++ {
		mc, rc := testMQTTConnectRetry(t, &mqttConnInfo{cleanSess: true}, lnc.opts[i%3].MQTT.Host, lnc.opts[i%3].MQTT.Port, 5)
		defer mc.Close()
		testMQTTCheckConnAck(t, rc, mqttConnAckRCConnectionAccepted, false)
	}

	// Now check that MQTT assets have been created in the LEAF node's side, not the Hub.
	nc := natsConnect(t, lnc.servers[0].ClientURL())
	defer nc.Close()
	js, err := nc.JetStream()
	if err != nil {
		t.Fatalf("Unable to get JetStream: %v", err)
	}
	count := 0
	for si := range js.StreamsInfo() {
		if si.Cluster == nil || si.Cluster.Name != "SPOKE" {
			t.Fatalf("Expected asset %q to be placed on spoke cluster, was placed on %+v", si.Config.Name, si.Cluster)
		}
		for _, repl := range si.Cluster.Replicas {
			if !strings.HasPrefix(repl.Name, "SPOKE-") {
				t.Fatalf("Replica on the wrong cluster: %+v", repl)
			}
		}
		if si.State.Consumers > 0 {
			for ci := range js.ConsumersInfo(si.Config.Name) {
				if ci.Cluster == nil || ci.Cluster.Name != "SPOKE" {
					t.Fatalf("Expected asset %q to be placed on spoke cluster, was placed on %+v", ci.Name, si.Cluster)
				}
				for _, repl := range ci.Cluster.Replicas {
					if !strings.HasPrefix(repl.Name, "SPOKE-") {
						t.Fatalf("Replica on the wrong cluster: %+v", repl)
					}
				}
			}
		}
		count++
	}
	if count == 0 {
		t.Fatal("No stream found!")
	}
}

func TestMQTTLeafnodeWithoutJSToClusterWithJSNoSharedSysAcc(t *testing.T) {
	test := func(t *testing.T, resolution int) {
		getClusterOpts := func(name string, i int) *Options {
			o := testMQTTDefaultOptions()
			o.ServerName = name
			o.Cluster.Name = "hub"
			// first two test cases rely on domain not being set in hub
			if resolution > 1 {
				o.JetStreamDomain = "DOMAIN"
			}
			o.Cluster.Host = "127.0.0.1"
			o.Cluster.Port = 2790 + i
			o.Routes = RoutesFromStr("nats://127.0.0.1:2791,nats://127.0.0.1:2792,nats://127.0.0.1:2793")
			o.LeafNode.Host = "127.0.0.1"
			o.LeafNode.Port = -1
			return o
		}
		o1 := getClusterOpts("S1", 1)
		s1 := testMQTTRunServer(t, o1)
		defer testMQTTShutdownServer(s1)

		o2 := getClusterOpts("S2", 2)
		s2 := testMQTTRunServer(t, o2)
		defer testMQTTShutdownServer(s2)

		o3 := getClusterOpts("S3", 3)
		s3 := testMQTTRunServer(t, o3)
		defer testMQTTShutdownServer(s3)

		cluster := []*Server{s1, s2, s3}
		checkClusterFormed(t, cluster...)
		checkFor(t, 10*time.Second, 50*time.Millisecond, func() error {
			for _, s := range cluster {
				if s.JetStreamIsLeader() {
					// Need to wait for usage updates now to propagate to meta leader.
					if len(s.JetStreamClusterPeers()) == len(cluster) {
						time.Sleep(100 * time.Millisecond)
						return nil
					}
				}
			}
			return fmt.Errorf("no leader yet")
		})

		// Now define a leafnode that has mqtt enabled, but no JS. This should still work.
		lno := testMQTTDefaultOptions()
		// Make sure jetstream is not explicitly defined here.
		lno.JetStream = false
		switch resolution {
		case 0:
			// turn off jetstream in $G by adding another account and set mqtt domain option and set account default
			lno.Accounts = append(lno.Accounts, NewAccount("unused-account"))
			fallthrough
		case 1:
			lno.JsAccDefaultDomain = map[string]string{"$G": ""}
		case 2:
			lno.JsAccDefaultDomain = map[string]string{"$G": o1.JetStreamDomain}
		case 3:
			// turn off jetstream in $G by adding another account and set mqtt domain option
			lno.Accounts = append(lno.Accounts, NewAccount("unused-account"))
			fallthrough
		case 4:
			// actual solution
			lno.MQTT.JsDomain = o1.JetStreamDomain
		case 5:
			// set per account overwrite and disable js in $G
			lno.Accounts = append(lno.Accounts, NewAccount("unused-account"))
			lno.JsAccDefaultDomain = map[string]string{
				"$G": o1.JetStreamDomain,
			}
		}
		// Whenever an account was added to disable JS in $G, enable it in the server
		if len(lno.Accounts) > 0 {
			lno.JetStream = true
			lno.JetStreamDomain = "OTHER"
			lno.StoreDir = t.TempDir()
		}

		// Use RoutesFromStr() to make an array of urls
		urls := RoutesFromStr(fmt.Sprintf("nats://127.0.0.1:%d,nats://127.0.0.1:%d,nats://127.0.0.1:%d",
			o1.LeafNode.Port, o2.LeafNode.Port, o3.LeafNode.Port))
		lno.LeafNode.Remotes = []*RemoteLeafOpts{{URLs: urls}}

		ln := RunServer(lno)
		defer testMQTTShutdownServer(ln)

		checkLeafNodeConnected(t, ln)

		// Now connect to leafnode and subscribe
		mc, rc := testMQTTConnectRetry(t, &mqttConnInfo{clientID: "sub", cleanSess: true}, lno.MQTT.Host, lno.MQTT.Port, 5)
		defer mc.Close()
		testMQTTCheckConnAck(t, rc, mqttConnAckRCConnectionAccepted, false)
		testMQTTSub(t, 1, mc, rc, []*mqttFilter{{filter: "foo", qos: 1}}, []byte{1})
		testMQTTFlush(t, mc, nil, rc)

		connectAndPublish := func(o *Options) {
			mp, rp := testMQTTConnectRetry(t, &mqttConnInfo{cleanSess: true}, o.MQTT.Host, o.MQTT.Port, 5)
			defer mp.Close()
			testMQTTCheckConnAck(t, rp, mqttConnAckRCConnectionAccepted, false)

			testMQTTPublish(t, mp, rp, 1, false, false, "foo", 1, []byte("msg"))
		}
		// Connect a publisher from leafnode and publish, verify message is received.
		connectAndPublish(lno)
		testMQTTCheckPubMsg(t, mc, rc, "foo", mqttPubQos1, []byte("msg"))

		// Connect from one server in the cluster check it works from there too.
		connectAndPublish(o3)
		testMQTTCheckPubMsg(t, mc, rc, "foo", mqttPubQos1, []byte("msg"))

		// Connect from a server in the hub and subscribe
		mc2, rc2 := testMQTTConnectRetry(t, &mqttConnInfo{clientID: "sub2", cleanSess: true}, o2.MQTT.Host, o2.MQTT.Port, 5)
		defer mc2.Close()
		testMQTTCheckConnAck(t, rc2, mqttConnAckRCConnectionAccepted, false)
		testMQTTSub(t, 1, mc2, rc2, []*mqttFilter{{filter: "foo", qos: 1}}, []byte{1})
		testMQTTFlush(t, mc2, nil, rc2)

		// Connect a publisher from leafnode and publish, verify message is received.
		connectAndPublish(lno)
		testMQTTCheckPubMsg(t, mc2, rc2, "foo", mqttPubQos1, []byte("msg"))

		// Connect from one server in the cluster check it works from there too.
		connectAndPublish(o1)
		testMQTTCheckPubMsg(t, mc2, rc2, "foo", mqttPubQos1, []byte("msg"))
	}
	t.Run("backwards-compatibility-default-js-enabled-in-leaf", func(t *testing.T) {
		test(t, 0) // test with JsAccDefaultDomain set to default (pointing at hub) but jetstream enabled in leaf node too
	})
	t.Run("backwards-compatibility-default-js-disabled-in-leaf", func(t *testing.T) {
		// test with JsAccDefaultDomain set. Checks if it works with backwards compatibility code for empty domain
		test(t, 1)
	})
	t.Run("backwards-compatibility-domain-js-disabled-in-leaf", func(t *testing.T) {
		// test with JsAccDefaultDomain set. Checks if it works with backwards compatibility code for domain set
		test(t, 2) // test with domain set in mqtt client
	})
	t.Run("mqtt-explicit-js-enabled-in-leaf", func(t *testing.T) {
		test(t, 3) // test with domain set in mqtt client (pointing at hub) but jetstream enabled in leaf node too
	})
	t.Run("mqtt-explicit-js-disabled-in-leaf", func(t *testing.T) {
		test(t, 4) // test with domain set in mqtt client
	})
	t.Run("backwards-compatibility-domain-js-enabled-in-leaf", func(t *testing.T) {
		test(t, 5) // test with JsAccDefaultDomain set to domain (pointing at hub) but jetstream enabled in leaf node too
	})
}

func TestMQTTImportExport(t *testing.T) {
	conf := createConfFile(t, []byte(fmt.Sprintf(`
		listen: "127.0.0.1:-1"
		server_name: "mqtt"
		jetstream {
			store_dir = %q
		}
		accounts {
			org {
				jetstream: enabled
				users: [{user: org, password: pwd}]
				imports = [{stream: {account: "device", subject: "foo"}, prefix: "org"}]
			}
			device {
				users: [{user: device, password: pwd}]
				exports = [{stream: "foo"}]
			}
		}
		mqtt {
			listen: "127.0.0.1:-1"
		}
		no_auth_user: device
	`, t.TempDir())))
	defer os.Remove(conf)

	s, o := RunServerWithConfig(conf)
	defer s.Shutdown()

	mc1, rc1 := testMQTTConnect(t, &mqttConnInfo{clientID: "sub1", user: "org", pass: "pwd", cleanSess: true}, o.MQTT.Host, o.MQTT.Port)
	defer mc1.Close()
	testMQTTCheckConnAck(t, rc1, mqttConnAckRCConnectionAccepted, false)
	testMQTTSub(t, 1, mc1, rc1, []*mqttFilter{{filter: "org/foo", qos: 0}}, []byte{0})
	testMQTTFlush(t, mc1, nil, rc1)

	mc2, rc2 := testMQTTConnect(t, &mqttConnInfo{clientID: "sub2", user: "org", pass: "pwd", cleanSess: true}, o.MQTT.Host, o.MQTT.Port)
	defer mc2.Close()
	testMQTTCheckConnAck(t, rc2, mqttConnAckRCConnectionAccepted, false)
	testMQTTSub(t, 1, mc2, rc2, []*mqttFilter{{filter: "org/foo", qos: 1}}, []byte{1})
	testMQTTFlush(t, mc2, nil, rc2)

	nc := natsConnect(t, s.ClientURL())
	defer nc.Close()
	natsPub(t, nc, "foo", []byte("msg"))

	// Verify message is received on receiver side.
	testMQTTCheckPubMsg(t, mc1, rc1, "org/foo", 0, []byte("msg"))
	testMQTTCheckPubMsg(t, mc2, rc2, "org/foo", 0, []byte("msg"))
}

func TestMQTTSessionMovingDomains(t *testing.T) {
	tmpl := strings.Replace(jsClusterTemplWithLeafAndMQTT, "{{leaf}}", `leafnodes { listen: 127.0.0.1:-1 }`, 1)
	tmpl = strings.Replace(tmpl, "store_dir:", "domain: HUB, store_dir:", 1)
	c := createJetStreamCluster(t, tmpl, "HUB", _EMPTY_, 3, 22020, true)
	defer c.shutdown()
	c.waitOnLeader()

	tmpl = strings.Replace(jsClusterTemplWithLeafAndMQTT, "store_dir:", "domain: SPOKE, store_dir:", 1)
	lnc := c.createLeafNodesWithTemplateAndStartPort(tmpl, "SPOKE", 3, 22111)
	defer lnc.shutdown()
	lnc.waitOnPeerCount(3)

	connectSubAndDisconnect := func(host string, port int, present bool) {
		t.Helper()
		mc, rc := testMQTTConnectRetry(t, &mqttConnInfo{clientID: "sub", cleanSess: false}, host, port, 5)
		defer mc.Close()
		testMQTTCheckConnAck(t, rc, mqttConnAckRCConnectionAccepted, present)
		testMQTTSub(t, 1, mc, rc, []*mqttFilter{{filter: "foo", qos: 1}}, []byte{1})
		testMQTTFlush(t, mc, nil, rc)
		testMQTTDisconnect(t, mc, nil)
	}

	// Create a session on the HUB. Make sure we don't use "clean" session so that
	// it is not removed when the client connection closes.
	for i := 0; i < 7; i++ {
		var present bool
		if i > 0 {
			present = true
		}
		connectSubAndDisconnect(c.opts[0].MQTT.Host, c.opts[0].MQTT.Port, present)
	}

	// Now move to the SPOKE cluster, this is a brand new session there, so should not be present.
	connectSubAndDisconnect(lnc.opts[1].MQTT.Host, lnc.opts[1].MQTT.Port, false)

	// Move back to HUB cluster. Make it interesting by connecting to a different
	// server in that cluster. This should work, and present flag should be true.
	connectSubAndDisconnect(c.opts[2].MQTT.Host, c.opts[2].MQTT.Port, true)
}

type remoteConnSameClientIDLogger struct {
	DummyLogger
	warn chan string
}

func (l *remoteConnSameClientIDLogger) Warnf(format string, args ...any) {
	msg := fmt.Sprintf(format, args...)
	if strings.Contains(msg, "remote connection has started with the same client ID") {
		l.warn <- msg
	}
}

func TestMQTTSessionsDifferentDomains(t *testing.T) {
	tmpl := strings.Replace(jsClusterTemplWithLeafAndMQTT, "{{leaf}}", `leafnodes { listen: 127.0.0.1:-1 }`, 1)
	c := createJetStreamCluster(t, tmpl, "HUB", _EMPTY_, 3, 22020, true)
	defer c.shutdown()
	c.waitOnLeader()

	tmpl = strings.Replace(jsClusterTemplWithLeafAndMQTT, "store_dir:", "domain: LEAF1, store_dir:", 1)
	lnc1 := c.createLeafNodesWithTemplateAndStartPort(tmpl, "LEAF1", 2, 22111)
	defer lnc1.shutdown()
	lnc1.waitOnPeerCount(2)

	l := &remoteConnSameClientIDLogger{warn: make(chan string, 10)}
	lnc1.servers[0].SetLogger(l, false, false)

	tmpl = strings.Replace(jsClusterTemplWithLeafAndMQTT, "store_dir:", "domain: LEAF2, store_dir:", 1)
	lnc2 := c.createLeafNodesWithTemplateAndStartPort(tmpl, "LEAF2", 2, 23111)
	defer lnc2.shutdown()
	lnc2.waitOnPeerCount(2)

	o := &(lnc1.opts[0].MQTT)
	mc1, rc1 := testMQTTConnectRetry(t, &mqttConnInfo{clientID: "sub", cleanSess: false}, o.Host, o.Port, 5)
	defer mc1.Close()
	testMQTTCheckConnAck(t, rc1, mqttConnAckRCConnectionAccepted, false)
	testMQTTSub(t, 1, mc1, rc1, []*mqttFilter{{filter: "foo", qos: 1}}, []byte{1})
	testMQTTFlush(t, mc1, nil, rc1)

	o = &(lnc2.opts[0].MQTT)
	connectSubAndDisconnect := func(host string, port int, present bool) {
		t.Helper()
		mc, rc := testMQTTConnectRetry(t, &mqttConnInfo{clientID: "sub", cleanSess: false}, host, port, 5)
		defer mc.Close()
		testMQTTCheckConnAck(t, rc, mqttConnAckRCConnectionAccepted, present)
		testMQTTSub(t, 1, mc, rc, []*mqttFilter{{filter: "foo", qos: 1}}, []byte{1})
		testMQTTFlush(t, mc, nil, rc)
		testMQTTDisconnect(t, mc, nil)
	}

	for i := 0; i < 2; i++ {
		connectSubAndDisconnect(o.Host, o.Port, i > 0)
	}

	select {
	case w := <-l.warn:
		t.Fatalf("Got a warning: %v", w)
	case <-time.After(500 * time.Millisecond):
		// OK
	}
}

func TestMQTTParseUnsub(t *testing.T) {
	for _, test := range []struct {
		name    string
		proto   []byte
		b       byte
		pl      int
		err     string
		wantErr bool
	}{
		{"reserved flag", nil, 3, 0, "wrong unsubscribe reserved flags", false},
		{"ensure packet loaded", []byte{1, 2}, mqttUnsubscribeFlags, 10, io.ErrUnexpectedEOF.Error(), false},
		{"error reading packet id", []byte{1}, mqttUnsubscribeFlags, 1, "reading packet identifier", false},
		{"packet id cannot be zero", []byte{0, 0}, mqttUnsubscribeFlags, 2, errMQTTPacketIdentifierIsZero.Error(), false},
		{"missing filters", []byte{0, 1}, mqttUnsubscribeFlags, 2, "subscribe protocol must contain at least 1 topic filter", false},
		{"error reading topic", []byte{0, 1, 0, 2, 'a'}, mqttUnsubscribeFlags, 5, "topic filter", false},
		{"empty topic", []byte{0, 1, 0, 0}, mqttUnsubscribeFlags, 4, errMQTTTopicFilterCannotBeEmpty.Error(), false},
		{"invalid utf8 topic", []byte{0, 1, 0, 1, 241}, mqttUnsubscribeFlags, 5, "invalid utf8 for topic filter", false},
		{"topic with null byte", []byte{0, 1, 0, 3, 'a', 0, 'b'}, mqttUnsubscribeFlags, 7, "", true},
	} {
		t.Run(test.name, func(t *testing.T) {
			r := &mqttReader{}
			r.reset(test.proto)
			mqtt := &mqtt{r: r}
			c := &client{mqtt: mqtt}
			_, _, err := c.mqttParseSubsOrUnsubs(r, test.b, test.pl, false)
			if test.wantErr {
				if err == nil {
					t.Fatal("Expected an error, got none")
				}
				return
			}
			if err == nil || !strings.Contains(err.Error(), test.err) {
				t.Fatalf("Expected error %q, got %v", test.err, err)
			}
		})
	}
}

func testMQTTUnsub(t *testing.T, pi uint16, c net.Conn, r *mqttReader, filters []*mqttFilter) {
	t.Helper()
	w := newMQTTWriter(0)
	pkLen := 2 // for pi
	for i := 0; i < len(filters); i++ {
		f := filters[i]
		pkLen += 2 + len(f.filter)
	}
	w.WriteByte(mqttPacketUnsub | mqttUnsubscribeFlags)
	w.WriteVarInt(pkLen)
	w.WriteUint16(pi)
	for i := 0; i < len(filters); i++ {
		f := filters[i]
		w.WriteBytes([]byte(f.filter))
	}
	if _, err := testMQTTWrite(c, w.Bytes()); err != nil {
		t.Fatalf("Error writing UNSUBSCRIBE protocol: %v", err)
	}
	b, _ := testMQTTReadPacket(t, r)
	if pt := b & mqttPacketMask; pt != mqttPacketUnsubAck {
		t.Fatalf("Expected UNSUBACK packet %x, got %x", mqttPacketUnsubAck, pt)
	}
	rpi, err := r.readUint16("packet identifier")
	if err != nil || rpi != pi {
		t.Fatalf("Error with packet identifier expected=%v got: %v err=%v", pi, rpi, err)
	}
}

func TestMQTTUnsub(t *testing.T) {
	o := testMQTTDefaultOptions()
	s := testMQTTRunServer(t, o)
	defer testMQTTShutdownServer(s)

	mcp, mpr := testMQTTConnect(t, &mqttConnInfo{cleanSess: true}, o.MQTT.Host, o.MQTT.Port)
	defer mcp.Close()
	testMQTTCheckConnAck(t, mpr, mqttConnAckRCConnectionAccepted, false)

	mc, r := testMQTTConnect(t, &mqttConnInfo{user: "sub", cleanSess: true}, o.MQTT.Host, o.MQTT.Port)
	defer mc.Close()
	testMQTTCheckConnAck(t, r, mqttConnAckRCConnectionAccepted, false)

	testMQTTSub(t, 1, mc, r, []*mqttFilter{{filter: "foo", qos: 0}}, []byte{0})
	testMQTTFlush(t, mc, nil, r)

	// Publish and test msg received
	testMQTTPublish(t, mcp, r, 0, false, false, "foo", 0, []byte("msg"))
	testMQTTCheckPubMsg(t, mc, r, "foo", 0, []byte("msg"))

	// Unsubscribe
	testMQTTUnsub(t, 1, mc, r, []*mqttFilter{{filter: "foo"}})

	// Publish and test msg not received
	testMQTTPublish(t, mcp, r, 0, false, false, "foo", 0, []byte("msg"))
	testMQTTExpectNothing(t, r)

	// Use of wildcards subs
	filters := []*mqttFilter{
		{filter: "foo/bar", qos: 0},
		{filter: "foo/#", qos: 0},
	}
	testMQTTSub(t, 1, mc, r, filters, []byte{0, 0})
	testMQTTFlush(t, mc, nil, r)

	// Publish and check that message received twice
	testMQTTPublish(t, mcp, r, 0, false, false, "foo/bar", 0, []byte("msg"))
	testMQTTCheckPubMsg(t, mc, r, "foo/bar", 0, []byte("msg"))
	testMQTTCheckPubMsg(t, mc, r, "foo/bar", 0, []byte("msg"))

	// Unsub the wildcard one
	testMQTTUnsub(t, 1, mc, r, []*mqttFilter{{filter: "foo/#"}})
	// Publish and check that message received once
	testMQTTPublish(t, mcp, r, 0, false, false, "foo/bar", 0, []byte("msg"))
	testMQTTCheckPubMsg(t, mc, r, "foo/bar", 0, []byte("msg"))
	testMQTTExpectNothing(t, r)

	// Unsub last
	testMQTTUnsub(t, 1, mc, r, []*mqttFilter{{filter: "foo/bar"}})
	// Publish and test msg not received
	testMQTTPublish(t, mcp, r, 0, false, false, "foo/bar", 0, []byte("msg"))
	testMQTTExpectNothing(t, r)
}

func testMQTTExpectDisconnect(t testing.TB, c net.Conn) {
	t.Helper()
	buf, err := testMQTTRead(c)
	if err == nil {
		t.Fatalf("Expected connection to be disconnected, got %s", buf)
	}
	// Distinguish real disconnection (EOF, connection reset) from timeout
	if ne, ok := err.(net.Error); ok && ne.Timeout() {
		t.Fatal("Expected a disconnect but got a timeout error")
	}
}

func TestMQTTPublishTopicErrors(t *testing.T) {
	o := testMQTTDefaultOptions()
	s := testMQTTRunServer(t, o)
	defer testMQTTShutdownServer(s)

	for _, test := range []struct {
		name  string
		topic string
	}{
		{"empty", ""},
		{"with single level wildcard", "foo/+"},
		{"with multiple level wildcard", "foo/#"},
		{"with tab", "foo\tbar"},
		{"with line feed", "foo\nbar"},
		{"with carriage return", "foo\rbar"},
		{"with form feed", "foo\fbar"},
	} {
		t.Run(test.name, func(t *testing.T) {
			mc, r := testMQTTConnect(t, &mqttConnInfo{cleanSess: true}, o.MQTT.Host, o.MQTT.Port)
			defer mc.Close()
			testMQTTCheckConnAck(t, r, mqttConnAckRCConnectionAccepted, false)

			testMQTTPublish(t, mc, r, 0, false, false, test.topic, 0, []byte("msg"))
			testMQTTExpectDisconnect(t, mc)
		})
	}
}

func testMQTTDisconnect(t testing.TB, c net.Conn, bw *bufio.Writer) {
	t.Helper()
	testMQTTDisconnectEx(t, c, bw, true)
}

func testMQTTDisconnectEx(t testing.TB, c net.Conn, bw *bufio.Writer, wait bool) {
	t.Helper()
	w := newMQTTWriter(0)
	w.WriteByte(mqttPacketDisconnect)
	w.WriteByte(0)
	if bw != nil {
		bw.Write(w.Bytes())
		bw.Flush()
	} else {
		c.Write(w.Bytes())
	}
	if wait {
		testMQTTExpectDisconnect(t, c)
	}
}

func TestMQTTWill(t *testing.T) {
	o := testMQTTDefaultOptions()
	s := testMQTTRunServer(t, o)
	defer testMQTTShutdownServer(s)

	nc := natsConnect(t, s.ClientURL())
	defer nc.Close()

	sub := natsSubSync(t, nc, "will.topic")
	natsFlush(t, nc)

	willMsg := []byte("bye")

	for _, test := range []struct {
		name         string
		willExpected bool
		willQoS      byte
	}{
		{"will qos 0", true, 0},
		{"will qos 1", true, 1},
		{"will qos 2", true, 2},
		{"proper disconnect no will", false, 0},
	} {
		t.Run(test.name, func(t *testing.T) {
			mcs, rs := testMQTTConnect(t, &mqttConnInfo{cleanSess: true}, o.MQTT.Host, o.MQTT.Port)
			defer mcs.Close()
			testMQTTCheckConnAck(t, rs, mqttConnAckRCConnectionAccepted, false)

			testMQTTSub(t, 1, mcs, rs, []*mqttFilter{{filter: "will/#", qos: 2}}, []byte{2})
			testMQTTFlush(t, mcs, nil, rs)

			mc, r := testMQTTConnect(t,
				&mqttConnInfo{
					cleanSess: true,
					will: &mqttWill{
						topic:   []byte("will/topic"),
						message: willMsg,
						qos:     test.willQoS,
					},
				}, o.MQTT.Host, o.MQTT.Port)
			defer mc.Close()
			testMQTTCheckConnAck(t, r, mqttConnAckRCConnectionAccepted, false)

			if test.willExpected {
				mc.Close()
				testMQTTCheckPubMsg(t, mcs, rs, "will/topic", test.willQoS<<1, willMsg)
				wm := natsNexMsg(t, sub, time.Second)
				if !bytes.Equal(wm.Data, willMsg) {
					t.Fatalf("Expected will message to be %q, got %q", willMsg, wm.Data)
				}
			} else {
				testMQTTDisconnect(t, mc, nil)
				testMQTTExpectNothing(t, rs)
				if wm, err := sub.NextMsg(100 * time.Millisecond); err == nil {
					t.Fatalf("Should not have receive a message, got subj=%q data=%q",
						wm.Subject, wm.Data)
				}
			}
		})
	}
}

func TestMQTTWillOnShutdown(t *testing.T) {
	for _, test := range []struct {
		name      string
		cleanSess bool
		qos       byte
		retain    bool
		ws        bool
	}{
		{"clean_session", true, 1, false, false},
		{"persistent_session", false, 1, false, false},
		{"qos2", true, 2, false, false},
		{"retained_qos0", true, 0, true, false},
		{"retained_qos1", true, 1, true, false},
		{"websocket", true, 1, false, true},
	} {
		t.Run(test.name, func(t *testing.T) {
			for _, mode := range []string{"shutdown", "lame_duck", "connection_closed", "disconnect_shutdown", "disconnect_lame_duck"} {
				t.Run(mode, func(t *testing.T) {
					o := testMQTTDefaultOptions()
					o.LameDuckDuration = 10 * time.Millisecond
					o.LameDuckGracePeriod = -time.Millisecond
					if test.ws {
						o.Websocket.Host = "127.0.0.1"
						o.Websocket.Port = -1
						o.Websocket.NoTLS = true
					}
					s := testMQTTRunServer(t, o)
					defer func() { s.Shutdown() }()

					// Leave a durable subscriber offline to check that the Will
					// is stored before JetStream shuts down.
					observer := &mqttConnInfo{clientID: "observer", cleanSess: false}
					mc, r := testMQTTConnect(t, observer, o.MQTT.Host, o.MQTT.Port)
					testMQTTCheckConnAck(t, r, mqttConnAckRCConnectionAccepted, false)
					testMQTTSub(t, 1, mc, r, []*mqttFilter{{filter: "will/topic", qos: 1}}, []byte{1})
					testMQTTDisconnect(t, mc, nil)
					mc.Close()

					host, port := o.MQTT.Host, o.MQTT.Port
					if test.ws {
						host, port = o.Websocket.Host, o.Websocket.Port
					}
					willMsg := []byte("bye")
					writer, wr := testMQTTConnect(t, &mqttConnInfo{
						clientID:  "writer",
						cleanSess: test.cleanSess,
						ws:        test.ws,
						will: &mqttWill{
							topic:   []byte("will/topic"),
							message: willMsg,
							qos:     test.qos,
							retain:  test.retain,
						},
					}, host, port)
					defer writer.Close()
					testMQTTCheckConnAck(t, wr, mqttConnAckRCConnectionAccepted, false)

					disconnect := strings.HasPrefix(mode, "disconnect_")
					if disconnect {
						testMQTTDisconnectEx(t, writer, nil, false)
						if test.ws {
							// WebSocket disconnects send a CLOSE frame before EOF.
							wc := writer.(*mqttWrapAsWs)
							wc.SetReadDeadline(time.Now().Add(testMQTTTimeout))
							frame, err := wc.br.ReadByte()
							require_NoError(t, err)
							require_Equal(t, frame, byte(wsFinalBit|wsCloseMessage))
							wc.Close()
						} else {
							testMQTTExpectDisconnect(t, writer)
						}
					}
					switch strings.TrimPrefix(mode, "disconnect_") {
					case "shutdown":
						s.Shutdown()
					case "lame_duck":
						s.LameDuckShutdown()
					case "connection_closed":
						// Wait for publication before reconnecting the observer,
						// otherwise a QoS 0 Will could arrive while it is online.
						nc := natsConnect(t, s.ClientURL())
						defer nc.Close()
						sub := natsSubSync(t, nc, "will.topic")
						natsFlush(t, nc)
						writer.Close()
						msg := natsNexMsg(t, sub, time.Second)
						require_True(t, bytes.Equal(msg.Data, willMsg))
					}

					if mode != "connection_closed" {
						o.Port, o.MQTT.Port = -1, -1
						if test.ws {
							o.Websocket.Port = -1
						}
						s = testMQTTRunServer(t, o)
					}
					if mode != "connection_closed" && !disconnect && test.qos > 0 {
						nc, js := jsClientConnect(t, s)
						info, err := js.StreamInfo(mqttStreamName)
						nc.Close()
						require_NoError(t, err)
						require_Equal(t, info.State.Msgs, uint64(1))
					}
					mc, r = testMQTTConnect(t, observer, o.MQTT.Host, o.MQTT.Port)
					defer mc.Close()
					testMQTTCheckConnAck(t, r, mqttConnAckRCConnectionAccepted, true)
					if !disconnect && test.qos > 0 {
						testMQTTCheckPubMsg(t, mc, r, "will/topic", mqttPubQos1, willMsg)
						testMQTTFlush(t, mc, nil, r)
					}
					testMQTTExpectNothing(t, r)

					if test.retain {
						// A new subscription must also recover the retained Will,
						// including QoS 0 Wills which are not queued offline.
						mc, r := testMQTTConnect(t, &mqttConnInfo{cleanSess: true}, o.MQTT.Host, o.MQTT.Port)
						defer mc.Close()
						testMQTTCheckConnAck(t, r, mqttConnAckRCConnectionAccepted, false)
						testMQTTSub(t, 1, mc, r, []*mqttFilter{{filter: "will/topic", qos: 1}}, []byte{1})
						if !disconnect {
							testMQTTCheckPubMsg(t, mc, r, "will/topic", test.qos<<1|mqttPubFlagRetain, willMsg)
							testMQTTFlush(t, mc, nil, r)
						}
						testMQTTExpectNothing(t, r)
					}
				})
			}
		})
	}
}

func TestMQTTLameDuckGracePeriod(t *testing.T) {
	o := testMQTTDefaultOptions()
	o.LameDuckGracePeriod = -250 * time.Millisecond
	o.LameDuckDuration = 500 * time.Millisecond
	s := testMQTTRunServer(t, o)
	defer s.Shutdown()

	var first net.Conn
	var reader *mqttReader
	for i := 0; i < 8; i++ {
		mc, r := testMQTTConnect(t, &mqttConnInfo{cleanSess: true}, o.MQTT.Host, o.MQTT.Port)
		defer mc.Close()
		testMQTTCheckConnAck(t, r, mqttConnAckRCConnectionAccepted, false)
		if i == 0 {
			first, reader = mc, r
		}
	}
	start := time.Now()
	done := make(chan struct{})
	go func() {
		s.LameDuckShutdown()
		close(done)
	}()
	checkFor(t, time.Second, time.Millisecond, func() error {
		if !s.isLameDuckMode() {
			return fmt.Errorf("not in lame-duck mode")
		}
		return nil
	})
	conn, err := net.DialTimeout("tcp", net.JoinHostPort(o.MQTT.Host, fmt.Sprint(o.MQTT.Port)), time.Second)
	if err == nil {
		conn.Close()
		t.Fatal("MQTT listener is still accepting connections in lame-duck mode")
	}
	// Established MQTT connections must stay usable during the grace period.
	testMQTTFlush(t, first, nil, reader)
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("lame-duck shutdown did not complete")
	}
	// The eight connections should also use the paced drain window.
	if elapsed := time.Since(start); elapsed < 330*time.Millisecond {
		t.Fatalf("MQTT clients were closed too quickly: %v", elapsed)
	}
}

func TestMQTTWillOnLeafShutdown(t *testing.T) {
	ho := testMQTTDefaultOptions()
	ho.Cluster.Name = "hub"
	ho.LeafNode.Host, ho.LeafNode.Port = "127.0.0.1", -1
	hub := testMQTTRunServer(t, ho)
	defer hub.Shutdown()

	o := testMQTTDefaultOptions()
	o.JetStream = false
	o.Cluster.Name = "leaf"
	o.JsAccDefaultDomain = map[string]string{"$G": ""}
	o.LeafNode.Remotes = []*RemoteLeafOpts{{URLs: RoutesFromStr(fmt.Sprintf("nats://127.0.0.1:%d", ho.LeafNode.Port))}}
	s := testMQTTRunServer(t, o)
	defer s.Shutdown()
	checkLeafNodeConnected(t, s)

	observer := &mqttConnInfo{clientID: "observer", cleanSess: false}
	mc, r := testMQTTConnect(t, observer, ho.MQTT.Host, ho.MQTT.Port)
	testMQTTCheckConnAck(t, r, mqttConnAckRCConnectionAccepted, false)
	testMQTTSub(t, 1, mc, r, []*mqttFilter{{filter: "will/topic", qos: 1}}, []byte{1})
	testMQTTDisconnect(t, mc, nil)
	mc.Close()

	const writers = 4
	for i := 0; i < writers; i++ {
		id := fmt.Sprintf("writer-%d", i)
		writer, wr := testMQTTConnect(t, &mqttConnInfo{
			clientID: id, cleanSess: true,
			will: &mqttWill{topic: []byte("will/topic"), message: []byte(id), qos: 1},
		}, o.MQTT.Host, o.MQTT.Port)
		defer writer.Close()
		testMQTTCheckConnAck(t, wr, mqttConnAckRCConnectionAccepted, false)
	}
	s.Shutdown()

	mc, r = testMQTTConnect(t, observer, ho.MQTT.Host, ho.MQTT.Port)
	defer mc.Close()
	testMQTTCheckConnAck(t, r, mqttConnAckRCConnectionAccepted, true)
	seen := make(map[string]bool)
	for i := 0; i < writers; i++ {
		flags, pi, _, payload := testMQTTGetPubMsgEx(t, mc, r, "will/topic", nil)
		require_Equal(t, flags, mqttPubQos1)
		require_False(t, seen[string(payload)])
		seen[string(payload)] = true
		testMQTTSendPIPacket(mqttPacketPubAck, t, mc, pi)
	}
	for i := 0; i < writers; i++ {
		require_True(t, seen[fmt.Sprintf("writer-%d", i)])
	}
	testMQTTFlush(t, mc, nil, r)
	testMQTTExpectNothing(t, r)
}

func TestMQTTShutdownReadLoopWait(t *testing.T) {
	for _, enabled := range []bool{false, true} {
		t.Run(fmt.Sprintf("enabled=%v", enabled), func(t *testing.T) {
			o := testMQTTDefaultOptions()
			o.MQTT.JSAPITimeout = 20 * time.Millisecond
			if !enabled {
				o.MQTT.Port = 0
			}
			s := testMQTTRunServer(t, o)
			defer s.Shutdown()
			l := &captureWarnLogger{warn: make(chan string, 10)}
			s.SetLogger(l, false, false)
			s.mqtt.readLoopWG.Add(1)
			var release sync.Once
			defer release.Do(s.mqtt.readLoopWG.Done)
			done := make(chan struct{})
			go func() {
				s.Shutdown()
				close(done)
			}()
			select {
			case <-done:
			case <-time.After(time.Second):
				release.Do(s.mqtt.readLoopWG.Done)
				<-done
				t.Fatal("MQTT read loops prevented shutdown from completing")
			}
			select {
			case warning := <-l.warn:
				require_True(t, enabled)
				require_Equal(t, warning, "Timed out waiting for MQTT clients to close")
			default:
				require_False(t, enabled)
			}
		})
	}
}

func TestMQTTShutdownPendingConnect(t *testing.T) {
	for _, tlsHandshake := range []bool{false, true} {
		t.Run(fmt.Sprintf("tls=%v", tlsHandshake), func(t *testing.T) {
			o := testMQTTDefaultOptions()
			if tlsHandshake {
				o = testMQTTDefaultTLSOptions(t, false)
				o.MQTT.TLSTimeout = 30
			}
			s := testMQTTRunServer(t, o)
			defer s.Shutdown()
			mc, err := net.Dial("tcp", net.JoinHostPort(o.MQTT.Host, fmt.Sprint(o.MQTT.Port)))
			require_NoError(t, err)
			defer mc.Close()
			checkClientsCount(t, s, 1)

			// Neither a missing CONNECT nor an unfinished TLS handshake may
			// prevent shutdown from waiting for the MQTT read loops to exit.
			done := make(chan struct{})
			go func() {
				s.Shutdown()
				close(done)
			}()
			select {
			case <-done:
			case <-time.After(3 * time.Second):
				t.Fatal("shutdown is waiting on an incomplete connection")
			}
		})
	}
}

func TestMQTTQoS2WillReject(t *testing.T) {
	o := testMQTTDefaultOptions()
	o.MQTT.rejectQoS2Pub = true
	s := testMQTTRunServer(t, o)
	defer testMQTTShutdownServer(s)

	mcs, rs := testMQTTConnect(t, &mqttConnInfo{cleanSess: true}, o.MQTT.Host, o.MQTT.Port)
	defer mcs.Close()
	testMQTTCheckConnAck(t, rs, mqttConnAckRCConnectionAccepted, false)

	testMQTTSub(t, 1, mcs, rs, []*mqttFilter{{filter: "will/#", qos: 2}}, []byte{2})
	testMQTTFlush(t, mcs, nil, rs)

	mc, r := testMQTTConnect(t,
		&mqttConnInfo{
			cleanSess: true,
			will: &mqttWill{
				topic:   []byte("will/topic"),
				message: []byte("bye"),
				qos:     2,
			},
		}, o.MQTT.Host, o.MQTT.Port)
	defer mc.Close()
	testMQTTCheckConnAck(t, r, mqttConnAckRCQoS2WillRejected, false)
}

func TestMQTTWillRetain(t *testing.T) {
	for _, test := range []struct {
		name   string
		pubQoS byte
		subQoS byte
	}{
		{"pub QoS0 sub QoS0", 0, 0},
		{"pub QoS0 sub QoS1", 0, 1},
		{"pub QoS1 sub QoS0", 1, 0},
		{"pub QoS1 sub QoS1", 1, 1},
	} {
		t.Run(test.name, func(t *testing.T) {
			o := testMQTTDefaultOptions()
			s := testMQTTRunServer(t, o)
			defer testMQTTShutdownServer(s)

			mces, res := testMQTTConnect(t, &mqttConnInfo{cleanSess: true}, o.MQTT.Host, o.MQTT.Port)
			defer mces.Close()
			testMQTTCheckConnAck(t, res, mqttConnAckRCConnectionAccepted, false)
			testMQTTSub(t, 1, mces, res, []*mqttFilter{{filter: "will/#", qos: test.subQoS}}, []byte{test.subQoS})

			willTopic := []byte("will/topic")
			willMsg := []byte("bye")

			mc, r := testMQTTConnect(t,
				&mqttConnInfo{
					cleanSess: true,
					will: &mqttWill{
						topic:   willTopic,
						message: willMsg,
						qos:     test.pubQoS,
						retain:  true,
					},
				}, o.MQTT.Host, o.MQTT.Port)
			defer mc.Close()
			testMQTTCheckConnAck(t, r, mqttConnAckRCConnectionAccepted, false)

			// Disconnect the client
			mc.Close()

			// Wait for the server to process the connection close, which will
			// cause the "will" message to be published (and retained).
			checkClientsCount(t, s, 1)

			// Create subscription on will topic and expect will message.
			mcs, rs := testMQTTConnect(t, &mqttConnInfo{cleanSess: true}, o.MQTT.Host, o.MQTT.Port)
			defer mcs.Close()
			testMQTTCheckConnAck(t, rs, mqttConnAckRCConnectionAccepted, false)

			testMQTTSub(t, 1, mcs, rs, []*mqttFilter{{filter: "will/#", qos: test.subQoS}}, []byte{test.subQoS})
			pflags, _ := testMQTTGetPubMsg(t, mcs, rs, "will/topic", willMsg)
			if !mqttIsRetained(pflags) {
				t.Fatalf("expected retain flag to be set, it was not: %v", pflags)
			}
			// Expected QoS will be the lesser of the pub/sub QoS.
			expectedQoS := test.pubQoS
			if test.subQoS == 0 {
				expectedQoS = 0
			}
			if qos := mqttGetQoS(pflags); qos != expectedQoS {
				t.Fatalf("expected qos to be %v, got %v", expectedQoS, qos)
			}

			// The existing subscription (prior to sending the will) should receive
			// the will but the retain flag should not be set.
			pflags, _ = testMQTTGetPubMsg(t, mces, res, "will/topic", willMsg)
			if mqttIsRetained(pflags) {
				t.Fatalf("expected retain flag to not be set, it was: %v", pflags)
			}
			// Expected QoS will be the lesser of the pub/sub QoS.
			if qos := mqttGetQoS(pflags); qos != expectedQoS {
				t.Fatalf("expected qos to be %v, got %v", expectedQoS, qos)
			}
		})
	}
}

func TestMQTTWillRetainPermViolation(t *testing.T) {
	template := `
		port: -1
		jetstream {
			store_dir = %q
		}
		server_name: mqtt
		authorization {
			mqtt_perms = {
				publish = ["%s"]
				subscribe = ["foo", "bar"]
			}
			users = [
				{user: mqtt, password: pass, permissions: $mqtt_perms}
			]
		}
		mqtt {
			port: -1
		}
	`
	tdir := t.TempDir()
	conf := createConfFile(t, fmt.Appendf(nil, template, tdir, "foo"))

	s, o := RunServerWithConfig(conf)
	defer testMQTTShutdownServer(s)

	ci := &mqttConnInfo{
		cleanSess: true,
		user:      "mqtt",
		pass:      "pass",
	}

	// We create first a connection with the Will topic that the publisher
	// is allowed to publish to.
	ci.will = &mqttWill{
		topic:   []byte("foo"),
		message: []byte("bye"),
		qos:     1,
		retain:  true,
	}
	mc, r := testMQTTConnect(t, ci, o.MQTT.Host, o.MQTT.Port)
	defer mc.Close()
	testMQTTCheckConnAck(t, r, mqttConnAckRCConnectionAccepted, false)

	// Disconnect, which will cause the Will to be sent with retain flag.
	mc.Close()

	// Wait for the server to process the connection close, which will
	// cause the "will" message to be published (and retained).
	checkClientsCount(t, s, 0)

	// Create a subscription on the Will subject and we should receive it.
	ci.will = nil
	mcs, rs := testMQTTConnect(t, ci, o.MQTT.Host, o.MQTT.Port)
	defer mcs.Close()
	testMQTTCheckConnAck(t, rs, mqttConnAckRCConnectionAccepted, false)

	testMQTTSub(t, 1, mcs, rs, []*mqttFilter{{filter: "foo", qos: 1}}, []byte{1})
	pflags, _ := testMQTTGetPubMsg(t, mcs, rs, "foo", []byte("bye"))
	if !mqttIsRetained(pflags) {
		t.Fatalf("expected retain flag to be set, it was not: %v", pflags)
	}
	if qos := mqttGetQoS(pflags); qos != 1 {
		t.Fatalf("expected qos to be 1, got %v", qos)
	}
	testMQTTDisconnect(t, mcs, nil)
	mcs.Close()

	// Now create another connection with a Will that client is not allowed to publish to.
	ci.will = &mqttWill{
		topic:   []byte("bar"),
		message: []byte("bye"),
		qos:     1,
		retain:  true,
	}
	mc, r = testMQTTConnect(t, ci, o.MQTT.Host, o.MQTT.Port)
	defer mc.Close()
	testMQTTCheckConnAck(t, r, mqttConnAckRCConnectionAccepted, false)

	// Disconnect, to cause Will to be produced, but in that case should not be stored
	// since user not allowed to publish on "bar".
	mc.Close()

	// Wait for the server to process the connection close, which will
	// cause the "will" message to be published (and retained).
	checkClientsCount(t, s, 0)

	// Create sub on "bar" which user is allowed to subscribe to.
	ci.will = nil
	mcs, rs = testMQTTConnect(t, ci, o.MQTT.Host, o.MQTT.Port)
	defer mcs.Close()
	testMQTTCheckConnAck(t, rs, mqttConnAckRCConnectionAccepted, false)

	testMQTTSub(t, 1, mcs, rs, []*mqttFilter{{filter: "bar", qos: 1}}, []byte{1})
	// No Will should be published since it should not have been stored in the first place.
	testMQTTExpectNothing(t, rs)
	testMQTTDisconnect(t, mcs, nil)
	mcs.Close()

	// Now remove permission to publish on "foo" and check that a new subscription
	// on "foo" is now not getting the will message because the original user no
	// longer has permission to do so.
	reloadUpdateConfig(t, s, conf, fmt.Sprintf(template, tdir, "baz"))

	mcs, rs = testMQTTConnect(t, ci, o.MQTT.Host, o.MQTT.Port)
	defer mcs.Close()
	testMQTTCheckConnAck(t, rs, mqttConnAckRCConnectionAccepted, false)

	testMQTTSub(t, 1, mcs, rs, []*mqttFilter{{filter: "foo", qos: 1}}, []byte{1})
	testMQTTExpectNothing(t, rs)
	testMQTTDisconnect(t, mcs, nil)
}

func TestMQTTPublishRetain(t *testing.T) {
	o := testMQTTDefaultOptions()
	s := testMQTTRunServer(t, o)
	defer testMQTTShutdownServer(s)

	buf := strings.Builder{}
	for i := 0; i < 128; i++ { // 128Kb
		for j := 0; j < 64; j++ { // 16 * 64 = 1Kb
			buf.Write([]byte("0123456789abcdef"))
		}
	}
	large := buf.String()

	for _, test := range []struct {
		name          string
		retained      bool
		sentValue     string
		expectedValue string
		subGetsIt     bool
	}{
		{"publish large retained", true, large, large, true},
		{"publish retained", true, "retained", "retained", true},
		{"publish not retained", false, "not retained", "retained", true},
		{"remove retained", true, "", "", false},
	} {
		t.Run(test.name, func(t *testing.T) {
			mc1, rs1 := testMQTTConnect(t, &mqttConnInfo{cleanSess: true}, o.MQTT.Host, o.MQTT.Port)
			defer mc1.Close()
			testMQTTCheckConnAck(t, rs1, mqttConnAckRCConnectionAccepted, false)
			testMQTTPublish(t, mc1, rs1, 0, false, test.retained, "foo", 0, []byte(test.sentValue))
			testMQTTFlush(t, mc1, nil, rs1)

			mc2, rs2 := testMQTTConnect(t, &mqttConnInfo{cleanSess: true}, o.MQTT.Host, o.MQTT.Port)
			defer mc2.Close()
			testMQTTCheckConnAck(t, rs2, mqttConnAckRCConnectionAccepted, false)

			testMQTTSub(t, 1, mc2, rs2, []*mqttFilter{{filter: "foo/#", qos: 1}}, []byte{1})

			if test.subGetsIt {
				pflags, _ := testMQTTGetPubMsg(t, mc2, rs2, "foo", []byte(test.expectedValue))
				if !mqttIsRetained(pflags) {
					t.Fatalf("retain flag should have been set, it was not: flags=%v", pflags)
				}
			} else {
				testMQTTExpectNothing(t, rs2)
			}

			testMQTTDisconnect(t, mc1, nil)
			testMQTTDisconnect(t, mc2, nil)
		})
	}
}

func TestMQTTQoS2RetainedReject(t *testing.T) {
	// Start the server with QOS2 enabled, and submit retained messages with QoS
	// 1 and 2.
	o := testMQTTDefaultOptions()
	s := testMQTTRunServer(t, o)
	mc1, rs1 := testMQTTConnect(t, &mqttConnInfo{cleanSess: true}, o.MQTT.Host, o.MQTT.Port)
	testMQTTCheckConnAck(t, rs1, mqttConnAckRCConnectionAccepted, false)
	testMQTTPublish(t, mc1, rs1, 2, false, true, "foo", 1, []byte("qos2 failed"))
	testMQTTPublish(t, mc1, rs1, 1, false, true, "bar", 2, []byte("qos1 retained"))
	testMQTTFlush(t, mc1, nil, rs1)
	testMQTTDisconnect(t, mc1, nil)
	mc1.Close()
	s.Shutdown()

	// Restart the server with QOS2 disabled; we should be using the same
	// JetStream store, so the retained message should still be there.
	o.MQTT.rejectQoS2Pub = true
	s = testMQTTRunServer(t, o)
	defer testMQTTShutdownServer(s)

	mc2, rs2 := testMQTTConnect(t, &mqttConnInfo{cleanSess: true}, o.MQTT.Host, o.MQTT.Port)
	defer mc2.Close()
	testMQTTCheckConnAck(t, rs2, mqttConnAckRCConnectionAccepted, false)

	testMQTTSub(t, 1, mc2, rs2, []*mqttFilter{{filter: "bar/#", qos: 2}}, []byte{2})
	pflags, _ := testMQTTGetPubMsg(t, mc2, rs2, "bar", []byte("qos1 retained"))
	if !mqttIsRetained(pflags) {
		t.Fatalf("retain flag should have been set, it was not: flags=%v", pflags)
	}

	testMQTTSub(t, 1, mc2, rs2, []*mqttFilter{{filter: "foo/#", qos: 2}}, []byte{2})
	testMQTTExpectNothing(t, rs2)
	testMQTTDisconnect(t, mc2, nil)
}

func TestMQTTRetainFlag(t *testing.T) {
	o := testMQTTDefaultOptions()
	s := testMQTTRunServer(t, o)
	defer testMQTTShutdownServer(s)

	mc1, rs1 := testMQTTConnect(t, &mqttConnInfo{cleanSess: true}, o.MQTT.Host, o.MQTT.Port)
	defer mc1.Close()
	testMQTTCheckConnAck(t, rs1, mqttConnAckRCConnectionAccepted, false)
	testMQTTPublish(t, mc1, rs1, 0, false, true, "foo/0", 0, []byte("flag set"))
	testMQTTPublish(t, mc1, rs1, 0, false, true, "foo/1", 0, []byte("flag set"))
	testMQTTFlush(t, mc1, nil, rs1)

	mc2, rs2 := testMQTTConnect(t, &mqttConnInfo{cleanSess: true}, o.MQTT.Host, o.MQTT.Port)
	defer mc2.Close()
	testMQTTCheckConnAck(t, rs2, mqttConnAckRCConnectionAccepted, false)

	testMQTTSub(t, 1, mc2, rs2, []*mqttFilter{{filter: "foo/0", qos: 0}}, []byte{0})
	pflags, _ := testMQTTGetPubMsg(t, mc2, rs2, "foo/0", []byte("flag set"))
	if !mqttIsRetained(pflags) {
		t.Fatalf("retain flag should have been set, it was not: flags=%v", pflags)
	}

	testMQTTSub(t, 1, mc2, rs2, []*mqttFilter{{filter: "foo/1", qos: 1}}, []byte{1})
	pflags, _ = testMQTTGetPubMsg(t, mc2, rs2, "foo/1", []byte("flag set"))
	if !mqttIsRetained(pflags) {
		t.Fatalf("retain flag should have been set, it was not: flags=%v", pflags)
	}

	// For existing subscriptions, RETAIN flag should not be set: [MQTT-3.3.1-9].
	testMQTTPublish(t, mc1, rs1, 0, false, true, "foo/0", 0, []byte("flag not set"))
	testMQTTFlush(t, mc1, nil, rs1)

	pflags, _ = testMQTTGetPubMsg(t, mc2, rs2, "foo/0", []byte("flag not set"))
	if mqttIsRetained(pflags) {
		t.Fatalf("retain flag should not have been set, it was: flags=%v", pflags)
	}

	testMQTTPublish(t, mc1, rs1, 0, false, true, "foo/1", 0, []byte("flag not set"))
	testMQTTFlush(t, mc1, nil, rs1)

	pflags, _ = testMQTTGetPubMsg(t, mc2, rs2, "foo/1", []byte("flag not set"))
	if mqttIsRetained(pflags) {
		t.Fatalf("retain flag should not have been set, it was: flags=%v", pflags)
	}
}

func TestMQTTPublishRetainPermViolation(t *testing.T) {
	o := testMQTTDefaultOptions()
	o.Users = []*User{
		{
			Username: "mqtt1",
			Password: "pass",
			Permissions: &Permissions{
				Publish:   &SubjectPermission{Allow: []string{"foo"}},
				Subscribe: &SubjectPermission{Allow: []string{"bar"}},
			},
		},
		{
			Username: "mqtt2",
			Password: "pass",
			Permissions: &Permissions{
				Publish:   &SubjectPermission{Allow: []string{"foo", "bar"}},
				Subscribe: &SubjectPermission{Allow: []string{">"}},
			},
		},
		{
			Username: "mqtt3",
			Password: "pass",
			Permissions: &Permissions{
				Publish:   &SubjectPermission{Allow: []string{"foo.bar", "baz", "barbaz"}},
				Subscribe: &SubjectPermission{Allow: []string{">"}},
			},
		},
		{
			Username: "mqtt4",
			Password: "pass",
		},
	}
	s := testMQTTRunServer(t, o)
	defer testMQTTShutdownServer(s)

	var asm *mqttAccountSessionManager

	pubRetained := func(user, subject string) {
		t.Helper()
		mc, rs := testMQTTConnect(t, &mqttConnInfo{
			cleanSess: true,
			clientID:  "pub",
			user:      user,
			pass:      "pass",
		}, o.MQTT.Host, o.MQTT.Port)
		defer mc.Close()
		testMQTTCheckConnAck(t, rs, mqttConnAckRCConnectionAccepted, false)
		testMQTTPublish(t, mc, rs, 0, false, true, subject, 0, []byte("retained"))
		testMQTTFlush(t, mc, nil, rs)
		if asm == nil {
			asm = testMQTTGetAccountSessionManager(t, s, "pub")
		}
		testMQTTDisconnect(t, mc, nil)
	}
	consumeRetained := func(user, subject string, expected bool) {
		t.Helper()
		mc, rs := testMQTTConnect(t, &mqttConnInfo{
			cleanSess: true,
			user:      user,
			pass:      "pass",
		}, o.MQTT.Host, o.MQTT.Port)
		defer mc.Close()
		testMQTTCheckConnAck(t, rs, mqttConnAckRCConnectionAccepted, false)
		testMQTTSub(t, 1, mc, rs, []*mqttFilter{{filter: subject, qos: 0}}, []byte{0})
		if expected {
			testMQTTCheckPubMsg(t, mc, rs, subject, mqttPubFlagRetain, []byte("retained"))
		} else {
			testMQTTExpectNothing(t, rs)
		}
		testMQTTDisconnect(t, mc, nil)
	}

	// With user "mqtt", publish a retained message on "bar".
	// Since this user has no permission, the server should not have stored it.
	pubRetained("mqtt1", "bar")

	// Verify that we can't get it with a new subscription.
	consumeRetained("mqtt1", "bar", false)

	// Use the user "mqtt2" that has permissions to publish on foo and bar.
	// Publish on "foo" and check retained message can be received.
	pubRetained("mqtt2", "foo")
	consumeRetained("mqtt2", "foo", true)

	// For user "mqtt3", we will publish on "foo/bar" and check retained
	// message is properly received.
	pubRetained("mqtt3", "foo/bar")
	consumeRetained("mqtt3", "foo/bar", true)

	// Simulate a message that would have been produced in a different server
	// on subject "barbaz". We will use user "mqtt4" that has no pub permissions
	// since we need to send to low-level "$MQTT.rmsgs.barbaz" subject...
	nc := natsConnect(t, s.ClientURL(), nats.UserInfo("mqtt4", "pass"))
	defer nc.Close()
	msg := nats.NewMsg("$MQTT.rmsgs.barbaz")
	msg.Header.Set(mqttNatsRetainedMessageOrigin, "SomeOtherServer")
	msg.Header.Set(mqttNatsRetainedMessageTopic, "barbaz")
	msg.Header.Set(mqttNatsRetainedMessageFlags, "1")
	msg.Data = []byte("retained")
	nc.PublishMsg(msg)
	natsFlush(t, nc)

	// Wait a bit to make sure it is processed.
	time.Sleep(250 * time.Millisecond)

	// Then check that it can be received
	consumeRetained("mqtt3", "barbaz", true)

	// Same with user "mqtt4" that does not have permissions defined, which
	// means allowed to pub/sub on everything.
	pubRetained("mqtt4", "bat")
	consumeRetained("mqtt4", "bat", true)

	// Do a config reload and make sure that the server does not panic
	// and we can still get the retained messages.
	no := *o
	// Remove the "bar" publish permission from "mqtt2"
	no.Users[1].Permissions.Publish = &SubjectPermission{Allow: []string{"foo"}}
	// And the "foo.bar" and "barbaz" publish permissions from "mqtt3"
	no.Users[2].Permissions.Publish = &SubjectPermission{Allow: []string{"baz"}}
	err := s.ReloadOptions(&no)
	require_NoError(t, err)

	checkFor(t, time.Second, 10*time.Millisecond, func() error {
		asm.mu.RLock()
		defer asm.mu.RUnlock()
		if _, ok := asm.retmsgs.Find([]byte("foo.bar")); ok {
			return errors.New("foo.bar subject still in map")
		}
		return nil
	})

	// Still message on "bar" should not exist
	consumeRetained("mqtt1", "bar", false)
	// This one should still be able to be received
	consumeRetained("mqtt2", "foo", true)
	// Retained message on "foo.bar" should have been removed.
	consumeRetained("mqtt3", "foo/bar", false)
	// However, message on "barbaz" should have been left alone since it
	// was produced on a different server.
	consumeRetained("mqtt3", "barbaz", true)
	// And finally, this user that had no permission should still be able
	// to get the retained message on "bat".
	consumeRetained("mqtt4", "bat", true)
}

func TestMQTTPermissionsViolation(t *testing.T) {
	o := testMQTTDefaultOptions()
	o.Users = []*User{
		{
			Username: "mqtt",
			Password: "pass",
			Permissions: &Permissions{
				Publish:   &SubjectPermission{Allow: []string{"foo.bar"}},
				Subscribe: &SubjectPermission{Allow: []string{"foo.*"}, Deny: []string{"foo.baz"}},
			},
		},
		{
			Username: "mqtt2",
			Password: "pass",
			Permissions: &Permissions{
				Publish: &SubjectPermission{Allow: []string{"foo"}},
				// We use to require explicit allow permission on "$MQTT.sub.>" when any
				// subscribe permission were provided. We now implicitly allow it, but
				// we want to make sure that, if needs arise, one can block the server
				// to subscribe on "$MQTT.sub".
				Subscribe: &SubjectPermission{Allow: []string{"foo"}, Deny: []string{"$MQTT.sub.>"}},
			},
		},
		{
			Username: "observer",
			Password: "pass",
		},
	}
	s := testMQTTRunServer(t, o)
	defer testMQTTShutdownServer(s)

	ci := &mqttConnInfo{
		user: "mqtt",
		pass: "pass",
	}
	ci.clientID = "sub"
	mc, rc := testMQTTConnect(t, ci, o.MQTT.Host, o.MQTT.Port)
	defer mc.Close()
	testMQTTCheckConnAck(t, rc, mqttConnAckRCConnectionAccepted, false)
	testMQTTSub(t, 1, mc, rc, []*mqttFilter{{filter: "foo/+", qos: 1}}, []byte{1})
	// Should not be allowed to subscribe on specifically on "foo/baz"
	testMQTTSub(t, 1, mc, rc, []*mqttFilter{{filter: "foo/baz", qos: 1}}, []byte{mqttSubAckFailure})
	// MQTT literals "*" and ">" are converted and interpreted as NATS wildcards
	testMQTTSub(t, 1, mc, rc, []*mqttFilter{{filter: "foo/*", qos: 1}}, []byte{1})
	testMQTTSub(t, 1, mc, rc, []*mqttFilter{{filter: "foo/*/*", qos: 1}}, []byte{mqttSubAckFailure})
	testMQTTSub(t, 1, mc, rc, []*mqttFilter{{filter: "foo/>", qos: 1}}, []byte{mqttSubAckFailure})
	testMQTTSub(t, 1, mc, rc, []*mqttFilter{{filter: "foo/*/>", qos: 1}}, []byte{mqttSubAckFailure})
	testMQTTSub(t, 1, mc, rc, []*mqttFilter{{filter: "foo/>/*", qos: 1}}, []byte{mqttSubAckFailure})
	testMQTTFlush(t, mc, nil, rc)

	ci.clientID = "pub"
	ci.cleanSess = true
	mp, rp := testMQTTConnect(t, ci, o.MQTT.Host, o.MQTT.Port)
	defer mp.Close()
	testMQTTCheckConnAck(t, rp, mqttConnAckRCConnectionAccepted, false)

	// These should be received since publisher has the right to publish on foo.bar
	testMQTTPublish(t, mp, rp, 0, false, false, "foo/bar", 0, []byte("msg1"))
	testMQTTCheckPubMsg(t, mc, rc, "foo/bar", 0, []byte("msg1"))
	testMQTTPublish(t, mp, rp, 1, false, false, "foo/bar", 1, []byte("msg2"))
	testMQTTCheckPubMsg(t, mc, rc, "foo/bar", mqttPubQos1, []byte("msg2"))

	// MQTT clients should not be able to access internal "$MQTT.msgs.*" subjects
	// unless explicitly granted by allow permissions.
	testMQTTSub(t, 1, mc, rc, []*mqttFilter{{filter: "$MQTT/msgs/#", qos: 1}}, []byte{mqttSubAckFailure})

	// MQTT clients should not be able to subscribe to the internal
	// "$MQTT.deliver.pubrel.*" subjects either. Unlike "$MQTT.msgs.*",
	// canSubscribe bypasses the allow-list for this prefix (so internal
	// subscriptions work), so processSubs is the only guard that rejects it.
	testMQTTSub(t, 1, mc, rc, []*mqttFilter{{filter: strings.ReplaceAll(mqttPubRelDeliverySubjectPrefix, ".", "/") + "+", qos: 1}}, []byte{mqttSubAckFailure})

	onc := natsConnect(t, s.ClientURL(), nats.UserInfo("observer", "pass"))
	defer onc.Close()
	osub := natsSubSync(t, onc, "$MQTT.msgs.>")
	natsFlush(t, onc)
	testMQTTPublish(t, mp, rp, 0, false, false, "$MQTT/msgs/injected", 0, []byte("internal"))
	if m, err := osub.NextMsg(250 * time.Millisecond); err == nil {
		t.Fatalf("Expected no internal MQTT message to be published, got subject=%q", m.Subject)
	}

	// But these should not be cause pub has no permission to publish on foo.baz
	testMQTTPublish(t, mp, rp, 0, false, false, "foo/baz", 0, []byte("msg3"))
	testMQTTExpectNothing(t, rc)
	testMQTTPublish(t, mp, rp, 1, false, false, "foo/baz", 1, []byte("msg4"))
	testMQTTExpectNothing(t, rc)

	// Disconnect publisher
	testMQTTDisconnect(t, mp, nil)
	mp.Close()
	// Disconnect subscriber and restart it to make sure that it does not receive msg3/msg4
	testMQTTDisconnect(t, mc, nil)
	mc.Close()

	ci.cleanSess = false
	ci.clientID = "sub"
	mc, rc = testMQTTConnect(t, ci, o.MQTT.Host, o.MQTT.Port)
	defer mc.Close()
	testMQTTCheckConnAck(t, rc, mqttConnAckRCConnectionAccepted, true)
	testMQTTSub(t, 1, mc, rc, []*mqttFilter{{filter: "foo/+", qos: 1}}, []byte{1})
	testMQTTExpectNothing(t, rc)
	mc.Close()

	ci.cleanSess = true
	ci.user = "mqtt2"
	ci.clientID = "sub"
	mc, rc = testMQTTConnect(t, ci, o.MQTT.Host, o.MQTT.Port)
	defer mc.Close()
	testMQTTCheckConnAck(t, rc, mqttConnAckRCConnectionAccepted, false)
	// This should fail because although this user is allowed to subscribe on "foo"
	// we deny permission to subscribe on "$MQTT.sub.>", so the server won't be
	// able to create the internal NATS subscription for the JS consumer.
	testMQTTSub(t, 1, mc, rc, []*mqttFilter{{filter: "foo/baz", qos: 1}}, []byte{mqttSubAckFailure})
}

func TestMQTTSubscribeDenyInternalMessageSubject(t *testing.T) {
	o := testMQTTDefaultOptions()
	o.Users = []*User{
		{
			Username: "victim",
			Password: "pass",
			Permissions: &Permissions{
				Publish: &SubjectPermission{Allow: []string{"secret.>"}},
			},
		},
		{
			Username: "attacker",
			Password: "pass",
			Permissions: &Permissions{
				Subscribe: &SubjectPermission{Deny: []string{"secret.>"}},
			},
		},
	}
	s := testMQTTRunServer(t, o)
	defer testMQTTShutdownServer(s)

	attacker := &mqttConnInfo{clientID: "attacker", user: "attacker", pass: "pass", cleanSess: true}
	ac, ar := testMQTTConnect(t, attacker, o.MQTT.Host, o.MQTT.Port)
	defer ac.Close()
	testMQTTCheckConnAck(t, ar, mqttConnAckRCConnectionAccepted, false)

	// The deny permission rejects the user topic directly. Neither the internal
	// namespace nor the "msgs" stream subject may be used to bypass it.
	testMQTTSub(t, 1, ac, ar, []*mqttFilter{{filter: "secret/#", qos: 0}}, []byte{mqttSubAckFailure})
	testMQTTSub(t, 2, ac, ar, []*mqttFilter{{filter: "$MQTT/#", qos: 0}}, []byte{mqttSubAckFailure})
	testMQTTSub(t, 3, ac, ar, []*mqttFilter{{filter: "$MQTT/msgs/secret/#", qos: 0}}, []byte{mqttSubAckFailure})

	victim := &mqttConnInfo{clientID: "victim", user: "victim", pass: "pass", cleanSess: true}
	vc, vr := testMQTTConnect(t, victim, o.MQTT.Host, o.MQTT.Port)
	defer vc.Close()
	testMQTTCheckConnAck(t, vr, mqttConnAckRCConnectionAccepted, false)
	testMQTTPublish(t, vc, vr, 1, false, false, "secret/x", 1, []byte("secret payload"))
	testMQTTExpectNothing(t, ar)
}

func TestMQTTSubscribeDenyRetainedAndQoSReplay(t *testing.T) {
	// A subscriber can be allowed to subscribe to a broad wildcard while being
	// denied delivery of more specific subjects. Normal live delivery applies
	// the deny filter (deliverMsg -> checkDenySub), but retained-message
	// serialization and QoS1+ durable replay must apply it too.
	o := testMQTTDefaultOptions()
	o.Users = []*User{
		{
			Username: "pub",
			Password: "pass",
			Permissions: &Permissions{
				Publish:   &SubjectPermission{Allow: []string{"secret.>", "foo.>"}},
				Subscribe: &SubjectPermission{Allow: []string{"_INBOX.>"}},
			},
		},
		{
			Username: "sub",
			Password: "pass",
			Permissions: &Permissions{
				Subscribe: &SubjectPermission{Allow: []string{">"}, Deny: []string{"secret.>"}},
			},
		},
	}
	s := testMQTTRunServer(t, o)
	defer testMQTTShutdownServer(s)

	pub := &mqttConnInfo{clientID: "pub", user: "pub", pass: "pass", cleanSess: true}
	mp, rp := testMQTTConnect(t, pub, o.MQTT.Host, o.MQTT.Port)
	defer mp.Close()
	testMQTTCheckConnAck(t, rp, mqttConnAckRCConnectionAccepted, false)

	// 1) Live delivery: the denied subject must not be delivered, but an allowed
	// subject on the same broad wildcard subscription must.
	subLive := &mqttConnInfo{clientID: "sub-live", user: "sub", pass: "pass", cleanSess: true}
	mc, rc := testMQTTConnect(t, subLive, o.MQTT.Host, o.MQTT.Port)
	defer mc.Close()
	testMQTTCheckConnAck(t, rc, mqttConnAckRCConnectionAccepted, false)
	testMQTTSub(t, 1, mc, rc, []*mqttFilter{{filter: "#", qos: 0}}, []byte{0})
	testMQTTPublish(t, mp, rp, 0, false, false, "secret/one", 0, []byte("live-denied"))
	testMQTTExpectNothing(t, rc)
	testMQTTPublish(t, mp, rp, 0, false, false, "foo/live", 0, []byte("live-allowed"))
	testMQTTCheckPubMsg(t, mc, rc, "foo/live", 0, []byte("live-allowed"))
	testMQTTDisconnect(t, mc, nil)
	mc.Close()

	// 2) QoS1 durable replay: subscribe with a persistent session, disconnect,
	// publish a denied and an allowed QoS1 message while offline, then reconnect.
	// The denied subject must not be replayed; the allowed one must.
	persist := &mqttConnInfo{clientID: "persist-sub", user: "sub", pass: "pass", cleanSess: false}
	mc, rc = testMQTTConnect(t, persist, o.MQTT.Host, o.MQTT.Port)
	defer mc.Close()
	testMQTTCheckConnAck(t, rc, mqttConnAckRCConnectionAccepted, false)
	testMQTTSub(t, 1, mc, rc, []*mqttFilter{{filter: "#", qos: 1}}, []byte{1})
	testMQTTDisconnect(t, mc, nil)
	mc.Close()

	// secret.one is published first so that, without the deny check, it would be
	// the first message replayed (i.e. the bypass would be observed here).
	testMQTTPublish(t, mp, rp, 1, false, false, "secret/one", 1, []byte("qos1-denied"))
	testMQTTPublish(t, mp, rp, 1, false, false, "foo/replay", 2, []byte("qos1-allowed"))

	mc, rc = testMQTTConnect(t, persist, o.MQTT.Host, o.MQTT.Port)
	defer mc.Close()
	testMQTTCheckConnAck(t, rc, mqttConnAckRCConnectionAccepted, true)
	// Only the allowed message is replayed; the denied one is filtered out (and
	// acked so JetStream does not keep redelivering it) before delivery.
	testMQTTCheckPubMsg(t, mc, rc, "foo/replay", mqttPubQos1, []byte("qos1-allowed"))
	testMQTTExpectNothing(t, rc)
	testMQTTDisconnect(t, mc, nil)
	mc.Close()

	// 3) Retained delivery: a retained denied subject must not be serialized to a
	// new subscriber, but a retained allowed subject must.
	testMQTTPublish(t, mp, rp, 0, false, true, "secret/one", 0, []byte("retained-denied"))
	testMQTTPublish(t, mp, rp, 0, false, true, "foo/ret", 0, []byte("retained-allowed"))
	testMQTTFlush(t, mp, nil, rp)

	subRet := &mqttConnInfo{clientID: "sub-retained", user: "sub", pass: "pass", cleanSess: true}
	mc, rc = testMQTTConnect(t, subRet, o.MQTT.Host, o.MQTT.Port)
	defer mc.Close()
	testMQTTCheckConnAck(t, rc, mqttConnAckRCConnectionAccepted, false)
	testMQTTSub(t, 1, mc, rc, []*mqttFilter{{filter: "#", qos: 0}}, []byte{0})
	// Only the allowed retained message is delivered (with the RETAIN flag set).
	testMQTTCheckPubMsg(t, mc, rc, "foo/ret", mqttPubFlagRetain, []byte("retained-allowed"))
	testMQTTExpectNothing(t, rc)
}

func TestMQTTLeafnodePermissionsNoImplicitMQTTPrefix(t *testing.T) {
	hubConf := createConfFile(t, fmt.Appendf(nil, `
		server_name: hub
		listen: "127.0.0.1:-1"
		jetstream { store_dir: %q }
		mqtt { listen: "127.0.0.1:-1" }
		leafnodes { listen: "127.0.0.1:-1" }
		authorization {
			users: [
				{ user: mqtt, password: pass }
				{ user: leaf, password: pass, permissions: { publish: "foo.>", subscribe: "foo.>" } }
			]
		}
	`, t.TempDir()))
	hub, ohub := RunServerWithConfig(hubConf)
	defer hub.Shutdown()

	leafConf := createConfFile(t, fmt.Appendf(nil, `
		server_name: leaf
		listen: "127.0.0.1:-1"
		leafnodes { remotes [ { urls: [ nats-leaf://leaf:pass@127.0.0.1:%d ] } ] }
	`, ohub.LeafNode.Port))
	leaf, _ := RunServerWithConfig(leafConf)
	defer leaf.Shutdown()
	checkLeafNodeConnected(t, hub)
	checkLeafNodeConnected(t, leaf)

	nc := natsConnect(t, leaf.ClientURL())
	defer nc.Close()
	allowed := natsSubSync(t, nc, "foo.>")
	internal := natsSubSync(t, nc, "$MQTT.>")
	natsFlush(t, nc)
	checkSubInterest(t, hub, globalAccountName, "foo.x", time.Second)

	mc, rc := testMQTTConnect(t, &mqttConnInfo{clientID: "sub", user: "mqtt", pass: "pass"}, ohub.MQTT.Host, ohub.MQTT.Port)
	defer mc.Close()
	testMQTTCheckConnAck(t, rc, mqttConnAckRCConnectionAccepted, false)
	testMQTTSub(t, 1, mc, rc, []*mqttFilter{{filter: "secret/x", qos: 1}}, []byte{1})
	testMQTTFlush(t, mc, nil, rc)

	mp, rp := testMQTTConnect(t, &mqttConnInfo{cleanSess: true, user: "mqtt", pass: "pass"}, ohub.MQTT.Host, ohub.MQTT.Port)
	defer mp.Close()
	testMQTTCheckConnAck(t, rp, mqttConnAckRCConnectionAccepted, false)
	testMQTTPublish(t, mp, rp, 1, false, false, "foo/x", 1, []byte("allowed"))
	testMQTTPublish(t, mp, rp, 1, false, true, "secret/x", 2, []byte("secret"))
	testMQTTCheckPubMsg(t, mc, rc, "secret/x", mqttPubQos1, []byte("secret"))

	// The leaf receives MQTT messages on subjects it is allowed to subscribe to.
	msg := natsNexMsg(t, allowed, time.Second)
	require_Equal(t, msg.Subject, "foo.x")
	require_Equal(t, string(msg.Data), "allowed")

	// But not the internal MQTT traffic, such as messages, retained messages and sessions.
	if msg, err := internal.NextMsg(250 * time.Millisecond); err == nil {
		t.Fatalf("Expected no internal MQTT message, got %q", msg.Subject)
	}

	// The leaf must also not be able to inject messages into the MQTT stream.
	im := nats.NewMsg(mqttStreamSubjectPrefix + "secret.x")
	im.Header.Set(mqttNatsHeader, "1")
	im.Data = []byte("injected")
	require_NoError(t, nc.PublishMsg(im))
	natsFlush(t, nc)
	testMQTTExpectNothing(t, rc)
}

func TestMQTTCleanSession(t *testing.T) {
	o := testMQTTDefaultOptions()
	s := testMQTTRunServer(t, o)
	defer testMQTTShutdownServer(s)

	ci := &mqttConnInfo{
		clientID:  "me",
		cleanSess: false,
	}
	c, r := testMQTTConnect(t, ci, o.MQTT.Host, o.MQTT.Port)
	defer c.Close()
	testMQTTCheckConnAck(t, r, mqttConnAckRCConnectionAccepted, false)
	testMQTTDisconnect(t, c, nil)
	c.Close()

	c, r = testMQTTConnect(t, ci, o.MQTT.Host, o.MQTT.Port)
	defer c.Close()
	testMQTTCheckConnAck(t, r, mqttConnAckRCConnectionAccepted, true)
	testMQTTDisconnect(t, c, nil)
	c.Close()

	ci.cleanSess = true
	c, r = testMQTTConnect(t, ci, o.MQTT.Host, o.MQTT.Port)
	defer c.Close()
	testMQTTCheckConnAck(t, r, mqttConnAckRCConnectionAccepted, false)
	testMQTTDisconnect(t, c, nil)
}

func TestMQTTDuplicateClientID(t *testing.T) {
	o := testMQTTDefaultOptions()
	s := testMQTTRunServer(t, o)
	defer testMQTTShutdownServer(s)

	ci := &mqttConnInfo{
		clientID:  "me",
		cleanSess: false,
	}
	c1, r1 := testMQTTConnect(t, ci, o.MQTT.Host, o.MQTT.Port)
	defer c1.Close()
	testMQTTCheckConnAck(t, r1, mqttConnAckRCConnectionAccepted, false)

	c2, r2 := testMQTTConnect(t, ci, o.MQTT.Host, o.MQTT.Port)
	defer c2.Close()
	testMQTTCheckConnAck(t, r2, mqttConnAckRCConnectionAccepted, true)

	// The old client should be disconnected.
	testMQTTExpectDisconnect(t, c1)
}

func TestMQTTPersistedSession(t *testing.T) {
	o := testMQTTDefaultOptions()
	s := testMQTTRunServer(t, o)
	defer testMQTTShutdownRestartedServer(&s)

	cisub := &mqttConnInfo{clientID: "sub", cleanSess: false}
	cipub := &mqttConnInfo{clientID: "pub", cleanSess: true}

	c, r := testMQTTConnect(t, cisub, o.MQTT.Host, o.MQTT.Port)
	defer c.Close()
	testMQTTCheckConnAck(t, r, mqttConnAckRCConnectionAccepted, false)

	testMQTTSub(t, 1, c, r,
		[]*mqttFilter{
			{filter: "foo/#", qos: 1},
			{filter: "bar", qos: 1},
			{filter: "baz", qos: 0},
		},
		[]byte{1, 1, 0})
	testMQTTFlush(t, c, nil, r)

	// Shutdown server, close connection and restart server. It should
	// have restored the session and consumers.
	dir := strings.TrimSuffix(s.JetStreamConfig().StoreDir, JetStreamStoreDir)
	s.Shutdown()
	c.Close()

	o.Port = -1
	o.MQTT.Port = -1
	o.StoreDir = dir
	s = testMQTTRunServer(t, o)
	// There is already the defer for shutdown at top of function

	// Create a publisher that will send qos1 so we verify that messages
	// are stored for the persisted sessions.
	c, r = testMQTTConnect(t, cipub, o.MQTT.Host, o.MQTT.Port)
	defer c.Close()
	testMQTTCheckConnAck(t, r, mqttConnAckRCConnectionAccepted, false)

	testMQTTPublish(t, c, r, 1, false, false, "foo/bar", 1, []byte("msg0"))
	testMQTTFlush(t, c, nil, r)
	testMQTTDisconnect(t, c, nil)
	c.Close()

	// Recreate session
	c, r = testMQTTConnect(t, cisub, o.MQTT.Host, o.MQTT.Port)
	defer c.Close()
	testMQTTCheckConnAck(t, r, mqttConnAckRCConnectionAccepted, true)

	// Since consumers have been recovered, messages should be received
	// (MQTT does not need client to recreate consumers for a recovered
	// session)

	// Check that qos1 publish message is received.
	testMQTTCheckPubMsg(t, c, r, "foo/bar", mqttPubQos1, []byte("msg0"))

	// Flush to prevent publishes to be done too soon since we are
	// receiving the CONNACK before the subscriptions are restored.
	testMQTTFlush(t, c, nil, r)

	// Now publish some messages to all subscriptions.
	nc := natsConnect(t, s.ClientURL())
	defer nc.Close()

	natsPub(t, nc, "foo.bar", []byte("msg1"))
	testMQTTCheckPubMsg(t, c, r, "foo/bar", 0, []byte("msg1"))

	natsPub(t, nc, "foo", []byte("msg2"))
	testMQTTCheckPubMsg(t, c, r, "foo", 0, []byte("msg2"))

	natsPub(t, nc, "bar", []byte("msg3"))
	testMQTTCheckPubMsg(t, c, r, "bar", 0, []byte("msg3"))

	natsPub(t, nc, "baz", []byte("msg4"))
	testMQTTCheckPubMsg(t, c, r, "baz", 0, []byte("msg4"))

	// Now unsub "bar" and verify that message published on this topic
	// is not received.
	testMQTTUnsub(t, 1, c, r, []*mqttFilter{{filter: "bar"}})
	natsPub(t, nc, "bar", []byte("msg5"))
	testMQTTExpectNothing(t, r)

	nc.Close()
	s.Shutdown()
	c.Close()

	o.Port = -1
	o.MQTT.Port = -1
	o.StoreDir = dir
	s = testMQTTRunServer(t, o)
	// There is already the defer for shutdown at top of function

	// Recreate a client
	c, r = testMQTTConnect(t, cisub, o.MQTT.Host, o.MQTT.Port)
	defer c.Close()
	testMQTTCheckConnAck(t, r, mqttConnAckRCConnectionAccepted, true)

	nc = natsConnect(t, s.ClientURL())
	defer nc.Close()

	natsPub(t, nc, "foo.bar", []byte("msg6"))
	testMQTTCheckPubMsg(t, c, r, "foo/bar", 0, []byte("msg6"))

	natsPub(t, nc, "foo", []byte("msg7"))
	testMQTTCheckPubMsg(t, c, r, "foo", 0, []byte("msg7"))

	// Make sure that we did not recover bar.
	natsPub(t, nc, "bar", []byte("msg8"))
	testMQTTExpectNothing(t, r)

	natsPub(t, nc, "baz", []byte("msg9"))
	testMQTTCheckPubMsg(t, c, r, "baz", 0, []byte("msg9"))

	// Have the sub client send a subscription downgrading the qos1 subscription.
	testMQTTSub(t, 1, c, r, []*mqttFilter{{filter: "foo/#", qos: 0}}, []byte{0})
	testMQTTFlush(t, c, nil, r)

	nc.Close()
	s.Shutdown()
	c.Close()

	o.Port = -1
	o.MQTT.Port = -1
	o.StoreDir = dir
	s = testMQTTRunServer(t, o)
	// There is already the defer for shutdown at top of function

	// Recreate the sub client
	c, r = testMQTTConnect(t, cisub, o.MQTT.Host, o.MQTT.Port)
	defer c.Close()
	testMQTTCheckConnAck(t, r, mqttConnAckRCConnectionAccepted, true)

	// Publish as a qos1
	c2, r2 := testMQTTConnect(t, cipub, o.MQTT.Host, o.MQTT.Port)
	defer c2.Close()
	testMQTTCheckConnAck(t, r2, mqttConnAckRCConnectionAccepted, false)
	testMQTTPublish(t, c2, r2, 1, false, false, "foo/bar", 1, []byte("msg10"))

	// Verify that it is received as qos0 which is the qos of the subscription.
	testMQTTCheckPubMsg(t, c, r, "foo/bar", 0, []byte("msg10"))

	testMQTTDisconnect(t, c, nil)
	c.Close()
	testMQTTDisconnect(t, c2, nil)
	c2.Close()

	// Finally, recreate the sub with clean session and ensure that all is gone
	cisub.cleanSess = true
	for i := 0; i < 2; i++ {
		c, r = testMQTTConnect(t, cisub, o.MQTT.Host, o.MQTT.Port)
		defer c.Close()
		testMQTTCheckConnAck(t, r, mqttConnAckRCConnectionAccepted, false)

		nc = natsConnect(t, s.ClientURL())
		defer nc.Close()

		natsPub(t, nc, "foo.bar", []byte("msg11"))
		testMQTTExpectNothing(t, r)

		natsPub(t, nc, "foo", []byte("msg12"))
		testMQTTExpectNothing(t, r)

		// Make sure that we did not recover bar.
		natsPub(t, nc, "bar", []byte("msg13"))
		testMQTTExpectNothing(t, r)

		natsPub(t, nc, "baz", []byte("msg14"))
		testMQTTExpectNothing(t, r)

		testMQTTDisconnect(t, c, nil)
		c.Close()
		nc.Close()

		s.Shutdown()
		o.Port = -1
		o.MQTT.Port = -1
		o.StoreDir = dir
		s = testMQTTRunServer(t, o)
		// There is already the defer for shutdown at top of function
	}
}

func TestMQTTRejectsHashCollidingClientID(t *testing.T) {
	o := testMQTTDefaultOptions()
	s := testMQTTRunServer(t, o)
	defer testMQTTShutdownServer(s)

	// These IDs were found to collide under getHash()
	victimID := "cid-03579431"
	attackerID := "cid-09191235"
	require_True(t, getHash(victimID) == getHash(attackerID))

	c, r := testMQTTConnect(t, &mqttConnInfo{clientID: victimID, cleanSess: false}, o.MQTT.Host, o.MQTT.Port)
	testMQTTCheckConnAck(t, r, mqttConnAckRCConnectionAccepted, false)
	testMQTTSub(t, 1, c, r, []*mqttFilter{{filter: "foo", qos: 1}}, []byte{1})
	testMQTTFlush(t, c, nil, r)
	testMQTTDisconnect(t, c, nil)
	c.Close()

	attacker, r := testMQTTConnect(t, &mqttConnInfo{clientID: attackerID, cleanSess: false}, o.MQTT.Host, o.MQTT.Port)
	defer attacker.Close()
	testMQTTCheckConnAck(t, r, mqttConnAckRCIdentifierRejected, false)
	testMQTTExpectDisconnect(t, attacker)
	attacker.Close()

	c, r = testMQTTConnect(t, &mqttConnInfo{clientID: victimID, cleanSess: false}, o.MQTT.Host, o.MQTT.Port)
	defer c.Close()
	testMQTTCheckConnAck(t, r, mqttConnAckRCConnectionAccepted, true)
	testMQTTFlush(t, c, nil, r)

	nc := natsConnect(t, s.ClientURL())
	defer nc.Close()

	natsPub(t, nc, "foo", []byte("msg"))
	testMQTTCheckPubMsg(t, c, r, "foo", 0, []byte("msg"))
}

func TestMQTTRecoverSessionAndAddNewSub(t *testing.T) {
	o := testMQTTDefaultOptions()
	s := testMQTTRunServer(t, o)
	defer testMQTTShutdownRestartedServer(&s)

	cisub := &mqttConnInfo{clientID: "sub1", cleanSess: false}
	c, r := testMQTTConnect(t, cisub, o.MQTT.Host, o.MQTT.Port)
	defer c.Close()
	testMQTTCheckConnAck(t, r, mqttConnAckRCConnectionAccepted, false)
	testMQTTDisconnect(t, c, nil)
	c.Close()

	// Shutdown server, close connection and restart server. It should
	// have restored the session and consumers.
	dir := strings.TrimSuffix(s.JetStreamConfig().StoreDir, JetStreamStoreDir)
	s.Shutdown()
	c.Close()

	o.Port = -1
	o.MQTT.Port = -1
	o.StoreDir = dir
	s = testMQTTRunServer(t, o)
	// No need for defer since it is done top of function

	c, r = testMQTTConnect(t, cisub, o.MQTT.Host, o.MQTT.Port)
	defer c.Close()
	testMQTTCheckConnAck(t, r, mqttConnAckRCConnectionAccepted, true)
	// Now add sub and make sure it does not crash
	testMQTTSub(t, 1, c, r, []*mqttFilter{{filter: "foo", qos: 1}}, []byte{1})
	testMQTTFlush(t, c, nil, r)

	// Now repeat with a new client but without server restart.
	cisub2 := &mqttConnInfo{clientID: "sub2", cleanSess: false}
	c2, r2 := testMQTTConnect(t, cisub2, o.MQTT.Host, o.MQTT.Port)
	defer c2.Close()
	testMQTTCheckConnAck(t, r2, mqttConnAckRCConnectionAccepted, false)
	testMQTTDisconnect(t, c2, nil)
	c2.Close()

	c2, r2 = testMQTTConnect(t, cisub2, o.MQTT.Host, o.MQTT.Port)
	defer c2.Close()
	testMQTTCheckConnAck(t, r2, mqttConnAckRCConnectionAccepted, true)
	testMQTTSub(t, 1, c2, r2, []*mqttFilter{{filter: "bar", qos: 1}}, []byte{1})
	testMQTTFlush(t, c2, nil, r2)
}

func TestMQTTRecoverSessionWithSubAndClientResendSub(t *testing.T) {
	o := testMQTTDefaultOptions()
	s := testMQTTRunServer(t, o)
	defer testMQTTShutdownRestartedServer(&s)

	cisub1 := &mqttConnInfo{clientID: "sub1", cleanSess: false}
	c, r := testMQTTConnect(t, cisub1, o.MQTT.Host, o.MQTT.Port)
	defer c.Close()
	testMQTTCheckConnAck(t, r, mqttConnAckRCConnectionAccepted, false)

	// Have a client send a SUBSCRIBE protocol for foo, QoS1
	testMQTTSub(t, 1, c, r, []*mqttFilter{{filter: "foo", qos: 1}}, []byte{1})
	testMQTTDisconnect(t, c, nil)
	c.Close()

	// Restart the server now.
	dir := strings.TrimSuffix(s.JetStreamConfig().StoreDir, JetStreamStoreDir)
	s.Shutdown()

	o.Port = -1
	o.MQTT.Port = -1
	o.StoreDir = dir
	s = testMQTTRunServer(t, o)
	// No need for defer since it is done top of function

	// Now restart the client. Since the client was created with cleanSess==false,
	// the server will have recorded the subscriptions for this client.
	c, r = testMQTTConnect(t, cisub1, o.MQTT.Host, o.MQTT.Port)
	defer c.Close()
	testMQTTCheckConnAck(t, r, mqttConnAckRCConnectionAccepted, true)
	// At this point, the server has recreated the subscription on foo, QoS1.

	// For applications that restart, it is possible (likely) that they
	// will resend their SUBSCRIBE protocols, so do so now:
	testMQTTSub(t, 1, c, r, []*mqttFilter{{filter: "foo", qos: 1}}, []byte{1})
	testMQTTFlush(t, c, nil, r)

	checkNumSub := func(clientID string) {
		t.Helper()

		// Find the MQTT client...
		mc := testMQTTGetClient(t, s, clientID)

		// Check how many NATS subscriptions are registered.
		var fooSub int
		var otherSub int
		mc.mu.Lock()
		for _, sub := range mc.subs {
			switch string(sub.subject) {
			case "foo":
				fooSub++
			default:
				otherSub++
			}
		}
		mc.mu.Unlock()

		// We should have 2 subscriptions, one on "foo", and one for the JS durable
		// consumer's delivery subject.
		if fooSub != 1 {
			t.Fatalf("Expected 1 sub on 'foo', got %v", fooSub)
		}
		if otherSub != 1 {
			t.Fatalf("Expected 1 subscription for JS durable, got %v", otherSub)
		}
	}
	checkNumSub("sub1")

	c.Close()

	// Now same but without the server restart in-between.
	cisub2 := &mqttConnInfo{clientID: "sub2", cleanSess: false}
	c, r = testMQTTConnect(t, cisub2, o.MQTT.Host, o.MQTT.Port)
	defer c.Close()
	testMQTTCheckConnAck(t, r, mqttConnAckRCConnectionAccepted, false)
	testMQTTSub(t, 1, c, r, []*mqttFilter{{filter: "foo", qos: 1}}, []byte{1})
	testMQTTDisconnect(t, c, nil)
	c.Close()
	// Restart client
	c, r = testMQTTConnect(t, cisub2, o.MQTT.Host, o.MQTT.Port)
	defer c.Close()
	testMQTTCheckConnAck(t, r, mqttConnAckRCConnectionAccepted, true)
	testMQTTSub(t, 1, c, r, []*mqttFilter{{filter: "foo", qos: 1}}, []byte{1})
	testMQTTFlush(t, c, nil, r)
	// Check client subs
	checkNumSub("sub2")
}

func TestMQTTFlappingSession(t *testing.T) {
	mqttSessJailDur = 250 * time.Millisecond
	mqttFlapCleanItvl = 350 * time.Millisecond
	defer func() {
		mqttSessJailDur = mqttSessFlappingJailDur
		mqttFlapCleanItvl = mqttSessFlappingCleanupInterval
	}()

	o := testMQTTDefaultOptions()
	s := testMQTTRunServer(t, o)
	defer testMQTTShutdownServer(s)

	ci := &mqttConnInfo{clientID: "flapper", cleanSess: false}
	c, r := testMQTTConnect(t, ci, o.MQTT.Host, o.MQTT.Port)
	defer c.Close()
	testMQTTCheckConnAck(t, r, mqttConnAckRCConnectionAccepted, false)

	// Let's get a handle on the asm to check things later.
	asm := testMQTTGetAccountSessionManager(t, s, "flapper")

	// Start a new connection with the same clientID, which should replace
	// the old one and put it in the flappers map.
	c2, r2 := testMQTTConnect(t, ci, o.MQTT.Host, o.MQTT.Port)
	defer c2.Close()
	testMQTTCheckConnAck(t, r2, mqttConnAckRCConnectionAccepted, true)

	// Should be disconnected...
	testMQTTExpectDisconnect(t, c)

	// Now try to reconnect "c" and we should fail. We have to do this manually,
	// since we expect it to fail.
	addr := net.JoinHostPort(o.MQTT.Host, fmt.Sprintf("%d", o.MQTT.Port))
	c, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatalf("Error creating mqtt connection: %v", err)
	}
	defer c.Close()
	proto := mqttCreateConnectProto(ci)
	if _, err := testMQTTWrite(c, proto); err != nil {
		t.Fatalf("Error writing protocols: %v", err)
	}
	// Misbehave and send a SUB protocol without waiting for the CONNACK
	w := newMQTTWriter(0)
	pkLen := 2 // for pi
	// Topic "foo"
	pkLen += 2 + 3 + 1
	w.WriteByte(mqttPacketSub | mqttSubscribeFlags)
	w.WriteVarInt(pkLen)
	w.WriteUint16(1)
	w.WriteBytes([]byte("foo"))
	w.WriteByte(1)
	if _, err := testMQTTWrite(c, w.Bytes()); err != nil {
		t.Fatalf("Error writing protocols: %v", err)
	}
	// Now read the CONNACK and we should have been disconnected.
	if _, err := testMQTTRead(c); err == nil {
		t.Fatal("Expected connection to fail")
	}

	// This should be in the flappers map, but after 250ms should be cleared.
	for i := 0; i < 2; i++ {
		asm.mu.RLock()
		_, present := asm.flappers["flapper"]
		asm.mu.RUnlock()
		if i == 0 {
			if !present {
				t.Fatal("Did not find the client ID in the flappers map")
			}
			// Wait for more than the cleanup interval
			time.Sleep(mqttFlapCleanItvl + 100*time.Millisecond)
		} else if present {
			t.Fatal("The client ID should have been cleared from the map")
		}
	}
}

func TestMQTTLockedSession(t *testing.T) {
	o := testMQTTDefaultOptions()
	s := testMQTTRunServer(t, o)
	defer testMQTTShutdownServer(s)

	subClientID := "sub"
	ci := &mqttConnInfo{clientID: subClientID, cleanSess: false}
	c, r := testMQTTConnect(t, ci, o.MQTT.Host, o.MQTT.Port)
	defer c.Close()
	testMQTTCheckConnAck(t, r, mqttConnAckRCConnectionAccepted, false)

	sm := &s.mqtt.sessmgr
	sm.mu.Lock()
	asm := sm.sessions[globalAccountName]
	sm.mu.Unlock()
	if asm == nil {
		t.Fatalf("account session manager not found")
	}

	// It is possible, however unlikely, to have received CONNACK while
	// mqttProcessConnect is still running, and the session remains locked. Wait
	// for it to finish.
	checkFor(t, 250*time.Millisecond, 10*time.Millisecond, func() error {
		asm.mu.RLock()
		defer asm.mu.RUnlock()
		if _, stillLocked := asm.sessLocked[subClientID]; stillLocked {
			return fmt.Errorf("session still locked")
		}
		return nil
	})

	// Get the session for "sub"
	cli := testMQTTGetClient(t, s, subClientID)
	sess := cli.mqtt.sess

	// Pretend that the session above is locked.
	if err := asm.lockSession(sess, cli); err != nil {
		t.Fatalf("Unable to lock session: %v", err)
	}
	defer asm.unlockSession(sess)

	// Now try to connect another client that wants to use "sub".
	// We can't use testMQTTConnect() because it is going to fail.
	addr := net.JoinHostPort(o.MQTT.Host, fmt.Sprintf("%d", o.MQTT.Port))
	c2, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatalf("Error creating mqtt connection: %v", err)
	}
	defer c2.Close()
	proto := mqttCreateConnectProto(ci)
	if _, err := testMQTTWrite(c2, proto); err != nil {
		t.Fatalf("Error writing connect: %v", err)
	}
	if _, err := testMQTTRead(c2); err == nil {
		t.Fatal("Expected connection to fail")
	}

	// Now try again, but this time release the session while waiting
	// to connect and it should succeed.
	time.AfterFunc(250*time.Millisecond, func() { asm.unlockSession(sess) })
	c3, r3 := testMQTTConnect(t, ci, o.MQTT.Host, o.MQTT.Port)
	defer c3.Close()
	testMQTTCheckConnAck(t, r3, mqttConnAckRCConnectionAccepted, true)
}

func TestMQTTPersistRetainedMsg(t *testing.T) {
	o := testMQTTDefaultOptions()
	s := testMQTTRunServer(t, o)
	defer testMQTTShutdownRestartedServer(&s)

	dir := strings.TrimSuffix(s.JetStreamConfig().StoreDir, JetStreamStoreDir)

	cipub := &mqttConnInfo{clientID: "pub", cleanSess: true}
	c, r := testMQTTConnect(t, cipub, o.MQTT.Host, o.MQTT.Port)
	defer c.Close()
	testMQTTCheckConnAck(t, r, mqttConnAckRCConnectionAccepted, false)

	testMQTTPublish(t, c, r, 1, false, true, "moo/foo", 1, []byte("foo1"))
	testMQTTPublish(t, c, r, 1, false, true, "moo/foo", 1, []byte("foo2"))
	testMQTTPublish(t, c, r, 1, false, true, "moo/bar", 1, []byte("bar1"))
	testMQTTPublish(t, c, r, 0, false, true, "moo/baz", 1, []byte("baz1"))
	// Remove bar
	testMQTTPublish(t, c, r, 1, false, true, "moo/bar", 1, nil)
	testMQTTFlush(t, c, nil, r)
	testMQTTDisconnect(t, c, nil)
	c.Close()

	s.Shutdown()

	o.Port = -1
	o.MQTT.Port = -1
	o.StoreDir = dir
	s = testMQTTRunServer(t, o)

	cisub := &mqttConnInfo{clientID: "sub", cleanSess: false}
	c, r = testMQTTConnect(t, cisub, o.MQTT.Host, o.MQTT.Port)
	defer c.Close()
	testMQTTCheckConnAck(t, r, mqttConnAckRCConnectionAccepted, false)

	t.Run("many subs one topic", func(t *testing.T) {
		testMQTTSub(t, 1, c, r, []*mqttFilter{{filter: "moo/foo", qos: 1}}, []byte{1})
		testMQTTCheckPubMsg(t, c, r, "moo/foo", mqttPubFlagRetain|mqttPubQos1, []byte("foo2"))

		testMQTTSub(t, 1, c, r, []*mqttFilter{{filter: "moo/baz", qos: 1}}, []byte{1})
		testMQTTCheckPubMsg(t, c, r, "moo/baz", mqttPubFlagRetain, []byte("baz1"))

		testMQTTSub(t, 1, c, r, []*mqttFilter{{filter: "moo/bar", qos: 1}}, []byte{1})
		testMQTTExpectNothing(t, r)
	})

	runSingleSubscribe := func(filters []*mqttFilter) func(t *testing.T) {
		return func(t *testing.T) {
			testMQTTSub(t, 1, c, r, filters, nil)

			for i := 0; i < 2; i++ {
				flags, pi, topic, payload := testMQTTReadPubPacket(t, r)
				if (flags & mqttPubFlagRetain) == 0 {
					t.Fatalf("Expected flags to have retain set, got %v", flags)
				}
				if (flags & mqttPubFlagDup) != 0 {
					t.Fatalf("Expected flags to not have Dup set, got %v", flags)
				}
				var expQOS byte
				var expPayload []byte
				switch topic {
				case "moo/foo":
					expQOS = mqttPubQos1
					expPayload = []byte("foo2")
					testMQTTSendPIPacket(mqttPacketPubAck, t, c, pi)
				case "moo/baz":
					expQOS = 0
					expPayload = []byte("baz1")
				default:
					t.Fatalf("Unexpected topic: %v", topic)
				}
				if (flags & mqttPubFlagQoS) != expQOS {
					t.Fatalf("Expected flags to have QOS %x set, got %x", expQOS, flags)
				}
				if string(payload) != string(expPayload) {
					t.Fatalf("Expected payload to be %q, got %v", expPayload, string(payload))
				}
			}
			testMQTTExpectNothing(t, r)
		}
	}

	t.Run("one sub many topics", runSingleSubscribe([]*mqttFilter{
		{filter: "moo/foo", qos: 1},
		{filter: "moo/baz", qos: 1},
		{filter: "moo/bar", qos: 1},
	}))

	t.Run("one sub wildcard plus", runSingleSubscribe([]*mqttFilter{
		{filter: "moo/+", qos: 1},
	}))

	t.Run("one sub wildcard hash", runSingleSubscribe([]*mqttFilter{
		{filter: "moo/#", qos: 1},
	}))

	testMQTTDisconnect(t, c, nil)
	c.Close()
}

func TestMQTTRetainedMsgCleanup(t *testing.T) {
	mqttRetainedCacheTTL = 250 * time.Millisecond
	defer func() { mqttRetainedCacheTTL = mqttDefaultRetainedCacheTTL }()

	o := testMQTTDefaultOptions()
	s := testMQTTRunServer(t, o)
	defer testMQTTShutdownServer(s)

	ci := &mqttConnInfo{clientID: "cache", cleanSess: true}
	c, r := testMQTTConnect(t, ci, o.MQTT.Host, o.MQTT.Port)
	defer c.Close()
	testMQTTCheckConnAck(t, r, mqttConnAckRCConnectionAccepted, false)

	// Send a retained message.
	testMQTTPublish(t, c, r, 1, false, true, "foo", 1, []byte("msg"))
	testMQTTFlush(t, c, nil, r)

	// Start a subscription.
	testMQTTSub(t, 1, c, r, []*mqttFilter{{filter: "foo", qos: 1}}, []byte{1})
	testMQTTCheckPubMsg(t, c, r, "foo", mqttPubQos1|mqttPubFlagRetain, []byte("msg"))

	time.Sleep(2 * mqttRetainedCacheTTL)

	// Make sure not in cache anymore
	asm := testMQTTGetAccountSessionManager(t, s, "cache")
	if v, ok := asm.rmsCache.Load("foo"); ok {
		t.Fatalf("Should not be in cache, got %+v", v)
	}
}

func TestMQTTRestoreRetainedMsgs(t *testing.T) {
	o := testMQTTDefaultOptions()
	s := testMQTTRunServer(t, o)
	defer testMQTTShutdownServer(s)

	ci := &mqttConnInfo{clientID: "retain", cleanSess: true}
	c, r := testMQTTConnect(t, ci, o.MQTT.Host, o.MQTT.Port)
	defer c.Close()
	testMQTTCheckConnAck(t, r, mqttConnAckRCConnectionAccepted, false)

	// Send several retained messages on different topic
	testMQTTPublish(t, c, r, 1, false, true, "foo", 1, []byte("msg1"))
	testMQTTPublish(t, c, r, 1, false, true, "bar", 1, []byte("msg2"))
	testMQTTPublish(t, c, r, 1, false, true, "baz", 1, []byte("msg3"))

	// Remove the two last ones (by sending empty body)
	testMQTTPublish(t, c, r, 1, false, true, "bar", 1, []byte(""))
	testMQTTPublish(t, c, r, 1, false, true, "baz", 1, []byte(""))
	testMQTTFlush(t, c, nil, r)
	testMQTTDisconnect(t, c, nil)
	c.Close()

	// Now restart the server. We had a bug where we would wait to restore retained
	// messages based on stream last sequence, which was wrong.
	s.Shutdown()
	s, err := NewServer(o)
	require_NoError(t, err)
	l := &captureWarnLogger{warn: make(chan string, 10)}
	s.SetLogger(l, false, false)
	s.Start()
	defer testMQTTShutdownServer(s)

	time.Sleep(500 * time.Millisecond)
	start := time.Now()
	c, r = testMQTTConnect(t, ci, o.MQTT.Host, o.MQTT.Port)
	defer c.Close()
	testMQTTCheckConnAck(t, r, mqttConnAckRCConnectionAccepted, false)
	dur := time.Since(start)
	if dur > 2*time.Second {
		var warnMsg string
		select {
		case warnMsg = <-l.warn:
		default:
		}
		t.Fatalf("Likely timing out restoring retained messages (%s)", warnMsg)
	}
}

func TestMQTTConnAckFirstPacket(t *testing.T) {
	o := testMQTTDefaultOptions()
	o.NoLog, o.Debug, o.Trace = true, false, false
	s := RunServer(o)
	defer testMQTTShutdownServer(s)

	cisub := &mqttConnInfo{clientID: "sub", cleanSess: false}
	c, r := testMQTTConnect(t, cisub, o.MQTT.Host, o.MQTT.Port)
	defer c.Close()
	testMQTTCheckConnAck(t, r, mqttConnAckRCConnectionAccepted, false)
	testMQTTSub(t, 1, c, r, []*mqttFilter{{filter: "foo", qos: 0}}, []byte{0})
	testMQTTDisconnect(t, c, nil)
	c.Close()

	nc := natsConnect(t, s.ClientURL())
	defer nc.Close()

	wg := sync.WaitGroup{}
	wg.Add(1)
	ch := make(chan struct{}, 1)
	ready := make(chan struct{})
	go func() {
		defer wg.Done()

		close(ready)
		for {
			nc.Publish("foo", []byte("msg"))
			select {
			case <-ch:
				return
			default:
			}
		}
	}()

	<-ready
	for i := 0; i < 100; i++ {
		c, r = testMQTTConnect(t, cisub, o.MQTT.Host, o.MQTT.Port)
		defer c.Close()
		testMQTTCheckConnAck(t, r, mqttConnAckRCConnectionAccepted, true)
		w := newMQTTWriter(0)
		w.WriteByte(mqttPacketDisconnect)
		w.WriteByte(0)
		c.Write(w.Bytes())
		// Wait to be disconnected, we can't use testMQTTDisconnect() because
		// it would fail because we may still receive some NATS messages.
		var b [10]byte
		for {
			if _, err := c.Read(b[:]); err != nil {
				break
			}
		}
		c.Close()
	}
	close(ch)
	wg.Wait()
}

func TestMQTTRedeliveryAckWait(t *testing.T) {
	o := testMQTTDefaultOptions()
	o.MQTT.AckWait = 250 * time.Millisecond
	s := testMQTTRunServer(t, o)
	defer testMQTTShutdownServer(s)

	cisub := &mqttConnInfo{clientID: "sub", cleanSess: false}
	c, r := testMQTTConnect(t, cisub, o.MQTT.Host, o.MQTT.Port)
	defer c.Close()
	testMQTTCheckConnAck(t, r, mqttConnAckRCConnectionAccepted, false)
	testMQTTSub(t, 1, c, r, []*mqttFilter{{filter: "foo", qos: 1}}, []byte{1})

	cipub := &mqttConnInfo{clientID: "pub", cleanSess: true}
	cp, rp := testMQTTConnect(t, cipub, o.MQTT.Host, o.MQTT.Port)
	defer cp.Close()
	testMQTTCheckConnAck(t, rp, mqttConnAckRCConnectionAccepted, false)

	testMQTTPublish(t, cp, rp, 1, false, false, "foo", 1, []byte("foo1"))
	testMQTTPublish(t, cp, rp, 1, false, false, "foo", 2, []byte("foo2"))
	testMQTTDisconnect(t, cp, nil)
	cp.Close()

	for i := 0; i < 2; i++ {
		flags := mqttPubQos1
		if i > 0 {
			flags |= mqttPubFlagDup
		}
		pi1 := testMQTTCheckPubMsgNoAck(t, c, r, "foo", flags, []byte("foo1"))
		pi2 := testMQTTCheckPubMsgNoAck(t, c, r, "foo", flags, []byte("foo2"))

		if pi1 != 1 || pi2 != 2 {
			t.Fatalf("Unexpected pi values: %v, %v", pi1, pi2)
		}
	}
	// Ack first message
	testMQTTSendPIPacket(mqttPacketPubAck, t, c, 1)
	// Redelivery should only be for second message now
	for i := 0; i < 2; i++ {
		flags := mqttPubQos1 | mqttPubFlagDup
		pi := testMQTTCheckPubMsgNoAck(t, c, r, "foo", flags, []byte("foo2"))
		if pi != 2 {
			t.Fatalf("Unexpected pi to be 2, got %v", pi)
		}
	}

	// Restart client, should receive second message with pi==2
	c.Close()
	c, r = testMQTTConnect(t, cisub, o.MQTT.Host, o.MQTT.Port)
	defer c.Close()
	testMQTTCheckConnAck(t, r, mqttConnAckRCConnectionAccepted, true)
	// Check that message is received with proper pi
	pi := testMQTTCheckPubMsgNoAck(t, c, r, "foo", mqttPubQos1|mqttPubFlagDup, []byte("foo2"))
	if pi != 2 {
		t.Fatalf("Unexpected pi to be 2, got %v", pi)
	}
	// Now ack second message
	testMQTTSendPIPacket(mqttPacketPubAck, t, c, 2)
	// Flush to make sure it is processed before checking client's maps
	testMQTTFlush(t, c, nil, r)

	// Look for the sub client
	mc := testMQTTGetClient(t, s, "sub")
	mc.mu.Lock()
	sess := mc.mqtt.sess
	sess.mu.Lock()
	lpi := len(sess.pendingPublish)
	var lsseq int
	for _, sseqToPi := range sess.cpending {
		lsseq += len(sseqToPi)
	}
	sess.mu.Unlock()
	mc.mu.Unlock()
	if lpi != 0 || lsseq != 0 {
		t.Fatalf("Maps should be empty, got %v, %v", lpi, lsseq)
	}
}

// [MQTT-2.3.1-4] A QoS 1/2 packet identifier must not be reused while it (or an
// earlier one) is still in flight, and clients treat a just-freed identifier
// re-appearing within a delivery burst as a duplicate. Deliver-and-ack one
// message so pendingPublish momentarily empties, then deliver a second: its
// packet identifier must advance rather than reset to the first one.
func TestMQTTPacketIdentifierMonotonicAfterAck(t *testing.T) {
	o := testMQTTDefaultOptions()
	s := testMQTTRunServer(t, o)
	defer testMQTTShutdownServer(s)

	cisub := &mqttConnInfo{clientID: "sub", cleanSess: true}
	c, r := testMQTTConnect(t, cisub, o.MQTT.Host, o.MQTT.Port)
	defer c.Close()
	testMQTTCheckConnAck(t, r, mqttConnAckRCConnectionAccepted, false)
	testMQTTSub(t, 1, c, r, []*mqttFilter{{filter: "foo", qos: 1}}, []byte{1})

	cipub := &mqttConnInfo{clientID: "pub", cleanSess: true}
	cp, rp := testMQTTConnect(t, cipub, o.MQTT.Host, o.MQTT.Port)
	defer cp.Close()
	testMQTTCheckConnAck(t, rp, mqttConnAckRCConnectionAccepted, false)

	// First message: receive it, then PUBACK so pendingPublish drains back to
	// empty before the next delivery is assigned an identifier.
	testMQTTPublish(t, cp, rp, 1, false, false, "foo", 1, []byte("msg1"))
	pi1 := testMQTTCheckPubMsgNoAck(t, c, r, "foo", mqttPubQos1, []byte("msg1"))
	testMQTTSendPIPacket(mqttPacketPubAck, t, c, pi1)
	testMQTTFlush(t, c, nil, r)

	// Second message: its identifier must not reuse the freed one.
	testMQTTPublish(t, cp, rp, 1, false, false, "foo", 1, []byte("msg2"))
	pi2 := testMQTTCheckPubMsgNoAck(t, c, r, "foo", mqttPubQos1, []byte("msg2"))
	if pi2 == pi1 {
		t.Fatalf("Packet identifier was reset and reused after ack: pi1=%v pi2=%v", pi1, pi2)
	}
	testMQTTSendPIPacket(mqttPacketPubAck, t, c, pi2)
}

// Re-subscribing a QoS 1/2 filter at QoS 0 deletes its JS consumer; the
// consumer's pending (unacknowledged) deliveries must be purged too, otherwise
// their packet identifiers leak and permanently count against the in-flight cap
// for the life of the session (as mqttProcessUnsubs already does). Same for
// in-flight retained deliveries, which have no JS consumer at all.
func TestMQTTQoS0DowngradePurgesPending(t *testing.T) {
	o := testMQTTDefaultOptions()
	s := testMQTTRunServer(t, o)
	defer testMQTTShutdownServer(s)

	cipub := &mqttConnInfo{clientID: "pub", cleanSess: true}
	cp, rp := testMQTTConnect(t, cipub, o.MQTT.Host, o.MQTT.Port)
	defer cp.Close()
	testMQTTCheckConnAck(t, rp, mqttConnAckRCConnectionAccepted, false)

	// Store a retained QoS 1 message before the subscriber connects.
	testMQTTPublish(t, cp, rp, 1, false, true, "foo", 1, []byte("retained"))

	ci := &mqttConnInfo{clientID: "sub", cleanSess: true}
	c, r := testMQTTConnect(t, ci, o.MQTT.Host, o.MQTT.Port)
	defer c.Close()
	testMQTTCheckConnAck(t, r, mqttConnAckRCConnectionAccepted, false)

	// Subscribing at QoS 1 delivers the retained message; receive it without
	// acking: tracked as pending under the sid's retained pseudo-durable.
	testMQTTSub(t, 1, c, r, []*mqttFilter{{filter: "foo", qos: 1}}, []byte{1})
	testMQTTCheckPubMsgNoAck(t, c, r, "foo", mqttPubQos1|mqttPubFlagRetain, []byte("retained"))

	// Deliver a QoS 1 message and receive it without acking: it is now tracked
	// as pending against the "foo" consumer.
	testMQTTPublish(t, cp, rp, 1, false, false, "foo", 2, []byte("msg"))
	testMQTTCheckPubMsgNoAck(t, c, r, "foo", mqttPubQos1, []byte("msg"))

	countPending := func() (int, int) {
		mc := testMQTTGetClient(t, s, "sub")
		mc.mu.Lock()
		sess := mc.mqtt.sess
		mc.mu.Unlock()
		sess.mu.Lock()
		defer sess.mu.Unlock()
		var np, nc int
		np = len(sess.pendingPublish)
		for _, m := range sess.cpending {
			nc += len(m)
		}
		return np, nc
	}

	if np, nc := countPending(); np != 2 || nc != 2 {
		t.Fatalf("Expected 2 pending publish/cpending before downgrade, got %v/%v", np, nc)
	}

	// Re-subscribe the same filter at QoS 0: deletes the JS consumer and must
	// purge its pending deliveries, consumer-based and retained alike.
	testMQTTSub(t, 1, c, r, []*mqttFilter{{filter: "foo", qos: 0}}, []byte{0})
	// The new subscription re-delivers the retained message, now at QoS 0
	// (no packet identifier, so nothing new is tracked).
	testMQTTCheckPubMsgNoAck(t, c, r, "foo", mqttPubFlagRetain, []byte("retained"))
	testMQTTFlush(t, c, nil, r)

	if np, nc := countPending(); np != 0 || nc != 0 {
		t.Fatalf("Expected pending maps purged after QoS 0 downgrade, got pendingPublish=%v cpending=%v", np, nc)
	}
}

// - [MQTT-3.10.4-3] If a Server deletes a Subscription It MUST complete the
// delivery of any QoS 1 or QoS 2 messages which it has started to send to the
// Client.
//
// Test flow:
//   - Subscribe to foo, publish 3 QoS2 messages.
//   - After one is PUBCOMP-ed, and one is PUBREC-ed, Unsubscribe.
//   - See that the remaining 2 are fully delivered.
func TestMQTTQoS2InflightMsgsDeliveredAfterUnsubscribe(t *testing.T) {
	// This test has proven flaky on Travis, so skip for now. Line 4926, the 3rd
	// testMQTTCheckPubMsgNoAck sometimes returns `data1`, instead of `data3`
	// that we are expecting. It must be a retry since we did not acknowledge
	// `data1` until later.
	t.Skip()

	o := testMQTTDefaultOptions()
	o.MQTT.AckWait = 10 * time.Millisecond
	s := testMQTTRunServer(t, o)
	defer testMQTTShutdownServer(s)

	var qos2 byte = 2
	cisub := &mqttConnInfo{clientID: "sub", cleanSess: true}
	c, r := testMQTTConnect(t, cisub, o.MQTT.Host, o.MQTT.Port)
	defer c.Close()
	testMQTTCheckConnAck(t, r, mqttConnAckRCConnectionAccepted, false)
	testMQTTSub(t, 1, c, r, []*mqttFilter{{filter: "foo", qos: qos2}}, []byte{qos2})

	cipub := &mqttConnInfo{clientID: "pub", cleanSess: true}
	cp, rp := testMQTTConnect(t, cipub, o.MQTT.Host, o.MQTT.Port)
	testMQTTCheckConnAck(t, rp, mqttConnAckRCConnectionAccepted, false)

	// send 3 messages
	testMQTTPublish(t, cp, rp, qos2, false, false, "foo", 441, []byte("data1"))
	testMQTTPublish(t, cp, rp, qos2, false, false, "foo", 442, []byte("data2"))
	testMQTTPublish(t, cp, rp, qos2, false, false, "foo", 443, []byte("data3"))

	testMQTTDisconnect(t, cp, nil)
	cp.Close()

	subPI1 := testMQTTCheckPubMsgNoAck(t, c, r, "foo", mqttPubQoS2, []byte("data1"))
	subPI2 := testMQTTCheckPubMsgNoAck(t, c, r, "foo", mqttPubQoS2, []byte("data2"))
	// subPI3 := testMQTTCheckPubMsgNoAck(t, c, r, "foo", mqttPubQoS2, []byte("data3"))
	_ = testMQTTCheckPubMsgNoAck(t, c, r, "foo", mqttPubQoS2, []byte("data3"))

	// fully receive first message
	testMQTTSendPIPacket(mqttPacketPubRec, t, c, subPI1)
	testMQTTReadPIPacket(mqttPacketPubRel, t, r, subPI1)
	testMQTTSendPIPacket(mqttPacketPubComp, t, c, subPI1)

	// Do not PUBCOMP the 2nd message yet.
	testMQTTSendPIPacket(mqttPacketPubRec, t, c, subPI2)
	testMQTTReadPIPacket(mqttPacketPubRel, t, r, subPI2)

	// Unsubscribe
	testMQTTUnsub(t, 1, c, r, []*mqttFilter{{filter: "foo", qos: qos2}})

	// We expect that PI2 and PI3 will continue to be delivered, from their
	// respective states.
	gotPI2PubRel := false

	// TODO: Currently, we do not get the unacknowledged PUBLISH re-delivered
	// after an UNSUBSCRIBE. Ongoing discussion if we should/must.
	// gotPI3Publish := false
	// gotPI3PubRel := false
	for !gotPI2PubRel /* || !gotPI3Publish || !gotPI3PubRel */ {
		b, _ /* len */ := testMQTTReadPacket(t, r)
		switch b & mqttPacketMask {
		case mqttPacketPubRel:
			pi, err := r.readUint16("packet identifier")
			if err != nil {
				t.Fatalf("got unexpected error: %v", err)
			}
			switch pi {
			case subPI2:
				testMQTTSendPIPacket(mqttPacketPubComp, t, c, pi)
				gotPI2PubRel = true
			// case subPI3:
			// 	testMQTTSendPIPacket(mqttPacketPubComp, t, c, pi)
			// 	gotPI3PubRel = true
			default:
				t.Fatalf("Expected PI %v got: %v", subPI2, pi)
			}

		// case mqttPacketPub:
		// 	_, pi, _ := testMQTTGetPubMsgExEx(t, c, r, b, len, "foo", []byte("data3"))
		// 	if pi != subPI3 {
		// 		t.Fatalf("Expected PI %v got: %v", subPI3, pi)
		// 	}
		// 	gotPI3Publish = true
		// 	testMQTTSendPIPacket(mqttPacketPubRec, t, c, subPI3)

		default:
			t.Fatalf("Unexpected packet type: %v", b&mqttPacketMask)
		}
	}

	testMQTTExpectNothing(t, r)
}

func TestMQTTQoS2RejectPublishDuplicates(t *testing.T) {
	o := testMQTTDefaultOptions()
	s := testMQTTRunServer(t, o)
	defer testMQTTShutdownServer(s)

	var qos2 byte = 2
	cisub := &mqttConnInfo{clientID: "sub", cleanSess: true}
	c, r := testMQTTConnect(t, cisub, o.MQTT.Host, o.MQTT.Port)
	defer c.Close()
	testMQTTCheckConnAck(t, r, mqttConnAckRCConnectionAccepted, false)
	testMQTTSub(t, 1, c, r, []*mqttFilter{{filter: "foo", qos: qos2}}, []byte{qos2})

	cipub := &mqttConnInfo{clientID: "pub", cleanSess: true}
	cp, rp := testMQTTConnect(t, cipub, o.MQTT.Host, o.MQTT.Port)
	defer cp.Close()
	testMQTTCheckConnAck(t, rp, mqttConnAckRCConnectionAccepted, false)

	// Publish 3 different with same PI before we get any PUBREC back, then
	// complete the PUBREL/PUBCOMP flow as needed. Only one message (first
	// payload) should be delivered. PUBRECs,
	var pubPI uint16 = 444
	testMQTTSendPublishPacket(t, cp, qos2, false, false, "foo", pubPI, []byte("data1"))
	testMQTTSendPublishPacket(t, cp, qos2, true, false, "foo", pubPI, []byte("data2"))
	testMQTTSendPublishPacket(t, cp, qos2, false, false, "foo", pubPI, []byte("data3"))

	for i := 0; i < 3; i++ {
		// [MQTT-4.3.3-1] The receiver
		//
		// - MUST respond with a PUBREC containing the Packet Identifier from
		// the incoming PUBLISH Packet, having accepted ownership of the
		// Application Message.
		//
		// - Until it has received the corresponding PUBREL packet, the Receiver
		// MUST acknowledge any subsequent PUBLISH packet with the same Packet
		// Identifier by sending a PUBREC. It MUST NOT cause duplicate messages
		// to be delivered to any onward recipients in this case.
		testMQTTReadPIPacket(mqttPacketPubRec, t, rp, pubPI)
	}
	for i := 0; i < 3; i++ {
		testMQTTSendPIPacket(mqttPacketPubRel|0x2, t, cp, pubPI)
	}
	for i := 0; i < 3; i++ {
		// [MQTT-4.3.3-1] MUST respond to a PUBREL packet by sending a PUBCOMP
		// packet containing the same Packet Identifier as the PUBREL.
		testMQTTReadPIPacket(mqttPacketPubComp, t, rp, pubPI)
	}

	// [MQTT-4.3.3-1] After it has sent a PUBCOMP, the receiver MUST treat any
	// subsequent PUBLISH packet that contains that Packet Identifier as being a
	// new publication.
	//
	// Publish another message, identical to the first one. Since the server
	// already sent us a PUBCOMP, it will deliver this message, for a total of 2
	// delivered.
	testMQTTPublish(t, cp, rp, qos2, false, false, "foo", pubPI, []byte("data5"))

	testMQTTDisconnect(t, cp, nil)
	cp.Close()

	// Verify we got a total of 2 messages.
	subPI1 := testMQTTCheckPubMsgNoAck(t, c, r, "foo", mqttPubQoS2, []byte("data1"))
	subPI2 := testMQTTCheckPubMsgNoAck(t, c, r, "foo", mqttPubQoS2, []byte("data5"))
	for _, pi := range []uint16{subPI1, subPI2} {
		testMQTTSendPIPacket(mqttPacketPubRec, t, c, pi)
		testMQTTReadPIPacket(mqttPacketPubRel, t, r, pi)
		testMQTTSendPIPacket(mqttPacketPubComp, t, c, pi)
	}
	testMQTTExpectNothing(t, r)
}

func TestMQTTQoS2RetriesPublish(t *testing.T) {
	o := testMQTTDefaultOptions()
	o.MQTT.AckWait = 100 * time.Millisecond
	s := testMQTTRunServer(t, o)
	defer testMQTTShutdownServer(s)

	var qos2 byte = 2
	cisub := &mqttConnInfo{clientID: "sub", cleanSess: true}
	c, r := testMQTTConnect(t, cisub, o.MQTT.Host, o.MQTT.Port)
	defer c.Close()
	testMQTTCheckConnAck(t, r, mqttConnAckRCConnectionAccepted, false)
	testMQTTSub(t, 1, c, r, []*mqttFilter{{filter: "foo", qos: qos2}}, []byte{qos2})

	cipub := &mqttConnInfo{clientID: "pub", cleanSess: true}
	cp, rp := testMQTTConnect(t, cipub, o.MQTT.Host, o.MQTT.Port)
	defer cp.Close()
	testMQTTCheckConnAck(t, rp, mqttConnAckRCConnectionAccepted, false)

	// Publish a message and close the pub connection.
	var pubPI uint16 = 444
	testMQTTPublish(t, cp, rp, qos2, false, false, "foo", pubPI, []byte("data1"))
	testMQTTDisconnect(t, cp, nil)
	cp.Close()

	// See that we got the message delivered to the sub, but don't PUBREC it
	// yet.
	subPI := testMQTTCheckPubMsgNoAck(t, c, r, "foo", mqttPubQoS2, []byte("data1"))

	// See that the message is redelivered again 2 times, with the DUP on, before we PUBREC it.
	for i := 0; i < 2; i++ {
		expectedFlags := mqttPubQoS2 | mqttPubFlagDup
		pi := testMQTTCheckPubMsgNoAck(t, c, r, "foo", expectedFlags, []byte("data1"))
		if pi != subPI {
			t.Fatalf("Expected pi to be %v, got %v", subPI, pi)
		}
	}

	// Finish the exchange and make sure there are no more attempts.
	testMQTTSendPIPacket(mqttPacketPubRec, t, c, subPI)
	testMQTTReadPIPacket(mqttPacketPubRel, t, r, subPI)
	testMQTTSendPIPacket(mqttPacketPubComp, t, c, subPI)
	testMQTTExpectNothing(t, r)
}

func TestMQTTQoS2RetriesPubRel(t *testing.T) {
	o := testMQTTDefaultOptions()
	o.MQTT.AckWait = 50 * time.Millisecond
	s := testMQTTRunServer(t, o)
	defer testMQTTShutdownServer(s)

	var qos2 byte = 2
	cisub := &mqttConnInfo{clientID: "sub", cleanSess: true}
	c, r := testMQTTConnect(t, cisub, o.MQTT.Host, o.MQTT.Port)
	defer c.Close()
	testMQTTCheckConnAck(t, r, mqttConnAckRCConnectionAccepted, false)
	testMQTTSub(t, 1, c, r, []*mqttFilter{{filter: "foo", qos: qos2}}, []byte{qos2})

	cipub := &mqttConnInfo{clientID: "pub", cleanSess: true}
	cp, rp := testMQTTConnect(t, cipub, o.MQTT.Host, o.MQTT.Port)
	defer cp.Close()
	testMQTTCheckConnAck(t, rp, mqttConnAckRCConnectionAccepted, false)

	// Publish a message and close the pub connection.
	var pubPI uint16 = 444
	testMQTTPublish(t, cp, rp, qos2, false, false, "foo", pubPI, []byte("data1"))
	testMQTTDisconnect(t, cp, nil)
	cp.Close()

	// See that we got the message delivered to the sub, PUBREC it and expect a
	// PUBREL from the server.
	subPI := testMQTTCheckPubMsgNoAck(t, c, r, "foo", mqttPubQoS2, []byte("data1"))
	testMQTTSendPIPacket(mqttPacketPubRec, t, c, subPI)

	// See that we get PUBREL redelivered several times, there's no DUP flag to
	// check.
	testMQTTReadPIPacket(mqttPacketPubRel, t, r, subPI)
	testMQTTReadPIPacket(mqttPacketPubRel, t, r, subPI)
	testMQTTReadPIPacket(mqttPacketPubRel, t, r, subPI)

	// Finish the exchange and make sure there are no more attempts.
	testMQTTSendPIPacket(mqttPacketPubComp, t, c, subPI)
	testMQTTExpectNothing(t, r)
}

func TestMQTTAckWaitConfigChange(t *testing.T) {
	o := testMQTTDefaultOptions()
	o.MQTT.AckWait = 250 * time.Millisecond
	s := testMQTTRunServer(t, o)
	defer testMQTTShutdownRestartedServer(&s)

	dir := strings.TrimSuffix(s.JetStreamConfig().StoreDir, JetStreamStoreDir)

	cisub := &mqttConnInfo{clientID: "sub", cleanSess: false}
	c, r := testMQTTConnect(t, cisub, o.MQTT.Host, o.MQTT.Port)
	defer c.Close()
	testMQTTCheckConnAck(t, r, mqttConnAckRCConnectionAccepted, false)
	testMQTTSub(t, 1, c, r, []*mqttFilter{{filter: "foo", qos: 1}}, []byte{1})

	sendMsg := func(topic, payload string) {
		t.Helper()
		cipub := &mqttConnInfo{clientID: "pub", cleanSess: true}
		cp, rp := testMQTTConnect(t, cipub, o.MQTT.Host, o.MQTT.Port)
		defer cp.Close()
		testMQTTCheckConnAck(t, rp, mqttConnAckRCConnectionAccepted, false)

		testMQTTPublish(t, cp, rp, 1, false, false, topic, 1, []byte(payload))
		testMQTTDisconnect(t, cp, nil)
		cp.Close()
	}
	sendMsg("foo", "msg1")

	for i := 0; i < 2; i++ {
		flags := mqttPubQos1
		if i > 0 {
			flags |= mqttPubFlagDup
		}
		testMQTTCheckPubMsgNoAck(t, c, r, "foo", flags, []byte("msg1"))
	}

	// Restart the server with a different AckWait option value.
	// Verify that MQTT sub restart succeeds. It will keep the
	// original value.
	c.Close()
	s.Shutdown()

	o.Port = -1
	o.MQTT.Port = -1
	o.MQTT.AckWait = 10 * time.Millisecond
	o.StoreDir = dir
	s = testMQTTRunServer(t, o)
	// There is already the defer for shutdown at top of function

	c, r = testMQTTConnect(t, cisub, o.MQTT.Host, o.MQTT.Port)
	defer c.Close()
	testMQTTCheckConnAck(t, r, mqttConnAckRCConnectionAccepted, true)
	testMQTTCheckPubMsgNoAck(t, c, r, "foo", mqttPubQos1|mqttPubFlagDup, []byte("msg1"))
	start := time.Now()
	testMQTTCheckPubMsgNoAck(t, c, r, "foo", mqttPubQos1|mqttPubFlagDup, []byte("msg1"))
	if dur := time.Since(start); dur < 200*time.Millisecond {
		t.Fatalf("AckWait seem to have changed for existing subscription: %v", dur)
	}

	// Create new subscription
	testMQTTSub(t, 1, c, r, []*mqttFilter{{filter: "bar", qos: 1}}, []byte{1})
	sendMsg("bar", "msg2")
	testMQTTCheckPubMsgNoAck(t, c, r, "bar", mqttPubQos1, []byte("msg2"))
	start = time.Now()
	testMQTTCheckPubMsgNoAck(t, c, r, "bar", mqttPubQos1|mqttPubFlagDup, []byte("msg2"))
	if dur := time.Since(start); dur > 50*time.Millisecond {
		t.Fatalf("AckWait new value not used by new sub: %v", dur)
	}
	c.Close()
}

func TestMQTTUnsubscribeWithPendingAcks(t *testing.T) {
	o := testMQTTDefaultOptions()
	o.MQTT.AckWait = 250 * time.Millisecond
	s := testMQTTRunServer(t, o)
	defer testMQTTShutdownServer(s)

	cisub := &mqttConnInfo{clientID: "sub", cleanSess: false}
	c, r := testMQTTConnect(t, cisub, o.MQTT.Host, o.MQTT.Port)
	defer c.Close()
	testMQTTCheckConnAck(t, r, mqttConnAckRCConnectionAccepted, false)
	testMQTTSub(t, 1, c, r, []*mqttFilter{{filter: "foo", qos: 1}}, []byte{1})

	cipub := &mqttConnInfo{clientID: "pub", cleanSess: true}
	cp, rp := testMQTTConnect(t, cipub, o.MQTT.Host, o.MQTT.Port)
	defer cp.Close()
	testMQTTCheckConnAck(t, rp, mqttConnAckRCConnectionAccepted, false)

	testMQTTPublish(t, cp, rp, 1, false, false, "foo", 1, []byte("msg"))
	testMQTTDisconnect(t, cp, nil)
	cp.Close()

	for i := 0; i < 2; i++ {
		flags := mqttPubQos1
		if i > 0 {
			flags |= mqttPubFlagDup
		}
		testMQTTCheckPubMsgNoAck(t, c, r, "foo", flags, []byte("msg"))
	}

	testMQTTUnsub(t, 1, c, r, []*mqttFilter{{filter: "foo"}})
	testMQTTFlush(t, c, nil, r)

	mc := testMQTTGetClient(t, s, "sub")
	mc.mu.Lock()
	sess := mc.mqtt.sess
	sess.mu.Lock()
	pal := len(sess.pendingPublish)
	sess.mu.Unlock()
	mc.mu.Unlock()
	if pal != 0 {
		t.Fatalf("Expected pending ack map to be empty, got %v", pal)
	}
}

func TestMQTTMaxAckPending(t *testing.T) {
	o := testMQTTDefaultOptions()
	o.MQTT.MaxAckPending = 1
	s := testMQTTRunServer(t, o)
	defer testMQTTShutdownRestartedServer(&s)

	nc, js := jsClientConnect(t, s)
	defer nc.Close()

	dir := strings.TrimSuffix(s.JetStreamConfig().StoreDir, JetStreamStoreDir)

	cisub := &mqttConnInfo{clientID: "sub", cleanSess: false}
	c, r := testMQTTConnect(t, cisub, o.MQTT.Host, o.MQTT.Port)
	defer c.Close()
	testMQTTCheckConnAck(t, r, mqttConnAckRCConnectionAccepted, false)
	testMQTTSub(t, 1, c, r, []*mqttFilter{{filter: "foo", qos: 1}}, []byte{1})

	cipub := &mqttConnInfo{clientID: "pub", cleanSess: true}
	cp, rp := testMQTTConnect(t, cipub, o.MQTT.Host, o.MQTT.Port)
	defer cp.Close()
	testMQTTCheckConnAck(t, rp, mqttConnAckRCConnectionAccepted, false)

	testMQTTPublish(t, cp, rp, 1, false, false, "foo", 1, []byte("msg1"))
	testMQTTPublish(t, cp, rp, 1, false, false, "foo", 1, []byte("msg2"))

	pi := testMQTTCheckPubMsgNoAck(t, c, r, "foo", mqttPubQos1, []byte("msg1"))
	// Check that we don't receive the second one due to max ack pending
	testMQTTExpectNothing(t, r)

	// Now ack first message
	testMQTTSendPIPacket(mqttPacketPubAck, t, c, pi)
	// Now we should receive message 2
	testMQTTCheckPubMsg(t, c, r, "foo", mqttPubQos1, []byte("msg2"))
	testMQTTDisconnect(t, c, nil)
	c.Close()

	// Give a chance to the server to "close" the consumer
	checkFor(t, 2*time.Second, 15*time.Millisecond, func() error {
		for ci := range js.ConsumersInfo("$MQTT_msgs") {
			if ci.PushBound {
				return fmt.Errorf("Consumer still connected")
			}
		}
		return nil
	})

	// Send 2 messages while sub is offline
	testMQTTPublish(t, cp, rp, 1, false, false, "foo", 1, []byte("msg3"))
	testMQTTPublish(t, cp, rp, 1, false, false, "foo", 1, []byte("msg4"))

	// Restart consumer
	c, r = testMQTTConnect(t, cisub, o.MQTT.Host, o.MQTT.Port)
	defer c.Close()
	testMQTTCheckConnAck(t, r, mqttConnAckRCConnectionAccepted, true)

	// Should receive only message 3
	pi = testMQTTCheckPubMsgNoAck(t, c, r, "foo", mqttPubQos1, []byte("msg3"))
	testMQTTExpectNothing(t, r)

	// Ack and get the next
	testMQTTSendPIPacket(mqttPacketPubAck, t, c, pi)
	testMQTTCheckPubMsg(t, c, r, "foo", mqttPubQos1, []byte("msg4"))

	// Make sure this message gets ack'ed
	mcli := testMQTTGetClient(t, s, cisub.clientID)
	checkFor(t, time.Second, 15*time.Millisecond, func() error {
		mcli.mu.Lock()
		sess := mcli.mqtt.sess
		sess.mu.Lock()
		np := len(sess.pendingPublish)
		sess.mu.Unlock()
		mcli.mu.Unlock()
		if np != 0 {
			return fmt.Errorf("Still %v pending messages", np)
		}
		return nil
	})

	// Check that change to config does not prevent restart of sub.
	cp.Close()
	c.Close()
	s.Shutdown()

	o.Port = -1
	o.MQTT.Port = -1
	o.MQTT.MaxAckPending = 2
	o.StoreDir = dir
	s = testMQTTRunServer(t, o)
	// There is already the defer for shutdown at top of function

	cp, rp = testMQTTConnect(t, cipub, o.MQTT.Host, o.MQTT.Port)
	defer cp.Close()
	testMQTTCheckConnAck(t, rp, mqttConnAckRCConnectionAccepted, false)

	testMQTTPublish(t, cp, rp, 1, false, false, "foo", 1, []byte("msg5"))
	testMQTTPublish(t, cp, rp, 1, false, false, "foo", 1, []byte("msg6"))

	// Restart consumer
	c, r = testMQTTConnect(t, cisub, o.MQTT.Host, o.MQTT.Port)
	defer c.Close()
	testMQTTCheckConnAck(t, r, mqttConnAckRCConnectionAccepted, true)

	// Should receive only message 5
	pi = testMQTTCheckPubMsgNoAck(t, c, r, "foo", mqttPubQos1, []byte("msg5"))
	testMQTTExpectNothing(t, r)

	// Ack and get the next
	testMQTTSendPIPacket(mqttPacketPubAck, t, c, pi)
	testMQTTCheckPubMsg(t, c, r, "foo", mqttPubQos1, []byte("msg6"))
}

func TestMQTTMaxAckPendingForMultipleSubs(t *testing.T) {
	o := testMQTTDefaultOptions()
	o.MQTT.AckWait = time.Second
	o.MQTT.MaxAckPending = 1
	s := testMQTTRunServer(t, o)
	defer testMQTTShutdownServer(s)

	cisub := &mqttConnInfo{clientID: "sub", cleanSess: true}
	c, r := testMQTTConnect(t, cisub, o.MQTT.Host, o.MQTT.Port)
	defer c.Close()
	testMQTTCheckConnAck(t, r, mqttConnAckRCConnectionAccepted, false)
	testMQTTSub(t, 1, c, r, []*mqttFilter{{filter: "foo", qos: 1}}, []byte{1})
	testMQTTSub(t, 1, c, r, []*mqttFilter{{filter: "bar", qos: 1}}, []byte{1})

	cipub := &mqttConnInfo{clientID: "pub", cleanSess: true}
	cp, rp := testMQTTConnect(t, cipub, o.MQTT.Host, o.MQTT.Port)
	defer cp.Close()
	testMQTTCheckConnAck(t, rp, mqttConnAckRCConnectionAccepted, false)

	testMQTTPublish(t, cp, rp, 1, false, false, "foo", 1, []byte("msg1"))
	pi := testMQTTCheckPubMsgNoAck(t, c, r, "foo", mqttPubQos1, []byte("msg1"))

	// Now send a second message but on topic bar
	testMQTTPublish(t, cp, rp, 1, false, false, "bar", 1, []byte("msg2"))

	// JS allows us to limit per consumer, but we apply the limit to the
	// session, so although JS will attempt to delivery this message,
	// the MQTT code will suppress it.
	testMQTTExpectNothing(t, r)

	// Ack the first message.
	testMQTTSendPIPacket(mqttPacketPubAck, t, c, pi)

	// Now we should get the second message
	testMQTTCheckPubMsg(t, c, r, "bar", mqttPubQos1|mqttPubFlagDup, []byte("msg2"))
}

func TestMQTTMaxAckPendingOverLimit(t *testing.T) {
	o := testMQTTDefaultOptions()
	o.MQTT.MaxAckPending = 20000
	s := testMQTTRunServer(t, o)
	defer testMQTTShutdownServer(s)

	checkTMax := func(sess *mqttSession, expected int) {
		t.Helper()
		sess.mu.Lock()
		tmax := sess.tmaxack
		sess.mu.Unlock()
		if tmax != expected {
			t.Fatalf("Expected current tmax to be %v, got %v", expected, tmax)
		}
	}

	cisub := &mqttConnInfo{clientID: "sub", cleanSess: false}
	c, r := testMQTTConnect(t, cisub, o.MQTT.Host, o.MQTT.Port)
	defer c.Close()
	testMQTTCheckConnAck(t, r, mqttConnAckRCConnectionAccepted, false)

	mc := testMQTTGetClient(t, s, "sub")
	mc.mu.Lock()
	sess := mc.mqtt.sess
	mc.mu.Unlock()

	// After this one, total would be 20000
	testMQTTSub(t, 1, c, r, []*mqttFilter{{filter: "foo", qos: 1}}, []byte{1})
	checkTMax(sess, 20000)
	// This one will count for 2, so total will be 60000
	testMQTTSub(t, 1, c, r, []*mqttFilter{{filter: "bar/#", qos: 1}}, []byte{1})
	checkTMax(sess, 60000)

	// This should fail
	testMQTTSub(t, 1, c, r, []*mqttFilter{{filter: "bar", qos: 1}}, []byte{mqttSubAckFailure})
	checkTMax(sess, 60000)

	// Remove the one with wildcard
	testMQTTUnsub(t, 1, c, r, []*mqttFilter{{filter: "bar/#"}})
	checkTMax(sess, 20000)

	// Now we could add 2 more without wildcards
	testMQTTSub(t, 1, c, r, []*mqttFilter{{filter: "bar", qos: 1}}, []byte{1})
	checkTMax(sess, 40000)
	testMQTTSub(t, 1, c, r, []*mqttFilter{{filter: "baz", qos: 1}}, []byte{1})
	checkTMax(sess, 60000)

	// Again, this one should fail
	testMQTTSub(t, 1, c, r, []*mqttFilter{{filter: "bat", qos: 1}}, []byte{mqttSubAckFailure})
	checkTMax(sess, 60000)

	// Now remove all and check that we are at 0
	testMQTTUnsub(t, 1, c, r, []*mqttFilter{{filter: "foo"}})
	checkTMax(sess, 40000)
	testMQTTUnsub(t, 1, c, r, []*mqttFilter{{filter: "bar"}})
	checkTMax(sess, 20000)
	testMQTTUnsub(t, 1, c, r, []*mqttFilter{{filter: "baz"}})
	checkTMax(sess, 0)
}

func TestMQTTConfigReload(t *testing.T) {
	template := `
		jetstream: true
		server_name: mqtt
		mqtt {
			port: -1
			ack_wait: %s
			max_ack_pending: %s
		}
	`
	conf := createConfFile(t, []byte(fmt.Sprintf(template, `"5s"`, `10000`)))

	s, o := RunServerWithConfig(conf)
	defer testMQTTShutdownServer(s)

	if val := o.MQTT.AckWait; val != 5*time.Second {
		t.Fatalf("Invalid ackwait: %v", val)
	}
	if val := o.MQTT.MaxAckPending; val != 10000 {
		t.Fatalf("Invalid ackwait: %v", val)
	}

	changeCurrentConfigContentWithNewContent(t, conf, []byte(fmt.Sprintf(template, `"250ms"`, `1`)))
	if err := s.Reload(); err != nil {
		t.Fatalf("Error on reload: %v", err)
	}

	cisub := &mqttConnInfo{clientID: "sub", cleanSess: false}
	c, r := testMQTTConnect(t, cisub, o.MQTT.Host, o.MQTT.Port)
	defer c.Close()
	testMQTTCheckConnAck(t, r, mqttConnAckRCConnectionAccepted, false)
	testMQTTSub(t, 1, c, r, []*mqttFilter{{filter: "foo", qos: 1}}, []byte{1})

	cipub := &mqttConnInfo{clientID: "pub", cleanSess: true}
	cp, rp := testMQTTConnect(t, cipub, o.MQTT.Host, o.MQTT.Port)
	defer cp.Close()
	testMQTTCheckConnAck(t, rp, mqttConnAckRCConnectionAccepted, false)

	testMQTTPublish(t, cp, rp, 1, false, false, "foo", 1, []byte("msg1"))
	testMQTTPublish(t, cp, rp, 1, false, false, "foo", 1, []byte("msg2"))

	testMQTTCheckPubMsgNoAck(t, c, r, "foo", mqttPubQos1, []byte("msg1"))
	start := time.Now()
	testMQTTCheckPubMsgNoAck(t, c, r, "foo", mqttPubQos1|mqttPubFlagDup, []byte("msg1"))
	if dur := time.Since(start); dur > 500*time.Millisecond {
		t.Fatalf("AckWait not applied? dur=%v", dur)
	}
	c.Close()
	cp.Close()
	testMQTTShutdownServer(s)

	changeCurrentConfigContentWithNewContent(t, conf, []byte(fmt.Sprintf(template, `"30s"`, `1`)))
	s, o = RunServerWithConfig(conf)
	defer testMQTTShutdownServer(s)

	cisub.cleanSess = true
	c, r = testMQTTConnect(t, cisub, o.MQTT.Host, o.MQTT.Port)
	defer c.Close()
	testMQTTCheckConnAck(t, r, mqttConnAckRCConnectionAccepted, false)
	testMQTTSub(t, 1, c, r, []*mqttFilter{{filter: "foo", qos: 1}}, []byte{1})

	cipub = &mqttConnInfo{clientID: "pub", cleanSess: true}
	cp, rp = testMQTTConnect(t, cipub, o.MQTT.Host, o.MQTT.Port)
	defer cp.Close()
	testMQTTCheckConnAck(t, rp, mqttConnAckRCConnectionAccepted, false)

	testMQTTPublish(t, cp, rp, 1, false, false, "foo", 1, []byte("msg1"))
	testMQTTPublish(t, cp, rp, 1, false, false, "foo", 1, []byte("msg2"))

	testMQTTCheckPubMsgNoAck(t, c, r, "foo", mqttPubQos1, []byte("msg1"))
	testMQTTExpectNothing(t, r)

	// Increase the max ack pending
	changeCurrentConfigContentWithNewContent(t, conf, []byte(fmt.Sprintf(template, `"30s"`, `10`)))
	// Reload now
	if err := s.Reload(); err != nil {
		t.Fatalf("Error on reload: %v", err)
	}

	// Reload will have effect only on new subscriptions.
	// Create a new subscription, and we should not be able to get the 2 messages.
	testMQTTSub(t, 1, c, r, []*mqttFilter{{filter: "bar", qos: 1}}, []byte{1})

	testMQTTPublish(t, cp, rp, 1, false, false, "bar", 1, []byte("msg3"))
	testMQTTPublish(t, cp, rp, 1, false, false, "bar", 1, []byte("msg4"))

	testMQTTCheckPubMsg(t, c, r, "bar", mqttPubQos1, []byte("msg3"))
	testMQTTCheckPubMsg(t, c, r, "bar", mqttPubQos1, []byte("msg4"))
}

func TestMQTTConfigReloadKeepsQoS1DeliveryWithDefaultMaxAckPending(t *testing.T) {
	tlsMap := `tls {
		cert_file: "../test/configs/certs/tlsauth/server.pem"
		key_file: "../test/configs/certs/tlsauth/server-key.pem"
		ca_file: "../test/configs/certs/tlsauth/ca.pem"
		verify_and_map: true
		timeout: 2
	}`
	for _, test := range []struct {
		name   string
		before string
		after  string
		users  string
		cert   bool
	}{
		// With a tls block the TLS config is rebuilt on every reload, so even a
		// reload that changes nothing applies the MQTT options again (#8661).
		{"no-op reload with certificate mapped user", tlsMap, tlsMap, `users = [ { user: "CN=example.com,OU=NATS.io" } ]`, true},
		{"reload changing ack_wait", `ack_wait: "30s"`, `ack_wait: "45s"`, `users = [ { user: "u", password: "p" } ]`, false},
	} {
		t.Run(test.name, func(t *testing.T) {
			tmpl := `
				listen: 127.0.0.1:-1
				server_name: mqtt
				jetstream { store_dir: %q }
				mqtt {
					listen: 127.0.0.1:-1
					%s
				}
				authorization { %s }
			`
			dir := t.TempDir()
			conf := createConfFile(t, []byte(fmt.Sprintf(tmpl, dir, test.before, test.users)))
			s, o := RunServerWithConfig(conf)
			defer testMQTTShutdownServer(s)

			connect := func(id string) (net.Conn, *mqttReader) {
				ci := &mqttConnInfo{clientID: id, cleanSess: true}
				if test.cert {
					tlsc, err := GenTLSConfig(&TLSConfigOpts{
						CertFile: "../test/configs/certs/tlsauth/client.pem",
						KeyFile:  "../test/configs/certs/tlsauth/client-key.pem",
					})
					require_NoError(t, err)
					tlsc.InsecureSkipVerify = true
					tlsc.MinVersion = tls.VersionTLS13
					ci.tls, ci.tlsc = true, tlsc
				} else {
					ci.user, ci.pass = "u", "p"
				}
				c, r := testMQTTConnect(t, ci, o.MQTT.Host, o.MQTT.Port)
				testMQTTCheckConnAck(t, r, mqttConnAckRCConnectionAccepted, false)
				return c, r
			}
			sub, rs := connect("sub")
			defer sub.Close()
			testMQTTSub(t, 1, sub, rs, []*mqttFilter{{filter: "foo", qos: 1}}, []byte{1})
			testMQTTFlush(t, sub, nil, rs)
			pub, rp := connect("pub")
			defer pub.Close()

			testMQTTPublish(t, pub, rp, 1, false, false, "foo", 1, []byte("msg1"))
			pi := testMQTTCheckPubMsg(t, sub, rs, "foo", mqttPubQos1, []byte("msg1"))
			testMQTTSendPIPacket(mqttPacketPubAck, t, sub, pi)

			changeCurrentConfigContentWithNewContent(t, conf, []byte(fmt.Sprintf(tmpl, dir, test.after, test.users)))
			require_NoError(t, s.Reload())

			// max_ack_pending is not set, so the session must keep the default
			// limit instead of 0, which would stop every QoS 1 delivery.
			testMQTTPublish(t, pub, rp, 1, false, false, "foo", 2, []byte("msg2"))
			testMQTTCheckPubMsg(t, sub, rs, "foo", mqttPubQos1, []byte("msg2"))
		})
	}
}

func TestMQTTStreamInfoReturnsNonEmptySubject(t *testing.T) {
	o := testMQTTDefaultOptions()
	s := testMQTTRunServer(t, o)
	defer testMQTTShutdownServer(s)

	cisub := &mqttConnInfo{clientID: "sub", cleanSess: false}
	c, r := testMQTTConnect(t, cisub, o.MQTT.Host, o.MQTT.Port)
	defer c.Close()
	testMQTTCheckConnAck(t, r, mqttConnAckRCConnectionAccepted, false)

	nc := natsConnect(t, s.ClientURL())
	defer nc.Close()

	// Check that we can query all MQTT streams. MQTT streams are
	// created without subject filter, however, if we return them like this,
	// the 'nats' utility will fail to display them due to some xml validation.
	for _, sname := range []string{
		mqttStreamName,
		mqttRetainedMsgsStreamName,
	} {
		t.Run(sname, func(t *testing.T) {
			resp, err := nc.Request(fmt.Sprintf(JSApiStreamInfoT, sname), nil, time.Second)
			if err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}
			var bResp JSApiStreamInfoResponse
			if err = json.Unmarshal(resp.Data, &bResp); err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}
			if len(bResp.Config.Subjects) == 0 {
				t.Fatalf("No subject returned, which will cause nats tooling to fail: %+v", bResp.Config)
			}
		})
	}
}

func TestMQTTWebsocketToMQTTPort(t *testing.T) {
	conf := createConfFile(t, []byte(fmt.Sprintf(`
		listen: "127.0.0.1:-1"
		http: "127.0.0.1:-1"
		server_name: "mqtt"
		jetstream {
			store_dir = %q
		}
		mqtt {
			listen: "127.0.0.1:-1"
		}
		websocket {
			listen: "127.0.0.1:-1"
			no_tls: true
		}
	`, t.TempDir())))
	s, o := RunServerWithConfig(conf)
	defer testMQTTShutdownServer(s)

	l := &captureErrorLogger{errCh: make(chan string, 10)}
	s.SetLogger(l, false, false)

	ci := &mqttConnInfo{cleanSess: true, ws: true}
	if _, _, err := testMQTTConnectRetryWithError(t, ci, o.MQTT.Host, o.MQTT.Port, 0); err == nil {
		t.Fatal("Expected error during connect")
	}
	select {
	case e := <-l.errCh:
		if !strings.Contains(e, errMQTTNotWebsocketPort.Error()) {
			t.Fatalf("Unexpected error: %v", e)
		}
	case <-time.After(time.Second):
		t.Fatal("No error regarding wrong port")
	}
}

func TestMQTTWebsocket(t *testing.T) {
	tdir := t.TempDir()
	template := `
		listen: "127.0.0.1:-1"
		http: "127.0.0.1:-1"
		server_name: "mqtt"
		jetstream {
			store_dir = %q
		}
		accounts {
			MQTT {
				jetstream: enabled
				users [
					{user: "mqtt", pass: "pwd", connection_types: ["%s"%s]}
				]
			}
		}
		mqtt {
			listen: "127.0.0.1:-1"
		}
		websocket {
			listen: "127.0.0.1:-1"
			no_tls: true
		}
	`
	s, o, conf := runReloadServerWithContent(t, []byte(fmt.Sprintf(template, tdir, jwt.ConnectionTypeMqtt, "")))
	defer testMQTTShutdownServer(s)

	cisub := &mqttConnInfo{clientID: "sub", user: "mqtt", pass: "pwd", ws: true}
	c, r := testMQTTConnect(t, cisub, o.Websocket.Host, o.Websocket.Port)
	defer c.Close()
	testMQTTCheckConnAck(t, r, mqttConnAckRCNotAuthorized, false)
	c.Close()

	ws := fmt.Sprintf(`, "%s"`, jwt.ConnectionTypeMqttWS)
	reloadUpdateConfig(t, s, conf, fmt.Sprintf(template, tdir, jwt.ConnectionTypeMqtt, ws))

	cisub = &mqttConnInfo{clientID: "sub", user: "mqtt", pass: "pwd", ws: true}
	c, r = testMQTTConnect(t, cisub, o.Websocket.Host, o.Websocket.Port)
	defer c.Close()
	testMQTTCheckConnAck(t, r, mqttConnAckRCConnectionAccepted, false)

	testMQTTSub(t, 1, c, r, []*mqttFilter{{filter: "foo", qos: 1}}, []byte{1})
	testMQTTFlush(t, c, nil, r)

	cipub := &mqttConnInfo{clientID: "pub", user: "mqtt", pass: "pwd", ws: true}
	cp, rp := testMQTTConnect(t, cipub, o.Websocket.Host, o.Websocket.Port)
	defer cp.Close()
	testMQTTCheckConnAck(t, rp, mqttConnAckRCConnectionAccepted, false)

	testMQTTPublish(t, cp, rp, 1, false, false, "foo", 1, []byte("msg1"))
	testMQTTCheckPubMsg(t, c, r, "foo", mqttPubQos1, []byte("msg1"))
}

type chunkWriteConn struct {
	net.Conn
}

func (cwc *chunkWriteConn) Write(p []byte) (int, error) {
	max := len(p)
	cs := rand.IntN(max) + 1
	if cs < max {
		if pn, perr := cwc.Conn.Write(p[:cs]); perr != nil {
			return pn, perr
		}
		time.Sleep(10 * time.Millisecond)
		if pn, perr := cwc.Conn.Write(p[cs:]); perr != nil {
			return pn, perr
		}
		return len(p), nil
	}
	return cwc.Conn.Write(p)
}

func TestMQTTPartial(t *testing.T) {
	conf := createConfFile(t, []byte(fmt.Sprintf(`
		listen: "127.0.0.1:-1"
		http: "127.0.0.1:-1"
		server_name: "mqtt"
		jetstream {
			store_dir = %q
		}
		mqtt {
			listen: "127.0.0.1:-1"
		}
		websocket {
			listen: "127.0.0.1:-1"
			no_tls: true
		}
	`, t.TempDir())))
	s, o := RunServerWithConfig(conf)
	defer testMQTTShutdownServer(s)

	for _, test := range []struct {
		name string
		ws   bool
	}{
		{"standard", false},
		{"websocket", true},
	} {
		t.Run(test.name, func(t *testing.T) {
			ci := &mqttConnInfo{cleanSess: true, ws: test.ws}
			host, port := o.MQTT.Host, o.MQTT.Port
			if test.ws {
				host, port = o.Websocket.Host, o.Websocket.Port
			}
			c, r := testMQTTConnect(t, ci, host, port)
			defer c.Close()
			testMQTTCheckConnAck(t, r, mqttConnAckRCConnectionAccepted, false)
			c = &chunkWriteConn{Conn: c}

			cp, rp := testMQTTConnect(t, ci, host, port)
			defer cp.Close()
			testMQTTCheckConnAck(t, rp, mqttConnAckRCConnectionAccepted, false)
			cp = &chunkWriteConn{Conn: cp}

			subj := nuid.Next()
			testMQTTSub(t, 1, c, r, []*mqttFilter{{filter: subj, qos: 1}}, []byte{1})
			testMQTTFlush(t, c, nil, r)
			for i := 0; i < 10; i++ {
				testMQTTPublish(t, cp, rp, 1, false, false, subj, 1, []byte("msg"))
				testMQTTCheckPubMsg(t, c, r, subj, mqttPubQos1, []byte("msg"))
			}
		})
	}
}

func TestMQTTWebsocketTLS(t *testing.T) {
	conf := createConfFile(t, []byte(fmt.Sprintf(`
		listen: "127.0.0.1:-1"
		http: "127.0.0.1:-1"
		server_name: "mqtt"
		jetstream {
			store_dir = %q
		}
		mqtt {
			listen: "127.0.0.1:-1"
		}
		websocket {
			listen: "127.0.0.1:-1"
			tls {
				cert_file: '../test/configs/certs/server-cert.pem'
				key_file: '../test/configs/certs/server-key.pem'
				ca_file: '../test/configs/certs/ca.pem'
			}
		}
	`, t.TempDir())))
	s, o := RunServerWithConfig(conf)
	defer testMQTTShutdownServer(s)

	c, r := testMQTTConnect(t, &mqttConnInfo{clientID: "sub", ws: true, tls: true}, o.Websocket.Host, o.Websocket.Port)
	defer c.Close()
	testMQTTCheckConnAck(t, r, mqttConnAckRCConnectionAccepted, false)
	testMQTTSub(t, 1, c, r, []*mqttFilter{{filter: "foo", qos: 1}}, []byte{1})
	testMQTTFlush(t, c, nil, r)

	cp, rp := testMQTTConnect(t, &mqttConnInfo{clientID: "pub", ws: true, tls: true}, o.Websocket.Host, o.Websocket.Port)
	defer cp.Close()
	testMQTTCheckConnAck(t, rp, mqttConnAckRCConnectionAccepted, false)

	testMQTTPublish(t, cp, rp, 1, false, false, "foo", 1, []byte("msg1"))
	testMQTTCheckPubMsg(t, c, r, "foo", mqttPubQos1, []byte("msg1"))
}

func TestMQTTTransferSessionStreamsToMuxed(t *testing.T) {
	cl := createJetStreamClusterWithTemplate(t, testMQTTGetClusterTemplaceNoLeaf(), "MQTT", 3)
	defer cl.shutdown()

	nc, js := jsClientConnect(t, cl.randomServer())
	defer nc.Close()

	// Create 2 streams that start with "$MQTT_sess_" to check for transfer to new
	// mux'ed unique "$MQTT_sess" stream. One of this stream will not contain a
	// proper session record, and we will check that the stream does not get deleted.
	sessStreamName1 := mqttSessionsStreamNamePrefix + getHash("sub")
	if _, err := js.AddStream(&nats.StreamConfig{
		Name:     sessStreamName1,
		Subjects: []string{sessStreamName1},
		Replicas: 3,
		MaxMsgs:  1,
	}); err != nil {
		t.Fatalf("Unable to add stream: %v", err)
	}
	// Then add the session record
	ps := mqttPersistedSession{
		ID:   "sub",
		Subs: map[string]byte{"foo": 1},
		Cons: map[string]*ConsumerConfig{"foo": {
			Durable:        "d6INCtp3_cK39H5WHEtOSU7sLy2oQv3",
			DeliverSubject: "$MQTT.sub.cK39H5WHEtOSU7sLy2oQrR",
			DeliverPolicy:  DeliverNew,
			AckPolicy:      AckExplicit,
			FilterSubject:  "$MQTT.msgs.foo",
			MaxAckPending:  1024,
		}},
	}
	b, _ := json.Marshal(&ps)
	if _, err := js.Publish(sessStreamName1, b); err != nil {
		t.Fatalf("Error on publish: %v", err)
	}

	// Create the stream that has "$MQTT_sess_" prefix, but that is not really a MQTT session stream
	sessStreamName2 := mqttSessionsStreamNamePrefix + "ivan"
	if _, err := js.AddStream(&nats.StreamConfig{
		Name:     sessStreamName2,
		Subjects: []string{sessStreamName2},
		Replicas: 3,
		MaxMsgs:  1,
	}); err != nil {
		t.Fatalf("Unable to add stream: %v", err)
	}
	if _, err := js.Publish(sessStreamName2, []byte("some content")); err != nil {
		t.Fatalf("Error on publish: %v", err)
	}

	cl.waitOnStreamLeader(globalAccountName, sessStreamName1)
	cl.waitOnStreamLeader(globalAccountName, sessStreamName2)

	// Now create a real MQTT connection
	o := cl.opts[0]
	sc, sr := testMQTTConnectRetry(t, &mqttConnInfo{clientID: "sub"}, o.MQTT.Host, o.MQTT.Port, 10)
	defer sc.Close()
	testMQTTCheckConnAck(t, sr, mqttConnAckRCConnectionAccepted, true)

	// Check that old session stream is gone, but the non session stream is still present.
	var gotIt = false
	for info := range js.StreamsInfo() {
		if strings.HasPrefix(info.Config.Name, mqttSessionsStreamNamePrefix) {
			if strings.HasSuffix(info.Config.Name, "_ivan") {
				gotIt = true
			} else {
				t.Fatalf("The stream %q should have been deleted", info.Config.Name)
			}
		}
	}
	if !gotIt {
		t.Fatalf("The stream %q should not have been deleted", mqttSessionsStreamNamePrefix+"ivan")
	}

	// We want to check that the record was properly transferred.
	rmsg, err := js.GetMsg(mqttSessStreamName, 2)
	if err != nil {
		t.Fatalf("Unable to get session message: %v", err)
	}
	ps2 := &mqttPersistedSession{}
	if err := json.Unmarshal(rmsg.Data, ps2); err != nil {
		t.Fatalf("Error unpacking session record: %v", err)
	}
	if ps2.ID != "sub" {
		t.Fatalf("Unexpected session record, %+v vs %+v", ps2, ps)
	}
	if qos, ok := ps2.Subs["foo"]; !ok || qos != 1 {
		t.Fatalf("Unexpected session record, %+v vs %+v", ps2, ps)
	}
	if cons, ok := ps2.Cons["foo"]; !ok || !reflect.DeepEqual(cons, ps.Cons["foo"]) {
		t.Fatalf("Unexpected session record, %+v vs %+v", ps2, ps)
	}

	// Make sure we don't attempt to transfer again by creating a subscription
	// on the "stream names" API, which is used to get the list of streams to transfer
	sub := natsSubSync(t, nc, JSApiStreams)

	// Make sure to connect an MQTT client from a different node so that this node
	// gets a connection for the account for the first time and tries to create
	// all MQTT streams, etc..
	o = cl.opts[1]
	sc, sr = testMQTTConnectRetry(t, &mqttConnInfo{clientID: "sub2"}, o.MQTT.Host, o.MQTT.Port, 10)
	defer sc.Close()
	testMQTTCheckConnAck(t, sr, mqttConnAckRCConnectionAccepted, false)

	if _, err := sub.NextMsg(200 * time.Millisecond); err == nil {
		t.Fatal("Looks like attempt to transfer was done again")
	}
}

func TestMQTTConnectAndDisconnectEvent(t *testing.T) {
	conf := createConfFile(t, []byte(fmt.Sprintf(`
		listen: "127.0.0.1:-1"
		http: "127.0.0.1:-1"
		server_name: "mqtt"
		jetstream {
			store_dir = %q
		}
		accounts {
			MQTT {
				jetstream: enabled
				users: [{user: "mqtt", password: "pwd"}]
			}
			SYS {
				users: [{user: "sys", password: "pwd"}]
			}
		}
		mqtt {
			listen: "127.0.0.1:-1"
		}
		system_account: "SYS"
	`, t.TempDir())))
	defer os.Remove(conf)
	s, o := RunServerWithConfig(conf)
	defer testMQTTShutdownServer(s)

	nc := natsConnect(t, s.ClientURL(), nats.UserInfo("sys", "pwd"))
	defer nc.Close()

	accConn := natsSubSync(t, nc, fmt.Sprintf(connectEventSubj, "MQTT"))
	accDisc := natsSubSync(t, nc, fmt.Sprintf(disconnectEventSubj, "MQTT"))
	accAuth := natsSubSync(t, nc, fmt.Sprintf(authErrorEventSubj, s.ID()))
	natsFlush(t, nc)

	checkConnEvent := func(data []byte, expected string) {
		t.Helper()
		var ce ConnectEventMsg
		json.Unmarshal(data, &ce)
		if ce.Client.MQTTClient != expected {
			t.Fatalf("Expected client ID %q, got this connect event: %+v", expected, ce)
		}
	}
	checkDiscEvent := func(data []byte, expected string) {
		t.Helper()
		var de DisconnectEventMsg
		json.Unmarshal(data, &de)
		if de.Client.MQTTClient != expected {
			t.Fatalf("Expected client ID %q, got this disconnect event: %+v", expected, de)
		}
	}

	c1, r1 := testMQTTConnect(t, &mqttConnInfo{user: "mqtt", pass: "pwd", clientID: "conn1", cleanSess: true}, o.MQTT.Host, o.MQTT.Port)
	defer c1.Close()
	testMQTTCheckConnAck(t, r1, mqttConnAckRCConnectionAccepted, false)

	cm := natsNexMsg(t, accConn, time.Second)
	checkConnEvent(cm.Data, "conn1")

	c2, r2 := testMQTTConnect(t, &mqttConnInfo{user: "mqtt", pass: "pwd", clientID: "conn2", cleanSess: true}, o.MQTT.Host, o.MQTT.Port)
	defer c2.Close()
	testMQTTCheckConnAck(t, r2, mqttConnAckRCConnectionAccepted, false)

	cm = natsNexMsg(t, accConn, time.Second)
	checkConnEvent(cm.Data, "conn2")

	c3, r3 := testMQTTConnect(t, &mqttConnInfo{user: "mqtt", pass: "pwd", clientID: "conn3", cleanSess: true}, o.MQTT.Host, o.MQTT.Port)
	defer c3.Close()
	testMQTTCheckConnAck(t, r3, mqttConnAckRCConnectionAccepted, false)

	cm = natsNexMsg(t, accConn, time.Second)
	checkConnEvent(cm.Data, "conn3")

	testMQTTDisconnect(t, c3, nil)
	c3.Close()
	cm = natsNexMsg(t, accDisc, time.Second)
	checkDiscEvent(cm.Data, "conn3")

	// Now try a bad auth
	c4, r4 := testMQTTConnect(t, &mqttConnInfo{clientID: "conn4", cleanSess: true}, o.MQTT.Host, o.MQTT.Port)
	defer c4.Close()
	testMQTTCheckConnAck(t, r4, mqttConnAckRCNotAuthorized, false)
	// This will generate an auth error, which is a disconnect event
	cm = natsNexMsg(t, accAuth, time.Second)
	checkDiscEvent(cm.Data, "conn4")

	url := fmt.Sprintf("http://127.0.0.1:%d/", s.MonitorAddr().Port)
	for mode := 0; mode < 2; mode++ {
		c := pollConnz(t, s, mode, url+"connz", nil)
		if c.Conns == nil || len(c.Conns) != 3 {
			t.Fatalf("Expected 3 connections in array, got %v", len(c.Conns))
		}

		// Check that client ID is present
		for _, conn := range c.Conns {
			if conn.Type == clientTypeStringMap[MQTT] && conn.MQTTClient == _EMPTY_ {
				t.Fatalf("Expected a client ID to be set, got %+v", conn)
			}
		}

		// Check that we can select based on client ID:
		c = pollConnz(t, s, mode, url+"connz?mqtt_client=conn2", &ConnzOptions{MQTTClient: "conn2"})
		if c.Conns == nil || len(c.Conns) != 1 {
			t.Fatalf("Expected 1 connection in array, got %v", len(c.Conns))
		}
		if c.Conns[0].MQTTClient != "conn2" {
			t.Fatalf("Unexpected client ID: %+v", c.Conns[0])
		}

		// Check that we have the closed ones
		c = pollConnz(t, s, mode, url+"connz?state=closed", &ConnzOptions{State: ConnClosed})
		if c.Conns == nil || len(c.Conns) != 2 {
			t.Fatalf("Expected 2 connections in array, got %v", len(c.Conns))
		}
		for _, conn := range c.Conns {
			if conn.MQTTClient == _EMPTY_ {
				t.Fatalf("Expected a client ID, got %+v", conn)
			}
		}

		// Check that we can select with client ID for closed state
		c = pollConnz(t, s, mode, url+"connz?state=closed&mqtt_client=conn3", &ConnzOptions{State: ConnClosed, MQTTClient: "conn3"})
		if c.Conns == nil || len(c.Conns) != 1 {
			t.Fatalf("Expected 1 connection in array, got %v", len(c.Conns))
		}
		if c.Conns[0].MQTTClient != "conn3" {
			t.Fatalf("Unexpected client ID: %+v", c.Conns[0])
		}
		// Check that we can select with client ID for closed state (but in this case not found)
		c = pollConnz(t, s, mode, url+"connz?state=closed&mqtt_client=conn5", &ConnzOptions{State: ConnClosed, MQTTClient: "conn5"})
		if len(c.Conns) != 0 {
			t.Fatalf("Expected 0 connection in array, got %v", len(c.Conns))
		}
	}

	reply := nc.NewRespInbox()
	replySub := natsSubSync(t, nc, reply)

	// Test system events now
	for _, test := range []struct {
		opt any
		cid string
	}{
		{&ConnzOptions{MQTTClient: "conn1"}, "conn1"},
		{&ConnzOptions{MQTTClient: "conn3", State: ConnClosed}, "conn3"},
		{&ConnzOptions{MQTTClient: "conn4", State: ConnClosed}, "conn4"},
		{&ConnzOptions{MQTTClient: "conn5"}, _EMPTY_},
		{json.RawMessage(`{"mqtt_client":"conn1"}`), "conn1"},
		{json.RawMessage(fmt.Sprintf(`{"mqtt_client":"conn3", "state":%v}`, ConnClosed)), "conn3"},
		{json.RawMessage(fmt.Sprintf(`{"mqtt_client":"conn4", "state":%v}`, ConnClosed)), "conn4"},
		{json.RawMessage(`{"mqtt_client":"conn5"}`), _EMPTY_},
	} {
		t.Run("sys connz", func(t *testing.T) {
			b, _ := json.Marshal(test.opt)

			// set a header to make sure request parsing knows to ignore them
			nc.PublishMsg(&nats.Msg{
				Subject: fmt.Sprintf("%s.CONNZ", serverStatsPingReqSubj),
				Reply:   reply,
				Data:    b,
			})

			msg := natsNexMsg(t, replySub, time.Second)
			var response ServerAPIResponse
			if err := json.Unmarshal(msg.Data, &response); err != nil {
				t.Fatalf("Error unmarshalling response json: %v", err)
			}
			tmp, _ := json.Marshal(response.Data)
			cz := &Connz{}
			if err := json.Unmarshal(tmp, cz); err != nil {
				t.Fatalf("Error unmarshalling connz: %v", err)
			}
			if test.cid == _EMPTY_ {
				if len(cz.Conns) != 0 {
					t.Fatalf("Expected no connections, got %v", len(cz.Conns))
				}
				return
			}
			if len(cz.Conns) != 1 {
				t.Fatalf("Expected single connection, got %v", len(cz.Conns))
			}
			conn := cz.Conns[0]
			if conn.MQTTClient != test.cid {
				t.Fatalf("Expected client ID %q, got %q", test.cid, conn.MQTTClient)
			}
		})
	}
}

func TestMQTTClientIDInLogStatements(t *testing.T) {
	o := testMQTTDefaultOptions()
	s := testMQTTRunServer(t, o)
	defer testMQTTShutdownServer(s)

	l := &captureDebugLogger{dbgCh: make(chan string, 10)}
	s.SetLogger(l, true, false)

	cisub := &mqttConnInfo{clientID: "my_client_id", cleanSess: false}
	c, r := testMQTTConnect(t, cisub, o.MQTT.Host, o.MQTT.Port)
	defer c.Close()
	testMQTTCheckConnAck(t, r, mqttConnAckRCConnectionAccepted, false)
	testMQTTDisconnect(t, c, nil)
	c.Close()

	tm := time.NewTimer(2 * time.Second)
	var connected bool
	var disconnected bool
	for {
		select {
		case dl := <-l.dbgCh:
			if strings.Contains(dl, "my_client_id") {
				if strings.Contains(dl, "Client connected") {
					connected = true
				} else if strings.Contains(dl, "Client connection closed") {
					disconnected = true
				}
				if connected && disconnected {
					// OK!
					return
				}
			}
		case <-tm.C:
			t.Fatal("Did not get the debug statements or client_id in them")
		}
	}
}

func TestMQTTStreamReplicasOverride(t *testing.T) {
	conf := `
		listen: 127.0.0.1:-1
		server_name: %s
		jetstream: {max_mem_store: 256MB, max_file_store: 2GB, store_dir: '%s'}

		cluster {
			name: %s
			listen: 127.0.0.1:%d
			routes = [%s]
		}

		mqtt {
			listen: 127.0.0.1:-1
			stream_replicas: 3
		}

		# For access to system account.
		accounts { $SYS { users = [ { user: "admin", pass: "s3cr3t!" } ] } }
	`
	cl := createJetStreamClusterWithTemplate(t, conf, "MQTT", 3)
	defer cl.shutdown()

	connectAndCheck := func(restarted bool) {
		t.Helper()

		o := cl.opts[0]
		mc, r := testMQTTConnectRetry(t, &mqttConnInfo{clientID: "test", cleanSess: false}, o.MQTT.Host, o.MQTT.Port, 5)
		defer mc.Close()
		testMQTTCheckConnAck(t, r, mqttConnAckRCConnectionAccepted, restarted)

		nc, js := jsClientConnect(t, cl.servers[2])
		defer nc.Close()

		streams := []string{mqttStreamName, mqttRetainedMsgsStreamName, mqttSessStreamName}
		for _, sn := range streams {
			si, err := js.StreamInfo(sn)
			require_NoError(t, err)
			if n := len(si.Cluster.Replicas); n != 2 {
				t.Fatalf("Expected stream %q to have 2 replicas, got %v", sn, n)
			}
		}
	}
	connectAndCheck(false)

	cl.stopAll()
	for _, o := range cl.opts {
		o.MQTT.StreamReplicas = 2
	}
	cl.restartAllSamePorts()
	cl.waitOnStreamLeader(globalAccountName, mqttStreamName)
	cl.waitOnStreamLeader(globalAccountName, mqttRetainedMsgsStreamName)
	cl.waitOnStreamLeader(globalAccountName, mqttSessStreamName)

	l := &captureWarnLogger{warn: make(chan string, 10)}
	cl.servers[0].SetLogger(l, false, false)

	connectAndCheck(true)

	select {
	case w := <-l.warn:
		if !strings.Contains(w, "current is 3 but configuration is 2") {
			t.Fatalf("Unexpected warning: %q", w)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Should have warned against replicas mismatch")
	}
}

func TestMQTTStreamReplicasConfigReload(t *testing.T) {
	tdir := t.TempDir()
	tmpl := `
		jetstream {
			store_dir = %q
		}
		server_name: mqtt
		mqtt {
			port: -1
			stream_replicas: %v
		}
	`
	conf := createConfFile(t, []byte(fmt.Sprintf(tmpl, tdir, 3)))
	s, o := RunServerWithConfig(conf)
	defer testMQTTShutdownServer(s)

	l := &captureErrorLogger{errCh: make(chan string, 10)}
	s.SetLogger(l, false, false)

	_, _, err := testMQTTConnectRetryWithError(t, &mqttConnInfo{clientID: "mqtt", cleanSess: false}, o.MQTT.Host, o.MQTT.Port, 0)
	if err == nil {
		t.Fatal("Expected to fail, did not")
	}

	select {
	case e := <-l.errCh:
		if !strings.Contains(e, NewJSStreamReplicasNotSupportedError().Description) {
			t.Fatalf("Expected error regarding replicas, got %v", e)
		}
	case <-time.After(2 * time.Second):
		t.Fatalf("Did not get the error regarding replicas count")
	}

	reloadUpdateConfig(t, s, conf, fmt.Sprintf(tmpl, tdir, 1))

	mc, r := testMQTTConnect(t, &mqttConnInfo{clientID: "mqtt", cleanSess: false}, o.MQTT.Host, o.MQTT.Port)
	defer mc.Close()
	testMQTTCheckConnAck(t, r, mqttConnAckRCConnectionAccepted, false)
}

func TestMQTTStreamReplicasInsufficientResources(t *testing.T) {
	conf := `
		listen: 127.0.0.1:-1
		server_name: %s
		jetstream: {max_mem_store: 256MB, max_file_store: 2GB, store_dir: '%s'}

		cluster {
			name: %s
			listen: 127.0.0.1:%d
			routes = [%s]
		}

		mqtt {
			listen: 127.0.0.1:-1
			stream_replicas: 5
		}

		# For access to system account.
		accounts { $SYS { users = [ { user: "admin", pass: "s3cr3t!" } ] } }
	`
	cl := createJetStreamClusterWithTemplate(t, conf, "MQTT", 3)
	defer cl.shutdown()

	l := &captureErrorLogger{errCh: make(chan string, 10)}
	for _, s := range cl.servers {
		s.SetLogger(l, false, false)
	}

	o := cl.opts[1]
	_, _, err := testMQTTConnectRetryWithError(t, &mqttConnInfo{clientID: "mqtt", cleanSess: false}, o.MQTT.Host, o.MQTT.Port, 0)
	if err == nil {
		t.Fatal("Expected to fail, did not")
	}

	select {
	case e := <-l.errCh:
		if !strings.Contains(e, fmt.Sprintf("%d", NewJSClusterNoPeersError(errors.New("")).ErrCode)) {
			t.Fatalf("Expected error regarding no peers error, got %v", e)
		}
	case <-time.After(2 * time.Second):
		t.Fatalf("Did not get the error regarding replicas count")
	}
}

func TestMQTTConsumerReplicasValidate(t *testing.T) {
	o := testMQTTDefaultOptions()
	for _, test := range []struct {
		name string
		sr   int
		cr   int
		err  bool
	}{
		{"stream replicas neg", -1, 3, false},
		{"stream replicas 0", 0, 3, false},
		{"consumer replicas neg", 0, -1, false},
		{"consumer replicas 0", -1, 0, false},
		{"consumer replicas too high", 1, 2, true},
	} {
		t.Run(test.name, func(t *testing.T) {
			o.MQTT.StreamReplicas = test.sr
			o.MQTT.ConsumerReplicas = test.cr
			err := validateMQTTOptions(o)
			if test.err {
				if err == nil {
					t.Fatal("Expected error, did not get one")
				}
				if !strings.Contains(err.Error(), "cannot be higher") {
					t.Fatalf("Unexpected error: %v", err)
				}
				// OK
				return
			} else if err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}
		})
	}
}

// This was ill-advised since the messages stream is currently interest policy.
// Interest policy streams require consumers match the replica count.
// Will leave her for now to make sure we do not override.
func TestMQTTConsumerReplicasOverride(t *testing.T) {
	conf := `
		listen: 127.0.0.1:-1
		server_name: %s
		jetstream: {max_mem_store: 256MB, max_file_store: 2GB, store_dir: '%s'}

		cluster {
			name: %s
			listen: 127.0.0.1:%d
			routes = [%s]
		}

		mqtt {
			listen: 127.0.0.1:-1
			stream_replicas: 5
			consumer_replicas: 1
			consumer_memory_storage: true
		}

		# For access to system account.
		accounts { $SYS { users = [ { user: "admin", pass: "s3cr3t!" } ] } }
	`
	cl := createJetStreamClusterWithTemplate(t, conf, "MQTT", 5)
	defer cl.shutdown()

	connectAndCheck := func(subject string, restarted bool) {
		t.Helper()

		o := cl.opts[0]
		mc, r := testMQTTConnectRetry(t, &mqttConnInfo{clientID: "test", cleanSess: false}, o.MQTT.Host, o.MQTT.Port, 5)
		defer mc.Close()
		testMQTTCheckConnAck(t, r, mqttConnAckRCConnectionAccepted, restarted)
		testMQTTSub(t, 1, mc, r, []*mqttFilter{{filter: subject, qos: 1}}, []byte{1})

		nc, js := jsClientConnect(t, cl.servers[2])
		defer nc.Close()

		for ci := range js.ConsumersInfo(mqttStreamName) {
			if ci.Config.FilterSubject == mqttStreamSubjectPrefix+subject {
				if rf := len(ci.Cluster.Replicas) + 1; rf != 5 {
					t.Fatalf("Expected consumer to be R5, got: %d", rf)
				}
			}
		}
	}
	connectAndCheck("foo", false)

	cl.stopAll()
	for _, o := range cl.opts {
		o.MQTT.ConsumerReplicas = 2
		o.MQTT.ConsumerMemoryStorage = false
	}
	cl.restartAllSamePorts()
	cl.waitOnStreamLeader(globalAccountName, mqttStreamName)
	cl.waitOnStreamLeader(globalAccountName, mqttRetainedMsgsStreamName)
	cl.waitOnStreamLeader(globalAccountName, mqttSessStreamName)

	connectAndCheck("bar", true)
}

func TestMQTTConsumerMemStorageReload(t *testing.T) {
	tdir := t.TempDir()
	tmpl := `
		jetstream {
			store_dir = %q
		}
		server_name: mqtt
		mqtt {
			port: -1
			consumer_memory_storage: %s
		}
	`
	conf := createConfFile(t, []byte(fmt.Sprintf(tmpl, tdir, "false")))
	s, o := RunServerWithConfig(conf)
	defer testMQTTShutdownServer(s)

	l := &captureErrorLogger{errCh: make(chan string, 10)}
	s.SetLogger(l, false, false)

	c, r := testMQTTConnect(t, &mqttConnInfo{clientID: "sub", cleanSess: false}, o.MQTT.Host, o.MQTT.Port)
	defer c.Close()
	testMQTTCheckConnAck(t, r, mqttConnAckRCConnectionAccepted, false)

	reloadUpdateConfig(t, s, conf, fmt.Sprintf(tmpl, tdir, "true"))

	testMQTTSub(t, 1, c, r, []*mqttFilter{{filter: "foo", qos: 1}}, []byte{1})

	mset, err := s.GlobalAccount().lookupStream(mqttStreamName)
	if err != nil {
		t.Fatalf("Error looking up stream: %v", err)
	}
	var cons *consumer
	mset.mu.RLock()
	for _, c := range mset.consumers {
		cons = c
		break
	}
	mset.mu.RUnlock()
	cons.mu.RLock()
	st := cons.store.Type()
	cons.mu.RUnlock()
	if st != MemoryStorage {
		t.Fatalf("Expected storage %v, got %v", MemoryStorage, st)
	}
}

// Test for auto-cleanup of consumers.
func TestMQTTConsumerInactiveThreshold(t *testing.T) {
	tdir := t.TempDir()
	tmpl := `
		listen: 127.0.0.1:-1
		server_name: mqtt
		jetstream {
			store_dir = %q
		}

		mqtt {
			listen: 127.0.0.1:-1
			consumer_inactive_threshold: %q
		}

		# For access to system account.
		accounts { $SYS { users = [ { user: "admin", pass: "s3cr3t!" } ] } }
	`
	conf := createConfFile(t, []byte(fmt.Sprintf(tmpl, tdir, "0.2s")))
	s, o := RunServerWithConfig(conf)
	defer testMQTTShutdownServer(s)

	mc, r := testMQTTConnectRetry(t, &mqttConnInfo{clientID: "test", cleanSess: true}, o.MQTT.Host, o.MQTT.Port, 5)
	defer mc.Close()
	testMQTTCheckConnAck(t, r, mqttConnAckRCConnectionAccepted, false)
	testMQTTSub(t, 1, mc, r, []*mqttFilter{{filter: "foo", qos: 1}}, []byte{1})

	nc, js := jsClientConnect(t, s)
	defer nc.Close()

	ci := <-js.ConsumersInfo("$MQTT_msgs")

	// Make sure we clean up this consumer.
	mc.Close()
	checkFor(t, 2*time.Second, 50*time.Millisecond, func() error {
		_, err := js.ConsumerInfo(ci.Stream, ci.Name)
		if err == nil {
			return fmt.Errorf("Consumer still present")
		}
		return nil
	})

	// Check reload.
	// We will not redo existing consumers however.
	reloadUpdateConfig(t, s, conf, fmt.Sprintf(tmpl, tdir, "22s"))
	if opts := s.getOpts(); opts.MQTT.ConsumerInactiveThreshold != 22*time.Second {
		t.Fatalf("Expected reloaded value of %v but got %v", 22*time.Second, opts.MQTT.ConsumerInactiveThreshold)
	}
}

func TestMQTTSubjectMapping(t *testing.T) {
	conf := createConfFile(t, []byte(fmt.Sprintf(`
		listen: 127.0.0.1:-1
		server_name: mqtt
		jetstream {
			store_dir = %q
		}

		mappings = {
			foo0: bar0
			foo1: bar1
		}

		mqtt {
			listen: 127.0.0.1:-1
		}
	`, t.TempDir())))
	s, o := RunServerWithConfig(conf)
	defer testMQTTShutdownServer(s)

	nc, js := jsClientConnect(t, s)
	defer nc.Close()

	for _, qos := range []byte{0, 1} {
		t.Run(fmt.Sprintf("qos%d", qos), func(t *testing.T) {
			mc, r := testMQTTConnect(t, &mqttConnInfo{clientID: "sub", cleanSess: true}, o.MQTT.Host, o.MQTT.Port)
			defer mc.Close()

			bar := fmt.Sprintf("bar%v", qos)
			foo := fmt.Sprintf("foo%v", qos)

			testMQTTCheckConnAck(t, r, mqttConnAckRCConnectionAccepted, false)
			testMQTTSub(t, 1, mc, r, []*mqttFilter{{filter: bar, qos: qos}}, []byte{qos})
			testMQTTFlush(t, mc, nil, r)

			mcp, rp := testMQTTConnect(t, &mqttConnInfo{clientID: "pub", cleanSess: true}, o.MQTT.Host, o.MQTT.Port)
			defer mcp.Close()
			testMQTTCheckConnAck(t, rp, mqttConnAckRCConnectionAccepted, false)

			natsPub(t, nc, foo, []byte("msg0"))
			testMQTTCheckPubMsgNoAck(t, mc, r, bar, 0, []byte("msg0"))

			testMQTTPublish(t, mcp, rp, 0, false, false, foo, 0, []byte("msg1"))
			testMQTTCheckPubMsgNoAck(t, mc, r, bar, 0, []byte("msg1"))

			testMQTTPublish(t, mcp, rp, 0, false, true, foo, 0, []byte("msg1_retained"))
			testMQTTCheckPubMsgNoAck(t, mc, r, bar, 0, []byte("msg1_retained"))

			testMQTTPublish(t, mcp, rp, 1, false, false, foo, 1, []byte("msg2"))
			// For the receiving side, the expected QoS is based on the subscription's QoS,
			// not the publisher.
			expected := byte(0)
			if qos == 1 {
				expected = mqttPubQos1
			}
			testMQTTCheckPubMsgNoAck(t, mc, r, bar, expected, []byte("msg2"))

			testMQTTPublish(t, mcp, rp, 1, false, true, foo, 1, []byte("msg2_retained"))
			testMQTTCheckPubMsgNoAck(t, mc, r, bar, expected, []byte("msg2_retained"))

			testMQTTDisconnect(t, mcp, nil)
			mcp.Close()

			// Try the with the "will" with QoS0 first
			mcp, rp = testMQTTConnect(t, &mqttConnInfo{
				clientID:  "pub",
				cleanSess: true,
				will:      &mqttWill{topic: []byte(foo), qos: 0, message: []byte("willmsg1")}},
				o.MQTT.Host, o.MQTT.Port)
			defer mcp.Close()
			testMQTTCheckConnAck(t, rp, mqttConnAckRCConnectionAccepted, false)
			testMQTTFlush(t, mcp, nil, rp)

			// Close the connection without proper disconnect for will to be sent
			mcp.Close()
			testMQTTCheckPubMsgNoAck(t, mc, r, bar, 0, []byte("willmsg1"))

			// Try the with the "will" with QoS1 now
			mcp, rp = testMQTTConnect(t, &mqttConnInfo{
				clientID:  "pub",
				cleanSess: true,
				will:      &mqttWill{topic: []byte(foo), qos: 1, message: []byte("willmsg2")}},
				o.MQTT.Host, o.MQTT.Port)
			defer mcp.Close()
			testMQTTCheckConnAck(t, rp, mqttConnAckRCConnectionAccepted, false)
			testMQTTFlush(t, mcp, nil, rp)

			// Close the connection without proper disconnect for will to be sent
			mcp.Close()
			testMQTTCheckPubMsgNoAck(t, mc, r, bar, expected, []byte("willmsg2"))

			si, err := js.StreamInfo("$MQTT_msgs")
			require_NoError(t, err)
			if qos == 0 {
				require_True(t, si.State.Msgs == 0)
				require_True(t, si.State.NumSubjects == 0)
			} else {
				// Number of QoS1 messages: 1 regular, 1 retained, 1 from will.
				require_True(t, si.State.Msgs == 3)
				require_True(t, si.State.NumSubjects == 1)
			}
			testMQTTDisconnect(t, mc, nil)
		})
	}
}

func TestMQTTSubjectMappingWithImportExport(t *testing.T) {
	conf := createConfFile(t, []byte(fmt.Sprintf(`
		listen: 127.0.0.1:-1
		server_name: mqtt
		jetstream {
			store_dir = %q
		}

		accounts {
			A {
				users = [{user: "a", password: "pwd"}]
				mappings = {
					bar: foo
				}
				exports = [{service: "bar"}]
				jetstream: enabled
			}
			B {
				users = [{user: "b", password: "pwd"}]
				mappings = {
					foo: bar
				}
				imports = [{service: {account: "A", subject: "bar"}}]
				jetstream: enabled
			}
			$SYS { users = [ { user: "admin", pass: "s3cr3t!" } ] }
		}

		mqtt {
			listen: 127.0.0.1:-1
		}
	`, t.TempDir())))
	s, o := RunServerWithConfig(conf)
	defer testMQTTShutdownServer(s)

	c1, r1 := testMQTTConnect(t, &mqttConnInfo{user: "a", pass: "pwd", clientID: "a1", cleanSess: true}, o.MQTT.Host, o.MQTT.Port)
	defer c1.Close()
	testMQTTCheckConnAck(t, r1, mqttConnAckRCConnectionAccepted, false)
	testMQTTSub(t, 1, c1, r1, []*mqttFilter{{filter: "foo", qos: 0}}, []byte{0})
	testMQTTFlush(t, c1, nil, r1)

	c2, r2 := testMQTTConnect(t, &mqttConnInfo{user: "a", pass: "pwd", clientID: "a2", cleanSess: true}, o.MQTT.Host, o.MQTT.Port)
	defer c2.Close()
	testMQTTCheckConnAck(t, r2, mqttConnAckRCConnectionAccepted, false)
	testMQTTSub(t, 1, c2, r2, []*mqttFilter{{filter: "foo", qos: 1}}, []byte{1})
	testMQTTFlush(t, c2, nil, r2)

	c3, r3 := testMQTTConnect(t, &mqttConnInfo{user: "a", pass: "pwd", clientID: "a3", cleanSess: true}, o.MQTT.Host, o.MQTT.Port)
	defer c3.Close()
	testMQTTCheckConnAck(t, r3, mqttConnAckRCConnectionAccepted, false)
	testMQTTSub(t, 1, c3, r3, []*mqttFilter{{filter: "bar", qos: 0}}, []byte{0})
	testMQTTFlush(t, c3, nil, r3)

	c4, r4 := testMQTTConnect(t, &mqttConnInfo{user: "a", pass: "pwd", clientID: "a4", cleanSess: true}, o.MQTT.Host, o.MQTT.Port)
	defer c4.Close()
	testMQTTCheckConnAck(t, r4, mqttConnAckRCConnectionAccepted, false)
	testMQTTSub(t, 1, c4, r4, []*mqttFilter{{filter: "bar", qos: 1}}, []byte{1})
	testMQTTFlush(t, c4, nil, r4)

	bc, br := testMQTTConnect(t, &mqttConnInfo{user: "b", pass: "pwd", clientID: "b0", cleanSess: true}, o.MQTT.Host, o.MQTT.Port)
	defer bc.Close()
	testMQTTCheckConnAck(t, br, mqttConnAckRCConnectionAccepted, false)

	bc1, br1 := testMQTTConnect(t, &mqttConnInfo{user: "b", pass: "pwd", clientID: "b1", cleanSess: true}, o.MQTT.Host, o.MQTT.Port)
	defer bc1.Close()
	testMQTTCheckConnAck(t, br1, mqttConnAckRCConnectionAccepted, false)
	testMQTTSub(t, 1, bc1, br1, []*mqttFilter{{filter: "foo", qos: 0}}, []byte{0})
	testMQTTFlush(t, bc1, nil, br1)

	bc2, br2 := testMQTTConnect(t, &mqttConnInfo{user: "b", pass: "pwd", clientID: "b2", cleanSess: true}, o.MQTT.Host, o.MQTT.Port)
	defer bc2.Close()
	testMQTTCheckConnAck(t, br2, mqttConnAckRCConnectionAccepted, false)
	testMQTTSub(t, 1, bc2, br2, []*mqttFilter{{filter: "b", qos: 1}}, []byte{1})
	testMQTTFlush(t, bc2, nil, br2)

	bc3, br3 := testMQTTConnect(t, &mqttConnInfo{user: "b", pass: "pwd", clientID: "b3", cleanSess: true}, o.MQTT.Host, o.MQTT.Port)
	defer bc3.Close()
	testMQTTCheckConnAck(t, br3, mqttConnAckRCConnectionAccepted, false)
	testMQTTSub(t, 1, bc3, br3, []*mqttFilter{{filter: "bar", qos: 0}}, []byte{0})
	testMQTTFlush(t, bc3, nil, br3)

	bc4, br4 := testMQTTConnect(t, &mqttConnInfo{user: "b", pass: "pwd", clientID: "b4", cleanSess: true}, o.MQTT.Host, o.MQTT.Port)
	defer bc4.Close()
	testMQTTCheckConnAck(t, br4, mqttConnAckRCConnectionAccepted, false)
	testMQTTSub(t, 1, bc4, br4, []*mqttFilter{{filter: "bar", qos: 1}}, []byte{1})
	testMQTTFlush(t, bc4, nil, br4)

	nc := natsConnect(t, fmt.Sprintf("nats://b:pwd@%s:%d", o.Host, o.Port))
	defer nc.Close()

	natsPub(t, nc, "foo", []byte("msg0"))
	testMQTTCheckPubMsgNoAck(t, c1, r1, "foo", 0, []byte("msg0"))
	testMQTTCheckPubMsgNoAck(t, c2, r2, "foo", 0, []byte("msg0"))
	testMQTTExpectNothing(t, r3)
	testMQTTExpectNothing(t, r4)
	testMQTTExpectNothing(t, br1)
	testMQTTExpectNothing(t, br2)
	testMQTTCheckPubMsgNoAck(t, bc3, br3, "bar", 0, []byte("msg0"))
	testMQTTCheckPubMsgNoAck(t, bc4, br4, "bar", 0, []byte("msg0"))

	testMQTTPublish(t, bc, br, 0, false, false, "foo", 0, []byte("msg1"))
	testMQTTCheckPubMsgNoAck(t, c1, r1, "foo", 0, []byte("msg1"))
	testMQTTCheckPubMsgNoAck(t, c2, r2, "foo", 0, []byte("msg1"))
	testMQTTExpectNothing(t, r3)
	testMQTTExpectNothing(t, r4)
	testMQTTExpectNothing(t, br1)
	testMQTTExpectNothing(t, br2)
	testMQTTCheckPubMsgNoAck(t, bc3, br3, "bar", 0, []byte("msg1"))
	testMQTTCheckPubMsgNoAck(t, bc4, br4, "bar", 0, []byte("msg1"))

	testMQTTPublish(t, bc, br, 1, false, false, "foo", 1, []byte("msg2"))
	testMQTTCheckPubMsgNoAck(t, c1, r1, "foo", 0, []byte("msg2"))
	testMQTTCheckPubMsgNoAck(t, c2, r2, "foo", mqttPubQos1, []byte("msg2"))
	testMQTTExpectNothing(t, r3)
	testMQTTExpectNothing(t, r4)
	testMQTTExpectNothing(t, br1)
	testMQTTExpectNothing(t, br2)
	testMQTTCheckPubMsgNoAck(t, bc3, br3, "bar", 0, []byte("msg2"))
	testMQTTCheckPubMsgNoAck(t, bc4, br4, "bar", mqttPubQos1, []byte("msg2"))

	// Connection nc is for account B
	check := func(nc *nats.Conn, subj string) {
		t.Helper()
		req, err := json.Marshal(&JSApiStreamInfoRequest{SubjectsFilter: ">"})
		require_NoError(t, err)
		resp, err := nc.Request(fmt.Sprintf(JSApiStreamInfoT, "$MQTT_msgs"), req, time.Second)
		require_NoError(t, err)
		var si StreamInfo
		err = json.Unmarshal(resp.Data, &si)
		require_NoError(t, err)
		require_True(t, si.State.Msgs == 1)
		require_True(t, si.State.NumSubjects == 1)
		require_True(t, si.State.Subjects[subj] == 1)
	}
	// Currently, nc is for account B
	check(nc, "$MQTT.msgs.bar")

	nc.Close()
	nc = natsConnect(t, fmt.Sprintf("nats://a:pwd@%s:%d", o.Host, o.Port))
	defer nc.Close()
	check(nc, "$MQTT.msgs.foo")
}

func TestMQTTSubRetainedRace(t *testing.T) {
	o := testMQTTDefaultOptions()

	useCases := []struct {
		name string
		f    func(t *testing.T, s *Server, o *Options, subTopic, pubTopic string, QOS byte)
	}{
		{"new top level", testMQTTNewSubRetainedRace},
		{"existing top level", testMQTTNewSubWithExistingTopLevelRetainedRace},
	}
	pubTopic := "/bar"
	subTopics := []string{"#", "/bar", "/+", "/#"}
	QOS := []byte{0, 1, 2}
	for _, tc := range useCases {
		t.Run(tc.name, func(t *testing.T) {
			for _, subTopic := range subTopics {
				t.Run(subTopic, func(t *testing.T) {
					for _, qos := range QOS {
						t.Run(fmt.Sprintf("QOS%d", qos), func(t *testing.T) {
							s := testMQTTRunServer(t, o)
							defer testMQTTShutdownServer(s)

							tc.f(t, s, o, subTopic, pubTopic, qos)
						})
					}
				})
			}
		})
	}
}

func testMQTTNewSubRetainedRace(t *testing.T, s *Server, o *Options, subTopic, pubTopic string, QOS byte) {
	expectedFlags := (QOS << 1) | mqttPubFlagRetain
	payload := []byte("testmsg")

	pubID := nuid.Next()
	pubc, pubr := testMQTTConnectRetry(t, &mqttConnInfo{clientID: pubID, cleanSess: true}, o.MQTT.Host, o.MQTT.Port, 3)
	testMQTTCheckConnAck(t, pubr, mqttConnAckRCConnectionAccepted, false)
	defer testMQTTDisconnectEx(t, pubc, nil, true)
	defer pubc.Close()
	testMQTTPublish(t, pubc, pubr, QOS, false, true, pubTopic, 1, payload)

	// Wait for retained messages stream to be populated.
	checkFor(t, time.Second, 10*time.Millisecond, func() error {
		acc, err := s.lookupAccount(globalAccountName)
		if err != nil {
			return err
		}
		mset, err := acc.lookupStream(mqttRetainedMsgsStreamName)
		if err != nil {
			return err
		}
		if mset.state().Msgs != 1 {
			return errors.New("retained message not populated yet")
		}
		return nil
	})

	subID := nuid.Next()
	subc, subr := testMQTTConnect(t, &mqttConnInfo{clientID: subID, cleanSess: true}, o.MQTT.Host, o.MQTT.Port)
	testMQTTCheckConnAck(t, subr, mqttConnAckRCConnectionAccepted, false)
	testMQTTSub(t, 1, subc, subr, []*mqttFilter{{filter: subTopic, qos: QOS}}, []byte{QOS})

	testMQTTCheckPubMsg(t, subc, subr, pubTopic, expectedFlags, payload)
	if QOS == 2 {
		testMQTTCheckPubAck(t, subr, mqttPacketPubRel)
		testMQTTSendPIPacket(mqttPacketPubComp, t, subc, 1)
	}

	testMQTTDisconnectEx(t, subc, nil, true)
	subc.Close()
}

func testMQTTNewSubWithExistingTopLevelRetainedRace(t *testing.T, s *Server, o *Options, subTopic, pubTopic string, QOS byte) {
	expectedFlags := (QOS << 1) | mqttPubFlagRetain
	payload := []byte("testmsg")

	pubID := nuid.Next()
	pubc, pubr := testMQTTConnectRetry(t, &mqttConnInfo{clientID: pubID, cleanSess: true}, o.MQTT.Host, o.MQTT.Port, 3)
	testMQTTCheckConnAck(t, pubr, mqttConnAckRCConnectionAccepted, false)
	defer testMQTTDisconnectEx(t, pubc, nil, true)
	defer pubc.Close()

	subID := nuid.Next()
	subc, subr := testMQTTConnect(t, &mqttConnInfo{clientID: subID, cleanSess: true}, o.MQTT.Host, o.MQTT.Port)
	testMQTTCheckConnAck(t, subr, mqttConnAckRCConnectionAccepted, false)

	// Subscribe to `#` first.
	testMQTTSub(t, 1, subc, subr, []*mqttFilter{{filter: `#`, qos: 0}}, []byte{0})
	testMQTTExpectNothing(t, subr)

	// Publish the retained message with QOS2, see that the `#` subscriber gets it.
	testMQTTPublish(t, pubc, pubr, 2, false, true, pubTopic, 1, payload)
	testMQTTCheckPubMsg(t, subc, subr, pubTopic, 0, payload)
	testMQTTExpectNothing(t, subr)

	// Wait for retained messages stream to be populated.
	checkFor(t, time.Second, 10*time.Millisecond, func() error {
		acc, err := s.lookupAccount(globalAccountName)
		if err != nil {
			return err
		}
		mset, err := acc.lookupStream(mqttRetainedMsgsStreamName)
		if err != nil {
			return err
		}
		if mset.state().Msgs != 1 {
			return errors.New("retained message not populated yet")
		}
		return nil
	})

	// Now subscribe to the topic we want to test. We should get the retained
	// message there.
	testMQTTSub(t, 1, subc, subr, []*mqttFilter{{filter: subTopic, qos: QOS}}, []byte{QOS})
	testMQTTCheckPubMsg(t, subc, subr, pubTopic, expectedFlags, payload)
	if QOS == 2 {
		testMQTTCheckPubAck(t, subr, mqttPacketPubRel)
		testMQTTSendPIPacket(mqttPacketPubComp, t, subc, 1)
	}

	testMQTTDisconnectEx(t, subc, nil, true)
	subc.Close()
}

// Issue https://github.com/nats-io/nats-server/issues/3924
// The MQTT Server MUST NOT match Topic Filters starting with a wildcard character (# or +),
// with Topic Names beginning with a $ character [MQTT-4.7.2-1]
func TestMQTTSubjectWildcardStart(t *testing.T) {
	conf := createConfFile(t, []byte(fmt.Sprintf(`
		listen: 127.0.0.1:-1
		server_name: mqtt
		jetstream {
			store_dir = %q
		}
		mqtt {
			listen: 127.0.0.1:-1
		}
	`, t.TempDir())))
	s, o := RunServerWithConfig(conf)
	defer testMQTTShutdownServer(s)

	nc := natsConnect(t, s.ClientURL())
	defer nc.Close()

	mc, r := testMQTTConnect(t, &mqttConnInfo{cleanSess: true}, o.MQTT.Host, o.MQTT.Port)
	defer mc.Close()
	testMQTTCheckConnAck(t, r, mqttConnAckRCConnectionAccepted, false)

	mc1, r1 := testMQTTConnect(t, &mqttConnInfo{cleanSess: true}, o.MQTT.Host, o.MQTT.Port)
	defer mc1.Close()
	testMQTTCheckConnAck(t, r1, mqttConnAckRCConnectionAccepted, false)

	mc2, r2 := testMQTTConnect(t, &mqttConnInfo{cleanSess: true}, o.MQTT.Host, o.MQTT.Port)
	defer mc2.Close()
	testMQTTCheckConnAck(t, r2, mqttConnAckRCConnectionAccepted, false)

	mc3, r3 := testMQTTConnect(t, &mqttConnInfo{cleanSess: true}, o.MQTT.Host, o.MQTT.Port)
	defer mc3.Close()
	testMQTTCheckConnAck(t, r3, mqttConnAckRCConnectionAccepted, false)

	// These will fail already with the bug due to messages already being queue up before the subAck.
	testMQTTSub(t, 1, mc1, r1, []*mqttFilter{{filter: "*", qos: 0}}, []byte{0})
	testMQTTFlush(t, mc1, nil, r1)

	testMQTTSub(t, 1, mc2, r2, []*mqttFilter{{filter: "#", qos: 1}}, []byte{1})
	testMQTTFlush(t, mc2, nil, r2)

	testMQTTSub(t, 1, mc3, r3, []*mqttFilter{{filter: "*/foo", qos: 1}}, []byte{1})
	testMQTTFlush(t, mc2, nil, r2)

	// Just as a barrier
	natsFlush(t, nc)

	// Now publish

	// NATS Publish
	msg := []byte("HELLO WORLD")
	natsPub(t, nc, "foo", msg)

	// Check messages received
	testMQTTCheckPubMsg(t, mc1, r1, "foo", 0, msg)
	testMQTTExpectNothing(t, r1)

	testMQTTCheckPubMsg(t, mc2, r2, "foo", 0, msg)
	testMQTTExpectNothing(t, r2)

	testMQTTExpectNothing(t, r3)

	// Anything that starts with $ is reserved against wildcard subjects like above.
	natsPub(t, nc, "$JS.foo", msg)
	testMQTTExpectNothing(t, r1)
	testMQTTExpectNothing(t, r2)
	testMQTTExpectNothing(t, r3)

	// Now do MQTT QoS-0
	testMQTTPublish(t, mc, r, 0, false, false, "foo", 0, msg)

	testMQTTCheckPubMsg(t, mc1, r1, "foo", 0, msg)
	testMQTTExpectNothing(t, r1)

	testMQTTCheckPubMsg(t, mc2, r2, "foo", 0, msg)
	testMQTTExpectNothing(t, r2)

	testMQTTExpectNothing(t, r3)

	testMQTTPublish(t, mc, r, 0, false, false, "$JS/foo", 1, msg)

	testMQTTExpectNothing(t, r1)
	testMQTTExpectNothing(t, r2)
	testMQTTExpectNothing(t, r3)

	// Now do MQTT QoS-1
	msg = []byte("HELLO WORLD - RETAINED")
	testMQTTPublish(t, mc, r, 1, false, false, "$JS/foo", 4, msg)

	testMQTTExpectNothing(t, r1)
	testMQTTExpectNothing(t, r2)
	testMQTTExpectNothing(t, r3)

	testMQTTPublish(t, mc, r, 1, false, false, "foo", 2, msg)

	testMQTTCheckPubMsg(t, mc1, r1, "foo", 0, msg)
	testMQTTExpectNothing(t, r1)

	testMQTTCheckPubMsg(t, mc2, r2, "foo", 2, msg)
	testMQTTExpectNothing(t, r2)

	testMQTTExpectNothing(t, r3)

	testMQTTPublish(t, mc, r, 1, false, false, "foo/foo", 3, msg)

	testMQTTExpectNothing(t, r1)

	testMQTTCheckPubMsg(t, mc2, r2, "foo/foo", 2, msg)
	testMQTTExpectNothing(t, r2)

	testMQTTCheckPubMsg(t, mc3, r3, "foo/foo", 2, msg)
	testMQTTExpectNothing(t, r3)

	// Make sure we did not retain the messages prefixed with $.
	nc, js := jsClientConnect(t, s)
	defer nc.Close()

	si, err := js.StreamInfo(mqttStreamName)
	require_NoError(t, err)

	require_True(t, si.State.Msgs == 0)
}

func TestMQTTTopicWithDot(t *testing.T) {
	o := testMQTTDefaultOptions()
	s := testMQTTRunServer(t, o)
	defer testMQTTShutdownServer(s)

	nc := natsConnect(t, s.ClientURL(), nats.UserInfo("mqtt", "pwd"))
	defer nc.Close()

	sub := natsSubSync(t, nc, "*.*")

	c1, r1 := testMQTTConnect(t, &mqttConnInfo{user: "mqtt", pass: "pwd", clientID: "conn1", cleanSess: true}, o.MQTT.Host, o.MQTT.Port)
	defer c1.Close()
	testMQTTCheckConnAck(t, r1, mqttConnAckRCConnectionAccepted, false)
	testMQTTSub(t, 1, c1, r1, []*mqttFilter{{filter: "spBv1.0/plant1", qos: 0}}, []byte{0})
	testMQTTSub(t, 1, c1, r1, []*mqttFilter{{filter: "spBv1.0/plant2", qos: 1}}, []byte{1})

	c2, r2 := testMQTTConnect(t, &mqttConnInfo{user: "mqtt", pass: "pwd", clientID: "conn2", cleanSess: true}, o.MQTT.Host, o.MQTT.Port)
	defer c2.Close()
	testMQTTCheckConnAck(t, r2, mqttConnAckRCConnectionAccepted, false)

	testMQTTPublish(t, c2, r2, 0, false, false, "spBv1.0/plant1", 0, []byte("msg1"))
	testMQTTCheckPubMsg(t, c1, r1, "spBv1.0/plant1", 0, []byte("msg1"))
	msg := natsNexMsg(t, sub, time.Second)
	require_Equal(t, msg.Subject, "spBv1//0.plant1")

	testMQTTPublish(t, c2, r2, 1, false, false, "spBv1.0/plant2", 1, []byte("msg2"))
	testMQTTCheckPubMsg(t, c1, r1, "spBv1.0/plant2", mqttPubQos1, []byte("msg2"))
	msg = natsNexMsg(t, sub, time.Second)
	require_Equal(t, msg.Subject, "spBv1//0.plant2")

	natsPub(t, nc, "spBv1//0.plant1", []byte("msg3"))
	testMQTTCheckPubMsg(t, c1, r1, "spBv1.0/plant1", 0, []byte("msg3"))
	msg = natsNexMsg(t, sub, time.Second)
	require_Equal(t, msg.Subject, "spBv1//0.plant1")

	natsPub(t, nc, "spBv1//0.plant2", []byte("msg4"))
	testMQTTCheckPubMsg(t, c1, r1, "spBv1.0/plant2", 0, []byte("msg4"))
	msg = natsNexMsg(t, sub, time.Second)
	require_Equal(t, msg.Subject, "spBv1//0.plant2")
}

// Issue https://github.com/nats-io/nats-server/issues/4291
func TestMQTTJetStreamRepublishAndQoS0Subscribers(t *testing.T) {
	conf := createConfFile(t, []byte(fmt.Sprintf(`
		listen: 127.0.0.1:-1
		server_name: mqtt
		jetstream {
			store_dir = %q
		}
		mqtt {
			listen: 127.0.0.1:-1
		}
	`, t.TempDir())))
	s, o := RunServerWithConfig(conf)
	defer testMQTTShutdownServer(s)

	nc, js := jsClientConnect(t, s)
	defer nc.Close()

	// Setup stream with republish on it.
	_, err := js.AddStream(&nats.StreamConfig{
		Name:     "TEST",
		Subjects: []string{"foo"},
		RePublish: &nats.RePublish{
			Source:      "foo",
			Destination: "mqtt.foo",
		},
	})
	require_NoError(t, err)

	// Create QoS0 subscriber to catch re-publishes.
	mc, r := testMQTTConnect(t, &mqttConnInfo{cleanSess: true}, o.MQTT.Host, o.MQTT.Port)
	defer mc.Close()
	testMQTTCheckConnAck(t, r, mqttConnAckRCConnectionAccepted, false)

	testMQTTSub(t, 1, mc, r, []*mqttFilter{{filter: "mqtt/foo", qos: 0}}, []byte{0})
	testMQTTFlush(t, mc, nil, r)

	msg := []byte("HELLO WORLD")
	_, err = js.Publish("foo", msg)
	require_NoError(t, err)

	testMQTTCheckPubMsg(t, mc, r, "mqtt/foo", 0, msg)
	testMQTTExpectNothing(t, r)
}

func TestMQTTDecodeRetainedMessage(t *testing.T) {
	tdir := t.TempDir()
	tmpl := `
		listen: 127.0.0.1:-1
		server_name: mqtt
		jetstream {
			store_dir = %q
		}

		mqtt {
			listen: 127.0.0.1:-1
			consumer_inactive_threshold: %q
		}

		# For access to system account.
		accounts { $SYS { users = [ { user: "admin", pass: "s3cr3t!" } ] } }
	`
	conf := createConfFile(t, []byte(fmt.Sprintf(tmpl, tdir, "0.2s")))
	s, o := RunServerWithConfig(conf)
	defer testMQTTShutdownServer(s)

	// Connect and publish a retained message, this will be in the "newer" form,
	// with the metadata in the header.
	mc, r := testMQTTConnectRetry(t, &mqttConnInfo{cleanSess: true}, o.MQTT.Host, o.MQTT.Port, 5)
	defer mc.Close()
	testMQTTCheckConnAck(t, r, mqttConnAckRCConnectionAccepted, false)
	testMQTTPublish(t, mc, r, 0, false, true, "foo/1", 0, []byte("msg1"))
	mc.Close()

	// Store a "legacy", JSON-encoded payload directly into the stream.
	nc, js := jsClientConnect(t, s)
	defer nc.Close()

	rm := mqttRetainedMsg{
		Origin:  "test",
		Subject: "foo.2",
		Topic:   "foo/2",
		Flags:   mqttPubFlagRetain,
		Msg:     []byte("msg2"),
	}
	jsonData, _ := json.Marshal(rm)
	_, err := js.PublishMsg(&nats.Msg{
		Subject: mqttRetainedMsgsStreamSubject + rm.Subject,
		Data:    jsonData,
	})
	if err != nil {
		t.Fatalf("Error publishing retained message to JS directly: %v", err)
	}
	// The MQTT retained message is stored async from the test's perspective.
	checkFor(t, 2*time.Second, 50*time.Millisecond, func() error {
		si, err := js.StreamInfo(mqttRetainedMsgsStreamName)
		if err != nil {
			return err
		}
		if si.State.Msgs != 2 {
			return fmt.Errorf("expected 2 retained messages stored, got %d", si.State.Msgs)
		}
		return nil
	})

	// Retained messages are loaded into (and removed from) the in-memory map async.
	checkRetained := func(s *Server, n int) {
		t.Helper()
		checkFor(t, 2*time.Second, 10*time.Millisecond, func() error {
			sm := &s.mqtt.sessmgr
			sm.mu.RLock()
			as := sm.sessions[globalAccountName]
			sm.mu.RUnlock()
			if as == nil {
				return errors.New("no account session manager")
			}
			as.mu.RLock()
			defer as.mu.RUnlock()
			if size := as.retmsgs.Size(); size != n {
				return fmt.Errorf("expected %d retained messages, got %d", n, size)
			}
			return nil
		})
	}

	// Restart the server to see that it picks up both retained messages on restart.
	s.Shutdown()
	s = RunServer(o)
	defer testMQTTShutdownServer(s)

	// Connect again, subscribe, and check that we get both messages.
	mc, r = testMQTTConnectRetry(t, &mqttConnInfo{cleanSess: true}, o.MQTT.Host, o.MQTT.Port, 5)
	defer mc.Close()
	testMQTTCheckConnAck(t, r, mqttConnAckRCConnectionAccepted, false)
	checkRetained(s, 2)
	testMQTTSub(t, 1, mc, r, []*mqttFilter{{filter: "foo/+", qos: 0}}, []byte{0})
	for i := 0; i < 2; i++ {
		b, pl := testMQTTReadPacket(t, r)
		if pt := b & mqttPacketMask; pt != mqttPacketPub {
			t.Fatalf("Expected PUBLISH packet %x, got %x", mqttPacketPub, pt)
		}
		_, _, topic, _ := testMQTTGetPubMsgExEx(t, mc, r, mqttPubFlagRetain, pl, "", nil)
		if string(topic) != "foo/1" && string(topic) != "foo/2" {
			t.Fatalf("Expected foo/1 or foo/2, got %q", topic)
		}
	}
	testMQTTExpectNothing(t, r)
	mc.Close()

	// Clear both retained messages.
	mc, r = testMQTTConnectRetry(t, &mqttConnInfo{cleanSess: true}, o.MQTT.Host, o.MQTT.Port, 5)
	defer mc.Close()
	testMQTTCheckConnAck(t, r, mqttConnAckRCConnectionAccepted, false)
	testMQTTPublish(t, mc, r, 0, false, true, "foo/1", 0, []byte{})
	testMQTTPublish(t, mc, r, 0, false, true, "foo/2", 0, []byte{})
	testMQTTFlush(t, mc, nil, r)
	testMQTTDisconnect(t, mc, nil)
	mc.Close()
	checkRetained(s, 0)

	// Connect again, subscribe, and check that we get nothing.
	mc, r = testMQTTConnectRetry(t, &mqttConnInfo{cleanSess: true}, o.MQTT.Host, o.MQTT.Port, 5)
	defer mc.Close()
	testMQTTCheckConnAck(t, r, mqttConnAckRCConnectionAccepted, false)
	testMQTTSub(t, 1, mc, r, []*mqttFilter{{filter: "foo/+", qos: 0}}, []byte{0})
	testMQTTExpectNothing(t, r)
}

func TestMQTTSparkbDeathHandling(t *testing.T) {
	o := testMQTTDefaultOptions()
	s := testMQTTRunServer(t, o)
	defer testMQTTShutdownServer(s)

	// protoMetrics is protobuf-encoded test data, with no timestamp.
	//
	//	p := &sproto.Payload{
	// 		Metrics: []*sproto.Payload_Metric{
	// 			newSparkbMetric_Uint64(sparkbBDSeqMetric, uint64(sparkbBDSeq)),
	// 			newSparkbMetric(sparkbDeviceMetric1, "value-at-death-1"),
	// 			newSparkbMetric(sparkbDeviceMetric2, "value-at-death-2"),
	// 		},
	// 		Timestamp: nil,
	// 	}
	protoMetrics := []byte{0x12, 0x0b, 0x0a, 0x05, 0x62, 0x64, 0x53, 0x65, 0x71, 0x20, 0x04, 0x58, 0x02, 0x12, 0x24, 0x0a, 0x0e, 0x64, 0x65, 0x76, 0x69, 0x63, 0x65, 0x2d, 0x6d, 0x65, 0x74, 0x72, 0x69, 0x63, 0x31, 0x20, 0x0c, 0x7a, 0x10, 0x76, 0x61, 0x6c, 0x75, 0x65, 0x2d, 0x61, 0x74, 0x2d, 0x64, 0x65, 0x61, 0x74, 0x68, 0x2d, 0x31, 0x12, 0x24, 0x0a, 0x0e, 0x64, 0x65, 0x76, 0x69, 0x63, 0x65, 0x2d, 0x6d, 0x65, 0x74, 0x72, 0x69, 0x63, 0x32, 0x20, 0x0c, 0x7a, 0x10, 0x76, 0x61, 0x6c, 0x75, 0x65, 0x2d, 0x61, 0x74, 0x2d, 0x64, 0x65, 0x61, 0x74, 0x68, 0x2d, 0x32}

	proto0Timestamp := []byte{0x08, 0x00}

	protoDeadbeefTimestamp := []byte{0x08, 0xef, 0xfd, 0xb6, 0xf5, 0xfd, 0xde, 0xef, 0xd6, 0xde, 0x01}

	mcPub, rPub := testMQTTConnect(t, &mqttConnInfo{cleanSess: true}, o.MQTT.Host, o.MQTT.Port)
	defer mcPub.Close()
	testMQTTCheckConnAck(t, rPub, mqttConnAckRCConnectionAccepted, false)

	mcSub, rSub := testMQTTConnect(t, &mqttConnInfo{cleanSess: true}, o.MQTT.Host, o.MQTT.Port)
	defer mcSub.Close()
	testMQTTCheckConnAck(t, rSub, mqttConnAckRCConnectionAccepted, false)
	testMQTTSub(t, 1, mcSub, rSub, []*mqttFilter{{filter: "spBv1.0/ggg/#", qos: 0}}, []byte{0})

	for _, test := range []*struct {
		name                  string
		payload               []byte
		expectTimestampOffset int
		expectUnchanged       bool
	}{
		{"append", protoMetrics, len(protoMetrics), false},
		{"replace at the end", append(protoMetrics, proto0Timestamp...), len(protoMetrics), false},
		{"replace at the start", append(protoDeadbeefTimestamp, protoMetrics...), 0, false},
		{"invalid", []byte{0xde, 0xad, 0xbe, 0xef}, 0, true}, // invalid payload
		{"invalid fixed32", []byte{0x0d, 0x01}, 0, true},
		{"invalid fixed64", []byte{0x09, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07}, 0, true},
	} {
		var sendPI uint16
		for _, topic := range []string{"NDEATH/nnn", "DDEATH/nnn/ddd"} {
			sendPI++
			t.Run(test.name+" "+topic, func(t *testing.T) {
				testMQTTPublish(t, mcPub, rPub, 1, false, false, "spBv1.0/ggg/"+topic, sendPI, test.payload)

				flags, pi, _, received := testMQTTGetPubMsgEx(t, mcSub, rSub, "", nil)
				if mqttGetQoS(flags) == 1 {
					testMQTTSendPIPacket(mqttPacketPubAck, t, mcSub, pi)
				}

				if test.expectUnchanged {
					if !bytes.Equal(test.payload, received) {
						t.Fatalf("Expected payload to be unchanged, got %q", received)
					}
					return
				}

				if test.expectTimestampOffset > len(received) {
					t.Fatalf("Expected timestamp offset %v to be less than %v", test.expectTimestampOffset, len(received))
				}
				if test.expectTimestampOffset > 0 {
					if !bytes.Equal(protoMetrics, received[:test.expectTimestampOffset]) {
						t.Fatalf("Expected payload to be unchanged up to %v, got %q", test.expectTimestampOffset, received)
					}
				}

				tsBytes := received[test.expectTimestampOffset:]
				if tsBytes[0] != 0x08 {
					t.Fatalf("Expected timestamp to start with 0x08, got %x", tsBytes)
				}
				v, _, err := protoScanVarint(tsBytes[1:])
				if err != nil {
					t.Fatalf("Error scanning varint: %v", err)
				}

				ts := time.UnixMilli(int64(v))
				if time.Now().Before(ts) {
					t.Fatalf("Expected timestamp to be in the past, got %v", ts)
				}
				if time.Since(ts) > time.Second {
					t.Fatalf("Expected timestamp to be within a second, got %v", time.Since(ts))
				}
			})
		}
	}
}

func TestMQTTSparkbBirthHandling(t *testing.T) {
	o := testMQTTDefaultOptions()
	s := testMQTTRunServer(t, o)
	defer testMQTTShutdownServer(s)

	// Publish an NBIRTH message. Make sure it is received as both the original
	// subject, and as a $sparkplug/certificates one.

	const NBORN = "NODE BORN FAKE MESSAGE"
	const DBORN = "DEVICE BORN FAKE MESSAGE"

	tests := []*struct {
		topic   string
		payload string
		flags   byte
		mc      net.Conn
		r       *mqttReader
	}{
		{
			topic:   "spBv1.0/ggg/NBIRTH/nnn",
			payload: NBORN,
			flags:   0,
		},
		{
			topic:   "spBv1.0/ggg/DBIRTH/nnn/ddd",
			payload: DBORN,
			flags:   0,
		},
		{
			topic:   "$sparkplug/certificates/spBv1.0/ggg/NBIRTH/nnn",
			payload: NBORN,
			flags:   mqttPubFlagRetain,
		},
		{
			topic:   "$sparkplug/certificates/spBv1.0/ggg/DBIRTH/nnn/ddd",
			payload: DBORN,
			flags:   mqttPubFlagRetain,
		},
	}

	t.Run("$sparkplug messages delivered", func(t *testing.T) {
		// Connect the subscribers, unique for each topic we monitor.
		for i, test := range tests {
			test.mc, test.r = testMQTTConnect(t, &mqttConnInfo{cleanSess: true, clientID: fmt.Sprintf("sub-%v", i)}, o.MQTT.Host, o.MQTT.Port)
			defer test.mc.Close()
			testMQTTCheckConnAck(t, test.r, mqttConnAckRCConnectionAccepted, false)

			// Subscribne at QoS2 to make sure the messages are posted at QoS0 and
			// not truncated to sub QoS.
			testMQTTSub(t, 1, test.mc, test.r, []*mqttFilter{{filter: test.topic, qos: 2}}, []byte{2})
		}

		// connect the publisher
		mc, r := testMQTTConnect(t, &mqttConnInfo{cleanSess: true, clientID: "pub"}, o.MQTT.Host, o.MQTT.Port)
		defer mc.Close()
		testMQTTCheckConnAck(t, r, mqttConnAckRCConnectionAccepted, false)

		// {Publish} [ND]BIRTH messages. Note that IRL these are protobuf payloads.
		testMQTTPublish(t, mc, r, 0, false, false, "spBv1.0/ggg/NBIRTH/nnn", 0, []byte("NODE BORN FAKE MESSAGE"))
		testMQTTPublish(t, mc, r, 0, false, false, "spBv1.0/ggg/DBIRTH/nnn/ddd", 0, []byte("DEVICE BORN FAKE MESSAGE"))

		// Check that the subscribers got the messages.
		for _, test := range tests {
			testMQTTCheckPubMsg(t, test.mc, test.r, test.topic, test.flags, []byte(test.payload))
		}
	})

	t.Run("$sparkplug messages retained", func(t *testing.T) {
		// Connect/subscribe again, and make sure that only retained messages are there.
		for i, test := range tests {
			mc, r := testMQTTConnect(t, &mqttConnInfo{cleanSess: true, clientID: fmt.Sprintf("sub-%v", i+100)}, o.MQTT.Host, o.MQTT.Port)
			defer mc.Close()
			testMQTTCheckConnAck(t, r, mqttConnAckRCConnectionAccepted, false)
			testMQTTSub(t, 1, mc, r, []*mqttFilter{{filter: test.topic, qos: 2}}, []byte{2})
			if test.flags&mqttPubFlagRetain != 0 {
				testMQTTCheckPubMsg(t, mc, r, test.topic, test.flags, []byte(test.payload))
			} else {
				testMQTTExpectNothing(t, r)
			}
		}
	})
}

func TestMQTTMaxPayloadEnforced(t *testing.T) {
	conf := createConfFile(t, []byte(`
		server_name: test_mqtt_max_payload
		port: -1
		max_payload: 1024
		mqtt { listen: "127.0.0.1:-1" }
		jetstream: { domain: "TEST", max_mem_store: 8MB, max_file_store: 8MB, store_dir: "`+t.TempDir()+`" }
	`))
	s, o := RunServerWithConfig(conf)
	defer s.Shutdown()

	mp := o.MQTT.Port
	host := o.MQTT.Host
	mc, _ := testMQTTConnect(t, &mqttConnInfo{clientID: "cid", cleanSess: true}, host, mp)
	defer mc.Close()

	oversized := bytes.Repeat([]byte{'A'}, 1500)
	testMQTTSendPublishPacket(t, mc, 0, false, false, "foo", 0, oversized)

	testMQTTExpectDisconnect(t, mc)
}

func TestMQTTMaxPayloadDoesNotCapPublishTopic(t *testing.T) {
	conf := createConfFile(t, []byte(`
		server_name: test_mqtt_max_payload_topic
		port: -1
		max_payload: 1024
		mqtt { listen: "127.0.0.1:-1" }
		jetstream: { domain: "TEST", max_mem_store: 8MB, max_file_store: 8MB, store_dir: "`+t.TempDir()+`" }
	`))
	s, o := RunServerWithConfig(conf)
	defer s.Shutdown()

	mc, r := testMQTTConnect(t, &mqttConnInfo{clientID: "cid", cleanSess: true}, o.MQTT.Host, o.MQTT.Port)
	defer mc.Close()
	testMQTTCheckConnAck(t, r, mqttConnAckRCConnectionAccepted, false)

	testMQTTSendPublishPacket(t, mc, 0, false, false, strings.Repeat("a", 1100), 0, nil)
	testMQTTFlush(t, mc, nil, r)
}

func TestMQTTIncompletePacketMaxPayloadViolationDisconnects(t *testing.T) {
	o := testMQTTDefaultOptions()
	o.MaxPayload = 1024
	s := testMQTTRunServer(t, o)
	defer testMQTTShutdownServer(s)

	for _, test := range []struct {
		name   string
		header byte
	}{
		{"publish", mqttPacketPub},
		{"subscribe", mqttPacketSub | mqttSubscribeFlags},
		{"unsubscribe", mqttPacketUnsub | mqttUnsubscribeFlags},
	} {
		t.Run(test.name, func(t *testing.T) {
			mc, r := testMQTTConnect(t, &mqttConnInfo{cleanSess: true}, o.MQTT.Host, o.MQTT.Port)
			defer mc.Close()
			testMQTTCheckConnAck(t, r, mqttConnAckRCConnectionAccepted, false)

			// Declare the maximum remaining length but only send a few bytes.
			w := newMQTTWriter(0)
			w.WriteByte(test.header)
			w.WriteVarInt(mqttMaxPayloadSize)
			w.WriteString("foo")
			w.Write(bytes.Repeat([]byte{'A'}, 100))

			_, err := testMQTTWrite(mc, w.Bytes())
			require_NoError(t, err)

			testMQTTExpectDisconnect(t, mc)
		})
	}
}

func TestMQTTJSApiMapping(t *testing.T) {
	td := t.TempDir()
	hubDir := filepath.Join(td, "hub")
	hubConf := createConfFile(t, fmt.Appendf(nil, `
        server_name: hub
        listen: "127.0.0.1:-1"
        jetstream {
            domain: "HUB"
            store_dir: "%s"
        }
        mqtt {
            listen: "127.0.0.1:-1"
        }
        leafnodes {
            listen: "127.0.0.1:-1"
            isolate: true
        }
        accounts: {
            SYS: { users: [ { user:s, password:x } ] }
            HUB: {
                jetstream: true
                users: [
                    {
                        user:h
                        password:x
                        permissions: {
                            subscribe: {allow: ["foo", "bar", "_INBOX.>", "$MQTT.>"], deny: ["baz"] }
                        }
                    }
                ]
            }
        }
        system_account: SYS
    `, hubDir))
	hub, ohub := RunServerWithConfig(hubConf)
	defer hub.Shutdown()

	leafDir := filepath.Join(td, "leaf")
	leafTmpl := `
        server_name: leaf
        listen: "127.0.0.1:-1"
        jetstream {
            domain: "LEAF"
            store_dir: "%s"
        }
        mqtt {
            listen: "127.0.0.1:-1"
            js_api_timeout: "500ms"
        }
        leafnodes {
            remotes [
                { urls: [ nats-leaf://s:x@127.0.0.1:%d ], account: SYS }
                { urls: [ nats-leaf://h:x@127.0.0.1:%d ], account: HUB }
            ]
        }
        accounts: {
            SYS: { users: [ { user:s, password:x } ] }
            HUB: {
                jetstream: false
                users: [ { user:h, password:x } ]
                mappings: {
                    "$JS.API.>" : "$JS.HUB.API.>"
                    %s
                }
            }
            LEAF: {
                jetstream: true
                users: [ { user:l, password:x } ]
            }
        }
        system_account: SYS
    `
	leafConf := createConfFile(t, fmt.Appendf(nil, leafTmpl, leafDir, ohub.LeafNode.Port, ohub.LeafNode.Port, ""))
	leaf, oleaf := RunServerWithConfig(leafConf)
	defer leaf.Shutdown()

	checkLeafNodeConnectedCount(t, leaf, 2)

	testRetained := func(user string, port int, msg string) {
		t.Helper()
		// Create a producer
		c, r := testMQTTConnect(t, &mqttConnInfo{
			cleanSess: true,
			user:      user,
			pass:      "x",
		}, "127.0.0.1", port)
		defer c.Close()
		testMQTTCheckConnAck(t, r, mqttConnAckRCConnectionAccepted, false)
		testMQTTPublish(t, c, r, 0, false, true, "foo", 0, []byte(msg))
		testMQTTFlush(t, c, nil, r)
		testMQTTDisconnect(t, c, nil)
		c.Close()

		// Create a consumer
		c, r = testMQTTConnect(t, &mqttConnInfo{
			cleanSess: true,
			user:      user,
			pass:      "x",
		}, "127.0.0.1", port)
		defer c.Close()
		testMQTTCheckConnAck(t, r, mqttConnAckRCConnectionAccepted, false)
		testMQTTSub(t, 1, c, r, []*mqttFilter{{filter: "foo", qos: 0}}, []byte{0})
		testMQTTCheckPubMsg(t, c, r, "foo", mqttPubFlagRetain, []byte(msg))
	}

	// Connect to leaf
	testRetained("l", oleaf.MQTT.Port, "hi leaf")

	// Now same test in the hub
	testRetained("h", ohub.MQTT.Port, "hi hub")

	// Now test the hub account in the leaf node. We expect it to fail
	// because we don't have the mapping for the sessions to be persisted
	// in the hub.
	_, _, err := testMQTTConnectRetryWithError(t, &mqttConnInfo{
		cleanSess: true,
		user:      "h",
		pass:      "x",
	}, oleaf.MQTT.Host, oleaf.MQTT.Port, 0)
	if err == nil {
		t.Fatal("Expected failure to connect, but did connect")
	}

	// Config reload the leaf server to add the missing mapping.
	reloadUpdateConfig(t, leaf, leafConf, fmt.Sprintf(leafTmpl,
		leafDir, ohub.LeafNode.Port, ohub.LeafNode.Port, `"$MQTT.sess.LEAF.>" : "$MQTT.sess.HUB.>"`))

	c, r := testMQTTConnect(t, &mqttConnInfo{
		cleanSess: true,
		user:      "h",
		pass:      "x",
	}, "127.0.0.1", oleaf.MQTT.Port)
	defer c.Close()
	testMQTTCheckConnAck(t, r, mqttConnAckRCConnectionAccepted, false)
	testMQTTSub(t, 1, c, r, []*mqttFilter{{filter: "foo", qos: 0}}, []byte{0})
	testMQTTCheckPubMsg(t, c, r, "foo", mqttPubFlagRetain, []byte("hi hub"))
}

func TestMQTTMappingsQoS0(t *testing.T) {
	td := t.TempDir()
	dir := filepath.Join(td, "js")
	conf := createConfFile(t, fmt.Appendf(nil, `
		server_name: server
		jetstream {
			domain: "A"
			store_dir: "%s"
		}
		mqtt {
			listen: "127.0.0.1:-1"
		}
		accounts: {
			A: {
				jetstream: true
				users: [ { user:a, password:x }]
				exports: [ { stream: "foo.>" }]
				mappings: { "baz.>" : "bazz.>" }
			}
			B: {
				jetstream: true
				users: [ { user:b, password:x }]
				imports: [ { stream: { account: A, subject: "foo.>" }, to: "bar.>" } ]
			}
			C: {
				jetstream: true
				users: [ { user:c, password:x }]
				imports: [ { stream: { account: A, subject: "foo.>" }, to: "baz.>" } ]
			}
			D: {
				jetstream: true
				users: [ { user:d, password:x }]
				# Same imports than for account C
				imports: [ { stream: { account: A, subject: "foo.>" }, to: "baz.>" } ]
			}
		}
	`, dir))
	s, o := RunServerWithConfig(conf)
	defer s.Shutdown()

	// First, we check for mapping inside the same account A.
	aSubConn, aSubReader := testMQTTConnect(t, &mqttConnInfo{
		cleanSess: true,
		clientID:  "sub",
		user:      "a",
		pass:      "x",
	}, "127.0.0.1", o.MQTT.Port)
	defer aSubConn.Close()
	testMQTTCheckConnAck(t, aSubReader, mqttConnAckRCConnectionAccepted, false)
	testMQTTSub(t, 1, aSubConn, aSubReader, []*mqttFilter{{filter: "#", qos: 0}}, []byte{0})
	testMQTTFlush(t, aSubConn, nil, aSubReader)

	aPubConn, aPubReader := testMQTTConnect(t, &mqttConnInfo{
		cleanSess: true,
		clientID:  "pub",
		user:      "a",
		pass:      "x",
	}, "127.0.0.1", o.MQTT.Port)
	defer aPubConn.Close()
	testMQTTCheckConnAck(t, aPubReader, mqttConnAckRCConnectionAccepted, false)
	testMQTTPublish(t, aPubConn, aPubReader, 0, false, true, "baz/x", 0, []byte("msg1"))
	// Because of mapping, the subscription should receive the message on "bazz/x"
	testMQTTCheckPubMsg(t, aSubConn, aSubReader, "bazz/x", 0, []byte("msg1"))

	// Helper to create a connection and sub for a given user on given topic
	createSub := func(user, topic string) (net.Conn, *mqttReader) {
		t.Helper()
		c, r := testMQTTConnect(t, &mqttConnInfo{
			cleanSess: true,
			clientID:  nuid.Next(),
			user:      user,
			pass:      "x",
		}, "127.0.0.1", o.MQTT.Port)
		testMQTTCheckConnAck(t, r, mqttConnAckRCConnectionAccepted, false)
		testMQTTSub(t, 1, c, r, []*mqttFilter{{filter: topic, qos: 0}}, []byte{0})
		testMQTTFlush(t, c, nil, r)
		return c, r
	}
	bConn1, bReader1 := createSub("b", "bar/#")
	defer bConn1.Close()
	bConn2, bReader2 := createSub("b", "bar/+")
	defer bConn2.Close()
	cConn1, cReader1 := createSub("c", "baz/#")
	defer cConn1.Close()
	cConn2, cReader2 := createSub("c", "baz/+")
	defer cConn2.Close()
	dConn1, dReader1 := createSub("d", "baz/#")
	defer dConn1.Close()
	dConn2, dReader2 := createSub("d", "baz/+")
	defer dConn2.Close()

	// Now from "A" publisher, publishes on "foo/x", and with mapping across
	// accounts, we should get the expected results. We send 2 messages in
	// a row and verify that we get those, and no more than that.
	testMQTTPublish(t, aPubConn, aPubReader, 0, false, true, "foo/x", 0, []byte("msg2"))
	testMQTTPublish(t, aPubConn, aPubReader, 0, false, true, "foo/x", 0, []byte("msg3"))

	// "B" consumers should receive on "bar/x"
	conns := []net.Conn{bConn1, bConn2}
	readers := []*mqttReader{bReader1, bReader2}
	for i := range len(conns) {
		testMQTTCheckPubMsg(t, conns[i], readers[i], "bar/x", 0, []byte("msg2"))
		testMQTTCheckPubMsg(t, conns[i], readers[i], "bar/x", 0, []byte("msg3"))
		testMQTTExpectNothing(t, readers[i])
	}
	// For "C" and "D" consumers, it should be "baz/x"
	conns = []net.Conn{cConn1, cConn2, dConn1, dConn2}
	readers = []*mqttReader{cReader1, cReader2, dReader1, dReader2}
	for i := range len(conns) {
		testMQTTCheckPubMsg(t, conns[i], readers[i], "baz/x", 0, []byte("msg2"))
		testMQTTCheckPubMsg(t, conns[i], readers[i], "baz/x", 0, []byte("msg3"))
		testMQTTExpectNothing(t, readers[i])
	}
}

func TestMQTTSliceHeadersAndDecodeRetainedMessage(t *testing.T) {
	// First check low level mqttSliceHeaders
	for _, test := range []struct {
		name     string
		hdr      string
		expected []string
	}{
		// Valid cases
		{"one key and some random", hdrLine + "key1:val1\r\nsomeotherkey:someval\r\n", []string{"val1", _EMPTY_, _EMPTY_}},
		{"two keys", hdrLine + "key2:val2\r\nthisisnotkey1:someval\r\nkey1:val1\r\n", []string{"val1", "val2", _EMPTY_}},
		{"three keys", hdrLine + "key2:val2\r\nkey3:val3\r\nkey1:val1\r\n", []string{"val1", "val2", "val3"}},
		{"space before value", hdrLine + "somekey:someval\r\nkey2:  val2withspacebefore\r\n", []string{_EMPTY_, "val2withspacebefore", _EMPTY_}},
		{"space between key and colon sign", hdrLine + "key1:val1\r\nkey2 :val2\r\nkey3  :  val3\r\n", []string{"val1", "val2", "val3"}},
		// Error cases
		{"no hdr line", "key1:val1\r\n", []string{_EMPTY_, _EMPTY_, _EMPTY_}},
		{"key length 0", hdrLine + "key1:val1\r\n:val2\r\nkey3:val3\r\n", []string{"val1", _EMPTY_, _EMPTY_}},
		{"key is only spaces", hdrLine + "key1:val1\r\nkey2:val2\r\n     :val3\r\n", []string{"val1", "val2", _EMPTY_}},
		{"value no crlf", hdrLine + "key1:val1\r\nkey2:val2", []string{"val1", _EMPTY_, _EMPTY_}},
	} {
		t.Run(test.name, func(t *testing.T) {
			headers := map[string][]byte{
				"key1": nil,
				"key2": nil,
				"key3": nil,
			}
			mqttSliceHeaders(headers, []byte(test.hdr))
			for i := range len(headers) {
				key := fmt.Sprintf("key%d", i+1)
				val := string(headers[key])
				if ev := test.expected[i]; ev != val {
					t.Fatalf("For key %q, expected value to be %q, got %q", key, ev, val)
				}
			}
		})
	}

	// Now test mqttDecodeRetainedMessage() itself.
	t.Run("flag with delete marker", func(t *testing.T) {
		hdr := fmt.Appendf(nil, "%sNmqtt-RFlags:%c1\r\n\r\n", hdrLine, mqttRetainedFlagDelMarker)
		rm, err := mqttDecodeRetainedMessage("$MQTT.rmsgs.bar.x", hdr, nil)
		require_NoError(t, err)
		require_Equal(t, rm.Flags, 1)
	})
	t.Run("flag not a number", func(t *testing.T) {
		hdr := fmt.Appendf(nil, "%sNmqtt-RFlags:bad\r\n\r\n", hdrLine)
		_, err := mqttDecodeRetainedMessage("$MQTT.rmsgs.bar.x", hdr, []byte("msg"))
		require_Error(t, err, errMQTTInvalidRetainFlags)
	})
	t.Run("flag not a number with delete marker", func(t *testing.T) {
		hdr := fmt.Appendf(nil, "%sNmqtt-RFlags:%cad\r\n\r\n", hdrLine, mqttRetainedFlagDelMarker)
		_, err := mqttDecodeRetainedMessage("$MQTT.rmsgs.bar.x", hdr, []byte("msg"))
		require_Error(t, err, errMQTTInvalidRetainFlags)
	})
	t.Run("flag too big", func(t *testing.T) {
		hdr := fmt.Appendf(nil, "%sNmqtt-RFlags:%c15\r\n\r\n", hdrLine, mqttRetainedFlagDelMarker)
		_, err := mqttDecodeRetainedMessage("$MQTT.rmsgs.bar.x", hdr, []byte("msg"))
		require_Error(t, err, errMQTTInvalidRetainFlags)
	})
	t.Run("flag invalid qos", func(t *testing.T) {
		hdr := fmt.Appendf(nil, "%sNmqtt-RFlags:%c7\r\n\r\n", hdrLine, mqttRetainedFlagDelMarker)
		_, err := mqttDecodeRetainedMessage("$MQTT.rmsgs.bar.x", hdr, []byte("msg"))
		require_Error(t, err, errMQTTInvalidRetainFlags)
	})
	t.Run("decode retained msg with space before header value", func(t *testing.T) {
		msg, hdrLen := mqttEncodeRetainedMessage(&mqttRetainedMsg{
			Topic:  "foo/x",
			Origin: "  Origin", // Add spaces in front on purpose
			Source: "Source",
			Flags:  1,
			Msg:    []byte("msg1"),
		})
		hdr := msg[:hdrLen]
		msg = msg[hdrLen:]
		rm, err := mqttDecodeRetainedMessage("$MQTT.rmsgs.foo.x", hdr, msg)
		require_NoError(t, err)
		require_Equal(t, rm.Topic, "foo/x")
		require_Equal(t, rm.Subject, "foo.x")
		require_Equal(t, rm.Origin, "Origin")
		require_Equal(t, rm.Source, "Source")
		require_Equal(t, rm.Flags, 1)
		require_Equal(t, string(rm.Msg), "msg1")
	})
	t.Run("decode retained msg with subject transformed", func(t *testing.T) {
		msg, hdrLen := mqttEncodeRetainedMessage(&mqttRetainedMsg{
			Topic:  "foo/x",
			Origin: "Origin",
			Source: "Source",
			Flags:  1,
			Msg:    []byte("msg2"),
		})
		hdr := msg[:hdrLen]
		msg = msg[hdrLen:]
		// Use different subject when calling the function. Make sure the
		// topic is properly reflecting the subject.
		rm, err := mqttDecodeRetainedMessage("$MQTT.rmsgs.bar.x", hdr, msg)
		require_NoError(t, err)
		require_Equal(t, rm.Topic, "bar/x")
		require_Equal(t, rm.Subject, "bar.x")
		require_Equal(t, rm.Origin, "Origin")
		require_Equal(t, rm.Source, "Source")
		require_Equal(t, rm.Flags, 1)
		require_Equal(t, string(rm.Msg), "msg2")
	})
	t.Run("decode deleted retained message", func(t *testing.T) {
		msg, hdrLen := mqttEncodeRetainedMessage(&mqttRetainedMsg{
			Topic:  "foo/x",
			Origin: "Origin",
			Source: "Source",
			Flags:  1,
			Msg:    nil,
		})
		hdr := msg[:hdrLen]
		msg = msg[hdrLen:]
		// Use different subject too
		rm, err := mqttDecodeRetainedMessage("$MQTT.rmsgs.bar.x", hdr, msg)
		require_NoError(t, err)
		require_Equal(t, rm.Topic, "bar/x")
		require_Equal(t, rm.Subject, "bar.x")
		require_Equal(t, rm.Origin, "Origin")
		require_Equal(t, rm.Source, "Source")
		require_Equal(t, rm.Flags, 1)
		require_Len(t, len(rm.Msg), 0)
	})
	t.Run("decode retained message as JSON with bad flags", func(t *testing.T) {
		rmo := &mqttRetainedMsg{Flags: 15}
		msg, err := json.Marshal(rmo)
		require_NoError(t, err)
		_, err = mqttDecodeRetainedMessage("$MQTT.rmsgs.foo.x", nil, msg)
		require_Error(t, err, errMQTTInvalidRetainFlags)
	})
	t.Run("decode retained message as JSON null", func(t *testing.T) {
		// A JSON `null` decodes into a nil pointer without an error, which
		// used to be dereferenced when checking the flags.
		_, err := mqttDecodeRetainedMessage("$MQTT.rmsgs.foo.x", nil, []byte("null"))
		require_Error(t, err, errMQTTInvalidRetainedMessage)
	})
	t.Run("decode retained message as JSON subject transform", func(t *testing.T) {
		rmo := &mqttRetainedMsg{
			Topic:  "foo/x",
			Origin: "Origin",
			Source: "Source",
			Flags:  1,
			Msg:    []byte("hello"),
		}
		msg, err := json.Marshal(rmo)
		require_NoError(t, err)
		rm, err := mqttDecodeRetainedMessage("$MQTT.rmsgs.bar.x", nil, msg)
		require_NoError(t, err)
		require_Equal(t, rm.Topic, "bar/x")
		require_Equal(t, rm.Subject, "bar.x")
		require_Equal(t, rm.Origin, "Origin")
		require_Equal(t, rm.Source, "Source")
		require_Equal(t, rm.Flags, 1)
		require_Equal(t, string(rm.Msg), "hello")
	})
}

func TestMQTTPoisonedJSONRecordsDoNotCrashServer(t *testing.T) {
	o := testMQTTDefaultOptions()
	s := testMQTTRunServer(t, o)
	defer testMQTTShutdownServer(s)

	// A first connection creates the account session manager, which is what
	// installs the internal subscriptions that process the records below.
	mc, mr := testMQTTConnect(t, &mqttConnInfo{clientID: "first", cleanSess: true}, o.MQTT.Host, o.MQTT.Port)
	defer mc.Close()
	testMQTTCheckConnAck(t, mr, mqttConnAckRCConnectionAccepted, false)

	nc := natsConnect(t, s.ClientURL())
	defer nc.Close()

	// A retained message stored as the JSON literal `null` decodes into a nil
	// *mqttRetainedMsg with no error. Processed on the stream's internal loop.
	if _, err := nc.Request(mqttRetainedMsgsStreamSubject+"x", []byte("null"), time.Second); err != nil {
		t.Fatalf("Error storing retained message: %v", err)
	}

	// A session persist reply that carries none of the PubAck fields leaves the
	// embedded *PubAck nil. The JS id token has to differ from this server's for
	// the record not to be ignored as our own.
	natsPub(t, nc, mqttJSARepliesPrefix+nuid.Next()+"."+mqttJSASessPersist+"."+getHash("first")+"."+nuid.Next(), []byte("{}"))
	natsFlush(t, nc)

	// A session record with a `null` consumer decodes into a present key with a
	// nil *ConsumerConfig, which is dereferenced when the session is cleared.
	poisonCID := "poison"
	sessSubj := mqttSessStreamSubjectPrefix + getHash(poisonCID)
	sessRec := fmt.Appendf(nil, `{"id":%q,"cons":{"sub1":null}}`, poisonCID)
	if _, err := nc.Request(sessSubj, sessRec, time.Second); err != nil {
		t.Fatalf("Error storing session record: %v", err)
	}

	// Connecting with a clean session recovers that record and then clears it.
	pc, pr := testMQTTConnect(t, &mqttConnInfo{clientID: poisonCID, cleanSess: true}, o.MQTT.Host, o.MQTT.Port)
	defer pc.Close()
	testMQTTCheckConnAck(t, pr, mqttConnAckRCConnectionAccepted, false)

	// The server must still be running, and the retained message machinery
	// still functional, which means the internal loop did not go away.
	testMQTTPublish(t, mc, mr, 0, false, true, "foo/bar", 0, []byte("msg"))

	sc, sr := testMQTTConnect(t, &mqttConnInfo{clientID: "sub", cleanSess: true}, o.MQTT.Host, o.MQTT.Port)
	defer sc.Close()
	testMQTTCheckConnAck(t, sr, mqttConnAckRCConnectionAccepted, false)
	testMQTTSub(t, 1, sc, sr, []*mqttFilter{{filter: "foo/bar", qos: 0}}, []byte{0})
	testMQTTCheckPubMsg(t, sc, sr, "foo/bar", mqttPubFlagRetain, []byte("msg"))
}

// mqttNewPoisonASM returns an isolated account session manager whose JS API send
// queue is never drained, so no real JetStream ever answers a request made
// through it. Requests block until a forged reply is injected.
func mqttNewPoisonASM(s *Server) *mqttAccountSessionManager {
	id := "POISONID"
	return &mqttAccountSessionManager{
		sessions:   make(map[string]*mqttSession),
		sessByHash: make(map[string]*mqttSession),
		sessLocked: make(map[string]struct{}),
		flappers:   make(map[string]time.Time),
		jsa: mqttJSA{
			id:      id,
			rplyr:   mqttJSARepliesPrefix + id + ".",
			sendq:   newIPQueue[*mqttJSPubMsg](s, "test-poison-send-"+nuid.Next()),
			nuid:    nuid.New(),
			quitCh:  make(chan struct{}),
			timeout: 10 * time.Second,
		},
		rmsCache: &sync.Map{},
	}
}

// mqttRunWithPoisonedReply runs fn, which must issue a JS API request through
// as.jsa and block waiting for the reply. Once the request is in flight, `reply`
// is injected as the forged reply body on the exact subject the request
// registered. Fails the test if fn panics (the pre-fix crash) or never returns.
// Any value fn assigns is safe to read after this returns.
func mqttRunWithPoisonedReply(t *testing.T, as *mqttAccountSessionManager, reply []byte, fn func()) {
	t.Helper()
	donec := make(chan any, 1)
	go func() {
		defer func() { donec <- recover() }()
		fn()
	}()

	// Wait for the request to register its reply channel, then inject the
	// forged reply on that exact subject.
	var replySubj string
	for i := 0; i < 400 && replySubj == _EMPTY_; i++ {
		as.jsa.replies.Range(func(k, _ any) bool {
			replySubj = k.(string)
			return false
		})
		if replySubj == _EMPTY_ {
			time.Sleep(5 * time.Millisecond)
		}
	}
	require_True(t, replySubj != _EMPTY_)
	as.processJSAPIReplies(nil, nil, nil, replySubj, _EMPTY_, reply)

	select {
	case p := <-donec:
		if p != nil {
			t.Fatalf("panicked on poisoned reply %q: %v", reply, p)
		}
	case <-time.After(15 * time.Second):
		t.Fatalf("did not return after injecting poisoned reply %q", reply)
	}
}

func TestMQTTJSAPIPoisonedRepliesDoNotCrash(t *testing.T) {
	o := testMQTTDefaultOptions()
	s := testMQTTRunServer(t, o)
	defer testMQTTShutdownServer(s)

	for _, tc := range []struct {
		name    string
		reply   []byte
		wantErr bool
		fn      func(jsa *mqttJSA) error
	}{
		// `null` exercises the &resp double-pointer decode sites (findings F, J, C).
		{"stream_lookup_null", []byte("null"), true, func(jsa *mqttJSA) error {
			_, err := jsa.lookupStream("foo")
			return err
		}},
		{"stream_delete_null", []byte("null"), false, func(jsa *mqttJSA) error {
			_, err := jsa.deleteStream("foo")
			return err
		}},
		{"msg_load_null", []byte("null"), true, func(jsa *mqttJSA) error {
			_, err := jsa.loadMsg("foo", 1)
			return err
		}},
		// `{}` exercises the inner nil-pointer sites that survive the decode
		// (findings G, H1, I, D, H2).
		{"stream_lookup_empty", []byte("{}"), true, func(jsa *mqttJSA) error {
			_, err := jsa.lookupStream("foo")
			return err
		}},
		{"stream_create_empty", []byte("{}"), true, func(jsa *mqttJSA) error {
			_, _, err := jsa.createStream(&StreamConfig{Name: "foo", Storage: FileStorage})
			return err
		}},
		{"stream_update_empty", []byte("{}"), true, func(jsa *mqttJSA) error {
			_, err := jsa.updateStream(&StreamConfig{Name: "foo", Storage: FileStorage})
			return err
		}},
		{"msg_load_empty", []byte("{}"), true, func(jsa *mqttJSA) error {
			_, err := jsa.loadMsg("foo", 1)
			return err
		}},
		// The same inner *StoredMsg guard is replicated in the other two message
		// load accessors, so cover them too. A `null` case is not repeated here:
		// all three accessors share the mqttJSAMsgLoad decode site, so
		// msg_load_null above already covers that regression for them.
		{"msg_load_last_for_empty", []byte("{}"), true, func(jsa *mqttJSA) error {
			_, err := jsa.loadLastMsgFor("foo", "bar")
			return err
		}},
		{"msg_load_next_for_empty", []byte("{}"), true, func(jsa *mqttJSA) error {
			_, err := jsa.loadNextMsgFor("foo", "bar")
			return err
		}},
		{"pub_ack_empty", []byte("{}"), true, func(jsa *mqttJSA) error {
			sess := mqttSessionCreate(jsa, "cid", getHash("cid"), 0, o)
			return sess.save()
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			as := mqttNewPoisonASM(s)
			var err error
			mqttRunWithPoisonedReply(t, as, tc.reply, func() { err = tc.fn(&as.jsa) })
			if tc.wantErr {
				require_Error(t, err)
			} else {
				require_NoError(t, err)
			}
		})
	}
}

func TestMQTTLoadRetainedMessagesPoisonedReplyDoesNotCrash(t *testing.T) {
	o := testMQTTDefaultOptions()
	s := testMQTTRunServer(t, o)
	defer testMQTTShutdownServer(s)

	as := mqttNewPoisonASM(s)
	var rms map[string]*mqttRetainedMsg
	// A `{}` body decodes into a non-nil JSApiMsgGetResponse with a nil Message.
	mqttRunWithPoisonedReply(t, as, []byte("{}"), func() {
		rms = as.loadRetainedMessages(map[string]uint64{"foo": 1}, s)
	})
	// The poisoned message is skipped, so nothing is returned.
	require_True(t, len(rms) == 0)
}

func TestMQTTRetainedMessageWithDelSubjectIsNotRestored(t *testing.T) {
	tdir := t.TempDir()
	tmpl := `
		listen: 127.0.0.1:-1
		server_name: mqtt
		jetstream {
			store_dir = %q
		}

		mqtt {
			listen: 127.0.0.1:-1
			consumer_inactive_threshold: %q
		}

		# For access to system account.
		accounts { $SYS { users = [ { user: "admin", pass: "s3cr3t!" } ] } }
	`
	conf := createConfFile(t, fmt.Appendf(nil, tmpl, tdir, "0.2s"))
	s, o := RunServerWithConfig(conf)
	defer testMQTTShutdownServer(s)

	// Publish one normal retained message through MQTT.
	mc, r := testMQTTConnectRetry(t, &mqttConnInfo{cleanSess: true}, o.MQTT.Host, o.MQTT.Port, 5)
	defer mc.Close()
	testMQTTCheckConnAck(t, r, mqttConnAckRCConnectionAccepted, false)
	testMQTTPublish(t, mc, r, 0, false, true, "foo/ok", 0, []byte("ok"))
	mc.Close()

	// Store a legacy retained message directly into JetStream using a subject
	// that contains DEL. That subject can no longer be indexed after restore.
	nc, js := jsClientConnect(t, s)
	defer nc.Close()
	rm := mqttRetainedMsg{
		Origin:  "test",
		Subject: "foo.\x7fbad",
		Topic:   "foo/\x7fbad",
		Flags:   mqttPubFlagRetain,
		Msg:     []byte("bad"),
	}
	jsonData, _ := json.Marshal(rm)
	_, err := js.PublishMsg(&nats.Msg{
		Subject: mqttRetainedMsgsStreamSubject + rm.Subject,
		Data:    jsonData,
	})
	require_NoError(t, err)

	// Restart the server so retained-message recovery runs through
	// processRetainedMsg().
	s.Shutdown()
	s = RunServer(o)
	defer testMQTTShutdownServer(s)

	mc, r = testMQTTConnectRetry(t, &mqttConnInfo{cleanSess: true}, o.MQTT.Host, o.MQTT.Port, 5)
	defer mc.Close()
	testMQTTCheckConnAck(t, r, mqttConnAckRCConnectionAccepted, false)
	testMQTTSub(t, 1, mc, r, []*mqttFilter{{filter: "foo/+", qos: 0}}, []byte{0})
	testMQTTCheckPubMsg(t, mc, r, "foo/ok", mqttPubFlagRetain, []byte("ok"))
	testMQTTExpectNothing(t, r)
}

func TestMQTTRetainedMsgRemovedFromMapIfNotInStream(t *testing.T) {
	mqttRetainedCacheTTL = 250 * time.Millisecond
	defer func() { mqttRetainedCacheTTL = mqttDefaultRetainedCacheTTL }()

	o := testMQTTDefaultOptions()
	s := testMQTTRunServer(t, o)
	defer testMQTTShutdownServer(s)

	c, r := testMQTTConnect(t, &mqttConnInfo{clientID: "pub", cleanSess: true}, o.MQTT.Host, o.MQTT.Port)
	defer c.Close()
	testMQTTCheckConnAck(t, r, mqttConnAckRCConnectionAccepted, false)
	testMQTTPublish(t, c, r, 0, false, true, "foo", 0, []byte("msg1"))
	testMQTTFlush(t, c, nil, r)

	checkRetained := func(expected string) {
		t.Helper()
		c, r := testMQTTConnect(t, &mqttConnInfo{cleanSess: true}, o.MQTT.Host, o.MQTT.Port)
		defer c.Close()
		testMQTTCheckConnAck(t, r, mqttConnAckRCConnectionAccepted, false)
		testMQTTSub(t, 1, c, r, []*mqttFilter{{filter: "foo", qos: 0}}, []byte{0})
		if expected == _EMPTY_ {
			testMQTTExpectNothing(t, r)
		} else {
			testMQTTCheckPubMsg(t, c, r, "foo", mqttPubFlagRetain, []byte(expected))
		}
	}
	checkRetained("msg1")

	testMQTTPublish(t, c, r, 0, false, true, "foo", 0, []byte("msg2"))
	testMQTTFlush(t, c, nil, r)

	checkRetained("msg2")

	// Now we will get the current sequence for the retained message and
	// remove it from the stream. We expect to get a warning that indicates
	// that the load failed. Restarting the subscription should not longer
	// cause this warning and the retained message should have been removed
	// from the map/cache.
	l := &captureWarnLogger{warn: make(chan string, 10)}
	s.SetLogger(l, false, false)

	asm := testMQTTGetAccountSessionManager(t, s, "pub")
	// Make sure it is in the cache
	rm := asm.getCachedRetainedMsg("foo")
	require_NotNil(t, rm)
	// Get the retained message sequence from the tree.
	asm.mu.RLock()
	rf, ok := asm.retmsgs.Find([]byte("foo"))
	asm.mu.RUnlock()
	require_True(t, ok)
	nc, js := jsClientConnect(t, s)
	defer nc.Close()
	err := js.DeleteMsg(mqttRetainedMsgsStreamName, rf.sseq)
	require_NoError(t, err)

	// Wait for more than the cache TTL
	time.Sleep(2 * mqttRetainedCacheTTL)

	checkRetained(_EMPTY_)

	// We should have got a warning.
	select {
	case w := <-l.warn:
		if !strings.Contains(w, ApiErrors[JSNoMessageFoundErr].Description) {
			t.Fatalf("Unexpected warning: %q", w)
		}
	case <-time.After(time.Second):
		t.Fatalf("Test timed out")
	}

	// But restarting it should not cause the server to try to load the retained
	// message again. So we should not have a warning.
	checkRetained(_EMPTY_)

	select {
	case w := <-l.warn:
		if strings.Contains(w, ApiErrors[JSNoMessageFoundErr].Description) {
			t.Fatalf("Got the warning: %q", w)
		}
	case <-time.After(250 * time.Millisecond):
		// OK
	}

	// Finally, check that the retmsgs map is empty.
	asm.mu.RLock()
	ok = asm.retmsgs.Size() == 0
	asm.mu.RUnlock()
	require_True(t, ok)
}

func TestMQTTCrossAccountRetain(t *testing.T) {
	for _, test := range []struct {
		name            string
		importTransform string
		sourceDest      string
		bDest           string
		getLastMsgSubj  string
	}{
		{"without transform", "", "", "foo/x", "$MQTT.rmsgs.foo.x"},
		{"with transform", `, to: "foobar.>"`, "$MQTT.rmsgs.foobar.>", "foobar/x", "$MQTT.rmsgs.foobar.x"},
	} {
		t.Run(test.name, func(t *testing.T) {
			td := t.TempDir()
			dir := filepath.Join(td, "js")
			conf := createConfFile(t, fmt.Appendf(nil, `
				server_name: server
				listen: "127.0.0.1:-1"
				jetstream {
					domain: "MYDOMAIN"
					store_dir: "%s"
				}
				mqtt {
					listen: "127.0.0.1:-1"
				}
				accounts: {
					A: {
						jetstream: true
						users: [ { user:a, password:x }]
						exports: [
							{ stream: "foo.>" }
							{ service: "$JS.API.>", response_type: stream }
							{ stream: "a2b.>" }
						]
					}
					B: {
						jetstream: true
						users: [ { user:b, password:x }]
						imports: [
							{ stream: { account: A, subject: "foo.>" }%s }
							{ service: { account: A, subject: "$JS.API.>"}, to: "A.$JS.API.>" }
							{ stream: { account: A, subject: "a2b.>" } }
						]
					}
				}
			`, dir, test.importTransform))
			s, o := RunServerWithConfig(conf)
			defer s.Shutdown()

			// Connect a user on "B" to create the MQTT assets.
			c, r := testMQTTConnect(t, &mqttConnInfo{cleanSess: true, user: "b", pass: "x"}, "127.0.0.1", o.MQTT.Port)
			defer c.Close()
			testMQTTCheckConnAck(t, r, mqttConnAckRCConnectionAccepted, false)
			testMQTTDisconnect(t, c, nil)
			c.Close()

			pubRetained := func(user, dest, msg string) {
				t.Helper()
				c, r := testMQTTConnect(t, &mqttConnInfo{
					cleanSess: true,
					user:      user,
					pass:      "x",
				}, "127.0.0.1", o.MQTT.Port)
				defer c.Close()
				testMQTTCheckConnAck(t, r, mqttConnAckRCConnectionAccepted, false)
				testMQTTPublish(t, c, r, 0, false, true, dest, 0, []byte(msg))
				testMQTTFlush(t, c, nil, r)
			}

			// Publish a retained message from account "A".
			retainInAMsg := "Retain in A"
			pubRetained("a", "foo/x", retainInAMsg)

			// Now we are going to do something unusual, which is to update
			// the MQTT retain stream in "B" to source from "A".
			nc, js := jsClientConnect(t, s, nats.UserInfo("b", "x"))
			defer nc.Close()

			si, err := js.StreamInfo(mqttRetainedMsgsStreamName)
			require_NoError(t, err)
			src := &nats.StreamSource{
				Name: mqttRetainedMsgsStreamName,
				External: &nats.ExternalStream{
					APIPrefix:     "A.$JS.API",
					DeliverPrefix: "a2b",
				},
				SubjectTransforms: []nats.SubjectTransformConfig{
					{
						Source:      mqttRetainedMsgsStreamSubject + "foo.>",
						Destination: test.sourceDest,
					},
				},
			}
			si.Config.Sources = []*nats.StreamSource{src}
			_, err = js.UpdateStream(&si.Config)
			require_NoError(t, err)

			// Now wait to make sure that the "B" retained messages stream
			// contains the message with body "Retain in A"
			checkFor(t, 2*time.Second, 15*time.Millisecond, func() error {
				msg, err := js.GetLastMsg(mqttRetainedMsgsStreamName, test.getLastMsgSubj)
				if err != nil {
					return err
				}
				if !bytes.Contains(msg.Data, []byte(retainInAMsg)) {
					return fmt.Errorf("Message is not from A: %q", msg.Data)
				}
				return nil
			})

			getRetained := func(user, dest, msg string) {
				t.Helper()
				c, r := testMQTTConnect(t, &mqttConnInfo{
					cleanSess: true,
					user:      user,
					pass:      "x",
				}, "127.0.0.1", o.MQTT.Port)
				testMQTTCheckConnAck(t, r, mqttConnAckRCConnectionAccepted, false)
				testMQTTSub(t, 1, c, r, []*mqttFilter{{filter: dest, qos: 0}}, []byte{0})
				if msg == _EMPTY_ {
					testMQTTExpectNothing(t, r)
				} else {
					testMQTTCheckPubMsg(t, c, r, dest, mqttPubFlagRetain, []byte(msg))
				}
			}

			// The retained message in account A should of course be "Retain in A"
			getRetained("a", "foo/x", retainInAMsg)
			// But because of the sourcing, the retained message in "B" should be
			// the retained message from the "A" account.
			getRetained("b", test.bDest, retainInAMsg)

			// Now publish a retained message from "B" account and make sure that
			// it is correctly replacing "Retain in A".
			retainInBMsg := "Retain in B"
			pubRetained("b", test.bDest, retainInBMsg)
			// Check that we can receive it.
			getRetained("b", test.bDest, retainInBMsg)
			// And "A" still has the "Retain in A" message.
			getRetained("a", "foo/x", retainInAMsg)

			// Publish from "A" a new message:
			retainInAMsg = "Retain in A2"
			pubRetained("a", "foo/x", retainInAMsg)
			// Make sure that this retained appears on "A" and "B".
			getRetained("a", "foo/x", retainInAMsg)
			getRetained("b", test.bDest, retainInAMsg)

			// Now publish an empty body retained message from "A". This
			// should remove the retained message from both "A" and "B".
			pubRetained("a", "foo/x", _EMPTY_)

			// We will check that the message gets removed from the stream.
			checkFor(t, 2*time.Second, 15*time.Millisecond, func() error {
				_, err := js.GetLastMsg(mqttRetainedMsgsStreamName, test.getLastMsgSubj)
				if err == nats.ErrMsgNotFound {
					return nil
				}
				return fmt.Errorf("Message still present or unexpected error %v", err)
			})
			// The helper will use "expect nothing" if the given string is empty.
			getRetained("a", "foo/x", _EMPTY_)
			getRetained("b", test.bDest, _EMPTY_)
		})
	}
}

// TestMQTTSessionPersistSpoofPrevented verifies that an MQTT client cannot
// publish to internal $MQTT.JSA.* subjects to spoof session-persist messages
// and evict other sessions.
func TestMQTTSessionPersistSpoofPrevented(t *testing.T) {
	o := testMQTTDefaultOptions()
	o.Users = []*User{
		{
			Username: "victim",
			Password: "pass",
			Permissions: &Permissions{
				Publish:   &SubjectPermission{Allow: []string{"foo.>"}},
				Subscribe: &SubjectPermission{Allow: []string{"foo.>"}},
			},
		},
		{
			Username: "attacker",
			Password: "pass",
			Permissions: &Permissions{
				Publish:   &SubjectPermission{Allow: []string{"foo.>"}},
				Subscribe: &SubjectPermission{Allow: []string{"foo.>"}},
			},
		},
	}
	s := testMQTTRunServer(t, o)
	defer testMQTTShutdownServer(s)

	// Connect the victim MQTT client with a persistent session.
	vc, vr := testMQTTConnect(t, &mqttConnInfo{
		clientID:  "victim",
		user:      "victim",
		pass:      "pass",
		cleanSess: true,
	}, o.MQTT.Host, o.MQTT.Port)
	defer vc.Close()
	testMQTTCheckConnAck(t, vr, mqttConnAckRCConnectionAccepted, false)
	testMQTTSub(t, 1, vc, vr, []*mqttFilter{{filter: "foo/+", qos: 1}}, []byte{1})
	testMQTTFlush(t, vc, nil, vr)

	// Connect the attacker MQTT client.
	ac, ar := testMQTTConnect(t, &mqttConnInfo{
		clientID:  "attacker",
		user:      "attacker",
		pass:      "pass",
		cleanSess: true,
	}, o.MQTT.Host, o.MQTT.Port)
	defer ac.Close()
	testMQTTCheckConnAck(t, ar, mqttConnAckRCConnectionAccepted, false)

	// Craft a forged session-persist message targeting the victim's session.
	// MQTT topic "/" maps to NATS ".", so this becomes:
	//   $MQTT.JSA.fakenode.SP.<victimHash>.fakeuuid
	victimHash := getHash("victim")
	spoofTopic := fmt.Sprintf("$MQTT/JSA/fakenode/%s/%s/fakeuuid", mqttJSASessPersist, victimHash)
	forgedPayload := []byte(`{"stream":"` + mqttSessStreamName + `","seq":999}`)

	// Attempt the spoofed publish. With the fix, the implicit allow bypass
	// for MQTT clients on $MQTT.* subjects is removed, so this publish is
	// denied by the permission check (attacker only has "foo.>" allow).
	testMQTTPublish(t, ac, ar, 0, false, false, spoofTopic, 0, forgedPayload)
	testMQTTFlush(t, ac, nil, ar)

	// Give the server time to process if the message leaked through.
	time.Sleep(250 * time.Millisecond)

	// Verify the victim session was NOT evicted by round-tripping a message.
	testMQTTPublish(t, ac, ar, 0, false, false, "foo/bar", 0, []byte("still-alive"))
	testMQTTCheckPubMsg(t, vc, vr, "foo/bar", 0, []byte("still-alive"))
}

func TestMQTTPublishSubjectLeafNodeParserFailures(t *testing.T) {
	const (
		hdrLen           = 25
		leafControlLimit = MAX_CONTROL_LINE_SIZE * 16
	)

	hdr := []byte("NATS/1.0\r\nNmqtt-Pub:0\r\n\r\n")
	require_Equal(t, len(hdr), hdrLen)

	convertMQTTTopic := func(t *testing.T, topic []byte) []byte {
		t.Helper()
		require_NoError(t, mqttValidateTopic(topic, "topic"))
		subject, err := mqttTopicToNATSPubSubject(topic)
		require_NoError(t, err)
		return subject
	}

	parseLeafHMSG := func(subject []byte) error {
		c := dummyClient()
		c.kind = LEAF
		c.leaf = &leaf{}
		c.flags.set(connectReceived)
		proto := fmt.Appendf(nil, "HMSG %s %d %d\r\n", subject, hdrLen, hdrLen)
		proto = append(proto, hdr...)
		proto = append(proto, _CRLF_...)
		return c.parse(proto)
	}

	leafHMSGArgLen := func(subject []byte) int {
		return len(subject) + len(" 25 25")
	}

	makeRepeatedLevelTopic := func(levels int) []byte {
		topic := bytes.Repeat([]byte("a."), levels)
		return topic[:len(topic)-1]
	}

	makeSlashExpansionTopic := func(t *testing.T) []byte {
		t.Helper()
		var topic []byte
		for {
			if len(topic) > 0 {
				topic = append(topic, '/', '/')
			}
			topic = append(topic, 'a')
			subject := convertMQTTTopic(t, topic)
			if len(subject) > leafControlLimit-len(" 25 25") {
				return topic
			}
		}
	}

	for _, test := range []struct {
		name          string
		topic         []byte
		wantErr       bool
		wantExpansion bool
		wantArgLen    int
	}{
		{
			name:       "converted subject at leaf control line limit is accepted",
			topic:      bytes.Repeat([]byte{'a'}, leafControlLimit-len(" 25 25")),
			wantErr:    false,
			wantArgLen: leafControlLimit,
		},
		{
			name:       "literal topic exceeds leaf control line limit",
			topic:      bytes.Repeat([]byte{'a'}, leafControlLimit-len(" 25 25")+1),
			wantErr:    true,
			wantArgLen: leafControlLimit + 1,
		},
		{
			name:          "dot expansion can make a shorter topic exceed leaf control line limit",
			topic:         makeRepeatedLevelTopic(21845),
			wantErr:       true,
			wantExpansion: true,
		},
		{
			name:          "slash escaping can make a shorter topic exceed leaf control line limit",
			topic:         makeSlashExpansionTopic(t),
			wantErr:       true,
			wantExpansion: true,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			subject := convertMQTTTopic(t, test.topic)
			if test.wantArgLen > 0 {
				require_Equal(t, leafHMSGArgLen(subject), test.wantArgLen)
			}
			if test.wantExpansion {
				require_True(t, len(test.topic) <= len(subject))
			}
			if err := parseLeafHMSG(subject); test.wantErr {
				require_Error(t, err, ErrMaxControlLine)
			} else {
				require_NoError(t, err)
			}
		})
	}

	for _, test := range []struct {
		name  string
		topic []byte
	}{
		{name: "space", topic: []byte("foo bar")},
		{name: "tab", topic: []byte("foo\tbar")},
		{name: "carriage return", topic: []byte("foo\rbar")},
		{name: "line feed", topic: []byte("foo\nbar")},
		{name: "carriage return line feed", topic: []byte("foo\r\nbar")},
		{name: "null", topic: []byte("foo\x00bar")},
		{name: "invalid utf8", topic: []byte{0xff}},
		{name: "single level wildcard", topic: []byte("foo/+")},
		{name: "multi level wildcard", topic: []byte("foo/#")},
	} {
		t.Run("rejected before leaf forwarding/"+test.name, func(t *testing.T) {
			if err := mqttValidateTopic(test.topic, "topic"); err == nil {
				_, err = mqttTopicToNATSPubSubject(test.topic)
				require_Error(t, err)
			}
		})
	}
}

//////////////////////////////////////////////////////////////////////////
//
// Benchmarks
//
//////////////////////////////////////////////////////////////////////////

const (
	mqttPubSubj     = "a"
	mqttBenchBufLen = 32768
)

func mqttBenchPubQoS0(b *testing.B, subject, payload string, numSubs int) {
	b.StopTimer()
	b.ReportAllocs()
	o := testMQTTDefaultOptions()
	s := RunServer(o)
	defer testMQTTShutdownServer(s)

	ci := &mqttConnInfo{clientID: "pub", cleanSess: true}
	c, br := testMQTTConnect(b, ci, o.MQTT.Host, o.MQTT.Port)
	testMQTTCheckConnAck(b, br, mqttConnAckRCConnectionAccepted, false)
	w := newMQTTWriter(0)
	testMQTTWritePublishPacket(b, w, 0, false, false, subject, 0, []byte(payload))
	sendOp := w.Bytes()

	dch := make(chan error, 1)
	totalSize := int64(len(sendOp))
	cdch := 0

	createSub := func(i int) {
		ci := &mqttConnInfo{clientID: fmt.Sprintf("sub%d", i), cleanSess: true}
		cs, brs := testMQTTConnect(b, ci, o.MQTT.Host, o.MQTT.Port)
		testMQTTCheckConnAck(b, brs, mqttConnAckRCConnectionAccepted, false)

		testMQTTSub(b, 1, cs, brs, []*mqttFilter{{filter: subject, qos: 0}}, []byte{0})
		testMQTTFlush(b, cs, nil, brs)

		w := newMQTTWriter(0)
		varHeaderAndPayload := 2 + len(subject) + len(payload)
		w.WriteVarInt(varHeaderAndPayload)
		size := 1 + w.Len() + varHeaderAndPayload
		totalSize += int64(size)

		go func() {
			mqttBenchConsumeMsgQoS0(cs, int64(b.N)*int64(size), dch)
			cs.Close()
		}()
	}
	for i := 0; i < numSubs; i++ {
		createSub(i + 1)
		cdch++
	}

	bw := bufio.NewWriterSize(c, mqttBenchBufLen)
	b.SetBytes(totalSize)
	b.StartTimer()
	for i := 0; i < b.N; i++ {
		bw.Write(sendOp)
	}
	testMQTTFlush(b, c, bw, br)
	for i := 0; i < cdch; i++ {
		if e := <-dch; e != nil {
			b.Fatal(e.Error())
		}
	}
	b.StopTimer()
	c.Close()
	s.Shutdown()
}

func mqttBenchConsumeMsgQoS0(c net.Conn, total int64, dch chan<- error) {
	var buf [mqttBenchBufLen]byte
	var err error
	var n int
	for size := int64(0); size < total; {
		n, err = c.Read(buf[:])
		if err != nil {
			break
		}
		size += int64(n)
	}
	dch <- err
}

func mqttBenchPubQoS1(b *testing.B, subject, payload string, numSubs int) {
	b.StopTimer()
	o := testMQTTDefaultOptions()
	o.MQTT.MaxAckPending = 0xFFFF
	s := RunServer(o)
	defer testMQTTShutdownServer(s)

	ci := &mqttConnInfo{cleanSess: true}
	c, br := testMQTTConnect(b, ci, o.MQTT.Host, o.MQTT.Port)
	testMQTTCheckConnAck(b, br, mqttConnAckRCConnectionAccepted, false)

	w := newMQTTWriter(0)
	testMQTTWritePublishPacket(b, w, 1, false, false, subject, 1, []byte(payload))
	// For reported bytes we will count the PUBLISH + PUBACK (4 bytes)
	totalSize := int64(w.Len() + 4)
	w.Reset()

	pi := uint16(1)
	maxpi := uint16(60000)
	ppich := make(chan error, 10)
	dch := make(chan error, 1+numSubs)
	cdch := 1
	// Start go routine to consume PUBACK for published QoS 1 messages.
	go mqttBenchConsumePubAck(c, b.N, dch, ppich)

	createSub := func(i int) {
		ci := &mqttConnInfo{clientID: fmt.Sprintf("sub%d", i), cleanSess: true}
		cs, brs := testMQTTConnect(b, ci, o.MQTT.Host, o.MQTT.Port)
		testMQTTCheckConnAck(b, brs, mqttConnAckRCConnectionAccepted, false)

		testMQTTSub(b, 1, cs, brs, []*mqttFilter{{filter: subject, qos: 1}}, []byte{1})
		testMQTTFlush(b, cs, nil, brs)

		w := newMQTTWriter(0)
		varHeaderAndPayload := 2 + len(subject) + 2 + len(payload)
		w.WriteVarInt(varHeaderAndPayload)
		size := 1 + w.Len() + varHeaderAndPayload
		// Add to the bytes reported the size of message sent to subscriber + PUBACK (4 bytes)
		totalSize += int64(size + 4)

		go func() {
			mqttBenchConsumeMsgQos1(cs, b.N, size, dch)
			cs.Close()
		}()
	}
	for i := 0; i < numSubs; i++ {
		createSub(i + 1)
		cdch++
	}

	flush := func() {
		b.Helper()
		if _, err := c.Write(w.Bytes()); err != nil {
			b.Fatalf("Error on write: %v", err)
		}
		w.Reset()
	}

	b.SetBytes(totalSize)
	b.StartTimer()
	for i := 0; i < b.N; i++ {
		if pi <= maxpi {
			testMQTTWritePublishPacket(b, w, 1, false, false, subject, pi, []byte(payload))
			pi++
			if w.Len() >= mqttBenchBufLen {
				flush()
			}
		} else {
			if w.Len() > 0 {
				flush()
			}
			if pi > 60000 {
				pi = 1
				maxpi = 0
			}
			if e := <-ppich; e != nil {
				b.Fatal(e.Error())
			}
			maxpi += 10000
			i--
		}
	}
	if w.Len() > 0 {
		flush()
	}
	for i := 0; i < cdch; i++ {
		if e := <-dch; e != nil {
			b.Fatal(e.Error())
		}
	}
	b.StopTimer()
	c.Close()
	s.Shutdown()
}

func mqttBenchConsumeMsgQos1(c net.Conn, total, size int, dch chan<- error) {
	var buf [mqttBenchBufLen]byte
	pubAck := [4]byte{mqttPacketPubAck, 0x2, 0, 0}
	var err error
	var n int
	var pi uint16
	var prev int
	for i := 0; i < total; {
		n, err = c.Read(buf[:])
		if err != nil {
			break
		}
		n += prev
		for ; n >= size; n -= size {
			i++
			pi++
			pubAck[2] = byte(pi >> 8)
			pubAck[3] = byte(pi)
			if _, err = c.Write(pubAck[:4]); err != nil {
				dch <- err
				return
			}
			if pi == 60000 {
				pi = 0
			}
		}
		prev = n
	}
	dch <- err
}

func mqttBenchConsumePubAck(c net.Conn, total int, dch, ppich chan<- error) {
	var buf [mqttBenchBufLen]byte
	var err error
	var n int
	var pi uint16
	var prev int
	for i := 0; i < total; {
		n, err = c.Read(buf[:])
		if err != nil {
			break
		}
		n += prev
		for ; n >= 4; n -= 4 {
			i++
			pi++
			if pi%10000 == 0 {
				ppich <- nil
			}
			if pi == 60001 {
				pi = 0
			}
		}
		prev = n
	}
	ppich <- err
	dch <- err
}

func BenchmarkMQTT_QoS0_Pub_______0b_Payload(b *testing.B) {
	mqttBenchPubQoS0(b, mqttPubSubj, "", 0)
}

func BenchmarkMQTT_QoS0_Pub_______8b_Payload(b *testing.B) {
	mqttBenchPubQoS0(b, mqttPubSubj, sizedString(8), 0)
}

func BenchmarkMQTT_QoS0_Pub______32b_Payload(b *testing.B) {
	mqttBenchPubQoS0(b, mqttPubSubj, sizedString(32), 0)
}

func BenchmarkMQTT_QoS0_Pub_____128b_Payload(b *testing.B) {
	mqttBenchPubQoS0(b, mqttPubSubj, sizedString(128), 0)
}

func BenchmarkMQTT_QoS0_Pub_____256b_Payload(b *testing.B) {
	mqttBenchPubQoS0(b, mqttPubSubj, sizedString(256), 0)
}

func BenchmarkMQTT_QoS0_Pub_______1K_Payload(b *testing.B) {
	mqttBenchPubQoS0(b, mqttPubSubj, sizedString(1024), 0)
}

func BenchmarkMQTT_QoS0_PubSub1___0b_Payload(b *testing.B) {
	mqttBenchPubQoS0(b, mqttPubSubj, "", 1)
}

func BenchmarkMQTT_QoS0_PubSub1___8b_Payload(b *testing.B) {
	mqttBenchPubQoS0(b, mqttPubSubj, sizedString(8), 1)
}

func BenchmarkMQTT_QoS0_PubSub1__32b_Payload(b *testing.B) {
	mqttBenchPubQoS0(b, mqttPubSubj, sizedString(32), 1)
}

func BenchmarkMQTT_QoS0_PubSub1_128b_Payload(b *testing.B) {
	mqttBenchPubQoS0(b, mqttPubSubj, sizedString(128), 1)
}

func BenchmarkMQTT_QoS0_PubSub1_256b_Payload(b *testing.B) {
	mqttBenchPubQoS0(b, mqttPubSubj, sizedString(256), 1)
}

func BenchmarkMQTT_QoS0_PubSub1___1K_Payload(b *testing.B) {
	mqttBenchPubQoS0(b, mqttPubSubj, sizedString(1024), 1)
}

func BenchmarkMQTT_QoS0_PubSub2___0b_Payload(b *testing.B) {
	mqttBenchPubQoS0(b, mqttPubSubj, "", 2)
}

func BenchmarkMQTT_QoS0_PubSub2___8b_Payload(b *testing.B) {
	mqttBenchPubQoS0(b, mqttPubSubj, sizedString(8), 2)
}

func BenchmarkMQTT_QoS0_PubSub2__32b_Payload(b *testing.B) {
	mqttBenchPubQoS0(b, mqttPubSubj, sizedString(32), 2)
}

func BenchmarkMQTT_QoS0_PubSub2_128b_Payload(b *testing.B) {
	mqttBenchPubQoS0(b, mqttPubSubj, sizedString(128), 2)
}

func BenchmarkMQTT_QoS0_PubSub2_256b_Payload(b *testing.B) {
	mqttBenchPubQoS0(b, mqttPubSubj, sizedString(256), 2)
}

func BenchmarkMQTT_QoS0_PubSub2___1K_Payload(b *testing.B) {
	mqttBenchPubQoS0(b, mqttPubSubj, sizedString(1024), 2)
}

func BenchmarkMQTT_QoS1_Pub_______0b_Payload(b *testing.B) {
	mqttBenchPubQoS1(b, mqttPubSubj, "", 0)
}

func BenchmarkMQTT_QoS1_Pub_______8b_Payload(b *testing.B) {
	mqttBenchPubQoS1(b, mqttPubSubj, sizedString(8), 0)
}

func BenchmarkMQTT_QoS1_Pub______32b_Payload(b *testing.B) {
	mqttBenchPubQoS1(b, mqttPubSubj, sizedString(32), 0)
}

func BenchmarkMQTT_QoS1_Pub_____128b_Payload(b *testing.B) {
	mqttBenchPubQoS1(b, mqttPubSubj, sizedString(128), 0)
}

func BenchmarkMQTT_QoS1_Pub_____256b_Payload(b *testing.B) {
	mqttBenchPubQoS1(b, mqttPubSubj, sizedString(256), 0)
}

func BenchmarkMQTT_QoS1_Pub_______1K_Payload(b *testing.B) {
	mqttBenchPubQoS1(b, mqttPubSubj, sizedString(1024), 0)
}

func BenchmarkMQTT_QoS1_PubSub1___0b_Payload(b *testing.B) {
	mqttBenchPubQoS1(b, mqttPubSubj, "", 1)
}

func BenchmarkMQTT_QoS1_PubSub1___8b_Payload(b *testing.B) {
	mqttBenchPubQoS1(b, mqttPubSubj, sizedString(8), 1)
}

func BenchmarkMQTT_QoS1_PubSub1__32b_Payload(b *testing.B) {
	mqttBenchPubQoS1(b, mqttPubSubj, sizedString(32), 1)
}

func BenchmarkMQTT_QoS1_PubSub1_128b_Payload(b *testing.B) {
	mqttBenchPubQoS1(b, mqttPubSubj, sizedString(128), 1)
}

func BenchmarkMQTT_QoS1_PubSub1_256b_Payload(b *testing.B) {
	mqttBenchPubQoS1(b, mqttPubSubj, sizedString(256), 1)
}

func BenchmarkMQTT_QoS1_PubSub1___1K_Payload(b *testing.B) {
	mqttBenchPubQoS1(b, mqttPubSubj, sizedString(1024), 1)
}

func BenchmarkMQTT_QoS1_PubSub2___0b_Payload(b *testing.B) {
	mqttBenchPubQoS1(b, mqttPubSubj, "", 2)
}

func BenchmarkMQTT_QoS1_PubSub2___8b_Payload(b *testing.B) {
	mqttBenchPubQoS1(b, mqttPubSubj, sizedString(8), 2)
}

func BenchmarkMQTT_QoS1_PubSub2__32b_Payload(b *testing.B) {
	mqttBenchPubQoS1(b, mqttPubSubj, sizedString(32), 2)
}

func BenchmarkMQTT_QoS1_PubSub2_128b_Payload(b *testing.B) {
	mqttBenchPubQoS1(b, mqttPubSubj, sizedString(128), 2)
}

func BenchmarkMQTT_QoS1_PubSub2_256b_Payload(b *testing.B) {
	mqttBenchPubQoS1(b, mqttPubSubj, sizedString(256), 2)
}

func BenchmarkMQTT_QoS1_PubSub2___1K_Payload(b *testing.B) {
	mqttBenchPubQoS1(b, mqttPubSubj, sizedString(1024), 2)
}

// A PUBREL arriving on a different connection than its PUBLISH (session
// resumed after a reconnect) has no staged in-memory copy and must fall
// back to loading the message from JetStream, still delivering it exactly
// once and completing the exchange.
func TestMQTTQoS2PubRelResumedSessionDelivery(t *testing.T) {
	o := testMQTTDefaultOptions()
	s := testMQTTRunServer(t, o)
	defer testMQTTShutdownServer(s)

	mcs, msr := testMQTTConnect(t, &mqttConnInfo{clientID: "sub", cleanSess: true}, o.MQTT.Host, o.MQTT.Port)
	defer mcs.Close()
	testMQTTCheckConnAck(t, msr, mqttConnAckRCConnectionAccepted, false)
	testMQTTSub(t, 1, mcs, msr, []*mqttFilter{{filter: "foo", qos: 1}}, []byte{1})
	testMQTTFlush(t, mcs, nil, msr)

	// Publish QoS2 retained and PUBREC it, then drop before the PUBREL.
	const pubPI = 7
	ci := &mqttConnInfo{clientID: "pub", cleanSess: false}
	mcp, mpr := testMQTTConnect(t, ci, o.MQTT.Host, o.MQTT.Port)
	testMQTTCheckConnAck(t, mpr, mqttConnAckRCConnectionAccepted, false)
	testMQTTSendPublishPacket(t, mcp, 2, false, true, "foo", pubPI, []byte("m"))
	testMQTTReadPIPacket(mqttPacketPubRec, t, mpr, pubPI)
	testMQTTDisconnect(t, mcp, nil)
	mcp.Close()
	testMQTTExpectNothing(t, msr)

	// Resume the session; the PUBREL now delivers the message, at the sub's QoS.
	mcp2, mpr2 := testMQTTConnect(t, ci, o.MQTT.Host, o.MQTT.Port)
	defer mcp2.Close()
	testMQTTCheckConnAck(t, mpr2, mqttConnAckRCConnectionAccepted, true)
	testMQTTSendPIPacket(mqttPacketPubRel|0x2, t, mcp2, pubPI)
	testMQTTReadPIPacket(mqttPacketPubComp, t, mpr2, pubPI)
	testMQTTCheckPubMsgNoAck(t, mcs, msr, "foo", mqttPubQos1, []byte("m"))
	testMQTTExpectNothing(t, msr)

	// A repeated PUBREL is acked without redelivering.
	testMQTTSendPIPacket(mqttPacketPubRel|0x2, t, mcp2, pubPI)
	testMQTTReadPIPacket(mqttPacketPubComp, t, mpr2, pubPI)
	testMQTTExpectNothing(t, msr)

	// The retain flag must survive the staging round-trip: a new
	// subscriber gets the message as retained.
	checkFor(t, time.Second, 10*time.Millisecond, func() error {
		acc, err := s.lookupAccount(globalAccountName)
		if err != nil {
			return err
		}
		mset, err := acc.lookupStream(mqttRetainedMsgsStreamName)
		if err != nil {
			return err
		}
		if mset.state().Msgs != 1 {
			return errors.New("retained message not stored")
		}
		return nil
	})
	mcs2, msr2 := testMQTTConnect(t, &mqttConnInfo{clientID: "sub2", cleanSess: true}, o.MQTT.Host, o.MQTT.Port)
	defer mcs2.Close()
	testMQTTCheckConnAck(t, msr2, mqttConnAckRCConnectionAccepted, false)
	testMQTTSub(t, 1, mcs2, msr2, []*mqttFilter{{filter: "foo", qos: 1}}, []byte{1})
	testMQTTCheckPubMsgNoAck(t, mcs2, msr2, "foo", mqttPubQos1|mqttPubFlagRetain, []byte("m"))
}

func TestMQTTCertMappedUserNotPasswordAuthenticatable(t *testing.T) {
	const certUser = "CN=example.com,OU=NATS.io"

	// The mqtt listener does not map, so a certificate identity must not be
	// authenticatable by username there. The leafnode and websocket listeners
	// are covered by TestAuthCertMappedUserNotPasswordAuthenticatable.
	conf := createConfFile(t, []byte(fmt.Sprintf(`
		listen: "127.0.0.1:-1"
		server_name: "S1"
		jetstream { store_dir: %q }
		tls {
			cert_file: "../test/configs/certs/tlsauth/server.pem"
			key_file:  "../test/configs/certs/tlsauth/server-key.pem"
			ca_file:   "../test/configs/certs/tlsauth/ca.pem"
			verify_and_map: true
		}
		mqtt { listen: "127.0.0.1:-1" }
		authorization {
			users [
				{ user = "CN=other.example.com,OU=NATS.io" }
				{ user = "%s" }
			]
		}
	`, t.TempDir(), certUser)))
	s, o := RunServerWithConfig(conf)
	defer testMQTTShutdownServer(s)

	for _, test := range []struct {
		name string
		user string
		pass string
	}{
		{"empty password", certUser, _EMPTY_},
		{"wrong password", certUser, "wrongpassword"},
		{"unknown user", "CN=attacker.com,OU=NATS.io", _EMPTY_},
		{"anonymous", _EMPTY_, _EMPTY_},
	} {
		t.Run(test.name, func(t *testing.T) {
			ci := &mqttConnInfo{clientID: "mqtt", cleanSess: true, user: test.user, pass: test.pass}
			mc, r := testMQTTConnect(t, ci, o.MQTT.Host, o.MQTT.Port)
			defer mc.Close()
			testMQTTCheckConnAck(t, r, mqttConnAckRCNotAuthorized, false)
		})
	}
}

// A burst of QoS1 PUBLISH packets must produce one PUBACK each, in publish
// order.
func TestMQTTQoS1PubAckPipelineOrder(t *testing.T) {
	o := testMQTTDefaultOptions()
	s := testMQTTRunServer(t, o)
	defer testMQTTShutdownServer(s)

	mcp, mpr := testMQTTConnect(t, &mqttConnInfo{cleanSess: true}, o.MQTT.Host, o.MQTT.Port)
	defer mcp.Close()
	testMQTTCheckConnAck(t, mpr, mqttConnAckRCConnectionAccepted, false)

	// QoS1 interest, so the publishes flow through the JS store.
	mcs, msr := testMQTTConnect(t, &mqttConnInfo{clientID: "pipesub", cleanSess: true}, o.MQTT.Host, o.MQTT.Port)
	defer mcs.Close()
	testMQTTCheckConnAck(t, msr, mqttConnAckRCConnectionAccepted, false)
	testMQTTSub(t, 1, mcs, msr, []*mqttFilter{{filter: "foo", qos: 1}}, []byte{1})
	testMQTTFlush(t, mcs, nil, msr)

	const numMsgs = 100
	for pi := uint16(1); pi <= numMsgs; pi++ {
		testMQTTSendPublishPacket(t, mcp, 1, false, false, "foo", pi, []byte("msg"))
	}

	for pi := uint16(1); pi <= numMsgs; pi++ {
		b, _ := testMQTTReadPacket(t, mpr)
		if pt := b & mqttPacketMask; pt != mqttPacketPubAck {
			t.Fatalf("Expected PUBACK packet %x, got %x", mqttPacketPubAck, pt)
		}
		rpi, err := mpr.readUint16("packet identifier")
		if err != nil {
			t.Fatalf("Error reading packet identifier: %v", err)
		}
		if rpi != pi {
			t.Fatalf("Expected PUBACK for pi=%v, got pi=%v (out of order)", pi, rpi)
		}
	}

	for i := 0; i < numMsgs; i++ {
		testMQTTCheckPubMsgNoAck(t, mcs, msr, "foo", mqttPubQos1, []byte("msg"))
	}
}

// Closing a connection with pipelined stores in flight must not lose the
// submitted messages, leak JSA reply registrations, or surface a stray
// PUBACK on a successor connection.
func TestMQTTQoS1PubAckPipelineConnClose(t *testing.T) {
	o := testMQTTDefaultOptions()
	s := testMQTTRunServer(t, o)
	defer testMQTTShutdownServer(s)

	// QoS1 interest, so the publishes are stored.
	mcs, msr := testMQTTConnect(t, &mqttConnInfo{clientID: "sub", cleanSess: true}, o.MQTT.Host, o.MQTT.Port)
	defer mcs.Close()
	testMQTTCheckConnAck(t, msr, mqttConnAckRCConnectionAccepted, false)
	testMQTTSub(t, 1, mcs, msr, []*mqttFilter{{filter: "foo", qos: 1}}, []byte{1})
	testMQTTFlush(t, mcs, nil, msr)

	// Wait for the server to process every PUBLISH before closing: the
	// abrupt close (unread PUBACKs cause a TCP RST) may discard the
	// server's unprocessed input.
	const numMsgs = 50
	mcp, mpr := testMQTTConnect(t, &mqttConnInfo{clientID: "pub", cleanSess: true}, o.MQTT.Host, o.MQTT.Port)
	testMQTTCheckConnAck(t, mpr, mqttConnAckRCConnectionAccepted, false)
	for pi := uint16(1); pi <= numMsgs; pi++ {
		testMQTTSendPublishPacket(t, mcp, 1, false, false, "foo", pi, []byte(fmt.Sprintf("msg-%d", pi)))
	}
	pc := testMQTTGetClient(t, s, "pub")
	checkFor(t, 2*time.Second, 10*time.Millisecond, func() error {
		if n := atomic.LoadInt64(&pc.inMsgs); n < numMsgs {
			return fmt.Errorf("server processed %v of %v publishes", n, numMsgs)
		}
		return nil
	})
	mcp.Close()

	for pi := uint16(1); pi <= numMsgs; pi++ {
		testMQTTCheckPubMsgNoAck(t, mcs, msr, "foo", mqttPubQos1, []byte(fmt.Sprintf("msg-%d", pi)))
	}

	// No dangling JSA reply registrations from the abandoned pipeline.
	jsa := s.mqttGetJSAForAccount(globalAccountName)
	if jsa == nil {
		t.Fatal("no JSA for the global account")
	}
	checkFor(t, 2*time.Second, 50*time.Millisecond, func() error {
		n := 0
		jsa.replies.Range(func(_, _ any) bool { n++; return true })
		if n > 0 {
			return fmt.Errorf("%v dangling JSA reply registration(s)", n)
		}
		return nil
	})

	// Session takeover: a PUBACK leaked from the abandoned pipeline would
	// show up as an extra packet here.
	mcp2, mpr2 := testMQTTConnect(t, &mqttConnInfo{clientID: "pub", cleanSess: true}, o.MQTT.Host, o.MQTT.Port)
	defer mcp2.Close()
	testMQTTCheckConnAck(t, mpr2, mqttConnAckRCConnectionAccepted, false)
	testMQTTPublish(t, mcp2, mpr2, 1, false, false, "foo", 77, []byte("msg2"))
	testMQTTExpectNothing(t, mpr2)

	testMQTTCheckPubMsgNoAck(t, mcs, msr, "foo", mqttPubQos1, []byte("msg2"))
}

// A pipeline shut down while the readLoop is still admitting entries must
// not leak JSA reply registrations, even when the close-time drain runs
// concurrently with (or before) a racing push.
func TestMQTTQoS1PubAckPipelineShutdownRace(t *testing.T) {
	for i := 0; i < 100; i++ {
		jsa := &mqttJSA{timeout: time.Second}
		pipe := &mqttAckPipeline{
			jsa:    jsa,
			q:      make(chan *mqttPipelined, 4),
			quitCh: make(chan struct{}),
		}

		done := make(chan struct{})
		go func() {
			// The readLoop side: register, admit until rejected.
			defer close(done)
			for n := 0; ; n++ {
				ack := newMQTTPipelined(mqttPacketPubAck, uint16(n%0xFFFF+1))
				ack.storeReply = fmt.Sprintf("reply.%d", n)
				jsa.replies.Store(ack.storeReply, func(any) {})
				if err := pipe.push(ack); err != nil {
					return
				}
			}
		}()

		// The consumer side: take a few entries, then stop and drain
		// concurrently with the pushes, as the connection-close handler
		// does.
		for j := 0; j < i%4; j++ {
			ack := <-pipe.q
			jsa.replies.Delete(ack.storeReply)
		}
		pipe.shutdown()
		<-done

		leaked := 0
		jsa.replies.Range(func(_, _ any) bool { leaked++; return true })
		if leaked > 0 {
			t.Fatalf("iteration %d: %v reply registration(s) leaked after shutdown", i, leaked)
		}
	}

	// A push after the pipeline has stopped must be rejected outright,
	// with its registration cleaned up and nothing left in the queue.
	jsa := &mqttJSA{timeout: time.Second}
	pipe := &mqttAckPipeline{
		jsa:    jsa,
		q:      make(chan *mqttPipelined, 4),
		quitCh: make(chan struct{}),
	}
	pipe.shutdown()
	ack := newMQTTPipelined(mqttPacketPubAck, 1)
	ack.storeReply = "reply.stopped"
	jsa.replies.Store(ack.storeReply, func(any) {})
	if err := pipe.push(ack); err != errMQTTAckPipelineStopped {
		t.Fatalf("Expected errMQTTAckPipelineStopped, got %v", err)
	}
	if _, ok := jsa.replies.Load(ack.storeReply); ok {
		t.Fatal("reply registration not cleaned up on rejected push")
	}
	if n := len(pipe.q); n != 0 {
		t.Fatalf("%v entry(ies) admitted into a stopped pipeline", n)
	}
}

// A burst of PUBLISHes gets its PUBRECs in order, a burst of PUBRELs its
// PUBCOMPs in order, and every message is delivered exactly once.
func TestMQTTQoS2AckPipelineOrder(t *testing.T) {
	o := testMQTTDefaultOptions()
	s := testMQTTRunServer(t, o)
	defer testMQTTShutdownServer(s)

	mcp, mpr := testMQTTConnect(t, &mqttConnInfo{cleanSess: true}, o.MQTT.Host, o.MQTT.Port)
	defer mcp.Close()
	testMQTTCheckConnAck(t, mpr, mqttConnAckRCConnectionAccepted, false)

	// Subscribe with a second connection so the messages have interest and
	// flow through the JS store + delivery path.
	mcs, msr := testMQTTConnect(t, &mqttConnInfo{clientID: "pipesub", cleanSess: true}, o.MQTT.Host, o.MQTT.Port)
	defer mcs.Close()
	testMQTTCheckConnAck(t, msr, mqttConnAckRCConnectionAccepted, false)
	testMQTTSub(t, 1, mcs, msr, []*mqttFilter{{filter: "foo", qos: 1}}, []byte{1})
	testMQTTFlush(t, mcs, nil, msr)

	// Send a burst of QoS2 PUBLISH packets without reading any PUBRECs.
	const numMsgs = 100
	for pi := uint16(1); pi <= numMsgs; pi++ {
		testMQTTSendPublishPacket(t, mcp, 2, false, false, "foo", pi, []byte(fmt.Sprintf("msg-%d", pi)))
	}
	for pi := uint16(1); pi <= numMsgs; pi++ {
		testMQTTReadPIPacket(mqttPacketPubRec, t, mpr, pi)
	}

	// Release them all, again without reading any PUBCOMPs.
	for pi := uint16(1); pi <= numMsgs; pi++ {
		testMQTTSendPIPacket(mqttPacketPubRel|0x2, t, mcp, pi)
	}
	for pi := uint16(1); pi <= numMsgs; pi++ {
		testMQTTReadPIPacket(mqttPacketPubComp, t, mpr, pi)
	}

	// The subscriber must receive every message exactly once, in order.
	for pi := uint16(1); pi <= numMsgs; pi++ {
		testMQTTCheckPubMsgNoAck(t, mcs, msr, "foo", mqttPubQos1, []byte(fmt.Sprintf("msg-%d", pi)))
	}
	testMQTTExpectNothing(t, msr)

	// Only QoS2 deliveries carry a message id, derived from the held copy's
	// sequence.
	nc, js := jsClientConnect(t, s)
	defer nc.Close()
	storeSubject := mqttStreamSubjectPrefix + "foo"
	sm, err := js.GetLastMsg(mqttStreamName, storeSubject)
	require_NoError(t, err)
	if id := sm.Header.Get(JSMsgId); !strings.HasPrefix(id, mqttQoS2DeliveryMsgIdPrefix) {
		t.Fatalf("QoS2 delivery message id %q is not sequence-derived", id)
	}
	testMQTTSendPublishPacket(t, mcp, 1, false, false, "foo", numMsgs+1, []byte("q1"))
	testMQTTReadPIPacket(mqttPacketPubAck, t, mpr, numMsgs+1)
	testMQTTCheckPubMsgNoAck(t, mcs, msr, "foo", mqttPubQos1, []byte("q1"))
	sm, err = js.GetLastMsg(mqttStreamName, storeSubject)
	require_NoError(t, err)
	if id := sm.Header.Get(JSMsgId); id != _EMPTY_ {
		t.Fatalf("QoS1 publish carried a message id %q", id)
	}
}

// An abrupt close with QoS2 exchanges in flight must not panic, hang, or
// leak JSA reply registrations, and a successor connection must work.
func TestMQTTQoS2AckPipelineConnClose(t *testing.T) {
	o := testMQTTDefaultOptions()
	s := testMQTTRunServer(t, o)
	defer testMQTTShutdownServer(s)

	mcs, msr := testMQTTConnect(t, &mqttConnInfo{clientID: "sub", cleanSess: true}, o.MQTT.Host, o.MQTT.Port)
	defer mcs.Close()
	testMQTTCheckConnAck(t, msr, mqttConnAckRCConnectionAccepted, false)
	testMQTTSub(t, 1, mcs, msr, []*mqttFilter{{filter: "foo", qos: 1}}, []byte{1})
	testMQTTFlush(t, mcs, nil, msr)

	// A burst of QoS2 PUBLISHes, some released, then an abrupt close with
	// everything unread.
	const numMsgs = 50
	mcp, mpr := testMQTTConnect(t, &mqttConnInfo{clientID: "pub", cleanSess: true}, o.MQTT.Host, o.MQTT.Port)
	testMQTTCheckConnAck(t, mpr, mqttConnAckRCConnectionAccepted, false)
	for pi := uint16(1); pi <= numMsgs; pi++ {
		testMQTTSendPublishPacket(t, mcp, 2, false, false, "foo", pi, []byte(fmt.Sprintf("msg-%d", pi)))
	}
	// Read the PUBRECs before releasing [MQTT-4.3.3-1]; everything after is
	// left unread for the abrupt close to strand.
	for pi := uint16(1); pi <= numMsgs/2; pi++ {
		testMQTTReadPIPacket(mqttPacketPubRec, t, mpr, pi)
	}
	for pi := uint16(1); pi <= numMsgs/2; pi++ {
		testMQTTSendPIPacket(mqttPacketPubRel|0x2, t, mcp, pi)
	}
	// inMsgs counts broadcasts, which only the PUBRELs cause; all of them
	// delivered implies every packet above was processed.
	pc := testMQTTGetClient(t, s, "pub")
	checkFor(t, 2*time.Second, 10*time.Millisecond, func() error {
		if n := atomic.LoadInt64(&pc.inMsgs); n < numMsgs/2 {
			return fmt.Errorf("server delivered %v of %v releases", n, numMsgs/2)
		}
		return nil
	})
	mcp.Close()

	// All JSA reply registrations from the abandoned pipeline must be
	// cleaned up, by the reply processing or by the pipeline's shutdown.
	jsa := s.mqttGetJSAForAccount(globalAccountName)
	if jsa == nil {
		t.Fatal("no JSA for the global account")
	}
	checkFor(t, 2*time.Second, 50*time.Millisecond, func() error {
		n := 0
		jsa.replies.Range(func(_, _ any) bool { n++; return true })
		if n > 0 {
			return fmt.Errorf("%v dangling JSA reply registration(s)", n)
		}
		return nil
	})

	// The released messages were delivered (the releases preceded the
	// close); the unreleased ones must not be.
	for pi := uint16(1); pi <= numMsgs/2; pi++ {
		testMQTTCheckPubMsgNoAck(t, mcs, msr, "foo", mqttPubQos1, []byte(fmt.Sprintf("msg-%d", pi)))
	}
	testMQTTExpectNothing(t, msr)

	// A successor connection completes a full QoS2 exchange cleanly.
	mcp2, mpr2 := testMQTTConnect(t, &mqttConnInfo{clientID: "pub", cleanSess: true}, o.MQTT.Host, o.MQTT.Port)
	defer mcp2.Close()
	testMQTTCheckConnAck(t, mpr2, mqttConnAckRCConnectionAccepted, false)
	testMQTTPublish(t, mcp2, mpr2, 2, false, false, "foo", 99, []byte("fresh"))
	testMQTTCheckPubMsgNoAck(t, mcs, msr, "foo", mqttPubQos1, []byte("fresh"))
}

// Reusing a PI after its exchange was released delivers every message
// exactly once: the old released mark must not short-circuit the new
// exchange, nor the new message dedupe against the old copy.
func TestMQTTQoS2PIReuseAfterRelease(t *testing.T) {
	o := testMQTTDefaultOptions()
	s := testMQTTRunServer(t, o)
	defer testMQTTShutdownServer(s)

	mcs, msr := testMQTTConnect(t, &mqttConnInfo{clientID: "sub", cleanSess: true}, o.MQTT.Host, o.MQTT.Port)
	defer mcs.Close()
	testMQTTCheckConnAck(t, msr, mqttConnAckRCConnectionAccepted, false)
	testMQTTSub(t, 1, mcs, msr, []*mqttFilter{{filter: "foo", qos: 1}}, []byte{1})
	testMQTTFlush(t, mcs, nil, msr)

	mcp, mpr := testMQTTConnect(t, &mqttConnInfo{clientID: "pub", cleanSess: true}, o.MQTT.Host, o.MQTT.Port)
	defer mcp.Close()
	testMQTTCheckConnAck(t, mpr, mqttConnAckRCConnectionAccepted, false)

	// Back-to-back exchanges on one PI; each PUBCOMP frees it for the next.
	const pi = uint16(1)
	const cycles = 10
	for i := 1; i <= cycles; i++ {
		testMQTTPublish(t, mcp, mpr, 2, false, false, "foo", pi, []byte(fmt.Sprintf("cycle-%d", i)))
	}

	// Every cycle's message, exactly once, in order.
	for i := 1; i <= cycles; i++ {
		testMQTTCheckPubMsgNoAck(t, mcs, msr, "foo", mqttPubQos1, []byte(fmt.Sprintf("cycle-%d", i)))
	}
	testMQTTExpectNothing(t, msr)
}

// A PUBLISH retransmitted without DUP, whose copy is already in the stream:
// the new exchange's store is deduped and yields no sequence, so the PUBREL
// resolves from the stream instead of being rejected. Delivered once.
func TestMQTTQoS2RetransmitWithoutDupResolvesFromStream(t *testing.T) {
	o := testMQTTDefaultOptions()
	s := testMQTTRunServer(t, o)
	defer testMQTTShutdownServer(s)

	mcs, msr := testMQTTConnect(t, &mqttConnInfo{clientID: "sub", cleanSess: true}, o.MQTT.Host, o.MQTT.Port)
	defer mcs.Close()
	testMQTTCheckConnAck(t, msr, mqttConnAckRCConnectionAccepted, false)
	testMQTTSub(t, 1, mcs, msr, []*mqttFilter{{filter: "foo", qos: 1}}, []byte{1})
	testMQTTFlush(t, mcs, nil, msr)

	// Hold a copy, then lose the connection before the PUBREL.
	const pi = uint16(6)
	ci := &mqttConnInfo{clientID: "pub", cleanSess: false}
	mcp, mpr := testMQTTConnect(t, ci, o.MQTT.Host, o.MQTT.Port)
	testMQTTCheckConnAck(t, mpr, mqttConnAckRCConnectionAccepted, false)
	testMQTTSendPublishPacket(t, mcp, 2, false, false, "foo", pi, []byte("m"))
	testMQTTReadPIPacket(mqttPacketPubRec, t, mpr, pi)
	mcp.Close()

	// Retransmit it with DUP clear (dup=false), so the new connection keeps
	// a copy and records an exchange, whose hold store the stream dedups.
	mcp2, mpr2 := testMQTTConnect(t, ci, o.MQTT.Host, o.MQTT.Port)
	defer mcp2.Close()
	testMQTTCheckConnAck(t, mpr2, mqttConnAckRCConnectionAccepted, true)
	testMQTTPublish(t, mcp2, mpr2, 2, false, false, "foo", pi, []byte("m"))

	testMQTTCheckPubMsgNoAck(t, mcs, msr, "foo", mqttPubQos1, []byte("m"))
	testMQTTExpectNothing(t, msr)
	if n := testMQTTQoS2HeldCount(t, s); n != 0 {
		t.Fatalf("PUBCOMP received but %v held message(s) remain", n)
	}
}

func testMQTTQoS2HeldCount(t *testing.T, s *Server) uint64 {
	t.Helper()
	acc, err := s.lookupAccount(globalAccountName)
	if err != nil {
		t.Fatalf("Error looking up account: %v", err)
	}
	mset, err := acc.lookupStream(mqttQoS2IncomingMsgsStreamName)
	if err != nil {
		t.Fatalf("Error looking up stream: %v", err)
	}
	return mset.state().Msgs
}

// A received PUBCOMP proves the held copy is gone: the stream is checked
// right after each one, no polling, since PUBCOMP is gated on the delete.
func TestMQTTQoS2PubCompImpliesDelete(t *testing.T) {
	o := testMQTTDefaultOptions()
	s := testMQTTRunServer(t, o)
	defer testMQTTShutdownServer(s)

	mcs, msr := testMQTTConnect(t, &mqttConnInfo{clientID: "sub", cleanSess: true}, o.MQTT.Host, o.MQTT.Port)
	defer mcs.Close()
	testMQTTCheckConnAck(t, msr, mqttConnAckRCConnectionAccepted, false)
	testMQTTSub(t, 1, mcs, msr, []*mqttFilter{{filter: "foo", qos: 1}}, []byte{1})
	testMQTTFlush(t, mcs, nil, msr)

	ci := &mqttConnInfo{clientID: "pub", cleanSess: false}
	mcp, mpr := testMQTTConnect(t, ci, o.MQTT.Host, o.MQTT.Port)
	testMQTTCheckConnAck(t, mpr, mqttConnAckRCConnectionAccepted, false)

	const numMsgs = 10
	for pi := uint16(1); pi <= numMsgs; pi++ {
		testMQTTPublish(t, mcp, mpr, 2, false, false, "foo", pi, []byte(fmt.Sprintf("msg-%d", pi)))
		if n := testMQTTQoS2HeldCount(t, s); n != 0 {
			t.Fatalf("pi %v: PUBCOMP received but %v held message(s) remain", pi, n)
		}
	}

	// Simulate a PUBCOMP lost in flight: drop the connection without a
	// DISCONNECT and retransmit the last PUBREL on the resumed session.
	mcp.Close()
	mcp2, mpr2 := testMQTTConnect(t, ci, o.MQTT.Host, o.MQTT.Port)
	defer mcp2.Close()
	testMQTTCheckConnAck(t, mpr2, mqttConnAckRCConnectionAccepted, true)
	testMQTTSendPIPacket(mqttPacketPubRel|0x2, t, mcp2, numMsgs)
	testMQTTReadPIPacket(mqttPacketPubComp, t, mpr2, numMsgs)

	// Exactly one delivery per message, and no duplicate from the
	// retransmit: the fallback load found nothing.
	for pi := uint16(1); pi <= numMsgs; pi++ {
		testMQTTCheckPubMsgNoAck(t, mcs, msr, "foo", mqttPubQos1, []byte(fmt.Sprintf("msg-%d", pi)))
	}
	testMQTTExpectNothing(t, msr)
}

// A PUBLISH retransmitted while its publication is held on the connection
// gets its PUBREC without a second hold store, which could otherwise be
// applied after the PUBREL's delete and leave a stray copy under the PI.
func TestMQTTQoS2RetransmitWhileHeldStoresOnce(t *testing.T) {
	o := testMQTTDefaultOptions()
	s := testMQTTRunServer(t, o)
	defer testMQTTShutdownServer(s)

	mcs, msr := testMQTTConnect(t, &mqttConnInfo{clientID: "sub", cleanSess: true}, o.MQTT.Host, o.MQTT.Port)
	defer mcs.Close()
	testMQTTCheckConnAck(t, msr, mqttConnAckRCConnectionAccepted, false)
	testMQTTSub(t, 1, mcs, msr, []*mqttFilter{{filter: "foo", qos: 1}}, []byte{1})
	testMQTTFlush(t, mcs, nil, msr)

	// Every hold store is published to the stream's subject, so a core
	// NATS subscription sees each one.
	nc := natsConnect(t, s.ClientURL())
	defer nc.Close()
	stores := natsSubSync(t, nc, mqttQoS2IncomingMsgsStreamSubjectPrefix+">")
	natsFlush(t, nc)

	mcp, mpr := testMQTTConnect(t, &mqttConnInfo{clientID: "pub", cleanSess: true}, o.MQTT.Host, o.MQTT.Port)
	defer mcp.Close()
	testMQTTCheckConnAck(t, mpr, mqttConnAckRCConnectionAccepted, false)

	// The publication, then two retransmits before any PUBREC, with and
	// without DUP. Each is owed a PUBREC [MQTT-4.3.3-2].
	const pi = uint16(7)
	testMQTTSendPublishPacket(t, mcp, 2, false, false, "foo", pi, []byte("m"))
	testMQTTSendPublishPacket(t, mcp, 2, true, false, "foo", pi, []byte("m"))
	testMQTTSendPublishPacket(t, mcp, 2, false, false, "foo", pi, []byte("m"))
	for i := 0; i < 3; i++ {
		testMQTTReadPIPacket(mqttPacketPubRec, t, mpr, pi)
	}
	testMQTTSendPIPacket(mqttPacketPubRel|0x2, t, mcp, pi)
	testMQTTReadPIPacket(mqttPacketPubComp, t, mpr, pi)

	// The PUBCOMP followed the PUBREL's delete through the same send queue
	// as any store the retransmits could have submitted, so all of them
	// have been published by now; the flush gets them to our subscription.
	natsFlush(t, nc)
	if n, _, err := stores.Pending(); err != nil || n != 1 {
		t.Fatalf("Expected 1 hold store for 3 PUBLISH packets, got %v (err=%v)", n, err)
	}
	testMQTTCheckPubMsgNoAck(t, mcs, msr, "foo", mqttPubQos1, []byte("m"))
	testMQTTExpectNothing(t, msr)
}

// The hand-built delete request must stay what the encoder would produce.
func TestMQTTDeleteMsgRequestEncoding(t *testing.T) {
	for _, seq := range []uint64{0, 1, 42, 1 << 63, ^uint64(0)} {
		expected, err := json.Marshal(JSApiMsgDeleteRequest{Seq: seq, NoErase: true})
		require_NoError(t, err)
		if got := mqttDeleteMsgRequest(seq); !bytes.Equal(got, expected) {
			t.Fatalf("seq %v: got %s, expected %s", seq, got, expected)
		}
	}
}

func TestMQTTQoS2UntrackedPubRecNotStored(t *testing.T) {
	o := testMQTTDefaultOptions()
	s := testMQTTRunServer(t, o)
	defer testMQTTShutdownServer(s)

	cp, rp := testMQTTConnect(t, &mqttConnInfo{clientID: "pub", cleanSess: true}, o.MQTT.Host, o.MQTT.Port)
	defer cp.Close()
	testMQTTCheckConnAck(t, rp, mqttConnAckRCConnectionAccepted, false)
	testMQTTPublish(t, cp, rp, 2, false, true, "baz", 1, []byte("retained"))

	c, r := testMQTTConnect(t, &mqttConnInfo{clientID: "sub", cleanSess: true}, o.MQTT.Host, o.MQTT.Port)
	defer c.Close()
	testMQTTCheckConnAck(t, r, mqttConnAckRCConnectionAccepted, false)
	testMQTTSub(t, 1, c, r, []*mqttFilter{{filter: "foo", qos: 2}, {filter: "bar", qos: 1}}, []byte{2, 1})
	testMQTTFlush(t, c, nil, r)

	// PUBRECs for a PI that was never delivered get a PUBREL, but must not store one.
	for range 100 {
		testMQTTSendPIPacket(mqttPacketPubRec, t, c, 100)
		testMQTTReadPIPacket(mqttPacketPubRel, t, r, 100)
	}

	// A PUBREC for a QoS1 delivery must not store a PUBREL, the PUBACK still works.
	testMQTTPublish(t, cp, rp, 1, false, false, "bar", 1, []byte("qos1"))
	pi := testMQTTCheckPubMsgNoAck(t, c, r, "bar", mqttPubQos1, []byte("qos1"))
	testMQTTSendPIPacket(mqttPacketPubRec, t, c, pi)
	testMQTTReadPIPacket(mqttPacketPubRel, t, r, pi)
	testMQTTSendPIPacket(mqttPacketPubAck, t, c, pi)
	testMQTTExpectNothing(t, r)

	// A genuine QoS2 delivery must still result in a PUBREL.
	testMQTTPublish(t, cp, rp, 2, false, false, "foo", 1, []byte("data"))
	pi = testMQTTCheckPubMsgNoAck(t, c, r, "foo", mqttPubQoS2, []byte("data"))
	testMQTTSendPIPacket(mqttPacketPubRec, t, c, pi)
	testMQTTReadPIPacket(mqttPacketPubRel, t, r, pi)

	// A retained QoS2 delivery must also result in a PUBREL.
	testMQTTSub(t, 2, c, r, []*mqttFilter{{filter: "baz", qos: 2}}, []byte{2})
	rpi := testMQTTCheckPubMsgNoAck(t, c, r, "baz", mqttPubQoS2|mqttPubFlagRetain, []byte("retained"))
	testMQTTSendPIPacket(mqttPacketPubRec, t, c, rpi)
	testMQTTReadPIPacket(mqttPacketPubRel, t, r, rpi)

	// A QoS2 delivery in flight during UNSUBSCRIBE must still complete [MQTT-3.10.4-3].
	testMQTTPublish(t, cp, rp, 2, false, false, "foo", 1, []byte("unsub"))
	upi := testMQTTCheckPubMsgNoAck(t, c, r, "foo", mqttPubQoS2, []byte("unsub"))
	testMQTTUnsub(t, 3, c, r, []*mqttFilter{{filter: "foo"}})
	testMQTTSendPIPacket(mqttPacketPubRec, t, c, upi)
	testMQTTReadPIPacket(mqttPacketPubRel, t, r, upi)
	testMQTTSendPIPacket(mqttPacketPubComp, t, c, upi)

	// Only the genuine PUBRELs were stored.
	mset, err := s.GlobalAccount().lookupStream(mqttOutStreamName)
	require_NoError(t, err)
	require_Equal(t, mset.state().Msgs, 2)

	testMQTTSendPIPacket(mqttPacketPubComp, t, c, pi)
	testMQTTSendPIPacket(mqttPacketPubComp, t, c, rpi)
	testMQTTExpectNothing(t, r)
}
