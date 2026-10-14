// Copyright 2019-2026 The NATS Authors
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

//go:build !skip_js_tests

package server

import (
	"bytes"
	"context"
	crand "crypto/rand"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"math/big"
	"math/rand/v2"
	"net"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"reflect"
	"runtime"
	"runtime/debug"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/klauspost/compress/s2"
	"github.com/nats-io/jwt/v2"
	"github.com/nats-io/nats-server/v2/server/archive"
	"github.com/nats-io/nats-server/v2/server/ats"
	"github.com/nats-io/nats-server/v2/server/sysmem"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/nats-io/nkeys"
	"github.com/nats-io/nuid"
)

func TestJetStreamBasicNilConfig(t *testing.T) {
	s := RunRandClientPortServer(t)
	defer s.Shutdown()

	if err := s.EnableJetStream(nil); err != nil {
		t.Fatalf("Expected no error, got %v", err)
	}
	if !s.JetStreamEnabled() {
		t.Fatalf("Expected JetStream to be enabled")
	}
	if s.SystemAccount() == nil {
		t.Fatalf("Expected system account to be created automatically")
	}
	// Grab our config since it was dynamically generated.
	config := s.JetStreamConfig()
	if config == nil {
		t.Fatalf("Expected non-nil config")
	}
	// Check dynamic max memory.
	hwMem := sysmem.Memory()
	if hwMem != 0 {
		// Check if memory being limited via GOMEMLIMIT if being set.
		if gml := debug.SetMemoryLimit(-1); gml != math.MaxInt64 {
			hwMem = gml
		}
		// Make sure its about 75%
		est := hwMem / 4 * 3
		if config.MaxMemory != est {
			t.Fatalf("Expected memory to be 80 percent of system memory, got %v vs %v", config.MaxMemory, est)
		}
	}
	// Make sure it was created.
	stat, err := os.Stat(config.StoreDir)
	if err != nil {
		t.Fatalf("Expected the store directory to be present, %v", err)
	}
	if stat == nil || !stat.IsDir() {
		t.Fatalf("Expected a directory")
	}
}

func RunBasicJetStreamServer(t testing.TB) *Server {
	opts := DefaultTestOptions
	opts.Port = -1
	opts.JetStream = true
	opts.StoreDir = t.TempDir()
	return RunServer(&opts)
}

func RunJetStreamServerOnPort(port int, sd string) *Server {
	opts := DefaultTestOptions
	opts.Port = port
	opts.JetStream = true
	opts.StoreDir = filepath.Dir(sd)
	return RunServer(&opts)
}

func TestJetStreamRemoteUsageUpdateLengthOverflow(t *testing.T) {
	s := RunBasicJetStreamServer(t)
	defer s.Shutdown()

	// Make sure the account has an active jsAccount.
	nc, js := jsClientConnect(t, s)
	defer nc.Close()
	_, err := js.AddStream(&nats.StreamConfig{Name: "T", Subjects: []string{"t"}})
	require_NoError(t, err)

	acc := s.globalAccount()
	jsa := s.getJetStream().lookupAccount(acc)
	require_NotNil(t, jsa)

	subject := fmt.Sprintf(jsaUpdatesPubT, acc.Name, "NODE1")
	// MaxUint64 in little-endian is simply eight 0xFF bytes.
	maxLen := []byte{0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff}

	// Case 1: overflow in the multi-tier guard. length lives at msg[minUsageUpdateLen+4:].
	multi := make([]byte, usageMultiTiersLen)
	copy(multi[minUsageUpdateLen+4:], maxLen) // length = MaxUint64
	jsa.remoteUpdateUsage(nil, nil, nil, subject, _EMPTY_, multi)

	// Case 2: overflow in the excess-record loop guard. A clean multi-tier header
	// (length 0) consumes exactly usageMultiTiersLen bytes, then one excess record
	// whose length field (at msg[16:]) is MaxUint64.
	excess := make([]byte, usageMultiTiersLen+usageRecordLen)
	excess[minUsageUpdateLen] = 1                // excessRecordCnt = 1 (uint32 LE)
	copy(excess[usageMultiTiersLen+16:], maxLen) // excess record length = MaxUint64
	jsa.remoteUpdateUsage(nil, nil, nil, subject, _EMPTY_, excess)

	// Reaching here without a panic means the bounds checks held. Confirm the
	// server is still responsive.
	_, err = js.AddStream(&nats.StreamConfig{Name: "T2", Subjects: []string{"t2"}})
	require_NoError(t, err)
}

func clientConnectToServer(t *testing.T, s *Server) *nats.Conn {
	t.Helper()
	nc, err := nats.Connect(s.ClientURL(),
		nats.Name("JS-TEST"),
		nats.ReconnectWait(5*time.Millisecond),
		nats.MaxReconnects(-1))
	if err != nil {
		t.Fatalf("Failed to create client: %v", err)
	}
	return nc
}

func clientConnectWithOldRequest(t *testing.T, s *Server) *nats.Conn {
	nc, err := nats.Connect(s.ClientURL(), nats.UseOldRequestStyle())
	if err != nil {
		t.Fatalf("Failed to create client: %v", err)
	}
	return nc
}

func TestJetStreamEnableAndDisableAccount(t *testing.T) {
	s := RunBasicJetStreamServer(t)
	defer s.Shutdown()

	// Global in simple setup should be enabled already.
	if !s.GlobalAccount().JetStreamEnabled() {
		t.Fatalf("Expected to have jetstream enabled on global account")
	}
	if na := s.JetStreamNumAccounts(); na != 1 {
		t.Fatalf("Expected 1 account, got %d", na)
	}

	if err := s.GlobalAccount().DisableJetStream(); err != nil {
		t.Fatalf("Did not expect error on disabling account: %v", err)
	}
	if na := s.JetStreamNumAccounts(); na != 0 {
		t.Fatalf("Expected no accounts, got %d", na)
	}
	// Make sure we unreserved resources.
	if rm, rd, err := s.JetStreamReservedResources(); err != nil {
		t.Fatalf("Unexpected error requesting jetstream reserved resources: %v", err)
	} else if rm != 0 || rd != 0 {
		t.Fatalf("Expected reserved memory and store to be 0, got %v and %v", friendlyBytes(rm), friendlyBytes(rd))
	}

	acc, _ := s.LookupOrRegisterAccount("$FOO")
	if err := acc.EnableJetStream(nil, nil); err != nil {
		t.Fatalf("Did not expect error on enabling account: %v", err)
	}
	if na := s.JetStreamNumAccounts(); na != 1 {
		t.Fatalf("Expected 1 account, got %d", na)
	}
	if err := acc.DisableJetStream(); err != nil {
		t.Fatalf("Did not expect error on disabling account: %v", err)
	}
	if na := s.JetStreamNumAccounts(); na != 0 {
		t.Fatalf("Expected no accounts, got %d", na)
	}
	// We should get error if disabling something not enabled.
	acc, _ = s.LookupOrRegisterAccount("$BAR")
	if err := acc.DisableJetStream(); err == nil {
		t.Fatalf("Expected error on disabling account that was not enabled")
	}
	// Should get an error for trying to enable a non-registered account.
	acc = NewAccount("$BAZ")
	if err := acc.EnableJetStream(nil, nil); err == nil {
		t.Fatalf("Expected error on enabling account that was not registered")
	}
}

func TestJetStreamAddStream(t *testing.T) {
	cases := []struct {
		name    string
		mconfig *StreamConfig
	}{
		{name: "MemoryStore",
			mconfig: &StreamConfig{
				Name:      "foo",
				Retention: LimitsPolicy,
				MaxAge:    time.Hour,
				Storage:   MemoryStorage,
				Replicas:  1,
			}},
		{name: "FileStore",
			mconfig: &StreamConfig{
				Name:      "foo",
				Retention: LimitsPolicy,
				MaxAge:    time.Hour,
				Storage:   FileStorage,
				Replicas:  1,
			}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			s := RunBasicJetStreamServer(t)
			defer s.Shutdown()

			mset, err := s.GlobalAccount().addStream(c.mconfig)
			if err != nil {
				t.Fatalf("Unexpected error adding stream: %v", err)
			}
			defer mset.delete()

			nc, js := jsClientConnect(t, s)
			defer nc.Close()

			js.Publish("foo", []byte("Hello World!"))
			state := mset.state()
			if state.Msgs != 1 {
				t.Fatalf("Expected 1 message, got %d", state.Msgs)
			}
			if state.Bytes == 0 {
				t.Fatalf("Expected non-zero bytes")
			}

			js.Publish("foo", []byte("Hello World Again!"))
			state = mset.state()
			if state.Msgs != 2 {
				t.Fatalf("Expected 2 messages, got %d", state.Msgs)
			}

			if err := mset.delete(); err != nil {
				t.Fatalf("Got an error deleting the stream: %v", err)
			}
		})
	}
}

func TestJetStreamAddStreamDiscardNew(t *testing.T) {
	cases := []struct {
		name    string
		mconfig *StreamConfig
	}{
		{name: "MemoryStore",
			mconfig: &StreamConfig{
				Name:     "foo",
				MaxMsgs:  10,
				MaxBytes: 4096,
				Discard:  DiscardNew,
				Storage:  MemoryStorage,
				Replicas: 1,
			}},
		{name: "FileStore",
			mconfig: &StreamConfig{
				Name:     "foo",
				MaxMsgs:  10,
				MaxBytes: 4096,
				Discard:  DiscardNew,
				Storage:  FileStorage,
				Replicas: 1,
			}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			s := RunBasicJetStreamServer(t)
			defer s.Shutdown()

			mset, err := s.GlobalAccount().addStream(c.mconfig)
			if err != nil {
				t.Fatalf("Unexpected error adding stream: %v", err)
			}
			defer mset.delete()

			nc := clientConnectToServer(t, s)
			defer nc.Close()

			subj := "foo"
			toSend := 10
			for i := 0; i < toSend; i++ {
				sendStreamMsg(t, nc, subj, fmt.Sprintf("MSG: %d", i+1))
			}
			// We expect this one to fail due to discard policy.
			resp, _ := nc.Request(subj, []byte("discard me"), 100*time.Millisecond)
			if resp == nil {
				t.Fatalf("No response, possible timeout?")
			}
			if pa := getPubAckResponse(resp.Data); pa == nil || pa.Error.Description != "maximum messages exceeded" || pa.Stream != "foo" {
				t.Fatalf("Expected to get an error about maximum messages, got %q", resp.Data)
			}

			// Now do bytes.
			mset.purge(nil)

			big := make([]byte, 8192)
			resp, _ = nc.Request(subj, big, 100*time.Millisecond)
			if resp == nil {
				t.Fatalf("No response, possible timeout?")
			}
			if pa := getPubAckResponse(resp.Data); pa == nil || pa.Error.Description != "maximum bytes exceeded" || pa.Stream != "foo" {
				t.Fatalf("Expected to get an error about maximum bytes, got %q", resp.Data)
			}
		})
	}
}

func TestJetStreamAutoTuneFSConfig(t *testing.T) {
	s := RunRandClientPortServer(t)
	defer s.Shutdown()

	jsconfig := &JetStreamConfig{MaxMemory: -1, MaxStore: 128 * 1024 * 1024, StoreDir: t.TempDir()}
	if err := s.EnableJetStream(jsconfig); err != nil {
		t.Fatalf("Expected no error, got %v", err)
	}

	maxMsgSize := int32(512)
	streamConfig := func(name string, maxMsgs, maxBytes int64) *StreamConfig {
		t.Helper()
		cfg := &StreamConfig{Name: name, MaxMsgSize: maxMsgSize, Storage: FileStorage}
		if maxMsgs > 0 {
			cfg.MaxMsgs = maxMsgs
		}
		if maxBytes > 0 {
			cfg.MaxBytes = maxBytes
		}
		return cfg
	}

	acc := s.GlobalAccount()

	testBlkSize := func(name string, maxMsgs, maxBytes int64, expectedBlkSize uint64) {
		t.Helper()
		mset, err := acc.addStream(streamConfig(name, maxMsgs, maxBytes))
		if err != nil {
			t.Fatalf("Unexpected error adding stream: %v", err)
		}
		defer mset.delete()
		fsCfg, err := mset.fileStoreConfig()
		if err != nil {
			t.Fatalf("Unexpected error retrieving file store: %v", err)
		}
		if fsCfg.BlockSize != expectedBlkSize {
			t.Fatalf("Expected auto tuned block size to be %d, got %d", expectedBlkSize, fsCfg.BlockSize)
		}
	}

	// Create a dummy stream, to ensure removing stream/account directories don't race.
	_, err := acc.addStream(streamConfig("dummy", 1, 0))
	require_NoError(t, err)

	testBlkSize("foo", 1, 0, FileStoreMinBlkSize)
	testBlkSize("foo", 1, 512, FileStoreMinBlkSize)
	testBlkSize("foo", 1, 1024*1024, defaultMediumBlockSize)
	testBlkSize("foo", 1, 8*1024*1024, defaultMediumBlockSize)
	testBlkSize("foo_bar_baz", -1, 32*1024*1024, FileStoreMaxBlkSize)
}

func TestJetStreamPubAck(t *testing.T) {
	s := RunBasicJetStreamServer(t)
	defer s.Shutdown()

	sname := "PUBACK"
	acc := s.GlobalAccount()
	mconfig := &StreamConfig{Name: sname, Subjects: []string{"foo"}, Storage: MemoryStorage}
	mset, err := acc.addStream(mconfig)
	if err != nil {
		t.Fatalf("Unexpected error adding stream: %v", err)
	}
	defer mset.delete()

	nc := clientConnectToServer(t, s)
	defer nc.Close()

	checkRespDetails := func(resp *nats.Msg, err error, seq uint64) {
		if err != nil {
			t.Fatalf("Unexpected error from send stream msg: %v", err)
		}
		if resp == nil {
			t.Fatalf("No response from send stream msg")
		}
		pa := getPubAckResponse(resp.Data)
		if pa == nil || pa.Error != nil {
			t.Fatalf("Expected a valid JetStreamPubAck, got %q", resp.Data)
		}
		if pa.Stream != sname {
			t.Fatalf("Expected %q for stream name, got %q", sname, pa.Stream)
		}
		if pa.Sequence != seq {
			t.Fatalf("Expected %d for sequence, got %d", seq, pa.Sequence)
		}
	}

	// Send messages and make sure pubAck details are correct.
	for i := uint64(1); i <= 1000; i++ {
		resp, err := nc.Request("foo", []byte("HELLO"), 100*time.Millisecond)
		checkRespDetails(resp, err, i)
	}
}

func TestJetStreamNextReqFromMsg(t *testing.T) {
	bef := time.Now()
	expires, _, _, _, _, _, _, err := nextReqFromMsg([]byte(`{"expires":5000000000}`)) // nanoseconds
	require_NoError(t, err)
	now := time.Now()
	if expires.Before(bef.Add(5*time.Second)) || expires.After(now.Add(5*time.Second)) {
		t.Fatal("Expires out of expected range")
	}
}

func TestJetStreamNoPanicOnRaceBetweenShutdownAndConsumerDelete(t *testing.T) {
	cases := []struct {
		name    string
		mconfig *StreamConfig
	}{
		{"MemoryStore", &StreamConfig{Name: "MY_STREAM", Storage: MemoryStorage}},
		{"FileStore", &StreamConfig{Name: "MY_STREAM", Storage: FileStorage}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			s := RunBasicJetStreamServer(t)
			defer s.Shutdown()

			mset, err := s.GlobalAccount().addStream(c.mconfig)
			if err != nil {
				t.Fatalf("Unexpected error adding stream: %v", err)
			}
			defer mset.delete()

			var cons []*consumer
			for i := 0; i < 100; i++ {
				o, err := mset.addConsumer(&ConsumerConfig{
					Durable:   fmt.Sprintf("d%d", i),
					AckPolicy: AckExplicit,
				})
				require_NoError(t, err)
				defer o.delete()
				cons = append(cons, o)
			}

			wg := sync.WaitGroup{}
			wg.Add(1)
			go func() {
				defer wg.Done()
				for _, c := range cons {
					c.delete()
				}
			}()
			time.Sleep(10 * time.Millisecond)
			s.Shutdown()
		})
	}
}

func TestJetStreamAddStreamMaxMsgSize(t *testing.T) {
	cases := []struct {
		name    string
		mconfig *StreamConfig
	}{
		{name: "MemoryStore",
			mconfig: &StreamConfig{
				Name:       "foo",
				Retention:  LimitsPolicy,
				MaxAge:     time.Hour,
				Storage:    MemoryStorage,
				MaxMsgSize: 22,
				Replicas:   1,
			}},
		{name: "FileStore",
			mconfig: &StreamConfig{
				Name:       "foo",
				Retention:  LimitsPolicy,
				MaxAge:     time.Hour,
				Storage:    FileStorage,
				MaxMsgSize: 22,
				Replicas:   1,
			}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			s := RunBasicJetStreamServer(t)
			defer s.Shutdown()

			mset, err := s.GlobalAccount().addStream(c.mconfig)
			if err != nil {
				t.Fatalf("Unexpected error adding stream: %v", err)
			}
			defer mset.delete()

			nc := clientConnectToServer(t, s)
			defer nc.Close()

			if _, err := nc.Request("foo", []byte("Hello World!"), time.Second); err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}

			tooBig := []byte("1234567890ABCDEFGHIJKLMNOPQRSTUVWXYZ")
			resp, err := nc.Request("foo", tooBig, time.Second)
			require_NoError(t, err)
			if pa := getPubAckResponse(resp.Data); pa == nil || pa.Error.Description != "message size exceeds maximum allowed" {
				t.Fatalf("Expected to get an error for maximum message size, got %q", pa.Error)
			}
		})
	}
}

func TestJetStreamAddStreamCanonicalNames(t *testing.T) {
	s := RunBasicJetStreamServer(t)
	defer s.Shutdown()

	acc := s.GlobalAccount()

	expectErr := func(_ *stream, err error) {
		t.Helper()
		if !IsNatsErr(err, JSStreamInvalidConfigF) {
			t.Fatalf("Expected error but got none")
		}
	}

	expectErr(acc.addStream(&StreamConfig{Name: "foo.bar"}))
	expectErr(acc.addStream(&StreamConfig{Name: "foo.bar."}))
	expectErr(acc.addStream(&StreamConfig{Name: "foo.*"}))
	expectErr(acc.addStream(&StreamConfig{Name: "foo.>"}))
	expectErr(acc.addStream(&StreamConfig{Name: "*"}))
	expectErr(acc.addStream(&StreamConfig{Name: ">"}))
	expectErr(acc.addStream(&StreamConfig{Name: "*>"}))
}

func TestJetStreamAddStreamBadSubjects(t *testing.T) {
	s := RunBasicJetStreamServer(t)
	defer s.Shutdown()

	// Client for API requests.
	nc := clientConnectToServer(t, s)
	defer nc.Close()

	expectAPIErr := func(cfg StreamConfig) {
		t.Helper()
		req, err := json.Marshal(cfg)
		require_NoError(t, err)
		resp, _ := nc.Request(fmt.Sprintf(JSApiStreamCreateT, cfg.Name), req, time.Second)
		var scResp JSApiStreamCreateResponse
		if err := json.Unmarshal(resp.Data, &scResp); err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}

		require_Error(t, scResp.ToError(), NewJSStreamInvalidConfigError(fmt.Errorf("invalid subject")))
	}

	expectAPIErr(StreamConfig{Name: "MyStream", Storage: MemoryStorage, Subjects: []string{"foo.bar."}})
	expectAPIErr(StreamConfig{Name: "MyStream", Storage: MemoryStorage, Subjects: []string{".."}})
	expectAPIErr(StreamConfig{Name: "MyStream", Storage: MemoryStorage, Subjects: []string{".*"}})
	expectAPIErr(StreamConfig{Name: "MyStream", Storage: MemoryStorage, Subjects: []string{".>"}})
	expectAPIErr(StreamConfig{Name: "MyStream", Storage: MemoryStorage, Subjects: []string{" x"}})
	expectAPIErr(StreamConfig{Name: "MyStream", Storage: MemoryStorage, Subjects: []string{"y "}})
}

func TestJetStreamMaxConsumers(t *testing.T) {
	s := RunBasicJetStreamServer(t)
	defer s.Shutdown()

	nc, js := jsClientConnect(t, s)
	defer nc.Close()

	cfg := &nats.StreamConfig{
		Name:         "MAXC",
		Storage:      nats.MemoryStorage,
		Subjects:     []string{"in.maxc.>"},
		MaxConsumers: 2,
	}
	if _, err := js.AddStream(cfg); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	si, err := js.StreamInfo("MAXC")
	require_NoError(t, err)
	if si.Config.MaxConsumers != 2 {
		t.Fatalf("Expected max of 2, got %d", si.Config.MaxConsumers)
	}
	// Make sure we get the right error.
	// This should succeed.
	if _, err := js.PullSubscribe("in.maxc.foo", "maxc_foo"); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	// Create for the same consumer must be idempotent, and not trigger limit.
	if _, err := js.PullSubscribe("in.maxc.foo", "maxc_foo"); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	// Create (with explicit create API) for the same consumer must be idempotent, and not trigger limit.
	obsReq := CreateConsumerRequest{
		Stream: "MAXC",
		Config: ConsumerConfig{Durable: "maxc_baz"},
		Action: ActionCreate,
	}
	req, err := json.Marshal(obsReq)
	require_NoError(t, err)

	msg, err := nc.Request(fmt.Sprintf(JSApiDurableCreateT, "MAXC", "maxc_baz"), req, time.Second)
	require_NoError(t, err)
	var resp JSApiConsumerInfoResponse
	require_NoError(t, json.Unmarshal(msg.Data, &resp))
	if resp.Error != nil {
		t.Fatalf("Unexpected error: %v", resp.Error)
	}

	msg, err = nc.Request(fmt.Sprintf(JSApiDurableCreateT, "MAXC", "maxc_baz"), req, time.Second)
	require_NoError(t, err)
	var resp2 JSApiConsumerInfoResponse
	require_NoError(t, json.Unmarshal(msg.Data, &resp2))
	if resp2.Error != nil {
		t.Fatalf("Unexpected error: %v", resp2.Error)
	}

	// Exceeds limit.
	if _, err := js.SubscribeSync("in.maxc.bar"); err == nil {
		t.Fatalf("Expected error but got none")
	}
}

func TestJetStreamAddStreamOverlappingSubjects(t *testing.T) {
	mconfig := &StreamConfig{
		Name:     "ok",
		Storage:  MemoryStorage,
		Subjects: []string{"foo", "bar", "baz.*", "foo.bar.baz.>"},
	}

	s := RunBasicJetStreamServer(t)
	defer s.Shutdown()

	acc := s.GlobalAccount()
	mset, err := acc.addStream(mconfig)
	if err != nil {
		t.Fatalf("Unexpected error adding stream: %v", err)
	}
	defer mset.delete()

	expectErr := func(_ *stream, err error) {
		t.Helper()
		if err == nil || !strings.Contains(err.Error(), "subjects overlap") {
			t.Fatalf("Expected error but got none")
		}
	}

	// Test that any overlapping subjects will fail.
	expectErr(acc.addStream(&StreamConfig{Name: "foo"}))
	expectErr(acc.addStream(&StreamConfig{Name: "a", Subjects: []string{"baz", "bar"}}))
	expectErr(acc.addStream(&StreamConfig{Name: "b", Subjects: []string{">"}, NoAck: true}))
	expectErr(acc.addStream(&StreamConfig{Name: "c", Subjects: []string{"baz.33"}}))

	// Using NoAck on the following because technically they overlap with the $JS.> namespace...
	expectErr(acc.addStream(&StreamConfig{Name: "d", Subjects: []string{"*.33"}, NoAck: true}))
	expectErr(acc.addStream(&StreamConfig{Name: "e", Subjects: []string{"*.>"}, NoAck: true}))
	expectErr(acc.addStream(&StreamConfig{Name: "f", Subjects: []string{"foo.bar", "*.bar.>"}, NoAck: true}))
}

func TestJetStreamAddStreamOverlapWithJSAPISubjects(t *testing.T) {
	s := RunBasicJetStreamServer(t)
	defer s.Shutdown()

	acc := s.GlobalAccount()

	expectNoErr := func(_ *stream, err error) {
		t.Helper()
		switch {
		case err == nil:
		default:
			t.Errorf("Unexpected error: %v", err)
		}
	}

	expectErr := func(_ *stream, err error) {
		t.Helper()
		switch {
		case err == nil:
			t.Errorf("Expected error but got none")
		case !strings.Contains(err.Error(), "subjects that overlap with jetstream api"):
		case !strings.Contains(err.Error(), "subjects that overlap with system api"):
		case !strings.Contains(err.Error(), "capturing all subjects requires no-ack to be true"):
		default:
			t.Errorf("Unexpected error: %v", err)
		}
	}

	// Test that any overlapping subjects with our JSAPI should fail.
	expectErr(acc.addStream(&StreamConfig{Name: "a", Subjects: []string{"$JS.API.foo", "$JS.API.bar"}}))
	expectErr(acc.addStream(&StreamConfig{Name: "b", Subjects: []string{"$JS.API.>"}}))
	expectErr(acc.addStream(&StreamConfig{Name: "c", Subjects: []string{"$JS.API.*"}}))
	expectErr(acc.addStream(&StreamConfig{Name: "d", Subjects: []string{"$JS.>"}}))
	expectErr(acc.addStream(&StreamConfig{Name: "e", Subjects: []string{"$SYS.>"}}))
	expectErr(acc.addStream(&StreamConfig{Name: "f", Subjects: []string{"*.>"}}))
	expectErr(acc.addStream(&StreamConfig{Name: "g", Subjects: []string{">"}}))

	// Events and advisories should be OK.
	expectNoErr(acc.addStream(&StreamConfig{Name: "h", Subjects: []string{"$JS.EVENT.>"}}))
	expectNoErr(acc.addStream(&StreamConfig{Name: "i", Subjects: []string{"$SYS.ACCOUNT.>"}}))

	// So should a full wild-card with NoAck, but need to clean up overlapping streams first.
	for _, name := range []string{"h", "i"} {
		mset, err := acc.lookupStream(name)
		require_NoError(t, err)
		require_NoError(t, mset.delete())
	}
	expectNoErr(acc.addStream(&StreamConfig{Name: "j", Subjects: []string{">"}, NoAck: true}))
}

func TestJetStreamAddStreamSameConfigOK(t *testing.T) {
	mconfig := &StreamConfig{
		Name:     "ok",
		Subjects: []string{"foo", "bar", "baz.*", "foo.bar.baz.>"},
		Storage:  MemoryStorage,
	}

	s := RunBasicJetStreamServer(t)
	defer s.Shutdown()

	acc := s.GlobalAccount()
	mset, err := acc.addStream(mconfig)
	if err != nil {
		t.Fatalf("Unexpected error adding stream: %v", err)
	}
	defer mset.delete()

	// Adding again with same config should be idempotent.
	if _, err = acc.addStream(mconfig); err != nil {
		t.Fatalf("Unexpected error adding stream: %v", err)
	}
}

func sendStreamMsg(t *testing.T, nc *nats.Conn, subject, msg string) *PubAck {
	t.Helper()
	resp, err := nc.Request(subject, []byte(msg), 500*time.Millisecond)
	if resp == nil {
		t.Fatalf("No response for %q (error: %v)", msg, err)
	}
	pa := getPubAckResponse(resp.Data)
	if pa == nil || pa.Error != nil {
		t.Fatalf("Expected a valid JetStreamPubAck, got %q", resp.Data)
	}
	return pa.PubAck
}

func TestJetStreamBasicAckPublish(t *testing.T) {
	cases := []struct {
		name    string
		mconfig *StreamConfig
	}{
		{"MemoryStore", &StreamConfig{Name: "foo", Storage: MemoryStorage, Subjects: []string{"foo.*"}}},
		{"FileStore", &StreamConfig{Name: "foo", Storage: FileStorage, Subjects: []string{"foo.*"}}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			s := RunBasicJetStreamServer(t)
			defer s.Shutdown()

			mset, err := s.GlobalAccount().addStream(c.mconfig)
			if err != nil {
				t.Fatalf("Unexpected error adding stream: %v", err)
			}
			defer mset.delete()

			nc := clientConnectToServer(t, s)
			defer nc.Close()

			for i := 0; i < 50; i++ {
				sendStreamMsg(t, nc, "foo.bar", "Hello World!")
			}
			state := mset.state()
			if state.Msgs != 50 {
				t.Fatalf("Expected 50 messages, got %d", state.Msgs)
			}
		})
	}
}

func TestJetStreamStateTimestamps(t *testing.T) {
	cases := []struct {
		name    string
		mconfig *StreamConfig
	}{
		{"MemoryStore", &StreamConfig{Name: "foo", Storage: MemoryStorage, Subjects: []string{"foo.*"}}},
		{"FileStore", &StreamConfig{Name: "foo", Storage: FileStorage, Subjects: []string{"foo.*"}}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			s := RunBasicJetStreamServer(t)
			defer s.Shutdown()

			mset, err := s.GlobalAccount().addStream(c.mconfig)
			if err != nil {
				t.Fatalf("Unexpected error adding stream: %v", err)
			}
			defer mset.delete()

			nc := clientConnectToServer(t, s)
			defer nc.Close()

			start := time.Now()
			delay := 250 * time.Millisecond
			sendStreamMsg(t, nc, "foo.bar", "Hello World!")
			time.Sleep(delay)
			sendStreamMsg(t, nc, "foo.bar", "Hello World Again!")

			state := mset.state()
			if state.FirstTime.Before(start) {
				t.Fatalf("Unexpected first message timestamp: %v", state.FirstTime)
			}
			if state.LastTime.Before(start.Add(delay)) {
				t.Fatalf("Unexpected last message timestamp: %v", state.LastTime)
			}
		})
	}
}

func TestJetStreamNoAckStream(t *testing.T) {
	cases := []struct {
		name    string
		mconfig *StreamConfig
	}{
		{"MemoryStore", &StreamConfig{Name: "foo", Storage: MemoryStorage, NoAck: true}},
		{"FileStore", &StreamConfig{Name: "foo", Storage: FileStorage, NoAck: true}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			s := RunBasicJetStreamServer(t)
			defer s.Shutdown()

			// We can use NoAck to suppress acks even when reply subjects are present.
			mset, err := s.GlobalAccount().addStream(c.mconfig)
			if err != nil {
				t.Fatalf("Unexpected error adding stream: %v", err)
			}
			defer mset.delete()

			nc := clientConnectToServer(t, s)
			defer nc.Close()

			if _, err := nc.Request("foo", []byte("Hello World!"), 25*time.Millisecond); err != nats.ErrTimeout {
				t.Fatalf("Expected a timeout error and no response with acks suppressed")
			}

			state := mset.state()
			if state.Msgs != 1 {
				t.Fatalf("Expected 1 message, got %d", state.Msgs)
			}
		})
	}
}

func TestJetStreamBasicDeliverSubject(t *testing.T) {
	cases := []struct {
		name    string
		mconfig *StreamConfig
	}{
		{"MemoryStore", &StreamConfig{Name: "MSET", Storage: MemoryStorage, Subjects: []string{"foo.*"}}},
		{"FileStore", &StreamConfig{Name: "MSET", Storage: FileStorage, Subjects: []string{"foo.*"}}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			s := RunBasicJetStreamServer(t)
			defer s.Shutdown()

			mset, err := s.GlobalAccount().addStream(c.mconfig)
			if err != nil {
				t.Fatalf("Unexpected error adding stream: %v", err)
			}
			defer mset.delete()

			nc := clientConnectToServer(t, s)
			defer nc.Close()

			toSend := 100
			sendSubj := "foo.bar"
			for i := 1; i <= toSend; i++ {
				sendStreamMsg(t, nc, sendSubj, strconv.Itoa(i))
			}
			state := mset.state()
			if state.Msgs != uint64(toSend) {
				t.Fatalf("Expected %d messages, got %d", toSend, state.Msgs)
			}

			// Now create an consumer. Use different connection.
			nc2 := clientConnectToServer(t, s)
			defer nc2.Close()

			sub, _ := nc2.SubscribeSync(nats.NewInbox())
			defer sub.Unsubscribe()
			nc2.Flush()

			o, err := mset.addConsumer(&ConsumerConfig{DeliverSubject: sub.Subject})
			if err != nil {
				t.Fatalf("Expected no error with registered interest, got %v", err)
			}
			defer o.delete()

			// Check for our messages.
			checkMsgs := func(seqOff int) {
				t.Helper()

				checkFor(t, 250*time.Millisecond, 10*time.Millisecond, func() error {
					if nmsgs, _, _ := sub.Pending(); err != nil || nmsgs != toSend {
						return fmt.Errorf("Did not receive correct number of messages: %d vs %d", nmsgs, toSend)
					}
					return nil
				})

				// Now let's check the messages
				for i := 0; i < toSend; i++ {
					m, _ := sub.NextMsg(time.Second)
					// JetStream will have the subject match the stream subject, not delivery subject.
					if m.Subject != sendSubj {
						t.Fatalf("Expected original subject of %q, but got %q", sendSubj, m.Subject)
					}
					// Now check that reply subject exists and has a sequence as the last token.
					if seq := o.seqFromReply(m.Reply); seq != uint64(i+seqOff) {
						t.Fatalf("Expected sequence of %d , got %d", i+seqOff, seq)
					}
					// Ack the message here.
					m.Respond(nil)
				}
			}

			checkMsgs(1)

			// Now send more and make sure delivery picks back up.
			for i := toSend + 1; i <= toSend*2; i++ {
				sendStreamMsg(t, nc, sendSubj, strconv.Itoa(i))
			}
			state = mset.state()
			if state.Msgs != uint64(toSend*2) {
				t.Fatalf("Expected %d messages, got %d", toSend*2, state.Msgs)
			}

			checkMsgs(101)

			checkSubEmpty := func() {
				if nmsgs, _, _ := sub.Pending(); err != nil || nmsgs != 0 {
					t.Fatalf("Expected sub to have no pending")
				}
			}
			checkSubEmpty()
			o.delete()

			// Now check for deliver last, deliver new and deliver by seq.
			o, err = mset.addConsumer(&ConsumerConfig{DeliverSubject: sub.Subject, DeliverPolicy: DeliverLast})
			if err != nil {
				t.Fatalf("Expected no error with registered interest, got %v", err)
			}
			defer o.delete()

			m, err := sub.NextMsg(time.Second)
			if err != nil {
				t.Fatalf("Did not get expected message, got %v", err)
			}
			// All Consumers start with sequence #1.
			if seq := o.seqFromReply(m.Reply); seq != 1 {
				t.Fatalf("Expected sequence to be 1, but got %d", seq)
			}
			// Check that is the last msg we sent though.
			if mseq, _ := strconv.Atoi(string(m.Data)); mseq != 200 {
				t.Fatalf("Expected message sequence to be 200, but got %d", mseq)
			}

			checkSubEmpty()
			o.delete()

			// Make sure we only got one message.
			if m, err := sub.NextMsg(5 * time.Millisecond); err == nil {
				t.Fatalf("Expected no msg, got %+v", m)
			}

			checkSubEmpty()
			o.delete()

			// Now try by sequence number.
			o, err = mset.addConsumer(&ConsumerConfig{DeliverSubject: sub.Subject, DeliverPolicy: DeliverByStartSequence, OptStartSeq: 101})
			if err != nil {
				t.Fatalf("Expected no error with registered interest, got %v", err)
			}
			defer o.delete()

			checkMsgs(1)

			// Now do push based queue-subscribers
			sub, _ = nc2.QueueSubscribeSync("_qg_", "dev")
			defer sub.Unsubscribe()
			nc2.Flush()

			o, err = mset.addConsumer(&ConsumerConfig{DeliverSubject: sub.Subject, DeliverGroup: "dev"})
			if err != nil {
				t.Fatalf("Expected no error with registered interest, got %v", err)
			}
			defer o.delete()

			// Since we sent another batch need check to be looking for 2x.
			toSend *= 2
			checkMsgs(1)
		})
	}
}

func workerModeConfig(name string) *ConsumerConfig {
	return &ConsumerConfig{Durable: name, AckPolicy: AckExplicit}
}

func TestJetStreamBasicWorkQueue(t *testing.T) {
	cases := []struct {
		name    string
		mconfig *StreamConfig
	}{
		{"MemoryStore", &StreamConfig{Name: "MY_MSG_SET", Storage: MemoryStorage, Subjects: []string{"foo", "bar"}}},
		{"FileStore", &StreamConfig{Name: "MY_MSG_SET", Storage: FileStorage, Subjects: []string{"foo", "bar"}}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			s := RunBasicJetStreamServer(t)
			defer s.Shutdown()

			mset, err := s.GlobalAccount().addStream(c.mconfig)
			if err != nil {
				t.Fatalf("Unexpected error adding stream: %v", err)
			}
			defer mset.delete()

			// Create basic work queue mode consumer.
			oname := "WQ"
			o, err := mset.addConsumer(workerModeConfig(oname))
			if err != nil {
				t.Fatalf("Expected no error with registered interest, got %v", err)
			}
			defer o.delete()

			if o.nextSeq() != 1 {
				t.Fatalf("Expected to be starting at sequence 1")
			}

			nc := clientConnectWithOldRequest(t, s)
			defer nc.Close()

			// Now load up some messages.
			toSend := 100
			sendSubj := "bar"
			for i := 0; i < toSend; i++ {
				sendStreamMsg(t, nc, sendSubj, "Hello World!")
			}
			state := mset.state()
			if state.Msgs != uint64(toSend) {
				t.Fatalf("Expected %d messages, got %d", toSend, state.Msgs)
			}

			getNext := func(seqno int) {
				t.Helper()
				nextMsg, err := nc.Request(o.requestNextMsgSubject(), nil, time.Second)
				if err != nil {
					t.Fatalf("Unexpected error for seq %d: %v", seqno, err)
				}
				if nextMsg.Subject != "bar" {
					t.Fatalf("Expected subject of %q, got %q", "bar", nextMsg.Subject)
				}
				if seq := o.seqFromReply(nextMsg.Reply); seq != uint64(seqno) {
					t.Fatalf("Expected sequence of %d , got %d", seqno, seq)
				}
			}

			// Make sure we can get the messages already there.
			for i := 1; i <= toSend; i++ {
				getNext(i)
			}

			// Now we want to make sure we can get a message that is published to the message
			// set as we are waiting for it.
			nextDelay := 50 * time.Millisecond

			go func() {
				time.Sleep(nextDelay)
				sendStreamMsg(t, nc, sendSubj, "Hello World!")
			}()

			start := time.Now()
			getNext(toSend + 1)
			if time.Since(start) < nextDelay {
				t.Fatalf("Received message too quickly")
			}

			// Now do same thing but combine waiting for new ones with sending.
			go func() {
				time.Sleep(nextDelay)
				for i := 0; i < toSend; i++ {
					nc.Request(sendSubj, []byte("Hello World!"), 50*time.Millisecond)
				}
			}()

			for i := toSend + 2; i < toSend*2+2; i++ {
				getNext(i)
			}
		})
	}
}

func TestJetStreamWorkQueueMaxWaiting(t *testing.T) {
	cases := []struct {
		name    string
		mconfig *StreamConfig
	}{
		{"MemoryStore", &StreamConfig{Name: "MY_MSG_SET", Storage: MemoryStorage, Subjects: []string{"foo", "bar"}}},
		{"FileStore", &StreamConfig{Name: "MY_MSG_SET", Storage: FileStorage, Subjects: []string{"foo", "bar"}}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			s := RunBasicJetStreamServer(t)
			defer s.Shutdown()

			mset, err := s.GlobalAccount().addStream(c.mconfig)
			if err != nil {
				t.Fatalf("Unexpected error adding stream: %v", err)
			}
			defer mset.delete()

			// Make sure these cases fail
			cfg := &ConsumerConfig{Durable: "foo", AckPolicy: AckExplicit, MaxWaiting: 10, DeliverSubject: "_INBOX.22"}
			if _, err := mset.addConsumer(cfg); err == nil {
				t.Fatalf("Expected an error with MaxWaiting set on non-pull based consumer")
			}

			// Create basic work queue mode consumer.
			wcfg := workerModeConfig("MAXWQ")
			o, err := mset.addConsumer(wcfg)
			if err != nil {
				t.Fatalf("Expected no error with registered interest, got %v", err)
			}
			defer o.delete()

			// Make sure we set default correctly.
			if cfg := o.config(); cfg.MaxWaiting != JSWaitQueueDefaultMax {
				t.Fatalf("Expected default max waiting to have been set to %d, got %d", JSWaitQueueDefaultMax, cfg.MaxWaiting)
			}

			expectWaiting := func(expected int) {
				t.Helper()
				checkFor(t, time.Second, 25*time.Millisecond, func() error {
					if oi := o.info(); oi.NumWaiting != expected {
						return fmt.Errorf("Expected %d waiting, got %d", expected, oi.NumWaiting)
					}
					return nil
				})
			}

			nc := clientConnectWithOldRequest(t, s)
			defer nc.Close()

			// Like muxed new INBOX.
			sub, _ := nc.SubscribeSync("req.*")
			defer sub.Unsubscribe()
			nc.Flush()

			checkSubPending := func(numExpected int) {
				t.Helper()
				checkFor(t, 200*time.Millisecond, 10*time.Millisecond, func() error {
					if nmsgs, _, err := sub.Pending(); err != nil || nmsgs != numExpected {
						return fmt.Errorf("Did not receive correct number of messages: %d vs %d", nmsgs, numExpected)
					}
					return nil
				})
			}

			getSubj := o.requestNextMsgSubject()
			// Queue up JSWaitQueueDefaultMax requests.
			for i := 0; i < JSWaitQueueDefaultMax; i++ {
				nc.PublishRequest(getSubj, fmt.Sprintf("req.%d", i), nil)
			}
			expectWaiting(JSWaitQueueDefaultMax)

			// We are at the max, so we should get a 409 saying that we have
			// exceeded the number of pull requests.
			m, err := nc.Request(getSubj, nil, 100*time.Millisecond)
			require_NoError(t, err)
			// Make sure this is the 409
			if v := m.Header.Get("Status"); v != "409" {
				t.Fatalf("Expected a 409 status code, got %q", v)
			}
			// The sub for the other requests should not have received anything
			checkSubPending(0)
			// Now send some messages that should make some of the requests complete
			sendStreamMsg(t, nc, "foo", "Hello World!")
			sendStreamMsg(t, nc, "bar", "Hello World!")
			expectWaiting(JSWaitQueueDefaultMax - 2)
		})
	}
}

func TestJetStreamWorkQueueWrapWaiting(t *testing.T) {
	cases := []struct {
		name    string
		mconfig *StreamConfig
	}{
		{"MemoryStore", &StreamConfig{Name: "MY_MSG_SET", Storage: MemoryStorage, Subjects: []string{"foo", "bar"}}},
		{"FileStore", &StreamConfig{Name: "MY_MSG_SET", Storage: FileStorage, Subjects: []string{"foo", "bar"}}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			s := RunBasicJetStreamServer(t)
			defer s.Shutdown()

			mset, err := s.GlobalAccount().addStream(c.mconfig)
			if err != nil {
				t.Fatalf("Unexpected error adding stream: %v", err)
			}
			defer mset.delete()

			maxWaiting := 8
			wcfg := workerModeConfig("WRAP")
			wcfg.MaxWaiting = maxWaiting

			o, err := mset.addConsumer(wcfg)
			if err != nil {
				t.Fatalf("Expected no error with registered interest, got %v", err)
			}
			defer o.delete()

			getSubj := o.requestNextMsgSubject()

			expectWaiting := func(expected int) {
				t.Helper()
				checkFor(t, time.Second, 25*time.Millisecond, func() error {
					if oi := o.info(); oi.NumWaiting != expected {
						return fmt.Errorf("Expected %d waiting, got %d", expected, oi.NumWaiting)
					}
					return nil
				})
			}

			nc := clientConnectToServer(t, s)
			defer nc.Close()

			sub, _ := nc.SubscribeSync("req.*")
			defer sub.Unsubscribe()
			nc.Flush()

			// Fill up waiting.
			for i := 0; i < maxWaiting; i++ {
				nc.PublishRequest(getSubj, fmt.Sprintf("req.%d", i), nil)
			}
			expectWaiting(maxWaiting)

			// Now use 1/2 of the waiting.
			for i := 0; i < maxWaiting/2; i++ {
				sendStreamMsg(t, nc, "foo", "Hello World!")
			}
			expectWaiting(maxWaiting / 2)

			// Now add in two (2) more pull requests.
			for i := maxWaiting; i < maxWaiting+2; i++ {
				nc.PublishRequest(getSubj, fmt.Sprintf("req.%d", i), nil)
			}
			expectWaiting(maxWaiting/2 + 2)

			// Now use second 1/2 of the waiting and the 2 extra.
			for i := 0; i < maxWaiting/2+2; i++ {
				sendStreamMsg(t, nc, "bar", "Hello World!")
			}
			expectWaiting(0)

			checkFor(t, 200*time.Millisecond, 10*time.Millisecond, func() error {
				if nmsgs, _, _ := sub.Pending(); err != nil || nmsgs != maxWaiting+2 {
					return fmt.Errorf("Expected sub to have %d pending, got %d", maxWaiting+2, nmsgs)
				}
				return nil
			})
		})
	}
}

func TestJetStreamWorkQueueRequest(t *testing.T) {
	cases := []struct {
		name    string
		mconfig *StreamConfig
	}{
		{"MemoryStore", &StreamConfig{Name: "MY_MSG_SET", Storage: MemoryStorage, Subjects: []string{"foo", "bar"}}},
		{"FileStore", &StreamConfig{Name: "MY_MSG_SET", Storage: FileStorage, Subjects: []string{"foo", "bar"}}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			s := RunBasicJetStreamServer(t)
			defer s.Shutdown()

			mset, err := s.GlobalAccount().addStream(c.mconfig)
			if err != nil {
				t.Fatalf("Unexpected error adding stream: %v", err)
			}
			defer mset.delete()

			o, err := mset.addConsumer(workerModeConfig("WRAP"))
			if err != nil {
				t.Fatalf("Expected no error with registered interest, got %v", err)
			}
			defer o.delete()

			nc := clientConnectToServer(t, s)
			defer nc.Close()

			toSend := 25
			for i := 0; i < toSend; i++ {
				sendStreamMsg(t, nc, "bar", "Hello World!")
			}

			reply := "_.consumer._"
			sub, _ := nc.SubscribeSync(reply)
			defer sub.Unsubscribe()

			getSubj := o.requestNextMsgSubject()

			checkSubPending := func(numExpected int) {
				t.Helper()
				checkFor(t, 200*time.Millisecond, 10*time.Millisecond, func() error {
					if nmsgs, _, _ := sub.Pending(); err != nil || nmsgs != numExpected {
						return fmt.Errorf("Did not receive correct number of messages: %d vs %d", nmsgs, numExpected)
					}
					return nil
				})
			}

			// Create a formal request object.
			req := &JSApiConsumerGetNextRequest{Batch: toSend}
			jreq, _ := json.Marshal(req)
			nc.PublishRequest(getSubj, reply, jreq)

			checkSubPending(toSend)

			// Now check that we can ask for NoWait
			req.Batch = 1
			req.NoWait = true
			jreq, _ = json.Marshal(req)

			resp, err := nc.Request(getSubj, jreq, 100*time.Millisecond)
			require_NoError(t, err)
			if status := resp.Header.Get("Status"); !strings.HasPrefix(status, "404") {
				t.Fatalf("Expected status code of 404")
			}
			// Load up more messages.
			for i := 0; i < toSend; i++ {
				sendStreamMsg(t, nc, "foo", "Hello World!")
			}
			// Now we will ask for a batch larger then what is queued up.
			req.Batch = toSend + 10
			req.NoWait = true
			jreq, _ = json.Marshal(req)

			nc.PublishRequest(getSubj, reply, jreq)
			// We should now have 2 * toSend + the 404 message.
			checkSubPending(2*toSend + 1)
			for i := 0; i < 2*toSend+1; i++ {
				sub.NextMsg(time.Millisecond)
			}
			checkSubPending(0)
			mset.purge(nil)

			// Now do expiration
			req.Batch = 1
			req.NoWait = false
			req.Expires = 100 * time.Millisecond
			jreq, _ = json.Marshal(req)

			nc.PublishRequest(getSubj, reply, jreq)
			// Let it expire
			time.Sleep(200 * time.Millisecond)

			// Send a few more messages. These should not be delivered to the sub.
			sendStreamMsg(t, nc, "foo", "Hello World!")
			sendStreamMsg(t, nc, "bar", "Hello World!")
			time.Sleep(100 * time.Millisecond)

			// Expect the request timed out message.
			checkSubPending(1)
			if resp, _ = sub.NextMsg(time.Millisecond); resp == nil {
				t.Fatalf("Expected an expired status message")
			}
			if status := resp.Header.Get("Status"); !strings.HasPrefix(status, "408") {
				t.Fatalf("Expected status code of 408")
			}

			// Send a new request, we should not get the 408 because our previous request
			// should have expired.
			nc.PublishRequest(getSubj, reply, jreq)
			checkSubPending(1)
			sub.NextMsg(time.Second)
			checkSubPending(0)
		})
	}
}

func TestJetStreamSubjectFiltering(t *testing.T) {
	cases := []struct {
		name    string
		mconfig *StreamConfig
	}{
		{"MemoryStore", &StreamConfig{Name: "MSET", Storage: MemoryStorage, Subjects: []string{"foo.*"}}},
		{"FileStore", &StreamConfig{Name: "MSET", Storage: FileStorage, Subjects: []string{"foo.*"}}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			s := RunBasicJetStreamServer(t)
			defer s.Shutdown()

			mset, err := s.GlobalAccount().addStream(c.mconfig)
			if err != nil {
				t.Fatalf("Unexpected error adding stream: %v", err)
			}
			defer mset.delete()

			nc := clientConnectToServer(t, s)
			defer nc.Close()

			toSend := 50
			subjA := "foo.A"
			subjB := "foo.B"

			for i := 0; i < toSend; i++ {
				sendStreamMsg(t, nc, subjA, "Hello World!")
				sendStreamMsg(t, nc, subjB, "Hello World!")
			}
			state := mset.state()
			if state.Msgs != uint64(toSend*2) {
				t.Fatalf("Expected %d messages, got %d", toSend*2, state.Msgs)
			}

			delivery := nats.NewInbox()
			sub, _ := nc.SubscribeSync(delivery)
			defer sub.Unsubscribe()
			nc.Flush()

			o, err := mset.addConsumer(&ConsumerConfig{DeliverSubject: delivery, FilterSubject: subjB})
			if err != nil {
				t.Fatalf("Expected no error with registered interest, got %v", err)
			}
			defer o.delete()

			// Now let's check the messages
			for i := 1; i <= toSend; i++ {
				m, err := sub.NextMsg(time.Second)
				require_NoError(t, err)
				// JetStream will have the subject match the stream subject, not delivery subject.
				// We want these to only be subjB.
				if m.Subject != subjB {
					t.Fatalf("Expected original subject of %q, but got %q", subjB, m.Subject)
				}
				// Now check that reply subject exists and has a sequence as the last token.
				if seq := o.seqFromReply(m.Reply); seq != uint64(i) {
					t.Fatalf("Expected sequence of %d , got %d", i, seq)
				}
				// Ack the message here.
				m.Respond(nil)
			}

			if nmsgs, _, _ := sub.Pending(); err != nil || nmsgs != 0 {
				t.Fatalf("Expected sub to have no pending")
			}
		})
	}
}

func TestJetStreamWorkQueueSubjectFiltering(t *testing.T) {
	cases := []struct {
		name    string
		mconfig *StreamConfig
	}{
		{"MemoryStore", &StreamConfig{Name: "MY_MSG_SET", Storage: MemoryStorage, Subjects: []string{"foo.*"}}},
		{"FileStore", &StreamConfig{Name: "MY_MSG_SET", Storage: FileStorage, Subjects: []string{"foo.*"}}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			s := RunBasicJetStreamServer(t)
			defer s.Shutdown()

			mset, err := s.GlobalAccount().addStream(c.mconfig)
			if err != nil {
				t.Fatalf("Unexpected error adding stream: %v", err)
			}
			defer mset.delete()

			nc := clientConnectToServer(t, s)
			defer nc.Close()

			toSend := 50
			subjA := "foo.A"
			subjB := "foo.B"

			for i := 0; i < toSend; i++ {
				sendStreamMsg(t, nc, subjA, "Hello World!")
				sendStreamMsg(t, nc, subjB, "Hello World!")
			}
			state := mset.state()
			if state.Msgs != uint64(toSend*2) {
				t.Fatalf("Expected %d messages, got %d", toSend*2, state.Msgs)
			}

			oname := "WQ"
			o, err := mset.addConsumer(&ConsumerConfig{Durable: oname, FilterSubject: subjA, AckPolicy: AckExplicit})
			if err != nil {
				t.Fatalf("Expected no error with registered interest, got %v", err)
			}
			defer o.delete()

			if o.nextSeq() != 1 {
				t.Fatalf("Expected to be starting at sequence 1")
			}

			getNext := func(seqno int) {
				t.Helper()
				nextMsg, err := nc.Request(o.requestNextMsgSubject(), nil, time.Second)
				require_NoError(t, err)
				if nextMsg.Subject != subjA {
					t.Fatalf("Expected subject of %q, got %q", subjA, nextMsg.Subject)
				}
				if seq := o.seqFromReply(nextMsg.Reply); seq != uint64(seqno) {
					t.Fatalf("Expected sequence of %d , got %d", seqno, seq)
				}
				nextMsg.Respond(nil)
			}

			// Make sure we can get the messages already there.
			for i := 1; i <= toSend; i++ {
				getNext(i)
			}
		})
	}
}

func TestJetStreamWildcardSubjectFiltering(t *testing.T) {
	cases := []struct {
		name    string
		mconfig *StreamConfig
	}{
		{"MemoryStore", &StreamConfig{Name: "ORDERS", Storage: MemoryStorage, Subjects: []string{"orders.*.*"}}},
		{"FileStore", &StreamConfig{Name: "ORDERS", Storage: FileStorage, Subjects: []string{"orders.*.*"}}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			s := RunBasicJetStreamServer(t)
			defer s.Shutdown()

			mset, err := s.GlobalAccount().addStream(c.mconfig)
			if err != nil {
				t.Fatalf("Unexpected error adding stream: %v", err)
			}
			defer mset.delete()

			nc := clientConnectToServer(t, s)
			defer nc.Close()

			toSend := 100
			for i := 1; i <= toSend; i++ {
				subj := fmt.Sprintf("orders.%d.%s", i, "NEW")
				sendStreamMsg(t, nc, subj, "new order")
			}
			// Randomly move 25 to shipped.
			toShip := 25
			shipped := make(map[int]bool)
			for i := 0; i < toShip; {
				orderId := rand.IntN(toSend-1) + 1
				if shipped[orderId] {
					continue
				}
				subj := fmt.Sprintf("orders.%d.%s", orderId, "SHIPPED")
				sendStreamMsg(t, nc, subj, "shipped order")
				shipped[orderId] = true
				i++
			}
			state := mset.state()
			if state.Msgs != uint64(toSend+toShip) {
				t.Fatalf("Expected %d messages, got %d", toSend+toShip, state.Msgs)
			}

			delivery := nats.NewInbox()
			sub, _ := nc.SubscribeSync(delivery)
			defer sub.Unsubscribe()
			nc.Flush()

			// Get all shipped.
			o, err := mset.addConsumer(&ConsumerConfig{DeliverSubject: delivery, FilterSubject: "orders.*.SHIPPED"})
			if err != nil {
				t.Fatalf("Expected no error with registered interest, got %v", err)
			}
			defer o.delete()

			checkFor(t, time.Second, 25*time.Millisecond, func() error {
				if nmsgs, _, _ := sub.Pending(); err != nil || nmsgs != toShip {
					return fmt.Errorf("Did not receive correct number of messages: %d vs %d", nmsgs, toShip)
				}
				return nil
			})
			for nmsgs, _, _ := sub.Pending(); nmsgs > 0; nmsgs, _, _ = sub.Pending() {
				sub.NextMsg(time.Second)
			}
			if nmsgs, _, _ := sub.Pending(); nmsgs != 0 {
				t.Fatalf("Expected no pending, got %d", nmsgs)
			}

			// Get all new
			o, err = mset.addConsumer(&ConsumerConfig{DeliverSubject: delivery, FilterSubject: "orders.*.NEW"})
			if err != nil {
				t.Fatalf("Expected no error with registered interest, got %v", err)
			}
			defer o.delete()

			checkFor(t, time.Second, 25*time.Millisecond, func() error {
				if nmsgs, _, _ := sub.Pending(); err != nil || nmsgs != toSend {
					return fmt.Errorf("Did not receive correct number of messages: %d vs %d", nmsgs, toSend)
				}
				return nil
			})
			for nmsgs, _, _ := sub.Pending(); nmsgs > 0; nmsgs, _, _ = sub.Pending() {
				sub.NextMsg(time.Second)
			}
			if nmsgs, _, _ := sub.Pending(); nmsgs != 0 {
				t.Fatalf("Expected no pending, got %d", nmsgs)
			}

			// Now grab a single orderId that has shipped, so we should have two messages.
			var orderId int
			for orderId = range shipped {
				break
			}
			subj := fmt.Sprintf("orders.%d.*", orderId)
			o, err = mset.addConsumer(&ConsumerConfig{DeliverSubject: delivery, FilterSubject: subj})
			if err != nil {
				t.Fatalf("Expected no error with registered interest, got %v", err)
			}
			defer o.delete()

			checkFor(t, time.Second, 25*time.Millisecond, func() error {
				if nmsgs, _, _ := sub.Pending(); err != nil || nmsgs != 2 {
					return fmt.Errorf("Did not receive correct number of messages: %d vs %d", nmsgs, 2)
				}
				return nil
			})
		})
	}
}

func TestJetStreamWorkQueueAckAndNext(t *testing.T) {
	cases := []struct {
		name    string
		mconfig *StreamConfig
	}{
		{"MemoryStore", &StreamConfig{Name: "MY_MSG_SET", Storage: MemoryStorage, Subjects: []string{"foo", "bar"}}},
		{"FileStore", &StreamConfig{Name: "MY_MSG_SET", Storage: FileStorage, Subjects: []string{"foo", "bar"}}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			s := RunBasicJetStreamServer(t)
			defer s.Shutdown()

			mset, err := s.GlobalAccount().addStream(c.mconfig)
			if err != nil {
				t.Fatalf("Unexpected error adding stream: %v", err)
			}
			defer mset.delete()

			// Create basic work queue mode consumer.
			oname := "WQ"
			o, err := mset.addConsumer(workerModeConfig(oname))
			if err != nil {
				t.Fatalf("Expected no error with registered interest, got %v", err)
			}
			defer o.delete()

			if o.nextSeq() != 1 {
				t.Fatalf("Expected to be starting at sequence 1")
			}

			nc := clientConnectToServer(t, s)
			defer nc.Close()

			// Now load up some messages.
			toSend := 100
			sendSubj := "bar"
			for i := 0; i < toSend; i++ {
				sendStreamMsg(t, nc, sendSubj, "Hello World!")
			}
			state := mset.state()
			if state.Msgs != uint64(toSend) {
				t.Fatalf("Expected %d messages, got %d", toSend, state.Msgs)
			}

			sub, _ := nc.SubscribeSync(nats.NewInbox())
			defer sub.Unsubscribe()

			// Kick things off.
			// For normal work queue semantics, you send requests to the subject with stream and consumer name.
			// We will do this to start it off then use ack+next to get other messages.
			nc.PublishRequest(o.requestNextMsgSubject(), sub.Subject, nil)

			for i := 0; i < toSend; i++ {
				m, err := sub.NextMsg(time.Second)
				if err != nil {
					t.Fatalf("Unexpected error waiting for messages: %v", err)
				}

				if !bytes.Equal(m.Data, []byte("Hello World!")) {
					t.Fatalf("Got an invalid message from the stream: %q", m.Data)
				}

				nc.PublishRequest(m.Reply, sub.Subject, AckNext)
			}
		})
	}
}

func TestJetStreamWorkQueueRequestBatch(t *testing.T) {
	cases := []struct {
		name    string
		mconfig *StreamConfig
	}{
		{"MemoryStore", &StreamConfig{Name: "MY_MSG_SET", Storage: MemoryStorage, Subjects: []string{"foo", "bar"}}},
		{"FileStore", &StreamConfig{Name: "MY_MSG_SET", Storage: FileStorage, Subjects: []string{"foo", "bar"}}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			s := RunBasicJetStreamServer(t)
			defer s.Shutdown()

			mset, err := s.GlobalAccount().addStream(c.mconfig)
			if err != nil {
				t.Fatalf("Unexpected error adding stream: %v", err)
			}
			defer mset.delete()

			// Create basic work queue mode consumer.
			oname := "WQ"
			o, err := mset.addConsumer(workerModeConfig(oname))
			if err != nil {
				t.Fatalf("Expected no error with registered interest, got %v", err)
			}
			defer o.delete()

			if o.nextSeq() != 1 {
				t.Fatalf("Expected to be starting at sequence 1")
			}

			nc := clientConnectToServer(t, s)
			defer nc.Close()

			// Now load up some messages.
			toSend := 100
			sendSubj := "bar"
			for i := 0; i < toSend; i++ {
				sendStreamMsg(t, nc, sendSubj, "Hello World!")
			}
			state := mset.state()
			if state.Msgs != uint64(toSend) {
				t.Fatalf("Expected %d messages, got %d", toSend, state.Msgs)
			}

			sub, _ := nc.SubscribeSync(nats.NewInbox())
			defer sub.Unsubscribe()

			// For normal work queue semantics, you send requests to the subject with stream and consumer name.
			// We will do this to start it off then use ack+next to get other messages.
			// Kick things off with batch size of 50.
			batchSize := 50
			nc.PublishRequest(o.requestNextMsgSubject(), sub.Subject, []byte(strconv.Itoa(batchSize)))

			// We should receive batchSize with no acks or additional requests.
			checkFor(t, 250*time.Millisecond, 10*time.Millisecond, func() error {
				if nmsgs, _, _ := sub.Pending(); err != nil || nmsgs != batchSize {
					return fmt.Errorf("Did not receive correct number of messages: %d vs %d", nmsgs, batchSize)
				}
				return nil
			})

			// Now queue up the request without messages and add them after.
			sub, _ = nc.SubscribeSync(nats.NewInbox())
			defer sub.Unsubscribe()
			mset.purge(nil)

			nc.PublishRequest(o.requestNextMsgSubject(), sub.Subject, []byte(strconv.Itoa(batchSize)))
			nc.Flush() // Make sure its registered.

			for i := 0; i < toSend; i++ {
				sendStreamMsg(t, nc, sendSubj, "Hello World!")
			}

			// We should receive batchSize with no acks or additional requests.
			checkFor(t, 250*time.Millisecond, 10*time.Millisecond, func() error {
				if nmsgs, _, _ := sub.Pending(); err != nil || nmsgs != batchSize {
					return fmt.Errorf("Did not receive correct number of messages: %d vs %d", nmsgs, batchSize)
				}
				return nil
			})
		})
	}
}

func TestJetStreamWorkQueueRetentionStream(t *testing.T) {
	cases := []struct {
		name    string
		mconfig *StreamConfig
	}{
		{name: "MemoryStore", mconfig: &StreamConfig{
			Name:      "MWQ",
			Storage:   MemoryStorage,
			Subjects:  []string{"MY_WORK_QUEUE.>"},
			Retention: WorkQueuePolicy},
		},
		{name: "FileStore", mconfig: &StreamConfig{
			Name:      "MWQ",
			Storage:   FileStorage,
			Subjects:  []string{"MY_WORK_QUEUE.>"},
			Retention: WorkQueuePolicy},
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			s := RunBasicJetStreamServer(t)
			defer s.Shutdown()

			mset, err := s.GlobalAccount().addStream(c.mconfig)
			if err != nil {
				t.Fatalf("Unexpected error adding stream: %v", err)
			}
			defer mset.delete()

			// This type of stream has restrictions which we will test here.
			// DeliverAll is only start mode allowed.
			if _, err := mset.addConsumer(&ConsumerConfig{DeliverPolicy: DeliverLast}); err == nil {
				t.Fatalf("Expected an error with anything but DeliverAll")
			}

			// We will create a non-partitioned consumer. This should succeed.
			o, err := mset.addConsumer(&ConsumerConfig{Durable: "PBO", AckPolicy: AckExplicit})
			require_NoError(t, err)
			defer o.delete()

			// Now if we create another this should fail, only can have one non-partitioned.
			if _, err := mset.addConsumer(&ConsumerConfig{}); err == nil {
				t.Fatalf("Expected an error on attempt for second consumer for a workqueue")
			}
			o.delete()

			if numo := mset.numConsumers(); numo != 0 {
				t.Fatalf("Expected to have zero consumers, got %d", numo)
			}

			// Now add in an consumer that has a partition.
			pindex := 1
			pConfig := func(pname string) *ConsumerConfig {
				dname := fmt.Sprintf("PPBO-%d", pindex)
				pindex += 1
				return &ConsumerConfig{Durable: dname, FilterSubject: pname, AckPolicy: AckExplicit}
			}
			o, err = mset.addConsumer(pConfig("MY_WORK_QUEUE.A"))
			require_NoError(t, err)
			defer o.delete()

			// Now creating another with separate partition should work.
			o2, err := mset.addConsumer(pConfig("MY_WORK_QUEUE.B"))
			require_NoError(t, err)
			defer o2.delete()

			// Anything that would overlap should fail though.
			if _, err := mset.addConsumer(pConfig("MY_WORK_QUEUE.A")); err == nil {
				t.Fatalf("Expected an error on attempt for partitioned consumer for a workqueue")
			}
			if _, err := mset.addConsumer(pConfig("MY_WORK_QUEUE.B")); err == nil {
				t.Fatalf("Expected an error on attempt for partitioned consumer for a workqueue")
			}

			o3, err := mset.addConsumer(pConfig("MY_WORK_QUEUE.C"))
			require_NoError(t, err)

			o.delete()
			o2.delete()
			o3.delete()

			// Test with wildcards, first from wider to narrower
			o, err = mset.addConsumer(pConfig("MY_WORK_QUEUE.>"))
			require_NoError(t, err)
			if _, err := mset.addConsumer(pConfig("MY_WORK_QUEUE.*.BAR")); err == nil {
				t.Fatalf("Expected an error on attempt for partitioned consumer for a workqueue")
			}
			o.delete()

			// Now from narrower to wider
			o, err = mset.addConsumer(pConfig("MY_WORK_QUEUE.*.BAR"))
			require_NoError(t, err)
			if _, err := mset.addConsumer(pConfig("MY_WORK_QUEUE.>")); err == nil {
				t.Fatalf("Expected an error on attempt for partitioned consumer for a workqueue")
			}
			o.delete()

			// Push based will be allowed now, including ephemerals.
			// They can not overlap etc meaning same rules as above apply.
			o4, err := mset.addConsumer(&ConsumerConfig{
				Durable:        "DURABLE",
				DeliverSubject: "SOME.SUBJ",
				AckPolicy:      AckExplicit,
			})
			if err != nil {
				t.Fatalf("Unexpected Error: %v", err)
			}
			defer o4.delete()

			// Now try to create an ephemeral
			nc := clientConnectToServer(t, s)
			defer nc.Close()

			sub, _ := nc.SubscribeSync(nats.NewInbox())
			defer sub.Unsubscribe()
			nc.Flush()

			// This should fail at first due to conflict above.
			ephCfg := &ConsumerConfig{DeliverSubject: sub.Subject, AckPolicy: AckExplicit}
			if _, err := mset.addConsumer(ephCfg); err == nil {
				t.Fatalf("Expected an error ")
			}
			// Delete of o4 should clear.
			o4.delete()
			o5, err := mset.addConsumer(ephCfg)
			if err != nil {
				t.Fatalf("Unexpected Error: %v", err)
			}
			defer o5.delete()
		})
	}
}

func TestJetStreamAckAllRedelivery(t *testing.T) {
	cases := []struct {
		name    string
		mconfig *StreamConfig
	}{
		{"MemoryStore", &StreamConfig{Name: "MY_S22", Storage: MemoryStorage}},
		{"FileStore", &StreamConfig{Name: "MY_S22", Storage: FileStorage}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			s := RunBasicJetStreamServer(t)
			defer s.Shutdown()

			mset, err := s.GlobalAccount().addStream(c.mconfig)
			if err != nil {
				t.Fatalf("Unexpected error adding stream: %v", err)
			}
			defer mset.delete()

			nc := clientConnectToServer(t, s)
			defer nc.Close()

			// Now load up some messages.
			toSend := 100
			for i := 0; i < toSend; i++ {
				sendStreamMsg(t, nc, c.mconfig.Name, "Hello World!")
			}
			state := mset.state()
			if state.Msgs != uint64(toSend) {
				t.Fatalf("Expected %d messages, got %d", toSend, state.Msgs)
			}

			sub, _ := nc.SubscribeSync(nats.NewInbox())
			defer sub.Unsubscribe()
			nc.Flush()

			o, err := mset.addConsumer(&ConsumerConfig{
				DeliverSubject: sub.Subject,
				AckWait:        50 * time.Millisecond,
				AckPolicy:      AckAll,
			})
			if err != nil {
				t.Fatalf("Unexpected error adding consumer: %v", err)
			}
			defer o.delete()

			// Wait for messages.
			// We will do 5 redeliveries.
			for i := 1; i <= 5; i++ {
				checkFor(t, 500*time.Millisecond, 10*time.Millisecond, func() error {
					if nmsgs, _, _ := sub.Pending(); err != nil || nmsgs != toSend*i {
						return fmt.Errorf("Did not receive correct number of messages: %d vs %d", nmsgs, toSend*i)
					}
					return nil
				})
			}
			// Stop redeliveries.
			o.delete()

			// Now make sure that they are all redelivered in order for each redelivered batch.
			for l := 1; l <= 5; l++ {
				for i := 1; i <= toSend; i++ {
					m, _ := sub.NextMsg(time.Second)
					if seq := o.streamSeqFromReply(m.Reply); seq != uint64(i) {
						t.Fatalf("Expected stream sequence of %d, got %d", i, seq)
					}
				}
			}
		})
	}
}

func TestJetStreamAckReplyStreamPending(t *testing.T) {
	msc := StreamConfig{
		Name:      "MY_WQ",
		Subjects:  []string{"foo.*"},
		Storage:   MemoryStorage,
		MaxAge:    1 * time.Second,
		Retention: WorkQueuePolicy,
	}
	fsc := msc
	fsc.Storage = FileStorage

	cases := []struct {
		name    string
		mconfig *StreamConfig
	}{
		{"MemoryStore", &msc},
		{"FileStore", &fsc},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			s := RunBasicJetStreamServer(t)
			defer s.Shutdown()

			mset, err := s.GlobalAccount().addStream(c.mconfig)
			if err != nil {
				t.Fatalf("Unexpected error adding stream: %v", err)
			}
			defer mset.delete()

			nc := clientConnectToServer(t, s)
			defer nc.Close()

			// Now load up some messages.
			toSend := 100
			for i := 0; i < toSend; i++ {
				sendStreamMsg(t, nc, "foo.1", "Hello World!")
			}
			nc.Flush()

			state := mset.state()
			if state.Msgs != uint64(toSend) {
				t.Fatalf("Expected %d messages, got %d", toSend, state.Msgs)
			}

			o, err := mset.addConsumer(&ConsumerConfig{Durable: "PBO", AckPolicy: AckExplicit})
			require_NoError(t, err)
			defer o.delete()

			expectPending := func(ep int) {
				t.Helper()
				// Now check consumer info.
				checkFor(t, time.Second, 10*time.Millisecond, func() error {
					if info, pep := o.info(), ep+1; int(info.NumPending) != pep {
						return fmt.Errorf("Expected consumer info pending of %d, got %d", pep, info.NumPending)
					}
					return nil
				})
				m, err := nc.Request(o.requestNextMsgSubject(), nil, time.Second)
				if err != nil {
					t.Fatalf("Unexpected error: %v", err)
				}
				_, _, _, _, pending := ackReplyInfo(m.Reply)
				if pending != uint64(ep) {
					t.Fatalf("Expected ack reply pending of %d, got %d - reply: %q", ep, pending, m.Reply)
				}
			}

			expectPending(toSend - 1)
			// Send some more while we are connected.
			for i := 0; i < toSend; i++ {
				sendStreamMsg(t, nc, "foo.1", "Hello World!")
			}
			nc.Flush()

			expectPending(toSend*2 - 2)
			// Purge and send a new one.
			mset.purge(nil)
			nc.Flush()

			sendStreamMsg(t, nc, "foo.1", "Hello World!")
			expectPending(0)
			for i := 0; i < toSend; i++ {
				sendStreamMsg(t, nc, "foo.22", "Hello World!")
			}
			expectPending(toSend - 1) // 201
			// Test that delete will not register for consumed messages.
			mset.removeMsg(mset.state().FirstSeq)
			expectPending(toSend - 2) // 202
			// Now remove one that has not been delivered.
			mset.removeMsg(250)
			expectPending(toSend - 4) // 203

			// Test Expiration.
			mset.purge(nil)
			for i := 0; i < toSend; i++ {
				sendStreamMsg(t, nc, "foo.1", "Hello World!")
			}
			nc.Flush()

			// Wait for expiration to kick in.
			checkFor(t, 5*time.Second, time.Second, func() error {
				if state := mset.state(); state.Msgs != 0 {
					return fmt.Errorf("Stream still has messages")
				}
				return nil
			})
			sendStreamMsg(t, nc, "foo.33", "Hello World!")
			expectPending(0)

			// Now do filtered consumers.
			o.delete()
			o, err = mset.addConsumer(&ConsumerConfig{Durable: "PBO-FILTERED", AckPolicy: AckExplicit, FilterSubject: "foo.22"})
			require_NoError(t, err)
			defer o.delete()

			for i := 0; i < toSend; i++ {
				sendStreamMsg(t, nc, "foo.33", "Hello World!")
			}
			nc.Flush()

			if info := o.info(); info.NumPending != 0 {
				t.Fatalf("Expected no pending, got %d", info.NumPending)
			}
			// Now send one message that will match us.
			sendStreamMsg(t, nc, "foo.22", "Hello World!")
			expectPending(0)
			sendStreamMsg(t, nc, "foo.22", "Hello World!") // 504
			sendStreamMsg(t, nc, "foo.22", "Hello World!") // 505
			sendStreamMsg(t, nc, "foo.22", "Hello World!") // 506
			sendStreamMsg(t, nc, "foo.22", "Hello World!") // 507
			expectPending(3)
			mset.removeMsg(506)
			expectPending(1)
			for i := 0; i < toSend; i++ {
				sendStreamMsg(t, nc, "foo.22", "Hello World!")
			}
			nc.Flush()
			expectPending(100)
			mset.purge(nil)
			sendStreamMsg(t, nc, "foo.22", "Hello World!")
			expectPending(0)
		})
	}
}

func TestJetStreamAckReplyStreamPendingWithAcks(t *testing.T) {
	msc := StreamConfig{
		Name:     "MY_STREAM",
		Subjects: []string{"foo", "bar", "baz"},
		Storage:  MemoryStorage,
	}
	fsc := msc
	fsc.Storage = FileStorage

	cases := []struct {
		name    string
		mconfig *StreamConfig
	}{
		{"MemoryStore", &msc},
		{"FileStore", &fsc},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			s := RunBasicJetStreamServer(t)
			defer s.Shutdown()

			mset, err := s.GlobalAccount().addStream(c.mconfig)
			if err != nil {
				t.Fatalf("Unexpected error adding stream: %v", err)
			}
			defer mset.delete()

			nc := clientConnectToServer(t, s)
			defer nc.Close()

			// Now load up some messages.
			toSend := 500
			for i := 0; i < toSend; i++ {
				sendStreamMsg(t, nc, "foo", "Hello Foo!")
				sendStreamMsg(t, nc, "bar", "Hello Bar!")
				sendStreamMsg(t, nc, "baz", "Hello Baz!")
			}
			state := mset.state()
			if state.Msgs != uint64(toSend*3) {
				t.Fatalf("Expected %d messages, got %d", toSend*3, state.Msgs)
			}
			dsubj := "_d_"
			o, err := mset.addConsumer(&ConsumerConfig{
				Durable:        "D-1",
				AckPolicy:      AckExplicit,
				FilterSubject:  "foo",
				DeliverSubject: dsubj,
			})
			require_NoError(t, err)
			defer o.delete()

			if info := o.info(); int(info.NumPending) != toSend {
				t.Fatalf("Expected consumer info pending of %d, got %d", toSend, info.NumPending)
			}

			sub, _ := nc.SubscribeSync(dsubj)
			defer sub.Unsubscribe()

			checkFor(t, 500*time.Millisecond, 10*time.Millisecond, func() error {
				if nmsgs, _, _ := sub.Pending(); err != nil || nmsgs != toSend {
					return fmt.Errorf("Did not receive correct number of messages: %d vs %d", nmsgs, toSend)
				}
				return nil
			})

			// Should be zero.
			if info := o.info(); int(info.NumPending) != 0 {
				t.Fatalf("Expected consumer info pending of %d, got %d", 0, info.NumPending)
			} else if info.NumAckPending != toSend {
				t.Fatalf("Expected %d to be pending acks, got %d", toSend, info.NumAckPending)
			}
		})
	}
}

func TestJetStreamWorkQueueAckWaitRedelivery(t *testing.T) {
	cases := []struct {
		name    string
		mconfig *StreamConfig
	}{
		{"MemoryStore", &StreamConfig{Name: "MY_WQ", Storage: MemoryStorage, Retention: WorkQueuePolicy}},
		{"FileStore", &StreamConfig{Name: "MY_WQ", Storage: FileStorage, Retention: WorkQueuePolicy}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			s := RunBasicJetStreamServer(t)
			defer s.Shutdown()

			mset, err := s.GlobalAccount().addStream(c.mconfig)
			if err != nil {
				t.Fatalf("Unexpected error adding stream: %v", err)
			}
			defer mset.delete()

			nc := clientConnectToServer(t, s)
			defer nc.Close()

			// Now load up some messages.
			toSend := 100
			for i := 0; i < toSend; i++ {
				sendStreamMsg(t, nc, c.mconfig.Name, "Hello World!")
			}
			state := mset.state()
			if state.Msgs != uint64(toSend) {
				t.Fatalf("Expected %d messages, got %d", toSend, state.Msgs)
			}

			ackWait := 100 * time.Millisecond

			o, err := mset.addConsumer(&ConsumerConfig{Durable: "PBO", AckPolicy: AckExplicit, AckWait: ackWait})
			require_NoError(t, err)
			defer o.delete()

			sub, _ := nc.SubscribeSync(nats.NewInbox())
			defer sub.Unsubscribe()

			reqNextMsgSubj := o.requestNextMsgSubject()

			// Consume all the messages. But do not ack.
			for i := 0; i < toSend; i++ {
				nc.PublishRequest(reqNextMsgSubj, sub.Subject, nil)
				if _, err := sub.NextMsg(time.Second); err != nil {
					t.Fatalf("Unexpected error waiting for messages: %v", err)
				}
			}

			if nmsgs, _, _ := sub.Pending(); err != nil || nmsgs != 0 {
				t.Fatalf("Did not consume all messages, still have %d", nmsgs)
			}

			// All messages should still be there.
			state = mset.state()
			if int(state.Msgs) != toSend {
				t.Fatalf("Expected %d messages, got %d", toSend, state.Msgs)
			}

			// Now consume and ack.
			for i := 1; i <= toSend; i++ {
				nc.PublishRequest(reqNextMsgSubj, sub.Subject, nil)
				m, err := sub.NextMsg(time.Second)
				if err != nil {
					t.Fatalf("Unexpected error waiting for message[%d]: %v", i, err)
				}
				sseq, dseq, dcount, _, _ := ackReplyInfo(m.Reply)
				if sseq != uint64(i) {
					t.Fatalf("Expected set sequence of %d , got %d", i, sseq)
				}
				// Delivery sequences should always increase.
				if dseq != uint64(toSend+i) {
					t.Fatalf("Expected delivery sequence of %d , got %d", toSend+i, dseq)
				}
				if dcount == 1 {
					t.Fatalf("Expected these to be marked as redelivered")
				}
				// Ack the message here.
				m.AckSync()
			}

			if nmsgs, _, _ := sub.Pending(); err != nil || nmsgs != 0 {
				t.Fatalf("Did not consume all messages, still have %d", nmsgs)
			}

			// Flush acks
			nc.Flush()

			// Now check the mset as well, since we have a WorkQueue retention policy this should be empty.
			if state := mset.state(); state.Msgs != 0 {
				t.Fatalf("Expected no messages, got %d", state.Msgs)
			}
		})
	}
}

func TestJetStreamWorkQueueNakRedelivery(t *testing.T) {
	cases := []struct {
		name    string
		mconfig *StreamConfig
	}{
		{"MemoryStore", &StreamConfig{Name: "MY_WQ", Storage: MemoryStorage, Retention: WorkQueuePolicy}},
		{"FileStore", &StreamConfig{Name: "MY_WQ", Storage: FileStorage, Retention: WorkQueuePolicy}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			s := RunBasicJetStreamServer(t)
			defer s.Shutdown()

			mset, err := s.GlobalAccount().addStream(c.mconfig)
			if err != nil {
				t.Fatalf("Unexpected error adding stream: %v", err)
			}
			defer mset.delete()

			nc := clientConnectToServer(t, s)
			defer nc.Close()

			// Now load up some messages.
			toSend := 10
			for i := 0; i < toSend; i++ {
				sendStreamMsg(t, nc, c.mconfig.Name, "Hello World!")
			}
			state := mset.state()
			if state.Msgs != uint64(toSend) {
				t.Fatalf("Expected %d messages, got %d", toSend, state.Msgs)
			}

			o, err := mset.addConsumer(&ConsumerConfig{Durable: "PBO", AckPolicy: AckExplicit})
			require_NoError(t, err)
			defer o.delete()

			getMsg := func(sseq, dseq int) *nats.Msg {
				t.Helper()
				m, err := nc.Request(o.requestNextMsgSubject(), nil, time.Second)
				if err != nil {
					t.Fatalf("Unexpected error: %v", err)
				}
				rsseq, rdseq, _, _, _ := ackReplyInfo(m.Reply)
				if rdseq != uint64(dseq) {
					t.Fatalf("Expected delivered sequence of %d , got %d", dseq, rdseq)
				}
				if rsseq != uint64(sseq) {
					t.Fatalf("Expected store sequence of %d , got %d", sseq, rsseq)
				}
				return m
			}

			for i := 1; i <= 5; i++ {
				m := getMsg(i, i)
				// Ack the message here.
				m.Respond(nil)
			}

			// Grab #6
			m := getMsg(6, 6)
			// NAK this one and make sure its processed.
			m.Respond(AckNak)
			nc.Flush()

			// When we request again should be store sequence 6 again.
			getMsg(6, 7)
			// Then we should get 7, 8, etc.
			getMsg(7, 8)
			getMsg(8, 9)
		})
	}
}

func TestJetStreamWorkQueueWorkingIndicator(t *testing.T) {
	cases := []struct {
		name    string
		mconfig *StreamConfig
	}{
		{"MemoryStore", &StreamConfig{Name: "MY_WQ", Storage: MemoryStorage, Retention: WorkQueuePolicy}},
		{"FileStore", &StreamConfig{Name: "MY_WQ", Storage: FileStorage, Retention: WorkQueuePolicy}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			s := RunBasicJetStreamServer(t)
			defer s.Shutdown()

			mset, err := s.GlobalAccount().addStream(c.mconfig)
			if err != nil {
				t.Fatalf("Unexpected error adding stream: %v", err)
			}
			defer mset.delete()

			nc := clientConnectToServer(t, s)
			defer nc.Close()

			// Now load up some messages.
			toSend := 2
			for i := 0; i < toSend; i++ {
				sendStreamMsg(t, nc, c.mconfig.Name, "Hello World!")
			}
			state := mset.state()
			if state.Msgs != uint64(toSend) {
				t.Fatalf("Expected %d messages, got %d", toSend, state.Msgs)
			}

			ackWait := 100 * time.Millisecond

			o, err := mset.addConsumer(&ConsumerConfig{Durable: "PBO", AckPolicy: AckExplicit, AckWait: ackWait})
			require_NoError(t, err)
			defer o.delete()

			getMsg := func(sseq, dseq int) *nats.Msg {
				t.Helper()
				m, err := nc.Request(o.requestNextMsgSubject(), nil, time.Second)
				if err != nil {
					t.Fatalf("Unexpected error: %v", err)
				}
				rsseq, rdseq, _, _, _ := ackReplyInfo(m.Reply)
				if rdseq != uint64(dseq) {
					t.Fatalf("Expected delivered sequence of %d , got %d", dseq, rdseq)
				}
				if rsseq != uint64(sseq) {
					t.Fatalf("Expected store sequence of %d , got %d", sseq, rsseq)
				}
				return m
			}

			getMsg(1, 1)
			// Now wait past ackWait
			time.Sleep(ackWait * 2)

			// We should get 1 back.
			m := getMsg(1, 2)

			// Now let's take longer than ackWait to process but signal we are working on the message.
			timeout := time.Now().Add(3 * ackWait)
			for time.Now().Before(timeout) {
				m.Respond(AckProgress)
				nc.Flush()
				time.Sleep(ackWait / 5)
			}
			// We should get 2 here, not 1 since we have indicated we are working on it.
			m2 := getMsg(2, 3)
			time.Sleep(ackWait / 2)
			m2.Respond(AckProgress)

			// Now should get 1 back then 2.
			m = getMsg(1, 4)
			m.Respond(nil)
			getMsg(2, 5)
		})
	}
}

func TestJetStreamWorkQueueTerminateDelivery(t *testing.T) {
	cases := []struct {
		name    string
		mconfig *StreamConfig
	}{
		{"MemoryStore", &StreamConfig{Name: "MY_WQ", Storage: MemoryStorage, Retention: WorkQueuePolicy}},
		{"FileStore", &StreamConfig{Name: "MY_WQ", Storage: FileStorage, Retention: WorkQueuePolicy}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			s := RunBasicJetStreamServer(t)
			defer s.Shutdown()

			mset, err := s.GlobalAccount().addStream(c.mconfig)
			if err != nil {
				t.Fatalf("Unexpected error adding stream: %v", err)
			}
			defer mset.delete()

			nc := clientConnectToServer(t, s)
			defer nc.Close()

			// Now load up some messages.
			toSend := 22
			for i := 0; i < toSend; i++ {
				sendStreamMsg(t, nc, c.mconfig.Name, "Hello World!")
			}
			state := mset.state()
			if state.Msgs != uint64(toSend) {
				t.Fatalf("Expected %d messages, got %d", toSend, state.Msgs)
			}

			ackWait := 25 * time.Millisecond

			o, err := mset.addConsumer(&ConsumerConfig{Durable: "PBO", AckPolicy: AckExplicit, AckWait: ackWait})
			require_NoError(t, err)
			defer o.delete()

			getMsg := func(sseq, dseq int) *nats.Msg {
				t.Helper()
				m, err := nc.Request(o.requestNextMsgSubject(), nil, time.Second)
				if err != nil {
					t.Fatalf("Unexpected error: %v", err)
				}
				rsseq, rdseq, _, _, _ := ackReplyInfo(m.Reply)
				if rdseq != uint64(dseq) {
					t.Fatalf("Expected delivered sequence of %d , got %d", dseq, rdseq)
				}
				if rsseq != uint64(sseq) {
					t.Fatalf("Expected store sequence of %d , got %d", sseq, rsseq)
				}
				return m
			}

			// Make sure we get the correct advisory
			sub, _ := nc.SubscribeSync(JSAdvisoryConsumerMsgTerminatedPre + ".>")
			defer sub.Unsubscribe()

			getMsg(1, 1)
			// Now wait past ackWait
			time.Sleep(ackWait * 2)

			// We should get 1 back.
			m := getMsg(1, 2)
			// Now terminate
			m.Respond([]byte(fmt.Sprintf("%s with reason", string(AckTerm))))
			time.Sleep(ackWait * 2)

			// We should get 2 here, not 1 since we have indicated we wanted to terminate.
			getMsg(2, 3)

			// Check advisory was delivered.
			am, err := sub.NextMsg(time.Second)
			if err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}
			var adv JSConsumerDeliveryTerminatedAdvisory
			json.Unmarshal(am.Data, &adv)
			if adv.Stream != "MY_WQ" {
				t.Fatalf("Expected stream of %s, got %s", "MY_WQ", adv.Stream)
			}
			if adv.Consumer != "PBO" {
				t.Fatalf("Expected consumer of %s, got %s", "PBO", adv.Consumer)
			}
			if adv.StreamSeq != 1 {
				t.Fatalf("Expected stream sequence of %d, got %d", 1, adv.StreamSeq)
			}
			if adv.ConsumerSeq != 2 {
				t.Fatalf("Expected consumer sequence of %d, got %d", 2, adv.ConsumerSeq)
			}
			if adv.Deliveries != 2 {
				t.Fatalf("Expected delivery count of %d, got %d", 2, adv.Deliveries)
			}
			if adv.Reason != "with reason" {
				t.Fatalf("Advisory did not have a reason")
			}
		})
	}
}

func TestJetStreamAckNext(t *testing.T) {
	s := RunBasicJetStreamServer(t)
	defer s.Shutdown()

	mname := "ACKNXT"
	mset, err := s.GlobalAccount().addStream(&StreamConfig{Name: mname, Storage: MemoryStorage})
	if err != nil {
		t.Fatalf("Unexpected error adding stream: %v", err)
	}
	defer mset.delete()

	o, err := mset.addConsumer(&ConsumerConfig{Durable: "worker", AckPolicy: AckExplicit})
	if err != nil {
		t.Fatalf("Expected no error with registered interest, got %v", err)
	}
	defer o.delete()

	nc := clientConnectToServer(t, s)
	defer nc.Close()

	for i := 0; i < 12; i++ {
		sendStreamMsg(t, nc, mname, fmt.Sprintf("msg %d", i))
	}

	q := make(chan *nats.Msg, 10)
	sub, err := nc.ChanSubscribe(nats.NewInbox(), q)
	if err != nil {
		t.Fatalf("SubscribeSync failed: %s", err)
	}

	nc.PublishRequest(o.requestNextMsgSubject(), sub.Subject, []byte("1"))

	// normal next should imply 1
	msg := <-q
	err = msg.RespondMsg(&nats.Msg{Reply: sub.Subject, Subject: msg.Reply, Data: AckNext})
	if err != nil {
		t.Fatalf("RespondMsg failed: %s", err)
	}

	// read 1 message and check ack was done etc
	msg = <-q
	if len(q) != 0 {
		t.Fatalf("Expected empty q got %d", len(q))
	}
	if o.info().AckFloor.Stream != 1 {
		t.Fatalf("First message was not acknowledged")
	}
	if !bytes.Equal(msg.Data, []byte("msg 1")) {
		t.Fatalf("wrong message received, expected: msg 1 got %q", msg.Data)
	}

	// now ack and request 5 more using a naked number
	err = msg.RespondMsg(&nats.Msg{Reply: sub.Subject, Subject: msg.Reply, Data: append(AckNext, []byte(" 5")...)})
	if err != nil {
		t.Fatalf("RespondMsg failed: %s", err)
	}

	getMsgs := func(start, count int) {
		t.Helper()

		ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
		defer cancel()

		for i := start; i < count+1; i++ {
			select {
			case msg := <-q:
				expect := fmt.Sprintf("msg %d", i+1)
				if !bytes.Equal(msg.Data, []byte(expect)) {
					t.Fatalf("wrong message received, expected: %s got %#v", expect, msg)
				}
			case <-ctx.Done():
				t.Fatalf("did not receive all messages")
			}
		}

	}

	getMsgs(1, 5)

	// now ack and request 5 more using the full request
	err = msg.RespondMsg(&nats.Msg{Reply: sub.Subject, Subject: msg.Reply, Data: append(AckNext, []byte(`{"batch": 5}`)...)})
	if err != nil {
		t.Fatalf("RespondMsg failed: %s", err)
	}

	getMsgs(6, 10)

	if o.info().AckFloor.Stream != 2 {
		t.Fatalf("second message was not acknowledged")
	}
}

func TestJetStreamPublishDeDupe(t *testing.T) {
	s := RunBasicJetStreamServer(t)
	defer s.Shutdown()

	mname := "DeDupe"
	mset, err := s.GlobalAccount().addStream(&StreamConfig{Name: mname, Storage: FileStorage, MaxAge: time.Hour, Subjects: []string{"foo.*"}})
	if err != nil {
		t.Fatalf("Unexpected error adding stream: %v", err)
	}
	defer mset.delete()

	// Check Duplicates setting.
	duplicates := mset.config().Duplicates
	if duplicates != StreamDefaultDuplicatesWindow {
		t.Fatalf("Expected a default of %v, got %v", StreamDefaultDuplicatesWindow, duplicates)
	}

	cfg := mset.config()
	// Make sure can't be negative.
	cfg.Duplicates = -25 * time.Millisecond
	if err := mset.update(&cfg); err == nil {
		t.Fatalf("Expected an error but got none")
	}
	// Make sure can't be longer than age if its set.
	cfg.Duplicates = 2 * time.Hour
	if err := mset.update(&cfg); err == nil {
		t.Fatalf("Expected an error but got none")
	}

	nc := clientConnectToServer(t, s)
	defer nc.Close()

	sendMsg := func(seq uint64, id, msg string) *PubAck {
		t.Helper()
		m := nats.NewMsg(fmt.Sprintf("foo.%d", seq))
		m.Header.Add(JSMsgId, id)
		m.Data = []byte(msg)
		resp, _ := nc.RequestMsg(m, 100*time.Millisecond)
		if resp == nil {
			t.Fatalf("No response for %q, possible timeout?", msg)
		}
		pa := getPubAckResponse(resp.Data)
		if pa == nil || pa.Error != nil {
			t.Fatalf("Expected a JetStreamPubAck, got %q", resp.Data)
		}
		if pa.Sequence != seq {
			t.Fatalf("Did not get correct sequence in PubAck, expected %d, got %d", seq, pa.Sequence)
		}
		return pa.PubAck
	}

	expect := func(n uint64) {
		t.Helper()
		state := mset.state()
		if state.Msgs != n {
			t.Fatalf("Expected %d messages, got %d", n, state.Msgs)
		}
	}

	sendMsg(1, "AA", "Hello DeDupe!")
	sendMsg(2, "BB", "Hello DeDupe!")
	sendMsg(3, "CC", "Hello DeDupe!")
	sendMsg(4, "ZZ", "Hello DeDupe!")
	expect(4)

	sendMsg(1, "AA", "Hello DeDupe!")
	sendMsg(2, "BB", "Hello DeDupe!")
	sendMsg(4, "ZZ", "Hello DeDupe!")
	expect(4)

	cfg = mset.config()
	cfg.Duplicates = 100 * time.Millisecond
	if err := mset.update(&cfg); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	nmids := func(expected int) {
		t.Helper()
		checkFor(t, 200*time.Millisecond, 10*time.Millisecond, func() error {
			if nids := mset.numMsgIds(); nids != expected {
				return fmt.Errorf("Expected %d message ids, got %d", expected, nids)
			}
			return nil
		})
	}

	nmids(4)
	time.Sleep(cfg.Duplicates * 2)

	sendMsg(5, "AAA", "Hello DeDupe!")
	sendMsg(6, "BBB", "Hello DeDupe!")
	sendMsg(7, "CCC", "Hello DeDupe!")
	sendMsg(8, "DDD", "Hello DeDupe!")
	sendMsg(9, "ZZZ", "Hello DeDupe!")
	nmids(5)
	// Eventually will drop to zero.
	nmids(0)

	// Now test server restart
	cfg.Duplicates = 30 * time.Minute
	if err := mset.update(&cfg); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	mset.purge(nil)

	// Send 5 new messages.
	sendMsg(10, "AAAA", "Hello DeDupe!")
	sendMsg(11, "BBBB", "Hello DeDupe!")
	sendMsg(12, "CCCC", "Hello DeDupe!")
	sendMsg(13, "DDDD", "Hello DeDupe!")
	sendMsg(14, "EEEE", "Hello DeDupe!")

	// Stop current
	sd := s.JetStreamConfig().StoreDir
	s.Shutdown()
	// Restart.
	s = RunJetStreamServerOnPort(-1, sd)
	defer s.Shutdown()

	nc = clientConnectToServer(t, s)
	defer nc.Close()

	mset, _ = s.GlobalAccount().lookupStream(mname)
	if nms := mset.state().Msgs; nms != 5 {
		t.Fatalf("Expected 5 restored messages, got %d", nms)
	}
	nmids(5)

	// Send same and make sure duplicate detection still works.
	// Send 5 duplicate messages.
	sendMsg(10, "AAAA", "Hello DeDupe!")
	sendMsg(11, "BBBB", "Hello DeDupe!")
	sendMsg(12, "CCCC", "Hello DeDupe!")
	sendMsg(13, "DDDD", "Hello DeDupe!")
	sendMsg(14, "EEEE", "Hello DeDupe!")

	if nms := mset.state().Msgs; nms != 5 {
		t.Fatalf("Expected 5 restored messages, got %d", nms)
	}
	nmids(5)

	// Check we set duplicate properly.
	pa := sendMsg(10, "AAAA", "Hello DeDupe!")
	if !pa.Duplicate {
		t.Fatalf("Expected duplicate to be set")
	}

	// Purge should NOT wipe the msgIds. They should still persist.
	mset.purge(nil)
	nmids(5)
}

func getPubAckResponse(msg []byte) *JSPubAckResponse {
	var par JSPubAckResponse
	if err := json.Unmarshal(msg, &par); err != nil {
		return nil
	}
	return &par
}

func TestJetStreamPublishExpect(t *testing.T) {
	s := RunBasicJetStreamServer(t)
	defer s.Shutdown()

	mname := "EXPECT"
	mset, err := s.GlobalAccount().addStream(&StreamConfig{Name: mname, Storage: FileStorage, MaxAge: time.Hour, Subjects: []string{"foo.*"}})
	if err != nil {
		t.Fatalf("Unexpected error adding stream: %v", err)
	}
	defer mset.delete()

	nc := clientConnectToServer(t, s)
	defer nc.Close()

	// Test that we get no error when expected stream is correct.
	m := nats.NewMsg("foo.bar")
	m.Data = []byte("HELLO")
	m.Header.Set(JSExpectedStream, mname)
	resp, err := nc.RequestMsg(m, 100*time.Millisecond)
	require_NoError(t, err)
	if pa := getPubAckResponse(resp.Data); pa == nil || pa.Error != nil {
		t.Fatalf("Expected a valid JetStreamPubAck, got %q", resp.Data)
	}

	// Now test that we get an error back when expecting a different stream.
	m.Header.Set(JSExpectedStream, "ORDERS")
	resp, err = nc.RequestMsg(m, 100*time.Millisecond)
	require_NoError(t, err)
	if pa := getPubAckResponse(resp.Data); pa == nil || pa.Error == nil {
		t.Fatalf("Expected an error, got %q", resp.Data)
	}

	// Now test that we get an error back when expecting a different sequence number.
	m.Header.Set(JSExpectedStream, mname)
	m.Header.Set(JSExpectedLastSeq, "10")
	resp, err = nc.RequestMsg(m, 100*time.Millisecond)
	require_NoError(t, err)
	if pa := getPubAckResponse(resp.Data); pa == nil || pa.Error == nil {
		t.Fatalf("Expected an error, got %q", resp.Data)
	}

	// Or if we expect that there are no messages by setting "0" as the expected last seq
	m.Header.Set(JSExpectedLastSeq, "0")
	resp, err = nc.RequestMsg(m, 100*time.Millisecond)
	require_NoError(t, err)
	if pa := getPubAckResponse(resp.Data); pa == nil || pa.Error == nil {
		t.Fatalf("Expected an error, got %q", resp.Data)
	}

	// Now send a message with a message ID and make sure we can match that.
	m = nats.NewMsg("foo.bar")
	m.Data = []byte("HELLO")
	m.Header.Set(JSMsgId, "AAA")
	if _, err = nc.RequestMsg(m, 100*time.Millisecond); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	// Now try again with new message ID but require last one to be 'BBB'
	m.Header.Set(JSMsgId, "ZZZ")
	m.Header.Set(JSExpectedLastMsgId, "BBB")
	resp, err = nc.RequestMsg(m, 100*time.Millisecond)
	require_NoError(t, err)
	if pa := getPubAckResponse(resp.Data); pa == nil || pa.Error == nil {
		t.Fatalf("Expected an error, got %q", resp.Data)
	}

	// Restart the server and make sure we remember/rebuild last seq and last msgId.
	// Stop current
	sd := s.JetStreamConfig().StoreDir
	s.Shutdown()
	// Restart.
	s = RunJetStreamServerOnPort(-1, sd)
	defer s.Shutdown()

	nc = clientConnectToServer(t, s)
	defer nc.Close()

	// Our last sequence was 2 and last msgId was "AAA"
	m = nats.NewMsg("foo.baz")
	m.Data = []byte("HELLO AGAIN")
	m.Header.Set(JSExpectedLastSeq, "2")
	m.Header.Set(JSExpectedLastMsgId, "AAA")
	m.Header.Set(JSMsgId, "BBB")
	resp, err = nc.RequestMsg(m, 100*time.Millisecond)
	require_NoError(t, err)
	if pa := getPubAckResponse(resp.Data); pa == nil || pa.Error != nil {
		t.Fatalf("Expected a valid JetStreamPubAck, got %q", resp.Data)
	}
}

func TestJetStreamRestoreLastMsgIDPastDedupeWindow(t *testing.T) {
	s := RunBasicJetStreamServer(t)
	defer s.Shutdown()

	const (
		streamName = "TEST"
		subject    = "foo"
		lastMsgID  = "A"
	)
	duplicateWindow := 100 * time.Millisecond

	nc, js := jsClientConnect(t, s)
	defer nc.Close()

	_, err := js.AddStream(&nats.StreamConfig{
		Name:       streamName,
		Subjects:   []string{subject},
		Storage:    nats.FileStorage,
		Duplicates: duplicateWindow,
	})
	require_NoError(t, err)

	pa, err := js.Publish(subject, nil, nats.MsgId(lastMsgID))
	require_NoError(t, err)
	require_Equal(t, pa.Sequence, 1)

	mset, err := s.GlobalAccount().lookupStream(streamName)
	require_NoError(t, err)
	checkFor(t, time.Second, 10*time.Millisecond, func() error {
		if seq := mset.store.GetSeqFromTime(time.Now().Add(-duplicateWindow)); seq <= pa.Sequence {
			return fmt.Errorf("last message still inside duplicate window at sequence %d", seq)
		}
		return nil
	})

	sd := s.JetStreamConfig().StoreDir
	nc.Close()
	s.Shutdown()
	s.WaitForShutdown()

	s = RunJetStreamServerOnPort(-1, sd)
	defer s.Shutdown()
	nc, js = jsClientConnect(t, s)
	defer nc.Close()

	msg := nats.NewMsg(subject)
	msg.Header.Set(JSExpectedLastMsgId, lastMsgID)
	pa, err = js.PublishMsg(msg)
	require_NoError(t, err)
	require_Equal(t, pa.Sequence, 2)
}

func TestJetStreamRedeliveryAfterServerRestart(t *testing.T) {
	s := RunBasicJetStreamServer(t)
	defer s.Shutdown()

	sendSubj := "MYQ"
	mset, err := s.GlobalAccount().addStream(&StreamConfig{Name: sendSubj, Storage: FileStorage})
	if err != nil {
		t.Fatalf("Unexpected error adding stream: %v", err)
	}
	defer mset.delete()

	nc := clientConnectToServer(t, s)
	defer nc.Close()

	// Now load up some messages.
	toSend := 25
	for i := 0; i < toSend; i++ {
		sendStreamMsg(t, nc, sendSubj, "Hello World!")
	}
	state := mset.state()
	if state.Msgs != uint64(toSend) {
		t.Fatalf("Expected %d messages, got %d", toSend, state.Msgs)
	}

	sub, _ := nc.SubscribeSync(nats.NewInbox())
	defer sub.Unsubscribe()
	nc.Flush()

	o, err := mset.addConsumer(&ConsumerConfig{
		Durable:        "TO",
		DeliverSubject: sub.Subject,
		AckPolicy:      AckExplicit,
		AckWait:        100 * time.Millisecond,
	})
	require_NoError(t, err)
	defer o.delete()

	checkFor(t, 250*time.Millisecond, 10*time.Millisecond, func() error {
		if nmsgs, _, _ := sub.Pending(); err != nil || nmsgs != toSend {
			return fmt.Errorf("Did not receive correct number of messages: %d vs %d", nmsgs, toSend)
		}
		return nil
	})

	// Capture port since it was dynamic.
	u, _ := url.Parse(s.ClientURL())
	port, _ := strconv.Atoi(u.Port())
	sd := s.JetStreamConfig().StoreDir

	// Stop current
	s.Shutdown()

	// Restart.
	s = RunJetStreamServerOnPort(port, sd)
	defer s.Shutdown()

	// Don't wait for reconnect from old client.
	nc = clientConnectToServer(t, s)
	defer nc.Close()

	sub, _ = nc.SubscribeSync(sub.Subject)
	defer sub.Unsubscribe()

	checkFor(t, time.Second, 50*time.Millisecond, func() error {
		if nmsgs, _, _ := sub.Pending(); err != nil || nmsgs != toSend {
			return fmt.Errorf("Did not receive correct number of messages: %d vs %d", nmsgs, toSend)
		}
		return nil
	})
}

func TestJetStreamUsageNoReservation(t *testing.T) {
	test := func(t *testing.T, replicas int) {
		var s *Server
		if replicas == 1 {
			s = RunBasicJetStreamServer(t)
			defer s.Shutdown()
		} else {
			c := createJetStreamClusterExplicit(t, "R3S", replicas)
			defer c.shutdown()
			s = c.randomServer()
		}

		nc, js := jsClientConnect(t, s)
		defer nc.Close()

		_, err := js.AddStream(&nats.StreamConfig{Name: "FILE", Storage: nats.FileStorage, Replicas: replicas})
		require_NoError(t, err)
		_, err = js.AddStream(&nats.StreamConfig{Name: "MEM", Storage: nats.MemoryStorage, Replicas: replicas})
		require_NoError(t, err)

		acc := s.globalAccount()
		checkFor(t, 2*time.Second, 200*time.Millisecond, func() error {
			if streams := acc.numStreams(); streams != 2 {
				return fmt.Errorf("not at 2 streams yet, got %d", streams)
			}
			return nil
		})

		stats := acc.JetStreamUsage()
		require_Equal(t, stats.ReservedStore, 0)
		require_Equal(t, stats.ReservedMemory, 0)
	}

	t.Run("R1", func(t *testing.T) { test(t, 1) })
	t.Run("R3", func(t *testing.T) { test(t, 3) })
}

func TestJetStreamUsageReservationNegativeMaxBytes(t *testing.T) {
	test := func(t *testing.T, replicas int) {
		var s *Server
		if replicas == 1 {
			s = RunBasicJetStreamServer(t)
			defer s.Shutdown()
		} else {
			c := createJetStreamClusterExplicit(t, "R3S", replicas)
			defer c.shutdown()
			s = c.randomServer()
		}

		nc, js := jsClientConnect(t, s)
		defer nc.Close()

		fileCfg := &nats.StreamConfig{Name: "FILE", Storage: nats.FileStorage, Replicas: replicas, MaxBytes: 1024}
		memCfg := &nats.StreamConfig{Name: "MEM", Storage: nats.MemoryStorage, Replicas: replicas, MaxBytes: 1024}

		_, err := js.AddStream(fileCfg)
		require_NoError(t, err)
		_, err = js.AddStream(memCfg)
		require_NoError(t, err)

		acc := s.globalAccount()
		checkFor(t, 2*time.Second, 200*time.Millisecond, func() error {
			if streams := acc.numStreams(); streams != 2 {
				return fmt.Errorf("not at 2 streams yet, got %d", streams)
			}
			return nil
		})

		validateReserved := func(total uint64) {
			t.Helper()
			checkFor(t, 2*time.Second, 200*time.Millisecond, func() error {
				stats := acc.JetStreamUsage()
				if stats.ReservedMemory != total {
					return fmt.Errorf("acc stats: expected %d, got %d", total, stats.ReservedMemory)
				}
				if stats.ReservedStore != total {
					return fmt.Errorf("acc stats: expected %d, got %d", total, stats.ReservedStore)
				}
				jstats := s.getJetStream().usageStats()
				if jstats.ReservedMemory != total {
					return fmt.Errorf("server stats: expected %d, got %d", total, jstats.ReservedMemory)
				}
				if jstats.ReservedStore != total {
					return fmt.Errorf("server stats: expected %d, got %d", total, jstats.ReservedStore)
				}
				return nil
			})
		}
		validateReserved(1024)

		// Resetting back to zero.
		fileCfg.MaxBytes, memCfg.MaxBytes = 0, 0
		_, err = js.UpdateStream(fileCfg)
		require_NoError(t, err)
		_, err = js.UpdateStream(memCfg)
		require_NoError(t, err)
		validateReserved(0)

		// Update to reset back again.
		fileCfg.MaxBytes, memCfg.MaxBytes = 512, 512
		_, err = js.UpdateStream(fileCfg)
		require_NoError(t, err)
		_, err = js.UpdateStream(memCfg)
		require_NoError(t, err)
		validateReserved(512)

		// Resetting back to -1 should mean zero as well, and should do proper accounting.
		fileCfg.MaxBytes, memCfg.MaxBytes = -1, -1
		_, err = js.UpdateStream(fileCfg)
		require_NoError(t, err)
		_, err = js.UpdateStream(memCfg)
		require_NoError(t, err)
		validateReserved(0)
	}

	t.Run("R1", func(t *testing.T) { test(t, 1) })
	t.Run("R3", func(t *testing.T) { test(t, 3) })
}

func TestJetStreamCheckBytesLimitsOverflow(t *testing.T) {
	s := RunBasicJetStreamServer(t)
	defer s.Shutdown()

	js := s.getJetStream()
	limits := &JetStreamAccountLimits{
		MaxStore:  1024,
		MaxMemory: 1024,
	}

	// addBytes + maxBytesOffset overflow.
	// In the snapshot-restore path, maxBytesOffset (bc) accumulates from
	// hdr.HeaderSize + hdr.PayloadSize and can be non-zero when addBytes is near MaxInt64.
	// Previously this silently wrapped negative, bypassing the limit.
	js.mu.RLock()
	err := js.checkBytesLimits(limits, _EMPTY_, math.MaxInt64, 1, FileStorage, false, 0, 1)
	js.mu.RUnlock()
	require_Error(t, err)

	js.mu.RLock()
	err = js.checkBytesLimits(limits, _EMPTY_, math.MaxInt64, 1, MemoryStorage, false, 0, 1)
	js.mu.RUnlock()
	require_Error(t, err)

	// Server-level storeReserved + totalBytes overflow.
	origStore := js.storeReserved
	js.mu.Lock()
	js.storeReserved = math.MaxInt64 - 1
	js.mu.Unlock()

	js.mu.RLock()
	err = js.checkBytesLimits(limits, _EMPTY_, 2, 1, FileStorage, true, 0, 0)
	js.mu.RUnlock()
	require_Error(t, err)

	js.mu.Lock()
	js.storeReserved = origStore
	js.mu.Unlock()

	// Server-level memReserved + totalBytes overflow.
	origMem := js.memReserved
	js.mu.Lock()
	js.memReserved = math.MaxInt64 - 1
	js.mu.Unlock()

	js.mu.RLock()
	err = js.checkBytesLimits(limits, _EMPTY_, 2, 1, MemoryStorage, true, 0, 0)
	js.mu.RUnlock()
	require_Error(t, err)

	js.mu.Lock()
	js.memReserved = origMem
	js.mu.Unlock()
}

func TestJetStreamAccountReservation(t *testing.T) {
	const maxInt64 = int64(math.MaxInt64)
	for _, test := range []struct {
		name     string
		tier     string
		replicas int
		bytes    int64
		expected int64
	}{
		// Non-positive bytes never reserve anything, regardless of tier/replicas.
		{"zero bytes", _EMPTY_, 3, 0, 0},
		{"negative bytes untiered", _EMPTY_, 3, -100, 0},
		{"negative bytes tiered", "R3", 3, -1, 0},
		// Un-tiered (flat) account limit: an R>1 stream counts as Replicas*bytes.
		{"untiered R1", _EMPTY_, 1, 100, 100},
		{"untiered R0 treated as single", _EMPTY_, 0, 100, 100},
		{"untiered R3", _EMPTY_, 3, 100, 300},
		{"untiered R5", _EMPTY_, 5, 100, 500},
		// Tiered account limit: replication is baked into the tier, counted as-is.
		{"tiered R1", "R1", 1, 100, 100},
		{"tiered R3", "R3", 3, 100, 100},
		// Replicas*bytes saturates at MaxInt64 instead of overflowing.
		{"untiered R3 saturates", _EMPTY_, 3, maxInt64, maxInt64},
		{"untiered R2 saturates", _EMPTY_, 2, maxInt64/2 + 1, maxInt64},
		// Tiered does not multiply, so large bytes pass through untouched.
		{"tiered large no multiply", "R3", 3, maxInt64, maxInt64},
	} {
		t.Run(test.name, func(t *testing.T) {
			if got := accountReservation(test.tier, test.replicas, test.bytes); got != test.expected {
				t.Fatalf("accountReservation(%q, %d, %d) = %d, want %d",
					test.tier, test.replicas, test.bytes, got, test.expected)
			}
		})
	}
}

func TestJetStreamSnapshots(t *testing.T) {
	s := RunBasicJetStreamServer(t)
	defer s.Shutdown()

	mname := "MY-STREAM"
	subjects := []string{"foo", "bar", "baz"}
	cfg := StreamConfig{
		Name:     mname,
		Storage:  FileStorage,
		Subjects: subjects,
		MaxMsgs:  1000,
	}

	acc := s.GlobalAccount()
	mset, err := acc.addStream(&cfg)
	if err != nil {
		t.Fatalf("Unexpected error adding stream: %v", err)
	}

	nc := clientConnectToServer(t, s)
	defer nc.Close()

	// Make sure we send some as floor.
	toSend := rand.IntN(200) + 22
	for i := 1; i <= toSend; i++ {
		msg := fmt.Sprintf("Hello World %d", i)
		subj := subjects[rand.IntN(len(subjects))]
		sendStreamMsg(t, nc, subj, msg)
	}

	// Create up to 10 consumers.
	numConsumers := rand.IntN(10) + 1
	var obs []obsi
	for i := 1; i <= numConsumers; i++ {
		cname := fmt.Sprintf("WQ-%d", i)
		ocfg := workerModeConfig(cname)
		ocfg.MemoryStorage = i == 1
		o, err := mset.addConsumer(ocfg)
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		// Now grab some messages.
		toReceive := rand.IntN(toSend/2) + 1
		for r := 0; r < toReceive; r++ {
			resp, err := nc.Request(o.requestNextMsgSubject(), nil, time.Second)
			if err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}
			if resp != nil {
				resp.Respond(nil)
			}
		}
		obs = append(obs, obsi{o.config(), toReceive})
	}
	nc.Flush()

	// Snapshot state of the stream and consumers.
	sc, ss, snapshot := performStreamBackup(t, nc, "MY-STREAM")

	// Try to restore from snapshot with current stream present, should error.
	require_False(t, performStreamRestore(t, nc, sc, ss, snapshot))

	// Now delete so we can restore.
	pusage := acc.JetStreamUsage()
	mset.delete()

	require_True(t, performStreamRestore(t, nc, sc, ss, snapshot))

	mset, err = acc.lookupStream("MY-STREAM")
	require_NoError(t, err)
	// Now compare to make sure they are equal.
	// Have to ignore API calls here, as we performed an API call to restore the stream.
	nusage := acc.JetStreamUsage()
	pusage.API, nusage.API = JetStreamAPIStats{}, JetStreamAPIStats{}
	if !reflect.DeepEqual(nusage, pusage) {
		t.Fatalf("Usage does not match after restore: %+v vs %+v", nusage, pusage)
	}
	if state := mset.state(); !reflect.DeepEqual(state, ss) {
		t.Fatalf("State does not match: %+v vs %+v", state, ss)
	}
	if cfg := mset.config(); !reflect.DeepEqual(cfg, sc) {
		t.Fatalf("Configs do not match: %+v vs %+v", cfg, sc)
	}
	// Consumers.
	if mset.numConsumers() != ss.Consumers {
		t.Fatalf("Number of consumers do not match: %d vs %d", mset.numConsumers(), ss.Consumers)
	}
	for _, oi := range obs {
		if o := mset.lookupConsumer(oi.cfg.Durable); o != nil {
			if uint64(oi.ack+1) != o.nextSeq() {
				t.Fatalf("[%v] Consumer next seq is not correct: %d vs %d", o.String(), oi.ack+1, o.nextSeq())
			}
			if oi.cfg.MemoryStorage {
				require_Equal(t, o.store.GetConfig().Name, oi.cfg.Durable)
			}
		} else {
			t.Fatalf("Expected to get an consumer")
		}
	}

	// Now try restoring to a different
	s2 := RunBasicJetStreamServer(t)
	defer s2.Shutdown()

	nc2 := clientConnectToServer(t, s2)
	defer nc2.Close()

	require_True(t, performStreamRestore(t, nc2, sc, ss, snapshot))

	mset, err = acc.lookupStream("MY-STREAM")
	require_NoError(t, err)

	o := mset.lookupConsumer("WQ-1")
	if o == nil {
		t.Fatalf("Could not lookup consumer")
	}

	// Make sure we can read messages.
	if _, err := nc2.Request(o.requestNextMsgSubject(), nil, 5*time.Second); err != nil {
		t.Fatalf("Unexpected error getting next message: %v", err)
	}
}

func TestJetStreamRestoreV2RejectsExistingStream(t *testing.T) {
	for _, storage := range []StorageType{MemoryStorage, FileStorage} {
		for _, restoring := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/restoring=%t", storage, restoring), func(t *testing.T) {
				s := RunBasicJetStreamServer(t)
				defer s.Shutdown()
				acc := s.GlobalAccount()
				cfg := StreamConfig{
					Name:     "TEST",
					Subjects: []string{"foo"},
					Storage:  storage,
				}
				var mset *stream
				var err error
				if restoring {
					mset, err = acc.addStreamForRestore(&cfg)
				} else {
					mset, err = acc.addStream(&cfg)
				}
				require_NoError(t, err)
				require_NoError(t, mset.store.StoreRawMsg("foo", nil, []byte("original"), 1, time.Now().UnixNano(), 0, false))
				state := mset.store.State()
				usage := acc.JetStreamUsage()

				// Exercise the creation check directly: another restore may have
				// passed its initial existence check before this stream was created.
				// Matching configurations must not allow it to reuse the stream,
				// whether the first restore is still running or has already finished.
				other, err := acc.addStreamForRestore(&cfg)
				require_True(t, IsNatsErr(err, JSStreamNameExistRestoreFailedErr))
				require_True(t, other == nil)

				current, err := acc.lookupStream(cfg.Name)
				require_NoError(t, err)
				require_True(t, current == mset)
				require_True(t, reflect.DeepEqual(mset.store.State(), state))
				require_True(t, reflect.DeepEqual(acc.JetStreamUsage(), usage))
				msg, err := mset.store.LoadMsg(1, nil)
				require_NoError(t, err)
				require_Equal(t, string(msg.msg), "original")
				mset.mu.RLock()
				stillRestoring := mset.restoring
				mset.mu.RUnlock()
				require_Equal(t, stillRestoring, restoring)
			})
		}
	}
}

func TestJetStreamSnapshotV2ClampsConsumerStateToStream(t *testing.T) {
	s := RunBasicJetStreamServer(t)
	defer s.Shutdown()

	mset, err := s.GlobalAccount().addStream(&StreamConfig{
		Name:     "TEST",
		Subjects: []string{"foo"},
		Storage:  MemoryStorage,
	})
	require_NoError(t, err)
	defer mset.delete()

	require_NoError(t, mset.store.StoreRawMsg("foo", nil, []byte("one"), 1, time.Now().UnixNano(), 0, false))

	o, err := mset.addConsumer(&ConsumerConfig{
		Name:      "C",
		Durable:   "C",
		AckPolicy: AckExplicit,
	})
	require_NoError(t, err)

	var streamState StreamState
	mset.store.FastState(&streamState)
	require_Equal(t, streamState.LastSeq, uint64(1))

	o.mu.Lock()
	err = o.setStoreState(&ConsumerState{
		Delivered: SequencePair{Consumer: 3, Stream: 3},
		AckFloor:  SequencePair{Consumer: 2, Stream: 2},
		Pending: map[uint64]*Pending{
			3: {Sequence: 3, Timestamp: time.Now().UnixNano()},
		},
		Redelivered: map[uint64]uint64{1: 2, 3: 2},
	})
	o.mu.Unlock()
	require_NoError(t, err)

	pr, pw := net.Pipe()
	defer pr.Close()
	errCh := make(chan error, 1)
	go mset.js.streamSnapshotV2(mset.store, &streamState, pw, true, nil, errCh)

	dec := s2.NewReader(pr)
	r := archive.NewReader(dec)

	hdr, err := r.Next()
	require_NoError(t, err)
	require_Equal(t, hdr.Name, "state.json")
	_, err = io.ReadAll(r)
	require_NoError(t, err)

	hdr, err = r.Next()
	require_NoError(t, err)
	require_Equal(t, hdr.Name, "consumers/C")
	buf, err := io.ReadAll(r)
	require_NoError(t, err)

	var snapshot SnapshotConsumerState
	require_NoError(t, json.Unmarshal(buf, &snapshot))
	require_Equal(t, snapshot.Delivered, SequencePair{Consumer: 3, Stream: 1})
	require_Equal(t, snapshot.AckFloor, SequencePair{Consumer: 2, Stream: 1})
	require_Equal(t, len(snapshot.Pending), 0)
	require_Equal(t, len(snapshot.Redelivered), 1)
	dc, ok := snapshot.Redelivered[1]
	require_True(t, ok)
	require_Equal(t, dc, uint64(2))

	_, err = io.Copy(io.Discard, dec)
	require_NoError(t, err)
	require_NoError(t, <-errCh)
}

func TestJetStreamSnapshotV2RestoreAtAccountReservationLimit(t *testing.T) {
	const restoreBytes = 1 << 30

	s := RunBasicJetStreamServer(t)
	defer s.Shutdown()

	require_NoError(t, s.GlobalAccount().UpdateJetStreamLimits(map[string]JetStreamAccountLimits{
		_EMPTY_: {
			MaxMemory:    -1,
			MaxStore:     restoreBytes,
			MaxStreams:   -1,
			MaxConsumers: -1,
		},
	}))

	var snapshot bytes.Buffer
	enc := s2.NewWriter(&snapshot)
	w := archive.NewWriter(enc)
	writeEntry := func(hdr *archive.Header, payload []byte) {
		t.Helper()
		require_NoError(t, w.WriteHeader(hdr))
		if len(payload) > 0 {
			_, err := w.Write(payload)
			require_NoError(t, err)
		}
	}

	state, err := json.Marshal(&StreamState{
		Msgs:     1,
		Bytes:    restoreBytes,
		FirstSeq: 1,
		LastSeq:  1,
	})
	require_NoError(t, err)
	writeEntry(&archive.Header{Name: "state.json", PayloadSize: int64(len(state))}, state)

	msg := []byte("snapshot")
	writeEntry(&archive.Header{
		Name:        "foo",
		Sequence:    1,
		Timestamp:   time.Now().UnixNano(),
		PayloadSize: int64(len(msg)),
	}, msg)
	writeEntry(&archive.Header{}, nil)
	require_NoError(t, w.Close())
	require_NoError(t, enc.Close())

	mset, err := s.GlobalAccount().RestoreStreamV2(&StreamConfig{
		Name:     "TEST",
		Subjects: []string{"foo"},
		Storage:  FileStorage,
		MaxBytes: restoreBytes,
	}, bytes.NewReader(snapshot.Bytes()))
	require_NoError(t, err)
	defer mset.delete()
	require_Equal(t, mset.store.State().Msgs, uint64(1))

	js := s.getJetStream()
	js.mu.RLock()
	reserved := js.storeReserved
	js.mu.RUnlock()
	require_Equal(t, reserved, int64(restoreBytes))
}

func TestJetStreamRestoreV2DefersSchedulesUntilReady(t *testing.T) {
	for _, storage := range []StorageType{MemoryStorage, FileStorage} {
		t.Run(storage.String(), func(t *testing.T) {
			s := RunBasicJetStreamServer(t)
			defer s.Shutdown()

			mset, err := s.GlobalAccount().addStreamForRestore(&StreamConfig{
				Name:              "TEST",
				Subjects:          []string{"foo.>"},
				Storage:           storage,
				AllowMsgSchedules: true,
			})
			require_NoError(t, err)
			defer mset.delete()

			due := time.Now().Add(100 * time.Millisecond)
			hdr := genHeader(nil, JSSchedulePattern, fmt.Sprintf("@at %s", due.Format(time.RFC3339Nano)))
			hdr = genHeader(hdr, JSScheduleTarget, "foo.target")
			require_NoError(t, mset.store.StoreRawMsg("foo.schedule", hdr, []byte("scheduled"), 1, time.Now().UnixNano(), 0, false))

			time.Sleep(500 * time.Millisecond)

			var restoring, paused, timerSet, inflight bool
			switch store := mset.store.(type) {
			case *memStore:
				store.mu.RLock()
				restoring = store.recovering
				paused = store.scheduling.paused
				timerSet = store.scheduling.timer != nil
				_, inflight = store.scheduling.inflight["foo.schedule"]
				store.mu.RUnlock()
			case *fileStore:
				store.mu.RLock()
				restoring = store.recovering
				paused = store.scheduling.paused
				timerSet = store.scheduling.timer != nil
				_, inflight = store.scheduling.inflight["foo.schedule"]
				store.mu.RUnlock()
			}
			require_True(t, restoring)
			require_True(t, paused)
			require_False(t, timerSet)
			require_False(t, inflight)

			mset.mu.RLock()
			active := mset.active
			mset.mu.RUnlock()
			require_False(t, active)

			require_NoError(t, mset.completeRestore())
			checkFor(t, 2*time.Second, 10*time.Millisecond, func() error {
				sm, err := mset.store.LoadMsg(2, nil)
				if err != nil {
					return err
				}
				if sm.subj != "foo.target" {
					return fmt.Errorf("unexpected scheduled message subject %q", sm.subj)
				}
				return nil
			})
		})
	}
}

func TestJetStreamSnapshotV2MemoryEphemeralConsumerRemainsEphemeral(t *testing.T) {
	s := RunBasicJetStreamServer(t)
	defer s.Shutdown()

	nc := clientConnectToServer(t, s)
	defer nc.Close()
	_, err := nc.Subscribe("deliver", func(*nats.Msg) {})
	require_NoError(t, err)
	require_NoError(t, nc.Flush())

	mset, err := s.GlobalAccount().addStream(&StreamConfig{
		Name:     "TEST",
		Subjects: []string{"foo"},
		Storage:  MemoryStorage,
	})
	require_NoError(t, err)

	_, err = mset.addConsumer(&ConsumerConfig{
		Name:           "E",
		DeliverSubject: "deliver",
		AckPolicy:      AckExplicit,
	})
	require_NoError(t, err)

	assertEphemeral := func(mset *stream) {
		t.Helper()
		o := mset.lookupConsumer("E")
		if o == nil {
			t.Fatal("ephemeral consumer not restored")
		}
		require_Equal(t, o.config().Durable, _EMPTY_)
		require_Equal(t, o.store.GetConfig().Durable, _EMPTY_)
	}

	for range 2 {
		sc, ss, snapshot := performStreamBackup(t, nc, "TEST")
		mset.delete()
		require_True(t, performStreamRestore(t, nc, sc, ss, snapshot))
		mset, err = s.GlobalAccount().lookupStream("TEST")
		require_NoError(t, err)
		assertEphemeral(mset)
	}
}

func TestJetStreamRestoreV2MalformedConsumerStateCompletesConsumer(t *testing.T) {
	s := RunBasicJetStreamServer(t)
	defer s.Shutdown()

	nc := clientConnectToServer(t, s)
	defer nc.Close()
	sub, err := nc.SubscribeSync("deliver")
	require_NoError(t, err)
	require_NoError(t, nc.Flush())

	var snapshot bytes.Buffer
	enc := s2.NewWriter(&snapshot)
	w := archive.NewWriter(enc)
	writeEntry := func(name string, value any) {
		t.Helper()
		payload, err := json.Marshal(value)
		require_NoError(t, err)
		require_NoError(t, w.WriteHeader(&archive.Header{Name: name, PayloadSize: int64(len(payload))}))
		_, err = w.Write(payload)
		require_NoError(t, err)
	}

	writeEntry("state.json", &StreamState{FirstSeq: 1, Consumers: 1})
	writeEntry("consumers/E", &SnapshotConsumerState{
		ConsumerConfig: &ConsumerConfig{
			Name:           "E",
			DeliverSubject: "deliver",
			AckPolicy:      AckExplicit,
		},
		ConsumerState: &ConsumerState{
			Delivered: SequencePair{Consumer: 1, Stream: 1},
			AckFloor:  SequencePair{Consumer: 2, Stream: 1},
		},
	})
	require_NoError(t, w.Close())
	require_NoError(t, enc.Close())

	_, err = s.GlobalAccount().RestoreStreamV2(&StreamConfig{
		Name:     "TEST",
		Subjects: []string{"foo"},
		Storage:  MemoryStorage,
	}, bytes.NewReader(snapshot.Bytes()))
	require_Error(t, err)
	if !strings.Contains(err.Error(), "failed to set consumer \"E\" state") {
		t.Fatalf("unexpected restore error: %v", err)
	}

	mset, err := s.GlobalAccount().lookupStream("TEST")
	require_NoError(t, err)
	o := mset.lookupConsumer("E")
	if o == nil {
		t.Fatal("partial restore did not retain consumer")
	}
	if !o.isLeader() {
		t.Fatal("retained consumer was not activated")
	}
	require_Equal(t, o.config().Durable, _EMPTY_)
	require_Equal(t, o.store.GetConfig().Durable, _EMPTY_)

	require_NoError(t, nc.Publish("foo", []byte("OK")))
	require_NoError(t, nc.Flush())
	_, err = sub.NextMsg(time.Second)
	require_NoError(t, err)
}

func TestJetStreamRestoreV2StagesStreamIngressAndSources(t *testing.T) {
	s := RunBasicJetStreamServer(t)
	defer s.Shutdown()

	nc := clientConnectToServer(t, s)
	defer nc.Close()

	state, err := json.Marshal(&StreamState{FirstSeq: 1, LastSeq: 1})
	require_NoError(t, err)

	pr, pw := io.Pipe()
	stateWritten := make(chan struct{})
	continueRestore := make(chan struct{})
	writerErr := make(chan error, 1)
	go func() {
		enc := s2.NewWriter(pw)
		w := archive.NewWriter(enc)
		writeEntry := func(hdr *archive.Header, payload []byte) error {
			if err := w.WriteHeader(hdr); err != nil {
				return err
			}
			if len(payload) > 0 {
				if _, err := w.Write(payload); err != nil {
					return err
				}
			}
			return w.Flush()
		}

		if err := writeEntry(&archive.Header{Name: "state.json", PayloadSize: int64(len(state))}, state); err != nil {
			pw.CloseWithError(err)
			writerErr <- err
			return
		}
		close(stateWritten)
		<-continueRestore

		msg := []byte("snapshot")
		if err := writeEntry(&archive.Header{Name: "foo", Sequence: 1, Timestamp: time.Now().UnixNano(), PayloadSize: int64(len(msg))}, msg); err != nil {
			pw.CloseWithError(err)
			writerErr <- err
			return
		}
		if err := writeEntry(&archive.Header{}, nil); err != nil {
			pw.CloseWithError(err)
			writerErr <- err
			return
		}
		if err := w.Close(); err != nil {
			pw.CloseWithError(err)
			writerErr <- err
			return
		}
		if err := enc.Close(); err != nil {
			pw.CloseWithError(err)
			writerErr <- err
			return
		}
		writerErr <- pw.Close()
	}()

	type restoreResult struct {
		mset *stream
		err  error
	}
	restoreDone := make(chan restoreResult, 1)
	go func() {
		mset, err := s.GlobalAccount().RestoreStreamV2(&StreamConfig{
			Name:     "TEST",
			Subjects: []string{"foo"},
			Storage:  MemoryStorage,
			Sources:  []*StreamSource{{Name: "MISSING"}},
		}, pr)
		restoreDone <- restoreResult{mset, err}
	}()

	select {
	case <-stateWritten:
	case err := <-writerErr:
		t.Fatalf("failed to start snapshot writer: %v", err)
	}

	var staged *stream
	checkFor(t, 2*time.Second, 10*time.Millisecond, func() error {
		var err error
		staged, err = s.GlobalAccount().lookupStream("TEST")
		return err
	})
	staged.mu.RLock()
	active, numSources := staged.active, len(staged.sources)
	staged.mu.RUnlock()
	require_False(t, active)
	require_Equal(t, numSources, 0)

	require_NoError(t, nc.Publish("foo", []byte("live during restore")))
	require_NoError(t, nc.Flush())
	require_Equal(t, staged.store.State().Msgs, uint64(0))

	close(continueRestore)
	result := <-restoreDone
	require_NoError(t, result.err)
	require_NoError(t, <-writerErr)
	require_Equal(t, result.mset.store.State().Msgs, uint64(1))
	require_Equal(t, result.mset.store.State().LastSeq, uint64(1))

	result.mset.mu.RLock()
	active, numSources = result.mset.active, len(result.mset.sources)
	result.mset.mu.RUnlock()
	require_True(t, active)
	require_Equal(t, numSources, 1)

	require_NoError(t, nc.Publish("foo", []byte("live after restore")))
	require_NoError(t, nc.Flush())
	checkFor(t, time.Second, 10*time.Millisecond, func() error {
		state := result.mset.store.State()
		if state.Msgs != 2 || state.LastSeq != 2 {
			return fmt.Errorf("unexpected stream state: %+v", state)
		}
		return nil
	})
}

func TestJetStreamRestoreV2StagesConsumerActivation(t *testing.T) {
	s := RunBasicJetStreamServer(t)
	defer s.Shutdown()

	mset, err := s.GlobalAccount().addStream(&StreamConfig{
		Name:      "TEST",
		Subjects:  []string{"foo"},
		Storage:   MemoryStorage,
		Retention: InterestPolicy,
	})
	require_NoError(t, err)
	defer mset.delete()

	nc := clientConnectToServer(t, s)
	defer nc.Close()
	sub, err := nc.SubscribeSync("deliver")
	require_NoError(t, err)
	require_NoError(t, nc.Flush())

	const ackWait = 25 * time.Millisecond
	o, err := mset.addConsumerForRestore(&ConsumerConfig{
		Name:           "ephemeral",
		Durable:        "ephemeral",
		DeliverSubject: "deliver",
		FilterSubject:  "foo",
		AckPolicy:      AckExplicit,
		AckWait:        ackWait,
	})
	require_NoError(t, err)
	if o.isLeader() {
		t.Fatal("consumer activated before restore completed")
	}

	o.mu.Lock()
	err = o.setStoreState(&ConsumerState{
		Delivered: SequencePair{Consumer: 1, Stream: 1},
		Pending: map[uint64]*Pending{
			1: {Sequence: 1, Timestamp: time.Now().Add(-time.Hour).UnixNano()},
		},
	})
	o.mu.Unlock()
	require_NoError(t, err)

	// If the consumer were active, the expired pending timer would remove this
	// entry because stream sequence 1 has not been restored yet.
	time.Sleep(10 * ackWait)
	state, err := o.store.State()
	require_NoError(t, err)
	if _, ok := state.Pending[1]; !ok {
		t.Fatal("pending state was processed while consumer restore was staged")
	}

	err = mset.store.StoreRawMsg("foo", nil, []byte("OK"), 1, time.Now().UnixNano(), 0, false)
	require_NoError(t, err)
	if _, err := sub.NextMsg(2 * ackWait); err == nil {
		t.Fatal("consumer delivered before restore completed")
	}

	o.switchToEphemeral()
	require_NoError(t, o.completeRestore())
	if !o.isLeader() {
		t.Fatal("consumer did not activate after restore completed")
	}
	if _, err := sub.NextMsg(time.Second); err != nil {
		t.Fatalf("pending message was not redelivered after restore completed: %v", err)
	}
}

func TestJetStreamSnapshotRejectsWildcardDeliverSubject(t *testing.T) {
	s := RunBasicJetStreamServer(t)
	defer s.Shutdown()

	nc, js := jsClientConnect(t, s)
	defer nc.Close()

	_, err := js.AddStream(&nats.StreamConfig{Name: "TEST"})
	require_NoError(t, err)

	for _, deliverSubject := range []string{"snap.*", "snap.>"} {
		req, err := json.Marshal(&JSApiStreamSnapshotRequest{DeliverSubject: deliverSubject})
		require_NoError(t, err)

		rmsg, err := nc.Request(fmt.Sprintf(JSApiStreamSnapshotT, "TEST"), req, time.Second)
		require_NoError(t, err)

		var resp JSApiStreamSnapshotResponse
		require_NoError(t, json.Unmarshal(rmsg.Data, &resp))
		if !IsNatsErr(resp.Error, JSSnapshotDeliverSubjectInvalidErr) {
			t.Fatalf("Expected wildcard deliver subject %q to be rejected, got %+v", deliverSubject, resp.Error)
		}
	}
}

func TestJetStreamSnapshotNotificationRegistrationError(t *testing.T) {
	acc := NewAccount("TEST")
	acc.sl = NewSublistWithCache()
	sr := &SnapshotResult{Reader: io.NopCloser(bytes.NewReader(nil))}
	req := &JSApiStreamSnapshotRequest{DeliverSubject: "snap.*"}

	// pass nil mset, registration failure returns before stream access
	err := (&Server{}).streamSnapshot(acc, nil, sr, req)
	if !errors.Is(err, ErrInvalidSubject) {
		t.Fatalf("Expected invalid subject error, got %v", err)
	}
}

func TestJetStreamSnapshotsAPI(t *testing.T) {
	lopts := DefaultTestOptions
	lopts.ServerName = "LS"
	lopts.Port = -1
	lopts.LeafNode.Host = lopts.Host
	lopts.LeafNode.Port = -1

	ls := RunServer(&lopts)
	defer ls.Shutdown()

	opts := DefaultTestOptions
	opts.ServerName = "S"
	opts.Port = -1
	tdir := t.TempDir()
	opts.JetStream = true
	opts.JetStreamDomain = "domain"
	opts.StoreDir = tdir
	maxStore := int64(1024 * 1024 * 1024)
	opts.maxStoreSet = true
	opts.JetStreamMaxStore = maxStore
	rurl, _ := url.Parse(fmt.Sprintf("nats-leaf://%s:%d", lopts.LeafNode.Host, lopts.LeafNode.Port))
	opts.LeafNode.Remotes = []*RemoteLeafOpts{{URLs: []*url.URL{rurl}}}

	s := RunServer(&opts)
	defer s.Shutdown()

	checkLeafNodeConnected(t, s)

	mname := "MY-STREAM"
	subjects := []string{"foo", "bar", "baz"}
	cfg := StreamConfig{
		Name:     mname,
		Storage:  FileStorage,
		Subjects: subjects,
		MaxMsgs:  1000,
	}

	acc := s.GlobalAccount()
	mset, err := acc.addStreamWithStore(&cfg, &FileStoreConfig{BlockSize: 128})
	if err != nil {
		t.Fatalf("Unexpected error adding stream: %v", err)
	}

	nc := clientConnectToServer(t, s)
	defer nc.Close()

	toSend := max(30, rand.IntN(100)+1)
	for i := 1; i <= toSend; i++ {
		msg := fmt.Sprintf("Hello World %d", i)
		subj := subjects[rand.IntN(len(subjects))]
		sendStreamMsg(t, nc, subj, msg)
	}

	// Introduce a gap to ensure we handle that correctly on restore.
	for n := range uint64(10) {
		removed, err := mset.removeMsg(10 + n)
		require_NoError(t, err)
		require_True(t, removed)
	}

	o, err := mset.addConsumer(workerModeConfig("WQ"))
	require_NoError(t, err)
	// Now grab some messages.
	toReceive := rand.IntN(int(mset.state().Msgs)) + 1
	for r := 0; r < toReceive; r++ {
		resp, err := nc.Request(o.requestNextMsgSubject(), nil, time.Second)
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		if resp != nil {
			resp.Respond(nil)
		}
	}

	// Make sure we get proper error for non-existent request, streams,etc,
	rmsg, err := nc.Request(fmt.Sprintf(JSApiStreamSnapshotT, "foo"), nil, time.Second)
	if err != nil {
		t.Fatalf("Unexpected error on snapshot request: %v", err)
	}
	var resp JSApiStreamSnapshotResponse
	json.Unmarshal(rmsg.Data, &resp)
	if resp.Error == nil || resp.Error.Code != 400 || resp.Error.Description != "bad request" {
		t.Fatalf("Did not get correct error response: %+v", resp.Error)
	}

	sreq := &JSApiStreamSnapshotRequest{}
	req, _ := json.Marshal(sreq)
	rmsg, err = nc.Request(fmt.Sprintf(JSApiStreamSnapshotT, "foo"), req, time.Second)
	if err != nil {
		t.Fatalf("Unexpected error on snapshot request: %v", err)
	}
	json.Unmarshal(rmsg.Data, &resp)
	if resp.Error == nil || resp.Error.Code != 404 || resp.Error.Description != "stream not found" {
		t.Fatalf("Did not get correct error response: %+v", resp.Error)
	}

	rmsg, err = nc.Request(fmt.Sprintf(JSApiStreamSnapshotT, mname), req, time.Second)
	if err != nil {
		t.Fatalf("Unexpected error on snapshot request: %v", err)
	}
	json.Unmarshal(rmsg.Data, &resp)
	if resp.Error == nil || resp.Error.Code != 400 || resp.Error.Description != "deliver subject not valid" {
		t.Fatalf("Did not get correct error response: %+v", resp.Error)
	}

	// Set delivery subject, do not subscribe yet. Want this to be an ok pattern.
	sreq.DeliverSubject = nats.NewInbox()
	// Just for test, usually left alone.
	sreq.ChunkSize = 1024
	req, _ = json.Marshal(sreq)
	rmsg, err = nc.Request(fmt.Sprintf(JSApiStreamSnapshotT, mname), req, time.Second)
	if err != nil {
		t.Fatalf("Unexpected error on snapshot request: %v", err)
	}
	resp.Error = nil
	json.Unmarshal(rmsg.Data, &resp)
	if resp.Error != nil {
		t.Fatalf("Did not get correct error response: %+v", resp.Error)
	}
	// Check that we have the config and the state.
	if resp.Config == nil {
		t.Fatalf("Expected a stream config in the response, got %+v\n", resp)
	}
	if resp.State == nil {
		t.Fatalf("Expected a stream state in the response, got %+v\n", resp)
	}

	// Grab state for comparison.
	state := *resp.State
	config := *resp.Config

	// Setup to process snapshot chunks.
	var snapshot []byte
	done := make(chan bool)

	sub, _ := nc.Subscribe(sreq.DeliverSubject, func(m *nats.Msg) {
		// EOF
		if len(m.Data) == 0 {
			done <- true
			return
		}
		// Could be writing to a file here too.
		snapshot = append(snapshot, m.Data...)
		// Flow ack
		m.Respond(nil)
	})
	defer sub.Unsubscribe()

	// Wait to receive the snapshot.
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatalf("Did not receive our snapshot in time")
	}

	// Now make sure this snapshot is legit.
	var rresp JSApiStreamRestoreResponse
	rreq := &JSApiStreamRestoreRequest{
		Config: config,
		State:  state,
	}
	req, _ = json.Marshal(rreq)

	// Make sure we get an error since stream still exists.
	rmsg, err = nc.Request(fmt.Sprintf(JSApiStreamRestoreT, mname), req, time.Second)
	if err != nil {
		t.Fatalf("Unexpected error on snapshot request: %v", err)
	}
	json.Unmarshal(rmsg.Data, &rresp)
	if !IsNatsErr(rresp.Error, JSStreamNameExistRestoreFailedErr) {
		t.Fatalf("Did not get correct error response: %+v", rresp.Error)
	}

	// Delete this stream.
	mset.delete()

	// Sending no request message will error now.
	rmsg, err = nc.Request(fmt.Sprintf(JSApiStreamRestoreT, mname), nil, time.Second)
	if err != nil {
		t.Fatalf("Unexpected error on snapshot request: %v", err)
	}
	// Make sure to clear.
	rresp.Error = nil
	json.Unmarshal(rmsg.Data, &rresp)
	if rresp.Error == nil || rresp.Error.Code != 400 || rresp.Error.Description != "bad request" {
		t.Fatalf("Did not get correct error response: %+v", rresp.Error)
	}

	// This should work.
	rmsg, err = nc.Request(fmt.Sprintf(JSApiStreamRestoreT, mname), req, time.Second)
	if err != nil {
		t.Fatalf("Unexpected error on snapshot request: %v", err)
	}
	// Make sure to clear.
	rresp.Error = nil
	json.Unmarshal(rmsg.Data, &rresp)
	if rresp.Error != nil {
		t.Fatalf("Got an unexpected error response: %+v", rresp.Error)
	}

	// Can be any size message.
	var chunk [512]byte
	for r := bytes.NewReader(snapshot); ; {
		n, err := r.Read(chunk[:])
		if err != nil {
			break
		}
		nc.Request(rresp.DeliverSubject, chunk[:n], time.Second)
	}
	nc.Request(rresp.DeliverSubject, nil, time.Second)

	mset, err = acc.lookupStream(mname)
	if err != nil {
		t.Fatalf("Expected to find a stream for %q", mname)
	}
	if !reflect.DeepEqual(mset.state(), state) {
		t.Fatalf("Did not match states, %+v vs %+v", mset.state(), state)
	}

	// Now ask that the stream be checked first.
	sreq.ChunkSize = 0
	sreq.CheckMsgs = true
	snapshot = snapshot[:0]

	req, _ = json.Marshal(sreq)
	if _, err = nc.Request(fmt.Sprintf(JSApiStreamSnapshotT, mname), req, 5*time.Second); err != nil {
		t.Fatalf("Unexpected error on snapshot request: %v", err)
	}
	// Wait to receive the snapshot.
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatalf("Did not receive our snapshot in time")
	}

	// Now connect through a cluster server and make sure we can get things to work this way as well.
	// This client, connecting to a leaf without shared system account and domain needs to provide the domain explicitly.
	nc2 := clientConnectToServer(t, ls)
	defer nc2.Close()
	// Wait a bit for interest to propagate.
	time.Sleep(100 * time.Millisecond)

	snapshot = snapshot[:0]

	req, _ = json.Marshal(sreq)
	rmsg, err = nc2.Request(fmt.Sprintf(strings.ReplaceAll(JSApiStreamSnapshotT, JSApiPrefix, "$JS.domain.API"), mname), req, time.Second)
	if err != nil {
		t.Fatalf("Unexpected error on snapshot request: %v", err)
	}
	resp.Error = nil
	json.Unmarshal(rmsg.Data, &resp)
	if resp.Error != nil {
		t.Fatalf("Did not get correct error response: %+v", resp.Error)
	}
	// Wait to receive the snapshot.
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatalf("Did not receive our snapshot in time")
	}

	// Now do a restore through the new client connection.
	// Delete this stream first.
	mset, err = acc.lookupStream(mname)
	if err != nil {
		t.Fatalf("Expected to find a stream for %q", mname)
	}
	state = mset.state()
	mset.delete()

	req, _ = json.Marshal(rreq)
	rmsg, err = nc2.Request(fmt.Sprintf(strings.ReplaceAll(JSApiStreamRestoreT, JSApiPrefix, "$JS.domain.API"), mname), req, time.Second)
	if err != nil {
		t.Fatalf("Unexpected error on snapshot request: %v", err)
	}
	// Make sure to clear.
	rresp.Error = nil
	json.Unmarshal(rmsg.Data, &rresp)
	if rresp.Error != nil {
		t.Fatalf("Got an unexpected error response: %+v", rresp.Error)
	}

	// Make sure when we send something without a reply subject the subscription is shutoff.
	r := bytes.NewReader(snapshot)
	n, _ := r.Read(chunk[:])
	nc2.Publish(rresp.DeliverSubject, chunk[:n])
	nc2.Flush()
	n, _ = r.Read(chunk[:])
	if _, err := nc2.Request(rresp.DeliverSubject, chunk[:n], 100*time.Millisecond); err == nil {
		t.Fatalf("Expected restore subscription to be closed")
	}

	req, _ = json.Marshal(rreq)
	rmsg, err = nc2.Request(fmt.Sprintf(strings.ReplaceAll(JSApiStreamRestoreT, JSApiPrefix, "$JS.domain.API"), mname), req, time.Second)
	if err != nil {
		t.Fatalf("Unexpected error on snapshot request: %v", err)
	}
	// Make sure to clear.
	rresp.Error = nil
	json.Unmarshal(rmsg.Data, &rresp)
	if rresp.Error != nil {
		t.Fatalf("Got an unexpected error response: %+v", rresp.Error)
	}

	for r := bytes.NewReader(snapshot); ; {
		n, err := r.Read(chunk[:])
		if err != nil {
			break
		}
		// Make sure other side responds to reply subjects for ack flow. Optional.
		if _, err := nc2.Request(rresp.DeliverSubject, chunk[:n], time.Second); err != nil {
			t.Fatalf("Restore not honoring reply subjects for ack flow")
		}
	}

	// For EOF this will send back stream info or an error.
	si, err := nc2.Request(rresp.DeliverSubject, nil, time.Second)
	if err != nil {
		t.Fatalf("Got an error restoring stream: %v", err)
	}
	var scResp JSApiStreamCreateResponse
	if err := json.Unmarshal(si.Data, &scResp); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if scResp.Error != nil {
		t.Fatalf("Got an unexpected error from EOF on restore: %+v", scResp.Error)
	}

	if !reflect.DeepEqual(scResp.StreamInfo.State, state) {
		t.Fatalf("Did not match states, %+v vs %+v", scResp.StreamInfo.State, state)
	}

	// Now make sure that if we try to change the name/identity of the stream we get an error.
	mset, err = acc.lookupStream(mname)
	if err != nil {
		t.Fatalf("Expected to find a stream for %q", mname)
	}
	mset.state()
	mset.delete()

	rreq.Config.Name = "NEW_STREAM"
	req, _ = json.Marshal(rreq)

	rmsg, err = nc2.Request(fmt.Sprintf(strings.ReplaceAll(JSApiStreamRestoreT, JSApiPrefix, "$JS.domain.API"), rreq.Config.Name), req, time.Second)
	if err != nil {
		t.Fatalf("Unexpected error on snapshot request: %v", err)
	}
	// Make sure to clear.
	rresp.Error = nil
	json.Unmarshal(rmsg.Data, &rresp)
	// We should not get an error here.
	if rresp.Error != nil {
		t.Fatalf("Got an unexpected error response: %+v", rresp.Error)
	}
	for r := bytes.NewReader(snapshot); ; {
		n, err := r.Read(chunk[:])
		if err != nil {
			break
		}
		nc2.Request(rresp.DeliverSubject, chunk[:n], time.Second)
	}

	si, err = nc2.Request(rresp.DeliverSubject, nil, time.Second)
	require_NoError(t, err)

	scResp.Error = nil
	if err := json.Unmarshal(si.Data, &scResp); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
}

func TestJetStreamSnapshotsAPIArchiveMessageLayout(t *testing.T) {
	var buf bytes.Buffer
	w := archive.NewWriter(&buf)

	hdr := []byte("NATS/1.0\r\nNats-Msg-Id: X\r\n\r\n")
	msg := []byte("hello world")
	subj := "foo.bar"
	body := append(hdr, msg...)
	entry := archive.Header{
		Name:        subj,
		Timestamp:   12345,
		Sequence:    99,
		HeaderSize:  int64(len(hdr)),
		PayloadSize: int64(len(msg)),
	}
	require_NoError(t, w.WriteHeader(&entry))
	_, err := w.Write(body)
	require_NoError(t, err)
	require_NoError(t, w.Close())

	r := archive.NewReader(bytes.NewReader(buf.Bytes()))
	gotHdr, err := r.Next()
	require_NoError(t, err)
	require_Equal(t, subj, gotHdr.Name)
	require_Equal(t, int64(len(hdr)), gotHdr.HeaderSize)
	require_Equal(t, int64(len(msg)), gotHdr.PayloadSize)
	require_Equal(t, uint64(99), gotHdr.Sequence)

	gotBody, err := io.ReadAll(r)
	require_NoError(t, err)
	require_True(t, bytes.Equal(body, gotBody))
}

func TestJetStreamSnapshotWithDeletedLastSeq(t *testing.T) {
	s := RunBasicJetStreamServer(t)
	defer s.Shutdown()

	nc, js := jsClientConnect(t, s)
	defer nc.Close()

	_, err := js.AddStream(&nats.StreamConfig{
		Name:     "TEST",
		Subjects: []string{"foo"},
		Storage:  nats.FileStorage,
	})
	require_NoError(t, err)

	for i := 0; i < 10; i++ {
		_, err := js.Publish("foo", []byte("hello"))
		require_NoError(t, err)
	}
	require_NoError(t, js.DeleteMsg("TEST", 10))

	sc, ss, snapshot := performStreamBackup(t, nc, "TEST")
	require_Equal(t, ss.LastSeq, uint64(10))
	require_Equal(t, ss.Msgs, uint64(9))

	require_NoError(t, js.DeleteStream("TEST"))
	require_True(t, performStreamRestore(t, nc, sc, ss, snapshot))

	si, err := js.StreamInfo("TEST")
	require_NoError(t, err)
	require_Equal(t, si.State.Msgs, uint64(9))
}

func TestJetStreamSnapshotFailedAdvisoryHasError(t *testing.T) {
	oldSnapshotAckTimeout := snapshotAckTimeout
	snapshotAckTimeout = 100 * time.Millisecond
	defer func() { snapshotAckTimeout = oldSnapshotAckTimeout }()

	s := RunBasicJetStreamServer(t)
	defer s.Shutdown()

	nc, js := jsClientConnect(t, s)
	defer nc.Close()

	_, err := js.AddStream(&nats.StreamConfig{
		Name:     "TEST",
		Subjects: []string{"foo"},
		Storage:  nats.FileStorage,
	})
	require_NoError(t, err)

	var x uint32 = 1
	payload := make([]byte, 4096)
	for range 20 {
		for i := range payload {
			x = x*1664525 + 1013904223
			payload[i] = byte(x >> 24)
		}
		_, err := js.Publish("foo", payload)
		require_NoError(t, err)
	}

	advSub := natsSubSync(t, nc, JSAdvisoryStreamSnapshotCompletePre+".TEST")
	deliver := nats.NewInbox()
	snapSub, err := nc.SubscribeSync(deliver)
	require_NoError(t, err)
	defer snapSub.Unsubscribe()

	req, err := json.Marshal(&JSApiStreamSnapshotRequest{
		DeliverSubject: deliver,
		ChunkSize:      1024,
		WindowSize:     1024,
	})
	require_NoError(t, err)
	respMsg, err := nc.Request(fmt.Sprintf(JSApiStreamSnapshotT, "TEST"), req, time.Second)
	require_NoError(t, err)

	var resp JSApiStreamSnapshotResponse
	require_NoError(t, json.Unmarshal(respMsg.Data, &resp))
	require_True(t, resp.Error == nil)

	msg, err := snapSub.NextMsg(time.Second)
	require_NoError(t, err)
	require_True(t, len(msg.Data) > 0)
	// Intentionally do not ack the chunk so the snapshot fails with no flow response.

	advMsg, err := advSub.NextMsg(5 * time.Second)
	require_NoError(t, err)

	var advisory struct {
		Error json.RawMessage `json:"error,omitempty"`
	}
	require_NoError(t, json.Unmarshal(advMsg.Data, &advisory))
	require_True(t, len(advisory.Error) > 0 && string(advisory.Error) != "null")
}

func TestJetStreamRestoreSkipsExpiredLastSeq(t *testing.T) {
	s := RunBasicJetStreamServer(t)
	defer s.Shutdown()

	nc, js := jsClientConnect(t, s)
	defer nc.Close()

	_, err := js.AddStream(&nats.StreamConfig{
		Name:        "TEST",
		Subjects:    []string{"foo"},
		Storage:     nats.FileStorage,
		AllowMsgTTL: true,
	})
	require_NoError(t, err)

	_, err = js.Publish("foo", []byte("live"))
	require_NoError(t, err)

	msg := nats.NewMsg("foo")
	msg.Header.Set(JSMessageTTL, "1s")
	msg.Data = []byte("expires")
	_, err = js.PublishMsg(msg)
	require_NoError(t, err)

	sc, ss, snapshot := performStreamBackup(t, nc, "TEST")
	require_Equal(t, ss.LastSeq, uint64(2))
	require_Equal(t, ss.Msgs, uint64(2))

	time.Sleep(2 * time.Second)

	require_NoError(t, js.DeleteStream("TEST"))
	require_True(t, performStreamRestore(t, nc, sc, ss, snapshot))

	si, err := js.StreamInfo("TEST")
	require_NoError(t, err)
	require_Equal(t, si.State.LastSeq, uint64(2))
	require_Equal(t, si.State.Msgs, uint64(1))

	pa, err := js.Publish("foo", []byte("next"))
	require_NoError(t, err)
	require_Equal(t, pa.Sequence, uint64(3))
}

func TestJetStreamSnapshotsAPIRestoreLimits(t *testing.T) {
	s := RunBasicJetStreamServer(t)
	defer s.Shutdown()

	nc, js := jsClientConnect(t, s)
	defer nc.Close()

	_, err := js.AddStream(&nats.StreamConfig{
		Name:     "TEST",
		Subjects: []string{"foo"},
	})
	require_NoError(t, err)

	for range 100 {
		_, err := js.Publish("foo", []byte("hello world"))
		require_NoError(t, err)
	}

	sc, ss, snapshot := performStreamBackup(t, nc, "TEST")

	sendRestore := func(t *testing.T, nc *nats.Conn) *JSApiStreamCreateResponse {
		t.Helper()
		endpoint := fmt.Sprintf(JSApiStreamRestoreT, sc.Name)

		req, err := json.Marshal(struct {
			StreamState  `json:"state"`
			StreamConfig `json:"config"`
		}{
			StreamState:  ss,
			StreamConfig: sc,
		})
		require_NoError(t, err)

		resp, err := nc.Request(endpoint, req, 5*time.Second)
		require_NoError(t, err)

		var rresp JSApiStreamRestoreResponse
		require_NoError(t, json.Unmarshal(resp.Data, &rresp))
		require_True(t, IsValidLiteralSubject(rresp.DeliverSubject))

		reader := bytes.NewReader(snapshot)
		buf := make([]byte, 128*1024)
		for {
			n, err := reader.Read(buf)
			if err == io.EOF {
				break
			}
			require_NoError(t, err)
			_, err = nc.Request(rresp.DeliverSubject, buf[:n], 5*time.Second)
			require_NoError(t, err)
		}

		done, err := nc.Request(rresp.DeliverSubject, nil, 5*time.Second)
		require_NoError(t, err)

		var cresp JSApiStreamCreateResponse
		require_NoError(t, json.Unmarshal(done.Data, &cresp))
		return &cresp
	}

	// Restore onto a server with tight account limits — should fail.
	t.Run("ExceedsLimits", func(t *testing.T) {
		ns := RunBasicJetStreamServer(t)
		defer ns.Shutdown()

		require_NoError(t, ns.GlobalAccount().UpdateJetStreamLimits(map[string]JetStreamAccountLimits{
			_EMPTY_: {
				MaxMemory:    1024,
				MaxStore:     1024,
				MaxStreams:   -1,
				MaxConsumers: -1,
			},
		}))

		nnc := clientConnectToServer(t, ns)
		defer nnc.Close()

		resp := sendRestore(t, nnc)
		require_NotNil(t, resp.Error)
		require_Equal(t, resp.Error.ErrCode, 10047) // JSStorageResourcesExceededErr
	})

	// Restore onto a server with plenty of room — should succeed.
	t.Run("WithinLimits", func(t *testing.T) {
		ns := RunBasicJetStreamServer(t)
		defer ns.Shutdown()

		nnc := clientConnectToServer(t, ns)
		defer nnc.Close()

		resp := sendRestore(t, nnc)
		require_True(t, resp.Error == nil)
	})
}

func TestJetStreamPubAckPerf(t *testing.T) {
	// Comment out to run, holding place for now.
	t.SkipNow()

	s := RunBasicJetStreamServer(t)
	defer s.Shutdown()

	nc, js := jsClientConnect(t, s)
	defer nc.Close()

	if _, err := js.AddStream(&nats.StreamConfig{Name: "foo", Storage: nats.MemoryStorage}); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	toSend := 1_000_000
	start := time.Now()
	for i := 0; i < toSend; i++ {
		js.PublishAsync("foo", []byte("OK"))
	}
	<-js.PublishAsyncComplete()
	tt := time.Since(start)
	fmt.Printf("time is %v\n", tt)
	fmt.Printf("%.0f msgs/sec\n", float64(toSend)/tt.Seconds())
}

func TestJetStreamPubPerfWithFullStream(t *testing.T) {
	// Comment out to run, holding place for now.
	t.SkipNow()

	s := RunBasicJetStreamServer(t)
	defer s.Shutdown()

	nc, js := jsClientConnect(t, s)
	defer nc.Close()

	toSend, msg := 1_000_000, []byte("OK")

	_, err := js.AddStream(&nats.StreamConfig{Name: "foo", MaxMsgs: int64(toSend)})
	require_NoError(t, err)

	start := time.Now()
	for i := 0; i < toSend; i++ {
		js.PublishAsync("foo", msg)
	}
	<-js.PublishAsyncComplete()
	tt := time.Since(start)
	fmt.Printf("time is %v\n", tt)
	fmt.Printf("%.0f msgs/sec\n", float64(toSend)/tt.Seconds())

	// Now do same amount but knowing we are at our limit.
	start = time.Now()
	for i := 0; i < toSend; i++ {
		js.PublishAsync("foo", msg)
	}
	<-js.PublishAsyncComplete()
	tt = time.Since(start)
	fmt.Printf("\ntime is %v\n", tt)
	fmt.Printf("%.0f msgs/sec\n", float64(toSend)/tt.Seconds())
}

func TestJetStreamSnapshotsAPIPerf(t *testing.T) {
	// Comment out to run, holding place for now.
	t.SkipNow()

	s := RunBasicJetStreamServer(t)
	defer s.Shutdown()

	cfg := StreamConfig{
		Name:    "snap-perf",
		Storage: FileStorage,
	}

	acc := s.GlobalAccount()
	if _, err := acc.addStream(&cfg); err != nil {
		t.Fatalf("Unexpected error adding stream: %v", err)
	}

	nc := clientConnectToServer(t, s)
	defer nc.Close()

	msg := make([]byte, 128*1024)
	// If you don't give gzip some data will spend too much time compressing everything to zero.
	crand.Read(msg)

	for i := 0; i < 10000; i++ {
		nc.Publish("snap-perf", msg)
	}
	nc.Flush()

	sreq := &JSApiStreamSnapshotRequest{DeliverSubject: nats.NewInbox()}
	req, _ := json.Marshal(sreq)
	rmsg, err := nc.Request(fmt.Sprintf(JSApiStreamSnapshotT, "snap-perf"), req, time.Second)
	if err != nil {
		t.Fatalf("Unexpected error on snapshot request: %v", err)
	}

	var resp JSApiStreamSnapshotResponse
	json.Unmarshal(rmsg.Data, &resp)
	if resp.Error != nil {
		t.Fatalf("Did not get correct error response: %+v", resp.Error)
	}

	done := make(chan bool)
	total := 0
	sub, _ := nc.Subscribe(sreq.DeliverSubject, func(m *nats.Msg) {
		// EOF
		if len(m.Data) == 0 {
			m.Sub.Unsubscribe()
			done <- true
			return
		}
		// We don't do anything with the snapshot, just take
		// note of the size.
		total += len(m.Data)
		// Flow ack
		m.Respond(nil)
	})
	defer sub.Unsubscribe()

	start := time.Now()
	// Wait to receive the snapshot.
	select {
	case <-done:
	case <-time.After(30 * time.Second):
		t.Fatalf("Did not receive our snapshot in time")
	}
	td := time.Since(start)
	fmt.Printf("Received %d bytes in %v\n", total, td)
	fmt.Printf("Rate %.0f MB/s\n", float64(total)/td.Seconds()/(1024*1024))
}

func TestJetStreamActiveDelivery(t *testing.T) {
	cases := []struct {
		name    string
		mconfig *StreamConfig
	}{
		{"MemoryStore", &StreamConfig{Name: "ADS", Storage: MemoryStorage, Subjects: []string{"foo.*"}}},
		{"FileStore", &StreamConfig{Name: "ADS", Storage: FileStorage, Subjects: []string{"foo.*"}}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			s := RunBasicJetStreamServer(t)
			defer s.Shutdown()

			mset, err := s.GlobalAccount().addStream(c.mconfig)
			if err != nil {
				t.Fatalf("Unexpected error adding stream: %v", err)
			}
			defer mset.delete()

			nc := clientConnectToServer(t, s)
			defer nc.Close()

			// Now load up some messages.
			toSend := 100
			sendSubj := "foo.22"
			for i := 0; i < toSend; i++ {
				sendStreamMsg(t, nc, sendSubj, "Hello World!")
			}
			state := mset.state()
			if state.Msgs != uint64(toSend) {
				t.Fatalf("Expected %d messages, got %d", toSend, state.Msgs)
			}

			o, err := mset.addConsumer(&ConsumerConfig{Durable: "to", DeliverSubject: "d"})
			if err != nil {
				t.Fatalf("Expected no error with registered interest, got %v", err)
			}
			defer o.delete()

			// We have no active interest above. So consumer will be considered inactive. Let's subscribe and make sure
			// we get the messages instantly. This will test that we hook interest activation correctly.
			sub, _ := nc.SubscribeSync("d")
			defer sub.Unsubscribe()
			nc.Flush()

			checkFor(t, 100*time.Millisecond, 10*time.Millisecond, func() error {
				if nmsgs, _, _ := sub.Pending(); err != nil || nmsgs != toSend {
					return fmt.Errorf("Did not receive correct number of messages: %d vs %d", nmsgs, toSend)
				}
				return nil
			})
		})
	}
}

func TestJetStreamEphemeralConsumers(t *testing.T) {
	cases := []struct {
		name    string
		mconfig *StreamConfig
	}{
		{"MemoryStore", &StreamConfig{Name: "EP", Storage: MemoryStorage, Subjects: []string{"foo.*"}}},
		{"FileStore", &StreamConfig{Name: "EP", Storage: FileStorage, Subjects: []string{"foo.*"}}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			s := RunBasicJetStreamServer(t)
			defer s.Shutdown()

			mset, err := s.GlobalAccount().addStream(c.mconfig)
			if err != nil {
				t.Fatalf("Unexpected error adding stream: %v", err)
			}
			defer mset.delete()

			nc := clientConnectToServer(t, s)
			defer nc.Close()

			sub, _ := nc.SubscribeSync(nats.NewInbox())
			defer sub.Unsubscribe()
			nc.Flush()

			o, err := mset.addConsumer(&ConsumerConfig{DeliverSubject: sub.Subject})
			if err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}
			if !o.isActive() {
				t.Fatalf("Expected the consumer to be considered active")
			}
			if numo := mset.numConsumers(); numo != 1 {
				t.Fatalf("Expected number of consumers to be 1, got %d", numo)
			}
			// Set our delete threshold to something low for testing purposes.
			o.setInActiveDeleteThreshold(100 * time.Millisecond)

			// Make sure works now.
			nc.Request("foo.22", nil, 100*time.Millisecond)
			checkFor(t, 250*time.Millisecond, 10*time.Millisecond, func() error {
				if nmsgs, _, _ := sub.Pending(); err != nil || nmsgs != 1 {
					return fmt.Errorf("Did not receive correct number of messages: %d vs %d", nmsgs, 1)
				}
				return nil
			})

			// Now close the subscription, this should trip active state on the ephemeral consumer.
			sub.Unsubscribe()
			checkFor(t, time.Second, 10*time.Millisecond, func() error {
				if o.isActive() {
					return fmt.Errorf("Expected the ephemeral consumer to be considered inactive")
				}
				return nil
			})
			// The reason for this still being 1 is that we give some time in case of a reconnect scenario.
			// We detect right away on the interest change but we wait for interest to be re-established.
			// This is in case server goes away but app is fine, we do not want to recycle those consumers.
			if numo := mset.numConsumers(); numo != 1 {
				t.Fatalf("Expected number of consumers to be 1, got %d", numo)
			}

			// We should delete this one after the delete threshold.
			checkFor(t, time.Second, 100*time.Millisecond, func() error {
				if numo := mset.numConsumers(); numo != 0 {
					return fmt.Errorf("Expected number of consumers to be 0, got %d", numo)
				}
				return nil
			})
		})
	}
}

func TestJetStreamMetadata(t *testing.T) {
	cases := []struct {
		name    string
		mconfig *StreamConfig
	}{
		{"MemoryStore", &StreamConfig{Name: "DC", Retention: WorkQueuePolicy, Storage: MemoryStorage}},
		{"FileStore", &StreamConfig{Name: "DC", Retention: WorkQueuePolicy, Storage: FileStorage}},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			s := RunBasicJetStreamServer(t)
			defer s.Shutdown()

			mset, err := s.GlobalAccount().addStream(c.mconfig)
			if err != nil {
				t.Fatalf("Unexpected error adding stream: %v", err)
			}
			defer mset.delete()

			nc := clientConnectToServer(t, s)
			defer nc.Close()

			for i := 0; i < 10; i++ {
				nc.Publish("DC", []byte("OK!"))
				nc.Flush()
				time.Sleep(time.Millisecond)
			}

			if state := mset.state(); state.Msgs != 10 {
				t.Fatalf("Expected %d messages, got %d", 10, state.Msgs)
			}

			o, err := mset.addConsumer(workerModeConfig("WQ"))
			if err != nil {
				t.Fatalf("Expected no error with registered interest, got %v", err)
			}
			defer o.delete()

			for i := uint64(1); i <= 10; i++ {
				m, err := nc.Request(o.requestNextMsgSubject(), nil, time.Second)
				if err != nil {
					t.Fatalf("Unexpected error: %v", err)
				}

				sseq, dseq, dcount, ts, _ := ackReplyInfo(m.Reply)

				mreq := &JSApiMsgGetRequest{Seq: sseq}
				req, err := json.Marshal(mreq)
				if err != nil {
					t.Fatalf("Unexpected error: %v", err)
				}
				// Load the original message from the stream to verify ReplyInfo ts against stored message
				smsgj, err := nc.Request(fmt.Sprintf(JSApiMsgGetT, c.mconfig.Name), req, time.Second)
				if err != nil {
					t.Fatalf("Could not retrieve stream message: %v", err)
				}

				var resp JSApiMsgGetResponse
				err = json.Unmarshal(smsgj.Data, &resp)
				if err != nil {
					t.Fatalf("Could not parse stream message: %v", err)
				}
				if resp.Message == nil || resp.Error != nil {
					t.Fatalf("Did not receive correct response")
				}
				smsg := resp.Message
				if ts != smsg.Time.UnixNano() {
					t.Fatalf("Wrong timestamp in ReplyInfo for msg %d, expected %v got %v", i, ts, smsg.Time.UnixNano())
				}
				if sseq != i {
					t.Fatalf("Expected set sequence of %d, got %d", i, sseq)
				}
				if dseq != i {
					t.Fatalf("Expected delivery sequence of %d, got %d", i, dseq)
				}
				if dcount != 1 {
					t.Fatalf("Expected delivery count to be 1, got %d", dcount)
				}
				m.Respond(AckAck)
			}

			// Now make sure we get right response when message is missing.
			mreq := &JSApiMsgGetRequest{Seq: 1}
			req, err := json.Marshal(mreq)
			if err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}
			// Load the original message from the stream to verify ReplyInfo ts against stored message
			rmsg, err := nc.Request(fmt.Sprintf(JSApiMsgGetT, c.mconfig.Name), req, time.Second)
			if err != nil {
				t.Fatalf("Could not retrieve stream message: %v", err)
			}
			var resp JSApiMsgGetResponse
			err = json.Unmarshal(rmsg.Data, &resp)
			if err != nil {
				t.Fatalf("Could not parse stream message: %v", err)
			}
			if resp.Error == nil || resp.Error.Code != 404 || resp.Error.Description != "no message found" {
				t.Fatalf("Did not get correct error response: %+v", resp.Error)
			}
		})
	}
}
func TestJetStreamRedeliverCount(t *testing.T) {
	cases := []struct {
		name    string
		mconfig *StreamConfig
	}{
		{"MemoryStore", &StreamConfig{Name: "DC", Storage: MemoryStorage}},
		{"FileStore", &StreamConfig{Name: "DC", Storage: FileStorage}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			s := RunBasicJetStreamServer(t)
			defer s.Shutdown()

			mset, err := s.GlobalAccount().addStream(c.mconfig)
			if err != nil {
				t.Fatalf("Unexpected error adding stream: %v", err)
			}
			defer mset.delete()

			nc, js := jsClientConnect(t, s)
			defer nc.Close()

			if _, err = js.Publish("DC", []byte("OK!")); err != nil {
				t.Fatal(err)
			}
			checkFor(t, time.Second, time.Millisecond*250, func() error {
				if state := mset.state(); state.Msgs != 1 {
					return fmt.Errorf("Expected %d messages, got %d", 1, state.Msgs)
				}
				return nil
			})

			o, err := mset.addConsumer(workerModeConfig("WQ"))
			if err != nil {
				t.Fatalf("Expected no error with registered interest, got %v", err)
			}
			defer o.delete()

			for i := uint64(1); i <= 10; i++ {
				m, err := nc.Request(o.requestNextMsgSubject(), nil, time.Second)
				if err != nil {
					t.Fatalf("Unexpected error: %v", err)
				}

				sseq, dseq, dcount, _, _ := ackReplyInfo(m.Reply)

				// Make sure we keep getting stream sequence #1
				if sseq != 1 {
					t.Fatalf("Expected set sequence of 1, got %d", sseq)
				}
				if dseq != i {
					t.Fatalf("Expected delivery sequence of %d, got %d", i, dseq)
				}
				// Now make sure dcount is same as dseq (or i).
				if dcount != i {
					t.Fatalf("Expected delivery count to be %d, got %d", i, dcount)
				}

				// Make sure it keeps getting sent back.
				m.Respond(AckNak)
				nc.Flush()
			}
		})
	}
}

// We want to make sure that for pull based consumers that if we ack
// late with no interest the redelivery attempt is removed and we do
// not get the message back.
func TestJetStreamRedeliverAndLateAck(t *testing.T) {
	s := RunBasicJetStreamServer(t)
	defer s.Shutdown()

	mset, err := s.GlobalAccount().addStream(&StreamConfig{Name: "LA", Storage: MemoryStorage})
	if err != nil {
		t.Fatalf("Unexpected error adding stream: %v", err)
	}
	defer mset.delete()

	o, err := mset.addConsumer(&ConsumerConfig{Durable: "DDD", AckPolicy: AckExplicit, AckWait: 100 * time.Millisecond})
	if err != nil {
		t.Fatalf("Expected no error with registered interest, got %v", err)
	}
	defer o.delete()

	nc := clientConnectToServer(t, s)
	defer nc.Close()

	// Queue up message
	sendStreamMsg(t, nc, "LA", "Hello World!")

	nextSubj := o.requestNextMsgSubject()
	msg, err := nc.Request(nextSubj, nil, time.Second)
	require_NoError(t, err)

	// Wait for past ackwait time
	time.Sleep(150 * time.Millisecond)
	// Now ack!
	msg.AckSync()
	// We should not get this back.
	if _, err := nc.Request(nextSubj, nil, 10*time.Millisecond); err == nil {
		t.Fatalf("Message should not have been sent back")
	}
}

// https://github.com/nats-io/nats-server/issues/1502
func TestJetStreamPendingNextTimer(t *testing.T) {
	s := RunBasicJetStreamServer(t)
	defer s.Shutdown()

	mset, err := s.GlobalAccount().addStream(&StreamConfig{Name: "NT", Storage: MemoryStorage, Subjects: []string{"ORDERS.*"}})
	if err != nil {
		t.Fatalf("Unexpected error adding stream: %v", err)
	}
	defer mset.delete()

	o, err := mset.addConsumer(&ConsumerConfig{
		Durable:       "DDD",
		AckPolicy:     AckExplicit,
		FilterSubject: "ORDERS.test",
		AckWait:       100 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("Expected no error with registered interest, got %v", err)
	}
	defer o.delete()

	sendAndReceive := func() {
		nc := clientConnectToServer(t, s)
		defer nc.Close()

		// Queue up message
		sendStreamMsg(t, nc, "ORDERS.test", "Hello World! #1")
		sendStreamMsg(t, nc, "ORDERS.test", "Hello World! #2")

		nextSubj := o.requestNextMsgSubject()
		for i := 0; i < 2; i++ {
			if _, err := nc.Request(nextSubj, nil, time.Second); err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}
		}
		nc.Close()
		time.Sleep(200 * time.Millisecond)
	}

	sendAndReceive()
	sendAndReceive()
	sendAndReceive()
}

func TestJetStreamCanNotNakAckd(t *testing.T) {
	cases := []struct {
		name    string
		mconfig *StreamConfig
	}{
		{"MemoryStore", &StreamConfig{Name: "DC", Storage: MemoryStorage}},
		{"FileStore", &StreamConfig{Name: "DC", Storage: FileStorage}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			s := RunBasicJetStreamServer(t)
			defer s.Shutdown()

			mset, err := s.GlobalAccount().addStream(c.mconfig)
			if err != nil {
				t.Fatalf("Unexpected error adding stream: %v", err)
			}
			defer mset.delete()

			nc, js := jsClientConnect(t, s)
			defer nc.Close()

			// Send 10 msgs
			for i := 0; i < 10; i++ {
				js.Publish("DC", []byte("OK!"))
			}
			if state := mset.state(); state.Msgs != 10 {
				t.Fatalf("Expected %d messages, got %d", 10, state.Msgs)
			}

			o, err := mset.addConsumer(workerModeConfig("WQ"))
			if err != nil {
				t.Fatalf("Expected no error with registered interest, got %v", err)
			}
			defer o.delete()

			for i := uint64(1); i <= 10; i++ {
				m, err := nc.Request(o.requestNextMsgSubject(), nil, time.Second)
				if err != nil {
					t.Fatalf("Unexpected error: %v", err)
				}
				// Ack evens.
				if i%2 == 0 {
					m.Respond(nil)
				}
			}
			nc.Flush()

			// Fake these for now.
			ackReplyT := "$JS.A.DC.WQ.1.%d.%d"
			checkBadNak := func(seq int) {
				t.Helper()
				if err := nc.Publish(fmt.Sprintf(ackReplyT, seq, seq), AckNak); err != nil {
					t.Fatalf("Error sending nak: %v", err)
				}
				nc.Flush()
				if _, err := nc.Request(o.requestNextMsgSubject(), nil, 10*time.Millisecond); err != nats.ErrTimeout {
					t.Fatalf("Did not expect new delivery on nak of %d", seq)
				}
			}

			// If the nak took action it will deliver another message, incrementing the next delivery seq.
			// We ack evens above, so these should fail
			for i := 2; i <= 10; i += 2 {
				checkBadNak(i)
			}

			// Now check we can not nak something we do not have.
			checkBadNak(22)
		})
	}
}

func TestJetStreamStreamPurge(t *testing.T) {
	cases := []struct {
		name    string
		mconfig *StreamConfig
	}{
		{"MemoryStore", &StreamConfig{Name: "DC", Storage: MemoryStorage}},
		{"FileStore", &StreamConfig{Name: "DC", Storage: FileStorage}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			s := RunBasicJetStreamServer(t)
			defer s.Shutdown()

			mset, err := s.GlobalAccount().addStream(c.mconfig)
			if err != nil {
				t.Fatalf("Unexpected error adding stream: %v", err)
			}
			defer mset.delete()

			nc, js := jsClientConnect(t, s)
			defer nc.Close()

			// Send 100 msgs
			for i := 0; i < 100; i++ {
				js.Publish("DC", []byte("OK!"))
			}
			if state := mset.state(); state.Msgs != 100 {
				t.Fatalf("Expected %d messages, got %d", 100, state.Msgs)
			}
			mset.purge(nil)
			state := mset.state()
			if state.Msgs != 0 {
				t.Fatalf("Expected %d messages, got %d", 0, state.Msgs)
			}
			// Make sure first timestamp are reset.
			if !state.FirstTime.IsZero() {
				t.Fatalf("Expected the state's first time to be zero after purge")
			}
			time.Sleep(10 * time.Millisecond)
			now := time.Now()
			js.Publish("DC", []byte("OK!"))

			state = mset.state()
			if state.Msgs != 1 {
				t.Fatalf("Expected %d message, got %d", 1, state.Msgs)
			}
			if state.FirstTime.Before(now) {
				t.Fatalf("First time is incorrect after adding messages back in")
			}
			if state.FirstTime != state.LastTime {
				t.Fatalf("Expected first and last times to be the same for only message")
			}
		})
	}
}

func TestJetStreamStreamPurgeWithConsumer(t *testing.T) {
	cases := []struct {
		name    string
		mconfig *StreamConfig
	}{
		{"MemoryStore", &StreamConfig{Name: "DC", Storage: MemoryStorage}},
		{"FileStore", &StreamConfig{Name: "DC", Storage: FileStorage}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			s := RunBasicJetStreamServer(t)
			defer s.Shutdown()

			mset, err := s.GlobalAccount().addStream(c.mconfig)
			if err != nil {
				t.Fatalf("Unexpected error adding stream: %v", err)
			}
			defer mset.delete()

			nc, js := jsClientConnect(t, s)
			defer nc.Close()

			// Send 100 msgs
			for i := 0; i < 100; i++ {
				js.Publish("DC", []byte("OK!"))
			}
			if state := mset.state(); state.Msgs != 100 {
				t.Fatalf("Expected %d messages, got %d", 100, state.Msgs)
			}
			// Now create an consumer and make sure it functions properly.
			o, err := mset.addConsumer(workerModeConfig("WQ"))
			if err != nil {
				t.Fatalf("Expected no error with registered interest, got %v", err)
			}
			defer o.delete()
			nextSubj := o.requestNextMsgSubject()
			for i := 0; i < 50; i++ {
				msg, err := nc.Request(nextSubj, nil, time.Second)
				if err != nil {
					t.Fatalf("Unexpected error: %v", err)
				}
				// Ack.
				msg.Respond(nil)
			}
			// Now grab next 25 without ack.
			for i := 0; i < 25; i++ {
				if _, err := nc.Request(nextSubj, nil, time.Second); err != nil {
					t.Fatalf("Unexpected error: %v", err)
				}
			}
			state := o.info()
			if state.AckFloor.Consumer != 50 {
				t.Fatalf("Expected ack floor of 50, got %d", state.AckFloor.Consumer)
			}
			if state.NumAckPending != 25 {
				t.Fatalf("Expected len(pending) to be 25, got %d", state.NumAckPending)
			}
			// Now do purge.
			mset.purge(nil)
			if state := mset.state(); state.Msgs != 0 {
				t.Fatalf("Expected %d messages, got %d", 0, state.Msgs)
			}
			// Now re-acquire state and check that we did the right thing.
			// Pending should be cleared, and stream sequences should have been set
			// to the total messages before purge + 1.
			state = o.info()
			if state.NumAckPending != 0 {
				t.Fatalf("Expected no pending, got %d", state.NumAckPending)
			}
			if state.Delivered.Stream != 100 {
				t.Fatalf("Expected to have setseq now at next seq of 100, got %d", state.Delivered.Stream)
			}
			// Check AckFloors which should have also been adjusted.
			if state.AckFloor.Stream != 100 {
				t.Fatalf("Expected ackfloor for setseq to be 100, got %d", state.AckFloor.Stream)
			}
			if state.AckFloor.Consumer != 75 {
				t.Fatalf("Expected ackfloor for obsseq to be 75, got %d", state.AckFloor.Consumer)
			}
			// Also make sure we can get new messages correctly.
			js.Publish("DC", []byte("OK-22"))
			if msg, err := nc.Request(nextSubj, nil, time.Second); err != nil {
				t.Fatalf("Unexpected error: %v", err)
			} else if string(msg.Data) != "OK-22" {
				t.Fatalf("Received wrong message, wanted 'OK-22', got %q", msg.Data)
			}
		})
	}
}

func TestJetStreamStreamPurgeWithConsumerAndRedelivery(t *testing.T) {
	cases := []struct {
		name    string
		mconfig *StreamConfig
	}{
		{"MemoryStore", &StreamConfig{Name: "DC", Storage: MemoryStorage}},
		{"FileStore", &StreamConfig{Name: "DC", Storage: FileStorage}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			s := RunBasicJetStreamServer(t)
			defer s.Shutdown()

			mset, err := s.GlobalAccount().addStream(c.mconfig)
			if err != nil {
				t.Fatalf("Unexpected error adding stream: %v", err)
			}
			defer mset.delete()

			nc, js := jsClientConnect(t, s)
			defer nc.Close()

			// Send 100 msgs
			for i := 0; i < 100; i++ {
				js.Publish("DC", []byte("OK!"))
			}
			if state := mset.state(); state.Msgs != 100 {
				t.Fatalf("Expected %d messages, got %d", 100, state.Msgs)
			}
			// Now create an consumer and make sure it functions properly.
			// This will test redelivery state and purge of the stream.
			wcfg := &ConsumerConfig{
				Durable:   "WQ",
				AckPolicy: AckExplicit,
				AckWait:   20 * time.Millisecond,
			}
			o, err := mset.addConsumer(wcfg)
			if err != nil {
				t.Fatalf("Expected no error with registered interest, got %v", err)
			}
			defer o.delete()
			nextSubj := o.requestNextMsgSubject()
			for i := 0; i < 50; i++ {
				// Do not ack these.
				if _, err := nc.Request(nextSubj, nil, time.Second); err != nil {
					t.Fatalf("Unexpected error: %v", err)
				}
			}
			// Now wait to make sure we are in a redelivered state.
			time.Sleep(wcfg.AckWait * 2)
			// Now do purge.
			mset.purge(nil)
			if state := mset.state(); state.Msgs != 0 {
				t.Fatalf("Expected %d messages, got %d", 0, state.Msgs)
			}
			// Now get the state and check that we did the right thing.
			// Pending should be cleared, and stream sequences should have been set
			// to the total messages before purge + 1.
			state := o.info()
			if state.NumAckPending != 0 {
				t.Fatalf("Expected no pending, got %d", state.NumAckPending)
			}
			if state.Delivered.Stream != 100 {
				t.Fatalf("Expected to have setseq now at next seq of 100, got %d", state.Delivered.Stream)
			}
			// Check AckFloors which should have also been adjusted.
			if state.AckFloor.Stream != 100 {
				t.Fatalf("Expected ackfloor for setseq to be 100, got %d", state.AckFloor.Stream)
			}
			if state.AckFloor.Consumer != 50 {
				t.Fatalf("Expected ackfloor for obsseq to be 75, got %d", state.AckFloor.Consumer)
			}
			// Also make sure we can get new messages correctly.
			js.Publish("DC", []byte("OK-22"))
			if msg, err := nc.Request(nextSubj, nil, time.Second); err != nil {
				t.Fatalf("Unexpected error: %v", err)
			} else if string(msg.Data) != "OK-22" {
				t.Fatalf("Received wrong message, wanted 'OK-22', got %q", msg.Data)
			}
		})
	}
}

func TestJetStreamInterestRetentionStream(t *testing.T) {
	cases := []struct {
		name    string
		mconfig *StreamConfig
	}{
		{"MemoryStore", &StreamConfig{Name: "DC", Storage: MemoryStorage, Retention: InterestPolicy}},
		{"FileStore", &StreamConfig{Name: "DC", Storage: FileStorage, Retention: InterestPolicy}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			s := RunBasicJetStreamServer(t)
			defer s.Shutdown()

			mset, err := s.GlobalAccount().addStream(c.mconfig)
			if err != nil {
				t.Fatalf("Unexpected error adding stream: %v", err)
			}
			defer mset.delete()

			nc, js := jsClientConnect(t, s)
			defer nc.Close()

			// Send 100 msgs
			totalMsgs := 100

			for i := 0; i < totalMsgs; i++ {
				js.Publish("DC", []byte("OK!"))
			}

			checkNumMsgs := func(numExpected int) {
				t.Helper()
				checkFor(t, time.Second, 15*time.Millisecond, func() error {
					if state := mset.state(); state.Msgs != uint64(numExpected) {
						return fmt.Errorf("Expected %d messages, got %d", numExpected, state.Msgs)
					}
					return nil
				})
			}

			// Since we had no interest this should be 0.
			checkNumMsgs(0)

			syncSub := func() *nats.Subscription {
				sub, _ := nc.SubscribeSync(nats.NewInbox())
				nc.Flush()
				return sub
			}

			// Now create three consumers.
			// 1. AckExplicit
			// 2. AckAll
			// 3. AckNone

			sub1 := syncSub()
			mset.addConsumer(&ConsumerConfig{DeliverSubject: sub1.Subject, AckPolicy: AckExplicit})

			sub2 := syncSub()
			mset.addConsumer(&ConsumerConfig{DeliverSubject: sub2.Subject, AckPolicy: AckAll})

			sub3 := syncSub()
			mset.addConsumer(&ConsumerConfig{DeliverSubject: sub3.Subject, AckPolicy: AckNone})

			for i := 0; i < totalMsgs; i++ {
				js.Publish("DC", []byte("OK!"))
			}

			checkNumMsgs(totalMsgs)

			// Wait for all messsages to be pending for each sub.
			for i, sub := range []*nats.Subscription{sub1, sub2, sub3} {
				checkFor(t, 5*time.Second, 25*time.Millisecond, func() error {
					if nmsgs, _, _ := sub.Pending(); nmsgs != totalMsgs {
						return fmt.Errorf("Did not receive correct number of messages: %d vs %d for sub %d", nmsgs, totalMsgs, i+1)
					}
					return nil
				})
			}

			getAndAck := func(sub *nats.Subscription) {
				t.Helper()
				if m, err := sub.NextMsg(time.Second); err != nil {
					t.Fatalf("Unexpected error: %v", err)
				} else {
					m.Respond(nil)
				}
				nc.Flush()
			}

			// Ack evens for the explicit ack sub.
			var odds []*nats.Msg
			for i := 1; i <= totalMsgs; i++ {
				if m, err := sub1.NextMsg(time.Second); err != nil {
					t.Fatalf("Unexpected error: %v", err)
				} else if i%2 == 0 {
					m.Respond(nil) // Ack evens.
				} else {
					odds = append(odds, m)
				}
			}
			nc.Flush()

			checkNumMsgs(totalMsgs)

			// Now ack first for AckAll sub2
			getAndAck(sub2)
			// We should be at the same number since we acked 1, explicit acked 2
			checkNumMsgs(totalMsgs)
			// Now ack second for AckAll sub2
			getAndAck(sub2)
			// We should now have 1 removed.
			checkNumMsgs(totalMsgs - 1)
			// Now ack third for AckAll sub2
			getAndAck(sub2)
			// We should still only have 1 removed.
			checkNumMsgs(totalMsgs - 1)

			// Now ack odds from explicit.
			for _, m := range odds {
				m.Respond(nil) // Ack
			}
			nc.Flush()

			// we should have 1, 2, 3 acks now.
			checkNumMsgs(totalMsgs - 3)

			nm, _, _ := sub2.Pending()
			// Now ack last ackAll message. This should clear all of them.
			for i := 1; i <= nm; i++ {
				if m, err := sub2.NextMsg(time.Second); err != nil {
					t.Fatalf("Unexpected error: %v", err)
				} else if i == nm {
					m.Respond(nil)
				}
			}
			nc.Flush()

			// Should be zero now.
			checkNumMsgs(0)
		})
	}
}

func TestJetStreamInterestRetentionStreamWithFilteredConsumers(t *testing.T) {
	cases := []struct {
		name    string
		mconfig *StreamConfig
	}{
		{"MemoryStore", &StreamConfig{Name: "DC", Subjects: []string{"*"}, Storage: MemoryStorage, Retention: InterestPolicy}},
		{"FileStore", &StreamConfig{Name: "DC", Subjects: []string{"*"}, Storage: FileStorage, Retention: InterestPolicy}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			s := RunBasicJetStreamServer(t)
			defer s.Shutdown()

			mset, err := s.GlobalAccount().addStream(c.mconfig)
			if err != nil {
				t.Fatalf("Unexpected error adding stream: %v", err)
			}
			defer mset.delete()

			nc, js := jsClientConnect(t, s)
			defer nc.Close()

			fsub, err := js.SubscribeSync("foo")
			if err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}
			defer fsub.Unsubscribe()

			bsub, err := js.SubscribeSync("bar")
			if err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}
			defer bsub.Unsubscribe()

			msg := []byte("FILTERED")
			sendMsg := func(subj string) {
				t.Helper()
				if _, err = js.Publish(subj, msg); err != nil {
					t.Fatalf("Unexpected publish error: %v", err)
				}
			}

			getAndAck := func(sub *nats.Subscription) {
				t.Helper()
				m, err := sub.NextMsg(time.Second)
				if err != nil {
					t.Fatalf("Unexpected error getting msg: %v", err)
				}
				m.AckSync()
			}

			checkState := func(expected uint64) {
				t.Helper()
				si, err := js.StreamInfo("DC")
				if err != nil {
					t.Fatalf("Unexpected error: %v", err)
				}
				if si.State.Msgs != expected {
					t.Fatalf("Expected %d msgs, got %d", expected, si.State.Msgs)
				}
			}

			sendMsg("foo")
			checkState(1)
			getAndAck(fsub)
			checkState(0)
			sendMsg("bar")
			sendMsg("foo")
			checkState(2)
			getAndAck(bsub)
			checkState(1)
			getAndAck(fsub)
			checkState(0)
		})
	}
}

func TestJetStreamInterestRetentionWithWildcardsAndFilteredConsumers(t *testing.T) {
	msc := StreamConfig{
		Name:      "DCWC",
		Subjects:  []string{"foo.*"},
		Storage:   MemoryStorage,
		Retention: InterestPolicy,
	}
	fsc := msc
	fsc.Storage = FileStorage

	cases := []struct {
		name    string
		mconfig *StreamConfig
	}{
		{"MemoryStore", &msc},
		{"FileStore", &fsc},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			s := RunBasicJetStreamServer(t)
			defer s.Shutdown()

			mset, err := s.GlobalAccount().addStream(c.mconfig)
			if err != nil {
				t.Fatalf("Unexpected error adding stream: %v", err)
			}
			defer mset.delete()

			nc := clientConnectToServer(t, s)
			defer nc.Close()

			// Send 10 msgs
			for i := 0; i < 10; i++ {
				sendStreamMsg(t, nc, "foo.bar", "Hello World!")
			}
			if state := mset.state(); state.Msgs != 0 {
				t.Fatalf("Expected %d messages, got %d", 0, state.Msgs)
			}

			cfg := &ConsumerConfig{Durable: "ddd", FilterSubject: "foo.bar", AckPolicy: AckExplicit}
			o, err := mset.addConsumer(cfg)
			if err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}
			defer o.delete()

			sendStreamMsg(t, nc, "foo.bar", "Hello World!")
			if state := mset.state(); state.Msgs != 1 {
				t.Fatalf("Expected %d message, got %d", 1, state.Msgs)
			} else if state.FirstSeq != 11 {
				t.Fatalf("Expected %d for first seq, got %d", 11, state.FirstSeq)
			}
			// Now send to foo.baz, which has no interest, so we should not hold onto this message.
			sendStreamMsg(t, nc, "foo.baz", "Hello World!")
			if state := mset.state(); state.Msgs != 1 {
				t.Fatalf("Expected %d message, got %d", 1, state.Msgs)
			}
		})
	}
}

func TestJetStreamInterestRetentionStreamWithDurableRestart(t *testing.T) {
	cases := []struct {
		name    string
		mconfig *StreamConfig
	}{
		{"MemoryStore", &StreamConfig{Name: "IK", Storage: MemoryStorage, Retention: InterestPolicy}},
		{"FileStore", &StreamConfig{Name: "IK", Storage: FileStorage, Retention: InterestPolicy}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			s := RunBasicJetStreamServer(t)
			defer s.Shutdown()

			mset, err := s.GlobalAccount().addStream(c.mconfig)
			if err != nil {
				t.Fatalf("Unexpected error adding stream: %v", err)
			}
			defer mset.delete()

			checkNumMsgs := func(numExpected int) {
				t.Helper()
				checkFor(t, time.Second, 50*time.Millisecond, func() error {
					if state := mset.state(); state.Msgs != uint64(numExpected) {
						return fmt.Errorf("Expected %d messages, got %d", numExpected, state.Msgs)
					}
					return nil
				})
			}

			nc := clientConnectToServer(t, s)
			defer nc.Close()

			sub, _ := nc.SubscribeSync(nats.NewInbox())
			nc.Flush()

			cfg := &ConsumerConfig{Durable: "ivan", DeliverPolicy: DeliverNew, DeliverSubject: sub.Subject, AckPolicy: AckNone}

			o, _ := mset.addConsumer(cfg)

			sendStreamMsg(t, nc, "IK", "M1")
			sendStreamMsg(t, nc, "IK", "M2")

			checkSubPending := func(numExpected int) {
				t.Helper()
				checkFor(t, 200*time.Millisecond, 10*time.Millisecond, func() error {
					if nmsgs, _, _ := sub.Pending(); err != nil || nmsgs != numExpected {
						return fmt.Errorf("Did not receive correct number of messages: %d vs %d", nmsgs, numExpected)
					}
					return nil
				})
			}

			checkSubPending(2)
			checkNumMsgs(0)

			// Now stop the subscription.
			sub.Unsubscribe()
			checkFor(t, 200*time.Millisecond, 10*time.Millisecond, func() error {
				if o.isActive() {
					return fmt.Errorf("Still active consumer")
				}
				return nil
			})

			sendStreamMsg(t, nc, "IK", "M3")
			sendStreamMsg(t, nc, "IK", "M4")

			checkNumMsgs(2)

			// Now restart the durable.
			sub, _ = nc.SubscribeSync(nats.NewInbox())
			nc.Flush()
			cfg.DeliverSubject = sub.Subject
			if o, err = mset.addConsumer(cfg); err != nil {
				t.Fatalf("Error re-establishing the durable consumer: %v", err)
			}
			checkSubPending(2)

			for _, expected := range []string{"M3", "M4"} {
				if m, err := sub.NextMsg(time.Second); err != nil {
					t.Fatalf("Unexpected error: %v", err)
				} else if string(m.Data) != expected {
					t.Fatalf("Expected %q, got %q", expected, m.Data)
				}
			}

			// Should all be gone now.
			checkNumMsgs(0)

			// Now restart again and make sure we do not get any messages.
			sub.Unsubscribe()
			checkFor(t, 200*time.Millisecond, 10*time.Millisecond, func() error {
				if o.isActive() {
					return fmt.Errorf("Still active consumer")
				}
				return nil
			})
			o.delete()

			sub, _ = nc.SubscribeSync(nats.NewInbox())
			nc.Flush()

			cfg.DeliverSubject = sub.Subject
			cfg.AckPolicy = AckExplicit // Set ack
			if o, err = mset.addConsumer(cfg); err != nil {
				t.Fatalf("Error re-establishing the durable consumer: %v", err)
			}
			time.Sleep(100 * time.Millisecond)
			checkSubPending(0)
			checkNumMsgs(0)

			// Now queue up some messages.
			for i := 1; i <= 10; i++ {
				sendStreamMsg(t, nc, "IK", fmt.Sprintf("M%d", i))
			}
			checkNumMsgs(10)
			checkSubPending(10)

			// Create second consumer
			sub2, _ := nc.SubscribeSync(nats.NewInbox())
			nc.Flush()
			cfg.DeliverSubject = sub2.Subject
			cfg.Durable = "derek"
			o2, err := mset.addConsumer(cfg)
			if err != nil {
				t.Fatalf("Error creating second durable consumer: %v", err)
			}

			// Now queue up some messages.
			for i := 11; i <= 20; i++ {
				sendStreamMsg(t, nc, "IK", fmt.Sprintf("M%d", i))
			}
			checkNumMsgs(20)
			checkSubPending(20)

			// Now make sure deleting the consumers will remove messages from
			// the stream since we are interest retention based.
			o.delete()
			checkNumMsgs(10)

			o2.delete()
			checkNumMsgs(0)
		})
	}
}

func TestJetStreamSystemLimits(t *testing.T) {
	s := RunRandClientPortServer(t)
	defer s.Shutdown()

	if _, _, err := s.JetStreamReservedResources(); err == nil {
		t.Fatalf("Expected error requesting jetstream reserved resources when not enabled")
	}
	// Create some accounts.
	facc, _ := s.LookupOrRegisterAccount("FOO")
	bacc, _ := s.LookupOrRegisterAccount("BAR")
	zacc, _ := s.LookupOrRegisterAccount("BAZ")

	jsconfig := &JetStreamConfig{MaxMemory: 1024, MaxStore: 8192, StoreDir: t.TempDir()}
	if err := s.EnableJetStream(jsconfig); err != nil {
		t.Fatalf("Expected no error, got %v", err)
	}

	if rm, rd, err := s.JetStreamReservedResources(); err != nil {
		t.Fatalf("Unexpected error requesting jetstream reserved resources: %v", err)
	} else if rm != 0 || rd != 0 {
		t.Fatalf("Expected reserved memory and store to be 0, got %d and %d", rm, rd)
	}

	limits := func(mem int64, store int64) map[string]JetStreamAccountLimits {
		return map[string]JetStreamAccountLimits{
			_EMPTY_: {
				MaxMemory:    mem,
				MaxStore:     store,
				MaxStreams:   -1,
				MaxConsumers: -1,
			},
		}
	}

	if err := facc.EnableJetStream(limits(24, 192), nil); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	// Use up rest of our resources in memory
	if err := bacc.EnableJetStream(limits(1000, 0), nil); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	// Now ask for more memory. Should error.
	if err := zacc.EnableJetStream(limits(1000, 0), nil); err == nil {
		t.Fatalf("Expected an error when exhausting memory resource limits")
	}
	// Disk too.
	if err := zacc.EnableJetStream(limits(0, 10000), nil); err == nil {
		t.Fatalf("Expected an error when exhausting memory resource limits")
	}
	facc.DisableJetStream()
	bacc.DisableJetStream()
	zacc.DisableJetStream()

	// Make sure we unreserved resources.
	if rm, rd, err := s.JetStreamReservedResources(); err != nil {
		t.Fatalf("Unexpected error requesting jetstream reserved resources: %v", err)
	} else if rm != 0 || rd != 0 {
		t.Fatalf("Expected reserved memory and store to be 0, got %v and %v", friendlyBytes(rm), friendlyBytes(rd))
	}

	if err := facc.EnableJetStream(limits(24, 192), nil); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	// Test Adjust
	lim := limits(jsconfig.MaxMemory, jsconfig.MaxStore)
	l := lim[_EMPTY_]
	l.MaxStreams = 10
	l.MaxConsumers = 10
	lim[_EMPTY_] = l
	if err := facc.UpdateJetStreamLimits(lim); err != nil {
		t.Fatalf("Unexpected error updating jetstream account limits: %v", err)
	}

	var msets []*stream
	// Now test max streams and max consumers. Note max consumers is per stream.
	for i := 0; i < 10; i++ {
		mname := fmt.Sprintf("foo.%d", i)
		mset, err := facc.addStream(&StreamConfig{Name: strconv.Itoa(i), Storage: MemoryStorage, Subjects: []string{mname}})
		if err != nil {
			t.Fatalf("Unexpected error adding stream: %v", err)
		}
		msets = append(msets, mset)
	}

	// Remove them all
	for _, mset := range msets {
		mset.delete()
	}

	// Now try to add one with bytes limit that would exceed the account limit.
	if _, err := facc.addStream(&StreamConfig{Name: "22", Storage: MemoryStorage, MaxBytes: jsconfig.MaxStore * 2}); err == nil {
		t.Fatalf("Expected error adding stream over limit")
	}

	// Replicas can't be > 1
	if _, err := facc.addStream(&StreamConfig{Name: "22", Storage: MemoryStorage, Replicas: 10}); err == nil {
		t.Fatalf("Expected error adding stream over limit")
	}

	// Test consumers limit against account limit when the stream does not set a limit
	mset, err := facc.addStream(&StreamConfig{Name: "22", Storage: MemoryStorage, Subjects: []string{"foo.22"}})
	if err != nil {
		t.Fatalf("Unexpected error adding stream: %v", err)
	}

	for i := 0; i < 10; i++ {
		oname := fmt.Sprintf("O:%d", i)
		_, err := mset.addConsumer(&ConsumerConfig{Durable: oname, AckPolicy: AckExplicit})
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
	}

	// This one should fail.
	if _, err := mset.addConsumer(&ConsumerConfig{Durable: "O:22", AckPolicy: AckExplicit}); err == nil {
		t.Fatalf("Expected error adding consumer over the limit")
	}

	// Test consumer limit against stream limit
	mset.delete()
	mset, err = facc.addStream(&StreamConfig{Name: "22", Storage: MemoryStorage, Subjects: []string{"foo.22"}, MaxConsumers: 5})
	if err != nil {
		t.Fatalf("Unexpected error adding stream: %v", err)
	}

	for i := 0; i < 5; i++ {
		oname := fmt.Sprintf("O:%d", i)
		_, err := mset.addConsumer(&ConsumerConfig{Durable: oname, AckPolicy: AckExplicit})
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
	}

	// This one should fail.
	if _, err := mset.addConsumer(&ConsumerConfig{Durable: "O:22", AckPolicy: AckExplicit}); err == nil {
		t.Fatalf("Expected error adding consumer over the limit")
	}

	// Test the account having smaller limits than the stream
	mset.delete()

	mset, err = facc.addStream(&StreamConfig{Name: "22", Storage: MemoryStorage, Subjects: []string{"foo.22"}, MaxConsumers: 10})
	if err != nil {
		t.Fatalf("Unexpected error adding stream: %v", err)
	}

	l.MaxConsumers = 5
	lim[_EMPTY_] = l
	if err := facc.UpdateJetStreamLimits(lim); err != nil {
		t.Fatalf("Unexpected error updating jetstream account limits: %v", err)
	}

	for i := 0; i < 5; i++ {
		oname := fmt.Sprintf("O:%d", i)
		_, err := mset.addConsumer(&ConsumerConfig{Durable: oname, AckPolicy: AckExplicit})
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
	}

	// This one should fail.
	if _, err := mset.addConsumer(&ConsumerConfig{Durable: "O:22", AckPolicy: AckExplicit}); err == nil {
		t.Fatalf("Expected error adding consumer over the limit")
	}
}

func TestJetStreamSystemLimitsPlacement(t *testing.T) {
	const smallSystemLimit = 128
	const mediumSystemLimit = smallSystemLimit * 2
	const largeSystemLimit = smallSystemLimit * 3

	tmpl := `
		listen: 127.0.0.1:-1
		server_name: %s
		jetstream: {
			max_mem_store: _MAXMEM_
			max_file_store: _MAXFILE_
			store_dir: '%s'
		}

		server_tags: [
			_TAG_
		]

		leaf {
			listen: 127.0.0.1:-1
		}
		cluster {
			name: %s
			listen: 127.0.0.1:%d
			routes = [%s]
		}
		accounts { $SYS { users = [ { user: "admin", pass: "s3cr3t!" } ] } }
	`
	storeCnf := func(serverName, clusterName, storeDir, conf string) string {
		switch serverName {
		case "S-1":
			conf = strings.Replace(conf, "_MAXMEM_", fmt.Sprint(smallSystemLimit), 1)
			conf = strings.Replace(conf, "_MAXFILE_", fmt.Sprint(smallSystemLimit), 1)
			return strings.Replace(conf, "_TAG_", "small", 1)
		case "S-2":
			conf = strings.Replace(conf, "_MAXMEM_", fmt.Sprint(mediumSystemLimit), 1)
			conf = strings.Replace(conf, "_MAXFILE_", fmt.Sprint(mediumSystemLimit), 1)
			return strings.Replace(conf, "_TAG_", "medium", 1)
		case "S-3":
			conf = strings.Replace(conf, "_MAXMEM_", fmt.Sprint(largeSystemLimit), 1)
			conf = strings.Replace(conf, "_MAXFILE_", fmt.Sprint(largeSystemLimit), 1)
			return strings.Replace(conf, "_TAG_", "large", 1)
		default:
			return conf
		}
	}

	cluster := createJetStreamClusterWithTemplateAndModHook(t, tmpl, "cluster-a", 3, storeCnf)
	defer cluster.shutdown()

	requestLeaderStepDown := func(clientURL string) error {
		nc, err := nats.Connect(clientURL, nats.UserInfo("admin", "s3cr3t!"))
		if err != nil {
			return err
		}
		defer nc.Close()

		ncResp, err := nc.Request(JSApiLeaderStepDown, nil, 3*time.Second)
		if err != nil {
			return err
		}

		var resp JSApiLeaderStepDownResponse
		if err := json.Unmarshal(ncResp.Data, &resp); err != nil {
			return err
		}
		if resp.Error != nil {
			return resp.Error
		}
		if !resp.Success {
			return fmt.Errorf("leader step down request not successful")
		}

		return nil
	}

	largeSrv := cluster.servers[2]
	// Force large server to be leader
	err := checkForErr(15*time.Second, 500*time.Millisecond, func() error {
		if largeSrv.JetStreamIsLeader() {
			return nil
		}

		if err := requestLeaderStepDown(largeSrv.ClientURL()); err != nil {
			return err
		}
		return fmt.Errorf("large server is not leader")
	})
	if err != nil {
		t.Skipf("failed to get desired layout: %s", err)
	}

	nc, js := jsClientConnect(t, largeSrv)
	defer nc.Close()

	cases := []struct {
		name           string
		storage        nats.StorageType
		createMaxBytes int64
		serverTag      string
		wantErr        bool
	}{
		{
			name:           "file create large stream on small server",
			storage:        nats.FileStorage,
			createMaxBytes: largeSystemLimit,
			serverTag:      "small",
			wantErr:        true,
		},
		{
			name:           "memory create large stream on small server",
			storage:        nats.MemoryStorage,
			createMaxBytes: largeSystemLimit,
			serverTag:      "small",
			wantErr:        true,
		},
		{
			name:           "file create large stream on medium server",
			storage:        nats.FileStorage,
			createMaxBytes: largeSystemLimit,
			serverTag:      "medium",
			wantErr:        true,
		},
		{
			name:           "memory create large stream on medium server",
			storage:        nats.MemoryStorage,
			createMaxBytes: largeSystemLimit,
			serverTag:      "medium",
			wantErr:        true,
		},
		{
			name:           "file create large stream on large server",
			storage:        nats.FileStorage,
			createMaxBytes: largeSystemLimit,
			serverTag:      "large",
		},
		{
			name:           "memory create large stream on large server",
			storage:        nats.MemoryStorage,
			createMaxBytes: largeSystemLimit,
			serverTag:      "large",
		},
	}

	for i := 0; i < len(cases) && !t.Failed(); i++ {
		c := cases[i]
		t.Run(c.name, func(st *testing.T) {
			_, err := js.AddStream(&nats.StreamConfig{
				Name:     "TEST",
				Subjects: []string{"foo"},
				Storage:  c.storage,
				MaxBytes: c.createMaxBytes,
				Placement: &nats.Placement{
					Cluster: "cluster-a",
					Tags:    []string{c.serverTag},
				},
			})
			if c.wantErr && err == nil {
				st.Fatalf("unexpected stream create success, maxBytes=%d, tag=%s",
					c.createMaxBytes, c.serverTag)
			} else if !c.wantErr && err != nil {
				st.Fatalf("unexpected error: %s", err)
			}

			if err == nil {
				err = js.DeleteStream("TEST")
				require_NoError(st, err)
			}
		})
	}

	// These next two tests should fail because although the stream fits in the
	// large and medium server, it doesn't fit on the small server.
	si, err := js.AddStream(&nats.StreamConfig{
		Name:     "TEST",
		Subjects: []string{"foo"},
		Storage:  nats.FileStorage,
		MaxBytes: smallSystemLimit + 1,
		Replicas: 3,
	})
	if err == nil {
		t.Fatalf("unexpected file stream create success, maxBytes=%d, replicas=%d",
			si.Config.MaxBytes, si.Config.Replicas)
	}

	si, err = js.AddStream(&nats.StreamConfig{
		Name:     "TEST",
		Subjects: []string{"foo"},
		Storage:  nats.MemoryStorage,
		MaxBytes: smallSystemLimit + 1,
		Replicas: 3,
	})
	if err == nil {
		t.Fatalf("unexpected memory stream create success, maxBytes=%d, replicas=%d",
			si.Config.MaxBytes, si.Config.Replicas)
	}

	t.Run("meta info placement in request - empty request", func(t *testing.T) {
		nc, err = nats.Connect(largeSrv.ClientURL(), nats.UserInfo("admin", "s3cr3t!"))
		require_NoError(t, err)
		defer nc.Close()

		var resp JSApiLeaderStepDownResponse
		ncResp, err := nc.Request(JSApiLeaderStepDown, []byte("{}"), 3*time.Second)
		require_NoError(t, err)
		err = json.Unmarshal(ncResp.Data, &resp)
		require_NoError(t, err)
		require_True(t, resp.Error == nil)
		require_True(t, resp.Success)

	})

	t.Run("meta info placement in request - uninitialized fields", func(t *testing.T) {
		nc, err = nats.Connect(largeSrv.ClientURL(), nats.UserInfo("admin", "s3cr3t!"))
		require_NoError(t, err)
		defer nc.Close()

		cluster.waitOnClusterReadyWithNumPeers(3)
		var resp JSApiLeaderStepDownResponse
		req, err := json.Marshal(JSApiLeaderStepdownRequest{Placement: nil})
		require_NoError(t, err)
		ncResp, err := nc.Request(JSApiLeaderStepDown, req, 10*time.Second)
		require_NoError(t, err)
		err = json.Unmarshal(ncResp.Data, &resp)
		require_NoError(t, err)
		require_True(t, resp.Error == nil)
	})

}

func TestJetStreamStreamLimitUpdate(t *testing.T) {
	s := RunBasicJetStreamServer(t)
	defer s.Shutdown()

	err := s.GlobalAccount().UpdateJetStreamLimits(map[string]JetStreamAccountLimits{
		_EMPTY_: {
			MaxMemory:  128,
			MaxStore:   128,
			MaxStreams: 1,
		},
	})
	require_NoError(t, err)

	nc, js := jsClientConnect(t, s)
	defer nc.Close()

	for _, storage := range []nats.StorageType{nats.MemoryStorage, nats.FileStorage} {
		cfg := &nats.StreamConfig{
			Name:     "TEST",
			Subjects: []string{"foo"},
			Storage:  storage,
			MaxBytes: 32,
		}

		_, err = js.AddStream(cfg)
		require_NoError(t, err)

		// Create with same config is idempotent, and must not exceed max streams as it already exists.
		_, err = js.AddStream(cfg)
		require_NoError(t, err)

		cfg.MaxBytes = 16
		_, err = js.UpdateStream(cfg)
		require_NoError(t, err)

		require_NoError(t, js.DeleteStream("TEST"))
	}
}

func TestJetStreamStreamStorageTrackingAndLimits(t *testing.T) {
	s := RunBasicJetStreamServer(t)
	defer s.Shutdown()

	gacc := s.GlobalAccount()

	al := map[string]JetStreamAccountLimits{
		_EMPTY_: {
			MaxMemory:    8192,
			MaxStore:     -1,
			MaxStreams:   -1,
			MaxConsumers: -1,
		},
	}

	if err := gacc.UpdateJetStreamLimits(al); err != nil {
		t.Fatalf("Unexpected error updating jetstream account limits: %v", err)
	}

	mset, err := gacc.addStream(&StreamConfig{Name: "LIMITS", Storage: MemoryStorage, Retention: WorkQueuePolicy})
	if err != nil {
		t.Fatalf("Unexpected error adding stream: %v", err)
	}
	defer mset.delete()

	nc := clientConnectToServer(t, s)
	defer nc.Close()

	toSend := 100
	for i := 0; i < toSend; i++ {
		sendStreamMsg(t, nc, "LIMITS", "Hello World!")
	}

	state := mset.state()
	usage := gacc.JetStreamUsage()

	// Make sure these are working correctly.
	if state.Bytes != usage.Memory {
		t.Fatalf("Expected to have stream bytes match memory usage, %d vs %d", state.Bytes, usage.Memory)
	}
	if usage.Streams != 1 {
		t.Fatalf("Expected to have 1 stream, got %d", usage.Streams)
	}

	// Do second stream.
	mset2, err := gacc.addStream(&StreamConfig{Name: "NUM22", Storage: MemoryStorage, Retention: WorkQueuePolicy})
	if err != nil {
		t.Fatalf("Unexpected error adding stream: %v", err)
	}
	defer mset2.delete()

	for i := 0; i < toSend; i++ {
		sendStreamMsg(t, nc, "NUM22", "Hello World!")
	}

	stats2 := mset2.state()
	usage = gacc.JetStreamUsage()

	if usage.Memory != (state.Bytes + stats2.Bytes) {
		t.Fatalf("Expected to track both streams, account is %v, stream1 is %v, stream2 is %v", usage.Memory, state.Bytes, stats2.Bytes)
	}

	// Make sure delete works.
	mset2.delete()
	stats2 = mset2.state()
	usage = gacc.JetStreamUsage()

	if usage.Memory != (state.Bytes + stats2.Bytes) {
		t.Fatalf("Expected to track both streams, account is %v, stream1 is %v, stream2 is %v", usage.Memory, state.Bytes, stats2.Bytes)
	}

	// Now drain the first one by consuming the messages.
	o, err := mset.addConsumer(workerModeConfig("WQ"))
	if err != nil {
		t.Fatalf("Expected no error with registered interest, got %v", err)
	}
	defer o.delete()

	for i := 0; i < toSend; i++ {
		msg, err := nc.Request(o.requestNextMsgSubject(), nil, time.Second)
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		msg.Respond(nil)
	}
	nc.Flush()

	state = mset.state()
	checkFor(t, time.Second, 15*time.Millisecond, func() error {
		usage = gacc.JetStreamUsage()
		if usage.Memory != 0 {
			return fmt.Errorf("Expected usage memory to be 0, got %d", usage.Memory)
		}
		return nil
	})

	// Now send twice the number of messages. Should receive an error at some point, and we will check usage against limits.
	var errSeen string
	for i := 0; i < toSend*2; i++ {
		resp, _ := nc.Request("LIMITS", []byte("The quick brown fox jumped over the..."), 50*time.Millisecond)
		if string(resp.Data) != OK {
			errSeen = string(resp.Data)
			break
		}
	}

	if errSeen == "" {
		t.Fatalf("Expected to see an error when exceeding the account limits")
	}

	state = mset.state()
	var lim JetStreamAccountLimits
	checkFor(t, time.Second, 15*time.Millisecond, func() error {
		usage = gacc.JetStreamUsage()
		lim = al[_EMPTY_]
		if usage.Memory > uint64(lim.MaxMemory) {
			return fmt.Errorf("Expected memory to not exceed limit of %d, got %d", lim.MaxMemory, usage.Memory)
		}
		return nil
	})

	// make sure that unlimited accounts work
	lim.MaxMemory = -1

	if err := gacc.UpdateJetStreamLimits(al); err != nil {
		t.Fatalf("Unexpected error updating jetstream account limits: %v", err)
	}

	for i := 0; i < toSend; i++ {
		sendStreamMsg(t, nc, "LIMITS", "Hello World!")
	}
}

func TestJetStreamStreamFileTrackingAndLimits(t *testing.T) {
	s := RunBasicJetStreamServer(t)
	defer s.Shutdown()

	gacc := s.GlobalAccount()

	al := map[string]JetStreamAccountLimits{
		_EMPTY_: {
			MaxMemory:    8192,
			MaxStore:     9600,
			MaxStreams:   -1,
			MaxConsumers: -1,
		},
	}

	if err := gacc.UpdateJetStreamLimits(al); err != nil {
		t.Fatalf("Unexpected error updating jetstream account limits: %v", err)
	}

	mconfig := &StreamConfig{Name: "LIMITS", Storage: FileStorage, Retention: WorkQueuePolicy}
	mset, err := gacc.addStream(mconfig)
	if err != nil {
		t.Fatalf("Unexpected error adding stream: %v", err)
	}
	defer mset.delete()

	nc := clientConnectToServer(t, s)
	defer nc.Close()

	toSend := 100
	for i := 0; i < toSend; i++ {
		sendStreamMsg(t, nc, "LIMITS", "Hello World!")
	}

	state := mset.state()
	usage := gacc.JetStreamUsage()

	// Make sure these are working correctly.
	if usage.Store != state.Bytes {
		t.Fatalf("Expected to have stream bytes match the store usage, %d vs %d", usage.Store, state.Bytes)
	}
	if usage.Streams != 1 {
		t.Fatalf("Expected to have 1 stream, got %d", usage.Streams)
	}

	// Do second stream.
	mconfig2 := &StreamConfig{Name: "NUM22", Storage: FileStorage, Retention: WorkQueuePolicy}
	mset2, err := gacc.addStream(mconfig2)
	if err != nil {
		t.Fatalf("Unexpected error adding stream: %v", err)
	}
	defer mset2.delete()

	for i := 0; i < toSend; i++ {
		sendStreamMsg(t, nc, "NUM22", "Hello World!")
	}

	stats2 := mset2.state()
	usage = gacc.JetStreamUsage()

	if usage.Store != (state.Bytes + stats2.Bytes) {
		t.Fatalf("Expected to track both streams, usage is %v, stream1 is %v, stream2 is %v", usage.Store, state.Bytes, stats2.Bytes)
	}

	// Make sure delete works.
	mset2.delete()
	stats2 = mset2.state()
	usage = gacc.JetStreamUsage()

	if usage.Store != (state.Bytes + stats2.Bytes) {
		t.Fatalf("Expected to track both streams, account is %v, stream1 is %v, stream2 is %v", usage.Store, state.Bytes, stats2.Bytes)
	}

	// Now drain the first one by consuming the messages.
	o, err := mset.addConsumer(workerModeConfig("WQ"))
	if err != nil {
		t.Fatalf("Expected no error with registered interest, got %v", err)
	}
	defer o.delete()

	for i := 0; i < toSend; i++ {
		msg, err := nc.Request(o.requestNextMsgSubject(), nil, time.Second)
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		msg.Respond(nil)
	}
	nc.Flush()

	state = mset.state()
	usage = gacc.JetStreamUsage()

	if usage.Memory != 0 {
		t.Fatalf("Expected usage memeory to be 0, got %d", usage.Memory)
	}

	// Now send twice the number of messages. Should receive an error at some point, and we will check usage against limits.
	var errSeen string
	for i := 0; i < toSend*2; i++ {
		resp, _ := nc.Request("LIMITS", []byte("The quick brown fox jumped over the..."), 50*time.Millisecond)
		if string(resp.Data) != OK {
			errSeen = string(resp.Data)
			break
		}
	}

	if errSeen == "" {
		t.Fatalf("Expected to see an error when exceeding the account limits")
	}

	state = mset.state()
	usage = gacc.JetStreamUsage()

	lim := al[_EMPTY_]
	if usage.Memory > uint64(lim.MaxMemory) {
		t.Fatalf("Expected memory to not exceed limit of %d, got %d", lim.MaxMemory, usage.Memory)
	}
}

func TestJetStreamTieredLimits(t *testing.T) {
	s := RunBasicJetStreamServer(t)
	defer s.Shutdown()

	gacc := s.GlobalAccount()

	tFail := map[string]JetStreamAccountLimits{
		"nottheer": {
			MaxMemory:    8192,
			MaxStore:     9600,
			MaxStreams:   -1,
			MaxConsumers: -1,
		},
	}

	if err := gacc.UpdateJetStreamLimits(tFail); err != nil {
		t.Fatalf("Unexpected error updating jetstream account limits: %v", err)
	}

	mconfig := &StreamConfig{Name: "LIMITS", Storage: FileStorage, Retention: WorkQueuePolicy}
	mset, err := gacc.addStream(mconfig)
	defer mset.delete()
	require_Error(t, err)
	require_Contains(t, err.Error(), "no JetStream default or applicable tiered limit present")

	tPass := map[string]JetStreamAccountLimits{
		"R1": {
			MaxMemory:    8192,
			MaxStore:     9600,
			MaxStreams:   -1,
			MaxConsumers: -1,
		},
	}

	if err := gacc.UpdateJetStreamLimits(tPass); err != nil {
		t.Fatalf("Unexpected error updating jetstream account limits: %v", err)
	}
}

func TestJetStreamDefaultMaxConsumers(t *testing.T) {
	storeDir := t.TempDir()
	confFmt := `
		listen: 127.0.0.1:-1
		jetstream: {
			store_dir: %q
			limits: {
				default_max_consumers: %d
			}
		}
	`
	conf := createConfFile(t, []byte(fmt.Sprintf(confFmt, storeDir, 2)))
	s, _ := RunServerWithConfig(conf)
	defer func() { s.Shutdown() }()

	nc, js := jsClientConnect(t, s)

	// Create consumers up to the limit of 2.
	_, err := js.AddStream(&nats.StreamConfig{Name: "S", Subjects: []string{"S.>"}})
	require_NoError(t, err)
	_, err = js.AddConsumer("S", &nats.ConsumerConfig{Durable: "C1", AckPolicy: nats.AckExplicitPolicy})
	require_NoError(t, err)
	_, err = js.AddConsumer("S", &nats.ConsumerConfig{Durable: "C2", AckPolicy: nats.AckExplicitPolicy})
	require_NoError(t, err)

	// The limit is now reached, so creating more is rejected.
	_, err = js.AddConsumer("S", &nats.ConsumerConfig{Durable: "C3", AckPolicy: nats.AckExplicitPolicy})
	require_Error(t, err, NewJSMaximumConsumersLimitError())

	// Updating or idempotently recreating existing assets does not add new
	// ones and must still be allowed while at the limit.
	_, err = js.AddConsumer("S", &nats.ConsumerConfig{Durable: "C2", AckPolicy: nats.AckExplicitPolicy})
	require_NoError(t, err)
	_, err = js.UpdateConsumer("S", &nats.ConsumerConfig{Durable: "C2", AckPolicy: nats.AckExplicitPolicy, MaxDeliver: 5})
	require_NoError(t, err)
	nc.Close()

	// Restart with the limit lowered to 1.
	s.Shutdown()
	conf = createConfFile(t, []byte(fmt.Sprintf(confFmt, storeDir, 1)))
	s, _ = RunServerWithConfig(conf)

	nc, js = jsClientConnect(t, s)
	defer nc.Close()

	checkFor(t, 2*time.Second, 100*time.Millisecond, func() error {
		if _, err = js.StreamInfo("S"); err != nil {
			return err
		}
		for _, dur := range []string{"C1", "C2"} {
			if _, err = js.ConsumerInfo("S", dur); err != nil {
				return err
			}
		}
		return nil
	})

	_, err = js.AddConsumer("S", &nats.ConsumerConfig{Durable: "C3", AckPolicy: nats.AckExplicitPolicy})
	require_Error(t, err, NewJSMaximumConsumersLimitError())
	// Ephemerals are new consumers as well.
	_, err = js.AddConsumer("S", &nats.ConsumerConfig{AckPolicy: nats.AckExplicitPolicy})
	require_Error(t, err, NewJSMaximumConsumersLimitError())

	// Existing assets can still be idempotently recreated and updated, even while
	// above the lowered limit, since that doesn't add new assets.
	_, err = js.AddConsumer("S", &nats.ConsumerConfig{Durable: "C2", AckPolicy: nats.AckExplicitPolicy, MaxDeliver: 5})
	require_NoError(t, err)
	_, err = js.UpdateConsumer("S", &nats.ConsumerConfig{Durable: "C2", AckPolicy: nats.AckExplicitPolicy, MaxDeliver: 10})
	require_NoError(t, err)
}

func TestJetStreamDefaultMaxConsumersSourcingExempt(t *testing.T) {
	conf := createConfFile(t, []byte(fmt.Sprintf(`
		listen: 127.0.0.1:-1
		jetstream: {
			store_dir: %q
			limits: {
				default_max_consumers: 1
			}
		}
	`, t.TempDir())))
	s, _ := RunServerWithConfig(conf)
	defer s.Shutdown()

	nc, js := jsClientConnect(t, s)
	defer nc.Close()

	_, err := js.AddStream(&nats.StreamConfig{Name: "S", Subjects: []string{"foo"}})
	require_NoError(t, err)

	// Fill up the default limit, so no regular consumers can be added anymore.
	_, err = js.AddConsumer("S", &nats.ConsumerConfig{Durable: "C1", AckPolicy: nats.AckExplicitPolicy})
	require_NoError(t, err)
	_, err = js.AddConsumer("S", &nats.ConsumerConfig{Durable: "C2", AckPolicy: nats.AckExplicitPolicy})
	require_Error(t, err, NewJSMaximumConsumersLimitError())

	// Mirroring/sourcing "S" creates internal Direct/Sourcing consumers on it,
	// those are not counted toward the limit and must not be rejected by it.
	_, err = js.AddStream(&nats.StreamConfig{Name: "M", Mirror: &nats.StreamSource{Name: "S"}})
	require_NoError(t, err)
	_, err = js.AddStream(&nats.StreamConfig{Name: "SRC", Sources: []*nats.StreamSource{{Name: "S"}}})
	require_NoError(t, err)

	sendStreamMsg(t, nc, "foo", "hello")
	checkFor(t, 5*time.Second, 100*time.Millisecond, func() error {
		for _, sname := range []string{"M", "SRC"} {
			si, err := js.StreamInfo(sname)
			if err != nil {
				return err
			}
			if si.State.Msgs != 1 {
				return fmt.Errorf("expected 1 message in %q, got %d", sname, si.State.Msgs)
			}
		}
		return nil
	})

	mset, err := s.globalAccount().lookupStream("S")
	require_NoError(t, err)
	require_Equal(t, mset.numConsumers(), 3)
	require_Equal(t, mset.numLimitableConsumers(), 1)

	// Regular consumers must still be rejected.
	_, err = js.AddConsumer("S", &nats.ConsumerConfig{Durable: "C2", AckPolicy: nats.AckExplicitPolicy})
	require_Error(t, err, NewJSMaximumConsumersLimitError())
}

func TestJetStreamDefaultMaxConsumersOnlyWithoutStreamOrAccountLimit(t *testing.T) {
	// The default only applies when neither the stream nor the account sets
	// a max consumers limit. If either is set, that takes precedence, even
	// when it's higher than the default.
	for _, test := range []struct {
		name       string
		accountMax int // 0 leaves the account limit unset, -1 is unlimited.
		streamMax  int // 0 leaves the stream limit unset, -1 is unlimited.
		expected   int
	}{
		{"neither set, default applies", 0, 0, 2},
		{"stream above default", 0, 4, 4},
		{"stream below default", 0, 1, 1},
		{"account above default", 4, 0, 4},
		{"account below default", 1, 0, 1},
		{"both set, stream lowest", 4, 3, 3},
		{"both set, equal", 3, 3, 3},
		// Unlimited counts as not set, the default can't be opted out of.
		{"stream unlimited, default applies", 0, -1, 2},
		{"account unlimited, default applies", -1, 0, 2},
		{"both unlimited, default applies", -1, -1, 2},
	} {
		t.Run(test.name, func(t *testing.T) {
			accountLimit := "enabled"
			if test.accountMax != 0 {
				accountLimit = fmt.Sprintf("{ max_consumers: %d }", test.accountMax)
			}
			conf := createConfFile(t, []byte(fmt.Sprintf(`
				listen: 127.0.0.1:-1
				jetstream: {
					store_dir: %q
					limits: {
						default_max_consumers: 2
					}
				}
				accounts {
					A {
						jetstream: %s
						users = [ { user: "a", pass: "pwd" } ]
					}
				}
			`, t.TempDir(), accountLimit)))
			s, _ := RunServerWithConfig(conf)
			defer s.Shutdown()

			nc, js := jsClientConnect(t, s, nats.UserInfo("a", "pwd"))
			defer nc.Close()

			_, err := js.AddStream(&nats.StreamConfig{Name: "S", Subjects: []string{"S.>"}, MaxConsumers: test.streamMax})
			require_NoError(t, err)
			for i := range test.expected {
				_, err = js.AddConsumer("S", &nats.ConsumerConfig{Durable: fmt.Sprintf("C%d", i), AckPolicy: nats.AckExplicitPolicy})
				require_NoError(t, err)
			}
			_, err = js.AddConsumer("S", &nats.ConsumerConfig{Durable: "OVER", AckPolicy: nats.AckExplicitPolicy})
			require_Error(t, err, NewJSMaximumConsumersLimitError())
		})
	}
}

type obsi struct {
	cfg ConsumerConfig
	ack int
}

type info struct {
	cfg   StreamConfig
	state StreamState
	obs   []obsi
}

func TestJetStreamSimpleFileRecovery(t *testing.T) {
	base := runtime.NumGoroutine()

	s := RunBasicJetStreamServer(t)
	defer s.Shutdown()

	acc := s.GlobalAccount()

	ostate := make(map[string]info)

	nid := nuid.New()
	randomSubject := func() string {
		nid.RandomizePrefix()
		return fmt.Sprintf("SUBJ.%s", nid.Next())
	}

	nc := clientConnectToServer(t, s)
	defer nc.Close()

	numStreams := 10
	for i := 1; i <= numStreams; i++ {
		msetName := fmt.Sprintf("MMS-%d", i)
		subjects := []string{randomSubject(), randomSubject(), randomSubject()}
		msetConfig := StreamConfig{
			Name:     msetName,
			Storage:  FileStorage,
			Subjects: subjects,
			MaxMsgs:  100,
		}
		mset, err := acc.addStream(&msetConfig)
		if err != nil {
			t.Fatalf("Unexpected error adding stream %q: %v", msetName, err)
		}

		toSend := rand.IntN(100) + 1
		for n := 1; n <= toSend; n++ {
			msg := fmt.Sprintf("Hello %d", n*i)
			subj := subjects[rand.IntN(len(subjects))]
			sendStreamMsg(t, nc, subj, msg)
		}
		// Create up to 5 consumers.
		numObs := rand.IntN(5) + 1
		var obs []obsi
		for n := 1; n <= numObs; n++ {
			oname := fmt.Sprintf("WQ-%d-%d", i, n)
			o, err := mset.addConsumer(workerModeConfig(oname))
			if err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}
			// Now grab some messages.
			toReceive := rand.IntN(toSend) + 1
			rsubj := o.requestNextMsgSubject()
			for r := 0; r < toReceive; r++ {
				resp, err := nc.Request(rsubj, nil, time.Second)
				require_NoError(t, err)
				if resp != nil {
					resp.Respond(nil)
				}
			}
			obs = append(obs, obsi{o.config(), toReceive})
		}
		ostate[msetName] = info{mset.config(), mset.state(), obs}
	}
	pusage := acc.JetStreamUsage()
	nc.Flush()

	// Shutdown and restart and make sure things come back.
	sd := s.JetStreamConfig().StoreDir
	s.Shutdown()

	checkFor(t, 2*time.Second, 200*time.Millisecond, func() error {
		delta := (runtime.NumGoroutine() - base)
		if delta > 3 {
			return fmt.Errorf("%d Go routines still exist post Shutdown()", delta)
		}
		return nil
	})

	s = RunJetStreamServerOnPort(-1, sd)
	defer s.Shutdown()

	acc = s.GlobalAccount()

	nusage := acc.JetStreamUsage()
	if !reflect.DeepEqual(nusage, pusage) {
		t.Fatalf("Usage does not match after restore: %+v vs %+v", nusage, pusage)
	}

	for mname, info := range ostate {
		mset, err := acc.lookupStream(mname)
		if err != nil {
			t.Fatalf("Expected to find a stream for %q", mname)
		}
		if state := mset.state(); !reflect.DeepEqual(state, info.state) {
			t.Fatalf("State does not match: %+v vs %+v", state, info.state)
		}
		if cfg := mset.config(); !reflect.DeepEqual(cfg, info.cfg) {
			t.Fatalf("Configs do not match: %+v vs %+v", cfg, info.cfg)
		}
		// Consumers.
		if mset.numConsumers() != len(info.obs) {
			t.Fatalf("Number of consumers do not match: %d vs %d", mset.numConsumers(), len(info.obs))
		}
		for _, oi := range info.obs {
			if o := mset.lookupConsumer(oi.cfg.Durable); o != nil {
				if uint64(oi.ack+1) != o.nextSeq() {
					t.Fatalf("Consumer next seq is not correct: %d vs %d", oi.ack+1, o.nextSeq())
				}
			} else {
				t.Fatalf("Expected to get an consumer")
			}
		}
	}
}

func TestJetStreamPushConsumerFlowControl(t *testing.T) {
	s := RunBasicJetStreamServer(t)
	defer s.Shutdown()

	// Client for API requests.
	nc, js := jsClientConnect(t, s)
	defer nc.Close()

	if _, err := js.AddStream(&nats.StreamConfig{Name: "TEST"}); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	sub, err := nc.SubscribeSync(nats.NewInbox())
	require_NoError(t, err)
	defer sub.Unsubscribe()

	obsReq := CreateConsumerRequest{
		Stream: "TEST",
		Config: ConsumerConfig{
			Durable:        "dlc",
			DeliverSubject: sub.Subject,
			FlowControl:    true,
			Heartbeat:      5 * time.Second,
		},
	}
	req, err := json.Marshal(obsReq)
	require_NoError(t, err)
	resp, err := nc.Request(fmt.Sprintf(JSApiDurableCreateT, "TEST", "dlc"), req, time.Second)
	require_NoError(t, err)
	var ccResp JSApiConsumerCreateResponse
	if err = json.Unmarshal(resp.Data, &ccResp); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if ccResp.Error != nil {
		t.Fatalf("Unexpected error: %+v", ccResp.Error)
	}

	// Grab the low level consumer so we can manually set the fc max.
	if mset, err := s.GlobalAccount().lookupStream("TEST"); err != nil {
		t.Fatalf("Error looking up stream: %v", err)
	} else if obs := mset.lookupConsumer("dlc"); obs == nil {
		t.Fatalf("Error looking up stream: %v", err)
	} else {
		obs.mu.Lock()
		obs.setMaxPendingBytes(16 * 1024)
		obs.mu.Unlock()
	}

	msgSize := 1024
	msg := make([]byte, msgSize)
	crand.Read(msg)

	sendBatch := func(n int) {
		for i := 0; i < n; i++ {
			if _, err := js.Publish("TEST", msg); err != nil {
				t.Fatalf("Unexpected publish error: %v", err)
			}
		}
	}

	checkSubPending := func(numExpected int) {
		t.Helper()
		checkFor(t, time.Second, 100*time.Millisecond, func() error {
			if nmsgs, _, err := sub.Pending(); err != nil || nmsgs != numExpected {
				return fmt.Errorf("Did not receive correct number of messages: %d vs %d", nmsgs, numExpected)
			}
			return nil
		})
	}

	sendBatch(100)
	checkSubPending(2) // First four data and flowcontrol from slow start pause.

	var n int
	for m, err := sub.NextMsg(time.Second); err == nil; m, err = sub.NextMsg(time.Second) {
		if m.Subject == "TEST" {
			n++
		} else {
			// This should be a FC control message.
			if m.Header.Get("Status") != "100" {
				t.Fatalf("Expected a 100 status code, got %q", m.Header.Get("Status"))
			}
			if m.Header.Get("Description") != "FlowControl Request" {
				t.Fatalf("Wrong description, got %q", m.Header.Get("Description"))
			}
			m.Respond(nil)
		}
	}

	if n != 100 {
		t.Fatalf("Expected to receive all 100 messages but got %d", n)
	}
}

func TestJetStreamFlowControlRequiresHeartbeats(t *testing.T) {
	s := RunBasicJetStreamServer(t)
	defer s.Shutdown()

	nc, js := jsClientConnect(t, s)
	defer nc.Close()

	if _, err := js.AddStream(&nats.StreamConfig{Name: "TEST"}); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	if _, err := js.AddConsumer("TEST", &nats.ConsumerConfig{
		Durable:        "dlc",
		DeliverSubject: nats.NewInbox(),
		FlowControl:    true,
	}); err == nil || IsNatsErr(err, JSConsumerWithFlowControlNeedsHeartbeats) {
		t.Fatalf("Unexpected error: %v", err)
	}
}

func TestJetStreamPushConsumerIdleHeartbeats(t *testing.T) {
	s := RunBasicJetStreamServer(t)
	defer s.Shutdown()

	// Client for API requests.
	nc, js := jsClientConnect(t, s)
	defer nc.Close()

	if _, err := js.AddStream(&nats.StreamConfig{Name: "TEST"}); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	sub, err := nc.SubscribeSync(nats.NewInbox())
	require_NoError(t, err)
	defer sub.Unsubscribe()

	// Test errors first
	obsReq := CreateConsumerRequest{
		Stream: "TEST",
		Config: ConsumerConfig{
			DeliverSubject: sub.Subject,
			Heartbeat:      time.Millisecond,
		},
	}
	req, err := json.Marshal(obsReq)
	require_NoError(t, err)
	resp, err := nc.Request(fmt.Sprintf(JSApiConsumerCreateT, "TEST"), req, time.Second)
	require_NoError(t, err)
	var ccResp JSApiConsumerCreateResponse
	if err = json.Unmarshal(resp.Data, &ccResp); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if ccResp.Error == nil {
		t.Fatalf("Expected an error, got none")
	}
	// Set acceptable heartbeat.
	obsReq.Config.Heartbeat = 100 * time.Millisecond
	req, err = json.Marshal(obsReq)
	require_NoError(t, err)
	resp, err = nc.Request(fmt.Sprintf(JSApiConsumerCreateT, "TEST"), req, time.Second)
	require_NoError(t, err)
	ccResp.Error = nil
	if err = json.Unmarshal(resp.Data, &ccResp); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	checkFor(t, time.Second, 20*time.Millisecond, func() error {
		if nmsgs, _, err := sub.Pending(); err != nil || nmsgs < 9 {
			return fmt.Errorf("Did not receive correct number of messages: %d vs %d", nmsgs, 9)
		}
		return nil
	})
	m, _ := sub.NextMsg(0)
	if m.Header.Get("Status") != "100" {
		t.Fatalf("Expected a 100 status code, got %q", m.Header.Get("Status"))
	}
	if m.Header.Get("Description") != "Idle Heartbeat" {
		t.Fatalf("Wrong description, got %q", m.Header.Get("Description"))
	}
}

func TestJetStreamPushConsumerIdleHeartbeatsWithFilterSubject(t *testing.T) {
	s := RunBasicJetStreamServer(t)
	defer s.Shutdown()

	// Client for API requests.
	nc, js := jsClientConnect(t, s)
	defer nc.Close()

	if _, err := js.AddStream(&nats.StreamConfig{Name: "TEST", Subjects: []string{"foo", "bar"}}); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	hbC := make(chan *nats.Msg, 8)
	sub, err := nc.ChanSubscribe(nats.NewInbox(), hbC)
	require_NoError(t, err)
	defer sub.Unsubscribe()

	obsReq := CreateConsumerRequest{
		Stream: "TEST",
		Config: ConsumerConfig{
			DeliverSubject: sub.Subject,
			FilterSubject:  "bar",
			Heartbeat:      100 * time.Millisecond,
		},
	}

	req, err := json.Marshal(obsReq)
	require_NoError(t, err)
	resp, err := nc.Request(fmt.Sprintf(JSApiConsumerCreateT, "TEST"), req, time.Second)
	require_NoError(t, err)
	var ccResp JSApiConsumerCreateResponse
	if err = json.Unmarshal(resp.Data, &ccResp); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	st := time.NewTicker(10 * time.Millisecond)
	defer st.Stop()

	done := time.NewTimer(time.Second)
	defer done.Stop()

	for {
		select {
		case <-st.C:
			js.Publish("foo", []byte("HELLO FOO"))
		case <-done.C:
			t.Fatalf("Expected to have seen idle heartbeats for consumer")
		case <-hbC:
			return
		}
	}
}

func TestJetStreamPushConsumerIdleHeartbeatsWithNoInterest(t *testing.T) {
	s := RunBasicJetStreamServer(t)
	defer s.Shutdown()

	// Client for API requests.
	nc, js := jsClientConnect(t, s)
	defer nc.Close()

	if _, err := js.AddStream(&nats.StreamConfig{Name: "TEST"}); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	dsubj := "d.22"
	hbC := make(chan *nats.Msg, 8)
	sub, err := nc.ChanSubscribe("d.>", hbC)
	require_NoError(t, err)
	defer sub.Unsubscribe()

	obsReq := CreateConsumerRequest{
		Stream: "TEST",
		Config: ConsumerConfig{
			DeliverSubject: dsubj,
			Heartbeat:      100 * time.Millisecond,
		},
	}

	req, err := json.Marshal(obsReq)
	require_NoError(t, err)
	resp, err := nc.Request(fmt.Sprintf(JSApiConsumerCreateT, "TEST"), req, time.Second)
	require_NoError(t, err)
	var ccResp JSApiConsumerCreateResponse
	if err = json.Unmarshal(resp.Data, &ccResp); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if ccResp.Error != nil {
		t.Fatalf("Unexpected error: %+v", ccResp.Error)
	}

	done := time.NewTimer(400 * time.Millisecond)
	defer done.Stop()

	for {
		select {
		case <-done.C:
			return
		case m := <-hbC:
			if m.Header.Get("Status") == "100" {
				t.Fatalf("Did not expect to see a heartbeat with no formal interest")
			}
		}
	}
}

func TestJetStreamInfoAPIWithHeaders(t *testing.T) {
	s := RunBasicJetStreamServer(t)
	defer s.Shutdown()

	// Client for API requests.
	nc := clientConnectToServer(t, s)
	defer nc.Close()

	m := nats.NewMsg(JSApiAccountInfo)
	m.Header.Add("Accept-Encoding", "json")
	m.Header.Add("Authorization", "s3cr3t")
	m.Data = []byte("HELLO-JS!")

	resp, err := nc.RequestMsg(m, time.Second)
	require_NoError(t, err)

	var info JSApiAccountInfoResponse
	if err := json.Unmarshal(resp.Data, &info); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if info.Error != nil {
		t.Fatalf("Received an error: %+v", info.Error)
	}
}

func TestJetStreamRequestAPI(t *testing.T) {
	s := RunBasicJetStreamServer(t)
	defer s.Shutdown()

	// Client for API requests.
	nc := clientConnectToServer(t, s)
	defer nc.Close()

	// This will get the current information about usage and limits for this account.
	resp, err := nc.Request(JSApiAccountInfo, nil, time.Second)
	require_NoError(t, err)
	var info JSApiAccountInfoResponse
	if err := json.Unmarshal(resp.Data, &info); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	// JS API level should be set.
	require_Equal(t, info.API.Level, JSApiLevel)

	// Now create a stream.
	msetCfg := StreamConfig{
		Name:     "MSET22",
		Storage:  FileStorage,
		Subjects: []string{"foo", "bar", "baz"},
		MaxMsgs:  100,
	}
	req, err := json.Marshal(msetCfg)
	require_NoError(t, err)
	resp, _ = nc.Request(fmt.Sprintf(JSApiStreamCreateT, msetCfg.Name), req, time.Second)
	var scResp JSApiStreamCreateResponse
	if err := json.Unmarshal(resp.Data, &scResp); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if scResp.StreamInfo == nil || scResp.Error != nil {
		t.Fatalf("Did not receive correct response: %+v", scResp.Error)
	}
	if time.Since(scResp.Created) > time.Second {
		t.Fatalf("Created time seems wrong: %v\n", scResp.Created)
	}

	// Check that the name in config has to match the name in the subject
	resp, _ = nc.Request(fmt.Sprintf(JSApiStreamCreateT, "BOB"), req, time.Second)
	scResp.Error, scResp.StreamInfo = nil, nil
	if err := json.Unmarshal(resp.Data, &scResp); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	checkNatsError(t, scResp.Error, JSStreamMismatchErr)

	// Check that update works.
	msetCfg.Subjects = []string{"foo", "bar", "baz"}
	msetCfg.MaxBytes = 2222222
	req, err = json.Marshal(msetCfg)
	require_NoError(t, err)
	resp, _ = nc.Request(fmt.Sprintf(JSApiStreamUpdateT, msetCfg.Name), req, time.Second)
	scResp.Error, scResp.StreamInfo = nil, nil
	if err := json.Unmarshal(resp.Data, &scResp); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if scResp.StreamInfo == nil || scResp.Error != nil {
		t.Fatalf("Did not receive correct response: %+v", scResp.Error)
	}

	// Check that updating a non existing stream fails
	cfg := StreamConfig{
		Name:     "UNKNOWN_STREAM",
		Storage:  FileStorage,
		Subjects: []string{"foo"},
	}
	req, err = json.Marshal(cfg)
	require_NoError(t, err)
	resp, _ = nc.Request(fmt.Sprintf(JSApiStreamUpdateT, cfg.Name), req, time.Second)
	scResp.Error, scResp.StreamInfo = nil, nil
	if err := json.Unmarshal(resp.Data, &scResp); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if scResp.StreamInfo != nil || scResp.Error == nil || scResp.Error.Code != 404 {
		t.Fatalf("Unexpected error: %+v", scResp.Error)
	}

	// Now lookup info again and see that we can see the new stream.
	resp, err = nc.Request(JSApiAccountInfo, nil, time.Second)
	require_NoError(t, err)
	if err = json.Unmarshal(resp.Data, &info); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if info.Streams != 1 {
		t.Fatalf("Expected to see 1 Stream, got %d", info.Streams)
	}

	// Make sure list names works.
	resp, err = nc.Request(JSApiStreams, nil, time.Second)
	require_NoError(t, err)
	var namesResponse JSApiStreamNamesResponse
	if err = json.Unmarshal(resp.Data, &namesResponse); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	if len(namesResponse.Streams) != 1 {
		t.Fatalf("Expected only 1 stream but got %d", len(namesResponse.Streams))
	}
	if namesResponse.Total != 1 {
		t.Fatalf("Expected total to be 1 but got %d", namesResponse.Total)
	}
	if namesResponse.Offset != 0 {
		t.Fatalf("Expected offset to be 0 but got %d", namesResponse.Offset)
	}
	if namesResponse.Limit != JSApiNamesLimit {
		t.Fatalf("Expected limit to be %d but got %d", JSApiNamesLimit, namesResponse.Limit)
	}
	if namesResponse.Streams[0] != msetCfg.Name {
		t.Fatalf("Expected to get %q, but got %q", msetCfg.Name, namesResponse.Streams[0])
	}

	// Now do detailed version.
	resp, err = nc.Request(JSApiStreamList, nil, time.Second)
	require_NoError(t, err)
	var listResponse JSApiStreamListResponse
	if err = json.Unmarshal(resp.Data, &listResponse); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	if len(listResponse.Streams) != 1 {
		t.Fatalf("Expected only 1 stream but got %d", len(listResponse.Streams))
	}
	if listResponse.Total != 1 {
		t.Fatalf("Expected total to be 1 but got %d", listResponse.Total)
	}
	if listResponse.Offset != 0 {
		t.Fatalf("Expected offset to be 0 but got %d", listResponse.Offset)
	}
	if listResponse.Limit != JSApiListLimit {
		t.Fatalf("Expected limit to be %d but got %d", JSApiListLimit, listResponse.Limit)
	}
	if listResponse.Streams[0].Config.Name != msetCfg.Name {
		t.Fatalf("Expected to get %q, but got %q", msetCfg.Name, listResponse.Streams[0].Config.Name)
	}

	// Now send some messages, then we can poll for info on this stream.
	toSend := 10
	for i := 0; i < toSend; i++ {
		nc.Request("foo", []byte("WELCOME JETSTREAM"), time.Second)
	}

	resp, err = nc.Request(fmt.Sprintf(JSApiStreamInfoT, msetCfg.Name), nil, time.Second)
	require_NoError(t, err)
	var msi StreamInfo
	if err = json.Unmarshal(resp.Data, &msi); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if msi.State.Msgs != uint64(toSend) {
		t.Fatalf("Expected to get %d msgs, got %d", toSend, msi.State.Msgs)
	}
	if time.Since(msi.Created) > time.Second {
		t.Fatalf("Created time seems wrong: %v\n", msi.Created)
	}

	// Looking up one that is not there should yield an error.
	resp, err = nc.Request(fmt.Sprintf(JSApiStreamInfoT, "BOB"), nil, time.Second)
	require_NoError(t, err)
	var bResp JSApiStreamInfoResponse
	if err = json.Unmarshal(resp.Data, &bResp); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	checkNatsError(t, bResp.Error, JSStreamNotFoundErr)

	// Now create a consumer.
	delivery := nats.NewInbox()
	obsReq := CreateConsumerRequest{
		Stream: msetCfg.Name,
		Config: ConsumerConfig{DeliverSubject: delivery},
	}
	req, err = json.Marshal(obsReq)
	require_NoError(t, err)
	resp, err = nc.Request(fmt.Sprintf(JSApiConsumerCreateT, msetCfg.Name), req, time.Second)
	require_NoError(t, err)
	var ccResp JSApiConsumerCreateResponse
	if err = json.Unmarshal(resp.Data, &ccResp); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	// Ephemerals are now not rejected when there is no interest.
	if ccResp.ConsumerInfo == nil || ccResp.Error != nil {
		t.Fatalf("Got a bad response %+v", ccResp)
	}
	if time.Since(ccResp.Created) > time.Second {
		t.Fatalf("Created time seems wrong: %v\n", ccResp.Created)
	}

	// Now create subscription and make sure we get proper response.
	sub, _ := nc.SubscribeSync(delivery)
	nc.Flush()

	checkFor(t, 250*time.Millisecond, 10*time.Millisecond, func() error {
		if nmsgs, _, _ := sub.Pending(); err != nil || nmsgs != toSend {
			return fmt.Errorf("Did not receive correct number of messages: %d vs %d", nmsgs, toSend)
		}
		return nil
	})

	// Check that we get an error if the stream name in the subject does not match the config.
	resp, err = nc.Request(fmt.Sprintf(JSApiConsumerCreateT, "BOB"), req, time.Second)
	require_NoError(t, err)
	ccResp.Error, ccResp.ConsumerInfo = nil, nil
	if err = json.Unmarshal(resp.Data, &ccResp); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	// Since we do not have interest this should have failed.
	checkNatsError(t, ccResp.Error, JSStreamMismatchErr)

	// Get the list of all of the consumers for our stream.
	resp, err = nc.Request(fmt.Sprintf(JSApiConsumersT, msetCfg.Name), nil, time.Second)
	require_NoError(t, err)
	var clResponse JSApiConsumerNamesResponse
	if err = json.Unmarshal(resp.Data, &clResponse); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if len(clResponse.Consumers) != 1 {
		t.Fatalf("Expected only 1 consumer but got %d", len(clResponse.Consumers))
	}
	// Now let's get info about our consumer.
	cName := clResponse.Consumers[0]
	resp, err = nc.Request(fmt.Sprintf(JSApiConsumerInfoT, msetCfg.Name, cName), nil, time.Second)
	require_NoError(t, err)
	var oinfo ConsumerInfo
	if err = json.Unmarshal(resp.Data, &oinfo); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	// Do some sanity checking.
	// Must match consumer.go
	const randConsumerNameLen = 8
	if len(oinfo.Name) != randConsumerNameLen {
		t.Fatalf("Expected ephemeral name, got %q", oinfo.Name)
	}
	if len(oinfo.Config.Durable) != 0 {
		t.Fatalf("Expected no durable name, but got %q", oinfo.Config.Durable)
	}
	if oinfo.Config.DeliverSubject != delivery {
		t.Fatalf("Expected to have delivery subject of %q, got %q", delivery, oinfo.Config.DeliverSubject)
	}
	if oinfo.Delivered.Consumer != 10 {
		t.Fatalf("Expected consumer delivered sequence of 10, got %d", oinfo.Delivered.Consumer)
	}
	if oinfo.AckFloor.Consumer != 10 {
		t.Fatalf("Expected ack floor to be 10, got %d", oinfo.AckFloor.Consumer)
	}

	// Now delete the consumer.
	resp, _ = nc.Request(fmt.Sprintf(JSApiConsumerDeleteT, msetCfg.Name, cName), nil, time.Second)
	var cdResp JSApiConsumerDeleteResponse
	if err = json.Unmarshal(resp.Data, &cdResp); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if !cdResp.Success || cdResp.Error != nil {
		t.Fatalf("Got a bad response %+v", ccResp)
	}

	// Make sure we can't create a durable using the ephemeral API endpoint.
	obsReq = CreateConsumerRequest{
		Stream: msetCfg.Name,
		Config: ConsumerConfig{Durable: "myd", DeliverSubject: delivery},
	}
	req, err = json.Marshal(obsReq)
	require_NoError(t, err)
	resp, err = nc.Request(fmt.Sprintf(JSApiConsumerCreateT, msetCfg.Name), req, time.Second)
	require_NoError(t, err)
	ccResp.Error, ccResp.ConsumerInfo = nil, nil
	if err = json.Unmarshal(resp.Data, &ccResp); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	checkNatsError(t, ccResp.Error, JSConsumerEphemeralWithDurableNameErr)

	// Now make sure we can create a durable on the subject with the proper name.
	resp, err = nc.Request(fmt.Sprintf(JSApiDurableCreateT, msetCfg.Name, obsReq.Config.Durable), req, time.Second)
	ccResp.Error, ccResp.ConsumerInfo = nil, nil
	if err = json.Unmarshal(resp.Data, &ccResp); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if ccResp.ConsumerInfo == nil || ccResp.Error != nil {
		t.Fatalf("Did not receive correct response")
	}

	// Make sure empty durable in cfg does not work
	obsReq2 := CreateConsumerRequest{
		Stream: msetCfg.Name,
		Config: ConsumerConfig{DeliverSubject: delivery},
	}
	req2, err := json.Marshal(obsReq2)
	require_NoError(t, err)
	resp, err = nc.Request(fmt.Sprintf(JSApiDurableCreateT, msetCfg.Name, obsReq.Config.Durable), req2, time.Second)
	require_NoError(t, err)
	ccResp.Error, ccResp.ConsumerInfo = nil, nil
	if err = json.Unmarshal(resp.Data, &ccResp); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	checkNatsError(t, ccResp.Error, JSConsumerDurableNameNotSetErr)

	// Now delete a msg.
	dreq := JSApiMsgDeleteRequest{Seq: 2}
	dreqj, err := json.Marshal(dreq)
	require_NoError(t, err)
	resp, _ = nc.Request(fmt.Sprintf(JSApiMsgDeleteT, msetCfg.Name), dreqj, time.Second)
	var delMsgResp JSApiMsgDeleteResponse
	if err = json.Unmarshal(resp.Data, &delMsgResp); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if !delMsgResp.Success || delMsgResp.Error != nil {
		t.Fatalf("Got a bad response %+v", delMsgResp.Error)
	}

	// Now purge the stream.
	resp, _ = nc.Request(fmt.Sprintf(JSApiStreamPurgeT, msetCfg.Name), nil, time.Second)
	var pResp JSApiStreamPurgeResponse
	if err = json.Unmarshal(resp.Data, &pResp); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if !pResp.Success || pResp.Error != nil {
		t.Fatalf("Got a bad response %+v", pResp)
	}
	if pResp.Purged != 9 {
		t.Fatalf("Expected 9 purged, got %d", pResp.Purged)
	}

	// Now delete the stream.
	resp, _ = nc.Request(fmt.Sprintf(JSApiStreamDeleteT, msetCfg.Name), nil, time.Second)
	var dResp JSApiStreamDeleteResponse
	if err = json.Unmarshal(resp.Data, &dResp); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if !dResp.Success || dResp.Error != nil {
		t.Fatalf("Got a bad response %+v", dResp.Error)
	}

	// Now grab stats again.
	// This will get the current information about usage and limits for this account.
	resp, err = nc.Request(JSApiAccountInfo, nil, time.Second)
	require_NoError(t, err)
	if err := json.Unmarshal(resp.Data, &info); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if info.Streams != 0 {
		t.Fatalf("Expected no remaining streams, got %d", info.Streams)
	}

	// Test that we can send nil or an empty legal json for requests that take no args.
	// We know this stream does not exist, this just checking request processing.
	checkEmptyReqArg := func(arg string) {
		t.Helper()
		var req []byte
		if len(arg) > 0 {
			req = []byte(arg)
		}
		resp, err = nc.Request(fmt.Sprintf(JSApiStreamDeleteT, "foo_bar_baz"), req, time.Second)
		var dResp JSApiStreamDeleteResponse
		if err = json.Unmarshal(resp.Data, &dResp); err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		if dResp.Error == nil || dResp.Error.Code != 404 {
			t.Fatalf("Got a bad response, expected a 404 response %+v", dResp.Error)
		}
	}

	checkEmptyReqArg("")
	checkEmptyReqArg("{}")
	checkEmptyReqArg(" {} ")
	checkEmptyReqArg(" { } ")
}

func TestJetStreamFilteredStreamNames(t *testing.T) {
	s := RunBasicJetStreamServer(t)
	defer s.Shutdown()

	// Client for API requests.
	nc := clientConnectToServer(t, s)
	defer nc.Close()

	// Create some streams.
	var snid int
	createStream := func(subjects []string) {
		t.Helper()
		snid++
		name := fmt.Sprintf("S-%d", snid)
		sc := &StreamConfig{Name: name, Subjects: subjects}
		if _, err := s.GlobalAccount().addStream(sc); err != nil {
			t.Fatalf("Unexpected error adding stream: %v", err)
		}
	}

	createStream([]string{"foo"})                  // S1
	createStream([]string{"bar"})                  // S2
	createStream([]string{"baz"})                  // S3
	createStream([]string{"foo.*", "bar.*"})       // S4
	createStream([]string{"foo-1.22", "bar-1.33"}) // S5

	expectStreams := func(filter string, streams []string) {
		t.Helper()
		req, _ := json.Marshal(&JSApiStreamNamesRequest{Subject: filter})
		r, _ := nc.Request(JSApiStreams, req, time.Second)
		var resp JSApiStreamNamesResponse
		if err := json.Unmarshal(r.Data, &resp); err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		if len(resp.Streams) != len(streams) {
			t.Fatalf("Expected %d results, got %d", len(streams), len(resp.Streams))
		}
	}

	expectStreams("foo", []string{"S1"})
	expectStreams("bar", []string{"S2"})
	expectStreams("baz", []string{"S3"})
	expectStreams("*", []string{"S1", "S2", "S3"})
	expectStreams(">", []string{"S1", "S2", "S3", "S4", "S5"})
	expectStreams("*.*", []string{"S4", "S5"})
	expectStreams("*.22", []string{"S4", "S5"})
}

func TestJetStreamUpdateStream(t *testing.T) {
	cases := []struct {
		name    string
		mconfig *StreamConfig
	}{
		{name: "MemoryStore",
			mconfig: &StreamConfig{
				Name:      "foo",
				Retention: LimitsPolicy,
				MaxAge:    time.Hour,
				Storage:   MemoryStorage,
				Replicas:  1,
			}},
		{name: "FileStore",
			mconfig: &StreamConfig{
				Name:      "foo",
				Retention: LimitsPolicy,
				MaxAge:    time.Hour,
				Storage:   FileStorage,
				Replicas:  1,
			}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			s := RunBasicJetStreamServer(t)
			defer s.Shutdown()

			mset, err := s.GlobalAccount().addStream(c.mconfig)
			if err != nil {
				t.Fatalf("Unexpected error adding stream: %v", err)
			}
			defer mset.delete()

			// Test basic updates. We allow changing the subjects, limits, and no_ack along with replicas(TBD w/ cluster)
			cfg := *c.mconfig

			// Can't change name.
			cfg.Name = "bar"
			if err := mset.update(&cfg); err == nil || !strings.Contains(err.Error(), "name must match") {
				t.Fatalf("Expected error trying to update name")
			}
			// Can't change storage types.
			cfg = *c.mconfig
			if cfg.Storage == FileStorage {
				cfg.Storage = MemoryStorage
			} else {
				cfg.Storage = FileStorage
			}
			if err := mset.update(&cfg); err == nil || !strings.Contains(err.Error(), "can not change") {
				t.Fatalf("Expected error trying to change Storage")
			}
			// Can't change replicas > 1 for now.
			cfg = *c.mconfig
			cfg.Replicas = 10
			if err := mset.update(&cfg); err == nil || !strings.Contains(err.Error(), "maximum replicas") {
				t.Fatalf("Expected error trying to change Replicas")
			}
			// Can't change limits policy.
			cfg = *c.mconfig
			cfg.Retention = WorkQueuePolicy
			if err := mset.update(&cfg); err == nil || !strings.Contains(err.Error(), "can not change") {
				t.Fatalf("Expected error trying to change Retention")
			}

			// Now test changing limits.
			nc := clientConnectToServer(t, s)
			defer nc.Close()

			pending := uint64(100)
			for i := uint64(0); i < pending; i++ {
				sendStreamMsg(t, nc, "foo", "0123456789")
			}
			pendingBytes := mset.state().Bytes

			checkPending := func(msgs, bts uint64) {
				t.Helper()
				state := mset.state()
				if state.Msgs != msgs {
					t.Fatalf("Expected %d messages, got %d", msgs, state.Msgs)
				}
				if state.Bytes != bts {
					t.Fatalf("Expected %d bytes, got %d", bts, state.Bytes)
				}
			}
			checkPending(pending, pendingBytes)

			// Update msgs to higher.
			cfg = *c.mconfig
			cfg.MaxMsgs = int64(pending * 2)
			if err := mset.update(&cfg); err != nil {
				t.Fatalf("Unexpected error %v", err)
			}
			if mset.config().MaxMsgs != cfg.MaxMsgs {
				t.Fatalf("Expected the change to take effect, %d vs %d", mset.config().MaxMsgs, cfg.MaxMsgs)
			}
			checkPending(pending, pendingBytes)

			// Update msgs to lower.
			cfg = *c.mconfig
			cfg.MaxMsgs = int64(pending / 2)
			if err := mset.update(&cfg); err != nil {
				t.Fatalf("Unexpected error %v", err)
			}
			if mset.config().MaxMsgs != cfg.MaxMsgs {
				t.Fatalf("Expected the change to take effect, %d vs %d", mset.config().MaxMsgs, cfg.MaxMsgs)
			}
			checkPending(pending/2, pendingBytes/2)
			// Now do bytes.
			cfg = *c.mconfig
			cfg.MaxBytes = int64(pendingBytes / 4)
			if err := mset.update(&cfg); err != nil {
				t.Fatalf("Unexpected error %v", err)
			}
			if mset.config().MaxBytes != cfg.MaxBytes {
				t.Fatalf("Expected the change to take effect, %d vs %d", mset.config().MaxBytes, cfg.MaxBytes)
			}
			checkPending(pending/4, pendingBytes/4)

			// Now do age.
			cfg = *c.mconfig
			cfg.MaxAge = time.Second
			if err := mset.update(&cfg); err != nil {
				t.Fatalf("Unexpected error %v", err)
			}
			// Just wait a bit for expiration.
			time.Sleep(2 * time.Second)
			if mset.config().MaxAge != cfg.MaxAge {
				t.Fatalf("Expected the change to take effect, %d vs %d", mset.config().MaxAge, cfg.MaxAge)
			}
			checkPending(0, 0)

			// Now put back to original.
			cfg = *c.mconfig
			if err := mset.update(&cfg); err != nil {
				t.Fatalf("Unexpected error %v", err)
			}
			for i := uint64(0); i < pending; i++ {
				sendStreamMsg(t, nc, "foo", "0123456789")
			}

			// subject changes.
			// Add in a subject first.
			cfg = *c.mconfig
			cfg.Subjects = []string{"foo", "bar"}
			if err := mset.update(&cfg); err != nil {
				t.Fatalf("Unexpected error %v", err)
			}
			// Make sure we can still send to foo.
			sendStreamMsg(t, nc, "foo", "0123456789")
			// And we can now send to bar.
			sendStreamMsg(t, nc, "bar", "0123456789")
			// Now delete both and change to baz only.
			cfg.Subjects = []string{"baz"}
			if err := mset.update(&cfg); err != nil {
				t.Fatalf("Unexpected error %v", err)
			}
			// Make sure we do not get response acks for "foo" or "bar".
			if resp, err := nc.Request("foo", nil, 25*time.Millisecond); err == nil || resp != nil {
				t.Fatalf("Expected no response from jetstream for deleted subject: %q", "foo")
			}
			if resp, err := nc.Request("bar", nil, 25*time.Millisecond); err == nil || resp != nil {
				t.Fatalf("Expected no response from jetstream for deleted subject: %q", "bar")
			}
			// Make sure we can send to "baz"
			sendStreamMsg(t, nc, "baz", "0123456789")
			if nmsgs := mset.state().Msgs; nmsgs != pending+3 {
				t.Fatalf("Expected %d msgs, got %d", pending+3, nmsgs)
			}

			// FileStore restarts for config save.
			cfg = *c.mconfig
			if cfg.Storage == FileStorage {
				cfg.Subjects = []string{"foo", "bar"}
				cfg.MaxMsgs = 2222
				cfg.MaxBytes = 3333333
				cfg.MaxAge = 22 * time.Hour
				if err := mset.update(&cfg); err != nil {
					t.Fatalf("Unexpected error %v", err)
				}
				// Pull since certain defaults etc are set in processing.
				cfg = mset.config()

				// Restart the
				// Capture port since it was dynamic.
				u, _ := url.Parse(s.ClientURL())
				port, _ := strconv.Atoi(u.Port())

				// Stop current
				sd := s.JetStreamConfig().StoreDir
				s.Shutdown()
				// Restart.
				s = RunJetStreamServerOnPort(port, sd)
				defer s.Shutdown()

				mset, err = s.GlobalAccount().lookupStream(cfg.Name)
				if err != nil {
					t.Fatalf("Expected to find a stream for %q", cfg.Name)
				}
				restored_cfg := mset.config()
				if !reflect.DeepEqual(cfg, restored_cfg) {
					t.Fatalf("restored configuration does not match: \n%+v\n vs \n%+v", restored_cfg, cfg)
				}
			}
		})
	}
}

func TestJetStreamFileStreamCreatedTimePreservedAfterRestartAndUpdate(t *testing.T) {
	s := RunBasicJetStreamServer(t)
	defer func() { s.Shutdown() }()

	nc, js := jsClientConnect(t, s)
	defer func() { nc.Close() }()
	si, err := js.AddStream(&nats.StreamConfig{Name: "TEST", Storage: nats.FileStorage})
	require_NoError(t, err)
	created := si.Created

	sd := s.JetStreamConfig().StoreDir
	nc.Close()
	s.Shutdown()
	time.Sleep(10 * time.Millisecond)

	s = RunJetStreamServerOnPort(-1, sd)
	nc, js = jsClientConnect(t, s)
	si, err = js.StreamInfo("TEST")
	require_NoError(t, err)
	if !si.Created.Equal(created) {
		t.Fatalf("Expected created time %v after restart, got %v", created, si.Created)
	}

	cfg := si.Config
	cfg.MaxMsgs = 1
	_, err = js.UpdateStream(&cfg)
	require_NoError(t, err)
	nc.Close()
	s.Shutdown()

	s = RunJetStreamServerOnPort(-1, sd)
	nc, js = jsClientConnect(t, s)
	si, err = js.StreamInfo("TEST")
	require_NoError(t, err)
	if !si.Created.Equal(created) {
		t.Fatalf("Expected created time %v after restart and update, got %v", created, si.Created)
	}
}

func TestJetStreamFileConsumerCreatedTimePreservedAfterRestartAndUpdate(t *testing.T) {
	s := RunBasicJetStreamServer(t)
	defer func() { s.Shutdown() }()

	nc, js := jsClientConnect(t, s)
	defer func() { nc.Close() }()

	_, err := js.AddStream(&nats.StreamConfig{Name: "TEST", Storage: nats.FileStorage})
	require_NoError(t, err)

	ci, err := js.AddConsumer("TEST", &nats.ConsumerConfig{
		Durable:   "DUR",
		AckPolicy: nats.AckExplicitPolicy,
		AckWait:   30 * time.Second,
	})
	require_NoError(t, err)
	created := ci.Created

	sd := s.JetStreamConfig().StoreDir
	nc.Close()
	s.Shutdown()
	time.Sleep(10 * time.Millisecond)

	s = RunJetStreamServerOnPort(-1, sd)
	nc, js = jsClientConnect(t, s)
	ci, err = js.ConsumerInfo("TEST", "DUR")
	require_NoError(t, err)
	if !ci.Created.Equal(created) {
		t.Fatalf("Expected created time %v after restart, got %v", created, ci.Created)
	}

	cfg := ci.Config
	cfg.AckWait = 45 * time.Second
	_, err = js.UpdateConsumer("TEST", &cfg)
	require_NoError(t, err)
	nc.Close()
	s.Shutdown()

	s = RunJetStreamServerOnPort(-1, sd)
	nc, js = jsClientConnect(t, s)
	ci, err = js.ConsumerInfo("TEST", "DUR")
	require_NoError(t, err)
	if !ci.Created.Equal(created) {
		t.Fatalf("Expected created time %v after restart and update, got %v", created, ci.Created)
	}
}

func TestJetStreamDeleteMsg(t *testing.T) {
	cases := []struct {
		name    string
		mconfig *StreamConfig
	}{
		{name: "MemoryStore",
			mconfig: &StreamConfig{
				Name:      "foo",
				Retention: LimitsPolicy,
				MaxAge:    time.Hour,
				Storage:   MemoryStorage,
				Replicas:  1,
			}},
		{name: "FileStore",
			mconfig: &StreamConfig{
				Name:      "foo",
				Retention: LimitsPolicy,
				MaxAge:    time.Hour,
				Storage:   FileStorage,
				Replicas:  1,
			}},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {

			s := RunBasicJetStreamServer(t)
			defer s.Shutdown()

			mset, err := s.GlobalAccount().addStream(c.mconfig)
			if err != nil {
				t.Fatalf("Unexpected error adding stream: %v", err)
			}

			nc, js := jsClientConnect(t, s)
			defer nc.Close()

			pubTen := func() {
				t.Helper()
				for i := 0; i < 10; i++ {
					js.Publish("foo", []byte("Hello World!"))
				}
			}

			pubTen()

			state := mset.state()
			if state.Msgs != 10 {
				t.Fatalf("Expected 10 messages, got %d", state.Msgs)
			}
			bytesPerMsg := state.Bytes / 10
			if bytesPerMsg == 0 {
				t.Fatalf("Expected non-zero bytes for msg size")
			}

			deleteAndCheck := func(seq, expectedFirstSeq uint64) {
				t.Helper()
				beforeState := mset.state()
				if removed, _ := mset.deleteMsg(seq); !removed {
					t.Fatalf("Expected the delete of sequence %d to succeed", seq)
				}
				expectedState := beforeState
				expectedState.Msgs--
				expectedState.Bytes -= bytesPerMsg
				expectedState.FirstSeq = expectedFirstSeq

				sm, err := mset.getMsg(expectedFirstSeq)
				if err != nil {
					t.Fatalf("Error fetching message for seq: %d - %v", expectedFirstSeq, err)
				}
				expectedState.FirstTime = sm.Time
				expectedState.Deleted = nil
				expectedState.NumDeleted = 0

				afterState := mset.state()
				afterState.Deleted = nil
				afterState.NumDeleted = 0

				// Ignore first time in this test.
				if !reflect.DeepEqual(afterState, expectedState) {
					t.Fatalf("Stats not what we expected. Expected %+v, got %+v\n", expectedState, afterState)
				}
			}

			// Delete one from the middle
			deleteAndCheck(5, 1)
			// Now make sure sequences are updated properly.
			// Delete first msg.
			deleteAndCheck(1, 2)
			// Now last
			deleteAndCheck(10, 2)
			// Now gaps.
			deleteAndCheck(3, 2)
			deleteAndCheck(2, 4)

			mset.purge(nil)
			// Put ten more one.
			pubTen()
			deleteAndCheck(11, 12)
			deleteAndCheck(15, 12)
			deleteAndCheck(16, 12)
			deleteAndCheck(20, 12)

			// Only file storage beyond here.
			if c.mconfig.Storage == MemoryStorage {
				return
			}

			// Capture port since it was dynamic.
			u, _ := url.Parse(s.ClientURL())
			port, _ := strconv.Atoi(u.Port())
			sd := s.JetStreamConfig().StoreDir

			// Shutdown the
			s.Shutdown()

			s = RunJetStreamServerOnPort(port, sd)
			defer s.Shutdown()

			mset, err = s.GlobalAccount().lookupStream("foo")
			if err != nil {
				t.Fatalf("Expected to get the stream back")
			}

			expected := StreamState{Msgs: 6, Bytes: 6 * bytesPerMsg, FirstSeq: 12, LastSeq: 20, NumSubjects: 1}
			state = mset.state()
			state.FirstTime, state.LastTime, state.Deleted, state.NumDeleted = time.Time{}, time.Time{}, nil, 0

			if !reflect.DeepEqual(expected, state) {
				t.Fatalf("State not what we expected. Expected %+v, got %+v\n", expected, state)
			}

			// Now create an consumer and make sure we get the right sequence.
			nc = clientConnectToServer(t, s)
			defer nc.Close()

			delivery := nats.NewInbox()
			sub, _ := nc.SubscribeSync(delivery)
			nc.Flush()

			o, err := mset.addConsumer(&ConsumerConfig{DeliverSubject: delivery, FilterSubject: "foo"})
			if err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}

			expectedStoreSeq := []uint64{12, 13, 14, 17, 18, 19}

			for i := 0; i < 6; i++ {
				m, err := sub.NextMsg(time.Second)
				if err != nil {
					t.Fatalf("Unexpected error: %v", err)
				}
				if o.streamSeqFromReply(m.Reply) != expectedStoreSeq[i] {
					t.Fatalf("Expected store seq of %d, got %d", expectedStoreSeq[i], o.streamSeqFromReply(m.Reply))
				}
			}
		})
	}
}

// https://github.com/nats-io/jetstream/issues/396
func TestJetStreamLimitLockBug(t *testing.T) {
	cases := []struct {
		name    string
		mconfig *StreamConfig
	}{
		{name: "MemoryStore",
			mconfig: &StreamConfig{
				Name:      "foo",
				Retention: LimitsPolicy,
				MaxMsgs:   10,
				Storage:   MemoryStorage,
				Replicas:  1,
			}},
		{name: "FileStore",
			mconfig: &StreamConfig{
				Name:      "foo",
				Retention: LimitsPolicy,
				MaxMsgs:   10,
				Storage:   FileStorage,
				Replicas:  1,
			}},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {

			s := RunBasicJetStreamServer(t)
			defer s.Shutdown()

			mset, err := s.GlobalAccount().addStream(c.mconfig)
			if err != nil {
				t.Fatalf("Unexpected error adding stream: %v", err)
			}

			nc := clientConnectToServer(t, s)
			defer nc.Close()

			for i := 0; i < 100; i++ {
				sendStreamMsg(t, nc, "foo", "ok")
			}

			state := mset.state()
			if state.Msgs != 10 {
				t.Fatalf("Expected 10 messages, got %d", state.Msgs)
			}
		})
	}
}

func TestJetStreamNextMsgNoInterest(t *testing.T) {
	cases := []struct {
		name    string
		mconfig *StreamConfig
	}{
		{name: "MemoryStore",
			mconfig: &StreamConfig{
				Name:      "foo",
				Retention: LimitsPolicy,
				MaxAge:    time.Hour,
				Storage:   MemoryStorage,
				Replicas:  1,
			}},
		{name: "FileStore",
			mconfig: &StreamConfig{
				Name:      "foo",
				Retention: LimitsPolicy,
				MaxAge:    time.Hour,
				Storage:   FileStorage,
				Replicas:  1,
			}},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			s := RunBasicJetStreamServer(t)
			defer s.Shutdown()

			cfg := &StreamConfig{Name: "foo", Storage: FileStorage}
			mset, err := s.GlobalAccount().addStream(cfg)
			if err != nil {
				t.Fatalf("Unexpected error adding stream: %v", err)
			}

			nc := clientConnectWithOldRequest(t, s)
			defer nc.Close()

			// Now create an consumer and make sure it functions properly.
			o, err := mset.addConsumer(workerModeConfig("WQ"))
			if err != nil {
				t.Fatalf("Expected no error with registered interest, got %v", err)
			}
			defer o.delete()

			nextSubj := o.requestNextMsgSubject()

			// Queue up a worker but use a short time out.
			if _, err := nc.Request(nextSubj, nil, time.Millisecond); err != nats.ErrTimeout {
				t.Fatalf("Expected a timeout error and no response with acks suppressed")
			}
			// Now send a message, the worker from above will still be known but we want to make
			// sure the system detects that so we will do a request for next msg right behind it.
			nc.Publish("foo", []byte("OK"))
			if msg, err := nc.Request(nextSubj, nil, 5*time.Millisecond); err != nil {
				t.Fatalf("Unexpected error: %v", err)
			} else {
				msg.Respond(nil) // Ack
			}
			// Now queue up 10 workers.
			for i := 0; i < 10; i++ {
				if _, err := nc.Request(nextSubj, nil, time.Microsecond); err != nats.ErrTimeout {
					t.Fatalf("Expected a timeout error and no response with acks suppressed")
				}
			}
			// Now publish ten messages.
			for i := 0; i < 10; i++ {
				nc.Publish("foo", []byte("OK"))
			}
			nc.Flush()
			for i := 0; i < 10; i++ {
				if msg, err := nc.Request(nextSubj, nil, 10*time.Millisecond); err != nil {
					t.Fatalf("Unexpected error for %d: %v", i, err)
				} else {
					msg.Respond(nil) // Ack
				}
			}
			nc.Flush()

			checkFor(t, time.Second, 10*time.Millisecond, func() error {
				ostate := o.info()
				if ostate.AckFloor.Stream != 11 || ostate.NumAckPending > 0 {
					return fmt.Errorf("Inconsistent ack state: %+v", ostate)
				}
				return nil
			})
		})
	}
}

func TestJetStreamMsgHeaders(t *testing.T) {
	cases := []struct {
		name    string
		mconfig *StreamConfig
	}{
		{name: "MemoryStore",
			mconfig: &StreamConfig{
				Name:      "foo",
				Retention: LimitsPolicy,
				MaxAge:    time.Hour,
				Storage:   MemoryStorage,
				Replicas:  1,
			}},
		{name: "FileStore",
			mconfig: &StreamConfig{
				Name:      "foo",
				Retention: LimitsPolicy,
				MaxAge:    time.Hour,
				Storage:   FileStorage,
				Replicas:  1,
			}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			s := RunBasicJetStreamServer(t)
			defer s.Shutdown()

			mset, err := s.GlobalAccount().addStream(c.mconfig)
			if err != nil {
				t.Fatalf("Unexpected error adding stream: %v", err)
			}
			defer mset.delete()

			nc := clientConnectToServer(t, s)
			defer nc.Close()

			m := nats.NewMsg("foo")
			m.Header.Add("Accept-Encoding", "json")
			m.Header.Add("Authorization", "s3cr3t")
			m.Data = []byte("Hello JetStream Headers - #1!")

			nc.PublishMsg(m)
			nc.Flush()

			checkFor(t, time.Second*2, time.Millisecond*250, func() error {
				state := mset.state()
				if state.Msgs != 1 {
					return fmt.Errorf("Expected 1 message, got %d", state.Msgs)
				}
				if state.Bytes == 0 {
					return fmt.Errorf("Expected non-zero bytes")
				}
				return nil
			})

			// Now access raw from stream.
			sm, err := mset.getMsg(1)
			if err != nil {
				t.Fatalf("Unexpected error getting stored message: %v", err)
			}
			// Compare decoded headers since their wire order is unspecified.
			hdr, err := nats.DecodeHeadersMsg(sm.Header)
			if err != nil {
				t.Fatalf("Error decoding stored headers: %v", err)
			}
			if !reflect.DeepEqual(hdr, m.Header) {
				t.Fatalf("Message headers do not match, %v vs %v", m.Header, hdr)
			}
			if !bytes.Equal(sm.Data, m.Data) {
				t.Fatalf("Message data do not match, %q vs %q", m.Data, sm.Data)
			}

			// Now do consumer based.
			sub, _ := nc.SubscribeSync(nats.NewInbox())
			defer sub.Unsubscribe()
			nc.Flush()

			o, err := mset.addConsumer(&ConsumerConfig{DeliverSubject: sub.Subject})
			if err != nil {
				t.Fatalf("Expected no error with registered interest, got %v", err)
			}
			defer o.delete()

			cm, err := sub.NextMsg(time.Second)
			if err != nil {
				t.Fatalf("Error getting message: %v", err)
			}
			// Check the message.
			// Check out original headers.
			if cm.Header.Get("Accept-Encoding") != "json" ||
				cm.Header.Get("Authorization") != "s3cr3t" {
				t.Fatalf("Original headers not present")
			}
			if !bytes.Equal(m.Data, cm.Data) {
				t.Fatalf("Message payloads are not the same: %q vs %q", cm.Data, m.Data)
			}
		})
	}
}

// This will be testing our ability to conditionally rewrite subjects for last mile
// when working with JetStream. Consumers receive messages that have their subjects
// rewritten to match the original subject. NATS routing is all subject based except
// for the last mile to the client.
func TestJetStreamSingleInstanceRemoteAccess(t *testing.T) {
	ca := createClusterWithName(t, "A", 1)
	defer shutdownCluster(ca)
	cb := createClusterWithName(t, "B", 1, ca)
	defer shutdownCluster(cb)

	// Connect our leafnode server to cluster B.
	opts := cb.opts[rand.IntN(len(cb.opts))]
	s, _ := runSolicitLeafServer(opts)
	defer s.Shutdown()

	checkLeafNodeConnected(t, s)

	if err := s.EnableJetStream(&JetStreamConfig{StoreDir: t.TempDir()}); err != nil {
		t.Fatalf("Expected no error, got %v", err)
	}

	mset, err := s.GlobalAccount().addStream(&StreamConfig{Name: "foo", Storage: MemoryStorage})
	if err != nil {
		t.Fatalf("Unexpected error adding stream: %v", err)
	}
	defer mset.delete()

	nc := clientConnectToServer(t, s)
	defer nc.Close()

	toSend := 10
	for i := 0; i < toSend; i++ {
		sendStreamMsg(t, nc, "foo", "Hello World!")
	}

	// Now create a push based consumer. Connected to the non-jetstream server via a random server on cluster A.
	sl := ca.servers[rand.IntN(len(ca.servers))]
	nc2 := clientConnectToServer(t, sl)
	defer nc2.Close()

	sub, _ := nc2.SubscribeSync(nats.NewInbox())
	defer sub.Unsubscribe()

	// Need to wait for interest to propagate across GW.
	nc2.Flush()
	time.Sleep(25 * time.Millisecond)

	o, err := mset.addConsumer(&ConsumerConfig{DeliverSubject: sub.Subject})
	if err != nil {
		t.Fatalf("Expected no error with registered interest, got %v", err)
	}
	defer o.delete()

	checkSubPending := func(numExpected int) {
		t.Helper()
		checkFor(t, 200*time.Millisecond, 10*time.Millisecond, func() error {
			if nmsgs, _, _ := sub.Pending(); err != nil || nmsgs != numExpected {
				return fmt.Errorf("Did not receive correct number of messages: %d vs %d", nmsgs, numExpected)
			}
			return nil
		})
	}
	checkSubPending(toSend)

	checkMsg := func(m *nats.Msg, err error, i int) {
		t.Helper()
		if err != nil {
			t.Fatalf("Got an error checking message: %v", err)
		}
		if m.Subject != "foo" {
			t.Fatalf("Expected original subject of %q, but got %q", "foo", m.Subject)
		}
		// Now check that reply subject exists and has a sequence as the last token.
		if seq := o.seqFromReply(m.Reply); seq != uint64(i) {
			t.Fatalf("Expected sequence of %d , got %d", i, seq)
		}
	}

	// Now check the subject to make sure its the original one.
	for i := 1; i <= toSend; i++ {
		m, err := sub.NextMsg(time.Second)
		checkMsg(m, err, i)
	}

	// Now do a pull based consumer.
	o, err = mset.addConsumer(workerModeConfig("p"))
	if err != nil {
		t.Fatalf("Expected no error with registered interest, got %v", err)
	}
	defer o.delete()

	nextMsg := o.requestNextMsgSubject()
	for i := 1; i <= toSend; i++ {
		m, err := nc.Request(nextMsg, nil, time.Second)
		checkMsg(m, err, i)
	}
}

func clientConnectToServerWithUP(t *testing.T, opts *Options, user, pass string) *nats.Conn {
	curl := fmt.Sprintf("nats://%s:%s@%s:%d", user, pass, opts.Host, opts.Port)
	nc, err := nats.Connect(curl, nats.Name("JS-UP-TEST"), nats.ReconnectWait(5*time.Millisecond), nats.MaxReconnects(-1))
	if err != nil {
		t.Fatalf("Failed to create client: %v", err)
	}
	return nc
}

func TestJetStreamCanNotEnableOnSystemAccount(t *testing.T) {
	s := RunBasicJetStreamServer(t)
	defer s.Shutdown()

	sa := s.SystemAccount()
	if err := sa.EnableJetStream(nil, nil); err == nil {
		t.Fatalf("Expected an error trying to enable on the system account")
	}
}

func TestJetStreamMultipleAccountsBasics(t *testing.T) {
	tdir := t.TempDir()
	conf := createConfFile(t, []byte(fmt.Sprintf(`
		listen: 127.0.0.1:-1
		jetstream: {max_mem_store: 64GB, max_file_store: 10TB, store_dir: %q}
		accounts: {
			A: {
				jetstream: enabled
				users: [ {user: ua, password: pwd} ]
			},
			B: {
				jetstream: {max_mem: 1GB, max_store: 1TB, max_streams: 10, max_consumers: 1k}
				users: [ {user: ub, password: pwd} ]
			},
			C: {
				users: [ {user: uc, password: pwd} ]
			},
		}
	`, tdir)))

	s, opts := RunServerWithConfig(conf)
	defer s.Shutdown()

	if !s.JetStreamEnabled() {
		t.Fatalf("Expected JetStream to be enabled")
	}

	nca := clientConnectToServerWithUP(t, opts, "ua", "pwd")
	defer nca.Close()

	ncb := clientConnectToServerWithUP(t, opts, "ub", "pwd")
	defer ncb.Close()

	resp, err := ncb.Request(JSApiAccountInfo, nil, time.Second)
	require_NoError(t, err)
	var info JSApiAccountInfoResponse
	if err := json.Unmarshal(resp.Data, &info); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	limits := info.Limits
	if limits.MaxStreams != 10 {
		t.Fatalf("Expected 10 for MaxStreams, got %d", limits.MaxStreams)
	}
	if limits.MaxConsumers != 1000 {
		t.Fatalf("Expected MaxConsumers of %d, got %d", 1000, limits.MaxConsumers)
	}
	gb := int64(1024 * 1024 * 1024)
	if limits.MaxMemory != gb {
		t.Fatalf("Expected MaxMemory to be 1GB, got %d", limits.MaxMemory)
	}
	if limits.MaxStore != 1024*gb {
		t.Fatalf("Expected MaxStore to be 1TB, got %d", limits.MaxStore)
	}

	ncc := clientConnectToServerWithUP(t, opts, "uc", "pwd")
	defer ncc.Close()

	expectNotEnabled := func(resp *nats.Msg, err error) {
		t.Helper()
		if err != nil {
			t.Fatalf("Unexpected error requesting enabled status: %v", err)
		}
		if resp == nil {
			t.Fatalf("No response, possible timeout?")
		}
		var iResp JSApiAccountInfoResponse
		if err := json.Unmarshal(resp.Data, &iResp); err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		if iResp.Error == nil {
			t.Fatalf("Expected an error on not enabled account")
		}
	}

	// Check C is not enabled. We expect a negative response, not a timeout.
	expectNotEnabled(ncc.Request(JSApiAccountInfo, nil, 250*time.Millisecond))

	// Now do simple reload and check that we do the right thing. Testing enable and disable and also change in limits
	newConf := []byte(fmt.Sprintf(`
		listen: 127.0.0.1:-1
		jetstream: {max_mem_store: 64GB, max_file_store: 10TB, store_dir: %q}
		accounts: {
			A: {
				jetstream: disabled
				users: [ {user: ua, password: pwd} ]
			},
			B: {
				jetstream: {max_mem: 32GB, max_store: 512GB, max_streams: 100, max_consumers: 4k}
				users: [ {user: ub, password: pwd} ]
			},
			C: {
				jetstream: {max_mem: 1GB, max_store: 1TB, max_streams: 10, max_consumers: 1k}
				users: [ {user: uc, password: pwd} ]
			},
		}
	`, tdir))
	if err := os.WriteFile(conf, newConf, 0600); err != nil {
		t.Fatalf("Error rewriting server's config file: %v", err)
	}
	if err := s.Reload(); err != nil {
		t.Fatalf("Error on server reload: %v", err)
	}
	expectNotEnabled(nca.Request(JSApiAccountInfo, nil, 250*time.Millisecond))

	resp, _ = ncb.Request(JSApiAccountInfo, nil, 250*time.Millisecond)
	if err := json.Unmarshal(resp.Data, &info); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if info.Error != nil {
		t.Fatalf("Expected JetStream to be enabled, got %+v", info.Error)
	}

	resp, _ = ncc.Request(JSApiAccountInfo, nil, 250*time.Millisecond)
	if err := json.Unmarshal(resp.Data, &info); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if info.Error != nil {
		t.Fatalf("Expected JetStream to be enabled, got %+v", info.Error)
	}

	// Now check that limits have been updated.
	// Account B
	resp, err = ncb.Request(JSApiAccountInfo, nil, time.Second)
	require_NoError(t, err)
	if err := json.Unmarshal(resp.Data, &info); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	limits = info.Limits
	if limits.MaxStreams != 100 {
		t.Fatalf("Expected 100 for MaxStreams, got %d", limits.MaxStreams)
	}
	if limits.MaxConsumers != 4000 {
		t.Fatalf("Expected MaxConsumers of %d, got %d", 4000, limits.MaxConsumers)
	}
	if limits.MaxMemory != 32*gb {
		t.Fatalf("Expected MaxMemory to be 32GB, got %d", limits.MaxMemory)
	}
	if limits.MaxStore != 512*gb {
		t.Fatalf("Expected MaxStore to be 512GB, got %d", limits.MaxStore)
	}

	// Account C
	resp, err = ncc.Request(JSApiAccountInfo, nil, time.Second)
	require_NoError(t, err)
	if err := json.Unmarshal(resp.Data, &info); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	limits = info.Limits
	if limits.MaxStreams != 10 {
		t.Fatalf("Expected 10 for MaxStreams, got %d", limits.MaxStreams)
	}
	if limits.MaxConsumers != 1000 {
		t.Fatalf("Expected MaxConsumers of %d, got %d", 1000, limits.MaxConsumers)
	}
	if limits.MaxMemory != gb {
		t.Fatalf("Expected MaxMemory to be 1GB, got %d", limits.MaxMemory)
	}
	if limits.MaxStore != 1024*gb {
		t.Fatalf("Expected MaxStore to be 1TB, got %d", limits.MaxStore)
	}
}

func TestJetStreamServerResourcesConfig(t *testing.T) {
	conf := createConfFile(t, []byte(fmt.Sprintf(`
		listen: 127.0.0.1:-1
		jetstream: {max_mem_store: 2GB, max_file_store: 1TB, store_dir: %q}
	`, t.TempDir())))

	s, _ := RunServerWithConfig(conf)
	defer s.Shutdown()

	if !s.JetStreamEnabled() {
		t.Fatalf("Expected JetStream to be enabled")
	}

	gb := int64(1024 * 1024 * 1024)
	jsc := s.JetStreamConfig()
	if jsc.MaxMemory != 2*gb {
		t.Fatalf("Expected MaxMemory to be %d, got %d", 2*gb, jsc.MaxMemory)
	}
	if jsc.MaxStore != 1024*gb {
		t.Fatalf("Expected MaxStore to be %d, got %d", 1024*gb, jsc.MaxStore)
	}
}

// https://github.com/nats-io/nats-server/issues/8160
func TestJetStreamProgrammaticMaxStoreAndMaxMemory(t *testing.T) {
	gb := int64(1024 * 1024 * 1024)

	for _, test := range []struct {
		name     string
		maxMem   int64
		maxStore int64
	}{
		{"only-max-store", 0, 1 * gb},
		{"only-max-memory", 2 * gb, 0},
		{"both", 2 * gb, 1 * gb},
		{"neither", 0, 0},
	} {
		t.Run(test.name, func(t *testing.T) {
			opts := DefaultTestOptions
			opts.Port = -1
			opts.JetStream = true
			opts.StoreDir = t.TempDir()
			opts.JetStreamMaxMemory = test.maxMem
			opts.JetStreamMaxStore = test.maxStore

			s := RunServer(&opts)
			defer s.Shutdown()

			require_True(t, s.JetStreamEnabled())
			jsc := s.JetStreamConfig()

			if test.maxStore > 0 {
				require_Equal(t, jsc.MaxStore, test.maxStore)
			} else {
				require_True(t, jsc.MaxStore > 0)
			}
			if test.maxMem > 0 {
				require_Equal(t, jsc.MaxMemory, test.maxMem)
			} else {
				require_True(t, jsc.MaxMemory > 0)
			}
		})
	}
}

func TestJetStreamDynConfigBothPositive(t *testing.T) {
	s := RunBasicJetStreamServer(t)
	defer s.Shutdown()

	// Both flags are false because opts came from code, not a config file.
	opts := s.getOpts()
	require_False(t, opts.maxMemSet)
	require_False(t, opts.maxStoreSet)

	const maxStore = int64(1024 * 1024 * 1024)
	const maxMem = int64(2 * 1024 * 1024 * 1024)

	jsc := s.dynJetStreamConfig(t.TempDir(), maxStore, maxMem)
	require_Equal(t, jsc.MaxStore, maxStore)
	require_Equal(t, jsc.MaxMemory, maxMem)
}

func TestJetStreamConfigNegativeLimitsFallBackToDynamic(t *testing.T) {
	conf := createConfFile(t, []byte(fmt.Sprintf(`
		listen: 127.0.0.1:-1
		jetstream: {max_mem_store: -1, max_file_store: -1, store_dir: %q}
	`, t.TempDir())))

	s, _ := RunServerWithConfig(conf)
	defer s.Shutdown()

	require_True(t, s.JetStreamEnabled())
	jsc := s.JetStreamConfig()
	// -1 is the "unset" sentinel: it must be replaced with dynamic limits,
	require_True(t, jsc.MaxMemory > 0)
	require_True(t, jsc.MaxStore > 0)
}

func TestJetStreamConfigExplicitZeroLimits(t *testing.T) {
	conf := createConfFile(t, []byte(fmt.Sprintf(`
		listen: 127.0.0.1:-1
		jetstream: {max_mem_store: 0, max_file_store: 0, store_dir: %q}
	`, t.TempDir())))

	s, _ := RunServerWithConfig(conf)
	defer s.Shutdown()

	require_True(t, s.JetStreamEnabled())
	jsc := s.JetStreamConfig()
	require_Equal(t, jsc.MaxMemory, 0)
	require_Equal(t, jsc.MaxStore, 0)
}

// From 2.2.2 to 2.2.3 we fixed a bug that would not consistently place a jetstream directory
// under the store directory configured. However there were some cases where the directory was
// created that way and therefore 2.2.3 would start and not recognize the existing accounts,
// streams and consumers.
func TestJetStreamStoreDirectoryFix(t *testing.T) {
	sd := filepath.Join(os.TempDir(), "sd_test")
	defer removeDir(t, sd)

	conf := createConfFile(t, []byte(fmt.Sprintf("listen: 127.0.0.1:-1\njetstream: {store_dir: %q}\n", sd)))

	s, _ := RunServerWithConfig(conf)
	defer s.Shutdown()

	nc, js := jsClientConnect(t, s)
	defer nc.Close()

	if _, err := js.AddStream(&nats.StreamConfig{Name: "TEST"}); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if _, err := js.Publish("TEST", []byte("TSS")); err != nil {
		t.Fatalf("Unexpected publish error: %v", err)
	}
	// Push based.
	sub, err := js.SubscribeSync("TEST", nats.Durable("dlc"))
	require_NoError(t, err)
	defer sub.Unsubscribe()

	// Now shutdown the server.
	nc.Close()
	s.Shutdown()

	// Now move stuff up from the jetstream directory etc.
	jssd := filepath.Join(sd, JetStreamStoreDir)
	fis, _ := os.ReadDir(jssd)
	// This will be accounts, move them up one directory.
	for _, fi := range fis {
		os.Rename(filepath.Join(jssd, fi.Name()), filepath.Join(sd, fi.Name()))
	}
	removeDir(t, jssd)

	// Restart our server. Make sure our assets got moved.
	s, _ = RunServerWithConfig(conf)
	defer s.Shutdown()

	nc, js = jsClientConnect(t, s)
	defer nc.Close()

	var names []string
	for name := range js.StreamNames() {
		names = append(names, name)
	}
	if len(names) != 1 {
		t.Fatalf("Expected only 1 stream but got %d", len(names))
	}
	names = names[:0]
	for name := range js.ConsumerNames("TEST") {
		names = append(names, name)
	}
	if len(names) != 1 {
		t.Fatalf("Expected only 1 consumer but got %d", len(names))
	}
}

func TestJetStreamPushConsumersPullError(t *testing.T) {
	s := RunBasicJetStreamServer(t)
	defer s.Shutdown()

	nc, js := jsClientConnect(t, s)
	defer nc.Close()

	if _, err := js.AddStream(&nats.StreamConfig{Name: "TEST"}); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if _, err := js.Publish("TEST", []byte("TSS")); err != nil {
		t.Fatalf("Unexpected publish error: %v", err)
	}
	// Push based.
	sub, err := js.SubscribeSync("TEST")
	require_NoError(t, err)
	defer sub.Unsubscribe()
	ci, err := sub.ConsumerInfo()
	require_NoError(t, err)

	// Now do a pull. Make sure we get an error.
	m, err := nc.Request(fmt.Sprintf(JSApiRequestNextT, "TEST", ci.Name), nil, time.Second)
	require_NoError(t, err)
	if m.Header.Get("Status") != "409" {
		t.Fatalf("Expected a 409 status code, got %q", m.Header.Get("Status"))
	}
}

func TestJetStreamChangeConsumerType(t *testing.T) {
	s := RunBasicJetStreamServer(t)
	defer s.Shutdown()

	nc, js := jsClientConnect(t, s)
	defer nc.Close()

	_, err := js.AddStream(&nats.StreamConfig{Name: "TEST", Subjects: []string{"test.*"}})
	require_NoError(t, err)

	// create pull consumer
	_, err = js.AddConsumer("TEST", &nats.ConsumerConfig{
		Name:      "pull",
		AckPolicy: nats.AckExplicitPolicy,
	})
	require_NoError(t, err)

	// cannot update pull -> push
	_, err = js.UpdateConsumer("TEST", &nats.ConsumerConfig{
		Name:           "pull",
		AckPolicy:      nats.AckExplicitPolicy,
		DeliverSubject: "foo",
	})
	require_Contains(t, err.Error(), "can not update pull consumer to push based")

	// create push consumer
	_, err = js.AddConsumer("TEST", &nats.ConsumerConfig{
		Name:           "push",
		AckPolicy:      nats.AckExplicitPolicy,
		DeliverSubject: "foo",
	})
	require_NoError(t, err)

	// cannot change push -> pull
	_, err = js.UpdateConsumer("TEST", &nats.ConsumerConfig{
		Name:      "push",
		AckPolicy: nats.AckExplicitPolicy,
	})
	require_Contains(t, err.Error(), "can not update push consumer to pull based")
}

////////////////////////////////////////
// Benchmark placeholders
// TODO(dlc) - move
////////////////////////////////////////

func TestJetStreamPubPerf(t *testing.T) {
	// Comment out to run, holding place for now.
	t.SkipNow()

	s := RunBasicJetStreamServer(t)
	defer s.Shutdown()

	acc := s.GlobalAccount()

	msetConfig := StreamConfig{
		Name:     "sr22",
		Storage:  FileStorage,
		Subjects: []string{"foo"},
	}

	if _, err := acc.addStream(&msetConfig); err != nil {
		t.Fatalf("Unexpected error adding stream: %v", err)
	}

	nc := clientConnectToServer(t, s)
	defer nc.Close()

	toSend := 5_000_000
	numProducers := 5

	payload := []byte("Hello World")

	startCh := make(chan bool)
	var wg sync.WaitGroup

	for n := 0; n < numProducers; n++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-startCh
			for i := 0; i < int(toSend)/numProducers; i++ {
				nc.Publish("foo", payload)
			}
			nc.Flush()
		}()
	}

	// Wait for Go routines.
	time.Sleep(20 * time.Millisecond)
	start := time.Now()
	close(startCh)
	wg.Wait()

	tt := time.Since(start)
	fmt.Printf("time is %v\n", tt)
	fmt.Printf("%.0f msgs/sec\n", float64(toSend)/tt.Seconds())

	// Stop current
	sd := s.JetStreamConfig().StoreDir
	s.Shutdown()
	// Restart.
	start = time.Now()
	s = RunJetStreamServerOnPort(-1, sd)
	defer s.Shutdown()
	fmt.Printf("Took %v to restart!\n", time.Since(start))
}

func TestJetStreamPubWithAsyncResponsePerf(t *testing.T) {
	// Comment out to run, holding place for now.
	t.SkipNow()

	s := RunBasicJetStreamServer(t)
	defer s.Shutdown()

	acc := s.GlobalAccount()

	msetConfig := StreamConfig{
		Name:     "sr33",
		Storage:  FileStorage,
		Subjects: []string{"foo"},
	}

	if _, err := acc.addStream(&msetConfig); err != nil {
		t.Fatalf("Unexpected error adding stream: %v", err)
	}

	nc := clientConnectToServer(t, s)
	defer nc.Close()

	toSend := 1_000_000
	payload := []byte("Hello World")

	start := time.Now()
	for i := 0; i < toSend; i++ {
		nc.PublishRequest("foo", "bar", payload)
	}
	nc.Flush()

	tt := time.Since(start)
	fmt.Printf("time is %v\n", tt)
	fmt.Printf("%.0f msgs/sec\n", float64(toSend)/tt.Seconds())
}

func TestJetStreamPubWithSyncPerf(t *testing.T) {
	// Comment out to run, holding place for now.
	t.SkipNow()

	s := RunBasicJetStreamServer(t)
	defer s.Shutdown()

	nc, js := jsClientConnect(t, s)
	defer nc.Close()

	_, err := js.AddStream(&nats.StreamConfig{Name: "foo"})
	require_NoError(t, err)

	toSend := 1_000_000
	payload := []byte("Hello World")

	start := time.Now()
	for i := 0; i < toSend; i++ {
		js.Publish("foo", payload)
	}

	tt := time.Since(start)
	fmt.Printf("time is %v\n", tt)
	fmt.Printf("%.0f msgs/sec\n", float64(toSend)/tt.Seconds())
}

func TestJetStreamPubSubPerf(t *testing.T) {
	// Comment out to run, holding place for now.
	t.SkipNow()

	s := RunBasicJetStreamServer(t)
	defer s.Shutdown()

	acc := s.GlobalAccount()

	msetConfig := StreamConfig{
		Name:     "MSET22",
		Storage:  FileStorage,
		Subjects: []string{"foo"},
	}

	mset, err := acc.addStream(&msetConfig)
	if err != nil {
		t.Fatalf("Unexpected error adding stream: %v", err)
	}

	nc := clientConnectToServer(t, s)
	defer nc.Close()

	var toSend = 1_000_000
	var received int
	done := make(chan bool)

	delivery := "d"

	nc.Subscribe(delivery, func(m *nats.Msg) {
		received++
		if received >= toSend {
			done <- true
		}
	})
	nc.Flush()

	_, err = mset.addConsumer(&ConsumerConfig{
		DeliverSubject: delivery,
		AckPolicy:      AckNone,
	})
	if err != nil {
		t.Fatalf("Error creating consumer: %v", err)
	}

	payload := []byte("Hello World")

	start := time.Now()

	for i := 0; i < toSend; i++ {
		nc.Publish("foo", payload)
	}

	<-done
	tt := time.Since(start)
	fmt.Printf("time is %v\n", tt)
	fmt.Printf("%.0f msgs/sec\n", float64(toSend)/tt.Seconds())
}

func TestJetStreamAckExplicitMsgRemoval(t *testing.T) {
	const burstSize = 20

	for _, storageType := range []StorageType{
		FileStorage,
		MemoryStorage,
	} {
		t.Run(storageType.String(), func(t *testing.T) {
			// Start a server
			s := RunBasicJetStreamServer(t)
			defer s.Shutdown()

			// Create a stream
			cfg := &StreamConfig{
				Name:      "MY_STREAM",
				Storage:   storageType,
				Subjects:  []string{"foo.*"},
				Retention: InterestPolicy,
			}
			mset, err := s.GlobalAccount().addStream(cfg)
			require_NoError(t, err)
			defer mset.delete()

			// Create 2 connections
			nc1 := clientConnectToServer(t, s)
			defer nc1.Close()
			nc2 := clientConnectToServer(t, s)
			defer nc2.Close()

			// Create 2 subscription inboxes (for durable consumers DeliverSubject forwarding)
			sub1, err := nc1.SubscribeSync(nats.NewInbox())
			require_NoError(t, err)
			defer sub1.Unsubscribe()
			err = nc1.Flush()
			require_NoError(t, err)

			sub2, err := nc2.SubscribeSync(nats.NewInbox())
			require_NoError(t, err)
			defer sub2.Unsubscribe()
			err = nc2.Flush()
			require_NoError(t, err)

			// Create 2 durable consumers (that forward messages to the inboxes above)
			dc1, err := mset.addConsumer(&ConsumerConfig{
				Durable:        "dur1",
				DeliverSubject: sub1.Subject,
				FilterSubject:  "foo.bar",
				AckPolicy:      AckExplicit,
			})
			require_NoError(t, err)
			defer dc1.delete()

			dc2, err := mset.addConsumer(&ConsumerConfig{
				Durable:        "dur2",
				DeliverSubject: sub2.Subject,
				FilterSubject:  "foo.bar",
				AckPolicy:      AckExplicit,
				AckWait:        100 * time.Millisecond,
			})
			require_NoError(t, err)
			defer dc2.delete()

			// Publish N messages (with subject matching the stream created above)
			for i := 1; i <= burstSize; i++ {
				pubAck := sendStreamMsg(t, nc1, "foo.bar", fmt.Sprintf("msg_%d", i))
				require_Equal(t, int(pubAck.Sequence), i)
			}

			// Check that the stream contains 2 messages
			state := mset.state()
			if state.Msgs != uint64(burstSize) {
				t.Fatalf("Expected %d messages in stream, found %d", burstSize, state.Msgs)
			}

			// Receive and ack both messages from the 2 subscriptions
			for subIndex, sub := range []*nats.Subscription{sub1, sub2} {
				sequences := make(map[uint64]bool, burstSize)

				for i := 1; i <= burstSize; i++ {
					m, err := sub.NextMsg(time.Second)
					require_NoError(t, err)
					err = m.Respond(nil)
					require_NoError(t, err)
					metadata, err := m.Metadata()
					require_NoError(t, err)
					sequences[metadata.Sequence.Stream] = true
				}

				// Verify all known sequence numbers are delivered (as opposed to duplicates)
				for i := uint64(1); i <= burstSize; i++ {
					_, ok := sequences[i]
					if !ok {
						t.Fatalf("Subscriber %d/2 did not deliver message sequence %d", subIndex+1, i)
					}
				}
			}

			// Flush both connections which may have pending ACKs
			err = nc1.Flush()
			require_NoError(t, err)
			err = nc2.Flush()
			require_NoError(t, err)

			// Verify all messages are eventually dropped from the stream (since all known consumers ack'd them)
			checkFor(t, 3*time.Second, 100*time.Millisecond, func() error {
				if state = mset.state(); state.Msgs != 0 {
					return fmt.Errorf("expected empty stream, but found %d messages", state.Msgs)
				}
				return nil
			})

			// Shut down one of the subscriptions
			err = sub2.Unsubscribe()
			require_NoError(t, err)
			err = nc2.Flush()
			require_NoError(t, err)

			// Publish N additional messages
			for i := burstSize + 1; i <= burstSize+burstSize; i++ {
				pubAck := sendStreamMsg(t, nc1, "foo.bar", fmt.Sprintf("msg_%d", i))
				require_Equal(t, i, int(pubAck.Sequence))
			}

			// Check that the stream contains N messages
			state = mset.state()
			if state.Msgs != uint64(burstSize) {
				t.Fatalf("Expected %d messages in stream, found %d", burstSize, state.Msgs)
			}

			// Receive and ack the new messages using the active subscription/consumer
			sequences := make(map[uint64]bool, burstSize)
			for i := 1; i <= burstSize; i++ {
				m, err := sub1.NextMsg(time.Second)
				require_NoError(t, err)
				err = m.Respond(nil)
				require_NoError(t, err)
				metadata, err := m.Metadata()
				require_NoError(t, err)
				sequences[metadata.Sequence.Stream] = true
			}
			// Verify all known sequence numbers are delivered (as opposed to duplicates)
			for i := uint64(burstSize) + 1; i <= burstSize+burstSize; i++ {
				_, ok := sequences[i]
				if !ok {
					t.Fatalf("Subscriber 1/2 did not deliver message sequence %d", i)
				}
			}

			// Create a different subscription inbox
			newSub2, err := nc2.SubscribeSync(nats.NewInbox())
			require_NoError(t, err)
			defer newSub2.Unsubscribe()
			err = nc2.Flush()
			require_NoError(t, err)

			// Update the second durable consumer with the new inbox subscription
			dc2, err = mset.addConsumer(&ConsumerConfig{
				Durable:        "dur2",
				DeliverSubject: newSub2.Subject,
				FilterSubject:  "foo.bar",
				AckPolicy:      AckExplicit,
				AckWait:        100 * time.Millisecond,
			})
			require_NoError(t, err)
			defer dc2.delete()

			// Receive and ack the new messages using the recreated/updated subscription/consumer
			sequences = make(map[uint64]bool, burstSize)
			for i := 1; i <= burstSize; i++ {
				m, err := newSub2.NextMsg(time.Second)
				require_NoError(t, err)
				err = m.Respond(nil)
				require_NoError(t, err)
				metadata, err := m.Metadata()
				require_NoError(t, err)
				sequences[metadata.Sequence.Stream] = true
			}
			// Verify all known sequence numbers are delivered (as opposed to duplicates)
			for i := uint64(burstSize) + 1; i <= burstSize+burstSize; i++ {
				_, ok := sequences[i]
				if !ok {
					t.Fatalf("Subscriber 2/2 did not deliver message sequence %d", i)
				}
			}

			// Flush both connections which may have pending ACKs
			err = nc1.Flush()
			require_NoError(t, err)
			err = nc2.Flush()
			require_NoError(t, err)

			// Verify all messages are eventually dropped from the stream, since all known consumers ack'd them
			checkFor(t, 3*time.Second, 100*time.Millisecond, func() error {
				if state = mset.state(); state.Msgs != 0 {
					return fmt.Errorf("expected empty stream, but found %d messages", state.Msgs)
				}
				return nil
			})
		})
	}
}

func TestJetStreamStoredMsgsDontDisappearAfterCacheExpiration(t *testing.T) {
	sc := &StreamConfig{
		Name:      "MY_STREAM",
		Storage:   FileStorage,
		Subjects:  []string{"foo.>"},
		Retention: InterestPolicy,
	}

	s := RunBasicJetStreamServer(t)
	defer s.Shutdown()

	mset, err := s.GlobalAccount().addStreamWithStore(sc, &FileStoreConfig{BlockSize: 128, CacheExpire: 15 * time.Millisecond})
	if err != nil {
		t.Fatalf("Unexpected error adding stream: %v", err)
	}
	defer mset.delete()

	nc1 := clientConnectWithOldRequest(t, s)
	defer nc1.Close()

	// Create a durable consumers
	sub, _ := nc1.SubscribeSync(nats.NewInbox())
	defer sub.Unsubscribe()
	nc1.Flush()

	o, err := mset.addConsumer(&ConsumerConfig{
		Durable:        "dur",
		DeliverSubject: sub.Subject,
		FilterSubject:  "foo.bar",
		DeliverPolicy:  DeliverNew,
		AckPolicy:      AckExplicit,
	})
	if err != nil {
		t.Fatalf("Unexpected error adding consumer: %v", err)
	}
	defer o.delete()

	nc2 := clientConnectWithOldRequest(t, s)
	defer nc2.Close()

	sendStreamMsg(t, nc2, "foo.bar", "msg1")

	msg, err := sub.NextMsg(time.Second)
	if err != nil {
		t.Fatalf("Did not get message: %v", err)
	}
	if string(msg.Data) != "msg1" {
		t.Fatalf("Unexpected message: %q", msg.Data)
	}

	nc1.Close()

	// Get the message from the stream
	getMsgSeq := func(seq uint64) {
		t.Helper()
		mreq := &JSApiMsgGetRequest{Seq: seq}
		req, err := json.Marshal(mreq)
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		smsgj, err := nc2.Request(fmt.Sprintf(JSApiMsgGetT, sc.Name), req, time.Second)
		if err != nil {
			t.Fatalf("Could not retrieve stream message: %v", err)
		}
		if strings.Contains(string(smsgj.Data), "code") {
			t.Fatalf("Error: %q", smsgj.Data)
		}
	}

	getMsgSeq(1)

	time.Sleep(time.Second)

	sendStreamMsg(t, nc2, "foo.bar", "msg2")
	sendStreamMsg(t, nc2, "foo.bar", "msg3")

	getMsgSeq(1)
	getMsgSeq(2)
	getMsgSeq(3)
}

func TestJetStreamDeliveryAfterServerRestart(t *testing.T) {
	opts := DefaultTestOptions
	opts.Port = -1
	opts.JetStream = true
	opts.StoreDir = t.TempDir()
	s := RunServer(&opts)
	defer s.Shutdown()

	mset, err := s.GlobalAccount().addStream(&StreamConfig{
		Name:      "MY_STREAM",
		Storage:   FileStorage,
		Subjects:  []string{"foo.>"},
		Retention: InterestPolicy,
	})
	if err != nil {
		t.Fatalf("Unexpected error adding stream: %v", err)
	}
	defer mset.delete()

	nc := clientConnectToServer(t, s)
	defer nc.Close()

	inbox := nats.NewInbox()
	o, err := mset.addConsumer(&ConsumerConfig{
		Durable:        "dur",
		DeliverSubject: inbox,
		DeliverPolicy:  DeliverNew,
		AckPolicy:      AckExplicit,
	})
	if err != nil {
		t.Fatalf("Expected no error, got %v", err)
	}
	defer o.delete()

	sub, err := nc.SubscribeSync(inbox)
	if err != nil {
		t.Fatalf("Error on subscribe: %v", err)
	}
	nc.Flush()

	// Send 1 message
	sendStreamMsg(t, nc, "foo.bar", "msg1")

	// Make sure we receive it and ack it.
	msg, err := sub.NextMsg(250 * time.Millisecond)
	if err != nil {
		t.Fatalf("Did not get message: %v", err)
	}
	// Ack it!
	msg.Respond(nil)
	nc.Flush()

	// Shutdown client and server
	nc.Close()

	dir := strings.TrimSuffix(s.JetStreamConfig().StoreDir, JetStreamStoreDir)
	s.Shutdown()

	opts.Port = -1
	opts.StoreDir = dir
	s = RunServer(&opts)
	defer s.Shutdown()

	// Lookup stream.
	mset, err = s.GlobalAccount().lookupStream("MY_STREAM")
	if err != nil {
		t.Fatalf("Error looking up stream: %v", err)
	}

	// Update consumer's deliver subject with new inbox
	inbox = nats.NewInbox()
	o, err = mset.addConsumer(&ConsumerConfig{
		Durable:        "dur",
		DeliverSubject: inbox,
		DeliverPolicy:  DeliverNew,
		AckPolicy:      AckExplicit,
	})
	if err != nil {
		t.Fatalf("Expected no error, got %v", err)
	}
	defer o.delete()

	nc = clientConnectToServer(t, s)
	defer nc.Close()

	// Send 2nd message
	sendStreamMsg(t, nc, "foo.bar", "msg2")

	// Start sub on new inbox
	sub, err = nc.SubscribeSync(inbox)
	if err != nil {
		t.Fatalf("Error on subscribe: %v", err)
	}
	nc.Flush()

	// Should receive message 2.
	if _, err := sub.NextMsg(500 * time.Millisecond); err != nil {
		t.Fatalf("Did not get message: %v", err)
	}
}

// This is for the basics of importing the ability to send to a stream and consume
// from a consumer that is pull based on push based on a well known delivery subject.
func TestJetStreamAccountImportBasics(t *testing.T) {
	conf := createConfFile(t, []byte(fmt.Sprintf(`
		listen: 127.0.0.1:-1
		no_auth_user: rip
		jetstream: {max_mem_store: 64GB, max_file_store: 10TB, store_dir: %q}
		accounts: {
			JS: {
				jetstream: enabled
				users: [ {user: dlc, password: foo} ]
				exports [
					# This is for sending into a stream from other accounts.
					{ service: "ORDERS.*" }
					# This is for accessing a pull based consumer.
					{ service: "$JS.API.CONSUMER.MSG.NEXT.*.*" }
					# This is streaming to a delivery subject for a push based consumer.
					{ stream: "deliver.ORDERS" }
					# This is to ack received messages. This is a service to ack acks..
					{ service: "$JS.ACK.ORDERS.*.>" }
					{ service: "$JS.ACK.*.*.ORDERS.*.>" }
				]
			},
			IU: {
				users: [ {user: rip, password: bar} ]
				imports [
					{ service: { subject: "ORDERS.*", account: JS }, to: "my.orders.$1" }
					{ service: { subject: "$JS.API.CONSUMER.MSG.NEXT.ORDERS.d", account: JS }, to: "nxt.msg" }
					{ stream:  { subject: "deliver.ORDERS", account: JS }, to: "d" }
					{ service: { subject: "$JS.ACK.ORDERS.*.>", account: JS } }
					{ service: { subject: "$JS.ACK.*.*.ORDERS.*.>", account: JS } }
				]
			},
		}
	`, t.TempDir())))

	s, _ := RunServerWithConfig(conf)
	defer s.Shutdown()

	acc, err := s.LookupAccount("JS")
	if err != nil {
		t.Fatalf("Unexpected error looking up account: %v", err)
	}

	mset, err := acc.addStream(&StreamConfig{Name: "ORDERS", Subjects: []string{"ORDERS.*"}})
	if err != nil {
		t.Fatalf("Unexpected error adding stream: %v", err)
	}
	defer mset.delete()

	// This should be the rip user, the one that imports some JS.
	nc := clientConnectToServer(t, s)
	defer nc.Close()

	// Simple publish to a stream.
	pubAck := sendStreamMsg(t, nc, "my.orders.foo", "ORDERS-1")
	if pubAck.Stream != "ORDERS" || pubAck.Sequence != 1 {
		t.Fatalf("Bad pubAck received: %+v", pubAck)
	}
	if msgs := mset.state().Msgs; msgs != 1 {
		t.Fatalf("Expected 1 message, got %d", msgs)
	}

	total := 2
	for i := 2; i <= total; i++ {
		sendStreamMsg(t, nc, "my.orders.bar", fmt.Sprintf("ORDERS-%d", i))
	}
	if msgs := mset.state().Msgs; msgs != uint64(total) {
		t.Fatalf("Expected %d messages, got %d", total, msgs)
	}

	// Now test access to a pull based consumer, e.g. workqueue.
	o, err := mset.addConsumer(&ConsumerConfig{
		Durable:   "d",
		AckPolicy: AckExplicit,
	})
	if err != nil {
		t.Fatalf("Expected no error, got %v", err)
	}
	defer o.delete()

	// We mapped the next message request, "$JS.API.CONSUMER.MSG.NEXT.ORDERS.d" -> "nxt.msg"
	m, err := nc.Request("nxt.msg", nil, time.Second)
	require_NoError(t, err)
	if string(m.Data) != "ORDERS-1" {
		t.Fatalf("Expected to receive %q, got %q", "ORDERS-1", m.Data)
	}

	// Now test access to a push based consumer
	o, err = mset.addConsumer(&ConsumerConfig{
		Durable:        "p",
		DeliverSubject: "deliver.ORDERS",
		AckPolicy:      AckExplicit,
	})
	if err != nil {
		t.Fatalf("Expected no error, got %v", err)
	}
	defer o.delete()

	// We remapped from above, deliver.ORDERS -> d
	sub, _ := nc.SubscribeSync("d")
	defer sub.Unsubscribe()

	checkFor(t, 250*time.Millisecond, 10*time.Millisecond, func() error {
		if nmsgs, _, _ := sub.Pending(); err != nil || nmsgs != total {
			return fmt.Errorf("Did not receive correct number of messages: %d vs %d", nmsgs, total)
		}
		return nil
	})

	m, _ = sub.NextMsg(time.Second)
	// Make sure we remapped subject correctly across the account boundary.
	if m.Subject != "ORDERS.foo" {
		t.Fatalf("Expected subject of %q, got %q", "ORDERS.foo", m.Subject)
	}
	// Now make sure we can ack messages correctly.
	m.Respond(AckAck)
	nc.Flush()

	if info := o.info(); info.AckFloor.Consumer != 1 {
		t.Fatalf("Did not receive the ack properly")
	}

	// Grab second one now.
	m, _ = sub.NextMsg(time.Second)
	// Make sure we remapped subject correctly across the account boundary.
	if m.Subject != "ORDERS.bar" {
		t.Fatalf("Expected subject of %q, got %q", "ORDERS.bar", m.Subject)
	}
	// Now make sure we can ack messages and get back an ack as well.
	resp, err := nc.Request(m.Reply, nil, 100*time.Millisecond)
	if resp == nil {
		t.Fatalf("No response (error: %v)", err)
	}
	if info := o.info(); info.AckFloor.Consumer != 2 {
		t.Fatalf("Did not receive the ack properly")
	}
}

// This tests whether we are able to aggregate all JetStream advisory events
// from all accounts into a single account. Config for this test uses
// service imports and exports as that allows for gathering all events
// without having to know the account name and without separate entries
// for each account in aggregate account config.
// This test fails as it is not receiving the api audit event ($JS.EVENT.ADVISORY.API).
func TestJetStreamAccountImportJSAdvisoriesAsService(t *testing.T) {
	conf := createConfFile(t, []byte(fmt.Sprintf(`
		listen=127.0.0.1:-1
		no_auth_user: pp
		jetstream: {max_mem_store: 64GB, max_file_store: 10TB, store_dir: %q}
		accounts {
			JS {
				jetstream: enabled
				users: [ {user: pp, password: foo} ]
				imports [
					{ service: { account: AGG, subject: '$JS.EVENT.ADVISORY.ACC.JS.>' }, to: '$JS.EVENT.ADVISORY.>' }
				]
			}
			AGG {
				users: [ {user: agg, password: foo} ]
				exports: [
					{ service: '$JS.EVENT.ADVISORY.ACC.*.>', response: Singleton, account_token_position: 5 }
				]
			}
		}
	`, t.TempDir())))

	s, _ := RunServerWithConfig(conf)
	defer s.Shutdown()

	// This should be the pp user, one which manages JetStream assets
	ncJS, err := nats.Connect(s.ClientURL())
	if err != nil {
		t.Fatalf("Unexpected error during connect: %v", err)
	}
	defer ncJS.Close()

	// This is the agg user, which should aggregate all JS advisory events.
	ncAgg, err := nats.Connect(s.ClientURL(), nats.UserInfo("agg", "foo"))
	if err != nil {
		t.Fatalf("Unexpected error during connect: %v", err)
	}
	defer ncAgg.Close()

	js, err := ncJS.JetStream()
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	// user from JS account should receive events on $JS.EVENT.ADVISORY.> subject
	subJS, err := ncJS.SubscribeSync("$JS.EVENT.ADVISORY.>")
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	defer subJS.Unsubscribe()
	require_NoError(t, ncJS.Flush())

	// user from AGG account should receive events on mapped $JS.EVENT.ADVISORY.ACC.JS.> subject (with account name)
	subAgg, err := ncAgg.SubscribeSync("$JS.EVENT.ADVISORY.ACC.JS.>")
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	defer subAgg.Unsubscribe()
	require_NoError(t, ncAgg.Flush())

	// add stream using JS account
	// this should trigger 2 events:
	// - an action event on $JS.EVENT.ADVISORY.STREAM.CREATED.ORDERS
	// - an api audit event on $JS.EVENT.ADVISORY.API
	_, err = js.AddStream(&nats.StreamConfig{Name: "ORDERS", Subjects: []string{"ORDERS.*"}})
	if err != nil {
		t.Fatalf("Unexpected error adding stream: %v", err)
	}

	gotEvents := map[string]int{}
	for i := 0; i < 2; i++ {
		msg, err := subJS.NextMsg(time.Second * 2)
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		gotEvents[msg.Subject]++
	}
	if c := gotEvents["$JS.EVENT.ADVISORY.STREAM.CREATED.ORDERS"]; c != 1 {
		t.Fatalf("Should have received one advisory from $JS.EVENT.ADVISORY.STREAM.CREATED.ORDERS but got %d", c)
	}
	if c := gotEvents["$JS.EVENT.ADVISORY.API"]; c != 1 {
		t.Fatalf("Should have received one advisory from $JS.EVENT.ADVISORY.API but got %d", c)
	}

	// same set of events should be received by AGG account
	// on subjects containing account name (ACC.JS)
	gotEvents = map[string]int{}
	for i := 0; i < 2; i++ {
		msg, err := subAgg.NextMsg(time.Second * 2)
		require_NoError(t, err)
		var adv JSAPIAudit
		require_NoError(t, json.Unmarshal(msg.Data, &adv))
		// Make sure we have full fidelity info via implicit share.
		if adv.Client != nil {
			require_True(t, adv.Client.Host != _EMPTY_)
			require_True(t, adv.Client.User != _EMPTY_)
			require_True(t, adv.Client.Lang != _EMPTY_)
		}
		gotEvents[msg.Subject]++
	}
	if c := gotEvents["$JS.EVENT.ADVISORY.ACC.JS.STREAM.CREATED.ORDERS"]; c != 1 {
		t.Fatalf("Should have received one advisory from $JS.EVENT.ADVISORY.ACC.JS.STREAM.CREATED.ORDERS but got %d", c)
	}
	if c := gotEvents["$JS.EVENT.ADVISORY.ACC.JS.API"]; c != 1 {
		t.Fatalf("Should have received one advisory from $JS.EVENT.ADVISORY.ACC.JS.API but got %d", c)
	}
}

// This tests whether we are able to aggregate all JetStream advisory events
// from all accounts into a single account. Config for this test uses
// stream imports and exports as that allows for gathering all events
// as long as there is a separate stream import entry for each account
// in aggregate account config.
func TestJetStreamAccountImportJSAdvisoriesAsStream(t *testing.T) {
	conf := createConfFile(t, []byte(fmt.Sprintf(`
		listen=127.0.0.1:-1
		no_auth_user: pp
		jetstream: {max_mem_store: 64GB, max_file_store: 10TB, store_dir: %q}
		accounts {
			JS {
				jetstream: enabled
				users: [ {user: pp, password: foo} ]
				exports [
					{ stream: '$JS.EVENT.ADVISORY.>' }
				]
			}
			AGG {
				users: [ {user: agg, password: foo} ]
				imports: [
					{ stream: { account: JS, subject: '$JS.EVENT.ADVISORY.>' }, to: '$JS.EVENT.ADVISORY.ACC.JS.>' }
				]
			}
		}
	`, t.TempDir())))

	s, _ := RunServerWithConfig(conf)
	defer s.Shutdown()

	// This should be the pp user, one which manages JetStream assets
	ncJS, err := nats.Connect(s.ClientURL())
	if err != nil {
		t.Fatalf("Unexpected error during connect: %v", err)
	}
	defer ncJS.Close()

	// This is the agg user, which should aggregate all JS advisory events.
	ncAgg, err := nats.Connect(s.ClientURL(), nats.UserInfo("agg", "foo"))
	if err != nil {
		t.Fatalf("Unexpected error during connect: %v", err)
	}
	defer ncAgg.Close()

	js, err := ncJS.JetStream()
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	// user from JS account should receive events on $JS.EVENT.ADVISORY.> subject
	subJS, err := ncJS.SubscribeSync("$JS.EVENT.ADVISORY.>")
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	defer subJS.Unsubscribe()
	require_NoError(t, ncJS.Flush())

	// user from AGG account should receive events on mapped $JS.EVENT.ADVISORY.ACC.JS.> subject (with account name)
	subAgg, err := ncAgg.SubscribeSync("$JS.EVENT.ADVISORY.ACC.JS.>")
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	defer subAgg.Unsubscribe()
	require_NoError(t, ncAgg.Flush())

	// add stream using JS account
	// this should trigger 2 events:
	// - an action event on $JS.EVENT.ADVISORY.STREAM.CREATED.ORDERS
	// - an api audit event on $JS.EVENT.ADVISORY.API
	_, err = js.AddStream(&nats.StreamConfig{Name: "ORDERS", Subjects: []string{"ORDERS.*"}})
	if err != nil {
		t.Fatalf("Unexpected error adding stream: %v", err)
	}

	var gotAPIAdvisory, gotCreateAdvisory bool
	for i := 0; i < 2; i++ {
		msg, err := subJS.NextMsg(time.Second * 2)
		if err != nil {
			t.Fatalf("Unexpected error on JS account: %v", err)
		}
		switch msg.Subject {
		case "$JS.EVENT.ADVISORY.STREAM.CREATED.ORDERS":
			gotCreateAdvisory = true
		case "$JS.EVENT.ADVISORY.API":
			gotAPIAdvisory = true
		default:
			t.Fatalf("Unexpected subject: %q", msg.Subject)
		}
	}
	if !gotAPIAdvisory || !gotCreateAdvisory {
		t.Fatalf("Expected to have received both advisories on JS account (API advisory %v, create advisory %v)", gotAPIAdvisory, gotCreateAdvisory)
	}

	// same set of events should be received by AGG account
	// on subjects containing account name (ACC.JS)
	gotAPIAdvisory, gotCreateAdvisory = false, false
	for i := 0; i < 2; i++ {
		msg, err := subAgg.NextMsg(time.Second * 2)
		if err != nil {
			t.Fatalf("Unexpected error on AGG account: %v", err)
		}
		switch msg.Subject {
		case "$JS.EVENT.ADVISORY.ACC.JS.STREAM.CREATED.ORDERS":
			gotCreateAdvisory = true
		case "$JS.EVENT.ADVISORY.ACC.JS.API":
			gotAPIAdvisory = true
		default:
			t.Fatalf("Unexpected subject: %q", msg.Subject)
		}
	}
	if !gotAPIAdvisory || !gotCreateAdvisory {
		t.Fatalf("Expected to have received both advisories on AGG account (API advisory %v, create advisory %v)", gotAPIAdvisory, gotCreateAdvisory)
	}
}

// This is for importing all of JetStream into another account for admin purposes.
func TestJetStreamAccountImportAll(t *testing.T) {
	conf := createConfFile(t, []byte(fmt.Sprintf(`
		listen: 127.0.0.1:-1
		no_auth_user: rip
		jetstream: {max_mem_store: 64GB, max_file_store: 10TB, store_dir: %q}
		accounts: {
			JS: {
				jetstream: enabled
				users: [ {user: dlc, password: foo} ]
				exports [ { service: "$JS.API.>" } ]
			},
			IU: {
				users: [ {user: rip, password: bar} ]
				imports [ { service: { subject: "$JS.API.>", account: JS }, to: "jsapi.>"} ]
			},
		}
	`, t.TempDir())))

	s, _ := RunServerWithConfig(conf)
	defer s.Shutdown()

	acc, err := s.LookupAccount("JS")
	if err != nil {
		t.Fatalf("Unexpected error looking up account: %v", err)
	}

	mset, err := acc.addStream(&StreamConfig{Name: "ORDERS", Subjects: []string{"ORDERS.*"}})
	if err != nil {
		t.Fatalf("Unexpected error adding stream: %v", err)
	}
	defer mset.delete()

	// This should be the rip user, the one that imports all of JS.
	nc := clientConnectToServer(t, s)
	defer nc.Close()

	mapSubj := func(subject string) string {
		return strings.Replace(subject, "$JS.API.", "jsapi.", 1)
	}

	// This will get the current information about usage and limits for this account.
	resp, err := nc.Request(mapSubj(JSApiAccountInfo), nil, time.Second)
	require_NoError(t, err)
	var info JSApiAccountInfoResponse
	if err := json.Unmarshal(resp.Data, &info); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if info.Error != nil {
		t.Fatalf("Unexpected error: %+v", info.Error)
	}
	// Lookup streams.
	resp, err = nc.Request(mapSubj(JSApiStreams), nil, time.Second)
	require_NoError(t, err)
	var namesResponse JSApiStreamNamesResponse
	if err = json.Unmarshal(resp.Data, &namesResponse); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if namesResponse.Error != nil {
		t.Fatalf("Unexpected error: %+v", namesResponse.Error)
	}
}

// https://github.com/nats-io/nats-server/issues/1736
func TestJetStreamServerReload(t *testing.T) {
	conf := createConfFile(t, []byte(fmt.Sprintf(`
		listen: 127.0.0.1:-1
		jetstream: {max_mem_store: 64GB, max_file_store: 10TB, store_dir: %q }
		accounts: {
			A: { users: [ {user: ua, password: pwd} ] },
			B: {
				jetstream: {max_mem: 1GB, max_store: 1TB, max_streams: 10, max_consumers: 1k}
				users: [ {user: ub, password: pwd} ]
			},
			SYS: { users: [ {user: uc, password: pwd} ] },
		}
		no_auth_user: ub
		system_account: SYS
	`, t.TempDir())))

	s, _ := RunServerWithConfig(conf)
	defer s.Shutdown()

	if !s.JetStreamEnabled() {
		t.Fatalf("Expected JetStream to be enabled")
	}

	// Client for API requests.
	nc := clientConnectToServer(t, s)
	defer nc.Close()

	checkJSAccount := func() {
		t.Helper()
		resp, err := nc.Request(JSApiAccountInfo, nil, time.Second)
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		var info JSApiAccountInfoResponse
		if err := json.Unmarshal(resp.Data, &info); err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
	}

	checkJSAccount()

	acc, err := s.LookupAccount("B")
	if err != nil {
		t.Fatalf("Unexpected error looking up account: %v", err)
	}
	mset, err := acc.addStream(&StreamConfig{Name: "22"})
	require_NoError(t, err)

	toSend := 10
	for i := 0; i < toSend; i++ {
		sendStreamMsg(t, nc, "22", fmt.Sprintf("MSG: %d", i+1))
	}
	if msgs := mset.state().Msgs; msgs != uint64(toSend) {
		t.Fatalf("Expected %d messages, got %d", toSend, msgs)
	}

	if err := s.Reload(); err != nil {
		t.Fatalf("Error on server reload: %v", err)
	}

	// Wait to get reconnected.
	checkFor(t, 5*time.Second, 10*time.Millisecond, func() error {
		if !nc.IsConnected() {
			return fmt.Errorf("Not connected")
		}
		return nil
	})

	checkJSAccount()
	sendStreamMsg(t, nc, "22", "MSG: 22")
}

func TestJetStreamConfigReloadWithGlobalAccount(t *testing.T) {
	tdir := t.TempDir()
	template := `
		listen: 127.0.0.1:-1
		authorization {
			users [
				{user: anonymous}
				{user: user1, password: %s}
			]
		}
		no_auth_user: anonymous
		jetstream {
			store_dir = %q
		}
	`
	conf := createConfFile(t, []byte(fmt.Sprintf(template, "pwd", tdir)))

	s, _ := RunServerWithConfig(conf)
	defer s.Shutdown()

	// Client for API requests.
	nc, js := jsClientConnect(t, s)
	defer nc.Close()

	checkJSAccount := func() {
		t.Helper()
		if _, err := js.AccountInfo(); err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
	}

	checkJSAccount()

	if _, err := js.AddStream(&nats.StreamConfig{Name: "foo"}); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	toSend := 10
	for i := 0; i < toSend; i++ {
		if _, err := js.Publish("foo", []byte(fmt.Sprintf("MSG: %d", i+1))); err != nil {
			t.Fatalf("Unexpected publish error: %v", err)
		}
	}
	si, err := js.StreamInfo("foo")
	require_NoError(t, err)
	if si.State.Msgs != uint64(toSend) {
		t.Fatalf("Expected %d msgs after restart, got %d", toSend, si.State.Msgs)
	}

	if err := os.WriteFile(conf, []byte(fmt.Sprintf(template, "pwd2", tdir)), 0666); err != nil {
		t.Fatalf("Error writing config: %v", err)
	}

	if err := s.Reload(); err != nil {
		t.Fatalf("Error during config reload: %v", err)
	}

	nc, js = jsClientConnect(t, s)
	defer nc.Close()

	// Try to add a new stream to the global account
	if _, err := js.AddStream(&nats.StreamConfig{Name: "bar"}); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	checkJSAccount()
}

// Test that we properly enforce per subject msg limits.
func TestJetStreamMaxMsgsPerSubject(t *testing.T) {
	const maxPer = 5
	msc := StreamConfig{
		Name:       "TEST",
		Subjects:   []string{"foo", "bar", "baz.*"},
		Storage:    MemoryStorage,
		MaxMsgsPer: maxPer,
	}
	fsc := msc
	fsc.Storage = FileStorage

	cases := []struct {
		name    string
		mconfig *StreamConfig
	}{
		{"MemoryStore", &msc},
		{"FileStore", &fsc},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			s := RunBasicJetStreamServer(t)
			defer s.Shutdown()

			mset, err := s.GlobalAccount().addStream(c.mconfig)
			if err != nil {
				t.Fatalf("Unexpected error adding stream: %v", err)
			}
			defer mset.delete()

			// Client for API requests.
			nc, js := jsClientConnect(t, s)
			defer nc.Close()

			pubAndCheck := func(subj string, num int, expectedNumMsgs uint64) {
				t.Helper()
				for i := 0; i < num; i++ {
					if _, err = js.Publish(subj, []byte("TSLA")); err != nil {
						t.Fatalf("Unexpected publish error: %v", err)
					}
				}
				si, err := js.StreamInfo("TEST")
				if err != nil {
					t.Fatalf("Unexpected error: %v", err)
				}
				if si.State.Msgs != expectedNumMsgs {
					t.Fatalf("Expected %d msgs, got %d", expectedNumMsgs, si.State.Msgs)
				}
			}

			pubAndCheck("foo", 1, 1)
			pubAndCheck("foo", 4, 5)
			// Now make sure our per subject limits kick in..
			pubAndCheck("foo", 2, 5)
			pubAndCheck("baz.22", 5, 10)
			pubAndCheck("baz.33", 5, 15)
			// We are maxed so totals should be same no matter what we add here.
			pubAndCheck("baz.22", 5, 15)
			pubAndCheck("baz.33", 5, 15)

			// Now purge and make sure all is still good.
			mset.purge(nil)
			pubAndCheck("foo", 1, 1)
			pubAndCheck("foo", 4, 5)
			pubAndCheck("baz.22", 5, 10)
			pubAndCheck("baz.33", 5, 15)
		})
	}
}

func TestJetStreamGetLastMsgBySubject(t *testing.T) {
	for _, st := range []StorageType{FileStorage, MemoryStorage} {
		t.Run(st.String(), func(t *testing.T) {
			c := createJetStreamClusterExplicit(t, "JSC", 3)
			defer c.shutdown()

			nc, js := jsClientConnect(t, c.randomServer())
			defer nc.Close()

			cfg := StreamConfig{
				Name:       "KV",
				Subjects:   []string{"kv.>"},
				Storage:    st,
				Replicas:   2,
				MaxMsgsPer: 20,
			}

			req, err := json.Marshal(cfg)
			if err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}
			// Do manually for now.
			nc.Request(fmt.Sprintf(JSApiStreamCreateT, cfg.Name), req, time.Second)
			si, err := js.StreamInfo("KV")
			if err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}
			if si == nil || si.Config.Name != "KV" {
				t.Fatalf("StreamInfo is not correct %+v", si)
			}

			for i := 0; i < 1000; i++ {
				msg := []byte(fmt.Sprintf("VAL-%d", i+1))
				js.PublishAsync("kv.foo", msg)
				js.PublishAsync("kv.bar", msg)
				js.PublishAsync("kv.baz", msg)
			}
			select {
			case <-js.PublishAsyncComplete():
			case <-time.After(5 * time.Second):
				t.Fatalf("Did not receive completion signal")
			}

			// Check that if both set that errors.
			b, _ := json.Marshal(JSApiMsgGetRequest{LastFor: "kv.foo", Seq: 950})
			rmsg, err := nc.Request(fmt.Sprintf(JSApiMsgGetT, "KV"), b, time.Second)
			if err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}
			var resp JSApiMsgGetResponse
			err = json.Unmarshal(rmsg.Data, &resp)
			if err != nil {
				t.Fatalf("Could not parse stream message: %v", err)
			}
			if resp.Error == nil {
				t.Fatalf("Expected an error when both are set, got %+v", resp.Error)
			}

			// Need to do stream GetMsg by hand for now until Go client support lands.
			getLast := func(subject string) *StoredMsg {
				t.Helper()
				req := &JSApiMsgGetRequest{LastFor: subject}
				b, _ := json.Marshal(req)
				rmsg, err := nc.Request(fmt.Sprintf(JSApiMsgGetT, "KV"), b, time.Second)
				if err != nil {
					t.Fatalf("Unexpected error: %v", err)
				}
				var resp JSApiMsgGetResponse
				err = json.Unmarshal(rmsg.Data, &resp)
				if err != nil {
					t.Fatalf("Could not parse stream message: %v", err)
				}
				if resp.Message == nil || resp.Error != nil {
					t.Fatalf("Did not receive correct response: %+v", resp.Error)
				}
				return resp.Message
			}
			// Do basic checks.
			basicCheck := func(subject string, expectedSeq uint64) {
				sm := getLast(subject)
				if sm == nil {
					t.Fatalf("Expected a message but got none")
				} else if string(sm.Data) != "VAL-1000" {
					t.Fatalf("Wrong message payload, wanted %q but got %q", "VAL-1000", sm.Data)
				} else if sm.Sequence != expectedSeq {
					t.Fatalf("Wrong message sequence, wanted %d but got %d", expectedSeq, sm.Sequence)
				} else if !subjectIsSubsetMatch(sm.Subject, subject) {
					t.Fatalf("Wrong subject, wanted %q but got %q", subject, sm.Subject)
				}
			}

			basicCheck("kv.foo", 2998)
			basicCheck("kv.bar", 2999)
			basicCheck("kv.baz", 3000)
			basicCheck("kv.*", 3000)
			basicCheck(">", 3000)
		})
	}
}

// https://github.com/nats-io/nats-server/issues/2329
func TestJetStreamGetLastMsgBySubjectAfterUpdate(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "JSC", 3)
	defer c.shutdown()

	nc, js := jsClientConnect(t, c.randomServer())
	defer nc.Close()

	sc := &nats.StreamConfig{
		Name:     "TEST",
		Subjects: []string{"foo"},
		Replicas: 2,
	}
	if _, err := js.AddStream(sc); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	// Now Update and add in other subjects.
	sc.Subjects = append(sc.Subjects, "bar", "baz")
	if _, err := js.UpdateStream(sc); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	js.Publish("foo", []byte("OK1")) // 1
	js.Publish("bar", []byte("OK1")) // 2
	js.Publish("foo", []byte("OK2")) // 3
	js.Publish("bar", []byte("OK2")) // 4

	// Need to do stream GetMsg by hand for now until Go client support lands.
	getLast := func(subject string) *StoredMsg {
		t.Helper()
		req := &JSApiMsgGetRequest{LastFor: subject}
		b, _ := json.Marshal(req)
		rmsg, err := nc.Request(fmt.Sprintf(JSApiMsgGetT, "TEST"), b, time.Second)
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		var resp JSApiMsgGetResponse
		err = json.Unmarshal(rmsg.Data, &resp)
		if err != nil {
			t.Fatalf("Could not parse stream message: %v", err)
		}
		if resp.Message == nil || resp.Error != nil {
			t.Fatalf("Did not receive correct response: %+v", resp.Error)
		}
		return resp.Message
	}
	// Do basic checks.
	basicCheck := func(subject string, expectedSeq uint64) {
		sm := getLast(subject)
		if sm == nil {
			t.Fatalf("Expected a message but got none")
		} else if sm.Sequence != expectedSeq {
			t.Fatalf("Wrong message sequence, wanted %d but got %d", expectedSeq, sm.Sequence)
		} else if !subjectIsSubsetMatch(sm.Subject, subject) {
			t.Fatalf("Wrong subject, wanted %q but got %q", subject, sm.Subject)
		}
	}

	basicCheck("foo", 3)
	basicCheck("bar", 4)
}

func TestJetStreamLastSequenceBySubject(t *testing.T) {
	for _, st := range []StorageType{FileStorage, MemoryStorage} {
		t.Run(st.String(), func(t *testing.T) {
			c := createJetStreamClusterExplicit(t, "JSC", 3)
			defer c.shutdown()

			nc, js := jsClientConnect(t, c.randomServer())
			defer nc.Close()

			cfg := StreamConfig{
				Name:       "KV",
				Subjects:   []string{"kv.>"},
				Storage:    st,
				Replicas:   3,
				MaxMsgsPer: 1,
			}

			req, err := json.Marshal(cfg)
			if err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}
			// Do manually for now.
			m, err := nc.Request(fmt.Sprintf(JSApiStreamCreateT, cfg.Name), req, time.Second)
			require_NoError(t, err)
			si, err := js.StreamInfo("KV")
			if err != nil {
				t.Fatalf("Unexpected error: %v, respmsg: %q", err, string(m.Data))
			}
			if si == nil || si.Config.Name != "KV" {
				t.Fatalf("StreamInfo is not correct %+v", si)
			}

			js.PublishAsync("kv.foo", []byte("1"))
			js.PublishAsync("kv.bar", []byte("2"))
			js.PublishAsync("kv.baz", []byte("3"))

			select {
			case <-js.PublishAsyncComplete():
			case <-time.After(time.Second):
				t.Fatalf("Did not receive completion signal")
			}

			// Now make sure we get an error if the last sequence is not correct per subject.
			pubAndCheck := func(subj, seq string, ok bool) {
				t.Helper()
				m := nats.NewMsg(subj)
				m.Data = []byte("HELLO")
				m.Header.Set(JSExpectedLastSubjSeq, seq)
				_, err := js.PublishMsg(m)
				if ok && err != nil {
					t.Fatalf("Unexpected error: %v", err)
				}
				if !ok && err == nil {
					t.Fatalf("Expected to get an error and got none")
				}
			}

			pubAndCheck("kv.foo", "1", true)  // So last is now 4.
			pubAndCheck("kv.foo", "1", false) // This should fail.
			pubAndCheck("kv.bar", "2", true)
			pubAndCheck("kv.bar", "5", true)
			pubAndCheck("kv.xxx", "5", false)
		})
	}
}

func TestJetStreamLastSequenceBySubjectWithSubject(t *testing.T) {
	test := func(replicas int, st StorageType) {
		t.Run(fmt.Sprintf("R%d/%s", replicas, st), func(t *testing.T) {
			c := createJetStreamClusterExplicit(t, "JSC", 3)
			defer c.shutdown()

			nc, js := jsClientConnect(t, c.randomServer())
			defer nc.Close()

			cfg := StreamConfig{
				Name:       "KV",
				Subjects:   []string{"kv.>"},
				Storage:    st,
				Replicas:   replicas,
				MaxMsgsPer: 1,
			}

			req, err := json.Marshal(cfg)
			if err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}
			// Do manually for now.
			m, err := nc.Request(fmt.Sprintf(JSApiStreamCreateT, cfg.Name), req, time.Second)
			require_NoError(t, err)
			si, err := js.StreamInfo("KV")
			if err != nil {
				t.Fatalf("Unexpected error: %v, respmsg: %q", err, string(m.Data))
			}
			if si == nil || si.Config.Name != "KV" {
				t.Fatalf("StreamInfo is not correct %+v", si)
			}

			js.PublishAsync("kv.1.foo", []byte("1:1")) // Last is 1 for kv.1.foo; 1 for kv.1.*;
			js.PublishAsync("kv.1.bar", []byte("1:2")) // Last is 2 for kv.1.bar; 2 for kv.1.*;
			js.PublishAsync("kv.2.foo", []byte("2:1")) // Last is 3 for kv.2.foo; 3 for kv.2.*;
			js.PublishAsync("kv.3.bar", []byte("3:1")) // Last is 4 for kv.3.bar; 4 for kv.3.*;
			js.PublishAsync("kv.1.baz", []byte("1:3")) // Last is 5 for kv.1.baz; 5 for kv.1.*;
			js.PublishAsync("kv.1.bar", []byte("1:4")) // Last is 6 for kv.1.baz; 6 for kv.1.*;
			js.PublishAsync("kv.2.baz", []byte("2:2")) // Last is 7 for kv.2.baz; 7 for kv.2.*;

			select {
			case <-js.PublishAsyncComplete():
			case <-time.After(time.Second):
				t.Fatalf("Did not receive completion signal")
			}

			// Now make sure we get an error if the last sequence is not correct per subject.
			pubAndCheck := func(subj, filterSubject, seq string, ok bool) {
				t.Helper()
				m := nats.NewMsg(subj)
				m.Data = []byte("HELLO")

				// Expect last to be seq.
				m.Header.Set(JSExpectedLastSubjSeq, seq)

				// Constrain the sequence restriction to a specific subject
				// e.g. "kv.1.*" for kv.1.foo, kv.1.bar, kv.1.baz; kv.2.* for kv.2.foo, kv.2.baz; kv.3.* for kv.3.bar
				m.Header.Set(JSExpectedLastSubjSeqSubj, filterSubject)
				_, err := js.PublishMsg(m)
				if ok && err != nil {
					t.Fatalf("Unexpected error: %v", err)
				}
				if !ok && err == nil {
					t.Fatalf("Expected to get an error and got none")
				}
			}

			pubAndCheck("kv.1.foo", "kv.1.*", "0", false)
			pubAndCheck("kv.1.bar", "kv.1.*", "0", false)
			pubAndCheck("kv.1.xxx", "kv.1.*", "0", false)
			pubAndCheck("kv.1.foo", "kv.1.*", "1", false)
			pubAndCheck("kv.1.bar", "kv.1.*", "1", false)
			pubAndCheck("kv.1.xxx", "kv.1.*", "1", false)
			pubAndCheck("kv.2.foo", "kv.2.*", "1", false)
			pubAndCheck("kv.2.bar", "kv.2.*", "1", false)
			pubAndCheck("kv.2.xxx", "kv.2.*", "1", false)
			pubAndCheck("kv.1.bar", "kv.1.*", "2", false)
			pubAndCheck("kv.1.bar", "kv.1.*", "3", false)
			pubAndCheck("kv.1.bar", "kv.1.*", "4", false)
			pubAndCheck("kv.1.bar", "kv.1.*", "5", false)
			pubAndCheck("kv.1.bar", "kv.1.*", "6", true) // Last is 8 for kv.1.bar; 8 for kv.1.*;
			pubAndCheck("kv.1.baz", "kv.1.*", "2", false)
			pubAndCheck("kv.1.bar", "kv.1.*", "7", false)
			pubAndCheck("kv.1.xxx", "kv.1.*", "8", true) // Last is 9 for kv.1.xxx; 9 for kv.1.*;
			pubAndCheck("kv.2.foo", "kv.2.*", "2", false)
			pubAndCheck("kv.2.foo", "kv.2.*", "7", true)  // Last is 10 for kv.2.foo; 10 for kv.2.*;
			pubAndCheck("kv.xxx", "kv.*", "0", true)      // Last is 0 for kv.xxx; 0 for kv.*;
			pubAndCheck("kv.xxx", "kv.*.*", "0", false)   // Last is 11 for kv.xxx; 11 for kv.*.*;
			pubAndCheck("kv.3.xxx", "kv.3.*", "4", true)  // Last is 12 for kv.3.xxx; 12 for kv.3.*;
			pubAndCheck("kv.3.xyz", "kv.3.*", "12", true) // Last is 13 for kv.3.xyz; 13 for kv.3.*;

			// When using the last-subj-seq-subj header, but the sequence header is missing.
			m = nats.NewMsg("kv.invalid")
			m.Data = []byte("HELLO")
			m.Header.Set(JSExpectedLastSubjSeqSubj, "kv.invalid")
			_, err = js.PublishMsg(m)
			require_Error(t, err, NewJSStreamExpectedLastSeqPerSubjectInvalidError())
		})
	}

	for _, replicas := range []int{1, 3} {
		for _, st := range []StorageType{FileStorage, MemoryStorage} {
			test(replicas, st)
		}
	}
}

func TestJetStreamFilteredConsumersWithWiderFilter(t *testing.T) {
	s := RunBasicJetStreamServer(t)
	defer s.Shutdown()

	// Client for API requests.
	nc, js := jsClientConnect(t, s)
	defer nc.Close()

	// Origin
	_, err := js.AddStream(&nats.StreamConfig{
		Name:     "TEST",
		Subjects: []string{"foo", "bar", "baz", "N.*"},
	})
	require_NoError(t, err)

	// Add in some messages.
	js.Publish("foo", []byte("OK"))
	js.Publish("bar", []byte("OK"))
	js.Publish("baz", []byte("OK"))
	for i := 0; i < 12; i++ {
		js.Publish(fmt.Sprintf("N.%d", i+1), []byte("OK"))
	}

	checkFor(t, 5*time.Second, 250*time.Millisecond, func() error {
		si, err := js.StreamInfo("TEST")
		require_NoError(t, err)

		if si.State.Msgs != 15 {
			return fmt.Errorf("Expected 15 msgs, got state: %+v", si.State)
		}
		return nil
	})

	checkWider := func(subj string, numExpected int) {
		sub, err := js.SubscribeSync(subj)
		require_NoError(t, err)

		defer sub.Unsubscribe()
		checkSubsPending(t, sub, numExpected)
	}

	checkWider("*", 3)
	checkWider("N.*", 12)
	checkWider("*.*", 12)
	checkWider("N.>", 12)
	checkWider(">", 15)
}

func TestJetStreamMirrorAndSourcesFilteredConsumers(t *testing.T) {
	s := RunBasicJetStreamServer(t)
	defer s.Shutdown()

	// Client for API requests.
	nc, js := jsClientConnect(t, s)
	defer nc.Close()

	// Origin
	_, err := js.AddStream(&nats.StreamConfig{
		Name:     "TEST",
		Subjects: []string{"foo", "bar", "baz.*"},
	})
	require_NoError(t, err)

	// Create Mirror now.
	_, err = js.AddStream(&nats.StreamConfig{
		Name:   "M",
		Mirror: &nats.StreamSource{Name: "TEST"},
	})
	require_NoError(t, err)

	dsubj := nats.NewInbox()
	nc.SubscribeSync(dsubj)
	nc.Flush()

	createConsumer := func(sn, fs string) {
		t.Helper()
		_, err = js.AddConsumer(sn, &nats.ConsumerConfig{DeliverSubject: dsubj, FilterSubject: fs})
		require_NoError(t, err)

	}

	createConsumer("M", "foo")
	createConsumer("M", "bar")
	createConsumer("M", "baz.foo")

	// Now do some sources.
	if _, err := js.AddStream(&nats.StreamConfig{Name: "O1", Subjects: []string{"foo.*"}}); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if _, err := js.AddStream(&nats.StreamConfig{Name: "O2", Subjects: []string{"bar.*"}}); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	// Create Mirror now.
	_, err = js.AddStream(&nats.StreamConfig{
		Name:    "S",
		Sources: []*nats.StreamSource{{Name: "O1"}, {Name: "O2"}},
	})
	require_NoError(t, err)

	createConsumer("S", "foo.1")
	createConsumer("S", "bar.1")

	// Chaining
	// Create Mirror now.
	_, err = js.AddStream(&nats.StreamConfig{
		Name:   "M2",
		Mirror: &nats.StreamSource{Name: "M"},
	})
	require_NoError(t, err)

	createConsumer("M2", "foo")
	createConsumer("M2", "bar")
	createConsumer("M2", "baz.foo")
}

func TestJetStreamMirrorBasics(t *testing.T) {
	s := RunBasicJetStreamServer(t)
	defer s.Shutdown()

	// Client for API requests.
	nc, js := jsClientConnect(t, s)
	defer nc.Close()

	createStream := func(cfg *nats.StreamConfig) (*nats.StreamInfo, error) {
		return js.AddStream(cfg)
	}

	createStreamOk := func(cfg *nats.StreamConfig) {
		t.Helper()
		if _, err := createStream(cfg); err != nil {
			t.Fatalf("Expected no error, got %+v", err)
		}
	}

	// Test we get right config errors etc.
	cfg := &nats.StreamConfig{
		Name:     "M1",
		Subjects: []string{"foo", "bar", "baz"},
		Mirror:   &nats.StreamSource{Name: "S1"},
	}
	_, err := createStream(cfg)
	if err == nil || !strings.Contains(err.Error(), "stream mirrors can not") {
		t.Fatalf("Expected error, got %+v", err)
	}

	// Clear subjects.
	cfg.Subjects = nil

	// Mirrored
	scfg := &nats.StreamConfig{
		Name:     "S1",
		Subjects: []string{"foo", "bar", "baz"},
	}

	// Create mirrored stream
	createStreamOk(scfg)

	// Now create our mirror stream.
	createStreamOk(cfg)

	// For now wait for the consumer state to register.
	time.Sleep(250 * time.Millisecond)

	// Send 100 messages.
	for i := 0; i < 100; i++ {
		if _, err := js.Publish("foo", []byte("OK")); err != nil {
			t.Fatalf("Unexpected publish error: %v", err)
		}
	}

	// Faster timeout since we loop below checking for condition.
	js2, err := nc.JetStream(nats.MaxWait(500 * time.Millisecond))
	require_NoError(t, err)

	checkFor(t, 5*time.Second, 250*time.Millisecond, func() error {
		si, err := js2.StreamInfo("M1")
		require_NoError(t, err)

		if si.State.Msgs != 100 {
			return fmt.Errorf("Expected 100 msgs, got state: %+v", si.State)
		}
		return nil
	})

	// Purge the mirrored stream.
	if err := js.PurgeStream("S1"); err != nil {
		t.Fatalf("Unexpected purge error: %v", err)
	}
	// Send 50 more msgs now.
	for i := 0; i < 50; i++ {
		if _, err := js.Publish("bar", []byte("OK")); err != nil {
			t.Fatalf("Unexpected publish error: %v", err)
		}
	}

	cfg = &nats.StreamConfig{
		Name:    "M2",
		Storage: nats.FileStorage,
		Mirror:  &nats.StreamSource{Name: "S1"},
	}

	// Now create our second mirror stream.
	createStreamOk(cfg)

	checkFor(t, 5*time.Second, 250*time.Millisecond, func() error {
		si, err := js2.StreamInfo("M2")
		require_NoError(t, err)

		if si.State.Msgs != 50 {
			return fmt.Errorf("Expected 50 msgs, got state: %+v", si.State)
		}
		if si.State.FirstSeq != 101 {
			return fmt.Errorf("Expected start seq of 101, got state: %+v", si.State)
		}
		return nil
	})

	// Send 100 more msgs now. Should be 150 total, 101 first.
	for i := 0; i < 100; i++ {
		if _, err := js.Publish("baz", []byte("OK")); err != nil {
			t.Fatalf("Unexpected publish error: %v", err)
		}
	}

	cfg = &nats.StreamConfig{
		Name:   "M3",
		Mirror: &nats.StreamSource{Name: "S1", OptStartSeq: 150},
	}

	createStreamOk(cfg)

	checkFor(t, 5*time.Second, 250*time.Millisecond, func() error {
		si, err := js2.StreamInfo("M3")
		require_NoError(t, err)

		if si.State.Msgs != 101 {
			return fmt.Errorf("Expected 101 msgs, got state: %+v", si.State)
		}
		if si.State.FirstSeq != 150 {
			return fmt.Errorf("Expected start seq of 150, got state: %+v", si.State)
		}
		return nil
	})

	// Make sure setting time works ok.
	start := time.Now().UTC().Add(-2 * time.Hour)
	cfg = &nats.StreamConfig{
		Name:   "M4",
		Mirror: &nats.StreamSource{Name: "S1", OptStartTime: &start},
	}
	createStreamOk(cfg)

	checkFor(t, 5*time.Second, 250*time.Millisecond, func() error {
		si, err := js2.StreamInfo("M4")
		require_NoError(t, err)

		if si.State.Msgs != 150 {
			return fmt.Errorf("Expected 150 msgs, got state: %+v", si.State)
		}
		if si.State.FirstSeq != 101 {
			return fmt.Errorf("Expected start seq of 101, got state: %+v", si.State)
		}
		return nil
	})

	// Test subject filtering and transformation
	createStreamServerStreamConfig := func(cfg *StreamConfig, errToCheck uint16) {
		t.Helper()
		req, err := json.Marshal(cfg)
		require_NoError(t, err)

		rm, err := nc.Request(fmt.Sprintf(JSApiStreamCreateT, cfg.Name), req, time.Second)
		require_NoError(t, err)

		var resp JSApiStreamCreateResponse
		if err := json.Unmarshal(rm.Data, &resp); err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}

		if errToCheck == 0 {
			if resp.Error != nil {
				t.Fatalf("Unexpected error: %+v", resp.Error)
			}
		} else {
			if resp.Error.ErrCode != errToCheck {
				t.Fatalf("Expected error %+v, got: %+v", errToCheck, resp.Error)
			}
		}
	}

	// check for errors
	createStreamServerStreamConfig(&StreamConfig{
		Name:    "MBAD",
		Storage: FileStorage,
		Mirror:  &StreamSource{Name: "S1", FilterSubject: "foo", SubjectTransforms: []SubjectTransformConfig{{Source: "foo", Destination: "foo3"}}},
	}, ApiErrors[JSMirrorMultipleFiltersNotAllowed].ErrCode)

	createStreamServerStreamConfig(&StreamConfig{
		Name:    "MBAD",
		Storage: FileStorage,
		Mirror:  &StreamSource{Name: "S1", SubjectTransforms: []SubjectTransformConfig{{Source: ".*.", Destination: "foo3"}}},
	}, ApiErrors[JSMirrorInvalidSubjectFilter].ErrCode)

	createStreamServerStreamConfig(&StreamConfig{
		Name:    "MBAD",
		Storage: FileStorage,
		Mirror:  &StreamSource{Name: "S1", SubjectTransforms: []SubjectTransformConfig{{Source: "*", Destination: "{{wildcard(2)}}"}}},
	}, ApiErrors[JSMirrorInvalidTransformDestination].ErrCode)

	createStreamServerStreamConfig(&StreamConfig{
		Name:    "MBAD",
		Storage: FileStorage,
		Mirror:  &StreamSource{Name: "S1", SubjectTransforms: []SubjectTransformConfig{{Source: "foo", Destination: ""}, {Source: "foo", Destination: "bar"}}},
	}, ApiErrors[JSMirrorOverlappingSubjectFilters].ErrCode)

	createStreamServerStreamConfig(&StreamConfig{
		Name:    "M5",
		Storage: FileStorage,
		Mirror:  &StreamSource{Name: "S1", SubjectTransforms: []SubjectTransformConfig{{Source: "foo", Destination: "foo2"}}},
	}, 0)

	createStreamServerStreamConfig(&StreamConfig{
		Name:    "M6",
		Storage: FileStorage,
		Mirror:  &StreamSource{Name: "S1", SubjectTransforms: []SubjectTransformConfig{{Source: "bar", Destination: "bar2"}, {Source: "baz", Destination: "baz2"}}},
	}, 0)

	// Send 100 messages on foo (there should already be 50 messages on bar and 100 on baz in the stream)
	for i := 0; i < 100; i++ {
		if _, err := js.Publish("foo", []byte("OK")); err != nil {
			t.Fatalf("Unexpected publish error: %v", err)
		}
	}

	var f = func(streamName string, subject string, subjectNumMsgs uint64, streamNumMsg uint64, firstSeq uint64, lastSeq uint64) func() error {
		return func() error {
			si, err := js2.StreamInfo(streamName, &nats.StreamInfoRequest{SubjectsFilter: subject})
			require_NoError(t, err)
			if ss, ok := si.State.Subjects[subject]; !ok {
				return fmt.Errorf("expected messages with the transformed subject %s", subject)
			} else {
				if ss != subjectNumMsgs {
					return fmt.Errorf("expected %d messages on the transformed subject %s but got %d", subjectNumMsgs, subject, ss)
				}
			}
			if si.State.Msgs != streamNumMsg {
				return fmt.Errorf("expected %d stream messages, got state: %+v", streamNumMsg, si.State)
			}
			if si.State.FirstSeq != firstSeq || si.State.LastSeq != lastSeq {
				return fmt.Errorf("expected first sequence=%d and last sequence=%d, but got state: %+v", firstSeq, lastSeq, si.State)
			}
			return nil
		}
	}

	checkFor(t, 10*time.Second, 500*time.Millisecond, f("M5", "foo2", 100, 100, 251, 350))
	checkFor(t, 10*time.Second, 500*time.Millisecond, f("M6", "bar2", 50, 150, 101, 250))
	checkFor(t, 10*time.Second, 500*time.Millisecond, f("M6", "baz2", 100, 150, 101, 250))

}

func TestJetStreamSourceStartSeqRespectedOnEmptyOrigin(t *testing.T) {
	s := RunBasicJetStreamServer(t)
	defer s.Shutdown()

	nc, js := jsClientConnect(t, s)
	defer nc.Close()

	waitForSourcingConsumer := func(origin string) {
		t.Helper()
		checkFor(t, 5*time.Second, 100*time.Millisecond, func() error {
			mset, err := s.globalAccount().lookupStream(origin)
			if err != nil {
				return err
			}
			if mset.numDirectConsumers() == 0 {
				return fmt.Errorf("no sourcing consumer attached to %q yet", origin)
			}
			return nil
		})
	}

	// Publish origin sequences 1..15, with the body carrying the origin sequence
	// so we can assert exactly which messages were sourced.
	publishOrigin := func(subject string) {
		t.Helper()
		for i := 1; i <= 15; i++ {
			_, err := js.Publish(subject, []byte(strconv.Itoa(i)))
			require_NoError(t, err)
		}
	}

	t.Run("mirror", func(t *testing.T) {
		// Origin stream, empty.
		_, err := js.AddStream(&nats.StreamConfig{Name: "MO", Subjects: []string{"m"}})
		require_NoError(t, err)

		// Mirror requesting to start at origin sequence 10, while the origin is empty.
		_, err = js.AddStream(&nats.StreamConfig{
			Name:   "M",
			Mirror: &nats.StreamSource{Name: "MO", OptStartSeq: 10},
		})
		require_NoError(t, err)

		waitForSourcingConsumer("MO")
		publishOrigin("m")

		// Mirrors preserve origin sequences, so the mirror must contain exactly
		// origin sequences 10..15 and nothing below 10.
		checkFor(t, 5*time.Second, 100*time.Millisecond, func() error {
			si, err := js.StreamInfo("M")
			if err != nil {
				return err
			}
			if si.State.Msgs != 6 {
				return fmt.Errorf("expected 6 msgs (origin 10..15), got state: %+v", si.State)
			}
			if si.State.FirstSeq != 10 {
				return fmt.Errorf("expected first seq 10, got state: %+v", si.State)
			}
			if si.State.LastSeq != 15 {
				return fmt.Errorf("expected last seq 15, got state: %+v", si.State)
			}
			return nil
		})
	})

	t.Run("source", func(t *testing.T) {
		// Origin stream, empty.
		_, err := js.AddStream(&nats.StreamConfig{Name: "SO", Subjects: []string{"s"}})
		require_NoError(t, err)

		// Source requesting to start at origin sequence 10, while the origin is empty.
		_, err = js.AddStream(&nats.StreamConfig{
			Name:    "S",
			Sources: []*nats.StreamSource{{Name: "SO", OptStartSeq: 10}},
		})
		require_NoError(t, err)

		waitForSourcingConsumer("SO")
		publishOrigin("s")

		// A source re-sequences into the destination, so assert on message count and
		// on the origin sequences that were actually sourced (carried in the body).
		checkFor(t, 5*time.Second, 100*time.Millisecond, func() error {
			si, err := js.StreamInfo("S")
			if err != nil {
				return err
			}
			if si.State.Msgs != 6 {
				return fmt.Errorf("expected 6 msgs (origin 10..15), got state: %+v", si.State)
			}
			return nil
		})

		// The first and last sourced messages must be origin sequences 10 and 15,
		// proving nothing below the requested start sequence was sourced.
		first, err := js.GetMsg("S", 1)
		require_NoError(t, err)
		require_Equal(t, string(first.Data), "10")

		last, err := js.GetMsg("S", 6)
		require_NoError(t, err)
		require_Equal(t, string(last.Data), "15")
	})
}

func TestJetStreamMirrorStripExpectedHeaders(t *testing.T) {
	s := RunBasicJetStreamServer(t)
	defer s.Shutdown()

	// Client for API requests.
	nc, js := jsClientConnect(t, s)
	defer nc.Close()

	// Create source and mirror streams.
	_, err := js.AddStream(&nats.StreamConfig{
		Name:     "S",
		Subjects: []string{"foo"},
	})
	require_NoError(t, err)
	_, err = js.AddStream(&nats.StreamConfig{
		Name:   "M",
		Mirror: &nats.StreamSource{Name: "S"},
	})
	require_NoError(t, err)

	m := nats.NewMsg("foo")
	pubAck, err := js.PublishMsg(m)
	require_NoError(t, err)
	require_Equal(t, pubAck.Sequence, 1)

	// Mirror should get message.
	checkFor(t, 2*time.Second, 200*time.Millisecond, func() error {
		if si, err := js.StreamInfo("M"); err != nil {
			return err
		} else if si.State.Msgs != 1 {
			return fmt.Errorf("expected 1 mirrored msg, got %d", si.State.Msgs)
		}
		return nil
	})

	m.Header.Set("Nats-Expected-Stream", "S")
	pubAck, err = js.PublishMsg(m)
	require_NoError(t, err)
	require_Equal(t, pubAck.Sequence, 2)

	// Mirror should strip expected headers and store the message.
	checkFor(t, 2*time.Second, 200*time.Millisecond, func() error {
		if si, err := js.StreamInfo("M"); err != nil {
			return err
		} else if si.State.Msgs != 2 {
			return fmt.Errorf("expected 2 mirrored msgs, got %d", si.State.Msgs)
		}
		return nil
	})
}

func TestJetStreamMirrorUpdatePreventsSubjects(t *testing.T) {
	s := RunBasicJetStreamServer(t)
	defer s.Shutdown()

	// Client for API requests.
	nc, js := jsClientConnect(t, s)
	defer nc.Close()

	_, err := js.AddStream(&nats.StreamConfig{Name: "ORIGINAL"})
	require_NoError(t, err)

	_, err = js.AddStream(&nats.StreamConfig{Name: "MIRROR", Mirror: &nats.StreamSource{Name: "ORIGINAL"}})
	require_NoError(t, err)

	_, err = js.UpdateStream(&nats.StreamConfig{Name: "MIRROR", Mirror: &nats.StreamSource{Name: "ORIGINAL"}, Subjects: []string{"x"}})
	if err == nil || err.Error() != "nats: stream mirrors can not contain subjects" {
		t.Fatalf("Expected to not be able to put subjects on a stream, got: %+v", err)
	}
}

func TestJetStreamSourceBasics(t *testing.T) {
	s := RunBasicJetStreamServer(t)
	defer s.Shutdown()

	// Client for API requests.
	nc, js := jsClientConnect(t, s)
	defer nc.Close()

	createStream := func(cfg *StreamConfig) {
		t.Helper()
		req, err := json.Marshal(cfg)
		require_NoError(t, err)

		rm, err := nc.Request(fmt.Sprintf(JSApiStreamCreateT, cfg.Name), req, time.Second)
		require_NoError(t, err)

		var resp JSApiStreamCreateResponse
		if err := json.Unmarshal(rm.Data, &resp); err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		if resp.Error != nil {
			t.Fatalf("Unexpected error: %+v", resp.Error)
		}
	}

	if _, err := js.AddStream(&nats.StreamConfig{Name: "test", Sources: []*nats.StreamSource{{Name: ""}}}); err.Error() == "source stream name is invalid" {
		t.Fatal("Expected a source stream name is invalid error")
	}

	for _, sname := range []string{"foo", "bar", "baz"} {
		if _, err := js.AddStream(&nats.StreamConfig{Name: sname}); err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
	}
	sendBatch := func(subject string, n int) {
		for i := 0; i < n; i++ {
			if _, err := js.Publish(subject, []byte("OK")); err != nil {
				t.Fatalf("Unexpected publish error: %v", err)
			}
		}
	}
	// Populate each one.
	sendBatch("foo", 10)
	sendBatch("bar", 15)
	sendBatch("baz", 25)

	cfg := &StreamConfig{
		Name:    "MS",
		Storage: FileStorage,
		Sources: []*StreamSource{
			{Name: "foo", SubjectTransforms: []SubjectTransformConfig{{Source: ">", Destination: "foo2.>"}}},
			{Name: "bar"},
			{Name: "baz"},
		},
	}

	createStream(cfg)

	// Faster timeout since we loop below checking for condition.
	js2, err := nc.JetStream(nats.MaxWait(250 * time.Millisecond))
	require_NoError(t, err)

	checkFor(t, 2*time.Second, 100*time.Millisecond, func() error {
		si, err := js2.StreamInfo("MS")
		require_NoError(t, err)

		if si.State.Msgs != 50 {
			return fmt.Errorf("Expected 50 msgs, got state: %+v", si.State)
		}
		return nil
	})

	ss, err := js.SubscribeSync("foo2.foo", nats.BindStream("MS"))
	require_NoError(t, err)
	// we must have at least one message on the transformed subject name (ie no timeout)
	_, err = ss.NextMsg(time.Millisecond)
	require_NoError(t, err)
	ss.Drain()

	// Test Source Updates
	ncfg := &nats.StreamConfig{
		Name: "MS",
		Sources: []*nats.StreamSource{
			// Keep foo, bar, remove baz, add dlc
			{Name: "foo"},
			{Name: "bar"},
			{Name: "dlc"},
		},
	}
	if _, err := js.UpdateStream(ncfg); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	// Test optional start times, filtered subjects etc.
	if _, err := js.AddStream(&nats.StreamConfig{Name: "TEST", Subjects: []string{"dlc", "rip", "jnm"}}); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	sendBatch("dlc", 20)
	sendBatch("rip", 20)
	sendBatch("dlc", 10)
	sendBatch("jnm", 10)

	cfg = &StreamConfig{
		Name:    "FMS",
		Storage: FileStorage,
		Sources: []*StreamSource{
			{Name: "TEST", OptStartSeq: 26},
		},
	}
	createStream(cfg)
	checkFor(t, 2*time.Second, 100*time.Millisecond, func() error {
		si, err := js2.StreamInfo("FMS")
		require_NoError(t, err)
		if si.State.Msgs != 35 {
			return fmt.Errorf("Expected 35 msgs, got state: %+v", si.State)
		}
		return nil
	})
	// Double check first starting.
	m, err := js.GetMsg("FMS", 1)
	require_NoError(t, err)
	if shdr := m.Header.Get(JSStreamSource); shdr == _EMPTY_ {
		t.Fatalf("Expected a header, got none")
	} else if _, _, sseq, _ := streamAndSeq(shdr); sseq != 26 {
		t.Fatalf("Expected header sequence of 26, got %d", sseq)
	}

	// Test Filters
	cfg = &StreamConfig{
		Name:    "FMS2",
		Storage: FileStorage,
		Sources: []*StreamSource{
			{Name: "TEST", OptStartSeq: 11, SubjectTransforms: []SubjectTransformConfig{{Source: "dlc", Destination: "dlc2"}}},
		},
	}
	createStream(cfg)
	checkFor(t, 2*time.Second, 100*time.Millisecond, func() error {
		si, err := js2.StreamInfo("FMS2")
		require_NoError(t, err)
		if si.State.Msgs != 20 {
			return fmt.Errorf("Expected 20 msgs, got state: %+v", si.State)
		}
		return nil
	})

	// Double check first starting.
	if m, err = js.GetMsg("FMS2", 1); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if shdr := m.Header.Get(JSStreamSource); shdr == _EMPTY_ {
		t.Fatalf("Expected a header, got none")
	} else if _, _, sseq, _ := streamAndSeq(shdr); sseq != 11 {
		t.Fatalf("Expected header sequence of 11, got %d", sseq)
	}
	if m.Subject != "dlc2" {
		t.Fatalf("Expected transformed subject dlc2, but got %s instead", m.Subject)
	}

	// Test Filters
	cfg = &StreamConfig{
		Name:    "FMS3",
		Storage: FileStorage,
		Sources: []*StreamSource{
			{Name: "TEST", SubjectTransforms: []SubjectTransformConfig{{Source: "dlc", Destination: "dlc2"}, {Source: "rip", Destination: ""}}},
		},
	}
	createStream(cfg)
	checkFor(t, 2*time.Second, 100*time.Millisecond, func() error {
		si, err := js2.StreamInfo("FMS3")
		require_NoError(t, err)
		if si.State.Msgs != 50 {
			return fmt.Errorf("Expected 50 msgs, got state: %+v", si.State)
		}
		return nil
	})

	// Double check first message
	if m, err = js.GetMsg("FMS3", 1); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if shdr := m.Header.Get(JSStreamSource); shdr == _EMPTY_ {
		t.Fatalf("Expected a header, got none")
	} else if m.Subject != "dlc2" {
		t.Fatalf("Expected subject 'dlc2' and got %s", m.Subject)
	}

	// Double check first message with the other subject
	if m, err = js.GetMsg("FMS3", 21); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if shdr := m.Header.Get(JSStreamSource); shdr == _EMPTY_ {
		t.Fatalf("Expected a header, got none")
	} else if m.Subject != "rip" {
		t.Fatalf("Expected subject 'rip' and got %s", m.Subject)
	}

	checkFor(t, 2*time.Second, 100*time.Millisecond, func() error {
		si, err := js2.StreamInfo("FMS3")
		require_NoError(t, err)
		if si.State.Subjects["jnm"] != 0 {
			return fmt.Errorf("Unexpected messages from the source found")
		}
		return nil
	})

	// pre 2.10 backwards compatibility
	transformConfig := nats.SubjectTransformConfig{Source: "B.*", Destination: "A.{{Wildcard(1)}}"}
	aConfig := nats.StreamConfig{Name: "A", Subjects: []string{"B.*"}, SubjectTransform: &transformConfig}
	if _, err := js.AddStream(&aConfig); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	sendBatch("B.A", 1)
	sendBatch("B.B", 1)
	bConfig := nats.StreamConfig{Name: "B", Subjects: []string{"A.*"}}

	if _, err := js.AddStream(&bConfig); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	// Fake a message that would have been sourced with pre 2.10, as if it
	// already existed in B's store from an old server.
	mset, err := s.globalAccount().lookupStream("B")
	require_NoError(t, err)
	// pre 2.10 header format just stream name and sequence number
	srcHdr := genHeader(nil, JSStreamSource, "A 1")
	_, _, err = mset.store.StoreMsg("A.A", srcHdr, []byte("OK"), 0)
	require_NoError(t, err)

	bConfig.Sources = []*nats.StreamSource{{Name: "A"}}
	if _, err := js.UpdateStream(&bConfig); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	checkFor(t, 2*time.Second, 100*time.Millisecond, func() error {
		si, err := js2.StreamInfo("B")
		require_NoError(t, err)
		if si.State.Msgs != 2 {
			return fmt.Errorf("Expected 2 msgs, got state: %+v", si.State)
		}
		return nil
	})

	// The pre 2.10 sourced message must be honored: B should hold the faked
	// A.A plus only the newly sourced A.B, not a re-sourced copy of A.A.
	if m, err := js.GetMsg("B", 1); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	} else if m.Subject != "A.A" {
		t.Fatalf("Expected subject 'A.A' and got %s", m.Subject)
	} else if shdr := m.Header.Get(JSStreamSource); shdr != "A 1" {
		t.Fatalf("Expected pre 2.10 source header 'A 1', got %q", shdr)
	}
	if m, err := js.GetMsg("B", 2); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	} else if m.Subject != "A.B" {
		t.Fatalf("Expected subject 'A.B' and got %s", m.Subject)
	}
}

func TestJetStreamSourceWorkingQueueWithLimit(t *testing.T) {
	s := RunBasicJetStreamServer(t)
	defer s.Shutdown()

	// Client for API requests.
	nc, js := jsClientConnect(t, s)
	defer nc.Close()

	_, err := js.AddStream(&nats.StreamConfig{Name: "test", Subjects: []string{"test"}})
	require_NoError(t, err)

	_, err = js.AddStream(&nats.StreamConfig{Name: "wq", MaxMsgs: 100, Discard: nats.DiscardNew, Retention: nats.WorkQueuePolicy,
		Sources: []*nats.StreamSource{{Name: "test"}}})
	require_NoError(t, err)

	sendBatch := func(subject string, n int) {
		for i := 0; i < n; i++ {
			_, err = js.Publish(subject, []byte("OK"))
			require_NoError(t, err)
		}
	}
	// Populate each one.
	sendBatch("test", 300)

	checkFor(t, 3*time.Second, 250*time.Millisecond, func() error {
		si, err := js.StreamInfo("wq")
		require_NoError(t, err)
		if si.State.Msgs != 100 {
			return fmt.Errorf("Expected 100 msgs, got state: %+v", si.State)
		}
		return nil
	})

	_, err = js.AddConsumer("wq", &nats.ConsumerConfig{Durable: "wqc", FilterSubject: "test", AckPolicy: nats.AckExplicitPolicy})
	require_NoError(t, err)

	ss, err := js.PullSubscribe("test", "wqc", nats.Bind("wq", "wqc"))
	require_NoError(t, err)
	// we must have at least one message on the transformed subject name (ie no timeout)
	f := func(done chan bool) {
		for i := 0; i < 300; i++ {
			m, err := ss.Fetch(1, nats.MaxWait(3*time.Second))
			require_NoError(t, err)
			time.Sleep(11 * time.Millisecond)
			err = m[0].Ack()
			require_NoError(t, err)
		}
		done <- true
	}

	var doneChan = make(chan bool)
	go f(doneChan)

	checkFor(t, 6*time.Second, 100*time.Millisecond, func() error {
		si, err := js.StreamInfo("wq")
		require_NoError(t, err)
		if si.State.Msgs > 0 && si.State.Msgs <= 100 {
			return fmt.Errorf("Expected 0 msgs, got: %d", si.State.Msgs)
		} else if si.State.Msgs > 100 {
			t.Fatalf("Got more than our 100 message limit: %+v", si.State)
		}
		return nil
	})

	select {
	case <-doneChan:
		ss.Drain()
	case <-time.After(5 * time.Second):
		t.Fatalf("Did not receive completion signal")
	}
}

func TestJetStreamStreamSourceFromKV(t *testing.T) {
	s := RunBasicJetStreamServer(t)
	defer s.Shutdown()

	// Client for API reuqests.
	nc, js := jsClientConnect(t, s)
	defer nc.Close()

	// Create a kv store
	kv, err := js.CreateKeyValue(&nats.KeyValueConfig{Bucket: "test"})
	require_NoError(t, err)

	// Create a stream with a source from the kv store
	_, err = js.AddStream(&nats.StreamConfig{Name: "test", Retention: nats.InterestPolicy, Sources: []*nats.StreamSource{{Name: "KV_" + kv.Bucket()}}})
	require_NoError(t, err)

	// Create a interested consumer
	_, err = js.AddConsumer("test", &nats.ConsumerConfig{Durable: "durable", AckPolicy: nats.AckExplicitPolicy})
	require_NoError(t, err)

	ss, err := js.PullSubscribe("", "", nats.Bind("test", "durable"))
	require_NoError(t, err)

	rev1, err := kv.Create("key", []byte("value1"))
	require_NoError(t, err)

	m, err := ss.Fetch(1, nats.MaxWait(2*time.Second))
	require_NoError(t, err)
	require_NoError(t, m[0].Ack())
	if string(m[0].Data) != "value1" {
		t.Fatalf("Expected value1, got %s", m[0].Data)
	}

	rev2, err := kv.Update("key", []byte("value2"), rev1)
	require_NoError(t, err)

	_, err = kv.Update("key", []byte("value3"), rev2)
	require_NoError(t, err)

	m, err = ss.Fetch(1, nats.MaxWait(500*time.Millisecond))
	require_NoError(t, err)
	require_NoError(t, m[0].Ack())
	if string(m[0].Data) != "value2" {
		t.Fatalf("Expected value2, got %s", m[0].Data)
	}

	m, err = ss.Fetch(1, nats.MaxWait(500*time.Millisecond))
	require_NoError(t, err)
	require_NoError(t, m[0].Ack())
	if string(m[0].Data) != "value3" {
		t.Fatalf("Expected value3, got %s", m[0].Data)
	}
}

func TestJetStreamInputTransform(t *testing.T) {
	s := RunBasicJetStreamServer(t)
	defer s.Shutdown()

	// Client for API requests.
	nc, js := jsClientConnect(t, s)
	defer nc.Close()

	createStream := func(cfg *StreamConfig) {
		t.Helper()
		req, err := json.Marshal(cfg)
		require_NoError(t, err)

		rm, err := nc.Request(fmt.Sprintf(JSApiStreamCreateT, cfg.Name), req, time.Second)
		require_NoError(t, err)

		var resp JSApiStreamCreateResponse
		if err := json.Unmarshal(rm.Data, &resp); err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		if resp.Error != nil {
			t.Fatalf("Unexpected error: %+v", resp.Error)
		}
	}

	createStream(&StreamConfig{Name: "T1", Subjects: []string{"foo"}, SubjectTransform: &SubjectTransformConfig{Source: ">", Destination: "transformed.>"}, Storage: MemoryStorage})

	// publish a message
	if _, err := js.Publish("foo", []byte("OK")); err != nil {
		t.Fatalf("Unexpected publish error: %v", err)
	}

	m, err := js.GetMsg("T1", 1)
	require_NoError(t, err)

	if m.Subject != "transformed.foo" {
		t.Fatalf("Expected message subject transformed.foo, got %s", m.Subject)
	}
}

func TestJetStreamOperatorAccounts(t *testing.T) {
	s, _ := RunServerWithConfig("./configs/js-op.conf")
	if config := s.JetStreamConfig(); config != nil {
		defer removeDir(t, config.StoreDir)
	}
	defer s.Shutdown()

	nc, js := jsClientConnect(t, s, nats.UserCredentials("./configs/one.creds"))
	defer nc.Close()

	if _, err := js.AddStream(&nats.StreamConfig{Name: "TEST"}); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	toSend := 100
	for i := 0; i < toSend; i++ {
		if _, err := js.Publish("TEST", []byte("OK")); err != nil {
			t.Fatalf("Unexpected publish error: %v", err)
		}
	}

	// Close our user for account one.
	nc.Close()

	// Restart the server.
	s.Shutdown()
	s, _ = RunServerWithConfig("./configs/js-op.conf")
	defer s.Shutdown()

	jsz, err := s.Jsz(nil)
	require_NoError(t, err)

	if jsz.Streams != 1 {
		t.Fatalf("Expected jsz to report our stream on restart")
	}
	if jsz.Messages != uint64(toSend) {
		t.Fatalf("Expected jsz to report our %d messages on restart, got %d", toSend, jsz.Messages)
	}
}

func TestJetStreamServerDomainBadConfig(t *testing.T) {
	shouldFail := func(domain string) {
		t.Helper()
		opts := DefaultTestOptions
		opts.JetStreamDomain = domain
		if err := validateOptions(&opts); err == nil || !strings.Contains(err.Error(), "invalid domain name") {
			t.Fatalf("Expected bad domain error, got %v", err)
		}
	}

	shouldFail("HU..B")
	shouldFail("HU B")
	shouldFail(" ")
	shouldFail("\t")
	shouldFail("CORE.")
	shouldFail(".CORE")
	shouldFail("C.*.O. RE")
	shouldFail("C.ORE")
}

func TestJetStreamServerDomainConfig(t *testing.T) {
	conf := createConfFile(t, []byte(fmt.Sprintf(`
		listen: 127.0.0.1:-1
		jetstream: {domain: "HUB", store_dir: %q}
	`, t.TempDir())))

	s, _ := RunServerWithConfig(conf)
	defer s.Shutdown()

	if !s.JetStreamEnabled() {
		t.Fatalf("Expected JetStream to be enabled")
	}

	config := s.JetStreamConfig()
	if config.Domain != "HUB" {
		t.Fatalf("Expected %q as domain name, got %q", "HUB", config.Domain)
	}
}

func TestJetStreamServerDomainConfigButDisabled(t *testing.T) {
	conf := createConfFile(t, []byte(`
		listen: 127.0.0.1:-1
		jetstream: {domain: "HUB", enabled: false}
	`))

	s, _ := RunServerWithConfig(conf)
	defer s.Shutdown()

	if s.JetStreamEnabled() {
		t.Fatalf("Expected JetStream NOT to be enabled")
	}

	opts := s.getOpts()
	if opts.JetStreamDomain != "HUB" {
		t.Fatalf("Expected %q as opts domain name, got %q", "HUB", opts.JetStreamDomain)
	}
}

func TestJetStreamDomainInPubAck(t *testing.T) {
	conf := createConfFile(t, []byte(fmt.Sprintf(`
		listen: 127.0.0.1:-1
		jetstream: {domain: "HUB", store_dir: %q}
	`, t.TempDir())))

	s, _ := RunServerWithConfig(conf)
	defer s.Shutdown()

	nc, js := jsClientConnect(t, s)
	defer nc.Close()

	cfg := &nats.StreamConfig{
		Name:     "TEST",
		Storage:  nats.MemoryStorage,
		Subjects: []string{"foo"},
	}
	if _, err := js.AddStream(cfg); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	// Check by hand for now til it makes its way into Go client.
	am, err := nc.Request("foo", nil, time.Second)
	require_NoError(t, err)
	var pa PubAck
	json.Unmarshal(am.Data, &pa)
	if pa.Domain != "HUB" {
		t.Fatalf("Expected PubAck to have domain of %q, got %q", "HUB", pa.Domain)
	}
}

// Issue #2213
func TestJetStreamDirectConsumersBeingReported(t *testing.T) {
	s := RunBasicJetStreamServer(t)
	defer s.Shutdown()

	// Client for API requests.
	nc, js := jsClientConnect(t, s)
	defer nc.Close()

	_, err := js.AddStream(&nats.StreamConfig{
		Name:     "TEST",
		Subjects: []string{"foo"},
	})
	require_NoError(t, err)

	_, err = js.AddStream(&nats.StreamConfig{
		Name: "S",
		Sources: []*nats.StreamSource{{
			Name: "TEST",
		}},
	})
	require_NoError(t, err)

	if _, err = js.Publish("foo", nil); err != nil {
		t.Fatalf("Unexpected publish error: %v", err)
	}
	checkFor(t, 2*time.Second, 100*time.Millisecond, func() error {
		si, err := js.StreamInfo("S")
		if err != nil {
			return fmt.Errorf("Could not get stream info: %v", err)
		}
		if si.State.Msgs != 1 {
			return fmt.Errorf("Expected 1 msg, got state: %+v", si.State)
		}
		return nil
	})

	si, err := js.StreamInfo("TEST")
	require_NoError(t, err)

	// Direct consumers should not be reported
	if si.State.Consumers != 0 {
		t.Fatalf("Did not expect any consumers, got %d", si.State.Consumers)
	}

	// Now check for consumer in consumer names list.
	var names []string
	for name := range js.ConsumerNames("TEST") {
		names = append(names, name)
	}
	if len(names) != 0 {
		t.Fatalf("Expected no consumers but got %+v", names)
	}

	// Now check detailed list.
	var cis []*nats.ConsumerInfo
	for ci := range js.ConsumersInfo("TEST") {
		cis = append(cis, ci)
	}
	if len(cis) != 0 {
		t.Fatalf("Expected no consumers but got %+v", cis)
	}
}

// https://github.com/nats-io/nats-server/issues/2290
func TestJetStreamTemplatedErrorsBug(t *testing.T) {
	s := RunBasicJetStreamServer(t)
	defer s.Shutdown()

	// Client for API requests.
	nc, js := jsClientConnect(t, s)
	defer nc.Close()

	_, err := js.AddStream(&nats.StreamConfig{
		Name:     "TEST",
		Subjects: []string{"foo"},
	})
	require_NoError(t, err)

	_, err = js.PullSubscribe("foo", "")
	if err != nil && strings.Contains(err.Error(), "{err}") {
		t.Fatalf("Error is not filled in: %v", err)
	}
}

func TestJetStreamServerEncryption(t *testing.T) {
	cases := []struct {
		name   string
		cstr   string
		cipher StoreCipher
	}{
		{"Default", _EMPTY_, ChaCha},
		{"ChaCha", ", cipher: chacha", ChaCha},
		{"AES", ", cipher: aes", AES},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			tmpl := `
				server_name: S22
				listen: 127.0.0.1:-1
				jetstream: {key: $JS_KEY, store_dir: '%s' %s}
			`
			storeDir := t.TempDir()

			conf := createConfFile(t, []byte(fmt.Sprintf(tmpl, storeDir, c.cstr)))

			os.Setenv("JS_KEY", "s3cr3t!!")
			defer os.Unsetenv("JS_KEY")

			s, _ := RunServerWithConfig(conf)
			defer s.Shutdown()

			config := s.JetStreamConfig()
			if config == nil {
				t.Fatalf("Expected config but got none")
			}
			defer removeDir(t, config.StoreDir)

			// Client based API
			nc, js := jsClientConnect(t, s)
			defer nc.Close()

			cfg := &nats.StreamConfig{
				Name:     "TEST",
				Subjects: []string{"foo", "bar", "baz"},
			}
			if _, err := js.AddStream(cfg); err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}

			msg := []byte("ENCRYPTED PAYLOAD!!")
			sendMsg := func(subj string) {
				t.Helper()
				if _, err := js.Publish(subj, msg); err != nil {
					t.Fatalf("Unexpected publish error: %v", err)
				}
			}
			// Send 10 msgs
			for i := 0; i < 10; i++ {
				sendMsg("foo")
			}

			// Now create a consumer.
			sub, err := js.PullSubscribe("foo", "dlc")
			if err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}
			for i, m := range fetchMsgs(t, sub, 10, 5*time.Second) {
				if i < 5 {
					m.AckSync()
				}
			}

			// Grab our state to compare after restart.
			si, _ := js.StreamInfo("TEST")
			ci, _ := js.ConsumerInfo("TEST", "dlc")

			// Quick check to make sure everything not just plaintext still.
			sdir := filepath.Join(config.StoreDir, "$G", "streams", "TEST")
			// Make sure we can not find any plaintext strings in the target file.
			checkFor := func(fn string, strs ...string) {
				t.Helper()
				data, err := os.ReadFile(fn)
				if err != nil {
					t.Fatalf("Unexpected error: %v", err)
				}
				for _, str := range strs {
					if bytes.Contains(data, []byte(str)) {
						t.Fatalf("Found %q in body of file contents", str)
					}
				}
			}
			checkKeyFile := func(fn string) {
				t.Helper()
				if _, err := os.Stat(fn); err != nil {
					t.Fatalf("Expected a key file at %q", fn)
				}
			}

			// Check stream meta.
			checkEncrypted := func() {
				t.Helper()
				checkKeyFile(filepath.Join(sdir, JetStreamMetaFileKey))
				checkFor(filepath.Join(sdir, JetStreamMetaFile), "TEST", "foo", "bar", "baz", "max_msgs", "max_bytes")
				// Check a message block.
				blocks, err := filepath.Glob(filepath.Join(sdir, "msgs", "*.blk"))
				require_NoError(t, err)
				require_True(t, len(blocks) > 0)
				block := blocks[0]
				checkKeyFile(strings.TrimSuffix(block, ".blk") + ".key")
				checkFor(block, "ENCRYPTED PAYLOAD!!", "foo", "bar", "baz")

				// Check consumer meta and state.
				checkKeyFile(filepath.Join(sdir, "obs", "dlc", JetStreamMetaFileKey))
				checkFor(filepath.Join(sdir, "obs", "dlc", JetStreamMetaFile), "TEST", "dlc", "foo", "bar", "baz", "max_msgs", "ack_policy")
				// Load and see if we can parse the consumer state.
				state, err := os.ReadFile(filepath.Join(sdir, "obs", "dlc", "o.dat"))
				require_NoError(t, err)

				if _, err := decodeConsumerState(state); err == nil {
					t.Fatalf("Expected decoding consumer state to fail")
				}
			}

			// Stop current
			s.Shutdown()

			checkEncrypted()

			// Restart.
			s, _ = RunServerWithConfig(conf)
			defer s.Shutdown()

			// Connect again.
			nc, js = jsClientConnect(t, s)
			defer nc.Close()

			si2, err := js.StreamInfo("TEST")
			require_NoError(t, err)

			if !reflect.DeepEqual(si, si2) {
				t.Fatalf("Stream infos did not match\n%+v\nvs\n%+v", si, si2)
			}

			ci2, _ := js.ConsumerInfo("TEST", "dlc")
			// Consumer create times can be slightly off after restore from disk.
			now := time.Now()
			ci.Created, ci2.Created = now, now
			ci.Delivered.Last, ci2.Delivered.Last = nil, nil
			ci.AckFloor.Last, ci2.AckFloor.Last = nil, nil
			// Also clusters will be different.
			ci.Cluster, ci2.Cluster = nil, nil
			if !reflect.DeepEqual(ci, ci2) {
				t.Fatalf("Consumer infos did not match\n%+v\nvs\n%+v", ci, ci2)
			}

			// Send 10 more msgs
			for i := 0; i < 10; i++ {
				sendMsg("foo")
			}
			if si, err = js.StreamInfo("TEST"); err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}
			if si.State.Msgs != 20 {
				t.Fatalf("Expected 20 msgs total, got %d", si.State.Msgs)
			}

			// Now test snapshots etc.
			acc := s.GlobalAccount()
			mset, err := acc.lookupStream("TEST")
			require_NoError(t, err)
			scfg := mset.config()
			sr, err := mset.snapshot(5*time.Second, false, true)
			if err != nil {
				t.Fatalf("Error getting snapshot: %v", err)
			}
			snapshot, err := io.ReadAll(sr.Reader)
			if err != nil {
				t.Fatalf("Error reading snapshot")
			}

			// Run new server w/o encryption. Make sure we can restore properly (meaning encryption was stripped etc).
			ns := RunBasicJetStreamServer(t)
			defer ns.Shutdown()

			nacc := ns.GlobalAccount()
			r := bytes.NewReader(snapshot)
			mset, err = nacc.RestoreStreamV2(&scfg, r)
			require_NoError(t, err)
			ss := mset.store.State()
			if ss.Msgs != si.State.Msgs || ss.FirstSeq != si.State.FirstSeq || ss.LastSeq != si.State.LastSeq {
				t.Fatalf("Stream states do not match: %+v vs %+v", ss, si.State)
			}

			// Now restore to our encrypted server as well.
			if err := js.DeleteStream("TEST"); err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}

			acc = s.GlobalAccount()
			r.Reset(snapshot)
			mset, err = acc.RestoreStreamV2(&scfg, r)
			require_NoError(t, err)
			ss = mset.store.State()
			if ss.Msgs != si.State.Msgs || ss.FirstSeq != si.State.FirstSeq || ss.LastSeq != si.State.LastSeq {
				t.Fatalf("Stream states do not match: %+v vs %+v", ss, si.State)
			}

			// Check that all is encrypted like above since we know we need to convert since snapshots always plaintext.
			checkEncrypted()
		})
	}
}

func TestJetStreamServerEncryptionServerRestarts(t *testing.T) {
	cases := []struct {
		name   string
		cstr   string
		cipher StoreCipher
	}{
		{"Default", _EMPTY_, ChaCha},
		{"ChaCha", ", cipher: chacha", ChaCha},
		{"AES", ", cipher: aes", AES},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			tmpl := `
				server_name: S22
				listen: 127.0.0.1:-1
				jetstream: {key: $JS_KEY, store_dir: '%s' %s}
			`
			storeDir := t.TempDir()

			conf := createConfFile(t, []byte(fmt.Sprintf(tmpl, storeDir, c.cstr)))

			os.Setenv("JS_KEY", "s3cr3t!!")
			defer os.Unsetenv("JS_KEY")

			s, _ := RunServerWithConfig(conf)
			defer s.Shutdown()

			config := s.JetStreamConfig()
			if config == nil {
				t.Fatalf("Expected config but got none")
			}
			defer removeDir(t, config.StoreDir)

			nc, js := jsClientConnect(t, s)
			defer nc.Close()

			// Add stream.
			cfg := &nats.StreamConfig{
				Name:     "TEST",
				Subjects: []string{"foo"},
				Storage:  nats.FileStorage,
			}
			_, err := js.AddStream(cfg)
			require_NoError(t, err)

			// Restart to invalidate any in-memory state that adding the stream created.
			s.Shutdown()
			s, _ = RunServerWithConfig(conf)
			defer s.Shutdown()
			nc.Close()
			nc, js = jsClientConnect(t, s)
			defer nc.Close()

			msg := []byte("ENCRYPTED PAYLOAD!!")
			_, err = js.Publish("foo", msg)
			require_NoError(t, err)

			// Restart to invalidate any in-memory state that the publish initialized.
			s.Shutdown()
			s, _ = RunServerWithConfig(conf)
			defer s.Shutdown()
			nc.Close()
			nc, js = jsClientConnect(t, s)
			defer nc.Close()

			// Should still be able to get the data.
			sub, err := js.SubscribeSync("foo")
			require_NoError(t, err)
			defer sub.Drain()

			m, err := sub.NextMsg(time.Second)
			require_NoError(t, err)
			meta, err := m.Metadata()
			require_NoError(t, err)
			require_Equal(t, meta.Sequence.Stream, 1)
		})
	}
}

func TestJetStreamDeliverLastPerSubject(t *testing.T) {
	for _, st := range []StorageType{FileStorage, MemoryStorage} {
		t.Run(st.String(), func(t *testing.T) {
			s := RunBasicJetStreamServer(t)
			defer s.Shutdown()

			// Client for API requests.
			nc, js := jsClientConnect(t, s)
			defer nc.Close()

			cfg := StreamConfig{
				Name:       "KV",
				Subjects:   []string{"kv.>"},
				Storage:    st,
				MaxMsgsPer: 5,
			}

			req, err := json.Marshal(cfg)
			if err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}
			// Do manually for now.
			nc.Request(fmt.Sprintf(JSApiStreamCreateT, cfg.Name), req, time.Second)
			si, err := js.StreamInfo("KV")
			if err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}
			if si == nil || si.Config.Name != "KV" {
				t.Fatalf("StreamInfo is not correct %+v", si)
			}

			// Interleave them on purpose.
			for i := 1; i <= 11; i++ {
				msg := []byte(fmt.Sprintf("%d", i))
				js.PublishAsync("kv.b1.foo", msg)
				js.PublishAsync("kv.b2.foo", msg)

				js.PublishAsync("kv.b1.bar", msg)
				js.PublishAsync("kv.b2.bar", msg)

				js.PublishAsync("kv.b1.baz", msg)
				js.PublishAsync("kv.b2.baz", msg)
			}

			select {
			case <-js.PublishAsyncComplete():
			case <-time.After(2 * time.Second):
				t.Fatalf("Did not receive completion signal")
			}

			// Do quick check that config needs FilteredSubjects otherwise bad config.
			badReq := CreateConsumerRequest{
				Stream: "KV",
				Config: ConsumerConfig{
					DeliverSubject: "b",
					DeliverPolicy:  DeliverLastPerSubject,
				},
			}
			req, err = json.Marshal(badReq)
			if err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}
			resp, err := nc.Request(fmt.Sprintf(JSApiConsumerCreateT, "KV"), req, time.Second)
			if err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}
			var ccResp JSApiConsumerCreateResponse
			if err = json.Unmarshal(resp.Data, &ccResp); err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}
			if ccResp.Error == nil || !strings.Contains(ccResp.Error.Description, "filter subject is not set") {
				t.Fatalf("Expected an error, got none")
			}

			// Now let's consume these via last per subject.
			obsReq := CreateConsumerRequest{
				Stream: "KV",
				Config: ConsumerConfig{
					DeliverSubject: "d",
					DeliverPolicy:  DeliverLastPerSubject,
					FilterSubject:  "kv.b1.*",
				},
			}
			req, err = json.Marshal(obsReq)
			if err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}
			resp, err = nc.Request(fmt.Sprintf(JSApiConsumerCreateT, "KV"), req, time.Second)
			if err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}
			ccResp.Error = nil
			if err = json.Unmarshal(resp.Data, &ccResp); err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}

			sub, _ := nc.SubscribeSync("d")
			defer sub.Unsubscribe()

			// Helper to check messages are correct.
			checkNext := func(subject string, sseq uint64, v string) {
				t.Helper()
				m, err := sub.NextMsg(time.Second)
				if err != nil {
					t.Fatalf("Error receiving message: %v", err)
				}
				if m.Subject != subject {
					t.Fatalf("Expected subject %q but got %q", subject, m.Subject)
				}
				meta, err := m.Metadata()
				if err != nil {
					t.Fatalf("didn't get metadata: %s", err)
				}
				if meta.Sequence.Stream != sseq {
					t.Fatalf("Expected stream seq %d but got %d", sseq, meta.Sequence.Stream)
				}
				if string(m.Data) != v {
					t.Fatalf("Expected data of %q but got %q", v, m.Data)
				}
			}

			checkSubsPending(t, sub, 3)

			// Now make sure they are what we expect.
			checkNext("kv.b1.foo", 61, "11")
			checkNext("kv.b1.bar", 63, "11")
			checkNext("kv.b1.baz", 65, "11")

			msg := []byte(fmt.Sprintf("%d", 22))
			js.Publish("kv.b1.bar", msg)
			js.Publish("kv.b2.foo", msg) // Not filtered through..

			checkSubsPending(t, sub, 1)
			checkNext("kv.b1.bar", 67, "22")
		})
	}
}

func TestJetStreamDeliverLastPerSubjectNumPending(t *testing.T) {
	s := RunBasicJetStreamServer(t)
	defer s.Shutdown()

	// Client for API requests.
	nc, js := jsClientConnect(t, s)
	defer nc.Close()

	if _, err := js.AddStream(&nats.StreamConfig{
		Name:              "KV",
		Subjects:          []string{"KV.>"},
		MaxMsgsPerSubject: 5,
		Replicas:          1,
	}); err != nil {
		t.Fatalf("Error adding stream: %v", err)
	}

	for i := 0; i < 5; i++ {
		msg := []byte(fmt.Sprintf("msg%d", i))
		js.Publish("KV.foo", msg)
		js.Publish("KV.bar", msg)
		js.Publish("KV.baz", msg)
		js.Publish("KV.bat", msg)
	}

	// Delete some messages
	js.DeleteMsg("KV", 2)
	js.DeleteMsg("KV", 5)

	ci, err := js.AddConsumer("KV", &nats.ConsumerConfig{
		DeliverSubject: nats.NewInbox(),
		AckPolicy:      nats.AckExplicitPolicy,
		DeliverPolicy:  nats.DeliverLastPerSubjectPolicy,
		FilterSubject:  "KV.>",
	})
	if err != nil {
		t.Fatalf("Error adding consumer: %v", err)
	}
	if ci.NumPending != 4 {
		t.Fatalf("Expected 4 pending msgs, got %v", ci.NumPending)
	}
}

// Issue #2392
func TestJetStreamPurgeEffectsConsumerDelivery(t *testing.T) {
	s := RunBasicJetStreamServer(t)
	defer s.Shutdown()

	// Client for API requests.
	nc, js := jsClientConnect(t, s)
	defer nc.Close()

	_, err := js.AddStream(&nats.StreamConfig{
		Name:     "TEST",
		Subjects: []string{"foo.*"},
	})
	require_NoError(t, err)

	js.Publish("foo.a", []byte("show once"))

	sub, err := js.SubscribeSync("foo.*", nats.AckWait(250*time.Millisecond), nats.DeliverAll(), nats.AckExplicit())
	require_NoError(t, err)

	defer sub.Unsubscribe()

	checkSubsPending(t, sub, 1)

	// Do not ack.
	if _, err := sub.NextMsg(time.Second); err != nil {
		t.Fatalf("Error receiving message: %v", err)
	}

	// Now purge stream.
	if err := js.PurgeStream("TEST"); err != nil {
		t.Fatalf("Unexpected purge error: %v", err)
	}

	js.Publish("foo.b", []byte("show twice?"))
	// Do not ack again, should show back up.
	if _, err := sub.NextMsg(time.Second); err != nil {
		t.Fatalf("Error receiving message: %v", err)
	}
	// Make sure we get it back.
	if _, err := sub.NextMsg(time.Second); err != nil {
		t.Fatalf("Error receiving message: %v", err)
	}
}

// Issue #2403
func TestJetStreamExpireCausesDeadlock(t *testing.T) {
	s := RunBasicJetStreamServer(t)
	defer s.Shutdown()

	// Client for API requests.
	nc, js := jsClientConnect(t, s)
	defer nc.Close()

	_, err := js.AddStream(&nats.StreamConfig{
		Name:      "TEST",
		Subjects:  []string{"foo.*"},
		Storage:   nats.MemoryStorage,
		MaxMsgs:   10,
		Retention: nats.InterestPolicy,
	})
	require_NoError(t, err)

	sub, err := js.SubscribeSync("foo.bar")
	require_NoError(t, err)

	defer sub.Unsubscribe()

	// Publish from two connections to get the write lock request wedged in between
	// having the RLock and wanting it again deeper in the stack.
	nc2, js2 := jsClientConnect(t, s)
	defer nc2.Close()

	for i := 0; i < 1000; i++ {
		js.PublishAsync("foo.bar", []byte("HELLO"))
		js2.PublishAsync("foo.bar", []byte("HELLO"))
	}
	select {
	case <-js.PublishAsyncComplete():
	case <-time.After(5 * time.Second):
		t.Fatalf("Did not receive completion signal")
	}

	// If we deadlocked then we will not be able to get stream info.
	if _, err := js.StreamInfo("TEST"); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
}

// Issue #2420
func TestJetStreamDefaultMaxMsgsPer(t *testing.T) {
	s := RunBasicJetStreamServer(t)
	defer s.Shutdown()

	// Client for API requests.
	nc, js := jsClientConnect(t, s)
	defer nc.Close()

	si, err := js.AddStream(&nats.StreamConfig{
		Name:     "TEST",
		Subjects: []string{"foo.*"},
		Storage:  nats.MemoryStorage,
		MaxMsgs:  10,
	})
	require_NoError(t, err)

	if si.Config.MaxMsgsPerSubject != -1 {
		t.Fatalf("Expected default of -1, got %d", si.Config.MaxMsgsPerSubject)
	}
}

// Got a report of streams that expire all messages while the server is down report errors when clients reconnect
// and try to send new messages.
func TestJetStreamExpireAllWhileServerDown(t *testing.T) {
	s := RunBasicJetStreamServer(t)
	defer s.Shutdown()

	nc, js := jsClientConnect(t, s)
	defer nc.Close()

	cfg := &nats.StreamConfig{
		Name:   "TEST",
		MaxAge: 250 * time.Millisecond,
	}
	if _, err := js.AddStream(cfg); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	toSend := 10_000
	for i := 0; i < toSend; i++ {
		js.PublishAsync("TEST", []byte("OK"))
	}
	select {
	case <-js.PublishAsyncComplete():
	case <-time.After(time.Second):
		t.Fatalf("Did not receive completion signal")
	}

	sd := s.JetStreamConfig().StoreDir
	s.Shutdown()

	time.Sleep(300 * time.Millisecond)

	// Restart after expire.
	s = RunJetStreamServerOnPort(-1, sd)
	defer s.Shutdown()

	nc, js = jsClientConnect(t, s)
	defer nc.Close()

	if si, err := js.StreamInfo("TEST"); err != nil || si.State.Msgs != 0 {
		t.Fatalf("Unexpected stream info state: %+v", si)
	}

	for i := 0; i < 10; i++ {
		if _, err := js.Publish("TEST", []byte("OK")); err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
	}

	if si, err := js.StreamInfo("TEST"); err != nil || si.State.Msgs != 10 {
		t.Fatalf("Unexpected stream info state: %+v", si)
	}
}

func TestJetStreamLongStreamNamesAndPubAck(t *testing.T) {
	s := RunBasicJetStreamServer(t)
	defer s.Shutdown()

	nc, js := jsClientConnect(t, s)
	defer nc.Close()

	cfg := &nats.StreamConfig{
		Name:     strings.Repeat("ZABC", 256/4)[:255],
		Subjects: []string{"foo"},
	}
	if _, err := js.AddStream(cfg); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	js.Publish("foo", []byte("HELLO"))
}

func TestJetStreamPerSubjectPending(t *testing.T) {
	for _, st := range []nats.StorageType{nats.FileStorage, nats.MemoryStorage} {
		t.Run(st.String(), func(t *testing.T) {

			s := RunBasicJetStreamServer(t)
			defer s.Shutdown()

			nc, js := jsClientConnect(t, s)
			defer nc.Close()

			_, err := js.AddStream(&nats.StreamConfig{
				Name:              "KV_X",
				Subjects:          []string{"$KV.X.>"},
				MaxMsgsPerSubject: 5,
				Storage:           st,
			})
			if err != nil {
				t.Fatalf("add stream failed: %s", err)
			}

			// the message we will care for
			_, err = js.Publish("$KV.X.x.y.z", []byte("hello world"))
			if err != nil {
				t.Fatalf("publish failed: %s", err)
			}

			// make sure there's some unrelated message after
			_, err = js.Publish("$KV.X.1", []byte("hello world"))
			if err != nil {
				t.Fatalf("publish failed: %s", err)
			}

			// we expect the wildcard filter subject to match only the one message and so pending will be 0
			sub, err := js.SubscribeSync("$KV.X.x.>", nats.DeliverLastPerSubject())
			if err != nil {
				t.Fatalf("subscribe failed: %s", err)
			}

			msg, err := sub.NextMsg(time.Second)
			if err != nil {
				t.Fatalf("next failed: %s", err)
			}

			meta, err := msg.Metadata()
			if err != nil {
				t.Fatalf("meta failed: %s", err)
			}

			// with DeliverLastPerSubject set this is never 0, but without setting that its 0 correctly
			if meta.NumPending != 0 {
				t.Fatalf("expected numpending 0 got %d", meta.NumPending)
			}
		})
	}
}

func TestJetStreamPublishExpectNoMsg(t *testing.T) {
	s := RunBasicJetStreamServer(t)
	defer s.Shutdown()

	nc, js := jsClientConnect(t, s)
	defer nc.Close()

	_, err := js.AddStream(&nats.StreamConfig{
		Name:              "KV",
		Subjects:          []string{"KV.>"},
		MaxMsgsPerSubject: 5,
	})
	if err != nil {
		t.Fatalf("add stream failed: %s", err)
	}

	if _, err = js.Publish("KV.22", []byte("hello world")); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	// This should succeed.
	m := nats.NewMsg("KV.33")
	m.Header.Set(JSExpectedLastSubjSeq, "0")
	if _, err := js.PublishMsg(m); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	// This should fail.
	m = nats.NewMsg("KV.22")
	m.Header.Set(JSExpectedLastSubjSeq, "0")
	if _, err := js.PublishMsg(m); err == nil {
		t.Fatalf("Expected error: %v", err)
	}

	if err := js.PurgeStream("KV"); err != nil {
		t.Fatalf("Unexpected purge error: %v", err)
	}

	// This should succeed now.
	if _, err := js.PublishMsg(m); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
}

func TestJetStreamNegativeDupeWindow(t *testing.T) {
	s := RunBasicJetStreamServer(t)
	defer s.Shutdown()

	nc, js := jsClientConnect(t, s)
	defer nc.Close()

	// we incorrectly set MaxAge to -1 which then as a side effect sets dupe window to -1 which should fail
	_, err := js.AddStream(&nats.StreamConfig{
		Name:              "TEST",
		Subjects:          nil,
		Retention:         nats.WorkQueuePolicy,
		MaxConsumers:      1,
		MaxMsgs:           -1,
		MaxBytes:          -1,
		Discard:           nats.DiscardNew,
		MaxAge:            0,
		Duplicates:        -1,
		MaxMsgsPerSubject: -1,
		MaxMsgSize:        -1,
		Storage:           nats.FileStorage,
		Replicas:          1,
		NoAck:             false,
	})
	if err == nil || err.Error() != "nats: duplicates window can not be negative" {
		t.Fatalf("Expected dupe window error got: %v", err)
	}
}

// Issue #2551
func TestJetStreamMirroredConsumerFailAfterRestart(t *testing.T) {
	s := RunBasicJetStreamServer(t)
	defer s.Shutdown()

	nc, js := jsClientConnect(t, s)
	defer nc.Close()

	_, err := js.AddStream(&nats.StreamConfig{
		Name:     "S1",
		Storage:  nats.FileStorage,
		Subjects: []string{"foo", "bar", "baz"},
	})
	if err != nil {
		t.Fatalf("create failed: %s", err)
	}

	_, err = js.AddStream(&nats.StreamConfig{
		Name:    "M1",
		Storage: nats.FileStorage,
		Mirror:  &nats.StreamSource{Name: "S1"},
	})
	if err != nil {
		t.Fatalf("create failed: %s", err)
	}

	_, err = js.AddConsumer("M1", &nats.ConsumerConfig{
		Durable:       "C1",
		FilterSubject: ">",
		AckPolicy:     nats.AckExplicitPolicy,
	})
	if err != nil {
		t.Fatalf("consumer create failed: %s", err)
	}

	// Stop current
	sd := s.JetStreamConfig().StoreDir
	s.Shutdown()
	s.WaitForShutdown()

	// Restart.
	s = RunJetStreamServerOnPort(-1, sd)
	defer s.Shutdown()

	nc, js = jsClientConnect(t, s)
	defer nc.Close()

	_, err = js.StreamInfo("M1")
	if err != nil {
		t.Fatalf("%s did not exist after start: %s", "M1", err)
	}

	_, err = js.ConsumerInfo("M1", "C1")
	if err != nil {
		t.Fatalf("C1 did not exist after start: %s", err)
	}
}

func TestJetStreamDisabledLimitsEnforcementJWT(t *testing.T) {
	updateJwt := func(url string, akp nkeys.KeyPair, pubKey string, jwt string) {
		t.Helper()
		c := natsConnect(t, url, createUserCreds(t, nil, akp))
		defer c.Close()
		if msg, err := c.Request(fmt.Sprintf(accUpdateEventSubjNew, pubKey), []byte(jwt), time.Second); err != nil {
			t.Fatal("error not expected in this test", err)
		} else {
			content := make(map[string]any)
			if err := json.Unmarshal(msg.Data, &content); err != nil {
				t.Fatalf("%v", err)
			} else if _, ok := content["data"]; !ok {
				t.Fatalf("did not get an ok response got: %v", content)
			}
		}
	}
	// create system account
	sysKp, _ := nkeys.CreateAccount()
	sysPub, _ := sysKp.PublicKey()
	// limits to apply and check
	limits1 := jwt.JetStreamLimits{MemoryStorage: 1024, DiskStorage: 0, Streams: 1, Consumer: 2}
	akp, _ := nkeys.CreateAccount()
	aPub, _ := akp.PublicKey()
	claim := jwt.NewAccountClaims(aPub)
	claim.Limits.JetStreamLimits = limits1
	aJwt1, err := claim.Encode(oKp)
	require_NoError(t, err)
	dir := t.TempDir()
	storeDir1 := t.TempDir()
	conf := createConfFile(t, []byte(fmt.Sprintf(`
		listen: -1
		jetstream: {store_dir: '%s'}
		operator: %s
		resolver: {
			type: full
			dir: '%s'
		}
		system_account: %s
    `, storeDir1, ojwt, dir, sysPub)))
	s, _ := RunServerWithConfig(conf)
	defer s.Shutdown()
	updateJwt(s.ClientURL(), sysKp, aPub, aJwt1)
	c := natsConnect(t, s.ClientURL(), createUserCreds(t, nil, akp), nats.ReconnectWait(200*time.Millisecond))
	defer c.Close()
	// keep using the same connection
	js, err := c.JetStream()
	require_NoError(t, err)
	_, err = js.AddStream(&nats.StreamConfig{
		Name:     "disk",
		Storage:  nats.FileStorage,
		Subjects: []string{"disk"},
	})
	require_Error(t, err)
}

func TestJetStreamDisabledLimitsEnforcement(t *testing.T) {
	storeDir1 := t.TempDir()
	conf1 := createConfFile(t, []byte(fmt.Sprintf(`
		listen: 127.0.0.1:-1
		jetstream: {max_mem_store: 256MB, max_file_store: 2GB, store_dir: '%s'}
		accounts {
			one {
				jetstream: {
					mem: 1024
					disk: 0
					streams: 1
					consumers: 2
				}
				users [{user: one, password: password}]
			}
		}
		no_auth_user: one
	`, storeDir1)))
	s, _ := RunServerWithConfig(conf1)
	defer s.Shutdown()

	c := natsConnect(t, s.ClientURL())
	defer c.Close()
	// keep using the same connection
	js, err := c.JetStream()
	require_NoError(t, err)
	_, err = js.AddStream(&nats.StreamConfig{
		Name:     "disk",
		Storage:  nats.FileStorage,
		Subjects: []string{"disk"},
	})
	require_Error(t, err)
}

// Issue #2607
func TestJetStreamPurgeAndFilteredConsumers(t *testing.T) {
	s := RunBasicJetStreamServer(t)
	defer s.Shutdown()

	nc, js := jsClientConnect(t, s)
	defer nc.Close()

	_, err := js.AddStream(&nats.StreamConfig{Name: "S", Subjects: []string{"FOO.*"}})
	require_NoError(t, err)

	for i := 0; i < 10; i++ {
		_, err = js.Publish("FOO.adam", []byte("M"))
		require_NoError(t, err)
		_, err = js.Publish("FOO.eve", []byte("F"))
		require_NoError(t, err)
	}

	ci, err := js.AddConsumer("S", &nats.ConsumerConfig{
		Durable:       "adam",
		AckPolicy:     nats.AckExplicitPolicy,
		FilterSubject: "FOO.adam",
	})
	require_NoError(t, err)
	if ci.NumPending != 10 {
		t.Fatalf("Expected NumPending to be 10, got %d", ci.NumPending)
	}

	ci, err = js.AddConsumer("S", &nats.ConsumerConfig{
		Durable:       "eve",
		AckPolicy:     nats.AckExplicitPolicy,
		FilterSubject: "FOO.eve",
	})
	require_NoError(t, err)
	if ci.NumPending != 10 {
		t.Fatalf("Expected NumPending to be 10, got %d", ci.NumPending)
	}

	// Also check unfiltered with interleaving messages.
	_, err = js.AddConsumer("S", &nats.ConsumerConfig{
		Durable:   "all",
		AckPolicy: nats.AckExplicitPolicy,
	})
	require_NoError(t, err)

	// Now purge only adam.
	jr, _ := json.Marshal(&JSApiStreamPurgeRequest{Subject: "FOO.adam"})
	_, err = nc.Request(fmt.Sprintf(JSApiStreamPurgeT, "S"), jr, time.Second)
	require_NoError(t, err)

	si, err := js.StreamInfo("S")
	require_NoError(t, err)
	if si.State.Msgs != 10 {
		t.Fatalf("Expected 10 messages after purge, got %d", si.State.Msgs)
	}

	ci, err = js.ConsumerInfo("S", "eve")
	require_NoError(t, err)
	if ci.NumPending != 10 {
		t.Fatalf("Expected NumPending to be 10, got %d", ci.NumPending)
	}

	ci, err = js.ConsumerInfo("S", "adam")
	require_NoError(t, err)
	if ci.NumPending != 0 {
		t.Fatalf("Expected NumPending to be 0, got %d", ci.NumPending)
	}
	if ci.AckFloor.Stream != 20 {
		t.Fatalf("Expected AckFloor for stream to be 20, got %d", ci.AckFloor.Stream)
	}

	ci, err = js.ConsumerInfo("S", "all")
	require_NoError(t, err)
	if ci.NumPending != 10 {
		t.Fatalf("Expected NumPending to be 10, got %d", ci.NumPending)
	}
}

// Issue #2662
func TestJetStreamLargeExpiresAndServerRestart(t *testing.T) {
	s := RunBasicJetStreamServer(t)
	defer s.Shutdown()

	nc, js := jsClientConnect(t, s)
	defer nc.Close()

	maxAge := 2 * time.Second

	_, err := js.AddStream(&nats.StreamConfig{
		Name:     "S",
		Subjects: []string{"foo"},
		MaxAge:   maxAge,
	})
	require_NoError(t, err)

	start := time.Now()
	_, err = js.Publish("foo", []byte("ok"))
	require_NoError(t, err)

	// Wait total of maxAge - 1s.
	time.Sleep(maxAge - time.Since(start) - time.Second)

	// Stop current
	sd := s.JetStreamConfig().StoreDir
	s.Shutdown()
	// Restart.
	s = RunJetStreamServerOnPort(-1, sd)
	defer s.Shutdown()

	nc, js = jsClientConnect(t, s)
	defer nc.Close()

	checkFor(t, 5*time.Second, 10*time.Millisecond, func() error {
		si, err := js.StreamInfo("S")
		require_NoError(t, err)
		if si.State.Msgs != 0 {
			return fmt.Errorf("Expected no messages, got %d", si.State.Msgs)
		}
		return nil
	})

	if waited := time.Since(start); waited > maxAge+time.Second {
		t.Fatalf("Waited to long %v vs %v for messages to expire", waited, maxAge)
	}
}

// Bug that was reported showing memstore not handling max per subject of 1.
func TestJetStreamMessagePerSubjectKeepBug(t *testing.T) {
	test := func(t *testing.T, keep int64, store nats.StorageType) {
		s := RunBasicJetStreamServer(t)
		defer s.Shutdown()

		nc, js := jsClientConnect(t, s)
		defer nc.Close()

		_, err := js.AddStream(&nats.StreamConfig{
			Name:              "TEST",
			MaxMsgsPerSubject: keep,
			Storage:           store,
		})
		require_NoError(t, err)

		for i := 0; i < 100; i++ {
			_, err = js.Publish("TEST", []byte(fmt.Sprintf("test %d", i)))
			require_NoError(t, err)
		}

		nfo, err := js.StreamInfo("TEST")
		require_NoError(t, err)

		if nfo.State.Msgs != uint64(keep) {
			t.Fatalf("Expected %d message got %d", keep, nfo.State.Msgs)
		}
	}

	t.Run("FileStore", func(t *testing.T) {
		t.Run("Keep 10", func(t *testing.T) { test(t, 10, nats.FileStorage) })
		t.Run("Keep 1", func(t *testing.T) { test(t, 1, nats.FileStorage) })
	})

	t.Run("MemStore", func(t *testing.T) {
		t.Run("Keep 10", func(t *testing.T) { test(t, 10, nats.MemoryStorage) })
		t.Run("Keep 1", func(t *testing.T) { test(t, 1, nats.MemoryStorage) })
	})
}

func TestJetStreamInvalidDeliverSubject(t *testing.T) {
	s := RunBasicJetStreamServer(t)
	defer s.Shutdown()

	nc, js := jsClientConnect(t, s)
	defer nc.Close()

	_, err := js.AddStream(&nats.StreamConfig{
		Name: "TEST",
	})
	require_NoError(t, err)

	_, err = js.AddConsumer("TEST", &nats.ConsumerConfig{DeliverSubject: " x"})
	require_Error(t, err, NewJSConsumerInvalidDeliverSubjectError())
}

func TestJetStreamMemoryCorruption(t *testing.T) {
	s := RunBasicJetStreamServer(t)
	defer s.Shutdown()

	nc, js := jsClientConnect(t, s)
	defer nc.Close()

	errCh := make(chan error, 10)
	nc.SetErrorHandler(func(_ *nats.Conn, _ *nats.Subscription, e error) {
		select {
		case errCh <- e:
		default:
		}
	})

	// The storage has to be MemoryStorage to show the issue.
	// History must cover all revisions, otherwise a Put can evict the
	// previous revision of the key before the watchers have received it.
	kv, err := js.CreateKeyValue(&nats.KeyValueConfig{Bucket: "bucket", Storage: nats.MemoryStorage, History: 64})
	require_NoError(t, err)

	w1, err := kv.WatchAll()
	require_NoError(t, err)

	w2, err := kv.WatchAll(nats.MetaOnly())
	require_NoError(t, err)

	_, err = kv.Put("key1", []byte("aaa"))
	require_NoError(t, err)
	_, err = kv.Put("key1", []byte("aab"))
	require_NoError(t, err)
	_, err = kv.Put("key2", []byte("zza"))
	require_NoError(t, err)
	_, err = kv.Put("key2", []byte("zzb"))
	require_NoError(t, err)
	require_NoError(t, kv.Delete("key1"))
	require_NoError(t, kv.Delete("key2"))
	_, err = kv.Put("key1", []byte("aac"))
	require_NoError(t, err)
	_, err = kv.Put("key2", []byte("zzc"))
	require_NoError(t, err)
	require_NoError(t, kv.Delete("key1"))
	require_NoError(t, kv.Delete("key2"))

	checkUpdates := func(updates <-chan nats.KeyValueEntry, expected int) {
		t.Helper()
		count := 0
		for {
			select {
			case <-updates:
				count++
				if count == expected {
					return
				}
			case <-time.After(5 * time.Second):
				t.Fatalf("Did not receive all updates, got %d of %d", count, expected)
			}
		}
	}
	// Both watchers must have received the initial marker and the 10 updates
	// before purging, since a purge removes any not-yet-delivered messages
	// for that key from the stream.
	checkUpdates(w1.Updates(), 11)
	checkUpdates(w2.Updates(), 11)

	require_NoError(t, kv.Purge("key1"))
	require_NoError(t, kv.Purge("key2"))

	checkUpdates(w1.Updates(), 2)
	checkUpdates(w2.Updates(), 2)

	select {
	case e := <-errCh:
		t.Fatal(e)
	case <-time.After(250 * time.Millisecond):
		// OK
	}
}

func TestJetStreamRecoverBadStreamSubjects(t *testing.T) {
	s := RunBasicJetStreamServer(t)
	sd := s.JetStreamConfig().StoreDir
	s.Shutdown()

	f := filepath.Join(sd, "$G", "streams", "TEST")
	fs, err := newFileStore(FileStoreConfig{StoreDir: f}, StreamConfig{
		Name:     "TEST",
		Subjects: []string{"foo", "bar", " baz "}, // baz has spaces
		Storage:  FileStorage,
	})
	require_NoError(t, err)
	fs.Stop()

	s = RunJetStreamServerOnPort(-1, sd)
	defer s.Shutdown()

	nc, js := jsClientConnect(t, s)
	defer nc.Close()

	si, err := js.StreamInfo("TEST")
	require_NoError(t, err)

	if len(si.Config.Subjects) != 3 {
		t.Fatalf("Expected to recover all subjects")
	}
}

func TestJetStreamRecoverBadMirrorConfigWithSubjects(t *testing.T) {
	s := RunBasicJetStreamServer(t)
	defer s.Shutdown()
	sd := s.JetStreamConfig().StoreDir

	// Client for API requests.
	nc, js := jsClientConnect(t, s)
	defer nc.Close()

	// Origin
	_, err := js.AddStream(&nats.StreamConfig{
		Name:     "S",
		Subjects: []string{"foo"},
	})
	require_NoError(t, err)

	s.Shutdown()

	f := filepath.Join(sd, "$G", "streams", "M")
	fs, err := newFileStore(FileStoreConfig{StoreDir: f}, StreamConfig{
		Name:     "M",
		Subjects: []string{"foo", "bar", "baz"}, // Mirrors should not have spaces.
		Mirror:   &StreamSource{Name: "S"},
		Storage:  FileStorage,
	})
	require_NoError(t, err)
	fs.Stop()

	s = RunJetStreamServerOnPort(-1, sd)
	defer s.Shutdown()

	nc, js = jsClientConnect(t, s)
	defer nc.Close()

	si, err := js.StreamInfo("M")
	require_NoError(t, err)

	if len(si.Config.Subjects) != 0 {
		t.Fatalf("Expected to have NO subjects on mirror")
	}
}

func TestJetStreamCrossAccountsDeliverSubjectInterest(t *testing.T) {
	conf := createConfFile(t, []byte(fmt.Sprintf(`
		listen: 127.0.0.1:-1
		jetstream: {max_mem_store: 4GB, max_file_store: 1TB, store_dir: %q}
		accounts: {
			A: {
				jetstream: enabled
				users: [ {user: a, password: pwd} ]
				exports [
					{ stream: "_d_" }   # For the delivery subject for the consumer
				]
			},
			B: {
				users: [ {user: b, password: pwd} ]
				imports [
					{ stream: { account: A, subject: "_d_"}, to: "foo" }
				]
			},
		}
	`, t.TempDir())))

	s, _ := RunServerWithConfig(conf)
	defer s.Shutdown()

	nc, js := jsClientConnect(t, s, nats.UserInfo("a", "pwd"))
	defer nc.Close()

	_, err := js.AddStream(&nats.StreamConfig{
		Name:     "TEST",
		Subjects: []string{"foo"},
	})
	require_NoError(t, err)

	msg, toSend := []byte("OK"), 100
	for i := 0; i < toSend; i++ {
		if _, err := js.PublishAsync("foo", msg); err != nil {
			t.Fatalf("Unexpected publish error: %v", err)
		}
	}
	select {
	case <-js.PublishAsyncComplete():
	case <-time.After(5 * time.Second):
		t.Fatalf("Did not receive completion signal")
	}

	// Now create the consumer as well here manually that we will want to reference from Account B.
	_, err = js.AddConsumer("TEST", &nats.ConsumerConfig{Durable: "dlc", DeliverSubject: "_d_"})
	require_NoError(t, err)

	// Wait to see if the stream import signals to deliver messages with no real subscriber interest.
	time.Sleep(200 * time.Millisecond)

	ci, err := js.ConsumerInfo("TEST", "dlc")
	require_NoError(t, err)

	// Make sure we have not delivered any messages based on the import signal alone.
	if ci.NumPending != uint64(toSend) || ci.Delivered.Consumer != 0 {
		t.Fatalf("Bad consumer info, looks like we started delivering: %+v", ci)
	}

	// Now create interest in the delivery subject through the import on account B.
	nc, _ = jsClientConnect(t, s, nats.UserInfo("b", "pwd"))
	defer nc.Close()
	sub, err := nc.SubscribeSync("foo")
	require_NoError(t, err)
	checkSubsPending(t, sub, toSend)

	ci, err = js.ConsumerInfo("TEST", "dlc")
	require_NoError(t, err)

	// Make sure our consumer info reflects we delivered the messages.
	if ci.NumPending != 0 || ci.Delivered.Consumer != uint64(toSend) {
		t.Fatalf("Bad consumer info, looks like we did not deliver: %+v", ci)
	}
}

func TestJetStreamEphemeralPullConsumers(t *testing.T) {
	s := RunBasicJetStreamServer(t)
	defer s.Shutdown()

	nc, js := jsClientConnect(t, s)
	defer nc.Close()

	_, err := js.AddStream(&nats.StreamConfig{Name: "EC", Storage: nats.MemoryStorage})
	require_NoError(t, err)

	ci, err := js.AddConsumer("EC", &nats.ConsumerConfig{AckPolicy: nats.AckExplicitPolicy})
	require_NoError(t, err)

	mset, err := s.GlobalAccount().lookupStream("EC")
	require_NoError(t, err)
	o := mset.lookupConsumer(ci.Name)
	if o == nil {
		t.Fatalf("Error looking up consumer %q", ci.Name)
	}
	err = o.setInActiveDeleteThreshold(50 * time.Millisecond)
	require_NoError(t, err)

	time.Sleep(100 * time.Millisecond)
	// Should no longer be around.
	if o := mset.lookupConsumer(ci.Name); o != nil {
		t.Fatalf("Expected consumer to be closed and removed")
	}

	// Make sure timer keeps firing etc. and does not delete until interest is gone.
	ci, err = js.AddConsumer("EC", &nats.ConsumerConfig{AckPolicy: nats.AckExplicitPolicy})
	require_NoError(t, err)
	if o = mset.lookupConsumer(ci.Name); o == nil {
		t.Fatalf("Error looking up consumer %q", ci.Name)
	}
	err = o.setInActiveDeleteThreshold(50 * time.Millisecond)
	require_NoError(t, err)

	// Need interest otherwise the requests will be recycled based on no real interest.
	sub, err := nc.SubscribeSync("xx")
	require_NoError(t, err)

	req := &JSApiConsumerGetNextRequest{Batch: 10, Expires: 250 * time.Millisecond}
	jreq, err := json.Marshal(req)
	require_NoError(t, err)
	rsubj := fmt.Sprintf(JSApiRequestNextT, "EC", ci.Name)
	err = nc.PublishRequest(rsubj, "xx", jreq)
	require_NoError(t, err)
	nc.Flush()

	time.Sleep(100 * time.Millisecond)
	// Should still be alive here.
	if o := mset.lookupConsumer(ci.Name); o == nil {
		t.Fatalf("Expected consumer to still be active")
	}
	// Remove interest.
	sub.Unsubscribe()
	// Make sure this EPC goes away now.
	checkFor(t, 5*time.Second, 10*time.Millisecond, func() error {
		if o := mset.lookupConsumer(ci.Name); o != nil {
			return fmt.Errorf("Consumer still present")
		}
		return nil
	})
}

func TestJetStreamEphemeralPullConsumersInactiveThresholdAndNoWait(t *testing.T) {
	s := RunBasicJetStreamServer(t)
	defer s.Shutdown()

	nc, js := jsClientConnect(t, s)
	defer nc.Close()

	_, err := js.AddStream(&nats.StreamConfig{Name: "ECIT", Storage: nats.MemoryStorage})
	require_NoError(t, err)

	ci, err := js.AddConsumer("ECIT", &nats.ConsumerConfig{
		AckPolicy:         nats.AckExplicitPolicy,
		InactiveThreshold: 100 * time.Millisecond,
	})
	require_NoError(t, err)

	// Send 10 no_wait requests every 25ms and consumer should still be present.
	req := &JSApiConsumerGetNextRequest{Batch: 10, NoWait: true}
	jreq, err := json.Marshal(req)
	require_NoError(t, err)
	rsubj := fmt.Sprintf(JSApiRequestNextT, "ECIT", ci.Name)
	for i := 0; i < 10; i++ {
		err = nc.PublishRequest(rsubj, "xx", jreq)
		require_NoError(t, err)
		nc.Flush()
		time.Sleep(25 * time.Millisecond)
	}

	_, err = js.ConsumerInfo("ECIT", ci.Name)
	require_NoError(t, err)
}

func TestJetStreamNakRedeliveryWithNoWait(t *testing.T) {
	s := RunBasicJetStreamServer(t)
	defer s.Shutdown()

	nc, js := jsClientConnect(t, s)
	defer nc.Close()

	_, err := js.AddStream(&nats.StreamConfig{
		Name:     "TEST",
		Subjects: []string{"foo"},
	})
	require_NoError(t, err)

	_, err = js.Publish("foo", []byte("NAK"))
	require_NoError(t, err)

	ccReq := &CreateConsumerRequest{
		Stream: "TEST",
		Config: ConsumerConfig{
			Durable:    "dlc",
			AckPolicy:  AckExplicit,
			MaxDeliver: 3,
			AckWait:    time.Minute,
			BackOff:    []time.Duration{5 * time.Second, 10 * time.Second},
		},
	}
	// Do by hand for now until Go client catches up.
	req, err := json.Marshal(ccReq)
	require_NoError(t, err)
	resp, err := nc.Request(fmt.Sprintf(JSApiDurableCreateT, "TEST", "dlc"), req, time.Second)
	require_NoError(t, err)
	var ccResp JSApiConsumerCreateResponse
	err = json.Unmarshal(resp.Data, &ccResp)
	require_NoError(t, err)
	if ccResp.Error != nil {
		t.Fatalf("Unexpected error: %+v", ccResp.Error)
	}

	rsubj := fmt.Sprintf(JSApiRequestNextT, "TEST", "dlc")
	m, err := nc.Request(rsubj, nil, time.Second)
	require_NoError(t, err)

	// NAK this message.
	delay, err := json.Marshal(&ConsumerNakOptions{Delay: 500 * time.Millisecond})
	require_NoError(t, err)
	dnak := []byte(fmt.Sprintf("%s  %s", AckNak, delay))
	m.Respond(dnak)

	// This message should come back to us after 500ms. If we do a one-shot request, with NoWait and Expires
	// this will do the right thing and we get the message.
	// What we want to test here is a true NoWait request with Expires==0 and eventually seeing the message be redelivered.
	expires := time.Now().Add(time.Second)
	for time.Now().Before(expires) {
		m, err = nc.Request(rsubj, []byte(`{"batch":1, "no_wait": true}`), time.Second)
		require_NoError(t, err)
		if len(m.Data) > 0 {
			// We got our message, so we are good.
			return
		}
		// So we do not spin.
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatalf("Did not get the message in time")
}

// Test that we properly enforce per subject msg limits when DiscardNew is set.
// DiscardNew should only apply to stream limits, subject based limits should always be DiscardOld.
func TestJetStreamMaxMsgsPerSubjectWithDiscardNew(t *testing.T) {
	msc := StreamConfig{
		Name:       "TEST",
		Subjects:   []string{"foo", "bar", "baz", "x"},
		Discard:    DiscardNew,
		Storage:    MemoryStorage,
		MaxMsgsPer: 4,
		MaxMsgs:    10,
		MaxBytes:   500,
	}
	fsc := msc
	fsc.Storage = FileStorage

	cases := []struct {
		name    string
		mconfig *StreamConfig
	}{
		{"MemoryStore", &msc},
		{"FileStore", &fsc},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			s := RunBasicJetStreamServer(t)
			defer s.Shutdown()

			mset, err := s.GlobalAccount().addStream(c.mconfig)
			require_NoError(t, err)
			defer mset.delete()

			// Client for API requests.
			nc, js := jsClientConnect(t, s)
			defer nc.Close()

			pubAndCheck := func(subj string, num int, expectedNumMsgs uint64) {
				t.Helper()
				for i := 0; i < num; i++ {
					_, err = js.Publish(subj, []byte("TSLA"))
					require_NoError(t, err)
				}
				si, err := js.StreamInfo("TEST")
				require_NoError(t, err)
				if si.State.Msgs != expectedNumMsgs {
					t.Fatalf("Expected %d msgs, got %d", expectedNumMsgs, si.State.Msgs)
				}
			}

			pubExpectErr := func(subj string, sz int) {
				t.Helper()
				_, err = js.Publish(subj, bytes.Repeat([]byte("X"), sz))
				require_Error(t, err, errors.New("nats: maximum bytes exceeded"), errors.New("nats: maximum messages exceeded"))
			}

			pubAndCheck("foo", 1, 1)
			// We should treat this as DiscardOld and only have 4 msgs after.
			pubAndCheck("foo", 4, 4)
			// Same thing here, shoud only have 4 foo and 4 bar for total of 8.
			pubAndCheck("bar", 8, 8)
			// We have 8 here, so only 2 left. If we add in a new subject when we have room it will be accepted.
			pubAndCheck("baz", 2, 10)
			// Now we are full, but makeup is foo-4 bar-4 baz-2.
			// We can add to foo and bar since they are at their max and adding new ones there stays the same in terms of total of 10.
			pubAndCheck("foo", 1, 10)
			pubAndCheck("bar", 1, 10)
			// Try to send a large message under an established subject that will exceed the 500 maximum.
			// Even though we have a bar subject and its at its maximum, the message to be dropped is not big enough, so this should err.
			pubExpectErr("bar", 300)
			// Also even though we have room bytes wise, if we introduce a new subject this should fail too on msg limit exceeded.
			pubExpectErr("x", 2)
		})
	}
}

func TestJetStreamStreamInfoSubjectsDetails(t *testing.T) {
	s := RunBasicJetStreamServer(t)
	defer s.Shutdown()

	nc, js := jsClientConnect(t, s)
	defer nc.Close()

	getInfo := func(t *testing.T, filter string) *StreamInfo {
		t.Helper()
		// Need to grab StreamInfo by hand for now.
		req, err := json.Marshal(&JSApiStreamInfoRequest{SubjectsFilter: filter})
		require_NoError(t, err)
		resp, err := nc.Request(fmt.Sprintf(JSApiStreamInfoT, "TEST"), req, time.Second)
		require_NoError(t, err)
		var si StreamInfo
		err = json.Unmarshal(resp.Data, &si)
		require_NoError(t, err)
		if si.State.NumSubjects != 3 {
			t.Fatalf("Expected NumSubjects to be 3, but got %d", si.State.NumSubjects)
		}
		return &si
	}

	testSubjects := func(t *testing.T, st nats.StorageType) {
		_, err := js.AddStream(&nats.StreamConfig{
			Name:     "TEST",
			Subjects: []string{"*"},
			Storage:  st,
		})
		require_NoError(t, err)
		defer js.DeleteStream("TEST")

		counts, msg := []int{22, 33, 44}, []byte("ok")
		// Now place msgs, foo-22, bar-33 and baz-44.
		for i, subj := range []string{"foo", "bar", "baz"} {
			for n := 0; n < counts[i]; n++ {
				_, err = js.Publish(subj, msg)
				require_NoError(t, err)
			}
		}

		// Test all subjects first.
		expected := map[string]uint64{"foo": 22, "bar": 33, "baz": 44}
		if si := getInfo(t, nats.AllKeys); !reflect.DeepEqual(si.State.Subjects, expected) {
			t.Fatalf("Expected subjects of %+v, but got %+v", expected, si.State.Subjects)
		}
		if si := getInfo(t, "*"); !reflect.DeepEqual(si.State.Subjects, expected) {
			t.Fatalf("Expected subjects of %+v, but got %+v", expected, si.State.Subjects)
		}
		// Filtered to 1.
		expected = map[string]uint64{"foo": 22}
		if si := getInfo(t, "foo"); !reflect.DeepEqual(si.State.Subjects, expected) {
			t.Fatalf("Expected subjects of %+v, but got %+v", expected, si.State.Subjects)
		}
	}

	t.Run("MemoryStore", func(t *testing.T) { testSubjects(t, nats.MemoryStorage) })
	t.Run("FileStore", func(t *testing.T) { testSubjects(t, nats.FileStorage) })
}

func TestJetStreamStreamInfoSubjectsDetailsWithDeleteAndPurge(t *testing.T) {
	s := RunBasicJetStreamServer(t)
	defer s.Shutdown()

	nc, js := jsClientConnect(t, s)
	defer nc.Close()

	getInfo := func(t *testing.T, filter string) *StreamInfo {
		t.Helper()
		// Need to grab StreamInfo by hand for now.
		req, err := json.Marshal(&JSApiStreamInfoRequest{SubjectsFilter: filter})
		require_NoError(t, err)
		resp, err := nc.Request(fmt.Sprintf(JSApiStreamInfoT, "TEST"), req, time.Second)
		require_NoError(t, err)
		var si StreamInfo
		err = json.Unmarshal(resp.Data, &si)
		require_NoError(t, err)
		return &si
	}

	checkResults := func(t *testing.T, expected map[string]uint64) {
		t.Helper()
		si := getInfo(t, nats.AllKeys)
		if !reflect.DeepEqual(si.State.Subjects, expected) {
			t.Fatalf("Expected subjects of %+v, but got %+v", expected, si.State.Subjects)
		}
		if si.State.NumSubjects != len(expected) {
			t.Fatalf("Expected NumSubjects to be %d, but got %d", len(expected), si.State.NumSubjects)
		}
	}

	testSubjects := func(t *testing.T, st nats.StorageType) {
		_, err := js.AddStream(&nats.StreamConfig{
			Name:     "TEST",
			Subjects: []string{"*"},
			Storage:  st,
		})
		require_NoError(t, err)
		defer js.DeleteStream("TEST")

		msg := []byte("ok")
		js.Publish("foo", msg) // 1
		js.Publish("foo", msg) // 2
		js.Publish("bar", msg) // 3
		js.Publish("baz", msg) // 4
		js.Publish("baz", msg) // 5
		js.Publish("bar", msg) // 6
		js.Publish("bar", msg) // 7

		checkResults(t, map[string]uint64{"foo": 2, "bar": 3, "baz": 2})

		// Now delete some messages.
		js.DeleteMsg("TEST", 6)

		checkResults(t, map[string]uint64{"foo": 2, "bar": 2, "baz": 2})

		// Delete and add right back, so no-op
		js.DeleteMsg("TEST", 5) // baz
		js.Publish("baz", msg)  // 8

		checkResults(t, map[string]uint64{"foo": 2, "bar": 2, "baz": 2})

		// Now do a purge only of bar.
		jr, _ := json.Marshal(&JSApiStreamPurgeRequest{Subject: "bar"})
		_, err = nc.Request(fmt.Sprintf(JSApiStreamPurgeT, "TEST"), jr, time.Second)
		require_NoError(t, err)

		checkResults(t, map[string]uint64{"foo": 2, "baz": 2})

		// Now purge everything
		err = js.PurgeStream("TEST")
		require_NoError(t, err)

		si := getInfo(t, nats.AllKeys)
		if len(si.State.Subjects) != 0 {
			t.Fatalf("Expected no subjects, but got %+v", si.State.Subjects)
		}
		if si.State.NumSubjects != 0 {
			t.Fatalf("Expected NumSubjects to be 0, but got %d", si.State.NumSubjects)
		}
	}

	t.Run("MemoryStore", func(t *testing.T) { testSubjects(t, nats.MemoryStorage) })
	t.Run("FileStore", func(t *testing.T) { testSubjects(t, nats.FileStorage) })
}

func TestJetStreamStreamInfoSubjectsDetailsAfterRestart(t *testing.T) {
	s := RunBasicJetStreamServer(t)
	defer s.Shutdown()

	nc, js := jsClientConnect(t, s)
	defer nc.Close()

	getInfo := func(t *testing.T, filter string) *StreamInfo {
		t.Helper()
		// Need to grab StreamInfo by hand for now.
		req, err := json.Marshal(&JSApiStreamInfoRequest{SubjectsFilter: filter})
		require_NoError(t, err)
		resp, err := nc.Request(fmt.Sprintf(JSApiStreamInfoT, "TEST"), req, time.Second)
		require_NoError(t, err)
		var si StreamInfo
		err = json.Unmarshal(resp.Data, &si)
		require_NoError(t, err)
		return &si
	}

	_, err := js.AddStream(&nats.StreamConfig{
		Name:     "TEST",
		Subjects: []string{"*"},
	})
	require_NoError(t, err)
	defer js.DeleteStream("TEST")

	msg := []byte("ok")
	js.Publish("foo", msg) // 1
	js.Publish("foo", msg) // 2
	js.Publish("bar", msg) // 3
	js.Publish("baz", msg) // 4
	js.Publish("baz", msg) // 5

	si := getInfo(t, nats.AllKeys)
	if si.State.NumSubjects != 3 {
		t.Fatalf("Expected 3 subjects, but got %d", si.State.NumSubjects)
	}

	// Stop current
	nc.Close()
	sd := s.JetStreamConfig().StoreDir
	s.Shutdown()
	// Restart.
	s = RunJetStreamServerOnPort(-1, sd)
	defer s.Shutdown()

	nc, _ = jsClientConnect(t, s)
	defer nc.Close()

	si = getInfo(t, nats.AllKeys)
	if si.State.NumSubjects != 3 {
		t.Fatalf("Expected 3 subjects, but got %d", si.State.NumSubjects)
	}
}

// Issue #2836
func TestJetStreamInterestRetentionBug(t *testing.T) {
	s := RunBasicJetStreamServer(t)
	defer s.Shutdown()

	nc, js := jsClientConnect(t, s)
	defer nc.Close()

	_, err := js.AddStream(&nats.StreamConfig{
		Name:      "TEST",
		Subjects:  []string{"foo.>"},
		Retention: nats.InterestPolicy,
	})
	require_NoError(t, err)

	_, err = js.AddConsumer("TEST", &nats.ConsumerConfig{Durable: "c1", AckPolicy: nats.AckExplicitPolicy})
	require_NoError(t, err)

	test := func(token string, fseq, msgs uint64) {
		t.Helper()
		subj := fmt.Sprintf("foo.%s", token)
		_, err = js.Publish(subj, nil)
		require_NoError(t, err)
		si, err := js.StreamInfo("TEST")
		require_NoError(t, err)
		if si.State.FirstSeq != fseq {
			t.Fatalf("Expected first to be %d, got %d", fseq, si.State.FirstSeq)
		}
		if si.State.Msgs != msgs {
			t.Fatalf("Expected msgs to be %d, got %d", msgs, si.State.Msgs)
		}
	}

	test("bar", 1, 1)

	// Create second filtered consumer.
	_, err = js.AddConsumer("TEST", &nats.ConsumerConfig{Durable: "c2", FilterSubject: "foo.foo", AckPolicy: nats.AckExplicitPolicy})
	require_NoError(t, err)

	test("bar", 1, 2)
}

// Under load testing for K3S and the KINE interface we saw some stalls.
// These were caused by a dynamic that would not send a second FC item with one
// pending, but when we sent the next message and got blocked, if that msg would
// exceed the outstanding FC we would become stalled.
func TestJetStreamFlowControlStall(t *testing.T) {
	s := RunBasicJetStreamServer(t)
	defer s.Shutdown()

	nc, js := jsClientConnect(t, s)
	defer nc.Close()

	_, err := js.AddStream(&nats.StreamConfig{Name: "FC"})
	require_NoError(t, err)

	msg := []byte(strings.Repeat("X", 32768))
	_, err = js.Publish("FC", msg)
	require_NoError(t, err)

	msg = []byte(strings.Repeat("X", 8*32768))
	_, err = js.Publish("FC", msg)
	require_NoError(t, err)
	_, err = js.Publish("FC", msg)
	require_NoError(t, err)

	sub, err := js.SubscribeSync("FC", nats.OrderedConsumer())
	require_NoError(t, err)

	checkSubsPending(t, sub, 3)
}

func TestJetStreamStorageReservedBytes(t *testing.T) {
	const systemLimit = 1024
	opts := DefaultTestOptions
	opts.Port = -1
	opts.JetStream = true
	opts.JetStreamMaxMemory = systemLimit
	opts.JetStreamMaxStore = systemLimit
	opts.StoreDir = t.TempDir()
	opts.HTTPPort = -1
	s := RunServer(&opts)

	defer s.Shutdown()

	// Client for API requests.
	nc, js := jsClientConnect(t, s)
	defer nc.Close()

	getJetStreamVarz := func(hc *http.Client, addr string) (JetStreamVarz, error) {
		resp, err := hc.Get(addr)
		if err != nil {
			return JetStreamVarz{}, err
		}
		defer resp.Body.Close()

		var v Varz
		if err := json.NewDecoder(resp.Body).Decode(&v); err != nil {
			return JetStreamVarz{}, err
		}

		return v.JetStream, nil
	}
	getReserved := func(hc *http.Client, addr string, st nats.StorageType) (uint64, error) {
		jsv, err := getJetStreamVarz(hc, addr)
		if err != nil {
			return 0, err
		}
		if st == nats.MemoryStorage {
			return jsv.Stats.ReservedMemory, nil
		}
		return jsv.Stats.ReservedStore, nil
	}

	varzAddr := fmt.Sprintf("http://127.0.0.1:%d/varz", s.MonitorAddr().Port)
	hc := &http.Client{Timeout: 5 * time.Second}

	jsv, err := getJetStreamVarz(hc, varzAddr)
	require_NoError(t, err)

	if got, want := systemLimit, int(jsv.Config.MaxMemory); got != want {
		t.Fatalf("Unexpected max memory: got=%d, want=%d", got, want)
	}
	if got, want := systemLimit, int(jsv.Config.MaxStore); got != want {
		t.Fatalf("Unexpected max store: got=%d, want=%d", got, want)
	}

	cases := []struct {
		name            string
		accountLimit    int64
		storage         nats.StorageType
		createMaxBytes  int64
		updateMaxBytes  int64
		wantUpdateError bool
	}{
		{
			name:           "file reserve 66% of system limit",
			accountLimit:   -1,
			storage:        nats.FileStorage,
			createMaxBytes: int64(math.Round(float64(systemLimit) * .666)),
			updateMaxBytes: int64(math.Round(float64(systemLimit)*.666)) + 1,
		},
		{
			name:           "memory reserve 66% of system limit",
			accountLimit:   -1,
			storage:        nats.MemoryStorage,
			createMaxBytes: int64(math.Round(float64(systemLimit) * .666)),
			updateMaxBytes: int64(math.Round(float64(systemLimit)*.666)) + 1,
		},
		{
			name:            "file update past system limit",
			accountLimit:    -1,
			storage:         nats.FileStorage,
			createMaxBytes:  systemLimit,
			updateMaxBytes:  systemLimit + 1,
			wantUpdateError: true,
		},
		{
			name:            "memory update past system limit",
			accountLimit:    -1,
			storage:         nats.MemoryStorage,
			createMaxBytes:  systemLimit,
			updateMaxBytes:  systemLimit + 1,
			wantUpdateError: true,
		},
		{
			name:           "file update to system limit",
			accountLimit:   -1,
			storage:        nats.FileStorage,
			createMaxBytes: systemLimit - 1,
			updateMaxBytes: systemLimit,
		},
		{
			name:           "memory update to system limit",
			accountLimit:   -1,
			storage:        nats.MemoryStorage,
			createMaxBytes: systemLimit - 1,
			updateMaxBytes: systemLimit,
		},
		{
			name:           "file reserve 66% of account limit",
			accountLimit:   systemLimit / 2,
			storage:        nats.FileStorage,
			createMaxBytes: int64(math.Round(float64(systemLimit/2) * .666)),
			updateMaxBytes: int64(math.Round(float64(systemLimit/2)*.666)) + 1,
		},
		{
			name:           "memory reserve 66% of account limit",
			accountLimit:   systemLimit / 2,
			storage:        nats.MemoryStorage,
			createMaxBytes: int64(math.Round(float64(systemLimit/2) * .666)),
			updateMaxBytes: int64(math.Round(float64(systemLimit/2)*.666)) + 1,
		},
		{
			name:            "file update past account limit",
			accountLimit:    systemLimit / 2,
			storage:         nats.FileStorage,
			createMaxBytes:  (systemLimit / 2),
			updateMaxBytes:  (systemLimit / 2) + 1,
			wantUpdateError: true,
		},
		{
			name:            "memory update past account limit",
			accountLimit:    systemLimit / 2,
			storage:         nats.MemoryStorage,
			createMaxBytes:  (systemLimit / 2),
			updateMaxBytes:  (systemLimit / 2) + 1,
			wantUpdateError: true,
		},
		{
			name:           "file update to account limit",
			accountLimit:   systemLimit / 2,
			storage:        nats.FileStorage,
			createMaxBytes: (systemLimit / 2) - 1,
			updateMaxBytes: (systemLimit / 2),
		},
		{
			name:           "memory update to account limit",
			accountLimit:   systemLimit / 2,
			storage:        nats.MemoryStorage,
			createMaxBytes: (systemLimit / 2) - 1,
			updateMaxBytes: (systemLimit / 2),
		},
	}
	for i := 0; i < len(cases) && !t.Failed(); i++ {
		c := cases[i]
		t.Run(c.name, func(st *testing.T) {
			// Setup limits
			err = s.GlobalAccount().UpdateJetStreamLimits(map[string]JetStreamAccountLimits{
				_EMPTY_: {
					MaxMemory: c.accountLimit,
					MaxStore:  c.accountLimit,
				},
			})
			require_NoError(st, err)

			// Create initial stream
			cfg := &nats.StreamConfig{
				Name:     "TEST",
				Subjects: []string{"foo"},
				Storage:  c.storage,
				MaxBytes: c.createMaxBytes,
			}
			_, err = js.AddStream(cfg)
			require_NoError(st, err)

			// Update stream MaxBytes
			cfg.MaxBytes = c.updateMaxBytes
			info, err := js.UpdateStream(cfg)
			if c.wantUpdateError && err == nil {
				got := info.Config.MaxBytes
				st.Fatalf("Unexpected update success, newMaxBytes=%d; systemLimit=%d; accountLimit=%d",
					got, systemLimit, c.accountLimit)
			} else if !c.wantUpdateError && err != nil {
				st.Fatalf("Unexpected update error: %s", err)
			}

			if !c.wantUpdateError && err == nil {
				// If update was successful, then ensure reserved shows new
				// amount
				reserved, err := getReserved(hc, varzAddr, c.storage)
				require_NoError(st, err)
				if got, want := reserved, uint64(c.updateMaxBytes); got != want {
					st.Fatalf("Unexpected reserved: %d, want %d", got, want)
				}
			}

			// Delete stream
			err = js.DeleteStream("TEST")
			require_NoError(st, err)

			// Ensure reserved shows 0 because we've deleted the stream
			reserved, err := getReserved(hc, varzAddr, c.storage)
			require_NoError(st, err)
			if reserved != 0 {
				st.Fatalf("Unexpected reserved: %d, want 0", reserved)
			}
		})
	}
}

func TestJetStreamRestoreBadStream(t *testing.T) {
	s := RunBasicJetStreamServer(t)
	defer s.Shutdown()

	nc, _ := jsClientConnect(t, s)
	defer nc.Close()

	var rreq JSApiStreamRestoreRequest
	buf, err := os.ReadFile("../test/configs/jetstream/restore_bad_stream/backup.json")
	require_NoError(t, err)
	err = json.Unmarshal(buf, &rreq)
	require_NoError(t, err)

	data, err := os.Open("../test/configs/jetstream/restore_bad_stream/stream.tar.s2")
	require_NoError(t, err)
	defer data.Close()

	var rresp JSApiStreamRestoreResponse
	msg, err := nc.Request(fmt.Sprintf(JSApiStreamRestoreT, rreq.Config.Name), buf, 5*time.Second)
	require_NoError(t, err)
	json.Unmarshal(msg.Data, &rresp)
	if rresp.Error != nil {
		t.Fatalf("Error on restore: %+v", rresp.Error)
	}

	var chunk [1024]byte
	gotChunkErr := false
	for {
		n, err := data.Read(chunk[:])
		if err == io.EOF {
			break
		}
		require_NoError(t, err)

		msg, err = nc.Request(rresp.DeliverSubject, chunk[:n], 5*time.Second)
		require_NoError(t, err)
		if len(msg.Data) == 0 {
			continue
		}
		require_False(t, bytes.Equal(msg.Data, []byte("-ERR 'restore stalled'")))
		json.Unmarshal(msg.Data, &rresp)
		if rresp.Error != nil {
			require_Contains(t, rresp.Error.Description, "unexpected")
			gotChunkErr = true
			break
		}
	}
	if !gotChunkErr {
		msg, err = nc.Request(rresp.DeliverSubject, nil, 5*time.Second)
		require_NoError(t, err)
		require_False(t, bytes.Equal(msg.Data, []byte("-ERR 'restore stalled'")))
		json.Unmarshal(msg.Data, &rresp)
		require_True(t, rresp.Error != nil)
		require_Contains(t, rresp.Error.Description, "unexpected")
	}

	dir := filepath.Join(s.JetStreamConfig().StoreDir, globalAccountName)
	f1 := filepath.Join(dir, "fail1.txt")
	f2 := filepath.Join(dir, "fail2.txt")
	for _, f := range []string{f1, f2} {
		if _, err := os.Stat(f); err == nil {
			t.Fatalf("Found file %s", f)
		}
	}
}

func TestJetStreamRestoreFormatDetectionBounded(t *testing.T) {
	s := RunBasicJetStreamServer(t)
	defer s.Shutdown()

	nc, _ := jsClientConnect(t, s)
	defer nc.Close()

	req, err := json.Marshal(&JSApiStreamRestoreRequest{Config: StreamConfig{Name: "TEST", Storage: FileStorage}})
	require_NoError(t, err)
	var rresp JSApiStreamRestoreResponse
	msg, err := nc.Request(fmt.Sprintf(JSApiStreamRestoreT, "TEST"), req, 5*time.Second)
	require_NoError(t, err)
	require_NoError(t, json.Unmarshal(msg.Data, &rresp))
	require_True(t, rresp.Error == nil)

	// An S2 stream identifier followed only by skippable padding chunks never yields a preamble.
	padLen := 64 * 1024
	padding := make([]byte, s2ChunkHeaderSize+padLen)
	padding[0], padding[1], padding[2], padding[3] = 0xfe, byte(padLen), byte(padLen>>8), byte(padLen>>16)
	chunks := [][]byte{[]byte(s2MagicChunk)}
	for sent := 0; sent <= 2*jsRestoreFormatDetectLimit; sent += len(padding) {
		chunks = append(chunks, padding)
	}

	// The empty end-of-stream chunk is sent last and returns the restore error.
	chunks = append(chunks, nil)
	for _, chunk := range chunks {
		msg, err = nc.Request(rresp.DeliverSubject, chunk, 5*time.Second)
		require_NoError(t, err)
		if len(msg.Data) == 0 {
			continue
		}
		require_NoError(t, json.Unmarshal(msg.Data, &rresp))
		break
	}
	require_True(t, rresp.Error != nil)
	require_Contains(t, rresp.Error.Description, "size limit")
}

func TestJetStreamRemoveExternalSource(t *testing.T) {
	ho := DefaultTestOptions
	ho.Port = 4000 //-1
	ho.LeafNode.Host = "127.0.0.1"
	ho.LeafNode.Port = -1
	hs := RunServer(&ho)
	defer hs.Shutdown()

	lu, err := url.Parse(fmt.Sprintf("nats://127.0.0.1:%d", ho.LeafNode.Port))
	require_NoError(t, err)

	lo1 := DefaultTestOptions
	lo1.Port = 4111 //-1
	lo1.ServerName = "a-leaf"
	lo1.JetStream = true
	lo1.StoreDir = t.TempDir()
	lo1.JetStreamDomain = "a-leaf"
	lo1.LeafNode.Remotes = []*RemoteLeafOpts{{URLs: []*url.URL{lu}}}
	l1 := RunServer(&lo1)
	defer l1.Shutdown()

	lo2 := DefaultTestOptions
	lo2.Port = 2111 //-1
	lo2.ServerName = "b-leaf"
	lo2.JetStream = true
	lo2.StoreDir = t.TempDir()
	lo2.JetStreamDomain = "b-leaf"
	lo2.LeafNode.Remotes = []*RemoteLeafOpts{{URLs: []*url.URL{lu}}}
	l2 := RunServer(&lo2)
	defer l2.Shutdown()

	checkLeafNodeConnected(t, l1)
	checkLeafNodeConnected(t, l2)

	checkStreamMsgs := func(js nats.JetStreamContext, stream string, expected uint64) {
		t.Helper()
		checkFor(t, 2*time.Second, 15*time.Millisecond, func() error {
			si, err := js.StreamInfo(stream)
			if err != nil {
				return err
			}
			if si.State.Msgs != expected {
				return fmt.Errorf("Expected %v messages, got %v", expected, si.State.Msgs)
			}
			return nil
		})
	}

	sendToStreamTest := func(js nats.JetStreamContext) {
		t.Helper()
		for i := 0; i < 10; i++ {
			_, err = js.Publish("test", []byte("hello"))
			require_NoError(t, err)
		}
	}

	nca, jsa := jsClientConnect(t, l1)
	defer nca.Close()
	_, err = jsa.AddStream(&nats.StreamConfig{Name: "queue", Subjects: []string{"queue"}})
	require_NoError(t, err)

	_, err = jsa.AddStream(&nats.StreamConfig{Name: "testdel", Subjects: []string{"testdel"}})
	require_NoError(t, err)

	ncb, jsb := jsClientConnect(t, l2)
	defer ncb.Close()
	_, err = jsb.AddStream(&nats.StreamConfig{Name: "test", Subjects: []string{"test"}})
	require_NoError(t, err)
	sendToStreamTest(jsb)
	checkStreamMsgs(jsb, "test", 10)

	_, err = jsb.AddStream(&nats.StreamConfig{Name: "testdelsrc1", Subjects: []string{"testdelsrc1"}})
	require_NoError(t, err)
	_, err = jsb.AddStream(&nats.StreamConfig{Name: "testdelsrc2", Subjects: []string{"testdelsrc2"}})
	require_NoError(t, err)

	// Add test as source to queue
	si, err := jsa.UpdateStream(&nats.StreamConfig{
		Name:     "queue",
		Subjects: []string{"queue"},
		Sources: []*nats.StreamSource{
			{
				Name: "test",
				External: &nats.ExternalStream{
					APIPrefix: "$JS.b-leaf.API",
				},
			},
		},
	})
	require_NoError(t, err)
	require_True(t, len(si.Config.Sources) == 1)
	checkStreamMsgs(jsa, "queue", 10)

	// add more entries to "test"
	sendToStreamTest(jsb)

	// verify entries are both in "test" and "queue"
	checkStreamMsgs(jsb, "test", 20)
	checkStreamMsgs(jsa, "queue", 20)

	// Remove source
	si, err = jsa.UpdateStream(&nats.StreamConfig{
		Name:     "queue",
		Subjects: []string{"queue"},
	})
	require_NoError(t, err)
	require_True(t, len(si.Config.Sources) == 0)

	// add more entries to "test"
	sendToStreamTest(jsb)
	// verify entries are in "test"
	checkStreamMsgs(jsb, "test", 30)

	// But they should not be in "queue". We will wait a bit before checking
	// to make sure that we are letting enough time for the sourcing to
	// incorrectly happen if there is a bug.
	time.Sleep(250 * time.Millisecond)
	checkStreamMsgs(jsa, "queue", 20)

	// Test that we delete correctly. First add source to a "testdel"
	si, err = jsa.UpdateStream(&nats.StreamConfig{
		Name:     "testdel",
		Subjects: []string{"testdel"},
		Sources: []*nats.StreamSource{
			{
				Name: "testdelsrc1",
				External: &nats.ExternalStream{
					APIPrefix: "$JS.b-leaf.API",
				},
			},
		},
	})
	require_NoError(t, err)
	require_True(t, len(si.Config.Sources) == 1)
	// Now add the second one...
	si, err = jsa.UpdateStream(&nats.StreamConfig{
		Name:     "testdel",
		Subjects: []string{"testdel"},
		Sources: []*nats.StreamSource{
			{
				Name: "testdelsrc1",
				External: &nats.ExternalStream{
					APIPrefix: "$JS.b-leaf.API",
				},
			},
			{
				Name: "testdelsrc2",
				External: &nats.ExternalStream{
					APIPrefix: "$JS.b-leaf.API",
				},
			},
		},
	})
	require_NoError(t, err)
	require_True(t, len(si.Config.Sources) == 2)
	// Now check that the stream testdel has still 2 source consumers...
	acc, err := l1.lookupAccount(globalAccountName)
	require_NoError(t, err)
	mset, err := acc.lookupStream("testdel")
	require_NoError(t, err)
	mset.mu.RLock()
	n := len(mset.sources)
	mset.mu.RUnlock()
	if n != 2 {
		t.Fatalf("Expected 2 sources, got %v", n)
	}

	// Restart leaf "a"
	nca.Close()
	l1.Shutdown()
	l1 = RunServer(&lo1)
	defer l1.Shutdown()

	// add more entries to "test"
	sendToStreamTest(jsb)
	checkStreamMsgs(jsb, "test", 40)

	nca, jsa = jsClientConnect(t, l1)
	defer nca.Close()
	time.Sleep(250 * time.Millisecond)
	checkStreamMsgs(jsa, "queue", 20)
}

func TestJetStreamAddStreamWithFilestoreFailure(t *testing.T) {
	s := RunBasicJetStreamServer(t)
	defer s.Shutdown()

	// Cause failure to create stream with filestore.
	// In one of ipQueue changes, this could cause a panic, so verify that we get
	// a failure to create, but no panic.
	if _, err := s.globalAccount().addStreamWithStore(
		&StreamConfig{Name: "TEST"},
		&FileStoreConfig{BlockSize: 2 * maxBlockSize}); err == nil {
		t.Fatal("Expected failure, did not get one")
	}
}

type checkFastState struct {
	count int64
	StreamStore
}

func (s *checkFastState) FastState(state *StreamState) {
	// Keep track only when called from checkPending()
	if bytes.Contains(debug.Stack(), []byte("checkPending(")) {
		atomic.AddInt64(&s.count, 1)
	}
	s.StreamStore.FastState(state)
}

func TestJetStreamBackOffCheckPending(t *testing.T) {
	s := RunBasicJetStreamServer(t)
	defer s.Shutdown()

	mset, err := s.GlobalAccount().addStream(&StreamConfig{Name: "TEST", Subjects: []string{"foo"}})
	if err != nil {
		t.Fatalf("Unexpected error adding stream: %v", err)
	}
	defer mset.delete()

	// Plug or store to see how many times we invoke FastState, which is done in checkPending
	mset.mu.Lock()
	st := &checkFastState{StreamStore: mset.store}
	mset.store = st
	mset.mu.Unlock()

	nc := clientConnectToServer(t, s)
	defer nc.Close()

	sendStreamMsg(t, nc, "foo", "Hello World!")

	sub, _ := nc.SubscribeSync(nats.NewInbox())
	defer sub.Unsubscribe()
	nc.Flush()

	o, err := mset.addConsumer(&ConsumerConfig{
		DeliverSubject: sub.Subject,
		AckPolicy:      AckExplicit,
		MaxDeliver:     1000,
		BackOff:        []time.Duration{50 * time.Millisecond, 250 * time.Millisecond, time.Second},
	})
	if err != nil {
		t.Fatalf("Expected no error, got %v", err)
	}
	defer o.delete()

	// Check the first delivery and the following 2 redeliveries
	start := time.Now()
	natsNexMsg(t, sub, time.Second)
	if dur := time.Since(start); dur >= 50*time.Millisecond {
		t.Fatalf("Expected first delivery to be fast, took: %v", dur)
	}
	start = time.Now()
	natsNexMsg(t, sub, time.Second)
	if dur := time.Since(start); dur < 25*time.Millisecond || dur > 75*time.Millisecond {
		t.Fatalf("Expected first redelivery to be ~50ms, took: %v", dur)
	}
	start = time.Now()
	natsNexMsg(t, sub, time.Second)
	if dur := time.Since(start); dur < 200*time.Millisecond || dur > 300*time.Millisecond {
		t.Fatalf("Expected first redelivery to be ~250ms, took: %v", dur)
	}
	// There was a bug that would cause checkPending to be invoked based on the
	// ackWait (which in this case would be the first value of BackOff, which
	// is 50ms). So we would call checkPending() too many times.
	time.Sleep(500 * time.Millisecond)
	// Check now, it should have been invoked 2/3 times.
	if n := atomic.LoadInt64(&st.count); n < 2 || n > 3 {
		t.Fatalf("Expected checkPending to be invoked 2/3 times, was %v", n)
	}
}

func TestJetStreamCrossAccounts(t *testing.T) {
	conf := createConfFile(t, []byte(fmt.Sprintf(`
		listen: 127.0.0.1:-1
        jetstream {
			store_dir = %q
		}
        accounts: {
           A: {
               users: [ {user: a, password: a} ]
               jetstream: enabled
               exports: [
                   {service: '$JS.API.>' }
                   {service: '$KV.>'}
                   {stream: 'accI.>'}
               ]
           },
           I: {
               users: [ {user: i, password: i} ]
               imports: [
                   {service: {account: A, subject: '$JS.API.>'}, to: 'fromA.>' }
                   {service: {account: A, subject: '$KV.>'}, to: 'fromA.$KV.>' }
                   {stream: {subject: 'accI.>', account: A}}
               ]
           }
		}`, t.TempDir())))
	s, _ := RunServerWithConfig(conf)
	defer s.Shutdown()

	watchNext := func(w nats.KeyWatcher) nats.KeyValueEntry {
		t.Helper()
		select {
		case e := <-w.Updates():
			return e
		case <-time.After(time.Second):
			t.Fatal("Fail to get the next update")
		}
		return nil
	}

	nc1, js1 := jsClientConnect(t, s, nats.UserInfo("a", "a"))
	defer nc1.Close()

	kv1, err := js1.CreateKeyValue(&nats.KeyValueConfig{Bucket: "Map", History: 10})
	if err != nil {
		t.Fatalf("Error creating kv store: %v", err)
	}

	w1, err := kv1.Watch("map")
	if err != nil {
		t.Fatalf("Error creating watcher: %v", err)
	}
	if e := watchNext(w1); e != nil {
		t.Fatalf("Expected nil entry, got %+v", e)
	}

	nc2, err := nats.Connect(s.ClientURL(), nats.UserInfo("i", "i"), nats.CustomInboxPrefix("accI"))
	if err != nil {
		t.Fatalf("Error on connect: %v", err)
	}
	defer nc2.Close()
	js2, err := nc2.JetStream(nats.APIPrefix("fromA"))
	if err != nil {
		t.Fatalf("Error getting jetstream context: %v", err)
	}

	kv2, err := js2.CreateKeyValue(&nats.KeyValueConfig{Bucket: "Map", History: 10})
	if err != nil {
		t.Fatalf("Error creating kv store: %v", err)
	}

	w2, err := kv2.Watch("map")
	if err != nil {
		t.Fatalf("Error creating watcher: %v", err)
	}
	if e := watchNext(w2); e != nil {
		t.Fatalf("Expected nil entry, got %+v", e)
	}

	// Do a Put from kv2
	rev, err := kv2.Put("map", []byte("value"))
	if err != nil {
		t.Fatalf("Error on put: %v", err)
	}

	// Get from kv1
	e, err := kv1.Get("map")
	if err != nil {
		t.Fatalf("Error on get: %v", err)
	}
	if e.Key() != "map" || string(e.Value()) != "value" {
		t.Fatalf("Unexpected entry: +%v", e)
	}

	// Get from kv2
	e, err = kv2.Get("map")
	if err != nil {
		t.Fatalf("Error on get: %v", err)
	}
	if e.Key() != "map" || string(e.Value()) != "value" {
		t.Fatalf("Unexpected entry: +%v", e)
	}

	// Watcher 1
	if e := watchNext(w1); e == nil || e.Key() != "map" || string(e.Value()) != "value" {
		t.Fatalf("Unexpected entry: %+v", e)
	}

	// Watcher 2
	if e := watchNext(w2); e == nil || e.Key() != "map" || string(e.Value()) != "value" {
		t.Fatalf("Unexpected entry: %+v", e)
	}

	// Try an update form kv2
	if _, err := kv2.Update("map", []byte("updated"), rev); err != nil {
		t.Fatalf("Failed to update: %v", err)
	}

	// Get from kv1
	e, err = kv1.Get("map")
	if err != nil {
		t.Fatalf("Error on get: %v", err)
	}
	if e.Key() != "map" || string(e.Value()) != "updated" {
		t.Fatalf("Unexpected entry: +%v", e)
	}

	// Get from kv2
	e, err = kv2.Get("map")
	if err != nil {
		t.Fatalf("Error on get: %v", err)
	}
	if e.Key() != "map" || string(e.Value()) != "updated" {
		t.Fatalf("Unexpected entry: +%v", e)
	}

	// Watcher 1
	if e := watchNext(w1); e == nil || e.Key() != "map" || string(e.Value()) != "updated" {
		t.Fatalf("Unexpected entry: %+v", e)
	}

	// Watcher 2
	if e := watchNext(w2); e == nil || e.Key() != "map" || string(e.Value()) != "updated" {
		t.Fatalf("Unexpected entry: %+v", e)
	}

	// Purge from kv2
	if err := kv2.Purge("map"); err != nil {
		t.Fatalf("Error on purge: %v", err)
	}

	// Check purge ok from w1
	if e := watchNext(w1); e == nil || e.Operation() != nats.KeyValuePurge {
		t.Fatalf("Unexpected entry: %+v", e)
	}

	// Check purge ok from w2
	if e := watchNext(w2); e == nil || e.Operation() != nats.KeyValuePurge {
		t.Fatalf("Unexpected entry: %+v", e)
	}

	// Delete purge records from kv2
	if err := kv2.PurgeDeletes(nats.DeleteMarkersOlderThan(-1)); err != nil {
		t.Fatalf("Error on purge deletes: %v", err)
	}

	// Check all gone from js1
	if si, err := js1.StreamInfo("KV_Map"); err != nil || si == nil || si.State.Msgs != 0 {
		t.Fatalf("Error getting stream info: err=%v si=%+v", err, si)
	}

	// Delete key from kv2
	if err := kv2.Delete("map"); err != nil {
		t.Fatalf("Error on delete: %v", err)
	}

	// Check key gone from kv1
	if e, err := kv1.Get("map"); err != nats.ErrKeyNotFound || e != nil {
		t.Fatalf("Expected key not found, got err=%v e=%+v", err, e)
	}
}

func TestJetStreamInvalidRestoreRequests(t *testing.T) {
	test := func(t *testing.T, s *Server, replica int) {
		nc := natsConnect(t, s.ClientURL())
		defer nc.Close()
		// test invalid stream config in restore request
		require_fail := func(cfg StreamConfig, errDesc string) {
			t.Helper()
			rreq := &JSApiStreamRestoreRequest{
				Config: cfg,
			}
			req, err := json.Marshal(rreq)
			require_NoError(t, err)
			rmsg, err := nc.Request(fmt.Sprintf(JSApiStreamRestoreT, "fail"), req, time.Second)
			if err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}
			var rresp JSApiStreamRestoreResponse
			json.Unmarshal(rmsg.Data, &rresp)
			require_True(t, rresp.Error != nil)
			require_Equal(t, rresp.Error.Description, errDesc)
		}
		require_fail(StreamConfig{Name: "fail", MaxBytes: 1024, Storage: FileStorage, Replicas: 6},
			"maximum replicas is 5")
		require_fail(StreamConfig{Name: "fail", MaxBytes: 2 * 1012 * 1024, Storage: FileStorage, Replicas: replica},
			"insufficient storage resources available")
		js, err := nc.JetStream()
		require_NoError(t, err)
		_, err = js.AddStream(&nats.StreamConfig{Name: "stream", MaxBytes: 1024, Storage: nats.FileStorage, Replicas: 1})
		require_NoError(t, err)
		require_fail(StreamConfig{Name: "fail", MaxBytes: 1024, Storage: FileStorage},
			"maximum number of streams reached")
	}

	commonAccSection := `
		no_auth_user: u
		accounts {
			ONE {
				users = [ { user: "u", pass: "s3cr3t!" } ]
				jetstream: {
					max_store: 1Mb
					max_streams: 1
				}
			}
			$SYS { users = [ { user: "admin", pass: "s3cr3t!" } ] }
		}`

	t.Run("clustered", func(t *testing.T) {
		c := createJetStreamClusterWithTemplate(t, `
			listen: 127.0.0.1:-1
			server_name: %s
			jetstream: {
				max_mem_store: 2MB,
				max_file_store: 8MB,
				store_dir: '%s',
			}
			cluster {
				name: %s
				listen: 127.0.0.1:%d
				routes = [%s]
			}`+commonAccSection, "clust", 3)
		defer c.shutdown()
		s := c.randomServer()
		test(t, s, 3)
	})
	t.Run("single", func(t *testing.T) {
		storeDir := t.TempDir()
		conf := createConfFile(t, []byte(fmt.Sprintf(`
			listen: 127.0.0.1:-1
			jetstream: {max_mem_store: 2MB, max_file_store: 8MB, store_dir: '%s'}
			%s`, storeDir, commonAccSection)))
		s, _ := RunServerWithConfig(conf)
		defer s.Shutdown()
		test(t, s, 1)
	})
}

func TestJetStreamLimits(t *testing.T) {
	test := func(t *testing.T, s *Server) {
		nc := natsConnect(t, s.ClientURL())
		defer nc.Close()

		js, err := nc.JetStream()
		require_NoError(t, err)

		si, err := js.AddStream(&nats.StreamConfig{Name: "foo"})
		require_NoError(t, err)
		require_True(t, si.Config.Duplicates == time.Minute)

		si, err = js.AddStream(&nats.StreamConfig{Name: "bar", Duplicates: 1500 * time.Millisecond})
		require_NoError(t, err)
		require_True(t, si.Config.Duplicates == 1500*time.Millisecond)

		_, err = js.UpdateStream(&nats.StreamConfig{Name: "bar", Duplicates: 2 * time.Minute})
		require_Error(t, err)
		require_Equal(t, err.Error(), "nats: duplicates window can not be larger then server limit of 1m0s")

		_, err = js.AddStream(&nats.StreamConfig{Name: "baz", Duplicates: 2 * time.Minute})
		require_Error(t, err)
		require_Equal(t, err.Error(), "nats: duplicates window can not be larger then server limit of 1m0s")

		ci, err := js.AddConsumer("foo", &nats.ConsumerConfig{Durable: "dur1", AckPolicy: nats.AckExplicitPolicy})
		require_NoError(t, err)
		require_True(t, ci.Config.MaxAckPending == 1000)
		require_True(t, ci.Config.MaxRequestBatch == 250)

		_, err = js.AddConsumer("foo", &nats.ConsumerConfig{Durable: "dur2", AckPolicy: nats.AckExplicitPolicy, MaxRequestBatch: 500})
		require_Error(t, err)
		require_Equal(t, err.Error(), "nats: consumer max request batch exceeds server limit of 250")

		ci, err = js.AddConsumer("foo", &nats.ConsumerConfig{Durable: "dur2", AckPolicy: nats.AckExplicitPolicy, MaxAckPending: 500})
		require_NoError(t, err)
		require_True(t, ci.Config.MaxAckPending == 500)
		require_True(t, ci.Config.MaxRequestBatch == 250)

		_, err = js.UpdateConsumer("foo", &nats.ConsumerConfig{Durable: "dur2", AckPolicy: nats.AckExplicitPolicy, MaxAckPending: 2000})
		require_Error(t, err)
		require_Equal(t, err.Error(), "nats: consumer max ack pending exceeds system limit of 1000")

		_, err = js.AddConsumer("foo", &nats.ConsumerConfig{Durable: "dur3", AckPolicy: nats.AckExplicitPolicy, MaxAckPending: 2000})
		require_Error(t, err)
		require_Equal(t, err.Error(), "nats: consumer max ack pending exceeds system limit of 1000")
	}

	t.Run("clustered", func(t *testing.T) {
		tmpl := `
			listen: 127.0.0.1:-1
			server_name: %s
			jetstream: {
				max_mem_store: 2MB,
				max_file_store: 8MB,
				store_dir: '%s',
				limits: {duplicate_window: "1m", max_request_batch: 250}
			}
			cluster {
				name: %s
				listen: 127.0.0.1:%d
				routes = [%s]
			}
			no_auth_user: u
			accounts {
				ONE {
					users = [ { user: "u", pass: "s3cr3t!" } ]
					jetstream: enabled
				}
				$SYS { users = [ { user: "admin", pass: "s3cr3t!" } ] }
			}`
		limitsTest := func(t *testing.T, tmpl string) {
			c := createJetStreamClusterWithTemplate(t, tmpl, "clust", 3)
			defer c.shutdown()
			s := c.randomServer()
			test(t, s)
		}
		// test with max_ack_pending being defined in operator or account
		t.Run("operator", func(t *testing.T) {
			limitsTest(t, strings.Replace(tmpl, "duplicate_window", "max_ack_pending: 1000, duplicate_window", 1))
		})
		t.Run("account", func(t *testing.T) {
			limitsTest(t, strings.Replace(tmpl, "jetstream: enabled", "jetstream: {max_ack_pending: 1000}", 1))
		})
	})

	t.Run("single", func(t *testing.T) {
		tmpl := `
			listen: 127.0.0.1:-1
			jetstream: {
				max_mem_store: 2MB,
				max_file_store: 8MB,
				store_dir: '%s',
				limits: {duplicate_window: "1m", max_request_batch: 250}
			}
			no_auth_user: u
			accounts {
				ONE {
					users = [ { user: "u", pass: "s3cr3t!" } ]
					jetstream: enabled
				}
				$SYS { users = [ { user: "admin", pass: "s3cr3t!" } ] }
			}`
		limitsTest := func(t *testing.T, tmpl string) {
			storeDir := t.TempDir()
			conf := createConfFile(t, []byte(fmt.Sprintf(tmpl, storeDir)))
			s, opts := RunServerWithConfig(conf)
			defer s.Shutdown()
			require_True(t, opts.JetStreamLimits.Duplicates == time.Minute)
			test(t, s)
		}
		// test with max_ack_pending being defined in operator or account
		t.Run("operator", func(t *testing.T) {
			limitsTest(t, strings.Replace(tmpl, "duplicate_window", "max_ack_pending: 1000, duplicate_window", 1))
		})
		t.Run("account", func(t *testing.T) {
			limitsTest(t, strings.Replace(tmpl, "jetstream: enabled", "jetstream: {max_ack_pending: 1000}", 1))
		})
	})
}

func TestJetStreamImportReload(t *testing.T) {
	storeDir := t.TempDir()

	conf := createConfFile(t, []byte(fmt.Sprintf(`
		listen: 127.0.0.1:-1
		jetstream: {max_mem_store: 2MB, max_file_store: 8MB, store_dir: '%s'}
		accounts: {
			account_a: {
				users: [{user: user_a, password: pwd}]
				exports: [{stream: news.>}]
			}
			account_b: {
				users: [{user: user_b, password: pwd}]
				jetstream: enabled
				imports: [{stream: {subject: news.>, account: account_a}}]
			}
		}`, storeDir)))
	s, _ := RunServerWithConfig(conf)
	defer s.Shutdown()

	ncA := natsConnect(t, s.ClientURL(), nats.UserInfo("user_a", "pwd"))
	defer ncA.Close()

	ncB := natsConnect(t, s.ClientURL(), nats.UserInfo("user_b", "pwd"))
	defer ncB.Close()

	jsB, err := ncB.JetStream()
	require_NoError(t, err)

	_, err = jsB.AddStream(&nats.StreamConfig{Name: "news", Subjects: []string{"news.>"}})
	require_NoError(t, err)

	require_NoError(t, ncA.Publish("news.article", nil))

	checkFor(t, time.Second, 100*time.Millisecond, func() error {
		si, err := jsB.StreamInfo("news")
		if err != nil {
			return err
		}
		if si.State.Msgs != 1 {
			return fmt.Errorf("require uint64 equal, but got: %d != 1", si.State.Msgs)
		}
		return nil
	})

	// Remove exports/imports
	reloadUpdateConfig(t, s, conf, fmt.Sprintf(`
		listen: 127.0.0.1:-1
		jetstream: {max_mem_store: 2MB, max_file_store: 8MB, store_dir: '%s'}
		accounts: {
			account_a: {
				users: [{user: user_a, password: pwd}]
			}
			account_b: {
				users: [{user: user_b, password: pwd}]
				jetstream: enabled
			}
		}`, storeDir))

	require_NoError(t, ncA.Publish("news.article", nil))
	require_NoError(t, ncA.Flush())

	si, err := jsB.StreamInfo("news")
	require_NoError(t, err)
	require_Equal(t, si.State.Msgs, 1)
}

func TestJetStreamRecoverSealedAfterServerRestart(t *testing.T) {
	s := RunBasicJetStreamServer(t)
	defer s.Shutdown()

	nc, js := jsClientConnect(t, s)
	defer nc.Close()

	_, err := js.AddStream(&nats.StreamConfig{Name: "foo"})
	require_NoError(t, err)

	for i := 0; i < 100; i++ {
		js.PublishAsync("foo", []byte("OK"))
	}
	<-js.PublishAsyncComplete()

	_, err = js.UpdateStream(&nats.StreamConfig{Name: "foo", Sealed: true})
	require_NoError(t, err)

	nc.Close()

	// Stop current
	sd := s.JetStreamConfig().StoreDir
	s.Shutdown()
	// Restart.
	s = RunJetStreamServerOnPort(-1, sd)
	defer s.Shutdown()

	nc, js = jsClientConnect(t, s)
	defer nc.Close()

	si, err := js.StreamInfo("foo")
	require_NoError(t, err)
	require_True(t, si.State.Msgs == 100)
}

func TestJetStreamImportConsumerStreamSubjectRemapSingle(t *testing.T) {
	conf := createConfFile(t, []byte(fmt.Sprintf(`
		listen: 127.0.0.1:-1
		jetstream: {max_mem_store: 4GB, max_file_store: 1TB, store_dir: %q}
		accounts: {
			JS: {
				jetstream: enabled
				users: [ {user: js, password: pwd} ]
				exports [
					# This is streaming to a delivery subject for a push based consumer.
					{ stream: "deliver.*" }
					{ stream: "foo.*" }
				]
			},
			IM: {
				users: [ {user: im, password: pwd} ]
				imports [
					{ stream:  { account: JS, subject: "deliver.ORDERS" }, to: "d.*" }
					{ stream:  { account: JS, subject: "foo.*" }, to: "bar.*" }
				]
			},
		}
	`, t.TempDir())))

	test := func(t *testing.T, queue bool) {
		s, _ := RunServerWithConfig(conf)
		defer s.Shutdown()

		nc, js := jsClientConnect(t, s, nats.UserInfo("js", "pwd"))
		defer nc.Close()

		_, err := js.AddStream(&nats.StreamConfig{
			Name:     "ORDERS",
			Subjects: []string{"foo"}, // The JS subject.
			Storage:  nats.MemoryStorage},
		)
		require_NoError(t, err)

		_, err = js.Publish("foo", []byte("OK"))
		require_NoError(t, err)

		queueName := ""
		if queue {
			queueName = "queue"
		}

		_, err = js.AddConsumer("ORDERS", &nats.ConsumerConfig{
			DeliverSubject: "deliver.ORDERS",
			AckPolicy:      nats.AckExplicitPolicy,
			DeliverGroup:   queueName,
		})
		require_NoError(t, err)

		nc2, err := nats.Connect(s.ClientURL(), nats.UserInfo("im", "pwd"))
		require_NoError(t, err)
		defer nc2.Close()

		var sub *nats.Subscription
		if queue {
			sub, err = nc2.QueueSubscribeSync("d.ORDERS", queueName)
			require_NoError(t, err)
		} else {
			sub, err = nc2.SubscribeSync("d.ORDERS")
			require_NoError(t, err)
		}

		m, err := sub.NextMsg(time.Second)
		require_NoError(t, err)

		if m.Subject != "foo" {
			t.Fatalf("Subject not mapped correctly across account boundary, expected %q got %q", "foo", m.Subject)
		}

		// Now do one that would kick in a transform.
		_, err = js.AddConsumer("ORDERS", &nats.ConsumerConfig{
			DeliverSubject: "foo.ORDERS",
			AckPolicy:      nats.AckExplicitPolicy,
			DeliverGroup:   queueName,
		})
		require_NoError(t, err)

		if queue {
			sub, err = nc2.QueueSubscribeSync("bar.ORDERS", queueName)
			require_NoError(t, err)
		} else {
			sub, err = nc2.SubscribeSync("bar.ORDERS")
			require_NoError(t, err)
		}
		m, err = sub.NextMsg(time.Second)
		require_NoError(t, err)

		if m.Subject != "foo" {
			t.Fatalf("Subject not mapped correctly across account boundary, expected %q got %q", "foo", m.Subject)
		}
	}

	t.Run("noqueue", func(t *testing.T) {
		test(t, false)
	})
	t.Run("queue", func(t *testing.T) {
		test(t, true)
	})
}

func TestJetStreamWorkQueueSourceRestart(t *testing.T) {
	s := RunBasicJetStreamServer(t)
	defer s.Shutdown()

	nc, js := jsClientConnect(t, s)
	defer nc.Close()

	sent := 10
	_, err := js.AddStream(&nats.StreamConfig{
		Name:     "FOO",
		Replicas: 1,
		Subjects: []string{"foo"},
	})
	require_NoError(t, err)

	for i := 0; i < sent; i++ {
		_, err = js.Publish("foo", nil)
		require_NoError(t, err)
	}

	_, err = js.AddStream(&nats.StreamConfig{
		Name:     "TEST",
		Replicas: 1,
		// TODO test will pass when retention commented out
		Retention: nats.WorkQueuePolicy,
		Sources:   []*nats.StreamSource{{Name: "FOO"}}})
	require_NoError(t, err)

	_, err = js.AddConsumer("TEST", &nats.ConsumerConfig{Durable: "dur", AckPolicy: nats.AckExplicitPolicy})
	require_NoError(t, err)

	sub, err := js.PullSubscribe("foo", "dur", nats.BindStream("TEST"))
	require_NoError(t, err)

	time.Sleep(1 * time.Second)

	ci, err := js.ConsumerInfo("TEST", "dur")
	require_NoError(t, err)
	require_True(t, ci.NumPending == uint64(sent))

	msgs, err := sub.Fetch(sent)
	require_NoError(t, err)
	require_True(t, len(msgs) == sent)

	for i := 0; i < sent; i++ {
		err = msgs[i].AckSync()
		require_NoError(t, err)
	}

	ci, err = js.ConsumerInfo("TEST", "dur")
	require_NoError(t, err)
	require_True(t, ci.NumPending == 0)

	si, err := js.StreamInfo("TEST")
	require_NoError(t, err)
	require_True(t, si.State.Msgs == 0)

	// Restart server
	nc.Close()
	sd := s.JetStreamConfig().StoreDir
	s.Shutdown()
	time.Sleep(200 * time.Millisecond)
	s = RunJetStreamServerOnPort(-1, sd)
	defer s.Shutdown()

	checkFor(t, 10*time.Second, 200*time.Millisecond, func() error {
		hs := s.healthz(nil)
		if hs.Status == "ok" && hs.Error == _EMPTY_ {
			return nil
		}
		return fmt.Errorf("healthz %s %s", hs.Error, hs.Status)
	})

	nc, js = jsClientConnect(t, s)
	defer nc.Close()

	si, err = js.StreamInfo("TEST")
	require_NoError(t, err)

	if si.State.Msgs != 0 {
		t.Fatalf("Expected 0 messages on restart, got %d", si.State.Msgs)
	}

	ctest, err := js.ConsumerInfo("TEST", "dur")
	require_NoError(t, err)

	//TODO (mh) I have experienced in other tests that NumPending has a value of 1 post restart.
	// seems to go awary in single server setup. It's also unrelated to work queue
	// but that error seems benign.
	if ctest.NumPending != 0 {
		t.Fatalf("Expected pending of 0 but got %d", ctest.NumPending)
	}

	sub, err = js.PullSubscribe("foo", "dur", nats.BindStream("TEST"))
	require_NoError(t, err)
	_, err = sub.Fetch(1, nats.MaxWait(time.Second))
	if err != nats.ErrTimeout {
		require_NoError(t, err)
	}
}

func TestJetStreamWorkQueueSourceNamingRestart(t *testing.T) {
	s := RunBasicJetStreamServer(t)
	defer s.Shutdown()

	nc, js := jsClientConnect(t, s)
	defer nc.Close()

	_, err := js.AddStream(&nats.StreamConfig{Name: "C1", Subjects: []string{"foo.*"}})
	require_NoError(t, err)
	_, err = js.AddStream(&nats.StreamConfig{Name: "C2", Subjects: []string{"bar.*"}})
	require_NoError(t, err)

	sendCount := 10
	for i := 0; i < sendCount; i++ {
		_, err = js.Publish(fmt.Sprintf("foo.%d", i), nil)
		require_NoError(t, err)
		_, err = js.Publish(fmt.Sprintf("bar.%d", i), nil)
		require_NoError(t, err)
	}

	// TODO Test will always pass if pending is 0
	pending := 1
	// For some yet unknown reason this failure seems to require 2 streams to source from.
	// This might possibly be timing, as the test sometimes passes
	streams := 2
	totalPending := uint64(streams * pending)
	totalMsgs := streams * sendCount
	totalNonPending := streams * (sendCount - pending)

	// TODO Test will always pass if this is named A (go returns directory names sorted)
	// A: this stream is recovered BEFORE C1/C2, tbh, I'd expect this to be the case to fail, but it isn't
	// D: this stream is recovered AFTER C1/C2, which is the case that fails (perhaps it is timing)
	srcName := "D"
	_, err = js.AddStream(&nats.StreamConfig{
		Name:      srcName,
		Retention: nats.WorkQueuePolicy,
		Sources:   []*nats.StreamSource{{Name: "C1"}, {Name: "C2"}},
	})
	require_NoError(t, err)

	// Add a consumer and consume all but totalPending messages
	_, err = js.AddConsumer(srcName, &nats.ConsumerConfig{Durable: "dur", AckPolicy: nats.AckExplicitPolicy})
	require_NoError(t, err)

	sub, err := js.PullSubscribe("", "dur", nats.BindStream(srcName))
	require_NoError(t, err)

	checkFor(t, 5*time.Second, time.Millisecond*200, func() error {
		if ci, err := js.ConsumerInfo(srcName, "dur"); err != nil {
			return err
		} else if ci.NumPending != uint64(totalMsgs) {
			return fmt.Errorf("not enough messages: %d", ci.NumPending)
		}
		return nil
	})

	// consume all but messages we want pending
	msgs, err := sub.Fetch(totalNonPending)
	require_NoError(t, err)
	require_True(t, len(msgs) == totalNonPending)

	for _, m := range msgs {
		err = m.AckSync()
		require_NoError(t, err)
	}

	ci, err := js.ConsumerInfo(srcName, "dur")
	require_NoError(t, err)
	require_True(t, ci.NumPending == totalPending)

	si, err := js.StreamInfo(srcName)
	require_NoError(t, err)
	require_True(t, si.State.Msgs == totalPending)

	// Restart server
	nc.Close()
	sd := s.JetStreamConfig().StoreDir
	s.Shutdown()
	time.Sleep(200 * time.Millisecond)
	s = RunJetStreamServerOnPort(-1, sd)
	defer s.Shutdown()

	checkFor(t, 10*time.Second, 200*time.Millisecond, func() error {
		hs := s.healthz(nil)
		if hs.Status == "ok" && hs.Error == _EMPTY_ {
			return nil
		}
		return fmt.Errorf("healthz %s %s", hs.Error, hs.Status)
	})

	nc, js = jsClientConnect(t, s)
	defer nc.Close()

	si, err = js.StreamInfo(srcName)
	require_NoError(t, err)

	if si.State.Msgs != totalPending {
		t.Fatalf("Expected 0 messages on restart, got %d", si.State.Msgs)
	}
}

func TestJetStreamDisabledHealthz(t *testing.T) {
	s := RunBasicJetStreamServer(t)
	defer s.Shutdown()

	if !s.JetStreamEnabled() {
		t.Fatalf("Expected JetStream to be enabled")
	}

	s.DisableJetStream()

	hs := s.healthz(&HealthzOptions{JSEnabledOnly: true})
	if hs.Status == "unavailable" && hs.Error == NewJSNotEnabledError().Error() {
		return
	}

	t.Fatalf("Expected healthz to return error if JetStream is disabled, got status: %s", hs.Status)
}

func TestJetStreamStreamRepublishCycle(t *testing.T) {
	s := RunBasicJetStreamServer(t)
	defer s.Shutdown()

	nc, _ := jsClientConnect(t, s)
	defer nc.Close()

	// Do by hand for now.
	cfg := &StreamConfig{
		Name:     "RPC",
		Storage:  MemoryStorage,
		Subjects: []string{"foo.>", "bar.*", "baz"},
	}

	expectFail := func() {
		t.Helper()
		req, err := json.Marshal(cfg)
		require_NoError(t, err)
		rmsg, err := nc.Request(fmt.Sprintf(JSApiStreamCreateT, cfg.Name), req, time.Second)
		require_NoError(t, err)
		var resp JSApiStreamCreateResponse
		err = json.Unmarshal(rmsg.Data, &resp)
		require_NoError(t, err)
		if resp.Type != JSApiStreamCreateResponseType {
			t.Fatalf("Invalid response type %s expected %s", resp.Type, JSApiStreamCreateResponseType)
		}
		if resp.Error == nil {
			t.Fatalf("Expected error but got none")
		}
		if !strings.Contains(resp.Error.Description, "republish destination forms a cycle") {
			t.Fatalf("Expected cycle error, got %q", resp.Error.Description)
		}
	}

	cfg.RePublish = &RePublish{
		Source:      "foo.>",
		Destination: "foo.>",
	}
	expectFail()

	cfg.RePublish = &RePublish{
		Source:      "bar.bar",
		Destination: "foo.bar",
	}
	expectFail()

	cfg.RePublish = &RePublish{
		Source:      "baz",
		Destination: "bar.bar",
	}
	expectFail()
}

func TestJetStreamStreamRepublishOneTokenMatch(t *testing.T) {
	s := RunBasicJetStreamServer(t)
	defer s.Shutdown()

	nc, js := jsClientConnect(t, s)
	defer nc.Close()

	// Do by hand for now.
	cfg := &StreamConfig{
		Name:     "Stream1",
		Storage:  MemoryStorage,
		Subjects: []string{"one", "four"},
		RePublish: &RePublish{
			Source:      "one",
			Destination: "uno",
			HeadersOnly: false,
		},
	}
	addStream(t, nc, cfg)

	sub, err := nc.SubscribeSync("uno")
	require_NoError(t, err)

	msg, toSend := bytes.Repeat([]byte("Z"), 512), 100
	for i := 0; i < toSend; i++ {
		js.PublishAsync("one", msg)
	}
	select {
	case <-js.PublishAsyncComplete():
	case <-time.After(5 * time.Second):
		t.Fatalf("Did not receive completion signal")
	}

	checkSubsPending(t, sub, toSend)
	m, err := sub.NextMsg(time.Second)
	require_NoError(t, err)

	if !(len(m.Data) > 0) {
		t.Fatalf("Expected msg data")
	}
}

func TestJetStreamStreamRepublishSuppressesInvalidSubject(t *testing.T) {
	s := RunBasicJetStreamServer(t)
	defer s.Shutdown()

	nc, js := jsClientConnect(t, s)
	defer nc.Close()

	addStream(t, nc, &StreamConfig{
		Name:     "Stream1",
		Storage:  MemoryStorage,
		Subjects: []string{"one"},
		RePublish: &RePublish{
			Source:      "one",
			Destination: "uno\r\nPUB hijack.subject 8\r\nhijacked!\r\n",
		},
	})

	hijackSub, err := nc.SubscribeSync("hijack.subject")
	require_NoError(t, err)
	repubSub, err := nc.SubscribeSync("uno")
	require_NoError(t, err)
	require_NoError(t, nc.Flush())

	_, err = js.Publish("one", []byte("payload"))
	require_NoError(t, err)

	// Should receive nothing on either the hijack or republish subjects.
	_, err = hijackSub.NextMsg(250 * time.Millisecond)
	require_Error(t, err)
	_, err = repubSub.NextMsg(250 * time.Millisecond)
	require_Error(t, err)
}

func TestJetStreamStreamRepublishLeftRightMappingFunctions(t *testing.T) {
	s := RunBasicJetStreamServer(t)
	defer s.Shutdown()

	nc, js := jsClientConnect(t, s)
	defer nc.Close()

	for i, dest := range []string{"{{left(1,3)}}", "{{right(1,3)}}"} {
		t.Run(dest, func(t *testing.T) {
			inSubj := fmt.Sprintf("in%d.*", i)
			addStream(t, nc, &StreamConfig{
				Name:     fmt.Sprintf("S%d", i),
				Storage:  MemoryStorage,
				Subjects: []string{inSubj},
				RePublish: &RePublish{
					Source:      inSubj,
					Destination: dest,
				},
			})

			sub, err := nc.SubscribeSync(">")
			require_NoError(t, err)
			require_NoError(t, nc.Flush())

			_, err = js.Publish(fmt.Sprintf("in%d.abcdef", i), []byte("x"))
			require_NoError(t, err)

			// We should receive both the original ("in.abcdef") and the republished message.
			gotRepub := false
			checkFor(t, time.Second, 10*time.Millisecond, func() error {
				msg, err := sub.NextMsg(100 * time.Millisecond)
				if err != nil {
					return err
				}
				if msg.Subject != fmt.Sprintf("in%d.abcdef", i) {
					gotRepub = true
					return nil
				}
				return fmt.Errorf("waiting for republished message")
			})
			require_True(t, gotRepub)
		})
	}
}

func TestJetStreamStreamRepublishMultiTokenMatch(t *testing.T) {
	s := RunBasicJetStreamServer(t)
	defer s.Shutdown()

	nc, js := jsClientConnect(t, s)
	defer nc.Close()

	// Do by hand for now.
	cfg := &StreamConfig{
		Name:     "Stream1",
		Storage:  MemoryStorage,
		Subjects: []string{"one.>", "four.>"},
		RePublish: &RePublish{
			Source:      "one.two.>",
			Destination: "uno.dos.>",
			HeadersOnly: false,
		},
	}
	addStream(t, nc, cfg)

	sub, err := nc.SubscribeSync("uno.dos.>")
	require_NoError(t, err)

	msg, toSend := bytes.Repeat([]byte("Z"), 512), 100
	for i := 0; i < toSend; i++ {
		js.PublishAsync("one.two.three", msg)
	}
	select {
	case <-js.PublishAsyncComplete():
	case <-time.After(5 * time.Second):
		t.Fatalf("Did not receive completion signal")
	}

	checkSubsPending(t, sub, toSend)
	m, err := sub.NextMsg(time.Second)
	require_NoError(t, err)

	if !(len(m.Data) > 0) {
		t.Fatalf("Expected msg data")
	}
}

func TestJetStreamStreamRepublishAnySubjectMatch(t *testing.T) {
	s := RunBasicJetStreamServer(t)
	defer s.Shutdown()

	nc, js := jsClientConnect(t, s)
	defer nc.Close()

	// Do by hand for now.
	cfg := &StreamConfig{
		Name:     "Stream1",
		Storage:  MemoryStorage,
		Subjects: []string{"one.>", "four.>"},
		RePublish: &RePublish{
			Destination: "uno.dos.>",
			HeadersOnly: false,
		},
	}
	addStream(t, nc, cfg)

	sub, err := nc.SubscribeSync("uno.dos.>")
	require_NoError(t, err)

	msg, toSend := bytes.Repeat([]byte("Z"), 512), 100
	for i := 0; i < toSend; i++ {
		js.PublishAsync("one.two.three", msg)
	}
	select {
	case <-js.PublishAsyncComplete():
	case <-time.After(5 * time.Second):
		t.Fatalf("Did not receive completion signal")
	}

	checkSubsPending(t, sub, toSend)
	m, err := sub.NextMsg(time.Second)
	require_NoError(t, err)

	if !(len(m.Data) > 0) {
		t.Fatalf("Expected msg data")
	}
}

func TestJetStreamStreamRepublishMultiTokenNoMatch(t *testing.T) {
	s := RunBasicJetStreamServer(t)
	defer s.Shutdown()

	nc, js := jsClientConnect(t, s)
	defer nc.Close()

	// Do by hand for now.
	cfg := &StreamConfig{
		Name:     "Stream1",
		Storage:  MemoryStorage,
		Subjects: []string{"one.>", "four.>"},
		RePublish: &RePublish{
			Source:      "one.two.>",
			Destination: "uno.dos.>",
			HeadersOnly: true,
		},
	}
	addStream(t, nc, cfg)

	sub, err := nc.SubscribeSync("uno.dos.>")
	require_NoError(t, err)

	msg, toSend := bytes.Repeat([]byte("Z"), 512), 100
	for i := 0; i < toSend; i++ {
		js.PublishAsync("four.five.six", msg)
	}
	select {
	case <-js.PublishAsyncComplete():
	case <-time.After(5 * time.Second):
		t.Fatalf("Did not receive completion signal")
	}

	checkSubsPending(t, sub, 0)
	require_NoError(t, err)
}

func TestJetStreamStreamRepublishOneTokenNoMatch(t *testing.T) {
	s := RunBasicJetStreamServer(t)
	defer s.Shutdown()

	nc, js := jsClientConnect(t, s)
	defer nc.Close()

	// Do by hand for now.
	cfg := &StreamConfig{
		Name:     "Stream1",
		Storage:  MemoryStorage,
		Subjects: []string{"one", "four"},
		RePublish: &RePublish{
			Source:      "one",
			Destination: "uno",
			HeadersOnly: true,
		},
	}
	addStream(t, nc, cfg)

	sub, err := nc.SubscribeSync("uno")
	require_NoError(t, err)

	msg, toSend := bytes.Repeat([]byte("Z"), 512), 100
	for i := 0; i < toSend; i++ {
		js.PublishAsync("four", msg)
	}
	select {
	case <-js.PublishAsyncComplete():
	case <-time.After(5 * time.Second):
		t.Fatalf("Did not receive completion signal")
	}

	checkSubsPending(t, sub, 0)
	require_NoError(t, err)
}

func TestJetStreamStreamRepublishHeadersOnly(t *testing.T) {
	s := RunBasicJetStreamServer(t)
	defer s.Shutdown()

	nc, js := jsClientConnect(t, s)
	defer nc.Close()

	// Do by hand for now.
	cfg := &StreamConfig{
		Name:     "RPC",
		Storage:  MemoryStorage,
		Subjects: []string{"foo", "bar", "baz"},
		RePublish: &RePublish{
			Destination: "RP.>",
			HeadersOnly: true,
		},
	}
	addStream(t, nc, cfg)

	sub, err := nc.SubscribeSync("RP.>")
	require_NoError(t, err)

	msg, toSend := bytes.Repeat([]byte("Z"), 512), 100
	for i := 0; i < toSend; i++ {
		js.PublishAsync("foo", msg)
	}
	select {
	case <-js.PublishAsyncComplete():
	case <-time.After(5 * time.Second):
		t.Fatalf("Did not receive completion signal")
	}

	checkSubsPending(t, sub, toSend)
	last := "0"
	for i := 0; i < toSend; i++ {
		m, err := sub.NextMsg(time.Second)
		require_NoError(t, err)

		if len(m.Data) > 0 {
			t.Fatalf("Expected no msg just headers, but got %d bytes", len(m.Data))
		}
		if sz := m.Header.Get(JSMsgSize); sz != "512" {
			t.Fatalf("Expected msg size hdr, got %q", sz)
		}
		if lastHeader := m.Header.Get(JSLastSequence); lastHeader != last {
			t.Fatalf("Expected last message header %q, got %q", last, lastHeader)
		}
		last = m.Header.Get(JSSequence)
	}
}

func TestJetStreamMsgGetNoAdvisory(t *testing.T) {
	s := RunBasicJetStreamServer(t)
	defer s.Shutdown()

	nc, js := jsClientConnect(t, s)
	defer nc.Close()

	_, err := js.AddStream(&nats.StreamConfig{Name: "foo"})
	require_NoError(t, err)

	for i := 0; i < 100; i++ {
		js.PublishAsync("foo", []byte("ok"))
	}
	select {
	case <-js.PublishAsyncComplete():
	case <-time.After(5 * time.Second):
		t.Fatalf("Did not receive completion signal")
	}

	sub, err := nc.SubscribeSync("$JS.EVENT.ADVISORY.>")
	require_NoError(t, err)

	_, err = js.GetMsg("foo", 1)
	require_NoError(t, err)

	checkSubsPending(t, sub, 0)
}

func TestJetStreamDirectMsgGet(t *testing.T) {
	s := RunBasicJetStreamServer(t)
	defer s.Shutdown()

	nc, _ := jsClientConnect(t, s)
	defer nc.Close()

	// Do by hand for now.
	cfg := &StreamConfig{
		Name:        "DSMG",
		Storage:     MemoryStorage,
		Subjects:    []string{"foo", "bar", "baz"},
		MaxMsgsPer:  1,
		AllowDirect: true,
	}
	addStream(t, nc, cfg)

	sendStreamMsg(t, nc, "foo", "foo")
	sendStreamMsg(t, nc, "bar", "bar")
	sendStreamMsg(t, nc, "baz", "baz")

	getSubj := fmt.Sprintf(JSDirectMsgGetT, "DSMG")
	getMsg := func(req *JSApiMsgGetRequest) *nats.Msg {
		var b []byte
		var err error
		if req != nil {
			b, err = json.Marshal(req)
			require_NoError(t, err)
		}
		m, err := nc.Request(getSubj, b, time.Second)
		require_NoError(t, err)
		return m
	}

	m := getMsg(&JSApiMsgGetRequest{LastFor: "foo"})
	require_True(t, string(m.Data) == "foo")
	require_True(t, m.Header.Get(JSStream) == "DSMG")
	require_True(t, m.Header.Get(JSSequence) == "1")
	require_True(t, m.Header.Get(JSSubject) == "foo")
	require_True(t, m.Subject != "foo")
	require_True(t, m.Header.Get(JSTimeStamp) != _EMPTY_)

	m = getMsg(&JSApiMsgGetRequest{LastFor: "bar"})
	require_True(t, string(m.Data) == "bar")
	require_True(t, m.Header.Get(JSStream) == "DSMG")
	require_True(t, m.Header.Get(JSSequence) == "2")
	require_True(t, m.Header.Get(JSSubject) == "bar")
	require_True(t, m.Subject != "bar")
	require_True(t, m.Header.Get(JSTimeStamp) != _EMPTY_)

	m = getMsg(&JSApiMsgGetRequest{LastFor: "baz"})
	require_True(t, string(m.Data) == "baz")
	require_True(t, m.Header.Get(JSStream) == "DSMG")
	require_True(t, m.Header.Get(JSSequence) == "3")
	require_True(t, m.Header.Get(JSSubject) == "baz")
	require_True(t, m.Subject != "baz")
	require_True(t, m.Header.Get(JSTimeStamp) != _EMPTY_)

	// Test error conditions

	// Nil request
	m = getMsg(nil)
	require_True(t, len(m.Data) == 0)
	require_True(t, m.Header.Get("Status") == "408")
	require_True(t, m.Header.Get("Description") == "Empty Request")

	// Empty request
	m = getMsg(&JSApiMsgGetRequest{})
	require_True(t, len(m.Data) == 0)
	require_True(t, m.Header.Get("Status") == "408")
	require_True(t, m.Header.Get("Description") == "Empty Request")

	// Both set
	m = getMsg(&JSApiMsgGetRequest{Seq: 1, LastFor: "foo"})
	require_True(t, len(m.Data) == 0)
	require_True(t, m.Header.Get("Status") == "408")
	require_True(t, m.Header.Get("Description") == "Bad Request")

	// Not found
	m = getMsg(&JSApiMsgGetRequest{LastFor: "foobar"})
	require_True(t, len(m.Data) == 0)
	require_True(t, m.Header.Get("Status") == "404")
	require_True(t, m.Header.Get("Description") == "Message Not Found")

	m = getMsg(&JSApiMsgGetRequest{Seq: 22})
	require_True(t, len(m.Data) == 0)
	require_True(t, m.Header.Get("Status") == "404")
	require_True(t, m.Header.Get("Description") == "Message Not Found")
}

// This allows support for a get next given a sequence as a starting.
// This allows these to be chained together if needed for sparse streams.
func TestJetStreamDirectMsgGetNext(t *testing.T) {
	s := RunBasicJetStreamServer(t)
	defer s.Shutdown()

	nc, _ := jsClientConnect(t, s)
	defer nc.Close()

	// Do by hand for now.
	cfg := &StreamConfig{
		Name:        "DSMG",
		Storage:     MemoryStorage,
		Subjects:    []string{"foo", "bar", "baz"},
		AllowDirect: true,
	}
	addStream(t, nc, cfg)

	sendStreamMsg(t, nc, "foo", "foo")
	for i := 0; i < 10; i++ {
		sendStreamMsg(t, nc, "bar", "bar")
	}
	for i := 0; i < 10; i++ {
		sendStreamMsg(t, nc, "baz", "baz")
	}
	sendStreamMsg(t, nc, "foo", "foo")

	getSubj := fmt.Sprintf(JSDirectMsgGetT, "DSMG")
	getMsg := func(seq uint64, subj string) *nats.Msg {
		req := []byte(fmt.Sprintf(`{"seq": %d, "next_by_subj": %q}`, seq, subj))
		m, err := nc.Request(getSubj, req, time.Second)
		require_NoError(t, err)
		return m
	}

	m := getMsg(0, "foo")
	require_True(t, m.Header.Get(JSSequence) == "1")
	require_True(t, m.Header.Get(JSSubject) == "foo")

	m = getMsg(1, "foo")
	require_True(t, m.Header.Get(JSSequence) == "1")
	require_True(t, m.Header.Get(JSSubject) == "foo")

	m = getMsg(2, "foo")
	require_True(t, m.Header.Get(JSSequence) == "22")
	require_True(t, m.Header.Get(JSSubject) == "foo")

	m = getMsg(2, "bar")
	require_True(t, m.Header.Get(JSSequence) == "2")
	require_True(t, m.Header.Get(JSSubject) == "bar")

	m = getMsg(5, "baz")
	require_True(t, m.Header.Get(JSSequence) == "12")
	require_True(t, m.Header.Get(JSSubject) == "baz")

	m = getMsg(14, "baz")
	require_True(t, m.Header.Get(JSSequence) == "14")
	require_True(t, m.Header.Get(JSSubject) == "baz")
}

///////////////////////////////////////////////////////////////////////////
// Simple JetStream Benchmarks
///////////////////////////////////////////////////////////////////////////

func Benchmark__JetStreamPubWithAck(b *testing.B) {
	s := RunBasicJetStreamServer(b)
	defer s.Shutdown()

	mset, err := s.GlobalAccount().addStream(&StreamConfig{Name: "foo"})
	if err != nil {
		b.Fatalf("Unexpected error adding stream: %v", err)
	}
	defer mset.delete()

	nc, err := nats.Connect(s.ClientURL())
	if err != nil {
		b.Fatalf("Failed to create client: %v", err)
	}
	defer nc.Close()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		nc.Request("foo", []byte("Hello World!"), 50*time.Millisecond)
	}
	b.StopTimer()

	state := mset.state()
	if int(state.Msgs) != b.N {
		b.Fatalf("Expected %d messages, got %d", b.N, state.Msgs)
	}
}

func Benchmark____JetStreamPubNoAck(b *testing.B) {
	s := RunBasicJetStreamServer(b)
	defer s.Shutdown()

	mset, err := s.GlobalAccount().addStream(&StreamConfig{Name: "foo"})
	if err != nil {
		b.Fatalf("Unexpected error adding stream: %v", err)
	}
	defer mset.delete()

	nc, err := nats.Connect(s.ClientURL())
	if err != nil {
		b.Fatalf("Failed to create client: %v", err)
	}
	defer nc.Close()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := nc.Publish("foo", []byte("Hello World!")); err != nil {
			b.Fatalf("Unexpected error: %v", err)
		}
	}
	nc.Flush()
	b.StopTimer()

	state := mset.state()
	if int(state.Msgs) != b.N {
		b.Fatalf("Expected %d messages, got %d", b.N, state.Msgs)
	}
}

func Benchmark_JetStreamPubAsyncAck(b *testing.B) {
	s := RunBasicJetStreamServer(b)
	defer s.Shutdown()

	mset, err := s.GlobalAccount().addStream(&StreamConfig{Name: "foo"})
	if err != nil {
		b.Fatalf("Unexpected error adding stream: %v", err)
	}
	defer mset.delete()

	nc, err := nats.Connect(s.ClientURL(), nats.NoReconnect())
	if err != nil {
		b.Fatalf("Failed to create client: %v", err)
	}
	defer nc.Close()

	// Put ack stream on its own connection.
	anc, err := nats.Connect(s.ClientURL())
	if err != nil {
		b.Fatalf("Failed to create client: %v", err)
	}
	defer anc.Close()

	acks := nats.NewInbox()
	sub, _ := anc.Subscribe(acks, func(m *nats.Msg) {
		// Just eat them for this test.
	})
	// set max pending to unlimited.
	sub.SetPendingLimits(-1, -1)
	defer sub.Unsubscribe()

	anc.Flush()
	runtime.GC()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := nc.PublishRequest("foo", acks, []byte("Hello World!")); err != nil {
			b.Fatalf("[%d] Unexpected error: %v", i, err)
		}
	}
	nc.Flush()
	b.StopTimer()

	state := mset.state()
	if int(state.Msgs) != b.N {
		b.Fatalf("Expected %d messages, got %d", b.N, state.Msgs)
	}
}

func Benchmark____JetStreamSubNoAck(b *testing.B) {
	if b.N < 10000 {
		return
	}

	s := RunBasicJetStreamServer(b)
	defer s.Shutdown()

	mname := "foo"
	mset, err := s.GlobalAccount().addStream(&StreamConfig{Name: mname})
	if err != nil {
		b.Fatalf("Unexpected error adding stream: %v", err)
	}
	defer mset.delete()

	nc, err := nats.Connect(s.ClientURL(), nats.NoReconnect())
	if err != nil {
		b.Fatalf("Failed to create client: %v", err)
	}
	defer nc.Close()

	// Queue up messages.
	for i := 0; i < b.N; i++ {
		nc.Publish(mname, []byte("Hello World!"))
	}
	nc.Flush()

	state := mset.state()
	if state.Msgs != uint64(b.N) {
		b.Fatalf("Expected %d messages, got %d", b.N, state.Msgs)
	}

	total := int32(b.N)
	received := int32(0)
	done := make(chan bool)

	deliverTo := "DM"
	oname := "O"

	nc.Subscribe(deliverTo, func(m *nats.Msg) {
		// We only are done when we receive all, we could check for gaps too.
		if atomic.AddInt32(&received, 1) >= total {
			done <- true
		}
	})
	nc.Flush()

	b.ResetTimer()
	o, err := mset.addConsumer(&ConsumerConfig{DeliverSubject: deliverTo, Durable: oname, AckPolicy: AckNone})
	if err != nil {
		b.Fatalf("Expected no error with registered interest, got %v", err)
	}
	defer o.delete()
	<-done
	b.StopTimer()
}

func benchJetStreamWorkersAndBatch(b *testing.B, numWorkers, batchSize int) {
	// Avoid running at too low of numbers since that chews up memory and GC.
	if b.N < numWorkers*batchSize {
		return
	}

	s := RunBasicJetStreamServer(b)
	defer s.Shutdown()

	mname := "MSET22"
	mset, err := s.GlobalAccount().addStream(&StreamConfig{Name: mname})
	if err != nil {
		b.Fatalf("Unexpected error adding stream: %v", err)
	}
	defer mset.delete()

	nc, err := nats.Connect(s.ClientURL(), nats.NoReconnect())
	if err != nil {
		b.Fatalf("Failed to create client: %v", err)
	}
	defer nc.Close()

	// Queue up messages.
	for i := 0; i < b.N; i++ {
		nc.Publish(mname, []byte("Hello World!"))
	}
	nc.Flush()

	state := mset.state()
	if state.Msgs != uint64(b.N) {
		b.Fatalf("Expected %d messages, got %d", b.N, state.Msgs)
	}

	// Create basic work queue mode consumer.
	oname := "WQ"
	o, err := mset.addConsumer(&ConsumerConfig{Durable: oname, AckPolicy: AckExplicit})
	if err != nil {
		b.Fatalf("Expected no error with registered interest, got %v", err)
	}
	defer o.delete()

	total := int32(b.N)
	received := int32(0)
	start := make(chan bool)
	done := make(chan bool)

	batchSizeMsg := []byte(strconv.Itoa(batchSize))
	reqNextMsgSubj := o.requestNextMsgSubject()

	for i := 0; i < numWorkers; i++ {
		nc, err := nats.Connect(s.ClientURL(), nats.NoReconnect())
		if err != nil {
			b.Fatalf("Failed to create client: %v", err)
		}
		defer nc.Close()

		deliverTo := nats.NewInbox()
		nc.Subscribe(deliverTo, func(m *nats.Msg) {
			if atomic.AddInt32(&received, 1) >= total {
				done <- true
			}
			// Ack + Next request.
			nc.PublishRequest(m.Reply, deliverTo, AckNext)
		})
		nc.Flush()
		go func() {
			<-start
			nc.PublishRequest(reqNextMsgSubj, deliverTo, batchSizeMsg)
		}()
	}

	b.ResetTimer()
	close(start)
	<-done
	b.StopTimer()
}

func Benchmark___JetStream1x1Worker(b *testing.B) {
	benchJetStreamWorkersAndBatch(b, 1, 1)
}

func Benchmark__JetStream1x1kWorker(b *testing.B) {
	benchJetStreamWorkersAndBatch(b, 1, 1024)
}

func Benchmark_JetStream10x1kWorker(b *testing.B) {
	benchJetStreamWorkersAndBatch(b, 10, 1024)
}

func Benchmark_JetStream4x512Worker(b *testing.B) {
	benchJetStreamWorkersAndBatch(b, 4, 512)
}

func TestJetStreamKVMemoryStorePerf(t *testing.T) {
	// Comment out to run, holding place for now.
	t.SkipNow()

	s := RunBasicJetStreamServer(t)
	defer s.Shutdown()

	nc, js := jsClientConnect(t, s)
	defer nc.Close()

	kv, err := js.CreateKeyValue(&nats.KeyValueConfig{Bucket: "TEST", History: 1, Storage: nats.MemoryStorage})
	require_NoError(t, err)

	start := time.Now()
	for i := 0; i < 100_000; i++ {
		_, err := kv.PutString(fmt.Sprintf("foo.%d", i), "HELLO")
		require_NoError(t, err)
	}
	fmt.Printf("Took %v for first run\n", time.Since(start))

	start = time.Now()
	for i := 0; i < 100_000; i++ {
		_, err := kv.PutString(fmt.Sprintf("foo.%d", i), "HELLO WORLD")
		require_NoError(t, err)
	}
	fmt.Printf("Took %v for second run\n", time.Since(start))

	start = time.Now()
	for i := 0; i < 100_000; i++ {
		_, err := kv.Get(fmt.Sprintf("foo.%d", i))
		require_NoError(t, err)
	}
	fmt.Printf("Took %v for get\n", time.Since(start))
}

func TestJetStreamKVMemoryStoreDirectGetPerf(t *testing.T) {
	// Comment out to run, holding place for now.
	t.SkipNow()

	s := RunBasicJetStreamServer(t)
	defer s.Shutdown()

	nc, js := jsClientConnect(t, s)
	defer nc.Close()

	cfg := &StreamConfig{
		Name:        "TEST",
		Storage:     MemoryStorage,
		Subjects:    []string{"foo.*"},
		MaxMsgsPer:  1,
		AllowDirect: true,
	}
	addStream(t, nc, cfg)

	start := time.Now()
	for i := 0; i < 100_000; i++ {
		_, err := js.Publish(fmt.Sprintf("foo.%d", i), []byte("HELLO"))
		require_NoError(t, err)
	}
	fmt.Printf("Took %v for put\n", time.Since(start))

	getSubj := fmt.Sprintf(JSDirectMsgGetT, "TEST")

	const tmpl = "{\"last_by_subj\":%q}"

	start = time.Now()
	for i := 0; i < 100_000; i++ {
		req := []byte(fmt.Sprintf(tmpl, fmt.Sprintf("foo.%d", i)))
		_, err := nc.Request(getSubj, req, time.Second)
		require_NoError(t, err)
	}
	fmt.Printf("Took %v for get\n", time.Since(start))
}

func TestJetStreamMultiplePullPerf(t *testing.T) {
	skip(t)

	s := RunBasicJetStreamServer(t)
	defer s.Shutdown()

	nc, js := jsClientConnect(t, s)
	defer nc.Close()

	js.AddStream(&nats.StreamConfig{Name: "mp22", Storage: nats.FileStorage})
	defer js.DeleteStream("mp22")

	n, msg := 1_000_000, []byte("OK")
	for i := 0; i < n; i++ {
		js.PublishAsync("mp22", msg)
	}
	select {
	case <-js.PublishAsyncComplete():
	case <-time.After(10 * time.Second):
		t.Fatalf("Did not receive completion signal")
	}

	si, err := js.StreamInfo("mp22")
	require_NoError(t, err)

	fmt.Printf("msgs: %d, total_bytes: %v\n", si.State.Msgs, friendlyBytes(int64(si.State.Bytes)))

	// 10 pull subscribers each asking for 100 msgs.
	_, err = js.AddConsumer("mp22", &nats.ConsumerConfig{
		Durable:       "d",
		MaxAckPending: 8_000,
		AckPolicy:     nats.AckExplicitPolicy,
	})
	require_NoError(t, err)

	startCh := make(chan bool)
	var wg sync.WaitGroup

	np, bs := 10, 100

	count := 0

	for i := 0; i < np; i++ {
		nc, js := jsClientConnect(t, s)
		defer nc.Close()
		sub, err := js.PullSubscribe("mp22", "d")
		require_NoError(t, err)

		wg.Add(1)
		go func(sub *nats.Subscription) {
			defer wg.Done()
			<-startCh
			for i := 0; i < n/(np*bs); i++ {
				msgs, err := sub.Fetch(bs)
				if err != nil {
					t.Logf("Got error on pull: %v", err)
					return
				}
				if len(msgs) != bs {
					t.Logf("Expected %d msgs, got %d", bs, len(msgs))
					return
				}
				count += len(msgs)
				for _, m := range msgs {
					m.Ack()
				}
			}
		}(sub)
	}

	start := time.Now()
	close(startCh)
	wg.Wait()

	tt := time.Since(start)
	fmt.Printf("Took %v to receive %d msgs [%d]\n", tt, n, count)
	fmt.Printf("%.0f msgs/s\n", float64(n)/tt.Seconds())
	fmt.Printf("%.0f mb/s\n\n", float64(si.State.Bytes/(1024*1024))/tt.Seconds())
}

func TestJetStreamMirrorUpdatesNotSupported(t *testing.T) {
	s := RunBasicJetStreamServer(t)
	defer s.Shutdown()

	nc, js := jsClientConnect(t, s)
	defer nc.Close()

	_, err := js.AddStream(&nats.StreamConfig{Name: "SOURCE"})
	require_NoError(t, err)

	cfg := &nats.StreamConfig{
		Name:   "M",
		Mirror: &nats.StreamSource{Name: "SOURCE"},
	}
	_, err = js.AddStream(cfg)
	require_NoError(t, err)

	// Promoting a mirror is supported though.
	cfg.Mirror = nil
	_, err = js.UpdateStream(cfg)
	require_NoError(t, err)
}

func TestJetStreamMirrorFirstSeqNotSupported(t *testing.T) {
	s := RunBasicJetStreamServer(t)
	defer s.Shutdown()

	_, err := s.gacc.addStream(&StreamConfig{Name: "SOURCE"})
	require_NoError(t, err)

	cfg := &StreamConfig{
		Name:     "M",
		Mirror:   &StreamSource{Name: "SOURCE"},
		FirstSeq: 123,
	}
	_, err = s.gacc.addStream(cfg)
	require_Error(t, err, NewJSMirrorWithFirstSeqError())
}

func TestJetStreamDirectGetBySubject(t *testing.T) {
	conf := createConfFile(t, []byte(fmt.Sprintf(`
		listen: 127.0.0.1:-1
		jetstream: {max_mem_store: 64GB, max_file_store: 10TB, store_dir: %q}

		ONLYME = {
			publish = { allow = "$JS.API.DIRECT.GET.KV.vid.22.>"}
		}

		accounts: {
			A: {
				jetstream: enabled
				users: [
					{ user: admin, password: s3cr3t },
					{ user: user, password: pwd, permissions: $ONLYME},
				]
			},
		}
	`, t.TempDir())))

	s, _ := RunServerWithConfig(conf)
	defer s.Shutdown()

	nc, js := jsClientConnect(t, s, nats.UserInfo("admin", "s3cr3t"))
	defer nc.Close()

	// Do by hand for now.
	cfg := &StreamConfig{
		Name:        "KV",
		Storage:     MemoryStorage,
		Subjects:    []string{"vid.*.>"},
		MaxMsgsPer:  1,
		AllowDirect: true,
	}
	addStream(t, nc, cfg)

	// Add in mirror as well.
	cfg = &StreamConfig{
		Name:         "M",
		Storage:      MemoryStorage,
		Mirror:       &StreamSource{Name: "KV"},
		MirrorDirect: true,
	}
	addStream(t, nc, cfg)

	v22 := "vid.22.speed"
	v33 := "vid.33.speed"
	_, err := js.Publish(v22, []byte("100"))
	require_NoError(t, err)
	_, err = js.Publish(v33, []byte("55"))
	require_NoError(t, err)

	// User the restricted user.
	nc, _ = jsClientConnect(t, s, nats.UserInfo("user", "pwd"))
	defer nc.Close()

	errCh := make(chan error, 10)
	nc.SetErrorHandler(func(_ *nats.Conn, _ *nats.Subscription, e error) {
		select {
		case errCh <- e:
		default:
		}
	})

	getSubj := fmt.Sprintf(JSDirectGetLastBySubjectT, "KV", v22)
	m, err := nc.Request(getSubj, nil, time.Second)
	require_NoError(t, err)
	require_True(t, string(m.Data) == "100")

	// Now attempt to access vid 33 data..
	getSubj = fmt.Sprintf(JSDirectGetLastBySubjectT, "KV", v33)
	_, err = nc.Request(getSubj, nil, 200*time.Millisecond)
	require_Error(t, err) // timeout here.

	select {
	case e := <-errCh:
		if !strings.HasPrefix(e.Error(), "nats: permissions violation") {
			t.Fatalf("Expected a permissions violation but got %v", e)
		}
	case <-time.After(time.Second):
		t.Fatalf("Expected to get a permissions error, got none")
	}

	// Now make sure mirrors are doing right thing with new way as well.
	var sawMirror bool
	getSubj = fmt.Sprintf(JSDirectGetLastBySubjectT, "KV", v22)
	for i := 0; i < 100; i++ {
		m, err := nc.Request(getSubj, nil, time.Second)
		require_NoError(t, err)
		if shdr := m.Header.Get(JSStream); shdr == "M" {
			sawMirror = true
			break
		}
	}
	if !sawMirror {
		t.Fatalf("Expected to see the mirror respond at least once")
	}
}

func TestJetStreamProperErrorDueToOverlapSubjects(t *testing.T) {
	s := RunBasicJetStreamServer(t)
	defer s.Shutdown()

	c := createJetStreamClusterExplicit(t, "R3S", 3)
	defer c.shutdown()

	test := func(t *testing.T, s *Server) {
		nc, js := jsClientConnect(t, s)
		defer nc.Close()

		_, err := js.AddStream(&nats.StreamConfig{
			Name:     "TEST",
			Subjects: []string{"foo.*"},
		})
		require_NoError(t, err)

		// Now do this by end since we want to check the error returned.
		sc := &nats.StreamConfig{
			Name:     "TEST2",
			Subjects: []string{"foo.>"},
		}
		req, _ := json.Marshal(sc)
		msg, err := nc.Request(fmt.Sprintf(JSApiStreamCreateT, sc.Name), req, time.Second)
		require_NoError(t, err)

		var scResp JSApiStreamCreateResponse
		err = json.Unmarshal(msg.Data, &scResp)
		require_NoError(t, err)

		if scResp.Error == nil || !IsNatsErr(scResp.Error, JSStreamSubjectOverlapErr) {
			t.Fatalf("Did not receive correct error: %+v", scResp)
		}
	}

	t.Run("standalone", func(t *testing.T) { test(t, s) })
	t.Run("clustered", func(t *testing.T) { test(t, c.randomServer()) })
}

func TestJetStreamServerCipherConvert(t *testing.T) {
	tmpl := `
		server_name: S22
		listen: 127.0.0.1:-1
		jetstream: {key: s3cr3t, store_dir: '%s', cipher: %s}
	`
	storeDir := t.TempDir()

	// Create a stream and a consumer under one cipher, and restart the server with a new cipher.
	conf := createConfFile(t, []byte(fmt.Sprintf(tmpl, storeDir, "AES")))

	s, _ := RunServerWithConfig(conf)
	defer s.Shutdown()

	// Client based API
	nc, js := jsClientConnect(t, s)
	defer nc.Close()

	cfg := &nats.StreamConfig{
		Name:     "TEST",
		Subjects: []string{"foo"},
	}
	if _, err := js.AddStream(cfg); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	for i := 0; i < 1000; i++ {
		msg := []byte(fmt.Sprintf("TOP SECRET DOCUMENT #%d", i+1))
		_, err := js.Publish("foo", msg)
		require_NoError(t, err)
	}

	// Make sure consumers convert as well.
	sub, err := js.PullSubscribe("foo", "dlc")
	require_NoError(t, err)
	for _, m := range fetchMsgs(t, sub, 100, 5*time.Second) {
		m.AckSync()
	}

	si, err := js.StreamInfo("TEST")
	require_NoError(t, err)

	ci, err := js.ConsumerInfo("TEST", "dlc")
	require_NoError(t, err)

	// Stop current
	s.Shutdown()

	conf = createConfFile(t, []byte(fmt.Sprintf(tmpl, storeDir, "ChaCha")))

	s, _ = RunServerWithConfig(conf)
	defer s.Shutdown()

	nc, js = jsClientConnect(t, s)
	defer nc.Close()

	si2, err := js.StreamInfo("TEST")
	require_NoError(t, err)

	if !reflect.DeepEqual(si, si2) {
		t.Fatalf("Stream infos did not match\n%+v\nvs\n%+v", si, si2)
	}

	ci2, err := js.ConsumerInfo("TEST", "dlc")
	require_NoError(t, err)

	// Consumer create times can be slightly off after restore from disk.
	now := time.Now()
	ci.Created, ci2.Created = now, now
	ci.Delivered.Last, ci2.Delivered.Last = nil, nil
	ci.AckFloor.Last, ci2.AckFloor.Last = nil, nil
	// Also clusters will be different.
	ci.Cluster, ci2.Cluster = nil, nil

	if !reflect.DeepEqual(ci, ci2) {
		t.Fatalf("Consumer infos did not match\n%+v\nvs\n%+v", ci, ci2)
	}
}

func TestJetStreamAllowDirectAfterUpdate(t *testing.T) {
	s := RunBasicJetStreamServer(t)
	defer s.Shutdown()

	nc, js := jsClientConnect(t, s)
	defer nc.Close()

	_, err := js.AddStream(&nats.StreamConfig{
		Name:     "TEST",
		Subjects: []string{"*"},
	})
	require_NoError(t, err)
	sendStreamMsg(t, nc, "foo", "msg")

	si, err := js.UpdateStream(&nats.StreamConfig{
		Name:        "TEST",
		Subjects:    []string{"*"},
		AllowDirect: true,
	})
	require_NoError(t, err)
	require_True(t, si.Config.AllowDirect)

	_, err = js.GetLastMsg("TEST", "foo", nats.DirectGet(), nats.MaxWait(100*time.Millisecond))
	require_NoError(t, err)

	// Make sure turning off works too.
	si, err = js.UpdateStream(&nats.StreamConfig{
		Name:        "TEST",
		Subjects:    []string{"*"},
		AllowDirect: false,
	})
	require_NoError(t, err)
	require_False(t, si.Config.AllowDirect)

	_, err = js.GetLastMsg("TEST", "foo", nats.DirectGet(), nats.MaxWait(100*time.Millisecond))
	require_Error(t, err)
}

func TestJetStreamSubjectBasedFilteredConsumers(t *testing.T) {
	conf := createConfFile(t, []byte(fmt.Sprintf(`
		listen: 127.0.0.1:-1
		jetstream: {max_mem_store: 64GB, max_file_store: 10TB, store_dir: %q}
		accounts: {
			A: {
				jetstream: enabled
				users: [ {
					user: u,
					password: p
					permissions {
						publish {
							allow: [
								'ID.>',
								'$JS.API.INFO',
								'$JS.API.STREAM.>',
								'$JS.API.CONSUMER.INFO.>',
								'$JS.API.CONSUMER.CREATE.TEST.VIN-xxx.ID.foo.>', # Only allow ID.foo.
							]
							deny: [ '$JS.API.CONSUMER.CREATE.*', '$JS.API.CONSUMER.DURABLE.CREATE.*.*']
						}
					}
				} ]
			},
		}
	`, t.TempDir())))

	s, _ := RunServerWithConfig(conf)
	defer s.Shutdown()

	nc, js := jsClientConnect(t, s, nats.UserInfo("u", "p"), nats.ErrorHandler(noOpErrHandler))
	defer nc.Close()

	_, err := js.AddStream(&nats.StreamConfig{
		Name:     "TEST",
		Subjects: []string{"ID.*.*"},
	})
	require_NoError(t, err)

	for i := 0; i < 100; i++ {
		js.Publish(fmt.Sprintf("ID.foo.%d", i*3), nil)
		js.Publish(fmt.Sprintf("ID.bar.%d", i*3+1), nil)
		js.Publish(fmt.Sprintf("ID.baz.%d", i*3+2), nil)
	}
	si, err := js.StreamInfo("TEST")
	require_NoError(t, err)
	require_True(t, si.State.Msgs == 300)

	// Trying to create a consumer with non filtered API should fail.
	js, err = nc.JetStream(nats.MaxWait(200 * time.Millisecond))
	require_NoError(t, err)

	_, err = js.SubscribeSync("ID.foo.*")
	require_Error(t, err, nats.ErrTimeout, context.DeadlineExceeded)

	_, err = js.SubscribeSync("ID.foo.*", nats.Durable("dlc"))
	require_Error(t, err, nats.ErrTimeout, context.DeadlineExceeded)

	// Direct filtered should work.
	// Need to do by hand for now.
	ecSubj := fmt.Sprintf(JSApiConsumerCreateExT, "TEST", "VIN-xxx", "ID.foo.*")

	crReq := CreateConsumerRequest{
		Stream: "TEST",
		Config: ConsumerConfig{
			DeliverPolicy: DeliverLast,
			FilterSubject: "ID.foo.*",
			AckPolicy:     AckExplicit,
		},
	}
	req, err := json.Marshal(crReq)
	require_NoError(t, err)

	resp, err := nc.Request(ecSubj, req, 500*time.Millisecond)
	require_NoError(t, err)
	var ccResp JSApiConsumerCreateResponse
	err = json.Unmarshal(resp.Data, &ccResp)
	require_NoError(t, err)
	if ccResp.Error != nil {
		t.Fatalf("Unexpected error: %v", ccResp.Error)
	}
	cfg := ccResp.Config
	ci := ccResp.ConsumerInfo
	// Make sure we recognized as an ephemeral (since no durable was set) and that we have an InactiveThreshold.
	// Make sure we captured preferred ephemeral name.
	if ci.Name != "VIN-xxx" {
		t.Fatalf("Did not get correct name, expected %q got %q", "xxx", ci.Name)
	}
	if cfg.InactiveThreshold == 0 {
		t.Fatalf("Expected default inactive threshold to be set, got %v", cfg.InactiveThreshold)
	}

	// Make sure we can not use different consumer name since locked above.
	ecSubj = fmt.Sprintf(JSApiConsumerCreateExT, "TEST", "VIN-zzz", "ID.foo.*")
	_, err = nc.Request(ecSubj, req, 500*time.Millisecond)
	require_Error(t, err, nats.ErrTimeout)

	// Now check that we error when we mismatch filtersubject.
	crReq = CreateConsumerRequest{
		Stream: "TEST",
		Config: ConsumerConfig{
			DeliverPolicy: DeliverLast,
			FilterSubject: "ID.bar.*",
			AckPolicy:     AckExplicit,
		},
	}
	req, err = json.Marshal(crReq)
	require_NoError(t, err)

	ecSubj = fmt.Sprintf(JSApiConsumerCreateExT, "TEST", "VIN-xxx", "ID.foo.*")
	resp, err = nc.Request(ecSubj, req, 500*time.Millisecond)
	require_NoError(t, err)
	err = json.Unmarshal(resp.Data, &ccResp)
	require_NoError(t, err)
	checkNatsError(t, ccResp.Error, JSConsumerCreateFilterSubjectMismatchErr)

	// Now make sure if we change subject to match that we can not create a filtered consumer on ID.bar.>
	ecSubj = fmt.Sprintf(JSApiConsumerCreateExT, "TEST", "VIN-xxx", "ID.bar.*")
	_, err = nc.Request(ecSubj, req, 500*time.Millisecond)
	require_Error(t, err, nats.ErrTimeout)
}

func TestJetStreamStreamSubjectsOverlap(t *testing.T) {
	s := RunBasicJetStreamServer(t)
	defer s.Shutdown()

	nc, js := jsClientConnect(t, s)
	defer nc.Close()

	_, err := js.AddStream(&nats.StreamConfig{
		Name:     "TEST",
		Subjects: []string{"foo.*", "foo.A"},
	})
	require_Error(t, err)
	require_True(t, strings.Contains(err.Error(), "overlaps"))

	_, err = js.AddStream(&nats.StreamConfig{
		Name:     "TEST",
		Subjects: []string{"foo.*"},
	})
	require_NoError(t, err)

	_, err = js.UpdateStream(&nats.StreamConfig{
		Name:     "TEST",
		Subjects: []string{"foo.*", "foo.A"},
	})
	require_Error(t, err)
	require_True(t, strings.Contains(err.Error(), "overlaps"))

	_, err = js.UpdateStream(&nats.StreamConfig{
		Name:     "TEST",
		Subjects: []string{"foo.bar.*", "foo.*.bar"},
	})
	require_Error(t, err)
	require_True(t, strings.Contains(err.Error(), "overlaps"))
}

func TestJetStreamStreamTransformOverlap(t *testing.T) {
	s := RunBasicJetStreamServer(t)
	defer s.Shutdown()

	nc, js := jsClientConnect(t, s)
	defer nc.Close()

	_, err := js.AddStream(&nats.StreamConfig{
		Name:     "TEST",
		Subjects: []string{"foo.>"},
	})
	require_NoError(t, err)

	_, err = js.AddStream(&nats.StreamConfig{
		Name: "MIRROR",
		Mirror: &nats.StreamSource{Name: "TEST",
			SubjectTransforms: []nats.SubjectTransformConfig{
				{
					Source:      "foo.*.bar",
					Destination: "baz",
				},
				{
					Source:      "foo.bar.*",
					Destination: "baz",
				},
			},
		}})
	require_Error(t, err)
	require_True(t, strings.Contains(err.Error(), "overlap"))

}

func TestJetStreamSuppressAllowDirect(t *testing.T) {
	s := RunBasicJetStreamServer(t)
	defer s.Shutdown()

	nc, js := jsClientConnect(t, s)
	defer nc.Close()

	si, err := js.AddStream(&nats.StreamConfig{
		Name:              "TEST",
		Subjects:          []string{"key.*"},
		MaxMsgsPerSubject: 1,
		AllowDirect:       true,
	})
	require_NoError(t, err)
	require_True(t, si.Config.AllowDirect)

	si, err = js.UpdateStream(&nats.StreamConfig{
		Name:              "TEST",
		Subjects:          []string{"key.*"},
		MaxMsgsPerSubject: 1,
		AllowDirect:       false,
	})
	require_NoError(t, err)
	require_False(t, si.Config.AllowDirect)

	sendStreamMsg(t, nc, "key.22", "msg")

	_, err = js.GetLastMsg("TEST", "foo", nats.DirectGet(), nats.MaxWait(100*time.Millisecond))
	require_Error(t, err)
}

func TestJetStreamAccountPurge(t *testing.T) {
	sysKp, syspub := createKey(t)
	sysJwt := encodeClaim(t, jwt.NewAccountClaims(syspub), syspub)
	sysCreds := newUser(t, sysKp)
	accKp, accpub := createKey(t)
	accClaim := jwt.NewAccountClaims(accpub)
	accClaim.Limits.JetStreamLimits.DiskStorage = 1024 * 1024 * 5
	accClaim.Limits.JetStreamLimits.MemoryStorage = 1024 * 1024 * 5
	accJwt := encodeClaim(t, accClaim, accpub)
	accCreds := newUser(t, accKp)

	storeDir := t.TempDir()

	cfg := createConfFile(t, []byte(fmt.Sprintf(`
        host: 127.0.0.1
        port:-1
        server_name: S1
        operator: %s
        system_account: %s
        resolver: {
                type: full
                dir: '%s/jwt'
        }
        jetstream: {max_mem_store: 256MB, max_file_store: 2GB, store_dir: '%s/js'}
`, ojwt, syspub, storeDir, storeDir)))
	defer os.Remove(cfg)

	s, o := RunServerWithConfig(cfg)
	updateJwt(t, s.ClientURL(), sysCreds, sysJwt, 1)
	updateJwt(t, s.ClientURL(), sysCreds, accJwt, 1)
	defer s.Shutdown()

	inspectDirs := func(t *testing.T, accTotal int) error {
		t.Helper()
		if accTotal == 0 {
			files, err := os.ReadDir(filepath.Join(o.StoreDir, "jetstream", accpub))
			require_True(t, len(files) == accTotal || err != nil)
		} else {
			files, err := os.ReadDir(filepath.Join(o.StoreDir, "jetstream", accpub, "streams"))
			require_NoError(t, err)
			require_True(t, len(files) == accTotal)
		}
		return nil
	}

	createTestData := func() {
		nc := natsConnect(t, s.ClientURL(), nats.UserCredentials(accCreds))
		defer nc.Close()
		js, err := nc.JetStream()
		require_NoError(t, err)
		_, err = js.AddStream(&nats.StreamConfig{
			Name:     "TEST1",
			Subjects: []string{"foo"},
		})
		require_NoError(t, err)
		_, err = js.AddConsumer("TEST1",
			&nats.ConsumerConfig{Durable: "DUR1",
				AckPolicy: nats.AckExplicitPolicy})
		require_NoError(t, err)
	}

	purge := func(t *testing.T) {
		t.Helper()
		var resp JSApiAccountPurgeResponse
		ncsys := natsConnect(t, s.ClientURL(), nats.UserCredentials(sysCreds))
		defer ncsys.Close()
		m, err := ncsys.Request(fmt.Sprintf(JSApiAccountPurgeT, accpub), nil, 5*time.Second)
		require_NoError(t, err)
		err = json.Unmarshal(m.Data, &resp)
		require_NoError(t, err)
		require_True(t, resp.Initiated)
	}

	createTestData()
	inspectDirs(t, 1)
	purge(t)
	inspectDirs(t, 0)
	createTestData()
	inspectDirs(t, 1)

	// A request with path separators should be rejected.
	var resp JSApiAccountPurgeResponse
	ncsys := natsConnect(t, s.ClientURL(), nats.UserCredentials(sysCreds))
	defer ncsys.Close()
	m, err := ncsys.Request(fmt.Sprintf(JSApiAccountPurgeT, "foo/hello"), nil, 5*time.Second)
	require_NoError(t, err)
	err = json.Unmarshal(m.Data, &resp)
	require_NoError(t, err)
	require_True(t, resp.Error != nil)
	require_Error(t, resp.Error, NewJSStreamGeneralError(errors.New("account name can not contain path separators")))

	s.Shutdown()
	require_NoError(t, os.Remove(storeDir+"/jwt/"+accpub+".jwt"))

	s, o = RunServerWithConfig(o.ConfigFile)
	defer s.Shutdown()
	inspectDirs(t, 1)
	purge(t)
	inspectDirs(t, 0)
}

// For issue https://github.com/nats-io/nats-server/issues/3612
// Do auto cleanup.
func TestJetStreamDanglingMessageAutoCleanup(t *testing.T) {
	s := RunBasicJetStreamServer(t)
	defer s.Shutdown()

	// Client for API requests.
	nc, js := jsClientConnect(t, s)
	defer nc.Close()

	_, err := js.AddStream(&nats.StreamConfig{
		Name:      "TEST",
		Subjects:  []string{"foo"},
		Retention: nats.InterestPolicy,
	})
	require_NoError(t, err)

	sub, err := js.PullSubscribe("foo", "dlc", nats.MaxAckPending(10))
	require_NoError(t, err)

	// Send 100 msgs
	n := 100
	for i := 0; i < n; i++ {
		sendStreamMsg(t, nc, "foo", "msg")
	}

	// Grab and ack 10 messages.
	for _, m := range fetchMsgs(t, sub, 10, time.Second) {
		m.AckSync()
	}

	ci, err := sub.ConsumerInfo()
	require_NoError(t, err)
	require_True(t, ci.AckFloor.Stream == 10)

	// Stop current
	sd := s.JetStreamConfig().StoreDir
	s.Shutdown()

	// We will hand move the ackfloor to simulate dangling message condition.
	cstore := filepath.Join(sd, "$G", "streams", "TEST", "obs", "dlc", "o.dat")

	buf, err := os.ReadFile(cstore)
	require_NoError(t, err)

	state, err := decodeConsumerState(buf)
	require_NoError(t, err)

	// Update from 10 for delivered and ack to 90.
	state.Delivered.Stream, state.Delivered.Consumer = 90, 90
	state.AckFloor.Stream, state.AckFloor.Consumer = 90, 90

	err = os.WriteFile(cstore, encodeConsumerState(state), defaultFilePerms)
	require_NoError(t, err)

	// Restart.
	s = RunJetStreamServerOnPort(-1, sd)
	defer s.Shutdown()

	nc, js = jsClientConnect(t, s)
	defer nc.Close()

	si, err := js.StreamInfo("TEST")
	require_NoError(t, err)

	if si.State.Msgs != 10 {
		t.Fatalf("Expected auto-cleanup to have worked but got %d msgs vs 10", si.State.Msgs)
	}
}

// Issue https://github.com/nats-io/nats-server/issues/3645
func TestJetStreamMsgIDHeaderCollision(t *testing.T) {
	s := RunBasicJetStreamServer(t)
	defer s.Shutdown()

	// Client for API requests.
	nc, js := jsClientConnect(t, s)
	defer nc.Close()

	_, err := js.AddStream(&nats.StreamConfig{
		Name:     "TEST",
		Subjects: []string{"ORDERS.*"},
	})
	require_NoError(t, err)

	m := nats.NewMsg("ORDERS.test")
	m.Header.Add(JSMsgId, "1")
	m.Data = []byte("ok")

	_, err = js.PublishMsg(m)
	require_NoError(t, err)

	m.Header = make(nats.Header)
	m.Header.Add("Orig-Nats-Msg-Id", "1")

	_, err = js.PublishMsg(m)
	require_NoError(t, err)

	m.Header = make(nats.Header)
	m.Header.Add("Original-Nats-Msg-Id", "1")

	_, err = js.PublishMsg(m)
	require_NoError(t, err)

	m.Header = make(nats.Header)
	m.Header.Add("Original-Nats-Msg-Id", "1")
	m.Header.Add("Really-Original-Nats-Msg-Id", "1")

	_, err = js.PublishMsg(m)
	require_NoError(t, err)

	m.Header = make(nats.Header)
	m.Header.Add("X", "Nats-Msg-Id:1")

	_, err = js.PublishMsg(m)
	require_NoError(t, err)

	si, err := js.StreamInfo("TEST")
	require_NoError(t, err)

	require_True(t, si.State.Msgs == 5)
}

// https://github.com/nats-io/nats-server/issues/3657
func TestJetStreamServerCrashOnPullConsumerDeleteWithInactiveThresholdAfterAck(t *testing.T) {
	s := RunBasicJetStreamServer(t)
	defer s.Shutdown()

	// Client for API requests.
	nc, js := jsClientConnect(t, s)
	defer nc.Close()

	_, err := js.AddStream(&nats.StreamConfig{
		Name:     "TEST",
		Subjects: []string{"foo"},
	})
	require_NoError(t, err)

	sendStreamMsg(t, nc, "foo", "msg")

	sub, err := js.PullSubscribe("foo", "dlc", nats.InactiveThreshold(10*time.Second))
	require_NoError(t, err)

	msgs := fetchMsgs(t, sub, 1, time.Second)
	require_True(t, len(msgs) == 1)
	msgs[0].Ack()
	err = js.DeleteConsumer("TEST", "dlc")
	require_NoError(t, err)

	// If server crashes this will fail.
	_, err = js.StreamInfo("TEST")
	require_NoError(t, err)
}

// createConsumer is a temporary method until nats.go client supports multiple subjects.
// it is used where lowe level call on mset is not enough, as we want to test error validation.
func createConsumer(t *testing.T, nc *nats.Conn, stream string, config ConsumerConfig) JSApiConsumerCreateResponse {
	req, err := json.Marshal(&CreateConsumerRequest{Stream: stream, Config: config})
	require_NoError(t, err)

	resp, err := nc.Request(fmt.Sprintf("$JS.API.CONSUMER.DURABLE.CREATE.%s.%s", stream, config.Durable), req, time.Second*10)
	require_NoError(t, err)

	var apiResp JSApiConsumerCreateResponse
	require_NoError(t, json.Unmarshal(resp.Data, &apiResp))

	return apiResp
}

func TestJetStreamBothFiltersSet(t *testing.T) {
	s := RunBasicJetStreamServer(t)
	if config := s.JetStreamConfig(); config != nil {
		defer removeDir(t, config.StoreDir)
	}
	defer s.Shutdown()

	nc, _ := jsClientConnect(t, s)
	defer nc.Close()
	acc := s.GlobalAccount()

	_, err := acc.addStream(&StreamConfig{
		Subjects: []string{"events.>"},
		Name:     "deliver",
	})
	require_NoError(t, err)

	resp := createConsumer(t, nc, "deliver", ConsumerConfig{
		FilterSubjects: []string{"events.one", "events.two"},
		FilterSubject:  "events.three",
		Durable:        "name",
	})
	require_True(t, resp.Error.ErrCode == 10136)
}

func TestJetStreamMultipleSubjectsPushBasic(t *testing.T) {
	s := RunBasicJetStreamServer(t)
	if config := s.JetStreamConfig(); config != nil {
		defer removeDir(t, config.StoreDir)
	}
	defer s.Shutdown()

	nc, js := jsClientConnect(t, s)
	defer nc.Close()

	mset, err := s.GlobalAccount().addStream(&StreamConfig{
		Subjects: []string{"events", "data", "other"},
		Name:     "deliver",
	})
	require_NoError(t, err)

	_, err = mset.addConsumer(&ConsumerConfig{
		FilterSubjects: []string{"events", "data"},
		Durable:        "name",
		DeliverSubject: "push",
	})
	require_NoError(t, err)

	sub, err := nc.SubscribeSync("push")
	require_NoError(t, err)

	sendStreamMsg(t, nc, "other", "10")
	sendStreamMsg(t, nc, "events", "0")
	sendStreamMsg(t, nc, "data", "1")
	sendStreamMsg(t, nc, "events", "2")
	sendStreamMsg(t, nc, "events", "3")
	sendStreamMsg(t, nc, "other", "10")
	sendStreamMsg(t, nc, "data", "4")
	sendStreamMsg(t, nc, "data", "5")

	for i := 0; i < 6; i++ {
		msg, err := sub.NextMsg(time.Second * 1)
		require_NoError(t, err)
		if fmt.Sprintf("%v", i) != string(msg.Data) {
			t.Fatalf("bad sequence. Expected %v, got %v", i, string(msg.Data))
		}
	}
	info, err := js.ConsumerInfo("deliver", "name")
	require_NoError(t, err)
	require_True(t, info.AckFloor.Consumer == 6)
	require_True(t, info.AckFloor.Stream == 8)
}
func TestJetStreamMultipleSubjectsBasic(t *testing.T) {
	s := RunBasicJetStreamServer(t)
	if config := s.JetStreamConfig(); config != nil {
		defer removeDir(t, config.StoreDir)
	}
	defer s.Shutdown()

	nc, js := jsClientConnect(t, s)
	defer nc.Close()
	acc := s.GlobalAccount()

	mset, err := acc.addStream(&StreamConfig{
		Subjects: []string{"events", "data", "other"},
		Name:     "deliver",
	})
	require_NoError(t, err)

	mset.addConsumer(&ConsumerConfig{
		FilterSubjects: []string{"events", "data"},
		Durable:        "name",
	})
	require_NoError(t, err)

	sendStreamMsg(t, nc, "other", "10")
	sendStreamMsg(t, nc, "events", "0")
	sendStreamMsg(t, nc, "data", "1")
	sendStreamMsg(t, nc, "events", "2")
	sendStreamMsg(t, nc, "events", "3")
	sendStreamMsg(t, nc, "other", "10")
	sendStreamMsg(t, nc, "data", "4")
	sendStreamMsg(t, nc, "data", "5")

	consumer, err := js.PullSubscribe("", "name", nats.Bind("deliver", "name"))
	require_NoError(t, err)

	msg, err := consumer.Fetch(6)
	require_NoError(t, err)

	for i, msg := range msg {
		if fmt.Sprintf("%v", i) != string(msg.Data) {
			t.Fatalf("bad sequence. Expected %v, got %v", i, string(msg.Data))
		}
	}
	_, err = js.ConsumerInfo("deliver", "name")
	require_NoError(t, err)
}

func TestJetStreamKVDelete(t *testing.T) {
	s := RunBasicJetStreamServer(t)
	if config := s.JetStreamConfig(); config != nil {
		defer removeDir(t, config.StoreDir)
	}
	defer s.Shutdown()

	nc, js := jsClientConnect(t, s)
	defer nc.Close()

	kv, err := js.CreateKeyValue(&nats.KeyValueConfig{
		Bucket:  "deletion",
		History: 10,
	})
	require_NoError(t, err)
	kv.Put("a", nil)
	kv.Put("a.a", nil)
	kv.Put("a.b", nil)
	kv.Put("a.b.c", nil)

	keys, err := kv.Keys()
	require_NoError(t, err)
	require_True(t, len(keys) == 4)

	info, err := js.AddConsumer("KV_deletion", &nats.ConsumerConfig{
		Name:           "keys",
		FilterSubject:  "$KV.deletion.a.*",
		DeliverPolicy:  nats.DeliverLastPerSubjectPolicy,
		DeliverSubject: "keys",
		MaxDeliver:     1,
		AckPolicy:      nats.AckNonePolicy,
		MemoryStorage:  true,
		FlowControl:    true,
		Heartbeat:      time.Second * 5,
	})
	require_NoError(t, err)
	require_True(t, info.NumPending == 2)

	sub, err := js.SubscribeSync("$KV.deletion.a.*", nats.Bind("KV_deletion", "keys"))
	require_NoError(t, err)

	_, err = sub.NextMsg(time.Second * 1)
	require_NoError(t, err)
	_, err = sub.NextMsg(time.Second * 1)
	require_NoError(t, err)
	msg, err := sub.NextMsg(time.Second * 1)
	require_True(t, msg == nil)
	require_Error(t, err)

	require_NoError(t, kv.Delete("a.a"))
	require_NoError(t, kv.Delete("a.b"))

	watcher, err := kv.WatchAll()
	require_NoError(t, err)

	updates := watcher.Updates()

	keys = []string{}
	for v := range updates {
		if v == nil {
			break
		}
		if v.Operation() == nats.KeyValueDelete {
			keys = append(keys, v.Key())
		}
	}
	require_True(t, len(keys) == 2)
}

func TestJetStreamDeliverLastPerSubjectWithKV(t *testing.T) {
	s := RunBasicJetStreamServer(t)
	if config := s.JetStreamConfig(); config != nil {
		defer removeDir(t, config.StoreDir)
	}
	defer s.Shutdown()

	nc, js := jsClientConnect(t, s)
	defer nc.Close()

	_, err := js.AddStream(&nats.StreamConfig{
		Name:              "TEST",
		MaxMsgsPerSubject: 5,
		Subjects:          []string{"kv.>"},
	})
	require_NoError(t, err)

	sendStreamMsg(t, nc, "kv.a", "bad")
	sendStreamMsg(t, nc, "kv.a", "bad")
	sendStreamMsg(t, nc, "kv.a", "bad")
	sendStreamMsg(t, nc, "kv.a", "a")
	sendStreamMsg(t, nc, "kv.a.b", "bad")
	sendStreamMsg(t, nc, "kv.a.b", "bad")
	sendStreamMsg(t, nc, "kv.a.b", "a.b")
	sendStreamMsg(t, nc, "kv.a.b.c", "bad")
	sendStreamMsg(t, nc, "kv.a.b.c", "bad")
	sendStreamMsg(t, nc, "kv.a.b.c", "bad")
	sendStreamMsg(t, nc, "kv.a.b.c", "a.b.c")

	_, err = js.AddConsumer("TEST", &nats.ConsumerConfig{
		Name:           "CONSUMER",
		FilterSubject:  "kv.>",
		DeliverPolicy:  nats.DeliverLastPerSubjectPolicy,
		DeliverSubject: "deliver",
		MaxDeliver:     1,
		AckPolicy:      nats.AckNonePolicy,
		MemoryStorage:  true,
		FlowControl:    true,
		Heartbeat:      time.Second * 5,
	})
	require_NoError(t, err)

	sub, err := js.SubscribeSync("kv.>", nats.Bind("TEST", "CONSUMER"))
	require_NoError(t, err)

	for i := 1; i <= 3; i++ {
		_, err := sub.NextMsg(time.Second * 1)
		require_NoError(t, err)
	}

	msg, err := sub.NextMsg(time.Second * 1)
	if err == nil || msg != nil {
		t.Fatalf("should not get any more messages")
	}
}

func TestJetStreamStreamUpdateSubjectsOverlapOthers(t *testing.T) {
	s := RunBasicJetStreamServer(t)
	defer s.Shutdown()

	nc, js := jsClientConnect(t, s)
	defer nc.Close()

	_, err := js.AddStream(&nats.StreamConfig{
		Name:     "TEST",
		Subjects: []string{"TEST"},
	})
	require_NoError(t, err)

	_, err = js.UpdateStream(&nats.StreamConfig{
		Name:     "TEST",
		Subjects: []string{"TEST", "foo.a"},
	})
	require_NoError(t, err)

	_, err = js.AddStream(&nats.StreamConfig{
		Name:     "TEST2",
		Subjects: []string{"TEST2"},
	})
	require_NoError(t, err)

	// we expect an error updating stream TEST2 with subject that overlaps that used by TEST
	// foo.a fails too, but foo.* also double-check for sophisticated overlap match
	_, err = js.UpdateStream(&nats.StreamConfig{
		Name:     "TEST2",
		Subjects: []string{"TEST2", "foo.*"},
	})
	require_Error(t, err)
	require_Contains(t, err.Error(), "overlap")
}

func TestJetStreamMetaDataFailOnKernelFault(t *testing.T) {
	s := RunBasicJetStreamServer(t)
	defer s.Shutdown()

	nc, js := jsClientConnect(t, s)
	defer nc.Close()

	_, err := js.AddStream(&nats.StreamConfig{
		Name:     "TEST",
		Subjects: []string{"foo"},
	})
	require_NoError(t, err)

	for i := 0; i < 10; i++ {
		sendStreamMsg(t, nc, "foo", "OK")
	}

	sd := s.JetStreamConfig().StoreDir
	sdir := filepath.Join(sd, "$G", "streams", "TEST")
	s.Shutdown()

	// Emulate if the kernel did not flush out to disk the meta information.
	// so we will zero out both meta.inf and meta.sum.
	err = os.WriteFile(filepath.Join(sdir, JetStreamMetaFile), nil, defaultFilePerms)
	require_NoError(t, err)

	err = os.WriteFile(filepath.Join(sdir, JetStreamMetaFileSum), nil, defaultFilePerms)
	require_NoError(t, err)

	// Restart.
	s = RunJetStreamServerOnPort(-1, sd)
	defer s.Shutdown()

	nc, js = jsClientConnect(t, s)
	defer nc.Close()

	// The stream will have not been recovered. So err is normal.
	_, err = js.StreamInfo("TEST")
	require_Error(t, err)

	// Make sure we are signaled here from healthz
	hs := s.healthz(nil)
	const expected = "JetStream stream '$G > TEST' could not be recovered"
	if hs.Status != "unavailable" || hs.Error == _EMPTY_ {
		t.Fatalf("Expected healthz to return an error")
	} else if hs.Error != expected {
		t.Fatalf("Expected healthz error %q got %q", expected, hs.Error)
	}

	// If we add it back, this should recover the msgs.
	_, err = js.AddStream(&nats.StreamConfig{
		Name:     "TEST",
		Subjects: []string{"foo"},
	})
	require_NoError(t, err)

	// Make sure we recovered.
	si, err := js.StreamInfo("TEST")
	require_NoError(t, err)
	require_True(t, si.State.Msgs == 10)

	// Now if we restart the server, meta should be correct,
	// and the stream should be restored.
	s.Shutdown()

	s = RunJetStreamServerOnPort(-1, sd)
	defer s.Shutdown()

	nc, js = jsClientConnect(t, s)
	defer nc.Close()

	// Make sure we recovered the stream correctly after re-adding.
	si, err = js.StreamInfo("TEST")
	require_NoError(t, err)
	require_True(t, si.State.Msgs == 10)
}

// https://github.com/nats-io/nats-server/issues/3734
func TestJetStreamMsgBlkFailOnKernelFault(t *testing.T) {
	s := RunBasicJetStreamServer(t)
	defer s.Shutdown()

	nc, js := jsClientConnect(t, s)
	defer nc.Close()

	_, err := js.AddStream(&nats.StreamConfig{
		Name:     "TEST",
		Subjects: []string{"foo"},
		MaxBytes: 10 * 1024 * 1024, // 10MB
	})
	require_NoError(t, err)

	msgSize := 1024 * 1024 // 1MB
	msg := make([]byte, msgSize)
	crand.Read(msg)

	for i := 0; i < 20; i++ {
		_, err := js.Publish("foo", msg)
		require_NoError(t, err)
	}

	si, err := js.StreamInfo("TEST")
	require_NoError(t, err)
	require_True(t, si.State.Bytes < uint64(si.Config.MaxBytes))

	// Now emulate a kernel fault that fails to write the last blk properly.
	mset, err := s.GlobalAccount().lookupStream("TEST")
	require_NoError(t, err)

	mset.mu.RLock()
	fs := mset.store.(*fileStore)
	fs.mu.RLock()
	require_True(t, len(fs.blks) > 2)
	// Here we do not grab the last one, which we handle correctly. We grab an interior one near the end.
	lmbf := fs.blks[len(fs.blks)-2].mfn
	fs.mu.RUnlock()
	mset.mu.RUnlock()

	sd := s.JetStreamConfig().StoreDir
	s.Shutdown()

	// Remove block.
	require_NoError(t, os.Remove(lmbf))

	s = RunJetStreamServerOnPort(-1, sd)
	defer s.Shutdown()

	nc, js = jsClientConnect(t, s)
	defer nc.Close()

	_, err = js.GetMsg("TEST", 17)
	require_Error(t, err, nats.ErrMsgNotFound)

	si, err = js.StreamInfo("TEST")
	require_NoError(t, err)
	require_True(t, si.State.NumDeleted == 3)

	// Test detailed version as well.
	si, err = js.StreamInfo("TEST", &nats.StreamInfoRequest{DeletedDetails: true})
	require_NoError(t, err)
	require_True(t, si.State.NumDeleted == 3)
	if !reflect.DeepEqual(si.State.Deleted, []uint64{16, 17, 18}) {
		t.Fatalf("Expected deleted of %+v, got %+v", []uint64{16, 17, 18}, si.State.Deleted)
	}

	for i := 0; i < 20; i++ {
		_, err := js.Publish("foo", msg)
		require_NoError(t, err)
	}

	si, err = js.StreamInfo("TEST")
	require_NoError(t, err)
	if si.State.Bytes > uint64(si.Config.MaxBytes) {
		t.Fatalf("MaxBytes not enforced with empty interior msg blk, max %v, bytes %v",
			friendlyBytes(si.Config.MaxBytes), friendlyBytes(int64(si.State.Bytes)))
	}
}

func TestJetStreamPurgeExAndAccounting(t *testing.T) {
	cases := []struct {
		name string
		cfg  *nats.StreamConfig
	}{
		{name: "MemoryStore",
			cfg: &nats.StreamConfig{
				Name:     "TEST",
				Storage:  nats.MemoryStorage,
				Subjects: []string{"*"},
			}},
		{name: "FileStore",
			cfg: &nats.StreamConfig{
				Name:     "TEST",
				Storage:  nats.FileStorage,
				Subjects: []string{"*"},
			}},
	}
	for _, c := range cases {
		s := RunBasicJetStreamServer(t)
		defer s.Shutdown()

		// Client for API requests.
		nc, js := jsClientConnect(t, s)
		defer nc.Close()

		_, err := js.AddStream(c.cfg)
		require_NoError(t, err)

		msg := []byte("accounting")
		for i := 0; i < 100; i++ {
			_, err = js.Publish("foo", msg)
			require_NoError(t, err)
			_, err = js.Publish("bar", msg)
			require_NoError(t, err)
		}

		info, err := js.AccountInfo()
		require_NoError(t, err)

		err = js.PurgeStream("TEST", &nats.StreamPurgeRequest{Subject: "foo"})
		require_NoError(t, err)

		ninfo, err := js.AccountInfo()
		require_NoError(t, err)

		// Make sure we did the proper accounting.
		if c.cfg.Storage == nats.MemoryStorage {
			if ninfo.Memory != info.Memory/2 {
				t.Fatalf("Accounting information incorrect for Memory: %d vs %d",
					ninfo.Memory, info.Memory/2)
			}
		} else {
			if ninfo.Store != info.Store/2 {
				t.Fatalf("Accounting information incorrect for FileStore: %d vs %d",
					ninfo.Store, info.Store/2)
			}
		}
	}
}

func TestJetStreamRollup(t *testing.T) {
	s := RunBasicJetStreamServer(t)
	defer s.Shutdown()

	nc, js := jsClientConnect(t, s)
	defer nc.Close()

	const STREAM = "S"
	const SUBJ = "S.*"

	js.AddStream(&nats.StreamConfig{
		Name:        STREAM,
		Subjects:    []string{SUBJ},
		AllowRollup: true,
	})

	for i := 1; i <= 10; i++ {
		sendStreamMsg(t, nc, "S.A", fmt.Sprintf("%v", i))
		sendStreamMsg(t, nc, "S.B", fmt.Sprintf("%v", i))
	}

	sinfo, err := js.StreamInfo(STREAM)
	require_NoError(t, err)
	require_True(t, sinfo.State.Msgs == 20)

	cinfo, err := js.AddConsumer(STREAM, &nats.ConsumerConfig{
		Durable:       "DUR-A",
		FilterSubject: "S.A",
		AckPolicy:     nats.AckExplicitPolicy,
	})
	require_NoError(t, err)
	require_True(t, cinfo.NumPending == 10)

	m := nats.NewMsg("S.A")
	m.Header.Set(JSMsgRollup, JSMsgRollupSubject)

	_, err = js.PublishMsg(m)
	require_NoError(t, err)

	cinfo, err = js.ConsumerInfo("S", "DUR-A")
	require_NoError(t, err)
	require_True(t, cinfo.NumPending == 1)

	sinfo, err = js.StreamInfo(STREAM)
	require_NoError(t, err)
	require_True(t, sinfo.State.Msgs == 11)

	cinfo, err = js.AddConsumer(STREAM, &nats.ConsumerConfig{
		Durable:       "DUR-B",
		FilterSubject: "S.B",
		AckPolicy:     nats.AckExplicitPolicy,
	})
	require_NoError(t, err)
	require_True(t, cinfo.NumPending == 10)
}

func TestJetStreamPartialPurgeWithAckPending(t *testing.T) {
	s := RunBasicJetStreamServer(t)
	defer s.Shutdown()

	nc, js := jsClientConnect(t, s)
	defer nc.Close()

	_, err := js.AddStream(&nats.StreamConfig{
		Name:     "TEST",
		Subjects: []string{"foo"},
	})
	require_NoError(t, err)

	nmsgs := 100
	for range nmsgs {
		sendStreamMsg(t, nc, "foo", "OK")
	}
	sub, err := js.PullSubscribe("foo", "dlc", nats.AckWait(time.Second))
	require_NoError(t, err)

	// Queue up all for ack pending.
	_, err = sub.Fetch(nmsgs)
	require_NoError(t, err)

	keep := nmsgs / 2
	require_NoError(t, js.PurgeStream("TEST", &nats.StreamPurgeRequest{Keep: uint64(keep)}))

	// Should be able to be redelivered now.
	time.Sleep(2 * time.Second)

	ci, err := js.ConsumerInfo("TEST", "dlc")
	require_NoError(t, err)
	// Make sure we calculated correctly.
	require_True(t, ci.AckFloor.Consumer == uint64(keep))
	require_True(t, ci.AckFloor.Stream == uint64(keep))
	require_True(t, ci.NumAckPending == keep)
	require_True(t, ci.NumPending == 0)

	for range nmsgs {
		sendStreamMsg(t, nc, "foo", "OK")
	}

	// Num pending is updated asynchronously after the publish acks.
	checkFor(t, 2*time.Second, 50*time.Millisecond, func() error {
		if ci, err = js.ConsumerInfo("TEST", "dlc"); err != nil {
			return err
		}
		if ci.NumPending != uint64(nmsgs) {
			return fmt.Errorf("expected %d pending, got %d", nmsgs, ci.NumPending)
		}
		return nil
	})
	// Make sure we calculated correctly.
	// Top 3 will be same.
	require_Equal(t, ci.AckFloor.Consumer, uint64(keep))
	require_Equal(t, ci.AckFloor.Stream, uint64(keep))
	require_Equal(t, ci.NumAckPending, keep)
	require_Equal(t, ci.NumPending, uint64(nmsgs))
	require_Equal(t, ci.NumRedelivered, 0)

	msgs, err := sub.Fetch(keep)
	require_NoError(t, err)
	require_True(t, len(msgs) == keep)

	ci, err = js.ConsumerInfo("TEST", "dlc")
	require_NoError(t, err)
	// Make sure we calculated correctly.
	require_Equal(t, ci.Delivered.Consumer, uint64(nmsgs+keep))
	require_Equal(t, ci.Delivered.Stream, uint64(nmsgs))
	require_Equal(t, ci.AckFloor.Consumer, uint64(keep))
	require_Equal(t, ci.AckFloor.Stream, uint64(keep))
	require_Equal(t, ci.NumAckPending, keep)
	require_Equal(t, ci.NumPending, uint64(nmsgs))
	require_Equal(t, ci.NumRedelivered, keep)

	// Ack all.
	for _, m := range msgs {
		require_NoError(t, m.AckSync())
	}

	ci, err = js.ConsumerInfo("TEST", "dlc")
	require_NoError(t, err)
	// Same for Delivered
	require_Equal(t, ci.Delivered.Consumer, uint64(nmsgs+keep))
	require_Equal(t, ci.Delivered.Stream, uint64(nmsgs))
	require_Equal(t, ci.AckFloor.Consumer, uint64(nmsgs+keep))
	require_Equal(t, ci.AckFloor.Stream, uint64(nmsgs))
	require_Equal(t, ci.NumAckPending, 0)
	require_Equal(t, ci.NumPending, uint64(nmsgs))
	require_Equal(t, ci.NumRedelivered, 0)

	msgs, err = sub.Fetch(nmsgs)
	require_NoError(t, err)
	require_True(t, len(msgs) == nmsgs)

	// Ack all again
	for _, m := range msgs {
		require_NoError(t, m.AckSync())
	}

	ci, err = js.ConsumerInfo("TEST", "dlc")
	require_NoError(t, err)
	// Make sure we calculated correctly.
	require_Equal(t, ci.Delivered.Consumer, uint64(nmsgs*2+keep))
	require_Equal(t, ci.Delivered.Stream, uint64(nmsgs*2))
	require_Equal(t, ci.AckFloor.Consumer, uint64(nmsgs*2+keep))
	require_Equal(t, ci.AckFloor.Stream, uint64(nmsgs*2))
	require_Equal(t, ci.NumAckPending, 0)
	require_Equal(t, ci.NumPending, 0)
	require_Equal(t, ci.NumRedelivered, 0)
}

func TestJetStreamPurgeWithRedeliveredPending(t *testing.T) {
	s := RunBasicJetStreamServer(t)
	defer s.Shutdown()

	nc, js := jsClientConnect(t, s)
	defer nc.Close()

	_, err := js.AddStream(&nats.StreamConfig{
		Name:     "TEST",
		Subjects: []string{"foo"},
	})
	require_NoError(t, err)

	nmsgs := 100
	for i := 0; i < nmsgs; i++ {
		sendStreamMsg(t, nc, "foo", "OK")
	}
	sub, err := js.PullSubscribe("foo", "dlc", nats.AckWait(time.Second))
	require_NoError(t, err)

	// Queue up all for ack pending.
	msgs, err := sub.Fetch(nmsgs)
	require_NoError(t, err)
	require_True(t, len(msgs) == nmsgs)

	// Should be able to be redelivered now.
	time.Sleep(2 * time.Second)

	// Queue up all for ack pending again.
	msgs, err = sub.Fetch(nmsgs)
	require_NoError(t, err)
	require_True(t, len(msgs) == nmsgs)

	require_NoError(t, js.PurgeStream("TEST"))

	ci, err := js.ConsumerInfo("TEST", "dlc")
	require_NoError(t, err)

	require_True(t, ci.Delivered.Consumer == uint64(2*nmsgs))
	require_True(t, ci.Delivered.Stream == uint64(nmsgs))
	require_True(t, ci.AckFloor.Consumer == uint64(2*nmsgs))
	require_True(t, ci.AckFloor.Stream == uint64(nmsgs))
	require_True(t, ci.NumAckPending == 0)
	require_True(t, ci.NumPending == 0)
	require_True(t, ci.NumRedelivered == 0)
}

func TestJetStreamStreamUpdateWithExternalSource(t *testing.T) {
	ho := DefaultTestOptions
	ho.Port = -1
	ho.LeafNode.Host = "127.0.0.1"
	ho.LeafNode.Port = -1
	ho.JetStream = true
	ho.JetStreamDomain = "hub"
	ho.StoreDir = t.TempDir()
	hs := RunServer(&ho)
	defer hs.Shutdown()

	lu, err := url.Parse(fmt.Sprintf("nats://127.0.0.1:%d", ho.LeafNode.Port))
	require_NoError(t, err)

	lo1 := DefaultTestOptions
	lo1.Port = -1
	lo1.ServerName = "a-leaf"
	lo1.JetStream = true
	lo1.StoreDir = t.TempDir()
	lo1.JetStreamDomain = "a-leaf"
	lo1.LeafNode.Remotes = []*RemoteLeafOpts{{URLs: []*url.URL{lu}}}
	l1 := RunServer(&lo1)
	defer l1.Shutdown()

	checkLeafNodeConnected(t, l1)

	// Test sources with `External` provided
	ncl, jsl := jsClientConnect(t, l1)
	defer ncl.Close()

	// Hub stream.
	_, err = jsl.AddStream(&nats.StreamConfig{Name: "stream", Subjects: []string{"leaf"}})
	require_NoError(t, err)

	nch, jsh := jsClientConnect(t, hs)
	defer nch.Close()

	// Leaf stream.
	// Both streams uses the same name, as we're testing if overlap does not check against itself
	// if `External` stream has the same name.
	_, err = jsh.AddStream(&nats.StreamConfig{
		Name:     "stream",
		Subjects: []string{"hub"},
	})
	require_NoError(t, err)

	// Add `Sources`.
	// This should not validate subjects overlap against itself.
	_, err = jsh.UpdateStream(&nats.StreamConfig{
		Name:     "stream",
		Subjects: []string{"hub"},
		Sources: []*nats.StreamSource{
			{
				Name:          "stream",
				FilterSubject: "leaf",
				External: &nats.ExternalStream{
					APIPrefix: "$JS.a-leaf.API",
				},
			},
		},
	})
	require_NoError(t, err)

	// Specifying not existing FilterSubject should also be fine, as we do not validate `External` stream.
	_, err = jsh.UpdateStream(&nats.StreamConfig{
		Name:     "stream",
		Subjects: []string{"hub"},
		Sources: []*nats.StreamSource{
			{
				Name:          "stream",
				FilterSubject: "foo",
				External: &nats.ExternalStream{
					APIPrefix: "$JS.a-leaf.API",
				},
			},
		},
	})
	require_NoError(t, err)
}

func TestJetStreamKVHistoryRegression(t *testing.T) {
	s := RunBasicJetStreamServer(t)
	defer s.Shutdown()

	nc, js := jsClientConnect(t, s)
	defer nc.Close()

	for _, storage := range []nats.StorageType{nats.FileStorage, nats.MemoryStorage} {
		t.Run(storage.String(), func(t *testing.T) {
			js.DeleteKeyValue("TEST")

			kv, err := js.CreateKeyValue(&nats.KeyValueConfig{
				Bucket:  "TEST",
				History: 4,
				Storage: storage,
			})
			require_NoError(t, err)

			r1, err := kv.Create("foo", []byte("a"))
			require_NoError(t, err)

			_, err = kv.Update("foo", []byte("ab"), r1)
			require_NoError(t, err)

			err = kv.Delete("foo")
			require_NoError(t, err)

			_, err = kv.Create("foo", []byte("abc"))
			require_NoError(t, err)

			err = kv.Delete("foo")
			require_NoError(t, err)

			history, err := kv.History("foo")
			require_NoError(t, err)
			require_True(t, len(history) == 4)

			_, err = kv.Update("foo", []byte("abcd"), history[len(history)-1].Revision())
			require_NoError(t, err)

			err = kv.Purge("foo")
			require_NoError(t, err)

			_, err = kv.Create("foo", []byte("abcde"))
			require_NoError(t, err)

			err = kv.Purge("foo")
			require_NoError(t, err)

			history, err = kv.History("foo")
			require_NoError(t, err)
			require_True(t, len(history) == 1)
		})
	}
}

func TestJetStreamSnapshotRestoreStallAndHealthz(t *testing.T) {
	s := RunBasicJetStreamServer(t)
	defer s.Shutdown()

	nc, js := jsClientConnect(t, s)
	defer nc.Close()

	_, err := js.AddStream(&nats.StreamConfig{
		Name:     "ORDERS",
		Subjects: []string{"orders.*"},
	})
	require_NoError(t, err)

	for i := 0; i < 1000; i++ {
		sendStreamMsg(t, nc, "orders.created", "new order")
	}

	hs := s.healthz(nil)
	if hs.Status != "ok" || hs.Error != _EMPTY_ {
		t.Fatalf("Expected health to be ok, got %+v", hs)
	}

	// Simulate the staging directory for restores. This is normally cleaned up
	// but since its at the root of the storage directory make sure healthz is not affected.
	snapDir := filepath.Join(s.getJetStream().config.StoreDir, snapStagingDir)
	require_NoError(t, os.MkdirAll(snapDir, defaultDirPerms))

	// Make sure healthz ok.
	hs = s.healthz(nil)
	if hs.Status != "ok" || hs.Error != _EMPTY_ {
		t.Fatalf("Expected health to be ok, got %+v", hs)
	}
}

// https://github.com/nats-io/nats-server/pull/4163
func TestJetStreamMaxBytesIgnored(t *testing.T) {
	s := RunBasicJetStreamServer(t)
	defer s.Shutdown()

	nc, js := jsClientConnect(t, s)
	defer nc.Close()

	_, err := js.AddStream(&nats.StreamConfig{
		Name:     "TEST",
		Subjects: []string{"*"},
		MaxBytes: 10 * 1024 * 1024,
	})
	require_NoError(t, err)

	msg := bytes.Repeat([]byte("A"), 1024*1024)

	for i := 0; i < 10; i++ {
		_, err := js.Publish("x", msg)
		require_NoError(t, err)
	}

	si, err := js.StreamInfo("TEST")
	require_NoError(t, err)
	require_True(t, si.State.Msgs == 9)

	// Stop current
	sd := s.JetStreamConfig().StoreDir
	s.Shutdown()

	// We will truncate blk file.
	mdir := filepath.Join(sd, "$G", "streams", "TEST", "msgs")
	// Truncate blk
	err = os.WriteFile(filepath.Join(mdir, "1.blk"), nil, defaultFilePerms)
	require_NoError(t, err)

	// Restart.
	s = RunJetStreamServerOnPort(-1, sd)
	defer s.Shutdown()

	nc, js = jsClientConnect(t, s)
	defer nc.Close()

	for i := 0; i < 10; i++ {
		_, err := js.Publish("x", msg)
		require_NoError(t, err)
	}

	si, err = js.StreamInfo("TEST")
	require_NoError(t, err)
	require_True(t, si.State.Bytes <= 10*1024*1024)
}

func TestJetStreamLastSequenceBySubjectConcurrent(t *testing.T) {
	for _, st := range []StorageType{FileStorage, MemoryStorage} {
		t.Run(st.String(), func(t *testing.T) {
			c := createJetStreamClusterExplicit(t, "JSC", 3)
			defer c.shutdown()

			nc0, js0 := jsClientConnect(t, c.randomServer())
			defer nc0.Close()

			nc1, js1 := jsClientConnect(t, c.randomServer())
			defer nc1.Close()

			cfg := StreamConfig{
				Name:     "KV",
				Subjects: []string{"kv.>"},
				Storage:  st,
				Replicas: 3,
			}

			req, err := json.Marshal(cfg)
			if err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}
			// Do manually for now.
			m, err := nc0.Request(fmt.Sprintf(JSApiStreamCreateT, cfg.Name), req, time.Second)
			require_NoError(t, err)
			si, err := js0.StreamInfo("KV")
			if err != nil {
				t.Fatalf("Unexpected error: %v, respmsg: %q", err, string(m.Data))
			}
			if si == nil || si.Config.Name != "KV" {
				t.Fatalf("StreamInfo is not correct %+v", si)
			}

			pub := func(js nats.JetStreamContext, subj, data, seq string) {
				t.Helper()
				m := nats.NewMsg(subj)
				m.Data = []byte(data)
				m.Header.Set(JSExpectedLastSubjSeq, seq)
				js.PublishMsg(m)
			}

			ready := make(chan struct{})
			wg := &sync.WaitGroup{}
			wg.Add(2)

			go func() {
				<-ready
				pub(js0, "kv.foo", "0-0", "0")
				pub(js0, "kv.foo", "0-1", "1")
				pub(js0, "kv.foo", "0-2", "2")
				wg.Done()
			}()

			go func() {
				<-ready
				pub(js1, "kv.foo", "1-0", "0")
				pub(js1, "kv.foo", "1-1", "1")
				pub(js1, "kv.foo", "1-2", "2")
				wg.Done()
			}()

			time.Sleep(50 * time.Millisecond)
			close(ready)
			wg.Wait()

			// Read the messages.
			sub, err := js0.PullSubscribe(_EMPTY_, _EMPTY_, nats.BindStream("KV"))
			require_NoError(t, err)
			msgs, err := sub.Fetch(10)
			require_NoError(t, err)
			if len(msgs) != 3 {
				t.Errorf("Expected 3 messages, got %d", len(msgs))
			}
			for i, m := range msgs {
				if m.Header.Get(JSExpectedLastSubjSeq) != fmt.Sprint(i) {
					t.Errorf("Expected %d for last sequence, got %q", i, m.Header.Get(JSExpectedLastSubjSeq))
				}
			}
		})
	}
}

func TestJetStreamServerReencryption(t *testing.T) {
	storeDir := t.TempDir()

	var i int
	for _, algo := range []struct {
		from string
		to   string
	}{
		{"aes", "aes"},
		{"aes", "chacha"},
		{"chacha", "chacha"},
		{"chacha", "aes"},
	} {
		for _, compression := range []StoreCompression{NoCompression, S2Compression} {
			t.Run(fmt.Sprintf("%s_to_%s/%s", algo.from, algo.to, compression), func(t *testing.T) {
				i++
				streamName := fmt.Sprintf("TEST_%d", i)
				subjectName := fmt.Sprintf("foo_%d", i)
				expected := 30

				checkStream := func(js nats.JetStreamContext) {
					si, err := js.StreamInfo(streamName)
					if err != nil {
						t.Fatal(err)
					}

					if si.State.Msgs != uint64(expected) {
						t.Fatalf("Should be %d messages but got %d messages", expected, si.State.Msgs)
					}

					sub, err := js.PullSubscribe(subjectName, "")
					if err != nil {
						t.Fatalf("Unexpected error: %v", err)
					}

					c := 0
					for _, m := range fetchMsgs(t, sub, expected, 5*time.Second) {
						m.AckSync()
						c++
					}
					if c != expected {
						t.Fatalf("Should have read back %d messages but got %d messages", expected, c)
					}
				}

				// First off, we start up using the original encryption key and algorithm.
				// We'll create a stream and populate it with some messages.
				t.Run("setup", func(t *testing.T) {
					conf := createConfFile(t, []byte(fmt.Sprintf(`
					server_name: S22
					listen: 127.0.0.1:-1
					jetstream: {
						key: %q,
						cipher: %s,
						store_dir: %q
					}
				`, "firstencryptionkey", algo.from, storeDir)))

					s, _ := RunServerWithConfig(conf)
					defer s.Shutdown()

					nc, js := jsClientConnect(t, s)
					defer nc.Close()

					cfg := &StreamConfig{
						Name:        streamName,
						Subjects:    []string{subjectName},
						Storage:     FileStorage,
						Compression: compression,
					}
					if _, err := jsStreamCreate(t, nc, cfg); err != nil {
						t.Fatalf("Unexpected error: %v", err)
					}

					payload := strings.Repeat("A", 512*1024)
					for i := 0; i < expected; i++ {
						if _, err := js.Publish(subjectName, []byte(payload)); err != nil {
							t.Fatalf("Unexpected publish error: %v", err)
						}
					}

					checkStream(js)
				})

				// Next up, we will restart the server, this time with both the new key
				// and algorithm and also the old key. At startup, the server will detect
				// the change in encryption key and/or algorithm and re-encrypt the stream.
				t.Run("reencrypt", func(t *testing.T) {
					conf := createConfFile(t, []byte(fmt.Sprintf(`
					server_name: S22
					listen: 127.0.0.1:-1
					jetstream: {
						key: %q,
						cipher: %s,
						prev_key: %q,
						store_dir: %q
					}
				`, "secondencryptionkey", algo.to, "firstencryptionkey", storeDir)))

					s, _ := RunServerWithConfig(conf)
					defer s.Shutdown()

					nc, js := jsClientConnect(t, s)
					defer nc.Close()

					checkStream(js)
				})

				// Finally, we'll restart the server using only the new key and algorithm.
				// At this point everything should have been re-encrypted, so we should still
				// be able to access the stream.
				t.Run("restart", func(t *testing.T) {
					conf := createConfFile(t, []byte(fmt.Sprintf(`
					server_name: S22
					listen: 127.0.0.1:-1
					jetstream: {
						key: %q,
						cipher: %s,
						store_dir: %q
					}
				`, "secondencryptionkey", algo.to, storeDir)))

					s, _ := RunServerWithConfig(conf)
					defer s.Shutdown()

					nc, js := jsClientConnect(t, s)
					defer nc.Close()

					checkStream(js)
				})
			})
		}
	}
}

func TestJetStreamLimitsToInterestPolicy(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "JSC", 3)
	defer c.shutdown()

	nc, js := jsClientConnect(t, c.leader())
	defer nc.Close()

	// This is the index of the consumer that we'll create as R1
	// instead of R3, just to prove that the switch to interest-based
	// retention also scales it up to an R3 consumer.
	singleReplica := 3

	streamCfg := nats.StreamConfig{
		Name:      "TEST",
		Subjects:  []string{"foo"},
		Retention: nats.LimitsPolicy,
		Storage:   nats.MemoryStorage,
		Replicas:  3,
	}

	stream, err := js.AddStream(&streamCfg)
	require_NoError(t, err)

	for i := 0; i < 10; i++ {
		replicas := streamCfg.Replicas
		if i == singleReplica {
			// Make one of the consumers R1 so that we can check
			// that the switch to interest-based retention is also
			// turning it into an R3 consumer.
			replicas = 1
		}
		cname := fmt.Sprintf("test_%d", i)
		_, err := js.AddConsumer("TEST", &nats.ConsumerConfig{
			Name:      cname,
			Durable:   cname,
			AckPolicy: nats.AckAllPolicy,
			Replicas:  replicas,
		})
		require_NoError(t, err)
	}

	for i := 0; i < 20; i++ {
		_, err := js.Publish("foo", []byte{1, 2, 3, 4, 5})
		require_NoError(t, err)
	}

	// Pull 10 or more messages from the stream. We will never pull
	// less than 10, which guarantees that the lowest ack floor of
	// all consumers should be 10.
	for i := 0; i < 10; i++ {
		cname := fmt.Sprintf("test_%d", i)
		count := 10 + i // At least 10 messages

		sub, err := js.PullSubscribe("foo", cname)
		require_NoError(t, err)

		msgs, err := sub.Fetch(count)
		require_NoError(t, err)
		require_Equal(t, len(msgs), count)
		require_NoError(t, msgs[len(msgs)-1].AckSync())

		// At this point the ack floor should match the count of
		// messages we received.
		info, err := js.ConsumerInfo("TEST", cname)
		require_NoError(t, err)
		require_Equal(t, info.AckFloor.Consumer, uint64(count))
	}

	// Try updating to interest-based. Even though we have a consumer
	// that is R1 on an R3 stream, this should succeed: the consumer
	// should be scaled up to match the stream instead of the update
	// being rejected.
	streamCfg = stream.Config
	streamCfg.Retention = nats.InterestPolicy
	_, err = js.UpdateStream(&streamCfg)
	require_NoError(t, err)

	// We need to wait for all nodes to have applied the new configs.
	c.waitOnStreamLeader(globalAccountName, "TEST")
	c.waitOnAllCurrent()

	// The previously R1 consumer should now be R3, matching the stream.
	cname := fmt.Sprintf("test_%d", singleReplica)
	c.waitOnConsumerLeader(globalAccountName, "TEST", cname)
	cinfo, err := js.ConsumerInfo("TEST", cname)
	require_NoError(t, err)
	require_Equal(t, cinfo.Config.Replicas, streamCfg.Replicas)
	require_Equal(t, len(cinfo.Cluster.Replicas), streamCfg.Replicas-1)

	// Now we should only have 10 messages left in the stream, as
	// each consumer has acked at least the first 10 messages.
	info, err := js.StreamInfo("TEST")
	require_NoError(t, err)
	require_Equal(t, info.State.FirstSeq, 11)
	require_Equal(t, info.State.Msgs, 10)
}

func TestJetStreamLimitsToInterestPolicyWhileAcking(t *testing.T) {
	for _, st := range []nats.StorageType{nats.FileStorage, nats.MemoryStorage} {
		t.Run(st.String(), func(t *testing.T) {
			c := createJetStreamClusterExplicit(t, "JSC", 3)
			defer c.shutdown()

			nc, js := jsClientConnect(t, c.leader())
			defer nc.Close()
			streamCfg := nats.StreamConfig{
				Name:      "TEST",
				Subjects:  []string{"foo"},
				Retention: nats.LimitsPolicy,
				Storage:   st,
				Replicas:  3,
			}

			stream, err := js.AddStream(&streamCfg)
			require_NoError(t, err)

			wg := sync.WaitGroup{}
			ctx, cancel := context.WithCancel(context.Background())
			payload := []byte(strings.Repeat("A", 128))

			wg.Add(1)
			go func() {
				defer wg.Done()
				for range time.NewTicker(10 * time.Millisecond).C {
					select {
					case <-ctx.Done():
						return
					default:
					}
					js.Publish("foo", payload)
				}
			}()
			for i := 0; i < 5; i++ {
				cname := fmt.Sprintf("test_%d", i)
				sub, err := js.PullSubscribe("foo", cname)
				require_NoError(t, err)

				wg.Add(1)
				go func() {
					defer wg.Done()
					for range time.NewTicker(10 * time.Millisecond).C {
						select {
						case <-ctx.Done():
							return
						default:
						}

						msgs, err := sub.Fetch(1)
						if err != nil && errors.Is(err, nats.ErrTimeout) {
							t.Logf("ERROR: %v", err)
						}
						for _, msg := range msgs {
							msg.Ack()
						}
					}
				}()
			}
			// Leave running for a few secs then do the change on a different connection.
			time.Sleep(5 * time.Second)
			nc2, js2 := jsClientConnect(t, c.leader())
			defer nc2.Close()

			// Try updating to interest-based and changing replicas too.
			streamCfg = stream.Config
			streamCfg.Retention = nats.InterestPolicy
			_, err = js2.UpdateStream(&streamCfg)
			require_NoError(t, err)

			// We need to wait for all nodes to have applied the new stream
			// configuration.
			c.waitOnStreamLeader(globalAccountName, "TEST")
			c.waitOnAllCurrent()

			var retention nats.RetentionPolicy
			checkFor(t, 15*time.Second, 500*time.Millisecond, func() error {
				info, err := js2.StreamInfo("TEST", nats.MaxWait(500*time.Millisecond))
				if err != nil {
					return err
				}
				retention = info.Config.Retention
				return nil
			})
			require_Equal(t, retention, nats.InterestPolicy)

			// Cancel and wait for goroutines underneath.
			cancel()
			wg.Wait()
		})
	}
}

func TestJetStreamUsageSyncDeadlock(t *testing.T) {
	s := RunBasicJetStreamServer(t)
	defer s.Shutdown()

	nc, js := jsClientConnect(t, s)
	defer nc.Close()

	_, err := js.AddStream(&nats.StreamConfig{
		Name:     "TEST",
		Subjects: []string{"*"},
	})
	require_NoError(t, err)

	sendStreamMsg(t, nc, "foo", "hello")

	// Now purposely mess up the usage that will force a sync.
	// Without the fix this will deadlock.
	jsa := s.getJetStream().lookupAccount(s.GlobalAccount())
	jsa.usageMu.Lock()
	st, ok := jsa.usage[_EMPTY_]
	require_True(t, ok)
	st.local.store = -1000
	jsa.usageMu.Unlock()

	sendStreamMsg(t, nc, "foo", "hello")
}

// https://github.com/nats-io/nats.go/issues/1382
// https://github.com/nats-io/nats-server/issues/4445
func TestJetStreamChangeMaxMessagesPerSubject(t *testing.T) {
	s := RunBasicJetStreamServer(t)
	defer s.Shutdown()

	nc, js := jsClientConnect(t, s)
	defer nc.Close()

	_, err := js.AddStream(&nats.StreamConfig{
		Name:              "TEST",
		Subjects:          []string{"one.>"},
		MaxMsgsPerSubject: 5,
	})
	require_NoError(t, err)

	for i := 0; i < 10; i++ {
		sendStreamMsg(t, nc, "one.data", "data")
	}

	expectMsgs := func(num int32) error {
		t.Helper()

		var msgs atomic.Int32
		sub, err := js.Subscribe("one.>", func(msg *nats.Msg) {
			msgs.Add(1)
			msg.Ack()
		})
		require_NoError(t, err)
		defer sub.Unsubscribe()

		checkFor(t, 5*time.Second, 100*time.Millisecond, func() error {
			if nm := msgs.Load(); nm != num {
				return fmt.Errorf("expected to get %v messages, got %v instead", num, nm)
			}
			return nil
		})
		return nil
	}

	require_NoError(t, expectMsgs(5))

	js.UpdateStream(&nats.StreamConfig{
		Name:              "TEST",
		Subjects:          []string{"one.>"},
		MaxMsgsPerSubject: 3,
	})

	info, err := js.StreamInfo("TEST")
	require_NoError(t, err)
	require_True(t, info.Config.MaxMsgsPerSubject == 3)
	require_True(t, info.State.Msgs == 3)

	require_NoError(t, expectMsgs(3))

	for i := 0; i < 10; i++ {
		sendStreamMsg(t, nc, "one.data", "data")
	}

	require_NoError(t, expectMsgs(3))
}

func TestJetStreamSyncInterval(t *testing.T) {
	sd := t.TempDir()
	tmpl := `
		listen: 127.0.0.1:-1
		jetstream: {
			store_dir: %q
			%s
		}`

	for _, test := range []struct {
		name     string
		sync     string
		expected time.Duration
		always   bool
	}{
		{"Default", _EMPTY_, defaultSyncInterval, false},
		{"10s", "sync_interval: 10s", time.Duration(10 * time.Second), false},
		{"Always", "sync_interval: always", defaultSyncInterval, true},
	} {
		t.Run(test.name, func(t *testing.T) {
			conf := createConfFile(t, []byte(fmt.Sprintf(tmpl, sd, test.sync)))
			s, _ := RunServerWithConfig(conf)
			defer s.Shutdown()

			opts := s.getOpts()
			require_True(t, opts.SyncInterval == test.expected)

			nc, js := jsClientConnect(t, s)
			defer nc.Close()

			_, err := js.AddStream(&nats.StreamConfig{
				Name:     "TEST",
				Subjects: []string{"test.>"},
			})
			require_NoError(t, err)

			mset, err := s.GlobalAccount().lookupStream("TEST")
			require_NoError(t, err)
			fs := mset.store.(*fileStore)
			fs.mu.RLock()
			fsSync := fs.fcfg.SyncInterval
			syncAlways := fs.fcfg.SyncAlways
			fs.mu.RUnlock()
			require_True(t, fsSync == test.expected)
			require_True(t, syncAlways == test.always)
		})
	}
}

func TestJetStreamFilteredSubjectUsesNewConsumerCreateSubject(t *testing.T) {
	s := RunBasicJetStreamServer(t)
	defer s.Shutdown()

	nc, _ := jsClientConnect(t, s)
	defer nc.Close()

	extEndpoint := make(chan *nats.Msg, 1)
	normalEndpoint := make(chan *nats.Msg, 1)

	_, err := nc.ChanSubscribe(JSApiConsumerCreateEx, extEndpoint)
	require_NoError(t, err)

	_, err = nc.ChanSubscribe(JSApiConsumerCreate, normalEndpoint)
	require_NoError(t, err)

	testStreamSource := func(name string, shouldBeExtended bool, ss StreamSource) {
		t.Run(name, func(t *testing.T) {
			req := StreamConfig{
				Name:     name,
				Storage:  MemoryStorage,
				Subjects: []string{fmt.Sprintf("foo.%s", name)},
				Sources:  []*StreamSource{&ss},
			}
			reqJson, err := json.Marshal(req)
			require_NoError(t, err)

			_, err = nc.Request(fmt.Sprintf(JSApiStreamCreateT, name), reqJson, time.Second)
			require_NoError(t, err)

			select {
			case <-time.After(time.Second * 5):
				t.Fatalf("Timed out waiting for receive consumer create")
			case <-extEndpoint:
				if !shouldBeExtended {
					t.Fatalf("Expected normal consumer create, got extended")
				}
			case <-normalEndpoint:
				if shouldBeExtended {
					t.Fatalf("Expected extended consumer create, got normal")
				}
			}
		})
	}

	testStreamSource("OneFilterSubject", true, StreamSource{
		Name:          "source",
		FilterSubject: "bar.>",
	})

	testStreamSource("OneTransform", true, StreamSource{
		Name: "source",
		SubjectTransforms: []SubjectTransformConfig{
			{
				Source:      "bar.one.>",
				Destination: "bar.two.>",
			},
		},
	})

	testStreamSource("TwoTransforms", false, StreamSource{
		Name: "source",
		SubjectTransforms: []SubjectTransformConfig{
			{
				Source:      "bar.one.>",
				Destination: "bar.two.>",
			},
			{
				Source:      "baz.one.>",
				Destination: "baz.two.>",
			},
		},
	})
}

// Make sure when we downgrade history to a smaller number that the account info
// is properly updated and all keys are still accessible.
// There was a bug calculating next first that was not taking into account the dbit slots.
func TestJetStreamKVReductionInHistory(t *testing.T) {
	s := RunBasicJetStreamServer(t)
	defer s.Shutdown()

	nc, js := jsClientConnect(t, s)
	defer nc.Close()

	startHistory := 4
	kv, err := js.CreateKeyValue(&nats.KeyValueConfig{Bucket: "TEST", History: uint8(startHistory)})
	require_NoError(t, err)

	numKeys, msg := 1000, bytes.Repeat([]byte("ABC"), 330) // ~1000bytes
	for {
		key := fmt.Sprintf("%X", rand.IntN(numKeys)+1)
		_, err = kv.Put(key, msg)
		require_NoError(t, err)
		status, err := kv.Status()
		require_NoError(t, err)
		if status.Values() >= uint64(startHistory*numKeys) {
			break
		}
	}
	info, err := js.AccountInfo()
	require_NoError(t, err)

	checkAllKeys := func() {
		t.Helper()
		// Make sure we can retrieve all of the keys.
		keys, err := kv.Keys()
		require_NoError(t, err)
		require_Equal(t, len(keys), numKeys)
		for _, key := range keys {
			_, err := kv.Get(key)
			require_NoError(t, err)
		}
	}

	// Quick sanity check.
	checkAllKeys()

	si, err := js.StreamInfo("KV_TEST")
	require_NoError(t, err)
	// Adjust down to history of 1.
	cfg := si.Config
	cfg.MaxMsgsPerSubject = 1
	_, err = js.UpdateStream(&cfg)
	require_NoError(t, err)
	// Make sure the accounting was updated.
	ninfo, err := js.AccountInfo()
	require_NoError(t, err)
	require_True(t, info.Store > ninfo.Store)

	// Make sure all keys still accessible.
	checkAllKeys()
}

func TestJetStreamDirectGetBatch(t *testing.T) {
	s := RunBasicJetStreamServer(t)
	defer s.Shutdown()

	nc, js := jsClientConnect(t, s)
	defer nc.Close()

	_, err := js.AddStream(&nats.StreamConfig{
		Name:     "TEST",
		Subjects: []string{"foo.*"},
	})
	require_NoError(t, err)

	// Add in messages
	for i := 0; i < 333; i++ {
		js.PublishAsync("foo.foo", []byte("HELLO"))
		js.PublishAsync("foo.bar", []byte("WORLD"))
		js.PublishAsync("foo.baz", []byte("AGAIN"))
	}
	select {
	case <-js.PublishAsyncComplete():
	case <-time.After(5 * time.Second):
		t.Fatalf("Did not receive completion signal")
	}

	// DirectGet is required for batch. Make sure we error correctly if not enabled.
	mreq := &JSApiMsgGetRequest{Seq: 1, Batch: 10}
	req, _ := json.Marshal(mreq)
	rr, err := nc.Request("$JS.API.STREAM.MSG.GET.TEST", req, time.Second)
	require_NoError(t, err)
	var resp JSApiMsgGetResponse
	json.Unmarshal(rr.Data, &resp)
	require_True(t, resp.Error != nil)
	require_Equal(t, resp.Error.Code, NewJSBadRequestError().Code)

	// Update stream to support direct.
	_, err = js.UpdateStream(&nats.StreamConfig{
		Name:        "TEST",
		Subjects:    []string{"foo.*"},
		AllowDirect: true,
	})
	require_NoError(t, err)

	// Direct subjects.
	sendRequest := func(mreq *JSApiMsgGetRequest) *nats.Subscription {
		t.Helper()
		req, _ := json.Marshal(mreq)
		// We will get multiple responses so can't do normal request.
		reply := nats.NewInbox()
		sub, err := nc.SubscribeSync(reply)
		require_NoError(t, err)
		err = nc.PublishRequest("$JS.API.DIRECT.GET.TEST", reply, req)
		require_NoError(t, err)
		return sub
	}

	// Batch sizes greater than 1 will have a nil message as the end marker.
	checkResponses := func(sub *nats.Subscription, numPendingStart int, expected ...string) {
		t.Helper()
		defer sub.Unsubscribe()
		checkSubsPending(t, sub, len(expected))
		np := numPendingStart
		last := "0"
		for i := 0; i < len(expected); i++ {
			msg, err := sub.NextMsg(10 * time.Millisecond)
			require_NoError(t, err)
			// If expected is _EMPTY_ that signals we expect a EOB marker.
			if subj := expected[i]; subj != _EMPTY_ {
				// Make sure subject is correct.
				require_Equal(t, expected[i], msg.Header.Get(JSSubject))
				// Should have Data field non-zero
				require_True(t, len(msg.Data) > 0)
				// Check we have NumPending and it's correct.
				if np > 0 {
					np--
				}
				require_Equal(t, strconv.Itoa(np), msg.Header.Get(JSNumPending))
				require_Equal(t, last, msg.Header.Get(JSLastSequence))
				last = msg.Header.Get(JSSequence)
			} else {
				// Check for properly formatted EOB marker.
				// Should have no body.
				require_Equal(t, len(msg.Data), 0)
				// We mark status as 204 - No Content
				require_Equal(t, msg.Header.Get("Status"), "204")
				// Check description is EOB
				require_Equal(t, msg.Header.Get("Description"), "EOB")
				// Check we have NumPending and it's correct.
				require_Equal(t, strconv.Itoa(np), msg.Header.Get(JSNumPending))
				require_Equal(t, last, msg.Header.Get(JSLastSequence))
			}
		}
	}

	// Run some simple tests.
	sub := sendRequest(&JSApiMsgGetRequest{Seq: 1, Batch: 2})
	checkResponses(sub, 999, "foo.foo", "foo.bar", _EMPTY_)

	sub = sendRequest(&JSApiMsgGetRequest{Seq: 1, Batch: 3})
	checkResponses(sub, 999, "foo.foo", "foo.bar", "foo.baz", _EMPTY_)

	// Test NextFor works
	sub = sendRequest(&JSApiMsgGetRequest{Seq: 1, Batch: 3, NextFor: "foo.*"})
	checkResponses(sub, 999, "foo.foo", "foo.bar", "foo.baz", _EMPTY_)

	sub = sendRequest(&JSApiMsgGetRequest{Seq: 1, Batch: 3, NextFor: "foo.baz"})
	checkResponses(sub, 333, "foo.baz", "foo.baz", "foo.baz", _EMPTY_)

	// Test stopping early by starting at 997 with only 3 messages.
	sub = sendRequest(&JSApiMsgGetRequest{Seq: 997, Batch: 10, NextFor: "foo.*"})
	checkResponses(sub, 3, "foo.foo", "foo.bar", "foo.baz", _EMPTY_)

	err = js.PurgeStream("TEST")
	require_NoError(t, err)

	// Add in messages
	js.PublishAsync("foo.foo", []byte("HELLO"))
	js.PublishAsync("foo.bar", []byte("WORLD"))
	js.PublishAsync("foo.baz", []byte("AGAIN"))

	select {
	case <-js.PublishAsyncComplete():
	case <-time.After(5 * time.Second):
		t.Fatalf("Did not receive completion signal")
	}

	// Verify that a batch get request starting at a deleted sequence number skips over the deleted messages.
	sub = sendRequest(&JSApiMsgGetRequest{Seq: 1, Batch: 10})
	checkResponses(sub, 3, "foo.foo", "foo.bar", "foo.baz", _EMPTY_)

	// verify that a non-batch direct get to a deleted message returns not found.
	_, err = js.GetMsg("TEST", 1, nats.DirectGet())
	require_Error(t, err, fmt.Errorf("nats: message not found"))

}

func TestJetStreamDirectGetBatchMaxBytes(t *testing.T) {
	s := RunBasicJetStreamServer(t)
	defer s.Shutdown()

	nc, js := jsClientConnect(t, s)
	defer nc.Close()

	_, err := js.AddStream(&nats.StreamConfig{
		Name:        "TEST",
		Subjects:    []string{"foo.*"},
		AllowDirect: true,
		Compression: nats.S2Compression,
	})
	require_NoError(t, err)

	msg := bytes.Repeat([]byte("Z"), 512*1024)
	// Add in messages
	for i := 0; i < 333; i++ {
		js.PublishAsync("foo.foo", msg)
		js.PublishAsync("foo.bar", msg)
		js.PublishAsync("foo.baz", msg)
	}
	select {
	case <-js.PublishAsyncComplete():
	case <-time.After(5 * time.Second):
		t.Fatalf("Did not receive completion signal")
	}

	sendRequestAndCheck := func(mreq *JSApiMsgGetRequest, numExpected int) {
		t.Helper()
		req, _ := json.Marshal(mreq)
		// We will get multiple responses so can't do normal request.
		reply := nats.NewInbox()
		sub, err := nc.SubscribeSync(reply)
		require_NoError(t, err)
		defer sub.Unsubscribe()
		err = nc.PublishRequest("$JS.API.DIRECT.GET.TEST", reply, req)
		require_NoError(t, err)
		// Make sure we get correct number of responses.
		checkSubsPending(t, sub, numExpected)
	}

	// Total msg size being sent back to us.
	msgSize := len(msg) + len("foo.foo")
	// We should get 1 msg and 1 EOB
	sendRequestAndCheck(&JSApiMsgGetRequest{Seq: 1, Batch: 3, MaxBytes: msgSize}, 2)

	// Test NextFor tracks as well.
	sendRequestAndCheck(&JSApiMsgGetRequest{Seq: 1, NextFor: "foo.bar", Batch: 3, MaxBytes: 2 * msgSize}, 3)

	// Now test no MaxBytes to inherit server max_num_pending.
	expected := (int(s.getOpts().MaxPending) / msgSize) + 1
	sendRequestAndCheck(&JSApiMsgGetRequest{Seq: 1, Batch: 200}, expected+1)
}

func TestJetStreamMsgGetAsOfTime(t *testing.T) {
	s := RunBasicJetStreamServer(t)
	defer s.Shutdown()

	nc, js := jsClientConnect(t, s)
	defer nc.Close()

	_, err := js.AddStream(&nats.StreamConfig{
		Name:        "TEST",
		Subjects:    []string{"foo.*"},
		AllowDirect: true,
	})
	require_NoError(t, err)

	sendRequestAndCheck := func(mreq *JSApiMsgGetRequest, seq uint64, eerr error) {
		t.Helper()
		req, _ := json.Marshal(mreq)
		rep, err := nc.Request(fmt.Sprintf(JSApiMsgGetT, "TEST"), req, time.Second)
		require_NoError(t, err)
		var mrep JSApiMsgGetResponse
		err = json.Unmarshal(rep.Data, &mrep)
		require_NoError(t, err)
		if eerr != nil {
			require_Error(t, mrep.ToError(), eerr)
			return
		}
		require_NoError(t, mrep.ToError())
		require_Equal(t, seq, mrep.Message.Sequence)
	}
	t0 := time.Now()

	// Check for conflicting options.
	sendRequestAndCheck(&JSApiMsgGetRequest{StartTime: &t0, LastFor: "foo.1"}, 0, NewJSBadRequestError())
	sendRequestAndCheck(&JSApiMsgGetRequest{StartTime: &t0, Seq: 1}, 0, NewJSBadRequestError())

	// Nothing exists yet in the stream.
	sendRequestAndCheck(&JSApiMsgGetRequest{StartTime: &t0}, 0, NewJSNoMessageFoundError())

	_, err = js.Publish("foo.1", nil)
	require_NoError(t, err)

	// Try again with t0 and now it will find the first message.
	sendRequestAndCheck(&JSApiMsgGetRequest{StartTime: &t0}, 1, nil)

	t1 := time.Now()
	_, err = js.Publish("foo.2", nil)
	require_NoError(t, err)

	// At t0, first message will still be returned first.
	sendRequestAndCheck(&JSApiMsgGetRequest{StartTime: &t0}, 1, nil)
	// Unless we combine with NextFor...
	sendRequestAndCheck(&JSApiMsgGetRequest{StartTime: &t0, NextFor: "foo.2"}, 2, nil)

	// At t1 (after first message), the second message will be returned.
	sendRequestAndCheck(&JSApiMsgGetRequest{StartTime: &t1}, 2, nil)

	t2 := time.Now()
	// t2 is later than the last message so nothing will be found.
	sendRequestAndCheck(&JSApiMsgGetRequest{StartTime: &t2}, 0, NewJSNoMessageFoundError())
}

func TestJetStreamMsgDirectGetAsOfTime(t *testing.T) {
	s := RunBasicJetStreamServer(t)
	defer s.Shutdown()

	nc, js := jsClientConnect(t, s)
	defer nc.Close()

	_, err := js.AddStream(&nats.StreamConfig{
		Name:        "TEST",
		Subjects:    []string{"foo.*"},
		AllowDirect: true,
	})
	require_NoError(t, err)

	sendRequestAndCheck := func(mreq *JSApiMsgGetRequest, seq uint64, eerr string) {
		t.Helper()
		req, _ := json.Marshal(mreq)
		rep, err := nc.Request(fmt.Sprintf(JSDirectMsgGetT, "TEST"), req, time.Second)
		require_NoError(t, err)
		if eerr != "" {
			require_Equal(t, rep.Header.Get("Description"), eerr)
			return
		}

		mseq, err := strconv.ParseUint(rep.Header.Get("Nats-Sequence"), 10, 64)
		require_NoError(t, err)
		require_Equal(t, seq, mseq)
	}
	t0 := time.Now()

	// Check for conflicting options.
	sendRequestAndCheck(&JSApiMsgGetRequest{StartTime: &t0, LastFor: "foo.1"}, 0, "Bad Request")
	sendRequestAndCheck(&JSApiMsgGetRequest{StartTime: &t0, Seq: 1}, 0, "Bad Request")

	// Nothing exists yet in the stream.
	sendRequestAndCheck(&JSApiMsgGetRequest{StartTime: &t0}, 0, "Message Not Found")

	_, err = js.Publish("foo.1", nil)
	require_NoError(t, err)

	// Try again with t0 and now it will find the first message.
	sendRequestAndCheck(&JSApiMsgGetRequest{StartTime: &t0}, 1, "")

	t1 := time.Now()
	_, err = js.Publish("foo.2", nil)
	require_NoError(t, err)

	// At t0, first message will still be returned first.
	sendRequestAndCheck(&JSApiMsgGetRequest{StartTime: &t0}, 1, "")
	// Unless we combine with NextFor..
	sendRequestAndCheck(&JSApiMsgGetRequest{StartTime: &t0, NextFor: "foo.2"}, 2, "")

	// At t1 (after first message), the second message will be returned.
	sendRequestAndCheck(&JSApiMsgGetRequest{StartTime: &t1}, 2, "")

	t2 := time.Now()
	// t2 is later than the last message so nothing will be found.
	sendRequestAndCheck(&JSApiMsgGetRequest{StartTime: &t2}, 0, "Message Not Found")
}

func TestJetStreamSubjectFilteredPurgeClearsPendingAcks(t *testing.T) {
	s := RunBasicJetStreamServer(t)
	defer s.Shutdown()

	nc, js := jsClientConnect(t, s)
	defer nc.Close()

	_, err := js.AddStream(&nats.StreamConfig{
		Name:     "TEST",
		Subjects: []string{"foo", "bar"},
	})
	require_NoError(t, err)

	for i := 0; i < 5; i++ {
		js.Publish("foo", []byte("OK"))
		js.Publish("bar", []byte("OK"))
	}

	// Note that there are no subject filters here, this is deliberate
	// as previously the purge with filter code was checking for them.
	// We want to prove that unfiltered consumers also get purged.
	ci, err := js.AddConsumer("TEST", &nats.ConsumerConfig{
		Name:          "my_consumer",
		AckPolicy:     nats.AckExplicitPolicy,
		MaxAckPending: 10,
	})
	require_NoError(t, err)
	require_Equal(t, ci.NumPending, 10)
	require_Equal(t, ci.NumAckPending, 0)

	sub, err := js.PullSubscribe(">", "", nats.Bind("TEST", "my_consumer"))
	require_NoError(t, err)

	msgs, err := sub.Fetch(10)
	require_NoError(t, err)
	require_Len(t, len(msgs), 10)

	ci, err = js.ConsumerInfo("TEST", "my_consumer")
	require_NoError(t, err)
	require_Equal(t, ci.NumPending, 0)
	require_Equal(t, ci.NumAckPending, 10)

	require_NoError(t, js.PurgeStream("TEST", &nats.StreamPurgeRequest{
		Subject: "foo",
	}))

	ci, err = js.ConsumerInfo("TEST", "my_consumer")
	require_NoError(t, err)
	require_Equal(t, ci.NumPending, 0)
	require_Equal(t, ci.NumAckPending, 5)

	for i := 0; i < 5; i++ {
		js.Publish("foo", []byte("OK"))
	}
	msgs, err = sub.Fetch(5)
	require_NoError(t, err)
	require_Len(t, len(msgs), 5)

	ci, err = js.ConsumerInfo("TEST", "my_consumer")
	require_NoError(t, err)
	require_Equal(t, ci.NumPending, 0)
	require_Equal(t, ci.NumAckPending, 10)
}

// Helper function for TestJetStreamConsumerPause*, TestJetStreamClusterConsumerPause*, TestJetStreamSuperClusterConsumerPause*
func jsTestPause_CreateOrUpdateConsumer(t *testing.T, nc *nats.Conn, action ConsumerAction, stream string, cc ConsumerConfig) *JSApiConsumerCreateResponse {
	t.Helper()
	j, err := json.Marshal(CreateConsumerRequest{
		Stream: stream,
		Config: cc,
		Action: action,
	})
	require_NoError(t, err)
	subj := fmt.Sprintf("$JS.API.CONSUMER.CREATE.%s.%s", stream, cc.Name)
	m, err := nc.Request(subj, j, time.Second*3)
	require_NoError(t, err)
	var res JSApiConsumerCreateResponse
	require_NoError(t, json.Unmarshal(m.Data, &res))
	require_True(t, res.Config != nil)
	return &res
}

// Helper function for TestJetStreamConsumerPause*, TestJetStreamClusterConsumerPause*, TestJetStreamSuperClusterConsumerPause*
func jsTestPause_PauseConsumer(t *testing.T, nc *nats.Conn, stream, consumer string, deadline time.Time) time.Time {
	t.Helper()
	j, err := json.Marshal(JSApiConsumerPauseRequest{
		PauseUntil: deadline,
	})
	require_NoError(t, err)
	subj := fmt.Sprintf("$JS.API.CONSUMER.PAUSE.%s.%s", stream, consumer)
	msg, err := nc.Request(subj, j, time.Second)
	require_NoError(t, err)
	var res JSApiConsumerPauseResponse
	require_NoError(t, json.Unmarshal(msg.Data, &res))
	return res.PauseUntil
}

func TestJetStreamDirectGetMulti(t *testing.T) {
	cases := []struct {
		name string
		cfg  *nats.StreamConfig
	}{
		{name: "MemoryStore",
			cfg: &nats.StreamConfig{
				Name:        "TEST",
				Subjects:    []string{"foo.*"},
				AllowDirect: true,
				Storage:     nats.MemoryStorage,
			}},
		{name: "FileStore",
			cfg: &nats.StreamConfig{
				Name:        "TEST",
				Subjects:    []string{"foo.*"},
				AllowDirect: true,
			}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {

			s := RunBasicJetStreamServer(t)
			defer s.Shutdown()

			nc, js := jsClientConnect(t, s)
			defer nc.Close()

			_, err := js.AddStream(c.cfg)
			require_NoError(t, err)

			// Add in messages
			for i := 0; i < 33; i++ {
				js.PublishAsync("foo.foo", []byte(fmt.Sprintf("HELLO-%d", i)))
				js.PublishAsync("foo.bar", []byte(fmt.Sprintf("WORLD-%d", i)))
				js.PublishAsync("foo.baz", []byte(fmt.Sprintf("AGAIN-%d", i)))
			}
			select {
			case <-js.PublishAsyncComplete():
			case <-time.After(5 * time.Second):
				t.Fatalf("Did not receive completion signal")
			}

			// Direct subjects.
			sendRequest := func(mreq *JSApiMsgGetRequest) *nats.Subscription {
				t.Helper()
				req, _ := json.Marshal(mreq)
				// We will get multiple responses so can't do normal request.
				reply := nats.NewInbox()
				sub, err := nc.SubscribeSync(reply)
				require_NoError(t, err)
				err = nc.PublishRequest("$JS.API.DIRECT.GET.TEST", reply, req)
				require_NoError(t, err)
				return sub
			}

			// Subject / Sequence pair
			type p struct {
				subj string
				seq  int
			}
			var eob p

			// Multi-Get will have a nil message as the end marker regardless.
			checkResponses := func(sub *nats.Subscription, numPendingStart int, expected ...p) {
				t.Helper()
				last := "0"
				defer sub.Unsubscribe()
				checkSubsPending(t, sub, len(expected))
				np := numPendingStart
				for i := 0; i < len(expected); i++ {
					msg, err := sub.NextMsg(10 * time.Millisecond)
					require_NoError(t, err)
					// If expected is _EMPTY_ that signals we expect a EOB marker.
					if subj := expected[i].subj; subj != _EMPTY_ {
						// Make sure subject is correct.
						require_Equal(t, subj, msg.Header.Get(JSSubject))
						// Make sure sequence is correct.
						require_Equal(t, strconv.Itoa(expected[i].seq), msg.Header.Get(JSSequence))
						// Should have Data field non-zero
						require_True(t, len(msg.Data) > 0)
						// Check we have NumPending and it's correct.
						if np > 0 {
							np--
						}
						require_Equal(t, strconv.Itoa(np), msg.Header.Get(JSNumPending))
						require_Equal(t, last, msg.Header.Get(JSLastSequence))
						last = msg.Header.Get(JSSequence)
					} else {
						// Check for properly formatted EOB marker.
						// Should have no body.
						require_Equal(t, len(msg.Data), 0)
						// We mark status as 204 - No Content
						require_Equal(t, msg.Header.Get("Status"), "204")
						// Check description is EOB
						require_Equal(t, msg.Header.Get("Description"), "EOB")
						// Check we have NumPending and it's correct.
						require_Equal(t, strconv.Itoa(np), msg.Header.Get(JSNumPending))
						require_Equal(t, last, msg.Header.Get(JSLastSequence))
					}
				}
			}

			sub := sendRequest(&JSApiMsgGetRequest{MultiLastFor: []string{"foo.*"}})
			checkResponses(sub, 3, p{"foo.foo", 97}, p{"foo.bar", 98}, p{"foo.baz", 99}, eob)
			// Check with UpToSeq
			sub = sendRequest(&JSApiMsgGetRequest{MultiLastFor: []string{"foo.*"}, UpToSeq: 3})
			checkResponses(sub, 3, p{"foo.foo", 1}, p{"foo.bar", 2}, p{"foo.baz", 3}, eob)
			// check last header sequence number is correct
			sub = sendRequest(&JSApiMsgGetRequest{MultiLastFor: []string{"foo.foo", "foo.baz"}})
			checkResponses(sub, 2, p{"foo.foo", 97}, p{"foo.baz", 99}, eob)
			// Test No Results.
			sub = sendRequest(&JSApiMsgGetRequest{MultiLastFor: []string{"bar.*"}})
			checkSubsPending(t, sub, 1)
			msg, err := sub.NextMsg(10 * time.Millisecond)
			require_NoError(t, err)
			// Check for properly formatted No Results.
			// Should have no body.
			require_Equal(t, len(msg.Data), 0)
			// We mark status as 204 - No Content
			require_Equal(t, msg.Header.Get("Status"), "404")
			// Check description is No Results
			require_Equal(t, msg.Header.Get("Description"), "No Results")
		})
	}
}

func TestJetStreamDirectGetMultiUpToTime(t *testing.T) {
	s := RunBasicJetStreamServer(t)
	defer s.Shutdown()

	nc, js := jsClientConnect(t, s)
	defer nc.Close()

	_, err := js.AddStream(&nats.StreamConfig{
		Name:        "TEST",
		Subjects:    []string{"foo.*"},
		AllowDirect: true,
	})
	require_NoError(t, err)

	js.Publish("foo.foo", []byte("1"))
	js.Publish("foo.bar", []byte("1"))
	js.Publish("foo.baz", []byte("1"))
	start := time.Now()
	time.Sleep(time.Second)
	js.Publish("foo.foo", []byte("2"))
	js.Publish("foo.bar", []byte("2"))
	js.Publish("foo.baz", []byte("2"))
	mid := time.Now()
	time.Sleep(time.Second)
	js.Publish("foo.foo", []byte("3"))
	js.Publish("foo.bar", []byte("3"))
	js.Publish("foo.baz", []byte("3"))
	end := time.Now()

	// Direct subjects.
	sendRequest := func(mreq *JSApiMsgGetRequest) *nats.Subscription {
		t.Helper()
		req, _ := json.Marshal(mreq)
		// We will get multiple responses so can't do normal request.
		reply := nats.NewInbox()
		sub, err := nc.SubscribeSync(reply)
		require_NoError(t, err)
		err = nc.PublishRequest("$JS.API.DIRECT.GET.TEST", reply, req)
		require_NoError(t, err)
		return sub
	}

	checkResponses := func(sub *nats.Subscription, val string, expected ...string) {
		t.Helper()
		defer sub.Unsubscribe()
		checkSubsPending(t, sub, len(expected))
		for i := 0; i < len(expected); i++ {
			msg, err := sub.NextMsg(10 * time.Millisecond)
			require_NoError(t, err)
			// If expected is _EMPTY_ that signals we expect a EOB marker.
			if subj := expected[i]; subj != _EMPTY_ {
				// Make sure subject is correct.
				require_Equal(t, subj, msg.Header.Get(JSSubject))
				// Should have Data field non-zero
				require_True(t, len(msg.Data) > 0)
				// Make sure the value matches.
				require_Equal(t, string(msg.Data), val)
			}
		}
	}

	// Make sure you can't set both.
	sub := sendRequest(&JSApiMsgGetRequest{Seq: 1, MultiLastFor: []string{"foo.*"}, UpToSeq: 3, UpToTime: &start})
	checkSubsPending(t, sub, 1)
	msg, err := sub.NextMsg(10 * time.Millisecond)
	require_NoError(t, err)
	// Check for properly formatted No Results.
	// Should have no body.
	require_Equal(t, len(msg.Data), 0)
	// We mark status as 204 - No Content
	require_Equal(t, msg.Header.Get("Status"), "408")
	// Check description is No Results
	require_Equal(t, msg.Header.Get("Description"), "Bad Request")

	// Valid responses.
	sub = sendRequest(&JSApiMsgGetRequest{Seq: 1, MultiLastFor: []string{"foo.*"}, UpToTime: &start})
	checkResponses(sub, "1", "foo.foo", "foo.bar", "foo.baz", _EMPTY_)

	sub = sendRequest(&JSApiMsgGetRequest{Seq: 1, MultiLastFor: []string{"foo.*"}, UpToTime: &mid})
	checkResponses(sub, "2", "foo.foo", "foo.bar", "foo.baz", _EMPTY_)

	sub = sendRequest(&JSApiMsgGetRequest{Seq: 1, MultiLastFor: []string{"foo.*"}, UpToTime: &end})
	checkResponses(sub, "3", "foo.foo", "foo.bar", "foo.baz", _EMPTY_)
}

func TestJetStreamDirectGetMultiMaxAllowed(t *testing.T) {
	s := RunBasicJetStreamServer(t)
	defer s.Shutdown()

	nc, js := jsClientConnect(t, s)
	defer nc.Close()

	_, err := js.AddStream(&nats.StreamConfig{
		Name:        "TEST",
		Subjects:    []string{"foo.*"},
		AllowDirect: true,
	})
	require_NoError(t, err)

	// from stream.go - const maxAllowedResponses = 1024, so max sure > 1024
	// Add in messages
	for i := 1; i <= 1025; i++ {
		js.PublishAsync(fmt.Sprintf("foo.%d", i), []byte("OK"))
	}
	select {
	case <-js.PublishAsyncComplete():
	case <-time.After(5 * time.Second):
		t.Fatalf("Did not receive completion signal")
	}

	req, _ := json.Marshal(&JSApiMsgGetRequest{Seq: 1, MultiLastFor: []string{"foo.*"}})
	msg, err := nc.Request("$JS.API.DIRECT.GET.TEST", req, time.Second)
	require_NoError(t, err)

	// Check for properly formatted Too Many Results error.
	// Should have no body.
	require_Equal(t, len(msg.Data), 0)
	// We mark status as 413 - Too Many Results
	require_Equal(t, msg.Header.Get("Status"), "413")
	// Check description is No Results
	require_Equal(t, msg.Header.Get("Description"), "Too Many Results")
}

func TestJetStreamDirectGetMultiPaging(t *testing.T) {
	s := RunBasicJetStreamServer(t)
	defer s.Shutdown()

	nc, js := jsClientConnect(t, s)
	defer nc.Close()

	_, err := js.AddStream(&nats.StreamConfig{
		Name:        "TEST",
		Subjects:    []string{"foo.*"},
		AllowDirect: true,
	})
	require_NoError(t, err)

	// We will queue up 500 messages, each 512k big and request them for a multi-get.
	// This will not hit the max allowed limit of 1024, but will bump up against max bytes and only return partial results.
	// We want to make sure we can pick up where we left off.

	// Add in messages
	data, sent := bytes.Repeat([]byte("Z"), 512*1024), 500
	for i := 1; i <= sent; i++ {
		js.PublishAsync(fmt.Sprintf("foo.%d", i), data)
	}
	select {
	case <-js.PublishAsyncComplete():
	case <-time.After(5 * time.Second):
		t.Fatalf("Did not receive completion signal")
	}
	// Wait for all replicas to be correct.
	time.Sleep(time.Second)

	// Direct subjects.
	sendRequest := func(mreq *JSApiMsgGetRequest) *nats.Subscription {
		t.Helper()
		req, _ := json.Marshal(mreq)
		// We will get multiple responses so can't do normal request.
		reply := nats.NewInbox()
		sub, err := nc.SubscribeSync(reply)
		require_NoError(t, err)
		err = nc.PublishRequest("$JS.API.DIRECT.GET.TEST", reply, req)
		require_NoError(t, err)
		return sub
	}

	// Setup variables that control procesPartial
	start, seq, np, b, bsz := 1, 1, sent, 0, 128

	processPartial := func(expected int) {
		t.Helper()
		sub := sendRequest(&JSApiMsgGetRequest{Seq: uint64(start), Batch: b, MultiLastFor: []string{"foo.*"}})
		checkSubsPending(t, sub, expected)
		// Check partial.
		// We should receive seqs seq-(seq+bsz-1)
		for ; seq < start+(expected-1); seq++ {
			msg, err := sub.NextMsg(10 * time.Millisecond)
			require_NoError(t, err)
			// Make sure sequence is correct.
			require_Equal(t, strconv.Itoa(int(seq)), msg.Header.Get(JSSequence))
			// Check we have NumPending and it's correct.
			if np > 0 {
				np--
			}
			require_Equal(t, strconv.Itoa(np), msg.Header.Get(JSNumPending))
		}
		// Now check EOB
		msg, err := sub.NextMsg(10 * time.Millisecond)
		require_NoError(t, err)
		// We mark status as 204 - No Content
		require_Equal(t, msg.Header.Get("Status"), "204")
		// Check description is EOB
		require_Equal(t, msg.Header.Get("Description"), "EOB")
		// Check we have NumPending and it's correct.
		require_Equal(t, strconv.Itoa(np), msg.Header.Get(JSNumPending))
		// Check we have LastSequence and it's correct.
		require_Equal(t, strconv.Itoa(seq-1), msg.Header.Get(JSLastSequence))
		// Check we have UpToSequence and it's correct.
		require_Equal(t, strconv.Itoa(sent), msg.Header.Get(JSUpToSequence))
		// Update start
		start = seq
	}

	processPartial(bsz + 1) // 128 + EOB
	processPartial(bsz + 1) // 128 + EOB
	processPartial(bsz + 1) // 128 + EOB
	// Last one will be a partial block.
	processPartial(116 + 1)

	// Now reset and test that batch is honored as well.
	start, seq, np, b = 1, 1, sent, 100
	for i := 0; i < 5; i++ {
		processPartial(b + 1) // 100 + EOB
	}
}

// https://github.com/nats-io/nats-server/issues/4878
func TestJetStreamInterestStreamConsumerFilterEdit(t *testing.T) {
	s := RunBasicJetStreamServer(t)
	defer s.Shutdown()

	nc, js := jsClientConnect(t, s)
	defer nc.Close()

	_, err := js.AddStream(&nats.StreamConfig{
		Name:      "INTEREST",
		Retention: nats.InterestPolicy,
		Subjects:  []string{"interest.>"},
	})
	require_NoError(t, err)

	_, err = js.AddConsumer("INTEREST", &nats.ConsumerConfig{
		Durable:       "C0",
		FilterSubject: "interest.>",
		AckPolicy:     nats.AckExplicitPolicy,
	})
	require_NoError(t, err)

	for i := 0; i < 10; i++ {
		_, err = js.Publish(fmt.Sprintf("interest.%d", i), []byte(strconv.Itoa(i)))
		require_NoError(t, err)
	}

	// we check we got 10 messages
	nfo, err := js.StreamInfo("INTEREST")
	require_NoError(t, err)
	if nfo.State.Msgs != 10 {
		t.Fatalf("expected 10 messages got %d", nfo.State.Msgs)
	}

	// now we lower the consumer interest from all subjects to 1,
	// then check the stream state and check if interest behavior still works
	_, err = js.UpdateConsumer("INTEREST", &nats.ConsumerConfig{
		Durable:       "C0",
		FilterSubject: "interest.1",
		AckPolicy:     nats.AckExplicitPolicy,
	})
	require_NoError(t, err)

	// we should now have only one message left
	nfo, err = js.StreamInfo("INTEREST")
	require_NoError(t, err)
	if nfo.State.Msgs != 1 {
		t.Fatalf("expected 1 message got %d", nfo.State.Msgs)
	}
}

// https://github.com/nats-io/nats-server/issues/5383
func TestJetStreamInterestStreamWithFilterSubjectsConsumer(t *testing.T) {
	s := RunBasicJetStreamServer(t)
	defer s.Shutdown()

	// Client for API requests.
	nc, js := jsClientConnect(t, s)
	defer nc.Close()

	_, err := js.AddStream(&nats.StreamConfig{
		Name:      "INTEREST",
		Retention: nats.InterestPolicy,
		Subjects:  []string{"interest.>"},
	})
	require_NoError(t, err)

	_, err = js.AddConsumer("INTEREST", &nats.ConsumerConfig{
		Durable:        "C",
		FilterSubjects: []string{"interest.a", "interest.b"},
		AckPolicy:      nats.AckExplicitPolicy,
	})
	require_NoError(t, err)

	for _, sub := range []string{"interest.a", "interest.b", "interest.c"} {
		_, err = js.Publish(sub, nil)
		require_NoError(t, err)
	}

	// we check we got 2 messages, interest.c doesn't have interest
	nfo, err := js.StreamInfo("INTEREST")
	require_NoError(t, err)
	if nfo.State.Msgs != 2 {
		t.Fatalf("expected 2 messages got %d", nfo.State.Msgs)
	}
}

func TestJetStreamAckAllWithLargeFirstSequenceAndNoAckFloor(t *testing.T) {
	s := RunBasicJetStreamServer(t)
	defer s.Shutdown()

	// Client for API requests.
	nc, js := jsClientConnect(t, s)
	defer nc.Close()

	_, err := js.AddStream(&nats.StreamConfig{
		Name:     "TEST",
		Subjects: []string{"foo.>"},
	})
	require_NoError(t, err)

	// Set first sequence to something very big here. This shows the issue with AckAll the
	// first time it is called and existing ack floor is 0.
	err = js.PurgeStream("TEST", &nats.StreamPurgeRequest{Sequence: 10_000_000_000})
	require_NoError(t, err)

	// Now add in 100 msgs
	for i := 0; i < 100; i++ {
		js.Publish("foo.bar", []byte("hello"))
	}

	ss, err := js.PullSubscribe("foo.*", "C1", nats.AckAll())
	require_NoError(t, err)
	msgs, err := ss.Fetch(10, nats.MaxWait(100*time.Millisecond))
	require_NoError(t, err)
	require_Equal(t, len(msgs), 10)

	start := time.Now()
	msgs[9].AckSync()
	if elapsed := time.Since(start); elapsed > 250*time.Millisecond {
		t.Fatalf("AckSync took too long %v", elapsed)
	}

	// Make sure next fetch works right away with low timeout.
	msgs, err = ss.Fetch(10, nats.MaxWait(100*time.Millisecond))
	require_NoError(t, err)
	require_Equal(t, len(msgs), 10)

	_, err = js.StreamInfo("TEST", nats.MaxWait(250*time.Millisecond))
	require_NoError(t, err)

	// Now make sure that if we ack in the middle, meaning we still have ack pending,
	// that we do the right thing as well.
	ss, err = js.PullSubscribe("foo.*", "C2", nats.AckAll())
	require_NoError(t, err)
	msgs, err = ss.Fetch(10, nats.MaxWait(100*time.Millisecond))
	require_NoError(t, err)
	require_Equal(t, len(msgs), 10)

	start = time.Now()
	msgs[5].AckSync()
	if elapsed := time.Since(start); elapsed > 250*time.Millisecond {
		t.Fatalf("AckSync took too long %v", elapsed)
	}

	// Make sure next fetch works right away with low timeout.
	msgs, err = ss.Fetch(10, nats.MaxWait(100*time.Millisecond))
	require_NoError(t, err)
	require_Equal(t, len(msgs), 10)

	_, err = js.StreamInfo("TEST", nats.MaxWait(250*time.Millisecond))
	require_NoError(t, err)
}

func TestJetStreamAckAllWithLargeFirstSequenceAndNoAckFloorWithInterestPolicy(t *testing.T) {
	s := RunBasicJetStreamServer(t)
	defer s.Shutdown()

	// Client for API requests.
	nc, js := jsClientConnect(t, s)
	defer nc.Close()

	_, err := js.AddStream(&nats.StreamConfig{
		Name:      "TEST",
		Subjects:  []string{"foo.>"},
		Retention: nats.InterestPolicy,
	})
	require_NoError(t, err)

	// Set first sequence to something very big here. This shows the issue with AckAll the
	// first time it is called and existing ack floor is 0.
	err = js.PurgeStream("TEST", &nats.StreamPurgeRequest{Sequence: 10_000_000_000})
	require_NoError(t, err)

	ss, err := js.PullSubscribe("foo.*", "C1", nats.AckAll())
	require_NoError(t, err)

	// Now add in 100 msgs
	for i := 0; i < 100; i++ {
		js.Publish("foo.bar", []byte("hello"))
	}

	msgs, err := ss.Fetch(10, nats.MaxWait(100*time.Millisecond))
	require_NoError(t, err)
	require_Equal(t, len(msgs), 10)

	start := time.Now()
	msgs[5].AckSync()
	if elapsed := time.Since(start); elapsed > 250*time.Millisecond {
		t.Fatalf("AckSync took too long %v", elapsed)
	}

	// We are testing for run away loops acking messages in the stream that are not there.
	_, err = js.StreamInfo("TEST", nats.MaxWait(100*time.Millisecond))
	require_NoError(t, err)
}

// Allow streams with $JS or $SYS prefixes for audit purposes but require no pub ack be set.
func TestJetStreamAuditStreams(t *testing.T) {
	s := RunBasicJetStreamServer(t)
	defer s.Shutdown()

	// Client for API requests.
	nc, js := jsClientConnect(t, s)
	defer nc.Close()

	jsOverlap := errors.New("subjects that overlap with jetstream api require no-ack to be true")
	sysOverlap := errors.New("subjects that overlap with system api require no-ack to be true")

	_, err := js.AddStream(&nats.StreamConfig{
		Name:     "TEST",
		Subjects: []string{"$JS.>"},
	})
	require_Error(t, err, NewJSStreamInvalidConfigError(jsOverlap))

	_, err = js.AddStream(&nats.StreamConfig{
		Name:     "TEST",
		Subjects: []string{"$JS.API.>"},
	})
	require_Error(t, err, NewJSStreamInvalidConfigError(jsOverlap))

	_, err = js.AddStream(&nats.StreamConfig{
		Name:     "TEST",
		Subjects: []string{"$JSC.>"},
	})
	require_Error(t, err, NewJSStreamInvalidConfigError(jsOverlap))

	_, err = js.AddStream(&nats.StreamConfig{
		Name:     "TEST",
		Subjects: []string{"$SYS.>"},
	})
	require_Error(t, err, NewJSStreamInvalidConfigError(sysOverlap))

	_, err = js.AddStream(&nats.StreamConfig{
		Name:     "TEST",
		Subjects: []string{">"},
	})
	require_Error(t, err, NewJSStreamInvalidConfigError(errors.New("capturing all subjects requires no-ack to be true")))

	// These should all be ok if no pub ack.
	_, err = js.AddStream(&nats.StreamConfig{
		Name:     "TEST1",
		Subjects: []string{"$JS.>"},
		NoAck:    true,
	})
	require_NoError(t, err)

	_, err = js.AddStream(&nats.StreamConfig{
		Name:     "TEST2",
		Subjects: []string{"$JSC.>"},
		NoAck:    true,
	})
	require_NoError(t, err)

	_, err = js.AddStream(&nats.StreamConfig{
		Name:     "TEST3",
		Subjects: []string{"$SYS.>"},
		NoAck:    true,
	})
	require_NoError(t, err)

	// Since prior behavior did allow $JS.EVENT to be captured without no-ack, these might break
	// on a server upgrade so make sure they still work ok without --no-ack.

	// To avoid overlap error.
	err = js.DeleteStream("TEST1")
	require_NoError(t, err)

	_, err = js.AddStream(&nats.StreamConfig{
		Name:     "TEST4",
		Subjects: []string{"$JS.EVENT.>"},
	})
	require_NoError(t, err)

	// Also allow $SYS.ACCOUNT to be captured without no-ack, these also might break
	// on a server upgrade so make sure they still work ok without --no-ack.

	// To avoid overlap error.
	err = js.DeleteStream("TEST3")
	require_NoError(t, err)

	_, err = js.AddStream(&nats.StreamConfig{
		Name:     "TEST5",
		Subjects: []string{"$SYS.ACCOUNT.>"},
	})
	require_NoError(t, err)

	// We will test handling of ">" on a cluster here.
	// Specific test for capturing everything which will require both no-ack and replicas of 1.
	c := createJetStreamClusterExplicit(t, "R3S", 3)
	defer c.shutdown()

	nc, js = jsClientConnect(t, c.randomServer())
	defer nc.Close()

	_, err = js.AddStream(&nats.StreamConfig{
		Name:     "TEST",
		Subjects: []string{">"},
	})
	require_Error(t, err, NewJSStreamInvalidConfigError(errors.New("capturing all subjects requires no-ack to be true")))

	_, err = js.AddStream(&nats.StreamConfig{
		Name:     "TEST",
		Subjects: []string{">"},
		Replicas: 3,
		NoAck:    true,
	})
	require_Error(t, err, NewJSStreamInvalidConfigError(errors.New("capturing all subjects requires replicas of 1")))

	// Ths should work ok.
	_, err = js.AddStream(&nats.StreamConfig{
		Name:     "TEST",
		Subjects: []string{">"},
		Replicas: 1,
		NoAck:    true,
	})
	require_NoError(t, err)
}

// https://github.com/nats-io/nats-server/issues/5570
func TestJetStreamBadSubjectMappingStream(t *testing.T) {
	s := RunBasicJetStreamServer(t)
	defer s.Shutdown()

	// Client for API requests.
	nc, js := jsClientConnect(t, s)
	defer nc.Close()

	_, err := js.AddStream(&nats.StreamConfig{Name: "test"})
	require_NoError(t, err)

	_, err = js.UpdateStream(&nats.StreamConfig{
		Name: "test",
		Sources: []*nats.StreamSource{
			{
				Name: "mapping",
				SubjectTransforms: []nats.SubjectTransformConfig{
					{
						Source:      "events.*",
						Destination: "events.{{wildcard(1)}}{{split(3,1)}}",
					},
				},
			},
		},
	})
	require_Error(t, err, NewJSStreamUpdateError(errors.New("nats: source transform: invalid mapping destination: too many arguments passed to the function in {{wildcard(1)}}{{split(3,1)}}")))

	_, err = js.UpdateStream(&nats.StreamConfig{
		Name: "test",
		Sources: []*nats.StreamSource{
			{
				Name: "mapping",
				SubjectTransforms: []nats.SubjectTransformConfig{
					{
						Source:      "events.>.*",
						Destination: "events.{{split(1,1)}}",
					},
				},
			},
		},
	})
	require_Error(t, err, NewJSStreamUpdateError(errors.New("nats: source transform source: invalid subject events.>.*")))

	_, err = js.UpdateStream(&nats.StreamConfig{
		Name: "test",
		Mirror: &nats.StreamSource{
			Name: "mapping",
			SubjectTransforms: []nats.SubjectTransformConfig{
				{
					Source:      "events.*",
					Destination: "events.{{split(3,1)}}",
				},
			},
		},
	})
	require_Error(t, err, NewJSStreamUpdateError(errors.New("nats: mirror transform: invalid mapping destination: wildcard index out of range in {{split(3,1)}}: [3]")))

	_, err = js.UpdateStream(&nats.StreamConfig{
		Name: "test",
		Mirror: &nats.StreamSource{
			Name: "mapping",
			SubjectTransforms: []nats.SubjectTransformConfig{
				{
					Source:      "events.>.*",
					Destination: "events.{{split(1,1)}}",
				},
			},
		},
	})
	require_Error(t, err, NewJSStreamUpdateError(errors.New("nats: mirror transform source: invalid subject events.>.*")))

	_, err = js.UpdateStream(&nats.StreamConfig{
		Name: "test",
		SubjectTransform: &nats.SubjectTransformConfig{
			Source:      "events.*",
			Destination: "events.{{split(3,1)}}",
		},
	})
	require_Error(t, err, NewJSStreamUpdateError(errors.New("nats: stream transform: invalid mapping destination: wildcard index out of range in {{split(3,1)}}: [3]")))

	_, err = js.UpdateStream(&nats.StreamConfig{
		Name: "test",
		SubjectTransform: &nats.SubjectTransformConfig{
			Source:      "events.>.*",
			Destination: "events.{{split(1,1)}}",
		},
	})
	require_Error(t, err, NewJSStreamUpdateError(errors.New("nats: stream transform source: invalid subject events.>.*")))
}

func TestJetStreamInterestStreamWithDuplicateMessages(t *testing.T) {
	s := RunBasicJetStreamServer(t)
	defer s.Shutdown()

	nc, js := jsClientConnect(t, s)
	defer nc.Close()

	cfg := &nats.StreamConfig{
		Name:      "INTEREST",
		Subjects:  []string{"interest"},
		Replicas:  1,
		Retention: nats.InterestPolicy,
	}
	_, err := js.AddStream(cfg)
	require_NoError(t, err)

	// Publishing the first time should give a sequence, even when there's no interest.
	pa, err := js.Publish("interest", nil, nats.MsgId("dedupe"))
	require_NoError(t, err)
	require_Equal(t, pa.Sequence, 1)
	require_Equal(t, pa.Duplicate, false)

	// Publishing a duplicate with no interest should return the same sequence as above.
	pa, err = js.Publish("interest", nil, nats.MsgId("dedupe"))
	require_NoError(t, err)
	require_Equal(t, pa.Sequence, 1)
	require_Equal(t, pa.Duplicate, true)
}

func TestJetStreamStreamCreatePedanticMode(t *testing.T) {
	cfgFmt := []byte(fmt.Sprintf(`
        jetstream: {
            enabled: true
            max_file_store: 100MB
            store_dir: %s
            limits: {duplicate_window: "1m", max_request_batch: 250}
        }
        accounts: {
            myacc: {
                jetstream: enabled
                users: [ { user: user, password: pass  } ]
            }
        }
        no_auth_user: user
	`, t.TempDir()))

	conf := createConfFile(t, cfgFmt)
	s, _ := RunServerWithConfig(conf)
	defer s.Shutdown()

	nc, _ := jsClientConnect(t, s)
	defer nc.Close()

	tests := []struct {
		name      string
		cfg       StreamConfigRequest
		shouldErr bool
		update    bool
	}{
		{
			name: "too_high_duplicate",
			cfg: StreamConfigRequest{
				StreamConfig: StreamConfig{
					Name:       "TEST",
					MaxAge:     time.Minute,
					Duplicates: time.Hour,
					Storage:    FileStorage,
				},
				Pedantic: true,
			},
			shouldErr: true,
		},
		{
			name: "duplicate_over_limits",
			cfg: StreamConfigRequest{
				StreamConfig: StreamConfig{
					Name:       "TEST",
					MaxAge:     time.Hour * 60,
					Duplicates: time.Hour,
					Storage:    FileStorage,
				},
				Pedantic: false,
			},
			shouldErr: true,
		},
		{
			name: "duplicate_window_within_limits",
			cfg: StreamConfigRequest{
				StreamConfig: StreamConfig{
					Name:       "TEST",
					MaxAge:     time.Hour * 60,
					Duplicates: time.Second * 30,
					Storage:    FileStorage,
				},
				Pedantic: false,
			},
			shouldErr: false,
		},
		{
			name: "update_too_high_duplicate",
			cfg: StreamConfigRequest{
				StreamConfig: StreamConfig{
					Name:       "TEST",
					MaxAge:     time.Minute,
					Duplicates: time.Hour,
					Storage:    FileStorage,
				},
				Pedantic: true,
			},
			update:    true,
			shouldErr: true,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if !test.update {
				_, err := addStreamPedanticWithError(t, nc, &test.cfg)
				require_True(t, (err != nil) == test.shouldErr)
			} else {
				_, err := updateStreamPedanticWithError(t, nc, &test.cfg)
				require_True(t, (err != nil) == test.shouldErr)
			}
		})
	}
}

func TestJetStreamStrictMode(t *testing.T) {
	cfgFmt := []byte(fmt.Sprintf(`
		jetstream: {
			strict: true
			enabled: true
			max_file_store: 100MB
			store_dir: %s
			limits: {duplicate_window: "1m", max_request_batch: 250}
		}
	`, t.TempDir()))
	conf := createConfFile(t, cfgFmt)
	s, _ := RunServerWithConfig(conf)
	defer s.Shutdown()

	nc, err := nats.Connect(s.ClientURL())
	if err != nil {
		t.Fatalf("Error connecting to NATS: %v", err)
	}
	defer nc.Close()

	tests := []struct {
		name        string
		subject     string
		payload     []byte
		expectedErr string
	}{
		{
			name:        "Stream Create",
			subject:     "$JS.API.STREAM.CREATE.TEST_STREAM",
			payload:     []byte(`{"name":"TEST_STREAM","subjects":["test.>"],"extra_field":"unexpected"}`),
			expectedErr: "invalid JSON",
		},
		{
			name:        "Stream Update",
			subject:     "$JS.API.STREAM.UPDATE.TEST_STREAM",
			payload:     []byte(`{"name":"TEST_STREAM","subjects":["test.>"],"extra_field":"unexpected"}`),
			expectedErr: "invalid JSON",
		},
		{
			name:        "Stream Delete",
			subject:     "$JS.API.STREAM.DELETE.TEST_STREAM",
			payload:     []byte(`{"extra_field":"unexpected"}`),
			expectedErr: "expected an empty request payload",
		},
		{
			name:        "Stream Info",
			subject:     "$JS.API.STREAM.INFO.TEST_STREAM",
			payload:     []byte(`{"extra_field":"unexpected"}`),
			expectedErr: "invalid JSON",
		},
		{
			name:        "Consumer Create",
			subject:     "$JS.API.CONSUMER.CREATE.TEST_STREAM.TEST_CONSUMER",
			payload:     []byte(`{"durable_name":"TEST_CONSUMER","ack_policy":"explicit","extra_field":"unexpected"}`),
			expectedErr: "invalid JSON",
		},
		{
			name:        "Consumer Delete",
			subject:     "$JS.API.CONSUMER.DELETE.TEST_STREAM.TEST_CONSUMER",
			payload:     []byte(`{"extra_field":"unexpected"}`),
			expectedErr: "expected an empty request payload",
		},
		{
			name:        "Consumer Info",
			subject:     "$JS.API.CONSUMER.INFO.TEST_STREAM.TEST_CONSUMER",
			payload:     []byte(`{"extra_field":"unexpected"}`),
			expectedErr: "expected an empty request payload",
		},
		{
			name:        "Stream List",
			subject:     "$JS.API.STREAM.LIST",
			payload:     []byte(`{"extra_field":"unexpected"}`),
			expectedErr: "invalid JSON",
		},
		{
			name:        "Consumer List",
			subject:     "$JS.API.CONSUMER.LIST.TEST_STREAM",
			payload:     []byte(`{"extra_field":"unexpected"}`),
			expectedErr: "invalid JSON",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			resp, err := nc.Request(tt.subject, tt.payload, time.Second*10)
			if err != nil {
				t.Fatalf("Request failed: %v", err)
			}

			var apiResp ApiResponse = ApiResponse{}

			if err := json.Unmarshal(resp.Data, &apiResp); err != nil {
				t.Fatalf("Error unmarshalling response: %v", err)
			}

			require_NotNil(t, apiResp.Error.Description)
			require_Contains(t, apiResp.Error.Description, tt.expectedErr)
		})
	}
}

func addConsumerWithError(t *testing.T, nc *nats.Conn, cfg *CreateConsumerRequest) (*ConsumerInfo, *ApiError) {
	t.Helper()
	req, err := json.Marshal(cfg)
	require_NoError(t, err)
	rmsg, err := nc.Request(fmt.Sprintf(JSApiDurableCreateT, cfg.Stream, cfg.Config.Durable), req, 5*time.Second)

	require_NoError(t, err)
	var resp JSApiConsumerCreateResponse
	err = json.Unmarshal(rmsg.Data, &resp)
	require_NoError(t, err)
	if resp.Type != JSApiConsumerCreateResponseType {
		t.Fatalf("Invalid response type %s expected %s", resp.Type, JSApiConsumerCreateResponseType)
	}
	return resp.ConsumerInfo, resp.Error
}

func TestJetStreamSourceRemovalAndReAdd(t *testing.T) {
	test := func(t *testing.T, storageType nats.StorageType) {
		s := RunBasicJetStreamServer(t)
		defer s.Shutdown()

		nc, js := jsClientConnect(t, s)
		defer nc.Close()

		// The source stream.
		_, err := js.AddStream(&nats.StreamConfig{
			Name:     "SRC",
			Subjects: []string{"foo.*"},
		})
		require_NoError(t, err)

		// The stream that sources.
		cfg := &nats.StreamConfig{
			Name: "TEST",
			Sources: []*nats.StreamSource{{
				Name: "SRC",
			}},
			Storage: storageType,
		}
		_, err = js.AddStream(cfg)
		require_NoError(t, err)

		// Now add in 10 msgs.
		for i := range 10 {
			_, err := js.Publish(fmt.Sprintf("foo.%d", i), []byte("test"))
			require_NoError(t, err)
		}

		// Make sure we have 10 msgs in TEST.
		checkFor(t, time.Second, 200*time.Millisecond, func() error {
			si, err := js.StreamInfo("TEST")
			require_NoError(t, err)
			if si.State.Msgs == 10 {
				return nil
			}
			return fmt.Errorf("Do not have all msgs yet, %d of 10", si.State.Msgs)
		})

		// Now update the TEST stream to no longer source.
		cfg.Sources = nil
		_, err = js.UpdateStream(cfg)
		require_NoError(t, err)

		// Now add in 10 more msgs.
		for i := range 10 {
			_, err := js.Publish(fmt.Sprintf("foo.%d", i+10), []byte("test"))
			require_NoError(t, err)
		}
		// Make sure we are still stuck at 10 for TEST.
		si, err := js.StreamInfo("TEST")
		require_NoError(t, err)
		require_Equal(t, si.State.Msgs, 10)

		// Now re-add the source to our stream.
		cfg.Sources = []*nats.StreamSource{{Name: "SRC"}}
		_, err = js.UpdateStream(cfg)
		require_NoError(t, err)

		// Make sure we have 20 msgs now.
		checkFor(t, time.Second, 200*time.Millisecond, func() error {
			si, err := js.StreamInfo("TEST")
			require_NoError(t, err)
			if si.State.Msgs == 20 {
				return nil
			}
			return fmt.Errorf("Do not have all msgs yet, %d of 20", si.State.Msgs)
		})

		// Check that we get what we want in the stream.
		sub, err := js.PullSubscribe("foo.*", "d")
		require_NoError(t, err)

		msgs, err := sub.Fetch(20)
		require_NoError(t, err)
		require_Equal(t, len(msgs), 20)

		for i, m := range msgs {
			require_Equal(t, m.Subject, fmt.Sprintf("foo.%d", i))
		}
	}

	for _, storageType := range []nats.StorageType{nats.FileStorage, nats.MemoryStorage} {
		t.Run(storageType.String(), func(t *testing.T) { test(t, storageType) })
	}
}

func TestJetStreamSourceInfoExposesHighestSourcedSeq(t *testing.T) {
	s := RunBasicJetStreamServer(t)
	defer s.Shutdown()

	nc, js := jsClientConnect(t, s)
	defer nc.Close()

	_, err := js.AddStream(&nats.StreamConfig{
		Name:     "ORIGIN",
		Subjects: []string{"foo"},
	})
	require_NoError(t, err)

	for range 3 {
		_, err = js.Publish("foo", []byte("OK"))
		require_NoError(t, err)
	}

	_, err = js.AddStream(&nats.StreamConfig{
		Name:    "SOURCE",
		Sources: []*nats.StreamSource{{Name: "ORIGIN"}},
	})
	require_NoError(t, err)

	checkFor(t, 2*time.Second, 200*time.Millisecond, func() error {
		rm, err := nc.Request(fmt.Sprintf(JSApiStreamInfoT, "SOURCE"), nil, time.Second)
		require_NoError(t, err)
		var resp JSApiStreamInfoResponse
		require_NoError(t, json.Unmarshal(rm.Data, &resp))
		if resp.Error != nil {
			return resp.Error
		}
		si := resp.StreamInfo
		if len(si.Sources) != 1 {
			return fmt.Errorf("expected 1 source, got %d", len(si.Sources))
		} else if si.State.Msgs != 3 {
			return fmt.Errorf("expected 3 msgs, got %d", si.State.Msgs)
		}
		src := si.Sources[0]
		if src.Name != "ORIGIN" {
			return fmt.Errorf("expected source name to be ORIGIN, got %q", src.Name)
		} else if src.Seq != 3 {
			return fmt.Errorf("expected highest sourced seq of 3, got %d", src.Seq)
		}
		return nil
	})

	// The reported sequence must come from the persisted sources state, the
	// in-memory sourceInfo can be ahead of what's actually been stored.
	mset, err := s.globalAccount().lookupStream("SOURCE")
	require_NoError(t, err)
	const iname = "ORIGIN > >"
	require_Equal(t, mset.store.SourcesState()[iname].Seq, 3)

	mset.mu.Lock()
	mset.sources[iname].sseq = 100
	mset.mu.Unlock()

	sis := mset.sourcesInfo()
	require_Len(t, len(sis), 1)
	require_Equal(t, sis[0].Seq, 3)

	// If the source stream is recreated the persisted state still holds the previous
	// incarnation's identity and sequence, until a new message from the recreated
	// stream is stored. Don't report that stale sequence as ours.
	sss := mset.store.SourcesState()[iname]
	require_True(t, sss.Ident != _EMPTY_)

	mset.mu.Lock()
	si := mset.sources[iname]
	ident := si.ident
	require_Equal(t, ident, sss.Ident)
	si.ident, si.sseq = "recreated", 0
	mset.mu.Unlock()

	sis = mset.sourcesInfo()
	require_Len(t, len(sis), 1)
	require_Equal(t, sis[0].Seq, 0)

	// Same when we're about to retry due to an identity mismatch, the identity is
	// unset but the persisted state is still from the previous incarnation.
	mset.mu.Lock()
	si.ident, si.rc = _EMPTY_, true
	mset.mu.Unlock()

	sis = mset.sourcesInfo()
	require_Len(t, len(sis), 1)
	require_Equal(t, sis[0].Seq, 0)

	// Once the identities match again we report the persisted sequence.
	mset.mu.Lock()
	si.ident, si.rc = ident, false
	mset.mu.Unlock()

	sis = mset.sourcesInfo()
	require_Len(t, len(sis), 1)
	require_Equal(t, sis[0].Seq, 3)

	// The persisted state can legitimately be identity-less, if the last sourced message
	// was stored before source identities existed. That's unverifiable, but still valid,
	// so we must keep reporting the persisted sequence.
	mset.store.ApplySourcesState(map[string]StreamSourceState{iname: {Seq: 3}})
	require_Equal(t, mset.store.SourcesState()[iname].Ident, _EMPTY_)

	sis = mset.sourcesInfo()
	require_Len(t, len(sis), 1)
	require_Equal(t, sis[0].Seq, 3)

	// Unless we know a recreation is in progress.
	mset.mu.Lock()
	si.rc = true
	mset.mu.Unlock()

	sis = mset.sourcesInfo()
	require_Len(t, len(sis), 1)
	require_Equal(t, sis[0].Seq, 0)

	// Resetting the source infos, which happens on startup/leader change and can be
	// followed by a delayed source consumer setup, must seed the identity and sequence
	// right away. Otherwise we'd report a mismatch and a zero sequence in the meantime.
	mset.mu.Lock()
	mset.store.ApplySourcesState(map[string]StreamSourceState{iname: {Seq: 3, Ident: ident}})
	mset.resetSourceInfo()
	rsi := mset.sources[iname]
	mset.mu.Unlock()

	require_NotNil(t, rsi)
	require_Equal(t, rsi.ident, ident)
	require_Equal(t, rsi.sseq, 3)
	require_False(t, rsi.rc)

	sis = mset.sourcesInfo()
	require_Len(t, len(sis), 1)
	require_Equal(t, sis[0].Seq, 3)
}

func TestJetStreamRateLimitHighStreamIngest(t *testing.T) {
	cfgFmt := []byte(fmt.Sprintf(`
        jetstream: {
            enabled: true
            store_dir: %s
            max_buffered_size: 1kb
            max_buffered_msgs: 1
        }
       `, t.TempDir()))

	conf := createConfFile(t, cfgFmt)
	s, opts := RunServerWithConfig(conf)
	defer s.Shutdown()

	require_Equal(t, opts.StreamMaxBufferedSize, 1024)
	require_Equal(t, opts.StreamMaxBufferedMsgs, 1)

	nc, js := jsClientConnect(t, s)
	defer nc.Close()

	_, err := js.AddStream(&nats.StreamConfig{
		Name:     "TEST",
		Subjects: []string{"test"},
	})
	require_NoError(t, err)

	// Create a reply inbox that we can await API requests on.
	// This is instead of using nc.Request().
	inbox := nc.NewRespInbox()
	resp := make(chan *nats.Msg, 1000)
	_, err = nc.ChanSubscribe(inbox, resp)
	require_NoError(t, err)

	// Publish a large number of messages using Core NATS withou
	// waiting for the responses from the API.
	msg := &nats.Msg{
		Subject: "test",
		Reply:   inbox,
	}
	for i := 0; i < 1000; i++ {
		require_NoError(t, nc.PublishMsg(msg))
	}

	// Now sort through the API responses. We're looking for one
	// that tells us that we were rate-limited. If we don't find
	// one then we fail the test.
	var rateLimited bool
	for i, msg := 0, <-resp; i < 1000; i, msg = i+1, <-resp {
		if msg.Header.Get("Status") == "429" {
			rateLimited = true
			break
		}
	}
	require_True(t, rateLimited)
}

func TestJetStreamRateLimitHighStreamIngestDefaults(t *testing.T) {
	s := RunBasicJetStreamServer(t)
	defer s.Shutdown()

	nc, js := jsClientConnect(t, s)
	defer nc.Close()

	_, err := js.AddStream(&nats.StreamConfig{
		Name:     "TEST",
		Subjects: []string{"test"},
	})
	require_NoError(t, err)

	stream, err := s.globalAccount().lookupStream("TEST")
	require_NoError(t, err)

	require_Equal(t, stream.msgs.mlen, streamDefaultMaxQueueMsgs)
	require_Equal(t, stream.msgs.msz, streamDefaultMaxQueueBytes)
}

func TestJetStreamStreamConfigClone(t *testing.T) {
	cfg := &StreamConfig{
		Name:             "name",
		Placement:        &Placement{Cluster: "placement", Tags: []string{"tag"}},
		Mirror:           &StreamSource{Name: "mirror"},
		Sources:          []*StreamSource{&StreamSource{Name: "source"}},
		SubjectTransform: &SubjectTransformConfig{Source: "source", Destination: "dest"},
		RePublish:        &RePublish{Source: "source", Destination: "dest", HeadersOnly: false},
		Metadata:         make(map[string]string),
	}

	// Copy should be complete.
	clone := cfg.clone()
	require_True(t, reflect.DeepEqual(cfg, clone))

	// Changing fields should not update the original.
	clone.Placement.Cluster = "diff"
	require_False(t, reflect.DeepEqual(cfg.Placement, clone.Placement))

	clone.Mirror.Name = "diff"
	require_False(t, reflect.DeepEqual(cfg.Mirror, clone.Mirror))

	clone.Sources[0].Name = "diff"
	require_False(t, reflect.DeepEqual(cfg.Sources, clone.Sources))

	clone.SubjectTransform.Source = "diff"
	require_False(t, reflect.DeepEqual(cfg.SubjectTransform, clone.SubjectTransform))

	clone.RePublish.Source = "diff"
	require_False(t, reflect.DeepEqual(cfg.RePublish, clone.RePublish))

	clone.Metadata["key"] = "value"
	require_False(t, reflect.DeepEqual(cfg.Metadata, clone.Metadata))
}

func TestIsJSONObjectOrArray(t *testing.T) {
	tests := []struct {
		name  string
		data  []byte
		valid bool
	}{
		{"empty", []byte{}, false},
		{"empty_object", []byte("{}"), true},
		{"empty object, not trimmed", []byte("\t\n\r{ }"), true},
		{"empty_array", []byte("[]"), true},
		{"empty array, not trimmed", []byte("\t\n\r[ ]"), true},
		// This is a valid JSON, but it's not a JSON object or array.
		{"empty_string", []byte("\"\""), false},
		{"whitespace_only", []byte("   "), false},
		{"object_with_whitespace", []byte("{   }"), true},
		{"array_with_whitespace", []byte("[   ]"), true},
		{"string_with_whitespace", []byte("   \"text\""), false},
		{"number", []byte("123"), false},
		{"boolean_true", []byte("true"), false},
		{"boolean_false", []byte("false"), false},
		{"null_value", []byte("null"), false}, {"invalid JSON", []byte("invalid"), false},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			require_Equal(t, isJSONObjectOrArray(test.data), test.valid)
		})
	}
}

func TestJetStreamSourcingDontClipStartSeq(t *testing.T) {
	s := RunBasicJetStreamServer(t)
	defer s.Shutdown()

	nc, js := jsClientConnect(t, s)
	defer nc.Close()

	_, err := js.AddStream(&nats.StreamConfig{
		Name:     "ORIGIN",
		Subjects: []string{"test"},
	})
	require_NoError(t, err)

	for i := 0; i < 10; i++ {
		_, err := js.Publish("test", nil)
		require_NoError(t, err)
	}

	_, err = js.AddStream(&nats.StreamConfig{
		Name: "SOURCING",
		Sources: []*nats.StreamSource{
			{
				Name:        "ORIGIN",
				OptStartSeq: 20,
			},
		},
	})
	require_NoError(t, err)

	// Wait for sourcing consumer to be created.
	time.Sleep(time.Second)

	mset, err := s.GlobalAccount().lookupStream("ORIGIN")
	require_NoError(t, err)
	require_True(t, mset != nil)
	require_Len(t, len(mset.consumers), 1)
	for _, o := range mset.consumers {
		// Should NOT have been clipped back to below 20.
		require_Equal(t, o.sseq, 20)
	}
}

func TestJetStreamMirroringClipStartSeq(t *testing.T) {
	s := RunBasicJetStreamServer(t)
	defer s.Shutdown()

	nc, js := jsClientConnect(t, s)
	defer nc.Close()

	_, err := js.AddStream(&nats.StreamConfig{
		Name:     "ORIGIN",
		Subjects: []string{"test"},
	})
	require_NoError(t, err)

	for i := 0; i < 10; i++ {
		_, err := js.Publish("test", nil)
		require_NoError(t, err)
	}

	_, err = js.AddStream(&nats.StreamConfig{
		Name: "MIRRORING",
		Mirror: &nats.StreamSource{
			Name:        "ORIGIN",
			OptStartSeq: 20,
		},
	})
	require_NoError(t, err)

	// Wait for mirroring consumer to be created.
	time.Sleep(time.Second)

	mset, err := s.GlobalAccount().lookupStream("ORIGIN")
	require_NoError(t, err)
	require_True(t, mset != nil)
	require_Len(t, len(mset.consumers), 1)
	for _, o := range mset.consumers {
		// Should NOT have been clipped back to below 20.
		require_Equal(t, o.sseq, 20)
	}
}

func TestJetStreamDelayedAPIResponses(t *testing.T) {
	tdir := t.TempDir()
	tmpl := `
		listen: 127.0.0.1:-1
		%sjetstream: {max_mem_store: 64GB, max_file_store: 10TB, store_dir: %q}
	`
	conf := createConfFile(t, []byte(fmt.Sprintf(tmpl, "", tdir)))

	s, _ := RunServerWithConfig(conf)
	defer s.Shutdown()

	nc := natsConnect(t, s.ClientURL())
	defer nc.Close()

	sub := natsSubSync(t, nc, JSAuditAdvisory)

	acc := s.GlobalAccount()

	// Send B, A, D, C and expected to receive A, B, C, D
	s.sendDelayedAPIErrResponse(nil, acc, "B", _EMPTY_, "request2", "response2", nil, 500*time.Millisecond)
	time.Sleep(50 * time.Millisecond)
	s.sendDelayedAPIErrResponse(nil, acc, "A", _EMPTY_, "request1", "response1", nil, 200*time.Millisecond)
	time.Sleep(50 * time.Millisecond)
	s.sendDelayedAPIErrResponse(nil, acc, "D", _EMPTY_, "request4", "response4", nil, 800*time.Millisecond)
	time.Sleep(50 * time.Millisecond)
	s.sendDelayedAPIErrResponse(nil, acc, "C", _EMPTY_, "request3", "response3", nil, 650*time.Millisecond)

	check := func(req, resp string) {
		t.Helper()
		msg := natsNexMsg(t, sub, time.Second)
		var audit JSAPIAudit
		err := json.Unmarshal(msg.Data, &audit)
		require_NoError(t, err)
		require_Equal(t, audit.Request, req)
		require_Equal(t, audit.Response, resp)
	}
	check("request1", "response1")
	check("request2", "response2")
	check("request3", "response3")
	check("request4", "response4")

	// Verify that if a raft group is canceled, the delayed API response is canceled too.
	node := &raft{quit: make(chan struct{})}
	g := &raftGroup{node: node}
	// Send delayed API response with this raft group
	s.sendDelayedAPIErrResponse(nil, acc, "E", _EMPTY_, "request5", "response5", g, 250*time.Millisecond)
	time.Sleep(50 * time.Millisecond)
	// Send that one without a group
	s.sendDelayedAPIErrResponse(nil, acc, "F", _EMPTY_, "request6", "response6", nil, 400*time.Millisecond)
	// Close the "request5"'s channel.
	close(node.quit)
	// So we should receive request6, not 5.
	check("request6", "response6")

	// Check config reload.
	s.sendDelayedAPIErrResponse(nil, acc, "G", _EMPTY_, "request7", "response7", nil, 400*time.Millisecond)
	time.Sleep(50 * time.Millisecond)
	// Send a bunch more
	for i := 0; i < 10; i++ {
		s.sendDelayedAPIErrResponse(nil, acc, "H", _EMPTY_, "request8", "response8", nil, 500*time.Millisecond)
	}
	// Config reload to disable JS.
	reloadUpdateConfig(t, s, conf, fmt.Sprintf(tmpl, "#", tdir))
	check("request7", "response7")
	// We should not receive more.
	if msg, err := sub.NextMsg(600 * time.Millisecond); err != nats.ErrTimeout {
		t.Fatalf("Did not expect to receive: %s", msg.Data)
	}
	// Check that the queue is empty.
	s.mu.RLock()
	q := s.delayedAPIResponses
	s.mu.RUnlock()
	require_Equal(t, q.len(), 0)

	// Restore JS and check delayed response can be received.
	reloadUpdateConfig(t, s, conf, fmt.Sprintf(tmpl, "", tdir))
	// Wait until js is re-enabled
	checkFor(t, 10*time.Second, 50*time.Millisecond, func() error {
		if s.getJetStream() == nil {
			return ErrJetStreamNotEnabled
		}
		return nil
	})
	s.sendDelayedAPIErrResponse(nil, acc, "I", _EMPTY_, "request9", "response9", nil, 100*time.Millisecond)
	check("request9", "response9")
}

func TestJetStreamMemoryPurgeClearsSubjectsState(t *testing.T) {
	s := RunBasicJetStreamServer(t)
	defer s.Shutdown()

	nc, js := jsClientConnect(t, s)
	defer nc.Close()

	_, err := js.AddStream(&nats.StreamConfig{
		Name:     "TEST",
		Subjects: []string{"foo"},
		Storage:  nats.MemoryStorage,
	})
	require_NoError(t, err)

	pa, err := js.Publish("foo", nil)
	require_NoError(t, err)
	require_Equal(t, pa.Sequence, 1)

	// When requesting stream info, we expect one subject foo with one entry.
	si, err := js.StreamInfo("TEST", &nats.StreamInfoRequest{SubjectsFilter: ">"})
	require_NoError(t, err)
	require_Len(t, len(si.State.Subjects), 1)
	require_Equal(t, si.State.Subjects["foo"], 1)

	// After purging, moving the sequence up, the subjects state should be cleared.
	err = js.PurgeStream("TEST", &nats.StreamPurgeRequest{Sequence: 100})
	require_NoError(t, err)

	si, err = js.StreamInfo("TEST", &nats.StreamInfoRequest{SubjectsFilter: ">"})
	require_NoError(t, err)
	require_Len(t, len(si.State.Subjects), 0)
}

func TestJetStreamWouldExceedLimits(t *testing.T) {
	s := RunBasicJetStreamServer(t)
	defer s.Shutdown()

	js := s.getJetStream()
	require_NotNil(t, js)

	// Storing exactly up to the limit should work.
	require_False(t, js.wouldExceedLimits(MemoryStorage, int(js.config.MaxMemory)))
	require_False(t, js.wouldExceedLimits(FileStorage, int(js.config.MaxStore)))

	// Storing one more than the max should exceed limits.
	require_True(t, js.wouldExceedLimits(MemoryStorage, int(js.config.MaxMemory)+1))
	require_True(t, js.wouldExceedLimits(FileStorage, int(js.config.MaxStore)+1))
}

func TestJetStreamMessageTTL(t *testing.T) {
	for _, storage := range []StorageType{FileStorage, MemoryStorage} {
		t.Run(storage.String(), func(t *testing.T) {
			s := RunBasicJetStreamServer(t)
			defer s.Shutdown()

			nc, js := jsClientConnect(t, s)
			defer nc.Close()

			jsStreamCreate(t, nc, &StreamConfig{
				Name:        "TEST",
				Storage:     storage,
				Subjects:    []string{"test"},
				AllowMsgTTL: true,
			})

			msg := &nats.Msg{
				Subject: "test",
				Header:  nats.Header{},
			}

			for i := 1; i <= 10; i++ {
				msg.Header.Set(JSMessageTTL, "1s")
				_, err := js.PublishMsg(msg)
				require_NoError(t, err)
			}

			si, err := js.StreamInfo("TEST")
			require_NoError(t, err)
			require_Equal(t, si.State.Msgs, 10)
			require_Equal(t, si.State.FirstSeq, 1)
			require_Equal(t, si.State.LastSeq, 10)

			time.Sleep(time.Second * 2)

			si, err = js.StreamInfo("TEST")
			require_NoError(t, err)
			require_Equal(t, si.State.Msgs, 0)
			require_Equal(t, si.State.FirstSeq, 11)
			require_Equal(t, si.State.LastSeq, 10)
		})
	}
}

func TestJetStreamMessageTTLRestart(t *testing.T) {
	s := RunBasicJetStreamServer(t)
	defer s.Shutdown()

	nc, js := jsClientConnect(t, s)
	defer nc.Close()

	jsStreamCreate(t, nc, &StreamConfig{
		Name:        "TEST",
		Storage:     FileStorage,
		Subjects:    []string{"test"},
		AllowMsgTTL: true,
	})

	msg := &nats.Msg{
		Subject: "test",
		Header:  nats.Header{},
	}

	for i := 1; i <= 10; i++ {
		msg.Header.Set(JSMessageTTL, "1s")
		_, err := js.PublishMsg(msg)
		require_NoError(t, err)
	}

	si, err := js.StreamInfo("TEST")
	require_NoError(t, err)
	require_Equal(t, si.State.Msgs, 10)
	require_Equal(t, si.State.FirstSeq, 1)
	require_Equal(t, si.State.LastSeq, 10)

	sd := s.JetStreamConfig().StoreDir
	s.Shutdown()

	s = RunJetStreamServerOnPort(-1, sd)
	defer s.Shutdown()

	nc, js = jsClientConnect(t, s)
	defer nc.Close()

	si, err = js.StreamInfo("TEST")
	require_NoError(t, err)
	require_Equal(t, si.State.Msgs, 10)
	require_Equal(t, si.State.FirstSeq, 1)
	require_Equal(t, si.State.LastSeq, 10)

	time.Sleep(time.Second * 2)

	si, err = js.StreamInfo("TEST")
	require_NoError(t, err)
	require_Equal(t, si.State.Msgs, 0)
	require_Equal(t, si.State.FirstSeq, 11)
	require_Equal(t, si.State.LastSeq, 10)
}

func TestJetStreamMessageTTLRecovered(t *testing.T) {
	s := RunBasicJetStreamServer(t)
	defer s.Shutdown()

	nc, js := jsClientConnect(t, s)
	defer nc.Close()

	jsStreamCreate(t, nc, &StreamConfig{
		Name:        "TEST",
		Storage:     FileStorage,
		Subjects:    []string{"test"},
		AllowMsgTTL: true,
	})

	msg := &nats.Msg{
		Subject: "test",
		Header:  nats.Header{},
	}

	for i := 1; i <= 10; i++ {
		msg.Header.Set(JSMessageTTL, "1s")
		_, err := js.PublishMsg(msg)
		require_NoError(t, err)
	}

	si, err := js.StreamInfo("TEST")
	require_NoError(t, err)
	require_Equal(t, si.State.Msgs, 10)
	require_Equal(t, si.State.FirstSeq, 1)
	require_Equal(t, si.State.LastSeq, 10)

	sd := s.JetStreamConfig().StoreDir
	s.Shutdown()

	fn := filepath.Join(sd, globalAccountName, streamsDir, "TEST", msgDir, ttlStreamStateFile)
	require_NoError(t, os.RemoveAll(fn))

	s = RunJetStreamServerOnPort(-1, sd)
	defer s.Shutdown()

	nc, js = jsClientConnect(t, s)
	defer nc.Close()

	si, err = js.StreamInfo("TEST")
	require_NoError(t, err)
	require_Equal(t, si.State.Msgs, 10)
	require_Equal(t, si.State.FirstSeq, 1)
	require_Equal(t, si.State.LastSeq, 10)

	time.Sleep(time.Second * 2)

	si, err = js.StreamInfo("TEST")
	require_NoError(t, err)
	require_Equal(t, si.State.Msgs, 0)
	require_Equal(t, si.State.FirstSeq, 11)
	require_Equal(t, si.State.LastSeq, 10)
}

func TestJetStreamMessageTTLInvalid(t *testing.T) {
	for _, storage := range []StorageType{FileStorage, MemoryStorage} {
		t.Run(storage.String(), func(t *testing.T) {
			s := RunBasicJetStreamServer(t)
			defer s.Shutdown()

			nc, js := jsClientConnect(t, s)
			defer nc.Close()

			jsStreamCreate(t, nc, &StreamConfig{
				Name:        "TEST",
				Storage:     storage,
				Subjects:    []string{"test"},
				AllowMsgTTL: true,
			})

			msg := &nats.Msg{
				Subject: "test",
				Header:  nats.Header{},
			}

			msg.Header.Set(JSMessageTTL, "500ms")
			_, err := js.PublishMsg(msg)
			require_Error(t, err)

			msg.Header.Set(JSMessageTTL, "something")
			_, err = js.PublishMsg(msg)
			require_Error(t, err)
		})
	}
}

func TestJetStreamMessageTTLNotUpdatable(t *testing.T) {
	s := RunBasicJetStreamServer(t)
	defer s.Shutdown()

	nc, _ := jsClientConnect(t, s)
	defer nc.Close()

	jsStreamCreate(t, nc, &StreamConfig{
		Name:        "TEST",
		Storage:     FileStorage,
		Subjects:    []string{"test"},
		AllowMsgTTL: true,
	})

	_, err := jsStreamUpdate(t, nc, &StreamConfig{
		Name:        "TEST",
		Storage:     FileStorage,
		Subjects:    []string{"test"},
		AllowMsgTTL: false,
	})
	require_Error(t, err)
}

func TestJetStreamMessageTTLNeverExpire(t *testing.T) {
	for _, storage := range []StorageType{FileStorage, MemoryStorage} {
		t.Run(storage.String(), func(t *testing.T) {
			s := RunBasicJetStreamServer(t)
			defer s.Shutdown()

			nc, js := jsClientConnect(t, s)
			defer nc.Close()

			jsStreamCreate(t, nc, &StreamConfig{
				Name:        "TEST",
				Storage:     storage,
				Subjects:    []string{"test"},
				AllowMsgTTL: true,
				MaxAge:      time.Second,
			})

			msg := &nats.Msg{
				Subject: "test",
				Header:  nats.Header{},
			}

			// The first message we publish is set to "never expire", therefore it
			// won't age out with the MaxAge policy.
			msg.Header.Set(JSMessageTTL, "never")
			_, err := js.PublishMsg(msg)
			require_NoError(t, err)

			// Following messages will be published as normal and will age out.
			msg.Header.Del(JSMessageTTL)
			for i := 1; i <= 10; i++ {
				_, err := js.PublishMsg(msg)
				require_NoError(t, err)
			}

			si, err := js.StreamInfo("TEST")
			require_NoError(t, err)
			require_Equal(t, si.State.Msgs, 11)
			require_Equal(t, si.State.FirstSeq, 1)
			require_Equal(t, si.State.LastSeq, 11)

			time.Sleep(time.Second * 2)

			si, err = js.StreamInfo("TEST")
			require_NoError(t, err)
			require_Equal(t, si.State.Msgs, 1)
			require_Equal(t, si.State.FirstSeq, 1)
			require_Equal(t, si.State.LastSeq, 11)
		})
	}
}

func TestJetStreamMessageTTLDisabled(t *testing.T) {
	for _, storage := range []StorageType{FileStorage, MemoryStorage} {
		t.Run(storage.String(), func(t *testing.T) {
			s := RunBasicJetStreamServer(t)
			defer s.Shutdown()

			nc, js := jsClientConnect(t, s)
			defer nc.Close()

			jsStreamCreate(t, nc, &StreamConfig{
				Name:     "TEST",
				Storage:  storage,
				Subjects: []string{"test"},
			})

			msg := &nats.Msg{
				Subject: "test",
				Header:  nats.Header{},
			}

			msg.Header.Set(JSMessageTTL, "1s")
			_, err := js.PublishMsg(msg)
			require_Error(t, err)
		})
	}
}

func TestJetStreamMessageTTLWhenSourcing(t *testing.T) {
	for _, storage := range []StorageType{FileStorage, MemoryStorage} {
		t.Run(storage.String(), func(t *testing.T) {
			s := RunBasicJetStreamServer(t)
			defer s.Shutdown()

			nc, js := jsClientConnect(t, s)
			defer nc.Close()

			_, err := jsStreamCreate(t, nc, &StreamConfig{
				Name:        "Origin",
				Storage:     storage,
				Subjects:    []string{"test"},
				AllowMsgTTL: true,
			})
			require_NoError(t, err)

			_, err = jsStreamCreate(t, nc, &StreamConfig{
				Name:    "TTLEnabled",
				Storage: storage,
				Sources: []*StreamSource{
					{Name: "Origin"},
				},
				AllowMsgTTL: true,
			})
			require_NoError(t, err)

			ttlDisabledConfig := &StreamConfig{
				Name:    "TTLDisabled",
				Storage: storage,
				Sources: []*StreamSource{
					{Name: "Origin"},
				},
				AllowMsgTTL: false,
			}
			_, err = jsStreamCreate(t, nc, ttlDisabledConfig)
			require_NoError(t, err)

			hdr := nats.Header{}
			hdr.Add(JSMessageTTL, "1s")

			_, err = js.PublishMsg(&nats.Msg{
				Subject: "test",
				Header:  hdr,
			})
			require_NoError(t, err)

			for _, stream := range []string{"TTLEnabled", "TTLDisabled"} {
				t.Run(stream, func(t *testing.T) {
					sc, err := js.PullSubscribe("test", "consumer", nats.BindStream(stream))
					require_NoError(t, err)

					msgs, err := sc.Fetch(1)
					require_NoError(t, err)
					require_Len(t, len(msgs), 1)
					require_Equal(t, msgs[0].Header.Get(JSMessageTTL), "1s")

					time.Sleep(time.Second)

					si, err := js.StreamInfo(stream)
					require_NoError(t, err)

					if stream != "TTLDisabled" {
						require_Equal(t, si.State.Msgs, 0)
						return
					}

					require_Equal(t, si.State.Msgs, 1)

					ttlDisabledConfig.AllowMsgTTL = true
					_, err = jsStreamUpdate(t, nc, ttlDisabledConfig)
					require_NoError(t, err)

					si, err = js.StreamInfo(stream)
					require_NoError(t, err)
					require_Equal(t, si.State.Msgs, 0)
				})
			}
		})
	}
}

func TestJetStreamMessageTTLWhenMirroring(t *testing.T) {
	for _, storage := range []StorageType{FileStorage, MemoryStorage} {
		t.Run(storage.String(), func(t *testing.T) {
			s := RunBasicJetStreamServer(t)
			defer s.Shutdown()

			nc, js := jsClientConnect(t, s)
			defer nc.Close()

			_, err := jsStreamCreate(t, nc, &StreamConfig{
				Name:        "Origin",
				Storage:     storage,
				Subjects:    []string{"test"},
				AllowMsgTTL: true,
			})
			require_NoError(t, err)

			_, err = jsStreamCreate(t, nc, &StreamConfig{
				Name:    "TTLEnabled",
				Storage: storage,
				Mirror: &StreamSource{
					Name: "Origin",
				},
				AllowMsgTTL: true,
			})
			require_NoError(t, err)

			ttlDisabledConfig := &StreamConfig{
				Name:    "TTLDisabled",
				Storage: storage,
				Mirror: &StreamSource{
					Name: "Origin",
				},
				AllowMsgTTL: false,
			}
			_, err = jsStreamCreate(t, nc, ttlDisabledConfig)
			require_NoError(t, err)

			hdr := nats.Header{}
			hdr.Add(JSMessageTTL, "1s")

			_, err = js.PublishMsg(&nats.Msg{
				Subject: "test",
				Header:  hdr,
			})
			require_NoError(t, err)

			for _, stream := range []string{"TTLEnabled", "TTLDisabled"} {
				t.Run(stream, func(t *testing.T) {
					sc, err := js.PullSubscribe("test", "consumer", nats.BindStream(stream))
					require_NoError(t, err)

					msgs, err := sc.Fetch(1)
					require_NoError(t, err)
					require_Len(t, len(msgs), 1)
					require_Equal(t, msgs[0].Header.Get(JSMessageTTL), "1s")

					time.Sleep(time.Second)

					si, err := js.StreamInfo(stream)
					require_NoError(t, err)
					if stream != "TTLDisabled" {
						require_Equal(t, si.State.Msgs, 0)
						return
					}

					require_Equal(t, si.State.Msgs, 1)

					ttlDisabledConfig.AllowMsgTTL = true
					_, err = jsStreamUpdate(t, nc, ttlDisabledConfig)
					require_NoError(t, err)

					si, err = js.StreamInfo(stream)
					require_NoError(t, err)
					require_Equal(t, si.State.Msgs, 0)
				})
			}
		})
	}
}

func TestJetStreamSubjectDeleteMarkers(t *testing.T) {
	for _, storage := range []StorageType{FileStorage, MemoryStorage} {
		t.Run(storage.String(), func(t *testing.T) {
			s := RunBasicJetStreamServer(t)
			defer s.Shutdown()

			nc, js := jsClientConnect(t, s)
			defer nc.Close()

			jsStreamCreate(t, nc, &StreamConfig{
				Name:                   "TEST",
				Storage:                storage,
				Subjects:               []string{"test"},
				MaxAge:                 time.Second,
				AllowMsgTTL:            true,
				SubjectDeleteMarkerTTL: time.Second,
			})

			sub, err := js.SubscribeSync("test")
			require_NoError(t, err)

			for i := 0; i < 3; i++ {
				_, err = js.Publish("test", nil)
				require_NoError(t, err)
			}

			for i := 0; i < 3; i++ {
				msg, err := sub.NextMsg(time.Second)
				require_NoError(t, err)
				require_NoError(t, msg.AckSync())
			}

			msg, err := sub.NextMsg(time.Second * 10)
			require_NoError(t, err)
			require_Equal(t, msg.Header.Get(JSMarkerReason), "MaxAge")
			require_Equal(t, msg.Header.Get(JSMessageTTL), "1s")
		})
	}
}

func TestJetStreamSubjectDeleteMarkersAfterRestart(t *testing.T) {
	s := RunBasicJetStreamServer(t)
	defer s.Shutdown()

	nc, js := jsClientConnect(t, s)
	defer nc.Close()

	_, err := jsStreamCreate(t, nc, &StreamConfig{
		Name:                   "TEST",
		Storage:                FileStorage,
		Subjects:               []string{"test"},
		MaxAge:                 time.Second,
		AllowMsgTTL:            true,
		SubjectDeleteMarkerTTL: time.Second,
	})
	require_NoError(t, err)

	_, err = js.AddConsumer("TEST", &nats.ConsumerConfig{
		Name:      "test_consumer",
		AckPolicy: nats.AckExplicitPolicy,
	})
	require_NoError(t, err)

	for i := 0; i < 3; i++ {
		_, err = js.Publish("test", nil)
		require_NoError(t, err)
	}

	sd := s.JetStreamConfig().StoreDir
	s.Shutdown()

	s = RunJetStreamServerOnPort(-1, sd)
	defer s.Shutdown()

	nc, js = jsClientConnect(t, s)
	defer nc.Close()

	sub, err := js.PullSubscribe("test", _EMPTY_, nats.Bind("TEST", "test_consumer"))
	require_NoError(t, err)

	for i := 0; i < 3; i++ {
		msgs, err := sub.Fetch(1)
		require_NoError(t, err)
		require_Len(t, len(msgs), 1)
		require_NoError(t, msgs[0].AckSync())
	}

	msgs, err := sub.Fetch(1)
	require_NoError(t, err)
	require_Len(t, len(msgs), 1)
	require_Equal(t, msgs[0].Header.Get(JSMarkerReason), "MaxAge")
	require_Equal(t, msgs[0].Header.Get(JSMessageTTL), "1s")
}

func TestJetStreamSubjectDeleteMarkersTTLRollupWithMaxAge(t *testing.T) {
	s := RunBasicJetStreamServer(t)
	defer s.Shutdown()

	nc, js := jsClientConnect(t, s)
	defer nc.Close()

	jsStreamCreate(t, nc, &StreamConfig{
		Name:                   "TEST",
		Storage:                FileStorage,
		Subjects:               []string{"test"},
		MaxAge:                 time.Second,
		AllowMsgTTL:            true,
		AllowRollup:            true,
		SubjectDeleteMarkerTTL: time.Second,
	})

	sub, err := js.SubscribeSync("test")
	require_NoError(t, err)

	for i := 0; i < 3; i++ {
		_, err = js.Publish("test", nil)
		require_NoError(t, err)
	}

	for i := 0; i < 3; i++ {
		msg, err := sub.NextMsg(time.Second)
		require_NoError(t, err)
		require_NoError(t, msg.AckSync())
	}

	rh := nats.Header{}
	rh.Set(JSMessageTTL, "2s") // MaxAge will get here first.
	rh.Set(JSMsgRollup, JSMsgRollupSubject)
	_, err = js.PublishMsg(&nats.Msg{
		Subject: "test",
		Header:  rh,
	})
	require_NoError(t, err)

	// Expect to only have the rollup message here.
	si, err := js.StreamInfo("TEST")
	require_NoError(t, err)
	require_Equal(t, si.State.Msgs, 1)
	require_Equal(t, si.State.FirstSeq, 4)

	msg, err := sub.NextMsg(time.Second * 10)
	require_NoError(t, err)
	require_Equal(t, msg.Header.Get(JSMsgRollup), JSMsgRollupSubject)
	require_Equal(t, msg.Header.Get(JSMessageTTL), "2s")
	meta, err := msg.Metadata()
	require_NoError(t, err)
	require_NoError(t, msg.AckSync())

	msg, err = sub.NextMsg(time.Second * 10)
	require_NoError(t, err)
	require_LessThan(t, time.Second, time.Since(meta.Timestamp))
	require_Equal(t, msg.Header.Get(JSMarkerReason), "MaxAge")
	require_Equal(t, msg.Header.Get(JSMessageTTL), "1s")
}

func TestJetStreamSubjectDeleteMarkersTTLRollupWithoutMaxAge(t *testing.T) {
	s := RunBasicJetStreamServer(t)
	defer s.Shutdown()

	nc, js := jsClientConnect(t, s)
	defer nc.Close()

	_, err := jsStreamCreate(t, nc, &StreamConfig{
		Name:                   "TEST",
		Storage:                FileStorage,
		Subjects:               []string{"test"},
		AllowMsgTTL:            true,
		AllowRollup:            true,
		SubjectDeleteMarkerTTL: time.Second,
	})
	require_NoError(t, err)

	sub, err := js.SubscribeSync("test")
	require_NoError(t, err)

	for i := 0; i < 3; i++ {
		_, err = js.Publish("test", nil)
		require_NoError(t, err)
	}

	for i := 0; i < 3; i++ {
		msg, err := sub.NextMsg(time.Second)
		require_NoError(t, err)
		require_NoError(t, msg.AckSync())
	}

	rh := nats.Header{}
	rh.Set(JSMessageTTL, "1s")
	rh.Set(JSMsgRollup, JSMsgRollupSubject)
	_, err = js.PublishMsg(&nats.Msg{
		Subject: "test",
		Header:  rh,
	})
	require_NoError(t, err)

	// Expect to only have the rollup message here.
	si, err := js.StreamInfo("TEST")
	require_NoError(t, err)
	require_Equal(t, si.State.Msgs, 1)
	require_Equal(t, si.State.FirstSeq, 4)

	msg, err := sub.NextMsg(time.Second * 10)
	require_NoError(t, err)
	require_Equal(t, msg.Header.Get(JSMsgRollup), JSMsgRollupSubject)
	require_Equal(t, msg.Header.Get(JSMessageTTL), "1s")
	require_NoError(t, msg.AckSync())

	// Wait for the rollup message to hit the TTL.
	time.Sleep(2500 * time.Millisecond)

	// Now it should be gone, and it will have been replaced with a
	// subject delete marker (which is also gone by now).
	si, err = js.StreamInfo("TEST")
	require_NoError(t, err)
	require_Equal(t, si.State.Msgs, 0)
	require_Equal(t, si.State.FirstSeq, 6)
}

func TestJetStreamSubjectDeleteMarkersWithMirror(t *testing.T) {
	s := RunBasicJetStreamServer(t)
	defer s.Shutdown()

	nc, _ := jsClientConnect(t, s)
	defer nc.Close()

	_, err := jsStreamCreate(t, nc, &StreamConfig{
		Name:     "Origin",
		Storage:  FileStorage,
		Subjects: []string{"test"},
		MaxAge:   time.Second,
	})
	require_NoError(t, err)

	_, err = jsStreamCreate(t, nc, &StreamConfig{
		Name:                   "Mirror",
		Storage:                FileStorage,
		AllowMsgTTL:            true,
		SubjectDeleteMarkerTTL: time.Second,
		Mirror: &StreamSource{
			Name: "Origin",
		},
	})
	require_Error(t, err)
}

// https://github.com/nats-io/nats-server/issues/6538
func TestJetStreamInterestMaxDeliveryReached(t *testing.T) {
	maxWait := 250 * time.Millisecond
	for _, useNak := range []bool{true, false} {
		for _, test := range []struct {
			title  string
			action func(s *Server, sub *nats.Subscription)
		}{
			{
				title: "fetch",
				action: func(s *Server, sub *nats.Subscription) {
					time.Sleep(time.Second)

					// max deliver 1 so this will fail
					_, err := sub.Fetch(1, nats.MaxWait(maxWait))
					require_Error(t, err)
				},
			},
			{
				title: "expire pending",
				action: func(s *Server, sub *nats.Subscription) {
					acc, err := s.lookupAccount(globalAccountName)
					require_NoError(t, err)
					mset, err := acc.lookupStream("TEST")
					require_NoError(t, err)
					o := mset.lookupConsumer("consumer")
					require_NotNil(t, o)

					o.mu.Lock()
					o.forceExpirePending()
					o.mu.Unlock()
				},
			},
		} {
			title := fmt.Sprintf("nak/%s", test.title)
			if !useNak {
				title = fmt.Sprintf("no-%s", title)
			}
			t.Run(title, func(t *testing.T) {
				s := RunBasicJetStreamServer(t)
				defer s.Shutdown()

				nc, js := jsClientConnect(t, s)
				defer nc.Close()

				_, err := js.AddStream(&nats.StreamConfig{
					Name:      "TEST",
					Storage:   nats.FileStorage,
					Subjects:  []string{"test"},
					Replicas:  1,
					Retention: nats.InterestPolicy,
				})
				require_NoError(t, err)

				sub, err := js.PullSubscribe("test", "consumer", nats.AckWait(time.Second), nats.MaxDeliver(1))
				require_NoError(t, err)

				_, err = nc.Request("test", []byte("hello"), maxWait)
				require_NoError(t, err)

				nfo, err := js.StreamInfo("TEST")
				require_NoError(t, err)
				require_Equal(t, nfo.State.Msgs, uint64(1))

				msg, err := sub.Fetch(1, nats.MaxWait(maxWait))
				require_NoError(t, err)
				require_Len(t, 1, len(msg))
				if useNak {
					require_NoError(t, msg[0].Nak())
				}

				cnfo, err := js.ConsumerInfo("TEST", "consumer")
				require_NoError(t, err)
				require_Equal(t, cnfo.NumAckPending, 1)

				test.action(s, sub)

				// max deliver 1 so this will fail
				_, err = sub.Fetch(1, nats.MaxWait(maxWait))
				require_Error(t, err)

				cnfo, err = js.ConsumerInfo("TEST", "consumer")
				require_NoError(t, err)
				require_Equal(t, cnfo.NumAckPending, 0)

				nfo, err = js.StreamInfo("TEST")
				require_NoError(t, err)
				require_Equal(t, nfo.State.Msgs, uint64(1))

				sub2, err := js.PullSubscribe("test", "consumer2")
				require_NoError(t, err)

				msg, err = sub2.Fetch(1)
				require_NoError(t, err)
				require_Len(t, 1, len(msg))
				require_NoError(t, msg[0].AckSync())

				nfo, err = js.StreamInfo("TEST")
				require_NoError(t, err)
				require_Equal(t, nfo.State.Msgs, uint64(1))
			})
		}
	}
}

// https://github.com/nats-io/nats-server/issues/7817
func TestJetStreamWQMaxDeliveryReached(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "R3S", 3)
	defer c.shutdown()

	nc, js := jsClientConnect(t, c.randomServer())
	defer nc.Close()

	_, err := js.AddStream(&nats.StreamConfig{
		Name:      "TEST",
		Subjects:  []string{"foo"},
		Replicas:  3,
		Retention: nats.WorkQueuePolicy,
	})
	require_NoError(t, err)

	for range 3 {
		_, err = js.Publish("foo", nil)
		require_NoError(t, err)
	}

	sub, err := js.PullSubscribe(_EMPTY_, "CONSUMER",
		nats.BindStream("TEST"),
		nats.MaxDeliver(2),
		nats.AckExplicit(),
		nats.AckWait(200*time.Millisecond),
	)
	require_NoError(t, err)
	defer sub.Drain()

	msgs, err := sub.Fetch(3, nats.MaxWait(time.Second))
	require_NoError(t, err)
	require_Len(t, len(msgs), 3)

	msgs, err = sub.Fetch(3, nats.MaxWait(time.Second))
	require_NoError(t, err)
	require_Len(t, len(msgs), 3)

	_, err = sub.Fetch(1, nats.MaxWait(500*time.Millisecond))
	require_Error(t, err, nats.ErrTimeout)

	_, err = js.Publish("foo", nil)
	require_NoError(t, err)
	msgs, err = sub.Fetch(1, nats.MaxWait(500*time.Millisecond))
	require_NoError(t, err)
	require_Len(t, len(msgs), 1)
	require_NoError(t, msgs[0].AckSync())

	cl := c.consumerLeader(globalAccountName, "TEST", "CONSUMER")
	require_NotNil(t, cl)
	mset, err := cl.globalAccount().lookupStream("TEST")
	require_NoError(t, err)
	o := mset.lookupConsumer("CONSUMER")
	require_NotNil(t, o)
	o.mu.RLock()
	pending, rdc, adflr, asflr := len(o.pending), len(o.rdc), o.adflr, o.asflr
	o.mu.RUnlock()
	require_Equal(t, pending, 0)
	require_Equal(t, rdc, 3)
	require_Equal(t, adflr, 7)
	require_Equal(t, asflr, 4)

	mset.checkInterestState()
	time.Sleep(200 * time.Millisecond)
	checkFor(t, 2*time.Second, 200*time.Millisecond, func() error {
		return checkState(t, c, globalAccountName, "TEST")
	})

	sm, err := mset.store.LoadMsg(1, nil)
	require_NoError(t, err)
	require_Equal(t, sm.subj, "foo")
}

// https://github.com/nats-io/nats-server/issues/6874
func TestJetStreamMaxDeliveryRedeliveredReporting(t *testing.T) {
	s := RunBasicJetStreamServer(t)
	defer s.Shutdown()

	nc, js := jsClientConnect(t, s)
	defer nc.Close()

	_, err := js.AddStream(&nats.StreamConfig{
		Name:      "TEST",
		Storage:   nats.FileStorage,
		Subjects:  []string{"foo"},
		Replicas:  1,
		Retention: nats.LimitsPolicy,
	})
	require_NoError(t, err)

	_, err = js.Publish("foo", nil)
	require_NoError(t, err)

	maxWait := 250 * time.Millisecond
	cfg := &nats.ConsumerConfig{
		Durable:    "CONSUMER",
		AckPolicy:  nats.AckExplicitPolicy,
		AckWait:    maxWait,
		MaxDeliver: 1,
	}
	_, err = js.AddConsumer("TEST", cfg)
	require_NoError(t, err)

	sub, err := js.PullSubscribe(_EMPTY_, "CONSUMER", nats.BindStream("TEST"))
	require_NoError(t, err)

	nfo, err := js.StreamInfo("TEST")
	require_NoError(t, err)
	require_Equal(t, nfo.State.Msgs, uint64(1))

	msgs, err := sub.Fetch(1, nats.MaxWait(maxWait))
	require_NoError(t, err)
	require_Len(t, 1, len(msgs))

	cnfo, err := js.ConsumerInfo("TEST", "CONSUMER")
	require_NoError(t, err)
	require_Equal(t, cnfo.NumAckPending, 1)
	require_Equal(t, cnfo.NumRedelivered, 0)

	time.Sleep(2 * maxWait)

	// Max deliver 1 so this will fail.
	_, err = sub.Fetch(1, nats.MaxWait(maxWait))
	require_Error(t, err)

	// Redelivered should remain 0, as it doesn't get redelivered with MaxDeliver 1.
	cnfo, err = js.ConsumerInfo("TEST", "CONSUMER")
	require_NoError(t, err)
	require_Equal(t, cnfo.NumAckPending, 0)
	require_Equal(t, cnfo.NumRedelivered, 0)

	// With a higher MaxDeliver we should report it.
	cfg.MaxDeliver = 2
	_, err = js.UpdateConsumer("TEST", cfg)
	require_NoError(t, err)

	cnfo, err = js.ConsumerInfo("TEST", "CONSUMER")
	require_NoError(t, err)
	require_Equal(t, cnfo.NumAckPending, 0)
	require_Equal(t, cnfo.NumRedelivered, 1)

	// Unset should also report.
	cfg.MaxDeliver = -1
	_, err = js.UpdateConsumer("TEST", cfg)
	require_NoError(t, err)

	cnfo, err = js.ConsumerInfo("TEST", "CONSUMER")
	require_NoError(t, err)
	require_Equal(t, cnfo.NumAckPending, 0)
	require_Equal(t, cnfo.NumRedelivered, 1)
}

func TestJetStreamRecoversStreamFirstSeqWhenNotEmpty(t *testing.T) {
	s := RunBasicJetStreamServer(t)
	defer s.Shutdown()

	nc, js := jsClientConnect(t, s)
	defer nc.Close()

	_, err := js.AddStream(&nats.StreamConfig{
		Name:      "TEST",
		Storage:   nats.FileStorage,
		Subjects:  []string{"test"},
		Retention: nats.InterestPolicy,
	})
	require_NoError(t, err)

	_, err = js.AddConsumer("TEST", &nats.ConsumerConfig{
		Name:          "CONSUMER",
		FilterSubject: "test",
		AckPolicy:     nats.AckExplicitPolicy,
	})
	require_NoError(t, err)

	for i := 0; i < 1000; i++ {
		_, err = js.Publish("test", nil)
		require_NoError(t, err)
	}

	si, err := js.StreamInfo("TEST")
	require_NoError(t, err)
	require_Equal(t, si.State.Msgs, 1000)
	require_Equal(t, si.State.FirstSeq, 1)
	require_Equal(t, si.State.LastSeq, 1000)

	ps, err := js.PullSubscribe("test", "", nats.Bind("TEST", "CONSUMER"))
	require_NoError(t, err)

	for i := 0; i < 500; i++ {
		msgs, err := ps.Fetch(1)
		require_NoError(t, err)
		require_Len(t, len(msgs), 1)
		require_NoError(t, msgs[0].AckSync())
	}

	ci, err := js.ConsumerInfo("TEST", "CONSUMER")
	require_NoError(t, err)
	require_Equal(t, ci.AckFloor.Stream, 500)

	s.shutdownJetStream()

	path := filepath.Join(s.opts.StoreDir, "jetstream", globalAccountName, "streams", "TEST", msgDir)
	dir, err := os.ReadDir(path)
	require_NoError(t, err)

	// Mangle the last message in the block so that it fails the checksum comparison
	// with index.db, which causes us to throw the prior state error and rebuild.
	// Once this is done we should still be able to figure out what the previous first
	// and last sequence were, even without messages.
	for _, f := range dir {
		if !strings.HasSuffix(f.Name(), ".blk") {
			continue
		}
		st, err := f.Info()
		require_NoError(t, err)
		require_NoError(t, os.Truncate(filepath.Join(path, f.Name()), st.Size()-1))
	}

	require_NoError(t, s.restartJetStream())

	si, err = js.StreamInfo("TEST")
	require_NoError(t, err)
	require_Equal(t, si.State.Msgs, 500)
	require_Equal(t, si.State.FirstSeq, 501)
	require_Equal(t, si.State.LastSeq, 1000)
}

func TestJetStreamRecoversStreamFirstSeqWhenEmpty(t *testing.T) {
	s := RunBasicJetStreamServer(t)
	defer s.Shutdown()

	nc, js := jsClientConnect(t, s)
	defer nc.Close()

	_, err := js.AddStream(&nats.StreamConfig{
		Name:      "TEST",
		Storage:   nats.FileStorage,
		Subjects:  []string{"test"},
		Retention: nats.InterestPolicy,
	})
	require_NoError(t, err)

	_, err = js.AddConsumer("TEST", &nats.ConsumerConfig{Durable: "CONSUMER"})
	require_NoError(t, err)

	for i := 0; i < 1000; i++ {
		_, err = js.Publish("test", nil)
		require_NoError(t, err)
	}

	require_NoError(t, js.PurgeStream("TEST"))

	si, err := js.StreamInfo("TEST")
	require_NoError(t, err)
	require_Equal(t, si.State.Msgs, 0)
	require_Equal(t, si.State.FirstSeq, 1001)
	require_Equal(t, si.State.LastSeq, 1000)

	ci, err := js.ConsumerInfo("TEST", "CONSUMER")
	require_NoError(t, err)
	require_Equal(t, ci.AckFloor.Stream, 1000)

	s.shutdownJetStream()

	path := filepath.Join(s.opts.StoreDir, "jetstream", globalAccountName, "streams", "TEST", msgDir)
	dir, err := os.ReadDir(path)
	require_NoError(t, err)

	// Mangle the last message in the block so that it fails the checksum comparison
	// with index.db, which causes us to throw the prior state error and rebuild.
	// Once this is done we should still be able to figure out what the previous first
	// and last sequence were, even without messages.
	for _, f := range dir {
		if !strings.HasSuffix(f.Name(), ".blk") {
			continue
		}
		st, err := f.Info()
		require_NoError(t, err)
		require_NoError(t, os.Truncate(filepath.Join(path, f.Name()), st.Size()-1))
	}

	s.restartJetStream()

	si, err = js.StreamInfo("TEST")
	require_NoError(t, err)
	require_Equal(t, si.State.Msgs, 0)
	require_Equal(t, si.State.FirstSeq, 1001)
	require_Equal(t, si.State.LastSeq, 1000)
}

func TestJetStreamUpgradeStreamVersioning(t *testing.T) {
	s := RunBasicJetStreamServer(t)
	defer s.Shutdown()

	nc, js := jsClientConnect(t, s)
	defer nc.Close()

	acc, err := s.lookupAccount(globalAccountName)
	require_NoError(t, err)

	// Create stream config.
	cfg, apiErr := s.checkStreamCfg(&StreamConfig{Name: "TEST", Subjects: []string{"foo"}}, acc, false)
	require_True(t, apiErr == nil)

	// Create stream.
	mset, err := acc.addStream(&cfg)
	require_NoError(t, err)
	require_True(t, mset.cfg.Metadata == nil)

	for _, create := range []bool{true, false} {
		title := "create"
		if !create {
			title = "update"
		}
		t.Run(title, func(t *testing.T) {
			if create {
				// Create on 2.11+ should be idempotent with previous create on 2.10-.
				mcfg := &StreamConfig{}
				setStaticStreamMetadata(mcfg)
				si, err := js.AddStream(&nats.StreamConfig{
					Name:     "TEST",
					Subjects: []string{"foo"},
					Metadata: setDynamicStreamMetadata(mcfg).Metadata,
				})
				require_NoError(t, err)
				deleteDynamicMetadata(si.Config.Metadata)
				require_Len(t, len(si.Config.Metadata), 0)
			} else {
				// Update populates the versioning metadata.
				si, err := js.UpdateStream(&nats.StreamConfig{Name: "TEST", Subjects: []string{"foo"}})
				require_NoError(t, err)
				require_Len(t, len(si.Config.Metadata), 3)
			}
		})
	}
}

func TestJetStreamUpgradeConsumerVersioning(t *testing.T) {
	s := RunBasicJetStreamServer(t)
	defer s.Shutdown()

	nc, js := jsClientConnect(t, s)
	defer nc.Close()

	_, err := js.AddStream(&nats.StreamConfig{
		Name:      "TEST",
		Subjects:  []string{"foo"},
		Retention: nats.LimitsPolicy,
		Replicas:  1,
	})
	require_NoError(t, err)

	acc, err := s.lookupAccount(globalAccountName)
	require_NoError(t, err)
	mset, err := acc.lookupStream("TEST")
	require_NoError(t, err)

	// Create consumer config.
	cfg := &ConsumerConfig{Durable: "CONSUMER", Name: "CONSUMER"}
	selectedLimits, _, _, apiErr := acc.selectLimits(cfg.replicas(&mset.cfg))
	if apiErr != nil {
		require_NoError(t, apiErr)
	}
	srvLim := &s.getOpts().JetStreamLimits
	apiErr = setConsumerConfigDefaults(cfg, &mset.cfg, srvLim, selectedLimits, false)
	if apiErr != nil {
		require_NoError(t, apiErr)
	}

	// Create consumer.
	_, err = mset.addConsumer(cfg)
	require_NoError(t, err)

	for _, create := range []bool{true, false} {
		title := "create"
		if !create {
			title = "update"
		}
		t.Run(title, func(t *testing.T) {
			createConsumerRequest := func(obsReq CreateConsumerRequest) (*JSApiConsumerInfoResponse, error) {
				req, err := json.Marshal(obsReq)
				if err != nil {
					return nil, err
				}
				msg, err := nc.Request(fmt.Sprintf(JSApiDurableCreateT, "TEST", "CONSUMER"), req, time.Second)
				if err != nil {
					return nil, err
				}
				var resp JSApiConsumerInfoResponse
				require_NoError(t, json.Unmarshal(msg.Data, &resp))
				if resp.Error != nil {
					return nil, resp.Error
				}
				return &resp, nil
			}

			if create {
				// Create on 2.11+ should be idempotent with previous create on 2.10-.
				ncfg := &ConsumerConfig{Durable: "CONSUMER"}
				setStaticConsumerMetadata(ncfg)
				obsReq := CreateConsumerRequest{
					Stream: "TEST",
					Config: *setDynamicConsumerMetadata(ncfg),
					Action: ActionCreate,
				}
				resp, err := createConsumerRequest(obsReq)
				require_NoError(t, err)
				deleteDynamicMetadata(resp.Config.Metadata)
				require_Len(t, len(resp.Config.Metadata), 0)
			} else {
				// Update populates the versioning metadata.
				obsReq := CreateConsumerRequest{
					Stream: "TEST",
					Config: ConsumerConfig{Durable: "CONSUMER"},
					Action: ActionUpdate,
				}
				resp, err := createConsumerRequest(obsReq)
				require_NoError(t, err)
				require_Len(t, len(resp.Config.Metadata), 3)
			}
		})
	}
}

func TestJetStreamMirrorCrossAccountWithFilteredSubjectAndSubjectTransform(t *testing.T) {
	conf := createConfFile(t, fmt.Appendf(nil, `
		listen: "127.0.0.1:-1"
		jetstream: {store_dir: %q}
		accounts: {
			PUBLIC_ACCOUNT: {
				jetstream: enabled
				exports: [
					{ service: "$JS.API.CONSUMER.CREATE.S.*.public.a", response_type: stream, accounts: ["INTERNAL_ACCOUNT"] },
					{ service: "$JS.API.CONSUMER.CREATE.S.*.public.b", response_type: stream, accounts: ["INTERNAL_ACCOUNT"] },
					{ service: "$JS.API.CONSUMER.CREATE.S.*.public.c", response_type: stream, accounts: ["INTERNAL_ACCOUNT"] },
					{ service: "$JS.FC.>" },
					{ stream: "shared.public.>", accounts: ["INTERNAL_ACCOUNT"] }
				]
				users: [ {user: "public", password: "pwd"} ]
			},

			INTERNAL_ACCOUNT: {
				jetstream: enabled
				imports: [
					{ service: { account: "PUBLIC_ACCOUNT", subject: "$JS.API.CONSUMER.CREATE.S.*.public.a" }, to: "JS.PUBLIC_ACCOUNT.CONSUMER.CREATE.S.*.public.a" },
					{ service: { account: "PUBLIC_ACCOUNT", subject: "$JS.API.CONSUMER.CREATE.S.*.public.b" }, to: "JS.PUBLIC_ACCOUNT.CONSUMER.CREATE.S.*.public.b" },
					{ service: { account: "PUBLIC_ACCOUNT", subject: "$JS.API.CONSUMER.CREATE.S.*.public.c" }, to: "JS.PUBLIC_ACCOUNT.CONSUMER.CREATE.S.*.public.c" },
					{ service: { account: "PUBLIC_ACCOUNT", subject: "$JS.FC.>" }, to: "$JS.FC.>" },
					{ stream: { account: "PUBLIC_ACCOUNT", subject: "shared.public.>" }, to: "shared.public.>" }
				]
				users: [ {user: "internal", password: "pwd"} ]
			}
		}
	`, t.TempDir()))
	s, _ := RunServerWithConfig(conf)
	defer s.Shutdown()

	pc, pjs := jsClientConnect(t, s, nats.UserInfo("public", "pwd"))
	defer pc.Close()

	_, err := pjs.AddStream(&nats.StreamConfig{
		Name:     "S",
		Subjects: []string{"public.>"},
	})
	require_NoError(t, err)

	ic, ijs := jsClientConnect(t, s, nats.UserInfo("internal", "pwd"))
	defer ic.Close()

	addStream := func(name, fs string, transform *nats.SubjectTransformConfig) {
		t.Helper()
		sc := &nats.StreamConfig{
			Name: name,
			Mirror: &nats.StreamSource{
				Name: "S",
				External: &nats.ExternalStream{
					APIPrefix:     "JS.PUBLIC_ACCOUNT",
					DeliverPrefix: "shared.public",
				},
			},
		}
		if fs != _EMPTY_ {
			sc.Mirror.FilterSubject = fs
		} else {
			sc.Mirror.SubjectTransforms = append(sc.Mirror.SubjectTransforms, *transform)
		}
		_, err = ijs.AddStream(sc)
		require_NoError(t, err)
	}

	checkMirror := func(name string, expected int) {
		t.Helper()
		checkFor(t, time.Second, 15*time.Millisecond, func() error {
			si, err := ijs.StreamInfo(name)
			if err != nil {
				return err
			}
			if n := int(si.State.Msgs); n != expected {
				return fmt.Errorf("Expected %v mirrored message, got %v", expected, n)
			}
			return nil
		})
	}

	// Just a subject filter
	addStream("M1", "public.a", nil)
	natsPub(t, pc, "public.a", []byte("hello"))
	checkMirror("M1", 1)

	// Now try with equivalent subject transform (no destination).
	addStream("M2", _EMPTY_, &nats.SubjectTransformConfig{Source: "public.b"})
	natsPub(t, pc, "public.b", []byte("hello"))
	checkMirror("M2", 1)

	// And now with a transform destination.
	addStream("M3", _EMPTY_, &nats.SubjectTransformConfig{Source: "public.c", Destination: "public.d"})
	natsPub(t, pc, "public.c", []byte("hello"))
	checkMirror("M3", 1)

	checkMsg := func(stream, subj string, seq uint64) {
		t.Helper()
		msg, err := ijs.GetMsg(stream, seq)
		require_NoError(t, err)
		require_Equal(t, msg.Subject, subj)
	}
	checkMsg("M1", "public.a", 1)
	checkMsg("M2", "public.b", 2)
	checkMsg("M3", "public.d", 3)

	ic.Close()
	pc.Close()
	s.Shutdown()
	s, _ = RunServerWithConfig(conf)
	defer s.Shutdown()

	ic, ijs = jsClientConnect(t, s, nats.UserInfo("internal", "pwd"))
	defer ic.Close()

	checkMirror("M1", 1)
	checkMirror("M2", 1)
	checkMirror("M3", 1)
	checkMsg("M1", "public.a", 1)
	checkMsg("M2", "public.b", 2)
	checkMsg("M3", "public.d", 3)

	pc, _ = jsClientConnect(t, s, nats.UserInfo("public", "pwd"))
	defer pc.Close()
	natsPub(t, pc, "public.a", []byte("hello"))
	natsPub(t, pc, "public.b", []byte("hello"))
	natsPub(t, pc, "public.c", []byte("hello"))
	natsFlush(t, pc)

	checkMirror("M1", 2)
	checkMirror("M2", 2)
	checkMirror("M3", 2)
	checkMsg("M1", "public.a", 1)
	checkMsg("M2", "public.b", 2)
	checkMsg("M3", "public.d", 3)
	checkMsg("M1", "public.a", 4)
	checkMsg("M2", "public.b", 5)
	checkMsg("M3", "public.d", 6)
}

func TestJetStreamFileStoreFirstSeqAfterRestart(t *testing.T) {
	s := RunBasicJetStreamServer(t)
	defer s.Shutdown()

	nc, js := jsClientConnect(t, s)
	defer nc.Close()

	// Create a stream with a first sequence.
	fseq := uint64(10_000)
	si, err := js.AddStream(&nats.StreamConfig{
		Name:      "TEST",
		Subjects:  []string{"foo"},
		Storage:   nats.FileStorage,
		Retention: nats.LimitsPolicy,
		FirstSeq:  fseq,
	})
	require_NoError(t, err)
	require_Equal(t, si.State.FirstSeq, fseq)
	require_Equal(t, si.State.LastSeq, fseq-1)

	// Publish one message to have some data in the stream.
	pubAck, err := js.Publish("foo", nil)
	require_NoError(t, err)
	require_Equal(t, pubAck.Sequence, fseq)

	// Confirm initial stream state.
	si, err = js.StreamInfo("TEST")
	require_NoError(t, err)
	require_Equal(t, si.State.Msgs, 1)
	require_Equal(t, si.State.FirstSeq, fseq)
	require_Equal(t, si.State.LastSeq, fseq)

	// Restart the server.
	sd := s.JetStreamConfig().StoreDir
	s.Shutdown()
	nc.Close()

	s = RunJetStreamServerOnPort(-1, sd)
	defer s.Shutdown()
	nc, js = jsClientConnect(t, s)
	defer nc.Close()

	// Stream should come back up with the same state prior to restart.
	si, err = js.StreamInfo("TEST")
	require_NoError(t, err)
	require_Equal(t, si.State.Msgs, 1)
	require_Equal(t, si.State.FirstSeq, fseq)
	require_Equal(t, si.State.LastSeq, fseq)
}

func TestJetStreamCreateStreamWithSubjectDeleteMarkersOptions(t *testing.T) {
	s := RunBasicJetStreamServer(t)
	defer s.Shutdown()

	nc := clientConnectToServer(t, s)
	defer nc.Close()

	cfg := StreamConfig{
		Name:                   "PEDANTIC",
		Storage:                FileStorage,
		Subjects:               []string{"pedantic"},
		SubjectDeleteMarkerTTL: -time.Millisecond,
	}

	_, err := addStreamPedanticWithError(t, nc, &StreamConfigRequest{cfg, true})
	require_Error(t, err, errors.New("subject delete marker TTL must not be negative"))

	cfg.SubjectDeleteMarkerTTL = time.Millisecond
	_, err = addStreamPedanticWithError(t, nc, &StreamConfigRequest{cfg, true})
	require_Error(t, err, errors.New("subject delete marker TTL must be at least 1 second"))

	cfg.SubjectDeleteMarkerTTL = time.Second
	_, err = addStreamPedanticWithError(t, nc, &StreamConfigRequest{cfg, true})
	require_Error(t, err, errors.New("subject delete marker cannot be set if message TTLs are disabled"))

	cfg.AllowMsgTTL = true
	_, err = addStreamPedanticWithError(t, nc, &StreamConfigRequest{cfg, true})
	require_Error(t, err, errors.New("subject delete marker cannot be set if roll-ups are disabled"))

	cfg.AllowRollup = true
	cfg.DenyPurge = true
	_, err = addStreamPedanticWithError(t, nc, &StreamConfigRequest{cfg, true})
	require_Error(t, err, errors.New("roll-ups require the purge permission"))

	cfg.DenyPurge = false
	_, err = addStreamPedanticWithError(t, nc, &StreamConfigRequest{cfg, true})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Automatically setup pre-requisites for SDM.
	cfg = StreamConfig{
		Name:                   "AUTO",
		Storage:                FileStorage,
		Subjects:               []string{"auto"},
		SubjectDeleteMarkerTTL: time.Second,
	}

	si, err := addStreamPedanticWithError(t, nc, &StreamConfigRequest{cfg, false})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	require_Equal(t, si.Config.SubjectDeleteMarkerTTL, time.Second)
	require_True(t, si.Config.AllowMsgTTL)
	require_True(t, si.Config.AllowRollup)
	require_False(t, si.Config.DenyPurge)

	// Allow updating to use SDM and TTL.
	cfg = StreamConfig{
		Name:     "UPDATE",
		Storage:  FileStorage,
		Subjects: []string{"update"},
	}

	si, err = addStreamPedanticWithError(t, nc, &StreamConfigRequest{cfg, false})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	require_False(t, si.Config.AllowMsgTTL)
	require_False(t, si.Config.AllowRollup)
	require_False(t, si.Config.DenyPurge)

	cfg.SubjectDeleteMarkerTTL = time.Second
	si, err = updateStreamPedanticWithError(t, nc, &StreamConfigRequest{cfg, false})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	require_Equal(t, si.Config.SubjectDeleteMarkerTTL, time.Second)
	require_True(t, si.Config.AllowMsgTTL)
	require_True(t, si.Config.AllowRollup)
	require_False(t, si.Config.DenyPurge)

	// Should not be allowed to disable msg TTL.
	cfg = si.Config
	cfg.SubjectDeleteMarkerTTL = 0
	cfg.AllowMsgTTL = false
	_, err = updateStreamPedanticWithError(t, nc, &StreamConfigRequest{cfg, true})
	require_Error(t, err, errors.New("message TTL status can not be disabled"))
}

func TestJetStreamTHWExpireTasksRace(t *testing.T) {
	for _, storageType := range []StorageType{FileStorage, MemoryStorage} {
		t.Run(storageType.String(), func(t *testing.T) {
			s := RunBasicJetStreamServer(t)
			defer s.Shutdown()

			nc, js := jsClientConnect(t, s)
			defer nc.Close()

			_, err := jsStreamCreate(t, nc, &StreamConfig{
				Name:        "TEST",
				Storage:     storageType,
				Subjects:    []string{"foo"},
				AllowMsgTTL: true,
			})
			require_NoError(t, err)

			acc, err := s.lookupAccount(globalAccountName)
			require_NoError(t, err)
			mset, err := acc.lookupStream("TEST")
			require_NoError(t, err)

			// Send a bunch of message that need to be expired.
			m := nats.NewMsg("foo")
			m.Header.Set(JSMessageTTL, "1s")
			for i := 0; i < 10_000; i++ {
				_, err = js.PublishMsg(m)
				require_NoError(t, err)
			}

			// Manually lock so that expirations can't be done without us unlocking.
			if fs, ok := mset.store.(*fileStore); ok {
				fs.mu.Lock()
			} else if ms, ok := mset.store.(*memStore); ok {
				ms.mu.Lock()
			}

			// Wait for all message TTLs to have expired.
			time.Sleep(1500 * time.Millisecond)

			// Spawn a number of goroutines that will all want to lock and unlock the store lock.
			n := 50
			var ready sync.WaitGroup
			var wg sync.WaitGroup
			ready.Add(n)
			wg.Add(n)
			for i := 0; i < n; i++ {
				go func() {
					defer wg.Done()
					ready.Done()
					if fs, ok := mset.store.(*fileStore); ok {
						fs.expireMsgs()
					} else if ms, ok := mset.store.(*memStore); ok {
						ms.expireMsgs()
					}
				}()
			}

			// Wait for all goroutines to be ready.
			ready.Wait()

			// Manually unlock so that goroutines can run expirations in parallel.
			if fs, ok := mset.store.(*fileStore); ok {
				fs.mu.Unlock()
			} else if ms, ok := mset.store.(*memStore); ok {
				ms.mu.Unlock()
			}

			// Wait for all goroutines to finish.
			wg.Wait()

			// Run once more to clean up for removed messages.
			if fs, ok := mset.store.(*fileStore); ok {
				fs.expireMsgs()
			} else if ms, ok := mset.store.(*memStore); ok {
				ms.expireMsgs()
			}

			// Count of entries in the THW should be exactly 0, and not underflow.
			var hwCount uint64
			if fs, ok := mset.store.(*fileStore); ok {
				fs.mu.Lock()
				hwCount = fs.ttls.Count()
				fs.mu.Unlock()
			} else if ms, ok := mset.store.(*memStore); ok {
				ms.mu.Lock()
				hwCount = ms.ttls.Count()
				ms.mu.Unlock()
			}
			require_Equal(t, hwCount, 0)
		})
	}
}

func TestJetStreamRejectLargePublishes(t *testing.T) {
	tdir := t.TempDir()

	// The test relies on the MaxPayload being larger than the
	// rlBadThresh, otherwise you can't publish a message large
	// enough.
	conf := createConfFile(t, []byte(fmt.Sprintf(`
		listen: 127.0.0.1:-1
		max_payload: %d
		jetstream: {store_dir: %q}
	`, rlBadThresh+2048, tdir)))

	s, _ := RunServerWithConfig(conf)
	defer s.Shutdown()

	nc, js := jsClientConnect(t, s)
	defer nc.Close()

	_, err := js.AddStream(&nats.StreamConfig{
		Name:     "TEST",
		Subjects: []string{"test"},
	})
	require_NoError(t, err)

	_, err = js.Publish("test", make([]byte, rlBadThresh-1024))
	require_NoError(t, err)

	_, err = js.Publish("test", make([]byte, rlBadThresh+1024))
	require_Error(t, err)
	require_Contains(t, err.Error(), ErrMsgTooLarge.Error())

	// The oversized publish is rejected before reaching StoreMsg, so it must
	// not prevent later valid publishes.
	_, err = js.Publish("test", []byte("ok"))
	require_NoError(t, err)
}

func TestJetStreamDirectGetSubjectDeleteMarker(t *testing.T) {
	for _, storageType := range []nats.StorageType{nats.FileStorage, nats.MemoryStorage} {
		t.Run(storageType.String(), func(t *testing.T) {
			s := RunBasicJetStreamServer(t)
			defer s.Shutdown()

			nc, js := jsClientConnect(t, s)
			defer nc.Close()

			_, err := js.AddStream(&nats.StreamConfig{
				Name:                   "TEST",
				Subjects:               []string{"test"},
				Storage:                storageType,
				SubjectDeleteMarkerTTL: time.Second,
				AllowMsgTTL:            true,
				AllowDirect:            true,
			})
			require_NoError(t, err)

			m := nats.NewMsg("test")
			m.Header.Set(JSMessageTTL, "1s")
			_, err = js.PublishMsg(m)
			require_NoError(t, err)

			first, err := js.GetLastMsg("TEST", "test", nats.DirectGet())
			require_NoError(t, err)
			require_Equal(t, first.Header.Get(JSSequence), "1")

			time.Sleep(1500 * time.Millisecond)

			second, err := js.GetLastMsg("TEST", "test", nats.DirectGet())
			require_NoError(t, err)
			require_Equal(t, second.Header.Get(JSSequence), "2")
		})
	}
}

func TestJetStreamPurgeExSeqSimple(t *testing.T) {
	for _, storageType := range []nats.StorageType{nats.FileStorage, nats.MemoryStorage} {
		t.Run(storageType.String(), func(t *testing.T) {
			s := RunBasicJetStreamServer(t)
			defer s.Shutdown()

			nc, js := jsClientConnect(t, s)
			defer nc.Close()

			_, err := js.AddStream(&nats.StreamConfig{
				Name:     "TEST",
				Subjects: []string{"test"},
				Storage:  storageType,
			})
			require_NoError(t, err)

			data := make([]byte, 1024)
			for i := 0; i < 10_000; i++ {
				_, err = js.Publish("test", data)
				require_NoError(t, err)
			}

			si, err := js.StreamInfo("TEST")
			require_NoError(t, err)
			require_Equal(t, si.State.Msgs, 10_000)

			require_NoError(t, js.PurgeStream("TEST", &nats.StreamPurgeRequest{Sequence: 9_000}))

			si, err = js.StreamInfo("TEST")
			require_NoError(t, err)
			require_Equal(t, si.State.Msgs, 1_001)
			require_Equal(t, si.State.NumDeleted, 0)
			require_Equal(t, si.State.FirstSeq, 9_000)
			require_Equal(t, si.State.LastSeq, 10_000)
		})
	}
}

func TestJetStreamPurgeExSeqInInteriorDeleteGap(t *testing.T) {
	for _, storageType := range []nats.StorageType{nats.FileStorage, nats.MemoryStorage} {
		t.Run(storageType.String(), func(t *testing.T) {
			s := RunBasicJetStreamServer(t)
			defer s.Shutdown()

			nc, js := jsClientConnect(t, s)
			defer nc.Close()

			_, err := js.AddStream(&nats.StreamConfig{
				Name:     "TEST",
				Subjects: []string{"test.*"},
				Storage:  storageType,
			})
			require_NoError(t, err)

			data := make([]byte, 1024)
			_, err = js.Publish("test.start", data)
			require_NoError(t, err)
			for i := 0; i < 10_000; i++ {
				_, err = js.Publish("test.mid", data)
				require_NoError(t, err)
			}
			_, err = js.Publish("test.end", data)
			require_NoError(t, err)

			si, err := js.StreamInfo("TEST")
			require_NoError(t, err)
			require_Equal(t, si.State.Msgs, 10_002)

			require_NoError(t, js.PurgeStream("TEST", &nats.StreamPurgeRequest{Subject: "test.mid"}))

			si, err = js.StreamInfo("TEST")
			require_NoError(t, err)
			require_Equal(t, si.State.Msgs, 2)
			require_Equal(t, si.State.NumDeleted, 10_000)

			require_NoError(t, js.PurgeStream("TEST", &nats.StreamPurgeRequest{Sequence: 9_000}))

			si, err = js.StreamInfo("TEST")
			require_NoError(t, err)
			require_Equal(t, si.State.Msgs, 1)
			require_Equal(t, si.State.NumDeleted, 0)
			require_Equal(t, si.State.FirstSeq, 10_002)
			require_Equal(t, si.State.LastSeq, 10_002)
		})
	}
}

func TestJetStreamDirectGetUpToTime(t *testing.T) {
	s := RunBasicJetStreamServer(t)
	defer s.Shutdown()

	nc, js := jsClientConnect(t, s)
	defer nc.Close()

	_, err := js.AddStream(&nats.StreamConfig{
		Name:        "TEST",
		Subjects:    []string{"foo"},
		AllowDirect: true,
		Storage:     nats.FileStorage,
	})
	require_NoError(t, err)

	for i := range 10 {
		sendStreamMsg(t, nc, "foo", fmt.Sprintf("message %d", i+1))
	}

	sendRequest := func(mreq *JSApiMsgGetRequest) *nats.Subscription {
		t.Helper()
		req, err := json.Marshal(mreq)
		require_NoError(t, err)
		reply := nats.NewInbox()
		sub, err := nc.SubscribeSync(reply)
		require_NoError(t, err)
		require_NoError(t, nc.PublishRequest("$JS.API.DIRECT.GET.TEST", reply, req))
		return sub
	}

	checkResponses := func(t *testing.T, upToTime time.Time, expected ...string) {
		t.Helper()
		sub := sendRequest(&JSApiMsgGetRequest{MultiLastFor: []string{"foo"}, UpToTime: &upToTime})
		defer sub.Unsubscribe()
		for _, expect := range expected {
			msg, err := sub.NextMsg(25 * time.Millisecond)
			require_NoError(t, err)
			require_Equal(t, msg.Header.Get(JSSubject), "foo")
			require_Equal(t, bytesToString(msg.Data), expect)
		}
		// By this time we're either at the end of our expected and looking
		// for an EOB marker (204) or we're not finding anything (404).
		msg, err := sub.NextMsg(25 * time.Millisecond)
		require_NoError(t, err)
		if len(expected) == 0 {
			require_Equal(t, msg.Header.Get("Status"), "404")
		} else {
			require_Equal(t, msg.Header.Get("Status"), "204")
		}
	}

	t.Run("DistantPast", func(t *testing.T) {
		checkResponses(t, time.Time{})
	})

	t.Run("DistantFuture", func(t *testing.T) {
		checkResponses(t, time.Unix(0, math.MaxInt64), "message 10")
	})

	t.Run("BeforeFirstSeq", func(t *testing.T) {
		first, err := js.GetMsg("TEST", 1)
		require_NoError(t, err)
		checkResponses(t, first.Time)
	})

	t.Run("BeforeFifthSeq", func(t *testing.T) {
		fifth, err := js.GetMsg("TEST", 5)
		require_NoError(t, err)
		checkResponses(t, fifth.Time, "message 4")
	})
}

func TestJetStreamDirectGetStartTimeSingleMsg(t *testing.T) {
	for _, storage := range []nats.StorageType{nats.FileStorage, nats.MemoryStorage} {
		t.Run(storage.String(), func(t *testing.T) {
			s := RunBasicJetStreamServer(t)
			defer s.Shutdown()

			nc, js := jsClientConnect(t, s)
			defer nc.Close()

			_, err := js.AddStream(&nats.StreamConfig{
				Name:        "TEST",
				Subjects:    []string{"foo"},
				AllowDirect: true,
				Storage:     storage,
			})
			require_NoError(t, err)

			sendStreamMsg(t, nc, "foo", "message")

			sendRequest := func(mreq *JSApiMsgGetRequest) *nats.Subscription {
				t.Helper()
				req, err := json.Marshal(mreq)
				require_NoError(t, err)
				reply := nats.NewInbox()
				sub, err := nc.SubscribeSync(reply)
				require_NoError(t, err)
				require_NoError(t, nc.PublishRequest("$JS.API.DIRECT.GET.TEST", reply, req))
				return sub
			}

			first, err := js.GetMsg("TEST", 1)
			require_NoError(t, err)

			future := first.Time.Add(10 * time.Second)
			sub := sendRequest(&JSApiMsgGetRequest{StartTime: &future, NextFor: "foo", Batch: 1})
			defer sub.Unsubscribe()

			msg, err := sub.NextMsg(25 * time.Millisecond)
			require_NoError(t, err)
			require_Equal(t, msg.Header.Get("Status"), "404")
		})
	}
}

func TestJetStreamStreamRetentionUpdatesConsumers(t *testing.T) {
	s := RunBasicJetStreamServer(t)
	defer s.Shutdown()

	nc, js := jsClientConnect(t, s)
	defer nc.Close()

	for _, tc := range []struct {
		from RetentionPolicy
		to   RetentionPolicy
	}{
		{LimitsPolicy, InterestPolicy},
		{InterestPolicy, LimitsPolicy},
	} {
		from, to, name := tc.from, tc.to, fmt.Sprintf("%sTo%s", tc.from, tc.to)
		t.Run(name, func(t *testing.T) {
			sc, err := jsStreamCreate(t, nc, &StreamConfig{
				Name:      name,
				Subjects:  []string{name},
				Retention: from,
				Storage:   FileStorage,
			})
			require_NoError(t, err)

			_, err = js.AddConsumer(name, &nats.ConsumerConfig{
				Name:      "test_consumer",
				AckPolicy: nats.AckExplicitPolicy,
			})
			require_NoError(t, err)

			mset, err := s.globalAccount().lookupStream(name)
			require_NoError(t, err)

			o := mset.lookupConsumer("test_consumer")
			require_NotNil(t, o)
			require_Equal(t, o.retention, from)

			sc.Retention = to
			_, err = jsStreamUpdate(t, nc, sc)
			require_NoError(t, err)

			require_Equal(t, o.retention, to)
		})
	}
}

func TestJetStreamMaxMsgsPerSubjectAndDeliverLastPerSubject(t *testing.T) {
	s := RunBasicJetStreamServer(t)
	defer s.Shutdown()

	nc, js := jsClientConnect(t, s)
	defer nc.Close()

	const subjects = 300
	const msgs = subjects * 10

	_, err := js.AddStream(&nats.StreamConfig{
		Name:              "test",
		Subjects:          []string{"foo.>", "bar.>"},
		Retention:         nats.LimitsPolicy,
		Storage:           nats.FileStorage,
		MaxMsgsPerSubject: 5,
	})
	require_NoError(t, err)

	// First, publish some messages that match the consumer filter. These
	// are the messages that we expect to consume a subset of. The random
	// sequences give us interior gaps after MaxMsgsPerSubject has been
	// enforced which is needed for this test to work.
	for range msgs {
		subj := fmt.Sprintf("foo.%d", rand.IntN(subjects))
		_, err = js.Publish(subj, nil)
		require_NoError(t, err)
	}

	// Then publish some messages on a different subject. These won't be
	// matched by the consumer filter.
	for range msgs / 10 {
		subj := fmt.Sprintf("bar.%d", rand.IntN(subjects/10))
		_, err = js.Publish(subj, nil)
		require_NoError(t, err)
	}

	// Add a deliver last per consumer that matches the first batch of
	// published messages only. We expect at this point that the skiplist
	// of the consumer will be for foo.> messages only, but the resume
	// sequence will be the stream last sequence at the time, i.e. the
	// last bar.> message.
	_, err = js.AddConsumer("test", &nats.ConsumerConfig{
		Name:           "test_consumer",
		FilterSubjects: []string{"foo.>"},
		DeliverPolicy:  nats.DeliverLastPerSubjectPolicy,
		AckPolicy:      nats.AckExplicitPolicy,
	})
	require_NoError(t, err)

	// Take a copy of the skiplist and the resume value so that we can
	// make sure we receive all the messages we expect.
	mset, err := s.globalAccount().lookupStream("test")
	require_NoError(t, err)
	o := mset.lookupConsumer("test_consumer")
	o.mu.RLock()
	pending := make(map[uint64]struct{}, len(o.lss.seqs))
	for _, seq := range o.lss.seqs {
		pending[seq] = struct{}{}
	}
	resume := o.lss.resume
	o.mu.RUnlock()

	// The publishes above are randomly distributed, so it's possible that not every
	// subject was published to. Use the skiplist length so we don't wait on messages
	// that were never published.
	expected := len(pending)

	// Now fetch the messages from the consumer.
	ps, err := js.PullSubscribe(_EMPTY_, _EMPTY_, nats.Bind("test", "test_consumer"))
	require_NoError(t, err)
	for range expected {
		msgs, err := ps.Fetch(1)
		require_NoError(t, err)
		for _, msg := range msgs {
			meta, err := msg.Metadata()
			require_NoError(t, err)
			// We must be expecting this sequence and not have seen it already.
			// Once we've seen it, take it out of the map.
			_, ok := pending[meta.Sequence.Stream]
			require_True(t, ok)
			delete(pending, meta.Sequence.Stream)
			// Then ack.
			require_NoError(t, msg.AckSync())
		}
	}

	// We should have received every message that was in the skiplist.
	require_Len(t, len(pending), 0)

	// When we've run out of last sequences per subject, the consumer
	// should now continue from the resume seq, i.e. the last bar.> message.
	o.mu.RLock()
	sseq := o.sseq
	o.mu.RUnlock()
	require_Equal(t, sseq, resume+1)
}

func TestJetStreamAllowMsgCounter(t *testing.T) {
	test := func(t *testing.T, replicas int) {
		var s *Server
		var servers []*Server
		if replicas == 1 {
			s = RunBasicJetStreamServer(t)
			servers = []*Server{s}
			defer s.Shutdown()
		} else {
			c := createJetStreamClusterExplicit(t, "R3S", 3)
			defer c.shutdown()
			servers = c.servers
			s = c.randomServer()
		}

		nc, js := jsClientConnect(t, s)
		defer nc.Close()

		cfg := &StreamConfig{
			Name:            "TEST",
			Subjects:        []string{"foo"},
			Storage:         FileStorage,
			AllowMsgCounter: false,
			Replicas:        replicas,
		}

		_, err := jsStreamCreate(t, nc, cfg)
		require_NoError(t, err)

		// A normal publish will succeed, as it's not a counter.
		m := nats.NewMsg("foo")
		m.Header.Set("Key", "Value")
		pa, err := js.PublishMsg(m)
		require_NoError(t, err)
		require_Equal(t, pa.Sequence, 1)

		// Stream with disabled counters doesn't allow to publish counters.
		m = nats.NewMsg("foo")
		m.Header.Set("Nats-Incr", "1")
		_, err = js.PublishMsg(m)
		require_Error(t, err, NewJSMessageIncrDisabledError())

		// Enabling counters is not allowed.
		cfg.AllowMsgCounter = true
		_, err = jsStreamUpdate(t, nc, cfg)
		require_Error(t, err, NewJSStreamInvalidConfigError(fmt.Errorf("stream configuration update can not change message counter setting")))

		// Recreate stream with counters enabled.
		require_NoError(t, js.DeleteStream("TEST"))
		_, err = jsStreamCreate(t, nc, cfg)
		require_NoError(t, err)

		// Don't allow if missing counter increment.
		m = nats.NewMsg("foo")
		_, err = js.PublishMsg(m)
		require_Error(t, err, NewJSMessageIncrMissingError())

		m.Header.Set("Key", "Value")
		_, err = js.PublishMsg(m)
		require_Error(t, err, NewJSMessageIncrMissingError())

		// Don't allow if increment contains payload.
		m.Header.Set("Nats-Incr", "1")
		m.Data = []byte("data")
		_, err = js.PublishMsg(m)
		require_Error(t, err, NewJSMessageIncrPayloadError())

		// Don't allow if counter increment is invalid.
		m = nats.NewMsg("foo")
		m.Header.Set("Nats-Incr", "bogus")
		_, err = js.PublishMsg(m)
		require_Error(t, err, NewJSMessageIncrInvalidError())

		validateTotal := func(seq uint64, total *big.Int) {
			t.Helper()
			rsm, err := js.GetLastMsg("TEST", "foo")
			require_NoError(t, err)
			require_Equal(t, rsm.Sequence, seq)

			var val CounterValue
			require_NoError(t, json.Unmarshal(rsm.Data, &val))
			var res big.Int
			res.SetString(val.Value, 10)
			require_Equal(t, res.Int64(), total.Int64())
		}

		increment := func(incr string, seq uint64, total *big.Int) {
			t.Helper()
			m = nats.NewMsg("foo")
			m.Header.Set("Nats-Incr", incr)

			msg, err := nc.RequestMsg(m, time.Second)
			require_NoError(t, err)
			var pubAck PubAck
			require_NoError(t, json.Unmarshal(msg.Data, &pubAck))
			require_Equal(t, pubAck.Sequence, seq)
			require_Equal(t, pubAck.Value, total.String())
			validateTotal(seq, total)
		}

		// Perform and check increments.
		increment("1", 1, big.NewInt(1))
		increment("2", 2, big.NewInt(3))

		// Can also decrement/reset the counter.
		increment("-3", 3, big.NewInt(0))
		increment("1", 4, big.NewInt(1))

		// Reset back to zero.
		increment("-1", 5, big.NewInt(0))

		// Check de-duplication.
		m = nats.NewMsg("foo")
		m.Header.Set("Nats-Incr", "1")
		m.Header.Set("Nats-Msg-Id", "dedupe")
		msg, err := nc.RequestMsg(m, time.Second)
		require_NoError(t, err)
		var pubAck1 PubAck
		require_NoError(t, json.Unmarshal(msg.Data, &pubAck1))
		require_False(t, pubAck1.Duplicate)
		require_Equal(t, pubAck1.Sequence, 6)
		require_Equal(t, pubAck1.Value, "1")
		validateTotal(6, big.NewInt(1))

		// Re-send should not up counter, but also not return current value.
		// When clustered this state can't be guaranteed.
		msg, err = nc.RequestMsg(m, time.Second)
		require_NoError(t, err)
		var pubAck2 PubAck
		require_NoError(t, json.Unmarshal(msg.Data, &pubAck2))
		require_True(t, pubAck2.Duplicate)
		require_Equal(t, pubAck2.Sequence, 6)
		require_Equal(t, pubAck2.Value, _EMPTY_)
		validateTotal(6, big.NewInt(1))

		// Check rejected headers.
		for _, header := range []string{JSMsgRollup, JSExpectedLastSeq, JSExpectedLastSubjSeq, JSExpectedLastSubjSeqSubj, JSExpectedLastMsgId} {
			m = nats.NewMsg("foo")
			m.Header.Set("Nats-Incr", "1")
			m.Header.Set(header, "1")
			_, err = js.PublishMsg(m)
			require_Error(t, err, NewJSMessageIncrInvalidError())
		}

		// Wait for all replicas to have applied all messages.
		checkFor(t, 2*time.Second, 50*time.Millisecond, func() error {
			for _, cs := range servers {
				mset, err := cs.globalAccount().lookupStream("TEST")
				if err != nil {
					return err
				}
				if lseq := mset.lastSeq(); lseq != 6 {
					return fmt.Errorf("expected last seq 6 on %s, got %d", cs.Name(), lseq)
				}
			}
			return nil
		})

		// Manually break a counter in storage.
		for _, cs := range servers {
			mset, err := cs.globalAccount().lookupStream("TEST")
			require_NoError(t, err)
			seq, _, err := mset.Store().StoreMsg("foo", nil, nil, 0)
			require_NoError(t, err)
			require_Equal(t, seq, 7)
		}

		// Should now error, because counter is broken.
		m = nats.NewMsg("foo")
		m.Header.Set("Nats-Incr", "1")
		_, err = js.PublishMsg(m)
		require_Error(t, err, NewJSMessageCounterBrokenError())
	}

	t.Run("R1", func(t *testing.T) { test(t, 1) })
	t.Run("R3", func(t *testing.T) { test(t, 3) })
}

func TestJetStreamAllowMsgCounterMaxPayloadAndSize(t *testing.T) {
	test := func(t *testing.T, replicas int) {
		var s *Server
		if replicas == 1 {
			s = RunBasicJetStreamServer(t)
			s.optsMu.Lock()
			s.opts.MaxPayload = 1024
			s.optsMu.Unlock()
			defer s.Shutdown()
		} else {
			c := createJetStreamClusterExplicit(t, "R3S", 3)
			defer c.shutdown()
			for _, cs := range c.servers {
				cs.optsMu.Lock()
				cs.opts.MaxPayload = 1024
				cs.optsMu.Unlock()
			}
			s = c.randomServer()
		}

		nc, js := jsClientConnect(t, s)
		defer nc.Close()

		cfg := &StreamConfig{
			Name:            "TEST",
			Subjects:        []string{"foo"},
			Storage:         FileStorage,
			AllowMsgCounter: true,
			Replicas:        replicas,
		}
		_, err := jsStreamCreate(t, nc, cfg)
		require_NoError(t, err)

		validateTotal := func(seq uint64, total *big.Int) {
			t.Helper()
			rsm, err := js.GetLastMsg("TEST", "foo")
			require_NoError(t, err)
			require_Equal(t, rsm.Sequence, seq)

			var val CounterValue
			require_NoError(t, json.Unmarshal(rsm.Data, &val))
			var res big.Int
			res.SetString(val.Value, 10)
			require_Equal(t, res.Int64(), total.Int64())
		}

		increment := func(incr string, seq uint64, total *big.Int) {
			t.Helper()
			m := nats.NewMsg("foo")
			m.Header.Set("Nats-Incr", incr)

			msg, err := nc.RequestMsg(m, time.Second)
			require_NoError(t, err)
			var pubAck PubAck
			require_NoError(t, json.Unmarshal(msg.Data, &pubAck))
			require_Equal(t, pubAck.Sequence, seq)
			require_Equal(t, pubAck.Value, total.String())
			validateTotal(seq, total)
		}

		// Check payload exceeded, positive bound.
		increment("1", 1, big.NewInt(1))
		tooLargeIncrement := strings.Repeat("1", 500)
		m := nats.NewMsg("foo")
		m.Header.Set("Nats-Incr", tooLargeIncrement)
		_, err = js.PublishMsg(m)
		require_Error(t, err, NewJSStreamMessageExceedsMaximumError())

		// Check payload exceeded, negative bound.
		increment("-2", 2, big.NewInt(-1))
		m.Header.Set("Nats-Incr", fmt.Sprintf("-%s", tooLargeIncrement))
		_, err = js.PublishMsg(m)
		require_Error(t, err, NewJSStreamMessageExceedsMaximumError())

		// Reset back to zero.
		increment("1", 3, big.NewInt(0))

		// Exactly equals the size of the message for the first message.
		cfg.MaxMsgSize = 37
		_, err = jsStreamUpdate(t, nc, cfg)
		require_NoError(t, err)
		increment("1", 4, big.NewInt(1))

		// Next increment bumps over MaxMsgSize limit defined above.
		m.Header.Set("Nats-Incr", "10")
		_, err = js.PublishMsg(m)
		require_Error(t, err, NewJSStreamMessageExceedsMaximumError())
	}

	t.Run("R1", func(t *testing.T) { test(t, 1) })
	t.Run("R3", func(t *testing.T) { test(t, 3) })
}

func TestJetStreamAllowMsgCounterIncompatibleSettings(t *testing.T) {
	s := RunBasicJetStreamServer(t)
	defer s.Shutdown()

	nc := clientConnectToServer(t, s)
	defer nc.Close()

	// DiscardNew not allowed.
	_, err := jsStreamCreate(t, nc, &StreamConfig{
		Name:            "TEST",
		Subjects:        []string{"foo"},
		Storage:         FileStorage,
		AllowMsgCounter: true,
		Discard:         DiscardNew,
	})
	require_Error(t, err, NewJSStreamInvalidConfigError(fmt.Errorf("counter stream cannot use discard new")))

	// AllowMsgTTL not allowed.
	_, err = jsStreamCreate(t, nc, &StreamConfig{
		Name:            "TEST",
		Subjects:        []string{"foo"},
		Storage:         FileStorage,
		AllowMsgCounter: true,
		AllowMsgTTL:     true,
	})
	require_Error(t, err, NewJSStreamInvalidConfigError(fmt.Errorf("counter stream cannot use message TTLs")))

	// AllowMsgSchedules not allowed.
	_, err = jsStreamCreate(t, nc, &StreamConfig{
		Name:              "TEST",
		Subjects:          []string{"foo"},
		Storage:           FileStorage,
		AllowMsgCounter:   true,
		AllowMsgSchedules: true,
	})
	require_Error(t, err, NewJSStreamInvalidConfigError(fmt.Errorf("counter stream cannot use message schedules")))

	// Only limits retention is allowed.
	for _, retention := range []RetentionPolicy{InterestPolicy, WorkQueuePolicy} {
		_, err = jsStreamCreate(t, nc, &StreamConfig{
			Name:            "TEST",
			Subjects:        []string{"foo"},
			Storage:         FileStorage,
			AllowMsgCounter: true,
			Retention:       retention,
		})
		require_Error(t, err, NewJSStreamInvalidConfigError(fmt.Errorf("counter stream can only use limits retention")))
	}
}

func TestJetStreamAllowMsgCounterMirror(t *testing.T) {
	test := func(t *testing.T, replicas int) {
		var s *Server
		if replicas == 1 {
			s = RunBasicJetStreamServer(t)
			defer s.Shutdown()
		} else {
			c := createJetStreamClusterExplicit(t, "R3S", 3)
			defer c.shutdown()
			s = c.randomServer()
		}

		nc, js := jsClientConnect(t, s)
		defer nc.Close()

		_, err := jsStreamCreate(t, nc, &StreamConfig{
			Name:            "O",
			Subjects:        []string{"foo"},
			Storage:         FileStorage,
			AllowMsgCounter: true,
			Replicas:        replicas,
		})
		require_NoError(t, err)

		// Mirror with counters enabled is rejected.
		mirrorCfg := &StreamConfig{
			Name:            "M",
			Mirror:          &StreamSource{Name: "O"},
			Storage:         FileStorage,
			AllowMsgCounter: true,
			Replicas:        replicas,
		}
		_, err = jsStreamCreate(t, nc, mirrorCfg)
		require_Error(t, err, NewJSMirrorWithCountersError())

		// Mirror with verbatim copying of counters.
		mirrorCfg.AllowMsgCounter = false
		_, err = jsStreamCreate(t, nc, mirrorCfg)
		require_NoError(t, err)

		// A normal publish will succeed, as it's not a counter.
		m := nats.NewMsg("foo")
		m.Header.Set("Nats-Incr", "1")
		pubAck, err := js.PublishMsg(m)
		require_NoError(t, err)
		require_Equal(t, pubAck.Sequence, 1)

		// Mirror should get message verbatim.
		checkFor(t, 2*time.Second, 200*time.Millisecond, func() error {
			rsm, err := js.GetMsg("M", 1)
			if err != nil {
				return err
			}
			if incr := rsm.Header.Get("Nats-Incr"); incr != "1" {
				return fmt.Errorf("incorrect increment: %q", incr)
			}
			var count CounterValue
			if err := json.Unmarshal(rsm.Data, &count); err != nil {
				return fmt.Errorf("JSON error: %w", err)
			} else if count.Value != "1" {
				return fmt.Errorf("unexpected value: %s", rsm.Data)
			}
			return nil
		})
	}

	t.Run("R1", func(t *testing.T) { test(t, 1) })
	t.Run("R3", func(t *testing.T) { test(t, 3) })
}

func TestJetStreamAllowMsgCounterSourceAggregates(t *testing.T) {
	test := func(t *testing.T, replicas int) {
		var s *Server
		if replicas == 1 {
			s = RunBasicJetStreamServer(t)
			defer s.Shutdown()
		} else {
			c := createJetStreamClusterExplicit(t, "R3S", 3)
			defer c.shutdown()
			s = c.randomServer()
		}

		nc, js := jsClientConnect(t, s)
		defer nc.Close()

		_, err := jsStreamCreate(t, nc, &StreamConfig{
			Name:            "O1",
			Subjects:        []string{"foo.1"},
			Storage:         FileStorage,
			AllowMsgCounter: true,
			Replicas:        replicas,
		})
		require_NoError(t, err)

		_, err = jsStreamCreate(t, nc, &StreamConfig{
			Name:            "O2",
			Subjects:        []string{"foo.2"},
			Storage:         FileStorage,
			AllowMsgCounter: true,
			Replicas:        replicas,
		})
		require_NoError(t, err)

		// Source will only work if counters are enabled on both streams.
		sourceCfg := &StreamConfig{
			Name: "M",
			Sources: []*StreamSource{
				{
					Name:              "O1",
					SubjectTransforms: []SubjectTransformConfig{{Source: "foo.*", Destination: "foo"}},
				},
				{
					Name:              "O2",
					SubjectTransforms: []SubjectTransformConfig{{Source: "foo.*", Destination: "foo"}},
				},
			},
			Storage:         FileStorage,
			AllowMsgCounter: true,
			Replicas:        replicas,
		}
		_, err = jsStreamCreate(t, nc, sourceCfg)
		require_NoError(t, err)

		m := nats.NewMsg("foo.1")
		m.Header.Set("Nats-Incr", "1")
		pubAck, err := js.PublishMsg(m)
		require_NoError(t, err)
		require_Equal(t, pubAck.Sequence, 1)

		m = nats.NewMsg("foo.2")
		m.Header.Set("Nats-Incr", "2")
		pubAck, err = js.PublishMsg(m)
		require_NoError(t, err)
		require_Equal(t, pubAck.Sequence, 1)

		// Source should aggregate.
		checkFor(t, 2*time.Second, 200*time.Millisecond, func() error {
			rsm, err := js.GetMsg("M", 2)
			if err != nil {
				return err
			}
			var count CounterValue
			if err := json.Unmarshal(rsm.Data, &count); err != nil {
				return fmt.Errorf("JSON error: %w", err)
			} else if count.Value != "3" {
				return fmt.Errorf("unexpected value: %s", rsm.Data)
			}
			return nil
		})
	}

	t.Run("R1", func(t *testing.T) { test(t, 1) })
	t.Run("R3", func(t *testing.T) { test(t, 3) })
}

func TestJetStreamAllowMsgCounterSourceVerbatim(t *testing.T) {
	test := func(t *testing.T, replicas int) {
		var s *Server
		if replicas == 1 {
			s = RunBasicJetStreamServer(t)
			defer s.Shutdown()
		} else {
			c := createJetStreamClusterExplicit(t, "R3S", 3)
			defer c.shutdown()
			s = c.randomServer()
		}

		nc, js := jsClientConnect(t, s)
		defer nc.Close()

		_, err := jsStreamCreate(t, nc, &StreamConfig{
			Name:            "O1",
			Subjects:        []string{"foo.1"},
			Storage:         FileStorage,
			AllowMsgCounter: true,
			Replicas:        replicas,
		})
		require_NoError(t, err)

		_, err = jsStreamCreate(t, nc, &StreamConfig{
			Name:            "O2",
			Subjects:        []string{"foo.2"},
			Storage:         FileStorage,
			AllowMsgCounter: true,
			Replicas:        replicas,
		})
		require_NoError(t, err)

		// Source will only work if counters are enabled on both streams.
		sourceCfg := &StreamConfig{
			Name: "M",
			Sources: []*StreamSource{
				{
					Name:              "O1",
					SubjectTransforms: []SubjectTransformConfig{{Source: "foo.*", Destination: "foo"}},
				},
				{
					Name:              "O2",
					SubjectTransforms: []SubjectTransformConfig{{Source: "foo.*", Destination: "foo"}},
				},
			},
			Storage:         FileStorage,
			AllowMsgCounter: false,
			Replicas:        replicas,
		}
		_, err = jsStreamCreate(t, nc, sourceCfg)
		require_NoError(t, err)

		m := nats.NewMsg("foo.1")
		m.Header.Set("Nats-Incr", "1")
		pubAck, err := js.PublishMsg(m)
		require_NoError(t, err)
		require_Equal(t, pubAck.Sequence, 1)

		// Source should store verbatim.
		checkFor(t, 2*time.Second, 200*time.Millisecond, func() error {
			rsm, err := js.GetMsg("M", 1)
			if err != nil {
				return err
			}
			var count CounterValue
			if err := json.Unmarshal(rsm.Data, &count); err != nil {
				return fmt.Errorf("JSON error: %w", err)
			} else if count.Value != "1" {
				return fmt.Errorf("unexpected value: %s", rsm.Data)
			}
			return nil
		})

		m = nats.NewMsg("foo.2")
		m.Header.Set("Nats-Incr", "2")
		pubAck, err = js.PublishMsg(m)
		require_NoError(t, err)
		require_Equal(t, pubAck.Sequence, 1)

		// Source should store verbatim.
		checkFor(t, 2*time.Second, 200*time.Millisecond, func() error {
			rsm, err := js.GetMsg("M", 2)
			if err != nil {
				return err
			}
			var count CounterValue
			if err := json.Unmarshal(rsm.Data, &count); err != nil {
				return fmt.Errorf("JSON error: %w", err)
			} else if count.Value != "2" {
				return fmt.Errorf("unexpected value: %s", rsm.Data)
			}
			return nil
		})
	}

	t.Run("R1", func(t *testing.T) { test(t, 1) })
	t.Run("R3", func(t *testing.T) { test(t, 3) })
}

func TestJetStreamAllowMsgCounterSourceStartingAboveZero(t *testing.T) {
	test := func(t *testing.T, replicas int) {
		var s *Server
		if replicas == 1 {
			s = RunBasicJetStreamServer(t)
			defer s.Shutdown()
		} else {
			c := createJetStreamClusterExplicit(t, "R3S", 3)
			defer c.shutdown()
			s = c.randomServer()
		}

		nc, js := jsClientConnect(t, s)
		defer nc.Close()

		_, err := jsStreamCreate(t, nc, &StreamConfig{
			Name:            "O1",
			Subjects:        []string{"foo.1"},
			Storage:         FileStorage,
			AllowMsgCounter: true,
			Replicas:        replicas,
			MaxMsgsPer:      1,
		})
		require_NoError(t, err)

		_, err = jsStreamCreate(t, nc, &StreamConfig{
			Name:            "O2",
			Subjects:        []string{"foo.2"},
			Storage:         FileStorage,
			AllowMsgCounter: true,
			Replicas:        replicas,
			MaxMsgsPer:      1,
		})
		require_NoError(t, err)

		for i := range uint64(5) {
			m := nats.NewMsg("foo.1")
			m.Header.Set("Nats-Incr", "1")
			pubAck, err := js.PublishMsg(m)
			require_NoError(t, err)
			require_Equal(t, pubAck.Sequence, i+1)

			m = nats.NewMsg("foo.2")
			m.Header.Set("Nats-Incr", "2")
			pubAck, err = js.PublishMsg(m)
			require_NoError(t, err)
			require_Equal(t, pubAck.Sequence, i+1)
		}

		// Source will only work if counters are enabled on both streams.
		_, err = jsStreamCreate(t, nc, &StreamConfig{
			Name:            "M",
			Subjects:        []string{"foo"},
			Storage:         FileStorage,
			AllowMsgCounter: true,
			Replicas:        replicas,
		})
		require_NoError(t, err)

		// Now make an addition locally on this stream too.
		m := nats.NewMsg("foo")
		m.Header.Set("Nats-Incr", "1")
		pubAck, err := js.PublishMsg(m)
		require_NoError(t, err)
		require_Equal(t, pubAck.Sequence, 1)

		// Source will only work if counters are enabled on both streams.
		_, err = jsStreamUpdate(t, nc, &StreamConfig{
			Name:     "M",
			Subjects: []string{"foo"},
			Sources: []*StreamSource{
				{
					Name:              "O1",
					SubjectTransforms: []SubjectTransformConfig{{Source: "foo.*", Destination: "foo"}},
				},
				{
					Name:              "O2",
					SubjectTransforms: []SubjectTransformConfig{{Source: "foo.*", Destination: "foo"}},
				},
			},
			Storage:         FileStorage,
			AllowMsgCounter: true,
			Replicas:        replicas,
		})
		require_NoError(t, err)

		// Source should aggregate.
		var first, second, third, fourth *nats.RawStreamMsg
		checkFor(t, 2*time.Second, 200*time.Millisecond, func() error {
			first, err = js.GetMsg("M", 1)
			if err != nil {
				return err
			}
			second, err = js.GetMsg("M", 2)
			if err != nil {
				return err
			}
			third, err = js.GetMsg("M", 3)
			return err
		})

		// Now make an addition locally on this stream too.
		m = nats.NewMsg("foo")
		m.Header.Set("Nats-Incr", "1")
		pubAck, err = js.PublishMsg(m)
		require_NoError(t, err)
		require_Equal(t, pubAck.Sequence, 4)

		// Fetch it back out of the stream.
		checkFor(t, 2*time.Second, 200*time.Millisecond, func() error {
			fourth, err = js.GetMsg("M", 4)
			return err
		})

		// There are no local additions to this counter, but the total
		// comprises changes from the sources.
		var count CounterValue
		require_NoError(t, json.Unmarshal(fourth.Data, &count))
		require_Equal(t, count.Value, "17")

		// The most recent message should contain information about both
		// sources, so let's check.
		var sources CounterSources
		require_NoError(t, json.Unmarshal([]byte(fourth.Header.Get(JSMessageCounterSources)), &sources))
		require_NotNil(t, sources["O1"])
		require_NotNil(t, sources["O2"])
		require_Equal(t, sources["O1"]["foo.1"], "5")
		require_Equal(t, sources["O2"]["foo.2"], "10")

		// Since this is the first time we've seen a message from these
		// sources, the Nats-Incr header should have been updated to reflect
		// the correct delta.
		for _, rsm := range []*nats.RawStreamMsg{first, second, third, fourth} {
			require_Equal(t, rsm.Subject, "foo") // Subject transform'd
			switch rsm {
			case first:
				require_Equal(t, rsm.Header.Get(JSMessageIncr), "1")
			case second, third: // We can't know which order they got sourced in
				incr := rsm.Header.Get(JSMessageIncr)
				require_True(t, incr == "5" || incr == "10")
			case fourth:
				require_Equal(t, rsm.Header.Get(JSMessageIncr), "1")
			}
		}
	}

	t.Run("R1", func(t *testing.T) { test(t, 1) })
	t.Run("R3", func(t *testing.T) { test(t, 3) })
}

func TestJetStreamGetNoHeaders(t *testing.T) {
	s := RunBasicJetStreamServer(t)
	defer s.Shutdown()

	nc, js := jsClientConnect(t, s)
	defer nc.Close()

	_, err := js.AddStream(&nats.StreamConfig{
		Name:        "TEST",
		Subjects:    []string{"foo"},
		Storage:     nats.FileStorage,
		AllowDirect: true,
	})
	require_NoError(t, err)

	msg := &nats.Msg{
		Subject: "foo",
		Header: nats.Header{
			"test": []string{"something"},
		},
		Data: []byte{1, 2, 3, 4, 5},
	}
	_, err = js.PublishMsg(msg)
	require_NoError(t, err)

	t.Run("MsgGet", func(t *testing.T) {
		msgGet := func(noHeaders bool) (payload []byte, hdrs nats.Header) {
			getSubj := fmt.Sprintf(JSApiMsgGetT, "TEST")
			req := fmt.Appendf(nil, `{"seq":1,"no_hdr":%v}`, noHeaders)
			resp, err := nc.Request(getSubj, req, time.Second)
			require_NoError(t, err)
			var get JSApiMsgGetResponse
			require_NoError(t, json.Unmarshal(resp.Data, &get))
			payload = get.Message.Data
			if len(get.Message.Header) > 0 {
				hdrs, err = nats.DecodeHeadersMsg(get.Message.Header)
				require_NoError(t, err)
			}
			return
		}

		payload, headers := msgGet(false)
		require_True(t, bytes.Equal(payload, msg.Data))
		require_Equal(t, headers.Get("test"), "something")

		payload, headers = msgGet(true)
		require_True(t, bytes.Equal(payload, msg.Data))
		require_Equal(t, headers.Get("test"), _EMPTY_)
	})

	t.Run("DirectGet", func(t *testing.T) {
		directGet := func(noHeaders bool) (payload []byte, hdrs nats.Header) {
			getSubj := fmt.Sprintf(JSDirectMsgGetT, "TEST")
			req := fmt.Appendf(nil, `{"seq":1,"no_hdr":%v}`, noHeaders)
			resp, err := nc.Request(getSubj, req, time.Second)
			require_NoError(t, err)
			return resp.Data, resp.Header
		}

		payload, headers := directGet(false)
		require_True(t, bytes.Equal(payload, msg.Data))
		require_Equal(t, headers.Get("test"), "something")

		payload, headers = directGet(true)
		require_True(t, bytes.Equal(payload, msg.Data))
		require_Equal(t, headers.Get("test"), _EMPTY_)
		require_Equal(t, headers.Get("Nats-Stream"), _EMPTY_)
		require_Equal(t, headers.Get("Nats-Sequence"), _EMPTY_)
		require_Equal(t, headers.Get("Nats-Time-Stamp"), _EMPTY_)
	})

	t.Run("DirectGetLastFor", func(t *testing.T) {
		directGet := func(noHeaders bool) (payload []byte, hdrs nats.Header) {
			getSubj := fmt.Sprintf(JSDirectGetLastBySubjectT, "TEST", "foo")
			req := fmt.Appendf(nil, `{"no_hdr":%v}`, noHeaders)
			resp, err := nc.Request(getSubj, req, time.Second)
			require_NoError(t, err)
			return resp.Data, resp.Header
		}

		payload, headers := directGet(false)
		require_True(t, bytes.Equal(payload, msg.Data))
		require_Equal(t, headers.Get("test"), "something")

		payload, headers = directGet(true)
		require_True(t, bytes.Equal(payload, msg.Data))
		require_Equal(t, headers.Get("test"), _EMPTY_)
		require_Equal(t, headers.Get("Nats-Stream"), _EMPTY_)
		require_Equal(t, headers.Get("Nats-Sequence"), _EMPTY_)
		require_Equal(t, headers.Get("Nats-Time-Stamp"), _EMPTY_)
	})
}

func TestJetStreamKVNoSubjectDeleteMarkerOnPurgeMarker(t *testing.T) {
	for _, storage := range []jetstream.StorageType{jetstream.FileStorage, jetstream.MemoryStorage} {
		t.Run(storage.String(), func(t *testing.T) {
			s := RunBasicJetStreamServer(t)
			defer s.Shutdown()

			nc, js := jsClientConnectNewAPI(t, s)
			defer nc.Close()

			ctx := context.Background()
			kv, err := js.CreateKeyValue(ctx, jetstream.KeyValueConfig{
				Bucket:         "bucket",
				History:        1,
				Storage:        storage,
				TTL:            2 * time.Second,
				LimitMarkerTTL: time.Minute,
			})
			require_NoError(t, err)

			stream, err := js.Stream(ctx, "KV_bucket")
			require_NoError(t, err)

			// Purge such that the bucket TTL expires this message.
			require_NoError(t, kv.Purge(ctx, "key"))
			rsm, err := stream.GetMsg(ctx, 1)
			require_NoError(t, err)
			require_Equal(t, rsm.Header.Get("KV-Operation"), "PURGE")

			// The bucket TTL should have removed the message by now.
			time.Sleep(2500 * time.Millisecond)

			// Confirm the purge marker is gone.
			_, err = stream.GetMsg(ctx, 1)
			require_Error(t, err, jetstream.ErrMsgNotFound)
			require_Equal(t, rsm.Header.Get("KV-Operation"), "PURGE")

			// Confirm we don't get a redundant subject delete marker.
			_, err = stream.GetMsg(ctx, 2)
			require_Error(t, err, jetstream.ErrMsgNotFound)

			// Purge with a TTL so it expires this message.
			require_NoError(t, kv.Purge(ctx, "key", jetstream.PurgeTTL(time.Second)))
			rsm, err = stream.GetMsg(ctx, 2)
			require_NoError(t, err)
			require_Equal(t, rsm.Header.Get("KV-Operation"), "PURGE")

			// The purge TTL should have removed the message by now.
			time.Sleep(1500 * time.Millisecond)

			// Confirm the purge marker is gone.
			_, err = stream.GetMsg(ctx, 2)
			require_Error(t, err, jetstream.ErrMsgNotFound)

			// Confirm we don't get a redundant subject delete marker.
			_, err = stream.GetMsg(ctx, 3)
			require_Error(t, err, jetstream.ErrMsgNotFound)
		})
	}
}

func TestJetStreamInvalidConfigValues(t *testing.T) {
	s := RunBasicJetStreamServer(t)
	defer s.Shutdown()

	acc := s.globalAccount()

	_, err := s.checkStreamCfg(&StreamConfig{Name: "TEST", Retention: -1}, acc, false)
	require_True(t, err != nil)
	require_Error(t, err, NewJSStreamInvalidConfigError(errors.New("invalid retention")))

	_, err = s.checkStreamCfg(&StreamConfig{Name: "TEST", Discard: -1}, acc, false)
	require_True(t, err != nil)
	require_Error(t, err, NewJSStreamInvalidConfigError(errors.New("invalid discard policy")))

	_, err = s.checkStreamCfg(&StreamConfig{Name: "TEST", Storage: -1}, acc, false)
	require_True(t, err != nil)
	require_Error(t, err, NewJSStreamInvalidConfigError(errors.New("invalid storage type")))

	_, err = s.checkStreamCfg(&StreamConfig{Name: "TEST", Compression: 255}, acc, false)
	require_True(t, err != nil)
	require_Error(t, err, NewJSStreamInvalidConfigError(errors.New("invalid compression")))

	_, err = s.checkStreamCfg(&StreamConfig{Name: "TEST", MaxAge: -time.Second}, acc, false)
	require_True(t, err != nil)
	require_Error(t, err, NewJSStreamInvalidConfigError(errors.New("max age can not be negative")))

	scfg := StreamConfig{Name: "TEST"}
	streamTests := []struct {
		name     string
		setValue func(value int)
		getValue func() int
	}{
		{
			name:     "max_msgs",
			setValue: func(value int) { scfg.MaxMsgs = int64(value) },
			getValue: func() int { return int(scfg.MaxMsgs) },
		},
		{
			name:     "max_msgs_per_subject",
			setValue: func(value int) { scfg.MaxMsgsPer = int64(value) },
			getValue: func() int { return int(scfg.MaxMsgsPer) },
		},
		{
			name:     "max_bytes",
			setValue: func(value int) { scfg.MaxBytes = int64(value) },
			getValue: func() int { return int(scfg.MaxBytes) },
		},
		{
			name:     "max_msg_size",
			setValue: func(value int) { scfg.MaxMsgSize = int32(value) },
			getValue: func() int { return int(scfg.MaxMsgSize) },
		},
		{
			name:     "max_consumers",
			setValue: func(value int) { scfg.MaxConsumers = value },
			getValue: func() int { return scfg.MaxConsumers },
		},
	}
	for _, streamTest := range streamTests {
		// Pedantic errors if less than -1.
		streamTest.setValue(-10)
		_, err = s.checkStreamCfg(&scfg, acc, true)
		if err == nil {
			t.Fatal("Expected error for pedantic mode")
		}
		require_Error(t, err, NewJSPedanticError(fmt.Errorf("%s must be set to -1", streamTest.name)))

		// Pedantic defaults if zero-value.
		streamTest.setValue(0)
		scfg, err = s.checkStreamCfg(&scfg, acc, true)
		if err != nil {
			require_NoError(t, err)
		}
		require_Equal(t, streamTest.getValue(), -1)

		// Non-pedantic defaults.
		streamTest.setValue(-10)
		scfg, err = s.checkStreamCfg(&scfg, acc, false)
		if err != nil {
			require_NoError(t, err)
		}
		require_Equal(t, streamTest.getValue(), -1)
	}

	ccfg := &ConsumerConfig{AckPolicy: -1}
	err = checkConsumerCfg(ccfg, &JSLimitOpts{}, &scfg, nil, &JetStreamAccountLimits{}, false)
	require_True(t, err != nil)
	require_Error(t, err, NewJSConsumerAckPolicyInvalidError())

	ccfg = &ConsumerConfig{ReplayPolicy: -1}
	err = checkConsumerCfg(ccfg, &JSLimitOpts{}, &scfg, nil, &JetStreamAccountLimits{}, false)
	require_True(t, err != nil)
	require_Error(t, err, NewJSConsumerReplayPolicyInvalidError())

	ccfg = &ConsumerConfig{AckWait: -time.Second}
	err = checkConsumerCfg(ccfg, &JSLimitOpts{}, &scfg, nil, &JetStreamAccountLimits{}, false)
	require_True(t, err != nil)
	require_Error(t, err, NewJSConsumerAckWaitNegativeError())

	ccfg = &ConsumerConfig{BackOff: []time.Duration{-time.Second}}
	err = checkConsumerCfg(ccfg, &JSLimitOpts{}, &scfg, nil, &JetStreamAccountLimits{}, false)
	require_True(t, err != nil)
	require_Error(t, err, NewJSConsumerBackOffNegativeError())

	ccfg = &ConsumerConfig{AckPolicy: AckExplicit}
	consumerTests := []struct {
		name         string
		setValue     func(value int)
		getValue     func() int
		defaultValue int
	}{
		{
			name:         "max_deliver",
			setValue:     func(value int) { ccfg.MaxDeliver = value },
			getValue:     func() int { return ccfg.MaxDeliver },
			defaultValue: -1,
		},
		{
			name:         "max_waiting",
			setValue:     func(value int) { ccfg.MaxWaiting = value },
			getValue:     func() int { return ccfg.MaxWaiting },
			defaultValue: JSWaitQueueDefaultMax,
		},
		{
			name:         "max_batch",
			setValue:     func(value int) { ccfg.MaxRequestBatch = value },
			getValue:     func() int { return ccfg.MaxRequestBatch },
			defaultValue: 0,
		},
		{
			name:         "max_expires",
			setValue:     func(value int) { ccfg.MaxRequestExpires = time.Duration(value) },
			getValue:     func() int { return int(ccfg.MaxRequestExpires) },
			defaultValue: 0,
		},
		{
			name:         "max_bytes",
			setValue:     func(value int) { ccfg.MaxRequestMaxBytes = value },
			getValue:     func() int { return ccfg.MaxRequestMaxBytes },
			defaultValue: 0,
		},
		{
			name:         "idle_heartbeat",
			setValue:     func(value int) { ccfg.Heartbeat = time.Duration(value) },
			getValue:     func() int { return int(ccfg.Heartbeat) },
			defaultValue: 0,
		},
		{
			name:         "inactive_threshold",
			setValue:     func(value int) { ccfg.InactiveThreshold = time.Duration(value) },
			getValue:     func() int { return int(ccfg.InactiveThreshold) },
			defaultValue: 0,
		},
		{
			name:         "priority_timeout",
			setValue:     func(value int) { ccfg.PinnedTTL = time.Duration(value) },
			getValue:     func() int { return int(ccfg.PinnedTTL) },
			defaultValue: 0,
		},
	}
	for _, consumerTest := range consumerTests {
		// Pedantic errors if less than -1.
		consumerTest.setValue(-10)
		err = setConsumerConfigDefaults(ccfg, &scfg, &JSLimitOpts{}, &JetStreamAccountLimits{}, true)
		if err == nil {
			t.Fatal("Expected error for pedantic mode")
		}
		if consumerTest.defaultValue == -1 {
			require_Error(t, err, NewJSPedanticError(fmt.Errorf("%s must be set to -1", consumerTest.name)))
		} else {
			require_Error(t, err, NewJSPedanticError(fmt.Errorf("%s must not be negative", consumerTest.name)))
		}

		// Pedantic defaults if zero-value.
		consumerTest.setValue(0)
		err = setConsumerConfigDefaults(ccfg, &scfg, &JSLimitOpts{}, &JetStreamAccountLimits{}, true)
		if err != nil {
			require_NoError(t, err)
		}
		require_Equal(t, consumerTest.getValue(), consumerTest.defaultValue)

		// Non-pedantic defaults.
		consumerTest.setValue(-10)
		err = setConsumerConfigDefaults(ccfg, &scfg, &JSLimitOpts{}, &JetStreamAccountLimits{}, false)
		if err != nil {
			require_NoError(t, err)
		}
		require_Equal(t, consumerTest.getValue(), consumerTest.defaultValue)
	}

	// Pedantic errors if less than -1.
	ccfg.MaxAckPending = -10
	err = setConsumerConfigDefaults(ccfg, &scfg, &JSLimitOpts{}, &JetStreamAccountLimits{}, true)
	if err == nil {
		t.Fatal("Expected error for pedantic mode")
	}
	require_Error(t, err, NewJSPedanticError(errors.New("max_ack_pending must be set to -1")))

	// Pedantic defaults if zero-value.
	ccfg.MaxAckPending = 0
	err = setConsumerConfigDefaults(ccfg, &scfg, &JSLimitOpts{}, &JetStreamAccountLimits{}, true)
	if err != nil {
		require_NoError(t, err)
	}
	require_Equal(t, ccfg.MaxAckPending, JsDefaultMaxAckPending)

	// Non-pedantic defaults.
	ccfg.MaxAckPending = -10
	err = setConsumerConfigDefaults(ccfg, &scfg, &JSLimitOpts{}, &JetStreamAccountLimits{}, false)
	if err != nil {
		require_NoError(t, err)
	}
	require_Equal(t, ccfg.MaxAckPending, -1)
}

func TestJetStreamInvalidStreamOrConsumerName(t *testing.T) {
	s := RunBasicJetStreamServer(t)
	defer s.Shutdown()

	invalidChars := " \t\r\n\f.*>\\/"
	for _, c := range invalidChars {
		name := fmt.Sprintf("%c", c)
		scfg := &StreamConfig{Name: name}
		_, err := s.checkStreamCfg(scfg, nil, false)
		require_NotNil(t, err)
		require_Contains(t, err.Description, "can not contain")

		ccfg := &ConsumerConfig{Name: name}
		err = checkConsumerCfg(ccfg, nil, nil, nil, nil, false)
		require_NotNil(t, err)
		require_Contains(t, err.Description, "can not contain")

		ccfg = &ConsumerConfig{Durable: name}
		err = checkConsumerCfg(ccfg, nil, nil, nil, nil, false)
		require_NotNil(t, err)
		require_Contains(t, err.Description, "can not contain")
	}
}

func TestJetStreamPromoteMirrorDeletingOrigin(t *testing.T) {
	test := func(t *testing.T, replicas int) {
		var s *Server
		if replicas == 1 {
			s = RunBasicJetStreamServer(t)
			defer s.Shutdown()
		} else {
			c := createJetStreamClusterExplicit(t, "R3S", 3)
			defer c.shutdown()
			s = c.randomServer()
		}

		nc, js := jsClientConnect(t, s)
		defer nc.Close()

		_, err := jsStreamCreate(t, nc, &StreamConfig{
			Name:     "O",
			Subjects: []string{"foo"},
			Storage:  FileStorage,
			Replicas: replicas,
		})
		require_NoError(t, err)

		mirrorCfg := &StreamConfig{
			Name:     "M",
			Mirror:   &StreamSource{Name: "O"},
			Storage:  FileStorage,
			Replicas: replicas,
		}
		mirrorCfg, err = jsStreamCreate(t, nc, mirrorCfg)
		require_NoError(t, err)

		pubAck, err := js.Publish("foo", nil)
		require_NoError(t, err)
		require_Equal(t, pubAck.Sequence, 1)

		// Mirror should get message.
		checkFor(t, 2*time.Second, 200*time.Millisecond, func() error {
			_, err := js.GetMsg("M", 1)
			return err
		})

		// Trying to promote the mirror with a subject conflict should fail
		// because the origin stream is listening on that subject still.
		mirrorCfg.Mirror = nil
		mirrorCfg.Subjects = []string{"foo"}
		_, err = jsStreamUpdate(t, nc, mirrorCfg)
		require_Error(t, err)
		apiErr, ok := err.(*ApiError)
		require_True(t, ok)
		require_Equal(t, apiErr.ErrCode, uint16(JSStreamSubjectOverlapErr))

		// But if we delete the stream, the subject conflict goes away...
		err = js.DeleteStream("O")
		require_NoError(t, err)

		// ... so now it should work.
		mirrorCfg, err = jsStreamUpdate(t, nc, mirrorCfg)
		require_NoError(t, err)
		require_Len(t, len(mirrorCfg.Subjects), 1)
		require_Equal(t, mirrorCfg.Mirror, nil)

		// Make sure the mirror state has gone away.
		checkFor(t, 5*time.Second, 200*time.Millisecond, func() error {
			si, err := js.StreamInfo("M")
			if err != nil {
				return err
			}
			if si.Mirror != nil {
				return fmt.Errorf("expecting no mirror status, got %+v", si.Mirror)
			}
			return nil
		})

		// Now we should be able to publish into the newly promoted mirror.
		pubAck, err = js.Publish("foo", nil)
		require_NoError(t, err)
		require_Equal(t, pubAck.Sequence, 2)

		// ... and confirm that it was received in the mirror stream.
		_, err = js.GetMsg("M", 2)
		require_NoError(t, err)
	}

	t.Run("R1", func(t *testing.T) { test(t, 1) })
	t.Run("R3", func(t *testing.T) { test(t, 3) })
}

func TestJetStreamPromoteMirrorUpdatingOrigin(t *testing.T) {
	test := func(t *testing.T, replicas int) {
		var s *Server
		if replicas == 1 {
			s = RunBasicJetStreamServer(t)
			defer s.Shutdown()
		} else {
			c := createJetStreamClusterExplicit(t, "R3S", 3)
			defer c.shutdown()
			s = c.randomServer()
		}

		nc, js := jsClientConnect(t, s)
		defer nc.Close()

		originCfg, err := jsStreamCreate(t, nc, &StreamConfig{
			Name:     "O",
			Subjects: []string{"foo"},
			Storage:  FileStorage,
			Replicas: replicas,
		})
		require_NoError(t, err)

		mirrorCfg := &StreamConfig{
			Name:     "M",
			Mirror:   &StreamSource{Name: "O"},
			Storage:  FileStorage,
			Replicas: replicas,
		}
		mirrorCfg, err = jsStreamCreate(t, nc, mirrorCfg)
		require_NoError(t, err)

		pubAck, err := js.Publish("foo", nil)
		require_NoError(t, err)
		require_Equal(t, pubAck.Sequence, 1)

		// Mirror should get message.
		checkFor(t, 2*time.Second, 200*time.Millisecond, func() error {
			_, err := js.GetMsg("M", 1)
			return err
		})

		// Trying to promote the mirror with a subject conflict should fail
		// because the origin stream is listening on that subject still.
		mirrorCfg.Mirror = nil
		mirrorCfg.Subjects = []string{"foo"}
		_, err = jsStreamUpdate(t, nc, mirrorCfg)
		require_Error(t, err)
		apiErr, ok := err.(*ApiError)
		require_True(t, ok)
		require_Equal(t, apiErr.ErrCode, uint16(JSStreamSubjectOverlapErr))

		// But if we change the subjects on the origin stream, the subject
		// conflict goes away...
		originCfg.Subjects = []string{"bar"}
		_, err = jsStreamUpdate(t, nc, originCfg)
		require_NoError(t, err)

		// ... so now it should work.
		mirrorCfg, err = jsStreamUpdate(t, nc, mirrorCfg)
		require_NoError(t, err)
		require_Len(t, len(mirrorCfg.Subjects), 1)
		require_Equal(t, mirrorCfg.Mirror, nil)

		// Make sure the mirror state has gone away.
		checkFor(t, 5*time.Second, 200*time.Millisecond, func() error {
			si, err := js.StreamInfo("M")
			if err != nil {
				return err
			}
			if si.Mirror != nil {
				return fmt.Errorf("expecting no mirror status, got %+v", si.Mirror)
			}
			return nil
		})

		// Publishing into the original stream should no longer mirror over
		// onto the mirror stream...
		pubAck, err = js.Publish("bar", nil)
		require_NoError(t, err)
		require_Equal(t, pubAck.Sequence, 2)

		// ... therefore a new publish to the mirror stream should also have
		// sequence 2 and not 3.
		pubAck, err = js.Publish("foo", nil)
		require_NoError(t, err)
		require_Equal(t, pubAck.Sequence, 2)
	}

	t.Run("R1", func(t *testing.T) { test(t, 1) })
	t.Run("R3", func(t *testing.T) { test(t, 3) })
}

func TestJetStreamScheduledMirrorOrSource(t *testing.T) {
	test := func(t *testing.T, replicas int) {
		var s *Server
		if replicas == 1 {
			s = RunBasicJetStreamServer(t)
			defer s.Shutdown()
		} else {
			c := createJetStreamClusterExplicit(t, "R3S", 3)
			defer c.shutdown()
			s = c.randomServer()
		}

		nc, js := jsClientConnect(t, s)
		defer nc.Close()

		_, err := jsStreamCreate(t, nc, &StreamConfig{
			Name:              "M",
			Storage:           FileStorage,
			Replicas:          replicas,
			Mirror:            &StreamSource{Name: "O"},
			AllowMsgSchedules: true,
		})
		require_Error(t, err, NewJSMirrorWithMsgSchedulesError())

		_, err = jsStreamCreate(t, nc, &StreamConfig{
			Name:              "S",
			Storage:           FileStorage,
			Replicas:          replicas,
			Sources:           []*StreamSource{{Name: "O"}},
			AllowMsgSchedules: true,
		})
		require_Error(t, err, NewJSSourceWithMsgSchedulesError())

		_, err = jsStreamCreate(t, nc, &StreamConfig{
			Name:              "O",
			Subjects:          []string{"O.>"},
			Storage:           FileStorage,
			Replicas:          replicas,
			AllowMsgSchedules: true,
		})
		require_NoError(t, err)
		_, err = jsStreamCreate(t, nc, &StreamConfig{
			Name:     "M",
			Storage:  FileStorage,
			Replicas: replicas,
			Mirror:   &StreamSource{Name: "O"},
		})
		require_NoError(t, err)
		_, err = jsStreamCreate(t, nc, &StreamConfig{
			Name:     "S",
			Storage:  FileStorage,
			Replicas: replicas,
			Sources:  []*StreamSource{{Name: "O"}},
		})
		require_NoError(t, err)

		m := nats.NewMsg("O.schedule")
		pattern := fmt.Sprintf("@at %s", time.Now().Add(time.Second).Format(time.RFC3339Nano))
		m.Header.Set("Nats-Schedule", pattern)
		m.Header.Set("Nats-Schedule-Target", "O.target")
		_, err = js.PublishMsg(m)
		require_NoError(t, err)

		check := func(streamName string) error {
			// Request message 2 first.
			// If a rollup was performed due to it, fetching the 1st message next will fail.
			sm, err := js.GetMsg(streamName, 2)
			if err != nil {
				return err
			}
			if sm.Subject != "O.target" {
				return fmt.Errorf("expected subject 'O.target', got '%s'", sm.Subject)
			}
			sm, err = js.GetMsg(streamName, 1)
			if err != nil {
				return err
			}
			if sm.Subject != "O.schedule" {
				return fmt.Errorf("expected subject 'O.schedule', got '%s'", sm.Subject)
			}
			return nil
		}

		checkFor(t, 5*time.Second, 200*time.Millisecond, func() error {
			if err = check("M"); err != nil {
				return fmt.Errorf("mirror: %v", err)
			}
			if err = check("S"); err != nil {
				return fmt.Errorf("source: %v", err)
			}
			return nil
		})
	}

	for _, replicas := range []int{1, 3} {
		t.Run(fmt.Sprintf("R%d", replicas), func(t *testing.T) { test(t, replicas) })
	}
}

func TestJetStreamRollupMirrorOrSource(t *testing.T) {
	test := func(t *testing.T, replicas int) {
		var s *Server
		if replicas == 1 {
			s = RunBasicJetStreamServer(t)
			defer s.Shutdown()
		} else {
			c := createJetStreamClusterExplicit(t, "R3S", 3)
			defer c.shutdown()
			s = c.randomServer()
		}

		nc, js := jsClientConnect(t, s)
		defer nc.Close()

		_, err := jsStreamCreate(t, nc, &StreamConfig{
			Name:        "O",
			Subjects:    []string{"O"},
			Storage:     FileStorage,
			Replicas:    replicas,
			AllowRollup: true,
		})
		require_NoError(t, err)
		_, err = jsStreamCreate(t, nc, &StreamConfig{
			Name:     "M",
			Storage:  FileStorage,
			Replicas: replicas,
			Mirror:   &StreamSource{Name: "O"},
		})
		require_NoError(t, err)
		_, err = jsStreamCreate(t, nc, &StreamConfig{
			Name:     "S",
			Storage:  FileStorage,
			Replicas: replicas,
			Sources:  []*StreamSource{{Name: "O"}},
		})
		require_NoError(t, err)

		m := nats.NewMsg("O")
		m.Header.Set("Nats-Rollup", "sub")
		_, err = js.PublishMsg(m)
		require_NoError(t, err)

		checkSequence := func(seq uint64) {
			check := func(streamName string) error {
				si, err := js.StreamInfo(streamName)
				if err != nil {
					return err
				}
				if si.State.Msgs != seq || si.State.FirstSeq != 1 || si.State.LastSeq != seq {
					return fmt.Errorf("expected seq %d, got %v", seq, si.State)
				}
				return nil
			}

			checkFor(t, 5*time.Second, 200*time.Millisecond, func() error {
				if err = check("M"); err != nil {
					return fmt.Errorf("mirror: %v", err)
				}
				if err = check("S"); err != nil {
					return fmt.Errorf("source: %v", err)
				}
				return nil
			})
		}
		checkSequence(1)

		_, err = js.PublishMsg(m)
		require_NoError(t, err)
		checkSequence(2)
	}

	for _, replicas := range []int{1, 3} {
		t.Run(fmt.Sprintf("R%d", replicas), func(t *testing.T) { test(t, replicas) })
	}
}

func TestJetStreamOfflineStreamAndConsumerAfterDowngrade(t *testing.T) {
	s := RunBasicJetStreamServer(t)
	defer s.Shutdown()
	port := s.getOpts().Port
	sd := s.JetStreamConfig().StoreDir

	_, err := s.globalAccount().addStream(&StreamConfig{
		Name:     "DowngradeStreamTest",
		Storage:  FileStorage,
		Replicas: 1,
		Metadata: map[string]string{"_nats.req.level": strconv.Itoa(math.MaxInt)},
	})
	require_NoError(t, err)

	s.Shutdown()
	s = RunJetStreamServerOnPort(port, sd)
	defer s.Shutdown()

	nc := clientConnectToServer(t, s)
	defer nc.Close()

	offlineReason := fmt.Sprintf("unsupported - required API level: %d, current API level: %d", math.MaxInt, JSApiLevel)
	msg, err := nc.Request(fmt.Sprintf(JSApiStreamInfoT, "DowngradeStreamTest"), nil, time.Second)
	require_NoError(t, err)
	var si JSApiStreamInfoResponse
	require_NoError(t, json.Unmarshal(msg.Data, &si))
	require_NotNil(t, si.Error)
	require_Error(t, si.Error, NewJSStreamOfflineReasonError(errors.New(offlineReason)))

	var sn JSApiStreamNamesResponse
	msg, err = nc.Request(JSApiStreams, nil, time.Second)
	require_NoError(t, err)
	require_NoError(t, json.Unmarshal(msg.Data, &sn))
	require_Len(t, len(sn.Streams), 1)
	require_Equal(t, sn.Streams[0], "DowngradeStreamTest")

	var sl JSApiStreamListResponse
	msg, err = nc.Request(JSApiStreamList, nil, time.Second)
	require_NoError(t, err)
	require_NoError(t, json.Unmarshal(msg.Data, &sl))
	require_Len(t, len(sl.Streams), 0)
	require_Len(t, len(sl.Missing), 1)
	require_Equal(t, sl.Missing[0], "DowngradeStreamTest")
	require_Len(t, len(sl.Offline), 1)
	require_Equal(t, sl.Offline["DowngradeStreamTest"], offlineReason)

	mset, err := s.globalAccount().lookupStream("DowngradeStreamTest")
	require_NoError(t, err)
	require_True(t, mset.closed.Load())
	require_Equal(t, mset.offlineReason, offlineReason)
	require_NoError(t, mset.delete())

	s.Shutdown()
	s = RunJetStreamServerOnPort(port, sd)
	defer s.Shutdown()

	_, err = s.globalAccount().addStream(&StreamConfig{
		Name:     "DowngradeConsumerTest",
		Storage:  FileStorage,
		Replicas: 1,
	})
	require_NoError(t, err)
	mset, err = s.globalAccount().lookupStream("DowngradeConsumerTest")
	require_NoError(t, err)
	_, err = mset.addConsumer(&ConsumerConfig{
		Name:     "DowngradeConsumerTest",
		Metadata: map[string]string{"_nats.req.level": strconv.Itoa(math.MaxInt)},
	})
	require_NoError(t, err)

	s.Shutdown()
	s = RunJetStreamServerOnPort(port, sd)
	defer s.Shutdown()

	mset, err = s.globalAccount().lookupStream("DowngradeConsumerTest")
	require_NoError(t, err)
	require_True(t, mset.closed.Load())
	require_Equal(t, mset.offlineReason, "stopped - unsupported consumer \"DowngradeConsumerTest\"")

	obs := mset.getPublicConsumers()
	require_Len(t, len(obs), 1)
	require_True(t, obs[0].isClosed())
	require_Equal(t, obs[0].offlineReason, offlineReason)

	msg, err = nc.Request(fmt.Sprintf(JSApiConsumerInfoT, "DowngradeConsumerTest", "DowngradeConsumerTest"), nil, time.Second)
	require_NoError(t, err)
	var ci JSApiConsumerInfoResponse
	require_NoError(t, json.Unmarshal(msg.Data, &ci))
	require_NotNil(t, ci.Error)
	require_Error(t, ci.Error, NewJSConsumerOfflineReasonError(errors.New(offlineReason)))

	var cn JSApiConsumerNamesResponse
	msg, err = nc.Request(fmt.Sprintf(JSApiConsumersT, "DowngradeConsumerTest"), nil, time.Second)
	require_NoError(t, err)
	require_NoError(t, json.Unmarshal(msg.Data, &cn))
	require_Len(t, len(cn.Consumers), 1)
	require_Equal(t, cn.Consumers[0], "DowngradeConsumerTest")

	var cl JSApiConsumerListResponse
	msg, err = nc.Request(fmt.Sprintf(JSApiConsumerListT, "DowngradeConsumerTest"), nil, time.Second)
	require_NoError(t, err)
	require_NoError(t, json.Unmarshal(msg.Data, &cl))
	require_Len(t, len(cl.Consumers), 0)
	require_Len(t, len(cl.Missing), 1)
	require_Equal(t, cl.Missing[0], "DowngradeConsumerTest")
	require_Len(t, len(cl.Offline), 1)
	require_Equal(t, cl.Offline["DowngradeConsumerTest"], offlineReason)
}

func TestJetStreamPersistModeAsync(t *testing.T) {
	s := RunBasicJetStreamServer(t)
	defer s.Shutdown()

	nc, js := jsClientConnect(t, s)
	defer nc.Close()

	cfg := &StreamConfig{
		Name:        "TEST",
		Subjects:    []string{"foo"},
		Storage:     MemoryStorage,
		PersistMode: AsyncPersistMode,
	}
	_, err := jsStreamCreate(t, nc, cfg)
	require_Error(t, err, NewJSStreamInvalidConfigError(fmt.Errorf("async persist mode is only supported on file storage")))

	cfg.Storage = FileStorage
	si, err := jsStreamCreate(t, nc, cfg)
	require_NoError(t, err)
	require_Equal(t, si.PersistMode, AsyncPersistMode)

	pubAck, err := js.Publish("foo", nil)
	require_NoError(t, err)
	require_Equal(t, pubAck.Sequence, 1)

	mset, err := s.globalAccount().lookupStream("TEST")
	require_NoError(t, err)
	fs := mset.store.(*fileStore)
	fs.mu.RLock()
	asyncFlush := fs.fcfg.AsyncFlush
	fs.mu.RUnlock()
	require_True(t, asyncFlush)

	cfg.PersistMode = DefaultPersistMode
	_, err = jsStreamUpdate(t, nc, cfg)
	require_Error(t, err, NewJSStreamInvalidConfigError(fmt.Errorf("stream configuration update can not change persist mode")))
}

func TestJetStreamRemoveTTLOnRemoveMsg(t *testing.T) {
	for _, storageType := range []nats.StorageType{nats.FileStorage, nats.MemoryStorage} {
		t.Run(storageType.String(), func(t *testing.T) {
			s := RunBasicJetStreamServer(t)
			defer s.Shutdown()

			nc, js := jsClientConnect(t, s)
			defer nc.Close()

			_, err := js.AddStream(&nats.StreamConfig{
				Name:        "TEST",
				Subjects:    []string{"foo"},
				Storage:     storageType,
				AllowMsgTTL: true,
			})
			require_NoError(t, err)

			_, err = js.Publish("foo", nil, nats.MsgTTL(time.Hour))
			require_NoError(t, err)

			mset, err := s.globalAccount().lookupStream("TEST")
			require_NoError(t, err)

			validateTTLCount := func(count uint64) {
				store := mset.Store()
				switch storageType {
				case nats.FileStorage:
					fs := store.(*fileStore)
					fs.mu.RLock()
					defer fs.mu.RUnlock()
					require_Equal(t, fs.ttls.Count(), count)
				case nats.MemoryStorage:
					ms := store.(*memStore)
					ms.mu.RLock()
					defer ms.mu.RUnlock()
					require_Equal(t, ms.ttls.Count(), count)
				}
			}
			validateTTLCount(1)

			require_NoError(t, js.DeleteMsg("TEST", 1))
			validateTTLCount(0)
		})
	}
}

func TestJetStreamMessageTTLNotExpiring(t *testing.T) {
	for _, storageType := range []nats.StorageType{nats.FileStorage, nats.MemoryStorage} {
		t.Run(storageType.String(), func(t *testing.T) {
			s := RunBasicJetStreamServer(t)
			defer s.Shutdown()

			nc, js := jsClientConnect(t, s)
			defer nc.Close()

			_, err := js.AddStream(&nats.StreamConfig{
				Name:        "TEST",
				Subjects:    []string{"foo"},
				Storage:     storageType,
				AllowMsgTTL: true,
			})
			require_NoError(t, err)

			// Triggers the expiry timer once, and needs to be reset to trigger earlier.
			_, err = js.Publish("foo", nil, nats.MsgTTL(time.Hour))
			require_NoError(t, err)

			mset, err := s.globalAccount().lookupStream("TEST")
			require_NoError(t, err)

			// Storing messages with a TTL would continuously reset the timer.
			var wg sync.WaitGroup
			wg.Add(1)
			defer wg.Wait()
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			go func() {
				defer wg.Done()
				for {
					select {
					case <-ctx.Done():
						return
					case <-time.After(100 * time.Millisecond):
						ttl := time.Hour.Nanoseconds()
						store := mset.Store()
						store.StoreMsg("foo", nil, nil, ttl)
					}
				}
			}()

			// The message should be expired timely.
			pubAck, err := js.Publish("foo", nil, nats.MsgTTL(time.Second))
			require_NoError(t, err)
			checkFor(t, 3*time.Second, 100*time.Millisecond, func() error {
				_, err = js.GetMsg("TEST", pubAck.Sequence)
				if err == nil {
					return fmt.Errorf("message not removed yet")
				}
				if !errors.Is(err, nats.ErrMsgNotFound) {
					return err
				}
				return nil
			})
		})
	}
}

func TestJetStreamScheduledMessageNotTriggering(t *testing.T) {
	for _, storageType := range []StorageType{FileStorage, MemoryStorage} {
		t.Run(storageType.String(), func(t *testing.T) {
			s := RunBasicJetStreamServer(t)
			defer s.Shutdown()

			nc, js := jsClientConnect(t, s)
			defer nc.Close()

			_, err := jsStreamCreate(t, nc, &StreamConfig{
				Name:              "TEST",
				Subjects:          []string{"foo.>"},
				Storage:           storageType,
				AllowMsgSchedules: true,
			})
			require_NoError(t, err)

			delay := func(d time.Duration) string {
				return fmt.Sprintf("@at %s", time.Now().Add(d).Format(time.RFC3339Nano))
			}

			// Triggers the schedule timer once, and needs to be reset to trigger earlier.
			m := nats.NewMsg("foo.schedule.first")
			m.Header.Set("Nats-Schedule", delay(time.Hour))
			m.Header.Set("Nats-Schedule-Target", "foo.msg")
			_, err = js.PublishMsg(m)
			require_NoError(t, err)

			// Storing messages with a schedule would continuously reset the timer.
			var wg sync.WaitGroup
			wg.Add(1)
			defer wg.Wait()
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			go func() {
				defer wg.Done()
				var i int
				for {
					select {
					case <-ctx.Done():
						return
					case <-time.After(100 * time.Millisecond):
						i++
						ms := nats.NewMsg(fmt.Sprintf("foo.schedule.%d", i))
						ms.Header.Set("Nats-Schedule", delay(time.Hour))
						ms.Header.Set("Nats-Schedule-Target", "foo.msg")
						js.PublishMsg(ms)
					}
				}
			}()

			// The message should be scheduled timely.
			m = nats.NewMsg("foo.schedule.validate")
			m.Header.Set("Nats-Schedule", delay(time.Second))
			m.Header.Set("Nats-Schedule-Target", "foo.msg")
			_, err = js.PublishMsg(m)
			require_NoError(t, err)
			pubAck, err := js.PublishMsg(m)
			require_NoError(t, err)
			checkFor(t, 3*time.Second, 100*time.Millisecond, func() error {
				_, err = js.GetMsg("TEST", pubAck.Sequence)
				if err == nil {
					return fmt.Errorf("message not removed yet")
				}
				if !errors.Is(err, nats.ErrMsgNotFound) {
					return err
				}
				return nil
			})
		})
	}
}

func TestJetStreamScheduledMessageIncompatibleSettings(t *testing.T) {
	s := RunBasicJetStreamServer(t)
	defer s.Shutdown()

	nc := clientConnectToServer(t, s)
	defer nc.Close()

	// DiscardNew not allowed.
	_, err := jsStreamCreate(t, nc, &StreamConfig{
		Name:              "TEST",
		Subjects:          []string{"foo"},
		Storage:           FileStorage,
		AllowMsgSchedules: true,
		Discard:           DiscardNew,
	})
	require_Error(t, err, NewJSStreamInvalidConfigError(fmt.Errorf("message scheduling cannot use discard new")))

	// AllowRollup required.
	_, err = addStreamPedanticWithError(t, nc, &StreamConfigRequest{
		StreamConfig: StreamConfig{
			Name:              "TEST",
			Subjects:          []string{"foo"},
			Storage:           FileStorage,
			AllowMsgSchedules: true,
			AllowRollup:       false,
		},
		Pedantic: true,
	})
	require_Error(t, err, NewJSStreamInvalidConfigError(fmt.Errorf("message scheduling cannot be set if roll-ups are disabled")))
}

func TestJetStreamScheduledPurgeHeadersRequiresSchedulingEnabled(t *testing.T) {
	s := RunBasicJetStreamServer(t)
	defer s.Shutdown()

	nc, js := jsClientConnect(t, s)
	defer nc.Close()

	_, err := jsStreamCreate(t, nc, &StreamConfig{
		Name:              "TEST",
		Storage:           FileStorage,
		Subjects:          []string{"foo.*"},
		AllowMsgSchedules: false,
	})
	require_NoError(t, err)

	_, err = js.Publish("foo.victim", []byte("OK"))
	require_NoError(t, err)

	m := nats.NewMsg("foo.attacker")
	m.Header.Set(JSScheduleNext, JSScheduleNextPurge)
	m.Header.Set(JSScheduler, "foo.victim")

	_, err = js.PublishMsg(m)
	require_Error(t, err, NewJSMessageSchedulesDisabledError())
}

func TestJetStreamScheduledMessageNotDeactivated(t *testing.T) {
	for _, storageType := range []StorageType{FileStorage, MemoryStorage} {
		t.Run(storageType.String(), func(t *testing.T) {
			s := RunBasicJetStreamServer(t)
			defer s.Shutdown()

			nc, js := jsClientConnect(t, s)
			defer nc.Close()

			_, err := jsStreamCreate(t, nc, &StreamConfig{
				Name:              "TEST",
				Subjects:          []string{"foo.>"},
				Storage:           storageType,
				AllowMsgSchedules: true,
			})
			require_NoError(t, err)

			delay := func(d time.Duration) string {
				return fmt.Sprintf("@at %s", time.Now().Add(d).Format(time.RFC3339Nano))
			}

			// Message should be scheduled.
			m := nats.NewMsg("foo.schedule")
			m.Header.Set("Nats-Schedule", delay(time.Second))
			m.Header.Set("Nats-Schedule-Target", "foo.msg1")
			_, err = js.PublishMsg(m)
			require_NoError(t, err)
			checkFor(t, 2*time.Second, 100*time.Millisecond, func() error {
				_, err = js.GetLastMsg("TEST", "foo.msg1")
				if err != nil {
					return err
				}
				return nil
			})

			// A message with a schedule is published.
			m = nats.NewMsg("foo.schedule")
			m.Header.Set("Nats-Schedule", delay(time.Second))
			m.Header.Set("Nats-Schedule-Target", "foo.msg2")
			_, err = js.PublishMsg(m)
			require_NoError(t, err)

			// But, a publish that is not a schedule should deactivate it.
			_, err = js.Publish("foo.schedule", nil)
			require_NoError(t, err)

			// Wait for some time, and confirm the schedule wasn't triggered.
			time.Sleep(1500 * time.Millisecond)
			_, err = js.GetLastMsg("TEST", "foo.msg2")
			require_Error(t, err, nats.ErrMsgNotFound)
		})
	}
}

func TestJetStreamScheduledMessageDeliveryPreservesTargetSchedule(t *testing.T) {
	for _, storageType := range []StorageType{FileStorage, MemoryStorage} {
		t.Run(storageType.String(), func(t *testing.T) {
			s := RunBasicJetStreamServer(t)
			defer s.Shutdown()

			nc, js := jsClientConnect(t, s)
			defer nc.Close()

			_, err := jsStreamCreate(t, nc, &StreamConfig{
				Name:              "TEST",
				Subjects:          []string{"foo.>"},
				Storage:           storageType,
				AllowMsgSchedules: true,
			})
			require_NoError(t, err)

			delay := func(d time.Duration) string {
				return fmt.Sprintf("@at %s", time.Now().Add(d).Format(time.RFC3339Nano))
			}

			// Schedule on foo.A fires quickly and delivers onto foo.B.
			mA := nats.NewMsg("foo.A")
			mA.Header.Set("Nats-Schedule", delay(500*time.Millisecond))
			mA.Header.Set("Nats-Schedule-Target", "foo.B")
			_, err = js.PublishMsg(mA)
			require_NoError(t, err)

			// Independent schedule on foo.B fires later and delivers onto foo.C.
			// Storing the fired delivery from foo.A onto foo.B must not tear
			// down foo.B's own schedule.
			mB := nats.NewMsg("foo.B")
			mB.Header.Set("Nats-Schedule", delay(2*time.Second))
			mB.Header.Set("Nats-Schedule-Target", "foo.C")
			_, err = js.PublishMsg(mB)
			require_NoError(t, err)

			// Wait for the foo.A schedule to fire onto foo.B.
			checkFor(t, 2*time.Second, 200*time.Millisecond, func() error {
				_, err = js.GetLastMsg("TEST", "foo.B")
				return err
			})

			// foo.B's schedule must still fire onto foo.C.
			checkFor(t, 4*time.Second, 200*time.Millisecond, func() error {
				_, err = js.GetLastMsg("TEST", "foo.C")
				return err
			})
		})
	}
}

func TestJetStreamScheduledMessageParse(t *testing.T) {
	// @at <ts>
	t.Run("@at", func(t *testing.T) {
		ts := time.Now().UTC()
		sts, repeat, ok := parseMsgSchedule(fmt.Sprintf("@at %s", ts.Format(time.RFC3339Nano)), nil, 0)
		require_True(t, ok)
		require_False(t, repeat)
		require_Equal(t, ts, sts)
	})

	// @every <duration>
	t.Run("@every", func(t *testing.T) {
		now := time.Now().UTC().Round(time.Second)
		sts, repeat, ok := parseMsgSchedule("@every 5s", nil, now.UnixNano())
		require_True(t, ok)
		require_True(t, repeat)
		require_Equal(t, now.Add(5*time.Second), sts)

		// A schedule on an interval should not spam loads of times if it hasn't run in a long while.
		now = time.Now().UTC().Round(time.Second)
		sts, repeat, ok = parseMsgSchedule("@every 5s", nil, 0)
		require_True(t, ok)
		require_True(t, repeat)
		require_True(t, !sts.Before(now.Add(5*time.Second)))

		// A schedule can only run at least once every second.
		_, _, ok = parseMsgSchedule("@every 999ms", nil, 0)
		require_False(t, ok)

		// The next fire time anchors on the previous fire time rounded to the
		// nearest second, plus the interval.
		const interval = 1500 * time.Millisecond
		anchor := time.Now().UTC().Add(time.Second)
		sts, repeat, ok = parseMsgSchedule("@every 1500ms", nil, anchor.UnixNano())
		require_True(t, ok)
		require_True(t, repeat)
		require_Equal(t, sts.UnixNano(), anchor.Round(time.Second).Add(interval).UnixNano())

		// Re-arming from that fire time rounds again before adding the interval.
		prev := sts
		sts, repeat, ok = parseMsgSchedule("@every 1500ms", nil, sts.UnixNano())
		require_True(t, ok)
		require_True(t, repeat)
		require_Equal(t, sts.UnixNano(), prev.Round(time.Second).Add(interval).UnixNano())
	})

	// <cron> pattern
	t.Run("cron", func(t *testing.T) {
		now := time.Now().UTC().Round(time.Second)
		sts, repeat, ok := parseMsgSchedule("* * * * * *", nil, now.UnixNano())
		require_True(t, ok)
		require_True(t, repeat)
		require_Equal(t, now.Add(time.Second), sts)

		// A schedule based on a cron should run the earliest "next" second.
		now = time.Now().UTC().Truncate(time.Second).Add(time.Second - time.Nanosecond)
		sts, repeat, ok = parseMsgSchedule("* * * * * *", nil, now.UnixNano())
		require_True(t, ok)
		require_True(t, repeat)
		require_Equal(t, now.Truncate(time.Second).Add(time.Second), sts)

		// A schedule based on cron should not spam loads of times if it hasn't run in a long while.
		now = time.Now().UTC().Round(time.Second)
		sts, repeat, ok = parseMsgSchedule("* * * * * *", nil, 0)
		require_True(t, ok)
		require_True(t, repeat)
		require_True(t, !sts.Before(now.Add(time.Second)))

		// Predefined cron patterns, e.g. @hourly.
		for _, p := range []struct {
			pattern string
			delay   func(time.Time) time.Time
		}{
			{pattern: "@yearly", delay: func(t time.Time) time.Time {
				return t.AddDate(1, 0, 0)
			}},
			{pattern: "@annually", delay: func(t time.Time) time.Time {
				return t.AddDate(1, 0, 0)
			}},
			{pattern: "@monthly", delay: func(t time.Time) time.Time {
				return t.AddDate(0, 1, 0)
			}},
			{pattern: "@weekly", delay: func(t time.Time) time.Time {
				return t.AddDate(0, 0, 7)
			}},
			{pattern: "@daily", delay: func(t time.Time) time.Time {
				return t.AddDate(0, 0, 1)
			}},
			{pattern: "@midnight", delay: func(t time.Time) time.Time {
				return t.AddDate(0, 0, 1)
			}},
			{pattern: "@hourly", delay: func(t time.Time) time.Time {
				return t.Add(time.Hour)
			}},
		} {
			t.Run(p.pattern, func(t *testing.T) {
				// Skip ahead "current time", so we can test the added delay is correct.
				now = time.Date(time.Now().Year()+2, 1, 1, 0, 0, 0, 0, time.UTC)
				// Also ensure we're on a Sunday of this month, to test "@weekly".
				if p.pattern == "@weekly" {
					for now.Weekday() != 0 {
						now = now.AddDate(0, 0, 1)
					}
				}
				sts, repeat, ok = parseMsgSchedule(p.pattern, nil, now.UnixNano())
				require_True(t, ok)
				require_True(t, repeat)
				require_Equal(t, p.delay(now), sts)
			})
		}
	})

	// <cron> pattern with time zone
	t.Run("cron_tz", func(t *testing.T) {
		loc, err := time.LoadLocation("Europe/Amsterdam")
		require_NoError(t, err)

		now := time.Now().UTC().Round(time.Second)
		sts, repeat, ok := parseMsgSchedule("* * * * * *", loc, now.UnixNano())
		require_True(t, ok)
		require_True(t, repeat)
		require_Equal(t, now.In(loc).Add(time.Second).String(), sts.String())
	})
}

func TestJetStreamDirectGetBatchParallelWriteDeadlock(t *testing.T) {
	s := RunBasicJetStreamServer(t)
	defer s.Shutdown()

	nc, js := jsClientConnect(t, s)
	defer nc.Close()

	_, err := js.AddStream(&nats.StreamConfig{
		Name:        "TEST",
		Subjects:    []string{"foo"},
		Storage:     nats.FileStorage,
		AllowDirect: true,
	})
	require_NoError(t, err)

	mset, err := s.globalAccount().lookupStream("TEST")
	require_NoError(t, err)
	for range 2 {
		require_NoError(t, mset.processJetStreamMsg("foo", _EMPTY_, nil, nil, 0, 0, nil, false, true))
	}

	// We'll lock the message blocks such that we can't read, but NumPending should still function.
	fs := mset.store.(*fileStore)
	fs.lockAllMsgBlocks()
	total, validThrough, err := fs.NumPending(1, _EMPTY_, false)
	require_NoError(t, err)
	require_Equal(t, total, 2)
	require_Equal(t, validThrough, 2)

	// We'll now run things in the following order:
	// - do a read through Direct Batch Get, which is blocked by the message blocks being locked
	// - do a write in parallel, which is blocked by the read to complete
	// - unlock the message blocks while both read and write goroutines are active
	// If there's no deadlock the read and write will complete, and 3 messages will end up in the stream.
	var wg, read sync.WaitGroup
	read.Add(1)
	wg.Add(1)
	go func() {
		read.Done()
		mset.getDirectRequest(&JSApiMsgGetRequest{Seq: 1, Batch: 2}, _EMPTY_)
	}()
	go func() {
		// Make sure we enter getDirectRequest first.
		read.Wait()
		<-time.After(100 * time.Millisecond)
		wg.Done()
		mset.processJetStreamMsg("foo", _EMPTY_, nil, nil, 0, 0, nil, false, true)
	}()
	go func() {
		// Run some time after we've entered processJetStreamMsg above.
		wg.Wait()
		<-time.After(100 * time.Millisecond)
		fs.unlockAllMsgBlocks()
	}()
	read.Wait()
	checkFor(t, 2*time.Second, 100*time.Millisecond, func() error {
		if msgs := mset.state().Msgs; msgs != 3 {
			return fmt.Errorf("expected 3 msgs, got %d", msgs)
		}
		return nil
	})
}

func TestJetStreamMaxConcurrentIO(t *testing.T) {
	for _, test := range []struct {
		Name    string
		Given   int
		Allowed bool
	}{
		{"ConfiguredMin", minConcurrentIOs, true},
		{"ConfiguredMid", 512, true},
		{"ConfiguredMax", maxConcurrentIOs, true},
		{"TooLow", -1, false},
		{"TooHigh", 10_000, false},
	} {
		t.Run(test.Name, func(t *testing.T) {
			storeDir := t.TempDir()
			defer func() {
				if test.Allowed {
					return
				}
				if err := recover(); err == nil {
					t.Fatalf("should have panicked at startup")
				}
			}()

			conf := createConfFile(t, fmt.Appendf(nil, `
				listen: 127.0.0.1:-1
				jetstream: {
					max_mem_store: 2MB
					max_file_store: 8MB
					store_dir: '%s'
					max_concurrent_io: %d
				}
			`, storeDir, test.Given))
			s, _ := RunServerWithConfig(conf)
			defer s.Shutdown()

			require_True(t, s.ReadyForConnections(5*time.Second))
			require_Equal(t, s.dios.cap(), test.Given)
		})
	}
}

func TestJetStreamReloadMetaCompact(t *testing.T) {
	storeDir := t.TempDir()

	conf := createConfFile(t, []byte(fmt.Sprintf(`
		listen: 127.0.0.1:-1
		jetstream: {
			max_mem_store: 2MB
			max_file_store: 8MB
			store_dir: '%s'
		}
	`, storeDir)))
	s, _ := RunServerWithConfig(conf)
	defer s.Shutdown()

	require_Equal(t, s.getOpts().JetStreamMetaCompact, 0)

	reloadUpdateConfig(t, s, conf, fmt.Sprintf(`
		listen: 127.0.0.1:-1
		jetstream: {
			max_mem_store: 2MB
			max_file_store: 8MB
			store_dir: '%s'
			meta_compact: 100
		}
	`, storeDir))

	require_Equal(t, s.getOpts().JetStreamMetaCompact, 100)

	reloadUpdateConfig(t, s, conf, fmt.Sprintf(`
		listen: 127.0.0.1:-1
		jetstream: {
			max_mem_store: 2MB
			max_file_store: 8MB
			store_dir: '%s'
			meta_compact: 0
		}
	`, storeDir))

	require_Equal(t, s.getOpts().JetStreamMetaCompact, 0)
}

// https://github.com/nats-io/nats-server/issues/7511
func TestJetStreamImplicitRePublishAfterSubjectTransform(t *testing.T) {
	s := RunBasicJetStreamServer(t)
	defer s.Shutdown()

	nc, js := jsClientConnect(t, s)
	defer nc.Close()

	cfg := &nats.StreamConfig{
		Name:             "TEST",
		Subjects:         []string{"a.>", "c.>"},
		SubjectTransform: &nats.SubjectTransformConfig{Source: "a.>", Destination: "b.>"},
		RePublish:        &nats.RePublish{Destination: ">"}, // Implicitly RePublish 'b.>'.
	}
	// Forms a cycle since the RePublish captures both 'a.>' and 'c.>'
	_, err := js.AddStream(cfg)
	require_Error(t, err, NewJSStreamInvalidConfigError(fmt.Errorf("stream configuration for republish destination forms a cycle")))

	// Doesn't form a cycle as 'a.>' is mapped to 'b.>'. A RePublish for '>' can be translated to 'b.>'.
	cfg.Subjects = []string{"a.>"}
	_, err = js.AddStream(cfg)
	require_NoError(t, err)

	sub, err := nc.SubscribeSync("b.>")
	require_NoError(t, err)
	defer sub.Drain()

	// The published message should be transformed and RePublished.
	_, err = js.Publish("a.hello", nil)
	require_NoError(t, err)
	msg, err := sub.NextMsg(time.Second)
	require_NoError(t, err)
	require_Equal(t, msg.Subject, "b.hello")

	// Forms a cycle since the implicit RePublish on 'b.>' is lost.
	// The RePublish would now mean publishing to 'c.>' which is a cycle.
	cfg.Subjects = []string{"c.>"}
	_, err = js.UpdateStream(cfg)
	require_Error(t, err, NewJSStreamInvalidConfigError(fmt.Errorf("stream configuration for republish destination forms a cycle")))
}

func TestJetStreamStreamMirrorWithoutDuplicateWindow(t *testing.T) {
	s := RunBasicJetStreamServer(t)
	defer s.Shutdown()

	nc, js := jsClientConnect(t, s)
	defer nc.Close()

	_, err := js.AddStream(&nats.StreamConfig{
		Name:       "TEST",
		Subjects:   []string{"foo"},
		Duplicates: time.Second,
	})
	require_NoError(t, err)

	pubAck, err := js.Publish("foo", nil, nats.MsgId("msgId"))
	require_NoError(t, err)
	require_Equal(t, pubAck.Sequence, 1)
	require_False(t, pubAck.Duplicate)

	pubAck, err = js.Publish("foo", nil, nats.MsgId("msgId"))
	require_NoError(t, err)
	require_Equal(t, pubAck.Sequence, 1)
	require_True(t, pubAck.Duplicate)

	time.Sleep(1200 * time.Millisecond)
	pubAck, err = js.Publish("foo", nil, nats.MsgId("msgId"))
	require_NoError(t, err)
	require_Equal(t, pubAck.Sequence, 2)
	require_False(t, pubAck.Duplicate)

	_, err = js.AddStream(&nats.StreamConfig{
		Name:       "M",
		Mirror:     &nats.StreamSource{Name: "TEST"},
		Duplicates: 0,
	})
	require_NoError(t, err)

	checkFor(t, 2*time.Second, 100*time.Millisecond, func() error {
		mset, err := s.globalAccount().lookupStream("M")
		if err != nil {
			return err
		}
		state := mset.state()
		if state.Msgs != 2 || state.FirstSeq != 1 || state.LastSeq != 2 {
			return fmt.Errorf("incorrect state: %v", state)
		}
		return nil
	})
}

func TestJetStreamStreamMirrorIgnoresUpstreamDuplicateMsgIds(t *testing.T) {
	s := RunBasicJetStreamServer(t)
	defer s.Shutdown()

	nc, js := jsClientConnect(t, s)
	defer nc.Close()

	_, err := js.AddStream(&nats.StreamConfig{
		Name:       "TEST",
		Storage:    nats.FileStorage,
		Subjects:   []string{"foo"},
		Duplicates: 100 * time.Millisecond,
	})
	require_NoError(t, err)

	pubAck, err := js.Publish("foo", nil, nats.MsgId("X"))
	require_NoError(t, err)
	require_Equal(t, pubAck.Sequence, 1)
	require_False(t, pubAck.Duplicate)

	// Wait for upstream's dedup window to expire so the next publish lands as
	// a fresh message at sseq=2 with the same Nats-Msg-Id.
	time.Sleep(200 * time.Millisecond)

	pubAck, err = js.Publish("foo", nil, nats.MsgId("X"))
	require_NoError(t, err)
	require_Equal(t, pubAck.Sequence, 2)
	require_False(t, pubAck.Duplicate)

	// Mirror must replicate both messages even though they share Nats-Msg-Id.
	_, err = js.AddStream(&nats.StreamConfig{
		Name:       "M",
		Storage:    nats.FileStorage,
		Mirror:     &nats.StreamSource{Name: "TEST"},
		Duplicates: 2 * time.Minute,
	})
	require_NoError(t, err)

	checkFor(t, 2*time.Second, 50*time.Millisecond, func() error {
		mset, err := s.globalAccount().lookupStream("M")
		if err != nil {
			return err
		}
		state := mset.state()
		if state.Msgs != 2 || state.LastSeq != 2 {
			return fmt.Errorf("incorrect state: %v", state)
		}
		return nil
	})

	// Mirror must keep advancing, not be stuck.
	pubAck, err = js.Publish("foo", nil, nats.MsgId("Y"))
	require_NoError(t, err)
	require_Equal(t, pubAck.Sequence, 3)

	checkFor(t, 2*time.Second, 50*time.Millisecond, func() error {
		mset, err := s.globalAccount().lookupStream("M")
		if err != nil {
			return err
		}
		state := mset.state()
		if state.Msgs != 3 || state.LastSeq != 3 {
			return fmt.Errorf("incorrect state: %v", state)
		}
		return nil
	})

	// After restart, rebuildDedupe repopulates ddmap from stored messages,
	// but the mirror must still ignore those entries for incoming upstream msgs.
	sd := s.JetStreamConfig().StoreDir
	nc.Close()
	s.Shutdown()
	s.WaitForShutdown()

	s = RunJetStreamServerOnPort(-1, sd)
	defer s.Shutdown()

	nc, js = jsClientConnect(t, s)
	defer nc.Close()

	// Wait for upstream's dedup window to expire again, then re-publish "X".
	time.Sleep(200 * time.Millisecond)
	pubAck, err = js.Publish("foo", nil, nats.MsgId("X"))
	require_NoError(t, err)
	require_Equal(t, pubAck.Sequence, 4)
	require_False(t, pubAck.Duplicate)

	checkFor(t, 2*time.Second, 50*time.Millisecond, func() error {
		mset, err := s.globalAccount().lookupStream("M")
		if err != nil {
			return err
		}
		state := mset.state()
		if state.Msgs != 4 || state.LastSeq != 4 {
			return fmt.Errorf("incorrect state: %v", state)
		}
		return nil
	})
}

func TestJetStreamStreamSourceWithoutDuplicateWindow(t *testing.T) {
	s := RunBasicJetStreamServer(t)
	defer s.Shutdown()

	nc, js := jsClientConnect(t, s)
	defer nc.Close()

	_, err := js.AddStream(&nats.StreamConfig{
		Name:       "TEST",
		Subjects:   []string{"foo"},
		Duplicates: time.Second,
	})
	require_NoError(t, err)

	pubAck, err := js.Publish("foo", nil, nats.MsgId("msgId"))
	require_NoError(t, err)
	require_Equal(t, pubAck.Sequence, 1)
	require_False(t, pubAck.Duplicate)

	pubAck, err = js.Publish("foo", nil, nats.MsgId("msgId"))
	require_NoError(t, err)
	require_Equal(t, pubAck.Sequence, 1)
	require_True(t, pubAck.Duplicate)

	time.Sleep(1200 * time.Millisecond)
	pubAck, err = js.Publish("foo", nil, nats.MsgId("msgId"))
	require_NoError(t, err)
	require_Equal(t, pubAck.Sequence, 2)
	require_False(t, pubAck.Duplicate)

	_, err = js.AddStream(&nats.StreamConfig{
		Name:       "S",
		Sources:    []*nats.StreamSource{{Name: "TEST"}},
		Duplicates: 0,
	})
	require_NoError(t, err)

	checkFor(t, 2*time.Second, 100*time.Millisecond, func() error {
		mset, err := s.globalAccount().lookupStream("S")
		if err != nil {
			return err
		}
		state := mset.state()
		if state.Msgs != 2 || state.FirstSeq != 1 || state.LastSeq != 2 {
			return fmt.Errorf("incorrect state: %v", state)
		}
		return nil
	})
}

func TestJetStreamServerEncryptionRecoveryWithoutStreamStateFile(t *testing.T) {
	cases := []struct {
		name   string
		cstr   string
		cipher StoreCipher
	}{
		{"Default", _EMPTY_, ChaCha},
		{"ChaCha", ", cipher: chacha", ChaCha},
		{"AES", ", cipher: aes", AES},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			tmpl := `
				server_name: S22
				listen: 127.0.0.1:-1
				jetstream: {key: $JS_KEY, store_dir: '%s' %s}
			`
			storeDir := t.TempDir()

			conf := createConfFile(t, []byte(fmt.Sprintf(tmpl, storeDir, c.cstr)))

			os.Setenv("JS_KEY", "s3cr3t!!")
			defer os.Unsetenv("JS_KEY")

			s, _ := RunServerWithConfig(conf)
			defer s.Shutdown()

			config := s.JetStreamConfig()
			if config == nil {
				t.Fatalf("Expected config but got none")
			}
			defer removeDir(t, config.StoreDir)

			nc, js := jsClientConnect(t, s)
			defer nc.Close()

			_, err := js.AddStream(&nats.StreamConfig{Name: "TEST", Subjects: []string{"foo"}})
			require_NoError(t, err)

			_, err = js.Publish("foo", nil)
			require_NoError(t, err)

			si, err := js.StreamInfo("TEST")
			require_NoError(t, err)
			before := si.State
			require_Equal(t, before.Msgs, 1)
			require_Equal(t, before.FirstSeq, 1)
			require_Equal(t, before.LastSeq, 1)

			for i := range 2 {
				s.Shutdown()
				s.WaitForShutdown()
				// Previously, the server would rely on this file to be present. If it wasn't, it would
				// not initialize the keys and erroneously regenerate the meta.key upon the next graceful
				// shutdown. A subsequent restart would not allow this stream to be loaded.
				if i == 0 {
					stateFile := filepath.Join(storeDir, JetStreamStoreDir, globalAccountName, streamsDir, "TEST", msgDir, streamStreamStateFile)
					require_NoError(t, os.Remove(stateFile))
				}

				s, _ = RunServerWithConfig(conf)
				defer s.Shutdown()

				// Reconnect.
				nc.Close()
				nc, js = jsClientConnect(t, s)
				defer nc.Close()

				// Previously, the next iteration would fail by not finding the stream.
				si, err = js.StreamInfo("TEST")
				require_NoError(t, err)
				if state := si.State; !reflect.DeepEqual(state, before) {
					t.Fatalf("Expected state\n of %+v, \ngot %+v without index.db state", before, state)
				}
			}
		})
	}
}

func TestJetStreamFileStoreErrorOpeningBlockAfterTruncate(t *testing.T) {
	storeDir := t.TempDir()
	conf := createConfFile(t, []byte(fmt.Sprintf(`
		listen: 127.0.0.1:-1
		jetstream: {store_dir: %q}
	`, storeDir)))

	s, _ := RunServerWithConfig(conf)
	defer s.Shutdown()

	nc, js := jsClientConnect(t, s)
	defer nc.Close()

	_, err := js.AddStream(&nats.StreamConfig{
		Name:     "TEST",
		Subjects: []string{"foo"},
	})
	require_NoError(t, err)

	pubAck, err := js.Publish("foo", nil)
	require_NoError(t, err)
	require_Equal(t, pubAck.Sequence, 1)

	// Shut down the server and manually truncate the message blocks to be entirely empty, simulating data loss.
	mset, err := s.globalAccount().lookupStream("TEST")
	require_NoError(t, err)
	fs := mset.store.(*fileStore)
	blk := filepath.Join(fs.fcfg.StoreDir, msgDir, "1.blk")
	index := filepath.Join(fs.fcfg.StoreDir, msgDir, streamStreamStateFile)
	nc.Close()
	s.Shutdown()

	// Truncate the block such that it isn't fully empty, but doesn't contain any messages.
	require_NoError(t, os.Truncate(blk, 1))
	require_NoError(t, os.Remove(index))

	// Restart the server and reconnect.
	s, _ = RunServerWithConfig(conf)
	defer s.Shutdown()
	nc, js = jsClientConnect(t, s)
	defer nc.Close()

	// Publish another message. Due to the simulated data loss, the stream sequence should continue
	// counting after truncating the corrupted data.
	pubAck, err = js.Publish("foo", nil)
	require_NoError(t, err)
	require_Equal(t, pubAck.Sequence, 1)
}

func TestJetStreamSourceConfigValidation(t *testing.T) {
	s := RunBasicJetStreamServer(t)
	defer s.Shutdown()
	nc := clientConnectToServer(t, s)
	defer nc.Close()

	// Not testing with js.AddStream as passing it a nil source causes it to panic.
	msg := nats.Msg{Subject: "$JS.API.STREAM.CREATE.crash", Data: []byte(`{"name":"crash","retention":"limits","max_consumers":-1,"max_msgs_per_subject":-1,"max_msgs":-1,"max_bytes":-1,"max_age":0,"max_msg_size":-1,"storage":"file","discard":"old","num_replicas":1,"duplicate_window":120000000000,"sources":[null],"sealed":false,"deny_delete":false,"deny_purge":false,"allow_rollup_hdrs":false,"allow_direct":true,"mirror_direct":false,"consumer_limits":{}}`)}
	response, err := nc.Request(msg.Subject, msg.Data, time.Second)
	require_NoError(t, err)

	require_Equal(t, string(response.Data), `{"type":"io.nats.jetstream.api.v1.stream_create_response","error":{"code":400,"err_code":10141,"description":"sourced stream name is invalid"}}`)
}

// https://github.com/nats-io/nats-server/issues/6747
func TestJetStreamCleanupNoInterestAboveThreshold(t *testing.T) {
	s := RunBasicJetStreamServer(t)
	defer s.Shutdown()

	nc, js := jsClientConnect(t, s)
	defer nc.Close()

	_, err := js.AddStream(&nats.StreamConfig{
		Name:      "TEST",
		Subjects:  []string{"a", "b"},
		Retention: nats.InterestPolicy,
	})
	require_NoError(t, err)

	_, err = js.AddConsumer("TEST", &nats.ConsumerConfig{Durable: "A", FilterSubject: "a"})
	require_NoError(t, err)
	_, err = js.AddConsumer("TEST", &nats.ConsumerConfig{Durable: "B", FilterSubject: "b"})
	require_NoError(t, err)

	mset, err := s.globalAccount().lookupStream("TEST")
	require_NoError(t, err)
	for _, subj := range []string{"a", "b"} {
		for range 100_000 {
			_, _, err = mset.store.StoreMsg(subj, nil, nil, 0)
			require_NoError(t, err)
		}
	}

	si, err := js.StreamInfo("TEST")
	require_NoError(t, err)
	require_Equal(t, si.State.Msgs, 200_000)

	// Deleting the consumer will trigger deletion of the messages that now have no interest.
	// Since the total number of messages in the stream is above the threshold, this might take some time.
	require_NoError(t, js.DeleteConsumer("TEST", "A"))
	checkFor(t, 4*time.Second, 200*time.Millisecond, func() error {
		si, err = js.StreamInfo("TEST")
		if err != nil {
			return err
		}
		if si.State.Msgs != 100_000 {
			return fmt.Errorf("expected 100k messages, got %d", si.State.Msgs)
		}
		return nil
	})
}

func TestJetStreamStoreFilterIsAll(t *testing.T) {
	test := func(t *testing.T, storage nats.StorageType) {
		s := RunBasicJetStreamServer(t)
		defer s.Shutdown()

		nc, js := jsClientConnect(t, s)
		defer nc.Close()

		_, err := js.AddStream(&nats.StreamConfig{
			Name:     "TEST",
			Subjects: []string{"c", "b", "a"},
			Storage:  storage,
		})
		require_NoError(t, err)

		mset, err := s.globalAccount().lookupStream("TEST")
		require_NoError(t, err)
		if fs, ok := mset.store.(*fileStore); ok {
			require_True(t, fs.filterIsAll([]string{"b", "a", "c"}))
		} else if ms, ok := mset.store.(*memStore); ok {
			require_True(t, ms.filterIsAll([]string{"b", "a", "c"}))
		} else {
			t.Fatal("unknown store type")
		}
	}
	t.Run("Memory", func(t *testing.T) { test(t, nats.MemoryStorage) })
	t.Run("File", func(t *testing.T) { test(t, nats.FileStorage) })
}

// Test that flow control works correctly when a stream sources from two
// different external accounts. Each account's $JS.FC.> service import
// causes FC replies to fan out to all accounts, but only the correct
// account's consumer handles the reply. This verifies the fan-out
// doesn't prevent FC from completing.
func TestJetStreamFlowControlCrossAccountFanOut(t *testing.T) {
	conf := createConfFile(t, fmt.Appendf(nil, `
		listen: "127.0.0.1:-1"
		jetstream: {store_dir: %q}
		accounts: {
			ACCT_A: {
				jetstream: enabled
				users: [{user: usrA, password: pwd}]
				exports: [
					{service: "$JS.API.CONSUMER.>"}
					{stream: "RI.DELIVER.A.>"}
					{service: "$JS.FC.>"}
				]
			}
			ACCT_B: {
				jetstream: enabled
				users: [{user: usrB, password: pwd}]
				exports: [
					{service: "$JS.API.CONSUMER.>"}
					{stream: "RI.DELIVER.B.>"}
					{service: "$JS.FC.>"}
				]
			}
			SOURCER: {
				jetstream: enabled
				users: [{user: src, password: pwd}]
				imports: [
					{service: {account: ACCT_A, subject: "$JS.API.CONSUMER.>"}, to: "RI.JS.A.API.CONSUMER.>"}
					{stream: {account: ACCT_A, subject: "RI.DELIVER.A.>"}}
					{service: {account: ACCT_A, subject: "$JS.FC.>"}}
					{service: {account: ACCT_B, subject: "$JS.API.CONSUMER.>"}, to: "RI.JS.B.API.CONSUMER.>"}
					{stream: {account: ACCT_B, subject: "RI.DELIVER.B.>"}}
					{service: {account: ACCT_B, subject: "$JS.FC.>"}}
				]
			}
			$SYS {users: [{user: admin, password: pwd}]}
		}
	`, t.TempDir()))
	s, _ := RunServerWithConfig(conf)
	defer s.Shutdown()

	// Create source streams in each account.
	ncA, jsA := jsClientConnect(t, s, nats.UserInfo("usrA", "pwd"))
	defer ncA.Close()
	_, err := jsA.AddStream(&nats.StreamConfig{Name: "TEST", Subjects: []string{"A"}})
	require_NoError(t, err)

	ncB, jsB := jsClientConnect(t, s, nats.UserInfo("usrB", "pwd"))
	defer ncB.Close()
	_, err = jsB.AddStream(&nats.StreamConfig{Name: "TEST", Subjects: []string{"B"}})
	require_NoError(t, err)

	// Publish 10 x 256KB messages to each source. Total 2.56MB exceeds the
	// initial FC threshold (maxpb/2 = 1MB), triggering at least one FC
	// round-trip per source.
	payload := []byte(strings.Repeat("X", 256*1024))
	for i := 0; i < 10; i++ {
		_, err = jsA.Publish("A", payload)
		require_NoError(t, err)
		_, err = jsB.Publish("B", payload)
		require_NoError(t, err)
	}

	// Create sourcing stream that pulls from both accounts.
	ncS, jsS := jsClientConnect(t, s, nats.UserInfo("src", "pwd"))
	defer ncS.Close()
	_, err = jsS.AddStream(&nats.StreamConfig{
		Name: "DEST",
		Sources: []*nats.StreamSource{
			{
				Name: "TEST",
				External: &nats.ExternalStream{
					APIPrefix:     "RI.JS.A.API",
					DeliverPrefix: "RI.DELIVER.A",
				},
			},
			{
				Name: "TEST",
				External: &nats.ExternalStream{
					APIPrefix:     "RI.JS.B.API",
					DeliverPrefix: "RI.DELIVER.B",
				},
			},
		},
	})
	require_NoError(t, err)

	// Wait for all 20 messages to arrive.
	checkFor(t, 10*time.Second, 250*time.Millisecond, func() error {
		si, err := jsS.StreamInfo("DEST")
		if err != nil {
			return err
		}
		if si.State.Msgs != 20 {
			return fmt.Errorf("expected 20 msgs, got %d", si.State.Msgs)
		}
		return nil
	})

	// Verify both sources converged with zero lag.
	si, err := jsS.StreamInfo("DEST")
	require_NoError(t, err)
	require_Equal(t, len(si.Sources), 2)
	for _, src := range si.Sources {
		require_Equal(t, src.Lag, 0)
	}

	// Verify FC was exercised by checking that the internal consumers on
	// each source account ramped up their maxpb beyond the initial value.
	for _, accName := range []string{"ACCT_A", "ACCT_B"} {
		acc, err := s.lookupAccount(accName)
		require_NoError(t, err)
		mset, err := acc.lookupStream("TEST")
		require_NoError(t, err)
		consumers := mset.getConsumers()
		require_Equal(t, len(consumers), 1)
		o := consumers[0]
		o.mu.RLock()
		maxpb := o.maxpb
		pblimit := o.pblimit
		o.mu.RUnlock()
		if maxpb <= pblimit/16 {
			t.Fatalf("account %s: expected FC ramp-up (maxpb=%d > initial=%d)", accName, maxpb, pblimit/16)
		}
	}
}

func TestJetStreamStreamCheckSourcesWithExternal(t *testing.T) {
	s := RunBasicJetStreamServer(t)
	defer s.Shutdown()

	nc, js := jsClientConnect(t, s)
	defer nc.Close()

	_, err := js.AddStream(&nats.StreamConfig{Name: "ORIGIN"})
	require_NoError(t, err)

	// Specifying both fields errors.
	src := &nats.StreamSource{
		Name:          "ORIGIN",
		FilterSubject: "filter",
		SubjectTransforms: []nats.SubjectTransformConfig{
			{Source: "filter"},
		},
	}
	cfg := &nats.StreamConfig{Name: "SOURCE", Sources: []*nats.StreamSource{src}}
	for _, external := range []*nats.ExternalStream{nil, {}} {
		src.External = external
		_, err = js.AddStream(cfg)
		require_Error(t, err, NewJSSourceMultipleFiltersNotAllowedError())
	}

	// Invalid source subject errors.
	src = &nats.StreamSource{
		Name: "ORIGIN",
		SubjectTransforms: []nats.SubjectTransformConfig{
			{Source: ".invalid"},
		},
	}
	cfg = &nats.StreamConfig{Name: "SOURCE", Sources: []*nats.StreamSource{src}}
	for _, external := range []*nats.ExternalStream{nil, {}} {
		src.External = external
		_, err = js.AddStream(cfg)
		require_Error(t, err, NewJSSourceInvalidSubjectFilterError(fmt.Errorf("%w %s", ErrBadSubject, ".invalid")))
	}

	// Invalid destination subject errors.
	src = &nats.StreamSource{
		Name: "ORIGIN",
		SubjectTransforms: []nats.SubjectTransformConfig{
			{Source: "src", Destination: ".invalid"},
		},
	}
	cfg = &nats.StreamConfig{Name: "SOURCE", Sources: []*nats.StreamSource{src}}
	for _, external := range []*nats.ExternalStream{nil, {}} {
		src.External = external
		_, err = js.AddStream(cfg)
		require_Error(t, err, NewJSSourceInvalidTransformDestinationError(ErrInvalidMappingDestinationSubject))
	}

	// Overlap errors.
	src = &nats.StreamSource{
		Name: "ORIGIN",
		SubjectTransforms: []nats.SubjectTransformConfig{
			{Source: "src"},
			{Source: "src"},
		},
	}
	cfg = &nats.StreamConfig{Name: "SOURCE", Sources: []*nats.StreamSource{src}}
	for _, external := range []*nats.ExternalStream{nil, {}} {
		src.External = external
		_, err = js.AddStream(cfg)
		require_Error(t, err, NewJSSourceOverlappingSubjectFiltersError())
	}
}

func TestJetStreamMirrorProcessMsgsNilQuitChannel(t *testing.T) {
	s := RunBasicJetStreamServer(t)
	defer s.Shutdown()

	nc, js := jsClientConnect(t, s)
	defer nc.Close()

	_, err := js.AddStream(&nats.StreamConfig{
		Name:     "TEST",
		Subjects: []string{"foo"},
	})
	require_NoError(t, err)

	_, err = js.AddStream(&nats.StreamConfig{
		Name:   "MIRROR",
		Mirror: &nats.StreamSource{Name: "TEST"},
	})
	require_NoError(t, err)

	// Verify the mirror works.
	_, err = js.Publish("foo", []byte("hello"))
	require_NoError(t, err)
	checkFor(t, 2*time.Second, 100*time.Millisecond, func() error {
		si, err := js.StreamInfo("MIRROR")
		if err != nil {
			return err
		}
		if si.State.Msgs != 1 {
			return fmt.Errorf("expected 1 msg, got %d", si.State.Msgs)
		}
		return nil
	})

	mset, err := s.globalAccount().lookupStream("MIRROR")
	require_NoError(t, err)

	// Cancel the current mirror consumer and wait for its processMirrorMsgs
	// goroutine to fully exit.
	mset.mu.Lock()
	mset.cancelMirrorConsumer()
	mirror := mset.mirror
	mset.mu.Unlock()
	mirror.wg.Wait()

	// Now reproduce the race condition: start processMirrorMsgs while
	// mirror.qch is nil. This simulates what happens when cancelSourceInfo
	// runs between the goroutine being launched and acquiring mset.mu.
	//
	// We hold mset.mu while starting the goroutine so that it blocks on
	// mset.mu.Lock() inside processMirrorMsgs. When we release the lock,
	// it captures siqch = mirror.qch which is nil (left nil by cancelSourceInfo).
	mset.mu.Lock()
	mirror = mset.mirror
	mirror.msgs = newIPQueue[*inMsg](s, "stream mirror")
	mirror.wg.Add(1)
	ready := sync.WaitGroup{}
	ready.Add(1)
	if !s.startGoRoutine(func() { mset.processMirrorMsgs(mirror, &ready) }) {
		mirror.wg.Done()
		ready.Done()
		mset.mu.Unlock()
		t.Fatal("Failed to start goroutine")
	}
	mset.mu.Unlock()
	ready.Wait()

	// Verify the goroutine exits promptly. With the bug, mirror.wg.Wait()
	// blocks forever because <-siqch on a nil channel never fires.
	done := make(chan struct{})
	go func() {
		mirror.wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		// Expected goroutine to exit properly after cancel.
	case <-time.After(2 * time.Second):
		t.Fatal("Mirror goroutine did not exit after cancel: stuck on nil quit channel")
	}
}

func TestJetStreamMirrorSetupStartGoRoutineFailMissingWgDone(t *testing.T) {
	s := RunBasicJetStreamServer(t)
	defer s.Shutdown()

	nc, js := jsClientConnect(t, s)
	defer nc.Close()

	_, err := js.AddStream(&nats.StreamConfig{
		Name:     "TEST",
		Subjects: []string{"foo"},
	})
	require_NoError(t, err)

	_, err = js.AddStream(&nats.StreamConfig{
		Name:   "MIRROR",
		Mirror: &nats.StreamSource{Name: "TEST"},
	})
	require_NoError(t, err)

	// Verify the mirror works.
	_, err = js.Publish("foo", []byte("hello"))
	require_NoError(t, err)
	checkFor(t, 2*time.Second, 100*time.Millisecond, func() error {
		si, err := js.StreamInfo("MIRROR")
		if err != nil {
			return err
		}
		if si.State.Msgs != 1 {
			return fmt.Errorf("expected 1 msg, got %d", si.State.Msgs)
		}
		return nil
	})

	mset, err := s.globalAccount().lookupStream("MIRROR")
	require_NoError(t, err)

	// Cancel the current mirror consumer and wait for its goroutine to exit.
	mset.mu.Lock()
	mset.cancelMirrorConsumer()
	mirror := mset.mirror
	mset.mu.Unlock()
	mirror.wg.Wait()

	// Simulate shutting down and not able to schedule more goroutines.
	s.grMu.Lock()
	s.grRunning = false
	s.grMu.Unlock()

	// Set up the mirror consumer which should not allow a new goroutine to be scheduled.
	mset.mu.Lock()
	mirror.lreq = time.Time{}
	err = mset.setupMirrorConsumer()
	mset.mu.Unlock()
	require_NoError(t, err)

	// Give the above setup enough time to start the mirror goroutine.
	time.Sleep(500 * time.Millisecond)

	done := make(chan struct{})
	go func() {
		mirror.wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		// Expected goroutine to exit properly.
	case <-time.After(2 * time.Second):
		// wg is stuck because Done() was never called.
		// Clean up the orphaned wg to prevent leaking the waiter goroutine.
		mirror.wg.Done()
		t.Fatal("mirror.wg.Wait() blocked indefinitely: missing wg.Done() in startGoRoutine failure path")
	}
}

func TestJetStreamSourcingIntoDiscardNewPerSubject(t *testing.T) {
	s := RunBasicJetStreamServer(t)
	defer s.Shutdown()

	nc, js := jsClientConnect(t, s)
	defer nc.Close()

	_, err := js.AddStream(&nats.StreamConfig{
		Name:       "A",
		Subjects:   []string{"foo.*"},
		Duplicates: 100 * time.Millisecond,
	})
	require_NoError(t, err)

	sConfig := StreamConfig{
		Name:          "B",
		Storage:       MemoryStorage,
		Sources:       []*StreamSource{{Name: "A"}},
		MaxMsgsPer:    1,
		Discard:       DiscardNew,
		DiscardNewPer: true,
		Duplicates:    10 * time.Second,
	}

	req, err := json.Marshal(&sConfig)
	require_NoError(t, err)

	r, err := nc.Request("$JS.API.STREAM.CREATE.B", req, 5*time.Second)
	require_NoError(t, err)

	var resp JSApiConsumerCreateResponse
	err = json.Unmarshal(r.Data, &resp)
	require_NoError(t, err)
	require_Equal(t, resp.Error, nil)

	_, err = js.Publish("foo.1", ([]byte)("1"))
	require_NoError(t, err)

	// this will not be sourced as discard new per subject
	_, err = js.Publish("foo.1", ([]byte)("2"))
	require_NoError(t, err)

	var si *nats.StreamInfo

	checkFor(t, 4*time.Second, 200*time.Millisecond, func() error {
		si, err = js.StreamInfo("B")
		if err != nil {
			return err
		}
		if si.State.Msgs != 1 {
			return fmt.Errorf("expected 1 messages, got %d", si.State.Msgs)
		}
		return nil
	})

	// Check the message
	msgp, err := js.GetMsg("B", uint64(1))
	require_NoError(t, err)
	require_Equal(t, msgp.Subject, "foo.1")
	require_Equal(t, string(msgp.Data), "1")

	// now purge the stream so sourcing can continue
	require_NoError(t, js.PurgeStream("B"))

	checkFor(t, 4*time.Second, 200*time.Millisecond, func() error {
		si, err = js.StreamInfo("B")
		if err != nil {
			return err
		}
		if si.State.Msgs != 1 {
			return fmt.Errorf("expected 1 messages, got %d", si.State.Msgs)
		}
		return nil
	})

	// check the message
	msgp, err = js.GetMsg("B", uint64(2))
	require_NoError(t, err)
	require_Equal(t, msgp.Subject, "foo.1")
	require_Equal(t, string(msgp.Data), "2")

	msg := nats.NewMsg("foo.2")
	msg.Data = []byte("1")
	msg.Header.Set("Nats-Msg-Id", "1")

	_, err = js.PublishMsg(msg)
	require_NoError(t, err)

	// Must be able to move on and source the next message after the duplicate
	checkFor(t, 4*time.Second, 200*time.Millisecond, func() error {
		si, err = js.StreamInfo("B")
		if err != nil {
			return err
		}
		if si.State.Msgs != 2 {
			return fmt.Errorf("expected 2 messages, got %d", si.State.Msgs)
		}
		return nil
	})

	// check the message
	msgp, err = js.GetMsg("B", uint64(3))
	require_NoError(t, err)
	require_Equal(t, msgp.Subject, "foo.2")
	require_Equal(t, string(msgp.Data), "1")

	time.Sleep(200 * time.Millisecond)

	msg = nats.NewMsg("foo.3")
	msg.Data = []byte("1")
	msg.Header.Set("Nats-Msg-Id", "1")

	_, err = js.PublishMsg(msg)
	require_NoError(t, err)

	// Duplicate message id, should get skipped
	checkFor(t, 4*time.Second, 200*time.Millisecond, func() error {
		si, err = js.StreamInfo("B")
		if err != nil {
			return err
		}
		if si.State.Msgs != 2 {
			return fmt.Errorf("expected 2 messages, got %d", si.State.Msgs)
		}
		return nil
	})

	// Must be able to move on
	_, err = js.Publish("foo.4", ([]byte)("1"))
	require_NoError(t, err)

	// Must be able to move on and source the next message after the duplicate
	checkFor(t, 4*time.Second, 200*time.Millisecond, func() error {
		si, err = js.StreamInfo("B")
		if err != nil {
			return err
		}
		if si.State.Msgs != 3 {
			return fmt.Errorf("expected 3 messages, got %d", si.State.Msgs)
		}
		return nil
	})

	msgp, err = js.GetMsg("B", uint64(4))
	require_NoError(t, err)
	require_Equal(t, msgp.Subject, "foo.4")
	require_Equal(t, string(msgp.Data), "1")

}

func TestJetStreamDurableStreamMirrorAndSourceIncorrectConsumerConfig(t *testing.T) {
	s := RunBasicJetStreamServer(t)
	defer s.Shutdown()

	nc, js := jsClientConnect(t, s)
	defer nc.Close()

	_, err := jsStreamCreate(t, nc, &StreamConfig{
		Name:      "O",
		Subjects:  []string{"foo"},
		Storage:   FileStorage,
		Retention: WorkQueuePolicy,
	})
	require_NoError(t, err)

	_, err = jsConsumerCreate(t, nc, "O", ConsumerConfig{
		Durable:        "C",
		DeliverSubject: "deliver-subject",
		AckPolicy:      AckExplicit,
	}, false)
	require_NoError(t, err)

	checkMirror := func(expected string) {
		t.Helper()
		checkFor(t, 2*time.Second, 200*time.Millisecond, func() error {
			si, err := js.StreamInfo("M")
			if err != nil {
				return err
			}
			if si.Mirror == nil {
				return errors.New("no mirror")
			}
			if si.Mirror.Error == nil {
				return errors.New("expected mirror error")
			}
			if si.Mirror.Error.Description != expected {
				return si.Mirror.Error
			}
			return nil
		})
	}

	// Mirror.
	cfg := &StreamConfig{
		Name:    "M",
		Mirror:  &StreamSource{Name: "O", Consumer: &StreamConsumerSource{Name: "", DeliverSubject: "deliver-subject"}},
		Storage: FileStorage,
	}
	_, err = jsStreamCreate(t, nc, cfg)
	require_Error(t, err, NewJSMirrorDurableConsumerCfgInvalidError())

	cfg.Mirror = &StreamSource{Name: "O", Consumer: &StreamConsumerSource{Name: "C", DeliverSubject: ""}}
	_, err = jsStreamCreate(t, nc, cfg)
	require_Error(t, err, NewJSMirrorDurableConsumerCfgInvalidError())

	// Reject invalid consumer name.
	cfg.Mirror = &StreamSource{Name: "O", Consumer: &StreamConsumerSource{Name: "C.b", DeliverSubject: "deliver-subject"}}
	_, err = jsStreamCreate(t, nc, cfg)
	require_Error(t, err, NewJSMirrorDurableConsumerCfgInvalidError())

	// Reject invalid deliver subject.
	cfg.Mirror = &StreamSource{Name: "O", Consumer: &StreamConsumerSource{Name: "C", DeliverSubject: "deliver.."}}
	_, err = jsStreamCreate(t, nc, cfg)
	require_Error(t, err, NewJSMirrorDurableConsumerCfgInvalidError())

	cfg.Mirror = &StreamSource{Name: "O", Consumer: &StreamConsumerSource{Name: "C", DeliverSubject: "deliver-subject"}}

	// Setting start seq shouldn't be allowed.
	cfg.Mirror.OptStartSeq = 1
	_, err = jsStreamCreate(t, nc, cfg)
	require_Error(t, err, NewJSMirrorDurableConsumerCfgInvalidError())

	// Setting start time shouldn't be allowed.
	now := time.Now()
	cfg.Mirror.OptStartSeq = 0
	cfg.Mirror.OptStartTime = &now
	_, err = jsStreamCreate(t, nc, cfg)
	require_Error(t, err, NewJSMirrorDurableConsumerCfgInvalidError())

	// Setting filter subject shouldn't be allowed.
	cfg.Mirror.OptStartTime = nil
	cfg.Mirror.FilterSubject = "filter"
	_, err = jsStreamCreate(t, nc, cfg)
	require_Error(t, err, NewJSMirrorDurableConsumerCfgInvalidError())

	cfg.Mirror = &StreamSource{Name: "O", Consumer: &StreamConsumerSource{Name: "C", DeliverSubject: "deliver-subject"}}
	_, err = jsStreamCreate(t, nc, cfg)
	require_NoError(t, err)
	checkMirror(NewJSMirrorConsumerRequiresAckFCError().Description)

	// Check mirror config is not updatable.
	cfg.Mirror.SubjectTransforms = []SubjectTransformConfig{{Source: ">", Destination: "a.>"}}
	_, err = jsStreamUpdate(t, nc, cfg)
	require_Error(t, err, NewJSStreamMirrorNotUpdatableError())

	checkSource := func(expected string) {
		t.Helper()
		checkFor(t, 2*time.Second, 200*time.Millisecond, func() error {
			si, err := js.StreamInfo("S")
			if err != nil {
				return err
			}
			if len(si.Sources) != 1 {
				return errors.New("no source")
			}
			sourceErr := si.Sources[0].Error
			if sourceErr == nil {
				return errors.New("expected source error")
			}
			if sourceErr.Description != expected {
				return sourceErr
			}
			return nil
		})
	}

	// Source.
	cfg = &StreamConfig{
		Name:    "S",
		Sources: []*StreamSource{{Name: "O", Consumer: &StreamConsumerSource{Name: "", DeliverSubject: "deliver-subject"}}},
		Storage: FileStorage,
	}
	_, err = jsStreamCreate(t, nc, cfg)
	require_Error(t, err, NewJSSourceDurableConsumerCfgInvalidError())

	cfg.Sources = []*StreamSource{{Name: "O", Consumer: &StreamConsumerSource{Name: "C", DeliverSubject: ""}}}
	_, err = jsStreamCreate(t, nc, cfg)
	require_Error(t, err, NewJSSourceDurableConsumerCfgInvalidError())

	// Reject invalid consumer name.
	cfg.Sources = []*StreamSource{{Name: "O", Consumer: &StreamConsumerSource{Name: "C.b", DeliverSubject: "deliver-subject"}}}
	_, err = jsStreamCreate(t, nc, cfg)
	require_Error(t, err, NewJSSourceDurableConsumerCfgInvalidError())

	// Reject invalid deliver subject.
	cfg.Sources = []*StreamSource{{Name: "O", Consumer: &StreamConsumerSource{Name: "C", DeliverSubject: "deliver.."}}}
	_, err = jsStreamCreate(t, nc, cfg)
	require_Error(t, err, NewJSSourceDurableConsumerCfgInvalidError())

	cfg.Sources = []*StreamSource{
		{Name: "O", SubjectTransforms: []SubjectTransformConfig{{Source: ">", Destination: "a.>"}}, Consumer: &StreamConsumerSource{Name: "C", DeliverSubject: "a"}},
		{Name: "O", SubjectTransforms: []SubjectTransformConfig{{Source: ">", Destination: "b.>"}}, Consumer: &StreamConsumerSource{Name: "C", DeliverSubject: "b"}},
	}
	_, err = jsStreamCreate(t, nc, cfg)
	require_Error(t, err, NewJSSourceDurableConsumerDuplicateDetectedError())

	cfg.Sources = []*StreamSource{{Name: "O", Consumer: &StreamConsumerSource{Name: "C", DeliverSubject: "deliver-subject"}}}
	_, err = jsStreamCreate(t, nc, cfg)
	require_NoError(t, err)
	checkSource(NewJSSourceConsumerRequiresAckFCError().Description)
	require_NoError(t, js.DeleteStream("S"))

	// Setting start seq shouldn't be allowed.
	cfg.Sources[0].OptStartSeq = 1
	_, err = jsStreamCreate(t, nc, cfg)
	require_Error(t, err, NewJSSourceDurableConsumerCfgInvalidError())

	// Setting start time shouldn't be allowed.
	cfg.Sources[0].OptStartSeq = 0
	cfg.Sources[0].OptStartTime = &now
	_, err = jsStreamCreate(t, nc, cfg)
	require_Error(t, err, NewJSSourceDurableConsumerCfgInvalidError())

	// Setting filter subject shouldn't be allowed.
	cfg.Sources[0].OptStartTime = nil
	cfg.Sources[0].FilterSubject = "filter"
	_, err = jsStreamCreate(t, nc, cfg)
	require_Error(t, err, NewJSSourceDurableConsumerCfgInvalidError())
}

func TestJetStreamDurableStreamSourcesWithUniqueConsumerNames(t *testing.T) {
	s := RunBasicJetStreamServer(t)
	defer s.Shutdown()

	nc, js := jsClientConnect(t, s)
	defer nc.Close()

	_, err := js.AddStream(&nats.StreamConfig{
		Name:      "O",
		Subjects:  []string{"a", "b"},
		Retention: nats.WorkQueuePolicy,
	})
	require_NoError(t, err)

	_, err = js.AddStream(&nats.StreamConfig{
		Name: "S",
		Sources: []*nats.StreamSource{
			{Name: "O", FilterSubject: "a"},
			{Name: "O", FilterSubject: "b"},
		},
	})
	require_NoError(t, err)

	// Expect two separate sourcing consumers to be created for the above sourcing stream.
	mset, err := s.globalAccount().lookupStream("O")
	require_NoError(t, err)
	checkFor(t, 2*time.Second, 200*time.Millisecond, func() error {
		if c := mset.numConsumers(); c != 2 {
			return fmt.Errorf("expected 2 consumers, got %d", c)
		}
		return nil
	})
}

func TestJetStreamSourcesState(t *testing.T) {
	test := func(t *testing.T, storage nats.StorageType) {
		s := RunBasicJetStreamServer(t)
		defer s.Shutdown()
		port := s.getOpts().Port
		sd := s.JetStreamConfig().StoreDir

		nc, js := jsClientConnect(t, s)
		defer nc.Close()

		_, err := js.AddStream(&nats.StreamConfig{
			Name:     "ORIGIN",
			Storage:  storage,
			Subjects: []string{"foo"},
		})
		require_NoError(t, err)

		cfg := &nats.StreamConfig{
			Name:    "SOURCE",
			Storage: storage,
			Sources: []*nats.StreamSource{{Name: "ORIGIN"}},
		}
		_, err = js.AddStream(cfg)
		require_NoError(t, err)

		mset, err := s.globalAccount().lookupStream("SOURCE")
		require_NoError(t, err)
		store := mset.Store()
		state := store.SourcesState()
		require_NotNil(t, state)
		require_Equal(t, state["ORIGIN > >"].Seq, 0)

		_, err = js.Publish("foo", nil)
		require_NoError(t, err)
		checkFor(t, 2*time.Second, 200*time.Millisecond, func() error {
			if msgs := store.State().Msgs; msgs != 1 {
				return fmt.Errorf("expected 1 message, got %d", msgs)
			}
			return nil
		})
		state = store.SourcesState()
		require_NotNil(t, state)
		require_Equal(t, state["ORIGIN > >"].Seq, 1)

		if storage == nats.FileStorage {
			s.Shutdown()
			s = RunJetStreamServerOnPort(port, sd)
			defer s.Shutdown()

			nc.Close()
			nc, js = jsClientConnect(t, s)
			defer nc.Close()

			mset, err = s.globalAccount().lookupStream("SOURCE")
			require_NoError(t, err)
			store = mset.Store()
			state = store.SourcesState()
			require_NotNil(t, state)
			require_Equal(t, state["ORIGIN > >"].Seq, 1)
		}

		cfg.Sources = nil
		_, err = js.UpdateStream(cfg)
		require_NoError(t, err)
		state = store.SourcesState()
		require_True(t, state == nil)
	}

	t.Run("File", func(t *testing.T) { test(t, nats.FileStorage) })
	t.Run("Memory", func(t *testing.T) { test(t, nats.MemoryStorage) })
}

func TestJetStreamSourcesStateStartingSequence(t *testing.T) {
	test := func(t *testing.T, storage nats.StorageType, purge bool) {
		s := RunBasicJetStreamServer(t)
		defer s.Shutdown()

		nc, js := jsClientConnect(t, s)
		defer nc.Close()

		_, err := js.AddStream(&nats.StreamConfig{
			Name:     "ORIGIN",
			Storage:  storage,
			Subjects: []string{"foo"},
		})
		require_NoError(t, err)

		_, err = js.AddStream(&nats.StreamConfig{
			Name:    "SOURCE",
			Storage: storage,
			Sources: []*nats.StreamSource{{Name: "ORIGIN"}},
		})
		require_NoError(t, err)

		const total = 5
		for range total {
			_, err = js.Publish("foo", nil)
			require_NoError(t, err)
		}

		mset, err := s.globalAccount().lookupStream("SOURCE")
		require_NoError(t, err)
		store := mset.Store()

		checkFor(t, 2*time.Second, 50*time.Millisecond, func() error {
			if msgs := store.State().Msgs; msgs != total {
				return fmt.Errorf("expected %d messages, got %d", total, msgs)
			}
			return nil
		})

		origin, err := s.globalAccount().lookupStream("ORIGIN")
		require_NoError(t, err)
		ident := origin.identity()
		require_NotEqual(t, ident, _EMPTY_)

		const iname = "ORIGIN > >"
		// The source's identity must be tracked alongside its sequence.
		require_Equal(t, store.SourcesState()[iname].Seq, total)
		require_Equal(t, store.SourcesState()[iname].Ident, ident)

		if purge {
			// Purge all destination messages. The tracked source state must survive
			// this so we can resume rather than start over.
			_, err = mset.purge(nil)
			require_NoError(t, err)
			require_Equal(t, store.State().Msgs, 0)
			require_Equal(t, store.SourcesState()[iname].Seq, total)
			require_Equal(t, store.SourcesState()[iname].Ident, ident)
		}

		// Both sequence and identity must survive the recovery path that runs on
		// restart and on leader election. Without the identity a recreated origin
		// goes undetected, and we would resume from a sequence that means nothing
		// in the new stream.
		mset.mu.Lock()
		mset.resetSourceInfo()
		si := mset.sources[iname]
		mset.mu.Unlock()

		require_NotNil(t, si)
		require_Equal(t, si.sseq, total)
		require_Equal(t, si.ident, ident)
	}

	t.Run("File", func(t *testing.T) {
		t.Run("Fresh", func(t *testing.T) { test(t, nats.FileStorage, false) })
		t.Run("AfterPurge", func(t *testing.T) { test(t, nats.FileStorage, true) })
	})
	t.Run("Memory", func(t *testing.T) {
		t.Run("Fresh", func(t *testing.T) { test(t, nats.MemoryStorage, false) })
		t.Run("AfterPurge", func(t *testing.T) { test(t, nats.MemoryStorage, true) })
	})
}

func TestJetStreamSourcesStateAddedSourceUsesAdoptedState(t *testing.T) {
	test := func(t *testing.T, storage nats.StorageType) {
		s := RunBasicJetStreamServer(t)
		defer s.Shutdown()

		nc, js := jsClientConnect(t, s)
		defer nc.Close()

		cfg := &nats.StreamConfig{
			Name:    "SOURCE",
			Storage: storage,
			Sources: []*nats.StreamSource{{Name: "ORIGIN1"}},
		}
		_, err := js.AddStream(cfg)
		require_NoError(t, err)

		mset, err := s.globalAccount().lookupStream("SOURCE")
		require_NoError(t, err)

		// Adopt a leader's sourcing state that already covers ORIGIN2, which this
		// stream's own config doesn't have yet. That is what a replica is left
		// holding when it installs a snapshot before the meta layer adds the
		// source, and the store has no messages to derive the position from.
		mset.store.ApplySourcesState(map[string]StreamSourceState{
			"ORIGIN2 > >": {Seq: 7, Ident: "IDENT2"},
		})

		// Adding the source has to reuse that state rather than start over.
		cfg.Sources = append(cfg.Sources, &nats.StreamSource{Name: "ORIGIN2"})
		_, err = js.UpdateStream(cfg)
		require_NoError(t, err)

		mset.mu.RLock()
		si := mset.sources["ORIGIN2 > >"]
		mset.mu.RUnlock()

		require_NotNil(t, si)
		require_Equal(t, si.sseq, 7)
		require_Equal(t, si.ident, "IDENT2")
	}

	t.Run("File", func(t *testing.T) { test(t, nats.FileStorage) })
	t.Run("Memory", func(t *testing.T) { test(t, nats.MemoryStorage) })
}

func TestJetStreamClusterInfoDoesNotBlockJSMutex(t *testing.T) {
	// Use a real raft node with its RWMutex held to simulate a Raft
	// runloop blocked during vote processing, e.g. by dios. clusterInfo
	// calls GroupLeader which needs the raft RLock, so it will block.
	n := &raft{}
	n.Lock()
	defer n.Unlock()

	js := &jetStream{srv: &Server{}}
	rg := &raftGroup{
		Name:  "test",
		Peers: []string{"a", "b"},
		node:  n,
	}

	// clusterInfo will block inside n.GroupLeader waiting for Raft RLock.
	go js.clusterInfo(rg)

	// Give it a moment to enter GroupLeader.
	time.Sleep(50 * time.Millisecond)

	// If clusterInfo holds js.mu.RLock during Raft calls,
	// this Lock will block and the test will time out.
	done := make(chan struct{})
	go func() {
		js.mu.Lock()
		runtime.Gosched()
		js.mu.Unlock()
		close(done)
	}()

	// If contending then this will error.
	require_ChanRead(t, done, time.Second)
}

// https://github.com/nats-io/nats-server/issues/8114
func TestJetStreamStreamConfigConcurrentReadWrite(t *testing.T) {
	s := RunBasicJetStreamServer(t)
	defer s.Shutdown()

	nc, js := jsClientConnect(t, s)
	defer nc.Close()

	_, err := js.AddStream(&nats.StreamConfig{
		Name:     "TEST",
		Subjects: []string{"foo"},
		MaxBytes: 1 << 10,
	})
	require_NoError(t, err)

	stop := make(chan struct{})
	var wg sync.WaitGroup

	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; ; i++ {
			select {
			case <-stop:
				return
			default:
			}
			_, err := js.UpdateStream(&nats.StreamConfig{
				Name:     "TEST",
				Subjects: []string{"foo"},
				MaxBytes: int64(1<<10) + int64(i%256),
			})
			require_NoError(t, err)
		}
	}()

	readers := []func(){
		// Hits jsa.reservedStorage, countStreams (indirectly via stats), tieredReservation.
		func() { _, _ = js.AccountInfo() },
		// Hits jsStreamNamesRequest.
		func() {
			for range js.StreamNames() {
			}
		},
		// Hits jsStreamListRequest.
		func() {
			for range js.StreamsInfo() {
			}
		},
		// Hits jsMsgDeleteRequest -> Sealed / DenyDelete read.
		func() { _ = js.DeleteMsg("TEST", 1) },
		// Hits jsStreamPurgeRequest -> Sealed / DenyPurge read.
		func() { _ = js.PurgeStream("TEST") },
	}
	for _, r := range readers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
				}
				r()
			}
		}()
	}

	// Run long enough for the race detector to observe interleaving.
	time.Sleep(500 * time.Millisecond)
	close(stop)
	wg.Wait()
}

func TestJetStreamMirrorProcessInboundNilDeref(t *testing.T) {
	s := RunBasicJetStreamServer(t)
	defer s.Shutdown()

	nc, js := jsClientConnect(t, s)
	defer nc.Close()

	_, err := js.AddStream(&nats.StreamConfig{
		Name:     "M",
		Subjects: []string{"foo"},
	})
	require_NoError(t, err)

	for range 5 {
		_, err := js.Publish("foo", []byte("seed"))
		require_NoError(t, err)
	}

	mset, err := s.globalAccount().lookupStream("M")
	require_NoError(t, err)

	var (
		stop atomic.Bool
		wg   sync.WaitGroup
	)

	wg.Add(1)
	go func() {
		defer wg.Done()
		for !stop.Load() {
			mset.mu.Lock()
			mset.mirror = &sourceInfo{name: "X", cname: "X", sseq: 4}
			mset.mu.Unlock()

			// With mset.mirror{cname:"X", sseq:4}, sseq==5 takes the
			// sequential branch and then forces errLastSeqMismatch from
			// processJetStreamMsg with sseq<=lseq.
			rply := fmt.Sprintf("$JS.ACK.M.X.1.5.1.%d.0", time.Now().UnixNano())
			mset.processInboundMirrorMsg(&inMsg{subj: "bar", rply: rply, msg: []byte("x")})
		}
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		for !stop.Load() {
			mset.mu.Lock()
			mset.mirror = nil
			mset.mu.Unlock()
			runtime.Gosched()
		}
	}()

	time.Sleep(3 * time.Second)
	stop.Store(true)
	wg.Wait()
}

func TestJetStreamMirrorConsumerRetryUsesFreshReplySubject(t *testing.T) {
	s := RunBasicJetStreamServer(t)
	defer s.Shutdown()

	nc, js := jsClientConnect(t, s)
	defer nc.Close()

	// Route the mirror's consumer create requests to our own fake API, so we fully control
	// the request/response loop instead of racing the server's real JetStream API handler.
	const apiPrefix = "$FAKE.API"
	reqCh := make(chan *nats.Msg, 4)
	_, err := nc.Subscribe(apiPrefix+".CONSUMER.CREATE.>", func(m *nats.Msg) {
		reqCh <- m
	})
	require_NoError(t, err)
	require_NoError(t, nc.Flush())

	_, err = js.AddStream(&nats.StreamConfig{
		Name: "MIRROR",
		Mirror: &nats.StreamSource{
			Name:     "ORIGIN",
			External: &nats.ExternalStream{APIPrefix: apiPrefix},
		},
	})
	require_NoError(t, err)

	mset, err := s.globalAccount().lookupStream("MIRROR")
	require_NoError(t, err)

	// Original request.
	m1 := require_ChanRead(t, reqCh, time.Second)
	var ccr1 CreateConsumerRequest
	require_NoError(t, json.Unmarshal(m1.Data, &ccr1))
	require_True(t, ccr1.Config.Sourcing)
	reply1 := m1.Reply

	// Answer with a required API level error.
	errResp, err := json.Marshal(JSApiConsumerCreateResponse{
		ApiResponse: ApiResponse{Error: NewJSRequiredApiLevelError()},
	})
	require_NoError(t, err)
	require_NoError(t, nc.Publish(reply1, errResp))
	require_NoError(t, nc.Flush())

	// Retry request.
	m2 := require_ChanRead(t, reqCh, time.Second)
	var ccr2 CreateConsumerRequest
	require_NoError(t, json.Unmarshal(m2.Data, &ccr2))
	require_False(t, ccr2.Config.Sourcing)
	reply2 := m2.Reply
	require_NotEqual(t, reply1, reply2)

	staleResp, err := json.Marshal(JSApiConsumerCreateResponse{
		ApiResponse: ApiResponse{Error: NewJSStreamNotFoundError()},
	})
	require_NoError(t, err)
	require_NoError(t, nc.Publish(reply1, staleResp))

	okResp, err := json.Marshal(JSApiConsumerCreateResponse{
		ConsumerInfo: &ConsumerInfo{Name: ccr2.Config.Name, Config: &ccr2.Config},
	})
	require_NoError(t, err)
	require_NoError(t, nc.Publish(reply2, okResp))
	require_NoError(t, nc.Flush())

	checkFor(t, 2*time.Second, 100*time.Millisecond, func() error {
		mset.mu.RLock()
		defer mset.mu.RUnlock()
		if mset.mirror == nil {
			return fmt.Errorf("mirror was removed")
		}
		if mset.mirror.err != nil {
			return fmt.Errorf("mirror is in error state: %v", mset.mirror.err)
		}
		if mset.mirror.cname != ccr2.Config.Name {
			return fmt.Errorf("expected consumer name %q, got %q", ccr2.Config.Name, mset.mirror.cname)
		}
		return nil
	})
}

func TestJetStreamSourceConsumerRetryUsesFreshReplySubject(t *testing.T) {
	s := RunBasicJetStreamServer(t)
	defer s.Shutdown()

	nc, js := jsClientConnect(t, s)
	defer nc.Close()

	// Route the mirror's consumer create requests to our own fake API, so we fully control
	// the request/response loop instead of racing the server's real JetStream API handler.
	const apiPrefix = "$FAKE.API"
	reqCh := make(chan *nats.Msg, 4)
	_, err := nc.Subscribe(apiPrefix+".CONSUMER.CREATE.>", func(m *nats.Msg) {
		reqCh <- m
	})
	require_NoError(t, err)
	require_NoError(t, nc.Flush())

	_, err = js.AddStream(&nats.StreamConfig{
		Name: "SOURCE",
		Sources: []*nats.StreamSource{
			{
				Name:     "ORIGIN",
				External: &nats.ExternalStream{APIPrefix: apiPrefix},
			},
		},
	})
	require_NoError(t, err)

	mset, err := s.globalAccount().lookupStream("SOURCE")
	require_NoError(t, err)

	errResp, err := json.Marshal(JSApiConsumerCreateResponse{
		ApiResponse: ApiResponse{Error: NewJSRequiredApiLevelError()},
	})
	require_NoError(t, err)

	// Original request, asking for API level 5 with stream recreation detection.
	m1 := require_ChanRead(t, reqCh, time.Second)
	var ccr1 CreateConsumerRequest
	require_NoError(t, json.Unmarshal(m1.Data, &ccr1))
	require_True(t, ccr1.Config.Sourcing)
	require_Equal(t, m1.Header.Get(JSRequiredApiLevel), "5")
	reply1 := m1.Reply

	// Answer with a required API level error.
	require_NoError(t, nc.Publish(reply1, errResp))
	require_NoError(t, nc.Flush())

	// First retry steps down to API level 4, still sourcing.
	m2 := require_ChanRead(t, reqCh, time.Second)
	var ccr2 CreateConsumerRequest
	require_NoError(t, json.Unmarshal(m2.Data, &ccr2))
	require_True(t, ccr2.Config.Sourcing)
	require_Equal(t, m2.Header.Get(JSRequiredApiLevel), "4")
	reply2 := m2.Reply
	require_NotEqual(t, reply1, reply2)

	// Answer with a required API level error again.
	require_NoError(t, nc.Publish(reply2, errResp))
	require_NoError(t, nc.Flush())

	// Second retry drops sourcing and the required API level.
	m3 := require_ChanRead(t, reqCh, time.Second)
	var ccr3 CreateConsumerRequest
	require_NoError(t, json.Unmarshal(m3.Data, &ccr3))
	require_False(t, ccr3.Config.Sourcing)
	require_Equal(t, m3.Header.Get(JSRequiredApiLevel), _EMPTY_)
	reply3 := m3.Reply
	require_NotEqual(t, reply1, reply3)
	require_NotEqual(t, reply2, reply3)

	// Stale responses on previous reply subjects must not be picked up.
	staleResp, err := json.Marshal(JSApiConsumerCreateResponse{
		ApiResponse: ApiResponse{Error: NewJSStreamNotFoundError()},
	})
	require_NoError(t, err)
	require_NoError(t, nc.Publish(reply1, staleResp))
	require_NoError(t, nc.Publish(reply2, staleResp))

	okResp, err := json.Marshal(JSApiConsumerCreateResponse{
		ConsumerInfo: &ConsumerInfo{Name: ccr3.Config.Name, Config: &ccr3.Config},
	})
	require_NoError(t, err)
	require_NoError(t, nc.Publish(reply3, okResp))
	require_NoError(t, nc.Flush())

	checkFor(t, 2*time.Second, 100*time.Millisecond, func() error {
		mset.mu.RLock()
		defer mset.mu.RUnlock()
		for _, si := range mset.sources {
			if si.err != nil {
				return fmt.Errorf("source is in error state: %v", si.err)
			}
			if si.cname != ccr3.Config.Name {
				return fmt.Errorf("expected consumer name %q, got %q", ccr3.Config.Name, si.cname)
			}
			return nil
		}
		return fmt.Errorf("source info not found")
	})
}

// https://github.com/nats-io/nats-server/issues/8558
func TestJetStreamSourceConsumerUsesScopedAPIReplySubject(t *testing.T) {
	for _, test := range []struct {
		kind     string
		name     string
		durable  bool
		consumer string
	}{
		{"source", "consumer create", false, ""},
		{"source", "consumer reset", true, "SOURCE_CONSUMER"},
		{"mirror", "consumer create", false, ""},
		{"mirror", "consumer reset", true, "MIRROR_CONSUMER"},
	} {
		t.Run(test.kind+"/"+test.name, func(t *testing.T) {
			s := RunBasicJetStreamServer(t)
			defer s.Shutdown()

			o := *s.getOpts()
			o.FeatureFlags = map[string]bool{FeatureFlagJsAPIReplyFormatV2: true}
			s.setOpts(&o)

			nc, _ := jsClientConnect(t, s)
			defer nc.Close()

			const apiPrefix = "$JS.CLOUD.API"
			reqCh := make(chan *nats.Msg, 1)
			_, err := nc.Subscribe(apiPrefix+".CONSUMER.>", func(m *nats.Msg) { reqCh <- m })
			require_NoError(t, err)
			require_NoError(t, nc.Flush())

			source := &StreamSource{
				Name:     "ORIGIN",
				External: &ExternalStream{ApiPrefix: apiPrefix},
			}
			if test.durable {
				source.Consumer = &StreamConsumerSource{Name: test.consumer, DeliverSubject: "sync"}
			}
			cfg := &StreamConfig{Name: "SOURCE", Storage: MemoryStorage}
			if test.kind == "mirror" {
				cfg.Mirror = source
			} else {
				cfg.Sources = []*StreamSource{source}
			}
			addStream(t, nc, cfg)

			req := require_ChanRead(t, reqCh, time.Second)
			consumer := test.consumer
			if !test.durable {
				var ccr CreateConsumerRequest
				require_NoError(t, json.Unmarshal(req.Data, &ccr))
				consumer = ccr.Config.Name
			}
			prefix := fmt.Sprintf("$JSC.R.CLOUD.%s.ORIGIN.%s.", getHash(globalAccountName), consumer)
			if !strings.HasPrefix(req.Reply, prefix) {
				t.Fatalf("expected scoped reply prefix %q, got %q", prefix, req.Reply)
			}
			require_Equal(t, len(strings.Split(req.Reply, ".")), 7)
		})
	}
}

func TestJetStreamMirrorRetriesOnSeqAlreadySeenMismatch(t *testing.T) {
	s := RunBasicJetStreamServer(t)
	defer s.Shutdown()

	nc, js := jsClientConnect(t, s)
	defer nc.Close()

	_, err := js.AddStream(&nats.StreamConfig{
		Name:     "M",
		Subjects: []string{"foo"},
	})
	require_NoError(t, err)

	for range 5 {
		_, err := js.Publish("foo", []byte("seed"))
		require_NoError(t, err)
	}

	mset, err := s.globalAccount().lookupStream("M")
	require_NoError(t, err)

	qch := make(chan struct{})
	mset.mu.Lock()
	mset.mirror = &sourceInfo{name: "X", cname: "X", sseq: 4, qch: qch}
	mset.mu.Unlock()

	// With mset.mirror{cname:"X", sseq:4}, sseq==5 takes the sequential
	// branch and then forces errLastSeqMismatch from processJetStreamMsg
	// with sseq<=lseq.
	rply := fmt.Sprintf("$JS.ACK.M.X.1.5.1.%d.0", time.Now().UnixNano())
	mset.processInboundMirrorMsg(&inMsg{subj: "bar", rply: rply, msg: []byte("x")})

	select {
	case <-qch:
		// expected: retryMirrorConsumer ran and canceled the mirror.
	case <-time.After(2 * time.Second):
		t.Fatal("retryMirrorConsumer was not called after errLastSeqMismatch in the sseq<=lseq branch")
	}
}

// Must be run with -race.
func TestJetStreamHealthzConsumerCheckConcurrentMapRace(t *testing.T) {
	s := RunBasicJetStreamServer(t)
	defer s.Shutdown()

	nc, js := jsClientConnect(t, s)
	defer nc.Close()

	_, err := js.AddStream(&nats.StreamConfig{
		Name:     "TEST",
		Subjects: []string{"foo"},
	})
	require_NoError(t, err)

	acc := s.globalAccount()
	mset, err := acc.lookupStream("TEST")
	require_NoError(t, err)

	opts := &HealthzOptions{
		Account:  acc.Name,
		Stream:   "TEST",
		Consumer: "MISSING",
		Details:  true,
	}

	var wg sync.WaitGroup
	stop := make(chan struct{})

	// Writer: repeatedly create and delete consumers in the stream's map,
	// holding mset.mu just like setConsumer/removeConsumer.
	wg.Add(1)
	go func() {
		defer wg.Done()
		i := 0
		for {
			select {
			case <-stop:
				return
			default:
			}
			name := fmt.Sprintf("C-%d", i)
			i++
			mset.mu.Lock()
			mset.consumers[name] = &consumer{name: name}
			mset.mu.Unlock()
			mset.mu.Lock()
			delete(mset.consumers, name)
			mset.mu.Unlock()
		}
	}()

	// Reader: hammer healthz which (on unfixed code) ranges mset.consumers
	// without any lock.
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-stop:
				return
			default:
			}
			_ = s.healthz(opts)
		}
	}()

	// Let the goroutines race for a bounded amount of time.
	time.Sleep(2 * time.Second)
	close(stop)
	wg.Wait()
}

// https://github.com/nats-io/nats-server/issues/7842
func TestJetStreamJSAckSuffixCorruption(t *testing.T) {
	const (
		accountStr  = "$G"
		subjectStr  = "foo.bar.baz"
		replyStr    = "$JS.ACK.STREAM.CONS.1.2.3.4.5"
		deliverStr  = "DELIVER_HERE"
		payloadStr  = "hello world"
		accountName = "ACC"
	)

	// Lay out a realistic routed pub frame header in one backing array so the
	// reply field shares storage with the following bytes. The "reply" array
	// is deliberately given more underlying capacity, as the parser would.
	backing := make([]byte, 0, 128)
	backing = append(backing, accountStr...)
	backing = append(backing, ' ')
	backing = append(backing, subjectStr...)
	backing = append(backing, ' ')
	replyStart := len(backing)
	backing = append(backing, replyStr...)
	reply := backing[replyStart : replyStart+len(replyStr) : cap(backing)]
	backing = append(backing, ' ')
	sizeBytes := []byte("11")
	sizeStart := len(backing)
	backing = append(backing, sizeBytes...)
	szb := backing[sizeStart : sizeStart+len(sizeBytes)]

	require_False(t, replyHasJSAckSuffix(reply))

	opts := defaultServerOptions
	srv := New(&opts)
	sender := &client{
		srv:  srv,
		kind: ROUTER,
		acc:  &Account{Name: accountName},
		parseState: parseState{
			pa: pubArg{
				hdr:     -1,
				size:    len(payloadStr),
				account: []byte(accountStr),
				subject: []byte(subjectStr),
				reply:   reply,
				szb:     szb,
			},
		},
	}
	sender.initClient()

	fakeConn := &testConnWritePartial{}
	target := &client{
		srv:   srv,
		nc:    fakeConn,
		kind:  ROUTER,
		route: &route{},
	}
	target.initClient()
	r := &SublistResult{
		psubs: []*subscription{{client: target}},
	}

	sender.processMsgResults(
		sender.acc, r,
		[]byte(payloadStr+CR_LF),
		[]byte(deliverStr),
		[]byte(subjectStr),
		reply,
		pmrAllowSendFromRouteToRoute,
	)

	target.mu.Lock()
	target.flushOutbound()
	target.mu.Unlock()

	frame := fakeConn.buf.Bytes()
	require_LessThan(t, 0, len(frame))

	receiver := dummyRouteClient()
	receiver.route = &route{}
	require_NoError(t, receiver.parse(frame))
}

func TestJetStreamMirrorDeliverByStartTime(t *testing.T) {
	test := func(t *testing.T, storage nats.StorageType) {
		s := RunBasicJetStreamServer(t)
		defer s.Shutdown()

		nc, js := jsClientConnect(t, s)
		defer nc.Close()

		_, err := js.AddStream(&nats.StreamConfig{
			Name:     "S",
			Subjects: []string{"data.>"},
			Storage:  storage,
		})
		require_NoError(t, err)

		// Seed source stream with old timestamps.
		mset, err := s.globalAccount().lookupStream("S")
		require_NoError(t, err)

		base := time.Now().Add(-30 * 24 * time.Hour).UTC().Truncate(time.Minute)
		payload := bytes.Repeat([]byte("A"), 64*1024)
		const (
			units = 200        // matching messages
			gap   = uint64(20) // non-matching messages between each
		)

		var seq uint64
		for i := range units {
			seq++
			ts := base.Add(time.Duration(i) * time.Minute).UnixNano()
			require_NoError(t, mset.store.StoreRawMsg("data.one", nil, payload, seq, ts, 0, false))
			for g := range gap {
				seq++
				fts := ts + int64(g+1)*int64(time.Second)
				require_NoError(t, mset.store.StoreRawMsg("data.other", nil, []byte("O"), seq, fts, 0, false))
			}
		}
		// Correct the stream's last sequence.
		mset.mu.Lock()
		mset.lseq = seq
		mset.mu.Unlock()

		// Create a filtered mirror.
		_, err = js.AddStream(&nats.StreamConfig{
			Name:    "M",
			Storage: storage,
			Mirror:  &nats.StreamSource{Name: "S", FilterSubject: "data.one"},
		})
		require_NoError(t, err)

		// Wait for the mirror to backfill all matching messages.
		checkFor(t, 30*time.Second, 100*time.Millisecond, func() error {
			si, err := js.StreamInfo("M")
			if err != nil {
				return err
			}
			if si.State.Msgs != units {
				return fmt.Errorf("mirror has %d msgs, want %d", si.State.Msgs, units)
			}
			return nil
		})

		// Request a start time ~2/3 into the data.
		startIdx := 130
		start := base.Add(time.Duration(startIdx) * time.Minute)

		sub, err := js.PullSubscribe(_EMPTY_, _EMPTY_, nats.BindStream("M"), nats.StartTime(start))
		require_NoError(t, err)
		defer sub.Unsubscribe()

		msgs, err := sub.Fetch(1, nats.MaxWait(time.Second))
		require_NoError(t, err)
		require_Equal(t, len(msgs), 1)
		meta, err := msgs[0].Metadata()
		require_NoError(t, err)
		ts := meta.Timestamp.UTC()
		require_True(t, ts.Equal(start))

		// The mirror preserves source sequences. Pin the exact sequence so
		// an over-skip (resolving too late) can't masquerade as a pass.
		require_Equal(t, meta.Sequence.Stream, uint64(startIdx)*(gap+1)+1)
	}

	t.Run("Memory", func(t *testing.T) { test(t, nats.MemoryStorage) })
	t.Run("File", func(t *testing.T) { test(t, nats.FileStorage) })
}

func TestJetStreamInterestStreamNoInterestSkipAdvancesLastTime(t *testing.T) {
	test := func(t *testing.T, storage nats.StorageType) {
		s := RunBasicJetStreamServer(t)
		defer s.Shutdown()

		nc, js := jsClientConnect(t, s)
		defer nc.Close()

		_, err := js.AddStream(&nats.StreamConfig{
			Name:      "TEST",
			Subjects:  []string{"foo"},
			Storage:   storage,
			Retention: nats.InterestPolicy,
		})
		require_NoError(t, err)

		// A publish without interest still increases the stream sequence.
		pubAck, err := js.Publish("foo", []byte("one"))
		require_NoError(t, err)
		require_Equal(t, pubAck.Sequence, uint64(1))

		si, err := js.StreamInfo("TEST")
		require_NoError(t, err)
		require_Equal(t, si.State.Msgs, uint64(0))
		require_Equal(t, si.State.LastSeq, uint64(1))
		first := si.State.LastTime

		// Let the clock advance past the access time service granularity.
		time.Sleep(2 * ats.TickInterval)

		// The second publish still has no interest, but should update both the sequence and time.
		pubAck, err = js.Publish("foo", []byte("two"))
		require_NoError(t, err)
		require_Equal(t, pubAck.Sequence, uint64(2))

		si, err = js.StreamInfo("TEST")
		require_NoError(t, err)
		require_Equal(t, si.State.Msgs, uint64(0))
		require_Equal(t, si.State.LastSeq, uint64(2))
		require_True(t, si.State.LastTime.After(first))
	}

	t.Run("Memory", func(t *testing.T) { test(t, nats.MemoryStorage) })
	t.Run("File", func(t *testing.T) { test(t, nats.FileStorage) })
}

// Must be run with -race.
func TestJetStreamStreamConfigSourcesDataRace(t *testing.T) {
	s := RunBasicJetStreamServer(t)
	defer s.Shutdown()

	nc, js := jsClientConnect(t, s)
	defer nc.Close()

	_, err := js.AddStream(&nats.StreamConfig{Name: "TEST", Subjects: []string{"foo"}})
	require_NoError(t, err)
	mset, err := s.globalAccount().lookupStream("TEST")
	require_NoError(t, err)

	var wg sync.WaitGroup
	stop := make(chan struct{})

	// Reader: continuously copy the whole config (reads Sources slice header)
	// under mset.cfgMu.RLock.
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-stop:
				return
			default:
			}
			cfg := mset.config()
			_ = len(cfg.Sources)
		}
	}()

	// Writer: drive updates that add a brand new Source each iteration, which
	// hits the `mset.cfg.Sources = append(...)` mutation under mset.mu only.
	wg.Add(1)
	go func() {
		defer wg.Done()
		defer close(stop)
		for i := range 200 {
			cfg := mset.config()
			cfg.Sources = append(cfg.Sources, &StreamSource{Name: fmt.Sprintf("SRC_%d", i)})
			if err := mset.updateWithAdvisory(&cfg, false, false); err != nil {
				// Don't fail the test on update errors (sources point at
				// non-existent streams); we only care about the mutation race.
				return
			}
		}
	}()

	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		close(stop)
		t.Fatal("timeout")
	}
}

// Must be run with -race.
func TestJetStreamStreamSubjectsOverlapDataRace(t *testing.T) {
	s := RunBasicJetStreamServer(t)
	defer s.Shutdown()

	acc := s.globalAccount()
	jsa := acc.js
	require_NotNil(t, jsa)

	// Stream A: the "other" stream whose cfg.Subjects gets concurrently
	// updated while subjectsOverlap reads it.
	msetA, err := acc.addStream(&StreamConfig{Name: "A", Subjects: []string{"a.>"}})
	require_NoError(t, err)

	var wg sync.WaitGroup
	stop := make(chan struct{})

	// Writer: drive concurrent writes of msetA.cfg under cfgMu, exactly as
	// updateConfig does (mset.cfg = *cfg under cfgMu).
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; ; i++ {
			select {
			case <-stop:
				return
			default:
			}
			ncfg := msetA.config()
			ncfg.Subjects = []string{fmt.Sprintf("a.%d.>", i)}
			// Mirror updateConfig's write: under mset.mu and mset.cfgMu.
			msetA.mu.Lock()
			msetA.cfgMu.Lock()
			msetA.cfg = ncfg
			msetA.cfgMu.Unlock()
			msetA.mu.Unlock()
		}
	}()

	// Reader: call the real subjectsOverlap under jsa.mu, which reads
	// msetA.cfg.Subjects. self is nil so A is inspected.
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-stop:
				return
			default:
			}
			jsa.mu.RLock()
			jsa.subjectsOverlap([]string{"b.>"}, nil)
			jsa.mu.RUnlock()
		}
	}()

	time.Sleep(300 * time.Millisecond)
	close(stop)
	wg.Wait()
}

// jsPoisonAPIPrefix is the external JetStream API prefix used by the poisoned
// consumer create reply tests below. It must not overlap with $JS.API.
const jsPoisonAPIPrefix = "XAPI"

// jsPoisonedAPIResponder answers every request sent to the external JetStream
// API prefix with the given (forged) body. Since a stream that sources or
// mirrors with an External.ApiPrefix sends its consumer create requests to a
// foreign account/domain, this responder is the only one answering, which makes
// these tests deterministic instead of racing the real JetStream API.
func jsPoisonedAPIResponder(t *testing.T, nc *nats.Conn, reply string) *atomic.Int64 {
	t.Helper()
	var requests atomic.Int64
	_, err := nc.Subscribe(jsPoisonAPIPrefix+".>", func(m *nats.Msg) {
		requests.Add(1)
		m.Respond([]byte(reply))
	})
	require_NoError(t, err)
	require_NoError(t, nc.Flush())
	return &requests
}

// jsPoisonStreamInfo requests stream info with a short timeout. It returns an
// error instead of failing the test so that a wedged stream (a panic while
// holding the stream lock would deadlock it forever) surfaces as a test failure
// with a useful message rather than as a hang.
func jsPoisonStreamInfo(nc *nats.Conn, stream string) (*StreamInfo, error) {
	rmsg, err := nc.Request(fmt.Sprintf(JSApiStreamInfoT, stream), nil, 2*time.Second)
	if err != nil {
		return nil, fmt.Errorf("stream info for %q failed, stream may be wedged: %v", stream, err)
	}
	var resp JSApiStreamInfoResponse
	if err := json.Unmarshal(rmsg.Data, &resp); err != nil {
		return nil, err
	}
	if resp.Error != nil {
		return nil, resp.Error
	}
	return resp.StreamInfo, nil
}

func TestJetStreamMirrorConsumerCreatePoisonedReply(t *testing.T) {
	for _, test := range []struct {
		name    string
		reply   string
		durable bool
	}{
		// A `{"config":null}` body decodes into a JSApiConsumerCreateResponse with a
		// non-nil embedded *ConsumerInfo but a nil Config, and no error. Without a
		// Config check it panics on ccr.ConsumerInfo.Config.DeliverSubject.
		{"config_null", `{"config":null}`, false},
		// Any other ConsumerInfo field without a "config" key does the same. With a
		// durable sourcing consumer we panic on ccr.ConsumerInfo.Config.AckPolicy.
		{"missing_config_durable", `{"stream_name":"S","name":"MC"}`, true},
	} {
		t.Run(test.name, func(t *testing.T) {
			s := RunBasicJetStreamServer(t)
			defer s.Shutdown()

			nc, _ := jsClientConnect(t, s)
			defer nc.Close()

			requests := jsPoisonedAPIResponder(t, nc, test.reply)

			mirror := &StreamSource{
				Name:     "S",
				External: &ExternalStream{ApiPrefix: jsPoisonAPIPrefix},
			}
			if test.durable {
				mirror.Consumer = &StreamConsumerSource{Name: "MC", DeliverSubject: "d.mirror"}
			}
			addStream(t, nc, &StreamConfig{Name: "M", Storage: MemoryStorage, Mirror: mirror})

			// The mirror must reject the response, report it and retry, and the stream
			// must remain responsive.
			checkFor(t, 30*time.Second, 250*time.Millisecond, func() error {
				si, err := jsPoisonStreamInfo(nc, "M")
				if err != nil {
					return err
				}
				if si.Mirror == nil || si.Mirror.Error == nil {
					return fmt.Errorf("expected mirror error to be reported, got %+v", si.Mirror)
				}
				if n := requests.Load(); n < 2 {
					return fmt.Errorf("expected mirror consumer create to be retried, got %d request(s)", n)
				}
				return nil
			})
		})
	}
}

func TestJetStreamSourceConsumerCreatePoisonedReply(t *testing.T) {
	for _, test := range []struct {
		name    string
		reply   string
		durable bool
	}{
		{"config_null", `{"config":null}`, false},
		{"missing_config_durable", `{"stream_name":"S","name":"SC"}`, true},
	} {
		t.Run(test.name, func(t *testing.T) {
			s := RunBasicJetStreamServer(t)
			defer s.Shutdown()

			nc, _ := jsClientConnect(t, s)
			defer nc.Close()

			requests := jsPoisonedAPIResponder(t, nc, test.reply)

			source := &StreamSource{
				Name:     "S",
				External: &ExternalStream{ApiPrefix: jsPoisonAPIPrefix},
			}
			if test.durable {
				source.Consumer = &StreamConsumerSource{Name: "SC", DeliverSubject: "d.source"}
			}
			addStream(t, nc, &StreamConfig{Name: "W", Storage: MemoryStorage, Sources: []*StreamSource{source}})

			checkFor(t, 30*time.Second, 250*time.Millisecond, func() error {
				si, err := jsPoisonStreamInfo(nc, "W")
				if err != nil {
					return err
				}
				if len(si.Sources) != 1 || si.Sources[0].Error == nil {
					return fmt.Errorf("expected source error to be reported, got %+v", si.Sources)
				}
				if n := requests.Load(); n < 2 {
					return fmt.Errorf("expected source consumer create to be retried, got %d request(s)", n)
				}
				return nil
			})
		})
	}
}

func TestJetStreamSourceStreamRecreated(t *testing.T) {
	const (
		durableConsumer = "C"
		deliverSubject  = "deliver-subject"
	)

	test := func(t *testing.T, replicas int, retention RetentionPolicy, durable bool) {
		var s *Server
		var c *cluster
		if replicas == 1 {
			s = RunBasicJetStreamServer(t)
			defer s.Shutdown()
		} else {
			c = createJetStreamClusterExplicit(t, "R3S", 3)
			defer c.shutdown()
			s = c.randomServer()
		}

		nc, js := jsClientConnect(t, s)
		defer nc.Close()

		lookupStreamOnLeader := func(name string) (*stream, error) {
			t.Helper()
			ls := s
			if c != nil {
				ls = c.streamLeader(globalAccountName, name)
			}
			if ls == nil {
				return nil, fmt.Errorf("no stream leader")
			}
			return ls.globalAccount().lookupStream(name)
		}

		addOrigin := func() {
			t.Helper()
			_, err := jsStreamCreate(t, nc, &StreamConfig{
				Name:      "ORIGIN",
				Subjects:  []string{"foo"},
				Storage:   FileStorage,
				Retention: retention,
				Replicas:  replicas,
			})
			require_NoError(t, err)
			if durable {
				_, err = jsConsumerCreate(t, nc, "ORIGIN", ConsumerConfig{
					Durable:        durableConsumer,
					DeliverSubject: deliverSubject,
					AckPolicy:      AckFlowControl,
					Heartbeat:      time.Second,
					Replicas:       replicas,
				}, false)
				require_NoError(t, err)
			}
		}
		publish := func(n int) {
			t.Helper()
			for range n {
				_, err := js.Publish("foo", nil)
				require_NoError(t, err)
			}
		}
		checkSourced := func(msgs uint64) {
			t.Helper()
			checkFor(t, 10*time.Second, 100*time.Millisecond, func() error {
				si, err := js.StreamInfo("SOURCE")
				if err != nil {
					return err
				}
				if si.State.Msgs != msgs {
					return fmt.Errorf("expected %d msgs, got %d", msgs, si.State.Msgs)
				}
				return nil
			})
		}
		waitForSourceConsumer := func() {
			t.Helper()
			// Must allow for the source consumer to be recreated, which is gated by sourceConsumerRetryThreshold.
			checkFor(t, 10*time.Second, 100*time.Millisecond, func() error {
				if mset, err := lookupStreamOnLeader("ORIGIN"); err != nil {
					return err
				} else if n := mset.numConsumers(); n == 0 {
					return errors.New("no sourcing consumer yet")
				}
				// The sourcing stream must also be subscribed on the deliver subject.
				smset, err := lookupStreamOnLeader("SOURCE")
				if err != nil {
					return err
				}
				smset.mu.RLock()
				defer smset.mu.RUnlock()
				if len(smset.sources) == 0 {
					return errors.New("no source info yet")
				}
				for iname, si := range smset.sources {
					if si.sip || si.sub == nil {
						return fmt.Errorf("source %q not wired up yet", iname)
					}
				}
				return nil
			})
		}

		addOrigin()
		srcCfg := &StreamSource{Name: "ORIGIN"}
		if durable {
			srcCfg.Consumer = &StreamConsumerSource{Name: durableConsumer, DeliverSubject: deliverSubject}
		}
		_, err := jsStreamCreate(t, nc, &StreamConfig{
			Name:     "SOURCE",
			Storage:  FileStorage,
			Replicas: replicas,
			Sources:  []*StreamSource{srcCfg},
		})
		require_NoError(t, err)
		waitForSourceConsumer()

		publish(10)
		checkSourced(10)

		// Delete and recreate the source stream, its sequences start from scratch.
		require_NoError(t, js.DeleteStream("ORIGIN"))
		addOrigin()

		// Short-circuit recreation of the source consumers on the SOURCE leader.
		mset, err := lookupStreamOnLeader("SOURCE")
		require_NoError(t, err)
		mset.mu.Lock()
		err = mset.setupSourceConsumers()
		mset.mu.Unlock()
		require_NoError(t, err)
		waitForSourceConsumer()

		// Sourcing must pick back up, instead of waiting for the source stream
		// to reach the sequence we had sourced up to before.
		publish(5)
		checkSourced(15)

		// There must only be a single consumer on the recreated stream. Having multiple
		// would show that consumers are leaked.
		checkFor(t, 5*time.Second, 100*time.Millisecond, func() error {
			if mset, err := lookupStreamOnLeader("ORIGIN"); err != nil {
				return err
			} else if n := mset.numConsumers(); n != 1 {
				return fmt.Errorf("expected 1 sourcing consumer, got %d", n)
			}
			return nil
		})
	}

	for _, replicas := range []int{1, 3} {
		for _, retention := range []RetentionPolicy{LimitsPolicy, InterestPolicy, WorkQueuePolicy} {
			for _, durable := range []bool{false, true} {
				kind := "Implicit"
				if durable {
					kind = "Durable"
				}
				t.Run(fmt.Sprintf("R%d/%s/%s", replicas, retention, kind), func(t *testing.T) {
					test(t, replicas, retention, durable)
				})
			}
		}
	}
}

func TestJetStreamSourceStreamRecreatedRespectsStartSeq(t *testing.T) {
	test := func(t *testing.T, replicas int) {
		var s *Server
		var c *cluster
		if replicas == 1 {
			s = RunBasicJetStreamServer(t)
			defer s.Shutdown()
		} else {
			c = createJetStreamClusterExplicit(t, "R3S", 3)
			defer c.shutdown()
			s = c.randomServer()
		}

		nc, js := jsClientConnect(t, s)
		defer nc.Close()

		lookupStreamOnLeader := func(name string) (*stream, error) {
			t.Helper()
			ls := s
			if c != nil {
				ls = c.streamLeader(globalAccountName, name)
			}
			if ls == nil {
				return nil, fmt.Errorf("no stream leader")
			}
			return ls.globalAccount().lookupStream(name)
		}

		addOrigin := func() {
			t.Helper()
			_, err := jsStreamCreate(t, nc, &StreamConfig{
				Name:     "ORIGIN",
				Subjects: []string{"foo"},
				Storage:  FileStorage,
				Replicas: replicas,
			})
			require_NoError(t, err)
			if c != nil {
				c.waitOnStreamLeader(globalAccountName, "ORIGIN")
			}
		}
		// publish sends n messages to the origin, each body carrying "<marker><i>" so we
		// can assert exactly which origin sequences were sourced into the destination.
		publish := func(marker string, n int) {
			t.Helper()
			for i := 1; i <= n; i++ {
				_, err := js.Publish("foo", []byte(fmt.Sprintf("%s%d", marker, i)))
				require_NoError(t, err)
			}
		}
		checkSourced := func(msgs uint64) {
			t.Helper()
			checkFor(t, 10*time.Second, 100*time.Millisecond, func() error {
				si, err := js.StreamInfo("SOURCE")
				if err != nil {
					return err
				}
				if si.State.Msgs != msgs {
					return fmt.Errorf("expected %d msgs, got %d", msgs, si.State.Msgs)
				}
				return nil
			})
		}
		requireBody := func(seq uint64, expected string) {
			t.Helper()
			m, err := js.GetMsg("SOURCE", seq)
			require_NoError(t, err)
			require_Equal(t, string(m.Data), expected)
		}
		waitForSourceConsumer := func() {
			t.Helper()
			// Must allow for the source consumer to be recreated, which is gated by sourceConsumerRetryThreshold.
			checkFor(t, 10*time.Second, 100*time.Millisecond, func() error {
				if mset, err := lookupStreamOnLeader("ORIGIN"); err != nil {
					return err
				} else if n := mset.numConsumers(); n == 0 {
					return errors.New("no sourcing consumer yet")
				}
				return nil
			})
		}

		// First incarnation of the origin, sourced with OptStartSeq set.
		addOrigin()
		_, err := jsStreamCreate(t, nc, &StreamConfig{
			Name:     "SOURCE",
			Storage:  FileStorage,
			Replicas: replicas,
			Sources:  []*StreamSource{{Name: "ORIGIN", OptStartSeq: 6}},
		})
		require_NoError(t, err)
		waitForSourceConsumer()

		// Publish origin sequences 1..10. OptStartSeq must be respected, so only origin
		// sequences 6..10 (5 messages) are sourced, and nothing below the start sequence.
		publish("a", 10)
		checkSourced(5)
		requireBody(1, "a6")
		requireBody(5, "a10")

		// Delete and recreate the origin, its sequences restart from scratch.
		require_NoError(t, js.DeleteStream("ORIGIN"))
		addOrigin()

		// Short-circuit recreation of the source consumer on the SOURCE leader.
		mset, err := lookupStreamOnLeader("SOURCE")
		require_NoError(t, err)
		mset.mu.Lock()
		err = mset.setupSourceConsumers()
		mset.mu.Unlock()
		require_NoError(t, err)
		waitForSourceConsumer()

		// Publish origin sequences 1..10 on the recreated origin. OptStartSeq must still be
		// respected, so only origin sequences 6..10 (5 messages) are sourced and nothing
		// below the start sequence. Sourcing from the beginning would instead pick up
		// sequence 1 (body "b1") at destination sequence 6.
		publish("b", 10)
		checkSourced(10)
		requireBody(6, "b6")
		requireBody(10, "b10")
	}

	for _, replicas := range []int{1, 3} {
		t.Run(fmt.Sprintf("R%d", replicas), func(t *testing.T) {
			test(t, replicas)
		})
	}
}

func TestJetStreamStreamUpdateReplicasNotSupported(t *testing.T) {
	s := RunBasicJetStreamServer(t)
	defer s.Shutdown()

	nc, js := jsClientConnect(t, s)
	defer nc.Close()

	// Creating a replicated stream is not supported while non-clustered.
	cfg := &nats.StreamConfig{Name: "TEST", Subjects: []string{"foo"}, Replicas: 3}
	_, err := js.AddStream(cfg)
	require_Error(t, err, NewJSStreamReplicasNotSupportedError())

	cfg.Replicas = 1
	si, err := js.AddStream(cfg)
	require_NoError(t, err)
	require_Equal(t, si.Config.Replicas, 1)

	// Scaling up using an update must be rejected as well, otherwise the stream
	// would report itself as replicated while it can't be, and it would not be
	// recovered from disk after a restart.
	cfg.Replicas = 3
	_, err = js.UpdateStream(cfg)
	require_Error(t, err, NewJSStreamReplicasNotSupportedError())

	// The update must not have been applied.
	si, err = js.StreamInfo("TEST")
	require_NoError(t, err)
	require_Equal(t, si.Config.Replicas, 1)

	// Unrelated updates must still be allowed.
	cfg.Replicas = 1
	cfg.Subjects = append(cfg.Subjects, "bar")
	si, err = js.UpdateStream(cfg)
	require_NoError(t, err)
	require_Equal(t, si.Config.Replicas, 1)
	require_Equal(t, len(si.Config.Subjects), 2)
}

func TestJetStreamStreamRecoverReplicasNotSupported(t *testing.T) {
	s := RunBasicJetStreamServer(t)
	defer s.Shutdown()

	nc, js := jsClientConnect(t, s)
	defer nc.Close()

	_, err := js.AddStream(&nats.StreamConfig{Name: "TEST", Subjects: []string{"foo"}})
	require_NoError(t, err)

	// Simulate a config stored by a server that still allowed updating to R>1
	// while non-clustered. Can't go through the API, that's rejected now.
	mset, err := s.globalAccount().lookupStream("TEST")
	require_NoError(t, err)
	cfg := mset.config()
	cfg.Replicas = 3
	mset.mu.RLock()
	store := mset.store
	mset.mu.RUnlock()
	require_NoError(t, store.UpdateConfig(&cfg))

	// Restart.
	sd := s.JetStreamConfig().StoreDir
	nc.Close()
	s.Shutdown()
	s = RunJetStreamServerOnPort(-1, sd)
	defer s.Shutdown()

	nc, js = jsClientConnect(t, s)
	defer nc.Close()

	// Must not be loaded, R>1 is not supported while non-clustered.
	_, err = js.StreamInfo("TEST")
	require_Error(t, err, nats.ErrStreamNotFound)
	_, err = s.GlobalAccount().lookupStream("TEST")
	require_Error(t, err, NewJSStreamNotFoundError())
}

func TestJetStreamSourceHeaderNotAllowedIfNotSourced(t *testing.T) {
	test := func(t *testing.T, replicas int) {
		var s *Server
		if replicas == 1 {
			s = RunBasicJetStreamServer(t)
			defer s.Shutdown()
		} else {
			c := createJetStreamClusterExplicit(t, "R3S", replicas)
			defer c.shutdown()
			s = c.randomServer()
		}

		nc, js := jsClientConnect(t, s)
		defer nc.Close()

		_, err := js.AddStream(&nats.StreamConfig{
			Name:     "TEST",
			Subjects: []string{"foo"},
			Replicas: replicas,
		})
		require_NoError(t, err)

		m := nats.NewMsg("foo")
		m.Header.Set("Nats-Stream-Source", "bar")
		_, err = js.PublishMsg(m)
		require_Error(t, err, NewJSMessageSourceHdrNotAllowedError())
	}

	t.Run("R1", func(t *testing.T) { test(t, 1) })
	t.Run("R3", func(t *testing.T) { test(t, 3) })
}

func TestJetStreamSetConsumerIgnoresDuplicateName(t *testing.T) {
	mset := &stream{consumers: make(map[string]*consumer)}

	first := &consumer{name: "dur", subjf: []*subjectFilter{{subject: "foo"}}}
	first.cfg.FilterSubject = "foo"
	mset.setConsumer(first)
	require_Equal(t, mset.consumers["dur"], first)
	require_Equal(t, mset.csl.Count(), 1)
	require_Len(t, len(mset.cList), 1)

	// A different object trying to register under the same name must be ignored.
	dup := &consumer{name: "dur", subjf: []*subjectFilter{{subject: "foo"}}}
	dup.cfg.FilterSubject = "foo"
	mset.setConsumer(dup)

	require_Equal(t, mset.consumers["dur"], first) // not overwritten
	require_Equal(t, mset.csl.Count(), 1)          // not double-counted
	require_Len(t, len(mset.cList), 1)             // no duplicate entry
	require_Equal(t, mset.cList[0], first)
}

func TestJetStreamRemoveConsumerOnlyRemovesMatchingInstance(t *testing.T) {
	mset := &stream{consumers: make(map[string]*consumer)}

	good := &consumer{name: "dur", subjf: []*subjectFilter{{subject: "foo"}}}
	good.cfg.FilterSubject = "foo"
	mset.setConsumer(good)
	require_Equal(t, mset.csl.Count(), 1)
	require_Len(t, len(mset.cList), 1)

	// A stale object left over from a failed create: same name, different identity.
	stale := &consumer{name: "dur"}
	stale.cfg.FilterSubject = "foo"
	mset.removeConsumer(stale)

	require_Equal(t, mset.consumers["dur"], good) // valid consumer survives
	require_Equal(t, mset.csl.Count(), 1)
	require_Len(t, len(mset.cList), 1)
	require_Equal(t, mset.cList[0], good)

	// Removing the registered consumer must still work.
	mset.removeConsumer(good)
	_, ok := mset.consumers["dur"]
	require_True(t, !ok)
	require_Equal(t, mset.csl.Count(), 0)
	require_Len(t, len(mset.cList), 0)
}

// https://github.com/nats-io/nats-server/issues/8322
func TestJetStreamDynamicMaxStoreStableAcrossRestart(t *testing.T) {
	sd := t.TempDir()
	tmpl := `
		listen: 127.0.0.1:-1
		jetstream: {store_dir: %q}
		accounts {
			A { jetstream: {max_store: %d}, users: [{user: a, password: p}] }
			$SYS { users: [{user: admin, password: s3cr3t!}] }
		}
	`
	// Start out with no meaningful account limit so we can lay down some data.
	conf := createConfFile(t, []byte(fmt.Sprintf(tmpl, sd, -1)))
	s, _ := RunServerWithConfig(conf)
	defer s.Shutdown()

	before := s.JetStreamConfig().MaxStore
	require_True(t, before > 0)

	nc, js := jsClientConnect(t, s, nats.UserInfo("a", "p"))
	defer nc.Close()

	_, err := js.AddStream(&nats.StreamConfig{Name: "TEST", Storage: nats.FileStorage})
	require_NoError(t, err)

	// Fill enough of the disk that an available disk space calculation would notice.
	const (
		msgSize = 128 * 1024
		numMsgs = 512
		written = msgSize * numMsgs
	)
	msg := make([]byte, msgSize)
	for range numMsgs {
		_, err = js.Publish("TEST", msg)
		require_NoError(t, err)
	}
	nc.Close()
	s.Shutdown()
	s.WaitForShutdown()

	// Pick a limit that fits what the server allowed on the first boot, but not
	// what a free-space-only recomputation would allow now that we wrote data.
	limit := before - written/4
	require_True(t, limit > 0)

	// Start by hand, RunServerWithConfig panics when JetStream fails to enable.
	conf = createConfFile(t, []byte(fmt.Sprintf(tmpl, sd, limit)))
	s, err = NewServer(LoadConfig(conf))
	require_NoError(t, err)
	defer s.Shutdown()
	s.Start()

	// An account limit that was valid on the previous boot must not stop us from
	// coming back up, the data it covers is already on disk.
	if err := s.readyForConnections(5 * time.Second); err != nil || !s.JetStreamEnabled() {
		t.Fatalf("JetStream failed to start with an account limit of %v, %v was available before",
			friendlyBytes(limit), friendlyBytes(before))
	}

	// Allow slack for other activity on the same volume, but the limit must not
	// have dropped by anything close to what we just wrote.
	after := s.JetStreamConfig().MaxStore
	if slack := int64(written / 4); before-after > slack {
		t.Fatalf("Dynamic max store shrank after writing %v: before %v, after %v",
			friendlyBytes(int64(written)), friendlyBytes(before), friendlyBytes(after))
	}
}

func TestJetStreamInterestCheckWithoutConsumersDoesNotCompact(t *testing.T) {
	s := RunBasicJetStreamServer(t)
	defer s.Shutdown()

	nc, js := jsClientConnect(t, s)
	defer nc.Close()

	_, err := js.AddStream(&nats.StreamConfig{
		Name:      "TEST",
		Subjects:  []string{"foo"},
		Storage:   nats.FileStorage,
		Retention: nats.InterestPolicy,
	})
	require_NoError(t, err)

	mset, err := s.GlobalAccount().lookupStream("TEST")
	require_NoError(t, err)
	const numMsgs = 3
	for range numMsgs {
		_, _, err = mset.store.StoreMsg("foo", nil, []byte("msg"), 0)
		require_NoError(t, err)
	}

	// Without consumers there is no ack floor to compact to.
	mset.checkInterestState()
	state := mset.state()
	require_Equal(t, state.Msgs, numMsgs)
	require_Equal(t, state.FirstSeq, 1)
	require_Equal(t, state.LastSeq, numMsgs)
}
