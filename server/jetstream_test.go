// Copyright 2019-2021 The NATS Authors
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
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"math/rand"
	"net/http"
	"net/url"
	"os"
	"path"
	"path/filepath"
	"reflect"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nats-io/nats-server/v2/server/sysmem"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nuid"
)

func TestJetStreamBasicNilConfig(t *testing.T) {
	s := RunRandClientPortServer()
	defer s.Shutdown()

	if config := s.JetStreamConfig(); config != nil {
		defer removeDir(t, config.StoreDir)
	}

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

func RunBasicJetStreamServer() *Server {
	opts := DefaultTestOptions
	opts.Port = -1
	opts.JetStream = true
	tdir, _ := ioutil.TempDir(tempRoot, "jstests-storedir-")
	opts.StoreDir = tdir
	return RunServer(&opts)
}

func RunJetStreamServerOnPort(port int, sd string) *Server {
	opts := DefaultTestOptions
	opts.Port = port
	opts.JetStream = true
	opts.StoreDir = filepath.Dir(sd)
	return RunServer(&opts)
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
	s := RunBasicJetStreamServer()
	defer s.Shutdown()

	if config := s.JetStreamConfig(); config != nil {
		defer removeDir(t, config.StoreDir)
	}

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
	if err := acc.EnableJetStream(nil); err != nil {
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
	if err := acc.EnableJetStream(nil); err == nil {
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
			s := RunBasicJetStreamServer()
			defer s.Shutdown()

			if config := s.JetStreamConfig(); config != nil {
				defer removeDir(t, config.StoreDir)
			}

			mset, err := s.GlobalAccount().addStream(c.mconfig)
			if err != nil {
				t.Fatalf("Unexpected error adding stream: %v", err)
			}
			defer mset.delete()

			nc := clientConnectToServer(t, s)
			defer nc.Close()

			nc.Publish("foo", []byte("Hello World!"))
			nc.Flush()

			state := mset.state()
			if state.Msgs != 1 {
				t.Fatalf("Expected 1 message, got %d", state.Msgs)
			}
			if state.Bytes == 0 {
				t.Fatalf("Expected non-zero bytes")
			}

			nc.Publish("foo", []byte("Hello World Again!"))
			nc.Flush()

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
			s := RunBasicJetStreamServer()
			defer s.Shutdown()

			if config := s.JetStreamConfig(); config != nil {
				defer removeDir(t, config.StoreDir)
			}

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
	s := RunRandClientPortServer()
	defer s.Shutdown()

	jsconfig := &JetStreamConfig{MaxMemory: -1, MaxStore: 128 * 1024 * 1024 * 1024 * 1024}
	if err := s.EnableJetStream(jsconfig); err != nil {
		t.Fatalf("Expected no error, got %v", err)
	}

	if config := s.JetStreamConfig(); config != nil {
		defer removeDir(t, config.StoreDir)
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

	testBlkSize := func(subject string, maxMsgs, maxBytes int64, expectedBlkSize uint64) {
		t.Helper()
		mset, err := acc.addStream(streamConfig(subject, maxMsgs, maxBytes))
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

	testBlkSize("foo", 1, 0, FileStoreMinBlkSize)
	testBlkSize("foo", 1, 512, FileStoreMinBlkSize)
	testBlkSize("foo", 1, 1024*1024, 262200)
	testBlkSize("foo", 1, 8*1024*1024, 2097200)
	testBlkSize("foo_bar_baz", -1, 32*1024*1024*1024*1024, FileStoreMaxBlkSize)
}

func TestJetStreamPubAck(t *testing.T) {
	s := RunBasicJetStreamServer()
	defer s.Shutdown()

	if config := s.JetStreamConfig(); config != nil {
		defer removeDir(t, config.StoreDir)
	}

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

func TestJetStreamConsumerWithStartTime(t *testing.T) {
	subj := "my_stream"
	cases := []struct {
		name    string
		mconfig *StreamConfig
	}{
		{"MemoryStore", &StreamConfig{Name: subj, Storage: MemoryStorage}},
		{"FileStore", &StreamConfig{Name: subj, Storage: FileStorage}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			s := RunBasicJetStreamServer()
			defer s.Shutdown()

			if config := s.JetStreamConfig(); config != nil {
				defer removeDir(t, config.StoreDir)
			}

			fsCfg := &FileStoreConfig{BlockSize: 100}
			mset, err := s.GlobalAccount().addStreamWithStore(c.mconfig, fsCfg)
			if err != nil {
				t.Fatalf("Unexpected error adding stream: %v", err)
			}
			defer mset.delete()

			nc := clientConnectToServer(t, s)
			defer nc.Close()

			toSend := 250
			for i := 0; i < toSend; i++ {
				sendStreamMsg(t, nc, subj, fmt.Sprintf("MSG: %d", i+1))
			}

			time.Sleep(10 * time.Millisecond)
			startTime := time.Now().UTC()

			for i := 0; i < toSend; i++ {
				sendStreamMsg(t, nc, subj, fmt.Sprintf("MSG: %d", i+1))
			}

			if msgs := mset.state().Msgs; msgs != uint64(toSend*2) {
				t.Fatalf("Expected %d messages, got %d", toSend*2, msgs)
			}

			o, err := mset.addConsumer(&ConsumerConfig{
				Durable:       "d",
				DeliverPolicy: DeliverByStartTime,
				OptStartTime:  &startTime,
				AckPolicy:     AckExplicit,
			})
			if err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}
			defer o.delete()

			msg, err := nc.Request(o.requestNextMsgSubject(), nil, time.Second)
			if err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}
			sseq, dseq, _, _, _ := replyInfo(msg.Reply)
			if dseq != 1 {
				t.Fatalf("Expected delivered seq of 1, got %d", dseq)
			}
			if sseq != uint64(toSend+1) {
				t.Fatalf("Expected to get store seq of %d, got %d", toSend+1, sseq)
			}
		})
	}
}

// Test for https://github.com/nats-io/jetstream/issues/143
func TestJetStreamConsumerWithMultipleStartOptions(t *testing.T) {
	subj := "my_stream"
	cases := []struct {
		name    string
		mconfig *StreamConfig
	}{
		{"MemoryStore", &StreamConfig{Name: subj, Subjects: []string{"foo.>"}, Storage: MemoryStorage}},
		{"FileStore", &StreamConfig{Name: subj, Subjects: []string{"foo.>"}, Storage: FileStorage}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			s := RunBasicJetStreamServer()
			defer s.Shutdown()

			if config := s.JetStreamConfig(); config != nil {
				defer removeDir(t, config.StoreDir)
			}

			mset, err := s.GlobalAccount().addStream(c.mconfig)
			if err != nil {
				t.Fatalf("Unexpected error adding stream: %v", err)
			}
			defer mset.delete()

			nc := clientConnectToServer(t, s)
			defer nc.Close()

			obsReq := CreateConsumerRequest{
				Stream: subj,
				Config: ConsumerConfig{
					Durable:       "d",
					DeliverPolicy: DeliverLast,
					FilterSubject: "foo.22",
					AckPolicy:     AckExplicit,
				},
			}
			req, err := json.Marshal(obsReq)
			if err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}
			_, err = nc.Request(fmt.Sprintf(JSApiConsumerCreateT, subj), req, time.Second)
			if err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}
			nc.Close()
			s.Shutdown()
		})
	}
}

func TestJetStreamConsumerMaxDeliveries(t *testing.T) {
	cases := []struct {
		name    string
		mconfig *StreamConfig
	}{
		{"MemoryStore", &StreamConfig{Name: "MY_WQ", Storage: MemoryStorage}},
		{"FileStore", &StreamConfig{Name: "MY_WQ", Storage: FileStorage}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			s := RunBasicJetStreamServer()
			defer s.Shutdown()

			if config := s.JetStreamConfig(); config != nil {
				defer removeDir(t, config.StoreDir)
			}

			mset, err := s.GlobalAccount().addStream(c.mconfig)
			if err != nil {
				t.Fatalf("Unexpected error adding stream: %v", err)
			}
			defer mset.delete()

			nc := clientConnectToServer(t, s)
			defer nc.Close()

			// Queue up our work item.
			sendStreamMsg(t, nc, c.mconfig.Name, "Hello World!")

			sub, _ := nc.SubscribeSync(nats.NewInbox())
			defer sub.Unsubscribe()
			nc.Flush()

			maxDeliver := 5
			ackWait := 10 * time.Millisecond

			o, err := mset.addConsumer(&ConsumerConfig{
				DeliverSubject: sub.Subject,
				AckPolicy:      AckExplicit,
				AckWait:        ackWait,
				MaxDeliver:     maxDeliver,
			})
			if err != nil {
				t.Fatalf("Expected no error, got %v", err)
			}
			defer o.delete()

			// Wait for redeliveries to pile up.
			checkFor(t, 250*time.Millisecond, 10*time.Millisecond, func() error {
				if nmsgs, _, _ := sub.Pending(); err != nil || nmsgs != maxDeliver {
					return fmt.Errorf("Did not receive correct number of messages: %d vs %d", nmsgs, maxDeliver)
				}
				return nil
			})

			// Now wait a bit longer and make sure we do not have more than maxDeliveries.
			time.Sleep(2 * ackWait)
			if nmsgs, _, _ := sub.Pending(); err != nil || nmsgs != maxDeliver {
				t.Fatalf("Did not receive correct number of messages: %d vs %d", nmsgs, maxDeliver)
			}
		})
	}
}

func TestJetStreamPullConsumerDelayedFirstPullWithReplayOriginal(t *testing.T) {
	cases := []struct {
		name    string
		mconfig *StreamConfig
	}{
		{"MemoryStore", &StreamConfig{Name: "MY_WQ", Storage: MemoryStorage}},
		{"FileStore", &StreamConfig{Name: "MY_WQ", Storage: FileStorage}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			s := RunBasicJetStreamServer()
			defer s.Shutdown()

			if config := s.JetStreamConfig(); config != nil {
				defer removeDir(t, config.StoreDir)
			}

			mset, err := s.GlobalAccount().addStream(c.mconfig)
			if err != nil {
				t.Fatalf("Unexpected error adding stream: %v", err)
			}
			defer mset.delete()

			nc := clientConnectToServer(t, s)
			defer nc.Close()

			// Queue up our work item.
			sendStreamMsg(t, nc, c.mconfig.Name, "Hello World!")

			o, err := mset.addConsumer(&ConsumerConfig{
				Durable:      "d",
				AckPolicy:    AckExplicit,
				ReplayPolicy: ReplayOriginal,
			})
			if err != nil {
				t.Fatalf("Expected no error, got %v", err)
			}
			defer o.delete()

			// Force delay here which triggers the bug.
			time.Sleep(250 * time.Millisecond)

			if _, err = nc.Request(o.requestNextMsgSubject(), nil, time.Second); err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}
		})
	}
}

func TestJetStreamConsumerAckFloorFill(t *testing.T) {
	cases := []struct {
		name    string
		mconfig *StreamConfig
	}{
		{"MemoryStore", &StreamConfig{Name: "MQ", Storage: MemoryStorage}},
		{"FileStore", &StreamConfig{Name: "MQ", Storage: FileStorage}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			s := RunBasicJetStreamServer()
			defer s.Shutdown()

			if config := s.JetStreamConfig(); config != nil {
				defer removeDir(t, config.StoreDir)
			}

			mset, err := s.GlobalAccount().addStream(c.mconfig)
			if err != nil {
				t.Fatalf("Unexpected error adding stream: %v", err)
			}
			defer mset.delete()

			nc := clientConnectToServer(t, s)
			defer nc.Close()

			for i := 1; i <= 4; i++ {
				sendStreamMsg(t, nc, c.mconfig.Name, fmt.Sprintf("msg-%d", i))
			}

			sub, _ := nc.SubscribeSync(nats.NewInbox())
			defer sub.Unsubscribe()
			nc.Flush()

			o, err := mset.addConsumer(&ConsumerConfig{
				Durable:        "d",
				DeliverSubject: sub.Subject,
				AckPolicy:      AckExplicit,
			})
			if err != nil {
				t.Fatalf("Expected no error, got %v", err)
			}
			defer o.delete()

			var first *nats.Msg

			for i := 1; i <= 3; i++ {
				m, err := sub.NextMsg(time.Second)
				if err != nil {
					t.Fatalf("Error receiving message %d: %v", i, err)
				}
				// Don't ack 1 or 4.
				if i == 1 {
					first = m
				} else if i == 2 || i == 3 {
					m.Respond(nil)
				}
			}
			nc.Flush()
			if info := o.info(); info.AckFloor.Consumer != 0 {
				t.Fatalf("Expected the ack floor to be 0, got %d", info.AckFloor.Consumer)
			}
			// Now ack first, should move ack floor to 3.
			first.Respond(nil)
			nc.Flush()
			if info := o.info(); info.AckFloor.Consumer != 3 {
				t.Fatalf("Expected the ack floor to be 3, got %d", info.AckFloor.Consumer)
			}
		})
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
			s := RunBasicJetStreamServer()
			defer s.Shutdown()

			if config := s.JetStreamConfig(); config != nil {
				defer removeDir(t, config.StoreDir)
			}

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
				if err != nil {
					t.Fatalf("Expected no error, got %v", err)
				}
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
			s := RunBasicJetStreamServer()
			defer s.Shutdown()

			if config := s.JetStreamConfig(); config != nil {
				defer removeDir(t, config.StoreDir)
			}

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
			if err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}
			if pa := getPubAckResponse(resp.Data); pa == nil || pa.Error.Description != "message size exceeds maximum allowed" {
				t.Fatalf("Expected to get an error for maximum message size, got %q", pa.Error)
			}
		})
	}
}

func TestJetStreamAddStreamCanonicalNames(t *testing.T) {
	s := RunBasicJetStreamServer()
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
	s := RunBasicJetStreamServer()
	defer s.Shutdown()

	if config := s.JetStreamConfig(); config != nil {
		defer removeDir(t, config.StoreDir)
	}

	// Client for API requests.
	nc := clientConnectToServer(t, s)
	defer nc.Close()

	expectAPIErr := func(cfg StreamConfig) {
		t.Helper()
		req, err := json.Marshal(cfg)
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		resp, _ := nc.Request(fmt.Sprintf(JSApiStreamCreateT, cfg.Name), req, time.Second)
		var scResp JSApiStreamCreateResponse
		if err := json.Unmarshal(resp.Data, &scResp); err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		e := scResp.Error
		if e == nil || e.Code != 500 || e.Description != ErrMalformedSubject.Error() {
			t.Fatalf("Did not get proper error response: %+v", e)
		}
	}

	expectAPIErr(StreamConfig{Name: "MyStream", Storage: MemoryStorage, Subjects: []string{"foo.bar."}})
	expectAPIErr(StreamConfig{Name: "MyStream", Storage: MemoryStorage, Subjects: []string{".."}})
	expectAPIErr(StreamConfig{Name: "MyStream", Storage: MemoryStorage, Subjects: []string{".*"}})
	expectAPIErr(StreamConfig{Name: "MyStream", Storage: MemoryStorage, Subjects: []string{".>"}})
}

func TestJetStreamMaxConsumers(t *testing.T) {
	s := RunBasicJetStreamServer()
	defer s.Shutdown()

	if config := s.JetStreamConfig(); config != nil {
		defer removeDir(t, config.StoreDir)
	}

	nc, js := jsClientConnect(t, s)
	defer nc.Close()

	cfg := &nats.StreamConfig{
		Name:         "MAXC",
		Storage:      nats.MemoryStorage,
		Subjects:     []string{"in.maxc.>"},
		MaxConsumers: 1,
	}
	if _, err := js.AddStream(cfg); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	si, err := js.StreamInfo("MAXC")
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if si.Config.MaxConsumers != 1 {
		t.Fatalf("Expected max of 1, got %d", si.Config.MaxConsumers)
	}
	// Make sure we get the right error.
	// This should succeed.
	if _, err := js.SubscribeSync("in.maxc.foo"); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if _, err := js.SubscribeSync("in.maxc.bar"); err == nil {
		t.Fatalf("Eexpected error but got none")
	}
}

func TestJetStreamAddStreamOverlappingSubjects(t *testing.T) {
	mconfig := &StreamConfig{
		Name:     "ok",
		Storage:  MemoryStorage,
		Subjects: []string{"foo", "bar", "baz.*", "foo.bar.baz.>"},
	}

	s := RunBasicJetStreamServer()
	defer s.Shutdown()

	if config := s.JetStreamConfig(); config != nil {
		defer removeDir(t, config.StoreDir)
	}

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
	expectErr(acc.addStream(&StreamConfig{Name: "b", Subjects: []string{">"}}))
	expectErr(acc.addStream(&StreamConfig{Name: "c", Subjects: []string{"baz.33"}}))
	expectErr(acc.addStream(&StreamConfig{Name: "d", Subjects: []string{"*.33"}}))
	expectErr(acc.addStream(&StreamConfig{Name: "e", Subjects: []string{"*.>"}}))
	expectErr(acc.addStream(&StreamConfig{Name: "f", Subjects: []string{"foo.bar", "*.bar.>"}}))
}

func TestJetStreamAddStreamOverlapWithJSAPISubjects(t *testing.T) {
	s := RunBasicJetStreamServer()
	defer s.Shutdown()

	if config := s.JetStreamConfig(); config != nil {
		defer removeDir(t, config.StoreDir)
	}

	acc := s.GlobalAccount()

	expectErr := func(_ *stream, err error) {
		t.Helper()
		if err == nil || !strings.Contains(err.Error(), "subjects overlap") {
			t.Fatalf("Expected error but got none")
		}
	}

	// Test that any overlapping subjects with our JSAPI should fail.
	expectErr(acc.addStream(&StreamConfig{Name: "a", Subjects: []string{"$JS.API.foo", "$JS.API.bar"}}))
	expectErr(acc.addStream(&StreamConfig{Name: "b", Subjects: []string{"$JS.API.>"}}))
	expectErr(acc.addStream(&StreamConfig{Name: "c", Subjects: []string{"$JS.API.*"}}))

	// Events and Advisories etc should be ok.
	if _, err := acc.addStream(&StreamConfig{Name: "a", Subjects: []string{"$JS.EVENT.>"}}); err != nil {
		t.Fatalf("Expected this to work: %v", err)
	}
}

func TestJetStreamAddStreamSameConfigOK(t *testing.T) {
	mconfig := &StreamConfig{
		Name:     "ok",
		Subjects: []string{"foo", "bar", "baz.*", "foo.bar.baz.>"},
		Storage:  MemoryStorage,
	}

	s := RunBasicJetStreamServer()
	defer s.Shutdown()

	if config := s.JetStreamConfig(); config != nil {
		defer removeDir(t, config.StoreDir)
	}

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
	resp, _ := nc.Request(subject, []byte(msg), 500*time.Millisecond)
	if resp == nil {
		t.Fatalf("No response for %q, possible timeout?", msg)
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
			s := RunBasicJetStreamServer()
			defer s.Shutdown()

			if config := s.JetStreamConfig(); config != nil {
				defer removeDir(t, config.StoreDir)
			}

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
			s := RunBasicJetStreamServer()
			defer s.Shutdown()

			if config := s.JetStreamConfig(); config != nil {
				defer removeDir(t, config.StoreDir)
			}

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
			s := RunBasicJetStreamServer()
			defer s.Shutdown()

			if config := s.JetStreamConfig(); config != nil {
				defer removeDir(t, config.StoreDir)
			}

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

func TestJetStreamCreateConsumer(t *testing.T) {
	cases := []struct {
		name    string
		mconfig *StreamConfig
	}{
		{"MemoryStore", &StreamConfig{Name: "foo", Storage: MemoryStorage, Subjects: []string{"foo", "bar"}}},
		{"FileStore", &StreamConfig{Name: "foo", Storage: FileStorage, Subjects: []string{"foo", "bar"}}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			s := RunBasicJetStreamServer()
			defer s.Shutdown()

			if config := s.JetStreamConfig(); config != nil {
				defer removeDir(t, config.StoreDir)
			}

			mset, err := s.GlobalAccount().addStream(c.mconfig)
			if err != nil {
				t.Fatalf("Unexpected error adding stream: %v", err)
			}
			defer mset.delete()

			// Check for basic errors.
			if _, err := mset.addConsumer(nil); err == nil {
				t.Fatalf("Expected an error for no config")
			}

			// No deliver subject, meaning its in pull mode, work queue mode means it is required to
			// do explicit ack.
			if _, err := mset.addConsumer(&ConsumerConfig{}); err == nil {
				t.Fatalf("Expected an error on work queue / pull mode without explicit ack mode")
			}

			// Check for delivery subject errors.

			// Literal delivery subject required.
			if _, err := mset.addConsumer(&ConsumerConfig{DeliverSubject: "foo.*"}); err == nil {
				t.Fatalf("Expected an error on bad delivery subject")
			}
			// Check for cycles
			if _, err := mset.addConsumer(&ConsumerConfig{DeliverSubject: "foo"}); err == nil {
				t.Fatalf("Expected an error on delivery subject that forms a cycle")
			}
			if _, err := mset.addConsumer(&ConsumerConfig{DeliverSubject: "bar"}); err == nil {
				t.Fatalf("Expected an error on delivery subject that forms a cycle")
			}
			if _, err := mset.addConsumer(&ConsumerConfig{DeliverSubject: "*"}); err == nil {
				t.Fatalf("Expected an error on delivery subject that forms a cycle")
			}

			// StartPosition conflicts
			now := time.Now().UTC()
			if _, err := mset.addConsumer(&ConsumerConfig{
				DeliverSubject: "A",
				OptStartSeq:    1,
				OptStartTime:   &now,
			}); err == nil {
				t.Fatalf("Expected an error on start position conflicts")
			}
			if _, err := mset.addConsumer(&ConsumerConfig{
				DeliverSubject: "A",
				OptStartTime:   &now,
			}); err == nil {
				t.Fatalf("Expected an error on start position conflicts")
			}

			// Non-Durables need to have subscription to delivery subject.
			delivery := nats.NewInbox()
			// Pull-based consumers are required to be durable since we do not know when they should
			// be cleaned up.
			if _, err := mset.addConsumer(&ConsumerConfig{AckPolicy: AckExplicit}); err == nil {
				t.Fatalf("Expected an error on pull-based that is non-durable.")
			}

			nc := clientConnectToServer(t, s)
			defer nc.Close()
			sub, _ := nc.SubscribeSync(delivery)
			defer sub.Unsubscribe()
			nc.Flush()

			o, err := mset.addConsumer(&ConsumerConfig{DeliverSubject: delivery})
			if err != nil {
				t.Fatalf("Expected no error with registered interest, got %v", err)
			}

			if err := mset.deleteConsumer(o); err != nil {
				t.Fatalf("Expected no error on delete, got %v", err)
			}

			// Now let's check that durables can be created and a duplicate call to add will be ok.
			dcfg := &ConsumerConfig{
				Durable:        "ddd",
				DeliverSubject: delivery,
				AckPolicy:      AckAll,
			}
			if _, err = mset.addConsumer(dcfg); err != nil {
				t.Fatalf("Unexpected error creating consumer: %v", err)
			}
			if _, err = mset.addConsumer(dcfg); err != nil {
				t.Fatalf("Unexpected error creating second identical consumer: %v", err)
			}
			// Not test that we can change the delivery subject if that is only thing that has not
			// changed and we are not active.
			sub.Unsubscribe()
			sub, _ = nc.SubscribeSync("d.d.d")
			nc.Flush()
			defer sub.Unsubscribe()
			dcfg.DeliverSubject = "d.d.d"
			if _, err = mset.addConsumer(dcfg); err != nil {
				t.Fatalf("Unexpected error creating third consumer with just deliver subject changed: %v", err)
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
			s := RunBasicJetStreamServer()
			defer s.Shutdown()

			if config := s.JetStreamConfig(); config != nil {
				defer removeDir(t, config.StoreDir)
			}

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
			// Check that is is the last msg we sent though.
			if mseq, _ := strconv.Atoi(string(m.Data)); mseq != 200 {
				t.Fatalf("Expected messag sequence to be 200, but got %d", mseq)
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

			o, err = mset.addConsumer(&ConsumerConfig{DeliverSubject: sub.Subject})
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
			s := RunBasicJetStreamServer()
			defer s.Shutdown()

			if config := s.JetStreamConfig(); config != nil {
				defer removeDir(t, config.StoreDir)
			}

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
			s := RunBasicJetStreamServer()
			defer s.Shutdown()

			if config := s.JetStreamConfig(); config != nil {
				defer removeDir(t, config.StoreDir)
			}

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
			cfg = &ConsumerConfig{Durable: "foo", AckPolicy: AckExplicit, MaxWaiting: -1}
			if _, err := mset.addConsumer(cfg); err == nil {
				t.Fatalf("Expected an error with MaxWaiting being negative")
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

			// So when we submit our next request this one should succeed since we do not want these to fail.
			// We should get notified that the first request is now stale and has been removed.
			if _, err := nc.Request(getSubj, nil, 10*time.Millisecond); err != nats.ErrTimeout {
				t.Fatalf("Expected timeout error, got: %v", err)
			}
			checkSubPending(1)
			m, _ := sub.NextMsg(0)
			// Make sure this is an alert that tells us our request is now stale.
			if m.Header.Get("Status") != "408" {
				t.Fatalf("Expected a 408 status code, got %q", m.Header.Get("Status"))
			}
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
			s := RunBasicJetStreamServer()
			defer s.Shutdown()

			if config := s.JetStreamConfig(); config != nil {
				defer removeDir(t, config.StoreDir)
			}

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
			s := RunBasicJetStreamServer()
			defer s.Shutdown()

			if config := s.JetStreamConfig(); config != nil {
				defer removeDir(t, config.StoreDir)
			}

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

			resp, err := nc.Request(getSubj, jreq, 50*time.Millisecond)
			if err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}
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
			req.Expires = 10 * time.Millisecond
			jreq, _ = json.Marshal(req)

			nc.PublishRequest(getSubj, reply, jreq)
			// Let it expire
			time.Sleep(20 * time.Millisecond)

			// Send a few more messages. These should not be delivered to the sub.
			sendStreamMsg(t, nc, "foo", "Hello World!")
			sendStreamMsg(t, nc, "bar", "Hello World!")
			// We will have an alert here.
			checkSubPending(1)
			m, _ := sub.NextMsg(0)
			// Make sure this is an alert that tells us our request is now stale.
			if m.Header.Get("Status") != "408" {
				t.Fatalf("Expected a 408 status code, got %q", m.Header.Get("Status"))
			}
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
			s := RunBasicJetStreamServer()
			defer s.Shutdown()

			if config := s.JetStreamConfig(); config != nil {
				defer removeDir(t, config.StoreDir)
			}

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
				if err != nil {
					t.Fatalf("Unexpected error: %v", err)
				}
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
			s := RunBasicJetStreamServer()
			defer s.Shutdown()

			if config := s.JetStreamConfig(); config != nil {
				defer removeDir(t, config.StoreDir)
			}

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
				if err != nil {
					t.Fatalf("Unexpected error: %v", err)
				}
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
			s := RunBasicJetStreamServer()
			defer s.Shutdown()

			if config := s.JetStreamConfig(); config != nil {
				defer removeDir(t, config.StoreDir)
			}

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
				orderId := rand.Intn(toSend-1) + 1
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
			s := RunBasicJetStreamServer()
			defer s.Shutdown()

			if config := s.JetStreamConfig(); config != nil {
				defer removeDir(t, config.StoreDir)
			}

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
			s := RunBasicJetStreamServer()
			defer s.Shutdown()

			if config := s.JetStreamConfig(); config != nil {
				defer removeDir(t, config.StoreDir)
			}

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
			Subjects:  []string{"MY_WORK_QUEUE.*"},
			Retention: WorkQueuePolicy},
		},
		{name: "FileStore", mconfig: &StreamConfig{
			Name:      "MWQ",
			Storage:   FileStorage,
			Subjects:  []string{"MY_WORK_QUEUE.*"},
			Retention: WorkQueuePolicy},
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			s := RunBasicJetStreamServer()
			defer s.Shutdown()

			if config := s.JetStreamConfig(); config != nil {
				defer removeDir(t, config.StoreDir)
			}

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
			if err != nil {
				t.Fatalf("Expected no error, got %v", err)
			}
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
			if err != nil {
				t.Fatalf("Expected no error, got %v", err)
			}
			defer o.delete()

			// Now creating another with separate partition should work.
			o2, err := mset.addConsumer(pConfig("MY_WORK_QUEUE.B"))
			if err != nil {
				t.Fatalf("Expected no error, got %v", err)
			}
			defer o2.delete()

			// Anything that would overlap should fail though.
			if _, err := mset.addConsumer(pConfig("MY_WORK_QUEUE.A")); err == nil {
				t.Fatalf("Expected an error on attempt for partitioned consumer for a workqueue")
			}
			if _, err := mset.addConsumer(pConfig("MY_WORK_QUEUE.A")); err == nil {
				t.Fatalf("Expected an error on attempt for partitioned consumer for a workqueue")
			}

			o3, err := mset.addConsumer(pConfig("MY_WORK_QUEUE.C"))
			if err != nil {
				t.Fatalf("Expected no error, got %v", err)
			}

			o.delete()
			o2.delete()
			o3.delete()

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
			s := RunBasicJetStreamServer()
			defer s.Shutdown()

			if config := s.JetStreamConfig(); config != nil {
				defer removeDir(t, config.StoreDir)
			}

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
		MaxAge:    250 * time.Millisecond,
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
			s := RunBasicJetStreamServer()
			defer s.Shutdown()

			if config := s.JetStreamConfig(); config != nil {
				defer removeDir(t, config.StoreDir)
			}

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
			if err != nil {
				t.Fatalf("Expected no error, got %v", err)
			}
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
				_, _, _, _, pending := replyInfo(m.Reply)
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
			checkFor(t, time.Second, 10*time.Millisecond, func() error {
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
			if err != nil {
				t.Fatalf("Expected no error, got %v", err)
			}
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
			s := RunBasicJetStreamServer()
			defer s.Shutdown()

			if config := s.JetStreamConfig(); config != nil {
				defer removeDir(t, config.StoreDir)
			}

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
			if err != nil {
				t.Fatalf("Expected no error, got %v", err)
			}
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
			s := RunBasicJetStreamServer()
			defer s.Shutdown()

			if config := s.JetStreamConfig(); config != nil {
				defer removeDir(t, config.StoreDir)
			}

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
			if err != nil {
				t.Fatalf("Expected no error, got %v", err)
			}
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
				sseq, dseq, dcount, _, _ := replyInfo(m.Reply)
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
				m.Respond(nil)
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
			s := RunBasicJetStreamServer()
			defer s.Shutdown()

			if config := s.JetStreamConfig(); config != nil {
				defer removeDir(t, config.StoreDir)
			}

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
			if err != nil {
				t.Fatalf("Expected no error, got %v", err)
			}
			defer o.delete()

			getMsg := func(sseq, dseq int) *nats.Msg {
				t.Helper()
				m, err := nc.Request(o.requestNextMsgSubject(), nil, time.Second)
				if err != nil {
					t.Fatalf("Unexpected error: %v", err)
				}
				rsseq, rdseq, _, _, _ := replyInfo(m.Reply)
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
			// NAK this one.
			m.Respond(AckNak)

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
			s := RunBasicJetStreamServer()
			defer s.Shutdown()

			if config := s.JetStreamConfig(); config != nil {
				defer removeDir(t, config.StoreDir)
			}

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
			if err != nil {
				t.Fatalf("Expected no error, got %v", err)
			}
			defer o.delete()

			getMsg := func(sseq, dseq int) *nats.Msg {
				t.Helper()
				m, err := nc.Request(o.requestNextMsgSubject(), nil, time.Second)
				if err != nil {
					t.Fatalf("Unexpected error: %v", err)
				}
				rsseq, rdseq, _, _, _ := replyInfo(m.Reply)
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
			s := RunBasicJetStreamServer()
			defer s.Shutdown()

			if config := s.JetStreamConfig(); config != nil {
				defer removeDir(t, config.StoreDir)
			}

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
			if err != nil {
				t.Fatalf("Expected no error, got %v", err)
			}
			defer o.delete()

			getMsg := func(sseq, dseq int) *nats.Msg {
				t.Helper()
				m, err := nc.Request(o.requestNextMsgSubject(), nil, time.Second)
				if err != nil {
					t.Fatalf("Unexpected error: %v", err)
				}
				rsseq, rdseq, _, _, _ := replyInfo(m.Reply)
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
			m.Respond(AckTerm)
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
		})
	}
}

func TestJetStreamConsumerAckAck(t *testing.T) {
	s := RunBasicJetStreamServer()
	defer s.Shutdown()

	if config := s.JetStreamConfig(); config != nil {
		defer removeDir(t, config.StoreDir)
	}

	if config := s.JetStreamConfig(); config != nil {
		defer removeDir(t, config.StoreDir)
	}

	mname := "ACK-ACK"
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
	rqn := o.requestNextMsgSubject()

	nc := clientConnectToServer(t, s)
	defer nc.Close()

	// 4 for number of ack protocols to test them all.
	for i := 0; i < 4; i++ {
		sendStreamMsg(t, nc, mname, "Hello World!")
	}

	testAck := func(ackType []byte) {
		m, err := nc.Request(rqn, nil, 10*time.Millisecond)
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		// Send a request for the ack and make sure the server "ack's" the ack.
		if _, err := nc.Request(m.Reply, ackType, 10*time.Millisecond); err != nil {
			t.Fatalf("Unexpected error on ack/ack: %v", err)
		}
	}

	testAck(AckAck)
	testAck(AckNak)
	testAck(AckProgress)
	testAck(AckTerm)
}

func TestJetStreamAckNext(t *testing.T) {
	s := RunBasicJetStreamServer()
	defer s.Shutdown()

	if config := s.JetStreamConfig(); config != nil {
		defer removeDir(t, config.StoreDir)
	}

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
	s := RunBasicJetStreamServer()
	defer s.Shutdown()

	if config := s.JetStreamConfig(); config != nil {
		defer removeDir(t, config.StoreDir)
	}

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
	cfg.Duplicates = 25 * time.Millisecond
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

	// Purge should wipe the msgIds as well.
	mset.purge(nil)
	nmids(0)
}

func getPubAckResponse(msg []byte) *JSPubAckResponse {
	var par JSPubAckResponse
	if err := json.Unmarshal(msg, &par); err != nil {
		return nil
	}
	return &par
}

func TestJetStreamPublishExpect(t *testing.T) {
	s := RunBasicJetStreamServer()
	defer s.Shutdown()

	if config := s.JetStreamConfig(); config != nil {
		defer removeDir(t, config.StoreDir)
	}

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
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if pa := getPubAckResponse(resp.Data); pa == nil || pa.Error != nil {
		t.Fatalf("Expected a valid JetStreamPubAck, got %q", resp.Data)
	}

	// Now test that we get an error back when expecting a different stream.
	m.Header.Set(JSExpectedStream, "ORDERS")
	resp, err = nc.RequestMsg(m, 100*time.Millisecond)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if pa := getPubAckResponse(resp.Data); pa == nil || pa.Error == nil {
		t.Fatalf("Expected an error, got %q", resp.Data)
	}

	// Now test that we get an error back when expecting a different sequence number.
	m.Header.Set(JSExpectedStream, mname)
	m.Header.Set(JSExpectedLastSeq, "10")
	resp, err = nc.RequestMsg(m, 100*time.Millisecond)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
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
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if pa := getPubAckResponse(resp.Data); pa == nil || pa.Error == nil {
		t.Fatalf("Expected an error, got %q", resp.Data)
	}

	// Check with JS Headers in different cases.
	m = nats.NewMsg("foo.bar")
	m.Data = []byte("HI")
	m.Header[JSExpectedLastSeq] = []string{"5"}
	resp, err = nc.RequestMsg(m, 100*time.Millisecond)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if pa := getPubAckResponse(resp.Data); pa == nil || pa.Error == nil {
		t.Fatalf("Expected an error, got %q", resp.Data)
	}

	// Try with JS Header in lower case.
	// nats-expected-last-sequence: 5
	m = nats.NewMsg("foo.bar")
	m.Header[strings.ToLower(JSExpectedLastSeq)] = []string{"5"}
	resp, err = nc.RequestMsg(m, 100*time.Millisecond)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
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
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if pa := getPubAckResponse(resp.Data); pa == nil || pa.Error != nil {
		t.Fatalf("Expected a valid JetStreamPubAck, got %q", resp.Data)
	}
}

func TestJetStreamPullConsumerRemoveInterest(t *testing.T) {
	s := RunBasicJetStreamServer()
	defer s.Shutdown()

	if config := s.JetStreamConfig(); config != nil {
		defer removeDir(t, config.StoreDir)
	}

	mname := "MYS-PULL"
	mset, err := s.GlobalAccount().addStream(&StreamConfig{Name: mname, Storage: MemoryStorage})
	if err != nil {
		t.Fatalf("Unexpected error adding stream: %v", err)
	}
	defer mset.delete()

	wcfg := &ConsumerConfig{Durable: "worker", AckPolicy: AckExplicit}
	o, err := mset.addConsumer(wcfg)
	if err != nil {
		t.Fatalf("Expected no error with registered interest, got %v", err)
	}
	rqn := o.requestNextMsgSubject()
	defer o.delete()

	nc := clientConnectToServer(t, s)
	defer nc.Close()

	// Ask for a message even though one is not there. This will queue us up for waiting.
	if _, err := nc.Request(rqn, nil, 10*time.Millisecond); err == nil {
		t.Fatalf("Expected an error, got none")
	}

	// This is using new style request mechanism. so drop the connection itself to get rid of interest.
	nc.Close()

	// Wait for client cleanup
	checkFor(t, 200*time.Millisecond, 10*time.Millisecond, func() error {
		if n := s.NumClients(); err != nil || n != 0 {
			return fmt.Errorf("Still have %d clients", n)
		}
		return nil
	})

	nc = clientConnectToServer(t, s)
	defer nc.Close()
	// Send a message
	sendStreamMsg(t, nc, mname, "Hello World!")

	msg, err := nc.Request(rqn, nil, time.Second)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	_, dseq, dc, _, _ := replyInfo(msg.Reply)
	if dseq != 1 {
		t.Fatalf("Expected consumer sequence of 1, got %d", dseq)
	}
	if dc != 1 {
		t.Fatalf("Expected delivery count of 1, got %d", dc)
	}

	// Now do old school request style and more than one waiting.
	nc = clientConnectWithOldRequest(t, s)
	defer nc.Close()

	// Now queue up 10 waiting via failed requests.
	for i := 0; i < 10; i++ {
		if _, err := nc.Request(rqn, nil, 1*time.Millisecond); err == nil {
			t.Fatalf("Expected an error, got none")
		}
	}

	// Send a second message
	sendStreamMsg(t, nc, mname, "Hello World!")

	msg, err = nc.Request(rqn, nil, time.Second)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	_, dseq, dc, _, _ = replyInfo(msg.Reply)
	if dseq != 2 {
		t.Fatalf("Expected consumer sequence of 2, got %d", dseq)
	}
	if dc != 1 {
		t.Fatalf("Expected delivery count of 1, got %d", dc)
	}
}

func TestJetStreamConsumerRateLimit(t *testing.T) {
	s := RunBasicJetStreamServer()
	defer s.Shutdown()

	if config := s.JetStreamConfig(); config != nil {
		defer removeDir(t, config.StoreDir)
	}

	mname := "RATELIMIT"
	mset, err := s.GlobalAccount().addStream(&StreamConfig{Name: mname, Storage: FileStorage})
	if err != nil {
		t.Fatalf("Unexpected error adding stream: %v", err)
	}

	nc := clientConnectToServer(t, s)
	defer nc.Close()

	msgSize := 128 * 1024
	msg := make([]byte, msgSize)
	rand.Read(msg)

	// 10MB
	totalSize := 10 * 1024 * 1024
	toSend := totalSize / msgSize
	for i := 0; i < toSend; i++ {
		nc.Publish(mname, msg)
	}
	nc.Flush()
	state := mset.state()
	if state.Msgs != uint64(toSend) {
		t.Fatalf("Expected %d messages, got %d", toSend, state.Msgs)
	}

	// 100Mbit
	rateLimit := uint64(100 * 1024 * 1024)
	// Make sure if you set a rate with a pull based consumer it errors.
	_, err = mset.addConsumer(&ConsumerConfig{Durable: "to", AckPolicy: AckExplicit, RateLimit: rateLimit})
	if err == nil {
		t.Fatalf("Expected an error, got none")
	}

	// Now create one and measure the rate delivered.
	o, err := mset.addConsumer(&ConsumerConfig{
		Durable:        "rate",
		DeliverSubject: "to",
		RateLimit:      rateLimit,
		AckPolicy:      AckNone})
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	defer o.delete()

	var received int
	done := make(chan bool)

	start := time.Now()

	nc.Subscribe("to", func(m *nats.Msg) {
		received++
		if received >= toSend {
			done <- true
		}
	})
	nc.Flush()

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatalf("Did not receive all the messages in time")
	}

	tt := time.Since(start)
	rate := float64(8*toSend*msgSize) / tt.Seconds()
	if rate > float64(rateLimit)*1.25 {
		t.Fatalf("Exceeded desired rate of %d mbps, got %0.f mbps", rateLimit/(1024*1024), rate/(1024*1024))
	}
}

func TestJetStreamEphemeralConsumerRecoveryAfterServerRestart(t *testing.T) {
	s := RunBasicJetStreamServer()
	defer s.Shutdown()

	if config := s.JetStreamConfig(); config != nil {
		defer removeDir(t, config.StoreDir)
	}

	mname := "MYS"
	mset, err := s.GlobalAccount().addStream(&StreamConfig{Name: mname, Storage: FileStorage})
	if err != nil {
		t.Fatalf("Unexpected error adding stream: %v", err)
	}
	defer mset.delete()

	nc := clientConnectToServer(t, s)
	defer nc.Close()

	sub, _ := nc.SubscribeSync(nats.NewInbox())
	defer sub.Unsubscribe()
	nc.Flush()

	o, err := mset.addConsumer(&ConsumerConfig{
		DeliverSubject: sub.Subject,
		AckPolicy:      AckExplicit,
	})
	if err != nil {
		t.Fatalf("Error creating consumer: %v", err)
	}
	defer o.delete()

	// Snapshot our name.
	oname := o.String()

	// Send 100 messages
	for i := 0; i < 100; i++ {
		sendStreamMsg(t, nc, mname, "Hello World!")
	}
	if state := mset.state(); state.Msgs != 100 {
		t.Fatalf("Expected %d messages, got %d", 100, state.Msgs)
	}

	// Read 6 messages
	for i := 0; i <= 6; i++ {
		if m, err := sub.NextMsg(time.Second); err == nil {
			m.Respond(nil)
		} else {
			t.Fatalf("Unexpected error: %v", err)
		}
	}

	// Capture port since it was dynamic.
	u, _ := url.Parse(s.ClientURL())
	port, _ := strconv.Atoi(u.Port())

	restartServer := func() {
		t.Helper()
		// Stop current
		sd := s.JetStreamConfig().StoreDir
		s.Shutdown()
		// Restart.
		s = RunJetStreamServerOnPort(port, sd)
	}

	// Do twice
	for i := 0; i < 2; i++ {
		// Restart.
		restartServer()
		defer s.Shutdown()

		mset, err = s.GlobalAccount().lookupStream(mname)
		if err != nil {
			t.Fatalf("Expected to find a stream for %q", mname)
		}
		o = mset.lookupConsumer(oname)
		if o == nil {
			t.Fatalf("Error looking up consumer %q", oname)
		}
		// Make sure config does not have durable.
		if cfg := o.config(); cfg.Durable != "" {
			t.Fatalf("Expected no durable to be set")
		}
		// Wait for it to become active
		checkFor(t, 200*time.Millisecond, 10*time.Millisecond, func() error {
			if !o.isActive() {
				return fmt.Errorf("Consumer not active")
			}
			return nil
		})
	}

	// Now close the connection. Make sure this acts like an ephemeral and goes away.
	o.setInActiveDeleteThreshold(10 * time.Millisecond)
	nc.Close()

	checkFor(t, 200*time.Millisecond, 10*time.Millisecond, func() error {
		if o := mset.lookupConsumer(oname); o != nil {
			return fmt.Errorf("Consumer still active")
		}
		return nil
	})
}

func TestJetStreamConsumerMaxDeliveryAndServerRestart(t *testing.T) {
	s := RunBasicJetStreamServer()
	defer s.Shutdown()

	if config := s.JetStreamConfig(); config != nil {
		defer removeDir(t, config.StoreDir)
	}

	mname := "MYS"
	mset, err := s.GlobalAccount().addStream(&StreamConfig{Name: mname, Storage: FileStorage})
	if err != nil {
		t.Fatalf("Unexpected error adding stream: %v", err)
	}
	defer mset.delete()

	streamCreated := mset.createdTime()

	dsubj := "D.TO"
	max := 3

	o, err := mset.addConsumer(&ConsumerConfig{
		Durable:        "TO",
		DeliverSubject: dsubj,
		AckPolicy:      AckExplicit,
		AckWait:        100 * time.Millisecond,
		MaxDeliver:     max,
	})
	defer o.delete()

	consumerCreated := o.createdTime()
	// For calculation of consumer created times below.
	time.Sleep(5 * time.Millisecond)

	nc := clientConnectToServer(t, s)
	defer nc.Close()

	sub, _ := nc.SubscribeSync(dsubj)
	nc.Flush()
	defer sub.Unsubscribe()

	// Send one message.
	sendStreamMsg(t, nc, mname, "order-1")

	checkSubPending := func(numExpected int) {
		t.Helper()
		checkFor(t, time.Second, 10*time.Millisecond, func() error {
			if nmsgs, _, _ := sub.Pending(); nmsgs != numExpected {
				return fmt.Errorf("Did not receive correct number of messages: %d vs %d", nmsgs, numExpected)
			}
			return nil
		})
	}

	checkNumMsgs := func(numExpected uint64) {
		t.Helper()
		mset, err = s.GlobalAccount().lookupStream(mname)
		if err != nil {
			t.Fatalf("Expected to find a stream for %q", mname)
		}
		state := mset.state()
		if state.Msgs != numExpected {
			t.Fatalf("Expected %d msgs, got %d", numExpected, state.Msgs)
		}
	}

	// Wait til we know we have max queued up.
	checkSubPending(max)

	// Once here we have gone over the limit for the 1st message for max deliveries.
	// Send second
	sendStreamMsg(t, nc, mname, "order-2")

	// Just wait for first delivery + one redelivery.
	checkSubPending(max + 2)

	// Capture port since it was dynamic.
	u, _ := url.Parse(s.ClientURL())
	port, _ := strconv.Atoi(u.Port())

	restartServer := func() {
		t.Helper()
		sd := s.JetStreamConfig().StoreDir
		// Stop current
		s.Shutdown()
		// Restart.
		s = RunJetStreamServerOnPort(port, sd)
	}

	waitForClientReconnect := func() {
		checkFor(t, 2500*time.Millisecond, 5*time.Millisecond, func() error {
			if !nc.IsConnected() {
				return fmt.Errorf("Not connected")
			}
			return nil
		})
	}

	// Restart.
	restartServer()
	defer s.Shutdown()

	checkNumMsgs(2)

	// Wait for client to be reconnected.
	waitForClientReconnect()

	// Once we are here send third order.
	sendStreamMsg(t, nc, mname, "order-3")
	checkNumMsgs(3)

	// Restart.
	restartServer()
	defer s.Shutdown()

	checkNumMsgs(3)

	// Wait for client to be reconnected.
	waitForClientReconnect()

	// Now we should have max times three on our sub.
	checkSubPending(max * 3)

	// Now do some checks on created timestamps.
	mset, err = s.GlobalAccount().lookupStream(mname)
	if err != nil {
		t.Fatalf("Expected to find a stream for %q", mname)
	}
	if mset.createdTime() != streamCreated {
		t.Fatalf("Stream creation time not restored, wanted %v, got %v", streamCreated, mset.createdTime())
	}
	o = mset.lookupConsumer("TO")
	if o == nil {
		t.Fatalf("Error looking up consumer: %v", err)
	}
	// Consumer created times can have a very small skew.
	delta := o.createdTime().Sub(consumerCreated)
	if delta > 5*time.Millisecond {
		t.Fatalf("Consumer creation time not restored, wanted %v, got %v", consumerCreated, o.createdTime())
	}
}

func TestJetStreamDeleteConsumerAndServerRestart(t *testing.T) {
	s := RunBasicJetStreamServer()
	defer s.Shutdown()

	if config := s.JetStreamConfig(); config != nil {
		defer removeDir(t, config.StoreDir)
	}

	sendSubj := "MYQ"
	mset, err := s.GlobalAccount().addStream(&StreamConfig{Name: sendSubj, Storage: FileStorage})
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

	// Now delete and then we will restart the
	o.delete()

	if numo := mset.numConsumers(); numo != 0 {
		t.Fatalf("Expected to have zero consumers, got %d", numo)
	}

	// Capture port since it was dynamic.
	u, _ := url.Parse(s.ClientURL())
	port, _ := strconv.Atoi(u.Port())
	sd := s.JetStreamConfig().StoreDir

	// Stop current
	s.Shutdown()

	// Restart.
	s = RunJetStreamServerOnPort(port, sd)
	defer s.Shutdown()

	mset, err = s.GlobalAccount().lookupStream(sendSubj)
	if err != nil {
		t.Fatalf("Expected to find a stream for %q", sendSubj)
	}

	if numo := mset.numConsumers(); numo != 0 {
		t.Fatalf("Expected to have zero consumers, got %d", numo)
	}
}

func TestJetStreamRedeliveryAfterServerRestart(t *testing.T) {
	s := RunBasicJetStreamServer()
	defer s.Shutdown()

	if config := s.JetStreamConfig(); config != nil {
		defer removeDir(t, config.StoreDir)
	}

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
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
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

func TestJetStreamSnapshots(t *testing.T) {
	s := RunBasicJetStreamServer()
	defer s.Shutdown()

	if config := s.JetStreamConfig(); config != nil {
		defer removeDir(t, config.StoreDir)
	}

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
	toSend := rand.Intn(200) + 22
	for i := 1; i <= toSend; i++ {
		msg := fmt.Sprintf("Hello World %d", i)
		subj := subjects[rand.Intn(len(subjects))]
		sendStreamMsg(t, nc, subj, msg)
	}

	// Create up to 10 consumers.
	numConsumers := rand.Intn(10) + 1
	var obs []obsi
	for i := 1; i <= numConsumers; i++ {
		cname := fmt.Sprintf("WQ-%d", i)
		o, err := mset.addConsumer(workerModeConfig(cname))
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		// Now grab some messages.
		toReceive := rand.Intn(toSend/2) + 1
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
	info := info{mset.config(), mset.state(), obs}

	sr, err := mset.snapshot(5*time.Second, false, true)
	if err != nil {
		t.Fatalf("Error getting snapshot: %v", err)
	}
	zr := sr.Reader
	snapshot, err := ioutil.ReadAll(zr)
	if err != nil {
		t.Fatalf("Error reading snapshot")
	}
	// Try to restore from snapshot with current stream present, should error.
	r := bytes.NewReader(snapshot)
	if _, err := acc.RestoreStream(&info.cfg, r); err == nil {
		t.Fatalf("Expected an error trying to restore existing stream")
	} else if !strings.Contains(err.Error(), "name already in use") {
		t.Fatalf("Incorrect error received: %v", err)
	}
	// Now delete so we can restore.
	pusage := acc.JetStreamUsage()
	mset.delete()
	r.Reset(snapshot)

	mset, err = acc.RestoreStream(&info.cfg, r)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	// Now compare to make sure they are equal.
	if nusage := acc.JetStreamUsage(); nusage != pusage {
		t.Fatalf("Usage does not match after restore: %+v vs %+v", nusage, pusage)
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
				t.Fatalf("[%v] Consumer next seq is not correct: %d vs %d", o.String(), oi.ack+1, o.nextSeq())
			}
		} else {
			t.Fatalf("Expected to get an consumer")
		}
	}

	// Now try restoring to a different
	s2 := RunBasicJetStreamServer()
	defer s2.Shutdown()

	if config := s2.JetStreamConfig(); config != nil && config.StoreDir != "" {
		defer removeDir(t, config.StoreDir)
	}
	acc = s2.GlobalAccount()
	r.Reset(snapshot)
	mset, err = acc.RestoreStream(&info.cfg, r)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	o := mset.lookupConsumer("WQ-1")
	if o == nil {
		t.Fatalf("Could not lookup consumer")
	}

	nc2 := clientConnectToServer(t, s2)
	defer nc2.Close()

	// Make sure we can read messages.
	if _, err := nc2.Request(o.requestNextMsgSubject(), nil, 5*time.Second); err != nil {
		t.Fatalf("Unexpected error getting next message: %v", err)
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
	tdir := createDir(t, "jstests-storedir-")
	opts.JetStream = true
	opts.StoreDir = tdir
	rurl, _ := url.Parse(fmt.Sprintf("nats-leaf://%s:%d", lopts.LeafNode.Host, lopts.LeafNode.Port))
	opts.LeafNode.Remotes = []*RemoteLeafOpts{{URLs: []*url.URL{rurl}}}

	s := RunServer(&opts)
	defer s.Shutdown()

	checkLeafNodeConnected(t, s)

	if config := s.JetStreamConfig(); config != nil {
		defer removeDir(t, config.StoreDir)
	}

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

	toSend := rand.Intn(100) + 1
	for i := 1; i <= toSend; i++ {
		msg := fmt.Sprintf("Hello World %d", i)
		subj := subjects[rand.Intn(len(subjects))]
		sendStreamMsg(t, nc, subj, msg)
	}

	o, err := mset.addConsumer(workerModeConfig("WQ"))
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	// Now grab some messages.
	toReceive := rand.Intn(toSend) + 1
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
	if !IsNatsErr(rresp.Error, JSStreamNameExistErr) {
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
	nc2 := clientConnectToServer(t, ls)
	defer nc2.Close()
	// Wait a bit for interest to propagate.
	time.Sleep(100 * time.Millisecond)

	snapshot = snapshot[:0]

	req, _ = json.Marshal(sreq)
	rmsg, err = nc2.Request(fmt.Sprintf(JSApiStreamSnapshotT, mname), req, time.Second)
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

	rmsg, err = nc2.Request(fmt.Sprintf(JSApiStreamRestoreT, mname), req, time.Second)
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

	rmsg, err = nc2.Request(fmt.Sprintf(JSApiStreamRestoreT, mname), req, time.Second)
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
		t.Fatalf("Got an unexpected error from EOF omn restore: %+v", scResp.Error)
	}

	if !reflect.DeepEqual(scResp.StreamInfo.State, state) {
		t.Fatalf("Did not match states, %+v vs %+v", scResp.StreamInfo.State, state)
	}
}

func TestJetStreamPubAckPerf(t *testing.T) {
	// Comment out to run, holding place for now.
	t.SkipNow()

	s := RunBasicJetStreamServer()
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

func TestJetStreamSnapshotsAPIPerf(t *testing.T) {
	// Comment out to run, holding place for now.
	t.SkipNow()

	s := RunBasicJetStreamServer()
	defer s.Shutdown()

	if config := s.JetStreamConfig(); config != nil {
		defer removeDir(t, config.StoreDir)
	}

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
	rand.Read(msg)

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
			s := RunBasicJetStreamServer()
			defer s.Shutdown()

			if config := s.JetStreamConfig(); config != nil && config.StoreDir != "" {
				defer removeDir(t, config.StoreDir)
			}

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
			s := RunBasicJetStreamServer()
			defer s.Shutdown()

			if config := s.JetStreamConfig(); config != nil {
				defer removeDir(t, config.StoreDir)
			}

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

func TestJetStreamConsumerReconnect(t *testing.T) {
	cases := []struct {
		name    string
		mconfig *StreamConfig
	}{
		{"MemoryStore", &StreamConfig{Name: "ET", Storage: MemoryStorage, Subjects: []string{"foo.*"}}},
		{"FileStore", &StreamConfig{Name: "ET", Storage: FileStorage, Subjects: []string{"foo.*"}}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			s := RunBasicJetStreamServer()
			defer s.Shutdown()

			if config := s.JetStreamConfig(); config != nil {
				defer removeDir(t, config.StoreDir)
			}

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

			// Capture the subscription.
			delivery := sub.Subject

			o, err := mset.addConsumer(&ConsumerConfig{DeliverSubject: delivery, AckPolicy: AckExplicit})
			if err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}
			if !o.isActive() {
				t.Fatalf("Expected the consumer to be considered active")
			}
			if numo := mset.numConsumers(); numo != 1 {
				t.Fatalf("Expected number of consumers to be 1, got %d", numo)
			}

			// We will simulate reconnect by unsubscribing on one connection and forming
			// the same on another. Once we have cluster tests we will do more testing on
			// reconnect scenarios.
			getMsg := func(seqno int) *nats.Msg {
				t.Helper()
				m, err := sub.NextMsg(time.Second)
				if err != nil {
					t.Fatalf("Unexpected error for %d: %v", seqno, err)
				}
				if seq := o.seqFromReply(m.Reply); seq != uint64(seqno) {
					t.Fatalf("Expected sequence of %d , got %d", seqno, seq)
				}
				m.Respond(nil)
				return m
			}

			sendMsg := func() {
				t.Helper()
				if err := nc.Publish("foo.22", []byte("OK!")); err != nil {
					return
				}
			}

			checkForInActive := func() {
				checkFor(t, 250*time.Millisecond, 50*time.Millisecond, func() error {
					if o.isActive() {
						return fmt.Errorf("Consumer is still active")
					}
					return nil
				})
			}

			// Send and Pull first message.
			sendMsg() // 1
			getMsg(1)
			// Cancel first one.
			sub.Unsubscribe()
			// Re-establish new sub on same subject.
			sub, _ = nc.SubscribeSync(delivery)
			nc.Flush()

			// We should be getting 2 here.
			sendMsg() // 2
			getMsg(2)

			sub.Unsubscribe()
			checkForInActive()

			// send 3-10
			for i := 0; i <= 7; i++ {
				sendMsg()
			}
			// Make sure they are all queued up with no interest.
			nc.Flush()

			// Restablish again.
			sub, _ = nc.SubscribeSync(delivery)
			nc.Flush()

			// We should be getting 3-10 here.
			for i := 3; i <= 10; i++ {
				getMsg(i)
			}
		})
	}
}

func TestJetStreamDurableConsumerReconnect(t *testing.T) {
	cases := []struct {
		name    string
		mconfig *StreamConfig
	}{
		{"MemoryStore", &StreamConfig{Name: "DT", Storage: MemoryStorage, Subjects: []string{"foo.*"}}},
		{"FileStore", &StreamConfig{Name: "DT", Storage: FileStorage, Subjects: []string{"foo.*"}}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			s := RunBasicJetStreamServer()
			defer s.Shutdown()

			if config := s.JetStreamConfig(); config != nil {
				defer removeDir(t, config.StoreDir)
			}

			mset, err := s.GlobalAccount().addStream(c.mconfig)
			if err != nil {
				t.Fatalf("Unexpected error adding stream: %v", err)
			}
			defer mset.delete()

			nc := clientConnectToServer(t, s)
			defer nc.Close()

			dname := "d22"
			subj1 := nats.NewInbox()

			o, err := mset.addConsumer(&ConsumerConfig{
				Durable:        dname,
				DeliverSubject: subj1,
				AckPolicy:      AckExplicit,
				AckWait:        50 * time.Millisecond})
			if err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}
			sendMsg := func() {
				t.Helper()
				if err := nc.Publish("foo.22", []byte("OK!")); err != nil {
					return
				}
			}

			// Send 10 msgs
			toSend := 10
			for i := 0; i < toSend; i++ {
				sendMsg()
			}

			sub, _ := nc.SubscribeSync(subj1)
			defer sub.Unsubscribe()

			checkFor(t, 500*time.Millisecond, 10*time.Millisecond, func() error {
				if nmsgs, _, _ := sub.Pending(); err != nil || nmsgs != toSend {
					return fmt.Errorf("Did not receive correct number of messages: %d vs %d", nmsgs, toSend)
				}
				return nil
			})

			getMsg := func(seqno int) *nats.Msg {
				t.Helper()
				m, err := sub.NextMsg(time.Second)
				if err != nil {
					t.Fatalf("Unexpected error: %v", err)
				}
				if seq := o.streamSeqFromReply(m.Reply); seq != uint64(seqno) {
					t.Fatalf("Expected sequence of %d , got %d", seqno, seq)
				}
				m.Respond(nil)
				return m
			}

			// Ack first half
			for i := 1; i <= toSend/2; i++ {
				m := getMsg(i)
				m.Respond(nil)
			}

			// Now unsubscribe and wait to become inactive
			sub.Unsubscribe()
			checkFor(t, 250*time.Millisecond, 50*time.Millisecond, func() error {
				if o.isActive() {
					return fmt.Errorf("Consumer is still active")
				}
				return nil
			})

			// Now we should be able to replace the delivery subject.
			subj2 := nats.NewInbox()
			sub, _ = nc.SubscribeSync(subj2)
			defer sub.Unsubscribe()
			nc.Flush()

			o, err = mset.addConsumer(&ConsumerConfig{
				Durable:        dname,
				DeliverSubject: subj2,
				AckPolicy:      AckExplicit,
				AckWait:        50 * time.Millisecond})
			if err != nil {
				t.Fatalf("Unexpected error trying to add a new durable consumer: %v", err)
			}

			// We should get the remaining messages here.
			for i := toSend/2 + 1; i <= toSend; i++ {
				m := getMsg(i)
				m.Respond(nil)
			}
		})
	}
}

func TestJetStreamDurableConsumerReconnectWithOnlyPending(t *testing.T) {
	cases := []struct {
		name    string
		mconfig *StreamConfig
	}{
		{"MemoryStore", &StreamConfig{Name: "DT", Storage: MemoryStorage, Subjects: []string{"foo.*"}}},
		{"FileStore", &StreamConfig{Name: "DT", Storage: FileStorage, Subjects: []string{"foo.*"}}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			s := RunBasicJetStreamServer()
			defer s.Shutdown()

			if config := s.JetStreamConfig(); config != nil {
				defer removeDir(t, config.StoreDir)
			}

			mset, err := s.GlobalAccount().addStream(c.mconfig)
			if err != nil {
				t.Fatalf("Unexpected error adding stream: %v", err)
			}
			defer mset.delete()

			nc := clientConnectToServer(t, s)
			defer nc.Close()

			dname := "d22"
			subj1 := nats.NewInbox()

			o, err := mset.addConsumer(&ConsumerConfig{
				Durable:        dname,
				DeliverSubject: subj1,
				AckPolicy:      AckExplicit,
				AckWait:        25 * time.Millisecond})
			if err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}

			sendMsg := func(payload string) {
				t.Helper()
				if err := nc.Publish("foo.22", []byte(payload)); err != nil {
					return
				}
			}

			sendMsg("1")

			sub, _ := nc.SubscribeSync(subj1)
			defer sub.Unsubscribe()

			checkFor(t, 500*time.Millisecond, 10*time.Millisecond, func() error {
				if nmsgs, _, _ := sub.Pending(); err != nil || nmsgs != 1 {
					return fmt.Errorf("Did not receive correct number of messages: %d vs %d", nmsgs, 1)
				}
				return nil
			})

			// Now unsubscribe and wait to become inactive
			sub.Unsubscribe()
			checkFor(t, 250*time.Millisecond, 50*time.Millisecond, func() error {
				if o.isActive() {
					return fmt.Errorf("Consumer is still active")
				}
				return nil
			})

			// Send the second message while delivery subscriber is not running
			sendMsg("2")

			// Now we should be able to replace the delivery subject.
			subj2 := nats.NewInbox()
			o, err = mset.addConsumer(&ConsumerConfig{
				Durable:        dname,
				DeliverSubject: subj2,
				AckPolicy:      AckExplicit,
				AckWait:        25 * time.Millisecond})
			if err != nil {
				t.Fatalf("Unexpected error trying to add a new durable consumer: %v", err)
			}
			sub, _ = nc.SubscribeSync(subj2)
			defer sub.Unsubscribe()
			nc.Flush()

			// We should get msg "1" and "2" delivered. They will be reversed.
			for i := 0; i < 2; i++ {
				msg, err := sub.NextMsg(500 * time.Millisecond)
				if err != nil {
					t.Fatalf("Unexpected error: %v", err)
				}
				sseq, _, dc, _, _ := replyInfo(msg.Reply)
				if sseq == 1 && dc == 1 {
					t.Fatalf("Expected a redelivery count greater then 1 for sseq 1, got %d", dc)
				}
				if sseq != 1 && sseq != 2 {
					t.Fatalf("Expected stream sequence of 1 or 2 but got %d", sseq)
				}
			}
		})
	}
}

func TestJetStreamDurableFilteredSubjectConsumerReconnect(t *testing.T) {
	cases := []struct {
		name    string
		mconfig *StreamConfig
	}{
		{"MemoryStore", &StreamConfig{Name: "DT", Storage: MemoryStorage, Subjects: []string{"foo.*"}}},
		{"FileStore", &StreamConfig{Name: "DT", Storage: FileStorage, Subjects: []string{"foo.*"}}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			s := RunBasicJetStreamServer()
			defer s.Shutdown()

			if config := s.JetStreamConfig(); config != nil {
				defer removeDir(t, config.StoreDir)
			}

			mset, err := s.GlobalAccount().addStream(c.mconfig)
			if err != nil {
				t.Fatalf("Unexpected error adding stream: %v", err)
			}
			defer mset.delete()

			nc := clientConnectToServer(t, s)
			defer nc.Close()

			sendMsgs := func(toSend int) {
				for i := 0; i < toSend; i++ {
					var subj string
					if i%2 == 0 {
						subj = "foo.AA"
					} else {
						subj = "foo.ZZ"
					}
					if err := nc.Publish(subj, []byte("OK!")); err != nil {
						return
					}
				}
				nc.Flush()
			}

			// Send 50 msgs
			toSend := 50
			sendMsgs(toSend)

			dname := "d33"
			dsubj := nats.NewInbox()

			// Now create an consumer for foo.AA, only requesting the last one.
			_, err = mset.addConsumer(&ConsumerConfig{
				Durable:        dname,
				DeliverSubject: dsubj,
				FilterSubject:  "foo.AA",
				DeliverPolicy:  DeliverLast,
				AckPolicy:      AckExplicit,
				AckWait:        100 * time.Millisecond,
			})
			if err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}
			sub, _ := nc.SubscribeSync(dsubj)
			defer sub.Unsubscribe()

			// Used to calculate difference between store seq and delivery seq.
			storeBaseOff := 47

			getMsg := func(seq int) *nats.Msg {
				t.Helper()
				sseq := 2*seq + storeBaseOff
				m, err := sub.NextMsg(time.Second)
				if err != nil {
					t.Fatalf("Unexpected error: %v", err)
				}
				rsseq, roseq, dcount, _, _ := replyInfo(m.Reply)
				if roseq != uint64(seq) {
					t.Fatalf("Expected consumer sequence of %d , got %d", seq, roseq)
				}
				if rsseq != uint64(sseq) {
					t.Fatalf("Expected stream sequence of %d , got %d", sseq, rsseq)
				}
				if dcount != 1 {
					t.Fatalf("Expected message to not be marked as redelivered")
				}
				return m
			}

			getRedeliveredMsg := func(seq int) *nats.Msg {
				t.Helper()
				m, err := sub.NextMsg(time.Second)
				if err != nil {
					t.Fatalf("Unexpected error: %v", err)
				}
				_, roseq, dcount, _, _ := replyInfo(m.Reply)
				if roseq != uint64(seq) {
					t.Fatalf("Expected consumer sequence of %d , got %d", seq, roseq)
				}
				if dcount < 2 {
					t.Fatalf("Expected message to be marked as redelivered")
				}
				// Ack this message.
				m.Respond(nil)
				return m
			}

			// All consumers start at 1 and always have increasing sequence numbers.
			m := getMsg(1)
			m.Respond(nil)

			// Now send 50 more, so 100 total, 26 (last + 50/2) for this consumer.
			sendMsgs(toSend)

			state := mset.state()
			if state.Msgs != uint64(toSend*2) {
				t.Fatalf("Expected %d messages, got %d", toSend*2, state.Msgs)
			}

			// For tracking next expected.
			nextSeq := 2
			noAcks := 0
			for i := 0; i < toSend/2; i++ {
				m := getMsg(nextSeq)
				if i%2 == 0 {
					m.Respond(nil) // Ack evens.
				} else {
					noAcks++
				}
				nextSeq++
			}

			// We should now get those redelivered.
			for i := 0; i < noAcks; i++ {
				getRedeliveredMsg(nextSeq)
				nextSeq++
			}

			// Now send 50 more.
			sendMsgs(toSend)

			storeBaseOff -= noAcks * 2

			for i := 0; i < toSend/2; i++ {
				m := getMsg(nextSeq)
				m.Respond(nil)
				nextSeq++
			}
		})
	}
}

func TestJetStreamConsumerInactiveNoDeadlock(t *testing.T) {
	cases := []struct {
		name    string
		mconfig *StreamConfig
	}{
		{"MemoryStore", &StreamConfig{Name: "DC", Storage: MemoryStorage}},
		{"FileStore", &StreamConfig{Name: "DC", Storage: FileStorage}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			s := RunBasicJetStreamServer()
			defer s.Shutdown()

			if config := s.JetStreamConfig(); config != nil {
				defer removeDir(t, config.StoreDir)
			}

			mset, err := s.GlobalAccount().addStream(c.mconfig)
			if err != nil {
				t.Fatalf("Unexpected error adding stream: %v", err)
			}
			defer mset.delete()

			nc := clientConnectToServer(t, s)
			defer nc.Close()

			// Send lots of msgs and have them queued up.
			for i := 0; i < 10000; i++ {
				nc.Publish("DC", []byte("OK!"))
			}
			nc.Flush()
			if state := mset.state(); state.Msgs != 10000 {
				t.Fatalf("Expected %d messages, got %d", 10000, state.Msgs)
			}

			sub, _ := nc.SubscribeSync(nats.NewInbox())
			sub.SetPendingLimits(-1, -1)
			defer sub.Unsubscribe()
			nc.Flush()

			o, err := mset.addConsumer(&ConsumerConfig{DeliverSubject: sub.Subject})
			if err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}
			defer o.delete()

			for i := 0; i < 10; i++ {
				if _, err := sub.NextMsg(time.Second); err != nil {
					t.Fatalf("Unexpected error: %v", err)
				}
			}
			// Force us to become inactive but we want to make sure we do not lock up
			// the internal sendq.
			sub.Unsubscribe()
			nc.Flush()

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
			s := RunBasicJetStreamServer()
			defer s.Shutdown()

			if config := s.JetStreamConfig(); config != nil {
				defer removeDir(t, config.StoreDir)
			}

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

				sseq, dseq, dcount, ts, _ := replyInfo(m.Reply)

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
			s := RunBasicJetStreamServer()
			defer s.Shutdown()

			if config := s.JetStreamConfig(); config != nil {
				defer removeDir(t, config.StoreDir)
			}

			mset, err := s.GlobalAccount().addStream(c.mconfig)
			if err != nil {
				t.Fatalf("Unexpected error adding stream: %v", err)
			}
			defer mset.delete()

			nc := clientConnectToServer(t, s)
			defer nc.Close()

			// Send 10 msgs
			for i := 0; i < 10; i++ {
				nc.Publish("DC", []byte("OK!"))
			}
			nc.Flush()
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

				sseq, dseq, dcount, _, _ := replyInfo(m.Reply)

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
			}
		})
	}
}

// We want to make sure that for pull based consumers that if we ack
// late with no interest the redelivery attempt is removed and we do
// not get the message back.
func TestJetStreamRedeliverAndLateAck(t *testing.T) {
	s := RunBasicJetStreamServer()
	defer s.Shutdown()

	// Forced cleanup of all persisted state.
	if config := s.JetStreamConfig(); config != nil {
		defer removeDir(t, config.StoreDir)
	}

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
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	// Wait for past ackwait time
	time.Sleep(150 * time.Millisecond)
	// Now ack!
	msg.Respond(nil)
	// We should not get this back.
	if _, err := nc.Request(nextSubj, nil, 10*time.Millisecond); err == nil {
		t.Fatalf("Message should not have been sent back")
	}
}

// https://github.com/nats-io/nats-server/issues/1502
func TestJetStreamPendingNextTimer(t *testing.T) {
	s := RunBasicJetStreamServer()
	defer s.Shutdown()

	// Forced cleanup of all persisted state.
	if config := s.JetStreamConfig(); config != nil {
		defer removeDir(t, config.StoreDir)
	}

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
			s := RunBasicJetStreamServer()
			defer s.Shutdown()

			if config := s.JetStreamConfig(); config != nil {
				defer removeDir(t, config.StoreDir)
			}

			mset, err := s.GlobalAccount().addStream(c.mconfig)
			if err != nil {
				t.Fatalf("Unexpected error adding stream: %v", err)
			}
			defer mset.delete()

			nc := clientConnectToServer(t, s)
			defer nc.Close()

			// Send 10 msgs
			for i := 0; i < 10; i++ {
				nc.Publish("DC", []byte("OK!"))
			}
			nc.Flush()
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
			s := RunBasicJetStreamServer()
			defer s.Shutdown()

			if config := s.JetStreamConfig(); config != nil {
				defer removeDir(t, config.StoreDir)
			}

			mset, err := s.GlobalAccount().addStream(c.mconfig)
			if err != nil {
				t.Fatalf("Unexpected error adding stream: %v", err)
			}
			defer mset.delete()

			nc := clientConnectToServer(t, s)
			defer nc.Close()

			// Send 100 msgs
			for i := 0; i < 100; i++ {
				nc.Publish("DC", []byte("OK!"))
			}
			nc.Flush()
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
			nc.Publish("DC", []byte("OK!"))
			nc.Flush()

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
			s := RunBasicJetStreamServer()
			defer s.Shutdown()

			if config := s.JetStreamConfig(); config != nil {
				defer removeDir(t, config.StoreDir)
			}

			mset, err := s.GlobalAccount().addStream(c.mconfig)
			if err != nil {
				t.Fatalf("Unexpected error adding stream: %v", err)
			}
			defer mset.delete()

			nc := clientConnectToServer(t, s)
			defer nc.Close()

			// Send 100 msgs
			for i := 0; i < 100; i++ {
				nc.Publish("DC", []byte("OK!"))
			}
			nc.Flush()
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
			nc.Request("DC", []byte("OK-22"), time.Second)
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
			s := RunBasicJetStreamServer()
			defer s.Shutdown()

			if config := s.JetStreamConfig(); config != nil {
				defer removeDir(t, config.StoreDir)
			}

			mset, err := s.GlobalAccount().addStream(c.mconfig)
			if err != nil {
				t.Fatalf("Unexpected error adding stream: %v", err)
			}
			defer mset.delete()

			nc := clientConnectToServer(t, s)
			defer nc.Close()

			// Send 100 msgs
			for i := 0; i < 100; i++ {
				nc.Publish("DC", []byte("OK!"))
			}
			nc.Flush()
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
			nc.Request("DC", []byte("OK-22"), time.Second)
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
			s := RunBasicJetStreamServer()
			defer s.Shutdown()

			if config := s.JetStreamConfig(); config != nil {
				defer removeDir(t, config.StoreDir)
			}

			mset, err := s.GlobalAccount().addStream(c.mconfig)
			if err != nil {
				t.Fatalf("Unexpected error adding stream: %v", err)
			}
			defer mset.delete()

			nc := clientConnectToServer(t, s)
			defer nc.Close()

			// Send 100 msgs
			totalMsgs := 100

			for i := 0; i < totalMsgs; i++ {
				nc.Publish("DC", []byte("OK!"))
			}
			nc.Flush()

			checkNumMsgs := func(numExpected int) {
				t.Helper()
				if state := mset.state(); state.Msgs != uint64(numExpected) {
					t.Fatalf("Expected %d messages, got %d", numExpected, state.Msgs)
				}
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
				nc.Publish("DC", []byte("OK!"))
			}
			nc.Flush()

			checkNumMsgs(totalMsgs)

			// Wait for all messsages to be pending for each sub.
			for i, sub := range []*nats.Subscription{sub1, sub2, sub3} {
				checkFor(t, 500*time.Millisecond, 25*time.Millisecond, func() error {
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
			s := RunBasicJetStreamServer()
			defer s.Shutdown()

			if config := s.JetStreamConfig(); config != nil {
				defer removeDir(t, config.StoreDir)
			}

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
				m.Ack()
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
			s := RunBasicJetStreamServer()
			defer s.Shutdown()

			if config := s.JetStreamConfig(); config != nil {
				defer removeDir(t, config.StoreDir)
			}

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
			s := RunBasicJetStreamServer()
			defer s.Shutdown()

			if config := s.JetStreamConfig(); config != nil {
				defer removeDir(t, config.StoreDir)
			}

			mset, err := s.GlobalAccount().addStream(c.mconfig)
			if err != nil {
				t.Fatalf("Unexpected error adding stream: %v", err)
			}
			defer mset.delete()

			checkNumMsgs := func(numExpected int) {
				t.Helper()
				checkFor(t, 200*time.Millisecond, 10*time.Millisecond, func() error {
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

func TestJetStreamConsumerReplayRate(t *testing.T) {
	cases := []struct {
		name    string
		mconfig *StreamConfig
	}{
		{"MemoryStore", &StreamConfig{Name: "DC", Storage: MemoryStorage}},
		{"FileStore", &StreamConfig{Name: "DC", Storage: FileStorage}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			s := RunBasicJetStreamServer()
			defer s.Shutdown()

			if config := s.JetStreamConfig(); config != nil {
				defer removeDir(t, config.StoreDir)
			}

			mset, err := s.GlobalAccount().addStream(c.mconfig)
			if err != nil {
				t.Fatalf("Unexpected error adding stream: %v", err)
			}
			defer mset.delete()

			nc := clientConnectToServer(t, s)
			defer nc.Close()

			// Send 10 msgs
			totalMsgs := 10

			var gaps []time.Duration
			lst := time.Now()

			for i := 0; i < totalMsgs; i++ {
				gaps = append(gaps, time.Since(lst))
				lst = time.Now()
				nc.Publish("DC", []byte("OK!"))
				// Calculate a gap between messages.
				gap := 10*time.Millisecond + time.Duration(rand.Intn(20))*time.Millisecond
				time.Sleep(gap)
			}

			if state := mset.state(); state.Msgs != uint64(totalMsgs) {
				t.Fatalf("Expected %d messages, got %d", totalMsgs, state.Msgs)
			}

			sub, _ := nc.SubscribeSync(nats.NewInbox())
			defer sub.Unsubscribe()
			nc.Flush()

			o, err := mset.addConsumer(&ConsumerConfig{DeliverSubject: sub.Subject})
			if err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}
			defer o.delete()

			// Firehose/instant which is default.
			last := time.Now()
			for i := 0; i < totalMsgs; i++ {
				if _, err := sub.NextMsg(time.Second); err != nil {
					t.Fatalf("Unexpected error: %v", err)
				}
				now := time.Now()
				// Delivery from addConsumer starts in a go routine, so be
				// more tolerant for the first message.
				limit := 5 * time.Millisecond
				if i == 0 {
					limit = 10 * time.Millisecond
				}
				if now.Sub(last) > limit {
					t.Fatalf("Expected firehose/instant delivery, got message gap of %v", now.Sub(last))
				}
				last = now
			}

			// Now do replay rate to match original.
			o, err = mset.addConsumer(&ConsumerConfig{DeliverSubject: sub.Subject, ReplayPolicy: ReplayOriginal})
			if err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}
			defer o.delete()

			// Original rate messsages were received for push based consumer.
			for i := 0; i < totalMsgs; i++ {
				start := time.Now()
				if _, err := sub.NextMsg(time.Second); err != nil {
					t.Fatalf("Unexpected error: %v", err)
				}
				gap := time.Since(start)
				// 15ms is high but on macs time.Sleep(delay) does not sleep only delay.
				// Also on travis if things get bogged down this could be delayed.
				gl, gh := gaps[i]-10*time.Millisecond, gaps[i]+15*time.Millisecond
				if gap < gl || gap > gh {
					t.Fatalf("Gap is off for %d, expected %v got %v", i, gaps[i], gap)
				}
			}

			// Now create pull based.
			oc := workerModeConfig("PM")
			oc.ReplayPolicy = ReplayOriginal
			o, err = mset.addConsumer(oc)
			if err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}
			defer o.delete()

			for i := 0; i < totalMsgs; i++ {
				start := time.Now()
				if _, err := nc.Request(o.requestNextMsgSubject(), nil, time.Second); err != nil {
					t.Fatalf("Unexpected error: %v", err)
				}
				gap := time.Since(start)
				// 10ms is high but on macs time.Sleep(delay) does not sleep only delay.
				gl, gh := gaps[i]-5*time.Millisecond, gaps[i]+10*time.Millisecond
				if gap < gl || gap > gh {
					t.Fatalf("Gap is incorrect for %d, expected %v got %v", i, gaps[i], gap)
				}
			}
		})
	}
}

func TestJetStreamConsumerReplayRateNoAck(t *testing.T) {
	cases := []struct {
		name    string
		mconfig *StreamConfig
	}{
		{"MemoryStore", &StreamConfig{Name: "DC", Storage: MemoryStorage}},
		{"FileStore", &StreamConfig{Name: "DC", Storage: FileStorage}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			s := RunBasicJetStreamServer()
			defer s.Shutdown()

			if config := s.JetStreamConfig(); config != nil {
				defer removeDir(t, config.StoreDir)
			}

			mset, err := s.GlobalAccount().addStream(c.mconfig)
			if err != nil {
				t.Fatalf("Unexpected error adding stream: %v", err)
			}
			defer mset.delete()

			nc := clientConnectToServer(t, s)
			defer nc.Close()

			// Send 10 msgs
			totalMsgs := 10
			for i := 0; i < totalMsgs; i++ {
				nc.Request("DC", []byte("Hello World"), time.Second)
				time.Sleep(time.Duration(rand.Intn(5)) * time.Millisecond)
			}
			if state := mset.state(); state.Msgs != uint64(totalMsgs) {
				t.Fatalf("Expected %d messages, got %d", totalMsgs, state.Msgs)
			}
			subj := "d.dc"
			o, err := mset.addConsumer(&ConsumerConfig{
				Durable:        "derek",
				DeliverSubject: subj,
				AckPolicy:      AckNone,
				ReplayPolicy:   ReplayOriginal,
			})
			if err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}
			defer o.delete()
			// Sleep a random amount of time.
			time.Sleep(time.Duration(rand.Intn(20)) * time.Millisecond)

			sub, _ := nc.SubscribeSync(subj)
			nc.Flush()

			checkFor(t, time.Second, 25*time.Millisecond, func() error {
				if nmsgs, _, _ := sub.Pending(); err != nil || nmsgs != totalMsgs {
					return fmt.Errorf("Did not receive correct number of messages: %d vs %d", nmsgs, totalMsgs)
				}
				return nil
			})
		})
	}
}

func TestJetStreamConsumerReplayQuit(t *testing.T) {
	cases := []struct {
		name    string
		mconfig *StreamConfig
	}{
		{"MemoryStore", &StreamConfig{Name: "DC", Storage: MemoryStorage}},
		{"FileStore", &StreamConfig{Name: "DC", Storage: FileStorage}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			s := RunBasicJetStreamServer()
			defer s.Shutdown()

			if config := s.JetStreamConfig(); config != nil {
				defer removeDir(t, config.StoreDir)
			}

			mset, err := s.GlobalAccount().addStream(c.mconfig)
			if err != nil {
				t.Fatalf("Unexpected error adding stream: %v", err)
			}
			defer mset.delete()

			nc := clientConnectToServer(t, s)
			defer nc.Close()

			// Send 2 msgs
			nc.Request("DC", []byte("OK!"), time.Second)
			time.Sleep(100 * time.Millisecond)
			nc.Request("DC", []byte("OK!"), time.Second)

			if state := mset.state(); state.Msgs != 2 {
				t.Fatalf("Expected %d messages, got %d", 2, state.Msgs)
			}

			sub, _ := nc.SubscribeSync(nats.NewInbox())
			defer sub.Unsubscribe()
			nc.Flush()

			// Now do replay rate to match original.
			o, err := mset.addConsumer(&ConsumerConfig{DeliverSubject: sub.Subject, ReplayPolicy: ReplayOriginal})
			if err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}

			// Allow loop and deliver / replay go routine to spin up
			time.Sleep(50 * time.Millisecond)
			base := runtime.NumGoroutine()
			o.delete()

			checkFor(t, 100*time.Millisecond, 10*time.Millisecond, func() error {
				if runtime.NumGoroutine() >= base {
					return fmt.Errorf("Consumer go routines still running")
				}
				return nil
			})
		})
	}
}

func TestJetStreamSystemLimits(t *testing.T) {
	s := RunRandClientPortServer()
	defer s.Shutdown()

	if config := s.JetStreamConfig(); config != nil {
		defer removeDir(t, config.StoreDir)
	}

	if _, _, err := s.JetStreamReservedResources(); err == nil {
		t.Fatalf("Expected error requesting jetstream reserved resources when not enabled")
	}
	// Create some accounts.
	facc, _ := s.LookupOrRegisterAccount("FOO")
	bacc, _ := s.LookupOrRegisterAccount("BAR")
	zacc, _ := s.LookupOrRegisterAccount("BAZ")

	jsconfig := &JetStreamConfig{MaxMemory: 1024, MaxStore: 8192}
	if err := s.EnableJetStream(jsconfig); err != nil {
		t.Fatalf("Expected no error, got %v", err)
	}

	if rm, rd, err := s.JetStreamReservedResources(); err != nil {
		t.Fatalf("Unexpected error requesting jetstream reserved resources: %v", err)
	} else if rm != 0 || rd != 0 {
		t.Fatalf("Expected reserved memory and store to be 0, got %d and %d", rm, rd)
	}

	limits := func(mem int64, store int64) *JetStreamAccountLimits {
		return &JetStreamAccountLimits{
			MaxMemory:    mem,
			MaxStore:     store,
			MaxStreams:   -1,
			MaxConsumers: -1,
		}
	}

	if err := facc.EnableJetStream(limits(24, 192)); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	// Use up rest of our resources in memory
	if err := bacc.EnableJetStream(limits(1000, 0)); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	// Now ask for more memory. Should error.
	if err := zacc.EnableJetStream(limits(1000, 0)); err == nil {
		t.Fatalf("Expected an error when exhausting memory resource limits")
	}
	// Disk too.
	if err := zacc.EnableJetStream(limits(0, 10000)); err == nil {
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

	if err := facc.EnableJetStream(limits(24, 192)); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	// Test Adjust
	l := limits(jsconfig.MaxMemory, jsconfig.MaxStore)
	l.MaxStreams = 10
	l.MaxConsumers = 10

	if err := facc.UpdateJetStreamLimits(l); err != nil {
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

	// This one should fail since over the limit for max number of streams.
	if _, err := facc.addStream(&StreamConfig{Name: "22", Storage: MemoryStorage, Subjects: []string{"foo.22"}}); err == nil {
		t.Fatalf("Expected error adding stream over limit")
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
	if err := facc.UpdateJetStreamLimits(l); err != nil {
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

func TestJetStreamStreamStorageTrackingAndLimits(t *testing.T) {
	s := RunBasicJetStreamServer()
	defer s.Shutdown()

	if config := s.JetStreamConfig(); config != nil {
		defer removeDir(t, config.StoreDir)
	}

	gacc := s.GlobalAccount()

	al := &JetStreamAccountLimits{
		MaxMemory:    8192,
		MaxStore:     -1,
		MaxStreams:   -1,
		MaxConsumers: -1,
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

	if usage.Memory > uint64(al.MaxMemory) {
		t.Fatalf("Expected memory to not exceed limit of %d, got %d", al.MaxMemory, usage.Memory)
	}

	// make sure that unlimited accounts work
	al.MaxMemory = -1

	if err := gacc.UpdateJetStreamLimits(al); err != nil {
		t.Fatalf("Unexpected error updating jetstream account limits: %v", err)
	}

	for i := 0; i < toSend; i++ {
		sendStreamMsg(t, nc, "LIMITS", "Hello World!")
	}
}

func TestJetStreamStreamFileTrackingAndLimits(t *testing.T) {
	s := RunBasicJetStreamServer()
	defer s.Shutdown()

	if config := s.JetStreamConfig(); config != nil {
		defer removeDir(t, config.StoreDir)
	}

	gacc := s.GlobalAccount()

	al := &JetStreamAccountLimits{
		MaxMemory:    8192,
		MaxStore:     9600,
		MaxStreams:   -1,
		MaxConsumers: -1,
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

	if usage.Memory > uint64(al.MaxMemory) {
		t.Fatalf("Expected memory to not exceed limit of %d, got %d", al.MaxMemory, usage.Memory)
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

	s := RunRandClientPortServer()
	defer s.Shutdown()

	jsconfig := &JetStreamConfig{MaxMemory: 128 * 1024 * 1024, MaxStore: 32 * 1024 * 1024 * 1024}
	if err := s.EnableJetStream(jsconfig); err != nil {
		t.Fatalf("Expected no error, got %v", err)
	}

	if config := s.JetStreamConfig(); config != nil {
		defer removeDir(t, config.StoreDir)
	}

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
		defer mset.delete()

		toSend := rand.Intn(100) + 1
		for n := 1; n <= toSend; n++ {
			msg := fmt.Sprintf("Hello %d", n*i)
			subj := subjects[rand.Intn(len(subjects))]
			sendStreamMsg(t, nc, subj, msg)
		}
		// Create up to 5 consumers.
		numObs := rand.Intn(5) + 1
		var obs []obsi
		for n := 1; n <= numObs; n++ {
			oname := fmt.Sprintf("WQ-%d", n)
			o, err := mset.addConsumer(workerModeConfig(oname))
			if err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}
			// Now grab some messages.
			toReceive := rand.Intn(toSend) + 1
			for r := 0; r < toReceive; r++ {
				resp, _ := nc.Request(o.requestNextMsgSubject(), nil, time.Second)
				if resp != nil {
					resp.Respond(nil)
				}
			}
			obs = append(obs, obsi{o.config(), toReceive})
		}
		ostate[msetName] = info{mset.config(), mset.state(), obs}
	}
	pusage := acc.JetStreamUsage()

	// Shutdown and restart and make sure things come back.
	s.Shutdown()

	checkFor(t, 2*time.Second, 200*time.Millisecond, func() error {
		delta := (runtime.NumGoroutine() - base)
		if delta > 3 {
			return fmt.Errorf("%d Go routines still exist post Shutdown()", delta)
		}
		return nil
	})

	s = RunRandClientPortServer()
	defer s.Shutdown()

	if err := s.EnableJetStream(jsconfig); err != nil {
		t.Fatalf("Expected no error, got %v", err)
	}

	acc = s.GlobalAccount()

	nusage := acc.JetStreamUsage()
	if nusage != pusage {
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
	s := RunBasicJetStreamServer()
	defer s.Shutdown()

	// Forced cleanup of all persisted state.
	if config := s.JetStreamConfig(); config != nil {
		defer removeDir(t, config.StoreDir)
	}

	// Client for API requests.
	nc, js := jsClientConnect(t, s)
	defer nc.Close()

	if _, err := js.AddStream(&nats.StreamConfig{Name: "TEST"}); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	sub, err := nc.SubscribeSync(nats.NewInbox())
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	defer sub.Unsubscribe()

	obsReq := CreateConsumerRequest{
		Stream: "TEST",
		Config: ConsumerConfig{
			Durable:        "dlc",
			DeliverSubject: sub.Subject,
			FlowControl:    true,
		},
	}
	req, err := json.Marshal(obsReq)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	resp, err := nc.Request(fmt.Sprintf(JSApiDurableCreateT, "TEST", "dlc"), req, time.Second)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
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
	rand.Read(msg)

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
	checkSubPending(2) // First and flowcontrol from slow start pause.

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

func TestJetStreamPushConsumerIdleHeartbeats(t *testing.T) {
	s := RunBasicJetStreamServer()
	defer s.Shutdown()

	// Forced cleanup of all persisted state.
	if config := s.JetStreamConfig(); config != nil {
		defer removeDir(t, config.StoreDir)
	}

	// Client for API requests.
	nc, js := jsClientConnect(t, s)
	defer nc.Close()

	if _, err := js.AddStream(&nats.StreamConfig{Name: "TEST"}); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	sub, err := nc.SubscribeSync(nats.NewInbox())
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
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
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	resp, err := nc.Request(fmt.Sprintf(JSApiConsumerCreateT, "TEST"), req, time.Second)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
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
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	resp, err = nc.Request(fmt.Sprintf(JSApiConsumerCreateT, "TEST"), req, time.Second)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
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
	s := RunBasicJetStreamServer()
	defer s.Shutdown()

	// Forced cleanup of all persisted state.
	if config := s.JetStreamConfig(); config != nil {
		defer removeDir(t, config.StoreDir)
	}

	// Client for API requests.
	nc, js := jsClientConnect(t, s)
	defer nc.Close()

	if _, err := js.AddStream(&nats.StreamConfig{Name: "TEST", Subjects: []string{"foo", "bar"}}); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	hbC := make(chan *nats.Msg, 8)
	sub, err := nc.ChanSubscribe(nats.NewInbox(), hbC)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
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
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	resp, err := nc.Request(fmt.Sprintf(JSApiConsumerCreateT, "TEST"), req, time.Second)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
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
	s := RunBasicJetStreamServer()
	defer s.Shutdown()

	// Forced cleanup of all persisted state.
	if config := s.JetStreamConfig(); config != nil {
		defer removeDir(t, config.StoreDir)
	}

	// Client for API requests.
	nc, js := jsClientConnect(t, s)
	defer nc.Close()

	if _, err := js.AddStream(&nats.StreamConfig{Name: "TEST"}); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	dsubj := "d.22"
	hbC := make(chan *nats.Msg, 8)
	sub, err := nc.ChanSubscribe("d.>", hbC)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	defer sub.Unsubscribe()

	obsReq := CreateConsumerRequest{
		Stream: "TEST",
		Config: ConsumerConfig{
			DeliverSubject: dsubj,
			Heartbeat:      100 * time.Millisecond,
		},
	}

	req, err := json.Marshal(obsReq)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	resp, err := nc.Request(fmt.Sprintf(JSApiConsumerCreateT, "TEST"), req, time.Second)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
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
	s := RunBasicJetStreamServer()
	defer s.Shutdown()

	// Forced cleanup of all persisted state.
	if config := s.JetStreamConfig(); config != nil {
		defer removeDir(t, config.StoreDir)
	}

	// Client for API requests.
	nc := clientConnectToServer(t, s)
	defer nc.Close()

	m := nats.NewMsg(JSApiAccountInfo)
	m.Header.Add("Accept-Encoding", "json")
	m.Header.Add("Authorization", "s3cr3t")
	m.Data = []byte("HELLO-JS!")

	resp, err := nc.RequestMsg(m, time.Second)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	var info JSApiAccountInfoResponse
	if err := json.Unmarshal(resp.Data, &info); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if info.Error != nil {
		t.Fatalf("Received an error: %+v", info.Error)
	}
}

func TestJetStreamRequestAPI(t *testing.T) {
	s := RunBasicJetStreamServer()
	defer s.Shutdown()

	// Forced cleanup of all persisted state.
	if config := s.JetStreamConfig(); config != nil {
		defer removeDir(t, config.StoreDir)
	}

	// Client for API requests.
	nc := clientConnectToServer(t, s)
	defer nc.Close()

	// This will get the current information about usage and limits for this account.
	resp, err := nc.Request(JSApiAccountInfo, nil, time.Second)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	var info JSApiAccountInfoResponse
	if err := json.Unmarshal(resp.Data, &info); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	// Now create a stream.
	msetCfg := StreamConfig{
		Name:     "MSET22",
		Storage:  FileStorage,
		Subjects: []string{"foo", "bar", "baz"},
		MaxMsgs:  100,
	}
	req, err := json.Marshal(msetCfg)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
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
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
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
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
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
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if err = json.Unmarshal(resp.Data, &info); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if info.Streams != 1 {
		t.Fatalf("Expected to see 1 Stream, got %d", info.Streams)
	}

	// Make sure list names works.
	resp, err = nc.Request(JSApiStreams, nil, time.Second)
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
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
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
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
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
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	resp, err = nc.Request(fmt.Sprintf(JSApiConsumerCreateT, msetCfg.Name), req, time.Second)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
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
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	ccResp.Error, ccResp.ConsumerInfo = nil, nil
	if err = json.Unmarshal(resp.Data, &ccResp); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	// Since we do not have interest this should have failed.
	checkNatsError(t, ccResp.Error, JSStreamMismatchErr)

	// Get the list of all of the consumers for our stream.
	resp, err = nc.Request(fmt.Sprintf(JSApiConsumersT, msetCfg.Name), nil, time.Second)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
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
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
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
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	resp, err = nc.Request(fmt.Sprintf(JSApiConsumerCreateT, msetCfg.Name), req, time.Second)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
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
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	resp, err = nc.Request(fmt.Sprintf(JSApiDurableCreateT, msetCfg.Name, obsReq.Config.Durable), req2, time.Second)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	ccResp.Error, ccResp.ConsumerInfo = nil, nil
	if err = json.Unmarshal(resp.Data, &ccResp); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	checkNatsError(t, ccResp.Error, JSConsumerDurableNameNotSetErr)

	// Now delete a msg.
	dreq := JSApiMsgDeleteRequest{Seq: 2}
	dreqj, err := json.Marshal(dreq)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
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
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if err := json.Unmarshal(resp.Data, &info); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if info.Streams != 0 {
		t.Fatalf("Expected no remaining streams, got %d", info.Streams)
	}

	// Now do templates.
	mcfg := &StreamConfig{
		Subjects:  []string{"kv.*"},
		Retention: LimitsPolicy,
		MaxAge:    time.Hour,
		MaxMsgs:   4,
		Storage:   MemoryStorage,
		Replicas:  1,
	}
	template := &StreamTemplateConfig{
		Name:       "kv",
		Config:     mcfg,
		MaxStreams: 4,
	}
	req, err = json.Marshal(template)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	// Check that the name in config has to match the name in the subject
	resp, _ = nc.Request(fmt.Sprintf(JSApiTemplateCreateT, "BOB"), req, time.Second)
	var stResp JSApiStreamTemplateCreateResponse
	if err = json.Unmarshal(resp.Data, &stResp); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	checkNatsError(t, stResp.Error, JSTemplateNameNotMatchSubjectErr)

	resp, _ = nc.Request(fmt.Sprintf(JSApiTemplateCreateT, template.Name), req, time.Second)
	stResp.Error, stResp.StreamTemplateInfo = nil, nil
	if err = json.Unmarshal(resp.Data, &stResp); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if stResp.StreamTemplateInfo == nil || stResp.Error != nil {
		t.Fatalf("Did not receive correct response")
	}

	// Create a second one.
	template.Name = "ss"
	template.Config.Subjects = []string{"foo", "bar"}

	req, err = json.Marshal(template)
	if err != nil {
		t.Fatalf("Unexpected error: %s", err)
	}

	resp, _ = nc.Request(fmt.Sprintf(JSApiTemplateCreateT, template.Name), req, time.Second)
	stResp.Error, stResp.StreamTemplateInfo = nil, nil
	if err = json.Unmarshal(resp.Data, &stResp); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if stResp.StreamTemplateInfo == nil || stResp.Error != nil {
		t.Fatalf("Did not receive correct response")
	}

	// Now grab the list of templates
	var tListResp JSApiStreamTemplateNamesResponse
	resp, err = nc.Request(JSApiTemplates, nil, time.Second)
	if err = json.Unmarshal(resp.Data, &tListResp); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if len(tListResp.Templates) != 2 {
		t.Fatalf("Expected 2 templates but got %d", len(tListResp.Templates))
	}
	sort.Strings(tListResp.Templates)
	if tListResp.Templates[0] != "kv" {
		t.Fatalf("Expected to get %q, but got %q", "kv", tListResp.Templates[0])
	}
	if tListResp.Templates[1] != "ss" {
		t.Fatalf("Expected to get %q, but got %q", "ss", tListResp.Templates[1])
	}

	// Now delete one.
	// Test bad name.
	resp, _ = nc.Request(fmt.Sprintf(JSApiTemplateDeleteT, "bob"), nil, time.Second)
	var tDeleteResp JSApiStreamTemplateDeleteResponse
	if err = json.Unmarshal(resp.Data, &tDeleteResp); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	checkNatsError(t, tDeleteResp.Error, JSStreamTemplateNotFoundErr)

	resp, _ = nc.Request(fmt.Sprintf(JSApiTemplateDeleteT, "ss"), nil, time.Second)
	tDeleteResp.Error = nil
	if err = json.Unmarshal(resp.Data, &tDeleteResp); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if !tDeleteResp.Success || tDeleteResp.Error != nil {
		t.Fatalf("Did not receive correct response: %+v", tDeleteResp.Error)
	}

	resp, err = nc.Request(JSApiTemplates, nil, time.Second)
	tListResp.Error, tListResp.Templates = nil, nil
	if err = json.Unmarshal(resp.Data, &tListResp); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if len(tListResp.Templates) != 1 {
		t.Fatalf("Expected 1 template but got %d", len(tListResp.Templates))
	}
	if tListResp.Templates[0] != "kv" {
		t.Fatalf("Expected to get %q, but got %q", "kv", tListResp.Templates[0])
	}

	// First create a stream from the template
	sendStreamMsg(t, nc, "kv.22", "derek")
	// Last do info
	resp, err = nc.Request(fmt.Sprintf(JSApiTemplateInfoT, "kv"), nil, time.Second)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	var ti StreamTemplateInfo
	if err = json.Unmarshal(resp.Data, &ti); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if len(ti.Streams) != 1 {
		t.Fatalf("Expected 1 stream, got %d", len(ti.Streams))
	}
	if ti.Streams[0] != canonicalName("kv.22") {
		t.Fatalf("Expected stream with name %q, but got %q", canonicalName("kv.22"), ti.Streams[0])
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
	s := RunBasicJetStreamServer()
	defer s.Shutdown()

	// Forced cleanup of all persisted state.
	if config := s.JetStreamConfig(); config != nil {
		defer removeDir(t, config.StoreDir)
	}

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
			s := RunBasicJetStreamServer()
			defer s.Shutdown()

			if config := s.JetStreamConfig(); config != nil && config.StoreDir != "" {
				defer removeDir(t, config.StoreDir)
			}

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
			// Can't change max consumers for now.
			cfg = *c.mconfig
			cfg.MaxConsumers = 10
			if err := mset.update(&cfg); err == nil || !strings.Contains(err.Error(), "can not change") {
				t.Fatalf("Expected error trying to change MaxConsumers")
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
			// Can't have a template set for now.
			cfg = *c.mconfig
			cfg.Template = "baz"
			if err := mset.update(&cfg); err == nil || !strings.Contains(err.Error(), "template") {
				t.Fatalf("Expected error trying to change Template owner")
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
			cfg.MaxAge = time.Millisecond
			if err := mset.update(&cfg); err != nil {
				t.Fatalf("Unexpected error %v", err)
			}
			// Just wait a bit for expiration.
			time.Sleep(25 * time.Millisecond)
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

			s := RunBasicJetStreamServer()
			defer s.Shutdown()

			if config := s.JetStreamConfig(); config != nil && config.StoreDir != "" {
				defer removeDir(t, config.StoreDir)
			}

			mset, err := s.GlobalAccount().addStream(c.mconfig)
			if err != nil {
				t.Fatalf("Unexpected error adding stream: %v", err)
			}

			nc := clientConnectToServer(t, s)
			defer nc.Close()

			pubTen := func() {
				t.Helper()
				for i := 0; i < 10; i++ {
					nc.Publish("foo", []byte("Hello World!"))
				}
				nc.Flush()
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

			expected := StreamState{Msgs: 6, Bytes: 6 * bytesPerMsg, FirstSeq: 12, LastSeq: 20}
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

			s := RunBasicJetStreamServer()
			defer s.Shutdown()

			if config := s.JetStreamConfig(); config != nil && config.StoreDir != "" {
				defer removeDir(t, config.StoreDir)
			}

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
			s := RunBasicJetStreamServer()
			defer s.Shutdown()

			if config := s.JetStreamConfig(); config != nil {
				defer removeDir(t, config.StoreDir)
			}

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
			ostate := o.info()
			if ostate.AckFloor.Stream != 11 || ostate.NumAckPending > 0 {
				t.Fatalf("Inconsistent ack state: %+v", ostate)
			}
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
			s := RunBasicJetStreamServer()
			defer s.Shutdown()

			if config := s.JetStreamConfig(); config != nil {
				defer removeDir(t, config.StoreDir)
			}

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

			state := mset.state()
			if state.Msgs != 1 {
				t.Fatalf("Expected 1 message, got %d", state.Msgs)
			}
			if state.Bytes == 0 {
				t.Fatalf("Expected non-zero bytes")
			}

			// Now access raw from stream.
			sm, err := mset.getMsg(1)
			if err != nil {
				t.Fatalf("Unexpected error getting stored message: %v", err)
			}
			// Calculate the []byte version of the headers.
			var b bytes.Buffer
			b.WriteString("NATS/1.0\r\n")
			http.Header(m.Header).Write(&b)
			b.WriteString("\r\n")
			hdr := b.Bytes()

			if !bytes.Equal(sm.Header, hdr) {
				t.Fatalf("Message headers do not match, %q vs %q", hdr, sm.Header)
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

func TestJetStreamTemplateBasics(t *testing.T) {
	s := RunBasicJetStreamServer()
	defer s.Shutdown()

	if config := s.JetStreamConfig(); config != nil {
		defer removeDir(t, config.StoreDir)
	}

	acc := s.GlobalAccount()

	mcfg := &StreamConfig{
		Subjects:  []string{"kv.*"},
		Retention: LimitsPolicy,
		MaxAge:    time.Hour,
		MaxMsgs:   4,
		Storage:   MemoryStorage,
		Replicas:  1,
	}
	template := &StreamTemplateConfig{
		Name:       "kv",
		Config:     mcfg,
		MaxStreams: 4,
	}

	if _, err := acc.addStreamTemplate(template); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if templates := acc.templates(); len(templates) != 1 {
		t.Fatalf("Expected to get array of 1 template, got %d", len(templates))
	}
	if err := acc.deleteStreamTemplate("foo"); err == nil {
		t.Fatalf("Expected an error for non-existent template")
	}
	if err := acc.deleteStreamTemplate(template.Name); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if templates := acc.templates(); len(templates) != 0 {
		t.Fatalf("Expected to get array of no templates, got %d", len(templates))
	}
	// Add it back in and test basics
	if _, err := acc.addStreamTemplate(template); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	// Connect a client and send a message which should trigger the stream creation.
	nc := clientConnectToServer(t, s)
	defer nc.Close()

	sendStreamMsg(t, nc, "kv.22", "derek")
	sendStreamMsg(t, nc, "kv.33", "cat")
	sendStreamMsg(t, nc, "kv.44", "sam")
	sendStreamMsg(t, nc, "kv.55", "meg")

	if nms := acc.numStreams(); nms != 4 {
		t.Fatalf("Expected 4 auto-created streams, got %d", nms)
	}

	// This one should fail due to max.
	if resp, err := nc.Request("kv.99", nil, 100*time.Millisecond); err == nil {
		t.Fatalf("Expected this to fail, but got %q", resp.Data)
	}

	// Now delete template and make sure the underlying streams go away too.
	if err := acc.deleteStreamTemplate(template.Name); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	if nms := acc.numStreams(); nms != 0 {
		t.Fatalf("Expected no auto-created streams to remain, got %d", nms)
	}
}

func TestJetStreamTemplateFileStoreRecovery(t *testing.T) {
	s := RunBasicJetStreamServer()
	defer s.Shutdown()

	if config := s.JetStreamConfig(); config != nil {
		defer removeDir(t, config.StoreDir)
	}

	acc := s.GlobalAccount()

	mcfg := &StreamConfig{
		Subjects:  []string{"kv.*"},
		Retention: LimitsPolicy,
		MaxAge:    time.Hour,
		MaxMsgs:   50,
		Storage:   FileStorage,
		Replicas:  1,
	}
	template := &StreamTemplateConfig{
		Name:       "kv",
		Config:     mcfg,
		MaxStreams: 100,
	}

	if _, err := acc.addStreamTemplate(template); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	// Make sure we can not add in a stream on our own with a template owner.
	badCfg := *mcfg
	badCfg.Name = "bad"
	badCfg.Template = "kv"
	if _, err := acc.addStream(&badCfg); err == nil {
		t.Fatalf("Expected error adding stream with direct template owner")
	}

	// Connect a client and send a message which should trigger the stream creation.
	nc := clientConnectToServer(t, s)
	defer nc.Close()

	for i := 1; i <= 100; i++ {
		subj := fmt.Sprintf("kv.%d", i)
		for x := 0; x < 50; x++ {
			sendStreamMsg(t, nc, subj, "Hello")
		}
	}
	nc.Flush()

	if nms := acc.numStreams(); nms != 100 {
		t.Fatalf("Expected 100 auto-created streams, got %d", nms)
	}

	// Capture port since it was dynamic.
	u, _ := url.Parse(s.ClientURL())
	port, _ := strconv.Atoi(u.Port())

	restartServer := func() {
		t.Helper()
		sd := s.JetStreamConfig().StoreDir
		// Stop current
		s.Shutdown()
		// Restart.
		s = RunJetStreamServerOnPort(port, sd)
	}

	// Restart.
	restartServer()
	defer s.Shutdown()

	acc = s.GlobalAccount()
	if nms := acc.numStreams(); nms != 100 {
		t.Fatalf("Expected 100 auto-created streams, got %d", nms)
	}
	tmpl, err := acc.lookupStreamTemplate(template.Name)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	// Make sure t.delete() survives restart.
	tmpl.delete()

	// Restart.
	restartServer()
	defer s.Shutdown()

	acc = s.GlobalAccount()
	if nms := acc.numStreams(); nms != 0 {
		t.Fatalf("Expected no auto-created streams, got %d", nms)
	}
	if _, err := acc.lookupStreamTemplate(template.Name); err == nil {
		t.Fatalf("Expected to not find the template after restart")
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
	opts := cb.opts[rand.Intn(len(cb.opts))]
	s, _ := runSolicitLeafServer(opts)
	defer s.Shutdown()

	checkLeafNodeConnected(t, s)

	if err := s.EnableJetStream(nil); err != nil {
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
	sl := ca.servers[rand.Intn(len(ca.servers))]
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
	s := RunBasicJetStreamServer()
	defer s.Shutdown()

	if config := s.JetStreamConfig(); config != nil {
		defer removeDir(t, config.StoreDir)
	}

	sa := s.SystemAccount()
	if err := sa.EnableJetStream(nil); err == nil {
		t.Fatalf("Expected an error trying to enable on the system account")
	}
}

func TestJetStreamMultipleAccountsBasics(t *testing.T) {
	conf := createConfFile(t, []byte(`
		listen: 127.0.0.1:-1
		jetstream: {max_mem_store: 64GB, max_file_store: 10TB}
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
	`))
	defer removeFile(t, conf)

	s, opts := RunServerWithConfig(conf)
	defer s.Shutdown()

	if config := s.JetStreamConfig(); config != nil {
		defer removeDir(t, config.StoreDir)
	}

	if !s.JetStreamEnabled() {
		t.Fatalf("Expected JetStream to be enabled")
	}

	nca := clientConnectToServerWithUP(t, opts, "ua", "pwd")
	defer nca.Close()

	ncb := clientConnectToServerWithUP(t, opts, "ub", "pwd")
	defer ncb.Close()

	resp, err := ncb.Request(JSApiAccountInfo, nil, time.Second)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
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
	newConf := []byte(`
		listen: 127.0.0.1:-1
		jetstream: {max_mem_store: 64GB, max_file_store: 10TB}
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
	`)
	if err := ioutil.WriteFile(conf, newConf, 0600); err != nil {
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
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
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
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
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
	conf := createConfFile(t, []byte(`
		listen: 127.0.0.1:-1
		jetstream: {max_mem_store: 2GB, max_file_store: 1TB}
	`))
	defer removeFile(t, conf)

	s, _ := RunServerWithConfig(conf)
	defer s.Shutdown()

	if !s.JetStreamEnabled() {
		t.Fatalf("Expected JetStream to be enabled")
	}

	if config := s.JetStreamConfig(); config != nil {
		defer removeDir(t, config.StoreDir)
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

// From 2.2.2 to 2.2.3 we fixed a bug that would not consistently place a jetstream directory
// under the store directory configured. However there were some cases where the directory was
// created that way and therefore 2.2.3 would start and not recognize the existing accounts,
// streams and consumers.
func TestJetStreamStoreDirectoryFix(t *testing.T) {
	sd := filepath.Join(os.TempDir(), "sd_test")
	defer removeDir(t, sd)

	conf := createConfFile(t, []byte(fmt.Sprintf("listen: 127.0.0.1:-1\njetstream: {store_dir: %q}\n", sd)))
	defer removeFile(t, conf)

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
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	defer sub.Unsubscribe()

	// Now shutdown the server.
	nc.Close()
	s.Shutdown()

	// Now move stuff up from the jetstream directory etc.
	jssd := filepath.Join(sd, JetStreamStoreDir)
	fis, _ := ioutil.ReadDir(jssd)
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
	s := RunBasicJetStreamServer()
	defer s.Shutdown()

	if config := s.JetStreamConfig(); config != nil {
		defer removeDir(t, config.StoreDir)
	}

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
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	defer sub.Unsubscribe()
	ci, err := sub.ConsumerInfo()
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	// Now do a pull. Make sure we get an error.
	m, err := nc.Request(fmt.Sprintf(JSApiRequestNextT, "TEST", ci.Name), nil, time.Second)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if m.Header.Get("Status") != "409" {
		t.Fatalf("Expected a 409 status code, got %q", m.Header.Get("Status"))
	}

	// Should not be possible to ask for more messages than MaxAckPending limit.
	ci, err = js.AddConsumer("TEST", &nats.ConsumerConfig{
		Durable:       "test",
		AckPolicy:     nats.AckExplicitPolicy,
		MaxAckPending: 5,
	})
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	m, err = nc.Request(fmt.Sprintf(JSApiRequestNextT, "TEST", ci.Name), []byte(`10`), time.Second)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if m.Header.Get("Status") != "409" {
		t.Fatalf("Expected a 409 status code, got %q", m.Header.Get("Status"))
	}
}

////////////////////////////////////////
// Benchmark placeholders
// TODO(dlc) - move
////////////////////////////////////////

func TestJetStreamPubPerf(t *testing.T) {
	// Comment out to run, holding place for now.
	t.SkipNow()

	s := RunBasicJetStreamServer()
	defer s.Shutdown()

	if config := s.JetStreamConfig(); config != nil {
		defer removeDir(t, config.StoreDir)
	}

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
	numProducers := 1

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
	time.Sleep(10 * time.Millisecond)

	start := time.Now()

	close(startCh)
	wg.Wait()

	tt := time.Since(start)
	fmt.Printf("time is %v\n", tt)
	fmt.Printf("%.0f msgs/sec\n", float64(toSend)/tt.Seconds())
}

func TestJetStreamPubWithAsyncResponsePerf(t *testing.T) {
	// Comment out to run, holding place for now.
	t.SkipNow()

	s := RunBasicJetStreamServer()
	defer s.Shutdown()

	if config := s.JetStreamConfig(); config != nil {
		defer removeDir(t, config.StoreDir)
	}

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

	s := RunBasicJetStreamServer()
	defer s.Shutdown()

	if config := s.JetStreamConfig(); config != nil {
		defer removeDir(t, config.StoreDir)
	}

	nc, js := jsClientConnect(t, s)
	defer nc.Close()

	_, err := js.AddStream(&nats.StreamConfig{Name: "foo"})
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

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

func TestJetStreamConsumerPerf(t *testing.T) {
	// Comment out to run, holding place for now.
	t.SkipNow()

	s := RunBasicJetStreamServer()
	defer s.Shutdown()

	if config := s.JetStreamConfig(); config != nil {
		defer removeDir(t, config.StoreDir)
	}

	acc := s.GlobalAccount()

	msetConfig := StreamConfig{
		Name:     "sr22",
		Storage:  MemoryStorage,
		Subjects: []string{"foo"},
	}

	mset, err := acc.addStream(&msetConfig)
	if err != nil {
		t.Fatalf("Unexpected error adding stream: %v", err)
	}

	nc := clientConnectToServer(t, s)
	defer nc.Close()

	payload := []byte("Hello World")

	toStore := 2000000
	for i := 0; i < toStore; i++ {
		nc.Publish("foo", payload)
	}
	nc.Flush()

	_, err = mset.addConsumer(&ConsumerConfig{
		Durable:        "d",
		DeliverSubject: "d",
		AckPolicy:      AckNone,
	})
	if err != nil {
		t.Fatalf("Error creating consumer: %v", err)
	}

	var received int
	done := make(chan bool)

	nc.Subscribe("d", func(m *nats.Msg) {
		received++
		if received >= toStore {
			done <- true
		}
	})
	start := time.Now()
	nc.Flush()

	<-done
	tt := time.Since(start)
	fmt.Printf("time is %v\n", tt)
	fmt.Printf("%.0f msgs/sec\n", float64(toStore)/tt.Seconds())
}

func TestJetStreamConsumerAckFileStorePerf(t *testing.T) {
	// Comment out to run, holding place for now.
	t.SkipNow()

	s := RunBasicJetStreamServer()
	defer s.Shutdown()

	if config := s.JetStreamConfig(); config != nil {
		defer removeDir(t, config.StoreDir)
	}

	acc := s.GlobalAccount()

	msetConfig := StreamConfig{
		Name:     "sr22",
		Storage:  FileStorage,
		Subjects: []string{"foo"},
	}

	mset, err := acc.addStream(&msetConfig)
	if err != nil {
		t.Fatalf("Unexpected error adding stream: %v", err)
	}

	nc := clientConnectToServer(t, s)
	defer nc.Close()

	payload := []byte("Hello World")

	toStore := uint64(200000)
	for i := uint64(0); i < toStore; i++ {
		nc.Publish("foo", payload)
	}
	nc.Flush()

	if msgs := mset.state().Msgs; msgs != uint64(toStore) {
		t.Fatalf("Expected %d messages, got %d", toStore, msgs)
	}

	o, err := mset.addConsumer(&ConsumerConfig{
		Durable:        "d",
		DeliverSubject: "d",
		AckPolicy:      AckExplicit,
		AckWait:        10 * time.Minute,
	})
	if err != nil {
		t.Fatalf("Error creating consumer: %v", err)
	}
	defer o.stop()

	var received uint64
	done := make(chan bool)

	sub, _ := nc.Subscribe("d", func(m *nats.Msg) {
		m.Respond(nil) // Ack
		received++
		if received >= toStore {
			done <- true
		}
	})
	sub.SetPendingLimits(-1, -1)

	start := time.Now()
	nc.Flush()

	<-done
	tt := time.Since(start)
	fmt.Printf("time is %v\n", tt)
	fmt.Printf("%.0f msgs/sec\n", float64(toStore)/tt.Seconds())
}

func TestJetStreamPubSubPerf(t *testing.T) {
	// Comment out to run, holding place for now.
	t.SkipNow()

	s := RunBasicJetStreamServer()
	defer s.Shutdown()

	if config := s.JetStreamConfig(); config != nil {
		defer removeDir(t, config.StoreDir)
	}

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
	cases := []struct {
		name    string
		mconfig *StreamConfig
	}{
		{"MemoryStore", &StreamConfig{
			Name:      "MY_STREAM",
			Storage:   MemoryStorage,
			Subjects:  []string{"foo.*"},
			Retention: InterestPolicy,
		}},
		{"FileStore", &StreamConfig{
			Name:      "MY_STREAM",
			Storage:   FileStorage,
			Subjects:  []string{"foo.*"},
			Retention: InterestPolicy,
		}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			s := RunBasicJetStreamServer()
			defer s.Shutdown()

			if config := s.JetStreamConfig(); config != nil {
				defer removeDir(t, config.StoreDir)
			}

			mset, err := s.GlobalAccount().addStream(c.mconfig)
			if err != nil {
				t.Fatalf("Unexpected error adding stream: %v", err)
			}
			defer mset.delete()

			nc1 := clientConnectToServer(t, s)
			defer nc1.Close()

			nc2 := clientConnectToServer(t, s)
			defer nc2.Close()

			// Create two durable consumers on the same subject
			sub1, _ := nc1.SubscribeSync(nats.NewInbox())
			defer sub1.Unsubscribe()
			nc1.Flush()

			o1, err := mset.addConsumer(&ConsumerConfig{
				Durable:        "dur1",
				DeliverSubject: sub1.Subject,
				FilterSubject:  "foo.bar",
				AckPolicy:      AckExplicit,
			})
			if err != nil {
				t.Fatalf("Unexpected error adding consumer: %v", err)
			}
			defer o1.delete()

			sub2, _ := nc2.SubscribeSync(nats.NewInbox())
			defer sub2.Unsubscribe()
			nc2.Flush()

			o2, err := mset.addConsumer(&ConsumerConfig{
				Durable:        "dur2",
				DeliverSubject: sub2.Subject,
				FilterSubject:  "foo.bar",
				AckPolicy:      AckExplicit,
				AckWait:        100 * time.Millisecond,
			})
			if err != nil {
				t.Fatalf("Unexpected error adding consumer: %v", err)
			}
			defer o2.delete()

			// Send 2 messages
			toSend := 2
			for i := 0; i < toSend; i++ {
				sendStreamMsg(t, nc1, "foo.bar", fmt.Sprintf("msg%v", i+1))
			}
			state := mset.state()
			if state.Msgs != uint64(toSend) {
				t.Fatalf("Expected %v messages, got %d", toSend, state.Msgs)
			}

			// Receive the messages and ack them.
			subs := []*nats.Subscription{sub1, sub2}
			for _, sub := range subs {
				for i := 0; i < toSend; i++ {
					m, err := sub.NextMsg(time.Second)
					if err != nil {
						t.Fatalf("Error acking message: %v", err)
					}
					m.Respond(nil)
				}
			}
			// To make sure acks are processed for checking state after sending new ones.
			checkFor(t, time.Second, 25*time.Millisecond, func() error {
				if state = mset.state(); state.Msgs != 0 {
					return fmt.Errorf("Stream still has messages")
				}
				return nil
			})

			// Now close the 2nd subscription...
			sub2.Unsubscribe()
			nc2.Flush()

			// Send 2 more new messages
			for i := 0; i < toSend; i++ {
				sendStreamMsg(t, nc1, "foo.bar", fmt.Sprintf("msg%v", 2+i+1))
			}
			state = mset.state()
			if state.Msgs != uint64(toSend) {
				t.Fatalf("Expected %v messages, got %d", toSend, state.Msgs)
			}

			// first subscription should get it and will ack it.
			for i := 0; i < toSend; i++ {
				m, err := sub1.NextMsg(time.Second)
				if err != nil {
					t.Fatalf("Error getting message to ack: %v", err)
				}
				m.Respond(nil)
			}
			// For acks from m.Respond above
			nc1.Flush()

			// Now recreate the subscription for the 2nd JS consumer
			sub2, _ = nc2.SubscribeSync(nats.NewInbox())
			defer sub2.Unsubscribe()

			o2, err = mset.addConsumer(&ConsumerConfig{
				Durable:        "dur2",
				DeliverSubject: sub2.Subject,
				FilterSubject:  "foo.bar",
				AckPolicy:      AckExplicit,
				AckWait:        100 * time.Millisecond,
			})
			if err != nil {
				t.Fatalf("Unexpected error adding consumer: %v", err)
			}
			defer o2.delete()

			// Those messages should be redelivered to the 2nd consumer
			for i := 1; i <= toSend; i++ {
				m, err := sub2.NextMsg(time.Second)
				if err != nil {
					t.Fatalf("Error receiving message %d: %v", i, err)
				}
				m.Respond(nil)

				sseq := o2.streamSeqFromReply(m.Reply)
				// Depending on timing from above we could receive stream sequences out of order but
				// we know we want 3 & 4.
				if sseq != 3 && sseq != 4 {
					t.Fatalf("Expected stream sequence of 3 or 4 but got %d", sseq)
				}
			}
		})
	}
}

// This test is in support fo clients that want to match on subject, they
// can set the filter subject always. We always store the subject so that
// should the stream later be edited to expand into more subjects the consumer
// still gets what was actually requested
func TestJetStreamConsumerFilterSubject(t *testing.T) {
	s := RunBasicJetStreamServer()
	defer s.Shutdown()

	sc := &StreamConfig{Name: "MY_STREAM", Subjects: []string{"foo"}}
	mset, err := s.GlobalAccount().addStream(sc)
	if err != nil {
		t.Fatalf("Unexpected error adding stream: %v", err)
	}
	defer mset.delete()

	cfg := &ConsumerConfig{
		Durable:        "d",
		DeliverSubject: "A",
		AckPolicy:      AckExplicit,
		FilterSubject:  "foo",
	}

	o, err := mset.addConsumer(cfg)
	if err != nil {
		t.Fatalf("Unexpected error adding consumer: %v", err)
	}
	defer o.delete()

	if o.info().Config.FilterSubject != "foo" {
		t.Fatalf("Expected the filter to be stored")
	}

	// Now use the original cfg with updated delivery subject and make sure that works ok.
	cfg = &ConsumerConfig{
		Durable:        "d",
		DeliverSubject: "B",
		AckPolicy:      AckExplicit,
		FilterSubject:  "foo",
	}

	o, err = mset.addConsumer(cfg)
	if err != nil {
		t.Fatalf("Unexpected error adding consumer: %v", err)
	}
	defer o.delete()
}

func TestJetStreamStoredMsgsDontDisappearAfterCacheExpiration(t *testing.T) {
	sc := &StreamConfig{
		Name:      "MY_STREAM",
		Storage:   FileStorage,
		Subjects:  []string{"foo.>"},
		Retention: InterestPolicy,
	}

	s := RunBasicJetStreamServer()
	defer s.Shutdown()

	if config := s.JetStreamConfig(); config != nil {
		defer removeDir(t, config.StoreDir)
	}

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

func TestJetStreamConsumerUpdateRedelivery(t *testing.T) {
	cases := []struct {
		name    string
		mconfig *StreamConfig
	}{
		{"MemoryStore", &StreamConfig{
			Name:      "MY_STREAM",
			Storage:   MemoryStorage,
			Subjects:  []string{"foo.>"},
			Retention: InterestPolicy,
		}},
		{"FileStore", &StreamConfig{
			Name:      "MY_STREAM",
			Storage:   FileStorage,
			Subjects:  []string{"foo.>"},
			Retention: InterestPolicy,
		}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			s := RunBasicJetStreamServer()
			defer s.Shutdown()

			if config := s.JetStreamConfig(); config != nil {
				defer removeDir(t, config.StoreDir)
			}

			mset, err := s.GlobalAccount().addStream(c.mconfig)
			if err != nil {
				t.Fatalf("Unexpected error adding stream: %v", err)
			}
			defer mset.delete()

			nc := clientConnectToServer(t, s)
			defer nc.Close()

			// Create a durable consumer.
			sub, _ := nc.SubscribeSync(nats.NewInbox())
			defer sub.Unsubscribe()

			o, err := mset.addConsumer(&ConsumerConfig{
				Durable:        "dur22",
				DeliverSubject: sub.Subject,
				FilterSubject:  "foo.bar",
				AckPolicy:      AckExplicit,
				AckWait:        100 * time.Millisecond,
				MaxDeliver:     3,
			})
			if err != nil {
				t.Fatalf("Unexpected error adding consumer: %v", err)
			}
			defer o.delete()

			// Send 20 messages
			toSend := 20
			for i := 1; i <= toSend; i++ {
				sendStreamMsg(t, nc, "foo.bar", fmt.Sprintf("msg-%v", i))
			}
			state := mset.state()
			if state.Msgs != uint64(toSend) {
				t.Fatalf("Expected %v messages, got %d", toSend, state.Msgs)
			}

			// Receive the messages and ack only every 4th
			for i := 0; i < toSend; i++ {
				m, err := sub.NextMsg(time.Second)
				if err != nil {
					t.Fatalf("Error getting message: %v", err)
				}
				seq, _, _, _, _ := replyInfo(m.Reply)
				// 4, 8, 12, 16, 20
				if seq%4 == 0 {
					m.Respond(nil)
				}
			}

			// Now close the sub and open a new one and update the consumer.
			sub.Unsubscribe()

			// Wait for it to become inactive
			checkFor(t, 200*time.Millisecond, 10*time.Millisecond, func() error {
				if o.isActive() {
					return fmt.Errorf("Consumer still active")
				}
				return nil
			})

			// Send 20 more messages.
			for i := toSend; i < toSend*2; i++ {
				sendStreamMsg(t, nc, "foo.bar", fmt.Sprintf("msg-%v", i))
			}

			// Create new subscription.
			sub, _ = nc.SubscribeSync(nats.NewInbox())
			defer sub.Unsubscribe()
			nc.Flush()

			o, err = mset.addConsumer(&ConsumerConfig{
				Durable:        "dur22",
				DeliverSubject: sub.Subject,
				FilterSubject:  "foo.bar",
				AckPolicy:      AckExplicit,
				AckWait:        100 * time.Millisecond,
				MaxDeliver:     3,
			})
			if err != nil {
				t.Fatalf("Unexpected error adding consumer: %v", err)
			}
			defer o.delete()

			expect := toSend + toSend - 5 // mod 4 acks
			checkFor(t, time.Second, 5*time.Millisecond, func() error {
				if nmsgs, _, _ := sub.Pending(); err != nil || nmsgs != expect {
					return fmt.Errorf("Did not receive correct number of messages: %d vs %d", nmsgs, expect)
				}
				return nil
			})

			for i, eseq := 0, uint64(1); i < expect; i++ {
				m, err := sub.NextMsg(time.Second)
				if err != nil {
					t.Fatalf("Error getting message: %v", err)
				}
				// Skip the ones we ack'd from above. We should not get them back here.
				if eseq <= uint64(toSend) && eseq%4 == 0 {
					eseq++
				}
				seq, _, dc, _, _ := replyInfo(m.Reply)
				if seq != eseq {
					t.Fatalf("Expected stream sequence of %d, got %d", eseq, seq)
				}
				if seq <= uint64(toSend) && dc != 2 {
					t.Fatalf("Expected delivery count of 2 for sequence of %d, got %d", seq, dc)
				}
				if seq > uint64(toSend) && dc != 1 {
					t.Fatalf("Expected delivery count of 1 for sequence of %d, got %d", seq, dc)
				}
				if seq > uint64(toSend) {
					m.Respond(nil) // Ack
				}
				eseq++
			}

			// We should get the second half back since we did not ack those from above.
			expect = toSend - 5
			checkFor(t, time.Second, 5*time.Millisecond, func() error {
				if nmsgs, _, _ := sub.Pending(); err != nil || nmsgs != expect {
					return fmt.Errorf("Did not receive correct number of messages: %d vs %d", nmsgs, expect)
				}
				return nil
			})

			for i, eseq := 0, uint64(1); i < expect; i++ {
				m, err := sub.NextMsg(time.Second)
				if err != nil {
					t.Fatalf("Error getting message: %v", err)
				}
				// Skip the ones we ack'd from above. We should not get them back here.
				if eseq <= uint64(toSend) && eseq%4 == 0 {
					eseq++
				}
				seq, _, dc, _, _ := replyInfo(m.Reply)
				if seq != eseq {
					t.Fatalf("Expected stream sequence of %d, got %d", eseq, seq)
				}
				if dc != 3 {
					t.Fatalf("Expected delivery count of 3 for sequence of %d, got %d", seq, dc)
				}
				eseq++
			}
		})
	}
}

func TestJetStreamConsumerMaxAckPending(t *testing.T) {
	cases := []struct {
		name    string
		mconfig *StreamConfig
	}{
		{"MemoryStore", &StreamConfig{
			Name:     "MY_STREAM",
			Storage:  MemoryStorage,
			Subjects: []string{"foo.*"},
		}},
		{"FileStore", &StreamConfig{
			Name:     "MY_STREAM",
			Storage:  FileStorage,
			Subjects: []string{"foo.*"},
		}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			s := RunBasicJetStreamServer()
			defer s.Shutdown()

			if config := s.JetStreamConfig(); config != nil {
				defer removeDir(t, config.StoreDir)
			}

			mset, err := s.GlobalAccount().addStream(c.mconfig)
			if err != nil {
				t.Fatalf("Unexpected error adding stream: %v", err)
			}
			defer mset.delete()

			nc := clientConnectToServer(t, s)
			defer nc.Close()

			// Do error scenarios.
			_, err = mset.addConsumer(&ConsumerConfig{
				Durable:        "d22",
				DeliverSubject: nats.NewInbox(),
				AckPolicy:      AckNone,
				MaxAckPending:  1,
			})
			if err == nil {
				t.Fatalf("Expected error, MaxAckPending only applicable to ack != AckNone")
			}

			// Queue up 100 messages.
			toSend := 100
			for i := 0; i < toSend; i++ {
				sendStreamMsg(t, nc, "foo.bar", fmt.Sprintf("MSG: %d", i+1))
			}

			// Limit to 33
			maxAckPending := 33

			o, err := mset.addConsumer(&ConsumerConfig{
				Durable:        "d22",
				DeliverSubject: nats.NewInbox(),
				AckPolicy:      AckExplicit,
				MaxAckPending:  maxAckPending,
			})
			if err != nil {
				t.Fatalf("Expected no error, got %v", err)
			}
			defer o.delete()

			sub, _ := nc.SubscribeSync(o.info().Config.DeliverSubject)
			defer sub.Unsubscribe()

			checkSubPending := func(numExpected int) {
				t.Helper()
				checkFor(t, time.Second, 20*time.Millisecond, func() error {
					if nmsgs, _, _ := sub.Pending(); err != nil || nmsgs != numExpected {
						return fmt.Errorf("Did not receive correct number of messages: %d vs %d", nmsgs, numExpected)
					}
					return nil
				})
			}

			checkSubPending(maxAckPending)
			// We hit the limit, double check we stayed there.
			if nmsgs, _, _ := sub.Pending(); err != nil || nmsgs != maxAckPending {
				t.Fatalf("Too many messages received: %d vs %d", nmsgs, maxAckPending)
			}

			// Now ack them all.
			for i := 0; i < maxAckPending; i++ {
				m, err := sub.NextMsg(time.Second)
				if err != nil {
					t.Fatalf("Error receiving message %d: %v", i, err)
				}
				m.Respond(nil)
			}
			checkSubPending(maxAckPending)

			o.stop()
			mset.purge(nil)

			// Now test a consumer that is live while we publish messages to the stream.
			o, err = mset.addConsumer(&ConsumerConfig{
				Durable:        "d33",
				DeliverSubject: nats.NewInbox(),
				AckPolicy:      AckExplicit,
				MaxAckPending:  maxAckPending,
			})
			if err != nil {
				t.Fatalf("Expected no error, got %v", err)
			}
			defer o.delete()

			sub, _ = nc.SubscribeSync(o.info().Config.DeliverSubject)
			defer sub.Unsubscribe()
			nc.Flush()

			checkSubPending(0)

			// Now stream more then maxAckPending.
			for i := 0; i < toSend; i++ {
				sendStreamMsg(t, nc, "foo.baz", fmt.Sprintf("MSG: %d", i+1))
			}
			checkSubPending(maxAckPending)
			// We hit the limit, double check we stayed there.
			if nmsgs, _, _ := sub.Pending(); err != nil || nmsgs != maxAckPending {
				t.Fatalf("Too many messages received: %d vs %d", nmsgs, maxAckPending)
			}
		})
	}
}

func TestJetStreamPullConsumerMaxAckPending(t *testing.T) {
	cases := []struct {
		name    string
		mconfig *StreamConfig
	}{
		{"MemoryStore", &StreamConfig{
			Name:     "MY_STREAM",
			Storage:  MemoryStorage,
			Subjects: []string{"foo.*"},
		}},
		{"FileStore", &StreamConfig{
			Name:     "MY_STREAM",
			Storage:  FileStorage,
			Subjects: []string{"foo.*"},
		}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			s := RunBasicJetStreamServer()
			defer s.Shutdown()

			if config := s.JetStreamConfig(); config != nil {
				defer removeDir(t, config.StoreDir)
			}

			mset, err := s.GlobalAccount().addStream(c.mconfig)
			if err != nil {
				t.Fatalf("Unexpected error adding stream: %v", err)
			}
			defer mset.delete()

			nc := clientConnectToServer(t, s)
			defer nc.Close()

			// Queue up 100 messages.
			toSend := 100
			for i := 0; i < toSend; i++ {
				sendStreamMsg(t, nc, "foo.bar", fmt.Sprintf("MSG: %d", i+1))
			}

			// Limit to 33
			maxAckPending := 33

			o, err := mset.addConsumer(&ConsumerConfig{
				Durable:       "d22",
				AckPolicy:     AckExplicit,
				MaxAckPending: maxAckPending,
			})
			if err != nil {
				t.Fatalf("Expected no error, got %v", err)
			}
			defer o.delete()

			getSubj := o.requestNextMsgSubject()

			var toAck []*nats.Msg

			for i := 0; i < maxAckPending; i++ {
				if m, err := nc.Request(getSubj, nil, time.Second); err != nil {
					t.Fatalf("Unexpected error: %v", err)
				} else {
					toAck = append(toAck, m)
				}
			}
			// This should fail.. But we do not want to queue up our request.
			req := &JSApiConsumerGetNextRequest{Batch: 1, NoWait: true}
			jreq, _ := json.Marshal(req)
			m, err := nc.Request(getSubj, jreq, time.Second)
			if err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}
			if m.Header.Get("Status") != "409" {
				t.Fatalf("Expected a 409 status code, got %q", m.Header.Get("Status"))
			}

			// Now ack them all.
			for _, m := range toAck {
				m.Respond(nil)
			}

			// Now do batch above the max.
			sub, _ := nc.SubscribeSync(nats.NewInbox())
			defer sub.Unsubscribe()

			checkSubPending := func(numExpected int) {
				t.Helper()
				checkFor(t, time.Second, 10*time.Millisecond, func() error {
					if nmsgs, _, _ := sub.Pending(); err != nil || nmsgs != numExpected {
						return fmt.Errorf("Did not receive correct number of messages: %d vs %d", nmsgs, numExpected)
					}
					return nil
				})
			}

			req = &JSApiConsumerGetNextRequest{Batch: maxAckPending}
			jreq, _ = json.Marshal(req)
			nc.PublishRequest(getSubj, sub.Subject, jreq)

			checkSubPending(maxAckPending)
			// We hit the limit, double check we stayed there.
			if nmsgs, _, _ := sub.Pending(); err != nil || nmsgs != maxAckPending {
				t.Fatalf("Too many messages received: %d vs %d", nmsgs, maxAckPending)
			}
		})
	}
}

func TestJetStreamPullConsumerMaxAckPendingRedeliveries(t *testing.T) {
	cases := []struct {
		name    string
		mconfig *StreamConfig
	}{
		{"MemoryStore", &StreamConfig{
			Name:     "MY_STREAM",
			Storage:  MemoryStorage,
			Subjects: []string{"foo.*"},
		}},
		{"FileStore", &StreamConfig{
			Name:     "MY_STREAM",
			Storage:  FileStorage,
			Subjects: []string{"foo.*"},
		}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			s := RunBasicJetStreamServer()
			defer s.Shutdown()

			if config := s.JetStreamConfig(); config != nil {
				defer removeDir(t, config.StoreDir)
			}

			mset, err := s.GlobalAccount().addStream(c.mconfig)
			if err != nil {
				t.Fatalf("Unexpected error adding stream: %v", err)
			}
			defer mset.delete()

			nc := clientConnectToServer(t, s)
			defer nc.Close()

			// Queue up 10 messages.
			toSend := 10
			for i := 0; i < toSend; i++ {
				sendStreamMsg(t, nc, "foo.bar", fmt.Sprintf("MSG: %d", i+1))
			}

			// Limit to 1
			maxAckPending := 1
			ackWait := 20 * time.Millisecond
			expSeq := uint64(4)

			o, err := mset.addConsumer(&ConsumerConfig{
				Durable:       "d22",
				DeliverPolicy: DeliverByStartSequence,
				OptStartSeq:   expSeq,
				AckPolicy:     AckExplicit,
				AckWait:       ackWait,
				MaxAckPending: maxAckPending,
			})
			if err != nil {
				t.Fatalf("Expected no error, got %v", err)
			}
			defer o.delete()

			getSubj := o.requestNextMsgSubject()
			delivery := uint64(1)

			getNext := func() {
				t.Helper()
				m, err := nc.Request(getSubj, nil, time.Second)
				if err != nil {
					t.Fatalf("Unexpected error: %v", err)
				}
				sseq, dseq, dcount, _, pending := replyInfo(m.Reply)
				if sseq != expSeq {
					t.Fatalf("Expected stream sequence of %d, got %d", expSeq, sseq)
				}
				if dseq != delivery {
					t.Fatalf("Expected consumer sequence of %d, got %d", delivery, dseq)
				}
				if dcount != delivery {
					t.Fatalf("Expected delivery count of %d, got %d", delivery, dcount)
				}
				if pending != uint64(toSend)-expSeq {
					t.Fatalf("Expected pending to be %d, got %d", uint64(toSend)-expSeq, pending)
				}
				delivery++
			}

			getNext()
			getNext()
			getNext()
			getNext()
			getNext()
		})
	}
}

func TestJetStreamDeliveryAfterServerRestart(t *testing.T) {
	opts := DefaultTestOptions
	opts.Port = -1
	opts.JetStream = true
	s := RunServer(&opts)
	defer s.Shutdown()

	if config := s.JetStreamConfig(); config != nil {
		defer removeDir(t, config.StoreDir)
	}

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
	conf := createConfFile(t, []byte(`
		listen: 127.0.0.1:-1
		no_auth_user: rip
		jetstream: {max_mem_store: 64GB, max_file_store: 10TB}
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
				]
			},
			IU: {
				users: [ {user: rip, password: bar} ]
				imports [
					{ service: { subject: "ORDERS.*", account: JS }, to: "my.orders.$1" }
					{ service: { subject: "$JS.API.CONSUMER.MSG.NEXT.ORDERS.d", account: JS }, to: "nxt.msg" }
					{ stream:  { subject: "deliver.ORDERS", account: JS }, to: "d" }
					{ service: { subject: "$JS.ACK.ORDERS.*.>", account: JS } }
				]
			},
		}
	`))
	defer removeFile(t, conf)

	s, _ := RunServerWithConfig(conf)
	defer s.Shutdown()

	if config := s.JetStreamConfig(); config != nil {
		defer removeDir(t, config.StoreDir)
	}

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
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
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
	resp, _ := nc.Request(m.Reply, nil, 100*time.Millisecond)
	if resp == nil {
		t.Fatalf("No response, possible timeout?")
	}
	if info := o.info(); info.AckFloor.Consumer != 2 {
		t.Fatalf("Did not receive the ack properly")
	}
}

// This is for importing all of JetStream into another account for admin purposes.
func TestJetStreamAccountImportAll(t *testing.T) {
	conf := createConfFile(t, []byte(`
		listen: 127.0.0.1:-1
		no_auth_user: rip
		jetstream: {max_mem_store: 64GB, max_file_store: 10TB}
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
	`))
	defer removeFile(t, conf)

	s, _ := RunServerWithConfig(conf)
	defer s.Shutdown()

	if config := s.JetStreamConfig(); config != nil {
		defer removeDir(t, config.StoreDir)
	}

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
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	var info JSApiAccountInfoResponse
	if err := json.Unmarshal(resp.Data, &info); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if info.Error != nil {
		t.Fatalf("Unexpected error: %+v", info.Error)
	}
	// Lookup streams.
	resp, err = nc.Request(mapSubj(JSApiStreams), nil, time.Second)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
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
	conf := createConfFile(t, []byte(`
		listen: 127.0.0.1:-1
		jetstream: {max_mem_store: 64GB, max_file_store: 10TB }
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
	`))
	defer removeFile(t, conf)

	s, _ := RunServerWithConfig(conf)
	defer s.Shutdown()

	if config := s.JetStreamConfig(); config != nil {
		defer removeDir(t, config.StoreDir)
	}

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
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

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
	template := `
		authorization {
			users [
				{user: anonymous}
				{user: user1, password: %s}
			]
		}
		no_auth_user: anonymous
		jetstream: enabled
	`
	conf := createConfFile(t, []byte(fmt.Sprintf(template, "pwd")))
	defer removeFile(t, conf)

	s, _ := RunServerWithConfig(conf)
	defer s.Shutdown()

	if config := s.JetStreamConfig(); config != nil {
		defer removeDir(t, config.StoreDir)
	}

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
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if si.State.Msgs != uint64(toSend) {
		t.Fatalf("Expected %d msgs after restart, got %d", toSend, si.State.Msgs)
	}

	if err := ioutil.WriteFile(conf, []byte(fmt.Sprintf(template, "pwd2")), 0666); err != nil {
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

// Test that we properly enfore per subject msg limits.
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
			s := RunBasicJetStreamServer()
			defer s.Shutdown()

			if config := s.JetStreamConfig(); config != nil {
				defer removeDir(t, config.StoreDir)
			}

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
			nc.Request(fmt.Sprintf(JSApiStreamCreateT, cfg.Name), req, time.Second)
			si, err := js.StreamInfo("KV")
			if err != nil {
				t.Fatalf("Unexpected error: %v", err)
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

// https://github.com/nats-io/nats-server/issues/2314
func TestJetStreamMaxMsgsPerAndDiscardNew(t *testing.T) {
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
				Discard:    DiscardNew,
				MaxMsgsPer: 1,
				Replicas:   3,
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

			js.Publish("kv.1", []byte("ok"))
			js.Publish("kv.2", []byte("ok"))
			js.Publish("kv.3", []byte("ok"))

			if si, _ := js.StreamInfo("KV"); si == nil || si.State.Msgs != 3 {
				t.Fatalf("Expected 3 messages, got %d", si.State.Msgs)
			}
			// This should fail.
			if pa, err := js.Publish("kv.1", []byte("last")); err == nil {
				t.Fatalf("Expected an error, got %+v and %v", pa, err)
			}
			// Make sure others work after the above failure.
			if _, err := js.Publish("kv.22", []byte("favorite")); err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}
		})
	}
}

func TestJetStreamFilteredConsumersWithWiderFilter(t *testing.T) {
	s := RunBasicJetStreamServer()
	defer s.Shutdown()

	if config := s.JetStreamConfig(); config != nil {
		defer removeDir(t, config.StoreDir)
	}

	// Client for API requests.
	nc, js := jsClientConnect(t, s)
	defer nc.Close()

	// Origin
	_, err := js.AddStream(&nats.StreamConfig{
		Name:     "TEST",
		Subjects: []string{"foo", "bar", "baz", "N.*"},
	})
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	// Add in some messages.
	js.Publish("foo", []byte("OK"))
	js.Publish("bar", []byte("OK"))
	js.Publish("baz", []byte("OK"))
	for i := 0; i < 12; i++ {
		js.Publish(fmt.Sprintf("N.%d", i+1), []byte("OK"))
	}

	checkFor(t, 5*time.Second, 250*time.Millisecond, func() error {
		si, err := js.StreamInfo("TEST")
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		if si.State.Msgs != 15 {
			return fmt.Errorf("Expected 15 msgs, got state: %+v", si.State)
		}
		return nil
	})

	checkWider := func(subj string, numExpected int) {
		sub, err := js.SubscribeSync(subj)
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
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
	s := RunBasicJetStreamServer()
	defer s.Shutdown()

	if config := s.JetStreamConfig(); config != nil {
		defer removeDir(t, config.StoreDir)
	}

	// Client for API requests.
	nc, js := jsClientConnect(t, s)
	defer nc.Close()

	// Origin
	_, err := js.AddStream(&nats.StreamConfig{
		Name:     "TEST",
		Subjects: []string{"foo", "bar", "baz.*"},
	})
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	// Create Mirror now.
	_, err = js.AddStream(&nats.StreamConfig{
		Name:   "M",
		Mirror: &nats.StreamSource{Name: "TEST"},
	})
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	dsubj := nats.NewInbox()
	nc.SubscribeSync(dsubj)
	nc.Flush()

	createConsumer := func(sn, fs string) {
		t.Helper()
		_, err = js.AddConsumer(sn, &nats.ConsumerConfig{DeliverSubject: dsubj, FilterSubject: fs})
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
	}
	expectFail := func(sn, fs string) {
		t.Helper()
		_, err = js.AddConsumer(sn, &nats.ConsumerConfig{DeliverSubject: dsubj, FilterSubject: fs})
		if err == nil {
			t.Fatalf("Expected error but got none")
		}
	}

	createConsumer("M", "foo")
	createConsumer("M", "bar")
	createConsumer("M", "baz.foo")
	expectFail("M", "baz")
	expectFail("M", "baz.1.2")
	expectFail("M", "apple")

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
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	createConsumer("S", "foo.1")
	createConsumer("S", "bar.1")
	expectFail("S", "baz")
	expectFail("S", "baz.1")
	expectFail("S", "apple")

	// Chaining
	// Create Mirror now.
	_, err = js.AddStream(&nats.StreamConfig{
		Name:   "M2",
		Mirror: &nats.StreamSource{Name: "M"},
	})
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	createConsumer("M2", "foo")
	createConsumer("M2", "bar")
	createConsumer("M2", "baz.foo")
	expectFail("M2", "baz")
	expectFail("M2", "baz.1.2")
	expectFail("M2", "apple")
}

func TestJetStreamMirrorBasics(t *testing.T) {
	s := RunBasicJetStreamServer()
	defer s.Shutdown()

	if config := s.JetStreamConfig(); config != nil {
		defer removeDir(t, config.StoreDir)
	}

	// Client for API requests.
	nc, js := jsClientConnect(t, s)
	defer nc.Close()

	createStream := func(cfg *nats.StreamConfig) (*nats.StreamInfo, error) {
		return js.AddStream(cfg)
	}

	createStreamOk := func(cfg *nats.StreamConfig) {
		t.Helper()
		if _, err := createStream(cfg); err != nil {
			t.Fatalf("Expected error, got %+v", err)
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

	// Source
	scfg := &nats.StreamConfig{
		Name:     "S1",
		Subjects: []string{"foo", "bar", "baz"},
	}

	// Create source stream
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
	js2, err := nc.JetStream(nats.MaxWait(10 * time.Millisecond))
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	checkFor(t, 5*time.Second, 250*time.Millisecond, func() error {
		si, err := js2.StreamInfo("M1")
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		if si.State.Msgs != 100 {
			return fmt.Errorf("Expected 100 msgs, got state: %+v", si.State)
		}
		return nil
	})

	// Purge the source stream.
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
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
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
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
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
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		if si.State.Msgs != 150 {
			return fmt.Errorf("Expected 150 msgs, got state: %+v", si.State)
		}
		if si.State.FirstSeq != 101 {
			return fmt.Errorf("Expected start seq of 101, got state: %+v", si.State)
		}
		return nil
	})
}

func TestJetStreamSourceBasics(t *testing.T) {
	s := RunBasicJetStreamServer()
	defer s.Shutdown()

	if config := s.JetStreamConfig(); config != nil {
		defer removeDir(t, config.StoreDir)
	}

	// Client for API requests.
	nc, js := jsClientConnect(t, s)
	defer nc.Close()

	createStream := func(cfg *StreamConfig) {
		t.Helper()
		req, err := json.Marshal(cfg)
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		rm, err := nc.Request(fmt.Sprintf(JSApiStreamCreateT, cfg.Name), req, time.Second)
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		var resp JSApiStreamCreateResponse
		if err := json.Unmarshal(rm.Data, &resp); err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		if resp.Error != nil {
			t.Fatalf("Unexpected error: %+v", resp.Error)
		}
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
			{Name: "foo"},
			{Name: "bar"},
			{Name: "baz"},
		},
	}

	createStream(cfg)

	// Faster timeout since we loop below checking for condition.
	js2, err := nc.JetStream(nats.MaxWait(50 * time.Millisecond))
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	checkFor(t, 2*time.Second, 100*time.Millisecond, func() error {
		si, err := js2.StreamInfo("MS")
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		if si.State.Msgs != 50 {
			return fmt.Errorf("Expected 50 msgs, got state: %+v", si.State)
		}
		return nil
	})

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
	if _, err := js.AddStream(&nats.StreamConfig{Name: "TEST", Subjects: []string{"dlc", "rip"}}); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	sendBatch("dlc", 20)
	sendBatch("rip", 20)
	sendBatch("dlc", 10)

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
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		if si.State.Msgs != 25 {
			return fmt.Errorf("Expected 25 msgs, got state: %+v", si.State)
		}
		return nil
	})
	// Double check first starting.
	m, err := js.GetMsg("FMS", 1)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if shdr := m.Header.Get(JSStreamSource); shdr == _EMPTY_ {
		t.Fatalf("Expected a header, got none")
	} else if _, sseq := streamAndSeq(shdr); sseq != 26 {
		t.Fatalf("Expected header sequence of 26, got %d", sseq)
	}

	// Test Filters
	cfg = &StreamConfig{
		Name:    "FMS2",
		Storage: FileStorage,
		Sources: []*StreamSource{
			{Name: "TEST", OptStartSeq: 11, FilterSubject: "dlc"},
		},
	}
	createStream(cfg)
	checkFor(t, 2*time.Second, 100*time.Millisecond, func() error {
		si, err := js2.StreamInfo("FMS2")
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
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
	} else if _, sseq := streamAndSeq(shdr); sseq != 11 {
		t.Fatalf("Expected header sequence of 11, got %d", sseq)
	}
}

func TestJetStreamOperatorAccounts(t *testing.T) {
	s, _ := RunServerWithConfig("./configs/js-op.conf")
	defer s.Shutdown()

	if config := s.JetStreamConfig(); config != nil {
		defer removeDir(t, config.StoreDir)
	}

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
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

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
	conf := createConfFile(t, []byte(`
		listen: 127.0.0.1:-1
		jetstream: {domain: "HUB"}
	`))
	defer removeFile(t, conf)

	s, _ := RunServerWithConfig(conf)
	defer s.Shutdown()

	if !s.JetStreamEnabled() {
		t.Fatalf("Expected JetStream to be enabled")
	}

	config := s.JetStreamConfig()
	if config != nil {
		defer removeDir(t, config.StoreDir)
	}
	if config.Domain != "HUB" {
		t.Fatalf("Expected %q as domain name, got %q", "HUB", config.Domain)
	}
}

func TestJetStreamServerDomainConfigButDisabled(t *testing.T) {
	conf := createConfFile(t, []byte(`
		listen: 127.0.0.1:-1
		jetstream: {domain: "HUB", enabled: false}
	`))
	defer removeFile(t, conf)

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

// Issue #2213
func TestJetStreamDirectConsumersBeingReported(t *testing.T) {
	s := RunBasicJetStreamServer()
	defer s.Shutdown()

	if config := s.JetStreamConfig(); config != nil {
		defer removeDir(t, config.StoreDir)
	}

	// Client for API requests.
	nc, js := jsClientConnect(t, s)
	defer nc.Close()

	_, err := js.AddStream(&nats.StreamConfig{
		Name:     "TEST",
		Subjects: []string{"foo"},
	})
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	_, err = js.AddStream(&nats.StreamConfig{
		Name: "S",
		Sources: []*nats.StreamSource{{
			Name: "TEST",
		}},
	})
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

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
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

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
	s := RunBasicJetStreamServer()
	defer s.Shutdown()

	if config := s.JetStreamConfig(); config != nil {
		defer removeDir(t, config.StoreDir)
	}

	// Client for API requests.
	nc, js := jsClientConnect(t, s)
	defer nc.Close()

	_, err := js.AddStream(&nats.StreamConfig{
		Name:     "TEST",
		Subjects: []string{"foo"},
	})
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	_, err = js.PullSubscribe("foo", "")
	if err != nil && strings.Contains(err.Error(), "{err}") {
		t.Fatalf("Error is not filled in: %v", err)
	}
}

func TestJetStreamServerEncryption(t *testing.T) {
	conf := createConfFile(t, []byte(`
		listen: 127.0.0.1:-1
		jetstream: {key: $JS_KEY}
	`))
	defer removeFile(t, conf)

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
			m.Ack()
		}
	}

	// Grab our state to compare after restart.
	si, _ := js.StreamInfo("TEST")
	ci, _ := js.ConsumerInfo("TEST", "dlc")

	// Quick check to make sure everything not just plaintext still.
	sdir := path.Join(config.StoreDir, "$G", "streams", "TEST")
	// Make sure we can not find any plaintext strings in the target file.
	checkFor := func(fn string, strs ...string) {
		t.Helper()
		data, err := ioutil.ReadFile(fn)
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
		checkKeyFile(path.Join(sdir, JetStreamMetaFileKey))
		checkFor(path.Join(sdir, JetStreamMetaFile), "TEST", "foo", "bar", "baz", "max_msgs", "max_bytes")
		// Check a message block.
		checkKeyFile(path.Join(sdir, "msgs", "1.key"))
		checkFor(path.Join(sdir, "msgs", "1.blk"), "ENCRYPTED PAYLOAD!!", "foo", "bar", "baz")

		// Check consumer meta and state.
		checkKeyFile(path.Join(sdir, "obs", "dlc", JetStreamMetaFileKey))
		checkFor(path.Join(sdir, "obs", "dlc", JetStreamMetaFile), "TEST", "dlc", "foo", "bar", "baz", "max_msgs", "ack_policy")
		// Load and see if we can parse the consumer state.
		state, err := ioutil.ReadFile(path.Join(sdir, "obs", "dlc", "o.dat"))
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
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
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if !reflect.DeepEqual(si, si2) {
		t.Fatalf("Stream infos did not match\n%+v\nvs\n%+v", si, si2)
	}

	ci2, _ := js.ConsumerInfo("TEST", "dlc")
	// Consumer create times can be slightly off after restore from disk.
	now := time.Now()
	ci.Created, ci2.Created = now, now
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
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	scfg := mset.config()
	sr, err := mset.snapshot(5*time.Second, false, true)
	if err != nil {
		t.Fatalf("Error getting snapshot: %v", err)
	}
	snapshot, err := ioutil.ReadAll(sr.Reader)
	if err != nil {
		t.Fatalf("Error reading snapshot")
	}

	// Run new server w/o encryption. Make sure we can restore properly (meaning encryption was stripped etc).
	ns := RunBasicJetStreamServer()
	defer ns.Shutdown()

	nacc := ns.GlobalAccount()
	r := bytes.NewReader(snapshot)
	mset, err = nacc.RestoreStream(&scfg, r)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
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
	mset, err = acc.RestoreStream(&scfg, r)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	ss = mset.store.State()
	if ss.Msgs != si.State.Msgs || ss.FirstSeq != si.State.FirstSeq || ss.LastSeq != si.State.LastSeq {
		t.Fatalf("Stream states do not match: %+v vs %+v", ss, si.State)
	}

	// Check that all is encrypted like above since we know we need to convert since snapshots always plaintext.
	checkEncrypted()
}

// User report of bug.
func TestJetStreamConsumerBadNumPending(t *testing.T) {
	s := RunBasicJetStreamServer()
	defer s.Shutdown()

	if config := s.JetStreamConfig(); config != nil {
		defer removeDir(t, config.StoreDir)
	}

	// Client for API requests.
	nc, js := jsClientConnect(t, s)
	defer nc.Close()

	_, err := js.AddStream(&nats.StreamConfig{
		Name:     "ORDERS",
		Subjects: []string{"orders.*"},
	})
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	newOrders := func(n int) {
		// Queue up new orders.
		for i := 0; i < n; i++ {
			js.Publish("orders.created", []byte("NEW"))
		}
	}

	newOrders(10)

	// Create to subscribers.
	process := func(m *nats.Msg) {
		js.Publish("orders.approved", []byte("APPROVED"))
	}

	op, err := js.Subscribe("orders.created", process)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	defer op.Unsubscribe()

	mon, err := js.SubscribeSync("orders.*")
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	defer mon.Unsubscribe()

	waitForMsgs := func(n uint64) {
		t.Helper()
		checkFor(t, 2*time.Second, 100*time.Millisecond, func() error {
			si, err := js.StreamInfo("ORDERS")
			if err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}
			if si.State.Msgs != n {
				return fmt.Errorf("Expected %d msgs, got state: %+v", n, si.State)
			}
			return nil
		})
	}

	checkForNoPending := func(sub *nats.Subscription) {
		t.Helper()
		if ci, err := sub.ConsumerInfo(); err != nil || ci == nil || ci.NumPending != 0 {
			if ci != nil && ci.NumPending != 0 {
				t.Fatalf("Bad consumer NumPending, expected 0 but got %d", ci.NumPending)
			} else {
				t.Fatalf("Bad consumer info: %+v", ci)
			}
		}
	}

	waitForMsgs(20)
	checkForNoPending(op)
	checkForNoPending(mon)

	newOrders(10)

	waitForMsgs(40)
	checkForNoPending(op)
	checkForNoPending(mon)
}

///////////////////////////////////////////////////////////////////////////
// Simple JetStream Benchmarks
///////////////////////////////////////////////////////////////////////////

func Benchmark__JetStreamPubWithAck(b *testing.B) {
	s := RunBasicJetStreamServer()
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
	s := RunBasicJetStreamServer()
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
	s := RunBasicJetStreamServer()
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

	s := RunBasicJetStreamServer()
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

	s := RunBasicJetStreamServer()
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
