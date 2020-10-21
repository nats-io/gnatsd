// Copyright 2019-2020 The NATS Authors
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package test

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"math/rand"
	"net/url"
	"os"
	"path/filepath"
	"reflect"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats-server/v2/server/sysmem"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nuid"
)

func TestJetStreamBasicNilConfig(t *testing.T) {
	s := RunRandClientPortServer()
	defer s.Shutdown()

	if config := s.JetStreamConfig(); config != nil {
		defer os.RemoveAll(config.StoreDir)
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

func RunBasicJetStreamServer() *server.Server {
	opts := DefaultTestOptions
	opts.Port = -1
	opts.JetStream = true
	return RunServer(&opts)
}

func RunJetStreamServerOnPort(port int, sd string) *server.Server {
	opts := DefaultTestOptions
	opts.Port = port
	opts.JetStream = true
	opts.StoreDir = filepath.Dir(sd)
	return RunServer(&opts)
}

func clientConnectToServer(t *testing.T, s *server.Server) *nats.Conn {
	nc, err := nats.Connect(s.ClientURL(),
		nats.Name("JS-TEST"),
		nats.ReconnectWait(5*time.Millisecond),
		nats.MaxReconnects(-1))
	if err != nil {
		t.Fatalf("Failed to create client: %v", err)
	}
	return nc
}

func clientConnectWithOldRequest(t *testing.T, s *server.Server) *nats.Conn {
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
		defer os.RemoveAll(config.StoreDir)
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
		t.Fatalf("Expected reserved memory and store to be 0, got %v and %v", server.FriendlyBytes(rm), server.FriendlyBytes(rd))
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
	acc = server.NewAccount("$BAZ")
	if err := acc.EnableJetStream(nil); err == nil {
		t.Fatalf("Expected error on enabling account that was not registered")
	}
}

func TestJetStreamAddStream(t *testing.T) {
	cases := []struct {
		name    string
		mconfig *server.StreamConfig
	}{
		{name: "MemoryStore",
			mconfig: &server.StreamConfig{
				Name:      "foo",
				Retention: server.LimitsPolicy,
				MaxAge:    time.Hour,
				Storage:   server.MemoryStorage,
				Replicas:  1,
			}},
		{name: "FileStore",
			mconfig: &server.StreamConfig{
				Name:      "foo",
				Retention: server.LimitsPolicy,
				MaxAge:    time.Hour,
				Storage:   server.FileStorage,
				Replicas:  1,
			}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			s := RunBasicJetStreamServer()
			defer s.Shutdown()

			if config := s.JetStreamConfig(); config != nil {
				defer os.RemoveAll(config.StoreDir)
			}

			mset, err := s.GlobalAccount().AddStream(c.mconfig)
			if err != nil {
				t.Fatalf("Unexpected error adding stream: %v", err)
			}
			defer mset.Delete()

			nc := clientConnectToServer(t, s)
			defer nc.Close()

			nc.Publish("foo", []byte("Hello World!"))
			nc.Flush()

			state := mset.State()
			if state.Msgs != 1 {
				t.Fatalf("Expected 1 message, got %d", state.Msgs)
			}
			if state.Bytes == 0 {
				t.Fatalf("Expected non-zero bytes")
			}

			nc.Publish("foo", []byte("Hello World Again!"))
			nc.Flush()

			state = mset.State()
			if state.Msgs != 2 {
				t.Fatalf("Expected 2 messages, got %d", state.Msgs)
			}

			if err := mset.Delete(); err != nil {
				t.Fatalf("Got an error deleting the stream: %v", err)
			}
		})
	}
}

func TestJetStreamAddStreamDiscardNew(t *testing.T) {
	cases := []struct {
		name    string
		mconfig *server.StreamConfig
	}{
		{name: "MemoryStore",
			mconfig: &server.StreamConfig{
				Name:     "foo",
				MaxMsgs:  10,
				MaxBytes: 4096,
				Discard:  server.DiscardNew,
				Storage:  server.MemoryStorage,
				Replicas: 1,
			}},
		{name: "FileStore",
			mconfig: &server.StreamConfig{
				Name:     "foo",
				MaxMsgs:  10,
				MaxBytes: 4096,
				Discard:  server.DiscardNew,
				Storage:  server.FileStorage,
				Replicas: 1,
			}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			s := RunBasicJetStreamServer()
			defer s.Shutdown()

			if config := s.JetStreamConfig(); config != nil {
				defer os.RemoveAll(config.StoreDir)
			}

			mset, err := s.GlobalAccount().AddStream(c.mconfig)
			if err != nil {
				t.Fatalf("Unexpected error adding stream: %v", err)
			}
			defer mset.Delete()

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
			if string(resp.Data) != "-ERR 'maximum messages exceeded'" {
				t.Fatalf("Expected to get an error about maximum messages, got %q", resp.Data)
			}

			// Now do bytes.
			mset.Purge()

			big := make([]byte, 8192)
			resp, _ = nc.Request(subj, big, 100*time.Millisecond)
			if resp == nil {
				t.Fatalf("No response, possible timeout?")
			}
			if string(resp.Data) != "-ERR 'maximum bytes exceeded'" {
				t.Fatalf("Expected to get an error about maximum bytes, got %q", resp.Data)
			}
		})
	}
}

func TestJetStreamPubAck(t *testing.T) {
	s := RunBasicJetStreamServer()
	defer s.Shutdown()

	if config := s.JetStreamConfig(); config != nil {
		defer os.RemoveAll(config.StoreDir)
	}

	sname := "PUBACK"
	acc := s.GlobalAccount()
	mconfig := &server.StreamConfig{Name: sname, Subjects: []string{"foo"}, Storage: server.MemoryStorage}
	mset, err := acc.AddStream(mconfig)
	if err != nil {
		t.Fatalf("Unexpected error adding stream: %v", err)
	}
	defer mset.Delete()

	nc := clientConnectToServer(t, s)
	defer nc.Close()

	checkRespDetails := func(resp *nats.Msg, err error, seq uint64) {
		if err != nil {
			t.Fatalf("Unexpected error from send stream msg: %v", err)
		}
		if resp == nil {
			t.Fatalf("No response from send stream msg")
		}
		if !bytes.HasPrefix(resp.Data, []byte("+OK {")) {
			t.Fatalf("Did not get a correct response: %q", resp.Data)
		}
		var pubAck server.PubAck
		if err := json.Unmarshal(resp.Data[3:], &pubAck); err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		if pubAck.Stream != sname {
			t.Fatalf("Expected %q for stream name, got %q", sname, pubAck.Stream)
		}
		if pubAck.Seq != seq {
			t.Fatalf("Expected %d for sequence, got %d", seq, pubAck.Seq)
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
		mconfig *server.StreamConfig
	}{
		{"MemoryStore", &server.StreamConfig{Name: subj, Storage: server.MemoryStorage}},
		{"FileStore", &server.StreamConfig{Name: subj, Storage: server.FileStorage}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			s := RunBasicJetStreamServer()
			defer s.Shutdown()

			if config := s.JetStreamConfig(); config != nil {
				defer os.RemoveAll(config.StoreDir)
			}

			fsCfg := &server.FileStoreConfig{BlockSize: 100}
			mset, err := s.GlobalAccount().AddStreamWithStore(c.mconfig, fsCfg)
			if err != nil {
				t.Fatalf("Unexpected error adding stream: %v", err)
			}
			defer mset.Delete()

			nc := clientConnectToServer(t, s)
			defer nc.Close()

			toSend := 250
			for i := 0; i < toSend; i++ {
				sendStreamMsg(t, nc, subj, fmt.Sprintf("MSG: %d", i+1))
			}

			time.Sleep(10 * time.Millisecond)
			startTime := time.Now()

			for i := 0; i < toSend; i++ {
				sendStreamMsg(t, nc, subj, fmt.Sprintf("MSG: %d", i+1))
			}

			if msgs := mset.State().Msgs; msgs != uint64(toSend*2) {
				t.Fatalf("Expected %d messages, got %d", toSend*2, msgs)
			}

			o, err := mset.AddConsumer(&server.ConsumerConfig{
				Durable:       "d",
				DeliverPolicy: server.DeliverByStartTime,
				OptStartTime:  &startTime,
				AckPolicy:     server.AckExplicit,
			})
			if err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}
			msg, err := nc.Request(o.RequestNextMsgSubject(), nil, time.Second)
			if err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}
			sseq, dseq, _, _ := o.ReplyInfo(msg.Reply)
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
		mconfig *server.StreamConfig
	}{
		{"MemoryStore", &server.StreamConfig{Name: subj, Subjects: []string{"foo.>"}, Storage: server.MemoryStorage}},
		{"FileStore", &server.StreamConfig{Name: subj, Subjects: []string{"foo.>"}, Storage: server.FileStorage}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			s := RunBasicJetStreamServer()
			defer s.Shutdown()

			if config := s.JetStreamConfig(); config != nil {
				defer os.RemoveAll(config.StoreDir)
			}

			mset, err := s.GlobalAccount().AddStream(c.mconfig)
			if err != nil {
				t.Fatalf("Unexpected error adding stream: %v", err)
			}
			defer mset.Delete()

			nc := clientConnectToServer(t, s)
			defer nc.Close()

			obsReq := server.CreateConsumerRequest{
				Stream: subj,
				Config: server.ConsumerConfig{
					Durable:       "d",
					DeliverPolicy: server.DeliverLast,
					FilterSubject: "foo.22",
					AckPolicy:     server.AckExplicit,
				},
			}
			req, err := json.Marshal(obsReq)
			if err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}
			_, err = nc.Request(fmt.Sprintf(server.JSApiConsumerCreateT, subj), req, time.Second)
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
		mconfig *server.StreamConfig
	}{
		{"MemoryStore", &server.StreamConfig{Name: "MY_WQ", Storage: server.MemoryStorage}},
		{"FileStore", &server.StreamConfig{Name: "MY_WQ", Storage: server.FileStorage}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			s := RunBasicJetStreamServer()
			defer s.Shutdown()

			if config := s.JetStreamConfig(); config != nil {
				defer os.RemoveAll(config.StoreDir)
			}

			mset, err := s.GlobalAccount().AddStream(c.mconfig)
			if err != nil {
				t.Fatalf("Unexpected error adding stream: %v", err)
			}
			defer mset.Delete()

			nc := clientConnectToServer(t, s)
			defer nc.Close()

			// Queue up our work item.
			sendStreamMsg(t, nc, c.mconfig.Name, "Hello World!")

			sub, _ := nc.SubscribeSync(nats.NewInbox())
			defer sub.Unsubscribe()
			nc.Flush()

			maxDeliver := 5
			ackWait := 10 * time.Millisecond

			o, err := mset.AddConsumer(&server.ConsumerConfig{
				DeliverSubject: sub.Subject,
				AckPolicy:      server.AckExplicit,
				AckWait:        ackWait,
				MaxDeliver:     maxDeliver,
			})
			if err != nil {
				t.Fatalf("Expected no error, got %v", err)
			}
			defer o.Delete()

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
		mconfig *server.StreamConfig
	}{
		{"MemoryStore", &server.StreamConfig{Name: "MY_WQ", Storage: server.MemoryStorage}},
		{"FileStore", &server.StreamConfig{Name: "MY_WQ", Storage: server.FileStorage}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			s := RunBasicJetStreamServer()
			defer s.Shutdown()

			if config := s.JetStreamConfig(); config != nil {
				defer os.RemoveAll(config.StoreDir)
			}

			mset, err := s.GlobalAccount().AddStream(c.mconfig)
			if err != nil {
				t.Fatalf("Unexpected error adding stream: %v", err)
			}
			defer mset.Delete()

			nc := clientConnectToServer(t, s)
			defer nc.Close()

			// Queue up our work item.
			sendStreamMsg(t, nc, c.mconfig.Name, "Hello World!")

			o, err := mset.AddConsumer(&server.ConsumerConfig{
				Durable:      "d",
				AckPolicy:    server.AckExplicit,
				ReplayPolicy: server.ReplayOriginal,
			})
			if err != nil {
				t.Fatalf("Expected no error, got %v", err)
			}
			defer o.Delete()

			// Force delay here which triggers the bug.
			time.Sleep(250 * time.Millisecond)

			if _, err = nc.Request(o.RequestNextMsgSubject(), nil, time.Second); err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}
		})
	}
}

func TestJetStreamNoPanicOnRaceBetweenShutdownAndConsumerDelete(t *testing.T) {
	cases := []struct {
		name    string
		mconfig *server.StreamConfig
	}{
		{"MemoryStore", &server.StreamConfig{Name: "MY_STREAM", Storage: server.MemoryStorage}},
		{"FileStore", &server.StreamConfig{Name: "MY_STREAM", Storage: server.FileStorage}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			s := RunBasicJetStreamServer()
			defer s.Shutdown()

			if config := s.JetStreamConfig(); config != nil {
				defer os.RemoveAll(config.StoreDir)
			}

			mset, err := s.GlobalAccount().AddStream(c.mconfig)
			if err != nil {
				t.Fatalf("Unexpected error adding stream: %v", err)
			}
			defer mset.Delete()

			var cons []*server.Consumer
			for i := 0; i < 100; i++ {
				o, err := mset.AddConsumer(&server.ConsumerConfig{
					Durable:   fmt.Sprintf("d%d", i),
					AckPolicy: server.AckExplicit,
				})
				if err != nil {
					t.Fatalf("Expected no error, got %v", err)
				}
				defer o.Delete()
				cons = append(cons, o)
			}

			wg := sync.WaitGroup{}
			wg.Add(1)
			go func() {
				defer wg.Done()
				for _, c := range cons {
					c.Delete()
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
		mconfig *server.StreamConfig
	}{
		{name: "MemoryStore",
			mconfig: &server.StreamConfig{
				Name:       "foo",
				Retention:  server.LimitsPolicy,
				MaxAge:     time.Hour,
				Storage:    server.MemoryStorage,
				MaxMsgSize: 22,
				Replicas:   1,
			}},
		{name: "FileStore",
			mconfig: &server.StreamConfig{
				Name:       "foo",
				Retention:  server.LimitsPolicy,
				MaxAge:     time.Hour,
				Storage:    server.FileStorage,
				MaxMsgSize: 22,
				Replicas:   1,
			}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			s := RunBasicJetStreamServer()
			defer s.Shutdown()

			if config := s.JetStreamConfig(); config != nil {
				defer os.RemoveAll(config.StoreDir)
			}

			mset, err := s.GlobalAccount().AddStream(c.mconfig)
			if err != nil {
				t.Fatalf("Unexpected error adding stream: %v", err)
			}
			defer mset.Delete()

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
			if string(resp.Data) != "-ERR 'message size exceeds maximum allowed'" {
				t.Fatalf("Expected to get an error for maximum message size, got %q", resp.Data)
			}
		})
	}
}

func TestJetStreamAddStreamCanonicalNames(t *testing.T) {
	s := RunBasicJetStreamServer()
	defer s.Shutdown()

	acc := s.GlobalAccount()

	expectErr := func(_ *server.Stream, err error) {
		t.Helper()
		if err == nil || !strings.Contains(err.Error(), "can not contain") {
			t.Fatalf("Expected error but got none")
		}
	}

	expectErr(acc.AddStream(&server.StreamConfig{Name: "foo.bar"}))
	expectErr(acc.AddStream(&server.StreamConfig{Name: "foo.bar."}))
	expectErr(acc.AddStream(&server.StreamConfig{Name: "foo.*"}))
	expectErr(acc.AddStream(&server.StreamConfig{Name: "foo.>"}))
	expectErr(acc.AddStream(&server.StreamConfig{Name: "*"}))
	expectErr(acc.AddStream(&server.StreamConfig{Name: ">"}))
	expectErr(acc.AddStream(&server.StreamConfig{Name: "*>"}))
}

func TestJetStreamAddStreamBadSubjects(t *testing.T) {
	s := RunBasicJetStreamServer()
	defer s.Shutdown()

	if config := s.JetStreamConfig(); config != nil {
		defer os.RemoveAll(config.StoreDir)
	}

	// Client for API requests.
	nc := clientConnectToServer(t, s)
	defer nc.Close()

	expectAPIErr := func(cfg server.StreamConfig) {
		t.Helper()
		req, err := json.Marshal(cfg)
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		resp, _ := nc.Request(fmt.Sprintf(server.JSApiStreamCreateT, cfg.Name), req, time.Second)
		var scResp server.JSApiStreamCreateResponse
		if err := json.Unmarshal(resp.Data, &scResp); err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		e := scResp.Error
		if e == nil || e.Code != 500 || e.Description != server.ErrMalformedSubject.Error() {
			t.Fatalf("Did not get proper error response: %+v", e)
		}
	}

	expectAPIErr(server.StreamConfig{Name: "MyStream", Subjects: []string{"foo.bar."}})
	expectAPIErr(server.StreamConfig{Name: "MyStream", Subjects: []string{".."}})
	expectAPIErr(server.StreamConfig{Name: "MyStream", Subjects: []string{".*"}})
	expectAPIErr(server.StreamConfig{Name: "MyStream", Subjects: []string{".>"}})
}

func TestJetStreamAddStreamMaxConsumers(t *testing.T) {
	s := RunBasicJetStreamServer()
	defer s.Shutdown()

	if config := s.JetStreamConfig(); config != nil {
		defer os.RemoveAll(config.StoreDir)
	}

	nc := clientConnectToServer(t, s)
	defer nc.Close()

	cfg := &server.StreamConfig{
		Name:         "MAXC",
		Subjects:     []string{"in.maxc.>"},
		MaxConsumers: 1,
	}

	acc := s.GlobalAccount()
	mset, err := acc.AddStream(cfg)
	if err != nil {
		t.Fatalf("Unexpected error adding stream: %v", err)
	}

	if mset.Config().MaxConsumers != 1 {
		t.Fatalf("Expected 1 MaxConsumers, got %d", mset.Config().MaxConsumers)
	}
}

func TestJetStreamAddStreamOverlappingSubjects(t *testing.T) {
	mconfig := &server.StreamConfig{
		Name:     "ok",
		Subjects: []string{"foo", "bar", "baz.*", "foo.bar.baz.>"},
		Storage:  server.MemoryStorage,
	}

	s := RunBasicJetStreamServer()
	defer s.Shutdown()

	if config := s.JetStreamConfig(); config != nil {
		defer os.RemoveAll(config.StoreDir)
	}

	acc := s.GlobalAccount()
	mset, err := acc.AddStream(mconfig)
	if err != nil {
		t.Fatalf("Unexpected error adding stream: %v", err)
	}
	defer mset.Delete()

	expectErr := func(_ *server.Stream, err error) {
		t.Helper()
		if err == nil || !strings.Contains(err.Error(), "subjects overlap") {
			t.Fatalf("Expected error but got none")
		}
	}

	// Test that any overlapping subjects will fail.
	expectErr(acc.AddStream(&server.StreamConfig{Name: "foo"}))
	expectErr(acc.AddStream(&server.StreamConfig{Name: "a", Subjects: []string{"baz", "bar"}}))
	expectErr(acc.AddStream(&server.StreamConfig{Name: "b", Subjects: []string{">"}}))
	expectErr(acc.AddStream(&server.StreamConfig{Name: "c", Subjects: []string{"baz.33"}}))
	expectErr(acc.AddStream(&server.StreamConfig{Name: "d", Subjects: []string{"*.33"}}))
	expectErr(acc.AddStream(&server.StreamConfig{Name: "e", Subjects: []string{"*.>"}}))
	expectErr(acc.AddStream(&server.StreamConfig{Name: "f", Subjects: []string{"foo.bar", "*.bar.>"}}))
}

func TestJetStreamAddStreamOverlapWithJSAPISubjects(t *testing.T) {
	s := RunBasicJetStreamServer()
	defer s.Shutdown()

	if config := s.JetStreamConfig(); config != nil {
		defer os.RemoveAll(config.StoreDir)
	}

	acc := s.GlobalAccount()

	expectErr := func(_ *server.Stream, err error) {
		t.Helper()
		if err == nil || !strings.Contains(err.Error(), "subjects overlap") {
			t.Fatalf("Expected error but got none")
		}
	}

	// Test that any overlapping subjects with our JSAPI should fail.
	expectErr(acc.AddStream(&server.StreamConfig{Name: "a", Subjects: []string{"$JS.API.foo", "$JS.API.bar"}}))
	expectErr(acc.AddStream(&server.StreamConfig{Name: "b", Subjects: []string{"$JS.API.>"}}))
	expectErr(acc.AddStream(&server.StreamConfig{Name: "c", Subjects: []string{"$JS.API.*"}}))

	// Events and Advisories etc should be ok.
	if _, err := acc.AddStream(&server.StreamConfig{Name: "a", Subjects: []string{"$JS.EVENT.>"}}); err != nil {
		t.Fatalf("Expected this to work: %v", err)
	}
}

func TestJetStreamAddStreamSameConfigOK(t *testing.T) {
	mconfig := &server.StreamConfig{
		Name:     "ok",
		Subjects: []string{"foo", "bar", "baz.*", "foo.bar.baz.>"},
		Storage:  server.MemoryStorage,
	}

	s := RunBasicJetStreamServer()
	defer s.Shutdown()

	if config := s.JetStreamConfig(); config != nil {
		defer os.RemoveAll(config.StoreDir)
	}

	acc := s.GlobalAccount()
	mset, err := acc.AddStream(mconfig)
	if err != nil {
		t.Fatalf("Unexpected error adding stream: %v", err)
	}
	defer mset.Delete()

	// Adding again with same config should be idempotent.
	if _, err = acc.AddStream(mconfig); err != nil {
		t.Fatalf("Unexpected error adding stream: %v", err)
	}
}

func sendStreamMsg(t *testing.T, nc *nats.Conn, subject, msg string) {
	t.Helper()
	resp, _ := nc.Request(subject, []byte(msg), 500*time.Millisecond)
	if resp == nil {
		t.Fatalf("No response for %q, possible timeout?", msg)
	}
	if !bytes.HasPrefix(resp.Data, []byte("+OK {")) {
		t.Fatalf("Expected a JetStreamPubAck, got %q", resp.Data)
	}
}

func TestJetStreamBasicAckPublish(t *testing.T) {
	cases := []struct {
		name    string
		mconfig *server.StreamConfig
	}{
		{"MemoryStore", &server.StreamConfig{Name: "foo", Storage: server.MemoryStorage, Subjects: []string{"foo.*"}}},
		{"FileStore", &server.StreamConfig{Name: "foo", Storage: server.FileStorage, Subjects: []string{"foo.*"}}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			s := RunBasicJetStreamServer()
			defer s.Shutdown()

			if config := s.JetStreamConfig(); config != nil {
				defer os.RemoveAll(config.StoreDir)
			}

			mset, err := s.GlobalAccount().AddStream(c.mconfig)
			if err != nil {
				t.Fatalf("Unexpected error adding stream: %v", err)
			}
			defer mset.Delete()

			nc := clientConnectToServer(t, s)
			defer nc.Close()

			for i := 0; i < 50; i++ {
				sendStreamMsg(t, nc, "foo.bar", "Hello World!")
			}
			state := mset.State()
			if state.Msgs != 50 {
				t.Fatalf("Expected 50 messages, got %d", state.Msgs)
			}
		})
	}
}

func TestJetStreamStateTimestamps(t *testing.T) {
	cases := []struct {
		name    string
		mconfig *server.StreamConfig
	}{
		{"MemoryStore", &server.StreamConfig{Name: "foo", Storage: server.MemoryStorage, Subjects: []string{"foo.*"}}},
		{"FileStore", &server.StreamConfig{Name: "foo", Storage: server.FileStorage, Subjects: []string{"foo.*"}}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			s := RunBasicJetStreamServer()
			defer s.Shutdown()

			if config := s.JetStreamConfig(); config != nil {
				defer os.RemoveAll(config.StoreDir)
			}

			mset, err := s.GlobalAccount().AddStream(c.mconfig)
			if err != nil {
				t.Fatalf("Unexpected error adding stream: %v", err)
			}
			defer mset.Delete()

			nc := clientConnectToServer(t, s)
			defer nc.Close()

			start := time.Now()
			delay := 250 * time.Millisecond
			sendStreamMsg(t, nc, "foo.bar", "Hello World!")
			time.Sleep(delay)
			sendStreamMsg(t, nc, "foo.bar", "Hello World Again!")

			state := mset.State()
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
		mconfig *server.StreamConfig
	}{
		{"MemoryStore", &server.StreamConfig{Name: "foo", Storage: server.MemoryStorage, NoAck: true}},
		{"FileStore", &server.StreamConfig{Name: "foo", Storage: server.FileStorage, NoAck: true}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			s := RunBasicJetStreamServer()
			defer s.Shutdown()

			if config := s.JetStreamConfig(); config != nil {
				defer os.RemoveAll(config.StoreDir)
			}

			// We can use NoAck to suppress acks even when reply subjects are present.
			mset, err := s.GlobalAccount().AddStream(c.mconfig)
			if err != nil {
				t.Fatalf("Unexpected error adding stream: %v", err)
			}
			defer mset.Delete()

			nc := clientConnectToServer(t, s)
			defer nc.Close()

			if _, err := nc.Request("foo", []byte("Hello World!"), 25*time.Millisecond); err != nats.ErrTimeout {
				t.Fatalf("Expected a timeout error and no response with acks suppressed")
			}

			state := mset.State()
			if state.Msgs != 1 {
				t.Fatalf("Expected 1 message, got %d", state.Msgs)
			}
		})
	}
}

func TestJetStreamCreateConsumer(t *testing.T) {
	cases := []struct {
		name    string
		mconfig *server.StreamConfig
	}{
		{"MemoryStore", &server.StreamConfig{Name: "foo", Storage: server.MemoryStorage, Subjects: []string{"foo", "bar"}}},
		{"FileStore", &server.StreamConfig{Name: "foo", Storage: server.FileStorage, Subjects: []string{"foo", "bar"}}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			s := RunBasicJetStreamServer()
			defer s.Shutdown()

			if config := s.JetStreamConfig(); config != nil {
				defer os.RemoveAll(config.StoreDir)
			}

			mset, err := s.GlobalAccount().AddStream(c.mconfig)
			if err != nil {
				t.Fatalf("Unexpected error adding stream: %v", err)
			}
			defer mset.Delete()

			// Check for basic errors.
			if _, err := mset.AddConsumer(nil); err == nil {
				t.Fatalf("Expected an error for no config")
			}

			// No deliver subject, meaning its in pull mode, work queue mode means it is required to
			// do explicit ack.
			if _, err := mset.AddConsumer(&server.ConsumerConfig{}); err == nil {
				t.Fatalf("Expected an error on work queue / pull mode without explicit ack mode")
			}

			// Check for delivery subject errors.

			// Literal delivery subject required.
			if _, err := mset.AddConsumer(&server.ConsumerConfig{DeliverSubject: "foo.*"}); err == nil {
				t.Fatalf("Expected an error on bad delivery subject")
			}
			// Check for cycles
			if _, err := mset.AddConsumer(&server.ConsumerConfig{DeliverSubject: "foo"}); err == nil {
				t.Fatalf("Expected an error on delivery subject that forms a cycle")
			}
			if _, err := mset.AddConsumer(&server.ConsumerConfig{DeliverSubject: "bar"}); err == nil {
				t.Fatalf("Expected an error on delivery subject that forms a cycle")
			}
			if _, err := mset.AddConsumer(&server.ConsumerConfig{DeliverSubject: "*"}); err == nil {
				t.Fatalf("Expected an error on delivery subject that forms a cycle")
			}

			// StartPosition conflicts
			now := time.Now()
			if _, err := mset.AddConsumer(&server.ConsumerConfig{
				DeliverSubject: "A",
				OptStartSeq:    1,
				OptStartTime:   &now,
			}); err == nil {
				t.Fatalf("Expected an error on start position conflicts")
			}
			if _, err := mset.AddConsumer(&server.ConsumerConfig{
				DeliverSubject: "A",
				OptStartTime:   &now,
			}); err == nil {
				t.Fatalf("Expected an error on start position conflicts")
			}

			// Non-Durables need to have subscription to delivery subject.
			delivery := nats.NewInbox()
			if _, err := mset.AddConsumer(&server.ConsumerConfig{DeliverSubject: delivery}); err == nil {
				t.Fatalf("Expected an error on unsubscribed delivery subject")
			}

			// Pull-based consumers are required to be durable since we do not know when they should
			// be cleaned up.
			if _, err := mset.AddConsumer(&server.ConsumerConfig{AckPolicy: server.AckExplicit}); err == nil {
				t.Fatalf("Expected an error on pull-based that is non-durable.")
			}

			nc := clientConnectToServer(t, s)
			defer nc.Close()
			sub, _ := nc.SubscribeSync(delivery)
			defer sub.Unsubscribe()
			nc.Flush()

			// Filtered subjects can not be AckAll.
			if _, err := mset.AddConsumer(&server.ConsumerConfig{
				DeliverSubject: delivery,
				FilterSubject:  "foo",
				AckPolicy:      server.AckAll,
			}); err == nil {
				t.Fatalf("Expected an error on partitioned consumer with ack policy of all")
			}

			// This should work..
			o, err := mset.AddConsumer(&server.ConsumerConfig{DeliverSubject: delivery})
			if err != nil {
				t.Fatalf("Expected no error with registered interest, got %v", err)
			}

			if err := mset.DeleteConsumer(o); err != nil {
				t.Fatalf("Expected no error on delete, got %v", err)
			}

			// Now let's check that durables can be created and a duplicate call to add will be ok.
			dcfg := &server.ConsumerConfig{
				Durable:        "ddd",
				DeliverSubject: delivery,
				AckPolicy:      server.AckAll,
			}
			if _, err = mset.AddConsumer(dcfg); err != nil {
				t.Fatalf("Unexpected error creating consumer: %v", err)
			}
			if _, err = mset.AddConsumer(dcfg); err != nil {
				t.Fatalf("Unexpected error creating second identical consumer: %v", err)
			}
			// Not test that we can change the delivery subject if that is only thing that has not
			// changed and we are not active.
			sub.Unsubscribe()
			sub, _ = nc.SubscribeSync("d.d.d")
			nc.Flush()
			defer sub.Unsubscribe()
			dcfg.DeliverSubject = "d.d.d"
			if _, err = mset.AddConsumer(dcfg); err != nil {
				t.Fatalf("Unexpected error creating third consumer with just deliver subject changed: %v", err)
			}
		})
	}
}

func TestJetStreamBasicDeliverSubject(t *testing.T) {
	cases := []struct {
		name    string
		mconfig *server.StreamConfig
	}{
		{"MemoryStore", &server.StreamConfig{Name: "MSET", Storage: server.MemoryStorage, Subjects: []string{"foo.*"}}},
		{"FileStore", &server.StreamConfig{Name: "MSET", Storage: server.FileStorage, Subjects: []string{"foo.*"}}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			s := RunBasicJetStreamServer()
			defer s.Shutdown()

			if config := s.JetStreamConfig(); config != nil {
				defer os.RemoveAll(config.StoreDir)
			}

			mset, err := s.GlobalAccount().AddStream(c.mconfig)
			if err != nil {
				t.Fatalf("Unexpected error adding stream: %v", err)
			}
			defer mset.Delete()

			nc := clientConnectToServer(t, s)
			defer nc.Close()

			toSend := 100
			sendSubj := "foo.bar"
			for i := 1; i <= toSend; i++ {
				sendStreamMsg(t, nc, sendSubj, strconv.Itoa(i))
			}
			state := mset.State()
			if state.Msgs != uint64(toSend) {
				t.Fatalf("Expected %d messages, got %d", toSend, state.Msgs)
			}

			// Now create an consumer. Use different connection.
			nc2 := clientConnectToServer(t, s)
			defer nc2.Close()

			sub, _ := nc2.SubscribeSync(nats.NewInbox())
			defer sub.Unsubscribe()
			nc2.Flush()

			o, err := mset.AddConsumer(&server.ConsumerConfig{DeliverSubject: sub.Subject})
			if err != nil {
				t.Fatalf("Expected no error with registered interest, got %v", err)
			}
			defer o.Delete()

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
					if seq := o.SeqFromReply(m.Reply); seq != uint64(i+seqOff) {
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
			state = mset.State()
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
			o.Delete()

			// Now check for deliver last, deliver new and deliver by seq.
			o, err = mset.AddConsumer(&server.ConsumerConfig{DeliverSubject: sub.Subject, DeliverPolicy: server.DeliverLast})
			if err != nil {
				t.Fatalf("Expected no error with registered interest, got %v", err)
			}
			defer o.Delete()

			m, err := sub.NextMsg(time.Second)
			if err != nil {
				t.Fatalf("Did not get expected message, got %v", err)
			}
			// All Consumers start with sequence #1.
			if seq := o.SeqFromReply(m.Reply); seq != 1 {
				t.Fatalf("Expected sequence to be 1, but got %d", seq)
			}
			// Check that is is the last msg we sent though.
			if mseq, _ := strconv.Atoi(string(m.Data)); mseq != 200 {
				t.Fatalf("Expected messag sequence to be 200, but got %d", mseq)
			}

			checkSubEmpty()
			o.Delete()

			// Make sure we only got one message.
			if m, err := sub.NextMsg(5 * time.Millisecond); err == nil {
				t.Fatalf("Expected no msg, got %+v", m)
			}

			checkSubEmpty()
			o.Delete()

			// Now try by sequence number.
			o, err = mset.AddConsumer(&server.ConsumerConfig{DeliverSubject: sub.Subject, DeliverPolicy: server.DeliverByStartSequence, OptStartSeq: 101})
			if err != nil {
				t.Fatalf("Expected no error with registered interest, got %v", err)
			}
			defer o.Delete()

			checkMsgs(1)

			// Now do push based queue-subscribers
			sub, _ = nc2.QueueSubscribeSync("_qg_", "dev")
			defer sub.Unsubscribe()
			nc2.Flush()

			o, err = mset.AddConsumer(&server.ConsumerConfig{DeliverSubject: sub.Subject})
			if err != nil {
				t.Fatalf("Expected no error with registered interest, got %v", err)
			}
			defer o.Delete()

			// Since we sent another batch need check to be looking for 2x.
			toSend *= 2
			checkMsgs(1)
		})
	}
}

func workerModeConfig(name string) *server.ConsumerConfig {
	return &server.ConsumerConfig{Durable: name, AckPolicy: server.AckExplicit}
}

func TestJetStreamBasicWorkQueue(t *testing.T) {
	cases := []struct {
		name    string
		mconfig *server.StreamConfig
	}{
		{"MemoryStore", &server.StreamConfig{Name: "MY_MSG_SET", Storage: server.MemoryStorage, Subjects: []string{"foo", "bar"}}},
		{"FileStore", &server.StreamConfig{Name: "MY_MSG_SET", Storage: server.FileStorage, Subjects: []string{"foo", "bar"}}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			s := RunBasicJetStreamServer()
			defer s.Shutdown()

			if config := s.JetStreamConfig(); config != nil {
				defer os.RemoveAll(config.StoreDir)
			}

			mset, err := s.GlobalAccount().AddStream(c.mconfig)
			if err != nil {
				t.Fatalf("Unexpected error adding stream: %v", err)
			}
			defer mset.Delete()

			// Create basic work queue mode consumer.
			oname := "WQ"
			o, err := mset.AddConsumer(workerModeConfig(oname))
			if err != nil {
				t.Fatalf("Expected no error with registered interest, got %v", err)
			}
			defer o.Delete()

			if o.NextSeq() != 1 {
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
			state := mset.State()
			if state.Msgs != uint64(toSend) {
				t.Fatalf("Expected %d messages, got %d", toSend, state.Msgs)
			}

			getNext := func(seqno int) {
				t.Helper()
				nextMsg, err := nc.Request(o.RequestNextMsgSubject(), nil, time.Second)
				if err != nil {
					t.Fatalf("Unexpected error for seq %d: %v", seqno, err)
				}
				if nextMsg.Subject != "bar" {
					t.Fatalf("Expected subject of %q, got %q", "bar", nextMsg.Subject)
				}
				if seq := o.SeqFromReply(nextMsg.Reply); seq != uint64(seqno) {
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
				nc.Request(sendSubj, []byte("Hello World!"), 100*time.Millisecond)
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

func TestJetStreamSubjectFiltering(t *testing.T) {
	cases := []struct {
		name    string
		mconfig *server.StreamConfig
	}{
		{"MemoryStore", &server.StreamConfig{Name: "MSET", Storage: server.MemoryStorage, Subjects: []string{"foo.*"}}},
		{"FileStore", &server.StreamConfig{Name: "MSET", Storage: server.FileStorage, Subjects: []string{"foo.*"}}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			s := RunBasicJetStreamServer()
			defer s.Shutdown()

			if config := s.JetStreamConfig(); config != nil {
				defer os.RemoveAll(config.StoreDir)
			}

			mset, err := s.GlobalAccount().AddStream(c.mconfig)
			if err != nil {
				t.Fatalf("Unexpected error adding stream: %v", err)
			}
			defer mset.Delete()

			nc := clientConnectToServer(t, s)
			defer nc.Close()

			toSend := 50
			subjA := "foo.A"
			subjB := "foo.B"

			for i := 0; i < toSend; i++ {
				sendStreamMsg(t, nc, subjA, "Hello World!")
				sendStreamMsg(t, nc, subjB, "Hello World!")
			}
			state := mset.State()
			if state.Msgs != uint64(toSend*2) {
				t.Fatalf("Expected %d messages, got %d", toSend*2, state.Msgs)
			}

			delivery := nats.NewInbox()
			sub, _ := nc.SubscribeSync(delivery)
			defer sub.Unsubscribe()
			nc.Flush()

			o, err := mset.AddConsumer(&server.ConsumerConfig{DeliverSubject: delivery, FilterSubject: subjB})
			if err != nil {
				t.Fatalf("Expected no error with registered interest, got %v", err)
			}
			defer o.Delete()

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
				if seq := o.SeqFromReply(m.Reply); seq != uint64(i) {
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
		mconfig *server.StreamConfig
	}{
		{"MemoryStore", &server.StreamConfig{Name: "MY_MSG_SET", Storage: server.MemoryStorage, Subjects: []string{"foo.*"}}},
		{"FileStore", &server.StreamConfig{Name: "MY_MSG_SET", Storage: server.FileStorage, Subjects: []string{"foo.*"}}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			s := RunBasicJetStreamServer()
			defer s.Shutdown()

			if config := s.JetStreamConfig(); config != nil {
				defer os.RemoveAll(config.StoreDir)
			}

			mset, err := s.GlobalAccount().AddStream(c.mconfig)
			if err != nil {
				t.Fatalf("Unexpected error adding stream: %v", err)
			}
			defer mset.Delete()

			nc := clientConnectToServer(t, s)
			defer nc.Close()

			toSend := 50
			subjA := "foo.A"
			subjB := "foo.B"

			for i := 0; i < toSend; i++ {
				sendStreamMsg(t, nc, subjA, "Hello World!")
				sendStreamMsg(t, nc, subjB, "Hello World!")
			}
			state := mset.State()
			if state.Msgs != uint64(toSend*2) {
				t.Fatalf("Expected %d messages, got %d", toSend*2, state.Msgs)
			}

			oname := "WQ"
			o, err := mset.AddConsumer(&server.ConsumerConfig{Durable: oname, FilterSubject: subjA, AckPolicy: server.AckExplicit})
			if err != nil {
				t.Fatalf("Expected no error with registered interest, got %v", err)
			}
			defer o.Delete()

			if o.NextSeq() != 1 {
				t.Fatalf("Expected to be starting at sequence 1")
			}

			getNext := func(seqno int) {
				t.Helper()
				nextMsg, err := nc.Request(o.RequestNextMsgSubject(), nil, time.Second)
				if err != nil {
					t.Fatalf("Unexpected error: %v", err)
				}
				if nextMsg.Subject != subjA {
					t.Fatalf("Expected subject of %q, got %q", subjA, nextMsg.Subject)
				}
				if seq := o.SeqFromReply(nextMsg.Reply); seq != uint64(seqno) {
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
		mconfig *server.StreamConfig
	}{
		{"MemoryStore", &server.StreamConfig{Name: "ORDERS", Storage: server.MemoryStorage, Subjects: []string{"orders.*.*"}}},
		{"FileStore", &server.StreamConfig{Name: "ORDERS", Storage: server.FileStorage, Subjects: []string{"orders.*.*"}}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			s := RunBasicJetStreamServer()
			defer s.Shutdown()

			if config := s.JetStreamConfig(); config != nil {
				defer os.RemoveAll(config.StoreDir)
			}

			mset, err := s.GlobalAccount().AddStream(c.mconfig)
			if err != nil {
				t.Fatalf("Unexpected error adding stream: %v", err)
			}
			defer mset.Delete()

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
			state := mset.State()
			if state.Msgs != uint64(toSend+toShip) {
				t.Fatalf("Expected %d messages, got %d", toSend+toShip, state.Msgs)
			}

			delivery := nats.NewInbox()
			sub, _ := nc.SubscribeSync(delivery)
			defer sub.Unsubscribe()
			nc.Flush()

			// Get all shipped.
			o, err := mset.AddConsumer(&server.ConsumerConfig{DeliverSubject: delivery, FilterSubject: "orders.*.SHIPPED"})
			if err != nil {
				t.Fatalf("Expected no error with registered interest, got %v", err)
			}
			defer o.Delete()

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
			o, err = mset.AddConsumer(&server.ConsumerConfig{DeliverSubject: delivery, FilterSubject: "orders.*.NEW"})
			if err != nil {
				t.Fatalf("Expected no error with registered interest, got %v", err)
			}
			defer o.Delete()

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
			o, err = mset.AddConsumer(&server.ConsumerConfig{DeliverSubject: delivery, FilterSubject: subj})
			if err != nil {
				t.Fatalf("Expected no error with registered interest, got %v", err)
			}
			defer o.Delete()

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
		mconfig *server.StreamConfig
	}{
		{"MemoryStore", &server.StreamConfig{Name: "MY_MSG_SET", Storage: server.MemoryStorage, Subjects: []string{"foo", "bar"}}},
		{"FileStore", &server.StreamConfig{Name: "MY_MSG_SET", Storage: server.FileStorage, Subjects: []string{"foo", "bar"}}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			s := RunBasicJetStreamServer()
			defer s.Shutdown()

			if config := s.JetStreamConfig(); config != nil {
				defer os.RemoveAll(config.StoreDir)
			}

			mset, err := s.GlobalAccount().AddStream(c.mconfig)
			if err != nil {
				t.Fatalf("Unexpected error adding stream: %v", err)
			}
			defer mset.Delete()

			// Create basic work queue mode consumer.
			oname := "WQ"
			o, err := mset.AddConsumer(workerModeConfig(oname))
			if err != nil {
				t.Fatalf("Expected no error with registered interest, got %v", err)
			}
			defer o.Delete()

			if o.NextSeq() != 1 {
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
			state := mset.State()
			if state.Msgs != uint64(toSend) {
				t.Fatalf("Expected %d messages, got %d", toSend, state.Msgs)
			}

			sub, _ := nc.SubscribeSync(nats.NewInbox())
			defer sub.Unsubscribe()

			// Kick things off.
			// For normal work queue semantics, you send requests to the subject with stream and consumer name.
			// We will do this to start it off then use ack+next to get other messages.
			nc.PublishRequest(o.RequestNextMsgSubject(), sub.Subject, nil)

			for i := 0; i < toSend; i++ {
				m, err := sub.NextMsg(time.Second)
				if err != nil {
					t.Fatalf("Unexpected error waiting for messages: %v", err)
				}

				if !bytes.Equal(m.Data, []byte("Hello World!")) {
					t.Fatalf("Got an invalid message from the stream: %q", m.Data)
				}

				nc.PublishRequest(m.Reply, sub.Subject, server.AckNext)
			}
		})
	}
}

func TestJetStreamWorkQueueRequestBatch(t *testing.T) {
	cases := []struct {
		name    string
		mconfig *server.StreamConfig
	}{
		{"MemoryStore", &server.StreamConfig{Name: "MY_MSG_SET", Storage: server.MemoryStorage, Subjects: []string{"foo", "bar"}}},
		{"FileStore", &server.StreamConfig{Name: "MY_MSG_SET", Storage: server.FileStorage, Subjects: []string{"foo", "bar"}}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			s := RunBasicJetStreamServer()
			defer s.Shutdown()

			if config := s.JetStreamConfig(); config != nil {
				defer os.RemoveAll(config.StoreDir)
			}

			mset, err := s.GlobalAccount().AddStream(c.mconfig)
			if err != nil {
				t.Fatalf("Unexpected error adding stream: %v", err)
			}
			defer mset.Delete()

			// Create basic work queue mode consumer.
			oname := "WQ"
			o, err := mset.AddConsumer(workerModeConfig(oname))
			if err != nil {
				t.Fatalf("Expected no error with registered interest, got %v", err)
			}
			defer o.Delete()

			if o.NextSeq() != 1 {
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
			state := mset.State()
			if state.Msgs != uint64(toSend) {
				t.Fatalf("Expected %d messages, got %d", toSend, state.Msgs)
			}

			sub, _ := nc.SubscribeSync(nats.NewInbox())
			defer sub.Unsubscribe()

			// For normal work queue semantics, you send requests to the subject with stream and consumer name.
			// We will do this to start it off then use ack+next to get other messages.
			// Kick things off with batch size of 50.
			batchSize := 50
			nc.PublishRequest(o.RequestNextMsgSubject(), sub.Subject, []byte(strconv.Itoa(batchSize)))

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
		mconfig *server.StreamConfig
	}{
		{name: "MemoryStore", mconfig: &server.StreamConfig{
			Name:      "MWQ",
			Storage:   server.MemoryStorage,
			Subjects:  []string{"MY_WORK_QUEUE.*"},
			Retention: server.WorkQueuePolicy},
		},
		{name: "FileStore", mconfig: &server.StreamConfig{
			Name:      "MWQ",
			Storage:   server.FileStorage,
			Subjects:  []string{"MY_WORK_QUEUE.*"},
			Retention: server.WorkQueuePolicy},
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			s := RunBasicJetStreamServer()
			defer s.Shutdown()

			if config := s.JetStreamConfig(); config != nil {
				defer os.RemoveAll(config.StoreDir)
			}

			mset, err := s.GlobalAccount().AddStream(c.mconfig)
			if err != nil {
				t.Fatalf("Unexpected error adding stream: %v", err)
			}
			defer mset.Delete()

			// This type of stream has restrictions which we will test here.
			// DeliverAll is only start mode allowed.
			if _, err := mset.AddConsumer(&server.ConsumerConfig{DeliverPolicy: server.DeliverLast}); err == nil {
				t.Fatalf("Expected an error with anything but DeliverAll")
			}

			// We will create a non-partitioned consumer. This should succeed.
			o, err := mset.AddConsumer(&server.ConsumerConfig{Durable: "PBO", AckPolicy: server.AckExplicit})
			if err != nil {
				t.Fatalf("Expected no error, got %v", err)
			}
			defer o.Delete()

			// Now if we create another this should fail, only can have one non-partitioned.
			if _, err := mset.AddConsumer(&server.ConsumerConfig{}); err == nil {
				t.Fatalf("Expected an error on attempt for second consumer for a workqueue")
			}
			o.Delete()

			if numo := mset.NumConsumers(); numo != 0 {
				t.Fatalf("Expected to have zero consumers, got %d", numo)
			}

			// Now add in an consumer that has a partition.
			pindex := 1
			pConfig := func(pname string) *server.ConsumerConfig {
				dname := fmt.Sprintf("PPBO-%d", pindex)
				pindex += 1
				return &server.ConsumerConfig{Durable: dname, FilterSubject: pname, AckPolicy: server.AckExplicit}
			}
			o, err = mset.AddConsumer(pConfig("MY_WORK_QUEUE.A"))
			if err != nil {
				t.Fatalf("Expected no error, got %v", err)
			}
			defer o.Delete()

			// Now creating another with separate partition should work.
			o2, err := mset.AddConsumer(pConfig("MY_WORK_QUEUE.B"))
			if err != nil {
				t.Fatalf("Expected no error, got %v", err)
			}
			defer o2.Delete()

			// Anything that would overlap should fail though.
			if _, err := mset.AddConsumer(pConfig(">")); err == nil {
				t.Fatalf("Expected an error on attempt for partitioned consumer for a workqueue")
			}
			if _, err := mset.AddConsumer(pConfig("MY_WORK_QUEUE.A")); err == nil {
				t.Fatalf("Expected an error on attempt for partitioned consumer for a workqueue")
			}
			if _, err := mset.AddConsumer(pConfig("MY_WORK_QUEUE.A")); err == nil {
				t.Fatalf("Expected an error on attempt for partitioned consumer for a workqueue")
			}

			o3, err := mset.AddConsumer(pConfig("MY_WORK_QUEUE.C"))
			if err != nil {
				t.Fatalf("Expected no error, got %v", err)
			}

			o.Delete()
			o2.Delete()
			o3.Delete()

			// Push based will be allowed now, including ephemerals.
			// They can not overlap etc meaning same rules as above apply.
			o4, err := mset.AddConsumer(&server.ConsumerConfig{
				Durable:        "DURABLE",
				DeliverSubject: "SOME.SUBJ",
				AckPolicy:      server.AckExplicit,
			})
			if err != nil {
				t.Fatalf("Unexpected Error: %v", err)
			}
			defer o4.Delete()

			// Now try to create an ephemeral
			nc := clientConnectToServer(t, s)
			defer nc.Close()

			sub, _ := nc.SubscribeSync(nats.NewInbox())
			defer sub.Unsubscribe()
			nc.Flush()

			// This should fail at first due to conflict above.
			ephCfg := &server.ConsumerConfig{DeliverSubject: sub.Subject, AckPolicy: server.AckExplicit}
			if _, err := mset.AddConsumer(ephCfg); err == nil {
				t.Fatalf("Expected an error ")
			}
			// Delete of o4 should clear.
			o4.Delete()
			o5, err := mset.AddConsumer(ephCfg)
			if err != nil {
				t.Fatalf("Unexpected Error: %v", err)
			}
			defer o5.Delete()
		})
	}
}

func TestJetStreamAckAllRedelivery(t *testing.T) {
	cases := []struct {
		name    string
		mconfig *server.StreamConfig
	}{
		{"MemoryStore", &server.StreamConfig{Name: "MY_S22", Storage: server.MemoryStorage}},
		{"FileStore", &server.StreamConfig{Name: "MY_S22", Storage: server.FileStorage}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			s := RunBasicJetStreamServer()
			defer s.Shutdown()

			if config := s.JetStreamConfig(); config != nil {
				defer os.RemoveAll(config.StoreDir)
			}

			mset, err := s.GlobalAccount().AddStream(c.mconfig)
			if err != nil {
				t.Fatalf("Unexpected error adding stream: %v", err)
			}
			defer mset.Delete()

			nc := clientConnectToServer(t, s)
			defer nc.Close()

			// Now load up some messages.
			toSend := 100
			for i := 0; i < toSend; i++ {
				sendStreamMsg(t, nc, c.mconfig.Name, "Hello World!")
			}
			state := mset.State()
			if state.Msgs != uint64(toSend) {
				t.Fatalf("Expected %d messages, got %d", toSend, state.Msgs)
			}

			sub, _ := nc.SubscribeSync(nats.NewInbox())
			defer sub.Unsubscribe()
			nc.Flush()

			o, err := mset.AddConsumer(&server.ConsumerConfig{
				DeliverSubject: sub.Subject,
				AckWait:        50 * time.Millisecond,
				AckPolicy:      server.AckAll,
			})
			if err != nil {
				t.Fatalf("Unexpected error adding consumer: %v", err)
			}
			defer o.Delete()

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
			o.Delete()

			// Now make sure that they are all redelivered in order for each redelivered batch.
			for l := 1; l <= 5; l++ {
				for i := 1; i <= toSend; i++ {
					m, _ := sub.NextMsg(time.Second)
					if seq := o.StreamSeqFromReply(m.Reply); seq != uint64(i) {
						t.Fatalf("Expected stream sequence of %d, got %d", i, seq)
					}
				}
			}
		})
	}
}

func TestJetStreamWorkQueueAckWaitRedelivery(t *testing.T) {
	cases := []struct {
		name    string
		mconfig *server.StreamConfig
	}{
		{"MemoryStore", &server.StreamConfig{Name: "MY_WQ", Storage: server.MemoryStorage, Retention: server.WorkQueuePolicy}},
		{"FileStore", &server.StreamConfig{Name: "MY_WQ", Storage: server.FileStorage, Retention: server.WorkQueuePolicy}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			s := RunBasicJetStreamServer()
			defer s.Shutdown()

			if config := s.JetStreamConfig(); config != nil {
				defer os.RemoveAll(config.StoreDir)
			}

			mset, err := s.GlobalAccount().AddStream(c.mconfig)
			if err != nil {
				t.Fatalf("Unexpected error adding stream: %v", err)
			}
			defer mset.Delete()

			nc := clientConnectToServer(t, s)
			defer nc.Close()

			// Now load up some messages.
			toSend := 100
			for i := 0; i < toSend; i++ {
				sendStreamMsg(t, nc, c.mconfig.Name, "Hello World!")
			}
			state := mset.State()
			if state.Msgs != uint64(toSend) {
				t.Fatalf("Expected %d messages, got %d", toSend, state.Msgs)
			}

			ackWait := 100 * time.Millisecond

			o, err := mset.AddConsumer(&server.ConsumerConfig{Durable: "PBO", AckPolicy: server.AckExplicit, AckWait: ackWait})
			if err != nil {
				t.Fatalf("Expected no error, got %v", err)
			}
			defer o.Delete()

			sub, _ := nc.SubscribeSync(nats.NewInbox())
			defer sub.Unsubscribe()

			reqNextMsgSubj := o.RequestNextMsgSubject()

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
			state = mset.State()
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
				sseq, dseq, dcount, _ := o.ReplyInfo(m.Reply)
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
			if state := mset.State(); state.Msgs != 0 {
				t.Fatalf("Expected no messages, got %d", state.Msgs)
			}
		})
	}
}

func TestJetStreamWorkQueueNakRedelivery(t *testing.T) {
	cases := []struct {
		name    string
		mconfig *server.StreamConfig
	}{
		{"MemoryStore", &server.StreamConfig{Name: "MY_WQ", Storage: server.MemoryStorage, Retention: server.WorkQueuePolicy}},
		{"FileStore", &server.StreamConfig{Name: "MY_WQ", Storage: server.FileStorage, Retention: server.WorkQueuePolicy}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			s := RunBasicJetStreamServer()
			defer s.Shutdown()

			if config := s.JetStreamConfig(); config != nil {
				defer os.RemoveAll(config.StoreDir)
			}

			mset, err := s.GlobalAccount().AddStream(c.mconfig)
			if err != nil {
				t.Fatalf("Unexpected error adding stream: %v", err)
			}
			defer mset.Delete()

			nc := clientConnectToServer(t, s)
			defer nc.Close()

			// Now load up some messages.
			toSend := 10
			for i := 0; i < toSend; i++ {
				sendStreamMsg(t, nc, c.mconfig.Name, "Hello World!")
			}
			state := mset.State()
			if state.Msgs != uint64(toSend) {
				t.Fatalf("Expected %d messages, got %d", toSend, state.Msgs)
			}

			o, err := mset.AddConsumer(&server.ConsumerConfig{Durable: "PBO", AckPolicy: server.AckExplicit})
			if err != nil {
				t.Fatalf("Expected no error, got %v", err)
			}
			defer o.Delete()

			getMsg := func(sseq, dseq int) *nats.Msg {
				t.Helper()
				m, err := nc.Request(o.RequestNextMsgSubject(), nil, time.Second)
				if err != nil {
					t.Fatalf("Unexpected error: %v", err)
				}
				rsseq, rdseq, _, _ := o.ReplyInfo(m.Reply)
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
			m.Respond(server.AckNak)

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
		mconfig *server.StreamConfig
	}{
		{"MemoryStore", &server.StreamConfig{Name: "MY_WQ", Storage: server.MemoryStorage, Retention: server.WorkQueuePolicy}},
		{"FileStore", &server.StreamConfig{Name: "MY_WQ", Storage: server.FileStorage, Retention: server.WorkQueuePolicy}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			s := RunBasicJetStreamServer()
			defer s.Shutdown()

			if config := s.JetStreamConfig(); config != nil {
				defer os.RemoveAll(config.StoreDir)
			}

			mset, err := s.GlobalAccount().AddStream(c.mconfig)
			if err != nil {
				t.Fatalf("Unexpected error adding stream: %v", err)
			}
			defer mset.Delete()

			nc := clientConnectToServer(t, s)
			defer nc.Close()

			// Now load up some messages.
			toSend := 2
			for i := 0; i < toSend; i++ {
				sendStreamMsg(t, nc, c.mconfig.Name, "Hello World!")
			}
			state := mset.State()
			if state.Msgs != uint64(toSend) {
				t.Fatalf("Expected %d messages, got %d", toSend, state.Msgs)
			}

			ackWait := 100 * time.Millisecond

			o, err := mset.AddConsumer(&server.ConsumerConfig{Durable: "PBO", AckPolicy: server.AckExplicit, AckWait: ackWait})
			if err != nil {
				t.Fatalf("Expected no error, got %v", err)
			}
			defer o.Delete()

			getMsg := func(sseq, dseq int) *nats.Msg {
				t.Helper()
				m, err := nc.Request(o.RequestNextMsgSubject(), nil, time.Second)
				if err != nil {
					t.Fatalf("Unexpected error: %v", err)
				}
				rsseq, rdseq, _, _ := o.ReplyInfo(m.Reply)
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
				m.Respond(server.AckProgress)
				nc.Flush()
				time.Sleep(ackWait / 5)
			}
			// We should get 2 here, not 1 since we have indicated we are working on it.
			m2 := getMsg(2, 3)
			time.Sleep(ackWait / 2)
			m2.Respond(server.AckProgress)

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
		mconfig *server.StreamConfig
	}{
		{"MemoryStore", &server.StreamConfig{Name: "MY_WQ", Storage: server.MemoryStorage, Retention: server.WorkQueuePolicy}},
		{"FileStore", &server.StreamConfig{Name: "MY_WQ", Storage: server.FileStorage, Retention: server.WorkQueuePolicy}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			s := RunBasicJetStreamServer()
			defer s.Shutdown()

			if config := s.JetStreamConfig(); config != nil {
				defer os.RemoveAll(config.StoreDir)
			}

			mset, err := s.GlobalAccount().AddStream(c.mconfig)
			if err != nil {
				t.Fatalf("Unexpected error adding stream: %v", err)
			}
			defer mset.Delete()

			nc := clientConnectToServer(t, s)
			defer nc.Close()

			// Now load up some messages.
			toSend := 22
			for i := 0; i < toSend; i++ {
				sendStreamMsg(t, nc, c.mconfig.Name, "Hello World!")
			}
			state := mset.State()
			if state.Msgs != uint64(toSend) {
				t.Fatalf("Expected %d messages, got %d", toSend, state.Msgs)
			}

			ackWait := 25 * time.Millisecond

			o, err := mset.AddConsumer(&server.ConsumerConfig{Durable: "PBO", AckPolicy: server.AckExplicit, AckWait: ackWait})
			if err != nil {
				t.Fatalf("Expected no error, got %v", err)
			}
			defer o.Delete()

			getMsg := func(sseq, dseq int) *nats.Msg {
				t.Helper()
				m, err := nc.Request(o.RequestNextMsgSubject(), nil, time.Second)
				if err != nil {
					t.Fatalf("Unexpected error: %v", err)
				}
				rsseq, rdseq, _, _ := o.ReplyInfo(m.Reply)
				if rdseq != uint64(dseq) {
					t.Fatalf("Expected delivered sequence of %d , got %d", dseq, rdseq)
				}
				if rsseq != uint64(sseq) {
					t.Fatalf("Expected store sequence of %d , got %d", sseq, rsseq)
				}
				return m
			}

			// Make sure we get the correct advisory
			sub, _ := nc.SubscribeSync(server.JSAdvisoryConsumerMsgTerminatedPre + ".>")
			defer sub.Unsubscribe()

			getMsg(1, 1)
			// Now wait past ackWait
			time.Sleep(ackWait * 2)

			// We should get 1 back.
			m := getMsg(1, 2)
			// Now terminate
			m.Respond(server.AckTerm)
			time.Sleep(ackWait * 2)

			// We should get 2 here, not 1 since we have indicated we wanted to terminate.
			getMsg(2, 3)

			// Check advisory was delivered.
			am, err := sub.NextMsg(time.Second)
			if err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}
			var adv server.JSConsumerDeliveryTerminatedAdvisory
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
		defer os.RemoveAll(config.StoreDir)
	}

	if config := s.JetStreamConfig(); config != nil {
		defer os.RemoveAll(config.StoreDir)
	}

	mname := "ACK-ACK"
	mset, err := s.GlobalAccount().AddStream(&server.StreamConfig{Name: mname, Storage: server.MemoryStorage})
	if err != nil {
		t.Fatalf("Unexpected error adding stream: %v", err)
	}
	defer mset.Delete()

	o, err := mset.AddConsumer(&server.ConsumerConfig{Durable: "worker", AckPolicy: server.AckExplicit})
	if err != nil {
		t.Fatalf("Expected no error with registered interest, got %v", err)
	}
	rqn := o.RequestNextMsgSubject()
	defer o.Delete()

	nc := clientConnectToServer(t, s)
	defer nc.Close()

	// 5 for number of ack protocols to test them all.
	for i := 0; i < 5; i++ {
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

	testAck(server.AckAck)
	testAck(server.AckNak)
	testAck(server.AckProgress)
	testAck(server.AckNext)
	testAck(server.AckTerm)
}

func TestJetStreamPublishDeDupe(t *testing.T) {
	s := RunBasicJetStreamServer()
	defer s.Shutdown()

	if config := s.JetStreamConfig(); config != nil {
		defer os.RemoveAll(config.StoreDir)
	}

	mname := "DeDupe"
	mset, err := s.GlobalAccount().AddStream(&server.StreamConfig{Name: mname, Storage: server.FileStorage, MaxAge: time.Hour, Subjects: []string{"foo.*"}})
	if err != nil {
		t.Fatalf("Unexpected error adding stream: %v", err)
	}
	defer mset.Delete()

	// Check Duplicates setting.
	duplicates := mset.Config().Duplicates
	if duplicates != server.StreamDefaultDuplicatesWindow {
		t.Fatalf("Expected a default of %v, got %v", server.StreamDefaultDuplicatesWindow, duplicates)
	}

	cfg := mset.Config()
	// Make sure can't be negative.
	cfg.Duplicates = -25 * time.Millisecond
	if err := mset.Update(&cfg); err == nil {
		t.Fatalf("Expected an error but got none")
	}
	// Make sure can't be longer than age if its set.
	cfg.Duplicates = 2 * time.Hour
	if err := mset.Update(&cfg); err == nil {
		t.Fatalf("Expected an error but got none")
	}

	nc := clientConnectToServer(t, s)
	defer nc.Close()

	sendMsg := func(seq uint64, id, msg string) *server.PubAck {
		t.Helper()
		m := nats.NewMsg(fmt.Sprintf("foo.%d", seq))
		m.Header.Add(server.JSPubId, id)
		m.Data = []byte(msg)
		resp, _ := nc.RequestMsg(m, 100*time.Millisecond)
		if resp == nil {
			t.Fatalf("No response for %q, possible timeout?", msg)
		}
		if !bytes.HasPrefix(resp.Data, []byte("+OK {")) {
			t.Fatalf("Expected a JetStreamPubAck, got %q", resp.Data)
		}
		var pubAck server.PubAck
		if err := json.Unmarshal(resp.Data[3:], &pubAck); err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		if pubAck.Seq != seq {
			t.Fatalf("Did not get correct sequence in PubAck, expected %d, got %d", seq, pubAck.Seq)
		}
		return &pubAck
	}

	expect := func(n uint64) {
		t.Helper()
		state := mset.State()
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

	cfg = mset.Config()
	cfg.Duplicates = 25 * time.Millisecond
	if err := mset.Update(&cfg); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	nmids := func(expected int) {
		t.Helper()
		checkFor(t, 200*time.Millisecond, 10*time.Millisecond, func() error {
			if nids := mset.NumMsgIds(); nids != expected {
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
	if err := mset.Update(&cfg); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	mset.Purge()

	// Send 5 new messages.
	sendMsg(10, "AAAA", "Hello DeDupe!")
	sendMsg(11, "BBBB", "Hello DeDupe!")
	sendMsg(12, "CCCC", "Hello DeDupe!")
	sendMsg(13, "DDDD", "Hello DeDupe!")
	sendMsg(14, "EEEE", "Hello DeDupe!")

	// Stop current server.
	sd := s.JetStreamConfig().StoreDir
	s.Shutdown()
	// Restart.
	s = RunJetStreamServerOnPort(-1, sd)
	defer s.Shutdown()

	nc = clientConnectToServer(t, s)
	defer nc.Close()

	mset, _ = s.GlobalAccount().LookupStream(mname)
	if nms := mset.State().Msgs; nms != 5 {
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

	if nms := mset.State().Msgs; nms != 5 {
		t.Fatalf("Expected 5 restored messages, got %d", nms)
	}
	nmids(5)

	// Check we set duplicate properly.
	pa := sendMsg(10, "AAAA", "Hello DeDupe!")
	if !pa.Duplicate {
		t.Fatalf("Expected duplicate to be set")
	}
}

func TestJetStreamPublishExpSeq(t *testing.T) {
	s := RunBasicJetStreamServer()
	defer s.Shutdown()

	if config := s.JetStreamConfig(); config != nil {
		defer os.RemoveAll(config.StoreDir)
	}

	mname := "ExpSeq"
	mset, err := s.GlobalAccount().AddStream(&server.StreamConfig{Name: mname, Storage: server.FileStorage, MaxAge: time.Hour, Subjects: []string{"foo.*"}})
	if err != nil {
		t.Fatalf("Unexpected error adding stream: %v", err)
	}
	defer mset.Delete()

	nc := clientConnectToServer(t, s)
	defer nc.Close()

	sendMsg := func(sub string, exp string, ok bool) {
		t.Helper()
		m := nats.NewMsg(sub)
		if exp != "" {
			m.Header.Add(server.JSExpSeq, exp)
		}
		m.Data = []byte("Hello exp seq")
		resp, _ := nc.RequestMsg(m, 100*time.Millisecond)
		if resp == nil {
			t.Fatalf("No response for %q, possible timeout?", string(m.Data))
		}
		if bytes.HasPrefix(resp.Data, []byte("+OK {")) {
			if !ok {
				t.Fatalf("Expected conflict, but OK")
			}
		} else if string(resp.Data) == "-ERR 'exp-seq conflict'" {
			if ok {
				t.Fatalf("expected OK, not conflict")
			}
			return // expected don't inspect pubAck
		}
		var pubAck server.PubAck
		if err := json.Unmarshal(resp.Data[3:], &pubAck); err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
	}

	expect := func(n uint64) {
		t.Helper()
		state := mset.State()
		if state.Msgs != n {
			t.Fatalf("Expected %d messages, got %d", n, state.Msgs)
		}
	}

	// No exp-seq, works as normal.
	sendMsg("foo.1", "", true) // 1
	sendMsg("foo.2", "", true) // 2
	expect(2)

	// Set stream-scoped sequence
	sendMsg("foo.1", "!2", true) // 3
	sendMsg("foo.2", "!3", true) // 4
	expect(4)

	// Wrong stream-scoped sequence.
	sendMsg("foo.1", "!1", false)
	sendMsg("foo.2", "!2", false)
	expect(4)

	// Subject-level sequence.
	// foo.1 at 3, jump to 5
	sendMsg("foo.1", "3", true) // 5
	sendMsg("foo.1", "5", true) // 6

	// Fail
	sendMsg("foo.1", "3", false) // 5
	sendMsg("foo.1", "5", false) // 6

	// foo.2 at 4, jump to 7
	sendMsg("foo.2", "4", true) // 7
	sendMsg("foo.2", "7", true) // 8
	expect(8)

	// Delete all messages.
	mset.Purge()

	// Subject-level zero value for subject.
	sendMsg("foo.1", "8", true) // 9
	sendMsg("foo.2", "9", true) // 10
	expect(2)

	// Stop current server.
	sd := s.JetStreamConfig().StoreDir
	s.Shutdown()
	// Restart.
	s = RunJetStreamServerOnPort(-1, sd)
	defer s.Shutdown()

	nc = clientConnectToServer(t, s)
	defer nc.Close()

	mset, _ = s.GlobalAccount().LookupStream(mname)
	expect(2)

	// Sequence should work.
	sendMsg("foo.1", "9", true)
	sendMsg("foo.2", "10", true)
	expect(4)
}

func TestJetStreamPullConsumerRemoveInterest(t *testing.T) {
	s := RunBasicJetStreamServer()
	defer s.Shutdown()

	if config := s.JetStreamConfig(); config != nil {
		defer os.RemoveAll(config.StoreDir)
	}

	if config := s.JetStreamConfig(); config != nil {
		defer os.RemoveAll(config.StoreDir)
	}

	mname := "MYS-PULL"
	mset, err := s.GlobalAccount().AddStream(&server.StreamConfig{Name: mname, Storage: server.MemoryStorage})
	if err != nil {
		t.Fatalf("Unexpected error adding stream: %v", err)
	}
	defer mset.Delete()

	wcfg := &server.ConsumerConfig{Durable: "worker", AckPolicy: server.AckExplicit}
	o, err := mset.AddConsumer(wcfg)
	if err != nil {
		t.Fatalf("Expected no error with registered interest, got %v", err)
	}
	rqn := o.RequestNextMsgSubject()
	defer o.Delete()

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
	_, dseq, dc, _ := o.ReplyInfo(msg.Reply)
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
	_, dseq, dc, _ = o.ReplyInfo(msg.Reply)
	if dseq != 2 {
		t.Fatalf("Expected consumer sequence of 2, got %d", dseq)
	}
	if dc != 1 {
		t.Fatalf("Expected delivery count of 1, got %d", dc)
	}
}

func TestJetStreamDeleteStreamManyConsumers(t *testing.T) {
	s := RunBasicJetStreamServer()
	defer s.Shutdown()

	if config := s.JetStreamConfig(); config != nil {
		defer os.RemoveAll(config.StoreDir)
	}

	mname := "MYS"
	mset, err := s.GlobalAccount().AddStream(&server.StreamConfig{Name: mname, Storage: server.FileStorage})
	if err != nil {
		t.Fatalf("Unexpected error adding stream: %v", err)
	}

	// This number needs to be higher than the internal sendq size to trigger what this test is testing.
	for i := 0; i < 2000; i++ {
		_, err := mset.AddConsumer(&server.ConsumerConfig{
			Durable:        fmt.Sprintf("D-%d", i),
			DeliverSubject: fmt.Sprintf("deliver.%d", i),
		})
		if err != nil {
			t.Fatalf("Error creating consumer: %v", err)
		}
	}
	// With bug this would not return and would hang.
	mset.Delete()
}

func TestJetStreamConsumerRateLimit(t *testing.T) {
	s := RunBasicJetStreamServer()
	defer s.Shutdown()

	if config := s.JetStreamConfig(); config != nil {
		defer os.RemoveAll(config.StoreDir)
	}

	mname := "RATELIMIT"
	mset, err := s.GlobalAccount().AddStream(&server.StreamConfig{Name: mname, Storage: server.FileStorage})
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
	state := mset.State()
	if state.Msgs != uint64(toSend) {
		t.Fatalf("Expected %d messages, got %d", toSend, state.Msgs)
	}

	// 100Mbit
	rateLimit := uint64(100 * 1024 * 1024)
	// Make sure if you set a rate with a pull based consumer it errors.
	_, err = mset.AddConsumer(&server.ConsumerConfig{Durable: "to", AckPolicy: server.AckExplicit, RateLimit: rateLimit})
	if err == nil {
		t.Fatalf("Expected an error, got none")
	}

	// Now create one and measure the rate delivered.
	o, err := mset.AddConsumer(&server.ConsumerConfig{
		Durable:        "rate",
		DeliverSubject: "to",
		RateLimit:      rateLimit,
		AckPolicy:      server.AckNone})
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	defer o.Delete()

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
		defer os.RemoveAll(config.StoreDir)
	}

	mname := "MYS"
	mset, err := s.GlobalAccount().AddStream(&server.StreamConfig{Name: mname, Storage: server.FileStorage})
	if err != nil {
		t.Fatalf("Unexpected error adding stream: %v", err)
	}
	defer mset.Delete()

	nc := clientConnectToServer(t, s)
	defer nc.Close()

	sub, _ := nc.SubscribeSync(nats.NewInbox())
	defer sub.Unsubscribe()
	nc.Flush()

	o, err := mset.AddConsumer(&server.ConsumerConfig{
		DeliverSubject: sub.Subject,
		AckPolicy:      server.AckExplicit,
	})
	if err != nil {
		t.Fatalf("Error creating consumer: %v", err)
	}
	defer o.Delete()

	// Snapshot our name.
	oname := o.Name()

	// Send 100 messages
	for i := 0; i < 100; i++ {
		sendStreamMsg(t, nc, mname, "Hello World!")
	}
	if state := mset.State(); state.Msgs != 100 {
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
		// Stop current server.
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

		mset, err = s.GlobalAccount().LookupStream(mname)
		if err != nil {
			t.Fatalf("Expected to find a stream for %q", mname)
		}
		o = mset.LookupConsumer(oname)
		if o == nil {
			t.Fatalf("Error looking up consumer %q", oname)
		}
		// Make sure config does not have durable.
		if cfg := o.Config(); cfg.Durable != "" {
			t.Fatalf("Expected no durable to be set")
		}
		// Wait for it to become active
		checkFor(t, 200*time.Millisecond, 10*time.Millisecond, func() error {
			if !o.Active() {
				return fmt.Errorf("Consumer not active")
			}
			return nil
		})
	}

	// Now close the connection. Make sure this acts like an ephemeral and goes away.
	o.SetInActiveDeleteThreshold(10 * time.Millisecond)
	nc.Close()

	checkFor(t, 200*time.Millisecond, 10*time.Millisecond, func() error {
		if o := mset.LookupConsumer(oname); o != nil {
			return fmt.Errorf("Consumer still active")
		}
		return nil
	})
}

func TestJetStreamConsumerMaxDeliveryAndServerRestart(t *testing.T) {
	s := RunBasicJetStreamServer()
	defer s.Shutdown()

	if config := s.JetStreamConfig(); config != nil {
		defer os.RemoveAll(config.StoreDir)
	}

	mname := "MYS"
	mset, err := s.GlobalAccount().AddStream(&server.StreamConfig{Name: mname, Storage: server.FileStorage})
	if err != nil {
		t.Fatalf("Unexpected error adding stream: %v", err)
	}
	defer mset.Delete()

	streamCreated := mset.Created()

	dsubj := "D.TO"
	max := 4

	o, err := mset.AddConsumer(&server.ConsumerConfig{
		Durable:        "TO",
		DeliverSubject: dsubj,
		AckPolicy:      server.AckExplicit,
		AckWait:        25 * time.Millisecond,
		MaxDeliver:     max,
	})
	defer o.Delete()

	consumerCreated := o.Created()
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
		mset, err = s.GlobalAccount().LookupStream(mname)
		if err != nil {
			t.Fatalf("Expected to find a stream for %q", mname)
		}
		state := mset.State()
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
		// Stop current server.
		s.Shutdown()
		// Restart.
		s = RunJetStreamServerOnPort(port, sd)
	}

	// Restart.
	restartServer()
	defer s.Shutdown()

	checkNumMsgs(2)

	// Wait for client to be reconnected.
	checkFor(t, 2500*time.Millisecond, 5*time.Millisecond, func() error {
		if !nc.IsConnected() {
			return fmt.Errorf("Not connected")
		}
		return nil
	})

	// Once we are here send third order.
	// Send third
	sendStreamMsg(t, nc, mname, "order-3")

	checkNumMsgs(3)

	// Restart.
	restartServer()
	defer s.Shutdown()

	checkNumMsgs(3)

	// Now we should have max times three on our sub.
	checkSubPending(max * 3)

	// Now do some checks on created timestamps.
	mset, err = s.GlobalAccount().LookupStream(mname)
	if err != nil {
		t.Fatalf("Expected to find a stream for %q", mname)
	}
	if mset.Created() != streamCreated {
		t.Fatalf("Stream creation time not restored, wanted %v, got %v", streamCreated, mset.Created())
	}
	o = mset.LookupConsumer("TO")
	if o == nil {
		t.Fatalf("Error looking up consumer: %v", err)
	}
	// Consumer created times can have a very small skew.
	delta := o.Created().Sub(consumerCreated)
	if delta > 5*time.Millisecond {
		t.Fatalf("Consumer creation time not restored, wanted %v, got %v", consumerCreated, o.Created())
	}
}

func TestJetStreamDeleteConsumerAndServerRestart(t *testing.T) {
	s := RunBasicJetStreamServer()
	defer s.Shutdown()

	if config := s.JetStreamConfig(); config != nil {
		defer os.RemoveAll(config.StoreDir)
	}

	sendSubj := "MYQ"
	mset, err := s.GlobalAccount().AddStream(&server.StreamConfig{Name: sendSubj, Storage: server.FileStorage})
	if err != nil {
		t.Fatalf("Unexpected error adding stream: %v", err)
	}
	defer mset.Delete()

	// Create basic work queue mode consumer.
	oname := "WQ"
	o, err := mset.AddConsumer(workerModeConfig(oname))
	if err != nil {
		t.Fatalf("Expected no error with registered interest, got %v", err)
	}

	// Now delete and then we will restart the server.
	o.Delete()

	if numo := mset.NumConsumers(); numo != 0 {
		t.Fatalf("Expected to have zero consumers, got %d", numo)
	}

	// Capture port since it was dynamic.
	u, _ := url.Parse(s.ClientURL())
	port, _ := strconv.Atoi(u.Port())
	sd := s.JetStreamConfig().StoreDir

	// Stop current server.
	s.Shutdown()

	// Restart.
	s = RunJetStreamServerOnPort(port, sd)
	defer s.Shutdown()

	mset, err = s.GlobalAccount().LookupStream(sendSubj)
	if err != nil {
		t.Fatalf("Expected to find a stream for %q", sendSubj)
	}

	if numo := mset.NumConsumers(); numo != 0 {
		t.Fatalf("Expected to have zero consumers, got %d", numo)
	}
}

func TestJetStreamRedeliveryAfterServerRestart(t *testing.T) {
	s := RunBasicJetStreamServer()
	defer s.Shutdown()

	if config := s.JetStreamConfig(); config != nil {
		defer os.RemoveAll(config.StoreDir)
	}

	sendSubj := "MYQ"
	mset, err := s.GlobalAccount().AddStream(&server.StreamConfig{Name: sendSubj, Storage: server.FileStorage})
	if err != nil {
		t.Fatalf("Unexpected error adding stream: %v", err)
	}
	defer mset.Delete()

	nc := clientConnectToServer(t, s)
	defer nc.Close()

	// Now load up some messages.
	toSend := 25
	for i := 0; i < toSend; i++ {
		sendStreamMsg(t, nc, sendSubj, "Hello World!")
	}
	state := mset.State()
	if state.Msgs != uint64(toSend) {
		t.Fatalf("Expected %d messages, got %d", toSend, state.Msgs)
	}

	sub, _ := nc.SubscribeSync(nats.NewInbox())
	defer sub.Unsubscribe()
	nc.Flush()

	o, err := mset.AddConsumer(&server.ConsumerConfig{
		Durable:        "TO",
		DeliverSubject: sub.Subject,
		AckPolicy:      server.AckExplicit,
		AckWait:        100 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	defer o.Delete()

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

	// Stop current server.
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
		defer os.RemoveAll(config.StoreDir)
	}

	mname := "MY-STREAM"
	subjects := []string{"foo", "bar", "baz"}
	cfg := server.StreamConfig{
		Name:     mname,
		Storage:  server.FileStorage,
		Subjects: subjects,
		MaxMsgs:  1000,
	}

	acc := s.GlobalAccount()
	mset, err := acc.AddStream(&cfg)
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
		o, err := mset.AddConsumer(workerModeConfig(cname))
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		// Now grab some messages.
		toReceive := rand.Intn(toSend/2) + 1
		for r := 0; r < toReceive; r++ {
			resp, err := nc.Request(o.RequestNextMsgSubject(), nil, time.Second)
			if err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}
			if resp != nil {
				resp.Respond(nil)
			}
		}
		obs = append(obs, obsi{o.Config(), toReceive})
	}
	nc.Flush()

	// Snapshot state of the stream and consumers.
	info := info{mset.Config(), mset.State(), obs}

	sr, err := mset.Snapshot(5*time.Second, false, true)
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
	if _, err := acc.RestoreStream(mname, r); err == nil {
		t.Fatalf("Expected an error trying to restore existing stream")
	} else if !strings.Contains(err.Error(), "already exists") {
		t.Fatalf("Incorrect error received: %v", err)
	}
	// Now delete so we can restore.
	pusage := acc.JetStreamUsage()
	mset.Delete()
	r.Reset(snapshot)

	// Now send in wrong name
	if _, err := acc.RestoreStream("foo", r); err == nil {
		t.Fatalf("Expected an error trying to restore stream with wrong name")
	}

	r.Reset(snapshot)
	mset, err = acc.RestoreStream(mname, r)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	// Now compare to make sure they are equal.
	if nusage := acc.JetStreamUsage(); nusage != pusage {
		t.Fatalf("Usage does not match after restore: %+v vs %+v", nusage, pusage)
	}
	if state := mset.State(); state != info.state {
		t.Fatalf("State does not match: %+v vs %+v", state, info.state)
	}
	if cfg := mset.Config(); !reflect.DeepEqual(cfg, info.cfg) {
		t.Fatalf("Configs do not match: %+v vs %+v", cfg, info.cfg)
	}
	// Consumers.
	if mset.NumConsumers() != len(info.obs) {
		t.Fatalf("Number of consumers do not match: %d vs %d", mset.NumConsumers(), len(info.obs))
	}
	for _, oi := range info.obs {
		if o := mset.LookupConsumer(oi.cfg.Durable); o != nil {
			if uint64(oi.ack+1) != o.NextSeq() {
				t.Fatalf("Consumer next seq is not correct: %d vs %d", oi.ack+1, o.NextSeq())
			}
		} else {
			t.Fatalf("Expected to get an consumer")
		}
	}

	// Now try restoring to a different server.
	s2 := RunBasicJetStreamServer()
	defer s2.Shutdown()

	if config := s2.JetStreamConfig(); config != nil && config.StoreDir != "" {
		defer os.RemoveAll(config.StoreDir)
	}
	acc = s2.GlobalAccount()
	r.Reset(snapshot)
	mset, err = acc.RestoreStream(mname, r)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	o := mset.LookupConsumer("WQ-1")
	if o == nil {
		t.Fatalf("Could not lookup consumer")
	}

	nc2 := clientConnectToServer(t, s2)
	defer nc2.Close()

	// Make sure we can read messages.
	if _, err := nc2.Request(o.RequestNextMsgSubject(), nil, 5*time.Second); err != nil {
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
	opts.JetStream = true
	rurl, _ := url.Parse(fmt.Sprintf("nats-leaf://%s:%d", lopts.LeafNode.Host, lopts.LeafNode.Port))
	opts.LeafNode.Remotes = []*server.RemoteLeafOpts{{URLs: []*url.URL{rurl}}}

	s := RunServer(&opts)
	defer s.Shutdown()

	checkLeafNodeConnected(t, s)

	if config := s.JetStreamConfig(); config != nil {
		defer os.RemoveAll(config.StoreDir)
	}

	mname := "MY-STREAM"
	subjects := []string{"foo", "bar", "baz"}
	cfg := server.StreamConfig{
		Name:     mname,
		Storage:  server.FileStorage,
		Subjects: subjects,
		MaxMsgs:  1000,
	}

	acc := s.GlobalAccount()
	mset, err := acc.AddStreamWithStore(&cfg, &server.FileStoreConfig{BlockSize: 128})
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

	o, err := mset.AddConsumer(workerModeConfig("WQ"))
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	// Now grab some messages.
	toReceive := rand.Intn(toSend) + 1
	for r := 0; r < toReceive; r++ {
		resp, err := nc.Request(o.RequestNextMsgSubject(), nil, time.Second)
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		if resp != nil {
			resp.Respond(nil)
		}
	}

	// Make sure we get proper error for non-existent request, streams,etc,
	rmsg, err := nc.Request(fmt.Sprintf(server.JSApiStreamSnapshotT, "foo"), nil, time.Second)
	if err != nil {
		t.Fatalf("Unexpected error on snapshot request: %v", err)
	}
	var resp server.JSApiStreamSnapshotResponse
	json.Unmarshal(rmsg.Data, &resp)
	if resp.Error == nil || resp.Error.Code != 400 || resp.Error.Description != "bad request" {
		t.Fatalf("Did not get correct error response: %+v", resp.Error)
	}

	sreq := &server.JSApiStreamSnapshotRequest{}
	req, _ := json.Marshal(sreq)
	rmsg, err = nc.Request(fmt.Sprintf(server.JSApiStreamSnapshotT, "foo"), req, time.Second)
	if err != nil {
		t.Fatalf("Unexpected error on snapshot request: %v", err)
	}
	json.Unmarshal(rmsg.Data, &resp)
	if resp.Error == nil || resp.Error.Code != 404 || resp.Error.Description != "stream not found" {
		t.Fatalf("Did not get correct error response: %+v", resp.Error)
	}

	req, _ = json.Marshal(sreq)
	rmsg, err = nc.Request(fmt.Sprintf(server.JSApiStreamSnapshotT, mname), req, time.Second)
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
	rmsg, err = nc.Request(fmt.Sprintf(server.JSApiStreamSnapshotT, mname), req, time.Second)
	if err != nil {
		t.Fatalf("Unexpected error on snapshot request: %v", err)
	}
	resp.Error = nil
	json.Unmarshal(rmsg.Data, &resp)
	if resp.Error != nil {
		t.Fatalf("Did not get correct error response: %+v", resp.Error)
	}

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
	var rresp server.JSApiStreamRestoreResponse

	// Make sure we get an error since stream still exists.
	rmsg, err = nc.Request(fmt.Sprintf(server.JSApiStreamRestoreT, mname), nil, time.Second)
	if err != nil {
		t.Fatalf("Unexpected error on snapshot request: %v", err)
	}
	json.Unmarshal(rmsg.Data, &rresp)
	if rresp.Error == nil || rresp.Error.Code != 400 || !strings.Contains(rresp.Error.Description, "already exists") {
		t.Fatalf("Did not get correct error response: %+v", rresp.Error)
	}

	// Grab state for comparison.
	state := mset.State()
	// Delete this stream.
	mset.Delete()

	rmsg, err = nc.Request(fmt.Sprintf(server.JSApiStreamRestoreT, mname), nil, time.Second)
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

	mset, err = acc.LookupStream(mname)
	if err != nil {
		t.Fatalf("Expected to find a stream for %q", mname)
	}
	if mset.State() != state {
		t.Fatalf("Did not match states, %+v vs %+v", mset.State(), state)
	}

	// Now ask that the stream be checked first.
	sreq.ChunkSize = 0
	sreq.CheckMsgs = true
	snapshot = snapshot[:0]

	req, _ = json.Marshal(sreq)
	if _, err = nc.Request(fmt.Sprintf(server.JSApiStreamSnapshotT, mname), req, time.Second); err != nil {
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

	snapshot = snapshot[:0]

	req, _ = json.Marshal(sreq)
	rmsg, err = nc2.Request(fmt.Sprintf(server.JSApiStreamSnapshotT, mname), req, time.Second)
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
	mset, err = acc.LookupStream(mname)
	if err != nil {
		t.Fatalf("Expected to find a stream for %q", mname)
	}
	state = mset.State()
	mset.Delete()

	rmsg, err = nc2.Request(fmt.Sprintf(server.JSApiStreamRestoreT, mname), nil, time.Second)
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
	if _, err := nc2.Request(rresp.DeliverSubject, chunk[:n], 50*time.Millisecond); err == nil {
		t.Fatalf("Expected restore subscriptionm to be closed")
	}

	rmsg, err = nc2.Request(fmt.Sprintf(server.JSApiStreamRestoreT, mname), nil, time.Second)
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
	var scResp server.JSApiStreamCreateResponse
	if err := json.Unmarshal(si.Data, &scResp); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if scResp.Error != nil {
		t.Fatalf("Got an unexpected error from EOF omn restore: %+v", scResp.Error)
	}

	if scResp.StreamInfo.State != state {
		t.Fatalf("Did not match states, %+v vs %+v", scResp.StreamInfo.State, state)
	}
}

func TestJetStreamSnapshotsAPIPerf(t *testing.T) {
	// Comment out to run, holding place for now.
	t.SkipNow()

	s := RunBasicJetStreamServer()
	defer s.Shutdown()

	if config := s.JetStreamConfig(); config != nil {
		defer os.RemoveAll(config.StoreDir)
	}

	cfg := server.StreamConfig{
		Name:    "snap-perf",
		Storage: server.FileStorage,
	}

	acc := s.GlobalAccount()
	if _, err := acc.AddStream(&cfg); err != nil {
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

	sreq := &server.JSApiStreamSnapshotRequest{DeliverSubject: nats.NewInbox()}
	req, _ := json.Marshal(sreq)
	rmsg, err := nc.Request(fmt.Sprintf(server.JSApiStreamSnapshotT, "snap-perf"), req, time.Second)
	if err != nil {
		t.Fatalf("Unexpected error on snapshot request: %v", err)
	}

	var resp server.JSApiStreamSnapshotResponse
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
		mconfig *server.StreamConfig
	}{
		{"MemoryStore", &server.StreamConfig{Name: "ADS", Storage: server.MemoryStorage, Subjects: []string{"foo.*"}}},
		{"FileStore", &server.StreamConfig{Name: "ADS", Storage: server.FileStorage, Subjects: []string{"foo.*"}}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			s := RunBasicJetStreamServer()
			defer s.Shutdown()

			if config := s.JetStreamConfig(); config != nil && config.StoreDir != "" {
				defer os.RemoveAll(config.StoreDir)
			}

			mset, err := s.GlobalAccount().AddStream(c.mconfig)
			if err != nil {
				t.Fatalf("Unexpected error adding stream: %v", err)
			}
			defer mset.Delete()

			nc := clientConnectToServer(t, s)
			defer nc.Close()

			// Now load up some messages.
			toSend := 100
			sendSubj := "foo.22"
			for i := 0; i < toSend; i++ {
				sendStreamMsg(t, nc, sendSubj, "Hello World!")
			}
			state := mset.State()
			if state.Msgs != uint64(toSend) {
				t.Fatalf("Expected %d messages, got %d", toSend, state.Msgs)
			}

			o, err := mset.AddConsumer(&server.ConsumerConfig{Durable: "to", DeliverSubject: "d"})
			if err != nil {
				t.Fatalf("Expected no error with registered interest, got %v", err)
			}
			defer o.Delete()

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
		mconfig *server.StreamConfig
	}{
		{"MemoryStore", &server.StreamConfig{Name: "EP", Storage: server.MemoryStorage, Subjects: []string{"foo.*"}}},
		{"FileStore", &server.StreamConfig{Name: "EP", Storage: server.FileStorage, Subjects: []string{"foo.*"}}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			s := RunBasicJetStreamServer()
			defer s.Shutdown()

			if config := s.JetStreamConfig(); config != nil {
				defer os.RemoveAll(config.StoreDir)
			}

			mset, err := s.GlobalAccount().AddStream(c.mconfig)
			if err != nil {
				t.Fatalf("Unexpected error adding stream: %v", err)
			}
			defer mset.Delete()

			nc := clientConnectToServer(t, s)
			defer nc.Close()

			sub, _ := nc.SubscribeSync(nats.NewInbox())
			defer sub.Unsubscribe()
			nc.Flush()

			o, err := mset.AddConsumer(&server.ConsumerConfig{DeliverSubject: sub.Subject})
			if err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}
			if !o.Active() {
				t.Fatalf("Expected the consumer to be considered active")
			}
			if numo := mset.NumConsumers(); numo != 1 {
				t.Fatalf("Expected number of consumers to be 1, got %d", numo)
			}
			// Set our delete threshold to something low for testing purposes.
			o.SetInActiveDeleteThreshold(100 * time.Millisecond)

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
				if o.Active() {
					return fmt.Errorf("Expected the ephemeral consumer to be considered inactive")
				}
				return nil
			})
			// The reason for this still being 1 is that we give some time in case of a reconnect scenario.
			// We detect right away on the interest change but we wait for interest to be re-established.
			// This is in case server goes away but app is fine, we do not want to recycle those consumers.
			if numo := mset.NumConsumers(); numo != 1 {
				t.Fatalf("Expected number of consumers to be 1, got %d", numo)
			}

			// We should delete this one after the delete threshold.
			checkFor(t, time.Second, 100*time.Millisecond, func() error {
				if numo := mset.NumConsumers(); numo != 0 {
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
		mconfig *server.StreamConfig
	}{
		{"MemoryStore", &server.StreamConfig{Name: "ET", Storage: server.MemoryStorage, Subjects: []string{"foo.*"}}},
		{"FileStore", &server.StreamConfig{Name: "ET", Storage: server.FileStorage, Subjects: []string{"foo.*"}}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			s := RunBasicJetStreamServer()
			defer s.Shutdown()

			if config := s.JetStreamConfig(); config != nil {
				defer os.RemoveAll(config.StoreDir)
			}

			mset, err := s.GlobalAccount().AddStream(c.mconfig)
			if err != nil {
				t.Fatalf("Unexpected error adding stream: %v", err)
			}
			defer mset.Delete()

			nc := clientConnectToServer(t, s)
			defer nc.Close()

			sub, _ := nc.SubscribeSync(nats.NewInbox())
			defer sub.Unsubscribe()
			nc.Flush()

			// Capture the subscription.
			delivery := sub.Subject

			o, err := mset.AddConsumer(&server.ConsumerConfig{DeliverSubject: delivery, AckPolicy: server.AckExplicit})
			if err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}
			if !o.Active() {
				t.Fatalf("Expected the consumer to be considered active")
			}
			if numo := mset.NumConsumers(); numo != 1 {
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
				if seq := o.SeqFromReply(m.Reply); seq != uint64(seqno) {
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
					if o.Active() {
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
		mconfig *server.StreamConfig
	}{
		{"MemoryStore", &server.StreamConfig{Name: "DT", Storage: server.MemoryStorage, Subjects: []string{"foo.*"}}},
		{"FileStore", &server.StreamConfig{Name: "DT", Storage: server.FileStorage, Subjects: []string{"foo.*"}}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			s := RunBasicJetStreamServer()
			defer s.Shutdown()

			if config := s.JetStreamConfig(); config != nil {
				defer os.RemoveAll(config.StoreDir)
			}

			mset, err := s.GlobalAccount().AddStream(c.mconfig)
			if err != nil {
				t.Fatalf("Unexpected error adding stream: %v", err)
			}
			defer mset.Delete()

			nc := clientConnectToServer(t, s)
			defer nc.Close()

			dname := "d22"
			subj1 := nats.NewInbox()

			o, err := mset.AddConsumer(&server.ConsumerConfig{
				Durable:        dname,
				DeliverSubject: subj1,
				AckPolicy:      server.AckExplicit,
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
				if seq := o.StreamSeqFromReply(m.Reply); seq != uint64(seqno) {
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
				if o.Active() {
					return fmt.Errorf("Consumer is still active")
				}
				return nil
			})

			// Now we should be able to replace the delivery subject.
			subj2 := nats.NewInbox()
			sub, _ = nc.SubscribeSync(subj2)
			defer sub.Unsubscribe()
			nc.Flush()

			o, err = mset.AddConsumer(&server.ConsumerConfig{
				Durable:        dname,
				DeliverSubject: subj2,
				AckPolicy:      server.AckExplicit,
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
		mconfig *server.StreamConfig
	}{
		{"MemoryStore", &server.StreamConfig{Name: "DT", Storage: server.MemoryStorage, Subjects: []string{"foo.*"}}},
		{"FileStore", &server.StreamConfig{Name: "DT", Storage: server.FileStorage, Subjects: []string{"foo.*"}}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			s := RunBasicJetStreamServer()
			defer s.Shutdown()

			if config := s.JetStreamConfig(); config != nil {
				defer os.RemoveAll(config.StoreDir)
			}

			mset, err := s.GlobalAccount().AddStream(c.mconfig)
			if err != nil {
				t.Fatalf("Unexpected error adding stream: %v", err)
			}
			defer mset.Delete()

			nc := clientConnectToServer(t, s)
			defer nc.Close()

			dname := "d22"
			subj1 := nats.NewInbox()

			o, err := mset.AddConsumer(&server.ConsumerConfig{
				Durable:        dname,
				DeliverSubject: subj1,
				AckPolicy:      server.AckExplicit,
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
				if o.Active() {
					return fmt.Errorf("Consumer is still active")
				}
				return nil
			})

			// Send the second message while delivery subscriber is not running
			sendMsg("2")

			// Now we should be able to replace the delivery subject.
			subj2 := nats.NewInbox()
			o, err = mset.AddConsumer(&server.ConsumerConfig{
				Durable:        dname,
				DeliverSubject: subj2,
				AckPolicy:      server.AckExplicit,
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
				sseq, _, dc, _ := o.ReplyInfo(msg.Reply)
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
		mconfig *server.StreamConfig
	}{
		{"MemoryStore", &server.StreamConfig{Name: "DT", Storage: server.MemoryStorage, Subjects: []string{"foo.*"}}},
		{"FileStore", &server.StreamConfig{Name: "DT", Storage: server.FileStorage, Subjects: []string{"foo.*"}}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			s := RunBasicJetStreamServer()
			defer s.Shutdown()

			if config := s.JetStreamConfig(); config != nil {
				defer os.RemoveAll(config.StoreDir)
			}

			mset, err := s.GlobalAccount().AddStream(c.mconfig)
			if err != nil {
				t.Fatalf("Unexpected error adding stream: %v", err)
			}
			defer mset.Delete()

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
			o, err := mset.AddConsumer(&server.ConsumerConfig{
				Durable:        dname,
				DeliverSubject: dsubj,
				FilterSubject:  "foo.AA",
				DeliverPolicy:  server.DeliverLast,
				AckPolicy:      server.AckExplicit,
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
				rsseq, roseq, dcount, _ := o.ReplyInfo(m.Reply)
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
				_, roseq, dcount, _ := o.ReplyInfo(m.Reply)
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

			state := mset.State()
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
		mconfig *server.StreamConfig
	}{
		{"MemoryStore", &server.StreamConfig{Name: "DC", Storage: server.MemoryStorage}},
		{"FileStore", &server.StreamConfig{Name: "DC", Storage: server.FileStorage}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			s := RunBasicJetStreamServer()
			defer s.Shutdown()

			if config := s.JetStreamConfig(); config != nil {
				defer os.RemoveAll(config.StoreDir)
			}

			mset, err := s.GlobalAccount().AddStream(c.mconfig)
			if err != nil {
				t.Fatalf("Unexpected error adding stream: %v", err)
			}
			defer mset.Delete()

			nc := clientConnectToServer(t, s)
			defer nc.Close()

			// Send lots of msgs and have them queued up.
			for i := 0; i < 10000; i++ {
				nc.Publish("DC", []byte("OK!"))
			}
			nc.Flush()
			if state := mset.State(); state.Msgs != 10000 {
				t.Fatalf("Expected %d messages, got %d", 10000, state.Msgs)
			}

			sub, _ := nc.SubscribeSync(nats.NewInbox())
			sub.SetPendingLimits(-1, -1)
			defer sub.Unsubscribe()
			nc.Flush()

			o, err := mset.AddConsumer(&server.ConsumerConfig{DeliverSubject: sub.Subject})
			if err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}
			defer o.Delete()

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
		mconfig *server.StreamConfig
	}{
		{"MemoryStore", &server.StreamConfig{Name: "DC", Retention: server.WorkQueuePolicy, Storage: server.MemoryStorage}},
		{"FileStore", &server.StreamConfig{Name: "DC", Retention: server.WorkQueuePolicy, Storage: server.FileStorage}},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			s := RunBasicJetStreamServer()
			defer s.Shutdown()

			if config := s.JetStreamConfig(); config != nil {
				defer os.RemoveAll(config.StoreDir)
			}

			mset, err := s.GlobalAccount().AddStream(c.mconfig)
			if err != nil {
				t.Fatalf("Unexpected error adding stream: %v", err)
			}
			defer mset.Delete()

			nc := clientConnectToServer(t, s)
			defer nc.Close()

			for i := 0; i < 10; i++ {
				nc.Publish("DC", []byte("OK!"))
				nc.Flush()
				time.Sleep(time.Millisecond)
			}

			if state := mset.State(); state.Msgs != 10 {
				t.Fatalf("Expected %d messages, got %d", 10, state.Msgs)
			}

			o, err := mset.AddConsumer(workerModeConfig("WQ"))
			if err != nil {
				t.Fatalf("Expected no error with registered interest, got %v", err)
			}
			defer o.Delete()

			for i := uint64(1); i <= 10; i++ {
				m, err := nc.Request(o.RequestNextMsgSubject(), nil, time.Second)
				if err != nil {
					t.Fatalf("Unexpected error: %v", err)
				}

				sseq, dseq, dcount, ts := o.ReplyInfo(m.Reply)

				mreq := &server.JSApiMsgGetRequest{Seq: sseq}
				req, err := json.Marshal(mreq)
				if err != nil {
					t.Fatalf("Unexpected error: %v", err)
				}
				// Load the original message from the stream to verify ReplyInfo ts against stored message
				smsgj, err := nc.Request(fmt.Sprintf(server.JSApiMsgGetT, c.mconfig.Name), req, time.Second)
				if err != nil {
					t.Fatalf("Could not retrieve stream message: %v", err)
				}

				var resp server.JSApiMsgGetResponse
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
				m.Respond(server.AckAck)
			}

			// Now make sure we get right response when message is missing.
			mreq := &server.JSApiMsgGetRequest{Seq: 1}
			req, err := json.Marshal(mreq)
			if err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}
			// Load the original message from the stream to verify ReplyInfo ts against stored message
			rmsg, err := nc.Request(fmt.Sprintf(server.JSApiMsgGetT, c.mconfig.Name), req, time.Second)
			if err != nil {
				t.Fatalf("Could not retrieve stream message: %v", err)
			}
			var resp server.JSApiMsgGetResponse
			err = json.Unmarshal(rmsg.Data, &resp)
			if err != nil {
				t.Fatalf("Could not parse stream message: %v", err)
			}
			if resp.Error == nil || resp.Error.Code != 500 || resp.Error.Description != "no message found" {
				t.Fatalf("Did not get correct error response: %+v", resp.Error)
			}
		})
	}
}
func TestJetStreamRedeliverCount(t *testing.T) {
	cases := []struct {
		name    string
		mconfig *server.StreamConfig
	}{
		{"MemoryStore", &server.StreamConfig{Name: "DC", Storage: server.MemoryStorage}},
		{"FileStore", &server.StreamConfig{Name: "DC", Storage: server.FileStorage}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			s := RunBasicJetStreamServer()
			defer s.Shutdown()

			if config := s.JetStreamConfig(); config != nil {
				defer os.RemoveAll(config.StoreDir)
			}

			mset, err := s.GlobalAccount().AddStream(c.mconfig)
			if err != nil {
				t.Fatalf("Unexpected error adding stream: %v", err)
			}
			defer mset.Delete()

			nc := clientConnectToServer(t, s)
			defer nc.Close()

			// Send 10 msgs
			for i := 0; i < 10; i++ {
				nc.Publish("DC", []byte("OK!"))
			}
			nc.Flush()
			if state := mset.State(); state.Msgs != 10 {
				t.Fatalf("Expected %d messages, got %d", 10, state.Msgs)
			}

			o, err := mset.AddConsumer(workerModeConfig("WQ"))
			if err != nil {
				t.Fatalf("Expected no error with registered interest, got %v", err)
			}
			defer o.Delete()

			for i := uint64(1); i <= 10; i++ {
				m, err := nc.Request(o.RequestNextMsgSubject(), nil, time.Second)
				if err != nil {
					t.Fatalf("Unexpected error: %v", err)
				}

				sseq, dseq, dcount, _ := o.ReplyInfo(m.Reply)

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
				m.Respond(server.AckNak)
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
		defer os.RemoveAll(config.StoreDir)
	}

	mset, err := s.GlobalAccount().AddStream(&server.StreamConfig{Name: "LA", Storage: server.MemoryStorage})
	if err != nil {
		t.Fatalf("Unexpected error adding stream: %v", err)
	}
	defer mset.Delete()

	o, err := mset.AddConsumer(&server.ConsumerConfig{Durable: "DDD", AckPolicy: server.AckExplicit, AckWait: 100 * time.Millisecond})
	if err != nil {
		t.Fatalf("Expected no error with registered interest, got %v", err)
	}
	defer o.Delete()

	nc := clientConnectToServer(t, s)
	defer nc.Close()

	// Queue up message
	sendStreamMsg(t, nc, "LA", "Hello World!")

	nextSubj := o.RequestNextMsgSubject()
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
		defer os.RemoveAll(config.StoreDir)
	}

	mset, err := s.GlobalAccount().AddStream(&server.StreamConfig{Name: "NT", Storage: server.MemoryStorage, Subjects: []string{"ORDERS.*"}})
	if err != nil {
		t.Fatalf("Unexpected error adding stream: %v", err)
	}
	defer mset.Delete()

	o, err := mset.AddConsumer(&server.ConsumerConfig{
		Durable:       "DDD",
		AckPolicy:     server.AckExplicit,
		FilterSubject: "ORDERS.test",
		AckWait:       100 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("Expected no error with registered interest, got %v", err)
	}
	defer o.Delete()

	sendAndReceive := func() {
		nc := clientConnectToServer(t, s)
		defer nc.Close()

		// Queue up message
		sendStreamMsg(t, nc, "ORDERS.test", "Hello World! #1")
		sendStreamMsg(t, nc, "ORDERS.test", "Hello World! #2")

		nextSubj := o.RequestNextMsgSubject()
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
		mconfig *server.StreamConfig
	}{
		{"MemoryStore", &server.StreamConfig{Name: "DC", Storage: server.MemoryStorage}},
		{"FileStore", &server.StreamConfig{Name: "DC", Storage: server.FileStorage}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			s := RunBasicJetStreamServer()
			defer s.Shutdown()

			if config := s.JetStreamConfig(); config != nil {
				defer os.RemoveAll(config.StoreDir)
			}

			mset, err := s.GlobalAccount().AddStream(c.mconfig)
			if err != nil {
				t.Fatalf("Unexpected error adding stream: %v", err)
			}
			defer mset.Delete()

			nc := clientConnectToServer(t, s)
			defer nc.Close()

			// Send 10 msgs
			for i := 0; i < 10; i++ {
				nc.Publish("DC", []byte("OK!"))
			}
			nc.Flush()
			if state := mset.State(); state.Msgs != 10 {
				t.Fatalf("Expected %d messages, got %d", 10, state.Msgs)
			}

			o, err := mset.AddConsumer(workerModeConfig("WQ"))
			if err != nil {
				t.Fatalf("Expected no error with registered interest, got %v", err)
			}
			defer o.Delete()

			for i := uint64(1); i <= 10; i++ {
				m, err := nc.Request(o.RequestNextMsgSubject(), nil, time.Second)
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
				if err := nc.Publish(fmt.Sprintf(ackReplyT, seq, seq), server.AckNak); err != nil {
					t.Fatalf("Error sending nak: %v", err)
				}
				nc.Flush()
				if _, err := nc.Request(o.RequestNextMsgSubject(), nil, 10*time.Millisecond); err != nats.ErrTimeout {
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
		mconfig *server.StreamConfig
	}{
		{"MemoryStore", &server.StreamConfig{Name: "DC", Storage: server.MemoryStorage}},
		{"FileStore", &server.StreamConfig{Name: "DC", Storage: server.FileStorage}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			s := RunBasicJetStreamServer()
			defer s.Shutdown()

			if config := s.JetStreamConfig(); config != nil {
				defer os.RemoveAll(config.StoreDir)
			}

			mset, err := s.GlobalAccount().AddStream(c.mconfig)
			if err != nil {
				t.Fatalf("Unexpected error adding stream: %v", err)
			}
			defer mset.Delete()

			nc := clientConnectToServer(t, s)
			defer nc.Close()

			// Send 100 msgs
			for i := 0; i < 100; i++ {
				nc.Publish("DC", []byte("OK!"))
			}
			nc.Flush()
			if state := mset.State(); state.Msgs != 100 {
				t.Fatalf("Expected %d messages, got %d", 100, state.Msgs)
			}
			mset.Purge()
			state := mset.State()
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

			state = mset.State()
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
		mconfig *server.StreamConfig
	}{
		{"MemoryStore", &server.StreamConfig{Name: "DC", Storage: server.MemoryStorage}},
		{"FileStore", &server.StreamConfig{Name: "DC", Storage: server.FileStorage}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			s := RunBasicJetStreamServer()
			defer s.Shutdown()

			if config := s.JetStreamConfig(); config != nil {
				defer os.RemoveAll(config.StoreDir)
			}

			mset, err := s.GlobalAccount().AddStream(c.mconfig)
			if err != nil {
				t.Fatalf("Unexpected error adding stream: %v", err)
			}
			defer mset.Delete()

			nc := clientConnectToServer(t, s)
			defer nc.Close()

			// Send 100 msgs
			for i := 0; i < 100; i++ {
				nc.Publish("DC", []byte("OK!"))
			}
			nc.Flush()
			if state := mset.State(); state.Msgs != 100 {
				t.Fatalf("Expected %d messages, got %d", 100, state.Msgs)
			}
			// Now create an consumer and make sure it functions properly.
			o, err := mset.AddConsumer(workerModeConfig("WQ"))
			if err != nil {
				t.Fatalf("Expected no error with registered interest, got %v", err)
			}
			defer o.Delete()
			nextSubj := o.RequestNextMsgSubject()
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
			state := o.Info()
			if state.AckFloor.ConsumerSeq != 50 {
				t.Fatalf("Expected ack floor of 50, got %d", state.AckFloor.ConsumerSeq)
			}
			if state.NumPending != 25 {
				t.Fatalf("Expected len(pending) to be 25, got %d", state.NumPending)
			}
			// Now do purge.
			mset.Purge()
			if state := mset.State(); state.Msgs != 0 {
				t.Fatalf("Expected %d messages, got %d", 0, state.Msgs)
			}
			// Now re-acquire state and check that we did the right thing.
			// Pending should be cleared, and stream sequences should have been set
			// to the total messages before purge + 1.
			state = o.Info()
			if state.NumPending != 0 {
				t.Fatalf("Expected no pending, got %d", state.NumPending)
			}
			if state.Delivered.StreamSeq != 100 {
				t.Fatalf("Expected to have setseq now at next seq of 100, got %d", state.Delivered.StreamSeq)
			}
			// Check AckFloors which should have also been adjusted.
			if state.AckFloor.StreamSeq != 100 {
				t.Fatalf("Expected ackfloor for setseq to be 100, got %d", state.AckFloor.StreamSeq)
			}
			if state.AckFloor.ConsumerSeq != 75 {
				t.Fatalf("Expected ackfloor for obsseq to be 75, got %d", state.AckFloor.ConsumerSeq)
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
		mconfig *server.StreamConfig
	}{
		{"MemoryStore", &server.StreamConfig{Name: "DC", Storage: server.MemoryStorage}},
		{"FileStore", &server.StreamConfig{Name: "DC", Storage: server.FileStorage}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			s := RunBasicJetStreamServer()
			defer s.Shutdown()

			if config := s.JetStreamConfig(); config != nil {
				defer os.RemoveAll(config.StoreDir)
			}

			mset, err := s.GlobalAccount().AddStream(c.mconfig)
			if err != nil {
				t.Fatalf("Unexpected error adding stream: %v", err)
			}
			defer mset.Delete()

			nc := clientConnectToServer(t, s)
			defer nc.Close()

			// Send 100 msgs
			for i := 0; i < 100; i++ {
				nc.Publish("DC", []byte("OK!"))
			}
			nc.Flush()
			if state := mset.State(); state.Msgs != 100 {
				t.Fatalf("Expected %d messages, got %d", 100, state.Msgs)
			}
			// Now create an consumer and make sure it functions properly.
			// This will test redelivery state and purge of the stream.
			wcfg := &server.ConsumerConfig{
				Durable:   "WQ",
				AckPolicy: server.AckExplicit,
				AckWait:   20 * time.Millisecond,
			}
			o, err := mset.AddConsumer(wcfg)
			if err != nil {
				t.Fatalf("Expected no error with registered interest, got %v", err)
			}
			defer o.Delete()
			nextSubj := o.RequestNextMsgSubject()
			for i := 0; i < 50; i++ {
				// Do not ack these.
				if _, err := nc.Request(nextSubj, nil, time.Second); err != nil {
					t.Fatalf("Unexpected error: %v", err)
				}
			}
			// Now wait to make sure we are in a redelivered state.
			time.Sleep(wcfg.AckWait * 2)
			// Now do purge.
			mset.Purge()
			if state := mset.State(); state.Msgs != 0 {
				t.Fatalf("Expected %d messages, got %d", 0, state.Msgs)
			}
			// Now get the state and check that we did the right thing.
			// Pending should be cleared, and stream sequences should have been set
			// to the total messages before purge + 1.
			state := o.Info()
			if state.NumPending != 0 {
				t.Fatalf("Expected no pending, got %d", state.NumPending)
			}
			if state.Delivered.StreamSeq != 100 {
				t.Fatalf("Expected to have setseq now at next seq of 100, got %d", state.Delivered.StreamSeq)
			}
			// Check AckFloors which should have also been adjusted.
			if state.AckFloor.StreamSeq != 100 {
				t.Fatalf("Expected ackfloor for setseq to be 100, got %d", state.AckFloor.StreamSeq)
			}
			if state.AckFloor.ConsumerSeq != 50 {
				t.Fatalf("Expected ackfloor for obsseq to be 75, got %d", state.AckFloor.ConsumerSeq)
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
		mconfig *server.StreamConfig
	}{
		{"MemoryStore", &server.StreamConfig{Name: "DC", Storage: server.MemoryStorage, Retention: server.InterestPolicy}},
		{"FileStore", &server.StreamConfig{Name: "DC", Storage: server.FileStorage, Retention: server.InterestPolicy}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			s := RunBasicJetStreamServer()
			defer s.Shutdown()

			if config := s.JetStreamConfig(); config != nil {
				defer os.RemoveAll(config.StoreDir)
			}

			mset, err := s.GlobalAccount().AddStream(c.mconfig)
			if err != nil {
				t.Fatalf("Unexpected error adding stream: %v", err)
			}
			defer mset.Delete()

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
				if state := mset.State(); state.Msgs != uint64(numExpected) {
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
			mset.AddConsumer(&server.ConsumerConfig{DeliverSubject: sub1.Subject, AckPolicy: server.AckExplicit})

			sub2 := syncSub()
			mset.AddConsumer(&server.ConsumerConfig{DeliverSubject: sub2.Subject, AckPolicy: server.AckAll})

			sub3 := syncSub()
			mset.AddConsumer(&server.ConsumerConfig{DeliverSubject: sub3.Subject, AckPolicy: server.AckNone})

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

func TestJetStreamInterestRetentionStreamWithDurableRestart(t *testing.T) {
	cases := []struct {
		name    string
		mconfig *server.StreamConfig
	}{
		{"MemoryStore", &server.StreamConfig{Name: "IK", Storage: server.MemoryStorage, Retention: server.InterestPolicy}},
		{"FileStore", &server.StreamConfig{Name: "IK", Storage: server.FileStorage, Retention: server.InterestPolicy}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			s := RunBasicJetStreamServer()
			defer s.Shutdown()

			if config := s.JetStreamConfig(); config != nil {
				defer os.RemoveAll(config.StoreDir)
			}

			mset, err := s.GlobalAccount().AddStream(c.mconfig)
			if err != nil {
				t.Fatalf("Unexpected error adding stream: %v", err)
			}
			defer mset.Delete()

			checkNumMsgs := func(numExpected int) {
				t.Helper()
				if state := mset.State(); state.Msgs != uint64(numExpected) {
					t.Fatalf("Expected %d messages, got %d", numExpected, state.Msgs)
				}
			}

			nc := clientConnectToServer(t, s)
			defer nc.Close()

			sub, _ := nc.SubscribeSync(nats.NewInbox())
			nc.Flush()

			cfg := &server.ConsumerConfig{Durable: "ivan", DeliverPolicy: server.DeliverNew, DeliverSubject: sub.Subject, AckPolicy: server.AckNone}

			o, _ := mset.AddConsumer(cfg)

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
				if o.Active() {
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
			if o, err = mset.AddConsumer(cfg); err != nil {
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
				if o.Active() {
					return fmt.Errorf("Still active consumer")
				}
				return nil
			})
			o.Delete()

			sub, _ = nc.SubscribeSync(nats.NewInbox())
			nc.Flush()

			cfg.DeliverSubject = sub.Subject
			cfg.AckPolicy = server.AckExplicit // Set ack
			if o, err = mset.AddConsumer(cfg); err != nil {
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
			o2, err := mset.AddConsumer(cfg)
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
			o.Delete()
			checkNumMsgs(10)

			o2.Delete()
			checkNumMsgs(0)
		})
	}
}

func TestJetStreamConsumerReplayRate(t *testing.T) {
	cases := []struct {
		name    string
		mconfig *server.StreamConfig
	}{
		{"MemoryStore", &server.StreamConfig{Name: "DC", Storage: server.MemoryStorage}},
		{"FileStore", &server.StreamConfig{Name: "DC", Storage: server.FileStorage}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			s := RunBasicJetStreamServer()
			defer s.Shutdown()

			if config := s.JetStreamConfig(); config != nil {
				defer os.RemoveAll(config.StoreDir)
			}

			mset, err := s.GlobalAccount().AddStream(c.mconfig)
			if err != nil {
				t.Fatalf("Unexpected error adding stream: %v", err)
			}
			defer mset.Delete()

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

			if state := mset.State(); state.Msgs != uint64(totalMsgs) {
				t.Fatalf("Expected %d messages, got %d", totalMsgs, state.Msgs)
			}

			sub, _ := nc.SubscribeSync(nats.NewInbox())
			defer sub.Unsubscribe()
			nc.Flush()

			o, err := mset.AddConsumer(&server.ConsumerConfig{DeliverSubject: sub.Subject})
			if err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}
			defer o.Delete()

			// Firehose/instant which is default.
			last := time.Now()
			for i := 0; i < totalMsgs; i++ {
				if _, err := sub.NextMsg(time.Second); err != nil {
					t.Fatalf("Unexpected error: %v", err)
				}
				now := time.Now()
				// Delivery from AddConsumer starts in a go routine, so be
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
			o, err = mset.AddConsumer(&server.ConsumerConfig{DeliverSubject: sub.Subject, ReplayPolicy: server.ReplayOriginal})
			if err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}
			defer o.Delete()

			// Original rate messsages were received for push based consumer.
			for i := 0; i < totalMsgs; i++ {
				start := time.Now()
				if _, err := sub.NextMsg(time.Second); err != nil {
					t.Fatalf("Unexpected error: %v", err)
				}
				gap := time.Since(start)
				// 10ms is high but on macs time.Sleep(delay) does not sleep only delay.
				gl, gh := gaps[i]-5*time.Millisecond, gaps[i]+10*time.Millisecond
				if gap < gl || gap > gh {
					t.Fatalf("Gap is off for %d, expected %v got %v", i, gaps[i], gap)
				}
			}

			// Now create pull based.
			oc := workerModeConfig("PM")
			oc.ReplayPolicy = server.ReplayOriginal
			o, err = mset.AddConsumer(oc)
			if err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}
			defer o.Delete()

			for i := 0; i < totalMsgs; i++ {
				start := time.Now()
				if _, err := nc.Request(o.RequestNextMsgSubject(), nil, time.Second); err != nil {
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
		mconfig *server.StreamConfig
	}{
		{"MemoryStore", &server.StreamConfig{Name: "DC", Storage: server.MemoryStorage}},
		{"FileStore", &server.StreamConfig{Name: "DC", Storage: server.FileStorage}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			s := RunBasicJetStreamServer()
			defer s.Shutdown()

			if config := s.JetStreamConfig(); config != nil {
				defer os.RemoveAll(config.StoreDir)
			}

			mset, err := s.GlobalAccount().AddStream(c.mconfig)
			if err != nil {
				t.Fatalf("Unexpected error adding stream: %v", err)
			}
			defer mset.Delete()

			nc := clientConnectToServer(t, s)
			defer nc.Close()

			// Send 10 msgs
			totalMsgs := 10
			for i := 0; i < totalMsgs; i++ {
				nc.Request("DC", []byte("Hello World"), time.Second)
				time.Sleep(time.Duration(rand.Intn(5)) * time.Millisecond)
			}
			if state := mset.State(); state.Msgs != uint64(totalMsgs) {
				t.Fatalf("Expected %d messages, got %d", totalMsgs, state.Msgs)
			}
			subj := "d.dc"
			o, err := mset.AddConsumer(&server.ConsumerConfig{
				Durable:        "derek",
				DeliverSubject: subj,
				AckPolicy:      server.AckNone,
				ReplayPolicy:   server.ReplayOriginal,
			})
			if err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}
			defer o.Delete()
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
		mconfig *server.StreamConfig
	}{
		{"MemoryStore", &server.StreamConfig{Name: "DC", Storage: server.MemoryStorage}},
		{"FileStore", &server.StreamConfig{Name: "DC", Storage: server.FileStorage}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			s := RunBasicJetStreamServer()
			defer s.Shutdown()

			if config := s.JetStreamConfig(); config != nil {
				defer os.RemoveAll(config.StoreDir)
			}

			mset, err := s.GlobalAccount().AddStream(c.mconfig)
			if err != nil {
				t.Fatalf("Unexpected error adding stream: %v", err)
			}
			defer mset.Delete()

			nc := clientConnectToServer(t, s)
			defer nc.Close()

			// Send 2 msgs
			nc.Request("DC", []byte("OK!"), time.Second)
			time.Sleep(100 * time.Millisecond)
			nc.Request("DC", []byte("OK!"), time.Second)

			if state := mset.State(); state.Msgs != 2 {
				t.Fatalf("Expected %d messages, got %d", 2, state.Msgs)
			}

			sub, _ := nc.SubscribeSync(nats.NewInbox())
			defer sub.Unsubscribe()
			nc.Flush()

			// Now do replay rate to match original.
			o, err := mset.AddConsumer(&server.ConsumerConfig{DeliverSubject: sub.Subject, ReplayPolicy: server.ReplayOriginal})
			if err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}

			// Allow loop and deliver / replay go routine to spin up
			time.Sleep(50 * time.Millisecond)
			base := runtime.NumGoroutine()
			o.Delete()

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
		defer os.RemoveAll(config.StoreDir)
	}

	if _, _, err := s.JetStreamReservedResources(); err == nil {
		t.Fatalf("Expected error requesting jetstream reserved resources when not enabled")
	}
	// Create some accounts.
	facc, _ := s.LookupOrRegisterAccount("FOO")
	bacc, _ := s.LookupOrRegisterAccount("BAR")
	zacc, _ := s.LookupOrRegisterAccount("BAZ")

	jsconfig := &server.JetStreamConfig{MaxMemory: 1024, MaxStore: 8192}
	if err := s.EnableJetStream(jsconfig); err != nil {
		t.Fatalf("Expected no error, got %v", err)
	}

	if rm, rd, err := s.JetStreamReservedResources(); err != nil {
		t.Fatalf("Unexpected error requesting jetstream reserved resources: %v", err)
	} else if rm != 0 || rd != 0 {
		t.Fatalf("Expected reserved memory and store to be 0, got %d and %d", rm, rd)
	}

	limits := func(mem int64, store int64) *server.JetStreamAccountLimits {
		return &server.JetStreamAccountLimits{
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
		t.Fatalf("Expected reserved memory and store to be 0, got %v and %v", server.FriendlyBytes(rm), server.FriendlyBytes(rd))
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

	var msets []*server.Stream
	// Now test max streams and max consumers. Note max consumers is per stream.
	for i := 0; i < 10; i++ {
		mname := fmt.Sprintf("foo.%d", i)
		mset, err := facc.AddStream(&server.StreamConfig{Name: strconv.Itoa(i), Subjects: []string{mname}})
		if err != nil {
			t.Fatalf("Unexpected error adding stream: %v", err)
		}
		msets = append(msets, mset)
	}

	// This one should fail since over the limit for max number of streams.
	if _, err := facc.AddStream(&server.StreamConfig{Name: "22", Subjects: []string{"foo.22"}}); err == nil {
		t.Fatalf("Expected error adding stream over limit")
	}

	// Remove them all
	for _, mset := range msets {
		mset.Delete()
	}

	// Now try to add one with bytes limit that would exceed the account limit.
	if _, err := facc.AddStream(&server.StreamConfig{Name: "22", MaxBytes: jsconfig.MaxMemory * 2}); err == nil {
		t.Fatalf("Expected error adding stream over limit")
	}

	// Replicas can't be > 1
	if _, err := facc.AddStream(&server.StreamConfig{Name: "22", Replicas: 10}); err == nil {
		t.Fatalf("Expected error adding stream over limit")
	}

	// Test consumers limit against account limit when the stream does not set a limit
	mset, err := facc.AddStream(&server.StreamConfig{Name: "22", Subjects: []string{"foo.22"}})
	if err != nil {
		t.Fatalf("Unexpected error adding stream: %v", err)
	}

	for i := 0; i < 10; i++ {
		oname := fmt.Sprintf("O:%d", i)
		_, err := mset.AddConsumer(&server.ConsumerConfig{Durable: oname, AckPolicy: server.AckExplicit})
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
	}

	// This one should fail.
	if _, err := mset.AddConsumer(&server.ConsumerConfig{Durable: "O:22", AckPolicy: server.AckExplicit}); err == nil {
		t.Fatalf("Expected error adding consumer over the limit")
	}

	// Test consumer limit against stream limit
	mset.Delete()
	mset, err = facc.AddStream(&server.StreamConfig{Name: "22", Subjects: []string{"foo.22"}, MaxConsumers: 5})
	if err != nil {
		t.Fatalf("Unexpected error adding stream: %v", err)
	}

	for i := 0; i < 5; i++ {
		oname := fmt.Sprintf("O:%d", i)
		_, err := mset.AddConsumer(&server.ConsumerConfig{Durable: oname, AckPolicy: server.AckExplicit})
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
	}

	// This one should fail.
	if _, err := mset.AddConsumer(&server.ConsumerConfig{Durable: "O:22", AckPolicy: server.AckExplicit}); err == nil {
		t.Fatalf("Expected error adding consumer over the limit")
	}

	// Test the account having smaller limits than the stream
	mset.Delete()

	mset, err = facc.AddStream(&server.StreamConfig{Name: "22", Subjects: []string{"foo.22"}, MaxConsumers: 10})
	if err != nil {
		t.Fatalf("Unexpected error adding stream: %v", err)
	}

	l.MaxConsumers = 5
	if err := facc.UpdateJetStreamLimits(l); err != nil {
		t.Fatalf("Unexpected error updating jetstream account limits: %v", err)
	}

	for i := 0; i < 5; i++ {
		oname := fmt.Sprintf("O:%d", i)
		_, err := mset.AddConsumer(&server.ConsumerConfig{Durable: oname, AckPolicy: server.AckExplicit})
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
	}

	// This one should fail.
	if _, err := mset.AddConsumer(&server.ConsumerConfig{Durable: "O:22", AckPolicy: server.AckExplicit}); err == nil {
		t.Fatalf("Expected error adding consumer over the limit")
	}

}

func TestJetStreamStreamStorageTrackingAndLimits(t *testing.T) {
	s := RunBasicJetStreamServer()
	defer s.Shutdown()

	if config := s.JetStreamConfig(); config != nil {
		defer os.RemoveAll(config.StoreDir)
	}

	gacc := s.GlobalAccount()

	al := &server.JetStreamAccountLimits{
		MaxMemory:    8192,
		MaxStore:     -1,
		MaxStreams:   -1,
		MaxConsumers: -1,
	}

	if err := gacc.UpdateJetStreamLimits(al); err != nil {
		t.Fatalf("Unexpected error updating jetstream account limits: %v", err)
	}

	mset, err := gacc.AddStream(&server.StreamConfig{Name: "LIMITS", Retention: server.WorkQueuePolicy})
	if err != nil {
		t.Fatalf("Unexpected error adding stream: %v", err)
	}
	defer mset.Delete()

	nc := clientConnectToServer(t, s)
	defer nc.Close()

	toSend := 100
	for i := 0; i < toSend; i++ {
		sendStreamMsg(t, nc, "LIMITS", "Hello World!")
	}

	state := mset.State()
	usage := gacc.JetStreamUsage()

	// Make sure these are working correctly.
	if state.Bytes != usage.Memory {
		t.Fatalf("Expected to have stream bytes match memory usage, %d vs %d", state.Bytes, usage.Memory)
	}
	if usage.Streams != 1 {
		t.Fatalf("Expected to have 1 stream, got %d", usage.Streams)
	}

	// Do second stream.
	mset2, err := gacc.AddStream(&server.StreamConfig{Name: "NUM22", Retention: server.WorkQueuePolicy})
	if err != nil {
		t.Fatalf("Unexpected error adding stream: %v", err)
	}
	defer mset2.Delete()

	for i := 0; i < toSend; i++ {
		sendStreamMsg(t, nc, "NUM22", "Hello World!")
	}

	stats2 := mset2.State()
	usage = gacc.JetStreamUsage()

	if usage.Memory != (state.Bytes + stats2.Bytes) {
		t.Fatalf("Expected to track both streams, account is %v, stream1 is %v, stream2 is %v", usage.Memory, state.Bytes, stats2.Bytes)
	}

	// Make sure delete works.
	mset2.Delete()
	stats2 = mset2.State()
	usage = gacc.JetStreamUsage()

	if usage.Memory != (state.Bytes + stats2.Bytes) {
		t.Fatalf("Expected to track both streams, account is %v, stream1 is %v, stream2 is %v", usage.Memory, state.Bytes, stats2.Bytes)
	}

	// Now drain the first one by consuming the messages.
	o, err := mset.AddConsumer(workerModeConfig("WQ"))
	if err != nil {
		t.Fatalf("Expected no error with registered interest, got %v", err)
	}
	defer o.Delete()

	for i := 0; i < toSend; i++ {
		msg, err := nc.Request(o.RequestNextMsgSubject(), nil, time.Second)
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		msg.Respond(nil)
	}
	nc.Flush()

	state = mset.State()
	usage = gacc.JetStreamUsage()

	if usage.Memory != 0 {
		t.Fatalf("Expected usage memeory to be 0, got %d", usage.Memory)
	}

	// Now send twice the number of messages. Should receive an error at some point, and we will check usage against limits.
	var errSeen string
	for i := 0; i < toSend*2; i++ {
		resp, _ := nc.Request("LIMITS", []byte("The quick brown fox jumped over the..."), 50*time.Millisecond)
		if string(resp.Data) != server.OK {
			errSeen = string(resp.Data)
			break
		}
	}

	if errSeen == "" {
		t.Fatalf("Expected to see an error when exceeding the account limits")
	}

	state = mset.State()
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

func TestJetStreamStreamFileStorageTrackingAndLimits(t *testing.T) {
	s := RunBasicJetStreamServer()
	defer s.Shutdown()

	if config := s.JetStreamConfig(); config != nil {
		defer os.RemoveAll(config.StoreDir)
	}

	gacc := s.GlobalAccount()

	al := &server.JetStreamAccountLimits{
		MaxMemory:    8192,
		MaxStore:     9600,
		MaxStreams:   -1,
		MaxConsumers: -1,
	}

	if err := gacc.UpdateJetStreamLimits(al); err != nil {
		t.Fatalf("Unexpected error updating jetstream account limits: %v", err)
	}

	mconfig := &server.StreamConfig{Name: "LIMITS", Storage: server.FileStorage, Retention: server.WorkQueuePolicy}
	mset, err := gacc.AddStream(mconfig)
	if err != nil {
		t.Fatalf("Unexpected error adding stream: %v", err)
	}
	defer mset.Delete()

	nc := clientConnectToServer(t, s)
	defer nc.Close()

	toSend := 100
	for i := 0; i < toSend; i++ {
		sendStreamMsg(t, nc, "LIMITS", "Hello World!")
	}

	state := mset.State()
	usage := gacc.JetStreamUsage()

	// Make sure these are working correctly.
	if usage.Store != state.Bytes {
		t.Fatalf("Expected to have stream bytes match the store usage, %d vs %d", usage.Store, state.Bytes)
	}
	if usage.Streams != 1 {
		t.Fatalf("Expected to have 1 stream, got %d", usage.Streams)
	}

	// Do second stream.
	mconfig2 := &server.StreamConfig{Name: "NUM22", Storage: server.FileStorage, Retention: server.WorkQueuePolicy}
	mset2, err := gacc.AddStream(mconfig2)
	if err != nil {
		t.Fatalf("Unexpected error adding stream: %v", err)
	}
	defer mset2.Delete()

	for i := 0; i < toSend; i++ {
		sendStreamMsg(t, nc, "NUM22", "Hello World!")
	}

	stats2 := mset2.State()
	usage = gacc.JetStreamUsage()

	if usage.Store != (state.Bytes + stats2.Bytes) {
		t.Fatalf("Expected to track both streams, usage is %v, stream1 is %v, stream2 is %v", usage.Store, state.Bytes, stats2.Bytes)
	}

	// Make sure delete works.
	mset2.Delete()
	stats2 = mset2.State()
	usage = gacc.JetStreamUsage()

	if usage.Store != (state.Bytes + stats2.Bytes) {
		t.Fatalf("Expected to track both streams, account is %v, stream1 is %v, stream2 is %v", usage.Store, state.Bytes, stats2.Bytes)
	}

	// Now drain the first one by consuming the messages.
	o, err := mset.AddConsumer(workerModeConfig("WQ"))
	if err != nil {
		t.Fatalf("Expected no error with registered interest, got %v", err)
	}
	defer o.Delete()

	for i := 0; i < toSend; i++ {
		msg, err := nc.Request(o.RequestNextMsgSubject(), nil, time.Second)
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		msg.Respond(nil)
	}
	nc.Flush()

	state = mset.State()
	usage = gacc.JetStreamUsage()

	if usage.Memory != 0 {
		t.Fatalf("Expected usage memeory to be 0, got %d", usage.Memory)
	}

	// Now send twice the number of messages. Should receive an error at some point, and we will check usage against limits.
	var errSeen string
	for i := 0; i < toSend*2; i++ {
		resp, _ := nc.Request("LIMITS", []byte("The quick brown fox jumped over the..."), 50*time.Millisecond)
		if string(resp.Data) != server.OK {
			errSeen = string(resp.Data)
			break
		}
	}

	if errSeen == "" {
		t.Fatalf("Expected to see an error when exceeding the account limits")
	}

	state = mset.State()
	usage = gacc.JetStreamUsage()

	if usage.Memory > uint64(al.MaxMemory) {
		t.Fatalf("Expected memory to not exceed limit of %d, got %d", al.MaxMemory, usage.Memory)
	}
}

type obsi struct {
	cfg server.ConsumerConfig
	ack int
}
type info struct {
	cfg   server.StreamConfig
	state server.StreamState
	obs   []obsi
}

func TestJetStreamSimpleFileStorageRecovery(t *testing.T) {
	base := runtime.NumGoroutine()

	s := RunBasicJetStreamServer()
	defer s.Shutdown()

	if config := s.JetStreamConfig(); config != nil {
		defer os.RemoveAll(config.StoreDir)
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
		msetConfig := server.StreamConfig{
			Name:     msetName,
			Storage:  server.FileStorage,
			Subjects: subjects,
			MaxMsgs:  100,
		}
		mset, err := acc.AddStream(&msetConfig)
		if err != nil {
			t.Fatalf("Unexpected error adding stream: %v", err)
		}
		defer mset.Delete()

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
			o, err := mset.AddConsumer(workerModeConfig(oname))
			if err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}
			// Now grab some messages.
			toReceive := rand.Intn(toSend) + 1
			for r := 0; r < toReceive; r++ {
				resp, _ := nc.Request(o.RequestNextMsgSubject(), nil, time.Second)
				if resp != nil {
					resp.Respond(nil)
				}
			}
			obs = append(obs, obsi{o.Config(), toReceive})
		}
		ostate[msetName] = info{mset.Config(), mset.State(), obs}
	}
	pusage := acc.JetStreamUsage()

	// Shutdown the server. Restart and make sure things come back.
	// Capture port since it was dynamic.
	u, _ := url.Parse(s.ClientURL())
	port, _ := strconv.Atoi(u.Port())
	sd := s.JetStreamConfig().StoreDir

	s.Shutdown()

	checkFor(t, 2*time.Second, 100*time.Millisecond, func() error {
		delta := (runtime.NumGoroutine() - base)
		if delta > 3 {
			return fmt.Errorf("%d Go routines still exist post Shutdown()", delta)
		}
		return nil
	})

	s = RunJetStreamServerOnPort(port, sd)
	defer s.Shutdown()

	acc = s.GlobalAccount()

	nusage := acc.JetStreamUsage()
	if nusage != pusage {
		t.Fatalf("Usage does not match after restore: %+v vs %+v", nusage, pusage)
	}

	for mname, info := range ostate {
		mset, err := acc.LookupStream(mname)
		if err != nil {
			t.Fatalf("Expected to find a stream for %q", mname)
		}
		if state := mset.State(); state != info.state {
			t.Fatalf("State does not match: %+v vs %+v", state, info.state)
		}
		if cfg := mset.Config(); !reflect.DeepEqual(cfg, info.cfg) {
			t.Fatalf("Configs do not match: %+v vs %+v", cfg, info.cfg)
		}
		// Consumers.
		if mset.NumConsumers() != len(info.obs) {
			t.Fatalf("Number of consumers do not match: %d vs %d", mset.NumConsumers(), len(info.obs))
		}
		for _, oi := range info.obs {
			if o := mset.LookupConsumer(oi.cfg.Durable); o != nil {
				if uint64(oi.ack+1) != o.NextSeq() {
					t.Fatalf("Consumer next seq is not correct: %d vs %d", oi.ack+1, o.NextSeq())
				}
			} else {
				t.Fatalf("Expected to get an consumer")
			}
		}
	}
}

func TestJetStreamRequestAPI(t *testing.T) {
	s := RunBasicJetStreamServer()
	defer s.Shutdown()

	// Forced cleanup of all persisted state.
	if config := s.JetStreamConfig(); config != nil {
		defer os.RemoveAll(config.StoreDir)
	}

	// Client for API requests.
	nc := clientConnectToServer(t, s)
	defer nc.Close()

	// This will get the current information about usage and limits for this account.
	resp, err := nc.Request(server.JSApiAccountInfo, nil, time.Second)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	var info server.JSApiAccountInfoResponse
	if err := json.Unmarshal(resp.Data, &info); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	// Now create a stream.
	msetCfg := server.StreamConfig{
		Name:     "MSET22",
		Storage:  server.FileStorage,
		Subjects: []string{"foo", "bar", "baz"},
		MaxMsgs:  100,
	}
	req, err := json.Marshal(msetCfg)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	resp, _ = nc.Request(fmt.Sprintf(server.JSApiStreamCreateT, msetCfg.Name), req, time.Second)
	var scResp server.JSApiStreamCreateResponse
	if err := json.Unmarshal(resp.Data, &scResp); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if scResp.StreamInfo == nil || scResp.Error != nil {
		t.Fatalf("Did not receive correct response")
	}
	if time.Since(scResp.Created) > time.Second {
		t.Fatalf("Created time seems wrong: %v\n", scResp.Created)
	}

	checkBadRequest := func(e *server.ApiError, description string) {
		t.Helper()
		if e == nil || e.Code != 400 || e.Description != description {
			t.Fatalf("Did not get proper error: %+v", e)
		}
	}

	checkServerError := func(e *server.ApiError, description string) {
		t.Helper()
		if e == nil || e.Code != 500 || e.Description != description {
			t.Fatalf("Did not get proper server error: %+v\n", e)
		}
	}

	checkNotFound := func(e *server.ApiError, description string) {
		t.Helper()
		if e == nil || e.Code != 404 || e.Description != description {
			t.Fatalf("Did not get proper server error: %+v\n", e)
		}
	}

	// Check that the name in config has to match the name in the subject
	resp, _ = nc.Request(fmt.Sprintf(server.JSApiStreamCreateT, "BOB"), req, time.Second)
	scResp.Error, scResp.StreamInfo = nil, nil
	if err := json.Unmarshal(resp.Data, &scResp); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	checkBadRequest(scResp.Error, "stream name in subject does not match request")

	// Check that update works.
	msetCfg.Subjects = []string{"foo", "bar", "baz"}
	msetCfg.MaxBytes = 2222222
	req, err = json.Marshal(msetCfg)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	resp, _ = nc.Request(fmt.Sprintf(server.JSApiStreamUpdateT, msetCfg.Name), req, time.Second)
	scResp.Error, scResp.StreamInfo = nil, nil
	if err := json.Unmarshal(resp.Data, &scResp); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if scResp.StreamInfo == nil || scResp.Error != nil {
		t.Fatalf("Did not receive correct response: %+v", scResp.Error)
	}

	// Now lookup info again and see that we can see the new stream.
	resp, err = nc.Request(server.JSApiAccountInfo, nil, time.Second)
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
	resp, err = nc.Request(server.JSApiStreams, nil, time.Second)
	var namesResponse server.JSApiStreamNamesResponse
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
	if namesResponse.Limit != server.JSApiNamesLimit {
		t.Fatalf("Expected limit to be %d but got %d", server.JSApiNamesLimit, namesResponse.Limit)
	}
	if namesResponse.Streams[0] != msetCfg.Name {
		t.Fatalf("Expected to get %q, but got %q", msetCfg.Name, namesResponse.Streams[0])
	}

	// Now do detailed version.
	resp, err = nc.Request(server.JSApiStreamList, nil, time.Second)
	var listResponse server.JSApiStreamListResponse
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
	if listResponse.Limit != server.JSApiListLimit {
		t.Fatalf("Expected limit to be %d but got %d", server.JSApiListLimit, listResponse.Limit)
	}
	if listResponse.Streams[0].Config.Name != msetCfg.Name {
		t.Fatalf("Expected to get %q, but got %q", msetCfg.Name, listResponse.Streams[0].Config.Name)
	}

	// Now send some messages, then we can poll for info on this stream.
	toSend := 10
	for i := 0; i < toSend; i++ {
		nc.Request("foo", []byte("WELCOME JETSTREAM"), time.Second)
	}

	resp, err = nc.Request(fmt.Sprintf(server.JSApiStreamInfoT, msetCfg.Name), nil, time.Second)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	var msi server.StreamInfo
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
	resp, err = nc.Request(fmt.Sprintf(server.JSApiStreamInfoT, "BOB"), nil, time.Second)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	var bResp server.JSApiStreamInfoResponse
	if err = json.Unmarshal(resp.Data, &bResp); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	checkNotFound(bResp.Error, "stream not found")

	// Now create an consumer.
	delivery := nats.NewInbox()
	obsReq := server.CreateConsumerRequest{
		Stream: msetCfg.Name,
		Config: server.ConsumerConfig{DeliverSubject: delivery},
	}
	req, err = json.Marshal(obsReq)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	resp, err = nc.Request(fmt.Sprintf(server.JSApiConsumerCreateT, msetCfg.Name), req, time.Second)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	var ccResp server.JSApiConsumerCreateResponse
	if err = json.Unmarshal(resp.Data, &ccResp); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	checkServerError(ccResp.Error, "consumer requires interest for delivery subject when ephemeral")

	// Now create subscription and make sure we get proper response.
	sub, _ := nc.SubscribeSync(delivery)
	nc.Flush()

	resp, err = nc.Request(fmt.Sprintf(server.JSApiConsumerCreateT, msetCfg.Name), req, time.Second)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	ccResp.Error, ccResp.ConsumerInfo = nil, nil
	if err = json.Unmarshal(resp.Data, &ccResp); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if ccResp.ConsumerInfo == nil || ccResp.Error != nil {
		t.Fatalf("Got a bad response %+v", ccResp)
	}
	if time.Since(ccResp.Created) > time.Second {
		t.Fatalf("Created time seems wrong: %v\n", ccResp.Created)
	}

	checkFor(t, 250*time.Millisecond, 10*time.Millisecond, func() error {
		if nmsgs, _, _ := sub.Pending(); err != nil || nmsgs != toSend {
			return fmt.Errorf("Did not receive correct number of messages: %d vs %d", nmsgs, toSend)
		}
		return nil
	})

	// Check that we get an error if the stream name in the subject does not match the config.
	resp, err = nc.Request(fmt.Sprintf(server.JSApiConsumerCreateT, "BOB"), req, time.Second)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	ccResp.Error, ccResp.ConsumerInfo = nil, nil
	if err = json.Unmarshal(resp.Data, &ccResp); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	// Since we do not have interest this should have failed.
	checkBadRequest(ccResp.Error, "stream name in subject does not match request")

	// Get the list of all of the consumers for our stream.
	resp, err = nc.Request(fmt.Sprintf(server.JSApiConsumersT, msetCfg.Name), nil, time.Second)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	var clResponse server.JSApiConsumerNamesResponse
	if err = json.Unmarshal(resp.Data, &clResponse); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if len(clResponse.Consumers) != 1 {
		t.Fatalf("Expected only 1 consumer but got %d", len(clResponse.Consumers))
	}
	// Now let's get info about our consumer.
	cName := clResponse.Consumers[0]
	resp, err = nc.Request(fmt.Sprintf(server.JSApiConsumerInfoT, msetCfg.Name, cName), nil, time.Second)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	var oinfo server.ConsumerInfo
	if err = json.Unmarshal(resp.Data, &oinfo); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	// Do some sanity checking.
	// Must match consumer.go
	const randConsumerNameLen = 6

	if len(oinfo.Name) != randConsumerNameLen {
		t.Fatalf("Expected ephemeral name, got %q", oinfo.Name)
	}
	if len(oinfo.Config.Durable) != 0 {
		t.Fatalf("Expected no durable name, but got %q", oinfo.Config.Durable)
	}
	if oinfo.Config.DeliverSubject != delivery {
		t.Fatalf("Expected to have delivery subject of %q, got %q", delivery, oinfo.Config.DeliverSubject)
	}
	if oinfo.Delivered.ConsumerSeq != 10 {
		t.Fatalf("Expected consumer delivered sequence of 10, got %d", oinfo.Delivered.ConsumerSeq)
	}
	if oinfo.AckFloor.ConsumerSeq != 10 {
		t.Fatalf("Expected ack floor to be 10, got %d", oinfo.AckFloor.ConsumerSeq)
	}

	// Now delete the consumer.
	resp, _ = nc.Request(fmt.Sprintf(server.JSApiConsumerDeleteT, msetCfg.Name, cName), nil, time.Second)
	var cdResp server.JSApiConsumerDeleteResponse
	if err = json.Unmarshal(resp.Data, &cdResp); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if !cdResp.Success || cdResp.Error != nil {
		t.Fatalf("Got a bad response %+v", ccResp)
	}

	// Make sure we can't create a durable using the ephemeral API endpoint.
	obsReq = server.CreateConsumerRequest{
		Stream: msetCfg.Name,
		Config: server.ConsumerConfig{Durable: "myd", DeliverSubject: delivery},
	}
	req, err = json.Marshal(obsReq)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	resp, err = nc.Request(fmt.Sprintf(server.JSApiConsumerCreateT, msetCfg.Name), req, time.Second)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	ccResp.Error, ccResp.ConsumerInfo = nil, nil
	if err = json.Unmarshal(resp.Data, &ccResp); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	checkBadRequest(ccResp.Error, "consumer expected to be ephemeral but a durable name was set in request")

	// Now make sure we can create a durable on the subject with the proper name.
	resp, err = nc.Request(fmt.Sprintf(server.JSApiDurableCreateT, msetCfg.Name, obsReq.Config.Durable), req, time.Second)
	ccResp.Error, ccResp.ConsumerInfo = nil, nil
	if err = json.Unmarshal(resp.Data, &ccResp); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if ccResp.ConsumerInfo == nil || ccResp.Error != nil {
		t.Fatalf("Did not receive correct response")
	}

	// Make sure empty durable in cfg does not work
	obsReq2 := server.CreateConsumerRequest{
		Stream: msetCfg.Name,
		Config: server.ConsumerConfig{DeliverSubject: delivery},
	}
	req2, err := json.Marshal(obsReq2)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	resp, err = nc.Request(fmt.Sprintf(server.JSApiDurableCreateT, msetCfg.Name, obsReq.Config.Durable), req2, time.Second)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	ccResp.Error, ccResp.ConsumerInfo = nil, nil
	if err = json.Unmarshal(resp.Data, &ccResp); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	checkBadRequest(ccResp.Error, "consumer expected to be durable but a durable name was not set")

	// Now delete a msg.
	dreq := server.JSApiMsgDeleteRequest{Seq: 2}
	dreqj, err := json.Marshal(dreq)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	resp, _ = nc.Request(fmt.Sprintf(server.JSApiMsgDeleteT, msetCfg.Name), dreqj, time.Second)
	var delMsgResp server.JSApiMsgDeleteResponse
	if err = json.Unmarshal(resp.Data, &delMsgResp); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if !delMsgResp.Success || delMsgResp.Error != nil {
		t.Fatalf("Got a bad response %+v", delMsgResp.Error)
	}

	// Now purge the stream.
	resp, _ = nc.Request(fmt.Sprintf(server.JSApiStreamPurgeT, msetCfg.Name), nil, time.Second)
	var pResp server.JSApiStreamPurgeResponse
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
	resp, _ = nc.Request(fmt.Sprintf(server.JSApiStreamDeleteT, msetCfg.Name), nil, time.Second)
	var dResp server.JSApiStreamDeleteResponse
	if err = json.Unmarshal(resp.Data, &dResp); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if !dResp.Success || dResp.Error != nil {
		t.Fatalf("Got a bad response %+v", dResp.Error)
	}

	// Now grab stats again.
	// This will get the current information about usage and limits for this account.
	resp, err = nc.Request(server.JSApiAccountInfo, nil, time.Second)
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
	mcfg := &server.StreamConfig{
		Subjects:  []string{"kv.*"},
		Retention: server.LimitsPolicy,
		MaxAge:    time.Hour,
		MaxMsgs:   4,
		Storage:   server.MemoryStorage,
		Replicas:  1,
	}
	template := &server.StreamTemplateConfig{
		Name:       "kv",
		Config:     mcfg,
		MaxStreams: 4,
	}
	req, err = json.Marshal(template)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	// Check that the name in config has to match the name in the subject
	resp, _ = nc.Request(fmt.Sprintf(server.JSApiTemplateCreateT, "BOB"), req, time.Second)
	var stResp server.JSApiStreamTemplateCreateResponse
	if err = json.Unmarshal(resp.Data, &stResp); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	checkBadRequest(stResp.Error, "template name in subject does not match request")

	resp, _ = nc.Request(fmt.Sprintf(server.JSApiTemplateCreateT, template.Name), req, time.Second)
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

	resp, _ = nc.Request(fmt.Sprintf(server.JSApiTemplateCreateT, template.Name), req, time.Second)
	stResp.Error, stResp.StreamTemplateInfo = nil, nil
	if err = json.Unmarshal(resp.Data, &stResp); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if stResp.StreamTemplateInfo == nil || stResp.Error != nil {
		t.Fatalf("Did not receive correct response")
	}

	// Now grab the list of templates
	var tListResp server.JSApiStreamTemplateNamesResponse
	resp, err = nc.Request(server.JSApiTemplates, nil, time.Second)
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
	resp, _ = nc.Request(fmt.Sprintf(server.JSApiTemplateDeleteT, "bob"), nil, time.Second)
	var tDeleteResp server.JSApiStreamTemplateDeleteResponse
	if err = json.Unmarshal(resp.Data, &tDeleteResp); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	checkServerError(tDeleteResp.Error, "template not found")

	resp, _ = nc.Request(fmt.Sprintf(server.JSApiTemplateDeleteT, "ss"), nil, time.Second)
	tDeleteResp.Error = nil
	if err = json.Unmarshal(resp.Data, &tDeleteResp); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if !tDeleteResp.Success || tDeleteResp.Error != nil {
		t.Fatalf("Did not receive correct response: %+v", tDeleteResp.Error)
	}

	resp, err = nc.Request(server.JSApiTemplates, nil, time.Second)
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
	resp, err = nc.Request(fmt.Sprintf(server.JSApiTemplateInfoT, "kv"), nil, time.Second)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	var ti server.StreamTemplateInfo
	if err = json.Unmarshal(resp.Data, &ti); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if len(ti.Streams) != 1 {
		t.Fatalf("Expected 1 stream, got %d", len(ti.Streams))
	}
	if ti.Streams[0] != server.CanonicalName("kv.22") {
		t.Fatalf("Expected stream with name %q, but got %q", server.CanonicalName("kv.22"), ti.Streams[0])
	}

	// Test that we can send nil or an empty legal json for requests that take no args.
	// We know this stream does not exist, this just checking request processing.
	checkEmptyReqArg := func(arg string) {
		t.Helper()
		var req []byte
		if len(arg) > 0 {
			req = []byte(arg)
		}
		resp, err = nc.Request(fmt.Sprintf(server.JSApiStreamDeleteT, "foo_bar_baz"), req, time.Second)
		var dResp server.JSApiStreamDeleteResponse
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

func TestJetStreamAPIStreamListPaging(t *testing.T) {
	s := RunBasicJetStreamServer()
	defer s.Shutdown()

	// Forced cleanup of all persisted state.
	if config := s.JetStreamConfig(); config != nil {
		defer os.RemoveAll(config.StoreDir)
	}

	// Create 4X limit
	streamsNum := 4 * server.JSApiNamesLimit
	for i := 1; i <= streamsNum; i++ {
		name := fmt.Sprintf("STREAM-%06d", i)
		cfg := server.StreamConfig{Name: name}
		_, err := s.GlobalAccount().AddStream(&cfg)
		if err != nil {
			t.Fatalf("Unexpected error adding stream: %v", err)
		}
	}

	// Client for API requests.
	nc := clientConnectToServer(t, s)
	defer nc.Close()

	reqList := func(offset int) []byte {
		t.Helper()
		var req []byte
		if offset > 0 {
			req, _ = json.Marshal(&server.JSApiStreamNamesRequest{ApiPagedRequest: server.ApiPagedRequest{Offset: offset}})
		}
		resp, err := nc.Request(server.JSApiStreams, req, time.Second)
		if err != nil {
			t.Fatalf("Unexpected error getting stream list: %v", err)
		}
		return resp.Data
	}

	checkResp := func(resp []byte, expectedLen, expectedOffset int) {
		t.Helper()
		var listResponse server.JSApiStreamNamesResponse
		if err := json.Unmarshal(resp, &listResponse); err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		if len(listResponse.Streams) != expectedLen {
			t.Fatalf("Expected only %d streams but got %d", expectedLen, len(listResponse.Streams))
		}
		if listResponse.Total != streamsNum {
			t.Fatalf("Expected total to be %d but got %d", streamsNum, listResponse.Total)
		}
		if listResponse.Offset != expectedOffset {
			t.Fatalf("Expected offset to be %d but got %d", expectedOffset, listResponse.Offset)
		}
		if expectedLen < 1 {
			return
		}
		// Make sure we get the right stream.
		sname := fmt.Sprintf("STREAM-%06d", expectedOffset+1)
		if listResponse.Streams[0] != sname {
			t.Fatalf("Expected stream %q to be first, got %q", sname, listResponse.Streams[0])
		}
	}

	checkResp(reqList(0), server.JSApiNamesLimit, 0)
	checkResp(reqList(server.JSApiNamesLimit), server.JSApiNamesLimit, server.JSApiNamesLimit)
	checkResp(reqList(2*server.JSApiNamesLimit), server.JSApiNamesLimit, 2*server.JSApiNamesLimit)
	checkResp(reqList(streamsNum), 0, streamsNum)
	checkResp(reqList(streamsNum-22), 22, streamsNum-22)
	checkResp(reqList(streamsNum+22), 0, streamsNum)
}

func TestJetStreamAPIConsumerListPaging(t *testing.T) {
	s := RunBasicJetStreamServer()
	defer s.Shutdown()

	// Forced cleanup of all persisted state.
	if config := s.JetStreamConfig(); config != nil {
		defer os.RemoveAll(config.StoreDir)
	}

	sname := "MYSTREAM"
	mset, err := s.GlobalAccount().AddStream(&server.StreamConfig{Name: sname})
	if err != nil {
		t.Fatalf("Unexpected error adding stream: %v", err)
	}

	// Client for API requests.
	nc := clientConnectToServer(t, s)
	defer nc.Close()

	sub, _ := nc.SubscribeSync("d.*")
	defer sub.Unsubscribe()
	nc.Flush()

	consumersNum := server.JSApiNamesLimit
	for i := 1; i <= consumersNum; i++ {
		_, err := mset.AddConsumer(&server.ConsumerConfig{DeliverSubject: fmt.Sprintf("d.%d", i)})
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
	}

	reqListSubject := fmt.Sprintf(server.JSApiConsumersT, sname)
	reqList := func(offset int) []byte {
		t.Helper()
		var req []byte
		if offset > 0 {
			req, _ = json.Marshal(&server.JSApiConsumersRequest{ApiPagedRequest: server.ApiPagedRequest{Offset: offset}})
		}
		resp, err := nc.Request(reqListSubject, req, time.Second)
		if err != nil {
			t.Fatalf("Unexpected error getting stream list: %v", err)
		}
		return resp.Data
	}

	checkResp := func(resp []byte, expectedLen, expectedOffset int) {
		t.Helper()
		var listResponse server.JSApiConsumerNamesResponse
		if err := json.Unmarshal(resp, &listResponse); err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		if len(listResponse.Consumers) != expectedLen {
			t.Fatalf("Expected only %d streams but got %d", expectedLen, len(listResponse.Consumers))
		}
		if listResponse.Total != consumersNum {
			t.Fatalf("Expected total to be %d but got %d", consumersNum, listResponse.Total)
		}
		if listResponse.Offset != expectedOffset {
			t.Fatalf("Expected offset to be %d but got %d", expectedOffset, listResponse.Offset)
		}
	}

	checkResp(reqList(0), server.JSApiNamesLimit, 0)
	checkResp(reqList(consumersNum-22), 22, consumersNum-22)
	checkResp(reqList(consumersNum+22), 0, consumersNum)
}

func TestJetStreamUpdateStream(t *testing.T) {
	cases := []struct {
		name    string
		mconfig *server.StreamConfig
	}{
		{name: "MemoryStore",
			mconfig: &server.StreamConfig{
				Name:      "foo",
				Retention: server.LimitsPolicy,
				MaxAge:    time.Hour,
				Storage:   server.MemoryStorage,
				Replicas:  1,
			}},
		{name: "FileStore",
			mconfig: &server.StreamConfig{
				Name:      "foo",
				Retention: server.LimitsPolicy,
				MaxAge:    time.Hour,
				Storage:   server.FileStorage,
				Replicas:  1,
			}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			s := RunBasicJetStreamServer()
			defer s.Shutdown()

			if config := s.JetStreamConfig(); config != nil && config.StoreDir != "" {
				defer os.RemoveAll(config.StoreDir)
			}

			mset, err := s.GlobalAccount().AddStream(c.mconfig)
			if err != nil {
				t.Fatalf("Unexpected error adding stream: %v", err)
			}
			defer mset.Delete()

			// Test basic updates. We allow changing the subjects, limits, and no_ack along with replicas(TBD w/ cluster)
			cfg := *c.mconfig

			// Can't change name.
			cfg.Name = "bar"
			if err := mset.Update(&cfg); err == nil || !strings.Contains(err.Error(), "name must match") {
				t.Fatalf("Expected error trying to update name")
			}
			// Can't change max consumers for now.
			cfg = *c.mconfig
			cfg.MaxConsumers = 10
			if err := mset.Update(&cfg); err == nil || !strings.Contains(err.Error(), "can not change") {
				t.Fatalf("Expected error trying to change MaxConsumers")
			}
			// Can't change storage types.
			cfg = *c.mconfig
			if cfg.Storage == server.FileStorage {
				cfg.Storage = server.MemoryStorage
			} else {
				cfg.Storage = server.FileStorage
			}
			if err := mset.Update(&cfg); err == nil || !strings.Contains(err.Error(), "can not change") {
				t.Fatalf("Expected error trying to change Storage")
			}
			// Can't change replicas > 1 for now.
			cfg = *c.mconfig
			cfg.Replicas = 10
			if err := mset.Update(&cfg); err == nil || !strings.Contains(err.Error(), "maximum replicas") {
				t.Fatalf("Expected error trying to change Replicas")
			}
			// Can't have a template set for now.
			cfg = *c.mconfig
			cfg.Template = "baz"
			if err := mset.Update(&cfg); err == nil || !strings.Contains(err.Error(), "template") {
				t.Fatalf("Expected error trying to change Template owner")
			}
			// Can't change limits policy.
			cfg = *c.mconfig
			cfg.Retention = server.WorkQueuePolicy
			if err := mset.Update(&cfg); err == nil || !strings.Contains(err.Error(), "can not change") {
				t.Fatalf("Expected error trying to change Retention")
			}

			// Now test changing limits.
			nc := clientConnectToServer(t, s)
			defer nc.Close()

			pending := uint64(100)
			for i := uint64(0); i < pending; i++ {
				sendStreamMsg(t, nc, "foo", "0123456789")
			}
			pendingBytes := mset.State().Bytes

			checkPending := func(msgs, bts uint64) {
				t.Helper()
				state := mset.State()
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
			if err := mset.Update(&cfg); err != nil {
				t.Fatalf("Unexpected error %v", err)
			}
			if mset.Config().MaxMsgs != cfg.MaxMsgs {
				t.Fatalf("Expected the change to take effect, %d vs %d", mset.Config().MaxMsgs, cfg.MaxMsgs)
			}
			checkPending(pending, pendingBytes)

			// Update msgs to lower.
			cfg = *c.mconfig
			cfg.MaxMsgs = int64(pending / 2)
			if err := mset.Update(&cfg); err != nil {
				t.Fatalf("Unexpected error %v", err)
			}
			if mset.Config().MaxMsgs != cfg.MaxMsgs {
				t.Fatalf("Expected the change to take effect, %d vs %d", mset.Config().MaxMsgs, cfg.MaxMsgs)
			}
			checkPending(pending/2, pendingBytes/2)
			// Now do bytes.
			cfg = *c.mconfig
			cfg.MaxBytes = int64(pendingBytes / 4)
			if err := mset.Update(&cfg); err != nil {
				t.Fatalf("Unexpected error %v", err)
			}
			if mset.Config().MaxBytes != cfg.MaxBytes {
				t.Fatalf("Expected the change to take effect, %d vs %d", mset.Config().MaxBytes, cfg.MaxBytes)
			}
			checkPending(pending/4, pendingBytes/4)

			// Now do age.
			cfg = *c.mconfig
			cfg.MaxAge = time.Millisecond
			if err := mset.Update(&cfg); err != nil {
				t.Fatalf("Unexpected error %v", err)
			}
			// Just wait a bit for expiration.
			time.Sleep(5 * time.Millisecond)
			if mset.Config().MaxAge != cfg.MaxAge {
				t.Fatalf("Expected the change to take effect, %d vs %d", mset.Config().MaxAge, cfg.MaxAge)
			}
			checkPending(0, 0)

			// Now put back to original.
			cfg = *c.mconfig
			if err := mset.Update(&cfg); err != nil {
				t.Fatalf("Unexpected error %v", err)
			}
			for i := uint64(0); i < pending; i++ {
				sendStreamMsg(t, nc, "foo", "0123456789")
			}

			// subject changes.
			// Add in a subject first.
			cfg = *c.mconfig
			cfg.Subjects = []string{"foo", "bar"}
			if err := mset.Update(&cfg); err != nil {
				t.Fatalf("Unexpected error %v", err)
			}
			// Make sure we can still send to foo.
			sendStreamMsg(t, nc, "foo", "0123456789")
			// And we can now send to bar.
			sendStreamMsg(t, nc, "bar", "0123456789")
			// Now delete both and change to baz only.
			cfg.Subjects = []string{"baz"}
			if err := mset.Update(&cfg); err != nil {
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
			if nmsgs := mset.State().Msgs; nmsgs != pending+3 {
				t.Fatalf("Expected %d msgs, got %d", pending+3, nmsgs)
			}

			// FileStore restarts for config save.
			cfg = *c.mconfig
			if cfg.Storage == server.FileStorage {
				cfg.Subjects = []string{"foo", "bar"}
				cfg.MaxMsgs = 2222
				cfg.MaxBytes = 3333333
				cfg.MaxAge = 22 * time.Hour
				if err := mset.Update(&cfg); err != nil {
					t.Fatalf("Unexpected error %v", err)
				}
				// Pull since certain defaults etc are set in processing.
				cfg = mset.Config()

				// Restart the server.
				// Capture port since it was dynamic.
				u, _ := url.Parse(s.ClientURL())
				port, _ := strconv.Atoi(u.Port())

				// Stop current server.
				sd := s.JetStreamConfig().StoreDir
				s.Shutdown()
				// Restart.
				s = RunJetStreamServerOnPort(port, sd)
				defer s.Shutdown()

				mset, err = s.GlobalAccount().LookupStream(cfg.Name)
				if err != nil {
					t.Fatalf("Expected to find a stream for %q", cfg.Name)
				}
				restored_cfg := mset.Config()
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
		mconfig *server.StreamConfig
	}{
		{name: "MemoryStore",
			mconfig: &server.StreamConfig{
				Name:      "foo",
				Retention: server.LimitsPolicy,
				MaxAge:    time.Hour,
				Storage:   server.MemoryStorage,
				Replicas:  1,
			}},
		{name: "FileStore",
			mconfig: &server.StreamConfig{
				Name:      "foo",
				Retention: server.LimitsPolicy,
				MaxAge:    time.Hour,
				Storage:   server.FileStorage,
				Replicas:  1,
			}},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {

			s := RunBasicJetStreamServer()
			defer s.Shutdown()

			if config := s.JetStreamConfig(); config != nil && config.StoreDir != "" {
				defer os.RemoveAll(config.StoreDir)
			}

			mset, err := s.GlobalAccount().AddStream(c.mconfig)
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

			state := mset.State()
			if state.Msgs != 10 {
				t.Fatalf("Expected 10 messages, got %d", state.Msgs)
			}
			bytesPerMsg := state.Bytes / 10
			if bytesPerMsg == 0 {
				t.Fatalf("Expected non-zero bytes for msg size")
			}

			deleteAndCheck := func(seq, expectedFirstSeq uint64) {
				t.Helper()
				beforeState := mset.State()
				if removed, _ := mset.DeleteMsg(seq); !removed {
					t.Fatalf("Expected the delete of sequence %d to succeed", seq)
				}
				expectedState := beforeState
				expectedState.Msgs--
				expectedState.Bytes -= bytesPerMsg
				expectedState.FirstSeq = expectedFirstSeq

				sm, err := mset.GetMsg(expectedFirstSeq)
				if err != nil {
					t.Fatalf("Error fetching message for seq: %d - %v", expectedFirstSeq, err)
				}
				expectedState.FirstTime = sm.Time

				afterState := mset.State()
				// Ignore first time in this test.
				if afterState != expectedState {
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

			mset.Purge()
			// Put ten more one.
			pubTen()
			deleteAndCheck(11, 12)
			deleteAndCheck(15, 12)
			deleteAndCheck(16, 12)
			deleteAndCheck(20, 12)

			// Only file storage beyond here.
			if c.mconfig.Storage == server.MemoryStorage {
				return
			}

			// Capture port since it was dynamic.
			u, _ := url.Parse(s.ClientURL())
			port, _ := strconv.Atoi(u.Port())
			sd := s.JetStreamConfig().StoreDir

			// Shutdown the server.
			s.Shutdown()

			s = RunJetStreamServerOnPort(port, sd)
			defer s.Shutdown()

			mset, err = s.GlobalAccount().LookupStream("foo")
			if err != nil {
				t.Fatalf("Expected to get the stream back")
			}

			expected := server.StreamState{Msgs: 6, Bytes: 6 * bytesPerMsg, FirstSeq: 12, LastSeq: 20}
			state = mset.State()
			state.FirstTime, state.LastTime = time.Time{}, time.Time{}
			if state != expected {
				t.Fatalf("State not what we expected. Expected %+v, got %+v\n", expected, state)
			}

			// Now create an consumer and make sure we get the right sequence.
			nc = clientConnectToServer(t, s)
			defer nc.Close()

			delivery := nats.NewInbox()
			sub, _ := nc.SubscribeSync(delivery)
			nc.Flush()

			o, err := mset.AddConsumer(&server.ConsumerConfig{DeliverSubject: delivery, FilterSubject: "foo"})
			if err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}

			expectedStoreSeq := []uint64{12, 13, 14, 17, 18, 19}

			for i := 0; i < 6; i++ {
				m, err := sub.NextMsg(time.Second)
				if err != nil {
					t.Fatalf("Unexpected error: %v", err)
				}
				if o.StreamSeqFromReply(m.Reply) != expectedStoreSeq[i] {
					t.Fatalf("Expected store seq of %d, got %d", expectedStoreSeq[i], o.StreamSeqFromReply(m.Reply))
				}
			}
		})
	}
}

func TestJetStreamNextMsgNoInterest(t *testing.T) {
	cases := []struct {
		name    string
		mconfig *server.StreamConfig
	}{
		{name: "MemoryStore",
			mconfig: &server.StreamConfig{
				Name:      "foo",
				Retention: server.LimitsPolicy,
				MaxAge:    time.Hour,
				Storage:   server.MemoryStorage,
				Replicas:  1,
			}},
		{name: "FileStore",
			mconfig: &server.StreamConfig{
				Name:      "foo",
				Retention: server.LimitsPolicy,
				MaxAge:    time.Hour,
				Storage:   server.FileStorage,
				Replicas:  1,
			}},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			s := RunBasicJetStreamServer()
			defer s.Shutdown()

			if config := s.JetStreamConfig(); config != nil {
				defer os.RemoveAll(config.StoreDir)
			}

			cfg := &server.StreamConfig{Name: "foo", Storage: server.FileStorage}
			mset, err := s.GlobalAccount().AddStream(cfg)
			if err != nil {
				t.Fatalf("Unexpected error adding stream: %v", err)
			}

			nc := clientConnectWithOldRequest(t, s)
			defer nc.Close()

			// Now create an consumer and make sure it functions properly.
			o, err := mset.AddConsumer(workerModeConfig("WQ"))
			if err != nil {
				t.Fatalf("Expected no error with registered interest, got %v", err)
			}
			defer o.Delete()

			nextSubj := o.RequestNextMsgSubject()

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
			ostate := o.Info()
			if ostate.AckFloor.StreamSeq != 11 || ostate.NumPending > 0 {
				t.Fatalf("Inconsistent ack state: %+v", ostate)
			}
		})
	}
}

func TestJetStreamMsgHeaders(t *testing.T) {
	cases := []struct {
		name    string
		mconfig *server.StreamConfig
	}{
		{name: "MemoryStore",
			mconfig: &server.StreamConfig{
				Name:      "foo",
				Retention: server.LimitsPolicy,
				MaxAge:    time.Hour,
				Storage:   server.MemoryStorage,
				Replicas:  1,
			}},
		{name: "FileStore",
			mconfig: &server.StreamConfig{
				Name:      "foo",
				Retention: server.LimitsPolicy,
				MaxAge:    time.Hour,
				Storage:   server.FileStorage,
				Replicas:  1,
			}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			s := RunBasicJetStreamServer()
			defer s.Shutdown()

			if config := s.JetStreamConfig(); config != nil {
				defer os.RemoveAll(config.StoreDir)
			}

			mset, err := s.GlobalAccount().AddStream(c.mconfig)
			if err != nil {
				t.Fatalf("Unexpected error adding stream: %v", err)
			}
			defer mset.Delete()

			nc := clientConnectToServer(t, s)
			defer nc.Close()

			m := nats.NewMsg("foo")
			m.Header.Add("Accept-Encoding", "json")
			m.Header.Add("Authorization", "s3cr3t")
			m.Data = []byte("Hello JetStream Headers - #1!")

			nc.PublishMsg(m)
			nc.Flush()

			state := mset.State()
			if state.Msgs != 1 {
				t.Fatalf("Expected 1 message, got %d", state.Msgs)
			}
			if state.Bytes == 0 {
				t.Fatalf("Expected non-zero bytes")
			}

			// Now access raw from stream.
			sm, err := mset.GetMsg(1)
			if err != nil {
				t.Fatalf("Unexpected error getting stored message: %v", err)
			}
			// Calculate the []byte version of the headers.
			var b bytes.Buffer
			b.WriteString("NATS/1.0\r\n")
			m.Header.Write(&b)
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

			o, err := mset.AddConsumer(&server.ConsumerConfig{DeliverSubject: sub.Subject})
			if err != nil {
				t.Fatalf("Expected no error with registered interest, got %v", err)
			}
			defer o.Delete()

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
		defer os.RemoveAll(config.StoreDir)
	}

	acc := s.GlobalAccount()

	mcfg := &server.StreamConfig{
		Subjects:  []string{"kv.*"},
		Retention: server.LimitsPolicy,
		MaxAge:    time.Hour,
		MaxMsgs:   4,
		Storage:   server.MemoryStorage,
		Replicas:  1,
	}
	template := &server.StreamTemplateConfig{
		Name:       "kv",
		Config:     mcfg,
		MaxStreams: 4,
	}

	if _, err := acc.AddStreamTemplate(template); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if templates := acc.Templates(); len(templates) != 1 {
		t.Fatalf("Expected to get array of 1 template, got %d", len(templates))
	}
	if err := acc.DeleteStreamTemplate("foo"); err == nil {
		t.Fatalf("Expected an error for non-existent template")
	}
	if err := acc.DeleteStreamTemplate(template.Name); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if templates := acc.Templates(); len(templates) != 0 {
		t.Fatalf("Expected to get array of no templates, got %d", len(templates))
	}
	// Add it back in and test basics
	if _, err := acc.AddStreamTemplate(template); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	// Connect a client and send a message which should trigger the stream creation.
	nc := clientConnectToServer(t, s)
	defer nc.Close()

	sendStreamMsg(t, nc, "kv.22", "derek")
	sendStreamMsg(t, nc, "kv.33", "cat")
	sendStreamMsg(t, nc, "kv.44", "sam")
	sendStreamMsg(t, nc, "kv.55", "meg")

	if nms := acc.NumStreams(); nms != 4 {
		t.Fatalf("Expected 4 auto-created streams, got %d", nms)
	}

	// This one should fail due to max.
	if resp, err := nc.Request("kv.99", nil, 100*time.Millisecond); err == nil {
		t.Fatalf("Expected this to fail, but got %q", resp.Data)
	}

	// Now delete template and make sure the underlying streams go away too.
	if err := acc.DeleteStreamTemplate(template.Name); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	if nms := acc.NumStreams(); nms != 0 {
		t.Fatalf("Expected no auto-created streams to remain, got %d", nms)
	}
}

func TestJetStreamTemplateFileStoreRecovery(t *testing.T) {
	s := RunBasicJetStreamServer()
	defer s.Shutdown()

	if config := s.JetStreamConfig(); config != nil {
		defer os.RemoveAll(config.StoreDir)
	}

	acc := s.GlobalAccount()

	mcfg := &server.StreamConfig{
		Subjects:  []string{"kv.*"},
		Retention: server.LimitsPolicy,
		MaxAge:    time.Hour,
		MaxMsgs:   50,
		Storage:   server.FileStorage,
		Replicas:  1,
	}
	template := &server.StreamTemplateConfig{
		Name:       "kv",
		Config:     mcfg,
		MaxStreams: 100,
	}

	if _, err := acc.AddStreamTemplate(template); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	// Make sure we can not add in a stream on our own with a template owner.
	badCfg := *mcfg
	badCfg.Name = "bad"
	badCfg.Template = "kv"
	if _, err := acc.AddStream(&badCfg); err == nil {
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

	if nms := acc.NumStreams(); nms != 100 {
		t.Fatalf("Expected 100 auto-created streams, got %d", nms)
	}

	// Capture port since it was dynamic.
	u, _ := url.Parse(s.ClientURL())
	port, _ := strconv.Atoi(u.Port())

	restartServer := func() {
		t.Helper()
		sd := s.JetStreamConfig().StoreDir
		// Stop current server.
		s.Shutdown()
		// Restart.
		s = RunJetStreamServerOnPort(port, sd)
	}

	// Restart.
	restartServer()
	defer s.Shutdown()

	acc = s.GlobalAccount()
	if nms := acc.NumStreams(); nms != 100 {
		t.Fatalf("Expected 100 auto-created streams, got %d", nms)
	}
	tmpl, err := acc.LookupStreamTemplate(template.Name)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	// Make sure t.Delete() survives restart.
	tmpl.Delete()

	// Restart.
	restartServer()
	defer s.Shutdown()

	acc = s.GlobalAccount()
	if nms := acc.NumStreams(); nms != 0 {
		t.Fatalf("Expected no auto-created streams, got %d", nms)
	}
	if _, err := acc.LookupStreamTemplate(template.Name); err == nil {
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

	mset, err := s.GlobalAccount().AddStream(&server.StreamConfig{Name: "foo", Storage: server.MemoryStorage})
	if err != nil {
		t.Fatalf("Unexpected error adding stream: %v", err)
	}
	defer mset.Delete()

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

	o, err := mset.AddConsumer(&server.ConsumerConfig{DeliverSubject: sub.Subject})
	if err != nil {
		t.Fatalf("Expected no error with registered interest, got %v", err)
	}
	defer o.Delete()

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
		if seq := o.SeqFromReply(m.Reply); seq != uint64(i) {
			t.Fatalf("Expected sequence of %d , got %d", i, seq)
		}
	}

	// Now check the subject to make sure its the original one.
	for i := 1; i <= toSend; i++ {
		m, err := sub.NextMsg(time.Second)
		checkMsg(m, err, i)
	}

	// Now do a pull based consumer.
	o, err = mset.AddConsumer(workerModeConfig("p"))
	if err != nil {
		t.Fatalf("Expected no error with registered interest, got %v", err)
	}
	defer o.Delete()

	nextMsg := o.RequestNextMsgSubject()
	for i := 1; i <= toSend; i++ {
		m, err := nc.Request(nextMsg, nil, time.Second)
		checkMsg(m, err, i)
	}
}

func clientConnectToServerWithUP(t *testing.T, opts *server.Options, user, pass string) *nats.Conn {
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
		defer os.RemoveAll(config.StoreDir)
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
	defer os.Remove(conf)

	s, opts := RunServerWithConfig(conf)
	defer s.Shutdown()

	if config := s.JetStreamConfig(); config != nil {
		defer os.RemoveAll(config.StoreDir)
	}

	if !s.JetStreamEnabled() {
		t.Fatalf("Expected JetStream to be enabled")
	}

	nca := clientConnectToServerWithUP(t, opts, "ua", "pwd")
	defer nca.Close()

	ncb := clientConnectToServerWithUP(t, opts, "ub", "pwd")
	defer ncb.Close()

	resp, err := ncb.Request(server.JSApiAccountInfo, nil, time.Second)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	var info server.JSApiAccountInfoResponse
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
		var iResp server.JSApiAccountInfoResponse
		if err := json.Unmarshal(resp.Data, &iResp); err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		if iResp.Error == nil {
			t.Fatalf("Expected an error on not enabled account")
		}
	}

	// Check C is not enabled. We expect a negative response, not a timeout.
	expectNotEnabled(ncc.Request(server.JSApiAccountInfo, nil, 250*time.Millisecond))

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
	expectNotEnabled(nca.Request(server.JSApiAccountInfo, nil, 250*time.Millisecond))

	resp, _ = ncb.Request(server.JSApiAccountInfo, nil, 250*time.Millisecond)
	if err := json.Unmarshal(resp.Data, &info); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if info.Error != nil {
		t.Fatalf("Expected JetStream to be enabled, got %+v", info.Error)
	}

	resp, _ = ncc.Request(server.JSApiAccountInfo, nil, 250*time.Millisecond)
	if err := json.Unmarshal(resp.Data, &info); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if info.Error != nil {
		t.Fatalf("Expected JetStream to be enabled, got %+v", info.Error)
	}

	// Now check that limits have been updated.
	// Account B
	resp, err = ncb.Request(server.JSApiAccountInfo, nil, time.Second)
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
	resp, err = ncc.Request(server.JSApiAccountInfo, nil, time.Second)
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
	defer os.Remove(conf)

	s, _ := RunServerWithConfig(conf)
	defer s.Shutdown()

	if !s.JetStreamEnabled() {
		t.Fatalf("Expected JetStream to be enabled")
	}

	if config := s.JetStreamConfig(); config != nil {
		defer os.RemoveAll(config.StoreDir)
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
		defer os.RemoveAll(config.StoreDir)
	}

	acc := s.GlobalAccount()

	msetConfig := server.StreamConfig{
		Name:     "sr22",
		Storage:  server.FileStorage,
		Subjects: []string{"foo"},
	}

	if _, err := acc.AddStream(&msetConfig); err != nil {
		t.Fatalf("Unexpected error adding stream: %v", err)
	}

	nc := clientConnectToServer(t, s)
	defer nc.Close()

	toSend := 5000000
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

func TestJetStreamPubSubPerf(t *testing.T) {
	// Comment out to run, holding place for now.
	t.SkipNow()

	s := RunBasicJetStreamServer()
	defer s.Shutdown()

	if config := s.JetStreamConfig(); config != nil {
		defer os.RemoveAll(config.StoreDir)
	}

	acc := s.GlobalAccount()

	msetConfig := server.StreamConfig{
		Name:     "MSET22",
		Storage:  server.FileStorage,
		Subjects: []string{"foo"},
	}

	mset, err := acc.AddStream(&msetConfig)
	if err != nil {
		t.Fatalf("Unexpected error adding stream: %v", err)
	}

	nc := clientConnectToServer(t, s)
	defer nc.Close()

	var toSend = 1000000
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

	_, err = mset.AddConsumer(&server.ConsumerConfig{
		DeliverSubject: delivery,
		AckPolicy:      server.AckNone,
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
		mconfig *server.StreamConfig
	}{
		{"MemoryStore", &server.StreamConfig{
			Name:      "MY_STREAM",
			Storage:   server.MemoryStorage,
			Subjects:  []string{"foo.*"},
			Retention: server.InterestPolicy,
		}},
		{"FileStore", &server.StreamConfig{
			Name:      "MY_STREAM",
			Storage:   server.FileStorage,
			Subjects:  []string{"foo.*"},
			Retention: server.InterestPolicy,
		}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			s := RunBasicJetStreamServer()
			defer s.Shutdown()

			if config := s.JetStreamConfig(); config != nil {
				defer os.RemoveAll(config.StoreDir)
			}

			mset, err := s.GlobalAccount().AddStream(c.mconfig)
			if err != nil {
				t.Fatalf("Unexpected error adding stream: %v", err)
			}
			defer mset.Delete()

			nc1 := clientConnectToServer(t, s)
			defer nc1.Close()

			nc2 := clientConnectToServer(t, s)
			defer nc2.Close()

			// Create two durable consumers on the same subject
			sub1, _ := nc1.SubscribeSync(nats.NewInbox())
			defer sub1.Unsubscribe()
			nc1.Flush()

			o1, err := mset.AddConsumer(&server.ConsumerConfig{
				Durable:        "dur1",
				DeliverSubject: sub1.Subject,
				FilterSubject:  "foo.bar",
				AckPolicy:      server.AckExplicit,
			})
			if err != nil {
				t.Fatalf("Unexpected error adding consumer: %v", err)
			}
			defer o1.Delete()

			sub2, _ := nc2.SubscribeSync(nats.NewInbox())
			defer sub2.Unsubscribe()

			o2, err := mset.AddConsumer(&server.ConsumerConfig{
				Durable:        "dur2",
				DeliverSubject: sub2.Subject,
				FilterSubject:  "foo.bar",
				AckPolicy:      server.AckExplicit,
				AckWait:        25 * time.Millisecond,
			})
			if err != nil {
				t.Fatalf("Unexpected error adding consumer: %v", err)
			}
			defer o2.Delete()

			// Send 2 messages
			toSend := 2
			for i := 0; i < toSend; i++ {
				sendStreamMsg(t, nc1, "foo.bar", fmt.Sprintf("msg%v", i+1))
			}
			state := mset.State()
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
			nc1.Flush()
			nc2.Flush()

			// Now close the 2nd subscription...
			sub2.Unsubscribe()

			// Send 2 more new messages
			for i := 0; i < toSend; i++ {
				sendStreamMsg(t, nc1, "foo.bar", fmt.Sprintf("msg%v", 2+i+1))
			}
			state = mset.State()
			if state.Msgs != uint64(toSend) {
				t.Fatalf("Expected %v messages, got %d", toSend, state.Msgs)
			}

			// first subscription should get it and will ack it.
			for i := 0; i < toSend; i++ {
				m, err := sub1.NextMsg(time.Second)
				if err != nil {
					t.Fatalf("Error acking message: %v", err)
				}
				m.Respond(nil)
			}
			// For acks from m.Respond above
			nc1.Flush()

			// Now recreate the subscription for the 2nd JS consumer
			sub2, _ = nc2.SubscribeSync(nats.NewInbox())
			defer sub2.Unsubscribe()

			o2, err = mset.AddConsumer(&server.ConsumerConfig{
				Durable:        "dur2",
				DeliverSubject: sub2.Subject,
				FilterSubject:  "foo.bar",
				AckPolicy:      server.AckExplicit,
				AckWait:        25 * time.Millisecond,
			})
			if err != nil {
				t.Fatalf("Unexpected error adding consumer: %v", err)
			}
			defer o2.Delete()

			// Those messages should be redelivered to the 2nd consumer
			for i := 1; i <= toSend; i++ {
				m, err := sub2.NextMsg(time.Second)
				if err != nil {
					t.Fatalf("Error receiving message %d: %v", i, err)
				}
				m.Respond(nil)

				sseq := o2.StreamSeqFromReply(m.Reply)
				// Depending on timing from above we could receive stream sequences out of order but
				// we know we want 3 & 4.
				if sseq != 3 && sseq != 4 {
					t.Fatalf("Expected stream sequence of 3 or 4 but got %d", sseq)
				}
			}
		})
	}
}
