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

//go:build !skip_store_tests

package server

import (
	"bytes"
	"errors"
	"fmt"
	"math/rand/v2"
	"reflect"
	"slices"
	"testing"
	"time"

	"github.com/nats-io/nats-server/v2/server/gsl"
	"github.com/nats-io/nuid"
)

func TestMemStoreBasics(t *testing.T) {
	ms, err := newMemStore(&StreamConfig{Storage: MemoryStorage})
	require_NoError(t, err)
	defer ms.Stop()

	subj, msg := "foo", []byte("Hello World")
	now := time.Now().UnixNano()
	if seq, ts, err := ms.StoreMsg(subj, nil, msg, 0); err != nil {
		t.Fatalf("Error storing msg: %v", err)
	} else if seq != 1 {
		t.Fatalf("Expected sequence to be 1, got %d", seq)
	} else if ts < now || ts > now+int64(time.Millisecond) {
		t.Fatalf("Expected timestamp to be current, got %v", ts-now)
	}

	state := ms.State()
	if state.Msgs != 1 {
		t.Fatalf("Expected 1 msg, got %d", state.Msgs)
	}
	expectedSize := memStoreMsgSize(subj, nil, msg)
	if state.Bytes != expectedSize {
		t.Fatalf("Expected %d bytes, got %d", expectedSize, state.Bytes)
	}
	sm, err := ms.LoadMsg(1, nil)
	if err != nil {
		t.Fatalf("Unexpected error looking up msg: %v", err)
	}
	if sm.subj != subj {
		t.Fatalf("Subjects don't match, original %q vs %q", subj, sm.subj)
	}
	if !bytes.Equal(sm.msg, msg) {
		t.Fatalf("Msgs don't match, original %q vs %q", msg, sm.msg)
	}
}

func TestMemStoreMsgLimit(t *testing.T) {
	ms, err := newMemStore(&StreamConfig{Storage: MemoryStorage, MaxMsgs: 10})
	require_NoError(t, err)
	defer ms.Stop()

	subj, msg := "foo", []byte("Hello World")
	for i := 0; i < 10; i++ {
		ms.StoreMsg(subj, nil, msg, 0)
	}
	state := ms.State()
	if state.Msgs != 10 {
		t.Fatalf("Expected %d msgs, got %d", 10, state.Msgs)
	}
	if _, _, err := ms.StoreMsg(subj, nil, msg, 0); err != nil {
		t.Fatalf("Error storing msg: %v", err)
	}
	state = ms.State()
	if state.Msgs != 10 {
		t.Fatalf("Expected %d msgs, got %d", 10, state.Msgs)
	}
	if state.LastSeq != 11 {
		t.Fatalf("Expected the last sequence to be 11 now, but got %d", state.LastSeq)
	}
	if state.FirstSeq != 2 {
		t.Fatalf("Expected the first sequence to be 2 now, but got %d", state.FirstSeq)
	}
	// Make sure we can not lookup seq 1.
	if _, err := ms.LoadMsg(1, nil); err == nil {
		t.Fatalf("Expected error looking up seq 1 but got none")
	}
}

func TestMemStoreBytesLimit(t *testing.T) {
	subj, msg := "foo", make([]byte, 512)
	storedMsgSize := memStoreMsgSize(subj, nil, msg)

	toStore := uint64(1024)
	maxBytes := storedMsgSize * toStore

	ms, err := newMemStore(&StreamConfig{Storage: MemoryStorage, MaxBytes: int64(maxBytes)})
	require_NoError(t, err)
	defer ms.Stop()

	for i := uint64(0); i < toStore; i++ {
		ms.StoreMsg(subj, nil, msg, 0)
	}
	state := ms.State()
	if state.Msgs != toStore {
		t.Fatalf("Expected %d msgs, got %d", toStore, state.Msgs)
	}
	if state.Bytes != storedMsgSize*toStore {
		t.Fatalf("Expected bytes to be %d, got %d", storedMsgSize*toStore, state.Bytes)
	}

	// Now send 10 more and check that bytes limit enforced.
	for i := 0; i < 10; i++ {
		if _, _, err := ms.StoreMsg(subj, nil, msg, 0); err != nil {
			t.Fatalf("Error storing msg: %v", err)
		}
	}
	state = ms.State()
	if state.Msgs != toStore {
		t.Fatalf("Expected %d msgs, got %d", toStore, state.Msgs)
	}
	if state.Bytes != storedMsgSize*toStore {
		t.Fatalf("Expected bytes to be %d, got %d", storedMsgSize*toStore, state.Bytes)
	}
	if state.FirstSeq != 11 {
		t.Fatalf("Expected first sequence to be 11, got %d", state.FirstSeq)
	}
	if state.LastSeq != toStore+10 {
		t.Fatalf("Expected last sequence to be %d, got %d", toStore+10, state.LastSeq)
	}
}

// https://github.com/nats-io/nats-server/issues/4771
func TestMemStoreBytesLimitWithDiscardNew(t *testing.T) {
	subj, msg := "tiny", make([]byte, 7)
	storedMsgSize := memStoreMsgSize(subj, nil, msg)

	toStore := uint64(3)
	maxBytes := 100

	ms, err := newMemStore(&StreamConfig{Storage: MemoryStorage, MaxBytes: int64(maxBytes), Discard: DiscardNew})
	require_NoError(t, err)
	defer ms.Stop()

	// Now send 10 messages and check that bytes limit enforced.
	for i := 0; i < 10; i++ {
		_, _, err := ms.StoreMsg(subj, nil, msg, 0)
		if i < int(toStore) {
			if err != nil {
				t.Fatalf("Error storing msg: %v", err)
			}
		} else if !errors.Is(err, ErrMaxBytes) {
			t.Fatalf("Storing msg should result in: %v", ErrMaxBytes)
		}
	}
	state := ms.State()
	if state.Msgs != toStore {
		t.Fatalf("Expected %d msgs, got %d", toStore, state.Msgs)
	}
	if state.Bytes != storedMsgSize*toStore {
		t.Fatalf("Expected bytes to be %d, got %d", storedMsgSize*toStore, state.Bytes)
	}
}

func TestMemStoreAgeLimit(t *testing.T) {
	maxAge := 10 * time.Millisecond
	ms, err := newMemStore(&StreamConfig{Storage: MemoryStorage, MaxAge: maxAge})
	require_NoError(t, err)
	defer ms.Stop()

	// Store some messages. Does not really matter how many.
	subj, msg := "foo", []byte("Hello World")
	toStore := 100
	for i := 0; i < toStore; i++ {
		ms.StoreMsg(subj, nil, msg, 0)
	}
	state := ms.State()
	if state.Msgs != uint64(toStore) {
		t.Fatalf("Expected %d msgs, got %d", toStore, state.Msgs)
	}
	checkExpired := func(t *testing.T) {
		t.Helper()
		checkFor(t, time.Second, maxAge, func() error {
			state = ms.State()
			if state.Msgs != 0 {
				return fmt.Errorf("Expected no msgs, got %d", state.Msgs)
			}
			if state.Bytes != 0 {
				return fmt.Errorf("Expected no bytes, got %d", state.Bytes)
			}
			return nil
		})
	}
	// Let them expire
	checkExpired(t)
	// Now add some more and make sure that timer will fire again.
	for i := 0; i < toStore; i++ {
		ms.StoreMsg(subj, nil, msg, 0)
	}
	state = ms.State()
	if state.Msgs != uint64(toStore) {
		t.Fatalf("Expected %d msgs, got %d", toStore, state.Msgs)
	}
	checkExpired(t)
}

func TestMemStoreTimeStamps(t *testing.T) {
	ms, err := newMemStore(&StreamConfig{Storage: MemoryStorage})
	require_NoError(t, err)
	defer ms.Stop()

	last := time.Now().UnixNano()
	subj, msg := "foo", []byte("Hello World")
	for i := 0; i < 10; i++ {
		time.Sleep(5 * time.Microsecond)
		ms.StoreMsg(subj, nil, msg, 0)
	}
	var smv StoreMsg
	for seq := uint64(1); seq <= 10; seq++ {
		sm, err := ms.LoadMsg(seq, &smv)
		if err != nil {
			t.Fatalf("Unexpected error looking up msg: %v", err)
		}
		// These should be different
		if sm.ts <= last {
			t.Fatalf("Expected different timestamps, got %v", sm.ts)
		}
		last = sm.ts
	}
}

func TestMemStorePurge(t *testing.T) {
	ms, err := newMemStore(&StreamConfig{Storage: MemoryStorage})
	require_NoError(t, err)
	defer ms.Stop()

	subj, msg := "foo", []byte("Hello World")
	for i := 0; i < 10; i++ {
		ms.StoreMsg(subj, nil, msg, 0)
	}
	if state := ms.State(); state.Msgs != 10 {
		t.Fatalf("Expected 10 msgs, got %d", state.Msgs)
	}
	ms.Purge()
	if state := ms.State(); state.Msgs != 0 {
		t.Fatalf("Expected no msgs, got %d", state.Msgs)
	}
}

func TestMemStoreCompact(t *testing.T) {
	ms, err := newMemStore(&StreamConfig{Storage: MemoryStorage})
	require_NoError(t, err)
	defer ms.Stop()

	subj, msg := "foo", []byte("Hello World")
	for i := 0; i < 10; i++ {
		ms.StoreMsg(subj, nil, msg, 0)
	}
	if state := ms.State(); state.Msgs != 10 {
		t.Fatalf("Expected 10 msgs, got %d", state.Msgs)
	}
	n, err := ms.Compact(6)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if n != 5 {
		t.Fatalf("Expected to have purged 5 msgs, got %d", n)
	}
	state := ms.State()
	if state.Msgs != 5 {
		t.Fatalf("Expected 5 msgs, got %d", state.Msgs)
	}
	if state.FirstSeq != 6 {
		t.Fatalf("Expected first seq of 6, got %d", state.FirstSeq)
	}
	// Now test that compact will also reset first if seq > last
	n, err = ms.Compact(100)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if n != 5 {
		t.Fatalf("Expected to have purged 5 msgs, got %d", n)
	}
	if state = ms.State(); state.FirstSeq != 100 {
		t.Fatalf("Expected first seq of 100, got %d", state.FirstSeq)
	}
}

func TestMemStoreEraseMsg(t *testing.T) {
	ms, err := newMemStore(&StreamConfig{Storage: MemoryStorage})
	require_NoError(t, err)
	defer ms.Stop()

	subj, msg := "foo", []byte("Hello World")
	ms.StoreMsg(subj, nil, msg, 0)
	sm, err := ms.LoadMsg(1, nil)
	if err != nil {
		t.Fatalf("Unexpected error looking up msg: %v", err)
	}
	if !bytes.Equal(msg, sm.msg) {
		t.Fatalf("Expected same msg, got %q vs %q", sm.msg, msg)
	}
	if removed, _ := ms.EraseMsg(1); !removed {
		t.Fatalf("Expected erase msg to return success")
	}
}

func TestMemStoreMsgHeaders(t *testing.T) {
	ms, err := newMemStore(&StreamConfig{Storage: MemoryStorage})
	require_NoError(t, err)
	defer ms.Stop()

	subj, hdr, msg := "foo", []byte("name:derek"), []byte("Hello World")
	if sz := int(memStoreMsgSize(subj, hdr, msg)); sz != (len(subj) + len(hdr) + len(msg) + 16) {
		t.Fatalf("Wrong size for stored msg with header")
	}
	ms.StoreMsg(subj, hdr, msg, 0)
	sm, err := ms.LoadMsg(1, nil)
	if err != nil {
		t.Fatalf("Unexpected error looking up msg: %v", err)
	}
	if !bytes.Equal(msg, sm.msg) {
		t.Fatalf("Expected same msg, got %q vs %q", sm.msg, msg)
	}
	if !bytes.Equal(hdr, sm.hdr) {
		t.Fatalf("Expected same hdr, got %q vs %q", sm.hdr, hdr)
	}
	if removed, _ := ms.EraseMsg(1); !removed {
		t.Fatalf("Expected erase msg to return success")
	}
}

func TestMemStoreStoreMsgPrivateCopy(t *testing.T) {
	tests := []struct {
		name string
		hdr  []byte
		msg  []byte
	}{
		{"body", nil, []byte("message")},
		{"header and body", []byte("NATS/1.0 200 OK\r\n\r\n"), []byte("message")},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ms, err := newMemStore(&StreamConfig{Name: "priv", Storage: MemoryStorage})
			require_NoError(t, err)
			defer ms.Stop()

			wantHdr := bytes.Clone(tt.hdr)
			wantMsg := bytes.Clone(tt.msg)
			seq, _, err := ms.StoreMsg("priv", tt.hdr, tt.msg, 0)
			require_NoError(t, err)

			for i := range tt.hdr {
				tt.hdr[i] = 0xEE
			}
			for i := range tt.msg {
				tt.msg[i] = 0xEE
			}

			sm, err := ms.LoadMsg(seq, nil)
			require_NoError(t, err)
			if !bytes.Equal(sm.hdr, wantHdr) {
				t.Fatalf("Stored header changed: got %q, want %q", sm.hdr, wantHdr)
			}
			if !bytes.Equal(sm.msg, wantMsg) {
				t.Fatalf("Stored message changed: got %q, want %q", sm.msg, wantMsg)
			}
		})
	}
}

func TestMemStoreStreamStateDeleted(t *testing.T) {
	ms, err := newMemStore(&StreamConfig{Storage: MemoryStorage})
	require_NoError(t, err)
	defer ms.Stop()

	subj, toStore := "foo", uint64(10)
	for i := uint64(1); i <= toStore; i++ {
		msg := []byte(fmt.Sprintf("[%08d] Hello World!", i))
		if _, _, err := ms.StoreMsg(subj, nil, msg, 0); err != nil {
			t.Fatalf("Error storing msg: %v", err)
		}
	}
	state := ms.State()
	if len(state.Deleted) != 0 {
		t.Fatalf("Expected deleted to be empty")
	}
	// Now remove some interior messages.
	var expected []uint64
	for seq := uint64(2); seq < toStore; seq += 2 {
		ms.RemoveMsg(seq)
		expected = append(expected, seq)
	}
	state = ms.State()
	if !reflect.DeepEqual(state.Deleted, expected) {
		t.Fatalf("Expected deleted to be %+v, got %+v\n", expected, state.Deleted)
	}
	// Now fill the gap by deleting 1 and 3
	ms.RemoveMsg(1)
	ms.RemoveMsg(3)
	expected = expected[2:]
	state = ms.State()
	if !reflect.DeepEqual(state.Deleted, expected) {
		t.Fatalf("Expected deleted to be %+v, got %+v\n", expected, state.Deleted)
	}
	if state.FirstSeq != 5 {
		t.Fatalf("Expected first seq to be 5, got %d", state.FirstSeq)
	}
	ms.Purge()
	if state = ms.State(); len(state.Deleted) != 0 {
		t.Fatalf("Expected no deleted after purge, got %+v\n", state.Deleted)
	}
}

func TestMemStoreStreamTruncate(t *testing.T) {
	ms, err := newMemStore(&StreamConfig{Storage: MemoryStorage})
	require_NoError(t, err)
	defer ms.Stop()

	tseq := uint64(50)

	subj, toStore := "foo", uint64(100)
	for i := uint64(1); i < tseq; i++ {
		_, _, err := ms.StoreMsg(subj, nil, []byte("ok"), 0)
		require_NoError(t, err)
	}
	subj = "bar"
	for i := tseq; i <= toStore; i++ {
		_, _, err := ms.StoreMsg(subj, nil, []byte("ok"), 0)
		require_NoError(t, err)
	}

	if state := ms.State(); state.Msgs != toStore {
		t.Fatalf("Expected %d msgs, got %d", toStore, state.Msgs)
	}

	if err := ms.Truncate(tseq); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if state := ms.State(); state.Msgs != tseq {
		t.Fatalf("Expected %d msgs, got %d", tseq, state.Msgs)
	}

	// Now make sure we report properly if we have some deleted interior messages.
	ms.RemoveMsg(10)
	ms.RemoveMsg(20)
	ms.RemoveMsg(30)
	ms.RemoveMsg(40)

	tseq = uint64(25)
	if err := ms.Truncate(tseq); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	state := ms.State()
	if state.Msgs != tseq-2 {
		t.Fatalf("Expected %d msgs, got %d", tseq-2, state.Msgs)
	}
	if state.NumSubjects != 1 {
		t.Fatalf("Expected only 1 subject, got %d", state.NumSubjects)
	}
	expected := []uint64{10, 20}
	if !reflect.DeepEqual(state.Deleted, expected) {
		t.Fatalf("Expected deleted to be %+v, got %+v\n", expected, state.Deleted)
	}
}

func TestMemStorePurgeExWithSubject(t *testing.T) {
	ms, err := newMemStore(&StreamConfig{Storage: MemoryStorage})
	require_NoError(t, err)
	defer ms.Stop()

	for i := 0; i < 100; i++ {
		_, _, err = ms.StoreMsg("foo", nil, nil, 0)
		require_NoError(t, err)
	}

	// This should purge all.
	ms.PurgeEx("foo", 0, 0)
	require_True(t, ms.State().Msgs == 0)
}

func TestMemStoreUpdateMaxMsgsPerSubject(t *testing.T) {
	cfg := &StreamConfig{
		Name:       "TEST",
		Storage:    MemoryStorage,
		Subjects:   []string{"foo"},
		MaxMsgsPer: 10,
	}
	ms, err := newMemStore(cfg)
	require_NoError(t, err)
	defer ms.Stop()

	// Make sure this is honored on an update.
	cfg.MaxMsgsPer = 50
	err = ms.UpdateConfig(cfg)
	require_NoError(t, err)

	numStored := 22
	for i := 0; i < numStored; i++ {
		_, _, err = ms.StoreMsg("foo", nil, nil, 0)
		require_NoError(t, err)
	}

	ss := ms.SubjectsState("foo")["foo"]
	if ss.Msgs != uint64(numStored) {
		t.Fatalf("Expected to have %d stored, got %d", numStored, ss.Msgs)
	}

	// Now make sure we trunk if setting to lower value.
	cfg.MaxMsgsPer = 10
	err = ms.UpdateConfig(cfg)
	require_NoError(t, err)

	ss = ms.SubjectsState("foo")["foo"]
	if ss.Msgs != 10 {
		t.Fatalf("Expected to have %d stored, got %d", 10, ss.Msgs)
	}
}

func TestMemStoreStreamTruncateReset(t *testing.T) {
	cfg := &StreamConfig{
		Name:     "TEST",
		Storage:  MemoryStorage,
		Subjects: []string{"foo"},
	}
	ms, err := newMemStore(cfg)
	require_NoError(t, err)
	defer ms.Stop()

	subj, msg := "foo", []byte("Hello World")
	for i := 0; i < 1000; i++ {
		_, _, err := ms.StoreMsg(subj, nil, msg, 0)
		require_NoError(t, err)
	}

	// Reset everything
	require_NoError(t, ms.Truncate(0))

	state := ms.State()
	require_True(t, state.Msgs == 0)
	require_True(t, state.Bytes == 0)
	require_True(t, state.FirstSeq == 0)
	require_True(t, state.LastSeq == 0)
	require_True(t, state.NumSubjects == 0)
	require_True(t, state.NumDeleted == 0)

	for i := 0; i < 1000; i++ {
		_, _, err := ms.StoreMsg(subj, nil, msg, 0)
		require_NoError(t, err)
	}

	state = ms.State()
	require_True(t, state.Msgs == 1000)
	require_True(t, state.Bytes == 30000)
	require_True(t, state.FirstSeq == 1)
	require_True(t, state.LastSeq == 1000)
	require_True(t, state.NumSubjects == 1)
	require_True(t, state.NumDeleted == 0)
}

func TestMemStoreStreamCompactMultiBlockSubjectInfo(t *testing.T) {
	cfg := &StreamConfig{
		Name:     "TEST",
		Storage:  MemoryStorage,
		Subjects: []string{"foo.*"},
	}
	ms, err := newMemStore(cfg)
	require_NoError(t, err)
	defer ms.Stop()

	for i := 0; i < 1000; i++ {
		subj := fmt.Sprintf("foo.%d", i)
		_, _, err := ms.StoreMsg(subj, nil, []byte("Hello World"), 0)
		require_NoError(t, err)
	}

	// Compact such that we know we throw blocks away from the beginning.
	deleted, err := ms.Compact(501)
	require_NoError(t, err)
	require_True(t, deleted == 500)

	// Make sure we adjusted for subjects etc.
	state := ms.State()
	require_True(t, state.NumSubjects == 500)
}

func TestMemStoreSubjectsTotals(t *testing.T) {
	cfg := &StreamConfig{
		Name:     "TEST",
		Storage:  MemoryStorage,
		Subjects: []string{"*.*"},
	}
	ms, err := newMemStore(cfg)
	require_NoError(t, err)
	defer ms.Stop()

	fmap := make(map[int]int)
	bmap := make(map[int]int)

	var m map[int]int
	var ft string

	for i := 0; i < 10_000; i++ {
		// Flip coin for prefix
		if rand.IntN(2) == 0 {
			ft, m = "foo", fmap
		} else {
			ft, m = "bar", bmap
		}
		dt := rand.IntN(100)
		subj := fmt.Sprintf("%s.%d", ft, dt)
		m[dt]++

		_, _, err := ms.StoreMsg(subj, nil, []byte("Hello World"), 0)
		require_NoError(t, err)
	}

	// Now test SubjectsTotal
	for dt, total := range fmap {
		subj := fmt.Sprintf("foo.%d", dt)
		m := ms.SubjectsTotals(subj)
		if m[subj] != uint64(total) {
			t.Fatalf("Expected %q to have %d total, got %d", subj, total, m[subj])
		}
	}

	// Check fmap.
	if st := ms.SubjectsTotals("foo.*"); len(st) != len(fmap) {
		t.Fatalf("Expected %d subjects for %q, got %d", len(fmap), "foo.*", len(st))
	} else {
		expected := 0
		for _, n := range fmap {
			expected += n
		}
		received := uint64(0)
		for _, n := range st {
			received += n
		}
		if received != uint64(expected) {
			t.Fatalf("Expected %d total but got %d", expected, received)
		}
	}

	// Check bmap.
	if st := ms.SubjectsTotals("bar.*"); len(st) != len(bmap) {
		t.Fatalf("Expected %d subjects for %q, got %d", len(bmap), "bar.*", len(st))
	} else {
		expected := 0
		for _, n := range bmap {
			expected += n
		}
		received := uint64(0)
		for _, n := range st {
			received += n
		}
		if received != uint64(expected) {
			t.Fatalf("Expected %d total but got %d", expected, received)
		}
	}

	// All with pwc match.
	if st, expected := ms.SubjectsTotals("*.*"), len(bmap)+len(fmap); len(st) != expected {
		t.Fatalf("Expected %d subjects for %q, got %d", expected, "*.*", len(st))
	}
}

func TestMemStoreNumPending(t *testing.T) {
	cfg := &StreamConfig{
		Name:     "TEST",
		Storage:  MemoryStorage,
		Subjects: []string{"*.*.*.*"},
	}
	ms, err := newMemStore(cfg)
	require_NoError(t, err)
	defer ms.Stop()

	tokens := []string{"foo", "bar", "baz"}
	genSubj := func() string {
		return fmt.Sprintf("%s.%s.%s.%s",
			tokens[rand.IntN(len(tokens))],
			tokens[rand.IntN(len(tokens))],
			tokens[rand.IntN(len(tokens))],
			tokens[rand.IntN(len(tokens))],
		)
	}

	for i := 0; i < 50_000; i++ {
		subj := genSubj()
		_, _, err := ms.StoreMsg(subj, nil, []byte("Hello World"), 0)
		require_NoError(t, err)
	}

	state := ms.State()

	// Scan one by one for sanity check against other calculations.
	sanityCheck := func(sseq uint64, filter string) SimpleState {
		t.Helper()
		var ss SimpleState
		var smv StoreMsg
		// For here we know 0 is invalid, set to 1.
		if sseq == 0 {
			sseq = 1
		}
		for seq := sseq; seq <= state.LastSeq; seq++ {
			sm, err := ms.LoadMsg(seq, &smv)
			if err != nil {
				t.Logf("Encountered error %v loading sequence: %d", err, seq)
				continue
			}
			if subjectIsSubsetMatch(sm.subj, filter) {
				ss.Msgs++
				ss.Last = seq
				if ss.First == 0 || seq < ss.First {
					ss.First = seq
				}
			}
		}
		return ss
	}

	check := func(sseq uint64, filter string) {
		t.Helper()
		np, lvs, err := ms.NumPending(sseq, filter, false)
		if err != nil {
			t.Fatalf("NumPending error: %v", err)
		}
		ss, err := ms.FilteredState(sseq, filter)
		require_NoError(t, err)
		sss := sanityCheck(sseq, filter)
		if lvs != state.LastSeq {
			t.Fatalf("Expected NumPending to return valid through last of %d but got %d", state.LastSeq, lvs)
		}
		if ss.Msgs != np {
			t.Fatalf("NumPending of %d did not match ss.Msgs of %d", np, ss.Msgs)
		}
		if ss != sss {
			t.Fatalf("Failed sanity check, expected %+v got %+v", sss, ss)
		}
	}

	sanityCheckLastOnly := func(sseq uint64, filter string) SimpleState {
		t.Helper()
		var ss SimpleState
		var smv StoreMsg
		// For here we know 0 is invalid, set to 1.
		if sseq == 0 {
			sseq = 1
		}
		seen := make(map[string]bool)
		for seq := state.LastSeq; seq >= sseq; seq-- {
			sm, err := ms.LoadMsg(seq, &smv)
			if err != nil {
				t.Logf("Encountered error %v loading sequence: %d", err, seq)
				continue
			}
			if !seen[sm.subj] && subjectIsSubsetMatch(sm.subj, filter) {
				ss.Msgs++
				if ss.Last == 0 {
					ss.Last = seq
				}
				if ss.First == 0 || seq < ss.First {
					ss.First = seq
				}
				seen[sm.subj] = true
			}
		}
		return ss
	}

	checkLastOnly := func(sseq uint64, filter string) {
		t.Helper()
		np, lvs, err := ms.NumPending(sseq, filter, true)
		if err != nil {
			t.Fatalf("NumPending error: %v", err)
		}
		ss := sanityCheckLastOnly(sseq, filter)
		if lvs != state.LastSeq {
			t.Fatalf("Expected NumPending to return valid through last of %d but got %d", state.LastSeq, lvs)
		}
		if ss.Msgs != np {
			t.Fatalf("NumPending of %d did not match ss.Msgs of %d", np, ss.Msgs)
		}
	}

	startSeqs := []uint64{0, 1, 2, 200, 444, 555, 2222, 8888, 12_345, 28_222, 33_456, 44_400, 49_999}
	checkSubs := []string{"foo.>", "*.bar.>", "foo.bar.*.baz", "*.bar.>", "*.foo.bar.*", "foo.foo.bar.baz"}

	for _, filter := range checkSubs {
		for _, start := range startSeqs {
			check(start, filter)
			checkLastOnly(start, filter)
		}
	}
}

func TestMemStoreInitialFirstSeq(t *testing.T) {
	cfg := &StreamConfig{
		Name:     "zzz",
		Storage:  MemoryStorage,
		FirstSeq: 1000,
	}
	ms, err := newMemStore(cfg)
	require_NoError(t, err)
	defer ms.Stop()

	seq, _, err := ms.StoreMsg("A", nil, []byte("OK"), 0)
	require_NoError(t, err)
	if seq != 1000 {
		t.Fatalf("Message should have been sequence 1000 but was %d", seq)
	}

	seq, _, err = ms.StoreMsg("B", nil, []byte("OK"), 0)
	require_NoError(t, err)
	if seq != 1001 {
		t.Fatalf("Message should have been sequence 1001 but was %d", seq)
	}

	var state StreamState
	ms.FastState(&state)
	switch {
	case state.Msgs != 2:
		t.Fatalf("Expected 2 messages, got %d", state.Msgs)
	case state.FirstSeq != 1000:
		t.Fatalf("Expected first seq 1000, got %d", state.FirstSeq)
	case state.LastSeq != 1001:
		t.Fatalf("Expected last seq 1001, got %d", state.LastSeq)
	}
}

func TestMemStoreDeleteBlocks(t *testing.T) {
	cfg := &StreamConfig{
		Name:     "zzz",
		Subjects: []string{"*"},
		Storage:  MemoryStorage,
	}
	ms, err := newMemStore(cfg)
	require_NoError(t, err)
	defer ms.Stop()

	// Put in 10_000 msgs.
	total := 10_000
	for i := 0; i < total; i++ {
		_, _, err := ms.StoreMsg("A", nil, []byte("OK"), 0)
		require_NoError(t, err)
	}

	// Now pick 5k random sequences.
	delete := 5000
	deleteMap := make(map[int]struct{}, delete)
	for len(deleteMap) < delete {
		deleteMap[rand.IntN(total)+1] = struct{}{}
	}
	// Now remove?
	for seq := range deleteMap {
		ms.RemoveMsg(uint64(seq))
	}

	var state StreamState
	ms.FastState(&state)

	// For now we just track via one dmap.
	ms.mu.RLock()
	dmap := ms.dmap.Clone()
	ms.mu.RUnlock()

	require_True(t, dmap.Size() == state.NumDeleted)
}

// https://github.com/nats-io/nats-server/issues/4850
func TestMemStoreGetSeqFromTimeWithLastDeleted(t *testing.T) {
	cfg := &StreamConfig{
		Name:     "zzz",
		Subjects: []string{"*"},
		Storage:  MemoryStorage,
	}
	ms, err := newMemStore(cfg)
	require_NoError(t, err)
	defer ms.Stop()

	// Put in 1000 msgs.
	total := 1000
	var st time.Time
	for i := 1; i <= total; i++ {
		_, _, err := ms.StoreMsg("A", nil, []byte("OK"), 0)
		require_NoError(t, err)
		if i == total/2 {
			time.Sleep(100 * time.Millisecond)
			st = time.Now()
		}
	}
	// Delete last 100
	for seq := total - 100; seq <= total; seq++ {
		ms.RemoveMsg(uint64(seq))
	}

	// Make sure this does not panic with last sequence no longer accessible.
	seq := ms.GetSeqFromTime(st)
	// Make sure we get the right value.
	require_Equal(t, seq, 501)
}

func TestMemStoreSkipMsgs(t *testing.T) {
	cfg := &StreamConfig{
		Name:     "zzz",
		Subjects: []string{"*"},
		Storage:  MemoryStorage,
	}
	ms, err := newMemStore(cfg)
	require_NoError(t, err)
	defer ms.Stop()

	// Test on empty FS first.
	// Make sure wrong starting sequence fails.
	err = ms.SkipMsgs(10, 100)
	require_Error(t, err, ErrSequenceMismatch)

	err = ms.SkipMsgs(1, 100)
	require_NoError(t, err)

	state := ms.State()
	require_Equal(t, state.FirstSeq, 101)
	require_Equal(t, state.LastSeq, 100)

	// Now add alot.
	err = ms.SkipMsgs(101, 100_000)
	require_NoError(t, err)
	state = ms.State()
	require_Equal(t, state.FirstSeq, 100_101)
	require_Equal(t, state.LastSeq, 100_100)

	// Now add in a message, and then skip to check dmap.
	ms, err = newMemStore(cfg)
	require_NoError(t, err)
	ms.StoreMsg("foo", nil, nil, 0)

	err = ms.SkipMsgs(2, 10)
	require_NoError(t, err)
	state = ms.State()
	require_Equal(t, state.FirstSeq, 1)
	require_Equal(t, state.LastSeq, 11)
	require_Equal(t, state.Msgs, 1)
	require_Equal(t, state.NumDeleted, 10)
	require_Equal(t, len(state.Deleted), 10)

	// Check Fast State too.
	state.Deleted = nil
	ms.FastState(&state)
	require_Equal(t, state.FirstSeq, 1)
	require_Equal(t, state.LastSeq, 11)
	require_Equal(t, state.Msgs, 1)
	require_Equal(t, state.NumDeleted, 10)
}

func TestMemStoreMultiLastSeqs(t *testing.T) {
	cfg := &StreamConfig{
		Name:     "zzz",
		Subjects: []string{"foo.*", "bar.*"},
		Storage:  MemoryStorage,
	}
	ms, err := newMemStore(cfg)
	require_NoError(t, err)
	defer ms.Stop()

	msg := []byte("abc")
	for i := 0; i < 33; i++ {
		ms.StoreMsg("foo.foo", nil, msg, 0)
		ms.StoreMsg("foo.bar", nil, msg, 0)
		ms.StoreMsg("foo.baz", nil, msg, 0)
	}
	for i := 0; i < 33; i++ {
		ms.StoreMsg("bar.foo", nil, msg, 0)
		ms.StoreMsg("bar.bar", nil, msg, 0)
		ms.StoreMsg("bar.baz", nil, msg, 0)
	}

	checkResults := func(seqs, expected []uint64) {
		t.Helper()
		if len(seqs) != len(expected) {
			t.Fatalf("Expected %+v got %+v", expected, seqs)
		}
		for i := range seqs {
			if seqs[i] != expected[i] {
				t.Fatalf("Expected %+v got %+v", expected, seqs)
			}
		}
	}

	// UpTo sequence 3. Tests block split.
	seqs, err := ms.MultiLastSeqs([]string{"foo.*"}, 3, -1)
	require_NoError(t, err)
	checkResults(seqs, []uint64{1, 2, 3})
	// Up to last sequence of the stream.
	seqs, err = ms.MultiLastSeqs([]string{"foo.*"}, 0, -1)
	require_NoError(t, err)
	checkResults(seqs, []uint64{97, 98, 99})
	// Check for bar.* at the end.
	seqs, err = ms.MultiLastSeqs([]string{"bar.*"}, 0, -1)
	require_NoError(t, err)
	checkResults(seqs, []uint64{196, 197, 198})
	// This should find nothing.
	seqs, err = ms.MultiLastSeqs([]string{"bar.*"}, 99, -1)
	require_NoError(t, err)
	checkResults(seqs, nil)

	// Do multiple subjects explicitly.
	seqs, err = ms.MultiLastSeqs([]string{"foo.foo", "foo.bar", "foo.baz"}, 3, -1)
	require_NoError(t, err)
	checkResults(seqs, []uint64{1, 2, 3})
	seqs, err = ms.MultiLastSeqs([]string{"foo.foo", "foo.bar", "foo.baz"}, 0, -1)
	require_NoError(t, err)
	checkResults(seqs, []uint64{97, 98, 99})
	seqs, err = ms.MultiLastSeqs([]string{"bar.foo", "bar.bar", "bar.baz"}, 0, -1)
	require_NoError(t, err)
	checkResults(seqs, []uint64{196, 197, 198})
	seqs, err = ms.MultiLastSeqs([]string{"bar.foo", "bar.bar", "bar.baz"}, 99, -1)
	require_NoError(t, err)
	checkResults(seqs, nil)

	// Check single works
	seqs, err = ms.MultiLastSeqs([]string{"foo.foo"}, 3, -1)
	require_NoError(t, err)
	checkResults(seqs, []uint64{1})

	// Now test that we properly de-duplicate between filters.
	seqs, err = ms.MultiLastSeqs([]string{"foo.*", "foo.bar"}, 3, -1)
	require_NoError(t, err)
	checkResults(seqs, []uint64{1, 2, 3})
	seqs, err = ms.MultiLastSeqs([]string{"bar.>", "bar.bar", "bar.baz"}, 0, -1)
	require_NoError(t, err)
	checkResults(seqs, []uint64{196, 197, 198})

	// All
	seqs, err = ms.MultiLastSeqs([]string{">"}, 0, -1)
	require_NoError(t, err)
	checkResults(seqs, []uint64{97, 98, 99, 196, 197, 198})
	seqs, err = ms.MultiLastSeqs([]string{">"}, 99, -1)
	require_NoError(t, err)
	checkResults(seqs, []uint64{97, 98, 99})
}

func TestMemStoreMultiLastSeqsMaxAllowed(t *testing.T) {
	cfg := &StreamConfig{
		Name:     "zzz",
		Subjects: []string{"foo.*"},
		Storage:  MemoryStorage,
	}
	ms, err := newMemStore(cfg)
	require_NoError(t, err)
	defer ms.Stop()

	msg := []byte("abc")
	for i := 1; i <= 100; i++ {
		ms.StoreMsg(fmt.Sprintf("foo.%d", i), nil, msg, 0)
	}
	// Test that if we specify maxAllowed that we get the correct error.
	seqs, err := ms.MultiLastSeqs([]string{"foo.*"}, 0, 10)
	require_True(t, seqs == nil)
	require_Error(t, err, ErrTooManyResults)
}

// Bug would cause PurgeEx to fail if it encountered a deleted msg at sequence to delete up to.
func TestMemStorePurgeExWithDeletedMsgs(t *testing.T) {
	cfg := &StreamConfig{
		Name:     "zzz",
		Subjects: []string{"foo"},
		Storage:  MemoryStorage,
	}
	ms, err := newMemStore(cfg)
	require_NoError(t, err)
	defer ms.Stop()

	msg := []byte("abc")
	for i := 1; i <= 10; i++ {
		ms.StoreMsg("foo", nil, msg, 0)
	}
	ms.RemoveMsg(2)
	ms.RemoveMsg(9) // This was the bug

	n, err := ms.PurgeEx(_EMPTY_, 9, 0)
	require_NoError(t, err)
	require_Equal(t, n, 7)

	var state StreamState
	ms.FastState(&state)
	require_Equal(t, state.FirstSeq, 10)
	require_Equal(t, state.LastSeq, 10)
	require_Equal(t, state.Msgs, 1)
}

// When all messages are deleted we should have a state of first = last + 1.
func TestMemStoreDeleteAllFirstSequenceCheck(t *testing.T) {
	cfg := &StreamConfig{
		Name:     "zzz",
		Subjects: []string{"foo"},
		Storage:  MemoryStorage,
	}
	ms, err := newMemStore(cfg)
	require_NoError(t, err)
	defer ms.Stop()

	msg := []byte("abc")
	for i := 1; i <= 10; i++ {
		ms.StoreMsg("foo", nil, msg, 0)
	}
	for seq := uint64(1); seq <= 10; seq++ {
		ms.RemoveMsg(seq)
	}
	var state StreamState
	ms.FastState(&state)
	require_Equal(t, state.FirstSeq, 11)
	require_Equal(t, state.LastSeq, 10)
	require_Equal(t, state.Msgs, 0)
}

func TestMemStoreNumPendingMulti(t *testing.T) {
	cfg := &StreamConfig{
		Name:     "zzz",
		Subjects: []string{"ev.*"},
		Storage:  MemoryStorage,
	}
	ms, err := newMemStore(cfg)
	require_NoError(t, err)
	defer ms.Stop()

	totalMsgs := 100_000
	totalSubjects := 10_000
	numFiltered := 5000
	startSeq := uint64(5_000 + rand.IntN(90_000))

	subjects := make([]string, 0, totalSubjects)
	for i := 0; i < totalSubjects; i++ {
		subjects = append(subjects, fmt.Sprintf("ev.%s", nuid.Next()))
	}

	// Put in 100k msgs with random subjects.
	msg := bytes.Repeat([]byte("ZZZ"), 333)
	for i := 0; i < totalMsgs; i++ {
		_, _, err = ms.StoreMsg(subjects[rand.IntN(totalSubjects)], nil, msg, 0)
		require_NoError(t, err)
	}

	// Now we want to do a calculate NumPendingMulti.
	filters := gsl.NewSublist[struct{}]()
	for filters.Count() < uint32(numFiltered) {
		filter := subjects[rand.IntN(totalSubjects)]
		if !filters.HasInterest(filter) {
			filters.Insert(filter, struct{}{})
		}
	}

	// Use new function.
	total, _, err := ms.NumPendingMulti(startSeq, filters, false)
	require_NoError(t, err)

	// Check our results.
	var checkTotal uint64
	var smv StoreMsg
	for seq := startSeq; seq <= uint64(totalMsgs); seq++ {
		sm, err := ms.LoadMsg(seq, &smv)
		require_NoError(t, err)
		if filters.HasInterest(sm.subj) {
			checkTotal++
		}
	}
	require_Equal(t, total, checkTotal)
}

func TestMemStoreNumPendingBug(t *testing.T) {
	cfg := &StreamConfig{
		Name:     "zzz",
		Subjects: []string{"foo.*"},
		Storage:  MemoryStorage,
	}
	ms, err := newMemStore(cfg)
	require_NoError(t, err)
	defer ms.Stop()

	// 12 msgs total
	for _, subj := range []string{"foo.foo", "foo.bar", "foo.baz", "foo.zzz"} {
		ms.StoreMsg("foo.aaa", nil, nil, 0)
		ms.StoreMsg(subj, nil, nil, 0)
		ms.StoreMsg(subj, nil, nil, 0)
	}
	total, _, err := ms.NumPending(4, "foo.*", false)
	require_NoError(t, err)

	var checkTotal uint64
	var smv StoreMsg
	for seq := 4; seq <= 12; seq++ {
		sm, err := ms.LoadMsg(uint64(seq), &smv)
		require_NoError(t, err)
		if subjectIsSubsetMatch(sm.subj, "foo.*") {
			checkTotal++
		}
	}
	require_Equal(t, total, checkTotal)
}

func TestMemStorePurgeLeaksDmap(t *testing.T) {
	cfg := &StreamConfig{
		Name:     "TEST",
		Subjects: []string{"foo"},
		Storage:  MemoryStorage,
	}
	ms, err := newMemStore(cfg)
	require_NoError(t, err)
	defer ms.Stop()

	for i := 0; i < 10; i++ {
		_, _, err = ms.StoreMsg("foo", nil, nil, 0)
		require_NoError(t, err)
	}

	for i := uint64(2); i <= 9; i++ {
		_, err = ms.RemoveMsg(i)
		require_NoError(t, err)
	}
	ms.mu.Lock()
	dmaps := ms.dmap.Size()
	ms.mu.Unlock()
	require_Equal(t, dmaps, 8)

	purged, err := ms.Purge()
	require_NoError(t, err)
	require_Equal(t, purged, 2)

	ms.mu.Lock()
	dmaps = ms.dmap.Size()
	ms.mu.Unlock()
	require_Equal(t, dmaps, 0)
}

func TestMemStoreMessageTTL(t *testing.T) {
	fs, err := newMemStore(
		&StreamConfig{Name: "zzz", Subjects: []string{"test"}, Storage: MemoryStorage, AllowMsgTTL: true},
	)
	require_NoError(t, err)
	defer fs.Stop()

	ttl := int64(1) // 1 second

	for i := 1; i <= 10; i++ {
		_, _, err = fs.StoreMsg("test", nil, nil, ttl)
		require_NoError(t, err)
	}

	var ss StreamState
	fs.FastState(&ss)
	require_Equal(t, ss.FirstSeq, 1)
	require_Equal(t, ss.LastSeq, 10)
	require_Equal(t, ss.Msgs, 10)

	time.Sleep(time.Second * 2)

	fs.FastState(&ss)
	require_Equal(t, ss.FirstSeq, 11)
	require_Equal(t, ss.LastSeq, 10)
	require_Equal(t, ss.Msgs, 0)
}

func TestMemStoreSubjectDeleteMarkers(t *testing.T) {
	fs, err := newMemStore(
		&StreamConfig{
			Name: "zzz", Subjects: []string{"test"}, Storage: MemoryStorage,
			MaxAge: time.Second, AllowMsgTTL: true,
			SubjectDeleteMarkerTTL: time.Second,
		},
	)
	require_NoError(t, err)
	defer fs.Stop()

	// Capture subject delete marker proposals.
	ch := make(chan *inMsg, 1)
	fs.rmcb = func(seq uint64) {
		_, err := fs.RemoveMsg(seq)
		require_NoError(t, err)
	}
	fs.pmsgcb = func(im *inMsg) {
		ch <- im
	}

	// Store three messages that will expire because of MaxAge.
	for i := 0; i < 3; i++ {
		_, _, err = fs.StoreMsg("test", nil, nil, 0)
		require_NoError(t, err)
	}

	// Wait for MaxAge to pass.
	time.Sleep(time.Second + time.Millisecond*500)

	// We should have placed a subject delete marker.
	im := require_ChanRead(t, ch, time.Second*5)
	require_Equal(t, bytesToString(getHeader(JSMarkerReason, im.hdr)), JSMarkerReasonMaxAge)
	require_Equal(t, bytesToString(getHeader(JSMessageTTL, im.hdr)), "1s")
}

func TestMemStoreAllLastSeqs(t *testing.T) {
	cfg := &StreamConfig{
		Name:       "zzz",
		Subjects:   []string{"*.*"},
		MaxMsgsPer: 50,
		Storage:    MemoryStorage,
	}
	ms, err := newMemStore(cfg)
	require_NoError(t, err)
	defer ms.Stop()

	subjs := []string{"foo.foo", "foo.bar", "foo.baz", "bar.foo", "bar.bar", "bar.baz"}
	msg := []byte("abc")

	for i := 0; i < 100_000; i++ {
		subj := subjs[rand.IntN(len(subjs))]
		ms.StoreMsg(subj, nil, msg, 0)
	}

	expected := make([]uint64, 0, len(subjs))
	var smv StoreMsg
	for _, subj := range subjs {
		sm, err := ms.LoadLastMsg(subj, &smv)
		require_NoError(t, err)
		expected = append(expected, sm.seq)
	}
	slices.Sort(expected)

	seqs, err := ms.AllLastSeqs()
	require_NoError(t, err)
	require_True(t, reflect.DeepEqual(seqs, expected))
}

func TestMemStoreMessageTTLRemovedOutOfBandDoesNotLeakTHW(t *testing.T) {
	ms, err := newMemStore(&StreamConfig{Name: "zzz", Subjects: []string{"test.>"}, Storage: MemoryStorage, AllowMsgTTL: true, AllowRollup: true})
	require_NoError(t, err)
	defer ms.Stop()

	ttl := int64(1) // 1 second

	for i := 1; i <= 10; i++ {
		_, _, err = ms.StoreMsg("test.a", nil, nil, ttl)
		require_NoError(t, err)
	}

	ms.mu.RLock()
	count := ms.ttls.Count()
	ms.mu.RUnlock()
	require_Equal(t, count, 10)

	// Remove the messages out of band, the way a rollup or a subject purge does.
	purged, err := ms.PurgeEx("test.a", 0, 0)
	require_NoError(t, err)
	require_Equal(t, purged, 10)

	// Once the TTLs are due, the expiry pass must drop the entries for messages
	// that are already gone instead of retrying them on every pass forever.
	time.Sleep(time.Second * 2)
	ms.expireMsgs()

	ms.mu.RLock()
	count = ms.ttls.Count()
	ms.mu.RUnlock()
	require_Equal(t, count, 0)
}

func TestMemStoreUpdateConfigTTLState(t *testing.T) {
	cfg := &StreamConfig{
		Name:     "zzz",
		Subjects: []string{">"},
		Storage:  MemoryStorage,
	}
	ms, err := newMemStore(cfg)
	require_NoError(t, err)
	defer ms.Stop()
	require_Equal(t, ms.ttls, nil)

	cfg.AllowMsgTTL = true
	require_NoError(t, ms.UpdateConfig(cfg))
	require_NotEqual(t, ms.ttls, nil)

	cfg.AllowMsgTTL = false
	require_NoError(t, ms.UpdateConfig(cfg))
	require_Equal(t, ms.ttls, nil)
}

func TestMemStoreSubjectForSeq(t *testing.T) {
	cfg := StreamConfig{
		Name:     "foo",
		Subjects: []string{"foo.>"},
		Storage:  MemoryStorage,
	}
	ms, err := newMemStore(&cfg)
	require_NoError(t, err)

	seq, _, err := ms.StoreMsg("foo.bar", nil, nil, 0)
	require_NoError(t, err)
	require_Equal(t, seq, 1)

	_, err = ms.SubjectForSeq(0)
	require_Error(t, err, ErrStoreMsgNotFound)

	subj, err := ms.SubjectForSeq(1)
	require_NoError(t, err)
	require_Equal(t, subj, "foo.bar")

	_, err = ms.SubjectForSeq(2)
	require_Error(t, err, ErrStoreMsgNotFound)
}

func TestMemStoreMessageSchedule(t *testing.T) {
	fs, err := newMemStore(
		&StreamConfig{
			Name: "TEST", Subjects: []string{"foo.*"}, Storage: MemoryStorage,
			AllowMsgSchedules: true,
		},
	)
	require_NoError(t, err)
	defer fs.Stop()

	// Capture message schedule proposals.
	ch := make(chan *inMsg, 1)
	fs.pmsgcb = func(im *inMsg) {
		ch <- im
	}

	// Store a single message schedule.
	schedule := time.Now().Add(time.Second).Format(time.RFC3339Nano)
	hdr := genHeader(nil, JSSchedulePattern, fmt.Sprintf("@at %s", schedule))
	hdr = genHeader(hdr, JSScheduleTarget, "foo.target")
	_, _, err = fs.StoreMsg("foo.schedule", hdr, nil, 0)
	require_NoError(t, err)

	// We should have published a scheduled message.
	im := require_ChanRead(t, ch, time.Second*5)
	require_Equal(t, im.subj, "foo.target")
	require_Equal(t, bytesToString(getHeader(JSScheduler, im.hdr)), "foo.schedule")
	require_Equal(t, bytesToString(getHeader(JSScheduleNext, im.hdr)), JSScheduleNextPurge)
}

func TestMemStoreMessageScheduleEverySubSecondPrecision(t *testing.T) {
	fs, err := newMemStore(
		&StreamConfig{
			Name: "TEST", Subjects: []string{"foo.*"}, Storage: MemoryStorage,
			AllowMsgSchedules: true,
		},
	)
	require_NoError(t, err)
	defer fs.Stop()

	// Capture message schedule proposals.
	ch := make(chan *inMsg, 1)
	fs.pmsgcb = func(im *inMsg) {
		ch <- im
	}

	// Store a repeating message schedule with a sub-second remainder.
	hdr := genHeader(nil, JSSchedulePattern, "@every 1500ms")
	hdr = genHeader(hdr, JSScheduleTarget, "foo.target")
	_, ts, err := fs.StoreMsg("foo.schedule", hdr, nil, 0)
	require_NoError(t, err)

	// We should have published a scheduled message.
	im := require_ChanRead(t, ch, time.Second*3)
	require_Equal(t, im.subj, "foo.target")
	require_Equal(t, bytesToString(getHeader(JSScheduler, im.hdr)), "foo.schedule")

	// The re-arm header is written with RFC3339Nano so the sub-second component
	// of the next fire time survives the round trip through the header. Each
	// fire time anchors on the previous one rounded to the nearest second, plus
	// the interval: the first fire is round(store time) + 1500ms, and the
	// re-arm rounds that again (up, .5s rounds away from zero) before adding
	// another 1500ms.
	const interval = 1500 * time.Millisecond
	first := time.Unix(0, ts).UTC().Round(time.Second).Add(interval)
	expected := first.Round(time.Second).Add(interval)
	scheduleNext := bytesToString(getHeader(JSScheduleNext, im.hdr))
	next, err := time.Parse(time.RFC3339Nano, scheduleNext)
	require_NoError(t, err)
	require_Equal(t, next.UnixNano(), expected.UnixNano())
}

func TestMemStoreNextWildcardMatch(t *testing.T) {
	cfg := &StreamConfig{
		Name:     "zzz",
		Subjects: []string{"foo.>"},
		Storage:  MemoryStorage,
	}
	ms, err := newMemStore(cfg)
	require_NoError(t, err)
	defer ms.Stop()

	msg := []byte("msg")
	storeN := func(subj string, n int) {
		t.Helper()
		for range n {
			_, _, err := ms.StoreMsg(subj, nil, msg, 0)
			require_NoError(t, err)
		}
	}
	storeN("foo.bar.a", 1)
	storeN("foo.baz.bar", 10)
	storeN("foo.bar.b", 1)
	storeN("foo.baz.bar", 10)
	storeN("foo.baz.bar.no.match", 10)

	ms.mu.Lock()
	defer ms.mu.Unlock()

	first, last, found := ms.nextWildcardMatchLocked("foo.bar.*", 0)
	require_True(t, found)
	require_Equal(t, first, 1)
	require_Equal(t, last, 12)

	first, last, found = ms.nextWildcardMatchLocked("foo.bar.*", 1)
	require_True(t, found)
	require_Equal(t, first, 1)
	require_Equal(t, last, 12)

	first, last, found = ms.nextWildcardMatchLocked("foo.bar.*", 2)
	require_True(t, found)
	require_Equal(t, first, 12)
	require_Equal(t, last, 12)

	_, _, found = ms.nextWildcardMatchLocked("foo.bar.*", first+1)
	require_False(t, found)

	first, last, found = ms.nextWildcardMatchLocked("foo.baz.*", 1)
	require_True(t, found)
	require_Equal(t, first, 2)
	require_Equal(t, last, 22)

	first, last, found = ms.nextWildcardMatchLocked("foo.baz.*", 11)
	require_True(t, found)
	require_Equal(t, first, 11)
	require_Equal(t, last, 22)

	first, last, found = ms.nextWildcardMatchLocked("foo.baz.*", 12)
	require_True(t, found)
	require_Equal(t, first, 12)
	require_Equal(t, last, 22)

	first, last, found = ms.nextWildcardMatchLocked("foo.baz.*", 22)
	require_True(t, found)
	require_Equal(t, first, 22)
	require_Equal(t, last, 22)

	first, last, found = ms.nextWildcardMatchLocked("foo.baz.*", 23)
	require_False(t, found)
	require_Equal(t, first, 0)
	require_Equal(t, last, 0)

	first, last, found = ms.nextWildcardMatchLocked("foo.nope.*", 1)
	require_False(t, found)
	require_Equal(t, first, 0)
	require_Equal(t, last, 0)

	first, last, found = ms.nextWildcardMatchLocked("foo.>", 1)
	require_True(t, found)
	require_Equal(t, first, 1)
	require_Equal(t, last, 32)
}

func TestMemStoreNextLiteralMatch(t *testing.T) {
	cfg := &StreamConfig{
		Name:     "zzz",
		Subjects: []string{"foo.>"},
		Storage:  MemoryStorage,
	}
	ms, err := newMemStore(cfg)
	require_NoError(t, err)
	defer ms.Stop()

	msg := []byte("msg")
	storeN := func(subj string, n int) {
		t.Helper()
		for range n {
			_, _, err := ms.StoreMsg(subj, nil, msg, 0)
			require_NoError(t, err)
		}
	}
	storeN("foo.bar.a", 1)             // seq 1
	storeN("foo.baz.bar", 10)          // seqs 2-11
	storeN("foo.bar.b", 1)             // seq 12
	storeN("foo.baz.bar", 10)          // seqs 13-22
	storeN("foo.baz.bar.no.match", 10) // seqs 23-32

	ms.mu.Lock()
	defer ms.mu.Unlock()

	first, last, found := ms.nextLiteralMatchLocked("foo.bar.a", 0)
	require_True(t, found)
	require_Equal(t, first, 1)
	require_Equal(t, last, 1)

	_, _, found = ms.nextLiteralMatchLocked("foo.bar.a", 2)
	require_False(t, found)

	first, last, found = ms.nextLiteralMatchLocked("foo.baz.bar", 1)
	require_True(t, found)
	require_Equal(t, first, 2)
	require_Equal(t, last, 22)

	first, last, found = ms.nextLiteralMatchLocked("foo.baz.bar", 11)
	require_True(t, found)
	require_Equal(t, first, 11)
	require_Equal(t, last, 22)

	first, last, found = ms.nextLiteralMatchLocked("foo.baz.bar", 22)
	require_True(t, found)
	require_Equal(t, first, 22)
	require_Equal(t, last, 22)

	first, last, found = ms.nextLiteralMatchLocked("foo.baz.bar", 23)
	require_False(t, found)
	require_Equal(t, first, 0)
	require_Equal(t, last, 0)

	first, last, found = ms.nextLiteralMatchLocked("foo.nope", 1)
	require_False(t, found)
	require_Equal(t, first, 0)
	require_Equal(t, last, 0)

}

func TestMemStoreMultiLastSeqsDoesNotUseStaleLastValue(t *testing.T) {
	cfg := &StreamConfig{
		Name:     "zzz",
		Subjects: []string{"foo", "bar", "baz"},
		Storage:  MemoryStorage,
	}
	ms, err := newMemStore(cfg)
	require_NoError(t, err)
	defer ms.Stop()

	subjects := []string{"foo", "bar", "foo", " baz", "foo", "foo", "bar", "foo", "bar", "baz"}
	for _, s := range subjects {
		_, _, err := ms.StoreMsg(s, nil, nil, 0)
		require_NoError(t, err)
	}

	// Initially we expect last seq for `foo` to be 8
	seqs, err := ms.MultiLastSeqs([]string{"foo", "bar"}, 0, 0)
	require_NoError(t, err)
	require_Equal(t, len(seqs), 2)
	require_Equal(t, seqs[0], uint64(8))

	// Remove latest foo message
	removed, err := ms.RemoveMsg(8)
	require_NoError(t, err)
	require_True(t, removed)

	// If bug is present: MultiLastSeqs returns last sequence
	// 8 for subject `foo`.
	// After removal, expect last sequence 6 for subject `foo`.
	seqs, err = ms.MultiLastSeqs([]string{"foo", "bar"}, 0, 0)
	require_NoError(t, err)
	require_Equal(t, len(seqs), 2)
	require_Equal(t, seqs[0], uint64(6))
}

///////////////////////////////////////////////////////////////////////////
// Benchmarks
///////////////////////////////////////////////////////////////////////////

func Benchmark_MemStoreStoreMsg(b *testing.B) {
	for _, size := range []int{64, 256, 1024, 4096, 16384, 65536, 131072, 262144, 524288, 983040} {
		b.Run(fmt.Sprintf("payload=%d", size), func(b *testing.B) {
			benchMemStoreStore(b, 0, size)
		})
	}
}

func Benchmark_MemStoreStoreMsgHdr(b *testing.B) {
	for _, size := range []int{64, 4096, 65536, 262144, 983040} {
		b.Run(fmt.Sprintf("payload=%d", size), func(b *testing.B) {
			benchMemStoreStore(b, 512, size)
		})
	}
}

func benchMemStoreStore(b *testing.B, hdrSize, msgSize int) {
	ms, err := newMemStore(&StreamConfig{Name: "bench", Storage: MemoryStorage, MaxMsgs: 1024})
	require_NoError(b, err)
	defer ms.Stop()

	hdr := make([]byte, hdrSize)
	msg := make([]byte, msgSize)
	b.SetBytes(int64(hdrSize + msgSize))
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, _, err := ms.StoreMsg("bench", hdr, msg, 0); err != nil {
			b.Fatal(err)
		}
	}
}

func Benchmark_MemStoreNumPendingWithLargeInteriorDeletesScan(b *testing.B) {
	cfg := &StreamConfig{
		Name:     "zzz",
		Subjects: []string{"foo.*.*"},
		Storage:  MemoryStorage,
	}
	ms, err := newMemStore(cfg)
	require_NoError(b, err)
	defer ms.Stop()

	msg := []byte("abc")
	ms.StoreMsg("foo.bar.baz", nil, msg, 0)
	for i := 1; i <= 1_000_000; i++ {
		ms.SkipMsg(0)
	}
	ms.StoreMsg("foo.bar.baz", nil, msg, 0)

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		total, _, err := ms.NumPending(600_000, "foo.*.baz", false)
		require_NoError(b, err)
		if total != 1 {
			b.Fatalf("Expected total of 2 got %d", total)
		}
	}
}

func Benchmark_MemStoreNumPendingWithLargeInteriorDeletesExclude(b *testing.B) {
	cfg := &StreamConfig{
		Name:     "zzz",
		Subjects: []string{"foo.*.*"},
		Storage:  MemoryStorage,
	}
	ms, err := newMemStore(cfg)
	require_NoError(b, err)
	defer ms.Stop()

	msg := []byte("abc")
	ms.StoreMsg("foo.bar.baz", nil, msg, 0)
	for i := 1; i <= 1_000_000; i++ {
		ms.SkipMsg(0)
	}
	ms.StoreMsg("foo.bar.baz", nil, msg, 0)

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		total, _, err := ms.NumPending(400_000, "foo.*.baz", false)
		require_NoError(b, err)
		if total != 1 {
			b.Fatalf("Expected total of 2 got %d", total)
		}
	}
}

func Benchmark_MemStoreSubjectStateConsistencyOptimizationPerf(b *testing.B) {
	cfg := &StreamConfig{Name: "TEST", Subjects: []string{"foo.*"}, Storage: MemoryStorage, MaxMsgsPer: 1}
	ms, err := newMemStore(cfg)
	require_NoError(b, err)
	defer ms.Stop()

	// Do R rounds of storing N messages.
	// MaxMsgsPer=1, so every unique subject that's placed only exists in the stream once.
	// If R=2, N=3 that means we'd place foo.0, foo.1, foo.2 in the first round, and the second
	// round we'd place foo.2, foo.1, foo.0, etc. This is intentional so that without any
	// optimizations we'd need to scan either 1 in the optimal case or N in the worst case.
	// Which is way more expensive than always knowing what the sequences are and it being O(1).
	r := max(2, b.N)
	n := 40_000
	b.ResetTimer()
	for i := 0; i < r; i++ {
		for j := 0; j < n; j++ {
			d := j
			if i%2 == 0 {
				d = n - j - 1
			}
			subject := fmt.Sprintf("foo.%d", d)
			_, _, err = ms.StoreMsg(subject, nil, nil, 0)
			require_NoError(b, err)
		}
	}
}

// This benchmark populates a memstore and then measures
// the time it takes to load the entire store using
// LoadNextMsg repeatedly until the store returns ErrStoreEOF.
func Benchmark_MemStoreLoadNextMsgFiltered(b *testing.B) {
	cases := []struct {
		name             string
		msgs             int
		matchingMsgEvery int
		filter           string
		wc               bool
		matchingSubject  func(i int) string
		expectLinear     bool
	}{
		{
			name:             "wildcard_linear_scan",
			msgs:             10_000_000,
			matchingMsgEvery: 10_000,
			filter:           "foo.baz.*",
			wc:               true,
			matchingSubject: func(i int) string {
				return fmt.Sprintf("foo.baz.%d", i)
			},
			expectLinear: true,
		},
		{
			name:             "wildcard_bounded_scan",
			msgs:             10_000_000,
			matchingMsgEvery: 100_000,
			filter:           "foo.baz.*",
			wc:               true,
			matchingSubject: func(i int) string {
				return fmt.Sprintf("foo.baz.%d", i)
			},
			expectLinear: false,
		},
		{
			name:             "literal_bounded_scan",
			msgs:             10_000_000,
			matchingMsgEvery: 100_000,
			filter:           "foo.baz",
			wc:               false,
			matchingSubject: func(i int) string {
				return "foo.baz"
			},
			expectLinear: false,
		},
	}

	for _, tc := range cases {
		b.Run(tc.name, func(b *testing.B) {
			cfg := &StreamConfig{Name: "TEST", Subjects: []string{"foo.>"}, Storage: MemoryStorage}
			ms, err := newMemStore(cfg)
			require_NoError(b, err)
			defer ms.Stop()

			msg := []byte("ok")
			for i := range tc.msgs {
				subject := "foo.bar"
				if i%tc.matchingMsgEvery == 0 {
					subject = tc.matchingSubject(i)
				}
				_, _, err = ms.StoreMsg(subject, nil, msg, 0)
				require_NoError(b, err)
			}

			ms.mu.Lock()
			require_Equal(b, ms.shouldLinearScan(tc.filter, tc.wc, 1), tc.expectLinear)
			ms.mu.Unlock()

			var smv StoreMsg
			expectedMatches := uint64(tc.msgs / tc.matchingMsgEvery)

			b.ResetTimer()
			for b.Loop() {
				var start uint64
				var count uint64
				for {
					_, start, err = ms.LoadNextMsg(tc.filter, tc.wc, start, &smv)
					start++
					if err == ErrStoreEOF {
						require_Equal(b, count, expectedMatches)
						break
					}
					require_NoError(b, err)
					count++
				}
			}
		})
	}
}

func TestMemStoreMultiLastSeqsDoesNotReorderConfigSubjects(t *testing.T) {
	subjects := []string{"orders.*", "billing.*"}
	ms, err := newMemStore(&StreamConfig{Name: "zzz", Storage: MemoryStorage, Subjects: subjects})
	require_NoError(t, err)

	_, _, err = ms.StoreMsg("orders.1", nil, []byte("x"), 0)
	require_NoError(t, err)
	_, _, err = ms.StoreMsg("billing.1", nil, []byte("x"), 0)
	require_NoError(t, err)

	// Filter count == subject count drives the filterIsAll path.
	_, err = ms.MultiLastSeqs([]string{"orders.*", "billing.*"}, 0, 0)
	require_NoError(t, err)

	// The advertised config subjects must keep their original order.
	require_Equal(t, ms.cfg.Subjects[0], "orders.*")
	require_Equal(t, ms.cfg.Subjects[1], "billing.*")
}

func TestMemStoreSourcesAddedSourceKeepsExistingState(t *testing.T) {
	origin1 := &StreamSource{Name: "ORIGIN1"}
	origin2 := &StreamSource{Name: "ORIGIN2"}
	iName1, iName2 := origin1.composeIName(), origin2.composeIName()

	cfg := StreamConfig{
		Name:     "SOURCE",
		Subjects: []string{"foo"},
		Storage:  MemoryStorage,
		Sources:  []*StreamSource{origin1},
	}
	ms, err := newMemStore(&cfg)
	require_NoError(t, err)
	defer ms.Stop()

	for i := range 10 {
		hdr := genHeader(nil, JSStreamSource, fmt.Sprintf("ORIGIN1 %d > > foo", i+1))
		_, _, err = ms.StoreMsg("foo", hdr, nil, 0)
		require_NoError(t, err)
	}
	state := ms.SourcesState()
	require_Len(t, len(state), 1)
	require_Equal(t, state[iName1].Seq, 10)

	// Remove the newest message, a scan would now only be able to derive
	// ORIGIN1 up to sequence 9.
	removed, err := ms.RemoveMsg(10)
	require_NoError(t, err)
	require_True(t, removed)

	// Adding a source must only recover the sequence of the source that was just
	// added. Sources that are already tracked must keep their state, even if a
	// scan derives a lower sequence for them, since that would result in
	// the source consumer re-delivering messages it had already stored.
	ucfg := cfg
	ucfg.Sources = []*StreamSource{origin1, origin2}
	require_NoError(t, ms.UpdateConfig(&ucfg))

	state = ms.SourcesState()
	require_Len(t, len(state), 2)
	// ORIGIN1 must not have moved backward.
	require_Equal(t, state[iName1].Seq, 10)
	// ORIGIN2 is newly seeded and has nothing to recover.
	require_Equal(t, state[iName2].Seq, 0)
}
