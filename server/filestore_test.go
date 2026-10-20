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
	"archive/tar"
	"bytes"
	"crypto/hmac"
	crand "crypto/rand"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"math"
	"math/bits"
	"math/rand/v2"
	"os"
	"path/filepath"
	"reflect"
	"runtime"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
	"unicode/utf8"
	"unsafe"

	"github.com/klauspost/compress/s2"
	"github.com/minio/highwayhash"
	"github.com/nats-io/nats-server/v2/server/ats"
	"github.com/nats-io/nats-server/v2/server/avl"
	"github.com/nats-io/nats-server/v2/server/gsl"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nuid"
)

func testFileStoreAllPermutations(t *testing.T, fn func(t *testing.T, fcfg FileStoreConfig)) {
	for _, fcfg := range []FileStoreConfig{
		{Cipher: NoCipher, Compression: NoCompression},
		{Cipher: NoCipher, Compression: S2Compression},
		{Cipher: AES, Compression: NoCompression},
		{Cipher: AES, Compression: S2Compression},
		{Cipher: ChaCha, Compression: NoCompression},
		{Cipher: ChaCha, Compression: S2Compression},
	} {
		subtestName := fmt.Sprintf("%s-%s", fcfg.Cipher, fcfg.Compression)
		t.Run(subtestName, func(t *testing.T) {
			fcfg.StoreDir = t.TempDir()
			fn(t, fcfg)
			time.Sleep(100 * time.Millisecond)
		})
	}
}

func prf(fcfg *FileStoreConfig) func(context []byte) ([]byte, error) {
	if fcfg.Cipher == NoCipher {
		return nil
	}
	return func(context []byte) ([]byte, error) {
		h := hmac.New(sha256.New, []byte("dlc22"))
		if _, err := h.Write(context); err != nil {
			return nil, err
		}
		return h.Sum(nil), nil
	}
}

func TestFileStoreBasics(t *testing.T) {
	testFileStoreAllPermutations(t, func(t *testing.T, fcfg FileStoreConfig) {
		fs, err := newFileStoreWithCreated(fcfg, StreamConfig{Name: "zzz", Storage: FileStorage}, time.Now(), prf(&fcfg), nil)
		require_NoError(t, err)
		defer fs.Stop()

		subj, msg := "foo", []byte("Hello World")
		for i := 1; i <= 5; i++ {
			now := time.Now().UnixNano()
			if seq, ts, err := fs.StoreMsg(subj, nil, msg, 0); err != nil {
				t.Fatalf("Error storing msg: %v", err)
			} else if seq != uint64(i) {
				t.Fatalf("Expected sequence to be %d, got %d", i, seq)
			} else if ts < now || ts > now+int64(time.Millisecond) {
				t.Fatalf("Expected timestamp to be current, got %v", ts-now)
			}
		}

		state := fs.State()
		if state.Msgs != 5 {
			t.Fatalf("Expected 5 msgs, got %d", state.Msgs)
		}
		expectedSize := 5 * fileStoreMsgSize(subj, nil, msg)
		if state.Bytes != expectedSize {
			t.Fatalf("Expected %d bytes, got %d", expectedSize, state.Bytes)
		}

		var smv StoreMsg
		sm, err := fs.LoadMsg(2, &smv)
		if err != nil {
			t.Fatalf("Unexpected error looking up msg: %v", err)
		}
		if sm.subj != subj {
			t.Fatalf("Subjects don't match, original %q vs %q", subj, sm.subj)
		}
		if !bytes.Equal(sm.msg, msg) {
			t.Fatalf("Msgs don't match, original %q vs %q", msg, sm.msg)
		}
		_, err = fs.LoadMsg(3, nil)
		if err != nil {
			t.Fatalf("Unexpected error looking up msg: %v", err)
		}

		remove := func(seq, expectedMsgs uint64) {
			t.Helper()
			removed, err := fs.RemoveMsg(seq)
			if err != nil {
				t.Fatalf("Got an error on remove of %d: %v", seq, err)
			}
			if !removed {
				t.Fatalf("Expected remove to return true for %d", seq)
			}
			if state := fs.State(); state.Msgs != expectedMsgs {
				t.Fatalf("Expected %d msgs, got %d", expectedMsgs, state.Msgs)
			}
		}

		// Remove first
		remove(1, 4)
		// Remove last
		remove(5, 3)
		// Remove a middle
		remove(3, 2)
	})
}

func TestFileStoreMsgHeaders(t *testing.T) {
	testFileStoreAllPermutations(t, func(t *testing.T, fcfg FileStoreConfig) {
		fs, err := newFileStoreWithCreated(fcfg, StreamConfig{Name: "zzz", Storage: FileStorage}, time.Now(), prf(&fcfg), nil)

		require_NoError(t, err)
		defer fs.Stop()
		subj, hdr, msg := "foo", []byte("name:derek"), []byte("Hello World")
		elen := 22 + len(subj) + 4 + len(hdr) + len(msg) + 8
		if sz := int(fileStoreMsgSize(subj, hdr, msg)); sz != elen {
			t.Fatalf("Wrong size for stored msg with header")
		}
		fs.StoreMsg(subj, hdr, msg, 0)
		var smv StoreMsg
		sm, err := fs.LoadMsg(1, &smv)
		if err != nil {
			t.Fatalf("Unexpected error looking up msg: %v", err)
		}
		if !bytes.Equal(msg, sm.msg) {
			t.Fatalf("Expected same msg, got %q vs %q", sm.msg, msg)
		}
		if !bytes.Equal(hdr, sm.hdr) {
			t.Fatalf("Expected same hdr, got %q vs %q", sm.hdr, hdr)
		}
		if removed, _ := fs.EraseMsg(1); !removed {
			t.Fatalf("Expected erase msg to return success")
		}
	})
}

func TestFileStoreBasicWriteMsgsAndRestore(t *testing.T) {
	testFileStoreAllPermutations(t, func(t *testing.T, fcfg FileStoreConfig) {
		if _, err := newFileStore(fcfg, StreamConfig{Storage: MemoryStorage}); err == nil {
			t.Fatalf("Expected an error with wrong type")
		}
		if _, err := newFileStore(fcfg, StreamConfig{Storage: FileStorage}); err == nil {
			t.Fatalf("Expected an error with no name")
		}

		created := time.Now()
		fs, err := newFileStoreWithCreated(fcfg, StreamConfig{Name: "zzz", Storage: FileStorage}, created, prf(&fcfg), nil)
		require_NoError(t, err)
		defer fs.Stop()

		subj := "foo"

		// Write 100 msgs
		toStore := uint64(100)
		for i := uint64(1); i <= toStore; i++ {
			msg := []byte(fmt.Sprintf("[%08d] Hello World!", i))
			if seq, _, err := fs.StoreMsg(subj, nil, msg, 0); err != nil {
				t.Fatalf("Error storing msg: %v", err)
			} else if seq != uint64(i) {
				t.Fatalf("Expected sequence to be %d, got %d", i, seq)
			}
		}
		state := fs.State()
		if state.Msgs != toStore {
			t.Fatalf("Expected %d msgs, got %d", toStore, state.Msgs)
		}
		msg22 := []byte(fmt.Sprintf("[%08d] Hello World!", 22))
		expectedSize := toStore * fileStoreMsgSize(subj, nil, msg22)

		if state.Bytes != expectedSize {
			t.Fatalf("Expected %d bytes, got %d", expectedSize, state.Bytes)
		}
		// Stop will flush to disk.
		fs.Stop()

		// Make sure Store call after does not work.
		if _, _, err := fs.StoreMsg(subj, nil, []byte("no work"), 0); err == nil {
			t.Fatalf("Expected an error for StoreMsg call after Stop, got none")
		}

		// Restart
		fs, err = newFileStoreWithCreated(fcfg, StreamConfig{Name: "zzz", Storage: FileStorage}, created, prf(&fcfg), nil)
		require_NoError(t, err)
		defer fs.Stop()

		state = fs.State()
		if state.Msgs != toStore {
			t.Fatalf("Expected %d msgs, got %d", toStore, state.Msgs)
		}
		if state.Bytes != expectedSize {
			t.Fatalf("Expected %d bytes, got %d", expectedSize, state.Bytes)
		}

		// Now write 100 more msgs
		for i := uint64(101); i <= toStore*2; i++ {
			msg := []byte(fmt.Sprintf("[%08d] Hello World!", i))
			if seq, _, err := fs.StoreMsg(subj, nil, msg, 0); err != nil {
				t.Fatalf("Error storing msg: %v", err)
			} else if seq != uint64(i) {
				t.Fatalf("Expected sequence to be %d, got %d", i, seq)
			}
		}
		state = fs.State()
		if state.Msgs != toStore*2 {
			t.Fatalf("Expected %d msgs, got %d", toStore*2, state.Msgs)
		}

		// Now cycle again and make sure that last batch was stored.
		// Stop will flush to disk.
		fs.Stop()

		// Restart
		fs, err = newFileStoreWithCreated(fcfg, StreamConfig{Name: "zzz", Storage: FileStorage}, created, prf(&fcfg), nil)
		require_NoError(t, err)
		defer fs.Stop()

		state = fs.State()
		if state.Msgs != toStore*2 {
			t.Fatalf("Expected %d msgs, got %d", toStore*2, state.Msgs)
		}
		if state.Bytes != expectedSize*2 {
			t.Fatalf("Expected %d bytes, got %d", expectedSize*2, state.Bytes)
		}

		fs.Purge()
		fs.Stop()

		// Restart
		fs, err = newFileStoreWithCreated(fcfg, StreamConfig{Name: "zzz", Storage: FileStorage}, created, prf(&fcfg), nil)
		require_NoError(t, err)
		defer fs.Stop()

		state = fs.State()
		if state.Msgs != 0 {
			t.Fatalf("Expected %d msgs, got %d", 0, state.Msgs)
		}
		if state.Bytes != 0 {
			t.Fatalf("Expected %d bytes, got %d", 0, state.Bytes)
		}

		seq, _, err := fs.StoreMsg(subj, nil, []byte("Hello"), 0)
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		fs.RemoveMsg(seq)

		fs.Stop()

		// Restart
		fs, err = newFileStoreWithCreated(fcfg, StreamConfig{Name: "zzz", Storage: FileStorage}, created, prf(&fcfg), nil)
		require_NoError(t, err)
		defer fs.Stop()

		state = fs.State()
		require_Equal(t, state.FirstSeq, seq+1)
	})
}

func TestFileStoreSelectNextFirst(t *testing.T) {
	testFileStoreAllPermutations(t, func(t *testing.T, fcfg FileStoreConfig) {
		fcfg.BlockSize = 256
		fs, err := newFileStoreWithCreated(fcfg, StreamConfig{Name: "zzz", Storage: FileStorage}, time.Now(), prf(&fcfg), nil)
		require_NoError(t, err)
		defer fs.Stop()

		numMsgs := 10
		subj, msg := "zzz", []byte("Hello World")
		for i := 0; i < numMsgs; i++ {
			fs.StoreMsg(subj, nil, msg, 0)
		}
		if state := fs.State(); state.Msgs != uint64(numMsgs) {
			t.Fatalf("Expected %d msgs, got %d", numMsgs, state.Msgs)
		}

		// Note the 256 block size is tied to the msg size below to give us 5 messages per block.
		if fmb := fs.selectMsgBlock(1); fmb.msgs != 5 {
			t.Fatalf("Expected 5 messages per block, but got %d", fmb.msgs)
		}

		// Delete 2-7, this will cross message blocks.
		for i := 2; i <= 7; i++ {
			fs.RemoveMsg(uint64(i))
		}

		if state := fs.State(); state.Msgs != 4 || state.FirstSeq != 1 {
			t.Fatalf("Expected 4 msgs, first seq of 11, got msgs of %d and first seq of %d", state.Msgs, state.FirstSeq)
		}
		// Now close the gap which will force the system to jump underlying message blocks to find the right sequence.
		fs.RemoveMsg(1)
		if state := fs.State(); state.Msgs != 3 || state.FirstSeq != 8 {
			t.Fatalf("Expected 3 msgs, first seq of 8, got msgs of %d and first seq of %d", state.Msgs, state.FirstSeq)
		}
	})
}

func TestFileStoreSkipMsg(t *testing.T) {
	testFileStoreAllPermutations(t, func(t *testing.T, fcfg FileStoreConfig) {
		fcfg.BlockSize = 256
		created := time.Now()
		fs, err := newFileStoreWithCreated(fcfg, StreamConfig{Name: "zzz", Storage: FileStorage}, created, prf(&fcfg), nil)
		require_NoError(t, err)
		defer fs.Stop()

		numSkips := 10
		for i := 0; i < numSkips; i++ {
			fs.SkipMsg(0)
		}
		state := fs.State()
		if state.Msgs != 0 {
			t.Fatalf("Expected %d msgs, got %d", 0, state.Msgs)
		}
		if state.FirstSeq != uint64(numSkips+1) || state.LastSeq != uint64(numSkips) {
			t.Fatalf("Expected first to be %d and last to be %d. got first %d and last %d", numSkips+1, numSkips, state.FirstSeq, state.LastSeq)
		}

		fs.StoreMsg("zzz", nil, []byte("Hello World!"), 0)
		fs.SkipMsg(0)
		fs.SkipMsg(0)
		fs.StoreMsg("zzz", nil, []byte("Hello World!"), 0)
		fs.SkipMsg(0)

		state = fs.State()
		if state.Msgs != 2 {
			t.Fatalf("Expected %d msgs, got %d", 2, state.Msgs)
		}
		if state.FirstSeq != uint64(numSkips+1) || state.LastSeq != uint64(numSkips+5) {
			t.Fatalf("Expected first to be %d and last to be %d. got first %d and last %d", numSkips+1, numSkips+5, state.FirstSeq, state.LastSeq)
		}

		// Make sure we recover same state.
		fs.Stop()

		fs, err = newFileStoreWithCreated(fcfg, StreamConfig{Name: "zzz", Storage: FileStorage}, created, prf(&fcfg), nil)
		require_NoError(t, err)
		defer fs.Stop()

		state = fs.State()
		if state.Msgs != 2 {
			t.Fatalf("Expected %d msgs, got %d", 2, state.Msgs)
		}
		if state.FirstSeq != uint64(numSkips+1) || state.LastSeq != uint64(numSkips+5) {
			t.Fatalf("Expected first to be %d and last to be %d. got first %d and last %d", numSkips+1, numSkips+5, state.FirstSeq, state.LastSeq)
		}

		var smv StoreMsg
		sm, err := fs.LoadMsg(11, &smv)
		if err != nil {
			t.Fatalf("Unexpected error looking up seq 11: %v", err)
		}
		if sm.subj != "zzz" || string(sm.msg) != "Hello World!" {
			t.Fatalf("Message did not match")
		}

		fs.SkipMsg(0)
		nseq, _, err := fs.StoreMsg("AAA", nil, []byte("Skip?"), 0)
		if err != nil {
			t.Fatalf("Unexpected error looking up seq 11: %v", err)
		}
		if nseq != 17 {
			t.Fatalf("Expected seq of %d but got %d", 17, nseq)
		}

		// Make sure we recover same state.
		fs.Stop()

		fs, err = newFileStoreWithCreated(fcfg, StreamConfig{Name: "zzz", Storage: FileStorage}, created, prf(&fcfg), nil)
		require_NoError(t, err)
		defer fs.Stop()

		sm, err = fs.LoadMsg(nseq, &smv)
		if err != nil {
			t.Fatalf("Unexpected error looking up seq %d: %v", nseq, err)
		}
		if sm.subj != "AAA" || string(sm.msg) != "Skip?" {
			t.Fatalf("Message did not match")
		}
	})
}

func TestFileStoreWriteExpireWrite(t *testing.T) {
	testFileStoreAllPermutations(t, func(t *testing.T, fcfg FileStoreConfig) {
		cexp := 10 * time.Millisecond
		fcfg.CacheExpire = cexp
		created := time.Now()
		fs, err := newFileStoreWithCreated(fcfg, StreamConfig{Name: "zzz", Storage: FileStorage}, created, prf(&fcfg), nil)
		require_NoError(t, err)
		defer fs.Stop()

		toSend := 10
		for i := 0; i < toSend; i++ {
			fs.StoreMsg("zzz", nil, []byte("Hello World!"), 0)
		}

		// Wait for write cache portion to go to zero.
		checkFor(t, time.Second, 20*time.Millisecond, func() error {
			if csz := fs.cacheSize(); csz != 0 {
				return fmt.Errorf("cache size not 0, got %s", friendlyBytes(int64(csz)))
			}
			return nil
		})

		for i := 0; i < toSend; i++ {
			fs.StoreMsg("zzz", nil, []byte("Hello World! - 22"), 0)
		}

		if state := fs.State(); state.Msgs != uint64(toSend*2) {
			t.Fatalf("Expected %d msgs, got %d", toSend*2, state.Msgs)
		}

		// Make sure we recover same state.
		fs.Stop()

		fcfg.CacheExpire = 0
		fs, err = newFileStoreWithCreated(fcfg, StreamConfig{Name: "zzz", Storage: FileStorage}, created, prf(&fcfg), nil)
		require_NoError(t, err)
		defer fs.Stop()

		if state := fs.State(); state.Msgs != uint64(toSend*2) {
			t.Fatalf("Expected %d msgs, got %d", toSend*2, state.Msgs)
		}

		// Now load them in and check.
		var smv StoreMsg
		for i := 1; i <= toSend*2; i++ {
			sm, err := fs.LoadMsg(uint64(i), &smv)
			if err != nil {
				t.Fatalf("Unexpected error looking up seq %d: %v", i, err)
			}
			str := "Hello World!"
			if i > toSend {
				str = "Hello World! - 22"
			}
			if sm.subj != "zzz" || string(sm.msg) != str {
				t.Fatalf("Message did not match")
			}
		}
	})
}

func TestFileStoreMsgLimit(t *testing.T) {
	testFileStoreAllPermutations(t, func(t *testing.T, fcfg FileStoreConfig) {
		fs, err := newFileStoreWithCreated(fcfg, StreamConfig{Name: "zzz", Storage: FileStorage, MaxMsgs: 10}, time.Now(), prf(&fcfg), nil)
		require_NoError(t, err)
		defer fs.Stop()

		subj, msg := "foo", []byte("Hello World")
		for i := 0; i < 10; i++ {
			fs.StoreMsg(subj, nil, msg, 0)
		}
		state := fs.State()
		if state.Msgs != 10 {
			t.Fatalf("Expected %d msgs, got %d", 10, state.Msgs)
		}
		if _, _, err := fs.StoreMsg(subj, nil, msg, 0); err != nil {
			t.Fatalf("Error storing msg: %v", err)
		}
		state = fs.State()
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
		if _, err := fs.LoadMsg(1, nil); err == nil {
			t.Fatalf("Expected error looking up seq 1 but got none")
		}
	})
}

func TestFileStoreMsgLimitBug(t *testing.T) {
	testFileStoreAllPermutations(t, func(t *testing.T, fcfg FileStoreConfig) {
		created := time.Now()
		fs, err := newFileStoreWithCreated(fcfg, StreamConfig{Name: "zzz", Storage: FileStorage, MaxMsgs: 1}, created, prf(&fcfg), nil)
		require_NoError(t, err)
		defer fs.Stop()

		subj, msg := "foo", []byte("Hello World")
		fs.StoreMsg(subj, nil, msg, 0)
		fs.StoreMsg(subj, nil, msg, 0)
		fs.Stop()

		fs, err = newFileStoreWithCreated(fcfg, StreamConfig{Name: "zzz", Storage: FileStorage, MaxMsgs: 1}, created, prf(&fcfg), nil)
		require_NoError(t, err)
		defer fs.Stop()
		fs.StoreMsg(subj, nil, msg, 0)
	})
}

func TestFileStoreBytesLimit(t *testing.T) {
	subj, msg := "foo", make([]byte, 512)
	storedMsgSize := fileStoreMsgSize(subj, nil, msg)

	toStore := uint64(1024)
	maxBytes := storedMsgSize * toStore

	testFileStoreAllPermutations(t, func(t *testing.T, fcfg FileStoreConfig) {
		fs, err := newFileStoreWithCreated(fcfg, StreamConfig{Name: "zzz", Storage: FileStorage, MaxBytes: int64(maxBytes)}, time.Now(), prf(&fcfg), nil)
		require_NoError(t, err)
		defer fs.Stop()

		for i := uint64(0); i < toStore; i++ {
			fs.StoreMsg(subj, nil, msg, 0)
		}
		state := fs.State()
		if state.Msgs != toStore {
			t.Fatalf("Expected %d msgs, got %d", toStore, state.Msgs)
		}
		if state.Bytes != storedMsgSize*toStore {
			t.Fatalf("Expected bytes to be %d, got %d", storedMsgSize*toStore, state.Bytes)
		}

		// Now send 10 more and check that bytes limit enforced.
		for i := 0; i < 10; i++ {
			if _, _, err := fs.StoreMsg(subj, nil, msg, 0); err != nil {
				t.Fatalf("Error storing msg: %v", err)
			}
		}
		state = fs.State()
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
	})
}

// https://github.com/nats-io/nats-server/issues/4771
func TestFileStoreBytesLimitWithDiscardNew(t *testing.T) {
	subj, msg := "tiny", make([]byte, 7)
	storedMsgSize := fileStoreMsgSize(subj, nil, msg)

	toStore := uint64(2)
	maxBytes := 100

	testFileStoreAllPermutations(t, func(t *testing.T, fcfg FileStoreConfig) {
		cfg := StreamConfig{Name: "zzz", Storage: FileStorage, MaxBytes: int64(maxBytes), Discard: DiscardNew}
		fs, err := newFileStoreWithCreated(fcfg, cfg, time.Now(), prf(&fcfg), nil)
		require_NoError(t, err)
		defer fs.Stop()

		for i := 0; i < 10; i++ {
			_, _, err := fs.StoreMsg(subj, nil, msg, 0)
			if i < int(toStore) {
				if err != nil {
					t.Fatalf("Error storing msg: %v", err)
				}
			} else if !errors.Is(err, ErrMaxBytes) {
				t.Fatalf("Storing msg should result in: %v", ErrMaxBytes)
			}
		}
		state := fs.State()
		if state.Msgs != toStore {
			t.Fatalf("Expected %d msgs, got %d", toStore, state.Msgs)
		}
		if state.Bytes != storedMsgSize*toStore {
			t.Fatalf("Expected bytes to be %d, got %d", storedMsgSize*toStore, state.Bytes)
		}
	})
}

func TestFileStoreAgeLimit(t *testing.T) {
	maxAge := 1 * time.Second

	testFileStoreAllPermutations(t, func(t *testing.T, fcfg FileStoreConfig) {
		if fcfg.Compression != NoCompression || fcfg.Cipher != NoCipher {
			// TODO(nat): This test flakes at the moment with compression or encryption
			// because it takes too long to compress/encrypt the blocks in CI, by which
			// time the messages have expired. Need to think about a balanced age so that
			// the test doesn't take too long in these cases.
			t.SkipNow()
		}

		fcfg.BlockSize = 256
		fs, err := newFileStoreWithCreated(fcfg, StreamConfig{Name: "zzz", Storage: FileStorage, MaxAge: maxAge}, time.Now(), prf(&fcfg), nil)
		require_NoError(t, err)
		defer fs.Stop()

		// Store some messages. Does not really matter how many.
		subj, msg := "foo", []byte("Hello World")
		toStore := 500
		for i := 0; i < toStore; i++ {
			if _, _, err := fs.StoreMsg(subj, nil, msg, 0); err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}
		}
		state := fs.State()
		if state.Msgs != uint64(toStore) {
			t.Fatalf("Expected %d msgs, got %d", toStore, state.Msgs)
		}
		checkExpired := func(t *testing.T) {
			t.Helper()
			checkFor(t, 5*time.Second, maxAge, func() error {
				state = fs.State()
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
			fs.StoreMsg(subj, nil, msg, 0)
		}
		state = fs.State()
		if state.Msgs != uint64(toStore) {
			t.Fatalf("Expected %d msgs, got %d", toStore, state.Msgs)
		}
		fs.RemoveMsg(502)
		fs.RemoveMsg(602)
		fs.RemoveMsg(702)
		fs.RemoveMsg(802)
		// We will measure the time to make sure expires works with interior deletes.
		start := time.Now()
		checkExpired(t)
		if elapsed := time.Since(start); elapsed > 5*time.Second {
			t.Fatalf("Took too long to expire: %v", elapsed)
		}
	})
}

func TestFileStoreTimeStamps(t *testing.T) {
	testFileStoreAllPermutations(t, func(t *testing.T, fcfg FileStoreConfig) {
		fs, err := newFileStoreWithCreated(fcfg, StreamConfig{Name: "zzz", Storage: FileStorage}, time.Now(), prf(&fcfg), nil)
		require_NoError(t, err)
		defer fs.Stop()

		last := time.Now().UnixNano()
		subj, msg := "foo", []byte("Hello World")
		for i := 0; i < 10; i++ {
			time.Sleep(5 * time.Millisecond)
			fs.StoreMsg(subj, nil, msg, 0)
		}
		var smv StoreMsg
		for seq := uint64(1); seq <= 10; seq++ {
			sm, err := fs.LoadMsg(seq, &smv)
			if err != nil {
				t.Fatalf("Unexpected error looking up msg [%d]: %v", seq, err)
			}
			// These should be different
			if sm.ts <= last {
				t.Fatalf("Expected different timestamps, got last %v vs %v", last, sm.ts)
			}
			last = sm.ts
		}
	})
}

func TestFileStorePurge(t *testing.T) {
	testFileStoreAllPermutations(t, func(t *testing.T, fcfg FileStoreConfig) {
		blkSize := uint64(64 * 1024)
		fcfg.BlockSize = blkSize
		created := time.Now()

		fs, err := newFileStoreWithCreated(fcfg, StreamConfig{Name: "zzz", Storage: FileStorage}, created, prf(&fcfg), nil)
		require_NoError(t, err)
		defer fs.Stop()

		subj, msg := "foo", make([]byte, 8*1024)
		storedMsgSize := fileStoreMsgSize(subj, nil, msg)

		toStore := uint64(1024)
		for i := uint64(0); i < toStore; i++ {
			fs.StoreMsg(subj, nil, msg, 0)
		}
		state := fs.State()
		if state.Msgs != toStore {
			t.Fatalf("Expected %d msgs, got %d", toStore, state.Msgs)
		}
		if state.Bytes != storedMsgSize*toStore {
			t.Fatalf("Expected bytes to be %d, got %d", storedMsgSize*toStore, state.Bytes)
		}

		expectedBlocks := int(storedMsgSize * toStore / blkSize)
		if numBlocks := fs.numMsgBlocks(); numBlocks <= expectedBlocks {
			t.Fatalf("Expected to have more then %d msg blocks, got %d", blkSize, numBlocks)
		}

		fs.Purge()

		if numBlocks := fs.numMsgBlocks(); numBlocks != 1 {
			t.Fatalf("Expected to have exactly 1 empty msg block, got %d", numBlocks)
		}

		checkPurgeState := func(stored uint64) {
			t.Helper()
			state = fs.State()
			if state.Msgs != 0 {
				t.Fatalf("Expected 0 msgs after purge, got %d", state.Msgs)
			}
			if state.Bytes != 0 {
				t.Fatalf("Expected 0 bytes after purge, got %d", state.Bytes)
			}
			if state.LastSeq != stored {
				t.Fatalf("Expected LastSeq to be %d., got %d", toStore, state.LastSeq)
			}
			if state.FirstSeq != stored+1 {
				t.Fatalf("Expected FirstSeq to be %d., got %d", toStore+1, state.FirstSeq)
			}
		}
		checkPurgeState(toStore)

		// Make sure we recover same state.
		fs.Stop()

		fs, err = newFileStoreWithCreated(fcfg, StreamConfig{Name: "zzz", Storage: FileStorage}, created, prf(&fcfg), nil)
		require_NoError(t, err)
		defer fs.Stop()

		if numBlocks := fs.numMsgBlocks(); numBlocks != 1 {
			t.Fatalf("Expected to have exactly 1 empty msg block, got %d", numBlocks)
		}

		checkPurgeState(toStore)

		// Now make sure we clean up any dangling purged messages.
		for i := uint64(0); i < toStore; i++ {
			fs.StoreMsg(subj, nil, msg, 0)
		}
		state = fs.State()
		if state.Msgs != toStore {
			t.Fatalf("Expected %d msgs, got %d", toStore, state.Msgs)
		}
		if state.Bytes != storedMsgSize*toStore {
			t.Fatalf("Expected bytes to be %d, got %d", storedMsgSize*toStore, state.Bytes)
		}

		// We will simulate crashing before the purge directory is cleared.
		mdir := filepath.Join(fs.fcfg.StoreDir, msgDir)
		pdir := filepath.Join(fs.fcfg.StoreDir, "ptest")
		os.Rename(mdir, pdir)
		os.MkdirAll(mdir, 0755)

		fs.Purge()
		checkPurgeState(toStore * 2)

		// Make sure we recover same state.
		fs.Stop()

		purgeDir := filepath.Join(fs.fcfg.StoreDir, purgeDir)
		os.Rename(pdir, purgeDir)

		fs, err = newFileStoreWithCreated(fcfg, StreamConfig{Name: "zzz", Storage: FileStorage}, created, prf(&fcfg), nil)
		require_NoError(t, err)
		defer fs.Stop()

		if numBlocks := fs.numMsgBlocks(); numBlocks != 1 {
			t.Fatalf("Expected to have exactly 1 empty msg block, got %d", numBlocks)
		}

		checkPurgeState(toStore * 2)

		checkFor(t, 2*time.Second, 100*time.Millisecond, func() error {
			if _, err := os.Stat(purgeDir); err == nil {
				return fmt.Errorf("purge directory still present")
			}
			return nil
		})
	})
}

func TestFileStoreEncryptedPurgeRecoveryAfterKeyRename(t *testing.T) {
	fcfg := FileStoreConfig{
		StoreDir:    t.TempDir(),
		Cipher:      AES,
		Compression: NoCompression,
		BlockSize:   64 * 1024,
	}
	created := time.Now()
	cfg := StreamConfig{Name: "zzz", Storage: FileStorage}

	fs, err := newFileStoreWithCreated(fcfg, cfg, created, prf(&fcfg), nil)
	require_NoError(t, err)

	subj, msg := "foo", make([]byte, 8*1024)
	toStore := uint64(1024)
	for i := uint64(0); i < toStore; i++ {
		_, _, err = fs.StoreMsg(subj, nil, msg, 0)
		require_NoError(t, err)
	}
	baseline := fs.State()

	storeDir := fcfg.StoreDir
	mdir := filepath.Join(storeDir, msgDir)
	preMsgs := filepath.Join(storeDir, "msgs.pre")
	// Snapshot the pre-purge msg directory so we can recreate the crash window.
	require_NoError(t, copyDir(t, preMsgs, mdir))

	// Run a real purge once to create a valid encrypted tombstone block + key.
	_, err = fs.Purge()
	require_NoError(t, err)

	fs.mu.RLock()
	tombIdx := fs.lmb.index
	fs.mu.RUnlock()

	require_NoError(t, fs.stop(false, false))

	postMsgs := filepath.Join(storeDir, "msgs.post")
	require_NoError(t, os.Rename(mdir, postMsgs))
	require_NoError(t, copyDir(t, mdir, preMsgs))
	// Force recovery from the block files instead of a fresh full-state snapshot.
	require_NoError(t, os.RemoveAll(filepath.Join(mdir, streamStreamStateFile)))

	tombBlk := fmt.Sprintf(blkScan, tombIdx)
	tombKey := fmt.Sprintf(keyScan, tombIdx)
	require_NoError(t, os.Rename(filepath.Join(postMsgs, tombBlk), filepath.Join(mdir, tombBlk)))

	ndir := filepath.Join(storeDir, newMsgDir)
	require_NoError(t, os.MkdirAll(ndir, defaultDirPerms))
	// Simulate a crash after moving N.key into __new_msgs__ but before moving N.blk.
	require_NoError(t, os.Rename(filepath.Join(postMsgs, tombKey), filepath.Join(ndir, tombKey)))

	fs, err = newFileStoreWithCreated(fcfg, cfg, created, prf(&fcfg), nil)
	require_NoError(t, err)
	defer fs.Stop()

	state := fs.State()
	require_Equal(t, state.Msgs, baseline.Msgs)
	require_Equal(t, state.Bytes, baseline.Bytes)
	require_Equal(t, state.FirstSeq, baseline.FirstSeq)
	require_Equal(t, state.LastSeq, baseline.LastSeq)

	if _, err := os.Stat(filepath.Join(mdir, tombBlk)); !os.IsNotExist(err) {
		t.Fatalf("Expected rollback to remove %q, got err=%v", tombBlk, err)
	}
	if _, err := os.Stat(ndir); !os.IsNotExist(err) {
		t.Fatalf("Expected rollback to remove %q, got err=%v", ndir, err)
	}
}

func TestFileStoreCompact(t *testing.T) {
	testFileStoreAllPermutations(t, func(t *testing.T, fcfg FileStoreConfig) {
		fcfg.BlockSize = 350
		created := time.Now()
		fs, err := newFileStoreWithCreated(fcfg, StreamConfig{Name: "zzz", Storage: FileStorage}, created, prf(&fcfg), nil)
		require_NoError(t, err)
		defer fs.Stop()

		subj, msg := "foo", []byte("Hello World")
		for i := 0; i < 10; i++ {
			fs.StoreMsg(subj, nil, msg, 0)
		}
		if state := fs.State(); state.Msgs != 10 {
			t.Fatalf("Expected 10 msgs, got %d", state.Msgs)
		}
		n, err := fs.Compact(6)
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		if n != 5 {
			t.Fatalf("Expected to have purged 5 msgs, got %d", n)
		}
		state := fs.State()
		if state.Msgs != 5 {
			t.Fatalf("Expected 5 msgs, got %d", state.Msgs)
		}
		if state.FirstSeq != 6 {
			t.Fatalf("Expected first seq of 6, got %d", state.FirstSeq)
		}
		// Now test that compact will also reset first if seq > last
		n, err = fs.Compact(100)
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		if n != 5 {
			t.Fatalf("Expected to have purged 5 msgs, got %d", n)
		}
		if state = fs.State(); state.FirstSeq != 100 {
			t.Fatalf("Expected first seq of 100, got %d", state.FirstSeq)
		}

		fs.Stop()

		fs, err = newFileStoreWithCreated(fcfg, StreamConfig{Name: "zzz", Storage: FileStorage}, created, prf(&fcfg), nil)
		require_NoError(t, err)
		defer fs.Stop()

		if state = fs.State(); state.FirstSeq != 100 {
			t.Fatalf("Expected first seq of 100, got %d", state.FirstSeq)
		}
	})
}

func TestFileStoreCompactLastPlusOne(t *testing.T) {
	testFileStoreAllPermutations(t, func(t *testing.T, fcfg FileStoreConfig) {
		fcfg.BlockSize = 8192
		fcfg.AsyncFlush = true

		fs, err := newFileStoreWithCreated(fcfg, StreamConfig{Name: "zzz", Storage: FileStorage}, time.Now(), prf(&fcfg), nil)
		require_NoError(t, err)
		defer fs.Stop()

		subj, msg := "foo", make([]byte, 10_000)
		for i := 0; i < 10_000; i++ {
			if _, _, err := fs.StoreMsg(subj, nil, msg, 0); err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}
		}

		// The performance of this test is quite terrible with compression
		// if we have AsyncFlush = false, so we'll batch flushes instead.
		fs.mu.Lock()
		fs.checkAndFlushLastBlock()
		fs.mu.Unlock()

		if state := fs.State(); state.Msgs != 10_000 {
			t.Fatalf("Expected 1000000 msgs, got %d", state.Msgs)
		}
		if _, err := fs.Compact(10_001); err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		state := fs.State()
		if state.Msgs != 0 {
			t.Fatalf("Expected no message but got %d", state.Msgs)
		}

		fs.StoreMsg(subj, nil, msg, 0)
		state = fs.State()
		if state.Msgs != 1 {
			t.Fatalf("Expected one message but got %d", state.Msgs)
		}
	})
}

func TestFileStoreCompactMsgCountBug(t *testing.T) {
	testFileStoreAllPermutations(t, func(t *testing.T, fcfg FileStoreConfig) {
		fs, err := newFileStoreWithCreated(fcfg, StreamConfig{Name: "zzz", Storage: FileStorage}, time.Now(), prf(&fcfg), nil)
		require_NoError(t, err)
		defer fs.Stop()

		subj, msg := "foo", []byte("Hello World")
		for i := 0; i < 10; i++ {
			fs.StoreMsg(subj, nil, msg, 0)
		}
		if state := fs.State(); state.Msgs != 10 {
			t.Fatalf("Expected 10 msgs, got %d", state.Msgs)
		}
		// Now delete 2,3,4.
		fs.EraseMsg(2)
		fs.EraseMsg(3)
		fs.EraseMsg(4)

		// Also delete 7,8, and 9.
		fs.RemoveMsg(7)
		fs.RemoveMsg(8)
		fs.RemoveMsg(9)

		n, err := fs.Compact(6)
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		// 1 & 5
		if n != 2 {
			t.Fatalf("Expected to have deleted 2 msgs, got %d", n)
		}
		if state := fs.State(); state.Msgs != 2 {
			t.Fatalf("Expected to have 2 remaining, got %d", state.Msgs)
		}
	})
}

func TestFileStoreCompactPerf(t *testing.T) {
	t.SkipNow()

	testFileStoreAllPermutations(t, func(t *testing.T, fcfg FileStoreConfig) {
		fcfg.BlockSize = 8192
		fcfg.AsyncFlush = true

		fs, err := newFileStoreWithCreated(fcfg, StreamConfig{Name: "zzz", Storage: FileStorage}, time.Now(), prf(&fcfg), nil)
		require_NoError(t, err)
		defer fs.Stop()

		subj, msg := "foo", []byte("Hello World")
		for i := 0; i < 100_000; i++ {
			fs.StoreMsg(subj, nil, msg, 0)
		}
		if state := fs.State(); state.Msgs != 100_000 {
			t.Fatalf("Expected 1000000 msgs, got %d", state.Msgs)
		}
		start := time.Now()
		n, err := fs.Compact(90_001)
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		t.Logf("Took %v to compact\n", time.Since(start))

		if n != 90_000 {
			t.Fatalf("Expected to have purged 90_000 msgs, got %d", n)
		}
		state := fs.State()
		if state.Msgs != 10_000 {
			t.Fatalf("Expected 10_000 msgs, got %d", state.Msgs)
		}
		if state.FirstSeq != 90_001 {
			t.Fatalf("Expected first seq of 90_001, got %d", state.FirstSeq)
		}
	})
}

func TestFileStoreStreamTruncate(t *testing.T) {
	testFileStoreAllPermutations(t, func(t *testing.T, fcfg FileStoreConfig) {
		fcfg.BlockSize = 350
		cfg := StreamConfig{Name: "zzz", Subjects: []string{"*"}, Storage: FileStorage}
		created := time.Now()

		fs, err := newFileStoreWithCreated(fcfg, cfg, created, prf(&fcfg), nil)
		require_NoError(t, err)
		defer fs.Stop()

		tseq := uint64(50)

		subj, toStore := "foo", uint64(100)
		for i := uint64(1); i < tseq; i++ {
			_, _, err := fs.StoreMsg(subj, nil, []byte("ok"), 0)
			require_NoError(t, err)
		}
		subj = "bar"
		for i := tseq; i <= toStore; i++ {
			_, _, err := fs.StoreMsg(subj, nil, []byte("ok"), 0)
			require_NoError(t, err)
		}

		if state := fs.State(); state.Msgs != toStore {
			t.Fatalf("Expected %d msgs, got %d", toStore, state.Msgs)
		}

		if err := fs.Truncate(tseq); err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		if state := fs.State(); state.Msgs != tseq {
			t.Fatalf("Expected %d msgs, got %d", tseq, state.Msgs)
		}

		// Now make sure we report properly if we have some deleted interior messages.
		fs.RemoveMsg(10)
		fs.RemoveMsg(20)
		fs.RemoveMsg(30)
		fs.RemoveMsg(40)

		tseq = uint64(25)
		if err := fs.Truncate(tseq); err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		state := fs.State()
		if state.Msgs != tseq-2 {
			t.Fatalf("Expected %d msgs, got %d", tseq-2, state.Msgs)
		}
		expected := []uint64{10, 20}
		if !reflect.DeepEqual(state.Deleted, expected) {
			t.Fatalf("Expected deleted to be %+v, got %+v\n", expected, state.Deleted)
		}

		before := state

		// Make sure we can recover same state.
		fs.Stop()

		fs, err = newFileStoreWithCreated(fcfg, cfg, created, prf(&fcfg), nil)
		require_NoError(t, err)
		defer fs.Stop()

		if state := fs.State(); !reflect.DeepEqual(state, before) {
			t.Fatalf("Expected state of %+v, got %+v", before, state)
		}

		mb := fs.getFirstBlock()
		require_True(t, mb != nil)
		require_NoError(t, mb.loadMsgs())

		// Also make sure we can recover properly with no index.db present.
		// We want to make sure we preserve tombstones from any blocks being deleted.
		fs.Stop()
		os.Remove(filepath.Join(fs.fcfg.StoreDir, msgDir, streamStreamStateFile))

		fs, err = newFileStoreWithCreated(fcfg, cfg, created, prf(&fcfg), nil)
		require_NoError(t, err)
		defer fs.Stop()

		if state := fs.State(); !reflect.DeepEqual(state, before) {
			t.Fatalf("Expected state of %+v, got %+v without index.db state", before, state)
		}
	})
}

func TestFileStoreStreamTruncateDeletedSequence(t *testing.T) {
	testFileStoreAllPermutations(t, func(t *testing.T, fcfg FileStoreConfig) {
		// Place sequences 1-10 in the first block and 11-12 in the next.
		fcfg.BlockSize = 350
		cfg := StreamConfig{Name: "zzz", Subjects: []string{"foo"}, Storage: FileStorage}
		created := time.Now()

		fs, err := newFileStoreWithCreated(fcfg, cfg, created, prf(&fcfg), nil)
		require_NoError(t, err)
		defer fs.Stop()

		for range 12 {
			_, _, err := fs.StoreMsg("foo", nil, []byte("ok"), 0)
			require_NoError(t, err)
		}

		// Delete and compact the truncation point
		for _, seq := range []uint64{4, 5} {
			removed, err := fs.RemoveMsg(seq)
			require_NoError(t, err)
			require_True(t, removed)
		}
		const truncateSeq = uint64(5)
		smb := fs.selectMsgBlock(truncateSeq)
		require_NotNil(t, smb)
		require_True(t, smb != fs.lmb)
		smb.mu.Lock()
		require_NoError(t, smb.compact())
		smb.finishedWithCache()
		smb.mu.Unlock()

		// Preserve the deleted truncation point as the logical LastSeq.
		require_NoError(t, fs.Truncate(truncateSeq))
		expected := fs.State()
		require_Equal(t, expected.Msgs, 3)
		require_Equal(t, expected.FirstSeq, 1)
		require_Equal(t, expected.LastSeq, truncateSeq)
		require_True(t, reflect.DeepEqual(expected.Deleted, []uint64{4, 5}))

		for seq := uint64(1); seq <= 3; seq++ {
			_, err := fs.LoadMsg(seq, nil)
			require_NoError(t, err)
		}
		for seq := uint64(4); seq <= 5; seq++ {
			_, err := fs.LoadMsg(seq, nil)
			require_Error(t, err, ErrStoreMsgNotFound, errDeletedMsg)
		}
		for seq := uint64(6); seq <= 12; seq++ {
			_, err := fs.LoadMsg(seq, nil)
			require_Error(t, err, ErrStoreEOF)
		}

		// Recover without index.db to verify that blocks are complete.
		require_NoError(t, fs.Stop())
		require_NoError(t, os.Remove(filepath.Join(fcfg.StoreDir, msgDir, streamStreamStateFile)))
		fs, err = newFileStoreWithCreated(fcfg, cfg, created, prf(&fcfg), nil)
		require_NoError(t, err)
		defer fs.Stop()
		require_True(t, reflect.DeepEqual(fs.State(), expected))
	})
}

func TestFileStoreStreamTruncateDeletedWithCompactedPrefix(t *testing.T) {
	testFileStoreAllPermutations(t, func(t *testing.T, fcfg FileStoreConfig) {
		fcfg.BlockSize = 120
		cfg := StreamConfig{Name: "zzz", Subjects: []string{"foo"}, Storage: FileStorage}

		fs, err := newFileStoreWithCreated(fcfg, cfg, time.Now(), prf(&fcfg), nil)
		require_NoError(t, err)
		defer fs.Stop()

		// Store 9 messages, creating 3 blocks
		// [1,2,3], [4,5,6], [7,8,9]
		for range 9 {
			_, _, err := fs.StoreMsg("foo", nil, []byte("ok"), 0)
			require_NoError(t, err)
		}

		// Remove messages 4 and 5, and compact
		for _, seq := range []uint64{4, 5} {
			removed, err := fs.RemoveMsg(seq)
			require_NoError(t, err)
			require_True(t, removed)
		}

		smb := fs.selectMsgBlock(4)
		require_NotNil(t, smb)
		smb.mu.Lock()
		require_NoError(t, smb.compact())
		smb.mu.Unlock()

		// With no prior message in the selected block, Truncate(5) removes the block.
		const truncateSeq = uint64(5)
		require_NoError(t, fs.Truncate(truncateSeq))
		fs.mu.RLock()
		removed := !slices.Contains(fs.blks, smb)
		fs.mu.RUnlock()
		require_True(t, removed)

		state := fs.State()
		require_Equal(t, state.Msgs, 3)
		require_Equal(t, state.FirstSeq, uint64(1))
		require_Equal(t, state.LastSeq, truncateSeq)
		require_True(t, reflect.DeepEqual(state.Deleted, []uint64{4, 5}))
	})
}

func TestFileStoreRemovePartialRecovery(t *testing.T) {
	testFileStoreAllPermutations(t, func(t *testing.T, fcfg FileStoreConfig) {
		cfg := StreamConfig{Name: "zzz", Subjects: []string{"foo"}, Storage: FileStorage}
		created := time.Now()

		fs, err := newFileStoreWithCreated(fcfg, cfg, created, prf(&fcfg), nil)
		require_NoError(t, err)
		defer fs.Stop()

		subj, msg := "foo", []byte("Hello World")
		toStore := 100
		for i := 0; i < toStore; i++ {
			fs.StoreMsg(subj, nil, msg, 0)
		}
		state := fs.State()
		if state.Msgs != uint64(toStore) {
			t.Fatalf("Expected %d msgs, got %d", toStore, state.Msgs)
		}

		// Remove half
		for i := 1; i <= toStore/2; i++ {
			fs.RemoveMsg(uint64(i))
		}

		state = fs.State()
		if state.Msgs != uint64(toStore/2) {
			t.Fatalf("Expected %d msgs, got %d", toStore/2, state.Msgs)
		}

		// Make sure we recover same state.
		fs.Stop()

		fs, err = newFileStoreWithCreated(fcfg, cfg, created, prf(&fcfg), nil)
		require_NoError(t, err)
		defer fs.Stop()

		state2 := fs.State()
		if !reflect.DeepEqual(state2, state) {
			t.Fatalf("Expected recovered state to be the same, got %+v vs %+v\n", state2, state)
		}
	})
}

func TestFileStoreRemoveOutOfOrderRecovery(t *testing.T) {
	testFileStoreAllPermutations(t, func(t *testing.T, fcfg FileStoreConfig) {
		cfg := StreamConfig{Name: "zzz", Subjects: []string{"foo"}, Storage: FileStorage}
		created := time.Now()

		fs, err := newFileStoreWithCreated(fcfg, cfg, created, prf(&fcfg), nil)
		require_NoError(t, err)
		defer fs.Stop()

		subj, msg := "foo", []byte("Hello World")
		toStore := 100
		for i := 0; i < toStore; i++ {
			fs.StoreMsg(subj, nil, msg, 0)
		}
		state := fs.State()
		if state.Msgs != uint64(toStore) {
			t.Fatalf("Expected %d msgs, got %d", toStore, state.Msgs)
		}

		// Remove evens
		for i := 2; i <= toStore; i += 2 {
			if removed, _ := fs.RemoveMsg(uint64(i)); !removed {
				t.Fatalf("Expected remove to return true")
			}
		}

		state = fs.State()
		if state.Msgs != uint64(toStore/2) {
			t.Fatalf("Expected %d msgs, got %d", toStore/2, state.Msgs)
		}

		var smv StoreMsg
		if _, err := fs.LoadMsg(1, &smv); err != nil {
			t.Fatalf("Expected to retrieve seq 1")
		}
		for i := 2; i <= toStore; i += 2 {
			if _, err := fs.LoadMsg(uint64(i), &smv); err == nil {
				t.Fatalf("Expected error looking up seq %d that should be deleted", i)
			}
		}

		// Make sure we recover same state.
		fs.Stop()

		fs, err = newFileStoreWithCreated(fcfg, cfg, created, prf(&fcfg), nil)
		require_NoError(t, err)
		defer fs.Stop()

		state2 := fs.State()
		if !reflect.DeepEqual(state2, state) {
			t.Fatalf("Expected recovered states to be the same, got %+v vs %+v\n", state, state2)
		}

		if _, err := fs.LoadMsg(1, &smv); err != nil {
			t.Fatalf("Expected to retrieve seq 1")
		}
		for i := 2; i <= toStore; i += 2 {
			if _, err := fs.LoadMsg(uint64(i), nil); err == nil {
				t.Fatalf("Expected error looking up seq %d that should be deleted", i)
			}
		}
	})
}

func TestFileStoreAgeLimitRecovery(t *testing.T) {
	maxAge := 1 * time.Second

	testFileStoreAllPermutations(t, func(t *testing.T, fcfg FileStoreConfig) {
		fcfg.CacheExpire = 1 * time.Millisecond
		cfg := StreamConfig{Name: "zzz", Subjects: []string{"foo"}, Storage: FileStorage, MaxAge: maxAge}
		created := time.Now()

		fs, err := newFileStoreWithCreated(fcfg, cfg, created, prf(&fcfg), nil)
		require_NoError(t, err)
		defer fs.Stop()

		// Store some messages. Does not really matter how many.
		subj, msg := "foo", []byte("Hello World")
		toStore := 100
		for i := 0; i < toStore; i++ {
			fs.StoreMsg(subj, nil, msg, 0)
		}
		state := fs.State()
		if state.Msgs != uint64(toStore) {
			t.Fatalf("Expected %d msgs, got %d", toStore, state.Msgs)
		}
		fs.Stop()

		time.Sleep(maxAge)

		fcfg.CacheExpire = 0
		fs, err = newFileStoreWithCreated(fcfg, cfg, created, prf(&fcfg), nil)
		require_NoError(t, err)
		defer fs.Stop()

		// Make sure they expire.
		checkFor(t, time.Second, 2*maxAge, func() error {
			t.Helper()
			state = fs.State()
			if state.Msgs != 0 {
				return fmt.Errorf("Expected no msgs, got %d", state.Msgs)
			}
			if state.Bytes != 0 {
				return fmt.Errorf("Expected no bytes, got %d", state.Bytes)
			}
			return nil
		})
	})
}

func TestFileStoreBitRot(t *testing.T) {
	testFileStoreAllPermutations(t, func(t *testing.T, fcfg FileStoreConfig) {
		created := time.Now()
		fs, err := newFileStoreWithCreated(fcfg, StreamConfig{Name: "zzz", Storage: FileStorage}, created, prf(&fcfg), nil)
		require_NoError(t, err)
		defer fs.Stop()

		// Store some messages. Does not really matter how many.
		subj, msg := "foo", []byte("Hello World")
		toStore := 100
		for i := 0; i < toStore; i++ {
			fs.StoreMsg(subj, nil, msg, 0)
		}
		state := fs.State()
		if state.Msgs != uint64(toStore) {
			t.Fatalf("Expected %d msgs, got %d", toStore, state.Msgs)
		}

		if ld, err := fs.checkMsgs(); err != nil {
			t.Fatalf("Unexpected error from checkMsgs: %v", err)
		} else if ld != nil && len(ld.Msgs) > 0 {
			t.Fatalf("Expected to have no corrupt msgs, got %d", len(ld.Msgs))
		}

		for i := 0; i < 10; i++ {
			// Now twiddle some bits.
			fs.mu.Lock()
			lmb := fs.lmb
			contents, err := os.ReadFile(lmb.mfn)
			require_NoError(t, err)
			require_True(t, len(contents) > 0)

			var index int
			for {
				index = rand.IntN(len(contents))
				// Reverse one byte anywhere.
				b := contents[index]
				contents[index] = bits.Reverse8(b)
				if b != contents[index] {
					break
				}
			}
			os.WriteFile(lmb.mfn, contents, 0644)
			fs.mu.Unlock()

			ld, err := fs.checkMsgs()
			require_NoError(t, err)
			if len(ld.Msgs) > 0 {
				break
			}
			// If our bitrot caused us to not be able to recover any messages we can break as well.
			if state := fs.State(); state.Msgs == 0 {
				break
			}
			// Fail the test if we have tried the 10 times and still did not
			// get any corruption report.
			if i == 9 {
				t.Fatalf("Expected to have corrupt msgs got none: changed [%d]", index)
			}
		}

		// Make sure we can restore.
		fs.Stop()

		fs, err = newFileStoreWithCreated(fcfg, StreamConfig{Name: "zzz", Storage: FileStorage}, created, prf(&fcfg), nil)
		require_NoError(t, err)
		defer fs.Stop()

		// checkMsgs will repair the underlying store, so checkMsgs should be clean now.
		if ld, err := fs.checkMsgs(); err != nil {
			t.Fatalf("Unexpected error from checkMsgs: %v", err)
		} else if ld != nil {
			// If we have no msgs left this will report the head msgs as lost again.
			if state := fs.State(); state.Msgs > 0 {
				t.Fatalf("Expected no errors restoring checked and fixed filestore, got %+v", ld)
			}
		}
	})
}

func TestFileStoreEraseMsg(t *testing.T) {
	// Just do no encryption, etc.
	fcfg := FileStoreConfig{StoreDir: t.TempDir()}
	fs, err := newFileStore(fcfg, StreamConfig{Name: "zzz", Storage: FileStorage})
	require_NoError(t, err)
	defer fs.Stop()

	subj, msg := "foo", []byte("Hello World")
	fs.StoreMsg(subj, nil, msg, 0)
	fs.StoreMsg(subj, nil, msg, 0) // To keep block from being deleted.
	var smv StoreMsg
	sm, err := fs.LoadMsg(1, &smv)
	if err != nil {
		t.Fatalf("Unexpected error looking up msg: %v", err)
	}
	if !bytes.Equal(msg, sm.msg) {
		t.Fatalf("Expected same msg, got %q vs %q", sm.msg, msg)
	}
	if removed, _ := fs.EraseMsg(1); !removed {
		t.Fatalf("Expected erase msg to return success")
	}
	if sm2, _ := fs.msgForSeq(1, nil); sm2 != nil {
		t.Fatalf("Expected msg to be erased")
	}
	fs.checkAndFlushLastBlock()

	// Now look on disk as well.
	rl := fileStoreMsgSize(subj, nil, msg)
	buf := make([]byte, rl)
	fp, err := os.Open(filepath.Join(fcfg.StoreDir, msgDir, fmt.Sprintf(blkScan, 1)))
	if err != nil {
		t.Fatalf("Error opening msg block file: %v", err)
	}
	defer fp.Close()

	fp.ReadAt(buf, 0)
	fs.mu.RLock()
	mb := fs.blks[0]
	fs.mu.RUnlock()
	mb.mu.Lock()
	sm, err = mb.msgFromBuf(buf, nil, nil)
	mb.mu.Unlock()
	if err != nil {
		t.Fatalf("error reading message from block: %v", err)
	}
	if sm.subj == subj {
		t.Fatalf("Expected the subjects to be different")
	}
	if sm.seq != 0 && sm.seq&ebit == 0 {
		t.Fatalf("Expected seq to be 0, marking as deleted, got %d", sm.seq)
	}
	if sm.ts != 0 {
		t.Fatalf("Expected timestamp to be 0, got %d", sm.ts)
	}
	if bytes.Equal(sm.msg, msg) {
		t.Fatalf("Expected message body to be randomized")
	}
}

func TestFileStoreEraseAndNoIndexRecovery(t *testing.T) {
	testFileStoreAllPermutations(t, func(t *testing.T, fcfg FileStoreConfig) {
		created := time.Now()
		fs, err := newFileStoreWithCreated(fcfg, StreamConfig{Name: "zzz", Storage: FileStorage}, created, prf(&fcfg), nil)
		require_NoError(t, err)
		defer fs.Stop()

		subj, msg := "foo", []byte("Hello World")
		toStore := 100
		for i := 0; i < toStore; i++ {
			fs.StoreMsg(subj, nil, msg, 0)
		}
		state := fs.State()
		if state.Msgs != uint64(toStore) {
			t.Fatalf("Expected %d msgs, got %d", toStore, state.Msgs)
		}

		// Erase the even messages.
		for i := 2; i <= toStore; i += 2 {
			if removed, _ := fs.EraseMsg(uint64(i)); !removed {
				t.Fatalf("Expected erase msg to return true")
			}
		}
		state = fs.State()
		if state.Msgs != uint64(toStore/2) {
			t.Fatalf("Expected %d msgs, got %d", toStore/2, state.Msgs)
		}

		// Stop and remove the optional index file.
		fs.Stop()
		ifn := filepath.Join(fcfg.StoreDir, msgDir, fmt.Sprintf(indexScan, 1))
		os.Remove(ifn)

		fs, err = newFileStoreWithCreated(fcfg, StreamConfig{Name: "zzz", Storage: FileStorage}, created, prf(&fcfg), nil)
		require_NoError(t, err)
		defer fs.Stop()

		state = fs.State()
		if state.Msgs != uint64(toStore/2) {
			t.Fatalf("Expected %d msgs, got %d", toStore/2, state.Msgs)
		}

		for i := 2; i <= toStore; i += 2 {
			if _, err := fs.LoadMsg(uint64(i), nil); err == nil {
				t.Fatalf("Expected error looking up seq %d that should be erased", i)
			}
		}
	})
}

func TestFileStoreMeta(t *testing.T) {
	// Just do no encryption, etc.
	fcfg := FileStoreConfig{StoreDir: t.TempDir()}
	mconfig := StreamConfig{Name: "ZZ-22-33", Storage: FileStorage, Subjects: []string{"foo.*"}, Replicas: 22}
	fs, err := newFileStore(fcfg, mconfig)
	require_NoError(t, err)
	defer fs.Stop()

	metafile := filepath.Join(fcfg.StoreDir, JetStreamMetaFile)
	metasum := filepath.Join(fcfg.StoreDir, JetStreamMetaFileSum)

	// Test to make sure meta file and checksum are present.
	if _, err := os.Stat(metafile); os.IsNotExist(err) {
		t.Fatalf("Expected metafile %q to exist", metafile)
	}
	if _, err := os.Stat(metasum); os.IsNotExist(err) {
		t.Fatalf("Expected metafile's checksum %q to exist", metasum)
	}

	buf, err := os.ReadFile(metafile)
	if err != nil {
		t.Fatalf("Error reading metafile: %v", err)
	}
	var mconfig2 StreamConfig
	if err := json.Unmarshal(buf, &mconfig2); err != nil {
		t.Fatalf("Error unmarshalling: %v", err)
	}
	if !reflect.DeepEqual(mconfig, mconfig2) {
		t.Fatalf("Stream configs not equal, got %+v vs %+v", mconfig2, mconfig)
	}
	checksum, err := os.ReadFile(metasum)
	if err != nil {
		t.Fatalf("Error reading metafile checksum: %v", err)
	}

	fs.mu.Lock()
	fs.hh.Reset()
	fs.hh.Write(buf)
	mychecksum := hex.EncodeToString(fs.hh.Sum(nil))
	fs.mu.Unlock()

	if mychecksum != string(checksum) {
		t.Fatalf("Checksums do not match, got %q vs %q", mychecksum, checksum)
	}

	// Now create a consumer. Same deal for them.
	oconfig := ConsumerConfig{
		DeliverSubject: "d",
		FilterSubject:  "foo",
		AckPolicy:      AckAll,
	}
	oname := "obs22"
	obs, err := fs.ConsumerStore(oname, time.Time{}, &oconfig)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	ometafile := filepath.Join(fcfg.StoreDir, consumerDir, oname, JetStreamMetaFile)
	ometasum := filepath.Join(fcfg.StoreDir, consumerDir, oname, JetStreamMetaFileSum)

	// Test to make sure meta file and checksum are present.
	if _, err := os.Stat(ometafile); os.IsNotExist(err) {
		t.Fatalf("Expected consumer metafile %q to exist", ometafile)
	}
	if _, err := os.Stat(ometasum); os.IsNotExist(err) {
		t.Fatalf("Expected consumer metafile's checksum %q to exist", ometasum)
	}

	buf, err = os.ReadFile(ometafile)
	if err != nil {
		t.Fatalf("Error reading consumer metafile: %v", err)
	}

	var oconfig2 ConsumerConfig
	if err := json.Unmarshal(buf, &oconfig2); err != nil {
		t.Fatalf("Error unmarshalling: %v", err)
	}
	// Since we set name we will get that back now.
	oconfig.Name = oname
	if !reflect.DeepEqual(oconfig2, oconfig) {
		t.Fatalf("Consumer configs not equal, got %+v vs %+v", oconfig2, oconfig)
	}
	checksum, err = os.ReadFile(ometasum)

	if err != nil {
		t.Fatalf("Error reading consumer metafile checksum: %v", err)
	}

	hh := obs.(*consumerFileStore).hh
	hh.Reset()
	hh.Write(buf)
	mychecksum = hex.EncodeToString(hh.Sum(nil))
	if mychecksum != string(checksum) {
		t.Fatalf("Checksums do not match, got %q vs %q", mychecksum, checksum)
	}
}

func TestFileStoreWriteAndReadSameBlock(t *testing.T) {
	testFileStoreAllPermutations(t, func(t *testing.T, fcfg FileStoreConfig) {
		fs, err := newFileStoreWithCreated(fcfg, StreamConfig{Name: "zzz", Storage: FileStorage}, time.Now(), prf(&fcfg), nil)
		require_NoError(t, err)
		defer fs.Stop()

		subj, msg := "foo", []byte("Hello World!")

		for i := uint64(1); i <= 10; i++ {
			fs.StoreMsg(subj, nil, msg, 0)
			if _, err := fs.LoadMsg(i, nil); err != nil {
				t.Fatalf("Error loading %d: %v", i, err)
			}
		}
	})
}

func TestFileStoreAndRetrieveMultiBlock(t *testing.T) {
	testFileStoreAllPermutations(t, func(t *testing.T, fcfg FileStoreConfig) {
		subj, msg := "foo", []byte("Hello World!")
		storedMsgSize := fileStoreMsgSize(subj, nil, msg)

		fcfg.BlockSize = 4 * storedMsgSize
		created := time.Now()

		fs, err := newFileStoreWithCreated(fcfg, StreamConfig{Name: "zzz", Storage: FileStorage}, created, prf(&fcfg), nil)
		require_NoError(t, err)
		defer fs.Stop()

		for i := 0; i < 20; i++ {
			fs.StoreMsg(subj, nil, msg, 0)
		}
		state := fs.State()
		if state.Msgs != 20 {
			t.Fatalf("Expected 20 msgs, got %d", state.Msgs)
		}
		fs.Stop()

		fs, err = newFileStoreWithCreated(fcfg, StreamConfig{Name: "zzz", Storage: FileStorage}, created, prf(&fcfg), nil)
		require_NoError(t, err)
		defer fs.Stop()

		var smv StoreMsg
		for i := uint64(1); i <= 20; i++ {
			if _, err := fs.LoadMsg(i, &smv); err != nil {
				t.Fatalf("Error loading %d: %v", i, err)
			}
		}
	})
}

func TestFileStoreCollapseDmap(t *testing.T) {
	testFileStoreAllPermutations(t, func(t *testing.T, fcfg FileStoreConfig) {
		subj, msg := "foo", []byte("Hello World!")
		storedMsgSize := fileStoreMsgSize(subj, nil, msg)

		fcfg.BlockSize = 4 * storedMsgSize
		fs, err := newFileStoreWithCreated(fcfg, StreamConfig{Name: "zzz", Storage: FileStorage}, time.Now(), prf(&fcfg), nil)
		require_NoError(t, err)
		defer fs.Stop()

		for i := 0; i < 10; i++ {
			fs.StoreMsg(subj, nil, msg, 0)
		}
		state := fs.State()
		if state.Msgs != 10 {
			t.Fatalf("Expected 10 msgs, got %d", state.Msgs)
		}

		checkDmapTotal := func(total int) {
			t.Helper()
			if nde := fs.dmapEntries(); nde != total {
				t.Fatalf("Expecting only %d entries, got %d", total, nde)
			}
		}

		checkFirstSeq := func(seq uint64) {
			t.Helper()
			state := fs.State()
			if state.FirstSeq != seq {
				t.Fatalf("Expected first seq to be %d, got %d", seq, state.FirstSeq)
			}
		}

		// Now remove some out of order, forming gaps and entries in dmaps.
		fs.RemoveMsg(2)
		checkFirstSeq(1)
		fs.RemoveMsg(4)
		checkFirstSeq(1)
		fs.RemoveMsg(8)
		checkFirstSeq(1)

		state = fs.State()
		if state.Msgs != 7 {
			t.Fatalf("Expected 7 msgs, got %d", state.Msgs)
		}

		checkDmapTotal(3)

		// Close gaps..
		fs.RemoveMsg(1)
		checkDmapTotal(2)
		checkFirstSeq(3)

		fs.RemoveMsg(3)
		checkDmapTotal(1)
		checkFirstSeq(5)

		fs.RemoveMsg(5)
		checkDmapTotal(1)
		checkFirstSeq(6)

		fs.RemoveMsg(7)
		checkDmapTotal(2)

		fs.RemoveMsg(6)
		checkDmapTotal(0)
	})
}

func TestFileStoreReadCache(t *testing.T) {
	testFileStoreAllPermutations(t, func(t *testing.T, fcfg FileStoreConfig) {
		fcfg.CacheExpire = ats.TickInterval

		subj, msg := "foo.bar", make([]byte, 1024)
		storedMsgSize := fileStoreMsgSize(subj, nil, msg)

		fs, err := newFileStoreWithCreated(fcfg, StreamConfig{Name: "zzz", Storage: FileStorage}, time.Now(), prf(&fcfg), nil)
		require_NoError(t, err)
		defer fs.Stop()

		toStore := 500
		totalBytes := uint64(toStore) * storedMsgSize

		for i := 0; i < toStore; i++ {
			fs.StoreMsg(subj, nil, msg, 0)
		}

		// Wait for cache to go to zero.
		checkFor(t, time.Second, 10*time.Millisecond, func() error {
			if csz := fs.cacheSize(); csz != 0 {
				return fmt.Errorf("cache size not 0, got %s", friendlyBytes(int64(csz)))
			}
			return nil
		})

		fs.LoadMsg(1, nil)
		if csz := fs.cacheSize(); csz != totalBytes {
			t.Fatalf("Expected all messages to be cached, got %d vs %d", csz, totalBytes)
		}
		// Should expire and be removed.
		checkFor(t, time.Second, 10*time.Millisecond, func() error {
			if csz := fs.cacheSize(); csz != 0 {
				return fmt.Errorf("cache size not 0, got %s", friendlyBytes(int64(csz)))
			}
			return nil
		})
		if cls := fs.cacheLoads(); cls != 1 {
			t.Fatalf("Expected only 1 cache load, got %d", cls)
		}
		// Now make sure we do not reload cache if there is activity.
		fs.LoadMsg(1, nil)
		timeout := time.Now().Add(250 * time.Millisecond)
		for time.Now().Before(timeout) {
			if cls := fs.cacheLoads(); cls != 2 {
				t.Fatalf("cache loads not 2, got %d", cls)
			}
			time.Sleep(5 * time.Millisecond)
			fs.LoadMsg(1, nil) // register activity.
		}
	})
}

func TestFileStoreWeakCachePromotionCleanup(t *testing.T) {
	newTestStore := func(t *testing.T) (*fileStore, *msgBlock, *cache) {
		t.Helper()
		fs, err := newFileStore(
			FileStoreConfig{StoreDir: t.TempDir(), BlockSize: 8192},
			StreamConfig{Name: "zzz", Storage: FileStorage, Subjects: []string{"foo", "bar"}},
		)
		require_NoError(t, err)
		t.Cleanup(func() { fs.Stop() })

		for i := 0; i < 4; i++ {
			_, _, err = fs.StoreMsg("foo", nil, []byte("hello"), 0)
			require_NoError(t, err)
		}
		_, _, err = fs.StoreMsg("bar", nil, []byte("hello"), 0)
		require_NoError(t, err)
		require_NoError(t, fs.FlushAllPending())

		fs.mu.Lock()
		defer fs.mu.Unlock()

		mb := fs.lmb
		mb.mu.Lock()
		defer mb.mu.Unlock()

		require_NoError(t, mb.loadMsgsWithLock())
		require_NotNil(t, mb.cache)
		c := mb.cache
		mb.ecache.Set(c)
		mb.ecache.Weaken()
		mb.cache = nil
		mb.fss = nil
		if info, ok := fs.psim.Find(stringToBytes("foo")); ok {
			info.fblk, info.lblk = mb.index, mb.index
		}
		if info, ok := fs.psim.Find(stringToBytes("bar")); ok {
			info.fblk, info.lblk = mb.index, mb.index
		}
		require_NotNil(t, mb.ecache.Value())

		return fs, mb, c
	}

	require_NoStrongCache := func(t *testing.T, mb *msgBlock, c *cache) {
		t.Helper()
		require_True(t, mb.cache == nil)
		require_True(t, mb.ecache.Value() == c)
		runtime.KeepAlive(c)
	}

	t.Run("removeMsgFromBlock", func(t *testing.T) {
		fs, mb, c := newTestStore(t)
		removed, err := fs.removeMsg(2, false, true, true)
		require_NoError(t, err)
		require_True(t, removed)
		mb.mu.Lock()
		defer mb.mu.Unlock()
		require_NoStrongCache(t, mb, c)
	})

	t.Run("generatePerSubjectInfo", func(t *testing.T) {
		_, mb, c := newTestStore(t)
		mb.mu.Lock()
		defer mb.mu.Unlock()
		require_NoError(t, mb.generatePerSubjectInfo())
		require_NotNil(t, mb.fss)
		require_NoStrongCache(t, mb, c)
	})

	t.Run("filteredPending", func(t *testing.T) {
		_, mb, c := newTestStore(t)
		total, first, last, err := mb.filteredPending("foo", false, 1)
		require_NoError(t, err)
		require_Equal(t, total, uint64(4))
		require_Equal(t, first, uint64(1))
		require_Equal(t, last, uint64(4))
		mb.mu.Lock()
		defer mb.mu.Unlock()
		require_NoStrongCache(t, mb, c)
	})

	t.Run("loadLast", func(t *testing.T) {
		fs, mb, c := newTestStore(t)
		sm, err := fs.loadLast("foo", nil)
		require_NoError(t, err)
		require_NotNil(t, sm)
		require_Equal(t, sm.seq, uint64(4))
		mb.mu.Lock()
		defer mb.mu.Unlock()
		require_NoStrongCache(t, mb, c)
	})

	t.Run("recalculateForSubj", func(t *testing.T) {
		fs, mb, c := newTestStore(t)
		// Removing the first msg for "foo" marks its SimpleState as needing
		// a lazy first sequence recalculation.
		removed, err := fs.removeMsg(1, false, true, true)
		require_NoError(t, err)
		require_True(t, removed)
		mb.mu.Lock()
		defer mb.mu.Unlock()
		ss, ok := mb.fss.Find(stringToBytes("foo"))
		require_True(t, ok)
		require_True(t, ss.firstNeedsUpdate)
		// The cache is only weakly referenced, so recalculateForSubj will
		// promote it and must release the strong reference again.
		require_True(t, mb.cache == nil)
		require_NoError(t, mb.recalculateForSubj("foo", ss))
		require_Equal(t, ss.First, uint64(2))
		require_NoStrongCache(t, mb, c)
	})

	t.Run("firstSeqForSubj", func(t *testing.T) {
		fs, mb, _ := newTestStore(t)
		// Clear the weak reference too, forcing firstSeqForSubj to reload
		// the block from disk to get fss, taking a strong cache reference.
		mb.mu.Lock()
		mb.ecache.Set(nil)
		mb.mu.Unlock()
		fs.mu.Lock()
		seq, err := fs.firstSeqForSubj("foo")
		fs.mu.Unlock()
		require_NoError(t, err)
		require_Equal(t, seq, uint64(1))
		mb.mu.Lock()
		defer mb.mu.Unlock()
		require_True(t, mb.cache == nil)
		require_NotNil(t, mb.ecache.Value())
	})

	t.Run("firstSeqForSubjRecalculate", func(t *testing.T) {
		fs, mb, c := newTestStore(t)
		// Removing the first msg for "foo" marks its SimpleState as needing
		// a lazy first sequence recalculation, done inside firstSeqForSubj.
		removed, err := fs.removeMsg(1, false, true, true)
		require_NoError(t, err)
		require_True(t, removed)
		fs.mu.Lock()
		seq, err := fs.firstSeqForSubj("foo")
		fs.mu.Unlock()
		require_NoError(t, err)
		require_Equal(t, seq, uint64(2))
		mb.mu.Lock()
		defer mb.mu.Unlock()
		require_NoStrongCache(t, mb, c)
	})
}

func TestFileStorePartialCacheExpiration(t *testing.T) {
	testFileStoreAllPermutations(t, func(t *testing.T, fcfg FileStoreConfig) {
		cexp := 10 * time.Millisecond
		fcfg.CacheExpire = cexp

		fs, err := newFileStoreWithCreated(fcfg, StreamConfig{Name: "zzz", Storage: FileStorage}, time.Now(), prf(&fcfg), nil)
		require_NoError(t, err)
		defer fs.Stop()

		fs.StoreMsg("foo", nil, []byte("msg1"), 0)

		// Should expire and be removed.
		time.Sleep(2 * cexp)
		fs.StoreMsg("bar", nil, []byte("msg2"), 0)

		// Again wait for cache to expire.
		time.Sleep(2 * cexp)
		if _, err := fs.LoadMsg(1, nil); err != nil {
			t.Fatalf("Error loading message 1: %v", err)
		}
	})
}

func TestFileStorePartialIndexes(t *testing.T) {
	// TODO(nat): This test is no longer applicable as we no longer have positional
	// write caches but check before removing whether it proves anything else of value.
	t.SkipNow()

	testFileStoreAllPermutations(t, func(t *testing.T, fcfg FileStoreConfig) {
		cexp := 10 * time.Millisecond
		fcfg.CacheExpire = cexp

		fs, err := newFileStoreWithCreated(fcfg, StreamConfig{Name: "zzz", Storage: FileStorage}, time.Now(), prf(&fcfg), nil)
		require_NoError(t, err)
		defer fs.Stop()

		toSend := 5
		for i := 0; i < toSend; i++ {
			fs.StoreMsg("foo", nil, []byte("ok-1"), 0)
		}

		// Now wait til the cache expires, including the index.
		fs.mu.Lock()
		mb := fs.blks[0]
		fs.mu.Unlock()

		// Force idx to expire by resetting last remove ts.
		mb.mu.Lock()
		mb.llts = mb.llts - int64(defaultCacheBufferExpiration*2)
		mb.mu.Unlock()

		checkFor(t, time.Second, 10*time.Millisecond, func() error {
			mb.mu.Lock()
			defer mb.mu.Unlock()
			if mb.cache == nil || len(mb.cache.idx) == 0 {
				return nil
			}
			return fmt.Errorf("Index not empty")
		})

		// Create a partial cache by adding more msgs.
		for i := 0; i < toSend; i++ {
			fs.StoreMsg("foo", nil, []byte("ok-2"), 0)
		}
		// If we now load in a message in second half if we do not
		// detect idx is a partial correctly this will panic.
		if _, err := fs.LoadMsg(8, nil); err != nil {
			t.Fatalf("Error loading %d: %v", 1, err)
		}
	})
}

func TestFileStoreInvalidIndexesRebuilt(t *testing.T) {
	testFileStoreAllPermutations(t, func(t *testing.T, fcfg FileStoreConfig) {
		fs, err := newFileStoreWithCreated(fcfg, StreamConfig{Name: "zzz", Storage: FileStorage}, time.Now(), prf(&fcfg), nil)
		require_NoError(t, err)
		defer fs.Stop()

		toSend := 5
		for i := 0; i < toSend; i++ {
			_, _, err = fs.StoreMsg("foo", nil, []byte("ok-1"), 0)
			require_NoError(t, err)
		}
		require_NoError(t, fs.FlushAllPending())

		// Now we're going to mangle the in-memory cache by changing
		// the sequence number of the first message. We also need to
		// trick the cache into believing the hash is already validated
		// so we don't fail on that. This is specifically testing the
		// seq != fsm.seq condition.
		mb := fs.selectMsgBlock(1)
		require_NotNil(t, mb)
		require_NoError(t, mb.loadMsgs())
		require_True(t, mb.cacheAlreadyLoaded())
		ri, rl, _, err := mb.slotInfo(0)
		require_NoError(t, err)
		require_NotNil(t, mb.cache)
		require_NotNil(t, mb.cache.buf)
		slot := mb.cache.buf[ri : ri+rl]
		require_Equal(t, binary.LittleEndian.Uint64(slot[4:]), 1)
		binary.LittleEndian.PutUint64(slot[4:], 12345)
		mb.cache.idx[0] = (mb.cache.idx[0] | cbit)

		// Expect an error on the first instance and for cacheLookupEx
		// to discard the cache.
		_, err = mb.cacheLookupEx(1, nil, false)
		require_Error(t, err)
		require_True(t, mb.ecache.Value() == nil)

		// Now fetchMsg should notice and rebuild the index with the
		// correct sequence from disk.
		sm, _, err := mb.fetchMsg(1, nil)
		require_NoError(t, err)
		require_Equal(t, sm.seq, 1)
	})
}

func TestFileStoreSnapshot(t *testing.T) {
	testFileStoreAllPermutations(t, func(t *testing.T, fcfg FileStoreConfig) {
		subj, msg := "foo", []byte("Hello Snappy!")
		scfg := StreamConfig{Name: "zzz", Subjects: []string{"foo"}, Storage: FileStorage}

		fs, err := newFileStoreWithCreated(fcfg, scfg, time.Now(), prf(&fcfg), nil)
		require_NoError(t, err)
		defer fs.Stop()

		toSend := 2233
		for i := 0; i < toSend; i++ {
			fs.StoreMsg(subj, nil, msg, 0)
		}

		// Create a few consumers.
		o1, err := fs.ConsumerStore("o22", time.Time{}, &ConsumerConfig{})
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		o2, err := fs.ConsumerStore("o33", time.Time{}, &ConsumerConfig{})
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		state := &ConsumerState{}
		state.Delivered.Consumer = 100
		state.Delivered.Stream = 100
		state.AckFloor.Consumer = 22
		state.AckFloor.Stream = 22

		if err := o1.Update(state); err != nil {
			t.Fatalf("Unexpected error updating state: %v", err)
		}
		state.AckFloor.Consumer = 33
		state.AckFloor.Stream = 33

		if err := o2.Update(state); err != nil {
			t.Fatalf("Unexpected error updating state: %v", err)
		}

		snapshot := func() []byte {
			t.Helper()
			r, err := fs.Snapshot(5*time.Second, true, true)
			if err != nil {
				t.Fatalf("Error creating snapshot")
			}
			snapshot, err := io.ReadAll(r.Reader)
			if err != nil {
				t.Fatalf("Error reading snapshot")
			}
			return snapshot
		}

		// This will unzip the snapshot and create a new filestore that will recover the state.
		// We will compare the states for this vs the original one.
		verifySnapshot := func(snap []byte) {
			t.Helper()
			r := bytes.NewReader(snap)
			tr := tar.NewReader(s2.NewReader(r))

			rstoreDir := t.TempDir()

			for {
				hdr, err := tr.Next()
				if err == io.EOF {
					break // End of archive
				}
				if err != nil {
					t.Fatalf("Error getting next entry from snapshot: %v", err)
				}
				fpath := filepath.Join(rstoreDir, filepath.Clean(hdr.Name))
				pdir := filepath.Dir(fpath)
				os.MkdirAll(pdir, 0755)
				fd, err := os.OpenFile(fpath, os.O_CREATE|os.O_RDWR, 0600)
				if err != nil {
					t.Fatalf("Error opening file[%s]: %v", fpath, err)
				}
				if _, err := io.Copy(fd, tr); err != nil {
					t.Fatalf("Error writing file[%s]: %v", fpath, err)
				}
				fd.Close()
			}

			fcfg.StoreDir = rstoreDir
			fsr, err := newFileStoreWithCreated(fcfg, scfg, time.Now(), prf(&fcfg), nil)
			require_NoError(t, err)
			defer fsr.Stop()
			state := fs.State()
			rstate := fsr.State()

			// FIXME(dlc)
			// Right now the upper layers in JetStream recover the consumers and do not expect
			// the lower layers to do that. So for now blank that out of our original state.
			// Will have more exhaustive tests in jetstream_test.go.
			state.Consumers = 0

			// Just check the state.
			if !reflect.DeepEqual(rstate, state) {
				t.Fatalf("Restored state does not match:\n%+v\n\n%+v", rstate, state)
			}
		}

		// Simple case first.
		snap := snapshot()
		verifySnapshot(snap)

		// Remove first 100 messages.
		for i := 1; i <= 100; i++ {
			fs.RemoveMsg(uint64(i))
		}

		snap = snapshot()
		verifySnapshot(snap)

		// Now sporadic messages inside the stream.
		total := int64(toSend - 100)
		// Delete 50 random messages.
		for i := 0; i < 50; i++ {
			seq := uint64(rand.Int64N(total) + 101)
			fs.RemoveMsg(seq)
		}

		snap = snapshot()
		verifySnapshot(snap)

		// Make sure compaction works with snapshots.
		fs.mu.RLock()
		for _, mb := range fs.blks {
			// Should not call compact on last msg block.
			if mb != fs.lmb {
				mb.mu.Lock()
				err = mb.compact()
				mb.mu.Unlock()
				require_NoError(t, err)
			}
		}
		fs.mu.RUnlock()

		snap = snapshot()
		verifySnapshot(snap)

		// Now check to make sure that we can still delete/erase messages.
		sr, err := fs.Snapshot(5*time.Second, false, true)
		if err != nil {
			t.Fatalf("Error creating snapshot")
		}
		_, err = fs.RemoveMsg(122)
		require_NoError(t, err)

		// Now make sure we can do these when we close the reader and release the snapshot condition.
		sr.Reader.Close()
		checkFor(t, time.Second, 10*time.Millisecond, func() error {
			fs.mu.RLock()
			defer fs.mu.RUnlock()
			if fs.sips != 0 {
				return errors.New("snapshot is not finished")
			}
			return nil
		})

		// Make sure if we do not read properly then it will close the writer and report an error.
		sr, err = fs.Snapshot(25*time.Millisecond, false, false)
		if err != nil {
			t.Fatalf("Error creating snapshot")
		}

		// Cause snapshot to timeout.
		time.Sleep(50 * time.Millisecond)
		// Read should fail
		var buf [32]byte
		if _, err := sr.Reader.Read(buf[:]); err != io.EOF {
			t.Fatalf("Expected read to produce an error, got none")
		}
	})
}

func TestFileStoreSnapshotAndSyncBlocks(t *testing.T) {
	testFileStoreAllPermutations(t, func(t *testing.T, fcfg FileStoreConfig) {
		scfg := StreamConfig{Name: "zzz", Subjects: []string{"foo"}, Storage: FileStorage}
		fs, err := newFileStoreWithCreated(fcfg, scfg, time.Now(), prf(&fcfg), nil)
		require_NoError(t, err)
		defer fs.Stop()

		fs.cancelSyncTimer()
		fs.syncBlocks()

		fs.mu.Lock()
		if fs.syncTmr == nil {
			fs.mu.Unlock()
			t.Fatal("Expected sync timer to be set")
		}
		// Simulate a snapshot being in progress. This should not prevent us syncing blocks.
		fs.sips++
		fs.mu.Unlock()

		fs.cancelSyncTimer()
		fs.syncBlocks()

		fs.mu.Lock()
		if fs.syncTmr == nil {
			fs.mu.Unlock()
			t.Fatal("Expected sync timer to be set")
		}
		fs.mu.Unlock()
	})
}

func TestFileStoreConsumer(t *testing.T) {
	testFileStoreAllPermutations(t, func(t *testing.T, fcfg FileStoreConfig) {
		fs, err := newFileStoreWithCreated(fcfg, StreamConfig{Name: "zzz", Storage: FileStorage}, time.Now(), prf(&fcfg), nil)
		require_NoError(t, err)
		defer fs.Stop()

		o, err := fs.ConsumerStore("obs22", time.Time{}, &ConsumerConfig{})
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		if state, err := o.State(); err != nil || state.Delivered.Consumer != 0 {
			t.Fatalf("Unexpected state or error: %v", err)
		}

		state := &ConsumerState{}

		updateAndCheck := func() {
			t.Helper()
			if err := o.Update(state); err != nil {
				t.Fatalf("Unexpected error updating state: %v", err)
			}
			s2, err := o.State()
			if err != nil {
				t.Fatalf("Unexpected error getting state: %v", err)
			}
			if !reflect.DeepEqual(state, s2) {
				t.Fatalf("State is not the same: wanted %+v got %+v", state, s2)
			}
		}

		shouldFail := func() {
			t.Helper()
			if err := o.Update(state); err == nil {
				t.Fatalf("Expected an error and got none")
			}
		}

		state.Delivered.Consumer = 1
		state.Delivered.Stream = 22
		updateAndCheck()

		state.Delivered.Consumer = 100
		state.Delivered.Stream = 122
		state.AckFloor.Consumer = 50
		state.AckFloor.Stream = 123
		// This should fail, bad state.
		shouldFail()
		// So should this.
		state.AckFloor.Consumer = 200
		state.AckFloor.Stream = 100
		shouldFail()

		// Should succeed
		state.AckFloor.Consumer = 50
		state.AckFloor.Stream = 72
		updateAndCheck()

		tn := time.Now().UnixNano()

		// We should sanity check pending here as well, so will check if a pending value is below
		// ack floor or above delivered.
		state.Pending = map[uint64]*Pending{70: {70, tn}}
		shouldFail()
		state.Pending = map[uint64]*Pending{140: {140, tn}}
		shouldFail()
		state.Pending = map[uint64]*Pending{72: {72, tn}} // exact on floor should fail
		shouldFail()

		// Put timestamps a second apart.
		// We will downsample to second resolution to save space. So setup our times
		// to reflect that.
		ago := time.Now().Add(-30 * time.Second).Truncate(time.Second)
		nt := func() *Pending {
			ago = ago.Add(time.Second)
			return &Pending{0, ago.UnixNano()}
		}
		// Should succeed.
		state.Pending = map[uint64]*Pending{75: nt(), 80: nt(), 83: nt(), 90: nt(), 111: nt()}
		updateAndCheck()

		// Now do redlivery, but first with no pending.
		state.Pending = nil
		state.Redelivered = map[uint64]uint64{22: 3, 44: 8}
		updateAndCheck()

		// All together.
		state.Pending = map[uint64]*Pending{75: nt(), 80: nt(), 83: nt(), 90: nt(), 111: nt()}
		updateAndCheck()

		// Large one
		state.Delivered.Consumer = 10000
		state.Delivered.Stream = 10000
		state.AckFloor.Consumer = 100
		state.AckFloor.Stream = 100
		// Generate 8k pending.
		state.Pending = make(map[uint64]*Pending)
		for len(state.Pending) < 8192 {
			seq := uint64(rand.IntN(9890) + 101)
			if _, ok := state.Pending[seq]; !ok {
				state.Pending[seq] = nt()
			}
		}
		updateAndCheck()

		state.Pending = nil
		state.AckFloor.Consumer = 10000
		state.AckFloor.Stream = 10000
		updateAndCheck()
	})
}

func TestFileStoreConsumerEncodeDecodeRedelivered(t *testing.T) {
	state := &ConsumerState{}

	state.Delivered.Consumer = 100
	state.Delivered.Stream = 100
	state.AckFloor.Consumer = 50
	state.AckFloor.Stream = 50

	state.Redelivered = map[uint64]uint64{122: 3, 144: 8}
	buf := encodeConsumerState(state)

	rstate, err := decodeConsumerState(buf)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if !reflect.DeepEqual(state, rstate) {
		t.Fatalf("States do not match: %+v vs %+v", state, rstate)
	}
}

func TestFileStoreConsumerEncodeDecodePendingBelowStreamAckFloor(t *testing.T) {
	state := &ConsumerState{}

	state.Delivered.Consumer = 1192
	state.Delivered.Stream = 10185
	state.AckFloor.Consumer = 1189
	state.AckFloor.Stream = 10815

	now := time.Now().Round(time.Second).Add(-10 * time.Second).UnixNano()
	state.Pending = map[uint64]*Pending{
		10782: {1190, now},
		10810: {1191, now + int64(time.Second)},
		10815: {1192, now + int64(2*time.Second)},
	}
	buf := encodeConsumerState(state)

	rstate, err := decodeConsumerState(buf)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if len(rstate.Pending) != 3 {
		t.Fatalf("Invalid pending: %v", rstate.Pending)
	}
	for k, v := range state.Pending {
		rv, ok := rstate.Pending[k]
		if !ok {
			t.Fatalf("Did not find sseq=%v", k)
		}
		if !reflect.DeepEqual(v, rv) {
			t.Fatalf("Pending for sseq=%v should be %+v, got %+v", k, v, rv)
		}
	}
	state.Pending, rstate.Pending = nil, nil
	if !reflect.DeepEqual(*state, *rstate) {
		t.Fatalf("States do not match: %+v vs %+v", state, rstate)
	}
}

func TestFileStoreWriteFailures(t *testing.T) {
	// This test should be run inside an environment where this directory
	// has a limited size.
	// E.g. Docker
	// docker run -ti --tmpfs /jswf_test:rw,size=32k --rm -v ~/Development/go/src:/go/src -w /go/src/github.com/nats-io/nats-server/ golang:1.21 /bin/bash
	tdir := filepath.Join("/", "jswf_test")
	if stat, err := os.Stat(tdir); err != nil || !stat.IsDir() {
		t.SkipNow()
	}

	storeDir := filepath.Join(tdir, JetStreamStoreDir)
	os.MkdirAll(storeDir, 0755)

	testFileStoreAllPermutations(t, func(t *testing.T, fcfg FileStoreConfig) {
		fcfg.StoreDir = storeDir
		cfg := StreamConfig{Name: "zzz", Subjects: []string{"foo"}, Storage: FileStorage}
		created := time.Now()

		fs, err := newFileStoreWithCreated(fcfg, cfg, created, prf(&fcfg), nil)
		require_NoError(t, err)
		defer fs.Stop()

		subj, msg := "foo", []byte("Hello Write Failures!")

		var lseq uint64
		// msz about will be ~54 bytes, so if limit is 32k trying to send 1000 will fail at some point.
		for i := 1; i <= 1000; i++ {
			if _, _, err := fs.StoreMsg(subj, nil, msg, 0); err != nil {
				lseq = uint64(i)
				break
			}
		}
		if lseq == 0 {
			t.Fatalf("Expected to get a failure but did not")
		}

		state := fs.State()

		if state.LastSeq != lseq-1 {
			t.Fatalf("Expected last seq to be %d, got %d\n", lseq-1, state.LastSeq)
		}
		if state.Msgs != lseq-1 {
			t.Fatalf("Expected total msgs to be %d, got %d\n", lseq-1, state.Msgs)
		}
		if _, err := fs.LoadMsg(lseq, nil); err == nil {
			t.Fatalf("Expected error loading seq that failed, got none")
		}
		// Loading should still work.
		if _, err := fs.LoadMsg(1, nil); err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}

		// Make sure we recover same state.
		fs.Stop()

		fs, err = newFileStoreWithCreated(fcfg, cfg, created, prf(&fcfg), nil)
		require_NoError(t, err)
		defer fs.Stop()

		state2 := fs.State()

		// Ignore lost state.
		state.Lost, state2.Lost = nil, nil
		if !reflect.DeepEqual(state2, state) {
			t.Fatalf("Expected recovered state to be the same\n%+v\nvs\n%+v\n", state2, state)
		}

		// We should still fail here.
		for i := 1; i <= 100; i++ {
			_, _, err = fs.StoreMsg(subj, nil, msg, 0)
			if err != nil {
				break
			}
		}
		require_Error(t, err)

		lseq = fs.State().LastSeq + 1

		// Purge should help.
		if _, err := fs.Purge(); err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		// Wait for purge to complete its out of band processing.
		time.Sleep(50 * time.Millisecond)

		// Check we will fail again in same spot.
		for i := 1; i <= 1000; i++ {
			if _, _, err := fs.StoreMsg(subj, nil, msg, 0); err != nil {
				if i != int(lseq) {
					t.Fatalf("Expected to fail after purge about the same spot, wanted %d got %d", lseq, i)
				}
				break
			}
		}
	})
}

func TestFileStorePerf(t *testing.T) {
	// Comment out to run, holding place for now.
	t.SkipNow()

	testFileStoreAllPermutations(t, func(t *testing.T, fcfg FileStoreConfig) {
		fcfg.AsyncFlush = true

		subj, msg := "foo", make([]byte, 1024-33)
		for i := 0; i < len(msg); i++ {
			msg[i] = 'D'
		}
		storedMsgSize := fileStoreMsgSize(subj, nil, msg)

		// 5GB
		toStore := 5 * 1024 * 1024 * 1024 / storedMsgSize

		fmt.Printf("storing %d msgs of %s each, totalling %s\n",
			toStore,
			friendlyBytes(int64(storedMsgSize)),
			friendlyBytes(int64(toStore*storedMsgSize)),
		)

		created := time.Now()
		fs, err := newFileStoreWithCreated(fcfg, StreamConfig{Name: "zzz", Storage: FileStorage}, created, prf(&fcfg), nil)
		require_NoError(t, err)
		defer fs.Stop()

		start := time.Now()
		for i := 0; i < int(toStore); i++ {
			fs.StoreMsg(subj, nil, msg, 0)
		}
		fs.Stop()

		tt := time.Since(start)
		fmt.Printf("time to store is %v\n", tt)
		fmt.Printf("%.0f msgs/sec\n", float64(toStore)/tt.Seconds())
		fmt.Printf("%s per sec\n", friendlyBytes(int64(float64(toStore*storedMsgSize)/tt.Seconds())))

		fmt.Printf("Filesystem cache flush, paused 5 seconds.\n\n")
		time.Sleep(5 * time.Second)

		fmt.Printf("Restoring..\n")
		start = time.Now()
		fcfg.AsyncFlush = false
		fs, err = newFileStoreWithCreated(fcfg, StreamConfig{Name: "zzz", Storage: FileStorage}, created, prf(&fcfg), nil)
		require_NoError(t, err)
		defer fs.Stop()

		fmt.Printf("time to restore is %v\n\n", time.Since(start))

		fmt.Printf("LOAD: reading %d msgs of %s each, totalling %s\n",
			toStore,
			friendlyBytes(int64(storedMsgSize)),
			friendlyBytes(int64(toStore*storedMsgSize)),
		)

		var smv StoreMsg
		start = time.Now()
		for i := uint64(1); i <= toStore; i++ {
			if _, err := fs.LoadMsg(i, &smv); err != nil {
				t.Fatalf("Error loading %d: %v", i, err)
			}
		}

		tt = time.Since(start)
		fmt.Printf("time to read all back messages is %v\n", tt)
		fmt.Printf("%.0f msgs/sec\n", float64(toStore)/tt.Seconds())
		fmt.Printf("%s per sec\n", friendlyBytes(int64(float64(toStore*storedMsgSize)/tt.Seconds())))

		// Do again to test skip for hash..
		fmt.Printf("\nSKIP CHECKSUM: reading %d msgs of %s each, totalling %s\n",
			toStore,
			friendlyBytes(int64(storedMsgSize)),
			friendlyBytes(int64(toStore*storedMsgSize)),
		)

		start = time.Now()
		for i := uint64(1); i <= toStore; i++ {
			if _, err := fs.LoadMsg(i, &smv); err != nil {
				t.Fatalf("Error loading %d: %v", i, err)
			}
		}

		tt = time.Since(start)
		fmt.Printf("time to read all back messages is %v\n", tt)
		fmt.Printf("%.0f msgs/sec\n", float64(toStore)/tt.Seconds())
		fmt.Printf("%s per sec\n", friendlyBytes(int64(float64(toStore*storedMsgSize)/tt.Seconds())))

		fs.Stop()

		fs, err = newFileStoreWithCreated(fcfg, StreamConfig{Name: "zzz", Storage: FileStorage}, created, prf(&fcfg), nil)
		require_NoError(t, err)
		defer fs.Stop()

		fmt.Printf("\nremoving [in order] %d msgs of %s each, totalling %s\n",
			toStore,
			friendlyBytes(int64(storedMsgSize)),
			friendlyBytes(int64(toStore*storedMsgSize)),
		)

		start = time.Now()
		// For reverse order.
		//for i := toStore; i > 0; i-- {
		for i := uint64(1); i <= toStore; i++ {
			fs.RemoveMsg(i)
		}
		fs.Stop()

		tt = time.Since(start)
		fmt.Printf("time to remove all messages is %v\n", tt)
		fmt.Printf("%.0f msgs/sec\n", float64(toStore)/tt.Seconds())
		fmt.Printf("%s per sec\n", friendlyBytes(int64(float64(toStore*storedMsgSize)/tt.Seconds())))

		fs, err = newFileStoreWithCreated(fcfg, StreamConfig{Name: "zzz", Storage: FileStorage}, created, prf(&fcfg), nil)
		require_NoError(t, err)
		defer fs.Stop()

		state := fs.State()
		if state.Msgs != 0 {
			t.Fatalf("Expected no msgs, got %d", state.Msgs)
		}
		if state.Bytes != 0 {
			t.Fatalf("Expected no bytes, got %d", state.Bytes)
		}
	})
}

func TestFileStoreReadBackMsgPerf(t *testing.T) {
	// Comment out to run, holding place for now.
	t.SkipNow()

	subj := "foo"
	msg := []byte("ABCDEFGH") // Smaller shows problems more.

	storedMsgSize := fileStoreMsgSize(subj, nil, msg)

	// Make sure we store 2 blocks.
	toStore := defaultLargeBlockSize * 2 / storedMsgSize

	testFileStoreAllPermutations(t, func(t *testing.T, fcfg FileStoreConfig) {
		fmt.Printf("storing %d msgs of %s each, totalling %s\n",
			toStore,
			friendlyBytes(int64(storedMsgSize)),
			friendlyBytes(int64(toStore*storedMsgSize)),
		)

		fs, err := newFileStoreWithCreated(fcfg, StreamConfig{Name: "zzz", Storage: FileStorage}, time.Now(), prf(&fcfg), nil)
		require_NoError(t, err)
		defer fs.Stop()

		start := time.Now()
		for i := 0; i < int(toStore); i++ {
			fs.StoreMsg(subj, nil, msg, 0)
		}

		tt := time.Since(start)
		fmt.Printf("time to store is %v\n", tt)
		fmt.Printf("%.0f msgs/sec\n", float64(toStore)/tt.Seconds())
		fmt.Printf("%s per sec\n", friendlyBytes(int64(float64(toStore*storedMsgSize)/tt.Seconds())))

		// We should not have cached here with no reads.
		// Pick something towards end of the block.
		index := defaultLargeBlockSize/storedMsgSize - 22
		start = time.Now()
		fs.LoadMsg(index, nil)
		fmt.Printf("Time to load first msg [%d] = %v\n", index, time.Since(start))

		start = time.Now()
		fs.LoadMsg(index+2, nil)
		fmt.Printf("Time to load second msg [%d] = %v\n", index+2, time.Since(start))
	})
}

// This test is testing an upper level stream with a message or byte limit.
// Even though this is 1, any limit would trigger same behavior once the limit was reached
// and we were adding and removing.
func TestFileStoreStoreLimitRemovePerf(t *testing.T) {
	// Comment out to run, holding place for now.
	t.SkipNow()

	subj, msg := "foo", make([]byte, 1024-33)
	for i := 0; i < len(msg); i++ {
		msg[i] = 'D'
	}
	storedMsgSize := fileStoreMsgSize(subj, nil, msg)

	// 1GB
	toStore := 1 * 1024 * 1024 * 1024 / storedMsgSize

	testFileStoreAllPermutations(t, func(t *testing.T, fcfg FileStoreConfig) {
		fs, err := newFileStoreWithCreated(fcfg, StreamConfig{Name: "zzz", Storage: FileStorage}, time.Now(), prf(&fcfg), nil)
		require_NoError(t, err)
		defer fs.Stop()

		fs.RegisterStorageUpdates(func(md, bd int64, seq uint64, subj string) {})

		fmt.Printf("storing and removing (limit 1) %d msgs of %s each, totalling %s\n",
			toStore,
			friendlyBytes(int64(storedMsgSize)),
			friendlyBytes(int64(toStore*storedMsgSize)),
		)

		start := time.Now()
		for i := 0; i < int(toStore); i++ {
			seq, _, err := fs.StoreMsg(subj, nil, msg, 0)
			if err != nil {
				t.Fatalf("Unexpected error storing message: %v", err)
			}
			if i > 0 {
				fs.RemoveMsg(seq - 1)
			}
		}
		fs.Stop()

		tt := time.Since(start)
		fmt.Printf("time to store and remove all messages is %v\n", tt)
		fmt.Printf("%.0f msgs/sec\n", float64(toStore)/tt.Seconds())
		fmt.Printf("%s per sec\n", friendlyBytes(int64(float64(toStore*storedMsgSize)/tt.Seconds())))
	})
}

func TestFileStorePubPerfWithSmallBlkSize(t *testing.T) {
	// Comment out to run, holding place for now.
	t.SkipNow()

	subj, msg := "foo", make([]byte, 1024-33)
	for i := 0; i < len(msg); i++ {
		msg[i] = 'D'
	}
	storedMsgSize := fileStoreMsgSize(subj, nil, msg)

	// 1GB
	toStore := 1 * 1024 * 1024 * 1024 / storedMsgSize
	testFileStoreAllPermutations(t, func(t *testing.T, fcfg FileStoreConfig) {
		fmt.Printf("storing %d msgs of %s each, totalling %s\n",
			toStore,
			friendlyBytes(int64(storedMsgSize)),
			friendlyBytes(int64(toStore*storedMsgSize)),
		)

		fcfg.BlockSize = FileStoreMinBlkSize

		fs, err := newFileStoreWithCreated(fcfg, StreamConfig{Name: "zzz", Storage: FileStorage}, time.Now(), prf(&fcfg), nil)
		require_NoError(t, err)
		defer fs.Stop()

		start := time.Now()
		for i := 0; i < int(toStore); i++ {
			fs.StoreMsg(subj, nil, msg, 0)
		}
		fs.Stop()

		tt := time.Since(start)
		fmt.Printf("time to store is %v\n", tt)
		fmt.Printf("%.0f msgs/sec\n", float64(toStore)/tt.Seconds())
		fmt.Printf("%s per sec\n", friendlyBytes(int64(float64(toStore*storedMsgSize)/tt.Seconds())))
	})
}

// Saw this manifest from a restart test with max delivered set for JetStream.
func TestFileStoreConsumerRedeliveredLost(t *testing.T) {
	testFileStoreAllPermutations(t, func(t *testing.T, fcfg FileStoreConfig) {
		fs, err := newFileStoreWithCreated(fcfg, StreamConfig{Name: "zzz", Storage: FileStorage}, time.Now(), prf(&fcfg), nil)
		require_NoError(t, err)
		defer fs.Stop()

		cfg := &ConsumerConfig{AckPolicy: AckExplicit}
		o, err := fs.ConsumerStore("o22", time.Time{}, cfg)
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}

		restartConsumer := func() {
			t.Helper()
			o.Stop()
			time.Sleep(20 * time.Millisecond) // Wait for all things to settle.
			o, err = fs.ConsumerStore("o22", time.Time{}, cfg)
			if err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}
			// Make sure we recovered Redelivered.
			state, err := o.State()
			if err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}
			if state == nil {
				t.Fatalf("Did not recover state")
			}
			if len(state.Redelivered) == 0 {
				t.Fatalf("Did not recover redelivered")
			}
		}

		ts := time.Now().UnixNano()
		o.UpdateDelivered(1, 1, 1, ts)
		o.UpdateDelivered(2, 1, 2, ts)
		o.UpdateDelivered(3, 1, 3, ts)
		o.UpdateDelivered(4, 1, 4, ts)
		o.UpdateDelivered(5, 2, 1, ts)

		restartConsumer()

		o.UpdateDelivered(6, 2, 2, ts)
		o.UpdateDelivered(7, 3, 1, ts)

		restartConsumer()
		if state, _ := o.State(); len(state.Pending) != 3 {
			t.Fatalf("Did not recover pending correctly")
		}

		o.UpdateAcks(7, 3)
		o.UpdateAcks(6, 2)

		restartConsumer()
		o.UpdateAcks(4, 1)

		state, _ := o.State()
		if len(state.Pending) != 0 {
			t.Fatalf("Did not clear pending correctly")
		}
		if len(state.Redelivered) != 0 {
			t.Fatalf("Did not clear redelivered correctly")
		}
	})
}

func TestFileStoreConsumerUpdateAcksFlushesRedelivered(t *testing.T) {
	testFileStoreAllPermutations(t, func(t *testing.T, fcfg FileStoreConfig) {
		fs, err := newFileStoreWithCreated(fcfg, StreamConfig{Name: "zzz", Storage: FileStorage}, time.Now(), prf(&fcfg), nil)
		require_NoError(t, err)
		defer fs.Stop()

		// MaxDeliver: 1 means that on the second delivery the message is dropped from
		// pending while still being tracked in Redelivered.
		cfg := &ConsumerConfig{AckPolicy: AckExplicit, MaxDeliver: 1}
		o, err := fs.ConsumerStore("o22", time.Time{}, cfg)
		require_NoError(t, err)

		restartConsumer := func() {
			t.Helper()
			require_NoError(t, o.Stop())
			time.Sleep(200 * time.Millisecond) // Wait for all things to settle.
			o, err = fs.ConsumerStore("o22", time.Time{}, cfg)
			require_NoError(t, err)
		}

		ts := time.Now().UnixNano()
		require_NoError(t, o.UpdateDelivered(1, 1, 1, ts))
		// Redelivery exceeds MaxDeliver, so sseq 1 is removed from pending but kept
		// in Redelivered.
		require_NoError(t, o.UpdateDelivered(2, 1, 2, ts))

		// Persist and recover so we know the Redelivered entry is on disk.
		restartConsumer()
		state, err := o.State()
		require_NoError(t, err)
		require_Equal(t, len(state.Pending), 0)
		require_Equal(t, len(state.Redelivered), 1)

		// Acking sseq 1 deletes it from Redelivered, but UpdateAcks bails out with
		// ErrStoreMsgNotFound since it is no longer pending. The deletion must still
		// be flushed.
		require_Error(t, o.UpdateAcks(2, 1), ErrStoreMsgNotFound)

		restartConsumer()
		defer o.Stop()
		state, err = o.State()
		require_NoError(t, err)
		require_Equal(t, len(state.Redelivered), 0)
	})
}

func TestFileStoreConsumerFlusher(t *testing.T) {
	testFileStoreAllPermutations(t, func(t *testing.T, fcfg FileStoreConfig) {
		fs, err := newFileStoreWithCreated(fcfg, StreamConfig{Name: "zzz", Storage: FileStorage}, time.Now(), prf(&fcfg), nil)
		require_NoError(t, err)
		defer fs.Stop()

		o, err := fs.ConsumerStore("o22", time.Time{}, &ConsumerConfig{})
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		// Get the underlying impl.
		oc := o.(*consumerFileStore)
		// Wait for flusher to be running.
		checkFor(t, time.Second, 20*time.Millisecond, func() error {
			if !oc.inFlusher() {
				return fmt.Errorf("Flusher not running")
			}
			return nil
		})
		// Stop and make sure the flusher goes away
		o.Stop()
		// Wait for flusher to stop.
		checkFor(t, time.Second, 20*time.Millisecond, func() error {
			if oc.inFlusher() {
				return fmt.Errorf("Flusher still running")
			}
			return nil
		})
	})
}

func TestFileStoreConsumerDeliveredUpdates(t *testing.T) {
	testFileStoreAllPermutations(t, func(t *testing.T, fcfg FileStoreConfig) {
		fs, err := newFileStoreWithCreated(fcfg, StreamConfig{Name: "zzz", Storage: FileStorage}, time.Now(), prf(&fcfg), nil)
		require_NoError(t, err)
		defer fs.Stop()

		// Simple consumer, no ack policy configured.
		o, err := fs.ConsumerStore("o22", time.Time{}, &ConsumerConfig{})
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		defer o.Stop()

		testDelivered := func(dseq, sseq uint64) {
			t.Helper()
			ts := time.Now().UnixNano()
			if err := o.UpdateDelivered(dseq, sseq, 1, ts); err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}
			state, err := o.State()
			if err != nil {
				t.Fatalf("Error getting state: %v", err)
			}
			if state == nil {
				t.Fatalf("No state available")
			}
			expected := SequencePair{dseq, sseq}
			if state.Delivered != expected {
				t.Fatalf("Unexpected state, wanted %+v, got %+v", expected, state.Delivered)
			}
			if state.AckFloor != expected {
				t.Fatalf("Unexpected ack floor state, wanted %+v, got %+v", expected, state.AckFloor)
			}
			if len(state.Pending) != 0 {
				t.Fatalf("Did not expect any pending, got %d pending", len(state.Pending))
			}
		}

		testDelivered(1, 100)
		testDelivered(2, 110)
		testDelivered(5, 130)

		// If we try to do an ack this should err since we are not configured with ack policy.
		if err := o.UpdateAcks(1, 100); err != ErrNoAckPolicy {
			t.Fatalf("Expected a no ack policy error on update acks, got %v", err)
		}
		// Also if we do an update with a delivery count of anything but 1 here should also give same error.
		ts := time.Now().UnixNano()
		if err := o.UpdateDelivered(5, 130, 2, ts); err != ErrNoAckPolicy {
			t.Fatalf("Expected a no ack policy error on update delivered with dc > 1, got %v", err)
		}
	})
}

func TestFileStoreConsumerDeliveredAndAckUpdates(t *testing.T) {
	testFileStoreAllPermutations(t, func(t *testing.T, fcfg FileStoreConfig) {
		fs, err := newFileStoreWithCreated(fcfg, StreamConfig{Name: "zzz", Storage: FileStorage}, time.Now(), prf(&fcfg), nil)
		require_NoError(t, err)
		defer fs.Stop()

		// Simple consumer, no ack policy configured.
		o, err := fs.ConsumerStore("o22", time.Time{}, &ConsumerConfig{AckPolicy: AckExplicit})
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		defer o.Stop()

		// Track pending.
		var pending int

		testDelivered := func(dseq, sseq uint64) {
			t.Helper()
			ts := time.Now().Round(time.Second).UnixNano()
			if err := o.UpdateDelivered(dseq, sseq, 1, ts); err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}
			pending++
			state, err := o.State()
			if err != nil {
				t.Fatalf("Error getting state: %v", err)
			}
			if state == nil {
				t.Fatalf("No state available")
			}
			expected := SequencePair{dseq, sseq}
			if state.Delivered != expected {
				t.Fatalf("Unexpected delivered state, wanted %+v, got %+v", expected, state.Delivered)
			}
			if len(state.Pending) != pending {
				t.Fatalf("Expected %d pending, got %d pending", pending, len(state.Pending))
			}
		}

		testDelivered(1, 100)
		testDelivered(2, 110)
		testDelivered(3, 130)
		testDelivered(4, 150)
		testDelivered(5, 165)

		testBadAck := func(dseq, sseq uint64) {
			t.Helper()
			if err := o.UpdateAcks(dseq, sseq); err == nil {
				t.Fatalf("Expected error but got none")
			}
		}
		testBadAck(3, 101)
		testBadAck(1, 1)

		testAck := func(dseq, sseq, dflr, sflr uint64) {
			t.Helper()
			if err := o.UpdateAcks(dseq, sseq); err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}
			pending--
			state, err := o.State()
			if err != nil {
				t.Fatalf("Error getting state: %v", err)
			}
			if state == nil {
				t.Fatalf("No state available")
			}
			if len(state.Pending) != pending {
				t.Fatalf("Expected %d pending, got %d pending", pending, len(state.Pending))
			}
			eflr := SequencePair{dflr, sflr}
			if state.AckFloor != eflr {
				t.Fatalf("Unexpected ack floor state, wanted %+v, got %+v", eflr, state.AckFloor)
			}
		}

		testAck(1, 100, 1, 109)
		testAck(3, 130, 1, 109)
		testAck(2, 110, 3, 149) // We do not track explicit state on previous stream floors, so we take last known -1
		testAck(5, 165, 3, 149)
		testAck(4, 150, 5, 165)

		testDelivered(6, 170)
		testDelivered(7, 171)
		testDelivered(8, 172)
		testDelivered(9, 173)
		testDelivered(10, 200)

		testAck(7, 171, 5, 165)
		testAck(8, 172, 5, 165)

		state, err := o.State()
		if err != nil {
			t.Fatalf("Unexpected error getting state: %v", err)
		}
		o.Stop()

		o, err = fs.ConsumerStore("o22", time.Time{}, &ConsumerConfig{AckPolicy: AckExplicit})
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		defer o.Stop()

		nstate, err := o.State()
		if err != nil {
			t.Fatalf("Unexpected error getting state: %v", err)
		}
		if !reflect.DeepEqual(nstate, state) {
			t.Fatalf("States don't match! NEW %+v OLD %+v", nstate, state)
		}
	})
}

func TestFileStoreStreamStateDeleted(t *testing.T) {
	testFileStoreAllPermutations(t, func(t *testing.T, fcfg FileStoreConfig) {
		fs, err := newFileStoreWithCreated(fcfg, StreamConfig{Name: "zzz", Storage: FileStorage}, time.Now(), prf(&fcfg), nil)
		require_NoError(t, err)
		defer fs.Stop()

		subj, toStore := "foo", uint64(10)
		for i := uint64(1); i <= toStore; i++ {
			msg := []byte(fmt.Sprintf("[%08d] Hello World!", i))
			if _, _, err := fs.StoreMsg(subj, nil, msg, 0); err != nil {
				t.Fatalf("Error storing msg: %v", err)
			}
		}
		state := fs.State()
		if len(state.Deleted) != 0 {
			t.Fatalf("Expected deleted to be empty")
		}
		// Now remove some interior messages.
		var expected []uint64
		for seq := uint64(2); seq < toStore; seq += 2 {
			fs.RemoveMsg(seq)
			expected = append(expected, seq)
		}
		state = fs.State()
		if !reflect.DeepEqual(state.Deleted, expected) {
			t.Fatalf("Expected deleted to be %+v, got %+v\n", expected, state.Deleted)
		}
		// Now fill the gap by deleting 1 and 3
		fs.RemoveMsg(1)
		fs.RemoveMsg(3)
		expected = expected[2:]
		state = fs.State()
		if !reflect.DeepEqual(state.Deleted, expected) {
			t.Fatalf("Expected deleted to be %+v, got %+v\n", expected, state.Deleted)
		}
		if state.FirstSeq != 5 {
			t.Fatalf("Expected first seq to be 5, got %d", state.FirstSeq)
		}
		fs.Purge()
		if state = fs.State(); len(state.Deleted) != 0 {
			t.Fatalf("Expected no deleted after purge, got %+v\n", state.Deleted)
		}
	})
}

// We have reports that sometimes under load a stream could complain about a storage directory
// not being empty.
func TestFileStoreStreamDeleteDirNotEmpty(t *testing.T) {
	testFileStoreAllPermutations(t, func(t *testing.T, fcfg FileStoreConfig) {
		fs, err := newFileStoreWithCreated(fcfg, StreamConfig{Name: "zzz", Storage: FileStorage}, time.Now(), prf(&fcfg), nil)
		require_NoError(t, err)
		defer fs.Stop()

		subj, toStore := "foo", uint64(10)
		for i := uint64(1); i <= toStore; i++ {
			msg := []byte(fmt.Sprintf("[%08d] Hello World!", i))
			if _, _, err := fs.StoreMsg(subj, nil, msg, 0); err != nil {
				t.Fatalf("Error storing msg: %v", err)
			}
		}

		ready := make(chan bool)
		go func() {
			g := filepath.Join(fcfg.StoreDir, "g")
			ready <- true
			for i := 0; i < 100; i++ {
				os.WriteFile(g, []byte("OK"), defaultFilePerms)
			}
		}()

		<-ready
		if err := fs.Delete(true); err != nil {
			t.Fatalf("Delete returned an error: %v", err)
		}
	})
}

func TestFileStoreConsumerPerf(t *testing.T) {
	// Comment out to run, holding place for now.
	t.SkipNow()

	testFileStoreAllPermutations(t, func(t *testing.T, fcfg FileStoreConfig) {
		fs, err := newFileStoreWithCreated(fcfg, StreamConfig{Name: "zzz", Storage: FileStorage}, time.Now(), prf(&fcfg), nil)
		require_NoError(t, err)
		defer fs.Stop()

		o, err := fs.ConsumerStore("o22", time.Time{}, &ConsumerConfig{AckPolicy: AckExplicit})
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		// Get the underlying impl.
		oc := o.(*consumerFileStore)
		// Wait for flusher to br running
		checkFor(t, time.Second, 20*time.Millisecond, func() error {
			if !oc.inFlusher() {
				return fmt.Errorf("not in flusher")
			}
			return nil
		})

		// Stop flusher for this benchmark since we will invoke directly.
		oc.mu.Lock()
		qch := oc.qch
		oc.qch = nil
		oc.mu.Unlock()
		close(qch)

		checkFor(t, time.Second, 20*time.Millisecond, func() error {
			if oc.inFlusher() {
				return fmt.Errorf("still in flusher")
			}
			return nil
		})

		toStore := uint64(1_000_000)

		start := time.Now()

		ts := start.UnixNano()

		for i := uint64(1); i <= toStore; i++ {
			if err := o.UpdateDelivered(i, i, 1, ts); err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}
		}
		tt := time.Since(start)
		fmt.Printf("time to update %d is %v\n", toStore, tt)
		fmt.Printf("%.0f updates/sec\n", float64(toStore)/tt.Seconds())

		start = time.Now()
		oc.mu.Lock()
		buf, err := oc.encodeState()
		oc.mu.Unlock()
		if err != nil {
			t.Fatalf("Error encoding state: %v", err)
		}
		fmt.Printf("time to encode %d bytes is %v\n", len(buf), time.Since(start))
		start = time.Now()
		oc.writeState(buf)
		fmt.Printf("time to write is %v\n", time.Since(start))
	})
}

// Reported by Ivan.
func TestFileStoreStreamDeleteCacheBug(t *testing.T) {
	testFileStoreAllPermutations(t, func(t *testing.T, fcfg FileStoreConfig) {
		fcfg.CacheExpire = 50 * time.Millisecond

		fs, err := newFileStoreWithCreated(fcfg, StreamConfig{Name: "zzz", Storage: FileStorage}, time.Now(), prf(&fcfg), nil)
		require_NoError(t, err)
		defer fs.Stop()

		subj, msg := "foo", []byte("Hello World")

		if _, _, err := fs.StoreMsg(subj, nil, msg, 0); err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		if _, _, err := fs.StoreMsg(subj, nil, msg, 0); err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		if _, err := fs.EraseMsg(1); err != nil {
			t.Fatalf("Got an error on remove of %d: %v", 1, err)
		}
		time.Sleep(100 * time.Millisecond)
		if _, err := fs.LoadMsg(2, nil); err != nil {
			t.Fatalf("Unexpected error looking up msg: %v", err)
		}
	})
}

// rip
func TestFileStoreStreamFailToRollBug(t *testing.T) {
	testFileStoreAllPermutations(t, func(t *testing.T, fcfg FileStoreConfig) {
		fcfg.BlockSize = 512

		fs, err := newFileStoreWithCreated(fcfg, StreamConfig{Name: "zzz", Storage: FileStorage, MaxBytes: 300}, time.Now(), prf(&fcfg), nil)
		require_NoError(t, err)
		defer fs.Stop()

		// Make sure we properly roll underlying blocks.
		n, msg := 200, bytes.Repeat([]byte("ABC"), 33) // ~100bytes
		for i := 0; i < n; i++ {
			if _, _, err := fs.StoreMsg("zzz", nil, msg, 0); err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}
		}

		// Grab some info for introspection.
		fs.mu.RLock()
		numBlks := len(fs.blks)
		var index uint32
		var blkSize int64
		if numBlks > 0 {
			mb := fs.blks[0]
			mb.mu.RLock()
			index = mb.index
			if fi, _ := os.Stat(mb.mfn); fi != nil {
				blkSize = fi.Size()
			}
			mb.mu.RUnlock()
		}
		fs.mu.RUnlock()

		if numBlks != 1 {
			t.Fatalf("Expected only one block, got %d", numBlks)
		}
		if index < 60 {
			t.Fatalf("Expected a block index > 60, got %d", index)
		}
		if blkSize > 512 {
			t.Fatalf("Expected block to be <= 512, got %d", blkSize)
		}
	})
}

// We had a case where a consumer state had a redelivered record that had seq of 0.
// This was causing the server to panic.
func TestFileStoreBadConsumerState(t *testing.T) {
	bs := []byte("\x16\x02\x01\x01\x03\x02\x01\x98\xf4\x8a\x8a\f\x01\x03\x86\xfa\n\x01\x00\x01")
	if cs, err := decodeConsumerState(bs); err != nil || cs == nil {
		t.Fatalf("Expected to not throw error, got %v and %+v", err, cs)
	}
}

func TestFileStoreExpireMsgsOnStart(t *testing.T) {
	testFileStoreAllPermutations(t, func(t *testing.T, fcfg FileStoreConfig) {
		if fcfg.Compression != NoCompression || fcfg.Cipher != NoCipher {
			// TODO(nat): For some reason this test is very flaky in CI when using
			// encryption or compression but passes fine without.
			t.SkipNow()
		}

		fcfg.BlockSize = 8 * 1024
		ttl := 250 * time.Millisecond
		cfg := StreamConfig{Name: "ORDERS", Subjects: []string{"orders.*"}, Storage: FileStorage, MaxAge: ttl}
		var fs *fileStore

		startFS := func() *fileStore {
			t.Helper()
			fs, err := newFileStoreWithCreated(fcfg, cfg, time.Now(), prf(&fcfg), nil)
			require_NoError(t, err)
			return fs
		}

		newFS := func() *fileStore {
			t.Helper()
			if fs != nil {
				fs.Stop()
				fs = nil
			}
			removeDir(t, fcfg.StoreDir)
			return startFS()
		}

		restartFS := func(delay time.Duration) *fileStore {
			if fs != nil {
				fs.Stop()
				fs = nil
				time.Sleep(delay)
			}
			fs = startFS()
			return fs
		}

		fs = newFS()
		defer fs.Stop()

		msg := bytes.Repeat([]byte("ABC"), 33) // ~100bytes
		loadMsgs := func(n int) {
			t.Helper()
			for i := 1; i <= n; i++ {
				if _, _, err := fs.StoreMsg(fmt.Sprintf("orders.%d", i%10), nil, msg, 0); err != nil {
					t.Fatalf("Unexpected error: %v", err)
				}
			}
		}

		checkState := func(msgs, first, last uint64) {
			t.Helper()
			if fs == nil {
				t.Fatalf("No fs")
				return
			}
			state := fs.State()
			if state.Msgs != msgs {
				t.Fatalf("Expected %d msgs, got %d", msgs, state.Msgs)
			}
			if state.FirstSeq != first {
				t.Fatalf("Expected %d as first, got %d", first, state.FirstSeq)
			}
			if state.LastSeq != last {
				t.Fatalf("Expected %d as last, got %d", last, state.LastSeq)
			}
		}

		checkNumBlks := func(expected int) {
			t.Helper()
			fs.mu.RLock()
			n := len(fs.blks)
			fs.mu.RUnlock()
			if n != expected {
				t.Fatalf("Expected %d msg blks, got %d", expected, n)
			}
		}

		// Check the filtered subject state and make sure that is tracked properly.
		checkFiltered := func(subject string, ss SimpleState) {
			t.Helper()
			fss, err := fs.FilteredState(1, subject)
			require_NoError(t, err)
			if fss != ss {
				t.Fatalf("Expected FilteredState of %+v, got %+v", ss, fss)
			}
		}

		// Make sure state on disk matches (e.g. writeIndexInfo properly called)
		checkBlkState := func(index int) {
			t.Helper()
			fs.mu.RLock()
			if index >= len(fs.blks) {
				t.Fatalf("Out of range, wanted %d but only %d blks", index, len(fs.blks))
			}
			fs.mu.RUnlock()
		}

		lastSeqForBlk := func() uint64 {
			t.Helper()
			fs.mu.RLock()
			defer fs.mu.RUnlock()
			if len(fs.blks) == 0 {
				t.Fatalf("No blocks?")
			}
			mb := fs.blks[0]
			mb.mu.RLock()
			defer mb.mu.RUnlock()
			return mb.last.seq
		}

		// Actual testing here.

		loadMsgs(500)
		restartFS(ttl + 100*time.Millisecond)
		checkState(0, 501, 500)
		// We actually hold onto the last one now to remember our starting sequence.
		checkNumBlks(1)

		// Now check partial expires and the fss tracking state.
		// Small numbers is to keep them in one block.
		fs = newFS()
		loadMsgs(10)
		time.Sleep(100 * time.Millisecond)
		loadMsgs(10)
		checkFiltered("orders.*", SimpleState{Msgs: 20, First: 1, Last: 20})

		restartFS(ttl - 100*time.Millisecond + 25*time.Millisecond) // Just want half
		checkState(10, 11, 20)
		checkNumBlks(1)
		checkFiltered("orders.*", SimpleState{Msgs: 10, First: 11, Last: 20})
		checkFiltered("orders.5", SimpleState{Msgs: 1, First: 15, Last: 15})
		checkBlkState(0)

		fs = newFS()
		loadMsgs(5)
		time.Sleep(100 * time.Millisecond)
		loadMsgs(15)
		restartFS(ttl - 100*time.Millisecond + 25*time.Millisecond) // Just want half
		checkState(15, 6, 20)
		checkFiltered("orders.*", SimpleState{Msgs: 15, First: 6, Last: 20})
		checkFiltered("orders.5", SimpleState{Msgs: 2, First: 10, Last: 20})

		// Now we want to test that if the end of a msg block is all deletes msgs that we do the right thing.
		fs = newFS()
		loadMsgs(150)
		time.Sleep(100 * time.Millisecond)
		loadMsgs(100)

		checkNumBlks(5)

		// Now delete 10 messages from the end of the first block which we will expire on restart.
		// We will expire up to seq 100, so delete 91-100.
		lseq := lastSeqForBlk()
		for seq := lseq; seq > lseq-10; seq-- {
			removed, err := fs.RemoveMsg(seq)
			if err != nil || !removed {
				t.Fatalf("Error removing message: %v", err)
			}
		}
		restartFS(ttl - 100*time.Millisecond + 25*time.Millisecond) // Just want half
		checkState(100, 151, 250)
		checkNumBlks(3) // We should only have 3 blks left.
		checkBlkState(0)

		// Now make sure that we properly clean up any internal dmap entries (sparse) when expiring.
		fs = newFS()
		loadMsgs(10)
		// Remove some in sparse fashion, adding to dmap.
		fs.RemoveMsg(2)
		fs.RemoveMsg(4)
		fs.RemoveMsg(6)
		time.Sleep(100 * time.Millisecond)
		loadMsgs(10)
		restartFS(ttl - 100*time.Millisecond + 25*time.Millisecond) // Just want half
		checkState(10, 11, 20)
		checkNumBlks(1)
		checkBlkState(0)

		// Make sure expiring a block with tail deleted messages removes the message block etc.
		fs = newFS()
		loadMsgs(7)
		time.Sleep(100 * time.Millisecond)
		loadMsgs(3)
		fs.RemoveMsg(8)
		fs.RemoveMsg(9)
		fs.RemoveMsg(10)
		restartFS(ttl - 100*time.Millisecond + 25*time.Millisecond)
		checkState(0, 11, 10)

		fs.Stop()
		// Not for start per se but since we have all the test tooling here check that Compact() does right thing as well.
		fs = newFS()
		defer fs.Stop()
		loadMsgs(100)
		checkFiltered("orders.*", SimpleState{Msgs: 100, First: 1, Last: 100})
		checkFiltered("orders.5", SimpleState{Msgs: 10, First: 5, Last: 95})
		// Check that Compact keeps fss updated, does dmap etc.
		fs.Compact(51)
		checkFiltered("orders.*", SimpleState{Msgs: 50, First: 51, Last: 100})
		checkFiltered("orders.5", SimpleState{Msgs: 5, First: 55, Last: 95})
		checkBlkState(0)
	})
}

func TestFileStoreSparseCompaction(t *testing.T) {
	testFileStoreAllPermutations(t, func(t *testing.T, fcfg FileStoreConfig) {
		fcfg.BlockSize = 1024 * 1024
		cfg := StreamConfig{Name: "KV", Subjects: []string{"kv.>"}, Storage: FileStorage}

		fs, err := newFileStoreWithCreated(fcfg, cfg, time.Now(), prf(&fcfg), nil)
		require_NoError(t, err)
		defer fs.Stop()

		msg := bytes.Repeat([]byte("ABC"), 33) // ~100bytes
		loadMsgs := func(n int) {
			t.Helper()
			for i := 1; i <= n; i++ {
				if _, _, err := fs.StoreMsg(fmt.Sprintf("kv.%d", i%10), nil, msg, 0); err != nil {
					t.Fatalf("Unexpected error: %v", err)
				}
			}
		}

		checkState := func(msgs, first, last uint64) {
			t.Helper()
			if fs == nil {
				t.Fatalf("No fs")
				return
			}
			state := fs.State()
			if state.Msgs != msgs {
				t.Fatalf("Expected %d msgs, got %d", msgs, state.Msgs)
			}
			if state.FirstSeq != first {
				t.Fatalf("Expected %d as first, got %d", first, state.FirstSeq)
			}
			if state.LastSeq != last {
				t.Fatalf("Expected %d as last, got %d", last, state.LastSeq)
			}
		}

		deleteMsgs := func(seqs ...uint64) {
			t.Helper()
			for _, seq := range seqs {
				removed, err := fs.RemoveMsg(seq)
				if err != nil || !removed {
					t.Fatalf("Got an error on remove of %d: %v", seq, err)
				}
			}
		}

		eraseMsgs := func(seqs ...uint64) {
			t.Helper()
			for _, seq := range seqs {
				removed, err := fs.EraseMsg(seq)
				if err != nil || !removed {
					t.Fatalf("Got an error on erase of %d: %v", seq, err)
				}
			}
		}

		compact := func() {
			t.Helper()
			var ssb, ssa StreamState
			fs.FastState(&ssb)
			tb, ub, _ := fs.Utilization()

			fs.mu.RLock()
			if len(fs.blks) < 2 {
				t.Fatalf("Not enough blocks?")
			}
			mb := fs.blks[0]
			fs.mu.RUnlock()

			mb.mu.Lock()
			err = mb.compact()
			mb.mu.Unlock()
			require_NoError(t, err)

			fs.FastState(&ssa)
			if !reflect.DeepEqual(ssb, ssa) {
				t.Fatalf("States do not match\n; %+v \nvs %+v", ssb, ssa)
			}
			ta, ua, _ := fs.Utilization()
			if ub != ua {
				t.Fatalf("Expected used to be the same, got %d vs %d", ub, ua)
			}
			// When using both encryption and compression, we're not always
			// guaranteed to have a smaller file after compaction.
			if ta >= tb && (fcfg.Cipher == NoCipher || fcfg.Compression == NoCompression) {
				t.Fatalf("Expected total after to be less then before, got %d vs %d", tb, ta)
			}
		}

		// Actual testing here.
		loadMsgs(1000)
		checkState(1000, 1, 1000)

		// Create a new lmb, since we'll compact the current one and that's not allowed on lmb.
		fs.mu.RLock()
		blks := len(fs.blks)
		fs.mu.RUnlock()
		require_Len(t, blks, 1)
		state := fs.State()
		_, err = fs.newMsgBlockForWrite()
		require_NoError(t, err)
		require_NoError(t, fs.writeTombstone(state.LastSeq, state.LastTime.UnixNano()))

		// Now delete a few messages.
		deleteMsgs(1)
		compact()

		deleteMsgs(1000, 999, 998, 997)
		compact()

		eraseMsgs(500, 502, 504, 506, 508, 510)
		compact()
	})
}

func TestFileStoreSparseCompactionWithInteriorDeletes(t *testing.T) {
	testFileStoreAllPermutations(t, func(t *testing.T, fcfg FileStoreConfig) {
		cfg := StreamConfig{Name: "KV", Subjects: []string{"kv.>"}, Storage: FileStorage}
		fs, err := newFileStoreWithCreated(fcfg, cfg, time.Now(), prf(&fcfg), nil)
		require_NoError(t, err)
		defer fs.Stop()

		for i := 1; i <= 1000; i++ {
			if _, _, err := fs.StoreMsg(fmt.Sprintf("kv.%d", i%10), nil, []byte("OK"), 0); err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}
		}

		// Now do interior deletes.
		for _, seq := range []uint64{500, 600, 700, 800} {
			removed, err := fs.RemoveMsg(seq)
			if err != nil || !removed {
				t.Fatalf("Got an error on remove of %d: %v", seq, err)
			}
		}

		_, err = fs.LoadMsg(900, nil)
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}

		// Do compact by hand, make sure we can still access msgs past the interior deletes.
		fs.mu.RLock()
		lmb := fs.lmb
		if err = lmb.dirtyCloseWithRemove(false); err != nil {
			fs.mu.RUnlock()
			require_NoError(t, err)
		}
		if err = lmb.compact(); err != nil {
			fs.mu.RUnlock()
			require_NoError(t, err)
		}
		fs.mu.RUnlock()

		if _, err = fs.LoadMsg(900, nil); err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
	})
}

// When messages span multiple blocks and we want to purge but keep some amount, say 1, we would remove all.
// This is because we would not break out of iterator across more message blocks.
// Issue #2622
func TestFileStorePurgeExKeepOneBug(t *testing.T) {
	testFileStoreAllPermutations(t, func(t *testing.T, fcfg FileStoreConfig) {
		fcfg.BlockSize = 128
		cfg := StreamConfig{Name: "zzz", Subjects: []string{"*"}, Storage: FileStorage}
		fs, err := newFileStoreWithCreated(fcfg, cfg, time.Now(), prf(&fcfg), nil)
		require_NoError(t, err)
		defer fs.Stop()

		fill := bytes.Repeat([]byte("X"), 128)

		fs.StoreMsg("A", nil, []byte("META"), 0)
		fs.StoreMsg("B", nil, fill, 0)
		fs.StoreMsg("A", nil, []byte("META"), 0)
		fs.StoreMsg("B", nil, fill, 0)

		fss, err := fs.FilteredState(1, "A")
		require_NoError(t, err)
		if fss.Msgs != 2 {
			t.Fatalf("Expected to find 2 `A` msgs, got %d", fss.Msgs)
		}

		n, err := fs.PurgeEx("A", 0, 1)
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		if n != 1 {
			t.Fatalf("Expected PurgeEx to remove 1 `A` msgs, got %d", n)
		}
		fss, err = fs.FilteredState(1, "A")
		require_NoError(t, err)
		if fss.Msgs != 1 {
			t.Fatalf("Expected to find 1 `A` msgs, got %d", fss.Msgs)
		}
	})
}

func TestFileStoreFilteredPendingBug(t *testing.T) {
	testFileStoreAllPermutations(t, func(t *testing.T, fcfg FileStoreConfig) {
		cfg := StreamConfig{Name: "zzz", Subjects: []string{"*"}, Storage: FileStorage}
		fs, err := newFileStoreWithCreated(fcfg, cfg, time.Now(), prf(&fcfg), nil)
		require_NoError(t, err)
		defer fs.Stop()

		_, _, err = fs.StoreMsg("foo", nil, []byte("msg"), 0)
		require_NoError(t, err)
		_, _, err = fs.StoreMsg("bar", nil, []byte("msg"), 0)
		require_NoError(t, err)
		_, _, err = fs.StoreMsg("baz", nil, []byte("msg"), 0)
		require_NoError(t, err)

		fs.mu.Lock()
		mb := fs.lmb
		fs.mu.Unlock()

		total, f, l, err := mb.filteredPending("foo", false, 3)
		require_NoError(t, err)
		if total != 0 {
			t.Fatalf("Expected total of 0 but got %d", total)
		}
		if f != 0 || l != 0 {
			t.Fatalf("Expected first and last to be 0 as well, but got %d %d", f, l)
		}
	})
}

// Test to optimize the selectMsgBlock with lots of blocks.
func TestFileStoreFetchPerf(t *testing.T) {
	// Comment out to run.
	t.SkipNow()

	testFileStoreAllPermutations(t, func(t *testing.T, fcfg FileStoreConfig) {
		fcfg.BlockSize = 8192
		fcfg.AsyncFlush = true
		cfg := StreamConfig{Name: "zzz", Subjects: []string{"*"}, Storage: FileStorage}
		fs, err := newFileStoreWithCreated(fcfg, cfg, time.Now(), prf(&fcfg), nil)
		require_NoError(t, err)
		defer fs.Stop()

		// Will create 25k msg blocks.
		n, subj, msg := 100_000, "zzz", bytes.Repeat([]byte("ABC"), 600)
		for i := 0; i < n; i++ {
			if _, _, err := fs.StoreMsg(subj, nil, msg, 0); err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}
		}

		// Time how long it takes us to load all messages.
		var smv StoreMsg
		now := time.Now()
		for i := 0; i < n; i++ {
			_, err := fs.LoadMsg(uint64(i), &smv)
			if err != nil {
				t.Fatalf("Unexpected error looking up seq %d: %v", i, err)
			}
		}
		fmt.Printf("Elapsed to load all messages is %v\n", time.Since(now))
	})
}

// For things like raft log when we compact and have a message block that could reclaim > 50% of space for block we want to do that.
// https://github.com/nats-io/nats-server/issues/2936
func TestFileStoreCompactReclaimHeadSpace(t *testing.T) {
	testFileStoreAllPermutations(t, func(t *testing.T, fcfg FileStoreConfig) {
		fcfg.BlockSize = 4 * 1024 * 1024
		cfg := StreamConfig{Name: "zzz", Subjects: []string{"*"}, Storage: FileStorage}
		created := time.Now()
		fs, err := newFileStoreWithCreated(fcfg, cfg, created, prf(&fcfg), nil)
		require_NoError(t, err)
		defer fs.Stop()

		// Create random bytes for payload to test for corruption vs repeated.
		msg := make([]byte, 64*1024)
		crand.Read(msg)

		// This gives us ~63 msgs in first and ~37 in second.
		n, subj := 100, "z"
		for i := 0; i < n; i++ {
			_, _, err := fs.StoreMsg(subj, nil, msg, 0)
			require_NoError(t, err)
		}

		checkNumBlocks := func(n int) {
			t.Helper()
			fs.mu.RLock()
			defer fs.mu.RUnlock()
			if len(fs.blks) != n {
				t.Fatalf("Expected to have %d blocks, got %d", n, len(fs.blks))
			}
		}

		getBlock := func(index int) *msgBlock {
			t.Helper()
			fs.mu.RLock()
			defer fs.mu.RUnlock()
			return fs.blks[index]
		}

		// Check that we did right thing and actually reclaimed since > 50%
		checkBlock := func(mb *msgBlock) {
			t.Helper()

			mb.mu.RLock()
			rbytes, mfn := mb.rbytes, mb.mfn
			fseq, lseq := mb.first.seq, mb.last.seq
			mb.mu.RUnlock()

			// Check that sizes match as long as we are not doing compression.
			if fcfg.Compression == NoCompression {
				file, err := os.Open(mfn)
				require_NoError(t, err)
				defer file.Close()
				fi, err := file.Stat()
				require_NoError(t, err)
				if rbytes != uint64(fi.Size()) {
					t.Fatalf("Expected to rbytes == fi.Size, got %d vs %d", rbytes, fi.Size())
				}
			}

			// Make sure we can pull messages and that they are ok.
			var smv StoreMsg
			sm, err := fs.LoadMsg(fseq, &smv)
			require_NoError(t, err)
			if !bytes.Equal(sm.msg, msg) {
				t.Fatalf("Msgs don't match, original %q vs %q", msg, sm.msg)
			}
			sm, err = fs.LoadMsg(lseq, &smv)
			require_NoError(t, err)
			if !bytes.Equal(sm.msg, msg) {
				t.Fatalf("Msgs don't match, original %q vs %q", msg, sm.msg)
			}
		}

		checkNumBlocks(2)
		_, err = fs.Compact(33)
		require_NoError(t, err)

		checkNumBlocks(2)
		checkBlock(getBlock(0))
		checkBlock(getBlock(1))

		_, err = fs.Compact(85)
		require_NoError(t, err)

		checkNumBlocks(1)
		checkBlock(getBlock(0))

		// Make sure we can write.
		_, _, err = fs.StoreMsg(subj, nil, msg, 0)
		require_NoError(t, err)

		checkNumBlocks(1)
		checkBlock(getBlock(0))

		// Stop and start again.
		fs.Stop()

		fs, err = newFileStoreWithCreated(fcfg, cfg, created, prf(&fcfg), nil)
		require_NoError(t, err)
		defer fs.Stop()

		checkNumBlocks(1)
		checkBlock(getBlock(0))

		// Make sure we can write.
		_, _, err = fs.StoreMsg(subj, nil, msg, 0)
		require_NoError(t, err)
	})
}

func TestFileStoreRememberLastMsgTime(t *testing.T) {
	testFileStoreAllPermutations(t, func(t *testing.T, fcfg FileStoreConfig) {
		var fs *fileStore
		cfg := StreamConfig{Name: "TEST", Storage: FileStorage, MaxAge: 1 * time.Second}

		getFS := func() *fileStore {
			t.Helper()
			fs, err := newFileStoreWithCreated(fcfg, cfg, time.Now(), prf(&fcfg), nil)
			require_NoError(t, err)
			return fs
		}
		restartFS := func() {
			t.Helper()
			fs.Stop()
			fs = getFS()
		}

		msg := bytes.Repeat([]byte("X"), 2*1024*1024)

		// Get first one.
		fs = getFS()
		defer fs.Stop()

		seq, ts, err := fs.StoreMsg("foo", nil, msg, 0)
		require_NoError(t, err)
		// We will test that last msg time survives from delete, purge and expires after restart.
		removed, err := fs.RemoveMsg(seq)
		require_NoError(t, err)
		require_True(t, removed)

		lt := time.Unix(0, ts).UTC()
		require_True(t, lt == fs.State().LastTime)

		// Restart
		restartFS()

		// Test that last time survived.
		require_True(t, lt == fs.State().LastTime)

		seq, ts, err = fs.StoreMsg("foo", nil, msg, 0)
		require_NoError(t, err)

		var smv StoreMsg
		_, err = fs.LoadMsg(seq, &smv)
		require_NoError(t, err)

		fs.Purge()

		// Restart
		restartFS()

		lt = time.Unix(0, ts).UTC()
		require_True(t, lt == fs.State().LastTime)

		_, _, err = fs.StoreMsg("foo", nil, msg, 0)
		require_NoError(t, err)
		seq, ts, err = fs.StoreMsg("foo", nil, msg, 0)
		require_NoError(t, err)

		require_True(t, seq == 4)

		// Wait til messages expire.
		checkFor(t, 5*time.Second, time.Second, func() error {
			state := fs.State()
			if state.Msgs == 0 {
				return nil
			}
			return fmt.Errorf("Still has %d msgs", state.Msgs)
		})

		// Restart
		restartFS()

		lt = time.Unix(0, ts).UTC()
		require_True(t, lt == fs.State().LastTime)

		// Now make sure we retain the true last seq.
		_, _, err = fs.StoreMsg("foo", nil, msg, 0)
		require_NoError(t, err)
		seq, ts, err = fs.StoreMsg("foo", nil, msg, 0)
		require_NoError(t, err)

		require_True(t, seq == 6)
		removed, err = fs.RemoveMsg(seq)
		require_NoError(t, err)
		require_True(t, removed)

		removed, err = fs.RemoveMsg(seq - 1)
		require_NoError(t, err)
		require_True(t, removed)

		// Restart
		restartFS()

		lt = time.Unix(0, ts).UTC()
		require_True(t, lt == fs.State().LastTime)
		require_True(t, seq == 6)
	})
}

func (fs *fileStore) getFirstBlock() *msgBlock {
	fs.mu.RLock()
	defer fs.mu.RUnlock()
	if len(fs.blks) == 0 {
		return nil
	}
	return fs.blks[0]
}

func TestFileStoreRebuildStateDmapAccountingBug(t *testing.T) {
	testFileStoreAllPermutations(t, func(t *testing.T, fcfg FileStoreConfig) {
		fcfg.BlockSize = 1024 * 1024

		fs, err := newFileStoreWithCreated(fcfg, StreamConfig{Name: "zzz", Storage: FileStorage}, time.Now(), prf(&fcfg), nil)
		require_NoError(t, err)
		defer fs.Stop()

		for i := 0; i < 100; i++ {
			_, _, err = fs.StoreMsg("foo", nil, nil, 0)
			require_NoError(t, err)
		}
		// Delete 2-40.
		for i := 2; i <= 40; i++ {
			_, err := fs.RemoveMsg(uint64(i))
			require_NoError(t, err)
		}

		mb := fs.getFirstBlock()
		require_True(t, mb != nil)

		check := func() {
			t.Helper()
			mb.mu.RLock()
			defer mb.mu.RUnlock()
			dmapLen := uint64(mb.dmap.Size())
			if mb.msgs != (mb.last.seq-mb.first.seq+1)-dmapLen {
				t.Fatalf("Consistency check failed: %d != %d -> last %d first %d len(dmap) %d",
					mb.msgs, (mb.last.seq-mb.first.seq+1)-dmapLen, mb.last.seq, mb.first.seq, dmapLen)
			}
		}

		check()

		mb.mu.Lock()
		err = mb.compact()
		mb.mu.Unlock()
		require_NoError(t, err)

		// Now delete first.
		_, err = fs.RemoveMsg(1)
		require_NoError(t, err)

		mb.mu.Lock()
		_, _, err = mb.rebuildStateLocked()
		mb.mu.Unlock()
		require_NoError(t, err)

		check()
	})
}

func TestFileStorePurgeExWithSubject(t *testing.T) {
	testFileStoreAllPermutations(t, func(t *testing.T, fcfg FileStoreConfig) {
		fcfg.BlockSize = 1000
		cfg := StreamConfig{Name: "TEST", Subjects: []string{"foo.>"}, Storage: FileStorage}
		created := time.Now()
		fs, err := newFileStoreWithCreated(fcfg, cfg, created, prf(&fcfg), nil)
		require_NoError(t, err)
		defer fs.Stop()

		payload := make([]byte, 20)

		_, _, err = fs.StoreMsg("foo.0", nil, payload, 0)
		require_NoError(t, err)

		total := 200
		for i := 0; i < total; i++ {
			_, _, err = fs.StoreMsg("foo.1", nil, payload, 0)
			require_NoError(t, err)
		}
		_, _, err = fs.StoreMsg("foo.2", nil, []byte("xxxxxx"), 0)
		require_NoError(t, err)

		// Make sure we have our state file prior to Purge call.
		require_NoError(t, fs.forceWriteFullState())

		// Capture the current index.db file.
		sfile := filepath.Join(fcfg.StoreDir, msgDir, streamStreamStateFile)
		buf, err := os.ReadFile(sfile)
		require_NoError(t, err)
		require_True(t, len(buf) > 0)

		// This should purge all "foo.1"
		p, err := fs.PurgeEx("foo.1", 0, 0)
		require_NoError(t, err)
		require_Equal(t, p, uint64(total))

		state := fs.State()
		require_Equal(t, state.Msgs, 2)
		require_Equal(t, state.FirstSeq, 1)

		// Make sure we can recover same state.
		require_NoError(t, fs.Stop())
		fs, err = newFileStoreWithCreated(fcfg, cfg, created, prf(&fcfg), nil)
		require_NoError(t, err)
		defer fs.Stop()

		before := state
		if state := fs.State(); !reflect.DeepEqual(state, before) {
			t.Fatalf("Expected state of %+v, got %+v", before, state)
		}

		// Also make sure we can recover properly with no index.db present.
		// We want to make sure we preserve any tombstones from the subject based purge.
		require_NoError(t, fs.Stop())
		require_NoError(t, os.Remove(filepath.Join(fs.fcfg.StoreDir, msgDir, streamStreamStateFile)))

		fs, err = newFileStoreWithCreated(fcfg, cfg, created, prf(&fcfg), nil)
		require_NoError(t, err)
		defer fs.Stop()

		if state := fs.State(); !reflect.DeepEqual(state, before) {
			t.Fatalf("Expected state of\n %+v, got\n %+v without index.db state", before, state)
		}

		// If we had an index.db from after PurgeEx but before Stop() would rewrite, make sure we
		// properly can recover with the old index file. This would be a crash after the PurgeEx() call.
		require_NoError(t, fs.Stop())
		err = os.WriteFile(sfile, buf, defaultFilePerms)
		require_NoError(t, err)

		fs, err = newFileStoreWithCreated(fcfg, cfg, created, prf(&fcfg), nil)
		require_NoError(t, err)
		defer fs.Stop()

		if state := fs.State(); !reflect.DeepEqual(state, before) {
			t.Fatalf("Expected state of %+v, got %+v with old index.db state", before, state)
		}
	})
}

func TestFileStorePurgeExNoTombsOnBlockRemoval(t *testing.T) {
	testFileStoreAllPermutations(t, func(t *testing.T, fcfg FileStoreConfig) {
		fcfg.BlockSize = 1000
		cfg := StreamConfig{Name: "TEST", Subjects: []string{"foo.>"}, Storage: FileStorage}
		created := time.Now()
		fs, err := newFileStoreWithCreated(fcfg, cfg, created, prf(&fcfg), nil)
		require_NoError(t, err)
		defer fs.Stop()

		payload := make([]byte, 20)

		total := 100
		for i := 0; i < total; i++ {
			_, _, err = fs.StoreMsg("foo.1", nil, payload, 0)
			require_NoError(t, err)
		}
		_, _, err = fs.StoreMsg("foo.2", nil, payload, 0)
		require_NoError(t, err)

		require_Equal(t, fs.numMsgBlocks(), 6)

		// Make sure we have our state file prior to Purge call.
		fs.forceWriteFullState()

		// Capture the current index.db file if it exists.
		sfile := filepath.Join(fcfg.StoreDir, msgDir, streamStreamStateFile)
		buf, err := os.ReadFile(sfile)
		require_NoError(t, err)
		require_True(t, len(buf) > 0)

		// This should purge all "foo.1". This will remove the blocks so we want to make sure
		// we do not write excessive tombstones here.
		p, err := fs.PurgeEx("foo.1", 0, 0)
		require_NoError(t, err)
		require_Equal(t, p, uint64(total))

		state := fs.State()
		require_Equal(t, state.Msgs, 1)
		require_Equal(t, state.FirstSeq, 101)

		// Check that we only have 1 msg block.
		require_Equal(t, fs.numMsgBlocks(), 1)

		// Put the old index.db back. We want to make sure without the empty block tombstones that we
		// properly recover state.
		fs.Stop()
		err = os.WriteFile(sfile, buf, defaultFilePerms)
		require_NoError(t, err)

		fs, err = newFileStoreWithCreated(fcfg, cfg, created, prf(&fcfg), nil)
		require_NoError(t, err)
		defer fs.Stop()

		state = fs.State()
		require_Equal(t, state.Msgs, 1)
		require_Equal(t, state.FirstSeq, 101)
	})
}

// When the N.idx file is shorter than the previous write we could fail to recover the idx properly.
// For instance, with encryption and an expiring stream that has no messages, when a restart happens the decrypt will fail
// since their are extra bytes, and this could lead to a stream sequence reset to zero.
//
// NOTE: We do not use idx files anymore, but keeping test.
func TestFileStoreShortIndexWriteBug(t *testing.T) {
	testFileStoreAllPermutations(t, func(t *testing.T, fcfg FileStoreConfig) {
		// Encrypted mode shows, but could effect non-encrypted mode.
		cfg := StreamConfig{Name: "TEST", Storage: FileStorage, MaxAge: time.Second}
		created := time.Now()
		fs, err := newFileStoreWithCreated(fcfg, cfg, created, prf(&fcfg), nil)
		require_NoError(t, err)
		defer fs.Stop()

		for i := 0; i < 100; i++ {
			_, _, err = fs.StoreMsg("foo", nil, nil, 0)
			require_NoError(t, err)
		}
		// Wait til messages all go away.
		checkFor(t, 5*time.Second, 200*time.Millisecond, func() error {
			if state := fs.State(); state.Msgs != 0 {
				return fmt.Errorf("Expected no msgs, got %d", state.Msgs)
			}
			return nil
		})

		if state := fs.State(); state.FirstSeq != 101 {
			t.Fatalf("Expected first sequence of 101 vs %d", state.FirstSeq)
		}

		// Now restart..
		fs.Stop()
		fs, err = newFileStoreWithCreated(fcfg, cfg, created, prf(&fcfg), nil)
		require_NoError(t, err)
		defer fs.Stop()

		if state := fs.State(); state.FirstSeq != 101 || state.LastSeq != 100 {
			t.Fatalf("Expected first sequence of 101 vs %d", state.FirstSeq)
		}
	})
}

func TestFileStoreDoubleCompactWithWriteInBetweenEncryptedBug(t *testing.T) {
	testFileStoreAllPermutations(t, func(t *testing.T, fcfg FileStoreConfig) {
		fs, err := newFileStoreWithCreated(fcfg, StreamConfig{Name: "zzz", Storage: FileStorage}, time.Now(), prf(&fcfg), nil)
		require_NoError(t, err)
		defer fs.Stop()

		subj, msg := "foo", []byte("ouch")
		for i := 0; i < 5; i++ {
			fs.StoreMsg(subj, nil, msg, 0)
		}
		_, err = fs.Compact(5)
		require_NoError(t, err)

		if state := fs.State(); state.LastSeq != 5 {
			t.Fatalf("Expected last sequence to be 5 but got %d", state.LastSeq)
		}
		for i := 0; i < 5; i++ {
			fs.StoreMsg(subj, nil, msg, 0)
		}
		_, err = fs.Compact(10)
		require_NoError(t, err)

		if state := fs.State(); state.LastSeq != 10 {
			t.Fatalf("Expected last sequence to be 10 but got %d", state.LastSeq)
		}
	})
}

// When we kept the empty block for tracking sequence, we needed to reset the bek
// counter when encrypted for subsequent writes to be correct. The bek in place could
// possibly still have a non-zero counter from previous writes.
// Happens when all messages expire and the are flushed and then subsequent writes occur.
func TestFileStoreEncryptedKeepIndexNeedBekResetBug(t *testing.T) {
	testFileStoreAllPermutations(t, func(t *testing.T, fcfg FileStoreConfig) {
		ttl := 1 * time.Second
		cfg := StreamConfig{Name: "zzz", Storage: FileStorage, MaxAge: ttl}
		fs, err := newFileStoreWithCreated(fcfg, cfg, time.Now(), prf(&fcfg), nil)
		require_NoError(t, err)
		defer fs.Stop()

		subj, msg := "foo", []byte("ouch")
		for i := 0; i < 5; i++ {
			fs.StoreMsg(subj, nil, msg, 0)
		}

		// Want to go to 0.
		// This will leave the marker.
		checkFor(t, 5*time.Second, ttl, func() error {
			if state := fs.State(); state.Msgs != 0 {
				return fmt.Errorf("Expected no msgs, got %d", state.Msgs)
			}
			return nil
		})

		// Now write additional messages.
		for i := 0; i < 5; i++ {
			fs.StoreMsg(subj, nil, msg, 0)
		}

		// Make sure the buffer is cleared.
		fs.mu.RLock()
		mb := fs.lmb
		fs.mu.RUnlock()
		mb.mu.Lock()
		mb.clearCacheAndOffset()
		mb.mu.Unlock()

		// Now make sure we can read.
		var smv StoreMsg
		_, err = fs.LoadMsg(10, &smv)
		require_NoError(t, err)
	})
}

func (fs *fileStore) reportMeta() (hasPSIM, hasAnyFSS bool) {
	fs.mu.RLock()
	defer fs.mu.RUnlock()

	hasPSIM = fs.psim != nil
	for _, mb := range fs.blks {
		mb.mu.RLock()
		hasAnyFSS = hasAnyFSS || mb.fss != nil
		mb.mu.RUnlock()
		if hasAnyFSS {
			break
		}
	}
	return hasPSIM, hasAnyFSS
}

func TestFileStoreExpireSubjectMeta(t *testing.T) {
	testFileStoreAllPermutations(t, func(t *testing.T, fcfg FileStoreConfig) {
		fcfg.BlockSize = 1024
		fcfg.CacheExpire = 500 * time.Millisecond
		fcfg.SubjectStateExpire = time.Second
		cfg := StreamConfig{Name: "zzz", Subjects: []string{"kv.>"}, Storage: FileStorage, MaxMsgsPer: 1}
		fs, err := newFileStoreWithCreated(fcfg, cfg, time.Now(), prf(&fcfg), nil)
		require_NoError(t, err)
		defer fs.Stop()

		ns := 100
		for i := 1; i <= ns; i++ {
			subj := fmt.Sprintf("kv.%d", i)
			_, _, err := fs.StoreMsg(subj, nil, []byte("value"), 0)
			require_NoError(t, err)
		}

		// Test that on restart we do not have extensize metadata but do have correct number of subjects/keys.
		// Only thing really needed for store state / stream info.
		fs.Stop()
		fs, err = newFileStoreWithCreated(fcfg, cfg, time.Now(), prf(&fcfg), nil)
		require_NoError(t, err)
		defer fs.Stop()

		var ss StreamState
		fs.FastState(&ss)
		if ss.NumSubjects != ns {
			t.Fatalf("Expected NumSubjects of %d, got %d", ns, ss.NumSubjects)
		}

		// Make sure we clear mb fss meta
		checkFor(t, fcfg.SubjectStateExpire*2, 500*time.Millisecond, func() error {
			if _, hasAnyFSS := fs.reportMeta(); hasAnyFSS {
				return fmt.Errorf("Still have mb fss state")
			}
			return nil
		})

		// LoadLast, which is what KV uses, should load meta and succeed.
		_, err = fs.LoadLastMsg("kv.22", nil)
		require_NoError(t, err)
		// Make sure we clear mb fss meta
		checkFor(t, fcfg.SubjectStateExpire*2, 500*time.Millisecond, func() error {
			if _, hasAnyFSS := fs.reportMeta(); hasAnyFSS {
				return fmt.Errorf("Still have mb fss state")
			}
			return nil
		})
	})
}

func TestFileStoreMaxMsgsPerSubject(t *testing.T) {
	testFileStoreAllPermutations(t, func(t *testing.T, fcfg FileStoreConfig) {
		fcfg.BlockSize = 128
		fcfg.CacheExpire = time.Second
		cfg := StreamConfig{Name: "zzz", Subjects: []string{"kv.>"}, Storage: FileStorage, MaxMsgsPer: 1}
		fs, err := newFileStoreWithCreated(fcfg, cfg, time.Now(), prf(&fcfg), nil)
		require_NoError(t, err)
		defer fs.Stop()

		ns := 100
		for i := 1; i <= ns; i++ {
			subj := fmt.Sprintf("kv.%d", i)
			_, _, err := fs.StoreMsg(subj, nil, []byte("value"), 0)
			require_NoError(t, err)
		}

		for i := 1; i <= ns; i++ {
			subj := fmt.Sprintf("kv.%d", i)
			_, _, err := fs.StoreMsg(subj, nil, []byte("value"), 0)
			require_NoError(t, err)
		}

		if state := fs.State(); state.Msgs != 100 || state.FirstSeq != 101 || state.LastSeq != 200 || len(state.Deleted) != 0 {
			t.Fatalf("Bad state: %+v", state)
		}

		if nb := fs.numMsgBlocks(); nb != 34 {
			t.Fatalf("Expected 34 blocks, got %d", nb)
		}
	})
}

// Testing the case in https://github.com/nats-io/nats-server/issues/4247
func TestFileStoreMaxMsgsAndMaxMsgsPerSubject(t *testing.T) {
	testFileStoreAllPermutations(t, func(t *testing.T, fcfg FileStoreConfig) {
		fcfg.BlockSize = 128
		fcfg.CacheExpire = time.Second
		cfg := StreamConfig{
			Name:     "zzz",
			Subjects: []string{"kv.>"},
			Storage:  FileStorage,
			Discard:  DiscardNew, MaxMsgs: 100, // Total stream policy
			DiscardNewPer: true, MaxMsgsPer: 1, // Per-subject policy
		}
		fs, err := newFileStoreWithCreated(fcfg, cfg, time.Now(), prf(&fcfg), nil)
		require_NoError(t, err)
		defer fs.Stop()

		for i := 1; i <= 101; i++ {
			subj := fmt.Sprintf("kv.%d", i)
			_, _, err := fs.StoreMsg(subj, nil, []byte("value"), 0)
			if i == 101 {
				// The 101th iteration should fail because MaxMsgs is set to
				// 100 and the policy is DiscardNew.
				require_Error(t, err)
			} else {
				require_NoError(t, err)
			}
		}

		for i := 1; i <= 100; i++ {
			subj := fmt.Sprintf("kv.%d", i)
			_, _, err := fs.StoreMsg(subj, nil, []byte("value"), 0)
			// All of these iterations should fail because MaxMsgsPer is set
			// to 1 and DiscardNewPer is set to true, forcing us to reject
			// cases where there is already a message on this subject.
			require_Error(t, err)
		}

		if state := fs.State(); state.Msgs != 100 || state.FirstSeq != 1 || state.LastSeq != 100 || len(state.Deleted) != 0 {
			// There should be 100 messages exactly, as the 101st subject
			// should have been rejected in the first loop, and any duplicates
			// on the other subjects should have been rejected in the second loop.
			t.Fatalf("Bad state: %+v", state)
		}
	})
}

func TestFileStoreSubjectStateCacheExpiration(t *testing.T) {
	testFileStoreAllPermutations(t, func(t *testing.T, fcfg FileStoreConfig) {
		fcfg.BlockSize = 32
		fcfg.CacheExpire = time.Second
		fcfg.SubjectStateExpire = time.Second
		cfg := StreamConfig{Name: "zzz", Subjects: []string{"kv.>"}, Storage: FileStorage, MaxMsgsPer: 2}
		fs, err := newFileStoreWithCreated(fcfg, cfg, time.Now(), prf(&fcfg), nil)
		require_NoError(t, err)
		defer fs.Stop()

		for i := 1; i <= 100; i++ {
			subj := fmt.Sprintf("kv.foo.%d", i)
			_, _, err := fs.StoreMsg(subj, nil, []byte("value"), 0)
			require_NoError(t, err)
		}
		for i := 1; i <= 100; i++ {
			subj := fmt.Sprintf("kv.bar.%d", i)
			_, _, err := fs.StoreMsg(subj, nil, []byte("value"), 0)
			require_NoError(t, err)
		}

		// Make sure we clear mb fss meta before asking for SubjectState.
		checkFor(t, 10*time.Second, 500*time.Millisecond, func() error {
			if _, hasAnyFSS := fs.reportMeta(); hasAnyFSS {
				return fmt.Errorf("Still have mb fss state")
			}
			return nil
		})

		if fss := fs.SubjectsState("kv.bar.>"); len(fss) != 100 {
			t.Fatalf("Expected 100 entries but got %d", len(fss))
		}

		fss := fs.SubjectsState("kv.bar.99")
		if len(fss) != 1 {
			t.Fatalf("Expected 1 entry but got %d", len(fss))
		}
		expected := SimpleState{Msgs: 1, First: 199, Last: 199}
		if ss := fss["kv.bar.99"]; ss != expected {
			t.Fatalf("Bad subject state, expected %+v but got %+v", expected, ss)
		}

		// Now add one to end and check as well for non-wildcard.
		_, _, err = fs.StoreMsg("kv.foo.1", nil, []byte("value22"), 0)
		require_NoError(t, err)

		if state := fs.State(); state.Msgs != 201 {
			t.Fatalf("Expected 201 msgs but got %+v", state)
		}

		fss = fs.SubjectsState("kv.foo.1")
		if len(fss) != 1 {
			t.Fatalf("Expected 1 entry but got %d", len(fss))
		}
		expected = SimpleState{Msgs: 2, First: 1, Last: 201}
		if ss := fss["kv.foo.1"]; ss != expected {
			t.Fatalf("Bad subject state, expected %+v but got %+v", expected, ss)
		}
	})
}

func TestFileStoreEncrypted(t *testing.T) {
	testFileStoreAllPermutations(t, func(t *testing.T, fcfg FileStoreConfig) {
		created := time.Now()
		fs, err := newFileStoreWithCreated(fcfg, StreamConfig{Name: "zzz", Storage: FileStorage}, created, prf(&fcfg), nil)
		require_NoError(t, err)
		defer fs.Stop()

		subj, msg := "foo", []byte("aes ftw")
		for i := 0; i < 50; i++ {
			fs.StoreMsg(subj, nil, msg, 0)
		}

		o, err := fs.ConsumerStore("o22", time.Time{}, &ConsumerConfig{})
		require_NoError(t, err)

		state := &ConsumerState{}
		state.Delivered.Consumer = 22
		state.Delivered.Stream = 22
		state.AckFloor.Consumer = 11
		state.AckFloor.Stream = 11
		err = o.Update(state)
		require_NoError(t, err)

		o.Stop()
		fs.Stop()
		fs, err = newFileStoreWithCreated(fcfg, StreamConfig{Name: "zzz", Storage: FileStorage}, created, prf(&fcfg), nil)
		require_NoError(t, err)
		defer fs.Stop()

		// Now make sure we can read.
		var smv StoreMsg
		sm, err := fs.LoadMsg(10, &smv)
		require_NoError(t, err)
		require_True(t, string(sm.msg) == "aes ftw")

		o, err = fs.ConsumerStore("o22", time.Time{}, &ConsumerConfig{})
		require_NoError(t, err)
		rstate, err := o.State()
		require_NoError(t, err)

		if rstate.Delivered != state.Delivered || rstate.AckFloor != state.AckFloor {
			t.Fatalf("Bad recovered consumer state, expected %+v got %+v", state, rstate)
		}
	})
}

// Make sure we do not go through block loads when we know no subjects will exists, e.g. raft.
func TestFileStoreNoFSSWhenNoSubjects(t *testing.T) {
	testFileStoreAllPermutations(t, func(t *testing.T, fcfg FileStoreConfig) {
		created := time.Now()
		fs, err := newFileStoreWithCreated(fcfg, StreamConfig{Name: "zzz", Storage: FileStorage}, created, prf(&fcfg), nil)
		require_NoError(t, err)
		defer fs.Stop()

		n, msg := 100, []byte("raft state")
		for i := 0; i < n; i++ {
			_, _, err := fs.StoreMsg(_EMPTY_, nil, msg, 0)
			require_NoError(t, err)
		}

		state := fs.State()
		require_True(t, state.Msgs == uint64(n))

		fs.Stop()
		fs, err = newFileStoreWithCreated(fcfg, StreamConfig{Name: "zzz", Storage: FileStorage}, created, prf(&fcfg), nil)
		require_NoError(t, err)
		defer fs.Stop()

		// Make sure we did not load the block trying to generate fss.
		fs.mu.RLock()
		mb := fs.blks[0]
		fs.mu.RUnlock()

		mb.mu.Lock()
		defer mb.mu.Unlock()

		if mb.cloads > 0 {
			t.Fatalf("Expected no cache loads but got %d", mb.cloads)
		}
		if mb.fss != nil {
			t.Fatalf("Expected fss to be nil")
		}
	})
}

func TestFileStoreNoFSSBugAfterRemoveFirst(t *testing.T) {
	testFileStoreAllPermutations(t, func(t *testing.T, fcfg FileStoreConfig) {
		fcfg.BlockSize = 8 * 1024 * 1024
		fcfg.CacheExpire = 200 * time.Millisecond
		cfg := StreamConfig{Name: "zzz", Subjects: []string{"foo.bar.*"}, Storage: FileStorage}
		fs, err := newFileStoreWithCreated(fcfg, cfg, time.Now(), prf(&fcfg), nil)
		require_NoError(t, err)
		defer fs.Stop()

		n, msg := 100, bytes.Repeat([]byte("ZZZ"), 33) // ~100bytes
		for i := 0; i < n; i++ {
			subj := fmt.Sprintf("foo.bar.%d", i)
			_, _, err := fs.StoreMsg(subj, nil, msg, 0)
			require_NoError(t, err)
		}

		state := fs.State()
		require_True(t, state.Msgs == uint64(n))

		// Let fss expire.
		time.Sleep(250 * time.Millisecond)

		_, err = fs.RemoveMsg(1)
		require_NoError(t, err)

		sm, _, err := fs.LoadNextMsg("foo.>", true, 1, nil)
		require_NoError(t, err)
		require_True(t, sm.subj == "foo.bar.1")

		// Make sure mb.fss does not have the entry for foo.bar.0
		fs.mu.Lock()
		mb := fs.blks[0]
		fs.mu.Unlock()
		mb.mu.RLock()
		ss, ok := mb.fss.Find([]byte("foo.bar.0"))
		mb.mu.RUnlock()

		if ok && ss != nil {
			t.Fatalf("Expected no state for %q, but got %+v\n", "foo.bar.0", ss)
		}
	})
}

// NOTE: We do not use fss files anymore, but leaving test in place.
func TestFileStoreNoFSSAfterRecover(t *testing.T) {
	testFileStoreAllPermutations(t, func(t *testing.T, fcfg FileStoreConfig) {
		cfg := StreamConfig{Name: "zzz", Subjects: []string{"foo"}, Storage: FileStorage}
		created := time.Now()
		fs, err := newFileStoreWithCreated(fcfg, cfg, created, prf(&fcfg), nil)
		require_NoError(t, err)
		defer fs.Stop()

		n, msg := 100, []byte("no fss for you!")
		for i := 0; i < n; i++ {
			_, _, err := fs.StoreMsg(_EMPTY_, nil, msg, 0)
			require_NoError(t, err)
		}

		state := fs.State()
		require_True(t, state.Msgs == uint64(n))

		fs.Stop()
		fs, err = newFileStoreWithCreated(fcfg, cfg, created, prf(&fcfg), nil)
		require_NoError(t, err)
		defer fs.Stop()

		// Make sure we did not load the block trying to generate fss.
		fs.mu.RLock()
		mb := fs.blks[0]
		fs.mu.RUnlock()

		mb.mu.Lock()
		defer mb.mu.Unlock()

		if mb.fss != nil {
			t.Fatalf("Expected no fss post recover")
		}
	})
}

func TestFileStoreFSSCloseAndKeepOnExpireOnRecoverBug(t *testing.T) {
	testFileStoreAllPermutations(t, func(t *testing.T, fcfg FileStoreConfig) {
		ttl := 100 * time.Millisecond
		cfg := StreamConfig{Name: "zzz", Subjects: []string{"foo"}, Storage: FileStorage, MaxAge: ttl}
		created := time.Now()
		fs, err := newFileStoreWithCreated(fcfg, cfg, created, prf(&fcfg), nil)
		require_NoError(t, err)
		defer fs.Stop()

		_, _, err = fs.StoreMsg("foo", nil, nil, 0)
		require_NoError(t, err)

		fs.Stop()
		time.Sleep(2 * ttl)
		fs, err = newFileStoreWithCreated(fcfg, cfg, created, prf(&fcfg), nil)
		require_NoError(t, err)
		defer fs.Stop()

		if state := fs.State(); state.NumSubjects != 0 {
			t.Fatalf("Expected no subjects with no messages, got %d", state.NumSubjects)
		}
	})
}

func TestFileStoreExpireOnRecoverSubjectAccounting(t *testing.T) {
	const msgLen = 19
	msg := bytes.Repeat([]byte("A"), msgLen)

	testFileStoreAllPermutations(t, func(t *testing.T, fcfg FileStoreConfig) {
		fcfg.BlockSize = 100
		ttl := 400 * time.Millisecond
		cfg := StreamConfig{Name: "zzz", Subjects: []string{"*"}, Storage: FileStorage, MaxAge: ttl}
		created := time.Now()
		fs, err := newFileStoreWithCreated(fcfg, cfg, created, prf(&fcfg), nil)
		require_NoError(t, err)
		defer fs.Stop()

		// These are in first block.
		fs.StoreMsg("A", nil, msg, 0)
		fs.StoreMsg("B", nil, msg, 0)
		stored := time.Now()
		// Leave a large margin so C doesn't expire as well if the restart is slow.
		time.Sleep(ttl * 3 / 4)
		// This one in 2nd block.
		fs.StoreMsg("C", nil, msg, 0)

		fs.Stop()
		time.Sleep(time.Until(stored.Add(ttl + 10*time.Millisecond)))
		fs, err = newFileStoreWithCreated(fcfg, cfg, created, prf(&fcfg), nil)
		require_NoError(t, err)
		defer fs.Stop()

		// Make sure we take into account PSIM when throwing a whole block away.
		if state := fs.State(); state.NumSubjects != 1 {
			t.Fatalf("Expected 1 subject, got %d", state.NumSubjects)
		}
	})
}

func TestFileStoreFSSExpireNumPendingBug(t *testing.T) {
	testFileStoreAllPermutations(t, func(t *testing.T, fcfg FileStoreConfig) {
		cexp := 100 * time.Millisecond
		fcfg.CacheExpire = cexp
		cfg := StreamConfig{Name: "zzz", Subjects: []string{"KV.>"}, MaxMsgsPer: 1, Storage: FileStorage}
		fs, err := newFileStoreWithCreated(fcfg, cfg, time.Now(), prf(&fcfg), nil)
		require_NoError(t, err)
		defer fs.Stop()

		// Let FSS meta expire.
		time.Sleep(2 * cexp)

		_, _, err = fs.StoreMsg("KV.X", nil, []byte("Y"), 0)
		require_NoError(t, err)

		fss, err := fs.FilteredState(1, "KV.X")
		require_NoError(t, err)
		if fss.Msgs != 1 {
			t.Fatalf("Expected only 1 msg, got %d", fss.Msgs)
		}
	})
}

// https://github.com/nats-io/nats-server/issues/3484
func TestFileStoreFilteredFirstMatchingBug(t *testing.T) {
	testFileStoreAllPermutations(t, func(t *testing.T, fcfg FileStoreConfig) {
		cfg := StreamConfig{Name: "zzz", Subjects: []string{"foo.>"}, Storage: FileStorage}
		fs, err := newFileStoreWithCreated(fcfg, cfg, time.Now(), prf(&fcfg), nil)
		require_NoError(t, err)
		defer fs.Stop()

		_, _, err = fs.StoreMsg("foo.foo", nil, []byte("A"), 0)
		require_NoError(t, err)

		_, _, err = fs.StoreMsg("foo.foo", nil, []byte("B"), 0)
		require_NoError(t, err)

		_, _, err = fs.StoreMsg("foo.foo", nil, []byte("C"), 0)
		require_NoError(t, err)

		fs.mu.RLock()
		mb := fs.lmb
		fs.mu.RUnlock()

		mb.mu.Lock()
		// Simulate swapping out the fss state and reading it back in with only one subject
		// present in the block.
		if mb.fss != nil {
			mb.fss = nil
		}
		// Now load info back in.
		mb.generatePerSubjectInfo()
		mb.mu.Unlock()

		// Now add in a different subject.
		_, _, err = fs.StoreMsg("foo.bar", nil, []byte("X"), 0)
		require_NoError(t, err)

		// Now see if a filtered load would incorrectly succeed.
		sm, _, err := fs.LoadNextMsg("foo.foo", false, 4, nil)
		if err == nil || sm != nil {
			t.Fatalf("Loaded filtered message with wrong subject, wanted %q got %q", "foo.foo", sm.subj)
		}
	})
}

func TestFileStoreOutOfSpaceRebuildState(t *testing.T) {
	testFileStoreAllPermutations(t, func(t *testing.T, fcfg FileStoreConfig) {
		cfg := StreamConfig{Name: "zzz", Subjects: []string{"*"}, Storage: FileStorage}
		fs, err := newFileStoreWithCreated(fcfg, cfg, time.Now(), prf(&fcfg), nil)
		require_NoError(t, err)
		defer fs.Stop()

		_, _, err = fs.StoreMsg("foo", nil, []byte("A"), 0)
		require_NoError(t, err)

		_, _, err = fs.StoreMsg("bar", nil, []byte("B"), 0)
		require_NoError(t, err)

		// Grab state.
		state := fs.State()
		ss := fs.SubjectsState(">")

		// Set mock out of space error to trip.
		fs.mu.RLock()
		mb := fs.lmb
		fs.mu.RUnlock()

		mb.mu.Lock()
		mb.mockWriteErr = true
		mb.mu.Unlock()

		_, _, err = fs.StoreMsg("baz", nil, []byte("C"), 0)
		require_Error(t, err, errors.New("mock write error"))

		nstate := fs.State()
		nss := fs.SubjectsState(">")

		if !reflect.DeepEqual(state, nstate) {
			t.Fatalf("State expected to be\n  %+v\nvs\n  %+v", state, nstate)
		}

		if !reflect.DeepEqual(ss, nss) {
			t.Fatalf("Subject state expected to be\n  %+v\nvs\n  %+v", ss, nss)
		}
	})
}

func TestFileStoreRebuildStateProperlyWithMaxMsgsPerSubject(t *testing.T) {
	testFileStoreAllPermutations(t, func(t *testing.T, fcfg FileStoreConfig) {
		fcfg.BlockSize = 4096
		cfg := StreamConfig{Name: "zzz", Subjects: []string{"foo", "bar", "baz"}, Storage: FileStorage, MaxMsgsPer: 1}
		fs, err := newFileStoreWithCreated(fcfg, cfg, time.Now(), prf(&fcfg), nil)
		require_NoError(t, err)
		defer fs.Stop()

		// Send one to baz at beginning.
		_, _, err = fs.StoreMsg("baz", nil, nil, 0)
		require_NoError(t, err)

		ns := 1000
		for i := 1; i <= ns; i++ {
			_, _, err := fs.StoreMsg("foo", nil, nil, 0)
			require_NoError(t, err)
			_, _, err = fs.StoreMsg("bar", nil, nil, 0)
			require_NoError(t, err)
		}

		var ss StreamState
		fs.FastState(&ss)
		if ss.NumSubjects != 3 {
			t.Fatalf("Expected NumSubjects of 3, got %d", ss.NumSubjects)
		}
		if ss.Msgs != 3 {
			t.Fatalf("Expected NumMsgs of 3, got %d", ss.Msgs)
		}
	})
}

func TestFileStoreUpdateMaxMsgsPerSubject(t *testing.T) {
	cfg := StreamConfig{
		Name:       "TEST",
		Storage:    FileStorage,
		Subjects:   []string{"foo"},
		MaxMsgsPer: 10,
	}

	testFileStoreAllPermutations(t, func(t *testing.T, fcfg FileStoreConfig) {
		fs, err := newFileStoreWithCreated(fcfg, cfg, time.Now(), prf(&fcfg), nil)
		require_NoError(t, err)
		defer fs.Stop()

		// Make sure this is honored on an update.
		cfg.MaxMsgsPer = 50
		err = fs.UpdateConfig(&cfg)
		require_NoError(t, err)

		numStored := 22
		for i := 0; i < numStored; i++ {
			_, _, err = fs.StoreMsg("foo", nil, nil, 0)
			require_NoError(t, err)
		}

		ss := fs.SubjectsState("foo")["foo"]
		if ss.Msgs != uint64(numStored) {
			t.Fatalf("Expected to have %d stored, got %d", numStored, ss.Msgs)
		}

		// Now make sure we trunk if setting to lower value.
		cfg.MaxMsgsPer = 10
		err = fs.UpdateConfig(&cfg)
		require_NoError(t, err)

		ss = fs.SubjectsState("foo")["foo"]
		if ss.Msgs != 10 {
			t.Fatalf("Expected to have %d stored, got %d", 10, ss.Msgs)
		}
	})
}

func TestFileStoreBadFirstAndFailedExpireAfterRestart(t *testing.T) {
	testFileStoreAllPermutations(t, func(t *testing.T, fcfg FileStoreConfig) {
		fcfg.BlockSize = 256
		ttl := time.Second
		cfg := StreamConfig{Name: "zzz", Subjects: []string{"foo"}, Storage: FileStorage, MaxAge: ttl}
		fs, err := newFileStoreWithCreated(fcfg, cfg, time.Now(), prf(&fcfg), nil)
		require_NoError(t, err)
		defer fs.Stop()

		// With block size of 256 and subject and message below, seq 8 starts new block.
		// Will double check and fail test if not the case since test depends on this.
		subj, msg := "foo", []byte("ZZ")
		// These are all instant and will expire after 1 sec.
		start := time.Now()
		for i := 0; i < 7; i++ {
			_, _, err := fs.StoreMsg(subj, nil, msg, 0)
			require_NoError(t, err)
		}

		// Put two more after a delay.
		time.Sleep(1500 * time.Millisecond)
		seq, _, err := fs.StoreMsg(subj, nil, msg, 0)
		require_NoError(t, err)
		_, _, err = fs.StoreMsg(subj, nil, msg, 0)
		require_NoError(t, err)

		// Make sure that sequence 8 is first in second block, and break test if that is not true.
		fs.mu.RLock()
		lmb := fs.lmb
		fs.mu.RUnlock()
		lmb.mu.RLock()
		first := lmb.first.seq
		lmb.mu.RUnlock()
		require_True(t, first == 8)

		// Instantly remove first one from second block.
		// On restart this will trigger expire on recover which will set fs.FirstSeq to the deleted one.
		fs.RemoveMsg(seq)

		// Stop the filstore and wait til first block expires.
		fs.Stop()
		time.Sleep(ttl - time.Since(start) + (time.Second))
		fs, err = newFileStoreWithCreated(fcfg, cfg, time.Now(), prf(&fcfg), nil)
		require_NoError(t, err)
		defer fs.Stop()

		// Check that state is correct for first message which should be 9 and have a proper timestamp.
		var state StreamState
		fs.FastState(&state)
		ts := state.FirstTime
		require_True(t, state.Msgs == 1)
		require_True(t, state.FirstSeq == 9)
		require_True(t, !state.FirstTime.IsZero())

		// Wait and make sure expire timer is still working properly.
		time.Sleep(2 * ttl)
		fs.FastState(&state)
		require_Equal(t, state.Msgs, 0)
		require_Equal(t, state.FirstSeq, 10)
		require_Equal(t, state.LastSeq, 9)
		require_Equal(t, state.LastTime, ts)
	})
}

func TestFileStoreCompactAllWithDanglingLMB(t *testing.T) {
	testFileStoreAllPermutations(t, func(t *testing.T, fcfg FileStoreConfig) {
		cfg := StreamConfig{Name: "zzz", Subjects: []string{"foo"}, Storage: FileStorage}
		fs, err := newFileStoreWithCreated(fcfg, cfg, time.Now(), prf(&fcfg), nil)
		require_NoError(t, err)
		defer fs.Stop()

		subj, msg := "foo", []byte("ZZ")
		for i := 0; i < 100; i++ {
			_, _, err := fs.StoreMsg(subj, nil, msg, 0)
			require_NoError(t, err)
		}

		fs.RemoveMsg(100)
		purged, err := fs.Compact(100)
		require_NoError(t, err)
		require_True(t, purged == 99)

		_, _, err = fs.StoreMsg(subj, nil, msg, 0)
		require_NoError(t, err)
	})
}

func TestFileStoreStateWithBlkFirstDeleted(t *testing.T) {
	testFileStoreAllPermutations(t, func(t *testing.T, fcfg FileStoreConfig) {
		fcfg.BlockSize = 4096
		cfg := StreamConfig{Name: "zzz", Subjects: []string{"foo"}, Storage: FileStorage}
		fs, err := newFileStoreWithCreated(fcfg, cfg, time.Now(), prf(&fcfg), nil)
		require_NoError(t, err)
		defer fs.Stop()

		subj, msg := "foo", []byte("Hello World")
		toStore := 500
		for i := 0; i < toStore; i++ {
			_, _, err := fs.StoreMsg(subj, nil, msg, 0)
			require_NoError(t, err)
		}

		// Delete some messages from the beginning of an interior block.
		fs.mu.RLock()
		require_True(t, len(fs.blks) > 2)
		fseq := fs.blks[1].first.seq
		fs.mu.RUnlock()

		// Now start from first seq of second blk and delete 10 msgs
		for seq := fseq; seq < fseq+10; seq++ {
			removed, err := fs.RemoveMsg(seq)
			require_NoError(t, err)
			require_True(t, removed)
		}

		// This bug was in normal detailed state. But check fast state too.
		var fstate StreamState
		fs.FastState(&fstate)
		require_True(t, fstate.NumDeleted == 10)
		state := fs.State()
		require_True(t, state.NumDeleted == 10)
	})
}

func TestFileStoreMsgBlkFailOnKernelFaultLostDataReporting(t *testing.T) {
	testFileStoreAllPermutations(t, func(t *testing.T, fcfg FileStoreConfig) {
		fcfg.BlockSize = 4096
		cfg := StreamConfig{Name: "zzz", Subjects: []string{"foo"}, Storage: FileStorage}
		created := time.Now()
		fs, err := newFileStoreWithCreated(fcfg, cfg, created, prf(&fcfg), nil)
		require_NoError(t, err)
		defer fs.Stop()

		subj, msg := "foo", []byte("Hello World")
		toStore := 500
		for i := 0; i < toStore; i++ {
			_, _, err := fs.StoreMsg(subj, nil, msg, 0)
			require_NoError(t, err)
		}

		// We want to make sure all of the scenarios report lost data properly.
		// Will run 3 scenarios, 1st block, last block, interior block.
		// The new system does not detect byzantine behavior by default on creating the store.
		// A LoadMsg() of checkMsgs() call will be needed now.

		// First block
		fs.mu.RLock()
		require_True(t, len(fs.blks) > 0)
		mfn := fs.blks[0].mfn
		fs.mu.RUnlock()

		require_NoError(t, fs.Stop())
		require_NoError(t, os.Remove(mfn))

		// Restart.
		fs, err = newFileStoreWithCreated(fcfg, cfg, created, prf(&fcfg), nil)
		require_NoError(t, err)
		defer fs.Stop()

		_, err = fs.LoadMsg(1, nil)
		require_Error(t, err, ErrStoreMsgNotFound)

		// Load will rebuild fs itself async..
		checkFor(t, time.Second, 50*time.Millisecond, func() error {
			if state := fs.State(); state.Lost != nil {
				return nil
			}
			return errors.New("no ld yet")
		})

		state := fs.State()
		require_Equal(t, state.FirstSeq, 94)
		require_True(t, state.Lost != nil)
		require_Len(t, len(state.Lost.Msgs), 93)

		// Last block
		fs.mu.RLock()
		require_True(t, len(fs.blks) > 0)
		require_True(t, fs.lmb != nil)
		mfn = fs.lmb.mfn
		fs.mu.RUnlock()

		require_NoError(t, fs.Stop())
		require_NoError(t, os.Remove(mfn))

		// Restart.
		fs, err = newFileStoreWithCreated(fcfg, cfg, created, prf(&fcfg), nil)
		require_NoError(t, err)
		defer fs.Stop()

		state = fs.State()
		require_Equal(t, state.FirstSeq, 94)
		require_Equal(t, state.LastSeq, 500)   // Make sure we do not lose last seq.
		require_Equal(t, state.NumDeleted, 35) // These are interiors
		require_True(t, state.Lost != nil)
		require_Len(t, len(state.Lost.Msgs), 35)

		// Interior block.
		fs.mu.RLock()
		require_True(t, len(fs.blks) > 3)
		mfn = fs.blks[len(fs.blks)-3].mfn
		fs.mu.RUnlock()

		require_NoError(t, fs.Stop())
		require_NoError(t, os.Remove(mfn))

		// Restart.
		fs, err = newFileStoreWithCreated(fcfg, cfg, created, prf(&fcfg), nil)
		require_NoError(t, err)
		defer fs.Stop()

		// Need checkMsgs to catch interior one.
		ld, err := fs.checkMsgs()
		require_NoError(t, err)
		require_True(t, ld != nil)

		state = fs.State()
		require_Equal(t, state.FirstSeq, 94)
		require_Equal(t, state.LastSeq, 500) // Make sure we do not lose last seq.
		require_Equal(t, state.NumDeleted, 128)
		require_True(t, state.Lost != nil)
		require_Len(t, len(state.Lost.Msgs), 93)
	})
}

func TestFileStoreAllFilteredStateWithDeleted(t *testing.T) {
	testFileStoreAllPermutations(t, func(t *testing.T, fcfg FileStoreConfig) {
		fcfg.BlockSize = 1024
		cfg := StreamConfig{Name: "zzz", Subjects: []string{"foo"}, Storage: FileStorage}
		fs, err := newFileStoreWithCreated(fcfg, cfg, time.Now(), prf(&fcfg), nil)
		require_NoError(t, err)
		defer fs.Stop()

		subj, msg := "foo", []byte("Hello World")
		for i := 0; i < 100; i++ {
			_, _, err := fs.StoreMsg(subj, nil, msg, 0)
			require_NoError(t, err)
		}

		remove := func(seqs ...uint64) {
			for _, seq := range seqs {
				ok, err := fs.RemoveMsg(seq)
				require_NoError(t, err)
				require_True(t, ok)
			}
		}

		checkFilteredState := func(start, msgs, first, last int) {
			fss, err := fs.FilteredState(uint64(start), _EMPTY_)
			require_NoError(t, err)
			if fss.Msgs != uint64(msgs) {
				t.Fatalf("Expected %d msgs, got %d", msgs, fss.Msgs)
			}
			if fss.First != uint64(first) {
				t.Fatalf("Expected %d to be first, got %d", first, fss.First)
			}
			if fss.Last != uint64(last) {
				t.Fatalf("Expected %d to be last, got %d", last, fss.Last)
			}
		}

		checkFilteredState(1, 100, 1, 100)
		remove(2)
		checkFilteredState(2, 98, 3, 100)
		remove(3, 4, 5)
		checkFilteredState(2, 95, 6, 100)
		checkFilteredState(6, 95, 6, 100)
		remove(8, 10, 12, 14, 16, 18)
		checkFilteredState(7, 88, 7, 100)

		// Now check when purged that we return first and last sequences properly.
		fs.Purge()
		checkFilteredState(0, 0, 101, 100)
	})
}

func TestFileStoreStreamTruncateResetMultiBlock(t *testing.T) {
	testFileStoreAllPermutations(t, func(t *testing.T, fcfg FileStoreConfig) {
		fcfg.BlockSize = 128
		cfg := StreamConfig{Name: "zzz", Subjects: []string{"foo"}, Storage: FileStorage}
		fs, err := newFileStoreWithCreated(fcfg, cfg, time.Now(), prf(&fcfg), nil)
		require_NoError(t, err)
		defer fs.Stop()

		subj, msg := "foo", []byte("Hello World")
		for i := 0; i < 1000; i++ {
			_, _, err := fs.StoreMsg(subj, nil, msg, 0)
			require_NoError(t, err)
		}
		fs.syncBlocks()
		require_True(t, fs.numMsgBlocks() == 500)

		// Reset everything
		require_NoError(t, fs.Truncate(0))
		require_True(t, fs.numMsgBlocks() == 0)

		state := fs.State()
		require_Equal(t, state.Msgs, 0)
		require_Equal(t, state.Bytes, 0)
		require_Equal(t, state.FirstSeq, 0)
		require_Equal(t, state.LastSeq, 0)
		require_Equal(t, state.NumSubjects, 0)
		require_Equal(t, state.NumDeleted, 0)

		for i := 0; i < 1000; i++ {
			_, _, err := fs.StoreMsg(subj, nil, msg, 0)
			require_NoError(t, err)
		}
		fs.syncBlocks()

		state = fs.State()
		require_Equal(t, state.Msgs, 1000)
		require_Equal(t, state.Bytes, 44000)
		require_Equal(t, state.FirstSeq, 1)
		require_Equal(t, state.LastSeq, 1000)
		require_Equal(t, state.NumSubjects, 1)
		require_Equal(t, state.NumDeleted, 0)
	})
}

func TestFileStoreStreamCompactMultiBlockSubjectInfo(t *testing.T) {
	testFileStoreAllPermutations(t, func(t *testing.T, fcfg FileStoreConfig) {
		fcfg.BlockSize = 128
		cfg := StreamConfig{Name: "zzz", Subjects: []string{"foo.*"}, Storage: FileStorage}
		fs, err := newFileStoreWithCreated(fcfg, cfg, time.Now(), prf(&fcfg), nil)
		require_NoError(t, err)
		defer fs.Stop()

		for i := 0; i < 1000; i++ {
			subj := fmt.Sprintf("foo.%d", i)
			_, _, err := fs.StoreMsg(subj, nil, []byte("Hello World"), 0)
			require_NoError(t, err)
		}
		require_True(t, fs.numMsgBlocks() == 500)

		// Compact such that we know we throw blocks away from the beginning.
		deleted, err := fs.Compact(501)
		require_NoError(t, err)
		require_True(t, deleted == 500)
		require_True(t, fs.numMsgBlocks() == 250)

		// Make sure we adjusted for subjects etc.
		state := fs.State()
		require_True(t, state.NumSubjects == 500)
	})
}

func TestFileStoreSubjectsTotals(t *testing.T) {
	// No need for all permutations here.
	storeDir := t.TempDir()
	fcfg := FileStoreConfig{StoreDir: storeDir}
	fs, err := newFileStore(fcfg, StreamConfig{Name: "zzz", Subjects: []string{"*.*"}, Storage: FileStorage})
	require_NoError(t, err)
	defer fs.Stop()

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

		_, _, err := fs.StoreMsg(subj, nil, []byte("Hello World"), 0)
		require_NoError(t, err)
	}

	// Now test SubjectsTotal
	for dt, total := range fmap {
		subj := fmt.Sprintf("foo.%d", dt)
		m := fs.SubjectsTotals(subj)
		if m[subj] != uint64(total) {
			t.Fatalf("Expected %q to have %d total, got %d", subj, total, m[subj])
		}
	}

	// Check fmap.
	if st := fs.SubjectsTotals("foo.*"); len(st) != len(fmap) {
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
	if st := fs.SubjectsTotals("bar.*"); len(st) != len(bmap) {
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
	if st, expected := fs.SubjectsTotals("*.*"), len(bmap)+len(fmap); len(st) != expected {
		t.Fatalf("Expected %d subjects for %q, got %d", expected, "*.*", len(st))
	}
}

func TestFileStoreConsumerStoreEncodeAfterRestart(t *testing.T) {
	testFileStoreAllPermutations(t, func(t *testing.T, fcfg FileStoreConfig) {
		state := &ConsumerState{}

		func() { // for defers
			fs, err := newFileStoreWithCreated(fcfg, StreamConfig{Name: "zzz", Storage: FileStorage}, time.Now(), prf(&fcfg), nil)
			require_NoError(t, err)
			defer fs.Stop()

			o, err := fs.ConsumerStore("o22", time.Time{}, &ConsumerConfig{AckPolicy: AckExplicit})
			require_NoError(t, err)
			defer o.Stop()

			state.Delivered.Consumer = 22
			state.Delivered.Stream = 22
			state.AckFloor.Consumer = 11
			state.AckFloor.Stream = 11
			err = o.Update(state)
			require_NoError(t, err)
		}()

		func() { // for defers
			fs, err := newFileStoreWithCreated(fcfg, StreamConfig{Name: "zzz", Storage: FileStorage}, time.Now(), prf(&fcfg), nil)
			require_NoError(t, err)
			defer fs.Stop()

			o, err := fs.ConsumerStore("o22", time.Time{}, &ConsumerConfig{AckPolicy: AckExplicit})
			require_NoError(t, err)
			defer o.Stop()

			if o.(*consumerFileStore).state.Delivered != state.Delivered {
				t.Fatalf("Consumer state is wrong %+v vs %+v", o.(*consumerFileStore).state, state)
			}
			if o.(*consumerFileStore).state.AckFloor != state.AckFloor {
				t.Fatalf("Consumer state is wrong %+v vs %+v", o.(*consumerFileStore).state, state)
			}
		}()
	})
}

func TestFileStoreNumPendingLargeNumBlks(t *testing.T) {
	// No need for all permutations here.
	storeDir := t.TempDir()
	fcfg := FileStoreConfig{
		StoreDir:  storeDir,
		BlockSize: 128, // Small on purpose to create alot of blks.
	}
	fs, err := newFileStore(fcfg, StreamConfig{Name: "zzz", Subjects: []string{"zzz"}, Storage: FileStorage})
	require_NoError(t, err)
	defer fs.Stop()

	subj, msg := "zzz", bytes.Repeat([]byte("X"), 100)
	numMsgs := 10_000

	for i := 0; i < numMsgs; i++ {
		fs.StoreMsg(subj, nil, msg, 0)
	}

	start := time.Now()
	total, _, err := fs.NumPending(4000, "zzz", false)
	require_NoError(t, err)
	require_LessThan(t, time.Since(start), 15*time.Millisecond)
	require_Equal(t, total, 6001)

	start = time.Now()
	total, _, err = fs.NumPending(6000, "zzz", false)
	require_NoError(t, err)
	require_LessThan(t, time.Since(start), 25*time.Millisecond)
	require_Equal(t, total, 4001)

	// Now delete a message in first half and second half.
	fs.RemoveMsg(1000)
	fs.RemoveMsg(9000)

	start = time.Now()
	total, _, err = fs.NumPending(4000, "zzz", false)
	require_NoError(t, err)
	require_LessThan(t, time.Since(start), 50*time.Millisecond)
	require_Equal(t, total, 6000)

	start = time.Now()
	total, _, err = fs.NumPending(6000, "zzz", false)
	require_NoError(t, err)
	require_LessThan(t, time.Since(start), 50*time.Millisecond)
	require_Equal(t, total, 4000)
}

func TestFileStoreSkipMsgAndNumBlocks(t *testing.T) {
	// No need for all permutations here.
	storeDir := t.TempDir()
	fcfg := FileStoreConfig{
		StoreDir:  storeDir,
		BlockSize: 128, // Small on purpose to create alot of blks.
	}
	fs, err := newFileStore(fcfg, StreamConfig{Name: "zzz", Subjects: []string{"zzz"}, Storage: FileStorage})
	require_NoError(t, err)
	defer fs.Stop()

	subj, msg := "zzz", bytes.Repeat([]byte("X"), 100)

	_, _, err = fs.StoreMsg(subj, nil, msg, 0)
	require_NoError(t, err)
	require_NoError(t, fs.SkipMsgs(0, 10_000))
	_, _, err = fs.StoreMsg(subj, nil, msg, 0)
	require_NoError(t, err)
	require_Equal(t, fs.numMsgBlocks(), 3)
}

func TestFileStoreRestoreEncryptedWithNoKeyFuncFails(t *testing.T) {
	// No need for all permutations here.
	fcfg := FileStoreConfig{StoreDir: t.TempDir(), Cipher: AES}
	cfg := StreamConfig{Name: "zzz", Subjects: []string{"zzz"}, Storage: FileStorage}
	fs, err := newFileStoreWithCreated(fcfg, cfg, time.Now(), prf(&fcfg), nil)
	require_NoError(t, err)
	defer fs.Stop()

	subj, msg := "zzz", bytes.Repeat([]byte("X"), 100)
	numMsgs := 100
	for i := 0; i < numMsgs; i++ {
		fs.StoreMsg(subj, nil, msg, 0)
	}

	fs.Stop()

	// Make sure if we try to restore with no prf (key) that it fails.
	_, err = newFileStoreWithCreated(fcfg, cfg, time.Now(), nil, nil)
	require_Error(t, err, errNoMainKey)
}

func TestFileStoreInitialFirstSeq(t *testing.T) {
	testFileStoreAllPermutations(t, func(t *testing.T, fcfg FileStoreConfig) {
		cfg := StreamConfig{Name: "zzz", Storage: FileStorage, FirstSeq: 1000}
		fs, err := newFileStoreWithCreated(fcfg, cfg, time.Now(), prf(&fcfg), nil)
		require_NoError(t, err)
		defer fs.Stop()

		seq, _, err := fs.StoreMsg("A", nil, []byte("OK"), 0)
		require_NoError(t, err)
		if seq != 1000 {
			t.Fatalf("Message should have been sequence 1000 but was %d", seq)
		}

		seq, _, err = fs.StoreMsg("B", nil, []byte("OK"), 0)
		require_NoError(t, err)
		if seq != 1001 {
			t.Fatalf("Message should have been sequence 1001 but was %d", seq)
		}

		var state StreamState
		fs.FastState(&state)
		switch {
		case state.Msgs != 2:
			t.Fatalf("Expected 2 messages, got %d", state.Msgs)
		case state.FirstSeq != 1000:
			t.Fatalf("Expected first seq 1000, got %d", state.FirstSeq)
		case state.LastSeq != 1001:
			t.Fatalf("Expected last seq 1001, got %d", state.LastSeq)
		}
	})
}

func TestFileStoreRecaluclateFirstForSubjBug(t *testing.T) {
	fs, err := newFileStore(FileStoreConfig{StoreDir: t.TempDir()}, StreamConfig{Name: "zzz", Subjects: []string{"*"}, Storage: FileStorage})
	require_NoError(t, err)
	defer fs.Stop()

	fs.StoreMsg("foo", nil, nil, 0) // 1
	fs.StoreMsg("bar", nil, nil, 0) // 2
	fs.StoreMsg("foo", nil, nil, 0) // 3

	// Now remove first 2..
	fs.RemoveMsg(1)
	fs.RemoveMsg(2)

	// Now grab first (and only) block.
	fs.mu.RLock()
	mb := fs.blks[0]
	fs.mu.RUnlock()

	// Since we lazy update the first, simulate that we have not updated it as of yet.
	ss := &SimpleState{Msgs: 1, First: 1, Last: 3, firstNeedsUpdate: true}

	mb.mu.Lock()
	defer mb.mu.Unlock()

	// Flush the cache.
	mb.clearCacheAndOffset()
	// Now call with start sequence of 1, the old one
	// This will panic without the fix.
	mb.recalculateForSubj("foo", ss)
	// Make sure it was update properly.
	require_True(t, *ss == SimpleState{Msgs: 1, First: 3, Last: 3, firstNeedsUpdate: false})
}

func TestFileStoreKeepWithDeletedMsgsBug(t *testing.T) {
	fs, err := newFileStore(FileStoreConfig{StoreDir: t.TempDir()}, StreamConfig{Name: "zzz", Subjects: []string{"*"}, Storage: FileStorage})
	require_NoError(t, err)
	defer fs.Stop()

	msg := bytes.Repeat([]byte("A"), 19)
	for i := 0; i < 5; i++ {
		fs.StoreMsg("A", nil, msg, 0)
		fs.StoreMsg("B", nil, msg, 0)
	}

	n, err := fs.PurgeEx("A", 0, 0)
	require_NoError(t, err)
	require_True(t, n == 5)

	// Purge with keep.
	n, err = fs.PurgeEx(_EMPTY_, 0, 2)
	require_NoError(t, err)
	require_True(t, n == 3)
}

func TestFileStoreRestartWithExpireAndLockingBug(t *testing.T) {
	sd := t.TempDir()
	scfg := StreamConfig{Name: "zzz", Subjects: []string{"*"}, Storage: FileStorage}
	fs, err := newFileStore(FileStoreConfig{StoreDir: sd}, scfg)
	require_NoError(t, err)
	defer fs.Stop()

	// 20 total
	msg := []byte("HELLO WORLD")
	for i := 0; i < 10; i++ {
		fs.StoreMsg("A", nil, msg, 0)
		fs.StoreMsg("B", nil, msg, 0)
	}
	fs.Stop()

	// Now change config underneath of so we will do expires at startup.
	scfg.MaxMsgs = 15
	scfg.MaxMsgsPer = 2
	newCfg := FileStreamInfo{Created: fs.cfg.Created, StreamConfig: scfg}

	// Replace
	fs.cfg = newCfg
	require_NoError(t, fs.writeStreamMeta())

	fs, err = newFileStore(FileStoreConfig{StoreDir: sd}, scfg)
	require_NoError(t, err)
	defer fs.Stop()
}

// Test that loads from lmb under lots of writes do not return errPartialCache.
func TestFileStoreErrPartialLoad(t *testing.T) {
	fs, err := newFileStore(FileStoreConfig{StoreDir: t.TempDir()}, StreamConfig{Name: "zzz", Subjects: []string{"*"}, Storage: FileStorage})
	require_NoError(t, err)
	defer fs.Stop()

	put := func(num int) {
		for i := 0; i < num; i++ {
			fs.StoreMsg("Z", nil, []byte("ZZZZZZZZZZZZZ"), 0)
		}
	}

	put(100)

	// Dump cache of lmb.
	clearCache := func() {
		fs.mu.RLock()
		lmb := fs.lmb
		fs.mu.RUnlock()
		lmb.mu.Lock()
		lmb.clearCache()
		lmb.mu.Unlock()
	}
	clearCache()

	qch := make(chan struct{})
	defer close(qch)

	for i := 0; i < 10; i++ {
		go func() {
			for {
				select {
				case <-qch:
					return
				default:
					put(5)
				}
			}
		}()
	}

	time.Sleep(100 * time.Millisecond)

	var smv StoreMsg
	for i := 0; i < 10_000; i++ {
		fs.mu.RLock()
		lmb := fs.lmb
		fs.mu.RUnlock()
		lmb.mu.Lock()
		first, last := fs.lmb.first.seq, fs.lmb.last.seq
		if i%100 == 0 {
			lmb.clearCache()
		}
		lmb.mu.Unlock()

		if spread := int(last - first); spread > 0 {
			seq := first + uint64(rand.IntN(spread))
			_, err = fs.LoadMsg(seq, &smv)
			require_NoError(t, err)
		}
	}
}

func TestFileStoreErrPartialLoadOnSyncClose(t *testing.T) {
	fs, err := newFileStore(
		FileStoreConfig{StoreDir: t.TempDir(), BlockSize: 500},
		StreamConfig{Name: "zzz", Subjects: []string{"*"}, Storage: FileStorage},
	)
	require_NoError(t, err)
	defer fs.Stop()

	// This yields an internal record length of 50 bytes. So 10 msgs per blk.
	msgLen := 19
	msg := bytes.Repeat([]byte("A"), msgLen)

	// Load up half the block.
	for _, subj := range []string{"A", "B", "C", "D", "E"} {
		fs.StoreMsg(subj, nil, msg, 0)
	}

	// Now simulate the sync timer closing the last block.
	fs.mu.RLock()
	lmb := fs.lmb
	fs.mu.RUnlock()
	require_True(t, lmb != nil)

	lmb.mu.Lock()
	lmb.tryExpireCacheLocked()
	lmb.dirtyCloseWithRemove(false)
	lmb.mu.Unlock()

	fs.StoreMsg("Z", nil, msg, 0)
	_, err = fs.LoadMsg(1, nil)
	require_NoError(t, err)
}

func TestFileStoreSyncIntervals(t *testing.T) {
	fcfg := FileStoreConfig{StoreDir: t.TempDir(), SyncInterval: 250 * time.Millisecond}
	fs, err := newFileStore(fcfg, StreamConfig{Name: "zzz", Subjects: []string{"*"}, Storage: FileStorage})
	require_NoError(t, err)
	defer fs.Stop()

	checkSyncFlag := func(expected bool) {
		fs.mu.RLock()
		lmb := fs.lmb
		fs.mu.RUnlock()
		lmb.mu.RLock()
		syncNeeded := lmb.needSync
		lmb.mu.RUnlock()
		if syncNeeded != expected {
			t.Fatalf("Expected needSync to be %v", expected)
		}
	}

	checkSyncFlag(false)
	fs.StoreMsg("Z", nil, []byte("hello"), 0)
	checkSyncFlag(true)
	time.Sleep(400 * time.Millisecond)
	checkSyncFlag(false)
	fs.Stop()

	// Now check always
	fcfg.SyncInterval = 10 * time.Second
	fcfg.SyncAlways = true
	fs, err = newFileStore(fcfg, StreamConfig{Name: "zzz", Subjects: []string{"*"}, Storage: FileStorage})
	require_NoError(t, err)
	defer fs.Stop()

	checkSyncFlag(false)
	fs.StoreMsg("Z", nil, []byte("hello"), 0)
	checkSyncFlag(false)
}

// https://github.com/nats-io/nats-server/issues/4529
// Run this wuth --race and you will see the unlocked access that probably caused this.
func TestFileStoreRecalcFirstSequenceBug(t *testing.T) {
	fcfg := FileStoreConfig{StoreDir: t.TempDir()}
	fs, err := newFileStore(fcfg, StreamConfig{Name: "zzz", Subjects: []string{"*"}, MaxMsgsPer: 2, Storage: FileStorage})
	require_NoError(t, err)
	defer fs.Stop()

	msg := bytes.Repeat([]byte("A"), 22)

	for _, subj := range []string{"A", "A", "B", "B"} {
		fs.StoreMsg(subj, nil, msg, 0)
	}
	// Make sure the buffer is cleared.
	clearLMBCache := func() {
		fs.mu.RLock()
		mb := fs.lmb
		fs.mu.RUnlock()
		mb.mu.Lock()
		mb.clearCacheAndOffset()
		mb.mu.Unlock()
	}

	clearLMBCache()

	// Do first here.
	fs.StoreMsg("A", nil, msg, 0)

	var wg sync.WaitGroup
	start := make(chan bool)

	wg.Add(1)
	go func() {
		defer wg.Done()
		<-start
		for i := 0; i < 1_000; i++ {
			fs.LoadLastMsg("A", nil)
			clearLMBCache()
		}
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		<-start
		for i := 0; i < 1_000; i++ {
			fs.StoreMsg("A", nil, msg, 0)
		}
	}()

	close(start)
	wg.Wait()
}

///////////////////////////////////////////////////////////////////////////
// New WAL based architecture tests
///////////////////////////////////////////////////////////////////////////

func TestFileStoreFullStateBasics(t *testing.T) {
	testFileStoreAllPermutations(t, func(t *testing.T, fcfg FileStoreConfig) {
		fcfg.BlockSize = 100
		cfg := StreamConfig{Name: "zzz", Subjects: []string{"*"}, Storage: FileStorage}
		created := time.Now()
		fs, err := newFileStoreWithCreated(fcfg, cfg, created, prf(&fcfg), nil)
		require_NoError(t, err)
		defer fs.Stop()

		// This yields an internal record length of 50 bytes. So 2 msgs per blk.
		subj, msgLen, recLen := "A", 19, uint64(50)
		msgA := bytes.Repeat([]byte("A"), msgLen)
		msgZ := bytes.Repeat([]byte("Z"), msgLen)

		// Send 2 msgs and stop, check for presence of our full state file.
		_, _, err = fs.StoreMsg(subj, nil, msgA, 0)
		require_NoError(t, err)
		_, _, err = fs.StoreMsg(subj, nil, msgZ, 0)
		require_NoError(t, err)
		require_Equal(t, fs.numMsgBlocks(), 1)

		// Make sure there is a full state file after we do a stop.
		require_NoError(t, fs.Stop())

		sfile := filepath.Join(fcfg.StoreDir, msgDir, streamStreamStateFile)
		if _, err := os.Stat(sfile); err != nil {
			t.Fatalf("Expected stream state file but got %v", err)
		}

		// Read it in and make sure len > 0.
		buf, err := os.ReadFile(sfile)
		require_NoError(t, err)
		require_True(t, len(buf) > 0)

		// Now make sure we recover properly.
		fs, err = newFileStoreWithCreated(fcfg, cfg, created, prf(&fcfg), nil)
		require_NoError(t, err)
		defer fs.Stop()

		// Make sure there are no old idx or fss files.
		matches, err := filepath.Glob(filepath.Join(fcfg.StoreDir, msgDir, "%d.fss"))
		require_NoError(t, err)
		require_Equal(t, len(matches), 0)
		matches, err = filepath.Glob(filepath.Join(fcfg.StoreDir, msgDir, "%d.idx"))
		require_NoError(t, err)
		require_Equal(t, len(matches), 0)

		state := fs.State()
		require_Equal(t, state.Msgs, 2)
		require_Equal(t, state.FirstSeq, 1)
		require_Equal(t, state.LastSeq, 2)

		// Now make sure we can read in values.
		var smv StoreMsg
		sm, err := fs.LoadMsg(1, &smv)
		require_NoError(t, err)
		require_True(t, bytes.Equal(sm.msg, msgA))

		sm, err = fs.LoadMsg(2, &smv)
		require_NoError(t, err)
		require_True(t, bytes.Equal(sm.msg, msgZ))

		// Now add in 1 more here to split the lmb.
		_, _, err = fs.StoreMsg(subj, nil, msgZ, 0)
		require_NoError(t, err)

		// Now stop the filestore and replace the old stream state and make sure we recover correctly.
		require_NoError(t, fs.Stop())

		// Regrab the stream state
		buf, err = os.ReadFile(sfile)
		require_NoError(t, err)

		fs, err = newFileStoreWithCreated(fcfg, cfg, created, prf(&fcfg), nil)
		require_NoError(t, err)
		defer fs.Stop()

		// Add in one more.
		_, _, err = fs.StoreMsg(subj, nil, msgZ, 0)
		require_NoError(t, err)
		require_NoError(t, fs.Stop())

		// Put old stream state back with only 3.
		err = os.WriteFile(sfile, buf, defaultFilePerms)
		require_NoError(t, err)

		fs, err = newFileStoreWithCreated(fcfg, cfg, created, prf(&fcfg), nil)
		require_NoError(t, err)
		defer fs.Stop()

		state = fs.State()
		require_Equal(t, state.Msgs, 4)
		require_Equal(t, state.Bytes, 4*recLen)
		require_Equal(t, state.FirstSeq, 1)
		require_Equal(t, state.LastSeq, 4)
		require_Equal(t, fs.numMsgBlocks(), 2)

		// Make sure we are tracking subjects correctly.
		fs.mu.RLock()
		info, _ := fs.psim.Find(stringToBytes(subj))
		psi := *info
		fs.mu.RUnlock()

		require_Equal(t, psi.total, 4)
		require_Equal(t, psi.fblk, 1)
		require_Equal(t, psi.lblk, 2)

		// Store 1 more
		_, _, err = fs.StoreMsg(subj, nil, msgA, 0)
		require_NoError(t, err)
		require_NoError(t, fs.Stop())
		// Put old stream state back with only 3.
		err = os.WriteFile(sfile, buf, defaultFilePerms)
		require_NoError(t, err)

		fs, err = newFileStoreWithCreated(fcfg, cfg, created, prf(&fcfg), nil)
		require_NoError(t, err)
		defer fs.Stop()

		state = fs.State()
		require_Equal(t, state.Msgs, 5)
		require_Equal(t, state.FirstSeq, 1)
		require_Equal(t, state.LastSeq, 5)
		require_Equal(t, fs.numMsgBlocks(), 3)
		// Make sure we are tracking subjects correctly.
		fs.mu.RLock()
		info, _ = fs.psim.Find(stringToBytes(subj))
		psi = *info
		fs.mu.RUnlock()
		require_Equal(t, psi.total, 5)
		require_Equal(t, psi.fblk, 1)
		require_Equal(t, psi.lblk, 3)
	})
}

func TestFileStoreFullStatePurge(t *testing.T) {
	testFileStoreFullStatePurge(t, false)
}

func TestFileStoreFullStatePurgeFullRecovery(t *testing.T) {
	testFileStoreFullStatePurge(t, true)
}

func testFileStoreFullStatePurge(t *testing.T, checkFullRecovery bool) {
	testFileStoreAllPermutations(t, func(t *testing.T, fcfg FileStoreConfig) {
		fcfg.BlockSize = 132 // Leave room for tombstones.
		cfg := StreamConfig{Name: "zzz", Subjects: []string{"*"}, Storage: FileStorage}
		created := time.Now()
		fs, err := newFileStoreWithCreated(fcfg, cfg, created, prf(&fcfg), nil)
		require_NoError(t, err)
		defer fs.Stop()

		// This yields an internal record length of 50 bytes. So 2 msgs per blk.
		subj, msg := "A", bytes.Repeat([]byte("A"), 19)

		// Should be 2 per block, so 5 blocks.
		for i := 0; i < 10; i++ {
			_, _, err = fs.StoreMsg(subj, nil, msg, 0)
			require_NoError(t, err)
		}
		n, err := fs.Purge()
		require_NoError(t, err)
		require_Equal(t, n, 10)
		state := fs.State()
		require_NoError(t, fs.Stop())
		if checkFullRecovery {
			require_NoError(t, os.Remove(filepath.Join(fcfg.StoreDir, msgDir, streamStreamStateFile)))
		}

		fs, err = newFileStoreWithCreated(fcfg, cfg, created, prf(&fcfg), nil)
		require_NoError(t, err)
		defer fs.Stop()

		if newState := fs.State(); !reflect.DeepEqual(state, newState) {
			t.Fatalf("Restore state after purge does not match:\n%+v\n%+v",
				state, newState)
		}

		if checkFullRecovery {
			fs.rebuildState(nil)
			if newState := fs.State(); !reflect.DeepEqual(state, newState) {
				t.Fatalf("Restore state after purge does not match:\n%+v\n%+v",
					state, newState)
			}
		}

		// Add in more 10 more total, some B some C.
		for i := 0; i < 5; i++ {
			_, _, err = fs.StoreMsg("B", nil, msg, 0)
			require_NoError(t, err)
			_, _, err = fs.StoreMsg("C", nil, msg, 0)
			require_NoError(t, err)
		}

		n, err = fs.PurgeEx("B", 0, 0)
		require_NoError(t, err)
		require_Equal(t, n, 5)

		state = fs.State()
		require_NoError(t, fs.Stop())
		if checkFullRecovery {
			require_NoError(t, os.Remove(filepath.Join(fcfg.StoreDir, msgDir, streamStreamStateFile)))
		}

		fs, err = newFileStoreWithCreated(fcfg, cfg, created, prf(&fcfg), nil)
		require_NoError(t, err)
		defer fs.Stop()

		if newState := fs.State(); !reflect.DeepEqual(state, newState) {
			t.Fatalf("Restore state after purge does not match:\n%+v\n%+v",
				state, newState)
		}

		if checkFullRecovery {
			fs.rebuildState(nil)
			if newState := fs.State(); !reflect.DeepEqual(state, newState) {
				t.Fatalf("Restore state after purge does not match:\n%+v\n%+v",
					state, newState)
			}
		}

		// Purge with keep.
		n, err = fs.PurgeEx(_EMPTY_, 0, 2)
		require_NoError(t, err)
		require_Equal(t, n, 3)

		state = fs.State()

		// Do some quick checks here, keep had a bug.
		require_Equal(t, state.Msgs, 2)
		require_Equal(t, state.FirstSeq, 18)
		require_Equal(t, state.LastSeq, 20)

		require_NoError(t, fs.Stop())
		if checkFullRecovery {
			require_NoError(t, os.Remove(filepath.Join(fcfg.StoreDir, msgDir, streamStreamStateFile)))
		}

		fs, err = newFileStoreWithCreated(fcfg, cfg, created, prf(&fcfg), nil)
		require_NoError(t, err)
		defer fs.Stop()

		if newState := fs.State(); !reflect.DeepEqual(state, newState) {
			t.Fatalf("Restore state after purge does not match:\n%+v\n%+v",
				state, newState)
		}

		if checkFullRecovery {
			fs.rebuildState(nil)
			if newState := fs.State(); !reflect.DeepEqual(state, newState) {
				t.Fatalf("Restore state after purge does not match:\n%+v\n%+v",
					state, newState)
			}
		}

		// Make sure we can survive a purge with no full stream state and have the correct first sequence.
		// This used to be provided by the idx file and is now tombstones and the full stream state snapshot.
		n, err = fs.Purge()
		require_NoError(t, err)
		require_Equal(t, n, 2)
		state = fs.State()
		require_NoError(t, fs.Stop())
		if checkFullRecovery {
			require_NoError(t, os.Remove(filepath.Join(fcfg.StoreDir, msgDir, streamStreamStateFile)))
		}

		fs, err = newFileStoreWithCreated(fcfg, cfg, created, prf(&fcfg), nil)
		require_NoError(t, err)
		defer fs.Stop()

		if newState := fs.State(); !reflect.DeepEqual(state, newState) {
			t.Fatalf("Restore state after purge does not match:\n%+v\n%+v",
				state, newState)
		}

		if checkFullRecovery {
			fs.rebuildState(nil)
			if newState := fs.State(); !reflect.DeepEqual(state, newState) {
				t.Fatalf("Restore state after purge does not match:\n%+v\n%+v",
					state, newState)
			}
		}
	})
}

func TestFileStoreFullStateTestUserRemoveWAL(t *testing.T) {
	testFileStoreAllPermutations(t, func(t *testing.T, fcfg FileStoreConfig) {
		fcfg.BlockSize = 132 // Leave room for tombstones.
		cfg := StreamConfig{Name: "zzz", Subjects: []string{"*"}, Storage: FileStorage}
		created := time.Now()
		fs, err := newFileStoreWithCreated(fcfg, cfg, created, prf(&fcfg), nil)
		require_NoError(t, err)
		defer fs.Stop()

		// This yields an internal record length of 50 bytes. So 2 msgs per blk.
		msgLen := 19
		msgA := bytes.Repeat([]byte("A"), msgLen)
		msgZ := bytes.Repeat([]byte("Z"), msgLen)

		// Store 2 msgs and delete first.
		fs.StoreMsg("A", nil, msgA, 0)
		fs.StoreMsg("Z", nil, msgZ, 0)
		fs.RemoveMsg(1)

		// Check we can load things properly since the block will have a tombstone now for seq 1.
		sm, err := fs.LoadMsg(2, nil)
		require_NoError(t, err)
		require_True(t, bytes.Equal(sm.msg, msgZ))

		require_Equal(t, fs.numMsgBlocks(), 1)
		state := fs.State()
		fs.Stop()

		// Grab the state from this stop.
		sfile := filepath.Join(fcfg.StoreDir, msgDir, streamStreamStateFile)
		buf, err := os.ReadFile(sfile)
		require_NoError(t, err)

		fs, err = newFileStoreWithCreated(fcfg, cfg, created, prf(&fcfg), nil)
		require_NoError(t, err)
		defer fs.Stop()

		// Check we can load things properly since the block will have a tombstone now for seq 1.
		_, err = fs.LoadMsg(2, nil)
		require_NoError(t, err)
		_, err = fs.LoadMsg(1, nil)
		require_Error(t, err, ErrStoreMsgNotFound)

		if newState := fs.State(); !reflect.DeepEqual(state, newState) {
			t.Fatalf("Restore state does not match:\n%+v\n%+v",
				state, newState)
		}
		require_True(t, !state.FirstTime.IsZero())

		// Store 2 more msgs and delete 2 & 4.
		fs.StoreMsg("A", nil, msgA, 0)
		fs.StoreMsg("Z", nil, msgZ, 0)
		fs.RemoveMsg(2)
		fs.RemoveMsg(4)

		state = fs.State()
		require_Equal(t, len(state.Deleted), state.NumDeleted)
		fs.Stop()

		fs, err = newFileStoreWithCreated(fcfg, cfg, created, prf(&fcfg), nil)
		require_NoError(t, err)
		defer fs.Stop()

		if newState := fs.State(); !reflect.DeepEqual(state, newState) {
			t.Fatalf("Restore state does not match:\n%+v\n%+v",
				state, newState)
		}
		require_True(t, !state.FirstTime.IsZero())

		// Now close again and put back old stream state.
		// This will test that we can remember user deletes by placing tombstones in the lmb/wal.
		fs.Stop()
		err = os.WriteFile(sfile, buf, defaultFilePerms)
		require_NoError(t, err)

		fs, err = newFileStoreWithCreated(fcfg, cfg, created, prf(&fcfg), nil)
		require_NoError(t, err)
		defer fs.Stop()

		newState := fs.State()
		// We will properly detect lost data for sequence #2 here.
		require_True(t, newState.Lost != nil)
		require_Equal(t, len(newState.Lost.Msgs), 1)
		require_Equal(t, newState.Lost.Msgs[0], 2)
		// Clear for deep equal compare below.
		newState.Lost = nil

		if !reflect.DeepEqual(state, newState) {
			t.Fatalf("Restore state does not match:\n%+v\n%+v",
				state, newState)
		}
		require_True(t, !state.FirstTime.IsZero())
	})
}

func TestFileStoreFullStateTestSysRemovals(t *testing.T) {
	testFileStoreAllPermutations(t, func(t *testing.T, fcfg FileStoreConfig) {
		fcfg.BlockSize = 100
		cfg := StreamConfig{
			Name:       "zzz",
			Subjects:   []string{"*"},
			MaxMsgs:    10,
			MaxMsgsPer: 1,
			Storage:    FileStorage,
		}
		created := time.Now()
		fs, err := newFileStoreWithCreated(fcfg, cfg, created, prf(&fcfg), nil)
		require_NoError(t, err)
		defer fs.Stop()

		// This yields an internal record length of 50 bytes. So 2 msgs per blk.
		msgLen := 19
		msg := bytes.Repeat([]byte("A"), msgLen)

		for _, subj := range []string{"A", "B", "A", "B"} {
			fs.StoreMsg(subj, nil, msg, 0)
		}

		state := fs.State()
		require_Equal(t, state.Msgs, 2)
		require_Equal(t, state.FirstSeq, 3)
		require_Equal(t, state.LastSeq, 4)
		fs.Stop()

		fs, err = newFileStoreWithCreated(fcfg, cfg, created, prf(&fcfg), nil)
		require_NoError(t, err)
		defer fs.Stop()

		if newState := fs.State(); !reflect.DeepEqual(state, newState) {
			t.Fatalf("Restore state after purge does not match:\n%+v\n%+v",
				state, newState)
		}

		for _, subj := range []string{"C", "D", "E", "F", "G", "H", "I", "J"} {
			fs.StoreMsg(subj, nil, msg, 0)
		}

		state = fs.State()
		require_Equal(t, state.Msgs, 10)
		require_Equal(t, state.FirstSeq, 3)
		require_Equal(t, state.LastSeq, 12)
		fs.Stop()

		fs, err = newFileStoreWithCreated(fcfg, cfg, created, prf(&fcfg), nil)
		require_NoError(t, err)
		defer fs.Stop()

		if newState := fs.State(); !reflect.DeepEqual(state, newState) {
			t.Fatalf("Restore state after purge does not match:\n%+v\n%+v",
				state, newState)
		}

		// Goes over limit
		fs.StoreMsg("ZZZ", nil, msg, 0)

		state = fs.State()
		require_Equal(t, state.Msgs, 10)
		require_Equal(t, state.FirstSeq, 4)
		require_Equal(t, state.LastSeq, 13)
		fs.Stop()

		fs, err = newFileStoreWithCreated(fcfg, cfg, created, prf(&fcfg), nil)
		require_NoError(t, err)
		defer fs.Stop()

		if newState := fs.State(); !reflect.DeepEqual(state, newState) {
			t.Fatalf("Restore state after purge does not match:\n%+v\n%+v",
				state, newState)
		}
	})
}

func TestFileStoreSelectBlockWithFirstSeqRemovals(t *testing.T) {
	testFileStoreAllPermutations(t, func(t *testing.T, fcfg FileStoreConfig) {
		fcfg.BlockSize = 100
		cfg := StreamConfig{
			Name:       "zzz",
			Subjects:   []string{"*"},
			MaxMsgsPer: 1,
			Storage:    FileStorage,
		}
		fs, err := newFileStoreWithCreated(fcfg, cfg, time.Now(), prf(&fcfg), nil)
		require_NoError(t, err)
		defer fs.Stop()

		// This yields an internal record length of 50 bytes. So 2 msgs per blk.
		msgLen := 19
		msg := bytes.Repeat([]byte("A"), msgLen)

		subjects := "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz+@$^"
		// We need over 32 blocks to kick in binary search. So 32*2+1 (65) msgs to get 33 blocks.
		for i := 0; i < 32*2+1; i++ {
			subj := string(subjects[i])
			fs.StoreMsg(subj, nil, msg, 0)
		}
		require_Equal(t, fs.numMsgBlocks(), 33)

		// Now we want to delete the first msg of each block to move the first sequence.
		// Want to do this via system removes, not user initiated moves.
		for i := 0; i < len(subjects); i += 2 {
			subj := string(subjects[i])
			fs.StoreMsg(subj, nil, msg, 0)
		}

		var ss StreamState
		fs.FastState(&ss)

		// We want to make sure that select always returns an index and a non-nil mb.
		for seq := ss.FirstSeq; seq <= ss.LastSeq; seq++ {
			fs.mu.RLock()
			index, mb := fs.selectMsgBlockWithIndex(seq)
			fs.mu.RUnlock()
			require_True(t, index >= 0)
			require_True(t, mb != nil)
			require_Equal(t, (seq-1)/2, uint64(index))
		}
	})
}

func TestFileStoreMsgBlockHolesAndIndexing(t *testing.T) {
	fs, err := newFileStore(
		FileStoreConfig{StoreDir: t.TempDir()},
		StreamConfig{Name: "zzz", Subjects: []string{"*"}, Storage: FileStorage, MaxMsgsPer: 1},
	)
	require_NoError(t, err)
	defer fs.Stop()

	// Grab the message block by hand and manipulate at that level.
	mb := fs.getFirstBlock()
	writeMsg := func(subj string, seq uint64) {
		rl := fileStoreMsgSize(subj, nil, []byte(subj))
		require_NoError(t, mb.writeMsgRecord(rl, seq, subj, nil, []byte(subj), time.Now().UnixNano(), true))
		fs.rebuildState(nil)
	}
	readMsg := func(seq uint64, expectedSubj string) {
		// Clear cache so we load back in from disk and need to properly process any holes.
		ld, tombs, err := mb.rebuildState()
		require_NoError(t, err)
		require_Equal(t, ld, nil)
		require_Equal(t, len(tombs), 0)
		fs.rebuildState(nil)
		sm, _, err := mb.fetchMsg(seq, nil)
		require_NoError(t, err)
		require_Equal(t, sm.subj, expectedSubj)
		require_True(t, bytes.Equal(sm.buf[:len(expectedSubj)], []byte(expectedSubj)))
	}

	writeMsg("A", 2)
	require_Equal(t, mb.first.seq, 2)
	require_Equal(t, mb.last.seq, 2)

	writeMsg("B", 4)
	require_Equal(t, mb.first.seq, 2)
	require_Equal(t, mb.last.seq, 4)

	writeMsg("C", 12)

	readMsg(4, "B")
	require_True(t, mb.dmap.Exists(3))

	readMsg(12, "C")
	readMsg(2, "A")

	// Check that we get deleted for the right ones etc.
	checkDeleted := func(seq uint64) {
		_, _, err = mb.fetchMsg(seq, nil)
		require_Error(t, err, ErrStoreMsgNotFound, errDeletedMsg)
		mb.mu.RLock()
		shouldExist, exists := seq >= mb.first.seq, mb.dmap.Exists(seq)
		mb.mu.RUnlock()
		if shouldExist {
			require_True(t, exists)
		}
	}
	checkDeleted(1)
	checkDeleted(3)
	for seq := 5; seq < 12; seq++ {
		checkDeleted(uint64(seq))
	}
}

func TestFileStoreMsgBlockCompactionAndHoles(t *testing.T) {
	fs, err := newFileStore(
		FileStoreConfig{StoreDir: t.TempDir()},
		StreamConfig{Name: "zzz", Subjects: []string{"*"}, Storage: FileStorage, MaxMsgsPer: 1},
	)
	require_NoError(t, err)
	defer fs.Stop()

	msg := bytes.Repeat([]byte("Z"), 1024)
	for _, subj := range []string{"A", "B", "C", "D", "E", "F", "G", "H", "I", "J"} {
		fs.StoreMsg(subj, nil, msg, 0)
	}
	// Leave first one but delete the rest.
	for seq := uint64(2); seq < 10; seq++ {
		fs.RemoveMsg(seq)
	}
	require_Equal(t, fs.numMsgBlocks(), 1)
	mb := fs.getFirstBlock()
	require_NotNil(t, mb)

	_, ub, _ := fs.Utilization()

	// Do compaction, should remove all excess now.
	mb.mu.Lock()
	err = mb.compact()
	mb.mu.Unlock()
	require_NoError(t, err)

	ta, ua, _ := fs.Utilization()
	require_Equal(t, ub, ua)
	require_Equal(t, ta, ua)
}

func TestFileStoreRemoveLastNoDoubleTombstones(t *testing.T) {
	fs, err := newFileStore(
		FileStoreConfig{StoreDir: t.TempDir()},
		StreamConfig{Name: "zzz", Subjects: []string{"*"}, Storage: FileStorage, MaxMsgsPer: 1},
	)
	require_NoError(t, err)
	defer fs.Stop()

	fs.StoreMsg("A", nil, []byte("hello"), 0)
	fs.mu.Lock()
	fs.removeMsgViaLimits(1)
	fs.mu.Unlock()

	require_Equal(t, fs.numMsgBlocks(), 1)
	mb := fs.getFirstBlock()
	require_NotNil(t, mb)
	mb.loadMsgs()
	rbytes, _, err := fs.Utilization()
	require_NoError(t, err)
	require_Equal(t, rbytes, emptyRecordLen)
}

func TestFileStoreFullStateMultiBlockPastWAL(t *testing.T) {
	testFileStoreAllPermutations(t, func(t *testing.T, fcfg FileStoreConfig) {
		fcfg.BlockSize = 100
		cfg := StreamConfig{Name: "zzz", Subjects: []string{"*"}, Storage: FileStorage}
		created := time.Now()
		fs, err := newFileStoreWithCreated(fcfg, cfg, created, prf(&fcfg), nil)
		require_NoError(t, err)
		defer fs.Stop()

		// This yields an internal record length of 50 bytes. So 2 msgs per blk.
		msgLen := 19
		msgA := bytes.Repeat([]byte("A"), msgLen)
		msgZ := bytes.Repeat([]byte("Z"), msgLen)

		// Store 2 msgs
		fs.StoreMsg("A", nil, msgA, 0)
		fs.StoreMsg("B", nil, msgZ, 0)
		require_Equal(t, fs.numMsgBlocks(), 1)
		fs.Stop()

		// Grab the state from this stop.
		sfile := filepath.Join(fcfg.StoreDir, msgDir, streamStreamStateFile)
		buf, err := os.ReadFile(sfile)
		require_NoError(t, err)

		fs, err = newFileStoreWithCreated(fcfg, cfg, created, prf(&fcfg), nil)
		require_NoError(t, err)
		defer fs.Stop()

		// Store 6 more msgs.
		fs.StoreMsg("C", nil, msgA, 0)
		fs.StoreMsg("D", nil, msgZ, 0)
		fs.StoreMsg("E", nil, msgA, 0)
		fs.StoreMsg("F", nil, msgZ, 0)
		fs.StoreMsg("G", nil, msgA, 0)
		fs.StoreMsg("H", nil, msgZ, 0)
		require_Equal(t, fs.numMsgBlocks(), 4)
		state := fs.State()
		fs.Stop()

		// Put back old stream state.
		// This will test that we properly walk multiple blocks past where we snapshotted state.
		err = os.WriteFile(sfile, buf, defaultFilePerms)
		require_NoError(t, err)

		fs, err = newFileStoreWithCreated(fcfg, cfg, created, prf(&fcfg), nil)
		require_NoError(t, err)
		defer fs.Stop()

		if newState := fs.State(); !reflect.DeepEqual(state, newState) {
			t.Fatalf("Restore state does not match:\n%+v\n%+v",
				state, newState)
		}
		require_True(t, !state.FirstTime.IsZero())
	})
}

// This tests we can successfully recover without having to rebuild the whole stream from a mid block index.db marker
// when the updated block has a removed entry.
// Make sure this does not cause a recover of the full state.
func TestFileStoreFullStateMidBlockPastWAL(t *testing.T) {
	testFileStoreAllPermutations(t, func(t *testing.T, fcfg FileStoreConfig) {
		cfg := StreamConfig{Name: "zzz", Subjects: []string{"*"}, Storage: FileStorage, MaxMsgsPer: 1}
		created := time.Now()
		fs, err := newFileStoreWithCreated(fcfg, cfg, created, prf(&fcfg), nil)
		require_NoError(t, err)
		defer fs.Stop()

		// This yields an internal record length of 50 bytes. So 2 msgs per blk.
		msg := bytes.Repeat([]byte("Z"), 19)

		// Store 5 msgs
		fs.StoreMsg("A", nil, msg, 0)
		fs.StoreMsg("B", nil, msg, 0)
		fs.StoreMsg("C", nil, msg, 0)
		fs.StoreMsg("D", nil, msg, 0)
		fs.StoreMsg("E", nil, msg, 0)
		require_Equal(t, fs.numMsgBlocks(), 1)
		fs.Stop()

		// Grab the state from this stop.
		sfile := filepath.Join(fcfg.StoreDir, msgDir, streamStreamStateFile)
		buf, err := os.ReadFile(sfile)
		require_NoError(t, err)

		fs, err = newFileStoreWithCreated(fcfg, cfg, created, prf(&fcfg), nil)
		require_NoError(t, err)
		defer fs.Stop()

		// Store 5 more messages, then remove seq 2, "B".
		fs.StoreMsg("F", nil, msg, 0)
		fs.StoreMsg("G", nil, msg, 0)
		fs.StoreMsg("H", nil, msg, 0)
		fs.StoreMsg("I", nil, msg, 0)
		fs.StoreMsg("J", nil, msg, 0)
		fs.RemoveMsg(2)

		require_Equal(t, fs.numMsgBlocks(), 1)
		state := fs.State()
		fs.Stop()

		// Put back old stream state.
		// This will test that we properly walk multiple blocks past where we snapshotted state.
		err = os.WriteFile(sfile, buf, defaultFilePerms)
		require_NoError(t, err)

		fs, err = newFileStoreWithCreated(fcfg, cfg, created, prf(&fcfg), nil)
		require_NoError(t, err)
		defer fs.Stop()

		if newState := fs.State(); !reflect.DeepEqual(state, newState) {
			t.Fatalf("Restore state does not match:\n%+v\n%+v",
				state, newState)
		}
		// Check that index.db is still there. If we recover by raw data on a corrupt state we delete this.
		_, err = os.Stat(sfile)
		require_NoError(t, err)
	})
}

func TestFileStoreCompactingBlocksOnSync(t *testing.T) {
	testFileStoreAllPermutations(t, func(t *testing.T, fcfg FileStoreConfig) {
		fcfg.BlockSize = 1000 // 20 msgs per block.
		fcfg.SyncInterval = 100 * time.Millisecond
		cfg := StreamConfig{Name: "zzz", Subjects: []string{"*"}, Storage: FileStorage, MaxMsgsPer: 1}
		fs, err := newFileStoreWithCreated(fcfg, cfg, time.Now(), prf(&fcfg), nil)
		require_NoError(t, err)
		defer fs.Stop()

		// This yields an internal record length of 50 bytes. So 20 msgs per blk.
		msg := bytes.Repeat([]byte("Z"), 19)
		subjects := "ABCDEFGHIJKLMNOPQRST"
		for _, subj := range subjects {
			fs.StoreMsg(string(subj), nil, msg, 0)
		}
		require_Equal(t, fs.numMsgBlocks(), 1)
		total, reported, err := fs.Utilization()
		require_NoError(t, err)

		require_Equal(t, total, reported)

		// Now start removing, since we are small this should not kick in any inline logic.
		// Remove all interior messages, leave 1 and 20. So write B-S
		for i := 1; i < 19; i++ {
			fs.StoreMsg(string(subjects[i]), nil, msg, 0)
		}
		require_Equal(t, fs.numMsgBlocks(), 2)

		blkUtil := func() (uint64, uint64) {
			fs.mu.RLock()
			fmb := fs.blks[0]
			fs.mu.RUnlock()
			fmb.mu.RLock()
			defer fmb.mu.RUnlock()
			return fmb.rbytes, fmb.bytes
		}

		total, reported = blkUtil()
		require_Equal(t, reported, 100)
		// Raw bytes will be 1000, but due to compression could be less.
		if fcfg.Compression != NoCompression {
			require_True(t, total > reported)
		} else {
			require_Equal(t, total, 1000)
		}

		// Make sure the sync interval when kicked in compacts down to rbytes == 100.
		checkFor(t, time.Second, 100*time.Millisecond, func() error {
			if total, reported := blkUtil(); total <= reported {
				return nil
			}
			return fmt.Errorf("Not compacted yet, raw %v vs reported %v",
				friendlyBytes(total), friendlyBytes(reported))
		})
	})
}

// Make sure a call to Compact() updates PSIM correctly.
func TestFileStoreCompactAndPSIMWhenDeletingBlocks(t *testing.T) {
	fs, err := newFileStore(
		FileStoreConfig{StoreDir: t.TempDir(), BlockSize: 512},
		StreamConfig{Name: "zzz", Subjects: []string{"*"}, Storage: FileStorage})
	require_NoError(t, err)
	defer fs.Stop()

	subj, msg := "A", bytes.Repeat([]byte("ABC"), 33) // ~100bytes

	// Add in 10 As
	for i := 0; i < 10; i++ {
		fs.StoreMsg(subj, nil, msg, 0)
	}
	require_Equal(t, fs.numMsgBlocks(), 4)

	// Should leave 1.
	n, err := fs.Compact(10)
	require_NoError(t, err)
	require_Equal(t, n, 9)
	require_Equal(t, fs.numMsgBlocks(), 1)

	fs.mu.RLock()
	info, _ := fs.psim.Find(stringToBytes(subj))
	psi := *info
	fs.mu.RUnlock()

	// PSIM remains the same, since we'll not always know
	// that the head was removed, instead of the tail.
	require_Equal(t, psi.total, 1)
	require_Equal(t, psi.fblk, 1)
	require_Equal(t, psi.lblk, 4)
}

func TestFileStoreTrackSubjLenForPSIM(t *testing.T) {
	sd := t.TempDir()
	fs, err := newFileStore(
		FileStoreConfig{StoreDir: sd},
		StreamConfig{Name: "zzz", Subjects: []string{">"}, Storage: FileStorage})
	require_NoError(t, err)
	defer fs.Stop()

	// Place 1000 msgs with varying subjects.
	// Make sure we track the subject length properly.
	smap := make(map[string]int, 1000)
	buf := make([]byte, 10)
	for i := 0; i < 1000; i++ {
		var b strings.Builder
		// 1-6 tokens.
		numTokens := rand.IntN(6) + 1
		for i := 0; i < numTokens; i++ {
			tlen := rand.IntN(4) + 2
			tok := buf[:tlen]
			crand.Read(tok)
			b.WriteString(hex.EncodeToString(tok))
			if i != numTokens-1 {
				b.WriteString(".")
			}
		}
		subj := b.String()
		// Avoid dupes since will cause check to fail after we delete messages.
		if _, ok := smap[subj]; ok {
			continue
		}
		smap[subj] = len(subj)
		fs.StoreMsg(subj, nil, nil, 0)
	}

	check := func() {
		t.Helper()
		var total int
		for _, slen := range smap {
			total += slen
		}
		fs.mu.RLock()
		tsl := fs.tsl
		fs.mu.RUnlock()
		require_Equal(t, tsl, total)
	}

	check()

	// Delete ~half
	var smv StoreMsg
	for i := 0; i < 500; i++ {
		seq := uint64(rand.IntN(1000) + 1)
		sm, err := fs.LoadMsg(seq, &smv)
		if err != nil {
			continue
		}
		fs.RemoveMsg(seq)
		delete(smap, sm.subj)
	}

	check()

	// Make sure we can recover same after restart.
	fs.Stop()
	fs, err = newFileStore(
		FileStoreConfig{StoreDir: sd},
		StreamConfig{Name: "zzz", Subjects: []string{">"}, Storage: FileStorage})
	require_NoError(t, err)
	defer fs.Stop()

	check()

	// Drain the rest through purge.
	fs.Purge()
	smap = nil
	check()
}

// This was used to make sure our estimate was correct, but not needed normally.
func TestFileStoreLargeFullStatePSIM(t *testing.T) {
	sd := t.TempDir()
	fs, err := newFileStore(
		FileStoreConfig{StoreDir: sd},
		StreamConfig{Name: "zzz", Subjects: []string{">"}, Storage: FileStorage})
	require_NoError(t, err)
	defer fs.Stop()

	buf := make([]byte, 20)
	for i := 0; i < 100_000; i++ {
		var b strings.Builder
		// 1-6 tokens.
		numTokens := rand.IntN(6) + 1
		for i := 0; i < numTokens; i++ {
			tlen := rand.IntN(8) + 2
			tok := buf[:tlen]
			crand.Read(tok)
			b.WriteString(hex.EncodeToString(tok))
			if i != numTokens-1 {
				b.WriteString(".")
			}
		}
		subj := b.String()
		fs.StoreMsg(subj, nil, nil, 0)
	}
	fs.Stop()
}

func TestFileStoreLargeFullStateMetaCleanup(t *testing.T) {
	sd := t.TempDir()
	fs, err := newFileStore(
		FileStoreConfig{StoreDir: sd},
		StreamConfig{Name: "zzz", Subjects: []string{">"}, Storage: FileStorage})
	require_NoError(t, err)
	defer fs.Stop()

	subj, msg := "foo.bar.baz", bytes.Repeat([]byte("ABC"), 33) // ~100bytes
	for i := 0; i < 1000; i++ {
		fs.StoreMsg(subj, nil, nil, 0)
	}
	fs.Stop()

	mdir := filepath.Join(sd, msgDir)
	idxFile := filepath.Join(mdir, "1.idx")
	fssFile := filepath.Join(mdir, "1.fss")
	require_NoError(t, os.WriteFile(idxFile, msg, defaultFilePerms))
	require_NoError(t, os.WriteFile(fssFile, msg, defaultFilePerms))

	fs, err = newFileStore(
		FileStoreConfig{StoreDir: sd},
		StreamConfig{Name: "zzz", Subjects: []string{">"}, Storage: FileStorage})
	require_NoError(t, err)
	defer fs.Stop()

	checkFor(t, time.Second, 50*time.Millisecond, func() error {
		if _, err := os.Stat(idxFile); err == nil {
			return errors.New("idx file still exists")
		}
		if _, err := os.Stat(fssFile); err == nil {
			return errors.New("fss file still exists")
		}
		return nil
	})
}

func TestFileStoreIndexDBExistsAfterShutdown(t *testing.T) {
	sd := t.TempDir()
	fs, err := newFileStore(
		FileStoreConfig{StoreDir: sd},
		StreamConfig{Name: "zzz", Subjects: []string{">"}, Storage: FileStorage})
	require_NoError(t, err)
	defer fs.Stop()

	subj := "foo.bar.baz"
	for i := 0; i < 1000; i++ {
		fs.StoreMsg(subj, nil, nil, 0)
	}

	idxFile := filepath.Join(sd, msgDir, streamStreamStateFile)

	fs.mu.Lock()
	fs.dirty = 1
	if err := os.Remove(idxFile); err != nil && !errors.Is(err, os.ErrNotExist) {
		t.Fatal(err)
	}
	fs.mu.Unlock()

	fs.Stop()

	checkFor(t, time.Second, 50*time.Millisecond, func() error {
		if _, err := os.Stat(idxFile); err != nil {
			return fmt.Errorf("%q doesn't exist", idxFile)
		}
		return nil
	})
}

// https://github.com/nats-io/nats-server/issues/4842
func TestFileStoreSubjectCorruption(t *testing.T) {
	sd, blkSize := t.TempDir(), uint64(2*1024*1024)
	fs, err := newFileStore(
		FileStoreConfig{StoreDir: sd, BlockSize: blkSize},
		StreamConfig{Name: "zzz", Subjects: []string{"foo.*"}, Storage: FileStorage})
	require_NoError(t, err)
	defer fs.Stop()

	numSubjects := 100
	msgs := [][]byte{bytes.Repeat([]byte("ABC"), 333), bytes.Repeat([]byte("ABC"), 888), bytes.Repeat([]byte("ABC"), 555)}
	for i := 0; i < 10_000; i++ {
		subj := fmt.Sprintf("foo.%d", rand.IntN(numSubjects)+1)
		msg := msgs[rand.IntN(len(msgs))]
		fs.StoreMsg(subj, nil, msg, 0)
	}
	fs.Stop()

	require_NoError(t, os.Remove(filepath.Join(sd, msgDir, streamStreamStateFile)))

	fs, err = newFileStore(
		FileStoreConfig{StoreDir: sd, BlockSize: blkSize},
		StreamConfig{Name: "zzz", Subjects: []string{"foo.*"}, Storage: FileStorage})
	require_NoError(t, err)
	defer fs.Stop()

	for subj := range fs.SubjectsTotals(">") {
		var n int
		_, err := fmt.Sscanf(subj, "foo.%d", &n)
		require_NoError(t, err)
	}
}

func TestFileStoreNumPendingCanSkipStartingBlock(t *testing.T) {
	for _, test := range []struct {
		name           string
		lastPerSubject bool
		maxMsgsPer     int64
	}{
		{"LastPerSubject", true, 0},
		{"MaxMsgsPerSubjectOne", false, 1},
	} {
		t.Run(test.name, func(t *testing.T) {
			fs, err := newFileStore(
				FileStoreConfig{StoreDir: t.TempDir(), BlockSize: 1024},
				StreamConfig{Name: "TEST", Subjects: []string{"foo.*", "bar.*"}, Storage: FileStorage, MaxMsgsPer: test.maxMsgsPer})
			require_NoError(t, err)
			defer fs.Stop()

			// Keep the first block full of non-matching subjects, with all
			// matching subjects' last messages in later blocks.
			msg := bytes.Repeat([]byte("A"), 100)
			for _, prefix := range []string{"bar", "foo"} {
				for i := range 20 {
					_, _, err := fs.StoreMsg(fmt.Sprintf("%s.%d", prefix, i), nil, msg, 0)
					require_NoError(t, err)
				}
			}

			const startSeq = uint64(2)
			fs.mu.RLock()
			seqStart, _ := fs.selectMsgBlockWithIndex(startSeq)
			require_Equal(t, seqStart, 0)
			mb := fs.blks[seqStart]
			fs.psim.Match([]byte("foo.*"), func(_ []byte, info *psi) {
				require_True(t, info.lblk > mb.index)
			})
			fs.mu.RUnlock()

			// Start inside an unloaded block so the correction scan would
			// have to load it without the empty-lbm early return.
			require_NoError(t, mb.flushPendingMsgs())
			mb.mu.Lock()
			require_True(t, startSeq > mb.first.seq && startSeq <= mb.last.seq)
			mb.clearCacheAndOffset()
			require_True(t, mb.cacheNotLoaded())
			loads := mb.cloads
			mb.mu.Unlock()

			total, validThrough, err := fs.NumPending(startSeq, "foo.*", test.lastPerSubject)
			require_NoError(t, err)
			require_Equal(t, total, uint64(20))
			require_Equal(t, validThrough, uint64(40))
			mb.mu.RLock()
			defer mb.mu.RUnlock()
			require_Equal(t, mb.cloads, loads)
			require_True(t, mb.cacheNotLoaded())
		})
	}
}

// Since 2.10 we no longer have fss, and the approach for calculating NumPending would branch
// based on the old fss metadata being present. This meant that calculating NumPending in >= 2.10.x
// would load all blocks to complete. This test makes sure we do not do that anymore.
func TestFileStoreNumPendingLastBySubject(t *testing.T) {
	sd, blkSize := t.TempDir(), uint64(1024)
	fs, err := newFileStore(
		FileStoreConfig{StoreDir: sd, BlockSize: blkSize},
		StreamConfig{Name: "zzz", Subjects: []string{"foo.*.*"}, Storage: FileStorage})
	require_NoError(t, err)
	defer fs.Stop()

	numSubjects := 20
	msg := bytes.Repeat([]byte("ABC"), 25)
	for i := 1; i <= 1000; i++ {
		subj := fmt.Sprintf("foo.%d.%d", rand.IntN(numSubjects)+1, i)
		fs.StoreMsg(subj, nil, msg, 0)
	}
	// Each block has ~8 msgs.
	require_True(t, fs.numMsgBlocks() > 100)

	calcCacheLoads := func() (cloads uint64) {
		fs.mu.RLock()
		defer fs.mu.RUnlock()
		for _, mb := range fs.blks {
			mb.mu.RLock()
			cloads += mb.cloads
			mb.mu.RUnlock()
		}
		return cloads
	}

	total, _, err := fs.NumPending(0, "foo.*.*", true)
	require_NoError(t, err)
	require_Equal(t, total, 1000)
	// Make sure no blocks were loaded to calculate this as a new consumer.
	require_Equal(t, calcCacheLoads(), 0)

	checkResult := func(sseq, np uint64, filter string) {
		t.Helper()
		var checkTotal uint64
		var smv StoreMsg
		for seq := sseq; seq <= 1000; seq++ {
			sm, err := fs.LoadMsg(seq, &smv)
			require_NoError(t, err)
			if subjectIsSubsetMatch(sm.subj, filter) {
				checkTotal++
			}
		}
		require_Equal(t, np, checkTotal)
	}

	// Make sure partials work properly.
	for _, filter := range []string{"foo.10.*", "*.22.*", "*.*.222", "foo.5.999", "*.2.*"} {
		sseq := uint64(rand.IntN(250) + 200) // Between 200-450
		total, _, err = fs.NumPending(sseq, filter, true)
		require_NoError(t, err)
		checkResult(sseq, total, filter)
	}
}

// We had a bug that could cause internal memory corruption of the psim keys in memory
// which could have been written to disk via index.db.
func TestFileStoreCorruptPSIMOnDisk(t *testing.T) {
	sd := t.TempDir()
	fs, err := newFileStore(
		FileStoreConfig{StoreDir: sd},
		StreamConfig{Name: "zzz", Subjects: []string{"foo.*"}, Storage: FileStorage})
	require_NoError(t, err)
	defer fs.Stop()

	fs.StoreMsg("foo.bar", nil, []byte("ABC"), 0)
	fs.StoreMsg("foo.baz", nil, []byte("XYZ"), 0)

	// Force bad subject.
	fs.mu.Lock()
	psi, _ := fs.psim.Find(stringToBytes("foo.bar"))
	bad := []rune("foo.bar")
	bad[3] = utf8.RuneError
	fs.psim.Insert(stringToBytes(string(bad)), *psi)
	fs.psim.Delete(stringToBytes("foo.bar"))
	fs.dirty++
	fs.mu.Unlock()

	// Restart
	fs.Stop()
	fs, err = newFileStore(
		FileStoreConfig{StoreDir: sd},
		StreamConfig{Name: "zzz", Subjects: []string{"foo.*"}, Storage: FileStorage})
	require_NoError(t, err)
	defer fs.Stop()

	sm, err := fs.LoadLastMsg("foo.bar", nil)
	require_NoError(t, err)
	require_True(t, bytes.Equal(sm.msg, []byte("ABC")))

	sm, err = fs.LoadLastMsg("foo.baz", nil)
	require_NoError(t, err)
	require_True(t, bytes.Equal(sm.msg, []byte("XYZ")))
}

func TestFileStorePurgeExBufPool(t *testing.T) {
	sd := t.TempDir()
	fs, err := newFileStore(
		FileStoreConfig{StoreDir: sd, BlockSize: 1024},
		StreamConfig{Name: "zzz", Subjects: []string{"foo.*"}, Storage: FileStorage})
	require_NoError(t, err)
	defer fs.Stop()

	msg := bytes.Repeat([]byte("ABC"), 33) // ~100bytes
	for i := 0; i < 1000; i++ {
		fs.StoreMsg("foo.foo", nil, msg, 0)
		fs.StoreMsg("foo.bar", nil, msg, 0)
	}

	p, err := fs.PurgeEx("foo.bar", 0, 0)
	require_NoError(t, err)
	require_Equal(t, p, 1000)

	// Now make sure we do not have all of the msg blocks cache's loaded.
	var loaded int
	fs.mu.RLock()
	for _, mb := range fs.blks {
		mb.mu.RLock()
		if mb.cacheAlreadyLoaded() {
			loaded++
		}
		mb.mu.RUnlock()
	}
	fs.mu.RUnlock()
	require_Equal(t, loaded, 1)
}

func TestFileStoreFSSMeta(t *testing.T) {
	sd := t.TempDir()
	fs, err := newFileStore(
		FileStoreConfig{StoreDir: sd, BlockSize: 100, CacheExpire: 200 * time.Millisecond, SubjectStateExpire: time.Second},
		StreamConfig{Name: "zzz", Subjects: []string{"*"}, Storage: FileStorage})
	require_NoError(t, err)
	defer fs.Stop()

	// This yields an internal record length of 50 bytes. So 2 msgs per blk with subject len of 1, e.g. "A" or "Z".
	msg := bytes.Repeat([]byte("Z"), 19)

	// Should leave us with |A-Z| |Z-Z| |Z-Z| |Z-A|
	fs.StoreMsg("A", nil, msg, 0)
	for i := 0; i < 6; i++ {
		fs.StoreMsg("Z", nil, msg, 0)
	}
	fs.StoreMsg("A", nil, msg, 0)

	// Let cache's expire before PurgeEx which will load them back in.
	time.Sleep(500 * time.Millisecond)

	p, err := fs.PurgeEx("A", 0, 0)
	require_NoError(t, err)
	require_Equal(t, p, 2)

	// Make sure cache is not loaded.
	var stillHasCache bool
	fs.mu.RLock()
	for _, mb := range fs.blks {
		mb.mu.RLock()
		stillHasCache = stillHasCache || mb.cacheAlreadyLoaded()
		mb.mu.RUnlock()
	}
	fs.mu.RUnlock()

	require_False(t, stillHasCache)

	// Let fss expire via SubjectStateExpire.
	time.Sleep(1500 * time.Millisecond)

	var noFSS bool
	fs.mu.RLock()
	for _, mb := range fs.blks {
		mb.mu.RLock()
		noFSS = noFSS || mb.fssNotLoaded()
		mb.mu.RUnlock()
	}
	fs.mu.RUnlock()

	require_True(t, noFSS)
}

func TestFileStoreExpireCacheOnLinearWalk(t *testing.T) {
	sd := t.TempDir()
	expire := 250 * time.Millisecond
	fs, err := newFileStore(
		FileStoreConfig{StoreDir: sd, CacheExpire: expire},
		StreamConfig{Name: "zzz", Subjects: []string{"*"}, Storage: FileStorage})
	require_NoError(t, err)
	defer fs.Stop()

	// This yields an internal record length of 50 bytes.
	subj, msg := "Z", bytes.Repeat([]byte("Z"), 19)

	// Store 10 messages, so 5 blocks.
	for i := 0; i < 10; i++ {
		fs.StoreMsg(subj, nil, msg, 0)
	}
	// Let them all expire. This way we load as we walk and can test that we expire all blocks without
	// needing to worry about last write times blocking forced expiration.
	time.Sleep(expire + ats.TickInterval*2)

	checkNoCache := func() {
		t.Helper()
		fs.mu.RLock()
		var stillHasCache bool
		for _, mb := range fs.blks {
			mb.mu.RLock()
			stillHasCache = stillHasCache || mb.cacheAlreadyLoaded()
			mb.mu.RUnlock()
		}
		fs.mu.RUnlock()
		require_False(t, stillHasCache)
	}

	// Walk forward.
	var smv StoreMsg
	for seq := uint64(1); seq <= 10; seq++ {
		_, err := fs.LoadMsg(seq, &smv)
		require_NoError(t, err)
	}
	checkNoCache()

	// No test walking backwards. We have this scenario when we search for starting points for sourced streams.
	// Noticed some memory bloat when we have to search many blocks looking for a source that may be closer to the
	// beginning of the stream (infrequently updated sourced stream).
	for seq := uint64(10); seq >= 1; seq-- {
		_, err := fs.LoadMsg(seq, &smv)
		require_NoError(t, err)
	}
	checkNoCache()

	// Now make sure still expires properly on linear scans with deleted msgs.
	// We want to make sure we track linear updates even if message deleted.
	_, err = fs.RemoveMsg(2)
	require_NoError(t, err)
	_, err = fs.RemoveMsg(9)
	require_NoError(t, err)

	// Walk forward.
	for seq := uint64(1); seq <= 10; seq++ {
		_, err := fs.LoadMsg(seq, &smv)
		if seq == 2 || seq == 9 {
			require_Error(t, err, errDeletedMsg)
		} else {
			require_NoError(t, err)
		}
	}
	checkNoCache()
}

func TestFileStoreSkipMsgs(t *testing.T) {
	fs, err := newFileStore(
		FileStoreConfig{StoreDir: t.TempDir(), BlockSize: 1024},
		StreamConfig{Name: "zzz", Subjects: []string{"*"}, Storage: FileStorage})
	require_NoError(t, err)
	defer fs.Stop()

	// Test on empty FS first.
	// Make sure wrong starting sequence fails.
	err = fs.SkipMsgs(10, 100)
	require_Error(t, err, ErrSequenceMismatch)

	err = fs.SkipMsgs(1, 100)
	require_NoError(t, err)

	state := fs.State()
	require_Equal(t, state.FirstSeq, 101)
	require_Equal(t, state.LastSeq, 100)
	require_Equal(t, fs.numMsgBlocks(), 1)

	// Now add alot.
	err = fs.SkipMsgs(101, 100_000)
	require_NoError(t, err)
	state = fs.State()
	require_Equal(t, state.FirstSeq, 100_101)
	require_Equal(t, state.LastSeq, 100_100)
	require_Equal(t, fs.numMsgBlocks(), 1)

	// Now add in a message, and then skip to check dmap.
	fs, err = newFileStore(
		FileStoreConfig{StoreDir: t.TempDir(), BlockSize: 1024},
		StreamConfig{Name: "zzz", Subjects: []string{"*"}, Storage: FileStorage})
	require_NoError(t, err)
	defer fs.Stop()

	fs.StoreMsg("foo", nil, nil, 0)
	err = fs.SkipMsgs(2, 10)
	require_NoError(t, err)
	state = fs.State()
	require_Equal(t, state.FirstSeq, 1)
	require_Equal(t, state.LastSeq, 11)
	require_Equal(t, state.Msgs, 1)
	require_Equal(t, state.NumDeleted, 10)
	require_Equal(t, len(state.Deleted), 10)

	// Check Fast State too.
	state.Deleted = nil
	fs.FastState(&state)
	require_Equal(t, state.FirstSeq, 1)
	require_Equal(t, state.LastSeq, 11)
	require_Equal(t, state.Msgs, 1)
	require_Equal(t, state.NumDeleted, 10)
}

func TestFileStoreOptimizeFirstLoadNextMsgWithSequenceZero(t *testing.T) {
	sd := t.TempDir()
	fs, err := newFileStore(
		FileStoreConfig{StoreDir: sd, BlockSize: 4096},
		StreamConfig{Name: "zzz", Subjects: []string{"foo.*"}, Storage: FileStorage})
	require_NoError(t, err)
	defer fs.Stop()

	msg := bytes.Repeat([]byte("ZZZ"), 33) // ~100bytes

	for i := 0; i < 5000; i++ {
		fs.StoreMsg("foo.A", nil, msg, 0)
	}
	// This will create alot of blocks, ~167.
	// Just used to check that we do not load these in when searching.
	// Now add in 10 for foo.bar at the end.
	for i := 0; i < 10; i++ {
		fs.StoreMsg("foo.B", nil, msg, 0)
	}
	// The bug would not be visible on running server per se since we would have had fss loaded
	// and that sticks around a bit longer, we would use that to skip over the early blocks. So stop
	// and restart the filestore.
	fs.Stop()
	fs, err = newFileStore(
		FileStoreConfig{StoreDir: sd, BlockSize: 4096},
		StreamConfig{Name: "zzz", Subjects: []string{"foo.*"}, Storage: FileStorage})
	require_NoError(t, err)
	defer fs.Stop()

	// Now fetch the next message for foo.B but set starting sequence to 0.
	_, nseq, err := fs.LoadNextMsg("foo.B", false, 0, nil)
	require_NoError(t, err)
	require_Equal(t, nseq, 5001)
	// Now check how many blks are loaded, should be only 1.
	require_Equal(t, fs.cacheLoads(), 1)
}

func TestFileStoreWriteFullStateHighSubjectCardinality(t *testing.T) {
	t.Skip()

	sd := t.TempDir()
	fs, err := newFileStore(
		FileStoreConfig{StoreDir: sd, BlockSize: 4096},
		StreamConfig{Name: "zzz", Subjects: []string{"foo.*"}, Storage: FileStorage})
	require_NoError(t, err)
	defer fs.Stop()

	msg := []byte{1, 2, 3}

	for i := 0; i < 1_000_000; i++ {
		subj := fmt.Sprintf("subj_%d", i)
		_, _, err := fs.StoreMsg(subj, nil, msg, 0)
		require_NoError(t, err)
	}

	start := time.Now()
	require_NoError(t, fs.writeFullState())
	t.Logf("Took %s to writeFullState", time.Since(start))
}

func TestFileStoreEraseMsgWithDbitSlots(t *testing.T) {
	fs, err := newFileStore(
		FileStoreConfig{StoreDir: t.TempDir()},
		StreamConfig{Name: "zzz", Subjects: []string{"foo"}, Storage: FileStorage})
	require_NoError(t, err)
	defer fs.Stop()

	fs.StoreMsg("foo", nil, []byte("abd"), 0)
	for i := 0; i < 10; i++ {
		fs.SkipMsg(0)
	}
	fs.StoreMsg("foo", nil, []byte("abd"), 0)
	// Now grab that first block and compact away the skips which will
	// introduce dbits into our idx.
	fs.mu.RLock()
	mb := fs.blks[0]
	fs.mu.RUnlock()
	// Compact.
	mb.mu.Lock()
	err = mb.compact()
	mb.mu.Unlock()
	require_NoError(t, err)

	removed, err := fs.EraseMsg(1)
	require_NoError(t, err)
	require_True(t, removed)
}

func TestFileStoreEraseMsgWithAllTrailingDbitSlots(t *testing.T) {
	fs, err := newFileStore(
		FileStoreConfig{StoreDir: t.TempDir()},
		StreamConfig{Name: "zzz", Subjects: []string{"foo"}, Storage: FileStorage})
	require_NoError(t, err)
	defer fs.Stop()

	fs.StoreMsg("foo", nil, []byte("abc"), 0)
	fs.StoreMsg("foo", nil, []byte("abcdefg"), 0)

	for i := 0; i < 10; i++ {
		fs.SkipMsg(0)
	}
	// Now grab that first block and compact away the skips which will
	// introduce dbits into our idx.
	fs.mu.RLock()
	mb := fs.blks[0]
	fs.mu.RUnlock()
	// Compact.
	mb.mu.Lock()
	err = mb.compact()
	mb.mu.Unlock()
	require_NoError(t, err)

	removed, err := fs.EraseMsg(2)
	require_NoError(t, err)
	require_True(t, removed)
}

func TestFileStoreMultiLastSeqs(t *testing.T) {
	fs, err := newFileStore(
		FileStoreConfig{StoreDir: t.TempDir(), BlockSize: 256}, // Make block size small to test multiblock selections with maxSeq
		StreamConfig{Name: "zzz", Subjects: []string{"foo.*", "bar.*"}, Storage: FileStorage})
	require_NoError(t, err)
	defer fs.Stop()

	msg := []byte("abc")
	for i := 0; i < 33; i++ {
		fs.StoreMsg("foo.foo", nil, msg, 0)
		fs.StoreMsg("foo.bar", nil, msg, 0)
		fs.StoreMsg("foo.baz", nil, msg, 0)
	}
	for i := 0; i < 33; i++ {
		fs.StoreMsg("bar.foo", nil, msg, 0)
		fs.StoreMsg("bar.bar", nil, msg, 0)
		fs.StoreMsg("bar.baz", nil, msg, 0)
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
	seqs, err := fs.MultiLastSeqs([]string{"foo.*"}, 3, -1)
	require_NoError(t, err)
	checkResults(seqs, []uint64{1, 2, 3})
	// Up to last sequence of the stream.
	seqs, err = fs.MultiLastSeqs([]string{"foo.*"}, 0, -1)
	require_NoError(t, err)
	checkResults(seqs, []uint64{97, 98, 99})
	// Check for bar.* at the end.
	seqs, err = fs.MultiLastSeqs([]string{"bar.*"}, 0, -1)
	require_NoError(t, err)
	checkResults(seqs, []uint64{196, 197, 198})
	// This should find nothing.
	seqs, err = fs.MultiLastSeqs([]string{"bar.*"}, 99, -1)
	require_NoError(t, err)
	checkResults(seqs, nil)

	// Do multiple subjects explicitly.
	seqs, err = fs.MultiLastSeqs([]string{"foo.foo", "foo.bar", "foo.baz"}, 3, -1)
	require_NoError(t, err)
	checkResults(seqs, []uint64{1, 2, 3})
	seqs, err = fs.MultiLastSeqs([]string{"foo.foo", "foo.bar", "foo.baz"}, 0, -1)
	require_NoError(t, err)
	checkResults(seqs, []uint64{97, 98, 99})
	seqs, err = fs.MultiLastSeqs([]string{"bar.foo", "bar.bar", "bar.baz"}, 0, -1)
	require_NoError(t, err)
	checkResults(seqs, []uint64{196, 197, 198})
	seqs, err = fs.MultiLastSeqs([]string{"bar.foo", "bar.bar", "bar.baz"}, 99, -1)
	require_NoError(t, err)
	checkResults(seqs, nil)

	// Check single works
	seqs, err = fs.MultiLastSeqs([]string{"foo.foo"}, 3, -1)
	require_NoError(t, err)
	checkResults(seqs, []uint64{1})

	// Now test that we properly de-duplicate between filters.
	seqs, err = fs.MultiLastSeqs([]string{"foo.*", "foo.bar"}, 3, -1)
	require_NoError(t, err)
	checkResults(seqs, []uint64{1, 2, 3})
	seqs, err = fs.MultiLastSeqs([]string{"bar.>", "bar.bar", "bar.baz"}, 0, -1)
	require_NoError(t, err)
	checkResults(seqs, []uint64{196, 197, 198})

	// All
	seqs, err = fs.MultiLastSeqs([]string{">"}, 0, -1)
	require_NoError(t, err)
	checkResults(seqs, []uint64{97, 98, 99, 196, 197, 198})
	seqs, err = fs.MultiLastSeqs([]string{">"}, 99, -1)
	require_NoError(t, err)
	checkResults(seqs, []uint64{97, 98, 99})

	// No subjects match the filter.
	seqs, err = fs.MultiLastSeqs([]string{"missing.*"}, 0, -1)
	require_NoError(t, err)
	checkResults(seqs, nil)
}

func TestFileStoreMultiLastSeqsDeletedBlocks(t *testing.T) {
	for _, test := range []struct {
		name     string
		subjects []string
		deletes  []uint64
		filters  []string
		maxSeq   uint64
		want     []uint64
	}{
		{
			name:     "removed last block",
			subjects: []string{"foo.a", "foo.b", "foo.b", "foo.a", "foo.a", "foo.a", "foo.b", "foo.b", "foo.b"},
			deletes:  []uint64{4, 5, 6},
			filters:  []string{"foo.a"},
			maxSeq:   8,
			want:     []uint64{1},
		},
		{
			name:     "multiple subjects with removed last block",
			subjects: []string{"foo.a", "foo.c", "foo.c", "foo.a", "foo.c", "foo.c", "foo.b", "foo.b", "foo.b"},
			deletes:  []uint64{4, 5, 6},
			filters:  []string{"foo.a", "foo.c"},
			maxSeq:   8,
			want:     []uint64{1, 3},
		},
		{
			name:     "subjects with existing and removed last blocks",
			subjects: []string{"foo.a", "foo.c", "foo.c", "foo.a", "foo.a", "foo.a", "foo.b", "foo.b", "foo.b"},
			deletes:  []uint64{4, 5, 6},
			filters:  []string{"foo.a", "foo.c"},
			maxSeq:   8,
			want:     []uint64{1, 3},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			// Each block holds three messages, so deleting 4-6 removes block 2.
			fs, err := newFileStore(
				FileStoreConfig{StoreDir: t.TempDir(), BlockSize: 128},
				StreamConfig{Name: "TEST", Subjects: []string{"foo.*"}, Storage: FileStorage})
			require_NoError(t, err)
			defer fs.Stop()
			for _, subject := range test.subjects {
				_, _, err := fs.StoreMsg(subject, nil, []byte("x"), 0)
				require_NoError(t, err)
			}
			for _, seq := range test.deletes {
				removed, err := fs.RemoveMsg(seq)
				require_NoError(t, err)
				require_True(t, removed)
			}
			seqs, err := fs.MultiLastSeqs(test.filters, test.maxSeq, 0)
			require_NoError(t, err)
			if !slices.Equal(seqs, test.want) {
				t.Errorf("MultiLastSeqs: expected %v, got %v", test.want, seqs)
			}
			var delivered []uint64
			total, pending, err := fs.MultiLastMsgs(test.filters, 0, test.maxSeq, 1024, func(sm *StoreMsg, _ uint64) bool {
				delivered = append(delivered, sm.seq)
				return true
			})
			require_NoError(t, err)
			require_Equal(t, total, uint64(len(test.want)))
			require_Equal(t, pending, uint64(0))
			if !slices.Equal(delivered, test.want) {
				t.Errorf("MultiLastMsgs: expected %v, got %v", test.want, delivered)
			}
		})
	}
}

func TestFileStoreMultiLastSeqsOverlappingFilters(t *testing.T) {
	// Each block holds three messages, placing foo.a and foo.b in separate blocks.
	fs, err := newFileStore(
		FileStoreConfig{StoreDir: t.TempDir(), BlockSize: 128},
		StreamConfig{Name: "TEST", Subjects: []string{"foo.*"}, Storage: FileStorage})
	require_NoError(t, err)
	defer fs.Stop()
	for _, subject := range []string{"foo.a", "foo.a", "foo.a", "foo.b", "foo.b", "foo.b"} {
		_, _, err := fs.StoreMsg(subject, nil, []byte("x"), 0)
		require_NoError(t, err)
	}

	filters := []string{"foo.*", "foo.a"}
	const maxSeq = 5
	want := []uint64{3, 5}
	seqs, err := fs.MultiLastSeqs(filters, maxSeq, 0)
	require_NoError(t, err)
	if !slices.Equal(seqs, want) {
		t.Errorf("MultiLastSeqs: expected %v, got %v", want, seqs)
	}
	var delivered []uint64
	total, pending, err := fs.MultiLastMsgs(filters, 0, maxSeq, 1024, func(sm *StoreMsg, _ uint64) bool {
		delivered = append(delivered, sm.seq)
		return true
	})
	require_NoError(t, err)
	require_Equal(t, total, uint64(len(want)))
	require_Equal(t, pending, uint64(0))
	if !slices.Equal(delivered, want) {
		t.Errorf("MultiLastMsgs: expected %v, got %v", want, delivered)
	}
}

func TestFileStoreMultiLastSeqsMaxAllowed(t *testing.T) {
	fs, err := newFileStore(
		FileStoreConfig{StoreDir: t.TempDir()},
		StreamConfig{Name: "zzz", Subjects: []string{"foo.*"}, Storage: FileStorage})
	require_NoError(t, err)
	defer fs.Stop()

	msg := []byte("abc")
	for i := 1; i <= 100; i++ {
		fs.StoreMsg(fmt.Sprintf("foo.%d", i), nil, msg, 0)
	}
	// Test that if we specify maxAllowed that we get the correct error.
	seqs, err := fs.MultiLastSeqs([]string{"foo.*"}, 0, 10)
	require_True(t, seqs == nil)
	require_Error(t, err, ErrTooManyResults)
}

// https://github.com/nats-io/nats-server/issues/5236
// Unclear how the sequences get off here, this is just forcing the situation reported.
func TestFileStoreMsgBlockFirstAndLastSeqCorrupt(t *testing.T) {
	fs, err := newFileStore(
		FileStoreConfig{StoreDir: t.TempDir()},
		StreamConfig{Name: "zzz", Subjects: []string{"foo.*"}, Storage: FileStorage})
	require_NoError(t, err)
	defer fs.Stop()

	msg := []byte("abc")
	for i := 1; i <= 10; i++ {
		fs.StoreMsg(fmt.Sprintf("foo.%d", i), nil, msg, 0)
	}
	fs.Purge()

	fs.mu.RLock()
	mb := fs.blks[0]
	fs.mu.RUnlock()

	mb.mu.Lock()
	mb.tryForceExpireCacheLocked()
	atomic.StoreUint64(&mb.last.seq, 9)
	mb.mu.Unlock()

	// We should rebuild here and return no error.
	require_NoError(t, mb.loadMsgs())
	mb.mu.RLock()
	fseq, lseq := atomic.LoadUint64(&mb.first.seq), atomic.LoadUint64(&mb.last.seq)
	mb.mu.RUnlock()
	require_Equal(t, fseq, 11)
	require_Equal(t, lseq, 10)
}

func TestFileStoreWriteFullStateAfterPurgeEx(t *testing.T) {
	fs, err := newFileStore(
		FileStoreConfig{StoreDir: t.TempDir()},
		StreamConfig{Name: "zzz", Subjects: []string{"foo.*"}, Storage: FileStorage})
	require_NoError(t, err)
	defer fs.Stop()

	msg := []byte("abc")
	for i := 1; i <= 10; i++ {
		fs.StoreMsg(fmt.Sprintf("foo.%d", i), nil, msg, 0)
	}
	fs.RemoveMsg(8)
	fs.RemoveMsg(9)
	fs.RemoveMsg(10)

	n, err := fs.PurgeEx(">", 8, 0)
	require_NoError(t, err)
	require_Equal(t, n, 7)

	var ss StreamState
	fs.FastState(&ss)
	require_Equal(t, ss.FirstSeq, 11)
	require_Equal(t, ss.LastSeq, 10)

	// Make sure this does not reset our state due to skew with msg blocks.
	fs.writeFullState()
	fs.FastState(&ss)
	require_Equal(t, ss.FirstSeq, 11)
	require_Equal(t, ss.LastSeq, 10)
}

func TestFileStoreFSSExpire(t *testing.T) {
	fs, err := newFileStore(
		FileStoreConfig{StoreDir: t.TempDir(), BlockSize: 8192, CacheExpire: 1 * time.Second, SubjectStateExpire: time.Second},
		StreamConfig{Name: "zzz", Subjects: []string{"foo.*"}, MaxMsgsPer: 1, Storage: FileStorage})
	require_NoError(t, err)
	defer fs.Stop()

	msg := []byte("abc")
	for i := 1; i <= 1000; i++ {
		fs.StoreMsg(fmt.Sprintf("foo.%d", i), nil, msg, 0)
	}
	// Flush fss by hand, cache should be flushed as well.
	fs.mu.RLock()
	for _, mb := range fs.blks {
		mb.mu.Lock()
		mb.fss = nil
		mb.mu.Unlock()
	}
	fs.mu.RUnlock()

	fs.StoreMsg("foo.11", nil, msg, 0)
	time.Sleep(900 * time.Millisecond)
	// This should keep fss alive in the first block..
	// As well as cache itself due to remove activity.
	fs.StoreMsg("foo.22", nil, msg, 0)
	time.Sleep(300 * time.Millisecond)
	// Check that fss and the cache are still loaded.
	fs.mu.RLock()
	mb := fs.blks[0]
	fs.mu.RUnlock()
	mb.mu.RLock()
	cache, fss := mb.ecache.Value(), mb.fss
	mb.mu.RUnlock()
	require_True(t, fss != nil)
	require_True(t, cache != nil)
}

func TestFileStoreFSSExpireNumPending(t *testing.T) {
	fs, err := newFileStore(
		FileStoreConfig{StoreDir: t.TempDir(), BlockSize: 8192, CacheExpire: 1 * time.Second, SubjectStateExpire: 2 * time.Second},
		StreamConfig{Name: "zzz", Subjects: []string{"foo.*.*"}, Storage: FileStorage})
	require_NoError(t, err)
	defer fs.Stop()

	msg := []byte("abc")
	for i := 1; i <= 100_000; i++ {
		fs.StoreMsg(fmt.Sprintf("foo.A.%d", i), nil, msg, 0)
		fs.StoreMsg(fmt.Sprintf("foo.B.%d", i), nil, msg, 0)
	}
	// Flush fss by hand, cache should be flushed as well.
	fs.mu.RLock()
	for _, mb := range fs.blks {
		mb.mu.Lock()
		mb.fss = nil
		mb.mu.Unlock()
	}
	fs.mu.RUnlock()

	nb := fs.numMsgBlocks()
	// Now execute NumPending() such that we load lots of blocks and make sure fss do not expire.
	start := time.Now()
	n, _, err := fs.NumPending(100_000, "foo.A.*", false)
	require_NoError(t, err)
	elapsed := time.Since(start)

	require_Equal(t, n, 50_000)
	// Make sure we did not force expire the fss. We would have loaded first half of blocks.
	var noFss bool
	last := nb/2 - 1
	fs.mu.RLock()
	for i, mb := range fs.blks {
		mb.mu.RLock()
		noFss = mb.fss == nil
		mb.mu.RUnlock()
		if noFss || i == last {
			break
		}
	}
	fs.mu.RUnlock()
	require_False(t, noFss)

	// Run again, make sure faster. This is consequence of fss being loaded now.
	start = time.Now()
	fs.NumPending(100_000, "foo.A.*", false)
	require_True(t, elapsed > 2*time.Since(start))

	// Now do with start past the mid-point.
	start = time.Now()
	fs.NumPending(150_000, "foo.B.*", false)
	elapsed = time.Since(start)
	time.Sleep(time.Second)
	start = time.Now()
	fs.NumPending(150_000, "foo.B.*", false)
	require_True(t, elapsed > time.Since(start))

	// Sleep enough so that all mb.fss should expire, which is 2s above.
	time.Sleep(4 * time.Second)
	fs.mu.RLock()
	for i, mb := range fs.blks {
		mb.mu.RLock()
		fss := mb.fss
		mb.mu.RUnlock()
		if fss != nil {
			fs.mu.RUnlock()
			t.Fatalf("Detected loaded fss for mb %d (size %d)", i, fss.Size())
		}
	}
	fs.mu.RUnlock()
}

// We want to ensure that recovery of deleted messages survives no index.db and compactions.
func TestFileStoreRecoverWithRemovesAndNoIndexDB(t *testing.T) {
	sd := t.TempDir()
	fs, err := newFileStore(
		FileStoreConfig{StoreDir: sd, BlockSize: 250},
		StreamConfig{Name: "zzz", Subjects: []string{"foo.*"}, Storage: FileStorage})
	require_NoError(t, err)
	defer fs.Stop()

	msg := []byte("abc")
	for i := 1; i <= 10; i++ {
		fs.StoreMsg(fmt.Sprintf("foo.%d", i), nil, msg, 0)
	}
	fs.RemoveMsg(1)
	fs.RemoveMsg(2)
	fs.RemoveMsg(8)

	var ss StreamState
	fs.FastState(&ss)
	require_Equal(t, ss.FirstSeq, 3)
	require_Equal(t, ss.LastSeq, 10)
	require_Equal(t, ss.Msgs, 7)

	// Compact last block.
	fs.mu.RLock()
	lmb := fs.lmb
	fs.mu.RUnlock()
	lmb.mu.Lock()
	err = lmb.compact()
	lmb.mu.Unlock()
	require_NoError(t, err)
	// Stop but remove index.db
	sfile := filepath.Join(sd, msgDir, streamStreamStateFile)
	fs.Stop()
	os.Remove(sfile)

	fs, err = newFileStore(
		FileStoreConfig{StoreDir: sd},
		StreamConfig{Name: "zzz", Subjects: []string{"foo.*"}, Storage: FileStorage})
	require_NoError(t, err)
	defer fs.Stop()

	fs.FastState(&ss)
	require_Equal(t, ss.FirstSeq, 3)
	require_Equal(t, ss.LastSeq, 10)
	require_Equal(t, ss.Msgs, 7)
}

func TestFileStoreReloadAndLoseLastSequence(t *testing.T) {
	sd := t.TempDir()
	fs, err := newFileStore(
		FileStoreConfig{StoreDir: sd},
		StreamConfig{Name: "zzz", Subjects: []string{"foo.*"}, Storage: FileStorage})
	require_NoError(t, err)
	defer fs.Stop()

	for i := 0; i < 22; i++ {
		fs.SkipMsg(0)
	}

	// Restart 5 times.
	for i := 0; i < 5; i++ {
		fs.Stop()
		fs, err = newFileStore(
			FileStoreConfig{StoreDir: sd},
			StreamConfig{Name: "zzz", Subjects: []string{"foo.*"}, Storage: FileStorage})
		require_NoError(t, err)
		defer fs.Stop()
		var ss StreamState
		fs.FastState(&ss)
		require_Equal(t, ss.FirstSeq, 23)
		require_Equal(t, ss.LastSeq, 22)
	}
}

func TestFileStoreReloadAndLoseLastSequenceWithSkipMsgs(t *testing.T) {
	sd := t.TempDir()
	fs, err := newFileStore(
		FileStoreConfig{StoreDir: sd},
		StreamConfig{Name: "zzz", Subjects: []string{"foo.*"}, Storage: FileStorage})
	require_NoError(t, err)
	defer fs.Stop()

	// Make sure same works with SkipMsgs which can kick in from delete blocks to replicas.
	require_NoError(t, fs.SkipMsgs(0, 22))

	// Restart 5 times.
	for i := 0; i < 5; i++ {
		fs.Stop()
		fs, err = newFileStore(
			FileStoreConfig{StoreDir: sd},
			StreamConfig{Name: "zzz", Subjects: []string{"foo.*"}, Storage: FileStorage})
		require_NoError(t, err)
		defer fs.Stop()
		var ss StreamState
		fs.FastState(&ss)
		require_Equal(t, ss.FirstSeq, 23)
		require_Equal(t, ss.LastSeq, 22)
	}
}

func TestFileStoreLoadLastWildcard(t *testing.T) {
	sd := t.TempDir()
	fs, err := newFileStore(
		FileStoreConfig{StoreDir: sd, BlockSize: 512},
		StreamConfig{Name: "zzz", Subjects: []string{"foo.*.*"}, Storage: FileStorage})
	require_NoError(t, err)
	defer fs.Stop()

	msg := []byte("hello")
	fs.StoreMsg("foo.22.baz", nil, msg, 0)
	fs.StoreMsg("foo.22.bar", nil, msg, 0)
	for i := 0; i < 1000; i++ {
		fs.StoreMsg("foo.11.foo", nil, msg, 0)
	}

	// Make sure we remove fss since that would mask the problem that we walk
	// all the blocks.
	fs.mu.RLock()
	for _, mb := range fs.blks {
		mb.mu.Lock()
		mb.fss = nil
		mb.mu.Unlock()
	}
	fs.mu.RUnlock()

	// Attempt to load the last msg using a wildcarded subject.
	sm, err := fs.LoadLastMsg("foo.22.*", nil)
	require_NoError(t, err)
	require_Equal(t, sm.seq, 2)

	// Make sure that we took advantage of psim meta data and only load one block.
	var cloads uint64
	fs.mu.RLock()
	for _, mb := range fs.blks {
		mb.mu.RLock()
		cloads += mb.cloads
		mb.mu.RUnlock()
	}
	fs.mu.RUnlock()
	require_Equal(t, cloads, 1)
}

func TestFileStoreLoadLastWildcardWithPresenceMultipleBlocks(t *testing.T) {
	sd := t.TempDir()
	fs, err := newFileStore(
		FileStoreConfig{StoreDir: sd, BlockSize: 64},
		StreamConfig{Name: "zzz", Subjects: []string{"foo.*.*"}, Storage: FileStorage})
	require_NoError(t, err)
	defer fs.Stop()

	// Make sure we have "foo.222.bar" in multiple blocks to show bug.
	fs.StoreMsg("foo.22.bar", nil, []byte("hello"), 0)
	fs.StoreMsg("foo.22.baz", nil, []byte("ok"), 0)
	fs.StoreMsg("foo.22.baz", nil, []byte("ok"), 0)
	fs.StoreMsg("foo.22.bar", nil, []byte("hello22"), 0)
	require_True(t, fs.numMsgBlocks() > 1)
	sm, err := fs.LoadLastMsg("foo.*.bar", nil)
	require_NoError(t, err)
	require_Equal(t, "hello22", string(sm.msg))
}

// We want to make sure that we update psim correctly on a miss.
func TestFileStoreFilteredPendingPSIMFirstBlockUpdate(t *testing.T) {
	sd := t.TempDir()
	fs, err := newFileStore(
		FileStoreConfig{StoreDir: sd, BlockSize: 512},
		StreamConfig{Name: "zzz", Subjects: []string{"foo.*"}, Storage: FileStorage})
	require_NoError(t, err)
	defer fs.Stop()

	// When PSIM detects msgs == 1 will catch up, so msgs needs to be > 1.
	// Then create a huge block gap.
	msg := []byte("hello")
	fs.StoreMsg("foo.baz", nil, msg, 0)
	for i := 0; i < 1000; i++ {
		fs.StoreMsg("foo.foo", nil, msg, 0)
	}
	// Bookend with 2 more foo.baz
	fs.StoreMsg("foo.baz", nil, msg, 0)
	fs.StoreMsg("foo.baz", nil, msg, 0)
	// Now remove first one.
	removed, err := fs.RemoveMsg(1)
	require_NoError(t, err)
	require_True(t, removed)
	// 84 blocks.
	require_Equal(t, fs.numMsgBlocks(), 84)
	fs.mu.RLock()
	psi, ok := fs.psim.Find([]byte("foo.baz"))
	fs.mu.RUnlock()
	require_True(t, ok)
	require_Equal(t, psi.total, 2)
	require_Equal(t, psi.fblk, 1)
	require_Equal(t, psi.lblk, 84)

	// No make sure that a call to numFilterPending which will initially walk all blocks if starting from seq 1 updates psi.
	var ss SimpleState
	fs.mu.RLock()
	err = fs.numFilteredPending("foo.baz", &ss)
	fs.mu.RUnlock()
	require_NoError(t, err)
	require_Equal(t, ss.Msgs, 2)
	require_Equal(t, ss.First, 1002)
	require_Equal(t, ss.Last, 1003)

	// Check psi was updated. This is done in separate go routine to acquire
	// the write lock now.
	checkFor(t, time.Second, 100*time.Millisecond, func() error {
		fs.mu.RLock()
		psi, ok = fs.psim.Find([]byte("foo.baz"))
		total, fblk, lblk := psi.total, psi.fblk, psi.lblk
		fs.mu.RUnlock()
		require_True(t, ok)
		require_Equal(t, total, 2)
		require_Equal(t, lblk, 84)
		if fblk != 84 {
			return fmt.Errorf("fblk should be 84, still %d", fblk)
		}
		return nil
	})
}

func TestFileStoreWildcardFilteredPendingPSIMFirstBlockUpdate(t *testing.T) {
	sd := t.TempDir()
	fs, err := newFileStore(
		FileStoreConfig{StoreDir: sd, BlockSize: 512},
		StreamConfig{Name: "zzz", Subjects: []string{"foo.*.*"}, Storage: FileStorage})
	require_NoError(t, err)
	defer fs.Stop()

	// When PSIM detects msgs == 1 will catch up, so msgs needs to be > 1.
	// Then create a huge block gap.
	msg := []byte("hello")
	fs.StoreMsg("foo.22.baz", nil, msg, 0)
	fs.StoreMsg("foo.22.bar", nil, msg, 0)
	for i := 0; i < 1000; i++ {
		fs.StoreMsg("foo.1.foo", nil, msg, 0)
	}
	// Bookend with 3 more, two foo.baz and two foo.bar.
	fs.StoreMsg("foo.22.baz", nil, msg, 0)
	fs.StoreMsg("foo.22.baz", nil, msg, 0)
	fs.StoreMsg("foo.22.bar", nil, msg, 0)
	fs.StoreMsg("foo.22.bar", nil, msg, 0)

	// Now remove first one for foo.bar and foo.baz.
	removed, err := fs.RemoveMsg(1)
	require_NoError(t, err)
	require_True(t, removed)
	removed, err = fs.RemoveMsg(2)
	require_NoError(t, err)
	require_True(t, removed)

	// 92 blocks.
	require_Equal(t, fs.numMsgBlocks(), 92)
	fs.mu.RLock()
	psi, ok := fs.psim.Find([]byte("foo.22.baz"))
	total, fblk, lblk := psi.total, psi.fblk, psi.lblk
	fs.mu.RUnlock()
	require_True(t, ok)
	require_Equal(t, total, 2)
	require_Equal(t, fblk, 1)
	require_Equal(t, lblk, 92)

	fs.mu.RLock()
	psi, ok = fs.psim.Find([]byte("foo.22.bar"))
	total, fblk, lblk = psi.total, psi.fblk, psi.lblk
	fs.mu.RUnlock()
	require_True(t, ok)
	require_Equal(t, total, 2)
	require_Equal(t, fblk, 1)
	require_Equal(t, lblk, 92)

	// No make sure that a call to numFilterPending which will initially walk all blocks if starting from seq 1 updates psi.
	var ss SimpleState
	fs.mu.RLock()
	err = fs.numFilteredPending("foo.22.*", &ss)
	fs.mu.RUnlock()
	require_NoError(t, err)
	require_Equal(t, ss.Msgs, 4)
	require_Equal(t, ss.First, 1003)
	require_Equal(t, ss.Last, 1006)

	// Check both psi were updated.
	checkFor(t, time.Second, 100*time.Millisecond, func() error {
		fs.mu.RLock()
		psi, ok = fs.psim.Find([]byte("foo.22.baz"))
		total, fblk, lblk = psi.total, psi.fblk, psi.lblk
		fs.mu.RUnlock()
		require_True(t, ok)
		require_Equal(t, total, 2)
		require_Equal(t, lblk, 92)
		if fblk != 92 {
			return fmt.Errorf("fblk should be 92, still %d", fblk)
		}
		return nil
	})

	checkFor(t, time.Second, 100*time.Millisecond, func() error {
		fs.mu.RLock()
		psi, ok = fs.psim.Find([]byte("foo.22.bar"))
		total, fblk, lblk = psi.total, psi.fblk, psi.lblk
		fs.mu.RUnlock()
		require_True(t, ok)
		require_Equal(t, total, 2)
		require_Equal(t, fblk, 92)
		if fblk != 92 {
			return fmt.Errorf("fblk should be 92, still %d", fblk)
		}
		return nil
	})
}

// Make sure if we only miss by one for fblk that we still update it.
func TestFileStoreFilteredPendingPSIMFirstBlockUpdateNextBlock(t *testing.T) {
	sd := t.TempDir()
	fs, err := newFileStore(
		FileStoreConfig{StoreDir: sd, BlockSize: 128},
		StreamConfig{Name: "zzz", Subjects: []string{"foo.*.*"}, Storage: FileStorage})
	require_NoError(t, err)
	defer fs.Stop()

	msg := []byte("hello")
	// Create 4 blocks, each block holds 2 msgs
	for i := 0; i < 4; i++ {
		fs.StoreMsg("foo.22.bar", nil, msg, 0)
		fs.StoreMsg("foo.22.baz", nil, msg, 0)
	}
	require_Equal(t, fs.numMsgBlocks(), 4)

	fetch := func(subj string) *psi {
		t.Helper()
		fs.mu.RLock()
		var info psi
		psi, ok := fs.psim.Find([]byte(subj))
		if ok && psi != nil {
			info = *psi
		}
		fs.mu.RUnlock()
		require_True(t, ok)
		return &info
	}

	psi := fetch("foo.22.bar")
	require_Equal(t, psi.total, 4)
	require_Equal(t, psi.fblk, 1)
	require_Equal(t, psi.lblk, 4)

	// Now remove first instance of "foo.22.bar"
	removed, err := fs.RemoveMsg(1)
	require_NoError(t, err)
	require_True(t, removed)

	// Call into numFilterePending(), we want to make sure it updates fblk.
	var ss SimpleState
	fs.mu.Lock()
	err = fs.numFilteredPending("foo.22.bar", &ss)
	fs.mu.Unlock()
	require_NoError(t, err)
	require_Equal(t, ss.Msgs, 3)
	require_Equal(t, ss.First, 3)
	require_Equal(t, ss.Last, 7)

	// Now make sure that we properly updated the psim entry.
	checkFor(t, time.Second, 100*time.Millisecond, func() error {
		psi = fetch("foo.22.bar")
		require_Equal(t, psi.total, 3)
		require_Equal(t, psi.lblk, 4)
		if psi.fblk != 2 {
			return fmt.Errorf("fblk should be 2, still %d", psi.fblk)
		}
		return nil
	})

	// Now make sure wildcard calls into also update blks.
	// First remove first "foo.22.baz" which will remove first block.
	removed, err = fs.RemoveMsg(2)
	require_NoError(t, err)
	require_True(t, removed)
	// Make sure 3 blks left.
	require_Equal(t, fs.numMsgBlocks(), 3)

	psi = fetch("foo.22.baz")
	require_Equal(t, psi.total, 3)
	require_Equal(t, psi.fblk, 1)
	require_Equal(t, psi.lblk, 4)

	// Now call wildcard version of numFilteredPending to make sure it clears.
	fs.mu.Lock()
	err = fs.numFilteredPending("foo.*.baz", &ss)
	fs.mu.Unlock()
	require_NoError(t, err)
	require_Equal(t, ss.Msgs, 3)
	require_Equal(t, ss.First, 4)
	require_Equal(t, ss.Last, 8)

	checkFor(t, time.Second, 100*time.Millisecond, func() error {
		psi = fetch("foo.22.baz")
		require_Equal(t, psi.total, 3)
		require_Equal(t, psi.lblk, 4)
		if psi.fblk != 2 {
			return fmt.Errorf("fblk should be 2, still %d", psi.fblk)
		}
		return nil
	})
}

func TestFileStoreLargeSparseMsgsDoNotLoadAfterLast(t *testing.T) {
	sd := t.TempDir()
	fs, err := newFileStore(
		FileStoreConfig{StoreDir: sd, BlockSize: 128},
		StreamConfig{Name: "zzz", Subjects: []string{"foo.*.*"}, Storage: FileStorage})
	require_NoError(t, err)
	defer fs.Stop()

	msg := []byte("hello")
	// Create 2 blocks with each, each block holds 2 msgs
	for i := 0; i < 2; i++ {
		fs.StoreMsg("foo.22.bar", nil, msg, 0)
		fs.StoreMsg("foo.22.baz", nil, msg, 0)
	}
	// Now create 8 more blocks with just baz. So no matches for these 8 blocks
	// for "foo.22.bar".
	for i := 0; i < 8; i++ {
		fs.StoreMsg("foo.22.baz", nil, msg, 0)
		fs.StoreMsg("foo.22.baz", nil, msg, 0)
	}
	require_Equal(t, fs.numMsgBlocks(), 10)

	// Remove all blk cache and fss.
	fs.mu.RLock()
	for _, mb := range fs.blks {
		mb.mu.Lock()
		mb.fss, mb.cache = nil, nil
		mb.mu.Unlock()
	}
	fs.mu.RUnlock()

	// "foo.22.bar" is at sequence 1 and 3.
	// Make sure if we do a LoadNextMsg() starting at 4 that we do not load
	// all the tail blocks.
	_, _, err = fs.LoadNextMsg("foo.*.bar", true, 4, nil)
	require_Error(t, err, ErrStoreEOF)

	// Now make sure we did not load fss and cache.
	var loaded int
	fs.mu.RLock()
	for _, mb := range fs.blks {
		mb.mu.RLock()
		if mb.cache != nil || mb.fss != nil {
			loaded++
		}
		mb.mu.RUnlock()
	}
	fs.mu.RUnlock()
	// We will load first block for starting seq 4, but no others should have loaded.
	require_Equal(t, loaded, 1)
}

func TestFileStoreCheckSkipFirstBlockBug(t *testing.T) {
	sd := t.TempDir()
	fs, err := newFileStore(
		FileStoreConfig{StoreDir: sd, BlockSize: 128},
		StreamConfig{Name: "zzz", Subjects: []string{"foo.*.*"}, Storage: FileStorage})
	require_NoError(t, err)
	defer fs.Stop()

	msg := []byte("hello")

	fs.StoreMsg("foo.BB.bar", nil, msg, 0)
	fs.StoreMsg("foo.BB.bar", nil, msg, 0)
	fs.StoreMsg("foo.AA.bar", nil, msg, 0)
	for i := 0; i < 5; i++ {
		fs.StoreMsg("foo.BB.bar", nil, msg, 0)
	}
	fs.StoreMsg("foo.AA.bar", nil, msg, 0)
	fs.StoreMsg("foo.AA.bar", nil, msg, 0)

	// Should have created 4 blocks.
	// BB BB | AA BB | BB BB | BB BB | AA AA
	require_Equal(t, fs.numMsgBlocks(), 5)

	fs.RemoveMsg(3)
	fs.RemoveMsg(4)

	// Second block should be gone now.
	// BB BB | -- -- | BB BB | BB BB | AA AA
	require_Equal(t, fs.numMsgBlocks(), 4)

	_, _, err = fs.LoadNextMsg("foo.AA.bar", false, 4, nil)
	require_NoError(t, err)
}

// https://github.com/nats-io/nats-server/issues/5702
func TestFileStoreTombstoneRbytes(t *testing.T) {
	fs, err := newFileStore(
		FileStoreConfig{StoreDir: t.TempDir(), BlockSize: 1024},
		StreamConfig{Name: "zzz", Subjects: []string{"foo.*"}, Storage: FileStorage})
	require_NoError(t, err)
	defer fs.Stop()

	// Block can hold 24 msgs.
	// So will fill one block and half of the other
	msg := []byte("hello")
	for i := 0; i < 34; i++ {
		fs.StoreMsg("foo.22", nil, msg, 0)
	}
	require_True(t, fs.numMsgBlocks() > 1)
	// Now delete second half of first block which will place tombstones in second blk.
	for seq := 11; seq <= 24; seq++ {
		fs.RemoveMsg(uint64(seq))
	}
	// Now check that rbytes has been properly accounted for in second block.
	fs.mu.RLock()
	blk := fs.blks[1]
	fs.mu.RUnlock()

	blk.mu.RLock()
	bytes, rbytes := blk.bytes, blk.rbytes
	blk.mu.RUnlock()
	require_True(t, rbytes > bytes)
}

func TestFileStoreMsgBlockShouldCompact(t *testing.T) {
	fs, err := newFileStore(
		FileStoreConfig{StoreDir: t.TempDir()},
		StreamConfig{Name: "zzz", Subjects: []string{"foo.*"}, Storage: FileStorage})
	require_NoError(t, err)
	defer fs.Stop()

	// 127 fit into a block.
	msg := bytes.Repeat([]byte("Z"), 64*1024)
	for i := 0; i < 190; i++ {
		fs.StoreMsg("foo.22", nil, msg, 0)
	}
	require_True(t, fs.numMsgBlocks() > 1)
	// Now delete second half of first block which will place tombstones in second blk.
	for seq := 64; seq <= 127; seq++ {
		fs.RemoveMsg(uint64(seq))
	}
	fs.mu.RLock()
	fblk := fs.blks[0]
	sblk := fs.blks[1]
	fs.mu.RUnlock()

	fblk.mu.RLock()
	bytes, rbytes := fblk.bytes, fblk.rbytes
	shouldCompact := fblk.shouldCompactInline()
	fblk.mu.RUnlock()
	// Should have tripped compaction already.
	require_Equal(t, bytes, rbytes)
	require_False(t, shouldCompact)

	sblk.mu.RLock()
	shouldCompact = sblk.shouldCompactInline()
	sblk.mu.RUnlock()
	require_False(t, shouldCompact)
}

func TestFileStoreInlineCompactionSync(t *testing.T) {
	for _, test := range []struct {
		name        string
		syncAlways  bool
		syncOnFlush bool
		needSync    bool
	}{
		{"no_sync", false, false, true},
		{"sync_always", true, false, false},
		{"sync_on_flush", true, true, false},
	} {
		t.Run(test.name, func(t *testing.T) {
			fs, err := newFileStore(
				FileStoreConfig{
					StoreDir:     t.TempDir(),
					BlockSize:    3 * 1024 * 1024,
					SyncInterval: time.Hour,
					SyncAlways:   test.syncAlways,
					SyncOnFlush:  test.syncOnFlush,
				},
				StreamConfig{Name: "zzz", Subjects: []string{"foo"}, Storage: FileStorage},
			)
			require_NoError(t, err)
			defer fs.Stop()

			// Eleven records fit in the first block and put it over the inline
			// compaction minimum. The twelfth record creates an active block.
			msg := bytes.Repeat([]byte("Z"), 256*1024)
			for range 12 {
				_, _, err = fs.StoreMsg("foo", nil, msg, 0)
				require_NoError(t, err)
			}
			require_Equal(t, fs.numMsgBlocks(), 2)

			// Use syncBlocks to make sure mb.needSync
			// is cleared on all blocks
			fs.syncBlocks()
			fs.mu.RLock()
			fmb := fs.blks[0]
			fmb.mu.RLock()
			oldRawbytes, needSync := fmb.rbytes, fmb.needSync
			fmb.mu.RUnlock()
			fs.mu.RUnlock()

			require_True(t, oldRawbytes > compactMinimum)
			require_False(t, needSync)

			// The final removal makes the first block less than half full and
			// compacts it inline before RemoveMsg returns.
			for seq := uint64(2); seq <= 7; seq++ {
				removed, err := fs.RemoveMsg(seq)
				require_True(t, removed)
				require_NoError(t, err)
			}

			fmb.mu.RLock()
			newRawbytes, needSync := fmb.rbytes, fmb.needSync
			fmb.mu.RUnlock()
			// Compaction happened
			require_LessThan(t, newRawbytes, oldRawbytes)
			// Durability modes should sync inline compaction.
			require_Equal(t, needSync, test.needSync)
		})
	}
}

func TestFileStoreCheckSkipFirstBlockNotLoadOldBlocks(t *testing.T) {
	sd := t.TempDir()
	fs, err := newFileStore(
		FileStoreConfig{StoreDir: sd, BlockSize: 128},
		StreamConfig{Name: "zzz", Subjects: []string{"foo.*.*"}, Storage: FileStorage})
	require_NoError(t, err)
	defer fs.Stop()

	msg := []byte("hello")

	fs.StoreMsg("foo.BB.bar", nil, msg, 0)
	fs.StoreMsg("foo.AA.bar", nil, msg, 0)
	for i := 0; i < 6; i++ {
		fs.StoreMsg("foo.BB.bar", nil, msg, 0)
	}
	fs.StoreMsg("foo.AA.bar", nil, msg, 0) // Sequence 9
	fs.StoreMsg("foo.AA.bar", nil, msg, 0) // Sequence 10

	for i := 0; i < 4; i++ {
		fs.StoreMsg("foo.BB.bar", nil, msg, 0)
	}

	// Should have created 7 blocks.
	// BB AA | BB BB | BB BB | BB BB | AA AA | BB BB | BB BB
	require_Equal(t, fs.numMsgBlocks(), 7)

	fs.RemoveMsg(1)
	fs.RemoveMsg(2)

	// First block should be gone now.
	// -- -- | BB BB | BB BB | BB BB | AA AA | BB BB | BB BB
	require_Equal(t, fs.numMsgBlocks(), 6)

	// Remove all blk cache and fss.
	fs.mu.RLock()
	for _, mb := range fs.blks {
		mb.mu.Lock()
		mb.fss, mb.cache = nil, nil
		mb.mu.Unlock()
	}
	fs.mu.RUnlock()

	// But this means that the psim still points fblk to block 1.
	// So when we try to load AA from near the end (last AA sequence), it will not find anything and will then
	// check if we can skip ahead, but in the process reload blocks 2, 3, 4 amd 5..
	// This can trigger for an up to date consumer near the end of the stream that gets a new pull request that will pop it out of msgWait
	// and it will call LoadNextMsg() like we do here with starting sequence of 11.
	_, _, err = fs.LoadNextMsg("foo.AA.bar", false, 11, nil)
	require_Error(t, err, ErrStoreEOF)

	// Now make sure we did not load fss and cache.
	var loaded int
	fs.mu.RLock()
	for _, mb := range fs.blks {
		mb.mu.RLock()
		if mb.cache != nil || mb.fss != nil {
			loaded++
		}
		mb.mu.RUnlock()
	}
	fs.mu.RUnlock()
	// We will load last block for starting seq 9, but no others should have loaded.
	require_Equal(t, loaded, 1)
}

func TestFileStoreSyncCompressOnlyIfDirty(t *testing.T) {
	sd := t.TempDir()
	fs, err := newFileStore(
		FileStoreConfig{StoreDir: sd, BlockSize: 256, SyncInterval: 250 * time.Millisecond},
		StreamConfig{Name: "zzz", Subjects: []string{"foo.*"}, Storage: FileStorage})
	require_NoError(t, err)
	defer fs.Stop()

	msg := []byte("hello")

	// 6 msgs per block.
	// Fill 2 blocks.
	for i := 0; i < 12; i++ {
		fs.StoreMsg("foo.BB", nil, msg, 0)
	}
	// Create third block with just one message in it.
	fs.StoreMsg("foo.BB", nil, msg, 0)

	// Should have created 3 blocks.
	require_Equal(t, fs.numMsgBlocks(), 3)

	// Now delete a bunch that will will fill up 3 block with tombstones.
	for _, seq := range []uint64{2, 3, 4, 5, 8, 9, 10, 11} {
		_, err = fs.RemoveMsg(seq)
		require_NoError(t, err)
	}
	// Now make sure we add 4/5th block so syncBlocks will try to compact.
	for i := 0; i < 6; i++ {
		fs.StoreMsg("foo.BB", nil, msg, 0)
	}
	require_Equal(t, fs.numMsgBlocks(), 5)

	// All should have compact set.
	fs.mu.Lock()
	// Only check first 3 blocks.
	for i := 0; i < 3; i++ {
		mb := fs.blks[i]
		mb.mu.Lock()
		shouldCompact := mb.shouldCompactSync()
		mb.mu.Unlock()
		if !shouldCompact {
			fs.mu.Unlock()
			t.Fatalf("Expected should compact to be true for %d, got false", mb.getIndex())
		}
	}
	fs.mu.Unlock()

	// We want to make sure the last block, which is filled with tombstones and is not compactable, returns false now.
	checkFor(t, 2*time.Second, 20*time.Millisecond, func() error {
		fs.mu.Lock()
		defer fs.mu.Unlock()
		for _, mb := range fs.blks {
			mb.mu.Lock()
			index, shouldCompact := mb.index, mb.shouldCompactSync()
			mb.mu.Unlock()
			if shouldCompact {
				return fmt.Errorf("Expected should compact to be false for %d, got true", index)
			}
		}
		return nil
	})

	// Now remove some from block 3 and verify that compact is not suppressed.
	_, err = fs.RemoveMsg(13)
	require_NoError(t, err)

	fs.mu.Lock()
	mb := fs.blks[2] // block 3.
	mb.mu.Lock()
	noCompact := mb.noCompact
	mb.mu.Unlock()
	fs.mu.Unlock()
	// Verify that since we deleted a message we should be considered for compaction again in syncBlocks().
	require_False(t, noCompact)
}

func TestFileStoreSyncBlocksSyncsCompaction(t *testing.T) {
	fs, err := newFileStore(
		FileStoreConfig{StoreDir: t.TempDir(), BlockSize: 256, SyncInterval: time.Hour},
		StreamConfig{Name: "zzz", Subjects: []string{"foo.*"}, Storage: FileStorage},
	)
	require_NoError(t, err)
	defer fs.Stop()

	// Create two full blocks and one active block.
	for range 13 {
		_, _, err = fs.StoreMsg("foo.BB", nil, []byte("hello"), 0)
		require_NoError(t, err)
	}
	require_Equal(t, fs.numMsgBlocks(), 3)

	checkAllNeedSync := func(expected bool) {
		t.Helper()
		fs.mu.RLock()
		blks := append([]*msgBlock(nil), fs.blks...)
		fs.mu.RUnlock()
		for _, mb := range blks {
			mb.mu.RLock()
			needSync := mb.needSync
			mb.mu.RUnlock()
			require_Equal(t, needSync, expected)
		}
	}

	// Start with all blocks synced.
	checkAllNeedSync(true)
	fs.syncBlocks()
	checkAllNeedSync(false)

	fs.mu.RLock()
	mb := fs.blks[0]
	fs.mu.RUnlock()

	// Logical deletes make the synced block compactable without dirtying its file.
	for _, seq := range []uint64{2, 3, 4, 5} {
		_, err = fs.RemoveMsg(seq)
		require_NoError(t, err)
	}
	mb.mu.RLock()
	shouldCompact, needSync, rbytes := mb.shouldCompactSync(), mb.needSync, mb.rbytes
	mb.mu.RUnlock()
	require_True(t, shouldCompact)
	require_False(t, needSync)

	// The pass that compacts the block must also sync the replacement.
	fs.syncBlocks()
	mb.mu.RLock()
	newRbytes := mb.rbytes
	mb.mu.RUnlock()
	require_LessThan(t, newRbytes, rbytes)
	checkAllNeedSync(false)
}

// This test is for deleted interior message tracking after compaction from limits based deletes, meaning no tombstones.
// Bug was that dmap would not be properly be hydrated after the compact from rebuild. But we did so in populateGlobalInfo.
// So this is just to fix a bug in rebuildState tracking gaps after a compact.
func TestFileStoreDmapBlockRecoverAfterCompact(t *testing.T) {
	sd := t.TempDir()
	fs, err := newFileStore(
		FileStoreConfig{StoreDir: sd, BlockSize: 256},
		StreamConfig{Name: "zzz", Subjects: []string{"foo.*"}, Storage: FileStorage, MaxMsgsPer: 1})
	require_NoError(t, err)
	defer fs.Stop()

	msg := []byte("hello")

	// 6 msgs per block.
	// Fill the first block.
	for i := 1; i <= 6; i++ {
		fs.StoreMsg(fmt.Sprintf("foo.%d", i), nil, msg, 0)
	}
	require_Equal(t, fs.numMsgBlocks(), 1)

	// Now create holes in the first block via the max msgs per subject of 1.
	for i := 2; i < 6; i++ {
		fs.StoreMsg(fmt.Sprintf("foo.%d", i), nil, msg, 0)
	}
	require_Equal(t, fs.numMsgBlocks(), 2)
	// Compact and rebuild the first blk. Do not have it call indexCacheBuf which will fix it up.
	mb := fs.getFirstBlock()
	mb.mu.Lock()
	if err = mb.compact(); err != nil {
		mb.mu.Unlock()
		require_NoError(t, err)
	}
	// Empty out dmap state.
	mb.dmap.Empty()
	ld, tombs, err := mb.rebuildStateLocked()
	dmap := mb.dmap.Clone()
	mb.mu.Unlock()

	require_NoError(t, err)
	require_Equal(t, ld, nil)
	require_Equal(t, len(tombs), 0)
	require_Equal(t, dmap.Size(), 4)
}

func TestFileStoreRestoreIndexWithMatchButLeftOverBlocks(t *testing.T) {
	sd := t.TempDir()
	fs, err := newFileStore(
		FileStoreConfig{StoreDir: sd, BlockSize: 256},
		StreamConfig{Name: "zzz", Subjects: []string{"foo.*"}, Storage: FileStorage, MaxMsgsPer: 1})
	require_NoError(t, err)
	defer fs.Stop()

	msg := []byte("hello")

	// 6 msgs per block.
	// Fill the first 2 blocks.
	for i := 1; i <= 12; i++ {
		fs.StoreMsg(fmt.Sprintf("foo.%d", i), nil, msg, 0)
	}
	require_Equal(t, fs.numMsgBlocks(), 2)

	// We will now stop which will create the index.db file which will
	// match the last record exactly.
	sfile := filepath.Join(sd, msgDir, streamStreamStateFile)
	fs.Stop()

	// Grab it since we will put it back.
	buf, err := os.ReadFile(sfile)
	require_NoError(t, err)
	require_True(t, len(buf) > 0)

	// Now do an additional block, but with the MaxMsgsPer this will remove the first block,
	// but leave the second so on recovery will match the checksum for the last msg in second block.

	fs, err = newFileStore(
		FileStoreConfig{StoreDir: sd, BlockSize: 256},
		StreamConfig{Name: "zzz", Subjects: []string{"foo.*"}, Storage: FileStorage, MaxMsgsPer: 1})
	require_NoError(t, err)
	defer fs.Stop()

	for i := 1; i <= 6; i++ {
		fs.StoreMsg(fmt.Sprintf("foo.%d", i), nil, msg, 0)
	}

	// Grab correct state, we will use it to make sure we do the right thing.
	var state StreamState
	fs.FastState(&state)

	require_Equal(t, state.Msgs, 12)
	require_Equal(t, state.FirstSeq, 7)
	require_Equal(t, state.LastSeq, 18)
	// This will be block 2 and 3.
	require_Equal(t, fs.numMsgBlocks(), 2)

	fs.Stop()
	// Put old stream state back.
	require_NoError(t, os.WriteFile(sfile, buf, defaultFilePerms))

	fs, err = newFileStore(
		FileStoreConfig{StoreDir: sd, BlockSize: 256},
		StreamConfig{Name: "zzz", Subjects: []string{"foo.*"}, Storage: FileStorage, MaxMsgsPer: 1})
	require_NoError(t, err)
	defer fs.Stop()

	fs.FastState(&state)
	require_Equal(t, state.Msgs, 12)
	require_Equal(t, state.FirstSeq, 7)
	require_Equal(t, state.LastSeq, 18)
}

func TestFileStoreRestoreDeleteTombstonesExceedingMaxBlkSize(t *testing.T) {
	testFileStoreAllPermutations(t, func(t *testing.T, fcfg FileStoreConfig) {
		fcfg.BlockSize = 256
		fs, err := newFileStoreWithCreated(
			fcfg,
			StreamConfig{Name: "zzz", Subjects: []string{"foo.*"}, Storage: FileStorage},
			time.Now(),
			prf(&fcfg),
			nil,
		)
		require_NoError(t, err)
		defer fs.Stop()

		n, err := fs.PurgeEx(_EMPTY_, 1_000_000_000, 0)
		require_NoError(t, err)
		require_Equal(t, n, 0)

		msg := []byte("hello")
		// 6 msgs per block with blk size 256.
		for i := 1; i <= 10_000; i++ {
			fs.StoreMsg(fmt.Sprintf("foo.%d", i), nil, msg, 0)
		}
		// Now delete msgs which will write tombstones.
		for seq := uint64(1_000_000_001); seq < 1_000_000_101; seq++ {
			removed, err := fs.RemoveMsg(seq)
			require_NoError(t, err)
			require_True(t, removed)
		}

		// Check last block and make sure the tombstones did not exceed blk size maximum.
		// Check to make sure no blocks exceed blk size.
		fs.mu.RLock()
		blks := append([]*msgBlock(nil), fs.blks...)
		lmb := fs.lmb
		fs.mu.RUnlock()

		var emptyBlks []*msgBlock
		for _, mb := range blks {
			mb.mu.RLock()
			bytes, rbytes := mb.bytes, mb.rbytes
			mb.mu.RUnlock()
			require_True(t, bytes < 256)
			require_True(t, rbytes < 256)
			if bytes == 0 && mb != lmb {
				emptyBlks = append(emptyBlks, mb)
			}
		}
		// Check each block such that it signals it can be compacted but if we attempt compact here nothing should change.
		for _, mb := range emptyBlks {
			mb.mu.Lock()
			mb.ensureRawBytesLoaded()
			bytes, rbytes, shouldCompact := mb.bytes, mb.rbytes, mb.shouldCompactSync()
			// Do the compact and make sure nothing changed.
			err = mb.compact()
			nbytes, nrbytes := mb.bytes, mb.rbytes
			mb.mu.Unlock()
			require_NoError(t, err)
			require_True(t, shouldCompact)
			require_Equal(t, bytes, nbytes)
			require_Equal(t, rbytes, nrbytes)
		}

		// Now remove first msg which will invalidate the tombstones since they will be < first sequence.
		removed, err := fs.RemoveMsg(1_000_000_000)
		require_NoError(t, err)
		require_True(t, removed)

		// Now simulate a syncBlocks call and make sure it cleans up the tombstones that are no longer relevant.
		fs.syncBlocks()
		for _, mb := range emptyBlks {
			mb.mu.Lock()
			mb.ensureRawBytesLoaded()
			index, bytes, rbytes := mb.index, mb.bytes, mb.rbytes
			mb.mu.Unlock()
			require_Equal(t, bytes, 0)
			require_Equal(t, rbytes, 0)
			// Also make sure we removed these blks all together.
			fs.mu.RLock()
			imb := fs.bim[index]
			fs.mu.RUnlock()
			require_True(t, imb == nil)
		}
	})
}

///////////////////////////////////////////////////////////////////////////
// Benchmarks
///////////////////////////////////////////////////////////////////////////

func Benchmark_FileStoreSelectMsgBlock(b *testing.B) {
	// We use small block size to create lots of blocks for this test.
	fs, err := newFileStore(
		FileStoreConfig{StoreDir: b.TempDir(), BlockSize: 128},
		StreamConfig{Name: "zzz", Subjects: []string{"*"}, Storage: FileStorage})
	require_NoError(b, err)
	defer fs.Stop()

	subj, msg := "A", bytes.Repeat([]byte("ABC"), 33) // ~100bytes

	// Add in a bunch of blocks.
	for i := 0; i < 1000; i++ {
		fs.StoreMsg(subj, nil, msg, 0)
	}
	if fs.numMsgBlocks() < 1000 {
		b.Fatalf("Expected at least 1000 blocks, got %d", fs.numMsgBlocks())
	}

	fs.mu.RLock()
	defer fs.mu.RUnlock()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, mb := fs.selectMsgBlockWithIndex(1)
		if mb == nil {
			b.Fatalf("Expected a non-nil mb")
		}
	}
	b.StopTimer()
}

func Benchmark_FileStoreLoadNextMsgSameFilterAsStream(b *testing.B) {
	fs, err := newFileStore(
		FileStoreConfig{StoreDir: b.TempDir()},
		StreamConfig{Name: "zzz", Subjects: []string{"foo.*"}, Storage: FileStorage})
	require_NoError(b, err)
	defer fs.Stop()

	// Small om purpose.
	msg := []byte("ok")

	// Add in a bunch of msgs
	for i := 0; i < 100_000; i++ {
		subj := fmt.Sprintf("foo.%d", rand.IntN(1024))
		fs.StoreMsg(subj, nil, msg, 0)
	}

	b.ResetTimer()

	var smv StoreMsg
	for i := 0; i < b.N; i++ {
		// Needs to start at ~1 to show slowdown.
		_, _, err := fs.LoadNextMsg("foo.*", true, 10, &smv)
		require_NoError(b, err)
	}
}

func Benchmark_FileStoreLoadNextMsgLiteralSubject(b *testing.B) {
	fs, err := newFileStore(
		FileStoreConfig{StoreDir: b.TempDir()},
		StreamConfig{Name: "zzz", Subjects: []string{"foo.*"}, Storage: FileStorage})
	require_NoError(b, err)
	defer fs.Stop()

	// Small om purpose.
	msg := []byte("ok")

	// Add in a bunch of msgs
	for i := 0; i < 100_000; i++ {
		subj := fmt.Sprintf("foo.%d", rand.IntN(1024))
		fs.StoreMsg(subj, nil, msg, 0)
	}
	// This is the one we will try to match.
	fs.StoreMsg("foo.2222", nil, msg, 0)
	// So not last and we think we are done linear scan.
	fs.StoreMsg("foo.3333", nil, msg, 0)

	b.ResetTimer()

	var smv StoreMsg
	for i := 0; i < b.N; i++ {
		_, _, err := fs.LoadNextMsg("foo.2222", false, 10, &smv)
		require_NoError(b, err)
	}
}

func Benchmark_FileStoreLoadNextMsgNoMsgsFirstSeq(b *testing.B) {
	fs, err := newFileStore(
		FileStoreConfig{StoreDir: b.TempDir(), BlockSize: 8192},
		StreamConfig{Name: "zzz", Subjects: []string{"foo.*"}, Storage: FileStorage})
	require_NoError(b, err)
	defer fs.Stop()

	// Small om purpose.
	msg := []byte("ok")

	// Add in a bunch of msgs
	for i := 0; i < 1_000_000; i++ {
		fs.StoreMsg("foo.bar", nil, msg, 0)
	}

	b.ResetTimer()

	var smv StoreMsg
	for i := 0; i < b.N; i++ {
		// This should error with EOF
		_, _, err := fs.LoadNextMsg("foo.baz", false, 1, &smv)
		if err != ErrStoreEOF {
			b.Fatalf("Wrong error, expected EOF got %v", err)
		}
	}
}

func Benchmark_FileStoreLoadNextMsgNoMsgsNotFirstSeq(b *testing.B) {
	fs, err := newFileStore(
		FileStoreConfig{StoreDir: b.TempDir(), BlockSize: 8192},
		StreamConfig{Name: "zzz", Subjects: []string{"foo.*"}, Storage: FileStorage})
	require_NoError(b, err)
	defer fs.Stop()

	// Small om purpose.
	msg := []byte("ok")

	// Add in a bunch of msgs
	for i := 0; i < 1_000_000; i++ {
		fs.StoreMsg("foo.bar", nil, msg, 0)
	}

	b.ResetTimer()

	var smv StoreMsg
	for i := 0; i < b.N; i++ {
		// This should error with EOF
		// Make sure the sequence is not first seq of 1.
		_, _, err := fs.LoadNextMsg("foo.baz", false, 10, &smv)
		if err != ErrStoreEOF {
			b.Fatalf("Wrong error, expected EOF got %v", err)
		}
	}
}

func Benchmark_FileStoreLoadNextMsgVerySparseMsgsFirstSeq(b *testing.B) {
	fs, err := newFileStore(
		FileStoreConfig{StoreDir: b.TempDir(), BlockSize: 8192},
		StreamConfig{Name: "zzz", Subjects: []string{"foo.*"}, Storage: FileStorage})
	require_NoError(b, err)
	defer fs.Stop()

	// Small om purpose.
	msg := []byte("ok")

	// Add in a bunch of msgs
	for i := 0; i < 1_000_000; i++ {
		fs.StoreMsg("foo.bar", nil, msg, 0)
	}
	// Make last msg one that would match.
	fs.StoreMsg("foo.baz", nil, msg, 0)

	b.ResetTimer()

	var smv StoreMsg
	for i := 0; i < b.N; i++ {
		_, _, err := fs.LoadNextMsg("foo.baz", false, 1, &smv)
		require_NoError(b, err)
	}
}

func Benchmark_FileStoreLoadNextMsgVerySparseMsgsNotFirstSeq(b *testing.B) {
	fs, err := newFileStore(
		FileStoreConfig{StoreDir: b.TempDir(), BlockSize: 8192},
		StreamConfig{Name: "zzz", Subjects: []string{"foo.*"}, Storage: FileStorage})
	require_NoError(b, err)
	defer fs.Stop()

	// Small om purpose.
	msg := []byte("ok")

	// Add in a bunch of msgs
	for i := 0; i < 1_000_000; i++ {
		fs.StoreMsg("foo.bar", nil, msg, 0)
	}
	// Make last msg one that would match.
	fs.StoreMsg("foo.baz", nil, msg, 0)

	b.ResetTimer()

	var smv StoreMsg
	for i := 0; i < b.N; i++ {
		// Make sure not first seq.
		_, _, err := fs.LoadNextMsg("foo.baz", false, 10, &smv)
		require_NoError(b, err)
	}
}

func Benchmark_FileStoreLoadNextMsgVerySparseMsgsInBetween(b *testing.B) {
	fs, err := newFileStore(
		FileStoreConfig{StoreDir: b.TempDir(), BlockSize: 8192},
		StreamConfig{Name: "zzz", Subjects: []string{"foo.*"}, Storage: FileStorage})
	require_NoError(b, err)
	defer fs.Stop()

	// Small om purpose.
	msg := []byte("ok")

	// Make first msg one that would match as well.
	fs.StoreMsg("foo.baz", nil, msg, 0)
	// Add in a bunch of msgs
	for i := 0; i < 1_000_000; i++ {
		fs.StoreMsg("foo.bar", nil, msg, 0)
	}
	// Make last msg one that would match as well.
	fs.StoreMsg("foo.baz", nil, msg, 0)

	b.ResetTimer()

	var smv StoreMsg
	for i := 0; i < b.N; i++ {
		// Make sure not first seq.
		_, _, err := fs.LoadNextMsg("foo.baz", false, 2, &smv)
		require_NoError(b, err)
	}
}

func Benchmark_FileStoreLoadNextMsgVerySparseMsgsInBetweenWithWildcard(b *testing.B) {
	fs, err := newFileStore(
		FileStoreConfig{StoreDir: b.TempDir()},
		StreamConfig{Name: "zzz", Subjects: []string{"foo.*.*"}, Storage: FileStorage})
	require_NoError(b, err)
	defer fs.Stop()

	// Small om purpose.
	msg := []byte("ok")

	// Make first msg one that would match as well.
	fs.StoreMsg("foo.1.baz", nil, msg, 0)
	// Add in a bunch of msgs.
	// We need to make sure we have a range of subjects that could kick in a linear scan.
	for i := 0; i < 1_000_000; i++ {
		subj := fmt.Sprintf("foo.%d.bar", rand.IntN(100_000)+2)
		fs.StoreMsg(subj, nil, msg, 0)
	}
	// Make last msg one that would match as well.
	fs.StoreMsg("foo.1.baz", nil, msg, 0)

	b.ResetTimer()

	var smv StoreMsg
	for i := 0; i < b.N; i++ {
		// Make sure not first seq.
		_, _, err := fs.LoadNextMsg("foo.*.baz", true, 2, &smv)
		require_NoError(b, err)
	}
}

func Benchmark_FileStoreLoadNextManySubjectsWithWildcardNearLastBlock(b *testing.B) {
	fs, err := newFileStore(
		FileStoreConfig{StoreDir: b.TempDir()},
		StreamConfig{Name: "zzz", Subjects: []string{"foo.*.*"}, Storage: FileStorage})
	require_NoError(b, err)
	defer fs.Stop()

	// Small om purpose.
	msg := []byte("ok")

	// Make first msg one that would match as well.
	fs.StoreMsg("foo.1.baz", nil, msg, 0)
	// Add in a bunch of msgs.
	// We need to make sure we have a range of subjects that could kick in a linear scan.
	for i := 0; i < 1_000_000; i++ {
		subj := fmt.Sprintf("foo.%d.bar", rand.IntN(100_000)+2)
		fs.StoreMsg(subj, nil, msg, 0)
	}
	// Make last msg one that would match as well.
	fs.StoreMsg("foo.1.baz", nil, msg, 0)

	b.ResetTimer()

	var smv StoreMsg
	for i := 0; i < b.N; i++ {
		// Make sure not first seq.
		_, _, err := fs.LoadNextMsg("foo.*.baz", true, 999_990, &smv)
		require_NoError(b, err)
	}
}

func Benchmark_FileStoreLoadNextMsgVerySparseMsgsLargeTail(b *testing.B) {
	fs, err := newFileStore(
		FileStoreConfig{StoreDir: b.TempDir()},
		StreamConfig{Name: "zzz", Subjects: []string{"foo.*.*"}, Storage: FileStorage})
	require_NoError(b, err)
	defer fs.Stop()

	// Small om purpose.
	msg := []byte("ok")

	// Make first msg one that would match as well.
	fs.StoreMsg("foo.1.baz", nil, msg, 0)
	// Add in a bunch of msgs.
	// We need to make sure we have a range of subjects that could kick in a linear scan.
	for i := 0; i < 1_000_000; i++ {
		subj := fmt.Sprintf("foo.%d.bar", rand.IntN(64_000)+2)
		fs.StoreMsg(subj, nil, msg, 0)
	}

	b.ResetTimer()

	var smv StoreMsg
	for i := 0; i < b.N; i++ {
		// Make sure not first seq.
		_, _, err := fs.LoadNextMsg("foo.*.baz", true, 2, &smv)
		require_Error(b, err, ErrStoreEOF)
	}
}

func Benchmark_FileStoreCreateConsumerStores(b *testing.B) {
	for _, syncAlways := range []bool{true, false} {
		b.Run(fmt.Sprintf("%v", syncAlways), func(b *testing.B) {
			fs, err := newFileStore(
				FileStoreConfig{StoreDir: b.TempDir(), SyncAlways: syncAlways},
				StreamConfig{Name: "zzz", Subjects: []string{"foo.*.*"}, Storage: FileStorage})
			require_NoError(b, err)
			defer fs.Stop()

			oconfig := ConsumerConfig{
				DeliverSubject: "d",
				FilterSubject:  "foo",
				AckPolicy:      AckAll,
			}

			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				oname := fmt.Sprintf("obs22_%d", i)
				ofs, err := fs.ConsumerStore(oname, time.Time{}, &oconfig)
				require_NoError(b, err)
				require_NoError(b, ofs.Stop())
			}
		})
	}
}

func BenchmarkFileStoreLoadNextMsgMultiRepeatedSubjects(b *testing.B) {
	for _, test := range []struct {
		name   string
		keys   int
		msgs   int
		pad    int
		blocks int
	}{
		{"RepeatedMoreSubjects", 4000, 40000, 0, 1},
		{"RepeatedAcrossBlocks", 2000, 16000, 2048, 5},
		{"Unique", 20000, 80000, 0, 1},
	} {
		b.Run(test.name, func(b *testing.B) {
			fs, err := newFileStore(
				FileStoreConfig{StoreDir: b.TempDir(), BlockSize: 8 * 1024 * 1024},
				StreamConfig{Name: "bench", Subjects: []string{"data.*.*"}, Storage: FileStorage})
			require_NoError(b, err)
			defer fs.Stop()

			payload := append([]byte(`{"v": 1}`), bytes.Repeat([]byte(" "), test.pad)...)
			for i := range test.msgs {
				subj := fmt.Sprintf("data.%016x.k%d", (i/4)%test.keys, i%4+1)
				_, _, err := fs.StoreMsg(subj, nil, payload, 0)
				require_NoError(b, err)
			}
			require_Equal(b, fs.numMsgBlocks(), test.blocks)

			for _, multi := range []bool{true, false} {
				name := "Single"
				sl := gsl.NewSublist[struct{}]()
				if multi {
					name = "Multi"
					for kind := 1; kind <= 4; kind++ {
						require_NoError(b, sl.Insert(fmt.Sprintf("data.*.k%d", kind), struct{}{}))
					}
				} else {
					require_NoError(b, sl.Insert("data.*.*", struct{}{}))
				}
				b.Run(name, func(b *testing.B) {
					var smv StoreMsg
					b.ReportAllocs()
					b.ResetTimer()
					for range b.N {
						for seq := uint64(1); seq <= uint64(test.msgs); seq++ {
							sm, nseq, err := fs.LoadNextMsgMulti(sl, seq, &smv)
							if err != nil {
								b.Fatal(err)
							}
							if nseq != seq || sm.seq != seq {
								b.Fatalf("Expected sequence %d, got %d (%d)", seq, nseq, sm.seq)
							}
						}
					}
				})
			}
		})
	}
}

func TestFileStoreMaxMsgsPerSubjectOneStaleFblkAfterRestart(t *testing.T) {
	sd := t.TempDir()
	fcfg := FileStoreConfig{StoreDir: sd, BlockSize: 256}
	scfg := StreamConfig{Name: "zzz", Subjects: []string{"foo.*"}, Storage: FileStorage, MaxMsgsPer: 1}

	fs, err := newFileStore(fcfg, scfg)
	require_NoError(t, err)
	defer fs.Stop()

	msg := []byte("hello")

	// Store "foo.0" into the first block.
	_, _, err = fs.StoreMsg("foo.0", nil, msg, 0)
	require_NoError(t, err)

	// Fill the first block with other subjects until a second block is created.
	for i := 1; fs.numMsgBlocks() < 2; i++ {
		_, _, err = fs.StoreMsg(fmt.Sprintf("foo.%d", i), nil, msg, 0)
		require_NoError(t, err)
	}

	// Store "foo.0" again. The message lands in the second block, and MaxMsgsPer=1 removes
	// the copy in the first block. lblk should now point to the last block.
	seq, _, err := fs.StoreMsg("foo.0", nil, msg, 0)
	require_NoError(t, err)

	// Sanity check, while the in-memory lblk is correct, the last message is found.
	var smv StoreMsg
	sm, err := fs.LoadLastMsg("foo.0", &smv)
	require_NoError(t, err)
	require_Equal(t, sm.seq, seq)

	// Stop writes the stream state file.
	require_NoError(t, fs.Stop())
	_, err = os.Stat(filepath.Join(sd, msgDir, streamStreamStateFile))
	require_NoError(t, err)

	// Restart, and recover from the stream state file. On 2.12.x recovery would set lblk to the
	// stale fblk, both pointing at the first block.
	fs, err = newFileStore(fcfg, scfg)
	require_NoError(t, err)
	defer fs.Stop()

	// The last message for "foo.0" must still be found after a restart.
	sm, err = fs.LoadLastMsg("foo.0", &smv)
	require_NoError(t, err)
	require_Equal(t, sm.subj, "foo.0")
	require_Equal(t, sm.seq, seq)

	// Validate internal subject state.
	fs.mu.RLock()
	defer fs.mu.RUnlock()
	info, ok := fs.psim.Find(stringToBytes("foo.0"))
	require_True(t, ok)
	require_Equal(t, info.total, 1)
	require_Equal(t, info.lblk, 2)
	require_Equal(t, info.fblk, 2) // Since it's MaxMsgsPer:1, this should be optimized to last block.
}

func Benchmark_FileStoreSubjectStateConsistencyOptimizationPerf(b *testing.B) {
	fs, err := newFileStore(
		FileStoreConfig{StoreDir: b.TempDir()},
		StreamConfig{Name: "TEST", Subjects: []string{"foo.*"}, Storage: FileStorage, MaxMsgsPer: 1},
	)
	require_NoError(b, err)
	defer fs.Stop()

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
			_, _, err = fs.StoreMsg(subject, nil, nil, 0)
			require_NoError(b, err)
		}
	}
}

func Benchmark_FileStoreNumPendingMaxMsgsPerSubjectOneByStartSequence(b *testing.B) {
	fs, err := newFileStore(
		FileStoreConfig{StoreDir: b.TempDir(), BlockSize: 1024},
		StreamConfig{Name: "TEST", Subjects: []string{"foo.*"}, Storage: FileStorage, MaxMsgsPer: 1},
	)
	require_NoError(b, err)
	defer fs.Stop()

	const numSubjects = 10_000
	for i := 0; i < numSubjects*2; i++ {
		_, _, err = fs.StoreMsg(fmt.Sprintf("foo.%d", i%numSubjects), nil, nil, 0)
		require_NoError(b, err)
	}

	const startSeq = numSubjects + numSubjects/2
	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		total, _, err := fs.NumPending(startSeq, "foo.*", false)
		require_NoError(b, err)
		require_Equal(b, total, uint64(numSubjects/2+1))
	}
}

func benchmarkFileStoreSyncDeletedFullBlocks(b *testing.B, msgSize int) {
	fs, _ := newFileStore(
		FileStoreConfig{
			StoreDir:  b.TempDir(),
			BlockSize: defaultLargeBlockSize,
		},
		StreamConfig{
			Name:    "zzz",
			Storage: FileStorage,
		},
	)
	defer fs.Stop()

	const numBlocks = 50
	subj, msg := "foo", make([]byte, msgSize)

	b.ResetTimer()
	for b.Loop() {
		b.StopTimer()
		for len(fs.blks) < numBlocks {
			fs.StoreMsg(subj, nil, msg, 0)
		}
		dbs := DeleteBlocks{
			&DeleteRange{First: fs.state.FirstSeq, Num: fs.state.Msgs}}

		b.StartTimer()
		fs.SyncDeleted(dbs)
	}
}

func Benchmark_FileStoreSyncDeletedFullBlocks(b *testing.B) {
	sizes := []int{32, 64, 128, 256, 512, 1024}
	for _, msgSize := range sizes {
		b.Run(fmt.Sprintf("MsgSize-%d", msgSize),
			func(b *testing.B) {
				benchmarkFileStoreSyncDeletedFullBlocks(b, msgSize)
			})
	}
}

func benchmarkFileStoreSyncDeletedPartialBlocks(b *testing.B, msgSize int) {
	fs, _ := newFileStore(
		FileStoreConfig{
			StoreDir:  b.TempDir(),
			BlockSize: defaultLargeBlockSize,
		},
		StreamConfig{
			Name:    "zzz",
			Storage: FileStorage,
		},
	)
	defer fs.Stop()

	const numBlocks = 100
	subj, msg := "foo", make([]byte, msgSize)

	b.ResetTimer()
	for b.Loop() {
		b.StopTimer()
		if len(fs.blks) > 1 {
			fs.removeMsgsInRange(fs.state.FirstSeq, fs.blks[1].last.seq, true, nil)
		}
		for len(fs.blks) <= numBlocks {
			fs.StoreMsg(subj, nil, msg, 0)
		}

		first := fs.state.FirstSeq + 1
		last := fs.blks[1].last.seq
		dbs := DeleteBlocks{&DeleteRange{
			First: first,
			Num:   last - first}}

		b.StartTimer()
		fs.SyncDeleted(dbs)
	}
}

func Benchmark_FileStoreSyncDeletedPartialBlocks(b *testing.B) {
	sizes := []int{16, 512}
	for _, msgSize := range sizes {
		b.Run(fmt.Sprintf("MsgSize-%d", msgSize),
			func(b *testing.B) {
				benchmarkFileStoreSyncDeletedPartialBlocks(b, msgSize)
			})
	}
}

// Based on TestNoRaceJetStreamConsumerFileStoreConcurrentDiskIO, a test that
// was introduced together with the disk IO semaphore "dios".
func BenchmarkFileStoreConsumerStoreConcurrentDiskIO(b *testing.B) {
	const consumersPerIteration = 10000

	// This compares the same operations of that using a filestore configure with
	// default "dios", against a filestore with an unbounded "dios" (make it as
	// (large as the number of consumer stores created by the test).
	for _, test := range []struct {
		name string
		dios *diskIOSemaphore
	}{
		{name: "default_dios", dios: defaultDiskIOSemaphore()},
		{name: "unbounded_dios", dios: newDiskIOSemaphore(consumersPerIteration)},
	} {
		b.Run(test.name, func(b *testing.B) {
			storeRoot := b.TempDir()
			var dioLimit int

			b.StopTimer()
			b.ResetTimer()

			for i := 0; i < b.N; i++ {
				storeDir := filepath.Join(storeRoot, fmt.Sprintf("%d", i))
				fs, err := newFileStore(FileStoreConfig{StoreDir: storeDir}, StreamConfig{Name: "MT", Storage: FileStorage})
				require_NoError(b, err)
				fs.dios = test.dios
				dioLimit = fs.dios.cap()

				var wg sync.WaitGroup
				ts := time.Now().UnixNano()

				b.StartTimer()
				for j := 1; j <= consumersPerIteration; j++ {
					name := fmt.Sprintf("o%d", j)
					o, err := fs.ConsumerStore(name, time.Time{}, &ConsumerConfig{AckPolicy: AckExplicit})
					require_NoError(b, err)
					wg.Add(1)

					go func(o ConsumerStore) {
						defer wg.Done()
						if err := o.UpdateDelivered(22, 22, 1, ts); err != nil {
							panic(err)
						}
						cfs := o.(*consumerFileStore)
						buf, err := cfs.encodeState()
						if err != nil {
							panic(err)
						}
						if err := cfs.writeState(buf); err != nil {
							panic(err)
						}
						if err := o.Delete(); err != nil {
							panic(err)
						}
					}(o)
				}

				wg.Wait()
				b.StopTimer()

				require_NoError(b, fs.Stop())
			}

			b.ReportMetric(0, "ns/op")
			b.ReportMetric(float64(dioLimit), "dios_limit")
			b.ReportMetric(float64(b.N*consumersPerIteration)/b.Elapsed().Seconds(), "consumer_ops/s")
		})
	}
}

func TestFileStoreWriteFullStateDetectCorruptState(t *testing.T) {
	fs, err := newFileStore(
		FileStoreConfig{StoreDir: t.TempDir()},
		StreamConfig{Name: "zzz", Subjects: []string{"foo.*"}, Storage: FileStorage})
	require_NoError(t, err)
	defer fs.Stop()

	msg := []byte("abc")
	for i := 1; i <= 10; i++ {
		_, _, err = fs.StoreMsg(fmt.Sprintf("foo.%d", i), nil, msg, 0)
		require_NoError(t, err)
	}

	// Simulate a change in a message block not being reflected in the fs.
	mb := fs.selectMsgBlock(2)
	mb.mu.Lock()
	mb.msgs--
	mb.mu.Unlock()

	var ss StreamState
	fs.FastState(&ss)
	require_Equal(t, ss.FirstSeq, 1)
	require_Equal(t, ss.LastSeq, 10)
	require_Equal(t, ss.Msgs, 10)

	// Make sure we detect the corrupt state and rebuild.
	err = fs.writeFullState()
	require_Error(t, err, errCorruptState)

	fs.FastState(&ss)
	require_Equal(t, ss.FirstSeq, 1)
	require_Equal(t, ss.LastSeq, 10)
	require_Equal(t, ss.Msgs, 9)
}

func TestFileStoreRecoverFullStateDetectCorruptState(t *testing.T) {
	fs, err := newFileStore(
		FileStoreConfig{StoreDir: t.TempDir()},
		StreamConfig{Name: "zzz", Subjects: []string{"foo.*"}, Storage: FileStorage})
	require_NoError(t, err)
	defer fs.Stop()

	msg := []byte("abc")
	for i := 1; i <= 10; i++ {
		_, _, err = fs.StoreMsg(fmt.Sprintf("foo.%d", i), nil, msg, 0)
		require_NoError(t, err)
	}

	err = fs.writeFullState()
	require_NoError(t, err)

	sfile := filepath.Join(fs.fcfg.StoreDir, msgDir, streamStreamStateFile)
	buf, err := os.ReadFile(sfile)
	require_NoError(t, err)
	// Update to an incorrect message count.
	binary.PutUvarint(buf[2:], 0)
	// Just append a corrected checksum to the end to make it pass the checks.
	fs.hh.Reset()
	fs.hh.Write(buf)
	buf = fs.hh.Sum(buf)
	err = os.WriteFile(sfile, buf, defaultFilePerms)
	require_NoError(t, err)

	err = fs.recoverFullState()
	require_Error(t, err, errCorruptState)
}

func TestFileStoreRecoverFullStateBoundsCounts(t *testing.T) {
	fcfg := FileStoreConfig{StoreDir: t.TempDir()}
	cfg := StreamConfig{Name: "zzz", Subjects: []string{"foo.*"}, Storage: FileStorage}
	fs, err := newFileStore(fcfg, cfg)
	require_NoError(t, err)
	defer fs.Stop()

	msg := []byte("abc")
	for i := 1; i <= 10; i++ {
		_, _, err = fs.StoreMsg(fmt.Sprintf("foo.%d", i), nil, msg, 0)
		require_NoError(t, err)
	}

	key := sha256.Sum256([]byte(cfg.Name))
	hh, err := highwayhash.NewDigest64(key[:])
	require_NoError(t, err)

	for _, test := range []struct {
		title  string
		fields []uint64
	}{
		{"SubjectCount", []uint64{1 << 40}},
		{"SubjectLen", []uint64{1, math.MaxInt64}},
		{"BlockCount", []uint64{0, 1 << 62}},
	} {
		t.Run(test.title, func(t *testing.T) {
			fs.Stop()

			// Empty stream state, followed by the counts under test and some padding.
			buf := []byte{fullStateMagic, fullStateVersion, 0, 0, 0, 0, 0, 0}
			for _, f := range test.fields {
				buf = binary.AppendUvarint(buf, f)
			}
			buf = append(buf, make([]byte, 32)...)
			hh.Reset()
			hh.Write(buf)
			buf = hh.Sum(buf)
			sfile := filepath.Join(fcfg.StoreDir, msgDir, streamStreamStateFile)
			require_NoError(t, os.WriteFile(sfile, buf, defaultFilePerms))

			// Must reject the state and rebuild from the blocks.
			fs, err = newFileStore(fcfg, cfg)
			require_NoError(t, err)
			state := fs.State()
			require_Equal(t, state.Msgs, 10)
			require_Equal(t, state.FirstSeq, 1)
			require_Equal(t, state.LastSeq, 10)
		})
	}
}

func TestFileStoreResetConsumerToStreamState(t *testing.T) {
	fs, err := newFileStore(
		FileStoreConfig{StoreDir: t.TempDir()},
		StreamConfig{Name: "zzz", Subjects: []string{"foo.*"}, Storage: FileStorage})
	require_NoError(t, err)
	defer fs.Stop()

	msg := []byte("abc")
	for i := 1; i <= 30; i++ {
		_, _, err = fs.StoreMsg(fmt.Sprintf("foo.%d", i), nil, msg, 0)
		require_NoError(t, err)
	}

	err = fs.writeFullState()
	require_NoError(t, err)

	obs, err := fs.ConsumerStore("c1", time.Now(), &ConsumerConfig{
		Durable:       "c1",
		FilterSubject: "foo.*",
		AckPolicy:     AckNone,
		DeliverPolicy: DeliverAll,
	})

	require_NoError(t, err)
	defer obs.Stop()

	state := &ConsumerState{}
	state.Delivered = SequencePair{Consumer: 5, Stream: 5}
	state.AckFloor = SequencePair{Consumer: 5, Stream: 5}

	// set to 5
	err = obs.Update(state)
	require_NoError(t, err)

	currState, err := obs.State()
	require_NoError(t, err)

	fsState := fs.State()
	require_Equal(t, fsState.LastSeq, uint64(30))
	require_Equal(t, fsState.FirstSeq, uint64(1))
	require_Equal(t, currState.AckFloor, state.AckFloor)
	require_Equal(t, currState.Delivered, state.Delivered)
	require_Equal(t, len(currState.Redelivered), len(state.Redelivered))
	require_Equal(t, len(currState.Pending), len(state.Pending))

	fs.mu.Lock()
	fs.state.FirstSeq = 0
	fs.state.LastSeq = 0
	fs.mu.Unlock()

	// set back to lower values
	newState := &ConsumerState{
		Delivered: SequencePair{Consumer: 1, Stream: 4},
		AckFloor:  SequencePair{Consumer: 1, Stream: 3},
	}

	// update should fail but force update should pass
	err = obs.Update(newState)
	require_Error(t, err, ErrStoreOldUpdate)

	err = obs.ForceUpdate(newState)
	require_NoError(t, err)

	currState, err = obs.State()
	require_NoError(t, err)

	require_Equal(t, currState.AckFloor, newState.AckFloor)
	require_Equal(t, currState.Delivered, newState.Delivered)
	require_Equal(t, len(currState.Redelivered), len(newState.Redelivered))
	require_Equal(t, len(currState.Pending), len(newState.Pending))
}

func TestFileStoreNumPendingMulti(t *testing.T) {
	fs, err := newFileStore(
		FileStoreConfig{StoreDir: t.TempDir()},
		StreamConfig{Name: "zzz", Subjects: []string{"ev.*"}, Storage: FileStorage})
	require_NoError(t, err)
	defer fs.Stop()

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
		_, _, err = fs.StoreMsg(subjects[rand.IntN(totalSubjects)], nil, msg, 0)
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
	total, _, err := fs.NumPendingMulti(startSeq, filters, false)
	require_NoError(t, err)

	// Check our results.
	var checkTotal uint64
	var smv StoreMsg
	for seq := startSeq; seq <= uint64(totalMsgs); seq++ {
		sm, err := fs.LoadMsg(seq, &smv)
		require_NoError(t, err)
		if filters.HasInterest(sm.subj) {
			checkTotal++
		}
	}
	require_Equal(t, total, checkTotal)
}

func TestFileStoreMessageTTL(t *testing.T) {
	fs, err := newFileStore(
		FileStoreConfig{StoreDir: t.TempDir()},
		StreamConfig{Name: "zzz", Subjects: []string{"test"}, Storage: FileStorage, AllowMsgTTL: true})
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

func TestFileStoreMessageTTLRemovedOutOfBandDoesNotLeakTHW(t *testing.T) {
	fs, err := newFileStore(
		FileStoreConfig{StoreDir: t.TempDir()},
		StreamConfig{Name: "zzz", Subjects: []string{"test.>"}, Storage: FileStorage, AllowMsgTTL: true, AllowRollup: true})
	require_NoError(t, err)
	defer fs.Stop()

	ttl := int64(1) // 1 second

	for i := 1; i <= 10; i++ {
		_, _, err = fs.StoreMsg("test.a", nil, nil, ttl)
		require_NoError(t, err)
	}

	fs.mu.RLock()
	count := fs.ttls.Count()
	fs.mu.RUnlock()
	require_Equal(t, count, 10)

	// Remove the messages out of band, the way a rollup or a subject purge does.
	// This path does not consult the THW, so the entries stay behind.
	purged, err := fs.PurgeEx("test.a", 0, 0)
	require_NoError(t, err)
	require_Equal(t, purged, 10)

	// Once the TTLs are due, the expiry pass must notice the messages are already
	// gone and drop the entries, instead of retrying them on every pass forever.
	time.Sleep(time.Second * 2)
	fs.expireMsgs()

	fs.mu.RLock()
	count = fs.ttls.Count()
	fs.mu.RUnlock()
	require_Equal(t, count, 0)
}

func TestFileStoreMessageTTLTruncatedBelowDoesNotLeakTHW(t *testing.T) {
	fs, err := newFileStore(
		FileStoreConfig{StoreDir: t.TempDir()},
		StreamConfig{Name: "zzz", Subjects: []string{"test.>"}, Storage: FileStorage, AllowMsgTTL: true})
	require_NoError(t, err)
	defer fs.Stop()

	ttl := int64(1) // 1 second

	for i := 1; i <= 10; i++ {
		_, _, err = fs.StoreMsg("test.a", nil, nil, ttl)
		require_NoError(t, err)
	}

	// Truncate below the last TTL message. LastSeq drops to 5 and the entries for
	// 6..10 now point past the end of the stream, so removeMsg reports ErrStoreEOF.
	require_NoError(t, fs.Truncate(5))

	var ss StreamState
	fs.FastState(&ss)
	require_Equal(t, ss.LastSeq, 5)

	time.Sleep(time.Second * 2)
	fs.expireMsgs()

	fs.mu.RLock()
	count := fs.ttls.Count()
	fs.mu.RUnlock()
	require_Equal(t, count, 0)
}

func TestFileStoreMessageTTLRemovedOutOfBandPrunedTHWIsPersisted(t *testing.T) {
	dir := t.TempDir()
	cfg := StreamConfig{Name: "zzz", Subjects: []string{"test.>"}, Storage: FileStorage, AllowMsgTTL: true, AllowRollup: true}

	fs, err := newFileStore(FileStoreConfig{StoreDir: dir}, cfg)
	require_NoError(t, err)

	ttl := int64(1) // 1 second

	for i := 1; i <= 10; i++ {
		_, _, err = fs.StoreMsg("test.a", nil, nil, ttl)
		require_NoError(t, err)
	}

	// Remove out of band and flush, so thw.db on disk still carries the ten entries
	// and nothing after this point dirties the state except the pruning itself.
	purged, err := fs.PurgeEx("test.a", 0, 0)
	require_NoError(t, err)
	require_Equal(t, purged, 10)
	require_NoError(t, fs.forceWriteFullState())

	time.Sleep(time.Second * 2)
	fs.expireMsgs()

	fs.mu.RLock()
	count := fs.ttls.Count()
	fs.mu.RUnlock()
	require_Equal(t, count, 0)

	// A restart must not bring the stale entries back from thw.db.
	fs.Stop()
	fs, err = newFileStore(FileStoreConfig{StoreDir: dir}, cfg)
	require_NoError(t, err)
	defer fs.Stop()

	fs.mu.RLock()
	count = fs.ttls.Count()
	fs.mu.RUnlock()
	require_Equal(t, count, 0)
}

func TestFileStoreMessageTTLRestart(t *testing.T) {
	dir := t.TempDir()

	t.Run("BeforeRestart", func(t *testing.T) {
		fs, err := newFileStore(
			FileStoreConfig{StoreDir: dir},
			StreamConfig{Name: "zzz", Subjects: []string{"test"}, Storage: FileStorage, AllowMsgTTL: true})
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
	})

	t.Run("AfterRestart", func(t *testing.T) {
		fs, err := newFileStore(
			FileStoreConfig{StoreDir: dir},
			StreamConfig{Name: "zzz", Subjects: []string{"test"}, Storage: FileStorage, AllowMsgTTL: true})
		require_NoError(t, err)
		defer fs.Stop()

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
	})
}

func TestFileStoreMessageTTLRecovered(t *testing.T) {
	s := RunBasicJetStreamServer(t)
	defer s.Shutdown()

	dir := t.TempDir()

	t.Run("BeforeRestart", func(t *testing.T) {
		fs, err := newFileStore(
			FileStoreConfig{StoreDir: dir, srv: s},
			StreamConfig{Name: "zzz", Subjects: []string{"test"}, Storage: FileStorage, AllowMsgTTL: true})
		require_NoError(t, err)
		defer fs.Stop()

		ttl := int64(1) // 1 second

		// Make the first message with no ttl.
		// This exposes a bug we had in recovery.
		_, _, err = fs.StoreMsg("test", nil, nil, 0)
		require_NoError(t, err)

		for i := 0; i < 9; i++ {
			// When the timed hash wheel state is deleted, the only way we can recover
			// the TTL is to look at the original message header, therefore the TTL
			// must be in the headers for this test to work.
			hdr := fmt.Appendf(nil, "NATS/1.0\r\n%s: %d\r\n", JSMessageTTL, ttl)
			_, _, err = fs.StoreMsg("test", hdr, nil, ttl)
			require_NoError(t, err)
		}

		var ss StreamState
		fs.FastState(&ss)
		require_Equal(t, ss.FirstSeq, 1)
		require_Equal(t, ss.LastSeq, 10)
		require_Equal(t, ss.Msgs, 10)
	})

	t.Run("AfterRestart", func(t *testing.T) {
		// Delete the timed hash wheel state so that we are forced to do a linear scan
		// of message blocks containing TTL'd messages.
		fn := filepath.Join(dir, msgDir, ttlStreamStateFile)
		require_NoError(t, os.RemoveAll(fn))

		fs, err := newFileStore(
			FileStoreConfig{StoreDir: dir, srv: s},
			StreamConfig{Name: "zzz", Subjects: []string{"test"}, Storage: FileStorage, AllowMsgTTL: true})
		require_NoError(t, err)
		defer fs.Stop()

		require_Equal(t, fs.numMsgBlocks(), 1)
		fs.mu.RLock()
		mb := fs.blks[0]
		fs.mu.RUnlock()
		mb.mu.RLock()
		ttls := mb.ttls
		mb.mu.RUnlock()

		require_Equal(t, ttls, 9)

		var ss StreamState
		fs.FastState(&ss)
		require_Equal(t, ss.FirstSeq, 1)
		require_Equal(t, ss.LastSeq, 10)
		require_Equal(t, ss.Msgs, 10)

		time.Sleep(time.Second * 2)

		fs.FastState(&ss)
		require_Equal(t, ss.FirstSeq, 1)
		require_Equal(t, ss.LastSeq, 10)
		require_Equal(t, ss.Msgs, 1)
	})
}

func TestFileStoreMessageTTLRecoveredSingleMessageWithoutStreamState(t *testing.T) {
	s := RunBasicJetStreamServer(t)
	defer s.Shutdown()

	dir := t.TempDir()

	t.Run("BeforeRestart", func(t *testing.T) {
		fs, err := newFileStore(
			FileStoreConfig{StoreDir: dir, srv: s},
			StreamConfig{Name: "zzz", Subjects: []string{"test"}, Storage: FileStorage, AllowMsgTTL: true})
		require_NoError(t, err)
		defer fs.Stop()

		ttl := int64(1) // 1 second
		hdr := fmt.Appendf(nil, "NATS/1.0\r\n%s: %d\r\n", JSMessageTTL, ttl)
		_, _, err = fs.StoreMsg("test", hdr, nil, ttl)
		require_NoError(t, err)

		var ss StreamState
		fs.FastState(&ss)
		require_Equal(t, ss.FirstSeq, 1)
		require_Equal(t, ss.LastSeq, 1)
		require_Equal(t, ss.Msgs, 1)
	})

	t.Run("AfterRestart", func(t *testing.T) {
		// Delete the stream state file so that we need to rebuild.
		fn := filepath.Join(dir, msgDir, streamStreamStateFile)
		require_NoError(t, os.RemoveAll(fn))
		// Delete the timed hash wheel state so that we are forced to do a linear scan
		// of message blocks containing TTL'd messages.
		fn = filepath.Join(dir, msgDir, ttlStreamStateFile)
		require_NoError(t, os.RemoveAll(fn))

		fs, err := newFileStore(
			FileStoreConfig{StoreDir: dir, srv: s},
			StreamConfig{Name: "zzz", Subjects: []string{"test"}, Storage: FileStorage, AllowMsgTTL: true})
		require_NoError(t, err)
		defer fs.Stop()

		var ss StreamState
		fs.FastState(&ss)
		require_Equal(t, ss.FirstSeq, 1)
		require_Equal(t, ss.LastSeq, 1)
		require_Equal(t, ss.Msgs, 1)

		time.Sleep(time.Second * 2)

		fs.FastState(&ss)
		require_Equal(t, ss.FirstSeq, 2)
		require_Equal(t, ss.LastSeq, 1)
		require_Equal(t, ss.Msgs, 0)
	})
}

func TestFileStoreMessageTTLWriteTombstone(t *testing.T) {
	s := RunBasicJetStreamServer(t)
	defer s.Shutdown()

	dir := t.TempDir()

	fs, err := newFileStore(
		FileStoreConfig{StoreDir: dir, srv: s},
		StreamConfig{Name: "zzz", Subjects: []string{"test"}, Storage: FileStorage, AllowMsgTTL: true})
	require_NoError(t, err)
	defer fs.Stop()

	ttl := int64(1)

	// When the timed hash wheel state is deleted, the only way we can recover
	// the TTL is to look at the original message header, therefore the TTL
	// must be in the headers for this test to work.
	hdr := fmt.Appendf(nil, "NATS/1.0\r\n%s: %d\r\n", JSMessageTTL, ttl)
	_, _, err = fs.StoreMsg("test", hdr, nil, ttl)
	require_NoError(t, err)

	// Publish another message, but without TTL.
	_, _, err = fs.StoreMsg("test", nil, nil, 0)
	require_NoError(t, err)

	var ss StreamState
	fs.FastState(&ss)
	require_Equal(t, ss.Msgs, 2)
	require_Equal(t, ss.FirstSeq, 1)
	require_Equal(t, ss.LastSeq, 2)

	checkFor(t, 2*time.Second, 100*time.Millisecond, func() error {
		if fs.State().FirstSeq == 1 {
			return errors.New("message not expired yet")
		}
		return nil
	})

	fs.FastState(&ss)
	require_Equal(t, ss.Msgs, 1)
	require_Equal(t, ss.FirstSeq, 2)
	require_Equal(t, ss.LastSeq, 2)

	fs.Stop()

	// Delete the stream state file so that we need to rebuild.
	// Should have written a tombstone so we can properly recover.
	fn := filepath.Join(dir, msgDir, streamStreamStateFile)
	require_NoError(t, os.RemoveAll(fn))

	fs, err = newFileStore(
		FileStoreConfig{StoreDir: dir, srv: s},
		StreamConfig{Name: "zzz", Subjects: []string{"test"}, Storage: FileStorage, AllowMsgTTL: true})
	require_NoError(t, err)
	defer fs.Stop()

	fs.FastState(&ss)
	require_Equal(t, ss.Msgs, 1)
	require_Equal(t, ss.FirstSeq, 2)
	require_Equal(t, ss.LastSeq, 2)
}

func TestFileStoreMessageTTLRecoveredOffByOne(t *testing.T) {
	s := RunBasicJetStreamServer(t)
	defer s.Shutdown()

	dir := t.TempDir()

	fs, err := newFileStore(
		FileStoreConfig{StoreDir: dir, srv: s},
		StreamConfig{Name: "zzz", Subjects: []string{"test"}, Storage: FileStorage, AllowMsgTTL: true})
	require_NoError(t, err)
	defer fs.Stop()

	ts := time.Now().UnixNano()
	ttl := int64(120) // 2 minutes
	expires := time.Duration(ts) + (time.Second * time.Duration(ttl))

	// When the timed hash wheel state is deleted, the only way we can recover
	// the TTL is to look at the original message header, therefore the TTL
	// must be in the headers for this test to work.
	hdr := fmt.Appendf(nil, "NATS/1.0\r\n%s: %d\r\n", JSMessageTTL, ttl)
	require_NoError(t, fs.StoreRawMsg("test", hdr, nil, 1, ts, ttl, false))

	var ss StreamState
	fs.FastState(&ss)
	require_Equal(t, ss.Msgs, 1)
	require_Equal(t, ss.FirstSeq, 1)
	require_Equal(t, ss.LastSeq, 1)

	fs.mu.Lock()
	ttlc := fs.ttls.Count()
	// Adding to the THW is idempotent, so we sneakily remove it here
	// so we can check it doesn't get re-added during restart.
	err = fs.ttls.Remove(1, int64(expires))
	fs.mu.Unlock()
	require_Equal(t, ttlc, 1)
	require_NoError(t, err)

	fs.Stop()
	fs, err = newFileStore(
		FileStoreConfig{StoreDir: dir, srv: s},
		StreamConfig{Name: "zzz", Subjects: []string{"test"}, Storage: FileStorage, AllowMsgTTL: true})
	require_NoError(t, err)
	defer fs.Stop()

	fs.FastState(&ss)
	require_Equal(t, ss.Msgs, 1)
	require_Equal(t, ss.FirstSeq, 1)
	require_Equal(t, ss.LastSeq, 1)

	fs.mu.Lock()
	ttlc = fs.ttls.Count()
	fs.mu.Unlock()
	require_Equal(t, ttlc, 0)
}

func TestFileStoreDontSpamCompactWhenMostlyTombstones(t *testing.T) {
	fs, err := newFileStore(
		FileStoreConfig{StoreDir: t.TempDir(), BlockSize: defaultMediumBlockSize},
		StreamConfig{Name: "TEST", Subjects: []string{"foo"}, Storage: FileStorage})
	require_NoError(t, err)
	defer fs.Stop()

	// Store a bunch of messages, ensuring we get enough blocks.
	msg := bytes.Repeat([]byte("X"), 100)
	totalBlksAfterStore := 6
	expectedLseq := uint64(31_536*(totalBlksAfterStore-1) + 2)
	for seq := uint64(1); seq <= expectedLseq; seq++ {
		_, _, err = fs.StoreMsg("foo", nil, msg, 0)
		require_NoError(t, err)
	}

	// Confirms we have the required amount of blocks.
	fs.mu.RLock()
	lenBlks := len(fs.blks)
	lmb := fs.lmb
	fs.mu.RUnlock()
	require_Len(t, lenBlks, totalBlksAfterStore)

	// Ensure the last block contains the last two messages.
	lmb.mu.RLock()
	fseq, lseq := lmb.first.seq, lmb.last.seq
	lmb.mu.RUnlock()
	require_Equal(t, fseq, expectedLseq-1)
	require_Equal(t, lseq, expectedLseq)

	// Remove all messages before the last block, will fill up last block with tombstones.
	for seq := uint64(1); seq < fseq; seq++ {
		removed, err := fs.RemoveMsg(seq)
		require_NoError(t, err)
		require_True(t, removed)
	}

	// The last block will be filled with so many tombstones that
	// a new message block will be created to store the rest.
	fs.mu.RLock()
	lenBlks = len(fs.blks)
	fs.mu.RUnlock()
	require_Len(t, lenBlks, 2)

	fs.mu.Lock()
	defer fs.mu.Unlock()

	fmb := fs.blks[0]
	fmb.mu.Lock()
	defer fmb.mu.Unlock()

	// We don't call fs.RemoveMsg as that would call into compact.
	// Instead, we mark as deleted and check compaction ourselves.
	fmb.dmap.Insert(expectedLseq)
	fmb.bytes /= 2

	// This message block takes up ~4MB but contains only one ~100 bytes message, and the rest is all tombstones.
	// We should allow trying to compact.
	require_Equal(t, fmb.bytes, 133)
	require_Equal(t, fmb.cbytes, 0)
	require_True(t, fmb.shouldCompactInline())

	// Compact will be successful, but since it doesn't clean up tombstones it will be ineffective.
	require_NoError(t, fmb.compact())

	// We should not allow compacting again as we're not removing tombstones inline.
	// Otherwise, we would spam compaction.
	require_False(t, fmb.shouldCompactInline())

	// Just checking fmb.cbytes is tracking the previous value of fmb.bytes,
	// so it can block compaction until enough data has been removed.
	require_Equal(t, fmb.bytes, 133)
	require_Equal(t, fmb.cbytes, fmb.bytes)

	// Simulate having removed sufficient bytes and being allowed to compact again.
	fmb.bytes /= 2
	require_True(t, fmb.shouldCompactInline())
}

func TestFileStoreSubjectDeleteMarkers(t *testing.T) {
	fs, err := newFileStore(
		FileStoreConfig{StoreDir: t.TempDir()},
		StreamConfig{
			Name: "zzz", Subjects: []string{"test"}, Storage: FileStorage,
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

func TestFileStoreStoreRawMessageThrowsPermissionErrorIfFSModeReadOnly(t *testing.T) {
	// Test fails in Buildkite environment. Skip it.
	skipIfBuildkite(t)

	cfg := StreamConfig{Name: "zzz", Subjects: []string{"ev.1"}, Storage: FileStorage}
	fs, err := newFileStore(FileStoreConfig{StoreDir: t.TempDir(), BlockSize: 1024}, cfg)
	require_NoError(t, err)
	defer fs.Stop()

	READONLY_MODE := os.FileMode(0o555)
	ORIGINAL_FILE_MODE, err := os.Stat(fs.fcfg.StoreDir)
	require_NoError(t, err)
	require_NoError(t, changeDirectoryPermission(fs.fcfg.StoreDir, READONLY_MODE))
	defer func() {
		require_NoError(t, changeDirectoryPermission(fs.fcfg.StoreDir, ORIGINAL_FILE_MODE.Mode()))
	}()

	totalMsgs := 10000
	msg := bytes.Repeat([]byte("Z"), 1024)
	for i := 0; i < totalMsgs; i++ {
		if _, _, err = fs.StoreMsg("ev.1", nil, msg, 0); err != nil {
			break
		}
	}
	require_Error(t, err, os.ErrPermission)
}

func TestFileStoreWriteFullStateThrowsPermissionErrorIfFSModeReadOnly(t *testing.T) {
	// Test fails in Buildkite environment. Skip it.
	skipIfBuildkite(t)

	cfg := StreamConfig{Name: "zzz", Subjects: []string{"ev.1"}, Storage: FileStorage}
	fs, err := newFileStore(FileStoreConfig{StoreDir: t.TempDir()}, cfg)
	require_NoError(t, err)
	defer fs.Stop()

	READONLY_MODE := os.FileMode(0o555)
	ORIGINAL_FILE_MODE, err := os.Stat(fs.fcfg.StoreDir)
	require_NoError(t, err)

	totalMsgs := 10000
	msg := bytes.Repeat([]byte("Z"), 1024)
	for i := 0; i < totalMsgs; i++ {
		if _, _, err := fs.StoreMsg("ev.1", nil, msg, 0); err != nil {
			break
		}
	}

	require_NoError(t, changeDirectoryPermission(fs.fcfg.StoreDir, READONLY_MODE))
	require_Error(t, fs.writeFullState(), os.ErrPermission)
	require_NoError(t, changeDirectoryPermission(fs.fcfg.StoreDir, ORIGINAL_FILE_MODE.Mode()))
}

func changeDirectoryPermission(directory string, mode fs.FileMode) error {
	err := filepath.Walk(directory, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return fmt.Errorf("error accessing path %q: %w", path, err)
		}

		// Check if the path is a directory or file and set permissions accordingly
		if info.IsDir() {
			err = os.Chmod(path, mode)
			if err != nil {
				return fmt.Errorf("error changing directory permissions for %q: %w", path, err)
			}
		} else {
			err = os.Chmod(path, mode)
			if err != nil {
				return fmt.Errorf("error changing file permissions for %q: %w", path, err)
			}
		}
		return nil
	})
	return err
}

func TestFileStoreLeftoverSkipMsgInDmap(t *testing.T) {
	storeDir := t.TempDir()
	fs, err := newFileStore(
		FileStoreConfig{StoreDir: storeDir},
		StreamConfig{Name: "zzz", Subjects: []string{"test.*"}, Storage: FileStorage, MaxMsgsPer: 1},
	)
	require_NoError(t, err)
	defer fs.Stop()

	getLmbState := func(fs *fileStore) (uint64, uint64, int) {
		fs.mu.RLock()
		lmb := fs.lmb
		fs.mu.RUnlock()
		lmb.mu.RLock()
		fseq := atomic.LoadUint64(&lmb.first.seq)
		lseq := atomic.LoadUint64(&lmb.last.seq)
		dmaps := lmb.dmap.Size()
		lmb.mu.RUnlock()
		return fseq, lseq, dmaps
	}

	// Only skip a message.
	fs.SkipMsg(0)

	// Confirm state.
	state := fs.State()
	require_Equal(t, state.FirstSeq, 2)
	require_Equal(t, state.LastSeq, 1)
	require_Equal(t, state.NumDeleted, 0)
	fseq, lseq, dmaps := getLmbState(fs)
	require_Equal(t, fseq, 2)
	require_Equal(t, lseq, 1)
	require_Len(t, dmaps, 0)

	// Stop without writing index.db so we recover based on just the blk file.
	require_NoError(t, fs.stop(false, false))

	fs, err = newFileStore(
		FileStoreConfig{StoreDir: storeDir},
		StreamConfig{Name: "zzz", Subjects: []string{"test.*"}, Storage: FileStorage, MaxMsgsPer: 1},
	)
	require_NoError(t, err)
	defer fs.Stop()

	// Confirm the skipped message is not included in the deletes.
	state = fs.State()
	require_Equal(t, state.FirstSeq, 2)
	require_Equal(t, state.LastSeq, 1)
	require_Equal(t, state.NumDeleted, 0)
	fseq, lseq, dmaps = getLmbState(fs)
	require_Equal(t, fseq, 2)
	require_Equal(t, lseq, 1)
	require_Len(t, dmaps, 0)
}

func TestFileStoreRecoverOnlyBlkFiles(t *testing.T) {
	testFileStoreAllPermutations(t, func(t *testing.T, fcfg FileStoreConfig) {
		cfg := StreamConfig{Name: "zzz", Subjects: []string{"foo"}, Storage: FileStorage}
		created := time.Now()
		fs, err := newFileStoreWithCreated(fcfg, cfg, created, prf(&fcfg), nil)
		require_NoError(t, err)
		defer fs.Stop()

		_, _, err = fs.StoreMsg("foo", nil, nil, 0)
		require_NoError(t, err)

		// Confirm state as baseline.
		before := fs.State()
		require_Equal(t, before.Msgs, 1)
		require_Equal(t, before.FirstSeq, 1)
		require_Equal(t, before.LastSeq, 1)

		// Restart should equal state.
		require_NoError(t, fs.Stop())
		fs, err = newFileStoreWithCreated(fcfg, cfg, created, prf(&fcfg), nil)
		require_NoError(t, err)
		defer fs.Stop()

		if state := fs.State(); !reflect.DeepEqual(state, before) {
			t.Fatalf("Expected state of %+v, got %+v", before, state)
		}

		// Stream state should exist.
		_, err = os.Stat(filepath.Join(fs.fcfg.StoreDir, msgDir, streamStreamStateFile))
		require_NoError(t, err)

		// Stop and write some random files, but containing ".blk", should be ignored.
		require_NoError(t, fs.Stop())
		require_NoError(t, os.WriteFile(filepath.Join(fs.fcfg.StoreDir, msgDir, "10.blk.random"), nil, defaultFilePerms))
		require_NoError(t, os.WriteFile(filepath.Join(fs.fcfg.StoreDir, msgDir, fmt.Sprintf("10.blk.%s", blkTmpSuffix)), nil, defaultFilePerms))

		fs, err = newFileStoreWithCreated(fcfg, cfg, created, prf(&fcfg), nil)
		require_NoError(t, err)
		defer fs.Stop()

		// The random files would previously result in stream state to be deleted.
		_, err = os.Stat(filepath.Join(fs.fcfg.StoreDir, msgDir, streamStreamStateFile))
		require_NoError(t, err)

		if state := fs.State(); !reflect.DeepEqual(state, before) {
			t.Fatalf("Expected state of %+v, got %+v", before, state)
		}

		// Stop and remove stream state file.
		require_NoError(t, fs.Stop())
		require_NoError(t, os.Remove(filepath.Join(fs.fcfg.StoreDir, msgDir, streamStreamStateFile)))

		// Recovering based on blocks should also ignore the random files.
		fs, err = newFileStoreWithCreated(fcfg, cfg, created, prf(&fcfg), nil)
		require_NoError(t, err)
		defer fs.Stop()

		if state := fs.State(); !reflect.DeepEqual(state, before) {
			t.Fatalf("Expected state of %+v, got %+v", before, state)
		}
	})
}

func TestFileStoreRecoverAfterRemoveOperation(t *testing.T) {
	tests := []struct {
		title    string
		action   func(fs *fileStore)
		validate func(state StreamState)
	}{
		{
			title:  "None",
			action: func(fs *fileStore) {},
			validate: func(state StreamState) {
				require_Equal(t, state.Msgs, 4)
				require_Equal(t, state.FirstSeq, 1)
				require_Equal(t, state.LastSeq, 4)
			},
		},
		{
			title: "RemoveMsg",
			action: func(fs *fileStore) {
				removed, err := fs.RemoveMsg(1)
				require_NoError(t, err)
				require_True(t, removed)
			},
			validate: func(state StreamState) {
				require_Equal(t, state.Msgs, 3)
				require_Equal(t, state.FirstSeq, 2)
				require_Equal(t, state.LastSeq, 4)
			},
		},
		{
			title: "EraseMsg",
			action: func(fs *fileStore) {
				erased, err := fs.EraseMsg(1)
				require_NoError(t, err)
				require_True(t, erased)
			},
			validate: func(state StreamState) {
				require_Equal(t, state.Msgs, 3)
				require_Equal(t, state.FirstSeq, 2)
				require_Equal(t, state.LastSeq, 4)
			},
		},
		{
			title: "Purge",
			action: func(fs *fileStore) {
				purged, err := fs.Purge()
				require_NoError(t, err)
				require_Equal(t, purged, 4)
			},
			validate: func(state StreamState) {
				require_Equal(t, state.Msgs, 0)
				require_Equal(t, state.FirstSeq, 5)
				require_Equal(t, state.LastSeq, 4)
			},
		},
		{
			title: "PurgeEx-0",
			action: func(fs *fileStore) {
				purged, err := fs.PurgeEx("foo.0", 0, 0)
				require_NoError(t, err)
				require_Equal(t, purged, 2)
			},
			validate: func(state StreamState) {
				require_Equal(t, state.Msgs, 2)
				require_Equal(t, state.FirstSeq, 2)
				require_Equal(t, state.LastSeq, 4)
				require_Equal(t, state.NumDeleted, 1)
			},
		},
		{
			title: "PurgeEx-1",
			action: func(fs *fileStore) {
				purged, err := fs.PurgeEx("foo.1", 0, 0)
				require_NoError(t, err)
				require_Equal(t, purged, 2)
			},
			validate: func(state StreamState) {
				require_Equal(t, state.Msgs, 2)
				require_Equal(t, state.FirstSeq, 1)
				require_Equal(t, state.LastSeq, 4)
				require_Equal(t, state.NumDeleted, 2)
			},
		},
		{
			title: "Compact",
			action: func(fs *fileStore) {
				purged, err := fs.Compact(3)
				require_NoError(t, err)
				require_Equal(t, purged, 2)
			},
			validate: func(state StreamState) {
				require_Equal(t, state.Msgs, 2)
				require_Equal(t, state.FirstSeq, 3)
				require_Equal(t, state.LastSeq, 4)
			},
		},
		{
			title: "Truncate",
			action: func(fs *fileStore) {
				require_NoError(t, fs.Truncate(2))
			},
			validate: func(state StreamState) {
				require_Equal(t, state.Msgs, 2)
				require_Equal(t, state.FirstSeq, 1)
				require_Equal(t, state.LastSeq, 2)
			},
		},
	}
	for _, test := range tests {
		t.Run(test.title, func(t *testing.T) {
			testFileStoreAllPermutations(t, func(t *testing.T, fcfg FileStoreConfig) {
				cfg := StreamConfig{Name: "zzz", Subjects: []string{"foo.*"}, Storage: FileStorage}
				created := time.Now()
				fs, err := newFileStoreWithCreated(fcfg, cfg, created, prf(&fcfg), nil)
				require_NoError(t, err)
				defer fs.Stop()

				for i := 0; i < 4; i++ {
					subject := fmt.Sprintf("foo.%d", i%2)
					_, _, err = fs.StoreMsg(subject, nil, nil, 0)
					require_NoError(t, err)
				}

				test.action(fs)

				// Confirm state as baseline.
				before := fs.State()
				test.validate(before)

				// Restart should equal state.
				require_NoError(t, fs.Stop())
				fs, err = newFileStoreWithCreated(fcfg, cfg, created, prf(&fcfg), nil)
				require_NoError(t, err)
				defer fs.Stop()

				if state := fs.State(); !reflect.DeepEqual(state, before) {
					t.Fatalf("Expected state of:\n%+v, got:\n%+v", before, state)
				}

				// Stop and remove stream state file.
				require_NoError(t, fs.Stop())
				require_NoError(t, os.Remove(filepath.Join(fs.fcfg.StoreDir, msgDir, streamStreamStateFile)))

				// Recovering based on blocks should result in the same state.
				fs, err = newFileStoreWithCreated(fcfg, cfg, created, prf(&fcfg), nil)
				require_NoError(t, err)
				defer fs.Stop()

				if state := fs.State(); !reflect.DeepEqual(state, before) {
					t.Fatalf("Expected state of:\n%+v, got:\n%+v", before, state)
				}

				// Rebuilding state must also result in the same state.
				fs.rebuildState(nil)
				if state := fs.State(); !reflect.DeepEqual(state, before) {
					t.Fatalf("Expected state of:\n%+v, got:\n%+v", before, state)
				}
			})
		})
	}
}

func TestFileStoreRecoverAfterCompact(t *testing.T) {
	/*
		fs.Compact may rewrite the .blk file if it's large enough. In which case we don't
		need to write tombstones. But if the rewrite isn't done, tombstones must be placed
		to ensure we can properly recover without the index.db file.
	*/
	for _, test := range []struct {
		payloadSize    int
		usesTombstones bool
	}{
		{payloadSize: 1024, usesTombstones: true},
		{payloadSize: 1024 * 1024, usesTombstones: false},
	} {
		t.Run(strconv.Itoa(test.payloadSize), func(t *testing.T) {
			testFileStoreAllPermutations(t, func(t *testing.T, fcfg FileStoreConfig) {
				cfg := StreamConfig{Name: "zzz", Subjects: []string{"foo"}, Storage: FileStorage}
				created := time.Now()
				fs, err := newFileStoreWithCreated(fcfg, cfg, created, prf(&fcfg), nil)
				require_NoError(t, err)
				defer fs.Stop()

				subject := "foo"
				payload := make([]byte, test.payloadSize)
				for i := 0; i < 4; i++ {
					_, _, err = fs.StoreMsg(subject, nil, payload, 0)
					require_NoError(t, err)
				}

				// Confirm state before compacting.
				before := fs.State()
				require_Equal(t, before.Msgs, 4)
				require_Equal(t, before.FirstSeq, 1)
				require_Equal(t, before.LastSeq, 4)
				require_Equal(t, before.Bytes, uint64(emptyRecordLen+len(subject)+test.payloadSize)*4)

				fs.mu.RLock()
				lmb := fs.lmb
				fs.mu.RUnlock()

				// Underlying message block should report the same.
				if fcfg.Cipher == NoCipher && fcfg.Compression == NoCompression {
					lmb.mu.RLock()
					bytes, rbytes := lmb.bytes, lmb.rbytes
					lmb.mu.RUnlock()
					size := uint64(emptyRecordLen+len(subject)+test.payloadSize) * 4
					require_Equal(t, bytes, size)
					require_Equal(t, rbytes, size)
				}

				// Now compact.
				purged, err := fs.Compact(4)
				require_NoError(t, err)
				require_Equal(t, purged, 3)

				// Confirm state after compacting.
				// Bytes should reflect only having a single message left.
				before = fs.State()
				require_Equal(t, before.Msgs, 1)
				require_Equal(t, before.FirstSeq, 4)
				require_Equal(t, before.LastSeq, 4)
				require_Equal(t, before.Bytes, uint64(emptyRecordLen+len(subject)+test.payloadSize))

				// Underlying message block should report the same.
				// Unless the compact didn't rewrite the block but used tombstones,
				// in which case we expect the raw bytes to include them.
				if fcfg.Cipher == NoCipher && fcfg.Compression == NoCompression {
					lmb.mu.RLock()
					bytes, rbytes := lmb.bytes, lmb.rbytes
					lmb.mu.RUnlock()
					size := uint64(emptyRecordLen + len(subject) + test.payloadSize)
					require_Equal(t, bytes, size)
					if test.usesTombstones {
						// 4 messages, 3 tombstones
						size = uint64(emptyRecordLen+len(subject)+test.payloadSize)*4 + uint64(emptyRecordLen)*3
					}
					require_Equal(t, rbytes, size)
				}

				// Restart should equal state.
				require_NoError(t, fs.Stop())
				fs, err = newFileStoreWithCreated(fcfg, cfg, created, prf(&fcfg), nil)
				require_NoError(t, err)
				defer fs.Stop()

				if state := fs.State(); !reflect.DeepEqual(state, before) {
					t.Fatalf("Expected state of:\n%+v, got:\n%+v", before, state)
				}

				// Stop and remove stream state file.
				require_NoError(t, fs.Stop())
				require_NoError(t, os.Remove(filepath.Join(fs.fcfg.StoreDir, msgDir, streamStreamStateFile)))

				// Recovering based on blocks should result in the same state.
				fs, err = newFileStoreWithCreated(fcfg, cfg, created, prf(&fcfg), nil)
				require_NoError(t, err)
				defer fs.Stop()

				if state := fs.State(); !reflect.DeepEqual(state, before) {
					t.Fatalf("Expected state of:\n%+v, got:\n%+v", before, state)
				}

				// Rebuilding state must also result in the same state.
				fs.rebuildState(nil)
				if state := fs.State(); !reflect.DeepEqual(state, before) {
					t.Fatalf("Expected state of:\n%+v, got:\n%+v", before, state)
				}
			})
		})
	}
}

func TestFileStoreRecoverWithEmptyMessageBlock(t *testing.T) {
	testFileStoreAllPermutations(t, func(t *testing.T, fcfg FileStoreConfig) {
		// 4 messages with subject 'foo' and no payload.
		fcfg.BlockSize = 33 * 4
		cfg := StreamConfig{Name: "zzz", Subjects: []string{"foo"}, Storage: FileStorage}
		created := time.Now()
		fs, err := newFileStoreWithCreated(fcfg, cfg, created, prf(&fcfg), nil)
		require_NoError(t, err)
		defer fs.Stop()

		// First message block contains 4 messages.
		for i := 0; i < 4; i++ {
			_, _, err = fs.StoreMsg("foo", nil, nil, 0)
			require_NoError(t, err)
		}

		fs.mu.RLock()
		lblks := len(fs.blks)
		fs.mu.RUnlock()
		require_Len(t, lblks, 1)

		// Second (empty) message block only contains 2 tombstones.
		for i := uint64(1); i <= 2; i++ {
			removed, err := fs.RemoveMsg(i)
			require_NoError(t, err)
			require_True(t, removed)
		}

		fs.mu.RLock()
		lblks = len(fs.blks)
		fs.mu.RUnlock()
		require_Len(t, lblks, 2)

		before := fs.State()
		require_Equal(t, before.Msgs, 2)
		require_Equal(t, before.FirstSeq, 3)
		require_Equal(t, before.LastSeq, 4)

		// Restart should equal state.
		require_NoError(t, fs.Stop())
		fs, err = newFileStoreWithCreated(fcfg, cfg, created, prf(&fcfg), nil)
		require_NoError(t, err)
		defer fs.Stop()

		if state := fs.State(); !reflect.DeepEqual(state, before) {
			t.Fatalf("Expected state of:\n%+v, got:\n%+v", before, state)
		}

		// Stop and remove stream state file.
		require_NoError(t, fs.Stop())
		require_NoError(t, os.Remove(filepath.Join(fs.fcfg.StoreDir, msgDir, streamStreamStateFile)))

		// Recovering based on blocks should result in the same state.
		fs, err = newFileStoreWithCreated(fcfg, cfg, created, prf(&fcfg), nil)
		require_NoError(t, err)
		defer fs.Stop()

		if state := fs.State(); !reflect.DeepEqual(state, before) {
			t.Fatalf("Expected state of:\n%+v, got:\n%+v", before, state)
		}

		// Rebuilding state must also result in the same state.
		fs.rebuildState(nil)
		if state := fs.State(); !reflect.DeepEqual(state, before) {
			t.Fatalf("Expected state of:\n%+v, got:\n%+v", before, state)
		}
	})
}

func TestFileStoreRemoveMsgBlockFirst(t *testing.T) {
	s := RunBasicJetStreamServer(t)
	defer s.Shutdown()

	dir := t.TempDir()

	fs, err := newFileStore(
		FileStoreConfig{StoreDir: dir, srv: s},
		StreamConfig{Name: "zzz", Subjects: []string{"test"}, Storage: FileStorage})
	require_NoError(t, err)
	defer fs.Stop()

	_, _, err = fs.StoreMsg("test", nil, nil, 0)
	require_NoError(t, err)

	var ss StreamState
	fs.FastState(&ss)
	require_Equal(t, ss.Msgs, 1)
	require_Equal(t, ss.FirstSeq, 1)
	require_Equal(t, ss.LastSeq, 1)

	fs.Stop()

	for _, f := range []string{streamStreamStateFile, "1.blk"} {
		fn := filepath.Join(dir, msgDir, f)
		require_NoError(t, os.RemoveAll(fn))
	}

	fs, err = newFileStore(
		FileStoreConfig{StoreDir: dir, srv: s},
		StreamConfig{Name: "zzz", Subjects: []string{"test"}, Storage: FileStorage, AllowMsgTTL: true})
	require_NoError(t, err)
	defer fs.Stop()

	// If the block is removed first, we have nothing to recover. So starting out empty would be expected.
	fs.FastState(&ss)
	require_Equal(t, ss.Msgs, 0)
	require_Equal(t, ss.FirstSeq, 0)
	require_Equal(t, ss.LastSeq, 0)
}

func TestFileStoreRemoveMsgBlockLast(t *testing.T) {
	s := RunBasicJetStreamServer(t)
	defer s.Shutdown()

	dir := t.TempDir()

	fs, err := newFileStore(
		FileStoreConfig{StoreDir: dir, srv: s},
		StreamConfig{Name: "zzz", Subjects: []string{"test"}, Storage: FileStorage})
	require_NoError(t, err)
	defer fs.Stop()

	_, _, err = fs.StoreMsg("test", nil, nil, 0)
	require_NoError(t, err)

	var ss StreamState
	fs.FastState(&ss)
	require_Equal(t, ss.Msgs, 1)
	require_Equal(t, ss.FirstSeq, 1)
	require_Equal(t, ss.LastSeq, 1)

	// Copy first block so we can put it back later.
	ofn := filepath.Join(dir, msgDir, "1.blk")
	nfn := filepath.Join(dir, msgDir, "1.blk.cp")
	require_NoError(t, os.Rename(ofn, nfn))

	// Removing the last message will result in '2.blk' to be created, and '1.blk' to be removed.
	_, err = fs.RemoveMsg(1)
	require_NoError(t, err)
	_, err = os.Stat(filepath.Join(dir, msgDir, "2.blk"))
	require_NoError(t, err)
	_, err = os.Stat(ofn)
	require_True(t, os.IsNotExist(err))

	fs.Stop()

	// Remove index.db so we need to recover based on blocks.
	fn := filepath.Join(dir, msgDir, streamStreamStateFile)
	require_NoError(t, os.RemoveAll(fn))

	// Put back '1.blk' file to simulate being hard killed right
	// after creating '2.blk' but before cleaning up '1.blk'.
	require_NoError(t, os.Rename(nfn, ofn))
	_, err = os.Stat(ofn)
	require_False(t, os.IsNotExist(err))

	fs, err = newFileStore(
		FileStoreConfig{StoreDir: dir, srv: s},
		StreamConfig{Name: "zzz", Subjects: []string{"test"}, Storage: FileStorage, AllowMsgTTL: true})
	require_NoError(t, err)
	defer fs.Stop()

	// Should recognize correct state, and remove '1.blk'.
	fs.FastState(&ss)
	require_Equal(t, ss.Msgs, 0)
	require_Equal(t, ss.FirstSeq, 2)
	require_Equal(t, ss.LastSeq, 1)
	_, err = os.Stat(ofn)
	require_True(t, os.IsNotExist(err))
}

func TestFileStoreAllLastSeqs(t *testing.T) {
	fs, err := newFileStore(
		FileStoreConfig{StoreDir: t.TempDir()},
		StreamConfig{Name: "zzz", Subjects: []string{"*.*"}, MaxMsgsPer: 50, Storage: FileStorage})
	require_NoError(t, err)
	defer fs.Stop()

	subjs := []string{"foo.foo", "foo.bar", "foo.baz", "bar.foo", "bar.bar", "bar.baz"}
	msg := []byte("abc")

	for i := 0; i < 100_000; i++ {
		subj := subjs[rand.IntN(len(subjs))]
		fs.StoreMsg(subj, nil, msg, 0)
	}

	expected := make([]uint64, 0, len(subjs))
	var smv StoreMsg
	for _, subj := range subjs {
		sm, err := fs.LoadLastMsg(subj, &smv)
		require_NoError(t, err)
		expected = append(expected, sm.seq)
	}
	slices.Sort(expected)

	seqs, err := fs.AllLastSeqs()
	require_NoError(t, err)
	require_True(t, reflect.DeepEqual(seqs, expected))
}

func TestFileStoreRecoverDoesNotResetStreamState(t *testing.T) {
	testFileStoreAllPermutations(t, func(t *testing.T, fcfg FileStoreConfig) {
		cfg := StreamConfig{Name: "zzz", Subjects: []string{"ev.1"}, Storage: FileStorage, MaxAge: 2 * time.Second, Retention: WorkQueuePolicy}
		created := time.Now()
		fcfg.BlockSize = 1024
		fs, err := newFileStoreWithCreated(fcfg, cfg, created, prf(&fcfg), nil)
		require_NoError(t, err)
		defer fs.Stop()

		subj, msg := "foo", []byte("Hello World")
		toStore := 500
		for i := 0; i < toStore; i++ {
			_, _, err := fs.StoreMsg(subj, nil, msg, 0)
			require_NoError(t, err)
		}
		fs.mu.RLock()
		blks := len(fs.blks)
		fs.mu.RUnlock()
		require_True(t, blks > 1)

		// Simulate a consumer consuming all messages, but this test
		// expires all messages from the stream instead.
		time.Sleep(2500 * time.Millisecond)

		// Capture the state before stopping the store.
		fs.mu.RLock()
		blks = len(fs.blks)
		var mfn string
		if blks > 0 {
			mfn = fs.blks[0].mfn
		}
		fs.mu.RUnlock()

		// Stream state should exist after shutting down.
		require_NoError(t, fs.Stop())
		_, err = os.Stat(filepath.Join(fs.fcfg.StoreDir, msgDir, streamStreamStateFile))
		require_NoError(t, err)

		// A single block should have remained, but remove it so we need to restore from the index file.
		// The first/last sequences should be preserved and restored from the index.
		require_Len(t, blks, 1)
		require_NoError(t, os.Remove(mfn))

		fs, err = newFileStoreWithCreated(fs.fcfg, cfg, time.Now(), prf(&fs.fcfg), nil)
		require_NoError(t, err)
		fs.mu.RLock()
		defer fs.mu.RUnlock()
		require_True(t, fs.state.FirstSeq|fs.state.LastSeq != 0)
	})
}

func TestFileStoreAccessTimeSpinUp(t *testing.T) {
	// Only count the access time Go routine, other tests may still be spinning down theirs.
	numAtsGoroutines := func() int {
		buf := make([]byte, 1<<20)
		for {
			n := runtime.Stack(buf, true)
			if n < len(buf) {
				return strings.Count(string(buf[:n]), "server/ats.Register.func")
			}
			buf = make([]byte, 2*len(buf))
		}
	}

	// In case running lots of tests, give other filestores a chance to stop.
	// Another filestore could still be registered, so this is our baseline.
	baseline := numAtsGoroutines()
	for start := time.Now(); baseline > 0 && time.Since(start) < 2*time.Second; {
		time.Sleep(50 * time.Millisecond)
		baseline = numAtsGoroutines()
	}

	fs, err := newFileStore(
		FileStoreConfig{StoreDir: t.TempDir()},
		StreamConfig{Name: "zzz", Subjects: []string{"*.*"}, Storage: FileStorage})
	require_NoError(t, err)
	defer fs.Stop()

	at := ats.AccessTime()
	require_True(t, at != 0)
	require_True(t, numAtsGoroutines() >= 1)

	// Now check we also cleanup.
	fs.Stop()
	checkFor(t, 2*time.Second, 50*time.Millisecond, func() error {
		if n := numAtsGoroutines(); n > baseline {
			return fmt.Errorf("expected access time Go routines to return to %d, got %d", baseline, n)
		}
		return nil
	})
}

func TestFileStoreUpdateConfigTTLState(t *testing.T) {
	cfg := StreamConfig{
		Name:     "zzz",
		Subjects: []string{">"},
		Storage:  FileStorage,
	}
	fs, err := newFileStore(FileStoreConfig{StoreDir: t.TempDir()}, cfg)
	require_NoError(t, err)
	defer fs.Stop()
	require_Equal(t, fs.ttls, nil)

	cfg.AllowMsgTTL = true
	require_NoError(t, fs.UpdateConfig(&cfg))
	require_NotEqual(t, fs.ttls, nil)

	cfg.AllowMsgTTL = false
	require_NoError(t, fs.UpdateConfig(&cfg))
	require_Equal(t, fs.ttls, nil)
}

func TestFileStoreSubjectForSeq(t *testing.T) {
	cfg := StreamConfig{
		Name:     "foo",
		Subjects: []string{"foo.>"},
		Storage:  FileStorage,
	}
	fs, err := newFileStore(FileStoreConfig{StoreDir: t.TempDir()}, cfg)
	require_NoError(t, err)
	defer fs.Stop()

	seq, _, err := fs.StoreMsg("foo.bar", nil, nil, 0)
	require_NoError(t, err)
	require_Equal(t, seq, 1)

	_, err = fs.SubjectForSeq(0)
	require_Error(t, err, ErrStoreMsgNotFound)

	subj, err := fs.SubjectForSeq(1)
	require_NoError(t, err)
	require_Equal(t, subj, "foo.bar")

	_, err = fs.SubjectForSeq(2)
	require_Error(t, err, ErrStoreMsgNotFound)
}

func BenchmarkFileStoreSubjectAccesses(b *testing.B) {
	fs, err := newFileStore(FileStoreConfig{StoreDir: b.TempDir()}, StreamConfig{
		Name:     "foo",
		Subjects: []string{"foo.>"},
		Storage:  FileStorage,
	})
	require_NoError(b, err)
	defer fs.Stop()

	seq, _, err := fs.StoreMsg("foo.bar", nil, []byte{1, 2, 3, 4, 5}, 0)
	require_NoError(b, err)
	require_Equal(b, seq, 1)

	b.Run("SubjectForSeq", func(b *testing.B) {
		b.ReportAllocs()
		for range b.N {
			subj, err := fs.SubjectForSeq(1)
			require_NoError(b, err)
			require_Equal(b, subj, "foo.bar")
		}
	})

	b.Run("LoadMsg", func(b *testing.B) {
		b.ReportAllocs()
		for range b.N {
			// smv is deliberately inside the loop here because that's
			// effectively what is happening with needAck.
			var smv StoreMsg
			sm, err := fs.LoadMsg(1, &smv)
			require_NoError(b, err)
			require_Equal(b, sm.subj, "foo.bar")
		}
	})
}

func TestFileStoreFirstMatchingMultiExpiry(t *testing.T) {
	testFileStoreAllPermutations(t, func(t *testing.T, fcfg FileStoreConfig) {
		cfg := StreamConfig{Name: "zzz", Subjects: []string{"foo.>"}, Storage: FileStorage}
		fs, err := newFileStoreWithCreated(fcfg, cfg, time.Now(), prf(&fcfg), nil)
		require_NoError(t, err)
		defer fs.Stop()

		_, _, err = fs.StoreMsg("foo.foo", nil, []byte("A"), 0)
		require_NoError(t, err)

		_, _, err = fs.StoreMsg("foo.foo", nil, []byte("B"), 0)
		require_NoError(t, err)

		_, _, err = fs.StoreMsg("foo.foo", nil, []byte("C"), 0)
		require_NoError(t, err)

		fs.mu.RLock()
		mb := fs.lmb
		mb.tryExpireCacheLocked()
		fs.mu.RUnlock()

		sl := gsl.NewSublist[struct{}]()
		sl.Insert("foo.foo", struct{}{})

		_, didLoad, err := mb.firstMatchingMulti(sl, 1, nil)
		require_NoError(t, err)
		require_False(t, didLoad)

		_, didLoad, err = mb.firstMatchingMulti(sl, 2, nil)
		require_NoError(t, err)
		require_False(t, didLoad)

		_, didLoad, err = mb.firstMatchingMulti(sl, 3, nil)
		require_NoError(t, err)
		require_True(t, didLoad) // last message, should expire
	})
}

func TestFileStoreNoPanicOnRecoverTTLWithCorruptBlocks(t *testing.T) {
	testFileStoreAllPermutations(t, func(t *testing.T, fcfg FileStoreConfig) {
		fs, err := newFileStoreWithCreated(fcfg, StreamConfig{Name: "zzz", Storage: FileStorage, AllowMsgTTL: true}, time.Now(), prf(&fcfg), nil)
		require_NoError(t, err)
		defer fs.Stop()

		hdr := genHeader(nil, JSMessageTTL, "1")
		for i := range 3 {
			if i > 0 {
				_, err = fs.newMsgBlockForWrite()
				require_NoError(t, err)
			}
			_, _, err = fs.StoreMsg("foo", hdr, []byte("A"), 1)
			require_NoError(t, err)
		}

		fs.mu.Lock()
		if blks := len(fs.blks); blks != 3 {
			fs.mu.Unlock()
			t.Fatalf("Expected 3 blocks, got %d", blks)
		}

		// Manually corrupt the blocks by removing the second and changing the
		// sequence range for the last to that of the first.
		fmb := fs.blks[0]
		smb := fs.blks[1]
		lmb := fs.lmb
		fseq, lseq := atomic.LoadUint64(&fmb.first.seq), atomic.LoadUint64(&fmb.last.seq)
		smb.mu.Lock()
		fs.removeMsgBlock(smb)
		smb.mu.Unlock()
		fs.mu.Unlock()
		atomic.StoreUint64(&lmb.first.seq, fseq)
		atomic.StoreUint64(&lmb.last.seq, lseq)

		// Reset TTL state so recoverPerMessageState actually re-runs TTL
		// recovery and scans the (corrupted) blocks.
		fs.mu.Lock()
		fs.ttls = nil
		fs.mu.Unlock()

		require_NoError(t, fs.recoverPerMessageState())
	})
}

func TestFileStoreAsyncTruncate(t *testing.T) {
	testFileStoreAllPermutations(t, func(t *testing.T, fcfg FileStoreConfig) {
		fcfg.BlockSize = 8192
		fcfg.AsyncFlush = true

		fs, err := newFileStoreWithCreated(fcfg, StreamConfig{Name: "zzz", Storage: FileStorage}, time.Now(), prf(&fcfg), nil)
		require_NoError(t, err)
		defer fs.Stop()

		fs.mu.RLock()
		lmb := fs.lmb
		fs.mu.RUnlock()
		require_NotNil(t, lmb)

		// Wait for flusher to be ready.
		checkFor(t, 2*time.Second, 200*time.Millisecond, func() error {
			lmb.mu.RLock()
			defer lmb.mu.RUnlock()
			if !lmb.flusher {
				return errors.New("flusher not active")
			}
			return nil
		})
		// Now shutdown flusher and wait for it to be closed.
		lmb.mu.Lock()
		if lmb.qch != nil {
			close(lmb.qch)
			lmb.qch = nil
		}
		lmb.mu.Unlock()
		checkFor(t, 2*time.Second, 200*time.Millisecond, func() error {
			lmb.mu.RLock()
			defer lmb.mu.RUnlock()
			if lmb.flusher {
				return errors.New("flusher still active")
			}
			return nil
		})

		// Write some messages, none of them will have been flushed asynchronously.
		subj, msg := "foo", make([]byte, 100)
		for i := uint64(1); i <= 2; i++ {
			seq, _, err := fs.StoreMsg(subj, nil, msg, 0)
			require_NoError(t, err)
			require_Equal(t, seq, i)
		}
		// Truncate needs to flush if the data was not yet flushed asynchronously.
		require_NoError(t, fs.Truncate(1))

		state := fs.State()
		require_Equal(t, state.Msgs, 1)
		require_Equal(t, state.FirstSeq, 1)
		require_Equal(t, state.LastSeq, 1)

		fs.mu.RLock()
		for _, mb := range fs.blks {
			if mb.pendingWriteSize() > 0 {
				fs.mu.RUnlock()
				t.Fatalf("Message block %d still has pending writes", mb.index)
			}
		}
		fs.mu.RUnlock()
	})
}

func TestFileStoreAsyncFlushOnSkipMsgs(t *testing.T) {
	for _, noFlushLoop := range []bool{false, true} {
		t.Run(fmt.Sprintf("NoFlushLoop=%v", noFlushLoop), func(t *testing.T) {
			testFileStoreAllPermutations(t, func(t *testing.T, fcfg FileStoreConfig) {
				fcfg.BlockSize = 8192
				fcfg.AsyncFlush = true

				fs, err := newFileStoreWithCreated(fcfg, StreamConfig{Name: "zzz", Storage: FileStorage}, time.Now(), prf(&fcfg), nil)
				require_NoError(t, err)
				defer fs.Stop()

				fs.mu.RLock()
				fmb := fs.lmb
				fs.mu.RUnlock()
				require_NotNil(t, fmb)

				if noFlushLoop {
					// Wait for flusher to be ready.
					checkFor(t, 2*time.Second, 200*time.Millisecond, func() error {
						fmb.mu.RLock()
						defer fmb.mu.RUnlock()
						if !fmb.flusher {
							return errors.New("flusher not active")
						}
						return nil
					})
					// Now shutdown flusher and wait for it to be closed.
					fmb.mu.Lock()
					if fmb.qch != nil {
						close(fmb.qch)
						fmb.qch = nil
					}
					fmb.mu.Unlock()
					checkFor(t, 2*time.Second, 200*time.Millisecond, func() error {
						fmb.mu.RLock()
						defer fmb.mu.RUnlock()
						if fmb.flusher {
							return errors.New("flusher still active")
						}
						return nil
					})
				}

				// Confirm no pending writes.
				require_Equal(t, fmb.pendingWriteSize(), 0)

				_, _, err = fs.StoreMsg("foo", nil, nil, 0)
				require_NoError(t, err)

				if noFlushLoop {
					// Confirm above write is pending.
					require_Equal(t, fmb.pendingWriteSize(), 33)
				}

				require_NoError(t, fs.SkipMsgs(2, 100_000))
				fs.mu.RLock()
				if blks := len(fs.blks); blks != 2 {
					fs.mu.RUnlock()
					t.Fatalf("Expected 2 blocks, got %d", blks)
				}
				lmb := fs.blks[1]
				fs.mu.RUnlock()

				// Should have immediately flushed the previous block.
				require_Equal(t, fmb.pendingWriteSize(), 0)

				// Should eventually flush the last block.
				checkFor(t, 2*time.Second, 100*time.Millisecond, func() error {
					if p := lmb.pendingWriteSize(); p > 0 {
						return fmt.Errorf("expected no pending writes, got %d", p)
					}
					return nil
				})
			})
		})
	}
}

func TestFileStoreCompressionHeaderCollision(t *testing.T) {
	for _, size := range []int{7368035, 24145251} {
		for _, hdr := range [][]byte{nil, []byte("NATS/1.0\r\nTest: value\r\n\r\n")} {
			t.Run(fmt.Sprintf("Size=%d/Headers=%v", size, len(hdr) > 0), func(t *testing.T) {
				testFileStoreAllPermutations(t, func(t *testing.T, fcfg FileStoreConfig) {
					cfg := StreamConfig{Name: "TEST", Storage: FileStorage}
					fs, err := newFileStoreWithCreated(fcfg, cfg, time.Now(), prf(&fcfg), nil)
					require_NoError(t, err)
					defer fs.Stop()

					subj := "test"
					msg := bytes.Repeat([]byte("a"), size-int(fileStoreMsgSize(subj, hdr, nil)))
					_, _, err = fs.StoreMsg(subj, hdr, msg, 0)
					require_NoError(t, err)
					// Keep a following message to check after deletion.
					_, _, err = fs.StoreMsg(subj, nil, []byte("next"), 0)
					require_NoError(t, err)
					require_NoError(t, fs.Stop())

					// Reopen to force decoding from disk instead of reading cached messages.
					fs, err = newFileStoreWithCreated(fcfg, cfg, time.Now(), prf(&fcfg), nil)
					require_NoError(t, err)
					defer fs.Stop()
					sm, err := fs.LoadMsg(1, nil)
					require_NoError(t, err)
					require_Equal(t, sm.subj, subj)
					require_True(t, bytes.Equal(sm.hdr, hdr))
					require_True(t, bytes.Equal(sm.msg, msg))
					removed, err := fs.RemoveMsg(1)
					require_NoError(t, err)
					require_True(t, removed)
					sm, err = fs.LoadMsg(2, nil)
					require_NoError(t, err)
					require_Equal(t, string(sm.msg), "next")
				})
			})
		}
	}
}

func TestFileStoreDecodeCorruptBlock(t *testing.T) {
	fs, err := newFileStore(FileStoreConfig{StoreDir: t.TempDir(), Compression: S2Compression}, StreamConfig{Name: "TEST", Storage: FileStorage})
	require_NoError(t, err)
	defer fs.Stop()

	// Create a large valid S2 compressed block
	msg := make([]byte, 25*1024*1024)
	_, err = crand.Read(msg)
	require_NoError(t, err)
	_, _, err = fs.StoreMsg("test", nil, msg, 0)
	require_NoError(t, err)
	mb := fs.getFirstBlock()
	require_NoError(t, mb.flushPendingMsgs())
	mb.mu.Lock()
	defer mb.mu.Unlock()
	require_NoError(t, mb.recompressOnDiskIfNeeded())
	compressed, err := mb.loadBlock(nil)
	require_NoError(t, err)

	// Corrupt a byte in the S2 data so decompression fails.
	// mb.decode will try to interpret this block as uncompressed,
	// but that will fail as well because of checksum mismatch.
	compressed[len(compressed)/2] ^= 0xff
	_, _, err = mb.decode(compressed)
	require_Error(t, err)
	require_True(t, errors.Is(err, s2.ErrCRC))
}

func TestFileStoreCompressionAfterTruncate(t *testing.T) {
	tests := []struct {
		title  string
		action func(fs *fileStore, seq uint64)
	}{
		{
			title: "RemoveMsg",
			action: func(fs *fileStore, seq uint64) {
				removed, err := fs.RemoveMsg(seq)
				require_NoError(t, err)
				require_True(t, removed)
			},
		},
		{
			title: "EraseMsg",
			action: func(fs *fileStore, seq uint64) {
				erased, err := fs.EraseMsg(seq)
				require_NoError(t, err)
				require_True(t, erased)
			},
		},
		{
			title: "Tombstone",
			action: func(fs *fileStore, seq uint64) {
				removed, err := fs.removeMsg(seq, false, false, true)
				require_NoError(t, err)
				require_True(t, removed)
			},
		},
	}
	for _, test := range tests {
		for _, recompress := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/Recompress=%v", test.title, recompress), func(t *testing.T) {
				testFileStoreAllPermutations(t, func(t *testing.T, fcfg FileStoreConfig) {
					cfg := StreamConfig{Name: "zzz", Subjects: []string{"foo"}, Storage: FileStorage}
					created := time.Now()
					fs, err := newFileStoreWithCreated(fcfg, cfg, created, prf(&fcfg), nil)
					require_NoError(t, err)
					defer fs.Stop()

					for range 2 {
						_, _, err = fs.StoreMsg("foo", nil, nil, 0)
						require_NoError(t, err)
					}

					checkCompressed := func(mb *msgBlock) (bool, error) {
						mb.mu.Lock()
						defer mb.mu.Unlock()
						buf, err := mb.loadBlock(nil)
						if err != nil {
							return false, err
						}
						if err = mb.encryptOrDecryptIfNeeded(buf); err != nil {
							return false, err
						}
						var meta CompressionInfo
						if n, err := meta.UnmarshalMetadata(buf); err != nil {
							return false, err
						} else if n == 0 {
							return false, nil
						} else {
							return meta.Algorithm != NoCompression, nil
						}
					}

					smb := fs.getFirstBlock()
					require_NotNil(t, smb)
					compressed, err := checkCompressed(smb)
					require_NoError(t, err)
					require_False(t, compressed)

					_, err = fs.newMsgBlockForWrite()
					require_NoError(t, err)

					if fcfg.Compression != NoCompression {
						checkFor(t, 2*time.Second, 100*time.Millisecond, func() error {
							if compressed, err = checkCompressed(smb); err != nil {
								return err
							} else if !compressed {
								return errors.New("block not compressed yet")
							}
							return nil
						})
					} else {
						compressed, err = checkCompressed(smb)
						require_NoError(t, err)
						require_False(t, compressed)
					}

					_, _, err = fs.StoreMsg("foo", nil, nil, 0)
					require_NoError(t, err)

					test.action(fs, 2)

					state := fs.State()
					require_Equal(t, state.Msgs, 2)
					require_Equal(t, state.FirstSeq, 1)
					require_Equal(t, state.LastSeq, 3)
					require_Equal(t, state.NumDeleted, 1)

					require_NoError(t, fs.Truncate(2))
					state = fs.State()
					require_Equal(t, state.Msgs, 1)
					require_Equal(t, state.FirstSeq, 1)
					require_Equal(t, state.LastSeq, 2)
					require_Equal(t, state.NumDeleted, 1)

					fs.mu.RLock()
					lmb := fs.lmb
					fs.mu.RUnlock()
					if smb == lmb {
						compressed, err = checkCompressed(smb)
						require_NoError(t, err)
						require_False(t, compressed)
					} else {
						compressed, err = checkCompressed(lmb)
						require_NoError(t, err)
						require_False(t, compressed)

						if fcfg.Compression != NoCompression {
							checkFor(t, 2*time.Second, 100*time.Millisecond, func() error {
								if compressed, err = checkCompressed(smb); err != nil {
									return err
								} else if !compressed {
									return errors.New("block not compressed yet")
								}
								return nil
							})
						}
					}

					require_NoError(t, fs.forceWriteFullState())

					seq, _, err := fs.StoreMsg("foo", nil, nil, 0)
					require_NoError(t, err)
					require_Equal(t, seq, 3)
					require_NoError(t, fs.forceWriteFullState())

					smb.mu.Lock()
					smb.clearCacheAndOffset()
					smb.mu.Unlock()

					require_NoError(t, smb.loadMsgsWithLock())
					compressed, err = checkCompressed(smb)
					require_NoError(t, err)
					if smb == lmb {
						require_False(t, compressed)
					} else {
						require_Equal(t, compressed, fcfg.Compression != NoCompression)
					}

					compressed, err = checkCompressed(lmb)
					require_NoError(t, err)
					require_False(t, compressed)
				})
			})
		}
	}
}

func TestFileStoreTruncateRemovedBlock(t *testing.T) {
	testFileStoreAllPermutations(t, func(t *testing.T, fcfg FileStoreConfig) {
		cfg := StreamConfig{Name: "zzz", Subjects: []string{"foo"}, Storage: FileStorage}
		created := time.Now()
		fs, err := newFileStoreWithCreated(fcfg, cfg, created, prf(&fcfg), nil)
		require_NoError(t, err)
		defer fs.Stop()

		for i := range 3 {
			if i > 0 {
				_, err = fs.newMsgBlockForWrite()
				require_NoError(t, err)
			}
			_, _, err = fs.StoreMsg("foo", nil, nil, 0)
			require_NoError(t, err)
		}

		fs.mu.RLock()
		blks := len(fs.blks)
		fs.mu.RUnlock()
		require_Len(t, blks, 3)

		state := fs.State()
		require_Equal(t, state.Msgs, 3)
		require_Equal(t, state.FirstSeq, 1)
		require_Equal(t, state.LastSeq, 3)
		require_Equal(t, state.NumDeleted, 0)

		removed, err := fs.RemoveMsg(2)
		require_NoError(t, err)
		require_True(t, removed)

		fs.mu.RLock()
		blks = len(fs.blks)
		fs.mu.RUnlock()
		require_Len(t, blks, 2)

		fs.mu.RLock()
		blks = len(fs.blks)
		fs.mu.RUnlock()
		require_Len(t, blks, 2)

		state = fs.State()
		require_Equal(t, state.Msgs, 2)
		require_Equal(t, state.FirstSeq, 1)
		require_Equal(t, state.LastSeq, 3)
		require_Equal(t, state.NumDeleted, 1)

		require_NoError(t, fs.Truncate(2))
		state = fs.State()
		require_Equal(t, state.Msgs, 1)
		require_Equal(t, state.FirstSeq, 1)
		require_Equal(t, state.LastSeq, 2)
		require_Equal(t, state.NumDeleted, 1)
	})
}

func TestFileStoreAtomicEraseMsg(t *testing.T) {
	for _, lmb := range []bool{true, false} {
		t.Run(fmt.Sprintf("lmb=%v", lmb), func(t *testing.T) {
			testFileStoreAllPermutations(t, func(t *testing.T, fcfg FileStoreConfig) {
				cfg := StreamConfig{Name: "zzz", Subjects: []string{"foo"}, Storage: FileStorage}
				created := time.Now()
				fs, err := newFileStoreWithCreated(fcfg, cfg, created, prf(&fcfg), nil)
				require_NoError(t, err)
				defer fs.Stop()

				for range 3 {
					_, _, err = fs.StoreMsg("foo", nil, nil, 0)
					require_NoError(t, err)
				}

				checkCompressed := func(mb *msgBlock) (bool, error) {
					mb.mu.Lock()
					defer mb.mu.Unlock()
					buf, err := mb.loadBlock(nil)
					if err != nil {
						return false, err
					}
					if err := mb.checkAndLoadEncryption(); err != nil {
						return false, err
					}
					if err = mb.encryptOrDecryptIfNeeded(buf); err != nil {
						return false, err
					}
					var meta CompressionInfo
					if n, err := meta.UnmarshalMetadata(buf); err != nil {
						return false, err
					} else if n == 0 {
						return false, nil
					} else {
						return meta.Algorithm != NoCompression, nil
					}
				}

				if !lmb {
					smb := fs.getFirstBlock()
					require_NotNil(t, smb)

					mb, err := fs.newMsgBlockForWrite()
					require_NoError(t, err)
					compressed, err := checkCompressed(mb)
					require_NoError(t, err)
					require_False(t, compressed)

					if fcfg.Compression != NoCompression {
						checkFor(t, 2*time.Second, 100*time.Millisecond, func() error {
							if compressed, err = checkCompressed(smb); err != nil {
								return err
							} else if !compressed {
								return errors.New("block not compressed yet")
							}
							return nil
						})
					}
				}

				before := fs.State()
				require_Equal(t, before.Msgs, 3)
				require_Equal(t, before.FirstSeq, 1)
				require_Equal(t, before.LastSeq, 3)
				require_Equal(t, before.NumDeleted, 0)

				removed, err := fs.EraseMsg(2)
				require_NoError(t, err)
				require_True(t, removed)

				before = fs.State()
				require_Equal(t, before.Msgs, 2)
				require_Equal(t, before.FirstSeq, 1)
				require_Equal(t, before.LastSeq, 3)
				require_Equal(t, before.NumDeleted, 1)

				seq, _, err := fs.StoreMsg("foo", nil, nil, 0)
				require_NoError(t, err)
				require_Equal(t, seq, 4)
				before = fs.State()

				validateCompressed := func() {
					t.Helper()
					fs.mu.Lock()
					defer fs.mu.Unlock()
					for _, mb := range fs.blks {
						compressed, err := checkCompressed(mb)
						require_NoError(t, err)
						if mb == fs.lmb {
							require_False(t, compressed)
						} else {
							require_Equal(t, compressed, fs.fcfg.Compression != NoCompression)
						}
					}
				}
				validateCompressed()

				// Restart should equal before.
				require_NoError(t, fs.Stop())
				fs, err = newFileStoreWithCreated(fcfg, cfg, created, prf(&fcfg), nil)
				require_NoError(t, err)
				defer fs.Stop()

				if state := fs.State(); !reflect.DeepEqual(state, before) {
					t.Fatalf("Expected before of:\n%+v, got:\n%+v", before, state)
				}
				validateCompressed()

				// Stop and remove stream before file.
				require_NoError(t, fs.Stop())
				require_NoError(t, os.Remove(filepath.Join(fs.fcfg.StoreDir, msgDir, streamStreamStateFile)))

				// Recovering based on blocks should result in the same before.
				fs, err = newFileStoreWithCreated(fcfg, cfg, created, prf(&fcfg), nil)
				require_NoError(t, err)
				defer fs.Stop()

				if state := fs.State(); !reflect.DeepEqual(state, before) {
					t.Fatalf("Expected before of:\n%+v, got:\n%+v", before, state)
				}
				validateCompressed()

				// Rebuilding before must also result in the same before.
				fs.rebuildState(nil)
				if state := fs.State(); !reflect.DeepEqual(state, before) {
					t.Fatalf("Expected before of:\n%+v, got:\n%+v", before, state)
				}
				validateCompressed()
			})
		})
	}
}

func TestFileStoreRemoveBlockWithStaleStreamState(t *testing.T) {
	testFileStoreAllPermutations(t, func(t *testing.T, fcfg FileStoreConfig) {
		cfg := StreamConfig{Name: "zzz", Subjects: []string{"foo"}, Storage: FileStorage}
		created := time.Now()
		fs, err := newFileStoreWithCreated(fcfg, cfg, created, prf(&fcfg), nil)
		require_NoError(t, err)
		defer fs.Stop()

		for i := range 3 {
			if i > 0 {
				_, err = fs.newMsgBlockForWrite()
				require_NoError(t, err)
			}
			_, _, err = fs.StoreMsg("foo", nil, nil, 0)
			require_NoError(t, err)
		}

		// Get middle block.
		fs.mu.RLock()
		require_Len(t, len(fs.blks), 3)
		midfn := fs.blks[1].mfn
		fs.mu.RUnlock()

		require_NoError(t, fs.Stop())
		require_NoError(t, os.Remove(midfn))

		// Restart.
		fs, err = newFileStoreWithCreated(fcfg, cfg, created, prf(&fcfg), nil)
		require_NoError(t, err)
		defer fs.Stop()

		for i := range 3 {
			seq := uint64(i + 1)
			_, err = fs.LoadMsg(seq, nil)
			if seq == 2 {
				require_Error(t, err, ErrStoreMsgNotFound)
			} else {
				require_NoError(t, err)
			}
		}
	})
}

func TestFileStoreMessageSchedule(t *testing.T) {
	s := RunBasicJetStreamServer(t)
	defer s.Shutdown()

	dir := t.TempDir()
	fs, err := newFileStore(
		FileStoreConfig{StoreDir: dir, srv: s},
		StreamConfig{Name: "TEST", Subjects: []string{"foo.*"}, Storage: FileStorage, AllowMsgSchedules: true})
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

func TestFileStoreMessageScheduleRecovered(t *testing.T) {
	s := RunBasicJetStreamServer(t)
	defer s.Shutdown()

	dir := t.TempDir()
	t.Run("BeforeRestart", func(t *testing.T) {
		fs, err := newFileStore(
			FileStoreConfig{StoreDir: dir, srv: s},
			StreamConfig{Name: "TEST", Subjects: []string{"foo.*"}, Storage: FileStorage, AllowMsgSchedules: true})
		require_NoError(t, err)
		defer fs.Stop()

		schedule := time.Now().Add(time.Second).Format(time.RFC3339Nano)
		hdr := genHeader(nil, JSSchedulePattern, fmt.Sprintf("@at %s", schedule))
		hdr = genHeader(hdr, JSScheduleTarget, "foo.target")
		_, _, err = fs.StoreMsg("foo.schedule", hdr, nil, 0)
		require_NoError(t, err)

		var ss StreamState
		fs.FastState(&ss)
		require_Equal(t, ss.FirstSeq, 1)
		require_Equal(t, ss.LastSeq, 1)
		require_Equal(t, ss.Msgs, 1)
	})

	t.Run("AfterRestart", func(t *testing.T) {
		// Delete the message scheduling state so that we are forced to do a linear scan
		// of message blocks containing message schedules.
		fn := filepath.Join(dir, msgDir, msgSchedulingStreamStateFile)
		require_NoError(t, os.Remove(fn))

		fs, err := newFileStore(
			FileStoreConfig{StoreDir: dir, srv: s},
			StreamConfig{Name: "TEST", Subjects: []string{"foo.*"}, Storage: FileStorage, AllowMsgSchedules: true})
		require_NoError(t, err)
		defer fs.Stop()

		require_Equal(t, fs.numMsgBlocks(), 1)
		fs.mu.RLock()
		mb := fs.blks[0]
		fs.mu.RUnlock()
		mb.mu.RLock()
		schedules := mb.schedules
		mb.mu.RUnlock()

		require_Equal(t, schedules, 1)

		var ss StreamState
		fs.FastState(&ss)
		require_Equal(t, ss.FirstSeq, 1)
		require_Equal(t, ss.LastSeq, 1)
		require_Equal(t, ss.Msgs, 1)

		fs.mu.RLock()
		defer fs.mu.RUnlock()
		require_True(t, fs.scheduling != nil)
		require_Len(t, len(fs.scheduling.schedules), 1)
	})
}

func TestFileStoreMessageScheduleEncodeDecode(t *testing.T) {
	ms := newMsgScheduling(func() {})
	now := time.Now()

	// Add many sequences.
	numSequences := 100_000
	for seq := 0; seq < numSequences; seq++ {
		ts := now.Add(time.Duration(seq) * time.Second).UnixNano()
		subj := fmt.Sprintf("foo.%d", seq)
		ms.add(uint64(seq), subj, ts)
	}

	b := ms.encode(12345)
	require_True(t, len(b) > 17) // Bigger than just the header

	nms := newMsgScheduling(func() {})
	stamp, err := nms.decode(b)
	require_NoError(t, err)
	require_Equal(t, stamp, 12345)
	require_Equal(t, ms.ttls.GetNextExpiration(math.MaxInt64), nms.ttls.GetNextExpiration(math.MaxInt64))

	require_Len(t, len(ms.seqToSubj), len(nms.seqToSubj))
	for seq, subj := range ms.seqToSubj {
		require_Equal(t, subj, nms.seqToSubj[seq])
	}

	require_Len(t, len(ms.schedules), len(nms.schedules))
	for subj, sched := range ms.schedules {
		nsched := nms.schedules[subj]
		require_NotNil(t, nsched)
		require_Equal(t, sched.ts, nsched.ts)
		require_Equal(t, sched.seq, nsched.seq)
	}
}

func TestFileStoreMessageScheduleDecodeRejectsMalformed(t *testing.T) {
	// Build a valid header that claims a single schedule entry.
	header := func(count uint64) []byte {
		b := make([]byte, headerLen)
		b[0] = 1                                    // Magic version
		binary.LittleEndian.PutUint64(b[1:], count) // Entry count
		binary.LittleEndian.PutUint64(b[9:], 0)     // High sequence stamp
		return b
	}

	for _, test := range []struct {
		title string
		buf   []byte
		err   error
	}{
		{title: "ShortHeader", buf: make([]byte, headerLen-1), err: io.ErrShortBuffer},
		{title: "BadVersion", buf: func() []byte { b := header(0); b[0] = 2; return b }(), err: ErrMsgScheduleInvalidVersion},
		// Claims one entry but the buffer ends right after the header.
		{title: "TruncatedAtSubjLen", buf: header(1), err: io.ErrUnexpectedEOF},
		// Claims one entry, has the subject length but the subject bytes are missing.
		{title: "TruncatedSubj", buf: append(header(1), 5, 0), err: io.ErrUnexpectedEOF},
		// Has the subject but the timestamp/seq varints are missing.
		{title: "TruncatedVarints", buf: append(header(1), 3, 0, 'f', 'o', 'o'), err: io.ErrUnexpectedEOF},
		// Has the subject and timestamp varint but the seq varint is missing.
		{title: "TruncatedSeq", buf: append(append(header(1), 3, 0, 'f', 'o', 'o'), binary.AppendVarint(nil, 12345)...), err: io.ErrUnexpectedEOF},
	} {
		t.Run(test.title, func(t *testing.T) {
			ms := newMsgScheduling(func() {})
			_, err := ms.decode(test.buf)
			require_Error(t, err, test.err)
		})
	}
}

func TestFileStoreSourcesDecodeRejectsMalformed(t *testing.T) {
	// Build a valid header that claims a given number of source entries.
	header := func(count uint64) []byte {
		b := make([]byte, sourcesHeaderLen)
		b[0] = 1                                    // Magic version
		binary.LittleEndian.PutUint64(b[1:], count) // Entry count
		binary.LittleEndian.PutUint64(b[9:], 0)     // High sequence stamp
		return b
	}
	uvi := func(v uint64) []byte {
		return binary.AppendUvarint(nil, v)
	}

	for _, test := range []struct {
		title string
		buf   []byte
		err   error
	}{
		{title: "ShortHeader", buf: make([]byte, sourcesHeaderLen-1), err: io.ErrShortBuffer},
		{title: "BadVersion", buf: func() []byte { b := header(0); b[0] = 2; return b }(), err: errSourcesInvalidVersion},
		// Claims one entry but the buffer ends right after the header.
		{title: "TruncatedAtSourceLen", buf: header(1), err: io.ErrUnexpectedEOF},
		// Has the source length but the source bytes are missing.
		{title: "TruncatedSource", buf: append(header(1), uvi(5)...), err: io.ErrUnexpectedEOF},
		// Has the source but the seq varint is missing.
		{title: "TruncatedSeq", buf: append(append(header(1), uvi(3)...), 'f', 'o', 'o'), err: io.ErrUnexpectedEOF},
		// Has the source and seq but the identity-length field is missing.
		{title: "TruncatedAtIdentLen", buf: append(append(append(header(1), uvi(3)...), 'f', 'o', 'o'), uvi(1)...), err: io.ErrUnexpectedEOF},
		// Has the identity length but the identity bytes are missing.
		{title: "TruncatedIdent", buf: append(append(append(append(header(1), uvi(3)...), 'f', 'o', 'o'), uvi(1)...), uvi(5)...), err: io.ErrUnexpectedEOF},
	} {
		t.Run(test.title, func(t *testing.T) {
			fs := &fileStore{}
			_, err := fs.decodeSourcesState(test.buf)
			require_Error(t, err, test.err)
		})
	}
}

func TestFileStoreSourcesRecovery(t *testing.T) {
	test := func(t *testing.T, remove bool) {
		dir := t.TempDir()
		cfg := StreamConfig{
			Name:     "SOURCE",
			Subjects: []string{"foo.*"},
			Storage:  FileStorage,
			Sources:  []*StreamSource{{Name: "ORIGIN"}},
		}
		fcfg := FileStoreConfig{StoreDir: dir, BlockSize: 256}

		fs, err := newFileStore(fcfg, cfg)
		require_NoError(t, err)

		// Store messages carrying a stream-source header so the sources map is populated.
		// Header format matches genSourceHeader: "<iName> <seq> <source> <dest> <orig> <ident>".
		const N = 10
		const ident = "IDENTITY1"
		for i := 1; i <= N; i++ {
			hdr := genHeader(nil, JSStreamSource, fmt.Sprintf("ORIGIN %d > > foo.bar %s", i, ident))
			_, _, err = fs.StoreMsg("foo.bar", hdr, nil, 0)
			require_NoError(t, err)
		}
		require_True(t, fs.numMsgBlocks() >= 2)

		// Sanity check the state before restart, last source seq should win.
		state := fs.SourcesState()
		require_NotNil(t, state)
		require_Equal(t, state["ORIGIN > >"].Seq, N)
		require_Equal(t, state["ORIGIN > >"].Ident, ident)
		require_NoError(t, fs.Stop())

		if remove {
			// Delete the persisted sources state.
			require_NoError(t, os.Remove(filepath.Join(dir, sourcesStreamStateFile)))
		}

		fs, err = newFileStore(fcfg, cfg)
		require_NoError(t, err)
		defer fs.Stop()

		// Sequence and identity must both survive the restart, otherwise
		// stream recreation can't be detected.
		// Recovered if it still exists, but otherwise the store needs to scan.
		state = fs.SourcesState()
		require_NotNil(t, state)
		require_Equal(t, state["ORIGIN > >"].Seq, N)
		require_Equal(t, state["ORIGIN > >"].Ident, ident)
	}

	t.Run("Normal", func(t *testing.T) { test(t, false) })
	t.Run("Remove", func(t *testing.T) { test(t, true) })
}

func TestFileStoreSourcesRecoveryPre210Headers(t *testing.T) {
	// Pre-2.10 source headers carry no index name, they match any source using
	// that stream name. Both the backward scan and the combined forward scan
	// (taken when TTL/scheduling state must be recovered as well) must honor them,
	// otherwise the source consumer restarts from sequence 1 and redelivers.
	test := func(t *testing.T, allowMsgTTL bool) {
		dir := t.TempDir()
		cfg := StreamConfig{
			Name:        "SOURCE",
			Subjects:    []string{"foo.*"},
			Storage:     FileStorage,
			Sources:     []*StreamSource{{Name: "ORIGIN"}},
			AllowMsgTTL: allowMsgTTL,
		}
		fcfg := FileStoreConfig{StoreDir: dir, BlockSize: 256}

		fs, err := newFileStore(fcfg, cfg)
		require_NoError(t, err)

		// Pre-2.10 header format, just "<stream> <seq>", no index name and no identity.
		const N = 10
		for i := 1; i <= N; i++ {
			hdr := genHeader(nil, JSStreamSource, fmt.Sprintf("ORIGIN %d", i))
			_, _, err = fs.StoreMsg("foo.bar", hdr, nil, 0)
			require_NoError(t, err)
		}
		require_NoError(t, fs.Stop())

		// Drop the persisted index so recovery must scan for the source sequence.
		require_NoError(t, os.Remove(filepath.Join(dir, sourcesStreamStateFile)))

		fs, err = newFileStore(fcfg, cfg)
		require_NoError(t, err)
		defer fs.Stop()

		// Must resume from the latest source sequence, not from zero.
		state := fs.SourcesState()
		require_NotNil(t, state)
		require_Equal(t, state["ORIGIN > >"].Seq, N)
		require_Equal(t, state["ORIGIN > >"].Ident, _EMPTY_)
	}

	// Without TTLs this takes the backward scan, with TTLs the combined forward scan.
	t.Run("BackwardScan", func(t *testing.T) { test(t, false) })
	t.Run("ForwardScan", func(t *testing.T) { test(t, true) })
}

func TestFileStoreSourcesStaleConfig(t *testing.T) {
	// Dropping one of several sources must prune its decoded key on recover.
	t.Run("PruneKeyOnRecover", func(t *testing.T) {
		dir := t.TempDir()
		fcfg := FileStoreConfig{StoreDir: dir}
		cfg := StreamConfig{
			Name:     "SOURCE",
			Subjects: []string{"foo.*"},
			Storage:  FileStorage,
			Sources:  []*StreamSource{{Name: "ORIGIN1"}, {Name: "ORIGIN2"}},
		}

		fs, err := newFileStore(fcfg, cfg)
		require_NoError(t, err)

		h1 := genHeader(nil, JSStreamSource, "ORIGIN1 1 > > foo.a")
		_, _, err = fs.StoreMsg("foo.a", h1, nil, 0)
		require_NoError(t, err)
		h2 := genHeader(nil, JSStreamSource, "ORIGIN2 1 > > foo.b")
		_, _, err = fs.StoreMsg("foo.b", h2, nil, 0)
		require_NoError(t, err)

		state := fs.SourcesState()
		require_Len(t, len(state), 2)

		// Persist the sources state so the recover path takes the decode (not linear-scan)
		// branch, which is the one that can carry stale keys.
		require_NoError(t, fs.writeFullState())
		require_NoError(t, fs.Stop())
		_, err = os.Stat(filepath.Join(dir, sourcesStreamStateFile))
		require_NoError(t, err)

		// Reopen with ORIGIN2 removed from the config. Its decoded key must be pruned.
		cfg.Sources = []*StreamSource{{Name: "ORIGIN1"}}
		fs, err = newFileStore(fcfg, cfg)
		require_NoError(t, err)
		defer fs.Stop()

		state = fs.SourcesState()
		require_Len(t, len(state), 1)
		_, ok := state["ORIGIN1 > >"]
		require_True(t, ok)
		_, ok = state["ORIGIN2 > >"]
		require_False(t, ok)
	})

	// An index with nothing to record, or whose sources are all gone, must
	// not linger on disk.
	t.Run("NoStaleIndexOnDisk", func(t *testing.T) {
		dir := t.TempDir()
		fcfg := FileStoreConfig{StoreDir: dir}
		cfg := StreamConfig{
			Name:     "SOURCE",
			Subjects: []string{"foo.*"},
			Storage:  FileStorage,
			Sources:  []*StreamSource{{Name: "ORIGIN"}},
		}

		fs, err := newFileStore(fcfg, cfg)
		require_NoError(t, err)
		defer fs.Stop()

		fn := filepath.Join(dir, sourcesStreamStateFile)

		// Sources are configured but nothing has been observed yet, so the map only
		// holds seeded (zero) entries.
		state := fs.SourcesState()
		require_Len(t, len(state), 1)
		sss, ok := state["ORIGIN > >"]
		require_True(t, ok)
		require_Equal(t, sss.Seq, 0)
		require_NoError(t, fs.writeFullState())
		_, err = os.Stat(fn)
		require_NoError(t, err)

		// Observe a source, the index should now be written.
		hdr := genHeader(nil, JSStreamSource, "ORIGIN 1 > > foo.bar")
		_, _, err = fs.StoreMsg("foo.bar", hdr, nil, 0)
		require_NoError(t, err)
		require_NoError(t, fs.writeFullState())
		_, err = os.Stat(fn)
		require_NoError(t, err)

		// Drop the sources from the config. recoverPerMessageState (via UpdateConfig)
		// should prune the now-stale index from disk.
		ucfg := cfg
		ucfg.Sources = nil
		require_NoError(t, fs.UpdateConfig(&ucfg))
		_, err = os.Stat(fn)
		require_True(t, os.IsNotExist(err))
	})
}

func TestFileStoreSourcesAddedSourceKeepsExistingState(t *testing.T) {
	origin1 := &StreamSource{Name: "ORIGIN1"}
	origin2 := &StreamSource{Name: "ORIGIN2"}
	iName1, iName2 := origin1.composeIName(), origin2.composeIName()

	// Adding a source must only recover the sequence of the source that was just
	// added. Sources that are already tracked must keep their state, even if a
	// scan derives a lower sequence for them, since that would result in
	// the source consumer re-delivering messages it had already stored.
	test := func(t *testing.T, allowMsgTTL bool) {
		dir := t.TempDir()
		fcfg := FileStoreConfig{StoreDir: dir}
		cfg := StreamConfig{
			Name:     "SOURCE",
			Subjects: []string{"foo"},
			Storage:  FileStorage,
			Sources:  []*StreamSource{origin1},
		}

		fs, err := newFileStore(fcfg, cfg)
		require_NoError(t, err)
		defer fs.Stop()

		for i := range 10 {
			hdr := genHeader(nil, JSStreamSource, fmt.Sprintf("ORIGIN1 %d > > foo", i+1))
			_, _, err = fs.StoreMsg("foo", hdr, nil, 0)
			require_NoError(t, err)
		}
		state := fs.SourcesState()
		require_Len(t, len(state), 1)
		require_Equal(t, state[iName1].Seq, 10)

		// Remove the newest message, a scan would now only be able to derive
		// ORIGIN1 up to sequence 9.
		removed, err := fs.RemoveMsg(10)
		require_NoError(t, err)
		require_True(t, removed)

		// Add a second source. Optionally enabling TTLs as well, which forces
		// recovery through the forward linear scan instead of the backward scan.
		ucfg := cfg
		ucfg.AllowMsgTTL = allowMsgTTL
		ucfg.Sources = []*StreamSource{origin1, origin2}
		require_NoError(t, fs.UpdateConfig(&ucfg))

		state = fs.SourcesState()
		require_Len(t, len(state), 2)
		// ORIGIN1 must not have moved backward.
		require_Equal(t, state[iName1].Seq, 10)
		// ORIGIN2 is newly seeded and has nothing to recover.
		require_Equal(t, state[iName2].Seq, 0)
	}

	t.Run("BackwardScan", func(t *testing.T) { test(t, false) })
	t.Run("LinearScan", func(t *testing.T) { test(t, true) })
}

func TestFileStoreSourcesRecoveredFromOutdatedState(t *testing.T) {
	srcs := []*StreamSource{
		{Name: "ORIGIN1", FilterSubject: "s1.>"},
		{Name: "ORIGIN2", FilterSubject: "s2.>"},
		{Name: "ORIGIN3", FilterSubject: "s3.>"},
	}

	test := func(t *testing.T, withTTL bool) {
		dir := t.TempDir()
		fcfg := FileStoreConfig{StoreDir: dir, BlockSize: 256}
		cfg := StreamConfig{
			Name:        "SOURCE",
			Subjects:    []string{">"},
			Storage:     FileStorage,
			Sources:     srcs,
			AllowMsgTTL: withTTL,
		}

		fs, err := newFileStore(fcfg, cfg)
		require_NoError(t, err)

		// Store an interleaved batch from each source, then persist the sources state.
		store := func(round int) {
			for i, src := range srcs {
				subj := fmt.Sprintf("s%d.x", i+1)
				hdr := genHeader(nil, JSStreamSource, fmt.Sprintf("%s %d %s > %s", src.Name, round, src.FilterSubject, subj))
				_, _, err = fs.StoreMsg(subj, hdr, nil, 0)
				require_NoError(t, err)
			}
		}
		const firstBatch = 15
		for r := 1; r <= firstBatch; r++ {
			store(r)
		}
		require_NoError(t, fs.writeFullState())

		// Snapshot the now-current (but soon-to-be-outdated) sources state file.
		fn := filepath.Join(dir, sourcesStreamStateFile)
		outdated, err := os.ReadFile(fn)
		require_NoError(t, err)

		// Store a second batch with higher source sequences, then stop.
		const lastSeq = 30
		for r := firstBatch + 1; r <= lastSeq; r++ {
			store(r)
		}
		require_True(t, fs.numMsgBlocks() >= 2)
		require_NoError(t, fs.Stop())

		// Roll the sources state file back to the outdated snapshot, simulating messages
		// stored after the last flush of the index.
		require_NoError(t, os.WriteFile(fn, outdated, defaultFilePerms))

		fs, err = newFileStore(fcfg, cfg)
		require_NoError(t, err)
		defer fs.Stop()

		// Each source should be recovered to its latest sequence, not the stale one.
		state := fs.SourcesState()
		require_Len(t, len(state), len(srcs))
		for _, src := range srcs {
			require_Equal(t, state[src.composeIName()].Seq, lastSeq)
		}
	}

	t.Run("BackwardScan", func(t *testing.T) { test(t, false) })
	t.Run("ForwardScanWithTTL", func(t *testing.T) { test(t, true) })
}

func TestFileStoreSourcesRecoveredOneMessageAfterFlush(t *testing.T) {
	dir := t.TempDir()
	fcfg := FileStoreConfig{StoreDir: dir}
	cfg := StreamConfig{
		Name:     "SOURCE",
		Subjects: []string{">"},
		Storage:  FileStorage,
		Sources:  []*StreamSource{{Name: "ORIGIN"}},
	}

	fs, err := newFileStore(fcfg, cfg)
	require_NoError(t, err)

	store := func(sseq uint64) {
		hdr := genHeader(nil, JSStreamSource, fmt.Sprintf("ORIGIN %d > > foo.bar", sseq))
		_, _, err = fs.StoreMsg("foo.bar", hdr, nil, 0)
		require_NoError(t, err)
	}

	const flushed = 5
	for i := uint64(1); i <= flushed; i++ {
		store(i)
	}
	require_NoError(t, fs.writeFullState())

	// Snapshot the index as it is right after the flush.
	fn := filepath.Join(dir, sourcesStreamStateFile)
	outdated, err := os.ReadFile(fn)
	require_NoError(t, err)

	// Exactly one message lands after that flush, which is the boundary the
	// stamp has to get right. If it claims to cover this message the recovery
	// scan skips it and the source silently resumes from the wrong sequence.
	const last = flushed + 1
	store(last)
	require_NoError(t, fs.Stop())
	require_NoError(t, os.WriteFile(fn, outdated, defaultFilePerms))

	fs, err = newFileStore(fcfg, cfg)
	require_NoError(t, err)
	defer fs.Stop()

	require_Equal(t, fs.SourcesState()["ORIGIN > >"].Seq, last)
}

func TestFileStoreSourcesRecoveredFromPre210Header(t *testing.T) {
	dir := t.TempDir()
	fcfg := FileStoreConfig{StoreDir: dir}
	cfg := StreamConfig{
		Name:     "SOURCE",
		Subjects: []string{">"},
		Storage:  FileStorage,
		Sources:  []*StreamSource{{Name: "ORIGIN"}},
	}

	fs, err := newFileStore(fcfg, cfg)
	require_NoError(t, err)

	// A modern header first, purely so an index exists on disk and recovery takes
	// the scan path rather than bailing out.
	hdr := genHeader(nil, JSStreamSource, "ORIGIN 1 > > foo.bar")
	_, _, err = fs.StoreMsg("foo.bar", hdr, nil, 0)
	require_NoError(t, err)
	require_NoError(t, fs.writeFullState())

	fn := filepath.Join(dir, sourcesStreamStateFile)
	outdated, err := os.ReadFile(fn)
	require_NoError(t, err)

	// The message that lands after the flush carries a pre-2.10 header, which has
	// no index name. Only the stream-name fallback in the scan can match it.
	const last = 9
	hdr = genHeader(nil, JSStreamSource, fmt.Sprintf("ORIGIN %d", last))
	_, _, err = fs.StoreMsg("foo.bar", hdr, nil, 0)
	require_NoError(t, err)
	require_NoError(t, fs.Stop())
	require_NoError(t, os.WriteFile(fn, outdated, defaultFilePerms))

	fs, err = newFileStore(fcfg, cfg)
	require_NoError(t, err)
	defer fs.Stop()

	require_Equal(t, fs.SourcesState()["ORIGIN > >"].Seq, last)
}

func TestFileStoreCorruptedNonOrderedSequences(t *testing.T) {
	for _, test := range []struct {
		title   string
		seqs    []uint64
		msgs    uint64
		deleted int
	}{
		{title: "Unordered", seqs: []uint64{1, 3, 2, 4}, msgs: 3, deleted: 1},
		{title: "Duplicated", seqs: []uint64{1, 2, 2, 3}, msgs: 3, deleted: 0},
	} {
		t.Run(test.title, func(t *testing.T) {
			testFileStoreAllPermutations(t, func(t *testing.T, fcfg FileStoreConfig) {
				cfg := StreamConfig{Name: "zzz", Subjects: []string{"foo"}, Storage: FileStorage}
				created := time.Now()
				fs, err := newFileStoreWithCreated(fcfg, cfg, created, prf(&fcfg), nil)
				require_NoError(t, err)
				defer fs.Stop()

				for _, seq := range test.seqs {
					_, err = fs.writeMsgRecord(seq, 0, _EMPTY_, nil, nil)
					require_NoError(t, err)
				}

				fs.mu.RLock()
				lmb := fs.lmb
				fs.mu.RUnlock()

				// The filestore will not yet know that something was corrupt.
				lmb.mu.RLock()
				defer lmb.mu.RUnlock()
				require_Equal(t, lmb.msgs, 4)
				require_Equal(t, lmb.dmap.Size(), 0)

				// Need to reset, otherwise the rebuild will be incorrect.
				atomic.StoreUint64(&lmb.first.seq, 0)

				// Upon rebuild it should realize and correct.
				_, _, err = lmb.rebuildStateLocked()
				require_NoError(t, err)
				require_Equal(t, lmb.msgs, test.msgs)
				require_Equal(t, lmb.dmap.Size(), test.deleted)

				// Indexing should also realize and correct.
				require_True(t, lmb.cacheNotLoaded())
				buf, err := lmb.loadBlock(nil)
				require_NoError(t, err)
				require_NoError(t, lmb.encryptOrDecryptIfNeeded(buf))
				buf, err = lmb.decompressIfNeeded(buf)
				require_NoError(t, err)
				require_NoError(t, lmb.indexCacheBuf(buf))
				require_True(t, lmb.cacheAlreadyLoaded())
			})
		})
	}
}

func BenchmarkFileStoreGetSeqFromTime(b *testing.B) {
	fs, err := newFileStore(
		FileStoreConfig{
			StoreDir:  b.TempDir(),
			BlockSize: 16,
		},
		StreamConfig{
			Name:     "foo",
			Subjects: []string{"foo.>"},
			Storage:  FileStorage,
		},
	)
	require_NoError(b, err)
	defer fs.Stop()

	for range 4096 {
		_, _, err := fs.StoreMsg("foo.bar", nil, []byte{1, 2, 3, 4, 5}, 0)
		require_NoError(b, err)
	}

	fs.mu.RLock()
	fs.blks[0].mu.RLock()
	fs.lmb.mu.RLock()
	start := time.Unix(0, fs.blks[0].first.ts)
	middle := time.Unix(0, fs.blks[0].first.ts+(fs.lmb.last.ts-fs.blks[0].first.ts)/2)
	end := time.Unix(0, fs.lmb.last.ts)
	fs.blks[0].mu.RUnlock()
	fs.lmb.mu.RUnlock()
	fs.mu.RUnlock()

	b.Run("Start", func(b *testing.B) {
		b.ReportAllocs()
		for range b.N {
			fs.GetSeqFromTime(start)
		}
	})

	b.Run("Middle", func(b *testing.B) {
		b.ReportAllocs()
		for range b.N {
			fs.GetSeqFromTime(middle)
		}
	})

	b.Run("End", func(b *testing.B) {
		b.ReportAllocs()
		for range b.N {
			fs.GetSeqFromTime(end)
		}
	})
}

func TestFileStoreCacheLookupOnEmptyBlock(t *testing.T) {
	testFileStoreAllPermutations(t, func(t *testing.T, fcfg FileStoreConfig) {
		cfg := StreamConfig{Name: "zzz", Subjects: []string{"foo"}, Storage: FileStorage}
		created := time.Now()
		fs, err := newFileStoreWithCreated(fcfg, cfg, created, prf(&fcfg), nil)
		require_NoError(t, err)
		defer fs.Stop()

		fs.mu.RLock()
		lmb := fs.lmb
		fs.mu.RUnlock()

		// First make sure that we haven't got a strong reference to the cache.
		require_NotNil(t, lmb)
		lmb.finishedWithCache()
		require_True(t, lmb.cache == nil)

		// Specifically we want ErrStoreMsgNotFound, not errNoCache.
		_, err = lmb.cacheLookup(atomic.LoadUint64(&lmb.first.seq), nil)
		require_Error(t, err, ErrStoreMsgNotFound)

		// Now make sure that we didn't strengthen the reference. This proves
		// that we short-circuited properly.
		require_True(t, lmb.cache == nil)
	})
}

func TestFileStoreEraseMsgDoesNotLoseTombstones(t *testing.T) {
	testFileStoreAllPermutations(t, func(t *testing.T, fcfg FileStoreConfig) {
		cfg := StreamConfig{Name: "zzz", Subjects: []string{"foo"}, Storage: FileStorage}
		created := time.Now()
		fs, err := newFileStoreWithCreated(fcfg, cfg, created, prf(&fcfg), nil)
		require_NoError(t, err)
		defer fs.Stop()

		secret := []byte("secret!")
		// The first message will remain throughout.
		_, _, err = fs.StoreMsg("foo", nil, nil, 0)
		require_NoError(t, err)
		// The second message wil be removed, so a tombstone will be placed.
		_, _, err = fs.StoreMsg("foo", nil, nil, 0)
		require_NoError(t, err)
		// The third message is secret and will be erased.
		_, _, err = fs.StoreMsg("foo", nil, secret, 0)
		require_NoError(t, err)

		// Removing the second message places a tombstone.
		_, err = fs.RemoveMsg(2)
		require_NoError(t, err)

		// A fourth message gets placed after the tombstone.
		_, _, err = fs.StoreMsg("foo", nil, nil, 0)
		require_NoError(t, err)

		// Now we erase the third message.
		// This erases this message and should not lose the tombstone that comes after it.
		_, err = fs.EraseMsg(3)
		require_NoError(t, err)

		before := fs.State()
		require_Equal(t, before.Msgs, 2)
		require_Equal(t, before.FirstSeq, 1)
		require_Equal(t, before.LastSeq, 4)
		require_True(t, slices.Equal(before.Deleted, []uint64{2, 3}))

		_, err = fs.LoadMsg(2, nil)
		require_Error(t, err, errDeletedMsg)
		_, err = fs.LoadMsg(3, nil)
		require_Error(t, err, errDeletedMsg)

		// Make sure we can recover properly with no index.db present.
		fs.Stop()
		os.Remove(filepath.Join(fs.fcfg.StoreDir, msgDir, streamStreamStateFile))

		fs, err = newFileStoreWithCreated(fcfg, cfg, created, prf(&fcfg), nil)
		require_NoError(t, err)
		defer fs.Stop()

		if state := fs.State(); !reflect.DeepEqual(state, before) {
			t.Fatalf("Expected state\n of %+v, \ngot %+v without index.db state", before, state)
		}

		_, err = fs.LoadMsg(2, nil)
		require_Error(t, err, errDeletedMsg)
		_, err = fs.LoadMsg(3, nil)
		require_Error(t, err, errDeletedMsg)
	})
}

func TestFileStoreEraseMsgDoesNotLoseTombstonesInEmptyBlock(t *testing.T) {
	testFileStoreAllPermutations(t, func(t *testing.T, fcfg FileStoreConfig) {
		cfg := StreamConfig{Name: "zzz", Subjects: []string{"foo"}, Storage: FileStorage}
		created := time.Now()
		fs, err := newFileStoreWithCreated(fcfg, cfg, created, prf(&fcfg), nil)
		require_NoError(t, err)
		defer fs.Stop()

		// The first message will remain throughout.
		_, _, err = fs.StoreMsg("foo", nil, nil, 0)
		require_NoError(t, err)
		// The second message wil be removed, so a tombstone will be placed.
		_, _, err = fs.StoreMsg("foo", nil, nil, 0)
		require_NoError(t, err)

		mb, err := fs.newMsgBlockForWrite()
		require_NoError(t, err)

		secret := []byte("secret!")
		// The third message is secret and will be erased.
		_, _, err = fs.StoreMsg("foo", nil, secret, 0)
		require_NoError(t, err)

		// Removing the second message places a tombstone.
		_, err = fs.RemoveMsg(2)
		require_NoError(t, err)

		// Now we erase the third message.
		// This erases this message and should not lose the tombstone that comes after it.
		// It should do the erase, even if the block would be empty afterward as it could contain tombstones.
		_, err = fs.EraseMsg(3)
		require_NoError(t, err)

		before := fs.State()
		require_Equal(t, before.Msgs, 1)
		require_Equal(t, before.FirstSeq, 1)
		require_Equal(t, before.LastSeq, 3)
		require_True(t, slices.Equal(before.Deleted, []uint64{2, 3}))

		_, err = fs.LoadMsg(2, nil)
		require_Error(t, err, errDeletedMsg)
		_, err = fs.LoadMsg(3, nil)
		require_Error(t, err, ErrStoreMsgNotFound)

		// The message should be erased.
		mb.mu.Lock()
		buf, err := mb.loadBlock(nil)
		mb.mu.Unlock()
		require_NoError(t, err)
		require_False(t, bytes.Contains(buf, secret))

		// Make sure we can recover properly with no index.db present.
		fs.Stop()
		os.Remove(filepath.Join(fs.fcfg.StoreDir, msgDir, streamStreamStateFile))

		fs, err = newFileStoreWithCreated(fcfg, cfg, created, prf(&fcfg), nil)
		require_NoError(t, err)
		defer fs.Stop()

		if state := fs.State(); !reflect.DeepEqual(state, before) {
			t.Fatalf("Expected state\n of %+v, \ngot %+v without index.db state", before, state)
		}

		_, err = fs.LoadMsg(2, nil)
		require_Error(t, err, errDeletedMsg)
		_, err = fs.LoadMsg(3, nil)
		require_Error(t, err, ErrStoreMsgNotFound)
	})
}

func TestFileStoreTombstonesNoFirstSeqRollback(t *testing.T) {
	testFileStoreAllPermutations(t, func(t *testing.T, fcfg FileStoreConfig) {
		fcfg.BlockSize = 10 * 33 // 10 messages per block.
		cfg := StreamConfig{Name: "zzz", Subjects: []string{"foo"}, Storage: FileStorage}
		created := time.Now()
		fs, err := newFileStoreWithCreated(fcfg, cfg, created, prf(&fcfg), nil)
		require_NoError(t, err)
		defer fs.Stop()

		for i := 0; i < 20; i++ {
			_, _, err = fs.StoreMsg("foo", nil, nil, 0)
			require_NoError(t, err)
		}

		before := fs.State()
		require_Equal(t, before.Msgs, 20)
		require_Equal(t, before.FirstSeq, 1)
		require_Equal(t, before.LastSeq, 20)

		// Expect 2 blocks with messages.
		fs.mu.RLock()
		lblks := len(fs.blks)
		fs.mu.RUnlock()
		require_Equal(t, lblks, 2)

		// Write some tombstones for all messages, these will be in multiple blocks.
		for seq := uint64(1); seq <= 20; seq++ {
			_, err = fs.RemoveMsg(seq)
			require_NoError(t, err)
		}

		before = fs.State()
		require_Equal(t, before.Msgs, 0)
		require_Equal(t, before.FirstSeq, 21)
		require_Equal(t, before.LastSeq, 20)

		// Expect 1 block purely with tombstones.
		fs.mu.RLock()
		lblks = len(fs.blks)
		fs.mu.RUnlock()
		require_Equal(t, lblks, 1)

		// Make sure we can recover properly with no index.db present.
		fs.Stop()
		os.Remove(filepath.Join(fs.fcfg.StoreDir, msgDir, streamStreamStateFile))

		fs, err = newFileStoreWithCreated(fcfg, cfg, created, prf(&fcfg), nil)
		require_NoError(t, err)
		defer fs.Stop()

		if state := fs.State(); !reflect.DeepEqual(state, before) {
			t.Fatalf("Expected state\n of %+v, \ngot %+v without index.db state", before, state)
		}
	})
}

func TestFileStoreTombstonesSelectNextFirstCleanup(t *testing.T) {
	testFileStoreAllPermutations(t, func(t *testing.T, fcfg FileStoreConfig) {
		fcfg.BlockSize = 10 * 33 // 10 messages per block.
		cfg := StreamConfig{Name: "zzz", Subjects: []string{"foo"}, Storage: FileStorage}
		created := time.Now()
		fs, err := newFileStoreWithCreated(fcfg, cfg, created, prf(&fcfg), nil)
		require_NoError(t, err)
		defer fs.Stop()

		// Write a bunch of messages in multiple blocks.
		for i := 0; i < 50; i++ {
			_, _, err = fs.StoreMsg("foo", nil, nil, 0)
			require_NoError(t, err)
		}

		for seq := uint64(2); seq <= 49; seq++ {
			_, err = fs.RemoveMsg(seq)
			require_NoError(t, err)
		}

		_, err = fs.newMsgBlockForWrite()
		require_NoError(t, err)
		for i := 0; i < 50; i++ {
			_, _, err = fs.StoreMsg("foo", nil, nil, 0)
			require_NoError(t, err)
		}

		for seq := uint64(50); seq <= 100; seq++ {
			_, err = fs.RemoveMsg(seq)
			require_NoError(t, err)
		}

		before := fs.State()
		require_Equal(t, before.Msgs, 1)
		require_Equal(t, before.FirstSeq, 1)
		require_Equal(t, before.LastSeq, 100)

		_, err = fs.RemoveMsg(1)
		require_NoError(t, err)

		before = fs.State()
		require_Equal(t, before.Msgs, 0)
		require_Equal(t, before.FirstSeq, 101)
		require_Equal(t, before.LastSeq, 100)

		// Make sure we can recover properly with no index.db present.
		fs.Stop()
		os.Remove(filepath.Join(fs.fcfg.StoreDir, msgDir, streamStreamStateFile))

		fs, err = newFileStoreWithCreated(fcfg, cfg, created, prf(&fcfg), nil)
		require_NoError(t, err)
		defer fs.Stop()

		if state := fs.State(); !reflect.DeepEqual(state, before) {
			t.Fatalf("Expected state\n of %+v, \ngot %+v without index.db state", before, state)
		}
	})
}

func TestFileStoreTombstonesSelectNextFirstCleanupOnRecovery(t *testing.T) {
	testFileStoreAllPermutations(t, func(t *testing.T, fcfg FileStoreConfig) {
		fcfg.BlockSize = 10 * 33 // 10 messages per block.
		cfg := StreamConfig{Name: "zzz", Subjects: []string{"foo"}, Storage: FileStorage}
		created := time.Now()
		fs, err := newFileStoreWithCreated(fcfg, cfg, created, prf(&fcfg), nil)
		require_NoError(t, err)
		defer fs.Stop()

		// Write a bunch of messages in multiple blocks.
		for i := 0; i < 50; i++ {
			_, _, err = fs.StoreMsg("foo", nil, nil, 0)
			require_NoError(t, err)
		}

		for seq := uint64(2); seq <= 49; seq++ {
			_, err = fs.RemoveMsg(seq)
			require_NoError(t, err)
		}

		_, err = fs.newMsgBlockForWrite()
		require_NoError(t, err)
		for i := 0; i < 50; i++ {
			_, _, err = fs.StoreMsg("foo", nil, nil, 0)
			require_NoError(t, err)
		}

		for seq := uint64(50); seq <= 100; seq++ {
			_, err = fs.RemoveMsg(seq)
			require_NoError(t, err)
		}

		before := fs.State()
		require_Equal(t, before.Msgs, 1)
		require_Equal(t, before.FirstSeq, 1)
		require_Equal(t, before.LastSeq, 100)

		// Explicitly write tombstone instead of calling fs.RemoveMsg,
		// so we need to recover from a hard kill.
		require_NoError(t, fs.writeTombstone(1, 0))
		before = StreamState{FirstSeq: 101, FirstTime: time.Time{}, LastSeq: 100, LastTime: before.LastTime}

		// Make sure we can recover properly with no index.db present.
		fs.Stop()
		os.Remove(filepath.Join(fs.fcfg.StoreDir, msgDir, streamStreamStateFile))

		fs, err = newFileStoreWithCreated(fcfg, cfg, created, prf(&fcfg), nil)
		require_NoError(t, err)
		defer fs.Stop()

		if state := fs.State(); !reflect.DeepEqual(state, before) {
			t.Fatalf("Expected state\n of %+v, \ngot %+v without index.db state", before, state)
		}
	})
}

func TestFileStoreDetectDeleteGapWithLastSkipMsg(t *testing.T) {
	testFileStoreAllPermutations(t, func(t *testing.T, fcfg FileStoreConfig) {
		cfg := StreamConfig{Name: "zzz", Subjects: []string{"foo"}, Storage: FileStorage}
		created := time.Now()
		fs, err := newFileStoreWithCreated(fcfg, cfg, created, prf(&fcfg), nil)
		require_NoError(t, err)
		defer fs.Stop()

		_, _, err = fs.StoreMsg("foo", nil, nil, 0)
		require_NoError(t, err)

		// Skip a message at a sequence such that a gap is created.
		// The gap should be detected later on as deleted messages.
		require_NoError(t, fs.SkipMsgs(2, 3))

		// We should have 3 deletes, one is the skip msg, the other two is the gap.
		before := fs.State()
		require_Equal(t, before.Msgs, 1)
		require_Equal(t, before.FirstSeq, 1)
		require_Equal(t, before.LastSeq, 4)
		require_Equal(t, before.NumDeleted, 3)

		// Make sure we can recover properly with no index.db present.
		fs.Stop()
		os.Remove(filepath.Join(fs.fcfg.StoreDir, msgDir, streamStreamStateFile))

		fs, err = newFileStoreWithCreated(fcfg, cfg, created, prf(&fcfg), nil)
		require_NoError(t, err)
		defer fs.Stop()

		if state := fs.State(); !reflect.DeepEqual(state, before) {
			t.Fatalf("Expected state of %+v, got %+v", before, state)
		}

		mb := fs.getFirstBlock()
		mb.mu.RLock()
		defer mb.mu.RUnlock()
		require_Equal(t, atomic.LoadUint64(&mb.first.seq), 1)
		require_Equal(t, atomic.LoadUint64(&mb.last.seq), 4)
		require_Len(t, mb.dmap.Size(), 3)
	})
}

func TestFileStoreDetectDeleteGapWithOnlySkipMsg(t *testing.T) {
	testFileStoreAllPermutations(t, func(t *testing.T, fcfg FileStoreConfig) {
		cfg := StreamConfig{Name: "zzz", Subjects: []string{"foo"}, Storage: FileStorage}
		created := time.Now()
		fs, err := newFileStoreWithCreated(fcfg, cfg, created, prf(&fcfg), nil)
		require_NoError(t, err)
		defer fs.Stop()

		// Skip messages with a gap.
		_, err = fs.SkipMsg(1)
		require_NoError(t, err)
		require_NoError(t, fs.SkipMsgs(2, 3))

		// We should have no deletes, as the SkipMsgs only move the sequences up.
		before := fs.State()
		require_Equal(t, before.Msgs, 0)
		require_Equal(t, before.FirstSeq, 5)
		require_Equal(t, before.LastSeq, 4)
		require_Equal(t, before.NumDeleted, 0)

		// Make sure we can recover properly with no index.db present.
		fs.Stop()
		os.Remove(filepath.Join(fs.fcfg.StoreDir, msgDir, streamStreamStateFile))

		fs, err = newFileStoreWithCreated(fcfg, cfg, created, prf(&fcfg), nil)
		require_NoError(t, err)
		defer fs.Stop()

		if state := fs.State(); !reflect.DeepEqual(state, before) {
			t.Fatalf("Expected state of %+v, got %+v", before, state)
		}

		// The block should not register the deletes between the two SkipMsgs.
		mb := fs.getFirstBlock()
		mb.mu.RLock()
		defer mb.mu.RUnlock()
		require_Equal(t, atomic.LoadUint64(&mb.first.seq), 5)
		require_Equal(t, atomic.LoadUint64(&mb.last.seq), 4)
		require_Len(t, mb.dmap.Size(), 0)
	})
}

func TestFileStoreEraseMsgErr(t *testing.T) {
	testFileStoreAllPermutations(t, func(t *testing.T, fcfg FileStoreConfig) {
		cfg := StreamConfig{Name: "zzz", Subjects: []string{"foo"}, Storage: FileStorage}
		created := time.Now()
		fs, err := newFileStoreWithCreated(fcfg, cfg, created, prf(&fcfg), nil)
		require_NoError(t, err)
		defer fs.Stop()

		_, _, err = fs.StoreMsg("foo", nil, nil, 0)
		require_NoError(t, err)
		_, _, err = fs.StoreMsg("foo", nil, nil, 0)
		require_NoError(t, err)

		mb := fs.getFirstBlock()
		mb.mu.Lock()
		if mb.cache == nil {
			mb.mu.Unlock()
			t.Fatal("Expected cache to be initialized")
		}
		// Set to a bogus value such that the file rename fails while performing the message erase.
		mb.mfn = _EMPTY_
		mb.mu.Unlock()
		fs.EraseMsg(2)

		// Cleanup ".tmp" file if it was created due to the purposefully invalid file name above.
		_, err = os.Stat(blkTmpSuffix)
		if err == nil {
			require_NoError(t, os.Remove(blkTmpSuffix))
		}
	})
}

func TestFileStorePurgeMsgBlock(t *testing.T) {
	testFileStoreAllPermutations(t, func(t *testing.T, fcfg FileStoreConfig) {
		fcfg.BlockSize = 10 * 33
		cfg := StreamConfig{Name: "zzz", Subjects: []string{"foo"}, Storage: FileStorage}
		created := time.Now()
		fs, err := newFileStoreWithCreated(fcfg, cfg, created, prf(&fcfg), nil)
		require_NoError(t, err)
		defer fs.Stop()

		for range 20 {
			_, _, err = fs.StoreMsg("foo", nil, nil, 0)
			require_NoError(t, err)
		}

		fs.mu.RLock()
		blks := len(fs.blks)
		fs.mu.RUnlock()
		require_Equal(t, blks, 2)

		state := fs.State()
		require_Equal(t, state.FirstSeq, 1)
		require_Equal(t, state.LastSeq, 20)
		require_Equal(t, state.Msgs, 20)
		require_Equal(t, state.Bytes, 20*33)

		// Purging the block should both remove the block and do the accounting.
		fmb := fs.getFirstBlock()
		fs.mu.Lock()
		fs.purgeMsgBlock(fmb, nil)
		blks = len(fs.blks)
		fs.mu.Unlock()

		require_Equal(t, blks, 1)
		state = fs.State()
		require_Equal(t, state.FirstSeq, 11)
		require_Equal(t, state.LastSeq, 20)
		require_Equal(t, state.Msgs, 10)
		require_Equal(t, state.Bytes, 10*33)
	})
}

func TestFileStorePurgeMsgBlockUpdatesSubjects(t *testing.T) {
	testFileStoreAllPermutations(t, func(t *testing.T, fcfg FileStoreConfig) {
		fcfg.BlockSize = 10 * 33
		cfg := StreamConfig{Name: "zzz", Subjects: []string{"foo"}, Storage: FileStorage}
		fs, err := newFileStoreWithCreated(fcfg, cfg, time.Now(), prf(&fcfg), nil)
		require_NoError(t, err)
		defer fs.Stop()

		for range 20 {
			_, _, err = fs.StoreMsg("foo", nil, nil, 0)
			require_NoError(t, err)
		}

		fst := fs.SubjectsTotals("foo")
		require_Equal(t, fst["foo"], uint64(20))

		fmb := fs.getFirstBlock()
		fs.mu.Lock()
		fs.purgeMsgBlock(fmb, nil)
		fs.mu.Unlock()

		state := fs.State()
		require_Equal(t, state.Msgs, uint64(10))
		require_Equal(t, state.FirstSeq, uint64(11))

		fst = fs.SubjectsTotals("foo")
		require_Equal(t, fst["foo"], uint64(10))
	})
}

func TestFileStorePurgeMsgBlockRemovesSchedules(t *testing.T) {
	testFileStoreAllPermutations(t, func(t *testing.T, fcfg FileStoreConfig) {
		schedule := time.Now().Add(time.Hour).Format(time.RFC3339Nano)
		hdr := genHeader(nil, JSSchedulePattern, fmt.Sprintf("@at %s", schedule))
		hdr = genHeader(hdr, JSScheduleTarget, "foo.target.0")
		msgSize := fileStoreMsgSize("foo.sched.0", hdr, []byte("x"))

		// Force two blocks of 5 messages each.
		fcfg.BlockSize = uint64(msgSize * 5)
		cfg := StreamConfig{
			Name:              "zzz",
			Subjects:          []string{"foo.*"},
			Storage:           FileStorage,
			AllowMsgSchedules: true,
		}
		fs, err := newFileStoreWithCreated(fcfg, cfg, time.Now(), prf(&fcfg), nil)
		require_NoError(t, err)
		defer fs.Stop()

		for i := range 10 {
			subj := fmt.Sprintf("foo.sched.%d", i)
			target := fmt.Sprintf("foo.target.%d", i)
			hdr := genHeader(nil, JSSchedulePattern, fmt.Sprintf("@at %s", schedule))
			hdr = genHeader(hdr, JSScheduleTarget, target)
			_, _, err = fs.StoreMsg(subj, hdr, []byte("x"), 0)
			require_NoError(t, err)
		}

		fs.mu.RLock()
		blks := len(fs.blks)
		sts, msgs := len(fs.scheduling.seqToSubj), int(fs.state.Msgs)
		fs.mu.RUnlock()
		require_True(t, blks >= 2)
		require_Equal(t, sts, msgs)

		fmb := fs.getFirstBlock()
		fs.mu.Lock()
		fs.purgeMsgBlock(fmb, nil)
		fs.mu.Unlock()

		state := fs.State()
		require_Equal(t, state.Msgs, uint64(5))

		fs.mu.Lock()
		defer fs.mu.Unlock()
		require_Equal(t, len(fs.scheduling.seqToSubj), int(state.Msgs))
		for seq := uint64(1); seq < state.FirstSeq; seq++ {
			if _, ok := fs.scheduling.seqToSubj[seq]; ok {
				t.Fatalf("expected schedule for seq %d to be removed", seq)
			}
		}
	})
}

func TestFileStorePurgeMsgBlockAccounting(t *testing.T) {
	test := func(t *testing.T, update func(cfg *nats.StreamConfig)) {
		s := RunBasicJetStreamServer(t)
		defer s.Shutdown()

		nc, js := jsClientConnect(t, s)
		defer nc.Close()

		cfg := &nats.StreamConfig{
			Name:     "TEST",
			Subjects: []string{"foo"},
			Storage:  nats.FileStorage,
		}
		_, err := js.AddStream(cfg)
		require_NoError(t, err)

		subj, data := "foo", make([]byte, 1024*1024)
		for range 10 {
			_, err = js.Publish(subj, data)
			require_NoError(t, err)
		}

		gacc := s.globalAccount()
		mset, err := gacc.lookupStream("TEST")
		require_NoError(t, err)
		state := mset.state()
		stats := gacc.JetStreamUsage()
		require_Equal(t, state.Bytes, stats.JetStreamTier.Store)

		update(cfg)
		_, err = js.UpdateStream(cfg)
		require_NoError(t, err)

		state = mset.state()
		stats = gacc.JetStreamUsage()
		require_Equal(t, state.Bytes, fileStoreMsgSizeRaw(len(subj), 0, len(data)))
		require_Equal(t, state.Bytes, stats.JetStreamTier.Store)
	}

	t.Run("MaxMsgs", func(t *testing.T) {
		test(t, func(cfg *nats.StreamConfig) {
			cfg.MaxMsgs = 1
		})
	})
	t.Run("MaxBytes", func(t *testing.T) {
		test(t, func(cfg *nats.StreamConfig) {
			cfg.MaxBytes = int64(fileStoreMsgSizeRaw(3, 0, 1024*1024))
		})
	})
}

func TestFileStoreMissingDeletesAfterCompact(t *testing.T) {
	testFileStoreAllPermutations(t, func(t *testing.T, fcfg FileStoreConfig) {
		cfg := StreamConfig{Name: "zzz", Subjects: []string{"foo"}, Storage: FileStorage}
		created := time.Now()
		fs, err := newFileStoreWithCreated(fcfg, cfg, created, prf(&fcfg), nil)
		require_NoError(t, err)
		defer fs.Stop()

		// Generate a block with 6 messages and then delete the first and last, as well as a larger gap in the middle.
		for range 6 {
			_, _, err = fs.StoreMsg("foo", nil, nil, 0)
			require_NoError(t, err)
		}
		_, err = fs.RemoveMsg(1)
		require_NoError(t, err)
		_, err = fs.RemoveMsg(3)
		require_NoError(t, err)
		_, err = fs.RemoveMsg(4)
		require_NoError(t, err)
		_, err = fs.RemoveMsg(6)
		require_NoError(t, err)

		// We'll compact the deletes later, but shouldn't be lmb.
		fmb := fs.getFirstBlock()
		_, err = fs.newMsgBlockForWrite()
		require_NoError(t, err)

		// Confirm the block's state.
		fmb.mu.Lock()
		defer fmb.mu.Unlock()
		require_Equal(t, atomic.LoadUint64(&fmb.first.seq), 2)
		require_Equal(t, atomic.LoadUint64(&fmb.last.seq), 6)
		require_Equal(t, fmb.msgs, 2)
		require_Len(t, fmb.dmap.Size(), 3)
		require_True(t, fmb.dmap.Exists(3))
		require_True(t, fmb.dmap.Exists(4))
		require_True(t, fmb.dmap.Exists(6))

		// Now compact and reload and the block should still have the correct deletes.
		require_NoError(t, fmb.compact())
		fmb.clearCache()
		fmb.dmap.Empty()
		require_NoError(t, fmb.loadMsgsWithLock())
		require_Equal(t, atomic.LoadUint64(&fmb.first.seq), 2)
		require_Equal(t, atomic.LoadUint64(&fmb.last.seq), 5)
		require_Equal(t, fmb.msgs, 2)
		require_Len(t, fmb.dmap.Size(), 2)
		require_True(t, fmb.dmap.Exists(3))
		require_True(t, fmb.dmap.Exists(4))

		// Rebuilding should have the state remain the same.
		_, _, err = fmb.rebuildStateLocked()
		require_NoError(t, err)
		require_Equal(t, atomic.LoadUint64(&fmb.first.seq), 2)
		require_Equal(t, atomic.LoadUint64(&fmb.last.seq), 5)
		require_Equal(t, fmb.msgs, 2)

		// Delete at sequence 5 such that the block can be compacted to a single message.
		fmb.mu.Unlock()
		_, err = fs.RemoveMsg(5)
		fmb.mu.Lock()
		require_NoError(t, err)
		require_NoError(t, fmb.compact())
		fmb.clearCache()
		fmb.dmap.Empty()
		require_NoError(t, fmb.loadMsgsWithLock())
		require_Equal(t, atomic.LoadUint64(&fmb.first.seq), 2)
		require_Equal(t, atomic.LoadUint64(&fmb.last.seq), 2)
		require_Equal(t, fmb.msgs, 1)
		require_Len(t, fmb.dmap.Size(), 0)

		// Rebuilding should have the state remain the same.
		_, _, err = fmb.rebuildStateLocked()
		require_NoError(t, err)
		require_Equal(t, atomic.LoadUint64(&fmb.first.seq), 2)
		require_Equal(t, atomic.LoadUint64(&fmb.last.seq), 2)
		require_Equal(t, fmb.msgs, 1)
		require_Len(t, fmb.dmap.Size(), 0)
	})
}

func TestFileStoreIdxAccountingForSkipMsgs(t *testing.T) {
	test := func(t *testing.T, skipMany bool) {
		testFileStoreAllPermutations(t, func(t *testing.T, fcfg FileStoreConfig) {
			cfg := StreamConfig{Name: "zzz", Subjects: []string{"foo"}, Storage: FileStorage}
			created := time.Now()
			fs, err := newFileStoreWithCreated(fcfg, cfg, created, prf(&fcfg), nil)
			require_NoError(t, err)
			defer fs.Stop()

			_, _, err = fs.StoreMsg("foo", nil, nil, 0)
			require_NoError(t, err)
			if skipMany {
				require_NoError(t, fs.SkipMsgs(2, 10))
			} else {
				for i := range 10 {
					_, err = fs.SkipMsg(uint64(i + 2))
					require_NoError(t, err)
				}
			}
			_, _, err = fs.StoreMsg("foo", nil, nil, 0)
			require_NoError(t, err)

			fmb := fs.getFirstBlock()
			fmb.mu.Lock()
			defer fmb.mu.Unlock()

			for i := range 12 {
				seq := uint64(i + 1)
				_, err = fmb.cacheLookupNoCopy(seq, nil)
				if seq >= 2 && seq <= 11 {
					require_Error(t, err, errDeletedMsg)
				} else {
					require_NoError(t, err)
				}
			}

			cache := fmb.cache
			require_NotNil(t, cache)
			require_Len(t, len(cache.idx), 12)
		})
	}

	t.Run("SkipMsg", func(t *testing.T) { test(t, false) })
	t.Run("SkipMsgs", func(t *testing.T) { test(t, true) })
}

func TestFileStoreEmptyBlockContainsPriorTombstones(t *testing.T) {
	testFileStoreAllPermutations(t, func(t *testing.T, fcfg FileStoreConfig) {
		cfg := StreamConfig{Name: "zzz", Subjects: []string{"foo"}, Storage: FileStorage}
		created := time.Now()
		fs, err := newFileStoreWithCreated(fcfg, cfg, created, prf(&fcfg), nil)
		require_NoError(t, err)
		defer fs.Stop()

		// 1.blk
		_, _, err = fs.StoreMsg("foo", nil, nil, 0)
		require_NoError(t, err)
		_, _, err = fs.StoreMsg("foo", nil, nil, 0)
		require_NoError(t, err)

		// 2.blk
		_, err = fs.newMsgBlockForWrite()
		require_NoError(t, err)
		_, _, err = fs.StoreMsg("foo", nil, nil, 0)
		require_NoError(t, err)
		_, err = fs.RemoveMsg(2)
		require_NoError(t, err)
		_, err = fs.RemoveMsg(3) // Will create a new lmb with this as tombstone.
		require_NoError(t, err)

		before := fs.State()
		require_Equal(t, before.Msgs, 1)
		require_Equal(t, before.FirstSeq, 1)
		require_Equal(t, before.LastSeq, 3)
		require_True(t, slices.Equal(before.Deleted, []uint64{2, 3}))

		fs.mu.RLock()
		lblks := len(fs.blks)
		fs.mu.RUnlock()
		require_Equal(t, lblks, 3)

		_, err = fs.LoadMsg(2, nil)
		require_Error(t, err, errDeletedMsg)
		_, err = fs.LoadMsg(3, nil)
		require_Error(t, err, ErrStoreMsgNotFound)

		// Make sure we can recover properly with no index.db present.
		fs.Stop()
		os.Remove(filepath.Join(fs.fcfg.StoreDir, msgDir, streamStreamStateFile))

		fs, err = newFileStoreWithCreated(fcfg, cfg, created, prf(&fcfg), nil)
		require_NoError(t, err)
		defer fs.Stop()

		if state := fs.State(); !reflect.DeepEqual(state, before) {
			t.Fatalf("Expected state\n of %+v, \ngot %+v without index.db state", before, state)
		}

		fs.mu.RLock()
		lblks = len(fs.blks)
		fs.mu.RUnlock()
		require_Equal(t, lblks, 3)

		_, err = fs.LoadMsg(2, nil)
		require_Error(t, err, errDeletedMsg)
		_, err = fs.LoadMsg(3, nil)
		require_Error(t, err, ErrStoreMsgNotFound)

		// 3.blk
		_, _, err = fs.StoreMsg("foo", nil, nil, 0)
		require_NoError(t, err)

		fs.mu.RLock()
		lblks = len(fs.blks)
		fs.mu.RUnlock()
		require_Equal(t, lblks, 3)

		// Removing the first message moves the first seq up.
		// Should also remove blocks without any messages and (invalidated) tombstones.
		_, err = fs.RemoveMsg(1)
		require_NoError(t, err)

		fs.mu.RLock()
		lblks = len(fs.blks)
		fs.mu.RUnlock()
		require_Equal(t, lblks, 1)
	})
}

func TestFileStoreCompactTombstonesBelowFirstSeq(t *testing.T) {
	testFileStoreAllPermutations(t, func(t *testing.T, fcfg FileStoreConfig) {
		cfg := StreamConfig{Name: "zzz", Subjects: []string{"foo"}, Storage: FileStorage}
		created := time.Now()
		fs, err := newFileStoreWithCreated(fcfg, cfg, created, prf(&fcfg), nil)
		require_NoError(t, err)
		defer fs.Stop()

		// 1.blk
		_, _, err = fs.StoreMsg("foo", nil, nil, 0)
		require_NoError(t, err)
		_, _, err = fs.StoreMsg("foo", nil, nil, 0)
		require_NoError(t, err)
		_, _, err = fs.StoreMsg("foo", nil, nil, 0)
		require_NoError(t, err)

		// 2.blk
		_, err = fs.newMsgBlockForWrite()
		require_NoError(t, err)
		_, _, err = fs.StoreMsg("foo", nil, nil, 0)
		require_NoError(t, err)
		_, err = fs.RemoveMsg(3)
		require_NoError(t, err)
		_, err = fs.RemoveMsg(2)
		require_NoError(t, err)

		state := fs.State()
		require_Equal(t, state.Msgs, 2)
		require_Equal(t, state.FirstSeq, 1)
		require_Equal(t, state.LastSeq, 4)
		require_True(t, slices.Equal(state.Deleted, []uint64{2, 3}))

		fs.mu.RLock()
		lblks := len(fs.blks)
		fs.mu.RUnlock()
		require_Equal(t, lblks, 2)

		// Block should report the two prior tombstones.
		fs.mu.Lock()
		lmb := fs.lmb
		lmb.mu.Lock()
		priorTombs := lmb.numPriorTombsLocked()
		lmb.mu.Unlock()
		fs.mu.Unlock()
		require_Equal(t, priorTombs, 2)

		// The first sequence moves up as a result of the removal.
		_, err = fs.RemoveMsg(1)
		require_NoError(t, err)

		// Block should now report no prior tombstones, since they are now invalid.
		fs.mu.Lock()
		lmb.mu.Lock()
		priorTombs = lmb.numPriorTombsLocked()
		lmb.mu.Unlock()
		fs.mu.Unlock()
		require_Equal(t, priorTombs, 0)

		// Make sure we have a new last block such that we can compact.
		_, err = fs.newMsgBlockForWrite()
		require_NoError(t, err)

		lmb.mu.RLock()
		rbytes := lmb.rbytes
		shouldCompact := lmb.shouldCompactSync()
		lmb.mu.RUnlock()
		require_True(t, shouldCompact)
		fs.syncBlocks()

		lmb.mu.RLock()
		defer lmb.mu.RUnlock()
		require_NotEqual(t, lmb.rbytes, rbytes)
	})
}

func TestFileStoreSyncBlocksFlushesAndSyncsMessages(t *testing.T) {
	testFileStoreAllPermutations(t, func(t *testing.T, fcfg FileStoreConfig) {
		fcfg.AsyncFlush = true

		fs, err := newFileStoreWithCreated(fcfg, StreamConfig{Name: "zzz", Storage: FileStorage}, time.Now(), prf(&fcfg), nil)
		require_NoError(t, err)
		defer fs.Stop()

		fs.mu.RLock()
		lmb := fs.lmb
		fs.mu.RUnlock()
		require_NotNil(t, lmb)

		// Wait for flusher to be ready.
		checkFor(t, 2*time.Second, 200*time.Millisecond, func() error {
			lmb.mu.RLock()
			defer lmb.mu.RUnlock()
			if !lmb.flusher {
				return errors.New("flusher not active")
			}
			return nil
		})
		// Now shutdown flusher and wait for it to be closed.
		lmb.mu.Lock()
		if lmb.qch != nil {
			close(lmb.qch)
			lmb.qch = nil
		}
		lmb.mu.Unlock()
		checkFor(t, 2*time.Second, 200*time.Millisecond, func() error {
			lmb.mu.RLock()
			defer lmb.mu.RUnlock()
			if lmb.flusher {
				return errors.New("flusher still active")
			}
			return nil
		})

		seq, _, err := fs.StoreMsg("foo", nil, nil, 0)
		require_NoError(t, err)
		require_Equal(t, seq, 1)

		// Update the last write timestamp to be in the past.
		lmb.mu.Lock()
		lwts := lmb.lwts
		lmb.lwts = 0
		lmb.mu.Unlock()
		require_NotEqual(t, lwts, 0)

		// Syncing should write out the data.
		fs.syncBlocks()

		// Manually reset, sync should have written the data.
		lmb.clearCacheAndOffset()

		sm, err := fs.LoadMsg(1, nil)
		require_NoError(t, err)
		require_Equal(t, sm.seq, 1)
		require_Equal(t, sm.subj, "foo")
	})
}

func TestJetStreamFileStoreSubjectsRemovedAfterSecureErase(t *testing.T) {
	s := RunBasicJetStreamServer(t)
	defer s.Shutdown()

	nc, js := jsClientConnect(t, s)
	defer nc.Close()

	_, err := js.AddStream(&nats.StreamConfig{
		Name:     "TEST",
		Subjects: []string{"test.*"},
		Storage:  nats.FileStorage,
	})
	require_NoError(t, err)

	_, err = js.Publish("test.1", []byte("msg1"))
	require_NoError(t, err)
	_, err = js.Publish("test.2", []byte("msg2"))
	require_NoError(t, err)
	_, err = js.Publish("test.3", []byte("msg3"))
	require_NoError(t, err)

	si, err := js.StreamInfo("TEST", &nats.StreamInfoRequest{SubjectsFilter: ">"})
	require_NoError(t, err)
	require_Equal(t, si.State.NumSubjects, 3)
	require_Len(t, len(si.State.Subjects), 3)

	// The bug happened here: the underlying eraseMsg() call in removeMsg() would
	// corrupt the sm.subj from the shallow cache lookup. We would then pass the
	// corrupted subject into removeSeqPerSubject() & removePerSubject(), resulting
	// in them being no-ops. This is now fixed.
	require_NoError(t, js.SecureDeleteMsg("TEST", 1))

	si, err = js.StreamInfo("TEST", &nats.StreamInfoRequest{SubjectsFilter: ">"})
	require_NoError(t, err)
	require_Equal(t, si.State.NumSubjects, 2)
	require_Len(t, len(si.State.Subjects), 2)

	_, exists := si.State.Subjects["test.1"]
	require_False(t, exists)
	require_Equal(t, si.State.Subjects["test.2"], uint64(1))
	require_Equal(t, si.State.Subjects["test.3"], uint64(1))
}

func TestFileStorePreserveLastSeqAfterCompact(t *testing.T) {
	testFileStoreAllPermutations(t, func(t *testing.T, fcfg FileStoreConfig) {
		cfg := StreamConfig{Name: "zzz", Storage: FileStorage}
		created := time.Now()
		fs, err := newFileStoreWithCreated(fcfg, cfg, created, prf(&fcfg), nil)
		require_NoError(t, err)
		defer fs.Stop()

		_, _, err = fs.StoreMsg("foo", nil, nil, 0)
		require_NoError(t, err)

		_, err = fs.Compact(2)
		require_NoError(t, err)

		before := fs.State()
		require_Equal(t, before.Msgs, 0)
		require_Equal(t, before.FirstSeq, 2)
		require_Equal(t, before.LastSeq, 1)

		fs.Stop()
		os.Remove(filepath.Join(fs.fcfg.StoreDir, msgDir, streamStreamStateFile))
		fs, err = newFileStoreWithCreated(fcfg, cfg, created, prf(&fcfg), nil)
		require_NoError(t, err)
		defer fs.Stop()

		if state := fs.State(); !reflect.DeepEqual(state, before) {
			t.Fatalf("Expected state\n of %+v, \ngot %+v without index.db state", before, state)
		}
	})
}

func TestFileStoreSkipMsgAndCompactRequiresAppend(t *testing.T) {
	testFileStoreAllPermutations(t, func(t *testing.T, fcfg FileStoreConfig) {
		fs, err := newFileStoreWithCreated(fcfg, StreamConfig{Name: "zzz", Storage: FileStorage}, time.Now(), prf(&fcfg), nil)
		require_NoError(t, err)
		defer fs.Stop()

		// Store one very long message.
		msg := make([]byte, 256*1024)
		_, _, err = fs.StoreMsg("foo", nil, msg, 0)
		require_NoError(t, err)

		// Skip and compact to that sequence, the block will be preserved and tombstones would be written.
		_, err = fs.SkipMsg(2)
		require_NoError(t, err)
		_, err = fs.Compact(2)
		require_NoError(t, err)

		state := fs.State()
		require_Equal(t, state.Msgs, 0)
		require_Equal(t, state.FirstSeq, 3)
		require_Equal(t, state.LastSeq, 2)

		// Skipping again would result in the bug. We tried to write to the start of the file,
		// but the old message data would still be there. Instead, now add the SkipMsg to the end.
		_, err = fs.SkipMsg(3)
		require_NoError(t, err)

		mb := fs.getFirstBlock()
		mb.mu.Lock()
		defer mb.mu.Unlock()
		mb.clearCacheAndOffset()
		require_NoError(t, mb.loadMsgsWithLock())
	})
}

func TestFileStoreCompactRewritesFileWithSwap(t *testing.T) {
	fcfg := FileStoreConfig{Cipher: NoCipher, Compression: NoCompression, StoreDir: t.TempDir()}
	fs, err := newFileStoreWithCreated(fcfg, StreamConfig{Name: "zzz", Storage: FileStorage}, time.Now(), prf(&fcfg), nil)
	require_NoError(t, err)
	defer fs.Stop()

	msg := make([]byte, 256*1024)
	for range 20 {
		_, _, err = fs.StoreMsg("foo", nil, msg, 0)
		require_NoError(t, err)
	}

	// Compact should realize the block's data was largely removed, the file should be rewritten
	_, err = fs.Compact(20)
	require_NoError(t, err)

	state := fs.State()
	require_Equal(t, state.Msgs, 1)
	require_Equal(t, state.FirstSeq, 20)
	require_Equal(t, state.LastSeq, 20)

	mb := fs.getFirstBlock()
	mb.mu.Lock()
	defer mb.mu.Unlock()
	mb.clearCacheAndOffset()
	require_NoError(t, mb.loadMsgsWithLock())

	mbcache := mb.cache
	require_NotNil(t, mbcache)
	require_Len(t, len(mbcache.idx), 1)
	require_Equal(t, mbcache.idx[0], 0)
}

func TestFileStoreCompactSync(t *testing.T) {
	fs, err := newFileStore(
		FileStoreConfig{StoreDir: t.TempDir(), BlockSize: defaultMediumBlockSize, SyncAlways: true, SyncInterval: time.Hour},
		StreamConfig{Name: "WAL", Storage: FileStorage},
	)
	require_NoError(t, err)
	defer fs.Stop()

	// Fill one block past the compact threshold
	mb := fs.getFirstBlock()
	msg := make([]byte, 256*1024)
	var seq, rbytes uint64
	for rbytes <= compactMinimum {
		seq, _, err = fs.StoreMsg(_EMPTY_, nil, msg, 0)
		require_NoError(t, err)
		mb.mu.RLock()
		rbytes = mb.rbytes
		mb.mu.RUnlock()
	}
	fs.syncBlocks()
	require_Equal(t, fs.numMsgBlocks(), 1)

	// Compact to seq so that a new block is written, containing only the last entry.
	// Verify that with SyncAlways the new block file does not need sync.
	purged, err := fs.Compact(seq)
	require_NoError(t, err)
	require_Equal(t, purged, seq-1)
	mb.mu.RLock()
	defer mb.mu.RUnlock()
	require_LessThan(t, mb.rbytes, rbytes)
	require_False(t, mb.needSync)
}

func TestFileStoreIndexCacheBufIdxMismatch(t *testing.T) {
	const (
		KindTruncateFull = iota
		KindTruncatePartial
		KindCompactHead
		KindCompactTail
		KindCompact
	)
	test := func(t *testing.T, kind int) {
		testFileStoreAllPermutations(t, func(t *testing.T, fcfg FileStoreConfig) {
			fs, err := newFileStoreWithCreated(fcfg, StreamConfig{Name: "zzz", Storage: FileStorage}, time.Now(), prf(&fcfg), nil)
			require_NoError(t, err)
			defer fs.Stop()

			for range 5 {
				_, _, err = fs.StoreMsg("foo", nil, nil, 0)
				require_NoError(t, err)
			}

			mb := fs.getFirstBlock()
			mb.mu.Lock()
			defer mb.mu.Unlock()

			var deleted int
			compactHead := kind == KindCompactHead || kind == KindCompact
			compactTail := kind == KindCompactTail || kind == KindCompact
			if compactHead || compactTail {
				require_Equal(t, atomic.LoadUint64(&mb.first.seq), 1)
				require_Equal(t, atomic.LoadUint64(&mb.last.seq), 5)
				if compactHead {
					atomic.StoreUint64(&mb.first.seq, 2)
					deleted++
				}
				if compactTail {
					mb.dmap.Insert(5)
					deleted++
				}
				require_NoError(t, mb.compactWithFloor(0, nil))

				// Revert the state, upon reload we need to recognize this.
				atomic.StoreUint64(&mb.first.seq, 1)
				atomic.StoreUint64(&mb.last.seq, 5)
			}

			mb.clearCacheAndOffset()
			switch kind {
			case KindTruncateFull:
				// Truncate to be empty.
				require_NoError(t, os.Truncate(mb.mfn, 0))

				// When loading messages, we should realize our in-memory state doesn't match what's on disk.
				// The block needs to be rebuilt and re-indexed.
				require_NoError(t, mb.loadMsgsWithLock())
				require_Equal(t, mb.msgs, 0)
				require_Equal(t, atomic.LoadUint64(&mb.first.seq), 6)
				require_Equal(t, atomic.LoadUint64(&mb.last.seq), 5)
				for i := range 5 {
					seq := uint64(i + 1)
					_, err = mb.cacheLookup(seq, nil)
					require_Error(t, err, ErrStoreMsgNotFound)
				}
			case KindTruncatePartial:
				// Truncate to half of the original file.
				stat, err := os.Stat(mb.mfn)
				require_NoError(t, err)
				require_NoError(t, os.Truncate(mb.mfn, stat.Size()/2))

				// When loading messages, we should realize our in-memory state doesn't match what's on disk.
				// The block needs to be rebuilt and re-indexed.
				require_NoError(t, mb.loadMsgsWithLock())
				require_Equal(t, mb.msgs, 2)
				require_Equal(t, atomic.LoadUint64(&mb.first.seq), 1)
				require_Equal(t, atomic.LoadUint64(&mb.last.seq), 2)
				for i := range 5 {
					seq := uint64(i + 1)
					_, err = mb.cacheLookup(seq, nil)
					if seq <= 2 {
						require_NoError(t, err)
					} else {
						require_Error(t, err, ErrStoreMsgNotFound)
					}
				}
			case KindCompact, KindCompactHead, KindCompactTail:
				// Since we've reverted the state after compacting, our in-memory state doesn't match what's on disk.
				// The block needs to be rebuilt and re-indexed.
				require_NoError(t, mb.loadMsgsWithLock())
				require_Equal(t, mb.msgs, uint64(5-deleted))
				if compactHead {
					require_Equal(t, atomic.LoadUint64(&mb.first.seq), 2)
				} else {
					require_Equal(t, atomic.LoadUint64(&mb.first.seq), 1)
				}
				if compactTail {
					require_Equal(t, atomic.LoadUint64(&mb.last.seq), 4)
				} else {
					require_Equal(t, atomic.LoadUint64(&mb.last.seq), 5)
				}
				for i := range 5 {
					seq := uint64(i + 1)
					_, err = mb.cacheLookup(seq, nil)
					if seq == 1 && compactHead {
						require_Error(t, err, ErrStoreMsgNotFound)
					} else if seq == 5 && compactTail {
						require_Error(t, err, ErrStoreMsgNotFound)
					} else {
						require_NoError(t, err)
					}
				}
			}
		})
	}

	t.Run("TruncateFull", func(t *testing.T) { test(t, KindTruncateFull) })
	t.Run("TruncatePartial", func(t *testing.T) { test(t, KindTruncatePartial) })
	t.Run("CompactHead", func(t *testing.T) { test(t, KindCompactHead) })
	t.Run("CompactTail", func(t *testing.T) { test(t, KindCompactTail) })
	t.Run("Compact", func(t *testing.T) { test(t, KindCompact) })
}

func TestFileStoreIndexCacheBufTombstoneMismatch(t *testing.T) {
	testFileStoreAllPermutations(t, func(t *testing.T, fcfg FileStoreConfig) {
		fs, err := newFileStoreWithCreated(fcfg, StreamConfig{Name: "zzz", Storage: FileStorage}, time.Now(), prf(&fcfg), nil)
		require_NoError(t, err)
		defer fs.Stop()

		for range 3 {
			_, _, err = fs.StoreMsg("foo", nil, nil, 0)
			require_NoError(t, err)
		}
		_, err = fs.RemoveMsg(2)
		require_NoError(t, err)

		mb := fs.getFirstBlock()
		mb.mu.Lock()
		defer mb.mu.Unlock()

		// Not only clear the cache, also clear the dmap.
		// indexCacheBuf should still properly index.
		mb.clearCacheAndOffset()
		mb.dmap.Empty()
		require_NoError(t, mb.loadMsgsWithLock())

		mbcache := mb.cache
		require_NotNil(t, mbcache)
		require_Len(t, len(mbcache.idx), 3)
		require_Equal(t, mbcache.idx[0], 0)
		require_Equal(t, mbcache.idx[1], 33)
		require_Equal(t, mbcache.idx[2], 66)
	})
}

func TestFileStoreIndexCacheBufTombstoneMismatchAfterCompact(t *testing.T) {
	testFileStoreAllPermutations(t, func(t *testing.T, fcfg FileStoreConfig) {
		fs, err := newFileStoreWithCreated(fcfg, StreamConfig{Name: "zzz", Storage: FileStorage}, time.Now(), prf(&fcfg), nil)
		require_NoError(t, err)
		defer fs.Stop()

		for range 3 {
			_, _, err = fs.StoreMsg("foo", nil, nil, 0)
			require_NoError(t, err)
		}
		_, err = fs.RemoveMsg(2)
		require_NoError(t, err)

		mb := fs.getFirstBlock()
		mb.mu.Lock()
		defer mb.mu.Unlock()

		require_NoError(t, mb.compactWithFloor(0, nil))

		// Not only clear the cache, also clear the dmap.
		mb.clearCacheAndOffset()
		mb.dmap.Empty()
		require_NoError(t, mb.loadMsgsWithLock())

		require_Equal(t, mb.dmap.Size(), 1)
		require_True(t, mb.dmap.Exists(2))
		mbcache := mb.cache
		require_NotNil(t, mbcache)
		require_Len(t, len(mbcache.idx), 3)
		require_Equal(t, mbcache.idx[0], 0)
		// Since the block was compacted, this will be both dbit and exist in the dmap which is checked above.
		require_Equal(t, mbcache.idx[1], dbit)
		// Due to the compact, this message will be one entry earlier in the file than it was before.
		require_Equal(t, mbcache.idx[2], 33)
	})
}

func TestFileStoreIndexCacheBufEraseMsgMismatch(t *testing.T) {
	fcfg := FileStoreConfig{Cipher: NoCipher, Compression: NoCompression, StoreDir: t.TempDir()}
	fs, err := newFileStoreWithCreated(fcfg, StreamConfig{Name: "zzz", Storage: FileStorage}, time.Now(), prf(&fcfg), nil)
	require_NoError(t, err)
	defer fs.Stop()

	for range 3 {
		_, _, err = fs.StoreMsg("foo", nil, nil, 0)
		require_NoError(t, err)
	}
	_, err = fs.EraseMsg(2)
	require_NoError(t, err)

	// Revert the state in the filestore itself, as well as the block.
	// This simulates recovery based on a stale stream state file that should be recognized during runtime.
	before := fs.State()
	fs.mu.Lock()
	msgs := fs.state.Msgs
	fs.state.Msgs = 3
	fs.mu.Unlock()
	require_Equal(t, msgs, 2)

	mb := fs.getFirstBlock()
	mb.mu.Lock()
	defer mb.mu.Unlock()

	// Not only clear the cache, also clear the dmap.
	// indexCacheBuf should still properly index.
	mb.clearCacheAndOffset()
	mb.dmap.Empty()
	require_NoError(t, mb.loadMsgsWithLock())

	mbcache := mb.cache
	require_NotNil(t, mbcache)
	require_Len(t, len(mbcache.idx), 3)
	require_Equal(t, mbcache.idx[0], 0)
	require_Equal(t, mbcache.idx[1], 33)
	require_Equal(t, mbcache.idx[2], 66)

	// This looks backwards but is intentional. The above asserts unlock based on the defer if they fail,
	// so need to do the reverse here while we inspect the filestore as a whole.
	mb.mu.Unlock()
	defer mb.mu.Lock()

	checkFor(t, 2*time.Second, 100*time.Millisecond, func() error {
		if state := fs.State(); !reflect.DeepEqual(state, before) {
			return fmt.Errorf("expected state\n of %+v,\n got %+v", before, state)
		}
		return nil
	})
}

func TestFileStoreCompactRestoresLastSeq(t *testing.T) {
	testFileStoreAllPermutations(t, func(t *testing.T, fcfg FileStoreConfig) {
		fs, err := newFileStoreWithCreated(fcfg, StreamConfig{Name: "zzz", Storage: FileStorage}, time.Now(), prf(&fcfg), nil)
		require_NoError(t, err)
		defer fs.Stop()

		var tss []int64
		for range 4 {
			_, ts, err := fs.StoreMsg("foo", nil, nil, 0)
			require_NoError(t, err)
			tss = append(tss, ts)
		}

		checkMbState := func(fseq, lseq, msgs uint64) {
			t.Helper()
			mb := fs.getFirstBlock()
			mb.mu.Lock()
			defer mb.mu.Unlock()

			require_Equal(t, atomic.LoadUint64(&mb.first.seq), fseq)
			require_Equal(t, atomic.LoadUint64(&mb.last.seq), lseq)
			require_Equal(t, mb.first.ts, tss[fseq-1])
			require_Equal(t, mb.last.ts, tss[lseq-1])
			require_Equal(t, mb.msgs, msgs)
			deletes := lseq - fseq + 1 - msgs
			require_Equal(t, mb.dmap.Size(), int(deletes))
		}
		checkMbState(1, 4, 4)

		_, err = fs.RemoveMsg(1)
		require_NoError(t, err)
		checkMbState(2, 4, 3)

		_, err = fs.RemoveMsg(4)
		require_NoError(t, err)
		checkMbState(2, 4, 2)

		mb := fs.getFirstBlock()
		mb.mu.Lock()
		err = mb.compactWithFloor(0, nil)
		mb.mu.Unlock()
		require_NoError(t, err)
		checkMbState(2, 3, 2)
	})
}

func TestFileStoreCompactFullyResetsFirstAndLastSeq(t *testing.T) {
	testFileStoreAllPermutations(t, func(t *testing.T, fcfg FileStoreConfig) {
		fs, err := newFileStoreWithCreated(fcfg, StreamConfig{Name: "zzz", Storage: FileStorage}, time.Now(), prf(&fcfg), nil)
		require_NoError(t, err)
		defer fs.Stop()

		var tss []int64
		for range 2 {
			_, ts, err := fs.StoreMsg("foo", nil, nil, 0)
			require_NoError(t, err)
			tss = append(tss, ts)
		}

		checkMbState := func(fseq, lseq, msgs uint64) {
			t.Helper()
			mb := fs.getFirstBlock()
			mb.mu.Lock()
			defer mb.mu.Unlock()

			require_Equal(t, atomic.LoadUint64(&mb.first.seq), fseq)
			require_Equal(t, mb.first.ts, tss[fseq-1])
			if lseq == 0 {
				require_Equal(t, atomic.LoadUint64(&mb.last.seq), fseq-1)
				require_Equal(t, mb.last.ts, 0)
			} else {
				require_Equal(t, atomic.LoadUint64(&mb.last.seq), lseq)
				require_Equal(t, mb.last.ts, tss[lseq-1])
			}
			require_Equal(t, mb.msgs, msgs)
			deletes := lseq - fseq + 1 - msgs
			require_Equal(t, mb.dmap.Size(), int(deletes))
		}
		checkMbState(1, 2, 2)

		mb := fs.getFirstBlock()
		// Load the cache before compacting so that the compact itself needs to (re)load it.
		require_NoError(t, mb.loadMsgs())
		mb.mu.Lock()
		// Manually 'delete' the whole block contents such that a call to rebuildState and indexCacheBuf
		// can't fix this. A call to compact needs to end with correctly resetting the first and last seq.
		mb.dmap.Insert(1)
		mb.dmap.Insert(2)
		mb.msgs = 0
		err = mb.compactWithFloor(0, nil)
		mb.mu.Unlock()
		require_NoError(t, err)
		checkMbState(1, 0, 0)
	})
}

func TestFileStoreCompactHeadReclaimRecompressedBlock(t *testing.T) {
	testFileStoreAllPermutations(t, func(t *testing.T, fcfg FileStoreConfig) {
		fcfg.BlockSize = 8 * 1024 * 1024 // Large so everything lands in one block.
		cfg := StreamConfig{Name: "zzz", Subjects: []string{"zzz"}, Storage: FileStorage}
		fs, err := newFileStoreWithCreated(fcfg, cfg, time.Now(), prf(&fcfg), nil)
		require_NoError(t, err)
		defer fs.Stop()

		// Use incompressible (random) payloads so the on-disk rbytes stays above
		// compactMinimum (2MB) even after S2 compression.
		const num = 50
		msgs := make([][]byte, num)
		for i := range num {
			m := make([]byte, 64*1024)
			_, err = crand.Read(m)
			require_NoError(t, err)
			msgs[i] = m
			_, _, err = fs.StoreMsg("zzz", nil, m, 0)
			require_NoError(t, err)
		}

		// Force the active block to be recompressed on disk so that smb.cmp
		// matches the configured algorithm, emulating the post-rotation state
		// that triggers the head-reclaim path. For a NoCompression store this is
		// a no-op and smb.cmp stays NoCompression.
		fs.mu.RLock()
		smb := fs.lmb
		fs.mu.RUnlock()
		smb.mu.Lock()
		require_NoError(t, smb.recompressOnDiskIfNeeded())
		cmp := smb.cmp
		rbytes := smb.rbytes
		smb.mu.Unlock()
		require_Equal(t, cmp, fcfg.Compression)
		require_True(t, rbytes > compactMinimum)

		// Compact away most of the block, leaving the tail. This drives the
		// head-space reclaim branch which rewrites the block on disk.
		_, err = fs.Compact(num - 5)
		require_NoError(t, err)

		// Sanity check we can still read the remaining messages in-process.
		var smv StoreMsg
		for seq := uint64(num - 5); seq <= num; seq++ {
			sm, err := fs.LoadMsg(seq, &smv)
			require_NoError(t, err)
			require_True(t, bytes.Equal(sm.msg, msgs[seq-1]))
		}

		// Now reload from disk: this exercises decompressIfNeeded, which is where
		// a missing CompressionInfo header would surface as a corrupt block.
		require_NoError(t, fs.Stop())
		fs, err = newFileStoreWithCreated(fcfg, cfg, time.Now(), prf(&fcfg), nil)
		require_NoError(t, err)
		defer fs.Stop()

		var state StreamState
		fs.FastState(&state)
		require_Equal(t, state.Msgs, 6)
		for seq := uint64(num - 5); seq <= num; seq++ {
			sm, err := fs.LoadMsg(seq, &smv)
			require_NoError(t, err)
			require_True(t, bytes.Equal(sm.msg, msgs[seq-1]))
		}
	})
}

func TestFileStoreDoesntRebuildSubjectStateWithNoTrack(t *testing.T) {
	testFileStoreAllPermutations(t, func(t *testing.T, fcfg FileStoreConfig) {
		fs, err := newFileStoreWithCreated(fcfg, StreamConfig{Name: "zzz", Storage: FileStorage}, time.Now(), prf(&fcfg), nil)
		require_NoError(t, err)
		defer fs.Stop()

		_, _, err = fs.StoreMsg("foo", nil, nil, 0)
		require_NoError(t, err)

		// This implicitly was calling resetGlobalPerSubjectInfo and
		// populating "foo" back into the psim.
		require_NoError(t, fs.Truncate(1))

		fs.mu.Lock()
		defer fs.mu.Unlock()
		require_Equal(t, fs.psim.Size(), 0)
	})
}

func TestFileStoreLoadNextMsgSkipAhead(t *testing.T) {
	test := func(t *testing.T, headStart, singleSubjPerBlock bool) {
		testFileStoreAllPermutations(t, func(t *testing.T, fcfg FileStoreConfig) {
			cfg := StreamConfig{Name: "zzz", Storage: FileStorage, Subjects: []string{">"}}
			fs, err := newFileStoreWithCreated(fcfg, cfg, time.Now(), prf(&fcfg), nil)
			require_NoError(t, err)
			defer fs.Stop()

			for i := range 10 {
				subj := "a"
				if !singleSubjPerBlock && i%2 == 0 {
					subj = "c"
				}
				_, _, err = fs.StoreMsg(subj, nil, nil, 0)
				require_NoError(t, err)
				if (i+1)%2 == 0 {
					_, err = fs.newMsgBlockForWrite()
					require_NoError(t, err)
				}
			}
			_, _, err = fs.StoreMsg("b", nil, nil, 0)
			require_NoError(t, err)

			fs.mu.Lock()
			fs.lockAllMsgBlocks()
			for _, mb := range fs.blks {
				mb.lsts = 0
			}
			fs.unlockAllMsgBlocks()
			fs.mu.Unlock()

			var start uint64
			if !headStart {
				start = 3
			}

			// Check we properly skip over blocks that aren't relevant for our filter subject.
			// If we're starting with start=0, we should skip to the correct block immediately.
			// Otherwise, we should only need to access one block and then skip to the correct one.
			sm, nseq, err := fs.LoadNextMsg("b", false, start, nil)
			require_NoError(t, err)
			require_Equal(t, sm.seq, 11)
			require_Equal(t, nseq, 11)

			fs.mu.Lock()
			defer fs.mu.Unlock()
			fs.lockAllMsgBlocks()
			defer fs.unlockAllMsgBlocks()
			for _, mb := range fs.blks {
				t.Logf("mb %d: lsts=%d", mb.index, mb.lsts)
				if mb.index == 6 || (mb.index == 2 && !headStart) {
					require_NotEqual(t, mb.lsts, 0)
				} else {
					require_Equal(t, mb.lsts, 0)
				}
			}
		})
	}

	for _, singleSubjPerBlock := range []bool{true, false} {
		title := "Single"
		if !singleSubjPerBlock {
			title = "Multi"
		}
		t.Run(fmt.Sprintf("%s/Head", title), func(t *testing.T) { test(t, true, singleSubjPerBlock) })
		t.Run(fmt.Sprintf("%s/Interior", title), func(t *testing.T) { test(t, false, singleSubjPerBlock) })
	}
}

func TestFileStoreLoadNextMsgMultiSkipAhead(t *testing.T) {
	test := func(t *testing.T, headStart, singleSubjPerBlock bool) {
		testFileStoreAllPermutations(t, func(t *testing.T, fcfg FileStoreConfig) {
			cfg := StreamConfig{Name: "zzz", Storage: FileStorage, Subjects: []string{">"}}
			fs, err := newFileStoreWithCreated(fcfg, cfg, time.Now(), prf(&fcfg), nil)
			require_NoError(t, err)
			defer fs.Stop()

			for i := range 10 {
				subj := "a"
				if !singleSubjPerBlock && i%2 == 0 {
					subj = "c"
				}
				_, _, err = fs.StoreMsg(subj, nil, nil, 0)
				require_NoError(t, err)
				if (i+1)%2 == 0 {
					_, err = fs.newMsgBlockForWrite()
					require_NoError(t, err)
				}
			}
			_, _, err = fs.StoreMsg("b", nil, nil, 0)
			require_NoError(t, err)

			fs.mu.Lock()
			fs.lockAllMsgBlocks()
			for _, mb := range fs.blks {
				mb.lsts = 0
			}
			fs.unlockAllMsgBlocks()
			fs.mu.Unlock()

			var start uint64
			if !headStart {
				start = 3
			}

			// Check we properly skip over blocks that aren't relevant for our filter subjects.
			// If we're starting with start=0, we should skip to the correct block immediately.
			// Otherwise, we should only need to access one block and then skip to the correct one.
			filters := gsl.NewSublist[struct{}]()
			require_NoError(t, filters.Insert("b", struct{}{}))
			require_NoError(t, filters.Insert("d", struct{}{}))
			sm, nseq, err := fs.LoadNextMsgMulti(filters, start, nil)
			require_NoError(t, err)
			require_Equal(t, sm.seq, 11)
			require_Equal(t, nseq, 11)

			fs.mu.Lock()
			defer fs.mu.Unlock()
			fs.lockAllMsgBlocks()
			defer fs.unlockAllMsgBlocks()
			for _, mb := range fs.blks {
				t.Logf("mb %d: lsts=%d", mb.index, mb.lsts)
				if mb.index == 6 || (mb.index == 2 && !headStart) {
					require_NotEqual(t, mb.lsts, 0)
				} else {
					require_Equal(t, mb.lsts, 0)
				}
			}
		})
	}

	for _, singleSubjPerBlock := range []bool{true, false} {
		title := "Single"
		if !singleSubjPerBlock {
			title = "Multi"
		}
		t.Run(fmt.Sprintf("%s/Head", title), func(t *testing.T) { test(t, true, singleSubjPerBlock) })
		t.Run(fmt.Sprintf("%s/Interior", title), func(t *testing.T) { test(t, false, singleSubjPerBlock) })
	}
}

func BenchmarkFileStoreCheckSkipFirstBlockMultiTippingPoint(b *testing.B) {
	if testing.Short() {
		b.Skip("performance-oriented benchmark")
	}

	start := uint64(2)
	cases := []int{1_000, 10_000, 100_000, 500_000, 1_000_000, 1_500_000, 2_000_000}
	const fillerBlocks = 4096
	const totalMsgs = 4_000_000
	msg := []byte("ok")

	for _, uniqueSubjects := range cases {
		b.Run(fmt.Sprintf("UniqueSubjects/%d", uniqueSubjects), func(b *testing.B) {
			fs, err := newFileStore(
				FileStoreConfig{
					StoreDir:  b.TempDir(),
					BlockSize: 1 << 20,
				},
				StreamConfig{Name: "zzz", Subjects: []string{"foo.>"}, Storage: FileStorage})
			require_NoError(b, err)
			defer func() { _ = fs.Stop() }()

			for i := range fillerBlocks {
				_, _, err = fs.StoreMsg("foo.fill", nil, msg, 0)
				require_NoError(b, err)
				if i < fillerBlocks-1 {
					_, err = fs.newMsgBlockForWrite()
					require_NoError(b, err)
				}
			}

			matchingMsgs := totalMsgs - fillerBlocks
			for i := range matchingMsgs {
				subj := fmt.Sprintf("foo.%d.bar", i%uniqueSubjects)
				_, _, err = fs.StoreMsg(subj, nil, msg, 0)
				require_NoError(b, err)
			}

			sl := gsl.NewSublist[struct{}]()
			require_NoError(b, sl.Insert("foo.*.bar", struct{}{}))
			require_NoError(b, sl.Insert("zzz.nope", struct{}{}))

			var sm StoreMsg
			_, _, err = fs.LoadNextMsgMulti(sl, start, &sm)
			require_NoError(b, err)

			fs.mu.RLock()
			psimSize := fs.psim.Size()
			fs.mu.RUnlock()

			b.ReportMetric(float64(psimSize), "psim")

			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				_, _, err = fs.LoadNextMsgMulti(sl, start, &sm)
				require_NoError(b, err)
			}
		})
	}
}

func TestFileStoreDeleteRangeTwoGaps(t *testing.T) {
	fcfg := FileStoreConfig{
		Cipher:      NoCipher,
		Compression: NoCompression,
		StoreDir:    t.TempDir(),
		BlockSize:   256,
	}
	fs, err := newFileStoreWithCreated(fcfg, StreamConfig{Name: "zzz", Storage: FileStorage}, time.Now(), prf(&fcfg), nil)
	require_NoError(t, err)
	defer fs.Stop()

	checkDeleteBlocks := func(exp DeleteBlocks) {
		t.Helper()
		dBlocks, _ := fs.deleteBlocks()
		require_Equal(t, len(exp), len(dBlocks))

		for i, found := range dBlocks {
			ef, el, en := exp[i].State()
			ff, fl, fn := found.State()

			require_Equal(t, reflect.TypeOf(exp[i]), reflect.TypeOf(found))
			require_Equal(t, ef, ff)
			require_Equal(t, el, fl)
			require_Equal(t, en, fn)
		}
	}

	msg := make([]byte, 256)
	for range 20 {
		_, _, err = fs.StoreMsg("foo", nil, msg, 0)
		require_NoError(t, err)
	}

	// Initially, we have sequences [1,20].
	// Each message fills one entire block
	require_Equal(t, len(fs.blks), 20)

	// Make two gaps by removing sequences 10 and 15
	// [1,9] [11,14] [16,20]
	fs.RemoveMsg(10)
	fs.RemoveMsg(15)

	// If bug is present: deleteBlocks extends the first gap
	// to the second, even though non-deleted blocks are in
	// between the two gaps. Expect two distinct gaps.
	checkDeleteBlocks(DeleteBlocks{
		&DeleteRange{First: 10, Num: 1},
		&DeleteRange{First: 15, Num: 1},
	})
}

func TestFileStoreDeleteBlocksWithSingleMessageBlocks(t *testing.T) {
	fcfg := FileStoreConfig{
		Cipher:      NoCipher,
		Compression: NoCompression,
		StoreDir:    t.TempDir(),
		BlockSize:   256,
	}
	fs, err := newFileStoreWithCreated(fcfg, StreamConfig{Name: "zzz", Storage: FileStorage}, time.Now(), prf(&fcfg), nil)
	require_NoError(t, err)
	defer fs.Stop()

	checkDeleteBlocks := func(exp DeleteBlocks) {
		dBlocks, _ := fs.deleteBlocks()
		require_Equal(t, len(exp), len(dBlocks))

		for i, found := range dBlocks {
			ef, el, en := exp[i].State()
			ff, fl, fn := found.State()

			require_Equal(t, reflect.TypeOf(exp[i]), reflect.TypeOf(found))
			require_Equal(t, ef, ff)
			require_Equal(t, el, fl)
			require_Equal(t, en, fn)
		}
	}

	msg := make([]byte, 256)
	for range 20 {
		_, _, err = fs.StoreMsg("foo", nil, msg, 0)
		require_NoError(t, err)
	}

	// Initially, we have sequences [1,20].
	// Each message fills one entire block
	require_Equal(t, len(fs.blks), 20)

	// Removing sequence 10 leaves a gap
	// [1,9] [11,20]
	fs.RemoveMsg(10)
	checkDeleteBlocks(DeleteBlocks{
		&DeleteRange{First: 10, Num: 1},
	})

	// Extend the gap by removing 11
	// [1,9] [12,20]
	fs.RemoveMsg(11)
	checkDeleteBlocks(DeleteBlocks{
		&DeleteRange{First: 10, Num: 2},
	})

	// Make another gap by removing 5
	// [1,4] [6,9] [12,20]
	fs.RemoveMsg(5)
	checkDeleteBlocks(DeleteBlocks{
		&DeleteRange{First: 5, Num: 1},
		&DeleteRange{First: 10, Num: 2},
	})

	// Remove blocks in the middle
	// [1,4] [12,20]
	fs.RemoveMsg(6)
	fs.RemoveMsg(7)
	fs.RemoveMsg(8)
	fs.RemoveMsg(9)
	checkDeleteBlocks(DeleteBlocks{
		&DeleteRange{First: 5, Num: 7},
	})

	// Make a couple more gaps by removing 3 and 16,17,18
	// [1,2] [4,4] [12,15] [19,20]
	fs.RemoveMsg(3)
	fs.RemoveMsg(16)
	fs.RemoveMsg(17)
	fs.RemoveMsg(18)
	checkDeleteBlocks(DeleteBlocks{
		&DeleteRange{First: 3, Num: 1},
		&DeleteRange{First: 5, Num: 7},
		&DeleteRange{First: 16, Num: 3},
	})

	// Removing the first sequence make no difference in gaps
	// [2,2] [4,4] [12,15] [19,20]
	fs.RemoveMsg(1)
	checkDeleteBlocks(DeleteBlocks{
		&DeleteRange{First: 3, Num: 1},
		&DeleteRange{First: 5, Num: 7},
		&DeleteRange{First: 16, Num: 3},
	})

	// Removing the last sequence creates a gap
	// [2,2] [4,4] [12,15] [19,19] (empty block starting at 21)
	fs.RemoveMsg(20)
	checkDeleteBlocks(DeleteBlocks{
		&DeleteRange{First: 3, Num: 1},
		&DeleteRange{First: 5, Num: 7},
		&DeleteRange{First: 16, Num: 3},
		&DeleteRange{First: 20, Num: 1},
	})

	// Extend the last gap by removing 19
	// [2,2] [4,4] [12,15] (empty block starting at 21)
	fs.RemoveMsg(19)
	checkDeleteBlocks(DeleteBlocks{
		&DeleteRange{First: 3, Num: 1},
		&DeleteRange{First: 5, Num: 7},
		&DeleteRange{First: 16, Num: 5},
	})
}

func makeSequenceSet(seqs []uint64) *avl.SequenceSet {
	var set avl.SequenceSet
	for _, seq := range seqs {
		set.Insert(seq)
	}
	return &set
}

func TestPruneDeleteBlock(t *testing.T) {
	dbs := DeleteBlocks{
		&DeleteRange{First: 5, Num: 1},
		makeSequenceSet([]uint64{8}),
		&DeleteRange{First: 9, Num: 2},
		&DeleteRange{First: 47, Num: 92},
		&DeleteRange{First: 139, Num: 22},
		&DeleteRange{First: 200, Num: 21},
	}
	local := DeleteBlocks{
		makeSequenceSet([]uint64{8}),
		&DeleteRange{First: 9, Num: 2},
		&DeleteRange{First: 139, Num: 22},
		&DeleteRange{First: 170, Num: 6},
		&DeleteRange{First: 200, Num: 11},
	}

	// New earlier block should not be pruned.
	prune, local := pruneDeleteBlock(dbs[0], local)
	require_False(t, prune)
	require_Equal(t, len(local), 5)

	// Exact SequenceSet match should be pruned.
	prune, local = pruneDeleteBlock(dbs[1], local)
	require_True(t, prune)
	require_Equal(t, len(local), 4)

	// Exact DeleteRange match should be pruned.
	prune, local = pruneDeleteBlock(dbs[2], local)
	require_True(t, prune)
	require_Equal(t, len(local), 3)

	// Overlap alone is not enough to prune.
	prune, local = pruneDeleteBlock(dbs[3], local)
	require_False(t, prune)
	require_Equal(t, len(local), 3)

	// A later exact match should still be pruned after misalignment.
	prune, local = pruneDeleteBlock(dbs[4], local)
	require_True(t, prune)
	require_Equal(t, len(local), 2)

	// The scan should skip earlier local blocks that cannot match.
	prune, local = pruneDeleteBlock(dbs[5], local)
	require_False(t, prune)
	require_Equal(t, len(local), 1)

	// If all remaining local blocks are behind, the scan should exhaust them.
	prune, local = pruneDeleteBlock(&DeleteRange{First: 300, Num: 1}, local)
	require_False(t, prune)
	require_Equal(t, len(local), 0)

	// An empty local slice should be handled without pruning.
	prune, local = pruneDeleteBlock(&DeleteRange{First: 301, Num: 1}, local)
	require_False(t, prune)
	require_Equal(t, len(local), 0)

	// Sparse sequence sets with the same state but different contents
	// should not be pruned.
	local = DeleteBlocks{makeSequenceSet([]uint64{1, 4, 5})}
	prune, local = pruneDeleteBlock(makeSequenceSet([]uint64{1, 3, 5}), local)
	require_False(t, prune)
	require_Equal(t, len(local), 1)

	// Sparse sequence sets with identical contents should be pruned.
	prune, local = pruneDeleteBlock(makeSequenceSet([]uint64{1, 4, 5}), local)
	require_True(t, prune)
	require_Equal(t, len(local), 0)

	// A DeleteRange matching a dense sequence set should be pruned.
	local = DeleteBlocks{makeSequenceSet([]uint64{7, 8, 9})}
	prune, local = pruneDeleteBlock(&DeleteRange{First: 7, Num: 3}, local)
	require_True(t, prune)
	require_Equal(t, len(local), 0)

	// A dense sequence set matching a DeleteRange should be pruned.
	local = DeleteBlocks{&DeleteRange{First: 7, Num: 3}}
	prune, local = pruneDeleteBlock(makeSequenceSet([]uint64{7, 8, 9}), local)
	require_True(t, prune)
	require_Equal(t, len(local), 0)
}

func TestFileStoreDeleteBlocks(t *testing.T) {
	fcfg := FileStoreConfig{
		Cipher:      NoCipher,
		Compression: NoCompression,
		StoreDir:    t.TempDir(),
		BlockSize:   256,
	}
	fs, err := newFileStoreWithCreated(fcfg, StreamConfig{Name: "zzz", Storage: FileStorage}, time.Now(), prf(&fcfg), nil)
	require_NoError(t, err)
	defer fs.Stop()

	checkDeleteBlocks := func(exp DeleteBlocks) {
		dBlocks, _ := fs.deleteBlocks()
		require_Equal(t, len(exp), len(dBlocks))

		for i, found := range dBlocks {
			ef, el, en := exp[i].State()
			ff, fl, fn := found.State()

			require_Equal(t, reflect.TypeOf(exp[i]), reflect.TypeOf(found))
			require_Equal(t, ef, ff)
			require_Equal(t, el, fl)
			require_Equal(t, en, fn)
		}
	}

	msg := make([]byte, 20)
	for range 20 {
		_, _, err = fs.StoreMsg("foo", nil, msg, 0)
		require_NoError(t, err)
	}

	// We have 5 blocks, with 4 messages each.
	// block 1: [1,4]
	// block 2: [5,8]
	// block 3: [9,12]
	// block 4: [13,16]
	// block 5: [17,20]
	require_Equal(t, len(fs.blks), 5)

	// Removing sequence 9, creates a gap between blocks 2 and 3
	// block 1: [1,4]
	// block 2: [5,8]
	// block 3: [10,12]
	// block 4: [13,16]
	// block 5: [17,20]
	fs.RemoveMsg(9)
	checkDeleteBlocks(DeleteBlocks{
		&DeleteRange{First: 9, Num: 1},
	})

	// Extend the gap by removing 10
	// block 1: [1,4]
	// block 2: [5,8]
	// block 3: [11,12]
	// block 4: [13,16]
	// block 5: [17,20]
	fs.RemoveMsg(10)
	checkDeleteBlocks(DeleteBlocks{
		&DeleteRange{First: 9, Num: 2},
	})

	// Extend the gap by removing 8, creates an interior delete
	// block 1: [1,4]
	// block 2: [5,7]
	// block 3: [11,12]
	// block 4: [13,16]
	// block 5: [17,20]
	fs.RemoveMsg(8)
	checkDeleteBlocks(DeleteBlocks{
		makeSequenceSet([]uint64{8}),
		&DeleteRange{First: 9, Num: 2},
	})

	// Make another gap by removing 17 and 18
	// block 1: [1,4]
	// block 2: [5,7]
	// block 3: [11,12]
	// block 4: [13,16]
	// block 5: [19,20]
	fs.RemoveMsg(18)
	fs.RemoveMsg(17)
	checkDeleteBlocks(DeleteBlocks{
		makeSequenceSet([]uint64{8}),
		&DeleteRange{First: 9, Num: 2},
		&DeleteRange{First: 17, Num: 2},
	})

	// Remove messages 5, 6, and 7 to create a fully deleted block
	// block 1: [1,4]
	// block 2: [11,12]
	// block 3: [13,16]
	// block 4: [19,20]
	fs.RemoveMsg(6)
	fs.RemoveMsg(5)
	fs.RemoveMsg(7)
	checkDeleteBlocks(DeleteBlocks{
		&DeleteRange{First: 5, Num: 6},
		&DeleteRange{First: 17, Num: 2},
	})

	// Remove the last message
	// block 1: [1,4]
	// block 2: [11,12]
	// block 3: [13,16]
	// block 4: [19,19]
	fs.RemoveMsg(20)
	checkDeleteBlocks(DeleteBlocks{
		&DeleteRange{First: 5, Num: 6},
		&DeleteRange{First: 17, Num: 2},
		makeSequenceSet([]uint64{20}),
	})

	// Remove the last message
	// block 1: [1,4]
	// block 2: [11,12]
	// block 3: [13,16]
	fs.RemoveMsg(19)
	checkDeleteBlocks(DeleteBlocks{
		&DeleteRange{First: 5, Num: 6},
		&DeleteRange{First: 17, Num: 4},
	})

	// Remove block in the middle
	// block 1: [1,4]
	// block 2: [13,16]
	fs.RemoveMsg(11)
	fs.RemoveMsg(12)
	checkDeleteBlocks(DeleteBlocks{
		&DeleteRange{First: 5, Num: 8},
		&DeleteRange{First: 17, Num: 4},
	})

	// Remove the first block
	// block 1: [13,16]
	fs.RemoveMsg(1)
	fs.RemoveMsg(2)
	fs.RemoveMsg(3)
	fs.RemoveMsg(4)
	checkDeleteBlocks(DeleteBlocks{
		&DeleteRange{First: 17, Num: 4},
	})
}

func TestFileStoreDeleteBlocksWithManyEmptyBlocks(t *testing.T) {
	fcfg := FileStoreConfig{
		Cipher:      NoCipher,
		Compression: NoCompression,
		StoreDir:    t.TempDir(),
		BlockSize:   33 * 6,
	}
	fs, err := newFileStoreWithCreated(fcfg, StreamConfig{Name: "zzz", Storage: FileStorage}, time.Now(), prf(&fcfg), nil)
	require_NoError(t, err)
	defer fs.Stop()

	checkDeleteBlocks := func(exp DeleteBlocks) {
		t.Helper()
		dBlocks, _ := fs.deleteBlocks()
		require_Equal(t, len(exp), len(dBlocks))

		for i, found := range dBlocks {
			ef, el, en := exp[i].State()
			ff, fl, fn := found.State()

			require_Equal(t, reflect.TypeOf(exp[i]), reflect.TypeOf(found))
			require_Equal(t, ef, ff)
			require_Equal(t, el, fl)
			require_Equal(t, en, fn)
		}
	}

	// 1.blk, 6 msgs
	for range 6 {
		_, _, err = fs.StoreMsg("foo", nil, nil, 0)
		require_NoError(t, err)
	}
	// *.blk, tombstone for 1.blk + 2 msgs
	// every next block adds 2 tombstones for the previous block
	for i := range 5 {
		_, err = fs.newMsgBlockForWrite()
		require_NoError(t, err)

		// Every iteration will remove one message from 1.blk
		_, err = fs.RemoveMsg(uint64(i + 2))
		require_NoError(t, err)

		// Every block after the first removes two messages from the previous block.
		if i > 0 {
			for j := range 2 {
				_, err = fs.RemoveMsg(uint64(7 + (i-1)*2 + j))
				require_NoError(t, err)
			}
		}
		// Every block except for the last stores two new messages for the next block to delete.
		if i < 4 {
			for range 2 {
				_, _, err = fs.StoreMsg("foo", nil, nil, 0)
				require_NoError(t, err)
			}
		}
	}

	checkDeleteBlocks(DeleteBlocks{
		makeSequenceSet([]uint64{2, 3, 4, 5, 6}),
		&DeleteRange{First: 7, Num: 8},
	})

	fs.mu.Lock()
	fs.lockAllMsgBlocks()
	for _, mb := range fs.blks {
		mb.compact()
	}
	fs.unlockAllMsgBlocks()
	fs.mu.Unlock()

	checkDeleteBlocks(DeleteBlocks{
		&DeleteRange{First: 2, Num: 13},
	})
}

func TestFileStoreTrailingSkipMsgsFromStreamStateFile(t *testing.T) {
	testFileStoreAllPermutations(t, func(t *testing.T, fcfg FileStoreConfig) {
		cfg := StreamConfig{Name: "zzz", Storage: FileStorage, Subjects: []string{">"}}
		created := time.Now()
		fs, err := newFileStoreWithCreated(fcfg, cfg, created, prf(&fcfg), nil)
		require_NoError(t, err)
		defer fs.Stop()

		for range 10 {
			_, _, err = fs.StoreMsg("foo", nil, nil, 0)
			require_NoError(t, err)
		}

		fs.mu.RLock()
		lmb := fs.lmb
		fs.mu.RUnlock()
		lmb.mu.RLock()
		mfn := lmb.mfn
		lmb.mu.RUnlock()
		require_NotEqual(t, mfn, _EMPTY_)

		// Stop the store and truncate the block file.
		// Stopping should write out our stream state file, letting us "remember" the highest last sequence.
		require_NoError(t, fs.Stop())
		require_NoError(t, os.Truncate(mfn, 33*5))

		fs, err = newFileStoreWithCreated(fcfg, cfg, created, prf(&fcfg), nil)
		require_NoError(t, err)
		defer fs.Stop()

		// We should recover with the right state, the last sequence coming from the stream state file,
		// and the messages from the recovered blocks.
		state := fs.State()
		require_Equal(t, state.Msgs, 5)
		require_Equal(t, state.FirstSeq, 1)
		require_Equal(t, state.LastSeq, 10)

		// Go through all messages, they should properly report deletes.
		for seq := uint64(1); seq <= 10; seq++ {
			_, err = fs.LoadMsg(seq, nil)
			if seq <= 5 {
				require_NoError(t, err)
			} else {
				require_Error(t, err, ErrStoreMsgNotFound)
			}
		}

		// Also check a new block was created to represent this.
		fs.mu.RLock()
		lmb = fs.lmb
		lblks := len(fs.blks)
		fs.mu.RUnlock()
		lmb.mu.RLock()
		ldmap := lmb.dmap.Size()
		lmb.mu.RUnlock()
		require_Len(t, lblks, 2)
		require_Len(t, ldmap, 0)
		require_Equal(t, atomic.LoadUint64(&lmb.first.seq), 11)
		require_Equal(t, atomic.LoadUint64(&lmb.last.seq), 10)
	})
}

func TestFileStoreSelectMsgBlockBinarySearch(t *testing.T) {
	testFileStoreAllPermutations(t, func(t *testing.T, fcfg FileStoreConfig) {
		created := time.Now()
		cfg := StreamConfig{Name: "zzz", Storage: FileStorage, Subjects: []string{">"}}
		fs, err := newFileStoreWithCreated(fcfg, cfg, created, prf(&fcfg), nil)
		require_NoError(t, err)
		defer fs.Stop()

		for i := range 33 {
			if i > 0 {
				_, err = fs.newMsgBlockForWrite()
				require_NoError(t, err)
			}
			for range 2 {
				_, _, err = fs.StoreMsg("foo", nil, nil, 0)
				require_NoError(t, err)
			}
			if i == 15 {
				for _, seq := range []uint64{2, 5} {
					_, err = fs.newMsgBlockForWrite()
					require_NoError(t, err)
					_, err = fs.RemoveMsg(seq)
					require_NoError(t, err)
				}
			}
		}

		test := func() {
			// Select a block containing a message we deleted.
			i, mb := fs.selectMsgBlockWithIndex(2)
			require_Equal(t, i, 0)
			require_Equal(t, mb.index, 1)

			// Once more for the other deleted message.
			i, mb = fs.selectMsgBlockWithIndex(5)
			require_Equal(t, i, 2)
			require_Equal(t, mb.index, 3)

			// Select the first block/message.
			i, mb = fs.selectMsgBlockWithIndex(1)
			require_Equal(t, i, 0)
			require_Equal(t, mb.index, 1)

			// Select a block before the delete block, but after the deleted sequences.
			i, mb = fs.selectMsgBlockWithIndex(10)
			require_Equal(t, i, 4)
			require_Equal(t, mb.index, 5)

			// Select a block after the delete block AND after the deleted sequences.
			i, mb = fs.selectMsgBlockWithIndex(64)
			require_Equal(t, i, 33)
			require_Equal(t, mb.index, 34)
		}
		test()

		require_NoError(t, fs.Stop())
		require_NoError(t, os.Remove(filepath.Join(fs.fcfg.StoreDir, msgDir, streamStreamStateFile)))

		fs, err = newFileStoreWithCreated(fcfg, cfg, created, prf(&fcfg), nil)
		require_NoError(t, err)
		defer fs.Stop()
		test()
	})
}

func TestFileStoreRemoveMsgsInRange(t *testing.T) {
	fcfg := FileStoreConfig{Cipher: NoCipher, Compression: NoCompression, StoreDir: t.TempDir(), BlockSize: 256}
	fs, err := newFileStoreWithCreated(fcfg, StreamConfig{Name: "zzz", Storage: FileStorage}, time.Now(), prf(&fcfg), nil)
	require_NoError(t, err)
	defer fs.Stop()

	msg := make([]byte, 256)
	for range 20 {
		_, _, err = fs.StoreMsg("foo", nil, msg, 0)
		require_NoError(t, err)
	}

	fs.mu.Lock()
	defer fs.mu.Unlock()

	checkDeleteBlocks := func(exp DeleteBlocks) {
		dBlocks, _ := fs.deleteBlocks()
		require_Equal(t, len(exp), len(dBlocks))

		for i, found := range dBlocks {
			ef, el, en := exp[i].State()
			ff, fl, fn := found.State()

			require_Equal(t, reflect.TypeOf(exp[i]), reflect.TypeOf(found))
			require_Equal(t, ef, ff)
			require_Equal(t, el, fl)
			require_Equal(t, en, fn)
		}
	}

	require_Equal(t, len(fs.blks), 20)

	// Remove range [1,1]
	fs.removeMsgsInRange(1, 1, true, nil)
	require_Equal(t, len(fs.blks), 19)
	require_Equal(t, atomic.LoadUint64(&fs.blks[0].first.seq), 2)

	// Removing range [1,1] again is a noop
	// We are left with blocks [2,20]
	fs.removeMsgsInRange(1, 1, true, nil)
	require_Equal(t, len(fs.blks), 19)
	require_Equal(t, atomic.LoadUint64(&fs.blks[0].first.seq), 2)

	// Remove the range [1,2] should remove block with sequence 2
	// We are left with blocks [3,20]
	fs.removeMsgsInRange(1, 2, true, nil)
	require_Equal(t, len(fs.blks), 18)
	require_Equal(t, atomic.LoadUint64(&fs.blks[0].first.seq), 3)

	// Remove the first two blocks [3,4]
	// We are left with blocks [5,20]
	fs.removeMsgsInRange(3, 4, true, nil)
	require_Equal(t, len(fs.blks), 16)
	require_Equal(t, atomic.LoadUint64(&fs.blks[0].first.seq), 5)

	// Remove range [9, 13]
	// We are left with [5,8] [14,20]
	fs.removeMsgsInRange(9, 13, true, nil)
	require_Equal(t, len(fs.blks), 11)
	checkDeleteBlocks(DeleteBlocks{
		&DeleteRange{First: 9, Num: 5},
	})

	// Make the gap larger by removing range [8 8]
	// We are left with [5,7] [14, 20]
	fs.removeMsgsInRange(8, 8, true, nil)
	require_Equal(t, len(fs.blks), 10)
	checkDeleteBlocks(DeleteBlocks{
		&DeleteRange{First: 8, Num: 6},
	})

	// Make another gap by removing range [17, 17]
	// We are left with [5,7] [14,16] [18,20]
	fs.removeMsgsInRange(17, 17, true, nil)
	require_Equal(t, len(fs.blks), 9)
	checkDeleteBlocks(DeleteBlocks{
		&DeleteRange{First: 8, Num: 6},
		&DeleteRange{First: 17, Num: 1},
	})

	// Remove the last block
	// We are left with [5,7] [14,16] [18,19] (empty block 21-20)
	fs.removeMsgsInRange(20, 20, true, nil)
	checkDeleteBlocks(DeleteBlocks{
		&DeleteRange{First: 8, Num: 6},
		&DeleteRange{First: 17, Num: 1},
		&DeleteRange{First: 20, Num: 1},
	})

	// Make a big gap removing range [7, 18]
	// We are left with [5,6] [19]
	fs.removeMsgsInRange(7, 18, true, nil)
	require_Equal(t, len(fs.blks), 4)
	checkDeleteBlocks(DeleteBlocks{
		&DeleteRange{First: 7, Num: 12},
		&DeleteRange{First: 20, Num: 1},
	})

	// Remove everything
	// We are left with an empty block
	fs.removeMsgsInRange(1, 20, true, nil)
	require_Equal(t, len(fs.blks), 1)
	require_Equal(t, fs.blks[0].msgs, 0)
}

func TestFileStoreRemoveMsgsInRangePartialBlocks(t *testing.T) {
	fcfg := FileStoreConfig{
		Cipher:      NoCipher,
		Compression: NoCompression,
		StoreDir:    t.TempDir(),
		BlockSize:   256,
	}
	fs, err := newFileStoreWithCreated(fcfg, StreamConfig{Name: "zzz", Storage: FileStorage}, time.Now(), prf(&fcfg), nil)
	require_NoError(t, err)
	defer fs.Stop()

	msg := make([]byte, 16)
	for range 20 {
		_, _, err = fs.StoreMsg("foo", nil, msg, 0)
		require_NoError(t, err)
	}

	fs.mu.Lock()
	defer fs.mu.Unlock()

	// Initially:
	// block 0 [ 1, 5]
	// block 1 [ 6,10]
	// block 2 [11,15]
	// block 3 [16,20]
	require_Equal(t, len(fs.blks), 4)

	// Remove [5,8] leaves:
	// block 0 [ 1, 4]
	// block 1 [ 9,10]
	// block 2 [11,15]
	// block 3 [16,20]
	fs.removeMsgsInRange(5, 6, true, nil)
	fs.blks[0].compact()

	require_Equal(t, len(fs.blks), 4)
	require_Equal(t, atomic.LoadUint64(&fs.blks[0].first.seq), 1)
	require_Equal(t, atomic.LoadUint64(&fs.blks[0].last.seq), 4)
	require_Equal(t, atomic.LoadUint64(&fs.blks[1].first.seq), 7)
	require_Equal(t, atomic.LoadUint64(&fs.blks[1].last.seq), 10)

	// Remove [5,8] leaves:
	// block 0 [ 1, 3]
	// block 1 [12,15]
	// block 2 [16,20]
	// empty block 21 20
	fs.removeMsgsInRange(4, 11, true, nil)
	fs.blks[0].compact()

	require_Equal(t, len(fs.blks), 3)
	require_Equal(t, atomic.LoadUint64(&fs.blks[0].first.seq), 1)
	require_Equal(t, atomic.LoadUint64(&fs.blks[0].last.seq), 3)
	require_Equal(t, atomic.LoadUint64(&fs.blks[1].first.seq), 12)
	require_Equal(t, atomic.LoadUint64(&fs.blks[1].last.seq), 15)

	// Remove [13,14] leaves:
	// block 0 [ 1, 3]
	// block 1 12 and 15
	// block 2 [16,20]
	fs.removeMsgsInRange(13, 14, true, nil)
	fs.blks[1].compact()

	require_Equal(t, len(fs.blks), 3)
	require_Equal(t, atomic.LoadUint64(&fs.blks[0].first.seq), 1)
	require_Equal(t, atomic.LoadUint64(&fs.blks[0].last.seq), 3)
	require_Equal(t, fs.blks[1].msgs, 2)
	require_Equal(t, atomic.LoadUint64(&fs.blks[1].first.seq), 12)
	require_Equal(t, atomic.LoadUint64(&fs.blks[1].last.seq), 15)

	// Remove [13,20] leaves:
	// block 0 [ 1, 3]
	// block 1 12
	// empty block 21 20
	fs.removeMsgsInRange(13, 20, true, nil)
	fs.blks[1].compact()

	require_Equal(t, len(fs.blks), 3)
	require_Equal(t, atomic.LoadUint64(&fs.blks[0].first.seq), 1)
	require_Equal(t, atomic.LoadUint64(&fs.blks[0].last.seq), 3)
	require_Equal(t, fs.blks[1].msgs, 1)
	require_Equal(t, atomic.LoadUint64(&fs.blks[1].first.seq), 12)
	require_Equal(t, atomic.LoadUint64(&fs.blks[1].last.seq), 12)

	// Remove [10,20] leaves:
	// block 0 [ 1, 3]
	// empty block 21 20
	fs.removeMsgsInRange(10, 20, true, nil)
	require_Equal(t, len(fs.blks), 2)

	// Remove everything
	// empty block 21 20
	fs.removeMsgsInRange(1, 30, true, nil)

	require_Equal(t, len(fs.blks), 1)
	require_Equal(t, atomic.LoadUint64(&fs.blks[0].first.seq), 21)
	require_Equal(t, atomic.LoadUint64(&fs.blks[0].last.seq), 20)
}

func TestFileStoreRemoveMsgsInRangeWithTombstones(t *testing.T) {
	fcfg := FileStoreConfig{
		Cipher:      NoCipher,
		Compression: NoCompression,
		StoreDir:    t.TempDir(),
		BlockSize:   256,
	}
	fs, err := newFileStoreWithCreated(fcfg, StreamConfig{Name: "zzz", Storage: FileStorage}, time.Now(), prf(&fcfg), nil)
	require_NoError(t, err)
	defer fs.Stop()

	// Block 1: [1,4]
	for range 4 {
		_, _, err = fs.StoreMsg("foo", nil, nil, 0)
		require_NoError(t, err)
	}

	// Block 2: [5,8]
	_, err = fs.newMsgBlockForWrite()
	require_NoError(t, err)
	for range 4 {
		_, _, err = fs.StoreMsg("foo", nil, nil, 0)
		require_NoError(t, err)
	}

	// Block 3 [9,12] - tombs {4,3,2,10}
	_, err = fs.newMsgBlockForWrite()
	require_NoError(t, err)
	for range 4 {
		_, _, err = fs.StoreMsg("foo", nil, nil, 0)
		require_NoError(t, err)
	}

	fs.RemoveMsg(4)
	fs.RemoveMsg(3)
	fs.RemoveMsg(2)
	fs.RemoveMsg(10)

	// Block 4 [13, 16] - tombs {14,15}
	_, err = fs.newMsgBlockForWrite()
	require_NoError(t, err)
	for range 4 {
		_, _, err = fs.StoreMsg("foo", nil, nil, 0)
		require_NoError(t, err)
	}
	fs.RemoveMsg(14)
	fs.RemoveMsg(15)

	// Block 5 [17,17]
	_, err = fs.newMsgBlockForWrite()
	require_NoError(t, err)
	for range 4 {
		_, _, err = fs.StoreMsg("foo", nil, nil, 0)
		require_NoError(t, err)
	}

	checkBlock := func(mb *msgBlock, fseq, lseq uint64, tombs []uint64) {
		t.Helper()
		require_Equal(t, mb.first.seq, fseq)
		require_Equal(t, mb.last.seq, lseq)
		mbTombs := make([]uint64, len(mb.tombs()))
		for i, id := range mb.tombs() {
			mbTombs[i] = id.seq
		}
		require_True(t, slices.Equal(tombs, mbTombs))
	}

	require_Equal(t, len(fs.blks), 5)
	checkBlock(fs.blks[0], 1, 4, nil)
	checkBlock(fs.blks[1], 5, 8, nil)
	checkBlock(fs.blks[2], 9, 12, []uint64{4, 3, 2, 10})
	checkBlock(fs.blks[3], 13, 16, []uint64{14, 15})
	checkBlock(fs.blks[4], 17, 20, nil)

	// for i, mb := range fs.blks {
	// 	mbFirstSeq := atomic.LoadUint64(&mb.first.seq)
	// 	mbLastSeq := atomic.LoadUint64(&mb.last.seq)
	// 	t.Log(i, mb.msgs, mbFirstSeq, mbLastSeq, mb.tombs())
	// }

	// Remove sequences 4-17
	// Block 1: removing the tail, noop because already deleted
	// Block 2: has no prior tombs, should be purged entirely
	// Block 3: has prior tombs, should be preserved
	// Block 4: has "internal" tombs, can be purged entirely
	// Block 5: updates mb first
	fs.removeMsgsInRange(4, 17, false, nil)

	checkBlock(fs.blks[0], 1, 4, nil)
	checkBlock(fs.blks[1], 13, 12, []uint64{4, 3, 2, 10})
	checkBlock(fs.blks[2], 18, 20, []uint64{9, 11, 17})

	// Remove everything (past the last sequence)
	fs.removeMsgsInRange(1, 100, false, nil)
	require_Equal(t, len(fs.blks), 1)
	checkBlock(fs.blks[0], 21, 20, []uint64{20})

	// Attempt to remove the empty block
	fs.removeMsgsInRange(1, 100, false, nil)
	require_Equal(t, len(fs.blks), 1)
	checkBlock(fs.blks[0], 21, 20, []uint64{20})
}

func TestFileStoreCorrectChecksumAfterTruncate(t *testing.T) {
	testFileStoreAllPermutations(t, func(t *testing.T, fcfg FileStoreConfig) {
		fs, err := newFileStoreWithCreated(fcfg, StreamConfig{Name: "zzz", Storage: FileStorage}, time.Now(), prf(&fcfg), nil)
		require_NoError(t, err)
		defer fs.Stop()

		var fchkCmp []byte
		var fchk [8]byte
		for i := range 5 {
			_, _, err = fs.StoreMsg("foo", nil, nil, 0)
			require_NoError(t, err)
			if i == 2 {
				mb := fs.getFirstBlock()
				mb.mu.RLock()
				fchkCmp = mb.lastChecksum()
				copy(fchk[:], mb.lchk[:])
				mb.mu.RUnlock()
			}
		}
		require_True(t, bytes.Equal(fchkCmp, fchk[:]))

		// After truncating we should return to the checksum we stored above.
		require_NoError(t, fs.Truncate(3))
		mb := fs.getFirstBlock()
		mb.mu.RLock()
		lchkCmp := mb.lastChecksum()
		var lchk [8]byte
		copy(lchk[:], mb.lchk[:])
		mb.mu.RUnlock()
		require_True(t, bytes.Equal(lchkCmp, lchk[:]))
		require_True(t, bytes.Equal(fchkCmp, lchkCmp))
	})
}

func TestFileStoreRecoverTTLAndScheduleStateAndCounters(t *testing.T) {
	testFileStoreAllPermutations(t, func(t *testing.T, fcfg FileStoreConfig) {
		created := time.Now()
		cfg := StreamConfig{Name: "zzz", Storage: FileStorage, Subjects: []string{">"}, AllowMsgTTL: true, AllowMsgSchedules: true}
		fs, err := newFileStoreWithCreated(fcfg, cfg, created, prf(&fcfg), nil)
		require_NoError(t, err)
		defer fs.Stop()

		ttl := int64(5)
		sched := time.Now().Add(5 * time.Second)
		hdr := genHeader(nil, JSMessageTTL, strconv.FormatInt(ttl, 10))
		hdr = genHeader(hdr, JSSchedulePattern, fmt.Sprintf("@at %s", sched.Format(time.RFC3339)))
		_, _, err = fs.StoreMsg("foo", hdr, nil, ttl)
		require_NoError(t, err)

		mb := fs.getFirstBlock()
		mb.mu.RLock()
		ttls := mb.ttls
		schedules := mb.schedules
		mb.mu.RUnlock()
		require_Equal(t, ttls, 1)
		require_Equal(t, schedules, 1)

		require_NoError(t, fs.Stop())
		require_NoError(t, os.Remove(filepath.Join(fs.fcfg.StoreDir, msgDir, streamStreamStateFile)))

		fs, err = newFileStoreWithCreated(fcfg, cfg, created, prf(&fcfg), nil)
		require_NoError(t, err)
		defer fs.Stop()

		mb = fs.getFirstBlock()
		mb.mu.RLock()
		ttls = mb.ttls
		schedules = mb.schedules
		mb.mu.RUnlock()
		require_Equal(t, ttls, 1)
		require_Equal(t, schedules, 1)
	})
}

func TestFileStoreCorruptionSetsHbitWithoutHeaders(t *testing.T) {
	const (
		KindMsgFromBuf = iota
		KindIndexCacheBuf
		KindRebuildState
	)
	test := func(t *testing.T, kind int) {
		testFileStoreAllPermutations(t, func(t *testing.T, fcfg FileStoreConfig) {
			fs, err := newFileStoreWithCreated(fcfg, StreamConfig{Name: "zzz", Storage: FileStorage, Subjects: []string{">"}}, time.Now(), prf(&fcfg), nil)
			require_NoError(t, err)
			defer fs.Stop()

			mb := fs.getFirstBlock()
			fs.mu.Lock()
			mb.mu.Lock()
			err = mb.writeMsgRecordLocked(emptyRecordLen|hbit, 1, _EMPTY_, nil, nil, 0, false, false)
			cache := mb.ecache.Value()
			mb.mu.Unlock()
			fs.mu.Unlock()
			require_NoError(t, err)
			require_NotNil(t, cache)

			switch kind {
			case KindMsgFromBuf:
				mb.mu.Lock()
				_, err = mb.msgFromBufNoCopy(cache.buf, nil, mb.hh)
				mb.mu.Unlock()
				require_True(t, strings.Contains(err.Error(), "sanity check failed"))
			case KindIndexCacheBuf:
				mb.mu.Lock()
				err = mb.indexCacheBuf(cache.buf)
				mb.mu.Unlock()
				require_Error(t, err, errCorruptState)
			case KindRebuildState:
				mb.mu.Lock()
				_, _, err = mb.rebuildStateFromBufLocked(cache.buf, false)
				mb.mu.Unlock()
				require_True(t, err != nil)
				require_True(t, strings.Contains(err.Error(), "sanity check failed"))
			default:
				t.Fatalf("unknown kind %d", kind)
			}
		})
	}
	t.Run("msgFromBuf", func(t *testing.T) { test(t, KindMsgFromBuf) })
	t.Run("indexCacheBuf", func(t *testing.T) { test(t, KindIndexCacheBuf) })
	t.Run("rebuildState", func(t *testing.T) { test(t, KindRebuildState) })
}

func TestFileStoreSyncBlocksNoErrorOnConcurrentRemovedBlock(t *testing.T) {
	fcfg := FileStoreConfig{Cipher: NoCipher, Compression: NoCompression, StoreDir: t.TempDir()}
	fs, err := newFileStoreWithCreated(fcfg, StreamConfig{Name: "zzz", Storage: FileStorage}, time.Now(), prf(&fcfg), nil)
	require_NoError(t, err)
	defer fs.Stop()

	fs.mu.Lock()
	if fs.syncTmr != nil {
		fs.syncTmr.Stop()
		fs.syncTmr = nil
	}
	fs.mu.Unlock()

	// 1.blk
	for range 2 {
		_, _, err = fs.StoreMsg("foo", nil, nil, 0)
		require_NoError(t, err)
	}

	// 2.blk
	_, err = fs.newMsgBlockForWrite()
	require_NoError(t, err)
	_, _, err = fs.StoreMsg("foo", nil, nil, 0)
	require_NoError(t, err)
	_, err = fs.RemoveMsg(2)
	require_NoError(t, err)

	// 3.blk
	_, err = fs.newMsgBlockForWrite()
	require_NoError(t, err)
	_, _, err = fs.StoreMsg("foo", nil, nil, 0)
	require_NoError(t, err)
	_, err = fs.RemoveMsg(3)
	require_NoError(t, err)

	fs.mu.RLock()
	mb := fs.blks[1]
	fs.mu.RUnlock()
	require_Equal(t, mb.index, 2)
	mb.mu.RLock()
	shouldCompactSync := mb.shouldCompactSync()
	mb.mu.RUnlock()
	require_True(t, shouldCompactSync)

	var ready sync.WaitGroup
	var wg sync.WaitGroup
	ready.Add(1)
	wg.Add(1)
	mb.mu.Lock()
	go func() {
		ready.Done()
		defer wg.Done()
		// Wait some time for fs.syncBlocks to wait on the mb's lock.
		time.Sleep(200 * time.Millisecond)
		// Remove the block while fs.syncBlocks is still waiting.
		fs.mu.Lock()
		err = fs.forceRemoveMsgBlock(mb)
		fs.mu.Unlock()
		mb.mu.Unlock()
		require_NoError(t, err)
	}()

	ready.Wait()
	fs.syncBlocks()
	wg.Wait()

	fs.mu.RLock()
	defer fs.mu.RUnlock()
	mb.mu.RLock()
	defer mb.mu.RUnlock()
	require_True(t, mb.werr == nil)
	require_True(t, fs.werr == nil)
}

func TestFileStoreDontExpireCacheWithRecentWrite(t *testing.T) {
	fcfg := FileStoreConfig{StoreDir: t.TempDir()}
	cfg := StreamConfig{Name: "zzz", Storage: FileStorage, Subjects: []string{">"}}
	fs, err := newFileStore(fcfg, cfg)
	require_NoError(t, err)
	defer fs.Stop()

	_, _, err = fs.StoreMsg("foo", nil, nil, 0)
	require_NoError(t, err)

	// Cache should still exist after above write.
	fmb := fs.getFirstBlock()
	fmb.mu.Lock()
	cacheLoaded := fmb.cacheAlreadyLoaded()
	fmb.llseq = 0
	fmb.mu.Unlock()
	require_True(t, cacheLoaded)

	// The load will try to expire the cache, but shouldn't due to the recent write.
	_, _, err = fs.LoadNextMsg(fwcs, true, 0, nil)
	require_NoError(t, err)

	fmb.mu.Lock()
	fmb.llseq = 0
	cacheLoaded = fmb.cacheAlreadyLoaded()
	// We update the last write timestamp to be very old, so the next load can expire the cache.
	fmb.lwts = 0
	fmb.mu.Unlock()
	require_True(t, cacheLoaded)
	_, _, err = fs.LoadNextMsg(fwcs, true, 0, nil)
	require_NoError(t, err)

	fmb.mu.Lock()
	cacheLoaded = fmb.cacheAlreadyLoaded()
	fmb.mu.Unlock()
	require_False(t, cacheLoaded)
}

func TestFileStoreNoDirectoryNotEmptyError(t *testing.T) {
	fcfg := FileStoreConfig{StoreDir: t.TempDir()}
	mconfig := StreamConfig{Name: "TEST_DELETE", Storage: FileStorage, Subjects: []string{"foo"}, Replicas: 1}
	fs, err := newFileStore(fcfg, mconfig)
	require_NoError(t, err)
	defer fs.Stop()

	oconfig := ConsumerConfig{
		DeliverSubject: "d",
		FilterSubject:  "foo",
		AckPolicy:      AckExplicit,
	}

	for i := range 100 {
		oname := fmt.Sprintf("delcons_%d", i)
		obs, err := fs.ConsumerStore(oname, time.Time{}, &oconfig)
		require_NoError(t, err)

		wg := sync.WaitGroup{}
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; ; i++ {
				if err := obs.SetStarting(uint64(i + 1)); err != nil {
					return
				}
			}
		}()

		time.Sleep(time.Duration(rand.IntN(10)) * time.Millisecond)

		err = obs.Delete()
		require_NoError(t, err)
		wg.Wait()
	}
}

func TestFileStoreConsumerNoMetaWriteAfterClose(t *testing.T) {
	for _, op := range []string{"Stop", "Delete"} {
		t.Run(op, func(t *testing.T) {
			// Writes after close would end up in the working directory.
			cwd := t.TempDir()
			t.Chdir(cwd)

			fs, err := newFileStore(FileStoreConfig{StoreDir: t.TempDir()}, StreamConfig{Name: "TEST", Storage: FileStorage, Subjects: []string{"foo"}})
			require_NoError(t, err)
			defer fs.Stop()

			cfg := ConsumerConfig{Durable: "CONSUMER", AckPolicy: AckExplicit}
			obs, err := fs.ConsumerStore("CONSUMER", time.Time{}, &cfg)
			require_NoError(t, err)
			o := obs.(*consumerFileStore)

			if op == "Stop" {
				require_NoError(t, o.Stop())
			} else {
				require_NoError(t, o.Delete())
			}
			require_Error(t, o.UpdateConfig(&cfg), ErrStoreClosed)
			require_Error(t, o.updateConfig(cfg), ErrStoreClosed)

			entries, err := os.ReadDir(cwd)
			require_NoError(t, err)
			require_Len(t, len(entries), 0)
		})
	}
}

func TestFileStoreDontLoadSubjectStateIfNotPurged(t *testing.T) {
	fcfg := FileStoreConfig{StoreDir: t.TempDir()}
	cfg := StreamConfig{Name: "zzz", Storage: FileStorage, Subjects: []string{">"}}
	fs, err := newFileStore(fcfg, cfg)
	require_NoError(t, err)
	defer fs.Stop()

	_, _, err = fs.StoreMsg("foo", nil, nil, 0)
	require_NoError(t, err)

	fmb := fs.getFirstBlock()
	fmb.mu.Lock()
	fmb.fss = nil
	fmb.mu.Unlock()

	// When purging a subject that doesn't exist, we shouldn't need to load subjects for any blocks.
	purged, err := fs.PurgeEx("bar", 0, 0)
	require_NoError(t, err)
	require_Equal(t, purged, 0)

	fmb.mu.RLock()
	defer fmb.mu.RUnlock()
	require_True(t, fmb.fss == nil)
}

func TestFileStoreOnlyLoadSubjectStateOnBlocksWithinInterestRange(t *testing.T) {
	fcfg := FileStoreConfig{StoreDir: t.TempDir()}
	cfg := StreamConfig{Name: "zzz", Storage: FileStorage, Subjects: []string{">"}}
	fs, err := newFileStore(fcfg, cfg)
	require_NoError(t, err)
	defer fs.Stop()

	_, _, err = fs.StoreMsg("foo", nil, nil, 0)
	require_NoError(t, err)
	_, err = fs.newMsgBlockForWrite()
	require_NoError(t, err)
	_, _, err = fs.StoreMsg("bar", nil, nil, 0)
	require_NoError(t, err)

	fs.mu.RLock()
	for _, mb := range fs.blks {
		mb.mu.Lock()
		mb.fss = nil
		mb.mu.Unlock()
	}
	fs.mu.RUnlock()

	// When purging a subject that only exists on a subset, we should only load blocks within its range.
	purged, err := fs.PurgeEx("bar", 0, 0)
	require_NoError(t, err)
	require_Equal(t, purged, 1)

	fs.mu.RLock()
	defer fs.mu.RUnlock()
	for i, mb := range fs.blks {
		mb.mu.Lock()
		loadedFss := mb.fss != nil
		mb.mu.Unlock()
		require_Equal(t, loadedFss, i == 1)
	}
}

func TestFileStoreRemovePerSubjectWithMultipleBlocks(t *testing.T) {
	fcfg := FileStoreConfig{StoreDir: t.TempDir()}
	cfg := StreamConfig{Name: "zzz", Storage: FileStorage, Subjects: []string{">"}}
	fs, err := newFileStore(fcfg, cfg)
	require_NoError(t, err)
	defer fs.Stop()

	// Store two messages in separate blocks.
	_, _, err = fs.StoreMsg("foo", nil, nil, 0)
	require_NoError(t, err)
	_, err = fs.newMsgBlockForWrite()
	require_NoError(t, err)
	_, _, err = fs.StoreMsg("foo", nil, nil, 0)
	require_NoError(t, err)

	fs.mu.Lock()
	seq, err := fs.firstSeqForSubj("foo")
	fs.mu.Unlock()
	require_NoError(t, err)
	require_Equal(t, seq, 1)

	// After removing the last message, we should still be able to find the first.
	removed, err := fs.RemoveMsg(2)
	require_NoError(t, err)
	require_True(t, removed)

	fs.mu.Lock()
	seq, err = fs.firstSeqForSubj("foo")
	fs.mu.Unlock()
	require_NoError(t, err)
	require_Equal(t, seq, 1)
}

func TestFileStoreSetWriteErrIgnoresReadErrors(t *testing.T) {
	fcfg := FileStoreConfig{StoreDir: t.TempDir()}
	cfg := StreamConfig{Name: "zzz", Storage: FileStorage, Subjects: []string{">"}}
	fs, err := newFileStore(fcfg, cfg)
	require_NoError(t, err)
	defer fs.Stop()

	structural := []error{
		errNoCache,
		errDeletedMsg,
		errPartialCache,
		errCorruptState,
		errPriorState,
		errBadMsg{fn: "block.blk", detail: "invalid checksum"},
		// Short reads of on-disk blocks surface as io.ErrUnexpectedEOF from
		// io.ReadFull in loadBlock. Treat like other on-disk-data read errors:
		// log + rebuild, don't disable writes.
		io.ErrUnexpectedEOF,
		// A block whose size doesn't fit in an int is an on-disk-data issue too.
		errMsgBlkTooBig,
		// Wrapped variants should also be recognized.
		fmt.Errorf("wrapped: %w", io.ErrUnexpectedEOF),
		fmt.Errorf("wrapped: %w", errCorruptState),
	}

	fs.mu.Lock()
	defer fs.mu.Unlock()
	for _, e := range structural {
		fs.setWriteErr(e)
		require_True(t, fs.werr == nil)
	}

	// Sanity: a genuine write error still sticks.
	fs.setWriteErr(io.ErrShortWrite)
	require_Error(t, fs.werr, io.ErrShortWrite)
}

func TestFileStoreMsgBlockWErrNotSetOnReadErrors(t *testing.T) {
	fs, err := newFileStore(
		FileStoreConfig{StoreDir: t.TempDir()},
		StreamConfig{Name: "zzz", Storage: FileStorage, Subjects: []string{"foo"}},
	)
	require_NoError(t, err)
	defer fs.Stop()

	_, _, err = fs.StoreMsg("foo", nil, []byte("hello"), 0)
	require_NoError(t, err)

	fs.mu.Lock()
	mb := fs.lmb
	mb.mu.Lock()

	// Ensure cache is fully loaded so cacheNotLoaded() returns false.
	if err = mb.loadMsgsWithLock(); err != nil {
		mb.mu.Unlock()
		fs.mu.Unlock()
		require_NoError(t, err)
	}

	// Zero out the cache buffer (same length) to corrupt the message data.
	// cacheAlreadyLoaded() still returns true: fseq≠0, idx unchanged, buf non-empty.
	// When generatePerSubjectInfo iterates and calls cacheLookupNoCopy, slotInfo
	// reads the valid idx entry but msgFromBuf parsing of the zeroed bytes returns errBadMsg.
	mb.cache.buf = make([]byte, len(mb.cache.buf))

	// Clear fss so ensurePerSubjectInfoLoaded calls generatePerSubjectInfo.
	mb.fss = nil
	mb.mu.Unlock()
	fs.mu.Unlock()

	// This write will return errBadMsg (a read error). Before the fix, the
	// deferred func would have set mb.werr and fs.werr, permanently blocking further writes.
	_, _, err = fs.StoreMsg("foo", nil, []byte("world"), 0)
	require_True(t, isReadErr(err))

	checkWerr := func(expected error) {
		fs.mu.RLock()
		defer fs.mu.RUnlock()
		mb.mu.RLock()
		defer mb.mu.RUnlock()
		require_Equal(t, mb.werr, expected)
		require_Equal(t, fs.werr, expected)
	}
	checkWerr(nil)

	// Store still accepts new writes after the transient read error.
	_, _, err = fs.StoreMsg("foo", nil, []byte("again"), 0)
	require_NoError(t, err)
	checkWerr(nil)

	// A genuine write error does still bubble up fully.
	mb.mu.Lock()
	mb.werr = io.ErrShortWrite
	mb.mu.Unlock()
	_, _, err = fs.StoreMsg("foo", nil, []byte("again"), 0)
	require_Error(t, err, io.ErrShortWrite)
	checkWerr(io.ErrShortWrite)
}

func TestFileStoreGeneratePerSubjectInfoFssNilOnError(t *testing.T) {
	fs, err := newFileStore(
		FileStoreConfig{StoreDir: t.TempDir()},
		StreamConfig{Name: "zzz", Storage: FileStorage, Subjects: []string{"foo"}},
	)
	require_NoError(t, err)
	defer fs.Stop()

	_, _, err = fs.StoreMsg("foo", nil, []byte("hello"), 0)
	require_NoError(t, err)

	fs.mu.Lock()
	defer fs.mu.Unlock()
	mb := fs.lmb
	mb.mu.Lock()
	defer mb.mu.Unlock()

	// Load cache so cacheNotLoaded() returns false inside generatePerSubjectInfo.
	require_NoError(t, mb.loadMsgsWithLock())

	// Corrupt the cache buffer so cacheLookupNoCopy returns errBadMsg.
	mb.cache.buf = make([]byte, len(mb.cache.buf))

	// Clear fss to nil so generatePerSubjectInfo tries to rebuild.
	mb.fss = nil

	// generatePerSubjectInfo must fail due to the corrupt cache.
	require_Error(t, mb.generatePerSubjectInfo())

	// After failure, fss must be nil, so that mb.ensurePerSubjectInfoLoaded
	// retries the rebuild on the next call instead of silently returning an
	// incomplete fss.
	require_True(t, mb.fss == nil)

	// Cache must also be cleared so the next retry reloads from disk instead
	// of hitting the same corrupt in-memory data indefinitely.
	require_True(t, mb.cache == nil)
}

func TestFileStoreSchedulingRecoveryAliasCorruption(t *testing.T) {
	dir := t.TempDir()

	// Small block size + chunky per-message padding so multiple blocks
	// roll over during the recovery scan, forcing cache eviction.
	const blockSize = 4 * 1024
	cfg := StreamConfig{
		Name:              "SCHED",
		Subjects:          []string{"sched.>"},
		Storage:           FileStorage,
		AllowMsgSchedules: true,
	}
	fcfg := FileStoreConfig{StoreDir: dir, BlockSize: blockSize}

	const N = 40
	subjects := make([]string, N)
	for i := range subjects {
		subjects[i] = fmt.Sprintf("sched.AAAA-%05d-%s", i, strings.Repeat("A", 40))
	}

	fs, err := newFileStore(fcfg, cfg)
	require_NoError(t, err)
	future := time.Now().Add(24 * time.Hour).Format(time.RFC3339Nano)
	for i, subj := range subjects {
		hdr := genHeader(nil, JSSchedulePattern, fmt.Sprintf("@at %s", future))
		hdr = genHeader(hdr, JSScheduleTarget, fmt.Sprintf("target.%d", i))
		hdr = genHeader(hdr, "Pad", strings.Repeat("X", 200))
		_, _, err = fs.StoreMsg(subj, hdr, nil, 0)
		require_NoError(t, err)
	}
	require_NoError(t, fs.Stop())

	// Remove the persisted scheduling state to force the linear-scan recovery path.
	require_NoError(t, os.Remove(filepath.Join(dir, msgDir, msgSchedulingStreamStateFile)))

	fs, err = newFileStore(fcfg, cfg)
	require_NoError(t, err)
	defer fs.Stop()

	require_True(t, fs.numMsgBlocks() >= 2)

	expected := make(map[string]struct{}, N)
	for _, s := range subjects {
		expected[s] = struct{}{}
	}

	fs.mu.Lock()
	defer fs.mu.Unlock()
	require_True(t, fs.scheduling != nil)
	require_Len(t, len(fs.scheduling.schedules), N)

	for subj, sched := range fs.scheduling.schedules {
		// Map invariant: the key must hash back to itself. If the underlying bytes were
		// rewritten after insertion, the lookup fails even though the entry is "there".
		_, ok := fs.scheduling.schedules[subj]
		require_True(t, ok)

		_, isOriginal := expected[subj]
		require_True(t, isOriginal)

		revSubj, ok := fs.scheduling.seqToSubj[sched.seq]
		require_True(t, ok)
		require_Equal(t, revSubj, subj)
	}
}

func TestFileStoreConvertToEncryptedDoesNotResurrectXoredCache(t *testing.T) {
	storeDir := t.TempDir()
	fcfg := FileStoreConfig{StoreDir: storeDir, BlockSize: 8192}
	cfg := StreamConfig{Name: "zzz", Subjects: []string{"foo"}, Storage: FileStorage}

	fs, err := newFileStore(fcfg, cfg)
	require_NoError(t, err)
	plaintext := bytes.Repeat([]byte("MARKER-PLAINTEXT-PAYLOAD-"), 32)
	_, _, err = fs.StoreMsg("foo", nil, plaintext, 0)
	require_NoError(t, err)
	fs.Stop()

	// Re-open with encryption, which will convert existing blocks.
	fcfg.Cipher = AES
	fs, err = newFileStoreWithCreated(fcfg, cfg, time.Now(), prf(&fcfg), nil)
	require_NoError(t, err)
	defer fs.Stop()

	fs.mu.RLock()
	require_True(t, fs.lmb != nil)
	mb := fs.lmb
	fs.mu.RUnlock()

	mb.mu.Lock()
	defer mb.mu.Unlock()

	require_True(t, mb.bek != nil)

	// Rewrite the on-disk file as plaintext so convertToEncrypted has
	// fresh plaintext to encrypt.
	encBuf, err := mb.loadBlock(nil)
	require_NoError(t, err)
	rbek, err := genBlockEncryptionKey(fcfg.Cipher, mb.seed, mb.nonce)
	require_NoError(t, err)
	rbek.XORKeyStream(encBuf, encBuf)
	fs.dios.acquire()
	err = os.WriteFile(mb.mfn, encBuf, defaultFilePerms)
	fs.dios.release()
	require_NoError(t, err)
	recycleMsgBlockBuf(encBuf)

	// Stage a sentinel on the elastic pointer; the fix must clear it.
	sentinelCache := &cache{buf: bytes.Repeat([]byte{0xCA, 0xFE}, 64)}
	mb.cache = sentinelCache
	mb.ecache.Set(sentinelCache)

	require_NoError(t, mb.convertToEncrypted())

	mb.cache = nil
	require_NoError(t, mb.setupWriteCache(nil))
	require_True(t, mb.cache != nil)
	if mb.cache == sentinelCache {
		t.Fatalf("setupWriteCache resurrected sentinel cache via mb.ecache after convertToEncrypted")
	}
	// If convertToEncrypted wrote ciphertext at the wrong keystream offset, the
	// block would decrypt to garbage and rebuildStateFromBufLocked would silently
	// truncate it to zero bytes — losing the message.
	require_NotEqual(t, len(mb.cache.buf), 0)
}

func TestFileStoreRemoveMsgViaLimitsCallbackAliasedSubj(t *testing.T) {
	fs, err := newFileStore(
		FileStoreConfig{StoreDir: t.TempDir()},
		StreamConfig{Name: "TEST", Storage: FileStorage, MaxMsgs: 1, Discard: DiscardOld},
	)
	require_NoError(t, err)
	defer fs.Stop()

	// Capture the data pointer of subj from the removal callback (md == -1).
	var cbSubjAddr uintptr
	fs.RegisterStorageUpdates(func(md, _ int64, _ uint64, subj string) {
		if md == -1 {
			cbSubjAddr = uintptr(unsafe.Pointer(unsafe.StringData(subj)))
		}
	})

	_, _, err = fs.StoreMsg("foo.bar", nil, []byte("payload-1"), 0)
	require_NoError(t, err)
	// Second store exceeds MaxMsgs=1, should trigger above callback.
	_, _, err = fs.StoreMsg("foo.bar", nil, []byte("payload-2"), 0)
	require_NoError(t, err)
	require_True(t, cbSubjAddr != 0)

	fs.mu.RLock()
	mb := fs.blks[0]
	fs.mu.RUnlock()

	mb.mu.Lock()
	defer mb.mu.Unlock()
	require_NotNil(t, mb.cache)
	bufStart := uintptr(unsafe.Pointer(unsafe.SliceData(mb.cache.buf)))
	bufEnd := bufStart + uintptr(cap(mb.cache.buf))

	if cbSubjAddr >= bufStart && cbSubjAddr < bufEnd {
		t.Fatalf("subj passed to callback aliases mb.cache.buf (subj@%#x in [%#x,%#x))",
			cbSubjAddr, bufStart, bufEnd)
	}
}

func TestFileStoreSubjectForSeqAliasRace(t *testing.T) {
	// Small blocks + many short subjects: many blocks all using the same
	// buffer-pool tier, maximizing the chance that a recycled buf is grabbed
	// by another mb's load.
	fcfg := FileStoreConfig{StoreDir: t.TempDir(), BlockSize: 1024}
	fs, err := newFileStore(fcfg, StreamConfig{
		Name:     "TEST",
		Subjects: []string{"foo.>"},
		Storage:  FileStorage,
	})
	require_NoError(t, err)
	defer fs.Stop()

	const N = 256
	subjects := make([]string, N) // indexed by seq-1
	for i := range subjects {
		// Distinct, recognizable subject per seq; long enough that mid-buffer
		// corruption shows up in the prefix/suffix comparison.
		subjects[i] = fmt.Sprintf("foo.seq-%05d-%s", i+1, strings.Repeat("A", 40))
		_, _, err = fs.StoreMsg(subjects[i], nil, []byte("payload"), 0)
		require_NoError(t, err)
	}
	require_True(t, fs.numMsgBlocks() >= 4)

	var (
		stop      atomic.Bool
		mismatch  atomic.Int64
		gotSubj   atomic.Value // captured corrupt subj for the failure message
		gotSeqVal atomic.Uint64
	)

	var wg sync.WaitGroup

	// Reader workers: random SubjectForSeq lookups, verify subject matches.
	readers := runtime.GOMAXPROCS(0)
	if readers < 4 {
		readers = 4
	}
	for i := 0; i < readers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for !stop.Load() {
				seq := uint64(rand.IntN(N) + 1)
				got, err := fs.SubjectForSeq(seq)
				if err != nil {
					continue
				}
				if got != subjects[seq-1] {
					if mismatch.Add(1) == 1 {
						gotSubj.Store(got)
						gotSeqVal.Store(seq)
					}
					return
				}
			}
		}()
	}

	// Thrasher: force-expire each block's cache so its buf is recycled into
	// the pool while readers may still hold aliased views of it.
	wg.Add(1)
	go func() {
		defer wg.Done()
		for !stop.Load() {
			fs.mu.RLock()
			blks := append([]*msgBlock(nil), fs.blks...)
			fs.mu.RUnlock()
			for _, mb := range blks {
				mb.tryForceExpireCache()
			}
		}
	}()

	// Run for up to 5s, exiting early as soon as a mismatch is seen.
	deadline := time.NewTimer(5 * time.Second)
	defer deadline.Stop()
	tick := time.NewTicker(20 * time.Millisecond)
	defer tick.Stop()
loop:
	for {
		select {
		case <-deadline.C:
			break loop
		case <-tick.C:
			if mismatch.Load() > 0 {
				break loop
			}
		}
	}
	stop.Store(true)
	wg.Wait()

	if n := mismatch.Load(); n > 0 {
		corrupt, _ := gotSubj.Load().(string)
		t.Fatalf("SubjectForSeq returned a corrupted subject %d time(s); seq=%d got=%q want=%q",
			n, gotSeqVal.Load(), corrupt, subjects[gotSeqVal.Load()-1])
	}
}

func TestFileStoreMultiLastSeqsDoesNotReorderConfigSubjects(t *testing.T) {
	fs, err := newFileStore(
		FileStoreConfig{StoreDir: t.TempDir()},
		StreamConfig{Name: "zzz", Subjects: []string{"orders.*", "billing.*"}, Storage: FileStorage})
	require_NoError(t, err)
	defer fs.Stop()

	_, _, err = fs.StoreMsg("orders.1", nil, []byte("x"), 0)
	require_NoError(t, err)
	_, _, err = fs.StoreMsg("billing.1", nil, []byte("x"), 0)
	require_NoError(t, err)

	// Filter count == subject count drives the filterIsAll path.
	_, err = fs.MultiLastSeqs([]string{"orders.*", "billing.*"}, 0, 0)
	require_NoError(t, err)

	// The advertised config subjects must keep their original order.
	require_Equal(t, fs.cfg.Subjects[0], "orders.*")
	require_Equal(t, fs.cfg.Subjects[1], "billing.*")
}

// Must be run with -race.
func TestFileStoreUpdateConfigSyncAlwaysRace(t *testing.T) {
	fcfg := FileStoreConfig{StoreDir: t.TempDir()}
	cfg := StreamConfig{
		Name:        "zzz",
		Storage:     FileStorage,
		Subjects:    []string{"foo"},
		Replicas:    3,
		PersistMode: AsyncPersistMode,
	}
	fs, err := newFileStore(fcfg, cfg)
	require_NoError(t, err)
	defer fs.Stop()

	// Make sure we have a last message block established.
	_, _, err = fs.StoreMsg("foo", nil, []byte("hello"), 0)
	require_NoError(t, err)

	var wg sync.WaitGroup
	stop := make(chan struct{})

	// Reader side: continuously create pending writes and flush them, which
	// reads lmb.syncAlways under mb.mu inside flushPendingMsgsLocked.
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-stop:
				return
			default:
			}
			_, _, _ = fs.StoreMsg("foo", nil, []byte("data"), 0)
			fs.mu.RLock()
			lmb := fs.lmb
			fs.mu.RUnlock()
			if lmb != nil {
				_ = lmb.flushPendingMsgs()
			}
		}
	}()

	// Writer side: UpdateConfig writes lmb.syncAlways without holding lmb.mu.
	wg.Add(1)
	go func() {
		defer wg.Done()
		for range 3000 {
			ncfg := cfg
			_ = fs.UpdateConfig(&ncfg)
		}
		close(stop)
	}()

	wg.Wait()
}

// Must be run with -race.
func TestFileStoreStoreRawMsgTTLsRace(t *testing.T) {
	sd := t.TempDir()
	fs, err := newFileStore(
		// Large block size so all messages stay in a single block (== lmb),
		// guaranteeing the storeRawMsg increments and the indexCacheBuf rewrite
		// target the same message block.
		FileStoreConfig{StoreDir: sd, BlockSize: 8 * 1024 * 1024, CacheExpire: time.Millisecond},
		StreamConfig{Name: "zzz", Subjects: []string{"test"}, Storage: FileStorage, AllowMsgTTL: true},
	)
	require_NoError(t, err)
	defer fs.Stop()

	// Large TTL so messages don't actually expire/remove during the test.
	ttl := int64(3600)
	hdr := fmt.Appendf(nil, "NATS/1.0\r\n%s: %d\r\n\r\n", JSMessageTTL, ttl)

	// Store an initial message so seq 1 exists and lives in the lmb.
	_, _, err = fs.StoreMsg("test", hdr, []byte("hello"), ttl)
	require_NoError(t, err)

	var stop atomic.Bool
	var wg sync.WaitGroup

	// Writers: store TTL messages into the lmb, each bumps fs.lmb.ttls++ under fs.mu only.
	for range 2 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for !stop.Load() {
				if _, _, err := fs.StoreMsg("test", hdr, []byte("hello"), ttl); err != nil {
					return
				}
			}
		}()
	}

	// Readers: force the lmb cache to drop and reload, which reindexes and rewrites
	// mb.ttls/mb.schedules under mb.mu only.
	for range 3 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for !stop.Load() {
				fs.mu.RLock()
				lmb := fs.lmb
				fseq := atomic.LoadUint64(&lmb.first.seq)
				fs.mu.RUnlock()
				// Flush pending to disk then drop the cache so the next load
				// re-reads the block and reindexes it.
				lmb.mu.Lock()
				lmb.flushPendingMsgsLocked()
				lmb.clearCacheAndOffset()
				lmb.mu.Unlock()
				// Load a seq that lives in the lmb; triggers loadMsgsWithLock and
				// indexCacheBuf, which rewrites mb.ttls/mb.schedules under mb.mu.
				fs.LoadMsg(fseq, nil)
			}
		}()
	}

	time.Sleep(2 * time.Second)
	stop.Store(true)
	wg.Wait()
}

// Must be run with -race.
func TestFileStoreSyncDeletedDmapAliasRace(t *testing.T) {
	fcfg := FileStoreConfig{
		StoreDir:  t.TempDir(),
		BlockSize: 1024, // small on purpose to create many blocks
	}
	fs, err := newFileStoreWithCreated(fcfg, StreamConfig{Name: "zzz", Storage: FileStorage}, time.Now(), prf(&fcfg), nil)
	require_NoError(t, err)
	defer fs.Stop()

	// Store enough messages across many blocks.
	subj := "foo"
	msg := bytes.Repeat([]byte("Z"), 50)
	const total = 600
	for range total {
		_, _, err = fs.StoreMsg(subj, nil, msg, 0)
		require_NoError(t, err)
	}

	// Create interior deletes so mb.dmap is non-empty in many blocks.
	for seq := uint64(2); seq <= total-1; seq += 3 {
		_, err = fs.RemoveMsg(seq)
		require_NoError(t, err)
	}

	// Compact each non-last block so the removed records are physically dropped
	// from disk, leaving real holes. This way a later reload via indexCacheBuf
	// must reconstruct the holes with mb.dmap.Insert (the racy writer).
	fs.mu.RLock()
	cblks := append([]*msgBlock(nil), fs.blks...)
	fs.mu.RUnlock()
	for _, mb := range cblks[:len(cblks)-1] {
		mb.mu.Lock()
		err = mb.compact()
		mb.mu.Unlock()
		require_NoError(t, err)
	}

	// Snapshot the delete blocks to feed back into SyncDeleted.
	fs.mu.Lock()
	fs.readLockAllMsgBlocks()
	live, _ := fs.deleteBlocks()
	dbs := make(DeleteBlocks, len(live))
	for i, db := range live {
		switch d := db.(type) {
		case *avl.SequenceSet:
			dbs[i] = d.Clone()
		case *DeleteRange:
			cp := *d
			dbs[i] = &cp
		default:
			dbs[i] = db
		}
	}
	fs.readUnlockAllMsgBlocks()
	fs.mu.Unlock()
	require_True(t, len(dbs) > 0)

	// Grab a snapshot of the blocks so the loader goroutine can force cache expiry.
	fs.mu.RLock()
	blks := append([]*msgBlock(nil), fs.blks...)
	fs.mu.RUnlock()

	var wg sync.WaitGroup
	stop := make(chan struct{})

	// Several reader goroutines continuously SyncDeleted, which traverses live
	// mb.dmap via pruneDeleteBlock without holding mb.mu.
	for range 3 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
				}
				_ = fs.SyncDeleted(dbs)
			}
		}()
	}

	// Several writer goroutines reload blocks, driving indexCacheBuf to Insert
	// into mb.dmap under mb.mu only. A lock-free dmap traversal in SyncDeleted
	// should trigger the race without a fix.
	for range 4 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
				}
				for _, mb := range blks {
					mb.mu.Lock()
					mb.tryForceExpireCacheLocked()
					_ = mb.loadMsgsWithLock()
					mb.mu.Unlock()
				}
			}
		}()
	}

	// Run briefly; the race detector should trip quickly.
	time.Sleep(2 * time.Second)
	close(stop)
	wg.Wait()

	// Sanity: store still usable.
	_ = fs.State()
}

// SyncDeleted collects the storage callbacks for removed blocks and only fires
// a single aggregated callback at the end. Confirm that stream/account
// accounting and consumer NumPending are unaffected by that, for single message
// removals, for bulk block removals, and for a range that mixes both.
func TestFileStoreSyncDeletedAccountingAndNumPending(t *testing.T) {
	for _, test := range []struct {
		name string
		// Returns the delete blocks to sync, and the first and last sequence they cover.
		dbs func(fseq, lseq uint64) (DeleteBlocks, uint64, uint64)
		// Whether all removed messages are removed one by one, which is the only
		// path that terminates outstanding pending messages for a consumer.
		// Removing a block as a whole doesn't.
		termsPending bool
		// Expected number of aggregated callbacks for removed blocks, and of
		// single message callbacks.
		expBatchCBs, expMsgCBs int64
	}{
		{
			// Single message removals only.
			"SingleMsgs",
			func(fseq, lseq uint64) (DeleteBlocks, uint64, uint64) {
				last := fseq + (lseq-fseq)/2
				set := &avl.SequenceSet{}
				for seq := fseq; seq <= last; seq++ {
					set.Insert(seq)
				}
				return DeleteBlocks{set}, fseq, last
			},
			true,
			// Nothing is removed as a block, so all messages one by one.
			0, 500,
		},
		{
			// A range that's block aligned, so full blocks are removed.
			"FullBlocks",
			func(fseq, lseq uint64) (DeleteBlocks, uint64, uint64) {
				return DeleteBlocks{&DeleteRange{First: fseq, Num: lseq - fseq + 1}}, fseq, lseq
			},
			false,
			// All blocks are removed as a whole, so one aggregated callback.
			1, 0,
		},
		{
			// A range that starts and ends in the middle of a block, so the
			// blocks at the edges are removed message by message, while the
			// blocks in between are removed as a whole.
			"MixedBlocks",
			func(fseq, lseq uint64) (DeleteBlocks, uint64, uint64) {
				first, last := fseq+3, lseq-3
				return DeleteBlocks{&DeleteRange{First: first, Num: last - first + 1}}, first, last
			},
			false,
			// Still only one aggregated callback for all removed blocks, plus the
			// messages at the edges that are removed one by one.
			1, 7,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			s := RunBasicJetStreamServer(t)
			defer s.Shutdown()

			acc := s.globalAccount()
			mset, err := acc.addStreamWithStore(
				&StreamConfig{Name: "TEST", Subjects: []string{"foo.>"}, Storage: FileStorage},
				&FileStoreConfig{BlockSize: 1024},
			)
			require_NoError(t, err)

			// Two consumers with a different filter, so they need to calculate
			// NumPending differently.
			for i, filter := range []string{"foo.*", "foo.1"} {
				_, err = mset.addConsumer(&ConsumerConfig{
					Durable:       fmt.Sprintf("d%d", i),
					FilterSubject: filter,
					AckPolicy:     AckExplicit,
				})
				require_NoError(t, err)
			}

			nc, js := jsClientConnect(t, s)
			defer nc.Close()

			for i := range 1000 {
				_, err = js.Publish(fmt.Sprintf("foo.%d", i%10), make([]byte, 100))
				require_NoError(t, err)
			}

			// Have one consumer with outstanding pending messages, those must be
			// terminated when the messages underneath them are removed.
			sub, err := js.PullSubscribe("foo.*", "d0")
			require_NoError(t, err)
			msgs, err := sub.Fetch(10)
			require_NoError(t, err)
			require_Equal(t, len(msgs), 10)

			fs := mset.store.(*fileStore)
			var before StreamState
			fs.FastState(&before)
			require_Equal(t, acc.JetStreamUsage().Store, before.Bytes)

			// Count the callbacks that reach the stream, all removed blocks must
			// result in one aggregated callback, no matter how many there were.
			var msgCBs, batchCBs atomic.Int64
			fs.mu.Lock()
			ocb := fs.scb
			fs.scb = func(md, bd int64, seq uint64, subj string) {
				if md == -1 && seq > 0 && subj != _EMPTY_ {
					msgCBs.Add(1)
				} else {
					batchCBs.Add(1)
				}
				ocb(md, bd, seq, subj)
			}
			fs.mu.Unlock()

			dbs, first, last := test.dbs(before.FirstSeq, before.LastSeq)
			require_NoError(t, fs.SyncDeleted(dbs))

			fs.mu.Lock()
			fs.scb = ocb
			fs.mu.Unlock()

			require_Equal(t, batchCBs.Load(), test.expBatchCBs)
			require_Equal(t, msgCBs.Load(), test.expMsgCBs)

			var after StreamState
			fs.FastState(&after)
			require_Equal(t, after.Msgs, before.Msgs-(last-first+1))

			// The aggregated callback is fired before SyncDeleted returns, so the
			// stream state must match the store and the account usage must have
			// been decremented by exactly the bytes we removed.
			state := mset.state()
			require_Equal(t, state.Msgs, after.Msgs)
			require_Equal(t, state.Bytes, after.Bytes)
			require_Equal(t, acc.JetStreamUsage().Store, after.Bytes)

			// All consumers must have an accurate NumPending.
			mset.clsMu.RLock()
			cList := append([]*consumer(nil), mset.cList...)
			mset.clsMu.RUnlock()
			require_Equal(t, len(cList), 2)

			for _, o := range cList {
				o.mu.RLock()
				npc, sseq, filter := o.npc, o.sseq, o.cfg.FilterSubject
				o.mu.RUnlock()

				expected, _, err := fs.NumPending(sseq, filter, false)
				require_NoError(t, err)
				require_Equal(t, npc, int64(expected))
			}

			if !test.termsPending {
				return
			}
			// Terminating pending messages is done in a separate goroutine.
			checkFor(t, 2*time.Second, 50*time.Millisecond, func() error {
				for _, o := range cList {
					o.mu.RLock()
					var stillPending []uint64
					for seq := range o.pending {
						if seq >= first && seq <= last {
							stillPending = append(stillPending, seq)
						}
					}
					filter := o.cfg.FilterSubject
					o.mu.RUnlock()
					if len(stillPending) > 0 {
						return fmt.Errorf("consumer %q still has removed msgs pending: %v", filter, stillPending)
					}
				}
				return nil
			})
		})
	}
}

func TestFileStoreEncryptionKeyFileSyncedBySyncBlocks(t *testing.T) {
	fcfg := FileStoreConfig{StoreDir: t.TempDir(), Cipher: AES}
	fs, err := newFileStoreWithCreated(fcfg, StreamConfig{Name: "S1", Storage: FileStorage}, time.Now(), prf(&fcfg), nil)
	require_NoError(t, err)
	defer fs.Stop()

	_, _, err = fs.StoreMsg("foo", nil, []byte("Hello World"), 0)
	require_NoError(t, err)

	// The key file is written without a sync, so it should be marked for one.
	fs.mu.RLock()
	lmb := fs.lmb
	fs.mu.RUnlock()
	require_NotNil(t, lmb)
	lmb.mu.RLock()
	needKeySync, kfn := lmb.needKeySync, lmb.kfn
	lmb.mu.RUnlock()
	require_True(t, needKeySync)
	require_True(t, kfn != _EMPTY_)

	// A sync pass should sync the key file and clear the flag.
	fs.syncBlocks()
	lmb.mu.RLock()
	needKeySync = lmb.needKeySync
	lmb.mu.RUnlock()
	require_False(t, needKeySync)

	// With SyncAlways the key file write is already synced, so the flag should not be set.
	fcfg = FileStoreConfig{StoreDir: t.TempDir(), Cipher: AES, SyncAlways: true}
	fs2, err := newFileStoreWithCreated(fcfg, StreamConfig{Name: "S2", Storage: FileStorage}, time.Now(), prf(&fcfg), nil)
	require_NoError(t, err)
	defer fs2.Stop()

	_, _, err = fs2.StoreMsg("foo", nil, []byte("Hello World"), 0)
	require_NoError(t, err)

	fs2.mu.RLock()
	lmb = fs2.lmb
	fs2.mu.RUnlock()
	require_NotNil(t, lmb)
	lmb.mu.RLock()
	needKeySync = lmb.needKeySync
	lmb.mu.RUnlock()
	require_False(t, needKeySync)

	// With SyncOnFlush the key file write is also already synced, so the flag should not be set.
	fcfg = FileStoreConfig{StoreDir: t.TempDir(), Cipher: AES, SyncAlways: true, SyncOnFlush: true}
	fs3, err := newFileStoreWithCreated(fcfg, StreamConfig{Name: "S3", Storage: FileStorage}, time.Now(), prf(&fcfg), nil)
	require_NoError(t, err)
	defer fs3.Stop()

	_, _, err = fs3.StoreMsg("foo", nil, []byte("Hello World"), 0)
	require_NoError(t, err)

	fs3.mu.RLock()
	lmb = fs3.lmb
	fs3.mu.RUnlock()
	require_NotNil(t, lmb)
	lmb.mu.RLock()
	needKeySync = lmb.needKeySync
	lmb.mu.RUnlock()
	require_False(t, needKeySync)
}

func TestFileStoreStoreRawMsgVsConcurrentBlockRemovalNoWriteErr(t *testing.T) {
	fcfg := FileStoreConfig{StoreDir: t.TempDir(), BlockSize: 256}
	cfg := StreamConfig{Name: "zzz", Subjects: []string{"foo.>"}, Storage: FileStorage, MaxMsgsPer: 1}
	fs, err := newFileStore(fcfg, cfg)
	require_NoError(t, err)
	defer fs.Stop()

	const writers = 4
	const readers = 4

	var stop atomic.Bool
	var wg sync.WaitGroup
	var lastSeq atomic.Uint64
	msg := make([]byte, 128)

	// Writers evict their own previous message on every store; removers race
	// them for that same sequence, deleting its (single-message) block.
	for w := 0; w < writers; w++ {
		seqs := make(chan uint64, 64)
		subj := fmt.Sprintf("foo.%d", w)
		wg.Add(2)
		go func() {
			defer wg.Done()
			defer close(seqs)
			for !stop.Load() {
				seq, _, err := fs.StoreMsg(subj, nil, msg, 0)
				if err != nil {
					return
				}
				lastSeq.Store(seq)
				select {
				case seqs <- seq:
				default:
				}
			}
		}()
		go func() {
			defer wg.Done()
			for seq := range seqs {
				fs.RemoveMsg(seq)
			}
		}()
	}

	// Readers keep mb.mu contended so a remover holding fs.mu can barge past
	// a storer parked on mb.mu inside firstSeqForSubj.
	for r := 0; r < readers; r++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			var smv StoreMsg
			for !stop.Load() {
				if seq := lastSeq.Load(); seq > 2 {
					fs.LoadMsg(seq-2, &smv)
				}
			}
		}()
	}

	// Run for a fixed period, checking that no write error is ever latched.
	timeout := time.Now().Add(2 * time.Second)
	for time.Now().Before(timeout) {
		fs.mu.RLock()
		werr := fs.werr
		fs.mu.RUnlock()
		if werr != nil {
			stop.Store(true)
			wg.Wait()
			t.Fatalf("store was disabled by a concurrent removal: werr=%v", werr)
		}
		time.Sleep(10 * time.Millisecond)
	}
	stop.Store(true)
	wg.Wait()

	// The store must still be fully writable.
	_, _, err = fs.StoreMsg("foo.healthy", nil, msg, 0)
	require_NoError(t, err)
}

func TestFileStoreStoreRawMsgVsAgeExpiryNoWriteErr(t *testing.T) {
	fcfg := FileStoreConfig{StoreDir: t.TempDir(), BlockSize: 256}
	cfg := StreamConfig{
		Name: "zzz", Subjects: []string{"foo.>"}, Storage: FileStorage,
		MaxMsgsPer: 1, MaxAge: time.Millisecond,
	}
	fs, err := newFileStore(fcfg, cfg)
	require_NoError(t, err)
	defer fs.Stop()

	const writers = 16
	const readers = 4

	var stop atomic.Bool
	var wg sync.WaitGroup
	msg := make([]byte, 128)

	prevSeqs := make([]atomic.Uint64, writers)
	for w := 0; w < writers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			subj := fmt.Sprintf("foo.%d", w)
			for !stop.Load() {
				seq, _, err := fs.StoreMsg(subj, nil, msg, 0)
				if err != nil {
					return
				}
				prevSeqs[w].Store(seq)
				// Pace so the previous message is expiry-eligible exactly
				// while this store's eviction is in flight.
				time.Sleep(time.Millisecond + time.Duration(w%4)*50*time.Microsecond)
			}
		}(w)
	}

	// Readers contend mb.mu on the blocks evictions will target.
	for r := 0; r < readers; r++ {
		wg.Add(1)
		go func(r int) {
			defer wg.Done()
			var smv StoreMsg
			for i := 0; !stop.Load(); i++ {
				if seq := prevSeqs[(r+i)%writers].Load(); seq > 0 {
					fs.LoadMsg(seq, &smv)
				}
			}
		}(r)
	}

	// Drive expiry sweeps continuously rather than waiting on the age check
	// timer, which fires far too infrequently to collide with the eviction
	// window inside the test's time budget. This runs the exact same sweep
	// the timer would, just often enough to race every store.
	wg.Add(1)
	go func() {
		defer wg.Done()
		for !stop.Load() {
			fs.expireMsgs()
		}
	}()

	timeout := time.Now().Add(2 * time.Second)
	for time.Now().Before(timeout) {
		fs.mu.RLock()
		werr := fs.werr
		fs.mu.RUnlock()
		if werr != nil {
			stop.Store(true)
			wg.Wait()
			t.Fatalf("store was disabled by racing age expiry: werr=%v", werr)
		}
		time.Sleep(10 * time.Millisecond)
	}
	stop.Store(true)
	wg.Wait()

	_, _, err = fs.StoreMsg("foo.healthy", nil, msg, 0)
	require_NoError(t, err)
}

func TestFileStoreCompactStoreMsgRace(t *testing.T) {
	fs, err := newFileStore(
		FileStoreConfig{StoreDir: t.TempDir()},
		StreamConfig{Name: "zzz", Storage: FileStorage},
	)
	require_NoError(t, err)
	defer fs.Stop()

	_, _, err = fs.StoreMsg("foo", nil, []byte("first"), 0)
	require_NoError(t, err)

	var purged, storedSeq uint64
	var compactErr, storeErr error

	var wg sync.WaitGroup
	wg.Go(func() {
		purged, compactErr = fs.Compact(2)
	})

	wg.Go(func() {
		storedSeq, _, storeErr = fs.StoreMsg("foo", nil, []byte("second"), 0)
	})
	wg.Wait()

	require_NoError(t, compactErr)
	require_Equal(t, purged, 1)
	require_NoError(t, storeErr)
	require_Equal(t, storedSeq, 2)

	var sm StoreMsg
	_, err = fs.LoadMsg(2, &sm)
	require_NoError(t, err)
	require_Equal(t, sm.seq, 2)

	state := fs.State()
	require_Equal(t, state.Msgs, 1)
	require_Equal(t, state.FirstSeq, 2)
	require_Equal(t, state.LastSeq, 2)
}

func TestFileStoreDeleteMapView(t *testing.T) {
	msg := []byte("hello")
	// Size blocks to hold 10 messages each.
	fs, err := newFileStore(
		FileStoreConfig{StoreDir: t.TempDir(), BlockSize: 10 * fileStoreMsgSize("foo", nil, msg)},
		StreamConfig{Name: "zzz", Subjects: []string{"foo"}, Storage: FileStorage})
	require_NoError(t, err)
	defer fs.Stop()

	const numMsgs = 100
	for range numMsgs {
		_, _, err = fs.StoreMsg("foo", nil, msg, 0)
		require_NoError(t, err)
	}

	// Create interior deletes in every block. Keep each block's first and
	// last message alive so all removals stay interior to their block; a
	// removal at a block's edge would advance the block's first sequence and
	// turn it into a gap delete, which the view excludes by design.
	expected := make(map[uint64]bool)
	for seq := uint64(1); seq <= numMsgs; seq++ {
		if seq%10 == 1 || seq%10 == 0 {
			continue
		}
		_, err = fs.RemoveMsg(seq)
		require_NoError(t, err)
		expected[seq] = true
	}

	snapshotBlks := func() []*msgBlock {
		fs.mu.RLock()
		defer fs.mu.RUnlock()
		return append([]*msgBlock(nil), fs.blks...)
	}

	checkView := func(v *interiorDeletes) {
		t.Helper()
		var total int
		for _, ss := range v.sets {
			total += ss.Size()
		}
		require_Equal(t, total, len(expected))
		// Ascending probes mostly hit the cursor fast path.
		for seq := uint64(0); seq <= numMsgs+1; seq++ {
			require_Equal(t, v.Exists(seq), expected[seq])
		}
		// Descending probes mostly take the binary search path.
		for seq := uint64(numMsgs + 1); ; seq-- {
			require_Equal(t, v.Exists(seq), expected[seq])
			if seq == 0 {
				break
			}
		}
		// Random probes mix both paths.
		rng := rand.New(rand.NewPCG(0, 0))
		for i := 0; i < numMsgs*4; i++ {
			seq := uint64(rng.IntN(numMsgs + 2))
			require_Equal(t, v.Exists(seq), expected[seq])
		}
	}
	checkView(deleteMap(snapshotBlks()))

	// Empty out one interior block completely. The store removes the block,
	// and its deletes become a gap that the view must exclude.
	fs.mu.RLock()
	mb := fs.blks[3]
	mb.mu.RLock()
	fseq, lseq := atomic.LoadUint64(&mb.first.seq), atomic.LoadUint64(&mb.last.seq)
	mb.mu.RUnlock()
	nblks := len(fs.blks)
	fs.mu.RUnlock()

	for seq := fseq; seq <= lseq; seq++ {
		if !expected[seq] {
			_, err = fs.RemoveMsg(seq)
			require_NoError(t, err)
		}
		delete(expected, seq)
	}
	fs.mu.RLock()
	lblks := len(fs.blks)
	fs.mu.RUnlock()
	require_Equal(t, lblks, nblks-1)

	checkView(deleteMap(snapshotBlks()))

	// Remove whole blocks while holding a stale snapshot, mirroring blocks
	// being removed between syncBlocks capturing its block list and building
	// the view. Compact removes the blocks wholesale, which marks them closed
	// but leaves their dmap populated; the view must exclude their deletes.
	staleBlks := snapshotBlks()
	_, err = fs.Compact(41)
	require_NoError(t, err)
	for seq := range expected {
		if seq < 41 {
			delete(expected, seq)
		}
	}
	checkView(deleteMap(staleBlks))
}

func Benchmark_FileStoreDeleteMap(b *testing.B) {
	msg := []byte("hello")
	// Many small blocks with mostly interior deletes, the shape a stream
	// with MaxMsgsPerSubject=1 and steady per-subject overwrites converges
	// to. Keep two messages alive per block so every block survives with a
	// dense interior delete map.
	fs, err := newFileStore(
		FileStoreConfig{StoreDir: b.TempDir(), BlockSize: 100 * fileStoreMsgSize("foo", nil, msg)},
		StreamConfig{Name: "zzz", Subjects: []string{"foo"}, Storage: FileStorage})
	require_NoError(b, err)
	defer fs.Stop()

	const numMsgs = 50_000
	for range numMsgs {
		_, _, err = fs.StoreMsg("foo", nil, msg, 0)
		require_NoError(b, err)
	}
	for seq := uint64(2); seq < numMsgs; seq++ {
		if seq%50 == 0 {
			continue
		}
		_, err = fs.RemoveMsg(seq)
		require_NoError(b, err)
	}

	fs.mu.RLock()
	blks := append([]*msgBlock(nil), fs.blks...)
	fs.mu.RUnlock()

	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		if v := deleteMap(blks); len(v.sets) == 0 {
			b.Fatalf("Expected a non-empty delete map view")
		}
	}
	b.StopTimer()
}

func Benchmark_FileStoreDeleteMapExists(b *testing.B) {
	msg := []byte("hello")
	fs, err := newFileStore(
		FileStoreConfig{StoreDir: b.TempDir(), BlockSize: 100 * fileStoreMsgSize("foo", nil, msg)},
		StreamConfig{Name: "zzz", Subjects: []string{"foo"}, Storage: FileStorage})
	require_NoError(b, err)
	defer fs.Stop()

	const numMsgs = 50_000
	for range numMsgs {
		_, _, err = fs.StoreMsg("foo", nil, msg, 0)
		require_NoError(b, err)
	}
	for seq := uint64(2); seq < numMsgs; seq++ {
		if seq%50 == 0 {
			continue
		}
		_, err = fs.RemoveMsg(seq)
		require_NoError(b, err)
	}

	fs.mu.RLock()
	blks := append([]*msgBlock(nil), fs.blks...)
	fs.mu.RUnlock()
	v := deleteMap(blks)

	// Probe ascending across the whole span, the same order compactWithFloor
	// checks tombstones in.
	b.ReportAllocs()
	b.ResetTimer()
	for i := range b.N {
		v.Exists(uint64(i%numMsgs) + 1)
	}
	b.StopTimer()
}

func TestFileStoreEncodedStreamStateWithSources(t *testing.T) {
	const iname = "ORIGIN1 > >"

	dir := t.TempDir()
	fcfg := FileStoreConfig{StoreDir: dir}
	cfg := StreamConfig{
		Name:     "SOURCE",
		Subjects: []string{">"},
		Storage:  FileStorage,
		Sources:  []*StreamSource{{Name: "ORIGIN1"}},
	}

	fs, err := newFileStore(fcfg, cfg)
	require_NoError(t, err)
	defer fs.Stop()

	hdr := genHeader(nil, JSStreamSource, "ORIGIN1 5 > > foo.a IDENT1")
	for range 3 {
		_, _, err = fs.StoreMsg("foo.a", hdr, nil, 0)
		require_NoError(t, err)
	}
	// Interior delete, so the encoding also carries delete blocks, which are read
	// to the end of the buffer and so must stay last.
	_, err = fs.RemoveMsg(2)
	require_NoError(t, err)

	// Without the sources, the encoding stays on the version every server accepts.
	buf, err := fs.EncodedStreamState(7, false)
	require_NoError(t, err)
	require_Equal(t, buf[1], streamStateVersion)
	require_True(t, IsEncodedStreamState(buf))

	ss, err := DecodeStreamState(buf)
	require_NoError(t, err)
	require_Equal(t, len(ss.Sources), 0)
	require_Equal(t, ss.Failed, 7)
	require_Len(t, len(ss.Deleted), 1)

	// With the sources, the version is bumped and the state round-trips.
	buf, err = fs.EncodedStreamState(7, true)
	require_NoError(t, err)
	require_Equal(t, buf[1], streamStateVersionSources)
	require_True(t, IsEncodedStreamState(buf))

	ss, err = DecodeStreamState(buf)
	require_NoError(t, err)
	require_Equal(t, ss.Failed, 7)
	require_Len(t, len(ss.Deleted), 1)
	require_Len(t, len(ss.Sources), 1)
	require_Equal(t, ss.Sources[iname].Seq, 5)
	require_Equal(t, ss.Sources[iname].Ident, "IDENT1")

	fs2, err := newFileStore(FileStoreConfig{StoreDir: t.TempDir()}, cfg)
	require_NoError(t, err)
	defer fs2.Stop()

	fs2.ApplySourcesState(ss.Sources)
	require_Equal(t, fs2.SourcesState()[iname].Seq, 5)
	require_Equal(t, fs2.SourcesState()[iname].Ident, "IDENT1")

	// The leader's view replaces what we had, it is authoritative even when that
	// means moving an entry back.
	fs2.ApplySourcesState(map[string]StreamSourceState{iname: {Seq: 2, Ident: "OLD"}})
	require_Equal(t, fs2.SourcesState()[iname].Seq, 2)
	require_Equal(t, fs2.SourcesState()[iname].Ident, "OLD")

	// An empty set says nothing, so it leaves what we have alone.
	fs2.ApplySourcesState(nil)
	require_Equal(t, fs2.SourcesState()[iname].Seq, 2)

	// Anything the leader no longer tracks is dropped, but a configured source
	// stays present as not yet observed.
	fs2.ApplySourcesState(map[string]StreamSourceState{"OTHER > >": {Seq: 9}})
	require_Equal(t, fs2.SourcesState()["OTHER > >"].Seq, 9)
	_, ok := fs2.SourcesState()[iname]
	require_True(t, ok)
	fs2.mu.RLock()
	seeded := fs2.sources[iname]
	fs2.mu.RUnlock()
	require_NotNil(t, seeded)
	require_Equal(t, seeded.Seq, 0)
}

// https://github.com/nats-io/nats-server/issues/8547
// A secure remove drops mb.mu while it writes the delete tombstone into the last block.
// If the block cache expires in that window, the secure branch used to dereference a nil mb.cache.
func TestFileStoreEraseMsgCacheExpiredDuringTombstoneWrite(t *testing.T) {
	defer require_NoPanic(t)

	fs, err := newFileStore(
		FileStoreConfig{StoreDir: t.TempDir(), BlockSize: 1024},
		StreamConfig{Name: "zzz", Storage: FileStorage})
	require_NoError(t, err)

	msg := []byte("Hello World")
	for i := 0; i < 200; i++ {
		_, _, err = fs.StoreMsg("foo", nil, msg, 0)
		require_NoError(t, err)
	}
	// We need more than one block so tombstones land in the last block, not the one we erase from.
	require_True(t, fs.numMsgBlocks() > 1)

	fs.mu.RLock()
	mb := fs.blks[0]
	fs.mu.RUnlock()
	mb.mu.Lock()
	first, last := atomic.LoadUint64(&mb.first.seq), atomic.LoadUint64(&mb.last.seq)
	// Pretend the last write was long ago so a forced expire actually drops the cache.
	mb.lwts = 0
	mb.mu.Unlock()

	// Expire the cache as fast as we can, like the expiration timer or a linear scan ending on this block.
	// TryLock so a panicking erase that still holds mb.mu cannot hang the test on wg.Wait.
	stop := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-stop:
				return
			default:
				if mb.mu.TryLock() {
					mb.tryForceExpireCacheLocked()
					mb.mu.Unlock()
				}
			}
		}
	}()
	defer func() {
		close(stop)
		wg.Wait()
	}()

	// Keep one message so the block is not removed as empty.
	for seq := first; seq < last; seq++ {
		removed, err := fs.EraseMsg(seq)
		require_NoError(t, err)
		require_True(t, removed)
	}
	fs.Stop()
}

// Delete renames the store directory, but syncBlocks fsyncs outside fs.mu, so
// stopping the store must wait for an in-flight sync before the directory can
// be moved away underneath it.
func TestFileStoreStopWaitsForInflightSync(t *testing.T) {
	fs, err := newFileStore(
		FileStoreConfig{StoreDir: t.TempDir(), SyncInterval: time.Hour},
		StreamConfig{Name: "zzz", Subjects: []string{"foo"}, Storage: FileStorage})
	require_NoError(t, err)
	defer fs.Delete(true)

	_, _, err = fs.StoreMsg("foo", nil, []byte("Hello World"), 0)
	require_NoError(t, err)

	// Stand in for a running syncBlocks.
	fs.syncMu.Lock()
	deleteDone := make(chan error, 1)
	go func() { deleteDone <- fs.Delete(true) }()

	select {
	case <-deleteDone:
		t.Fatal("Delete did not wait for the in-flight sync")
	case <-time.After(100 * time.Millisecond):
	}
	_, err = os.Stat(fs.fcfg.StoreDir)
	require_NoError(t, err)

	fs.syncMu.Unlock()
	require_NoError(t, <-deleteDone)
}

func testFileStoreConcurrentCompaction(t *testing.T, read func(*fileStore, uint64) error) {
	t.Helper()
	for _, blocks := range []int{2, 64} {
		t.Run(fmt.Sprintf("blocks=%d", blocks), func(t *testing.T) {
			fs, err := newFileStore(
				FileStoreConfig{
					StoreDir: t.TempDir(), BlockSize: 1024 * 1024,
					CacheExpire: time.Hour, SyncInterval: time.Hour,
				},
				StreamConfig{Name: "TEST", Subjects: []string{"foo"}, Storage: FileStorage, Retention: WorkQueuePolicy},
			)
			require_NoError(t, err)
			defer fs.Stop()

			// A populated first block gives readers time to overlap the compaction scan.
			const seq = 4096
			for range seq {
				_, _, err := fs.StoreMsg("foo", nil, []byte("hello"), 0)
				require_NoError(t, err)
			}
			for i := 1; i < blocks; i++ {
				fs.mu.Lock()
				_, err := fs.newMsgBlockForWrite()
				fs.mu.Unlock()
				require_NoError(t, err)
				_, _, err = fs.StoreMsg("foo", nil, []byte("hello"), 0)
				require_NoError(t, err)
			}
			removed, err := fs.RemoveMsg(2)
			require_NoError(t, err)
			require_True(t, removed)
			require_NoError(t, fs.FlushAllPending())
			require_Equal(t, fs.numMsgBlocks(), blocks)
			require_NoError(t, read(fs, seq))

			var stop atomic.Bool
			var reads atomic.Uint64
			var wg sync.WaitGroup
			errs := make(chan error, 1)
			for range 4 {
				wg.Add(1)
				go func() {
					defer wg.Done()
					for !stop.Load() {
						if err := read(fs, seq); err != nil {
							select {
							case errs <- err:
							default:
							}
							return
						}
						reads.Add(1)
						runtime.Gosched()
					}
				}()
			}
			defer func() { stop.Store(true); wg.Wait() }()

			mb := fs.getFirstBlock()
			for range 25 {
				// Match syncBlocks' shared store lock and exclusive block lock.
				fs.mu.RLock()
				mb.mu.Lock()
				err := mb.compact()
				mb.mu.Unlock()
				fs.mu.RUnlock()
				require_NoError(t, err)
				runtime.Gosched()
			}
			stop.Store(true)
			wg.Wait()
			_, err = fs.LoadMsg(seq, nil)
			require_NoError(t, err)
			select {
			case err := <-errs:
				t.Fatal(err)
			default:
			}
			require_True(t, reads.Load() > 0)
			require_NoError(t, read(fs, seq))
		})
	}
}

func TestFileStoreCompactionPreservesConcurrentBlockSelection(t *testing.T) {
	testFileStoreConcurrentCompaction(t, func(fs *fileStore, seq uint64) error {
		sm, _, err := fs.LoadNextMsg("", false, seq, nil)
		if err != nil {
			return err
		}
		if sm.seq != seq {
			return fmt.Errorf("read starting at %d returned %d", seq, sm.seq)
		}
		return nil
	})
}

func TestFileStoreCompactionPreservesRedelivery(t *testing.T) {
	testFileStoreConcurrentCompaction(t, func(fs *fileStore, seq uint64) error {
		var state StreamState
		fs.FastState(&state)
		o := &consumer{
			srv:       &Server{opts: &Options{}},
			mset:      &stream{store: fs},
			cfg:       ConsumerConfig{AckPolicy: AckExplicit},
			retention: WorkQueuePolicy,
			sseq:      state.LastSeq + 1,
			dseq:      2,
			asflr:     seq - 1,
			maxdc:     10,
			pending:   map[uint64]*Pending{seq: {Sequence: 1, Timestamp: time.Now().Add(-time.Minute).UnixNano()}},
		}
		o.addToRedeliverQueue(seq)
		o.mu.Lock()
		pmsg, dc, err := o.getNextMsg()
		_, pending := o.pending[seq]
		_, retry := o.rdc[seq]
		floor := o.asflr
		o.mu.Unlock()
		var delivered uint64
		if pmsg != nil {
			delivered = pmsg.seq
			pmsg.returnToPool()
		}
		if err != nil || delivered != seq || dc != 2 || !pending || !retry || floor != seq-1 {
			return fmt.Errorf("retained redelivery lost: seq=%d deliveries=%d error=%v pending=%v retry=%v ack_floor=%d",
				delivered, dc, err, pending, retry, floor)
		}
		return nil
	})
}
