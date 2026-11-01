// Copyright 2012-2026 The NATS Authors
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
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"testing"
	"time"

	"github.com/nats-io/nats-server/v2/server/ats"
	"github.com/nats-io/nats-server/v2/server/gsl"
)

func testAllStoreAllPermutations(t *testing.T, compressionAndEncryption bool, cfg StreamConfig, fn func(t *testing.T, fs StreamStore)) {
	t.Run("Memory", func(t *testing.T) {
		cfg.Storage = MemoryStorage
		fs, err := newMemStore(&cfg)
		require_NoError(t, err)
		defer fs.Stop()
		fn(t, fs)
	})
	t.Run("File", func(t *testing.T) {
		cfg.Storage = FileStorage
		if compressionAndEncryption {
			testFileStoreAllPermutations(t, func(t *testing.T, fcfg FileStoreConfig) {
				fs, err := newFileStore(fcfg, cfg)
				require_NoError(t, err)
				defer fs.Stop()
				fn(t, fs)
			})
		} else {
			fs, err := newFileStore(FileStoreConfig{
				StoreDir: t.TempDir(),
			}, cfg)
			require_NoError(t, err)
			defer fs.Stop()
			fn(t, fs)
		}
	})
}

func TestStoreMsgLoadNextMsgMulti(t *testing.T) {
	testAllStoreAllPermutations(
		t, false,
		StreamConfig{Name: "zzz", Subjects: []string{"foo.*"}},
		func(t *testing.T, fs StreamStore) {
			// Put 1k msgs in
			for i := 0; i < 1000; i++ {
				subj := fmt.Sprintf("foo.%d", i)
				fs.StoreMsg(subj, nil, []byte("ZZZ"), 0)
			}

			var smv StoreMsg
			// Do multi load next with 1 wc entry.
			sl := gsl.NewSublist[struct{}]()
			sl.Insert("foo.>", struct{}{})
			for i, seq := 0, uint64(1); i < 1000; i++ {
				sm, nseq, err := fs.LoadNextMsgMulti(sl, seq, &smv)
				require_NoError(t, err)
				require_Equal(t, sm.subj, fmt.Sprintf("foo.%d", i))
				require_Equal(t, nseq, seq)
				seq++
			}

			// Now do multi load next with 1000 literal subjects.
			sl = gsl.NewSublist[struct{}]()
			for i := 0; i < 1000; i++ {
				subj := fmt.Sprintf("foo.%d", i)
				sl.Insert(subj, struct{}{})
			}
			for i, seq := 0, uint64(1); i < 1000; i++ {
				sm, nseq, err := fs.LoadNextMsgMulti(sl, seq, &smv)
				require_NoError(t, err)
				require_Equal(t, sm.subj, fmt.Sprintf("foo.%d", i))
				require_Equal(t, nseq, seq)
				seq++
			}

			// Check that we can pull out 3 individuals.
			sl = gsl.NewSublist[struct{}]()
			sl.Insert("foo.2", struct{}{})
			sl.Insert("foo.222", struct{}{})
			sl.Insert("foo.999", struct{}{})
			sm, seq, err := fs.LoadNextMsgMulti(sl, 1, &smv)
			require_NoError(t, err)
			require_Equal(t, sm.subj, "foo.2")
			require_Equal(t, seq, 3)
			sm, seq, err = fs.LoadNextMsgMulti(sl, seq+1, &smv)
			require_NoError(t, err)
			require_Equal(t, sm.subj, "foo.222")
			require_Equal(t, seq, 223)
			sm, seq, err = fs.LoadNextMsgMulti(sl, seq+1, &smv)
			require_NoError(t, err)
			require_Equal(t, sm.subj, "foo.999")
			require_Equal(t, seq, 1000)
			_, seq, err = fs.LoadNextMsgMulti(sl, seq+1, &smv)
			require_Error(t, err)
			require_Equal(t, seq, 1000)
		},
	)
}

func TestStoreLoadNextMsgWildcardStartBeforeFirstMatch(t *testing.T) {
	testAllStoreAllPermutations(
		t, false,
		StreamConfig{Name: "zzz", Subjects: []string{"bar.*", "foo.*"}},
		func(t *testing.T, fs StreamStore) {
			// Fill non-matching subjects first so the first wildcard match starts
			// strictly after the requested start sequence.
			for i := 0; i < 100; i++ {
				subj := fmt.Sprintf("bar.%d", i)
				_, _, err := fs.StoreMsg(subj, nil, nil, 0)
				require_NoError(t, err)
			}
			seq, _, err := fs.StoreMsg("foo.1", nil, nil, 0)
			require_NoError(t, err)
			require_Equal(t, seq, uint64(101))

			var smv StoreMsg
			sm, nseq, err := fs.LoadNextMsg("foo.*", true, 1, &smv)
			require_NoError(t, err)
			require_Equal(t, sm.subj, "foo.1")
			require_Equal(t, nseq, uint64(101))

			_, nseq, err = fs.LoadNextMsg("foo.*", true, nseq+1, &smv)
			require_Error(t, err)
			require_Equal(t, nseq, uint64(101))
		},
	)
}

func TestStoreDeleteSlice(t *testing.T) {
	ds := DeleteSlice{2}
	var deletes []uint64
	ds.Range(func(seq uint64) bool {
		deletes = append(deletes, seq)
		return true
	})
	require_Len(t, len(deletes), 1)
	require_Equal(t, deletes[0], 2)

	first, last, num := ds.State()
	require_Equal(t, first, 2)
	require_Equal(t, last, 2)
	require_Equal(t, num, 1)
}

func TestStoreDeleteRange(t *testing.T) {
	dr := DeleteRange{First: 2, Num: 1}
	var deletes []uint64
	dr.Range(func(seq uint64) bool {
		deletes = append(deletes, seq)
		return true
	})
	require_Len(t, len(deletes), 1)
	require_Equal(t, deletes[0], 2)

	first, last, num := dr.State()
	require_Equal(t, first, 2)
	require_Equal(t, last, 2)
	require_Equal(t, num, 1)
}

func TestStoreSubjectStateConsistency(t *testing.T) {
	testAllStoreAllPermutations(
		t, false,
		StreamConfig{Name: "TEST", Subjects: []string{"foo"}},
		func(t *testing.T, fs StreamStore) {
			getSubjectState := func() SimpleState {
				t.Helper()
				ss := fs.SubjectsState("foo")
				return ss["foo"]
			}
			var smp StoreMsg
			expectFirstSeq := func(eseq uint64) {
				t.Helper()
				sm, _, err := fs.LoadNextMsg("foo", false, 0, &smp)
				require_NoError(t, err)
				require_Equal(t, sm.seq, eseq)
			}
			expectLastSeq := func(eseq uint64) {
				t.Helper()
				sm, err := fs.LoadLastMsg("foo", &smp)
				require_NoError(t, err)
				require_Equal(t, sm.seq, eseq)
			}

			// Publish an initial batch of messages.
			for i := 0; i < 4; i++ {
				_, _, err := fs.StoreMsg("foo", nil, nil, 0)
				require_NoError(t, err)
			}

			// Expect 4 msgs, with first=1, last=4.
			ss := getSubjectState()
			require_Equal(t, ss.Msgs, 4)
			require_Equal(t, ss.First, 1)
			expectFirstSeq(1)
			require_Equal(t, ss.Last, 4)
			expectLastSeq(4)

			// Remove first message, ss.First is lazy so will only mark ss.firstNeedsUpdate.
			removed, err := fs.RemoveMsg(1)
			require_NoError(t, err)
			require_True(t, removed)

			// Will update first, so corrects to seq 2.
			ss = getSubjectState()
			require_Equal(t, ss.Msgs, 3)
			require_Equal(t, ss.First, 2)
			expectFirstSeq(2)
			require_Equal(t, ss.Last, 4)
			expectLastSeq(4)

			// Remove last message, ss.Last is lazy so will only mark ss.lastNeedsUpdate.
			removed, err = fs.RemoveMsg(4)
			require_NoError(t, err)
			require_True(t, removed)

			// Will update last, so corrects to 3.
			ss = getSubjectState()
			require_Equal(t, ss.Msgs, 2)
			require_Equal(t, ss.First, 2)
			expectFirstSeq(2)
			require_Equal(t, ss.Last, 3)
			expectLastSeq(3)

			// Remove first message again.
			removed, err = fs.RemoveMsg(2)
			require_NoError(t, err)
			require_True(t, removed)

			// Since we only have one message left, must update ss.First and ensure ss.Last equals.
			ss = getSubjectState()
			require_Equal(t, ss.Msgs, 1)
			require_Equal(t, ss.First, 3)
			expectFirstSeq(3)
			require_Equal(t, ss.Last, 3)
			expectLastSeq(3)

			// Publish some more messages so we can test another scenario.
			for i := 0; i < 3; i++ {
				_, _, err := fs.StoreMsg("foo", nil, nil, 0)
				require_NoError(t, err)
			}

			// Just check the state is complete again.
			ss = getSubjectState()
			require_Equal(t, ss.Msgs, 4)
			require_Equal(t, ss.First, 3)
			expectFirstSeq(3)
			require_Equal(t, ss.Last, 7)
			expectLastSeq(7)

			// Remove last sequence, ss.Last is lazy so doesn't get updated.
			removed, err = fs.RemoveMsg(7)
			require_NoError(t, err)
			require_True(t, removed)

			// Remove first sequence, ss.First is lazy so doesn't get updated.
			removed, err = fs.RemoveMsg(3)
			require_NoError(t, err)
			require_True(t, removed)

			// Remove (now) first sequence. Both ss.First and ss.Last are lazy and both need to be recalculated later.
			removed, err = fs.RemoveMsg(5)
			require_NoError(t, err)
			require_True(t, removed)

			// ss.First and ss.Last should both be recalculated and equal each other.
			ss = getSubjectState()
			require_Equal(t, ss.Msgs, 1)
			require_Equal(t, ss.First, 6)
			expectFirstSeq(6)
			require_Equal(t, ss.Last, 6)
			expectLastSeq(6)

			// We store a new message for ss.Last and remove it after, which marks it to be recalculated.
			_, _, err = fs.StoreMsg("foo", nil, nil, 0)
			require_NoError(t, err)
			removed, err = fs.RemoveMsg(8)
			require_NoError(t, err)
			require_True(t, removed)
			// This will be the new ss.Last message, so reset ss.lastNeedsUpdate
			_, _, err = fs.StoreMsg("foo", nil, nil, 0)
			require_NoError(t, err)

			// ss.First should remain the same, but ss.Last should equal the last message.
			ss = getSubjectState()
			require_Equal(t, ss.Msgs, 2)
			require_Equal(t, ss.First, 6)
			expectFirstSeq(6)
			require_Equal(t, ss.Last, 9)
			expectLastSeq(9)
		},
	)
}

func TestStoreSubjectStateConsistencyOptimization(t *testing.T) {
	testAllStoreAllPermutations(
		t, false,
		StreamConfig{Name: "TEST", Subjects: []string{"foo"}},
		func(t *testing.T, fs StreamStore) {
			fillMsgs := func(c int) {
				t.Helper()
				for i := 0; i < c; i++ {
					_, _, err := fs.StoreMsg("foo", nil, nil, 0)
					require_NoError(t, err)
				}
			}
			removeMsgs := func(seqs ...uint64) {
				t.Helper()
				for _, seq := range seqs {
					removed, err := fs.RemoveMsg(seq)
					require_NoError(t, err)
					require_True(t, removed)
				}
			}
			getSubjectState := func() (ss *SimpleState) {
				t.Helper()
				if f, ok := fs.(*fileStore); ok {
					ss, ok = f.lmb.fss.Find([]byte("foo"))
					require_True(t, ok)
				} else if ms, ok := fs.(*memStore); ok {
					ss, ok = ms.fss.Find([]byte("foo"))
					require_True(t, ok)
				} else {
					t.Fatal("Store not supported")
				}
				return ss
			}
			var smp StoreMsg
			expectSeq := func(seq uint64) {
				t.Helper()
				sm, _, err := fs.LoadNextMsg("foo", false, 0, &smp)
				require_NoError(t, err)
				require_Equal(t, sm.seq, seq)
				sm, err = fs.LoadLastMsg("foo", &smp)
				require_NoError(t, err)
				require_Equal(t, sm.seq, seq)
			}

			// results in ss.Last, ss.First is marked lazy (when we hit ss.Msgs-1==1).
			fillMsgs(3)
			removeMsgs(2, 1)
			ss := getSubjectState()
			require_Equal(t, ss.Msgs, 1)
			require_Equal(t, ss.First, 3)
			require_Equal(t, ss.Last, 3)
			require_False(t, ss.firstNeedsUpdate)
			require_False(t, ss.lastNeedsUpdate)
			expectSeq(3)

			// ss.First is marked lazy first, then ss.Last is marked lazy (when we hit ss.Msgs-1==1).
			fillMsgs(2)
			removeMsgs(3, 5)
			ss = getSubjectState()
			require_Equal(t, ss.Msgs, 1)
			require_Equal(t, ss.First, 3)
			require_Equal(t, ss.Last, 5)
			require_True(t, ss.firstNeedsUpdate)
			require_True(t, ss.lastNeedsUpdate)
			expectSeq(4)

			// ss.Last is marked lazy first, then ss.First is marked lazy (when we hit ss.Msgs-1==1).
			fillMsgs(2)
			removeMsgs(7, 4)
			ss = getSubjectState()
			require_Equal(t, ss.Msgs, 1)
			require_Equal(t, ss.First, 4)
			require_Equal(t, ss.Last, 7)
			require_True(t, ss.firstNeedsUpdate)
			require_True(t, ss.lastNeedsUpdate)
			expectSeq(6)

			// ss.Msgs=1, results in ss.First, ss.Last is marked lazy (when we hit ss.Msgs-1==1).
			fillMsgs(2)
			removeMsgs(9, 8)
			ss = getSubjectState()
			require_Equal(t, ss.Msgs, 1)
			require_Equal(t, ss.First, 6)
			require_Equal(t, ss.Last, 6)
			require_False(t, ss.firstNeedsUpdate)
			require_False(t, ss.lastNeedsUpdate)
			expectSeq(6)
		},
	)
}

func TestStoreMaxMsgsPerUpdateBug(t *testing.T) {
	config := func() StreamConfig {
		return StreamConfig{Name: "TEST", Subjects: []string{"foo"}, MaxMsgsPer: 0}
	}
	testAllStoreAllPermutations(
		t, false, config(),
		func(t *testing.T, fs StreamStore) {
			for i := 0; i < 5; i++ {
				_, _, err := fs.StoreMsg("foo", nil, nil, 0)
				require_NoError(t, err)
			}

			ss := fs.State()
			require_Equal(t, ss.Msgs, 5)
			require_Equal(t, ss.FirstSeq, 1)
			require_Equal(t, ss.LastSeq, 5)

			// Update max messages per-subject from 0 (infinite) to 1.
			// Since the per-subject limit was not specified before, messages should be removed upon config update.
			cfg := config()
			if _, ok := fs.(*fileStore); ok {
				cfg.Storage = FileStorage
			} else {
				cfg.Storage = MemoryStorage
			}
			cfg.MaxMsgsPer = 1
			err := fs.UpdateConfig(&cfg)
			require_NoError(t, err)

			// Only one message should remain.
			ss = fs.State()
			require_Equal(t, ss.Msgs, 1)
			require_Equal(t, ss.FirstSeq, 5)
			require_Equal(t, ss.LastSeq, 5)

			// Update max messages per-subject from 0 (infinite) to an invalid value (< -1).
			cfg.MaxMsgsPer = -2
			err = fs.UpdateConfig(&cfg)
			require_NoError(t, err)
			require_Equal(t, cfg.MaxMsgsPer, -1)
		},
	)
}

func TestStoreMaxMsgsPerUpdateToOneRemoveNewest(t *testing.T) {
	config := func() StreamConfig {
		return StreamConfig{Name: "TEST", Subjects: []string{"foo.*"}, MaxMsgsPer: -1}
	}
	testAllStoreAllPermutations(
		t, false, config(),
		func(t *testing.T, fs StreamStore) {
			// Store the first copy of "foo.0".
			_, _, err := fs.StoreMsg("foo.0", nil, nil, 0)
			require_NoError(t, err)

			// Store filler subjects with enough data that the filestore rolls over into a
			// second message block, so both copies of "foo.0" live in different blocks.
			msg := make([]byte, 1024*1024)
			fillBlock := func(expectedBlocks int) {
				for i := 1; i <= 9; i++ {
					_, _, err := fs.StoreMsg(fmt.Sprintf("foo.%d", i), nil, msg, 0)
					require_NoError(t, err)
				}
				if f, ok := fs.(*fileStore); ok {
					require_True(t, f.numMsgBlocks() >= expectedBlocks)
				}
			}
			fillBlock(2)

			// Store a second copy of "foo.0".
			nseq, _, err := fs.StoreMsg("foo.0", nil, nil, 0)
			require_NoError(t, err)

			// And a third, in its own block, which we'll remove as well.
			fillBlock(3)
			lseq, _, err := fs.StoreMsg("foo.0", nil, nil, 0)
			require_NoError(t, err)
			removed, err := fs.RemoveMsg(lseq)
			require_NoError(t, err)
			require_True(t, removed)

			// Update max messages per-subject from -1 (unlimited) to 1.
			// This transition does not run per-subject limit enforcement, so both copies remain.
			cfg := config()
			if _, ok := fs.(*fileStore); ok {
				cfg.Storage = FileStorage
			} else {
				cfg.Storage = MemoryStorage
			}
			cfg.MaxMsgsPer = 1
			require_NoError(t, fs.UpdateConfig(&cfg))

			ss := fs.SubjectsState("foo.0")["foo.0"]
			require_Equal(t, ss.Msgs, 1)
			require_Equal(t, ss.First, nseq)
			require_Equal(t, ss.Last, nseq)

			var smv StoreMsg
			sm, err := fs.LoadLastMsg("foo.0", &smv)
			require_NoError(t, err)
			require_Equal(t, sm.seq, nseq)

			sm, _, err = fs.LoadNextMsg("foo.0", false, 0, &smv)
			require_NoError(t, err)
			require_Equal(t, sm.seq, nseq)

			// The older copy must also still be found after a restart.
			if f, ok := fs.(*fileStore); ok {
				fcfg := f.fcfg
				require_NoError(t, f.Stop())
				f, err = newFileStore(fcfg, cfg)
				require_NoError(t, err)
				defer f.Stop()

				sm, err = f.LoadLastMsg("foo.0", &smv)
				require_NoError(t, err)
				require_Equal(t, sm.seq, nseq)
			}
		},
	)
}

func TestStoreCompactCleansUpDmap(t *testing.T) {
	config := func() StreamConfig {
		return StreamConfig{Name: "TEST", Subjects: []string{"foo"}, MaxMsgsPer: 0}
	}
	for cseq := uint64(2); cseq <= 4; cseq++ {
		t.Run(fmt.Sprintf("Compact(%d)", cseq), func(t *testing.T) {
			testAllStoreAllPermutations(
				t, false, config(),
				func(t *testing.T, fs StreamStore) {
					dmapEntries := func() int {
						if fss, ok := fs.(*fileStore); ok {
							return fss.dmapEntries()
						} else if mss, ok := fs.(*memStore); ok {
							mss.mu.RLock()
							defer mss.mu.RUnlock()
							return mss.dmap.Size()
						} else {
							return 0
						}
					}

					// Publish messages, should have no interior deletes.
					for i := 0; i < 3; i++ {
						_, _, err := fs.StoreMsg("foo", nil, nil, 0)
						require_NoError(t, err)
					}
					require_Len(t, dmapEntries(), 0)

					// Removing one message in the middle should be an interior delete.
					_, err := fs.RemoveMsg(2)
					require_NoError(t, err)
					require_Len(t, dmapEntries(), 1)

					// Compacting must always clean up the interior delete.
					_, err = fs.Compact(cseq)
					require_NoError(t, err)
					require_Len(t, dmapEntries(), 0)

					// Validate first/last sequence.
					state := fs.State()
					fseq := uint64(3)
					if fseq < cseq {
						fseq = cseq
					}
					require_Equal(t, state.FirstSeq, fseq)
					require_Equal(t, state.LastSeq, 3)
				})
		})
	}
}

func TestStoreCompactFullyDeletedRange(t *testing.T) {
	config := func() StreamConfig {
		return StreamConfig{Name: "TEST", Subjects: []string{"foo"}, MaxMsgsPer: 0}
	}
	testAllStoreAllPermutations(
		t, false, config(),
		func(t *testing.T, fs StreamStore) {
			// Publish some messages.
			for range 3 {
				_, _, err := fs.StoreMsg("foo", nil, nil, 0)
				require_NoError(t, err)
			}

			// Remove all messages in the range we're about to compact,
			// keeping the first message so FirstSeq stays at 1.
			_, err := fs.RemoveMsg(2)
			require_NoError(t, err)
			_, err = fs.RemoveMsg(3)
			require_NoError(t, err)

			state := fs.State()
			require_Equal(t, state.FirstSeq, 1)
			require_Equal(t, state.LastSeq, 3)

			// Compact to a deleted sequence, and the stream is left empty.
			_, err = fs.Compact(2)
			require_NoError(t, err)
			state = fs.State()
			require_Equal(t, state.Msgs, 0)
			require_Equal(t, state.LastSeq, 3)
			require_Equal(t, state.FirstSeq, state.LastSeq+1)
			require_True(t, state.FirstTime.IsZero())
		})
}

func TestStoreTruncateCleansUpDmap(t *testing.T) {
	config := func() StreamConfig {
		return StreamConfig{Name: "TEST", Subjects: []string{"foo"}, MaxMsgsPer: 0}
	}
	for tseq := uint64(0); tseq <= 1; tseq++ {
		t.Run(fmt.Sprintf("Truncate(%d)", tseq), func(t *testing.T) {
			testAllStoreAllPermutations(
				t, false, config(),
				func(t *testing.T, fs StreamStore) {
					dmapEntries := func() int {
						if fss, ok := fs.(*fileStore); ok {
							return fss.dmapEntries()
						} else if mss, ok := fs.(*memStore); ok {
							mss.mu.RLock()
							defer mss.mu.RUnlock()
							return mss.dmap.Size()
						} else {
							return 0
						}
					}

					// Publish messages, should have no interior deletes.
					for i := 0; i < 3; i++ {
						_, _, err := fs.StoreMsg("foo", nil, nil, 0)
						require_NoError(t, err)
					}
					require_Len(t, dmapEntries(), 0)

					// Removing one message in the middle should be an interior delete.
					_, err := fs.RemoveMsg(2)
					require_NoError(t, err)
					require_Len(t, dmapEntries(), 1)

					// Truncating must always clean up the interior delete.
					err = fs.Truncate(tseq)
					require_NoError(t, err)
					require_Len(t, dmapEntries(), 0)

					// Validate first/last sequence.
					state := fs.State()
					fseq := uint64(1)
					if fseq > tseq {
						fseq = tseq
					}
					require_Equal(t, state.FirstSeq, fseq)
					require_Equal(t, state.LastSeq, tseq)
				})
		})
	}
}

// https://github.com/nats-io/nats-server/issues/6709
func TestStorePurgeExZero(t *testing.T) {
	testAllStoreAllPermutations(
		t, false,
		StreamConfig{Name: "TEST", Subjects: []string{"foo"}},
		func(t *testing.T, fs StreamStore) {
			// Simple purge all.
			_, err := fs.Purge()
			require_NoError(t, err)
			ss := fs.State()
			require_Equal(t, ss.FirstSeq, 1)
			require_Equal(t, ss.LastSeq, 0)

			// PurgeEx(seq=0) must be equal.
			_, err = fs.PurgeEx(_EMPTY_, 0, 0)
			require_NoError(t, err)
			ss = fs.State()
			require_Equal(t, ss.FirstSeq, 1)
			require_Equal(t, ss.LastSeq, 0)
		},
	)
}

func TestStorePurgeExSequenceOne(t *testing.T) {
	testAllStoreAllPermutations(
		t, true,
		StreamConfig{Name: "TEST", Subjects: []string{"foo", "bar"}},
		func(t *testing.T, fs StreamStore) {
			for range 5 {
				_, _, err := fs.StoreMsg("foo", nil, nil, 0)
				require_NoError(t, err)
				_, _, err = fs.StoreMsg("bar", nil, nil, 0)
				require_NoError(t, err)
			}
			before := fs.State()
			check := func(t *testing.T, subject string, keep uint64) {
				n, err := fs.PurgeEx(subject, 1, keep)
				require_NoError(t, err)
				require_Equal(t, n, 0)
				after := fs.State()
				require_Equal(t, after.Msgs, before.Msgs)
				require_Equal(t, after.FirstSeq, before.FirstSeq)
				require_Equal(t, after.LastSeq, before.LastSeq)
			}
			t.Run("empty-subject", func(t *testing.T) { check(t, _EMPTY_, 0) })
			t.Run("wildcard-subject", func(t *testing.T) { check(t, fwcs, 0) })
			t.Run("specific-subject", func(t *testing.T) { check(t, "foo", 0) })
			t.Run("empty-subject-with-keep", func(t *testing.T) { check(t, _EMPTY_, 3) })
			t.Run("specific-subject-with-keep", func(t *testing.T) { check(t, "foo", 3) })
		},
	)
}

func TestStorePurgeExKeepWithInteriorDeletes(t *testing.T) {
	testAllStoreAllPermutations(
		t, false,
		StreamConfig{Name: "TEST", Subjects: []string{"foo"}},
		func(t *testing.T, fs StreamStore) {
			for range 50 {
				_, _, err := fs.StoreMsg("foo", nil, nil, 0)
				require_NoError(t, err)
			}
			// Remove every other message to create interior gaps.
			for seq := uint64(2); seq <= 50; seq += 2 {
				_, err := fs.RemoveMsg(seq)
				require_NoError(t, err)
			}
			ss := fs.State()
			require_Equal(t, ss.Msgs, 25)
			require_Equal(t, ss.FirstSeq, 1)
			require_Equal(t, ss.LastSeq, 50)

			// Keep the 5 newest. Newest 5 existing seqs are 41, 43, 45, 47, 49.
			n, err := fs.PurgeEx(_EMPTY_, 0, 5)
			require_NoError(t, err)
			require_Equal(t, n, 20)

			ss = fs.State()
			require_Equal(t, ss.Msgs, 5)
			require_Equal(t, ss.FirstSeq, 41)
			require_Equal(t, ss.LastSeq, 50)
		},
	)
}

func TestStoreUpdateConfigTTLState(t *testing.T) {
	config := func() StreamConfig {
		return StreamConfig{Name: "TEST", Subjects: []string{"foo"}}
	}
	testAllStoreAllPermutations(
		t, false, config(),
		func(t *testing.T, fs StreamStore) {
			cfg := config()
			switch fs.(type) {
			case *fileStore:
				cfg.Storage = FileStorage
			case *memStore:
				cfg.Storage = MemoryStorage
			}

			// TTLs disabled at this point so this message should survive.
			seq, _, err := fs.StoreMsg("foo", nil, nil, 1)
			require_NoError(t, err)
			time.Sleep(2 * time.Second)
			_, err = fs.LoadMsg(seq, nil)
			require_NoError(t, err)

			// Now enable TTLs.
			cfg.AllowMsgTTL = true
			require_NoError(t, fs.UpdateConfig(&cfg))

			// TTLs enabled at this point so this message should be cleaned up.
			seq, _, err = fs.StoreMsg("foo", nil, nil, 1)
			require_NoError(t, err)
			time.Sleep(2 * time.Second)
			_, err = fs.LoadMsg(seq, nil)
			require_Error(t, err)

			// Now disable TTLs again.
			cfg.AllowMsgTTL = false
			require_NoError(t, fs.UpdateConfig(&cfg))

			// TTLs disabled again so this message should survive.
			seq, _, err = fs.StoreMsg("foo", nil, nil, 1)
			require_NoError(t, err)
			time.Sleep(2 * time.Second)
			_, err = fs.LoadMsg(seq, nil)
			require_NoError(t, err)
		},
	)
}

func TestStoreStreamInteriorDeleteAccounting(t *testing.T) {
	tests := []struct {
		title  string
		action func(s StreamStore, lseq uint64)
	}{
		{
			title: "TruncateWithRemove",
			action: func(s StreamStore, lseq uint64) {
				seq, _, err := s.StoreMsg("foo", nil, nil, 0)
				require_NoError(t, err)
				require_Equal(t, seq, lseq)
				removed, err := s.RemoveMsg(lseq)
				require_NoError(t, err)
				require_True(t, removed)
				require_NoError(t, s.Truncate(lseq))
			},
		},
		{
			title: "TruncateWithErase",
			action: func(s StreamStore, lseq uint64) {
				seq, _, err := s.StoreMsg("foo", nil, nil, 0)
				require_NoError(t, err)
				require_Equal(t, seq, lseq)
				removed, err := s.EraseMsg(lseq)
				require_NoError(t, err)
				require_True(t, removed)
				require_NoError(t, s.Truncate(lseq))
			},
		},
		{
			title: "TruncateWithTombstone",
			action: func(s StreamStore, lseq uint64) {
				seq, _, err := s.StoreMsg("foo", nil, nil, 0)
				require_NoError(t, err)
				require_Equal(t, seq, lseq)
				if fs, ok := s.(*fileStore); ok {
					removed, err := fs.removeMsg(lseq, false, false, true)
					require_NoError(t, err)
					require_True(t, removed)
				} else {
					removed, err := s.RemoveMsg(lseq)
					require_NoError(t, err)
					require_True(t, removed)
				}
				require_NoError(t, s.Truncate(lseq))
			},
		},
		{
			title: "SkipMsg",
			action: func(s StreamStore, lseq uint64) {
				s.SkipMsg(0)
			},
		},
		{
			title: "SkipMsgs",
			action: func(s StreamStore, lseq uint64) {
				require_NoError(t, s.SkipMsgs(lseq, 1))
			},
		},
	}
	for _, empty := range []bool{false, true} {
		for _, test := range tests {
			t.Run(fmt.Sprintf("Empty=%v/%s", empty, test.title), func(t *testing.T) {
				cfg := StreamConfig{Name: "zzz", Subjects: []string{"foo"}}
				testAllStoreAllPermutations(t, true, cfg, func(t *testing.T, s StreamStore) {
					var err error
					var lseq uint64
					if !empty {
						lseq, _, err = s.StoreMsg("foo", nil, nil, 0)
						require_NoError(t, err)
						require_Equal(t, lseq, 1)
					}
					lseq++

					test.action(s, lseq)

					// Confirm state as baseline.
					before := s.State()
					if empty {
						require_Equal(t, before.Msgs, 0)
						require_Equal(t, before.FirstSeq, 2)
						require_Equal(t, before.LastSeq, 1)
					} else {
						require_Equal(t, before.Msgs, 1)
						require_Equal(t, before.FirstSeq, 1)
						require_Equal(t, before.LastSeq, 2)
					}

					var fs *fileStore
					var ok bool
					if fs, ok = s.(*fileStore); !ok {
						return
					}
					cfg.Storage = FileStorage
					fcfg := fs.fcfg
					created := time.Time{}

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
}

func TestStoreMsgLoadPrevMsgMulti(t *testing.T) {
	testAllStoreAllPermutations(
		t, false,
		StreamConfig{Name: "zzz", Subjects: []string{"foo.*"}},
		func(t *testing.T, fs StreamStore) {
			// Put 1k msgs in
			for i := range 1000 {
				subj := fmt.Sprintf("foo.%d", i+1)
				fs.StoreMsg(subj, nil, []byte("ZZZ"), 0)
			}

			var sm StoreMsg
			var count int
			var state StreamState
			fs.FastState(&state)

			sl := gsl.NewSimpleSublist()
			sl.Insert("foo.5", struct{}{})
			sl.Insert("foo.15", struct{}{})
			sl.Insert("foo.105", struct{}{})

			for seq := state.LastSeq; seq > 5; seq-- {
				var err error
				_, seq, err = fs.LoadPrevMsgMulti(sl, seq, &sm)
				require_NoError(t, err)
				require_Equal(t, sm.subj, fmt.Sprintf("foo.%d", sm.seq))
				count++
			}

			_, _, err := fs.LoadPrevMsgMulti(sl, 4, &sm)
			require_Error(t, err, ErrStoreEOF)
			require_Equal(t, count, 3)
		},
	)
}

func TestStoreMsgLoadPrevMsg(t *testing.T) {
	testAllStoreAllPermutations(
		t, false,
		StreamConfig{Name: "zzz", Subjects: []string{"foo.*", "bar.*"}},
		func(t *testing.T, fs StreamStore) {
			for _, subj := range []string{"foo.1", "bar.1", "foo.2", "bar.2", "foo.3"} {
				_, _, err := fs.StoreMsg(subj, nil, []byte("ZZZ"), 0)
				require_NoError(t, err)
			}

			var sm StoreMsg

			smp, seq, err := fs.LoadPrevMsg(_EMPTY_, false, 5, &sm)
			require_NoError(t, err)
			require_Equal(t, smp.subj, "foo.3")
			require_Equal(t, seq, uint64(5))

			smp, seq, err = fs.LoadPrevMsg("foo.2", false, 5, &sm)
			require_NoError(t, err)
			require_Equal(t, smp.subj, "foo.2")
			require_Equal(t, seq, uint64(3))

			smp, seq, err = fs.LoadPrevMsg("foo.*", true, 5, &sm)
			require_NoError(t, err)
			require_Equal(t, smp.subj, "foo.3")
			require_Equal(t, seq, uint64(5))

			_, seq, err = fs.LoadPrevMsg("baz.*", true, 5, &sm)
			require_Error(t, err, ErrStoreEOF)
			require_Equal(t, seq, uint64(1))
		},
	)
}

func TestStoreMsgLoadPrevMsgMultiFullWildcardSkip(t *testing.T) {
	testAllStoreAllPermutations(
		t, false,
		StreamConfig{Name: "zzz", Subjects: []string{"foo.*"}},
		func(t *testing.T, fs StreamStore) {
			for i := range 10 {
				subj := fmt.Sprintf("foo.%d", i+1)
				_, _, err := fs.StoreMsg(subj, nil, []byte("ZZZ"), 0)
				require_NoError(t, err)
			}

			var sm StoreMsg
			var state StreamState
			fs.FastState(&state)

			sl := gsl.NewSimpleSublist()
			require_NoError(t, sl.Insert(">", struct{}{}))

			var got []uint64
			for seq := state.LastSeq; ; {
				smp, nseq, err := fs.LoadPrevMsgMulti(sl, seq, &sm)
				if err == ErrStoreEOF {
					require_Equal(t, nseq, state.FirstSeq)
					break
				}
				require_NoError(t, err)
				require_Equal(t, smp.seq, nseq)
				got = append(got, nseq)
				if nseq == state.FirstSeq {
					_, nseq, err = fs.LoadPrevMsgMulti(sl, nseq-1, &sm)
					require_Error(t, err, ErrStoreEOF)
					require_Equal(t, nseq, state.FirstSeq)
					break
				}
				seq = nseq - 1
			}

			require_True(t, slices.Equal(got, []uint64{10, 9, 8, 7, 6, 5, 4, 3, 2, 1}))
		},
	)
}

func TestStoreDiscardNew(t *testing.T) {
	test := func(t *testing.T, updateConfig func(cfg *StreamConfig), expectedErr error) {
		cfg := StreamConfig{Name: "zzz", Subjects: []string{"foo"}, Discard: DiscardNew}
		updateConfig(&cfg)
		testAllStoreAllPermutations(t, false, cfg, func(t *testing.T, fs StreamStore) {
			ts := time.Now().UnixNano()
			expectedSeq := uint64(1)
			requireState := func() {
				t.Helper()
				state := fs.State()
				require_Equal(t, state.Msgs, 1)
				require_Equal(t, state.FirstSeq, expectedSeq)
				require_Equal(t, state.LastSeq, expectedSeq)
			}

			_, _, err := fs.StoreMsg("foo", nil, nil, 0)
			require_NoError(t, err)

			err = fs.StoreRawMsg("foo", nil, nil, 0, ts, 0, true)
			if expectedErr == nil {
				require_NoError(t, err)
				expectedSeq++
			} else {
				require_Equal(t, err, expectedErr)
			}
			requireState()

			// For a clustered stream DiscardNew should only be enforced by the stream leader.
			// Followers MUST always accept data that they've received from the leader,
			// otherwise we risk stream desync if some servers decide to reject.
			err = fs.StoreRawMsg("foo", nil, nil, 0, ts, 0, false)
			require_NoError(t, err)
			expectedSeq++

			// Since DiscardNew we must only add to the stream, and not act like
			// DiscardOld based on MaxMsgs/MaxBytes limits, unless MaxMsgsPer is set.
			if cfg.MaxMsgsPer > 0 {
				requireState()
			} else {
				state := fs.State()
				require_Equal(t, state.Msgs, 2)
				require_Equal(t, state.FirstSeq, expectedSeq-1)
				require_Equal(t, state.LastSeq, expectedSeq)
			}
		})
	}

	t.Run("MaxMsgs", func(t *testing.T) { test(t, func(cfg *StreamConfig) { cfg.MaxMsgs = 1 }, ErrMaxMsgs) })
	t.Run("MaxBytes", func(t *testing.T) { test(t, func(cfg *StreamConfig) { cfg.MaxBytes = 33 }, ErrMaxBytes) })
	t.Run("MaxMsgsPer", func(t *testing.T) { test(t, func(cfg *StreamConfig) { cfg.MaxMsgsPer = 1 }, nil) })
	t.Run("MaxMsgsPer_DiscardNewPer", func(t *testing.T) {
		test(t, func(cfg *StreamConfig) {
			cfg.DiscardNewPer = true
			cfg.MaxMsgsPer = 1
		}, ErrMaxMsgsPerSubject)
	})
	t.Run("MaxMsgsPer_MaxMsgs", func(t *testing.T) {
		test(t, func(cfg *StreamConfig) {
			// Without DiscardNewPerSubject we can replace the message with the new one if it fits (it will for this test).
			cfg.MaxMsgs = 1
			cfg.MaxMsgsPer = 1
		}, nil)
	})
	t.Run("MaxMsgsPer_MaxBytes", func(t *testing.T) {
		test(t, func(cfg *StreamConfig) {
			// Without DiscardNewPerSubject we can replace the message with the new one if it fits (it will for this test).
			cfg.MaxBytes = 33
			cfg.MaxMsgsPer = 1
		}, nil)
	})
	t.Run("MaxMsgsPer_MaxMsgs_DiscardNewPer", func(t *testing.T) {
		test(t, func(cfg *StreamConfig) {
			cfg.DiscardNewPer = true
			cfg.MaxMsgs = 1
			cfg.MaxMsgsPer = 1
		}, ErrMaxMsgsPerSubject)
	})
	t.Run("MaxMsgsPer_MaxBytes_DiscardNewPer", func(t *testing.T) {
		test(t, func(cfg *StreamConfig) {
			cfg.DiscardNewPer = true
			cfg.MaxBytes = 33
			cfg.MaxMsgsPer = 1
		}, ErrMaxMsgsPerSubject)
	})
}

func TestStoreGetSeqFromTimeWithInteriorDeletesGap(t *testing.T) {
	testAllStoreAllPermutations(
		t, false,
		StreamConfig{Name: "zzz", Subjects: []string{"foo"}},
		func(t *testing.T, fs StreamStore) {
			var start int64
			for i := range 10 {
				_, ts, err := fs.StoreMsg("foo", nil, nil, 0)
				require_NoError(t, err)
				if i == 1 {
					start = ts
				}
			}
			// Create a delete gap to prove a simple binary search between sequences
			// does not work, and deletes need to be accounted for. A simple binary search
			// will hit the deleted sequences and then return the last sequence.
			for seq := uint64(4); seq <= 7; seq++ {
				_, err := fs.RemoveMsg(seq)
				require_NoError(t, err)
			}
			ts := time.Unix(0, start).UTC()
			require_Equal(t, fs.GetSeqFromTime(ts), 2)
		},
	)
}

func TestStoreGetSeqFromTimeWithTrailingDeletes(t *testing.T) {
	testAllStoreAllPermutations(
		t, false,
		StreamConfig{Name: "zzz", Subjects: []string{"foo"}},
		func(t *testing.T, fs StreamStore) {
			var start int64
			for i := range 3 {
				_, ts, err := fs.StoreMsg("foo", nil, nil, 0)
				require_NoError(t, err)
				if i == 1 {
					start = ts
				}
			}
			_, err := fs.RemoveMsg(3)
			require_NoError(t, err)
			ts := time.Unix(0, start).UTC()
			require_Equal(t, fs.GetSeqFromTime(ts), 2)
		},
	)
}

func TestStoreSkipMsgsCarryForwardLastTime(t *testing.T) {
	testAllStoreAllPermutations(
		t, false,
		StreamConfig{Name: "zzz", Subjects: []string{"foo"}},
		func(t *testing.T, fs StreamStore) {
			// Store a message with an old timestamp.
			old := time.Now().Add(-30 * 24 * time.Hour).UTC().Truncate(time.Second)
			err := fs.StoreRawMsg("foo", nil, nil, 1, old.UnixNano(), 0, false)
			require_NoError(t, err)

			var ss StreamState
			fs.FastState(&ss)
			require_Equal(t, ss.LastSeq, 1)
			require_Equal(t, ss.LastTime.UnixNano(), old.UnixNano())

			// SkipMsgs should not move the last time forward.
			require_NoError(t, fs.SkipMsgs(2, 10))
			fs.FastState(&ss)
			require_Equal(t, ss.LastSeq, 11)
			require_Equal(t, ss.LastTime.UnixNano(), old.UnixNano())

			// SkipMsg should not move the last time forward.
			seq, err := fs.SkipMsg(12)
			require_NoError(t, err)
			require_Equal(t, seq, 12)
			fs.FastState(&ss)
			require_Equal(t, ss.LastSeq, 12)
			require_Equal(t, ss.LastTime.UnixNano(), old.UnixNano())

			// Another chained skip should not move the last time forward.
			require_NoError(t, fs.SkipMsgs(13, 5))
			fs.FastState(&ss)
			require_Equal(t, ss.LastSeq, 17)
			require_Equal(t, ss.LastTime.UnixNano(), old.UnixNano())

			// Time-based lookups must resolve across the skip gaps.
			newer := old.Add(15 * 24 * time.Hour)
			err = fs.StoreRawMsg("foo", nil, nil, 18, newer.UnixNano(), 0, false)
			require_NoError(t, err)
			require_Equal(t, fs.GetSeqFromTime(old), 1)
			require_Equal(t, fs.GetSeqFromTime(old.Add(5*24*time.Hour)), 18)
			require_Equal(t, fs.GetSeqFromTime(newer), 18)
		},
	)
}

func TestStoreSkipMsgsCarryForwardLastTimeFromEmpty(t *testing.T) {
	t.Run("SkipMsg", func(t *testing.T) {
		testAllStoreAllPermutations(
			t, false,
			StreamConfig{Name: "zzz", Subjects: []string{"foo"}},
			func(t *testing.T, fs StreamStore) {
				var ss StreamState
				fs.FastState(&ss)
				require_Equal(t, ss.LastSeq, uint64(0))
				require_True(t, ss.LastTime.IsZero())

				// Zero state, so falls back to ats.AccessTime().
				before := ats.AccessTime()
				seq, err := fs.SkipMsg(0)
				require_NoError(t, err)
				require_Equal(t, seq, uint64(1))

				fs.FastState(&ss)
				require_Equal(t, ss.LastSeq, uint64(1))
				require_False(t, ss.LastTime.IsZero())
				require_True(t, ss.LastTime.UnixNano() >= before)
				require_True(t, ss.LastTime.UnixNano() <= ats.AccessTime())
				first := ss.LastTime

				// Advance the clock; a subsequent skip must preserve the time.
				time.Sleep(2 * ats.TickInterval)

				seq, err = fs.SkipMsg(0)
				require_NoError(t, err)
				require_Equal(t, seq, uint64(2))

				fs.FastState(&ss)
				require_Equal(t, ss.LastSeq, uint64(2))
				require_Equal(t, ss.LastTime.UnixNano(), first.UnixNano())
			},
		)
	})

	t.Run("SkipMsgs", func(t *testing.T) {
		testAllStoreAllPermutations(
			t, false,
			StreamConfig{Name: "zzz", Subjects: []string{"foo"}},
			func(t *testing.T, fs StreamStore) {
				var ss StreamState
				fs.FastState(&ss)
				require_Equal(t, ss.LastSeq, uint64(0))
				require_True(t, ss.LastTime.IsZero())

				// Zero state, so falls back to ats.AccessTime().
				before := ats.AccessTime()
				require_NoError(t, fs.SkipMsgs(1, 5))

				fs.FastState(&ss)
				require_Equal(t, ss.LastSeq, uint64(5))
				require_False(t, ss.LastTime.IsZero())
				require_True(t, ss.LastTime.UnixNano() >= before)
				require_True(t, ss.LastTime.UnixNano() <= ats.AccessTime())
				first := ss.LastTime

				// Advance the clock; a subsequent skip must preserve the time.
				time.Sleep(2 * ats.TickInterval)

				seq, err := fs.SkipMsg(0)
				require_NoError(t, err)
				require_Equal(t, seq, uint64(6))

				fs.FastState(&ss)
				require_Equal(t, ss.LastSeq, uint64(6))
				require_Equal(t, ss.LastTime.UnixNano(), first.UnixNano())
			},
		)
	})
}

func TestStoreSkipMsgNoInterestAdvancesLastTime(t *testing.T) {
	testAllStoreAllPermutations(
		t, false,
		StreamConfig{Name: "zzz", Subjects: []string{"foo"}},
		func(t *testing.T, fs StreamStore) {
			// A skip without interest still increases the sequence.
			seq, err := fs.SkipMsgNoInterest(0)
			require_NoError(t, err)
			require_Equal(t, seq, uint64(1))

			var ss StreamState
			fs.FastState(&ss)
			require_Equal(t, ss.Msgs, uint64(0))
			require_Equal(t, ss.LastSeq, uint64(1))
			first := ss.LastTime

			// Let the clock advance past the access time service granularity.
			time.Sleep(2 * ats.TickInterval)

			// The second skip still has no interest, but should update both the sequence and time.
			seq, err = fs.SkipMsgNoInterest(0)
			require_NoError(t, err)
			require_Equal(t, seq, uint64(2))

			fs.FastState(&ss)
			require_Equal(t, ss.Msgs, uint64(0))
			require_Equal(t, ss.LastSeq, uint64(2))
			require_True(t, ss.LastTime.After(first))
		},
	)
}

func TestFileStoreMultiLastSeqsAndLoadLastMsgWithLazySubjectState(t *testing.T) {
	testAllStoreAllPermutations(
		t, false,
		StreamConfig{Name: "zzz", Subjects: []string{"foo"}},
		func(t *testing.T, fs StreamStore) {
			for range 3 {
				_, _, err := fs.StoreMsg("foo", nil, nil, 0)
				require_NoError(t, err)
			}
			seqs, err := fs.MultiLastSeqs([]string{"foo"}, 0, 0)
			require_NoError(t, err)
			require_Equal(t, len(seqs), 1)
			require_Equal(t, seqs[0], 3)

			_, err = fs.RemoveMsg(3)
			require_NoError(t, err)
			seqs, err = fs.MultiLastSeqs([]string{"foo"}, 0, 0)
			require_NoError(t, err)
			require_Equal(t, len(seqs), 1)
			require_Equal(t, seqs[0], 2)

			_, _, err = fs.StoreMsg("foo", nil, nil, 0)
			require_NoError(t, err)
			sm, err := fs.LoadLastMsg("foo", nil)
			require_NoError(t, err)
			require_Equal(t, sm.seq, 4)

			_, err = fs.RemoveMsg(4)
			require_NoError(t, err)
			sm, err = fs.LoadLastMsg("foo", nil)
			require_NoError(t, err)
			require_Equal(t, sm.seq, 2)
		},
	)
}

func TestStoreMultiLastMsgs(t *testing.T) {
	testAllStoreAllPermutations(
		t, false,
		StreamConfig{Name: "zzz", Subjects: []string{"foo.*"}},
		func(t *testing.T, fs StreamStore) {
			// Three rounds over ten subjects, so every subject has multiple
			// revisions and the last per subject sits in the final round.
			// Seqs 1-30, last for foo.<i> is 21+i.
			for range 3 {
				for i := range 10 {
					_, _, err := fs.StoreMsg(fmt.Sprintf("foo.%d", i), nil, nil, 0)
					require_NoError(t, err)
				}
			}

			type delivery struct {
				seq uint64
				np  uint64
			}
			load := func(filters []string, minSeq, maxSeq uint64, maxAllowed, stopAfter int) ([]delivery, uint64, uint64, error) {
				var msgs []delivery
				total, np, err := fs.MultiLastMsgs(filters, minSeq, maxSeq, maxAllowed, func(sm *StoreMsg, np uint64) bool {
					msgs = append(msgs, delivery{sm.seq, np})
					return stopAfter == 0 || len(msgs) < stopAfter
				})
				return msgs, total, np, err
			}

			// Whole stream delivers the last message per subject in ascending
			// sequence order, with np counting down the remaining messages.
			msgs, total, np, err := load([]string{"foo.*"}, 0, 0, -1, 0)
			require_NoError(t, err)
			require_Equal(t, total, 10)
			require_Equal(t, np, 0)
			require_Len(t, len(msgs), 10)
			for i, d := range msgs {
				require_Equal(t, d.seq, uint64(21+i))
				require_Equal(t, d.np, uint64(9-i))
			}

			// minSeq skips lower sequences but still accounts for them in np.
			msgs, total, np, err = load([]string{"foo.*"}, 26, 0, -1, 0)
			require_NoError(t, err)
			require_Equal(t, total, 10)
			require_Equal(t, np, 0)
			require_Len(t, len(msgs), 5)
			for i, d := range msgs {
				require_Equal(t, d.seq, uint64(26+i))
				require_Equal(t, d.np, uint64(4-i))
			}

			// maxSeq resolves the last message per subject at or below it,
			// stepping back to an earlier round where needed.
			msgs, total, np, err = load([]string{"foo.*"}, 0, 25, -1, 0)
			require_NoError(t, err)
			require_Equal(t, total, 10)
			require_Equal(t, np, 0)
			require_Len(t, len(msgs), 10)
			for i, d := range msgs {
				require_Equal(t, d.seq, uint64(16+i))
			}

			// Stopping the callback early keeps np at the remaining count.
			msgs, total, np, err = load([]string{"foo.*"}, 0, 0, -1, 3)
			require_NoError(t, err)
			require_Equal(t, total, 10)
			require_Equal(t, np, 7)
			require_Len(t, len(msgs), 3)

			// Exceeding maxAllowed errors without delivering anything.
			msgs, _, _, err = load([]string{"foo.*"}, 0, 0, 5, 0)
			require_Error(t, err, ErrTooManyResults)
			require_Len(t, len(msgs), 0)

			// No matches.
			msgs, total, _, err = load([]string{"bar.>"}, 0, 0, -1, 0)
			require_NoError(t, err)
			require_Equal(t, total, 0)
			require_Len(t, len(msgs), 0)
		},
	)
}

func TestStoreLoadLastSeq(t *testing.T) {
	test := func(t *testing.T, fs StreamStore) {
		for _, subj := range []string{"foo.a", "foo.b", "foo.c", "foo.a", "foo.b", "bar", "foo.c", "foo.a", "foo.b"} {
			_, _, err := fs.StoreMsg(subj, nil, []byte("x"), 0)
			require_NoError(t, err)
		}
		if fs, ok := fs.(*fileStore); ok {
			require_True(t, fs.numMsgBlocks() > 1)
		}
		// LoadLastSeq must agree with LoadLastMsg.
		check := func(subj string, expected uint64) {
			t.Helper()
			var smv StoreMsg
			sm, merr := fs.LoadLastMsg(subj, &smv)
			seq, err := fs.LoadLastSeq(subj)
			if sm == nil {
				require_Error(t, merr, ErrStoreMsgNotFound)
				require_Error(t, err, ErrStoreMsgNotFound)
			} else {
				require_NoError(t, err)
				require_Equal(t, seq, sm.seq)
			}
			require_Equal(t, seq, expected)
		}
		check("foo.a", 8)
		check("foo.b", 9)
		check("foo.c", 7)
		check("bar", 6)
		check("foo.*", 9)
		check(_EMPTY_, 9)
		check(fwcs, 9)
		check("baz", 0)

		// Removing a subject's last message out of order moves it back to the previous one.
		_, err := fs.RemoveMsg(8)
		require_NoError(t, err)
		check("foo.a", 4)

		// The stream's last sequence stays after its message is removed, but has no message.
		_, err = fs.RemoveMsg(9)
		require_NoError(t, err)
		check("foo.b", 5)
		check("foo.*", 7)
		check(_EMPTY_, 0)
		check(fwcs, 0)

		_, err = fs.RemoveMsg(4)
		require_NoError(t, err)
		check("foo.a", 1)
		_, err = fs.RemoveMsg(1)
		require_NoError(t, err)
		check("foo.a", 0)

		_, err = fs.Purge()
		require_NoError(t, err)
		check("foo.b", 0)
		check(fwcs, 0)
	}

	cfg := StreamConfig{Name: "zzz", Subjects: []string{"foo.*", "bar"}}
	t.Run("Memory", func(t *testing.T) {
		cfg.Storage = MemoryStorage
		ms, err := newMemStore(&cfg)
		require_NoError(t, err)
		defer ms.Stop()
		test(t, ms)
	})
	t.Run("File", func(t *testing.T) {
		cfg.Storage = FileStorage
		testFileStoreAllPermutations(t, func(t *testing.T, fcfg FileStoreConfig) {
			// Small blocks, so stale last block indexes make the lookup walk back across blocks.
			fcfg.BlockSize = 128
			fs, err := newFileStore(fcfg, cfg)
			require_NoError(t, err)
			defer fs.Stop()
			test(t, fs)
		})
	})
}

func TestStoreNumPendingLastPerSubjectExcludeOvercount(t *testing.T) {
	testAllStoreAllPermutations(
		t, false,
		StreamConfig{Name: "zzz", Subjects: []string{"foo.*"}},
		func(t *testing.T, fs StreamStore) {
			put := func(subj string) {
				_, _, err := fs.StoreMsg(subj, nil, []byte("x"), 0)
				require_NoError(t, err)
			}
			// foo.A occupies seqs 1-10 (entirely below our start seq).
			for range 10 {
				put("foo.A")
			}
			put("foo.B")                              // seq 11
			put("foo.C")                              // seq 12
			require_NoError(t, fs.SkipMsgs(13, 87))   // skip 13-99
			put("foo.B")                              // seq 100
			require_NoError(t, fs.SkipMsgs(101, 899)) // skip 101-999
			put("foo.C")                              // seq 1000

			// lastPerSubject starting at seq 13: foo.A's last (10) is below 13 so it
			// contributes 0; foo.B (last 100) and foo.C (last 1000) each contribute 1.
			total, _, err := fs.NumPending(13, "foo.*", true)
			require_NoError(t, err)
			require_Equal(t, total, 2)

			filters := gsl.NewSublist[struct{}]()
			filters.Insert("foo.*", struct{}{})
			total, _, err = fs.NumPendingMulti(13, filters, true)
			require_NoError(t, err)
			require_Equal(t, total, 2)
		},
	)
}

// Resolving last sequences for many subjects whose last messages are spread
// over the whole stream. For the filestore a small block size spreads them
// across many blocks: WholeStream can resolve via each subject's last block,
// Bounded must walk the blocks backwards from maxSeq.
func Benchmark_StoreMultiLastSeqsManyBlocks(b *testing.B) {
	const (
		numSubjects = 1_000
		msgsPerSubj = 100
	)

	run := func(b *testing.B, fs StreamStore) {
		// Store each subject's messages contiguously so the per-subject last
		// messages land spread out over the whole stream.
		msg := []byte("ok")
		for i := range numSubjects {
			subj := fmt.Sprintf("foo.%d", i)
			for range msgsPerSubj {
				_, _, err := fs.StoreMsg(subj, nil, msg, 0)
				require_NoError(b, err)
			}
		}

		// A sparse subset of subjects spread over the whole stream. Resolving
		// via each subject's last block only visits these few blocks, while
		// the backwards walk must scan nearly every block to find them all.
		var sparse []string
		for i := 0; i < numSubjects; i += numSubjects / 10 {
			sparse = append(sparse, fmt.Sprintf("foo.%d", i))
		}

		for _, bc := range []struct {
			name    string
			filters []string
			matches int
			maxSeq  uint64
		}{
			{"AllSubjects/WholeStream", []string{">"}, numSubjects, 0},
			{"AllSubjects/Bounded", []string{">"}, numSubjects, numSubjects*msgsPerSubj - 1},
			{"SparseSubjects/WholeStream", sparse, len(sparse), 0},
			{"SparseSubjects/Bounded", sparse, len(sparse), numSubjects*msgsPerSubj - 1},
		} {
			b.Run(bc.name, func(b *testing.B) {
				for range b.N {
					seqs, err := fs.MultiLastSeqs(bc.filters, bc.maxSeq, -1)
					require_NoError(b, err)
					require_Len(b, len(seqs), bc.matches)
				}
			})
		}
	}

	cfg := StreamConfig{Name: "zzz", Subjects: []string{"foo.*", "bar.*"}}

	b.Run("Memory", func(b *testing.B) {
		cfg := cfg
		cfg.Storage = MemoryStorage
		ms, err := newMemStore(&cfg)
		require_NoError(b, err)
		defer ms.Stop()
		run(b, ms)
	})
	b.Run("File", func(b *testing.B) {
		cfg := cfg
		cfg.Storage = FileStorage
		fs, err := newFileStore(
			FileStoreConfig{StoreDir: b.TempDir(), BlockSize: 8192}, cfg)
		require_NoError(b, err)
		defer fs.Stop()
		run(b, fs)
	})
}

func TestStoreSourcesOnlyTracksConfiguredSources(t *testing.T) {
	testAllStoreAllPermutations(
		t, false,
		StreamConfig{
			Name:     "SOURCE",
			Subjects: []string{">"},
			Sources:  []*StreamSource{{Name: "ORIGIN1"}},
		},
		func(t *testing.T, fs StreamStore) {
			h1 := genHeader(nil, JSStreamSource, "ORIGIN1 5 > > foo.a IDENT1")
			_, _, err := fs.StoreMsg("foo.a", h1, nil, 0)
			require_NoError(t, err)

			// Only configured sources are seeded into the map, so a header naming one that
			// isn't configured is not tracked. It is recovered by a scan if it is added.
			h2 := genHeader(nil, JSStreamSource, "ORIGIN2 7 > > foo.b IDENT2")
			_, _, err = fs.StoreMsg("foo.b", h2, nil, 0)
			require_NoError(t, err)

			// Nor is a pre-2.10 header, which carries no index name at all.
			h3 := genHeader(nil, JSStreamSource, "ORIGIN3 9")
			_, _, err = fs.StoreMsg("foo.c", h3, nil, 0)
			require_NoError(t, err)

			state := fs.SourcesState()
			require_Len(t, len(state), 1)
			require_Equal(t, state["ORIGIN1 > >"].Seq, 5)
			require_Equal(t, state["ORIGIN1 > >"].Ident, "IDENT1")

			var tracked int
			if fss, ok := fs.(*fileStore); ok {
				fss.mu.RLock()
				tracked = len(fss.sources)
				fss.mu.RUnlock()
			} else if mss, ok := fs.(*memStore); ok {
				mss.mu.RLock()
				tracked = len(mss.sources)
				mss.mu.RUnlock()
			} else {
				t.Fatal("unknown store")
			}
			require_Equal(t, tracked, 1)

			// A new message should update both the sequence and identity.
			h4 := genHeader(nil, JSStreamSource, "ORIGIN1 2 > > foo.a IDENT3")
			_, _, err = fs.StoreMsg("foo.a", h4, nil, 0)
			require_NoError(t, err)
			state = fs.SourcesState()
			require_Len(t, len(state), 1)
			require_Equal(t, state["ORIGIN1 > >"].Seq, 2)
			require_Equal(t, state["ORIGIN1 > >"].Ident, "IDENT3")
		},
	)
}
