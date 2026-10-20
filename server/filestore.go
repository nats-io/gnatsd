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

package server

import (
	"archive/tar"
	"bytes"
	"cmp"
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"iter"
	"math"
	mrand "math/rand/v2"
	"net"
	"os"
	"path/filepath"
	"runtime"
	"runtime/debug"
	"slices"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/antithesishq/antithesis-sdk-go/assert"
	"github.com/klauspost/compress/s2"
	"github.com/minio/highwayhash"
	"github.com/nats-io/nats-server/v2/server/ats"
	"github.com/nats-io/nats-server/v2/server/avl"
	"github.com/nats-io/nats-server/v2/server/elastic"
	"github.com/nats-io/nats-server/v2/server/gsl"
	"github.com/nats-io/nats-server/v2/server/stree"
	"github.com/nats-io/nats-server/v2/server/thw"
	"golang.org/x/crypto/chacha20"
	"golang.org/x/crypto/chacha20poly1305"
)

type FileStoreConfig struct {
	// Where the parent directory for all storage will be located.
	StoreDir string
	// BlockSize is the file block size. This also represents the maximum overhead size.
	BlockSize uint64
	// CacheExpire is how long with no activity until we expire the cache.
	CacheExpire time.Duration
	// SubjectStateExpire is how long with no activity until we expire a msg block's subject state.
	SubjectStateExpire time.Duration
	// SyncInterval is how often we sync to disk in the background.
	SyncInterval time.Duration
	// SyncAlways is when the stream should sync all data writes.
	SyncAlways bool
	// SyncOnFlush relaxes SyncAlways to flush writes on FlushAllPending.
	SyncOnFlush bool
	// AsyncFlush allows async flush to batch write operations.
	AsyncFlush bool
	// Cipher is the cipher to use when encrypting.
	Cipher StoreCipher
	// Compression is the algorithm to use when compressing.
	Compression StoreCompression

	// Internal reference to our server.
	srv *Server
}

// FileStreamInfo allows us to remember created time.
type FileStreamInfo struct {
	Created time.Time
	StreamConfig
}

type StoreCipher int

const (
	ChaCha StoreCipher = iota
	AES
	NoCipher
)

func (cipher StoreCipher) String() string {
	switch cipher {
	case ChaCha:
		return "ChaCha20-Poly1305"
	case AES:
		return "AES-GCM"
	case NoCipher:
		return "None"
	default:
		return "Unknown StoreCipher"
	}
}

type StoreCompression uint8

const (
	NoCompression StoreCompression = iota
	S2Compression
)

func (alg StoreCompression) String() string {
	switch alg {
	case NoCompression:
		return "None"
	case S2Compression:
		return "S2"
	default:
		return "Unknown StoreCompression"
	}
}

func (alg StoreCompression) MarshalJSON() ([]byte, error) {
	var str string
	switch alg {
	case S2Compression:
		str = "s2"
	case NoCompression:
		str = "none"
	default:
		return nil, fmt.Errorf("unknown compression algorithm")
	}
	return json.Marshal(str)
}

func (alg *StoreCompression) UnmarshalJSON(b []byte) error {
	var str string
	if err := json.Unmarshal(b, &str); err != nil {
		return err
	}
	switch str {
	case "s2":
		*alg = S2Compression
	case "none":
		*alg = NoCompression
	default:
		return fmt.Errorf("unknown compression algorithm")
	}
	return nil
}

// File ConsumerInfo is used for creating consumer stores.
type FileConsumerInfo struct {
	Created time.Time
	Name    string
	ConsumerConfig
}

// Default file and directory permissions.
const (
	defaultDirPerms  = os.FileMode(0700)
	defaultFilePerms = os.FileMode(0600)
)

type psi struct {
	total uint64
	fblk  uint32
	lblk  uint32
}

type fileStore struct {
	srv         *Server
	mu          sync.RWMutex
	state       StreamState
	tombs       []uint64
	ld          *LostStreamData
	scb         StorageUpdateHandler
	rmcb        StorageRemoveMsgHandler
	pmsgcb      ProcessJetStreamMsgHandler
	ageChk      *time.Timer // Timer to expire messages.
	ageChkRun   bool        // Whether message expiration is currently running.
	ageChkTime  int64       // When the message expiration is scheduled to run.
	recovering  bool        // Timers, schedules, TTLs etc won't fire while set.
	syncTmr     *time.Timer
	syncMu      sync.Mutex // Serializes background and explicit block syncs.
	cfg         FileStreamInfo
	fcfg        FileStoreConfig
	syncAlways  atomic.Bool // Effective SyncAlways behavior for lock-free reads.
	syncOnFlush atomic.Bool // Effective sync on flush behavior. True only if SyncAlways and replicas > 1.
	prf         keyGen
	oldprf      keyGen
	aek         cipher.AEAD
	lmb         *msgBlock
	blks        []*msgBlock
	bim         map[uint32]*msgBlock
	psim        *stree.SubjectTree[psi]
	tsl         int
	wfsmu       sync.Mutex   // Only one writeFullState at a time to protect from overwrites.
	wfsrun      atomic.Int64 // Is writeFullState already running? For timer check only
	wfsadml     int          // writeFullState average dmap length, protected by wfsmu.
	hh          *highwayhash.Digest64
	qch         chan struct{}
	fsld        chan struct{}
	cmu         sync.RWMutex
	cfs         []ConsumerStore
	werr        error
	sips        int
	dirty       int
	closing     bool
	closed      atomic.Bool // Atomic to reduce contention on ConsumerStores.
	fip         bool
	receivedAny bool
	firstMoved  bool
	ttls        *thw.HashWheel
	scheduling  *MsgScheduling
	sdm         *SDMMeta
	lpex        time.Time // Last PurgeEx call.
	dios        *diskIOSemaphore
	sources     map[string]*StreamSourceState
}

// Represents a message store block and its data.
type msgBlock struct {
	// Here for 32bit systems and atomic.
	first       msgId
	last        msgId
	mu          sync.RWMutex
	fs          *fileStore
	aek         cipher.AEAD
	bek         cipher.Stream
	seed        []byte
	nonce       []byte
	mfn         string
	mfd         *os.File
	cmp         StoreCompression // Effective compression at the time of loading the block
	liwsz       int64
	index       uint32
	bytes       uint64 // User visible bytes count.
	rbytes      uint64 // Total bytes (raw) including deleted. Used for rolling to new blk.
	cbytes      uint64 // Bytes count after last compaction. 0 if no compaction happened yet.
	msgs        uint64 // User visible message count.
	fss         *stree.SubjectTree[SimpleState]
	kfn         string
	lwts        int64
	llts        int64
	lrts        int64
	lsts        int64
	llseq       uint64
	hh          *highwayhash.Digest64
	ecache      elastic.Pointer[cache]
	cache       *cache
	cloads      uint64
	cexp        time.Duration
	fexp        time.Duration
	ctmr        *time.Timer
	werr        error
	dmap        avl.SequenceSet
	fch         chan struct{}
	qch         chan struct{}
	lchk        [8]byte
	loading     bool
	flusher     bool
	noTrack     bool
	needSync    bool
	needKeySync bool // Key file is written once and immutable, cleared after its one sync.
	noCompact   bool
	closed      bool
	ttls        uint64 // How many msgs have TTLs?
	schedules   uint64 // How many msgs have schedules?

	// Used to mock write failures.
	mockWriteErr bool
}

// Write through caching layer that is also used on loading messages.
type cache struct {
	buf  []byte
	wp   int
	idx  []uint32
	fseq uint64
	nra  bool
	// When GC collects this cache due to it being a weak pointer,
	// recycle the buf back into the block buffer pools.
	clean runtime.Cleanup
}

type msgId struct {
	seq uint64
	ts  int64
}

const (
	// Magic is used to identify the file store files.
	magic = uint8(22)
	// Version
	version = uint8(1)
	// New IndexInfo Version
	newVersion = uint8(2)
	// hdrLen
	hdrLen = 2
	// This is where we keep the streams.
	streamsDir = "streams"
	// This is where we keep inflight batches for streams.
	batchesDir = "batches"
	// This is where we keep the message store blocks.
	msgDir = "msgs"
	// This is where we temporarily move the messages dir.
	purgeDir = "__msgs__"
	// This is where we temporarily move the new message block during purge.
	newMsgDir = "__new_msgs__"
	// used to scan blk file names.
	blkScan = "%d.blk"
	// suffix of a block file
	blkSuffix = ".blk"
	// used for compacted blocks that are staged.
	newScan = "%d.new"
	// used to scan index file names.
	indexScan = "%d.idx"
	// used to store our block encryption key.
	keyScan = "%d.key"
	// to look for orphans
	keyScanAll = "*.key"
	// This is where we keep state on consumers.
	consumerDir = "obs"
	// Index file for a consumer.
	consumerState = "o.dat"
	// The suffix that will be given to a new temporary block for compression or when rewriting the full file.
	blkTmpSuffix = ".tmp"
	// default cache buffer expiration
	defaultCacheBufferExpiration = 10 * time.Second
	// default sync interval
	defaultSyncInterval = 2 * time.Minute
	// default idle timeout to close FDs.
	closeFDsIdle = 30 * time.Second
	// default expiration time for mb.fss when idle.
	defaultFssExpiration = 2 * time.Minute
	// coalesceMinimum
	coalesceMinimum = 16 * 1024
	// maxFlushWait is maximum we will wait to gather messages to flush.
	maxFlushWait = 8 * time.Millisecond

	// Metafiles for streams and consumers.
	JetStreamMetaFile    = "meta.inf"
	JetStreamMetaFileSum = "meta.sum"
	JetStreamMetaFileKey = "meta.key"

	// This is the full snapshotted state for the stream.
	streamStreamStateFile = "index.db"

	// This is the encoded time hash wheel for TTLs.
	ttlStreamStateFile = "thw.db"

	// This is the encoded message scheduling file.
	msgSchedulingStreamStateFile = "sched.db"

	// This is the encoded sources state file. Unlike the other state files it
	// lives at the FileStoreConfig.StoreDir root rather than under msgDir, so
	// a full purge does not also purge this file.
	sourcesStreamStateFile = "sources.db"

	// AEK key sizes
	minMetaKeySize = 64
	minBlkKeySize  = 64

	// Default stream block size.
	defaultLargeBlockSize = 8 * 1024 * 1024 // 8MB
	// Default for workqueue or interest based.
	defaultMediumBlockSize = 4 * 1024 * 1024 // 4MB
	// For smaller reuse buffers. Usually being generated during contention on the lead write buffer.
	// E.g. mirrors/sources etc.
	defaultSmallBlockSize = 1 * 1024 * 1024 // 1MB
	// NOT an actual block size, but used for the sync.Pools, so that we don't allocate huge buffers
	// unnecessarily until there are enough writes to justify it.
	defaultTinyBlockSize = 1 * 1024 * 256 // 256KB
	// Maximum size for the encrypted head block.
	maximumEncryptedBlockSize = 2 * 1024 * 1024 // 2MB
	// Default for KV based
	defaultKVBlockSize = defaultMediumBlockSize
	// max block size for now.
	maxBlockSize = defaultLargeBlockSize
	// Compact minimum threshold.
	compactMinimum = 2 * 1024 * 1024 // 2MB
	// FileStoreMinBlkSize is minimum size we will do for a blk size.
	FileStoreMinBlkSize = 32 * 1000 // 32kib
	// FileStoreMaxBlkSize is maximum size we will do for a blk size.
	FileStoreMaxBlkSize = maxBlockSize
	// Check for bad record length value due to corrupt data.
	rlBadThresh = 32 * 1024 * 1024
	// Checksum size for hash for msg records.
	recordHashSize = 8

	// Above this number of subjects, index.db may not be written regularly anymore, and
	// certain psim optimisations may not be used.
	highCardinalityThreshold = 1_000_000
)

func newFileStore(fcfg FileStoreConfig, cfg StreamConfig) (*fileStore, error) {
	return newFileStoreWithCreated(fcfg, cfg, time.Now().UTC(), nil, nil)
}

func newFileStoreWithCreated(fcfg FileStoreConfig, cfg StreamConfig, created time.Time, prf, oldprf keyGen) (fs *fileStore, err error) {
	return newFileStoreWithCreatedAndMode(fcfg, cfg, created, prf, oldprf, false)
}

func newFileStoreWithCreatedAndMode(fcfg FileStoreConfig, cfg StreamConfig, created time.Time, prf, oldprf keyGen, recovering bool) (fs *fileStore, err error) {
	if cfg.Name == _EMPTY_ {
		return nil, fmt.Errorf("name required")
	}
	if cfg.Storage != FileStorage {
		return nil, fmt.Errorf("fileStore requires file storage type in config")
	}
	// Default values.
	if fcfg.BlockSize == 0 {
		fcfg.BlockSize = dynBlkSize(cfg.Retention, cfg.MaxBytes, prf != nil)
	}
	if fcfg.BlockSize > maxBlockSize {
		return nil, fmt.Errorf("filestore max block size is %s", friendlyBytes(maxBlockSize))
	}
	if fcfg.CacheExpire == 0 {
		fcfg.CacheExpire = defaultCacheBufferExpiration
	}
	if fcfg.SubjectStateExpire == 0 {
		fcfg.SubjectStateExpire = defaultFssExpiration
	}
	if fcfg.SyncInterval == 0 {
		fcfg.SyncInterval = defaultSyncInterval
	}
	dios := fcfg.srv.diskIOSemaphore()

	// Check the directory
	if stat, err := os.Stat(fcfg.StoreDir); os.IsNotExist(err) {
		if err := os.MkdirAll(fcfg.StoreDir, defaultDirPerms); err != nil {
			return nil, fmt.Errorf("could not create storage directory - %v", err)
		}
	} else if stat == nil || !stat.IsDir() {
		return nil, fmt.Errorf("storage directory is not a directory")
	}
	tmpfile, err := os.CreateTemp(fcfg.StoreDir, "_test_")
	if err != nil {
		return nil, fmt.Errorf("storage directory is not writable")
	}

	tmpfile.Close()
	dios.acquire()
	os.Remove(tmpfile.Name())
	dios.release()

	fs = &fileStore{
		fcfg:       fcfg,
		dios:       dios,
		psim:       stree.NewSubjectTree[psi](),
		bim:        make(map[uint32]*msgBlock),
		cfg:        FileStreamInfo{Created: created, StreamConfig: cfg},
		prf:        prf,
		oldprf:     oldprf,
		qch:        make(chan struct{}),
		fsld:       make(chan struct{}),
		srv:        fcfg.srv,
		recovering: recovering,
	}
	fs.syncAlways.Store(fcfg.SyncAlways && !fcfg.SyncOnFlush)
	fs.syncOnFlush.Store(fcfg.SyncOnFlush)

	// Register with access time service.
	ats.Register()

	// If we error before completion make sure to cleanup.
	defer func() {
		if err != nil {
			ats.Unregister()
		}
	}()

	// Set flush in place to AsyncFlush which by default is false.
	fs.fip = !fcfg.AsyncFlush

	// Check if this is a new setup.
	mdir := filepath.Join(fcfg.StoreDir, msgDir)
	odir := filepath.Join(fcfg.StoreDir, consumerDir)
	if err := os.MkdirAll(mdir, defaultDirPerms); err != nil {
		return nil, fmt.Errorf("could not create message storage directory - %v", err)
	}
	if err := os.MkdirAll(odir, defaultDirPerms); err != nil {
		return nil, fmt.Errorf("could not create consumer storage directory - %v", err)
	}

	// Create highway hash for message blocks. Use sha256 of directory as key.
	key := sha256.Sum256([]byte(cfg.Name))
	fs.hh, err = highwayhash.NewDigest64(key[:])
	if err != nil {
		return nil, fmt.Errorf("could not create hash: %v", err)
	}

	keyFile := filepath.Join(fs.fcfg.StoreDir, JetStreamMetaFileKey)
	_, err = os.Stat(keyFile)
	// Either the file should exist (err=nil), or it shouldn't. Any other error is reported.
	if err != nil && !os.IsNotExist(err) {
		return nil, err
	}
	// Make sure we do not have an encrypted store underneath of us but no main key.
	if fs.prf == nil && err == nil {
		return nil, errNoMainKey
	} else if fs.prf != nil && err == nil {
		// If encryption is configured and the key file exists, recover our keys.
		if err = fs.recoverAEK(); err != nil {
			return nil, err
		}
	}

	// Attempt to recover our state.
	err = fs.recoverFullState()
	if err != nil {
		if !os.IsNotExist(err) {
			fs.warn("Recovering stream state from index errored: %v", err)
		}
		// Hold onto state
		prior := fs.state
		// Reset anything that could have been set from above.
		fs.state = StreamState{}
		fs.psim, fs.tsl = fs.psim.Empty(), 0
		fs.bim = make(map[uint32]*msgBlock)
		fs.blks = nil
		fs.tombs = nil

		// Recover our message state the old way
		if err := fs.recoverMsgs(); err != nil {
			return nil, err
		}

		fs.mu.Lock()
		// Check if our prior state remembers a last sequence past where we can see.
		// Unless we're async flushing, in which case this can happen if some blocks weren't flushed.
		if prior.LastSeq > fs.state.LastSeq && !fs.fcfg.AsyncFlush {
			if mb, err := fs.newMsgBlockForWrite(); err != nil {
				fs.mu.Unlock()
				return nil, err
			} else if err = mb.writeTombstone(prior.LastSeq, prior.LastTime.UnixNano()); err != nil {
				fs.mu.Unlock()
				return nil, err
			}
			fs.state.LastSeq, fs.state.LastTime = prior.LastSeq, prior.LastTime
			if fs.state.Msgs == 0 {
				fs.state.FirstSeq = fs.state.LastSeq + 1
				fs.state.FirstTime = time.Time{}
			}
		}
		// Since we recovered here, make sure to kick ourselves to write out our stream state.
		fs.dirty++
		fs.mu.Unlock()
	}

	// Lock during the remainder of the recovery.
	fs.mu.Lock()
	// Use defer to ensure the lock is released if any of the enforcement operations
	// run into issues to avoid potential deadlocks on exit.
	unlocked := false
	defer func() {
		if !unlocked {
			fs.mu.Unlock()
		}
	}()

	// Recover per-message state like TTLs and schedules.
	if err = fs.recoverPerMessageState(); err != nil {
		fs.mu.Unlock()
		return nil, err
	}

	// Also make sure we get rid of old idx and fss files on return.
	// Do this in separate go routine vs inline and at end of processing.
	defer func() {
		if fs != nil {
			go fs.cleanupOldMeta()
		}
	}()

	// Check if we have any left over tombstones to process.
	if len(fs.tombs) > 0 {
		for _, seq := range fs.tombs {
			_, err = fs.removeMsg(seq, false, true, false)
			if err != nil && err != ErrStoreEOF && err != ErrStoreMsgNotFound {
				return nil, err
			}
			fs.removeFromLostData(seq)
		}
		// Not needed after this phase.
		fs.tombs = nil
	}

	// Limits checks and enforcement.
	if err = fs.enforceMsgLimit(); err != nil {
		return nil, err
	}
	if err = fs.enforceBytesLimit(); err != nil {
		return nil, err
	}

	// Do age checks too, make sure to call in place.
	if fs.cfg.MaxAge != 0 {
		if err = fs.expireMsgsOnRecover(); err != nil {
			return nil, err
		}
		fs.startAgeChk()
	}

	// If we have max msgs per subject make sure the is also enforced.
	if fs.cfg.MaxMsgsPer > 0 {
		if err = fs.enforceMsgPerSubjectLimit(false); err != nil {
			return nil, err
		}
	}

	// Grab first sequence for check below while we have lock.
	firstSeq := fs.state.FirstSeq
	fs.mu.Unlock()
	unlocked = true

	// If the stream has an initial sequence number then make sure we
	// have purged up until that point. We will do this only if the
	// recovered first sequence number is before our configured first
	// sequence. Need to do this locked as by now the age check timer
	// has started.
	if cfg.FirstSeq > 0 && firstSeq < cfg.FirstSeq {
		if _, err := fs.purge(cfg.FirstSeq); err != nil {
			return nil, err
		}
	}

	// Write our meta data if it does not exist or is zero'd out.
	meta := filepath.Join(fcfg.StoreDir, JetStreamMetaFile)
	fi, err := os.Stat(meta)
	if err != nil && os.IsNotExist(err) || fi != nil && fi.Size() == 0 {
		if err := fs.writeStreamMeta(); err != nil {
			return nil, err
		}
	}

	// If we expect to be encrypted check that what we are restoring is not plaintext.
	// This can happen on snapshot restores or conversions.
	if fs.prf != nil {
		if _, err := os.Stat(keyFile); err != nil && os.IsNotExist(err) {
			if err := fs.writeStreamMeta(); err != nil {
				return nil, err
			}
		}
	}

	// Setup our sync timer.
	fs.setSyncTimer()

	// Spin up the go routine that will write out our full state stream index.
	go fs.flushStreamStateLoop(fs.qch, fs.fsld)

	return fs, nil
}

// Lock all existing message blocks.
// Lock held on entry.
func (fs *fileStore) lockAllMsgBlocks() {
	for _, mb := range fs.blks {
		mb.mu.Lock()
	}
}

// Unlock all existing message blocks.
// Lock held on entry.
func (fs *fileStore) unlockAllMsgBlocks() {
	for _, mb := range fs.blks {
		mb.mu.Unlock()
	}
}

func (fs *fileStore) setCreatedTime(created time.Time) {
	fs.mu.Lock()
	fs.cfg.Created = created
	fs.mu.Unlock()
}

func (fs *fileStore) updateDurabilitySettingsLocked(syncOnFlush bool) {
	if !fs.fcfg.SyncAlways {
		return
	}
	// If the stream is backed by a Raft WAL we relax the SyncAlways
	// setting in favor of SyncOnFlush. Enable the new sync mode
	// before disabling the old one so writes are never processed
	// with both modes disabled.
	if syncOnFlush {
		fs.syncOnFlush.Store(true)
		fs.syncAlways.Store(false)
	} else {
		fs.syncAlways.Store(true)
		fs.syncOnFlush.Store(false)
	}
}

func (fs *fileStore) resetDurabilitySettingsLocked() {
	fs.updateDurabilitySettingsLocked(fs.cfg.Replicas > 1)
}

// updateAsyncFlushLocked enables or disables async flush based on replication and persist mode.
// Lock should be held.
func (fs *fileStore) updateAsyncFlushLocked() error {
	lmb := fs.lmb
	if lmb == nil {
		return nil
	}
	// Enable/disable async flush depending on if it's supported and already initialized.
	supportsAsyncFlush := !fs.fcfg.SyncAlways && fs.cfg.Replicas > 1

	// Async persist mode opts in to async flushing,
	// sync always would also be disabled if it was configured.
	if fs.cfg.PersistMode == AsyncPersistMode {
		supportsAsyncFlush = true
		fs.fcfg.SyncAlways = false
		fs.syncAlways.Store(false)
	}

	if supportsAsyncFlush && !fs.fcfg.AsyncFlush {
		fs.fcfg.AsyncFlush = true
		lmb.spinUpFlushLoop()
	} else if !supportsAsyncFlush && fs.fcfg.AsyncFlush {
		fs.fcfg.AsyncFlush = false
		lmb.mu.Lock()
		// Quit the flush loop.
		if lmb.qch != nil {
			close(lmb.qch)
			lmb.qch = nil
		}
		_, err := lmb.flushPendingMsgsLocked()
		lmb.mu.Unlock()
		if err != nil {
			return err
		}
	}
	// Set flush in place to AsyncFlush which by default is false.
	fs.fip = !fs.fcfg.AsyncFlush
	return nil
}

// flushForScaleDown transitions a SyncOnFlush store back to SyncAlways.
func (fs *fileStore) flushForScaleDown() error {
	fs.mu.Lock()
	if !fs.fcfg.SyncAlways {
		fs.mu.Unlock()
		return fs.FlushAllPending()
	}
	// Turn off syncOnFlush, and enable sync after every write.
	// First enable sync after every write, so that any inflight
	// or subsequent write gets synced
	fs.updateDurabilitySettingsLocked(false)
	fs.mu.Unlock()

	// Sync all dirty blocks, so that we no longer have to rely
	// on the backing Raft WAL.
	fs.syncBlocks()

	fs.mu.RLock()
	defer fs.mu.RUnlock()
	return fs.werr
}

func (fs *fileStore) UpdateConfig(cfg *StreamConfig) error {
	start := time.Now()
	defer func() {
		if took := time.Since(start); took > time.Minute {
			fs.warn("UpdateConfig took %v", took.Round(time.Millisecond))
		}
	}()

	if cfg.Name == _EMPTY_ {
		return fmt.Errorf("name required")
	}
	if cfg.Storage != FileStorage {
		return fmt.Errorf("fileStore requires file storage type in config")
	}
	if cfg.MaxMsgsPer < -1 {
		cfg.MaxMsgsPer = -1
	}

	fs.mu.Lock()
	if fs.isClosed() {
		fs.mu.Unlock()
		return ErrStoreClosed
	}
	new_cfg := FileStreamInfo{Created: fs.cfg.Created, StreamConfig: *cfg}
	old_cfg := fs.cfg
	// The reference story has changed here, so this full msg block lock
	// may not be needed.
	fs.lockAllMsgBlocks()
	fs.cfg = new_cfg
	fs.unlockAllMsgBlocks()
	if err := fs.writeStreamMeta(); err != nil {
		fs.lockAllMsgBlocks()
		fs.cfg = old_cfg
		fs.unlockAllMsgBlocks()
		fs.mu.Unlock()
		return err
	}

	// Recover per-message state like TTLs and schedules.
	if err := fs.recoverPerMessageState(); err != nil {
		fs.mu.Unlock()
		return err
	}
	// Limits checks and enforcement.
	if err := fs.enforceMsgLimit(); err != nil {
		fs.mu.Unlock()
		return err
	}
	if err := fs.enforceBytesLimit(); err != nil {
		fs.mu.Unlock()
		return err
	}

	// Do age timers.
	if fs.ageChk == nil && fs.cfg.MaxAge != 0 {
		fs.startAgeChk()
	}
	if fs.ageChk != nil && fs.cfg.MaxAge == 0 {
		fs.ageChk.Stop()
		fs.ageChk = nil
		fs.ageChkTime = 0
	}

	if fs.cfg.MaxMsgsPer > 0 && (old_cfg.MaxMsgsPer <= 0 || fs.cfg.MaxMsgsPer < old_cfg.MaxMsgsPer) {
		if err := fs.enforceMsgPerSubjectLimit(true); err != nil {
			fs.mu.Unlock()
			return err
		}
	}

	fs.resetDurabilitySettingsLocked()
	if err := fs.updateAsyncFlushLocked(); err != nil {
		fs.mu.Unlock()
		return err
	}
	fs.mu.Unlock()

	if cfg.MaxAge != 0 || cfg.AllowMsgTTL {
		fs.expireMsgs()
	}
	if cfg.AllowMsgSchedules {
		fs.runMsgScheduling()
	}
	return nil
}

func dynBlkSize(retention RetentionPolicy, maxBytes int64, encrypted bool) uint64 {
	if maxBytes > 0 {
		blkSize := (maxBytes / 4) + 1 // (25% overhead)
		// Round up to nearest 100
		if m := blkSize % 100; m != 0 {
			blkSize += 100 - m
		}
		if blkSize <= FileStoreMinBlkSize {
			blkSize = FileStoreMinBlkSize
		} else if blkSize >= FileStoreMaxBlkSize {
			blkSize = FileStoreMaxBlkSize
		} else {
			blkSize = defaultMediumBlockSize
		}
		if encrypted && blkSize > maximumEncryptedBlockSize {
			// Notes on this below.
			blkSize = maximumEncryptedBlockSize
		}
		return uint64(blkSize)
	}

	switch {
	case encrypted:
		// In the case of encrypted stores, large blocks can result in worsened perf
		// since many writes on disk involve re-encrypting the entire block. For now,
		// we will enforce a cap on the block size when encryption is enabled to avoid
		// this.
		return maximumEncryptedBlockSize
	case retention == LimitsPolicy:
		// TODO(dlc) - Make the blocksize relative to this if set.
		return defaultLargeBlockSize
	default:
		// TODO(dlc) - Make the blocksize relative to this if set.
		return defaultMediumBlockSize
	}
}

func genEncryptionKey(sc StoreCipher, seed []byte) (ek cipher.AEAD, err error) {
	if sc == ChaCha {
		ek, err = chacha20poly1305.NewX(seed)
	} else if sc == AES {
		block, e := aes.NewCipher(seed)
		if e != nil {
			return nil, e
		}
		ek, err = cipher.NewGCMWithNonceSize(block, block.BlockSize())
	} else {
		err = errUnknownCipher
	}
	return ek, err
}

// Generate an asset encryption key from the context and server PRF.
func (fs *fileStore) genEncryptionKeys(context string) (aek cipher.AEAD, bek cipher.Stream, seed, encrypted []byte, err error) {
	if fs.prf == nil {
		return nil, nil, nil, nil, errNoEncryption
	}
	// Generate key encryption key.
	rb, err := fs.prf([]byte(context))
	if err != nil {
		return nil, nil, nil, nil, err
	}

	sc := fs.fcfg.Cipher

	kek, err := genEncryptionKey(sc, rb)
	if err != nil {
		return nil, nil, nil, nil, err
	}
	// Generate random asset encryption key seed.

	const seedSize = 32
	seed = make([]byte, seedSize)
	if n, err := rand.Read(seed); err != nil {
		return nil, nil, nil, nil, err
	} else if n != seedSize {
		return nil, nil, nil, nil, fmt.Errorf("not enough seed bytes read (%d != %d", n, seedSize)
	}

	aek, err = genEncryptionKey(sc, seed)
	if err != nil {
		return nil, nil, nil, nil, err
	}

	// Generate our nonce. Use same buffer to hold encrypted seed.
	nonce := make([]byte, kek.NonceSize(), kek.NonceSize()+len(seed)+kek.Overhead())
	if n, err := rand.Read(nonce); err != nil {
		return nil, nil, nil, nil, err
	} else if n != len(nonce) {
		return nil, nil, nil, nil, fmt.Errorf("not enough nonce bytes read (%d != %d)", n, len(nonce))
	}

	bek, err = genBlockEncryptionKey(sc, seed[:], nonce)
	if err != nil {
		return nil, nil, nil, nil, err
	}

	return aek, bek, seed, kek.Seal(nonce, nonce, seed, nil), nil
}

// Will generate the block encryption key.
func genBlockEncryptionKey(sc StoreCipher, seed, nonce []byte) (cipher.Stream, error) {
	if sc == ChaCha {
		return chacha20.NewUnauthenticatedCipher(seed, nonce)
	} else if sc == AES {
		block, err := aes.NewCipher(seed)
		if err != nil {
			return nil, err
		}
		return cipher.NewCTR(block, nonce), nil
	}
	return nil, errUnknownCipher
}

// Lock should be held.
func (fs *fileStore) recoverAEK() error {
	if fs.prf != nil && fs.aek == nil {
		ekey, err := os.ReadFile(filepath.Join(fs.fcfg.StoreDir, JetStreamMetaFileKey))
		if err != nil {
			return err
		}
		rb, err := fs.prf([]byte(fs.cfg.Name))
		if err != nil {
			return err
		}
		kek, err := genEncryptionKey(fs.fcfg.Cipher, rb)
		if err != nil {
			return err
		}
		ns := kek.NonceSize()
		seed, err := kek.Open(nil, ekey[:ns], ekey[ns:], nil)
		if err != nil {
			return err
		}
		aek, err := genEncryptionKey(fs.fcfg.Cipher, seed)
		if err != nil {
			return err
		}
		fs.aek = aek
	}
	return nil
}

// Lock should be held.
func (fs *fileStore) setupAEK() error {
	if fs.prf != nil && fs.aek == nil {
		key, _, _, encrypted, err := fs.genEncryptionKeys(fs.cfg.Name)
		if err != nil {
			return err
		}
		keyFile := filepath.Join(fs.fcfg.StoreDir, JetStreamMetaFileKey)
		if _, err := os.Stat(keyFile); err != nil && !os.IsNotExist(err) {
			return err
		}
		err = fs.writeFileWithOptionalSync(keyFile, encrypted, defaultFilePerms)
		if err != nil {
			return err
		}
		// Set our aek.
		fs.aek = key
	}
	return nil
}

// Write out meta and the checksum.
// Lock should be held.
func (fs *fileStore) writeStreamMeta() error {
	if err := fs.setupAEK(); err != nil {
		return err
	}

	meta := filepath.Join(fs.fcfg.StoreDir, JetStreamMetaFile)
	if _, err := os.Stat(meta); err != nil && !os.IsNotExist(err) {
		return err
	}
	b, err := json.Marshal(fs.cfg)
	if err != nil {
		return err
	}
	// Encrypt if needed.
	if fs.aek != nil {
		nonce := make([]byte, fs.aek.NonceSize(), fs.aek.NonceSize()+len(b)+fs.aek.Overhead())
		if n, err := rand.Read(nonce); err != nil {
			return err
		} else if n != len(nonce) {
			return fmt.Errorf("not enough nonce bytes read (%d != %d)", n, len(nonce))
		}
		b = fs.aek.Seal(nonce, nonce, b, nil)
	}

	err = fs.writeFileWithOptionalSync(meta, b, defaultFilePerms)
	if err != nil {
		return err
	}
	fs.hh.Reset()
	fs.hh.Write(b)
	var hb [highwayhash.Size64]byte
	checksum := hex.EncodeToString(fs.hh.Sum(hb[:0]))
	sum := filepath.Join(fs.fcfg.StoreDir, JetStreamMetaFileSum)
	err = fs.writeFileWithOptionalSync(sum, []byte(checksum), defaultFilePerms)
	if err != nil {
		return err
	}
	return nil
}

// Pools to recycle the blocks to help with memory pressure.
var blkPoolTiny = &sync.Pool{
	New: func() any {
		b := [defaultTinyBlockSize]byte{}
		return &b
	},
}
var blkPoolSmall = &sync.Pool{
	New: func() any {
		b := [defaultSmallBlockSize]byte{}
		return &b
	},
}
var blkPoolMedium = &sync.Pool{
	New: func() any {
		b := [defaultMediumBlockSize]byte{}
		return &b
	},
}
var blkPoolBig = &sync.Pool{
	New: func() any {
		b := [defaultLargeBlockSize]byte{}
		return &b
	},
}

// Get a new msg block based on sz estimate.
func getMsgBlockBuf(sz int) (buf []byte) {
	switch {
	case sz <= defaultTinyBlockSize:
		return blkPoolTiny.Get().(*[defaultTinyBlockSize]byte)[:0]
	case sz <= defaultSmallBlockSize:
		return blkPoolSmall.Get().(*[defaultSmallBlockSize]byte)[:0]
	case sz <= defaultMediumBlockSize:
		return blkPoolMedium.Get().(*[defaultMediumBlockSize]byte)[:0]
	case sz <= defaultLargeBlockSize:
		return blkPoolBig.Get().(*[defaultLargeBlockSize]byte)[:0]
	default:
		// Ideally this should not happen, once we return a buffer that's
		// larger than defaultLargeBlockSize then we will refuse to recycle
		// it to stop the pools from bloating excessively.
		return make([]byte, 0, sz)
	}
}

// registerRecycle arranges for the cache's buffer to be returned to the
// block buffer pools when the cache itself is garbage collected due to it
// remaining as a weak pointer. Without it, subsequent block loads would
// allocate fresh buffers, which could result in GC re-running, which is
// pathological under sustained block loads.
func (c *cache) registerRecycle() {
	if c == nil {
		return
	}
	// Any previous registration is canceled first.
	c.stopRecycle()
	// Skip if recycle isn't allowed or nothing to recycle.
	if c.nra || cap(c.buf) == 0 {
		return
	}
	// Don't make this cleanup conditional (e.g. skip if under memory pressure), GC
	// will drain the block buffer pools separately, so this doesn't pin memory.
	// Skipping would be worse, we'd delay freeing the buffer until the next GC cycle.
	c.clean = runtime.AddCleanup(c, recycleMsgBlockBuf, c.buf)
}

// stopRecycle cancels a pending recycle registration. Must be called before
// the buffer is recycled or handed off explicitly, otherwise the cleanup
// could return the same buffer to the pool a second time while it is in use.
func (c *cache) stopRecycle() {
	if c.clean != (runtime.Cleanup{}) {
		c.clean.Stop()
		c.clean = runtime.Cleanup{}
	}
}

// Recycle the msg block.
func recycleMsgBlockBuf(buf []byte) {
	switch cap(buf) {
	case defaultTinyBlockSize:
		b := (*[defaultTinyBlockSize]byte)(buf[0:defaultTinyBlockSize])
		blkPoolTiny.Put(b)
	case defaultSmallBlockSize:
		b := (*[defaultSmallBlockSize]byte)(buf[0:defaultSmallBlockSize])
		blkPoolSmall.Put(b)
	case defaultMediumBlockSize:
		b := (*[defaultMediumBlockSize]byte)(buf[0:defaultMediumBlockSize])
		blkPoolMedium.Put(b)
	case defaultLargeBlockSize:
		b := (*[defaultLargeBlockSize]byte)(buf[0:defaultLargeBlockSize])
		blkPoolBig.Put(b)
	default:
		// Too large, let the GC collect it instead.
	}
}

const (
	msgHdrSize     = 22
	checksumSize   = 8
	emptyRecordLen = msgHdrSize + checksumSize
)

// Lock should be held.
func (fs *fileStore) noTrackSubjects() bool {
	return !(fs.psim.Size() > 0 || len(fs.cfg.Subjects) > 0 || fs.cfg.Mirror != nil || len(fs.cfg.Sources) > 0)
}

// Will init the basics for a message block.
func (fs *fileStore) initMsgBlock(index uint32) *msgBlock {
	mb := &msgBlock{
		fs:      fs,
		index:   index,
		cexp:    fs.fcfg.CacheExpire,
		fexp:    fs.fcfg.SubjectStateExpire,
		noTrack: fs.noTrackSubjects(),
	}

	mdir := filepath.Join(fs.fcfg.StoreDir, msgDir)
	mb.mfn = filepath.Join(mdir, fmt.Sprintf(blkScan, index))

	if mb.hh == nil {
		key := sha256.Sum256(fs.hashKeyForBlock(index))
		mb.hh, _ = highwayhash.NewDigest64(key[:])
	}
	return mb
}

// Check for encryption, we do not load keys on startup anymore so might need to load them here.
// Lock for fs should be held.
func (mb *msgBlock) checkAndLoadEncryption() error {
	if mb.fs != nil && mb.fs.prf != nil && (mb.aek == nil || mb.bek == nil) {
		if err := mb.fs.loadEncryptionForMsgBlock(mb); err != nil {
			return err
		}
	}
	return nil
}

// Lock for fs should be held.
func (fs *fileStore) loadEncryptionForMsgBlock(mb *msgBlock) error {
	if fs.prf == nil {
		return nil
	}

	var createdKeys bool
	mdir := filepath.Join(fs.fcfg.StoreDir, msgDir)
	ekey, err := os.ReadFile(filepath.Join(mdir, fmt.Sprintf(keyScan, mb.index)))
	if err != nil {
		// We do not seem to have keys even though we should. Could be a plaintext conversion.
		// Create the keys and we will double check below.
		if err := fs.genEncryptionKeysForBlock(mb); err != nil {
			return err
		}
		createdKeys = true
	} else {
		if len(ekey) < minBlkKeySize {
			return errBadKeySize
		}
		// Recover key encryption key.
		rb, err := fs.prf([]byte(fmt.Sprintf("%s:%d", fs.cfg.Name, mb.index)))
		if err != nil {
			return err
		}

		sc := fs.fcfg.Cipher
		kek, err := genEncryptionKey(sc, rb)
		if err != nil {
			return err
		}
		ns := kek.NonceSize()
		seed, err := kek.Open(nil, ekey[:ns], ekey[ns:], nil)
		if err != nil {
			// We may be here on a cipher conversion, so attempt to convert.
			if err = mb.convertCipher(); err != nil {
				return err
			}
		} else {
			mb.seed, mb.nonce = seed, ekey[:ns]
		}
		mb.aek, err = genEncryptionKey(sc, mb.seed)
		if err != nil {
			return err
		}
		if mb.bek, err = genBlockEncryptionKey(sc, mb.seed, mb.nonce); err != nil {
			return err
		}
	}

	// If we created keys here, let's check the data and if it is plaintext convert here.
	if createdKeys {
		if err := mb.convertToEncrypted(); err != nil {
			return err
		}
	}

	return nil
}

// Load a last checksum if needed from the block file.
// Lock should be held.
func (mb *msgBlock) ensureLastChecksumLoaded() {
	var empty [8]byte
	if mb.lchk != empty {
		return
	}
	copy(mb.lchk[0:], mb.lastChecksum())
}

// Lock held on entry
func (fs *fileStore) recoverMsgBlock(index uint32) (*msgBlock, error) {
	mb := fs.initMsgBlock(index)
	// Open up the message file, but we will try to recover from the index file.
	// We will check that the last checksums match.
	file, err := mb.openBlock()
	if err != nil {
		return nil, err
	}
	defer file.Close()

	if fi, err := file.Stat(); fi != nil {
		mb.rbytes = uint64(fi.Size())
	} else {
		return nil, err
	}

	// Make sure encryption loaded if needed.
	if err = fs.loadEncryptionForMsgBlock(mb); err != nil {
		// If the encryption key is truncated or unrecoverable, return the block so it can be deleted.
		if err == errBadKeySize || err == errKeyInvalid {
			return mb, err
		}
		return nil, err
	}

	// Grab last checksum from main block file.
	var lchk [8]byte
	if mb.rbytes >= checksumSize {
		if mb.bek != nil {
			// We pass nil, so get a buf from the block pool, we'll need to recycle it afterward.
			buf, _ := mb.loadBlock(nil)
			if len(buf) >= checksumSize {
				mb.bek.XORKeyStream(buf, buf)
				copy(lchk[0:], buf[len(buf)-checksumSize:])
			}
			// We can recycle it now.
			recycleMsgBlockBuf(buf)
		} else if _, err = file.ReadAt(lchk[:], int64(mb.rbytes)-checksumSize); err != nil {
			return nil, err
		}
	}

	if err = file.Close(); err != nil {
		return nil, err
	}

	// Read our index file. Use this as source of truth if possible.
	// This not applicable in >= 2.10 servers. Here for upgrade paths from < 2.10.
	if err := mb.readIndexInfo(); err == nil {
		// Quick sanity check here.
		// Note this only checks that the message blk file is not newer then this file, or is empty and we expect empty.
		if (mb.rbytes == 0 && mb.msgs == 0) || bytes.Equal(lchk[:], mb.lchk[:]) {
			if mb.msgs > 0 && !mb.noTrack && fs.psim != nil {
				if err = fs.populateGlobalPerSubjectInfo(mb); err != nil {
					return nil, err
				}
				// Try to dump any state we needed on recovery.
				mb.tryForceExpireCacheLocked()
			}
			fs.addMsgBlock(mb)
			return mb, nil
		}
	}

	// If we get data loss rebuilding the message block state record that with the fs itself.
	ld, tombs, err := mb.rebuildState()
	if err != nil {
		return nil, err
	} else if ld != nil {
		fs.addLostData(ld)
	}
	// Collect all tombstones.
	if len(tombs) > 0 {
		fs.tombs = append(fs.tombs, tombs...)
	}

	if mb.msgs > 0 && !mb.noTrack && fs.psim != nil {
		if err = fs.populateGlobalPerSubjectInfo(mb); err != nil {
			return nil, err
		}
		// Try to dump any state we needed on recovery.
		mb.tryForceExpireCacheLocked()
	}

	if err = mb.closeFDs(); err != nil {
		return nil, err
	}
	fs.addMsgBlock(mb)

	return mb, nil
}

func (fs *fileStore) lostData() *LostStreamData {
	fs.mu.RLock()
	defer fs.mu.RUnlock()
	if fs.ld == nil {
		return nil
	}
	nld := *fs.ld
	return &nld
}

// Lock should be held.
func (fs *fileStore) addLostData(ld *LostStreamData) {
	if ld == nil {
		return
	}
	if fs.ld != nil {
		var added bool
		for _, seq := range ld.Msgs {
			if _, found := fs.ld.exists(seq); !found {
				fs.ld.Msgs = append(fs.ld.Msgs, seq)
				added = true
			}
		}
		if added {
			msgs := fs.ld.Msgs
			slices.Sort(msgs)
			fs.ld.Bytes += ld.Bytes
		}
	} else {
		fs.ld = ld
	}
}

// Helper to see if we already have this sequence reported in our lost data.
func (ld *LostStreamData) exists(seq uint64) (int, bool) {
	i := slices.IndexFunc(ld.Msgs, func(i uint64) bool {
		return i == seq
	})
	return i, i > -1
}

func (fs *fileStore) removeFromLostData(seq uint64) {
	if fs.ld == nil {
		return
	}
	if i, found := fs.ld.exists(seq); found {
		fs.ld.Msgs = append(fs.ld.Msgs[:i], fs.ld.Msgs[i+1:]...)
		if len(fs.ld.Msgs) == 0 {
			fs.ld = nil
		}
	}
}

func (fs *fileStore) rebuildState(ld *LostStreamData) {
	fs.mu.Lock()
	defer fs.mu.Unlock()
	fs.rebuildStateLocked(ld)
}

// Lock should be held.
func (fs *fileStore) rebuildStateLocked(ld *LostStreamData) {
	fs.addLostData(ld)

	fs.state.Msgs, fs.state.Bytes = 0, 0
	fs.state.FirstSeq, fs.state.LastSeq = 0, 0

	for _, mb := range fs.blks {
		mb.mu.RLock()
		fs.state.Msgs += mb.msgs
		fs.state.Bytes += mb.bytes
		fseq := atomic.LoadUint64(&mb.first.seq)
		if fs.state.FirstSeq == 0 || (fseq < fs.state.FirstSeq && mb.first.ts != 0) {
			fs.state.FirstSeq = fseq
			if mb.first.ts == 0 {
				fs.state.FirstTime = time.Time{}
			} else {
				fs.state.FirstTime = time.Unix(0, mb.first.ts).UTC()
			}
		}
		// Preserve last time, could have erased the last message in one block, and then
		// have a tombstone with the proper timestamp afterward in another block
		if lseq := atomic.LoadUint64(&mb.last.seq); lseq >= fs.state.LastSeq {
			fs.state.LastSeq = lseq
			if mb.last.ts == 0 {
				fs.state.LastTime = time.Time{}
			} else {
				fs.state.LastTime = time.Unix(0, mb.last.ts).UTC()
			}
		}
		mb.mu.RUnlock()
	}
}

// Attempt to convert the cipher used for this message block.
func (mb *msgBlock) convertCipher() error {
	fs := mb.fs
	sc := fs.fcfg.Cipher

	var osc StoreCipher
	switch sc {
	case ChaCha:
		osc = AES
	case AES:
		osc = ChaCha
	}

	mdir := filepath.Join(fs.fcfg.StoreDir, msgDir)
	ekey, err := os.ReadFile(filepath.Join(mdir, fmt.Sprintf(keyScan, mb.index)))
	if err != nil {
		return err
	}
	if len(ekey) < minBlkKeySize {
		return errBadKeySize
	}
	type prfWithCipher struct {
		keyGen
		StoreCipher
	}
	var prfs []prfWithCipher
	if fs.prf != nil {
		prfs = append(prfs, prfWithCipher{fs.prf, sc})
		prfs = append(prfs, prfWithCipher{fs.prf, osc})
	}
	if fs.oldprf != nil {
		prfs = append(prfs, prfWithCipher{fs.oldprf, sc})
		prfs = append(prfs, prfWithCipher{fs.oldprf, osc})
	}

	for _, prf := range prfs {
		// Recover key encryption key.
		rb, err := prf.keyGen([]byte(fmt.Sprintf("%s:%d", fs.cfg.Name, mb.index)))
		if err != nil {
			continue
		}
		kek, err := genEncryptionKey(prf.StoreCipher, rb)
		if err != nil {
			continue
		}
		ns := kek.NonceSize()
		seed, err := kek.Open(nil, ekey[:ns], ekey[ns:], nil)
		if err != nil {
			continue
		}
		nonce := ekey[:ns]
		bek, err := genBlockEncryptionKey(prf.StoreCipher, seed, nonce)
		if err != nil {
			return err
		}

		buf, _ := mb.loadBlock(nil)
		bek.XORKeyStream(buf, buf)
		// Check for compression, and make sure we can parse with old cipher and key file.
		if nbuf, err := mb.decompressIfNeeded(buf); err != nil {
			return err
		} else if _, _, err = mb.rebuildStateFromBufLocked(nbuf, false); err != nil {
			return err
		} else if err = mb.indexCacheBuf(nbuf); err != nil {
			return err
		}

		// Reset the cache since we just read everything in.
		mb.cache.stopRecycle()
		mb.cache = nil
		mb.ecache.Set(nil)

		// Generate new keys. If we error for some reason then we will put
		// the old keyfile back.
		if err := fs.genEncryptionKeysForBlock(mb); err != nil {
			keyFile := filepath.Join(mdir, fmt.Sprintf(keyScan, mb.index))
			writeFileWithSync(fs.dios, keyFile, ekey, defaultFilePerms)
			return err
		}
		mb.bek.XORKeyStream(buf, buf)
		mb.fs.dios.acquire()
		err = os.WriteFile(mb.mfn, buf, defaultFilePerms)
		mb.fs.dios.release()
		if err != nil {
			return err
		}
		return nil
	}
	return errKeyInvalid
}

// Convert a plaintext block to encrypted.
func (mb *msgBlock) convertToEncrypted() error {
	if mb.bek == nil {
		return nil
	}
	buf, err := mb.loadBlock(nil)
	if err != nil {
		return err
	}
	// Check for compression.
	if buf, err = mb.decompressIfNeeded(buf); err != nil {
		return err
	} else if _, _, err = mb.rebuildStateFromBufLocked(buf, false); err != nil {
		return err
	} else if err = mb.indexCacheBuf(buf); err != nil {
		// This likely indicates this was already encrypted or corrupt.
		mb.cache = nil
		mb.ecache.Set(nil)
		return err
	}
	// Undo cache from above for later.
	mb.cache.stopRecycle()
	mb.cache = nil
	mb.ecache.Set(nil)
	// Regenerate mb.bek so that the keystream offset is at zero. This matches
	// what encryptOrDecryptIfNeeded does on read-back, otherwise re-entering
	// convertToEncrypted with a previously-used mb.bek would write ciphertext at
	// the wrong stream offset and silently corrupt the block.
	if mb.bek, err = genBlockEncryptionKey(mb.fs.fcfg.Cipher, mb.seed, mb.nonce); err != nil {
		return err
	}
	mb.bek.XORKeyStream(buf, buf)
	mb.fs.dios.acquire()
	err = os.WriteFile(mb.mfn, buf, defaultFilePerms)
	mb.fs.dios.release()
	if err != nil {
		return err
	}
	return nil
}

// Return the mb's index.
func (mb *msgBlock) getIndex() uint32 {
	mb.mu.RLock()
	defer mb.mu.RUnlock()
	return mb.index
}

// Rebuild the state of the blk based on what we have on disk in the N.blk file.
// We will return any lost data, and we will return any delete tombstones we encountered.
func (mb *msgBlock) rebuildState() (*LostStreamData, []uint64, error) {
	mb.mu.Lock()
	defer mb.mu.Unlock()
	return mb.rebuildStateLocked()
}

// Rebuild the state of the blk based on what we have on disk in the N.blk file.
// Lock should be held.
func (mb *msgBlock) rebuildStateLocked() (*LostStreamData, []uint64, error) {
	// Remove the .fss file and clear any cache we have set.
	mb.clearCacheAndOffset()

	buf, err := mb.loadBlock(nil)
	defer recycleMsgBlockBuf(buf)

	if err != nil || len(buf) == 0 {
		// Only allow continuing to mark lost data if the file itself doesn't exist, or was empty.
		if err != nil && err != errNoBlkData {
			return nil, nil, err
		}
		var ld *LostStreamData
		// No data to rebuild from here.
		if mb.msgs > 0 {
			// We need to declare lost data here.
			ld = &LostStreamData{Msgs: make([]uint64, 0, mb.msgs), Bytes: mb.bytes}
			firstSeq, lastSeq := atomic.LoadUint64(&mb.first.seq), atomic.LoadUint64(&mb.last.seq)
			for seq := firstSeq; seq <= lastSeq; seq++ {
				if !mb.dmap.Exists(seq) {
					ld.Msgs = append(ld.Msgs, seq)
				}
			}
			// Clear invalid state. We will let this blk be added in here.
			mb.msgs, mb.bytes, mb.rbytes, mb.fss = 0, 0, 0, nil
			mb.dmap.Empty()
			atomic.StoreUint64(&mb.first.seq, atomic.LoadUint64(&mb.last.seq)+1)
		}
		return ld, nil, nil
	}

	// Check if we need to decrypt.
	if err = mb.encryptOrDecryptIfNeeded(buf); err != nil {
		return nil, nil, err
	}
	// Check for compression.
	if buf, err = mb.decompressIfNeeded(buf); err != nil {
		return nil, nil, err
	}
	return mb.rebuildStateFromBufLocked(buf, true)
}

// Lock should be held.
func (mb *msgBlock) rebuildStateFromBufLocked(buf []byte, allowTruncate bool) (*LostStreamData, []uint64, error) {
	var err error
	startLastSeq := atomic.LoadUint64(&mb.last.seq)

	// Clear state we need to rebuild.
	mb.msgs, mb.bytes, mb.rbytes, mb.fss = 0, 0, 0, nil
	atomic.StoreUint64(&mb.last.seq, 0)
	mb.last.ts = 0
	firstNeedsSet := true

	mb.rbytes = uint64(len(buf))

	addToDmap := func(seq uint64) {
		if seq == 0 {
			return
		}
		mb.dmap.Insert(seq)
	}

	// For tombstones that we find and collect.
	var (
		tombstones      []uint64
		maxTombstoneSeq uint64
		maxTombstoneTs  int64
	)

	defer func() {
		// For empty msg blocks make sure we recover last seq correctly based off of first.
		// Or if we seem to have no messages but had a tombstone, which we use to remember
		// sequences and timestamps now, use that to properly setup the first and last.
		if mb.msgs == 0 {
			fseq := atomic.LoadUint64(&mb.first.seq)
			if fseq > 0 {
				atomic.StoreUint64(&mb.last.seq, fseq-1)
			} else if fseq == 0 && maxTombstoneSeq > 0 {
				atomic.StoreUint64(&mb.first.seq, maxTombstoneSeq+1)
				mb.first.ts = 0
				if mb.last.seq == 0 {
					atomic.StoreUint64(&mb.last.seq, maxTombstoneSeq)
					mb.last.ts = maxTombstoneTs
				}
			}
		}
	}()

	var le = binary.LittleEndian

	// There are cases where we're not allowed to truncate, like for an encrypted or compressed
	// block since the index will be the decrypted and decompressed index.
	truncate := func(index uint32) error {
		var fd *os.File
		if mb.mfd != nil {
			fd = mb.mfd
		} else {
			mb.fs.dios.acquire()
			fd, err = os.OpenFile(mb.mfn, os.O_RDWR, defaultFilePerms)
			mb.fs.dios.release()
			if err == nil {
				defer fd.Close()
			}
		}
		if fd == nil {
			return nil
		}
		if err := fd.Truncate(int64(index)); err != nil {
			return err
		}

		// Update our checksum.
		if index >= 8 {
			var lchk [8]byte
			if _, err = fd.ReadAt(lchk[:], int64(index-8)); err != nil {
				return err
			}
			copy(mb.lchk[0:], lchk[:])
		}
		if err = fd.Sync(); err != nil {
			return err
		}
		return nil
	}

	gatherLost := func(lb uint32) *LostStreamData {
		var ld LostStreamData
		for seq := atomic.LoadUint64(&mb.last.seq) + 1; seq <= startLastSeq; seq++ {
			ld.Msgs = append(ld.Msgs, seq)
		}
		ld.Bytes = uint64(lb)
		return &ld
	}

	// To detect gaps from compaction, and to ensure the sequence keeps moving up.
	var last uint64
	var hb [highwayhash.Size64]byte

	updateLast := func(seq uint64, ts int64) {
		// The sequence needs to only ever move up.
		if seq <= last {
			return
		}

		// Check for any gaps from compaction, meaning no ebit entry.
		if last > 0 && seq != last+1 && mb.msgs != 0 {
			for dseq := last + 1; dseq < seq; dseq++ {
				addToDmap(dseq)
			}
		}
		last = seq
		atomic.StoreUint64(&mb.last.seq, last)
		mb.last.ts = ts
	}

	for index, lbuf := uint32(0), uint32(len(buf)); index < lbuf; {
		if index+msgHdrSize > lbuf {
			err = errBadMsg{mb.mfn, fmt.Sprintf("message overrun (index %d lbuf %d)", index, lbuf)}
			if allowTruncate {
				if err = truncate(index); err != nil {
					return nil, nil, err
				}
			}
			return gatherLost(lbuf - index), tombstones, err
		}

		hdr := buf[index : index+msgHdrSize]
		rl, slen := le.Uint32(hdr[0:]), int(le.Uint16(hdr[20:]))

		hasHeaders := rl&hbit != 0
		// Clear any headers bit that could be set.
		rl &^= hbit
		shlen := slen
		if hasHeaders {
			shlen += 4
		}
		dlen := int(rl) - msgHdrSize
		// Do some quick sanity checks here.
		if dlen < 0 || shlen > (dlen-recordHashSize) || dlen > int(rl) || index+rl > lbuf || rl > rlBadThresh {
			err = errBadMsg{mb.mfn, fmt.Sprintf("sanity check failed (dlen %d slen %d rl %d index %d lbuf %d)", dlen, slen, rl, index, lbuf)}
			if allowTruncate {
				if err = truncate(index); err != nil {
					return nil, nil, err
				}
			}
			return gatherLost(lbuf - index), tombstones, err
		}

		// Check for checksum failures before additional processing.
		data := buf[index+msgHdrSize : index+rl]
		subj := data[:slen]
		if hh := mb.hh; hh != nil {
			hh.Reset()
			hh.Write(hdr[4:20])
			hh.Write(subj)
			if hasHeaders {
				hh.Write(data[slen+4 : dlen-recordHashSize])
			} else {
				hh.Write(data[slen : dlen-recordHashSize])
			}
			checksum := hh.Sum(hb[:0])
			if !bytes.Equal(checksum, data[len(data)-recordHashSize:]) {
				err = errBadMsg{mb.mfn, "invalid checksum"}
				if allowTruncate {
					if err = truncate(index); err != nil {
						return nil, nil, err
					}
				}
				return gatherLost(lbuf - index), tombstones, err
			}
			copy(mb.lchk[0:], checksum)
		}

		// Grab our sequence and timestamp.
		seq := le.Uint64(hdr[4:])
		ts := int64(le.Uint64(hdr[12:]))

		// Check if this is a delete tombstone.
		if seq&tbit != 0 {
			seq = seq &^ tbit
			// Need to process this here and make sure we have accounted for this properly.
			tombstones = append(tombstones, seq)
			if maxTombstoneSeq == 0 || seq > maxTombstoneSeq {
				maxTombstoneSeq, maxTombstoneTs = seq, ts
			}
			index += rl
			continue
		}

		fseq := atomic.LoadUint64(&mb.first.seq)
		// This is an old erased message, or a new one that we can track.
		if seq == 0 || seq&ebit != 0 || seq < fseq {
			seq = seq &^ ebit
			if seq >= fseq {
				updateLast(seq, ts)
				if mb.msgs == 0 {
					atomic.StoreUint64(&mb.first.seq, seq+1)
					mb.first.ts = 0
				} else if seq != 0 {
					// Only add to dmap if past recorded first seq and non-zero.
					addToDmap(seq)
				}
			}
			index += rl
			continue
		}

		// This is for when we have index info that adjusts for deleted messages
		// at the head. So the first.seq will be already set here. If this is larger
		// replace what we have with this seq.
		if firstNeedsSet && seq >= fseq {
			atomic.StoreUint64(&mb.first.seq, seq)
			firstNeedsSet, mb.first.ts = false, ts
		}

		// The sequence needs to only ever move up.
		if seq <= last {
			// Advance to next record.
			// We've already accounted for this sequence and marked it as deleted.
			index += rl
			continue
		}
		if !mb.dmap.Exists(seq) {
			mb.msgs++
			mb.bytes += uint64(rl)
		}

		updateLast(seq, ts)

		// Advance to next record.
		index += rl
	}

	// For empty msg blocks make sure we recover last seq correctly based off of first.
	// Or if we seem to have no messages but had a tombstone, which we use to remember
	// sequences and timestamps now, use that to properly setup the first and last.
	if mb.msgs == 0 {
		fseq := atomic.LoadUint64(&mb.first.seq)
		if fseq > 0 {
			atomic.StoreUint64(&mb.last.seq, fseq-1)
		} else if fseq == 0 && maxTombstoneSeq > 0 {
			atomic.StoreUint64(&mb.first.seq, maxTombstoneSeq+1)
			mb.first.ts = 0
			if lseq := atomic.LoadUint64(&mb.last.seq); lseq == 0 {
				atomic.StoreUint64(&mb.last.seq, maxTombstoneSeq)
				mb.last.ts = maxTombstoneTs
			}
		}
	}

	return nil, tombstones, nil
}

// For doing warn logging.
// Lock should be held.
func (fs *fileStore) warn(format string, args ...any) {
	// No-op if no server configured.
	if fs.srv == nil {
		return
	}
	fs.srv.Warnf(fmt.Sprintf("Filestore [%s] %s", fs.cfg.Name, format), args...)
}

// For doing rate-limited warn logging.
// Lock should be held.
func (fs *fileStore) rateLimitWarn(format string, args ...any) {
	// No-op if no server configured.
	if fs.srv == nil {
		return
	}
	fs.srv.RateLimitWarnf(fmt.Sprintf("Filestore [%s] %s", fs.cfg.Name, format), args...)
}

// For doing error logging.
// Lock should be held.
func (fs *fileStore) error(format string, args ...any) {
	// No-op if no server configured.
	if fs.srv == nil {
		return
	}
	fs.srv.Errorf(fmt.Sprintf("Filestore [%s] %s", fs.cfg.Name, format), args...)
}

// For doing debug logging.
// Lock should be held.
func (fs *fileStore) debug(format string, args ...any) {
	// No-op if no server configured.
	if fs.srv == nil {
		return
	}
	fs.srv.Debugf(fmt.Sprintf("Filestore [%s] %s", fs.cfg.Name, format), args...)
}

// Track local state but ignore timestamps here.
func updateTrackingState(state *StreamState, mb *msgBlock) {
	first := atomic.LoadUint64(&mb.first.seq)
	last := atomic.LoadUint64(&mb.last.seq)
	if state.FirstSeq == 0 {
		state.FirstSeq = first
	} else if first < state.FirstSeq && mb.first.ts != 0 {
		state.FirstSeq = first
	}
	if last > state.LastSeq {
		state.LastSeq = last
	}
	state.Msgs += mb.msgs
	state.Bytes += mb.bytes
}

// Determine if our tracking states are the same.
func trackingStatesEqual(fs, mb *StreamState) bool {
	// When a fs is brand new the fs state will have first seq of 0, but tracking mb may have 1.
	// If either has a first sequence that is not 0 or 1 we will check if they are the same, otherwise skip.
	if (fs.FirstSeq > 1 && mb.FirstSeq > 1) || mb.FirstSeq > 1 {
		return fs.Msgs == mb.Msgs && fs.FirstSeq == mb.FirstSeq && fs.LastSeq == mb.LastSeq && fs.Bytes == mb.Bytes
	}
	return fs.Msgs == mb.Msgs && fs.LastSeq == mb.LastSeq && fs.Bytes == mb.Bytes
}

// recoverFullState will attempt to receover our last full state and re-process any state changes
// that happened afterwards.
func (fs *fileStore) recoverFullState() (rerr error) {
	fs.mu.Lock()
	defer fs.mu.Unlock()

	// Check for any left over purged messages.
	fs.dios.acquire()
	if err := fs.recoverPartialPurge(); err != nil {
		fs.dios.release()
		return err
	}
	// Grab our stream state file and load it in.
	fn := filepath.Join(fs.fcfg.StoreDir, msgDir, streamStreamStateFile)
	buf, err := os.ReadFile(fn)
	fs.dios.release()

	if err != nil {
		if !os.IsNotExist(err) {
			fs.warn("Could not read stream state file: %v", err)
		}
		return err
	}

	const minLen = 32
	if len(buf) < minLen {
		_ = os.Remove(fn)
		fs.warn("Stream state too short (%d bytes)", len(buf))
		return errCorruptState
	}

	// The highwayhash will be on the end. Check that it still matches.
	h := buf[len(buf)-highwayhash.Size64:]
	buf = buf[:len(buf)-highwayhash.Size64]
	fs.hh.Reset()
	fs.hh.Write(buf)
	var hb [highwayhash.Size64]byte
	if !bytes.Equal(h, fs.hh.Sum(hb[:0])) {
		_ = os.Remove(fn)
		fs.warn("Stream state checksum did not match")
		return errCorruptState
	}

	// Decrypt if needed.
	// We can be setup for encryption but if this is a snapshot restore we will be missing the keyfile
	// since snapshots strip encryption.
	if fs.prf != nil && fs.aek != nil {
		ns := fs.aek.NonceSize()
		buf, err = fs.aek.Open(nil, buf[:ns], buf[ns:], nil)
		if err != nil {
			fs.warn("Stream state error reading encryption key: %v", err)
			return err
		}
	}

	version := buf[1]
	if buf[0] != fullStateMagic || version < fullStateMinVersion || version > fullStateVersion {
		_ = os.Remove(fn)
		fs.warn("Stream state magic and version mismatch")
		return errCorruptState
	}

	bi := hdrLen

	readU64 := func() uint64 {
		if bi < 0 {
			return 0
		}
		v, n := binary.Uvarint(buf[bi:])
		if n <= 0 {
			bi = -1
			return 0
		}
		bi += n
		return v
	}
	readI64 := func() int64 {
		if bi < 0 {
			return 0
		}
		v, n := binary.Varint(buf[bi:])
		if n <= 0 {
			bi = -1
			return -1
		}
		bi += n
		return v
	}

	setTime := func(t *time.Time, ts int64) {
		if ts == 0 {
			*t = time.Time{}
		} else {
			*t = time.Unix(0, ts).UTC()
		}
	}

	var state StreamState
	state.Msgs = readU64()
	state.Bytes = readU64()
	state.FirstSeq = readU64()
	baseTime := readI64()
	setTime(&state.FirstTime, baseTime)
	state.LastSeq = readU64()
	setTime(&state.LastTime, readI64())

	// Check for per subject info.
	numSubjects := readU64()
	// Each subject takes at least one byte, so the count can't exceed the remaining length.
	if bi < 0 || numSubjects > uint64(len(buf)-bi) {
		_ = os.Remove(fn)
		fs.warn("Stream state bad subject count (%d)", numSubjects)
		return errCorruptState
	}
	if numSubjects > 0 {
		fs.psim, fs.tsl = fs.psim.Empty(), 0
		for i := uint64(0); i < numSubjects; i++ {
			// Compare against the remaining length so a large subject length can't overflow.
			n := readU64()
			if bi < 0 || n > uint64(len(buf)-bi) {
				_ = os.Remove(fn)
				fs.warn("Stream state bad subject len (%d)", n)
				return errCorruptState
			}
			if lsubj := int(n); lsubj > 0 {
				// If we have lots of subjects this will alloc for each one.
				// We could reference the underlying buffer, but we could guess wrong if
				// number of blocks is large and subjects is low, since we would reference buf.
				subj := buf[bi : bi+lsubj]
				// We had a bug that could cause memory corruption in the PSIM that could have gotten stored to disk.
				// Only would affect subjects, so do quick check.
				if !isValidSubject(bytesToString(subj), true) {
					_ = os.Remove(fn)
					fs.warn("Stream state corrupt subject detected")
					return errCorruptState
				}
				bi += lsubj
				psi := psi{total: readU64(), fblk: uint32(readU64())}
				if psi.total > 1 || version >= 4 {
					psi.lblk = uint32(readU64())
				} else {
					psi.lblk = psi.fblk
				}
				fs.psim.Insert(subj, psi)
				fs.tsl += lsubj
			}
		}
	}

	// Track the state as represented by the blocks themselves.
	var mstate StreamState

	numBlocks := readU64()
	// Each block takes at least one byte, so the count can't exceed the remaining length.
	if bi < 0 || numBlocks > uint64(len(buf)-bi) {
		_ = os.Remove(fn)
		fs.warn("Stream state bad block count (%d)", numBlocks)
		return errCorruptState
	}
	if numBlocks > 0 {
		lastIndex := int(numBlocks - 1)
		fs.blks = make([]*msgBlock, 0, numBlocks)
		for i := 0; i < int(numBlocks); i++ {
			index, nbytes, fseq, fts, lseq, lts, numDeleted := uint32(readU64()), readU64(), readU64(), readI64(), readU64(), readI64(), readU64()
			var ttls uint64
			if version >= 2 {
				ttls = readU64()
			}
			var schedules uint64
			if version >= 3 {
				schedules = readU64()
			}
			if bi < 0 {
				_ = os.Remove(fn)
				return errCorruptState
			}
			mb := fs.initMsgBlock(index)
			atomic.StoreUint64(&mb.first.seq, fseq)
			atomic.StoreUint64(&mb.last.seq, lseq)
			mb.msgs, mb.bytes = lseq-fseq+1, nbytes
			mb.first.ts, mb.last.ts = fts+baseTime, lts+baseTime
			mb.ttls = ttls
			mb.schedules = schedules
			if numDeleted > 0 {
				dmap, n, err := avl.Decode(buf[bi:])
				if err != nil {
					_ = os.Remove(fn)
					fs.warn("Stream state error decoding avl dmap: %v", err)
					return errCorruptState
				}
				mb.dmap = *dmap
				if mb.msgs > numDeleted {
					mb.msgs -= numDeleted
				} else {
					mb.msgs = 0
				}
				bi += n
			}

			// Pre-emptively mark block as closed, we'll confirm this block
			// still exists on disk and report it as lost if not.
			mb.closed = true

			// Only add in if not empty or the lmb.
			if mb.msgs > 0 || i == lastIndex {
				fs.addMsgBlock(mb)
				updateTrackingState(&mstate, mb)
			} else {
				// Mark dirty to cleanup.
				fs.dirty++
			}
		}
	}

	// Pull in last block index for the block that had last checksum when we wrote the full state.
	blkIndex := uint32(readU64())
	var lchk [8]byte
	if bi+len(lchk) > len(buf) {
		bi = -1
	} else {
		copy(lchk[0:], buf[bi:bi+len(lchk)])
	}

	// Check if we had any errors.
	if bi < 0 {
		_ = os.Remove(fn)
		fs.warn("Stream state has no checksum present")
		return errCorruptState
	}

	// Move into place our state, msgBlks and subject info.
	fs.state = state

	// First let's check the happy path, open the blk file that was the lmb when we created the full state.
	// See if we have the last block available.
	var matched bool
	mb := fs.lmb
	if mb == nil || mb.index != blkIndex {
		_ = os.Remove(fn)
		fs.warn("Stream state block does not exist or index mismatch")
		return errCorruptState
	}
	if _, err := os.Stat(mb.mfn); err != nil && os.IsNotExist(err) {
		// If our saved state is past what we see on disk, fallback and rebuild.
		if ld, _, _ := mb.rebuildState(); ld != nil {
			fs.addLostData(ld)
		}
		fs.warn("Stream state detected prior state, could not locate msg block %d", blkIndex)
		return errPriorState
	}
	if matched = bytes.Equal(mb.lastChecksum(), lchk[:]); !matched {
		// Detected a stale index.db, we didn't write it upon shutdown so can't rely on it being correct.
		fs.warn("Stream state outdated, last block has additional entries, will rebuild")
		return errPriorState
	}

	// We need to see if any blocks exist after our last one even though we matched the last record exactly.
	mdir := filepath.Join(fs.fcfg.StoreDir, msgDir)
	var dirs []os.DirEntry

	fs.dios.acquire()
	if f, err := os.Open(mdir); err == nil {
		dirs, _ = f.ReadDir(-1)
		f.Close()
	}
	fs.dios.release()

	var index uint32
	for _, fi := range dirs {
		// Ensure it's actually a block file, otherwise fmt.Sscanf also matches %d.blk.tmp
		if !strings.HasSuffix(fi.Name(), blkSuffix) {
			continue
		}
		if n, err := fmt.Sscanf(fi.Name(), blkScan, &index); err == nil && n == 1 {
			if index > blkIndex {
				fs.warn("Stream state outdated, found extra blocks, will rebuild")
				return errPriorState
			} else if mb, ok := fs.bim[index]; ok {
				mb.closed = false
			}
		}
	}

	var rebuild bool
	for _, mb := range fs.blks {
		if mb.closed {
			rebuild = true
			if ld, _, _ := mb.rebuildState(); ld != nil {
				fs.addLostData(ld)
			}
			fs.warn("Stream state detected prior state, could not locate msg block %d", mb.index)
		}
	}
	if rebuild {
		return errPriorState
	}

	// We check first and last seq and number of msgs and bytes. If there is a difference,
	// return and error so we rebuild from the message block state on disk.
	if !trackingStatesEqual(&fs.state, &mstate) {
		_ = os.Remove(fn)
		fs.warn("Stream state encountered internal inconsistency on recover")
		return errCorruptState
	}

	return nil
}

// Recover per-message state like TTLs and schedules. Doing a linear scan through the stream
// only once if both states need to be recovered from the same sequences.
// Lock should be held.
func (fs *fileStore) recoverPerMessageState() error {
	scanSeq := uint64(math.MaxUint64)
	var ttlSeq, schedSeq, sourcesSeq uint64
	var ttlRecover, schedRecover, sourcesRecover bool
	allowMsgTTL, allowMsgSchedules := fs.cfg.AllowMsgTTL, fs.cfg.AllowMsgSchedules

	var sources map[string]*StreamSource
	if len(fs.cfg.Sources) > 0 {
		sources = make(map[string]*StreamSource, len(fs.cfg.Sources))
		for _, ssi := range fs.cfg.Sources {
			sources[ssi.composeIName()] = ssi
		}
	}

	// Create or delete the THW if needed.
	if allowMsgTTL && fs.ttls == nil {
		ttlSeq = fs.recoverTTLState()
		ttlRecover = true
		scanSeq = min(scanSeq, ttlSeq)
	} else if !allowMsgTTL && fs.ttls != nil {
		fs.ttls = nil
	}

	// Create or delete the message scheduling state if needed.
	if allowMsgSchedules && fs.scheduling == nil {
		schedSeq = fs.recoverMsgSchedulingState()
		schedRecover = true
		scanSeq = min(scanSeq, schedSeq)
	} else if !allowMsgSchedules && fs.scheduling != nil {
		fs.scheduling = nil
	}

	// Create or delete the source tracking if needed.
	var sourcesScan map[string]*StreamSource
	if sources != nil && fs.sources == nil {
		sourcesSeq = fs.recoverSourcesState()
		sourcesRecover = true
		scanSeq = min(scanSeq, sourcesSeq)
		if sourcesSeq <= fs.state.LastSeq {
			sourcesScan = sources
		}
	}
	if len(sources) > 0 {
		// Ensure the map exists even on a fresh stream that has no persisted
		// index yet, so storeRawMsg can validate headers against the configured
		// set (zero sequence == configured but not yet observed).
		if fs.sources == nil {
			fs.sources = make(map[string]*StreamSourceState, len(sources))
		}
		// Remove sources that are tracked but no longer exist.
		for k := range fs.sources {
			if _, ok := sources[k]; !ok {
				delete(fs.sources, k)
			}
		}
		// Seed sources that aren't in the map yet.
		for k, v := range sources {
			if _, ok := fs.sources[k]; !ok {
				fs.sources[k] = &StreamSourceState{}
				// Since we're adding a new source, we'll need to recover its sequence (if any).
				sourcesSeq = fs.state.FirstSeq
				sourcesRecover = true
				scanSeq = min(scanSeq, sourcesSeq)
				if sourcesScan == nil {
					sourcesScan = make(map[string]*StreamSource)
				}
				if _, ok = sourcesScan[k]; !ok {
					sourcesScan[k] = v
				}
			}
		}
	}
	// No sources configured anymore, clear and remove the index from disk.
	if fs.sources != nil && len(sources) == 0 {
		fs.sources = nil
		_ = os.Remove(fs.sourcesStatePath())
	}

	// Short-circuit if no recovering is required.
	if !ttlRecover && !schedRecover && !sourcesRecover {
		return nil
	}

	if allowMsgTTL {
		defer fs.resetAgeChk(0)
	}
	if allowMsgSchedules {
		defer fs.scheduling.resetTimer()
	}

	// If only sources need recovering, then we can optimize to a backward scan.
	if sourcesRecover && !ttlRecover && !schedRecover {
		if fs.state.Msgs > 0 && sourcesSeq <= fs.state.LastSeq {
			fs.warn("Sourcing state is outdated; attempting to recover using backward scan (seq %d to %d)", sourcesSeq, fs.state.LastSeq)
			if err := fs.recoverSourcesBackwardScan(sourcesSeq, sourcesScan); err != nil {
				return err
			}
		}
		return nil
	}

	if fs.state.Msgs > 0 && scanSeq <= fs.state.LastSeq {
		if ttlRecover && ttlSeq <= fs.state.LastSeq {
			fs.warn("TTL state is outdated; attempting to recover using linear scan (seq %d to %d)", ttlSeq, fs.state.LastSeq)
		}
		if schedRecover && schedSeq <= fs.state.LastSeq {
			fs.warn("Message scheduling state is outdated; attempting to recover using linear scan (seq %d to %d)", schedSeq, fs.state.LastSeq)
		}
		if sourcesRecover && sourcesSeq <= fs.state.LastSeq {
			fs.warn("Sourcing state is outdated; attempting to recover using linear scan (seq %d to %d)", sourcesSeq, fs.state.LastSeq)
		}
		// Only consider sources that need recovering.
		update := func(iName string, sseq uint64, ident string) {
			if _, ok := sourcesScan[iName]; !ok {
				return
			}
			if sss, ok := fs.sources[iName]; ok {
				sss.Seq, sss.Ident = sseq, ident
			}
		}
		var (
			mb     *msgBlock
			sm     StoreMsg
			mblseq uint64
		)
		for seq := scanSeq; seq <= fs.state.LastSeq; seq++ {
		retry:
			if mb == nil {
				if mb = fs.selectMsgBlock(seq); mb == nil {
					// Selecting the message block should return a block that contains this sequence,
					// or a later block if it can't be found.
					// It's an error if we can't find any block within the bounds of first and last seq.
					fs.warn("Error loading msg block with seq %d for recovering per-message state", seq)
					continue
				}
				seq = atomic.LoadUint64(&mb.first.seq)
				mblseq = atomic.LoadUint64(&mb.last.seq)
			}
			// Check if this block can be skipped.
			ttlBlock := ttlRecover && mblseq >= ttlSeq
			schedBlock := schedRecover && mblseq >= schedSeq
			sourcesBlock := sourcesRecover && mblseq >= sourcesSeq
			if !sourcesBlock && (!ttlBlock || mb.ttls == 0) && (!schedBlock || mb.schedules == 0) {
				// None of the messages in the block have state that needs recovering, so
				// don't bother doing anything further with this block, skip to the end.
				seq = mblseq + 1
			}
			if seq > mblseq {
				// We've reached the end of the loaded block, so let's go back to the
				// beginning and process the next block.
				mb.tryForceExpireCache()
				mb = nil
				if seq <= fs.state.LastSeq {
					goto retry
				}
				// Done.
				break
			}
			mb.mu.Lock()
			msg, _, err := mb.fetchMsgNoCopyLocked(seq, &sm)
			if err != nil {
				mb.finishedWithCache()
				mb.mu.Unlock()
				if err == ErrStoreMsgNotFound || err == errDeletedMsg {
					continue
				}
				fs.warn("Error loading msg seq %d for recovering per-message state: %s", seq, err)
				return err
			}
			if len(msg.hdr) > 0 {
				if ttlRecover && seq >= ttlSeq {
					if ttl, _ := getMessageTTL(msg.hdr); ttl > 0 {
						expires := time.Duration(msg.ts) + (time.Second * time.Duration(ttl))
						fs.ttls.Add(seq, int64(expires))
					}
				}
				if schedRecover && seq >= schedSeq {
					if schedule, apiErr := nextMessageSchedule(msg.hdr, msg.ts); apiErr == nil && !schedule.IsZero() {
						// Copy the subject, as it's stored in the scheduling maps and the backing cache could be reused in the meantime.
						fs.scheduling.init(seq, copyString(msg.subj), schedule.UnixNano())
					}
				}
				if sourcesRecover && seq >= sourcesSeq {
					if ss := sliceHeader(JSStreamSource, msg.hdr); len(ss) > 0 {
						streamName, iName, sseq, ident := streamAndSeq(bytesToString(ss))
						if iName == _EMPTY_ {
							// Pre-2.10 message header means it's a match for any source using that stream name.
							// Such headers carry no identity, so it is left unset.
							for _, ssi := range fs.cfg.Sources {
								if streamName == ssi.Name || (ssi.External != nil && streamName == ssi.Name+":"+getHash(ssi.External.ApiPrefix)) {
									update(ssi.composeIName(), sseq, _EMPTY_)
								}
							}
						} else {
							update(iName, sseq, ident)
						}
					}
				}
			}
			mb.finishedWithCache()
			mb.mu.Unlock()
		}
	}
	return nil
}

// Lock should be held.
func (fs *fileStore) recoverTTLState() uint64 {
	// See if we have a timed hash wheel for TTLs.
	fs.dios.acquire()
	fn := filepath.Join(fs.fcfg.StoreDir, msgDir, ttlStreamStateFile)
	buf, err := os.ReadFile(fn)
	fs.dios.release()

	if err != nil && !os.IsNotExist(err) {
		fs.warn("Recovering TTL state from index errored: %v", err)
	}

	fs.ttls = thw.NewHashWheel()

	var ttlseq uint64
	if err == nil {
		ttlseq, err = fs.ttls.Decode(buf)
		if err != nil {
			fs.warn("Error decoding TTL state: %s", err)
			// Remove the file, and reset collected state (if any).
			_ = os.Remove(fn)
			fs.ttls = thw.NewHashWheel()
		}
	}

	if ttlseq < fs.state.FirstSeq {
		ttlseq = fs.state.FirstSeq
	}
	return ttlseq
}

// Lock should be held.
func (fs *fileStore) recoverMsgSchedulingState() uint64 {
	// See if we have a schedule index file.
	fs.dios.acquire()
	fn := filepath.Join(fs.fcfg.StoreDir, msgDir, msgSchedulingStreamStateFile)
	buf, err := os.ReadFile(fn)
	fs.dios.release()

	if err != nil && !os.IsNotExist(err) {
		fs.warn("Recovering message scheduling state from index errored: %v", err)
	}

	fs.scheduling = newMsgScheduling(fs.runMsgScheduling)
	fs.scheduling.paused = fs.recovering

	var schedSeq uint64
	if err == nil {
		schedSeq, err = fs.scheduling.decode(buf)
		if err != nil {
			fs.warn("Error decoding message scheduling state: %s", err)
			// Remove the file, and reset collected state (if any).
			_ = os.Remove(fn)
			fs.scheduling = newMsgScheduling(fs.runMsgScheduling)
			fs.scheduling.paused = fs.recovering
		}
	}

	if schedSeq < fs.state.FirstSeq {
		schedSeq = fs.state.FirstSeq
	}
	return schedSeq
}

// Grabs last checksum for the named block file.
// Takes into account encryption etc.
func (mb *msgBlock) lastChecksum() []byte {
	f, err := mb.openBlock()
	if err != nil {
		return nil
	}
	defer f.Close()

	var lchk [8]byte
	if fi, _ := f.Stat(); fi != nil {
		mb.rbytes = uint64(fi.Size())
	}
	if mb.rbytes < checksumSize {
		return lchk[:]
	}
	// Encrypted?
	if err = mb.checkAndLoadEncryption(); err != nil {
		return nil
	}
	if mb.bek != nil {
		if buf, _ := mb.loadBlock(nil); len(buf) >= checksumSize {
			if err = mb.encryptOrDecryptIfNeeded(buf); err != nil {
				return nil
			}
			copy(lchk[0:], buf[len(buf)-checksumSize:])
		} else {
			return nil
		}
	} else if _, err = f.ReadAt(lchk[:], int64(mb.rbytes)-checksumSize); err != nil {
		return nil
	}
	return lchk[:]
}

// This will make sure we clean up old idx and fss files.
func (fs *fileStore) cleanupOldMeta() {
	fs.mu.RLock()
	mdir := filepath.Join(fs.fcfg.StoreDir, msgDir)
	fs.mu.RUnlock()

	fs.dios.acquire()
	f, err := os.Open(mdir)
	fs.dios.release()
	if err != nil {
		return
	}

	dirs, _ := f.ReadDir(-1)
	f.Close()

	const (
		minLen    = 4
		idxSuffix = ".idx"
		fssSuffix = ".fss"
	)
	for _, fi := range dirs {
		if name := fi.Name(); strings.HasSuffix(name, idxSuffix) || strings.HasSuffix(name, fssSuffix) {
			os.Remove(filepath.Join(mdir, name))
		}
	}
}

func (fs *fileStore) recoverMsgs() error {
	fs.mu.Lock()
	defer fs.mu.Unlock()

	// Check for any left over purged messages.
	fs.dios.acquire()
	if err := fs.recoverPartialPurge(); err != nil {
		fs.dios.release()
		return err
	}
	mdir := filepath.Join(fs.fcfg.StoreDir, msgDir)
	f, err := os.Open(mdir)
	if err != nil {
		fs.dios.release()
		return errNotReadable
	}
	dirs, err := f.ReadDir(-1)
	f.Close()
	fs.dios.release()

	if err != nil {
		return errNotReadable
	}

	indices := make(sort.IntSlice, 0, len(dirs))
	var index int
	for _, fi := range dirs {
		// Ensure it's actually a block file, otherwise fmt.Sscanf also matches %d.blk.tmp
		if !strings.HasSuffix(fi.Name(), blkSuffix) {
			continue
		}
		if n, err := fmt.Sscanf(fi.Name(), blkScan, &index); err == nil && n == 1 {
			indices = append(indices, index)
		}
	}
	indices.Sort()

	// Recover all of the msg blocks.
	// We now guarantee they are coming in order.
	for _, index := range indices {
		if mb, err := fs.recoverMsgBlock(uint32(index)); err == nil && mb != nil {
			// This is a truncate block with possibly no index. If the OS got shutdown
			// out from underneath of us this is possible.
			mb.mu.Lock()
			if atomic.LoadUint64(&mb.first.seq) == 0 {
				if err := mb.dirtyCloseWithRemove(true); err != nil {
					mb.mu.Unlock()
					return err
				}
				fs.removeMsgBlockFromList(mb)
				mb.mu.Unlock()
				continue
			}
			// If the stream is empty, reset the first/last sequences so these can
			// properly move up based purely on tombstones spread over multiple blocks.
			if fs.state.Msgs == 0 {
				fs.state.FirstSeq, fs.state.LastSeq = 0, 0
				fs.state.FirstTime, fs.state.LastTime = time.Time{}, time.Time{}
			}
			fseq := atomic.LoadUint64(&mb.first.seq)
			if fs.state.FirstSeq == 0 || (fseq < fs.state.FirstSeq && mb.first.ts != 0) {
				fs.state.FirstSeq = fseq
				if mb.first.ts == 0 {
					fs.state.FirstTime = time.Time{}
				} else {
					fs.state.FirstTime = time.Unix(0, mb.first.ts).UTC()
				}
			}
			// Preserve last time, could have erased the last message in one block, and then
			// have a tombstone with the proper timestamp afterward in another block
			if lseq := atomic.LoadUint64(&mb.last.seq); lseq >= fs.state.LastSeq {
				fs.state.LastSeq = lseq
				if mb.last.ts == 0 {
					fs.state.LastTime = time.Time{}
				} else {
					fs.state.LastTime = time.Unix(0, mb.last.ts).UTC()
				}
			}
			fs.state.Msgs += mb.msgs
			fs.state.Bytes += mb.bytes
			// If the block is empty, correct the sequences to be aligned with the current filestore state.
			if mb.msgs == 0 {
				atomic.StoreUint64(&mb.first.seq, fs.state.LastSeq+1)
				mb.first.ts = 0
				atomic.StoreUint64(&mb.last.seq, fs.state.LastSeq)
				mb.last.ts = fs.state.LastTime.UnixNano()
			}
			mb.mu.Unlock()
		} else if (err == errBadKeySize || err == errKeyInvalid) && mb != nil {
			// If we can't load the encryption key, we can't decrypt the block's data.
			// We'll revert to deleting this block until there is peer-based recovery. This still
			// catches up from the leader if it happened in the stream's tail.
			mb.mu.Lock()
			if err := mb.dirtyCloseWithRemove(true); err != nil {
				mb.mu.Unlock()
				return err
			}
			fs.removeMsgBlockFromList(mb)
			mb.mu.Unlock()
			continue
		} else {
			return err
		}
	}

	if len(fs.blks) > 0 {
		fs.lmb = fs.blks[len(fs.blks)-1]
	} else if _, err = fs.newMsgBlockForWrite(); err != nil {
		return err
	}

	// Check if we encountered any lost data.
	if fs.ld != nil {
		var emptyBlks []*msgBlock
		for _, mb := range fs.blks {
			if mb.msgs == 0 && mb.rbytes == 0 && mb != fs.lmb {
				emptyBlks = append(emptyBlks, mb)
			}
		}
		for _, mb := range emptyBlks {
			// Need the mb lock here.
			mb.mu.Lock()
			err = fs.forceRemoveMsgBlock(mb)
			mb.mu.Unlock()
			if err != nil {
				return err
			}
		}
	}

	// Check for keyfiles orphans.
	if kms, err := filepath.Glob(filepath.Join(mdir, keyScanAll)); err == nil && len(kms) > 0 {
		valid := make(map[uint32]bool)
		for _, mb := range fs.blks {
			valid[mb.index] = true
		}
		for _, fn := range kms {
			var index uint32
			shouldRemove := true
			if n, err := fmt.Sscanf(filepath.Base(fn), keyScan, &index); err == nil && n == 1 && valid[index] {
				shouldRemove = false
			}
			if shouldRemove {
				os.Remove(fn)
			}
		}
	}

	return nil
}

// Will expire msgs that have aged out on restart.
// We will treat this differently in case we have a recovery
// that will expire alot of messages on startup.
// Should only be called on startup.
func (fs *fileStore) expireMsgsOnRecover() error {
	if fs.state.Msgs == 0 {
		return nil
	}

	// If subject delete markers is configured, can't expire on recover.
	// When clustered we need to go through proposals.
	if fs.cfg.SubjectDeleteMarkerTTL > 0 {
		return nil
	}

	var minAge = time.Now().UnixNano() - int64(fs.cfg.MaxAge)
	var purged, bytes uint64
	var deleted int
	var nts int64

	// If we expire all make sure to write out a tombstone. Need to be done by hand here,
	// usually taken care of by fs.removeMsgBlock() but we do not call that here.
	var last msgId

	deleteEmptyBlock := func(mb *msgBlock) error {
		// If we are the last keep state to remember first/last sequence.
		// Do this part by hand since not deleting one by one.
		if mb == fs.lmb {
			last.seq = atomic.LoadUint64(&mb.last.seq)
			last.ts = mb.last.ts
		}
		// Make sure we do subject cleanup as well.
		if err := mb.ensurePerSubjectInfoLoaded(); err != nil {
			return err
		}
		mb.fss.IterOrdered(func(bsubj []byte, ss *SimpleState) bool {
			subj := bytesToString(bsubj)
			for i := uint64(0); i < ss.Msgs; i++ {
				fs.removePerSubject(subj)
			}
			return true
		})
		if err := mb.dirtyCloseWithRemove(true); err != nil {
			return err
		}
		deleted++
		return nil
	}

	for _, mb := range fs.blks {
		mb.mu.Lock()
		if minAge < mb.first.ts {
			nts = mb.first.ts
			mb.mu.Unlock()
			break
		}
		// Can we remove whole block here?
		if mb.last.ts <= minAge {
			purged += mb.msgs
			bytes += mb.bytes
			err := deleteEmptyBlock(mb)
			mb.mu.Unlock()
			if err != nil {
				return err
			}
			continue
		}

		// If we are here we have to process the interior messages of this blk.
		// This will load fss as well.
		if err := mb.loadMsgsWithLock(); err != nil {
			mb.mu.Unlock()
			return err
		}

		var smv StoreMsg
		var needNextFirst bool

		// Walk messages and remove if expired.
		fseq, lseq := atomic.LoadUint64(&mb.first.seq), atomic.LoadUint64(&mb.last.seq)
		for seq := fseq; seq <= lseq; seq++ {
			sm, err := mb.cacheLookupNoCopy(seq, &smv)
			// Process interior deleted msgs.
			if err == errDeletedMsg {
				// Update dmap.
				if mb.dmap.Exists(seq) {
					mb.dmap.Delete(seq)
				}
				// Keep this updated just in case since we are removing dmap entries.
				atomic.StoreUint64(&mb.first.seq, seq)
				needNextFirst = true
				continue
			}
			// Break on other errors.
			if err != nil || sm == nil {
				atomic.StoreUint64(&mb.first.seq, seq)
				needNextFirst = true
				break
			}

			// No error and sm != nil from here onward.

			// Check for done.
			if minAge < sm.ts {
				atomic.StoreUint64(&mb.first.seq, sm.seq)
				mb.first.ts = sm.ts
				needNextFirst = false
				nts = sm.ts
				break
			}

			// Delete the message here.
			if mb.msgs > 0 {
				atomic.StoreUint64(&mb.first.seq, seq)
				needNextFirst = true
				sz := fileStoreMsgSize(sm.subj, sm.hdr, sm.msg)
				if sz > mb.bytes {
					sz = mb.bytes
				}
				mb.bytes -= sz
				bytes += sz
				mb.msgs--
				purged++
			}
			// Update fss
			// Make sure we have fss loaded.
			if _, err = mb.removeSeqPerSubject(sm.subj, seq); err != nil {
				mb.finishedWithCache()
				mb.mu.Unlock()
				return err
			}
			fs.removePerSubject(sm.subj)
		}
		// Make sure we have a proper next first sequence.
		if needNextFirst {
			mb.selectNextFirst()
		}
		// Check if empty after processing, could happen if tail of messages are all deleted.
		var err error
		if mb.msgs == 0 {
			err = deleteEmptyBlock(mb)
		}
		mb.finishedWithCache()
		mb.mu.Unlock()
		if err != nil {
			return err
		}
		break
	}

	if nts > 0 {
		// Make sure to set age check based on this value.
		fs.resetAgeChk(nts - minAge)
	}

	if deleted > 0 {
		// Update block map.
		if fs.bim != nil {
			for _, mb := range fs.blks[:deleted] {
				delete(fs.bim, mb.index)
			}
		}
		// Update blks slice.
		fs.blks = copyMsgBlocks(fs.blks[deleted:])
		if lb := len(fs.blks); lb == 0 {
			fs.lmb = nil
		} else {
			fs.lmb = fs.blks[lb-1]
		}
	}
	// Update top level accounting.
	if purged < fs.state.Msgs {
		fs.state.Msgs -= purged
	} else {
		fs.state.Msgs = 0
	}
	if bytes < fs.state.Bytes {
		fs.state.Bytes -= bytes
	} else {
		fs.state.Bytes = 0
	}
	// Make sure to we properly set the fs first sequence and timestamp.
	if err := fs.selectNextFirst(); err != nil {
		return err
	}

	// Check if we have no messages and blocks left.

	if fs.lmb == nil && last.seq != 0 {
		if lmb, err := fs.newMsgBlockForWrite(); err != nil || lmb == nil {
			if err == nil {
				err = errors.New("lmb missing")
			}
			return err
		} else if err = fs.writeTombstone(last.seq, last.ts); err != nil {
			return err
		}
		// Clear any global subject state.
		fs.psim, fs.tsl = fs.psim.Empty(), 0
	}

	// If we purged anything, make sure we kick flush state loop.
	if purged > 0 {
		fs.dirty++
	}
	return nil
}

func copyMsgBlocks(src []*msgBlock) []*msgBlock {
	if src == nil {
		return nil
	}
	dst := make([]*msgBlock, len(src))
	copy(dst, src)
	return dst
}

// GetSeqFromTime looks for the first sequence number that has
// the message with >= timestamp.
func (fs *fileStore) GetSeqFromTime(t time.Time) uint64 {
	fs.mu.RLock()
	lastSeq := fs.state.LastSeq
	closed := fs.isClosed()
	fs.mu.RUnlock()

	if closed {
		return 0
	}

	mb := fs.selectMsgBlockForStart(t)
	if mb == nil {
		return lastSeq + 1
	}

	fseq := atomic.LoadUint64(&mb.first.seq)
	lseq := atomic.LoadUint64(&mb.last.seq)

	var (
		smv  StoreMsg
		cts  int64
		cseq uint64
		off  uint64
	)
	ts := t.UnixNano()

	// Using a binary search, but need to be aware of interior deletes in the block.
	seq := lseq + 1
	mb.mu.Lock()
loop:
	for fseq <= lseq {
		mid := fseq + (lseq-fseq)/2
		off = 0
		// Potentially skip over gaps. We keep the original middle but keep track of a
		// potential delete range with an offset.
		for {
			sm, _, err := mb.fetchMsgNoCopyLocked(mid+off, &smv)
			if err != nil || sm == nil {
				off++
				if mid+off <= lseq {
					continue
				} else {
					// Continue search to the left. Purposely ignore the skipped deletes here.
					lseq = mid - 1
					continue loop
				}
			}
			cts = sm.ts
			cseq = sm.seq
			break
		}
		if cts >= ts {
			seq = cseq
			if mid == fseq {
				break
			}
			// Continue search to the left.
			lseq = mid - 1
		} else {
			// Continue search to the right (potentially skipping over interior deletes).
			fseq = mid + off + 1
		}
	}
	mb.finishedWithCache()
	mb.mu.Unlock()
	return seq
}

// Find the first matching message against a sublist.
func (mb *msgBlock) firstMatchingMulti(sl *gsl.SimpleSublist, start uint64, sm *StoreMsg) (*StoreMsg, bool, error) {
	mb.mu.Lock()
	var didLoad bool
	var updateLLTS bool
	defer func() {
		if updateLLTS {
			mb.llts = ats.AccessTime()
		}
		mb.finishedWithCache()
		mb.mu.Unlock()
	}()

	if mb.fssNotLoaded() {
		// Make sure we have fss loaded.
		if err := mb.loadMsgsWithLock(); err != nil {
			return nil, false, err
		}
		didLoad = true
	}
	// Mark fss activity.
	mb.lsts = ats.AccessTime()

	// Make sure to start at mb.first.seq if fseq < mb.first.seq
	if seq := atomic.LoadUint64(&mb.first.seq); seq > start {
		start = seq
	}

	fseq := start
	lseq := atomic.LoadUint64(&mb.last.seq)

	// If the FSS state has fewer entries than sequences in the linear scan,
	// then use intersection instead as likely going to be cheaper. This will
	// often be the case with high numbers of deletes, as well as a smaller
	// number of subjects in the block.
	useIndex := uint64(mb.fss.Size()) < lseq-start
	if useIndex {
		// If there are no subject matches then this is effectively no-op.
		fseq = uint64(math.MaxUint64)
		var ierr error
		stree.IntersectGSL(mb.fss, sl, func(subj []byte, ss *SimpleState) bool {
			if ss.firstNeedsUpdate || ss.lastNeedsUpdate {
				if ierr = mb.recalculateForSubj(bytesToString(subj), ss); ierr != nil {
					return false
				}
			}
			if start <= ss.Last {
				fseq = min(fseq, max(start, ss.First))
			}
			// No candidate can precede the requested starting sequence.
			return fseq != start
		})
		if ierr != nil {
			return nil, false, ierr
		}
		if fseq == uint64(math.MaxUint64) {
			return nil, didLoad, ErrStoreMsgNotFound
		}
	}

	// Need messages loaded from here on out.
	if mb.cacheNotLoaded() {
		if err := mb.loadMsgsWithLock(); err != nil {
			return nil, false, err
		}
		didLoad = true
	}
	if sm == nil {
		sm = new(StoreMsg)
	}

	for seq := fseq; seq <= lseq; seq++ {
		if mb.dmap.Exists(seq) {
			// Optimisation to avoid calling cacheLookup which hits time.Now().
			// Instead we will update it only once in a defer.
			updateLLTS = true
			continue
		}
		llseq := mb.llseq
		fsm, err := mb.cacheLookup(seq, sm)
		if err != nil {
			continue
		}
		expireOk := !useIndex && seq == lseq && mb.llseq != llseq && mb.llseq == seq
		updateLLTS = false // cacheLookup already updated it.
		if sl.HasInterest(fsm.subj) {
			return fsm, expireOk, nil
		}
		// If we are here we did not match, so put the llseq back.
		mb.llseq = llseq
	}

	return nil, didLoad, ErrStoreMsgNotFound
}

// Find the first matching message.
// fs lock should be held.
func (mb *msgBlock) firstMatching(filter string, wc bool, start uint64, sm *StoreMsg) (*StoreMsg, bool, error) {
	mb.mu.Lock()
	var updateLLTS bool
	defer func() {
		if updateLLTS {
			mb.llts = ats.AccessTime()
		}
		mb.finishedWithCache()
		mb.mu.Unlock()
	}()

	fseq, isAll := start, filter == _EMPTY_ || filter == fwcs

	var didLoad bool
	if mb.fssNotLoaded() {
		// Make sure we have fss loaded.
		if err := mb.loadMsgsWithLock(); err != nil {
			return nil, false, err
		}
		didLoad = true
	}
	// Mark fss activity.
	mb.lsts = ats.AccessTime()

	if filter == _EMPTY_ {
		filter = fwcs
		wc = true
	}

	// If we only have 1 subject currently and it matches our filter we can also set isAll.
	if !isAll && mb.fss.Size() == 1 {
		if !wc {
			_, isAll = mb.fss.Find(stringToBytes(filter))
		} else {
			// Since mb.fss.Find won't work if filter is a wildcard, need to use Match instead.
			mb.fss.Match(stringToBytes(filter), func(subject []byte, _ *SimpleState) {
				isAll = true
			})
		}
		// If the only subject in this block isn't our filter, can simply short-circuit.
		if !isAll {
			return nil, didLoad, ErrStoreMsgNotFound
		}
	}
	// Make sure to start at mb.first.seq if fseq < mb.first.seq
	fseq = max(fseq, atomic.LoadUint64(&mb.first.seq))
	lseq := atomic.LoadUint64(&mb.last.seq)

	// Optionally build the isMatch for wildcard filters.
	var isMatch func(subj string) bool
	// Decide to build.
	if wc {
		_tsa, _fsa := [32]string{}, [32]string{}
		tsa, fsa := _tsa[:0], tokenizeSubjectIntoSlice(_fsa[:0], filter)
		isMatch = func(subj string) bool {
			tsa = tokenizeSubjectIntoSlice(tsa[:0], subj)
			return isSubsetMatchTokenized(tsa, fsa)
		}
	}

	subjs := mb.fs.cfg.Subjects
	// If isAll or our single filter matches the filter arg do linear scan.
	doLinearScan := isAll || (wc && len(subjs) == 1 && subjs[0] == filter)
	// If we do not think we should do a linear scan check how many fss we
	// would need to scan vs the full range of the linear walk. Optimize for
	// 25th quantile of a match in a linear walk. Filter should be a wildcard.
	// We should consult fss if our cache is not loaded and we only have fss loaded.
	if !doLinearScan && wc && mb.cacheAlreadyLoaded() {
		doLinearScan = mb.fss.Size()*4 > int(lseq-fseq)
	}

	if !doLinearScan {
		// If we have a wildcard match against all tracked subjects we know about.
		fseq = lseq + 1
		if bfilter := stringToBytes(filter); wc {
			var ierr error
			mb.fss.Match(bfilter, func(bsubj []byte, ss *SimpleState) {
				if ierr != nil {
					return
				}
				if ss.firstNeedsUpdate || ss.lastNeedsUpdate {
					if ierr = mb.recalculateForSubj(bytesToString(bsubj), ss); ierr != nil {
						return
					}
				}
				if start <= ss.Last {
					fseq = min(fseq, max(start, ss.First))
				}
			})
			if ierr != nil {
				return nil, false, ierr
			}
		} else if ss, _ := mb.fss.Find(bfilter); ss != nil {
			if ss.firstNeedsUpdate || ss.lastNeedsUpdate {
				if err := mb.recalculateForSubj(filter, ss); err != nil {
					return nil, false, err
				}
			}
			if start <= ss.Last {
				fseq = min(fseq, max(start, ss.First))
			}
		}
	}

	if fseq > lseq {
		return nil, didLoad, ErrStoreMsgNotFound
	}

	// Need messages loaded from here on out.
	if mb.cacheNotLoaded() {
		if err := mb.loadMsgsWithLock(); err != nil {
			return nil, false, err
		}
		didLoad = true
	}

	if sm == nil {
		sm = new(StoreMsg)
	}

	for seq := fseq; seq <= lseq; seq++ {
		if mb.dmap.Exists(seq) {
			// Optimisation to avoid calling cacheLookup which hits time.Now().
			// Instead we will update it only once in a defer.
			updateLLTS = true
			continue
		}
		llseq := mb.llseq
		fsm, err := mb.cacheLookup(seq, sm)
		if err != nil {
			if err == errPartialCache || err == errNoCache {
				return nil, false, err
			}
			continue
		}
		updateLLTS = false // cacheLookup already updated it.
		expireOk := seq == lseq && mb.llseq != llseq && mb.llseq == seq
		if isAll {
			return fsm, expireOk, nil
		}
		if wc && isMatch(sm.subj) {
			return fsm, expireOk, nil
		} else if !wc && fsm.subj == filter {
			return fsm, expireOk, nil
		}
		// If we are here we did not match, so put the llseq back.
		mb.llseq = llseq
	}

	return nil, didLoad, ErrStoreMsgNotFound
}

// Find the previous matching message against a sublist, working BACKWARDS from start.
func (mb *msgBlock) prevMatchingMulti(sl *gsl.SimpleSublist, start uint64, sm *StoreMsg) (*StoreMsg, bool, error) {
	mb.mu.Lock()
	var didLoad bool
	var updateLLTS bool
	defer func() {
		if updateLLTS {
			mb.llts = ats.AccessTime()
		}
		mb.finishedWithCache()
		mb.mu.Unlock()
	}()

	// Need messages loaded from here on out.
	if mb.cacheNotLoaded() {
		if err := mb.loadMsgsWithLock(); err != nil {
			return nil, false, err
		}
		didLoad = true
	}

	// Make sure to start at mb.last.seq if lseq < mb.last.seq
	if seq := atomic.LoadUint64(&mb.last.seq); start > seq {
		start = seq
	}
	lseq := atomic.LoadUint64(&mb.first.seq)

	if sm == nil {
		sm = new(StoreMsg)
	}

	// If the FSS state has fewer entries than sequences in the linear scan,
	// then use intersection instead as likely going to be cheaper. This will
	// often be the case with high numbers of deletes, as well as a smaller
	// number of subjects in the block.
	if uint64(mb.fss.Size()) < start-lseq {
		// If there are no subject matches then this is effectively no-op.
		hseq := uint64(0)
		var ierr error
		stree.IntersectGSL(mb.fss, sl, func(subj []byte, ss *SimpleState) bool {
			if ss.firstNeedsUpdate || ss.lastNeedsUpdate {
				// mb is already loaded into the cache so should be fast-ish.
				if ierr = mb.recalculateForSubj(bytesToString(subj), ss); ierr != nil {
					return false
				}
			}
			first := min(start, ss.Last)
			// Skip if cutoff is before this subject's first, or if we already
			// have a higher-or-equal candidate (hseq holds the highest found).
			if first < ss.First || first <= hseq {
				// The start cutoff is before the first sequence for this subject,
				// or we already know of a subject with a later-or-equal msg.
				return true
			}
			if first == ss.Last {
				// If the start floor is above where this subject starts then we can
				// short-circuit, avoiding needing to scan for the next message.
				if fsm, err := mb.cacheLookup(ss.Last, sm); err == nil {
					sm = fsm
					hseq = ss.Last
				}
				return true
			}
			for seq := first; seq >= ss.First; seq-- {
				// Otherwise we have a start floor that intersects where this subject
				// has messages in the block, so we need to walk up until we find a
				// message matching the subject.
				if mb.dmap.Exists(seq) {
					// Optimisation to avoid calling cacheLookup which hits time.Now().
					// Instead we will update it only once in a defer.
					updateLLTS = true
					continue
				}
				llseq := mb.llseq
				fsm, err := mb.cacheLookup(seq, sm)
				if err != nil {
					continue
				}
				updateLLTS = false // cacheLookup already updated it.
				if sl.HasInterest(fsm.subj) {
					hseq = seq
					sm = fsm
					break
				}
				// If we are here we did not match, so put the llseq back.
				mb.llseq = llseq
			}
			return true
		})
		if ierr != nil {
			return nil, false, ierr
		}
		if hseq > 0 && sm != nil {
			return sm, didLoad && start == lseq, nil
		}
	} else {
		for seq := start; seq >= lseq; seq-- {
			if mb.dmap.Exists(seq) {
				// Optimisation to avoid calling cacheLookup which hits time.Now().
				// Instead we will update it only once in a defer.
				updateLLTS = true
				continue
			}
			llseq := mb.llseq
			fsm, err := mb.cacheLookup(seq, sm)
			if err != nil {
				continue
			}
			expireOk := seq == lseq && mb.llseq != llseq && mb.llseq == seq
			updateLLTS = false // cacheLookup already updated it.
			if sl.HasInterest(fsm.subj) {
				return fsm, expireOk, nil
			}
			// If we are here we did not match, so put the llseq back.
			mb.llseq = llseq
		}
	}

	return nil, didLoad, ErrStoreMsgNotFound
}

// This will traverse a message block and generate the filtered pending.
func (mb *msgBlock) filteredPending(subj string, wc bool, seq uint64) (total, first, last uint64, err error) {
	mb.mu.Lock()
	defer mb.mu.Unlock()
	return mb.filteredPendingLocked(subj, wc, seq)
}

// This will traverse a message block and generate the filtered pending.
// Lock should be held.
func (mb *msgBlock) filteredPendingLocked(filter string, wc bool, sseq uint64) (total, first, last uint64, err error) {
	isAll := filter == _EMPTY_ || filter == fwcs

	// First check if we can optimize this part.
	// This means we want all and the starting sequence was before this block.
	if isAll {
		if fseq := atomic.LoadUint64(&mb.first.seq); sseq <= fseq {
			return mb.msgs, fseq, atomic.LoadUint64(&mb.last.seq), nil
		}
	}

	needsCleanup := mb.cache == nil
	defer func() {
		if needsCleanup {
			mb.finishedWithCache()
		}
	}()

	if filter == _EMPTY_ {
		filter, wc = fwcs, true
	}

	update := func(ss *SimpleState) {
		total += ss.Msgs
		if first == 0 || ss.First < first {
			first = ss.First
		}
		if ss.Last > last {
			last = ss.Last
		}
	}

	// Make sure we have fss loaded.
	if err = mb.ensurePerSubjectInfoLoaded(); err != nil {
		return 0, 0, 0, err
	}

	var havePartial bool

	// If we are not a wildcard just use Find() here. Avoids allocations.
	if !wc {
		if ss, ok := mb.fss.Find(stringToBytes(filter)); ok && ss != nil {
			if ss.firstNeedsUpdate || ss.lastNeedsUpdate {
				if err = mb.recalculateForSubj(filter, ss); err != nil {
					return 0, 0, 0, err
				}
			}
			if sseq <= ss.First {
				update(ss)
			} else if sseq <= ss.Last {
				// We matched but its a partial.
				havePartial = true
			}
		}
	} else {
		mb.fss.Match(stringToBytes(filter), func(bsubj []byte, ss *SimpleState) {
			if err != nil {
				return
			}
			if havePartial {
				// If we already found a partial then don't do anything else.
				return
			}
			if ss.firstNeedsUpdate || ss.lastNeedsUpdate {
				if err = mb.recalculateForSubj(bytesToString(bsubj), ss); err != nil {
					return
				}
			}
			if sseq <= ss.First {
				update(ss)
			} else if sseq <= ss.Last {
				// We matched but its a partial.
				havePartial = true
			}
		})
		if err != nil {
			return 0, 0, 0, err
		}
	}

	// If we did not encounter any partials we can return here.
	if !havePartial {
		return total, first, last, nil
	}

	// If we are here we need to scan the msgs.
	// Clear what we had.
	total, first, last = 0, 0, 0

	// If we load the cache for a linear scan we want to expire that cache upon exit.
	var shouldExpire bool
	if mb.cacheNotLoaded() {
		if err = mb.loadMsgsWithLock(); err != nil {
			return 0, 0, 0, err
		}
		shouldExpire = true
	}

	_tsa, _fsa := [32]string{}, [32]string{}
	tsa, fsa := _tsa[:0], _fsa[:0]
	var isMatch func(subj string) bool

	if !wc {
		isMatch = func(subj string) bool { return subj == filter }
	} else {
		fsa = tokenizeSubjectIntoSlice(fsa[:0], filter)
		isMatch = func(subj string) bool {
			tsa = tokenizeSubjectIntoSlice(tsa[:0], subj)
			return isSubsetMatchTokenized(tsa, fsa)
		}
	}

	// 1. See if we match any subs from fss.
	// 2. If we match and the sseq is past ss.Last then we can use meta only.
	// 3. If we match and we need to do a partial, break and clear any totals and do a full scan like num pending.

	var smv StoreMsg
	for seq, lseq := sseq, atomic.LoadUint64(&mb.last.seq); seq <= lseq; seq++ {
		sm, _ := mb.cacheLookupNoCopy(seq, &smv)
		if sm == nil {
			continue
		}
		if isAll || isMatch(sm.subj) {
			total++
			if first == 0 || seq < first {
				first = seq
			}
			if seq > last {
				last = seq
			}
		}
	}
	// If we loaded this block for this operation go ahead and expire it here.
	if shouldExpire {
		mb.tryForceExpireCacheLocked()
	}

	return total, first, last, nil
}

// FilteredState will return the SimpleState associated with the filtered subject and a proposed starting sequence.
func (fs *fileStore) FilteredState(sseq uint64, subj string) (SimpleState, error) {
	fs.mu.RLock()
	defer fs.mu.RUnlock()

	lseq := fs.state.LastSeq
	if sseq < fs.state.FirstSeq {
		sseq = fs.state.FirstSeq
	}

	// Returned state.
	var ss SimpleState

	// If past the end no results.
	if sseq > lseq {
		// Make sure we track sequences
		ss.First = fs.state.FirstSeq
		ss.Last = fs.state.LastSeq
		return ss, nil
	}

	// If we want all msgs that match we can shortcircuit.
	// TODO(dlc) - This can be extended for all cases but would
	// need to be careful on total msgs calculations etc.
	if sseq == fs.state.FirstSeq {
		if err := fs.numFilteredPending(subj, &ss); err != nil {
			return ss, err
		}
	} else {
		wc := subjectHasWildcard(subj)
		// Tracking subject state.
		// TODO(dlc) - Optimize for 2.10 with avl tree and no atomics per block.
		for _, mb := range fs.blks {
			// Skip blocks that are less than our starting sequence.
			if sseq > atomic.LoadUint64(&mb.last.seq) {
				continue
			}
			t, f, l, err := mb.filteredPending(subj, wc, sseq)
			if err != nil {
				return ss, err
			}
			ss.Msgs += t
			if ss.First == 0 || (f > 0 && f < ss.First) {
				ss.First = f
			}
			if l > ss.Last {
				ss.Last = l
			}
		}
	}

	return ss, nil
}

// This is used to see if we can selectively jump start blocks based on filter subject and a starting block index.
// Will return -1 and ErrStoreEOF if no matches at all or no more from where we are.
func (fs *fileStore) checkSkipFirstBlock(filter string, wc bool, bi int) (int, error) {
	// If we match everything, just move to next blk.
	if filter == _EMPTY_ || filter == fwcs {
		return bi + 1, nil
	}
	// Move through psim to gather start and stop bounds.
	start, stop := uint32(math.MaxUint32), uint32(0)
	if wc {
		if fs.psim.Size() > highCardinalityThreshold {
			return bi + 1, nil
		}
		fs.psim.Match(stringToBytes(filter), func(_ []byte, psi *psi) {
			if psi.fblk < start {
				start = psi.fblk
			}
			if psi.lblk > stop {
				stop = psi.lblk
			}
		})
	} else if psi, ok := fs.psim.Find(stringToBytes(filter)); ok {
		start, stop = psi.fblk, psi.lblk
	}
	// Nothing was found.
	if start == uint32(math.MaxUint32) {
		return -1, ErrStoreEOF
	}
	return fs.selectSkipFirstBlock(bi, start, stop)
}

// This is used to see if we can selectively jump start blocks based on filter subjects and a starting block index.
// Will return -1 and ErrStoreEOF if no matches at all or no more from where we are.
func (fs *fileStore) checkSkipFirstBlockMulti(sl *gsl.SimpleSublist, bi int) (int, error) {
	// Don't bother if full wildcard.
	if sl.MatchesFullWildcard() || fs.psim.Size() > highCardinalityThreshold {
		return bi + 1, nil
	}
	// Move through psim to gather start and stop bounds.
	start, stop := uint32(math.MaxUint32), uint32(0)
	guard := fs.blks[bi].getIndex() + 1
	stree.IntersectGSL(fs.psim, sl, func(subj []byte, psi *psi) bool {
		if psi.fblk < start {
			start = psi.fblk
		}
		if start == guard {
			// One of the subjects matches the next block, so there's no point in carrying on trying to skip.
			return false
		}
		if psi.lblk > stop {
			stop = psi.lblk
		}
		return true
	})
	if start == guard {
		return bi + 1, nil
	}
	// Nothing was found.
	if start == uint32(math.MaxUint32) {
		return -1, ErrStoreEOF
	}
	return fs.selectSkipFirstBlock(bi, start, stop)
}

func (fs *fileStore) selectSkipFirstBlock(bi int, start, stop uint32) (int, error) {
	// Can not be nil so ok to inline dereference.
	mbi := fs.blks[bi].getIndex()
	// All matching msgs are behind us.
	// Less than AND equal is important because we were called because we missed searching bi.
	if stop <= mbi {
		return -1, ErrStoreEOF
	}
	// If start is > index return dereference of fs.blks index.
	if start > mbi {
		if mb := fs.bim[start]; mb != nil {
			ni, _ := fs.selectMsgBlockWithIndex(atomic.LoadUint64(&mb.last.seq))
			return ni, nil
		}
	}
	// Otherwise just bump to the next one.
	return bi + 1, nil
}

// Optimized way for getting all num pending matching a filter subject.
// Lock should be held.
func (fs *fileStore) numFilteredPending(filter string, ss *SimpleState) error {
	return fs.numFilteredPendingWithLast(filter, true, ss)
}

// Optimized way for getting all num pending matching a filter subject and first sequence only.
// Lock should be held.
func (fs *fileStore) numFilteredPendingNoLast(filter string, ss *SimpleState) error {
	return fs.numFilteredPendingWithLast(filter, false, ss)
}

// Optimized way for getting all num pending matching a filter subject.
// Optionally look up last sequence. Sometimes do not need last and this avoids cost.
// Read lock should be held.
func (fs *fileStore) numFilteredPendingWithLast(filter string, last bool, ss *SimpleState) error {
	isAll := filter == _EMPTY_ || filter == fwcs

	// If isAll we do not need to do anything special to calculate the first and last and total.
	if isAll {
		ss.First = fs.state.FirstSeq
		ss.Last = fs.state.LastSeq
		ss.Msgs = fs.state.Msgs
		return nil
	}
	// Always reset.
	ss.First, ss.Last, ss.Msgs = 0, 0, 0

	// We do need to figure out the first and last sequences.
	wc := subjectHasWildcard(filter)
	start, stop := uint32(math.MaxUint32), uint32(0)

	if wc {
		fs.psim.Match(stringToBytes(filter), func(_ []byte, psi *psi) {
			ss.Msgs += psi.total
			// Keep track of start and stop indexes for this subject.
			if psi.fblk < start {
				start = psi.fblk
			}
			if psi.lblk > stop {
				stop = psi.lblk
			}
		})
	} else if psi, ok := fs.psim.Find(stringToBytes(filter)); ok {
		ss.Msgs += psi.total
		start, stop = psi.fblk, psi.lblk
	}

	// Did not find anything.
	if stop == 0 {
		return nil
	}

	// Do start
	mb := fs.bim[start]
	if mb != nil {
		_, f, _, err := mb.filteredPending(filter, wc, 0)
		if err != nil {
			return err
		}
		ss.First = f
	}

	if ss.First == 0 {
		// This is a miss. This can happen since psi.fblk is lazy.
		// We will make sure to update fblk.

		// Hold this outside loop for psim fblk updates when done.
		i := start + 1
		for ; i <= stop; i++ {
			mb := fs.bim[i]
			if mb == nil {
				continue
			}
			if _, f, _, err := mb.filteredPending(filter, wc, 0); err != nil {
				return err
			} else if f > 0 {
				ss.First = f
				break
			}
		}
		// Update fblk since fblk was outdated.
		// We only require read lock here as that is desirable,
		// so we need to do this in a go routine to acquire write lock.
		go func() {
			fs.mu.Lock()
			defer fs.mu.Unlock()
			if !wc {
				if info, ok := fs.psim.Find(stringToBytes(filter)); ok {
					if i > info.fblk {
						info.fblk = i
					}
				}
			} else {
				fs.psim.Match(stringToBytes(filter), func(subj []byte, psi *psi) {
					if i > psi.fblk {
						psi.fblk = i
					}
				})
			}
		}()
	}
	// Now gather last sequence if asked to do so.
	if last {
		if mb = fs.bim[stop]; mb != nil {
			_, _, l, err := mb.filteredPending(filter, wc, 0)
			if err != nil {
				return err
			}
			ss.Last = l
		}
	}
	return nil
}

// SubjectsState returns a map of SimpleState for all matching subjects.
func (fs *fileStore) SubjectsState(subject string) map[string]SimpleState {
	fs.mu.RLock()
	defer fs.mu.RUnlock()

	if fs.state.Msgs == 0 || fs.noTrackSubjects() {
		return nil
	}

	if subject == _EMPTY_ {
		subject = fwcs
	}

	start, stop := fs.blks[0], fs.lmb
	// We can short circuit if not a wildcard using psim for start and stop.
	if !subjectHasWildcard(subject) {
		info, ok := fs.psim.Find(stringToBytes(subject))
		if !ok {
			return nil
		}
		if f := fs.bim[info.fblk]; f != nil {
			start = f
		}
		if l := fs.bim[info.lblk]; l != nil {
			stop = l
		}
	}

	// Aggregate fss.
	fss := make(map[string]SimpleState)
	var startFound bool

	for _, mb := range fs.blks {
		if !startFound {
			if mb != start {
				continue
			}
			startFound = true
		}

		mb.mu.Lock()
		var shouldExpire bool
		if mb.fssNotLoaded() {
			// Make sure we have fss loaded.
			if err := mb.loadMsgsWithLock(); err != nil {
				mb.mu.Unlock()
				return nil
			}
			shouldExpire = true
		}
		// Mark fss activity.
		mb.lsts = ats.AccessTime()
		var ierr error
		mb.fss.Match(stringToBytes(subject), func(bsubj []byte, ss *SimpleState) {
			if ierr != nil {
				return
			}
			subj := string(bsubj)
			if ss.firstNeedsUpdate || ss.lastNeedsUpdate {
				if ierr = mb.recalculateForSubj(subj, ss); ierr != nil {
					return
				}
			}
			oss := fss[subj]
			if oss.First == 0 { // New
				fss[subj] = *ss
			} else {
				// Merge here.
				oss.Last, oss.Msgs = ss.Last, oss.Msgs+ss.Msgs
				fss[subj] = oss
			}
		})
		if shouldExpire {
			// Expire this cache before moving on.
			mb.tryForceExpireCacheLocked()
		} else {
			mb.finishedWithCache()
		}
		mb.mu.Unlock()
		if ierr != nil {
			return nil
		}
		if mb == stop {
			break
		}
	}

	return fss
}

// AllLastSeqs will return a sorted list of last sequences for all subjects.
func (fs *fileStore) AllLastSeqs() ([]uint64, error) {
	fs.mu.RLock()
	defer fs.mu.RUnlock()
	return fs.allLastSeqsLocked()
}

// allLastSeqsLocked will return a sorted list of last sequences for all
// subjects, but won't take the lock to do it, to avoid the issue of compounding
// read locks causing a deadlock with a write lock.
func (fs *fileStore) allLastSeqsLocked() ([]uint64, error) {
	if fs.state.Msgs == 0 || fs.noTrackSubjects() {
		return nil, nil
	}

	numSubjects := fs.psim.Size()
	seqs := make([]uint64, 0, numSubjects)
	subs := make(map[string]struct{}, numSubjects)

	for i := len(fs.blks) - 1; i >= 0; i-- {
		if len(subs) == numSubjects {
			break
		}
		mb := fs.blks[i]
		mb.mu.Lock()

		var shouldExpire bool
		if mb.fssNotLoaded() {
			// Make sure we have fss loaded.
			if err := mb.loadMsgsWithLock(); err != nil {
				mb.mu.Unlock()
				return nil, err
			}
			shouldExpire = true
		}

		var ierr error
		mb.fss.IterFast(func(bsubj []byte, ss *SimpleState) bool {
			// Check if already been processed and accounted.
			if _, ok := subs[string(bsubj)]; !ok {
				// Check if we need to recalculate. We only care about the last sequence.
				if ss.lastNeedsUpdate {
					// mb is already loaded into the cache so should be fast-ish.
					if ierr = mb.recalculateForSubj(bytesToString(bsubj), ss); ierr != nil {
						return false
					}
				}
				seqs = append(seqs, ss.Last)
				subs[string(bsubj)] = struct{}{}
			}
			return true
		})
		if shouldExpire {
			// Expire this cache before moving on.
			mb.tryForceExpireCacheLocked()
		}
		mb.finishedWithCache()
		mb.mu.Unlock()
		if ierr != nil {
			return nil, ierr
		}
	}

	slices.Sort(seqs)
	return seqs, nil
}

// Helper to determine if the filter(s) represent all the subjects.
// Most clients send in subjects even if they match the stream's ingest subjects.
// Lock should be held.
func (fs *fileStore) filterIsAll(filters []string) bool {
	if len(filters) == 1 && filters[0] == fwcs {
		return true
	}
	if len(filters) != len(fs.cfg.Subjects) {
		return false
	}
	// Sort so we can compare.
	subjects := copyStrings(fs.cfg.Subjects)
	slices.Sort(filters)
	slices.Sort(subjects)
	for i, subj := range filters {
		if !subjectIsSubsetMatch(subjects[i], subj) {
			return false
		}
	}
	return true
}

// MultiLastSeqs will return a sorted list of sequences that match all subjects presented in filters.
// We will not exceed the maxSeq, which if 0 becomes the store's last sequence.
func (fs *fileStore) MultiLastSeqs(filters []string, maxSeq uint64, maxAllowed int) ([]uint64, error) {
	fs.mu.RLock()
	defer fs.mu.RUnlock()
	return fs.multiLastSeqsLocked(filters, maxSeq, maxAllowed)
}

// Lock should be held.
func (fs *fileStore) multiLastSeqsLocked(filters []string, maxSeq uint64, maxAllowed int) ([]uint64, error) {
	if fs.state.Msgs == 0 || fs.noTrackSubjects() {
		return nil, nil
	}

	// See if we can short circuit if we think they are asking for all last sequences and have no maxSeq or maxAllowed set.
	if maxSeq == 0 && maxAllowed <= 0 && fs.filterIsAll(filters) {
		return fs.allLastSeqsLocked()
	}

	lastBlkIndex := len(fs.blks) - 1
	lastMB := fs.blks[lastBlkIndex]

	// Implied last sequence.
	if maxSeq == 0 {
		maxSeq = fs.state.LastSeq
	} else {
		// Udate last mb index if not last seq.
		lastBlkIndex, lastMB = fs.selectMsgBlockWithIndex(maxSeq)
	}
	// Make sure non-nil
	if lastMB == nil {
		return nil, nil
	}

	// Find a starting point for the backward scan over matching subjects.
	subs := make(map[string]*psi)
	var maxBlk uint32

	for _, filter := range filters {
		fs.psim.Match(stringToBytes(filter), func(subj []byte, psi *psi) {
			subs[string(subj)] = psi
			if psi.lblk > maxBlk {
				maxBlk = psi.lblk
			}
		})
	}

	if len(subs) == 0 {
		return nil, nil
	}

	// Collect all sequences needed.
	seqs := make([]uint64, 0, len(subs))

	// If covering the whole stream, check fast path of visiting only last blocks.
	if maxSeq >= fs.state.LastSeq {
		if maxAllowed > 0 && len(subs) > maxAllowed {
			return nil, ErrTooManyResults
		}
		if err := fs.multiLastSeqsByLastBlockLocked(subs, &seqs); err != nil {
			return nil, err
		}
	}

	for i := lastBlkIndex; i >= 0; i-- {
		if len(subs) == 0 {
			break
		}
		mb := fs.blks[i]
		if mb.index > maxBlk {
			continue
		}
		// We can start properly looking here.
		mb.mu.Lock()
		var ierr error
		if ierr = mb.ensurePerSubjectInfoLoaded(); ierr != nil {
			mb.mu.Unlock()
			return nil, ierr
		}

		// Iterate the fss and check against our subs. We will delete from subs as we add.
		// Once len(subs) == 0 we are done.
		mb.fss.IterFast(func(bsubj []byte, ss *SimpleState) bool {
			// Already been processed and accounted for was not matched in the first place.
			if subs[string(bsubj)] == nil {
				return true
			}
			// Check if we need to recalculate. We only care about the last sequence.
			if ss.lastNeedsUpdate {
				// mb is already loaded into the cache so should be fast-ish.
				if ierr = mb.recalculateForSubj(bytesToString(bsubj), ss); ierr != nil {
					return false
				}
			}
			// If we are equal or below just add to seqs slice.
			if ss.Last <= maxSeq {
				seqs = append(seqs, ss.Last)
				delete(subs, bytesToString(bsubj))
			} else {
				// Need to search for the real last since recorded last is > maxSeq.
				needsCleanup := mb.cache == nil
				if mb.cacheNotLoaded() {
					if ierr = mb.loadMsgsWithLock(); ierr != nil {
						if needsCleanup {
							mb.finishedWithCache()
						}
						return false
					}
				}
				var smv StoreMsg
				fseq := atomic.LoadUint64(&mb.first.seq)
				lseq := min(atomic.LoadUint64(&mb.last.seq), maxSeq)
				ssubj := bytesToString(bsubj)
				for seq := lseq; seq >= fseq; seq-- {
					sm, _ := mb.cacheLookupNoCopy(seq, &smv)
					if sm == nil || sm.subj != ssubj {
						continue
					}
					seqs = append(seqs, sm.seq)
					delete(subs, ssubj)
					break
				}
				if needsCleanup {
					mb.finishedWithCache()
				}
			}
			return true
		})
		mb.mu.Unlock()
		if ierr != nil {
			return nil, ierr
		}

		// If maxAllowed was sepcified check that we will not exceed that.
		if maxAllowed > 0 && len(seqs) > maxAllowed {
			return nil, ErrTooManyResults
		}
	}
	if len(seqs) == 0 {
		return nil, nil
	}
	slices.Sort(seqs)
	return seqs, nil
}

// MultiLastMsgs delivers the last message per subject matching the filters,
// up to maxSeq and skipping sequences below minSeq, in ascending sequence
// order through cb, all within a single read section so the batch is a
// point-in-time snapshot of the store.
func (fs *fileStore) MultiLastMsgs(filters []string, minSeq, maxSeq uint64, maxAllowed int, cb func(sm *StoreMsg, np uint64) bool) (uint64, uint64, error) {
	fs.mu.RLock()
	defer fs.mu.RUnlock()

	seqs, err := fs.multiLastSeqsLocked(filters, maxSeq, maxAllowed)
	if err != nil || len(seqs) == 0 {
		return 0, 0, err
	}
	total := uint64(len(seqs))
	np := total
	for _, seq := range seqs {
		if np > 0 {
			np--
		}
		if seq < minSeq {
			continue
		}
		var svp StoreMsg
		sm, err := fs.msgForSeqLocked(seq, &svp, false)
		if err != nil {
			return total, np, err
		}
		if !cb(sm, np) {
			break
		}
	}
	return total, np, nil
}

// multiLastSeqsByLastBlockLocked resolves last sequences via each subject's
// last block (psi.lblk), moving resolved subjects from subs into seqs and
// leaving unresolved ones (e.g. stale lblk) in subs for the caller.
// Lock should be held.
func (fs *fileStore) multiLastSeqsByLastBlockLocked(subs map[string]*psi, seqs *[]uint64) error {
	// Sort requested subjects by their last block so each block is visited
	// once and in order, one flat slice instead of per-block map bookkeeping.
	type lblkSubj struct {
		lblk uint32
		subj string
		info *psi
	}
	pairs := make([]lblkSubj, 0, len(subs))
	for subj, info := range subs {
		pairs = append(pairs, lblkSubj{info.lblk, subj, info})
	}
	slices.SortFunc(pairs, func(a, b lblkSubj) int {
		return cmp.Compare(a.lblk, b.lblk)
	})

	var unresolved []lblkSubj
	for i := 0; i < len(pairs); {
		blkIdx := pairs[i].lblk
		j := i + 1
		for j < len(pairs) && pairs[j].lblk == blkIdx {
			j++
		}
		mb := fs.bim[blkIdx]
		if mb == nil {
			unresolved = append(unresolved, pairs[i:j]...)
			i = j
			continue
		}
		mb.mu.Lock()
		if err := mb.ensurePerSubjectInfoLoaded(); err != nil {
			mb.mu.Unlock()
			return err
		}
		for ; i < j; i++ {
			subj := pairs[i].subj
			ss, ok := mb.fss.Find(stringToBytes(subj))
			if !ok || ss == nil {
				unresolved = append(unresolved, pairs[i])
				continue
			}
			// Check if we need to recalculate. We only care about the last sequence.
			if ss.lastNeedsUpdate {
				if err := mb.recalculateForSubj(subj, ss); err != nil {
					mb.mu.Unlock()
					return err
				}
			}
			*seqs = append(*seqs, ss.Last)
		}
		mb.mu.Unlock()
	}

	// Shrink subs down to only the unresolved subjects, clearing wholesale
	// rather than deleting per resolved subject.
	if len(unresolved) < len(subs) {
		clear(subs)
		for _, p := range unresolved {
			subs[p.subj] = p.info
		}
	}
	return nil
}

// NumPending will return the number of pending messages matching the filter subject starting at sequence.
// Optimized for stream num pending calculations for consumers.
func (fs *fileStore) NumPending(sseq uint64, filter string, lastPerSubject bool) (total, validThrough uint64, err error) {
	fs.mu.RLock()
	defer fs.mu.RUnlock()

	// This can always be last for these purposes.
	validThrough = fs.state.LastSeq

	if fs.state.Msgs == 0 || sseq > fs.state.LastSeq {
		return 0, validThrough, nil
	}

	isAll := filter == _EMPTY_ || filter == fwcs
	if isAll && filter == _EMPTY_ {
		filter = fwcs
	}
	wc := subjectHasWildcard(filter)

	// Resolve exact filters once, including those with no retained messages.
	var info *psi
	if !isAll && !wc {
		var ok bool
		if info, ok = fs.psim.Find(stringToBytes(filter)); !ok {
			return 0, validThrough, nil
		}
	}

	// When starting at the beginning, count directly from stream and subject state.
	// No message blocks need to be loaded, even if there are sequence gaps.
	if sseq <= fs.state.FirstSeq {
		if isAll {
			if lastPerSubject {
				return uint64(fs.psim.Size()), validThrough, nil
			}
			return fs.state.Msgs, validThrough, nil
		}
		if !wc {
			if lastPerSubject {
				return 1, validThrough, nil
			}
			return info.total, validThrough, nil
		}
		fs.psim.Match(stringToBytes(filter), func(_ []byte, info *psi) {
			if lastPerSubject {
				total++
			} else {
				total += info.total
			}
		})
		return total, validThrough, nil
	}

	// See if filter was provided but its the only subject.
	if !isAll && !wc && fs.psim.Size() == 1 {
		isAll = true
	}
	// If we are isAll and have no deleted we can do a simpler calculation.
	if !lastPerSubject && isAll && (fs.state.LastSeq-fs.state.FirstSeq+1) == fs.state.Msgs {
		return fs.state.LastSeq - sseq + 1, validThrough, nil
	}

	// The remaining paths start after FirstSeq and need the starting block.
	seqStart, _ := fs.selectMsgBlockWithIndex(sseq)
	// This should not, but can return -1, so make sure we check to avoid panic below.
	if seqStart < 0 {
		seqStart = 0
	}

	// Block indexes can have gaps, and fblk can lag after deletions. Search the
	// retained blocks for the subject's conservative bounds rather than requiring
	// the indexed blocks to still exist.
	blkStart, blkEnd := 0, len(fs.blks)
	if info != nil {
		blkStart = sort.Search(len(fs.blks), func(i int) bool { return fs.blks[i].index >= info.fblk })
		blkEnd = sort.Search(len(fs.blks), func(i int) bool { return fs.blks[i].index > info.lblk })
		if seqStart >= blkEnd {
			return 0, validThrough, nil
		}
		if seqStart < blkStart {
			if lastPerSubject {
				return 1, validThrough, nil
			}
			return info.total, validThrough, nil
		}
	}

	_tsa, _fsa := [32]string{}, [32]string{}
	tsa, fsa := _tsa[:0], _fsa[:0]
	if wc {
		fsa = tokenizeSubjectIntoSlice(fsa[:0], filter)
	}

	isMatch := func(subj string) bool {
		if isAll {
			return true
		}
		if !wc {
			return subj == filter
		}
		tsa = tokenizeSubjectIntoSlice(tsa[:0], subj)
		return isSubsetMatchTokenized(tsa, fsa)
	}

	// Use the per-subject index when at most one message per subject should be counted.
	// This applies to last-per-subject consumers and streams limited to one message per subject.
	// We will scan PSIM since we accurately track the last block we have seen the subject in. This
	// allows us to only need to load at most one block now.
	// For the last block, we need to track the subjects that we know are in that block, and track seen
	// while in the block itself, but complexity there worth it.
	if lastPerSubject || fs.cfg.MaxMsgsPer == 1 {
		// If we are here we need to scan. We are going to scan the PSIM looking for lblks that are >= seqStart.
		// This will build up a list of all subjects from the selected block onward.
		lbm := make(map[string]bool)
		mb := fs.blks[seqStart]
		bi := mb.index

		fs.psim.Match(stringToBytes(filter), func(subj []byte, psi *psi) {
			// If the select blk start is greater than entry's last blk skip.
			if bi > psi.lblk {
				return
			}
			total++
			// We will track the subjects that are an exact match to the last block.
			// This is needed for last block processing.
			if psi.lblk == bi {
				lbm[string(subj)] = true
			}
		})

		// Subjects whose last block is later need no correction in this block.
		if len(lbm) == 0 {
			return total, validThrough, nil
		}

		// Now check if we need to inspect the seqStart block.
		// Grab write lock in case we need to load in msgs.
		mb.mu.Lock()
		var updateLLTS bool
		var shouldExpire bool
		// We need to walk this block to correct accounting from above.
		if sseq > mb.first.seq {
			// Track the ones we add back in case more than one.
			seen := make(map[string]bool)
			// We need to discount the total by subjects seen before sseq, but also add them right back in if they are >= sseq for this blk.
			// This only should be subjects we know have the last blk in this block.
			if mb.cacheNotLoaded() {
				if err = mb.loadMsgsWithLock(); err != nil {
					mb.mu.Unlock()
					return 0, 0, err
				}
				shouldExpire = true
			}
			var smv StoreMsg
			for seq, lseq := atomic.LoadUint64(&mb.first.seq), atomic.LoadUint64(&mb.last.seq); seq <= lseq; seq++ {
				if end, deleted := mb.dmap.SpanEnd(seq); deleted {
					// Skip the deleted span without calling cacheLookup, which hits time.Now().
					updateLLTS = true
					if end >= lseq {
						break
					}
					seq = end
					continue
				}
				sm, _ := mb.cacheLookupNoCopy(seq, &smv)
				if sm == nil || sm.subj == _EMPTY_ || !lbm[sm.subj] {
					continue
				}
				updateLLTS = false // cacheLookup already updated it.
				if isMatch(sm.subj) {
					// If less than sseq adjust off of total as long as this subject matched the last block.
					if seq < sseq {
						if !seen[sm.subj] {
							total--
							seen[sm.subj] = true
						}
					} else if seen[sm.subj] {
						// This is equal or more than sseq, so add back in.
						total++
						// Make sure to not process anymore.
						delete(seen, sm.subj)
					}
				}
			}
		}
		// If we loaded the block try to force expire.
		if shouldExpire {
			mb.tryForceExpireCacheLocked()
		} else {
			mb.finishedWithCache()
		}
		if updateLLTS {
			mb.llts = ats.AccessTime()
		}
		mb.mu.Unlock()
		return total, validThrough, nil
	}

	// If we would need to scan more from the beginning, revert back to calculating directly here.
	// TODO(dlc) - Redo properly with sublists etc for subject-based filtering.
	if seqStart >= (len(fs.blks) / 2) {
		for i := max(seqStart, blkStart); i < blkEnd; i++ {
			var shouldExpire bool
			var updateLLTS bool
			mb := fs.blks[i]
			// Hold write lock in case we need to load cache.
			mb.mu.Lock()
			if isAll && sseq <= atomic.LoadUint64(&mb.first.seq) {
				total += mb.msgs
				mb.mu.Unlock()
				continue
			}
			// If we are here we need to at least scan the subject fss.
			// Make sure we have fss loaded.
			if mb.fssNotLoaded() {
				if err = mb.loadMsgsWithLock(); err != nil {
					mb.mu.Unlock()
					return 0, 0, err
				}
				shouldExpire = true
			}
			// Mark fss activity.
			mb.lsts = ats.AccessTime()

			var t uint64
			var ierr error
			var havePartial bool
			mb.fss.Match(stringToBytes(filter), func(bsubj []byte, ss *SimpleState) {
				if ierr != nil {
					return
				}
				if havePartial {
					// If we already found a partial then don't do anything else.
					return
				}
				subj := bytesToString(bsubj)
				if ss.firstNeedsUpdate || ss.lastNeedsUpdate {
					if ierr = mb.recalculateForSubj(subj, ss); ierr != nil {
						return
					}
				}
				if sseq <= ss.First {
					t += ss.Msgs
				} else if sseq <= ss.Last {
					// We matched but its a partial.
					havePartial = true
				}
			})
			if ierr != nil {
				mb.mu.Unlock()
				return 0, 0, ierr
			}

			// See if we need to scan msgs here.
			if havePartial {
				// Make sure we have the cache loaded.
				if mb.cacheNotLoaded() {
					if err = mb.loadMsgsWithLock(); err != nil {
						mb.mu.Unlock()
						return 0, 0, err
					}
					shouldExpire = true
				}
				// Clear on partial.
				t = 0
				start := sseq
				if fseq := atomic.LoadUint64(&mb.first.seq); fseq > start {
					start = fseq
				}
				var smv StoreMsg
				for seq, lseq := start, atomic.LoadUint64(&mb.last.seq); seq <= lseq; seq++ {
					if end, deleted := mb.dmap.SpanEnd(seq); deleted {
						updateLLTS = true
						if end >= lseq {
							break
						}
						seq = end
						continue
					}
					if sm, _ := mb.cacheLookupNoCopy(seq, &smv); sm != nil && isMatch(sm.subj) {
						t++
						updateLLTS = false // cacheLookup already updated it.
					}
				}
			}
			// If we loaded this block for this operation go ahead and expire it here.
			if shouldExpire {
				mb.tryForceExpireCacheLocked()
			} else {
				mb.finishedWithCache()
			}
			if updateLLTS {
				mb.llts = ats.AccessTime()
			}
			mb.mu.Unlock()
			total += t
		}
		return total, validThrough, nil
	}

	// If we are here it's better to calculate totals from psim and adjust downward by scanning less blocks.
	// TODO(dlc) - Eventually when sublist uses generics, make this sublist driven instead.
	start := uint32(math.MaxUint32)
	if info != nil {
		total, start = info.total, info.fblk
	} else {
		fs.psim.Match(stringToBytes(filter), func(_ []byte, psi *psi) {
			total += psi.total
			// Keep track of start index for this subject.
			if psi.fblk < start {
				start = psi.fblk
			}
		})
	}

	// If we are here we need to calculate partials for the first blocks.
	firstSubjBlk := fs.bim[start]
	var firstSubjBlkFound bool
	// Adjust in case not found.
	if firstSubjBlk == nil {
		firstSubjBlkFound = true
	}

	// Track how many we need to adjust against the total.
	var adjust uint64
	for i := blkStart; i <= seqStart && i < blkEnd; i++ {
		mb := fs.blks[i]
		// We can skip blks if we know they are below the first one that has any subject matches.
		if !firstSubjBlkFound {
			if firstSubjBlkFound = (mb == firstSubjBlk); !firstSubjBlkFound {
				continue
			}
		}
		// We need to scan this block.
		var shouldExpire bool
		var updateLLTS bool
		mb.mu.Lock()
		// Check if we should include all of this block in adjusting. If so work with metadata.
		if sseq > atomic.LoadUint64(&mb.last.seq) {
			if isAll {
				adjust += mb.msgs
			} else {
				// We need to adjust for all matches in this block.
				// Make sure we have fss loaded. This loads whole block now.
				if mb.fssNotLoaded() {
					if err = mb.loadMsgsWithLock(); err != nil {
						mb.mu.Unlock()
						return 0, 0, err
					}
					shouldExpire = true
				}
				// Mark fss activity.
				mb.lsts = ats.AccessTime()

				mb.fss.Match(stringToBytes(filter), func(bsubj []byte, ss *SimpleState) {
					adjust += ss.Msgs
				})
			}
		} else {
			// This is the last block. We need to scan per message here.
			if mb.cacheNotLoaded() {
				if err = mb.loadMsgsWithLock(); err != nil {
					mb.mu.Unlock()
					return 0, 0, err
				}
				shouldExpire = true
			}
			var last = atomic.LoadUint64(&mb.last.seq)
			if sseq < last {
				last = sseq
			}
			// We need to walk all messages in this block
			var smv StoreMsg
			for seq := atomic.LoadUint64(&mb.first.seq); seq < last; seq++ {
				if end, deleted := mb.dmap.SpanEnd(seq); deleted {
					// Skip the deleted span without calling cacheLookup, which hits time.Now().
					updateLLTS = true
					if end >= last {
						break
					}
					seq = end
					continue
				}
				sm, _ := mb.cacheLookupNoCopy(seq, &smv)
				if sm == nil || sm.subj == _EMPTY_ {
					continue
				}
				updateLLTS = false // cacheLookup already updated it.
				// Check if it matches our filter.
				if sm.seq < sseq && isMatch(sm.subj) {
					adjust++
				}
			}
		}
		// If we loaded the block try to force expire.
		if shouldExpire {
			mb.tryForceExpireCacheLocked()
		} else {
			mb.finishedWithCache()
		}
		if updateLLTS {
			mb.llts = ats.AccessTime()
		}
		mb.mu.Unlock()
	}
	// Make final adjustment.
	if adjust > total {
		total = 0
	} else {
		total -= adjust
	}

	return total, validThrough, nil
}

// NumPending will return the number of pending messages matching any subject in the sublist starting at sequence.
// Optimized for stream num pending calculations for consumers with lots of filtered subjects.
// Subjects should not overlap, this property is held when doing multi-filtered consumers.
func (fs *fileStore) NumPendingMulti(sseq uint64, sl *gsl.SimpleSublist, lastPerSubject bool) (total, validThrough uint64, err error) {
	fs.mu.RLock()
	defer fs.mu.RUnlock()

	// This can always be last for these purposes.
	validThrough = fs.state.LastSeq

	if fs.state.Msgs == 0 || sseq > fs.state.LastSeq {
		return 0, validThrough, nil
	}

	isAll := sl == nil

	// When starting at the beginning, count directly from stream and subject state.
	// No message blocks need to be loaded, even if there are sequence gaps.
	if sseq <= fs.state.FirstSeq {
		if isAll {
			if lastPerSubject {
				return uint64(fs.psim.Size()), validThrough, nil
			}
			return fs.state.Msgs, validThrough, nil
		}
		stree.IntersectGSL(fs.psim, sl, func(_ []byte, info *psi) bool {
			if lastPerSubject {
				total++
			} else {
				total += info.total
			}
			return true
		})
		return total, validThrough, nil
	}

	// See if filter was provided but its the only subject.
	if !isAll && fs.psim.Size() == 1 {
		fs.psim.IterFast(func(subject []byte, _ *psi) bool {
			isAll = sl.HasInterest(bytesToString(subject))
			return true
		})
	}
	// If we are isAll and have no deleted we can do a simpler calculation.
	if !lastPerSubject && isAll && (fs.state.LastSeq-fs.state.FirstSeq+1) == fs.state.Msgs {
		return fs.state.LastSeq - sseq + 1, validThrough, nil
	}

	// The remaining paths start after FirstSeq and need the starting block.
	seqStart, _ := fs.selectMsgBlockWithIndex(sseq)
	// This should not, but can return -1, so make sure we check to avoid panic below.
	if seqStart < 0 {
		seqStart = 0
	}

	// Resolve the matching subjects' conservative block bounds once. Block
	// indexes can have gaps, and first-block tracking can lag after deletions.
	blkStart, blkEnd := 0, len(fs.blks)
	start, stop := uint32(math.MaxUint32), uint32(0)
	var matchedMsgs, matchedSubjects, pendingSubjects uint64
	var lbm map[string]bool
	if lastPerSubject || fs.cfg.MaxMsgsPer == 1 {
		lbm = make(map[string]bool)
	}
	if sl != nil {
		bi := fs.blks[seqStart].index
		stree.IntersectGSL(fs.psim, sl, func(subj []byte, info *psi) bool {
			matchedMsgs += info.total
			matchedSubjects++
			start, stop = min(start, info.fblk), max(stop, info.lblk)
			if lbm != nil && info.lblk >= bi {
				pendingSubjects++
				if info.lblk == bi {
					lbm[string(subj)] = true
				}
			}
			return true
		})
		if matchedSubjects == 0 {
			return 0, validThrough, nil
		}
		blkStart = sort.Search(len(fs.blks), func(i int) bool { return fs.blks[i].index >= start })
		blkEnd = sort.Search(len(fs.blks), func(i int) bool { return fs.blks[i].index > stop })
		if seqStart >= blkEnd {
			return 0, validThrough, nil
		}
		if seqStart < blkStart {
			if lastPerSubject {
				return matchedSubjects, validThrough, nil
			}
			return matchedMsgs, validThrough, nil
		}
	}

	// Setup the isMatch function.
	isMatch := func(subj string) bool {
		if isAll {
			return true
		}
		return sl.HasInterest(subj)
	}

	// Use the per-subject index when at most one message per subject should be counted.
	// This applies to last-per-subject consumers and streams limited to one message per subject.
	// We will scan PSIM since we accurately track the last block we have seen the subject in. This
	// allows us to only need to load at most one block now.
	// For the last block, we need to track the subjects that we know are in that block, and track seen
	// while in the block itself, but complexity there worth it.
	if lastPerSubject || fs.cfg.MaxMsgsPer == 1 {
		// If we are here we need to scan. We are going to scan the PSIM looking for lblks that are >= seqStart.
		// This will build up a list of all subjects from the selected block onward.
		mb := fs.blks[seqStart]
		bi := mb.index

		if sl != nil {
			total = pendingSubjects
		} else {
			fs.psim.IterFast(func(subj []byte, info *psi) bool {
				if info.lblk >= bi {
					total++
					if info.lblk == bi {
						lbm[string(subj)] = true
					}
				}
				return true
			})
		}
		// Subjects whose last block is later need no correction in this block.
		if len(lbm) == 0 {
			return total, validThrough, nil
		}

		// Now check if we need to inspect the seqStart block.
		// Grab write lock in case we need to load in msgs.
		mb.mu.Lock()
		var shouldExpire bool
		var updateLLTS bool
		// We need to walk this block to correct accounting from above.
		if sseq > atomic.LoadUint64(&mb.first.seq) {
			// Track the ones we add back in case more than one.
			seen := make(map[string]bool)
			// We need to discount the total by subjects seen before sseq, but also add them right back in if they are >= sseq for this blk.
			// This only should be subjects we know have the last blk in this block.
			if mb.cacheNotLoaded() {
				if err = mb.loadMsgsWithLock(); err != nil {
					mb.mu.Unlock()
					return 0, 0, err
				}
				shouldExpire = true
			}
			var smv StoreMsg
			for seq, lseq := atomic.LoadUint64(&mb.first.seq), atomic.LoadUint64(&mb.last.seq); seq <= lseq; seq++ {
				if end, deleted := mb.dmap.SpanEnd(seq); deleted {
					// Skip the deleted span without calling cacheLookup, which hits time.Now().
					updateLLTS = true
					if end >= lseq {
						break
					}
					seq = end
					continue
				}
				sm, _ := mb.cacheLookupNoCopy(seq, &smv)
				if sm == nil || sm.subj == _EMPTY_ || !lbm[sm.subj] {
					continue
				}
				updateLLTS = false // cacheLookup already updated it.
				if isMatch(sm.subj) {
					// If less than sseq adjust off of total as long as this subject matched the last block.
					if seq < sseq {
						if !seen[sm.subj] {
							total--
							seen[sm.subj] = true
						}
					} else if seen[sm.subj] {
						// This is equal or more than sseq, so add back in.
						total++
						// Make sure to not process anymore.
						delete(seen, sm.subj)
					}
				}
			}
		}
		// If we loaded the block try to force expire.
		if shouldExpire {
			mb.tryForceExpireCacheLocked()
		} else {
			mb.finishedWithCache()
		}
		if updateLLTS {
			mb.llts = ats.AccessTime()
		}
		mb.mu.Unlock()
		return total, validThrough, nil
	}

	// If we would need to scan more from the beginning, revert back to calculating directly here.
	if seqStart >= (len(fs.blks) / 2) {
		for i := max(seqStart, blkStart); i < blkEnd; i++ {
			var shouldExpire bool
			mb := fs.blks[i]
			// Hold write lock in case we need to load cache.
			mb.mu.Lock()
			if isAll && sseq <= atomic.LoadUint64(&mb.first.seq) {
				total += mb.msgs
				mb.mu.Unlock()
				continue
			}
			// If we are here we need to at least scan the subject fss.
			// Make sure we have fss loaded.
			if mb.fssNotLoaded() {
				if err = mb.loadMsgsWithLock(); err != nil {
					mb.mu.Unlock()
					return 0, 0, err
				}
				shouldExpire = true
			}
			// Mark fss activity.
			mb.lsts = ats.AccessTime()

			var t uint64
			var ierr error
			var havePartial bool
			var updateLLTS bool
			stree.IntersectGSL[SimpleState](mb.fss, sl, func(bsubj []byte, ss *SimpleState) bool {
				subj := bytesToString(bsubj)
				if ss.firstNeedsUpdate || ss.lastNeedsUpdate {
					if ierr = mb.recalculateForSubj(subj, ss); ierr != nil {
						return false
					}
				}
				if sseq <= ss.First {
					t += ss.Msgs
				} else if sseq <= ss.Last {
					// We matched but its a partial.
					havePartial = true
					return false
				}
				return true
			})
			if ierr != nil {
				mb.mu.Unlock()
				return 0, 0, ierr
			}

			// See if we need to scan msgs here.
			if havePartial {
				// Make sure we have the cache loaded.
				if mb.cacheNotLoaded() {
					if err = mb.loadMsgsWithLock(); err != nil {
						mb.mu.Unlock()
						return 0, 0, err
					}
					shouldExpire = true
				}
				// Clear on partial.
				t = 0
				start := sseq
				if fseq := atomic.LoadUint64(&mb.first.seq); fseq > start {
					start = fseq
				}
				var smv StoreMsg
				for seq, lseq := start, atomic.LoadUint64(&mb.last.seq); seq <= lseq; seq++ {
					if end, deleted := mb.dmap.SpanEnd(seq); deleted {
						// Skip the deleted span without calling cacheLookup, which hits time.Now().
						updateLLTS = true
						if end >= lseq {
							break
						}
						seq = end
						continue
					}
					if sm, _ := mb.cacheLookupNoCopy(seq, &smv); sm != nil && isMatch(sm.subj) {
						t++
						updateLLTS = false // cacheLookup already updated it.
					}
				}
			}
			// If we loaded this block for this operation go ahead and expire it here.
			if shouldExpire {
				mb.tryForceExpireCacheLocked()
			} else {
				mb.finishedWithCache()
			}
			if updateLLTS {
				mb.llts = ats.AccessTime()
			}
			mb.mu.Unlock()
			total += t
		}
		return total, validThrough, nil
	}

	// If we are here it's better to calculate totals from psim and adjust downward by scanning less blocks.
	if sl != nil {
		total = matchedMsgs
	} else {
		total, start = fs.state.Msgs, fs.blks[0].index
	}

	// If we are here we need to calculate partials for the first blocks.
	firstSubjBlk := fs.bim[start]
	var firstSubjBlkFound bool
	// Adjust in case not found.
	if firstSubjBlk == nil {
		firstSubjBlkFound = true
	}

	// Track how many we need to adjust against the total.
	var adjust uint64
	for i := blkStart; i <= seqStart && i < blkEnd; i++ {
		mb := fs.blks[i]
		// We can skip blks if we know they are below the first one that has any subject matches.
		if !firstSubjBlkFound {
			if firstSubjBlkFound = (mb == firstSubjBlk); !firstSubjBlkFound {
				continue
			}
		}
		// We need to scan this block.
		var shouldExpire bool
		var updateLLTS bool
		mb.mu.Lock()
		// Check if we should include all of this block in adjusting. If so work with metadata.
		if sseq > atomic.LoadUint64(&mb.last.seq) {
			if isAll {
				adjust += mb.msgs
			} else {
				// We need to adjust for all matches in this block.
				// Make sure we have fss loaded. This loads whole block now.
				if mb.fssNotLoaded() {
					if err = mb.loadMsgsWithLock(); err != nil {
						mb.mu.Unlock()
						return 0, 0, err
					}
					shouldExpire = true
				}
				// Mark fss activity.
				mb.lsts = ats.AccessTime()
				stree.IntersectGSL(mb.fss, sl, func(bsubj []byte, ss *SimpleState) bool {
					adjust += ss.Msgs
					return true
				})
			}
		} else {
			// This is the last block. We need to scan per message here.
			if mb.cacheNotLoaded() {
				if err = mb.loadMsgsWithLock(); err != nil {
					mb.mu.Unlock()
					return 0, 0, err
				}
				shouldExpire = true
			}
			var last = atomic.LoadUint64(&mb.last.seq)
			if sseq < last {
				last = sseq
			}
			// We need to walk all messages in this block
			var smv StoreMsg
			for seq := atomic.LoadUint64(&mb.first.seq); seq < last; seq++ {
				if end, deleted := mb.dmap.SpanEnd(seq); deleted {
					// Skip the deleted span without calling cacheLookup, which hits time.Now().
					updateLLTS = true
					if end >= last {
						break
					}
					seq = end
					continue
				}
				sm, _ := mb.cacheLookupNoCopy(seq, &smv)
				if sm == nil || sm.subj == _EMPTY_ {
					continue
				}
				updateLLTS = false // cacheLookup already updated it.
				// Check if it matches our filter.
				if sm.seq < sseq && isMatch(sm.subj) {
					adjust++
				}
			}
		}
		// If we loaded the block try to force expire.
		if shouldExpire {
			mb.tryForceExpireCacheLocked()
		} else {
			mb.finishedWithCache()
		}
		if updateLLTS {
			mb.llts = ats.AccessTime()
		}
		mb.mu.Unlock()
	}
	// Make final adjustment.
	if adjust > total {
		total = 0
	} else {
		total -= adjust
	}

	return total, validThrough, nil
}

// SubjectsTotals return message totals per subject.
func (fs *fileStore) SubjectsTotals(filter string) map[string]uint64 {
	fs.mu.RLock()
	defer fs.mu.RUnlock()
	return fs.subjectsTotalsLocked(filter)
}

// Lock should be held.
func (fs *fileStore) subjectsTotalsLocked(filter string) map[string]uint64 {
	if fs.psim.Size() == 0 {
		return nil
	}
	// Match all if no filter given.
	if filter == _EMPTY_ {
		filter = fwcs
	}
	fst := make(map[string]uint64)
	fs.psim.Match(stringToBytes(filter), func(subj []byte, psi *psi) {
		fst[string(subj)] = psi.total
	})
	return fst
}

// ApplySourcesState replaces the tracked sourcing state with the leader's.
func (fs *fileStore) ApplySourcesState(sources map[string]StreamSourceState) {
	if len(sources) == 0 {
		return
	}
	fs.mu.Lock()
	defer fs.mu.Unlock()
	// Nil means no sources are configured, so there is nothing to track.
	if fs.sources == nil {
		return
	}
	clear(fs.sources)
	for name, ss := range sources {
		fs.sources[name] = &StreamSourceState{Seq: ss.Seq, Ident: ss.Ident}
	}
	// Keep every configured source present, seeded as not yet observed, which is
	// the invariant recoverPerMessageState and storeRawMsg expect.
	for _, ssi := range fs.cfg.Sources {
		if iname := ssi.composeIName(); fs.sources[iname] == nil {
			fs.sources[iname] = &StreamSourceState{}
		}
	}
	fs.dirty++
}

// SourcesState return sources keys and their last known state.
func (fs *fileStore) SourcesState() map[string]StreamSourceState {
	fs.mu.RLock()
	defer fs.mu.RUnlock()
	if len(fs.sources) == 0 {
		return nil
	}
	dst := make(map[string]StreamSourceState, len(fs.sources))
	for k, ss := range fs.sources {
		dst[k] = *ss
	}
	return dst
}

// RegisterStorageUpdates registers a callback for updates to storage changes.
// It will present number of messages and bytes as a signed integer and an
// optional sequence number of the message if a single.
func (fs *fileStore) RegisterStorageUpdates(cb StorageUpdateHandler) {
	fs.mu.Lock()
	fs.scb = cb
	bsz := fs.state.Bytes
	fs.mu.Unlock()
	if cb != nil && bsz > 0 {
		cb(0, int64(bsz), 0, _EMPTY_)
	}
}

// RegisterStorageRemoveMsg registers a callback to remove messages.
// Replicated streams should propose removals, R1 can remove inline.
func (fs *fileStore) RegisterStorageRemoveMsg(cb StorageRemoveMsgHandler) {
	fs.mu.Lock()
	fs.rmcb = cb
	fs.mu.Unlock()
}

// RegisterProcessJetStreamMsg registers a callback to process new JetStream messages.
func (fs *fileStore) RegisterProcessJetStreamMsg(cb ProcessJetStreamMsgHandler) {
	fs.mu.Lock()
	fs.pmsgcb = cb
	fs.mu.Unlock()
}

// Helper to get hash key for specific message block.
// Lock should be held
func (fs *fileStore) hashKeyForBlock(index uint32) []byte {
	return []byte(fmt.Sprintf("%s-%d", fs.cfg.Name, index))
}

func (mb *msgBlock) setupWriteCache(buf []byte) error {
	// Not safe to use mb.cacheAlreadyLoaded() here as the heuristic
	// is wrong for writing to the beginning of an empty block cache.
	if mb.cache != nil {
		return nil
	}
	if mb.cache = mb.ecache.Value(); mb.cache != nil {
		return nil
	}

	// If there's an existing block on disk then we'll load it in.
	var fi os.FileInfo
	if mb.mfd != nil {
		fi, _ = mb.mfd.Stat()
	} else if mb.mfn != _EMPTY_ {
		fi, _ = os.Stat(mb.mfn)
	}
	if fi != nil {
		return mb.loadMsgsWithLock()
	}

	// Looks like there isn't an existing file on disk, mint a new cache.
	mb.cache = &cache{buf: buf}
	mb.cache.registerRecycle()
	mb.ecache.Set(mb.cache)
	mb.llts = ats.AccessTime()
	mb.startCacheExpireTimer()
	return nil
}

// finishedWithCache removes the strong reference to the cache, instead
// returning it to the elastic pointer. Note that the elastic pointer can
// be either weak or strong, they are strengthened when there are pending
// writes and weak otherwise.
// Lock must be held.
func (mb *msgBlock) finishedWithCache() {
	if mb.cache != nil && mb.pendingWriteSizeLocked() == 0 {
		mb.cache = nil
	}
}

// This rolls to a new append msg block.
// Lock should be held.
func (fs *fileStore) newMsgBlockForWrite() (*msgBlock, error) {
	index := uint32(1)
	var rbuf []byte

	if lmb := fs.lmb; lmb != nil {
		lmb.mu.Lock()
		index = lmb.index + 1

		// Quit our loops.
		if lmb.qch != nil {
			close(lmb.qch)
			lmb.qch = nil
		}
		// If we had a write error before, don't allow continuing into a new block.
		if err := lmb.werr; err != nil {
			lmb.mu.Unlock()
			return nil, err
		}
		// Flush any pending messages.
		if _, err := lmb.flushPendingMsgsLocked(); err != nil {
			lmb.mu.Unlock()
			return nil, err
		}
		// Determine if we can reclaim any resources here.
		lmb.closeFDsLockedNoCheck()
		if lmb.cache != nil {
			// Reset write timestamp and see if we can expire this cache.
			rbuf = lmb.tryExpireWriteCache()
		}
		lmb.mu.Unlock()

		if fs.fcfg.Compression != NoCompression {
			// We've now reached the end of this message block, if we want
			// to compress blocks then now's the time to do it.
			go func() {
				lmb.mu.Lock()
				defer lmb.mu.Unlock()
				// Might error, but we can't handle it here anyway.
				_ = lmb.recompressOnDiskIfNeeded()
			}()
		}
	}

	mb := fs.initMsgBlock(index)
	// Lock should be held to quiet race detector.
	mb.mu.Lock()
	if err := mb.setupWriteCache(rbuf); err != nil {
		mb.mu.Unlock()
		return nil, err
	}
	mb.fss = stree.NewSubjectTree[SimpleState]()

	// Set cache time to creation time to start.
	mb.llts, mb.lwts = 0, ats.AccessTime()
	// Remember our last sequence number.
	atomic.StoreUint64(&mb.first.seq, fs.state.LastSeq+1)
	atomic.StoreUint64(&mb.last.seq, fs.state.LastSeq)
	mb.mu.Unlock()

	// Now do local hash.
	key := sha256.Sum256(fs.hashKeyForBlock(index))
	hh, err := highwayhash.NewDigest64(key[:])
	if err != nil {
		return nil, fmt.Errorf("could not create hash: %v", err)
	}
	mb.hh = hh

	fs.dios.acquire()
	mfd, err := os.OpenFile(mb.mfn, os.O_CREATE|os.O_RDWR, defaultFilePerms)
	fs.dios.release()

	if err != nil {
		if isPermissionError(err) {
			return nil, err
		}
		_ = mb.dirtyCloseWithRemove(true)
		return nil, fmt.Errorf("Error creating msg block file: %v", err)
	}
	mb.mfd = mfd

	// Check if encryption is enabled.
	if fs.prf != nil {
		if err := fs.genEncryptionKeysForBlock(mb); err != nil {
			return nil, err
		}
	}

	// If we know we will need this so go ahead and spin up.
	if !fs.fip {
		mb.spinUpFlushLoop()
	}

	// Add to our list of blocks and mark as last.
	fs.addMsgBlock(mb)

	return mb, nil
}

// Generate the keys for this message block and write them out.
func (fs *fileStore) genEncryptionKeysForBlock(mb *msgBlock) error {
	if mb == nil {
		return nil
	}
	key, bek, seed, encrypted, err := fs.genEncryptionKeys(fmt.Sprintf("%s:%d", fs.cfg.Name, mb.index))
	if err != nil {
		return err
	}
	mb.aek, mb.bek, mb.seed, mb.nonce = key, bek, seed, encrypted[:key.NonceSize()]
	mdir := filepath.Join(fs.fcfg.StoreDir, msgDir)
	keyFile := filepath.Join(mdir, fmt.Sprintf(keyScan, mb.index))
	if _, err := os.Stat(keyFile); err != nil && !os.IsNotExist(err) {
		return err
	}
	sync := fs.syncAlways.Load() || fs.syncOnFlush.Load()
	err = writeAtomically(fs.dios, keyFile, encrypted, defaultFilePerms, sync)
	if err != nil {
		return err
	}
	mb.kfn = keyFile
	// If we did not sync the key file above, mark it to be synced on the next syncBlocks pass.
	mb.needKeySync = !sync
	return nil
}

// Stores a raw message with expected sequence number and timestamp.
// Lock should be held.
func (fs *fileStore) storeRawMsg(subj string, hdr, msg []byte, seq uint64, ts, ttl int64, discardNewCheck bool) (err error) {
	if fs.isClosed() {
		return ErrStoreClosed
	}

	// Per subject max check needed.
	mmp := uint64(fs.cfg.MaxMsgsPer)
	var psmc uint64
	psmax := mmp > 0 && len(subj) > 0
	var info *psi
	if psmax {
		if info, _ = fs.psim.Find(stringToBytes(subj)); info != nil {
			// Take current total, but add 1 for the message we are about to store.
			psmc = info.total + 1
		}
	}

	var fseq uint64
	// Check if we are discarding new messages when we reach the limit.
	// If we are clustered, we do the enforcement above and should not disqualify
	// the message here since it could cause replicas to drift.
	if discardNewCheck && fs.cfg.Discard == DiscardNew {
		var asl bool
		if psmax && psmc > mmp {
			// If we are instructed to discard new per subject, this is an error.
			// However, allow rollup messages through since they will purge old
			// messages for the subject after storing, restoring the limit.
			if fs.cfg.DiscardNewPer && len(sliceHeader(JSMsgRollup, hdr)) == 0 {
				return ErrMaxMsgsPerSubject
			}
			if fseq, err = fs.firstSeqForSubj(subj); err != nil {
				return err
			}
			// fs.firstSeqForSubj releases and re-acquires the lock, need to fetch the state again.
			if info, _ = fs.psim.Find(stringToBytes(subj)); info != nil {
				// Take current total, but add 1 for the message we are about to store.
				psmc = info.total + 1
			}
			asl = psmc > mmp
		}
		if fs.cfg.MaxMsgs > 0 && fs.state.Msgs >= uint64(fs.cfg.MaxMsgs) && !asl {
			return ErrMaxMsgs
		}
		if fs.cfg.MaxBytes > 0 && fs.state.Bytes+fileStoreMsgSize(subj, hdr, msg) > uint64(fs.cfg.MaxBytes) {
			if !asl || fs.sizeForSeq(fseq) < int(fileStoreMsgSize(subj, hdr, msg)) {
				return ErrMaxBytes
			}
		}
	}

	// Check sequence.
	if seq != fs.state.LastSeq+1 {
		if seq > 0 {
			return ErrSequenceMismatch
		}
		seq = fs.state.LastSeq + 1
	}

	// Return previous write errors immediately.
	if fs.werr != nil {
		return fs.werr
	}
	// Persist any returned errors to be used in the future.
	defer func() {
		if err != nil {
			fs.setWriteErr(err)
		}
	}()

	// Write msg record.
	// Add expiry bit to sequence if needed. This is so that if we need to
	// rebuild, we know which messages to look at more quickly.
	n, err := fs.writeMsgRecord(seq, ts, subj, hdr, msg)
	if err != nil {
		return err
	}

	// Adjust top level tracking of per subject msg counts.
	if len(subj) > 0 && fs.psim != nil {
		index := fs.lmb.index
		if info == nil {
			info, _ = fs.psim.Find(stringToBytes(subj))
		}
		if info != nil {
			info.total++
			if index > info.lblk {
				info.lblk = index
			}
		} else {
			info, _ = fs.psim.Insert(stringToBytes(subj), psi{total: 1, fblk: index, lblk: index})
			fs.tsl += len(subj)
		}
	}

	// Adjust first if needed.
	now := time.Unix(0, ts).UTC()
	if fs.state.Msgs == 0 {
		fs.state.FirstSeq = seq
		fs.state.FirstTime = now
	}

	fs.state.Msgs++
	fs.state.Bytes += n
	fs.state.LastSeq = seq
	fs.state.LastTime = now

	// Enforce per message limits.
	for psmax && psmc > mmp {
		// We may have done this above.
		if fseq == 0 {
			fseq, err = fs.firstSeqForSubj(subj)
			if err != nil {
				return err
			} else if fseq == 0 {
				break
			}
			// fs.firstSeqForSubj releases and re-acquires the lock, need to fetch the state again.
			if info, _ = fs.psim.Find(stringToBytes(subj)); info != nil {
				psmc = info.total
			} else {
				break
			}
			// Re-check if we're at the limit.
			continue
		}
		if _, err = fs.removeMsgViaLimits(fseq); err != nil && err != ErrStoreMsgNotFound {
			return err
		}
		fseq = 0
		// fs.removeMsgViaLimits releases and re-acquires the lock, need to fetch the state again.
		if info, _ = fs.psim.Find(stringToBytes(subj)); info != nil {
			psmc = info.total
		} else {
			break
		}
	}
	// If we only ever store one/last message for a subject, can correct the first block to where we've just written.
	if info != nil && info.total == 1 && mmp == 1 {
		info.fblk = info.lblk
	}

	// Limits checks and enforcement.
	// If they do any deletions they will update the
	// byte count on their own, so no need to compensate.
	if err = fs.enforceMsgLimit(); err != nil {
		return err
	}
	if err = fs.enforceBytesLimit(); err != nil {
		return err
	}

	// Per-message TTL.
	if ttl > 0 {
		if fs.ttls != nil {
			expires := time.Duration(ts) + (time.Second * time.Duration(ttl))
			fs.ttls.Add(seq, int64(expires))
		}
		// Need to count these regardless as we might want to enable TTLs
		// later via UpdateConfig.
		fs.lmb.mu.Lock()
		fs.lmb.ttls++
		fs.lmb.mu.Unlock()
	}

	// Check if we have and need the age expiration timer running.
	switch {
	case fs.ttls != nil && ttl > 0:
		fs.resetAgeChk(0)
	case fs.ageChk == nil && (fs.cfg.MaxAge > 0 || fs.ttls != nil):
		fs.startAgeChk()
	}

	// Message scheduling.
	if fs.scheduling != nil {
		if schedule, apiErr := nextMessageSchedule(hdr, ts); apiErr == nil && !schedule.IsZero() {
			fs.scheduling.add(seq, subj, schedule.UnixNano())
			fs.lmb.mu.Lock()
			fs.lmb.schedules++
			fs.lmb.mu.Unlock()
		} else if getMessageScheduler(hdr) == _EMPTY_ {
			fs.scheduling.removeSubject(subj)
		}

		// Check for a repeating schedule and update such that it triggers again.
		if scheduleNext := bytesToString(sliceHeader(JSScheduleNext, hdr)); scheduleNext != _EMPTY_ && scheduleNext != JSScheduleNextPurge {
			scheduler := getMessageScheduler(hdr)
			if next, err := time.Parse(time.RFC3339Nano, scheduleNext); err == nil && scheduler != _EMPTY_ {
				fs.scheduling.update(scheduler, next.UnixNano())
			}
		}
	}

	// Track sources.
	if len(fs.cfg.Sources) > 0 {
		if ss := sliceHeader(JSStreamSource, hdr); len(ss) > 0 {
			_, indexName, sseq, ident := streamAndSeq(bytesToString(ss))
			if sss, ok := fs.sources[indexName]; ok {
				sss.Seq, sss.Ident = sseq, ident
			}
		}
	}

	return nil
}

// isReadErr reports whether err originated from reading or interpreting
// existing on-disk data rather than from a failed write. These surface through
// cache/load paths and should not permanently disable writes.
func isReadErr(err error) bool {
	var badMsg errBadMsg
	return errors.Is(err, errNoCache) ||
		errors.Is(err, errDeletedMsg) ||
		errors.Is(err, errPartialCache) ||
		errors.Is(err, errCorruptState) ||
		errors.Is(err, errPriorState) ||
		errors.Is(err, io.ErrUnexpectedEOF) ||
		errors.Is(err, errMsgBlkTooBig) ||
		errors.As(err, &badMsg)
}

// Lock should be held.
func (fs *fileStore) setWriteErr(err error) {
	if fs.werr != nil {
		return
	}
	// Ignore non-write errors.
	if err == ErrStoreClosed {
		return
	}
	// If this is a not found report but do not disable.
	if os.IsNotExist(err) {
		fs.warn("Resource not found: %v", err)
		return
	}
	// Read/decode errors surfaced from existing on-disk data are not write failures.
	// Log and continue instead of disabling writes.
	if isReadErr(err) {
		fs.rateLimitWarn("Ignoring non-write error: %v", err)
		assert.Unreachable("Filestore encountered read error", map[string]any{
			"name":  fs.cfg.Name,
			"err":   err,
			"stack": string(debug.Stack()),
		})
		return
	}
	fs.error("Critical write error: %v", err)
	fs.werr = err
	assert.Unreachable("Filestore encountered write error", map[string]any{
		"name":  fs.cfg.Name,
		"err":   err,
		"stack": string(debug.Stack()),
	})
}

// StoreRawMsg stores a raw message with expected sequence number and timestamp.
func (fs *fileStore) StoreRawMsg(subj string, hdr, msg []byte, seq uint64, ts, ttl int64, discardNewCheck bool) error {
	fs.mu.Lock()
	// Always return previous write errors.
	if err := fs.werr; err != nil {
		fs.mu.Unlock()
		return err
	}
	err := fs.storeRawMsg(subj, hdr, msg, seq, ts, ttl, discardNewCheck)
	cb := fs.scb
	// Check if first message timestamp requires expiry
	// sooner than initial replica expiry timer set to MaxAge when initializing.
	if !fs.receivedAny && fs.cfg.MaxAge != 0 && ts > 0 {
		fs.receivedAny = true
		fs.resetAgeChk(0)
	}
	fs.mu.Unlock()

	if err == nil && cb != nil {
		cb(1, int64(fileStoreMsgSize(subj, hdr, msg)), seq, subj)
	}

	return err
}

// Store stores a message. We hold the main filestore lock for any write operation.
func (fs *fileStore) StoreMsg(subj string, hdr, msg []byte, ttl int64) (uint64, int64, error) {
	fs.mu.Lock()
	// Always return previous write errors.
	if err := fs.werr; err != nil {
		fs.mu.Unlock()
		return 0, 0, err
	}
	seq, ts := fs.state.LastSeq+1, time.Now().UnixNano()
	// This is called for a R1 with no expected sequence number, so perform DiscardNew checks on the store-level.
	err := fs.storeRawMsg(subj, hdr, msg, seq, ts, ttl, true)
	cb := fs.scb
	fs.mu.Unlock()

	if err != nil {
		seq, ts = 0, 0
	} else if cb != nil {
		cb(1, int64(fileStoreMsgSize(subj, hdr, msg)), seq, subj)
	}

	return seq, ts, err
}

// skipMsg will update this message block for a skipped message.
// If we do not have any messages, just update the metadata, otherwise
// we will place an empty record marking the sequence as used. The
// sequence will be marked erased.
// fs lock should be held.
func (mb *msgBlock) skipMsg(seq uint64, ts int64) error {
	if mb == nil {
		return nil
	}
	var needsRecord bool

	mb.mu.Lock()
	if err := mb.werr; err != nil {
		mb.mu.Unlock()
		return err
	}
	// If we are empty can just do meta.
	if mb.msgs == 0 {
		atomic.StoreUint64(&mb.last.seq, seq)
		mb.last.ts = ts
		atomic.StoreUint64(&mb.first.seq, seq+1)
		mb.first.ts = 0
		needsRecord = mb == mb.fs.lmb
	} else {
		needsRecord = true
		mb.dmap.Insert(seq)
	}
	if needsRecord {
		if err := mb.writeMsgRecordLocked(emptyRecordLen, seq|ebit, _EMPTY_, nil, nil, ts, true, true); err != nil {
			mb.mu.Unlock()
			return err
		}
	}
	mb.mu.Unlock()
	if !needsRecord {
		mb.kickFlusher()
	}
	return nil
}

// SkipMsg will use the next sequence number but not store anything.
func (fs *fileStore) SkipMsg(seq uint64) (uint64, error) {
	return fs.skipMsg(seq, false)
}

// SkipMsgNoInterest will use the next sequence number but not store anything.
// Unlike SkipMsg it also advances LastTime, which is used for Interest retention.
func (fs *fileStore) SkipMsgNoInterest(seq uint64) (uint64, error) {
	return fs.skipMsg(seq, true)
}

func (fs *fileStore) skipMsg(seq uint64, noInterest bool) (uint64, error) {
	fs.mu.Lock()
	defer fs.mu.Unlock()

	if fs.isClosed() {
		return 0, ErrStoreClosed
	}
	// Always return previous write errors.
	if err := fs.werr; err != nil {
		return 0, err
	}

	// Check sequence matches our last sequence.
	if seq != fs.state.LastSeq+1 {
		if seq > 0 {
			return 0, ErrSequenceMismatch
		}
		seq = fs.state.LastSeq + 1
	}

	// Grab our current last message block.
	mb, err := fs.checkLastBlock(emptyRecordLen)
	if err != nil {
		return 0, err
	}

	// Only update time if not already set or for Interest retention.
	var ts int64
	updateTime := noInterest || fs.state.LastTime.IsZero()
	if updateTime {
		ts = ats.AccessTime()
	} else {
		ts = fs.state.LastTime.UnixNano()
	}

	// Write skip msg.
	if err = mb.skipMsg(seq, ts); err != nil {
		fs.setWriteErr(err)
		return 0, err
	}

	// Update fs state.
	fs.state.LastSeq = seq
	if updateTime {
		fs.state.LastTime = time.Unix(0, ts).UTC()
	}
	if fs.state.Msgs == 0 {
		fs.state.FirstSeq, fs.state.FirstTime = seq, time.Time{}
	}
	if seq == fs.state.FirstSeq {
		fs.state.FirstSeq, fs.state.FirstTime = seq+1, time.Time{}
	}
	// Mark as dirty for stream state.
	fs.dirty++

	return seq, nil
}

// SkipMsgs skips multiple msgs. We will determine if we can fit into current lmb or we need to create a new block.
func (fs *fileStore) SkipMsgs(seq uint64, num uint64) error {
	fs.mu.Lock()
	defer fs.mu.Unlock()

	if fs.isClosed() {
		return ErrStoreClosed
	}
	// Always return previous write errors.
	if err := fs.werr; err != nil {
		return err
	}

	// Check sequence matches our last sequence.
	if seq != fs.state.LastSeq+1 {
		if seq > 0 {
			return ErrSequenceMismatch
		}
		seq = fs.state.LastSeq + 1
	}
	// Make sure the last sequence leaves room for the next message.
	if num == 0 || seq == 0 || num > math.MaxUint64-seq {
		return ErrSequenceMismatch
	}

	// Limit number of dmap entries
	const maxDeletes = 64 * 1024
	mb := fs.lmb

	var msgs uint64
	numDeletes := num
	if mb != nil {
		mb.mu.RLock()
		numDeletes = addSaturate(numDeletes, uint64(mb.dmap.Size()))
		msgs = mb.msgs
		mb.mu.RUnlock()
	}
	if mb == nil || numDeletes > maxDeletes && msgs > 0 || msgs > 0 && mb.blkSize()+emptyRecordLen > fs.fcfg.BlockSize {
		var err error
		if mb, err = fs.newMsgBlockForWrite(); err != nil {
			return err
		}
	}

	// Insert into dmap all entries and place last as marker.
	var ts int64
	if !fs.state.LastTime.IsZero() {
		ts = fs.state.LastTime.UnixNano()
	} else {
		ts = ats.AccessTime()
	}
	lseq := seq + num - 1

	mb.mu.Lock()
	if err := mb.werr; err != nil {
		mb.mu.Unlock()
		return err
	}
	// If we are empty update meta directly.
	if mb.msgs == 0 {
		atomic.StoreUint64(&mb.last.seq, lseq)
		mb.last.ts = ts
		atomic.StoreUint64(&mb.first.seq, lseq+1)
		mb.first.ts = 0
	} else {
		for ; seq <= lseq; seq++ {
			mb.dmap.Insert(seq)
		}
	}
	// Write out our placeholder.
	err := mb.writeMsgRecordLocked(emptyRecordLen, lseq|ebit, _EMPTY_, nil, nil, ts, true, true)
	mb.mu.Unlock()
	if err != nil {
		fs.setWriteErr(err)
		return err
	}

	// Now update FS accounting.
	// Update fs state.
	fs.state.LastSeq = lseq
	if fs.state.LastTime.IsZero() {
		fs.state.LastTime = time.Unix(0, ts).UTC()
	}
	if fs.state.Msgs == 0 {
		fs.state.FirstSeq, fs.state.FirstTime = lseq+1, time.Time{}
	}

	// Mark as dirty for stream state.
	fs.dirty++

	return nil
}

// FlushAllPending flushes all data that was still pending to be written.
func (fs *fileStore) FlushAllPending() error {
	fs.mu.Lock()
	defer fs.mu.Unlock()
	// Return previous write errors immediately.
	if fs.werr != nil {
		return fs.werr
	}
	if fs.syncOnFlush.Load() {
		fs.mu.Unlock()
		fs.syncBlocks()
		fs.mu.Lock()
		return fs.werr
	}
	return fs.checkAndFlushLastBlock()
}

// Lock should be held.
func (fs *fileStore) rebuildFirst() error {
	if len(fs.blks) == 0 {
		return nil
	}
	fmb := fs.blks[0]
	if fmb == nil {
		return nil
	}

	ld, _, _ := fmb.rebuildState()
	fmb.mu.RLock()
	isEmpty := fmb.msgs == 0
	fmb.mu.RUnlock()
	if isEmpty {
		fmb.mu.Lock()
		err := fs.forceRemoveMsgBlock(fmb)
		fmb.mu.Unlock()
		if err != nil {
			return err
		}
	}
	if err := fs.selectNextFirst(); err != nil {
		return err
	}
	fs.rebuildStateLocked(ld)
	return nil
}

// Optimized helper function to return first sequence.
// subj will always be publish subject here, meaning non-wildcard.
// We assume a fast check that this subj even exists already happened.
// Write lock should be held.
func (fs *fileStore) firstSeqForSubj(subj string) (uint64, error) {
	if len(fs.blks) == 0 {
		return 0, nil
	}

	// See if we can optimize where we start.
	start, stop := fs.blks[0].index, fs.lmb.index
	if info, ok := fs.psim.Find(stringToBytes(subj)); ok {
		start, stop = info.fblk, info.lblk
	}

	for i := start; i <= stop; i++ {
		mb := fs.bim[i]
		if mb == nil {
			continue
		}
		// If we need to load msgs here and we need to walk multiple blocks this
		// could tie up the upper fs lock, so release while dealing with the block.
		fs.mu.Unlock()

		mb.mu.Lock()
		// If marked closed, the block is already gone.
		if mb.closed {
			mb.mu.Unlock()
			fs.mu.Lock()
			continue
		}
		needsCleanup := mb.cache == nil
		var shouldExpire bool
		if mb.fssNotLoaded() {
			// Make sure we have fss loaded.
			if err := mb.loadMsgsWithLock(); err != nil {
				if needsCleanup {
					mb.finishedWithCache()
				}
				mb.mu.Unlock()
				// Re-acquire fs lock
				fs.mu.Lock()
				return 0, err
			}
			shouldExpire = true
		}
		// Mark fss activity.
		mb.lsts = ats.AccessTime()

		bsubj := stringToBytes(subj)
		if ss, ok := mb.fss.Find(bsubj); ok && ss != nil {
			var err error
			if ss.firstNeedsUpdate || ss.lastNeedsUpdate {
				err = mb.recalculateForSubj(subj, ss)
			}
			if needsCleanup {
				mb.finishedWithCache()
			}
			mb.mu.Unlock()
			// Re-acquire fs lock
			fs.mu.Lock()
			// Adjust first if it was not where we thought it should be.
			if i != start {
				if info, ok := fs.psim.Find(bsubj); ok {
					info.fblk = i
				}
			}
			if err != nil {
				return 0, err
			}
			return ss.First, nil
		}
		// If we did not find it and we loaded this msgBlock try to expire as long as not the last.
		if shouldExpire {
			// Expire this cache before moving on.
			mb.tryForceExpireCacheLocked()
		} else if needsCleanup {
			mb.finishedWithCache()
		}
		mb.mu.Unlock()
		// Re-acquire fs lock
		fs.mu.Lock()
	}
	return 0, nil
}

// Will check the msg limit and drop firstSeq msg if needed.
// Lock should be held.
func (fs *fileStore) enforceMsgLimit() error {
	if fs.cfg.Discard != DiscardOld {
		return nil
	}
	if fs.cfg.MaxMsgs <= 0 || fs.state.Msgs <= uint64(fs.cfg.MaxMsgs) {
		return nil
	}
	for nmsgs := fs.state.Msgs; nmsgs > uint64(fs.cfg.MaxMsgs); nmsgs = fs.state.Msgs {
		// If the first block can be removed fully, purge it entirely without needing to walk sequences.
		if len(fs.blks) > 0 {
			fmb := fs.blks[0]
			fmb.mu.RLock()
			msgs := fmb.msgs
			fmb.mu.RUnlock()
			if nmsgs-msgs > uint64(fs.cfg.MaxMsgs) {
				if err := fs.purgeMsgBlock(fmb, nil); err != nil {
					return err
				}
				continue
			}
		}
		if removed, err := fs.deleteFirstMsg(); err != nil {
			return err
		} else if !removed {
			return fs.rebuildFirst()
		}
	}
	return nil
}

// Will check the bytes limit and drop msgs if needed.
// Lock should be held.
func (fs *fileStore) enforceBytesLimit() error {
	if fs.cfg.Discard != DiscardOld {
		return nil
	}
	if fs.cfg.MaxBytes <= 0 || fs.state.Bytes <= uint64(fs.cfg.MaxBytes) {
		return nil
	}
	for bs := fs.state.Bytes; bs > uint64(fs.cfg.MaxBytes); bs = fs.state.Bytes {
		// If the first block can be removed fully, purge it entirely without needing to walk sequences.
		if len(fs.blks) > 0 {
			fmb := fs.blks[0]
			fmb.mu.RLock()
			bytes := fmb.bytes
			fmb.mu.RUnlock()
			if bs-bytes > uint64(fs.cfg.MaxBytes) {
				if err := fs.purgeMsgBlock(fmb, nil); err != nil {
					return err
				}
				continue
			}
		}
		if removed, err := fs.deleteFirstMsg(); err != nil {
			return err
		} else if !removed {
			return fs.rebuildFirst()
		}
	}
	return nil
}

// Will make sure we have limits honored for max msgs per subject on recovery or config update.
// We will make sure to go through all msg blocks etc. but in practice this
// will most likely only be the last one, so can take a more conservative approach.
// Lock should be held.
func (fs *fileStore) enforceMsgPerSubjectLimit(fireCallback bool) error {
	start := time.Now()
	defer func() {
		if took := time.Since(start); took > time.Minute {
			fs.warn("enforceMsgPerSubjectLimit took %v", took.Round(time.Millisecond))
		}
	}()

	maxMsgsPer := uint64(fs.cfg.MaxMsgsPer)

	// We may want to suppress callbacks from remove during this process
	// since these should have already been deleted and accounted for.
	if !fireCallback {
		cb := fs.scb
		fs.scb = nil
		defer func() { fs.scb = cb }()
	}

	var numMsgs uint64

	// collect all that are not correct.
	needAttention := stree.NewSubjectTree[uint64]()
	fblk, lblk := uint32(math.MaxUint32), uint32(0)
	fs.psim.IterFast(func(subj []byte, psi *psi) bool {
		numMsgs += psi.total
		if psi.total > maxMsgsPer {
			needAttention.Insert(subj, psi.total)
			if psi.fblk < fblk {
				fblk = psi.fblk
			}
			if psi.lblk > lblk {
				lblk = psi.lblk
			}
		}
		return true
	})

	// We had an issue with a use case where psim (and hence fss) were correct but idx was not and was not properly being caught.
	// So do a quick sanity check here. If we detect a skew do a rebuild then re-check.
	if numMsgs != fs.state.Msgs {
		fs.warn("Detected skew in subject-based total (%d) vs raw total (%d), rebuilding", numMsgs, fs.state.Msgs)
		// Clear any global subject state.
		fs.psim, fs.tsl = fs.psim.Empty(), 0
		for _, mb := range fs.blks {
			ld, _, err := mb.rebuildState()
			if err != nil {
				return err
			} else if ld != nil {
				fs.addLostData(ld)
			}
			if err = fs.populateGlobalPerSubjectInfo(mb); err != nil {
				return err
			}
		}
		// Rebuild fs state too.
		fs.rebuildStateLocked(nil)
		// Need to redo blocks that need attention.
		needAttention.Empty()
		fblk, lblk = uint32(math.MaxUint32), uint32(0)
		fs.psim.IterFast(func(subj []byte, psi *psi) bool {
			if psi.total > maxMsgsPer {
				needAttention.Insert(subj, psi.total)
				if psi.fblk < fblk {
					fblk = psi.fblk
				}
				if psi.lblk > lblk {
					lblk = psi.lblk
				}
			}
			return true
		})
	}

	// If nothing to do then stop.
	if fblk == math.MaxUint32 {
		return nil
	}

	// Collect all the msgBlks we alter.
	blks := make(map[*msgBlock]struct{})

	// For re-use below.
	var sm StoreMsg
	var fss *stree.SubjectTree[*SimpleState]
	for i := fblk; i <= lblk; i++ {
		mb := fs.bim[i]
		if mb == nil {
			continue
		}
		mb.mu.Lock()
		if err := mb.ensurePerSubjectInfoLoaded(); err != nil {
			mb.mu.Unlock()
			return err
		}
		// It isn't safe to intersect mb.fss directly, because removeMsgViaLimits modifies it
		// during the iteration, which can cause us to miss keys. We won't copy the entire
		// SimpleState structs though but rather just take pointers for speed.
		fss = fss.Empty()
		mb.fss.IterFast(func(subject []byte, val *SimpleState) bool {
			fss.Insert(subject, val)
			return true
		})
		mb.mu.Unlock()
		var ierr error
		stree.LazyIntersect(needAttention, fss, func(subj []byte, total *uint64, ssptr **SimpleState) {
			if ssptr == nil || total == nil || ierr != nil {
				return
			}
			ss := *ssptr
			if ss.firstNeedsUpdate || ss.lastNeedsUpdate {
				mb.mu.Lock()
				ierr = mb.recalculateForSubj(bytesToString(subj), ss)
				mb.mu.Unlock()
				if ierr != nil {
					return
				}
			}
			for first := ss.First; *total > maxMsgsPer && first <= ss.Last; {
				m, _, err := mb.firstMatching(bytesToString(subj), false, first, &sm)
				if err != nil {
					break
				}
				first = m.seq + 1
				if removed, _ := fs.removeMsgViaLimits(m.seq); removed {
					blks[mb] = struct{}{}
					*total--
				}
			}
		})
		if ierr != nil {
			return ierr
		}
	}

	// Expire the cache if we can.
	for mb := range blks {
		mb.mu.Lock()
		if mb.msgs > 0 {
			mb.tryForceExpireCacheLocked()
		}
		mb.mu.Unlock()
	}
	return nil
}

// Lock should be held.
func (fs *fileStore) deleteFirstMsg() (bool, error) {
	return fs.removeMsgViaLimits(fs.state.FirstSeq)
}

// If we remove via limits that can always be recovered on a restart we
// do not force the system to update the index file.
// Lock should be held.
func (fs *fileStore) removeMsgViaLimits(seq uint64) (bool, error) {
	return fs.removeMsg(seq, false, true, false)
}

// RemoveMsg will remove the message from this store.
// Will return the number of bytes removed.
func (fs *fileStore) RemoveMsg(seq uint64) (bool, error) {
	return fs.removeMsg(seq, false, false, true)
}

func (fs *fileStore) EraseMsg(seq uint64) (bool, error) {
	return fs.removeMsg(seq, true, false, true)
}

// Convenience function to remove per subject tracking at the filestore level.
// Lock should be held.
func (fs *fileStore) removePerSubject(subj string) uint64 {
	if len(subj) == 0 || fs.psim == nil {
		return 0
	}
	// We do not update sense of fblk here but will do so when we resolve during lookup.
	bsubj := stringToBytes(subj)
	if info, ok := fs.psim.Find(bsubj); ok {
		info.total--
		if info.total == 0 {
			if _, ok = fs.psim.Delete(bsubj); ok {
				fs.tsl -= len(subj)
				return 0
			}
		}
		return info.total
	}
	return 0
}

// Remove a message, optionally rewriting the mb file.
func (fs *fileStore) removeMsg(seq uint64, secure, viaLimits, needFSLock bool) (bool, error) {
	if seq == 0 {
		return false, ErrStoreMsgNotFound
	}
	fsLock := func() {
		if needFSLock {
			fs.mu.Lock()
		}
	}
	fsUnlock := func() {
		if needFSLock {
			fs.mu.Unlock()
		}
	}

	fsLock()
	defer fsUnlock()

	if fs.isClosed() {
		return false, ErrStoreClosed
	}
	// Always return previous write errors.
	if err := fs.werr; err != nil {
		return false, err
	}
	// If in encrypted mode negate secure rewrite here.
	if secure && fs.prf != nil {
		secure = false
	}

	mb := fs.selectMsgBlock(seq)
	if mb == nil {
		var err = ErrStoreEOF
		if seq <= fs.state.LastSeq {
			err = ErrStoreMsgNotFound
		}
		return false, err
	}

	return fs.removeMsgFromBlock(mb, seq, secure, viaLimits)
}

// Remove a message from the given block, optionally rewriting the mb file.
// fs lock should be held.
func (fs *fileStore) removeMsgFromBlock(mb *msgBlock, seq uint64, secure, viaLimits bool) (removed bool, rerr error) {
	mb.mu.Lock()

	// See if we are closed or the sequence number is still relevant or if we know its deleted.
	if mb.closed || seq < atomic.LoadUint64(&mb.first.seq) || mb.dmap.Exists(seq) {
		mb.mu.Unlock()
		return false, nil
	}

	// Persist any write errors.
	defer func() {
		if rerr != nil {
			fs.setWriteErr(rerr)
		}
	}()

	fifo := seq == atomic.LoadUint64(&mb.first.seq)
	isLastBlock := mb == fs.lmb
	isEmpty := mb.msgs == 1 // ... about to be zero though.

	// We used to not have to load in the messages except with callbacks or the filtered subject state (which is now always on).
	// Only load from disk if the cache is not already in memory, cacheNotLoaded will also revive a weakly held cache.
	// TODO(dlc) - Figure out a way not to have to load it in, we need subject tracking outside main data block.
	needsCleanup := mb.cache == nil
	if mb.cacheNotLoaded() {
		if err := mb.loadMsgsWithLock(); err != nil {
			if needsCleanup {
				mb.finishedWithCache()
			}
			mb.mu.Unlock()
			return false, err
		}
	}
	finishedWithCache := func() {
		if needsCleanup {
			mb.finishedWithCache()
		}
	}

	var (
		smv        StoreMsg
		subj       string
		ts         int64
		lhdr, lmsg int
		ttl        int64
	)
	sm, err := mb.cacheLookupNoCopy(seq, &smv)
	if err != nil {
		finishedWithCache()
		mb.mu.Unlock()
		// Mimic err behavior from above check to dmap. No error returned if already removed.
		if err == ErrStoreMsgNotFound || err == errDeletedMsg {
			err = nil
		}
		return false, err
	} else if sm != nil {
		// subj aliases mb.cache.buf; copy now because the cache may be erased or
		// recycled after we drop mb.mu. The rest are scalars stashed for later use.
		subj = copyString(sm.subj)
		ts = sm.ts
		lhdr = len(sm.hdr)
		lmsg = len(sm.msg)
		ttl, _ = getMessageTTL(sm.hdr)
	}

	// Check if we need to write a deleted record tombstone.
	// This is for user initiated removes or to hold the first seq
	// when the last block is empty.
	// If not via limits and not empty (empty writes tombstone below if last) write tombstone.
	if !viaLimits && !isEmpty && sm != nil {
		mb.mu.Unlock() // Only safe way to checkLastBlock is to unlock here...
		lmb, err := fs.checkLastBlock(emptyRecordLen)
		if err != nil {
			mb.mu.Lock()
			finishedWithCache()
			mb.mu.Unlock()
			return false, err
		}
		if err := lmb.writeTombstone(seq, ts); err != nil {
			mb.mu.Lock()
			finishedWithCache()
			mb.mu.Unlock()
			return false, err
		}
		mb.mu.Lock() // We'll need the lock back to carry on safely.
	}

	// Grab size
	msz := fileStoreMsgSizeRaw(len(subj), lhdr, lmsg)

	// Set cache timestamp for last remove.
	mb.lrts = ats.AccessTime()

	// Must always perform the erase, even if the block is empty as it could contain tombstones.
	if secure {
		// The cache may have been expired or recycled while we dropped mb.mu above.
		if mb.cacheNotLoaded() {
			if err := mb.loadMsgsWithLock(); err != nil {
				finishedWithCache()
				mb.mu.Unlock()
				return false, err
			}
		}
		// Grab record info, but use the pre-computed record length.
		ri, _, _, err := mb.slotInfo(int(seq - mb.cache.fseq))
		if err != nil {
			finishedWithCache()
			mb.mu.Unlock()
			return false, err
		}
		if err := mb.eraseMsg(seq, int(ri), int(msz), isLastBlock); err != nil {
			finishedWithCache()
			mb.mu.Unlock()
			return false, err
		}
	}

	// Global stats
	if fs.state.Msgs > 0 {
		fs.state.Msgs--
	}
	if msz < fs.state.Bytes {
		fs.state.Bytes -= msz
	} else {
		fs.state.Bytes = 0
	}

	// Now local mb updates.
	if mb.msgs > 0 {
		mb.msgs--
	}
	if msz < mb.bytes {
		mb.bytes -= msz
	} else {
		mb.bytes = 0
	}

	// Allow us to check compaction again.
	mb.noCompact = false

	// Mark as dirty for stream state.
	fs.dirty++

	// If we are tracking subjects here make sure we update that accounting.
	if err = mb.ensurePerSubjectInfoLoaded(); err != nil {
		finishedWithCache()
		mb.mu.Unlock()
		return false, err
	}

	// If we are tracking multiple subjects here make sure we update that accounting.
	if _, err = mb.removeSeqPerSubject(subj, seq); err != nil {
		finishedWithCache()
		mb.mu.Unlock()
		return false, err
	}
	fs.removePerSubject(subj)
	if fs.ttls != nil && ttl > 0 {
		expires := time.Duration(ts) + (time.Second * time.Duration(ttl))
		fs.ttls.Remove(seq, int64(expires))
	}

	if fifo {
		mb.selectNextFirst()
		if !isEmpty {
			// Can update this one in place.
			if seq == fs.state.FirstSeq {
				fs.state.FirstSeq = atomic.LoadUint64(&mb.first.seq) // new one.
				if mb.first.ts == 0 {
					fs.state.FirstTime = time.Time{}
				} else {
					fs.state.FirstTime = time.Unix(0, mb.first.ts).UTC()
				}
			}
		}
	} else if !isEmpty {
		// Out of order delete.
		mb.dmap.Insert(seq)
		// Make simple check here similar to Compact(). If we can save 50% and over a certain threshold do inline.
		// All other more thorough cleanup will happen in syncBlocks logic.
		// Note that we do not have to store empty records for the deleted, so don't use to calculate.
		// TODO(dlc) - This should not be inline, should kick the sync routine.
		if !isLastBlock && mb.shouldCompactInline() {
			if err = mb.compact(); err != nil {
				finishedWithCache()
				mb.mu.Unlock()
				return false, err
			}
		}
	}

	if secure {
		if ld, _ := mb.flushPendingMsgsLocked(); ld != nil {
			// We have the mb lock here, this needs the mb locks so do in its own go routine.
			go fs.rebuildState(ld)
		}
	}

	// If empty remove this block and check if we need to update first sequence.
	// We will write a tombstone at the end.
	var firstSeqNeedsUpdate bool
	if isEmpty {
		// This writes tombstone iff mb == lmb, so no need to do above.
		if err = fs.removeMsgBlock(mb); err != nil {
			finishedWithCache()
			mb.mu.Unlock()
			return false, err
		}
		firstSeqNeedsUpdate = seq == fs.state.FirstSeq
	}
	finishedWithCache()
	mb.mu.Unlock()

	// If we emptied the current message block and the seq was state.FirstSeq
	// then we need to jump message blocks. We will also write the index so
	// we don't lose track of the first sequence.
	if firstSeqNeedsUpdate {
		if err = fs.selectNextFirst(); err != nil {
			return false, err
		}
	}

	if cb := fs.scb; cb != nil {
		// If we have a callback registered we need to release lock regardless since cb might need it to lookup msg, etc.
		fs.mu.Unlock()
		// Storage updates.
		delta := int64(msz)
		cb(-1, -delta, seq, subj)

		fs.mu.Lock()
	}

	return true, nil
}

// Remove all messages in the range [first, last]
// If agg is set the storage updates for fully removed blocks are aggregated.
// Lock should be held.
func (fs *fileStore) removeMsgsInRange(first, last uint64, viaLimits bool, agg *storeUpdateAgg) error {
	last = min(last, fs.state.LastSeq)
	if first > last {
		return nil
	}

	firstBlock := sort.Search(len(fs.blks), func(i int) bool {
		return atomic.LoadUint64(&fs.blks[i].last.seq) >= first
	})
	if firstBlock >= len(fs.blks) {
		return nil
	}

	for i := firstBlock; i < len(fs.blks); {
		mb := fs.blks[i]
		mbFirstSeq := atomic.LoadUint64(&mb.first.seq)
		mbLastSeq := atomic.LoadUint64(&mb.last.seq)
		if mbFirstSeq > last {
			break
		}
		if mbFirstSeq >= first && mbLastSeq <= last && mb.numPriorTombs() == 0 {
			// If this block stores no tombstones for previous blocks,
			// and its sequences are within the range to be removed,
			// we can get rid of the block entirely. To do that we use
			// purgeMgsBlock, which also removes the block from fs.blks.
			// After purgeMsgBlock, i will be the index of the following
			// msgBlock, if any. Therefore, continue without incrementing i.
			if err := fs.purgeMsgBlock(mb, agg); err != nil {
				return err
			}
		} else {
			from := max(first, mbFirstSeq)
			to := min(last, mbLastSeq)
			for seq := from; seq <= to; seq++ {
				if _, err := fs.removeMsgFromBlock(mb, seq, false, viaLimits); err != nil {
					return err
				}
			}
			i++
		}
	}
	return nil
}

// Tests whether we should try to compact this block while inline removing msgs.
// We will want rbytes to be over the minimum and have a 2x potential savings.
// If we compacted before but rbytes didn't improve much, guard against constantly compacting.
// Lock should be held.
func (mb *msgBlock) shouldCompactInline() bool {
	return mb.rbytes > compactMinimum && mb.bytes*2 < mb.rbytes && (mb.cbytes == 0 || mb.bytes*2 < mb.cbytes)
}

// Tests whether we should try to compact this block while running periodic sync.
// We will want rbytes to be over the minimum and have a 2x potential savings.
// Ignores 2MB minimum.
// Lock should be held.
func (mb *msgBlock) shouldCompactSync() bool {
	return mb.bytes*2 < mb.rbytes && !mb.noCompact
}

// This will compact and rewrite this block. This version will not process any tombstone cleanup.
// Write lock needs to be held.
func (mb *msgBlock) compact() error {
	return mb.compactWithFloor(0, nil)
}

// This will compact and rewrite this block. This should only be called when we know we want to rewrite this block.
// This should not be called on the lmb since we will prune tail deleted messages which could cause issues with
// writing new messages. We will silently bail on any issues with the underlying block and let someone else detect.
// if fseq > 0 we will attempt to cleanup stale tombstones.
// Write lock needs to be held.
func (mb *msgBlock) compactWithFloor(floor uint64, fsDmap *interiorDeletes) error {
	wasLoaded := mb.cache != nil && mb.cacheAlreadyLoaded()
	if !wasLoaded {
		if err := mb.loadMsgsWithLock(); err != nil {
			return err
		}
	}
	defer mb.finishedWithCache()

	buf := mb.cache.buf
	nbuf := getMsgBlockBuf(len(buf))
	// Recycle our nbuf when we are done.
	defer recycleMsgBlockBuf(nbuf)

	var le = binary.LittleEndian
	var firstSet bool
	var last uint64
	var lastTime int64
	var msgs uint64

	fseq := atomic.LoadUint64(&mb.first.seq)
	lseq := atomic.LoadUint64(&mb.last.seq)
	isDeleted := func(seq uint64) bool {
		return seq == 0 || seq&ebit != 0 || mb.dmap.Exists(seq) || seq < fseq
	}

	for index, lbuf := uint32(0), uint32(len(buf)); index < lbuf; {
		if index+msgHdrSize > lbuf {
			return fmt.Errorf("message overrun")
		}
		hdr := buf[index : index+msgHdrSize]
		rl, slen := le.Uint32(hdr[0:]), int(le.Uint16(hdr[20:]))
		hasHeaders := rl&hbit != 0
		// Clear any headers bit that could be set.
		rl &^= hbit
		shlen := slen
		if hasHeaders {
			shlen += 4
		}
		dlen := int(rl) - msgHdrSize
		// Do some quick sanity checks here.
		if dlen < 0 || shlen > (dlen-recordHashSize) || dlen > int(rl) || index+rl > lbuf || rl > rlBadThresh {
			return fmt.Errorf("sanity check failed")
		}
		// Only need to process non-deleted messages.
		seq := le.Uint64(hdr[4:])
		ts := int64(le.Uint64(hdr[12:]))

		if !isDeleted(seq) {
			// Check for tombstones.
			if seq&tbit != 0 {
				seq = seq &^ tbit
				// If this entry is for a lower seq than ours then keep around.
				// We also check that it is greater than our floor. Floor is zero on normal
				// calls to compact.
				// If the global delete map is set, check if a tombstone is still
				// referencing a message in another block. If not, it can be removed.
				if seq < fseq && seq >= floor && (fsDmap == nil || fsDmap.Exists(seq)) {
					nbuf = append(nbuf, buf[index:index+rl]...)
				}
			} else {
				// Normal message here.
				msgs++
				nbuf = append(nbuf, buf[index:index+rl]...)
				if !firstSet {
					firstSet = true
					atomic.StoreUint64(&mb.first.seq, seq)
				}
				if seq >= last {
					last, lastTime = seq, ts
				}
			}
		}
		// Advance to next record.
		index += rl
	}

	// Handle compression
	if mb.cmp != NoCompression && len(nbuf) > 0 {
		originalSize := len(nbuf)
		var err error
		if nbuf, err = mb.cmp.Compress(nbuf); err != nil {
			return err
		}
		meta := &CompressionInfo{
			Algorithm:    mb.cmp,
			OriginalSize: uint64(originalSize),
		}
		nbuf = append(meta.MarshalMetadata(), nbuf...)
	}

	// Check for encryption.
	if mb.bek != nil && len(nbuf) > 0 {
		// Recreate to reset counter.
		rbek, err := genBlockEncryptionKey(mb.fs.fcfg.Cipher, mb.seed, mb.nonce)
		if err != nil {
			return err
		}
		rbek.XORKeyStream(nbuf, nbuf)
	}

	// Close FDs first.
	mb.closeFDsLocked()

	// We will write to a new file and mv/rename it in case of failure.
	mfn := filepath.Join(mb.fs.fcfg.StoreDir, msgDir, fmt.Sprintf(newScan, mb.index))
	sync := mb.fs.syncAlways.Load() || mb.fs.syncOnFlush.Load()
	if err := writeAtomicallyWithTemp(mb.fs.dios, mfn, mb.mfn, nbuf, defaultFilePerms, sync); err != nil {
		return err
	}

	// Block selection reads last.seq without mb.mu, so publish it only
	// after the replacement file is installed.
	if msgs > 0 {
		atomic.StoreUint64(&mb.last.seq, last)
		mb.last.ts = lastTime
	}

	// Make sure to sync if we have not done so yet
	mb.needSync = !sync

	// Capture the updated rbytes.
	if rbytes := uint64(len(nbuf)); rbytes == mb.rbytes {
		// No change, so set our noCompact bool here to avoid attempting to continually compress in syncBlocks.
		mb.noCompact = true
	} else {
		mb.rbytes = rbytes
	}
	mb.cbytes = mb.bytes

	// Remove any seqs from the beginning of the blk.
	for seq, nfseq := fseq, atomic.LoadUint64(&mb.first.seq); seq < nfseq; seq++ {
		mb.dmap.Delete(seq)
	}
	// Remove any seqs from the ending of the blk.
	for seq, nlseq := lseq, atomic.LoadUint64(&mb.last.seq); seq > nlseq; seq-- {
		mb.dmap.Delete(seq)
	}
	// If the block itself has no messages anymore (could still contain tombstones though),
	// then we need to account for that by resetting the last sequence and timestamp.
	if msgs == 0 {
		atomic.StoreUint64(&mb.last.seq, fseq-1)
		mb.last.ts = 0
		mb.dmap.Empty()
	}
	// Make sure we clear the cache since no longer valid.
	mb.clearCacheAndOffset()
	// If we entered with the msgs loaded make sure to reload them.
	if wasLoaded {
		if err := mb.loadMsgsWithLock(); err != nil {
			return err
		}
	}
	return nil
}

// Grab info from a slot.
// Lock should be held.
func (mb *msgBlock) slotInfo(slot int) (uint32, uint32, bool, error) {
	switch {
	case mb.cache == nil: // Shouldn't be possible, but check it anyway.
		return 0, 0, false, errNoCache
	case slot < 0:
		mb.fs.warn("Partial cache: offset slot index %d is less zero", slot)
		return 0, 0, false, errPartialCache
	case slot >= len(mb.cache.idx):
		mb.fs.warn("Partial cache: offset slot index %d is greater than index len %d", slot, len(mb.cache.idx))
		return 0, 0, false, errPartialCache
	}

	bi := mb.cache.idx[slot]
	ri, hashChecked := (bi &^ cbit), (bi&cbit) != 0

	// If this is a deleted slot return here.
	if bi == dbit {
		return 0, 0, false, errDeletedMsg
	}

	// Determine record length
	var rl uint32
	// Need to account for dbit markers in idx.
	// So we will walk until we find valid idx slot to calculate rl.
	for i := 1; slot+i < len(mb.cache.idx); i++ {
		ni := mb.cache.idx[slot+i] &^ cbit
		if ni == dbit {
			continue
		}
		rl = ni - ri
		break
	}
	// check if we had all trailing dbits.
	// If so use len of cache buf minus ri.
	if rl == 0 {
		rl = uint32(len(mb.cache.buf)) - ri
	}
	if rl < msgHdrSize {
		return 0, 0, false, errBadMsg{mb.mfn, fmt.Sprintf("length too short for slot %d", slot)}
	}
	return uint32(ri), rl, hashChecked, nil
}

func (fs *fileStore) isClosed() bool {
	return fs.closed.Load()
}

// Will spin up our flush loop.
func (mb *msgBlock) spinUpFlushLoop() {
	mb.mu.Lock()
	defer mb.mu.Unlock()
	mb.spinUpFlushLoopLocked()
}

// Will spin up our flush loop.
// Lock should be held.
func (mb *msgBlock) spinUpFlushLoopLocked() {
	// Are we already running or closed?
	if mb.flusher || mb.closed {
		return
	}
	mb.flusher = true
	mb.fch = make(chan struct{}, 1)
	mb.qch = make(chan struct{})
	fch, qch := mb.fch, mb.qch

	go mb.flushLoop(fch, qch)
}

// Raw low level kicker for flush loops.
func kickFlusher(fch chan struct{}) {
	if fch != nil {
		select {
		case fch <- struct{}{}:
		default:
		}
	}
}

// Kick flusher for this message block.
func (mb *msgBlock) kickFlusher() {
	mb.mu.RLock()
	defer mb.mu.RUnlock()
	kickFlusher(mb.fch)
}

func (mb *msgBlock) setInFlusher() {
	mb.mu.Lock()
	mb.flusher = true
	mb.mu.Unlock()
}

func (mb *msgBlock) clearInFlusher() {
	mb.mu.Lock()
	mb.flusher = false
	if mb.qch != nil {
		close(mb.qch)
		mb.qch = nil
	}
	if mb.fch != nil {
		close(mb.fch)
		mb.fch = nil
	}
	mb.mu.Unlock()
}

// flushLoop watches for messages, index info, or recently closed msg block updates.
func (mb *msgBlock) flushLoop(fch, qch chan struct{}) {
	mb.setInFlusher()
	defer mb.clearInFlusher()

	for {
		select {
		case <-fch:
			// If we have pending messages process them first.
			if waiting := mb.pendingWriteSize(); waiting != 0 {
				ts := 1 * time.Millisecond
				var waited time.Duration

				for waiting < coalesceMinimum {
					time.Sleep(ts)
					select {
					case <-qch:
						return
					default:
					}
					newWaiting := mb.pendingWriteSize()
					if waited = waited + ts; waited > maxFlushWait || newWaiting <= waiting {
						break
					}
					waiting = newWaiting
					ts *= 2
				}

				// Ignore error here, the error is persisted as mb.werr and will be bubbled up later.
				_ = mb.flushPendingMsgs()
			}

			// Check if we are no longer the last message block. If we are
			// not we can close FDs and exit.
			mb.fs.mu.RLock()
			notLast := mb != mb.fs.lmb
			mb.fs.mu.RUnlock()

			if notLast {
				if err := mb.closeFDs(); err == nil {
					return
				}
			}
		case <-qch:
			return
		}
	}
}

// Lock should be held.
func (mb *msgBlock) eraseMsg(seq uint64, ri, rl int, isLastBlock bool) error {
	var le = binary.LittleEndian
	var hdr [msgHdrSize]byte

	le.PutUint32(hdr[0:], uint32(rl))
	le.PutUint64(hdr[4:], seq|ebit)
	le.PutUint64(hdr[12:], 0)
	le.PutUint16(hdr[20:], 0)

	// Randomize record
	data := make([]byte, rl-emptyRecordLen)
	if n, err := rand.Read(data); err != nil {
		return err
	} else if n != len(data) {
		return fmt.Errorf("not enough overwrite bytes read (%d != %d)", n, len(data))
	}

	// Now write to underlying buffer.
	var b bytes.Buffer
	b.Write(hdr[:])
	b.Write(data)

	// Calculate hash.
	mb.hh.Reset()
	mb.hh.Write(hdr[4:20])
	mb.hh.Write(data)
	var hb [highwayhash.Size64]byte
	checksum := mb.hh.Sum(hb[:0])
	// Write to msg record.
	b.Write(checksum)

	// Update both cache and disk.
	nbytes := b.Bytes()

	// Cache
	if ri+rl <= len(mb.cache.buf) {
		copy(mb.cache.buf[ri:ri+rl], nbytes)
	}

	// Disk
	if mb.cache.wp > ri {
		if err := mb.atomicOverwriteFile(mb.cache.buf, !isLastBlock); err != nil {
			return err
		}
	}
	return nil
}

// Truncate this message block to the tseq and ts.
// Lock should be held.
func (mb *msgBlock) truncate(tseq uint64, ts int64) (nmsgs, nbytes uint64, err error) {
	// Make sure we are loaded to process messages etc.
	if err := mb.loadMsgsWithLock(); err != nil {
		return 0, 0, err
	}
	defer mb.finishedWithCache()

	// Calculate new eof using slot info from our new last sm.
	ri, rl, _, err := mb.slotInfo(int(tseq - mb.cache.fseq))
	if err != nil {
		return 0, 0, err
	}
	// Calculate new eof.
	eof := int64(ri + rl)

	// FIXME(dlc) - We could be smarter here.
	if buf, _ := mb.bytesPending(); len(buf) > 0 {
		ld, err := mb.flushPendingMsgsLocked()
		if ld != nil && mb.fs != nil {
			// We do not know if fs is locked or not at this point.
			// This should be an exceptional condition so do so in Go routine.
			go mb.fs.rebuildState(ld)
		}
		if err != nil {
			return 0, 0, err
		}
	}

	var purged, bytes uint64

	checkDmap := mb.dmap.Size() > 0
	var smv StoreMsg

	for seq := atomic.LoadUint64(&mb.last.seq); seq > tseq; seq-- {
		if checkDmap {
			if mb.dmap.Exists(seq) {
				// Delete and skip to next.
				mb.dmap.Delete(seq)
				checkDmap = !mb.dmap.IsEmpty()
				continue
			}
		}
		// We should have a valid msg to calculate removal stats.
		if m, err := mb.cacheLookupNoCopy(seq, &smv); err == nil {
			if mb.msgs > 0 {
				rl := fileStoreMsgSize(m.subj, m.hdr, m.msg)
				mb.msgs--
				if rl > mb.bytes {
					rl = mb.bytes
				}
				mb.bytes -= rl
				mb.rbytes -= rl
				// For return accounting.
				purged++
				bytes += uint64(rl)
			}
		}
	}

	// If the block is compressed/encrypted then we have to load it into memory
	// and decompress/decrypt it, truncate it and then write it back out.
	// Otherwise, truncate the file itself and close the descriptor.
	if mb.cmp != NoCompression || mb.bek != nil {
		buf, err := mb.loadBlock(nil)
		if err != nil {
			return 0, 0, fmt.Errorf("failed to load block from disk: %w", err)
		}
		if err = mb.encryptOrDecryptIfNeeded(buf); err != nil {
			return 0, 0, err
		}
		if buf, err = mb.decompressIfNeeded(buf); err != nil {
			return 0, 0, fmt.Errorf("failed to decompress block: %w", err)
		}
		buf = buf[:eof]
		copy(mb.lchk[0:], buf[len(buf)-checksumSize:])
		// We did decompress but don't recompress the truncated buffer here since we're the last block
		// and would otherwise have compressed data and allow to write uncompressed data in the same block.
		if err = mb.atomicOverwriteFile(buf, false); err != nil {
			return 0, 0, err
		}
	} else if mb.mfd != nil {
		if err = mb.mfd.Truncate(eof); err != nil {
			return 0, 0, err
		}
		if err = mb.mfd.Sync(); err != nil {
			return 0, 0, err
		}
		// Update our checksum.
		var lchk [8]byte
		if _, err = mb.mfd.ReadAt(lchk[:], eof-8); err != nil {
			return 0, 0, err
		}
		copy(mb.lchk[0:], lchk[:])
	} else {
		return 0, 0, fmt.Errorf("failed to truncate msg block %d, file not open", mb.index)
	}

	// Update our last msg.
	atomic.StoreUint64(&mb.last.seq, tseq)
	mb.last.ts = ts

	// Clear our cache.
	mb.clearCacheAndOffset()

	// Redo per subject info for this block.
	if err = mb.resetPerSubjectInfo(); err != nil {
		return purged, bytes, err
	}

	// Load msgs again.
	return purged, bytes, mb.loadMsgsWithLock()
}

// Helper to determine if the mb is empty.
func (mb *msgBlock) isEmpty() bool {
	return atomic.LoadUint64(&mb.first.seq) > atomic.LoadUint64(&mb.last.seq)
}

// Lock should be held.
func (mb *msgBlock) selectNextFirst() {
	var seq uint64
	fseq, lseq := atomic.LoadUint64(&mb.first.seq), atomic.LoadUint64(&mb.last.seq)
	for seq = fseq + 1; seq <= lseq; seq++ {
		if mb.dmap.Exists(seq) {
			// We will move past this so we can delete the entry.
			mb.dmap.Delete(seq)
		} else {
			break
		}
	}
	// Set new first sequence.
	atomic.StoreUint64(&mb.first.seq, seq)

	// Check if we are empty..
	if seq > lseq {
		mb.first.ts = 0
		return
	}

	// Need to get the timestamp.
	// We will try the cache direct and fallback if needed.
	var smv StoreMsg
	sm, _ := mb.cacheLookupNoCopy(seq, &smv)
	if sm == nil {
		// Slow path, cache not loaded.
		sm, _, _ = mb.fetchMsgNoCopyLocked(seq, &smv)
		mb.finishedWithCache()
	}
	if sm != nil {
		mb.first.ts = sm.ts
	} else {
		mb.first.ts = 0
	}
}

// Select the next FirstSeq
// Also cleans up empty blocks at the start only containing tombstones.
// Lock should be held.
func (fs *fileStore) selectNextFirst() error {
	if len(fs.blks) > 0 {
		for len(fs.blks) > 1 {
			mb := fs.blks[0]
			mb.mu.Lock()
			empty := mb.msgs == 0
			if !empty {
				mb.mu.Unlock()
				break
			}
			err := fs.forceRemoveMsgBlock(mb)
			mb.mu.Unlock()
			if err != nil {
				return err
			}
		}
		mb := fs.blks[0]
		mb.mu.RLock()
		fs.state.FirstSeq = atomic.LoadUint64(&mb.first.seq)
		if mb.first.ts == 0 {
			fs.state.FirstTime = time.Time{}
		} else {
			fs.state.FirstTime = time.Unix(0, mb.first.ts).UTC()
		}
		mb.mu.RUnlock()
	} else {
		// Could not find anything, so treat like purge
		fs.state.FirstSeq = fs.state.LastSeq + 1
		fs.state.FirstTime = time.Time{}
	}
	// Mark first as moved. Plays into tombstone cleanup for syncBlocks.
	fs.firstMoved = true
	return nil
}

// Lock should be held.
func (mb *msgBlock) resetCacheExpireTimer(td time.Duration) {
	if td == 0 {
		td = mb.cexp + 100*time.Millisecond
	}
	if mb.ctmr == nil {
		mb.ctmr = time.AfterFunc(td, mb.tryExpireCache)
	} else {
		mb.ctmr.Reset(td)
	}
}

// Lock should be held.
func (mb *msgBlock) startCacheExpireTimer() {
	mb.resetCacheExpireTimer(0)
}

// Used when we load in a message block.
// Lock should be held.
func (mb *msgBlock) clearCacheAndOffset() {
	// Reset linear scan tracker.
	mb.llseq = 0
	mb.clearCache()
}

// Lock should be held.
func (mb *msgBlock) clearCache() {
	if mb.ctmr != nil {
		tsla := mb.sinceLastActivity()
		if mb.fss == nil || tsla > mb.fexp {
			// Force
			mb.fss = nil
			mb.ctmr.Stop()
			mb.ctmr = nil
		} else {
			mb.resetCacheExpireTimer(mb.fexp - tsla)
		}
	}

	mbcache := mb.cache
	if mbcache == nil {
		mbcache = mb.ecache.Value()
	}
	if mbcache == nil {
		return
	}

	buf := mbcache.buf
	mb.cache = nil
	mb.ecache.Set(nil)
	mbcache.stopRecycle()
	recycleMsgBlockBuf(buf)
}

// Called to possibly expire a message block cache.
func (mb *msgBlock) tryExpireCache() {
	mb.mu.Lock()
	defer mb.mu.Unlock()
	mb.tryExpireCacheLocked()
}

func (mb *msgBlock) tryForceExpireCache() {
	mb.mu.Lock()
	defer mb.mu.Unlock()
	mb.tryForceExpireCacheLocked()
}

// We will attempt to force expire this by temporarily clearing the last load time.
func (mb *msgBlock) tryForceExpireCacheLocked() {
	llts := mb.llts
	mb.llts = 0
	mb.tryExpireCacheLocked()
	mb.llts = llts
}

// This is for expiration of the write cache, which will be partial with fip.
// So we want to bypass the Pools here.
// Lock should be held.
func (mb *msgBlock) tryExpireWriteCache() []byte {
	if mb.cache == nil {
		return nil
	}
	lwts, buf, llts, nra := mb.lwts, mb.cache.buf, mb.llts, mb.cache.nra
	mb.lwts, mb.cache.nra = 0, true
	mb.tryExpireCacheLocked()
	mb.lwts = lwts
	if mb.cache != nil {
		mb.cache.nra = nra
	}
	// We could check for a certain time since last load, but to be safe just reuse if no loads at all.
	if llts == 0 && (mb.cache == nil || mb.cache.buf == nil) {
		// Clear last write time since we now are about to move on to a new lmb.
		mb.lwts = 0
		return buf[:0]
	}
	// The cache may have expired above without recycling the buffer.
	if mb.cache == nil && !nra {
		recycleMsgBlockBuf(buf)
	}
	return nil
}

// Lock should be held.
func (mb *msgBlock) tryExpireCacheLocked() {
	var strengthened bool
	if mb.cache == nil {
		mb.cache = mb.ecache.Value()
		strengthened = true
	}
	if mb.cache == nil && mb.fss == nil {
		if mb.ctmr != nil {
			mb.ctmr.Stop()
			mb.ctmr = nil
		}
		return
	}

	// Can't expire if we still have pending.
	if mb.cache != nil && len(mb.cache.buf)-int(mb.cache.wp) > 0 {
		mb.resetCacheExpireTimer(mb.cexp)
		if strengthened {
			mb.finishedWithCache()
		}
		return
	}

	// Grab timestamp to compare.
	tns := ats.AccessTime()

	// For the core buffer of messages, we care about reads and writes, but not removes.
	bufts := mb.llts
	if mb.lwts > bufts {
		bufts = mb.lwts
	}

	// Check for activity on the cache that would prevent us from expiring.
	// Both tns and bufts come from ats.AccessTime(), which means bufts can understate
	// how recent the last activity actually was by up to one tick.
	if delta := tns - bufts; delta <= int64(mb.cexp)+int64(ats.TickInterval) {
		td := mb.cexp - time.Duration(delta)
		if td <= 0 {
			td = ats.TickInterval
		}
		mb.resetCacheExpireTimer(td)
		if strengthened {
			mb.finishedWithCache()
		}
		return
	}

	// If we are here we will at least expire the core msg buffer.
	// We need to capture offset in case we do a write next before a full load.
	if mb.cache != nil {
		mb.cache.stopRecycle()
		if !mb.cache.nra {
			recycleMsgBlockBuf(mb.cache.buf)
		}
		mb.cache.buf = nil
		mb.cache.idx = mb.cache.idx[:0]
		mb.cache.wp = 0
	}

	// Check if we can clear out our idx unless under force expire.
	// fss we keep longer and expire under sync timer checks.
	mb.clearCache()
}

func (fs *fileStore) startAgeChk() {
	if fs.recovering || fs.ageChk != nil {
		return
	}
	if fs.cfg.MaxAge != 0 || fs.ttls != nil {
		fs.ageChk = time.AfterFunc(fs.cfg.MaxAge, fs.expireMsgs)
	}
}

// Lock should be held.
func (fs *fileStore) resetAgeChk(delta int64) {
	if fs.recovering {
		return
	}
	// If we're already expiring messages, it will make sure to reset.
	// Don't trigger again, as that could result in many expire goroutines.
	if fs.ageChkRun {
		return
	}

	var next int64 = math.MaxInt64
	if fs.ttls != nil {
		next = fs.ttls.GetNextExpiration(next)
	}

	// If there's no MaxAge and there's nothing waiting to be expired then
	// don't bother continuing. The next storeRawMsg() will wake us up if
	// needs be.
	if fs.cfg.MaxAge <= 0 && next == math.MaxInt64 {
		clearTimer(&fs.ageChk)
		return
	}

	// Check to see if we should be firing sooner than MaxAge for an expiring TTL.
	fireIn := fs.cfg.MaxAge

	// If delta for next-to-expire message is unset, but we still have messages to remove.
	// Assume messages are removed through proposals, and we need to speed up subsequent age check.
	if delta == 0 && fs.state.Msgs > 0 {
		if until := 2 * time.Second; until < fireIn {
			fireIn = until
		}
	}

	if next < math.MaxInt64 {
		// Looks like there's a next expiration, use it either if there's no
		// MaxAge set or if it looks to be sooner than MaxAge is.
		if until := time.Until(time.Unix(0, next)); fireIn == 0 || until < fireIn {
			fireIn = until
		}
	}

	// If not then look at the delta provided (usually gap to next age expiry).
	if delta > 0 {
		if fireIn == 0 || time.Duration(delta) < fireIn {
			fireIn = time.Duration(delta)
		}
	}

	// Make sure we aren't firing too often either way, otherwise we can
	// negatively impact stream ingest performance.
	if fireIn < 250*time.Millisecond {
		fireIn = 250 * time.Millisecond
	}

	// If we want to kick the timer to run later than what was assigned before, don't reset it.
	// Otherwise, we could get in a situation where the timer is continuously reset, and it never runs.
	expires := ats.AccessTime() + fireIn.Nanoseconds()
	if fs.ageChkTime > 0 && expires > fs.ageChkTime {
		return
	}

	fs.ageChkTime = expires
	if fs.ageChk != nil {
		fs.ageChk.Reset(fireIn)
	} else {
		fs.ageChk = time.AfterFunc(fireIn, fs.expireMsgs)
	}
}

// Lock should be held.
func (fs *fileStore) cancelAgeChk() {
	if fs.ageChk != nil {
		fs.ageChk.Stop()
		fs.ageChk = nil
		fs.ageChkTime = 0
	}
}

// Will expire msgs that are too old.
func (fs *fileStore) expireMsgs() {
	// We need to delete one by one here and can not optimize for the time being.
	// Reason is that we need more information to adjust ack pending in consumers.
	var smv StoreMsg
	var sm *StoreMsg

	fs.mu.Lock()
	if fs.recovering {
		fs.cancelAgeChk()
		fs.mu.Unlock()
		return
	}
	maxAge := int64(fs.cfg.MaxAge)
	minAge := ats.AccessTime() - maxAge
	rmcb := fs.rmcb
	pmsgcb := fs.pmsgcb
	sdmTTL := int64(fs.cfg.SubjectDeleteMarkerTTL.Seconds())
	sdmEnabled := sdmTTL > 0

	// If SDM is enabled, but handlers aren't set up yet. Try again later.
	if sdmEnabled && (rmcb == nil || pmsgcb == nil) {
		fs.resetAgeChk(0)
		fs.mu.Unlock()
		return
	}
	fs.ageChkRun = true
	fs.mu.Unlock()

	if maxAge > 0 {
		var seq uint64
		for sm, seq, _ = fs.LoadNextMsg(fwcs, true, 0, &smv); sm != nil && sm.ts <= minAge; sm, seq, _ = fs.LoadNextMsg(fwcs, true, seq+1, &smv) {
			if len(sm.hdr) > 0 {
				if ttl, err := getMessageTTL(sm.hdr); err == nil && ttl < 0 {
					// The message has a negative TTL, therefore it must "never expire".
					minAge = ats.AccessTime() - maxAge
					continue
				}
			}
			// Remove the message and then, if LimitsTTL is enabled, try and work out
			// if it was the last message of that particular subject that we just deleted.
			if sdmEnabled {
				if last, ok := fs.shouldProcessSdm(seq, sm.subj); ok {
					sdm := last && !isSubjectDeleteMarker(sm.hdr)
					fs.handleRemovalOrSdm(seq, sm.subj, sdm, sdmTTL)
				}
			} else {
				fs.mu.Lock()
				fs.removeMsgViaLimits(sm.seq)
				fs.mu.Unlock()
			}
			// Recalculate in case we are expiring a bunch.
			minAge = ats.AccessTime() - maxAge
		}
	}
	var ageDelta int64
	if sm != nil {
		ageDelta = sm.ts - minAge
	}

	fs.mu.Lock()
	defer fs.mu.Unlock()

	// TODO: Not great that we're holding the lock here, but the timed hash wheel isn't thread-safe.
	nextTTL := int64(math.MaxInt64)
	var rmSeqs []thw.HashWheelEntry
	if fs.ttls != nil {
		fs.ttls.ExpireTasks(func(seq uint64, ts int64) bool {
			rmSeqs = append(rmSeqs, thw.HashWheelEntry{Seq: seq, Expires: ts})
			// We might need to remove messages out of band, those can fail, and we can be shutdown halfway
			// through so don't remove from THW just yet.
			return false
		})
		nextTTL = fs.ttls.GetNextExpiration(math.MaxInt64)
	}

	// Remove messages collected by THW.
	if !sdmEnabled {
		for _, rm := range rmSeqs {
			removed, err := fs.removeMsg(rm.Seq, false, false, false)
			// The message may already be gone, removed out of band by a purge, a rollup,
			// a compact or a truncate, none of which consult the THW. Drop the entry in
			// that case, otherwise it is collected again on every pass forever. A genuine
			// removal failure (write error, closed store) keeps the entry so it is retried.
			if !removed && (err == nil || err == ErrStoreMsgNotFound || err == ErrStoreEOF) {
				fs.ttls.Remove(rm.Seq, rm.Expires)
				// The removal that orphaned the entry may already have been flushed, so
				// mark the state dirty or the pruned wheel never reaches thw.db.
				fs.dirty++
			}
		}
	} else {
		// THW is unordered, so must sort by sequence and must not be holding the lock.
		fs.mu.Unlock()
		slices.SortFunc(rmSeqs, func(a, b thw.HashWheelEntry) int {
			if a.Seq == b.Seq {
				return 0
			} else if a.Seq < b.Seq {
				return -1
			} else {
				return 1
			}
		})
		for _, rm := range rmSeqs {
			// Need to grab subject for the specified sequence if for SDM, and check
			// if the message hasn't been removed in the meantime.
			// We need to grab the message and check if we should process SDM while holding the lock,
			// otherwise we can race if a deletion of this message is in progress.
			fs.mu.Lock()
			sm, _ = fs.msgForSeqLocked(rm.Seq, &smv, false)
			if sm == nil {
				fs.ttls.Remove(rm.Seq, rm.Expires)
				fs.dirty++
				fs.mu.Unlock()
				continue
			}
			last, ok := fs.shouldProcessSdmLocked(rm.Seq, sm.subj)
			fs.mu.Unlock()
			if ok {
				sdm := last && !isSubjectDeleteMarker(sm.hdr)
				fs.handleRemovalOrSdm(rm.Seq, sm.subj, sdm, sdmTTL)
			}
		}
		fs.mu.Lock()
	}

	// Only cancel if no message left, not on potential lookup error that would result in sm == nil.
	fs.ageChkRun, fs.ageChkTime = false, 0
	if fs.state.Msgs == 0 && nextTTL == math.MaxInt64 {
		fs.cancelAgeChk()
	} else {
		fs.resetAgeChk(ageDelta)
	}
}

func (fs *fileStore) shouldProcessSdm(seq uint64, subj string) (bool, bool) {
	fs.mu.Lock()
	defer fs.mu.Unlock()
	return fs.shouldProcessSdmLocked(seq, subj)
}

// Lock should be held.
func (fs *fileStore) shouldProcessSdmLocked(seq uint64, subj string) (bool, bool) {
	if fs.sdm == nil {
		fs.sdm = newSDMMeta()
	}

	if p, ok := fs.sdm.pending[seq]; ok {
		// Don't allow more proposals for the same sequence if we already did recently.
		if time.Since(time.Unix(0, p.ts)) < 2*time.Second {
			return p.last, false
		}
		// If we're about to use the cached value, and we knew it was last before,
		// quickly check that we don't have more remaining messages for the subject now.
		// Which means we are not the last anymore and must reset to not remove later data.
		if p.last {
			msgs := fs.subjectsTotalsLocked(subj)[subj]
			numPending := fs.sdm.totals[subj]
			if remaining := msgs - numPending; remaining > 0 {
				p.last = false
			}
		}
		fs.sdm.pending[seq] = SDMBySeq{p.last, time.Now().UnixNano()}
		return p.last, true
	}

	msgs := fs.subjectsTotalsLocked(subj)[subj]
	if msgs == 0 {
		return false, true
	}
	numPending := fs.sdm.totals[subj]
	remaining := msgs - numPending
	return fs.sdm.trackPending(seq, subj, remaining == 1), true
}

func (fs *fileStore) handleRemovalOrSdm(seq uint64, subj string, sdm bool, sdmTTL int64) {
	if sdm {
		var _hdr [128]byte
		hdr := fmt.Appendf(
			_hdr[:0],
			"NATS/1.0\r\n%s: %s\r\n%s: %s\r\n%s: %s\r\n\r\n",
			JSMarkerReason, JSMarkerReasonMaxAge,
			JSMessageTTL, time.Duration(sdmTTL)*time.Second,
			JSMsgRollup, JSMsgRollupSubject,
		)
		msg := &inMsg{
			subj: subj,
			hdr:  hdr,
		}
		fs.pmsgcb(msg)
	} else {
		fs.rmcb(seq)
	}
}

// Will run through scheduled messages.
func (fs *fileStore) runMsgScheduling() {
	// TODO: Not great that we're holding the lock here, but the timed hash wheel and message scheduling isn't thread-safe.
	fs.mu.Lock()
	defer fs.mu.Unlock()

	// If scheduling is enabled, but handler isn't set up yet. Try again later.
	if fs.scheduling == nil || fs.scheduling.paused {
		return
	}
	if fs.pmsgcb == nil {
		fs.scheduling.resetTimer()
		return
	}
	fs.scheduling.running = true

	scheduledMsgs := fs.scheduling.getScheduledMessages(
		func(seq uint64, smv *StoreMsg) *StoreMsg {
			sm, _ := fs.msgForSeqLocked(seq, smv, false)
			return sm
		},
		func(subj string, smv *StoreMsg) *StoreMsg {
			sm, _ := fs.loadLastLocked(subj, smv)
			return sm
		},
	)
	if len(scheduledMsgs) > 0 {
		fs.mu.Unlock()
		for _, msg := range scheduledMsgs {
			fs.pmsgcb(msg)
		}
		fs.mu.Lock()
	}

	fs.scheduling.running, fs.scheduling.deadline = false, 0
	fs.scheduling.resetTimer()
}

// Lock should be held.
func (fs *fileStore) checkAndFlushLastBlock() error {
	lmb := fs.lmb
	if lmb == nil {
		return nil
	}
	lmb.mu.Lock()
	if err := lmb.werr; err != nil {
		lmb.mu.Unlock()
		return err
	}

	if lmb.pendingWriteSizeLocked() == 0 {
		lmb.mu.Unlock()
		return nil
	}
	ld, err := lmb.flushPendingMsgsLocked()
	lmb.mu.Unlock()
	if err != nil {
		return err
	}
	// Since fs lock is held need to unlock the mb in case we need to rebuild state.
	if ld != nil {
		fs.rebuildStateLocked(ld)
	}
	return nil
}

// This will check all the checksums on messages and report back any sequence numbers with errors.
func (fs *fileStore) checkMsgs() (*LostStreamData, error) {
	fs.mu.Lock()
	defer fs.mu.Unlock()

	var firstErr error
	storeErr := func(err error) error {
		fs.warn("checkMsgs: %v", err)
		if firstErr == nil {
			firstErr = err
		}
		return err
	}

	if err := fs.checkAndFlushLastBlock(); err != nil {
		return nil, storeErr(fmt.Errorf("flush of last block failed: %w", err))
	}

	// Clear any global subject state.
	fs.psim, fs.tsl = fs.psim.Empty(), 0

	for _, mb := range fs.blks {
		// Make sure encryption loaded if needed for the block.
		if err := fs.loadEncryptionForMsgBlock(mb); err != nil {
			_ = storeErr(fmt.Errorf("loading encryption for block %d failed: %w", mb.index, err))
			continue
		}
		// FIXME(dlc) - check tombstones here too?
		ld, _, err := mb.rebuildState()
		if err != nil {
			_ = storeErr(fmt.Errorf("rebuildState for block %d failed: %w", mb.index, err))
			continue
		}
		if ld != nil {
			// Rebuild fs state too.
			fs.rebuildStateLocked(ld)
		}
		if err = fs.populateGlobalPerSubjectInfo(mb); err != nil {
			_ = storeErr(fmt.Errorf("populating per-subject info for block %d failed: %w", mb.index, err))
			continue
		}
	}

	return fs.ld, firstErr
}

// Lock should be held.
func (mb *msgBlock) enableForWriting(fip bool) error {
	if mb == nil {
		return errNoMsgBlk
	}
	if mb.mfd != nil {
		return nil
	}
	mb.fs.dios.acquire()
	mfd, err := os.OpenFile(mb.mfn, os.O_CREATE|os.O_RDWR, defaultFilePerms)
	mb.fs.dios.release()
	if err != nil {
		return fmt.Errorf("error opening msg block file [%q]: %v", mb.mfn, err)
	}
	mb.mfd = mfd

	// Spin up our flusher loop if needed.
	if !fip {
		mb.spinUpFlushLoopLocked()
	}

	return nil
}

// Helper function to place a delete tombstone.
func (mb *msgBlock) writeTombstone(seq uint64, ts int64) error {
	return mb.writeMsgRecord(emptyRecordLen, seq|tbit, _EMPTY_, nil, nil, ts, true)
}

// Helper function to place a delete tombstone without flush.
// Lock should not be held.
func (mb *msgBlock) writeTombstoneNoFlush(seq uint64, ts int64) error {
	mb.mu.Lock()
	defer mb.mu.Unlock()
	return mb.writeMsgRecordLocked(emptyRecordLen, seq|tbit, _EMPTY_, nil, nil, ts, false, false)
}

// Will write the message record to the underlying message block.
// filestore lock will be held.
func (mb *msgBlock) writeMsgRecord(rl, seq uint64, subj string, mhdr, msg []byte, ts int64, flush bool) error {
	mb.mu.Lock()
	defer mb.mu.Unlock()
	return mb.writeMsgRecordLocked(rl, seq, subj, mhdr, msg, ts, flush, true)
}

// Will write the message record to the underlying message block.
// filestore lock will be held.
// mb lock should be held.
func (mb *msgBlock) writeMsgRecordLocked(rl, seq uint64, subj string, mhdr, msg []byte, ts int64, flush, kick bool) (rerr error) {
	// Return previous write errors immediately.
	if mb.werr != nil {
		return mb.werr
	}
	// Persist any returned errors to be used in the future.
	defer func() {
		if rerr != nil && mb.werr == nil {
			// Read/decode errors surfaced from existing on-disk data are not write failures.
			// Log and continue instead of disabling writes.
			if isReadErr(rerr) {
				mb.fs.rateLimitWarn("Ignoring non-write error: %v", rerr)
				assert.Unreachable("Filestore msg block encountered read error", map[string]any{
					"name":     mb.fs.cfg.Name,
					"mb.index": mb.index,
					"err":      rerr,
					"stack":    string(debug.Stack()),
				})
				return
			}
			mb.werr = rerr
			assert.Unreachable("Filestore msg block encountered write error", map[string]any{
				"name":     mb.fs.cfg.Name,
				"mb.index": mb.index,
				"err":      rerr,
				"stack":    string(debug.Stack()),
			})
		}
	}()

	// Enable for writing if our mfd is not open.
	if mb.mfd == nil {
		if err := mb.enableForWriting(flush && kick); err != nil {
			return err
		}
	}

	// Check if we are tracking per subject for our simple state.
	// Do this before changing the cache that would trigger a flush pending msgs call
	// if we needed to regenerate the per subject info.
	// Note that tombstones have no subject so will not trigger here.
	if len(subj) > 0 && !mb.noTrack {
		if err := mb.ensurePerSubjectInfoLoaded(); err != nil {
			return err
		}
		// Mark fss activity.
		mb.lsts = ats.AccessTime()
		if ss, ok := mb.fss.Find(stringToBytes(subj)); ok && ss != nil {
			ss.Msgs++
			ss.Last = seq
			ss.lastNeedsUpdate = false
		} else {
			mb.fss.Insert(stringToBytes(subj), SimpleState{Msgs: 1, First: seq, Last: seq})
		}
	}

	// Make sure we have a cache setup. Do so after ensurePerSubjectInfoLoaded as it may
	// have already brought in a cache for us.
	if err := mb.setupWriteCache(nil); err != nil {
		return err
	}

	// Make sure that the GC can't take away our writes by strengthening the elastic
	// reference. It will now stay strong until the flusher decides it is time to weaken.
	mb.ecache.Strengthen()

	// Make sure we have enough space to write into. If we don't then we can pull a buffer
	// from the next pool size up to save us from reallocating in append() below.
	if nsz := len(mb.cache.buf) + int(rl); cap(mb.cache.buf) < nsz {
		prev := mb.cache.buf
		mb.cache.stopRecycle()
		mb.cache.buf = getMsgBlockBuf(nsz)
		if prev != nil {
			mb.cache.buf = mb.cache.buf[:copy(mb.cache.buf[:nsz], prev)]
			recycleMsgBlockBuf(prev)
		}
		mb.cache.registerRecycle()
	}

	// Indexing
	index := len(mb.cache.buf)

	// Formats
	// Format with no header
	// total_len(4) sequence(8) timestamp(8) subj_len(2) subj msg hash(8)
	// With headers, high bit on total length will be set.
	// total_len(4) sequence(8) timestamp(8) subj_len(2) subj hdr_len(4) hdr msg hash(8)

	var le = binary.LittleEndian

	l := uint32(rl)
	hasHeaders := len(mhdr) > 0
	if hasHeaders {
		l |= hbit
	}

	// Reserve space for the header on the underlying buffer.
	mb.cache.buf = append(mb.cache.buf, make([]byte, msgHdrSize)...)
	hdr := mb.cache.buf[len(mb.cache.buf)-msgHdrSize : len(mb.cache.buf)]
	le.PutUint32(hdr[0:], l)
	le.PutUint64(hdr[4:], seq)
	le.PutUint64(hdr[12:], uint64(ts))
	le.PutUint16(hdr[20:], uint16(len(subj)))

	// Now write to underlying buffer.
	mb.cache.buf = append(mb.cache.buf, subj...)

	if hasHeaders {
		var hlen [4]byte
		le.PutUint32(hlen[0:], uint32(len(mhdr)))
		mb.cache.buf = append(mb.cache.buf, hlen[:]...)
		mb.cache.buf = append(mb.cache.buf, mhdr...)
	}
	mb.cache.buf = append(mb.cache.buf, msg...)

	// Calculate hash.
	mb.hh.Reset()
	mb.hh.Write(hdr[4:20])
	mb.hh.Write(stringToBytes(subj))
	if hasHeaders {
		mb.hh.Write(mhdr)
	}
	mb.hh.Write(msg)
	checksum := mb.hh.Sum(mb.lchk[:0:highwayhash.Size64])
	copy(mb.lchk[0:], checksum)

	// Update write through cache.
	// Write to msg record.
	mb.cache.buf = append(mb.cache.buf, checksum...)

	// Set cache timestamp for last store.
	mb.lwts = ts

	// Only update index and do accounting if not a delete tombstone.
	if seq&tbit == 0 {
		last := atomic.LoadUint64(&mb.last.seq)
		// Accounting, do this before stripping ebit, it is ebit aware.
		mb.updateAccounting(seq, ts, rl)
		// Strip ebit if set.
		seq = seq &^ ebit
		// If we have a hole due to skipping many messages, fill it.
		if len(mb.cache.idx) > 0 && last+1 < seq {
			for dseq := last + 1; dseq < seq; dseq++ {
				mb.cache.idx = append(mb.cache.idx, dbit)
			}
		}
		// Write index
		if mb.cache.idx = append(mb.cache.idx, uint32(index)|cbit); len(mb.cache.idx) == 1 {
			mb.cache.fseq = seq
		}
	} else {
		// If the block is empty, still adjust the accounting accordingly.
		tseq := seq &^ tbit
		if mb.msgs == 0 && tseq > atomic.LoadUint64(&mb.last.seq) {
			atomic.StoreUint64(&mb.last.seq, tseq)
			mb.last.ts = ts
			atomic.StoreUint64(&mb.first.seq, tseq+1)
			mb.first.ts = 0
		}
		// Make sure to account for tombstones in rbytes.
		mb.rbytes += rl
	}

	fch, werr := mb.fch, mb.werr
	if werr != nil {
		return werr
	}

	// If we should be flushing, or had a write error, do so here.
	if flush && mb.fs.fip {
		ld, err := mb.flushPendingMsgsLocked()
		if ld != nil {
			// We have the mb lock here, this needs the mb locks so do in its own go routine.
			go mb.fs.rebuildState(ld)
		}
		if err != nil {
			return err
		}
	} else if kick {
		// Kick the flusher here.
		kickFlusher(fch)
	}
	return nil
}

// How many bytes pending to be written for this message block.
func (mb *msgBlock) pendingWriteSize() int {
	if mb == nil {
		return 0
	}
	mb.mu.RLock()
	defer mb.mu.RUnlock()
	return mb.pendingWriteSizeLocked()
}

// How many bytes pending to be written for this message block.
func (mb *msgBlock) pendingWriteSizeLocked() int {
	if mb == nil {
		return 0
	}
	var pending int
	if !mb.closed && mb.mfd != nil && mb.cache != nil {
		pending = len(mb.cache.buf) - int(mb.cache.wp)
	}
	return pending
}

// Try to close our FDs if we can.
func (mb *msgBlock) closeFDs() error {
	mb.mu.Lock()
	defer mb.mu.Unlock()
	return mb.closeFDsLocked()
}

func (mb *msgBlock) closeFDsLocked() error {
	if buf, _ := mb.bytesPending(); len(buf) > 0 {
		return errPendingData
	}
	mb.closeFDsLockedNoCheck()
	return nil
}

func (mb *msgBlock) closeFDsLockedNoCheck() {
	if mb.mfd != nil {
		_ = mb.mfd.Close()
		mb.mfd = nil
	}
}

// bytesPending returns the buffer to be used for writing to the underlying file.
// This marks we are in flush and will return nil if asked again until cleared.
// Lock should be held.
func (mb *msgBlock) bytesPending() ([]byte, error) {
	if mb == nil || mb.mfd == nil {
		return nil, errNoPending
	}
	if mb.cache == nil {
		return nil, errNoCache
	}
	if len(mb.cache.buf) <= mb.cache.wp {
		return nil, errNoPending
	}
	buf := mb.cache.buf[mb.cache.wp:]
	if len(buf) == 0 {
		return nil, errNoPending
	}
	return buf, nil
}

// Returns the current blkSize including deleted msgs etc.
func (mb *msgBlock) blkSize() uint64 {
	if mb == nil {
		return 0
	}
	mb.mu.RLock()
	nb := mb.rbytes
	mb.mu.RUnlock()
	return nb
}

// Update accounting on a write msg.
// Lock should be held.
func (mb *msgBlock) updateAccounting(seq uint64, ts int64, rl uint64) {
	isDeleted := seq&ebit != 0
	if isDeleted {
		seq = seq &^ ebit
	}

	fseq := atomic.LoadUint64(&mb.first.seq)
	if (fseq == 0 || mb.first.ts == 0) && seq >= fseq {
		atomic.StoreUint64(&mb.first.seq, seq)
		mb.first.ts = ts
	}
	// Need atomics here for selectMsgBlock speed.
	atomic.StoreUint64(&mb.last.seq, seq)
	mb.last.ts = ts
	mb.rbytes += rl
	if !isDeleted {
		mb.bytes += rl
		mb.msgs++
	}
}

// Helper to check last msg block and create new one if too big.
// Lock should be held.
func (fs *fileStore) checkLastBlock(rl uint64) (lmb *msgBlock, err error) {
	// Grab our current last message block.
	lmb = fs.lmb
	rbytes := lmb.blkSize()
	if lmb == nil || (rbytes > 0 && rbytes+rl > fs.fcfg.BlockSize) {
		if lmb, err = fs.newMsgBlockForWrite(); err != nil {
			return nil, err
		}
	}
	return lmb, nil
}

// Lock should be held.
func (fs *fileStore) writeMsgRecord(seq uint64, ts int64, subj string, hdr, msg []byte) (uint64, error) {
	// Get size for this message.
	rl := fileStoreMsgSize(subj, hdr, msg)
	if isFileStoreMsgTooLarge(rl) {
		return 0, ErrMsgTooLarge
	}
	// Grab our current last message block.
	mb, err := fs.checkLastBlock(rl)
	if err != nil {
		return 0, err
	}

	// Mark as dirty for stream state.
	fs.dirty++

	// Ask msg block to store in write through cache.
	err = mb.writeMsgRecord(rl, seq, subj, hdr, msg, ts, fs.fip)

	return rl, err
}

// For writing tombstones to our lmb. This version will enforce maximum block sizes.
// Lock should be held.
func (fs *fileStore) writeTombstone(seq uint64, ts int64) error {
	// Grab our current last message block.
	lmb, err := fs.checkLastBlock(emptyRecordLen)
	if err != nil {
		return err
	}
	return lmb.writeTombstone(seq, ts)
}

// For writing tombstones to our lmb. This version will enforce maximum block sizes.
// This version does not flush contents.
// Lock should be held.
func (fs *fileStore) writeTombstoneNoFlush(seq uint64, ts int64) error {
	lmb, err := fs.checkLastBlock(emptyRecordLen)
	if err != nil {
		return err
	}
	// Write tombstone without flush or kick.
	return lmb.writeTombstoneNoFlush(seq, ts)
}

// Lock should be held.
func (mb *msgBlock) recompressOnDiskIfNeeded() error {
	// If the block has been closed in the meantime, skip.
	if mb.closed {
		return nil
	}

	alg := mb.fs.fcfg.Compression

	// Open up the file block and read in the entire contents into memory.
	// One of two things will happen:
	// 1. The block will be compressed already and have a valid metadata
	//    header, in which case we do nothing.
	// 2. The block will be uncompressed, in which case we will compress it
	//    and then write it back out to disk, re-encrypting if necessary.
	mb.fs.dios.acquire()
	origBuf, err := os.ReadFile(mb.mfn)
	mb.fs.dios.release()

	if err != nil {
		return fmt.Errorf("failed to read original block from disk: %w", err)
	}

	// If the block is encrypted then we will need to decrypt it before
	// doing anything. We always encrypt after compressing because then the
	// compression can be as efficient as possible on the raw data, whereas
	// the encrypted ciphertext will not compress anywhere near as well.
	// The block encryption also covers the optional compression metadata.
	if err = mb.encryptOrDecryptIfNeeded(origBuf); err != nil {
		return err
	}

	decoded, diskAlg, err := mb.decode(origBuf)
	if err != nil {
		return fmt.Errorf("failed to decode original block: %w", err)
	}
	if diskAlg == alg {
		// The block is already compressed with the chosen algorithm so there
		// is nothing else to do. This is not a common case, it is here only
		// to ensure we don't do unnecessary work in case something asked us
		// to recompress an already compressed block with the same algorithm.
		return nil
	}

	return mb.atomicOverwriteFile(decoded, true)
}

// Lock should be held.
func (mb *msgBlock) atomicOverwriteFile(buf []byte, allowCompress bool) error {
	if mb.mfd != nil {
		mb.closeFDsLockedNoCheck()
		defer mb.enableForWriting(mb.fs.fip)
	}

	origFN := mb.mfn               // The original message block on disk.
	tmpFN := mb.mfn + blkTmpSuffix // The new block will be written here.

	// Rather than modifying the existing block on disk (which is a dangerous
	// operation if something goes wrong), create a new temporary file. We will
	// write out the new block here and then swap the files around afterwards
	// once everything else has succeeded correctly.
	mb.fs.dios.acquire()
	tmpFD, err := os.OpenFile(tmpFN, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, defaultFilePerms)
	mb.fs.dios.release()

	if err != nil {
		return fmt.Errorf("failed to create temporary file: %w", err)
	}

	errorCleanup := func(err error) error {
		_ = tmpFD.Close()
		_ = os.Remove(tmpFN)
		return err
	}

	alg := NoCompression
	if calg := mb.fs.fcfg.Compression; calg != NoCompression && allowCompress {
		alg = calg
		// The original buffer at this point is uncompressed, so we will now compress
		// it if needed. Note that if the selected algorithm is NoCompression, the
		// Compress function will just return the input buffer unmodified.
		originalSize := len(buf)
		if buf, err = alg.Compress(buf); err != nil {
			return errorCleanup(fmt.Errorf("failed to compress block: %w", err))
		}

		// We only need to write out the metadata header if compression is enabled.
		// If we're trying to uncompress the file on disk at this point, don't bother
		// writing metadata.
		meta := &CompressionInfo{
			Algorithm:    alg,
			OriginalSize: uint64(originalSize),
		}
		buf = append(meta.MarshalMetadata(), buf...)
	}

	// Re-encrypt the block if necessary.
	if err = mb.encryptOrDecryptIfNeeded(buf); err != nil {
		return errorCleanup(err)
	}

	// Write the new block data (which might be compressed or encrypted) to the
	// temporary file.
	if n, err := tmpFD.Write(buf); err != nil {
		return errorCleanup(fmt.Errorf("failed to write to temporary file: %w", err))
	} else if n != len(buf) {
		return errorCleanup(fmt.Errorf("short write to temporary file (%d != %d)", n, len(buf)))
	}
	if err := tmpFD.Sync(); err != nil {
		return errorCleanup(fmt.Errorf("failed to sync temporary file: %w", err))
	}
	if err := tmpFD.Close(); err != nil {
		return errorCleanup(fmt.Errorf("failed to close temporary file: %w", err))
	}

	// Now replace the original file with the newly updated temp file.
	if err := os.Rename(tmpFN, origFN); err != nil {
		return fmt.Errorf("failed to move temporary file into place: %w", err)
	}

	// Since the message block might be retained in memory, make sure the
	// compression algorithm is up-to-date, since this will be needed when
	// compacting or truncating.
	mb.cmp = alg

	// Also update rbytes
	mb.rbytes = uint64(len(buf))

	return nil
}

// Lock should be held.
func (mb *msgBlock) decode(buf []byte) ([]byte, StoreCompression, error) {
	var meta CompressionInfo
	if n, err := meta.UnmarshalMetadata(buf); err != nil {
		// There was a problem parsing the metadata header of the block.
		// If there's no metadata header, an error isn't returned here,
		// we will instead just use default values of no compression.
		return nil, meta.Algorithm, err
	} else if n == 0 {
		// There were no metadata bytes, so we assume the block is not
		// compressed and return it as-is.
		return buf, NoCompression, nil
	} else {
		// Metadata was present, so try to decompress the block.
		decoded, err := meta.Algorithm.Decompress(buf[n:])
		if err == nil {
			return decoded, meta.Algorithm, nil
		}
		// Decompression failed. Before returning the error, check if the
		// buffer happens to store a valid uncompressed record.
		// A record with length 24145251 without headers happens to
		// collide with a valid compression header "cmp" and
		// compression algorithm 1 (S2).
		if meta.Algorithm == S2Compression && len(buf) >= 24145251 {
			var sm StoreMsg
			if _, msgErr := mb.msgFromBufNoCopy(buf, &sm, mb.hh); msgErr == nil {
				return buf, NoCompression, nil
			}
		}
		// Return the original error
		return nil, meta.Algorithm, err
	}
}

// Lock should be held.
func (mb *msgBlock) decompressIfNeeded(buf []byte) ([]byte, error) {
	decoded, _, err := mb.decode(buf)
	return decoded, err
}

// Lock should be held.
func (mb *msgBlock) encryptOrDecryptIfNeeded(buf []byte) error {
	if mb.bek != nil && len(buf) > 0 {
		bek, err := genBlockEncryptionKey(mb.fs.fcfg.Cipher, mb.seed, mb.nonce)
		if err != nil {
			return err
		}
		mb.bek = bek
		mb.bek.XORKeyStream(buf, buf)
	}
	return nil
}

// Lock should be held.
func (mb *msgBlock) ensureRawBytesLoaded() error {
	if mb.rbytes > 0 {
		return nil
	}
	f, err := mb.openBlock()
	if err != nil {
		return err
	}
	defer f.Close()
	if fi, err := f.Stat(); fi != nil && err == nil {
		mb.rbytes = uint64(fi.Size())
	} else {
		return err
	}
	return nil
}

// Lock should be held.
func (mb *msgBlock) syncFile() error {
	mb.fs.dios.acquire()
	defer mb.fs.dios.release()
	fd, didOpen := mb.mfd, false
	if fd == nil {
		var err error
		fd, err = os.OpenFile(mb.mfn, os.O_RDWR, defaultFilePerms)
		if err != nil {
			if os.IsNotExist(err) {
				return nil
			}
			return err
		}
		didOpen = true
	}
	if err := fd.Sync(); err != nil {
		// Close fd if we opened it, but ignore its error since sync takes precedence.
		if didOpen {
			_ = fd.Close()
		}
		return err
	}
	if didOpen {
		return fd.Close()
	}
	return nil
}

// Sync msg and index files as needed. This is called from a timer.
func (fs *fileStore) syncBlocks() {
	fs.syncMu.Lock()
	defer fs.syncMu.Unlock()

	if fs.isClosed() {
		return
	}
	fs.mu.Lock()
	if err := fs.werr; err != nil {
		fs.mu.Unlock()
		return
	}
	blks := append([]*msgBlock(nil), fs.blks...)
	lmb, firstMoved, firstSeq := fs.lmb, fs.firstMoved, fs.state.FirstSeq
	// Clear first moved.
	fs.firstMoved = false
	fs.mu.Unlock()

	storeFsWerr := func(err error) {
		// A file or directory that no longer exists was removed by a
		// concurrent purge, compact or delete. Syncing it is moot, not a
		// write failure.
		if os.IsNotExist(err) {
			return
		}
		fs.mu.Lock()
		defer fs.mu.Unlock()
		fs.setWriteErr(err)
	}

	var fsDmap *interiorDeletes

	var markDirty bool
	var needDirSync bool
	for _, mb := range blks {
		// Do actual sync. Hold lock for consistency.
		mb.mu.Lock()
		if mb.closed {
			mb.mu.Unlock()
			continue
		}
		// Bubble up an individual block error into the broader filestore.
		if err := mb.werr; err != nil {
			mb.mu.Unlock()
			storeFsWerr(err)
			continue
		}
		// See if we can close FDs due to being idle.
		if mb.mfd != nil && mb.sinceLastWriteActivity() > closeFDsIdle && mb.pendingWriteSizeLocked() == 0 {
			if err := mb.dirtyCloseWithRemove(false); err != nil {
				mb.mu.Unlock()
				storeFsWerr(err)
				continue
			}
		}
		// If our first has moved and we are set to noCompact (which is from tombstones),
		// clear so that we might cleanup tombstones.
		if firstMoved && mb.noCompact {
			mb.noCompact = false
		}
		// Check if we should compact here as well.
		// Do not compact last mb.
		var needsCompact bool
		if mb != lmb && mb.ensureRawBytesLoaded() == nil && mb.shouldCompactSync() {
			needsCompact = true
			markDirty = true
		}

		// Flush anything that may be pending.
		if _, err := mb.flushPendingMsgsLocked(); err != nil {
			mb.mu.Unlock()
			storeFsWerr(err)
			continue
		}
		// Check if we need to sync. We will not hold lock during actual sync.
		needSync := mb.needSync
		needKeySync, kfn := mb.needKeySync, mb.kfn

		// Reset. Because we let go of the lock, we could write new data to this mb which might or
		// might not be synced later if we would've reset after letting go of the lock.
		mb.needSync = false
		mb.needKeySync = false
		mb.mu.Unlock()

		// Check if we should compact here.
		// Need to hold fs lock in case we reference psim when loading in the mb and we may remove this block if truly empty.
		if needsCompact {
			// Load a delete map containing only interior deletes.
			// This is used when compacting to know if tombstones are still relevant,
			// and if not they can be compacted.
			if fsDmap == nil {
				fsDmap = deleteMap(blks)
			}
			fs.mu.RLock()
			mb.mu.Lock()
			// If the block has already been removed in the meantime, we can simply skip.
			if _, ok := fs.bim[mb.index]; !ok {
				mb.mu.Unlock()
				fs.mu.RUnlock()
				continue
			}
			err := mb.compactWithFloor(firstSeq, fsDmap)
			// If this compact removed all raw bytes due to tombstone cleanup, schedule to remove.
			shouldRemove := mb.rbytes == 0
			needSync = mb.needSync
			mb.mu.Unlock()
			fs.mu.RUnlock()
			if err != nil {
				storeFsWerr(err)
				continue
			}
			needDirSync = needDirSync || needSync || shouldRemove

			// Check if we should remove. This will not be common, so we will re-take fs write lock here vs changing
			//  it above which we would prefer to be a readlock such that other lookups can occur while compacting this block.
			if shouldRemove {
				fs.mu.Lock()
				mb.mu.Lock()
				err = fs.removeMsgBlock(mb)
				mb.mu.Unlock()
				fs.mu.Unlock()
				needSync = false
				if err != nil {
					storeFsWerr(err)
					continue
				}
			}
		}

		// Check if we need to sync this block's key file.
		if needKeySync && kfn != _EMPTY_ {
			if err := fs.syncFileAndDir(kfn); err != nil {
				storeFsWerr(err)
				continue
			}
		}

		// Check if we need to sync this block.
		if needSync {
			mb.mu.Lock()
			err := mb.syncFile()
			if err == nil {
				mb.needSync = false
			}
			mb.mu.Unlock()
			if err != nil {
				storeFsWerr(err)
				continue
			}
		}
	}
	if needDirSync && canFsyncDirectories {
		fs.dios.acquire()
		err := syncDir(filepath.Join(fs.fcfg.StoreDir, msgDir))
		fs.dios.release()
		if err != nil {
			storeFsWerr(err)
		}
	}

	if fs.isClosed() {
		return
	}
	fs.mu.Lock()
	defer fs.mu.Unlock()
	fs.setSyncTimer()
	if markDirty {
		fs.dirty++
	}

	// Sync state file if we are not running with sync always.
	if !fs.syncAlways.Load() {
		fn := filepath.Join(fs.fcfg.StoreDir, msgDir, streamStreamStateFile)
		var fd *os.File
		var err error
		fs.dios.acquire()
		fd, err = os.OpenFile(fn, os.O_RDWR, defaultFilePerms)
		fs.dios.release()
		if err != nil && !os.IsNotExist(err) {
			fs.setWriteErr(err)
			return
		}
		if fd != nil {
			if err = fd.Sync(); err != nil {
				// Close fd, but ignore its error since sync takes precedence.
				_ = fd.Close()
				fs.setWriteErr(err)
				return
			}
			if err = fd.Close(); err != nil {
				fs.setWriteErr(err)
				return
			}
		}
	}
}

// Select the message block where this message should be found.
// Return nil if not in the set.
// Read lock should be held.
func (fs *fileStore) selectMsgBlock(seq uint64) *msgBlock {
	_, mb := fs.selectMsgBlockWithIndex(seq)
	return mb
}

// Lock should be held.
func (fs *fileStore) selectMsgBlockWithIndex(seq uint64) (int, *msgBlock) {
	// Check for out of range.
	if seq < fs.state.FirstSeq || seq > fs.state.LastSeq || fs.state.Msgs == 0 {
		return -1, nil
	}

	const linearThresh = 32
	nb := len(fs.blks) - 1

	if nb < linearThresh {
		for i, mb := range fs.blks {
			if seq <= atomic.LoadUint64(&mb.last.seq) {
				return i, mb
			}
		}
		return -1, nil
	}

	// Do traditional binary search here since we know the blocks are sorted by sequence first and last.
	for low, high, mid := 0, nb, nb/2; low <= high; mid = (low + high) / 2 {
		mb := fs.blks[mid]
		// Right now these atomic loads do not factor in, so fine to leave. Was considering
		// uplifting these to fs scope to avoid atomic load but not needed.
		first, last := atomic.LoadUint64(&mb.first.seq), atomic.LoadUint64(&mb.last.seq)
		if seq > last {
			low = mid + 1
		} else if seq < first {
			// A message block's first sequence can change here meaning we could find a gap.
			// We want to behave like above, which if inclusive (we check at start) should
			// always return an index and a valid mb.
			// If we have a gap then our seq would be > fs.blks[mid-1].last.seq
			if mid == 0 || seq > atomic.LoadUint64(&fs.blks[mid-1].last.seq) {
				return mid, mb
			}
			high = mid - 1
		} else {
			return mid, mb
		}
	}

	return -1, nil
}

// Select the message block where this message should be found.
// Return nil if not in the set.
func (fs *fileStore) selectMsgBlockForStart(minTime time.Time) *msgBlock {
	fs.mu.RLock()
	defer fs.mu.RUnlock()

	// Binary search for first block where last.ts >= t.
	i, _ := slices.BinarySearchFunc(fs.blks, minTime.UnixNano(), func(mb *msgBlock, target int64) int {
		mb.mu.RLock()
		last := mb.last.ts
		mb.mu.RUnlock()
		switch {
		case last < target:
			return -1
		case last > target:
			return 1
		default:
			return 0
		}
	})

	// BinarySearchFunc returns an insertion point if not found.
	// Either way, i is the index of the first mb where mb.last.ts >= t.
	if i < len(fs.blks) {
		return fs.blks[i]
	}
	return nil
}

// Index a raw msg buffer.
// Lock should be held.
func (mb *msgBlock) indexCacheBuf(buf []byte) error {
	var le = binary.LittleEndian

	var alloc bool
	var idx []uint32
	var index uint32

	mbFirstSeq := atomic.LoadUint64(&mb.first.seq)
	mbLastSeq := atomic.LoadUint64(&mb.last.seq)

	// Sanity check here since we calculate size to allocate based on this.
	if mbFirstSeq > (mbLastSeq + 1) { // Purged state first == last + 1
		mb.fs.warn("indexCacheBuf corrupt state in %s: mb.first %d mb.last %d", mb.mfn, mbFirstSeq, mbLastSeq)
		// This would cause idxSz to wrap.
		return errCorruptState
	}

	idxSz := mbLastSeq - mbFirstSeq + 1

	if mb.cache == nil {
		mb.cache = mb.ecache.Value()
	}
	if mb.cache == nil {
		mb.cache = &cache{}
		alloc = true
	} else {
		// The buf arg already came from the pool probably, so there's
		// no point in reusing mb.cache.buf's underlying capacity here.
		// Just recycle it for the next block load.
		mb.cache.stopRecycle()
		recycleMsgBlockBuf(mb.cache.buf)
	}
	if idx = mb.cache.idx; uint64(cap(idx)) >= idxSz {
		idx = idx[:0]
	} else {
		idx = make([]uint32, 0, idxSz)
	}
	if !alloc {
		// We didn't allocate a new *cache so instead wipe the current
		// one, we already have reused idx if possible.
		*mb.cache = cache{}
	}

	// Create FSS if we should track.
	var popFss bool
	if mb.fssNotLoaded() {
		mb.fss = stree.NewSubjectTree[SimpleState]()
		popFss = true
	}
	// Mark fss activity.
	mb.lsts = ats.AccessTime()
	mb.ttls = 0
	mb.schedules = 0

	lbuf := uint32(len(buf))
	var seq, ttls, schedules uint64
	var sm StoreMsg // Used for finding headers

	// To ensure the sequence keeps moving up. As well as confirming our index
	// is aligned with the mb's first and last sequence.
	var first uint64
	var last uint64

	for index < lbuf {
		if index+msgHdrSize > lbuf {
			return errCorruptState
		}
		hdr := buf[index : index+msgHdrSize]
		rl, slen := le.Uint32(hdr[0:]), int(le.Uint16(hdr[20:]))
		seq = le.Uint64(hdr[4:])

		hasHeaders := rl&hbit != 0
		// Clear any headers bit that could be set.
		rl &^= hbit
		shlen := slen
		if hasHeaders {
			shlen += 4
		}
		dlen := int(rl) - msgHdrSize
		// Do some quick sanity checks here.
		if dlen < 0 || shlen > (dlen-recordHashSize) || dlen > int(rl) || index+rl > lbuf || rl > rlBadThresh {
			mb.fs.warn("indexCacheBuf corrupt record state in %s: dlen %d slen %d index %d rl %d lbuf %d", mb.mfn, dlen, slen, index, rl, lbuf)
			// This means something is off.
			// TODO(dlc) - Add into bad list?
			return errCorruptState
		}

		// Check for tombstones which we can skip in terms of indexing.
		if seq&tbit != 0 {
			index += rl
			continue
		}

		// Clear any erase bits.
		erased := seq&ebit != 0
		seq = seq &^ ebit

		// The sequence needs to only ever move up.
		if seq <= last {
			// Advance to next record.
			// We've already accounted for this sequence and marked it as deleted.
			index += rl
			continue
		}
		// We defer checksum checks to individual msg cache lookups to amortorize costs and
		// not introduce latency for first message from a newly loaded block.
		if seq >= mbFirstSeq {
			last = seq

			// If the first sequence doesn't align with what we had in-memory, we need to rebuild.
			if first == 0 {
				first = seq
				if mbFirstSeq != first {
					return errCorruptState
				}
			}

			// Track that we do not have holes.
			if slot := int(seq - mbFirstSeq); slot != len(idx) {
				// If we have a hole fill it.
				for dseq := mbFirstSeq + uint64(len(idx)); dseq < seq; dseq++ {
					idx = append(idx, dbit)
					if dseq != 0 {
						mb.dmap.Insert(dseq)
					}
				}
			}
			// Add to our index.
			idx = append(idx, index)

			// Make sure our dmap has this entry if it was erased.
			// If not, that means this erased message was not accounted for in our in-memory state.
			if erased && seq != 0 && !mb.dmap.Exists(seq) {
				return errCorruptState
			}

			// Handle FSS inline here.
			if popFss && slen > 0 && !mb.noTrack && !erased && !mb.dmap.Exists(seq) {
				bsubj := buf[index+msgHdrSize : index+msgHdrSize+uint32(slen)]
				if ss, ok := mb.fss.Find(bsubj); ok && ss != nil {
					ss.Msgs++
					ss.Last = seq
					ss.lastNeedsUpdate = false
				} else {
					mb.fss.Insert(bsubj, SimpleState{
						Msgs:  1,
						First: seq,
						Last:  seq,
					})
				}
			}

			// Count how many TTLs/schedules we think are in this message block.
			// TODO(nat): Not terribly optimal...
			if hasHeaders {
				if fsm, err := mb.msgFromBufNoCopy(buf[index:], &sm, nil); err == nil && fsm != nil {
					if ttl := sliceHeader(JSMessageTTL, fsm.hdr); len(ttl) > 0 {
						ttls++
					}
					if sched := sliceHeader(JSSchedulePattern, fsm.hdr); len(sched) > 0 {
						schedules++
					}
				}
			}
		}
		index += rl
	}

	// If we ended up with a smaller or larger index, or the first/last sequence
	// doesn't align with what we had in-memory, we need to rebuild.
	if len(idx) != int(idxSz) || (first > 0 && mbFirstSeq != first) || (last > 0 && mbLastSeq != last) {
		return errCorruptState
	}

	mb.cache.buf = buf
	mb.cache.idx = idx
	mb.cache.fseq = mbFirstSeq
	mb.cache.wp = int(lbuf)
	mb.ttls = ttls
	mb.schedules = schedules
	mb.cache.registerRecycle()

	return nil
}

// flushPendingMsgs writes out any messages for this message block.
func (mb *msgBlock) flushPendingMsgs() error {
	mb.mu.Lock()
	fsLostData, err := mb.flushPendingMsgsLocked()
	fs := mb.fs
	mb.mu.Unlock()

	// Signals us that we need to rebuild filestore state.
	if fsLostData != nil && fs != nil {
		// Rebuild fs state too.
		fs.rebuildState(fsLostData)
	}
	return err
}

// Write function for actual data.
// mb.mfd should not be nil.
// Lock should held.
func (mb *msgBlock) writeAt(buf []byte, woff int64) (int, error) {
	// Used to mock write failures.
	if mb.mockWriteErr {
		// Reset on trip.
		mb.mockWriteErr = false
		return 0, errors.New("mock write error")
	}
	mb.fs.dios.acquire()
	n, err := mb.mfd.WriteAt(buf, woff)
	mb.fs.dios.release()
	return n, err
}

// flushPendingMsgsLocked writes out any messages for this message block.
// Lock should be held.
func (mb *msgBlock) flushPendingMsgsLocked() (*LostStreamData, error) {
	var weakenCache bool
	if mb.cache == nil {
		mb.cache = mb.ecache.Value()
		weakenCache = mb.cache != nil
	}
	if mb.cache == nil || mb.mfd == nil || mb.werr != nil {
		return nil, mb.werr
	}

	buf, err := mb.bytesPending()
	// If we got an error back return here.
	if err != nil {
		// No pending data to be written is not an error.
		if err == errNoPending || err == errNoCache {
			err = nil
		}
		return nil, err
	}

	wp := int64(mb.cache.wp)
	lob := len(buf)

	// TODO(dlc) - Normally we would not hold the lock across I/O so we can improve performance.
	// We will hold to stabilize the code base, as we have had a few anomalies with partial cache errors
	// under heavy load.

	// Check if we need to encrypt.
	if err := mb.checkAndLoadEncryption(); err != nil {
		return nil, err
	}
	if mb.bek != nil && lob > 0 {
		// Need to leave original alone.
		var dst []byte
		if lob <= defaultLargeBlockSize {
			dst = getMsgBlockBuf(lob)[:lob]
			defer recycleMsgBlockBuf(dst)
		} else {
			dst = make([]byte, lob)
		}
		mb.bek.XORKeyStream(dst, buf)
		buf = dst
	}

	// Append new data to the message block file.
	for lbb := lob; lbb > 0; lbb = len(buf) {
		n, err := mb.writeAt(buf, wp)
		if err != nil {
			// Ignore the errors here, we'll try reloading just to figure out and return the lost data if we can.
			_ = mb.dirtyCloseWithRemove(false)
			ld, _, _ := mb.rebuildStateLocked()
			mb.werr = err
			assert.Unreachable("Filestore msg block encountered flush error", map[string]any{
				"name":     mb.fs.cfg.Name,
				"mb.index": mb.index,
				"err":      err,
				"stack":    string(debug.Stack()),
			})
			return ld, err
		}
		// Update our write offset.
		wp += int64(n)
		buf = buf[n:]
	}

	// Cache may be gone.
	if mb.cache == nil || mb.mfd == nil {
		return nil, mb.werr
	}

	// Update write pointer.
	mb.cache.wp = int(wp)

	// Check if we are in sync always mode.
	if mb.fs.syncAlways.Load() {
		if err = mb.mfd.Sync(); err != nil {
			mb.werr = err
			assert.Unreachable("Filestore msg block encountered sync error", map[string]any{
				"name":     mb.fs.cfg.Name,
				"mb.index": mb.index,
				"err":      err,
				"stack":    string(debug.Stack()),
			})
			return nil, err
		}
	} else {
		mb.needSync = true
	}

	// Check for additional writes while we were writing to the disk.
	// TODO(nat): This should not be possible at present since, as above, we are
	// not releasing the lock during I/O operation. Therefore this will always
	// return zero.
	if mb.pendingWriteSizeLocked() > 0 {
		return nil, mb.werr
	}

	// Check last access time. If we think the block still has read interest
	// then we will weaken the pointer but otherwise try to hold onto it.
	if ts := ats.AccessTime(); ts < mb.llts || (ts-mb.llts) <= int64(mb.cexp) {
		if weakenCache {
			mb.cache = nil
			mb.ecache.Weaken()
		}
		mb.resetCacheExpireTimer(0)
		return nil, mb.werr
	}

	// If not, we'll just drop the cache altogether & recycle the buffer.
	mb.cache.nra = false
	mb.tryExpireCacheLocked()
	return nil, mb.werr
}

// Lock should be held.
func (mb *msgBlock) clearLoading() {
	mb.loading = false
}

// Will load msgs from disk.
func (mb *msgBlock) loadMsgs() error {
	// We hold the lock here the whole time by design.
	mb.mu.Lock()
	defer mb.mu.Unlock()
	return mb.loadMsgsWithLock()
}

// Lock should be held.
func (mb *msgBlock) cacheAlreadyLoaded() bool {
	if mb.cache == nil {
		mb.cache = mb.ecache.Value()
	}
	if mb.cache == nil || mb.cache.fseq == 0 || len(mb.cache.buf) == 0 {
		return false
	}
	numEntries := mb.msgs + uint64(mb.dmap.Size()) + (atomic.LoadUint64(&mb.first.seq) - mb.cache.fseq)
	return numEntries == uint64(len(mb.cache.idx))
}

// Lock should be held.
func (mb *msgBlock) cacheNotLoaded() bool {
	return !mb.cacheAlreadyLoaded()
}

// Report if our fss is not loaded.
// Lock should be held.
func (mb *msgBlock) fssNotLoaded() bool {
	return mb.fss == nil && !mb.noTrack
}

// Wrap openBlock for the gated semaphore processing.
// Lock should be held
func (mb *msgBlock) openBlock() (*os.File, error) {
	// Gate with concurrent IO semaphore.
	mb.fs.dios.acquire()
	f, err := os.Open(mb.mfn)
	mb.fs.dios.release()
	return f, err
}

// Used to load in the block contents.
// Lock should be held and all conditionals satisfied prior.
func (mb *msgBlock) loadBlock(buf []byte) ([]byte, error) {
	var f *os.File
	// Re-use if we have mfd open.
	if mb.mfd != nil {
		f = mb.mfd
		if n, err := f.Seek(0, 0); n != 0 || err != nil {
			f = nil
			mb.closeFDsLockedNoCheck()
		}
	}
	if f == nil {
		var err error
		f, err = mb.openBlock()
		if err != nil {
			if os.IsNotExist(err) {
				err = errNoBlkData
			}
			return nil, err
		}
		defer f.Close()
	}

	var sz int
	if info, err := f.Stat(); err == nil {
		sz64 := info.Size()
		if int64(int(sz64)) == sz64 {
			sz = int(sz64)
		} else {
			return nil, errMsgBlkTooBig
		}
	}

	if buf == nil || cap(buf) < sz {
		// getMsgBlockBuf will try to return a slice that fits from the
		// buffer pool, but if by chance `sz` is greater than the maximum
		// large block size, it'll return a new slice to fit regardless.
		buf = getMsgBlockBuf(sz)
	}

	mb.fs.dios.acquire()
	n, err := io.ReadFull(f, buf[:sz])
	mb.fs.dios.release()
	// On success capture raw bytes size.
	if err == nil {
		mb.rbytes = uint64(n)
	}
	return buf[:n], err
}

// Lock should be held.
func (mb *msgBlock) loadMsgsWithLock() error {
	if err := mb.checkAndLoadEncryption(); err != nil {
		return err
	}

	// Check to see if we are loading already.
	if mb.loading {
		return nil
	}

	// Set loading status.
	mb.loading = true
	defer mb.clearLoading()

	var nchecks int

checkCache:
	nchecks++
	if nchecks > 8 {
		return errNoCache
	}

	// Check to see if we have a full cache.
	if mb.cacheAlreadyLoaded() {
		return nil
	}

	mb.llts = ats.AccessTime()

	// FIXME(dlc) - We could be smarter here.
	if buf, _ := mb.bytesPending(); len(buf) > 0 {
		ld, err := mb.flushPendingMsgsLocked()
		if ld != nil && mb.fs != nil {
			// We do not know if fs is locked or not at this point.
			// This should be an exceptional condition so do so in Go routine.
			go mb.fs.rebuildState(ld)
		}
		if err != nil {
			return err
		}
		goto checkCache
	}

	// Load in the whole block.
	// We want to hold the mb lock here to avoid any changes to state.
	buf, err := mb.loadBlock(nil)
	if err != nil {
		mb.fs.warn("loadBlock error: %v", err)
		if err == errNoBlkData {
			if ld, _, _ := mb.rebuildStateLocked(); ld != nil {
				// Rebuild fs state too.
				go mb.fs.rebuildState(ld)
			}
		}
		return err
	}

	// Check if we need to decrypt.
	if err = mb.encryptOrDecryptIfNeeded(buf); err != nil {
		return err
	}
	// Check for compression.
	if buf, err = mb.decompressIfNeeded(buf); err != nil {
		return err
	}

	if err := mb.indexCacheBuf(buf); err != nil {
		if err == errCorruptState {
			var ld *LostStreamData
			ld, _, err = mb.rebuildStateLocked()
			// We do not know if fs is locked or not at this point.
			// This should be an exceptional condition so do so in Go routine.
			// Always rebuild the filestore's state if indexing fails, even if no data was lost,
			// our in-memory state was stale in that case.
			go mb.fs.rebuildState(ld)
		}
		if err != nil {
			return err
		}
		goto checkCache
	}

	if len(buf) > 0 {
		mb.cloads++
		mb.startCacheExpireTimer()
	}

	// Update the cache pointer since we cleared & rebuilt.
	mb.ecache.Set(mb.cache)

	return nil
}

// Fetch a message from this block, possibly reading in and caching the messages.
// We assume the block was selected and is correct, so we do not do range checks.
// Lock should not be held.
func (mb *msgBlock) fetchMsg(seq uint64, sm *StoreMsg) (*StoreMsg, bool, error) {
	mb.mu.Lock()
	defer mb.mu.Unlock()
	defer mb.finishedWithCache()
	return mb.fetchMsgEx(seq, sm, true)
}

// Fetch a message from this block, possibly reading in and caching the messages.
// We assume the block was selected and is correct, so we do not do range checks.
// We will not copy the msg data, the returned StoreMsg's subj/hdr/msg/buf are aliased
// into mb.cache.buf and are only safe to read while mb.mu is held.
func (mb *msgBlock) fetchMsgNoCopyLocked(seq uint64, sm *StoreMsg) (*StoreMsg, bool, error) {
	return mb.fetchMsgEx(seq, sm, false)
}

// Fetch a message from this block, possibly reading in and caching the messages.
// We assume the block was selected and is correct, so we do not do range checks.
// We will copy the msg data based on doCopy boolean.
// Lock should be held.
func (mb *msgBlock) fetchMsgEx(seq uint64, sm *StoreMsg, doCopy bool) (*StoreMsg, bool, error) {
	fseq, lseq := atomic.LoadUint64(&mb.first.seq), atomic.LoadUint64(&mb.last.seq)
	if seq < fseq || seq > lseq {
		return nil, false, ErrStoreMsgNotFound
	}

	// See if we can short circuit if we already know msg deleted.
	if mb.dmap.Exists(seq) {
		// Update for scanning like cacheLookup would have.
		llseq := mb.llseq
		if mb.llseq == 0 || seq < mb.llseq || seq == mb.llseq+1 || seq == mb.llseq-1 {
			mb.llseq = seq
		}
		expireOk := (seq == lseq && llseq == seq-1) || (seq == fseq && llseq == seq+1)
		return nil, expireOk, errDeletedMsg
	}

	if mb.cacheNotLoaded() {
		if err := mb.loadMsgsWithLock(); err != nil {
			return nil, false, err
		}
	}
	llseq := mb.llseq

	fsm, err := mb.cacheLookupEx(seq, sm, doCopy)
	if err != nil {
		return nil, false, err
	}
	expireOk := (seq == lseq && llseq == seq-1) || (seq == fseq && llseq == seq+1)
	return fsm, expireOk, nil
}

var (
	errNoCache       = errors.New("no message cache")
	errDeletedMsg    = errors.New("deleted message")
	errPartialCache  = errors.New("partial cache")
	errNoPending     = errors.New("message block does not have pending data")
	errNotReadable   = errors.New("storage directory not readable")
	errCorruptState  = errors.New("corrupt state file")
	errPriorState    = errors.New("prior state file")
	errPendingData   = errors.New("pending data still present")
	errNoEncryption  = errors.New("encryption not enabled")
	errBadKeySize    = errors.New("encryption bad key size")
	errKeyInvalid    = errors.New("unable to recover keys")
	errNoMsgBlk      = errors.New("no message block")
	errMsgBlkTooBig  = errors.New("message block size exceeded int capacity")
	errUnknownCipher = errors.New("unknown cipher")
	errNoMainKey     = errors.New("encrypted store encountered with no main key")
	errNoBlkData     = errors.New("message block data missing")
	errStateTooBig   = errors.New("store state too big for optional write")
)

type (
	errBadMsg struct{ fn, detail string }
)

func (e errBadMsg) Error() string {
	if e.detail != _EMPTY_ {
		return fmt.Sprintf("malformed or corrupt message in %s: %s", filepath.Base(e.fn), e.detail)
	}
	return fmt.Sprintf("malformed or corrupt message in %s", filepath.Base(e.fn))
}

const (
	// "Checksum bit" is used in "mb.cache.idx" for marking messages that have had their checksums checked.
	cbit = 1 << 31
	// "Delete bit" is used in "mb.cache.idx" to mark an index as deleted and non-existent.
	dbit = 1 << 30
	// "Header bit" is used in "rl" to signal a message record with headers.
	hbit = 1 << 31
	// "Erase bit" is used in "seq" for marking erased messages sequences.
	ebit = 1 << 63
	// "Tombstone bit" is used in "seq" for marking tombstone sequences.
	tbit = 1 << 62
)

// Will do a lookup from cache.
// This will copy the msg from the cache.
// Lock should be held.
func (mb *msgBlock) cacheLookup(seq uint64, sm *StoreMsg) (*StoreMsg, error) {
	return mb.cacheLookupEx(seq, sm, true)
}

// Will do a lookup from cache.
// This will NOT copy the msg from the cache.
// Lock should be held.
func (mb *msgBlock) cacheLookupNoCopy(seq uint64, sm *StoreMsg) (*StoreMsg, error) {
	return mb.cacheLookupEx(seq, sm, false)
}

// Will do a lookup from cache.
// Lock should be held.
func (mb *msgBlock) cacheLookupEx(seq uint64, sm *StoreMsg, doCopy bool) (*StoreMsg, error) {
	fseq, lseq := atomic.LoadUint64(&mb.first.seq), atomic.LoadUint64(&mb.last.seq)
	switch {
	case lseq == fseq-1:
		// The block is empty, no messages have been written yet. This works because
		// newMsgBlockForWrite sets fseq=fs.State.LastSeq+1 and lseq=fs.State.LastSeq.
		return nil, ErrStoreMsgNotFound
	case seq < fseq || seq > lseq:
		// Sequence is out of range for this block.
		return nil, ErrStoreMsgNotFound
	}

	// The llseq signals us when we can expire a cache at the end of a linear scan.
	// We want to only update when we know the last reads (multiple consumers) are sequential.
	// We want to account for forwards and backwards linear scans.
	if mb.llseq == 0 || seq < mb.llseq || seq == mb.llseq+1 || seq == mb.llseq-1 {
		mb.llseq = seq
	}

	// If we have a delete map check it.
	if mb.dmap.Exists(seq) {
		mb.llts = ats.AccessTime()
		return nil, errDeletedMsg
	}

	// Detect no cache loaded.
	if mb.cache == nil {
		mb.cache = mb.ecache.Value()
	}
	if mb.cache == nil || mb.cache.fseq == 0 || len(mb.cache.idx) == 0 || len(mb.cache.buf) == 0 {
		var reason string
		if mb.cache == nil {
			reason = "no cache"
		} else if mb.cache.fseq == 0 {
			reason = "fseq is 0"
		} else if len(mb.cache.idx) == 0 {
			reason = "no idx present"
		} else {
			reason = "cache buf empty"
		}
		mb.fs.warn("Cache lookup for sequence %d in block %d detected no cache: %s", seq, mb.index, reason)
		if mb.cache != nil {
			mb.tryForceExpireCacheLocked()
		}
		return nil, errNoCache
	}
	// Check partial cache status.
	if seq < mb.cache.fseq {
		mb.fs.warn("Partial cache: seq %d is less than cache fseq %d", seq, mb.cache.fseq)
		return nil, errPartialCache
	}

	bi, _, hashChecked, err := mb.slotInfo(int(seq - mb.cache.fseq))
	if err != nil {
		return nil, err
	}

	// Update cache activity.
	mb.llts = ats.AccessTime()

	li := int(bi)
	if li >= len(mb.cache.buf) {
		mb.fs.warn("Partial cache: slot index %d is less than cache buffer len %d", li, len(mb.cache.buf))
		return nil, errPartialCache
	}
	buf := mb.cache.buf[li:]

	// We use the high bit to denote we have already checked the checksum.
	var hh *highwayhash.Digest64
	if !hashChecked {
		hh = mb.hh // This will force the hash check in msgFromBuf.
	}

	// Parse from the raw buffer.
	fsm, err := mb.msgFromBufEx(buf, sm, hh, doCopy)
	if err != nil || fsm == nil {
		return nil, err
	}

	// Deleted messages that are decoded return a 0 for sequence.
	if fsm.seq == 0 {
		return nil, errDeletedMsg
	}

	if seq != fsm.seq { // See TestFileStoreInvalidIndexesRebuilt.
		mb.clearCacheAndOffset()
		return nil, fmt.Errorf("sequence numbers for cache load did not match, %d vs %d", seq, fsm.seq)
	}

	// Clear the check bit here after we know all is good.
	if !hashChecked {
		mb.cache.idx[seq-mb.cache.fseq] = (bi | cbit)
	}

	return fsm, nil
}

// Used when we are checking if discarding a message due to max msgs per subject will give us
// enough room for a max bytes condition.
// Lock should be already held.
func (fs *fileStore) sizeForSeq(seq uint64) int {
	if seq == 0 {
		return 0
	}
	var smv StoreMsg
	if mb := fs.selectMsgBlock(seq); mb != nil {
		mb.mu.Lock()
		sm, _, _ := mb.fetchMsgNoCopyLocked(seq, &smv)
		var sz int
		if sm != nil {
			sz = int(fileStoreMsgSize(sm.subj, sm.hdr, sm.msg))
		}
		mb.finishedWithCache()
		mb.mu.Unlock()
		return sz
	}
	return 0
}

// Will return message for the given sequence number.
// This will be returned to external callers.
func (fs *fileStore) msgForSeq(seq uint64, sm *StoreMsg) (*StoreMsg, error) {
	return fs.msgForSeqLocked(seq, sm, true)
}

// Will return message for the given sequence number.
func (fs *fileStore) msgForSeqLocked(seq uint64, sm *StoreMsg, needFSLock bool) (*StoreMsg, error) {
	if fs.isClosed() {
		return nil, ErrStoreClosed
	}
	// TODO(dlc) - Since Store, Remove, Skip all hold the write lock on fs this will
	// be stalled. Need another lock if want to happen in parallel.
	if needFSLock {
		fs.mu.RLock()
	}
	// Indicates we want first msg.
	if seq == 0 {
		seq = fs.state.FirstSeq
	}
	// Make sure to snapshot here.
	mb, lseq := fs.selectMsgBlock(seq), fs.state.LastSeq
	if needFSLock {
		fs.mu.RUnlock()
	}

	if mb == nil {
		var err = ErrStoreEOF
		if seq <= lseq {
			err = ErrStoreMsgNotFound
		}
		return nil, err
	}

	fsm, expireOk, err := mb.fetchMsg(seq, sm)
	if err != nil {
		return nil, err
	}

	// We detected a linear scan and access to the last message.
	// If we are not the last message block we can try to expire the cache.
	if expireOk {
		mb.tryForceExpireCache()
	}

	return fsm, nil
}

// Internal function to return msg parts from a raw buffer.
// Raw buffer will be copied into sm.
// Lock should be held.
func (mb *msgBlock) msgFromBuf(buf []byte, sm *StoreMsg, hh *highwayhash.Digest64) (*StoreMsg, error) {
	return mb.msgFromBufEx(buf, sm, hh, true)
}

// Internal function to return msg parts from a raw buffer.
// Raw buffer will NOT be copied into sm.
// Only use for internal use, any message that is passed to upper layers should use mb.msgFromBuf.
// Lock should be held.
func (mb *msgBlock) msgFromBufNoCopy(buf []byte, sm *StoreMsg, hh *highwayhash.Digest64) (*StoreMsg, error) {
	return mb.msgFromBufEx(buf, sm, hh, false)
}

// Internal function to return msg parts from a raw buffer.
// copy boolean will determine if we make a copy or not.
// Lock should be held.
func (mb *msgBlock) msgFromBufEx(buf []byte, sm *StoreMsg, hh *highwayhash.Digest64, doCopy bool) (*StoreMsg, error) {
	if len(buf) < emptyRecordLen {
		return nil, errBadMsg{mb.mfn, "record too short"}
	}
	var le = binary.LittleEndian

	hdr := buf[:msgHdrSize]
	rl := le.Uint32(hdr[0:])
	hasHeaders := rl&hbit != 0
	rl &^= hbit // clear header bit
	dlen := int(rl) - msgHdrSize
	slen := int(le.Uint16(hdr[20:]))

	shlen := slen
	if hasHeaders {
		shlen += 4
	}
	// Simple sanity check.
	if dlen < 0 || shlen > (dlen-recordHashSize) || dlen > int(rl) || int(rl) > len(buf) || rl > rlBadThresh {
		return nil, errBadMsg{mb.mfn, fmt.Sprintf("sanity check failed (dlen %d slen %d rl %d buf %d)", dlen, slen, rl, buf)}
	}
	data := buf[msgHdrSize : msgHdrSize+dlen]
	// Do checksum tests here if requested.
	if hh != nil {
		hh.Reset()
		hh.Write(hdr[4:20])
		hh.Write(data[:slen])
		if hasHeaders {
			hh.Write(data[slen+4 : dlen-recordHashSize])
		} else {
			hh.Write(data[slen : dlen-recordHashSize])
		}
		var hb [highwayhash.Size64]byte
		if !bytes.Equal(hh.Sum(hb[:0]), data[len(data)-8:]) {
			return nil, errBadMsg{mb.mfn, "invalid checksum"}
		}
	}
	seq := le.Uint64(hdr[4:])
	if seq&ebit != 0 {
		seq = 0
	}
	ts := int64(le.Uint64(hdr[12:]))

	// Create a StoreMsg if needed.
	if sm == nil {
		sm = new(StoreMsg)
	} else {
		sm.clear()
	}
	// To recycle the large blocks we can never pass back a reference, so need to copy for the upper
	// layers and for us to be safe to expire, and recycle, the large msgBlocks.
	end := dlen - 8
	if len(data) < end {
		return nil, errBadMsg{mb.mfn, "invalid data length"}
	}

	if hasHeaders {
		if slen+4 > len(data) {
			return nil, errBadMsg{mb.mfn, "invalid subject length greataer than data length"}
		}
		hl := le.Uint32(data[slen:])
		bi := slen + 4
		li := bi + int(hl)
		if bi > end {
			return nil, errBadMsg{mb.mfn, "invalid buffer index"}
		}
		if doCopy {
			sm.buf = append(sm.buf, data[bi:end]...)
		} else {
			sm.buf = data[bi:end]
		}
		li, end = li-bi, end-bi
		if li > len(sm.buf) || end > len(sm.buf) {
			return nil, errBadMsg{mb.mfn, "invalid message length or end greater than buffer length"}
		}
		sm.hdr = sm.buf[0:li:li]
		sm.msg = sm.buf[li:end]
	} else {
		if slen > end {
			return nil, errBadMsg{mb.mfn, "invalid subject length greater than end"}
		}
		if doCopy {
			sm.buf = append(sm.buf, data[slen:end]...)
		} else {
			sm.buf = data[slen:end]
		}
		mlen := end - slen
		if mlen > len(sm.buf) {
			return nil, errBadMsg{mb.mfn, "invalid message length greater than buffer length"}
		}
		sm.msg = sm.buf[0:mlen]
	}
	sm.seq, sm.ts = seq, ts
	if slen > 0 {
		if slen > len(data) {
			return nil, errBadMsg{mb.mfn, "invalid subject length greater than data length"}
		}
		if doCopy {
			// Make a copy since sm.subj lifetime may last longer.
			sm.subj = string(data[:slen])
		} else {
			sm.subj = bytesToString(data[:slen])
		}
	}

	return sm, nil
}

// SubjectForSeq will return what the subject is for this sequence if found.
func (fs *fileStore) SubjectForSeq(seq uint64) (string, error) {
	fs.mu.RLock()
	if seq < fs.state.FirstSeq {
		fs.mu.RUnlock()
		return _EMPTY_, ErrStoreMsgNotFound
	}
	var smv StoreMsg
	mb := fs.selectMsgBlock(seq)
	fs.mu.RUnlock()
	if mb != nil {
		mb.mu.Lock()
		sm, _, _ := mb.fetchMsgNoCopyLocked(seq, &smv)
		var subj string
		if sm != nil {
			// Copy the subject, as it's used elsewhere, and the backing cache could be reused in the meantime.
			subj = copyString(sm.subj)
		}
		mb.finishedWithCache()
		mb.mu.Unlock()
		if sm != nil {
			return subj, nil
		}
	}
	return _EMPTY_, ErrStoreMsgNotFound
}

// LoadMsg will lookup the message by sequence number and return it if found.
func (fs *fileStore) LoadMsg(seq uint64, sm *StoreMsg) (*StoreMsg, error) {
	return fs.msgForSeq(seq, sm)
}

// loadLast will load the last message for a subject. Subject should be non empty and not ">".
func (fs *fileStore) loadLast(subj string, sm *StoreMsg) (lsm *StoreMsg, err error) {
	if fs.isClosed() {
		return nil, ErrStoreClosed
	}

	fs.mu.RLock()
	defer fs.mu.RUnlock()
	return fs.loadLastLocked(subj, sm)
}

// Lock should be held.
func (fs *fileStore) loadLastLocked(subj string, sm *StoreMsg) (lsm *StoreMsg, err error) {
	if fs.isClosed() || fs.lmb == nil {
		return nil, ErrStoreClosed
	}

	if len(fs.blks) == 0 {
		return nil, ErrStoreMsgNotFound
	}

	wc := subjectHasWildcard(subj)
	var start, stop uint32

	// If literal subject check for presence.
	if wc {
		start = fs.lmb.index
		fs.psim.Match(stringToBytes(subj), func(_ []byte, psi *psi) {
			// Keep track of start and stop indexes for this subject.
			if psi.fblk < start {
				start = psi.fblk
			}
			if psi.lblk > stop {
				stop = psi.lblk
			}
		})
		// None matched.
		if stop == 0 {
			return nil, ErrStoreMsgNotFound
		}
		// These need to be swapped.
		start, stop = stop, start
	} else if info, ok := fs.psim.Find(stringToBytes(subj)); ok {
		start, stop = info.lblk, info.fblk
	} else {
		return nil, ErrStoreMsgNotFound
	}

	// Walk blocks backwards.
	for i := start; i >= stop; i-- {
		mb := fs.bim[i]
		if mb == nil {
			continue
		}
		mb.mu.Lock()
		if err = mb.ensurePerSubjectInfoLoaded(); err != nil {
			mb.mu.Unlock()
			return nil, err
		}
		// Mark fss activity.
		mb.lsts = ats.AccessTime()

		var l uint64
		// Optimize if subject is not a wildcard.
		if !wc {
			if ss, ok := mb.fss.Find(stringToBytes(subj)); ok && ss != nil {
				// Check if we need to recalculate. We only care about the last sequence.
				if ss.lastNeedsUpdate {
					// mb is already loaded into the cache so should be fast-ish.
					if err = mb.recalculateForSubj(subj, ss); err != nil {
						mb.mu.Unlock()
						return nil, err
					}
				}
				l = ss.Last
			}
		}
		if l == 0 {
			_, _, l, err = mb.filteredPendingLocked(subj, wc, atomic.LoadUint64(&mb.first.seq))
			if err != nil {
				mb.mu.Unlock()
				return nil, err
			}
		}
		needsCleanup := mb.cache == nil
		if l > 0 {
			if mb.cacheNotLoaded() {
				if err := mb.loadMsgsWithLock(); err != nil {
					if needsCleanup {
						mb.finishedWithCache()
					}
					mb.mu.Unlock()
					return nil, err
				}
			}
			lsm, err = mb.cacheLookup(l, sm)
		}
		if needsCleanup {
			mb.finishedWithCache()
		}
		mb.mu.Unlock()
		if l > 0 {
			break
		}
	}
	return lsm, err
}

// LoadLastMsg will return the last message we have that matches a given subject.
// The subject can be a wildcard.
func (fs *fileStore) LoadLastMsg(subject string, smv *StoreMsg) (sm *StoreMsg, err error) {
	if subject == _EMPTY_ || subject == fwcs {
		sm, err = fs.msgForSeq(fs.lastSeq(), smv)
	} else {
		sm, err = fs.loadLast(subject, smv)
	}
	if sm == nil || (err != nil && err != ErrStoreClosed) {
		err = ErrStoreMsgNotFound
	}
	return sm, err
}

// LoadNextMsgMulti will find the next message matching any entry in the sublist.
func (fs *fileStore) LoadNextMsgMulti(sl *gsl.SimpleSublist, start uint64, smp *StoreMsg) (sm *StoreMsg, skip uint64, err error) {
	if fs.isClosed() {
		return nil, 0, ErrStoreClosed
	}
	if sl == nil || sl.MatchesFullWildcard() {
		return fs.LoadNextMsg(_EMPTY_, false, start, smp)
	}
	if filter, ok := sl.MatchesSingleFilter(); ok {
		return fs.LoadNextMsg(filter, subjectHasWildcard(filter), start, smp)
	}
	fs.mu.RLock()
	defer fs.mu.RUnlock()

	if fs.state.Msgs == 0 || start > fs.state.LastSeq {
		return nil, fs.state.LastSeq, ErrStoreEOF
	}
	if start < fs.state.FirstSeq {
		start = fs.state.FirstSeq
	}

	// If start is less than or equal to beginning of our stream, meaning our first call,
	// let's check the psim to see if we can skip ahead.
	if start <= fs.state.FirstSeq {
		var total uint64
		blkStart := uint32(math.MaxUint32)
		stree.IntersectGSL(fs.psim, sl, func(subj []byte, psi *psi) bool {
			total += psi.total
			// Keep track of start index for this subject.
			if psi.fblk < blkStart {
				blkStart = psi.fblk
			}
			return true
		})
		// Nothing available.
		if total == 0 {
			return nil, fs.state.LastSeq, ErrStoreEOF
		}
		// We can skip ahead.
		if mb := fs.bim[blkStart]; mb != nil {
			fseq := atomic.LoadUint64(&mb.first.seq)
			if fseq > start {
				start = fseq
			}
		}
	}

	if bi, _ := fs.selectMsgBlockWithIndex(start); bi >= 0 {
		for i := bi; i < len(fs.blks); i++ {
			mb := fs.blks[i]
			if sm, expireOk, err := mb.firstMatchingMulti(sl, start, smp); err == nil {
				if expireOk {
					mb.tryForceExpireCache()
				}
				return sm, sm.seq, nil
			} else if err != ErrStoreMsgNotFound {
				return nil, 0, err
			} else {
				// Nothing found in this block. We missed, if first block (bi) check psim.
				// Similar to above if start <= first seq.
				// TODO(dlc) - For v2 track these by filter subject since they will represent filtered consumers.
				// We should not do this at all if we are already on the last block.
				if i == bi && i < len(fs.blks)-1 {
					nbi, err := fs.checkSkipFirstBlockMulti(sl, bi)
					// Nothing available.
					if err == ErrStoreEOF {
						return nil, fs.state.LastSeq, ErrStoreEOF
					}
					// See if we can jump ahead here.
					// Right now we can only spin on first, so if we have interior sparseness need to favor checking per block fss if loaded.
					// For v2 will track all blocks that have matches for psim.
					if nbi > i {
						i = nbi - 1 // For the iterator condition i++
					}
				}
				// Check if we can expire.
				if expireOk {
					mb.tryForceExpireCache()
				}
			}
		}
	}

	return nil, fs.state.LastSeq, ErrStoreEOF

}

func (fs *fileStore) LoadNextMsg(filter string, wc bool, start uint64, sm *StoreMsg) (*StoreMsg, uint64, error) {
	if fs.isClosed() {
		return nil, 0, ErrStoreClosed
	}

	fs.mu.RLock()
	defer fs.mu.RUnlock()

	if fs.state.Msgs == 0 || start > fs.state.LastSeq {
		return nil, fs.state.LastSeq, ErrStoreEOF
	}
	if start < fs.state.FirstSeq {
		start = fs.state.FirstSeq
	}

	// If start is less than or equal to beginning of our stream, meaning our first call,
	// let's check the psim to see if we can skip ahead.
	if start <= fs.state.FirstSeq {
		var ss SimpleState
		if err := fs.numFilteredPendingNoLast(filter, &ss); err != nil {
			return nil, 0, err
		}
		// Nothing available.
		if ss.Msgs == 0 {
			return nil, fs.state.LastSeq, ErrStoreEOF
		}
		// We can skip ahead.
		if ss.First > start {
			start = ss.First
		}
	}

	if bi, _ := fs.selectMsgBlockWithIndex(start); bi >= 0 {
		for i := bi; i < len(fs.blks); i++ {
			mb := fs.blks[i]
			if sm, expireOk, err := mb.firstMatching(filter, wc, start, sm); err == nil {
				if expireOk {
					mb.tryForceExpireCache()
				}
				return sm, sm.seq, nil
			} else if err != ErrStoreMsgNotFound {
				return nil, 0, err
			} else {
				// Nothing found in this block. We missed, if first block (bi) check psim.
				// Similar to above if start <= first seq.
				// TODO(dlc) - For v2 track these by filter subject since they will represent filtered consumers.
				// We should not do this at all if we are already on the last block.
				// Also if we are a wildcard do not check if large subject space.
				const wcMaxSizeToCheck = 64 * 1024
				if i == bi && i < len(fs.blks)-1 && (!wc || fs.psim.Size() < wcMaxSizeToCheck) {
					nbi, err := fs.checkSkipFirstBlock(filter, wc, bi)
					// Nothing available.
					if err == ErrStoreEOF {
						return nil, fs.state.LastSeq, ErrStoreEOF
					}
					// See if we can jump ahead here.
					// Right now we can only spin on first, so if we have interior sparseness need to favor checking per block fss if loaded.
					// For v2 will track all blocks that have matches for psim.
					if nbi > i {
						i = nbi - 1 // For the iterator condition i++
					}
				}
				// Check if we can expire.
				if expireOk {
					mb.tryForceExpireCache()
				}
			}
		}
	}

	return nil, fs.state.LastSeq, ErrStoreEOF
}

// Find the previous matching message.
// fs lock should be held.
func (mb *msgBlock) prevMatching(filter string, wc bool, start uint64, sm *StoreMsg) (*StoreMsg, bool, error) {
	mb.mu.Lock()
	var updateLLTS bool
	defer func() {
		if updateLLTS {
			mb.llts = ats.AccessTime()
		}
		mb.finishedWithCache()
		mb.mu.Unlock()
	}()

	end, isAll := start, filter == _EMPTY_ || filter == fwcs

	var didLoad bool
	if mb.fssNotLoaded() {
		if err := mb.loadMsgsWithLock(); err != nil {
			return nil, false, err
		}
		didLoad = true
	}
	mb.lsts = ats.AccessTime()

	if filter == _EMPTY_ {
		filter = fwcs
		wc = true
	}

	if !isAll && mb.fss.Size() == 1 {
		if !wc {
			_, isAll = mb.fss.Find(stringToBytes(filter))
		} else {
			mb.fss.Match(stringToBytes(filter), func(subject []byte, _ *SimpleState) {
				isAll = true
			})
		}
		if !isAll {
			return nil, didLoad, ErrStoreMsgNotFound
		}
	}

	lseq := atomic.LoadUint64(&mb.first.seq)
	end = min(end, atomic.LoadUint64(&mb.last.seq))

	var isMatch func(subj string) bool
	if wc {
		_tsa, _fsa := [32]string{}, [32]string{}
		tsa, fsa := _tsa[:0], tokenizeSubjectIntoSlice(_fsa[:0], filter)
		isMatch = func(subj string) bool {
			tsa = tokenizeSubjectIntoSlice(tsa[:0], subj)
			return isSubsetMatchTokenized(tsa, fsa)
		}
	}

	subjs := mb.fs.cfg.Subjects
	doLinearScan := isAll || (wc && len(subjs) == 1 && subjs[0] == filter)
	if !doLinearScan && wc && mb.cacheAlreadyLoaded() {
		doLinearScan = mb.fss.Size()*4 > int(end-lseq)
	}

	if !doLinearScan {
		var found bool
		var first, last uint64
		if bfilter := stringToBytes(filter); wc {
			var ierr error
			mb.fss.Match(bfilter, func(bsubj []byte, ss *SimpleState) {
				if ierr != nil {
					return
				}
				if ss.firstNeedsUpdate || ss.lastNeedsUpdate {
					if ierr = mb.recalculateForSubj(bytesToString(bsubj), ss); ierr != nil {
						return
					}
				}
				if end < ss.First {
					return
				}
				if !found {
					found = true
					first, last = ss.First, min(end, ss.Last)
					return
				}
				first = min(first, ss.First)
				last = max(last, min(end, ss.Last))
			})
			if ierr != nil {
				return nil, false, ierr
			}
		} else if ss, _ := mb.fss.Find(bfilter); ss != nil {
			if ss.firstNeedsUpdate || ss.lastNeedsUpdate {
				if err := mb.recalculateForSubj(filter, ss); err != nil {
					return nil, false, err
				}
			}
			if end >= ss.First {
				found = true
				first, last = ss.First, min(end, ss.Last)
			}
		}
		if !found || first > last {
			return nil, didLoad, ErrStoreMsgNotFound
		}
		lseq, end = max(lseq, first), last
	}

	if mb.cacheNotLoaded() {
		if err := mb.loadMsgsWithLock(); err != nil {
			return nil, false, err
		}
		didLoad = true
	}

	if sm == nil {
		sm = new(StoreMsg)
	}

	for seq := end; seq >= lseq; seq-- {
		if mb.dmap.Exists(seq) {
			updateLLTS = true
			continue
		}
		llseq := mb.llseq
		fsm, err := mb.cacheLookup(seq, sm)
		if err != nil {
			if err == errPartialCache || err == errNoCache {
				return nil, false, err
			}
			continue
		}
		updateLLTS = false
		expireOk := seq == lseq && mb.llseq != llseq && mb.llseq == seq
		if isAll {
			return fsm, expireOk, nil
		}
		if wc && isMatch(sm.subj) {
			return fsm, expireOk, nil
		} else if !wc && fsm.subj == filter {
			return fsm, expireOk, nil
		}
		mb.llseq = llseq
	}

	return nil, didLoad, ErrStoreMsgNotFound
}

// Will load the previous message matching the filter subject, starting at the start sequence and walking backwards.
func (fs *fileStore) LoadPrevMsg(filter string, wc bool, start uint64, smp *StoreMsg) (sm *StoreMsg, skip uint64, err error) {
	if fs.isClosed() {
		return nil, 0, ErrStoreClosed
	}

	fs.mu.RLock()
	defer fs.mu.RUnlock()

	if fs.state.Msgs == 0 || start < fs.state.FirstSeq {
		return nil, fs.state.FirstSeq, ErrStoreEOF
	}

	if start > fs.state.LastSeq {
		start = fs.state.LastSeq
	}

	if bi, _ := fs.selectMsgBlockWithIndex(start); bi >= 0 {
		for i := bi; i >= 0; i-- {
			mb := fs.blks[i]
			if sm, expireOk, err := mb.prevMatching(filter, wc, start, smp); err == nil {
				if expireOk {
					mb.tryForceExpireCache()
				}
				return sm, sm.seq, nil
			} else if err != ErrStoreMsgNotFound {
				return nil, 0, err
			} else if expireOk {
				mb.tryForceExpireCache()
			}
		}
	}

	return nil, fs.state.FirstSeq, ErrStoreEOF
}

// LoadPrevMsgMulti will find the previous message matching any entry in the sublist.
func (fs *fileStore) LoadPrevMsgMulti(sl *gsl.SimpleSublist, start uint64, smp *StoreMsg) (sm *StoreMsg, skip uint64, err error) {
	if fs.isClosed() {
		return nil, 0, ErrStoreClosed
	}

	if sl == nil || sl.MatchesFullWildcard() {
		return fs.LoadPrevMsg(_EMPTY_, false, start, smp)
	}
	if filter, ok := sl.MatchesSingleFilter(); ok {
		return fs.LoadPrevMsg(filter, subjectHasWildcard(filter), start, smp)
	}
	fs.mu.RLock()
	defer fs.mu.RUnlock()
	return fs.loadPrevMsgMultiLocked(sl, start, smp)
}

// loadPrevMsgMultiLocked will find the previous message matching any entry in the sublist.
// Lock should be held.
func (fs *fileStore) loadPrevMsgMultiLocked(sl *gsl.SimpleSublist, start uint64, smp *StoreMsg) (sm *StoreMsg, skip uint64, err error) {
	if fs.state.Msgs == 0 || start < fs.state.FirstSeq {
		return nil, fs.state.FirstSeq, ErrStoreEOF
	}
	if start > fs.state.LastSeq {
		start = fs.state.LastSeq
	}

	if bi, _ := fs.selectMsgBlockWithIndex(start); bi >= 0 {
		for i := bi; i >= 0; i-- {
			mb := fs.blks[i]
			if sm, expireOk, err := mb.prevMatchingMulti(sl, start, smp); err == nil {
				if expireOk {
					mb.tryForceExpireCache()
				}
				return sm, sm.seq, nil
			} else if err != ErrStoreMsgNotFound {
				return nil, 0, err
			} else if expireOk {
				mb.tryForceExpireCache()
			}
		}
	}

	return nil, fs.state.FirstSeq, ErrStoreEOF
}

// Type returns the type of the underlying store.
func (fs *fileStore) Type() StorageType {
	return FileStorage
}

// Returns number of subjects in this store.
// Lock should be held.
func (fs *fileStore) numSubjects() int {
	return fs.psim.Size()
}

// numConsumers uses new lock.
func (fs *fileStore) numConsumers() int {
	fs.cmu.RLock()
	defer fs.cmu.RUnlock()
	return len(fs.cfs)
}

// FastState will fill in state with only the following.
// Msgs, Bytes, First and Last Sequence and Time and NumDeleted.
func (fs *fileStore) FastState(state *StreamState) {
	fs.mu.RLock()
	state.Msgs = fs.state.Msgs
	state.Bytes = fs.state.Bytes
	state.FirstSeq = fs.state.FirstSeq
	state.FirstTime = fs.state.FirstTime
	state.LastSeq = fs.state.LastSeq
	state.LastTime = fs.state.LastTime
	// Make sure to reset if being re-used.
	state.Deleted, state.NumDeleted = nil, 0
	if state.LastSeq > state.FirstSeq {
		state.NumDeleted = int((state.LastSeq - state.FirstSeq + 1) - state.Msgs)
		if state.NumDeleted < 0 {
			state.NumDeleted = 0
		}
	}
	state.Consumers = fs.numConsumers()
	state.NumSubjects = fs.numSubjects()
	fs.mu.RUnlock()
}

// State returns the current state of the stream.
func (fs *fileStore) State() StreamState {
	fs.mu.RLock()
	state := fs.state
	state.Consumers = fs.numConsumers()
	state.NumSubjects = fs.numSubjects()
	state.Deleted = nil // make sure.

	if numDeleted := int((state.LastSeq - state.FirstSeq + 1) - state.Msgs); numDeleted > 0 {
		state.Deleted = make([]uint64, 0, numDeleted)
		cur := fs.state.FirstSeq

		for _, mb := range fs.blks {
			mb.mu.Lock()
			fseq := atomic.LoadUint64(&mb.first.seq)
			// Account for messages missing from the head.
			if fseq > cur {
				for seq := cur; seq < fseq; seq++ {
					state.Deleted = append(state.Deleted, seq)
				}
			}
			// Only advance cur if we are increasing. We could have marker blocks with just tombstones.
			if last := atomic.LoadUint64(&mb.last.seq); last >= cur {
				cur = last + 1 // Expected next first.
			}
			// Add in deleted.
			mb.dmap.Range(func(seq uint64) bool {
				state.Deleted = append(state.Deleted, seq)
				return true
			})
			mb.mu.Unlock()
		}
	}
	fs.mu.RUnlock()

	state.Lost = fs.lostData()

	// Can not be guaranteed to be sorted.
	if len(state.Deleted) > 0 {
		slices.Sort(state.Deleted)
		state.NumDeleted = len(state.Deleted)
	}
	return state
}

func (fs *fileStore) Utilization() (total, reported uint64, err error) {
	fs.mu.RLock()
	defer fs.mu.RUnlock()
	for _, mb := range fs.blks {
		mb.mu.RLock()
		reported += mb.bytes
		total += mb.rbytes
		mb.mu.RUnlock()
	}
	return total, reported, nil
}

func fileStoreMsgSizeRaw(slen, hlen, mlen int) uint64 {
	if hlen == 0 {
		// length of the message record (4bytes) + seq(8) + ts(8) + subj_len(2) + subj + msg + hash(8)
		return uint64(22 + slen + mlen + 8)
	}
	// length of the message record (4bytes) + seq(8) + ts(8) + subj_len(2) + subj + hdr_len(4) + hdr + msg + hash(8)
	return uint64(22 + slen + 4 + hlen + mlen + 8)
}

func fileStoreMsgSize(subj string, hdr, msg []byte) uint64 {
	return fileStoreMsgSizeRaw(len(subj), len(hdr), len(msg))
}

// isFileStoreMsgTooLarge reports whether a message record cannot be represented
// safely by the file store.
func isFileStoreMsgTooLarge(rl uint64) bool {
	return rl&hbit != 0 || rl > rlBadThresh
}

func fileStoreMsgSizeEstimate(slen, maxPayload int) uint64 {
	return uint64(emptyRecordLen + slen + 4 + maxPayload)
}

// ResetState resets any state that's temporary. For example when changing leaders.
func (fs *fileStore) ResetState() {
	fs.mu.Lock()
	defer fs.mu.Unlock()
	if fs.scheduling != nil {
		fs.scheduling.clearInflight()
	}
}

func (fs *fileStore) Ready() {
	fs.mu.Lock()
	defer fs.mu.Unlock()
	if !fs.recovering {
		return
	}
	fs.recovering = false
	var ageDelta int64
	if fs.cfg.MaxAge > 0 && !fs.state.FirstTime.IsZero() {
		ageDelta = time.Until(fs.state.FirstTime.Add(fs.cfg.MaxAge)).Nanoseconds()
		if ageDelta <= 0 {
			ageDelta = 1
		}
	}
	fs.resetAgeChk(ageDelta)
	if fs.scheduling != nil {
		fs.scheduling.paused = false
		fs.scheduling.resetTimer()
	}
}

// Determine time since any last activity, read/load, write or remove.
func (mb *msgBlock) sinceLastActivity() time.Duration {
	if mb.closed {
		return 0
	}
	last := mb.lwts
	if mb.lrts > last {
		last = mb.lrts
	}
	if mb.llts > last {
		last = mb.llts
	}
	if mb.lsts > last {
		last = mb.lsts
	}
	return time.Since(time.Unix(0, last).UTC())
}

// Determine time since last write or remove of a message.
// Read lock should be held.
func (mb *msgBlock) sinceLastWriteActivity() time.Duration {
	if mb.closed {
		return 0
	}
	last := mb.lwts
	if mb.lrts > last {
		last = mb.lrts
	}
	return time.Since(time.Unix(0, last).UTC())
}

func checkNewHeader(hdr []byte) error {
	if len(hdr) < 2 || hdr[0] != magic ||
		(hdr[1] != version && hdr[1] != newVersion) {
		return errCorruptState
	}
	return nil
}

// readIndexInfo will read in the index information for the message block.
func (mb *msgBlock) readIndexInfo() error {
	ifn := filepath.Join(mb.fs.fcfg.StoreDir, msgDir, fmt.Sprintf(indexScan, mb.index))
	buf, err := os.ReadFile(ifn)
	if err != nil {
		return err
	}

	// Set if first time.
	if mb.liwsz == 0 {
		mb.liwsz = int64(len(buf))
	}

	// Decrypt if needed.
	if mb.aek != nil {
		buf, err = mb.aek.Open(buf[:0], mb.nonce, buf, nil)
		if err != nil {
			return err
		}
	}

	if err := checkNewHeader(buf); err != nil {
		defer os.Remove(ifn)
		return fmt.Errorf("bad index file")
	}

	bi := hdrLen

	// Helpers, will set i to -1 on error.
	readSeq := func() uint64 {
		if bi < 0 {
			return 0
		}
		seq, n := binary.Uvarint(buf[bi:])
		if n <= 0 {
			bi = -1
			return 0
		}
		bi += n
		return seq &^ ebit
	}
	readCount := readSeq
	readTimeStamp := func() int64 {
		if bi < 0 {
			return 0
		}
		ts, n := binary.Varint(buf[bi:])
		if n <= 0 {
			bi = -1
			return -1
		}
		bi += n
		return ts
	}
	mb.msgs = readCount()
	mb.bytes = readCount()
	atomic.StoreUint64(&mb.first.seq, readSeq())
	mb.first.ts = readTimeStamp()
	atomic.StoreUint64(&mb.last.seq, readSeq())
	mb.last.ts = readTimeStamp()
	dmapLen := readCount()

	// Check if this is a short write index file.
	if bi < 0 || bi+checksumSize > len(buf) {
		os.Remove(ifn)
		return fmt.Errorf("short index file")
	}

	// Check for consistency if accounting. If something is off bail and we will rebuild.
	if mb.msgs != (atomic.LoadUint64(&mb.last.seq)-atomic.LoadUint64(&mb.first.seq)+1)-dmapLen {
		os.Remove(ifn)
		return fmt.Errorf("accounting inconsistent")
	}

	// Checksum
	copy(mb.lchk[0:], buf[bi:bi+checksumSize])
	bi += checksumSize

	// Now check for presence of a delete map
	if dmapLen > 0 {
		// New version is encoded avl seqset.
		if buf[1] == newVersion {
			dmap, _, err := avl.Decode(buf[bi:])
			if err != nil {
				return fmt.Errorf("could not decode avl dmap: %v", err)
			}
			mb.dmap = *dmap
		} else {
			// This is the old version.
			for i, fseq := 0, atomic.LoadUint64(&mb.first.seq); i < int(dmapLen); i++ {
				seq := readSeq()
				if seq == 0 {
					break
				}
				mb.dmap.Insert(seq + fseq)
			}
		}
	}

	return nil
}

// Will return total number of cache loads.
func (fs *fileStore) cacheLoads() uint64 {
	var tl uint64
	fs.mu.RLock()
	for _, mb := range fs.blks {
		mb.mu.RLock()
		tl += mb.cloads
		mb.mu.RUnlock()
	}
	fs.mu.RUnlock()
	return tl
}

// Will return total number of cached bytes.
func (fs *fileStore) cacheSize() uint64 {
	var sz uint64
	fs.mu.RLock()
	for _, mb := range fs.blks {
		mb.mu.Lock()
		var needsCleanup bool
		if mb.cache == nil {
			mb.cache = mb.ecache.Value()
			needsCleanup = mb.cache != nil
		}
		if mb.cache != nil {
			sz += uint64(len(mb.cache.buf))
		}
		if needsCleanup {
			mb.finishedWithCache()
		}
		mb.mu.Unlock()
	}
	fs.mu.RUnlock()
	return sz
}

// Will return total number of dmapEntries for all msg blocks.
func (fs *fileStore) dmapEntries() int {
	var total int
	fs.mu.RLock()
	for _, mb := range fs.blks {
		total += mb.dmap.Size()
	}
	fs.mu.RUnlock()
	return total
}

// Fixed helper for iterating.
func subjectsEqual(a, b string) bool {
	return a == b
}

func subjectsAll(a, b string) bool {
	return true
}

func compareFn(subject string) func(string, string) bool {
	if subject == _EMPTY_ || subject == fwcs {
		return subjectsAll
	}
	if subjectHasWildcard(subject) {
		return subjectIsSubsetMatch
	}
	return subjectsEqual
}

// PurgeEx will remove messages based on subject filters, sequence and number of messages to keep.
// Will return the number of purged messages.
func (fs *fileStore) PurgeEx(subject string, sequence, keep uint64) (purged uint64, err error) {
	// sequence == 1 means "purge up to but not including 1", a no-op.
	if sequence == 1 {
		return 0, nil
	}
	if subject == _EMPTY_ || subject == fwcs {
		if keep == 0 && sequence == 0 {
			return fs.purge(0)
		}
		if sequence > 1 {
			return fs.compact(sequence)
		}
		// Make sure to not leave subject if empty.
		if subject == _EMPTY_ {
			subject = fwcs
		}
	}

	// Persist any write errors.
	defer func() {
		if err != nil {
			fs.mu.Lock()
			fs.setWriteErr(err)
			fs.mu.Unlock()
		}
	}()

	eq, wc := compareFn(subject), subjectHasWildcard(subject)
	var firstSeqNeedsUpdate bool
	var bytes uint64

	// If we have a "keep" designation need to get full filtered state so we know how many to purge.
	var maxp uint64
	if keep > 0 {
		ss, err := fs.FilteredState(1, subject)
		if err != nil || keep >= ss.Msgs {
			return 0, err
		}
		maxp = ss.Msgs - keep
	}

	var smv StoreMsg
	var tombs []msgId
	var lowSeq uint64

	fs.mu.Lock()
	if fs.isClosed() {
		fs.mu.Unlock()
		return purged, ErrStoreClosed
	}
	// Always return previous write errors.
	if err := fs.werr; err != nil {
		fs.mu.Unlock()
		return purged, err
	}
	if len(fs.blks) == 0 || fs.lmb == nil {
		fs.mu.Unlock()
		return purged, nil
	}

	var start, stop uint32

	// If literal subject check for presence.
	if wc {
		start = fs.lmb.index
		fs.psim.Match(stringToBytes(subject), func(_ []byte, psi *psi) {
			// Keep track of start and stop indexes for this subject.
			if psi.fblk < start {
				start = psi.fblk
			}
			if psi.lblk > stop {
				stop = psi.lblk
			}
		})
		// None matched.
		if stop == 0 {
			fs.mu.Unlock()
			return purged, nil
		}
	} else if info, ok := fs.psim.Find(stringToBytes(subject)); ok {
		start, stop = info.fblk, info.lblk
	} else {
		fs.mu.Unlock()
		return purged, nil
	}

	// We may remove blocks as we purge, so don't range directly on fs.blks
	// otherwise we may jump over some (see https://github.com/nats-io/nats-server/issues/3528)
	for i := 0; i < len(fs.blks); i++ {
		mb := fs.blks[i]
		// Skip if not within our range for the purge subject.
		if mb.index < start || mb.index > stop {
			continue
		}
		mb.mu.Lock()

		// If we do not have our fss, try to expire the cache if we have no items in this block.
		shouldExpire := mb.fssNotLoaded()

		t, f, l, err := mb.filteredPendingLocked(subject, wc, atomic.LoadUint64(&mb.first.seq))
		if err != nil {
			mb.mu.Unlock()
			fs.mu.Unlock()
			return purged, err
		}
		if t == 0 {
			// Expire if we were responsible for loading.
			if shouldExpire {
				// Expire this cache before moving on.
				mb.tryForceExpireCacheLocked()
			}
			mb.mu.Unlock()
			continue
		}

		// "Purge up to but not including sequence": sequence == 0 means no
		// sequence filter; sequence >= 1 clamps the per-block upper bound to
		// sequence-1 (so sequence == 1 leaves nothing to process).
		if sequence >= 1 && sequence <= l {
			l = sequence - 1
		}

		if mb.cacheNotLoaded() {
			if err := mb.loadMsgsWithLock(); err != nil {
				mb.mu.Unlock()
				fs.mu.Unlock()
				return 0, err
			}
			shouldExpire = true
		}

		var nrg uint64 // Number of remaining messages globally after removal from psim.

		for seq, te := f, len(tombs); seq <= l; seq++ {
			if sm, _ := mb.cacheLookupNoCopy(seq, &smv); sm != nil && eq(sm.subj, subject) {
				rl := fileStoreMsgSize(sm.subj, sm.hdr, sm.msg)
				// Do fast in place remove.
				// Stats
				if mb.msgs > 0 {
					// Msgs
					fs.state.Msgs--
					mb.msgs--
					// Bytes, make sure to not go negative.
					if rl > fs.state.Bytes {
						rl = fs.state.Bytes
					}
					if rl > mb.bytes {
						rl = mb.bytes
					}
					fs.state.Bytes -= rl
					mb.bytes -= rl
					// Totals
					purged++
					bytes += rl
				}
				// PSIM and FSS updates.
				nr, err := mb.removeSeqPerSubject(sm.subj, seq)
				if err != nil {
					mb.mu.Unlock()
					fs.mu.Unlock()
					return purged, err
				}
				nrg = fs.removePerSubject(sm.subj)

				// Track tombstones we need to write.
				tombs = append(tombs, msgId{sm.seq, sm.ts})
				if sm.seq < lowSeq || lowSeq == 0 {
					lowSeq = sm.seq
				}

				// Check for first message.
				if seq == atomic.LoadUint64(&mb.first.seq) {
					mb.selectNextFirst()
					if mb.isEmpty() {
						// Since we are removing this block don't need to write tombstones.
						tombs = tombs[:te]
						if err = fs.removeMsgBlock(mb); err != nil {
							mb.mu.Unlock()
							fs.mu.Unlock()
							return 0, err
						}
						i--
						// keep flag set, if set previously
						firstSeqNeedsUpdate = firstSeqNeedsUpdate || seq == fs.state.FirstSeq
					} else if seq == fs.state.FirstSeq {
						fs.state.FirstSeq = atomic.LoadUint64(&mb.first.seq) // new one.
						if mb.first.ts == 0 {
							fs.state.FirstTime = time.Time{}
						} else {
							fs.state.FirstTime = time.Unix(0, mb.first.ts).UTC()
						}
					}
				} else {
					// Out of order delete.
					mb.dmap.Insert(seq)
				}
				// Break if we have emptied this block or if we set a maximum purge count.
				if mb.isEmpty() || (maxp > 0 && purged >= maxp) {
					break
				}
				// Also break if we know we have no more messages matching here.
				// This is only applicable for non-wildcarded filters.
				if !wc && nr == 0 {
					break
				}
			}
		}
		// Expire if we were responsible for loading and we do not seem to be doing successive purgeEx calls.
		// On successive calls - most likely from KV purge deletes, we want to keep the data loaded.
		if shouldExpire && time.Since(fs.lpex) > time.Second {
			// Expire this cache before moving on.
			mb.tryForceExpireCacheLocked()
		} else {
			mb.finishedWithCache()
		}
		mb.mu.Unlock()

		// Check if we should break out of top level too.
		if maxp > 0 && purged >= maxp {
			break
		}
		// Also check if not wildcarded and we have no remaining matches.
		if !wc && nrg == 0 {
			break
		}
	}
	if firstSeqNeedsUpdate {
		if err = fs.selectNextFirst(); err != nil {
			fs.mu.Unlock()
			return purged, err
		}
	}

	// Update the last purgeEx call time.
	defer func() { fs.lpex = time.Now() }()

	// Write any tombstones as needed.
	// When writing multiple tombstones we will flush at the end.
	if len(tombs) > 0 {
		for _, tomb := range tombs {
			if err = fs.writeTombstoneNoFlush(tomb.seq, tomb.ts); err != nil {
				fs.mu.Unlock()
				return purged, err
			}
		}
		// Flush any pending. If we change blocks the newMsgBlockForWrite() will flush any pending for us.
		if lmb := fs.lmb; lmb != nil {
			if err = lmb.flushPendingMsgs(); err != nil {
				fs.mu.Unlock()
				return purged, err
			}
		}
	}

	fs.dirty++
	cb := fs.scb
	fs.mu.Unlock()

	if cb != nil {
		if purged == 1 {
			cb(-int64(purged), -int64(bytes), lowSeq, subject)
		} else {
			// FIXME(dlc) - Since we track lowSeq we could send to upper layer if they dealt with the condition properly.
			cb(-int64(purged), -int64(bytes), 0, _EMPTY_)
		}
	}

	return purged, nil
}

// Purge will remove all messages from this store.
// Will return the number of purged messages.
func (fs *fileStore) Purge() (uint64, error) {
	return fs.purge(0)
}

func (fs *fileStore) purge(fseq uint64) (uint64, error) {
	fs.mu.Lock()
	if fs.isClosed() {
		fs.mu.Unlock()
		return 0, ErrStoreClosed
	}
	cb := fs.scb
	purged, bytes, err := fs.purgeLocked(fseq)
	if err != nil {
		fs.setWriteErr(err)
		fs.mu.Unlock()
		return purged, err
	}
	fs.mu.Unlock()

	// Force a new index.db to be written.
	if purged > 0 {
		fs.forceWriteFullState()
	}
	if cb != nil {
		cb(-int64(purged), -int64(bytes), 0, _EMPTY_)
	}
	return purged, nil
}

// Lock must be held.
func (fs *fileStore) purgeLocked(fseq uint64) (purged, bytes uint64, err error) {
	// Always return previous write errors.
	if err := fs.werr; err != nil {
		return 0, 0, err
	}

	purged, bytes = fs.state.Msgs, fs.state.Bytes

	fs.state.FirstSeq = fs.state.LastSeq + 1
	fs.state.FirstTime = time.Time{}

	fs.state.Bytes = 0
	fs.state.Msgs = 0

	for _, mb := range fs.blks {
		// These blocks are being discarded by the purge, so mark them closed.
		mb.mu.Lock()
		mb.dirtyCloseWithRemove(false)
		mb.closed = true
		mb.mu.Unlock()
	}

	// Check if we need to set the first seq to a new number.
	if fseq > fs.state.FirstSeq {
		fs.state.FirstSeq = fseq
		fs.state.LastSeq = fseq - 1
	}

	// Make sure we have a lmb to write to.
	if _, err := fs.newMsgBlockForWrite(); err != nil {
		return purged, bytes, err
	}

	lmb := fs.lmb
	atomic.StoreUint64(&lmb.first.seq, fs.state.FirstSeq)
	atomic.StoreUint64(&lmb.last.seq, fs.state.LastSeq)
	lmb.last.ts = fs.state.LastTime.UnixNano()

	if lseq := atomic.LoadUint64(&lmb.last.seq); lseq > 0 {
		// Leave a tombstone so we can remember our starting sequence in case
		// full state becomes corrupted.
		if err := fs.writeTombstone(lseq, lmb.last.ts); err != nil {
			return purged, bytes, err
		}
	}
	// Close FDs since we'll move the file. We re-enable the FD after the purge is complete.
	if err := lmb.flushPendingMsgs(); err != nil {
		return purged, bytes, err
	}
	if err := lmb.closeFDs(); err != nil {
		return purged, bytes, err
	}

	fs.blks = nil
	fs.lmb = nil
	fs.bim = make(map[uint32]*msgBlock)
	// Clear any per subject tracking.
	fs.psim, fs.tsl = fs.psim.Empty(), 0
	fs.sdm.empty()
	// Mark dirty.
	fs.dirty++
	fs.addMsgBlock(lmb)

	// Move the msgs directory out of the way, will delete out of band.
	mdir := filepath.Join(fs.fcfg.StoreDir, msgDir)
	ndir := filepath.Join(fs.fcfg.StoreDir, newMsgDir)
	pdir := filepath.Join(fs.fcfg.StoreDir, purgeDir)
	fs.dios.acquire()
	// If purge directory still exists then we need to wait
	// in place and remove since rename would fail.
	if _, err := os.Stat(ndir); err == nil {
		if err = os.RemoveAll(ndir); err != nil {
			fs.dios.release()
			return purged, bytes, err
		}
	} else if !os.IsNotExist(err) {
		fs.dios.release()
		return purged, bytes, err
	}
	if _, err := os.Stat(pdir); err == nil {
		if err = os.RemoveAll(pdir); err != nil {
			fs.dios.release()
			return purged, bytes, err
		}
	} else if !os.IsNotExist(err) {
		fs.dios.release()
		return purged, bytes, err
	}

	// Create directory to move the new tombstone to.
	if err := os.MkdirAll(ndir, defaultDirPerms); err != nil {
		fs.dios.release()
		return purged, bytes, err
	}
	// Move out the block containing the tombstone. Also move the key file if encrypted.
	// The block file itself MUST be moved last to ensure we can assume the prior renames
	// were successful during recovery.
	for _, mbf := range []string{fmt.Sprintf(keyScan, lmb.index), fmt.Sprintf(blkScan, lmb.index)} {
		b := filepath.Join(mdir, mbf)
		a := filepath.Join(ndir, mbf)
		if err := os.Rename(b, a); err != nil && !os.IsNotExist(err) {
			fs.dios.release()
			return purged, bytes, err
		}
	}
	// Purge all remaining messages.
	if err := os.Rename(mdir, pdir); err != nil {
		fs.dios.release()
		return purged, bytes, err
	}
	// Rename the directory back to be left only with the tombstone.
	if err := os.Rename(ndir, mdir); err != nil {
		fs.dios.release()
		return purged, bytes, err
	}
	fs.dios.release()

	// Remove the purged messages directory asynchronously.
	go func() {
		fs.dios.acquire()
		_ = os.RemoveAll(pdir)
		fs.dios.release()
	}()

	// Re-enable writing for the lmb.
	lmb.mu.Lock()
	err = lmb.enableForWriting(fs.fip)
	lmb.mu.Unlock()
	if err != nil {
		return purged, bytes, err
	}

	return purged, bytes, nil
}

// Lock and dios should be held.
func (fs *fileStore) recoverPartialPurge() error {
	mdir := filepath.Join(fs.fcfg.StoreDir, msgDir)
	ndir := filepath.Join(fs.fcfg.StoreDir, newMsgDir)
	if entries, err := os.ReadDir(ndir); err != nil && !os.IsNotExist(err) {
		return err
	} else if err == nil {
		hasBlk := slices.ContainsFunc(entries, func(e os.DirEntry) bool {
			return strings.HasSuffix(e.Name(), blkSuffix)
		})
		if hasBlk {
			// We have a tombstone, we can purge the old messages.
			if err = os.RemoveAll(mdir); err != nil {
				return err
			}
			if err = os.Rename(ndir, mdir); err != nil {
				return err
			}
		} else {
			// No .blk means the purge did not complete, so clear
			// any progress made by the partial purge.
			for _, entry := range entries {
				var index uint32
				if n, err := fmt.Sscanf(entry.Name(), keyScan, &index); err != nil || n != 1 {
					continue
				}
				// Found a key file, remove the corresponding .blk, if any.
				// Recovery may otherwise wrongly conclude that the .blk is
				// is plaintext, and consider it corrupt when trying to open it.
				err := os.Remove(filepath.Join(mdir, fmt.Sprintf(blkScan, index)))
				if err != nil && !os.IsNotExist(err) {
					return err
				}
			}
			_ = os.RemoveAll(ndir)
			return nil
		}
	}
	pdir := filepath.Join(fs.fcfg.StoreDir, purgeDir)
	if _, err := os.Stat(pdir); err == nil {
		_ = os.RemoveAll(pdir)
	}
	return nil
}

// Compact will remove all messages from this store up to
// but not including the seq parameter.
// Will return the number of purged messages.
func (fs *fileStore) Compact(seq uint64) (uint64, error) {
	return fs.compact(seq)
}

func (fs *fileStore) compact(seq uint64) (uint64, error) {
	var err error
	var purged, bytes uint64
	fs.mu.Lock()
	if fs.isClosed() {
		fs.mu.Unlock()
		return 0, ErrStoreClosed
	}
	if seq == 0 || seq > fs.state.LastSeq {
		purged, bytes, err = fs.purgeLocked(seq)
	} else {
		purged, bytes, err = fs.compactLocked(seq)
	}
	if err != nil {
		fs.setWriteErr(err)
		fs.mu.Unlock()
		return purged, err
	}
	cb := fs.scb
	fs.mu.Unlock()

	// Force a new index.db to be written.
	if purged > 0 {
		fs.forceWriteFullState()
	}

	if cb != nil && purged > 0 {
		cb(-int64(purged), -int64(bytes), 0, _EMPTY_)
	}

	return purged, nil
}

// Lock must be held.
func (fs *fileStore) compactLocked(seq uint64) (purged, bytes uint64, err error) {
	// Always return previous write errors.
	if err := fs.werr; err != nil {
		return 0, 0, err
	}

	// Short-circuit if the store was already compacted past this point.
	if fs.state.FirstSeq > seq {
		return 0, 0, nil
	}
	// We have to delete interior messages.
	smb := fs.selectMsgBlock(seq)
	if smb == nil {
		return 0, 0, nil
	}

	// All msgblocks up to this one can be thrown away.
	var deleted int
	for _, mb := range fs.blks {
		if mb == smb {
			break
		}
		mb.mu.Lock()
		purged += mb.msgs
		bytes += mb.bytes
		// Make sure we do subject cleanup as well.
		if err := mb.ensurePerSubjectInfoLoaded(); err != nil {
			mb.mu.Unlock()
			return 0, 0, err
		}
		mb.fss.IterOrdered(func(bsubj []byte, ss *SimpleState) bool {
			subj := bytesToString(bsubj)
			for i := uint64(0); i < ss.Msgs; i++ {
				fs.removePerSubject(subj)
			}
			return true
		})
		// Now close.
		err := mb.dirtyCloseWithRemove(true)
		mb.mu.Unlock()
		if err != nil {
			return purged, bytes, err
		}
		deleted++
	}

	var smv StoreMsg
	var tombs []msgId

	smb.mu.Lock()
	if atomic.LoadUint64(&smb.first.seq) == seq {
		fs.state.FirstSeq = atomic.LoadUint64(&smb.first.seq)
		fs.state.FirstTime = time.Unix(0, smb.first.ts).UTC()
		goto SKIP
	}

	// Make sure we have the messages loaded.
	if smb.cacheNotLoaded() {
		if err = smb.loadMsgsWithLock(); err != nil {
			smb.mu.Unlock()
			return purged, bytes, err
		}
		defer func() {
			// The block lock is released once we get here, so need to re-acquire.
			smb.mu.Lock()
			smb.finishedWithCache()
			smb.mu.Unlock()
		}()
	}
	for mseq := atomic.LoadUint64(&smb.first.seq); mseq < seq; mseq++ {
		sm, err := smb.cacheLookupNoCopy(mseq, &smv)
		if err == errDeletedMsg {
			// Update dmap.
			if !smb.dmap.IsEmpty() {
				smb.dmap.Delete(mseq)
			}
		} else if sm != nil {
			sz := fileStoreMsgSize(sm.subj, sm.hdr, sm.msg)
			if smb.msgs > 0 {
				smb.msgs--
				if sz > smb.bytes {
					sz = smb.bytes
				}
				smb.bytes -= sz
				bytes += sz
				purged++
			}
			// Update fss
			if _, err := smb.removeSeqPerSubject(sm.subj, mseq); err != nil {
				smb.mu.Unlock()
				return purged, bytes, err
			}
			fs.removePerSubject(sm.subj)
			tombs = append(tombs, msgId{sm.seq, sm.ts})
		}
	}

	// Check if empty after processing, could happen if tail of messages are all deleted.
	if isEmpty := smb.msgs == 0; isEmpty {
		// Only remove if not the last block.
		if smb != fs.lmb {
			if err = smb.dirtyCloseWithRemove(true); err != nil {
				smb.mu.Unlock()
				return purged, bytes, err
			}
			deleted++
		} else {
			// Make sure to sync changes.
			smb.needSync = true
		}
		// Update fs first here as well.
		fs.state.FirstSeq = atomic.LoadUint64(&smb.last.seq) + 1
		fs.state.FirstTime = time.Time{}

	} else {
		// Make sure to sync changes.
		smb.needSync = true
		// Just for start condition for selectNextFirst.
		if smb.first.seq < seq {
			atomic.StoreUint64(&smb.first.seq, seq-1)
		} else {
			// selectNextFirst always adds 1, so need to subtract 1 here.
			atomic.StoreUint64(&smb.first.seq, smb.first.seq-1)
		}
		smb.selectNextFirst()

		fs.state.FirstSeq = atomic.LoadUint64(&smb.first.seq)
		fs.state.FirstTime = time.Unix(0, smb.first.ts).UTC()

		// Check if we should reclaim the head space from this block.
		// This will be optimistic only, so don't continue if we encounter any errors here.
		if smb.rbytes > compactMinimum && smb.bytes*2 < smb.rbytes {
			var moff uint32
			moff, _, _, err = smb.slotInfo(int(atomic.LoadUint64(&smb.first.seq) - smb.cache.fseq))
			if err != nil {
				smb.mu.Unlock()
				return purged, bytes, err
			} else if moff >= uint32(len(smb.cache.buf)) {
				goto SKIP
			}
			buf := smb.cache.buf[moff:]
			// Don't reuse, copy to new recycled buf.
			nbuf := getMsgBlockBuf(len(buf))
			// Recycle our nbuf when we are done.
			defer recycleMsgBlockBuf(nbuf)
			nbuf = append(nbuf, buf...)
			smb.closeFDsLockedNoCheck()
			// Check for compression.
			if smb.cmp != NoCompression && len(nbuf) > 0 {
				originalSize := len(nbuf)
				if nbuf, err = smb.cmp.Compress(nbuf); err != nil {
					smb.mu.Unlock()
					return purged, bytes, err
				}
				meta := &CompressionInfo{
					Algorithm:    smb.cmp,
					OriginalSize: uint64(originalSize),
				}
				nbuf = append(meta.MarshalMetadata(), nbuf...)
			}
			// Check for encryption.
			if smb.bek != nil && len(nbuf) > 0 {
				// Recreate to reset counter.
				bek, err := genBlockEncryptionKey(smb.fs.fcfg.Cipher, smb.seed, smb.nonce)
				if err != nil {
					smb.mu.Unlock()
					return purged, bytes, err
				}
				// For future writes make sure to set smb.bek to keep counter correct.
				smb.bek = bek
				smb.bek.XORKeyStream(nbuf, nbuf)
			}

			// We will write to a new file and mv/rename it in case of failure.
			sync := fs.syncAlways.Load() || fs.syncOnFlush.Load()
			err = writeAtomically(fs.dios, smb.mfn, nbuf, defaultFilePerms, sync)
			if err != nil {
				smb.mu.Unlock()
				return purged, bytes, err
			}
			smb.needSync = !sync

			// Make sure to remove fss state.
			smb.fss = nil
			smb.clearCacheAndOffset()
			smb.rbytes = uint64(len(nbuf))
			// Make sure we don't write any additional tombstones.
			tombs = nil
		}
	}

SKIP:
	smb.mu.Unlock()

	// Write any tombstones as needed.
	// When writing multiple tombstones we will flush at the end.
	if len(tombs) > 0 {
		for _, tomb := range tombs {
			if err = fs.writeTombstoneNoFlush(tomb.seq, tomb.ts); err != nil {
				return purged, bytes, err
			}
		}
		// Flush any pending. If we change blocks the newMsgBlockForWrite() will flush any pending for us.
		if lmb := fs.lmb; lmb != nil {
			if err = lmb.flushPendingMsgs(); err != nil {
				return purged, bytes, err
			}
		}
	}

	if deleted > 0 {
		// Update block map.
		if fs.bim != nil {
			for _, mb := range fs.blks[:deleted] {
				delete(fs.bim, mb.index)
			}
		}
		// Update blks slice.
		fs.blks = copyMsgBlocks(fs.blks[deleted:])
		if lb := len(fs.blks); lb == 0 {
			fs.lmb = nil
		} else {
			fs.lmb = fs.blks[lb-1]
		}
	}

	// Update top level accounting.
	if purged > fs.state.Msgs {
		purged = fs.state.Msgs
	}
	fs.state.Msgs -= purged
	if fs.state.Msgs == 0 {
		fs.state.FirstSeq = fs.state.LastSeq + 1
		fs.state.FirstTime = time.Time{}
	}

	if bytes > fs.state.Bytes {
		bytes = fs.state.Bytes
	}
	fs.state.Bytes -= bytes

	// Any existing state file no longer applicable. We will force write a new one
	// after we release the lock.
	os.Remove(filepath.Join(fs.fcfg.StoreDir, msgDir, streamStreamStateFile))
	fs.dirty++

	return purged, bytes, nil
}

// Will completely reset our store.
func (fs *fileStore) reset() error {
	fs.mu.Lock()
	if fs.isClosed() {
		fs.mu.Unlock()
		return ErrStoreClosed
	}

	var purged, bytes uint64
	cb := fs.scb

	for _, mb := range fs.blks {
		mb.mu.Lock()
		purged += mb.msgs
		bytes += mb.bytes
		err := mb.dirtyCloseWithRemove(true)
		mb.mu.Unlock()
		if err != nil {
			fs.mu.Unlock()
			return err
		}
	}

	// Reset
	fs.state.FirstSeq = 0
	fs.state.FirstTime = time.Time{}
	fs.state.LastSeq = 0
	fs.state.LastTime = time.Now().UTC()
	// Update msgs and bytes.
	fs.state.Msgs = 0
	fs.state.Bytes = 0

	// Reset blocks.
	fs.blks, fs.lmb = nil, nil

	// Reset subject mappings.
	fs.psim, fs.tsl = fs.psim.Empty(), 0
	fs.sdm.empty()
	fs.bim = make(map[uint32]*msgBlock)

	// If we purged anything, make sure we kick flush state loop.
	if purged > 0 {
		fs.dirty++
	}

	fs.mu.Unlock()

	if cb != nil {
		cb(-int64(purged), -int64(bytes), 0, _EMPTY_)
	}

	return nil
}

// Return all active tombstones in this msgBlock.
func (mb *msgBlock) tombs() []msgId {
	mb.mu.Lock()
	defer mb.mu.Unlock()
	return mb.tombsLocked()
}

// Return all active tombstones in this msgBlock.
// Write lock should be held.
func (mb *msgBlock) tombsLocked() []msgId {
	if mb.cacheNotLoaded() {
		if err := mb.loadMsgsWithLock(); err != nil {
			return nil
		}
	}
	defer mb.finishedWithCache()

	var tombs []msgId
	var le = binary.LittleEndian
	buf := mb.cache.buf

	for index, lbuf := uint32(0), uint32(len(buf)); index < lbuf; {
		if index+msgHdrSize > lbuf {
			return tombs
		}
		hdr := buf[index : index+msgHdrSize]
		rl, seq := le.Uint32(hdr[0:]), le.Uint64(hdr[4:])
		// Clear any headers bit that could be set.
		rl &^= hbit
		// Check for tombstones.
		if seq&tbit != 0 {
			ts := int64(le.Uint64(hdr[12:]))
			tombs = append(tombs, msgId{seq &^ tbit, ts})
		}
		// Advance to next record.
		index += rl
	}

	return tombs
}

// fs lock should be held.
func (mb *msgBlock) numPriorTombs() int {
	mb.mu.Lock()
	defer mb.mu.Unlock()
	return mb.numPriorTombsLocked()
}

// Return number of tombstones for messages prior to this msgBlock.
// Both locks should be held.
// Write lock should be held for block.
func (mb *msgBlock) numPriorTombsLocked() int {
	if mb.cacheNotLoaded() {
		if err := mb.loadMsgsWithLock(); err != nil {
			return 0
		}
	}
	defer mb.finishedWithCache()

	var fseq uint64
	var tombs int
	var le = binary.LittleEndian
	buf := mb.cache.buf

	for index, lbuf := uint32(0), uint32(len(buf)); index < lbuf; {
		if index+msgHdrSize > lbuf {
			return tombs
		}
		hdr := buf[index : index+msgHdrSize]
		rl, seq := le.Uint32(hdr[0:]), le.Uint64(hdr[4:])
		// Clear any headers bit that could be set.
		rl &^= hbit
		// Check for tombstones.
		if seq&tbit != 0 {
			seq = seq &^ tbit
			// Tombstones below the global first seq are irrelevant.
			// And we only count tombstones below this block's first seq.
			if seq >= mb.fs.state.FirstSeq && (fseq == 0 || seq < fseq) {
				tombs++
			}
			index += rl
			continue
		}
		if seq == 0 || seq&ebit != 0 {
			index += rl
			continue
		}
		// Advance to next record.
		index += rl
		if fseq == 0 {
			fseq = seq
		}
	}

	return tombs
}

// Truncate will truncate a stream store up to seq. Sequence needs to be valid.
func (fs *fileStore) Truncate(seq uint64) (rerr error) {
	// Check for request to reset.
	if seq == 0 {
		return fs.reset()
	}

	fs.mu.Lock()
	if fs.isClosed() {
		fs.mu.Unlock()
		return ErrStoreClosed
	}
	// Always return previous write errors.
	if err := fs.werr; err != nil {
		fs.mu.Unlock()
		return err
	}

	// Persist any write errors.
	defer func() {
		if rerr != nil {
			fs.mu.Lock()
			fs.setWriteErr(rerr)
			fs.mu.Unlock()
		}
	}()

	// Any existing state file will no longer be applicable. We will force write a new one
	// at the end, after we release the lock.
	os.Remove(filepath.Join(fs.fcfg.StoreDir, msgDir, streamStreamStateFile))

	var hasLsm bool
	var lastTime int64
	var lsm StoreMsg
	smb := fs.selectMsgBlock(seq)
	if smb != nil {
		_, _, err := smb.prevMatching(fwcs, true, seq, &lsm)
		if err == nil && lsm.seq == seq {
			hasLsm = true
			lastTime = lsm.ts
		}
		if err != nil && err != ErrStoreMsgNotFound {
			fs.mu.Unlock()
			return err
		}
	}

	// Reset last so new block doesn't contain truncated sequences/timestamps.
	if !hasLsm {
		if smb != nil {
			lastTime = smb.last.ts
		} else {
			lastTime = fs.state.LastTime.UnixNano()
		}
	}
	fs.state.LastSeq = seq
	fs.state.LastTime = time.Unix(0, lastTime).UTC()

	// Always create a new write block for any tombstones.
	// We'll truncate the selected message block as the last step, so can't write tombstones to it.
	// If we end up not needing to write tombstones, this block will be cleaned up at the end.
	tmb, err := fs.newMsgBlockForWrite()
	if err != nil {
		fs.mu.Unlock()
		return err
	}

	// The selected message block needs to be removed if it needs to be fully truncated.
	var removeSmb bool
	if smb != nil {
		removeSmb = atomic.LoadUint64(&smb.first.seq) > seq
	}

	// If the selected block is not found or the message was deleted, we'll need to write a tombstone
	// at the truncated sequence so we don't roll backward on our last sequence and timestamp.
	if !hasLsm || removeSmb {
		if err = fs.writeTombstone(seq, lastTime); err != nil {
			fs.mu.Unlock()
			return err
		}
	}

	var purged, bytes uint64

	// Remove any left over msg blocks.
	getLastMsgBlock := func() *msgBlock {
		// Start at one before last, tmb will be the last most of the time
		// unless a new block gets added for tombstones.
		for i := len(fs.blks) - 2; i >= 0; i-- {
			if mb := fs.blks[i]; mb.index < tmb.index {
				return mb
			}
		}
		return nil
	}
	for mb := getLastMsgBlock(); mb != nil && mb != smb; mb = getLastMsgBlock() {
		mb.mu.Lock()
		purged += mb.msgs
		bytes += mb.bytes

		// We could have tombstones for messages before the truncated sequence.
		// Need to store those for blocks we're about to remove.
		if tombs := mb.tombsLocked(); len(tombs) > 0 {
			// Temporarily unlock while we write tombstones.
			mb.mu.Unlock()
			for _, tomb := range tombs {
				if tomb.seq < seq {
					if err = fs.writeTombstone(tomb.seq, tomb.ts); err != nil {
						fs.mu.Unlock()
						return err
					}
				}
			}
			mb.mu.Lock()
		}
		err = fs.forceRemoveMsgBlock(mb)
		mb.mu.Unlock()
		if err != nil {
			fs.mu.Unlock()
			return err
		}
	}

	hasWrittenTombstones := len(tmb.tombs()) > 0
	if smb != nil {
		smb.mu.Lock()
		if removeSmb {
			purged += smb.msgs
			bytes += smb.bytes

			// We could have tombstones for messages before the truncated sequence.
			if tombs := smb.tombsLocked(); len(tombs) > 0 {
				// Temporarily unlock while we write tombstones.
				smb.mu.Unlock()
				for _, tomb := range tombs {
					if tomb.seq < seq {
						if err = fs.writeTombstone(tomb.seq, tomb.ts); err != nil {
							fs.mu.Unlock()
							return err
						}
					}
				}
				smb.mu.Lock()
			}
			err = fs.forceRemoveMsgBlock(smb)
			smb.mu.Unlock()
			if err != nil {
				fs.mu.Unlock()
				return err
			}
			goto SKIP
		}

		// Make sure writeable.
		if err := smb.enableForWriting(fs.fip); err != nil {
			smb.mu.Unlock()
			fs.mu.Unlock()
			return err
		}

		// Truncate our selected message block.
		nmsgs, nbytes, err := smb.truncate(lsm.seq, lsm.ts)
		if err != nil {
			smb.mu.Unlock()
			fs.mu.Unlock()
			return fmt.Errorf("smb.truncate: %w", err)
		}
		// Account for the truncated msgs and bytes.
		purged += nmsgs
		bytes += nbytes

		// The selected message block is not the last anymore, need to close down resources.
		if hasWrittenTombstones {
			// Quit our loops.
			if smb.qch != nil {
				close(smb.qch)
				smb.qch = nil
			}
			smb.closeFDsLockedNoCheck()
			smb.recompressOnDiskIfNeeded()
		}
		smb.mu.Unlock()
	}

SKIP:
	// If no tombstones were written, we can remove the block and
	// purely rely on the selected block as the last block.
	if !hasWrittenTombstones {
		fs.lmb = smb
		tmb.mu.Lock()
		err = fs.forceRemoveMsgBlock(tmb)
		tmb.mu.Unlock()
		if err != nil {
			fs.mu.Unlock()
			return err
		}
	}

	// Reset last.
	fs.state.LastSeq = seq
	fs.state.LastTime = time.Unix(0, lastTime).UTC()
	// Update msgs and bytes.
	if purged > fs.state.Msgs {
		purged = fs.state.Msgs
	}
	fs.state.Msgs -= purged
	if bytes > fs.state.Bytes {
		bytes = fs.state.Bytes
	}
	fs.state.Bytes -= bytes

	// Reset our subject lookup info.
	if err = fs.resetGlobalPerSubjectInfo(); err != nil {
		fs.mu.Unlock()
		return err
	}

	fs.dirty++

	cb := fs.scb
	fs.mu.Unlock()

	// Force a new index.db to be written.
	if purged > 0 {
		fs.forceWriteFullState()
	}

	if cb != nil {
		cb(-int64(purged), -int64(bytes), 0, _EMPTY_)
	}

	return nil
}

func (fs *fileStore) lastSeq() uint64 {
	fs.mu.RLock()
	seq := fs.state.LastSeq
	fs.mu.RUnlock()
	return seq
}

// Returns number of msg blks.
func (fs *fileStore) numMsgBlocks() int {
	fs.mu.RLock()
	defer fs.mu.RUnlock()
	return len(fs.blks)
}

// Will add a new msgBlock.
// Lock should be held.
func (fs *fileStore) addMsgBlock(mb *msgBlock) {
	fs.blks = append(fs.blks, mb)
	fs.lmb = mb
	fs.bim[mb.index] = mb
}

// Remove from our list of blks.
// Both locks should be held.
func (fs *fileStore) removeMsgBlockFromList(mb *msgBlock) {
	// Remove from list.
	for i, omb := range fs.blks {
		if mb == omb {
			fs.dirty++
			blks := append(fs.blks[:i], fs.blks[i+1:]...)
			fs.blks = copyMsgBlocks(blks)
			if fs.bim != nil {
				delete(fs.bim, mb.index)
			}
			break
		}
	}
}

// Removes the msgBlock
// Both locks should be held.
func (fs *fileStore) removeMsgBlock(mb *msgBlock) error {
	// Check for us being last message block
	lseq, lts := atomic.LoadUint64(&mb.last.seq), mb.last.ts
	if mb == fs.lmb {
		// Creating a new message write block requires that the lmb lock is not held.
		mb.mu.Unlock()
		// Write the tombstone to remember since this was last block.
		if lmb, err := fs.newMsgBlockForWrite(); err != nil || lmb == nil {
			if err != nil {
				err = errors.New("lmb missing")
			}
			// Re-acquire mb lock
			mb.mu.Lock()
			return err
		} else if err = fs.writeTombstone(lseq, lts); err != nil {
			// Re-acquire mb lock
			mb.mu.Lock()
			return err
		}
		mb.mu.Lock()
	} else if lseq == fs.state.LastSeq {
		// Need to write a tombstone for the last sequence if we're removing the block containing it.
		if err := fs.writeTombstone(lseq, lts); err != nil {
			return err
		}
	}
	// Only delete message block after (potentially) writing a tombstone.
	// But only if it doesn't contain any tombstones for prior blocks.
	if mb.numPriorTombsLocked() > 0 {
		return nil
	}
	return fs.forceRemoveMsgBlock(mb)
}

// Removes the msgBlock, without writing tombstones to ensure the last sequence is preserved.
// Both locks should be held.
func (fs *fileStore) forceRemoveMsgBlock(mb *msgBlock) error {
	if err := mb.dirtyCloseWithRemove(true); err != nil {
		return err
	}
	fs.removeMsgBlockFromList(mb)
	return nil
}

// Purges and removes the msgBlock from the store.
// If agg is set the storage updates for fully removed blocks are aggregated.
// Lock should be held.
func (fs *fileStore) purgeMsgBlock(mb *msgBlock, agg *storeUpdateAgg) error {
	mb.mu.Lock()
	// Adjust per-subject tracking if present.
	if err := mb.ensurePerSubjectInfoLoaded(); err != nil {
		mb.mu.Unlock()
		return err
	} else if mb.fss != nil {
		mb.fss.IterFast(func(bsubj []byte, ss *SimpleState) bool {
			subj := bytesToString(bsubj)
			for range ss.Msgs {
				fs.removePerSubject(subj)
			}
			return true
		})
	}
	// Clean up scheduled message metadata if we know this block contained any.
	if fs.scheduling != nil && mb.schedules > 0 {
		if mb.cacheNotLoaded() {
			if err := mb.loadMsgsWithLock(); err != nil {
				mb.mu.Unlock()
				return err
			}
		}
		var smv StoreMsg
		fseq, lseq := atomic.LoadUint64(&mb.first.seq), atomic.LoadUint64(&mb.last.seq)
		for seq := fseq; seq <= lseq; seq++ {
			sm, err := mb.cacheLookupNoCopy(seq, &smv)
			if err != nil && err != errDeletedMsg {
				mb.mu.Unlock()
				return err
			}
			if sm == nil {
				continue
			}
			if schedule, apiErr := getMessageSchedule(sm.hdr); apiErr == nil && !schedule.IsZero() {
				fs.scheduling.remove(seq)
			}
		}
	}
	// Update top level accounting.
	msgs, bytes := mb.msgs, mb.bytes
	if msgs > fs.state.Msgs {
		msgs = fs.state.Msgs
	}
	if bytes > fs.state.Bytes {
		bytes = fs.state.Bytes
	}
	fs.state.Msgs -= msgs
	fs.state.Bytes -= bytes
	if err := fs.removeMsgBlock(mb); err != nil {
		mb.mu.Unlock()
		return err
	}
	mb.tryForceExpireCacheLocked()
	mb.finishedWithCache()
	mb.mu.Unlock()
	if err := fs.selectNextFirst(); err != nil {
		return err
	}

	if agg != nil {
		// Aggregating, so no callback and no need to release fs.mu.
		agg.md, agg.bd = agg.md-int64(msgs), agg.bd-int64(bytes)
	} else if cb := fs.scb; cb != nil {
		// If we have a callback registered, we need to release lock regardless since consumers will recalculate pending.
		fs.mu.Unlock()
		// Storage updates.
		cb(-int64(msgs), -int64(bytes), 0, _EMPTY_)
		fs.mu.Lock()
	}
	return nil
}

// Called by purge to simply get rid of the cache and close our fds.
// Lock should not be held.
func (mb *msgBlock) dirtyClose() {
	mb.mu.Lock()
	defer mb.mu.Unlock()
	mb.dirtyCloseWithRemove(false)
}

// Should be called with lock held.
func (mb *msgBlock) dirtyCloseWithRemove(remove bool) error {
	if mb == nil {
		return nil
	}
	// Stop cache expiration timer.
	if mb.ctmr != nil {
		mb.ctmr.Stop()
		mb.ctmr = nil
	}
	// Close cache
	mb.clearCacheAndOffset()
	// Quit our loops.
	if mb.qch != nil {
		close(mb.qch)
		mb.qch = nil
	}
	if fd := mb.mfd; fd != nil {
		mb.mfd = nil
		if err := fd.Close(); err != nil && !os.IsNotExist(err) {
			return err
		}
	}
	if remove {
		// The block is being destroyed, so mark it closed.
		mb.closed = true
		// Clear any tracking by subject if we are removing.
		mb.fss = nil
		if mb.mfn != _EMPTY_ {
			if err := os.Remove(mb.mfn); err != nil && !os.IsNotExist(err) {
				return err
			}
			mb.mfn = _EMPTY_
		}
		if mb.kfn != _EMPTY_ {
			if err := os.Remove(mb.kfn); err != nil && !os.IsNotExist(err) {
				return err
			}
		}
	}
	return nil
}

// Remove a seq from the fss and select new first.
// Lock should be held.
func (mb *msgBlock) removeSeqPerSubject(subj string, seq uint64) (uint64, error) {
	if err := mb.ensurePerSubjectInfoLoaded(); err != nil {
		return 0, err
	}
	if mb.fss == nil {
		return 0, nil
	}
	bsubj := stringToBytes(subj)
	ss, ok := mb.fss.Find(bsubj)
	if !ok || ss == nil {
		return 0, nil
	}

	mb.fs.sdm.removeSeqAndSubject(seq, subj)
	if ss.Msgs == 1 {
		mb.fss.Delete(bsubj)
		return 0, nil
	}

	ss.Msgs--

	// Only one left.
	if ss.Msgs == 1 {
		if !ss.lastNeedsUpdate && seq != ss.Last {
			ss.First = ss.Last
			ss.firstNeedsUpdate = false
			return 1, nil
		}
		if !ss.firstNeedsUpdate && seq != ss.First {
			ss.Last = ss.First
			ss.lastNeedsUpdate = false
			return 1, nil
		}
	}

	// We can lazily calculate the first/last sequence when needed.
	ss.firstNeedsUpdate = seq == ss.First || ss.firstNeedsUpdate
	ss.lastNeedsUpdate = seq == ss.Last || ss.lastNeedsUpdate

	return ss.Msgs, nil
}

// Will recalculate the first and/or last sequence for this subject in this block.
// Will avoid slower path message lookups and scan the cache directly instead.
func (mb *msgBlock) recalculateForSubj(subj string, ss *SimpleState) error {
	// Need to make sure messages are loaded.
	needsCleanup := mb.cache == nil
	if mb.cacheNotLoaded() {
		if err := mb.loadMsgsWithLock(); err != nil {
			return err
		}
	}
	if needsCleanup {
		defer mb.finishedWithCache()
	}

	startSlot := int(ss.First - mb.cache.fseq)
	if startSlot < 0 {
		startSlot = 0
	}
	if startSlot >= len(mb.cache.idx) {
		ss.First = ss.Last
		ss.firstNeedsUpdate = false
		ss.lastNeedsUpdate = false
		return nil
	}

	endSlot := int(ss.Last - mb.cache.fseq)
	if endSlot < 0 {
		endSlot = 0
	}
	if endSlot >= len(mb.cache.idx) || startSlot > endSlot {
		return nil
	}

	var le = binary.LittleEndian
	if ss.firstNeedsUpdate {
		// Mark first as updated.
		ss.firstNeedsUpdate = false

		fseq := ss.First + 1
		if mbFseq := atomic.LoadUint64(&mb.first.seq); fseq < mbFseq {
			fseq = mbFseq
		}
		for slot := startSlot; slot < len(mb.cache.idx); slot++ {
			bi := mb.cache.idx[slot] &^ cbit
			if bi == dbit {
				// delete marker so skip.
				continue
			}
			li := int(bi)
			if li >= len(mb.cache.buf) {
				ss.First = ss.Last
				// Only need to reset ss.lastNeedsUpdate, ss.firstNeedsUpdate is already reset above.
				ss.lastNeedsUpdate = false
				return nil
			}
			buf := mb.cache.buf[li:]
			hdr := buf[:msgHdrSize]
			slen := int(le.Uint16(hdr[20:]))
			if subj == bytesToString(buf[msgHdrSize:msgHdrSize+slen]) {
				seq := le.Uint64(hdr[4:])
				if seq < fseq || seq&ebit != 0 || mb.dmap.Exists(seq) {
					continue
				}
				ss.First = seq
				if ss.Msgs == 1 {
					ss.Last = seq
					ss.lastNeedsUpdate = false
					return nil
				}
				// Skip the start slot ahead, if we need to recalculate last we can stop early.
				startSlot = slot
				break
			}
		}
	}
	if ss.lastNeedsUpdate {
		// Mark last as updated.
		ss.lastNeedsUpdate = false

		lseq := ss.Last - 1
		if mbLseq := atomic.LoadUint64(&mb.last.seq); lseq > mbLseq {
			lseq = mbLseq
		}
		for slot := endSlot; slot >= startSlot; slot-- {
			bi := mb.cache.idx[slot] &^ cbit
			if bi == dbit {
				// delete marker so skip.
				continue
			}
			li := int(bi)
			if li >= len(mb.cache.buf) {
				// Can't overwrite ss.Last, just skip.
				return nil
			}
			buf := mb.cache.buf[li:]
			hdr := buf[:msgHdrSize]
			slen := int(le.Uint16(hdr[20:]))
			if subj == bytesToString(buf[msgHdrSize:msgHdrSize+slen]) {
				seq := le.Uint64(hdr[4:])
				if seq > lseq || seq&ebit != 0 || mb.dmap.Exists(seq) {
					continue
				}
				// Sequence should never be lower, but guard against it nonetheless.
				if seq < ss.First {
					seq = ss.First
				}
				ss.Last = seq
				if ss.Msgs == 1 {
					ss.First = seq
					ss.firstNeedsUpdate = false
				}
				return nil
			}
		}
	}
	return nil
}

// Lock should be held.
func (fs *fileStore) resetGlobalPerSubjectInfo() error {
	// Clear any global subject state.
	fs.psim, fs.tsl = fs.psim.Empty(), 0
	if fs.noTrackSubjects() {
		return nil
	}
	for _, mb := range fs.blks {
		if err := fs.populateGlobalPerSubjectInfo(mb); err != nil {
			return err
		}
	}
	return nil
}

// Lock should be held.
func (mb *msgBlock) resetPerSubjectInfo() error {
	mb.fss = nil
	return mb.generatePerSubjectInfo()
}

// generatePerSubjectInfo will generate the per subject info via the raw msg block.
// Lock should be held.
func (mb *msgBlock) generatePerSubjectInfo() error {
	// Check if this mb is empty. This can happen when its the last one and we are holding onto it for seq and timestamp info.
	if mb.msgs == 0 {
		return nil
	}

	needsCleanup := mb.cache == nil
	if mb.cacheNotLoaded() {
		if err := mb.loadMsgsWithLock(); err != nil {
			if needsCleanup {
				mb.finishedWithCache()
			}
			return err
		}
	}
	if needsCleanup {
		defer mb.finishedWithCache()
	}
	// indexCacheBuf can produce fss now, so if non-nil we are good.
	if mb.fss != nil {
		return nil
	}

	// Create new one regardless.
	mb.fss = mb.fss.Empty()

	var smv StoreMsg
	fseq, lseq := atomic.LoadUint64(&mb.first.seq), atomic.LoadUint64(&mb.last.seq)
	for seq := fseq; seq <= lseq; seq++ {
		if mb.dmap.Exists(seq) {
			// Optimisation to avoid calling cacheLookup which hits time.Now().
			// It gets set later on if the fss is non-empty anyway.
			continue
		}
		sm, err := mb.cacheLookupNoCopy(seq, &smv)
		if err != nil {
			// Since we are walking by sequence we can ignore some errors that are benign to rebuilding our state.
			if err == ErrStoreMsgNotFound || err == errDeletedMsg {
				continue
			}
			if err == errNoCache {
				return nil
			}
			// Clear partially built fss so callers don't operate on incomplete state.
			mb.fss = nil
			mb.clearCacheAndOffset()
			return err
		}
		if sm != nil && len(sm.subj) > 0 {
			if ss, ok := mb.fss.Find(stringToBytes(sm.subj)); ok && ss != nil {
				ss.Msgs++
				ss.Last = seq
				ss.lastNeedsUpdate = false
			} else {
				mb.fss.Insert(stringToBytes(sm.subj), SimpleState{Msgs: 1, First: seq, Last: seq})
			}
		}
	}

	if mb.fss.Size() > 0 {
		// Make sure we run the cache expire timer.
		mb.llts = ats.AccessTime()
		// Mark fss activity same as load time.
		mb.lsts = mb.llts
		mb.startCacheExpireTimer()
	}
	return nil
}

// Helper to make sure fss loaded if we are tracking.
// Lock should be held
func (mb *msgBlock) ensurePerSubjectInfoLoaded() error {
	if mb.fss != nil || mb.noTrack {
		if mb.fss != nil {
			// Mark fss activity.
			mb.lsts = ats.AccessTime()
		}
		return nil
	}
	if mb.msgs == 0 {
		mb.fss = stree.NewSubjectTree[SimpleState]()
		return nil
	}
	return mb.generatePerSubjectInfo()
}

// Called on recovery to populate the global psim state.
// Lock should be held.
func (fs *fileStore) populateGlobalPerSubjectInfo(mb *msgBlock) error {
	mb.mu.Lock()
	defer mb.mu.Unlock()

	if err := mb.ensurePerSubjectInfoLoaded(); err != nil {
		return err
	}

	// Now populate psim.
	mb.fss.IterFast(func(bsubj []byte, ss *SimpleState) bool {
		if len(bsubj) > 0 {
			if info, ok := fs.psim.Find(bsubj); ok {
				info.total += ss.Msgs
				if mb.index > info.lblk {
					info.lblk = mb.index
				}
			} else {
				fs.psim.Insert(bsubj, psi{total: ss.Msgs, fblk: mb.index, lblk: mb.index})
				fs.tsl += len(bsubj)
			}
		}
		return true
	})
	return nil
}

// Calls os.RemoveAll on the given `dir` directory, but if an error occurs,
// retries up to one second. If that still fails, returns the last error
// that os.RemoveAll returned.
func removeAllWithRetry(dios *diskIOSemaphore, dir string) error {
	dios.acquire()
	err := os.RemoveAll(dir)
	dios.release()
	if err == nil {
		return nil
	}
	ttl := time.Now().Add(time.Second)
	for time.Now().Before(ttl) {
		time.Sleep(10 * time.Millisecond)
		dios.acquire()
		err = os.RemoveAll(dir)
		dios.release()
		if err == nil {
			return nil
		}
	}
	return err
}

// Close the message block.
func (mb *msgBlock) close(sync bool) {
	if mb == nil {
		return
	}
	mb.mu.Lock()
	defer mb.mu.Unlock()

	if mb.closed {
		return
	}

	// Stop cache expiration timer.
	if mb.ctmr != nil {
		mb.ctmr.Stop()
		mb.ctmr = nil
	}

	// Clear fss.
	mb.fss = nil

	// Flush pending writes.
	mb.flushPendingMsgsLocked()
	// Close cache
	mb.clearCacheAndOffset()
	// Quit our loops.
	if mb.qch != nil {
		close(mb.qch)
		mb.qch = nil
	}
	if mb.mfd != nil {
		if sync {
			mb.mfd.Sync()
		}
		mb.mfd.Close()
	}
	mb.mfd = nil
	// Mark as closed.
	mb.closed = true
}

func (fs *fileStore) closeAllMsgBlocks(sync bool) {
	for _, mb := range fs.blks {
		mb.close(sync)
	}
}

func (fs *fileStore) Delete(inline bool) error {
	if fs.isClosed() {
		// Always attempt to remove since we could have been closed beforehand.
		os.RemoveAll(fs.fcfg.StoreDir)
		// Since we did remove, if we did have anything remaining make sure to
		// call into any storage updates that had been registered.
		fs.mu.Lock()
		cb, msgs, bytes := fs.scb, int64(fs.state.Msgs), int64(fs.state.Bytes)
		// Guard against double accounting if called twice.
		fs.state.Msgs, fs.state.Bytes = 0, 0
		fs.mu.Unlock()
		if msgs > 0 && cb != nil {
			cb(-msgs, -bytes, 0, _EMPTY_)
		}
		return ErrStoreClosed
	}

	pdir := filepath.Join(fs.fcfg.StoreDir, purgeDir)
	if _, err := os.Stat(pdir); err == nil {
		_ = os.RemoveAll(pdir)
	}

	// Quickly close all blocks and simulate a purge w/o overhead an new write block.
	fs.mu.Lock()
	for _, mb := range fs.blks {
		mb.dirtyClose()
	}
	dmsgs := fs.state.Msgs
	dbytes := int64(fs.state.Bytes)
	fs.state.Msgs, fs.state.Bytes = 0, 0
	fs.blks = nil
	cb := fs.scb
	fs.mu.Unlock()

	if cb != nil {
		cb(-int64(dmsgs), -dbytes, 0, _EMPTY_)
	}

	if err := fs.stop(true, false); err != nil {
		return err
	}

	// Make sure we will not try to recover if killed before removal below completes.
	if err := os.Remove(filepath.Join(fs.fcfg.StoreDir, JetStreamMetaFile)); err != nil {
		return err
	}
	// Now move into different directory with "." prefix.
	ndir := filepath.Join(filepath.Dir(fs.fcfg.StoreDir), tsep+filepath.Base(fs.fcfg.StoreDir))
	if err := os.Rename(fs.fcfg.StoreDir, ndir); err != nil {
		return err
	}
	// Do this in separate Go routine in case lots of blocks.
	// Purge above protects us as does the removal of meta artifacts above.
	if inline {
		if err := removeAllWithRetry(fs.dios, ndir); err != nil {
			return err
		}
	} else {
		go removeAllWithRetry(fs.dios, ndir)
	}
	return nil
}

// Lock should be held.
func (fs *fileStore) setSyncTimer() {
	if fs.syncTmr != nil {
		fs.syncTmr.Reset(fs.fcfg.SyncInterval)
	} else {
		// First time this fires will be between SyncInterval/2 and SyncInterval,
		// so that different stores are spread out, rather than having many of
		// them trying to all sync at once, causing blips and contending dios.
		start := (fs.fcfg.SyncInterval / 2) + (time.Duration(mrand.Int64N(int64(fs.fcfg.SyncInterval / 2))))
		fs.syncTmr = time.AfterFunc(start, fs.syncBlocks)
	}
}

// Lock should be held.
func (fs *fileStore) cancelSyncTimer() {
	if fs.syncTmr != nil {
		fs.syncTmr.Stop()
		fs.syncTmr = nil
	}
}

// The full state file is versioned.
// - 0x1: original binary index.db format
// - 0x2: adds support for TTL count field after num deleted
const (
	fullStateMagic      = uint8(11)
	fullStateMinVersion = uint8(1) // What is the minimum version we know how to parse?
	fullStateVersion    = uint8(4) // What is the current version written out to index.db?
)

// This go routine periodically writes out our full stream state index.
func (fs *fileStore) flushStreamStateLoop(qch, done chan struct{}) {
	// Signal we are done on exit.
	defer close(done)

	// Make sure we do not try to write these out too fast.
	// Spread these out for large numbers on a server restart.
	const writeThreshold = 2 * time.Minute
	writeJitter := time.Duration(mrand.Int64N(int64(30 * time.Second)))
	t := time.NewTicker(writeThreshold + writeJitter)
	defer t.Stop()

	for {
		select {
		case <-t.C:
			err := fs.writeFullState()
			if isPermissionError(err) && fs.srv != nil {
				fs.warn("File system permission denied when flushing stream state, disabling JetStream: %v", err)
				// messages in block cache could be lost in the worst case.
				// In the clustered mode it is very highly unlikely as a result of replication.
				fs.srv.ShutdownJetStream()
				return
			}

		case <-qch:
			return
		}
	}
}

// Helper since unixnano of zero time undefined.
func timestampNormalized(t time.Time) int64 {
	if t.IsZero() {
		return 0
	}
	return t.UnixNano()
}

// writeFullState will proceed to write the full meta state iff not complex and time consuming.
// Since this is for quick recovery it is optional and should not block/stall normal operations.
func (fs *fileStore) writeFullState() error {
	return fs._writeFullState(false)
}

// forceWriteFullState will proceed to write the full meta state.
func (fs *fileStore) forceWriteFullState() error {
	return fs._writeFullState(true)
}

// This will write the full binary state for the stream.
// This plus everything new since last hash will be the total recovered state.
// This state dump will have the following.
// 1. Stream summary - Msgs, Bytes, First and Last (Sequence and Timestamp)
// 2. PSIM - Per Subject Index Map - Tracks first and last blocks with subjects present.
// 3. MBs - Index, Bytes, First and Last Sequence and Timestamps, and the deleted map (avl.seqset).
// 4. Last block index and hash of record inclusive to this stream state.
func (fs *fileStore) _writeFullState(force bool) error {
	if fs.isClosed() {
		return nil
	}

	// If we aren't forcing an update then only queue this up if we aren't already
	// running. This means we can keep waiting on shutdown if needed but not build up
	// lots of waiting goroutines in a bad timer case.
	if fs.wfsrun.Add(1) > 1 && !force {
		fs.wfsrun.Add(-1)
		return nil
	}
	defer fs.wfsrun.Add(-1)

	// Only allow one _writeFullState to take place at a time, otherwise we can
	// have multiple goroutines trying to write the same file after we've released
	// the store lock.
	fs.wfsmu.Lock()
	defer fs.wfsmu.Unlock()

	start := time.Now()
	fs.mu.RLock()
	if fs.dirty == 0 {
		fs.mu.RUnlock()
		return nil
	}

	// Configure encryption if needed.
	if fs.prf != nil {
		// Re-acquire temporarily as write lock to set up AEK.
		fs.mu.RUnlock()
		fs.mu.Lock()
		err := fs.setupAEK()
		fs.mu.Unlock()
		if err != nil {
			return err
		}
		fs.mu.RLock()
	}

	// For calculating size and checking time costs for non forced calls.
	numSubjects := fs.numSubjects()

	// If we are not being forced to write out our state, check the complexity for time costs as to not
	// block or stall normal operations.
	// We will base off of number of subjects and interior deletes. A very large number of msg blocks could also
	// be used, but for next server version will redo all meta handling to be disk based. So this is temporary.
	if !force {
		// Calculate interior deletes.
		var numDeleted int
		if fs.state.LastSeq > fs.state.FirstSeq {
			numDeleted = int((fs.state.LastSeq - fs.state.FirstSeq + 1) - fs.state.Msgs)
		}
		if numSubjects > highCardinalityThreshold || numDeleted > highCardinalityThreshold {
			fs.mu.RUnlock()
			return errStateTooBig
		}
	}

	// We track this through subsequent runs to get an avg per blk used for subsequent runs.
	avgDmapLen := fs.wfsadml
	// If first time through could be 0
	if avgDmapLen == 0 && ((fs.state.LastSeq-fs.state.FirstSeq+1)-fs.state.Msgs) > 0 {
		avgDmapLen = 1024
	}

	// Calculate and estimate of the uper bound on the  size to avoid multiple allocations.
	sz := hdrLen + // Magic and Version
		(binary.MaxVarintLen64 * 6) + // FS data
		binary.MaxVarintLen64 + fs.tsl + // NumSubjects + total subject length
		numSubjects*(binary.MaxVarintLen64*4) + // psi record
		binary.MaxVarintLen64 + // Num blocks.
		len(fs.blks)*((binary.MaxVarintLen64*8)+avgDmapLen) + // msg blocks, avgDmapLen is est for dmaps
		binary.MaxVarintLen64 + 8 + 8 // last index + record checksum + full state checksum

	// Do 4k on stack if possible.
	const ssz = 4 * 1024
	var buf []byte

	if sz <= ssz {
		var _buf [ssz]byte
		buf, sz = _buf[0:hdrLen:ssz], ssz
	} else {
		buf = make([]byte, hdrLen, sz)
	}

	buf[0], buf[1] = fullStateMagic, fullStateVersion
	buf = binary.AppendUvarint(buf, fs.state.Msgs)
	buf = binary.AppendUvarint(buf, fs.state.Bytes)
	buf = binary.AppendUvarint(buf, fs.state.FirstSeq)
	buf = binary.AppendVarint(buf, timestampNormalized(fs.state.FirstTime))
	buf = binary.AppendUvarint(buf, fs.state.LastSeq)
	buf = binary.AppendVarint(buf, timestampNormalized(fs.state.LastTime))

	// Do per subject information map if applicable.
	buf = binary.AppendUvarint(buf, uint64(numSubjects))
	if numSubjects > 0 {
		fs.psim.Match([]byte(fwcs), func(subj []byte, psi *psi) {
			buf = binary.AppendUvarint(buf, uint64(len(subj)))
			buf = append(buf, subj...)
			buf = binary.AppendUvarint(buf, psi.total)
			buf = binary.AppendUvarint(buf, uint64(psi.fblk))
			buf = binary.AppendUvarint(buf, uint64(psi.lblk))
		})
	}

	// Now walk all blocks and write out first and last and optional dmap encoding.
	var lbi uint32
	var lchk [8]byte

	nb := len(fs.blks)
	buf = binary.AppendUvarint(buf, uint64(nb))

	// Use basetime to save some space.
	baseTime := timestampNormalized(fs.state.FirstTime)
	var scratch [8 * 1024]byte

	// Track the state as represented by the mbs.
	var mstate StreamState

	var dmapTotalLen int
	for _, mb := range fs.blks {
		mb.mu.RLock()
		buf = binary.AppendUvarint(buf, uint64(mb.index))
		buf = binary.AppendUvarint(buf, mb.bytes)
		buf = binary.AppendUvarint(buf, atomic.LoadUint64(&mb.first.seq))
		buf = binary.AppendVarint(buf, mb.first.ts-baseTime)
		buf = binary.AppendUvarint(buf, atomic.LoadUint64(&mb.last.seq))
		buf = binary.AppendVarint(buf, mb.last.ts-baseTime)

		numDeleted := mb.dmap.Size()
		buf = binary.AppendUvarint(buf, uint64(numDeleted))
		buf = binary.AppendUvarint(buf, mb.ttls)      // Field is new in version 2
		buf = binary.AppendUvarint(buf, mb.schedules) // Field is new in version 3
		if numDeleted > 0 {
			dmap := mb.dmap.Encode(scratch[:0])
			dmapTotalLen += len(dmap)
			buf = append(buf, dmap...)
		}
		// If this is the last one grab the last checksum and the block index, e.g. 22.blk, 22 is the block index.
		// We use this to quickly open this file on recovery.
		if mb == fs.lmb {
			lbi = mb.index
			mb.ensureLastChecksumLoaded()
			copy(lchk[0:], mb.lchk[:])
		}
		updateTrackingState(&mstate, mb)
		mb.mu.RUnlock()
	}
	if dmapTotalLen > 0 {
		fs.wfsadml = dmapTotalLen / len(fs.blks)
	}

	// Place block index and hash onto the end.
	buf = binary.AppendUvarint(buf, uint64(lbi))
	buf = append(buf, lchk[:]...)

	// Encrypt if needed.
	if fs.prf != nil {
		nonce := make([]byte, fs.aek.NonceSize(), fs.aek.NonceSize()+len(buf)+fs.aek.Overhead())
		if n, err := rand.Read(nonce); err != nil {
			fs.mu.RUnlock()
			return err
		} else if n != len(nonce) {
			fs.mu.RUnlock()
			return fmt.Errorf("not enough nonce bytes read (%d != %d)", n, len(nonce))
		}
		buf = fs.aek.Seal(nonce, nonce, buf, nil)
	}

	fn := filepath.Join(fs.fcfg.StoreDir, msgDir, streamStreamStateFile)

	// Need to have our own hasher here, as under a read lock we can't mutate the
	// fs.hh safely.
	key := sha256.Sum256([]byte(fs.cfg.Name))
	hh, _ := highwayhash.NewDigest64(key[:])
	hh.Write(buf)
	buf = hh.Sum(buf)

	// Snapshot prior dirty count.
	priorDirty := fs.dirty

	statesEqual := trackingStatesEqual(&fs.state, &mstate)
	// Release lock.
	fs.mu.RUnlock()

	// Check consistency here.
	if !statesEqual {
		fs.warn("Stream state encountered internal inconsistency on write")
		// Rebuild our fs state from the mb state.
		fs.rebuildState(nil)
		return errCorruptState
	}

	if cap(buf) > sz {
		fs.debug("WriteFullState reallocated from %d to %d", sz, cap(buf))
	}

	// Only warn about construction time since file write not holding any locks.
	if took := time.Since(start); took > time.Minute {
		fs.warn("WriteFullState took %v (%d bytes)", took.Round(time.Millisecond), len(buf))
	}

	// Write our update index.db
	// Protect with dios.
	fs.dios.acquire()
	err := os.WriteFile(fn, buf, defaultFilePerms)
	// if file system is not writable isPermissionError is set to true
	fs.dios.release()
	if err != nil {
		return err
	}

	// Update dirty if successful.
	fs.mu.Lock()
	fs.dirty -= priorDirty
	fs.mu.Unlock()

	// Attempt to write other index files, an error in one should not prevent the other from being written.
	ttlErr := fs.writeTTLState()
	schedErr := fs.writeMsgSchedulingState()
	sourcesErr := fs.writeSourcesState()
	if ttlErr != nil {
		return ttlErr
	} else if schedErr != nil {
		return schedErr
	} else if sourcesErr != nil {
		return sourcesErr
	}
	return nil
}

func (fs *fileStore) writeTTLState() error {
	fs.mu.RLock()
	if fs.ttls == nil {
		fs.mu.RUnlock()
		return nil
	}
	fn := filepath.Join(fs.fcfg.StoreDir, msgDir, ttlStreamStateFile)
	// Must be lseq+1 to identify up to which sequence the TTLs are valid.
	buf := fs.ttls.Encode(fs.state.LastSeq + 1)
	fs.mu.RUnlock()

	return fs.writeFileWithOptionalSync(fn, buf, defaultFilePerms)
}

func (fs *fileStore) writeMsgSchedulingState() error {
	fs.mu.RLock()
	if fs.scheduling == nil {
		fs.mu.RUnlock()
		return nil
	}
	fn := filepath.Join(fs.fcfg.StoreDir, msgDir, msgSchedulingStreamStateFile)
	// Must be lseq+1 to identify up to which sequence the schedules are valid.
	buf := fs.scheduling.encode(fs.state.LastSeq + 1)
	fs.mu.RUnlock()

	return fs.writeFileWithOptionalSync(fn, buf, defaultFilePerms)
}

const sourcesHeaderLen = 17 // 1 byte magic + 2x uint64s

func (fs *fileStore) sourcesStatePath() string {
	return filepath.Join(fs.fcfg.StoreDir, sourcesStreamStateFile)
}

func (fs *fileStore) writeSourcesState() error {
	fs.mu.RLock()
	if len(fs.sources) == 0 {
		fs.mu.RUnlock()
		return nil
	}
	fn := fs.sourcesStatePath()

	// Must be lseq+1 to identify up to which sequence the sources are valid.
	highSeq := fs.state.LastSeq + 1

	count := uint64(len(fs.sources))
	var sz uint64
	for source, ss := range fs.sources {
		// Length prefix + source bytes + seq varint +
		// length prefix + identity bytes.
		sz += uint64(uvarintLen(uint64(len(source)))) + uint64(len(source)) + binary.MaxVarintLen64 +
			uint64(uvarintLen(uint64(len(ss.Ident)))) + uint64(len(ss.Ident))
	}
	buf := make([]byte, 0, sourcesHeaderLen+sz)
	buf = append(buf, 1)                                 // Magic version
	buf = binary.LittleEndian.AppendUint64(buf, count)   // Entry count
	buf = binary.LittleEndian.AppendUint64(buf, highSeq) // Stamp
	for source, ss := range fs.sources {
		buf = binary.AppendUvarint(buf, uint64(len(source)))
		buf = append(buf, source...)
		buf = binary.AppendUvarint(buf, ss.Seq)
		buf = binary.AppendUvarint(buf, uint64(len(ss.Ident)))
		buf = append(buf, ss.Ident...)
	}
	fs.mu.RUnlock()

	return fs.writeFileWithOptionalSync(fn, buf, defaultFilePerms)
}

// Lock should be held.
func (fs *fileStore) recoverSourcesState() uint64 {
	// See if we have a sources index file.
	fs.dios.acquire()
	fn := fs.sourcesStatePath()
	buf, err := os.ReadFile(fn)
	fs.dios.release()

	if err != nil && !os.IsNotExist(err) {
		fs.warn("Recovering sources state from index errored: %v", err)
	}

	var sourcesSeq uint64
	if err == nil {
		sourcesSeq, err = fs.decodeSourcesState(buf)
		if err != nil {
			fs.warn("Error decoding sources state: %s", err)
			// Remove the file, and reset collected state (if any).
			_ = os.Remove(fn)
			fs.sources = nil
		}
	}

	if sourcesSeq < fs.state.FirstSeq {
		sourcesSeq = fs.state.FirstSeq
	}
	return sourcesSeq
}

var errSourcesInvalidVersion = errors.New("sources: encoded version not known")

func (fs *fileStore) decodeSourcesState(b []byte) (uint64, error) {
	if len(b) < sourcesHeaderLen {
		return 0, io.ErrShortBuffer
	}
	if b[0] != 1 {
		return 0, errSourcesInvalidVersion
	}

	count := binary.LittleEndian.Uint64(b[1:])
	stamp := binary.LittleEndian.Uint64(b[9:])
	if count > 0 && fs.sources == nil {
		fs.sources = make(map[string]*StreamSourceState)
	}
	b = b[sourcesHeaderLen:]
	readStr := func() (string, bool) {
		n, vn := binary.Uvarint(b)
		if vn <= 0 || uint64(len(b)-vn) < n {
			return _EMPTY_, false
		}
		b = b[vn:]
		s := string(b[:n])
		b = b[n:]
		return s, true
	}
	for i := uint64(0); i < count; i++ {
		source, ok := readStr()
		if !ok {
			return 0, io.ErrUnexpectedEOF
		}
		seq, vn := binary.Uvarint(b)
		if vn <= 0 {
			return 0, io.ErrUnexpectedEOF
		}
		b = b[vn:]
		ident, ok := readStr()
		if !ok {
			return 0, io.ErrUnexpectedEOF
		}
		fs.sources[source] = &StreamSourceState{Seq: seq, Ident: ident}
	}
	return stamp, nil
}

// recoverSourcesBackwardScan recovers fs.sources by scanning the stream backwards.
// Lock should be held.
func (fs *fileStore) recoverSourcesBackwardScan(stopSeq uint64, sources map[string]*StreamSource) error {
	var sl *gsl.SimpleSublist
	refreshSublist := func() {
		sl = gsl.NewSimpleSublist()
		for _, src := range sources {
			if src.FilterSubject == _EMPTY_ {
				sl.Insert(fwcs, struct{}{})
			} else {
				sl.Insert(src.FilterSubject, struct{}{})
			}
			for _, tr := range src.SubjectTransforms {
				if tr.Destination == _EMPTY_ {
					sl.Insert(fwcs, struct{}{})
				} else {
					sl.Insert(tr.Destination, struct{}{})
				}
			}
		}
	}
	refreshSublist()

	update := func(iName string, sseq uint64, ident string) {
		// Only consider sources that are configured and not yet found. The backward
		// scan visits the highest matching sequence first, so the first hit per source
		// is the latest and overwrites any older decoded value.
		if _, ok := sources[iName]; !ok {
			return
		}
		if fs.sources == nil {
			fs.sources = make(map[string]*StreamSourceState, 1)
		}
		fs.sources[iName] = &StreamSourceState{Seq: sseq, Ident: ident}
		delete(sources, iName)
		refreshSublist()
	}

	var smv StoreMsg
	for last := fs.state.LastSeq; ; {
		sm, seq, err := fs.loadPrevMsgMultiLocked(sl, last, &smv)
		if err == ErrStoreEOF {
			// Done.
			break
		}
		if err != nil {
			return err
		}
		// Stop if we've gone below the point where decoded state is still valid.
		if seq < stopSeq {
			break
		}
		if len(sm.hdr) > 0 {
			if ss := sliceHeader(JSStreamSource, sm.hdr); len(ss) > 0 {
				streamName, iName, sseq, ident := streamAndSeq(bytesToString(ss))
				if iName == _EMPTY_ {
					// Pre-2.10 message header means it's a match for any source using that stream name.
					// Such headers carry no identity, so it is left unset.
					for _, ssi := range fs.cfg.Sources {
						if streamName == ssi.Name || (ssi.External != nil && streamName == ssi.Name+":"+getHash(ssi.External.ApiPrefix)) {
							update(ssi.composeIName(), sseq, _EMPTY_)
						}
					}
				} else {
					update(iName, sseq, ident)
				}
			}
		}
		// Done once every source has been resolved, or we've reached the floor.
		if len(sources) == 0 || seq <= stopSeq {
			break
		}
		last = seq - 1
	}
	return nil
}

// Stop the current filestore.
func (fs *fileStore) Stop() error {
	return fs.stop(false, true)
}

// Stop the current filestore.
func (fs *fileStore) stop(delete, writeState bool) error {
	if fs.isClosed() {
		return ErrStoreClosed
	}

	fs.mu.Lock()
	if fs.closing {
		fs.mu.Unlock()
		return ErrStoreClosed
	}

	// Mark as closing. Do before releasing the lock to wait on the state flush loop
	// so we don't end up with this function running more than once.
	fs.closing = true

	// Release the state flusher loop.
	if fs.qch != nil {
		close(fs.qch)
		fs.qch = nil
	}

	if writeState {
		// Wait for the state flush loop to exit.
		fsld := fs.fsld
		fs.mu.Unlock()
		<-fsld
		fs.mu.Lock()

		fs.checkAndFlushLastBlock()
	}
	fs.closeAllMsgBlocks(false)

	fs.cancelSyncTimer()
	fs.cancelAgeChk()

	if writeState {
		// Write full state if needed. If not dirty this is a no-op.
		fs.mu.Unlock()
		fs.forceWriteFullState()
		fs.mu.Lock()
	}

	// Mark as closed. Last message block needs to be cleared after
	// writeFullState has completed.
	fs.closed.Store(true)
	fs.lmb = nil

	// We should update the upper usage layer on a stop.
	cb, bytes := fs.scb, int64(fs.state.Bytes)
	fs.mu.Unlock()

	// Wait for any in-flight syncBlocks to finish now that we are marked
	// closed. This ensures a Delete can't remove or rename directories from
	// underneath a running sync. Any sync starting after this sees closed and
	// returns immediately. Must not hold fs.mu here, syncBlocks acquires
	// syncMu before fs.mu.
	fs.syncMu.Lock()
	fs.syncMu.Unlock() //nolint:staticcheck // Only waiting for in-flight syncBlocks.

	fs.cmu.Lock()
	var _cfs [256]ConsumerStore
	cfs := append(_cfs[:0], fs.cfs...)
	fs.cfs = nil
	fs.cmu.Unlock()

	for _, o := range cfs {
		if delete {
			o.StreamDelete()
		} else {
			o.Stop()
		}
	}

	if bytes > 0 && cb != nil {
		cb(0, -bytes, 0, _EMPTY_)
	}

	// Unregister from the access time service.
	ats.Unregister()

	fs.debug("Stopped")
	return nil
}

const errFile = "errors.txt"

// Stream our snapshot through S2 compression and tar.
func (fs *fileStore) streamSnapshot(w io.WriteCloser, includeConsumers bool, errCh chan error) {
	defer close(errCh)
	defer w.Close()

	enc := s2.NewWriter(w)
	defer enc.Close()

	tw := tar.NewWriter(enc)
	defer tw.Close()

	defer func() {
		fs.mu.Lock()
		fs.sips--
		fs.mu.Unlock()
	}()

	modTime := time.Now().UTC()

	writeFile := func(name string, buf []byte) error {
		hdr := &tar.Header{
			Name:    name,
			Mode:    0600,
			ModTime: modTime,
			Uname:   "nats",
			Gname:   "nats",
			Size:    int64(len(buf)),
			Format:  tar.FormatPAX,
		}
		if err := tw.WriteHeader(hdr); err != nil {
			return err
		}
		if _, err := tw.Write(buf); err != nil {
			return err
		}
		return nil
	}

	writeErr := func(err error) {
		writeFile(errFile, []byte(err.Error()))
		errCh <- err
	}

	fs.mu.Lock()
	blks := fs.blks
	// Grab our general meta data.
	// We do this now instead of pulling from files since they could be encrypted.
	meta, err := json.Marshal(fs.cfg)
	if err != nil {
		fs.mu.Unlock()
		writeErr(fmt.Errorf("could not gather stream meta file: %w", err))
		return
	}
	hh := fs.hh
	hh.Reset()
	hh.Write(meta)
	var hb [highwayhash.Size64]byte
	sum := []byte(hex.EncodeToString(fs.hh.Sum(hb[:0])))
	fs.mu.Unlock()

	// Meta first.
	if writeFile(JetStreamMetaFile, meta) != nil {
		return
	}
	if writeFile(JetStreamMetaFileSum, sum) != nil {
		return
	}

	// Can't use join path here, tar only recognizes relative paths with forward slashes.
	msgPre := msgDir + "/"
	var bbuf []byte

	// Now do messages themselves.
	for _, mb := range blks {
		if mb.pendingWriteSize() > 0 {
			mb.flushPendingMsgs()
		}
		mb.mu.Lock()
		// We could stream but don't want to hold the lock and prevent changes, so just read in and
		// release the lock for now.
		bbuf, err = mb.loadBlock(bbuf)
		if err != nil {
			mb.mu.Unlock()
			writeErr(fmt.Errorf("could not read message block [%d]: %w", mb.index, err))
			return
		}
		// Check for encryption.
		if mb.bek != nil && len(bbuf) > 0 {
			rbek, err := genBlockEncryptionKey(fs.fcfg.Cipher, mb.seed, mb.nonce)
			if err != nil {
				mb.mu.Unlock()
				writeErr(fmt.Errorf("could not create encryption key for message block [%d]: %w", mb.index, err))
				return
			}
			rbek.XORKeyStream(bbuf, bbuf)
		}
		// Check for compression.
		if bbuf, err = mb.decompressIfNeeded(bbuf); err != nil {
			mb.mu.Unlock()
			writeErr(fmt.Errorf("could not decompress message block [%d]: %w", mb.index, err))
			return
		}
		mb.mu.Unlock()

		// Do this one unlocked.
		if writeFile(msgPre+fmt.Sprintf(blkScan, mb.index), bbuf) != nil {
			return
		}
	}

	// Do index.db last. We will force a write as well.
	// Write out full state as well before proceeding.
	if err := fs.forceWriteFullState(); err == nil {
		const minLen = 32
		sfn := filepath.Join(fs.fcfg.StoreDir, msgDir, streamStreamStateFile)
		if buf, err := os.ReadFile(sfn); err == nil && len(buf) >= minLen {
			fs.mu.Lock()
			if fs.aek != nil {
				ns := fs.aek.NonceSize()
				buf, err = fs.aek.Open(nil, buf[:ns], buf[ns:len(buf)-highwayhash.Size64], nil)
				if err == nil {
					// Redo hash checksum at end on plaintext.
					hh.Reset()
					hh.Write(buf)
					buf = fs.hh.Sum(buf)
				}
			}
			fs.mu.Unlock()
			if err == nil && writeFile(msgPre+streamStreamStateFile, buf) != nil {
				return
			}
		}
	}

	// Bail if no consumers requested.
	if !includeConsumers {
		return
	}

	// Do consumers' state last.
	fs.cmu.RLock()
	cfs := fs.cfs
	fs.cmu.RUnlock()

	for _, cs := range cfs {
		o, ok := cs.(*consumerFileStore)
		if !ok {
			continue
		}
		o.mu.Lock()
		// Grab our general meta data.
		// We do this now instead of pulling from files since they could be encrypted.
		meta, err := json.Marshal(o.cfg)
		if err != nil {
			o.mu.Unlock()
			writeErr(fmt.Errorf("could not gather consumer meta file for %q: %w", o.name, err))
			return
		}
		o.hh.Reset()
		o.hh.Write(meta)
		var hb [highwayhash.Size64]byte
		sum := []byte(hex.EncodeToString(o.hh.Sum(hb[:0])))

		// We can have the running state directly encoded now.
		state, err := o.encodeState()
		if err != nil {
			o.mu.Unlock()
			writeErr(fmt.Errorf("could not encode consumer state for %q: %w", o.name, err))
			return
		}
		odirPre := filepath.Join(consumerDir, o.name)
		o.mu.Unlock()

		// Write all the consumer files.
		if writeFile(filepath.Join(odirPre, JetStreamMetaFile), meta) != nil {
			return
		}
		if writeFile(filepath.Join(odirPre, JetStreamMetaFileSum), sum) != nil {
			return
		}
		writeFile(filepath.Join(odirPre, consumerState), state)
	}
}

// Create a snapshot of this stream and its consumer's state along with messages.
func (fs *fileStore) Snapshot(deadline time.Duration, checkMsgs, includeConsumers bool) (*SnapshotResult, error) {
	if fs.isClosed() {
		return nil, ErrStoreClosed
	}

	fs.mu.Lock()

	// Only allow one at a time.
	if fs.sips > 0 {
		fs.mu.Unlock()
		return nil, ErrStoreSnapshotInProgress
	}
	// Mark us as snapshotting
	fs.sips += 1
	fs.mu.Unlock()

	if checkMsgs {
		ld, err := fs.checkMsgs()
		clearSips := func() {
			fs.mu.Lock()
			fs.sips--
			fs.mu.Unlock()
		}
		if err != nil {
			clearSips()
			return nil, fmt.Errorf("snapshot check failed: %w", err)
		}
		if ld != nil && len(ld.Msgs) > 0 {
			clearSips()
			return nil, fmt.Errorf("snapshot check detected %d bad messages", len(ld.Msgs))
		}
	}

	pr, pw := net.Pipe()

	// Set a write deadline here to protect ourselves.
	if deadline > 0 {
		pw.SetWriteDeadline(time.Now().Add(deadline))
	}

	// We can add to our stream while snapshotting but not "user" delete anything.
	var state StreamState
	fs.FastState(&state)

	// Stream in separate Go routine.
	errCh := make(chan error, 1)
	go fs.streamSnapshot(pw, includeConsumers, errCh)

	return &SnapshotResult{pr, state, errCh}, nil
}

// Helper to return the config.
func (fs *fileStore) fileStoreConfig() FileStoreConfig {
	fs.mu.RLock()
	defer fs.mu.RUnlock()
	return fs.fcfg
}

// Read lock all existing message blocks.
// Lock held on entry.
func (fs *fileStore) readLockAllMsgBlocks() {
	for _, mb := range fs.blks {
		mb.mu.RLock()
	}
}

// Read unlock all existing message blocks.
// Lock held on entry.
func (fs *fileStore) readUnlockAllMsgBlocks() {
	for _, mb := range fs.blks {
		mb.mu.RUnlock()
	}
}

// Binary encoded state snapshot, >= v2.10 server.
func (fs *fileStore) EncodedStreamState(failed uint64, withSources bool) ([]byte, error) {
	fs.mu.RLock()
	defer fs.mu.RUnlock()

	withSources = withSources && len(fs.sources) > 0
	version := streamStateVersion

	// Calculate deleted.
	var numDeleted int64
	if fs.state.LastSeq > fs.state.FirstSeq {
		numDeleted = int64(fs.state.LastSeq-fs.state.FirstSeq+1) - int64(fs.state.Msgs)
		if numDeleted < 0 {
			numDeleted = 0
		}
	}

	// Encoded is Msgs, Bytes, FirstSeq, LastSeq, Failed, NumDeleted and optional DeletedBlocks.
	// Calculate the exact encoded size up front so the buffer is allocated once.
	total := hdrLen + uvarintLen(fs.state.Msgs) + uvarintLen(fs.state.Bytes) +
		uvarintLen(fs.state.FirstSeq) + uvarintLen(fs.state.LastSeq) +
		uvarintLen(failed) + uvarintLen(uint64(numDeleted))

	var numSources uint64
	var sourcesLen int
	if withSources {
		for name, ss := range fs.sources {
			if ss.Seq == 0 {
				continue
			}
			numSources++
			// Length prefix + source bytes + seq varint +
			// length prefix + identity bytes.
			sourcesLen += uvarintLen(uint64(len(name))) + len(name) + uvarintLen(ss.Seq) +
				uvarintLen(uint64(len(ss.Ident))) + len(ss.Ident)
		}
		// Nothing observed yet, then there is nothing to carry.
		if withSources = numSources > 0; withSources {
			total += uvarintLen(numSources) + sourcesLen
			version = streamStateVersionSources
		}
	}

	var dbs DeleteBlocks
	if numDeleted > 0 {
		fs.readLockAllMsgBlocks()
		defer fs.readUnlockAllMsgBlocks()
		var sz int
		dbs, sz = fs.deleteBlocks()
		total += sz
	}

	b := make([]byte, 0, total)
	b = append(b, streamStateMagic, version)
	b = binary.AppendUvarint(b, fs.state.Msgs)
	b = binary.AppendUvarint(b, fs.state.Bytes)
	b = binary.AppendUvarint(b, fs.state.FirstSeq)
	b = binary.AppendUvarint(b, fs.state.LastSeq)
	b = binary.AppendUvarint(b, failed)
	if withSources {
		b = binary.AppendUvarint(b, numSources)
		for name, ss := range fs.sources {
			if ss.Seq == 0 {
				continue
			}
			b = binary.AppendUvarint(b, uint64(len(name)))
			b = append(b, name...)
			b = binary.AppendUvarint(b, ss.Seq)
			b = binary.AppendUvarint(b, uint64(len(ss.Ident)))
			b = append(b, ss.Ident...)
		}
	}
	b = binary.AppendUvarint(b, uint64(numDeleted))

	for _, db := range dbs {
		switch db := db.(type) {
		case *DeleteRange:
			b = appendRunLength(b, db.First, db.Num)
		case *avl.SequenceSet:
			enc := db.Encode(b[len(b):])
			if n := len(b) + len(enc); n <= cap(b) {
				b = b[:n]
			} else {
				// Fallback if the buffer didn't have spare capacity.
				b = append(b, enc...)
			}
		default:
			return nil, errors.New("no impl")
		}
	}

	if len(b) != total {
		assert.Unreachable("Filestore EncodedStreamState size accounting mismatch", map[string]any{
			"name":   fs.cfg.Name,
			"total":  total,
			"length": len(b),
		})
	}
	return b, nil
}

// deleteBlocks returns DeleteBlocks representing interior deletes
// and gaps between blocks, as well as their total binary encoded size.
// All blocks should be at least read locked.
func (fs *fileStore) deleteBlocks() (dbs DeleteBlocks, sz int) {
	var prevLast uint64
	var prevRange *DeleteRange
	var msgsSinceGap bool

	for _, mb := range fs.blks {
		// Detect if we have a gap between these blocks.
		fseq := atomic.LoadUint64(&mb.first.seq)
		if prevLast > 0 && prevLast+1 != fseq {
			gapSize := fseq - prevLast - 1
			// The previous DeleteRange can be extended
			// to include this gap, if there are no
			// blocks containing messages between the
			// two gaps.
			if prevRange != nil && !msgsSinceGap {
				sz -= runLengthEncodeLen(prevRange.First, prevRange.Num)
				prevRange.Num += gapSize
				sz += runLengthEncodeLen(prevRange.First, prevRange.Num)
			} else {
				prevRange = &DeleteRange{
					First: prevLast + 1,
					Num:   gapSize,
				}
				sz += runLengthEncodeLen(prevRange.First, prevRange.Num)
				msgsSinceGap = false
				dbs = append(dbs, prevRange)
			}
		}
		if mb.dmap.Size() > 0 {
			dbs = append(dbs, &mb.dmap)
			sz += mb.dmap.EncodeLen()
			prevRange = nil
		}
		prevLast = atomic.LoadUint64(&mb.last.seq)
		msgsSinceGap = msgsSinceGap || mb.msgs > 0
	}
	return dbs, sz
}

// interiorDeletes is a point-in-time view of the interior deletes tracked by
// the live message blocks, held as per-block clones of each mb.dmap. Blocks
// own disjoint ascending sequence ranges, so a lookup binary searches for the
// owning clone. Reads require no locks.
type interiorDeletes struct {
	sets []*avl.SequenceSet // Per-block dmap clones, ascending disjoint ranges.
	maxs []uint64           // Last sequence of the block owning each clone.
	last int                // Clone index of the previous lookup.
}

// Exists returns whether the sequence was marked as an interior delete by a
// live block at the time the view was built.
// Not safe for concurrent use.
func (v *interiorDeletes) Exists(seq uint64) bool {
	if v == nil {
		return false
	}
	// Check the clone that answered the previous lookup first, sequences are
	// mostly checked in ascending order and cluster per block.
	if i := v.last; i < len(v.maxs) && seq <= v.maxs[i] && (i == 0 || v.maxs[i-1] < seq) {
		return v.sets[i].Exists(seq)
	}
	// First clone whose max is >= seq is the only one that can contain it.
	i, _ := slices.BinarySearch(v.maxs, seq)
	if i == len(v.sets) {
		return false
	}
	v.last = i
	return v.sets[i].Exists(seq)
}

// deleteMap returns a view of all interior deletes for each of the given blocks,
// based on the mb.dmap. Specifically, this will not contain any deletes for blocks
// that had already been removed. This is useful to know whether a tombstone is
// still relevant and marked as deleted by an active block.
// No locks should be held on entry.
func deleteMap(blks []*msgBlock) *interiorDeletes {
	v := interiorDeletes{
		sets: make([]*avl.SequenceSet, 0, len(blks)),
		maxs: make([]uint64, 0, len(blks)),
	}
	for _, mb := range blks {
		mb.mu.RLock()
		if !mb.closed && mb.dmap.Size() > 0 {
			// The block's last sequence bounds all of its dmap entries and
			// preserves the ascending disjoint ordering across clones.
			v.sets = append(v.sets, mb.dmap.Clone())
			v.maxs = append(v.maxs, atomic.LoadUint64(&mb.last.seq))
		}
		mb.mu.RUnlock()
	}
	return &v
}

// storeUpdateAgg aggregates storage update deltas, so removing multiple blocks
// fires one storage callback instead of one per block.
// fs lock should be held for all access.
type storeUpdateAgg struct {
	md, bd int64
}

// SyncDeleted will make sure this stream has same deleted state as dbs.
// This will only process deleted state within our current state.
func (fs *fileStore) SyncDeleted(dbs DeleteBlocks) error {
	if len(dbs) == 0 {
		return nil
	}

	fs.mu.Lock()
	defer fs.mu.Unlock()

	if fs.isClosed() {
		return ErrStoreClosed
	}

	// Always return previous write errors.
	if err := fs.werr; err != nil {
		return err
	}

	// Removing a block has all consumers recalculate their pending state, so
	// aggregate those into one callback at the end. Single message removals
	// keep NumPending up to date incrementally, which is cheaper.
	agg := &storeUpdateAgg{}
	defer func() {
		if agg.md == 0 && agg.bd == 0 {
			return
		}
		if cb := fs.scb; cb != nil {
			fs.mu.Unlock()
			cb(agg.md, agg.bd, 0, _EMPTY_)
			fs.mu.Lock()
		}
	}()

	lseq := fs.state.LastSeq
	fs.readLockAllMsgBlocks()
	mdbs, _ := fs.deleteBlocks()
	// We'll release the locks below, so need to copy the ones that are references
	// which are only safe while the locks are still held.
	for i, db := range mdbs {
		if ss, ok := db.(*avl.SequenceSet); ok {
			mdbs[i] = ss.Clone()
		}
	}
	fs.readUnlockAllMsgBlocks()

	for _, db := range dbs {
		first, last, _ := db.State()
		if first > lseq {
			break
		}

		var prune bool
		if prune, mdbs = pruneDeleteBlock(db, mdbs); prune {
			continue
		}

		var err error
		if _, ok := db.(*DeleteRange); ok {
			err = fs.removeMsgsInRange(first, last, true, agg)
		} else {
			db.Range(func(dseq uint64) bool {
				_, err = fs.removeMsg(dseq, false, true, false)
				// Can continue safely if the message doesn't exist.
				if err == ErrStoreEOF || err == ErrStoreMsgNotFound {
					err = nil
				}
				return err == nil
			})
		}
		if err != nil {
			return err
		}
	}
	return nil
}

// pruneDeleteBlock tries to find a delete block in the ordered blocks slice
// that matches db. It skips blocks that are already behind db and returns
// whether the next candidate matches exactly, along with the remaining
// suffix to use for the next comparison.
func pruneDeleteBlock(db DeleteBlock, blocks DeleteBlocks) (bool, DeleteBlocks) {
	if len(blocks) == 0 {
		return false, blocks
	}

	aFirst, aLast, aNum := db.State()
	bFirst, bLast, bNum := blocks[0].State()

	// Drop blocks that end before db starts.
	for bLast < aFirst {
		blocks = blocks[1:]
		if len(blocks) == 0 {
			return false, blocks
		}
		bFirst, bLast, bNum = blocks[0].State()
	}

	if aFirst == bFirst && aLast == bLast && aNum == bNum {
		// Matching state is only conclusive for a dense block; two sparse
		// sequence sets can share the same state but differ in contents.
		if aNum == aLast-aFirst+1 || deleteBlockContentsEqual(db, blocks[0]) {
			return true, blocks[1:]
		}
	}

	return false, blocks
}

// deleteBlockContentsEqual reports whether two sparse delete blocks with
// identical State() contain the same sequences. If neither block is a
// sequence set we can't compare cheaply and safely report unequal.
func deleteBlockContentsEqual(a, b DeleteBlock) bool {
	ssa, aIsSet := a.(*avl.SequenceSet)
	ssb, bIsSet := b.(*avl.SequenceSet)
	if aIsSet && bIsSet {
		return ssa.Equal(ssb)
	}
	// Use whichever side is a SequenceSet for fast Exists lookups.
	var ss *avl.SequenceSet
	var other DeleteBlock
	if bIsSet {
		ss, other = ssb, a
	} else if aIsSet {
		ss, other = ssa, b
	} else {
		return false
	}
	equal := true
	other.Range(func(seq uint64) bool {
		equal = ss.Exists(seq)
		return equal
	})
	return equal
}

////////////////////////////////////////////////////////////////////////////////
// Consumers
////////////////////////////////////////////////////////////////////////////////

type consumerFileStore struct {
	mu      sync.Mutex
	fs      *fileStore
	cfg     *FileConsumerInfo
	prf     keyGen
	aek     cipher.AEAD
	name    string
	odir    string
	ifn     string
	hh      *highwayhash.Digest64
	state   ConsumerState
	fch     chan struct{}
	qch     chan struct{}
	flusher bool
	writing bool
	dirty   bool
	closed  bool
}

func (fs *fileStore) ConsumerStore(name string, created time.Time, cfg *ConsumerConfig) (ConsumerStore, error) {
	if fs == nil {
		return nil, fmt.Errorf("filestore is nil")
	}
	if fs.isClosed() {
		return nil, ErrStoreClosed
	}
	if cfg == nil || name == _EMPTY_ {
		return nil, fmt.Errorf("bad consumer config")
	}

	// We now allow overrides from a stream being a filestore type and forcing a consumer to be memory store.
	if cfg.MemoryStorage {
		// Create directly here.
		o := &consumerMemStore{ms: fs, name: name, cfg: *cfg}
		if err := fs.AddConsumer(o); err != nil {
			return nil, err
		}
		return o, nil
	}

	odir := filepath.Join(fs.fcfg.StoreDir, consumerDir, name)
	if err := os.MkdirAll(odir, defaultDirPerms); err != nil {
		return nil, fmt.Errorf("could not create consumer directory - %v", err)
	}
	csi := &FileConsumerInfo{Name: name, Created: created, ConsumerConfig: *cfg}
	o := &consumerFileStore{
		fs:   fs,
		cfg:  csi,
		prf:  fs.prf,
		name: name,
		odir: odir,
		ifn:  filepath.Join(odir, consumerState),
	}
	key := sha256.Sum256([]byte(fs.cfg.Name + "/" + name))
	hh, err := highwayhash.NewDigest64(key[:])
	if err != nil {
		return nil, fmt.Errorf("could not create hash: %v", err)
	}
	o.hh = hh

	// Check for encryption.
	if o.prf != nil {
		if ekey, err := os.ReadFile(filepath.Join(odir, JetStreamMetaFileKey)); err == nil {
			if len(ekey) < minBlkKeySize {
				return nil, errBadKeySize
			}
			// Recover key encryption key.
			rb, err := fs.prf([]byte(fs.cfg.Name + tsep + o.name))
			if err != nil {
				return nil, err
			}

			sc := fs.fcfg.Cipher
			kek, err := genEncryptionKey(sc, rb)
			if err != nil {
				return nil, err
			}
			ns := kek.NonceSize()
			nonce := ekey[:ns]
			seed, err := kek.Open(nil, nonce, ekey[ns:], nil)
			if err != nil {
				// We may be here on a cipher conversion, so attempt to convert.
				if err = o.convertCipher(); err != nil {
					return nil, err
				}
			} else {
				o.aek, err = genEncryptionKey(sc, seed)
			}
			if err != nil {
				return nil, err
			}
		}
	}

	// Track if we are creating the directory so that we can clean up if we encounter an error.
	var didCreate bool

	// Write our meta data iff does not exist.
	meta := filepath.Join(odir, JetStreamMetaFile)
	if _, err := os.Stat(meta); err != nil && os.IsNotExist(err) {
		didCreate = true
		if err := o.writeConsumerMeta(); err != nil {
			_ = os.RemoveAll(odir)
			return nil, err
		}
	}

	// If we expect to be encrypted check that what we are restoring is not plaintext.
	// This can happen on snapshot restores or conversions.
	if o.prf != nil {
		keyFile := filepath.Join(odir, JetStreamMetaFileKey)
		if _, err := os.Stat(keyFile); err != nil && os.IsNotExist(err) {
			if err := o.writeConsumerMeta(); err != nil {
				if didCreate {
					_ = os.RemoveAll(odir)
				}
				return nil, err
			}
			// Redo the state file as well here if we have one and we can tell it was plaintext.
			if buf, err := os.ReadFile(o.ifn); err == nil {
				if _, err := decodeConsumerState(buf); err == nil {
					state, err := o.encryptState(buf)
					if err != nil {
						return nil, err
					}
					err = fs.writeFileWithOptionalSync(o.ifn, state, defaultFilePerms)
					if err != nil {
						if didCreate {
							_ = os.RemoveAll(odir)
						}
						return nil, err
					}
				}
			}
		}
	}

	// Create channels to control our flush go routine.
	o.fch = make(chan struct{}, 1)
	o.qch = make(chan struct{})

	// Make sure to load in our state from disk if needed.
	if err = o.loadState(); err != nil {
		return nil, err
	}

	// Assign to filestore.
	if err = fs.AddConsumer(o); err != nil {
		return nil, err
	}

	go o.flushLoop(o.fch, o.qch)
	return o, nil
}

func (o *consumerFileStore) convertCipher() error {
	fs := o.fs
	odir := filepath.Join(fs.fcfg.StoreDir, consumerDir, o.name)

	ekey, err := os.ReadFile(filepath.Join(odir, JetStreamMetaFileKey))
	if err != nil {
		return err
	}
	if len(ekey) < minBlkKeySize {
		return errBadKeySize
	}
	// Recover key encryption key.
	rb, err := fs.prf([]byte(fs.cfg.Name + tsep + o.name))
	if err != nil {
		return err
	}

	// Do these in reverse since converting.
	sc := fs.fcfg.Cipher
	osc := AES
	if sc == AES {
		osc = ChaCha
	}
	kek, err := genEncryptionKey(osc, rb)
	if err != nil {
		return err
	}
	ns := kek.NonceSize()
	nonce := ekey[:ns]
	seed, err := kek.Open(nil, nonce, ekey[ns:], nil)
	if err != nil {
		return err
	}
	aek, err := genEncryptionKey(osc, seed)
	if err != nil {
		return err
	}
	// Now read in and decode our state using the old cipher.
	buf, err := os.ReadFile(o.ifn)
	if err != nil {
		return err
	}
	buf, err = aek.Open(nil, buf[:ns], buf[ns:], nil)
	if err != nil {
		return err
	}

	// Since we are here we recovered our old state.
	// Now write our meta, which will generate the new keys with the new cipher.
	if err := o.writeConsumerMeta(); err != nil {
		return err
	}

	// Now write out or state with the new cipher.
	return o.writeState(buf)
}

// Kick flusher for this consumer.
// Lock should be held.
func (o *consumerFileStore) kickFlusher() {
	if o.fch != nil {
		select {
		case o.fch <- struct{}{}:
		default:
		}
	}
	o.dirty = true
}

// Set in flusher status
func (o *consumerFileStore) setInFlusher() {
	o.mu.Lock()
	o.flusher = true
	o.mu.Unlock()
}

// Clear in flusher status
func (o *consumerFileStore) clearInFlusher() {
	o.mu.Lock()
	o.flusher = false
	o.mu.Unlock()
}

// Report in flusher status
func (o *consumerFileStore) inFlusher() bool {
	o.mu.Lock()
	defer o.mu.Unlock()
	return o.flusher
}

// flushLoop watches for consumer updates and the quit channel.
func (o *consumerFileStore) flushLoop(fch, qch chan struct{}) {

	o.setInFlusher()
	defer o.clearInFlusher()

	// Maintain approximately 10 updates per second per consumer under load.
	const minTime = 100 * time.Millisecond
	var lastWrite time.Time
	var dt *time.Timer

	setDelayTimer := func(addWait time.Duration) {
		if dt == nil {
			dt = time.NewTimer(addWait)
			return
		}
		if !dt.Stop() {
			select {
			case <-dt.C:
			default:
			}
		}
		dt.Reset(addWait)
	}

	for {
		select {
		case <-fch:
			if ts := time.Since(lastWrite); ts < minTime {
				setDelayTimer(minTime - ts)
				select {
				case <-dt.C:
				case <-qch:
					return
				}
			}
			o.mu.Lock()
			if o.closed {
				o.mu.Unlock()
				return
			}
			buf, err := o.encodeState()
			o.mu.Unlock()
			if err != nil {
				return
			}
			// TODO(dlc) - if we error should start failing upwards.
			if err := o.writeState(buf); err == nil {
				lastWrite = time.Now()
			}
		case <-qch:
			return
		}
	}
}

// SetStarting sets our starting stream sequence.
func (o *consumerFileStore) SetStarting(sseq uint64) error {
	o.mu.Lock()
	o.state.Delivered.Stream = sseq
	o.state.AckFloor.Stream = sseq
	buf := encodeConsumerState(&o.state)
	o.mu.Unlock()
	return o.writeState(buf)
}

// UpdateStarting updates our starting stream sequence.
func (o *consumerFileStore) UpdateStarting(sseq uint64) {
	o.mu.Lock()
	defer o.mu.Unlock()

	if sseq > o.state.Delivered.Stream {
		o.state.Delivered.Stream = sseq
		// For AckNone just update delivered and ackfloor at the same time.
		if o.cfg.AckPolicy == AckNone {
			o.state.AckFloor.Stream = sseq
		}
	}
	// Make sure we flush to disk.
	o.kickFlusher()
}

// Reset all values in the store, and reset the starting sequence.
func (o *consumerFileStore) Reset(sseq uint64) error {
	o.mu.Lock()
	o.state = ConsumerState{}
	o.mu.Unlock()
	return o.SetStarting(sseq)
}

// HasState returns if this store has a recorded state.
func (o *consumerFileStore) HasState() bool {
	o.mu.Lock()
	// We have a running state, or stored on disk but not yet initialized.
	if o.state.Delivered.Consumer != 0 || o.state.Delivered.Stream != 0 {
		o.mu.Unlock()
		return true
	}
	_, err := os.Stat(o.ifn)
	o.mu.Unlock()
	return err == nil
}

// UpdateDelivered is called whenever a new message has been delivered.
func (o *consumerFileStore) UpdateDelivered(dseq, sseq, dc uint64, ts int64) error {
	o.mu.Lock()
	defer o.mu.Unlock()

	if dc != 1 && o.cfg.AckPolicy == AckNone {
		return ErrNoAckPolicy
	}

	// On restarts the old leader may get a replay from the raft logs that are old.
	if dseq <= o.state.AckFloor.Consumer {
		return nil
	}

	// See if we expect an ack for this.
	if o.cfg.AckPolicy != AckNone {
		// Need to create pending records here.
		if o.state.Pending == nil {
			o.state.Pending = make(map[uint64]*Pending)
		}
		var p *Pending
		// Check for an update to a message already delivered.
		if sseq <= o.state.Delivered.Stream {
			if p = o.state.Pending[sseq]; p != nil {
				// Do not update p.Sequence, that should be the original delivery sequence.
				p.Timestamp = ts
			}
		} else {
			// Add to pending.
			o.state.Pending[sseq] = &Pending{dseq, ts}
		}
		// Update delivered as needed.
		if dseq > o.state.Delivered.Consumer {
			o.state.Delivered.Consumer = dseq
		}
		if sseq > o.state.Delivered.Stream {
			o.state.Delivered.Stream = sseq
		}

		if dc > 1 {
			if maxdc := uint64(o.cfg.MaxDeliver); maxdc > 0 && dc > maxdc {
				// Make sure to remove from pending.
				delete(o.state.Pending, sseq)
			}
			if o.state.Redelivered == nil {
				o.state.Redelivered = make(map[uint64]uint64)
			}
			// Only update if greater than what we already have.
			if o.state.Redelivered[sseq] < dc-1 {
				o.state.Redelivered[sseq] = dc - 1
			}
		}
	} else {
		// For AckNone just update delivered and ackfloor at the same time.
		if dseq > o.state.Delivered.Consumer {
			o.state.Delivered.Consumer = dseq
			o.state.AckFloor.Consumer = dseq
		}
		if sseq > o.state.Delivered.Stream {
			o.state.Delivered.Stream = sseq
			o.state.AckFloor.Stream = sseq
		}
	}
	// Make sure we flush to disk.
	o.kickFlusher()

	return nil
}

// UpdateAcks is called whenever a consumer with explicit ack or ack all acks a message.
func (o *consumerFileStore) UpdateAcks(dseq, sseq uint64) error {
	o.mu.Lock()
	defer o.mu.Unlock()

	if o.cfg.AckPolicy == AckNone {
		return ErrNoAckPolicy
	}

	var kick bool
	defer func() {
		if kick {
			o.kickFlusher()
		}
	}()

	// We do this regardless.
	if _, ok := o.state.Redelivered[sseq]; ok {
		delete(o.state.Redelivered, sseq)
		kick = true
	}

	// On restarts the old leader may get a replay from the raft logs that are old.
	if dseq <= o.state.AckFloor.Consumer {
		return nil
	}

	if len(o.state.Pending) == 0 || o.state.Pending[sseq] == nil {
		return ErrStoreMsgNotFound
	}

	// Done with the consistency checks, we'll always kick for below updates.
	kick = true

	// Check for AckAll here (or AckFlowControl which functions like AckAll).
	if o.cfg.AckPolicy == AckAll || o.cfg.AckPolicy == AckFlowControl {
		sgap := sseq - o.state.AckFloor.Stream
		o.state.AckFloor.Consumer = dseq
		o.state.AckFloor.Stream = sseq
		if sgap > uint64(len(o.state.Pending)) {
			for seq := range o.state.Pending {
				if seq <= sseq {
					delete(o.state.Pending, seq)
					delete(o.state.Redelivered, seq)
				}
			}
		} else {
			for seq := sseq; seq > sseq-sgap && len(o.state.Pending) > 0; seq-- {
				delete(o.state.Pending, seq)
				delete(o.state.Redelivered, seq)
			}
		}
		return nil
	}

	// AckExplicit

	// First delete from our pending state.
	if p, ok := o.state.Pending[sseq]; ok {
		delete(o.state.Pending, sseq)
		if dseq > p.Sequence && p.Sequence > 0 {
			dseq = p.Sequence // Use the original.
		}
	}
	if len(o.state.Pending) == 0 {
		o.state.AckFloor.Consumer = o.state.Delivered.Consumer
		o.state.AckFloor.Stream = o.state.Delivered.Stream
	} else if dseq == o.state.AckFloor.Consumer+1 {
		o.state.AckFloor.Consumer = dseq
		o.state.AckFloor.Stream = sseq

		if o.state.Delivered.Consumer > dseq {
			for ss := sseq + 1; ss <= o.state.Delivered.Stream; ss++ {
				if p, ok := o.state.Pending[ss]; ok {
					if p.Sequence > 0 {
						o.state.AckFloor.Consumer = p.Sequence - 1
						o.state.AckFloor.Stream = ss - 1
					}
					break
				}
			}
		}
	}
	return nil
}

func (o *consumerFileStore) RemoveRedeliveredBelow(seq uint64) {
	if seq == 0 {
		return
	}
	o.mu.Lock()
	defer o.mu.Unlock()
	var removed bool
	for s := range o.state.Redelivered {
		if s < seq {
			delete(o.state.Redelivered, s)
			removed = true
		}
	}
	if removed {
		o.kickFlusher()
	}
}

const seqsHdrSize = 6*binary.MaxVarintLen64 + hdrLen

// Encode our consumer state, version 2.
// Lock should be held.

func (o *consumerFileStore) EncodedState() ([]byte, error) {
	o.mu.Lock()
	defer o.mu.Unlock()
	return o.encodeState()
}

func (o *consumerFileStore) encodeState() ([]byte, error) {
	// Grab reference to state, but make sure we load in if needed, so do not reference o.state directly.
	state, err := o.stateWithCopyLocked(false)
	if err != nil {
		return nil, err
	}
	return encodeConsumerState(state), nil
}

func (o *consumerFileStore) GetConfig() *ConsumerConfig {
	o.mu.Lock()
	defer o.mu.Unlock()
	clone := o.cfg.clone()
	clone.Name = o.name
	return clone
}

func (o *consumerFileStore) setCreatedTime(created time.Time) {
	o.mu.Lock()
	o.cfg.Created = created
	o.mu.Unlock()
}

func (o *consumerFileStore) UpdateConfig(cfg *ConsumerConfig) error {
	o.mu.Lock()
	defer o.mu.Unlock()

	// This is mostly unchecked here. We are assuming the upper layers have done sanity checking.
	csi := o.cfg
	csi.ConsumerConfig = *cfg

	return o.writeConsumerMeta()
}

func (o *consumerFileStore) Update(state *ConsumerState) error {
	// Sanity checks.
	if state.AckFloor.Consumer > state.Delivered.Consumer {
		return fmt.Errorf("bad ack floor for consumer")
	}
	if state.AckFloor.Stream > state.Delivered.Stream {
		return fmt.Errorf("bad ack floor for stream")
	}

	// Copy to our state.
	var pending map[uint64]*Pending
	var redelivered map[uint64]uint64
	if len(state.Pending) > 0 {
		pending = make(map[uint64]*Pending, len(state.Pending))
		for seq, p := range state.Pending {
			pending[seq] = &Pending{p.Sequence, p.Timestamp}
			if seq <= state.AckFloor.Stream || seq > state.Delivered.Stream {
				return fmt.Errorf("bad pending entry, sequence [%d] out of range", seq)
			}
		}
	}
	if len(state.Redelivered) > 0 {
		redelivered = make(map[uint64]uint64, len(state.Redelivered))
		for seq, dc := range state.Redelivered {
			redelivered[seq] = dc
		}
	}

	// Replace our state.
	o.mu.Lock()
	defer o.mu.Unlock()

	// Check to see if this is an outdated update.
	if state.Delivered.Consumer < o.state.Delivered.Consumer || state.AckFloor.Stream < o.state.AckFloor.Stream {
		return ErrStoreOldUpdate
	}

	o.state.Delivered = state.Delivered
	o.state.AckFloor = state.AckFloor
	o.state.Pending = pending
	o.state.Redelivered = redelivered

	o.kickFlusher()

	return nil
}

// ForceUpdate updates the consumer state without the backwards check.
// This is used during recovery when we need to reset the consumer to an earlier sequence.
func (o *consumerFileStore) ForceUpdate(state *ConsumerState) error {
	// Sanity checks.
	if state.AckFloor.Consumer > state.Delivered.Consumer {
		return fmt.Errorf("bad ack floor for consumer")
	}
	if state.AckFloor.Stream > state.Delivered.Stream {
		return fmt.Errorf("bad ack floor for stream")
	}

	// Copy to our state.
	var pending map[uint64]*Pending
	var redelivered map[uint64]uint64
	if len(state.Pending) > 0 {
		pending = make(map[uint64]*Pending, len(state.Pending))
		for seq, p := range state.Pending {
			pending[seq] = &Pending{p.Sequence, p.Timestamp}
			if seq <= state.AckFloor.Stream || seq > state.Delivered.Stream {
				return fmt.Errorf("bad pending entry, sequence [%d] out of range", seq)
			}
		}
	}
	if len(state.Redelivered) > 0 {
		redelivered = make(map[uint64]uint64, len(state.Redelivered))
		for seq, dc := range state.Redelivered {
			redelivered[seq] = dc
		}
	}

	// Replace our state.
	o.mu.Lock()
	o.state.Delivered = state.Delivered
	o.state.AckFloor = state.AckFloor
	o.state.Pending = pending
	o.state.Redelivered = redelivered
	buf, err := o.encodeState()
	o.mu.Unlock()
	if err != nil {
		return err
	}
	return o.writeState(buf)
}

// Will encrypt the state with our asset key. Will be a no-op if encryption not enabled.
// Lock should be held.
func (o *consumerFileStore) encryptState(buf []byte) ([]byte, error) {
	if o.aek == nil {
		return buf, nil
	}
	// TODO(dlc) - Optimize on space usage a bit?
	nonce := make([]byte, o.aek.NonceSize(), o.aek.NonceSize()+len(buf)+o.aek.Overhead())
	if n, err := rand.Read(nonce); err != nil {
		return nil, err
	} else if n != len(nonce) {
		return nil, fmt.Errorf("not enough nonce bytes read (%d != %d)", n, len(nonce))
	}
	return o.aek.Seal(nonce, nonce, buf, nil), nil
}

func (o *consumerFileStore) writeState(buf []byte) error {
	o.mu.Lock()
	// Don't write after being stopped or deleted, otherwise we could recreate the state file.
	if o.closed {
		o.mu.Unlock()
		return ErrStoreClosed
	}
	// Check if we have the index file open.
	if o.writing || len(buf) == 0 {
		o.mu.Unlock()
		return nil
	}

	// Check on encryption.
	if o.aek != nil {
		var err error
		if buf, err = o.encryptState(buf); err != nil {
			o.mu.Unlock()
			return err
		}
	}

	o.writing = true
	o.dirty = false
	ifn := o.ifn
	o.mu.Unlock()

	// Lock not held here but we do limit number of outstanding calls that could block OS threads.
	err := o.fs.writeFileWithOptionalSync(ifn, buf, defaultFilePerms)

	o.mu.Lock()
	if err != nil {
		o.dirty = true
	}
	o.writing = false
	o.mu.Unlock()

	return err
}

// Will upodate the config. Only used when recovering ephemerals.
func (o *consumerFileStore) updateConfig(cfg ConsumerConfig) error {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.cfg = &FileConsumerInfo{ConsumerConfig: cfg}
	return o.writeConsumerMeta()
}

// Write out the consumer meta data, i.e. state.
// Lock should be held.
func (cfs *consumerFileStore) writeConsumerMeta() error {
	// Don't write after being stopped or deleted, otherwise we'd write into the working directory.
	if cfs.closed {
		return ErrStoreClosed
	}
	meta := filepath.Join(cfs.odir, JetStreamMetaFile)
	if _, err := os.Stat(meta); err != nil && !os.IsNotExist(err) {
		return err
	}

	if cfs.prf != nil && cfs.aek == nil {
		fs := cfs.fs
		key, _, _, encrypted, err := fs.genEncryptionKeys(fs.cfg.Name + tsep + cfs.name)
		if err != nil {
			return err
		}
		cfs.aek = key
		keyFile := filepath.Join(cfs.odir, JetStreamMetaFileKey)
		if _, err := os.Stat(keyFile); err != nil && !os.IsNotExist(err) {
			return err
		}
		err = cfs.fs.writeFileWithOptionalSync(keyFile, encrypted, defaultFilePerms)
		if err != nil {
			return err
		}
	}

	b, err := json.Marshal(cfs.cfg)
	if err != nil {
		return err
	}
	// Encrypt if needed.
	if cfs.aek != nil {
		nonce := make([]byte, cfs.aek.NonceSize(), cfs.aek.NonceSize()+len(b)+cfs.aek.Overhead())
		if n, err := rand.Read(nonce); err != nil {
			return err
		} else if n != len(nonce) {
			return fmt.Errorf("not enough nonce bytes read (%d != %d)", n, len(nonce))
		}
		b = cfs.aek.Seal(nonce, nonce, b, nil)
	}

	err = cfs.fs.writeFileWithOptionalSync(meta, b, defaultFilePerms)
	if err != nil {
		return err
	}
	cfs.hh.Reset()
	cfs.hh.Write(b)
	var hb [highwayhash.Size64]byte
	checksum := hex.EncodeToString(cfs.hh.Sum(hb[:0]))
	sum := filepath.Join(cfs.odir, JetStreamMetaFileSum)

	err = cfs.fs.writeFileWithOptionalSync(sum, []byte(checksum), defaultFilePerms)
	if err != nil {
		return err
	}
	return nil
}

// Consumer version.
func checkConsumerHeader(hdr []byte) (uint8, error) {
	if len(hdr) < 2 || hdr[0] != magic {
		return 0, errCorruptState
	}
	version := hdr[1]
	switch version {
	case 1, 2:
		return version, nil
	}
	return 0, fmt.Errorf("unsupported version: %d", version)
}

func (o *consumerFileStore) copyPending() map[uint64]*Pending {
	pending := make(map[uint64]*Pending, len(o.state.Pending))
	for seq, p := range o.state.Pending {
		pending[seq] = &Pending{p.Sequence, p.Timestamp}
	}
	return pending
}

func (o *consumerFileStore) copyRedelivered() map[uint64]uint64 {
	redelivered := make(map[uint64]uint64, len(o.state.Redelivered))
	for seq, dc := range o.state.Redelivered {
		redelivered[seq] = dc
	}
	return redelivered
}

// Type returns the type of the underlying store.
func (o *consumerFileStore) Type() StorageType { return FileStorage }

// State retrieves the state from the state file.
// This is not expected to be called in high performance code, only on startup.
func (o *consumerFileStore) State() (*ConsumerState, error) {
	return o.stateWithCopy(true)
}

// This will not copy pending or redelivered, so should only be done under the
// consumer owner's lock.
func (o *consumerFileStore) BorrowState() (*ConsumerState, error) {
	return o.stateWithCopy(false)
}

func (o *consumerFileStore) stateWithCopy(doCopy bool) (*ConsumerState, error) {
	o.mu.Lock()
	defer o.mu.Unlock()
	return o.stateWithCopyLocked(doCopy)
}

// Lock should be held.
func (o *consumerFileStore) stateWithCopyLocked(doCopy bool) (*ConsumerState, error) {
	if o.closed {
		return nil, ErrStoreClosed
	}

	state := &ConsumerState{}

	// See if we have a running state or if we need to read in from disk.
	if o.state.Delivered.Consumer != 0 || o.state.Delivered.Stream != 0 {
		state.Delivered = o.state.Delivered
		state.AckFloor = o.state.AckFloor
		if len(o.state.Pending) > 0 {
			if doCopy {
				state.Pending = o.copyPending()
			} else {
				state.Pending = o.state.Pending
			}
		}
		if len(o.state.Redelivered) > 0 {
			if doCopy {
				state.Redelivered = o.copyRedelivered()
			} else {
				state.Redelivered = o.state.Redelivered
			}
		}
		return state, nil
	}

	// Read the state in here from disk..
	o.fs.dios.acquire()
	buf, err := os.ReadFile(o.ifn)
	o.fs.dios.release()

	if err != nil && !os.IsNotExist(err) {
		return nil, err
	}

	if len(buf) == 0 {
		return state, nil
	}

	// Check on encryption.
	if o.aek != nil {
		ns := o.aek.NonceSize()
		buf, err = o.aek.Open(nil, buf[:ns], buf[ns:], nil)
		if err != nil {
			return nil, err
		}
	}

	state, err = decodeConsumerState(buf)
	if err != nil {
		return nil, err
	}

	// Copy this state into our own.
	o.state.Delivered = state.Delivered
	o.state.AckFloor = state.AckFloor
	if len(state.Pending) > 0 {
		if doCopy {
			o.state.Pending = make(map[uint64]*Pending, len(state.Pending))
			for seq, p := range state.Pending {
				o.state.Pending[seq] = &Pending{p.Sequence, p.Timestamp}
			}
		} else {
			o.state.Pending = state.Pending
		}
	}
	if len(state.Redelivered) > 0 {
		if doCopy {
			o.state.Redelivered = make(map[uint64]uint64, len(state.Redelivered))
			for seq, dc := range state.Redelivered {
				o.state.Redelivered[seq] = dc
			}
		} else {
			o.state.Redelivered = state.Redelivered
		}
	}

	return state, nil
}

// Lock should be held. Called at startup.
func (o *consumerFileStore) loadState() error {
	if _, err := os.Stat(o.ifn); err == nil {
		// This will load our state in from disk.
		_, err = o.stateWithCopyLocked(false)
		return err
	} else if os.IsNotExist(err) {
		return nil
	} else {
		return err
	}
}

// Decode consumer state.
func decodeConsumerState(buf []byte) (*ConsumerState, error) {
	version, err := checkConsumerHeader(buf)
	if err != nil {
		return nil, err
	}

	bi := hdrLen
	// Helpers, will set i to -1 on error.
	readSeq := func() uint64 {
		if bi < 0 {
			return 0
		}
		seq, n := binary.Uvarint(buf[bi:])
		if n <= 0 {
			bi = -1
			return 0
		}
		bi += n
		return seq
	}
	readTimeStamp := func() int64 {
		if bi < 0 {
			return 0
		}
		ts, n := binary.Varint(buf[bi:])
		if n <= 0 {
			bi = -1
			return -1
		}
		bi += n
		return ts
	}
	// Just for clarity below.
	readLen := readSeq
	readCount := readSeq

	state := &ConsumerState{}
	state.AckFloor.Consumer = readSeq()
	state.AckFloor.Stream = readSeq()
	state.Delivered.Consumer = readSeq()
	state.Delivered.Stream = readSeq()

	if bi == -1 {
		return nil, errCorruptState
	}
	if version == 1 {
		// Adjust back. Version 1 also stored delivered as next to be delivered,
		// so adjust that back down here.
		if state.AckFloor.Consumer > 1 {
			state.Delivered.Consumer += state.AckFloor.Consumer - 1
		}
		if state.AckFloor.Stream > 1 {
			state.Delivered.Stream += state.AckFloor.Stream - 1
		}
	}

	// Protect ourselves against rolling backwards.
	const hbit = 1 << 63
	if state.AckFloor.Stream&hbit != 0 || state.Delivered.Stream&hbit != 0 {
		return nil, errCorruptState
	}

	// We have additional stuff.
	if numPending := readLen(); numPending > 0 {
		mints := readTimeStamp()
		state.Pending = make(map[uint64]*Pending, numPending)
		for i := 0; i < int(numPending); i++ {
			sseq := readSeq()
			var dseq uint64
			if version == 2 {
				dseq = readSeq()
			}
			ts := readTimeStamp()
			// Check the state machine for corruption, not the value which could be -1.
			if bi == -1 {
				return nil, errCorruptState
			}
			// Adjust seq back.
			sseq += state.AckFloor.Stream
			if sseq == 0 {
				return nil, errCorruptState
			}
			if version == 2 {
				dseq += state.AckFloor.Consumer
			}
			// Adjust the timestamp back.
			if version == 1 {
				ts = (ts + mints) * int64(time.Second)
			} else {
				ts = (mints - ts) * int64(time.Second)
			}
			// Store in pending.
			state.Pending[sseq] = &Pending{dseq, ts}
		}
	}

	// We have redelivered entries here.
	if numRedelivered := readLen(); numRedelivered > 0 {
		state.Redelivered = make(map[uint64]uint64, numRedelivered)
		for i := 0; i < int(numRedelivered); i++ {
			if seq, n := readSeq(), readCount(); seq > 0 && n > 0 {
				// Adjust seq back.
				seq += state.AckFloor.Stream
				state.Redelivered[seq] = n
			}
		}
	}

	return state, nil
}

// Stop the processing of the consumers's state.
func (o *consumerFileStore) Stop() error {
	o.mu.Lock()
	if o.closed {
		o.mu.Unlock()
		return nil
	}
	if o.qch != nil {
		close(o.qch)
		o.qch = nil
	}

	var err error
	var buf []byte

	if o.dirty {
		// Make sure to write this out..
		if buf, err = o.encodeState(); err == nil && len(buf) > 0 {
			if o.aek != nil {
				if buf, err = o.encryptState(buf); err != nil {
					o.mu.Unlock()
					return err
				}
			}
		}
	}

	o.odir = _EMPTY_
	o.closed = true
	ifn, fs := o.ifn, o.fs
	o.mu.Unlock()

	if err = fs.RemoveConsumer(o); err != nil {
		return err
	}

	// Wait for an in-progress flush, even if not dirty, so the state is on disk when we return.
	o.waitOnFlusher()
	if len(buf) > 0 {
		err = o.fs.writeFileWithOptionalSync(ifn, buf, defaultFilePerms)
	}
	return err
}

func (o *consumerFileStore) waitOnFlusher() {
	if !o.inFlusher() {
		return
	}

	timeout := time.Now().Add(100 * time.Millisecond)
	for time.Now().Before(timeout) {
		if !o.inFlusher() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// Delete the consumer.
func (o *consumerFileStore) Delete() error {
	return o.delete(false)
}

func (o *consumerFileStore) StreamDelete() error {
	return o.delete(true)
}

func (o *consumerFileStore) delete(streamDeleted bool) error {
	o.mu.Lock()
	if o.closed {
		o.mu.Unlock()
		return nil
	}
	if o.qch != nil {
		close(o.qch)
		o.qch = nil
	}

	odir := o.odir
	o.odir = _EMPTY_
	o.closed = true
	fs := o.fs
	o.mu.Unlock()

	// If our stream was not deleted this will remove the directories.
	if odir != _EMPTY_ && !streamDeleted {
		if err := removeAllWithRetry(o.fs.dios, odir); err != nil {
			return err
		}
	}

	if !streamDeleted {
		if err := fs.RemoveConsumer(o); err != nil {
			return err
		}
	}

	return nil
}

func (fs *fileStore) AddConsumer(o ConsumerStore) error {
	fs.cmu.Lock()
	defer fs.cmu.Unlock()
	fs.cfs = append(fs.cfs, o)
	return nil
}

func (fs *fileStore) RemoveConsumer(o ConsumerStore) error {
	fs.cmu.Lock()
	defer fs.cmu.Unlock()
	for i, cfs := range fs.cfs {
		if o == cfs {
			fs.cfs = append(fs.cfs[:i], fs.cfs[i+1:]...)
			break
		}
	}
	return nil
}

func (fs *fileStore) Consumers() iter.Seq[ConsumerStore] {
	return func(yield func(ConsumerStore) bool) {
		fs.cmu.RLock()
		defer fs.cmu.RUnlock()

		for _, v := range fs.cfs {
			if !yield(v) {
				return
			}
		}
	}
}

////////////////////////////////////////////////////////////////////////////////
// Compression
////////////////////////////////////////////////////////////////////////////////

type CompressionInfo struct {
	Algorithm    StoreCompression
	OriginalSize uint64
}

func (c *CompressionInfo) MarshalMetadata() []byte {
	b := make([]byte, 14) // 4 + potentially up to 10 for uint64
	b[0], b[1], b[2] = 'c', 'm', 'p'
	b[3] = byte(c.Algorithm)
	n := binary.PutUvarint(b[4:], c.OriginalSize)
	return b[:4+n]
}

func (c *CompressionInfo) UnmarshalMetadata(b []byte) (int, error) {
	c.Algorithm = NoCompression
	c.OriginalSize = 0
	if len(b) < 5 { // 4 + min 1 for uvarint uint64
		return 0, nil
	}
	// An uncompressed record with length 7,368,035 starts with "cmp",
	// so we also check algorithm byte.
	if b[0] != 'c' || b[1] != 'm' || b[2] != 'p' || StoreCompression(b[3]) != S2Compression {
		return 0, nil
	}
	var n int
	c.Algorithm = StoreCompression(b[3])
	c.OriginalSize, n = binary.Uvarint(b[4:])
	if n <= 0 {
		return 0, fmt.Errorf("metadata incomplete")
	}
	return 4 + n, nil
}

func (alg StoreCompression) Compress(buf []byte) ([]byte, error) {
	if len(buf) < checksumSize {
		return nil, fmt.Errorf("uncompressed buffer is too short")
	}
	bodyLen := int64(len(buf) - checksumSize)
	var output bytes.Buffer
	var writer io.WriteCloser
	switch alg {
	case NoCompression:
		return buf, nil
	case S2Compression:
		writer = s2.NewWriter(&output)
	default:
		return nil, fmt.Errorf("compression algorithm not known")
	}

	input := bytes.NewReader(buf[:bodyLen])
	checksum := buf[bodyLen:]

	// Compress the block content, but don't compress the checksum.
	// We will preserve it at the end of the block as-is.
	if n, err := io.CopyN(writer, input, bodyLen); err != nil {
		return nil, fmt.Errorf("error writing to compression writer: %w", err)
	} else if n != bodyLen {
		return nil, fmt.Errorf("short write on body (%d != %d)", n, bodyLen)
	}
	if err := writer.Close(); err != nil {
		return nil, fmt.Errorf("error closing compression writer: %w", err)
	}

	// Now add the checksum back onto the end of the block.
	if n, err := output.Write(checksum); err != nil {
		return nil, fmt.Errorf("error writing checksum: %w", err)
	} else if n != checksumSize {
		return nil, fmt.Errorf("short write on checksum (%d != %d)", n, checksumSize)
	}

	return output.Bytes(), nil
}

func (alg StoreCompression) Decompress(buf []byte) ([]byte, error) {
	if len(buf) < checksumSize {
		return nil, fmt.Errorf("compressed buffer is too short")
	}
	bodyLen := int64(len(buf) - checksumSize)
	input := bytes.NewReader(buf[:bodyLen])

	var reader io.ReadCloser
	switch alg {
	case NoCompression:
		return buf, nil
	case S2Compression:
		reader = io.NopCloser(s2.NewReader(input))
	default:
		return nil, fmt.Errorf("compression algorithm not known")
	}

	// Decompress the block content. The checksum isn't compressed so
	// we can preserve it from the end of the block as-is.
	checksum := buf[bodyLen:]
	output, err := io.ReadAll(reader)
	if err != nil {
		return nil, fmt.Errorf("error reading compression reader: %w", err)
	}
	output = append(output, checksum...)

	return output, reader.Close()
}

// writeFileWithOptionalSync is equivalent to os.WriteFile() but optionally
// sets O_SYNC on the open file if SyncAlways is set. The dios semaphore is
// handled automatically by this function, so don't wrap calls to it in dios.
func (fs *fileStore) writeFileWithOptionalSync(name string, data []byte, perm fs.FileMode) error {
	sync := fs.syncAlways.Load() || fs.syncOnFlush.Load()
	return writeAtomically(fs.dios, name, data, perm, sync)
}

func writeFileWithSync(dios *diskIOSemaphore, name string, data []byte, perm fs.FileMode) error {
	return writeAtomically(dios, name, data, perm, true)
}

// Windows does not support fsyncing directory metadata, it results in a panic, so
// we need to skip doing this there.
const canFsyncDirectories = runtime.GOOS != "windows"

func writeAtomically(dios *diskIOSemaphore, name string, data []byte, perm fs.FileMode, sync bool) error {
	return writeAtomicallyWithTemp(dios, name+".tmp", name, data, perm, sync)
}

func writeAtomicallyWithTemp(dios *diskIOSemaphore, tmp, name string, data []byte, perm fs.FileMode, sync bool) error {
	flags := os.O_CREATE | os.O_WRONLY | os.O_TRUNC
	if sync {
		flags = flags | os.O_SYNC
	}
	dios.acquire()
	defer dios.release()
	f, err := os.OpenFile(tmp, flags, perm)
	if err != nil {
		return err
	}
	if _, err := f.Write(data); err != nil {
		// Close fd, but ignore its error since write takes precedence.
		_ = f.Close()
		_ = os.Remove(tmp)
		return err
	}
	if err := f.Close(); err != nil {
		_ = os.Remove(tmp)
		return err
	}
	if err := os.Rename(tmp, name); err != nil {
		_ = os.Remove(tmp)
		return err
	}
	if sync && canFsyncDirectories {
		// To ensure that the file rename was persisted on all filesystems,
		// also try to flush the directory metadata.
		if err = syncDir(filepath.Dir(name)); err != nil {
			return err
		}
	}
	return nil
}

func (fs *fileStore) syncFileAndDir(name string) error {
	fs.dios.acquire()
	defer fs.dios.release()
	f, err := os.OpenFile(name, os.O_RDWR, defaultFilePerms)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}
	if err = f.Sync(); err != nil {
		// Close fd, but ignore its error since sync takes precedence.
		_ = f.Close()
		return err
	}
	if err = f.Close(); err != nil {
		return err
	}
	if canFsyncDirectories {
		if err = syncDir(filepath.Dir(name)); err != nil {
			return err
		}
	}
	return nil
}

// Dios should already be held.
func syncDir(path string) error {
	var d *os.File
	var err error
	if d, err = os.Open(path); err != nil {
		return err
	}
	if err = d.Sync(); err != nil {
		// Close fd, but ignore its error since sync takes precedence.
		_ = d.Close()
		return err
	}
	if err = d.Close(); err != nil {
		return err
	}
	return nil
}
