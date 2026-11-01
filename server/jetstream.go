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
	"bytes"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"runtime/debug"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/minio/highwayhash"
	"github.com/nats-io/nats-server/v2/server/gsl"
	"github.com/nats-io/nats-server/v2/server/sysmem"
	"github.com/nats-io/nats-server/v2/server/tpm"
	"github.com/nats-io/nkeys"
	"github.com/nats-io/nuid"
)

// JetStreamConfig determines this server's configuration.
// MaxMemory and MaxStore are in bytes.
type JetStreamConfig struct {
	MaxMemory    int64         `json:"max_memory"`              // MaxMemory is the maximum size of memory type streams
	MaxStore     int64         `json:"max_storage"`             // MaxStore is the maximum size of file store type streams
	StoreDir     string        `json:"store_dir,omitempty"`     // StoreDir is where storage files are stored
	SyncInterval time.Duration `json:"sync_interval,omitempty"` // SyncInterval is how frequently we sync to disk in the background by calling fsync
	SyncAlways   bool          `json:"sync_always,omitempty"`   // SyncAlways indicates flushes are done after every write
	Domain       string        `json:"domain,omitempty"`        // Domain is the JetStream domain
	CompressOK   bool          `json:"compress_ok,omitempty"`   // CompressOK indicates if compression is supported
	UniqueTag    string        `json:"unique_tag,omitempty"`    // UniqueTag is the unique tag assigned to this instance
	Strict       bool          `json:"strict,omitempty"`        // Strict indicates if strict JSON parsing is performed

	// maxStorePending is set when MaxStore was derived from the available disk
	// space rather than configured, and has not been adjusted yet for the space
	// that recovered streams already occupy. See finalizeDynamicMaxStore.
	maxStorePending bool
}

// Statistics about JetStream for this server.
type JetStreamStats struct {
	Memory         uint64            `json:"memory"`
	Store          uint64            `json:"storage"`
	ReservedMemory uint64            `json:"reserved_memory"`
	ReservedStore  uint64            `json:"reserved_storage"`
	Accounts       int               `json:"accounts"`
	HAAssets       int               `json:"ha_assets"`
	API            JetStreamAPIStats `json:"api"`
}

type JetStreamAccountLimits struct {
	MaxMemory            int64 `json:"max_memory"`
	MaxStore             int64 `json:"max_storage"`
	MaxStreams           int   `json:"max_streams"`
	MaxConsumers         int   `json:"max_consumers"`
	MaxAckPending        int   `json:"max_ack_pending"`
	MemoryMaxStreamBytes int64 `json:"memory_max_stream_bytes"`
	StoreMaxStreamBytes  int64 `json:"storage_max_stream_bytes"`
	MaxBytesRequired     bool  `json:"max_bytes_required"`
}

type JetStreamTier struct {
	Memory         uint64                 `json:"memory"`
	Store          uint64                 `json:"storage"`
	ReservedMemory uint64                 `json:"reserved_memory"`
	ReservedStore  uint64                 `json:"reserved_storage"`
	Streams        int                    `json:"streams"`
	Consumers      int                    `json:"consumers"`
	Limits         JetStreamAccountLimits `json:"limits"`
}

// JetStreamAccountStats returns current statistics about the account's JetStream usage.
type JetStreamAccountStats struct {
	JetStreamTier                          // in case tiers are used, reflects totals with limits not set
	Domain        string                   `json:"domain,omitempty"`
	API           JetStreamAPIStats        `json:"api"`
	Tiers         map[string]JetStreamTier `json:"tiers,omitempty"` // indexed by tier name
}

// JetStreamAPIStats holds stats about the API usage for this server
type JetStreamAPIStats struct {
	Level    int    `json:"level"`              // Level is the active API level this server implements
	Total    uint64 `json:"total"`              // Total is the total API requests received since start
	Errors   uint64 `json:"errors"`             // Errors is the total API requests that resulted in error responses
	Inflight uint64 `json:"inflight,omitempty"` // Inflight are the number of API requests currently being served
}

// This is for internal accounting for JetStream for this server.
type jetStream struct {
	// These are here first because of atomics on 32bit systems.
	apiInflight    int64
	apiTotal       int64
	apiErrors      int64
	memMax         int64
	memReserved    int64 // Requires JS lock to be held.
	memUsed        int64
	storeMax       int64
	storeReserved  int64 // Requires JS lock to be held.
	storeUsed      int64
	queueLimit     int64
	infoQueueLimit int64
	clustered      int32
	mu             sync.RWMutex
	srv            *Server
	config         JetStreamConfig
	cluster        *jetStreamCluster
	accounts       map[string]*jsAccount
	apiSubs        *Sublist
	infoSubs       *gsl.SimpleSublist // Subjects for info-specific queue.
	started        time.Time

	// System level request to purge a stream move
	accountPurge *subscription

	// Debounced reconcile of assignments for peers that became selectable.
	// Has its own lock so signaling is cheap and never contends on the JS lock.
	prMu    sync.Mutex
	prPeers map[string]struct{}
	// Meta peers that were just added but that we can't place on yet, because
	// we have no STATSZ for them. Their first STATSZ moves them into prPeers.
	prNewPeers map[string]struct{}
	prRunning  bool

	// Some bools regarding general state.
	metaRecovering bool
	standAlone     bool
	oos            bool
	shuttingDown   bool

	// Atomic versions
	disabled atomic.Bool
}

type remoteUsage struct {
	tiers map[string]*jsaUsage // indexed by tier name
	api   uint64
	err   uint64
}

type jsaStorage struct {
	total jsaUsage
	local jsaUsage
}

// This represents a jetstream enabled account.
// Worth noting that we include the jetstream pointer, this is because
// in general we want to be very efficient when receiving messages on
// an internal sub for a stream, so we will direct link to the stream
// and walk backwards as needed vs multiple hash lookups and locks, etc.
type jsAccount struct {
	mu       sync.RWMutex
	js       *jetStream
	account  *Account
	storeDir string
	inflight sync.Map
	streams  map[string]*stream

	// From server
	sendq *ipQueue[*pubMsg]

	// For limiting only running one checkAndSync at a time.
	sync atomic.Bool

	// Usage/limits related fields that will be protected by usageMu
	usageMu    sync.RWMutex
	limits     map[string]JetStreamAccountLimits // indexed by tierName
	usage      map[string]*jsaStorage            // indexed by tierName
	rusage     map[string]*remoteUsage           // indexed by node id
	apiTotal   uint64
	apiErrors  uint64
	usageApi   uint64
	usageErr   uint64
	updatesPub string
	updatesSub *subscription
	lupdate    time.Time
	utimer     *time.Timer
}

// Track general usage for this account.
type jsaUsage struct {
	mem   int64
	store int64
}

// EnableJetStream will enable JetStream support on this server with the given configuration.
// A nil configuration will dynamically choose the limits and temporary file storage directory.
func (s *Server) EnableJetStream(config *JetStreamConfig) error {
	if s.JetStreamEnabled() {
		return fmt.Errorf("jetstream already enabled")
	}

	s.Noticef("Starting JetStream")
	start := time.Now()
	defer func() {
		s.Noticef("Took %s to start JetStream", time.Since(start))
	}()

	if config == nil || config.MaxMemory <= 0 || config.MaxStore <= 0 {
		var storeDir, domain, uniqueTag string
		var maxStore, maxMem int64
		if config != nil {
			storeDir, domain, uniqueTag = config.StoreDir, config.Domain, config.UniqueTag
			maxStore, maxMem = config.MaxStore, config.MaxMemory
		}
		config = s.dynJetStreamConfig(storeDir, maxStore, maxMem)
		if domain != _EMPTY_ {
			config.Domain = domain
		}
		if uniqueTag != _EMPTY_ {
			config.UniqueTag = uniqueTag
		}
		s.Debugf("JetStream creating dynamic configuration - %s memory, %s disk", friendlyBytes(config.MaxMemory), friendlyBytes(config.MaxStore))
	} else if config.StoreDir != _EMPTY_ {
		config.StoreDir = filepath.Join(config.StoreDir, JetStreamStoreDir)
	}

	cfg := *config
	if cfg.StoreDir == _EMPTY_ {
		cfg.StoreDir = filepath.Join(os.TempDir(), JetStreamStoreDir)
		s.Warnf("Temporary storage directory used, data could be lost on system reboot")
	}

	// We will consistently place the 'jetstream' directory under the storedir that was handed to us. Prior to 2.2.3 though
	// we could have a directory on disk without the 'jetstream' directory. This will check and fix if needed.
	if err := s.checkStoreDir(&cfg); err != nil {
		return err
	}

	return s.enableJetStream(cfg)
}

// Function signature to generate a key encryption key.
type keyGen func(context []byte) ([]byte, error)

// Return a key generation function or nil if encryption not enabled.
func (s *Server) jsKeyGen(jsKey, info string) keyGen {
	if ek := jsKey; ek != _EMPTY_ {
		return func(context []byte) ([]byte, error) {
			h := hmac.New(sha256.New, []byte(ek))
			if _, err := h.Write([]byte(info)); err != nil {
				return nil, err
			}
			if _, err := h.Write(context); err != nil {
				return nil, err
			}
			return h.Sum(nil), nil
		}
	}
	return nil
}

// Decode the encrypted metafile.
func (s *Server) decryptMeta(sc StoreCipher, ekey, buf []byte, acc, context string) ([]byte, bool, error) {
	if len(ekey) < minMetaKeySize {
		return nil, false, errBadKeySize
	}
	var osc StoreCipher
	switch sc {
	case AES:
		osc = ChaCha
	case ChaCha:
		osc = AES
	}
	type prfWithCipher struct {
		keyGen
		StoreCipher
	}
	var prfs []prfWithCipher
	if prf := s.jsKeyGen(s.getOpts().JetStreamKey, acc); prf == nil {
		return nil, false, errNoEncryption
	} else {
		// First of all, try our current encryption keys with both
		// store cipher algorithms.
		prfs = append(prfs, prfWithCipher{prf, sc})
		prfs = append(prfs, prfWithCipher{prf, osc})
	}
	if prf := s.jsKeyGen(s.getOpts().JetStreamOldKey, acc); prf != nil {
		// Then, if we have an old encryption key, try with also with
		// both store cipher algorithms.
		prfs = append(prfs, prfWithCipher{prf, sc})
		prfs = append(prfs, prfWithCipher{prf, osc})
	}

	for i, prf := range prfs {
		rb, err := prf.keyGen([]byte(context))
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
		aek, err := genEncryptionKey(prf.StoreCipher, seed)
		if err != nil {
			continue
		}
		if aek.NonceSize() != kek.NonceSize() {
			continue
		}
		plain, err := aek.Open(nil, buf[:ns], buf[ns:], nil)
		if err != nil {
			continue
		}
		return plain, i > 0, nil
	}
	return nil, false, fmt.Errorf("unable to recover keys")
}

// Check to make sure directory has the jetstream directory.
// We will have it properly configured here now regardless, so need to look inside.
func (s *Server) checkStoreDir(cfg *JetStreamConfig) error {
	fis, _ := os.ReadDir(cfg.StoreDir)
	// If we have nothing underneath us, could be just starting new, but if we see this we can check.
	if len(fis) != 0 {
		return nil
	}
	// Let's check the directory above. If it has us 'jetstream' but also other stuff that we can
	// identify as accounts then we can fix.
	fis, _ = os.ReadDir(filepath.Dir(cfg.StoreDir))
	// If just one that is us 'jetstream' and all is ok.
	if len(fis) == 1 {
		return nil
	}

	haveJetstreamDir := false
	for _, fi := range fis {
		if fi.Name() == JetStreamStoreDir {
			haveJetstreamDir = true
			break
		}
	}

	for _, fi := range fis {
		// Skip the 'jetstream' directory.
		if fi.Name() == JetStreamStoreDir {
			continue
		}
		// Let's see if this is an account.
		if accName := fi.Name(); accName != _EMPTY_ {
			_, ok := s.accounts.Load(accName)
			if !ok && s.AccountResolver() != nil && nkeys.IsValidPublicAccountKey(accName) {
				// Account is not local but matches the NKEY account public key,
				// this is enough indication to move this directory, no need to
				// fetch the account.
				ok = true
			}
			// If this seems to be an account go ahead and move the directory. This will include all assets
			// like streams and consumers.
			if ok {
				if !haveJetstreamDir {
					err := os.Mkdir(filepath.Join(filepath.Dir(cfg.StoreDir), JetStreamStoreDir), defaultDirPerms)
					if err != nil {
						return err
					}
					haveJetstreamDir = true
				}
				old := filepath.Join(filepath.Dir(cfg.StoreDir), fi.Name())
				new := filepath.Join(cfg.StoreDir, fi.Name())
				s.Noticef("JetStream relocated account %q to %q", old, new)
				if err := os.Rename(old, new); err != nil {
					return err
				}
			}
		}
	}

	return nil
}

// This function sets/updates the jetstream encryption key and cipher based
// on options. If the TPM options have been specified, a key is generated
// and sealed by the TPM.
func (s *Server) initJetStreamEncryption() (err error) {
	opts := s.getOpts()

	// The TPM settings and other encryption settings are mutually exclusive.
	if opts.JetStreamKey != _EMPTY_ && opts.JetStreamTpm.KeysFile != _EMPTY_ {
		return fmt.Errorf("JetStream encryption key may not be used with TPM options")
	}
	// if we are using the standard method to set the encryption key just return and carry on.
	if opts.JetStreamKey != _EMPTY_ {
		return nil
	}
	// if the tpm options are not used then no encryption has been configured and return.
	if opts.JetStreamTpm.KeysFile == _EMPTY_ {
		return nil
	}

	if opts.JetStreamTpm.Pcr == 0 {
		// Default PCR to use in the TPM. Values can be 0-23, and most platforms
		// reserve values 0-12 for the OS, boot locker, disc encryption, etc.
		// 16 used for debugging. In sticking to NATS tradition, we'll use 22
		// as the default with the option being configurable.
		opts.JetStreamTpm.Pcr = 22
	}

	// Using the TPM to generate or get the encryption key and update the encryption options.
	opts.JetStreamKey, err = tpm.LoadJetStreamEncryptionKeyFromTPM(opts.JetStreamTpm.SrkPassword,
		opts.JetStreamTpm.KeysFile, opts.JetStreamTpm.KeyPassword, opts.JetStreamTpm.Pcr)

	return err
}

// enableJetStream will start up the JetStream subsystem.
func (s *Server) enableJetStream(cfg JetStreamConfig) error {
	js := &jetStream{srv: s, config: cfg, accounts: make(map[string]*jsAccount), apiSubs: NewSublistNoCache(), infoSubs: gsl.NewSimpleSublist()}
	s.gcbMu.Lock()
	if s.gcbOutMax = s.getOpts().JetStreamMaxCatchup; s.gcbOutMax == 0 {
		s.gcbOutMax = defaultMaxTotalCatchupOutBytes
	}
	s.gcbMu.Unlock()

	atomic.StoreInt64(&js.memMax, cfg.MaxMemory)
	atomic.StoreInt64(&js.storeMax, cfg.MaxStore)

	// TODO: Not currently reloadable.
	atomic.StoreInt64(&js.queueLimit, s.getOpts().JetStreamRequestQueueLimit)
	atomic.StoreInt64(&js.infoQueueLimit, s.getOpts().JetStreamInfoQueueLimit)

	s.js.Store(js)

	// FIXME(dlc) - Allow memory only operation?
	if stat, err := os.Stat(cfg.StoreDir); os.IsNotExist(err) {
		if err := os.MkdirAll(cfg.StoreDir, defaultDirPerms); err != nil {
			return fmt.Errorf("could not create storage directory - %v", err)
		}
	} else {
		// Make sure its a directory and that we can write to it.
		if stat == nil || !stat.IsDir() {
			return fmt.Errorf("storage directory is not a directory")
		}
		tmpfile, err := os.CreateTemp(cfg.StoreDir, "_test_")
		if err != nil {
			return fmt.Errorf("storage directory is not writable")
		}
		tmpfile.Close()
		os.Remove(tmpfile.Name())
	}

	if err := s.initJetStreamEncryption(); err != nil {
		return err
	}

	// JetStream is an internal service so we need to make sure we have a system account.
	// This system account will export the JetStream service endpoints.
	if s.SystemAccount() == nil {
		s.SetDefaultSystemAccount()
	}

	opts := s.getOpts()
	if !opts.DisableJetStreamBanner {
		s.Noticef("    _ ___ _____ ___ _____ ___ ___   _   __  __")
		s.Noticef(" _ | | __|_   _/ __|_   _| _ \\ __| /_\\ |  \\/  |")
		s.Noticef("| || | _|  | | \\__ \\ | | |   / _| / _ \\| |\\/| |")
		s.Noticef(" \\__/|___| |_| |___/ |_| |_|_\\___/_/ \\_\\_|  |_|")
		s.Noticef("")
		s.Noticef("         https://docs.nats.io/jetstream")
		s.Noticef("")
	}
	s.Noticef("---------------- JETSTREAM ----------------")

	if cfg.Strict {
		s.Noticef("  Strict:          %t", cfg.Strict)
	}

	s.Noticef("  Max Memory:      %s", friendlyBytes(cfg.MaxMemory))
	s.Noticef("  Max Storage:     %s", friendlyBytes(cfg.MaxStore))
	s.Noticef("  Store Directory: \"%s\"", cfg.StoreDir)
	if cfg.Domain != _EMPTY_ {
		s.Noticef("  Domain:          %s", cfg.Domain)
	}

	if ek := opts.JetStreamKey; ek != _EMPTY_ {
		s.Noticef("  Encryption:      %s", opts.JetStreamCipher)
	}
	if opts.JetStreamTpm.KeysFile != _EMPTY_ {
		s.Noticef("  TPM File:        %q, Pcr: %d", opts.JetStreamTpm.KeysFile,
			opts.JetStreamTpm.Pcr)
	}
	s.Noticef("  API Level:       %d", JSApiLevel)
	s.Noticef("-------------------------------------------")

	// Setup our internal subscriptions.
	if err := s.setJetStreamExportSubs(); err != nil {
		return fmt.Errorf("setting up internal jetstream subscriptions failed: %v", err)
	}

	// Setup our internal system exports.
	s.Debugf("  Exports:")
	s.Debugf("     %s", jsAllAPI)
	s.setupJetStreamExports()

	standAlone, canExtend := s.standAloneMode(), s.canExtendOtherDomain()
	if standAlone && canExtend && s.getOpts().JetStreamExtHint != jsWillExtend {
		canExtend = false
		s.Noticef("Standalone server started in clustered mode do not support extending domains")
		s.Noticef(`Manually disable standalone mode by setting the JetStream Option "extension_hint: %s"`, jsWillExtend)
	}

	// Indicate if we will be standalone for checking resource reservations, etc.
	js.setJetStreamStandAlone(standAlone && !canExtend)

	// Enable accounts and restore state before starting clustering.
	if err := s.enableJetStreamAccounts(); err != nil {
		return err
	}

	// All file based streams have been recovered, so we now know how much of
	// the disk we already occupy and can settle on a dynamic limit.
	js.finalizeDynamicMaxStore()

	// If we are in clustered mode go ahead and start the meta controller.
	if !standAlone || canExtend {
		if err := s.enableJetStreamClustering(); err != nil {
			return err
		}
		// Set our atomic bool to clustered.
		s.jsClustered.Store(true)
	}

	// Mark when we are up and running.
	js.setStarted()

	return nil
}

// finalizeDynamicMaxStore settles a dynamic max store limit once all file based
// streams have been recovered. diskAvailable only reports free space, so add back
// what we occupy ourselves to keep the limit stable across restarts.
func (js *jetStream) finalizeDynamicMaxStore() {
	js.mu.Lock()
	if !js.config.maxStorePending {
		js.mu.Unlock()
		return
	}
	// From here on the limit is real and can be enforced, see sufficientResources.
	js.config.maxStorePending = false
	recovered := atomic.LoadInt64(&js.storeUsed)
	if recovered <= 0 {
		js.mu.Unlock()
		return
	}
	// diskAvailable is already scaled down to 75% of what is free, so scale the
	// recovered bytes the same way before adding them back.
	maxStore := addSaturate(js.config.MaxStore, recovered/4*3)
	js.config.MaxStore = maxStore
	atomic.StoreInt64(&js.storeMax, maxStore)
	js.mu.Unlock()
}

const jsNoExtend = "no_extend"
const jsWillExtend = "will_extend"

// This will check if we have a solicited leafnode that shares the system account
// and extension is not manually disabled
func (s *Server) canExtendOtherDomain() bool {
	opts := s.getOpts()
	sysAcc := s.SystemAccount().GetName()
	for _, r := range opts.LeafNode.Remotes {
		if r.LocalAccount == sysAcc {
			for _, denySub := range r.DenyImports {
				if subjectIsSubsetMatch(denySub, raftAllSubj) {
					return false
				}
			}
			return true
		}
	}
	return false
}

func (s *Server) updateJetStreamInfoStatus(enabled bool) {
	s.mu.Lock()
	s.info.JetStream = enabled
	s.mu.Unlock()
}

// restartJetStream will try to re-enable JetStream during a reload if it had been disabled during runtime.
func (s *Server) restartJetStream() error {
	opts := s.getOpts()
	cfg := JetStreamConfig{
		StoreDir:     opts.StoreDir,
		SyncInterval: opts.SyncInterval,
		SyncAlways:   opts.SyncAlways,
		MaxMemory:    opts.JetStreamMaxMemory,
		MaxStore:     opts.JetStreamMaxStore,
		Domain:       opts.JetStreamDomain,
		Strict:       !opts.NoJetStreamStrict,
	}
	s.Noticef("Restarting JetStream")
	err := s.EnableJetStream(&cfg)
	if err != nil {
		s.Warnf("Can't start JetStream: %v", err)
		return s.ShutdownJetStream()
	}
	s.updateJetStreamInfoStatus(true)
	return nil
}

// checkJetStreamExports will check if we have the JS exports setup
// on the system account, and if not go ahead and set them up.
func (s *Server) checkJetStreamExports() {
	if sacc := s.SystemAccount(); sacc != nil {
		sacc.mu.RLock()
		se := sacc.getServiceExport(jsAllAPI)
		sacc.mu.RUnlock()
		if se == nil {
			s.setupJetStreamExports()
		}
	}
}

func (s *Server) setupJetStreamExports() {
	// Setup our internal system export.
	sacc := s.SystemAccount()
	if err := sacc.AddServiceExport(jsAllAPI, nil); err != nil {
		s.Warnf("Error setting up jetstream service exports: %v", err)
	}
	// Map the domain prefixed API too, so an isolated JetStream is addressable by
	// domain like an account is. Unprefixed always means this server's own JetStream.
	if domain := s.getOpts().JetStreamDomain; domain != _EMPTY_ {
		src, dest := fmt.Sprintf(jsDomainAPI, domain), jsAllAPI
		if err := sacc.AddMapping(src, dest); err != nil {
			s.Errorf("Error adding JetStream domain mapping to system account: %v", err)
		} else {
			s.Debugf("Adding JetStream Domain Mapping %q -> %s to system account", src, dest)
		}
	}
}

func (s *Server) jetStreamOOSPending() (wasPending bool) {
	if js := s.getJetStream(); js != nil {
		js.mu.Lock()
		wasPending = js.oos
		js.oos = true
		js.mu.Unlock()
	}
	return wasPending
}

func (s *Server) setJetStreamDisabled() {
	if js := s.getJetStream(); js != nil {
		js.disabled.Store(true)
	}
}

func (s *Server) handleOutOfSpace(mset *stream) {
	if s.JetStreamEnabled() && !s.jetStreamOOSPending() {
		var stream string
		if mset != nil {
			stream = mset.name()
			s.Errorf("JetStream out of %s resources, will be DISABLED", mset.Store().Type())
		} else {
			s.Errorf("JetStream out of resources, will be DISABLED")
		}

		go s.ShutdownJetStream()

		adv := &JSServerOutOfSpaceAdvisory{
			TypedEvent: TypedEvent{
				Type: JSServerOutOfStorageAdvisoryType,
				ID:   nuid.Next(),
				Time: time.Now().UTC(),
			},
			Server:   s.Name(),
			ServerID: s.ID(),
			Stream:   stream,
			Cluster:  s.cachedClusterName(),
			Domain:   s.getOpts().JetStreamDomain,
		}
		s.publishAdvisory(nil, JSAdvisoryServerOutOfStorage, adv)
	}
}

// DisableJetStream will turn off JetStream and signals in clustered mode
// to have the metacontroller remove us from the peer list. Persistent
// meta-raft state on disk is removed. For transient runtime errors where
// the server should rejoin its existing meta group on restart, use
// ShutdownJetStream instead.
func (s *Server) DisableJetStream() error {
	return s.disableJetStream(true)
}

// ShutdownJetStream is like DisableJetStream but preserves persistent
// meta-raft state on disk so the server can rejoin the existing meta
// group on restart. Use for transient runtime errors that the operator
// is expected to fix before restarting.
func (s *Server) ShutdownJetStream() error {
	return s.disableJetStream(false)
}

func (s *Server) disableJetStream(deleteState bool) error {
	if !s.JetStreamEnabled() {
		return nil
	}

	s.setJetStreamDisabled()

	if s.JetStreamIsClustered() {
		isLeader := s.JetStreamIsLeader()
		js, cc := s.getJetStreamCluster()
		if js == nil {
			s.shutdownJetStream()
			return nil
		}
		js.mu.RLock()
		meta := cc.meta
		js.mu.RUnlock()

		if meta != nil {
			if isLeader {
				s.Warnf("JetStream initiating meta leader transfer")
				meta.StepDown()
				select {
				case <-s.quitCh:
					return nil
				case <-time.After(2 * time.Second):
				}
				if !s.JetStreamIsCurrent() {
					s.Warnf("JetStream timeout waiting for meta leader transfer")
				}
			}
			if deleteState {
				meta.Delete()
			} else {
				meta.Stop()
				meta.WaitForStop()
			}
		}
	}

	// Update our info status.
	s.updateJetStreamInfoStatus(false)

	// Normal shutdown.
	s.shutdownJetStream()

	// Shut down the RAFT groups.
	s.shutdownRaftNodes()

	return nil
}

func (s *Server) enableJetStreamAccounts() error {
	// Reuse the same task workers across all accounts, so that we don't explode
	// with a large number of goroutines on multi-account systems.
	tq := parallelTaskQueue(min(64, s.diskIOSemaphore().cap()))
	defer close(tq)

	// If we have no configured accounts setup then setup imports on global account.
	if s.globalAccountOnly() {
		gacc := s.GlobalAccount()
		gacc.mu.Lock()
		if len(gacc.jsLimits) == 0 {
			gacc.jsLimits = defaultJSAccountTiers
		}
		gacc.mu.Unlock()
		if err := s.configJetStream(gacc, tq); err != nil {
			return err
		}
	} else if err := s.configAllJetStreamAccounts(tq); err != nil {
		return fmt.Errorf("Error enabling jetstream on configured accounts: %v", err)
	}
	return nil
}

// enableAllJetStreamServiceImportsAndMappings turns on all service imports and mappings for jetstream for this account.
func (a *Account) enableAllJetStreamServiceImportsAndMappings() error {
	a.mu.RLock()
	s := a.srv
	a.mu.RUnlock()

	if s == nil {
		return fmt.Errorf("jetstream account not registered")
	}

	var dstAccName string
	if sacc := s.SystemAccount(); sacc != nil {
		dstAccName = sacc.Name
	}

	if !a.serviceImportExists(dstAccName, jsAllAPI) {
		// Capture si so we can turn on implicit sharing with JetStream layer.
		// Make sure to set "to" otherwise will incur performance slow down.
		si, err := a.addServiceImport(s.SystemAccount(), jsAllAPI, jsAllAPI, nil)
		if err != nil {
			return fmt.Errorf("Error setting up jetstream service imports for account: %v", err)
		}
		a.mu.Lock()
		si.share = true
		a.mu.Unlock()
	}

	// Check if we have a Domain specified.
	// If so add in a subject mapping that will allow local connected clients to reach us here as well.
	if opts := s.getOpts(); opts.JetStreamDomain != _EMPTY_ {
		mappings := generateJSMappingTable(opts.JetStreamDomain)
		a.mu.RLock()
		for _, m := range a.mappings {
			delete(mappings, m.src)
		}
		a.mu.RUnlock()
		for src, dest := range mappings {
			if err := a.AddMapping(src, dest); err != nil {
				s.Errorf("Error adding JetStream domain mapping: %v", err)
			}
		}
	}

	return nil
}

// enableJetStreamInfoServiceImportOnly will enable the single service import responder.
// Should we do them all regardless?
func (a *Account) enableJetStreamInfoServiceImportOnly() error {
	// Check if this import would be overshadowed. This can happen when accounts
	// are importing from another account for JS access.
	if a.serviceImportShadowed(JSApiAccountInfo) {
		return nil
	}

	return a.enableAllJetStreamServiceImportsAndMappings()
}

func (s *Server) configJetStream(acc *Account, tq chan<- func()) error {
	if acc == nil {
		return nil
	}
	acc.mu.RLock()
	jsLimits := acc.jsLimits
	acc.mu.RUnlock()
	if jsLimits != nil {
		// Check if already enabled. This can be during a reload.
		if acc.JetStreamEnabled() {
			if err := acc.enableAllJetStreamServiceImportsAndMappings(); err != nil {
				return err
			}
			if err := acc.UpdateJetStreamLimits(jsLimits); err != nil {
				return err
			}
		} else {
			if err := acc.EnableJetStream(jsLimits, tq); err != nil {
				return err
			}
			if s.gateway.enabled {
				s.switchAccountToInterestMode(acc.GetName())
			}
		}
	} else if acc != s.SystemAccount() {
		if acc.JetStreamEnabled() {
			acc.DisableJetStream()
		}
		// We will setup basic service imports to respond to
		// requests if JS is enabled for this account.
		if err := acc.enableJetStreamInfoServiceImportOnly(); err != nil {
			return err
		}
	}
	return nil
}

// configAllJetStreamAccounts walk all configured accounts and turn on jetstream if requested.
func (s *Server) configAllJetStreamAccounts(tq chan<- func()) error {
	// Check to see if system account has been enabled. We could arrive here via reload and
	// a non-default system account.
	s.checkJetStreamExports()

	// Bail if server not enabled. If it was enabled and a reload turns it off
	// that will be handled elsewhere.
	js := s.getJetStream()
	if js == nil {
		return nil
	}

	// Snapshot into our own list. Might not be needed.
	s.mu.RLock()
	if s.sys != nil {
		// clustered stream removal will perform this cleanup as well
		// this is mainly for initial cleanup
		saccName := s.sys.account.Name
		accStoreDirs, _ := os.ReadDir(js.config.StoreDir)
		for _, acc := range accStoreDirs {
			if accName := acc.Name(); accName != saccName {
				// no op if not empty
				accDir := filepath.Join(js.config.StoreDir, accName)
				os.Remove(filepath.Join(accDir, streamsDir))
				os.Remove(accDir)
			}
		}
	}

	var jsAccounts []*Account
	s.accounts.Range(func(k, v any) bool {
		jsAccounts = append(jsAccounts, v.(*Account))
		return true
	})
	accounts := &s.accounts
	s.mu.RUnlock()

	// Process any jetstream enabled accounts here. These will be accounts we are
	// already aware of at startup etc.
	for _, acc := range jsAccounts {
		if err := s.configJetStream(acc, tq); err != nil {
			return err
		}
	}

	// Now walk all the storage we have and resolve any accounts that we did not process already.
	// This is important in resolver/operator models.
	fis, _ := os.ReadDir(js.config.StoreDir)
	for _, fi := range fis {
		if accName := fi.Name(); accName != _EMPTY_ {
			// Only load up ones not already loaded since they are processed above.
			if _, ok := accounts.Load(accName); !ok {
				if acc, err := s.lookupAccount(accName); err != nil && acc != nil {
					if err := s.configJetStream(acc, tq); err != nil {
						return err
					}
				}
			}
		}
	}

	return nil
}

// Mark our started time.
func (js *jetStream) setStarted() {
	js.mu.Lock()
	defer js.mu.Unlock()
	js.started = time.Now()
}

func (js *jetStream) isEnabled() bool {
	if js == nil {
		return false
	}
	return !js.disabled.Load()
}

// Mark that we will be in standlone mode.
func (js *jetStream) setJetStreamStandAlone(isStandAlone bool) {
	if js == nil {
		return
	}
	js.mu.Lock()
	defer js.mu.Unlock()
	if js.standAlone = isStandAlone; js.standAlone {
		// Update our server atomic.
		js.srv.isMetaLeader.Store(true)
		js.accountPurge, _ = js.srv.systemSubscribe(JSApiAccountPurge, _EMPTY_, false, nil, js.srv.jsLeaderAccountPurgeRequest)
	} else if js.accountPurge != nil {
		js.srv.sysUnsubscribe(js.accountPurge)
	}
}

// JetStreamEnabled reports if jetstream is enabled for this server.
func (s *Server) JetStreamEnabled() bool {
	return s.getJetStream().isEnabled()
}

// JetStreamEnabledForDomain will report if any servers have JetStream enabled within this domain.
func (s *Server) JetStreamEnabledForDomain() bool {
	if s.JetStreamEnabled() {
		return true
	}

	var jsFound bool
	// If we are here we do not have JetStream enabled for ourselves, but we need to check all connected servers.
	// TODO(dlc) - Could optimize and memoize this.
	s.nodeToInfo.Range(func(k, v any) bool {
		// This should not be dependent on online status, so only check js.
		if v.(nodeInfo).js {
			jsFound = true
			return false
		}
		return true
	})

	return jsFound
}

// Will signal that all pull requests for consumers on this server are now invalid.
func (s *Server) signalPullConsumers() {
	js := s.getJetStream()
	if js == nil {
		return
	}

	js.mu.RLock()
	defer js.mu.RUnlock()

	// In case we have stale pending requests.
	const hdr = "NATS/1.0 409 Server Shutdown\r\n" + JSPullRequestPendingMsgs + ": %d\r\n" + JSPullRequestPendingBytes + ": %d\r\n\r\n"
	var didSend bool

	for _, jsa := range js.accounts {
		jsa.mu.RLock()
		for _, stream := range jsa.streams {
			stream.mu.RLock()
			for _, o := range stream.consumers {
				o.mu.RLock()
				// Only signal on R1.
				if o.cfg.Replicas <= 1 {
					for reply, wr := range o.pendingRequests() {
						shdr := fmt.Sprintf(hdr, wr.n, wr.b)
						o.outq.send(newJSPubMsg(reply, _EMPTY_, _EMPTY_, []byte(shdr), nil, nil, 0))
						didSend = true
					}
				}
				o.mu.RUnlock()
			}
			stream.mu.RUnlock()
		}
		jsa.mu.RUnlock()
	}
	// Give time for migration information to make it out of our server.
	if didSend {
		time.Sleep(50 * time.Millisecond)
	}
}

// Helper for determining if we are shutting down.
func (js *jetStream) isShuttingDown() bool {
	js.mu.RLock()
	defer js.mu.RUnlock()
	return js.shuttingDown
}

// Shutdown jetstream for this server.
func (s *Server) shutdownJetStream() {
	js := s.getJetStream()
	if js == nil {
		return
	}

	s.Noticef("Initiating JetStream Shutdown...")
	defer s.Noticef("JetStream Shutdown")

	// If we have folks blocked on sync requests, unblock.
	// Send 1 is enough, but use select in case they were all present.
	select {
	case s.syncOutSem <- struct{}{}:
	default:
	}

	var _a [512]*Account
	accounts := _a[:0]

	js.mu.Lock()
	// Collect accounts.
	for _, jsa := range js.accounts {
		if a := jsa.acc(); a != nil {
			accounts = append(accounts, a)
		}
	}
	accPurgeSub := js.accountPurge
	js.accountPurge = nil
	// Signal we are shutting down.
	js.shuttingDown = true
	var qch chan struct{}
	var stopped chan struct{}
	if cc := js.cluster; cc != nil && cc.qch != nil {
		qch, stopped = cc.qch, cc.stopped
		cc.qch, cc.stopped = nil, nil
	}
	js.mu.Unlock()

	if accPurgeSub != nil {
		s.sysUnsubscribe(accPurgeSub)
	}

	// If we were clustered signal the monitor cluster go routine.
	// We will wait for a bit for it to close, before removing the accounts' JetStream.
	// Do this without the lock.
	if qch != nil {
		close(qch) // Must be close() to signal *all* listeners
		select {
		case <-stopped:
		case <-time.After(10 * time.Second):
			s.Warnf("Did not receive signal for successful shutdown of cluster routine")
		}
	}

	for _, a := range accounts {
		a.removeJetStream()
	}

	s.js.Store(nil)

	js.mu.Lock()
	js.accounts = nil

	if cc := js.cluster; cc != nil {
		js.stopUpdatesSub()
		if cc.metaRescue != nil {
			s.sysUnsubscribe(cc.metaRescue)
			cc.metaRescue = nil
		}
		if cc.c != nil {
			cc.c.closeConnection(ClientClosed)
			cc.c = nil
		}
		cc.meta = nil
		// Set our atomic bool to false.
		s.jsClustered.Store(false)
	}
	js.mu.Unlock()
}

// JetStreamConfig will return the current config. Useful if the system
// created a dynamic configuration. A copy is returned.
func (s *Server) JetStreamConfig() *JetStreamConfig {
	var c *JetStreamConfig
	if js := s.getJetStream(); js != nil {
		js.mu.RLock()
		copy := js.config
		c = &(copy)
		js.mu.RUnlock()
	}
	return c
}

// StoreDir returns the current JetStream directory.
func (s *Server) StoreDir() string {
	js := s.getJetStream()
	if js == nil {
		return _EMPTY_
	}
	return js.config.StoreDir
}

// JetStreamNumAccounts returns the number of enabled accounts this server is tracking.
func (s *Server) JetStreamNumAccounts() int {
	js := s.getJetStream()
	if js == nil {
		return 0
	}
	js.mu.Lock()
	defer js.mu.Unlock()
	return len(js.accounts)
}

// JetStreamReservedResources returns the reserved resources if JetStream is enabled.
func (s *Server) JetStreamReservedResources() (int64, int64, error) {
	js := s.getJetStream()
	if js == nil {
		return -1, -1, NewJSNotEnabledForAccountError()
	}
	js.mu.RLock()
	defer js.mu.RUnlock()
	return js.memReserved, js.storeReserved, nil
}

func (s *Server) getJetStream() *jetStream {
	return s.js.Load()
}

func (a *Account) assignJetStreamLimits(limits map[string]JetStreamAccountLimits) {
	a.mu.Lock()
	a.jsLimits = limits
	a.mu.Unlock()
}

// EnableJetStream will enable JetStream on this account with the defined limits.
// This is a helper for JetStreamEnableAccount.
func (a *Account) EnableJetStream(limits map[string]JetStreamAccountLimits, tq chan<- func()) error {
	a.mu.RLock()
	s := a.srv
	a.mu.RUnlock()

	if s == nil {
		return fmt.Errorf("jetstream account not registered")
	}

	if s.SystemAccount() == a {
		return fmt.Errorf("jetstream can not be enabled on the system account")
	}

	s.mu.RLock()
	if s.sys == nil {
		s.mu.RUnlock()
		return ErrServerNotRunning
	}
	sendq := s.sys.sendq
	s.mu.RUnlock()

	// No limits means we dynamically set up limits.
	// We also place limits here so we know that the account is configured for JetStream.
	if len(limits) == 0 {
		limits = defaultJSAccountTiers
	}

	a.assignJetStreamLimits(limits)

	js := s.getJetStream()
	if js == nil {
		return NewJSNotEnabledError()
	}

	js.mu.Lock()
	// Accounts get reset to nil on shutdown, since we re-acquire the locks here, we need to check again.
	if js.accounts == nil {
		js.mu.Unlock()
		return NewJSNotEnabledError()
	}

	if jsa, ok := js.accounts[a.Name]; ok {
		a.mu.Lock()
		a.js = jsa
		a.mu.Unlock()
		js.mu.Unlock()
		return a.enableAllJetStreamServiceImportsAndMappings()
	}

	// Check the limits against existing reservations.
	if err := js.sufficientResources(limits); err != nil {
		js.mu.Unlock()
		return err
	}

	sysNode := s.Node()

	jsa := &jsAccount{js: js, account: a, limits: limits, streams: make(map[string]*stream), sendq: sendq, usage: make(map[string]*jsaStorage)}
	jsa.storeDir = filepath.Join(js.config.StoreDir, a.Name)

	// Make sure the account's streams directory exists before the account is
	// published, so that stream creation can't race with its setup. Do this
	// before starting the usage timer and subscription below, so there is
	// nothing to tear down if it fails.
	sdir := filepath.Join(jsa.storeDir, streamsDir)
	hasState := false
	if _, err := os.Stat(sdir); err == nil {
		hasState = true
	} else if os.IsNotExist(err) {
		if err := os.MkdirAll(sdir, defaultDirPerms); err != nil {
			js.mu.Unlock()
			return fmt.Errorf("could not create storage streams directory - %v", err)
		}
	} else {
		js.mu.Unlock()
		return fmt.Errorf("could not create storage streams directory - %v", err)
	}

	// A single server does not need to do the account updates at this point.
	if js.cluster != nil || !s.standAloneMode() {
		jsa.usageMu.Lock()
		jsa.utimer = time.AfterFunc(usageTick, jsa.sendClusterUsageUpdateTimer)
		// Cluster mode updates to resource usage. System internal prevents echos.
		jsa.updatesPub = fmt.Sprintf(jsaUpdatesPubT, a.Name, sysNode)
		jsa.updatesSub, _ = s.sysSubscribe(fmt.Sprintf(jsaUpdatesSubT, a.Name), jsa.remoteUpdateUsage)
		jsa.usageMu.Unlock()
	}

	js.accounts[a.Name] = jsa
	// Stamp inside account as well. Needs to be done under js's lock.
	a.mu.Lock()
	a.js = jsa
	a.mu.Unlock()
	js.mu.Unlock()

	// Create the proper imports here.
	if err := a.enableAllJetStreamServiceImportsAndMappings(); err != nil {
		return err
	}

	s.Debugf("Enabled JetStream for account %q", a.Name)
	if l, ok := limits[_EMPTY_]; ok {
		s.Debugf("  Max Memory:      %s", friendlyBytes(l.MaxMemory))
		s.Debugf("  Max Storage:     %s", friendlyBytes(l.MaxStore))
	} else {
		for t, l := range limits {
			s.Debugf("  Tier: %s", t)
			s.Debugf("    Max Memory:      %s", friendlyBytes(l.MaxMemory))
			s.Debugf("    Max Storage:     %s", friendlyBytes(l.MaxStore))
		}
	}

	// Clean up any old snapshots that were orphaned while staging.
	os.RemoveAll(filepath.Join(js.config.StoreDir, snapStagingDir))

	if hasState {
		// Restore any state here.
		s.Debugf("Recovering JetStream state for account %q", a.Name)
	}

	// Remember if we should be encrypted and what cipher we think we should use.
	encrypted := s.getOpts().JetStreamKey != _EMPTY_
	sc := s.getOpts().JetStreamCipher

	doConsumers := func(mset *stream, odir string) {
		ofis, _ := os.ReadDir(odir)
		if len(ofis) > 0 {
			s.Noticef("  Recovering %d consumers for stream - '%s > %s'", len(ofis), mset.accName(), mset.name())
		}
		for _, ofi := range ofis {
			metafile := filepath.Join(odir, ofi.Name(), JetStreamMetaFile)
			metasum := filepath.Join(odir, ofi.Name(), JetStreamMetaFileSum)
			if _, err := os.Stat(metafile); os.IsNotExist(err) {
				s.Warnf("    Missing consumer metafile %q", metafile)
				continue
			}
			buf, err := os.ReadFile(metafile)
			if err != nil {
				s.Warnf("    Error reading consumer metafile %q: %v", metafile, err)
				continue
			}
			if _, err := os.Stat(metasum); os.IsNotExist(err) {
				s.Warnf("    Missing consumer checksum for %q", metasum)
				continue
			}

			// Check if we are encrypted.
			if key, err := os.ReadFile(filepath.Join(odir, ofi.Name(), JetStreamMetaFileKey)); err == nil {
				s.Debugf("  Consumer metafile is encrypted, reading encrypted keyfile")
				// Decode the buffer before proceeding.
				ctxName := mset.name() + tsep + ofi.Name()
				nbuf, _, err := s.decryptMeta(sc, key, buf, a.Name, ctxName)
				if err != nil {
					s.Warnf("  Error decrypting our consumer metafile: %v", err)
					continue
				}
				buf = nbuf
			}

			var cfg FileConsumerInfo
			decoder := json.NewDecoder(bytes.NewReader(buf))
			decoder.DisallowUnknownFields()
			strictErr := decoder.Decode(&cfg)
			if strictErr != nil {
				cfg = FileConsumerInfo{}
				if err := json.Unmarshal(buf, &cfg); err != nil {
					s.Warnf("    Error unmarshalling consumer metafile %q: %v", metafile, err)
					continue
				}
			}
			if supported := supportsRequiredApiLevel(cfg.Metadata); !supported || strictErr != nil {
				var offlineReason string
				if !supported {
					apiLevel := getRequiredApiLevel(cfg.Metadata)
					if strictErr != nil {
						offlineReason = fmt.Sprintf("unsupported - config error: %s", strings.TrimPrefix(strictErr.Error(), "json: "))
					} else {
						offlineReason = fmt.Sprintf("unsupported - required API level: %s, current API level: %d", apiLevel, JSApiLevel)
					}
					s.Warnf("  Detected unsupported consumer '%s > %s > %s': %s", a.Name, mset.name(), cfg.Name, offlineReason)
				} else {
					offlineReason = fmt.Sprintf("decoding error: %v", strictErr)
					s.Warnf("  Error unmarshalling consumer metafile %q: %v", metafile, strictErr)
				}
				singleServerMode := !s.JetStreamIsClustered() && s.standAloneMode()
				if singleServerMode {
					if !mset.closed.Load() {
						s.Warnf("  Stopping unsupported stream '%s > %s'", a.Name, mset.name())
						mset.mu.Lock()
						mset.offlineReason = fmt.Sprintf("stopped - unsupported consumer %q", cfg.Name)
						mset.mu.Unlock()
						mset.stop(false, false)
					}

					// Fake a consumer, so we can respond to API requests as single-server.
					o := &consumer{
						mset:          mset,
						js:            s.getJetStream(),
						acc:           a,
						srv:           s,
						cfg:           cfg.ConsumerConfig,
						direct:        cfg.Direct,
						sourcing:      cfg.Sourcing,
						active:        false,
						stream:        mset.name(),
						name:          cfg.Name,
						dseq:          1,
						sseq:          1,
						created:       time.Now().UTC(),
						closed:        true,
						offlineReason: offlineReason,
					}
					if !cfg.Created.IsZero() {
						o.created = cfg.Created
					}

					mset.mu.Lock()
					mset.setConsumer(o)
					mset.mu.Unlock()
				}
				continue
			}

			isEphemeral := !isDurableConsumer(&cfg.ConsumerConfig)
			if isEphemeral {
				// This is an ephemeral consumer and this could fail on restart until
				// the consumer can reconnect. We will create it as a durable and switch it.
				cfg.ConsumerConfig.Durable = ofi.Name()
			}
			obs, err := mset.addConsumerWithAssignment(&cfg.ConsumerConfig, _EMPTY_, nil, true, ActionCreateOrUpdate, false)
			if err != nil {
				s.Warnf("    Error adding consumer '%s > %s > %s': %v", a.Name, mset.name(), cfg.Name, err)
				continue
			}
			if isEphemeral {
				obs.switchToEphemeral()
			}
			if !cfg.Created.IsZero() {
				obs.setCreatedTime(cfg.Created)
			}
		}
	}

	// Now recover the streams.
	fis, _ := os.ReadDir(sdir)
	doStream := func(fi os.DirEntry) error {
		plaintext := true
		mdir := filepath.Join(sdir, fi.Name())
		// Check for partially deleted streams. They are marked with "." prefix.
		if strings.HasPrefix(fi.Name(), tsep) {
			go os.RemoveAll(mdir)
			return nil
		}
		key := sha256.Sum256([]byte(fi.Name()))
		hh, err := highwayhash.NewDigest64(key[:])
		if err != nil {
			return err
		}
		metafile := filepath.Join(mdir, JetStreamMetaFile)
		metasum := filepath.Join(mdir, JetStreamMetaFileSum)
		if _, err := os.Stat(metafile); os.IsNotExist(err) {
			s.Warnf("  Missing stream metafile for %q", metafile)
			return nil
		}
		buf, err := os.ReadFile(metafile)
		if err != nil {
			s.Warnf("  Error reading metafile %q: %v", metafile, err)
			return nil
		}
		if _, err := os.Stat(metasum); os.IsNotExist(err) {
			s.Warnf("  Missing stream checksum file %q", metasum)
			return nil
		}
		sum, err := os.ReadFile(metasum)
		if err != nil {
			s.Warnf("  Error reading Stream metafile checksum %q: %v", metasum, err)
			return nil
		}
		hh.Write(buf)
		var hb [highwayhash.Size64]byte
		checksum := hex.EncodeToString(hh.Sum(hb[:0]))
		if checksum != string(sum) {
			s.Warnf("  Stream metafile %q: checksums do not match %q vs %q", metafile, sum, checksum)
			return nil
		}

		// Track if we are converting ciphers.
		var convertingCiphers bool

		// Check if we are encrypted.
		keyFile := filepath.Join(mdir, JetStreamMetaFileKey)
		keyBuf, err := os.ReadFile(keyFile)
		if err == nil {
			s.Debugf("  Stream metafile is encrypted, reading encrypted keyfile")
			if len(keyBuf) < minMetaKeySize {
				s.Warnf("  Bad stream encryption key length of %d", len(keyBuf))
				return nil
			}
			// Decode the buffer before proceeding.
			var nbuf []byte
			nbuf, convertingCiphers, err = s.decryptMeta(sc, keyBuf, buf, a.Name, fi.Name())
			if err != nil {
				s.Warnf("  Error decrypting our stream metafile: %v", err)
				return nil
			}
			buf = nbuf
			plaintext = false
		}

		var cfg FileStreamInfo
		decoder := json.NewDecoder(bytes.NewReader(buf))
		decoder.DisallowUnknownFields()
		strictErr := decoder.Decode(&cfg)
		if strictErr != nil {
			cfg = FileStreamInfo{}
			if err := json.Unmarshal(buf, &cfg); err != nil {
				s.Warnf("  Error unmarshalling stream metafile %q: %v", metafile, err)
				return nil
			}
		}
		if supported := supportsRequiredApiLevel(cfg.Metadata); !supported || strictErr != nil {
			var offlineReason string
			if !supported {
				apiLevel := getRequiredApiLevel(cfg.Metadata)
				if strictErr != nil {
					offlineReason = fmt.Sprintf("unsupported - config error: %s", strings.TrimPrefix(strictErr.Error(), "json: "))
				} else {
					offlineReason = fmt.Sprintf("unsupported - required API level: %s, current API level: %d", apiLevel, JSApiLevel)
				}
				s.Warnf("  Detected unsupported stream '%s > %s': %s", a.Name, cfg.StreamConfig.Name, offlineReason)
			} else {
				offlineReason = fmt.Sprintf("decoding error: %v", strictErr)
				s.Warnf("  Error unmarshalling stream metafile %q: %v", metafile, strictErr)
			}
			singleServerMode := !s.JetStreamIsClustered() && s.standAloneMode()
			if singleServerMode {
				// Fake a stream, so we can respond to API requests as single-server.
				mset := &stream{
					acc:           a,
					jsa:           jsa,
					cfg:           cfg.StreamConfig,
					js:            js,
					srv:           s,
					stype:         cfg.Storage,
					consumers:     make(map[string]*consumer),
					active:        false,
					created:       time.Now().UTC(),
					offlineReason: offlineReason,
				}
				if !cfg.Created.IsZero() {
					mset.created = cfg.Created
				}
				mset.closed.Store(true)

				jsa.mu.Lock()
				jsa.streams[cfg.Name] = mset
				jsa.mu.Unlock()

				// Now do the consumers.
				odir := filepath.Join(sdir, fi.Name(), consumerDir)
				doConsumers(mset, odir)
			}
			return nil
		}

		// We had a bug that set a default de dupe window on mirror, despite that being not a valid config
		fixCfgMirrorWithDedupWindow(&cfg.StreamConfig)

		// We had a bug that could allow subjects in that had prefix or suffix spaces. We check for that here
		// and will patch them on the fly for now. We will warn about them.
		var hadSubjErr bool
		for i, subj := range cfg.StreamConfig.Subjects {
			if !IsValidSubject(subj) {
				s.Warnf("  Detected bad subject %q while adding stream %q, will attempt to repair", subj, cfg.Name)
				if nsubj := strings.TrimSpace(subj); IsValidSubject(nsubj) {
					s.Warnf("  Bad subject %q repaired to %q", subj, nsubj)
					cfg.StreamConfig.Subjects[i] = nsubj
				} else {
					s.Warnf("  Error recreating stream %q: %v", cfg.Name, "invalid subject")
					hadSubjErr = true
					break
				}
			}
		}
		if hadSubjErr {
			return nil
		}

		// The other possible bug is assigning subjects to mirrors, so check for that and patch as well.
		if cfg.StreamConfig.Mirror != nil && len(cfg.StreamConfig.Subjects) > 0 {
			s.Warnf("  Detected subjects on a mirrored stream %q, will remove", cfg.Name)
			cfg.StreamConfig.Subjects = nil
		}

		s.Noticef("  Starting restore for stream '%s > %s'", a.Name, cfg.StreamConfig.Name)
		rt := time.Now()

		// Log if we are converting from plaintext to encrypted.
		if encrypted {
			if plaintext {
				s.Noticef("  Encrypting stream '%s > %s'", a.Name, cfg.StreamConfig.Name)
			} else if convertingCiphers {
				s.Noticef("  Converting to %s for stream '%s > %s'", sc, a.Name, cfg.StreamConfig.Name)
				// Remove the key file to have system regenerate with the new cipher.
				os.Remove(keyFile)
			}
		}

		// Add in the stream.
		mset, err := a.recoverStream(&cfg.StreamConfig)
		if err != nil {
			s.Warnf("  Error recreating stream %q: %v", cfg.Name, err)
			// If we removed a keyfile from above make sure to put it back.
			if convertingCiphers {
				err := os.WriteFile(keyFile, keyBuf, defaultFilePerms)
				if err != nil {
					s.Warnf("  Error replacing meta keyfile for stream %q: %v", cfg.Name, err)
				}
			}
			return nil
		}
		if !cfg.Created.IsZero() {
			mset.setCreatedTime(cfg.Created)
		}

		// Might need to recover from a partial batch write, but only if a single replica stream.
		if cfg.AllowAtomicPublish && cfg.Replicas == 1 {
			var (
				ok            bool
				smv           StoreMsg
				batchId       string
				batchSeq      uint64
				commit        bool
				commitEob     bool
				batchStoreDir string
				store         StreamStore
				state         StreamState
			)
			// Check if the last message was part of a batch.
			sm, err := mset.store.LoadLastMsg(fwcs, &smv)
			if err != nil || sm == nil {
				goto SKIP
			}
			batchId = getBatchId(sm.hdr)
			batchSeq, ok = getBatchSequence(sm.hdr)
			commit = len(sliceHeader(JSBatchCommit, sm.hdr)) != 0
			if batchId == _EMPTY_ || !ok || commit {
				goto SKIP
			}
			// We've observed a partial batch write. Write the remainder of the batch.
			batchSeq++
			_, batchStoreDir = getBatchStoreDir(jsa.storeDir, cfg.Name, batchId)
			if _, err = os.Stat(batchStoreDir); err != nil {
				s.Errorf("  Failed restoring partial batch write for stream '%s > %s' at sequence %d: %v",
					mset.accName(), mset.name(), batchSeq, err)
				goto SKIP
			}
			store, err = newBatchStore(mset, batchId, cfg.Replicas, cfg.Storage, jsa.storeDir, cfg.Name)
			if err != nil {
				s.Errorf("  Failed restoring partial batch write for stream '%s > %s' at sequence %d: %v",
					mset.accName(), mset.name(), batchSeq, err)
				goto SKIP
			}
			store.FastState(&state)
			sm, err = store.LoadMsg(state.LastSeq, &smv)
			if err != nil || sm == nil {
				s.Errorf("  Failed restoring partial batch write for stream '%s > %s' at sequence %d: last msg not found %d",
					mset.accName(), mset.name(), batchSeq, state.LastSeq)
				goto SKIP
			}
			commitEob = bytes.Equal(sliceHeader(JSBatchCommit, sm.hdr), []byte("eob"))
			// If the commit ends with an "End Of Batch" message, we don't store this.
			if commitEob {
				state.LastSeq--
			}
			s.Noticef("  Restoring partial batch write for stream '%s > %s' (seq %d to %d)",
				mset.accName(), mset.name(), batchSeq, state.LastSeq)
			// Loop through items that weren't persisted yet.
			for seq := batchSeq; seq <= state.LastSeq; seq++ {
				sm, err = store.LoadMsg(seq, &smv)
				if err != nil || sm == nil {
					s.Errorf("  Failed restoring partial batch write for stream '%s > %s' at sequence %d: %v",
						mset.accName(), mset.name(), seq, err)
					break
				}
				hdr := sm.hdr
				// If committed by EOB, the last message must get the normal commit header.
				if commitEob && seq == state.LastSeq {
					hdr = genHeader(hdr, JSBatchCommit, "1")
				}
				mset.processJetStreamMsg(sm.subj, _EMPTY_, hdr, sm.msg, 0, 0, nil, false, true)
			}
			store.Delete(true)
		SKIP:
			os.RemoveAll(filepath.Join(sdir, fi.Name(), batchesDir))
		}

		state := mset.state()
		s.Noticef("  Restored %s messages for stream '%s > %s' in %v",
			comma(int64(state.Msgs)), mset.accName(), mset.name(), time.Since(rt).Round(time.Millisecond))

		// Now do the consumers.
		odir := filepath.Join(sdir, fi.Name(), consumerDir)
		doConsumers(mset, odir)

		// Collect to check for dangling messages.
		// TODO(dlc) - Can be removed eventually.
		if cfg.StreamConfig.Retention == InterestPolicy {
			mset.checkForOrphanMsgs()
			mset.checkConsumerReplication()
		}

		return nil
	}

	if tq != nil {
		// If a parallelTaskQueue was provided then use that for concurrency.
		var wg sync.WaitGroup
		wg.Add(len(fis))
		for _, fi := range fis {
			tq <- func() {
				doStream(fi)
				wg.Done()
			}
		}
		wg.Wait()
	} else {
		// No parallelTaskQueue provided, do inline as before.
		for _, fi := range fis {
			doStream(fi)
		}
	}

	// Make sure to cleanup any old remaining snapshots.
	os.RemoveAll(filepath.Join(jsa.storeDir, snapsDir))

	s.Debugf("JetStream state for account %q recovered", a.Name)

	return nil
}

// Return whether we require MaxBytes to be set and if > 0 an upper limit for stream size exists
// Both limits are independent of each other.
func (a *Account) maxBytesLimits(cfg *StreamConfig) (bool, int64) {
	a.mu.RLock()
	jsa := a.js
	a.mu.RUnlock()
	if jsa == nil {
		return false, 0
	}
	jsa.usageMu.RLock()
	var replicas int
	if cfg != nil {
		replicas = cfg.Replicas
	}
	selectedLimits, _, ok := jsa.selectLimits(replicas)
	jsa.usageMu.RUnlock()
	if !ok {
		return false, 0
	}
	maxStreamBytes := int64(0)
	if cfg.Storage == MemoryStorage {
		maxStreamBytes = selectedLimits.MemoryMaxStreamBytes
	} else {
		maxStreamBytes = selectedLimits.StoreMaxStreamBytes
	}
	return selectedLimits.MaxBytesRequired, maxStreamBytes
}

// NumStreams will return how many streams we have.
func (a *Account) numStreams() int {
	a.mu.RLock()
	jsa := a.js
	a.mu.RUnlock()
	if jsa == nil {
		return 0
	}
	jsa.mu.Lock()
	n := len(jsa.streams)
	jsa.mu.Unlock()
	return n
}

// Streams will return all known streams.
func (a *Account) streams() []*stream {
	return a.filteredStreams(_EMPTY_)
}

func (a *Account) filteredStreams(filter string) []*stream {
	a.mu.RLock()
	jsa := a.js
	a.mu.RUnlock()

	if jsa == nil {
		return nil
	}

	jsa.mu.RLock()
	defer jsa.mu.RUnlock()

	var msets []*stream
	for _, mset := range jsa.streams {
		if filter != _EMPTY_ {
			mset.cfgMu.RLock()
			for _, subj := range mset.cfg.Subjects {
				if SubjectsCollide(filter, subj) {
					msets = append(msets, mset)
					break
				}
			}
			mset.cfgMu.RUnlock()
		} else {
			msets = append(msets, mset)
		}
	}

	return msets
}

// lookupStream will lookup a stream by name.
func (a *Account) lookupStream(name string) (*stream, error) {
	a.mu.RLock()
	jsa := a.js
	a.mu.RUnlock()

	if jsa == nil {
		return nil, NewJSNotEnabledForAccountError()
	}
	jsa.mu.RLock()
	defer jsa.mu.RUnlock()

	mset, ok := jsa.streams[name]
	if !ok {
		return nil, NewJSStreamNotFoundError()
	}
	return mset, nil
}

// UpdateJetStreamLimits will update the account limits for a JetStream enabled account.
func (a *Account) UpdateJetStreamLimits(limits map[string]JetStreamAccountLimits) error {
	a.mu.RLock()
	s, jsa := a.srv, a.js
	a.mu.RUnlock()

	if s == nil {
		return fmt.Errorf("jetstream account not registered")
	}
	js := s.getJetStream()
	if js == nil {
		return NewJSNotEnabledError()
	}
	if jsa == nil {
		return NewJSNotEnabledForAccountError()
	}

	if len(limits) == 0 {
		limits = defaultJSAccountTiers
	}

	// Calculate the delta between what we have and what we want.
	jsa.usageMu.RLock()
	dl := diffCheckedLimits(jsa.limits, limits)
	jsa.usageMu.RUnlock()

	js.mu.Lock()
	// Check the limits against existing reservations.
	if err := js.sufficientResources(dl); err != nil {
		js.mu.Unlock()
		return err
	}
	js.mu.Unlock()

	// Update
	jsa.usageMu.Lock()
	jsa.limits = limits
	jsa.usageMu.Unlock()

	return nil
}

func diffCheckedLimits(a, b map[string]JetStreamAccountLimits) map[string]JetStreamAccountLimits {
	diff := map[string]JetStreamAccountLimits{}
	for t, la := range a {
		// in a, not in b will return 0
		lb := b[t]
		diff[t] = JetStreamAccountLimits{
			MaxMemory: lb.MaxMemory - la.MaxMemory,
			MaxStore:  lb.MaxStore - la.MaxStore,
		}
	}
	for t, lb := range b {
		if la, ok := a[t]; !ok {
			// only in b not in a. (in a and b already covered)
			diff[t] = JetStreamAccountLimits{
				MaxMemory: lb.MaxMemory - la.MaxMemory,
				MaxStore:  lb.MaxStore - la.MaxStore,
			}
		}
	}
	return diff
}

// Return reserved bytes for memory and file store streams for this account on this server.
// Lock should be held.
func (jsa *jsAccount) reservedStorage(tier string) (mem, store uint64) {
	for _, mset := range jsa.streams {
		mset.cfgMu.RLock()
		storage, replicas, maxBytes := mset.cfg.Storage, mset.cfg.Replicas, mset.cfg.MaxBytes
		mset.cfgMu.RUnlock()
		if (tier == _EMPTY_ || tier == tierName(replicas)) && maxBytes > 0 {
			switch storage {
			case FileStorage:
				store += uint64(maxBytes)
			case MemoryStorage:
				mem += uint64(maxBytes)
			}
		}
	}
	return mem, store
}

// Return reserved bytes for memory and file store streams for this account in clustered mode.
// js lock should be held.
func reservedStorage(sas map[string]*streamAssignment, tier string) (mem, store uint64) {
	for _, sa := range sas {
		cfg := sa.Config
		if (tier == _EMPTY_ || tier == tierName(cfg.Replicas)) && cfg.MaxBytes > 0 {
			switch cfg.Storage {
			case FileStorage:
				store += uint64(cfg.MaxBytes)
			case MemoryStorage:
				mem += uint64(cfg.MaxBytes)
			}
		}
	}
	return mem, store
}

// JetStreamUsage reports on JetStream usage and limits for an account.
func (a *Account) JetStreamUsage() JetStreamAccountStats {
	a.mu.RLock()
	jsa, aname := a.js, a.Name
	accJsLimits := a.jsLimits
	a.mu.RUnlock()

	var stats JetStreamAccountStats
	if jsa != nil {
		js := jsa.js
		js.mu.RLock()
		cc := js.cluster
		singleServer := cc == nil
		jsa.mu.RLock()
		jsa.usageMu.RLock()
		stats.Memory, stats.Store = jsa.storageTotals()
		stats.Domain = js.config.Domain
		stats.API = JetStreamAPIStats{
			Level:  JSApiLevel,
			Total:  jsa.apiTotal,
			Errors: jsa.apiErrors,
		}
		if singleServer {
			stats.ReservedMemory, stats.ReservedStore = jsa.reservedStorage(_EMPTY_)
		} else {
			stats.ReservedMemory, stats.ReservedStore = reservedStorage(cc.streams[aname], _EMPTY_)
		}
		l, defaultTier := jsa.limits[_EMPTY_]
		if defaultTier {
			stats.Limits = l
		} else {
			skipped := 0
			stats.Tiers = make(map[string]JetStreamTier)
			for t, total := range jsa.usage {
				if _, ok := jsa.limits[t]; !ok && (*total) == (jsaStorage{}) {
					// skip tiers not present that don't contain a count
					// In case this shows an empty stream, that tier will be added when iterating over streams
					skipped++
				} else {
					tier := JetStreamTier{
						Memory: uint64(total.total.mem),
						Store:  uint64(total.total.store),
						Limits: jsa.limits[t],
					}
					if singleServer {
						tier.ReservedMemory, tier.ReservedStore = jsa.reservedStorage(t)
					} else {
						tier.ReservedMemory, tier.ReservedStore = reservedStorage(cc.streams[aname], t)
					}
					stats.Tiers[t] = tier
				}
			}
			if len(accJsLimits) != len(jsa.usage)-skipped {
				// insert unused limits
				for t, lim := range accJsLimits {
					if _, ok := stats.Tiers[t]; !ok {
						tier := JetStreamTier{Limits: lim}
						if singleServer {
							tier.ReservedMemory, tier.ReservedStore = jsa.reservedStorage(t)
						} else {
							tier.ReservedMemory, tier.ReservedStore = reservedStorage(cc.streams[aname], t)
						}
						stats.Tiers[t] = tier
					}
				}
			}
		}
		jsa.usageMu.RUnlock()

		// Clustered
		if cc := js.cluster; cc != nil {
			sas := cc.streams[aname]
			if defaultTier {
				stats.Streams = len(sas)
				stats.ReservedMemory, stats.ReservedStore = reservedStorage(sas, _EMPTY_)
			}
			for _, sa := range sas {
				if defaultTier {
					stats.Consumers += len(sa.consumers)
				} else {
					stats.Streams++
					streamTier := tierName(sa.Config.Replicas)
					su, ok := stats.Tiers[streamTier]
					if !ok {
						su = JetStreamTier{}
					}
					su.Streams++
					stats.Tiers[streamTier] = su

					// Now consumers, check each since could be different tiers.
					for _, ca := range sa.consumers {
						stats.Consumers++
						consumerTier := tierName(ca.Config.replicas(sa.Config))
						cu, ok := stats.Tiers[consumerTier]
						if !ok {
							cu = JetStreamTier{}
						}
						cu.Consumers++
						stats.Tiers[consumerTier] = cu
					}
				}
			}
		} else {
			if defaultTier {
				stats.Streams = len(jsa.streams)
			}
			for _, mset := range jsa.streams {
				consCount := mset.numConsumers()
				stats.Consumers += consCount
				if !defaultTier {
					u, ok := stats.Tiers[mset.tier]
					if !ok {
						u = JetStreamTier{}
					}
					u.Streams++
					stats.Streams++
					u.Consumers += consCount
					stats.Tiers[mset.tier] = u
				}
			}
		}
		jsa.mu.RUnlock()
		js.mu.RUnlock()
	}
	return stats
}

// DisableJetStream will disable JetStream for this account.
func (a *Account) DisableJetStream() error {
	return a.removeJetStream()
}

// removeJetStream is called when JetStream has been disabled for this account.
func (a *Account) removeJetStream() error {
	a.mu.Lock()
	s := a.srv
	a.js = nil
	a.mu.Unlock()

	if s == nil {
		return fmt.Errorf("jetstream account not registered")
	}

	js := s.getJetStream()
	if js == nil {
		return NewJSNotEnabledForAccountError()
	}

	return js.disableJetStream(js.lookupAccount(a))
}

// Disable JetStream for the account.
func (js *jetStream) disableJetStream(jsa *jsAccount) error {
	if jsa == nil || jsa.account == nil {
		return NewJSNotEnabledForAccountError()
	}

	js.mu.Lock()
	delete(js.accounts, jsa.account.Name)
	js.mu.Unlock()

	jsa.delete()

	return nil
}

// jetStreamConfigured reports whether the account has JetStream configured, regardless of this
// servers JetStream status.
func (a *Account) jetStreamConfigured() bool {
	if a == nil {
		return false
	}
	a.mu.RLock()
	defer a.mu.RUnlock()
	return len(a.jsLimits) > 0
}

// JetStreamEnabled is a helper to determine if jetstream is enabled for an account.
func (a *Account) JetStreamEnabled() bool {
	if a == nil {
		return false
	}
	a.mu.RLock()
	enabled := a.js != nil
	a.mu.RUnlock()
	return enabled
}

func (jsa *jsAccount) removeRemoteUsage(rnode string) {
	jsa.usageMu.Lock()
	defer jsa.usageMu.Unlock()

	rUsage := jsa.rusage[rnode]
	if rUsage == nil {
		return
	}
	for tierName, usage := range rUsage.tiers {
		if total := jsa.usage[tierName]; total != nil {
			total.total.mem -= usage.mem
			total.total.store -= usage.store
		}
	}
	jsa.apiTotal -= rUsage.api
	jsa.apiErrors -= rUsage.err
	delete(jsa.rusage, rnode)
}

func (jsa *jsAccount) remoteUpdateUsage(sub *subscription, c *client, _ *Account, subject, _ string, msg []byte) {
	// jsa.js.srv is immutable and guaranteed to no be nil, so no lock needed.
	s := jsa.js.srv

	if len(msg) < minUsageUpdateLen {
		s.Warnf("Ignoring remote usage update with size too short")
		return
	}
	var rnode string
	if li := strings.LastIndexByte(subject, btsep); li > 0 && li < len(subject) {
		rnode = subject[li+1:]
	}
	if rnode == _EMPTY_ {
		s.Warnf("Received remote usage update with no remote node")
		return
	}

	// Capture the meta group before the usage lock so we do not invert lock ordering.
	meta := jsa.js.getMetaGroup()
	jsa.usageMu.Lock()
	defer jsa.usageMu.Unlock()

	rUsage, ok := jsa.rusage[rnode]
	if !ok {
		// Once a server has been removed from the meta group, a usage update that
		// was already in flight must not recreate its remote usage entry. Only do
		// the membership check for new entries; steady-state updates avoid it.
		if meta != nil {
			var current bool
			for _, peer := range meta.VotingPeerNames() {
				if peer == rnode {
					current = true
					break
				}
			}
			if !current {
				return
			}
		}
		if jsa.rusage == nil {
			jsa.rusage = make(map[string]*remoteUsage)
		}
		rUsage = &remoteUsage{tiers: make(map[string]*jsaUsage)}
		jsa.rusage[rnode] = rUsage
	}
	updateTotal := func(tierName string, memUsed, storeUsed int64) {
		total, ok := jsa.usage[tierName]
		if !ok {
			total = &jsaStorage{}
			jsa.usage[tierName] = total
		}
		// Update the usage for this remote.
		if usage := rUsage.tiers[tierName]; usage != nil {
			// Decrement our old values.
			total.total.mem -= usage.mem
			total.total.store -= usage.store
			usage.mem, usage.store = memUsed, storeUsed
		} else {
			rUsage.tiers[tierName] = &jsaUsage{memUsed, storeUsed}
		}
		total.total.mem += memUsed
		total.total.store += storeUsed
	}

	var le = binary.LittleEndian
	apiTotal, apiErrors := le.Uint64(msg[16:]), le.Uint64(msg[24:])
	memUsed, storeUsed := int64(le.Uint64(msg[0:])), int64(le.Uint64(msg[8:]))

	// We later extended the data structure to support multiple tiers
	var excessRecordCnt uint32
	var tierName string

	if len(msg) >= usageMultiTiersLen {
		excessRecordCnt = le.Uint32(msg[minUsageUpdateLen:])
		length := le.Uint64(msg[minUsageUpdateLen+4:])
		// Need to protect past this point in case this is wrong.
		if length > uint64(len(msg))-usageMultiTiersLen {
			s.Warnf("Received corrupt remote usage update")
			return
		}
		tierName = string(msg[usageMultiTiersLen : usageMultiTiersLen+length])
		msg = msg[usageMultiTiersLen+length:]
	}
	updateTotal(tierName, memUsed, storeUsed)
	for ; excessRecordCnt > 0 && len(msg) >= usageRecordLen; excessRecordCnt-- {
		memUsed, storeUsed := int64(le.Uint64(msg[0:])), int64(le.Uint64(msg[8:]))
		length := le.Uint64(msg[16:])
		if length > uint64(len(msg))-usageRecordLen {
			s.Warnf("Received corrupt remote usage update on excess record")
			return
		}
		tierName = string(msg[usageRecordLen : usageRecordLen+length])
		msg = msg[usageRecordLen+length:]
		updateTotal(tierName, memUsed, storeUsed)
	}
	jsa.apiTotal -= rUsage.api
	jsa.apiErrors -= rUsage.err
	rUsage.api = apiTotal
	rUsage.err = apiErrors
	jsa.apiTotal += apiTotal
	jsa.apiErrors += apiErrors
}

// When we detect a skew of some sort this will verify the usage reporting is correct.
// No locks should be held.
func (jsa *jsAccount) checkAndSyncUsage(tierName string, storeType StorageType) {
	// This will run in a separate go routine, so check that we are only running once.
	if !jsa.sync.CompareAndSwap(false, true) {
		return
	}
	defer jsa.sync.Store(false)

	// Hold the account read lock and the usage lock while we calculate.
	// We scope by tier and storage type, but if R3 File has 200 streams etc. could
	// show a pause. I did test with > 100 non-active streams and was 80-200ns or so.
	// Should be rare this gets called as well.
	jsa.mu.RLock()
	defer jsa.mu.RUnlock()
	js := jsa.js
	if js == nil {
		return
	}
	s := js.srv

	// We need to collect the stream stores before we acquire the usage lock since in storeUpdates the
	// stream lock could be held if deletion are inline with storing a new message, e.g. via limits.
	var stores []StreamStore
	for _, mset := range jsa.streams {
		mset.mu.RLock()
		if mset.tier == tierName && mset.stype == storeType && mset.store != nil {
			stores = append(stores, mset.store)
		}
		mset.mu.RUnlock()
	}

	// Now range and qualify, hold usage lock to prevent updates.
	jsa.usageMu.Lock()
	defer jsa.usageMu.Unlock()

	usage, ok := jsa.usage[tierName]
	if !ok {
		return
	}

	// Collect current total for all stream stores that matched.
	var total int64
	var state StreamState
	for _, store := range stores {
		store.FastState(&state)
		total += int64(state.Bytes)
	}

	var needClusterUpdate bool
	// If we do not match on our calculations compute delta and adjust.
	if storeType == MemoryStorage {
		if total != usage.local.mem {
			s.Warnf("MemStore usage drift of %v vs %v detected for account %q",
				friendlyBytes(total), friendlyBytes(usage.local.mem), jsa.account.GetName())
			delta := total - usage.local.mem
			usage.local.mem += delta
			usage.total.mem += delta
			atomic.AddInt64(&js.memUsed, delta)
			needClusterUpdate = true
		}
	} else {
		if total != usage.local.store {
			s.Warnf("FileStore usage drift of %v vs %v detected for account %q",
				friendlyBytes(total), friendlyBytes(usage.local.store), jsa.account.GetName())
			delta := total - usage.local.store
			usage.local.store += delta
			usage.total.store += delta
			atomic.AddInt64(&js.storeUsed, delta)
			needClusterUpdate = true
		}
	}

	// Publish our local updates if in clustered mode.
	if needClusterUpdate && js.isClusteredNoLock() {
		jsa.sendClusterUsageUpdate()
	}
}

// Updates accounting on in use memory and storage. This is called from locally
// by the lower storage layers.
func (jsa *jsAccount) updateUsage(tierName string, storeType StorageType, delta int64) {
	// jsa.js is immutable and cannot be nil, so ok w/o lock.
	js := jsa.js
	// updateUsage() may be invoked under the mset's lock, so we can't get
	// the js' lock to check if clustered. So use this function that make
	// use of an atomic to do the check without having data race reports.
	isClustered := js.isClusteredNoLock()

	var needsCheck bool
	jsa.usageMu.Lock()
	s, ok := jsa.usage[tierName]
	if !ok {
		s = &jsaStorage{}
		jsa.usage[tierName] = s
	}
	if storeType == MemoryStorage {
		s.local.mem += delta
		s.total.mem += delta
		atomic.AddInt64(&js.memUsed, delta)
		needsCheck = s.local.mem < 0
	} else {
		s.local.store += delta
		s.total.store += delta
		atomic.AddInt64(&js.storeUsed, delta)
		needsCheck = s.local.store < 0
	}
	// Publish our local updates if in clustered mode.
	if isClustered {
		jsa.sendClusterUsageUpdate()
	}
	jsa.usageMu.Unlock()

	if needsCheck {
		// We could be holding the stream lock from up in the stack, and this
		// will want the jsa lock, which would violate locking order.
		// So do this in a Go routine. The function will check if it is already running.
		go jsa.checkAndSyncUsage(tierName, storeType)
	}
}

var usageTick = 1500 * time.Millisecond

func (jsa *jsAccount) sendClusterUsageUpdateTimer() {
	jsa.usageMu.Lock()
	defer jsa.usageMu.Unlock()
	jsa.sendClusterUsageUpdate()
	if jsa.utimer != nil {
		jsa.utimer.Reset(usageTick)
	}
}

// For usage fields.
const (
	minUsageUpdateLen    = 32
	stackUsageUpdate     = 72
	usageRecordLen       = 24
	usageMultiTiersLen   = 44
	apiStatsAndNumTiers  = 20
	minUsageUpdateWindow = 250 * time.Millisecond
)

// Send updates to our account usage for this server.
// jsa.usageMu lock should be held.
func (jsa *jsAccount) sendClusterUsageUpdate() {
	// These values are absolute so we can limit send rates.
	now := time.Now()
	if now.Sub(jsa.lupdate) < minUsageUpdateWindow {
		return
	}
	jsa.lupdate = now

	lenUsage := len(jsa.usage)
	if lenUsage == 0 {
		return
	}
	// every base record contains mem/store/len(tier) as well as the tier name
	l := usageRecordLen * lenUsage
	for tier := range jsa.usage {
		l += len(tier)
	}
	// first record contains api/usage errors as well as count for extra base records
	l += apiStatsAndNumTiers

	var raw [stackUsageUpdate]byte
	var b []byte
	if l > stackUsageUpdate {
		b = make([]byte, l)
	} else {
		b = raw[:l]
	}

	var i int
	var le = binary.LittleEndian
	for tier, usage := range jsa.usage {
		le.PutUint64(b[i+0:], uint64(usage.local.mem))
		le.PutUint64(b[i+8:], uint64(usage.local.store))
		if i == 0 {
			le.PutUint64(b[16:], jsa.usageApi)
			le.PutUint64(b[24:], jsa.usageErr)
			le.PutUint32(b[32:], uint32(len(jsa.usage)-1))
			le.PutUint64(b[36:], uint64(len(tier)))
			copy(b[usageMultiTiersLen:], tier)
			i = usageMultiTiersLen + len(tier)
		} else {
			le.PutUint64(b[i+16:], uint64(len(tier)))
			copy(b[i+usageRecordLen:], tier)
			i += usageRecordLen + len(tier)
		}
	}
	jsa.sendq.push(newPubMsg(nil, jsa.updatesPub, _EMPTY_, nil, nil, b, noCompression, false, false))
}

func (js *jetStream) wouldExceedLimits(storeType StorageType, sz int) bool {
	var (
		total *int64
		max   *int64
	)
	if storeType == MemoryStorage {
		total, max = &js.memUsed, &js.memMax
	} else {
		total, max = &js.storeUsed, &js.storeMax
	}
	return (atomic.LoadInt64(total) + int64(sz)) > atomic.LoadInt64(max)
}

func (js *jetStream) limitsExceeded(storeType StorageType) bool {
	return js.wouldExceedLimits(storeType, 0)
}

func tierName(replicas int) string {
	// TODO (mh) this is where we could select based off a placement tag as well "qos:tier"
	if replicas == 0 {
		replicas = 1
	}
	return fmt.Sprintf("R%d", replicas)
}

func isSameTier(replicasA, replicasB int) bool {
	a := max(1, replicasA)
	b := max(1, replicasB)
	// TODO (mh) this is where we could select based off a placement tag as well "qos:tier"
	return a == b
}

func (jsa *jsAccount) jetStreamAndClustered() (*jetStream, bool) {
	jsa.mu.RLock()
	js := jsa.js
	jsa.mu.RUnlock()
	return js, js.isClustered()
}

// jsa.usageMu read lock should be held.
func (jsa *jsAccount) selectLimits(replicas int) (JetStreamAccountLimits, string, bool) {
	if selectedLimits, ok := jsa.limits[_EMPTY_]; ok {
		return selectedLimits, _EMPTY_, true
	}
	tier := tierName(replicas)
	if selectedLimits, ok := jsa.limits[tier]; ok {
		return selectedLimits, tier, true
	}
	return JetStreamAccountLimits{}, _EMPTY_, false
}

// Lock should be held.
func (jsa *jsAccount) countStreams(tier string, cfg *StreamConfig) (streams int) {
	for _, mset := range jsa.streams {
		mset.cfgMu.RLock()
		name, replicas := mset.cfg.Name, mset.cfg.Replicas
		mset.cfgMu.RUnlock()
		// Don't count the stream toward the limit if it already exists.
		if (tier == _EMPTY_ || isSameTier(replicas, cfg.Replicas)) && name != cfg.Name {
			streams++
		}
	}
	return streams
}

// jsa.usageMu read lock (at least) should be held.
func (jsa *jsAccount) storageTotals() (uint64, uint64) {
	mem := uint64(0)
	store := uint64(0)
	for _, sa := range jsa.usage {
		mem += uint64(sa.total.mem)
		store += uint64(sa.total.store)
	}
	return mem, store
}

func (jsa *jsAccount) limitsExceeded(storeType StorageType, tierName string, replicas int) (bool, *ApiError) {
	return jsa.wouldExceedLimits(storeType, tierName, replicas, _EMPTY_, nil, nil)
}

func (jsa *jsAccount) wouldExceedLimits(storeType StorageType, tierName string, replicas int, subj string, hdr, msg []byte) (bool, *ApiError) {
	jsa.usageMu.RLock()
	defer jsa.usageMu.RUnlock()

	selectedLimits, ok := jsa.limits[tierName]
	if !ok {
		return true, NewJSNoLimitsError()
	}
	inUse := jsa.usage[tierName]
	if inUse == nil {
		// Imply totals of 0
		return false, nil
	}
	r := int64(replicas)
	// Make sure replicas is correct.
	if r < 1 {
		r = 1
	}
	// This is for limits. If we have no tier, consider all to be flat, vs tiers like R3 where we want to scale limit by replication.
	lr := r
	if tierName == _EMPTY_ {
		lr = 1
	}

	// Since tiers are flat we need to scale limit up by replicas when checking.
	if storeType == MemoryStorage {
		totalMem := inUse.total.mem + (int64(memStoreMsgSize(subj, hdr, msg)) * r)
		if selectedLimits.MaxMemory >= 0 && totalMem > selectedLimits.MaxMemory*lr {
			return true, nil
		}
	} else {
		totalStore := inUse.total.store + (int64(fileStoreMsgSize(subj, hdr, msg)) * r)
		if selectedLimits.MaxStore >= 0 && totalStore > selectedLimits.MaxStore*lr {
			return true, nil
		}
	}

	return false, nil
}

// Check account limits.
// Read Lock should be held
func (js *jetStream) checkAccountLimits(selected *JetStreamAccountLimits, tier string, config *StreamConfig, currentRes int64) error {
	return js.checkLimits(selected, tier, config, false, currentRes, 0)
}

// Check account and server limits.
// Read Lock should be held
func (js *jetStream) checkAllLimits(selected *JetStreamAccountLimits, tier string, config *StreamConfig, currentRes, maxBytesOffset int64) error {
	return js.checkLimits(selected, tier, config, true, currentRes, maxBytesOffset)
}

// Check if a new proposed msg set while exceed our account limits.
// Lock should be held.
func (js *jetStream) checkLimits(selected *JetStreamAccountLimits, tier string, config *StreamConfig, checkServer bool, currentRes, maxBytesOffset int64) error {
	// Check MaxConsumers
	if config.MaxConsumers > 0 && selected.MaxConsumers > 0 && config.MaxConsumers > selected.MaxConsumers {
		return NewJSMaximumConsumersLimitError()
	}
	// stream limit is checked separately on stream create only!
	// Check storage, memory or disk.
	return js.checkBytesLimits(selected, tier, config.MaxBytes, config.Replicas, config.Storage, checkServer, currentRes, maxBytesOffset)
}

// accountReservation returns how many bytes count against the account limit
// for a stream with the given replica count. Un-tiered limits are flat, so R>1
// is counted as Replicas*bytes; tiered limits already bake in replication.
func accountReservation(tier string, replicas int, bytes int64) int64 {
	if bytes <= 0 {
		return 0
	}
	if tier == _EMPTY_ && replicas > 1 {
		return mulSaturate(int64(replicas), bytes)
	}
	return bytes
}

// Check if additional bytes will exceed our account limits and optionally the server itself.
// Read Lock should be held.
func (js *jetStream) checkBytesLimits(selectedLimits *JetStreamAccountLimits, tier string, addBytes int64, replicas int, storage StorageType, checkServer bool, currentRes, maxBytesOffset int64) error {
	if addBytes < 0 {
		addBytes = 1
	}
	// The per-server footprint is a single replica's worth of bytes; the
	// account footprint additionally accounts for replication in un-tiered setups.
	serverBytes := addSaturate(addBytes, maxBytesOffset)
	accountBytes := accountReservation(tier, replicas, serverBytes)

	switch storage {
	case MemoryStorage:
		// Account limits defined.
		if selectedLimits.MaxMemory >= 0 && (currentRes > selectedLimits.MaxMemory || accountBytes > selectedLimits.MaxMemory-currentRes) {
			return NewJSMemoryResourcesExceededError()
		}
		// Check if this server can handle request.
		if checkServer && (js.memReserved > js.config.MaxMemory || serverBytes > js.config.MaxMemory-js.memReserved) {
			return NewJSMemoryResourcesExceededError()
		}
	case FileStorage:
		// Account limits defined.
		if selectedLimits.MaxStore >= 0 && (currentRes > selectedLimits.MaxStore || accountBytes > selectedLimits.MaxStore-currentRes) {
			return NewJSStorageResourcesExceededError()
		}
		// Check if this server can handle request.
		if checkServer && (js.storeReserved > js.config.MaxStore || serverBytes > js.config.MaxStore-js.storeReserved) {
			return NewJSStorageResourcesExceededError()
		}
	}

	return nil
}

func (jsa *jsAccount) acc() *Account {
	return jsa.account
}

// Delete the JetStream resources.
func (jsa *jsAccount) delete() {
	var streams []*stream

	jsa.mu.Lock()
	// The update timer and subs need to be protected by usageMu lock
	jsa.usageMu.Lock()
	if jsa.utimer != nil {
		jsa.utimer.Stop()
		jsa.utimer = nil
	}
	if jsa.updatesSub != nil && jsa.js.srv != nil {
		s := jsa.js.srv
		s.sysUnsubscribe(jsa.updatesSub)
		jsa.updatesSub = nil
	}
	jsa.usageMu.Unlock()

	for _, ms := range jsa.streams {
		streams = append(streams, ms)
	}
	jsa.mu.Unlock()

	for _, mset := range streams {
		mset.stop(false, false)
	}
}

// Lookup the jetstream account for a given account.
func (js *jetStream) lookupAccount(a *Account) *jsAccount {
	if a == nil {
		return nil
	}
	js.mu.RLock()
	jsa := js.accounts[a.Name]
	js.mu.RUnlock()
	return jsa
}

// Report on JetStream stats and usage for this server.
func (js *jetStream) usageStats() *JetStreamStats {
	var stats JetStreamStats
	js.mu.RLock()
	stats.Accounts = len(js.accounts)
	stats.ReservedMemory = uint64(js.memReserved)
	stats.ReservedStore = uint64(js.storeReserved)
	s := js.srv
	js.mu.RUnlock()
	stats.API.Level = JSApiLevel
	stats.API.Total = uint64(atomic.LoadInt64(&js.apiTotal))
	stats.API.Errors = uint64(atomic.LoadInt64(&js.apiErrors))
	stats.API.Inflight = uint64(atomic.LoadInt64(&js.apiInflight))
	// Make sure we do not report negative.
	used := atomic.LoadInt64(&js.memUsed)
	if used < 0 {
		used = 0
	}
	stats.Memory = uint64(used)
	used = atomic.LoadInt64(&js.storeUsed)
	if used < 0 {
		used = 0
	}
	stats.Store = uint64(used)
	stats.HAAssets = s.numRaftNodes()
	return &stats
}

// Check to see if we have enough system resources for this account.
// Lock should be held.
func (js *jetStream) sufficientResources(limits map[string]JetStreamAccountLimits) error {
	// If we are clustered we do not really know how many resources will be ultimately available.
	// This needs to be handled out of band.
	// If we are a single server, we can make decisions here.
	if limits == nil || !js.standAlone {
		return nil
	}

	totalMaxBytes := func(limits map[string]JetStreamAccountLimits) (int64, int64) {
		totalMaxMemory := int64(0)
		totalMaxStore := int64(0)
		for _, l := range limits {
			if l.MaxMemory > 0 {
				totalMaxMemory += l.MaxMemory
			}
			if l.MaxStore > 0 {
				totalMaxStore += l.MaxStore
			}
		}
		return totalMaxMemory, totalMaxStore
	}

	totalMaxMemory, totalMaxStore := totalMaxBytes(limits)

	// Reserved is now specific to the MaxBytes for streams.
	if js.memReserved+totalMaxMemory > js.config.MaxMemory {
		return NewJSMemoryResourcesExceededError()
	}
	// A dynamic limit is still provisional until recovery has run.
	recovering := js.config.maxStorePending
	if !recovering && js.storeReserved+totalMaxStore > js.config.MaxStore {
		return NewJSStorageResourcesExceededError()
	}

	// Since we know if we are here we are single server mode, check the account reservations.
	var storeReserved, memReserved int64
	for _, jsa := range js.accounts {
		if jsa.account.IsExpired() {
			continue
		}
		jsa.usageMu.RLock()
		maxMemory, maxStore := totalMaxBytes(jsa.limits)
		jsa.usageMu.RUnlock()
		memReserved += maxMemory
		storeReserved += maxStore
	}

	if memReserved+totalMaxMemory > js.config.MaxMemory {
		return NewJSMemoryResourcesExceededError()
	}
	if !recovering && storeReserved+totalMaxStore > js.config.MaxStore {
		return NewJSStorageResourcesExceededError()
	}

	return nil
}

// This will reserve the stream resources requested.
// This will spin off off of MaxBytes.
func (js *jetStream) reserveStreamResources(cfg *StreamConfig) {
	if cfg == nil || cfg.MaxBytes <= 0 {
		return
	}

	js.mu.Lock()
	switch cfg.Storage {
	case MemoryStorage:
		js.memReserved += cfg.MaxBytes
	case FileStorage:
		js.storeReserved += cfg.MaxBytes
	}
	s, clustered := js.srv, !js.standAlone
	js.mu.Unlock()
	// If clustered send an update to the system immediately.
	if clustered {
		s.sendStatszUpdate()
	}
}

// Release reserved resources held by a stream.
func (js *jetStream) releaseStreamResources(cfg *StreamConfig) {
	if cfg == nil || cfg.MaxBytes <= 0 {
		return
	}

	js.mu.Lock()
	switch cfg.Storage {
	case MemoryStorage:
		js.memReserved -= cfg.MaxBytes
	case FileStorage:
		js.storeReserved -= cfg.MaxBytes
	}
	s, clustered := js.srv, !js.standAlone
	js.mu.Unlock()
	// If clustered send an update to the system immediately.
	if clustered {
		s.sendStatszUpdate()
	}
}

const (
	// JetStreamStoreDir is the prefix we use.
	JetStreamStoreDir = "jetstream"
	// JetStreamMaxStoreDefault is the default disk storage limit. 1TB
	JetStreamMaxStoreDefault = 1024 * 1024 * 1024 * 1024
	// JetStreamMaxMemDefault is only used when we can't determine system memory. 256MB
	JetStreamMaxMemDefault = 1024 * 1024 * 256
	// snapshot staging for restores.
	snapStagingDir = ".snap-staging"
)

// Dynamically create a config with a tmp based directory (repeatable) and 75% of system memory.
func (s *Server) dynJetStreamConfig(storeDir string, maxStore, maxMem int64) *JetStreamConfig {
	jsc := &JetStreamConfig{}
	if storeDir != _EMPTY_ {
		jsc.StoreDir = filepath.Join(storeDir, JetStreamStoreDir)
	} else {
		// Create one in tmp directory, but make it consistent for restarts.
		jsc.StoreDir = filepath.Join(os.TempDir(), "nats", JetStreamStoreDir)
		s.Warnf("Temporary storage directory used, data could be lost on system reboot")
	}

	opts := s.getOpts()

	// Strict mode.
	jsc.Strict = !opts.NoJetStreamStrict

	// Sync options.
	jsc.SyncInterval = opts.SyncInterval
	jsc.SyncAlways = opts.SyncAlways

	if maxStore > 0 || (opts.maxStoreSet && maxStore == 0) {
		jsc.MaxStore = maxStore
	} else {
		jsc.MaxStore = diskAvailable(jsc.StoreDir)
		jsc.maxStorePending = true
	}

	if maxMem > 0 || (opts.maxMemSet && maxMem == 0) {
		jsc.MaxMemory = maxMem
	} else {
		// Estimate to 75% of total memory if we can determine system memory.
		if sysMem := sysmem.Memory(); sysMem > 0 {
			// Check if we have been limited with GOMEMLIMIT and if lower use that value.
			if gml := debug.SetMemoryLimit(-1); gml != math.MaxInt64 && gml < sysMem {
				s.Debugf("JetStream detected GOMEMLIMIT of %v", friendlyBytes(gml))
				sysMem = gml
			}
			jsc.MaxMemory = sysMem / 4 * 3
		} else {
			jsc.MaxMemory = JetStreamMaxMemDefault
		}
	}

	return jsc
}

// Helper function.
func (a *Account) checkForJetStream() (*Server, *jsAccount, error) {
	a.mu.RLock()
	s := a.srv
	jsa := a.js
	a.mu.RUnlock()

	if s == nil || jsa == nil {
		return nil, nil, NewJSNotEnabledForAccountError()
	}

	return s, jsa, nil
}

type Number interface {
	int | int8 | int16 | int32 | int64 | uint | uint8 | uint16 | uint32 | uint64 | float32 | float64
}

// friendlyBytes returns a string with the given bytes int64
// represented as a size, such as 1KB, 10MB, etc...
func friendlyBytes[T Number](bytes T) string {
	fbytes := float64(bytes)
	base := 1024
	pre := []string{"K", "M", "G", "T", "P", "E"}
	if fbytes < float64(base) {
		return fmt.Sprintf("%v B", fbytes)
	}
	exp := int(math.Log(fbytes) / math.Log(float64(base)))
	index := exp - 1
	return fmt.Sprintf("%.2f %sB", fbytes/math.Pow(float64(base), float64(exp)), pre[index])
}

func isValidName(name string) bool {
	if name == _EMPTY_ {
		return false
	}
	return !strings.ContainsAny(name, " \t\r\n\f.*>")
}

func isValidAssetName(name string) bool {
	if name == _EMPTY_ {
		return false
	}
	return !strings.ContainsAny(name, " \t\r\n\f.*>\\/")
}

// To throttle the out of resources errors.
func (s *Server) resourcesExceededError(storeType StorageType) {
	var didAlert bool

	s.rerrMu.Lock()
	if now := time.Now(); now.Sub(s.rerrLast) > 10*time.Second {
		s.Errorf("JetStream %s resource limits exceeded for server", strings.ToLower(storeType.String()))
		s.rerrLast = now
		didAlert = true
	}
	s.rerrMu.Unlock()

	// If we are meta leader we should relinquish that here.
	if didAlert {
		if js := s.getJetStream(); js != nil {
			js.mu.RLock()
			if cc := js.cluster; cc != nil && cc.meta != nil {
				cc.meta.StepDown()
			}
			js.mu.RUnlock()
		}
	}
}

// For validating options.
func validateJetStreamOptions(o *Options) error {
	// in non operator mode, the account names need to be configured
	if len(o.JsAccDefaultDomain) > 0 {
		if len(o.TrustedOperators) == 0 {
			for a, domain := range o.JsAccDefaultDomain {
				found := false
				if isReservedAccount(a) {
					found = true
				} else {
					for _, acc := range o.Accounts {
						if a == acc.GetName() {
							if len(acc.jsLimits) > 0 && domain != _EMPTY_ {
								return fmt.Errorf("default_js_domain contains account name %q with enabled JetStream", a)
							}
							found = true
							break
						}
					}
				}
				if !found {
					return fmt.Errorf("in non operator mode, `default_js_domain` references non existing account %q", a)
				}
			}
		} else {
			for a := range o.JsAccDefaultDomain {
				if !nkeys.IsValidPublicAccountKey(a) {
					return fmt.Errorf("default_js_domain contains account name %q, which is not a valid public account nkey", a)
				}
			}
		}
		for a, d := range o.JsAccDefaultDomain {
			sacc := DEFAULT_SYSTEM_ACCOUNT
			if o.SystemAccount != _EMPTY_ {
				sacc = o.SystemAccount
			}
			if a == sacc {
				return fmt.Errorf("system account %q can not be in default_js_domain", a)
			}
			if d == _EMPTY_ {
				continue
			}
			if sub := fmt.Sprintf(jsDomainAPI, d); !IsValidSubject(sub) {
				return fmt.Errorf("default_js_domain contains account %q with invalid domain name %q", a, d)
			}
		}
	}
	if o.JetStreamDomain != _EMPTY_ {
		if subj := fmt.Sprintf(jsDomainAPI, o.JetStreamDomain); !IsValidSubject(subj) {
			return fmt.Errorf("invalid domain name: derived %q is not a valid subject", subj)
		}

		if !isValidName(o.JetStreamDomain) {
			return fmt.Errorf("invalid domain name: may not contain ., * or >")
		}
	}
	// If not clustered no checks needed past here.
	if !o.JetStream || o.Cluster.Port == 0 {
		return nil
	}
	if o.ServerName == _EMPTY_ {
		return fmt.Errorf("jetstream cluster requires `server_name` to be set")
	}
	if o.Cluster.Name == _EMPTY_ {
		return fmt.Errorf("jetstream cluster requires `cluster.name` to be set")
	}

	h := strings.ToLower(o.JetStreamExtHint)
	switch h {
	case jsWillExtend, jsNoExtend, _EMPTY_:
		o.JetStreamExtHint = h
	default:
		return fmt.Errorf("expected 'no_extend' for string value, got '%s'", h)
	}

	if o.JetStreamMaxCatchup < 0 {
		return fmt.Errorf("jetstream max catchup cannot be negative")
	}
	return nil
}

// We had a bug that set a default de dupe window on mirror, despite that being not a valid config
func fixCfgMirrorWithDedupWindow(cfg *StreamConfig) {
	if cfg == nil || cfg.Mirror == nil {
		return
	}
	if cfg.Duplicates != 0 {
		cfg.Duplicates = 0
	}
}

func (s *Server) handleWritePermissionError() {
	//TODO Check if we should add s.jetStreamOOSPending in condition
	if s.JetStreamEnabled() {
		s.Errorf("File system permission denied while writing, disabling JetStream")

		go s.ShutdownJetStream()

		//TODO Send respective advisory if needed, same as in handleOutOfSpace
	}
}
