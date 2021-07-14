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
	"crypto/hmac"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"math"
	"os"
	"path"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/minio/highwayhash"
	"github.com/nats-io/nats-server/v2/server/sysmem"
	"github.com/nats-io/nkeys"
	"github.com/nats-io/nuid"
	"golang.org/x/crypto/chacha20poly1305"
)

// JetStreamConfig determines this server's configuration.
// MaxMemory and MaxStore are in bytes.
type JetStreamConfig struct {
	MaxMemory int64  `json:"max_memory"`
	MaxStore  int64  `json:"max_storage"`
	StoreDir  string `json:"store_dir,omitempty"`
	Domain    string `json:"domain,omitempty"`
}

type JetStreamStats struct {
	Memory   uint64            `json:"memory"`
	Store    uint64            `json:"storage"`
	Accounts int               `json:"accounts,omitempty"`
	API      JetStreamAPIStats `json:"api"`
}

type JetStreamAccountLimits struct {
	MaxMemory    int64 `json:"max_memory"`
	MaxStore     int64 `json:"max_storage"`
	MaxStreams   int   `json:"max_streams"`
	MaxConsumers int   `json:"max_consumers"`
}

// JetStreamAccountStats returns current statistics about the account's JetStream usage.
type JetStreamAccountStats struct {
	Memory    uint64                 `json:"memory"`
	Store     uint64                 `json:"storage"`
	Streams   int                    `json:"streams"`
	Consumers int                    `json:"consumers"`
	Domain    string                 `json:"domain,omitempty"`
	API       JetStreamAPIStats      `json:"api"`
	Limits    JetStreamAccountLimits `json:"limits"`
}

type JetStreamAPIStats struct {
	Total  uint64 `json:"total"`
	Errors uint64 `json:"errors"`
}

// This is for internal accounting for JetStream for this server.
type jetStream struct {
	// These are here first because of atomics on 32bit systems.
	memReserved   int64
	storeReserved int64
	apiCalls      int64
	apiErrors     int64
	memTotal      int64
	storeTotal    int64
	mu            sync.RWMutex
	srv           *Server
	config        JetStreamConfig
	cluster       *jetStreamCluster
	accounts      map[string]*jsAccount
	apiSubs       *Sublist
	disabled      bool
	oos           bool
}

// This represents a jetstream enabled account.
// Worth noting that we include the jetstream pointer, this is because
// in general we want to be very efficient when receiving messages on
// an internal sub for a stream, so we will direct link to the stream
// and walk backwards as needed vs multiple hash lookups and locks, etc.
type jsAccount struct {
	mu            sync.RWMutex
	js            *jetStream
	account       *Account
	limits        JetStreamAccountLimits
	memReserved   int64
	storeReserved int64
	memTotal      int64
	storeTotal    int64
	apiTotal      uint64
	apiErrors     uint64
	usage         jsaUsage
	rusage        map[string]*jsaUsage
	storeDir      string
	streams       map[string]*stream
	templates     map[string]*streamTemplate
	store         TemplateStore

	// Cluster support
	updatesPub string
	updatesSub *subscription
	// From server
	sendq   chan *pubMsg
	lupdate time.Time
	utimer  *time.Timer
}

// Track general usage for this account.
type jsaUsage struct {
	mem   int64
	store int64
	api   uint64
	err   uint64
}

// EnableJetStream will enable JetStream support on this server with the given configuration.
// A nil configuration will dynamically choose the limits and temporary file storage directory.
func (s *Server) EnableJetStream(config *JetStreamConfig) error {
	if s.JetStreamEnabled() {
		return fmt.Errorf("jetstream already enabled")
	}

	s.Noticef("Starting JetStream")
	if config == nil || config.MaxMemory <= 0 || config.MaxStore <= 0 {
		var storeDir, domain string
		var maxStore, maxMem int64
		if config != nil {
			storeDir, domain = config.StoreDir, config.Domain
			maxStore, maxMem = config.MaxStore, config.MaxMemory
		}
		config = s.dynJetStreamConfig(storeDir, maxStore)
		if maxMem > 0 {
			config.MaxMemory = maxMem
		}
		if domain != _EMPTY_ {
			config.Domain = domain
		}
		s.Debugf("JetStream creating dynamic configuration - %s memory, %s disk", friendlyBytes(config.MaxMemory), friendlyBytes(config.MaxStore))
	} else if config.StoreDir != _EMPTY_ {
		config.StoreDir = filepath.Join(config.StoreDir, JetStreamStoreDir)
	}

	cfg := *config
	if cfg.StoreDir == _EMPTY_ {
		cfg.StoreDir = filepath.Join(os.TempDir(), JetStreamStoreDir)
	}

	// We will consistently place the 'jetstream' directory under the storedir that was handed to us. Prior to 2.2.3 though
	// we could have a directory on disk without the 'jetstream' directory. This will check and fix if needed.
	if err := s.checkStoreDir(&cfg); err != nil {
		return err
	}

	if ek := s.getOpts().JetStreamKey; ek != _EMPTY_ {
		s.Warnf("JetStream Encryption is Beta")
	}

	return s.enableJetStream(cfg)
}

// Function signature to generate a key encryption key.
type keyGen func(context []byte) []byte

// Return a key generation function or nil if encryption not enabled.
// keyGen defined in filestore.go - keyGen func(iv, context []byte) []byte
func (s *Server) jsKeyGen(info string) keyGen {
	if ek := s.getOpts().JetStreamKey; ek != _EMPTY_ {
		return func(context []byte) []byte {
			h := hmac.New(sha256.New, []byte(ek))
			h.Write([]byte(info))
			h.Write(context)
			return h.Sum(nil)
		}
	}
	return nil
}

// Decode the encrypted metafile.
func (s *Server) decryptMeta(ekey, buf []byte, acc, context string) ([]byte, error) {
	if len(ekey) != metaKeySize {
		return nil, errors.New("bad encryption key")
	}
	prf := s.jsKeyGen(acc)
	if prf == nil {
		return nil, errNoEncryption
	}
	kek, err := chacha20poly1305.NewX(prf([]byte(context)))
	if err != nil {
		return nil, err
	}
	ns := kek.NonceSize()
	seed, err := kek.Open(nil, ekey[:ns], ekey[ns:], nil)
	if err != nil {
		return nil, err
	}
	aek, err := chacha20poly1305.NewX(seed[:])
	if err != nil {
		return nil, err
	}
	ns = kek.NonceSize()
	plain, err := aek.Open(nil, buf[:ns], buf[ns:], nil)
	if err != nil {
		return nil, err
	}
	return plain, nil
}

// Check to make sure directory has the jetstream directory.
// We will have it properly configured here now regardless, so need to look inside.
func (s *Server) checkStoreDir(cfg *JetStreamConfig) error {
	fis, _ := ioutil.ReadDir(cfg.StoreDir)
	// If we have nothing underneath us, could be just starting new, but if we see this we can check.
	if len(fis) != 0 {
		return nil
	}
	// Let's check the directory above. If it has us 'jetstream' but also other stuff that we can
	// identify as accounts then we can fix.
	fis, _ = ioutil.ReadDir(filepath.Dir(cfg.StoreDir))
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

// enableJetStream will start up the JetStream subsystem.
func (s *Server) enableJetStream(cfg JetStreamConfig) error {
	s.mu.Lock()
	s.js = &jetStream{srv: s, config: cfg, accounts: make(map[string]*jsAccount), apiSubs: NewSublistNoCache()}
	s.mu.Unlock()

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
		tmpfile, err := ioutil.TempFile(cfg.StoreDir, "_test_")
		if err != nil {
			return fmt.Errorf("storage directory is not writable")
		}
		tmpfile.Close()
		os.Remove(tmpfile.Name())
	}

	// JetStream is an internal service so we need to make sure we have a system account.
	// This system account will export the JetStream service endpoints.
	if s.SystemAccount() == nil {
		s.SetDefaultSystemAccount()
	}

	s.Noticef("    _ ___ _____ ___ _____ ___ ___   _   __  __")
	s.Noticef(" _ | | __|_   _/ __|_   _| _ \\ __| /_\\ |  \\/  |")
	s.Noticef("| || | _|  | | \\__ \\ | | |   / _| / _ \\| |\\/| |")
	s.Noticef(" \\__/|___| |_| |___/ |_| |_|_\\___/_/ \\_\\_|  |_|")
	s.Noticef("")
	s.Noticef("         https://docs.nats.io/jetstream")
	s.Noticef("")
	s.Noticef("---------------- JETSTREAM ----------------")
	s.Noticef("  Max Memory:      %s", friendlyBytes(cfg.MaxMemory))
	s.Noticef("  Max Storage:     %s", friendlyBytes(cfg.MaxStore))
	s.Noticef("  Store Directory: \"%s\"", cfg.StoreDir)
	if cfg.Domain != _EMPTY_ {
		s.Noticef("  Domain:          %s", cfg.Domain)
	}
	s.Noticef("-------------------------------------------")

	// Setup our internal subscriptions.
	if err := s.setJetStreamExportSubs(); err != nil {
		return fmt.Errorf("setting up internal jetstream subscriptions failed: %v", err)
	}

	// Setup our internal system exports.
	s.Debugf("  Exports:")
	s.Debugf("     %s", jsAllAPI)
	s.setupJetStreamExports()

	// Enable accounts and restore state before starting clustering.
	if err := s.enableJetStreamAccounts(); err != nil {
		return err
	}

	// If we are in clustered mode go ahead and start the meta controller.
	if !s.standAloneMode() || s.wantsToExtendOtherDomain() {
		if err := s.enableJetStreamClustering(); err != nil {
			return err
		}
	}

	return nil
}

// This will check if we have a solicited leafnode that shares the system account
// and we are not our own domain and we are not denying subjects that would prevent
// extending JetStream.
func (s *Server) wantsToExtendOtherDomain() bool {
	opts := s.getOpts()
	// If we have a domain name set we want to be our own domain.
	if opts.JetStreamDomain != _EMPTY_ {
		return false
	}
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
		StoreDir:  opts.StoreDir,
		MaxMemory: opts.JetStreamMaxMemory,
		MaxStore:  opts.JetStreamMaxStore,
		Domain:    opts.JetStreamDomain,
	}
	s.Noticef("Restarting JetStream")
	err := s.EnableJetStream(&cfg)
	if err != nil {
		s.Warnf("Can't start JetStream: %v", err)
		return s.DisableJetStream()
	}
	s.updateJetStreamInfoStatus(true)
	return nil
}

// checkStreamExports will check if we have the JS exports setup
// on the system account, and if not go ahead and set them up.
func (s *Server) checkJetStreamExports() {
	sacc := s.SystemAccount()
	if sacc != nil && sacc.getServiceExport(jsAllAPI) == nil {
		s.setupJetStreamExports()
	}
}

func (s *Server) setupJetStreamExports() {
	// Setup our internal system export.
	if err := s.SystemAccount().AddServiceExport(jsAllAPI, nil); err != nil {
		s.Warnf("Error setting up jetstream service exports: %v", err)
	}
}

func (s *Server) jetStreamOOSPending() (wasPending bool) {
	s.mu.Lock()
	js := s.js
	s.mu.Unlock()
	if js != nil {
		js.mu.Lock()
		wasPending = js.oos
		js.oos = true
		js.mu.Unlock()
	}
	return wasPending
}

func (s *Server) setJetStreamDisabled() {
	s.mu.Lock()
	js := s.js
	s.mu.Unlock()
	if js != nil {
		js.mu.Lock()
		js.disabled = true
		js.mu.Unlock()
	}
}

func (s *Server) handleOutOfSpace(stream string) {
	if s.JetStreamEnabled() && !s.jetStreamOOSPending() {
		s.Errorf("JetStream out of resources, will be DISABLED")
		go s.DisableJetStream()

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
// to have the metacontroller remove us from the peer list.
func (s *Server) DisableJetStream() error {
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
			meta.Delete()
		}
	}

	// Update our info status.
	s.updateJetStreamInfoStatus(false)

	// Normal shutdown.
	s.shutdownJetStream()

	return nil
}

func (s *Server) enableJetStreamAccounts() error {
	// If we have no configured accounts setup then setup imports on global account.
	if s.globalAccountOnly() {
		gacc := s.GlobalAccount()
		if err := s.configJetStream(gacc); err != nil {
			return err
		}
		if err := gacc.EnableJetStream(nil); err != nil {
			return fmt.Errorf("Error enabling jetstream on the global account")
		}
	} else if err := s.configAllJetStreamAccounts(); err != nil {
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

	if !a.serviceImportExists(jsAllAPI) {
		if err := a.AddServiceImport(s.SystemAccount(), jsAllAPI, _EMPTY_); err != nil {
			return fmt.Errorf("Error setting up jetstream service imports for account: %v", err)
		}
	}

	// Check if we have a Domain specified.
	// If so add in a subject mapping that will allow local connected clients to reach us here as well.
	if opts := s.getOpts(); opts.JetStreamDomain != _EMPTY_ {
		src := fmt.Sprintf(jsDomainAPI, opts.JetStreamDomain)
		found := false
		a.mu.RLock()
		for _, m := range a.mappings {
			if src == m.src {
				found = true
				break
			}
		}
		a.mu.RUnlock()
		if !found {
			if err := a.AddMapping(src, jsAllAPI); err != nil {
				s.Errorf("Error adding JetStream domain mapping: %v", err)
			}
		}
	}

	return nil
}

// enableJetStreamEnabledServiceImportOnly will enable the single service import responder.
// Should we do them all regardless?
func (a *Account) enableJetStreamInfoServiceImportOnly() error {
	// Check if this import would be overshadowed. This can happen when accounts
	// are importing from another account for JS access.
	if a.serviceImportShadowed(JSApiAccountInfo) {
		return nil
	}

	return a.enableAllJetStreamServiceImportsAndMappings()
}

func (s *Server) configJetStream(acc *Account) error {
	if acc == nil {
		return nil
	}
	if acc.jsLimits != nil {
		// Check if already enabled. This can be during a reload.
		if acc.JetStreamEnabled() {
			if err := acc.enableAllJetStreamServiceImportsAndMappings(); err != nil {
				return err
			}
			if err := acc.UpdateJetStreamLimits(acc.jsLimits); err != nil {
				return err
			}
		} else {
			if err := acc.EnableJetStream(acc.jsLimits); err != nil {
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
func (s *Server) configAllJetStreamAccounts() error {
	// Check to see if system account has been enabled. We could arrive here via reload and
	// a non-default system account.
	s.checkJetStreamExports()

	// Snapshot into our own list. Might not be needed.
	s.mu.Lock()
	// Bail if server not enabled. If it was enabled and a reload turns it off
	// that will be handled elsewhere.
	js := s.js
	if js == nil {
		s.mu.Unlock()
		return nil
	}

	var jsAccounts []*Account
	s.accounts.Range(func(k, v interface{}) bool {
		jsAccounts = append(jsAccounts, v.(*Account))
		return true
	})
	accounts := &s.accounts
	s.mu.Unlock()

	// Process any jetstream enabled accounts here. These will be accounts we are
	// already aware of at startup etc.
	for _, acc := range jsAccounts {
		if err := s.configJetStream(acc); err != nil {
			return err
		}
	}

	// Now walk all the storage we have and resolve any accounts that we did not process already.
	// This is important in resolver/operator models.
	fis, _ := ioutil.ReadDir(js.config.StoreDir)
	for _, fi := range fis {
		if accName := fi.Name(); accName != _EMPTY_ {
			// Only load up ones not already loaded since they are processed above.
			if _, ok := accounts.Load(accName); !ok {
				if acc, err := s.lookupAccount(accName); err != nil && acc != nil {
					if err := s.configJetStream(acc); err != nil {
						return err
					}
				}
			}
		}
	}

	return nil
}

// JetStreamEnabled reports if jetstream is enabled.
func (s *Server) JetStreamEnabled() bool {
	s.mu.Lock()
	js := s.js
	s.mu.Unlock()

	var enabled bool
	if js != nil {
		js.mu.RLock()
		enabled = !js.disabled
		js.mu.RUnlock()
	}

	return enabled
}

// Will migrate off ephemerals if possible.
// This means parent stream needs to be replicated.
func (s *Server) migrateEphemerals() {
	js, cc := s.getJetStreamCluster()
	// Make sure JetStream is enabled and we are clustered.
	if js == nil || cc == nil {
		return
	}

	var consumers []*consumerAssignment

	js.mu.Lock()
	ourID := cc.meta.ID()
	for _, asa := range cc.streams {
		for _, sa := range asa {
			if rg := sa.Group; rg != nil && len(rg.Peers) > 1 && rg.isMember(ourID) && len(sa.consumers) > 0 {
				for _, ca := range sa.consumers {
					if ca.Group != nil && len(ca.Group.Peers) == 1 && ca.Group.isMember(ourID) {
						// Need to select possible new peer from parent stream.
						for _, p := range rg.Peers {
							if p != ourID {
								ca.Group.Peers = []string{p}
								ca.Group.Preferred = p
								consumers = append(consumers, ca)
								break
							}
						}
					}
				}
			}
		}
	}
	js.mu.Unlock()

	// Process the consumers.
	for _, ca := range consumers {
		// Locate the consumer itself.
		if acc, err := s.LookupAccount(ca.Client.Account); err == nil && acc != nil {
			if mset, err := acc.lookupStream(ca.Stream); err == nil && mset != nil {
				if o := mset.lookupConsumer(ca.Name); o != nil {
					state := o.readStoreState()
					o.deleteWithoutAdvisory()
					js.mu.Lock()
					// Delete old one.
					cc.meta.ForwardProposal(encodeDeleteConsumerAssignment(ca))
					// Encode state and new name.
					ca.State = state
					ca.Name = createConsumerName()
					addEntry := encodeAddConsumerAssignmentCompressed(ca)
					cc.meta.ForwardProposal(addEntry)
					js.mu.Unlock()
				}
			}
		}
	}

	// Give time for migration information to make it out of our server.
	if len(consumers) > 0 {
		time.Sleep(50 * time.Millisecond)
	}
}

// Shutdown jetstream for this server.
func (s *Server) shutdownJetStream() {
	s.mu.Lock()
	js := s.js
	s.mu.Unlock()

	if js == nil {
		return
	}

	s.Noticef("Initiating JetStream Shutdown...")
	defer s.Noticef("JetStream Shutdown")

	var _a [512]*Account
	accounts := _a[:0]

	js.mu.RLock()
	// Collect accounts.
	for _, jsa := range js.accounts {
		if a := jsa.acc(); a != nil {
			accounts = append(accounts, a)
		}
	}
	js.mu.RUnlock()

	for _, a := range accounts {
		a.removeJetStream()
	}

	s.mu.Lock()
	s.js = nil
	s.mu.Unlock()

	js.mu.Lock()
	js.accounts = nil

	if cc := js.cluster; cc != nil {
		js.stopUpdatesSub()
		if cc.c != nil {
			cc.c.closeConnection(ClientClosed)
			cc.c = nil
		}
		cc.meta = nil
	}
	js.mu.Unlock()
}

// JetStreamConfig will return the current config. Useful if the system
// created a dynamic configuration. A copy is returned.
func (s *Server) JetStreamConfig() *JetStreamConfig {
	var c *JetStreamConfig
	s.mu.Lock()
	if s.js != nil {
		copyConf := s.js.config
		c = &(copyConf)
	}
	s.mu.Unlock()
	return c
}

func (s *Server) StoreDir() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.js == nil {
		return _EMPTY_
	}
	return s.js.config.StoreDir
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
		return -1, -1, ApiErrors[JSNotEnabledForAccountErr]
	}
	js.mu.RLock()
	defer js.mu.RUnlock()
	return js.memReserved, js.storeReserved, nil
}

func (s *Server) getJetStream() *jetStream {
	s.mu.Lock()
	js := s.js
	s.mu.Unlock()
	return js
}

func (a *Account) assignJetStreamLimits(limits *JetStreamAccountLimits) {
	a.mu.Lock()
	a.jsLimits = limits
	a.mu.Unlock()
}

// EnableJetStream will enable JetStream on this account with the defined limits.
// This is a helper for JetStreamEnableAccount.
func (a *Account) EnableJetStream(limits *JetStreamAccountLimits) error {
	a.mu.RLock()
	s := a.srv
	a.mu.RUnlock()

	if s == nil {
		return fmt.Errorf("jetstream account not registered")
	}

	s.mu.Lock()
	sendq := s.sys.sendq
	s.mu.Unlock()

	// No limits means we dynamically set up limits.
	// We also place limits here so we know that the account is configured for JetStream.
	if limits == nil {
		limits = dynamicJSAccountLimits
	}

	js := s.getJetStream()
	if js == nil {
		a.assignJetStreamLimits(limits)
		return ApiErrors[JSNotEnabledErr]
	}

	if s.SystemAccount() == a {
		return fmt.Errorf("jetstream can not be enabled on the system account")
	}

	if limits == nil {
		limits = dynamicJSAccountLimits
	}
	a.assignJetStreamLimits(limits)

	js.mu.Lock()
	// Check the limits against existing reservations.
	if _, ok := js.accounts[a.Name]; ok && a.JetStreamEnabled() {
		js.mu.Unlock()
		return fmt.Errorf("jetstream already enabled for account")
	}
	if err := js.sufficientResources(limits); err != nil {
		js.mu.Unlock()
		return err
	}
	jsa := &jsAccount{js: js, account: a, limits: *limits, streams: make(map[string]*stream), sendq: sendq}
	jsa.utimer = time.AfterFunc(usageTick, jsa.sendClusterUsageUpdateTimer)
	jsa.storeDir = path.Join(js.config.StoreDir, a.Name)

	js.accounts[a.Name] = jsa
	js.reserveResources(limits)
	js.mu.Unlock()

	sysNode := s.Node()

	// Cluster mode updates to resource usages, but we always will turn on. System internal prevents echos.
	jsa.mu.Lock()
	jsa.updatesPub = fmt.Sprintf(jsaUpdatesPubT, a.Name, sysNode)
	jsa.updatesSub, _ = s.sysSubscribe(fmt.Sprintf(jsaUpdatesSubT, a.Name), jsa.remoteUpdateUsage)
	jsa.mu.Unlock()

	// Stamp inside account as well.
	a.mu.Lock()
	a.js = jsa
	a.mu.Unlock()

	// Create the proper imports here.
	if err := a.enableAllJetStreamServiceImportsAndMappings(); err != nil {
		return err
	}

	s.Debugf("Enabled JetStream for account %q", a.Name)
	s.Debugf("  Max Memory:      %s", friendlyBytes(limits.MaxMemory))
	s.Debugf("  Max Storage:     %s", friendlyBytes(limits.MaxStore))

	// Clean up any old snapshots that were orphaned while staging.
	os.RemoveAll(path.Join(js.config.StoreDir, snapStagingDir))

	sdir := path.Join(jsa.storeDir, streamsDir)
	if _, err := os.Stat(sdir); os.IsNotExist(err) {
		if err := os.MkdirAll(sdir, defaultDirPerms); err != nil {
			return fmt.Errorf("could not create storage streams directory - %v", err)
		}
		// Just need to make sure we can write to the directory.
		// Remove the directory will create later if needed.
		os.RemoveAll(sdir)

	} else {
		// Restore any state here.
		s.Debugf("Recovering JetStream state for account %q", a.Name)
	}

	// Check templates first since messsage sets will need proper ownership.
	// FIXME(dlc) - Make this consistent.
	tdir := path.Join(jsa.storeDir, tmplsDir)
	if stat, err := os.Stat(tdir); err == nil && stat.IsDir() {
		key := sha256.Sum256([]byte("templates"))
		hh, err := highwayhash.New64(key[:])
		if err != nil {
			return err
		}
		fis, _ := ioutil.ReadDir(tdir)
		for _, fi := range fis {
			metafile := path.Join(tdir, fi.Name(), JetStreamMetaFile)
			metasum := path.Join(tdir, fi.Name(), JetStreamMetaFileSum)
			buf, err := ioutil.ReadFile(metafile)
			if err != nil {
				s.Warnf("  Error reading StreamTemplate metafile %q: %v", metasum, err)
				continue
			}
			if _, err := os.Stat(metasum); os.IsNotExist(err) {
				s.Warnf("  Missing StreamTemplate checksum for %q", metasum)
				continue
			}
			sum, err := ioutil.ReadFile(metasum)
			if err != nil {
				s.Warnf("  Error reading StreamTemplate checksum %q: %v", metasum, err)
				continue
			}
			hh.Reset()
			hh.Write(buf)
			checksum := hex.EncodeToString(hh.Sum(nil))
			if checksum != string(sum) {
				s.Warnf("  StreamTemplate checksums do not match %q vs %q", sum, checksum)
				continue
			}
			var cfg StreamTemplateConfig
			if err := json.Unmarshal(buf, &cfg); err != nil {
				s.Warnf("  Error unmarshalling StreamTemplate metafile: %v", err)
				continue
			}
			cfg.Config.Name = _EMPTY_
			if _, err := a.addStreamTemplate(&cfg); err != nil {
				s.Warnf("  Error recreating StreamTemplate %q: %v", cfg.Name, err)
				continue
			}
		}
	}

	// Now recover the streams.
	fis, _ := ioutil.ReadDir(sdir)
	for _, fi := range fis {
		mdir := path.Join(sdir, fi.Name())
		key := sha256.Sum256([]byte(fi.Name()))
		hh, err := highwayhash.New64(key[:])
		if err != nil {
			return err
		}
		metafile := path.Join(mdir, JetStreamMetaFile)
		metasum := path.Join(mdir, JetStreamMetaFileSum)
		if _, err := os.Stat(metafile); os.IsNotExist(err) {
			s.Warnf("  Missing stream metafile for %q", metafile)
			continue
		}
		buf, err := ioutil.ReadFile(metafile)
		if err != nil {
			s.Warnf("  Error reading metafile %q: %v", metasum, err)
			continue
		}
		if _, err := os.Stat(metasum); os.IsNotExist(err) {
			s.Warnf("  Missing stream checksum for %q", metasum)
			continue
		}
		sum, err := ioutil.ReadFile(metasum)
		if err != nil {
			s.Warnf("  Error reading Stream metafile checksum %q: %v", metasum, err)
			continue
		}
		hh.Write(buf)
		checksum := hex.EncodeToString(hh.Sum(nil))
		if checksum != string(sum) {
			s.Warnf("  Stream metafile checksums do not match %q vs %q", sum, checksum)
			continue
		}

		// Check if we are encrypted.
		if key, err := ioutil.ReadFile(path.Join(mdir, JetStreamMetaFileKey)); err == nil {
			s.Debugf("  Stream metafile is encrypted, reading encrypted keyfile")
			if len(key) != metaKeySize {
				s.Warnf("  Bad stream encryption key length of %d", len(key))
				continue
			}
			// Decode the buffer before proceeding.
			if buf, err = s.decryptMeta(key, buf, a.Name, fi.Name()); err != nil {
				s.Warnf("  Error decrypting our stream metafile: %v", err)
				continue
			}
		}

		var cfg FileStreamInfo
		if err := json.Unmarshal(buf, &cfg); err != nil {
			s.Warnf("  Error unmarshalling stream metafile: %v", err)
			continue
		}

		if cfg.Template != _EMPTY_ {
			if err := jsa.addStreamNameToTemplate(cfg.Template, cfg.Name); err != nil {
				s.Warnf("  Error adding stream %q to template %q: %v", cfg.Name, cfg.Template, err)
			}
		}
		mset, err := a.addStream(&cfg.StreamConfig)
		if err != nil {
			s.Warnf("  Error recreating stream %q: %v", cfg.Name, err)
			continue
		}
		if !cfg.Created.IsZero() {
			mset.setCreatedTime(cfg.Created)
		}

		state := mset.state()
		s.Noticef("  Restored %s messages for stream %q", comma(int64(state.Msgs)), fi.Name())

		// Now do the consumers.
		odir := path.Join(sdir, fi.Name(), consumerDir)
		ofis, _ := ioutil.ReadDir(odir)
		if len(ofis) > 0 {
			s.Noticef("  Recovering %d consumers for stream - %q", len(ofis), fi.Name())
		}
		for _, ofi := range ofis {
			metafile := path.Join(odir, ofi.Name(), JetStreamMetaFile)
			metasum := path.Join(odir, ofi.Name(), JetStreamMetaFileSum)
			if _, err := os.Stat(metafile); os.IsNotExist(err) {
				s.Warnf("    Missing consumer metafile %q", metafile)
				continue
			}
			buf, err := ioutil.ReadFile(metafile)
			if err != nil {
				s.Warnf("    Error reading consumer metafile %q: %v", metasum, err)
				continue
			}
			if _, err := os.Stat(metasum); os.IsNotExist(err) {
				s.Warnf("    Missing consumer checksum for %q", metasum)
				continue
			}

			// Check if we are encrypted.
			if key, err := ioutil.ReadFile(path.Join(odir, ofi.Name(), JetStreamMetaFileKey)); err == nil {
				s.Debugf("  Consumer metafile is encrypted, reading encrypted keyfile")
				// Decode the buffer before proceeding.
				if buf, err = s.decryptMeta(key, buf, a.Name, fi.Name()+tsep+ofi.Name()); err != nil {
					s.Warnf("  Error decrypting our consumer metafile: %v", err)
					continue
				}
			}

			var cfg FileConsumerInfo
			if err := json.Unmarshal(buf, &cfg); err != nil {
				s.Warnf("    Error unmarshalling consumer metafile: %v", err)
				continue
			}
			isEphemeral := !isDurableConsumer(&cfg.ConsumerConfig)
			if isEphemeral {
				// This is an ephermal consumer and this could fail on restart until
				// the consumer can reconnect. We will create it as a durable and switch it.
				cfg.ConsumerConfig.Durable = ofi.Name()
			}
			obs, err := mset.addConsumer(&cfg.ConsumerConfig)
			if err != nil {
				s.Warnf("    Error adding consumer: %v", err)
				continue
			}
			if isEphemeral {
				obs.switchToEphemeral()
			}
			if !cfg.Created.IsZero() {
				obs.setCreatedTime(cfg.Created)
			}
			obs.mu.Lock()
			err = obs.readStoredState()
			obs.mu.Unlock()
			if err != nil {
				s.Warnf("    Error restoring consumer state: %v", err)
			}
		}
	}

	// Make sure to cleanup and old remaining snapshots.
	os.RemoveAll(path.Join(jsa.storeDir, snapsDir))

	s.Debugf("JetStream state for account %q recovered", a.Name)

	return nil
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

	jsa.mu.Lock()
	defer jsa.mu.Unlock()

	var msets []*stream
	for _, mset := range jsa.streams {
		if filter != _EMPTY_ {
			for _, subj := range mset.cfg.Subjects {
				if SubjectsCollide(filter, subj) {
					msets = append(msets, mset)
					break
				}
			}
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
		return nil, ApiErrors[JSNotEnabledForAccountErr]
	}
	jsa.mu.Lock()
	defer jsa.mu.Unlock()

	mset, ok := jsa.streams[name]
	if !ok {
		return nil, ApiErrors[JSStreamNotFoundErr]
	}
	return mset, nil
}

// UpdateJetStreamLimits will update the account limits for a JetStream enabled account.
func (a *Account) UpdateJetStreamLimits(limits *JetStreamAccountLimits) error {
	a.mu.RLock()
	s := a.srv
	jsa := a.js
	a.mu.RUnlock()

	if s == nil {
		return fmt.Errorf("jetstream account not registered")
	}
	js := s.getJetStream()
	if js == nil {
		return ApiErrors[JSNotEnabledErr]
	}
	if jsa == nil {
		return ApiErrors[JSNotEnabledForAccountErr]
	}

	if limits == nil {
		limits = dynamicJSAccountLimits
	}

	// Calculate the delta between what we have and what we want.
	jsa.mu.Lock()
	dl := diffCheckedLimits(&jsa.limits, limits)
	jsaLimits := jsa.limits
	jsa.mu.Unlock()

	js.mu.Lock()
	// Check the limits against existing reservations.
	if err := js.sufficientResources(&dl); err != nil {
		js.mu.Unlock()
		return err
	}
	// FIXME(dlc) - If we drop and are over the max on memory or store, do we delete??
	js.releaseResources(&jsaLimits)
	js.reserveResources(limits)
	js.mu.Unlock()

	// Update
	jsa.mu.Lock()
	jsa.limits = *limits
	jsa.mu.Unlock()

	return nil
}

func diffCheckedLimits(a, b *JetStreamAccountLimits) JetStreamAccountLimits {
	return JetStreamAccountLimits{
		MaxMemory: b.MaxMemory - a.MaxMemory,
		MaxStore:  b.MaxStore - a.MaxStore,
	}
}

// JetStreamUsage reports on JetStream usage and limits for an account.
func (a *Account) JetStreamUsage() JetStreamAccountStats {
	a.mu.RLock()
	jsa, aname := a.js, a.Name
	a.mu.RUnlock()

	var stats JetStreamAccountStats
	if jsa != nil {
		js := jsa.js
		jsa.mu.RLock()
		stats.Memory = uint64(jsa.memTotal)
		stats.Store = uint64(jsa.storeTotal)
		stats.Domain = js.config.Domain
		stats.API = JetStreamAPIStats{
			Total:  jsa.apiTotal,
			Errors: jsa.apiErrors,
		}
		if cc := jsa.js.cluster; cc != nil {
			js.mu.RLock()
			sas := cc.streams[aname]
			stats.Streams = len(sas)
			for _, sa := range sas {
				stats.Consumers += len(sa.consumers)
			}
			js.mu.RUnlock()
		} else {
			stats.Streams = len(jsa.streams)
			for _, mset := range jsa.streams {
				stats.Consumers += mset.numConsumers()
			}
		}
		stats.Limits = jsa.limits
		jsa.mu.RUnlock()
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
		return ApiErrors[JSNotEnabledForAccountErr]
	}

	return js.disableJetStream(js.lookupAccount(a))
}

// Disable JetStream for the account.
func (js *jetStream) disableJetStream(jsa *jsAccount) error {
	if jsa == nil || jsa.account == nil {
		return ApiErrors[JSNotEnabledForAccountErr]
	}

	js.mu.Lock()
	delete(js.accounts, jsa.account.Name)
	js.releaseResources(&jsa.limits)
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
	return a.jsLimits != nil
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

func (jsa *jsAccount) remoteUpdateUsage(sub *subscription, c *client, subject, _ string, msg []byte) {
	const usageSize = 32

	jsa.mu.Lock()
	s := jsa.js.srv
	if len(msg) < usageSize {
		jsa.mu.Unlock()
		s.Warnf("Ignoring remote usage update with size too short")
		return
	}
	var rnode string
	if li := strings.LastIndexByte(subject, btsep); li > 0 && li < len(subject) {
		rnode = subject[li+1:]
	}
	if rnode == _EMPTY_ {
		jsa.mu.Unlock()
		s.Warnf("Received remote usage update with no remote node")
		return
	}
	var le = binary.LittleEndian
	memUsed, storeUsed := int64(le.Uint64(msg[0:])), int64(le.Uint64(msg[8:]))
	apiTotal, apiErrors := le.Uint64(msg[16:]), le.Uint64(msg[24:])

	if jsa.rusage == nil {
		jsa.rusage = make(map[string]*jsaUsage)
	}
	// Update the usage for this remote.
	if usage := jsa.rusage[rnode]; usage != nil {
		// Decrement our old values.
		jsa.memTotal -= usage.mem
		jsa.storeTotal -= usage.store
		jsa.apiTotal -= usage.api
		jsa.apiErrors -= usage.err
		usage.mem, usage.store = memUsed, storeUsed
		usage.api, usage.err = apiTotal, apiErrors
	} else {
		jsa.rusage[rnode] = &jsaUsage{memUsed, storeUsed, apiTotal, apiErrors}
	}
	jsa.memTotal += memUsed
	jsa.storeTotal += storeUsed
	jsa.apiTotal += apiTotal
	jsa.apiErrors += apiErrors
	jsa.mu.Unlock()
}

// Updates accounting on in use memory and storage. This is called from locally
// by the lower storage layers.
func (jsa *jsAccount) updateUsage(storeType StorageType, delta int64) {
	jsa.mu.Lock()
	js := jsa.js
	if storeType == MemoryStorage {
		jsa.usage.mem += delta
		jsa.memTotal += delta
		atomic.AddInt64(&js.memTotal, delta)
	} else {
		jsa.usage.store += delta
		jsa.storeTotal += delta
		atomic.AddInt64(&js.storeTotal, delta)
	}
	// Publish our local updates if in clustered mode.
	if js.cluster != nil {
		jsa.sendClusterUsageUpdate()
	}
	jsa.mu.Unlock()
}

const usageTick = 1500 * time.Millisecond

func (jsa *jsAccount) sendClusterUsageUpdateTimer() {
	jsa.mu.Lock()
	defer jsa.mu.Unlock()
	jsa.sendClusterUsageUpdate()
	if jsa.utimer != nil {
		jsa.utimer.Reset(usageTick)
	}
}

// Send updates to our account usage for this server.
// Lock should be held.
func (jsa *jsAccount) sendClusterUsageUpdate() {
	if jsa.js == nil || jsa.js.srv == nil {
		return
	}
	// These values are absolute so we can limit send rates.
	now := time.Now()
	if now.Sub(jsa.lupdate) < 250*time.Millisecond {
		return
	}
	jsa.lupdate = now

	b := make([]byte, 32)
	var le = binary.LittleEndian
	le.PutUint64(b[0:], uint64(jsa.usage.mem))
	le.PutUint64(b[8:], uint64(jsa.usage.store))
	le.PutUint64(b[16:], uint64(jsa.usage.api))
	le.PutUint64(b[24:], uint64(jsa.usage.err))
	if jsa.sendq != nil {
		jsa.sendq <- &pubMsg{nil, jsa.updatesPub, _EMPTY_, nil, b, false}
	}
}

func (js *jetStream) wouldExceedLimits(storeType StorageType, sz int) bool {
	var (
		total *int64
		max   int64
	)
	if storeType == MemoryStorage {
		total, max = &js.memTotal, js.config.MaxMemory
	} else {
		total, max = &js.storeTotal, js.config.MaxStore
	}
	return atomic.LoadInt64(total) > (max + int64(sz))
}

func (js *jetStream) limitsExceeded(storeType StorageType) bool {
	return js.wouldExceedLimits(storeType, 0)
}

func (jsa *jsAccount) limitsExceeded(storeType StorageType) bool {
	jsa.mu.RLock()
	defer jsa.mu.RUnlock()

	if storeType == MemoryStorage {
		if jsa.limits.MaxMemory > 0 && jsa.memTotal > jsa.limits.MaxMemory {
			return true
		}
	} else {
		if jsa.limits.MaxStore > 0 && jsa.storeTotal > jsa.limits.MaxStore {
			return true
		}
	}

	return false
}

// Check if a new proposed msg set while exceed our account limits.
// Lock should be held.
func (jsa *jsAccount) checkLimits(config *StreamConfig) error {
	if jsa.limits.MaxStreams > 0 && len(jsa.streams) >= jsa.limits.MaxStreams {
		return ApiErrors[JSMaximumStreamsLimitErr]
	}
	// Check MaxConsumers
	if config.MaxConsumers > 0 && jsa.limits.MaxConsumers > 0 && config.MaxConsumers > jsa.limits.MaxConsumers {
		return ApiErrors[JSMaximumConsumersLimitErr]
	}

	// Check storage, memory or disk.
	if config.MaxBytes > 0 {
		return jsa.checkBytesLimits(config.MaxBytes, config.Storage, config.Replicas)
	}
	return nil
}

// Check if additional bytes will exceed our account limits.
// This should account for replicas.
// Lock should be held.
func (jsa *jsAccount) checkBytesLimits(addBytes int64, storage StorageType, replicas int) error {
	if replicas < 1 {
		replicas = 1
	}
	js, totalBytes := jsa.js, addBytes*int64(replicas)

	switch storage {
	case MemoryStorage:
		// Account limits defined.
		if jsa.limits.MaxMemory > 0 {
			if jsa.memReserved+totalBytes > jsa.limits.MaxMemory {
				return ApiErrors[JSMemoryResourcesExceededErr]
			}
		} else {
			// Account is unlimited, check if this server can handle request.
			if js.memReserved+addBytes > js.config.MaxMemory {
				return ApiErrors[JSMemoryResourcesExceededErr]
			}
		}
	case FileStorage:
		// Account limits defined.
		if jsa.limits.MaxStore > 0 {
			if jsa.storeReserved+totalBytes > jsa.limits.MaxStore {
				return ApiErrors[JSStorageResourcesExceededErr]
			}
		} else {
			// Account is unlimited, check if this server can handle request.
			if js.storeReserved+addBytes > js.config.MaxStore {
				return ApiErrors[JSStorageResourcesExceededErr]
			}
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
	var ts []string

	jsa.mu.Lock()
	if jsa.utimer != nil {
		jsa.utimer.Stop()
		jsa.utimer = nil
	}

	if jsa.updatesSub != nil && jsa.js.srv != nil {
		s := jsa.js.srv
		s.sysUnsubscribe(jsa.updatesSub)
		jsa.updatesSub = nil
	}

	for _, ms := range jsa.streams {
		streams = append(streams, ms)
	}
	acc := jsa.account
	for _, t := range jsa.templates {
		ts = append(ts, t.Name)
	}
	jsa.templates = nil
	jsa.mu.Unlock()

	for _, ms := range streams {
		ms.stop(false, false)
	}

	for _, t := range ts {
		acc.deleteStreamTemplate(t)
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
	js.mu.RUnlock()
	stats.API.Total = (uint64)(atomic.LoadInt64(&js.apiCalls))
	stats.API.Errors = (uint64)(atomic.LoadInt64(&js.apiErrors))
	stats.Memory = (uint64)(atomic.LoadInt64(&js.memTotal))
	stats.Store = (uint64)(atomic.LoadInt64(&js.storeTotal))
	return &stats
}

// Check to see if we have enough system resources for this account.
// Lock should be held.
func (js *jetStream) sufficientResources(limits *JetStreamAccountLimits) error {
	if limits == nil {
		return nil
	}

	if js.memReserved+limits.MaxMemory > js.config.MaxMemory {
		return ApiErrors[JSMemoryResourcesExceededErr]
	}
	if js.storeReserved+limits.MaxStore > js.config.MaxStore {
		return ApiErrors[JSStorageResourcesExceededErr]
	}
	return nil
}

// This will (blindly) reserve the respources requested.
// Lock should be held.
func (js *jetStream) reserveResources(limits *JetStreamAccountLimits) error {
	if limits == nil {
		return nil
	}
	if limits.MaxMemory > 0 {
		js.memReserved += limits.MaxMemory
	}
	if limits.MaxStore > 0 {
		js.storeReserved += limits.MaxStore
	}
	return nil
}

// Lock should be held.
func (js *jetStream) releaseResources(limits *JetStreamAccountLimits) error {
	if limits == nil {
		return nil
	}
	if limits.MaxMemory > 0 {
		js.memReserved -= limits.MaxMemory
	}
	if limits.MaxStore > 0 {
		js.storeReserved -= limits.MaxStore
	}
	return nil
}

// Will clear the resource reservations. Mostly for reload of a config.
func (js *jetStream) clearResources() {
	if js == nil {
		return
	}
	js.mu.Lock()
	js.memReserved = 0
	js.storeReserved = 0
	js.mu.Unlock()
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
func (s *Server) dynJetStreamConfig(storeDir string, maxStore int64) *JetStreamConfig {
	jsc := &JetStreamConfig{}
	if storeDir != _EMPTY_ {
		jsc.StoreDir = filepath.Join(storeDir, JetStreamStoreDir)
	} else {
		// Create one in tmp directory, but make it consistent for restarts.
		jsc.StoreDir = filepath.Join(os.TempDir(), "nats", JetStreamStoreDir)
	}

	if maxStore > 0 {
		jsc.MaxStore = maxStore
	} else {
		jsc.MaxStore = diskAvailable(jsc.StoreDir)
	}
	// Estimate to 75% of total memory if we can determine system memory.
	if sysMem := sysmem.Memory(); sysMem > 0 {
		jsc.MaxMemory = sysMem / 4 * 3
	} else {
		jsc.MaxMemory = JetStreamMaxMemDefault
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
		return nil, nil, ApiErrors[JSNotEnabledForAccountErr]
	}

	return s, jsa, nil
}

// StreamTemplateConfig allows a configuration to auto-create streams based on this template when a message
// is received that matches. Each new stream will use the config as the template config to create them.
type StreamTemplateConfig struct {
	Name       string        `json:"name"`
	Config     *StreamConfig `json:"config"`
	MaxStreams uint32        `json:"max_streams"`
}

// StreamTemplateInfo
type StreamTemplateInfo struct {
	Config  *StreamTemplateConfig `json:"config"`
	Streams []string              `json:"streams"`
}

// streamTemplate
type streamTemplate struct {
	mu  sync.Mutex
	tc  *client
	jsa *jsAccount
	*StreamTemplateConfig
	streams []string
}

func (t *StreamTemplateConfig) deepCopy() *StreamTemplateConfig {
	copyTConf := *t
	cfg := *t.Config
	copyTConf.Config = &cfg
	return &copyTConf
}

// addStreamTemplate will add a stream template to this account that allows auto-creation of streams.
func (a *Account) addStreamTemplate(tc *StreamTemplateConfig) (*streamTemplate, error) {
	s, jsa, err := a.checkForJetStream()
	if err != nil {
		return nil, err
	}
	if tc.Config.Name != "" {
		return nil, fmt.Errorf("template config name should be empty")
	}
	if len(tc.Name) > JSMaxNameLen {
		return nil, fmt.Errorf("template name is too long, maximum allowed is %d", JSMaxNameLen)
	}

	// FIXME(dlc) - Hacky
	tcopy := tc.deepCopy()
	tcopy.Config.Name = "_"
	cfg, err := checkStreamCfg(tcopy.Config)
	if err != nil {
		return nil, err
	}
	tcopy.Config = &cfg
	t := &streamTemplate{
		StreamTemplateConfig: tcopy,
		tc:                   s.createInternalJetStreamClient(),
		jsa:                  jsa,
	}
	t.tc.registerWithAccount(a)

	jsa.mu.Lock()
	if jsa.templates == nil {
		jsa.templates = make(map[string]*streamTemplate)
		// Create the appropriate store
		if cfg.Storage == FileStorage {
			jsa.store = newTemplateFileStore(jsa.storeDir)
		} else {
			jsa.store = newTemplateMemStore()
		}
	} else if _, ok := jsa.templates[tcopy.Name]; ok {
		jsa.mu.Unlock()
		return nil, fmt.Errorf("template with name %q already exists", tcopy.Name)
	}
	jsa.templates[tcopy.Name] = t
	jsa.mu.Unlock()

	// FIXME(dlc) - we can not overlap subjects between templates. Need to have test.

	// Setup the internal subscriptions to trap the messages.
	if err := t.createTemplateSubscriptions(); err != nil {
		return nil, err
	}
	if err := jsa.store.Store(t); err != nil {
		t.delete()
		return nil, err
	}
	return t, nil
}

func (t *streamTemplate) createTemplateSubscriptions() error {
	if t == nil {
		return fmt.Errorf("no template")
	}
	if t.tc == nil {
		return fmt.Errorf("template not enabled")
	}
	c := t.tc
	if !c.srv.eventsEnabled() {
		return ErrNoSysAccount
	}
	sid := 1
	for _, subject := range t.Config.Subjects {
		// Now create the subscription
		if _, err := c.processSub([]byte(subject), nil, []byte(strconv.Itoa(sid)), t.processInboundTemplateMsg, false); err != nil {
			c.acc.deleteStreamTemplate(t.Name)
			return err
		}
		sid++
	}
	return nil
}

func (t *streamTemplate) processInboundTemplateMsg(_ *subscription, pc *client, subject, reply string, msg []byte) {
	if t == nil || t.jsa == nil {
		return
	}
	jsa := t.jsa
	cn := canonicalName(subject)

	jsa.mu.Lock()
	// If we already are registered then we can just return here.
	if _, ok := jsa.streams[cn]; ok {
		jsa.mu.Unlock()
		return
	}
	acc := jsa.account
	jsa.mu.Unlock()

	// Check if we are at the maximum and grab some variables.
	t.mu.Lock()
	c := t.tc
	cfg := *t.Config
	cfg.Template = t.Name
	atLimit := len(t.streams) >= int(t.MaxStreams)
	if !atLimit {
		t.streams = append(t.streams, cn)
	}
	t.mu.Unlock()

	if atLimit {
		c.Warnf("JetStream could not create stream for account %q on subject %q, at limit", acc.Name, subject)
		return
	}

	// We need to create the stream here.
	// Change the config from the template and only use literal subject.
	cfg.Name = cn
	cfg.Subjects = []string{subject}
	mset, err := acc.addStream(&cfg)
	if err != nil {
		acc.validateStreams(t)
		c.Warnf("JetStream could not create stream for account %q on subject %q", acc.Name, subject)
		return
	}

	// Process this message directly by invoking mset.
	mset.processInboundJetStreamMsg(nil, pc, subject, reply, msg)
}

// lookupStreamTemplate looks up the names stream template.
func (a *Account) lookupStreamTemplate(name string) (*streamTemplate, error) {
	_, jsa, err := a.checkForJetStream()
	if err != nil {
		return nil, err
	}
	jsa.mu.Lock()
	defer jsa.mu.Unlock()
	if jsa.templates == nil {
		return nil, fmt.Errorf("template not found")
	}
	t, ok := jsa.templates[name]
	if !ok {
		return nil, fmt.Errorf("template not found")
	}
	return t, nil
}

// This function will check all named streams and make sure they are valid.
func (a *Account) validateStreams(t *streamTemplate) {
	t.mu.Lock()
	var vstreams []string
	for _, sname := range t.streams {
		if _, err := a.lookupStream(sname); err == nil {
			vstreams = append(vstreams, sname)
		}
	}
	t.streams = vstreams
	t.mu.Unlock()
}

func (t *streamTemplate) delete() error {
	if t == nil {
		return fmt.Errorf("nil stream template")
	}

	t.mu.Lock()
	jsa := t.jsa
	c := t.tc
	t.tc = nil
	defer func() {
		if c != nil {
			c.closeConnection(ClientClosed)
		}
	}()
	t.mu.Unlock()

	if jsa == nil {
		return ApiErrors[JSNotEnabledForAccountErr]
	}

	jsa.mu.Lock()
	if jsa.templates == nil {
		jsa.mu.Unlock()
		return fmt.Errorf("template not found")
	}
	if _, ok := jsa.templates[t.Name]; !ok {
		jsa.mu.Unlock()
		return fmt.Errorf("template not found")
	}
	delete(jsa.templates, t.Name)
	acc := jsa.account
	jsa.mu.Unlock()

	// Remove streams associated with this template.
	var streams []*stream
	t.mu.Lock()
	for _, name := range t.streams {
		if mset, err := acc.lookupStream(name); err == nil {
			streams = append(streams, mset)
		}
	}
	t.mu.Unlock()

	if jsa.store != nil {
		if err := jsa.store.Delete(t); err != nil {
			return fmt.Errorf("error deleting template from store: %v", err)
		}
	}

	var lastErr error
	for _, mset := range streams {
		if err := mset.delete(); err != nil {
			lastErr = err
		}
	}
	return lastErr
}

func (a *Account) deleteStreamTemplate(name string) error {
	t, err := a.lookupStreamTemplate(name)
	if err != nil {
		return ApiErrors[JSStreamTemplateNotFoundErr]
	}
	return t.delete()
}

func (a *Account) templates() []*streamTemplate {
	var ts []*streamTemplate
	_, jsa, err := a.checkForJetStream()
	if err != nil {
		return nil
	}

	jsa.mu.Lock()
	for _, t := range jsa.templates {
		// FIXME(dlc) - Copy?
		ts = append(ts, t)
	}
	jsa.mu.Unlock()

	return ts
}

// Will add a stream to a template, this is for recovery.
func (jsa *jsAccount) addStreamNameToTemplate(tname, mname string) error {
	if jsa.templates == nil {
		return fmt.Errorf("template not found")
	}
	t, ok := jsa.templates[tname]
	if !ok {
		return fmt.Errorf("template not found")
	}
	// We found template.
	t.mu.Lock()
	t.streams = append(t.streams, mname)
	t.mu.Unlock()
	return nil
}

// This will check if a template owns this stream.
// jsAccount lock should be held
func (jsa *jsAccount) checkTemplateOwnership(tname, sname string) bool {
	if jsa.templates == nil {
		return false
	}
	t, ok := jsa.templates[tname]
	if !ok {
		return false
	}
	// We found template, make sure we are in streams.
	for _, streamName := range t.streams {
		if sname == streamName {
			return true
		}
	}
	return false
}

// friendlyBytes returns a string with the given bytes int64
// represented as a size, such as 1KB, 10MB, etc...
func friendlyBytes(bytes int64) string {
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
	if name == "" {
		return false
	}
	return !strings.ContainsAny(name, " \t\r\n\f.*>")
}

// CanonicalName will replace all token separators '.' with '_'.
// This can be used when naming streams or consumers with multi-token subjects.
func canonicalName(name string) string {
	return strings.ReplaceAll(name, ".", "_")
}

// To throttle the out of resources errors.
func (s *Server) resourcesExeededError() {
	var didAlert bool

	s.rerrMu.Lock()
	if now := time.Now(); now.Sub(s.rerrLast) > 10*time.Second {
		s.Errorf("JetStream resource limits exceeded for server")
		s.rerrLast = now
		didAlert = true
	}
	s.rerrMu.Unlock()

	// If we are meta leader we should relinguish that here.
	if didAlert {
		if js := s.getJetStream(); js != nil {
			js.mu.RLock()
			if cc := js.cluster; cc != nil && cc.isLeader() {
				cc.meta.StepDown()
			}
			js.mu.RUnlock()
		}
	}
}

// For validating options.
func validateJetStreamOptions(o *Options) error {
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

	return nil
}
